// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::{collections::HashMap, io::Write};

use crate::framing::{reading::MapiReader, ServerSock, BLOCKSIZE};

use super::{delayed::DelayedCommands, Cursor, CursorError, CursorResult};

const FILE_TRANSFER: &[u8] = b"\x01\x03\n";
const MORE: &[u8] = b"\x01\x02\n";
const UPLOAD_CHUNK: usize = 1024 * 1024;

impl Cursor {
    pub(super) fn command_with_uploads(
        &mut self,
        command: &[&[u8]],
        response: &mut Vec<u8>,
        uploads: &HashMap<String, Vec<u8>>,
    ) -> CursorResult<()> {
        self.conn.run_locked(
            |_state,
             delayed: &mut DelayedCommands,
             mut sock: ServerSock|
             -> CursorResult<ServerSock> {
                sock = delayed.send_delayed_plus(sock, command)?;
                sock = delayed.recv_delayed(sock, response)?;
                response.clear();
                loop {
                    sock = MapiReader::to_end(sock, response)?;
                    let Some(request) = take_file_request(response)? else {
                        return Ok(sock);
                    };
                    let Some(filename) = request.strip_prefix("rb ") else {
                        return Err(CursorError::FileTransfer(format!(
                            "unsupported server request {request:?}"
                        )));
                    };
                    let Some(data) = uploads.get(filename) else {
                        return Err(CursorError::FileTransfer(format!(
                            "server requested unknown file {filename:?}"
                        )));
                    };
                    sock = upload(sock, data)?;
                }
            },
        )
    }
}

fn take_file_request(response: &mut Vec<u8>) -> CursorResult<Option<String>> {
    let Some(marker) = response
        .windows(FILE_TRANSFER.len())
        .rposition(|window| window == FILE_TRANSFER)
    else {
        return Ok(None);
    };
    let command = &response[marker + FILE_TRANSFER.len()..];
    let Some(command) = command.strip_suffix(b"\n") else {
        return Err(CursorError::FileTransfer(
            "unterminated server request".into(),
        ));
    };
    let command = std::str::from_utf8(command)
        .map_err(|_| CursorError::FileTransfer("server request is not UTF-8".into()))?
        .to_owned();
    response.truncate(marker);
    Ok(Some(command))
}

fn upload(mut sock: ServerSock, data: &[u8]) -> CursorResult<ServerSock> {
    // A non-final message fragment containing just a newline accepts the
    // upload. See pymonetdb.filetransfer.uploads.Upload._raw and MonetDB's
    // clients/mapilib/mapi.c `rb FILE` handling.
    write_fragment(&mut sock, b"\n", false)?;

    let mut chunks = data.chunks(UPLOAD_CHUNK).peekable();
    if chunks.peek().is_none() {
        write_fragment(&mut sock, b"", true)?;
        sock = expect_upload_prompt(sock, MORE)?;
    } else {
        for chunk in chunks {
            write_fragment(&mut sock, chunk, true)?;
            let mut prompt = Vec::new();
            sock = MapiReader::to_end(sock, &mut prompt)?;
            if prompt == FILE_TRANSFER {
                return Ok(sock);
            }
            if prompt != MORE {
                return Err(CursorError::FileTransfer(format!(
                    "unexpected upload prompt {:?}",
                    String::from_utf8_lossy(&prompt)
                )));
            }
        }
    }

    // MORE after the last data chunk asks for another message. An empty
    // message marks EOF; FILE_TRANSFER acknowledges completion.
    write_fragment(&mut sock, b"", true)?;
    expect_upload_prompt(sock, FILE_TRANSFER)
}

fn expect_upload_prompt(mut sock: ServerSock, expected: &[u8]) -> CursorResult<ServerSock> {
    let mut prompt = Vec::new();
    sock = MapiReader::to_end(sock, &mut prompt)?;
    if prompt != expected {
        return Err(CursorError::FileTransfer(format!(
            "unexpected upload prompt {:?}",
            String::from_utf8_lossy(&prompt)
        )));
    }
    Ok(sock)
}

fn write_fragment(sock: &mut ServerSock, mut data: &[u8], finish: bool) -> CursorResult<()> {
    if data.is_empty() {
        if finish {
            sock.write_all(&1u16.to_le_bytes())?;
        }
        return Ok(());
    }
    while !data.is_empty() {
        let length = data.len().min(BLOCKSIZE);
        let (chunk, remaining) = data.split_at(length);
        let last = finish && remaining.is_empty();
        let header = ((length as u16) << 1) | u16::from(last);
        sock.write_all(&header.to_le_bytes())?;
        sock.write_all(chunk)?;
        data = remaining;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_file_request_and_preserves_query_output() {
        let mut response = b"&2 3\n\x01\x03\nrb c0\n".to_vec();
        assert_eq!(
            take_file_request(&mut response).unwrap(),
            Some("rb c0".into())
        );
        assert_eq!(response, b"&2 3\n");
        assert_eq!(take_file_request(&mut response).unwrap(), None);
    }
}
