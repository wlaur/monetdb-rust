// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::{borrow::Cow, io::Write, mem, num::NonZeroUsize};

use crate::framing::{BLOCKSIZE, ServerSock, reading::MapiReader};

use super::{
    Cursor, CursorError, CursorResult,
    delayed::DelayedCommands,
    replies::{ReplyParser, response_autocommit},
};

const FILE_TRANSFER: &[u8] = b"\x01\x03\n";
const MORE: &[u8] = b"\x01\x02\n";
pub(super) const DEFAULT_UPLOAD_CHUNK_SIZE: NonZeroUsize =
    NonZeroUsize::new(16 * 1024 * 1024).unwrap();

impl Cursor {
    pub(super) fn command_with_uploads<'a, F>(
        &mut self,
        command: &[&[u8]],
        response: &mut Vec<u8>,
        upload_chunk_size: NonZeroUsize,
        mut upload: F,
    ) -> CursorResult<()>
    where
        F: FnMut(&str) -> CursorResult<Cow<'a, [u8]>>,
    {
        let mut refused = None;
        self.conn.run_locked_with_timeouts(
            self.timeouts,
            |state,
             delayed: &mut DelayedCommands,
             mut sock: ServerSock|
             -> CursorResult<ServerSock> {
                sock = delayed.send_delayed_plus(sock, command)?;
                sock = delayed.recv_delayed(sock, response, self.conn.max_response_size)?;
                response.clear();
                loop {
                    sock = MapiReader::to_limited(sock, response, self.conn.max_response_size)?;
                    let Some(request) = take_file_request(response)? else {
                        if let Some(autocommit) = response_autocommit(response) {
                            state.autocommit = autocommit;
                        }
                        return Ok(sock);
                    };
                    let filename = match request.strip_prefix("rb ") {
                        Some(filename) => filename,
                        None => {
                            let error = CursorError::FileTransfer(format!(
                                "unsupported server request {request:?}"
                            ));
                            refuse_upload(&mut sock, &error)?;
                            if refused.is_none() {
                                refused = Some(error);
                            }
                            continue;
                        }
                    };
                    let data = match upload(filename) {
                        Ok(data) => data,
                        Err(error) => {
                            refuse_upload(&mut sock, &error)?;
                            if refused.is_none() {
                                refused = Some(error);
                            }
                            continue;
                        }
                    };
                    match send_upload(sock, &data, upload_chunk_size, self.conn.max_response_size)?
                    {
                        UploadOutcome::Complete(next) => sock = next,
                        UploadOutcome::ServerResponse(next, final_response) => {
                            sock = next;
                            response.extend_from_slice(&final_response);
                            if let Some(autocommit) = response_autocommit(response) {
                                state.autocommit = autocommit;
                            }
                            return Ok(sock);
                        }
                    }
                }
            },
        )?;
        if let Some(error) = refused {
            let mut response_problem = ReplyParser::detect_errors(response)
                .err()
                .map(|error| error.to_string());
            match ReplyParser::new(mem::take(response)) {
                Ok(replies) => {
                    self.replies = replies;
                    if let Err(error) = self.exhaust()
                        && response_problem.is_none()
                    {
                        response_problem = Some(error.to_string());
                    }
                }
                Err(error) => {
                    if response_problem.is_none() {
                        response_problem = Some(error.to_string());
                    }
                }
            }
            if let Some(server) = response_problem {
                Err(CursorError::UploadRefused {
                    refusal: Box::new(error),
                    server,
                })
            } else {
                Err(error)
            }
        } else {
            Ok(())
        }
    }
}

fn take_file_request(response: &mut Vec<u8>) -> CursorResult<Option<String>> {
    let Some(marker) = response
        .windows(FILE_TRANSFER.len())
        .enumerate()
        .rfind(|(index, window)| {
            *window == FILE_TRANSFER && (*index == 0 || response[*index - 1] == b'\n')
        })
        .map(|(index, _)| index)
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

fn refuse_upload(sock: &mut ServerSock, error: &CursorError) -> CursorResult<()> {
    let mut message = error.to_string().replace(['\r', '\n'], " ");
    message.push('\n');
    write_fragment(sock, message.as_bytes(), true, &mut Vec::new())
}

fn send_upload(
    mut sock: ServerSock,
    data: &[u8],
    upload_chunk_size: NonZeroUsize,
    max_response_size: usize,
) -> CursorResult<UploadOutcome> {
    let mut framed = Vec::new();
    // A non-final message fragment containing just a newline accepts the
    // upload. See pymonetdb.filetransfer.uploads.Upload._raw and MonetDB's
    // clients/mapilib/mapi.c `rb FILE` handling.
    write_fragment(&mut sock, b"\n", false, &mut framed)?;

    let mut chunks = data.chunks(upload_chunk_size.get()).peekable();
    if chunks.peek().is_none() {
        write_fragment(&mut sock, b"", true, &mut framed)?;
        match expect_upload_prompt(sock, MORE, max_response_size)? {
            UploadOutcome::Complete(next) => sock = next,
            response @ UploadOutcome::ServerResponse(..) => return Ok(response),
        }
    } else {
        let mut prompt = Vec::new();
        // Every iteration consumes one distinct upload chunk and one server
        // prompt, so an empty prompt cannot repeat the same request indefinitely.
        for chunk in chunks {
            write_fragment(&mut sock, chunk, true, &mut framed)?;
            prompt.clear();
            sock = MapiReader::to_limited(sock, &mut prompt, max_response_size)?;
            if prompt == FILE_TRANSFER {
                return Ok(UploadOutcome::Complete(sock));
            }
            if prompt != MORE {
                return Ok(UploadOutcome::ServerResponse(sock, prompt));
            }
        }
    }

    // MORE after the last data chunk asks for another message. An empty
    // message marks EOF; FILE_TRANSFER acknowledges completion.
    write_fragment(&mut sock, b"", true, &mut framed)?;
    expect_upload_prompt(sock, FILE_TRANSFER, max_response_size)
}

fn expect_upload_prompt(
    mut sock: ServerSock,
    expected: &[u8],
    max_response_size: usize,
) -> CursorResult<UploadOutcome> {
    let mut prompt = Vec::new();
    sock = MapiReader::to_limited(sock, &mut prompt, max_response_size)?;
    if prompt == expected {
        Ok(UploadOutcome::Complete(sock))
    } else {
        Ok(UploadOutcome::ServerResponse(sock, prompt))
    }
}

enum UploadOutcome {
    Complete(ServerSock),
    ServerResponse(ServerSock, Vec<u8>),
}

fn write_fragment(
    sock: &mut impl Write,
    mut data: &[u8],
    finish: bool,
    framed: &mut Vec<u8>,
) -> CursorResult<()> {
    framed.clear();
    if data.is_empty() {
        if finish {
            framed.extend_from_slice(&1u16.to_le_bytes());
        }
    } else {
        framed.reserve(data.len() + 2 * data.len().div_ceil(BLOCKSIZE));
        while !data.is_empty() {
            let length = data.len().min(BLOCKSIZE);
            let (chunk, remaining) = data.split_at(length);
            let last = finish && remaining.is_empty();
            let header = ((length as u16) << 1) | u16::from(last);
            framed.extend_from_slice(&header.to_le_bytes());
            framed.extend_from_slice(chunk);
            data = remaining;
        }
    }
    sock.write_all(framed)?;
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

    #[test]
    fn ignores_embedded_file_transfer_marker() {
        let mut response = b"[ \"prefix\x01\x03\nrb not-a-request\"\t]\n".to_vec();
        assert_eq!(take_file_request(&mut response).unwrap(), None);
    }

    #[test]
    fn batches_fragment_headers_and_payload() {
        let data = vec![b'x'; BLOCKSIZE + 1];
        let mut output = Vec::new();
        let mut framed = Vec::new();
        write_fragment(&mut output, &data, true, &mut framed).unwrap();

        assert_eq!(&output[..2], &((BLOCKSIZE as u16) << 1).to_le_bytes());
        let second_header = 2 + BLOCKSIZE;
        assert_eq!(
            &output[second_header..second_header + 2],
            &3u16.to_le_bytes()
        );
        assert_eq!(output.len(), data.len() + 4);
    }
}
