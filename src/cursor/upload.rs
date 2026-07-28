// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::{
    io::{IoSlice, Write},
    mem,
    num::NonZeroUsize,
};

use crate::framing::{BLOCKSIZE, ServerSock, reading::MapiReader};

use super::{
    Cursor, CursorError, CursorResult, UploadSink,
    delayed::DelayedCommands,
    replies::{ReplyParser, response_autocommit},
};

const FILE_TRANSFER: &[u8] = b"\x01\x03\n";
const MORE: &[u8] = b"\x01\x02\n";
const SCATTER_BLOCKS_PER_WRITE: usize = 64;
pub(super) const DEFAULT_UPLOAD_CHUNK_SIZE: NonZeroUsize =
    NonZeroUsize::new(super::DEFAULT_UPLOAD_CHUNK_SIZE_BYTES).unwrap();

impl Cursor {
    pub(super) fn command_with_uploads<F>(
        &mut self,
        command: &[&[u8]],
        response: &mut Vec<u8>,
        upload_chunk_size: NonZeroUsize,
        mut upload: F,
    ) -> CursorResult<()>
    where
        F: FnMut(&str, &mut dyn UploadSink) -> CursorResult<()>,
    {
        let mut refused = None;
        let mut delayed_error = None;
        self.conn.run_locked_with_timeouts(
            self.timeouts,
            |state,
             delayed: &mut DelayedCommands,
             mut sock: ServerSock|
             -> CursorResult<ServerSock> {
                sock = delayed.send_delayed_plus(sock, command)?;
                (sock, delayed_error) =
                    delayed.recv_delayed(sock, response, self.conn.max_response_size)?;
                response.clear();
                loop {
                    sock = MapiReader::to_limited(sock, response, self.conn.max_response_size)?;
                    let Some(request) = take_file_request(response)? else {
                        if let Some(autocommit) = response_autocommit(response) {
                            state.autocommit = autocommit;
                        }
                        return Ok(sock);
                    };
                    let filename = match upload_filename(&request) {
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
                    let mut sink =
                        StreamingUpload::new(sock, upload_chunk_size, self.conn.max_response_size);
                    match upload(filename, &mut sink) {
                        Ok(()) => {}
                        Err(error) if sink.started() => match sink.take_outcome() {
                            Some(UploadOutcome::Complete(next)) => {
                                sock = next;
                                continue;
                            }
                            Some(UploadOutcome::ServerResponse(_, _)) | None => return Err(error),
                        },
                        Err(error) => {
                            sock = sink.into_socket()?;
                            refuse_upload(&mut sock, &error)?;
                            if refused.is_none() {
                                refused = Some(error);
                            }
                            continue;
                        }
                    }
                    match sink.finish()? {
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
        if let Some(error) = delayed_error {
            self.discard_sql_response(response);
            return Err(CursorError::Server(error));
        }
        if let Some(error) = refused {
            let mut response_problem = ReplyParser::detect_errors(response)
                .err()
                .map(|error| error.to_string());
            match ReplyParser::new(mem::take(response)) {
                Ok(replies) => {
                    self.replies = replies;
                    if let Err(error) = self.exhaust() {
                        if response_problem.is_none() {
                            response_problem = Some(error.to_string());
                        }
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

fn upload_filename(request: &str) -> Option<&str> {
    if let Some(filename) = request.strip_prefix("rb ") {
        return Some(filename);
    }
    let filename = request.strip_prefix("r 0 ")?;
    (!filename.is_empty()).then_some(filename)
}

fn take_file_request(response: &mut Vec<u8>) -> CursorResult<Option<String>> {
    let mut end = response.len();
    let marker = loop {
        let Some(marker) = memchr::memmem::rfind(&response[..end], FILE_TRANSFER) else {
            return Ok(None);
        };
        if marker == 0 || response[marker - 1] == b'\n' {
            break marker;
        }
        end = marker;
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
    write_fragment(sock, message.as_bytes(), true)
}

struct StreamingUpload {
    sock: Option<ServerSock>,
    upload_chunk_size: NonZeroUsize,
    max_response_size: usize,
    pending: Vec<u8>,
    started: bool,
    outcome: Option<UploadOutcome>,
}

impl StreamingUpload {
    fn new(sock: ServerSock, upload_chunk_size: NonZeroUsize, max_response_size: usize) -> Self {
        Self {
            sock: Some(sock),
            upload_chunk_size,
            max_response_size,
            pending: Vec::new(),
            started: false,
            outcome: None,
        }
    }

    fn started(&self) -> bool {
        self.started
    }

    fn into_socket(mut self) -> CursorResult<ServerSock> {
        self.sock.take().ok_or(CursorError::Closed)
    }

    fn take_outcome(&mut self) -> Option<UploadOutcome> {
        self.outcome.take()
    }

    fn start(&mut self) -> CursorResult<()> {
        if self.started {
            return Ok(());
        }
        let sock = self.sock.as_mut().ok_or(CursorError::Closed)?;
        // A non-final message fragment containing just a newline accepts the
        // upload. See pymonetdb.filetransfer.uploads.Upload._raw and MonetDB's
        // clients/mapilib/mapi.c `rb FILE` handling.
        write_fragment(sock, b"\n", false)?;
        self.started = true;
        Ok(())
    }

    fn send_message(&mut self, data: &[u8]) -> CursorResult<()> {
        let mut sock = self.sock.take().ok_or(CursorError::Closed)?;
        write_fragment(&mut sock, data, true)?;
        let mut prompt = Vec::new();
        sock = MapiReader::to_limited(sock, &mut prompt, self.max_response_size)?;
        if prompt == MORE {
            self.sock = Some(sock);
        } else if prompt == FILE_TRANSFER {
            self.outcome = Some(UploadOutcome::Complete(sock));
        } else {
            self.outcome = Some(UploadOutcome::ServerResponse(sock, prompt));
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> CursorResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = mem::take(&mut self.pending);
        let result = self.send_message(&pending);
        self.pending = pending;
        self.pending.clear();
        result
    }

    fn finish(mut self) -> CursorResult<UploadOutcome> {
        if let Some(outcome) = self.outcome.take() {
            return Ok(outcome);
        }
        self.flush_pending()?;
        if let Some(outcome) = self.outcome.take() {
            return Ok(outcome);
        }
        self.start()?;
        self.send_message(b"")?;
        if let Some(outcome) = self.outcome.take() {
            return Ok(outcome);
        }
        // MonetDB can request a second empty message to terminate an empty file.
        self.send_message(b"")?;
        self.outcome.take().ok_or_else(|| {
            CursorError::FileTransfer("server requested data after upload EOF".into())
        })
    }
}

impl UploadSink for StreamingUpload {
    fn write_chunk(&mut self, mut data: &[u8]) -> CursorResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        if self.outcome.is_some() {
            return Err(CursorError::FileTransfer(
                "server completed the upload before the producer".into(),
            ));
        }
        self.start()?;
        let target = self.upload_chunk_size.get();
        if !self.pending.is_empty() {
            let needed = target - self.pending.len();
            let split = needed.min(data.len());
            self.pending.extend_from_slice(&data[..split]);
            data = &data[split..];
            if self.pending.len() == target {
                self.flush_pending()?;
                if self.outcome.is_some() && !data.is_empty() {
                    return Err(CursorError::FileTransfer(
                        "server completed the upload before the producer".into(),
                    ));
                }
            }
        }
        while data.len() >= target {
            let (chunk, remaining) = data.split_at(target);
            self.send_message(chunk)?;
            if self.outcome.is_some() {
                if remaining.is_empty() {
                    return Ok(());
                }
                return Err(CursorError::FileTransfer(
                    "server completed the upload before the producer".into(),
                ));
            }
            data = remaining;
        }
        self.pending.extend_from_slice(data);
        Ok(())
    }
}

enum UploadOutcome {
    Complete(ServerSock),
    ServerResponse(ServerSock, Vec<u8>),
}

fn write_fragment(sock: &mut impl Write, data: &[u8], finish: bool) -> CursorResult<()> {
    if data.is_empty() {
        if finish {
            sock.write_all(&1u16.to_le_bytes())?;
        }
        return Ok(());
    }

    let blocks = data.len().div_ceil(BLOCKSIZE);
    let headers = (0..blocks)
        .map(|index| {
            let offset = index * BLOCKSIZE;
            let length = (data.len() - offset).min(BLOCKSIZE);
            let last = finish && index + 1 == blocks;
            (((length as u16) << 1) | u16::from(last)).to_le_bytes()
        })
        .collect::<Vec<_>>();
    for first in (0..blocks).step_by(SCATTER_BLOCKS_PER_WRITE) {
        let last = (first + SCATTER_BLOCKS_PER_WRITE).min(blocks);
        let mut slices = Vec::with_capacity((last - first) * 2);
        for (index, header) in headers.iter().enumerate().take(last).skip(first) {
            let offset = index * BLOCKSIZE;
            let end = (offset + BLOCKSIZE).min(data.len());
            slices.push(IoSlice::new(header));
            slices.push(IoSlice::new(&data[offset..end]));
        }
        write_all_vectored(sock, &mut slices)?;
    }
    Ok(())
}

fn write_all_vectored(writer: &mut impl Write, buffers: &mut [IoSlice<'_>]) -> std::io::Result<()> {
    let mut remaining = buffers;
    IoSlice::advance_slices(&mut remaining, 0);
    while !remaining.is_empty() {
        match writer.write_vectored(remaining) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write framed upload",
                ));
            }
            Ok(written) => IoSlice::advance_slices(&mut remaining, written),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn copied_fragment(data: &[u8], finish: bool) -> Vec<u8> {
        let mut framed = Vec::new();
        if data.is_empty() {
            if finish {
                framed.extend_from_slice(&1u16.to_le_bytes());
            }
            return framed;
        }
        for (index, chunk) in data.chunks(BLOCKSIZE).enumerate() {
            let last = finish && (index + 1) * BLOCKSIZE >= data.len();
            let header = ((chunk.len() as u16) << 1) | u16::from(last);
            framed.extend_from_slice(&header.to_le_bytes());
            framed.extend_from_slice(chunk);
        }
        framed
    }

    struct PartialWriter {
        output: Vec<u8>,
        limit: usize,
    }

    impl Write for PartialWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            let written = data.len().min(self.limit);
            self.output.extend_from_slice(&data[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        fn write_vectored(&mut self, buffers: &[IoSlice<'_>]) -> std::io::Result<usize> {
            let mut written = 0;
            for buffer in buffers {
                let count = buffer.len().min(self.limit - written);
                self.output.extend_from_slice(&buffer[..count]);
                written += count;
                if written == self.limit {
                    break;
                }
            }
            Ok(written)
        }
    }

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
    fn accepts_binary_and_initial_text_upload_requests() {
        assert_eq!(upload_filename("rb c0"), Some("c0"));
        assert_eq!(upload_filename("r 0 c1"), Some("c1"));
        assert_eq!(upload_filename("r 42 c1"), None);
        assert_eq!(upload_filename("wb output"), None);
    }

    #[test]
    fn batches_fragment_headers_and_payload() {
        let data = vec![b'x'; BLOCKSIZE + 1];
        let mut output = Vec::new();
        write_fragment(&mut output, &data, true).unwrap();

        assert_eq!(&output[..2], &((BLOCKSIZE as u16) << 1).to_le_bytes());
        let second_header = 2 + BLOCKSIZE;
        assert_eq!(
            &output[second_header..second_header + 2],
            &3u16.to_le_bytes()
        );
        assert_eq!(output.len(), data.len() + 4);
    }

    #[test]
    fn scatter_framing_handles_partial_vectored_writes() {
        let data = vec![b'x'; 2 * BLOCKSIZE + 17];
        let mut output = PartialWriter {
            output: Vec::new(),
            limit: 37,
        };

        write_fragment(&mut output, &data, true).unwrap();

        assert_eq!(output.output, copied_fragment(&data, true));
    }

    proptest! {
        #[test]
        fn scatter_framing_matches_copied_framing(
            data in prop::collection::vec(any::<u8>(), 0..(4 * BLOCKSIZE + 10)),
            finish in any::<bool>(),
        ) {
            let mut output = Vec::new();
            write_fragment(&mut output, &data, finish).unwrap();
            prop_assert_eq!(output, copied_fragment(&data, finish));
        }
    }
}
