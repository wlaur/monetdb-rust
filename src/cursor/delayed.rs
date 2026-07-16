// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use core::fmt;
use std::{borrow::Cow, io::Write};

use crate::framing::{ServerSock, reading::MapiReader, writing::MapiBuf};

use super::CursorResult;

pub struct ExpectedResponse {
    pub description: Cow<'static, str>,
    pub ignore_server_error: bool,
}

pub struct DelayedCommands {
    pub buffer: MapiBuf,
    pub responses: Vec<ExpectedResponse>,
}

impl Default for DelayedCommands {
    fn default() -> Self {
        Self::new()
    }
}
impl DelayedCommands {
    pub fn new() -> Self {
        DelayedCommands {
            buffer: MapiBuf::default(),
            responses: Vec::default(),
        }
    }

    pub fn add(&mut self, descr: &'static str, cmd: impl fmt::Display) {
        self.add_inner(descr, cmd, false);
    }

    pub fn add_cleanup(&mut self, descr: &'static str, cmd: impl fmt::Display) {
        self.add_inner(descr, cmd, true);
    }

    fn add_inner(
        &mut self,
        descr: &'static str,
        cmd: impl fmt::Display,
        ignore_server_error: bool,
    ) {
        use fmt::Write;
        write!(self.buffer, "{}", cmd).unwrap();
        if !self.buffer.peek().ends_with(b"\n") {
            self.buffer.append("\n");
        }
        self.buffer.end();
        self.responses.push(ExpectedResponse {
            description: descr.into(),
            ignore_server_error,
        })
    }

    pub fn add_xcommand_cleanup(&mut self, command: &'static str, value: impl fmt::Display) {
        self.add_cleanup(command, format_args!("X{command} {value}"))
    }

    pub fn send_delayed(&mut self, mut conn: ServerSock) -> CursorResult<ServerSock> {
        let raw = self.buffer.reset();
        conn.write_all(raw)?;
        Ok(conn)
    }

    pub fn send_delayed_plus(
        &mut self,
        mut conn: ServerSock,
        extra: &[&[u8]],
    ) -> CursorResult<ServerSock> {
        conn = self.buffer.write_reset_plus(conn, extra)?;
        Ok(conn)
    }

    pub fn recv_delayed(
        &mut self,
        conn: ServerSock,
        buffer: &mut Vec<u8>,
        max_response_size: usize,
    ) -> CursorResult<ServerSock> {
        let res = self.recv_delayed_inner(conn, buffer, max_response_size);
        buffer.clear();
        res
    }

    pub fn recv_delayed_inner(
        &mut self,
        mut conn: ServerSock,
        buffer: &mut Vec<u8>,
        max_response_size: usize,
    ) -> CursorResult<ServerSock> {
        for resp in self.responses.drain(..) {
            buffer.clear();
            conn = MapiReader::to_limited(conn, buffer, max_response_size)?;
            if let Some(msg) = super::replies::server_error_message(buffer) {
                let description = &resp.description;
                if resp.ignore_server_error {
                    log::warn!("delayed {description}: {msg}");
                    continue;
                }
                return Err(super::CursorError::Server(format!(
                    "delayed {description}: {msg}"
                )));
            }
        }
        Ok(conn)
    }
}
