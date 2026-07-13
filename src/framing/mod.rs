// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

pub mod blockstate;
pub mod connecting;
pub mod reading;
pub mod tls;
pub mod writing;

use std::{error, fmt, io, net::TcpStream, sync::Arc};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::{conn::InnerServerMetadata, framing::connecting::Endian};

pub const BLOCKSIZE: usize = 8190;
const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FramingError {
    InvalidBlockSize,
    Unicode,
    TooLong,
}

impl FramingError {
    fn to_str(&self) -> &'static str {
        match self {
            FramingError::InvalidBlockSize => {
                "network layer: invalid block; network byte stream out of sync?"
            }
            FramingError::Unicode => {
                "network layer: invalid utf-8 encoding, block was expected to contain text"
            }
            FramingError::TooLong => "network layer: message too long",
        }
    }
}

impl fmt::Display for FramingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_str().fmt(f)
    }
}

pub type FramingResult<T> = Result<T, FramingError>;

impl From<FramingError> for io::Error {
    fn from(value: FramingError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, value.to_str())
    }
}

impl error::Error for FramingError {}

#[derive(Debug, Clone)]
pub struct ServerState {
    pub autocommit: bool,
    pub reply_size: usize,
    pub time_zone_seconds: i32,
    pub sql_metadata: Option<Arc<InnerServerMetadata>>,
    pub prehash_algo: &'static str,
    pub server_endian: Endian,
    pub binary_level: u16,
    pub max_response_size: usize,
}

impl ServerState {
    fn new(
        prehash_algo: &'static str,
        server_endian: Endian,
        binary_level: u16,
        max_response_size: usize,
    ) -> Self {
        Self {
            autocommit: true,
            reply_size: 100,
            time_zone_seconds: 0,
            sql_metadata: None,
            prehash_algo,
            server_endian,
            binary_level,
            max_response_size,
        }
    }
}

trait ServerSockTrait: fmt::Debug + io::Read + io::Write + Send + 'static {}

#[cfg(unix)]
impl ServerSockTrait for UnixStream {}

impl ServerSockTrait for TcpStream {}

#[derive(Debug)]
pub struct ServerSock {
    inner: Box<dyn ServerSockTrait>,
    read_buffer: Vec<u8>,
    read_position: usize,
}

impl ServerSock {
    fn new(sock: impl ServerSockTrait) -> Self {
        ServerSock {
            inner: Box::new(sock),
            read_buffer: Vec::with_capacity(READ_BUFFER_SIZE),
            read_position: 0,
        }
    }
}

impl io::Read for ServerSock {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.read_position == self.read_buffer.len() {
            self.read_buffer.resize(READ_BUFFER_SIZE, 0);
            let read = match self.inner.read(&mut self.read_buffer) {
                Ok(read) => read,
                Err(error) => {
                    self.read_buffer.truncate(self.read_position);
                    return Err(error);
                }
            };
            self.read_buffer.truncate(read);
            self.read_position = 0;
        }
        let available = &self.read_buffer[self.read_position..];
        let count = available.len().min(buf.len());
        buf[..count].copy_from_slice(&available[..count]);
        self.read_position += count;
        Ok(count)
    }
}

impl io::Write for ServerSock {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.inner.write_vectored(bufs)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor, Read, Write},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::{ServerSock, ServerSockTrait, blockstate::Header, reading::MapiReader};

    #[derive(Debug)]
    struct CountingSock {
        data: Cursor<Vec<u8>>,
        reads: Arc<AtomicUsize>,
    }

    impl Read for CountingSock {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.data.read(buf)
        }
    }

    impl Write for CountingSock {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ServerSockTrait for CountingSock {}

    #[derive(Debug)]
    struct InterruptedOnceSock {
        data: Cursor<Vec<u8>>,
        interrupted: bool,
    }

    impl Read for InterruptedOnceSock {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(io::ErrorKind::Interrupted.into());
            }
            self.data.read(buf)
        }
    }

    impl Write for InterruptedOnceSock {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ServerSockTrait for InterruptedOnceSock {}

    #[test]
    fn read_ahead_preserves_the_next_message() {
        let mut data = Vec::new();
        data.extend_from_slice(Header::new(3, true).as_bytes());
        data.extend_from_slice(b"one");
        data.extend_from_slice(Header::new(3, true).as_bytes());
        data.extend_from_slice(b"two");
        let reads = Arc::new(AtomicUsize::new(0));
        let raw = CountingSock {
            data: Cursor::new(data),
            reads: Arc::clone(&reads),
        };

        let mut first = Vec::new();
        let sock = MapiReader::to_end(ServerSock::new(raw), &mut first).unwrap();
        let mut second = Vec::new();
        let _sock = MapiReader::to_end(sock, &mut second).unwrap();

        assert_eq!(first, b"one");
        assert_eq!(second, b"two");
        assert_eq!(reads.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn interrupted_read_does_not_expose_buffer_capacity_as_data() {
        let raw = InterruptedOnceSock {
            data: Cursor::new(b"actual wire data".to_vec()),
            interrupted: false,
        };
        let mut sock = ServerSock::new(raw);
        let mut output = Vec::new();

        sock.read_to_end(&mut output).unwrap();

        assert_eq!(output, b"actual wire data");
    }
}
