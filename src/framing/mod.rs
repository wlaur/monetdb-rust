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

use std::{
    error, fmt, io,
    net::{Shutdown, TcpStream},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::{conn::InnerServerMetadata, framing::connecting::Endian};

pub const BLOCKSIZE: usize = 8190;
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Largest whole-second timeout that remains finite on every supported socket API.
pub const MAX_TIMEOUT_SECONDS: i64 = (u32::MAX as i64 - 1) / 1000;
const MAX_TIMEOUT: Duration = Duration::from_secs(MAX_TIMEOUT_SECONDS as u64);

/// Idle socket timeouts and the absolute timeout for one post-login operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub read: Option<Duration>,
    pub write: Option<Duration>,
    pub operation: Option<Duration>,
}

impl Timeouts {
    pub(crate) fn from_validated(parms: &crate::parms::Validated<'_>) -> Self {
        Self {
            read: parms.read_timeout,
            write: parms.write_timeout,
            operation: parms.operation_timeout,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ActiveTimeouts {
    read: Option<Duration>,
    write: Option<Duration>,
    deadline: Option<Instant>,
}

impl ActiveTimeouts {
    fn new(timeouts: Timeouts) -> Self {
        let now = Instant::now();
        Self {
            read: timeouts.read,
            write: timeouts.write,
            deadline: timeouts
                .operation
                .and_then(|timeout| now.checked_add(timeout)),
        }
    }

    fn for_connection(timeouts: Timeouts, deadline: Option<Instant>) -> Self {
        Self {
            read: timeouts.read,
            write: timeouts.write,
            deadline,
        }
    }

    fn read_limit(self) -> io::Result<Option<Duration>> {
        self.limit(self.read)
    }

    fn write_limit(self) -> io::Result<Option<Duration>> {
        self.limit(self.write)
    }

    fn limit(self, idle: Option<Duration>) -> io::Result<Option<Duration>> {
        let remaining = match self.deadline {
            Some(deadline) => {
                let remaining =
                    deadline
                        .checked_duration_since(Instant::now())
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::TimedOut, "operation deadline expired")
                        })?;
                if remaining.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "operation deadline expired",
                    ));
                }
                Some(remaining)
            }
            None => None,
        };
        let limit = match (idle, remaining) {
            (Some(idle), Some(remaining)) => Some(idle.min(remaining)),
            (Some(idle), None) => Some(idle),
            (None, remaining) => remaining,
        };
        if limit.is_some_and(|limit| limit > MAX_TIMEOUT) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timeout exceeds the portable socket limit",
            ));
        }
        Ok(limit)
    }
}

#[derive(Debug)]
enum RawSocket {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
    #[cfg(test)]
    Test,
}

#[derive(Debug)]
pub(crate) struct SocketControl {
    raw: RawSocket,
    timeouts: Mutex<ActiveTimeouts>,
}

impl SocketControl {
    fn new(raw: RawSocket, active: ActiveTimeouts) -> Self {
        Self {
            raw,
            timeouts: Mutex::new(active),
        }
    }

    fn start_operation(&self, timeouts: Timeouts) {
        *self
            .timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = ActiveTimeouts::new(timeouts);
    }

    fn set_connection_deadline(&self, timeouts: Timeouts, deadline: Option<Instant>) {
        *self
            .timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            ActiveTimeouts::for_connection(timeouts, deadline);
    }

    fn read_limit(&self) -> io::Result<Option<Duration>> {
        self.timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read_limit()
    }

    fn write_limit(&self) -> io::Result<Option<Duration>> {
        self.timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .write_limit()
    }

    fn read_timeout_active(&self) -> bool {
        let timeouts = self
            .timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        timeouts.read.is_some() || timeouts.deadline.is_some()
    }

    fn write_timeout_active(&self) -> bool {
        let timeouts = self
            .timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        timeouts.write.is_some() || timeouts.deadline.is_some()
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        match &self.raw {
            RawSocket::Tcp(socket) => socket.shutdown(Shutdown::Both),
            #[cfg(unix)]
            RawSocket::Unix(socket) => socket.shutdown(Shutdown::Both),
            #[cfg(test)]
            RawSocket::Test => Ok(()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FramingError {
    InvalidBlockSize,
    InvalidHeaderLength(usize),
    MessageAlreadyComplete,
    Unicode,
    TooLong,
}

impl FramingError {
    fn to_str(&self) -> &'static str {
        match self {
            FramingError::InvalidBlockSize => {
                "network layer: invalid block; network byte stream out of sync?"
            }
            FramingError::InvalidHeaderLength(_) => "network layer: invalid block header length",
            FramingError::MessageAlreadyComplete => "network layer: message is already complete",
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

pub(crate) trait ServerSockTrait:
    fmt::Debug + io::Read + io::Write + Send + 'static
{
    fn set_socket_read_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }

    fn set_socket_write_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(unix)]
impl ServerSockTrait for UnixStream {
    fn set_socket_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        UnixStream::set_read_timeout(self, timeout)
    }

    fn set_socket_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        UnixStream::set_write_timeout(self, timeout)
    }
}

impl ServerSockTrait for TcpStream {
    fn set_socket_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn set_socket_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }
}

#[derive(Debug)]
pub struct ServerSock {
    inner: Box<dyn ServerSockTrait>,
    control: Arc<SocketControl>,
    read_buffer: Vec<u8>,
    read_position: usize,
}

impl ServerSock {
    #[cfg(test)]
    fn new(sock: impl ServerSockTrait) -> Self {
        let control = Arc::new(SocketControl::new(
            RawSocket::Test,
            ActiveTimeouts::new(Timeouts {
                read: None,
                write: None,
                operation: None,
            }),
        ));
        Self::with_control(sock, control)
    }

    fn with_control(sock: impl ServerSockTrait, control: Arc<SocketControl>) -> Self {
        ServerSock {
            inner: Box::new(sock),
            control,
            read_buffer: Vec::with_capacity(READ_BUFFER_SIZE),
            read_position: 0,
        }
    }

    pub(crate) fn from_tcp(
        sock: TcpStream,
        timeouts: Timeouts,
        deadline: Option<Instant>,
    ) -> io::Result<Self> {
        let control = Arc::new(SocketControl::new(
            RawSocket::Tcp(sock.try_clone()?),
            ActiveTimeouts::for_connection(timeouts, deadline),
        ));
        Ok(Self::with_control(sock, control))
    }

    #[cfg(unix)]
    pub(crate) fn from_unix(
        sock: UnixStream,
        timeouts: Timeouts,
        deadline: Option<Instant>,
    ) -> io::Result<Self> {
        let control = Arc::new(SocketControl::new(
            RawSocket::Unix(sock.try_clone()?),
            ActiveTimeouts::for_connection(timeouts, deadline),
        ));
        Ok(Self::with_control(sock, control))
    }

    #[cfg(feature = "rustls")]
    pub(crate) fn wrap(sock: impl ServerSockTrait, control: Arc<SocketControl>) -> Self {
        Self::with_control(sock, control)
    }

    pub(crate) fn control(&self) -> Arc<SocketControl> {
        Arc::clone(&self.control)
    }

    pub(crate) fn start_operation(&self, timeouts: Timeouts) {
        self.control.start_operation(timeouts);
    }

    pub(crate) fn set_connection_deadline(&self, timeouts: Timeouts, deadline: Option<Instant>) {
        self.control.set_connection_deadline(timeouts, deadline);
    }

    pub(crate) fn set_socket_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_socket_read_timeout(timeout)
    }

    pub(crate) fn set_socket_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_socket_write_timeout(timeout)
    }
}

impl io::Read for ServerSock {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.read_position == self.read_buffer.len() {
            let timeout = self.control.read_limit()?;
            self.inner.set_socket_read_timeout(timeout)?;
            self.read_buffer.resize(READ_BUFFER_SIZE, 0);
            let read = match self.inner.read(&mut self.read_buffer) {
                Ok(read) => read,
                Err(mut error) => {
                    self.read_buffer.truncate(self.read_position);
                    if error.kind() == io::ErrorKind::WouldBlock
                        && self.control.read_timeout_active()
                    {
                        error = io::Error::new(io::ErrorKind::TimedOut, error);
                    }
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
        let timeout = self.control.write_limit()?;
        self.inner.set_socket_write_timeout(timeout)?;
        self.inner
            .write(buf)
            .map_err(|error| normalize_timeout(error, self.control.write_timeout_active()))
    }

    fn flush(&mut self) -> io::Result<()> {
        let timeout = self.control.write_limit()?;
        self.inner.set_socket_write_timeout(timeout)?;
        self.inner
            .flush()
            .map_err(|error| normalize_timeout(error, self.control.write_timeout_active()))
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let timeout = self.control.write_limit()?;
        self.inner.set_socket_write_timeout(timeout)?;
        self.inner
            .write_vectored(bufs)
            .map_err(|error| normalize_timeout(error, self.control.write_timeout_active()))
    }
}

fn normalize_timeout(error: io::Error, timeout_active: bool) -> io::Error {
    if error.kind() == io::ErrorKind::WouldBlock && timeout_active {
        io::Error::new(io::ErrorKind::TimedOut, error)
    } else {
        error
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

    #[derive(Debug)]
    struct WouldBlockSock;

    impl Read for WouldBlockSock {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }
    }

    impl Write for WouldBlockSock {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::WouldBlock.into())
        }

        fn write_vectored(&mut self, _bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }
    }

    impl ServerSockTrait for WouldBlockSock {}

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

    #[test]
    fn all_writes_report_active_socket_timeouts_consistently() {
        let control = Arc::new(super::SocketControl::new(
            super::RawSocket::Test,
            super::ActiveTimeouts::new(super::Timeouts {
                read: None,
                write: Some(std::time::Duration::from_secs(1)),
                operation: None,
            }),
        ));
        let mut sock = ServerSock::with_control(WouldBlockSock, control);

        assert_eq!(
            sock.write(b"data").unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(sock.flush().unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            sock.write_vectored(&[io::IoSlice::new(b"data")])
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[test]
    fn active_timeouts_reject_values_that_windows_would_make_infinite() {
        let active = super::ActiveTimeouts::new(super::Timeouts {
            read: Some(std::time::Duration::from_secs(
                (super::MAX_TIMEOUT_SECONDS + 1) as u64,
            )),
            write: None,
            operation: None,
        });

        assert_eq!(
            active.read_limit().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
