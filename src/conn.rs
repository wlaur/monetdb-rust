// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{self, AtomicBool, AtomicU8},
    },
};

use crate::{
    cursor::{Cursor, CursorError, CursorResult, delayed::DelayedCommands},
    framing::{
        ServerSock, ServerState, SocketControl, Timeouts,
        connecting::{ConnectResult, Endian, establish_connection},
        reading::MapiReader,
    },
    parms::Parameters,
};

/// A connection to MonetDB.
///
/// The [top-level documentation](`super#examples`) contains some examples of how a
/// connection can be created.
///
/// Executing queries on a connection is done with a [`Cursor`] object, which
/// can be obtained using the [`cursor()`](`Connection::cursor`) method.
pub struct Connection(Arc<Conn>);

/// Thread-safe handle that can interrupt the connection's active operation.
#[derive(Clone)]
pub struct CancelHandle(Arc<Conn>);

pub(crate) struct Conn {
    pub(crate) reply_size: usize,
    pub(crate) max_response_size: usize,
    locked: Mutex<Locked>,
    pending_closes: Mutex<Vec<u64>>,
    closing: AtomicBool,
    operation_state: AtomicU8,
    control: Arc<SocketControl>,
    pub(crate) timeouts: Timeouts,
}

struct Locked {
    state: ServerState,
    sock: Option<ServerSock>,
    delayed: DelayedCommands,
}

const OPERATION_IDLE: u8 = 0;
const OPERATION_ACTIVE: u8 = 1;
const OPERATION_CANCELLED: u8 = 2;

impl Connection {
    /// Create a new connection based on the given [`Parameters`] object.
    pub fn new(parameters: Parameters) -> ConnectResult<Connection> {
        let (sock, state, delayed, timeouts) = establish_connection(parameters)?;

        let reply_size = state.reply_size;
        let max_response_size = state.max_response_size;

        let locked = Locked {
            state,
            sock: Some(sock),
            delayed,
        };
        let control = locked.sock.as_ref().expect("socket is present").control();
        let conn = Conn {
            locked: Mutex::new(locked),
            pending_closes: Mutex::new(Vec::new()),
            closing: AtomicBool::new(false),
            operation_state: AtomicU8::new(OPERATION_IDLE),
            control,
            timeouts,
            reply_size,
            max_response_size,
        };
        let connection = Connection(Arc::new(conn));

        Ok(connection)
    }

    /// Create a new connection based on the given URL.
    pub fn connect_url(url: &str) -> ConnectResult<Connection> {
        let parms = Parameters::from_url(url)?;
        Self::new(parms)
    }

    /// Create a new [`Cursor`] for this connection
    pub fn cursor(&self) -> Cursor {
        Cursor::new(Arc::clone(&self.0))
    }

    /// Close the connection.
    ///
    /// Any remaining cursors will not be able to fetch new data.
    /// They may still be able to return some already retrieved data but
    /// you shouldn't count on that.
    pub fn close(self) {
        drop(self);
    }

    fn close_connection(&mut self) {
        let conn = self.0.as_ref();
        conn.closing.store(true, atomic::Ordering::SeqCst);
        let _ = conn.control.shutdown();
        match conn.locked.try_lock() {
            Ok(mut locked) => locked.sock = None,
            Err(TryLockError::Poisoned(mut poisoned)) => poisoned.get_mut().sock = None,
            Err(TryLockError::WouldBlock) => {}
        }
    }

    /// Interrupt the operation currently using this connection.
    ///
    /// Cancellation closes the transport because MAPI has no safe
    /// out-of-band interrupt that preserves a partially read frame.
    pub fn cancel(&self) -> CursorResult<()> {
        self.0.cancel()
    }

    /// Return a handle that can cancel an operation without acquiring the
    /// connection's operation mutex.
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(Arc::clone(&self.0))
    }

    /// Return the default idle and absolute operation timeouts.
    pub fn timeouts(&self) -> Timeouts {
        self.0.timeouts
    }

    /// Return server environment and version metadata, loading it on first use.
    pub fn metadata(&self) -> CursorResult<ServerMetadata> {
        self.metadata_with_timeouts(self.0.timeouts)
    }

    /// Return server metadata using the supplied timeouts while loading it.
    pub fn metadata_with_timeouts(&self, timeouts: Timeouts) -> CursorResult<ServerMetadata> {
        let mut inner = None;
        self.0
            .run_locked_with_timeouts(timeouts, |state, _delayed, sock| {
                inner = state.sql_metadata.clone();
                Ok(sock)
            })?;
        if let Some(md) = inner {
            return Ok(ServerMetadata(md));
        }

        // create it and put it in the state
        // (ignore harmless race condition)
        let new_metadata = ServerMetadata::new(self, timeouts)?;
        self.0
            .run_locked_with_timeouts(timeouts, |state, _delayed, sock| {
                state.sql_metadata = Some(Arc::clone(&new_metadata.0));
                Ok(sock)
            })?;
        Ok(new_metadata)
    }

    /// Return protocol capabilities advertised by the server during login.
    pub fn server_info(&self) -> CursorResult<ServerInfo> {
        let mut info = None;
        self.0.run_locked(|state, _delayed, sock| {
            info = Some(ServerInfo {
                endian: state.server_endian,
                binary_level: state.binary_level,
                autocommit: state.autocommit,
                reply_size: state.reply_size,
                time_zone_seconds: state.time_zone_seconds,
            });
            Ok(sock)
        })?;
        info.ok_or(CursorError::Closed)
    }

    /// Enable or disable server-side autocommit for this connection.
    pub fn set_autocommit(&self, enabled: bool) -> CursorResult<()> {
        self.set_autocommit_with_timeouts(enabled, self.0.timeouts)
    }

    /// Enable or disable server-side autocommit using the supplied timeouts.
    pub fn set_autocommit_with_timeouts(
        &self,
        enabled: bool,
        timeouts: Timeouts,
    ) -> CursorResult<()> {
        let mut response_error = None;
        self.0
            .run_locked_with_timeouts(timeouts, |state, delayed, mut sock| {
                let mut response = Vec::new();
                sock = delayed.send_delayed_plus(
                    sock,
                    &[format!("Xauto_commit {}", i32::from(enabled)).as_bytes()],
                )?;
                sock = delayed.recv_delayed(sock, &mut response, self.0.max_response_size)?;
                response.clear();
                sock = MapiReader::to_limited(sock, &mut response, self.0.max_response_size)?;
                let expected = if enabled { b"&4 t" } else { b"&4 f" };
                if !response.is_empty() && !response.starts_with(expected) {
                    if let Some(message) = crate::cursor::replies::server_error_message(&response) {
                        response_error = Some(CursorError::Server(message));
                    } else {
                        response_error = Some(CursorError::BadReply(
                            crate::cursor::replies::BadReply::UnexpectedHeader(response.into()),
                        ));
                    }
                    return Ok(sock);
                }
                state.autocommit = enabled;
                Ok(sock)
            })?;
        match response_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Queue a prepared statement for deallocation without waiting for network I/O.
    ///
    /// Returns `false` when another operation currently owns the connection; the
    /// server will reclaim the statement when the connection closes in that case.
    pub fn try_deallocate(&self, statement_id: u64) -> bool {
        self.0.try_queue_deallocate(statement_id)
    }
}

/// Protocol capabilities negotiated for a live connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerInfo {
    /// Byte order advertised by the server during the handshake.
    pub endian: Endian,
    /// Highest binary result-set protocol level supported by both peers.
    pub binary_level: u16,
    /// Current server-side autocommit state cached from protocol replies.
    pub autocommit: bool,
    /// Session-level text result window size.
    pub reply_size: usize,
    /// Session time-zone offset east of UTC, in seconds.
    pub time_zone_seconds: i32,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.close_connection();
    }
}

impl CancelHandle {
    /// Interrupt the operation currently using the connection.
    pub fn cancel(&self) -> CursorResult<()> {
        self.0.cancel()
    }
}

impl Conn {
    pub(crate) fn run_locked<F>(&self, f: F) -> CursorResult<()>
    where
        F: for<'x> FnOnce(
            &'x mut ServerState,
            &'x mut DelayedCommands,
            ServerSock,
        ) -> CursorResult<ServerSock>,
    {
        self.run_locked_with_timeouts(self.timeouts, f)
    }

    pub(crate) fn run_locked_with_timeouts<F>(&self, timeouts: Timeouts, f: F) -> CursorResult<()>
    where
        F: for<'x> FnOnce(
            &'x mut ServerState,
            &'x mut DelayedCommands,
            ServerSock,
        ) -> CursorResult<ServerSock>,
    {
        let mut guard = match self.locked.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.closing.store(true, atomic::Ordering::Release);
                let _ = self.control.shutdown();
                return Err(CursorError::Poisoned);
            }
        };
        if self.closing.load(atomic::Ordering::Acquire) {
            guard.sock = None;
            let _ = self.control.shutdown();
            return Err(CursorError::Closed);
        }
        let Some(sock) = guard.sock.take() else {
            return Err(CursorError::Closed);
        };
        if self
            .operation_state
            .compare_exchange(
                OPERATION_IDLE,
                OPERATION_ACTIVE,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            )
            .is_err()
        {
            self.closing.store(true, atomic::Ordering::Release);
            let _ = self.control.shutdown();
            return Err(CursorError::Poisoned);
        }
        sock.start_operation(timeouts);
        let pending_closes = match self.pending_closes.lock() {
            Ok(mut pending) => std::mem::take(&mut *pending),
            Err(poisoned) => {
                let mut pending = poisoned.into_inner();
                std::mem::take(&mut *pending)
            }
        };
        let Locked { state, delayed, .. } = &mut *guard;
        for result_id in pending_closes {
            delayed.add_xcommand_cleanup("close", result_id);
        }
        let result = f(state, delayed, sock);
        let operation_state = self
            .operation_state
            .swap(OPERATION_IDLE, atomic::Ordering::AcqRel);
        if operation_state == OPERATION_CANCELLED {
            self.closing.store(true, atomic::Ordering::Release);
            return Err(CursorError::Cancelled);
        }
        match result {
            Ok(sock) => {
                if self.closing.load(atomic::Ordering::Acquire) {
                    drop(sock);
                    return Err(CursorError::Closed);
                }
                guard.sock = Some(sock);
                Ok(())
            }
            Err(CursorError::IO(error)) if error.kind() == std::io::ErrorKind::TimedOut => {
                self.closing.store(true, atomic::Ordering::Release);
                let _ = self.control.shutdown();
                Err(CursorError::Timeout)
            }
            Err(error) => {
                self.closing.store(true, atomic::Ordering::Release);
                let _ = self.control.shutdown();
                Err(error)
            }
        }
    }

    pub(crate) fn cancel(&self) -> CursorResult<()> {
        self.operation_state
            .compare_exchange(
                OPERATION_ACTIVE,
                OPERATION_CANCELLED,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            )
            .map_err(|_| CursorError::NoActiveOperation)?;
        let _ = self.control.shutdown();
        Ok(())
    }

    pub(crate) fn try_queue_closes(&self, result_ids: &[u64]) {
        let mut guard = match self.locked.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                let mut pending = match self.pending_closes.lock() {
                    Ok(pending) => pending,
                    Err(poisoned) => poisoned.into_inner(),
                };
                pending.extend_from_slice(result_ids);
                return;
            }
        };
        for result_id in result_ids {
            guard.delayed.add_xcommand_cleanup("close", result_id);
        }
    }

    fn try_queue_deallocate(&self, statement_id: u64) -> bool {
        let mut guard = match self.locked.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return false,
        };
        if guard.sock.is_none() {
            return false;
        }
        guard
            .delayed
            .add_cleanup("deallocate", format_args!("sDEALLOCATE {statement_id}\n;"));
        true
    }
}

#[derive(Debug, Clone)]
pub struct ServerMetadata(Arc<InnerServerMetadata>);

#[derive(Debug, Clone)]
pub struct InnerServerMetadata {
    environment: HashMap<String, String>,
    version: (u16, u16, u16),
    prehash_algo: &'static str,
}

impl ServerMetadata {
    fn new(conn: &Connection, timeouts: Timeouts) -> CursorResult<Self> {
        let mut cursor = conn.cursor();
        cursor.set_timeouts(timeouts);
        cursor.set_reply_size(NonZeroUsize::new(1024).unwrap());
        cursor.execute("SELECT name, value FROM sys.environment")?;
        let mut environment = HashMap::new();
        while cursor.next_row()? {
            let name = cursor
                .get_str(0)?
                .ok_or(CursorError::Metadata("sys.environment.name is null"))?;
            let value = cursor.get_str(1)?.unwrap_or("");
            environment.insert(name.to_string(), value.to_string());
        }

        // parse version
        let Some(v) = environment.get("monet_version") else {
            return Err(CursorError::Metadata(
                "'monet_version' not found in environment",
            ));
        };
        let mut parts = v.split('.');
        let mut next_part = || -> CursorResult<u16> {
            let Some(s) = parts.next() else {
                return Err(CursorError::Metadata(
                    "'monet_version' does not have 3 components",
                ));
            };
            s.parse()
                .map_err(|_| CursorError::Metadata("invalid int component in 'monet_version'"))
        };
        let major = next_part()?;
        let minor = next_part()?;
        let patch = next_part()?;
        if parts.next().is_some() {
            return Err(CursorError::Metadata(
                "'monet_version' has more than 3 components",
            ));
        }
        let version = (major, minor, patch);

        let mut prehash_algo: &'static str = "";
        conn.0.run_locked(|state, _delayed, sock| {
            prehash_algo = state.prehash_algo;
            Ok(sock)
        })?;

        let inner = InnerServerMetadata {
            environment,
            version,
            prehash_algo,
        };
        let metadata = ServerMetadata(Arc::new(inner));
        Ok(metadata)
    }

    pub fn env(&self, key: &str) -> Option<&str> {
        self.0.environment.get(key).map(String::as_ref)
    }

    pub fn version(&self) -> (u16, u16, u16) {
        self.0.version
    }

    pub fn password_prehash_algo(&self) -> &str {
        self.0.prehash_algo
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Read, Write},
        net::{TcpListener, TcpStream},
        sync::mpsc,
        thread,
        time::Duration,
    };

    use super::*;

    fn write_message(stream: &mut TcpStream, message: &[u8]) {
        let header = (((message.len() as u16) << 1) | 1).to_le_bytes();
        stream.write_all(&header).unwrap();
        stream.write_all(message).unwrap();
    }

    fn read_message(stream: &mut TcpStream) -> Vec<u8> {
        let mut message = Vec::new();
        loop {
            let mut header = [0; 2];
            stream.read_exact(&mut header).unwrap();
            let header = u16::from_le_bytes(header);
            let length = usize::from(header >> 1);
            let start = message.len();
            message.resize(start + length, 0);
            stream.read_exact(&mut message[start..]).unwrap();
            if header & 1 != 0 {
                return message;
            }
        }
    }

    fn black_hole_query_server() -> (u16, mpsc::Receiver<()>, mpsc::Receiver<io::Result<usize>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (query_sender, query_receiver) = mpsc::sync_channel(1);
        let (disconnect_sender, disconnect_receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut nuls = [0; 8];
            stream.read_exact(&mut nuls).unwrap();
            assert_eq!(nuls, [0; 8]);
            write_message(
                &mut stream,
                b"salt:mserver:9:SHA512:LIT:SHA512:sql=9:BINARY=1:",
            );
            let _response = read_message(&mut stream);
            write_message(&mut stream, b"=OK");
            let _query = read_message(&mut stream);
            query_sender.send(()).unwrap();
            let mut byte = [0];
            disconnect_sender.send(stream.read(&mut byte)).unwrap();
        });
        (port, query_receiver, disconnect_receiver)
    }

    #[test]
    fn cancellation_interrupts_a_locked_network_read_and_closes_the_connection() {
        let (port, query_received, disconnected) = black_hole_query_server();
        let mut parameters = Parameters::default();
        parameters.set_host("127.0.0.1").unwrap();
        parameters.set_port(port).unwrap();
        parameters.set_client_info("false").unwrap();
        parameters.set_connect_timeout(2).unwrap();
        parameters.set_operation_timeout(5).unwrap();
        let connection = Connection::new(parameters).unwrap();
        let cancel = connection.cancel_handle();
        assert_eq!(cancel.cancel(), Err(CursorError::NoActiveOperation));

        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let mut cursor = connection.cursor();
            let result = cursor.execute("SELECT 1");
            let after_cancel = connection.cursor().execute("SELECT 2");
            result_sender.send((result, after_cancel)).unwrap();
        });
        query_received
            .recv_timeout(Duration::from_secs(2))
            .expect("query did not reach the server");
        cancel.cancel().unwrap();
        let (result, after_cancel) = result_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("cancelled query did not return");
        assert_eq!(result, Err(CursorError::Cancelled));
        assert_eq!(after_cancel, Err(CursorError::Closed));
        assert_eq!(
            disconnected
                .recv_timeout(Duration::from_secs(2))
                .expect("cancelled connection remained open")
                .unwrap(),
            0
        );
        worker.join().unwrap();
    }

    #[test]
    fn idle_read_timeout_closes_a_black_holed_connection() {
        let (port, query_received, disconnected) = black_hole_query_server();
        let mut parameters = Parameters::default();
        parameters.set_host("127.0.0.1").unwrap();
        parameters.set_port(port).unwrap();
        parameters.set_client_info("false").unwrap();
        parameters.set_connect_timeout(2).unwrap();
        parameters.set_read_timeout(1).unwrap();
        parameters.set_operation_timeout(0).unwrap();
        let connection = Connection::new(parameters).unwrap();

        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let worker = thread::spawn(move || {
            let mut cursor = connection.cursor();
            let result = cursor.execute("SELECT 1");
            let after_timeout = connection.cursor().execute("SELECT 2");
            result_sender.send((result, after_timeout)).unwrap();
            release_receiver.recv().unwrap();
        });
        query_received
            .recv_timeout(Duration::from_secs(2))
            .expect("query did not reach the server");
        let (result, after_timeout) = result_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("timed out query did not return");
        assert_eq!(result, Err(CursorError::Timeout));
        assert_eq!(after_timeout, Err(CursorError::Closed));
        assert_eq!(
            disconnected
                .recv_timeout(Duration::from_secs(2))
                .expect("timed out connection remained open")
                .unwrap(),
            0
        );
        release_sender.send(()).unwrap();
        worker.join().unwrap();
    }
}
