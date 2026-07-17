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
        Arc, Condvar, Mutex, TryLockError, Weak,
        atomic::{self, AtomicBool, AtomicU8},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
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
    operation_watchdog: OperationWatchdog,
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

struct OperationWatchdog {
    shared: Arc<WatchdogShared>,
    control: Weak<SocketControl>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct WatchdogShared {
    state: Mutex<WatchdogState>,
    wake: Condvar,
}

#[derive(Default)]
struct WatchdogState {
    active: Option<(u64, Instant)>,
    fired_token: u64,
    next_token: u64,
    stopped: bool,
}

impl OperationWatchdog {
    fn new(control: &Arc<SocketControl>) -> Self {
        let shared = Arc::new(WatchdogShared {
            state: Mutex::new(WatchdogState::default()),
            wake: Condvar::new(),
        });
        Self {
            shared,
            control: Arc::downgrade(control),
            worker: Mutex::new(None),
        }
    }

    fn ensure_worker(&self) {
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker.is_none() {
            let shared = Arc::clone(&self.shared);
            let control = self.control.clone();
            *worker = Some(thread::spawn(move || Self::run(shared, control)));
        }
    }

    fn run(shared: Arc<WatchdogShared>, control: Weak<SocketControl>) {
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if state.stopped {
                return;
            }
            let Some((token, deadline)) = state.active else {
                state = shared
                    .wake
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            };
            let now = Instant::now();
            if now < deadline {
                state = match shared.wake.wait_timeout(state, deadline - now) {
                    Ok((state, _)) => state,
                    Err(poisoned) => poisoned.into_inner().0,
                };
                continue;
            }
            if state.active.is_some_and(|(active, _)| active == token) {
                state.active = None;
                state.fired_token = token;
                drop(state);
                if let Some(control) = control.upgrade() {
                    let _ = control.shutdown();
                }
                state = shared
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    fn arm(&self, timeout: Option<Duration>) -> Option<u64> {
        let timeout = timeout?;
        self.ensure_worker();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next_token = state.next_token.wrapping_add(1);
        if state.next_token == 0 {
            state.next_token = 1;
        }
        let token = state.next_token;
        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("portable operation timeout fits Instant");
        state.active = Some((token, deadline));
        state.fired_token = 0;
        self.shared.wake.notify_one();
        Some(token)
    }

    fn disarm(&self, token: Option<u64>) -> bool {
        let Some(token) = token else {
            return false;
        };
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active.is_some_and(|(active, _)| active == token) {
            state.active = None;
            self.shared.wake.notify_one();
            false
        } else {
            state.fired_token == token
        }
    }
}

impl Drop for OperationWatchdog {
    fn drop(&mut self) {
        {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stopped = true;
            self.shared.wake.notify_one();
        }
        let worker = self
            .worker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }
}

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
        let operation_watchdog = OperationWatchdog::new(&control);
        let conn = Conn {
            locked: Mutex::new(locked),
            pending_closes: Mutex::new(Vec::new()),
            closing: AtomicBool::new(false),
            operation_state: AtomicU8::new(OPERATION_IDLE),
            control,
            operation_watchdog,
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
                if !response.is_empty() && response.trim_ascii() != expected {
                    if let Some(error) = crate::cursor::replies::server_error(&response) {
                        response_error = Some(CursorError::Server(error));
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
        let timeouts = timeouts.bounded();
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
        let watchdog_token = self.operation_watchdog.arm(timeouts.operation);
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
        let operation_timed_out = self.operation_watchdog.disarm(watchdog_token);
        let operation_state = self
            .operation_state
            .swap(OPERATION_IDLE, atomic::Ordering::AcqRel);
        if operation_state == OPERATION_CANCELLED {
            self.closing.store(true, atomic::Ordering::Release);
            return Err(CursorError::Cancelled);
        }
        if operation_timed_out {
            self.closing.store(true, atomic::Ordering::Release);
            let _ = self.control.shutdown();
            return Err(CursorError::Timeout);
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
        collections::HashMap,
        io::{self, Read, Write},
        net::{TcpListener, TcpStream},
        num::NonZeroUsize,
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

    fn accept_login(listener: TcpListener) -> TcpStream {
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
        stream
    }

    fn test_parameters(port: u16) -> Parameters {
        let mut parameters = Parameters::default();
        parameters.set_host("127.0.0.1").unwrap();
        parameters.set_port(port).unwrap();
        parameters.set_client_info("false").unwrap();
        parameters.set_connect_timeout(2).unwrap();
        parameters.set_operation_timeout(0).unwrap();
        parameters
    }

    fn black_hole_query_server() -> (u16, mpsc::Receiver<()>, mpsc::Receiver<io::Result<usize>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (query_sender, query_receiver) = mpsc::sync_channel(1);
        let (disconnect_sender, disconnect_receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let mut stream = accept_login(listener);
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
        let mut parameters = test_parameters(port);
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
    fn operation_watchdog_starts_only_when_armed() {
        let (port, _, _) = black_hole_query_server();
        let connection = Connection::new(test_parameters(port)).unwrap();
        assert!(
            connection
                .0
                .operation_watchdog
                .worker
                .lock()
                .unwrap()
                .is_none()
        );

        let token = connection
            .0
            .operation_watchdog
            .arm(Some(Duration::from_secs(1)));
        assert!(token.is_some());
        assert!(
            connection
                .0
                .operation_watchdog
                .worker
                .lock()
                .unwrap()
                .is_some()
        );
        assert!(!connection.0.operation_watchdog.disarm(token));
    }

    #[test]
    fn idle_read_timeout_closes_a_black_holed_connection() {
        let (port, query_received, disconnected) = black_hole_query_server();
        let mut parameters = test_parameters(port);
        parameters.set_read_timeout(1).unwrap();
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
            .recv_timeout(Duration::from_secs(8))
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

    #[test]
    fn binary_fetch_timeout_closes_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (fetch_sender, fetch_receiver) = mpsc::sync_channel(1);
        let (disconnect_sender, disconnect_receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let mut stream = accept_login(listener);
            let _query = read_message(&mut stream);
            write_message(
                &mut stream,
                concat!(
                    "&1 42 2 1 1\n",
                    "% t # table_name\n",
                    "% value # name\n",
                    "% int # type\n",
                    "% 32 # length\n",
                    "% 0 0 # typesizes\n",
                    "[ 1\t]\n"
                )
                .as_bytes(),
            );
            let export = read_message(&mut stream);
            assert!(export.starts_with(b"Xexportbin 42 1 1"));
            fetch_sender.send(()).unwrap();
            let mut byte = [0];
            disconnect_sender.send(stream.read(&mut byte)).unwrap();
        });

        let mut parameters = test_parameters(port);
        parameters.set_read_timeout(1).unwrap();
        let connection = Connection::new(parameters).unwrap();
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let worker = thread::spawn(move || {
            let mut cursor = connection.cursor();
            cursor.execute("SELECT value").unwrap();
            let result = cursor.fetch_binary(1, 1);
            let after_timeout = connection.cursor().execute("SELECT 2");
            result_sender.send((result, after_timeout)).unwrap();
            release_receiver.recv().unwrap();
        });
        fetch_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("binary fetch did not reach the server");
        let (result, after_timeout) = result_receiver
            .recv_timeout(Duration::from_secs(8))
            .expect("binary fetch did not time out");
        assert_eq!(result, Err(CursorError::Timeout));
        assert_eq!(after_timeout, Err(CursorError::Closed));
        assert_eq!(
            disconnect_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("timed out binary fetch remained open")
                .unwrap(),
            0
        );
        release_sender.send(()).unwrap();
        worker.join().unwrap();
    }

    fn upload_server(
        read_upload: bool,
    ) -> (
        u16,
        mpsc::Receiver<()>,
        mpsc::SyncSender<()>,
        mpsc::Receiver<io::Result<usize>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (upload_sender, upload_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let (disconnect_sender, disconnect_receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let mut stream = accept_login(listener);
            let _query = read_message(&mut stream);
            write_message(&mut stream, b"\x01\x03\nrb c0\n");
            if read_upload {
                let _upload = read_message(&mut stream);
                upload_sender.send(()).unwrap();
            } else {
                upload_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
            }
            let mut buffer = [0; 64 * 1024];
            let result = loop {
                match stream.read(&mut buffer) {
                    Ok(0) => break Ok(0),
                    Ok(_) => continue,
                    Err(error) => break Err(error),
                }
            };
            disconnect_sender.send(result).unwrap();
        });
        (port, upload_receiver, release_sender, disconnect_receiver)
    }

    #[test]
    fn upload_prompt_timeout_closes_the_connection() {
        let (port, upload_received, _server_release, disconnected) = upload_server(true);
        let mut parameters = test_parameters(port);
        parameters.set_read_timeout(1).unwrap();
        parameters.set_write_timeout(5).unwrap();
        let connection = Connection::new(parameters).unwrap();
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let worker = thread::spawn(move || {
            let mut cursor = connection.cursor();
            let uploads = HashMap::from([("c0".to_string(), vec![1])]);
            let result = cursor.execute_with_binary_uploads("COPY BINARY", &uploads);
            let after_timeout = connection.cursor().execute("SELECT 2");
            result_sender.send((result, after_timeout)).unwrap();
            release_receiver.recv().unwrap();
        });
        upload_received
            .recv_timeout(Duration::from_secs(2))
            .expect("upload did not reach the server");
        let (result, after_timeout) = result_receiver
            .recv_timeout(Duration::from_secs(8))
            .expect("upload prompt did not time out");
        assert_eq!(result, Err(CursorError::Timeout));
        assert_eq!(after_timeout, Err(CursorError::Closed));
        assert_eq!(
            disconnected
                .recv_timeout(Duration::from_secs(2))
                .expect("timed out upload prompt remained open")
                .unwrap(),
            0
        );
        release_sender.send(()).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn upload_body_timeout_closes_the_connection() {
        let (port, request_sent, server_release, disconnected) = upload_server(false);
        let mut parameters = test_parameters(port);
        parameters.set_read_timeout(30).unwrap();
        parameters.set_write_timeout(1).unwrap();
        parameters.set_operation_timeout(2).unwrap();
        let connection = Connection::new(parameters).unwrap();
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let worker = thread::spawn(move || {
            let mut cursor = connection.cursor();
            let uploads = HashMap::from([("c0".to_string(), vec![1; 64 * 1024 * 1024])]);
            let result = cursor.execute_with_binary_uploads_with_chunk_size(
                "COPY BINARY",
                &uploads,
                NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
            );
            let after_timeout = connection.cursor().execute("SELECT 2");
            result_sender.send((result, after_timeout)).unwrap();
            release_receiver.recv().unwrap();
        });
        request_sent
            .recv_timeout(Duration::from_secs(2))
            .expect("upload request did not reach the client");
        let (result, after_timeout) = result_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("upload body did not time out");
        assert_eq!(result, Err(CursorError::Timeout));
        assert_eq!(after_timeout, Err(CursorError::Closed));
        server_release.send(()).unwrap();
        assert_eq!(
            disconnected
                .recv_timeout(Duration::from_secs(2))
                .expect("timed out upload body remained open")
                .unwrap(),
            0
        );
        release_sender.send(()).unwrap();
        worker.join().unwrap();
    }
}
