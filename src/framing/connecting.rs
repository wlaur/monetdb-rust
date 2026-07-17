// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use core::{fmt, str};
use std::{
    borrow::Cow,
    env,
    ffi::OsStr,
    io::{self, ErrorKind, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::PathBuf,
    process,
    str::Utf8Error,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use gethostname;

use crate::{
    PUBLIC_NAME,
    cursor::{
        CursorError,
        delayed::{DelayedCommands, ExpectedResponse},
    },
    framing::{reading::MapiReader, writing::MapiBuf},
    parms::{Parameters, ParmError, Validated},
    util::{hash_algorithms, ioerror::IoError},
};

use super::{ServerSock, ServerState, Timeouts};

/// An error that occurs while trying to connect to MonetDB.
#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum ConnectError {
    #[error(transparent)]
    Parm(#[from] ParmError),
    #[error(transparent)]
    IO(#[from] IoError),
    #[error("connection deadline expired")]
    Timeout,
    #[error("invalid utf-8 sequence")]
    Utf(#[from] Utf8Error),
    #[error("{0} in server challenge")]
    InvalidChallenge(String),
    #[error("server requested unsupported hash algorithm: {0}")]
    UnsupportedHashAlgo(String),
    #[error("TLS (monetdbs://) has not been enabled")]
    TlsNotSupported,
    #[error("TLS connection cannot follow a plaintext redirect")]
    TlsDowngrade,
    #[error("TLS error: {0}")]
    TlsError(String),
    #[error("only language=sql is supported")]
    OnlySqlSupported,
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("login rejected: {0}")]
    Rejected(String),
    #[error("unexpected server response: {0:?}")]
    UnexpectedResponse(String),
    #[error("Unix domain sockets are not supported on this platform")]
    UnixDomain,
}

pub type ConnectResult<T> = Result<T, ConnectError>;

impl From<io::Error> for ConnectError {
    fn from(value: io::Error) -> Self {
        if value.kind() == io::ErrorKind::TimedOut {
            ConnectError::Timeout
        } else {
            IoError::from(value).into()
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ConnectionDeadline(Option<Instant>);

impl ConnectionDeadline {
    fn new(timeout: Option<Duration>) -> Self {
        let now = Instant::now();
        Self(timeout.and_then(|timeout| now.checked_add(timeout)))
    }

    fn instant(self) -> Option<Instant> {
        self.0
    }

    fn remaining(self) -> ConnectResult<Option<Duration>> {
        self.0
            .map(|deadline| {
                deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(ConnectError::Timeout)
            })
            .transpose()
    }
}

fn run_io_with_deadline<T, F>(
    deadline: ConnectionDeadline,
    thread_name: &str,
    operation: F,
) -> ConnectResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let Some(remaining) = deadline.remaining()? else {
        return operation().map_err(Into::into);
    };
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || {
            let _ = sender.send(operation());
        })?;
    match receiver.recv_timeout(remaining) {
        Ok(result) => result.map_err(Into::into),
        Err(mpsc::RecvTimeoutError::Timeout) => Err(ConnectError::Timeout),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err(io::Error::other(format!("{thread_name} worker stopped without a result")).into())
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum Endian {
    Big,
    Lit,
}

impl Endian {
    #[cfg(target_endian = "little")]
    pub const NATIVE: Endian = Endian::Lit;

    #[cfg(target_endian = "big")]
    pub const NATIVE: Endian = Endian::Big;
}

impl fmt::Display for Endian {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Endian::Big => "BIG",
            Endian::Lit => "LIT",
        };
        s.fmt(f)
    }
}

#[cfg(not(unix))]
fn connect_unix_socket(
    _parms: &Validated,
    _deadline: ConnectionDeadline,
) -> ConnectResult<ServerSock> {
    Err(ConnectError::UnixDomain)
}

#[cfg(unix)]
fn connect_unix_socket(
    parms: &Validated,
    deadline: ConnectionDeadline,
) -> ConnectResult<ServerSock> {
    let path = PathBuf::from(parms.connect_unix.as_ref());
    let display_path = path.display().to_string();
    match run_io_with_deadline(deadline, "monetdb-unix-connect", move || {
        UnixStream::connect(path)
    }) {
        Ok(s) => {
            debug!("connected to {display_path}");
            let timeouts = Timeouts::from_validated(parms);
            let mut sock = ServerSock::from_unix(s, timeouts, deadline.instant())?;
            sock.write_all(b"0")?;
            Ok(sock)
        }
        Err(e) => {
            debug!("{display_path}: {e}");
            Err(e)
        }
    }
}

fn resolve_tcp_addresses(
    host: String,
    port: u16,
    deadline: ConnectionDeadline,
) -> ConnectResult<Vec<SocketAddr>> {
    run_io_with_deadline(deadline, "monetdb-dns-resolution", move || {
        (host, port).to_socket_addrs().map(Iterator::collect)
    })
}

fn connect_tcp_socket(
    parms: &Validated,
    deadline: ConnectionDeadline,
) -> ConnectResult<ServerSock> {
    let host = parms.connect_tcp.to_string();
    let port = parms.connect_port;
    let addresses = resolve_tcp_addresses(host.clone(), port, deadline)?;

    let sock = connect_tcp_addresses(&addresses, deadline, |address, remaining| {
        if let Some(duration) = remaining {
            TcpStream::connect_timeout(address, duration)
        } else {
            TcpStream::connect(address)
        }
    })?
    .ok_or_else(|| {
        debug!("no ip addresses found for '{host}'");
        io::Error::new(ErrorKind::NotFound, format!("no ip addresses for '{host}'"))
    })?;
    if let Err(e) = sock.set_nodelay(true) {
        debug!("failed to set nodelay: {e}");
    }
    ServerSock::from_tcp(sock, Timeouts::from_validated(parms), deadline.instant())
        .map_err(Into::into)
}

fn connect_tcp_addresses<T, F>(
    addresses: &[SocketAddr],
    deadline: ConnectionDeadline,
    mut connect: F,
) -> ConnectResult<Option<T>>
where
    F: FnMut(&SocketAddr, Option<Duration>) -> io::Result<T>,
{
    let mut err = None;
    for address in addresses {
        let remaining = deadline.remaining()?;
        match connect(address, remaining) {
            Err(e) => {
                debug!("{address}: {e}");
                err = Some(e);
            }
            Ok(value) => {
                debug!("connected to {address}");
                return Ok(Some(value));
            }
        }
    }
    if let Some(e) = err {
        Err(e.into())
    } else {
        Ok(None)
    }
}

fn connect_socket(parms: &Validated, deadline: ConnectionDeadline) -> ConnectResult<ServerSock> {
    let mut err: Option<ConnectError> = None;

    if !parms.connect_unix.is_empty() {
        match connect_unix_socket(parms, deadline) {
            Ok(s) => return Ok(s),
            Err(e) => err = Some(e),
        }
    }
    if !parms.connect_tcp.is_empty() {
        match connect_tcp_socket(parms, deadline) {
            Ok(s) => return wrap_tls(parms, s),
            Err(e) => err = Some(e),
        }
    }
    Err(err.unwrap_or_else(|| {
        io::Error::new(ErrorKind::InvalidInput, "no connection address configured").into()
    }))
}

fn wrap_tls(parms: &Validated, mut sock: ServerSock) -> ConnectResult<ServerSock> {
    if !parms.tls {
        // Prime the connection with a number of NUL bytes.
        // This has two purposes:
        // 1. if we're accidentally connecting to a TLS server it may cause the
        // server to close the connection instead of hanging waiting for us to
        // speak.
        // 2. somehow it makes establishing the connection slightly faster, not
        // clear why.
        //
        // Note: it must be an even number of NUL bytes so the server ignores it.
        let nuls = [0u8; 8];
        sock.write_all(&nuls)?;
        return Ok(sock);
    }

    let implementations: &[&TlsImplementation] = &[
        #[cfg(feature = "rustls")]
        &super::tls::rustls::wrap_with_rustls,
        // dummy implementation
        &|_, _| Err(ConnectError::TlsNotSupported),
    ];

    implementations[0](parms, sock)
}

type TlsImplementation = dyn Fn(&Validated, ServerSock) -> ConnectResult<ServerSock>;

#[derive(Debug)]
enum Login {
    Redirect(String),
    Restart(ServerSock),
    Complete(ServerSock, ServerState),
}

pub fn establish_connection(
    mut parms: Parameters,
) -> ConnectResult<(ServerSock, ServerState, DelayedCommands, Timeouts)> {
    let deadline = ConnectionDeadline::new(parms.validate()?.connect_timeout);
    let mut restarted_socket = None;
    for _ in 0..10 {
        let validated = parms.validate()?;
        let sock = match restarted_socket.take() {
            Some(sock) => sock,
            None => {
                if log_enabled!(log::Level::Debug)
                    && let Ok(url) = parms.url_without_credentials()
                {
                    debug!("connecting to {url}");
                }
                connect_socket(&validated, deadline)?
            }
        };
        let (login, mut delayed) = login(&validated, sock)?;
        match login {
            Login::Complete(sock, state) => {
                // Send the delayed commands, do not wait to receive the
                // reply, we will do that later
                return match delayed.send_delayed(sock) {
                    Ok(sock) => {
                        let timeouts = Timeouts::from_validated(&validated);
                        sock.set_connection_deadline(timeouts, None);
                        Ok((sock, state, delayed, timeouts))
                    }
                    Err(CursorError::IO(error)) => Err(ConnectError::IO(error)),
                    Err(error) => Err(ConnectError::UnexpectedResponse(error.to_string())),
                };
            }
            Login::Redirect(url) => {
                debug!("redirected to {url}");
                apply_redirect(&mut parms, &url)?;
            }
            Login::Restart(sock) => {
                debug!("local redirect, restarting authentication");
                restarted_socket = Some(sock);
            }
        }
    }
    Err(ConnectError::TooManyRedirects)
}

fn apply_redirect(parms: &mut Parameters, url: &str) -> ConnectResult<()> {
    let required_tls = parms.validate()?.tls;
    parms.apply_url(url)?;
    if required_tls && !parms.validate()?.tls {
        return Err(ConnectError::TlsDowngrade);
    }
    Ok(())
}

fn login(parms: &Validated, sock: ServerSock) -> ConnectResult<(Login, DelayedCommands)> {
    let mut server_message = String::with_capacity(1000);
    let mut mbuf = MapiBuf::new();

    // read the challenge
    let sock = MapiReader::to_limited_string(sock, &mut server_message, 5000)?;

    // determine the response
    let chal = Challenge::new(&server_message)?;
    let mut response = String::with_capacity(500);
    let (state, delayed) = challenge_response(parms, &chal, &mut response)?;

    // send the response
    mbuf.append(response);
    let sock = mbuf.write_reset(sock)?;

    // read the server response
    server_message.clear();
    let sock = MapiReader::to_limited_string(sock, &mut server_message, 5000)?;

    // process the server
    let login = process_redirects(sock, state, &server_message)?;
    Ok((login, delayed))
}

fn challenge_response(
    parms: &Validated,
    chal: &Challenge,
    response: &mut String,
) -> ConnectResult<(ServerState, DelayedCommands)> {
    use fmt::Write;

    let my_endian = Endian::NATIVE;
    let (user, password) = if chal.server_type == "merovingian" {
        ("merovingian", "")
    } else {
        (&*parms.user, &*parms.password)
    };

    let Some((prehash_algo_name, algo)) = hash_algorithms::find_algo(chal.prehash_algo) else {
        return Err(ConnectError::UnsupportedHashAlgo(
            chal.prehash_algo.to_string(),
        ));
    };

    let prehashed_password = if let Some(hex_digits) = password.strip_prefix('\u{0001}') {
        Cow::Borrowed(hex_digits)
    } else {
        let mut hasher = algo();
        hasher.update(password.as_bytes());
        let bindigest = hasher.finalize();
        let hexdigest = hex::encode(bindigest);
        Cow::Owned(hexdigest)
    };

    let response_algos = chal.response_algos;
    let Some((algo_name, algo)) = hash_algorithms::find_algo(response_algos) else {
        return Err(ConnectError::UnsupportedHashAlgo(
            response_algos.to_string(),
        ));
    };
    let mut hasher = algo();
    let ph = prehashed_password.as_bytes();
    hasher.update(ph);
    let salt = chal.salt.as_bytes();
    hasher.update(salt);
    let hashed_password = hex::encode(hasher.finalize());

    let language = &*parms.language;
    let database = &*parms.database;

    write!(
        response,
        "{my_endian}:{user}:{{{algo_name}}}{hashed_password}:{language}:{database}:FILETRANS:"
    )
    .unwrap();

    let binary_level = chal.binary.min(parms.connect_binary);
    let mut state = ServerState::new(
        prehash_algo_name,
        chal.endian,
        binary_level,
        parms.max_response_size,
    );
    let mut delayed = DelayedCommands::new();

    if parms.language == "sql" {
        // Append handshake options to the response, numbers based on enum
        // mapi_handshake_options_levels in mapi.h

        let level_limit = chal.sql_handshake_option_level;
        let mut sep = "";

        let mut arrange = |lvl: u8, key: &'static str, value: i64, cmd: fmt::Arguments| {
            if lvl < level_limit {
                // use a handshake option
                write!(response, "{sep}{key}={value}").unwrap();
                sep = ",";
            } else {
                // use a delayed Xcommand
                delayed.add(key, cmd)
            }
        };

        // MAPI_HANDSHAKE_AUTOCOMMIT = 1,
        if state.autocommit != parms.autocommit {
            let v = parms.autocommit as i64;
            arrange(1, "auto_commit", v, format_args!("Xauto_commit {v}"));
            state.autocommit = parms.autocommit;
        }

        // MAPI_HANDSHAKE_REPLY_SIZE = 2,
        if state.reply_size != parms.replysize {
            let v = parms.replysize;
            arrange(2, "reply_size", v as i64, format_args!("Xreply_size {v}"));
            state.reply_size = parms.replysize;
        }

        // MAPI_HANDSHAKE_SIZE_HEADER = 3,
        // always enabled. note: Xcommand has no underscore
        arrange(3, "size_header", 1, format_args!("Xsizeheader 1"));

        // MAPI_HANDSHAKE_COLUMNAR_PROTOCOL = 4,
        // (do not enable that)

        // MAPI_HANDSHAKE_TIME_ZONE = 5,
        let seconds_east = if let Some(tz_seconds) = parms.connect_timezone_seconds {
            tz_seconds
        } else {
            // If a date/time crate has been activated, use that.
            // Otherwise, return UTC.
            let implementations = [
                #[cfg(feature = "time")]
                crate::convert::temporal_time::timezone_offset_east_of_utc,
                // Fallback
                || 0i32,
            ];
            (implementations[0])()
        };
        if state.time_zone_seconds != seconds_east {
            let mins = seconds_east / 60;
            let sign = if mins < 0 { '-' } else { '+' };
            let a = mins.abs();
            let h = a / 60;
            let m = a % 60;
            arrange(
                5,
                "time_zone",
                seconds_east as i64,
                format_args!("sSET TIME ZONE INTERVAL '{sign}{h:02}:{m:02}' HOUR TO MINUTE;"),
            );
            state.time_zone_seconds = if 5 < level_limit {
                seconds_east
            } else {
                mins * 60
            };
        }

        if !parms.schema.is_empty() {
            let schema = parms.schema.replace('"', "\"\"");
            delayed.add("schema", format_args!("sSET SCHEMA \"{schema}\";"));
        }
    }

    response.push(':'); // after the handshake options

    if chal.clientinfo && parms.client_info {
        let mut info = ClientInfo::default();
        if !parms.client_application.is_empty() {
            info.application_name = Cow::Owned(parms.client_application.to_string());
        }
        if !parms.client_remark.is_empty() {
            info.client_remark = Cow::Owned(parms.client_remark.to_string());
        }
        write!(delayed.buffer, "{}", SqlForm(&info)).unwrap();
        delayed.buffer.end();
        delayed.responses.push(ExpectedResponse {
            description: "ClientInfo".into(),
            ignore_server_error: true,
        });
    }

    Ok((state, delayed))
}

fn process_redirects(sock: ServerSock, state: ServerState, reply: &str) -> ConnectResult<Login> {
    let reply = reply.trim_ascii();

    if reply.is_empty() || reply.starts_with("=OK") {
        debug!("login complete");
    } else if reply.starts_with('^') {
        // we only want the first one
        let first_line = reply.split('\n').next().unwrap();
        let redirect = &first_line[1..];
        if redirect.starts_with("mapi:merovingian://proxy") {
            return Ok(Login::Restart(sock));
        } else {
            return Ok(Login::Redirect(redirect.to_string()));
        }
    } else if let Some(message) = reply.strip_prefix('!') {
        debug!("login rejected: {message}");
        return Err(ConnectError::Rejected(message.to_string()));
    } else if let Some(message) = reply.strip_prefix('#') {
        debug!("login complete with welcome message {message:?}");
    } else {
        debug!("unexpected response: {reply:?}");
        return Err(ConnectError::UnexpectedResponse(reply.to_string()));
    }
    Ok(Login::Complete(sock, state))
}

#[derive(Debug)]
struct Challenge<'a> {
    salt: &'a str,
    server_type: &'a str,
    response_algos: &'a str,
    endian: Endian,
    prehash_algo: &'a str,
    sql_handshake_option_level: u8,
    binary: u16,
    clientinfo: bool,
}

impl<'a> Challenge<'a> {
    fn new(line: &'a str) -> ConnectResult<Self> {
        // trace!("parsing challenge {line:?}");
        let mut parts = line.trim_end_matches(':').split(':');

        let err = |msg: &str| ConnectError::InvalidChallenge(msg.to_string());

        let Some(salt) = parts.next() else {
            return Err(err("salt missing"));
        };

        let Some(server_type) = parts.next() else {
            return Err(err("server_type missing"));
        };

        match parts.next() {
            Some("9") => {}
            Some(_) => return Err(err("unknown protocol")),
            None => return Err(err("protocol missing")),
        };

        let Some(response_algos) = parts.next() else {
            return Err(err("hashes missing"));
        };

        let endian = match parts.next() {
            Some("BIG") => Endian::Big,
            Some("LIT") => Endian::Lit,
            Some(_) => return Err(err("invalid endian")),
            None => return Err(err("endian missing")),
        };

        let Some(prehash_algo) = parts.next() else {
            return Err(err("password hash algo missing"));
        };

        let mut sql_handshake_option_level = 0;
        let mut binary = 0;
        let mut clientinfo = false;
        for option in parts.flat_map(|field| field.split(',')) {
            if let Some(level) = option.strip_prefix("sql=") {
                sql_handshake_option_level = level
                    .parse()
                    .map_err(|_| err("invalid handshake options level"))?;
            } else if let Some(level) = option.strip_prefix("BINARY=") {
                binary = level.parse().map_err(|_| err("invalid binary level"))?;
            } else if let Some(level) = option.strip_prefix("OOBINTR=") {
                let _: u16 = level.parse().map_err(|_| err("invalid oobintr level"))?;
            } else if option == "CLIENTINFO" {
                clientinfo = true;
            }
        }

        let challenge = Challenge {
            salt,
            server_type,
            response_algos,
            endian,
            prehash_algo,
            sql_handshake_option_level,
            binary,
            clientinfo,
        };
        Ok(challenge)
    }
}

struct ClientInfo {
    client_hostname: String,
    application_name: Cow<'static, str>,
    client_library: Cow<'static, str>,
    client_remark: Cow<'static, str>,
    client_pid: u32,
}

impl Default for ClientInfo {
    fn default() -> Self {
        let client_hostname = gethostname::gethostname().to_string_lossy().to_string();
        let application_name = match env::args_os().next() {
            None => "".into(),
            Some(s) => {
                let path = PathBuf::from(s);
                let name = path.file_name().unwrap_or(OsStr::new(""));
                name.to_string_lossy().to_string().into()
            }
        };
        let client_library = PUBLIC_NAME.into();
        let client_remark = "".into();
        let client_pid = process::id();
        Self {
            client_hostname,
            application_name,
            client_library,
            client_remark,
            client_pid,
        }
    }
}

impl ClientInfo {
    fn items(&self) -> impl Iterator<Item = (&str, &dyn fmt::Display)> {
        let bla: [(&str, bool, &dyn fmt::Display); 5] = [
            (
                "ClientHostname",
                !self.client_hostname.is_empty(),
                &self.client_hostname,
            ),
            (
                "ApplicationName",
                !self.application_name.is_empty(),
                &self.application_name,
            ),
            (
                "ClientLibrary",
                !self.client_library.is_empty(),
                &self.client_library,
            ),
            (
                "ClientRemark",
                !self.client_remark.is_empty(),
                &self.client_remark,
            ),
            ("ClientPid", true, &self.client_pid),
        ];
        bla.into_iter()
            .filter(|(_, keep, _)| *keep)
            .map(|(k, _, v)| (k, v))
    }
}

struct SqlForm<'a>(&'a ClientInfo);

impl fmt::Display for SqlForm<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut prefix = "Xclientinfo ";
        for (k, v) in self.0.items() {
            writeln!(f, "{prefix}{k}={v}")?;
            prefix = "";
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
    };

    use super::*;

    #[cfg(unix)]
    #[test]
    fn nonexistent_unix_socket_returns_an_error() {
        let mut parameters = Parameters::default();
        parameters
            .set_sock("/definitely/not/a/monetdb/socket")
            .unwrap();
        let validated = parameters.validate().unwrap();

        assert!(
            connect_socket(
                &validated,
                ConnectionDeadline::new(Some(Duration::from_secs(1)))
            )
            .is_err()
        );
    }

    #[test]
    fn blocking_resolution_is_bounded_by_the_connection_deadline() {
        let started = Instant::now();
        let result = run_io_with_deadline(
            ConnectionDeadline::new(Some(Duration::from_millis(25))),
            "monetdb-test-resolution",
            || {
                thread::sleep(Duration::from_secs(1));
                Ok(())
            },
        );

        assert_eq!(result, Err(ConnectError::Timeout));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn multiple_addresses_share_one_connection_deadline() {
        let addresses = [
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ];
        let mut remaining = Vec::new();
        let connected = connect_tcp_addresses(
            &addresses,
            ConnectionDeadline::new(Some(Duration::from_secs(1))),
            |_address, timeout| {
                remaining.push(timeout.unwrap());
                if remaining.len() == 1 {
                    thread::sleep(Duration::from_millis(20));
                    Err(io::Error::new(io::ErrorKind::ConnectionRefused, "first"))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();

        assert_eq!(connected, Some(()));
        assert!(remaining[1] < remaining[0]);
    }

    fn silent_server_parameters(tls: bool) -> Parameters {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            let (_socket, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_secs(5));
        });
        let mut parameters = Parameters::default();
        parameters.set_host("127.0.0.1").unwrap();
        parameters.set_port(port).unwrap();
        parameters.set_tls(tls).unwrap();
        parameters.set_connect_timeout(1).unwrap();
        parameters
    }

    fn write_message(stream: &mut TcpStream, message: &[u8]) {
        let header = (((message.len() as u16) << 1) | 1).to_le_bytes();
        stream.write_all(&header).unwrap();
        stream.write_all(message).unwrap();
    }

    fn read_message(stream: &mut TcpStream) {
        loop {
            let mut header = [0; 2];
            stream.read_exact(&mut header).unwrap();
            let header = u16::from_le_bytes(header);
            let mut body = vec![0; usize::from(header >> 1)];
            stream.read_exact(&mut body).unwrap();
            if header & 1 != 0 {
                return;
            }
        }
    }

    fn redirect_stall_parameters() -> Parameters {
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        let target_port = target.local_addr().unwrap().port();
        let redirector = TcpListener::bind("127.0.0.1:0").unwrap();
        let redirector_port = redirector.local_addr().unwrap().port();
        thread::spawn(move || {
            let (mut stream, _) = redirector.accept().unwrap();
            let mut nuls = [0; 8];
            stream.read_exact(&mut nuls).unwrap();
            write_message(
                &mut stream,
                b"salt:mserver:9:SHA512:LIT:SHA512:sql=9:BINARY=1:",
            );
            read_message(&mut stream);
            write_message(
                &mut stream,
                format!("^mapi:monetdb://127.0.0.1:{target_port}/demo").as_bytes(),
            );
            let (_target_stream, _) = target.accept().unwrap();
            thread::sleep(Duration::from_secs(5));
        });
        let mut parameters = Parameters::default();
        parameters.set_host("127.0.0.1").unwrap();
        parameters.set_port(redirector_port).unwrap();
        parameters.set_connect_timeout(1).unwrap();
        parameters
    }

    fn restart_loop_parameters() -> Parameters {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut nuls = [0; 8];
            stream.read_exact(&mut nuls).unwrap();
            for _ in 0..10 {
                write_message(
                    &mut stream,
                    b"salt:mserver:9:SHA512:LIT:SHA512:sql=9:BINARY=1:",
                );
                read_message(&mut stream);
                write_message(&mut stream, b"^mapi:merovingian://proxy");
            }
        });
        let mut parameters = Parameters::default();
        parameters.set_host("127.0.0.1").unwrap();
        parameters.set_port(port).unwrap();
        parameters.set_client_info("false").unwrap();
        parameters.set_connect_timeout(2).unwrap();
        parameters
    }

    #[test]
    fn silent_login_is_bounded_by_the_connection_deadline() {
        let result = establish_connection(silent_server_parameters(false));
        match result {
            Err(error) => assert_eq!(error, ConnectError::Timeout),
            Ok(_) => panic!("silent login unexpectedly connected"),
        }
    }

    #[cfg(feature = "rustls")]
    #[test]
    fn silent_tls_is_bounded_by_the_connection_deadline() {
        let result = establish_connection(silent_server_parameters(true));
        match result {
            Err(error) => assert_eq!(error, ConnectError::Timeout),
            Ok(_) => panic!("silent TLS server unexpectedly connected"),
        }
    }

    #[test]
    fn redirect_stalls_share_the_original_connection_deadline() {
        let result = establish_connection(redirect_stall_parameters());
        match result {
            Err(error) => assert_eq!(error, ConnectError::Timeout),
            Ok(_) => panic!("redirect stall unexpectedly connected"),
        }
    }

    #[test]
    fn merovingian_restarts_share_the_redirect_budget() {
        let result = establish_connection(restart_loop_parameters());
        match result {
            Err(error) => assert_eq!(error, ConnectError::TooManyRedirects),
            Ok(_) => panic!("restart loop unexpectedly connected"),
        }
    }

    #[test]
    fn challenge_optional_fields_are_order_independent_and_extensible() {
        let challenge = Challenge::new(
            "salt:mserver:9:SHA512:LIT:SHA512:FUTURE=7,CLIENTINFO,OOBINTR=3,sql=9,BINARY=2:",
        )
        .unwrap();

        assert_eq!(challenge.sql_handshake_option_level, 9);
        assert_eq!(challenge.binary, 2);
        assert!(challenge.clientinfo);
    }

    #[test]
    fn challenge_reports_invalid_oobintr_by_name() {
        let error = Challenge::new("salt:mserver:9:SHA512:LIT:SHA512:OOBINTR=nope:").unwrap_err();
        assert_eq!(
            error,
            ConnectError::InvalidChallenge("invalid oobintr level".into())
        );
    }

    #[test]
    fn tls_connections_reject_plaintext_redirects() {
        let mut parameters = Parameters::default();
        parameters.set_tls(true).unwrap();

        assert_eq!(
            apply_redirect(&mut parameters, "monetdb://other.example/demo"),
            Err(ConnectError::TlsDowngrade)
        );
    }

    #[test]
    fn tls_connections_accept_tls_redirects() {
        let mut parameters = Parameters::default();
        parameters.set_tls(true).unwrap();

        apply_redirect(&mut parameters, "monetdbs://other.example/demo").unwrap();
        assert!(parameters.validate().unwrap().tls);
    }

    #[test]
    fn configured_schema_is_applied_as_a_quoted_identifier() {
        let mut parameters = Parameters::default();
        parameters.set_schema("a\"b").unwrap();
        let validated = parameters.validate().unwrap();
        let challenge = Challenge {
            salt: "salt",
            server_type: "mserver",
            response_algos: "SHA512",
            endian: Endian::Lit,
            prehash_algo: "SHA512",
            sql_handshake_option_level: 9,
            binary: 1,
            clientinfo: false,
        };
        let mut response = String::new();

        let (_, delayed) = challenge_response(&validated, &challenge, &mut response).unwrap();
        assert!(
            delayed
                .buffer
                .peek()
                .windows(b"sSET SCHEMA \"a\"\"b\";\n".len())
                .any(|window| window == b"sSET SCHEMA \"a\"\"b\";\n")
        );
    }
}
