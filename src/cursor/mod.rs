// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

pub(crate) mod delayed;
pub(crate) mod replies;
pub(crate) mod rowset;
mod upload;

use std::borrow::Cow;
use std::fmt;
use std::mem;
use std::num::NonZeroUsize;
use std::{io, sync::Arc};

use delayed::DelayedCommands;
use replies::{BadReply, ReplyBuf, ReplyParser, ResultColumn, ResultSet, response_autocommit};
use rowset::RowSet;

use crate::conn::Conn;
use crate::convert::{FromMonet, from_utf8};
use crate::framing::FramingError;
use crate::framing::reading::MapiReader;
use crate::framing::{ServerSock, ServerState, Timeouts};
use crate::util::ioerror::IoError;

/// An error that occurs while accessing data with a [`Cursor`].
#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum CursorError {
    /// The server returned an error.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// The connection has been closed.
    #[error("connection has been closed")]
    Closed,
    /// An IO Error occurred.
    #[error(transparent)]
    IO(#[from] IoError),
    /// A configured idle or absolute operation timeout expired.
    #[error("operation timed out")]
    Timeout,
    /// The operation was explicitly cancelled from another thread.
    #[error("operation was cancelled; connection closed by cancel")]
    Cancelled,
    /// Retained for API compatibility; cancellation is currently idempotent
    /// when no operation is running and does not return this variant.
    #[error("there is no active operation to cancel")]
    NoActiveOperation,
    #[error(transparent)]
    /// Something went wrong in the communication with the server.
    Framing(#[from] FramingError),
    /// The server sent a response that we do not understand.
    #[error(transparent)]
    BadReply(#[from] BadReply),
    /// [`next_row()`](`Cursor::next_row`) or [`next_reply()`](`Cursor::next_reply`)
    /// was called but the server did not send a result set.
    #[error("there is no result set")]
    NoResultSet,
    /// The user called the wrong typed getter, for example
    /// [`get_bool()`](`Cursor::get_bool`) on an INT column.
    #[error("could not convert to {expected_type}: {message}")]
    Conversion {
        expected_type: &'static str,
        message: Cow<'static, str>,
    },
    /// Required server environment metadata was missing or malformed.
    #[error("could not retrieve server metadata: {0}")]
    Metadata(&'static str),
    /// A binary fetch requested rows outside the current result set.
    #[error("invalid binary fetch [{start}, {end}) for result set with {total_rows} rows", end = start.saturating_add(*count as u64))]
    InvalidRange {
        start: u64,
        count: usize,
        total_rows: u64,
    },
    /// The server requested an invalid or unavailable client-side file.
    #[error("file transfer failed: {0}")]
    FileTransfer(String),
    /// A client-side upload refusal and the server's final response to it.
    #[error("{refusal}; server response: {server}")]
    UploadRefused {
        refusal: Box<CursorError>,
        server: String,
    },
    /// Binary export was requested for a PREPARE metadata result.
    #[error("prepared statement metadata cannot be fetched with Xexportbin")]
    PreparedResult,
    /// Binary export was requested after MonetDB returned the complete result inline.
    #[error(
        "result is not server-resident ({rows_included} of {total_rows} rows were returned inline)"
    )]
    ResultNotResident { rows_included: u64, total_rows: u64 },
    /// Shared connection state was poisoned by an earlier panic.
    #[error("connection state is poisoned")]
    Poisoned,
}

/// A structured error returned by the MonetDB server.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ServerError {
    sqlstate: Option<String>,
    message: String,
    display: String,
}

impl ServerError {
    pub(crate) fn from_wire(display: String) -> Self {
        let bytes = display.as_bytes();
        let has_sqlstate = bytes.len() >= 6
            && bytes[5] == b'!'
            && bytes[..5].iter().all(u8::is_ascii_alphanumeric);
        let (sqlstate, message) = if has_sqlstate {
            (Some(display[..5].to_string()), display[6..].to_string())
        } else {
            (None, display.clone())
        };
        Self {
            sqlstate,
            message,
            display,
        }
    }

    pub(crate) fn with_context(context: &str, error: Self) -> Self {
        Self {
            sqlstate: error.sqlstate,
            message: format!("{context}: {}", error.message),
            display: format!("{context}: {}", error.display),
        }
    }

    /// Return the five-character SQLSTATE reported by the server, when present.
    pub fn sqlstate(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }

    /// Return the server message without the SQLSTATE prefix.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display.fmt(formatter)
    }
}

impl std::error::Error for ServerError {}

pub type CursorResult<T> = Result<T, CursorError>;

impl From<io::Error> for CursorError {
    fn from(value: io::Error) -> Self {
        IoError::from(value).into()
    }
}

/// Executes queries on a connection and manages retrieval of the
/// results. It can be obtained using the
/// [`cursor()`](`super::conn::Connection::cursor`) method on the connection.
///
/// The method [`execute()`][`Cursor::execute`] can be used to send SQL
/// statements to the server. The server will return zero or more replies,
/// usually one per statement. A reply may be an error, an acknowledgement such
/// as "your UPDATE statement affected 1001 rows", or a result set. This method
/// will immediately abort with `Err(CursorError::Server(_))` if *any* of the
/// replies is an error message, not just the first reply.
///
/// Most retrieval methods on a cursor operate on the *current reply*. To move
/// on to the next reply, call [`next_reply()`][`Cursor::next_reply`]. The only
/// exception is [`next_row()`][`Cursor::next_row`], which will automatically
/// try to skip to the next result set reply if the current reply is not a
/// result set. This is useful because people often write things like
/// ```sql
/// CREATE TABLE foo(..);
/// INSERT INTO foo SELECT .. FROM other_table;
/// INSERT INTO foo SELECT .. FROM yet_another_table;
/// SELECT COUNT(*) FROM foo;
/// ```
/// and they expect to be able to directly retrieve the count, not get an error
/// message "CREATE TABLE did not return a result set". Note that
/// [`next_row()`][`Cursor::next_row`] will *not* automatically skip to the next
/// result set if the current result set is exhausted.
///
/// To retrieve data from a result set, first call
/// [`next_row()`][`Cursor::next_row`]. This tries to move the cursor to the
/// next row and returns a boolean indicating if a new row was found. if so,
/// methods like [`get_str(colnr)`][`Cursor::get_str`] and
/// [`get_i32(colnr)`][`Cursor::get_i32`] can be used to retrieve individual
/// fields from this row.
/// Note that you **must** call [`next_row()`][`Cursor::next_row`] before you
/// call a getter. Before the first call to [`next_row()`][`Cursor::next_row`],
/// the cursor is *before* the first row, not *at* the first row. This behaviour
/// is convenient because it allows to write things like
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut cursor: monetdb::Cursor = todo!();
/// cursor.execute("SELECT * FROM mytable")?;
/// while cursor.next_row()? {
///     let value: Option<&str> = cursor.get_str(0)?;
///     println!("{}", value.unwrap());
/// }
/// # Ok(())
/// # }
/// ```
pub struct Cursor {
    conn: Arc<Conn>,
    replies: ReplyParser,
    reply_size: usize,
    timeouts: Timeouts,
}

/// Metadata needed to fetch a result set through `Xexportbin`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryResult {
    pub result_id: u64,
    pub total_rows: u64,
    /// Rows already present in the initial text-protocol response.
    pub rows_included: u64,
    pub columns: Vec<ResultColumn>,
}

impl BinaryResult {
    /// Whether MonetDB retained this result for a subsequent `Xexportbin` request.
    pub fn is_server_resident(&self) -> bool {
        self.rows_included < self.total_rows
    }
}

impl Cursor {
    pub(crate) fn new(conn: Arc<Conn>) -> Self {
        Cursor {
            replies: ReplyParser::default(),
            reply_size: conn.reply_size,
            timeouts: conn.timeouts,
            conn,
        }
    }

    /// Override the connection's idle and absolute timeouts for this cursor.
    /// Values above the portable socket timeout limit are clamped to that limit.
    pub fn set_timeouts(&mut self, timeouts: Timeouts) {
        self.timeouts = timeouts.bounded();
    }

    /// Return the idle and absolute timeouts used by this cursor.
    pub fn timeouts(&self) -> Timeouts {
        self.timeouts
    }

    /// Interrupt the operation currently using this cursor's connection.
    pub fn cancel(&self) -> CursorResult<()> {
        self.conn.cancel()
    }

    /// Execute the given SQL statements and place the cursor at the first
    /// reply. The results of any earlier queries on this cursor are discarded.
    pub fn execute(&mut self, statements: &str) -> CursorResult<()> {
        self.exhaust()?;

        let mut vec = self.replies.take_buffer();
        let command = &[b"s", statements.as_bytes(), b"\n;"];

        self.command(command, &mut vec)?;

        let error = ReplyParser::detect_errors(&vec);

        // Always create and install a replyparser, even if an error occurred.
        // We need to make sure all result sets are being released etc.
        self.replies = ReplyParser::new(vec)?;

        if let Err(err) = error {
            let _ = self.exhaust();
            return Err(err);
        }

        Ok(())
    }

    /// Execute SQL while serving named in-memory files requested through
    /// MonetDB's binary `rb` file-transfer subprotocol.
    pub fn execute_with_binary_uploads(
        &mut self,
        statements: &str,
        uploads: &std::collections::HashMap<String, Vec<u8>>,
    ) -> CursorResult<()> {
        self.execute_with_binary_uploads_with_chunk_size(
            statements,
            uploads,
            upload::DEFAULT_UPLOAD_CHUNK_SIZE,
        )
    }

    /// Execute SQL while serving named in-memory binary files with a custom
    /// maximum upload message size. Larger messages reduce prompt round trips.
    pub fn execute_with_binary_uploads_with_chunk_size(
        &mut self,
        statements: &str,
        uploads: &std::collections::HashMap<String, Vec<u8>>,
        upload_chunk_size: NonZeroUsize,
    ) -> CursorResult<()> {
        self.exhaust()?;

        let mut vec = self.replies.take_buffer();
        let command = &[b"s", statements.as_bytes(), b"\n;"];
        self.command_with_uploads(command, &mut vec, upload_chunk_size, |filename| {
            uploads
                .get(filename)
                .map(|data| Cow::Borrowed(data.as_slice()))
                .ok_or_else(|| {
                    CursorError::FileTransfer(format!("server requested unknown file {filename:?}"))
                })
        })?;

        let error = ReplyParser::detect_errors(&vec);
        self.replies = ReplyParser::new(vec)?;
        if let Err(error) = error {
            let _ = self.exhaust();
            return Err(error);
        }
        Ok(())
    }

    /// Execute SQL while producing each named binary upload only when MonetDB
    /// requests it through the `rb` file-transfer subprotocol.
    pub fn execute_with_binary_uploads_lazy<F>(
        &mut self,
        statements: &str,
        upload: F,
    ) -> CursorResult<()>
    where
        F: FnMut(&str) -> CursorResult<Vec<u8>>,
    {
        self.execute_with_binary_uploads_lazy_with_chunk_size(
            statements,
            upload::DEFAULT_UPLOAD_CHUNK_SIZE,
            upload,
        )
    }

    /// Execute SQL with lazy binary uploads and a custom maximum upload
    /// message size. Larger messages reduce prompt round trips.
    pub fn execute_with_binary_uploads_lazy_with_chunk_size<F>(
        &mut self,
        statements: &str,
        upload_chunk_size: NonZeroUsize,
        mut upload: F,
    ) -> CursorResult<()>
    where
        F: FnMut(&str) -> CursorResult<Vec<u8>>,
    {
        self.exhaust()?;

        let mut vec = self.replies.take_buffer();
        let command = &[b"s", statements.as_bytes(), b"\n;"];
        self.command_with_uploads(command, &mut vec, upload_chunk_size, |filename| {
            upload(filename).map(Cow::Owned)
        })?;

        let error = ReplyParser::detect_errors(&vec);
        self.replies = ReplyParser::new(vec)?;
        if let Err(error) = error {
            let _ = self.exhaust();
            return Err(error);
        }
        Ok(())
    }

    fn command(&mut self, command: &[&[u8]], vec: &mut Vec<u8>) -> Result<(), CursorError> {
        self.command_inner(command, vec, true)
    }

    fn command_raw(&mut self, command: &[&[u8]], vec: &mut Vec<u8>) -> Result<(), CursorError> {
        self.command_inner(command, vec, false)
    }

    fn command_inner(
        &mut self,
        command: &[&[u8]],
        vec: &mut Vec<u8>,
        update_autocommit: bool,
    ) -> Result<(), CursorError> {
        self.conn.run_locked_with_timeouts(
            self.timeouts,
            |state: &mut ServerState,
             delayed: &mut DelayedCommands,
             mut sock: ServerSock|
             -> CursorResult<ServerSock> {
                sock = delayed.send_delayed_plus(sock, command)?;
                sock = delayed.recv_delayed(sock, vec, self.conn.max_response_size)?;
                vec.clear();
                sock = MapiReader::to_limited(sock, vec, self.conn.max_response_size)?;
                if update_autocommit && let Some(autocommit) = response_autocommit(vec) {
                    state.autocommit = autocommit;
                }
                Ok(sock)
            },
        )?;
        Ok(())
    }

    /// Retrieve the number of affected rows from the current reply. INSERT,
    /// UPDATE and SELECT statements provide the number of affected rows, but
    /// for example CREATE TABLE doesn't. Returns a signed value because we're
    /// not entirely sure whether the server ever sends negative values to indicate
    /// exceptional conditions.
    ///
    /// TODO figure this out and deal with it.
    pub fn affected_rows(&self) -> Option<i64> {
        self.replies.affected_rows()
    }

    /// Return `true` if the current reply is a result set.
    pub fn has_result_set(&self) -> bool {
        self.replies.at_result_set()
    }

    /// Try to move the cursor to the next reply.
    pub fn next_reply(&mut self) -> CursorResult<bool> {
        let old = mem::take(&mut self.replies);
        let (new, to_close) = old.into_next_reply()?;
        if let Some(res_id) = to_close {
            self.queue_close(res_id)?;
        }
        self.switch_to_reply(new)
    }

    fn switch_to_reply(&mut self, replies: ReplyParser) -> CursorResult<bool> {
        self.replies = replies;
        let have_next = !matches!(self.replies, ReplyParser::Exhausted(..));
        Ok(have_next)
    }

    fn queue_close(&mut self, res_id: u64) -> CursorResult<()> {
        self.conn.try_queue_closes(&[res_id]);
        Ok(())
    }

    fn exhaust(&mut self) -> CursorResult<()> {
        loop {
            if let ReplyParser::Exhausted(..) = self.replies {
                return Ok(());
            }
            self.next_reply()?;
        }
    }

    /// Destroy the cursor, discarding all results. This may need to communicate with the server
    /// to release resources there.
    pub fn close(mut self) -> CursorResult<()> {
        self.do_close()?;
        Ok(())
    }

    fn do_close(&mut self) -> CursorResult<()> {
        self.exhaust()?;
        let mut vec = self.replies.take_buffer();
        self.conn
            .run_locked_with_timeouts(self.timeouts, |_state, delayed, mut sock| {
                if !delayed.responses.is_empty() {
                    sock = delayed.send_delayed(sock)?;
                    sock = delayed.recv_delayed(sock, &mut vec, self.conn.max_response_size)?;
                }
                Ok(sock)
            })
    }

    /// Return information about the columns of the current result set.
    pub fn column_metadata(&self) -> &[ResultColumn] {
        if let ReplyParser::Data(ResultSet { columns, .. }) = &self.replies {
            &columns[..]
        } else {
            &[]
        }
    }

    /// Return the server-side statement id when the current result is from PREPARE.
    pub fn prepared_statement_id(&self) -> Option<u64> {
        match &self.replies {
            ReplyParser::Data(ResultSet {
                result_id,
                prepared: true,
                ..
            }) => Some(*result_id),
            _ => None,
        }
    }

    /// Return metadata for the current result set.
    pub fn binary_result(&mut self) -> CursorResult<BinaryResult> {
        self.skip_to_result_set()?;
        let result = self.result_set()?;
        if result.prepared {
            return Err(CursorError::PreparedResult);
        }
        Ok(BinaryResult {
            result_id: result.result_id,
            total_rows: result.total_rows,
            rows_included: result.rows_included,
            columns: result.columns.clone(),
        })
    }

    /// Fetch a row window using MonetDB's binary result-set protocol.
    ///
    /// The returned message is the complete `Xexportbin` payload, including
    /// the `&6` header, aligned column buffers, table of contents, and trailing
    /// table-of-contents offset described by `binary-resultset.rst`.
    pub fn fetch_binary(&mut self, start: u64, count: usize) -> CursorResult<Vec<u8>> {
        let mut response = Vec::new();
        self.fetch_binary_into(start, count, &mut response)?;
        Ok(response)
    }

    /// Fetch a binary row window into a caller-owned buffer.
    ///
    /// The buffer is cleared but retains its allocation, allowing callers to
    /// reuse response capacity across successive windows.
    pub fn fetch_binary_into(
        &mut self,
        start: u64,
        count: usize,
        response: &mut Vec<u8>,
    ) -> CursorResult<()> {
        self.skip_to_result_set()?;
        let result = self.result_set()?;
        if result.prepared {
            return Err(CursorError::PreparedResult);
        }
        if result.rows_included == result.total_rows {
            return Err(CursorError::ResultNotResident {
                rows_included: result.rows_included,
                total_rows: result.total_rows,
            });
        }
        if count == 0 || start >= result.total_rows {
            return Err(CursorError::InvalidRange {
                start,
                count,
                total_rows: result.total_rows,
            });
        }
        let available = result.total_rows - start;
        let count = count.min(usize::try_from(available).unwrap_or(usize::MAX));
        let result_id = result.result_id;
        let expected_columns = result.columns.len();
        let command = format!("Xexportbin {result_id} {start} {count}");
        response.clear();
        self.command_raw(&[command.as_bytes()], response)?;
        if response.first() == Some(&b'!') {
            ReplyParser::detect_errors(response)?;
        }
        let header_end = response
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or(BadReply::UnexpectedEnd)?;
        let mut header = ReplyBuf::new(response[..=header_end].to_vec());
        let mut fields = [0u64; 4];
        ReplyParser::parse_export_header(&mut header, &mut fields)?;
        validate_export_header(&fields, result_id, expected_columns, count, start)?;
        Ok(())
    }

    /// Advance the cursor to the next available row in the result set,
    /// returning a boolean that indicates whether such a row was present.
    ///
    /// When the cursor enters a new result set after
    /// [`execute()`][`Cursor::execute`] or
    /// [`next_reply()`][`Cursor::next_reply`], it is initially positioned
    /// *before* the first row, and the first call to this method will advance
    /// it to be *at* the first row. This means you always have to call this method
    /// before calling getters.
    pub fn next_row(&mut self) -> CursorResult<bool> {
        self.skip_to_result_set()?;

        loop {
            let ResultSet {
                row_set,
                next_row,
                total_rows,
                ..
            } = self.result_set_mut();

            if row_set.advance()? {
                count_row(next_row, *total_rows)?;
                return Ok(true);
            }
            if next_row >= total_rows {
                return Ok(false);
            }
            self.fetch_more_rows()?;
        }
    }

    pub(crate) fn result_set(&self) -> CursorResult<&ResultSet> {
        if let ReplyParser::Data(rs) = &self.replies {
            Ok(rs)
        } else {
            Err(CursorError::NoResultSet)
        }
    }

    fn result_set_mut(&mut self) -> &mut ResultSet {
        let ReplyParser::Data(rs) = &mut self.replies else {
            unreachable!("skip_to_result_set() should have ensured a result set");
        };
        rs
    }

    fn skip_to_result_set(&mut self) -> CursorResult<()> {
        loop {
            match &mut self.replies {
                ReplyParser::Data(_) => return Ok(()),
                ReplyParser::Exhausted(_) => return Err(CursorError::NoResultSet),
                _ => self.next_reply()?,
            };
        }
    }

    /// Override the number of rows requested by each text-protocol `Xexport`.
    pub fn set_reply_size(&mut self, reply_size: NonZeroUsize) {
        self.reply_size = reply_size.get();
    }

    fn decide_next_fetch(&self) -> CursorResult<(u64, u64, usize, usize)> {
        let ResultSet {
            result_id,
            next_row,
            total_rows,
            columns,
            ..
        } = self.result_set()?;

        let remaining = total_rows
            .checked_sub(*next_row)
            .ok_or(BadReply::TooManyRows { total: *total_rows })?;
        let n = remaining.min(self.reply_size as u64) as usize;
        Ok((*result_id, *next_row, n, columns.len()))
    }

    fn fetch_more_rows(&mut self) -> CursorResult<()> {
        let (res_id, start, n, expected_columns) = self.decide_next_fetch()?;
        let cmd = format!("Xexport {res_id} {start} {n}");

        // scratch vector. TODO re-use this
        let mut vec = vec![];

        // execute the command
        self.command(&[cmd.as_bytes()], &mut vec)?;
        ReplyParser::detect_errors(&vec)?;

        // parse it into a rowset
        let mut buf = ReplyBuf::new(vec);
        let mut fields = [0u64; 4];
        ReplyParser::parse_export_header(&mut buf, &mut fields)?;
        validate_export_header(&fields, res_id, expected_columns, n, start)?;
        let mut new_row_set = RowSet::new(buf, expected_columns);
        validate_fetch_progress(&new_row_set, start, n)?;

        // If we were reading the initial response, save it.
        // Then install the new rowset, saving the old one if it's the primary.
        // We know it's the primary when stashed_primary_row_set is still None.
        let ResultSet {
            row_set,
            stashed: stashed_primary_row_set,
            ..
        } = self.result_set_mut();
        mem::swap(row_set, &mut new_row_set);
        if stashed_primary_row_set.is_none() {
            // new_row_set is actually the old row set now
            *stashed_primary_row_set = Some(new_row_set);
        }

        // Now the new rows are in place!
        Ok(())
    }

    fn row_set(&self) -> CursorResult<&RowSet> {
        if let ReplyParser::Data(ResultSet { row_set, .. }) = &self.replies {
            Ok(row_set)
        } else {
            Err(CursorError::NoResultSet)
        }
    }

    pub fn get_str(&self, colnr: usize) -> CursorResult<Option<&str>> {
        let Some(field) = self.row_set()?.get_field_raw(colnr)? else {
            return Ok(None);
        };
        let s = from_utf8(field)?;
        Ok(Some(s))
    }

    pub fn get<T: FromMonet>(&self, colnr: usize) -> CursorResult<Option<T>> {
        T::extract(self.result_set()?, colnr)
    }
}

fn count_row(next_row: &mut u64, total_rows: u64) -> Result<(), BadReply> {
    if *next_row >= total_rows {
        return Err(BadReply::TooManyRows { total: total_rows });
    }
    *next_row += 1;
    Ok(())
}

fn validate_fetch_progress(row_set: &RowSet, start: u64, requested: usize) -> Result<(), BadReply> {
    if requested > 0 && !row_set.has_pending_row() {
        return Err(BadReply::EmptyExportWindow { start, requested });
    }
    Ok(())
}

fn validate_export_header(
    fields: &[u64; 4],
    expected_result_id: u64,
    expected_columns: usize,
    expected_rows: usize,
    expected_offset: u64,
) -> Result<(), BadReply> {
    if fields[0] != expected_result_id {
        return Err(BadReply::ResultIdMismatch {
            expected: expected_result_id,
            actual: fields[0],
        });
    }
    if fields[1] != expected_columns as u64 {
        return Err(BadReply::ColumnCountMismatch {
            expected: expected_columns,
            actual: fields[1],
        });
    }
    if fields[2] != expected_rows as u64 {
        return Err(BadReply::RowCountMismatch {
            expected: expected_rows,
            actual: fields[2],
        });
    }
    if fields[3] != expected_offset {
        return Err(BadReply::RowOffsetMismatch {
            expected: expected_offset,
            actual: fields[3],
        });
    }
    Ok(())
}

macro_rules! define_getter {
    ($method:ident, $type:ty) => {
        pub fn $method(&self, col: usize) -> CursorResult<Option<$type>> {
            self.get(col)
        }
    };
}

/// These getters can be called to retrieve values from the current row, after
/// [`next_row()`][`Cursor::next_row`] has confirmed that that row exists.
/// They return None if the value is NULL.
impl Cursor {
    define_getter!(get_bool, bool);
    define_getter!(get_i8, i8);
    define_getter!(get_u8, u8);
    define_getter!(get_i16, i16);
    define_getter!(get_u16, u16);
    define_getter!(get_i32, i32);
    define_getter!(get_u32, u32);
    define_getter!(get_i64, i64);
    define_getter!(get_u64, u64);
    define_getter!(get_i128, i128);
    define_getter!(get_u128, u128);
    define_getter!(get_isize, isize);
    define_getter!(get_usize, usize);
    define_getter!(get_f32, f32);
    define_getter!(get_f64, f64);
}

impl Drop for Cursor {
    fn drop(&mut self) {
        let mut result_ids = Vec::new();
        loop {
            let replies = mem::take(&mut self.replies);
            match replies.into_next_reply() {
                Ok((next, to_close)) => {
                    if let Some(result_id) = to_close {
                        result_ids.push(result_id);
                    }
                    let exhausted = matches!(next, ReplyParser::Exhausted(_));
                    self.replies = next;
                    if exhausted {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        self.conn.try_queue_closes(&result_ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_header_must_match_current_result() {
        assert_eq!(
            validate_export_header(&[8, 2, 1, 0], 7, 2, 1, 0),
            Err(BadReply::ResultIdMismatch {
                expected: 7,
                actual: 8,
            })
        );
        assert_eq!(
            validate_export_header(&[7, 4_000_000_000, 1, 0], 7, 2, 1, 0),
            Err(BadReply::ColumnCountMismatch {
                expected: 2,
                actual: 4_000_000_000,
            })
        );
        assert_eq!(
            validate_export_header(&[7, 2, 2, 0], 7, 2, 1, 0),
            Err(BadReply::RowCountMismatch {
                expected: 1,
                actual: 2,
            })
        );
        assert_eq!(
            validate_export_header(&[7, 2, 1, 3], 7, 2, 1, 0),
            Err(BadReply::RowOffsetMismatch {
                expected: 0,
                actual: 3,
            })
        );
    }

    #[test]
    fn result_rows_cannot_exceed_the_reported_total() {
        let mut next_row = 0;
        count_row(&mut next_row, 1).unwrap();
        assert_eq!(next_row, 1);
        assert_eq!(
            count_row(&mut next_row, 1),
            Err(BadReply::TooManyRows { total: 1 })
        );
    }

    #[test]
    fn fetched_window_must_contain_a_row() {
        let row_set = RowSet::new(ReplyBuf::new(Vec::new()), 1);
        assert_eq!(
            validate_fetch_progress(&row_set, 7, 3),
            Err(BadReply::EmptyExportWindow {
                start: 7,
                requested: 3
            })
        );
    }
}
