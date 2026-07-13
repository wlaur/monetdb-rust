// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

#![allow(dead_code)]

pub(crate) mod delayed;
pub(crate) mod replies;
pub(crate) mod rowset;
mod upload;

use std::borrow::Cow;
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
use crate::framing::writing::MapiBuf;
use crate::framing::{ServerSock, ServerState};
use crate::util::ioerror::IoError;

/// An error that occurs while accessing data with a [`Cursor`].
#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum CursorError {
    /// The server returned an error.
    #[error("{0}")]
    Server(String),
    /// The connection has been closed.
    #[error("connection has been closed")]
    Closed,
    /// An IO Error occurred.
    #[error(transparent)]
    IO(#[from] IoError),
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
    buf: MapiBuf,
    replies: ReplyParser,
    reply_size: usize,
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
            buf: MapiBuf::new(),
            replies: ReplyParser::default(),
            reply_size: conn.reply_size,
            conn,
        }
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
            self.exhaust()?;
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
            self.exhaust()?;
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
            self.exhaust()?;
            return Err(error);
        }
        Ok(())
    }

    fn command(&mut self, command: &[&[u8]], vec: &mut Vec<u8>) -> Result<(), CursorError> {
        self.conn.run_locked(
            |state: &mut ServerState,
             delayed: &mut DelayedCommands,
             mut sock: ServerSock|
             -> CursorResult<ServerSock> {
                sock = delayed.send_delayed_plus(sock, command)?;
                sock = delayed.recv_delayed(sock, vec)?;
                vec.clear();
                sock = MapiReader::to_end(sock, vec)?;
                if let Some(autocommit) = response_autocommit(vec) {
                    state.initial_auto_commit = autocommit;
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
        // todo: close server side result set if necessary
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
        self.conn.run_locked(|_, delayed, sock| {
            delayed.add_xcommand_cleanup("close", res_id);
            Ok(sock)
        })?;
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
        self.conn.run_locked(|_state, delayed, mut sock| {
            if !delayed.responses.is_empty() {
                sock = delayed.send_delayed(sock)?;
                sock = delayed.recv_delayed(sock, &mut vec)?;
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
        let command = format!("Xexportbin {} {start} {count}", result.result_id);
        response.clear();
        self.command(&[command.as_bytes()], response)?;
        if response.first() == Some(&b'!') {
            ReplyParser::detect_errors(response)?;
        }
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
                *next_row += 1;
                return Ok(true);
            }
            if next_row == total_rows {
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

    fn decide_next_fetch(&self) -> (u64, u64, usize) {
        let ResultSet {
            result_id,
            next_row,
            total_rows,
            ..
        } = self.result_set().unwrap();

        let n = (total_rows - *next_row).min(self.reply_size as u64) as usize;
        (*result_id, *next_row, n)
    }

    fn fetch_more_rows(&mut self) -> CursorResult<()> {
        let (res_id, start, n) = self.decide_next_fetch();
        let cmd = format!("Xexport {res_id} {start} {n}");

        // scratch vector. TODO re-use this
        let mut vec = vec![];

        // execute the command
        self.command(&[cmd.as_bytes()], &mut vec)?;
        ReplyParser::detect_errors(&vec)?;

        // parse it into a rowset
        let mut buf = ReplyBuf::new(vec);
        let mut fields = [0u64; 4];
        ReplyParser::parse_header(&mut buf, &mut fields)?;
        let ncol = fields[1];
        let mut new_row_set = RowSet::new(buf, ncol as usize);

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
        let Some(field) = self.row_set()?.get_field_raw(colnr) else {
            return Ok(None);
        };
        let s = from_utf8(field)?;
        Ok(Some(s))
    }

    pub(crate) fn get_map<F, T>(&self, colnr: usize, f: F) -> CursorResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> CursorResult<T>,
    {
        let Some(field) = self.row_set()?.get_field_raw(colnr) else {
            return Ok(None);
        };
        let value = f(field)?;
        Ok(Some(value))
    }

    pub fn get<T: FromMonet>(&self, colnr: usize) -> CursorResult<Option<T>> {
        T::extract(self.result_set()?, colnr)
    }
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
