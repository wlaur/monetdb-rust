// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::{borrow::Cow, error, mem, str::FromStr};

use bstr::{BStr, BString, ByteSlice};
use memchr::memmem;

use crate::monettypes::MonetType;

use super::{CursorError, CursorResult, ServerError, rowset::RowSet};

#[derive(Debug, PartialEq, Eq, Clone, thiserror::Error)]
pub enum BadReply {
    #[error("invalid utf-8 encoding in {0}")]
    Unicode(&'static str),
    #[error("unknown server response: {0}")]
    UnknownResponse(BString),
    #[error("expected separator {:?} not found", *.0 as char)]
    SepNotFound(u8),
    #[error("invalid reply header: {0}")]
    InvalidHeader(String),
    #[error("unexpected reply header: {0}")]
    UnexpectedHeader(BString),
    #[error("unexpected end of server response")]
    UnexpectedEnd,
    #[error("too many columns in result set: {0}")]
    TooManyColumns(u64),
    #[error("too few columns in result set: {0}")]
    TooFewColumns(usize),
    #[error("result header includes {included} rows but reports only {total} total rows")]
    TooManyIncludedRows { included: u64, total: u64 },
    #[error("result contains more rows than its reported total of {total}")]
    TooManyRows { total: u64 },
    #[error("invalid backslash escape in result set")]
    InvalidBackslashEscape,
    #[error("invalid quoted identifier in result metadata")]
    InvalidQuotedIdentifier,
    #[error("column index {0} out of bounds, have only {1} columns")]
    ColumnIndexOutOfBounds(usize, usize),
    #[error("result id {actual} in export reply does not match requested result id {expected}")]
    ResultIdMismatch { expected: u64, actual: u64 },
    #[error("export reply has {actual} columns, expected {expected}")]
    ColumnCountMismatch { expected: usize, actual: u64 },
    #[error("export reply contains {actual} rows, expected {expected}")]
    RowCountMismatch { expected: usize, actual: u64 },
    #[error("export reply starts at row {actual}, expected {expected}")]
    RowOffsetMismatch { expected: u64, actual: u64 },
    #[error("export reply for {requested} rows at offset {start} contains no row data")]
    EmptyExportWindow { start: u64, requested: usize },
}

pub type RResult<T> = Result<T, BadReply>;

pub(crate) fn response_autocommit(response: &[u8]) -> Option<bool> {
    response
        .split(|byte| *byte == b'\n')
        .filter_map(|line| {
            if line == b"&4 f" {
                Some(false)
            } else if line == b"&4 t" {
                Some(true)
            } else {
                None
            }
        })
        .next_back()
}

pub(crate) fn server_error(response: &[u8]) -> Option<ServerError> {
    let mut messages = response
        .split(|byte| *byte == b'\n')
        .filter_map(|line| line.strip_prefix(b"!"));
    let first = messages.next()?;
    let mut result = String::from_utf8_lossy(first).into_owned();
    for message in messages {
        result.push('\n');
        result.push_str(&String::from_utf8_lossy(message));
    }
    Some(ServerError::from_wire(result))
}

#[derive(Debug)]
pub struct ReplyBuf {
    data: Vec<u8>,
    pos: usize,
}

impl ReplyBuf {
    pub fn new(vec: Vec<u8>) -> Self {
        ReplyBuf { data: vec, pos: 0 }
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn peek(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    pub fn is_empty(&self) -> bool {
        self.peek().is_empty()
    }

    pub fn consume(&mut self, nbytes: usize) -> RResult<&mut [u8]> {
        let Some(newpos) = self.pos.checked_add(nbytes) else {
            return Err(BadReply::UnexpectedEnd);
        };
        if newpos > self.data.len() {
            return Err(BadReply::UnexpectedEnd);
        }
        let ret = &mut self.data[self.pos..newpos];
        self.pos = newpos;
        Ok(ret)
    }

    pub fn find(&self, byte: u8) -> Option<usize> {
        memchr::memchr(byte, self.peek())
    }

    pub fn find2(&self, byte1: u8, byte2: u8) -> Option<(usize, u8)> {
        let haystack = self.peek();
        memchr::memchr2(byte1, byte2, haystack).map(|i| (i, haystack[i]))
    }

    pub fn find_line(&mut self, first: u8) -> Option<usize> {
        let haystack = self.peek();
        if haystack.is_empty() {
            None
        } else if haystack[0] == first {
            Some(0)
        } else {
            memmem::find(haystack, &[b'\n', first]).map(|idx| idx + 1)
        }
    }

    pub fn split(&mut self, sep: u8) -> RResult<&'_ mut [u8]> {
        let Some(end) = self.find(sep) else {
            if self.is_empty() {
                return Err(BadReply::UnexpectedEnd);
            } else {
                return Err(BadReply::SepNotFound(sep));
            }
        };
        let ret = self.consume(end + 1)?;
        Ok(&mut ret[..end])
    }

    pub(super) fn position(&self) -> usize {
        self.pos
    }

    pub(super) fn range(&self, start: usize, len: usize) -> RResult<&[u8]> {
        let end = start.checked_add(len).ok_or(BadReply::UnexpectedEnd)?;
        self.data.get(start..end).ok_or(BadReply::UnexpectedEnd)
    }

    pub fn split_str(&mut self, sep: u8, context: &'static str) -> RResult<&str> {
        let head = self.split(sep)?;
        from_utf8(context, head)
    }

    pub fn convert_backslashes(&mut self, skip: usize) -> RResult<&'_ mut [u8]> {
        let start_offset = self.pos + skip;
        let start = self.data.as_mut_ptr().wrapping_add(start_offset);
        let end = self.data.as_mut_ptr().wrapping_add(self.data.len());
        assert!(start <= end);

        let mut wr = start;
        let mut rd = start as *const u8;

        // wr <= rd <= end
        loop {
            if rd == end {
                return Err(BadReply::UnexpectedEnd); // end quote missing
            }
            // Here, wr <= rd < end

            // SAFETY: the loop invariant above guarantees `rd < end`.
            let b = unsafe { rd.read() };
            rd = rd.wrapping_add(1);
            // Now, wr < rd <= end

            let unescaped = if b == b'\\' {
                // avail is nr of bytes available AFTER the backslash
                // SAFETY: `rd` and `end` originate from the same Vec allocation,
                // and the loop invariant guarantees `rd <= end`.
                let avail = unsafe { end.offset_from(rd) };
                if avail < 1 {
                    return Err(BadReply::InvalidBackslashEscape);
                }
                // SAFETY: `avail >= 1` proves `rd` points to an initialized byte.
                let chr = unsafe { rd.read() };
                rd = rd.wrapping_add(1);
                match chr {
                    b't' => b'\t',
                    b'n' => b'\n',
                    b'f' => 0x0C,
                    b'r' => b'\r',
                    b'\\' => b'\\',
                    b'"' => b'"',
                    b'0'..=b'3' => {
                        // octal escape
                        let e1 = chr.wrapping_sub(b'0');
                        if avail < 3 {
                            return Err(BadReply::UnexpectedEnd);
                        }
                        // SAFETY: `avail >= 3` proves both remaining escape bytes
                        // are initialized and within the allocation.
                        let e2 = unsafe { rd.read().wrapping_sub(b'0') };
                        rd = rd.wrapping_add(1);
                        // SAFETY: `avail >= 3` proves this byte is initialized and
                        // within the same allocation.
                        let e3 = unsafe { rd.read().wrapping_sub(b'0') };
                        rd = rd.wrapping_add(1);
                        if ((e2 | e3) & 0b1111_1000) != 0 {
                            return Err(BadReply::InvalidBackslashEscape);
                        }
                        (e1 << 6) | (e2 << 3) | e3
                    }
                    _ => return Err(BadReply::InvalidBackslashEscape),
                }
            } else if b == b'"' {
                break;
            } else {
                // nothing to unescape
                b
            };
            // rd may have moved but still, wr < rd <= end

            // SAFETY: `wr <= rd <= end`; unescaping never expands input, so `wr`
            // always points at initialized capacity within the Vec allocation.
            unsafe { wr.write(unescaped) };
            wr = wr.wrapping_add(1);
            // wr <= rd <= end
        }

        // SAFETY: both pointers were derived from `self.data` and remain within
        // that allocation (or one-past it).
        let rd_offset = unsafe { rd.offset_from(self.data.as_mut_ptr()) as usize };
        // SAFETY: both pointers were derived from `self.data` and remain within
        // that allocation (or one-past it).
        let wr_offset = unsafe { wr.offset_from(self.data.as_mut_ptr()) as usize };

        let old_pos = self.pos;
        self.pos = rd_offset;
        Ok(&mut self.data[old_pos..wr_offset])
    }
}

#[test]
fn test_convert_backslashes() {
    #[track_caller]
    fn f(s: &str, skip: usize, expected: RResult<&str>) {
        let Some(opening_quote_idx) = s.find('"') else {
            panic!("test data must have opening quote");
        };
        let mut buf = ReplyBuf::new(s.into());
        buf.consume(opening_quote_idx).unwrap();
        assert!(buf.peek().starts_with(b"\""), "{}", BStr::new(buf.peek()));
        buf.consume(1).unwrap();
        // now we have the buf where we want it, right after the opening quote.

        let actual: Result<&BStr, BadReply> = buf.convert_backslashes(skip).map(|x| BStr::new(x));
        let expected: Result<&BStr, BadReply> = expected.map(|t| t.into());

        assert_eq!(actual, expected);
    }

    f(r#"foo"banana""#, 0, Ok("banana"));
    f(r#"foo"banana"#, 0, Err(BadReply::UnexpectedEnd));

    f(r#"foo"""#, 0, Ok(""));
    f(r#"foo""#, 0, Err(BadReply::UnexpectedEnd));

    f(r#"foo"bana\na""#, 0, Ok("bana\na"));
    f(r#"foo"bana\ta""#, 0, Ok("bana\ta"));
    f(r#"foo"bana\fa""#, 0, Ok("bana\x0Ca"));
    f(r#"foo"bana\ra""#, 0, Ok("bana\ra"));
    f(r#"foo"bana\"a""#, 0, Ok("bana\"a"));
    f(r#"foo"bana\xa""#, 0, Err(BadReply::InvalidBackslashEscape));

    f(r#"foo"bana\Na""#, 0, Err(BadReply::InvalidBackslashEscape));
    f(r#"foo"bana\Ta""#, 0, Err(BadReply::InvalidBackslashEscape));
    f(r#"foo"bana\Fa""#, 0, Err(BadReply::InvalidBackslashEscape));
    f(r#"foo"bana\Ra""#, 0, Err(BadReply::InvalidBackslashEscape));

    f(r#"foo"bana\R4""#, 0, Err(BadReply::InvalidBackslashEscape));
    f(r#"foo"bana\R5""#, 0, Err(BadReply::InvalidBackslashEscape));
    f(r#"foo"bana\R6""#, 0, Err(BadReply::InvalidBackslashEscape));
    f(r#"foo"bana\R7""#, 0, Err(BadReply::InvalidBackslashEscape));

    f(r#"foo"bana\r""#, 0, Ok("bana\r"));
    f(r#"foo"\tbanana""#, 0, Ok("\tbanana"));

    // LATIN SMALL LETTER A == a == oct utf-8 \141
    f(r#"foo"b\141nana""#, 0, Ok("banana"));
    // LATIN SMALL LETTER A WITH DIAERESIS == \u{e4} == oct utf-8 \303\244
    f(r#"foo"b\303\244nana""#, 0, Ok("b\u{e4}nana"));

    // Test the skip. 4 skips the bana but it's still included in the result
    f(r#"foo"bana\na""#, 4, Ok("bana\na"));
}

#[derive(Debug)]
pub enum ReplyParser {
    Exhausted(Vec<u8>),
    Error(ReplyBuf),
    Success {
        buf: ReplyBuf,
        affected: Option<i64>,
    },
    Data(ResultSet),
    Tx {
        buf: ReplyBuf,
    },
}

#[derive(Debug)]
pub struct ResultSet {
    pub result_id: u64,
    pub prepared: bool,
    pub next_row: u64,
    pub total_rows: u64,
    pub rows_included: u64,
    pub columns: Vec<ResultColumn>,
    pub row_set: RowSet,
    pub stashed: Option<RowSet>,
    pub to_close: Option<u64>,
}

impl Default for ReplyParser {
    fn default() -> Self {
        ReplyParser::Exhausted(vec![])
    }
}

impl ReplyParser {
    pub fn new(mut vec: Vec<u8>) -> RResult<Self> {
        let min_cap = 8192;
        if vec.capacity() < min_cap {
            vec.reserve(min_cap - vec.capacity());
        }
        let buf = ReplyBuf::new(vec);
        Self::parse(buf)
    }

    pub fn take_buffer(&mut self) -> Vec<u8> {
        if let ReplyParser::Exhausted(vec) = self {
            mem::take(vec)
        } else {
            panic!("cannot call ReplyParser::take_buffer() when parser is not exhausted");
        }
    }

    pub fn affected_rows(&self) -> Option<i64> {
        match self {
            ReplyParser::Success { affected, .. } => *affected,
            ReplyParser::Data(ResultSet {
                total_rows: nrows, ..
            }) => i64::try_from(*nrows).ok(),
            _ => None,
        }
    }

    pub fn at_result_set(&self) -> bool {
        matches!(self, ReplyParser::Data { .. })
    }

    pub fn into_next_reply(self) -> RResult<(ReplyParser, Option<u64>)> {
        let mut return_to_close = None;
        use ReplyParser::*;
        let buf = match self {
            Exhausted(vec) => ReplyBuf::new(vec),
            Error(buf) | Success { buf, .. } | Tx { buf } => buf,
            Data(
                ResultSet {
                    stashed: Some(row_set),
                    to_close,
                    ..
                }
                | ResultSet {
                    stashed: None,
                    row_set,
                    to_close,
                    ..
                },
            ) => {
                return_to_close = to_close;
                row_set.finish()?
            }
        };

        ReplyParser::parse(buf).map(|parser| (parser, return_to_close))
    }

    pub fn detect_errors(response: &[u8]) -> CursorResult<()> {
        match server_error(response) {
            Some(error) => Err(CursorError::Server(error)),
            None => Ok(()),
        }
    }

    fn parse(buf: ReplyBuf) -> RResult<ReplyParser> {
        let ahead = buf.peek();
        match ahead {
            [] => {
                let mut vec = buf.into_vec();
                vec.clear();
                Ok(ReplyParser::Exhausted(vec))
            }
            [b'&', b'1', ..] => Self::parse_data(buf, false),
            [b'&', b'2', ..] => Self::parse_successful_update(buf),
            [b'&', b'3', ..] => Self::parse_successful_other(buf),
            [b'&', b'4', ..] => Self::parse_autocommit_status(buf),
            [b'&', b'5', ..] => Self::parse_data(buf, true),
            [b'!', ..] => Self::parse_error(buf),
            [b'=', b'O', b'K', b'\n', ..] => Self::parse_ok(buf),
            [b'=', ..] => Self::parse_noslice(buf),
            _ => {
                let line = ahead.as_bstr().lines().next().unwrap();
                Err(BadReply::UnknownResponse(line.into()))
            }
        }
    }

    fn parse_successful_update(mut buf: ReplyBuf) -> RResult<ReplyParser> {
        let mut fields = [0]; // don't care about the other fields yet
        Self::parse_header(&mut buf, &mut fields)?;
        Ok(ReplyParser::Success {
            buf,
            affected: Some(fields[0]),
        })
    }

    fn parse_successful_other(mut buf: ReplyBuf) -> RResult<ReplyParser> {
        let mut fields: [i64; 0] = [];
        Self::parse_header(&mut buf, &mut fields)?;
        Ok(ReplyParser::Success {
            buf,
            affected: None,
        })
    }

    fn parse_ok(mut buf: ReplyBuf) -> RResult<ReplyParser> {
        let line = buf.split_str(b'\n', "success line")?;
        if line != "=OK" {
            return Err(BadReply::InvalidHeader(format!("expected '=OK': {line}")));
        }
        Ok(ReplyParser::Success {
            buf,
            affected: None,
        })
    }

    fn parse_noslice(buf: ReplyBuf) -> RResult<ReplyParser> {
        // MonetDB's `testing/mapicursor.py` names '=' replies
        // `MSG_TUPLE_NOSLICE`; EXPLAIN emits one such line per plan row.
        let rows = buf
            .peek()
            .as_bstr()
            .lines()
            .take_while(|line| line.starts_with(b"="))
            .count();
        let total_rows = u64::try_from(rows)
            .map_err(|_| BadReply::InvalidHeader("too many '=' result rows".into()))?;
        Ok(ReplyParser::Data(ResultSet {
            result_id: 0,
            prepared: false,
            next_row: 0,
            total_rows,
            rows_included: total_rows,
            columns: vec![ResultColumn::new("rel", MonetType::Varchar(0))],
            row_set: RowSet::new_noslice(buf),
            to_close: None,
            stashed: None,
        }))
    }

    pub(crate) fn parse_header<T: FromStr>(buf: &mut ReplyBuf, dest: &mut [T]) -> RResult<()> {
        let line = buf.split_str(b'\n', "header line")?.trim_ascii();
        Self::parse_header_line(line, dest)
    }

    pub(crate) fn parse_export_header<T: FromStr>(
        buf: &mut ReplyBuf,
        dest: &mut [T],
    ) -> RResult<()> {
        let line = buf.split_str(b'\n', "header line")?.trim_ascii();
        if !line.starts_with("&6 ") {
            return Err(BadReply::InvalidHeader(format!(
                "expected '&6 ' prefix: {line}"
            )));
        }
        Self::parse_header_line(line, dest)
    }

    fn parse_header_line<T: FromStr>(line: &str, dest: &mut [T]) -> RResult<()> {
        let bytes = line.as_bytes();
        if bytes.len() < 3 || bytes[0] != b'&' || !bytes[1].is_ascii_digit() || bytes[2] != b' ' {
            return Err(BadReply::InvalidHeader(format!(
                "expected '&<digit> ' prefix: {line}"
            )));
        }
        let mut parts = line[3..].split(' ');
        for (i, d) in dest.iter_mut().enumerate() {
            let Some(p) = parts.next() else {
                return Err(BadReply::InvalidHeader(format!(
                    "not enough header items, expected {n}: {line}",
                    n = dest.len()
                )));
            };
            let Ok(value) = p.parse() else {
                return Err(BadReply::InvalidHeader(format!(
                    "cannot parse header item {i}: {line}"
                )));
            };
            *d = value;
        }
        Ok(())
    }

    fn parse_autocommit_status(mut buf: ReplyBuf) -> RResult<ReplyParser> {
        let line = buf.split_str(b'\n', "header line")?.trim_ascii();
        let auto_commit = if line == "&4 f" {
            false
        } else if line == "&4 t" {
            true
        } else {
            return Err(BadReply::InvalidHeader(format!(
                "invalid autocommit header: {line}"
            )));
        };
        let _ = auto_commit;
        Ok(ReplyParser::Tx { buf })
    }

    fn parse_error(mut buf: ReplyBuf) -> RResult<ReplyParser> {
        // for now, .execute() has already returned the error, no reason to hold on to it
        let _line = buf.split_str(b'\n', "error header")?.trim_ascii();
        Ok(ReplyParser::Error(buf))
    }

    fn parse_data(mut buf: ReplyBuf, prepared: bool) -> RResult<ReplyParser> {
        let mut fields = [0; 4];
        Self::parse_header(&mut buf, &mut fields)?;
        let [result_id, rows_total, ncols, rows_included] = fields;
        if rows_included > rows_total {
            return Err(BadReply::TooManyIncludedRows {
                included: rows_included,
                total: rows_total,
            });
        }
        if ncols > usize::MAX as u64 {
            return Err(BadReply::TooManyColumns(ncols));
        }
        let ncols = ncols as usize;
        // Each of the five column metadata lines needs a `,\t` delimiter for
        // every column after the first. Also prevent an untrusted header from
        // amplifying the received reply into a much larger per-column allocation.
        let minimum_metadata_bytes = ncols.saturating_sub(1).saturating_mul(10);
        let column_allocation_bytes = ncols
            .checked_mul(std::mem::size_of::<ResultColumn>())
            .ok_or(BadReply::TooManyColumns(ncols as u64))?;
        let maximum_column_allocation = buf.peek().len().saturating_mul(2);
        if minimum_metadata_bytes > buf.peek().len()
            || column_allocation_bytes > maximum_column_allocation
        {
            return Err(BadReply::TooManyColumns(ncols as u64));
        }
        let to_close = (!prepared && rows_included < rows_total).then_some(result_id);

        let mut columns = Vec::new();
        columns
            .try_reserve_exact(ncols)
            .map_err(|_| BadReply::TooManyColumns(ncols as u64))?;
        columns.resize(ncols, ResultColumn::empty());

        // parse the table_name header
        Self::parse_data_header(&mut buf, "table_name", &mut columns, &|col, s| {
            col.table_name.push_str(s);
            Ok(())
        })?;

        // parse the name header
        Self::parse_data_header(&mut buf, "name", &mut columns, &|col, s| {
            // `sql/backends/monet5/sql_result.c:mvc_export_head` surrounds
            // names containing SQL separators with double quotes and
            // backslash-escapes embedded quotes and backslashes.
            col.name.push_str(&decode_column_name(s)?);
            Ok(())
        })?;

        // parse the type header
        Self::parse_data_header(&mut buf, "type", &mut columns, &|col, s| {
            let Some(typ) = MonetType::from_mapi_code(s) else {
                return Err(format!("unknown column type: {s}").into());
            };
            col.typ = typ;
            Ok(())
        })?;

        // parse the length header
        Self::parse_data_header(&mut buf, "length", &mut columns, &|col, s| {
            if let MonetType::Varchar(n) = &mut col.typ {
                *n = u32::from_str(s)?
            };
            Ok(())
        })?;

        // `sql/backends/monet5/sql_gencode.c` handcrafts EXPLAIN results with
        // four metadata lines and then '=' rows, omitting `typesizes`.
        if buf.peek().starts_with(b"% ") {
            Self::parse_data_header(&mut buf, "typesizes", &mut columns, &|col, s| {
                if let MonetType::Decimal(precision, scale) = &mut col.typ {
                    let Some((pr, sc)) = s.split_once(' ') else {
                        return Err("expect typesizes to be PRECISION <space> SCALE".into());
                    };
                    *precision = pr.parse()?;
                    *scale = sc.parse()?;
                };
                Ok(())
            })?;
        }

        let row_set = if buf.peek().starts_with(b"=") {
            RowSet::new_noslice(buf)
        } else {
            RowSet::new(buf, columns.len())
        };
        Ok(ReplyParser::Data(ResultSet {
            result_id,
            prepared,
            next_row: 0,
            total_rows: rows_total,
            rows_included,
            columns,
            row_set,
            to_close,
            stashed: None,
        }))
    }

    fn parse_data_header<'a>(
        buf: &'a mut ReplyBuf,
        expected_kind: &str,
        columns: &'a mut [ResultColumn],
        f: ResultColumnUpdater<'_, 'a>,
    ) -> RResult<()> {
        let line: &[u8] = buf.split(b'\n')?;
        let line = from_utf8("data header line", line)?;
        let Some(line) = line.strip_prefix("% ") else {
            return Err(BadReply::UnexpectedHeader(line.into()));
        };
        let Some((body, kind)) = line.split_once(" # ") else {
            return Err(BadReply::InvalidHeader(
                "expected '# ' in data header".into(),
            ));
        };
        if kind != expected_kind {
            return Err(BadReply::InvalidHeader(format!(
                "expected '{expected_kind}' header, found {}",
                BStr::new(kind)
            )));
        }

        let mut columns = columns.iter_mut();
        for (i, part) in body.split(",\t").enumerate() {
            let Some(col) = columns.next() else {
                return Err(BadReply::InvalidHeader(
                    "too many columns in data header".into(),
                ));
            };
            let result = f(col, part);
            if let Err(e) = result {
                return Err(BadReply::InvalidHeader(format!("col {i}: {e}")));
            }
        }
        if columns.next().is_some() {
            return Err(BadReply::InvalidHeader(
                "too few columns in data header".into(),
            ));
        }
        Ok(())
    }
}

fn decode_column_name(rendered: &str) -> RResult<Cow<'_, str>> {
    if !rendered.starts_with('"') {
        return Ok(Cow::Borrowed(rendered));
    }
    let Some(inner) = rendered
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return Err(BadReply::InvalidQuotedIdentifier);
    };
    let mut decoded = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            decoded.push(character);
            continue;
        }
        match chars.next() {
            Some(escaped @ ('"' | '\\')) => decoded.push(escaped),
            _ => return Err(BadReply::InvalidQuotedIdentifier),
        }
    }
    Ok(Cow::Owned(decoded))
}

/// Holds information about a column of a result set.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ResultColumn {
    pub(crate) table_name: String,
    pub(crate) name: String,
    pub(crate) typ: MonetType,
}

impl ResultColumn {
    pub(crate) fn empty() -> Self {
        Self::new("", MonetType::Bool)
    }

    pub(crate) fn new(name: &str, typ: MonetType) -> Self {
        ResultColumn {
            table_name: String::new(),
            name: name.into(),
            typ,
        }
    }

    /// Return the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the source table name, or an empty string for an expression.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Return the type of the column.
    pub fn sql_type(&self) -> &MonetType {
        &self.typ
    }
}

type ResultColumnUpdater<'x, 'a> =
    &'x dyn Fn(&'a mut ResultColumn, &'a str) -> Result<(), Box<dyn error::Error>>;

pub fn from_utf8<'a>(context: &'static str, bytes: &'a [u8]) -> RResult<&'a str> {
    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(_) => Err(BadReply::Unicode(context)),
    }
}

#[cfg(test)]
mod tests {
    use super::{BadReply, ReplyParser, ResultSet, response_autocommit, server_error};

    #[test]
    fn extracts_last_autocommit_status() {
        assert_eq!(response_autocommit(b"&4 f\n"), Some(false));
        assert_eq!(response_autocommit(b"&4 f\n&4 t\n"), Some(true));
        assert_eq!(response_autocommit(b"[ \"&4 f\"\t]\n"), None);
        assert_eq!(response_autocommit(b"&4 false\n"), None);
        assert!(ReplyParser::new(b"&4 false\n".to_vec()).is_err());
    }

    #[test]
    fn preserves_all_server_error_lines() {
        let response = b"&2 0\n!42000!syntax error\n!detail from optimizer\n&4 f\n";
        let error = server_error(response).unwrap();
        assert_eq!(error.sqlstate(), Some("42000"));
        assert_eq!(error.message(), "syntax error\ndetail from optimizer");
        assert_eq!(
            error.to_string(),
            "42000!syntax error\ndetail from optimizer"
        );
    }

    #[test]
    fn parses_prepare_result_header() {
        let response = concat!(
            "&5 17 1 1 1\n",
            "% .prepare # table_name\n",
            "% type # name\n",
            "% varchar # type\n",
            "% 7 # length\n",
            "% 0 0 # typesizes\n",
            "[ \"hugeint\"\t]\n"
        );
        let parser = ReplyParser::new(response.as_bytes().to_vec()).unwrap();
        let ReplyParser::Data(ResultSet {
            result_id,
            prepared,
            total_rows,
            to_close,
            ..
        }) = parser
        else {
            panic!("expected prepare result set");
        };
        assert_eq!(result_id, 17);
        assert!(prepared);
        assert_eq!(total_rows, 1);
        assert_eq!(to_close, None);
    }

    #[test]
    fn decodes_quoted_column_names() {
        let response = concat!(
            "&1 17 0 5 0\n",
            "% t,\tt,\tt,\tt,\tt # table_name\n",
            "% \"a\\\"b\",\t\"c d\",\tselect,\tMiXeD,\tä # name\n",
            "% int,\tint,\tint,\tint,\tint # type\n",
            "% 32,\t32,\t32,\t32,\t32 # length\n",
            "% 0 0,\t0 0,\t0 0,\t0 0,\t0 0 # typesizes\n",
        );
        let ReplyParser::Data(ResultSet { columns, .. }) =
            ReplyParser::new(response.as_bytes().to_vec()).unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name())
                .collect::<Vec<_>>(),
            ["a\"b", "c d", "select", "MiXeD", "ä"]
        );
    }

    #[test]
    fn parses_tuple_without_slicing_result() {
        let response = b"=project (\n=| expression\n=)\n".to_vec();
        let ReplyParser::Data(ResultSet {
            total_rows,
            rows_included,
            columns,
            mut row_set,
            ..
        }) = ReplyParser::new(response).unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(total_rows, 3);
        assert_eq!(rows_included, 3);
        assert_eq!(columns[0].name(), "rel");
        assert!(row_set.advance().unwrap());
        assert_eq!(row_set.get_field_raw(0).unwrap(), Some(&b"project ("[..]));
    }

    #[test]
    fn rejects_short_headers_without_panicking() {
        for response in ["&", "&1", "x1 2\n"] {
            let parsed = ReplyParser::new(response.as_bytes().to_vec());
            assert!(matches!(
                parsed,
                Err(BadReply::InvalidHeader(_)
                    | BadReply::UnknownResponse(_)
                    | BadReply::UnexpectedEnd
                    | BadReply::SepNotFound(_))
            ));
        }
    }

    #[test]
    fn rejects_more_included_rows_than_total_rows() {
        let response = concat!(
            "&1 17 1 1 2\n",
            "% t # table_name\n",
            "% c # name\n",
            "% int # type\n",
            "% 32 # length\n",
            "% 0 0 # typesizes\n",
            "[ 1\t]\n"
        );
        assert!(matches!(
            ReplyParser::new(response.as_bytes().to_vec()),
            Err(BadReply::TooManyIncludedRows {
                included: 2,
                total: 1
            })
        ));
    }

    #[test]
    fn affected_rows_does_not_wrap_large_server_counts() {
        let parser = ReplyParser::Data(ResultSet {
            result_id: 1,
            prepared: false,
            next_row: 0,
            total_rows: u64::MAX,
            rows_included: 0,
            columns: Vec::new(),
            row_set: super::RowSet::new(super::ReplyBuf::new(Vec::new()), 0),
            stashed: None,
            to_close: Some(1),
        });
        assert_eq!(parser.affected_rows(), None);
    }

    #[test]
    fn rejects_column_count_larger_than_reply_metadata() {
        let response = "&1 17 0 1000000 0\n";
        assert_eq!(
            ReplyParser::new(response.as_bytes().to_vec()).unwrap_err(),
            BadReply::TooManyColumns(1_000_000)
        );
    }

    #[test]
    fn rejects_column_allocation_larger_than_reply() {
        let response = format!("&1 17 0 100 0\n{}", " ".repeat(1_000));
        assert_eq!(
            ReplyParser::new(response.into_bytes()).unwrap_err(),
            BadReply::TooManyColumns(100)
        );
    }
}
