// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use crate::cursor::replies::BadReply;

use super::replies::{RResult, ReplyBuf};

#[derive(Debug)]
pub struct RowSet {
    buf: ReplyBuf,
    ncols: usize,
    fields: Vec<Option<(usize, usize)>>,
    noslice: bool,
}

// [ 1,→"one"→]↵
// [ 42,→"forty-two"→]↵
// [ -1,→"a\\\"b"→]↵

impl RowSet {
    pub fn new(buf: ReplyBuf, ncols: usize) -> Self {
        let fields = vec![None; ncols];
        RowSet {
            buf,
            ncols,
            fields,
            noslice: false,
        }
    }

    pub(super) fn new_noslice(buf: ReplyBuf) -> Self {
        RowSet {
            buf,
            ncols: 1,
            fields: vec![None],
            noslice: true,
        }
    }

    pub fn advance(&mut self) -> RResult<bool> {
        let ret = self.do_advance();
        if ret.is_err() {
            self.fields.clear();
        }
        ret
    }

    pub(super) fn has_pending_row(&self) -> bool {
        self.buf.peek().starts_with(b"[")
    }

    fn do_advance(&mut self) -> RResult<bool> {
        if self.noslice {
            if !self.buf.peek().starts_with(b"=") {
                self.fields.fill(None);
                return Ok(false);
            }
            self.buf.consume(1)?;
            let start = self.buf.position();
            let value = self.buf.split(b'\n')?;
            self.fields[0] = Some((start, value.len()));
            return Ok(true);
        }
        if !self.buf.peek().starts_with(b"[") {
            self.fields.fill(None);
            return Ok(false);
        }
        if self.buf.peek().len() < 2 {
            return Err(BadReply::UnexpectedEnd);
        }
        if !self.buf.peek().starts_with(b"[ ") {
            return Err(BadReply::UnexpectedHeader(self.buf.peek().into()));
        }
        self.buf.consume(2)?;
        for (i, field) in self.fields.iter_mut().enumerate() {
            let comma_skip = (i + 1 < self.ncols) as usize;
            let Some(first) = self.buf.peek().first() else {
                return Err(BadReply::UnexpectedEnd);
            };
            match first {
                b']' => {
                    return Err(BadReply::TooFewColumns(i));
                }
                b'"' => {
                    // skip it
                    self.buf.consume(1)?;
                    let Some((pos, char)) = self.buf.find2(b'"', b'\\') else {
                        return Err(BadReply::UnexpectedEnd);
                    };
                    if char == b'"' {
                        // no backslashes
                        *field = Some((self.buf.position(), pos));
                        // skip the data and closing quote
                        self.buf.consume(pos + 1)?;
                        Self::consume_field_separator(&mut self.buf, comma_skip != 0)?;
                    } else {
                        let start = self.buf.position();
                        let unescaped = self.buf.convert_backslashes(pos)?;
                        *field = Some((start, unescaped.len()));
                        // buf has already skipped the closing quote
                        Self::consume_field_separator(&mut self.buf, comma_skip != 0)?;
                    }
                }
                _ => {
                    let start = self.buf.position();
                    let rough: &[u8] = self.buf.split(b'\t')?;
                    let adjusted = if comma_skip == 0 {
                        rough
                    } else {
                        let Some(adjusted) = rough.strip_suffix(b",") else {
                            return if rough.is_empty() {
                                Err(BadReply::UnexpectedEnd)
                            } else {
                                Err(BadReply::SepNotFound(b','))
                            };
                        };
                        adjusted
                    };
                    *field = if adjusted == b"NULL" {
                        None
                    } else {
                        Some((start, adjusted.len()))
                    };
                }
            }
        }

        // now we should be looking at the trailing ]
        if self.buf.peek().len() < 2 {
            return Err(BadReply::UnexpectedEnd);
        }
        if !self.buf.peek().starts_with(b"]\n") {
            return Err(BadReply::SepNotFound(b']'));
        }
        self.buf.consume(2)?;
        Ok(true)
    }

    fn consume_field_separator(buf: &mut ReplyBuf, comma: bool) -> RResult<()> {
        let expected: &[u8] = if comma { b",\t" } else { b"\t" };
        if buf.peek().len() < expected.len() {
            return Err(BadReply::UnexpectedEnd);
        }
        if !buf.peek().starts_with(expected) {
            return Err(BadReply::SepNotFound(if comma { b',' } else { b'\t' }));
        }
        buf.consume(expected.len())?;
        Ok(())
    }

    pub fn finish(mut self) -> RResult<ReplyBuf> {
        let boundary = [b'&', b'=']
            .into_iter()
            .filter_map(|first| self.buf.find_line(first))
            .min();
        if let Some(idx) = boundary {
            self.buf.consume(idx)?;
        } else {
            self.buf.consume(self.buf.peek().len())?;
        }
        Ok(self.buf)
    }

    pub fn get_field_raw(&self, idx: usize) -> RResult<Option<&[u8]>> {
        let field = *self
            .fields
            .get(idx)
            .ok_or(BadReply::ColumnIndexOutOfBounds(idx, self.fields.len()))?;
        // NULL -> None
        let Some(field) = field else {
            return Ok(None);
        };
        Ok(Some(self.buf.range(field.0, field.1)?))
    }

    #[cfg(test)]
    fn get_str(&self, idx: usize) -> RResult<Option<&str>> {
        let Some(bytes) = self.get_field_raw(idx)? else {
            return Ok(None);
        };
        let str = std::str::from_utf8(bytes).unwrap();
        Ok(Some(str))
    }
}

#[test]
fn test_rowset_unquoted() {
    let testdata = "[ 11,\tNULL,\t33\t]\n";
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 3);

    assert_eq!(rs.get_str(0), Ok(None));
    assert_eq!(rs.get_str(1), Ok(None));
    assert_eq!(rs.get_str(2), Ok(None));
    assert_eq!(rs.get_str(3), Err(BadReply::ColumnIndexOutOfBounds(3, 3)));

    let have_row = rs.advance().unwrap();
    assert!(have_row);

    assert_eq!(rs.get_str(0), Ok(Some("11")));
    assert_eq!(rs.get_str(1), Ok(None)); // was NULL
    assert_eq!(rs.get_str(2), Ok(Some("33")));
    assert_eq!(rs.get_str(3), Err(BadReply::ColumnIndexOutOfBounds(3, 3)));

    let have_row = rs.advance().unwrap();
    assert!(!have_row);
}

#[test]
fn test_rowset_quoted() {
    let testdata = "[ \"\",\t\"MonetDB\",\t\"NULL\"\t]\n";
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 3);

    let have_row = rs.advance().unwrap();
    assert!(have_row);

    assert_eq!(rs.get_str(0), Ok(Some("")));
    assert_eq!(rs.get_str(1), Ok(Some("MonetDB")));
    assert_eq!(rs.get_str(2), Ok(Some("NULL")));
    assert_eq!(rs.get_str(3), Err(BadReply::ColumnIndexOutOfBounds(3, 3)));

    let have_row = rs.advance().unwrap();
    assert!(!have_row);

    let testdata = "[ \"mon\\\"etdb\",\tNULL\t]\n";
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 2);
    assert_eq!(rs.advance(), Ok(true));

    assert_eq!(rs.get_str(0), Ok(Some(r##"mon"etdb"##)));
    assert_eq!(rs.get_str(1), Ok(None));
}

#[test]
fn malformed_rows_return_errors_instead_of_panicking() {
    let cases = [
        ("[", 1, BadReply::UnexpectedEnd),
        ("[ \t]\n", 2, BadReply::UnexpectedEnd),
        ("[ \"value\"", 1, BadReply::UnexpectedEnd),
    ];

    for (input, columns, expected) in cases {
        let mut row_set = RowSet::new(ReplyBuf::new(input.into()), columns);
        assert_eq!(row_set.advance(), Err(expected), "input {input:?}");
    }
}

#[test]
fn test_tuple_without_slicing_rows() {
    let mut rows = RowSet::new_noslice(ReplyBuf::new(b"=project (\n=| expression\n=)\n".to_vec()));

    assert_eq!(rows.advance(), Ok(true));
    assert_eq!(rows.get_str(0), Ok(Some("project (")));
    assert_eq!(rows.advance(), Ok(true));
    assert_eq!(rows.get_str(0), Ok(Some("| expression")));
    assert_eq!(rows.advance(), Ok(true));
    assert_eq!(rows.get_str(0), Ok(Some(")")));
    assert_eq!(rows.advance(), Ok(false));
}

#[test]
fn test_rowset_escaped_strings() {
    use std::fmt::Write;

    fn escape(s: &str) -> String {
        let mut answer = String::new();
        answer.push('"');
        for &b in s.as_bytes() {
            match b {
                b'\t' => write!(answer, "\\t").unwrap(),
                b'\n' => write!(answer, "\\n").unwrap(),
                b'\r' => write!(answer, "\\r").unwrap(),
                b'\\' => write!(answer, "\\\\").unwrap(),
                b'"' => write!(answer, "\\\"").unwrap(),
                ..=31 | 127.. => write!(answer, "\\{b:03o}").unwrap(),
                _ => answer.push(b as char),
            }
        }
        answer.push('"');
        answer
    }

    let expected = [
        ["", "FOO", "TAB\tTAB"],
        ["CR\rLF\n", "FF\u{C}", "BACK\\SLASH"],
        ["DOUBLE\"QUOTE", "B\u{c4}NANA", "SMILEY\u{263A}SMILEY"],
    ];

    let mut testdata = String::new();
    for row in expected {
        write!(testdata, "[ ").unwrap();
        for (i, field) in row.iter().enumerate() {
            testdata.push_str(&escape(field));
            if i + 1 < row.len() {
                testdata.push(',');
            }
            testdata.push('\t');
        }
        testdata.push_str("]\n");
    }

    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 3);

    for (row_nr, expected_row) in expected.iter().enumerate() {
        let advance = rs.advance();
        assert_eq!(advance, Ok(true), "advancing to row {row_nr}");
        for (col_nr, &expected_field) in expected_row.iter().enumerate() {
            let field = rs.get_str(col_nr);
            assert_eq!(field, Ok(Some(expected_field)), "row {row_nr} col {col_nr}");
        }
    }
    assert!(!rs.advance().unwrap());
}

#[test]
fn test_single_column() {
    // multiple types in one column shouldn't happen but we're
    // not going to notice that here
    let testdata = "[ 1\t]\n[ NULL\t]\n[ \"foo\\\"bar\"\t]\n";
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 1);

    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.get_str(0), Ok(Some("1")));

    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.get_str(0), Ok(None));

    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.get_str(0), Ok(Some(r#"    foo"bar     "#.trim())));

    assert_eq!(rs.advance(), Ok(false));
}

#[test]
fn test_finish() {
    use bstr::BStr;
    let testdata = "[ 1,\t2\t]\n[ 3,\t4\t]\n[ 5,\t6\t]\n&lalala\n";

    // .finish() works after we've consumed three rows
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 2);
    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.get_str(0), Ok(Some("1")));
    assert_eq!(rs.get_str(1), Ok(Some("2")));
    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.get_str(0), Ok(Some("3")));
    assert_eq!(rs.get_str(1), Ok(Some("4")));
    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.get_str(0), Ok(Some("5")));
    assert_eq!(rs.get_str(1), Ok(Some("6")));
    let buf = rs.finish().unwrap();
    assert_eq!(BStr::new(buf.peek()), BStr::new("&lalala\n"));

    // .finish() works after we've consumed only two rows
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 2);
    assert_eq!(rs.advance(), Ok(true));
    assert_eq!(rs.advance(), Ok(true));
    let buf = rs.finish().unwrap();
    assert_eq!(BStr::new(buf.peek()), BStr::new("&lalala\n"));

    // .finish() works after we've consumed only one rows
    let mut rs = RowSet::new(ReplyBuf::new(testdata.into()), 2);
    assert_eq!(rs.advance(), Ok(true));
    let buf = rs.finish().unwrap();
    assert_eq!(BStr::new(buf.peek()), BStr::new("&lalala\n"));

    // .finish() works after we've consumed no rows at all
    let rs = RowSet::new(ReplyBuf::new(testdata.into()), 2);
    let buf = rs.finish().unwrap();
    assert_eq!(BStr::new(buf.peek()), BStr::new("&lalala\n"));

    let testdata = "[ 1\t]\n[ 2\t]\n=plan row\n";
    let rs = RowSet::new(ReplyBuf::new(testdata.into()), 1);
    let buf = rs.finish().unwrap();
    assert_eq!(BStr::new(buf.peek()), BStr::new("=plan row\n"));
}
