// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::collections::HashMap;
use std::num::NonZeroUsize;

use anyhow::{Context, Result};
use monetdb::Endian;

use crate::context::get_server;

#[test]
fn test_binary_result_window_and_server_info() -> Result<()> {
    let mut parameters = get_server().parms();
    parameters.set_replysize(1)?;
    let connection = monetdb::Connection::new(parameters)?;

    let info = connection.server_info()?;
    assert_eq!(info.endian, Endian::Lit);
    assert_eq!(info.reply_size, 1);
    if info.binary_level < 1 {
        return Ok(());
    }

    let mut cursor = connection.cursor();
    cursor.execute(
        "SELECT * FROM (VALUES (1, 'one'), (2, CAST(NULL AS VARCHAR(8))), (3, 'three')) AS t(i, s)",
    )?;
    let result = cursor.binary_result()?;
    assert_eq!(result.total_rows, 3);
    assert_eq!(result.rows_included, 1);
    assert!(result.is_server_resident());
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].name(), "i");
    assert_eq!(result.columns[0].table_name(), ".t");

    let mut frame = Vec::new();
    cursor.fetch_binary_into(1, 2, &mut frame)?;
    assert!(
        frame.starts_with(format!("&6 {} 2 2 1\n", result.result_id).as_bytes()),
        "unexpected frame prefix: {:?}",
        &frame[..frame.len().min(80)]
    );
    assert!(frame.len() > 32);

    let capacity = frame.capacity();
    cursor.fetch_binary_into(1, 1, &mut frame)?;
    assert!(frame.capacity() >= capacity);

    let error = cursor.fetch_binary(4, 1).unwrap_err();
    assert!(error.to_string().contains("invalid binary fetch"));
    assert!(matches!(
        cursor.fetch_binary(3, 1),
        Err(monetdb::CursorError::InvalidRange { .. })
    ));
    assert!(matches!(
        cursor.fetch_binary(1, 0),
        Err(monetdb::CursorError::InvalidRange { .. })
    ));
    Ok(())
}

#[test]
fn test_binary_payload_cannot_change_cached_autocommit() -> Result<()> {
    let mut parameters = get_server().parms();
    parameters.set_replysize(1)?;
    let connection = monetdb::Connection::new(parameters)?;
    if connection.server_info()?.binary_level < 1 {
        return Ok(());
    }

    let mut cursor = connection.cursor();
    cursor.execute("SELECT s FROM (VALUES (1, 'safe'), (2, '\n&4 f')) AS t(i, s) ORDER BY i")?;
    let mut frame = Vec::new();
    cursor.fetch_binary_into(1, 1, &mut frame)?;

    assert!(
        frame.windows(4).any(|window| window == b"&4 f"),
        "frame did not contain marker: {:?}",
        String::from_utf8_lossy(&frame)
    );
    assert!(connection.server_info()?.autocommit);
    Ok(())
}

#[test]
fn test_inline_result_reports_not_resident() -> Result<()> {
    let mut parameters = get_server().parms();
    parameters.set_replysize(1)?;
    let connection = monetdb::Connection::new(parameters)?;
    if connection.server_info()?.binary_level < 1 {
        return Ok(());
    }

    let mut cursor = connection.cursor();
    cursor.execute("SELECT 42")?;
    let result = cursor.binary_result()?;
    assert_eq!(result.total_rows, 1);
    assert_eq!(result.rows_included, 1);
    assert!(!result.is_server_resident());
    assert!(matches!(
        cursor.fetch_binary(0, 1),
        Err(monetdb::CursorError::ResultNotResident {
            rows_included: 1,
            total_rows: 1
        })
    ));
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(42));
    Ok(())
}

#[test]
fn test_empty_result_reports_not_resident() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.server_info()?.binary_level < 1 {
        return Ok(());
    }

    let mut cursor = connection.cursor();
    cursor.execute("SELECT 42 WHERE FALSE")?;
    let result = cursor.binary_result()?;
    assert_eq!(result.total_rows, 0);
    assert_eq!(result.rows_included, 0);
    assert!(!result.is_server_resident());
    assert!(matches!(
        cursor.fetch_binary(0, 1),
        Err(monetdb::CursorError::ResultNotResident {
            rows_included: 0,
            total_rows: 0
        })
    ));
    Ok(())
}

#[test]
fn test_autocommit_control() -> Result<()> {
    let connection = get_server().connect()?;
    connection.set_autocommit(false)?;
    assert!(!connection.server_info()?.autocommit);
    connection.set_autocommit(true)?;
    assert!(connection.server_info()?.autocommit);
    Ok(())
}

#[test]
fn test_sql_transaction_updates_autocommit_state() -> Result<()> {
    let connection = get_server().connect()?;
    assert!(connection.server_info()?.autocommit);
    let mut cursor = connection.cursor();
    cursor.execute("START TRANSACTION")?;
    assert!(!connection.server_info()?.autocommit);
    cursor.execute("COMMIT")?;
    assert!(connection.server_info()?.autocommit);
    Ok(())
}

#[test]
fn test_client_binary_level_caps_server_capability() -> Result<()> {
    let mut parameters = get_server().parms();
    parameters.set_binary("0")?;
    let connection = monetdb::Connection::new(parameters)?;
    assert_eq!(connection.server_info()?.binary_level, 0);
    Ok(())
}

#[test]
fn test_failed_delayed_deallocate_preserves_connection() -> Result<()> {
    let connection = get_server().connect()?;
    let mut cursor = connection.cursor();
    assert!(connection.try_deallocate(999_999_999));

    cursor.execute("SELECT 42")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(42));
    Ok(())
}

#[test]
fn test_binary_uploads() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.metadata()?.version() < (11, 41, 0) {
        return Ok(());
    }
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS monetdb_rust_binary_upload")?;
    cursor.execute("CREATE TABLE monetdb_rust_binary_upload(i INT, s VARCHAR(8))")?;

    let ints = [
        1i32.to_le_bytes(),
        2i32.to_le_bytes(),
        i32::MIN.to_le_bytes(),
    ]
    .concat();
    let strings = b"one\0two\0\x80\0".to_vec();
    let uploads = HashMap::from([("c0".into(), ints), ("c1".into(), strings)]);
    cursor.execute_with_binary_uploads(
        "COPY LITTLE ENDIAN BINARY INTO monetdb_rust_binary_upload FROM 'c0', 'c1' ON CLIENT",
        &uploads,
    )?;
    assert_eq!(cursor.affected_rows(), Some(3));

    cursor.execute("SELECT i, s FROM monetdb_rust_binary_upload ORDER BY i NULLS LAST")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(1));
    assert_eq!(cursor.get_str(1)?, Some("one"));
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(2));
    assert_eq!(cursor.get_str(1)?, Some("two"));
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, None);
    assert_eq!(cursor.get_str(1)?, None);
    assert!(!cursor.next_row()?);
    cursor.execute("DROP TABLE monetdb_rust_binary_upload")?;
    Ok(())
}

#[test]
fn test_lazy_binary_uploads() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.metadata()?.version() < (11, 41, 0) {
        return Ok(());
    }
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS monetdb_rust_lazy_binary_upload")?;
    cursor.execute("CREATE TABLE monetdb_rust_lazy_binary_upload(i INT, s VARCHAR(8))")?;

    let mut requested = Vec::new();
    cursor.execute_with_binary_uploads_lazy_with_chunk_size(
        "COPY LITTLE ENDIAN BINARY INTO monetdb_rust_lazy_binary_upload FROM 'c0', 'c1' ON CLIENT",
        NonZeroUsize::new(4).unwrap(),
        |filename| {
            requested.push(filename.to_owned());
            match filename {
                "c0" => Ok([1i32.to_le_bytes(), 2i32.to_le_bytes()].concat()),
                "c1" => Ok(b"one\0two\0".to_vec()),
                _ => Err(monetdb::CursorError::FileTransfer(format!(
                    "unexpected file {filename:?}"
                ))),
            }
        },
    )?;
    assert_eq!(requested, ["c0", "c1"]);
    assert_eq!(cursor.affected_rows(), Some(2));

    cursor.execute("SELECT i, s FROM monetdb_rust_lazy_binary_upload ORDER BY i")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(1));
    assert_eq!(cursor.get_str(1)?, Some("one"));
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(2));
    assert_eq!(cursor.get_str(1)?, Some("two"));
    assert!(!cursor.next_row()?);
    cursor.execute("DROP TABLE monetdb_rust_lazy_binary_upload")?;
    Ok(())
}

#[test]
fn test_empty_binary_upload_preserves_connection() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.metadata()?.version() < (11, 41, 0) {
        return Ok(());
    }
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS monetdb_rust_empty_binary_upload")?;
    cursor.execute("CREATE TABLE monetdb_rust_empty_binary_upload(i INT)")?;

    let uploads = HashMap::from([("c0".into(), Vec::new())]);
    cursor.execute_with_binary_uploads(
        "COPY LITTLE ENDIAN BINARY INTO monetdb_rust_empty_binary_upload FROM 'c0' ON CLIENT",
        &uploads,
    )?;
    assert_eq!(cursor.affected_rows(), Some(0));

    cursor.execute("SELECT 42")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(42));
    cursor.execute("DROP TABLE monetdb_rust_empty_binary_upload")?;
    Ok(())
}

#[test]
fn test_refused_binary_upload_preserves_connection() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.metadata()?.version() < (11, 41, 0) {
        return Ok(());
    }
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS monetdb_rust_refused_binary_upload")?;
    cursor.execute("CREATE TABLE monetdb_rust_refused_binary_upload(i INT)")?;

    let error = cursor
        .execute_with_binary_uploads_lazy(
            "COPY LITTLE ENDIAN BINARY INTO monetdb_rust_refused_binary_upload FROM 'c0' ON CLIENT",
            |_| {
                Err(monetdb::CursorError::FileTransfer(
                    "intentional refusal".into(),
                ))
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("intentional refusal"));
    assert!(error.to_string().contains("server response"));

    cursor.execute("SELECT COUNT(*) FROM monetdb_rust_refused_binary_upload")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i64(0)?, Some(0));
    cursor.execute("DROP TABLE monetdb_rust_refused_binary_upload")?;
    Ok(())
}

#[test]
fn test_server_abort_during_binary_upload_preserves_connection() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.metadata()?.version() < (11, 41, 0) {
        return Ok(());
    }
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS monetdb_rust_aborted_binary_upload")?;
    cursor.execute("CREATE TABLE monetdb_rust_aborted_binary_upload(i INT)")?;

    let uploads = HashMap::from([("c0".into(), vec![0; 3])]);
    let error = cursor
        .execute_with_binary_uploads(
            "COPY LITTLE ENDIAN BINARY INTO monetdb_rust_aborted_binary_upload FROM 'c0' ON CLIENT",
            &uploads,
        )
        .unwrap_err();
    assert!(!error.to_string().is_empty());

    cursor.execute("SELECT 42")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(42));
    cursor.execute("DROP TABLE monetdb_rust_aborted_binary_upload")?;
    Ok(())
}

#[test]
fn test_raw_client_file_transfers_are_refused_without_desynchronizing() -> Result<()> {
    let connection = get_server().connect()?;
    if connection.metadata()?.version() < (11, 41, 0) {
        return Ok(());
    }
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS monetdb_rust_raw_client_copy")?;
    cursor.execute("CREATE TABLE monetdb_rust_raw_client_copy(i INT)")?;

    for statement in [
        "COPY INTO monetdb_rust_raw_client_copy FROM 'input.csv' ON CLIENT",
        "COPY SELECT 1 INTO 'output.csv' ON CLIENT",
    ] {
        let error = cursor.execute(statement).unwrap_err();
        assert!(
            error.to_string().contains("file transfer"),
            "unexpected error: {error}"
        );
        cursor.execute("SELECT 42")?;
        assert!(cursor.next_row()?);
        assert_eq!(cursor.get_i32(0)?, Some(42));
    }

    cursor.execute("DROP TABLE monetdb_rust_raw_client_copy")?;
    Ok(())
}

#[test]
fn test_explain_rows_and_connection_reuse() -> Result<()> {
    let connection = get_server().connect()?;
    let mut cursor = connection.cursor();

    cursor
        .execute("EXPLAIN SELECT 1")
        .context("executing EXPLAIN")?;
    assert_eq!(cursor.column_metadata()[0].name(), "rel");
    let mut plan = Vec::new();
    while cursor.next_row().context("reading an EXPLAIN row")? {
        plan.push(cursor.get_str(0)?.unwrap().to_owned());
    }
    assert!(plan.iter().all(|line| !line.is_empty()));
    assert!(plan.len() > 1);

    cursor
        .execute("SELECT 42")
        .context("executing the sentinel query")?;
    assert!(cursor.next_row()?);
    assert_eq!(cursor.get_i32(0)?, Some(42));
    Ok(())
}
