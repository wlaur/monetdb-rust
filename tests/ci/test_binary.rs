// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::collections::HashMap;

use anyhow::Result;
use monetdb::Endian;

use crate::context::get_server;

#[test]
fn test_binary_result_window_and_server_info() -> Result<()> {
    let mut parameters = get_server().parms();
    parameters.set_replysize(1)?;
    let connection = monetdb::Connection::new(parameters)?;

    let info = connection.server_info()?;
    assert_eq!(info.endian, Endian::Lit);
    assert!(info.binary_level >= 1);
    assert_eq!(info.reply_size, 1);

    let mut cursor = connection.cursor();
    cursor.execute(
        "SELECT * FROM (VALUES (1, 'one'), (2, CAST(NULL AS VARCHAR(8))), (3, 'three')) AS t(i, s)",
    )?;
    let result = cursor.binary_result()?;
    assert_eq!(result.total_rows, 3);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].name(), "i");
    assert_eq!(result.columns[0].table_name(), ".t");

    let frame = cursor.fetch_binary(1, 2)?;
    assert!(
        frame.starts_with(format!("&6 {} 2 2 1\n", result.result_id).as_bytes()),
        "unexpected frame prefix: {:?}",
        &frame[..frame.len().min(80)]
    );
    assert!(frame.len() > 32);

    let error = cursor.fetch_binary(4, 1).unwrap_err();
    assert!(error.to_string().contains("exceeds result set"));
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
fn test_binary_uploads() -> Result<()> {
    let connection = get_server().connect()?;
    let mut cursor = connection.cursor();
    cursor.execute("DROP TABLE IF EXISTS adbc_binary_upload")?;
    cursor.execute("CREATE TABLE adbc_binary_upload(i INT, s VARCHAR(8))")?;

    let ints = [
        1i32.to_le_bytes(),
        2i32.to_le_bytes(),
        i32::MIN.to_le_bytes(),
    ]
    .concat();
    let strings = b"one\0two\0\x80\0".to_vec();
    let uploads = HashMap::from([("c0".into(), ints), ("c1".into(), strings)]);
    cursor.execute_with_binary_uploads(
        "COPY LITTLE ENDIAN BINARY INTO adbc_binary_upload FROM 'c0', 'c1' ON CLIENT",
        &uploads,
    )?;
    assert_eq!(cursor.affected_rows(), Some(3));

    cursor.execute("SELECT i, s FROM adbc_binary_upload ORDER BY i NULLS LAST")?;
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
    cursor.execute("DROP TABLE adbc_binary_upload")?;
    Ok(())
}
