// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

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
