// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use time::UtcOffset;

/// Return the local UTC offset, falling back to UTC when the host offset cannot
/// be determined safely.
///
/// The `time` crate can refuse local-offset discovery in a multi-threaded
/// process. In that case this function logs the fallback; callers that require
/// a specific session zone should set [`Parameters::set_timezone`](crate::Parameters::set_timezone).
pub fn timezone_offset_east_of_utc() -> i32 {
    match UtcOffset::current_local_offset() {
        Ok(offset) => offset.whole_seconds(),
        Err(error) => {
            log::warn!("could not determine local UTC offset ({error}); using UTC");
            0
        }
    }
}
