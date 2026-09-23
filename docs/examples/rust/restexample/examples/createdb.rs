// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ANCHOR: create_db_and_table

    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

    // create database and use it
    match taos
        .exec_many(["CREATE DATABASE IF NOT EXISTS power"])
        .await
    {
        Ok(afffected_rows) => println!(
            "Create database power successfully, rowsAffected: {}",
            afffected_rows
        ),
        Err(err) => {
            eprintln!("Failed to create database power, ErrMessage: {}", err);
            return Err(err.into());
        }
    }

    // create super table
    match taos.exec_many([
        "CREATE STABLE IF NOT EXISTS power.meters (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) \
        TAGS (`groupid` INT, `location` BINARY(24))",
    ]).await {
        Ok(afffected_rows) => println!("Create stable power.meters successfully, rowsAffected: {}", afffected_rows),
        Err(err) => {
            eprintln!("Failed to create stable power.meters, ErrMessage: {}", err);
            return Err(err.into());
        }
    }

    // ANCHOR_END: create_db_and_table

    Ok(())
}
