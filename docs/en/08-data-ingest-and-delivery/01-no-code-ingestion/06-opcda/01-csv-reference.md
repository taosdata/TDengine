---
title: OPC DA CSV Configuration File Reference
sidebar_label: CSV Reference
---

This page describes the complete rules for configuring points through a CSV file in an OPC DA data ingestion task. The CSV file defines the mapping from points to TDengine supertables and subtables. For how to upload the CSV when creating a task, see [OPC DA](./index.md#upload-csv-configuration-file).

## File Encoding

The encoding format of the uploaded CSV file must be one of the following:

1. UTF-8 with BOM
2. UTF-8 (i.e., UTF-8 without BOM)

## Header Configuration Rules

The header is the first line of the CSV file, with the following rules:

(1) The header of the CSV can configure the following columns:

| No. | Column Name | Description | Required | Default Behavior |
|----|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | tag_name | The id of the data point on the OPC DA server | Yes | None |
| 2 | stable | The supertable in TDengine corresponding to the data point | Yes | None |
| 3 | tbname | The subtable in TDengine corresponding to the data point | Yes | None |
| 4 | enable | Whether to collect data from this point | No | Use a unified default value `1` for enable |
| 5 | value_col | The column name in TDengine corresponding to the collected value of the data point | No | Use a unified default value `val` as the value_col |
| 6 | value_transform | The transform function executed in taosX for the collected value of the data point | No | Do not perform a transform on the collected value |
| 7 | type | The data type of the collected value of the data point | No | Use the original type of the collected value as the data type in TDengine |
| 8 | quality_col | The column name in TDengine corresponding to the quality of the collected value | No | Do not add a quality column in TDengine |
| 9 | ts_col | The timestamp column in TDengine corresponding to the original timestamp of the data point | No | Among `ts_col`, `request_ts_col`, and `received_ts_col`, when 2 or more columns exist, the leftmost column is used as the primary key in TDengine |
| 10 | request_ts_col | The timestamp column in TDengine corresponding to the timestamp when the data point value was requested | No | Same as above |
| 11 | received_ts_col | The timestamp column in TDengine corresponding to the timestamp when the data point value was received | No | Same as above |
| 12 | ts_transform | The transform function executed in taosX for the original timestamp of the data point | No | Do not perform a transform on the original timestamp of the data point |
| 13 | request_ts_transform | The transform function executed in taosX for the request timestamp of the data point | No | Do not perform a transform on the request timestamp of the data point |
| 14 | received_ts_transform | The transform function executed in taosX for the received timestamp of the data point | No | Do not perform a transform on the received timestamp of the data point |
| 15 | tag::VARCHAR(200)::name | The Tag column in TDengine corresponding to the data point. Where `tag` is a reserved keyword, indicating that this column is a tag column; `VARCHAR(200)` indicates the type of this tag, which can also be other legal types; `name` is the actual name of this tag. | No | If configuring more than one tag column, use the configured tag columns; if no tag columns are configured, and stable exists in TDengine, use the tags of the stable in TDengine; if no tag columns are configured, and stable does not exist in TDengine, automatically add the following two tag columns by default: `tag::VARCHAR(256)::point_id`, `tag::VARCHAR(256)::point_name` |

(2) In the CSV Header, there cannot be duplicate columns;

(3) In the CSV Header, columns like `tag::VARCHAR(200)::name` can be configured multiple times, corresponding to multiple Tags in TDengine, but the names of the Tags cannot be duplicated.

(4) In the CSV Header, the order of columns does not affect the CSV file validation rules;

(5) In the CSV Header, columns that are not listed in the table above, such as: serial number, will be automatically ignored.

## Row Configuration Rules

Each Row in the CSV file configures an OPC data point. The rules for Rows are as follows:

(1) Correspondence with columns in the Header

| No. | Column in Header | Type of Value | Range of Values | Mandatory | Default Value |
|----|-------------------------| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --------- | ------------------------ |
| 1 | tag_name | String | Strings like `root.parent.temperature`, must meet the OPC DA ID specification | Yes | |
| 2 | enable | int | 0: Do not collect this point, and delete the corresponding subtable in TDengine before the OPC DataIn task starts; 1: Collect this point, do not delete the subtable before the OPC DataIn task starts. | No | 1 |
| 3 | stable | String | Any string that meets the TDengine supertable naming convention; if there are special characters `.`, replace with underscore; if `{type}` exists: if type in CSV file is not empty, replace with the value of type; if empty, replace with the original type of the collected value | Yes | |
| 4 | tbname | String | Any string that meets the TDengine subtable naming convention; for OPC UA: if `{ns}` exists, replace with ns from point_id; if `{id}` exists, replace with id from point_id; for OPC DA: if `{tag_name}` exists, replace with tag_name | Yes | |
| 5 | value_col | String | Column name that meets TDengine naming convention | No | val |
| 6 | value_transform | String | Computation expressions supported by the Rhai engine, such as: `(val + 10) / 1000 * 2.0`, `log(val) + 10`, etc. | No | None |
| 7 | type | String | Supported types include: `b`/`bool`, `i8`/`tinyint`, `i16`/`smallint`, `i32`/`int`, `i64`/`bigint`, `u8`/`tinyint unsigned`, `u16`/`smallint unsigned`, `u32`/`int unsigned`, `u64`/`bigint unsigned`, `f32`/`float`, `f64`/`double`, `timestamp`/`timestamp(ms)`/`timestamp(us)`/`timestamp(ns)`, `json` | No | Original type of data point value |
| 8 | quality_col | String | Column name that meets TDengine naming convention | No | None |
| 9 | ts_col | String | Column name that meets TDengine naming convention | No | ts |
| 10 | request_ts_col | String | Column name that meets TDengine naming convention | No | qts |
| 11 | received_ts_col | String | Column name that meets TDengine naming convention | No | rts |
| 12 | ts_transform | String | Supports +, -, *, /, % operators, for example: `ts / 1000* 1000` sets the last 3 digits of a ms-unit timestamp to 0; `ts + 8 *3600* 1000` adds 8 hours to a ms-precision timestamp; `ts - 8 *3600* 1000` subtracts 8 hours from a ms-precision timestamp | No | None |
| 13 | request_ts_transform | String | Supports +, -, *, /, % operators, for example: `ts / 1000* 1000` sets the last 3 digits of a ms-unit timestamp to 0; `qts + 8 *3600* 1000` adds 8 hours to a ms-precision timestamp; `qts - 8 *3600* 1000` subtracts 8 hours from a ms-precision timestamp | No | None |
| 14 | received_ts_transform | String | Supports +, -, *, /, % operators, for example: `ts / 1000* 1000` sets the last 3 digits of a ms-unit timestamp to 0; `rts + 8 *3600* 1000` adds 8 hours to a ms-precision timestamp; `rts - 8 *3600* 1000` subtracts 8 hours from a ms-precision timestamp | No | None |
| 15 | tag::VARCHAR(200)::name | String | The value in tag; when the tag type is VARCHAR, it can be in Chinese | No | NULL |

(2) `tag_name` is unique throughout the DataIn task, that is: in an OPC DataIn task, a data point can only be written to one subtable in TDengine. If you need to write a data point to multiple subtables, you need to create multiple OPC DataIn tasks;

(3) When `tag_name` is different but `tbname` is the same, `value_col` must be different. This configuration allows data from multiple data points of different types to be written to different columns in the same subtable. This corresponds to the "OPC data into TDengine wide table" scenario.

## Other Rules

(1) If the number of columns in Header and Row are not consistent, validation fails, and the user is prompted with the line number that does not meet the requirements;

(2) Header is on the first line and cannot be empty;

(3) Row must have more than 1 line;

## Related Documents

- [OPC DA](./index.md): complete steps for creating an OPC DA data ingestion task
- [OPC UA CSV Mapping Reference](../05-opcua/02-csv-reference.md): column definitions for the OPC UA point CSV (including `{ns}`/`{id}` placeholders)
