---
title: "OPC UA CSV Mapping File Reference"
sidebar_label: "CSV Reference"
---

This page documents the CSV mapping file used by the taosX **OPC UA DataIn** task. It focuses on two questions that come up repeatedly:

1. What every column in the mapping CSV means, especially `point_id`, `stable`, `tbname`, and how the CSV relates to the **path** and **display name** seen in the OPC server browser.
2. How **commas** (and other reserved characters) inside an OPC node ID should be handled when the CSV is uploaded.

It applies to anyone using taosX to ingest data from an OPC UA server.

## 1. The CSV is the mapping, not the address space

It is important to draw a clear line between two different things:

### OPC server address space (browsed)

- Returned by browsing the OPC UA server (Explorer "Select Data Points" panel or the `points` command).
- Each browsed point exposes: `id`, `name`, `description`, `display_name`, `node_type`, `path`.
- `id` is the canonical OPC UA Node ID, for example `ns=3;i=1005` or `ns=2;s=Channel1.Device1.Tag1`.
- `display_name` and `path` are **human-readable** identifiers shown by the server. They help engineers locate a node, but they are **not** how the server is queried.

### taosX OPC UA mapping CSV (uploaded)

- A flat CSV that tells taosX, for every point you want to ingest:
  - which OPC node to subscribe to (`point_id`),
  - which TDengine super table / sub-table to write into (`stable`, `tbname`),
  - which timestamp / value / quality columns to use,
  - which TDengine tag values to attach.
- Only one column refers to the OPC server: **`point_id`**. The rest define the TDengine side.

:::tip
The upload CSV does **not** have a `path` column or a `display_name` column at the structural level. Path and display name live on the OPC server side; they help you decide *which* `point_id` to put in the CSV. Their values are still carried into TDengine through dedicated **tag columns** that taosX fills in for you (see Section 2.2).
:::

## 2. CSV column reference

When you select data points in Explorer and download the CSV template, taosX automatically generates a complete header based on the browse information returned by the OPC UA server. The header produced by the latest version is:

```text
No.,point_id,enabled,stable,tbname,value_col,value_transform,type,quality_col,ts_col,ts_transform,request_ts_col,request_ts_transform,received_ts_col,received_ts_transform,tag::VARCHAR(1024)::name,tag::VARCHAR(1024)::BrowseName,tag::VARCHAR(1024)::DisplayName,tag::VARCHAR(1024)::Description,tag::VARCHAR(1024)::Path
```

The downloaded CSV already contains the **BrowseName**, **DisplayName**, **Description**, and **Path** returned by the OPC server in the corresponding tag columns, plus a normalized human-readable name in the `name` column. **You usually only need to adjust `enabled`, `stable`, `tbname`, etc. as required — there is no need to fill in browse information manually.**

A real sample (excerpt from the default `taosx-opc` export):

```text
1,ns=9;s=/beijing/haidian/humidity,1,opc_{type},t_9_beijing_haidian_humidity,val,,,quality,ts,,qts,,rts,,beijing.haidian.humidity,/beijing/haidian/humidity,/beijing/haidian/humidity,,Objects./beijing./beijing/haidian./beijing/haidian/humidity
2,ns=9;s=/beijing/haidian,1,opc_object,t_9_beijing_haidian,val,,,quality,ts,,qts,,rts,,beijing.haidian,/beijing/haidian,/beijing/haidian,,Objects./beijing./beijing/haidian
3,ns=9;s=beijing,1,opc_object,t_9_beijing,val,,,quality,ts,,qts,,rts,,beijing,/beijing,/beijing,,Objects./beijing
4,i=85,1,opc_object,t_Objects,val,,,quality,ts,,qts,,rts,,Objects,Objects,Objects,,Objects
5,ns=9;s=/beijing/haidian/temperature,1,opc_{type},t_9_beijing_haidian_temperature,val,,,quality,ts,,qts,,rts,,beijing.haidian.temperature,/beijing/haidian/temperature,/beijing/haidian/temperature,,Objects./beijing./beijing/haidian./beijing/haidian/temperature
```

Notes from the sample:

- Leaf nodes that carry a value (e.g. `humidity`, `temperature`) use `stable = opc_{type}`, so they are split across super tables by data type.
- Intermediate object nodes (e.g. `/beijing`, `/beijing/haidian`) use `stable = opc_object`, grouping all object-type nodes into a single super table.
- `tbname` defaults to `t_{ns}_{id}`. Characters that are illegal in TDengine table names (such as `/`) are replaced by `_`, so the sub-table name is both legal and stable.
- Each point's `BrowseName`, `DisplayName`, and `Path` are pre-filled in the tag columns, so downstream queries can filter by readable names.

### 2.1 Column meanings

| Column | Required | Meaning |
| --- | --- | --- |
| `No.` | Optional | Row number, for human inspection only. taosX ignores its value during parsing. |
| `point_id` | Yes | The OPC UA Node ID to subscribe to. Format follows the OPC UA spec, for example `ns=9;s=/beijing/haidian/humidity` (string), `i=85` (numeric, namespace defaults to 0), `ns=2;g=09087e75-...` (GUID), `ns=2;b=base64==` (opaque). |
| `enabled` | Optional (default `1`) | `1` collect this point and create the sub-table when missing. `0` skip this point and drop the corresponding sub-table. |
| `stable` | Yes | The TDengine **super table** name. Supports the placeholder `{type}`, which is substituted by the source data type at runtime (for example `int`, `double`, `varchar`). The exported template uses `opc_{type}` for leaf nodes and `opc_object` for object nodes that have no value. |
| `tbname` | Yes | The TDengine **sub-table** name. Supports the placeholders `{ns}` (namespace part of `point_id`) and `{id}` (the value after the `i=` / `s=` / `g=` / `b=` prefix). Characters that are illegal in TDengine table names (such as `/` and `.`) are replaced by `_`, so the sub-table name is both legal and stable per point. |
| `value_col` | Optional (default `val`) | Column name in TDengine that stores the OPC value. |
| `value_transform` | Optional | Numerical expression evaluated in taosX before writing, for example `val*1.8 + 32`. See the transform expression docs. |
| `type` | Optional (default = source type) | Override the value column type. Also used to substitute `{type}` in `stable`. |
| `quality_col` | Optional | Column name that stores OPC quality. Leave empty if you do not want to keep it. |
| `ts_col`, `ts_transform` | At least one of `ts_col` / `request_ts_col` / `received_ts_col` is required | The **source** timestamp column (the timestamp produced by the OPC server) and its optional transform expression. |
| `request_ts_col`, `request_ts_transform` | Same as above | The **request** timestamp column (the time when taosX issued the read/subscribe request) and its optional transform expression. |
| `received_ts_col`, `received_ts_transform` | Same as above | The **received** timestamp column (when taosX actually received the value) and its optional transform expression. Transform expressions are typically used for time-zone correction, for example `ts + 8 * 3600 * 1000`. |
| `tag::TYPE::name` | Optional (zero or many) | Each `tag::TYPE::name` column produces a TDengine tag column. `tag` is a reserved keyword. `TYPE` is any valid TDengine tag type. `name` is the tag column name. The exported template includes five tag columns by default — `name`, `BrowseName`, `DisplayName`, `Description`, `Path` — all populated from OPC browse information and need no manual editing. |

:::note
For each row, **at least one of `ts_col`, `request_ts_col`, `received_ts_col` must be configured**. When more than one is configured, the first configured column becomes the TDengine primary key.
:::

### 2.2 Auto-generated tag columns: name / BrowseName / DisplayName / Description / Path

The five tag columns in the exported template come directly from the OPC UA browse result for each node:

| Tag column | Meaning |
| --- | --- |
| `name` | A normalized, human-readable name derived by taosX from the BrowseName (leading separators removed, levels joined with `.`). Convenient for fuzzy matching in SQL. |
| `BrowseName` | Raw BrowseName returned by the OPC UA server. |
| `DisplayName` | Raw DisplayName returned by the OPC UA server, typically the name engineers see in their OPC client. |
| `Description` | Description of the node (often empty for many servers). |
| `Path` | Browse path from the root (usually `Objects`) to the current node, joined with `.` between every BrowseName level. Useful for rebuilding an asset tree in dashboards. |

If you want to filter by area later with conditions like `WHERE Path LIKE 'Objects./beijing./beijing/haidian.%'`, **just keep the five default tag columns**. To add custom dimensions (e.g. plant code, line number), append columns following the `tag::TYPE::name` format and fill in the value for each row.

### 2.3 Recommended template patterns

| Goal | Pattern | Notes |
| --- | --- | --- |
| One super table per data type | `stable = opc_{type}` | Generates `opc_int`, `opc_double`, `opc_bool`, etc. Avoids type conflicts. The exported template uses this for leaf nodes by default. |
| Dedicated super table for object nodes | `stable = opc_object` | OPC object nodes carry no value; the exported template groups them into `opc_object` for easier management. |
| Deterministic sub-table per point | `tbname = t_{ns}_{id}` | Same point always lands in the same sub-table across restarts; illegal characters are auto-replaced with `_`. |
| Keep / extend tag columns | Default 5 tag columns + custom `tag::TYPE::name` | `name` / `DisplayName` provide readable names; `Path` provides hierarchy. Add business dimensions (site, line, etc.) as needed. |

## 3. Commas inside OPC node IDs

The taosX CSV parser uses the standard CSV format with `,` as the field delimiter and `"` as the quote character. The same rules as RFC 4180 apply. OPC UA Node IDs commonly contain `;`, `=`, and `.`, none of which conflict with CSV. **However, string Node IDs are allowed to contain commas**, for example:

```text
ns=2;s=Site,Plant,Tag-01
```

If you place such a Node ID into a CSV cell as-is, the parser will split the cell at every comma and the row will have the wrong number of columns.

### 3.1 Correct way to handle commas

**Wrap the entire `point_id` cell in double quotes**:

```text
point_id,enabled,stable,tbname,value_col,type,quality_col,ts_col,received_ts_col
"ns=2;s=Site,Plant,Tag-01",1,opc_{type},t_{ns},val,double,quality,ts,rts
```

The parser interprets everything between the outer `"` characters as a single field. Commas inside the quotes are treated as literal characters and are passed through to taosX exactly as written.

### 3.2 Embedded double quotes

If a Node ID itself contains a `"` character, double the quote, again per RFC 4180:

```text
"ns=2;s=He said ""hi""",1,opc_{type},t_{ns},val,double,quality,ts,rts
```

The parser will deliver `ns=2;s=He said "hi"` to taosX as the `point_id`.

### 3.3 What you do **not** need to do

- Do **not** URL-encode the Node ID.
- Do **not** replace `,` with `\,` — backslash escaping is not used by the parser.
- Do **not** change the CSV delimiter to `;` or `\t`. The parser is fixed to `,`.
- Do **not** double-quote the entire row. Only quote the cells that need it.

### 3.4 Practical recommendation

Always double-quote `point_id` when it is a **string Node ID** (`s=...`). It costs nothing, and it is the only safe way to handle any reserved character (commas today, possibly other characters tomorrow). For numeric Node IDs (`i=...`) and GUID Node IDs (`g=...`) quoting is not required, because they cannot contain commas.

A convenient rule of thumb:

:::tip
**Quote every `point_id` that contains `s=`.** Numeric (`i=`) IDs do not need quoting. This makes the upload CSV safe regardless of the tag naming conventions used on the OPC server.
:::

## 4. Quick checklist

- Browse the OPC server in the Explorer "Select Data Points" panel (or with the `points` command) and select the points you want to ingest.
- Download the CSV template. The template already has the correct header and pre-fills `BrowseName`, `DisplayName`, `Path` and other browse information into the corresponding tag columns. You usually only need to adjust:
  - `enabled` (set to `0` for points you do not want)
  - `stable`: defaults of `opc_{type}` / `opc_object` cover most cases
  - `tbname`: default `t_{ns}_{id}` already produces stable, legal sub-table names
  - Keep at least one of `ts_col` / `request_ts_col` / `received_ts_col`
  - Append `tag::TYPE::name` columns if you need extra business dimensions in TDengine
- Always wrap string Node IDs (`"ns=2;s=..."`) in double quotes, especially when they may contain commas.
- Save the file as **UTF-8** (with or without BOM). Other encodings such as GBK are rejected.
- Upload the file in Explorer. The backend automatically validates the CSV; any parse error is reported in Explorer immediately, before the task is started.
