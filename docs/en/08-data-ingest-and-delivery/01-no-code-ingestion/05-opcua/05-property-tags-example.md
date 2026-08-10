---
title: "OPC UA Property to TAG Example (limit as example)"
sidebar_label: "Property to TAG Example"
---

This page explains how to ingest OPC UA point Properties into TDengine supertable TAG columns through taosX.
It uses `limit` as an example, but the same workflow applies to other Property names.

## When to use this

- Your OPC UA server defines business metadata as Properties under Variable nodes (for example `limit`, `EngineeringUnits`, `upperBound`)
- You want taosX to map these Properties into TAG columns automatically

## Key rules

1. The attribute must be a **Property** of the target Variable (`HasProperty`), not an independent data point.
2. In taosExplorer task configuration, prefer:
   - **Node Class**: `all`
   - **Root Node ID (optional)**: set it to a parent path that includes the target Variable to narrow scan scope; if omitted, browsing usually starts from the server default root
3. Avoid name collisions between `custom_tags` and Property names.

## Example: ingest `limit` as TAG

### OPC UA node layout example

```text
Objects
└── Plant
    └── Area1
        └── Sensor01
            └── Temperature                              (Variable)
                ├── limit = "0~100"                      (Property)
                └── EngineeringUnits = "degree Celsius"  (Property)
```

### Recommended taosExplorer task settings

In **Select Data Points** mode, use:

- Root Node ID (optional): for example `ns=2;i=3` (use your actual address space; values differ across servers)
- Node Class: `all`
- Super Table Name: `opc_limit_{type}` (example only)
- Table Name: `t_{ns}_{id#/_}`
- Timestamp: `received_ts`

Fill connection/authentication fields based on your real OPC UA environment.

## Validation steps

```sql
USE <target_db>;
SHOW STABLES;
DESCRIBE <stable_name>;
```

You should see TAG columns that match the actual Property names on your server, for example:

- `limit` `VARCHAR(1024)` `TAG`
- `EngineeringUnits` `VARCHAR(1024)` `TAG`

Then verify values:

```sql
SELECT tbname, `limit`, `EngineeringUnits`
FROM <stable_name>
LIMIT 5;
```

## Is `limit` a special keyword?

No. `limit` is only an example name.
As long as the server models metadata as OPC UA Properties under the Variable, other names (for example `high_limit`, `alarmLevel`, `unit`) can be mapped to TAG columns in the same way.

## Troubleshooting

### Only `custom_tags` are present, but Property TAGs are missing

Check:

- Root Node ID is appropriate (optional, but recommended for large address spaces)
- Node Class is `all`
- The source attribute is truly modeled as a Property (not a normal Variable)

### New TAGs do not appear after config updates

If the supertable already exists with an old schema, TAG columns are not auto-backfilled.
Create the task with a new database/table name, or drop/recreate the old schema and rerun.
