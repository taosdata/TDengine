# API feature gates

| feature         | includes                         | versions                       | removed |
| --------------- | -------------------------------- | ------------------------------ | ------- |
| tmq             | tmq API series                   | >= 3.0                         |         |
| result_block    | taos_result_block                | >= 2.4.0.4, < 3.0              |         |
| json_tag        | JSON data type support in tag    | >= 2.4.0.0                     |         |
| set_config      | taos_set_config API and types    | >= 2.4.0.0, < 3.0              |         |
| is_update_query | taos_is_update_query             | >= 2.4.0.0, < 3.0              |         |
| reset_db        | taos_reset_current_db            | >= 2.4.0.0, < 3.0              |         |
| sml             | taos_schemaless_insert and types | >= 2.4.0.0                     |         |
| parse_time      | taos_parse_time                  | >= 2.4.0.0, < 3.0              |         |
| connect_auth    | taos_connect_auth                | >= 2.2.0.2                     |         |
| stmt_full       | taos_stmt multi band APIs        | >= 2.2.0.2                     |         |
| fetch_lengths   | taos_fetch_lengths               | >= 2.2.0.2                     |         |
| insert_lines    | taos_insert_lines                | >= 2.2.0.2, < 2.4.0.0          | removed |
| stmt_core       | stmt bind api                    | >= 2.0.20.10 (win), >= 2.0.6.0 |         |
| fetch_block     | taos_fetch_block                 | >= 2.0.20.10                   |         |
| is_null         | taos_is_null                     | >= 2.0.8.2                     |         |
| basic           | 2.x basic apis                   | >= 2.0                         |         |

The latest connectors claims to support the latest version of each alive stable branches.

For example, the latest alive versions are 2.0.22.3, 2.2.2.10, 2.4.0.10, connectors will work on these versions of TDengine client. Older versions of each branches are recommended to upgrade to the latest version.
