---
title: "Dataset Filter Configuration"
sidebar_label: "Dataset Filter Configuration"
---

This page describes how to use **Dataset Filter** in PI data ingestion tasks, including the applicable scenarios, wildcard syntax, and common examples for the three filter units: `point`, `element`, and `template`.

## What Is Dataset Filter

When creating a PI or PI backfill task in taosExplorer, you can fill in the **Dataset Filter** in the **Data Model Configuration** area to filter PI Points or AF Elements before generating the model configuration file.

- To sync all points or all template elements, use the default configuration.
- To sync only points, elements, or templates whose names match specific patterns, fill in the Dataset Filter before clicking **Download Default Configuration**.

## Relationship Among System Configuration, Data Model, and Dataset Filter

The available Dataset Filter options depend on the **System Configuration** (connection mode) and **Data Model**.

| System Configuration | Data Model | Dataset Filter Unit | Description |
| --- | --- | --- | --- |
| PI Data Archive Only | Single-column model | `point` | Filter by PI Point name directly |
| PI Data Archive + AF Server | Single-column model | `element` / `template` | Filter by AF Element path or template name; each Point is still stored as a separate subtable |
| PI Data Archive + AF Server | Multi-column model | `element` / `template` | Filter by AF Element path or template name; each Element is stored as a separate subtable |

Key points:

- **PI Data Archive Only** does not support AF, so only `point` filtering is available.
- **Multi-column model** relies on the AF asset framework, so it is only available in AF Server mode.
- **Single-column model in AF Server mode** uses `element` or `template` filtering; each Point is still stored as a separate subtable. This mode does not support `point` filtering because the single-column AF mode requires the Element → Attribute context to obtain metadata such as UOM and Element ownership.

## Wildcard Syntax

All three Dataset Filter units support the following wildcards:

| Wildcard | Meaning |
| --- | --- |
| `*` | Matches 0 or more characters |
| `?` | Matches exactly 1 character |

## Point Filter

Applicable when **System Configuration = PI Data Archive Only**, filtering by PI Point name.

### Syntax

- Only name patterns are supported; path syntax is not supported.
- The expression is passed to the PI backend as-is; no `*` is appended automatically.

### Examples

| Input | Meaning |
| --- | --- |
| `sinusoid` | Exact match for the Point named `sinusoid` |
| `CDT15*` | Matches Points starting with `CDT15` |
| `sinu?` | Matches Points with `sinu` followed by any single character, e.g., `sinus` |

## Element Filter

Applicable when **System Configuration = PI Data Archive + AF Server**, filtering by AF Element name or path. It is the only filter type that **supports path matching**.

### Path Matching Rules

- Paths are written relative to the AF database root; **do not include the AF Database name**.
- Leading `\` is optional.
- The rule depends on whether the expression contains `\`:
  - **Without `\`**: the entire string is used as the element name pattern and matched across the whole database.
  - **With `\`**: split at the last `\`.
    - The left part is used as the `Root:` path (parent path).
    - The right part is used as the `Name:` pattern (empty means `*`).
    - Only leaf Elements (elements without child elements) are returned.

### Examples

| Input | Meaning |
| --- | --- |
| `Meter_100001` | Exact match for the Element named `Meter_100001` |
| `Meter_10000*` | Matches Elements starting with `Meter_10000` |
| `Meter_100000?` | Matches Elements with `Meter_100000` followed by a single character |
| `\California\San Diego\` | Matches all leaf Elements under `California\San Diego` |
| `\California\Meter_*` | Matches leaf Elements under the `California` subtree whose names start with `Meter_` |
| `\California\San Diego\Meter_10000?` | Matches leaf Elements under the specified path whose names start with `Meter_10000` and end with a single character |

### Notes

1. **When selecting a path, the path must end with `\`**. For example, `\California\San Diego` is parsed as looking for a leaf Element named `San Diego` under `California`; since `San Diego` is usually a grouping node, the result may be empty. The correct form is `\California\San Diego\`.
2. **Each segment in the path must be an exact, real node name**; path segments themselves do not support `*` or `?` wildcards.
3. **Do not include the AF Database name in the path**. Paths are relative to the database root; including the database name will cause an error.
4. **Path mode returns only leaf Elements**. PI Points attached to non-leaf grouping nodes are not collected.

## Template Filter

Applicable when **System Configuration = PI Data Archive + AF Server**, filtering by AF Element Template name.

### Syntax

- Only template name patterns are supported; path syntax is not supported.
- The expression is passed to the PI backend as-is; no `*` is appended automatically.
- After a template is matched, taosX automatically collects all Elements that use that template.

### Examples

| Input | Meaning |
| --- | --- |
| `MeterBasic` | Exact match for the template named `MeterBasic` |
| `Meter*` | Matches templates starting with `Meter` |
| `Meter?` | Matches templates with `Meter` followed by a single character |

## Relationship Between Dataset Filter and CSV Configuration File

Dataset Filter only affects the range of points or elements generated when you click **Download Default Configuration**. After downloading, the CSV model configuration file contains:

- **Multi-column model**: the `Filter` row stores the Dataset Filter value.
- **Single-column model**: specific PI Points are listed in the point mapping section; you can manually delete unwanted Point rows for further trimming.

## Common Misconceptions

| Misconception | Clarification |
| --- | --- |
| Single-column model in AF Server mode supports `point` filtering | Not supported. The single-column AF mode requires the Element → Attribute context to obtain UOM, Element ownership, and other metadata; using `point` directly would lose this metadata. |
| Path segments in Element Filter can use `*` wildcards | Not supported. `*` / `?` can only be used in the last segment (the name). |
| The AF Database name needs to be included in the path | No. Paths are relative to the database root; including the database name causes an error. |
| `point` / `template` support path syntax | Not supported. Only `element` supports path matching. |
