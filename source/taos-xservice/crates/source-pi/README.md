# source-pi

Rust-side implementation of the PI Data In connector. Launches C# connector subprocesses (`taosx-pi.exe` / `taosx-pi-backfill.exe`), receives PI data via IPC, and writes it into TDengine.

> **Platform restriction**: The PI connector runs on Windows only. On other platforms the crate compiles but returns "PI connector support only windows platform" at runtime.

## Directory Structure

```
crates/source-pi/
├── src/
│   └── lib.rs                  # Entry point: connector lifecycle management
├── docs/
│   ├── azvm-piserver.md        # Azure VM piserver demo/setup guide
│   └── design/
│       ├── pi-csv.md           # PI CSV configuration file format specification
│       └── pi-uom.md           # UOM (Unit of Measure) and SuperTable naming rules
├── scripts/                    # PI Server maintenance scripts (PowerShell)
│   ├── fix_pi_time.ps1         # Set Windows clock to bypass expired PI license
│   └── cleanup_pi.ps1          # Delete all PI Points and AF Database (Meters)
└── Cargo.toml
```

## Core Logic Distribution

This crate is thin (`src/lib.rs` ~700 lines). The core logic is spread across several modules:

| Module                          | Path                                                 | Responsibility                                                                                                                                                                     |
| ------------------------------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Connector launch & IPC**      | `crates/source-pi/src/lib.rs`                        | Spawn subprocess, receive IPC data, forward logs, manage lifecycle                                                                                                                 |
| **CSV parsing & data model**    | `taosx-core/src/plugins/runners/pi/transform/mod.rs` | CSV config parsing, `PIPointModelConfig` / `PIElementModelConfig`, SuperTable schema definitions                                                                                   |
| **Connector config generation** | `taosx-core/src/plugins/runners/pi/config.rs`        | `PiConfig` construction, DSN parsing, extract PointList/TemplateList from CSV for the C# connector                                                                                 |
| **Write-side mapping**          | `taosx-core/src/plugins/sink/lush.rs`                | `LushModelConfig`: map IPC fields to TDengine SuperTable/SubTable names via `super_table_name_mapping` (type-based) and `point_super_table_mapping` (point-based, for UOM routing) |

## Architecture Overview

```
CSV config ──→ taosx (Rust)
                 │
                 ├── Parse CSV → PIPointModelConfig / PIElementModelConfig
                 ├── Generate PiConfig (TOML) → pass to C# connector
                 ├── Build LushModelConfig
                 │     ├── super_table_name_mapping (type → SuperTable)
                 │     └── point_super_table_mapping (point_name → SuperTable, for UOM)
                 │
                 └── Spawn subprocess taosx-pi.exe
                       │
                       ├── Connect to PI Server (PI SDK / AF SDK)
                       ├── Subscribe to data changes
                       └── Send data via IPC ──→ taosx routes by point_name → writes to TDengine
```

**Key constraint**: The C# connector only receives PointList / ElementIDList / TemplateList. It is unaware of SuperTable names, SubTable patterns, or column names. All name mapping is done on the taosx Rust side.

## Data Models

Two data models are supported, controlled by the DSN parameter `model`:

- **Single-column model** (`single-column`, default): Groups PI points by data type, one SuperTable per type. IPC `using` field is a type identifier (e.g. `ts_float32`), `name` is the PI point name.
- **Multi-column model** (`multi-column`): Groups by AF Template, one SuperTable per template. IPC `using` field is the template name, `name` is the AF Element path.

## Public API

| Function                          | Purpose                                                                          |
| --------------------------------- | -------------------------------------------------------------------------------- |
| `pi_to_taos()`                    | Main entry point: launch PI/PIBackfill connector and run the data ingestion task |
| `query_data_source()`             | Query PI data source metadata (point lists, etc.)                                |
| `is_pi_valid()`                   | Validate that the PI connection is available                                     |
| `is_pi_backfill_valid()`          | Validate that the PI Backfill connection is available                            |
| `parse_query_datasource_params()` | Parse query parameters from DSN (mode, filter criteria)                          |

## Maintenance Scripts

### `scripts/fix_pi_time.ps1`

For the Azure piserver VM. Bypasses an expired PI Server license by changing the Windows system clock:

1. Disables Hyper-V time sync and Windows Time service (prevents automatic clock correction)
2. Sets system time to a date within the license validity period (hardcoded to `2025-04-18`)
3. Restarts all PI services and IIS

```powershell
# Run as Administrator on piserver
powershell -ExecutionPolicy Bypass -File fix_pi_time.ps1
```

### `scripts/cleanup_pi.ps1`

Wipes all test data on piserver to reset the test environment:

1. Connects to PI Server via AF SDK and deletes all PI Points in a loop
2. Deletes the AF Database named `Meters`
3. Prints verification results to confirm cleanup is complete

```powershell
# Run on piserver (requires PI AF SDK)
powershell -ExecutionPolicy Bypass -File cleanup_pi.ps1
```

> Both scripts must be run with Administrator privileges on piserver (Azure VM).
