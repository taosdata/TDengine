# Azure VM PI Server — Setup & Demo Guide

This document describes the Azure Windows VM running PI System, used for development, testing, and demos of the PI Data In connector. It provides a step-by-step workflow to set up the environment and run a full PI → TDengine IDMP data ingestion demo.

## VM Access

| Item        | Value                                           |
| ----------- | ----------------------------------------------- |
| OS          | Windows Server 2022                             |
| SSH         | OpenSSH enabled                                 |
| TDengine    | Enterprise 3.4.1.8, installed at `C:\TDengine\` |
| Source code | `C:\workspace\taosx\`                           |

## Overview

The PI license on this VM has expired. To use PI System, the Windows clock must be set back to a date within the license validity period. After that, existing PI data (created with stale timestamps) must be cleaned. Then the PISimulator generates fresh data, TDengine ingests it via a Data In task, and optionally IDMP imports from TDengine.

**Workflow:**

1. Stop TDengine & IDMP Services
2. Fix PI License
3. Clean PI System
4. Start PISimulator
5. Start TDengine TSDB
6. Create PI Data In Task in Explorer
7. Start TDengine IDMP
8. Use EasyImport in IDMP

## Step 1: Stop TDengine & IDMP Services

Before modifying the system clock, stop TDengine and IDMP services to prevent them from writing data with incorrect timestamps.

**Stop TDengine services:**

```powershell
C:\TDengine\stop-all.bat
```

**Stop taosx-agent**

```powershell
sc.exe stop taosx-agent
```

**Stop IDMP services:**

```powershell
C:\TDengine\idmp\bin\stop-tdengine-idmp.bat
```

**Verify:**

```powershell
sc.exe query taosd
sc.exe query taosx
sc.exe query taosx-agent
sc.exe query taos-explorer
sc.exe query tdengine-idmp
# All should show STOPPED
```

## Step 2: Fix PI License (Set Windows Clock)

The PI Server license has expired. To bypass this, disable time-sync services and set the system clock to a date within the license period.

**Script:** `C:\workspace\taosx\crates\source-pi\scripts\fix_pi_time.ps1`

**What it does:**

1. Stops and disables Hyper-V time sync (`vmictimesync`) and Windows Time (`w32time`) services
2. Disables NTP and VMIC time providers in the registry
3. Sets system time to `2025-04-18 07:05:00`
4. Restarts all PI services (`PI*`) and IIS

**Run (PowerShell as Administrator):**

```powershell
powershell -ExecutionPolicy Bypass -File C:\workspace\taosx\crates\source-pi\scripts\fix_pi_time.ps1
```

**Verify:**

- `Get-Date` should show a date in April 2025
- `Get-Service PI* | Format-Table Name, Status` — all automatic PI services should be Running

> **Note:** After VM restart, Windows may resync time. Re-run this script if PI services fail with license errors.

## Step 3: Clean PI System

After time modification, clean the entire PI System before generating fresh data.

**Script:** `C:\workspace\taosx\crates\source-pi\scripts\cleanup_pi.ps1`

**What it does:**

1. Connects to PI Server `piserver` via AF SDK
2. Iteratively deletes all PI Points (loops until none remain)
3. Deletes the AF Database named `Meters`
4. Verifies cleanup: reports remaining PI Points and AF Database status

**Run (PowerShell as Administrator):**

```powershell
powershell -ExecutionPolicy Bypass -File C:\workspace\taosx\crates\source-pi\scripts\cleanup_pi.ps1
```

**Expected output:**

```
Connected to PI Server: piserver
Found N PI Points remaining
...
Total PI Points deleted: N
Deleted AF Database: Meters
Verification - Remaining PI Points: 0
Verification - AF Database Meters: DELETED
Cleanup complete!
```

## Step 4: Start PISimulator

The PISimulator generates simulated meter data and writes it to PI Server. It creates an AF Database (`Meters`), an Element Template (`MeterTemplate`), a hierarchical AF tree of meter elements, and continuously writes simulated values (Current, Voltage) to corresponding PI Points.

> **Prerequisites:** The PISimulator binary and its `AFSettings.json` configuration must be pre-built and configured before running. If you need to build or modify the PISimulator, contact the developer. Source code is at `plugins/pi/src/PISimulator/`.

**Run PISimulator (console mode):**

```powershell
C:\workspace\taosx\plugins\pi\src\PISimulator\PISimulator.Service\bin\Release\PISimulator.Service.exe
```

The simulator runs in the foreground and continuously writes data to PI Server. Press any key to stop it.

**Verify:** After starting, open PI System Explorer and confirm:

- AF Database `Meters` exists with element hierarchy
- PI Points named like `Meters_Current_0`, `Meters_Voltage_0` are being created and receiving data

## Step 5: Start TDengine TSDB

Start the TDengine database server and related services (taosx, taos-explorer, taosx-agent).

**Start all services:**

```powershell
C:\TDengine\start-all.bat

sc.exe start taosx-agent
```

**Verify:**

- taos-explorer web UI: `http://localhost:6060`
- TDengine CLI: `C:\TDengine\taos.exe`

## Step 6: Create PI Data In Task in Explorer

1. Open taos-explorer in a browser: `http://localhost:6060`
2. Log in
3. Navigate to **Data In**
4. Click **+ New Task**
5. Select data source type: **PI**
6. Configure the PI connection:
   - **PI Server**: `piserver` (local hostname)
   - **Target Database**: Create or select a database (e.g., `zyyang`)
7. The task will automatically discover PI Points and generate a CSV configuration
8. Optionally customize the CSV to rename SuperTables, columns, or subtables (see `crates/source-pi/docs/design/pi-csv.md`)
9. Start the task

**Verify:**

```sql
-- Connect to TDengine
C:\TDengine\taos.exe

-- Check data
USE <database_name>;
SHOW STABLES;
SHOW TABLES;
SELECT * FROM `<database_name>`.`<subtable_name>` LIMIT 10;
```

## Step 7: Start TDengine IDMP

```powershell
C:\TDengine\idmp\bin\start-tdengine-idmp.bat
```

**Verify:** Open `http://localhost:6042` in a browser. The IDMP web UI should load.

## Step 8: EasyImport in IDMP

Use IDMP's EasyImport feature to import PI data from TDengine TSDB:

1. Open IDMP web UI at `http://localhost:6042`
2. Navigate to **EasyImport**
3. Select the TDengine data source connection (pointing to the database used by the PI Data In task, e.g., `zyyang2`)
4. Choose the SuperTables/tables to import
5. Choose the element_path Tag as the Path to build IDMP elements tree
6. Confirm and start the import

After import completes, the PI time-series data is available in IDMP for visualization and analysis.

## Troubleshooting

### PI License Errors

If PI services fail to start with license-related errors, re-run `fix_pi_time.ps1` to reset the clock.

### PISimulator Fails to Connect

Ensure PI Data Archive and PI AF Server services are running:

```powershell
Get-Service PIArchSubsystem, AFService | Format-Table Name, Status
```

### Log Files

- TDengine logs: `C:\TDengine\log\`
- taosx logs: `C:\TDengine\log\taosx*.log`
- taosx-agent logs: `C:\TDengine\log\taosx-agent*.log`
- taos-explorer logs: `C:\TDengine\log\taos-explorer*.log`
