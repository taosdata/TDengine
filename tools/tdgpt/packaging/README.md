# TDgpt Windows Packaging

> Language: [English](README.md) | [中文](README-CN.md)

This directory contains the Windows packaging flow for TDgpt (TDengine Analytics Node). The main entry is `win_release.py`, which stages files and builds an Inno Setup installer.

## Scope

The current Windows delivery keeps only two install paths:

- Base installer + online install
- Base installer + external offline tar package

The old `full-offline` all-in-one installer mode has been removed.

## Main Files

```text
packaging/
├── win_release.py              # Main Windows packaging entry
├── installer/
│   ├── tdgpt.iss               # Inno Setup template
│   └── taosanode-service.xml   # WinSW service template
├── bin/
│   ├── WinSW.exe               # Optional cached WinSW binary for packaging
│   ├── uv.exe                  # Optional uv binary used by offline asset bundling
│   └── README.md               # Binary cache notes
├── README.md
└── README-CN.md

script/
├── install.py                  # Windows install logic
├── uninstall.py                # Windows uninstall logic
└── taosanode_service.py        # Service and model manager
```

## Build Host Prerequisites

Before packaging, verify the following prerequisites on the build host.

### 1. Base Installer Packaging Prerequisites

1. Python `3.10`, `3.11`, or `3.12` is installed and directly available from `PATH`
2. Inno Setup 6 is installed
3. The source tree is complete, including at least:
   - `packaging/win_release.py`
   - `packaging/installer/tdgpt.iss`
   - `script/install.py`
   - `script/uninstall.py`
   - `script/taosanode_service.py`
4. If you want to bundle model archives into the base installer, prepare a model archive directory in advance, for example:
   - `tdtsfm.tar.gz`
   - `timemoe.tar.gz`
   - `moirai.tar.gz`
   - `chronos.tar.gz`
   - `timesfm.tar.gz`
   - `moment-large.tar.gz`

### 2. Additional Prerequisites For Offline Tar Packaging

If you also want to generate the external offline tar bundle, prepare:

1. one main venv directory
2. zero or more model venv directories
3. one Python runtime source, using either:
   - an existing Python runtime directory containing `python.exe`
   - `packaging/bin/uv.exe`, or a path passed with `--uv-exe`
4. an optional seed package:
   - to carry existing offline model payloads
   - optionally to carry model venv payloads as well

### 3. Optional Local Binary Cache

1. `packaging/bin/WinSW.exe`
   - reused by base installer packaging as the local WinSW cache
   - if missing, the script tries to download it from GitHub
2. `packaging/bin/uv.exe`
   - reused by offline tar packaging to prepare a Python runtime automatically
   - not required when `--python-runtime-dir` is provided

### 4. Tooling Notes

- `ISCC.exe` defaults to `C:\Program Files (x86)\Inno Setup 6\ISCC.exe`.
- You can override it with `--iscc-path`.
- `win_release.py` does not package Python runtime or virtual environments into the base installer.
- `build_offline_assets.py` prefers `packaging/bin/uv.exe` when `--python-runtime-dir` is not provided.

## Target Machine Requirements

The generated installer has these runtime expectations:

- Windows 10 (version 1803 or later) or Windows Server 2019+
- Microsoft Visual C++ Redistributable x64 must be installed
- For online first install, the target machine needs Python `3.10` / `3.11` / `3.12` in `PATH`
- For offline install, system Python is **not** required — the installer bootstraps Python from the offline tar using the built-in `tar.exe` (available since Windows 10 1803)

## What The Base Installer Contains

Always included:

- `cfg/`
- `lib/`
- `resource/`
- `requirements/`
- `bin/`
- `install.py`, `install.bat`
- `uninstall.py`, `uninstall.bat`
- WinSW executable and XML
- package metadata

Optional:

- packaged model archives copied from `--model-dir` into `<install_dir>\model\`

Not included:

- `python/runtime`
- extracted virtual environments
- extracted model directories

## Usage

### Basic Commands

```bash
# Community edition
python packaging/win_release.py -e community -v 3.4.1.0.0325

# Enterprise edition
python packaging/win_release.py -e enterprise -v 3.4.1.0.0325

# Bundle model archives from a directory
python packaging/win_release.py -e community -v 3.4.1.0.0325 -m D:\models

# Bundle all recognized model archives from a directory
python packaging/win_release.py -e community -v 3.4.1.0.0325 -m D:\models -a

# Custom output directory
python packaging/win_release.py -e community -v 3.4.1.0.0325 -o D:\tdgpt-release\20260325-r7
```

### Build External Offline Tar Bundle

Use `build_offline_assets.py` to generate the external offline tar consumed by Windows offline install.

Required inputs:

- one main venv directory
- optional extra model venv directories
- one Python runtime directory, or `uv.exe` so the script can prepare one automatically
- optional seed package containing offline model payloads

Typical command:

```bash
python packaging/build_offline_assets.py ^
  --output-file D:\offline-tar\tdgpt-offline-full-bundle-win-x64.tar ^
  --seed-package D:\offline-seed\tdgpt-model-seed.tar ^
  --python-runtime-dir C:\TDengine\python311 ^
  --main-venv-dir C:\TDengine\taosanode\venvs\venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\moirai_venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\chronos_venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\timesfm_venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\momentfm_venv
```

If you do not want to prepare a Python runtime directory in advance:

```bash
python packaging/build_offline_assets.py ^
  --output-file D:\offline-tar\tdgpt-offline-full-bundle-win-x64.tar ^
  --main-venv-dir C:\TDengine\taosanode\venvs\venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\moirai_venv ^
  --uv-exe packaging\bin\uv.exe ^
  --python-version 3.11
```

What the tar contains:

- `python/runtime/`
- `venvs/venv/`
- `venvs/<extra_venv>/`
- model payloads copied from the optional `--seed-package`
- `offline-assets-manifest.txt`

Recommended flow:

1. Build the base installer with `win_release.py`
2. Build the external offline tar with `build_offline_assets.py`
3. Deliver both files together
4. In Windows setup, choose `Offline package` and select the generated tar

### Parameters

| Parameter | Short | Description |
| --- | --- | --- |
| `--edition` | `-e` | `community` or `enterprise` |
| `--version` | `-v` | Package version, for example `3.4.1.0.0325` |
| `--model-dir` | `-m` | Optional directory containing model archives to copy into the installer payload |
| `--all-models` | `-a` | When `--model-dir` is provided, copy all recognized model archives |
| `--output` | `-o` | Output directory, default `D:\tdgpt-release` |
| `--iscc-path` |  | Override Inno Setup compiler path |
| `--skip-model-check` |  | Legacy compatibility flag; model archive validation is no longer required by the base installer |

### Offline Tar Parameters

`build_offline_assets.py` supports these main parameters:

| Parameter | Description |
| --- | --- |
| `--output-file` | Output tar path |
| `--seed-package` | Optional seed tar containing offline model payloads and optional model venv payloads |
| `--python-runtime-dir` | Existing Python runtime directory containing `python.exe` |
| `--main-venv-dir` | Main taosanode venv, packaged as `venvs/venv` |
| `--extra-venv-dir` | Extra model venv directory, repeat as needed |
| `--uv-exe` | Optional `uv.exe` path used when `--python-runtime-dir` is omitted |
| `--python-version` | Python version used by `uv`, default `3.11` |

### Model Archive Notes

If `--model-dir` is provided, `win_release.py` copies recognized archives such as:

- `tdtsfm.tar.gz`
- `timemoe.tar.gz`
- `moirai.tar.gz`
- `chronos.tar.gz`
- `timesfm.tar.gz`
- `moment-large.tar.gz`

These archives are optional. The installer can still be built without bundled model archives.

## Output

The generated installer name is:

```text
tdengine-tdgpt-oss-<version>-Windows-x64.exe
tdengine-tdgpt-enterprise-<version>-Windows-x64.exe
```

The package name no longer includes `-base`.

## Silent and Command-Line Installation

The installer supports fully silent (unattended) installation, which is useful for scripted deployments and All-in-One packaging.

### Silent Offline Install (No System Python Required)

```bat
tdgpt-setup.exe /VERYSILENT /NORESTART /OFFLINE="D:\packages\tdgpt-offline.tar"
```

| Parameter | Description |
| --- | --- |
| `/VERYSILENT` | No UI at all, completely silent |
| `/SILENT` | Minimal UI, shows only the progress bar |
| `/NORESTART` | Do not reboot after installation |
| `/OFFLINE="path"` | Path to the external offline tar package |
| `/DIR="path"` | Override install directory (only for first install) |

When `/OFFLINE` is provided, the installer automatically uses offline mode. The Python runtime is bootstrapped from the tar package using the system `tar.exe`, so no system Python is needed.

### Silent Online Install (Requires System Python)

```bat
tdgpt-setup.exe /VERYSILENT /NORESTART
```

Online mode is not the default in silent installation. To force online mode, the target machine must have Python 3.10–3.12 in `PATH`. If the installer detects no Python and no offline package, it will fail with a clear error in the install log.

### Python Discovery Priority

During installation, the `install.bat` helper locates a Python interpreter in this order:

1. **Existing venv Python** — `<install_dir>\venvs\venv\Scripts\python.exe` (upgrade scenario)
2. **Packaged runtime Python** — `<install_dir>\python\runtime\python.exe` (previously extracted)
3. **System Python** — `python` or `python3` in `PATH` (online mode)
4. **Bootstrap from offline tar** — uses `tar.exe` to extract `python/runtime/` from the offline package into a temporary `_bootstrap` directory, then runs `install.py` with that Python

The `_bootstrap` directory is cleaned up automatically after installation completes.

### All-in-One (AIO) Integration Example

```bat
REM install-all.bat for AIO packaging
installers\tdgpt-setup.exe /VERYSILENT /NORESTART /OFFLINE="%CD%\offline-packages\tdgpt-offline.tar"
if errorlevel 1 (
    echo TDgpt installation failed
    exit /b 1
)
echo TDgpt installed successfully
```

### Checking Installation Results

After silent installation, check:

- Exit code: `0` = success, non-zero = failure
- Install log: `C:\TDengine\taosanode\log\install.log`
- Service status: `sc query Taosanode`

## Installer Behavior

Current wizard behavior:

- Default recommended install source: `Offline package`
- Online install is still available
- Windows service is installed automatically, not shown as an optional checkbox
- Upgrade installs reuse existing `venvs` and model files by default
- Offline first install requires one external tar package
- Offline upgrade can leave the tar path blank to reuse the current runtime and model files
- The `/OFFLINE` command-line parameter pre-populates the offline package path for silent installation

Current wrapper behavior:

- `start-taosanode.bat`, `stop-taosanode.bat`, `status-taosanode.bat` require `<install_dir>\venvs\venv\Scripts\python.exe`
- `start-model.bat`, `stop-model.bat`, `status-model.bat` also require the same main venv Python
- These wrappers do not fall back to system `python`
- `start-model.bat` defaults to `all` when no model name is provided

## Service And Model Commands

After install:

```bat
net start Taosanode
net stop Taosanode
sc query Taosanode

C:\TDengine\taosanode\bin\start-taosanode.bat
C:\TDengine\taosanode\bin\stop-taosanode.bat
C:\TDengine\taosanode\bin\status-taosanode.bat

C:\TDengine\taosanode\bin\start-model.bat
C:\TDengine\taosanode\bin\start-model.bat all
C:\TDengine\taosanode\bin\stop-model.bat all
C:\TDengine\taosanode\bin\status-model.bat
```

Direct Python invocation should use the main installed venv:

```bat
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py start
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py model-start all
```

## Logs

Important Windows log files:

- `<install_dir>\log\install.log`
- `<install_dir>\log\uninstall.log`
- `<install_dir>\log\install-progress.log`
- `<install_dir>\log\taosanode-service.log`
- `<install_dir>\log\taosanode.app.log`
- `<install_dir>\log\model_*.log`

Notes:

- The service manager log is `taosanode-service.log`.
- WinSW wrapper logs such as `taosanode-service.wrapper.log`, `*.out.log`, and `*.err.log` are not part of the current default logging flow.

## Troubleshooting

### Packaging Fails Before ISCC

- Check Python version on the build host
- Check the version string format
- Check whether the output directory can be deleted and recreated

### ISCC Not Found

- Install Inno Setup 6
- Pass `--iscc-path` explicitly if `ISCC.exe` is not under the default path

### Service Wrapper Reports Main Python Missing

- Verify `<install_dir>\venvs\venv\Scripts\python.exe` exists
- For offline install, re-run setup with a valid offline tar if the environment was not imported successfully
- For online install, re-run setup so the main venv can be rebuilt

### Service Start Returned But Readiness Was Not Confirmed

- Check `<install_dir>\log\taosanode-service.log`
- Check `<install_dir>\log\taosanode.app.log`
- Run `status-taosanode.bat`
- Run `sc query Taosanode`

## Review Summary

This README was aligned with the current code in:

- `packaging/win_release.py`
- `packaging/installer/tdgpt.iss`
- `script/install.py`
- `script/taosanode_service.py`

Key corrections made:

- removed obsolete `full-offline` wording
- clarified that the base installer does not bundle runtime or venvs
- clarified that offline install does not require system Python
- clarified that wrapper scripts require the main installed venv Python
- corrected log file names and removed outdated WinSW wrapper log claims
- clarified that service installation is automatic
- clarified that model archive bundling is optional
- added silent installation documentation with `/OFFLINE` parameter
- added Python discovery priority documentation
- added AIO integration example
- added Windows 10 1803+ requirement for offline `tar.exe` bootstrap
