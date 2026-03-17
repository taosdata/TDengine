# 2. TDGPT Windows Packaging Script

> 📖 **Language**: [English](README.md) | [中文](README-CN.md)

This directory contains the Windows packaging script for TDGPT (TDengine Analytics Node), which creates installation programs using Inno Setup.

## File Structure

```
packaging/
├── win_release.py           # Python packaging script (main entry point)
└── README_EN.md             # This documentation

script/
├── taosanode_service.py     # Unified service management script (cross-platform)
└── ...                      # Other scripts
```

## Unified Service Management Script

Uses **Python unified script** `taosanode_service.py` to replace the original shell/bat scripts, providing cross-platform support (Linux/Windows) with a single codebase.

### Features

| Command | Description |
|---------|-------------|
| `start` | Start taosanode main service |
| `stop` | Stop taosanode main service |
| `status` | View service status |
| `model-start [name]` | Start model service (name: tdtsfm, timemoe, chronos, moirai, moment, timesfm, all) |
| `model-stop [name]` | Stop model service |
| `model-status` | View model service status |

### Usage Examples

```bash
# Main service
python taosanode_service.py start
python taosanode_service.py stop
python taosanode_service.py status

# Model services
python taosanode_service.py model-start tdtsfm
python taosanode_service.py model-start all
python taosanode_service.py model-stop all
python taosanode_service.py model-status
```

### Windows Batch Wrappers

For convenience of Windows users, batch wrappers are provided:

```
bin/
├── taosanode_service.py    # Core Python script
├── start-taosanode.bat     # Wrapper: python taosanode_service.py start
├── stop-taosanode.bat      # Wrapper: python taosanode_service.py stop
├── status-taosanode.bat    # Wrapper: python taosanode_service.py status
├── start-model.bat         # Wrapper: python taosanode_service.py model-start
├── stop-model.bat          # Wrapper: python taosanode_service.py model-stop
└── status-model.bat        # Wrapper: python taosanode_service.py model-status
```

## Prerequisites

1. **Python 3.10+** - Python must be installed and added to PATH
2. **Inno Setup 6** - For creating installation programs
   - Download: https://jrsoftware.org/isdl.php
   - After installation, ensure `ISCC.exe` is in PATH or specify the path in the script

## Usage

### Basic Usage

```bash
# Community edition (production packaging, requires model files)
python packaging/win_release.py -e community -v 3.4.0.11.0316 -m D:\models

# Enterprise edition
python packaging/win_release.py -e enterprise -v 3.4.0.11.0316 -m D:\models

# Include all models
python packaging/win_release.py -e community -v 3.4.0.11.0316 -m D:\models -a

# Custom output directory
python packaging/win_release.py -e community -v 3.4.0.11.0316 -m D:\models -o D:\release

# Testing mode (quick workflow validation, no model files needed)
python packaging/win_release.py -e community -v 3.4.0.11.0316 --skip-model-check
```

### Model Files Requirements

**Production Packaging (default):**
- Must specify model directory using `-m` parameter
- Model directory must contain the following files:
  - `timemoe.tar.gz` (required)
  - `tdtsfm.tar.gz` (required)
- Missing any required file will cause packaging to fail

**Testing Mode (`--skip-model-check`):**
- Skip model validation, no model files needed
- For quick testing of packaging workflow only
- ⚠️ **NOT for production use**

## Packaging Script Parameters

| Parameter | Short | Description | Required |
|-----------|-------|-------------|----------|
| `--edition` | `-e` | Edition type: enterprise or community | Yes |
| `--version` | `-v` | Version number (e.g., 3.4.0.11.0316) | Yes |
| `--model-dir` | `-m` | Model files directory (required for production) | Production only |
| `--all-models` | `-a` | Package all models | No |
| `--output` | `-o` | Output directory (default: D:\tdgpt-release) | No |
| `--iscc-path` | | Inno Setup compiler path | No |
| `--skip-model-check` | | Skip model validation (testing only) | No |

## Installer Features

- **Default Installation Path**: `C:\TDengine\taosanode`
- **Service Management**: Uses unified Python script `taosanode_service.py`
- **Virtual Environment**: Automatically creates Python virtual environment
- **Environment Variables**: Automatically adds `bin` directory to PATH
- **Log Directory**: `C:\TDengine\taosanode\log`
- **Configuration Protection**: Preserves existing configuration files during upgrades

## Service Management

After installation, you can manage services using the following commands:

```bash
# Start/Stop/View status
C:\TDengine\taosanode\bin\start-taosanode.bat
C:\TDengine\taosanode\bin\stop-taosanode.bat
C:\TDengine\taosanode\bin\status-taosanode.bat

# Model services
C:\TDengine\taosanode\bin\start-model.bat tdtsfm
C:\TDengine\taosanode\bin\start-model.bat all
C:\TDengine\taosanode\bin\stop-model.bat all
C:\TDengine\taosanode\bin\status-model.bat
```

Or use the Python script directly:

```bash
cd C:\TDengine\taosanode
python bin\taosanode_service.py start
python bin\taosanode_service.py model-start all
```

## Differences from Linux Packaging

| Feature | Linux | Windows |
|---------|-------|---------|
| Service Management | Python unified script | Python unified script + bat wrappers |
| WSGI Server | gunicorn | waitress |
| Default Path | /usr/local/taos/taosanode | C:\TDengine\taosanode |
| Configuration File | taosanode.config.py | taosanode.config.py |
| Process Management | signal / ps | taskkill |

## Important Notes

1. **Python Version**: Python 3.10 or higher is recommended
2. **Configuration File**: `taosanode.config.py` has built-in Windows path support, automatically switching via the `on_windows` variable
3. **Firewall**: The service uses port 6035 by default, firewall configuration may be needed
4. **Dependencies**: Python dependencies are automatically installed on first startup, internet access may be required

## Troubleshooting

### Service Won't Start

1. Check if Python is correctly installed and added to PATH
2. Check log files: `C:\TDengine\taosanode\log\taosanode_service_*.log`
3. Run the startup script manually to see errors:
   ```bash
   cd C:\TDengine\taosanode
   python bin\taosanode_service.py start
   ```

### Dependency Installation Failed

1. Ensure you can access PyPI
2. Install dependencies manually:
   ```bash
   cd C:\TDengine\taosanode
   python -m venv venv
   venv\Scripts\activate.bat
   pip install -r requirements_ess.txt
   ```

### Port Already in Use

Modify the `bind` setting in the configuration file `C:\TDengine\taosanode\cfg\taosanode.config.py`.

## References

- [Inno Setup Documentation](https://jrsoftware.org/ishelp/)
- [TDGPT Linux Packaging Script](../script/release.sh)
- [TDGPT Linux Installation Script](../script/install.sh)
- [TDengine Windows Packaging Process](../../../../enterprise/packaging/new_win_release.py)

## 2026-03-17 Layout And Installer UX Update

- Requirements files are now packaged under `<install_dir>\requirements\`.
- All Windows virtual environments are now created under `<install_dir>\venvs\`.
- The installer finish flow now includes a default-enabled checkbox to install/register the Windows service.
- The finish page now shows both service commands and script commands:
  - `net start Taosanode`
  - `net stop Taosanode`
  - `<install_dir>\bin\start-taosanode.bat`
  - `<install_dir>\bin\stop-taosanode.bat`
- In non-silent mode the finish page automatically opens `<install_dir>\log\install.log`.

### Log Files

- `<install_dir>\log\install.log`: full install output and install summary.
- `<install_dir>\log\uninstall.log`: full uninstall output and uninstall summary.
- `<install_dir>\log\taosanode-service.log`: service manager lifecycle log.
- `<install_dir>\log\taosanode-winsw.wrapper.log`: WinSW wrapper log.
- `<install_dir>\log\taosanode.app.log`: taosanode application log.

### Standard Uninstall

- Standard uninstall preserves `cfg`, `data`, `model`, `venvs`, and `log`.
- `model` is removed only when `uninstall.py --remove-model` is used explicitly.
