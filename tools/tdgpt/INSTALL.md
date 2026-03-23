# 1. TDGPT Installation and Usage Guide

> 📖 **Language**: [English](INSTALL.md) | [中文](INSTALL-CN.md)

This guide covers installation and usage of TDGPT on both Linux and Windows platforms.

## Table of Contents

- [System Requirements](#system-requirements)
- [Linux Installation](#linux-installation)
- [Windows Installation](#windows-installation)
- [Service Management](#service-management)
- [Configuration](#configuration)
- [Model Management](#model-management)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

---

## System Requirements

### Supported Platforms

- **Linux**: Ubuntu 18.04+, CentOS 7+, or other Linux distributions
- **Windows**: Windows Server 2016+ or Windows 10/11

### Python Version

- Python 3.10, 3.11, or 3.12 (required)

### Hardware Requirements

- **CPU**: 4+ cores recommended
- **Memory**: 8GB+ recommended
- **Disk**: 20GB+ for models and data

### Network Ports

- **6035**: Main taosanode service
- **6036**: tdtsfm model service
- **6037**: timemoe model service
- **6038-6040**: Optional model services (chronos, timesfm, moirai, moment)

---

## Linux Installation

### Prerequisites

1. Install Python 3.10+ (if not already installed):

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install python3.10 python3.10-venv python3.10-dev

# CentOS/RHEL
sudo yum install python310 python310-devel
```

2. Ensure you have sudo privileges for system-wide installation.

### Installation Steps

1. **Extract the installation package**:

```bash
tar -xzf tdgpt-linux-x.x.x.tar.gz
cd tdgpt-linux-x.x.x
```

2. **Run the installation script**:

```bash
# Standard installation
bash install.sh

# Offline installation (if wheels are included)
bash install.sh -o

# Custom installation directory
bash install.sh -d /opt/tdgpt
```

3. **Verify installation**:

```bash
# Check service status
python /usr/local/taos/taosanode/bin/taosanode_service.py status

# Check model status
python /usr/local/taos/taosanode/bin/taosanode_service.py model-status
```

### Linux Service Management

#### Using systemd (Recommended)

The installation script automatically creates a systemd service. Manage it with:

```bash
# Start service
sudo systemctl start taosanode

# Stop service
sudo systemctl stop taosanode

# Restart service
sudo systemctl restart taosanode

# Check status
sudo systemctl status taosanode

# Enable auto-start on boot
sudo systemctl enable taosanode

# Disable auto-start
sudo systemctl disable taosanode

# View logs
sudo journalctl -u taosanode -f
```

#### Using Python Script Directly

```bash
# Start service
python /usr/local/taos/taosanode/bin/taosanode_service.py start

# Stop service
python /usr/local/taos/taosanode/bin/taosanode_service.py stop

# Check status
python /usr/local/taos/taosanode/bin/taosanode_service.py status
```

### Linux Uninstallation

```bash
# Run uninstall script
bash /usr/local/taos/taosanode/uninstall.sh

# Or manually remove
sudo rm -rf /usr/local/taos/taosanode
sudo systemctl disable taosanode
```

---

## Windows Installation

### Prerequisites

1. **Install Python 3.10+**:
   - Download from [python.org](https://www.python.org/downloads/)
   - During installation, check "Add Python to PATH"
   - Verify installation: `python --version`

2. **Administrator Privileges**:
   - Required for service installation
   - Run Command Prompt as Administrator

### Installation Steps

1. **Extract the installation package**:
   - Right-click the installer `.exe` file
   - Select "Run as administrator"
   - Follow the installation wizard

   The Windows installer wizard can now configure:
   - Python package mode: online or offline
   - Python package source: official PyPI, Tsinghua, Aliyun, or custom
   - Optional TensorFlow CPU support in online mode
   - Model installation source: skip, online download, or offline package import
   - Model download endpoint: official Hugging Face, HF Mirror, or custom
   - Windows service registration
   - Model enablement order: TDtsfm v1.0, TimeMoE, Moirai, Chronos, TimesFM, MOMENT

   Default wizard behavior:
   - install Python packages online
   - install TensorFlow CPU support
   - do not install models yet
   - register the Windows service

   Model preparation details:
   - Offline Python package mode does not ask about TensorFlow because the packaged offline runtime already includes it.
   - Offline model import automatically scans `<install_dir>\model\` and imports every packaged model archive that is present.
   - Offline mode also provides one optional offline model package input. That archive can contain all offline models together.
   - Offline mode does not ask users to select models. The wizard shows one offline import page instead.
   - `start-model.bat` defaults to `all` when no argument is provided. `start-model.bat all` and `model-start all` check model directories at runtime. Models with existing directories are started automatically, and missing ones are skipped with a message.
   - Online model installation creates only the selected model virtual environments.
   - Offline model installation does not create extra model virtual environments during setup.
   - Online default model selection is `Moirai Small` and `MOMENT Base`.

2. **Or extract ZIP package manually**:
   - Extract to desired location (e.g., `C:\TDengine\taosanode`)
   - Open Command Prompt as Administrator
   - Navigate to installation directory
   - Run: `install.bat`

3. **Verify installation**:

```batch
python C:\TDengine\taosanode\bin\taosanode_service.py status
```

### Windows Service Management

#### Option 1: Using Windows Service (Recommended)

Requires Administrator privileges.

```batch
# Install as Windows service
python C:\TDengine\taosanode\bin\taosanode_service.py install-service

# Start service
net start Taosanode
# Or: python C:\TDengine\taosanode\bin\taosanode_service.py start-service

# Stop service
net stop Taosanode
# Or: python C:\TDengine\taosanode\bin\taosanode_service.py stop-service

# Check service status
sc query Taosanode

# Uninstall service
python C:\TDengine\taosanode\bin\taosanode_service.py uninstall-service
```

#### Option 2: Using Batch Scripts (Foreground Mode)

```batch
# Start service (foreground)
C:\TDengine\taosanode\bin\start-taosanode.bat

# Stop service
C:\TDengine\taosanode\bin\stop-taosanode.bat

# Check status
C:\TDengine\taosanode\bin\status-taosanode.bat
```

#### Option 3: Using Python Script Directly

```batch
# Start
python C:\TDengine\taosanode\bin\taosanode_service.py start

# Stop
python C:\TDengine\taosanode\bin\taosanode_service.py stop

# Status
python C:\TDengine\taosanode\bin\taosanode_service.py status
```

### Windows Firewall Configuration

If you encounter connection issues, configure Windows Firewall:

```batch
# Open ports automatically (run as Administrator)
netsh advfirewall firewall add rule name="TDGPT" dir=in action=allow protocol=TCP localport=6035,6036,6037 profile=any

# Or manually:
# 1. Open Windows Defender Firewall with Advanced Security
# 2. Click "Inbound Rules" → "New Rule"
# 3. Select "Port" → "TCP" → Specific local ports: 6035,6036,6037
# 4. Allow the connection
```

### Windows Uninstallation

```batch
# Run uninstall script
C:\TDengine\taosanode\uninstall.bat

# Or manually:
# 1. Uninstall Windows service (if installed):
#    python C:\TDengine\taosanode\bin\taosanode_service.py uninstall-service
# 2. Delete installation directory:
#    rmdir /s /q C:\TDengine\taosanode
```

---

## Service Management

### Common Commands

#### Start/Stop/Status

```bash
# Linux
python /usr/local/taos/taosanode/bin/taosanode_service.py start
python /usr/local/taos/taosanode/bin/taosanode_service.py stop
python /usr/local/taos/taosanode/bin/taosanode_service.py status

# Windows
python C:\TDengine\taosanode\bin\taosanode_service.py start
python C:\TDengine\taosanode\bin\taosanode_service.py stop
python C:\TDengine\taosanode\bin\taosanode_service.py status
```

#### Model Management

```bash
# Start specific model
python <install_dir>/bin/taosanode_service.py model-start tdtsfm

# Start all models
python <install_dir>/bin/taosanode_service.py model-start all

# Stop specific model
python <install_dir>/bin/taosanode_service.py model-stop tdtsfm

# Stop all models
python <install_dir>/bin/taosanode_service.py model-stop all

# Check model status
python <install_dir>/bin/taosanode_service.py model-status
```

---

## Configuration

### Configuration File Location

- **Linux**: `/usr/local/taos/taosanode/cfg/taosanode.config.py`
- **Windows**: `C:\TDengine\taosanode\cfg\taosanode.config.py`

### Key Configuration Parameters

#### Service Binding

```python
# Listen address and port
bind = '0.0.0.0:6035'

# Number of worker processes
workers = 2
```

#### Logging

```python
# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
log_level = 'DEBUG'

# Log file location
app_log = '/var/log/taos/taosanode/taosanode.app.log'  # Linux
app_log = 'c:/TDengine/taosanode/log/taosanode.app.log'  # Windows
```

#### Model Configuration

```python
# Model storage directory
model_dir = '/usr/local/taos/taosanode/model/'  # Linux
model_dir = 'c:/TDengine/taosanode/model/'  # Windows

# Model definitions
models = {
    "tdtsfm": {
        "script": "tdtsfm-server.py",
        "port": 6036,
        "required": True,  # Must exist
    },
    "timemoe": {
        "script": "timemoe-server.py",
        "port": 6037,
        "required": True,  # Must exist
    },
    "chronos": {
        "script": "chronos-server.py",
        "port": 0,
        "required": False,  # Optional
    },
    # ... other models
}
```

#### Windows Waitress Configuration

```python
# Waitress server configuration (Windows only)
waitress_config = {
    'threads': 4,                    # Worker threads
    'channel_timeout': 1200,         # Channel timeout (seconds)
    'connection_limit': 1000,        # Max connections
    'cleanup_interval': 30,          # Cleanup interval (seconds)
    'log_socket_errors': True        # Log socket errors
}
```

### Modifying Configuration

1. Edit the configuration file with a text editor
2. Restart the service for changes to take effect:

```bash
# Linux
sudo systemctl restart taosanode

# Windows
net stop Taosanode
net start Taosanode
```

---

## Model Management

### Supported Models

| Model | Required | Port | Description |
|-------|----------|------|-------------|
| tdtsfm | Yes | 6036 | Time Series Foundation Model |
| timemoe | Yes | 6037 | Time Series Mixture of Experts |
| chronos | No | 6038 | Amazon Chronos |
| timesfm | No | 6039 | Google TimesFM |
| moirai | No | 6040 | Salesforce Moirai |
| moment | No | 6041 | AutonLab MOMENT |

### Starting Models

```bash
# Start required models (tdtsfm, timemoe)
python <install_dir>/bin/taosanode_service.py model-start all

# Start specific model
python <install_dir>/bin/taosanode_service.py model-start chronos

# Check model status
python <install_dir>/bin/taosanode_service.py model-status
```

### Stopping Models

```bash
# Stop all models
python <install_dir>/bin/taosanode_service.py model-stop all

# Stop specific model
python <install_dir>/bin/taosanode_service.py model-stop chronos
```

---

## Troubleshooting

### Service Won't Start

**Linux**:

```bash
# Check systemd logs
sudo journalctl -u taosanode -n 50

# Check Python errors
python /usr/local/taos/taosanode/bin/taosanode_service.py start
```

**Windows**:

```batch
# Check startup log
type C:\TDengine\taosanode\log\taosanode_startup.log

# Run directly to see errors
python C:\TDengine\taosanode\bin\taosanode_service.py start
```

### Port Already in Use

```bash
# Linux: Find process using port 6035
sudo lsof -i :6035

# Windows: Find process using port 6035
netstat -ano | findstr :6035

# Kill the process
# Linux: sudo kill -9 <PID>
# Windows: taskkill /PID <PID> /F
```

### Models Not Starting

1. Check model directory exists:

```bash
# Linux
ls -la /usr/local/taos/taosanode/model/

# Windows
dir C:\TDengine\taosanode\model\
```

2. Check model logs:

```bash
# Linux
tail -f /var/log/taos/taosanode/taosanode_service_tdtsfm.log

# Windows
type C:\TDengine\taosanode\log\taosanode_service_tdtsfm.log
```

3. Verify Python dependencies:

```bash
python -m pip list | grep -E "torch|transformers"
```

### Connection Refused

1. Check if service is running:

```bash
python <install_dir>/bin/taosanode_service.py status
```

2. Check firewall settings:

```bash
# Linux
sudo ufw status
sudo ufw allow 6035/tcp

# Windows
netsh advfirewall firewall show rule name="TDGPT"
```

3. Check binding address:

```bash
# Linux
sudo netstat -tlnp | grep 6035

# Windows
netstat -ano | findstr :6035
```

---

## FAQ

### Q: How do I check if the service is running?

**A**: Use the status command:

```bash
python <install_dir>/bin/taosanode_service.py status
```

### Q: Can I run multiple instances?

**A**: Not recommended. The service is designed to run as a single instance. Use different ports if needed by modifying the configuration.

### Q: How do I view logs?

**A**:

- **Linux**: `tail -f /var/log/taos/taosanode/taosanode.app.log`
- **Windows**: `type C:\TDengine\taosanode\log\taosanode.app.log`

### Q: How do I update the configuration?

**A**: Edit the configuration file and restart the service. Changes take effect after restart.

### Q: What if I need to reinstall?

**A**:

1. Uninstall the current version
2. Delete the installation directory
3. Run the new installer

### Q: How do I enable debug logging?

**A**: Edit the configuration file and set `log_level = 'DEBUG'`, then restart the service.

### Q: Can I use a custom Python virtual environment?

**A**: Yes, modify the `venv_dir` in the configuration file to point to your virtual environment.

### Q: How do I backup my configuration?

**A**: Copy the configuration file to a safe location:

```bash
# Linux
cp /usr/local/taos/taosanode/cfg/taosanode.config.py ~/taosanode.config.backup.py

# Windows
copy C:\TDengine\taosanode\cfg\taosanode.config.py C:\backup\taosanode.config.backup.py
```

---

## Support

For issues or questions, please refer to the internal documentation or contact the development team.
