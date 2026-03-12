# TDGPT Development and Maintenance Guide

This guide is for developers and maintainers working on TDGPT packaging, service management, and configuration.

## Table of Contents

- [Project Structure](#project-structure)
- [Packaging Script (win_release.py)](#packaging-script-win_releasepy)
- [Service Management Script (taosanode_service.py)](#service-management-script-taosanode_servicepy)
- [Configuration File (taosanode.config.py)](#configuration-file-taosanodeconfigpy)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## Project Structure

```
tdgpt/
├── script/
│   ├── taosanode_service.py      # Main service management script
│   ├── install.sh                # Linux installation script
│   └── README.md                 # Script documentation
├── packaging/
│   ├── win_release.py            # Windows packaging script
│   ├── wheels/                   # Pre-downloaded Python wheels (optional)
│   └── README.md                 # Packaging documentation
├── cfg/
│   └── taosanode.config.py       # Configuration file
├── tests/
│   ├── conftest.py               # Pytest configuration
│   ├── test_process_manager.py   # ProcessManager tests
│   ├── test_config.py            # Config tests
│   ├── test_taosanode_service.py # TaosanodeService tests
│   ├── test_model_service.py     # ModelService tests
│   └── README.md                 # Test documentation
├── INSTALL.md                    # User installation guide
├── DEVELOPMENT.md                # Developer guide (this file)
└── README.md                     # Project overview
```

---

## Packaging Script (win_release.py)

### Purpose

The `win_release.py` script automates the creation of Windows installation packages for TDGPT. It handles:
- Copying source files to staging directory
- Downloading and configuring winsw (Windows Service Wrapper)
- Generating batch scripts for installation/uninstallation
- Creating Inno Setup installer script
- Building the final `.exe` installer

### Usage

```bash
# Basic usage
python packaging/win_release.py -e community -v 3.3.6.0

# With offline wheels
python packaging/win_release.py -e community -v 3.3.6.0 --offline

# With model files
python packaging/win_release.py -e community -v 3.3.6.0 -m /path/to/models

# With all models
python packaging/win_release.py -e community -v 3.3.6.0 -m /path/to/models -a

# Custom output directory
python packaging/win_release.py -e community -v 3.3.6.0 -o D:\custom-release
```

### Key Functions

#### `check_python_version()`
Validates that Python 3.10, 3.11, or 3.12 is being used.

#### `parse_arguments()`
Parses command-line arguments and validates version format.

#### `copy_config_files()`
Copies configuration files to the staging directory.

#### `copy_python_files()`
Copies Python source code and libraries.

#### `prepare_offline_packages()`
Copies pre-downloaded wheel files for offline installation.

#### `download_winsw()`
Downloads the winsw executable from GitHub for Windows service support.

#### `create_winsw_config()`
Generates the XML configuration file for winsw service wrapper.

#### `create_install_script()`
Generates `install.bat` batch script for Windows installation.

#### `create_uninstall_script()`
Generates `uninstall.bat` batch script for Windows uninstallation.

#### `create_iss_script()`
Generates Inno Setup script for building the installer.

#### `build_installer()`
Compiles the Inno Setup script to create the final `.exe` installer.

### Configuration

Edit the `InstallInfo` class to customize:
- Installation directory paths
- Package naming
- Release directory location

### Offline Installation Support

To enable offline installation:

1. Pre-download wheels:
```bash
mkdir -p packaging/wheels
pip download -r requirements_ess.txt -d packaging/wheels
```

2. Build with `--offline` flag:
```bash
python packaging/win_release.py -e community -v 3.3.6.0 --offline
```

3. The installer will automatically detect and use local wheels during installation.

---

## Service Management Script (taosanode_service.py)

### Purpose

The `taosanode_service.py` script provides unified service management for both Linux and Windows platforms. It handles:
- Service startup/shutdown
- Process management (PID tracking, process checking)
- Model service management
- Windows service registration (via winsw)
- Structured logging with log rotation

### Architecture

#### Classes

**Config**
- Loads configuration from `taosanode.config.py`
- Provides default values for all configuration parameters
- Handles path creation and validation

**ProcessManager**
- Manages process lifecycle (start, stop, check status)
- Handles PID file operations
- Platform-specific process checking (Windows tasklist vs Unix signals)
- Implements graceful shutdown (SIGTERM before SIGKILL)

**TaosanodeService**
- Manages the main taosanode service
- Handles startup with gunicorn (Linux) or waitress (Windows)
- Implements service registration for Windows (via winsw)
- Provides status reporting

**ModelService**
- Manages individual model services
- Supports concurrent model startup/shutdown
- Handles required vs optional models
- Implements model-specific logging

#### Logging

Uses Python's `logging` module with:
- File handlers with log rotation (10MB per file, 5 backups)
- Console handlers for user feedback
- Separate loggers for each component

### Usage

```bash
# Service management
python taosanode_service.py start
python taosanode_service.py stop
python taosanode_service.py status

# Model management
python taosanode_service.py model-start all
python taosanode_service.py model-stop all
python taosanode_service.py model-status

# Windows service management (requires admin)
python taosanode_service.py install-service
python taosanode_service.py uninstall-service
python taosanode_service.py start-service
python taosanode_service.py stop-service

# Custom configuration
python taosanode_service.py -c /path/to/config.py start
```

### Key Methods

#### ProcessManager

- `read_pid(service_name)`: Read PID from file
- `write_pid(pid, service_name)`: Write PID to file
- `remove_pid(service_name)`: Delete PID file
- `is_running(service_name)`: Check if process is running
- `wait_for_service(service_name, timeout)`: Poll until service starts
- `kill_process(pid, force)`: Terminate process (graceful or forceful)

#### TaosanodeService

- `start()`: Start the main service
- `stop()`: Stop the main service
- `status()`: Get service status
- `install_service()`: Register as Windows service
- `uninstall_service()`: Unregister Windows service
- `start_service()`: Start Windows service
- `stop_service()`: Stop Windows service

#### ModelService

- `start(model_name)`: Start a model service
- `stop(model_name)`: Stop a model service
- `status()`: Get status of all models
- `_start_all()`: Start all models concurrently
- `_stop_all()`: Stop all models concurrently

### Platform-Specific Behavior

**Linux**
- Uses gunicorn as WSGI server
- Daemon mode (`-D` flag)
- systemd service management
- SIGTERM for graceful shutdown, SIGKILL for forced

**Windows**
- Uses waitress as WSGI server
- Foreground process with log redirection
- winsw for service registration
- `taskkill` for process termination
- Configurable waitress parameters

### Error Handling

- Validates configuration before startup
- Checks for required model directories
- Skips optional models if not found
- Implements timeout for service startup verification
- Graceful degradation if winsw not available

---

## Configuration File (taosanode.config.py)

### Purpose

Central configuration file for all TDGPT services. Defines:
- Service binding address and port
- Worker processes and threads
- Logging configuration
- Model definitions and locations
- Waitress server parameters (Windows)

### Key Parameters

#### Service Configuration

```python
bind = '0.0.0.0:6035'           # Listen address and port
workers = 2                      # Number of worker processes
worker_class = 'sync'            # Worker type
threads = 4                      # Threads per worker
timeout = 1200                   # Request timeout (seconds)
keepalive = 1200                 # Keep-alive timeout (seconds)
```

#### Logging

```python
log_level = 'DEBUG'              # Log level
app_log = '...'                  # Application log file
accesslog = '...'                # Access log (Linux only)
errorlog = '...'                 # Error log (Linux only)
waitresslog = '...'              # Waitress log (Windows only)
```

#### Model Configuration

```python
models = {
    "tdtsfm": {
        "script": "tdtsfm-server.py",
        "default_model": None,
        "port": 6036,
        "required": True,        # Must exist
    },
    "chronos": {
        "script": "chronos-server.py",
        "default_model": "amazon/chronos-bolt-base",
        "port": 0,
        "required": False,       # Optional
    },
    # ... other models
}
```

#### Windows Waitress Configuration

```python
waitress_config = {
    'threads': 4,                # Worker threads
    'channel_timeout': 1200,     # Channel timeout (seconds)
    'connection_limit': 1000,    # Max connections
    'cleanup_interval': 30,      # Cleanup interval (seconds)
    'log_socket_errors': True    # Log socket errors
}
```

### Adding New Models

1. Add model definition to `models` dictionary:
```python
"new_model": {
    "script": "new-model-server.py",
    "default_model": "vendor/model-name",
    "port": 6042,
    "required": False,
}
```

2. Create model directory:
```bash
mkdir -p /usr/local/taos/taosanode/model/new_model
```

3. Place model files in the directory

4. Restart service:
```bash
python taosanode_service.py model-start new_model
```

### Platform-Specific Paths

The configuration uses platform detection to set appropriate paths:

```python
on_windows = platform.system().lower() == "windows"

# Linux paths
pidfile = '/usr/local/taos/taosanode/taosanode.pid' if not on_windows else ...
pythonpath = '/usr/local/taos/taosanode/lib/taosanalytics/' if not on_windows else ...
model_dir = '/usr/local/taos/taosanode/model/' if not on_windows else ...

# Windows paths
pidfile = 'c:/TDengine/taosanode/taosanode.pid' if on_windows else ...
pythonpath = 'c:/TDengine/taosanode/lib/taosanalytics/' if on_windows else ...
model_dir = 'c:/TDengine/taosanode/model/' if on_windows else ...
```

---

## Testing

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-mock pytest-cov

# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_process_manager.py

# Run with coverage report
pytest --cov=script/taosanode_service --cov-report=html tests/

# Run specific test
pytest tests/test_process_manager.py::TestProcessManager::test_read_pid_file_exists
```

### Test Structure

- **conftest.py**: Shared fixtures and configuration
- **test_process_manager.py**: ProcessManager unit tests
- **test_config.py**: Config class tests
- **test_taosanode_service.py**: TaosanodeService tests
- **test_model_service.py**: ModelService tests

### Test Coverage

Target coverage: >80% for critical paths

Key areas:
- Process management (PID operations, process checking)
- Configuration loading and validation
- Service startup/shutdown
- Model management
- Error handling

---

## Troubleshooting

### Build Issues

**Issue**: Inno Setup not found
- **Solution**: Install Inno Setup 6 or specify path with `--iscc-path`

**Issue**: Python version mismatch
- **Solution**: Ensure Python 3.10+ is used: `python --version`

**Issue**: Wheel download fails
- **Solution**: Pre-download wheels manually to `packaging/wheels/`

### Service Issues

**Issue**: Service won't start
- **Solution**: Check logs in `log/` directory
- **Solution**: Run directly to see error: `python taosanode_service.py start`

**Issue**: Models not starting
- **Solution**: Verify model directories exist
- **Solution**: Check model logs in `log/taosanode_service_<model>.log`

**Issue**: Port conflicts
- **Solution**: Change port in configuration file
- **Solution**: Kill existing process: `lsof -i :<port>` (Linux) or `netstat -ano | findstr :<port>` (Windows)

### Configuration Issues

**Issue**: Configuration not loaded
- **Solution**: Verify file path is correct
- **Solution**: Check Python syntax: `python -m py_compile taosanode.config.py`

**Issue**: Paths not found
- **Solution**: Verify paths exist and are readable
- **Solution**: Use absolute paths instead of relative paths

### Testing Issues

**Issue**: Tests fail on Windows
- **Solution**: Some tests are platform-specific and skip automatically
- **Solution**: Run with `-v` flag for verbose output

**Issue**: Mock objects not working
- **Solution**: Ensure pytest-mock is installed: `pip install pytest-mock`

---

## Development Workflow

### Adding a New Feature

1. Create a feature branch:
```bash
git checkout -b feature/new-feature
```

2. Implement the feature with tests:
```bash
# Write tests first (TDD)
# Implement feature
# Run tests: pytest tests/
```

3. Update documentation:
- Update INSTALL.md for user-facing changes
- Update DEVELOPMENT.md for developer changes
- Update docstrings in code

4. Commit and push:
```bash
git add .
git commit -m "feat: add new feature"
git push origin feature/new-feature
```

5. Create pull request and request review

### Code Style

- Follow PEP 8 guidelines
- Use type hints for function parameters and returns
- Add docstrings to all classes and methods
- Keep functions focused and testable

### Version Management

Version format: `MAJOR.MINOR.PATCH.BUILD`

Example: `3.3.6.0`

Update version in:
- `win_release.py` (default version)
- Build scripts
- Documentation

---

## Performance Optimization

### Service Startup

- Models start concurrently using ThreadPoolExecutor
- Configurable timeout for service verification
- Graceful degradation if optional models fail

### Resource Usage

- Log rotation prevents disk space issues
- Process cleanup on shutdown
- Memory-efficient model loading

### Scalability

- Multiple worker processes (configurable)
- Thread pool for concurrent requests
- Connection pooling and limits

---

## Security Considerations

### File Permissions

- Configuration files should be readable only by service user
- Log files should be writable by service user
- PID files should be in secure directory

### Network Security

- Bind to specific interface (not 0.0.0.0 in production)
- Use firewall rules to restrict access
- Consider reverse proxy for external access

### Process Security

- Run service with minimal privileges
- Validate all configuration inputs
- Implement request timeouts

---

## Future Improvements

- [ ] Add metrics collection and monitoring
- [ ] Implement health check endpoints
- [ ] Add configuration hot-reload
- [ ] Support for custom model plugins
- [ ] Kubernetes deployment support
- [ ] Docker containerization
- [ ] API documentation generation
- [ ] Performance profiling tools

---

## Support and Contact

For questions or issues:
1. Check the troubleshooting section
2. Review test cases for usage examples
3. Check git history for similar changes
4. Contact the development team
