# TDGPT Tests

This directory contains unit tests and integration tests for the TDGPT service, including service management, configuration, and SSL/TLS functionality.

## Prerequisites

Install test dependencies:

```bash
pip install pytest pytest-mock pytest-cov
```

## Running Tests

### Run all tests

```bash
pytest tests/
```

### Run specific test file

```bash
pytest tests/test_process_manager.py
pytest tests/test_config.py
pytest tests/test_taosanode_service.py
pytest tests/test_model_service.py
```

### Run specific test class

```bash
pytest tests/test_process_manager.py::TestProcessManager
```

### Run specific test method

```bash
pytest tests/test_process_manager.py::TestProcessManager::test_read_pid_file_exists
```

### Run with verbose output

```bash
pytest -v tests/
```

### Generate coverage report

```bash
pytest --cov=script/taosanode_service --cov-report=html tests/
```

This will generate an HTML coverage report in `htmlcov/index.html`.

### Run tests with specific markers

```bash
# Run only Windows tests
pytest -m "not skipif" tests/

# Run only Unix tests
pytest -m "not skipif" tests/
```

## Test Structure

### Unit Tests
- **conftest.py**: Shared pytest fixtures and configuration
- **test_process_manager.py**: Tests for ProcessManager class
- **test_config.py**: Tests for Config class
- **test_taosanode_service.py**: Tests for TaosanodeService class
- **test_model_service.py**: Tests for ModelService class

### Integration Tests
- **test_ssl.py**: HTTPS connectivity tests for SSL-enabled service
- **setup_ssl_test.sh**: Self-signed certificate generation helper

## Test Coverage

The test suite covers:

1. **ProcessManager**
   - PID file operations (read, write, remove)
   - Process status checking (Windows and Unix)
   - Process termination (graceful and forceful)
   - Service waiting with timeout

2. **Config**
   - Default configuration values
   - Model configuration (required/optional)
   - Custom configuration loading
   - Path creation and validation

3. **TaosanodeService**
   - Service start/stop operations
   - Service status checking
   - Error handling and timeouts

4. **ModelService**
   - Model start/stop operations
   - Model status checking
   - Concurrent model operations
   - Required vs optional model handling

## Platform-Specific Tests

Some tests are platform-specific:

- Windows-only tests use `@pytest.mark.skipif(not IS_WINDOWS, reason="Windows-only test")`
- Unix-only tests use `@pytest.mark.skipif(IS_WINDOWS, reason="Unix-only test")`

These tests will be automatically skipped on incompatible platforms.

## Mocking

Tests use `unittest.mock` and `pytest-mock` for:

- Mocking subprocess calls
- Mocking file operations
- Mocking process management
- Mocking logger calls

## SSL/TLS Testing

### Quick Start: SSL Development/Testing

#### 1. Generate Self-Signed Certificate

```bash
# Use the helper script (recommended)
bash tests/setup_ssl_test.sh .

# Or manually generate
openssl req -x509 -newkey rsa:4096 -nodes -days 365 \
  -out cert.pem -keyout key.pem \
  -subj "/C=CN/ST=BJ/L=Beijing/O=TDengine/CN=localhost"
```

#### 2. Start TDgpt with SSL

```bash
# Method 1: Command-line arguments
python -m taosanalytics.app --cert cert.pem --key key.pem

# Method 2: Configuration file
# Edit taosanode.config.py and add:
# certfile = 'cert.pem'
# keyfile = 'key.pem'
python -m taosanalytics.app -c taosanode.config.py
```

#### 3. Test HTTPS Connection

```bash
# Quick test with curl (skip certificate verification for self-signed certs)
curl -k https://localhost:6035/status

# Or use the Python test script
python tests/test_ssl.py

# Test with specific URL
python tests/test_ssl.py -u https://your-server:6035
```

### Test Script Details

**test_ssl.py** - Automated HTTPS connectivity test
- Tests multiple endpoints: `/status`, `/list`, `/models`
- Supports custom URLs and CA certificate bundles
- Handles self-signed certificates with `--insecure` flag
- Usage: `python tests/test_ssl.py [--url URL] [--ca-bundle PATH] [--insecure]`

**setup_ssl_test.sh** - Helper script for certificate generation
- Generates 4096-bit RSA self-signed certificate
- Valid for 365 days
- Usage: `bash tests/setup_ssl_test.sh [output_dir]`

### Production SSL Setup

For Gunicorn production deployment, see [../SSL_SETUP.md](../SSL_SETUP.md):

```python
# In taosanode.config.py
certfile = '/path/to/cert.pem'
keyfile = '/path/to/key.pem'
ssl_version = 'TLSv1_2'
```

## Continuous Integration

To run tests in CI/CD pipeline:

```bash
pytest tests/ --cov=script/taosanode_service --cov-report=xml --junit-xml=test-results.xml
```

This generates:

- XML coverage report for coverage tools
- JUnit XML report for CI/CD integration
