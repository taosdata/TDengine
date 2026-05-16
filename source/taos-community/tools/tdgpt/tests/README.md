# TDGPT Service Manager Tests

This directory contains unit tests for the TDGPT service management system.

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

- **conftest.py**: Shared pytest fixtures and configuration
- **test_process_manager.py**: Tests for ProcessManager class
- **test_config.py**: Tests for Config class
- **test_taosanode_service.py**: Tests for TaosanodeService class
- **test_model_service.py**: Tests for ModelService class

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

## Continuous Integration

To run tests in CI/CD pipeline:

```bash
pytest tests/ --cov=script/taosanode_service --cov-report=xml --junit-xml=test-results.xml
```

This generates:

- XML coverage report for coverage tools
- JUnit XML report for CI/CD integration
