# Coverage Testing Quick Start Guide

## Prerequisites
```bash
# Install cargo-llvm-cov and cargo-nextest
cargo install cargo-llvm-cov cargo-nextest
```

## Generate Coverage Reports

### Full Workspace Coverage
```bash
# Generate LCOV report (for CI/Codecov)
cargo make test

# This runs:
# - cargo llvm-cov for taosx (excluding explorer)
# - cargo llvm-cov for taos-explorer separately
# - Merges both reports into target/llvm-cov-merged.lcov
```

### HTML Coverage Report (Visual Inspection)
```bash
# Generate and open HTML report in browser
cargo llvm-cov --html --open nextest --workspace --exclude taos-explorer
```

The HTML report will be saved to `target/llvm-cov/html/index.html`

### Single Package Coverage
```bash
# Test only taosx-core
cargo llvm-cov nextest -p taosx-core

# Test with specific features
cargo llvm-cov nextest --all-features
cargo llvm-cov nextest --no-default-features --features rustls
```

## Run Specific Tests

### By Module
```bash
# Run tests in a specific module
cargo nextest run -p taosx-core lib::tests::
cargo nextest run -p taosx-core migrations::users::tests::
cargo nextest run -p taosx-core migrations::privileges::tests::
```

### By Test Name
```bash
# Run specific test
cargo nextest run -p taosx-core test_is_expired_day_with_expired_license

# Run tests matching pattern
cargo nextest run -p taosx-core ".*expired.*"
```

### List All Tests
```bash
# List all available tests
cargo nextest list

# List tests in specific package
cargo nextest list -p taosx-core
```

## Coverage Analysis

### Parse LCOV Report
```bash
# Extract coverage summary
python3 << 'EOF'
import re

with open('target/llvm-cov-merged.lcov', 'r') as f:
    content = f.read()

files = content.split('end_of_record')
total_found = total_hit = 0

for record in files:
    if '/taosx-core/src/' not in record:
        continue
    lf = re.search(r'LF:(\d+)', record)
    lh = re.search(r'LH:(\d+)', record)
    if lf and lh:
        total_found += int(lf.group(1))
        total_hit += int(lh.group(1))

pct = total_hit/total_found*100 if total_found else 0
print(f'taosx-core coverage: {pct:.1f}% ({total_hit}/{total_found} lines)')
EOF
```

### View Coverage by Module
```bash
# Generate summary by directory
lcov --summary target/llvm-cov-merged.lcov
```

## CI Integration

### GitHub Actions Workflow
Coverage is automatically generated in CI:
- Workflow: `.github/workflows/cov.yaml`
- Trigger: Push to `main` or `3.0` branches
- Artifacts: Coverage report uploaded to Codecov
- Report: `target/llvm-cov-merged.lcov`

### Local CI Simulation
```bash
# Run the same coverage as CI
cargo make test
```

## Writing Tests

### Unit Test Template
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_happy_path() {
        // Arrange
        let input = create_test_input();
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert_eq!(result, expected_value);
    }

    #[test]
    fn test_feature_error_case() {
        let input = create_invalid_input();
        let result = function_under_test(input);
        assert!(result.is_err());
    }
}
```

### Test Best Practices
1. **Isolated**: No external dependencies (network, filesystem, database)
2. **Deterministic**: Same input always produces same output
3. **Fast**: Unit tests should complete in milliseconds
4. **Clear**: Test name describes what is being tested
5. **Comprehensive**: Cover happy path, edge cases, and error conditions

### Use Test Helpers
```rust
// Create helper functions for common test setup
fn create_test_license(expire: i64) -> ConnectorLicense {
    ConnectorLicense {
        r#type: Some("test".to_string()),
        number: 1000,
        speed: 100,
        expire,
        expire_time: None,
    }
}
```

## Troubleshooting

### Tests Fail Only in Coverage Mode
- Check if tests rely on timing or external state
- Use deterministic time sources in tests
- Avoid tests that depend on execution order

### Coverage Report Not Generated
```bash
# Ensure tools are installed
cargo llvm-cov --version
cargo nextest --version

# Clean and retry
cargo clean
cargo make test
```

### HTML Report Shows "No coverage data"
- Ensure you're running tests with `nextest` or `test`
- Check that `llvm-cov` is using the correct profile
- Verify paths in LLVM_PROFILE_FILE

## Coverage Goals

### Current Status (2025-12-12)
- Overall: 60.5%
- Target: >70%

### Module Targets
- **High Priority** (>80%): lib.rs ✅, core_metrics, global, migrations
- **Medium Priority** (>70%): plugins/config, plugins/expr, runners
- **Low Priority** (>50%): utils, s3, tmq

## Resources
- [cargo-llvm-cov Documentation](https://github.com/taiki-e/cargo-llvm-cov)
- [nextest Documentation](https://nexte.st/)
- [Coverage Plan](./test-coverage-plan.md)
- [Progress Report](../../COVERAGE_REPORT.md)
