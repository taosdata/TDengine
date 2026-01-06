#!/bin/bash

# TaosX Integration Test Runner
# This script helps run various integration test configurations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Print usage
usage() {
    cat << EOF
TaosX Integration Test Runner

Usage: $0 [OPTIONS] [TEST_PATTERN]

OPTIONS:
    -h, --help          Show this help message
    -b, --basic         Run only basic API tests
    -e, --extended      Run only extended API tests
    -a, --all           Run all integration tests (default)
    -v, --verbose       Enable verbose output with --nocapture
    -s, --sequential    Run tests sequentially (--test-threads=1)
    -d, --debug         Enable debug logging (RUST_LOG=debug)
    -q, --quiet         Suppress test output
    --no-capture        Always show test output (alias for -v)

EXAMPLES:
    # Run all tests
    $0

    # Run only basic API tests with verbose output
    $0 -b -v

    # Run extended tests with debug logging
    $0 -e -d

    # Run specific test pattern
    $0 test_taosx_api

    # Run all tests sequentially with verbose output
    $0 -a -s -v

EOF
}

# Default values
TEST_PATTERN=""
VERBOSE=""
SEQUENTIAL=""
DEBUG=""
QUIET=""
TEST_FILTER="test_taosx_api"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -b|--basic)
            TEST_FILTER="test_taosx_api -- --exact"
            shift
            ;;
        -e|--extended)
            TEST_FILTER="test_taosx_api_extended -- --exact"
            shift
            ;;
        -a|--all)
            TEST_FILTER="test_taosx_api"
            shift
            ;;
        -v|--verbose|--no-capture)
            VERBOSE="--nocapture"
            shift
            ;;
        -s|--sequential)
            SEQUENTIAL="--test-threads=1"
            shift
            ;;
        -d|--debug)
            DEBUG="RUST_LOG=debug"
            shift
            ;;
        -q|--quiet)
            QUIET="--quiet"
            shift
            ;;
        -*)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            TEST_PATTERN="$1"
            shift
            ;;
    esac
done

# Set test filter if custom pattern provided
if [ -n "$TEST_PATTERN" ]; then
    TEST_FILTER="$TEST_PATTERN"
fi

# Build the command
CMD="cargo test -p taosx-integration-tests"

if [ -n "$QUIET" ]; then
    CMD="$CMD $QUIET"
fi

CMD="$CMD $TEST_FILTER"

# Add test arguments
if [ -n "$VERBOSE" ] || [ -n "$SEQUENTIAL" ]; then
    CMD="$CMD --"
    if [ -n "$VERBOSE" ]; then
        CMD="$CMD $VERBOSE"
    fi
    if [ -n "$SEQUENTIAL" ]; then
        CMD="$CMD $SEQUENTIAL"
    fi
fi

# Print test configuration
echo ""
echo "========================================="
echo "  TaosX Integration Test Runner"
echo "========================================="
echo ""
print_info "Test filter: $TEST_FILTER"
if [ -n "$VERBOSE" ]; then
    print_info "Verbose output: enabled"
fi
if [ -n "$SEQUENTIAL" ]; then
    print_info "Sequential execution: enabled"
fi
if [ -n "$DEBUG" ]; then
    print_info "Debug logging: enabled"
fi
echo ""
print_info "Running command:"
if [ -n "$DEBUG" ]; then
    echo "  $DEBUG $CMD"
else
    echo "  $CMD"
fi
echo ""
echo "========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    print_error "Cargo.toml not found. Please run from the taosx directory."
    exit 1
fi

# Run the tests
if [ -n "$DEBUG" ]; then
    eval "$DEBUG $CMD"
    EXIT_CODE=$?
else
    eval "$CMD"
    EXIT_CODE=$?
fi

echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    print_success "All tests passed!"
else
    print_error "Tests failed with exit code: $EXIT_CODE"
fi
echo "========================================="
echo ""

exit $EXIT_CODE
