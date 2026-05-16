#!/bin/bash
set -euo pipefail

# Determine the script's directory as BASE_DIR
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
BASE_DIR="${SCRIPT_DIR}"

# Configuration parameters
BUILD_DIR="${BASE_DIR}/debug"
OUTPUT_DIR="${BASE_DIR}/coverage_report"
REPORT_TITLE="ODBC Core Coverage"

# Step 1: Collect raw coverage data
echo "[1/4] Collecting raw coverage data..."
lcov --capture \
    --directory "${BUILD_DIR}" \
    --base-directory "${BASE_DIR}" \
    --output-file "${BASE_DIR}/coverage.info" \
    --rc branch_coverage=1 \
    --no-external

# Step 2: Extract src directory data
echo "[2/4] Extracting src directory data..."
lcov --extract "${BASE_DIR}/coverage.info" \
    "${BASE_DIR}/src/**" \
    --output-file "${BASE_DIR}/src_coverage.info"

# Step 3: Filter out test directories
echo "[3/4] Filtering test directories..."
lcov --remove "${BASE_DIR}/src_coverage.info" \
    "*/src/tests/*" \
    --output-file "${BASE_DIR}/src_coverage_filtered.info"

# Step 4: Generate HTML report
echo "[4/4] Generating HTML report..."
rm -rf "${OUTPUT_DIR}"
genhtml "${BASE_DIR}/src_coverage_filtered.info" \
    --output-directory "${OUTPUT_DIR}" \
    --title "${REPORT_TITLE}" \
    --prefix "${BASE_DIR}" \
    --legend \
    --demangle-cpp \
    --branch-coverage

echo "Done! Coverage report has been generated at: ${OUTPUT_DIR}"
