#!/bin/bash
set -e

# This script converts .profraw files to .lcov format inside the container
# It must run in the same environment where the instrumented binaries were executed

COVERAGE_DIR="${1:-/data/coverage}"
OUTPUT_FILE="${2:-/tmp/integration-test-coverage.lcov}"

echo "Converting coverage files in $COVERAGE_DIR to $OUTPUT_FILE"

# Find all profraw files
mapfile -t PROFRAW_FILES < <(find "$COVERAGE_DIR" -name "coverage-*.profraw" 2>/dev/null | sort || true)

if [ "${#PROFRAW_FILES[@]}" -eq 0 ]; then
    echo "Warning: No .profraw files found in $COVERAGE_DIR"
    exit 0
fi

echo "Found ${#PROFRAW_FILES[@]} profraw files:"
printf '%s\n' "${PROFRAW_FILES[@]}"

# Get llvm-profdata and llvm-cov paths
LLVM_PROFDATA=$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | grep host | cut -d' ' -f2)/bin/llvm-profdata
LLVM_COV=$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | grep host | cut -d' ' -f2)/bin/llvm-cov

if [ ! -f "$LLVM_PROFDATA" ]; then
    echo "Error: llvm-profdata not found at $LLVM_PROFDATA"
    exit 1
fi

if [ ! -f "$LLVM_COV" ]; then
    echo "Error: llvm-cov not found at $LLVM_COV"
    exit 1
fi

# Merge profraw files
MERGED_PROFDATA="$COVERAGE_DIR/merged.profdata"
echo "Merging profraw files to $MERGED_PROFDATA"
"$LLVM_PROFDATA" merge -sparse "${PROFRAW_FILES[@]}" -o "$MERGED_PROFDATA"

# Find instrumented binaries
BINARIES=()
for binary in /usr/bin/taosx /usr/bin/taosx-agent /usr/bin/taos-explorer /usr/bin/xnoded; do
    if [ -f "$binary" ]; then
        BINARIES+=(--object "$binary")
    fi
done

if [ "${#BINARIES[@]}" -eq 0 ]; then
    echo "Error: No instrumented binaries found"
    exit 1
fi

# Export to lcov format
echo "Generating lcov file: $OUTPUT_FILE"
"$LLVM_COV" export "${BINARIES[@]}" \
    --instr-profile="$MERGED_PROFDATA" \
    --format=lcov \
    --ignore-filename-regex='/.cargo/registry' \
    --ignore-filename-regex='/rustc/' \
    > "$OUTPUT_FILE"

echo "✅ Coverage conversion complete: $OUTPUT_FILE"
ls -lh "$OUTPUT_FILE"
