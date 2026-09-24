#!/usr/bin/env bash

set -euo pipefail

if (($# != 3)); then
  printf 'Usage: %s CORE_TEST FAKE_NATIVE_DIR FAKE_BIN_DIR\n' "$0" >&2
  exit 2
fi

core_test=$1
fake_native=$2
fake_bin=$3

test -x "$core_test"
test -f "$fake_native/libtaos.so"
test -x "$fake_bin/taosd"

# The focused C regression injects one pair-supervisor crash, verifies recovery
# and the reaped pair facts, and runs the supported coverage path only.
QST_CORE_PAIR_CRASH_ONLY=1 \
LD_LIBRARY_PATH="$fake_native${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
PATH="$fake_bin:$PATH" \
  "$core_test"
