#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_ROOT="/home/simon/dev/TDinternal/community/test"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <pytest args...>" >&2
  exit 1
fi

printf -v PYTEST_ARGS '%q ' "$@"
PYTEST_ARGS="${PYTEST_ARGS% }"
REMOTE_CMD="source ~/myenv/bin/activate && cd ${TEST_ROOT} && ./ci/pytest.sh pytest ${PYTEST_ARGS}"
"${SCRIPT_DIR}/remote_exec.sh" "$REMOTE_CMD"
