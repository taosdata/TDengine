#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
SCRATCH="${ROOT}/temp/test-docs-ci-docker.$$"
rm -rf "$SCRATCH"
trap 'rm -rf "$SCRATCH"' EXIT

mkdir -p "$SCRATCH/bin" "$SCRATCH/capture" "$SCRATCH/work"

cat >"$SCRATCH/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$@" > "${CAPTURE_DIR}/docker.args"
EOF

cat >"$SCRATCH/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'git %s\n' "$*" >> "${CAPTURE_DIR}/git.args"

if [ "$1" = "clone" ]; then
  target="${@: -1}"
  mkdir -p "$target/.git"
  exit 0
fi

exit 0
EOF

chmod +x "$SCRATCH/bin/docker" "$SCRATCH/bin/git"

PATH="$SCRATCH/bin:$PATH" \
CAPTURE_DIR="$SCRATCH/capture" \
DOCS_CI_WORKDIR="$SCRATCH/work" \
DOCS_CI_IMAGE="example.invalid/docs-ci:latest" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" bash .gitlab/scripts/tsdb-docs-ci/check-typos.sh

cat >"$SCRATCH/expected.args" <<EOF
run
--rm
-v
$SCRATCH/work:$SCRATCH/work
-v
$ROOT:$SCRATCH/work/tsdb
-e
DOCS_CI_WORKDIR=$SCRATCH/work
-e
TSDB_DIR=$SCRATCH/work/tsdb
-e
ZH_DOC_DIR=$SCRATCH/work/docs.taosdata.com
-e
EN_DOC_DIR=$SCRATCH/work/docs.tdengine.com
-w
$SCRATCH/work/tsdb
example.invalid/docs-ci:latest
bash
.gitlab/scripts/tsdb-docs-ci/check-typos.sh
EOF

cmp -s "$SCRATCH/expected.args" "$SCRATCH/capture/docker.args"

mkdir -p "$SCRATCH/equal/tsdb/.gitlab/scripts/tsdb-docs-ci" "$SCRATCH/equal/capture"
cp "$ROOT/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" "$SCRATCH/equal/tsdb/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh"
cp "$ROOT/.gitlab/scripts/tsdb-docs-ci/common.sh" "$SCRATCH/equal/tsdb/.gitlab/scripts/tsdb-docs-ci/common.sh"

PATH="$SCRATCH/bin:$PATH" \
CAPTURE_DIR="$SCRATCH/equal/capture" \
DOCS_CI_WORKDIR="$SCRATCH/equal" \
DOCS_CI_IMAGE="example.invalid/docs-ci:latest" \
bash "$SCRATCH/equal/tsdb/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" bash .gitlab/scripts/tsdb-docs-ci/check-typos.sh

cat >"$SCRATCH/equal/expected.args" <<EOF
run
--rm
-v
$SCRATCH/equal:$SCRATCH/equal
-e
DOCS_CI_WORKDIR=$SCRATCH/equal
-e
TSDB_DIR=$SCRATCH/equal/tsdb
-e
ZH_DOC_DIR=$SCRATCH/equal/docs.taosdata.com
-e
EN_DOC_DIR=$SCRATCH/equal/docs.tdengine.com
-w
$SCRATCH/equal/tsdb
example.invalid/docs-ci:latest
bash
.gitlab/scripts/tsdb-docs-ci/check-typos.sh
EOF

cmp -s "$SCRATCH/equal/expected.args" "$SCRATCH/equal/capture/docker.args"

# --- CI env vars are forwarded when set ---
mkdir -p "$SCRATCH/cienv/capture"

CI_COMMIT_SHA=abc123 \
CI_MERGE_REQUEST_DIFF_BASE_SHA=def456 \
PATH="$SCRATCH/bin:$PATH" \
CAPTURE_DIR="$SCRATCH/cienv/capture" \
DOCS_CI_WORKDIR="$SCRATCH/work" \
DOCS_CI_IMAGE="example.invalid/docs-ci:latest" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" bash .gitlab/scripts/tsdb-docs-ci/check-typos.sh

cat >"$SCRATCH/cienv/expected.args" <<EOF
run
--rm
-v
$SCRATCH/work:$SCRATCH/work
-v
$ROOT:$SCRATCH/work/tsdb
-e
DOCS_CI_WORKDIR=$SCRATCH/work
-e
TSDB_DIR=$SCRATCH/work/tsdb
-e
ZH_DOC_DIR=$SCRATCH/work/docs.taosdata.com
-e
EN_DOC_DIR=$SCRATCH/work/docs.tdengine.com
-e
CI_COMMIT_SHA=abc123
-e
CI_MERGE_REQUEST_DIFF_BASE_SHA=def456
-w
$SCRATCH/work/tsdb
example.invalid/docs-ci:latest
bash
.gitlab/scripts/tsdb-docs-ci/check-typos.sh
EOF

cmp -s "$SCRATCH/cienv/expected.args" "$SCRATCH/cienv/capture/docker.args"

mkdir -p "$SCRATCH/buildprep/capture"

PATH="$SCRATCH/bin:$PATH" \
CAPTURE_DIR="$SCRATCH/buildprep/capture" \
DOCS_CI_WORKDIR="$SCRATCH/buildprep/work" \
DOCS_CI_IMAGE="example.invalid/docs-ci:latest" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/run-in-docker.sh" bash .gitlab/scripts/tsdb-docs-ci/build-doc.sh

cat >"$SCRATCH/buildprep/expected.git.args" <<EOF
git clone https://github.com/taosdata/docs.taosdata.com.git $SCRATCH/buildprep/work/docs.taosdata.com
git -C $SCRATCH/buildprep/work/docs.taosdata.com remote set-url origin https://github.com/taosdata/docs.taosdata.com.git
git -C $SCRATCH/buildprep/work/docs.taosdata.com fetch origin feat/tsdb-path-env --prune
git -C $SCRATCH/buildprep/work/docs.taosdata.com reset --hard FETCH_HEAD
git -C $SCRATCH/buildprep/work/docs.taosdata.com clean -fd
git -C $SCRATCH/buildprep/work/docs.taosdata.com checkout -B feat/tsdb-path-env FETCH_HEAD
git clone https://github.com/taosdata/docs.tdengine.com.git $SCRATCH/buildprep/work/docs.tdengine.com
git -C $SCRATCH/buildprep/work/docs.tdengine.com remote set-url origin https://github.com/taosdata/docs.tdengine.com.git
git -C $SCRATCH/buildprep/work/docs.tdengine.com fetch origin feat/tsdb-path-env --prune
git -C $SCRATCH/buildprep/work/docs.tdengine.com reset --hard FETCH_HEAD
git -C $SCRATCH/buildprep/work/docs.tdengine.com clean -fd
git -C $SCRATCH/buildprep/work/docs.tdengine.com checkout -B feat/tsdb-path-env FETCH_HEAD
EOF

cmp -s "$SCRATCH/buildprep/expected.git.args" "$SCRATCH/buildprep/capture/git.args"

echo "All docker wrapper tests passed."
