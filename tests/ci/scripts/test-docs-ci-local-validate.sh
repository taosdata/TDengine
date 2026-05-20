#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TMP="${ROOT}/temp/test-docs-ci-local-validate.$$.${RANDOM}"
rm -rf "$TMP"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/work/tsdb/.git" "$TMP/capture" \
  "$TMP/work/tsdb/source/taos-community/docs/zh" \
  "$TMP/work/tsdb/source/taos-community/docs/en" \
  "$TMP/work/tsdb/source/taos-community/docs/examples"
touch "$TMP/docs-ci.tar.gz"

cat >"$TMP/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'git %s\n' "$*" >> "${CAPTURE_DIR}/git.log"

if [ "$1" = "clone" ]; then
  target="${@: -1}"
  mkdir -p "$target/.git"
  exit 0
fi

if [ "$1" = "-C" ]; then
  repo="$2"
  shift 2
  case "$1" in
    fetch)
      exit 0
      ;;
    checkout)
      exit 0
      ;;
    rev-parse)
      if [ "${2:-}" = "HEAD" ]; then
        printf 'headsha\n'
      elif [ "${2:-}" = "--show-toplevel" ]; then
        printf '%s\n' "$repo"
      else
        printf 'rev-%s\n' "${2:-unknown}"
      fi
      exit 0
      ;;
    merge-base)
      printf 'basesha\n'
      exit 0
      ;;
    status)
      exit 0
      ;;
  esac
fi

exit 0
EOF

cat >"$TMP/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'docker %s\n' "$*" >> "${CAPTURE_DIR}/docker.log"

if [ "$1" = "load" ]; then
  printf 'Loaded image: docs-ci:latest\n'
  exit 0
fi

exit 0
EOF

chmod +x "$TMP/bin/"*

PATH="$TMP/bin:$PATH" \
CAPTURE_DIR="$TMP/capture" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/local-validate.sh" \
  --workdir "$TMP/work" \
  --tsdb-dir "$TMP/work/tsdb" \
  --image-tar "$TMP/docs-ci.tar.gz" \
  --base-ref origin/main

grep -F "git clone https://github.com/taosdata/docs.taosdata.com.git $TMP/work/docs.taosdata.com" "$TMP/capture/git.log"
grep -F "git clone https://github.com/taosdata/docs.tdengine.com.git $TMP/work/docs.tdengine.com" "$TMP/capture/git.log"
! grep -F "git -C $TMP/work/tsdb checkout -B" "$TMP/capture/git.log" || (echo "specified tsdb repo should not be checked out" && false)
grep -F "git -C $TMP/work/docs.taosdata.com checkout -B feat/tsdb-path-env origin/feat/tsdb-path-env" "$TMP/capture/git.log"
grep -F "git -C $TMP/work/docs.tdengine.com checkout -B feat/tsdb-path-env origin/feat/tsdb-path-env" "$TMP/capture/git.log"

grep -F "docker load -i $TMP/docs-ci.tar.gz" "$TMP/capture/docker.log"
grep -F "docker tag docs-ci:latest docs-ci:local" "$TMP/capture/docker.log"
! grep -F "ZH_DOC_BRANCH=feat/tsdb-path-env" "$TMP/capture/docker.log" || (echo "docs branch selection should stay on the host" && false)
! grep -F "EN_DOC_BRANCH=feat/tsdb-path-env" "$TMP/capture/docker.log" || (echo "docs branch selection should stay on the host" && false)
grep -F "TSDB_DIR=$TMP/work/tsdb" "$TMP/capture/docker.log"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/check-typos.sh" "$TMP/capture/docker.log"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/check-autocorrect.sh" "$TMP/capture/docker.log"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/check-markdownlint.sh" "$TMP/capture/docker.log"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/build-doc.sh" "$TMP/capture/docker.log"
if grep -F "bash .gitlab/scripts/tsdb-docs-ci/autofix.sh" "$TMP/capture/docker.log"; then
  echo "local validation must only autofix when --fix is requested" >&2
  exit 1
fi

: > "$TMP/capture/docker.log"

PATH="$TMP/bin:$PATH" \
CAPTURE_DIR="$TMP/capture" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/local-validate.sh" \
  --workdir "$TMP/work" \
  --tsdb-dir "$TMP/work/tsdb" \
  --image-tar "$TMP/docs-ci.tar.gz" \
  --base-ref origin/main \
  --fix

grep -F "bash .gitlab/scripts/tsdb-docs-ci/autofix.sh" "$TMP/capture/docker.log"

: > "$TMP/capture/git.log"
: > "$TMP/capture/docker.log"

PATH="$TMP/bin:$PATH" \
CAPTURE_DIR="$TMP/capture" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/local-validate.sh" \
  --workdir "$TMP/work" \
  --image-tar "$TMP/docs-ci.tar.gz" \
  --base-ref origin/main \
  --fix

grep -F "TSDB_DIR=$ROOT" "$TMP/capture/docker.log"
grep -F "bash .gitlab/scripts/tsdb-docs-ci/autofix.sh" "$TMP/capture/docker.log"
if grep -F "git -C $ROOT checkout -B" "$TMP/capture/git.log"; then
  echo "default current tsdb repo must not be checked out unless --tsdb-branch is set" >&2
  exit 1
fi
if grep -F "git -C $TMP/work/tsdb" "$TMP/capture/git.log"; then
  echo "default local validation must use the current checkout, not WORKDIR/tsdb" >&2
  exit 1
fi
