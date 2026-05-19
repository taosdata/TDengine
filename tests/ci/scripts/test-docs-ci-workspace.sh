#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
SCRATCH="${ROOT}/temp/test-docs-ci-workspace.$$"
rm -rf "$SCRATCH"
trap 'rm -rf "$SCRATCH"' EXIT

mkdir -p "$SCRATCH/bin" "$SCRATCH/work"

cat >"$SCRATCH/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [ "$1" = "clone" ]; then
  target="${@: -1}"
  mkdir -p "$target/.git" "$target/docs" "$target/source/taos-community/docs/zh" "$target/source/taos-community/docs/en" "$target/source/taos-community/tools/taos-tools/example"
  touch "$target/docs/ROOT_DOC_MARKER" "$target/source/taos-community/docs/zh/SOURCE_DOC_MARKER"
  touch "$target/source/taos-community/tools/taos-tools/example/insert.json"
  exit 0
fi

if [ "$1" = "-C" ]; then
  repo="$2"
  shift 2
  case "${1:-}" in
    remote)
      if [ "${2:-}" = "set-url" ] && [ "${3:-}" = "origin" ]; then
        if [ "$repo" = "${DOCS_CI_WORKDIR}/docs.taosdata.com" ] || [ "$repo" = "${DOCS_CI_WORKDIR}/docs.tdengine.com" ]; then
          echo "prepare-workspace must not rewrite existing docs origin" >&2
          exit 1
        fi
      fi
      ;;
    fetch)
      if [ "$repo" = "${DOCS_CI_WORKDIR}/tsdb" ]; then
        echo "prepare-workspace must not fetch the existing tsdb checkout" >&2
        exit 1
      fi
      if [ "$repo" = "${DOCS_CI_WORKDIR}/docs.taosdata.com" ] || [ "$repo" = "${DOCS_CI_WORKDIR}/docs.tdengine.com" ]; then
        echo "prepare-workspace must not fetch existing docs checkout" >&2
        exit 1
      fi
      exit 0
      ;;
    checkout)
      if [ "${2:-}" = "--" ] && [ "${3:-}" = "docs" ]; then
        echo "prepare-workspace must not check out tsdb/docs" >&2
        exit 1
      fi
      exit 0
      ;;
  esac
  exit 0
fi

exit 0
EOF

chmod +x "$SCRATCH/bin/git"

mkdir -p "$SCRATCH/work/tsdb/.git" \
  "$SCRATCH/work/tsdb/source/taos-community/docs/zh" \
  "$SCRATCH/work/tsdb/source/taos-community/docs/en" \
  "$SCRATCH/work/tsdb/docs" \
  "$SCRATCH/work/docs.taosdata.com/.git" \
  "$SCRATCH/work/docs.tdengine.com/.git"
touch "$SCRATCH/work/tsdb/docs/ROOT_DOC_MARKER"
touch "$SCRATCH/work/tsdb/source/taos-community/docs/zh/SOURCE_DOC_MARKER"

PATH="$SCRATCH/bin:$PATH" \
DOCS_CI_WORKDIR="$SCRATCH/work" \
TSDB_REPO_URL="https://example.invalid/tsdb.git" \
ZH_DOC_REPO_URL="https://example.invalid/docs.taosdata.com.git" \
EN_DOC_REPO_URL="https://example.invalid/docs.tdengine.com.git" \
bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/prepare-workspace.sh"

test -d "$SCRATCH/work/tsdb/docs"
test ! -L "$SCRATCH/work/tsdb/docs"
test -f "$SCRATCH/work/tsdb/docs/ROOT_DOC_MARKER"
test -f "$SCRATCH/work/tsdb/source/taos-community/docs/zh/SOURCE_DOC_MARKER"
test ! -e "$SCRATCH/work/TDengine/docs"
test ! -e "$SCRATCH/work/tsdb/tools/taos-tools"
test -d "$SCRATCH/work/docs.taosdata.com/.git"
test -d "$SCRATCH/work/docs.tdengine.com/.git"
