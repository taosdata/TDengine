#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TMP="${ROOT}/temp/test-docs-ci-build.$$.${RANDOM}"
rm -rf "$TMP"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/capture"

cat >"$TMP/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# Simulated git for tests. Behavior varies by GIT_DIFF_MODE:
# - examples (default): prints examples path
# - zh: prints a zh path
# - en: prints an en path
# - unknown: prints an unknown docs subtree path to trigger conservative behavior

if [ "$1" = "clone" ]; then
  target="${@: -1}"
  mkdir -p "$target/.git" "$target/source/taos-community/docs/examples" "$target/docs"
  touch "$target/docs/ROOT_DOC_MARKER" "$target/source/taos-community/docs/examples/SOURCE_DOC_MARKER"
  exit 0
fi

if [ "$1" = "-C" ]; then
  repo="$2"
  if [ "${3:-}" = "checkout" ] && [ "${4:-}" = "--" ] && [ "${5:-}" = "docs" ]; then
    echo "build-doc must not check out tsdb/docs" >&2
    exit 1
  fi
  exit 0
fi

if [ "$1" = "pull" ]; then
  if [ "$PWD" = "${DOCS_CI_WORKDIR}/docs.taosdata.com" ] || [ "$PWD" = "${DOCS_CI_WORKDIR}/docs.tdengine.com" ]; then
    echo "build-doc must not pull existing docs checkout" >&2
    exit 1
  fi
fi

if [ "$1" = "checkout" ]; then
  if [ "$PWD" = "${DOCS_CI_WORKDIR}/docs.taosdata.com" ] || [ "$PWD" = "${DOCS_CI_WORKDIR}/docs.tdengine.com" ]; then
    echo "build-doc must not switch docs branches inside the container" >&2
    exit 1
  fi
fi

if [ "$1" = "diff" ]; then
  mode="${GIT_DIFF_MODE:-examples}"
  case "$mode" in
    examples)
      printf '%s\n' "source/taos-community/docs/examples/ws.mdx"
      ;;
    zh)
      printf '%s\n' "source/taos-community/docs/zh/guide.mdx"
      ;;
    en)
      printf '%s\n' "source/taos-community/docs/en/guide.mdx"
      ;;
    unknown)
      printf '%s\n' "source/taos-community/docs/otherlang/unknown.mdx"
      ;;
  esac
  exit 0
fi

exit 0
EOF

cat >"$TMP/bin/yarn" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Log the working directory and the full yarn command (include the 'yarn' literal)
printf '%s:yarn %s\n' "$PWD" "$*" >> "${CAPTURE_DIR}/yarn.log"
EOF

chmod +x "$TMP/bin/"*

mkdir -p "$TMP/work/tsdb/.git" \
  "$TMP/work/tsdb/source/taos-community/docs/examples" \
  "$TMP/work/tsdb/source/taos-community/docs/zh" \
  "$TMP/work/tsdb/source/taos-community/docs/en" \
  "$TMP/work/tsdb/docs" \
  "$TMP/work/docs.taosdata.com/.git" \
  "$TMP/work/docs.tdengine.com/.git"
touch "$TMP/work/tsdb/docs/ROOT_DOC_MARKER"
touch "$TMP/work/tsdb/source/taos-community/docs/examples/SOURCE_DOC_MARKER"

# Run five scenarios to exercise zh-only, en-only, unknown (conservative fallback), examples (both) and the unset-CI-vars conservative fallback.
for mode in zh en unknown examples ci-unset; do
  if [ "$mode" = "ci-unset" ]; then
    # Do not set CI_* variables to simulate the CI environment where they are
    # unavailable. Use a GIT_DIFF_MODE value (examples) but the lack of CI vars
    # should cause a conservative build of both zh and en.
    PATH="$TMP/bin:$PATH" \
    CAPTURE_DIR="$TMP/capture" \
    DOCS_CI_WORKDIR="$TMP/work" \
    GIT_DIFF_MODE=examples \
    bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/build-doc.sh"
  else
    PATH="$TMP/bin:$PATH" \
    CAPTURE_DIR="$TMP/capture" \
    DOCS_CI_WORKDIR="$TMP/work" \
    CI_MERGE_REQUEST_DIFF_BASE_SHA=base \
    CI_COMMIT_SHA=head \
    GIT_DIFF_MODE="$mode" \
    bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/build-doc.sh"
  fi

  # Check expected yarn invocations per mode
  if [ "$mode" = "zh" ]; then
    grep -F "$TMP/work/docs.taosdata.com:yarn install" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.taosdata.com:yarn ass local" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.taosdata.com:yarn build" "$TMP/capture/yarn.log"
    # en should not run
    ! grep -F "$TMP/work/docs.tdengine.com:yarn install" "$TMP/capture/yarn.log" || (echo "en ran unexpectedly" && false)
  elif [ "$mode" = "en" ]; then
    grep -F "$TMP/work/docs.tdengine.com:yarn install" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn ass local" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn build" "$TMP/capture/yarn.log"
    # zh should not run
    ! grep -F "$TMP/work/docs.taosdata.com:yarn install" "$TMP/capture/yarn.log" || (echo "zh ran unexpectedly" && false)
  elif [ "$mode" = "unknown" ] || [ "$mode" = "ci-unset" ]; then
    # unknown and the CI-vars-unset scenario both trigger conservative fallback:
    # both zh and en should be built
    grep -F "$TMP/work/docs.taosdata.com:yarn install" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.taosdata.com:yarn ass local" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.taosdata.com:yarn build" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn install" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn ass local" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn build" "$TMP/capture/yarn.log"
  else
    # examples: both should run
    grep -F "$TMP/work/docs.taosdata.com:yarn install" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.taosdata.com:yarn ass local" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.taosdata.com:yarn build" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn install" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn ass local" "$TMP/capture/yarn.log"
    grep -F "$TMP/work/docs.tdengine.com:yarn build" "$TMP/capture/yarn.log"
  fi

  # tracked docs tree should remain untouched
  test -d "$TMP/work/tsdb/docs"
  test ! -L "$TMP/work/tsdb/docs"
  test -f "$TMP/work/tsdb/docs/ROOT_DOC_MARKER"
  test -f "$TMP/work/tsdb/source/taos-community/docs/examples/SOURCE_DOC_MARKER"
  test -d "$TMP/work/TDengine/docs"

  # Clean capture log for next iteration
  rm -f "$TMP/capture/yarn.log"
done
