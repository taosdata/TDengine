#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/capture" "$TMP/repo/source/taos-community/docs/zh" "$TMP/repo/source/taos-community/docs/examples/java"
touch "$TMP/repo/source/taos-community/docs/zh/test.md"
touch "$TMP/repo/source/taos-community/docs/examples/java/Test.java"

cat >"$TMP/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Verify invocation shape: git diff --name-only <base> <head> -- source/taos-community/docs
if [ "${1:-}" != "diff" ]; then
  printf 'unexpected git subcommand: %s\n' "${1:-}" >&2
  exit 2
fi
if [ "${2:-}" != "--name-only" ]; then
  printf 'expected --name-only as second arg, got: %s\n' "${2:-}" >&2
  exit 2
fi
# Ensure we have at least: diff --name-only <base> <head> -- path
if [ "$#" -lt 6 ]; then
  printf 'git diff invocation too short: %s\n' "$*" >&2
  exit 2
fi
if [ "${5:-}" != "--" ]; then
  printf 'expected -- before path filter, got: %s\n' "${5:-}" >&2
  exit 2
fi
if [ "${6:-}" != "source/taos-community/docs" ]; then
  printf 'expected path filter source/taos-community/docs, got: %s\n' "${6:-}" >&2
  exit 2
fi
printf '%s\n' "source/taos-community/docs/zh/test.md"
printf '%s\n' "source/taos-community/docs/zh/deleted.md"
printf '%s\n' "source/taos-community/docs/examples/java/Test.java"
EOF

cat >"$TMP/bin/typos" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${CAPTURE_DIR}/typos.args"
EOF

cat >"$TMP/bin/autocorrect" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${CAPTURE_DIR}/autocorrect.args"
EOF

cat >"$TMP/bin/markdownlint-cli2" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${CAPTURE_DIR}/markdownlint.args"
EOF

chmod +x "$TMP/bin/"*

(
  cd "$TMP/repo"
  PATH="$TMP/bin:$PATH" CAPTURE_DIR="$TMP/capture" CI_MERGE_REQUEST_DIFF_BASE_SHA=base CI_COMMIT_SHA=head \
    bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/check-typos.sh"
  PATH="$TMP/bin:$PATH" CAPTURE_DIR="$TMP/capture" CI_MERGE_REQUEST_DIFF_BASE_SHA=base CI_COMMIT_SHA=head \
    bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/check-autocorrect.sh"
  PATH="$TMP/bin:$PATH" CAPTURE_DIR="$TMP/capture" CI_MERGE_REQUEST_DIFF_BASE_SHA=base CI_COMMIT_SHA=head \
    bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/check-markdownlint.sh"
)

grep -F "source/taos-community/docs/zh/test.md" "$TMP/capture/typos.args"
grep -F "source/taos-community/docs/examples/java/Test.java" "$TMP/capture/typos.args"
grep -F -- "--config" "$TMP/capture/typos.args"
grep -F -- "--lint" "$TMP/capture/autocorrect.args"
grep -F "source/taos-community/docs/zh/test.md" "$TMP/capture/autocorrect.args"
if grep -F "source/taos-community/docs/examples/java/Test.java" "$TMP/capture/autocorrect.args"; then
  echo "autocorrect must only lint changed markdown files" >&2
  exit 1
fi
grep -F ".markdownlint-cli2.jsonc" "$TMP/capture/markdownlint.args"
grep -F "source/taos-community/docs/zh/test.md" "$TMP/capture/markdownlint.args"
if grep -F "source/taos-community/docs/zh/deleted.md" "$TMP/capture/markdownlint.args"; then
  echo "markdownlint must skip deleted markdown files" >&2
  exit 1
fi
