#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/capture" "$TMP/repo/source/taos-community/docs/zh"
touch "$TMP/repo/source/taos-community/docs/zh/test.md"

cat >"$TMP/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "diff" ] && [ "${2:-}" = "--name-only" ]; then
  printf '%s\n' "source/taos-community/docs/zh/test.md"
  printf '%s\n' "source/taos-community/docs/examples/java/Test.java"
  exit 0
fi

if [ "${1:-}" = "diff" ] && [ "${2:-}" = "--quiet" ]; then
  exit 1
fi

if [ "${1:-}" = "diff" ]; then
  printf 'diff --git a/source/taos-community/docs/zh/test.md b/source/taos-community/docs/zh/test.md\n'
  printf -- '--- a/source/taos-community/docs/zh/test.md\n'
  printf -- '+++ b/source/taos-community/docs/zh/test.md\n'
  printf '@@ -1 +1 @@\n'
  printf -- '-bad\n'
  printf -- '+good\n'
  exit 0
fi

printf 'unexpected git invocation: %s\n' "$*" >&2
exit 2
EOF

cat >"$TMP/bin/autocorrect" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${CAPTURE_DIR}/autocorrect.args"
printf 'verbose autocorrect output should be captured\n'
EOF

cat >"$TMP/bin/markdownlint-cli2" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${CAPTURE_DIR}/markdownlint.args"
printf 'verbose markdownlint output should be captured\n'
EOF

chmod +x "$TMP/bin/"*

autofix_output=$(
  cd "$TMP/repo"
  PATH="$TMP/bin:$PATH" \
  CAPTURE_DIR="$TMP/capture" \
  CI_MERGE_REQUEST_DIFF_BASE_SHA=base \
  CI_COMMIT_SHA=head \
    bash "$ROOT/.gitlab/scripts/tsdb-docs-ci/autofix.sh"
)

grep -F "source/taos-community/docs/zh/test.md" "$TMP/capture/autocorrect.args"
grep -F -- "--fix" "$TMP/capture/autocorrect.args"
if grep -F "source/taos-community/docs/examples/java/Test.java" "$TMP/capture/autocorrect.args"; then
  echo "autocorrect autofix must only run on changed markdown files" >&2
  exit 1
fi
if grep -F -- "--lint" "$TMP/capture/autocorrect.args"; then
  echo "autocorrect autofix must not run in lint mode" >&2
  exit 1
fi

grep -F -- "--fix" "$TMP/capture/markdownlint.args"
grep -F ".markdownlint-cli2.jsonc" "$TMP/capture/markdownlint.args"
grep -F "source/taos-community/docs/zh/test.md" "$TMP/capture/markdownlint.args"
grep -F "docs autofix finished; review changes with git diff" <<<"${autofix_output}"
if compgen -G "$TMP/repo/docs-autofix*" >/dev/null; then
  echo "local autofix must not generate CI patch artifacts" >&2
  exit 1
fi
