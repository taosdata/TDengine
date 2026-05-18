#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${ROOT}/tests/smoke/.tmp/test-prepare-externals-verify-warning.$$"
mkdir -p "${TMP}"
trap 'rm -rf "${TMP}"' EXIT

BIN_DIR="${TMP}/bin"
mkdir -p "${BIN_DIR}"

cat > "${BIN_DIR}/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

url="${*: -1}"
case "${url}" in
  *foo-v1.tar.gz|*bar-2.0.tar.gz)
    printf '200'
    ;;
  *)
    printf '404'
    ;;
esac
EOF
chmod +x "${BIN_DIR}/curl"

SCRIPT_LINK="${TMP}/prepare-externals.sh"
ln -s "${ROOT}/scripts/prepare-externals.sh" "${SCRIPT_LINK}"

cat > "${TMP}/external.cmake" <<'EOF'
get_from_local_if_exists(
    "https://example.com/foo/archive/refs/tags/v1.tar.gz"
    "foo-v1.tar.gz"
)
get_from_local_if_exists("https://example.com/releases/bar-2.0.tar.gz")
EOF

cat > "${TMP}/externals-manifest.txt" <<'EOF'
# stale on purpose
deadbeef  foo-v1.tar.gz
EOF
cp "${TMP}/externals-manifest.txt" "${TMP}/externals-manifest.before"

PATH="${BIN_DIR}:$PATH" \
GITLAB_TOKEN=dummy \
GITLAB_PROJECT_ID=70 \
bash "${SCRIPT_LINK}" --cmake "${TMP}/external.cmake" --verify > "${TMP}/run.out" 2>&1

grep -F -q "Results: 2 OK, 0 missing, 0 errors" "${TMP}/run.out"
grep -F -q "[WARN] Manifest is stale" "${TMP}/run.out"
cmp -s "${TMP}/externals-manifest.before" "${TMP}/externals-manifest.txt"

echo "PASS"
