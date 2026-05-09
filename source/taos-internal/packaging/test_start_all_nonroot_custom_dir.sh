#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_START_ALL="${SCRIPT_DIR}/start-all.sh"
WORKDIR="${SCRIPT_DIR}/.test_start_all_nonroot_custom_dir.$$"
FAKE_HOME="${WORKDIR}/home"
INSTALL_ROOT="${WORKDIR}/custom install/taos"
INSTALL_BIN="${INSTALL_ROOT}/bin"
INSTALL_CFG="${INSTALL_ROOT}/cfg"
FAKE_BIN="${WORKDIR}/fake-bin"
TEST_TMPDIR="${WORKDIR}/tmp"

cleanup() {
    rm -rf "${WORKDIR}"
}
trap cleanup EXIT

mkdir -p "${FAKE_HOME}/.local/bin" "${FAKE_HOME}/taos/cfg" "${INSTALL_BIN}" "${INSTALL_CFG}" "${FAKE_BIN}" "${TEST_TMPDIR}"
cp "${SOURCE_START_ALL}" "${INSTALL_BIN}/start-all.sh"
chmod +x "${INSTALL_BIN}/start-all.sh"
printf 'fqdn test.example.com\n' > "${INSTALL_CFG}/taos.cfg"

cat > "${INSTALL_BIN}/taos" <<'TAOS_EOF'
#!/bin/bash
printf '<install-local>\n' >> "${TAOS_LOG}"
printf '<%s>\n' "$@" >> "${TAOS_LOG}"
case "$*" in
    *"select server_status();"*)
        exit 0
        ;;
    *"show snodes;"*)
        exit 1
        ;;
    *"show xnodes;"*)
        exit 1
        ;;
esac
exit 0
TAOS_EOF
chmod +x "${INSTALL_BIN}/taos"

ln -s "${INSTALL_BIN}/start-all.sh" "${FAKE_HOME}/.local/bin/start-all.sh"

cat > "${FAKE_BIN}/uname" <<'EOF_UNAME'
#!/bin/bash
printf 'Linux\n'
EOF_UNAME

cat > "${FAKE_BIN}/id" <<'EOF_ID'
#!/bin/bash
if [ "${1:-}" = "-u" ]; then
    printf '%s\n' "${TEST_UID}"
    exit 0
fi
exec /usr/bin/id "$@"
EOF_ID

cat > "${FAKE_BIN}/systemctl" <<'EOF_SYSTEMCTL'
#!/bin/bash
printf '%s\n' "$*" >> "${SYSTEMCTL_LOG}"
exit 0
EOF_SYSTEMCTL

cat > "${FAKE_BIN}/sleep" <<'EOF_SLEEP'
#!/bin/bash
exit 0
EOF_SLEEP

cat > "${FAKE_BIN}/taos" <<'EOF_TAOS'
#!/bin/bash
printf '<path>\n' >> "${TAOS_LOG}"
printf '<%s>\n' "$@" >> "${TAOS_LOG}"
case "$*" in
    *"select server_status();"*)
        exit 0
        ;;
    *"show snodes;"*)
        exit 1
        ;;
    *"show xnodes;"*)
        exit 1
        ;;
esac
exit 0
EOF_TAOS

cat > "${FAKE_BIN}/mktemp" <<'EOF_MKTEMP'
#!/bin/bash
path="${TEST_TMPDIR}/mktemp.$$.$RANDOM"
: > "${path}"
printf '%s\n' "${path}"
EOF_MKTEMP

chmod +x "${FAKE_BIN}/uname" "${FAKE_BIN}/id" "${FAKE_BIN}/systemctl" "${FAKE_BIN}/sleep" "${FAKE_BIN}/taos" "${FAKE_BIN}/mktemp"

assert_log_contains() {
    local description="$1"
    local file="$2"
    local pattern="$3"

    if grep -F -- "$pattern" "$file" >/dev/null 2>&1; then
        printf 'PASS: %s\n' "$description"
    else
        printf 'FAIL: %s\n' "$description"
        printf 'Expected to find: %s\n' "$pattern"
        printf 'Actual log:\n'
        cat "$file"
        exit 1
    fi
}

assert_log_sequence() {
    local description="$1"
    local file="$2"
    local first="$3"
    local second="$4"

    if python3 - "$file" "$first" "$second" <<'PY'
from pathlib import Path
import sys

content = Path(sys.argv[1]).read_text()
needle = f"{sys.argv[2]}\n{sys.argv[3]}"
sys.exit(0 if needle in content else 1)
PY
    then
        printf 'PASS: %s\n' "$description"
    else
        printf 'FAIL: %s\n' "$description"
        printf 'Expected sequence:\n%s\n%s\n' "$first" "$second"
        printf 'Actual log:\n'
        cat "$file"
        exit 1
    fi
}

run_start_all() {
    local uid="$1"
    local taos_log="$2"
    local systemctl_log="$3"

    : > "${taos_log}"
    : > "${systemctl_log}"

    HOME="${FAKE_HOME}" \
    PATH="${FAKE_BIN}:${FAKE_HOME}/.local/bin:${INSTALL_BIN}:${PATH}" \
    TEST_UID="${uid}" \
    TAOS_LOG="${taos_log}" \
    SYSTEMCTL_LOG="${systemctl_log}" \
    TEST_TMPDIR="${TEST_TMPDIR}" \
    bash "${FAKE_HOME}/.local/bin/start-all.sh" >/dev/null 2>/dev/null
}

NONROOT_TAOS_LOG="${WORKDIR}/nonroot-taos.log"
NONROOT_SYSTEMCTL_LOG="${WORKDIR}/nonroot-systemctl.log"
run_start_all 1000 "${NONROOT_TAOS_LOG}" "${NONROOT_SYSTEMCTL_LOG}"
assert_log_contains "non-root prefers install-local taos over PATH" "${NONROOT_TAOS_LOG}" "<install-local>"
assert_log_sequence "non-root custom install uses install cfg dir" "${NONROOT_TAOS_LOG}" "<-c>" "<${INSTALL_CFG}>"
assert_log_contains "non-root uses systemctl --user" "${NONROOT_SYSTEMCTL_LOG}" "--user start taosd"

ROOT_TAOS_LOG="${WORKDIR}/root-taos.log"
ROOT_SYSTEMCTL_LOG="${WORKDIR}/root-systemctl.log"
run_start_all 0 "${ROOT_TAOS_LOG}" "${ROOT_SYSTEMCTL_LOG}"
assert_log_contains "root still uses PATH taos" "${ROOT_TAOS_LOG}" "<path>"
assert_log_sequence "root keeps /etc/taos cfg dir" "${ROOT_TAOS_LOG}" "<-c>" "</etc/taos>"

printf 'All regression checks passed.\n'
