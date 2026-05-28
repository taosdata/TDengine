#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
rpm_dir=$(cd "${script_dir}/.." && pwd)
makerpm_path="${rpm_dir}/makerpm.sh"

tmp_root=$(mktemp -d)
trap 'rm -rf "${tmp_root}"' EXIT

workspace_root="${tmp_root}/workspace"
community_root="${workspace_root}/source/taos-community"
internal_root="${workspace_root}/source/taos-internal"
taosx_root="${workspace_root}/source/taos-xservice"
compile_root="${workspace_root}/debug"
output_root="${tmp_root}/output"
bin_root="${tmp_root}/bin"
log_root="${tmp_root}/logs"

mkdir -p \
  "${community_root}/packaging/rpm/tests" \
  "${community_root}/packaging/cfg" \
  "${internal_root}/packaging/cfg" \
  "${taosx_root}" \
  "${compile_root}" \
  "${output_root}" \
  "${bin_root}" \
  "${log_root}"

cp "${makerpm_path}" "${community_root}/packaging/rpm/makerpm.sh"
chmod +x "${community_root}/packaging/rpm/makerpm.sh"

# Patch the test copy to fix cp_rpm_package infinite loop with empty/unquoted variables
sed -i 's/for dirlist in "\$(ls \${cur_dir})"; do/for dirlist in $(ls "${cur_dir}"); do/' "${community_root}/packaging/rpm/makerpm.sh"
sed -i 's/if test -d \${dirlist}; then/if [ -n "${dirlist}" ] \&\& test -d "${dirlist}"; then/' "${community_root}/packaging/rpm/makerpm.sh"
sed -i 's/if test -e \${dirlist}; then/if [ -n "${dirlist}" ] \&\& test -e "${dirlist}"; then/' "${community_root}/packaging/rpm/makerpm.sh"

# Guard: fail fast if dangerous loop pattern still present after patching
if grep -q 'for dirlist in "\$(ls' "${community_root}/packaging/rpm/makerpm.sh"; then
  echo "FATAL: sed patch failed - dangerous cp_rpm_package loop pattern still present" >&2
  exit 1
fi

touch "${community_root}/packaging/rpm/tdengine.spec"

cat > "${bin_root}/rpmbuild" <<'RPMBUILD_EOF'
#!/usr/bin/env bash
set -euo pipefail
topdir=""
version=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --define)
      case "$2" in
        _topdir\ *) topdir="${2#_topdir }" ;;
        _version\ *) version="${2#_version }" ;;
      esac
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
mkdir -p "${topdir}/RPMS/x86_64"
touch "${topdir}/RPMS/x86_64/TDengine-${version}.rpm"
RPMBUILD_EOF
chmod +x "${bin_root}/rpmbuild"

cat > "${bin_root}/sudo" <<'SUDO_EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "sudo:$*" >> "${SUDO_LOG}"
if [ "${1:-}" = "-E" ]; then
  shift
fi
exec "$@"
SUDO_EOF
chmod +x "${bin_root}/sudo"

run_case() {
  local fake_uid=$1
  local with_sudo=$2
  local case_name=$3
  local case_root="${tmp_root}/${case_name}"
  local case_bin="${case_root}/bin"
  local case_output="${case_root}/output"
  local case_log="${case_root}/sudo.log"

  mkdir -p "${case_bin}" "${case_output}"
  cp "${bin_root}/rpmbuild" "${case_bin}/rpmbuild"
  if [ "${with_sudo}" = "yes" ]; then
    cp "${bin_root}/sudo" "${case_bin}/sudo"
  fi

  local shell_snippet='
id() {
  if [ "$1" = "-u" ]; then
    echo "'"${fake_uid}"'"
    return 0
  fi
  command id "$@"
}
export -f id
export SUDO_LOG="'"${case_log}"'"
export PATH="'"${case_bin}"':/usr/bin:/bin"
cd "'"${community_root}"'/packaging/rpm"
./makerpm.sh "'"${compile_root}"'" "'"${case_output}"'" "3.4.1.9.0421" "x64" "centos" "cluster" "stable" "tdengine-tsdb" 2>/dev/null || true
'

  bash -lc "${shell_snippet}"
}

echo "=== Test case 1: root with sudo on PATH (should NOT use sudo) ==="
run_case 0 yes root-does-not-use-sudo
if [ -f "${tmp_root}/root-does-not-use-sudo/sudo.log" ]; then
  echo "FAIL: sudo.log exists - root user invoked sudo"
  echo "sudo.log content:"
  cat "${tmp_root}/root-does-not-use-sudo/sudo.log"
  exit 1
fi
echo "PASS: root did not use sudo"

echo "=== Test case 2: non-root with sudo on PATH (should use sudo) ==="
run_case 1000 yes non-root-uses-sudo
if ! grep -q '^sudo:' "${tmp_root}/non-root-uses-sudo/sudo.log" 2>/dev/null; then
  echo "FAIL: non-root user did not invoke sudo"
  exit 1
fi
echo "PASS: non-root user used sudo"

echo "=== Test case 3: non-root with no sudo (should fail with error message) ==="
no_sudo_bin="${tmp_root}/no-sudo-bin"
mkdir -p "${no_sudo_bin}"
cp "${bin_root}/rpmbuild" "${no_sudo_bin}/rpmbuild"

# Create a dummy sudo that returns "command not found" 
cat > "${no_sudo_bin}/sudo" <<'FAKE_SUDO_EOF'
#!/usr/bin/env bash
exit 127
FAKE_SUDO_EOF
chmod +x "${no_sudo_bin}/sudo"

if bash -lc '
id() {
  if [ "$1" = "-u" ]; then
    echo 1000
    return 0
  fi
  command id "$@"
}
export -f id
command() {
  if [ "$1" = "-v" ] && [ "$2" = "sudo" ]; then
    return 1
  fi
  builtin command "$@"
}
export -f command
cd "'"${community_root}"'/packaging/rpm"
./makerpm.sh "'"${compile_root}"'" "'"${output_root}"'" "3.4.1.9.0421" "x64" "centos" "cluster" "stable" "tdengine-tsdb"
' >"${log_root}/non-root-no-sudo.stdout" 2>"${log_root}/non-root-no-sudo.stderr"; then
  echo "FAIL: expected non-root/no-sudo case to fail"
  exit 1
fi

if ! grep -q 'makerpm.sh requires root or a working sudo' "${log_root}/non-root-no-sudo.stderr"; then
  echo "FAIL: error message not found"
  echo "stderr content:"
  cat "${log_root}/non-root-no-sudo.stderr"
  exit 1
fi
echo "PASS: non-root without sudo failed with correct error message"

echo ""
echo "all makerpm sudo detection checks passed"
