#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${ROOT}/tests/smoke/.tmp/test-setup-host-installs.$$"
mkdir -p "${TMP}"
trap 'rm -rf "${TMP}"' EXIT

make_fake_common_commands() {
  local bin_dir="$1"
  local log_file="$2"

  cat > "${bin_dir}/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'curl %s\n' "$*" >> "${FAKE_LOG}"
out=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o)
      out="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [[ -n "${out}" ]]; then
  : > "${out}"
fi
EOF
  chmod +x "${bin_dir}/curl"

  cat > "${bin_dir}/sudo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'sudo %s\n' "$*" >> "${FAKE_LOG}"
exit 0
EOF
  chmod +x "${bin_dir}/sudo"

  export FAKE_LOG="${log_file}"
}

test_go_install_uses_full_toolchain_version() {
  local case_dir="${TMP}/go"
  local bin_dir="${case_dir}/bin"
  local home_dir="${case_dir}/home"
  local log_file="${case_dir}/calls.log"
  mkdir -p "${bin_dir}" "${home_dir}"
  : > "${home_dir}/.bashrc"
  : > "${log_file}"

  make_fake_common_commands "${bin_dir}" "${log_file}"

  # shellcheck disable=SC1091
  source "${ROOT}/utils/common.sh"
  # shellcheck disable=SC1091
  source "${ROOT}/config.sh"
  # shellcheck disable=SC1091
  source "${ROOT}/modules/go.sh"

  confirm() { return 0; }
  cmd_exists() {
    [[ "$1" == "go" ]] && return 1
    command -v "$1" >/dev/null 2>&1
  }

  export HOME="${home_dir}"
  SHELL_RC="${home_dir}/.bashrc"
  PKG_MGR="apt"
  SETUP_ARCH="arm64"

  PATH="${bin_dir}:$PATH" mod_go_install

  grep -F -q "curl -fsSL https://go.dev/dl/go1.23.4.linux-arm64.tar.gz -o /tmp/go.tar.gz" "${log_file}"
  grep -F -q 'export PATH=/usr/local/go/bin:$PATH' "${SHELL_RC}"
}

test_cpp_install_uses_venv_for_conan() {
  local case_dir="${TMP}/cpp"
  local bin_dir="${case_dir}/bin"
  local home_dir="${case_dir}/home"
  local log_file="${case_dir}/calls.log"
  mkdir -p "${bin_dir}" "${home_dir}"
  : > "${home_dir}/.bashrc"
  : > "${log_file}"

  make_fake_common_commands "${bin_dir}" "${log_file}"

  cat > "${bin_dir}/python3" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'python3 %s\n' "$*" >> "${FAKE_LOG}"
if [[ "${1:-}" == "-m" && "${2:-}" == "venv" ]]; then
  venv_dir="${3}"
  mkdir -p "${venv_dir}/bin"
  cat > "${venv_dir}/bin/pip" <<'PIP'
#!/usr/bin/env bash
set -euo pipefail
printf 'venv-pip %s\n' "$*" >> "${FAKE_LOG}"
exit 0
PIP
  chmod +x "${venv_dir}/bin/pip"
  exit 0
fi
exit 1
EOF
  chmod +x "${bin_dir}/python3"

  cat > "${bin_dir}/pip3" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'pip3 %s\n' "$*" >> "${FAKE_LOG}"
exit 99
EOF
  chmod +x "${bin_dir}/pip3"

  cat > "${bin_dir}/pip" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'pip %s\n' "$*" >> "${FAKE_LOG}"
exit 99
EOF
  chmod +x "${bin_dir}/pip"

  # shellcheck disable=SC1091
  source "${ROOT}/utils/common.sh"
  # shellcheck disable=SC1091
  source "${ROOT}/config.sh"
  # shellcheck disable=SC1091
  source "${ROOT}/modules/cpp.sh"

  confirm() { return 0; }
  cmd_exists() {
    case "$1" in
      conan) return 1 ;;
      cmake|gcc|ccache|python3) return 0 ;;
      *) command -v "$1" >/dev/null 2>&1 ;;
    esac
  }
  pkg_install() {
    printf 'pkg_install %s\n' "$*" >> "${FAKE_LOG}"
  }

  export HOME="${home_dir}"
  SHELL_RC="${home_dir}/.bashrc"
  PKG_MGR="apt"

  PATH="${bin_dir}:$PATH" mod_cpp_install

  grep -F -q "pkg_install python3-venv" "${log_file}"
  grep -F -q "python3 -m venv ${home_dir}/.local/share/tsdb-setup/conan-venv" "${log_file}"
  grep -F -q "venv-pip install conan" "${log_file}"
  if grep -F -q "pip3 install --user conan" "${log_file}" || grep -F -q "pip install --user conan" "${log_file}"; then
    echo "unexpected pip --user conan install path used" >&2
    exit 1
  fi
  grep -F -q "export PATH=${home_dir}/.local/share/tsdb-setup/conan-venv/bin:\$PATH" "${SHELL_RC}"
}

test_rust_setup_handles_rustup_without_default_toolchain() {
  local case_dir="${TMP}/rust"
  local bin_dir="${case_dir}/bin"
  local home_dir="${case_dir}/home"
  local log_file="${case_dir}/calls.log"
  mkdir -p "${bin_dir}" "${home_dir}"
  : > "${home_dir}/.bashrc"
  : > "${log_file}"

  cat > "${bin_dir}/rustc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'rustc %s\n' "$*" >> "${FAKE_LOG}"
echo "error: rustup could not choose a version of rustc to run" >&2
exit 1
EOF
  chmod +x "${bin_dir}/rustc"

  cat > "${bin_dir}/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'cargo %s\n' "$*" >> "${FAKE_LOG}"
echo "error: no default cargo toolchain" >&2
exit 1
EOF
  chmod +x "${bin_dir}/cargo"

  cat > "${bin_dir}/rustup" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'rustup %s\n' "$*" >> "${FAKE_LOG}"
exit 0
EOF
  chmod +x "${bin_dir}/rustup"

  cat > "${bin_dir}/protoc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "libprotoc 3.21.12"
EOF
  chmod +x "${bin_dir}/protoc"

  export FAKE_LOG="${log_file}"

  # shellcheck disable=SC1091
  source "${ROOT}/utils/common.sh"
  # shellcheck disable=SC1091
  source "${ROOT}/config.sh"
  # shellcheck disable=SC1091
  source "${ROOT}/modules/rust.sh"

  confirm() { return 0; }
  cmd_exists() {
    case "$1" in
      rustc|cargo|rustup|protoc) return 0 ;;
      *) command -v "$1" >/dev/null 2>&1 ;;
    esac
  }

  export HOME="${home_dir}"
  SHELL_RC="${home_dir}/.bashrc"

  PATH="${bin_dir}:$PATH" mod_rust_check
  PATH="${bin_dir}:$PATH" mod_rust_install

  grep -F -q "rustup toolchain install stable" "${log_file}"
  grep -F -q "rustup default stable" "${log_file}"
}

test_go_install_uses_full_toolchain_version
test_cpp_install_uses_venv_for_conan
test_rust_setup_handles_rustup_without_default_toolchain

echo "PASS"
