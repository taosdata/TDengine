#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD_SCRIPT="${SCRIPT_DIR}/test_install_sh_client_uninstall_entrypoint.sh"

RED='\033[0;31m'
GREEN='\033[1;32m'
NC='\033[0m'

pass=0
fail=0

create_fixture() {
  local fixture_dir="$1"
  local prelude_block="$2"
  local client_block="$3"
  local server_block="$4"

  mkdir -p "${fixture_dir}/smokeTest" "${fixture_dir}/tools"
  cp "${GUARD_SCRIPT}" "${fixture_dir}/smokeTest/test_install_sh_client_uninstall_entrypoint.sh"

  cat > "${fixture_dir}/tools/install.sh" <<EOF
#!/bin/bash

${prelude_block}

function install_bin() {
  if [ "\${verType}" == "client" ]; then
${client_block}
  else
${server_block}
  fi
}

elif [ "\$verType" == "client" ]; then
  interactiveFqdn=no
  if [ -x \${bin_dir}/\${clientName} ]; then
    update_flag=1
    updateProduct client
  else
    installProduct client
  fi
else
  echo "please input correct verType"
fi
EOF
}

run_case() {
  local description="$1"
  local expected_status="$2"
  local fixture_dir="$3"

  local status=0
  set +e
  bash "${fixture_dir}/smokeTest/test_install_sh_client_uninstall_entrypoint.sh" >/tmp/guard-selftest.log 2>&1
  status=$?
  set -e

  if [[ "${expected_status}" == "pass" && ${status} -eq 0 ]] || [[ "${expected_status}" == "fail" && ${status} -ne 0 ]]; then
    echo -e "  ${GREEN}PASS${NC}: ${description}"
    ((pass++)) || true
  else
    echo -e "  ${RED}FAIL${NC}: ${description}"
    cat /tmp/guard-selftest.log | sed 's/^/       /'
    ((fail++)) || true
  fi
}

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}" /tmp/guard-selftest.log' EXIT

create_fixture "${tmp_dir}/good-direct" \
"" \
"    cp -r \${script_dir}/bin/remove.sh \${install_main_dir}/bin" \
"    cp -r \${script_dir}/bin/remove.sh \${install_main_dir}/bin"

create_fixture "${tmp_dir}/good-variable" \
"if [ \"\$verType\" == \"client\" ]; then
  remove_name=\"remove.sh\"
else
  remove_name=\"remove.sh\"
fi" \
"    remove_name=\"remove.sh\"
    cp -r \${script_dir}/bin/\${remove_name} \${install_main_dir}/bin" \
"    remove_name=\"remove.sh\"
    cp -r \${script_dir}/bin/\${remove_name} \${install_main_dir}/bin"

create_fixture "${tmp_dir}/bad-direct" \
"" \
"    cp -r \${script_dir}/bin/remove_client.sh \${install_main_dir}/bin" \
"    cp -r \${script_dir}/bin/remove.sh \${install_main_dir}/bin"

create_fixture "${tmp_dir}/bad-variable" \
"if [ \"\$verType\" == \"client\" ]; then
  remove_name=\"remove_client.sh\"
else
  remove_name=\"remove.sh\"
fi" \
"    remove_name=\"remove_client.sh\"
    cp -r \${script_dir}/bin/\${remove_name} \${install_main_dir}/bin" \
"    remove_name=\"remove.sh\"
    cp -r \${script_dir}/bin/\${remove_name} \${install_main_dir}/bin"

echo "=== install.sh client uninstall entrypoint guard self-test ==="
echo

run_case "accepts direct-copy remove.sh client branch" pass "${tmp_dir}/good-direct"
run_case "accepts variable-based remove.sh client branch" pass "${tmp_dir}/good-variable"
run_case "rejects direct-copy remove_client.sh client branch" fail "${tmp_dir}/bad-direct"
run_case "rejects variable-based remove_client.sh client branch" fail "${tmp_dir}/bad-variable"

echo
if [[ $fail -gt 0 ]]; then
  echo -e "${RED}FAILED${NC}: $fail check(s) failed, $pass passed"
  exit 1
else
  echo -e "${GREEN}guard self-test passed${NC} ($pass/$((pass+fail)))"
fi
