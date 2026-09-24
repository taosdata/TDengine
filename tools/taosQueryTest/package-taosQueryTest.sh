#!/usr/bin/env bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -euo pipefail

usage() {
  printf '%s\n' \
    "Usage: $0 --binary PATH --output PATH --version VERSION [--arch ARCH]"
}

binary_path=
output_path=
package_version=
package_arch="$(uname -m)"
script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
stage_dir=

qst_package_temp_root() {
  if [[ -n "${TMPDIR:-}" ]]; then
    printf '%s\n' "$TMPDIR"
  else
    printf '%s\n' /tmp
  fi
}

while (($# > 0)); do
  case "$1" in
    --binary)
      (($# >= 2)) || { usage >&2; exit 2; }
      binary_path=$2
      shift 2
      ;;
    --output)
      (($# >= 2)) || { usage >&2; exit 2; }
      output_path=$2
      shift 2
      ;;
    --version)
      (($# >= 2)) || { usage >&2; exit 2; }
      package_version=$2
      shift 2
      ;;
    --arch)
      (($# >= 2)) || { usage >&2; exit 2; }
      package_arch=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$binary_path" || -z "$output_path" || -z "$package_version" ]]; then
  usage >&2
  exit 2
fi
if [[ "$(uname -s)" != "Linux" ]]; then
  printf 'taosQueryTest packaging supports Linux only\n' >&2
  exit 1
fi
if [[ ! -x "$binary_path" ]]; then
  printf 'taosQueryTest binary is not executable: %s\n' "$binary_path" >&2
  exit 1
fi
for required in taosQueryTest.yaml dataset prompts dependency-lock.json \
    qst-runtime-env.sh qst-external-env.sh run-taosQueryTest.sh run-audited-round.py \
    run-endurance-matrix.py \
    prepare-test-machine.sh; do
  if [[ ! -e "$script_dir/$required" ]]; then
    printf 'missing package asset: %s\n' "$script_dir/$required" >&2
    exit 1
  fi
done

package_name="taosQueryTest-${package_version}-linux-${package_arch}"
package_temp_root=$(qst_package_temp_root)
stage_dir=$(mktemp -d "$package_temp_root/taosQueryTest-package.XXXXXX")
cleanup() {
  rm -rf -- "$stage_dir"
}
trap cleanup EXIT

package_root="$stage_dir/$package_name"
mkdir -p -- "$package_root"
install -m 0755 -- "$binary_path" "$package_root/taosQueryTest.bin"
install -m 0755 -- "$script_dir/run-taosQueryTest.sh" "$package_root/taosQueryTest"
install -m 0755 -- "$script_dir/run-audited-round.py" "$package_root/run-audited-round.py"
install -m 0755 -- "$script_dir/run-endurance-matrix.py" "$package_root/run-endurance-matrix.py"
install -m 0644 -- "$script_dir/qst-runtime-env.sh" "$package_root/qst-runtime-env.sh"
install -m 0644 -- "$script_dir/qst-external-env.sh" "$package_root/qst-external-env.sh"
install -m 0755 -- "$script_dir/prepare-test-machine.sh" \
  "$package_root/prepare-test-machine.sh"
install -m 0644 -- "$script_dir/taosQueryTest.yaml" "$package_root/taosQueryTest.yaml"
install -m 0644 -- "$script_dir/dependency-lock.json" "$package_root/dependency-lock.json"
cp -a -- "$script_dir/dataset" "$package_root/dataset"
mkdir -p -- "$package_root/prompts"
install -m 0644 -- "$script_dir/prompts/result_judgment.md" \
  "$package_root/prompts/result_judgment.md"
mkdir -p -- "$package_root/results"
cat >"$package_root/PACKAGE-METADATA" <<EOF
name=taosQueryTest
version=$package_version
platform=linux
arch=$package_arch
tool_root=executable-directory
external_components=system-provided
EOF
(
  cd -- "$package_root"
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)

mkdir -p -- "$(dirname -- "$output_path")"
tar -C "$stage_dir" -czf "$output_path" "$package_name"
printf 'created %s\n' "$output_path"
printf 'package_root=%s\n' "$package_name"
printf 'files=%s\n' "$(find "$package_root" -type f | wc -l)"
