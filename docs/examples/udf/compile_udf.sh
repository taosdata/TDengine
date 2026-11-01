# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

# Compile the UDF examples of this directory into shared libraries.
#
# Works both in the source tree (source/taos-community/docs/examples/udf) and in
# an installed package (<prefix>/examples/udf): the TDengine headers are looked
# up next to this script first, then in the default install locations.
#
# Two header layouts are supported:
#   - source tree:   <repo>/source/taos-community/include, which keeps the
#                    original sub-directories (client/, util/, libs/function/)
#   - installed pkg: <prefix>/include, where the headers are flattened (deb,
#                    rpm and both tar packages all install them flat)
#
#   TAOS_INC=/path/to/include   header directory that contains taosudf.h
#   UDF_OUT=/path/to/output     output directory (default: /tmp/udf)
#
# The generated libraries follow the names used by the documentation, for
# example bit_and.c -> libbitand.so and perm_entropy.c -> libperm_entropy.so.

set -e

udf_dir=$(cd "$(dirname "$0")" && pwd)
out_dir=${UDF_OUT:-/tmp/udf}

# A header directory is usable when it either keeps the source-tree layout
# (libs/function/taosudf.h) or holds the flattened headers of an install
# (taosudf.h directly in the directory).
has_headers() {
  [ -d "$1/libs/function" ] || [ -f "$1/taosudf.h" ]
}

if [ -z "${TAOS_INC}" ]; then
  for candidate in "${udf_dir}/../../../include" "${udf_dir}/../../include" \
                   "${udf_dir}/../include" /usr/local/taos/include \
                   /usr/local/include /usr/include; do
    if has_headers "${candidate}"; then
      TAOS_INC=${candidate}
      break
    fi
  done
fi

if [ -z "${TAOS_INC}" ] || ! has_headers "${TAOS_INC}"; then
  echo "ERROR: TDengine headers (taosudf.h) not found." >&2
  echo "       Set TAOS_INC to the directory that contains taosudf.h:" >&2
  echo "         source tree: TAOS_INC=<repo>/source/taos-community/include" >&2
  echo "         installed:   TAOS_INC=/usr/local/taos/include" >&2
  exit 1
fi

mkdir -p "${out_dir}"

# The UDF entry points (taosPrintLog, udfDebugFlag, ...) are provided by taosd
# when the library is loaded. Linux links shared libraries with undefined
# symbols by default; macOS needs an explicit flag for the same behaviour.
ldflags=""
if [ "$(uname -s)" = "Darwin" ]; then
  ldflags="-undefined dynamic_lookup"
fi

for src in "${udf_dir}"/*.c; do
  [ -f "${src}" ] || continue
  name=$(basename "${src}" .c)
  case "${name}" in
    bit_and) lib=libbitand.so ;;
    *)       lib="lib${name}.so" ;;
  esac

  echo "compile udf ${name} -> ${lib}"
  # All -I directories are passed unconditionally: gcc silently ignores ones
  # that do not exist, so a single command covers both header layouts.
  gcc -fPIC -shared "${src}" \
      "-I${TAOS_INC}" \
      "-I${TAOS_INC}/client" \
      "-I${TAOS_INC}/util" \
      "-I${TAOS_INC}/common" \
      "-I${TAOS_INC}/libs/function" \
      ${ldflags} -lm -o "${out_dir}/${lib}"
done

echo "UDF libraries in ${out_dir}:"
ls -l "${out_dir}"/*.so

