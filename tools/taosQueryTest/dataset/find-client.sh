#!/bin/sh

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

# Resolves an external service client without requiring callers to edit PATH.
qst_find_client() {
  override_name=$1
  command_name=$2
  standard_pattern=$3
  legacy_override=${4:-}
  override_value=
  candidate=

  eval "override_value=\${$override_name:-}"
  if [ -n "$override_value" ]; then
    [ -x "$override_value" ] || return 1
    printf '%s\n' "$override_value"
    return 0
  fi
  if [ -n "$legacy_override" ]; then
    eval "override_value=\${$legacy_override:-}"
    if [ -n "$override_value" ]; then
      [ -x "$override_value" ] || return 1
      printf '%s\n' "$override_value"
      return 0
    fi
  fi
  candidate=$(command -v "$command_name" 2>/dev/null || true)
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    printf '%s\n' "$candidate"
    return 0
  fi
  for candidate in $standard_pattern; do
    if [ -x "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}
