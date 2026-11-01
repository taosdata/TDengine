#!/usr/bin/env bash
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

# Select a worker-local, persistent cache. Print only the bind-mount source.
# Existing preseeded caches take precedence over the CI-root default.
compat_package_cache_dir() {
    local ci_root="$1"
    local cache_dir="${COMPAT_PACKAGES_CACHE_DIR:-}"
    if [ -z "$cache_dir" ]; then
        if [ -d /data0/compat-packages ]; then
            cache_dir=/data0/compat-packages
        else
            cache_dir="${ci_root}/compat-packages"
        fi
    fi
    case "$cache_dir" in
        /*) ;;
        *) echo "[compat-cache] cache path must be absolute: $cache_dir" >&2; return 1 ;;
    esac
    if ! mkdir -p "$cache_dir" || [ ! -w "$cache_dir" ]; then
        echo "[compat-cache] cannot create/use persistent cache: $cache_dir" >&2
        return 1
    fi
    echo "[compat-cache] /usr/local/src -> $cache_dir" >&2
    printf '%s\n' "$cache_dir"
}
