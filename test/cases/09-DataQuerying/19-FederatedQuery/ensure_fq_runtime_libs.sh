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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null || (cd "${SCRIPT_DIR}/../../../../../.." && pwd -P))"
BUILD_DIR="${BUILD_DIR:-${TD_BUILD_DIR:-${REPO_ROOT}/debug}}"
TAOS_BIN_PATH="${TAOS_BIN_PATH:-${BUILD_DIR}/build/bin}"
STAGED_DIR="${BUILD_DIR}/build/lib"
CACHE_DIR="${FQ_RUNTIME_CACHE_DIR:-${FQ_TARBALL_CACHE_DIR:-/tmp/fq-runtime-libs}}"
STAGE_SCRIPT="${REPO_ROOT}/source/taos-community/cmake/stage_runtime_libs.sh"
OS="$(uname -s)"
FQ_BASE_DIR="${FQ_BASE_DIR:-$([ "${OS}" = "Darwin" ] && printf '%s\n' "${HOME}/taostest/fq" || printf '%s\n' "/opt/taostest/fq")}"
RUNTIME_DIR="${BUILD_DIR}/build/lib"

mkdir -p "${CACHE_DIR}"
mkdir -p "${RUNTIME_DIR}"

is_true() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

expect_enabled() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

expect_disabled() {
    case "${1:-}" in
        0|false|FALSE|no|NO|off|OFF) return 0 ;;
        *) return 1 ;;
    esac
}

copy_glob() {
    local src_dir="$1"
    shift
    [ -d "${src_dir}" ] || return 1
    bash "${STAGE_SCRIPT}" copy-glob "${src_dir}" "${RUNTIME_DIR}" "$@"
    bash "${STAGE_SCRIPT}" copy-ldd-glob "${src_dir}" "${RUNTIME_DIR}" "$@"
}

copy_tree_libs() {
    local src_dir="$1"
    [ -d "${src_dir}" ] || return 1
    find "${src_dir}" \( -type f -o -type l \) \
        \( -name 'lib*.so' -o -name 'lib*.so.*' -o -name 'lib*.dylib' -o -name 'lib*.dylib.*' \) \
        -exec cp -Lf {} "${RUNTIME_DIR}/" \;
    bash "${STAGE_SCRIPT}" copy-ldd-glob "${src_dir}" "${RUNTIME_DIR}" 'lib*.so*' 'lib*.dylib*' || true
}

fetch_runtime() {
    local name="$1"
    local url="$2"
    local out_dir="${CACHE_DIR}/${name}"
    local archive

    [ -n "${url}" ] || return 1
    rm -rf "${out_dir}"
    mkdir -p "${out_dir}"

    if [ -d "${url}" ]; then
        copy_tree_libs "${url}"
        return 0
    fi
    if [ -f "${url}" ]; then
        archive="${url}"
    else
        archive="${CACHE_DIR}/$(basename "${url%%\?*}")"
        if [ ! -s "${archive}" ]; then
            curl -fL --retry 3 --retry-delay 5 --connect-timeout 30 -o "${archive}" "${url}"
        fi
    fi

    case "$(basename "${archive}")" in
        *.tar.gz|*.tgz) tar -xzf "${archive}" -C "${out_dir}" ;;
        *.tar.xz)       tar -xJf "${archive}" -C "${out_dir}" ;;
        *.tar.bz2)      tar -xjf "${archive}" -C "${out_dir}" ;;
        *.zip)          unzip -q "${archive}" -d "${out_dir}" ;;
        lib*.so*|lib*.dylib*)
            cp -Lf "${archive}" "${RUNTIME_DIR}/"
            bash "${STAGE_SCRIPT}" copy-ldd "${archive}" "${RUNTIME_DIR}" || true
            ;;
        *)              return 1 ;;
    esac
    copy_tree_libs "${out_dir}"
}

has_any() {
    local name
    for name in "$@"; do
        [ -e "${RUNTIME_DIR}/${name}" ] && return 0
    done
    return 1
}

has_glob() {
    local dir=$1
    local pattern=$2
    local found
    # shellcheck disable=SC2086
    for found in "${dir}"/${pattern}; do
        [ -e "${found}" ] && return 0
    done
    return 1
}

first_csv_value() {
    local value="${1:-}"
    value="${value%%,*}"
    printf '%s\n' "${value}"
}

bootstrap_ext_env() {
    local service="$1"
    FQ_SERVICES_TO_RESET="${service}" bash "${SCRIPT_DIR}/ensure_ext_env.sh"
}

clean_mysql() {
    rm -f "${RUNTIME_DIR}"/libmariadb* "${RUNTIME_DIR}"/libmysqlclient* 2>/dev/null || true
}

clean_pg() {
    rm -f "${RUNTIME_DIR}"/libpq* 2>/dev/null || true
}

clean_influx() {
    rm -f "${RUNTIME_DIR}"/libtaos_ext_influx_arrow* \
          "${RUNTIME_DIR}"/libarrow* \
          "${RUNTIME_DIR}"/libparquet* 2>/dev/null || true
}

clean_odbc() {
    rm -f "${RUNTIME_DIR}"/libodbc* 2>/dev/null || true
}

ensure_mysql() {
    local ver="${FQ_MYSQL_RUNTIME_VERSION:-$(first_csv_value "${FQ_MYSQL_VERSIONS:-8.0}")}"
    if [ "${OS}" = "Darwin" ]; then
        has_any libmariadb.3.dylib libmariadb.dylib libmysqlclient.dylib && return 0
    else
        has_any libmariadb.so.3 libmariadb.so libmysqlclient.so && return 0
    fi

    copy_glob "${STAGED_DIR}" 'libmariadb.so*' 'libmysqlclient.so*' 'libmariadb*.dylib*' 'libmysqlclient*.dylib*' || true
    copy_glob "${REPO_ROOT}/.externals/install/ext_mariadb/${TD_CONFIG_NAME:-Debug}/lib/mariadb" \
        'libmariadb.so*' 'libmysqlclient.so*' 'libmariadb*.dylib*' 'libmysqlclient*.dylib*' || true
    copy_glob "${FQ_BASE_DIR}/mysql/${ver}/lib" 'libmariadb.so*' 'libmysqlclient.so*' 'libmariadb*.dylib*' 'libmysqlclient*.dylib*' || true
    copy_glob /usr/lib/x86_64-linux-gnu 'libmariadb.so*' 'libmysqlclient.so*' || true
    copy_glob /usr/lib/aarch64-linux-gnu 'libmariadb.so*' 'libmysqlclient.so*' || true
    copy_glob /usr/local/lib 'libmariadb.so*' 'libmysqlclient.so*' 'libmariadb*.dylib*' 'libmysqlclient*.dylib*' || true
    copy_glob /opt/homebrew/lib 'libmariadb*.dylib*' 'libmysqlclient*.dylib*' || true
    fetch_runtime mysql "${FQ_MYSQL_RUNTIME_URL:-}" || true

    if ! has_any libmariadb.so.3 libmariadb.so libmysqlclient.so libmariadb.3.dylib libmariadb.dylib libmysqlclient.dylib; then
        bootstrap_ext_env mysql || true
        copy_glob "${FQ_BASE_DIR}/mysql/${ver}/lib" 'libmariadb.so*' 'libmysqlclient.so*' 'libmariadb*.dylib*' 'libmysqlclient*.dylib*' || true
    fi

    if [ "${OS}" = "Darwin" ]; then
        has_any libmariadb.3.dylib libmariadb.dylib libmysqlclient.dylib
    else
        has_any libmariadb.so.3 libmariadb.so libmysqlclient.so
    fi
}

ensure_pg() {
    local ver="${FQ_PG_RUNTIME_VERSION:-$(first_csv_value "${FQ_PG_VERSIONS:-16}")}"
    if [ "${OS}" = "Darwin" ]; then
        has_any libpq.5.dylib libpq.dylib && return 0
    else
        has_any libpq.so.5 libpq.so && return 0
    fi

    copy_glob "${STAGED_DIR}" 'libpq.so*' 'libpq*.dylib*' || true
    copy_glob "${REPO_ROOT}/.externals/install/ext_libpq/${TD_CONFIG_NAME:-Debug}/lib" 'libpq.so*' 'libpq*.dylib*' || true
    copy_glob "${FQ_BASE_DIR}/pg/${ver}/lib" 'libpq.so*' 'libpq*.dylib*' || true
    copy_glob "/usr/lib/postgresql/${ver}/lib" 'libpq.so*' || true
    copy_glob /usr/lib/x86_64-linux-gnu 'libpq.so*' || true
    copy_glob /usr/lib/aarch64-linux-gnu 'libpq.so*' || true
    copy_glob /usr/local/lib 'libpq.so*' 'libpq*.dylib*' || true
    copy_glob /opt/homebrew/lib 'libpq*.dylib*' || true
    fetch_runtime pg "${FQ_PG_RUNTIME_URL:-}" || true

    if ! has_any libpq.so.5 libpq.so libpq.5.dylib libpq.dylib; then
        bootstrap_ext_env pg || true
        copy_glob "${FQ_BASE_DIR}/pg/${ver}/lib" 'libpq.so*' 'libpq*.dylib*' || true
        copy_glob "/usr/lib/postgresql/${ver}/lib" 'libpq.so*' || true
        copy_glob /usr/lib/x86_64-linux-gnu 'libpq.so*' || true
        copy_glob /usr/lib/aarch64-linux-gnu 'libpq.so*' || true
        copy_glob /usr/local/lib 'libpq.so*' 'libpq*.dylib*' || true
        copy_glob /opt/homebrew/lib 'libpq*.dylib*' || true
    fi

    if [ "${OS}" = "Darwin" ]; then
        has_any libpq.5.dylib libpq.dylib
    else
        has_any libpq.so.5 libpq.so
    fi
}

ensure_influx() {
    if [ "${OS}" = "Darwin" ]; then
        has_any libtaos_ext_influx_arrow.dylib && return 0
    else
        has_any libtaos_ext_influx_arrow.so && return 0
    fi

    copy_glob "${STAGED_DIR}" 'lib*.so*' 'lib*.dylib*' || true
    fetch_runtime influx "${FQ_INFLUX_RUNTIME_URL:-}" || true

    if [ "${OS}" = "Darwin" ]; then
        has_any libtaos_ext_influx_arrow.dylib
    else
        has_any libtaos_ext_influx_arrow.so
    fi
}

ensure_odbc() {
    if [ "${OS}" = "Darwin" ]; then
        has_any libodbc.2.dylib libodbc.dylib && return 0
    else
        has_any libodbc.so.2 libodbc.so && return 0
    fi

    copy_glob "${STAGED_DIR}" 'libodbc.so*' 'libodbc*.dylib*' || true
    copy_glob "${REPO_ROOT}/.externals/install/ext_odbc/${TD_CONFIG_NAME:-Debug}/lib" \
        'libodbc.so*' 'libodbcinst.so*' 'libodbc*.dylib*' || true
    copy_glob /usr/lib/x86_64-linux-gnu 'libodbc.so*' 'libodbcinst.so*' || true
    copy_glob /usr/lib/aarch64-linux-gnu 'libodbc.so*' 'libodbcinst.so*' || true
    copy_glob /usr/local/lib 'libodbc.so*' 'libodbcinst.so*' 'libodbc*.dylib*' || true

    if [ "${OS}" = "Darwin" ]; then
        has_any libodbc.2.dylib libodbc.dylib
    else
        has_any libodbc.so.2 libodbc.so
    fi
}

# The Microsoft ODBC Driver 18 travels with the dependency package; stage it
# next to libodbc, where the provider resolves `libmsodbcsql-18.*` from
# (`../lib` or `../driver` relative to taosd).  Only SQL Server needs it, so a
# miss is reported as a warning instead of failing the whole staging step.
ensure_msodbc() {
    if has_glob "${RUNTIME_DIR}" 'libmsodbcsql*.so*' ||
       has_glob "${RUNTIME_DIR}" 'libmsodbcsql*.dylib*'; then
        return 0
    fi

    copy_glob "${STAGED_DIR}" 'libmsodbcsql*.so*' 'libmsodbcsql*.dylib*' || true
    copy_glob "${REPO_ROOT}/.externals/install/ext_msodbc/${TD_CONFIG_NAME:-Debug}/lib" \
        'libmsodbcsql*.so*' 'libmsodbcsql*.dylib*' || true
    copy_glob "${BUILD_DIR}/build/driver" 'libmsodbcsql*.so*' 'libmsodbcsql*.dylib*' || true
    copy_glob /opt/microsoft/msodbcsql18/lib64 'libmsodbcsql*.so*' || true
    copy_glob /opt/microsoft/msodbcsql18/lib 'libmsodbcsql*.so*' || true
    copy_glob /opt/microsoft/msodbcsql/lib64 'libmsodbcsql*.so*' || true

    has_glob "${RUNTIME_DIR}" 'libmsodbcsql*.so*' ||
        has_glob "${RUNTIME_DIR}" 'libmsodbcsql*.dylib*'
}

# The Microsoft driver loads its localisation resource relative to its own
# directory (`<driver_dir>/../share/resources/en_US/msodbcsqlr18.rll`).  A
# tree that staged only the .so makes every SQLAllocHandle(SQL_HANDLE_ENV)
# fail — sqlcmd *and* the engine's own connections — with the opaque
# "Driver's SQLAllocHandle on SQL_HANDLE_HENV failed" (seen on CI, where the
# driver was picked up from the image's /opt/microsoft install and the build
# tree never carried share/resources).
ensure_msodbc_resources() {
    local dst="${BUILD_DIR}/build/share/resources"
    [ -f "${dst}/en_US/msodbcsqlr18.rll" ] && return 0

    local src drv
    for src in \
        /opt/microsoft/msodbcsql18/share/resources \
        /opt/microsoft/msodbcsql/share/resources \
        "${REPO_ROOT}/.externals/install/ext_msodbc/${TD_CONFIG_NAME:-Debug}/share/resources"; do
        [ -d "${src}" ] || continue
        mkdir -p "${dst}"
        if cp -r "${src}/." "${dst}/" 2>/dev/null && [ -f "${dst}/en_US/msodbcsqlr18.rll" ]; then
            echo "[fq-runtime] staged Microsoft ODBC driver resources from ${src}" >&2
            return 0
        fi
    done
    # A driver already copied into the tree carries its share/ one level up.
    for drv in "${RUNTIME_DIR}"/libmsodbcsql*.so* "${STAGED_DIR}"/libmsodbcsql*.so*; do
        [ -e "${drv}" ] || continue
        src="$(dirname "$(dirname "${drv}")")/share/resources"
        [ -d "${src}" ] || continue
        mkdir -p "${dst}"
        if cp -r "${src}/." "${dst}/" 2>/dev/null && [ -f "${dst}/en_US/msodbcsqlr18.rll" ]; then
            echo "[fq-runtime] staged Microsoft ODBC driver resources from ${src}" >&2
            return 0
        fi
    done

    # Last resort: the mirrored msodbcsql18 package carries share/resources/.
    local arch url archive out_dir
    case "$(uname -m)" in
        x86_64|amd64)  arch=amd64 ;;
        aarch64|arm64) arch=arm64 ;;
        *)             arch="" ;;
    esac
    [ -n "${arch}" ] || return 1
    url="${FQ_MSODBC_RESOURCES_URL:-${FQ_SQLSERVER_MIRROR_URL:-${FQ_MIRROR_URL:-https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest}}/msodbcsql18-18.7.1.1-${arch}.tar.gz}"
    archive="${CACHE_DIR}/$(basename "${url}")"
    if [ ! -s "${archive}" ]; then
        curl -fL --retry 3 --retry-delay 5 --connect-timeout 30 -o "${archive}" "${url}" 2>/dev/null || true
    fi
    [ -s "${archive}" ] || return 1
    out_dir="${CACHE_DIR}/msodbc-resources"
    rm -rf "${out_dir}"
    mkdir -p "${out_dir}"
    tar -xzf "${archive}" -C "${out_dir}" 2>/dev/null || true
    src="$(find "${out_dir}" -type d -path '*/share/resources' 2>/dev/null | head -1)"
    [ -n "${src}" ] || return 1
    mkdir -p "${dst}"
    if cp -r "${src}/." "${dst}/" 2>/dev/null && [ -f "${dst}/en_US/msodbcsqlr18.rll" ]; then
        echo "[fq-runtime] staged Microsoft ODBC driver resources from ${url}" >&2
        return 0
    fi
    return 1
}

if ! ensure_mysql; then
    echo "[fq-runtime] missing MySQL runtime libs; set FQ_MYSQL_RUNTIME_URL to a libmariadb runtime archive" >&2
    exit 1
fi
if ! ensure_pg; then
    echo "[fq-runtime] missing PostgreSQL runtime libs; set FQ_PG_RUNTIME_URL to a libpq runtime archive" >&2
    exit 1
fi
if ! ensure_influx; then
    echo "[fq-runtime] missing Influx runtime libs; set FQ_INFLUX_RUNTIME_URL to a runtime archive" >&2
    exit 1
fi
if ! ensure_odbc; then
    echo "[fq-runtime] missing ODBC driver-manager runtime libs (libodbc); rebuild with BUILD_WITH_ODBC=ON" >&2
    exit 1
fi
if ! ensure_msodbc; then
    echo "[fq-runtime] WARNING: Microsoft ODBC Driver 18 (libmsodbcsql-18.*) not found; SQL Server" >&2
    echo "[fq-runtime]          federated-query tests will fail. Install msodbcsql18, set" >&2
    echo "[fq-runtime]          EXT_MSODBC_SO_SRCPATH, or rebuild with INSTALL_FQ_RUNTIME_LIBS=ON." >&2
fi
if ! ensure_msodbc_resources; then
    echo "[fq-runtime] WARNING: Microsoft ODBC driver resources (msodbcsqlr18.rll) not staged;" >&2
    echo "[fq-runtime]          SQL Server connections will fail with SQLAllocHandle(HENV)." >&2
fi

bash "${STAGE_SCRIPT}" fix-darwin-rpaths "${RUNTIME_DIR}" || true

cat <<EOF
[fq-runtime] staged runtime libs in ${RUNTIME_DIR}
export LD_LIBRARY_PATH=${RUNTIME_DIR}:\$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=${RUNTIME_DIR}:\$DYLD_LIBRARY_PATH
EOF
