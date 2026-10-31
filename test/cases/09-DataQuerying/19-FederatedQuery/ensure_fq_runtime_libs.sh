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

clean_oracle() {
    rm -f "${RUNTIME_DIR}"/libsqora* \
          "${RUNTIME_DIR}"/libclntsh* \
          "${RUNTIME_DIR}"/libnnz* \
          "${RUNTIME_DIR}"/libons* \
          "${RUNTIME_DIR}"/libnfsodm* \
          "${RUNTIME_DIR}"/liborazt* \
          "${RUNTIME_DIR}"/libociei* \
          "${RUNTIME_DIR}"/legacy.so* \
          "${RUNTIME_DIR}"/fips*.so* 2>/dev/null || true
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

# Oracle Instant Client (Basic + ODBC packages) ships the ODBC driver
# `libsqora.so.<ver>` together with the Instant Client base libraries it
# resolves by soname.  Both must land in the same runtime directory: the
# provider resolves the driver by absolute path there and preloads the base
# libraries with RTLD_GLOBAL.
#
# `legacy.so` belongs to that set even though nothing links against it: the
# driver dlopen()s it to support the newer password verifiers, and without it
# a 26ai instance rejects every login with ORA-28041.
ensure_oracle() {
    # Every Instant Client shared object except legacy.so links libaio; the
    # bundle ships it because the CI test image does not provide it (the driver
    # manager then reports "Can't open lib ...: file not found").  A runtime
    # dir that already has the driver but not libaio is still incomplete.
    if { has_glob "${RUNTIME_DIR}" 'libsqora*.so*' ||
         has_glob "${RUNTIME_DIR}" 'libsqora*.dylib*'; } &&
       { [ "${OS}" = "Darwin" ] || has_glob "${RUNTIME_DIR}" 'libaio*.so*'; }; then
        return 0
    fi

    local dir
    copy_glob "${STAGED_DIR}" 'libsqora*.so*' 'libclntsh*.so*' 'libclntshcore*.so*' \
        'libnnz*.so*' 'libons*.so*' 'libnfsodm*.so*' 'liborazt*.so*' 'libociei*.so*' \
        'legacy.so*' 'fips*.so*' 'libaio*.so*' \
        'libsqora*.dylib*' 'libclntsh*.dylib*' || true
    copy_glob "${REPO_ROOT}/.externals/install/ext_oracle/${TD_CONFIG_NAME:-Debug}/lib" \
        'libsqora*.so*' 'libclntsh*.so*' 'libclntshcore*.so*' 'libnnz*.so*' \
        'libons*.so*' 'libnfsodm*.so*' 'liborazt*.so*' 'libociei*.so*' 'legacy.so*' \
        'libaio*.so*' || true
    copy_glob "${BUILD_DIR}/build/driver/oracle" 'libsqora*.so*' 'libclntsh*.so*' \
        'libclntshcore*.so*' 'libnnz*.so*' 'libons*.so*' 'libnfsodm*.so*' \
        'liborazt*.so*' 'libociei*.so*' 'legacy.so*' 'libaio*.so*' || true
    # An explicitly configured bundle (the CI image and a developer's own
    # Instant Client extraction both land here).
    for dir in "${FQ_ORACLE_RUNTIME_DIR:-}" \
               "${FQ_BASE_DIR}/oracle/instantclient" \
               "${FQ_BASE_DIR}/oracle/instantclient"/*; do
        [ -d "${dir}" ] || continue
        copy_glob "${dir}" 'libsqora*.so*' 'libclntsh*.so*' 'libclntshcore*.so*' \
            'libnnz*.so*' 'libons*.so*' 'libnfsodm*.so*' 'liborazt*.so*' \
            'libociei*.so*' 'legacy.so*' 'fips*.so*' 'libaio*.so*' \
            'libsqora*.dylib*' 'libclntsh*.dylib*' || true
    done
    copy_glob /usr/lib/oracle 'libsqora*.so*' 'libclntsh*.so*' 'libclntshcore*.so*' \
        'libnnz*.so*' 'libons*.so*' 'libnfsodm*.so*' 'liborazt*.so*' 'libociei*.so*' \
        'legacy.so*' 'libaio*.so*' || true

    has_glob "${RUNTIME_DIR}" 'libsqora*.so*' ||
        has_glob "${RUNTIME_DIR}" 'libsqora*.dylib*'
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

# Oracle Instant Client (Basic + ODBC) travels as one bundle: the provider
# resolves `libsqora.so.<ver>` by absolute path from this directory and
# preloads the Instant Client base libraries it needs (libclntsh and the leaf
# libraries) with RTLD_GLOBAL, so no ORACLE_HOME / ld.so.conf entry is needed.
# Only Oracle needs them, so a miss is a warning like SQL Server's.
if ! ensure_oracle; then
    echo "[fq-runtime] WARNING: Oracle Instant Client (libsqora.so.*) not found; Oracle" >&2
    echo "[fq-runtime]          federated-query tests will fail. Set FQ_ORACLE_RUNTIME_DIR" >&2
    echo "[fq-runtime]          (or EXT_ORACLE_SO_SRCPATH), or rebuild with" >&2
    echo "[fq-runtime]          INSTALL_FQ_RUNTIME_LIBS=ON." >&2
fi

# A build tree stages both Instant Client generations under
# build/driver/oracle/<version> and selects one through the `active` marker.
# The marker is written at configure time, but the same tree serves every
# test run: CI defaults to an 11g server (19c client), while a manual 26ai
# run needs 23ai.  Align the marker with the selection this run is validated
# against; a tree without the versioned layout (an installed package, or a
# build that staged only one flat client) is left untouched.
select_oracle_client() {
    local layout="${BUILD_DIR}/build/driver/oracle"
    local want="${FQ_ORACLE_ACTIVE_CLIENT:-}"
    if [ -z "${want}" ]; then
        case "$(printf '%s' "${FQ_ORACLE_VERSIONS:-11g}" | cut -d, -f1)" in
            11g|12c|18c) want=19c ;;
            *) want=23ai ;;
        esac
    fi
    [ -d "${layout}/${want}" ] || return 0
    # The 19c Instant Client's libnnz19.so carries no SONAME, so the loader
    # cannot satisfy libclntsh's DT_NEEDED for it from the preloaded objects
    # alone; put the active client directory on the library search path.
    # It has to come before build/lib, whose flat client copies belong to the
    # other generation.
    oracleClientDir="${layout}/${want}"
    if [ -f "${layout}/active" ] && \
       [ "$(cat "${layout}/active" 2>/dev/null)" = "${want}" ]; then
        return 0
    fi
    printf '%s\n' "${want}" > "${layout}/active"
    echo "[fq-runtime] Oracle client ${want} selected for ${FQ_ORACLE_VERSIONS:-11g}" >&2
}
oracleClientDir=""
select_oracle_client

# The client-side planner opens Oracle connections in the pytest process
# itself, whose loader search path was fixed at exec time and therefore never
# sees a per-run directory prepend.  Keep the flat client copy in build/lib in
# step with the selected generation: both the pytest process and the taosd it
# starts resolve libnnz*/libclntsh* from there.  The other generation's stale
# copies are removed first so a flat symlink cannot redirect a copy into the
# wrong file.
flatten_oracle_client() {
    local dir="$oracleClientDir"
    [ -n "$dir" ] || return 0
    local pattern src base tmp
    local -A keep=()
    # Copy through a temporary name and rename into place: replacing a file
    # that a running process has mmap'ed by truncation turns the next access
    # into SIGBUS, while an atomic rename leaves the old inode intact.
    for pattern in 'libsqora*.so*' 'libclntsh*.so*' 'libnnz*.so*' 'libociei*.so*' \
                   'libons*.so*' 'libnfsodm*.so*' 'liborazt*.so*' 'legacy.so*' \
                   'fips*.so*' 'libaio*.so*'; do
        for src in "${dir}"/${pattern}; do
            [ -e "${src}" ] || continue
            base=${src##*/}
            keep["${base}"]=1
            tmp="${RUNTIME_DIR}/.fq-oracle.$$.${base}"
            if cp -Lf "${src}" "${tmp}"; then
                mv -f "${tmp}" "${RUNTIME_DIR}/${base}"
            else
                rm -f "${tmp}"
            fi
        done
    done
    # Drop files from the other generation that the selected client does not
    # provide (libnnz.so, legacy.so, ...), so a by-name lookup cannot pick a
    # foreign client.
    for pattern in 'libsqora*.so*' 'libclntsh*.so*' 'libnnz*.so*' 'libociei*.so*' \
                   'libons*.so*' 'libnfsodm*.so*' 'liborazt*.so*' 'legacy.so*' \
                   'fips*.so*' 'libaio*.so*'; do
        for src in "${RUNTIME_DIR}"/${pattern}; do
            [ -e "${src}" ] || continue
            base=${src##*/}
            [ -n "${keep[${base}]:-}" ] || rm -f "${src}"
        done
    done
}
flatten_oracle_client

bash "${STAGE_SCRIPT}" fix-darwin-rpaths "${RUNTIME_DIR}" || true

cat <<EOF
[fq-runtime] staged runtime libs in ${RUNTIME_DIR}
export LD_LIBRARY_PATH=${oracleClientDir:+${oracleClientDir}:}${RUNTIME_DIR}:\$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=${oracleClientDir:+${oracleClientDir}:}${RUNTIME_DIR}:\$DYLD_LIBRARY_PATH
EOF
