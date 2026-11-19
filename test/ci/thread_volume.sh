#!/bin/bash

# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

# 按调度选中的通道把用例数据分到多块盘。通道 0 是 WORKDIR 所在盘，保持原路径。
# 其余通道来自 CI_EXTRA_VOLUME_ROOT（逗号分隔，显式指定时不再扫描），
# 未设置时自动使用 /dataN/tdengine-ci 里与 WORKDIR 不同块设备的目录。
# 通道号由 worker 按磁盘利用率写入 ${WORKDIR}/tmp/thread_volume/<thread>.lane，
# 也可以用第二个参数直接指定。没有通道号时留在主盘。

_thread_volume_primary_root() {
    local base="${WORKDIR:-}"
    base="${base%/}"
    [[ -n "${base}" ]] || return 1
    if [[ "$(basename "${base}")" == job-* ]]; then
        dirname -- "${base}"
    else
        printf '%s\n' "${base}"
    fi
}

_thread_volume_device() {
    stat -c %d -- "$1" 2>/dev/null || stat -f %d -- "$1" 2>/dev/null || true
}

# 额外盘剩余空间，单位 KiB。df 读不到时返回空，调用方会跳过这块盘。
_thread_volume_avail_kib() {
    df -Pk "${1}" 2>/dev/null | awk 'NR==2 { print $4 }'
}

# 接受一块额外盘。调用方传入主盘路径和它的设备号。不合法时返回 1。
# 这里不看剩余空间。清理要能找到已经用过的盘，哪怕它后来满了。
_thread_volume_accept_extra() {
    local root="${1%/}"
    local primary="${2%/}"
    local primary_dev=$3
    local dev
    [[ -n "${root}" && "${root}" == /* && -d "${root}" && -w "${root}" ]] || return 1
    case "${root}" in
        / | /root | /home | /etc | /var | /usr | /tmp | /data1 | /data1/*) return 1 ;;
    esac
    if [[ -n "${primary}" && ( "${root}" == "${primary}" || "${primary}" == "${root}"/* || "${root}" == "${primary}"/* ) ]]; then
        return 1
    fi
    dev="$(_thread_volume_device "${root}")"
    [[ -n "${dev}" ]] || return 1
    [[ -n "${primary_dev}" && "${dev}" == "${primary_dev}" ]] && return 1
    printf '%s\n' "${root}"
}

# 登记和选盘才看剩余空间。低于 20GiB 的盘不接收新用例。
_thread_volume_enough_space() {
    local avail_kib min_kib=$((20 * 1024 * 1024))
    avail_kib="$(_thread_volume_avail_kib "$1")"
    [[ "${avail_kib}" =~ ^[0-9]+$ && ${avail_kib} -ge ${min_kib} ]]
}

# 每行一个通道，第一行是主盘。额外盘按路径排序，保证各 job 的取模一致。
_thread_volume_lanes() {
    local primary primary_dev candidate root glob_was=0
    local -a extras
    extras=()
    primary="$(_thread_volume_primary_root)" || return 1
    primary_dev="$(_thread_volume_device "${primary}")"
    printf '%s\n' "${primary}"
    if [[ -n "${CI_EXTRA_VOLUME_ROOT:-}" ]]; then
        local part
        for part in ${CI_EXTRA_VOLUME_ROOT//,/ }; do
            root="$(_thread_volume_accept_extra "${part}" "${primary}" "${primary_dev}")" || continue
            extras+=("${root}")
        done
    else
        shopt -q nullglob && glob_was=1
        shopt -s nullglob
        for candidate in ${CI_VOLUME_DISCOVER_ROOT:-}/data[0-9]*/tdengine-ci; do
            root="$(_thread_volume_accept_extra "${candidate}" "${primary}" "${primary_dev}")" || continue
            extras+=("${root}")
        done
        [[ ${glob_was} -eq 1 ]] || shopt -u nullglob
    fi
    if [[ ${#extras[@]} -gt 0 ]]; then
        printf '%s\n' "${extras[@]}" | sort -u
    fi
}

# 登记用的通道。额外盘空间不足时不出现，主盘始终保留。
_thread_volume_admission_lanes() {
    local line first=1
    while IFS= read -r line; do
        [[ -n "${line}" ]] || continue
        if [[ ${first} -eq 1 ]]; then
            first=0
            printf '%s\n' "${line}"
            continue
        fi
        _thread_volume_enough_space "${line}" || continue
        printf '%s\n' "${line}"
    done < <(_thread_volume_lanes)
}

_thread_volume_admission_extra_roots() {
    local line first=1
    while IFS= read -r line; do
        if [[ ${first} -eq 1 ]]; then
            first=0
            continue
        fi
        [[ -n "${line}" ]] || continue
        printf '%s\n' "${line}"
    done < <(_thread_volume_admission_lanes)
}

# 本 job 登记时写下的通道。运行中不再重新发现，避免 lane 号错位。
# 没有这份文件时只用主盘。实时发现可能带上满盘，也可能和登记时的列表不一致。
_thread_volume_prepare_lanes() {
    local cache="${WORKDIR}/tmp/thread_volume/lanes"
    if [[ -f "${cache}" ]]; then
        cat -- "${cache}"
        return 0
    fi
    _thread_volume_primary_root || true
}

_thread_volume_extra_roots() {
    local line first=1
    while IFS= read -r line; do
        if [[ ${first} -eq 1 ]]; then
            first=0
            continue
        fi
        [[ -n "${line}" ]] || continue
        printf '%s\n' "${line}"
    done < <(_thread_volume_lanes)
}

_thread_volume_extra_root() {
    local line
    line="$(_thread_volume_extra_roots)" || return 1
    [[ -n "${line}" ]] || return 1
    printf '%s\n' "${line%%$'\n'*}"
}

prepare_thread_volume() {
    local thread_no=$1
    local requested=${2:-}
    local legacy="${WORKDIR}/tmp/thread_volume/${thread_no}"
    local lane_file="${WORKDIR}/tmp/thread_volume/${thread_no}.lane"
    local -a lanes
    lanes=()
    local line lane_count lane=0 root real current

    mkdir -p "${legacy}"
    while IFS= read -r line; do
        [[ -n "${line}" ]] && lanes+=("${line}")
    done < <(_thread_volume_prepare_lanes)
    lane_count=${#lanes[@]}
    [[ "${thread_no}" =~ ^[0-9]+$ ]] || {
        echo "[thread-volume] ERROR: thread_no must be numeric, got '${thread_no}'" >&2
        return 1
    }
    if [[ -n "${requested}" ]]; then
        lane=${requested}
    elif [[ -f "${lane_file}" ]]; then
        lane=$(tr -d '[:space:]' < "${lane_file}")
    fi
    [[ "${lane}" =~ ^[0-9]+$ ]] || lane=0
    # 主盘，或只有一块盘：卸掉上一个用例留下的 bind。
    if [[ ${lane_count} -le 1 || ${lane} -eq 0 ]]; then
        [[ ${lane} -eq 0 ]] || printf '%s\n' 0 > "${lane_file}"
        if mountpoint -q "${legacy}"; then
            umount "${legacy}" 2>/dev/null || umount -l "${legacy}" 2>/dev/null || return 1
        fi
        return 0
    fi
    if [[ ${lane} -ge ${lane_count} ]]; then
        echo "[thread-volume] lane ${lane} is outside ${lane_count} volumes; staying on the primary disk" >&2
        printf '%s\n' 0 > "${lane_file}"
        if mountpoint -q "${legacy}"; then
            umount "${legacy}" 2>/dev/null || umount -l "${legacy}" 2>/dev/null || return 1
        fi
        return 0
    fi
    # Jenkins 和本地调用没有 CI_JOB_ID。留在原盘，不要让整个用例失败。
    [[ "${CI_JOB_ID:-}" =~ ^[0-9]+$ ]] || {
        echo "[thread-volume] extra volume present but CI_JOB_ID is unset; staying on the primary disk" >&2
        printf '%s\n' 0 > "${lane_file}"
        if mountpoint -q "${legacy}"; then
            umount "${legacy}" 2>/dev/null || umount -l "${legacy}" 2>/dev/null || return 1
        fi
        return 0
    }
    root="${lanes[${lane}]}"
    # 登记后这块盘可能被写满。通道号不变，这一条用例留在主盘。
    if ! _thread_volume_enough_space "${root}"; then
        echo "[thread-volume] thread ${thread_no} lane ${lane} has under 20GiB free; staying on the primary disk" >&2
        printf '%s\n' 0 > "${lane_file}"
        if mountpoint -q "${legacy}"; then
            umount "${legacy}" 2>/dev/null || umount -l "${legacy}" 2>/dev/null || return 1
        fi
        return 0
    fi
    real="${root}/job-${CI_JOB_ID}/thread_volume/${thread_no}"
    mkdir -p "${real}"
    if mountpoint -q "${legacy}"; then
        current=$(findmnt -n -o SOURCE -- "${legacy}" 2>/dev/null || true)
        [[ "${current}" == "${real}" ]] && return 0
        umount "${legacy}" 2>/dev/null || umount -l "${legacy}" 2>/dev/null || return 1
    fi
    echo "[thread-volume] thread ${thread_no} lane ${lane} bind ${real} -> ${legacy}"
    if ! mount --bind "${real}" "${legacy}"; then
        # bind 失败就留在主盘。卸掉可能留下的半成品挂载，用例仍要继续。
        echo "[thread-volume] thread ${thread_no} lane ${lane} bind failed; staying on the primary disk" >&2
        printf '%s\n' 0 > "${lane_file}"
        if mountpoint -q "${legacy}"; then
            umount "${legacy}" 2>/dev/null || umount -l "${legacy}" 2>/dev/null || true
        fi
        return 0
    fi
}

_thread_volume_job_mounts() {
    local tvol="${1%/}" targets mp
    if [[ -d "${tvol}" ]]; then
        tvol=$(cd -- "${tvol}" && pwd -P) || return 1
    fi
    targets=$(findmnt --raw --noheadings --output TARGET) || return 1
    while IFS= read -r mp; do
        printf -v mp '%b' "${mp}"
        if [[ "${mp}" == "${tvol}" || "${mp}" == "${tvol}"/* ]]; then
            printf '%s\n' "${mp}"
        fi
    done <<< "${targets}" | sort -r
}

unmount_job_thread_volumes() {
    local workdir="${1%/}"
    local tvol="${workdir}/tmp/thread_volume"
    local mp mounts

    [[ "${workdir}" == /* && "${workdir##*/}" =~ ^job-[0-9]+$ ]] || return 1

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        echo "[thread-volume] DRY-RUN unmount ${workdir}"
        return 0
    fi
    mounts=$(_thread_volume_job_mounts "${tvol}") || return 1
    while IFS= read -r mp; do
        [[ -n "${mp}" ]] || continue
        if ! umount -- "${mp}"; then
            echo "[thread-volume] ERROR: unable to unmount ${mp}; retaining job data" >&2
            return 1
        fi
    done <<< "${mounts}"
    mounts=$(_thread_volume_job_mounts "${tvol}") || return 1
    if [[ -n "${mounts}" ]]; then
        echo "[thread-volume] ERROR: job mounts remain; retaining job data" >&2
        return 1
    fi
}

# 先卸掉 job 工作目录上的 bind，再删除第二块盘上的 job 目录。
# 调用方在收完日志之后、删除 WORKDIR 之前调用。
# 卸不掉，或额外盘目录删不掉，就停。继续 rm 会删掉额外盘上仍被挂着的内容。
release_job_thread_volumes() {
    local workdir="${1%/}"
    local job_id=$2
    local root dest mounts

    [[ "${job_id}" =~ ^[0-9]+$ && "${workdir}" == /*/job-"${job_id}" ]] || return 1
    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        echo "[thread-volume] DRY-RUN release ${workdir} job ${job_id}"
        return 0
    fi
    unmount_job_thread_volumes "${workdir}" || return 1
    mounts=$(_thread_volume_job_mounts "${workdir}") || return 1
    [[ -z "${mounts}" ]] || return 1
    while IFS= read -r root; do
        [[ -n "${root}" ]] || continue
        dest="${root}/job-${job_id}"
        if [[ -d "${dest}" ]]; then
            chmod -R u+w -- "${dest}" 2>/dev/null || true
            rm -rf --one-file-system -- "${dest}" || return 1
            if [[ -e "${dest}" ]]; then
                echo "[thread-volume] failed to remove ${dest}" >&2
                return 1
            fi
        fi
    done < <(_thread_volume_extra_roots)
}
