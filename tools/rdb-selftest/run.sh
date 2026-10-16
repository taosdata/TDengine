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

set -u

WORK="./rdbselftest-work"
ITERS=20
RECORDS=200000
VSIZE=96
ROUNDS=1
MAXSEC=0
JOBS=1
KEEPDB=0
DROPCACHE=0

while [ $# -gt 0 ]; do
    case "$1" in
        -d) WORK=$2; shift 2 ;;
        -i) ITERS=$2; shift 2 ;;
        -n) RECORDS=$2; shift 2 ;;
        -v) VSIZE=$2; shift 2 ;;
        -r) ROUNDS=$2; shift 2 ;;
        -t) MAXSEC=$2; shift 2 ;;
        -j) JOBS=$2; shift 2 ;;
        --keep-db) KEEPDB=1; shift ;;
        --drop-cache) DROPCACHE=1; shift ;;
        -h|--help) sed -n '3,45p' "$0" | sed 's|^# \{0,1\}||'; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

HERE=$(cd "$(dirname "$0")" && pwd)

# --- locate the binaries, building them if needed ---
find_bin() {
    local name=$1
    for c in "$HERE/$name" \
             "$HERE/../../../../debug/build/bin/$name" \
             "$HERE/../../../../../debug/build/bin/$name" \
             "$(command -v "$name" 2>/dev/null)"; do
        if [ -n "$c" ] && [ -x "$c" ]; then echo "$c"; return 0; fi
    done
    return 1
}

WRITE=$(find_bin rdbwrite || true)
READ=$(find_bin rdbread || true)
SSTCHK=$(find_bin rdbsst || true)

if [ -z "${WRITE:-}" ] || [ -z "${READ:-}" ]; then
    echo "rdbwrite/rdbread not found. Build them first:"
    echo
    echo "  cd <repo>/debug && make rdbwrite rdbread rdbsst -j8"
    echo
    echo "or standalone, pointing at any librocksdb.a plus its headers:"
    echo "  gcc -O2 -I<rocksdb>/include -I. -o rdbwrite rdbwrite.c \\"
    echo "      <path>/librocksdb.a -lpthread -lz -lsnappy -lbz2 -ldl -lstdc++ -lm"
    exit 2
fi

mkdir -p "$WORK" || exit 2
WORK=$(cd "$WORK" && pwd)

echo "rdb-selftest"
echo "  work dir : $WORK"
echo "  records  : $RECORDS x $VSIZE bytes"
echo "  read     : $ROUNDS round(s) per iteration"
echo "  jobs     : $JOBS"
[ "$KEEPDB" = 1 ]    && echo "  mode     : write once, re-read each iteration"
[ "$DROPCACHE" = 1 ] && echo "  cache    : dropped before each read"
if [ "$ITERS" = 0 ]; then echo "  loop     : until Ctrl-C"; else echo "  loop     : $ITERS iterations"; fi
[ "$MAXSEC" != 0 ]   && echo "  time cap : ${MAXSEC}s"
echo

STOP=0
trap 'STOP=1; echo; echo "interrupted, finishing current iteration..."' INT

START=$(date +%s)
PASSED=0
FAILED=0
FAILDIRS=""

# one_iteration <index> ; echoes "ok" or "fail <dbpath>"
one_iteration() {
    local idx=$1
    local db="$WORK/db-$idx"
    local seed=$((idx + 1))
    local log="$WORK/iter-$idx.log"

    if [ "$KEEPDB" = 1 ]; then
        db="$WORK/db-keep"
        seed=1
        if [ ! -d "$db" ]; then
            if ! "$WRITE" "$db" -n "$RECORDS" -v "$VSIZE" -s "$seed" >"$log" 2>&1; then
                echo "fail $db"; return
            fi
        fi
    else
        rm -rf "$db"
        if ! "$WRITE" "$db" -n "$RECORDS" -v "$VSIZE" -s "$seed" >"$log" 2>&1; then
            echo "fail $db"; return
        fi
    fi

    if [ "$DROPCACHE" = 1 ]; then
        sync
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    fi

    if ! "$READ" "$db" -n "$RECORDS" -v "$VSIZE" -s "$seed" -r "$ROUNDS" >>"$log" 2>&1; then
        echo "fail $db"; return
    fi

    # Success: reclaim the space unless we are reusing the database.
    [ "$KEEPDB" = 1 ] || rm -rf "$db"
    rm -f "$log"
    echo "ok"
}

i=0
while : ; do
    [ "$ITERS" != 0 ] && [ "$i" -ge "$ITERS" ] && break
    [ "$STOP" = 1 ] && break
    if [ "$MAXSEC" != 0 ]; then
        NOW=$(date +%s)
        [ $((NOW - START)) -ge "$MAXSEC" ] && { echo "time cap reached"; break; }
    fi

    if [ "$JOBS" -le 1 ]; then
        RES=$(one_iteration "$i")
        set -- $RES
        if [ "$1" = "ok" ]; then
            PASSED=$((PASSED + 1))
            printf "\riteration %-6d passed=%-6d failed=%-4d" "$i" "$PASSED" "$FAILED"
        else
            FAILED=$((FAILED + 1))
            FAILDIRS="$FAILDIRS $2"
            echo
            echo "=== ITERATION $i FAILED ==="
            cat "$WORK/iter-$i.log" 2>/dev/null | tail -20
            echo "database kept at: $2"
        fi
        i=$((i + 1))
    else
        # Run a batch of JOBS iterations concurrently.
        PIDS=""
        IDX=""
        for _ in $(seq 1 "$JOBS"); do
            ( one_iteration "$i" > "$WORK/res-$i" ) &
            PIDS="$PIDS $!"
            IDX="$IDX $i"
            i=$((i + 1))
        done
        for pid in $PIDS; do wait "$pid"; done
        for j in $IDX; do
            RES=$(cat "$WORK/res-$j" 2>/dev/null)
            rm -f "$WORK/res-$j"
            set -- $RES
            if [ "${1:-}" = "ok" ]; then
                PASSED=$((PASSED + 1))
            else
                FAILED=$((FAILED + 1))
                FAILDIRS="$FAILDIRS ${2:-unknown}"
                echo
                echo "=== ITERATION $j FAILED ==="
                tail -20 "$WORK/iter-$j.log" 2>/dev/null
                echo "database kept at: ${2:-unknown}"
            fi
        done
        printf "\riterations %-6d passed=%-6d failed=%-4d" "$i" "$PASSED" "$FAILED"
    fi
done

ELAPSED=$(( $(date +%s) - START ))
echo
echo
echo "=============================== summary ==============================="
echo "iterations : $((PASSED + FAILED))"
echo "passed     : $PASSED"
echo "failed     : $FAILED"
echo "elapsed    : ${ELAPSED}s"

if [ "$FAILED" = 0 ]; then
    cat <<'EOF'

No failure observed.

This does not prove the machine is healthy. An intermittent fault can stay
quiet for a long time, so a clean run is evidence only in proportion to how
long it ran and how much load it ran under. To push harder:

  ./run.sh -i 0 -j 4 -n 500000 -r 3 --drop-cache

and in parallel, keep the machine under the memory pressure it sees in
production -- the original incident coincided with heavy WAL replay.
EOF
    exit 0
fi

cat <<'EOF'

Failures were observed. The databases have been kept.

Next step, and the one that decides the diagnosis: re-check the kept files with
rdbsst. Take the offset and size from the error message above.
EOF
echo
for d in $FAILDIRS; do
    [ -d "$d" ] || continue
    for f in "$d"/*.sst; do
        [ -r "$f" ] || continue
        if [ -n "${SSTCHK:-}" ]; then
            echo "--- rdbsst $f ---"
            "$SSTCHK" "$f" 2>&1 | sed -n '4,12p'
        else
            echo "  $f  (build rdbsst to check it)"
        fi
    done
done

cat <<'EOF'

Reading the result:

  rdbsst also reports MISMATCH
        -> the bytes on disk really are wrong. The corruption is persistent:
           look at the media, the controller, the RAID write cache and its
           battery, and the filesystem.

  rdbsst reports OK / MATCH
        -> the bytes on disk are correct, so the read that failed was transient.
           SST files are immutable once written, so nothing changed the file
           between the two reads. The fault is in the path between the platter
           and the process: RAM, CPU cache, HBA, cable, or controller cache.

Either way, check the hardware counters on this host:

  smartctl -a /dev/XXX | grep -iE "UDMA_CRC_Error|Media_and_Data_Integrity|Current_Pending|Reallocated"
  ras-mc-ctl --error-count ; edac-util -v
  dmidecode -t memory | grep -i "error correction"   # non-ECC => counters stay empty
  dmesg -T | grep -iE "edac|mce|hardware error|blk_update_request|i/o error"
EOF
exit 1
