#!/bin/bash
# filepath: /usr/local/taos/bin/set_malloc.sh

# ====== CONSTANTS ======
INSTALL_DIR="/usr/local"
TAOS_DIR="${INSTALL_DIR}/taos"
DRIVER_DIR="${TAOS_DIR}/driver"
LOG_DIR="${TAOS_DIR}/log"
BIN_DIR="${TAOS_DIR}/bin"
CFG_DIR="/etc/default/taos"

MALLOC_ENV_SH="${BIN_DIR}/set_taos_malloc_env.sh"
MALLOC_ENV_CONF="${CFG_DIR}/set_taos_malloc_env.conf"
MALLOC_LOG_FILE="${LOG_DIR}/set_taos_malloc.log"
TCMALLOC_LIB="${DRIVER_DIR}/libtcmalloc.so"
TCMALLOC_PREFIX="${LOG_DIR}/tcmalloc/prof"
JEMALLOC_LIB="${DRIVER_DIR}/libjemalloc.so"
JEMALLOC_PREFIX="${LOG_DIR}/jemalloc/prof"

mkdir -p ${CFG_DIR}

usage() {
  echo "Usage: $0 -m <0|1|2|3|4> -q -h"
  echo "Set the memory allocator for TDengine."
  echo "Options:"
  echo "  -m <mode>   Set the memory allocator mode:"
  echo "    0: Default"
  echo "    1: Use tcmalloc for memory optimization"
  echo "    2: Use tcmalloc for memory checking"
  echo "    3: Use jemalloc for memory optimization"
  echo "    4: Use jemalloc for memory checking"
  echo "  -q          Quiet mode, suppress output"
  echo "  -h          Show this help message"
  exit 1
}

quiet=0

while getopts "m:qh" arg; do
  case $arg in
    m)
      mode=$OPTARG
      ;;
    q)
      quiet=1
      ;;
    h)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

if [ -z "$mode" ]; then
  usage
fi

echo "# Auto-generated memory allocator environment variables" > "$MALLOC_ENV_SH"
echo "# Auto-generated memory allocator environment variables" > "$MALLOC_ENV_CONF"


declare -A MODE_DESC
# Define associative arrays for each mode
declare -A SH_VARS
declare -A CONF_VARS

case "$mode" in
  0)
    MODE_DESC="Glibc default malloc"
    SH_VARS[0]="# Use system default memory allocator"
    CONF_VARS[0]="# Use system default memory allocator"
    ;;
  1)
    MODE_DESC="Tcmalloc optimization"
    SH_VARS[0]="export LD_PRELOAD=${TCMALLOC_LIB}"
    CONF_VARS[0]="LD_PRELOAD=${TCMALLOC_LIB}"
    ;;
  2)
    MODE_DESC="Tcmalloc strict checking"
    SH_VARS[0]="export LD_PRELOAD=${TCMALLOC_LIB}"
    SH_VARS[1]="export HEAPCHECK=strict"
    SH_VARS[2]="export HEAPPROFILE=${TCMALLOC_PREFIX}"
    SH_VARS[3]="export HEAP_PROFILE_ALLOCATION_INTERVAL=8147483648"
    SH_VARS[4]="export HEAP_PROFILE_INUSE_INTERVAL=536870912"
    CONF_VARS[0]="LD_PRELOAD=${TCMALLOC_LIB}"
    CONF_VARS[1]="HEAPCHECK=strict"
    CONF_VARS[2]="HEAPPROFILE=${TCMALLOC_PREFIX}"
    CONF_VARS[3]="HEAP_PROFILE_ALLOCATION_INTERVAL=8147483648"
    CONF_VARS[4]="HEAP_PROFILE_INUSE_INTERVAL=536870912"
    ;;
  3)
    MODE_DESC="Jemalloc optimization"
    SH_VARS[0]="export LD_PRELOAD=${JEMALLOC_LIB}"
    SH_VARS[1]="export MALLOC_CONF=\"percpu_arena:percpu,metadata_thp:auto,dirty_decay_ms:10000\""
    CONF_VARS[0]="LD_PRELOAD=${JEMALLOC_LIB}"
    CONF_VARS[1]="MALLOC_CONF=\"percpu_arena:percpu,metadata_thp:auto,dirty_decay_ms:10000\""
    ;;
  4)
    MODE_DESC="Jemalloc checking"
    SH_VARS[0]="export LD_PRELOAD=${JEMALLOC_LIB}"
    SH_VARS[1]="export MALLOC_CONF=\"percpu_arena:percpu,abort_conf:true,prof:true,prof_prefix:${JEMALLOC_PREFIX},prof_active:true,lg_prof_sample:20\""
    CONF_VARS[0]="LD_PRELOAD=${JEMALLOC_LIB}"
    CONF_VARS[1]="MALLOC_CONF=\"percpu_arena:percpu,abort_conf:true,prof:true,prof_prefix:${JEMALLOC_PREFIX},prof_active:true,lg_prof_sample:20\""
    ;;
  *)
    usage
    ;;
esac

echo "$(date '+%Y-%m-%d %H:%M:%S') set mode $mode (${MODE_DESC}) by $USER" >> "$MALLOC_LOG_FILE"

for i in $(seq 0 $((${#SH_VARS[@]}-1))); do
  echo "${SH_VARS[$i]}" >> "$MALLOC_ENV_SH"
done

for i in $(seq 0 $((${#CONF_VARS[@]}-1))); do
  echo "${CONF_VARS[$i]}" >> "$MALLOC_ENV_CONF"
done

for i in $(seq 0 $((${#CONF_VARS[@]}-1))); do
  echo "${CONF_VARS[$i]}" >> "$MALLOC_ENV_CONF"
done

if [ "$quiet" -ne 1 ]; then
  echo "---------------------------------------------"
  echo "Memory allocator setting complete!"
  echo "  Mode: $mode (${MODE_DESC})"
  echo "  Shell env file:    $MALLOC_ENV_SH"
  echo "  Systemd env file:  $MALLOC_ENV_CONF"
  echo
  echo "To use in shell:    source $MALLOC_ENV_SH"
  echo "To use in systemd:  Just restart your service, EnvironmentFile is already configured."
  echo "---------------------------------------------"
else
  echo "Memory allocator Mode: $mode (${MODE_DESC})"
fi