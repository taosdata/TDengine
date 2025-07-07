#!/bin/bash
# filepath: /usr/local/taos/bin/set_malloc.sh

install_dir="/usr/local/"
ENV_SH="${install_dir}/taos/bin/taos_env.sh"
ENV_CONF="${install_dir}/taos/cfg/taos_env.conf"
LOG_FILE="${install_dir}/taos/log/set_taos_malloc.log"

usage() {
  echo "Usage: $0 -m <0|1|2|3|4>"
  echo "  0: Default"
  echo "  1: Use tcmalloc for memory optimization"
  echo "  2: Use tcmalloc for memory checking"
  echo "  3: Use jemalloc for memory optimization"
  echo "  4: Use jemalloc for memory checking"
  exit 1
}

while getopts "m:h" arg; do
  case $arg in
    m)
      mode=$OPTARG
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

echo "# Auto-generated memory allocator environment variables" > "$ENV_SH"
echo "# Auto-generated memory allocator environment variables" > "$ENV_CONF"

declare -A MODE_DESC
MODE_DESC[0]="Default"
MODE_DESC[1]="tcmalloc optimization"
MODE_DESC[2]="tcmalloc strict checking"
MODE_DESC[3]="jemalloc optimization"
MODE_DESC[4]="jemalloc checking"


echo "$(date '+%Y-%m-%d %H:%M:%S') set mode $mode (${MODE_DESC[$mode]}) by $USER" >> "$LOG_FILE"

# Define associative arrays for each mode
declare -A SH_VARS
declare -A CONF_VARS

case "$mode" in
  0)
    SH_VARS[0]="# Use system default memory allocator"
    CONF_VARS[0]="# Use system default memory allocator"
    ;;
  1)
    SH_VARS[0]="export LD_PRELOAD=${install_dir}/taos/driver/libtcmalloc.so"
    CONF_VARS[0]="LD_PRELOAD=${install_dir}/taos/driver/libtcmalloc.so"
    ;;
  2)
    SH_VARS[0]="export LD_PRELOAD=${install_dir}/taos/driver/libtcmalloc.so"
    SH_VARS[1]="export HEAPCHECK=strict"
    SH_VARS[2]="export HEAPPROFILE=${install_dir}/taos/log"
    CONF_VARS[0]="LD_PRELOAD=${install_dir}/taos/driver/libtcmalloc.so"
    CONF_VARS[1]="HEAPCHECK=strict"
    CONF_VARS[2]="HEAPPROFILE=${install_dir}/taos/log/report"
    ;;
  3)
    SH_VARS[0]="export LD_PRELOAD=${install_dir}/taos/driver/libjemalloc.so"
    SH_VARS[1]="export MALLOC_CONF=\"percpu_arena:percpu,metadata_thp:auto,dirty_decay_ms:10000\""
    CONF_VARS[0]="LD_PRELOAD=${install_dir}/taos/driver/libjemalloc.so"
    CONF_VARS[1]="MALLOC_CONF=\"percpu_arena:percpu,metadata_thp:auto,dirty_decay_ms:10000\""
    ;;
  4)
    SH_VARS[0]="export LD_PRELOAD=${install_dir}/taos/driver/libjemalloc.so"
    SH_VARS[1]="export MALLOC_CONF=\"percpu_arena:percpu,abort_conf:true,prof:true,prof_prefix:/tmp/jemalloc_profile/prof,prof_active:true,lg_prof_sample:20\""
    CONF_VARS[0]="LD_PRELOAD=${install_dir}/taos/driver/libjemalloc.so"
    CONF_VARS[1]="MALLOC_CONF=\"percpu_arena:percpu,abort_conf:true,prof:true,prof_prefix:/tmp/jemalloc_profile/prof,prof_active:true,lg_prof_sample:20\""
    ;;
  *)
    usage
    ;;
esac

for v in "${SH_VARS[@]}"; do
  echo "$v" >> "$ENV_SH"
done

for v in "${CONF_VARS[@]}"; do
  echo "$v" >> "$ENV_CONF"
done

echo "---------------------------------------------"
echo "Memory allocator setting complete!"
echo "  Mode: $mode (${MODE_DESC[$mode]})"
echo "  Shell env file:    $ENV_SH"
echo "  Systemd env file:  $ENV_CONF"
echo
echo "To use in shell:    source $ENV_SH"
echo "To use in systemd:  Just restart your service, EnvironmentFile is already configured."
echo "---------------------------------------------"