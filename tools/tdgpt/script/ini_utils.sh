#!/bin/bash
# Common INI parser and argument handler for TDengine TDgpt scripts

# Default config path, can be overridden by -c or TAOSANODE_CONFIG env
CONFIG_FILE="${TAOSANODE_CONFIG:-/etc/taos/taosanode.ini}"

# Parse -c (config file) and -h (help) arguments
parse_args() {
  while getopts ":c:h" opt; do
    case "$opt" in
      c) CONFIG_FILE="$OPTARG" ;;
      h) echo "usage: $0 [-c config_file_path]"; exit 0 ;;
      *) echo "Invalid option: -$OPTARG" >&2; exit 2 ;;
    esac
  done
}

# Pure Bash INI parser: supports [section], key=value, #/; comments, trims spaces and quotes
ini_get() {
  local section="$1" key="$2" default="${3-}" file="$CONFIG_FILE"
  local in_section=0 line k v
  while IFS= read -r line || [ -n "$line" ]; do
    # Remove comments and trim spaces
    line="${line%%\#*}"; line="${line%%;*}"
    line="$(printf '%s' "$line" | sed -e 's/^[[:space:]]\+//' -e 's/[[:space:]]\+$//')"
    [ -z "$line" ] && continue
    # Section header
    if [[ "$line" =~ ^\[(.+)\]$ ]]; then
      [[ "${BASH_REMATCH[1]}" == "$section" ]] && in_section=1 || in_section=0
      continue
    fi
    # Key-value pair
    if [[ $in_section -eq 1 && "$line" =~ ^([^=]+)=[[:space:]]*(.*)$ ]]; then
      k="$(printf '%s' "${BASH_REMATCH[1]}" | sed 's/[[:space:]]//g')"
      v="${BASH_REMATCH[2]}"
      # Remove surrounding quotes
      v="${v%\"}"; v="${v#\"}"; v="${v%\'}"; v="${v#\'}"
      if [[ "$k" == "$key" ]]; then
        printf '%s\n' "$v"
        return 0
      fi
    fi
  done < "$file"
  printf '%s\n' "$default"
}