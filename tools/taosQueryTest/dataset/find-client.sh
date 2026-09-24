#!/bin/sh

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
