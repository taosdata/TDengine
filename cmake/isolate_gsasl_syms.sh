#!/usr/bin/env bash
#
# Isolate the gnulib symbols that GNU SASL (libgsasl) statically bundles, so they
# do not collide with TDengine's own implementations when libgsasl.a is linked into
# the same binary.
#
# libgsasl.a exports generic gnulib names such as base64_encode / base64_decode /
# md5_* / sha*_* / hmac_* / memxor. TDengine defines its own base64_encode/base64_decode
# (source/util/src/tbase64.c) with a DIFFERENT signature; statically linking both would
# either fail with a duplicate-symbol error or silently bind callers to the wrong
# implementation (memory corruption). To avoid this we rename every GLOBAL DEFINED symbol
# in the archive that does not belong to the public gsasl API (i.e. whose name does not
# contain "gsasl") to a "tdgs_" prefix. Renaming is applied consistently to both the
# definition and any intra-archive references, so libgsasl's internal linkage is preserved.
#
# Usage: isolate_gsasl_syms.sh <path-to-libgsasl.a> [nm] [objcopy]
set -euo pipefail

LIB="${1:?usage: isolate_gsasl_syms.sh <libgsasl.a> [nm] [objcopy]}"
NM="${2:-}"; [ -z "$NM" ] && NM="nm"
OBJCOPY="${3:-}"; [ -z "$OBJCOPY" ] && OBJCOPY="objcopy"

if [ ! -f "$LIB" ]; then
  echo "isolate_gsasl_syms: archive not found: $LIB" >&2
  exit 1
fi

RD="$(dirname "$LIB")/gsasl_redefine.txt"

# Columns from `nm`: <addr> <type> <name>. Uppercase type = global. We keep only
# globally-defined symbols (T D B R W V) whose name does not contain "gsasl". The
# `!~ /^tdgs_/` guard keeps this idempotent: re-running on an already-isolated archive
# (e.g. CI cache replay or a partial rebuild) must not produce tdgs_tdgs_* symbols.
"$NM" "$LIB" \
  | awk '$2 ~ /^[TDBRWV]$/ && $3 != "" && $3 !~ /gsasl/ && $3 !~ /^tdgs_/ { print $3" tdgs_"$3 }' \
  | sort -u > "$RD"

if [ -s "$RD" ]; then
  "$OBJCOPY" --redefine-syms="$RD" "$LIB"
  echo "isolate_gsasl_syms: renamed $(wc -l < "$RD") symbol(s) in $LIB"
else
  echo "isolate_gsasl_syms: nothing to rename in $LIB"
fi
