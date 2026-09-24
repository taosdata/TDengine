#!/usr/bin/env bash

set -euo pipefail

generator=$1
ds=$2
header=$3
source=$4
temporary_header=$(mktemp "${TMPDIR:-/tmp}/taosQueryTest-dimension-ids.XXXXXX")
temporary_source=$(mktemp "${TMPDIR:-/tmp}/taosQueryTest-dimension-source.XXXXXX")

cleanup() {
  rm -f -- "$temporary_header" "$temporary_source"
}
trap cleanup EXIT

python3 "$generator" --ds "$ds" --header "$temporary_header" --source "$temporary_source"
cmp -- "$temporary_header" "$header"
cmp -- "$temporary_source" "$source"
