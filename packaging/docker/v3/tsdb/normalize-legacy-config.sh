#!/usr/bin/env bash

# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

set -euo pipefail

# Only undo known host-installer rewrites of local service addresses.
# Leave node identities, listeners, credentials and unrelated settings untouched.
config_dir=${1:?configuration directory required}
export LEGACY_BUILD_HOST=${2:?build hostname required}
for config in "$config_dir"/taos.cfg "$config_dir"/taosx.toml \
              "$config_dir"/explorer.toml "$config_dir"/taosadapter.toml \
              "$config_dir"/taoskeeper.toml; do
    [ -f "$config" ] || continue
    awk -v name="$(basename "$config")" '
        BEGIN { host = ENVIRON["LEGACY_BUILD_HOST"]; section = "" }
        /^[[:space:]]*#/ { print; next }
        /^[[:space:]]*\[/ {
            section = $0
            sub(/^[[:space:]]*\[/, "", section)
            sub(/\].*$/, "", section)
        }
        {
            key = $0
            sub(/^[[:space:]]*/, "", key)
            sub(/[[:space:]=].*$/, "", key)
            if (name == "taos.cfg") {
                if (key == "fqdn" && $2 == host) next
                if (key == "monitorFQDN" && $2 == host) $0 = "monitorFQDN localhost"
            } else {
                local_address = \
                    (name == "taosx.toml" && section == "monitor" && key == "fqdn") || \
                    (name == "explorer.toml" && section == "" && \
                        (key == "cluster" || key == "cluster_native" || key == "x_api" || key == "grpc")) || \
                    (name == "taoskeeper.toml" && section == "tdengine" && key == "host") || \
                    (name == "taosadapter.toml" && \
                        ((section == "node_exporter" || section == "open_metrics") && key == "urls"))
                if (local_address) $0 = replace_host($0)
            }
            print
        }
        function replace_host(line, result, pos, before, after) {
            result = ""
            while ((pos = index(line, host)) > 0) {
                before = substr(line, pos - 1, 1)
                after = substr(line, pos + length(host), 1)
                result = result substr(line, 1, pos - 1)
                if ((before == "\"" || before == "/") && \
                    (after == "\"" || after == ":" || after == "/")) {
                    result = result "localhost"
                } else {
                    result = result host
                }
                line = substr(line, pos + length(host))
            }
            return result line
        }
    ' "$config" > "$config.image-build"
    cat "$config.image-build" > "$config"
    rm -f "$config.image-build"
done
