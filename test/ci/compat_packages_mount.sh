#!/bin/bash
# Map container /usr/local/src to host compat-packages cache when available.
# ensure_ext_env.sh reads MySQL/Influx tarballs and fq-apt-*.tar.gz from
# /usr/local/src (container mount of the returned SOURCEDIR path).

prepare_compat_packages_sourcedir() {
    local workdir="$1"
    local sourcedir="${workdir}/src"

    if [ -d "/data0/compat-packages" ]; then
        # $workdir/src may already be a populated directory on long-lived CI
        # runners; ln cannot replace it.  Mount compat-packages directly instead.
        if [ ! -e "$sourcedir" ] || [ -L "$sourcedir" ]; then
            ln -sfn /data0/compat-packages "$sourcedir"
            sourcedir=$(readlink -f "$sourcedir")
        else
            sourcedir="/data0/compat-packages"
        fi
        echo "[compat-packages] mount ${sourcedir} -> /usr/local/src/" >&2
    else
        mkdir -p "$sourcedir"
    fi
    printf '%s\n' "$sourcedir"
}
