#!/bin/bash
# Usage: extract_pkg.sh <package_path> <target_dir>
# Extracts taosd and the client library from a TDengine installation package.
# A package built with the thin dispatcher ships libtaos.so and libtaosnative.so
# (both are copied, names unchanged); older packages ship a single self-contained
# library, which becomes libtaos.so.

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 <package_path> <target_dir>"
    exit 1
fi

PKG_PATH="$1"
TARGET_DIR="$2"

if [ ! -f "$PKG_PATH" ]; then
    echo "Error: package not found: $PKG_PATH"
    exit 1
fi

if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: target directory not found: $TARGET_DIR"
    exit 1
fi

# Create a temp dir to extract into
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

echo "Extracting $PKG_PATH ..."
tar -xzf "$PKG_PATH" -C "$TMP_DIR"

# Find and extract inner package.tar.gz
INNER_PKG=$(find "$TMP_DIR" -name "package.tar.gz" | head -1)
if [ -z "$INNER_PKG" ]; then
    echo "Error: package.tar.gz not found inside the package"
    exit 1
fi

INNER_DIR=$(dirname "$INNER_PKG")
echo "Extracting inner package.tar.gz ..."
tar -xzf "$INNER_PKG" -C "$INNER_DIR"

# Find taosd
TAOSD=$(find "$INNER_DIR/bin" -name "taosd" -type f 2>/dev/null | head -1)
if [ -z "$TAOSD" ]; then
    echo "Error: taosd not found in bin/"
    exit 1
fi

# Collect the client libraries shipped in driver/.
#
# Three layouts exist:
#   * libtaos.so.<ver> + libtaosnative.so.<ver>  current build.  libtaos.so is a thin
#     dispatcher that dlopen()s the driver found next to it, so the two files only
#     work together and each has to keep its own name.
#   * libtaos.so.<ver> only                      older self-contained client.
#   * libtaosnative.so.<ver> only                older layout, where the native
#     library was also the client and used to be published as libtaos.so.
#
# Dropping the driver is not an option when both are present: the driver hides its
# public API behind tdNativeApiTable, so a renamed copy exports no taos_* symbol
# and the Python client fails on `import taos`.  Existing green version dirs are
# the single-file layouts, so they keep working unchanged.
DRIVER_DIR="$INNER_DIR/driver"
SO_FILE=$(find "$DRIVER_DIR" -name "libtaos.so.*" ! -name "*.so" 2>/dev/null | head -1)
NATIVE_FILE=$(find "$DRIVER_DIR" -name "libtaosnative.so.*" ! -name "*.so" 2>/dev/null | head -1)

if [ -z "$SO_FILE" ] && [ -z "$NATIVE_FILE" ]; then
    echo "Error: libtaos.so.* or libtaosnative.so.* not found in driver/"
    exit 1
fi

# Extract version from .so filename (e.g. libtaosnative.so.3.4.0.0 -> 3.4.0.0)
SO_BASENAME=$(basename "${SO_FILE:-$NATIVE_FILE}")
VERSION=$(echo "$SO_BASENAME" | grep -oP '\d+\.\d+\.\d+\.\d+')
if [ -z "$VERSION" ]; then
    echo "Error: could not extract version from filename: $SO_BASENAME"
    exit 1
fi

echo "Version: $VERSION"

# Create versioned subdir in target
VERSION_DIR="$TARGET_DIR/$VERSION"
mkdir -p "$VERSION_DIR"

# Copy files
cp "$TAOSD" "$VERSION_DIR/taosd"
chmod +x "$VERSION_DIR/taosd"

COPIED=""
if [ -n "$SO_FILE" ]; then
    cp "$SO_FILE" "$VERSION_DIR/libtaos.so"
    COPIED="libtaos.so"
    if [ -n "$NATIVE_FILE" ]; then
        cp "$NATIVE_FILE" "$VERSION_DIR/libtaosnative.so"
        COPIED="$COPIED, libtaosnative.so"
    fi
else
    # Only the native library is available: it is self-contained, publish it as
    # libtaos.so (this is what every already-published green version looks like).
    cp "$NATIVE_FILE" "$VERSION_DIR/libtaos.so"
    COPIED="libtaos.so (from $(basename "$NATIVE_FILE"))"
fi

echo "Done. Files placed in $VERSION_DIR: $COPIED"
ls -lh "$VERSION_DIR"
