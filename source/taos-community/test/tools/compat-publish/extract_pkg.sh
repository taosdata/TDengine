#!/bin/bash
# Usage: extract_pkg.sh <package_path> <target_dir>
# Extracts taosd and libtaos.so from a TDengine installation package

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

# Find libtaosnative.so.* (preferred) or libtaos.so.*
SO_FILE=$(find "$INNER_DIR/driver" -name "libtaosnative.so.*" ! -name "*.so" 2>/dev/null | head -1)
if [ -z "$SO_FILE" ]; then
    # Exclude *ws* to skip the WebSocket-based client library (libtaosws.so.*),
    # which is not the native driver we need here.
    SO_FILE=$(find "$INNER_DIR/driver" -name "libtaos.so.*" ! -name "*.so" ! -name "*ws*" 2>/dev/null | head -1)
fi

if [ -z "$SO_FILE" ]; then
    echo "Error: libtaosnative.so.* or libtaos.so.* not found in driver/"
    exit 1
fi

# Extract version from .so filename (e.g. libtaosnative.so.3.4.0.0 -> 3.4.0.0)
SO_BASENAME=$(basename "$SO_FILE")
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
cp "$SO_FILE" "$VERSION_DIR/libtaos.so"

echo "Done. Files placed in $VERSION_DIR:"
ls -lh "$VERSION_DIR"
