#!/bin/bash
set -e

PNPM_VERSION=10.33.4

echo "install library ..."
ls -l

npm config set registry https://registry.npmmirror.com
npm install -g pnpm@${PNPM_VERSION}
pnpm config set registry https://registry.npmmirror.com
pnpm install || pnpm install || pnpm install || pnpm install

echo "start build, community=${COMMUNITY}..."
pnpm run build
cp -r static dist/
echo "build finished ..."
