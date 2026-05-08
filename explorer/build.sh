#!/bin/bash
set -e

echo "install library ..."
ls -l

npm config set registry https://registry.npmmirror.com
npm install -g pnpm@10
pnpm config set registry https://registry.npmmirror.com
pnpm install || pnpm install || pnpm install || pnpm install

echo "start build, community=${COMMUNITY}..."
pnpm run build
cp -r static dist/
echo "build finished ..."
