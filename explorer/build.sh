#!/bin/bash
set -e

echo "install library ..."
cd /app
ls -l
npm install -g pnpm
pnpm self-update
pnpm install

echo "start build ..."
pnpm run build
cp -r static dist/
echo "build finished ..."
