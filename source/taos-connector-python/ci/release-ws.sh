#!/bin/bash
set -ex
ci=$(realpath $(dirname $0))
newv=$1
if [ "$newv" = "" ]; then
  echo "$0 <version>"
  exit 1
fi
cd taos-ws-py
sed -E '3s#version.*#version = "'$newv'"#' Cargo.toml >> Cargo.toml2
mv Cargo.toml2 Cargo.toml

sed -n "1,9p" CHANGELOG.md > CHANGELOG.md2
printf "## v$newv - $(date +%F)\n\n" >> CHANGELOG.md2
$ci/changelog-generate.sh . taos-ws-py >> CHANGELOG.md2
sed "1,9d" CHANGELOG.md >> CHANGELOG.md2
mv CHANGELOG.md2 CHANGELOG.md

# git commit -a -m "release(taos-ws-py): v$newv"
# git push
# git tag taos-ws-py-v$newv
# git push --force origin taos-ws-py-v$newv:taos-ws-py-v$newv
