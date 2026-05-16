#!/bin/bash
ci=$(realpath $(dirname $0))
newv=$1
if [ "$newv" = "" ]; then
  echo "$0 <version>"
  exit 1
fi
echo "__version__ = '$newv'" > taos/_version.py
sed -E '3s#version.*#version = "'$newv'"#' pyproject.toml >> pyproject.toml2
mv pyproject.toml2 pyproject.toml

sed -n "1,9p" CHANGELOG.md > CHANGELOG.md2
printf "## v$newv - $(date +%F)\n\n" >> CHANGELOG.md2
$ci/changelog-generate.sh >> CHANGELOG.md2
sed "1,9d" CHANGELOG.md >> CHANGELOG.md2
mv CHANGELOG.md2 CHANGELOG.md

# git commit -a -m "release: v$newv"
# git push
# git tag v$newv
# git push --force origin v$newv:v$newv
# bash build_doc.sh
