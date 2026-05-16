#!/bin/bash
ci=$(realpath $(dirname $0))
newv=$1
if [ "$newv" = "" ]; then
  echo "$0 <version>"
  exit 1
fi
sed -Ei 's#"version":\s*.*$#"version": "'$newv'",#' package.json src/plugin.json
sed -Ei 's#"updated":\s*.*$#"updated": "'`date +%F`'"#' src/plugin.json

# command -v git-cliff > /dev/null || \
#   (wget -c https://github.com/orhun/git-cliff/releases/download/v0.7.0/git-cliff-0.7.0-x86_64-unknown-linux-musl.tar.gz && \
#     tar xvf git-cliff-0.7.0-x86_64-unknown-linux-musl.tar.gz && \
#     install git-cliff-0.7.0/git-cliff /usr/bin/git-cliff)

# git cliff -t v$newv |tee CHANGELOG.md > /dev/null

git config user.email || git config user.email "github-actions@github.com"
git config user.name || git config user.name "GitHub Actions [bot]"

git commit -a -m "chore(release): bump v$newv"
git tag v$newv
