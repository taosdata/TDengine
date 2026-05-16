#!/bin/bash
set -ex
rm -rf dist
old=$(git rev-parse --abbrev-ref HEAD)
poetry publish --build -u "${PYPI_USER}" -p "${PYPI_PASS}" || true
version=$(ls -rt dist/*.tar.gz| tail -n1|sed -E 's/.*taospy-//;s/.tar.gz$//')
git checkout release
tar --strip-components=1 -xvf dist/*.tar.gz
tar -tf dist/*.tar.gz |sed -E 's#[^/]+/##'|xargs -i git add '{}' -f
git commit -m "release: $version"
git checkout $old
