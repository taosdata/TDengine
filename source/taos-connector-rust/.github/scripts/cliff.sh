#!/bin/bash

set -e
git cliff --tag $(toml get ./Cargo.toml workspace.package.version | sed 's/"//g' | rg '\d+\.\d+\.\d+') -o CHANGELOG.md
git add CHANGELOG.md
git commit -m "chore: add changelog for pre release"
