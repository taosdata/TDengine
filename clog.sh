#!/bin/bash
clog --setversion v$(toml get Cargo.toml package.version |sed 's/"//g' | rg '\d+\.\d+\.\d+') \
  && git add CHANGELOG.md && git commit -m "chore: add changelog for pre release"
