#!/bin/sh
lastv=$(grep -n '## v' CHANGELOG.md |cut -f1 -d:|head -n2 |tail -n1)
thisv=$(grep -n '## v' CHANGELOG.md |cut -f1 -d:|head -n1)

if [ $lastv = $thisv ]; then
  cat CHANGELOG.md | tail -n+$thisv
else
  sed -n "$thisv,$(expr $lastv - 1)p" CHANGELOG.md
fi