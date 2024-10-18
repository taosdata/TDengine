#!/bin/bash

last=$(git describe --tags --abbrev=0 2>/dev/null)

if [ "$last" = "" ]; then
  git log --pretty=format:'%s' | sort -k2n | uniq >./releaseNotes.tmp
else
  git log --pretty=format:'%s' $last..HEAD | sort -k2n | uniq >./releaseNotes.tmp
fi

function part() {
  name=$1
  pattern=$2
  changes=$(grep -P '\[\w+-\d+\]\s*<('$pattern')>:' ./releaseNotes.tmp | sed -E 's/ *<('$pattern')>//' | sed 's/[ci skip]\s*//' | awk -F: '{print "- " $1 ": " $2}' | sort | uniq)
  lines=$(printf "\\$changes\n" | wc -l)
  # echo $name $pattern $lines >&2
  if [ $lines -gt 0 ]; then
    echo "### $name"
    echo ""
    echo "$changes"
    echo ""
  fi
}

part "Features" "feature|feat"
part "Bug Fixes" "bugfix|fix"
part "Enhancements" "enhance"
part "Tests" "test"
part "Documents" "docs|doc"

rm -f ./releaseNotes.tmp
