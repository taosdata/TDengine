#!/bin/bash

CLANG_FORMAT=${CLANG_FORMAT:-clang-format}
CLANG_FORMAT_VERSION=${CLANG_FORMAT_VERSION:-}

if ! type $CLANG_FORMAT >/dev/null || \
        ! $CLANG_FORMAT --version | grep -q "version ${CLANG_FORMAT_VERSION}"; then
    # If running tests, mark this test as skipped.
    exit 77
fi

errors=0
paths=$(git ls-files | grep '\.[ch]$')
for path in $paths; do
    in=$(cat $path)
    out=$($CLANG_FORMAT $path)

    if [ "$in" != "$out" ]; then
        diff -u -L $path -L "$path.formatted" $path - <<<$out
        errors=1
    fi
done

if [ $errors -ne 0 ]; then
    echo "Formatting errors detected, run ./scripts/clang-format to fix!"
    exit 1
fi
