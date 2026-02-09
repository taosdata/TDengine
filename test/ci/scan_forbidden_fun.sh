#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "plz use <path>"
    exit 1
fi

SEARCH_PATH=$1

FORBIDDEN_FUNCS=(
    strcpy
    strncpy
    strcat
    sprintf
    vsprintf
    gets
    strtok
    asctime
    localtime
    atoi
    atol
    atof
)

# 黑名单：不检测的文件或目录
BLACKLIST_FILES=(
    "ignore_this.c"
    "legacy_code.c"
)
BLACKLIST_DIRS=(
    "third_party"
    "vendor"
)

# 构造 grep 的 --exclude 和 --exclude-dir 参数
EXCLUDE_ARGS=()
for f in "${BLACKLIST_FILES[@]}"; do
    EXCLUDE_ARGS+=(--exclude="$f")
done
for d in "${BLACKLIST_DIRS[@]}"; do
    EXCLUDE_ARGS+=(--exclude-dir="$d")
done

for FUNC in "${FORBIDDEN_FUNCS[@]}"; do
    grep -rnw --include="*.c" --exclude-dir={test,tests} "${EXCLUDE_ARGS[@]}" "$SEARCH_PATH" -e "$FUNC" | sed "s|$SEARCH_PATH/||g"
done | sort