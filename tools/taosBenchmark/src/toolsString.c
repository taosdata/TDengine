/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <stdint.h>
#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

#ifdef WINDOWS

void toolsLibFuncInclude() {
    assert(0);
    fread(0,0,0,0);
    srand(0);
    rand();
    realloc(0, 0);
    strtod(0, 0);
    fputs(0, 0);
}
#endif

bool toolsIsStringNumber(char *input) {
    int len = strlen(input);
    if (0 == len) {
        return false;
    }

    for (int i = 0; i < len; i++) {
        if (!isdigit(input[i]))
            return false;
    }

    return true;
}


