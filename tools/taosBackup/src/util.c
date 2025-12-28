/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
    
#include "util.h"
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include "taoserror.h"

void sleepMs(int ms) {
    usleep(ms * 1000);
}

void freeArrayPtr(char **ptr) {
    if (ptr == NULL) {
        return;
    }

    for (int i = 0; ptr[i] != NULL; i++) {
        free(ptr[i]);
    }
    free(ptr);
}

bool errorCodeCanRetry(int code) {
    if (code == TSDB_CODE_RPC_NETWORK_ERROR ||
        code == TSDB_CODE_RPC_NETWORK_BUSY ||
        code == TSDB_CODE_RPC_TIMEOUT) {
        return true;
    }

    return false;
}

unsigned int getCrc(const char *name) {
    unsigned int crc = 0xFFFFFFFF;
    while (*name) {
        crc ^= (unsigned char)(*name++);
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    return ~crc;
}
