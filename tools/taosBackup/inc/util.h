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

#ifndef INC_UTIL_H_
#define INC_UTIL_H_


//
// ---------------- define ----------------
//

// ---------------- interface ----------------
unsigned int getCrc(const char *name);

void sleepMs(int ms);

void freeArrayPtr(char **ptr);

bool errorCodeCanRetry(int code);

#endif  // INC_UTIL_H_
