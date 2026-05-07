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

#ifndef INC_BCKLOG_H_
#define INC_BCKLOG_H_

#ifdef __cplusplus
extern "C" {
#endif

//
// ---------------- define ----------------
//

void logError(const char *format, ...);
void logInfo(const char *format, ...);
void logWarn(const char *format, ...);
void logDebug(const char *format, ...);

#ifdef __cplusplus
}
#endif

#endif  // INC_BCKLOG_H_
