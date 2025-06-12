/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "../../inc/pub.h"
#include "bench.h"
#include "benchLog.h"


//
// -----------  log -----------
//

//  ------- interface api ---------
TdThreadMutex mutexs[LOG_COUNT];

// init log
bool initLog() {
    for (int32_t i = 0; i < LOG_COUNT; i++) {
        if (taosThreadMutexInit(&mutexs[i], NULL) != 0) {
            printf("taosThreadMutexInit i=%d failed.\n", i);
        }
    }
    return true;
}

// exit log
void exitLog() {
    for (int32_t i = 0; i < LOG_COUNT; i++) {
        taosThreadMutexDestroy(&mutexs[i]);
    }
}

// lock
void lockLog(int8_t idx) {
    taosThreadMutexLock(&mutexs[idx]);
}
// unlock
void unlockLog(int8_t idx) {
    taosThreadMutexUnlock(&mutexs[idx]);
}