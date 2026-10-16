/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "bench.h"
#include "benchLog.h"


//
// -----------  log -----------
//


// TdThreadMutex/TdThreadMutexAttr and the taosThreadMutex* helpers are provided
// by the os library (see osThread.h / osThread.c, pulled in via toolsdef.h).
// Do NOT re-implement them here: local copies duplicated os.lib symbols and
// broke the Windows Release link with "fatal error LNK1179: duplicate COMDAT"
// at the LTCG (/GL) stage.

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
        if (taosThreadMutexDestroy(&mutexs[i]) != 0) {
            fprintf(stderr, "Failed to destroy mutex %d, error: %s\n", i, strerror(errno));
        }
    }
}

// lock
void lockLog(int8_t idx) {
    if (taosThreadMutexLock(&mutexs[idx]) != 0) {
        fprintf(stderr, "FATAL: Failed to lock mutex %d, error: %s. Aborting.\n", idx, strerror(errno));
        abort();
    }
}
// unlock
void unlockLog(int8_t idx) {
    if (taosThreadMutexUnlock(&mutexs[idx]) != 0) {
        fprintf(stderr, "FATAL: Failed to unlock mutex %d, error: %s. Aborting.\n", idx, strerror(errno));
        abort();
    }
}