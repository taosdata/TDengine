/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _DEFAULT_SOURCE
#include "trnInt.h"

int32_t trnInit() { return 0; }
void    trnCleanup();

STrans *trnCreate() { return NULL; }
int32_t trnCommit(STrans *pTrans) { return 0; }
void    trnDrop(STrans *pTrans) {}

void trnAppendRedoLog(STrans *pTrans, SSdbRawData *pRaw) {}
void trnAppendUndoLog(STrans *pTrans, SSdbRawData *pRaw) {}
void trnAppendCommitLog(STrans *pTrans, SSdbRawData *pRaw) {}
void trnAppendRedoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {}
void trnAppendUndoAction(STrans *pTrans, SEpSet *pEpSet, void *pMsg) {}
