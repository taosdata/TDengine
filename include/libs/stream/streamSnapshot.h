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
#ifndef _STREAM_BACKEDN_SNAPSHOT_H_
#define _STREAM_BACKEDN_SNAPSHOT_H_
#include "tcommon.h"

typedef struct SStreamSnapReader SStreamSnapReader;
typedef struct StreamSnapWriter  StreamSnapWriter;

typedef struct SStreamSnapHandle SStreamSnapHandle;

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t ever, void** ppReader);
int32_t streamSnapReaderClose(void** ppReader);
int32_t streamSnapRead(void* pReader, uint8_t** ppData);

// SMetaSnapWriter ========================================
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, void** ppWriter);
int32_t streamSnapWrite(void* pWriter, uint8_t* pData, uint32_t nData);
int32_t streamSnapWriterClose(void** ppWriter, int8_t rollback);

#endif