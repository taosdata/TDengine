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

#define STREAM_STATE_TRANSFER "stream-state-transfer"

typedef struct SStreamSnapReader SStreamSnapReader;
typedef struct SStreamSnapWriter SStreamSnapWriter;

typedef struct SStreamSnapHandle   SStreamSnapHandle;
typedef struct SStreamSnapBlockHdr SStreamSnapBlockHdr;

int32_t streamSnapReaderOpen(void* pMeta, int64_t sver, int64_t ever, char* path, SStreamSnapReader** ppReader);
int32_t streamSnapReaderClose(SStreamSnapReader* pReader);
int32_t streamSnapRead(SStreamSnapReader* pReader, uint8_t** ppData, int64_t* size);

// SMetaSnapWriter ========================================
int32_t streamSnapWriterOpen(void* pMeta, int64_t sver, int64_t ever, char* path, SStreamSnapWriter** ppWriter);
int32_t streamSnapWrite(SStreamSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t streamSnapWriterClose(SStreamSnapWriter* ppWriter, int8_t rollback);

#endif