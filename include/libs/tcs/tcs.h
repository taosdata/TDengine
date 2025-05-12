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

#ifndef _TD_TCS_H_
#define _TD_TCS_H_

#include "os.h"
#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int8_t tsS3Enabled;
extern int8_t tsS3EnabledCfg;

extern int32_t tsS3UploadDelaySec;
extern int32_t tsS3BlockSize;
extern int32_t tsS3BlockCacheSize;
extern int32_t tsS3PageCacheSize;

extern int8_t tsS3StreamEnabled;

int32_t tcsInit();
void    tcsUninit();

int32_t tcsCheckCfg();

int32_t tcsPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size);
int32_t tcsGetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock);

void tcsDeleteObjectsByPrefix(const char *prefix);

int32_t tcsPutObjectFromFile2(const char *file, const char *object, int8_t withcp);
int32_t tcsGetObjectsByPrefix(const char *prefix, const char *path);
int32_t tcsDeleteObjects(const char *object_name[], int nobject);
int32_t tcsGetObjectToFile(const char *object_name, const char *fileName);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TCS_H_
