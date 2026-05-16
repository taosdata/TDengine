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

#ifndef _TD_AZURE_H_
#define _TD_AZURE_H_

#include "os.h"
#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t azBegin();
void    azEnd();
int32_t azCheckCfg();
int32_t azPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size);
int32_t azGetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock);
void    azDeleteObjectsByPrefix(const char *prefix);

int32_t azPutObjectFromFile2(const char *file, const char *object, int8_t withcp);
int32_t azGetObjectsByPrefix(const char *prefix, const char *path);
int32_t azGetObjectToFile(const char *object_name, const char *fileName);
int32_t azDeleteObjects(const char *object_name[], int nobject);

#ifdef __cplusplus
}
#endif

#endif  // _TD_AZURE_H_
