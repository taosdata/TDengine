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

#ifndef _TD_VND_COS_H_
#define _TD_VND_COS_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

#define S3_BLOCK_CACHE

int32_t s3Init();
int32_t s3Begin();
void    s3End();
int32_t s3CheckCfg();
int32_t s3PutObjectFromFile(const char *file, const char *object);
int32_t s3PutObjectFromFile2(const char *file, const char *object, int8_t withcp);
int32_t s3PutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size);
void    s3DeleteObjectsByPrefix(const char *prefix);
int32_t s3DeleteObjects(const char *object_name[], int nobject);
bool    s3Exists(const char *object_name);
bool    s3Get(const char *object_name, const char *path);
int32_t s3GetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock);
int32_t s3GetObjectsByPrefix(const char *prefix, const char *path);
void    s3EvictCache(const char *path, long object_size);
long    s3Size(const char *object_name);
int32_t s3GetObjectToFile(const char *object_name, const char *fileName);

#define S3_DATA_CHUNK_PAGES (256 * 1024 * 1024)

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_COS_H_*/
