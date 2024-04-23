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

extern int8_t  tsS3StreamEnabled;
extern int8_t  tsS3Enabled;
extern int32_t tsS3BlockSize;
extern int32_t tsS3BlockCacheSize;
extern int32_t tsS3PageCacheSize;
extern int32_t tsS3UploadDelaySec;

int32_t s3Init();
void    s3CleanUp();
int32_t s3PutObjectFromFile(const char *file, const char *object);
int32_t s3PutObjectFromFile2(const char *file, const char *object, int8_t withcp);
void    s3DeleteObjectsByPrefix(const char *prefix);
void    s3DeleteObjects(const char *object_name[], int nobject);
bool    s3Exists(const char *object_name);
bool    s3Get(const char *object_name, const char *path);
int32_t s3GetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock);
int32_t s3GetObjectsByPrefix(const char *prefix, const char *path);
void    s3EvictCache(const char *path, long object_size);
long    s3Size(const char *object_name);
int32_t s3GetObjectToFile(const char *object_name, char *fileName);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_COS_H_*/
