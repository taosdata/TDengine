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

#include "vnd.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int8_t tsS3Enabled;

int32_t s3Init();
void    s3CleanUp();
int32_t s3PutObjectFromFile(const char *file, const char *object);
void    s3DeleteObjectsByPrefix(const char *prefix);
void    s3DeleteObjects(const char *object_name[], int nobject);
bool    s3Exists(const char *object_name);
bool    s3Get(const char *object_name, const char *path);
void    s3EvictCache(const char *path, long object_size);
long    s3Size(const char *object_name);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_COS_H_*/
