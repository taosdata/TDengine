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

extern int8_t  tsS3Enabled;
extern int8_t  tsS3EnabledCfg;
extern int32_t tsS3UploadDelaySec;

int32_t tcsInit();
void    tcsUninit();

int32_t tcsCheckCfg();

int32_t tcsPutObjectFromFileOffset(const char* file, const char* object_name, int64_t offset, int64_t size);
int32_t tcsGetObjectBlock(const char* object_name, int64_t offset, int64_t size, bool check, uint8_t** ppBlock);

void tcsDeleteObjectsByPrefix(const char* prefix);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TCS_H_
