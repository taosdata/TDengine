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

#include "tcs.h"

#include "os.h"
#include "taoserror.h"
#include "tglobal.h"

#include "az.h"
#include "cos.h"

typedef enum {
  TOS_PROTO_NIL,
  TOS_PROTO_S3,
  TOS_PROTO_ABLOB,
} STosProto;

typedef struct {
  int32_t (*Begin)();
  void (*End)();
  int32_t (*CheckCfg)();

  int32_t (*PutObjectFromFileOffset)(const char* file, const char* object_name, int64_t offset, int64_t size);
  int32_t (*GetObjectBlock)(const char* object_name, int64_t offset, int64_t size, bool check, uint8_t** ppBlock);

  void (*DeleteObjectsByPrefix)(const char* prefix);
} STcs;

STcs tcs;

extern int8_t tsS3Ablob;

int32_t tcsInit() {
  int32_t code = 0;

  STosProto proto = tsS3Ablob ? TOS_PROTO_ABLOB : TOS_PROTO_S3;

  if (TOS_PROTO_S3 == proto) {
    tcs.Begin = s3Init;
    tcs.End = s3End;
    tcs.CheckCfg = s3CheckCfg;

    tcs.PutObjectFromFileOffset = s3PutObjectFromFileOffset;
    tcs.GetObjectBlock = s3GetObjectBlock;

    tcs.DeleteObjectsByPrefix = s3DeleteObjectsByPrefix;
  } else if (TOS_PROTO_ABLOB == proto) {
    tcs.Begin = azBegin;
    tcs.End = azEnd;
    tcs.CheckCfg = azCheckCfg;

    tcs.PutObjectFromFileOffset = azPutObjectFromFileOffset;
    tcs.GetObjectBlock = azGetObjectBlock;

    tcs.DeleteObjectsByPrefix = azDeleteObjectsByPrefix;
  } else {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  code = tcs.Begin();

  return code;
}

void tcsUninit() { tcs.End(); }

int32_t tcsCheckCfg() { return tcs.CheckCfg(); }

int32_t tcsPutObjectFromFileOffset(const char* file, const char* object_name, int64_t offset, int64_t size) {
  return tcs.PutObjectFromFileOffset(file, object_name, offset, size);
}

int32_t tcsGetObjectBlock(const char* object_name, int64_t offset, int64_t size, bool check, uint8_t** ppBlock) {
  return tcs.GetObjectBlock(object_name, offset, size, check, ppBlock);
}

void tcsDeleteObjectsByPrefix(const char* prefix) { return tcs.DeleteObjectsByPrefix(prefix); }
