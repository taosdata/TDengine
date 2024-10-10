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

//#include "az.h"
#include "cos.h"

extern int8_t tsS3Ablob;

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

  int32_t (*PutObjectFromFile2)(const char* file, const char* object, int8_t withcp);
  int32_t (*GetObjectsByPrefix)(const char* prefix, const char* path);
  int32_t (*DeleteObjects)(const char* object_name[], int nobject);
  int32_t (*GetObjectToFile)(const char* object_name, const char* fileName);
} STcs;

static STcs tcs;

int32_t tcsInit() {
  int32_t code = 0;

  STosProto proto = tsS3Ablob ? TOS_PROTO_ABLOB : TOS_PROTO_S3;

  if (TOS_PROTO_S3 == proto) {
    tcs.Begin = s3Begin;
    tcs.End = s3End;
    tcs.CheckCfg = s3CheckCfg;

    tcs.PutObjectFromFileOffset = s3PutObjectFromFileOffset;
    tcs.GetObjectBlock = s3GetObjectBlock;

    tcs.DeleteObjectsByPrefix = s3DeleteObjectsByPrefix;

    tcs.PutObjectFromFile2 = s3PutObjectFromFile2;
    tcs.GetObjectsByPrefix = s3GetObjectsByPrefix;
    tcs.DeleteObjects = s3DeleteObjects;
    tcs.GetObjectToFile = s3GetObjectToFile;
  } else if (TOS_PROTO_ABLOB == proto) {
    /*
    tcs.Begin = azBegin;
    tcs.End = azEnd;
    tcs.CheckCfg = azCheckCfg;

    tcs.PutObjectFromFileOffset = azPutObjectFromFileOffset;
    tcs.GetObjectBlock = azGetObjectBlock;

    tcs.DeleteObjectsByPrefix = azDeleteObjectsByPrefix;

    tcs.PutObjectFromFile2 = azPutObjectFromFile2;
    tcs.GetObjectsByPrefix = azGetObjectsByPrefix;
    tcs.DeleteObjects = azDeleteObjects;
    tcs.GetObjectToFile = azGetObjectToFile;
    */
  } else {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  code = tcs.Begin();

  return code;
}

void tcsUninit() { tcs.End(); }

int32_t tcsCheckCfg() {
  int32_t code = 0;

  if (!tsS3Enabled) {
    (void)fprintf(stderr, "s3 not configured.\n");
    TAOS_RETURN(code);
  }

  code = tcsInit();
  if (code != 0) {
    (void)fprintf(stderr, "failed to initialize s3.\n");
    TAOS_RETURN(code);
  }

  code = s3Begin();
  if (code != 0) {
    (void)fprintf(stderr, "failed to begin s3.\n");
    TAOS_RETURN(code);
  }

  code = tcs.CheckCfg();
  if (code != 0) {
    (void)fprintf(stderr, "failed to check s3.\n");
    TAOS_RETURN(code);
  }

  tcsUninit();

  return code;
}

int32_t tcsPutObjectFromFileOffset(const char* file, const char* object_name, int64_t offset, int64_t size) {
  return tcs.PutObjectFromFileOffset(file, object_name, offset, size);
}

int32_t tcsGetObjectBlock(const char* object_name, int64_t offset, int64_t size, bool check, uint8_t** ppBlock) {
  return tcs.GetObjectBlock(object_name, offset, size, check, ppBlock);
}

void tcsDeleteObjectsByPrefix(const char* prefix) { return tcs.DeleteObjectsByPrefix(prefix); }

int32_t tcsPutObjectFromFile2(const char* file, const char* object, int8_t withcp) {
  return tcs.PutObjectFromFile2(file, object, withcp);
}

int32_t tcsGetObjectsByPrefix(const char* prefix, const char* path) { return tcs.GetObjectsByPrefix(prefix, path); }

int32_t tcsDeleteObjects(const char* object_name[], int nobject) { return tcs.DeleteObjects(object_name, nobject); }

int32_t tcsGetObjectToFile(const char* object_name, const char* fileName) {
  return tcs.GetObjectToFile(object_name, fileName);
}
