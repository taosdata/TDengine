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

#include "tos.h"

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

STos tos;

extern int8_t tsS3Ablob;

int32_t tosInit() {
  int32_t code = 0;

  STosProto proto = tsS3Ablob ? TOS_PROTO_ABLOB : TOS_PROTO_S3;

  if (TOS_PROTO_S3 == proto) {
    tos.Begin = s3Init;
    tos.End = s3End;
    tos.CheckCfg = s3CheckCfg;

    tos.PutObjectFromFileOffset = s3PutObjectFromFileOffset;
    tos.GetObjectBlock = s3GetObjectBlock;

    tos.DeleteObjectsByPrefix = s3DeleteObjectsByPrefix;
  } else if (TOS_PROTO_ABLOB == proto) {
    tos.Begin = azBegin;
    tos.End = azEnd;
    tos.CheckCfg = azCheckCfg;

    tos.PutObjectFromFileOffset = azPutObjectFromFileOffset;
    tos.GetObjectBlock = azGetObjectBlock;

    tos.DeleteObjectsByPrefix = azDeleteObjectsByPrefix;
  } else {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  code = tos.Begin();

  return code;
}

void tosUninit() { tos.End(); }
