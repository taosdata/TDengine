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

#include "tsdbFile.h"

typedef enum {
  TSDB_FOP_CREATE = -2,  // create a file
  TSDB_FOP_EXTEND,       // extend a file
  TSDB_FOP_NONE,         // no operation
  TSDB_FOP_TRUNCATE,     // truncate a file
  TSDB_FOP_DELETE,       // delete a file
  TSDB_FOP_MAX,
} tsdb_fop_t;

typedef enum {
  TSDB_FTYPE_NONE = 0,  // no file type
  TSDB_FTYPE_STT,       // .stt
  TSDB_FTYPE_HEAD,      // .head
  TSDB_FTYPE_DATA,      // .data
  TSDB_FTYPE_SMA,       // .sma
  TSDB_FTYPE_TOMB,      // .tomb
} tsdb_ftype_t;

const char *tsdb_ftype_suffix[] = {
    "none", "stt", "head", "data", "sma", "tomb",
};

typedef struct SFStt {
  int64_t offset;
} SFStt;

typedef struct SFHead {
  int64_t offset;
} SFHead;

typedef struct SFData {
  // TODO
} SFData;

typedef struct SFSma {
  // TODO
} SFSma;

typedef struct SFTomb {
  // TODO
} SFTomb;

struct STFile {
  SDiskID      diskId;
  int64_t      size;
  int64_t      cid;
  int32_t      fid;
  tsdb_ftype_t type;
  union {
    SFStt  fstt;
    SFHead fhead;
    SFData fdata;
    SFSma  fsma;
    SFTomb ftomb;
  };
};

struct SFileObj {
  volatile int32_t nRef;
  STFile           file;
};

struct SFileOp {
  tsdb_fop_t op;
  union {
    struct {
    } create;
  };
};