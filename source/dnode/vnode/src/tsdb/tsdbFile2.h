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

#include "tsdbDef.h"

#ifndef _TSDB_FILE_H
#define _TSDB_FILE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STFile    STFile;
typedef struct STFileObj STFileObj;

typedef enum {
  TSDB_FTYPE_HEAD = 0,                   // .head
  TSDB_FTYPE_DATA,                       // .data
  TSDB_FTYPE_SMA,                        // .sma
  TSDB_FTYPE_TOMB,                       // .tomb
  TSDB_FTYPE_STT = TSDB_FTYPE_TOMB + 2,  // .stt
} tsdb_ftype_t;

enum {
  TSDB_FSTATE_LIVE = 1,
  TSDB_FSTATE_DEAD,
};

#define TSDB_FTYPE_MIN TSDB_FTYPE_HEAD
#define TSDB_FTYPE_MAX (TSDB_FTYPE_TOMB + 1)

// STFile
int32_t tsdbTFileToJson(const STFile *f, cJSON *json);
int32_t tsdbJsonToTFile(const cJSON *json, tsdb_ftype_t ftype, STFile *f);
int32_t tsdbTFileName(STsdb *pTsdb, const STFile *f, char fname[]);
bool    tsdbIsSameTFile(const STFile *f1, const STFile *f2);
bool    tsdbIsTFileChanged(const STFile *f1, const STFile *f2);

// STFileObj
int32_t tsdbTFileObjInit(STsdb *pTsdb, const STFile *f, STFileObj **fobj);
int32_t tsdbTFileObjRef(STFileObj *fobj);
int32_t tsdbTFileObjUnref(STFileObj *fobj);
int32_t tsdbTFileObjRemove(STFileObj *fobj);
int32_t tsdbTFileObjCmpr(const STFileObj **fobj1, const STFileObj **fobj2);

struct STFile {
  tsdb_ftype_t type;
  SDiskID      did;  // disk id
  int32_t      s3flag;
  int32_t      fid;  // file id
  int64_t      cid;  // commit id
  int64_t      size;
  int64_t      minVer;
  int64_t      maxVer;
  union {
    struct {
      int32_t level;
    } stt[1];
  };
};

struct STFileObj {
  TdThreadMutex mutex;
  STFile        f[1];
  int32_t       state;
  int32_t       ref;
  int32_t       nlevel;
  char          fname[TSDB_FILENAME_LEN];
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_FILE_H*/
