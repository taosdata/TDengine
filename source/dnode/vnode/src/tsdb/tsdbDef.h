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

#include "tarray2.h"
#include "tsdb.h"

#ifndef _TD_TSDB_DEF_H_
#define _TD_TSDB_DEF_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_ERROR_LOG(vid, lino, code) \
  tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code))

typedef struct SFDataPtr {
  int64_t offset;
  int64_t size;
} SFDataPtr;

extern int32_t tsdbOpenFile(const char *path, STsdb *pTsdb, int32_t flag, STsdbFD **ppFD);
extern void    tsdbCloseFile(STsdbFD **ppFD);
extern int32_t tsdbWriteFile(STsdbFD *pFD, int64_t offset, const uint8_t *pBuf, int64_t size);
extern int32_t tsdbReadFile(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size, int64_t szHint);
extern int32_t tsdbFsyncFile(STsdbFD *pFD);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_DEF_H_*/
