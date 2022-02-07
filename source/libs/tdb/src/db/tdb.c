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

#include "tdbInt.h"

// static int tdbOpenImpl(TDB *dbp);

// int tdbOpen(TDB **dbpp, const char *fname, const char *dbname, uint32_t flags) {
//   TDB *       dbp;
//   TDB_MPFILE *mpf;
//   uint8_t     fileid[TDB_FILE_ID_LEN];

//   if ((dbp = (TDB *)calloc(1, sizeof(*dbp))) == NULL) {
//     return -1;
//   }

//   if ((dbp->fname = strdup(fname)) == NULL) {
//     free(dbp);
//     return -1;
//   }

//   if ((dbname) && ((dbp->dbname = strdup(dbname)) == NULL)) {
//     free(dbp->fname);
//     free(dbp);
//     return -1;
//   }

//   // if (tdbGnrtFileID(fname, fileid) < 0) {
//   //   // todo
//   //   return -1;
//   // }

//   // TODO: mpf = tdbGetMPFileByID(fileid);
//   if (mpf == NULL) {
//     // todoerr: maybe we need to create one
//     return -1;
//   }

//   if (tdbOpenImpl(dbp) < 0) {
//     // todoerr
//     return -1;
//   }

//   *dbpp = dbp;
//   return 0;
// }

// int tdbClose(TDB *dbp, uint32_t flags) {
//   // TODO
//   return 0;
// }

// static int tdbOpenImpl(TDB *dbp) {
//   if (dbp->dbname == NULL) {
//     // todo: open the DB as a master DB
//   } else {
//     // todo: open the DB as a sub-db
//   }
//   // TODO
//   return 0;
// }