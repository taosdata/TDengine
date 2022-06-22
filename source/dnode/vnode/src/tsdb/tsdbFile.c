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

#include "tsdb.h"

static const char *tsdbFileSuffix[] = {".del", ".cache", ".head", ".data", ".last", ".sma", ""};

// SHeadFile ===============================================
void tsdbHeadFileName(STsdb *pTsdb, SHeadFile *pFile, char fname[]) {
  // snprintf(fname, TSDB_FILENAME_LEN - 1, "%s/v%df%dver%18d.head", );
}

// SDataFile ===============================================
void tsdbDataFileName(STsdb *pTsdb, SDataFile *pFile, char fname[]) {
  // TODO
}

// SLastFile ===============================================
void tsdbLastFileName(STsdb *pTsdb, SLastFile *pFile, char fname[]) {
  // TODO
}

// SSmaFile ===============================================
void tsdbSmaFileName(STsdb *pTsdb, SSmaFile *pFile, char fname[]) {
  // TODO
}

// SDelFile ===============================================
void tsdbDelFileName(STsdb *pTsdb, SDelFile *pFile, char fname[]) {
  // snprintf(fname, TSDB_FILENAME_LEN, "", pTsdb->path);
}