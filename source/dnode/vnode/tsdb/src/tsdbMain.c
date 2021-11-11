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

static STsdb *tsdbNew(const char *path, const STsdbOptions *pTsdbOptions);
static void   tsdbFree(STsdb *pTsdb);
static int    tsdbOpenImpl(STsdb *pTsdb);
static void   tsdbCloseImpl(STsdb *pTsdb);

STsdb *tsdbOpen(const char *path, const STsdbOptions *pTsdbOptions) {
  STsdb *pTsdb = NULL;

  // Set default TSDB Options
  if (pTsdbOptions == NULL) {
    pTsdbOptions = &defautlTsdbOptions;
  }

  // Validate the options
  if (tsdbValidateOptions(pTsdbOptions) < 0) {
    // TODO: handle error
    return NULL;
  }

  // Create the handle
  pTsdb = tsdbNew(path, pTsdbOptions);
  if (pTsdb == NULL) {
    // TODO: handle error
    return NULL;
  }

  taosMkDir(path);

  // Open the TSDB
  if (tsdbOpenImpl(pTsdb) < 0) {
    // TODO: handle error
    return NULL;
  }

  return pTsdb;
}

void tsdbClose(STsdb *pTsdb) {
  if (pTsdb) {
    tsdbCloseImpl(pTsdb);
    tsdbFree(pTsdb);
  }
}

void tsdbRemove(const char *path) { taosRemoveDir(path); }

/* ------------------------ STATIC METHODS ------------------------ */
static STsdb *tsdbNew(const char *path, const STsdbOptions *pTsdbOptions) {
  STsdb *pTsdb = NULL;

  pTsdb = (STsdb *)calloc(1, sizeof(STsdb));
  if (pTsdb == NULL) {
    // TODO: handle error
    return NULL;
  }

  pTsdb->path = strdup(path);
  tsdbOptionsCopy(&(pTsdb->options), pTsdbOptions);

  return pTsdb;
}

static void tsdbFree(STsdb *pTsdb) {
  if (pTsdb) {
    tfree(pTsdb->path);
    free(pTsdb);
  }
}

static int tsdbOpenImpl(STsdb *pTsdb) {
  // TODO
  return 0;
}

static void tsdbCloseImpl(STsdb *pTsdb) {
  // TODO
}