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

#include "tcoding.h"

#include "metaDef.h"

static SMeta *metaNew(const char *path, const SMetaCfg *pMetaCfg, SMemAllocatorFactory *pMAF);
static void   metaFree(SMeta *pMeta);
static int    metaOpenImpl(SMeta *pMeta);
static void   metaCloseImpl(SMeta *pMeta);

SMeta *metaOpen(const char *path, const SMetaCfg *pMetaCfg, SMemAllocatorFactory *pMAF) {
  SMeta *pMeta = NULL;

  // Set default options
  if (pMetaCfg == NULL) {
    pMetaCfg = &defaultMetaOptions;
  }

  // // Validate the options
  // if (metaValidateOptions(pMetaCfg) < 0) {
  //   // TODO: deal with error
  //   return NULL;
  // }

  // Allocate handle
  pMeta = metaNew(path, pMetaCfg, pMAF);
  if (pMeta == NULL) {
    // TODO: handle error
    return NULL;
  }

  // Create META path (TODO)
  taosMkDir(path);

  // Open meta
  if (metaOpenImpl(pMeta) < 0) {
    metaFree(pMeta);
    return NULL;
  }

  return pMeta;
}

void metaClose(SMeta *pMeta) {
  if (pMeta) {
    metaCloseImpl(pMeta);
    metaFree(pMeta);
  }
}

void metaRemove(const char *path) { taosRemoveDir(path); }

/* ------------------------ STATIC METHODS ------------------------ */
static SMeta *metaNew(const char *path, const SMetaCfg *pMetaCfg, SMemAllocatorFactory *pMAF) {
  SMeta *pMeta;
  size_t psize = strlen(path);

  pMeta = (SMeta *)taosMemoryCalloc(1, sizeof(*pMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pMeta->path = strdup(path);
  if (pMeta->path == NULL) {
    metaFree(pMeta);
    return NULL;
  }

  metaOptionsCopy(&(pMeta->options), pMetaCfg);
  pMeta->pmaf = pMAF;

  return pMeta;
};

static void metaFree(SMeta *pMeta) {
  if (pMeta) {
    taosMemoryFreeClear(pMeta->path);
    taosMemoryFree(pMeta);
  }
}

static int metaOpenImpl(SMeta *pMeta) {
  // Open meta cache
  if (metaOpenCache(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  // Open meta db
  if (metaOpenDB(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  // Open meta index
  if (metaOpenIdx(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  // Open meta table uid generator
  if (metaOpenUidGnrt(pMeta) < 0) {
    // TODO: handle error
    metaCloseImpl(pMeta);
    return -1;
  }

  return 0;
}

static void metaCloseImpl(SMeta *pMeta) {
  metaCloseUidGnrt(pMeta);
  metaCloseIdx(pMeta);
  metaCloseDB(pMeta);
  metaCloseCache(pMeta);
}