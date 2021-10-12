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

#include "tkv.h"

struct STkvDb {
  // TODO
};
struct STkvOpts {
  // TODO
};
struct STkvCache {
  // TODO
};
struct STkvReadOpts {
  // TODO
};
struct STkvWriteOpts {
  // TODO
};

STkvDb *tkvOpen(const STkvOpts *options, const char *path) {
  // TODO
  return NULL;
}

void tkvClose(STkvDb *db) {
  // TODO
}

void tkvPut(STkvDb *db, STkvWriteOpts *pwopts, char *key, size_t keylen, char *val, size_t vallen) {
  // TODO
}

char *tkvGet(STkvDb *db, STkvReadOpts *propts, char *key, size_t keylen, size_t *vallen) {
  // TODO
  return NULL;
}

STkvOpts *tkvOptionsCreate() {
  // TODO
  return NULL;
}

void tkvOptionsDestroy(STkvOpts *popts) {
  // TODO
}

void tkvOptionsSetCache(STkvOpts *popts, STkvCache *pCache) {
  // TODO
}

STkvReadOpts *tkvReadOptsCreate() {
    // TODO
    return NULL;
}

void tkvReadOptsDestroy(STkvReadOpts *propts) {
  // TODO
}

STkvWriteOpts *tkvWriteOptsCreate() {
    // TODO
    return NULL;
}

void tkvWriteOptsDestroy(STkvWriteOpts *pwopts) {
  // TODO
}