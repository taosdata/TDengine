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

#ifndef HTTP_GZIP_H
#define HTTP_GZIP_H

#define EHTTP_GZIP_CHUNK_SIZE_DEFAULT      (1024*16)

typedef struct ehttp_gzip_s              ehttp_gzip_t;

typedef struct ehttp_gzip_callbacks_s    ehttp_gzip_callbacks_t;
typedef struct ehttp_gzip_conf_s         ehttp_gzip_conf_t;

struct ehttp_gzip_callbacks_s {
  void (*on_data)(ehttp_gzip_t *gzip, void *arg, const char *buf, int32_t len);
};

struct ehttp_gzip_conf_s {
  int32_t     get_header:2; // 0: not fetching header info
  int32_t     chunk_size;   // 0: fallback to default: EHTTP_GZIP_CHUNK_SIZE_DEFAULT
};

ehttp_gzip_t* ehttp_gzip_create_decompressor(ehttp_gzip_conf_t conf, ehttp_gzip_callbacks_t callbacks, void *arg);
ehttp_gzip_t* ehttp_gzip_create_compressor(ehttp_gzip_conf_t conf, ehttp_gzip_callbacks_t callbacks, void *arg);
void          ehttp_gzip_destroy(ehttp_gzip_t *gzip);

int32_t       ehttp_gzip_write(ehttp_gzip_t *gzip, const char *buf, int32_t len);
int32_t       ehttp_gzip_finish(ehttp_gzip_t *gzip);

#endif // _ehttp_gzip_h_9196791b_ac2a_4d73_9979_f4b41abbc4c0_

