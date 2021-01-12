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

#define _DEFAULT_SOURCE
#include "os.h"
#include "zlib.h"
#include "httpGzip.h"

typedef enum {
  EHTTP_GZIP_INITING,
  EHTTP_GZIP_READY,
  EHTTP_GZIP_CLOSED,
} EHTTP_GZIP_STATE;

struct ehttp_gzip_s {
  ehttp_gzip_conf_t       conf;
  ehttp_gzip_callbacks_t  callbacks;
  void                   *arg;
  z_stream               *gzip;
  gz_header              *header;
  char                   *chunk;

  int32_t                 state;
};

static void dummy_on_data(ehttp_gzip_t *gzip, void *arg, const char *buf, int32_t len) {
}

static void ehttp_gzip_cleanup(ehttp_gzip_t *gzip) {
  switch(gzip->state) {
    case EHTTP_GZIP_READY: {
      inflateEnd(gzip->gzip);
    } break;
    default: break;
  }
  if (gzip->gzip) {
    free(gzip->gzip);
    gzip->gzip = NULL;
  }
  if (gzip->header) {
    free(gzip->header);
    gzip->header = NULL;
  }
  if (gzip->chunk) {
    free(gzip->chunk);
    gzip->chunk = NULL;
  }
  gzip->state = EHTTP_GZIP_CLOSED;
}

ehttp_gzip_t* ehttp_gzip_create_decompressor(ehttp_gzip_conf_t conf, ehttp_gzip_callbacks_t callbacks, void *arg) {
  ehttp_gzip_t *gzip = (ehttp_gzip_t*)calloc(1, sizeof(*gzip));
  if (!gzip) return NULL;

  do {
    gzip->conf                = conf;
    gzip->callbacks           = callbacks;
    gzip->arg                 = arg;
    if (gzip->callbacks.on_data == NULL) gzip->callbacks.on_data = dummy_on_data;
    gzip->gzip                = (z_stream*)calloc(1, sizeof(*gzip->gzip));
    if (gzip->conf.get_header) {
      gzip->header            = (gz_header*)calloc(1, sizeof(*gzip->header));
    }
    if (gzip->conf.chunk_size<=0) gzip->conf.chunk_size = EHTTP_GZIP_CHUNK_SIZE_DEFAULT;
    gzip->chunk                 = (char*)malloc(gzip->conf.chunk_size);
    if (!gzip->gzip || (gzip->conf.get_header && !gzip->header) || !gzip->chunk) break;
    gzip->gzip->zalloc      = Z_NULL;
    gzip->gzip->zfree       = Z_NULL;
    gzip->gzip->opaque      = Z_NULL;

  // 863      windowBits can also be greater than 15 for optional gzip decoding.  Add
  // 864    32 to windowBits to enable zlib and gzip decoding with automatic header
  // 865    detection, or add 16 to decode only the gzip format (the zlib format will
  // 866    return a Z_DATA_ERROR).  If a gzip stream is being decoded, strm->adler is a
  // 867    CRC-32 instead of an Adler-32.  Unlike the gunzip utility and gzread() (see
  // 868    below), inflate() will not automatically decode concatenated gzip streams.
  // 869    inflate() will return Z_STREAM_END at the end of the gzip stream.  The state
  // 870    would need to be reset to continue decoding a subsequent gzip stream.
    int32_t ret = inflateInit2(gzip->gzip, 32); // 32/16? 32/16 + MAX_WBITS
    if (ret != Z_OK) break;
    if (gzip->header) {
      ret = inflateGetHeader(gzip->gzip, gzip->header);
    }
    if (ret != Z_OK) break;

    gzip->gzip->next_out           = (z_const Bytef*)gzip->chunk;
    gzip->gzip->avail_out          = gzip->conf.chunk_size;
    gzip->state = EHTTP_GZIP_READY;
    return gzip;
  } while (0);

  ehttp_gzip_destroy(gzip);
  return NULL;
}

ehttp_gzip_t* ehttp_gzip_create_compressor(ehttp_gzip_conf_t conf, ehttp_gzip_callbacks_t callbacks, void *arg);

void ehttp_gzip_destroy(ehttp_gzip_t *gzip) {
  ehttp_gzip_cleanup(gzip);

  free(gzip);
}

int32_t ehttp_gzip_write(ehttp_gzip_t *gzip, const char *buf, int32_t len) {
  if (gzip->state != EHTTP_GZIP_READY) return -1;
  if (len <= 0) return 0;

  gzip->gzip->next_in        = (z_const Bytef*)buf;
  gzip->gzip->avail_in       = len;

  while (gzip->gzip->avail_in) {
    int32_t ret;
    if (gzip->header) {
      ret = inflate(gzip->gzip, Z_BLOCK);
    } else {
      ret = inflate(gzip->gzip, Z_SYNC_FLUSH);
    }
    if (ret != Z_OK && ret != Z_STREAM_END) return -1;

    if (gzip->gzip->avail_out>0) {
      if (ret!=Z_STREAM_END) continue;
    }

    int32_t len = (int32_t)(gzip->gzip->next_out - (z_const Bytef*)gzip->chunk);

    gzip->gzip->next_out[0] = '\0';
    gzip->callbacks.on_data(gzip, gzip->arg, gzip->chunk, len);
    gzip->gzip->next_out       = (z_const Bytef*)gzip->chunk;
    gzip->gzip->avail_out      = gzip->conf.chunk_size;
  }

  return 0;
}

int32_t ehttp_gzip_finish(ehttp_gzip_t *gzip) {
  if (gzip->state != EHTTP_GZIP_READY) return -1;

  gzip->gzip->next_in        = NULL;
  gzip->gzip->avail_in       = 0;

  int32_t ret;
  ret = inflate(gzip->gzip, Z_FINISH);

  if (ret != Z_STREAM_END) return -1;

  int32_t len = (int32_t)(gzip->gzip->next_out - (z_const Bytef*)gzip->chunk);

  gzip->gzip->next_out[0] = '\0';
  gzip->callbacks.on_data(gzip, gzip->arg, gzip->chunk, len);
  gzip->gzip->next_out       = NULL;
  gzip->gzip->avail_out      = 0;

  return 0;
}

