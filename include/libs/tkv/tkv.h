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

#ifndef _TD_TKV_H_
#define _TD_TKV_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tkv_db_s tkv_db_t;

typedef struct {
  /* data */
} tkv_key_t;

typedef struct {
  bool    pinned;
  int64_t ref;  // TODO: use util library
                // TODO: add a RW latch here
  uint64_t offset;
  void *   pObj;
} tkv_obj_t;

typedef int (*tkv_key_comp_fn_t)(const tkv_key_t *, const tkv_key_t *);
typedef void (*tkv_get_key_fn_t)(const tkv_obj_t *, tkv_key_t *);
typedef int (*tkv_obj_encode_fn_t)(void **buf, void *pObj);
typedef void *(*tkv_obj_decode_fn_t)(void *buf, void **pObj);
typedef int (*tkv_obj_comp_fn_t)(const tkv_obj_t *, const tkv_obj_t *);

typedef struct {
  uint64_t            memLimit;
  tkv_get_key_fn_t    getKey;
  tkv_obj_encode_fn_t encode;
  tkv_obj_decode_fn_t decode;
  tkv_obj_comp_fn_t   compare;
} tkv_db_option_t;

tkv_db_t *       tkvOpenDB(char *dir, tkv_db_option_t *);
int              tkvCloseDB(tkv_db_t *);
int              tkvPut(tkv_db_t *, tkv_obj_t *);
int              tkvPutBatch(tkv_db_t *, tkv_obj_t **, int);
const tkv_obj_t *tkvGet(tkv_key_t *);
int              tkvGetBatch(tkv_db_t *, tkv_key_t **, int, tkv_obj_t **);
int              tkvDrop(tkv_db_t *, tkv_key_t *);
int              tkvCommit(tkv_db_t *, void * /*TODO*/);
// TODO: iter function

#ifdef __cplusplus
}
#endif

#endif /*_TD_TKV_H_*/