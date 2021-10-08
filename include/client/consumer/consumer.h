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

#ifndef _TD_CONSUMER_H_
#define _TD_CONSUMER_H_

#include "tlist.h"
#include "tarray.h"
#include "hash.h"

#ifdef __cplusplus
extern "C" {
#endif

  //consumer handle
  struct tmq_consumer_t;
  typedef struct tmq_consumer_t tmq_consumer_t;

  //consumer config
  struct tmq_consumer_config_t;
  typedef struct tmq_consumer_config_t tmq_consumer_config_t;

  //response err
  struct tmq_resp_err_t;
  typedef struct tmq_resp_err_t tmq_resp_err_t;

  struct tmq_message_t;
  typedef struct tmq_message_t tmq_message_t;

  struct tmq_col_batch_t;
  typedef struct tmq_col_batch_t tmq_col_batch_t;

  //get content of message
  tmq_col_batch_t* tmq_get_msg_col_by_idx(tmq_message_t*, int32_t col_id);
  tmq_col_batch_t* tmq_get_msg_col_by_name(tmq_message_t*, const char*);

  //consumer config
  int32_t tmq_conf_set(tmq_consumer_config_t* , const char* config_key, const char* config_value, char* errstr, int32_t errstr_cap);

  //consumer initialization
  //resouces are supposed to be free by users by calling tmq_consumer_destroy
  tmq_consumer_t* tmq_consumer_new(tmq_consumer_config_t* , char* errstr, int32_t errstr_cap);

  //subscribe
  tmq_resp_err_t tmq_subscribe(tmq_consumer_t*, const SList*);
  tmq_resp_err_t tmq_unsubscribe(tmq_consumer_t*);

  //consume
  //resouces are supposed to be free by users by calling tmq_message_destroy
  tmq_message_t* tmq_consume_poll(tmq_consumer_t*, int64_t blocking_time);

  //destroy message and free memory
  void tmq_message_destroy(tmq_message_t*);

  //close consumer
  int32_t tmq_consumer_close(tmq_consumer_t*);

  //destroy consumer
  void tmq_consumer_destroy(tmq_message_t*);


#ifdef __cplusplus
}
#endif

#endif /*_TD_CONSUMER_H_*/
