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

#ifndef _env_h_
#define _env_h_

#include "../base.h"

#include "err.h"

struct env_s {
  int64_t                 refcount;

  char                    enc_charset[64];  // default charset from system locale
  int32_t                 odbc_ver;         // default SQL_OV_ODBC3

  errs_t                  errs;

  void (*env_free)(env_t* env);
};

int env_init(env_t *env);

env_t* env_create(void);




#endif // _env_h_


