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

#ifndef _err_h_
#define _err_h_

#include "../base.h"

typedef struct err_s               err_t;

struct errs_s {
  todbc_buf_t        *cache;

  int                 count;
  err_t              *errs;
};

int  errs_init(errs_t *errs);
void errs_reclaim(errs_t *errs);
void errs_release(errs_t *errs);

#endif // _err_h_

