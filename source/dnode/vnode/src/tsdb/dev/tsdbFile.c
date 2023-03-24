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

#include "dev.h"

typedef enum {
  TSDB_FOP_CREATE = -2,  // create a file
  TSDB_FOP_EXTEND,       // extend a file
  TSDB_FOP_NONE,         // no operation
  TSDB_FOP_TRUNCATE,     // truncate a file
  TSDB_FOP_DELETE,       // delete a file
  TSDB_FOP_MAX,
} tsdb_fop_t;

const char *tsdb_ftype_suffix[] = {
    "none", "stt", "head", "data", "sma", "tomb",
};

struct SFileOp {
  tsdb_fop_t op;
  union {
    struct {
    } create;
  };
};