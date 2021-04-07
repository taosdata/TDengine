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

#ifndef _todbc_buf_h_
#define _todbc_buf_h_

#include <stdint.h>
#include <stddef.h>

// non-thread-safe
typedef struct todbc_buf_s            todbc_buf_t;

todbc_buf_t* todbc_buf_create(void);
void         todbc_buf_free(todbc_buf_t *buf);

void*        todbc_buf_alloc(todbc_buf_t *buf, size_t size);
void*        todbc_buf_calloc(todbc_buf_t *buf, size_t count, size_t size);
void*        todbc_buf_realloc(todbc_buf_t *buf, void *ptr, size_t size);
char*        todbc_buf_strdup(todbc_buf_t *buf, const char *str);
void         todbc_buf_reclaim(todbc_buf_t *buf);

void todbc_buf_test_random(size_t iterates, size_t size, int raw);
void todbc_buf_test(size_t iterates, size_t size, int raw);

#endif // _todbc_buf_h_

