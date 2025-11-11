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

#ifndef _TD_TTQ_MEMORY_H_
#define _TD_TTQ_MEMORY_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

void *ttq_calloc(size_t nmemb, size_t size);
void  ttq_free(void *mem);
void *ttq_malloc(size_t size);
void *ttq_realloc(void *ptr, size_t size);
char *ttq_strdup(const char *s);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TTQ_MEMORY_H_*/
