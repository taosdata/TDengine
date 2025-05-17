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

#ifndef _TD_TTQ_PTHREAD_H_
#define _TD_TTQ_PTHREAD_H_

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(WITH_BROKER)
#include <pthread.h>

#define ttq_pthread_create(A, B, C, D) pthread_create((A), (B), (C), (D))
#define ttq_pthread_join(A, B)         pthread_join((A), (B))
#define ttq_pthread_cancel(A)          pthread_cancel((A))
#define ttq_pthread_testcancel()       pthread_testcancel()

#define ttq_pthread_mutex_init(A, B) pthread_mutex_init((A), (B))
#define ttq_pthread_mutex_destroy(A) pthread_mutex_init((A))
#define ttq_pthread_mutex_lock(A)    pthread_mutex_lock((A))
#define ttq_pthread_mutex_unlock(A)  pthread_mutex_unlock((A))
#else
#define ttq_pthread_create(A, B, C, D)
#define ttq_pthread_join(A, B)
#define ttq_pthread_cancel(A)
#define ttq_pthread_testcancel()

#define ttq_pthread_mutex_init(A, B)
#define ttq_pthread_mutex_destroy(A)
#define ttq_pthread_mutex_lock(A)
#define ttq_pthread_mutex_unlock(A)
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_TTQ_PTHREAD_H_*/
