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

#ifndef TAOS_QUEUE_H
#define TAOS_QUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void* taos_queue;
typedef void* taos_qset;
typedef void* taos_qall;

taos_queue taosOpenQueue(int itemSize);
void       taosCloseQueue(taos_queue);
int        taosWriteQitem(taos_queue, void *item);
int        taosReadQitem(taos_queue, void *item);

int        taosReadAllQitems(taos_queue, taos_qall *);
int        taosGetQitem(taos_qall, void *item);
void       taosResetQitems(taos_qall);
void       taosFreeQitems(taos_qall);

taos_qset  taosOpenQset();
void       taosCloseQset();
int        taosAddIntoQset(taos_qset, taos_queue);
void       taosRemoveFromQset(taos_qset, taos_queue);
int        taosGetQueueNumber(taos_qset);

int        taosReadQitemFromQset(taos_qset, void *item);
int        taosReadAllQitemsFromQset(taos_qset, taos_qall *);

int        taosGetQueueItemsNumber(taos_queue param);
int        taosGetQsetItemsNumber(taos_qset param);

#ifdef __cplusplus
}
#endif

#endif


