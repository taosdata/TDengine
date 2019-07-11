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

#ifndef TDENGINE_VNODETAGMGMT_H
#define TDENGINE_VNODETAGMGMT_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * @version 0.1

 * @date   2018/01/02
 * @author liaohj
 * management of the tag value of tables
 * in query, client need the vnode to aggregate results according to tags
 * values,
 * the grouping operation is done here.
 * Note:
 * 1. we implement a quick sort algorithm, may remove it later.
 */

typedef struct tTagSchema {
  struct SSchema *pSchema;
  int32_t         numOfCols;
  int32_t         colOffset[];
} tTagSchema;

typedef struct tSidSet {
  int32_t            numOfSids;
  int32_t            numOfSubSet;
  SMeterSidExtInfo **pSids;
  int32_t *          starterPos;  // position of each subgroup, generated according to

  tTagSchema *pTagSchema;
  tOrderIdx   orderIdx;
} tSidSet;

typedef int32_t (*__ext_compar_fn_t)(const void *p1, const void *p2, void *param);

tSidSet *tSidSetCreate(struct SMeterSidExtInfo **pMeterSidExtInfo, int32_t numOfMeters, SSchema *pSchema,
                       int32_t numOfTags, int16_t *orderList, int32_t numOfOrderCols);

tTagSchema *tCreateTagSchema(SSchema *pSchema, int32_t numOfTagCols);

int32_t *calculateSubGroup(void **pSids, int32_t numOfMeters, int32_t *numOfSubset, tOrderDescriptor *pOrderDesc,
                           __ext_compar_fn_t compareFn);

void tSidSetDestroy(tSidSet **pSets);

void tSidSetSort(tSidSet *pSets);

int32_t meterSidComparator(const void *s1, const void *s2, void *param);

int32_t doCompare(char *f1, char *f2, int32_t type, int32_t size);

void tQSortEx(void **pMeterSids, size_t size, int32_t start, int32_t end, void *param, __ext_compar_fn_t compareFn);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODETAGMGMT_H
