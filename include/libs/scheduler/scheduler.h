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

#ifndef _TD_SCHEDULER_H_
#define _TD_SCHEDULER_H_

#ifdef __cplusplus
extern "C" {
#endif

struct SQueryJob;

/**
 * Process the query job, generated according to the query physical plan.
 * This is a synchronized API, and is also thread-safety.
 * @param pJob
 * @return
 */
int32_t qProcessQueryJob(struct SQueryJob* pJob);

/**
 * The SSqlObj should not be here????
 * @param pSql
 * @param pVgroupId
 * @param pRetVgroupId
 * @return
 */
//SArray* qGetInvolvedVgroupIdList(struct SSqlObj* pSql, SArray* pVgroupId, SArray* pRetVgroupId);

/**
 * Cancel query job
 * @param pJob
 * @return
 */
int32_t qKillQueryJob(struct SQueryJob* pJob);

#ifdef __cplusplus
}
#endif

#endif /*_TD_SCHEDULER_H_*/