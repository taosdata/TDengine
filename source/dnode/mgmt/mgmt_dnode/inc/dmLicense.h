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

#ifndef _TD_DM_LICENSE_H_
#define _TD_DM_LICENSE_H_

#include "os.h"
#include "tglobal.h"
#include "taos_license.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DM_LICENSE_STATE_INIT    = 0,
  DM_LICENSE_STATE_OK,
  DM_LICENSE_STATE_GRACE,
  DM_LICENSE_STATE_EXPIRED,
} EDmLicenseState;

#define DM_LICENSE_GRACE_PERIOD_MS  (14LL * 24LL * 3600LL * 1000LL)  // 2 weeks

typedef struct SDmLicenseCtx {
  EDmLicenseState     state;
  taos_sdk_handle_t  *pSdk;
  int64_t             gracePeriodStartMs;  // 0 = not in grace period
  taos_license_info_t lastLicense;
} SDmLicenseCtx;

// Forward declaration (SDnodeMgmt defined in dmInt.h)
struct SDnodeMgmt;

int32_t dmStartLicenseThread(struct SDnodeMgmt *pMgmt);
void    dmStopLicenseThread(struct SDnodeMgmt *pMgmt);
int32_t dmLicenseLoadOrStartGrace(SDmLicenseCtx *pCtx);
void    dmLicenseCancelGrace(SDmLicenseCtx *pCtx);

#ifdef __cplusplus
}
#endif

#endif /* _TD_DM_LICENSE_H_ */
