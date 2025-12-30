/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#ifndef _TD_MND_SCAN_H_
#define _TD_MND_SCAN_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitScan(SMnode *pMnode);
void    mndCleanupScan(SMnode *pMnode);

int32_t mndProcessScanDbReq(SRpcMsg *pReq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SCAN_H_*/