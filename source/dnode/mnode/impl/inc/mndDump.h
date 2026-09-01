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

#ifndef _TD_MND_DUMP_H_
#define _TD_MND_DUMP_H_

#include "mndDef.h"
#include "sdb.h"
#include "tjson.h"

#ifdef __cplusplus
extern "C" {
#endif

// Exposed (non-static) purely so unit tests can exercise the sdb.json
// import/export field-mapping logic in mndDump.c without booting a full
// mnode; not part of any public API. Keep in sync with mndDump.c's switch
// statements (dumpRecordFields/applyOverlay) if a table type is added.
ESdbType sdbTypeFromName(const char *name);
bool     isOverlayType(ESdbType t);

void overlayDnode(SDnodeObj *p, SJson *f);
void overlayVgroup(SVgObj *p, SJson *f);
void overlayCluster(SClusterObj *p, SJson *f);
void overlayDb(SDbObj *p, SJson *f);
void overlayConfig(SConfigObj *p, SJson *f);
void overlayCompact(SCompactObj *p, SJson *f);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DUMP_H_*/
