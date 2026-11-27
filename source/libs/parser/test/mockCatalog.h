/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef MOCK_CATALOG_H
#define MOCK_CATALOG_H

#include "mockCatalogService.h"

void initMetaDataEnv();
void generateMetaData();
void destroyMetaDataEnv();

// Override what the stubbed mnode round trip behind checkColRefByPriv() answers for
// RENAME COLUMN / RENAME TAG on a normal / virtual-normal table. Defaults to
// TSDB_CODE_SUCCESS ("no privilege references this column"); pass an error code to make
// the RESTRICT check reject, and reset it to TSDB_CODE_SUCCESS afterwards since the
// state is process-global.
void mockChkColRefByPrivCode(int32_t code);

#endif  // MOCK_CATALOG_H
