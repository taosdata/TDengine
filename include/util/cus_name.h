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

#ifndef _CUS_NAME_H_
#define _CUS_NAME_H_

//
// support OEM
//
#ifndef TD_PRODUCT_NAME
#if defined(TD_ENTERPRISE) || defined(TD_ASTRA)
#define TD_PRODUCT_NAME "TDengine TSDB-Enterprise"
#else
#define TD_PRODUCT_NAME "TDengine TSDB-OSS"
#endif
#endif

#ifndef CUS_NAME
// CUS_NAME should only be defined by the build system, define it here results in
// bugs like TD-37744.
// #define CUS_NAME "TDengine TSDB"
#endif

#ifndef CUS_PROMPT
#define CUS_PROMPT "taos"
#endif

#ifndef CUS_EMAIL
#define CUS_EMAIL "<support@taosdata.com>"
#endif

#endif  // _CUS_NAME_H_
