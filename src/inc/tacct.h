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

#ifndef TDENGINE_ACCT_H
#define TDENGINE_ACCT_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  ACCT_GRANT_USER,
  ACCT_GRANT_DB,
  ACCT_GRANT_TABLE
} EAcctGrantType;

int32_t acctInit();
void    acctCleanUp();
int32_t acctCheck(void *pAcct, EAcctGrantType type);

#ifdef __cplusplus
}
#endif

#endif
