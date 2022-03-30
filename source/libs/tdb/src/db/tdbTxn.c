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

#include "tdbInt.h"

// int tdbTxnBegin(TENV *pEnv) {
//   // TODO
//   return 0;
// }

// int tdbTxnCommit(TENV *pEnv) {
//   SPager *pPager = NULL;
//   int     ret;

//   for (;;) {
//     break;
//     ret = tdbPagerCommit(pPager);
//     if (ret < 0) {
//       ASSERT(0);
//       return -1;
//     }
//   }

//   // TODO
//   return 0;
// }

// int tdbTxnRollback(TENV *pEnv) {
//   // TODO
//   return 0;
// }