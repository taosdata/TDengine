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
#ifndef TDENGINE_JOIN_H
#define TDENGINE_JOIN_H

#ifdef __cplusplus
extern "C" {
#endif


typedef enum EJoinTableType {
  E_JOIN_TB_BUILD = 1,
  E_JOIN_TB_PROBE
} EJoinTableType;


#define JOIN_TBTYPE(_type) (E_JOIN_TB_BUILD == (_type) ? "BUILD" : "PROBE")
#define IS_FULL_OUTER_JOIN(_jtype, _stype) ((_jtype) == JOIN_TYPE_FULL && (_stype) == JOIN_STYPE_OUTER)



#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_JOIN_H
