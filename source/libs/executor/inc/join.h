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

/*
 * Shared join primitives used by both hash join and merge join operators.
 * Defines the build/probe table role enum and common helper macros.
 */
#ifndef TDENGINE_JOIN_H
#define TDENGINE_JOIN_H

#ifdef __cplusplus
extern "C" {
#endif


/*
 * Identifies the role of a table in the join execution.
 * BUILD: the side whose rows are loaded into the hash table (or sorted buffer).
 * PROBE: the side whose rows are streamed through and matched against the build side.
 * For RIGHT joins, build and probe sides are swapped so that LEFT join logic applies symmetrically.
 */
typedef enum EJoinTableType {
  E_JOIN_TB_BUILD = 1,
  E_JOIN_TB_PROBE
} EJoinTableType;


/* Returns a human-readable string for the table role (for logging/debugging). */
#define JOIN_TBTYPE(_type) (E_JOIN_TB_BUILD == (_type) ? "BUILD" : "PROBE")

/* Checks whether the join is FULL OUTER (requires special build-side non-match tracking). */
#define IS_FULL_OUTER_JOIN(_jtype, _stype) ((_jtype) == JOIN_TYPE_FULL && (_stype) == JOIN_STYPE_OUTER)



#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_JOIN_H
