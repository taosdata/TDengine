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

#ifndef _TD_UTIL_VERSION_H_
#define _TD_UTIL_VERSION_H_

#ifdef __cplusplus
extern "C" {
#endif

extern char td_version[];
extern char td_compatible_version[];
extern char td_gitinfo[];
extern char td_gitinfoOfInternal[];
extern char td_buildinfo[];

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_VERSION_H_*/
