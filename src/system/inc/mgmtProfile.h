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

#ifndef TDENGINE_MGMTPROFILE_H
#define TDENGINE_MGMTPROFILE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "mgmt.h"

int mgmtGetQueryMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn);

int mgmtGetStreamMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn);

int mgmtRetrieveQueries(SShowObj *pShow, char *data, int rows, SConnObj *pConn);

int mgmtRetrieveStreams(SShowObj *pShow, char *data, int rows, SConnObj *pConn);

int mgmtSaveQueryStreamList(char *cont, int contLen, SConnObj *pConn);

int mgmtKillQuery(char *qidstr, SConnObj *pConn);

int mgmtKillStream(char *qidstr, SConnObj *pConn);

int mgmtKillConnection(char *qidstr, SConnObj *pConn);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_MGMTPROFILE_H
