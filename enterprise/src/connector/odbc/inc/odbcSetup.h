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

#ifndef TDENGINE_ODBCSETUP_H
#define TDENGINE_ODBCSETUP_H

#include "odbc.h"

/* Attribute key indexes into an array of Attr structs, see below */

#define KEY_DSN        0
#define KEY_IP         1
#define KEY_DBNAME     2
#define KEY_USER       3
#define KEY_PASSWORD   4
#define NUMOFKEYS      5

typedef struct {
    BOOL supplied;
    char attr[MAXPATHLEN + 1];
} ATTR;

typedef struct {
    SQLHWND parent;
    ATTR    attr[NUMOFKEYS + 1];
    char    driver[MAXKEYLEN + 1];
    BOOL    isAdd;
} SETUPDLG;

typedef struct {
    char *key;
    int ikey;
} ATTRMAP;

#if defined(_WIN32) || defined(_WIN64)
#include <ShlObj.h>
#include <ShObjIdl.h>
#endif

BOOL INSTAPI ConfigDSN(HWND hwnd, WORD request, LPCSTR driver, LPCSTR attribs);
void odbc_setup_init();
void odbcGetInfoFromSetupDlg(char *dsn, char* server, char* dbname, char *uid, char *pwd);
extern bool odbcSetupSilent;

#endif