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

#ifndef TDENGINE_ODBCUTIL_H
#define TDENGINE_ODBCUTIL_H

#include "odbc.h"
#include "odbcDriver.h"

const char * odbcHandleTypeString(SQLSMALLINT type);
const char * odbcInfoTypeString(SQLUSMALLINT type);
const char * odbcStmtOptionString(SQLUSMALLINT opt);
const char * odbcFreeStmtOptionString(SQLUSMALLINT opt);
const char * odbcConnectOptionString(SQLUSMALLINT opt);
const char * odbcStmtAttrString(SQLINTEGER attr);
const char * odbcFunctionString(SQLUSMALLINT func);
const char * odbcConnectAttrString(SQLINTEGER attr);
char * odbcDataTypeString(SQLSMALLINT type);
const char * odbcCDataTypeString(SQLSMALLINT type);
const char * odbcDriverCompleteString(SQLUSMALLINT drvcompl);
const char * odbcAttachMsgName(DWORD reason);
const char * odbcConfigDsnType(WORD request);
const char * odbcEnvAttrString(SQLINTEGER attr);
const char * odbcStmtSqlType(int type);
const char * odbcSqlTypeinfoString(SQLSMALLINT sqltype);
const char * odbcColAttrString(SQLUSMALLINT id);
const char * odbcDataTypeTDengineString(int type);
const char * odbcReturnCodeString(SQLRETURN code);
const char * odbcDiagFieldIdString(SQLSMALLINT id);

int odbcTDengineType2SqlCType(int tdType);
int odbcTDengineType2SqlType(int tdType);
int odbcTDengineTypeString2SqlType(const char* tdTypeString);
int odbcTDengineTypeString2TdengineType(const char* tdTypeString);

//int odbcBufferLengthOfTDengineType(int tdType);
int odbcColumnSizeOfTDengineType(int tdType);
int odbcDecimalDigitsOfTDengineType(int tdType);
int odbcNumPrecRadixOfTDengineType(int tdType);

void setstatd(DBC *d, int naterr, char *msg, char *st, ...);
void setstat(STMT *s, int naterr, char *msg, char *st, ...);

int uc_strlen(SQLWCHAR *str);
SQLWCHAR * uc_strncpy(SQLWCHAR *dest, SQLWCHAR *src, int len);
void uc_from_utf_buf(unsigned char *str, int len, SQLWCHAR *uc, int ucLen);
SQLWCHAR * uc_from_utf(unsigned char *str, int len);
char * uc_to_utf(SQLWCHAR *str, int len);
char * uc_to_utf_c(SQLWCHAR *str, int len);
void uc_free(void *str);

void * xmalloc(size_t size);
void xfree(void *p);

#endif