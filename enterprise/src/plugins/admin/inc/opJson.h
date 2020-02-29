/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/

#ifndef TDENGINE_OP_JSON_H
#define TDENGINE_OP_JSON_H

#include "taos.h"
#include "httpHandle.h"
#include "httpJson.h"

void opInitPutDetailJson(HttpContext *pContext);
void opCleanPutDetailJson(HttpContext *pContext);
void opStartPutDetailJson(HttpContext *pContext, HttpSqlCmd *cmd, TAOS_RES *result);
void opStopPutDetailJson(HttpContext *pContext, HttpSqlCmd *cmd);
void opBuildPutDetailAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int affect_rows);
bool opCheckPutDetailFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);
void opSetPutDetailNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);

void opInitPutSummaryJson(HttpContext *pContext);
void opCleanPutSummaryJson(HttpContext *pContext);
void opBuildPutSummaryAffectRowsJson(HttpContext *pContext, HttpSqlCmd *cmd, int affect_rows);
bool opCheckPutSummaryFinished(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);
void opSetPutSummaryNextCmd(struct HttpContext *pContext, HttpSqlCmd *cmd, int code);

#endif