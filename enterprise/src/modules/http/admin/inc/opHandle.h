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

#ifndef TDENGINE_OP_HANDLE_H
#define TDENGINE_OP_HANDLE_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "http.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

#define OP_ROOT_URL_POS 0
#define OP_DB_URL_POS 1
#define OP_ACTION_URL_POS 2
#define OP_USER_URL_POS 3
#define OP_PASS_URL_POS 4

void opInitHandle(HttpServer* pServer);
bool opProcessRequest(struct HttpContext* pContext);

#endif