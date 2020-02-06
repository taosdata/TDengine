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

#ifndef TDENGINE_ADMIN_HANDLE_H
#define TDENGINE_ADMIN_HANDLE_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "http.h"
#include "httpCode.h"
#include "httpHandle.h"
#include "httpResp.h"

// this define is for url parse, such as:
// 1. /admin/login/user/pwd
// 2. /admin/logout
// 3. /admin/sql
// 4. /admin/meta
// 6. /admon/info
#define ADMIN_ROOT_URL_POS 0
#define ADMIN_ACTION_URL_POS 1
#define ADMIN_USER_URL_POS 2
#define ADMIN_PASS_URL_POS 3

void adminInitHandle(HttpServer* pServer);
bool adminProcessRequest(struct HttpContext* pContext);

#endif