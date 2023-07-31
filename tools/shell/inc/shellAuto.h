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

#ifndef __SHELL_AUTO__
#define __SHELL_AUTO__

#include "shellInt.h"

#define TAB_KEY 0x09

// press tab key
void pressTabKey(SShellCmd* cmd);

// press othr key
void pressOtherKey(char c);

// init shell auto function , shell start call once
bool shellAutoInit();

// set conn
void shellSetConn(TAOS* conn, bool runOnce);

// exit shell auto function, shell exit call once
void shellAutoExit();

// callback autotab module
void callbackAutoTab(char* sqlstr, TAOS* pSql, bool usedb);

// introduction
void printfIntroduction();

// show all commands help
void showHelp();

#endif
