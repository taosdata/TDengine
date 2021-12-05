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
#ifndef __INDEX_FST_AUTAOMATION_H__
#define __INDEX_FST_AUTAOMATION_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct AutomationCtx AutomationCtx;

typedef struct StartWith {
  AutomationCtx  *autoSelf;
} StartWith;

typedef struct Complement {
  AutomationCtx *autoSelf;
  
} Complement;

// automation 
typedef struct AutomationCtx {
// automation interface
  void *data;
} AutomationCtx;

typedef struct Automation {
  void* (*start)() ; 
  bool (*isMatch)(void *);
  bool (*canMatch)(void *data);
  bool (*willAlwaysMatch)(void *state); 
  void* (*accept)(void *state, uint8_t byte);
  void* (*acceptEof)(void *state);
  void *data;
} Automation; 

#ifdef __cplusplus
}
#endif

#endif
