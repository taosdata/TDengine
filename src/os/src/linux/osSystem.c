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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tulog.h"

void* taosLoadDll(const char *filename) {
  void *handle = dlopen (filename, RTLD_LAZY);  
  if (!handle) {  
    uError("load dll:%s failed, error:%s", filename, dlerror());  
    return NULL;  
  }

  uDebug("dll %s loaded", filename);

  return handle;
}

void* taosLoadSym(void* handle, char* name) {
	void* sym = dlsym(handle, name);
  char* error = NULL;
  
	if ((error = dlerror()) != NULL)  {  
    uWarn("load sym:%s failed, error:%s", name, dlerror());  
		return NULL;  
	} 

  uDebug("sym %s loaded", name)

  return sym;
}

void taosCloseDll(void *handle) {
  if (handle) {
    dlclose(handle);
  }
}


