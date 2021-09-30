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

#include "os.h"
#include "tthread.h"
#include "tglobal.h"
#include "taosdef.h"
#include "tutil.h"
#include "tulog.h"
#include "taoserror.h"

// create new thread
pthread_t* taosCreateThread( void *(*__start_routine) (void *), void* param) {
  pthread_t* pthread = (pthread_t*)malloc(sizeof(pthread_t));
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  int32_t ret = pthread_create(pthread, &thattr, __start_routine, param);
  pthread_attr_destroy(&thattr);

  if (ret != 0) {
    free(pthread);
    return NULL;
  }
  return pthread;
}

// destory thread 
bool taosDestoryThread(pthread_t* pthread) {
  if(pthread == NULL) return false;
  if(taosThreadRunning(pthread)) {
    pthread_cancel(*pthread);
    pthread_join(*pthread, NULL);
  }
  
  free(pthread);
  return true;
}

// thread running return true
bool taosThreadRunning(pthread_t* pthread) {
  if(pthread == NULL) return false;
  int ret = pthread_kill(*pthread, 0);
  if(ret == ESRCH)
    return false;
  if(ret == EINVAL) 
    return false;   
  // alive
  return true;
}
