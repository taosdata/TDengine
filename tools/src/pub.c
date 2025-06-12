/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

 #include <stdio.h>
 #include <taos.h>
 #include "../inc/pub.h"


 char* strToLowerCopy(const char *str) {
     if (str == NULL) {
         return NULL;
     }
     size_t len = strlen(str);
     char *result = (char*)malloc(len + 1);
     if (result == NULL) {
         return NULL;
     }
     for (size_t i = 0; i < len; i++) {
         result[i] = tolower((unsigned char)str[i]);
     }
     result[len] = '\0';
     return result;
 }
 
 int32_t parseDsn(char* dsn, char **host, char **port, char **user, char **pwd, char *error) {
     // dsn format:
     // local  http://127.0.0.1:6041 
     // cloud  https://gw.cloud.taosdata.com?token=617ffdf...
     //        https://gw.cloud.taosdata.com:433?token=617ffdf...
 
     // find "://"
     char *p1 = strstr(dsn, "://");
     if (p1 == NULL) {
         sprintf(error, "%s", "dsn invalid, not found \"://\" ");
         return -1;
     }
     *host = p1 + 3; // host
     char *p = *host;
 
     // find ":" - option
     char *p2 = strstr(p, ":");
     if (p2) {
         p     = p2 + 1;
         *port = p2 + 1; // port
         *p2   = 0;
     }
 
     // find "?"
     char *p3 = strstr(p, "?");
     if (p3) {
         p     = p3 + 1;
         *user = p3 + 1;
         *p3   = 0;
     } else {
         return 0;
     }
 
     // find "="
     char *p4 = strstr(p, "=");
     if (p4) {
         *p4  = 0; 
         *pwd = p4 + 1;
     } else {
         sprintf(error, "%s", "dsn invalid, found \"?\" but not found \"=\" ");
         return -1;
     }
 
     return 0;
 }

 // get comn mode, if invalid exit app
 int8_t getConnMode(char *arg) {
    // compare
    if (strcasecmp(arg, STR_NATIVE) == 0 || strcasecmp(arg, "0") == 0) {
        return  CONN_MODE_NATIVE;
    } else if (strcasecmp(arg, STR_WEBSOCKET) == 0 || strcasecmp(arg, "1") == 0) {
        return CONN_MODE_WEBSOCKET;
    } else {
        fprintf(stderr, "invalid input %s for option -Z, only support: %s or %s\r\n", arg, STR_NATIVE, STR_WEBSOCKET);
        exit(-1);
    }
 }

 // set conn mode
int32_t setConnMode(int8_t connMode, char *dsn, bool show) {
    // check default
    if (connMode == CONN_MODE_INVALID) {
      if (dsn && dsn[0] != 0) {
        connMode = CONN_MODE_WEBSOCKET;
      } else {
        // default
        connMode = CONN_MODE_DEFAULT;
      }    
    }
  
    // set conn mode
    char * strMode = connMode == CONN_MODE_NATIVE ? STR_NATIVE : STR_WEBSOCKET;
    int32_t code = taos_options(TSDB_OPTION_DRIVER, strMode);
    if (code != 0) {
      fprintf(stderr, "failed to load driver. since %s [0x%08X]\r\n", taos_errstr(NULL), taos_errno(NULL));
      return code;
    }

    if (show) {
        fprintf(stdout, "\nConnect mode is : %s\n\n", strMode);
    }

    return 0;
}

// default mode
int8_t workingMode(int8_t connMode, char *dsn) {
    int8_t mode = connMode;
    if (connMode == CONN_MODE_INVALID) {
        // no input from command line or config
        if (dsn && dsn[0] != 0) {
          mode = CONN_MODE_WEBSOCKET;
        } else {
          // default
          mode = CONN_MODE_DEFAULT;
        }    
    }
    return mode;
}

// get default port
uint16_t defaultPort(int8_t connMode, char *dsn) {
    // port 0 is default
    return 0;
    
    /*
    // consistent with setConnMode
    int8_t mode = workingMode(connMode, dsn);

    // default port
    return mode == CONN_MODE_NATIVE ? DEFAULT_PORT_NATIVE : DEFAULT_PORT_WS_LOCAL;
    */
}
 
//
// ---------- pthread mutex impl -------------
//

int32_t taosThreadMutexInit(TdThreadMutex *mutex, const TdThreadMutexAttr *attr) {
#ifdef WINDOWS
  /**
   * Windows Server 2003 and Windows XP:  In low memory situations, InitializeCriticalSection can raise a
   * STATUS_NO_MEMORY exception. Starting with Windows Vista, this exception was eliminated and
   * InitializeCriticalSection always succeeds, even in low memory situations.
   */
  InitializeCriticalSection(mutex);
  return 0;
#else
  return pthread_mutex_init(mutex, attr);
#endif
}

int32_t taosThreadMutexDestroy(TdThreadMutex *mutex) {
#ifdef WINDOWS
  DeleteCriticalSection(mutex);
  return 0;
#else
  return pthread_mutex_destroy(mutex);
#endif
}

int32_t taosThreadMutexLock(TdThreadMutex *mutex) {
#ifdef WINDOWS
  EnterCriticalSection(mutex);
  return 0;
#else
  return pthread_mutex_lock(mutex);
#endif
}

int32_t taosThreadMutexUnlock(TdThreadMutex *mutex) {
#ifdef WINDOWS
  LeaveCriticalSection(mutex);
  return 0;
#else
  return pthread_mutex_unlock(mutex);
#endif
}