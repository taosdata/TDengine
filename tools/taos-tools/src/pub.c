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
 #include "pub.h"
 #include "benchLog.h"

struct tm* toolsLocalTime(const time_t *timep, struct tm *result) {
    #if defined(LINUX) || defined(DARWIN)
        localtime_r(timep, result);
    #else
        localtime_s(result, timep);
    #endif
        return result;
}
    
int32_t toolsGetTimeOfDay(struct timeval *tv) {
    #if defined(WIN32) || defined(WIN64)
        LARGE_INTEGER t;
        FILETIME      f;
    
        GetSystemTimeAsFileTime(&f);
        t.QuadPart = f.dwHighDateTime;
        t.QuadPart <<= 32;
        t.QuadPart |= f.dwLowDateTime;
    
        t.QuadPart -= TIMEEPOCH;
        tv->tv_sec = t.QuadPart / 10000000;
        tv->tv_usec = (t.QuadPart % 10000000) / 10;
        return (0);
    #else
        return gettimeofday(tv, NULL);
    #endif
}

void engineError(char * module, char * fun, int32_t code) {
    errorPrint("%s %s fun=%s error code:0x%08X \n", TIP_ENGINE_ERR, module, fun, code);
}

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
 
 int32_t parseDsn(char* dsn, char **host, char **port, char **user, char **pwd) {
     // dsn format:
     // local  http://127.0.0.1:6041 
     // cloud  https://gw.cloud.taosdata.com?token=617ffdf...
     //        https://gw.cloud.taosdata.com:433?token=617ffdf...
 
     // find "://"
     char *p1 = strstr(dsn, "://");
     if (p1 == NULL) {
         errorPrint("dsn invalid, not found \"://\" in ds:%s\n", dsn);
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
         errorPrint("dsn invalid, found \"?\" but not found \"=\" in ds:%s\n", dsn);
         return -1;
     }
 
     return 0;
 }
 