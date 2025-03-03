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
 