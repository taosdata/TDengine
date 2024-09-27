/* COPYRIGHT
 * Copyright (c) 2002-2003 Igor Brezac
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY IGOR BREZAC. ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL IGOR BREZAC OR
 * ITS EMPLOYEES OR AGENTS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 * END COPYRIGHT */

#ifndef _LAK_H
#define _LAK_H

#include <ldap.h>
#include <lber.h>

#if HAVE_SYS_TIME_H
#  include <sys/time.h>
#endif
# include <time.h>

#define LAK_OK 0
#define LAK_FAIL -1
#define LAK_NOMEM -2
#define LAK_RETRY -3
#define LAK_NOT_GROUP_MEMBER -4
#define LAK_INVALID_PASSWORD -5
#define LAK_USER_NOT_FOUND -6
#define LAK_BIND_FAIL -7
#define LAK_CONNECT_FAIL -8

#define LAK_NOT_BOUND 1
#define LAK_BOUND 2

#define LAK_AUTH_METHOD_BIND 0
#define LAK_AUTH_METHOD_CUSTOM 1
#define LAK_AUTH_METHOD_FASTBIND 2

#define LAK_GROUP_MATCH_METHOD_ATTR 0
#define LAK_GROUP_MATCH_METHOD_FILTER 1

#define LAK_BUF_LEN 128
#define LAK_DN_LEN 4096
#define LAK_PATH_LEN 1024
#define LAK_URL_LEN LAK_PATH_LEN

typedef struct lak_conf {
    char   path[LAK_PATH_LEN];
    char   servers[LAK_URL_LEN];
    char   bind_dn[LAK_DN_LEN];
    char   password[LAK_BUF_LEN];
    int    version;
    struct timeval timeout;
    int    size_limit;
    int    time_limit;
    int    deref;
    int    referrals;
    int    restart;
    int    scope;
    char   default_realm[LAK_BUF_LEN];
    char   search_base[LAK_DN_LEN];
    char   filter[LAK_DN_LEN];
    char   password_attr[LAK_BUF_LEN];
    char   group_dn[LAK_DN_LEN];
    char   group_attr[LAK_BUF_LEN];
    char   group_filter[LAK_DN_LEN];
    char   group_search_base[LAK_DN_LEN];
    int    group_scope;
    int    group_match_method;
    char   auth_method;
    int    use_sasl;
    char   id[LAK_BUF_LEN];
    char   authz_id[LAK_BUF_LEN];
    char   mech[LAK_BUF_LEN];
    char   realm[LAK_BUF_LEN];
    char   sasl_secprops[LAK_BUF_LEN];
    int    start_tls;
    int    tls_check_peer;
    char   tls_cacert_file[LAK_PATH_LEN];
    char   tls_cacert_dir[LAK_PATH_LEN];
    char   tls_ciphers[LAK_BUF_LEN];
    char   tls_cert[LAK_PATH_LEN];
    char   tls_key[LAK_PATH_LEN];
    int    debug;
} LAK_CONF;

typedef struct lak_user {
    char bind_dn[LAK_DN_LEN];
    char id[LAK_BUF_LEN];
    char authz_id[LAK_BUF_LEN];
    char mech[LAK_BUF_LEN];
    char realm[LAK_BUF_LEN];
    char password[LAK_BUF_LEN];
} LAK_USER;


typedef struct lak {
    LDAP     *ld;
    char      status;
    LAK_USER *user;
    LAK_CONF *conf;
} LAK;

typedef struct lak_result {
    char              *attribute;
    char              *value;
    size_t             len;
    struct lak_result *next;
} LAK_RESULT;

int lak_init(const char *, LAK **);
void lak_close(LAK *);
int lak_authenticate(LAK *, const char *, const char *, const char *, const char *);
int lak_retrieve(LAK *, const char *, const char *, const char *, const char **, LAK_RESULT **);
void lak_result_free(LAK_RESULT *);
char *lak_error(const int errno);

#endif  /* _LAK_H */
