/* MODULE: auth_sasldb */

/* COPYRIGHT
 * Copyright (c) 1997-2000 Messaging Direct Ltd.
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
 * THIS SOFTWARE IS PROVIDED BY MESSAGING DIRECT LTD. ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL MESSAGING DIRECT LTD. OR
 * ITS EMPLOYEES OR AGENTS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 * END COPYRIGHT */

/* SYNOPSIS
 * crypt(3) based passwd file validation
 * END SYNOPSIS */

/* PUBLIC DEPENDENCIES */
#include "mechanisms.h"

#include <string.h>
#include <stdlib.h>
#include <pwd.h>
#include <config.h>
#include <unistd.h>
/* END PUBLIC DEPENDENCIES */

#include "../lib/saslint.h"
#define RETURN(x) return strdup(x)


#ifdef AUTH_SASLDB
#include "../include/sasl.h"
#include "../include/saslplug.h"
#include "../sasldb/sasldb.h"

static int
vf(void *context __attribute__((unused)),
   char *file  __attribute__((unused)),
   int type  __attribute__((unused)))
{
    /* always say ok */ 
    return SASL_OK;
}

static int lame_getcallback(sasl_conn_t *conn __attribute__((unused)),
			    unsigned long callbackid,
			    int (**pproc)(),
			    void **pcontext)
{
    if(callbackid == SASL_CB_VERIFYFILE) {
	*pproc = vf;
	*pcontext = NULL;
	return SASL_OK;
    }
	
    return SASL_FAIL;
}

static void lame_log(sasl_conn_t *conn __attribute__((unused)),
                     int level __attribute__((unused)),
                     const char *fmt __attribute__((unused)),
                     ...) 
{
    return;
}

static void lame_seterror(sasl_conn_t *conn __attribute__((unused)),
                          unsigned flags __attribute__((unused)),
			  const char *fmt __attribute__((unused)),
                          ...) 
{
    return;
}

/* FUNCTION: init_lame_utils */
/* This sets up a very minimal sasl_utils_t for use only with the
 * database functions */
static void init_lame_utils(sasl_utils_t *utils) 
{
    memset(utils, 0, sizeof(sasl_utils_t));

    utils->malloc=(sasl_malloc_t *)malloc;
    utils->calloc=(sasl_calloc_t *)calloc;
    utils->realloc=(sasl_realloc_t *)realloc;
    utils->free=(sasl_free_t *)free;

    utils->getcallback=lame_getcallback;
    utils->log=lame_log;
    utils->seterror=lame_seterror;

    return;
}

/* END FUNCTION: init_lame_utils */
	
#endif /* AUTH_SASLDB */

/* FUNCTION: auth_sasldb */

char *					/* R: allocated response string */
auth_sasldb (
  /* PARAMETERS */
#ifdef AUTH_SASLDB
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm
#else
  const char *login __attribute__((unused)),/* I: plaintext authenticator */
  const char *password __attribute__((unused)),  /* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
#endif
  /* END PARAMETERS */
  )
{
#ifdef AUTH_SASLDB
    /* VARIABLES */
    char pw[1024];			/* pointer to passwd file entry */
    sasl_utils_t utils;
    int ret;
    size_t outsize;
    const char *use_realm;
    char realm_buf[MAXFQDNLEN];
    /* END VARIABLES */

    init_lame_utils(&utils);

    _sasl_check_db(&utils, (void *)0x1);

    if(!realm || !strlen(realm)) {
	ret = gethostname(realm_buf,MAXFQDNLEN);
	if(ret) RETURN("NO");
	use_realm = realm_buf;
    } else {
	use_realm = realm;
    }
    

    ret = _sasldb_getdata(&utils, (void *)0x1, login, use_realm,
			  "userPassword", pw, 1024, &outsize);

    if (ret != SASL_OK) {
	RETURN("NO");
    }

    if (strcmp(pw, password)) {
	RETURN("NO");
    }

    RETURN("OK");
#else
    RETURN("NO");
#endif
}

/* END FUNCTION: auth_sasldb */

/* END MODULE: auth_sasldb */
