/* MODULE: auth_sia */

/* COPYRIGHT
 * Copyright (c) 1998 Messaging Direct Ltd.
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

/* PUBLIC DEPENDENCIES */
#include "mechanisms.h"

#ifdef AUTH_SIA

#include <string.h>
#include <sia.h>
#include <siad.h>

#include "auth_sia.h"
#include "globals.h"
/* END PUBLIC DEPENDENCIES */

/* FUNCTION: auth_sia */

/* SYNOPSIS
 * Authenticate against the Digital UNIX SIA environment.
 */

char *					/* R: allocated response string */
auth_sia (
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    int rc;
    /* END VARIABLES */

    rc = sia_validate_user(0, g_argc, g_argv, 0, login, 0, 0, 0, password);
    if (rc == SIASUCCESS) {
	return strdup("OK");
    }
    if (rc == SIAFAIL) {
	return strdup("NO");
    }
    /* Shouldn't happen */
    syslog(LOG_WARNING,
	   "auth_sia: impossible return (%d) from sia_validate_user", rc);
    return strdup("NO (possible system error)");
}

#else /* ! AUTH_SIA */

char *
auth_sia(
  const char *login __attribute__((unused)),
  const char *password __attribute__((unused)),
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  )
{
    return NULL;
}
#endif

/* END FUNCTION: auth_sia */

/* END MODULE: auth_sia */
