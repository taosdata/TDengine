/* MODULE: auth_dce */

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
 * Authenticate against DCE.
 * END SYNOPSIS */

/* PUBLIC DEPENDENCIES */
#include <stdlib.h>
#include <string.h>
#include "mechanisms.h"

#include "auth_dce.h"

/* END PUBLIC DEPENDENCIES */

# define RETURN(x) {return strdup(x);}

/* FUNCTION: auth_dce */

#ifdef AUTH_DCE

char *					/* R: allocated response string */
auth_dce(
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  /* END PARAMETERS */
  )
{
    int reenter = 0;			/* internal to authenticate() */
    int rc;				/* return code holder */
    char *msg;				/* response from authenticate() */
    static char *reply;			/* our reply string */
    
    int authenticate(char *, char *, int *, char **); /* DCE authenticator */

    rc = authenticate(login, password, &reenter, &msg);
    if (rc != 0) {
	/*
	 * Failed. authenticate() has allocated storage for msg. We have
	 * to copy the message text into a static buffer and free the
	 * space allocated inside of authenticate().
	 */
	if (reply != 0) {
	    free(reply);
	    reply = 0;
	}
	if (msg == 0)
	    RETURN("NO");
	reply = malloc(strlen(msg) + sizeof("NO "));
	if (reply == 0) {
	    if (msg != 0)
		free(msg);
	    RETURN("NO (auth_dce malloc failure)");
	}
	strcpy(reply, "NO ");
	strcat(reply, msg);
	free(msg);
	RETURN(reply);
    } else {
	if (msg != 0)
	    free(msg);
	RETURN("OK");
    }
}

#else /* !AUTH_DCE */

char *
auth_dce(
  const char *login __attribute__((unused)),
  const char *password __attribute__((unused)),
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  )
{
     return NULL;
}

#endif /* !AUTH_DCE */

/* END FUNCTION: auth_dce */

/* END MODULE: auth_dce */
