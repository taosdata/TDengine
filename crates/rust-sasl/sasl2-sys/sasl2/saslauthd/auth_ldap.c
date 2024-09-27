/* MODULE: auth_ldap */
/* COPYRIGHT
 * Copyright (c) 2002-2002 Igor Brezac
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

/* SYNOPSIS
 * Authenticate against LDAP.
 * END SYNOPSIS */

/* PUBLIC DEPENDENCIES */
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <ctype.h>
#include "mechanisms.h"

/* END PUBLIC DEPENDENCIES */

# define RETURN(x) {return strdup(x);}

/* FUNCTION: auth_ldap */

#ifdef AUTH_LDAP

#include "lak.h"
#include "globals.h"

const char *SASLAUTHD_CONF_FILE = SASLAUTHD_CONF_FILE_DEFAULT;

char *					/* R: allocated response string */
auth_ldap(
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service,
  const char *realm
  /* END PARAMETERS */
  )
{
	static LAK *lak = NULL;
	int rc = 0;

	if (lak == NULL) {
		rc = lak_init(SASLAUTHD_CONF_FILE, &lak);
		if (rc != LAK_OK) {
			lak = NULL;
			RETURN("NO");
		}
	}

	rc = lak_authenticate(lak, login, service, realm, password);
    	if (rc == LAK_OK) {
		RETURN("OK");
	} else {
		RETURN("NO");
	}
}

/* FUNCTION: auth_ldap_init */

/* SYNOPSIS
 * Validate the host and service names for the remote server.
 * END SYNOPSIS */

int
auth_ldap_init (
  /* PARAMETERS */
  void					/* no parameters */
  /* END PARAMETERS */
  )
{
    if (mech_option != NULL) {
	SASLAUTHD_CONF_FILE = mech_option;
    }

    return 0;
}

#else /* !AUTH_LDAP */

char *
auth_ldap(
  const char *login __attribute__((unused)),
  const char *password __attribute__((unused)),
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  )
{
     return NULL;
}

#endif /* !AUTH_LDAP */

/* END FUNCTION: auth_ldap */

/* END MODULE: auth_ldap */
