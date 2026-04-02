/* MODULE: auth_getpwent */

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
#include <unistd.h>
#include <string.h>
#include <pwd.h>
#include <errno.h>
#include <syslog.h>
#include <stdio.h>

#ifdef HAVE_CRYPT_H
#include <crypt.h>
#endif

# ifdef WITH_DES
#  ifdef WITH_SSL_DES
#   ifndef OPENSSL_DISABLE_OLD_DES_SUPPORT
#    define OPENSSL_DISABLE_OLD_DES_SUPPORT
#   endif
#   include <openssl/des.h>
#  else
#   include <des.h>
#  endif /* WITH_SSL_DES */
# endif /* WITH_DES */

# include "globals.h"
/* END PUBLIC DEPENDENCIES */

#define RETURN(x) return strdup(x)

/* FUNCTION: auth_getpwent */

char *					/* R: allocated response string */
auth_getpwent (
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    struct passwd *pw;			/* pointer to passwd file entry */
    char *crpt_passwd;			/* encrypted password */
    int errnum;
    /* END VARIABLES */
  
    errno = 0;
    pw = getpwnam(login);
    errnum = errno;
    endpwent();

    if (pw == NULL) {
	if (errnum != 0) {
	    char *errstr;

	    if (flags & VERBOSE) {
		syslog(LOG_DEBUG, "DEBUG: auth_getpwent: getpwnam(%s) failure: %m", login);
	    }
	    if (asprintf(&errstr, "NO Username lookup failure: %s", strerror(errno)) == -1) {
		/* XXX the hidden strdup() will likely fail and return NULL here.... */
		RETURN("NO Username lookup failure: unknown error (ENOMEM formatting strerror())");
	    }
	    return errstr;
	} else {
	    if (flags & VERBOSE) {
		syslog(LOG_DEBUG, "DEBUG: auth_getpwent: getpwnam(%s): invalid username", login);
	    }
	    RETURN("NO Invalid username");
	}
    }

    crpt_passwd = crypt(password, pw->pw_passwd);
    if (!crpt_passwd || strcmp(pw->pw_passwd, (const char *)crpt_passwd)) {
	if (flags & VERBOSE) {
	    syslog(LOG_DEBUG, "DEBUG: auth_getpwent: %s: invalid password", login);
	}
	RETURN("NO Incorrect password");
    }

    if (flags & VERBOSE) {
	syslog(LOG_DEBUG, "DEBUG: auth_getpwent: OK: %s", login);
    }
    RETURN("OK");
}

/* END FUNCTION: auth_getpwent */

/* END MODULE: auth_getpwent */
