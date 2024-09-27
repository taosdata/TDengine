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

#ifndef _MECHANISMS_H
#define _MECHANISMS_H

#include <config.h>

/* PUBLIC DEPENDENCIES */
/* Authentication mechanism dispatch table definition */
typedef struct {
    char *name;				/* name of the mechanism */
    int (*initialize)(void);		/* initialization function */
    char *(*authenticate)(const char *, const char *,
			  const char *, const char *); /* authentication
							  function */
} authmech_t;

extern authmech_t mechanisms[];		/* array of supported auth mechs */
extern authmech_t *authmech;		/* auth mech daemon is using */
/* END PUBLIC DEPENDENCIES */

/*
 * Figure out which optional drivers we support.
 */
#ifndef AUTH_KRB5
# if defined(HAVE_KRB5_H) && defined(HAVE_GSSAPI)
#  define AUTH_KRB5
# endif
#endif

#ifndef AUTH_KRB4
# if defined(HAVE_KRB)
#  define AUTH_KRB4
# endif
#endif

#ifndef AUTH_DCE
# if defined(HAVE_USERSEC_H) && defined(HAVE_AUTHENTICATE)
#  define AUTH_DCE
# endif
#endif

#ifndef AUTH_SHADOW
# if defined(HAVE_GETSPNAM) || defined(HAVE_GETUSERPW)
#  define AUTH_SHADOW
# endif
#endif

#ifndef AUTH_SIA
# if defined(HAVE_SIA_VALIDATE_USER)
#  define AUTH_SIA
# endif
#endif

#ifndef AUTH_PAM
# ifdef HAVE_PAM
#  define AUTH_PAM
# endif
#endif

#ifndef AUTH_LDAP
# ifdef HAVE_LDAP
#  define AUTH_LDAP
# endif
#endif

#ifndef AUTH_HTTPFORM
# ifdef HAVE_HTTPFORM
#  define AUTH_HTTPFORM
# endif
#endif

#endif  /* _MECHANISMS_H */
