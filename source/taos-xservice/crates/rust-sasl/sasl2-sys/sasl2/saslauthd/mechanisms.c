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
 * mechanisms[] contains the NULL terminated list of supported
 * authentication drivers.
 * END SYNOPSIS */

/* PUBLIC DEPENDENCIES */
#include "mechanisms.h"

#ifdef AUTH_DCE
# include "auth_dce.h"
#endif /* AUTH_DCE */
#ifdef AUTH_SHADOW
# include "auth_shadow.h"
#endif /* AUTH_SHADOW */
#ifdef AUTH_SIA
# include "auth_sia.h"
#endif /* AUTH_SIA */
#include "auth_krb4.h"
#include "auth_krb5.h"
#include "auth_getpwent.h"
#include "auth_sasldb.h"
#include "auth_rimap.h"
#ifdef AUTH_PAM
# include "auth_pam.h"
#endif
#ifdef AUTH_LDAP
#include "auth_ldap.h"
#endif
#ifdef AUTH_HTTPFORM
#include "auth_httpform.h"
#endif
/* END PUBLIC DEPENDENCIES */

authmech_t mechanisms[] =
{
#ifdef AUTH_SASLDB
    {	"sasldb",	0,			auth_sasldb },
#endif /* AUTH_SASLDB */
#ifdef AUTH_DCE
    {	"dce",		0,			auth_dce },
#endif /* AUTH_DCE */
    {	"getpwent",	0,			auth_getpwent },
#ifdef AUTH_KRB4
    {	"kerberos4",	auth_krb4_init,		auth_krb4 },
#endif /* AUTH_KRB4 */
#ifdef AUTH_KRB5
    {	"kerberos5",	auth_krb5_init,		auth_krb5 },
#endif /* AUTH_KRB5 */
#ifdef AUTH_PAM
    {	"pam",		0,			auth_pam },
#endif /* AUTH_PAM */
    {	"rimap",	auth_rimap_init,	auth_rimap },
#ifdef AUTH_SHADOW
    {	"shadow",	0,			auth_shadow },
#endif /* AUTH_SHADOW */
#ifdef AUTH_SIA
    {   "sia",		0,			auth_sia },
#endif /* AUTH_SIA */
#ifdef AUTH_LDAP
    {   "ldap",		auth_ldap_init,		auth_ldap },
#endif /* AUTH_LDAP */
#ifdef AUTH_HTTPFORM
    {   "httpform",     auth_httpform_init,     auth_httpform },
#endif /* AUTH_LDAP */
    {	0,		0,			0 }
};

