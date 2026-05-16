/* MODULE: auth_krb4 */

/* COPYRIGHT
 * Copyright (c) 1997 Messaging Direct Ltd.
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
#include <unistd.h>
#include "mechanisms.h"
#include "globals.h"
#include "cfile.h"
#include "krbtf.h"

#ifdef AUTH_KRB4

# include <krb.h>

# ifdef WITH_DES
#  ifdef WITH_SSL_DES
#   include <openssl/des.h>
#  else
#   include <des.h>
#  endif /* WITH_SSL_DES */
# endif /* WITH_DES */

#endif /* AUTH_KRB4 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/stat.h>
#include "auth_krb4.h"

#ifdef DEADCODE
extern int swap_bytes;			/* from libkrb.a   */
#endif /* DEADCODE */
/* END PUBLIC DEPENDENCIES */

/* PRIVATE DEPENDENCIES */
#ifdef AUTH_KRB4
static char default_realm[REALM_SZ];
static cfile config = 0;
static char myhostname[BUFSIZ];    /* Is BUFSIZ right here? */
static char *srvtabname = "";      /* "" means "system default srvtab" */
static char *verify_principal = "rcmd"; /* A principal in the default srvtab */
#endif /* AUTH_KRB4 */
/* END PRIVATE DEPENDENCIES */

#define TF_NAME_LEN 128

/* Kerberos for Macintosh doesn't define this, so we will. (Thanks Fink!) */
#ifndef KRB_TICKET_GRANTING_TICKET
#define KRB_TICKET_GRANTING_TICKET "krbtgt"
#endif /* !defined(KRB_TICKET_GRANTING_TICKET) */


/* FUNCTION: auth_krb4_init */

/* SYNOPSIS
 * Initialize the Kerberos IV authentication environment.
 *
 * krb4 proxy authentication has a side effect of creating a ticket
 * file for the user we are authenticating. We keep these in a private
 * directory so as not to override a system ticket file that may be
 * in use.
 *
 * This function tries to create the directory, and initializes the
 * global variable tf_dir with the pathname of the directory.
 * END SYNOPSIS */

int					/* R: -1 on failure, else 0 */
auth_krb4_init (
  /* PARAMETERS */
  void					/* no parameters */
  /* END PARAMETERS */
  )
{
#ifdef AUTH_KRB4
    /* VARIABLES */
    int rc;				/* return code holder */
    char *configname = 0;
    /* END VARIABLES */

    if (mech_option)
      configname = mech_option;
    else if (access(SASLAUTHD_CONF_FILE_DEFAULT, F_OK) == 0)
      configname = SASLAUTHD_CONF_FILE_DEFAULT;

    if (configname) {
      char complaint[1024];

      config = cfile_read(configname, complaint, sizeof(complaint));
      if (!config) {
	syslog(LOG_ERR, "auth_krb4_init %s", complaint);
	return -1;
      }
    }

    if (config) {
      srvtabname = cfile_getstring(config, "krb4_srvtab", srvtabname);
      verify_principal = cfile_getstring(config, "krb4_verify_principal",
					 verify_principal);
    }
    
    if (krbtf_init() == -1) {
      syslog(LOG_ERR, "auth_krb4_init krbtf_init failed");
      return -1;
    }

    rc = krb_get_lrealm(default_realm, 1);
    if (rc) {
	syslog(LOG_ERR, "auth_krb4: krb_get_lrealm: %s",
	       krb_get_err_text(rc));
	return -1;
    }

    if (gethostname(myhostname, sizeof(myhostname)) < 0) {
      syslog(LOG_ERR, "auth_krb4: gethoanem(): %m");
      return -1;
    }
    myhostname[sizeof(myhostname) - 1] = '\0';

    return 0;
#else /* ! AUTH_KRB4 */
    return -1;
#endif /* ! AUTH_KRB4 */
}

/* END FUNCTION: auth_krb4_init */

/* FUNCTION: auth_krb4 */

/* SYNOPSIS
 * Authenticate against Kerberos IV.
 * END SYNOPSIS */

#ifdef AUTH_KRB4

char *					/* R: allocated response string */
auth_krb4 (
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service,
  const char *realm_in
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    char aname[ANAME_SZ];		/* Kerberos principal */
    const char *realm;		        /* Kerberos realm to authenticate in */
    int rc;				/* return code */
    char tf_name[TF_NAME_LEN];		/* Ticket file name */
    char *instance, *user_specified;
    KTEXT_ST ticket;
    AUTH_DAT kdata;
    /* END VARIABLES */

    /*
     * Make sure we have a password. If this is NULL the call
     * to krb_get_pw_in_tkt below would try to prompt for
     * one interactively.
     */
    if (password == NULL) {
	syslog(LOG_ERR, "auth_krb4: NULL password?");
	return strdup("NO saslauthd internal error");
    }

    if (krbtf_name(tf_name, sizeof(tf_name)) != 0) {
      syslog(LOG_ERR, "auth_krb4: could not generate ticket file name");
      return strdup("NO saslauthd internal error");
    }
    krb_set_tkt_string(tf_name);

    strncpy(aname, login, ANAME_SZ-1);
    aname[ANAME_SZ-1] = '\0';

    instance = "";

    if (config) {
      char keyname[1024];

      snprintf(keyname, sizeof(keyname), "krb4_%s_instance", service);
      instance = cfile_getstring(config, keyname, "");
    }

    user_specified = strchr(aname, '.');
    if (user_specified) {
      if (instance && instance[0]) {
	/* sysadmin specified a (mandatory) instance */
	if (strcmp(user_specified + 1, instance)) {
	  return strdup("NO saslauthd principal name error");
	}
	/* nuke instance from "aname"-- matches what's already in "instance" */
	*user_specified = '\0';
      } else {
	/* sysadmin has no preference, so we shift
	 * instance name from "aname" to "instance"
	 */
	*user_specified = '\0';
	instance = user_specified + 1;
      }
    }

    if(realm_in && *realm_in != '\0') {
	realm = realm_in;
    } else {
	realm = default_realm;
    }

    rc = krb_get_pw_in_tkt(aname, instance, realm,
			   KRB_TICKET_GRANTING_TICKET,
			   realm, 1, password);

    if (rc == INTK_BADPW || rc == KDC_PR_UNKNOWN) {
	return strdup("NO");
    } else if (rc != KSUCCESS) {
      syslog(LOG_ERR, "ERROR: auth_krb4: krb_get_pw_in_tkt: %s",
	     krb_get_err_text(rc));

      return strdup("NO saslauthd internal error");
    }

    /* if the TGT wasn't spoofed, it should entitle us to an rcmd ticket... */
    rc = krb_mk_req(&ticket, verify_principal, myhostname, default_realm, 0);

    if (rc != KSUCCESS) {
      syslog(LOG_ERR, "ERROR: auth_krb4: krb_get_pw_in_tkt: %s",
	     krb_get_err_text(rc));
      dest_tkt();
      return strdup("NO saslauthd internal error");
    }

    /* .. and that ticket should match our secret host key */
    rc = krb_rd_req(&ticket, verify_principal, myhostname, 0, &kdata, srvtabname);

    if (rc != RD_AP_OK) {
      syslog(LOG_ERR, "ERROR: auth_krb4: krb_rd_req:%s",
	     krb_get_err_text(rc));
      dest_tkt();
      return strdup("NO saslauthd internal error");
    }

    dest_tkt();

    return strdup("OK");
}

#else /* ! AUTH_KRB4 */

char *
auth_krb4 (
  const char *login __attribute__((unused)),
  const char *password __attribute__((unused)),
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  )
{
    return NULL;
}

#endif /* ! AUTH_KRB4 */

/* END FUNCTION: auth_krb4 */

/* END MODULE: auth_krb4 */
