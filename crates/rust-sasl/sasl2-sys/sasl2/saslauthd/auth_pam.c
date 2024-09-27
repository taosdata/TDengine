/* MODULE: auth_pam */

/* COPYRIGHT
 * Copyright (c) 2000 Fabian Knittel.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain any existing copyright
 *    notice, and this entire permission notice in its entirety,
 *    including the disclaimer of warranties.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 2. Redistributions in binary form must reproduce all prior and current
 *    copyright notices, this list of conditions, and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 * END COPYRIGHT */

/*
 * Pluggable Authentication Modules, PAM(8), based authentication module
 * for saslauthd.
 *
 * Written by Fabian Knittel <fknittel@gmx.de>. Original implementation
 * Debian's pwcheck_pam daemon by Michael-John Turner <mj@debian.org>.
 */

/* PUBLIC DEPENDENCIES */
#include "mechanisms.h"
#include <stdio.h>

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef AUTH_PAM

# include <string.h>
# include <syslog.h>
# include <security/pam_appl.h>
# include "auth_pam.h"
/* END PUBLIC DEPENDENCIES */


/* Structure for application specific data passed through PAM
 * to our conv call-back routine saslauthd_pam_conv. */
typedef struct {
    const char *login;			/* plaintext authenticator */
    const char *password;		/* plaintext password */
    pam_handle_t *pamh;			/* pointer to PAM handle */
} pam_appdata;

# define RETURN(x) return strdup(x)


/* FUNCTION: saslauthd_pam_conv */

/* SYNOPSIS
 * Call-back function used by the PAM library to communicate with us. Each
 * received message expects a response, pointed to by resp.
 * END SYNOPSIS */

static int				/* R: PAM return code */
saslauthd_pam_conv (
  /* PARAMETERS */
  int num_msg,				/* I: number of messages */
  const struct pam_message **msg,	/* I: pointer to array of messages */
  struct pam_response **resp,		/* O: pointer to pointer of response */
  void *appdata_ptr			/* I: pointer to app specific data */
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    pam_appdata *my_appdata;		/* application specific data */
    struct pam_response *my_resp;	/* response created by this func */
    int i;				/* loop counter */
    const char *login_prompt;		/* string prompting for user-name */
    int rc;				/* return code holder */
    /* END VARIABLES */

    my_appdata = appdata_ptr;

    my_resp = malloc(sizeof(struct pam_response) * num_msg);
    if (my_resp == NULL)
	return PAM_CONV_ERR;

    for (i = 0; i < num_msg; i++)
	switch (msg[i]->msg_style) {
	/*
	 * We assume PAM_PROMPT_ECHO_OFF to be a request for password.
	 * This assumption might be unsafe.
	 *
	 * For PAM_PROMPT_ECHO_ON we first check whether the provided
	 * request string matches PAM_USER_PROMPT and, only if they do
	 * match, assume it to be a request for the login.
	 */
	case PAM_PROMPT_ECHO_OFF:	/* password */
	    my_resp[i].resp = strdup(my_appdata->password);
	    if (my_resp[i].resp == NULL) {
		syslog(LOG_DEBUG, "DEBUG: saslauthd_pam_conv: strdup failed");
		goto ret_error;
	    }
	    my_resp[i].resp_retcode = PAM_SUCCESS;
	    break;

	case PAM_PROMPT_ECHO_ON:	/* username? */
	    /* Recheck setting each time, as it might have been changed
	       in the mean-while. */
	    rc = pam_get_item(my_appdata->pamh, PAM_USER_PROMPT,
			      (void *) &login_prompt);
	    if (rc != PAM_SUCCESS) {
		syslog(LOG_DEBUG, "DEBUG: saslauthd_pam_conv: unable to read "
		       "login prompt string: %s",
		       pam_strerror(my_appdata->pamh, rc));
		goto ret_error;
	    }

	    if (strcmp(msg[i]->msg, login_prompt) == 0) {
		my_resp[i].resp = strdup(my_appdata->login);
		my_resp[i].resp_retcode = PAM_SUCCESS;
	    } else {			/* ignore */
		syslog(LOG_DEBUG, "DEBUG: saslauthd_pam_conv: unknown prompt "
		       "string: %s", msg[i]->msg);
		my_resp[i].resp = NULL;
		my_resp[i].resp_retcode = PAM_SUCCESS;
	    }
	    break;

	case PAM_ERROR_MSG:		/* ignore */
	case PAM_TEXT_INFO:		/* ignore */
	    my_resp[i].resp = NULL;
	    my_resp[i].resp_retcode = PAM_SUCCESS;
	    break;

	default:			/* error */
	    goto ret_error;
	}
    *resp = my_resp;
    return PAM_SUCCESS;

ret_error:
    /*
     * Free response structure. Don't free my_resp[i], as that
     * isn't initialised yet.
     */
    {
	int y;

	for (y = 0; y < i; y++)
	    if (my_resp[y].resp != NULL)
		free(my_resp[y].resp);
	free(my_resp);
    }
    return PAM_CONV_ERR;
}

/* END FUNCTION: saslauthd_pam_conv */

/* FUNCTION: auth_pam */

char *					/* R: allocated response string */
auth_pam (
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service,			/* I: service name */
  const char *realm __attribute__((unused))
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    pam_appdata my_appdata;		/* application specific data */
    struct pam_conv my_conv;		/* pam conversion data */
    pam_handle_t *pamh;			/* pointer to PAM handle */
    int rc;				/* return code holder */
    /* END VARIABLES */

    my_appdata.login = login;
    my_appdata.password = password;
    my_appdata.pamh = NULL;

    my_conv.conv = saslauthd_pam_conv;
    my_conv.appdata_ptr = &my_appdata;

    rc = pam_start(service, login, &my_conv, &pamh);
    if (rc != PAM_SUCCESS) {
	syslog(LOG_DEBUG, "DEBUG: auth_pam: pam_start failed: %s",
	       pam_strerror(pamh, rc));
	RETURN("NO PAM start error");
    }

    my_appdata.pamh = pamh;

    rc = pam_authenticate(pamh, PAM_SILENT);
    if (rc != PAM_SUCCESS) {
	syslog(LOG_DEBUG, "DEBUG: auth_pam: pam_authenticate failed: %s",
	       pam_strerror(pamh, rc));
	pam_end(pamh, rc);
	RETURN("NO PAM auth error");
    }

    rc = pam_acct_mgmt(pamh, PAM_SILENT);
    if (rc != PAM_SUCCESS) {
	syslog(LOG_DEBUG, "DEBUG: auth_pam: pam_acct_mgmt failed: %s",
	       pam_strerror(pamh, rc));
	pam_end(pamh, rc);
	RETURN("NO PAM acct error");
    }

    pam_end(pamh, PAM_SUCCESS);
    RETURN("OK");
}

/* END FUNCTION: auth_pam */

#else /* !AUTH_PAM */

char *
auth_pam(
  const char *login __attribute__((unused)),
  const char *password __attribute__((unused)),
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  )
{
    return NULL;
}

#endif /* !AUTH_PAM */

/* END MODULE: auth_pam */
