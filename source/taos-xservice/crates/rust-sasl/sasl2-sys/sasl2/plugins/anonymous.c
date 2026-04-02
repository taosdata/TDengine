/* Anonymous SASL plugin
 * Rob Siemborski
 * Tim Martin 
 */
/* 
 * Copyright (c) 1998-2016 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact  
 *      Carnegie Mellon University
 *      Center for Technology Transfer and Enterprise Creation
 *      4615 Forbes Avenue
 *      Suite 302
 *      Pittsburgh, PA  15213
 *      (412) 268-7393, fax: (412) 268-7395
 *      innovation@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <config.h>
#include <stdio.h>
#include <string.h> 
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <sasl.h>
#include <saslplug.h>

#include "plugin_common.h"

#ifdef macintosh 
#include <sasl_anonymous_plugin_decl.h> 
#endif 

/*****************************  Common Section  *****************************/

static const char anonymous_id[] = "anonymous";

/*****************************  Server Section  *****************************/

static int
anonymous_server_mech_new(void *glob_context __attribute__((unused)),
			  sasl_server_params_t *sparams,
			  const char *challenge __attribute__((unused)),
			  unsigned challen __attribute__((unused)),
			  void **conn_context)
{
    /* holds state are in */
    if (!conn_context) {
	PARAMERROR( sparams->utils );
	return SASL_BADPARAM;
    }
    
    *conn_context = NULL;
    
    return SASL_OK;
}

static int
anonymous_server_mech_step(void *conn_context __attribute__((unused)),
			   sasl_server_params_t *sparams,
			   const char *clientin,
			   unsigned clientinlen,
			   const char **serverout,
			   unsigned *serveroutlen,
			   sasl_out_params_t *oparams)
{
    char *clientdata;
    int result;
    
    if (!sparams
	|| !serverout
	|| !serveroutlen
	|| !oparams) {
	if (sparams) PARAMERROR( sparams->utils );
	return SASL_BADPARAM;
    }
    
    *serverout = NULL;
    *serveroutlen = 0;
    
    if (!clientin) {
	return SASL_CONTINUE;
    }
    
    /* We force a truncation 255 characters (specified by RFC 2245) */
    if (clientinlen > 255) clientinlen = 255;
    
    /* NULL-terminate the clientin... */
    clientdata = sparams->utils->malloc(clientinlen + 1);
    if (!clientdata) {
	MEMERROR(sparams->utils);
	return SASL_NOMEM;
    }
    
    strncpy(clientdata, clientin, clientinlen);
    clientdata[clientinlen] = '\0';
    
    sparams->utils->log(sparams->utils->conn,
			SASL_LOG_NOTE,
			"ANONYMOUS login: \"%s\"",
			clientdata);
    
    if (clientdata != clientin) 
	sparams->utils->free(clientdata);
    
    result = sparams->canon_user(sparams->utils->conn,
				 anonymous_id, 0,
				 SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);

    if (result != SASL_OK) return result;
    
    /* set oparams */
    oparams->doneflag = 1;
    oparams->mech_ssf = 0;
    oparams->maxoutbuf = 0;
    oparams->encode_context = NULL;
    oparams->encode = NULL;
    oparams->decode_context = NULL;
    oparams->decode = NULL;
    oparams->param_version = 0;
    
    return SASL_OK;
}

static sasl_server_plug_t anonymous_server_plugins[] = 
{
    {
	"ANONYMOUS",			/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT,		/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_DONTUSE_USERPASSWD,	/* features */
	NULL,				/* glob_context */
	&anonymous_server_mech_new,	/* mech_new */
	&anonymous_server_mech_step,	/* mech_step */
	NULL,				/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech_avail */
	NULL                        	/* spare */
    }
};

int anonymous_server_plug_init(const sasl_utils_t *utils,
			       int maxversion,
			       int *out_version,
			       sasl_server_plug_t **pluglist,
			       int *plugcount)
{
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	SETERROR( utils, "ANONYMOUS version mismatch" );
	return SASL_BADVERS;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = anonymous_server_plugins;
    *plugcount = 1;  
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

typedef struct client_context {
    char *out_buf;
    unsigned out_buf_len;
} client_context_t;

static int
anonymous_client_mech_new(void *glob_context __attribute__((unused)),
			  sasl_client_params_t *cparams,
			  void **conn_context)
{
    client_context_t *text;
    
    if (!conn_context) {
	PARAMERROR(cparams->utils);
	return SASL_BADPARAM;
    }
    
    /* holds state are in */
    text = cparams->utils->malloc(sizeof(client_context_t));
    if (text == NULL) {
	MEMERROR(cparams->utils);
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(client_context_t));
    
    *conn_context = text;
    
    return SASL_OK;
}

static int
anonymous_client_mech_step(void *conn_context,
			   sasl_client_params_t *cparams,
			   const char *serverin __attribute__((unused)),
			   unsigned serverinlen,
			   sasl_interact_t **prompt_need,
			   const char **clientout,
			   unsigned *clientoutlen,
			   sasl_out_params_t *oparams)
{
    client_context_t *text = (client_context_t *) conn_context;
    size_t userlen;
    char hostname[256];
    const char *user = NULL;
    int user_result = SASL_OK;
    int result;
    
    if (!cparams
	|| !clientout
	|| !clientoutlen
	|| !oparams) {
	if (cparams) PARAMERROR( cparams->utils );
	return SASL_BADPARAM;
    }
    
    *clientout = NULL;
    *clientoutlen = 0;
    
    if (serverinlen != 0) {
	SETERROR( cparams->utils,
		  "Nonzero serverinlen in ANONYMOUS continue_step" );
	return SASL_BADPROT;
    }
    
    /* check if sec layer strong enough */
    if (cparams->props.min_ssf > cparams->external_ssf) {
	SETERROR( cparams->utils, "SSF requested of ANONYMOUS plugin");
	return SASL_TOOWEAK;
    }
    
    /* try to get the trace info */
    if (user == NULL) {
	user_result = _plug_get_userid(cparams->utils, &user, prompt_need);
	
	if ((user_result != SASL_OK) && (user_result != SASL_INTERACT)) {
	    return user_result;
	}
    }
    
    /* free prompts we got */
    if (prompt_need && *prompt_need) {
	cparams->utils->free(*prompt_need);
	*prompt_need = NULL;
    }
    
    /* if there are prompts not filled in */
    if (user_result == SASL_INTERACT) {
	/* make the prompt list */
	result =
	    _plug_make_prompts(cparams->utils, prompt_need,
			       "Please enter anonymous identification",
			       "",
			       NULL, NULL,
			       NULL, NULL,
			       NULL, NULL, NULL,
			       NULL, NULL, NULL);
	if (result != SASL_OK) return result;
	
	return SASL_INTERACT;
    }
    
    if (!user || !*user) {
	user = anonymous_id;
    }
    userlen = strlen(user);
    
    result = cparams->canon_user(cparams->utils->conn,
				 anonymous_id, 0,
				 SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
    if (result != SASL_OK) return result;
    
    memset(hostname, 0, sizeof(hostname));
    gethostname(hostname, sizeof(hostname));
    hostname[sizeof(hostname)-1] = '\0';
    
    *clientoutlen = (unsigned) (userlen + strlen(hostname) + 1);
    
    result = _plug_buf_alloc(cparams->utils, &text->out_buf,
			     &text->out_buf_len, *clientoutlen);
    
    if (result != SASL_OK) return result;
    
    strcpy(text->out_buf, user);
    text->out_buf[userlen] = '@';
    /* use memcpy() instead of strcpy() so we don't add the NUL */
    memcpy(text->out_buf + userlen + 1, hostname, strlen(hostname));
    
    *clientout = text->out_buf;
    
    /* set oparams */
    oparams->doneflag = 1;
    oparams->mech_ssf = 0;
    oparams->maxoutbuf = 0;
    oparams->encode_context = NULL;
    oparams->encode = NULL;
    oparams->decode_context = NULL;
    oparams->decode = NULL;
    oparams->param_version = 0;
    
    return SASL_OK;
}

static void anonymous_client_dispose(void *conn_context,
				     const sasl_utils_t *utils)
{
    client_context_t *text = (client_context_t *) conn_context;
    
    if(!text) return;
    
    if (text->out_buf) utils->free(text->out_buf);
    
    utils->free(text);
}

static const unsigned long anonymous_required_prompts[] = {
    SASL_CB_LIST_END
};

static sasl_client_plug_t anonymous_client_plugins[] = 
{
    {
	"ANONYMOUS",			/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT,		/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST,	/* features */
	anonymous_required_prompts,	/* required_prompts */
	NULL,				/* glob_context */
	&anonymous_client_mech_new, 	/* mech_new */
	&anonymous_client_mech_step,	/* mech_step */
	&anonymous_client_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL                        	/* spare */
    }
};

int anonymous_client_plug_init(const sasl_utils_t *utils,
			       int maxversion,
			       int *out_version,
			       sasl_client_plug_t **pluglist,
			       int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR( utils, "ANONYMOUS version mismatch" );
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = anonymous_client_plugins;
    *plugcount = 1;
    
    return SASL_OK;
}
