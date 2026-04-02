/* SASL server API implementation
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
#include <stdlib.h>
#include <limits.h>
#include <ctype.h>
#include <string.h>
#include <sasl.h>
#include <saslplug.h>
#include "saslint.h"

#include "../common/plugin_common.h"

/*****************************  Common Section  *****************************/

/*****************************  Server Section  *****************************/

static int
external_server_mech_new(void *glob_context __attribute__((unused)),
			 sasl_server_params_t *sparams,
			 const char *challenge __attribute__((unused)),
			 unsigned challen __attribute__((unused)),
			 void **conn_context)
{
    if (!conn_context
	|| !sparams
	|| !sparams->utils
	|| !sparams->utils->conn)
	return SASL_BADPARAM;
    
    if (!sparams->utils->conn->external.auth_id)
	return SASL_NOMECH;
    
    *conn_context = NULL;

    return SASL_OK;
}

static int
external_server_mech_step(void *conn_context __attribute__((unused)),
			  sasl_server_params_t *sparams,
			  const char *clientin,
			  unsigned clientinlen,
			  const char **serverout,
			  unsigned *serveroutlen,
			  sasl_out_params_t *oparams)
{
    int result;
    
    if (!sparams
	|| !sparams->utils
	|| !sparams->utils->conn
	|| !sparams->utils->getcallback
	|| !serverout
	|| !serveroutlen
	|| !oparams)
	return SASL_BADPARAM;
    
    if (!sparams->utils->conn->external.auth_id)
	return SASL_BADPROT;
    
    /* xxx arbitrary limit here */
    if (clientinlen > 16384) return SASL_BADPROT;

    if ((sparams->props.security_flags & SASL_SEC_NOANONYMOUS) &&
	(!strcmp(sparams->utils->conn->external.auth_id, "anonymous"))) {
	sasl_seterror(sparams->utils->conn,0,"anonymous login not allowed");
	return SASL_NOAUTHZ;
    }
    
    *serverout = NULL;
    *serveroutlen = 0;
    
    if (!clientin) {
	/* No initial data; we're in a protocol which doesn't support it.
	 * So we let the server app know that we need some... */
	return SASL_CONTINUE;
    }
    
    if (clientinlen) {		/* if we have a non-zero authorization id */
	/* The user's trying to authorize as someone they didn't
	 * authenticate as */
	result = sparams->canon_user(sparams->utils->conn,
				     clientin, clientinlen,
				     SASL_CU_AUTHZID, oparams);
	if(result != SASL_OK) return result;
	
	result = sparams->canon_user(sparams->utils->conn,
				     sparams->utils->conn->external.auth_id, 0,
				     SASL_CU_AUTHID | SASL_CU_EXTERNALLY_VERIFIED, oparams);
    } else {
	result = sparams->canon_user(sparams->utils->conn,
				     sparams->utils->conn->external.auth_id, 0,
				     SASL_CU_AUTHID | SASL_CU_EXTERNALLY_VERIFIED | SASL_CU_AUTHZID, oparams);
    }
    
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

static int
external_server_mech_avail(void *glob_context __attribute__((unused)),
			   sasl_server_params_t *sparams,
			   void **conn_context __attribute__((unused)))
{
    if (!sparams->utils->conn->external.auth_id) {
	/* Return Temporary Failure */
	return SASL_NOTDONE;
    }
    
    return SASL_OK;
}

static sasl_server_plug_t external_server_plugins[] =
{
    {
	"EXTERNAL",			/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NODICTIONARY,	/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	NULL,				/* glob_context */
	&external_server_mech_new,	/* mech_new */
	&external_server_mech_step,	/* mech_step */
	NULL,				/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	&external_server_mech_avail,	/* mech_avail */
	NULL				/* spare */
    }
};

int external_server_plug_init(const sasl_utils_t *utils,
			      int max_version,
			      int *out_version,
			      sasl_server_plug_t **pluglist,
			      int *plugcount)
{
    if (!out_version || !pluglist || !plugcount)
	return SASL_BADPARAM;
    
    if (max_version != SASL_SERVER_PLUG_VERSION) {
	SETERROR( utils, "EXTERNAL version mismatch" );
	return SASL_BADVERS;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = external_server_plugins;
    *plugcount = 1;
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

typedef struct client_context 
{
    char *out_buf;
    size_t out_buf_len;
} client_context_t;

static int external_client_mech_new(void *glob_context __attribute__((unused)),
				    sasl_client_params_t *params,
				    void **conn_context)
{
    client_context_t *text;
    
    if (!params
	|| !params->utils
	|| !params->utils->conn
	|| !conn_context)
	return SASL_BADPARAM;
    
    if (!params->utils->conn->external.auth_id)
	return SASL_NOMECH;
    
    text = sasl_ALLOC(sizeof(client_context_t));
    if(!text) return SASL_NOMEM;
    
    memset(text, 0, sizeof(client_context_t));
    
    *conn_context = text;

    return SASL_OK;
}

static int
external_client_mech_step(void *conn_context,
			  sasl_client_params_t *params,
			  const char *serverin __attribute__((unused)),
			  unsigned serverinlen,
			  sasl_interact_t **prompt_need,
			  const char **clientout,
			  unsigned *clientoutlen,
			  sasl_out_params_t *oparams)
{
    client_context_t *text = (client_context_t *)conn_context;
    const char *user = NULL;
    int user_result = SASL_OK;
    int result;
    
    if (!params
	|| !params->utils
	|| !params->utils->conn
	|| !params->utils->getcallback
	|| !clientout
	|| !clientoutlen
	|| !oparams)
	return SASL_BADPARAM;
    
    if (!params->utils->conn->external.auth_id)
	return SASL_BADPROT;
    
    if (serverinlen != 0)
	return SASL_BADPROT;
    
    *clientout = NULL;
    *clientoutlen = 0;
    
    /* try to get the userid */
    if (user == NULL) {
	user_result = _plug_get_userid(params->utils, &user, prompt_need);
	
	if ((user_result != SASL_OK) && (user_result != SASL_INTERACT))
	    return user_result;
    }
    
    /* free prompts we got */
    if (prompt_need && *prompt_need) {
	params->utils->free(*prompt_need);
	*prompt_need = NULL;
    }
    
    /* if there are prompts not filled in */
    if (user_result == SASL_INTERACT) {
	/* make the prompt list */
	int result =
	    _plug_make_prompts(params->utils, prompt_need,
			       "Please enter your authorization name",
			       "",
			       NULL, NULL,
			       NULL, NULL,
			       NULL, NULL, NULL,
			       NULL, NULL, NULL);
	if (result != SASL_OK) return result;
	
	return SASL_INTERACT;
    }
    
    *clientoutlen = user ? (unsigned) strlen(user) : 0;
    
    result = _buf_alloc(&text->out_buf, &text->out_buf_len, *clientoutlen + 1);
    
    if (result != SASL_OK) return result;
    
    if (user && *user) {
	result = params->canon_user(params->utils->conn,
				    user, 0, SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) return result;
	
	result = params->canon_user(params->utils->conn,
				    params->utils->conn->external.auth_id, 0,
				    SASL_CU_AUTHID, oparams);
	if (result != SASL_OK) return result;
	
	memcpy(text->out_buf, user, *clientoutlen);
    } else {
	result = params->canon_user(params->utils->conn,
				    params->utils->conn->external.auth_id, 0,
				    SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) return result;
    }
    
    text->out_buf[*clientoutlen] = '\0';
    
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

static void
external_client_mech_dispose(void *conn_context,
			     const sasl_utils_t *utils __attribute__((unused))) 
{
    client_context_t *text = (client_context_t *) conn_context;
    
    if (!text) return;
    
    if(text->out_buf) sasl_FREE(text->out_buf);
    
    sasl_FREE(text);
}

static const unsigned long external_required_prompts[] = {
    SASL_CB_LIST_END
};

static sasl_client_plug_t external_client_plugins[] =
{
    {
	"EXTERNAL",			/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NODICTIONARY,	/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	external_required_prompts,	/* required_prompts */
	NULL,				/* glob_context */
	&external_client_mech_new,	/* mech_new */
	&external_client_mech_step,	/* mech_step */
	&external_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int external_client_plug_init(const sasl_utils_t *utils,
			      int max_version,
			      int *out_version,
			      sasl_client_plug_t **pluglist,
			      int *plugcount)
{
    if (!utils || !out_version || !pluglist || !plugcount)
	return SASL_BADPARAM;
    
    if (max_version != SASL_CLIENT_PLUG_VERSION) {
	SETERROR( utils, "EXTERNAL version mismatch" );
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = external_client_plugins;
    *plugcount = 1;
    
    return SASL_OK;
}
