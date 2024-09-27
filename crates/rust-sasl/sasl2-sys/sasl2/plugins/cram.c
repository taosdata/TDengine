/* CRAM-MD5 SASL plugin
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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifndef macintosh
#include <sys/stat.h>
#endif
#include <fcntl.h>

#ifdef HAVE_TIME_H
#include <time.h>
#endif

#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>

#include "plugin_common.h"

#ifdef macintosh
#include <sasl_cram_plugin_decl.h>
#endif

/*****************************  Common Section  *****************************/

/* convert a string of 8bit chars to it's representation in hex
 * using lowercase letters
 */
static char *convert16(unsigned char *in, int inlen, const sasl_utils_t *utils)
{
    static char hex[]="0123456789abcdef";
    int lup;
    char *out;

    out = utils->malloc(inlen*2+1);
    if (out == NULL) return NULL;
    
    for (lup=0; lup < inlen; lup++) {
	out[lup*2] = hex[in[lup] >> 4];
	out[lup*2+1] = hex[in[lup] & 15];
    }

    out[lup*2] = 0;
    return out;
}


/*****************************  Server Section  *****************************/

typedef struct server_context {
    int state;

    char *challenge;
} server_context_t;

static int
crammd5_server_mech_new(void *glob_context __attribute__((unused)),
			sasl_server_params_t *sparams,
			const char *challenge __attribute__((unused)),
			unsigned challen __attribute__((unused)),
			void **conn_context)
{
    server_context_t *text;
    
    /* holds state are in */
    text = sparams->utils->malloc(sizeof(server_context_t));
    if (text == NULL) {
	MEMERROR( sparams->utils );
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(server_context_t));
    
    text->state = 1;
    
    *conn_context = text;
    
    return SASL_OK;
}

/*
 * Returns the current time (or part of it) in string form
 *  maximum length=15
 */
static char *gettime(sasl_server_params_t *sparams)
{
    char *ret;
    time_t t;
    
    t=time(NULL);
    ret= sparams->utils->malloc(15);
    if (ret==NULL) return NULL;
    
    /* the bottom bits are really the only random ones so if
       we overflow we don't want to loose them */
    snprintf(ret,15,"%lu",t%(0xFFFFFF));
    
    return ret;
}

static char *randomdigits(sasl_server_params_t *sparams)
{
    unsigned int num;
    char *ret;
    unsigned char temp[5]; /* random 32-bit number */
    
    sparams->utils->rand(sparams->utils->rpool,(char *) temp,4);
    num=(temp[0] * 256 * 256 * 256) +
	(temp[1] * 256 * 256) +
	(temp[2] * 256) +
	(temp[3] );
    
    ret = sparams->utils->malloc(15); /* there's no way an unsigned can be longer than this right? */
    if (ret == NULL) return NULL;
    sprintf(ret, "%u", num);
    
    return ret;
}

static int
crammd5_server_mech_step1(server_context_t *text,
			  sasl_server_params_t *sparams,
			  const char *clientin __attribute__((unused)),
			  unsigned clientinlen,
			  const char **serverout,
			  unsigned *serveroutlen,
			  sasl_out_params_t *oparams __attribute__((unused)))
{
    char *time, *randdigits;
	    
    /* we shouldn't have received anything */
    if (clientinlen != 0) {
	SETERROR(sparams->utils, "CRAM-MD5 does not accept inital data");
	return SASL_BADPROT;
    }
    
    /* get time and a random number for the nonce */
    time = gettime(sparams);
    randdigits = randomdigits(sparams);
    if ((time == NULL) || (randdigits == NULL)) {
	MEMERROR( sparams->utils );
	return SASL_NOMEM;
    }
    
    /* allocate some space for the challenge */
    text->challenge = sparams->utils->malloc(200 + 1);
    if (text->challenge == NULL) {
	MEMERROR(sparams->utils);
	return SASL_NOMEM;
    }
    
    /* create the challenge */
    snprintf(text->challenge, 200, "<%s.%s@%s>", randdigits, time,
	     sparams->serverFQDN);
    
    *serverout = text->challenge;
    *serveroutlen = (unsigned) strlen(text->challenge);
    
    /* free stuff */
    sparams->utils->free(time);    
    sparams->utils->free(randdigits);    
    
    text->state = 2;
    
    return SASL_CONTINUE;
}
    
static int
crammd5_server_mech_step2(server_context_t *text,
			  sasl_server_params_t *sparams,
			  const char *clientin,
			  unsigned clientinlen,
			  const char **serverout __attribute__((unused)),
			  unsigned *serveroutlen __attribute__((unused)),
			  sasl_out_params_t *oparams)
{
    char *userid = NULL;
    sasl_secret_t *sec = NULL;
    int pos;
    size_t len;
    int result = SASL_FAIL;
    const char *password_request[] = { SASL_AUX_PASSWORD,
#if defined(OBSOLETE_CRAM_ATTR)
				       "*cmusaslsecretCRAM-MD5",
#endif
				       NULL };
    struct propval auxprop_values[3];
    HMAC_MD5_CTX tmphmac;
    HMAC_MD5_STATE md5state;
    int clear_md5state = 0;
    char *digest_str = NULL;
    UINT4 digest[4];
    
    /* extract userid; everything before last space */
    pos = clientinlen-1;
    while ((pos > 0) && (clientin[pos] != ' ')) pos--;
    
    if (pos <= 0) {
	SETERROR( sparams->utils,"need authentication name");
	return SASL_BADPROT;
    }
    
    userid = (char *) sparams->utils->malloc(pos+1);
    if (userid == NULL) {
	MEMERROR( sparams->utils);
	return SASL_NOMEM;
    }
    
    /* copy authstr out */
    memcpy(userid, clientin, pos);
    userid[pos] = '\0';
    
    result = sparams->utils->prop_request(sparams->propctx, password_request);
    if (result != SASL_OK) goto done;
    
    /* this will trigger the getting of the aux properties */
    result = sparams->canon_user(sparams->utils->conn,
				 userid, 0, SASL_CU_AUTHID | SASL_CU_AUTHZID,
				 oparams);
    if (result != SASL_OK) goto done;
    
    result = sparams->utils->prop_getnames(sparams->propctx,
					   password_request,
					   auxprop_values);
    if (result < 0 ||
	((!auxprop_values[0].name || !auxprop_values[0].values)
#if defined(OBSOLETE_CRAM_ATTR)
	  && (!auxprop_values[1].name || !auxprop_values[1].values)
#endif
	)) {
	/* We didn't find this username */
	sparams->utils->seterror(sparams->utils->conn,0,
				 "no secret in database");
	result = sparams->transition ? SASL_TRANS : SASL_NOUSER;
	goto done;
    }
    
    if (auxprop_values[0].name && auxprop_values[0].values) {
	len = strlen(auxprop_values[0].values[0]);
	if (len == 0) {
	    sparams->utils->seterror(sparams->utils->conn,0,
				     "empty secret");
	    result = SASL_FAIL;
	    goto done;
	}
	
	sec = sparams->utils->malloc(sizeof(sasl_secret_t) + len);
	if (!sec) goto done;
	
	sec->len = (unsigned) len;
	strncpy((char *)sec->data, auxprop_values[0].values[0], len + 1);   
	
	clear_md5state = 1;
	/* Do precalculation on plaintext secret */
	sparams->utils->hmac_md5_precalc(&md5state, /* OUT */
					 sec->data,
					 sec->len);
#if defined(OBSOLETE_CRAM_ATTR)
    } else if (auxprop_values[1].name && auxprop_values[1].values) {
	/* We have a precomputed secret */
	memcpy(&md5state, auxprop_values[1].values[0],
	       sizeof(HMAC_MD5_STATE));
#endif
    } else {
	sparams->utils->seterror(sparams->utils->conn, 0,
				 "Have neither type of secret");
	return SASL_FAIL;
    }
    
    /* erase the plaintext password */
    sparams->utils->prop_erase(sparams->propctx, password_request[0]);

    /* ok this is annoying:
       so we have this half-way hmac transform instead of the plaintext
       that means we half to:
       -import it back into a md5 context
       -do an md5update with the nonce 
       -finalize it
    */
    sparams->utils->hmac_md5_import(&tmphmac, (HMAC_MD5_STATE *) &md5state);
    sparams->utils->MD5Update(&(tmphmac.ictx),
			      (const unsigned char *) text->challenge,
			      (unsigned) strlen(text->challenge));
    sparams->utils->hmac_md5_final((unsigned char *) &digest, &tmphmac);
    
    /* convert to base 16 with lower case letters */
    digest_str = convert16((unsigned char *) digest, 16, sparams->utils);
    
    /* if same then verified 
     *  - we know digest_str is null terminated but clientin might not be
     *  - verify the length of clientin anyway!
     */
    len = strlen(digest_str);
    if (clientinlen-pos-1 < len ||
	strncmp(digest_str, clientin+pos+1, len) != 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
				 "incorrect digest response");
	result = SASL_BADAUTH;
	goto done;
    }
    
    /* set oparams */
    oparams->doneflag = 1;
    oparams->mech_ssf = 0;
    oparams->maxoutbuf = 0;
    oparams->encode_context = NULL;
    oparams->encode = NULL;
    oparams->decode_context = NULL;
    oparams->decode = NULL;
    oparams->param_version = 0;
    
    result = SASL_OK;
    
  done:
    if (userid) sparams->utils->free(userid);
    if (sec) _plug_free_secret(sparams->utils, &sec);

    if (digest_str) sparams->utils->free(digest_str);
    if (clear_md5state) memset(&md5state, 0, sizeof(md5state));
    
    return result;
}

static int crammd5_server_mech_step(void *conn_context,
				    sasl_server_params_t *sparams,
				    const char *clientin,
				    unsigned clientinlen,
				    const char **serverout,
				    unsigned *serveroutlen,
				    sasl_out_params_t *oparams)
{
    server_context_t *text = (server_context_t *) conn_context;
    
    *serverout = NULL;
    *serveroutlen = 0;

    if (text == NULL) {
	return SASL_BADPROT;
    }

    /* this should be well more than is ever needed */
    if (clientinlen > 1024) {
	SETERROR(sparams->utils, "CRAM-MD5 input longer than 1024 bytes");
	return SASL_BADPROT;
    }
    
    switch (text->state) {

    case 1:
	return crammd5_server_mech_step1(text, sparams,
					 clientin, clientinlen,
					 serverout, serveroutlen,
					 oparams);

    case 2:
	return crammd5_server_mech_step2(text, sparams,
					 clientin, clientinlen,
					 serverout, serveroutlen,
					 oparams);

    default: /* should never get here */
	sparams->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid CRAM-MD5 server step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static void crammd5_server_mech_dispose(void *conn_context,
					const sasl_utils_t *utils)
{
    server_context_t *text = (server_context_t *) conn_context;
    
    if (!text) return;
    
    if (text->challenge) _plug_free_string(utils,&(text->challenge));
    
    utils->free(text);
}

static sasl_server_plug_t crammd5_server_plugins[] = 
{
    {
	"CRAM-MD5",			/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS,		/* security_flags */
	SASL_FEAT_SERVER_FIRST,		/* features */
	NULL,				/* glob_context */
	&crammd5_server_mech_new,	/* mech_new */
	&crammd5_server_mech_step,	/* mech_step */
	&crammd5_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech avail */
	NULL				/* spare */
    }
};

int crammd5_server_plug_init(const sasl_utils_t *utils,
			     int maxversion,
			     int *out_version,
			     sasl_server_plug_t **pluglist,
			     int *plugcount)
{
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	SETERROR( utils, "CRAM version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = crammd5_server_plugins;
    *plugcount = 1;  
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

typedef struct client_context {
    char *out_buf;
    unsigned out_buf_len;
} client_context_t;

static int crammd5_client_mech_new(void *glob_context __attribute__((unused)), 
				   sasl_client_params_t *params,
				   void **conn_context)
{
    client_context_t *text;
    
    /* holds state are in */
    text = params->utils->malloc(sizeof(client_context_t));
    if (text == NULL) {
	MEMERROR(params->utils);
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(client_context_t));

    *conn_context = text;
    
    return SASL_OK;
}

static char *make_hashed(sasl_secret_t *sec, char *nonce, int noncelen, 
			 const sasl_utils_t *utils)
{
    unsigned char digest[24];  
    char *in16;
    
    if (sec == NULL) return NULL;
    
    /* do the hmac md5 hash output 128 bits */
    utils->hmac_md5((unsigned char *) nonce, noncelen,
		    sec->data, sec->len, digest);
    
    /* convert that to hex form */
    in16 = convert16(digest, 16, utils);
    if (in16 == NULL) return NULL;
    
    return in16;
}

static int crammd5_client_mech_step(void *conn_context,
				    sasl_client_params_t *params,
				    const char *serverin,
				    unsigned serverinlen,
				    sasl_interact_t **prompt_need,
				    const char **clientout,
				    unsigned *clientoutlen,
				    sasl_out_params_t *oparams)
{
    client_context_t *text = (client_context_t *) conn_context;
    const char *authid = NULL;
    sasl_secret_t *password = NULL;
    unsigned int free_password = 0; /* set if we need to free password */
    int auth_result = SASL_OK;
    int pass_result = SASL_OK;
    int result;
    size_t maxsize;
    char *in16 = NULL;

    *clientout = NULL;
    *clientoutlen = 0;
    
    /* First check for absurd lengths */
    if (serverinlen > 1024) {
	params->utils->seterror(params->utils->conn, 0,
				"CRAM-MD5 input longer than 1024 bytes");
	return SASL_BADPROT;
    }
    
    /* check if sec layer strong enough */
    if (params->props.min_ssf > params->external_ssf) {
	SETERROR( params->utils, "SSF requested of CRAM-MD5 plugin");
	return SASL_TOOWEAK;
    }
    
    /* try to get the userid */
    if (oparams->authid == NULL) {
	auth_result=_plug_get_authid(params->utils, &authid, prompt_need);
	
	if ((auth_result != SASL_OK) && (auth_result != SASL_INTERACT))
	    return auth_result;
    }
    
    /* try to get the password */
    if (password == NULL) {
	pass_result=_plug_get_password(params->utils, &password,
				       &free_password, prompt_need);
	
	if ((pass_result != SASL_OK) && (pass_result != SASL_INTERACT))
	    return pass_result;
    }
    
    /* free prompts we got */
    if (prompt_need && *prompt_need) {
	params->utils->free(*prompt_need);
	*prompt_need = NULL;
    }
    
    /* if there are prompts not filled in */
    if ((auth_result == SASL_INTERACT) || (pass_result == SASL_INTERACT)) {
	/* make the prompt list */
	result =
	    _plug_make_prompts(params->utils, prompt_need,
			       NULL, NULL,
			       auth_result == SASL_INTERACT ?
			       "Please enter your authentication name" : NULL,
			       NULL,
			       pass_result == SASL_INTERACT ?
			       "Please enter your password" : NULL, NULL,
			       NULL, NULL, NULL,
			       NULL, NULL, NULL);
	if (result != SASL_OK) goto cleanup;
	
	return SASL_INTERACT;
    }
    
    if (!password) {
	PARAMERROR(params->utils);
	return SASL_BADPARAM;
    }
    
    result = params->canon_user(params->utils->conn, authid, 0,
				SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
    if (result != SASL_OK) goto cleanup;
    
    /*
     * username SP digest (keyed md5 where key is passwd)
     */
    
    in16 = make_hashed(password, (char *) serverin, serverinlen,
		       params->utils);
    
    if (in16 == NULL) {
	SETERROR(params->utils, "whoops, make_hashed failed us this time");
	result = SASL_FAIL;
	goto cleanup;
    }
    
    maxsize = 32+1+strlen(oparams->authid)+30;
    result = _plug_buf_alloc(params->utils, &(text->out_buf),
			     &(text->out_buf_len), (unsigned) maxsize);
    if (result != SASL_OK) goto cleanup;
    
    snprintf(text->out_buf, maxsize, "%s %s", oparams->authid, in16);
    
    *clientout = text->out_buf;
    *clientoutlen = (unsigned) strlen(*clientout);
    
    /* set oparams */
    oparams->doneflag = 1;
    oparams->mech_ssf = 0;
    oparams->maxoutbuf = 0;
    oparams->encode_context = NULL;
    oparams->encode = NULL;
    oparams->decode_context = NULL;
    oparams->decode = NULL;
    oparams->param_version = 0;
    
    result = SASL_OK;

  cleanup:
    /* get rid of private information */
    if (in16) _plug_free_string(params->utils, &in16);
    
    /* get rid of all sensitive info */
    if (free_password) _plug_free_secret(params-> utils, &password);

    return result;
}

static void crammd5_client_mech_dispose(void *conn_context,
					const sasl_utils_t *utils)
{
    client_context_t *text = (client_context_t *) conn_context;
    
    if (!text) return;
    
    if (text->out_buf) utils->free(text->out_buf);
    
    utils->free(text);
}

static sasl_client_plug_t crammd5_client_plugins[] = 
{
    {
	"CRAM-MD5",			/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS,		/* security_flags */
	SASL_FEAT_SERVER_FIRST,		/* features */
	NULL,				/* required_prompts */
	NULL,				/* glob_context */
	&crammd5_client_mech_new,	/* mech_new */
	&crammd5_client_mech_step,	/* mech_step */
	&crammd5_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int crammd5_client_plug_init(const sasl_utils_t *utils,
			     int maxversion,
			     int *out_version,
			     sasl_client_plug_t **pluglist,
			     int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR( utils, "CRAM version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = crammd5_client_plugins;
    *plugcount = 1;
    
    return SASL_OK;
}
