/* GSSAPI SASL plugin
 * Leif Johansson
 * Rob Siemborski (SASL v2 Conversion)
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

#ifdef HAVE_GSSAPI_H
#include <gssapi.h>
#elif defined(HAVE_GSSAPI_GSSAPI_H)
#include <gssapi/gssapi.h>
#endif

#ifdef HAVE_GSSAPI_GSSAPI_KRB5_H
#include <gssapi/gssapi_krb5.h>
#endif
#ifdef HAVE_GSSAPI_GSSAPI_EXT_H
#include <gssapi/gssapi_ext.h>
#endif

#ifdef WIN32
#  include <winsock2.h>

#  ifndef R_OK
#    define R_OK 04
#  endif
/* we also need io.h for access() prototype */
#  include <io.h>
#else
#  include <sys/param.h>
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <arpa/inet.h>
#  include <netdb.h>
#endif /* WIN32 */
#include <fcntl.h>
#include <stdio.h>
#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>

#include "plugin_common.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <errno.h>
#include <assert.h>

/*****************************  Common Section  *****************************/

static const char * GSSAPI_BLANK_STRING = "";

static gss_OID_desc gss_spnego_oid = { 6, (void *) "\x2b\x06\x01\x05\x05\x02" };

#if !defined(HAVE_GSS_C_NT_HOSTBASED_SERVICE) && !defined(GSS_C_NT_HOSTBASED_SERVICE)
extern gss_OID gss_nt_service_name;
#define GSS_C_NT_HOSTBASED_SERVICE gss_nt_service_name
#endif

#ifdef WANT_KERBEROS5_3DES
/* Check if CyberSafe flag is defined */
#ifdef CSF_GSS_C_DES3_FLAG
#define K5_MAX_SSF	112
#define K5_MIN_SSF	112
#endif

/* Heimdal and MIT use the following */
#ifdef GSS_KRB5_CONF_C_QOP_DES3_KD
#define K5_MAX_SSF	112
#define K5_MIN_SSF	112
#endif

#endif

#ifndef K5_MAX_SSF
/* All modern Kerberos implementations support AES */
#define K5_MAX_SSF	256
#endif

/* All Kerberos implementations support DES */
#ifndef K5_MIN_SSF
#define K5_MIN_SSF      56
#endif

/* GSSAPI SASL Mechanism by Leif Johansson <leifj@matematik.su.se>
 * inspired by the kerberos mechanism and the gssapi_server and
 * gssapi_client from the heimdal distribution by Assar Westerlund
 * <assar@sics.se> and Johan Danielsson <joda@pdc.kth.se>. 
 * See the configure.in file for details on dependencies.
 *
 * Important contributions from Sam Hartman <hartmans@fundsxpress.com>.
 *
 * This code was tested with the following distributions of Kerberos:
 * Heimdal (http://www.pdc.kth.se/heimdal), MIT (http://web.mit.edu/kerberos/www/)
 * CyberSafe (http://www.cybersafe.com/) and SEAM.
 */

#ifdef GSS_USE_MUTEXES
#define GSS_LOCK_MUTEX_EXT(utils, mutex)  \
    if(((sasl_utils_t *)(utils))->mutex_lock(mutex) != 0) { \
       return SASL_FAIL; \
    }

#define GSS_UNLOCK_MUTEX_EXT(utils, mutex) \
    if(((sasl_utils_t *)(utils))->mutex_unlock(mutex) != 0) { \
        return SASL_FAIL; \
    }

#define GSS_LOCK_MUTEX(utils) GSS_LOCK_MUTEX_EXT(utils, gss_mutex)
#define GSS_UNLOCK_MUTEX(utils) GSS_UNLOCK_MUTEX_EXT(utils, gss_mutex)

#define GSS_LOCK_MUTEX_CTX(utils, ctx) GSS_LOCK_MUTEX_EXT(utils, (ctx)->ctx_mutex)
#define GSS_UNLOCK_MUTEX_CTX(utils, ctx) GSS_UNLOCK_MUTEX_EXT(utils, (ctx)->ctx_mutex)


static void *gss_mutex = NULL;
#else
#define GSS_LOCK_MUTEX(utils)
#define GSS_UNLOCK_MUTEX(utils)
#define GSS_LOCK_MUTEX_CTX(utils, ctx)
#define GSS_UNLOCK_MUTEX_CTX(utils, ctx)
#endif

typedef struct context {
    int state;

    gss_OID mech_type;		     /* GSS-SPNEGO or GSSAPI */
    int http_mode;		     /* use RFC 4559 compatible protocol? */
    
    gss_ctx_id_t gss_ctx;
    gss_name_t   client_name;
    gss_name_t   server_name;
    gss_cred_id_t server_creds;
    gss_cred_id_t client_creds;

    sasl_ssf_t limitssf, requiressf; /* application defined bounds, for the
					server */
    unsigned char qop;		     /* as allowed by GSSAPI */

    const sasl_utils_t *utils;
    
    /* layers buffering */
    decode_context_t decode_context;
    
    char *encode_buf;                /* For encoding/decoding mem management */
    char *decode_buf;
    char *decode_once_buf;
    unsigned encode_buf_len;
    unsigned decode_buf_len;
    unsigned decode_once_buf_len;
    buffer_info_t *enc_in_buf;
    
    char *out_buf;                   /* per-step mem management */
    unsigned out_buf_len;    
    
    char *authid; /* hold the authid between steps - server */
    const char *user;   /* hold the userid between steps - client */
    void *ctx_mutex; /* A per-context mutex */
} context_t;

enum {
    SASL_GSSAPI_STATE_AUTHNEG = 1,
    SASL_GSSAPI_STATE_SSFCAP = 2,
    SASL_GSSAPI_STATE_SSFREQ = 3,
    SASL_GSSAPI_STATE_AUTHENTICATED = 4
};

#define LAYER_CONFIDENTIALITY	4
#define LAYER_INTEGRITY		2
#define LAYER_NONE		1

/* sasl_gss_log: only logs status string returned from gss_display_status() */
#define sasl_gss_log(x,y,z) sasl_gss_seterror_(x,y,z,1)
#define sasl_gss_seterror(x,y,z) sasl_gss_seterror_(x,y,z,0)

static int
sasl_gss_seterror_(const sasl_utils_t *utils, OM_uint32 maj, OM_uint32 min,
		   int logonly)
{
    OM_uint32 maj_stat, min_stat;
    gss_buffer_desc msg;
    OM_uint32 msg_ctx;
    int ret;
    char *out = NULL;
    size_t len;
    unsigned curlen = 0;
    const char prefix[] = "GSSAPI Error: ";

    if (!utils) return SASL_OK;
    
    len = sizeof(prefix);
    ret = _plug_buf_alloc(utils, &out, &curlen, 256);
    if (ret != SASL_OK) return SASL_NOMEM;
    
    strcpy(out, prefix);
    
    msg_ctx = 0;
    while (1) {
	GSS_LOCK_MUTEX(utils);
	maj_stat = gss_display_status(&min_stat, maj,
				      GSS_C_GSS_CODE, GSS_C_NULL_OID,
				      &msg_ctx, &msg);
	GSS_UNLOCK_MUTEX(utils);
	
	if(GSS_ERROR(maj_stat)) {
	    if (logonly) {
		utils->log(utils->conn, SASL_LOG_FAIL,
			"GSSAPI Failure: (could not get major error message)");
	    } else {
		utils->seterror(utils->conn, 0,
				"GSSAPI Failure "
				"(could not get major error message)");
	    }
	    utils->free(out);
	    return SASL_OK;
	}
	
	len += len + msg.length;
	ret = _plug_buf_alloc(utils, &out, &curlen, len);
	
	if(ret != SASL_OK) {
	    utils->free(out);
	    return SASL_NOMEM;
	}
	
	strcat(out, msg.value);
	
	GSS_LOCK_MUTEX(utils);
	gss_release_buffer(&min_stat, &msg);
	GSS_UNLOCK_MUTEX(utils);
	
	if (!msg_ctx)
	    break;
    }
    
    /* Now get the minor status */
    
    len += 2;
    ret = _plug_buf_alloc(utils, &out, &curlen, len);
    if(ret != SASL_OK) {
	utils->free(out);
	return SASL_NOMEM;
    }
    
    strcat(out, " (");
    
    msg_ctx = 0;
    while (1) {
	GSS_LOCK_MUTEX(utils);
	maj_stat = gss_display_status(&min_stat, min,
				      GSS_C_MECH_CODE, GSS_C_NULL_OID,
				      &msg_ctx, &msg);
	GSS_UNLOCK_MUTEX(utils);
	
	if(GSS_ERROR(maj_stat)) {
	    if (logonly) {
		utils->log(utils->conn, SASL_LOG_FAIL,
			"GSSAPI Failure: (could not get minor error message)");
	    } else {
		utils->seterror(utils->conn, 0,
				"GSSAPI Failure "
				"(could not get minor error message)");
	    }
	    utils->free(out);
	    return SASL_OK;
	}
	
	len += len + msg.length;

	ret = _plug_buf_alloc(utils, &out, &curlen, len);
	if(ret != SASL_OK) {
	    utils->free(out);
	    return SASL_NOMEM;
	}
	
	strcat(out, msg.value);
	
	GSS_LOCK_MUTEX(utils);
	gss_release_buffer(&min_stat, &msg);
	GSS_UNLOCK_MUTEX(utils);
	
	if (!msg_ctx)
	    break;
    }
    
    len += 1;
    ret = _plug_buf_alloc(utils, &out, &curlen, len);
    if(ret != SASL_OK) {
	utils->free(out);
	return SASL_NOMEM;
    }
    
    strcat(out, ")");
    
    if (logonly) {
	utils->log(utils->conn, SASL_LOG_FAIL, "%s", out);
    } else {
	utils->seterror(utils->conn, 0, "%s", out);
    }
    utils->free(out);

    return SASL_OK;
}

static int 
sasl_gss_encode(void *context, const struct iovec *invec, unsigned numiov,
		const char **output, unsigned *outputlen, int privacy)
{
    context_t *text = (context_t *)context;
    OM_uint32 maj_stat, min_stat;
    gss_buffer_t input_token, output_token;
    gss_buffer_desc real_input_token, real_output_token;
    int ret;
    struct buffer_info *inblob, bufinfo;
    
    if (!output) return SASL_BADPARAM;
    
    if (numiov > 1) {
	ret = _plug_iovec_to_buf(text->utils, invec, numiov, &text->enc_in_buf);
	if (ret != SASL_OK) return ret;
	inblob = text->enc_in_buf;
    } else {
	bufinfo.data = invec[0].iov_base;
	bufinfo.curlen = invec[0].iov_len;
	inblob = &bufinfo;
    }
    
    if (text->state != SASL_GSSAPI_STATE_AUTHENTICATED) return SASL_NOTDONE;
    
    input_token = &real_input_token;
    
    real_input_token.value  = inblob->data;
    real_input_token.length = inblob->curlen;
    
    output_token = &real_output_token;
    output_token->value = NULL;
    output_token->length = 0;
    
    GSS_LOCK_MUTEX_CTX(text->utils, text);
    maj_stat = gss_wrap (&min_stat,
			 text->gss_ctx,
			 privacy,
			 GSS_C_QOP_DEFAULT,
			 input_token,
			 NULL,
			 output_token);
    GSS_UNLOCK_MUTEX_CTX(text->utils, text);
    
    if (GSS_ERROR(maj_stat)) {
	sasl_gss_seterror(text->utils, maj_stat, min_stat);
	if (output_token->value) {
	    GSS_LOCK_MUTEX_CTX(text->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(text->utils, text);
	}
	return SASL_FAIL;
    }
    
    if (output_token->value && output) {
	uint32_t len;
	
	ret = _plug_buf_alloc(text->utils,
			      &(text->encode_buf),
			      &(text->encode_buf_len),
			      output_token->length + 4);
	
	if (ret != SASL_OK) {
	    GSS_LOCK_MUTEX_CTX(text->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(text->utils, text);
	    return ret;
	}

	len = htonl(output_token->length);
	memcpy(text->encode_buf, &len, 4);
	memcpy(text->encode_buf + 4, output_token->value, output_token->length);
    }
    
    if (outputlen) {
	*outputlen = output_token->length + 4;
    }
    
    *output = text->encode_buf;
    
    if (output_token->value) {
	GSS_LOCK_MUTEX_CTX(text->utils, text);
	gss_release_buffer(&min_stat, output_token);
	GSS_UNLOCK_MUTEX_CTX(text->utils, text);
    }

    return SASL_OK;
}

static int gssapi_privacy_encode(void *context, const struct iovec *invec,
				 unsigned numiov, const char **output,
				 unsigned *outputlen)
{
    return sasl_gss_encode(context,invec,numiov,output,outputlen,1);
}

static int gssapi_integrity_encode(void *context, const struct iovec *invec,
				   unsigned numiov, const char **output,
				   unsigned *outputlen) 
{
    return sasl_gss_encode(context,invec,numiov,output,outputlen,0);
}

static int
gssapi_decode_packet(void *context,
		     const char *input,
		     unsigned inputlen,
		     char **output,
		     unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    OM_uint32 maj_stat, min_stat;
    gss_buffer_t input_token, output_token;
    gss_buffer_desc real_input_token, real_output_token;
    int result;
    
    if (text->state != SASL_GSSAPI_STATE_AUTHENTICATED) {
	SETERROR(text->utils, "GSSAPI Failure");
	return SASL_NOTDONE;
    }
    
    input_token = &real_input_token; 
    real_input_token.value = (char *) input;
    real_input_token.length = inputlen;
    
    output_token = &real_output_token;
    output_token->value = NULL;
    output_token->length = 0;
    
    GSS_LOCK_MUTEX_CTX(text->utils, text);
    maj_stat = gss_unwrap (&min_stat,
			   text->gss_ctx,
			   input_token,
			   output_token,
			   NULL,
			   NULL);
    GSS_UNLOCK_MUTEX_CTX(text->utils, text);
    
    if (GSS_ERROR(maj_stat)) {
	sasl_gss_seterror(text->utils,maj_stat,min_stat);
	if (output_token->value) {
	    GSS_LOCK_MUTEX_CTX(text->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(text->utils, text);
	}
	return SASL_FAIL;
    }
    
    if (outputlen) {
	*outputlen = output_token->length;
    }
    
    if (output_token->value) {
	if (output && outputlen) {
	    result = _plug_buf_alloc(text->utils, &text->decode_once_buf,
				     &text->decode_once_buf_len,
				     *outputlen);
	    if (result != SASL_OK) {
		GSS_LOCK_MUTEX_CTX(text->utils, text);
		gss_release_buffer(&min_stat, output_token);
		GSS_UNLOCK_MUTEX_CTX(text->utils, text);
		return result;
	    }
	    *output = text->decode_once_buf;
	    memcpy(*output, output_token->value, *outputlen);
	}
	GSS_LOCK_MUTEX_CTX(text->utils, text);
	gss_release_buffer(&min_stat, output_token);
	GSS_UNLOCK_MUTEX_CTX(text->utils, text);
    }
    
    return SASL_OK;
}

static int gssapi_decode(void *context,
			 const char *input, unsigned inputlen,
			 const char **output, unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    int ret;
    
    ret = _plug_decode(&text->decode_context, input, inputlen,
		       &text->decode_buf, &text->decode_buf_len, outputlen,
		       gssapi_decode_packet, text);
    
    *output = text->decode_buf;
    
    return ret;
}

static context_t *sasl_gss_new_context(const sasl_utils_t *utils)
{
    context_t *ret;
    
    ret = utils->malloc(sizeof(context_t));
    if(!ret) return NULL;
    
    memset(ret,0,sizeof(context_t));
    ret->utils = utils;
#ifdef GSS_USE_MUTEXES
    ret->ctx_mutex = utils->mutex_alloc();
    if (!ret->ctx_mutex) {
           utils->free(ret);
           return NULL;
    }
#endif

    return ret;
}

static int sasl_gss_free_context_contents(context_t *text)
{
    OM_uint32 min_stat;
    
    if (!text) return SASL_OK;
    
#ifdef GSS_USE_MUTEXES
    if (text->ctx_mutex) {
        GSS_LOCK_MUTEX_CTX(text->utils, text);
    }
#endif

    if (text->gss_ctx != GSS_C_NO_CONTEXT) {
        (void) gss_delete_sec_context(&min_stat,&text->gss_ctx,
                                      GSS_C_NO_BUFFER);
	text->gss_ctx = GSS_C_NO_CONTEXT;
    }
    
    if (text->client_name != GSS_C_NO_NAME) {
	(void) gss_release_name(&min_stat,&text->client_name);
	text->client_name = GSS_C_NO_NAME;
    }
    
    if (text->server_name != GSS_C_NO_NAME) {
	(void) gss_release_name(&min_stat,&text->server_name);
	text->server_name = GSS_C_NO_NAME;
    }
    
    if ( text->server_creds != GSS_C_NO_CREDENTIAL) {
	(void) gss_release_cred(&min_stat, &text->server_creds);
	text->server_creds = GSS_C_NO_CREDENTIAL;
    }

    if ( text->client_creds != GSS_C_NO_CREDENTIAL) {
	(void) gss_release_cred(&min_stat, &text->client_creds);
	text->client_creds = GSS_C_NO_CREDENTIAL;
    }

    if (text->out_buf) {
	text->utils->free(text->out_buf);
	text->out_buf = NULL;
    }
    
    if (text->encode_buf) {
	text->utils->free(text->encode_buf);
	text->encode_buf = NULL;
    }
    
    if (text->decode_buf) {
	text->utils->free(text->decode_buf);
	text->decode_buf = NULL;
    }
    
    if (text->decode_once_buf) {
	text->utils->free(text->decode_once_buf);
	text->decode_once_buf = NULL;
    }
    
    if (text->enc_in_buf) {
	if(text->enc_in_buf->data) text->utils->free(text->enc_in_buf->data);
	text->utils->free(text->enc_in_buf);
	text->enc_in_buf = NULL;
    }

    _plug_decode_free(&text->decode_context);
    
    if (text->authid) { /* works for both client and server */
	text->utils->free(text->authid);
	text->authid = NULL;
    }

#ifdef GSS_USE_MUTEXES
    if (text->ctx_mutex) {
        GSS_UNLOCK_MUTEX_CTX(text->utils, text);
        text->utils->mutex_free(text->ctx_mutex);
        text->ctx_mutex = NULL;
    }
#endif

    return SASL_OK;

}

static void gssapi_common_mech_dispose(void *conn_context,
				       const sasl_utils_t *utils)
{
    sasl_gss_free_context_contents((context_t *)(conn_context));
    utils->free(conn_context);
}

static void gssapi_common_mech_free(void *global_context __attribute__((unused)),
				    const sasl_utils_t *utils)
{
#ifdef GSS_USE_MUTEXES
    if (gss_mutex) {
      utils->mutex_free(gss_mutex);
      gss_mutex=NULL;
    }
#endif
}

static int gssapi_wrap_sizes(context_t *text, sasl_out_params_t *oparams)
{
    OM_uint32 maj_stat = 0, min_stat = 0;
    OM_uint32 max_input = 0;

    maj_stat = gss_wrap_size_limit(&min_stat,
                                   text->gss_ctx,
                                   1,
                                   GSS_C_QOP_DEFAULT,
                                   (OM_uint32)oparams->maxoutbuf,
                                   &max_input);
   if (maj_stat != GSS_S_COMPLETE) {
       return SASL_FAIL;
   }

    if (max_input > oparams->maxoutbuf) {
        /* Heimdal appears to get this wrong */
        oparams->maxoutbuf -= (max_input - oparams->maxoutbuf);
    } else {
        /* This code is actually correct */
        oparams->maxoutbuf = max_input;
    }

    return SASL_OK;
}

#if !defined(HAVE_GSS_C_SEC_CONTEXT_SASL_SSF)
gss_OID_desc gss_sasl_ssf = {
    11, (void *)"\x2a\x86\x48\x86\xf7\x12\x01\x02\x02\x05\x0f"
};
gss_OID GSS_C_SEC_CONTEXT_SASL_SSF = &gss_sasl_ssf;
#endif

static int gssapi_get_ssf(context_t *text, sasl_ssf_t *mech_ssf)
{
#ifdef HAVE_GSS_INQUIRE_SEC_CONTEXT_BY_OID
    OM_uint32 maj_stat = 0, min_stat = 0;
    gss_buffer_set_t bufset = GSS_C_NO_BUFFER_SET;
    gss_OID ssf_oid = GSS_C_SEC_CONTEXT_SASL_SSF;
    uint32_t ssf;

    maj_stat = gss_inquire_sec_context_by_oid(&min_stat, text->gss_ctx,
                                              ssf_oid, &bufset);
    switch (maj_stat) {
    case GSS_S_UNAVAILABLE:
        /* Not supported by the library, fallback to default */
        goto fallback;

    case GSS_S_COMPLETE:
        if ((bufset->count != 1) || (bufset->elements[0].length != 4)) {
            /* Malformed bufset, fail */
            (void)gss_release_buffer_set(&min_stat, &bufset);
            return SASL_FAIL;
        }
        memcpy(&ssf, bufset->elements[0].value, 4);
        (void)gss_release_buffer_set(&min_stat, &bufset);
        *mech_ssf = ntohl(ssf);
        return SASL_OK;

    case GSS_S_FAILURE:
        /* Not supported by Heimdal, fallback to default */
        if (min_stat == 0) {
            goto fallback;
        }

        GCC_FALLTHROUGH
	
    default:
        if (GSS_ERROR(maj_stat)) {
            sasl_gss_seterror(text->utils,maj_stat,min_stat);
        }
        return SASL_FAIL;
    }

fallback:
#endif
    *mech_ssf = K5_MIN_SSF;
    return SASL_OK;
}

/* The GSS-SPNEGO mechanism does not do SSF negotiation, instead it uses the
 * flags negotiated by GSSAPI to determine If confidentiality or integrity are
 * used. These flags are stored in text->qop transalated as layers by the
 * caller */
static int gssapi_spnego_ssf(context_t *text,
                             sasl_security_properties_t *props,
                             sasl_out_params_t *oparams)
{
    int ret;

    if (text->qop & LAYER_CONFIDENTIALITY) {
        oparams->encode = &gssapi_privacy_encode;
        oparams->decode = &gssapi_decode;
        ret = gssapi_get_ssf(text, &oparams->mech_ssf);
        if (ret != SASL_OK) {
            return ret;
        }
    } else if (text->qop & LAYER_INTEGRITY) {
        oparams->encode = &gssapi_integrity_encode;
        oparams->decode = &gssapi_decode;
        oparams->mech_ssf = 1;
    } else {
        oparams->encode = NULL;
        oparams->decode = NULL;
        oparams->mech_ssf = 0;
    }

    if (oparams->mech_ssf) {
        ret = gssapi_wrap_sizes(text, oparams);
        if (ret != SASL_OK) {
            return ret;
        }
    }

    text->state = SASL_GSSAPI_STATE_AUTHENTICATED;

    /* used by layers */
    _plug_decode_init(&text->decode_context, text->utils,
		      (props->maxbufsize > 0xFFFFFF) ? 0xFFFFFF :
                      props->maxbufsize);

    return SASL_OK;
}

/*****************************  Server Section  *****************************/

static int 
gssapi_server_mech_new(void *glob_context,
		       sasl_server_params_t *params,
		       const char *challenge __attribute__((unused)), 
		       unsigned challen __attribute__((unused)),
		       void **conn_context)
{
    context_t *text;
    
    text = sasl_gss_new_context(params->utils);
    if (text == NULL) {
	MEMERROR(params->utils);
	return SASL_NOMEM;
    }
    
    text->gss_ctx = GSS_C_NO_CONTEXT;
    text->client_name = GSS_C_NO_NAME;
    text->server_name = GSS_C_NO_NAME;
    text->server_creds = GSS_C_NO_CREDENTIAL;
    text->client_creds = GSS_C_NO_CREDENTIAL;
    text->state = SASL_GSSAPI_STATE_AUTHNEG;
    
    text->http_mode = (params->flags & SASL_NEED_HTTP);
    text->mech_type = (gss_OID) glob_context;

    *conn_context = text;
    
    return SASL_OK;
}

static int 
gssapi_server_mech_authneg(context_t *text,
			   sasl_server_params_t *params,
			   const char *clientin,
			   unsigned clientinlen,
			   const char **serverout,
			   unsigned *serveroutlen,
			   sasl_out_params_t *oparams)
{
    gss_buffer_t input_token, output_token;
    gss_buffer_desc real_input_token, real_output_token;
    OM_uint32 maj_stat = 0, min_stat = 0;
    gss_buffer_desc name_token;
    int ret, equal = 0;
    unsigned out_flags = 0;
    gss_cred_id_t server_creds = (gss_cred_id_t) params->gss_creds;
    gss_buffer_desc name_without_realm;
    gss_name_t client_name_MN = NULL, without = NULL;
    gss_OID mech_type;
	
    input_token = &real_input_token;
    output_token = &real_output_token;
    output_token->value = NULL; output_token->length = 0;
    input_token->value = NULL; input_token->length = 0;
    
    if (text->server_name == GSS_C_NO_NAME) { /* only once */
	if (params->serverFQDN == NULL
	    || strlen(params->serverFQDN) == 0) {
	    SETERROR(text->utils, "GSSAPI Failure: no serverFQDN");
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}
	name_token.length = strlen(params->service) + 1 + strlen(params->serverFQDN);
	name_token.value = (char *)params->utils->malloc((name_token.length + 1) * sizeof(char));
	if (name_token.value == NULL) {
	    MEMERROR(text->utils);
	    sasl_gss_free_context_contents(text);
	    return SASL_NOMEM;
	}
	sprintf(name_token.value,"%s@%s", params->service, params->serverFQDN);

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	maj_stat = gss_import_name (&min_stat,
				    &name_token,
				    GSS_C_NT_HOSTBASED_SERVICE,
				    &text->server_name);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);

	params->utils->free(name_token.value);
	name_token.value = NULL;

	if (GSS_ERROR(maj_stat)) {
	    sasl_gss_seterror(text->utils, maj_stat, min_stat);
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}

	if ( text->server_creds != GSS_C_NO_CREDENTIAL) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    maj_stat = gss_release_cred(&min_stat, &text->server_creds);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    text->server_creds = GSS_C_NO_CREDENTIAL;
	}

	/* If caller didn't provide creds already */
	if ( server_creds == GSS_C_NO_CREDENTIAL) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    maj_stat = gss_acquire_cred(&min_stat, 
					text->server_name,
					GSS_C_INDEFINITE, 
					GSS_C_NO_OID_SET,
					GSS_C_ACCEPT,
					&text->server_creds, 
					NULL, 
					NULL);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

	    if (GSS_ERROR(maj_stat)) {
		sasl_gss_seterror(text->utils, maj_stat, min_stat);
		sasl_gss_free_context_contents(text);
		return SASL_FAIL;
	    }
	    server_creds = text->server_creds;
	}
    }
	
    if (clientinlen) {
	real_input_token.value = (void *)clientin;
	real_input_token.length = clientinlen;
    }


    GSS_LOCK_MUTEX_CTX(params->utils, text);
    maj_stat =
	gss_accept_sec_context(&min_stat,
			       &(text->gss_ctx),
			       server_creds,
			       input_token,
			       GSS_C_NO_CHANNEL_BINDINGS,
			       &text->client_name,
			       &mech_type,
			       output_token,
			       &out_flags,
			       NULL,	/* context validity period */
			       &(text->client_creds));
    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

    if (GSS_ERROR(maj_stat)) {
	sasl_gss_log(text->utils, maj_stat, min_stat);
	text->utils->seterror(text->utils->conn, SASL_NOLOG, "GSSAPI Failure: gss_accept_sec_context");
	if (output_token->value) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	}
	sasl_gss_free_context_contents(text);
	return SASL_BADAUTH;
    }

    if (serveroutlen) {
	*serveroutlen = output_token->length;
    }
    if (output_token->value) {
	if (serverout && serveroutlen) {
	    ret = _plug_buf_alloc(text->utils, &(text->out_buf),
				  &(text->out_buf_len), *serveroutlen);
	    if(ret != SASL_OK) {
		GSS_LOCK_MUTEX_CTX(params->utils, text);
		gss_release_buffer(&min_stat, output_token);
		GSS_UNLOCK_MUTEX_CTX(params->utils, text);
		return ret;
	    }
	    memcpy(text->out_buf, output_token->value, *serveroutlen);
	    *serverout = text->out_buf;
	}

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	gss_release_buffer(&min_stat, output_token);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);
    } else if (serverout && serveroutlen) {
	/* No output token, send an empty string */
	*serverout = GSSAPI_BLANK_STRING;
	*serveroutlen = 0;
    }

    if (maj_stat == GSS_S_CONTINUE_NEEDED) {
	/* Context isn't complete */
        return SASL_CONTINUE;
    }

    assert(maj_stat == GSS_S_COMPLETE);

    /* When GSS_Accept_sec_context returns GSS_S_COMPLETE, the server
       examines the context to ensure that it provides a level of protection
       permitted by the server's security policy.  In particular, if the
       integ_avail flag is not set in the context, then no security layer
       can be offered or accepted.  If the conf_avail flag is not set in the
       context, then no security layer with confidentiality can be offered
       or accepted. */
    if ((out_flags & GSS_C_INTEG_FLAG) == 0) {
	/* if the integ_avail flag is not set in the context,
	   then no security layer can be offered or accepted. */
	text->qop = LAYER_NONE;
    } else if ((out_flags & GSS_C_CONF_FLAG) == 0) {
	/* If the conf_avail flag is not set in the context,
	   then no security layer with confidentiality can be offered
	   or accepted. */
	text->qop = LAYER_NONE | LAYER_INTEGRITY;
    } else {
	text->qop = LAYER_NONE | LAYER_INTEGRITY | LAYER_CONFIDENTIALITY;
    }

    if ((params->props.security_flags & SASL_SEC_PASS_CREDENTIALS) &&
	(!(out_flags & GSS_C_DELEG_FLAG) ||
	 text->client_creds == GSS_C_NO_CREDENTIAL) ) 
	{
	    text->utils->seterror(text->utils->conn, SASL_LOG_WARN,
				  "GSSAPI warning: no credentials were passed");
	    /* continue with authentication */
	}

    GSS_LOCK_MUTEX_CTX(params->utils, text);
    maj_stat = gss_canonicalize_name(&min_stat,
				     text->client_name,
				     mech_type,
				     &client_name_MN);
    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

    if (GSS_ERROR(maj_stat)) {
	SETERROR(text->utils, "GSSAPI Failure: gss_canonicalize_name");
	sasl_gss_free_context_contents(text);
	return SASL_BADAUTH;
    }

    name_token.value = NULL;
    name_without_realm.value = NULL;

    GSS_LOCK_MUTEX_CTX(params->utils, text);
    maj_stat = gss_display_name (&min_stat,
				 client_name_MN,
				 &name_token,
				 NULL);
    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

    if (GSS_ERROR(maj_stat)) {
	SETERROR(text->utils, "GSSAPI Failure: gss_display_name");
	sasl_gss_free_context_contents(text);
	ret = SASL_BADAUTH;
	goto cleanup;
    }

    /* If the id contains a realm get the identifier for the user
       without the realm and see if it's the same id (i.e. 
       tmartin == tmartin@ANDREW.CMU.EDU. If this is the case we just want
       to return the id (i.e. just "tmartin" */
    if (strchr((char *) name_token.value, (int) '@') != NULL) {
	/* NOTE: libc malloc, as it is freed below by a gssapi internal
	 *       function! */
	name_without_realm.value = params->utils->malloc(strlen(name_token.value)+1);
	if (name_without_realm.value == NULL) {
	    MEMERROR(text->utils);
	    ret = SASL_NOMEM;
	    goto cleanup;
	}

	strcpy(name_without_realm.value, name_token.value);

	/* cut off string at '@' */
	(strchr(name_without_realm.value,'@'))[0] = '\0';

	name_without_realm.length = strlen( (char *) name_without_realm.value );

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	maj_stat = gss_import_name (&min_stat,
				    &name_without_realm,
	    /* Solaris 8/9 gss_import_name doesn't accept GSS_C_NULL_OID here,
	       so use GSS_C_NT_USER_NAME instead if available.  */
#ifdef HAVE_GSS_C_NT_USER_NAME
				    GSS_C_NT_USER_NAME,
#else
				    GSS_C_NULL_OID,
#endif
				    &without);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);

	if (GSS_ERROR(maj_stat)) {
	    SETERROR(text->utils, "GSSAPI Failure: gss_import_name");
	    sasl_gss_free_context_contents(text);
	    ret = SASL_BADAUTH;
	    goto cleanup;
	}

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	maj_stat = gss_compare_name(&min_stat,
				    client_name_MN,
				    without,
				    &equal);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);

	if (GSS_ERROR(maj_stat)) {
	    SETERROR(text->utils, "GSSAPI Failure: gss_compare_name");
	    sasl_gss_free_context_contents(text);
	    ret = SASL_BADAUTH;
	    goto cleanup;
	}

    } else {
	equal = 0;
    }

    if (equal) {
	text->authid = strdup(name_without_realm.value);
    } else {
	text->authid = strdup(name_token.value);
    }

    if (text->authid == NULL) {
	MEMERROR(params->utils);
	ret = SASL_NOMEM;
	goto cleanup;
    }

    if (text->http_mode) {
	/* HTTP doesn't do any ssf negotiation */
	text->state = SASL_GSSAPI_STATE_AUTHENTICATED;
	ret = SASL_OK;
    } else if (text->mech_type && text->mech_type == &gss_spnego_oid) {
        ret = gssapi_spnego_ssf(text, &params->props, oparams);
    } else {
	/* Switch to ssf negotiation */
	text->state = SASL_GSSAPI_STATE_SSFCAP;
	ret = SASL_CONTINUE;
    }

    /* Release server creds which are no longer needed */
     if ( text->server_creds != GSS_C_NO_CREDENTIAL) {
        maj_stat = gss_release_cred(&min_stat, &text->server_creds);
        text->server_creds = GSS_C_NO_CREDENTIAL;
     }

  cleanup:
    if (text->server_creds != GSS_C_NO_CREDENTIAL) {
	GSS_LOCK_MUTEX(params->utils);
	maj_stat = gss_release_cred(&min_stat, &text->server_creds);
	GSS_UNLOCK_MUTEX(params->utils);
	text->server_creds = GSS_C_NO_CREDENTIAL;
    }
    if (client_name_MN) {
	GSS_LOCK_MUTEX(params->utils);
	gss_release_name(&min_stat, &client_name_MN);
	GSS_UNLOCK_MUTEX(params->utils);
    }
    if (name_token.value) {
	GSS_LOCK_MUTEX(params->utils);
	gss_release_buffer(&min_stat, &name_token);
	GSS_UNLOCK_MUTEX(params->utils);
    }
    if (name_without_realm.value) {
	params->utils->free(name_without_realm.value);
    }
    if (without) {
	GSS_LOCK_MUTEX(params->utils);
	gss_release_name(&min_stat, &without);
	GSS_UNLOCK_MUTEX(params->utils);
    }

    return ret;
}

static int 
gssapi_server_mech_ssfcap(context_t *text,
			  sasl_server_params_t *params,
			  const char *clientin __attribute__((unused)),
			  unsigned clientinlen,
			  const char **serverout,
			  unsigned *serveroutlen,
			  sasl_out_params_t *oparams __attribute__((unused)))
{
    gss_buffer_t input_token, output_token;
    gss_buffer_desc real_input_token, real_output_token;
    OM_uint32 maj_stat = 0, min_stat = 0;
    unsigned char sasldata[4];
    sasl_ssf_t mech_ssf;
    int ret;

    input_token = &real_input_token;
    output_token = &real_output_token;
    output_token->value = NULL; output_token->length = 0;
    

    if (clientinlen != 0) {
	SETERROR(text->utils, "GSSAPI server is not expecting data at this stage");
	sasl_gss_free_context_contents(text);
	return SASL_BADAUTH;
    }

    /* we have to decide what sort of encryption/integrity/etc.,
       we support */
    if (params->props.max_ssf < params->external_ssf) {
	text->limitssf = 0;
    } else {
	text->limitssf = params->props.max_ssf - params->external_ssf;
    }
    if (params->props.min_ssf < params->external_ssf) {
	text->requiressf = 0;
    } else {
	text->requiressf = params->props.min_ssf - params->external_ssf;
    }

    /* build up our security properties token */
    if (params->props.maxbufsize > 0xFFFFFF) {
        /* make sure maxbufsize isn't too large */
        /* maxbufsize = 0xFFFFFF */
        sasldata[1] = sasldata[2] = sasldata[3] = 0xFF;
    } else {
        sasldata[1] = (params->props.maxbufsize >> 16) & 0xFF;
        sasldata[2] = (params->props.maxbufsize >> 8) & 0xFF;
        sasldata[3] = (params->props.maxbufsize >> 0) & 0xFF;
    }

    sasldata[0] = 0;
    if(text->requiressf != 0 && !params->props.maxbufsize) {
	params->utils->seterror(params->utils->conn, 0,
				"GSSAPI needs a security layer but one is forbidden");
	return SASL_TOOWEAK;
    }

    if (text->requiressf == 0) {
	sasldata[0] |= LAYER_NONE; /* authentication */
    }
    if ((text->qop & LAYER_INTEGRITY) &&
	text->requiressf <= 1 &&
	text->limitssf >= 1 &&
	params->props.maxbufsize) {
	sasldata[0] |= LAYER_INTEGRITY;
    }
    ret = gssapi_get_ssf(text, &mech_ssf);
    if (ret != SASL_OK) {
	sasl_gss_free_context_contents(text);
        return ret;
    }
    if ((text->qop & LAYER_CONFIDENTIALITY) &&
	text->requiressf <= mech_ssf &&
	text->limitssf >= mech_ssf &&
	params->props.maxbufsize) {
	sasldata[0] |= LAYER_CONFIDENTIALITY;
    }

    if ((sasldata[0] & ~LAYER_NONE) == 0) {
	/* From RFC 4752: "The client verifies that the server maximum buffer is 0
	   if the server does not advertise support for any security layer." */
	sasldata[1] = sasldata[2] = sasldata[3] = 0;
    }

    /* Remember what we want and can offer */
    text->qop = sasldata[0];

    real_input_token.value = (void *)sasldata;
    real_input_token.length = 4;

    GSS_LOCK_MUTEX_CTX(params->utils, text);
    maj_stat = gss_wrap(&min_stat,
			text->gss_ctx,
			0, /* Just integrity checking here */
			GSS_C_QOP_DEFAULT,
			input_token,
			NULL,
			output_token);
    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

    if (GSS_ERROR(maj_stat)) {
	sasl_gss_seterror(text->utils, maj_stat, min_stat);
	if (output_token->value) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	}
	sasl_gss_free_context_contents(text);
	return SASL_FAIL;
    }


    if (serveroutlen)
	*serveroutlen = output_token->length;
    if (output_token->value) {
	if (serverout && serveroutlen) {
	    ret = _plug_buf_alloc(text->utils, &(text->out_buf),
				  &(text->out_buf_len), *serveroutlen);
	    if(ret != SASL_OK) {
		GSS_LOCK_MUTEX_CTX(params->utils, text);
		gss_release_buffer(&min_stat, output_token);
		GSS_UNLOCK_MUTEX_CTX(params->utils, text);
		return ret;
	    }
	    memcpy(text->out_buf, output_token->value, *serveroutlen);
	    *serverout = text->out_buf;
	}

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	gss_release_buffer(&min_stat, output_token);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);
    }

    /* Wait for ssf request and authid */
    text->state = SASL_GSSAPI_STATE_SSFREQ; 
	
    return SASL_CONTINUE;
}

static int 
gssapi_server_mech_ssfreq(context_t *text,
			  sasl_server_params_t *params,
			  const char *clientin,
			  unsigned clientinlen,
			  const char **serverout __attribute__((unused)),
			  unsigned *serveroutlen __attribute__((unused)),
			  sasl_out_params_t *oparams)
{
    gss_buffer_t input_token, output_token;
    gss_buffer_desc real_input_token, real_output_token;
    OM_uint32 maj_stat = 0, min_stat = 0;
    int layerchoice;
	
    input_token = &real_input_token;
    output_token = &real_output_token;
    output_token->value = NULL; output_token->length = 0;

    real_input_token.value = (void *)clientin;
    real_input_token.length = clientinlen;

    GSS_LOCK_MUTEX_CTX(params->utils, text);
    maj_stat = gss_unwrap(&min_stat,
			  text->gss_ctx,
			  input_token,
			  output_token,
			  NULL,
			  NULL);
    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

    if (GSS_ERROR(maj_stat)) {
	sasl_gss_seterror(text->utils, maj_stat, min_stat);
	sasl_gss_free_context_contents(text);
	return SASL_FAIL;
    }

    if (output_token->length < 4) {
	SETERROR(text->utils,
		 "token too short");
	GSS_LOCK_MUTEX_CTX(params->utils, text);
	gss_release_buffer(&min_stat, output_token);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	sasl_gss_free_context_contents(text);
	return SASL_FAIL;
    }

    layerchoice = (int)(((char *)(output_token->value))[0]);
    if (!(layerchoice & (LAYER_INTEGRITY | LAYER_CONFIDENTIALITY)) &&
	(text->qop & LAYER_NONE)) { /* no encryption */
	oparams->encode = NULL;
	oparams->decode = NULL;
	oparams->mech_ssf = 0;
    } else if ((layerchoice & LAYER_INTEGRITY) &&
	       (text->qop & LAYER_INTEGRITY)) { /* integrity */
	oparams->encode = &gssapi_integrity_encode;
	oparams->decode = &gssapi_decode;
	oparams->mech_ssf = 1;
    } else if (/* For compatibility with broken clients setting both bits */
		(layerchoice & (LAYER_CONFIDENTIALITY | LAYER_INTEGRITY)) &&
	       (text->qop & LAYER_CONFIDENTIALITY)) { /* privacy */
        int ret;
	oparams->encode = &gssapi_privacy_encode;
	oparams->decode = &gssapi_decode;

	ret = gssapi_get_ssf(text, &oparams->mech_ssf);
        if (ret != SASL_OK) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    sasl_gss_free_context_contents(text);
	    return ret;
	}
    } else {
	/* not a supported encryption layer */
	SETERROR(text->utils,
		 "protocol violation: client requested invalid layer");
	/* Mark that we attempted negotiation */
	oparams->mech_ssf = 2;
	if (output_token->value) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	}
	sasl_gss_free_context_contents(text);
	return SASL_FAIL;
    }

    if (output_token->length > 4) {
	int ret;

	ret = params->canon_user(params->utils->conn,
				 ((char *) output_token->value) + 4,
				 (output_token->length - 4) * sizeof(char),
				 SASL_CU_AUTHZID | SASL_CU_EXTERNALLY_VERIFIED,
				 oparams);

	if (ret != SASL_OK) {
	    sasl_gss_free_context_contents(text);
	    return ret;
	}
    }
	
    /* No matter what, set the rest of the oparams */

    oparams->maxoutbuf =
	(((unsigned char *) output_token->value)[1] << 16) |
	(((unsigned char *) output_token->value)[2] << 8) |
	(((unsigned char *) output_token->value)[3] << 0);

    GSS_LOCK_MUTEX_CTX(params->utils, text);
    gss_release_buffer(&min_stat, output_token);
    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

    if (oparams->mech_ssf) {
        int ret;

        ret = gssapi_wrap_sizes(text, oparams);
        if (ret != SASL_OK) {
	    sasl_gss_free_context_contents(text);
            return ret;
        }
    }

    text->state = SASL_GSSAPI_STATE_AUTHENTICATED;

    /* used by layers */
    _plug_decode_init(&text->decode_context,
		      text->utils,
		      (params->props.maxbufsize > 0xFFFFFF) ? 0xFFFFFF :
		      params->props.maxbufsize);
	
    return SASL_OK;
}

static int 
gssapi_server_mech_step(void *conn_context,
			sasl_server_params_t *params,
			const char *clientin,
			unsigned clientinlen,
			const char **serverout,
			unsigned *serveroutlen,
			sasl_out_params_t *oparams)
{
    context_t *text = (context_t *) conn_context;
    int ret;

    if (!serverout) {
	PARAMERROR(text->utils);
	return SASL_BADPARAM;
    }

    *serverout = NULL;
    *serveroutlen = 0;

    if (text == NULL) return SASL_BADPROT;

    switch (text->state) {

    case SASL_GSSAPI_STATE_AUTHNEG:
	ret = gssapi_server_mech_authneg(text, params, clientin, clientinlen,
					 serverout, serveroutlen, oparams);
	if (ret != SASL_CONTINUE || *serveroutlen) break;

	/* Pretend that we just got an empty response from the client */
	clientinlen = 0;

	/* fall through */

    case SASL_GSSAPI_STATE_SSFCAP:
	ret = gssapi_server_mech_ssfcap(text, params, clientin, clientinlen,
					serverout, serveroutlen, oparams);
	break;

    case SASL_GSSAPI_STATE_SSFREQ:
	ret = gssapi_server_mech_ssfreq(text, params, clientin, clientinlen,
					serverout, serveroutlen, oparams);
	break;

    default:
	params->utils->log(params->utils->conn, SASL_LOG_ERR,
			   "Invalid GSSAPI server step %d\n", text->state);
	return SASL_FAIL;
    }

    if (ret == SASL_OK) {
	ret = params->canon_user(params->utils->conn,
				 text->authid,
				 0, /* strlen(text->authid) */
				 (oparams->user ? 0 : SASL_CU_AUTHZID)
				 | SASL_CU_AUTHID | SASL_CU_EXTERNALLY_VERIFIED,
				 oparams);

	if (ret != SASL_OK) {
	    sasl_gss_free_context_contents(text);
	    return ret;
	}

	if (text->client_creds != GSS_C_NO_CREDENTIAL)	{
	    oparams->client_creds =  &text->client_creds;
	}
	else {
	    oparams->client_creds = NULL;
	}

	oparams->doneflag = 1;
    } else {
        params->utils->log(params->utils->conn, SASL_LOG_DEBUG,
		           "GSSAPI server step failed: %d\n", text->state);
    }
    
    return ret;
}

static sasl_server_plug_t gssapi_server_plugins[] = 
{
    {
	"GSSAPI",			/* mech_name */
	K5_MAX_SSF,			/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH		/* security_flags */
	| SASL_SEC_PASS_CREDENTIALS,
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY
	| SASL_FEAT_DONTUSE_USERPASSWD,	/* features */
	NULL,				/* glob_context */
	&gssapi_server_mech_new,	/* mech_new */
	&gssapi_server_mech_step,	/* mech_step */
	&gssapi_common_mech_dispose,	/* mech_dispose */
	&gssapi_common_mech_free,	/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech_avail */
	NULL				/* spare */
    }
#ifdef HAVE_GSS_SPNEGO
    ,{
	"GSS-SPNEGO",			/* mech_name */
	K5_MAX_SSF,			/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH		/* security_flags */
	| SASL_SEC_PASS_CREDENTIALS,
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY
	| SASL_FEAT_DONTUSE_USERPASSWD
	| SASL_FEAT_SUPPORTS_HTTP,	/* features */
	&gss_spnego_oid,		/* glob_context */
	&gssapi_server_mech_new,	/* mech_new */
	&gssapi_server_mech_step,	/* mech_step */
	&gssapi_common_mech_dispose,	/* mech_dispose */
	&gssapi_common_mech_free,	/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech_avail */
	NULL				/* spare */
    }
#endif
};

int gssapiv2_server_plug_init(
#ifndef HAVE_GSSKRB5_REGISTER_ACCEPTOR_IDENTITY
    const sasl_utils_t *utils __attribute__((unused)),
#else
    const sasl_utils_t *utils,
#endif 
    int maxversion,
    int *out_version,
    sasl_server_plug_t **pluglist,
    int *plugcount)
{
#ifdef HAVE_GSSKRB5_REGISTER_ACCEPTOR_IDENTITY
    const char *keytab = NULL;
    char keytab_path[1024];
    unsigned int rl;
#endif
    
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	return SASL_BADVERS;
    }
    
#ifdef HAVE_GSSKRB5_REGISTER_ACCEPTOR_IDENTITY
    /* unfortunately, we don't check for readability of keytab if it's
       the standard one, since we don't know where it is */
    
    /* FIXME: This code is broken */
    
    utils->getopt(utils->getopt_context, "GSSAPI", "keytab", &keytab, &rl);
    if (keytab != NULL) {
	if (access(keytab, R_OK) != 0) {
	    utils->log(NULL, SASL_LOG_ERR,
		       "Could not find keytab file: %s: %m", keytab);
	    return SASL_FAIL;
	}
	
	if(strlen(keytab) > 1024) {
	    utils->log(NULL, SASL_LOG_ERR,
		       "path to keytab is > 1024 characters");
	    return SASL_BUFOVER;
	}
	
	strncpy(keytab_path, keytab, 1024);
	
	gsskrb5_register_acceptor_identity(keytab_path);
    }
#endif
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = gssapi_server_plugins;
#ifdef HAVE_GSS_SPNEGO
    *plugcount = 2;
#else
    *plugcount = 1;  
#endif

#ifdef GSS_USE_MUTEXES
    if (!gss_mutex) {
       gss_mutex = utils->mutex_alloc();
       if (!gss_mutex) {
           return SASL_FAIL;
       }
    }
#endif
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

static int gssapi_client_mech_new(void *glob_context,
				  sasl_client_params_t *params,
				  void **conn_context)
{
    context_t *text;
    
    /* holds state are in */
    text = sasl_gss_new_context(params->utils);
    if (text == NULL) {
	MEMERROR(params->utils);
	return SASL_NOMEM;
    }
    
    text->state = SASL_GSSAPI_STATE_AUTHNEG;
    text->mech_type = (gss_OID) glob_context;
    text->gss_ctx = GSS_C_NO_CONTEXT;
    text->client_name = GSS_C_NO_NAME;
    text->server_creds = GSS_C_NO_CREDENTIAL;
    text->client_creds  = GSS_C_NO_CREDENTIAL;

    text->http_mode = (params->flags & SASL_NEED_HTTP);

    *conn_context = text;
    
    return SASL_OK;
}

static int gssapi_client_mech_step(void *conn_context,
				   sasl_client_params_t *params,
				   const char *serverin,
				   unsigned serverinlen,
				   sasl_interact_t **prompt_need,
				   const char **clientout,
				   unsigned *clientoutlen,
				   sasl_out_params_t *oparams)
{
    context_t *text = (context_t *)conn_context;
    gss_buffer_t input_token, output_token;
    gss_buffer_desc real_input_token, real_output_token;
    OM_uint32 maj_stat = 0, min_stat = 0;
    gss_buffer_desc name_token;
    int ret;
    OM_uint32 req_flags = 0, out_req_flags = 0;
    input_token = &real_input_token;
    output_token = &real_output_token;
    output_token->value = NULL;
    input_token->value = NULL; 
    input_token->length = 0;
    gss_cred_id_t client_creds = (gss_cred_id_t)params->gss_creds;

    if (clientout)
        *clientout = NULL;
    if (clientoutlen)
        *clientoutlen = 0;
    
    params->utils->log(params->utils->conn, SASL_LOG_DEBUG,
		       "GSSAPI client step %d", text->state);

    switch (text->state) {

    case SASL_GSSAPI_STATE_AUTHNEG:
	/* try to get the userid */
	if (text->user == NULL) {
	    int user_result = SASL_OK;
	    
	    user_result = _plug_get_userid(params->utils, &text->user,
					   prompt_need);
	    
	    if ((user_result != SASL_OK) && (user_result != SASL_INTERACT)) {
		sasl_gss_free_context_contents(text);
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
				       "Please enter your authorization name", NULL,
				       NULL, NULL,
				       NULL, NULL,
				       NULL, NULL, NULL,
				       NULL, NULL, NULL);
		if (result != SASL_OK) return result;
		
		return SASL_INTERACT;
	    }
	}
	    
	if (text->server_name == GSS_C_NO_NAME) { /* only once */
	    if (params->serverFQDN == NULL
		|| strlen(params->serverFQDN) == 0) {
		SETERROR(text->utils, "GSSAPI Failure: no serverFQDN");
		sasl_gss_free_context_contents(text);
		return SASL_FAIL;
	    }
	    name_token.length = strlen(params->service) + 1 + strlen(params->serverFQDN);
	    name_token.value = (char *)params->utils->malloc((name_token.length + 1) * sizeof(char));
	    if (name_token.value == NULL) {
		sasl_gss_free_context_contents(text);
		return SASL_NOMEM;
	    }
	    
	    sprintf(name_token.value,"%s@%s", params->service, params->serverFQDN);
	    
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    maj_stat = gss_import_name (&min_stat,
					&name_token,
					GSS_C_NT_HOSTBASED_SERVICE,
					&text->server_name);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    
	    params->utils->free(name_token.value);
	    name_token.value = NULL;
	    
	    if (GSS_ERROR(maj_stat)) {
		sasl_gss_seterror(text->utils, maj_stat, min_stat);
		sasl_gss_free_context_contents(text);
		return SASL_FAIL;
	    }
	}
	    
	if (serverinlen == 0)
	    input_token = GSS_C_NO_BUFFER;

	if (serverinlen) {
	    real_input_token.value = (void *)serverin;
	    real_input_token.length = serverinlen;
	}
	else if (text->gss_ctx != GSS_C_NO_CONTEXT ) {
	    /* This can't happen under GSSAPI: we have a non-null context
	     * and no input from the server.  However, thanks to Imap,
	     * which discards our first output, this happens all the time.
	     * Throw away the context and try again. */
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    maj_stat = gss_delete_sec_context (&min_stat,&text->gss_ctx,GSS_C_NO_BUFFER);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    text->gss_ctx = GSS_C_NO_CONTEXT;
	}

	/* Setup req_flags properly */
	req_flags = GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG;
	if (params->props.max_ssf > params->external_ssf) {
	    /* We are requesting a security layer */
	    req_flags |= GSS_C_INTEG_FLAG;
	    /* Any SSF bigger than 1 is confidentiality. */
	    /* Let's check if the client of the API requires confidentiality,
	       and it wasn't already provided by an external layer */
	    if (params->props.max_ssf - params->external_ssf > 1) {
		/* We want to try for privacy */
		req_flags |= GSS_C_CONF_FLAG;
	    }
	}

	if (params->props.security_flags & SASL_SEC_PASS_CREDENTIALS) {
	    req_flags = req_flags |  GSS_C_DELEG_FLAG;
	}

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	maj_stat = gss_init_sec_context(&min_stat,
					client_creds, /* GSS_C_NO_CREDENTIAL */
					&text->gss_ctx,
					text->server_name,
					text->mech_type,
					req_flags,
					0,
					GSS_C_NO_CHANNEL_BINDINGS,
					input_token,
					NULL,
					output_token,
					&out_req_flags,
					NULL);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	
	if (GSS_ERROR(maj_stat)) {
	    sasl_gss_seterror(text->utils, maj_stat, min_stat);
	    if (output_token->value) {
		GSS_LOCK_MUTEX_CTX(params->utils, text);
		gss_release_buffer(&min_stat, output_token);
		GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    }
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}

	if ((out_req_flags & GSS_C_INTEG_FLAG) == 0) {
	    /* if the integ_avail flag is not set in the context,
	       then no security layer can be offered or accepted. */
	    text->qop = LAYER_NONE;
	} else if ((out_req_flags & GSS_C_CONF_FLAG) == 0) {
	    /* If the conf_avail flag is not set in the context,
	       then no security layer with confidentiality can be offered
	       or accepted. */
	    text->qop = LAYER_NONE | LAYER_INTEGRITY;
	} else {
	    text->qop = LAYER_NONE | LAYER_INTEGRITY | LAYER_CONFIDENTIALITY;
	}

	if ((out_req_flags & GSS_C_DELEG_FLAG) != (req_flags & GSS_C_DELEG_FLAG)) {
	    text->utils->seterror(text->utils->conn, SASL_LOG_WARN, "GSSAPI warning: no credentials were passed");
	    /* not a fatal error */
	}

        if (clientoutlen)
            *clientoutlen = output_token->length;
	    
	if (output_token->value) {
	    if (clientout && clientoutlen) {
		ret = _plug_buf_alloc(text->utils, &(text->out_buf),
				      &(text->out_buf_len), *clientoutlen);
		if(ret != SASL_OK) {
		    GSS_LOCK_MUTEX_CTX(params->utils, text);
		    gss_release_buffer(&min_stat, output_token);
		    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
		    return ret;
		}
		memcpy(text->out_buf, output_token->value, *clientoutlen);
		*clientout = text->out_buf;
	    }
	    
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	}
	
	if (maj_stat == GSS_S_COMPLETE) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    maj_stat = gss_inquire_context(&min_stat,
					   text->gss_ctx,
					   &text->client_name,
					   NULL,       /* targ_name */
					   NULL,       /* lifetime */
					   NULL,       /* mech */
					   /* FIX ME: Should check the resulting flags here */
					   NULL,       /* flags */
					   NULL,       /* local init */
					   NULL);      /* open */
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    
	    if (GSS_ERROR(maj_stat)) {
		sasl_gss_seterror(text->utils, maj_stat, min_stat);
		sasl_gss_free_context_contents(text);
		return SASL_FAIL;
	    }
	    
	    name_token.length = 0;
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    maj_stat = gss_display_name(&min_stat,
					text->client_name,
					&name_token,
					NULL);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    
	    if (GSS_ERROR(maj_stat)) {
		if (name_token.value) {
		    GSS_LOCK_MUTEX_CTX(params->utils, text);
		    gss_release_buffer(&min_stat, &name_token);
		    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
		}
		SETERROR(text->utils, "GSSAPI Failure");
		sasl_gss_free_context_contents(text);
		return SASL_FAIL;
	    }
	    
	    if (text->user && text->user[0]) {
		ret = params->canon_user(params->utils->conn,
					 text->user, 0,
					 SASL_CU_AUTHZID, oparams);
		if (ret == SASL_OK) 
		    ret = params->canon_user(params->utils->conn,
					     name_token.value, 0,
					     SASL_CU_AUTHID, oparams);
	    } else {
		ret = params->canon_user(params->utils->conn,
					 name_token.value, 0,
					 SASL_CU_AUTHID | SASL_CU_AUTHZID,
					 oparams);
	    }
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, &name_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    
	    if (ret != SASL_OK) return ret;
	    
	    if (text->http_mode) {
		/* HTTP doesn't do any ssf negotiation */
		text->state = SASL_GSSAPI_STATE_AUTHENTICATED;
		oparams->doneflag = 1;
		return SASL_OK;
	    } else if (text->mech_type && text->mech_type == &gss_spnego_oid) {
		oparams->doneflag = 1;
                return gssapi_spnego_ssf(text, &params->props, oparams);
            }

	    /* Switch to ssf negotiation */
	    text->state = SASL_GSSAPI_STATE_SSFCAP;
	}
	
	return SASL_CONTINUE;

    case SASL_GSSAPI_STATE_SSFCAP: {
	sasl_security_properties_t *secprops = &(params->props);
	unsigned int alen, external = params->external_ssf;
	sasl_ssf_t need, allowed;
	char serverhas, mychoice;
	sasl_ssf_t mech_ssf;
	int ret;
	
	real_input_token.value = (void *) serverin;
	real_input_token.length = serverinlen;
	
	GSS_LOCK_MUTEX_CTX(params->utils, text);
	maj_stat = gss_unwrap(&min_stat,
			      text->gss_ctx,
			      input_token,
			      output_token,
			      NULL,
			      NULL);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	
	if (GSS_ERROR(maj_stat)) {
	    sasl_gss_seterror(text->utils, maj_stat, min_stat);
	    if (output_token->value) {
		GSS_LOCK_MUTEX_CTX(params->utils, text);
		gss_release_buffer(&min_stat, output_token);
		GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    }
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}
	
	if (output_token->length != 4) {
	    SETERROR(text->utils,
		     (output_token->length < 4) ? "token too short" : "token too long");
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}

	ret = gssapi_get_ssf(text, &mech_ssf);
	if (ret != SASL_OK) {
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}

	/* taken from kerberos.c */
	if (secprops->min_ssf > (mech_ssf + external)) {
	    return SASL_TOOWEAK;
	} else if (secprops->min_ssf > secprops->max_ssf) {
	    return SASL_BADPARAM;
	}
	
	/* need bits of layer -- sasl_ssf_t is unsigned so be careful */
	if (secprops->max_ssf >= external) {
	    allowed = secprops->max_ssf - external;
	} else {
	    allowed = 0;
	}
	if (secprops->min_ssf >= external) {
	    need = secprops->min_ssf - external;
	} else {
	    /* good to go */
	    need = 0;
	}

	/* bit mask of server support */
	serverhas = ((char *)output_token->value)[0];
	
	/* use the strongest layer available */
	if ((text->qop & LAYER_CONFIDENTIALITY) &&
	    allowed >= mech_ssf &&
	    need <= mech_ssf &&
	    (serverhas & LAYER_CONFIDENTIALITY)) {
	    
	    const char *ad_compat;

	    /* encryption */
	    oparams->encode = &gssapi_privacy_encode;
	    oparams->decode = &gssapi_decode;
	    oparams->mech_ssf = mech_ssf;
	    mychoice = LAYER_CONFIDENTIALITY;

	    if (serverhas & LAYER_INTEGRITY) {
		/* should we send an AD compatible choice of security layers? */
		params->utils->getopt(params->utils->getopt_context,
				      "GSSAPI",
				      "ad_compat",
				      &ad_compat,
				      NULL);
		if (ad_compat &&
		    (ad_compat[0] == '1' || ad_compat[0] == 'y' ||
		     (ad_compat[0] == 'o' && ad_compat[1] == 'n') ||
		     ad_compat[0] == 't')) {
		    mychoice = LAYER_INTEGRITY|LAYER_CONFIDENTIALITY;
		}
	    }
	} else if ((text->qop & LAYER_INTEGRITY) &&
		    allowed >= 1 &&
		    need <= 1 &&
		    (serverhas & LAYER_INTEGRITY)) {
	    /* integrity */
	    oparams->encode = &gssapi_integrity_encode;
	    oparams->decode = &gssapi_decode;
	    oparams->mech_ssf = 1;
	    mychoice = LAYER_INTEGRITY;
	} else if ((text->qop & LAYER_NONE) &&
		   need <= 0 && (serverhas & LAYER_NONE)) {
	    /* no layer */
	    oparams->encode = NULL;
	    oparams->decode = NULL;
	    oparams->mech_ssf = 0;
	    mychoice = LAYER_NONE;
	} else {
	    /* there's no appropriate layering for us! */
	    sasl_gss_free_context_contents(text);
	    return SASL_TOOWEAK;
	}
	
        oparams->maxoutbuf =
	    (((unsigned char *) output_token->value)[1] << 16) |
            (((unsigned char *) output_token->value)[2] << 8) |
            (((unsigned char *) output_token->value)[3] << 0);

	GSS_LOCK_MUTEX_CTX(params->utils, text);
	gss_release_buffer(&min_stat, output_token);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);

	if (oparams->mech_ssf) {
            int ret;

            ret = gssapi_wrap_sizes(text, oparams);
            if (ret != SASL_OK) {
	        sasl_gss_free_context_contents(text);
                return ret;
            }
	}
	/* oparams->user is always set, due to canon_user requirements.
	 * Make sure the client actually requested it though, by checking
	 * if our context was set.
	 */
	if (text->user && text->user[0]) {
	    alen = strlen(oparams->user);
	} else {
	    alen = 0;
	}
	
	input_token->length = 4 + alen;
	input_token->value =
	    (char *)params->utils->malloc((input_token->length + 1)*sizeof(char));
	if (input_token->value == NULL) {
	    sasl_gss_free_context_contents(text);
	    return SASL_NOMEM;
	}
	
	if (alen)
	    memcpy((char *)input_token->value+4,oparams->user,alen);

	/* build up our security properties token */
	if (mychoice > 1) {
	    if (params->props.maxbufsize > 0xFFFFFF) {
		/* make sure maxbufsize isn't too large */
		/* maxbufsize = 0xFFFFFF */
		((unsigned char *)input_token->value)[1] = 0xFF;
		((unsigned char *)input_token->value)[2] = 0xFF;
		((unsigned char *)input_token->value)[3] = 0xFF;
	    } else {
		((unsigned char *)input_token->value)[1] = 
		    (params->props.maxbufsize >> 16) & 0xFF;
		((unsigned char *)input_token->value)[2] = 
		    (params->props.maxbufsize >> 8) & 0xFF;
		((unsigned char *)input_token->value)[3] = 
		    (params->props.maxbufsize >> 0) & 0xFF;
	    }
	} else {
	    ((unsigned char *)input_token->value)[1] = 0;
	    ((unsigned char *)input_token->value)[2] = 0;
	    ((unsigned char *)input_token->value)[3] = 0;
	}
	((unsigned char *)input_token->value)[0] = mychoice;
	
	GSS_LOCK_MUTEX_CTX(params->utils, text);
	maj_stat = gss_wrap (&min_stat,
			     text->gss_ctx,
			     0, /* Just integrity checking here */
			     GSS_C_QOP_DEFAULT,
			     input_token,
			     NULL,
			     output_token);
	GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	
	params->utils->free(input_token->value);
	input_token->value = NULL;
	
	if (GSS_ERROR(maj_stat)) {
	    sasl_gss_seterror(text->utils, maj_stat, min_stat);
	    if (output_token->value) {
		GSS_LOCK_MUTEX_CTX(params->utils, text);
		gss_release_buffer(&min_stat, output_token);
		GSS_UNLOCK_MUTEX_CTX(params->utils, text);
	    }
	    sasl_gss_free_context_contents(text);
	    return SASL_FAIL;
	}
	
	if (clientoutlen) {
	    *clientoutlen = output_token->length;
	}
	if (output_token->value) {
	    if (clientout && clientoutlen) {
		ret = _plug_buf_alloc(text->utils,
				      &(text->out_buf),
				      &(text->out_buf_len),
				      *clientoutlen);
		if (ret != SASL_OK) {
		    GSS_LOCK_MUTEX_CTX(params->utils, text);
		    gss_release_buffer(&min_stat, output_token);
		    GSS_UNLOCK_MUTEX_CTX(params->utils, text);
		    return ret;
		}
		memcpy(text->out_buf, output_token->value, *clientoutlen);
		*clientout = text->out_buf;
	    }
	    
	    GSS_LOCK_MUTEX_CTX(params->utils, text);
	    gss_release_buffer(&min_stat, output_token);
	    GSS_UNLOCK_MUTEX_CTX(params->utils, text);

	}
	
	text->state = SASL_GSSAPI_STATE_AUTHENTICATED;
	
	oparams->doneflag = 1;
	
	/* used by layers */
	_plug_decode_init(&text->decode_context, text->utils,
			  (params->props.maxbufsize > 0xFFFFFF) ? 0xFFFFFF :
			  params->props.maxbufsize);
	
	return SASL_OK;
    }
	
    default:
	params->utils->log(params->utils->conn, SASL_LOG_ERR,
			   "Invalid GSSAPI client step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static const unsigned long gssapi_required_prompts[] = {
    SASL_CB_LIST_END
};  

static sasl_client_plug_t gssapi_client_plugins[] = 
{
    {
	"GSSAPI",			/* mech_name */
	K5_MAX_SSF,			/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH 
	| SASL_SEC_PASS_CREDENTIALS,    /* security_flags */
	SASL_FEAT_NEEDSERVERFQDN
	| SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	gssapi_required_prompts,	/* required_prompts */
	GSS_C_NO_OID,			/* glob_context */
	&gssapi_client_mech_new,	/* mech_new */
	&gssapi_client_mech_step,	/* mech_step */
	&gssapi_common_mech_dispose,	/* mech_dispose */
	&gssapi_common_mech_free,	/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
#ifdef HAVE_GSS_SPNEGO
    ,{
	"GSS-SPNEGO",			/* mech_name */
	K5_MAX_SSF,			/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH 
	| SASL_SEC_PASS_CREDENTIALS,    /* security_flags */
	SASL_FEAT_NEEDSERVERFQDN
	| SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY
	| SASL_FEAT_SUPPORTS_HTTP,	/* features */
	gssapi_required_prompts,	/* required_prompts */
	&gss_spnego_oid,		/* glob_context */
	&gssapi_client_mech_new,	/* mech_new */
	&gssapi_client_mech_step,	/* mech_step */
	&gssapi_common_mech_dispose,	/* mech_dispose */
	&gssapi_common_mech_free,	/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
#endif
};

int gssapiv2_client_plug_init(const sasl_utils_t *utils __attribute__((unused)), 
			      int maxversion,
			      int *out_version, 
			      sasl_client_plug_t **pluglist,
			      int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR(utils, "Version mismatch in GSSAPI");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = gssapi_client_plugins;
#ifdef HAVE_GSS_SPNEGO
    *plugcount = 2;
#else
    *plugcount = 1;
#endif

#ifdef GSS_USE_MUTEXES
    if(!gss_mutex) {
      gss_mutex = utils->mutex_alloc();
      if(!gss_mutex) {
        return SASL_FAIL;
      }
    }
#endif
    
    return SASL_OK;
}
