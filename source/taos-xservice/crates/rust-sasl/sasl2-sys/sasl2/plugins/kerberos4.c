/* Kerberos4 SASL plugin
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
#include <stdlib.h>
#include <string.h>
#include <krb.h>

#ifdef WITH_DES
# ifdef WITH_SSL_DES
#  include <openssl/des.h>
# else
#  include <des.h>
# endif /* WITH_SSL_DES */
#endif /* WITH_DES */

#ifdef WIN32
# include <winsock2.h>
#elif defined(macintosh)
#include <kcglue_krb.h>
#else
# include <sys/param.h>
# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>
# include <netdb.h>
#endif /* WIN32 */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <fcntl.h>
#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>

#include <errno.h>
#include <ctype.h>

#include "plugin_common.h"

#ifdef macintosh
/*
 * krb.h doenst include some functions and mac compiler is picky
 * about declartions
 */
#include <extra_krb.h>
#include <sasl_kerberos4_plugin_decl.h>
#endif

#ifdef WIN32
/* This must be after sasl.h, saslutil.h */
# include "saslKERBEROSV4.h"

/* KClient doesn't define this */
typedef struct krb_principal {
    char name[ANAME_SZ];
    char instance[INST_SZ];
    char realm[REALM_SZ];
} krb_principal;

/* This isn't defined under WIN32.  For access() */
#ifndef R_OK
#define R_OK 04
#endif
/* we also need io.h for access() prototype */
#include <io.h>
#endif /* WIN32 */

/*****************************  Common Section  *****************************/

#ifndef KEYFILE
#define KEYFILE "/etc/srvtab";
#endif

#define KRB_SECFLAG_NONE (1)
#define KRB_SECFLAG_INTEGRITY (2)
#define KRB_SECFLAG_ENCRYPTION (4)
#define KRB_SECFLAGS (7)
#define KRB_SECFLAG_CREDENTIALS (8)

#define KRB_DES_SECURITY_BITS (56)
#define KRB_INTEGRITY_BITS (1)

typedef enum Krb_sec {
    KRB_SEC_NONE = 0,
    KRB_SEC_INTEGRITY = 1,
    KRB_SEC_ENCRYPTION = 2
} Krb_sec_t;

typedef struct context {
    int state;
    
    int challenge;         /* this is the challenge (32 bit int) used 
			      for the authentication */
    
    char *service;                   /* kerberos service */
    char instance[ANAME_SZ];
    char pname[ANAME_SZ];
    char pinst[INST_SZ];
    char prealm[REALM_SZ];
    char *hostname;                  /* hostname */
    char *realm;                     /* kerberos realm */
    char *auth;                      /* */
    
    CREDENTIALS credentials;
    
    des_cblock key;                  /* session key */
    des_cblock session;              /* session key */
    
    des_key_schedule init_keysched;  /* key schedule for initialization */
    des_key_schedule enc_keysched;   /* encryption key schedule */
    des_key_schedule dec_keysched;   /* decryption key schedule */
    
    
    struct sockaddr_in ip_local;     /* local ip address and port.
					needed for layers */
    struct sockaddr_in ip_remote;    /* remote ip address and port.
					needed for layers */
    
    const sasl_utils_t *utils;       /* this is useful to have around */
    
    Krb_sec_t sec_type;
    char *encode_buf;                /* For encoding/decoding mem management */
    char *decode_buf;
    char *decode_once_buf;
    unsigned encode_buf_len;
    unsigned decode_buf_len;
    unsigned decode_once_buf_len;
    buffer_info_t *enc_in_buf;

    decode_context_t decode_context;
    
    char *out_buf;                   /* per-step mem management */
    unsigned out_buf_len;
    
    const char *user;                      /* used by client */
    
    int secflags; /* client/server supports layers? */
    
    long time_sec; /* These are used to make sure we are getting */
    char time_5ms; /* strictly increasing timestamps */
    
} context_t;

#define KRB_LOCK_MUTEX(utils)  \
    if(((sasl_utils_t *)(utils))->mutex_lock(krb_mutex) != 0) { \
       ((sasl_utils_t *)(utils))->seterror(((sasl_utils_t *)(utils))->conn, \
                                           0, "error locking mutex"); \
			           return SASL_FAIL; \
                                }
#define KRB_UNLOCK_MUTEX(utils) \
    if(((sasl_utils_t *)(utils))->mutex_unlock(krb_mutex) != 0) { \
       ((sasl_utils_t *)(utils))->seterror(((sasl_utils_t *)(utils))->conn, \
                                           0, "error unlocking mutex"); \
			           return SASL_FAIL; \
                                }

/* Mutex for not-thread-safe kerberos 4 library */
static void *krb_mutex = NULL;
static char *srvtab = NULL;
static unsigned refcount = 0;

static int kerberosv4_encode(void *context,
			     const struct iovec *invec,
			     unsigned numiov,
			     const char **output,
			     unsigned *outputlen)
{
    int len, ret;
    context_t *text = (context_t *)context;
    struct buffer_info *inblob, bufinfo;
    
    if(numiov > 1) {
	ret = _plug_iovec_to_buf(text->utils, invec, numiov, &text->enc_in_buf);
	if(ret != SASL_OK) return ret;
	inblob = text->enc_in_buf;
    } else {
	bufinfo.data = invec[0].iov_base;
	bufinfo.curlen = invec[0].iov_len;
	inblob = &bufinfo;
    }
    
    ret = _plug_buf_alloc(text->utils, &(text->encode_buf),
			  &text->encode_buf_len, inblob->curlen+40);
    
    if(ret != SASL_OK) return ret;
    
    KRB_LOCK_MUTEX(text->utils);
    
    if (text->sec_type == KRB_SEC_ENCRYPTION) {
	/* Type incompatibility on 4th arg probably means you're
	   building against krb4 in MIT krb5, but got the OpenSSL
	   headers in your way. You need to not use openssl/des.h with
	   MIT kerberos. */
	len=krb_mk_priv(inblob->data, (text->encode_buf+4),
			inblob->curlen,  text->init_keysched, 
			&text->session, &text->ip_local,
			&text->ip_remote);
    } else if (text->sec_type == KRB_SEC_INTEGRITY) {
	len=krb_mk_safe(inblob->data, (text->encode_buf+4),
			inblob->curlen,
			&text->session, &text->ip_local, &text->ip_remote);
    } else {
	len = -1;
    }
    
    KRB_UNLOCK_MUTEX(text->utils);
    
    /* returns -1 on error */
    if (len==-1) return SASL_FAIL;
    
    /* now copy in the len of the buffer in network byte order */
    *outputlen=len+4;
    len=htonl(len);
    memcpy(text->encode_buf, &len, 4);
    
    /* Setup the const pointer */
    *output = text->encode_buf;
    
    return SASL_OK;
}

static int kerberosv4_decode_packet(void *context,
				    const char *input, unsigned inputlen,
				    char **output, unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    int result;
    MSG_DAT data;
    
    memset(&data,0,sizeof(MSG_DAT));
    
    KRB_LOCK_MUTEX(text->utils);
    
    if (text->sec_type == KRB_SEC_ENCRYPTION) {
	result=krb_rd_priv(input, inputlen, text->init_keysched, 
			   &text->session, &text->ip_remote,
			   &text->ip_local, &data);
    } else if (text->sec_type == KRB_SEC_INTEGRITY) {
        result = krb_rd_safe(input, inputlen,
			     &text->session, &text->ip_remote,
			     &text->ip_local, &data);
    } else {
        KRB_UNLOCK_MUTEX(text->utils);
	text->utils->seterror(text->utils->conn, 0,
			      "KERBEROS_4 decode called with KRB_SEC_NONE");
	return SASL_FAIL;
    }
    
    KRB_UNLOCK_MUTEX(text->utils);
    
    /* see if the krb library gave us a failure */
    if (result != 0) {
	text->utils->seterror(text->utils->conn, 0, get_krb_err_txt(result));
	return SASL_FAIL;
    }
    
    /* check to make sure the timestamps are ok */
    if ((data.time_sec < text->time_sec) || /* if an earlier time */
	(((data.time_sec == text->time_sec) && /* or the exact same time */
	  (data.time_5ms < text->time_5ms)))) 
	{
	    text->utils->seterror(text->utils->conn, 0, "timestamps not ok");
	    return SASL_FAIL;
	}
    
    text->time_sec = data.time_sec;
    text->time_5ms = data.time_5ms;
    
    result = _plug_buf_alloc(text->utils, &text->decode_once_buf,
			     &text->decode_once_buf_len,
			     data.app_length + 1);
    if(result != SASL_OK)
	return result;
    
    *output = text->decode_once_buf;
    *outputlen = data.app_length;
    memcpy(*output, data.app_data, data.app_length);
    (*output)[*outputlen] = '\0';
    
    return SASL_OK;
}

static int kerberosv4_decode(void *context,
			     const char *input, unsigned inputlen,
			     const char **output, unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    int ret;
    
    ret = _plug_decode(&text->decode_context, input, inputlen,
		       &text->decode_buf, &text->decode_buf_len, outputlen,
		       kerberosv4_decode_packet, text);
    
    *output = text->decode_buf;
    
    return ret;
}

static int new_text(const sasl_utils_t *utils, context_t **text)
{
    context_t *ret = (context_t *) utils->malloc(sizeof(context_t));

    if (ret == NULL) {
	MEMERROR(utils);
	return SASL_NOMEM;
    }
    
    memset(ret, 0, sizeof(context_t));
    
    ret->state = 1;
    ret->utils = utils;
    
    *text = ret;
    
    return SASL_OK;
}

static void kerberosv4_common_mech_dispose(void *conn_context,
					   const sasl_utils_t *utils)
{
    context_t *text = (context_t *)conn_context;
    
    if(!text) return;
    
    _plug_decode_free(&text->decode_context);
    if (text->encode_buf) utils->free(text->encode_buf);
    if (text->decode_buf) utils->free(text->decode_buf);
    if (text->decode_once_buf) utils->free(text->decode_once_buf);
    if (text->out_buf) utils->free(text->out_buf);
    if (text->enc_in_buf) {
	if(text->enc_in_buf->data) utils->free(text->enc_in_buf->data);
	utils->free(text->enc_in_buf);
    }
    /* no need to free userid, it's just the interaction result */
    
    utils->free(text);
}

static void
kerberosv4_common_mech_free(void *glob_context __attribute__((unused)),
			    const sasl_utils_t *utils)
{
    if (krb_mutex) {
	utils->mutex_free(krb_mutex);
	krb_mutex = NULL; /* in case we need to re-use it */
    }
    refcount--;
    if (srvtab && !refcount) {
	utils->free(srvtab);
	srvtab = NULL;
    }
}

/*****************************  Server Section  *****************************/

static int cando_sec(sasl_security_properties_t *props,
		     int external_ssf,
		     int secflag)
{
    int need;
    int musthave;
    
    if(props->maxbufsize == 0) {
	need = musthave = 0;
    } else {
	need = props->max_ssf - external_ssf;
	musthave = props->min_ssf - external_ssf;
    }

    switch (secflag) {
    case KRB_SECFLAG_NONE:
	if (musthave <= 0)
	    return 1;
	break;
    case KRB_SECFLAG_INTEGRITY:
	if ((musthave <= KRB_INTEGRITY_BITS)
	    && (KRB_INTEGRITY_BITS <= need))
	    return 1;
	break;
    case KRB_SECFLAG_ENCRYPTION:
	if ((musthave <= KRB_DES_SECURITY_BITS)
	    && (KRB_DES_SECURITY_BITS <= need))
	    return 1;
	break;
    case KRB_SECFLAG_CREDENTIALS:
	if (props->security_flags & SASL_SEC_PASS_CREDENTIALS)
	    return 1;
	break;
    }
    return 0;
}

static int ipv4_ipfromstring(const sasl_utils_t *utils, const char *addr,
			     struct sockaddr_in *out) 
{
    struct sockaddr_storage ss;
    int result;
    
    result = _plug_ipfromstring(utils, addr,
				(struct sockaddr *)&ss, sizeof(ss));
    if (result != SASL_OK) {
	/* couldn't get local IP address */
	return result;
    }
    /* Kerberos_V4 supports only IPv4 */
    if (((struct sockaddr *)&ss)->sa_family != AF_INET)
	return SASL_FAIL;
    memcpy(out, &ss, sizeof(struct sockaddr_in));
    
    return SASL_OK;
}

#ifndef macintosh
static int
kerberosv4_server_mech_new(void *glob_context __attribute__((unused)),
			   sasl_server_params_t *sparams,
			   const char *challenge __attribute__((unused)),
			   unsigned challen __attribute__((unused)),
			   void **conn_context)
{
    return new_text(sparams->utils, (context_t **) conn_context);
}

static int kerberosv4_server_mech_step(void *conn_context,
				       sasl_server_params_t *sparams,
				       const char *clientin,
				       unsigned clientinlen,
				       const char **serverout,
				       unsigned *serveroutlen,
				       sasl_out_params_t *oparams)
{
    context_t *text = (context_t *) conn_context;
    int result;

    *serverout = NULL;
    *serveroutlen = 0;
    
    switch (text->state) {

    case 1: {
	/* random 32-bit number */
	int randocts, nchal;
	
	/* shouldn't we check for erroneous client input here?!? */
	
	sparams->utils->rand(sparams->utils->rpool,(char *) &randocts ,
			     sizeof(randocts));    
	text->challenge=randocts; 
	nchal = htonl(text->challenge);
	
	result = _plug_buf_alloc(text->utils, &text->out_buf,
				 &text->out_buf_len, 5);
	if (result != SASL_OK) return result;
	
	memcpy(text->out_buf,&nchal,4);
	*serverout = text->out_buf;
	*serveroutlen = 4;
	
	text->state = 2;

	return SASL_CONTINUE;
    }
    
    case 2: {
	int nchal;
	unsigned char sout[8];  
	AUTH_DAT ad;
	KTEXT_ST ticket;
	unsigned lup;
	struct sockaddr_in addr;
	char *dot;
	
	/* received authenticator */
	
	/* create ticket */
	if (clientinlen > MAX_KTXT_LEN) {
	    text->utils->seterror(text->utils->conn,0,
				  "request larger than maximum ticket size");
	    return SASL_FAIL;
	}
	
	ticket.length=clientinlen;
	for (lup = 0; lup < clientinlen; lup++)      
	    ticket.dat[lup] = clientin[lup];
	
	KRB_LOCK_MUTEX(sparams->utils);
	
	text->realm = krb_realmofhost(sparams->serverFQDN);
	
	/* get instance */
	strncpy (text->instance, krb_get_phost (sparams->serverFQDN),
		 sizeof (text->instance));
	
	KRB_UNLOCK_MUTEX(sparams->utils);
	
	text->instance[sizeof(text->instance)-1] = 0;

	/* At some sites, krb_get_phost() sensibly but
	 * atypically returns FQDNs, versus the first component,
	 * which is what we need for RFC2222 section 7.1
	 */
	dot = strchr(text->instance, '.');
	if (dot) *dot = '\0';

	memset(&addr, 0, sizeof(struct sockaddr_in));
	
#ifndef KRB4_IGNORE_IP_ADDRESS
	/* (we ignore IP addresses in krb4 tickets at CMU to facilitate moving
	   from machine to machine) */
	
	/* get ip number in addr*/
	result = ipv4_ipfromstring(sparams->utils, sparams->ipremoteport, &addr);
	if (result != SASL_OK || !sparams->ipremoteport) {
	    SETERROR(text->utils, "couldn't get remote IP address");
	    return result;
	}
#endif
	
	/* check ticket */
	
	KRB_LOCK_MUTEX(sparams->utils);
	result = krb_rd_req(&ticket, (char *) sparams->service, text->instance, 
			    addr.sin_addr.s_addr, &ad, srvtab);
	KRB_UNLOCK_MUTEX(sparams->utils);
	
	if (result) { /* if fails mechanism fails */
	    text->utils->seterror(text->utils->conn,0,
				  "krb_rd_req failed service=%s instance=%s error code=%s (%i)",
				  sparams->service, text->instance,get_krb_err_txt(result),result);
	    return SASL_BADAUTH;
	}
	
	/* 8 octets of data
	 * 1-4 checksum+1
	 * 5 security layers
	 * 6-8max cipher text buffer size
	 * use DES ECB in the session key
	 */
	
	nchal=htonl(text->challenge+1);
	memcpy(sout, &nchal, 4);
	sout[4]= 0;
	if (cando_sec(&sparams->props, sparams->external_ssf,
		      KRB_SECFLAG_NONE))
	    sout[4] |= KRB_SECFLAG_NONE;
	if (cando_sec(&sparams->props, sparams->external_ssf,
		      KRB_SECFLAG_INTEGRITY))
	    sout[4] |= KRB_SECFLAG_INTEGRITY;
	if (cando_sec(&sparams->props, sparams->external_ssf,
		      KRB_SECFLAG_ENCRYPTION))
	    sout[4] |= KRB_SECFLAG_ENCRYPTION;
	if (cando_sec(&sparams->props, sparams->external_ssf,
		      KRB_SECFLAG_CREDENTIALS))
	    sout[4] |= KRB_SECFLAG_CREDENTIALS;

	if(sparams->props.maxbufsize) {
	    int tmpmaxbuf = (sparams->props.maxbufsize > 0xFFFFFF) ? 0xFFFFFF : sparams->props.maxbufsize;

	    sout[5]=((tmpmaxbuf >> 16) & 0xFF);
	    sout[6]=((tmpmaxbuf >> 8) & 0xFF);
	    sout[7]=(tmpmaxbuf & 0xFF);
	} else {
            /* let's say we can support up to 64K */
	    /* no inherent inability with our layers to support more */

	    sout[5]=0x00;  /* max ciphertext buffer size */
	    sout[6]=0xFF;
	    sout[7]=0xFF;
	}
    
	memcpy(text->session, ad.session, 8);
	memcpy(text->pname, ad.pname, sizeof(text->pname));
	memcpy(text->pinst, ad.pinst, sizeof(text->pinst));
	memcpy(text->prealm, ad.prealm, sizeof(text->prealm));
	des_key_sched(&ad.session, text->init_keysched);
	
	/* make keyschedule for encryption and decryption */
	des_key_sched(&ad.session, text->enc_keysched);
	des_key_sched(&ad.session, text->dec_keysched);
	
	des_ecb_encrypt((des_cblock *)sout,
			(des_cblock *)sout,
			text->init_keysched,
			DES_ENCRYPT);
	
	result = _plug_buf_alloc(text->utils, &text->out_buf,
				 &text->out_buf_len, 9);
	if(result != SASL_OK)
	    return result;
	
	memcpy(text->out_buf,&sout,8);
	*serverout = text->out_buf;
	*serveroutlen = 8;
	
	text->state = 3;

	return SASL_CONTINUE;
    }
    
    case 3: {
	int result;
	int testnum;
	int flag;
	unsigned char *in;
	
	if ((clientinlen == 0) || (clientinlen % 8 != 0)) {
	    text->utils->seterror(text->utils->conn,0,
				  "Response to challengs is not a multiple of 8 octets (a DES block)");
	    return SASL_FAIL;	
	}
	
	/* we need to make a copy because des does in place decrpytion */
	in = sparams->utils->malloc(clientinlen + 1);
	if (in == NULL) {
	    MEMERROR(sparams->utils);
	    return SASL_NOMEM;
	}
	
	memcpy(in, clientin, clientinlen);
	in[clientinlen] = '\0';
	
	/* decrypt; verify checksum */
	
	des_pcbc_encrypt((des_cblock *)in,
			 (des_cblock *)in,
			 clientinlen,
			 text->init_keysched,
			 &text->session,
			 DES_DECRYPT);
	
	testnum = (in[0]*256*256*256)+(in[1]*256*256)+(in[2]*256)+in[3];
	
	if (testnum != text->challenge) {
	    SETERROR(sparams->utils, "incorrect response to challenge");
	    return SASL_BADAUTH;
	}
	
	if (!cando_sec(&sparams->props, sparams->external_ssf,
		       in[4] & KRB_SECFLAGS)) {
	    SETERROR(sparams->utils,
		     "invalid security property specified");
	    return SASL_BADPROT;
	}
	
	oparams->encode = &kerberosv4_encode;
	oparams->decode = &kerberosv4_decode;
	
	switch (in[4] & KRB_SECFLAGS) {
	case KRB_SECFLAG_NONE:
	    text->sec_type = KRB_SEC_NONE;
	    oparams->encode = NULL;
	    oparams->decode = NULL;
	    oparams->mech_ssf = 0;
	    break;
	case KRB_SECFLAG_INTEGRITY:
	    text->sec_type = KRB_SEC_INTEGRITY;
	    oparams->mech_ssf = KRB_INTEGRITY_BITS;
	    break;
	case KRB_SECFLAG_ENCRYPTION:
	    text->sec_type = KRB_SEC_ENCRYPTION;
	    oparams->mech_ssf = KRB_DES_SECURITY_BITS;
	    break;
	default:
	    /* Mark that we tried */
	    oparams->mech_ssf = 2;
	    SETERROR(sparams->utils, "not a supported encryption layer");
	    return SASL_BADPROT;
	}
	
	/* get ip data */
	/* get ip number in addr*/
	result = ipv4_ipfromstring(sparams->utils,
				   sparams->iplocalport, &(text->ip_local));
	if (result != SASL_OK) {
	    SETERROR(sparams->utils, "couldn't get local ip address");
	    /* couldn't get local IP address */
	    return result;
	}
	
	result = ipv4_ipfromstring(sparams->utils,
				   sparams->ipremoteport, &(text->ip_remote));
	if (result != SASL_OK) {
	    SETERROR(sparams->utils, "couldn't get remote ip address");
	    /* couldn't get remote IP address */
	    return result;
	}
	
	/* fill in oparams */
	oparams->maxoutbuf = (in[5] << 16) + (in[6] << 8) + in[7];
	if(oparams->mech_ssf) {
	    /* FIXME: Likely to be too large */
	    oparams->maxoutbuf -= 50;
	}
	
	if (sparams->canon_user) {
	    char *user=NULL, *authid=NULL;
	    size_t ulen = 0, alen = strlen(text->pname);
	    int ret, cflag = SASL_CU_AUTHID | SASL_CU_EXTERNALLY_VERIFIED;
	    
	    if (text->pinst[0]) {
		alen += strlen(text->pinst) + 1 /* for the . */;
	    }
	    flag = 0;
	    if (strcmp(text->realm, text->prealm)) {
		alen += strlen(text->prealm) + 1 /* for the @ */;
		flag = 1;
	    }
	    
	    authid = sparams->utils->malloc(alen + 1);
	    if (!authid) {
		MEMERROR(sparams->utils);
		return SASL_NOMEM;
	    }
	    
	    strcpy(authid, text->pname);
	    if (text->pinst[0]) {
		strcat(authid, ".");
		strcat(authid, text->pinst);
	    }
	    if (flag) {
		strcat(authid, "@");
		strcat(authid, text->prealm);
	    }
	    
	    if (in[8]) {
		user = sparams->utils->malloc(strlen((char *) in + 8) + 1);
		if (!user) {
		    MEMERROR(sparams->utils);
		    return SASL_NOMEM;
		}
		
		strcpy(user, (char *) in + 8);
		ulen = strlen(user);
	    } else {
	    	cflag |= SASL_CU_AUTHZID;
	    }
	    
	    ret = sparams->canon_user(sparams->utils->conn, authid, alen,
				      cflag, oparams);
	    sparams->utils->free(authid);
	    if (ret != SASL_OK) {
		if (user)
		    sparams->utils->free(user);
		return ret;
	    }
	    
	    if (user) {
	    	ret = sparams->canon_user(sparams->utils->conn, user, ulen,
				      SASL_CU_AUTHZID, oparams);
	    
		sparams->utils->free(user);
	    }
	    
	    if (ret != SASL_OK) return ret;
	}
	
	/* nothing more to do; authenticated */
	oparams->doneflag = 1;
	oparams->param_version = 0;
	
	/* used by layers */
	_plug_decode_init(&text->decode_context, text->utils,
			  (sparams->props.maxbufsize > 0xFFFFFF) ? 0xFFFFFF :
			  sparams->props.maxbufsize);
	
	sparams->utils->free(in);

	return SASL_OK;
    }
    
    default:
	sparams->utils->log(NULL, SASL_LOG_ERR,
			    "Invalid Kerberos server step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static int kerberosv4_mech_avail(void *glob_context __attribute__((unused)),
				 sasl_server_params_t *sparams,
				 void **conn_context __attribute__((unused))) 
{
    struct sockaddr_in addr;
    
    if (!sparams->iplocalport || !sparams->ipremoteport
	|| ipv4_ipfromstring(sparams->utils,
			     sparams->iplocalport, &addr) != SASL_OK
	|| ipv4_ipfromstring(sparams->utils,
			     sparams->ipremoteport, &addr) != SASL_OK) {
	SETERROR(sparams->utils,
		 "KERBEROS_V4 unavailable due to lack of IPv4 information");
	return SASL_NOMECH;
    }
    
    return SASL_OK;
}


static sasl_server_plug_t kerberosv4_server_plugins[] = 
{
    {
	"KERBEROS_V4",			/* mech_name */
	KRB_DES_SECURITY_BITS,		/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_SERVER_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	NULL,				/* glob_context */
	&kerberosv4_server_mech_new,	/* mech_new */
	&kerberosv4_server_mech_step,	/* mech_step */
	&kerberosv4_common_mech_dispose,/* mech_dispose */
	&kerberosv4_common_mech_free,	/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	&kerberosv4_mech_avail,		/* mech_avail */
	NULL				/* spare */
    }
};
#endif /* macintosh */

int kerberos4_server_plug_init(const sasl_utils_t *utils,
			       int maxversion,
			       int *out_version,
			       sasl_server_plug_t **pluglist,
			       int *plugcount)
{
#ifdef macintosh
    return SASL_BADVERS;
#else
    const char *ret;
    unsigned int rl;
    
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	return SASL_BADVERS;
    }
    
    
    if (!krb_mutex) {
	krb_mutex = utils->mutex_alloc();
	if(!krb_mutex) {
	    return SASL_FAIL;
	}
    }
    
    if (!srvtab) {	
	utils->getopt(utils->getopt_context,
		      "KERBEROS_V4", "srvtab", &ret, &rl);
	
	if (ret == NULL) {
	    ret = KEYFILE;
	    rl = strlen(ret);
	}
	
	srvtab = utils->malloc(sizeof(char) * (rl + 1));
	if(!srvtab) {
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}
	
	strcpy(srvtab, ret);
    }
    
    refcount++;
    
    /* fail if we can't open the srvtab file */
    if (access(srvtab, R_OK) != 0) {
	utils->log(NULL, SASL_LOG_ERR,
		   "can't access srvtab file %s: %m", srvtab, errno);
	if(!(--refcount)) {
	    utils->free(srvtab);
	    srvtab=NULL;
	}
	return SASL_FAIL;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = kerberosv4_server_plugins;
    *plugcount = 1;
    
    return SASL_OK;
#endif
}

/*****************************  Client Section  *****************************/

static int
kerberosv4_client_mech_new(void *glob_context __attribute__((unused)), 
			   sasl_client_params_t *params,
			   void **conn_context)
{
    return new_text(params->utils, (context_t **) conn_context);
}

static int kerberosv4_client_mech_step(void *conn_context,
				       sasl_client_params_t *cparams,
				       const char *serverin,
				       unsigned serverinlen,
				       sasl_interact_t **prompt_need,
				       const char **clientout,
				       unsigned *clientoutlen,
				       sasl_out_params_t *oparams)
{
    context_t *text = (context_t *) conn_context;
    KTEXT_ST authent;
    int ret;

    *clientout = NULL;
    *clientoutlen = 0;
    
    authent.length = MAX_KTXT_LEN;
    
    switch (text->state) {

    case 1: {
	/* We should've just recieved a 32-bit number in network byte order.
	 * We want to reply with an authenticator. */
	int result;
	KTEXT_ST ticket;
	char *dot;
	
	memset(&ticket, 0L, sizeof(ticket));
	ticket.length = MAX_KTXT_LEN;   
	
	if (serverinlen != 4) {
	    text->utils->seterror(text->utils->conn, 0,
				  "server challenge not 4 bytes long");
	    return SASL_BADPROT; 
	}
	
	memcpy(&text->challenge, serverin, 4);
	
	text->challenge=ntohl(text->challenge); 
	
	if (cparams->serverFQDN == NULL) {
	    cparams->utils->log(NULL, SASL_LOG_ERR,
				"no 'serverFQDN' set");
	    SETERROR(text->utils, "paramater error");
	    return SASL_BADPARAM;
	}
	if (cparams->service == NULL) {
	    cparams->utils->log(NULL, SASL_LOG_ERR,
				"no 'service' set");
	    SETERROR(text->utils, "paramater error");
	    return SASL_BADPARAM;
	}
	
	KRB_LOCK_MUTEX(cparams->utils);
	text->realm=krb_realmofhost(cparams->serverFQDN);
	text->hostname=(char *) cparams->serverFQDN;
	
	/* the instance of the principal we're authenticating with */
	strncpy (text->instance, krb_get_phost (cparams->serverFQDN), 
		 sizeof (text->instance));
	
	/* text->instance is NULL terminated unless it was too long */
	text->instance[sizeof(text->instance)-1] = '\0';

	/* At some sites, krb_get_phost() sensibly but
	 * atypically returns FQDNs, versus the first component,
	 * which is what we need for RFC2222 section 7.1
	 */
	dot = strchr(text->instance, '.');
	if (dot) *dot = '\0';
	
#ifndef macintosh
	if ((result = krb_mk_req(&ticket, (char *) cparams->service, 
				 text->instance, text->realm, text->challenge)))
#else
	    memset(&text->credentials,0,sizeof(text->credentials));
	if (kcglue_krb_mk_req(ticket.dat,
			      &ticket.length,
			      cparams->service,
			      text->instance,
			      text->realm,
			      text->challenge,
			      &text->credentials.session,
			      text->credentials.pname,
			      text->credentials.pinst) != 0)
#endif
	    {
		KRB_UNLOCK_MUTEX(cparams->utils);
		
		text->utils->seterror(text->utils->conn,SASL_NOLOG,
				      "krb_mk_req() failed");
		
		cparams->utils->log(NULL, SASL_LOG_ERR, 
				    "krb_mk_req() failed: %s (%d)",
				    get_krb_err_txt(result), result);
		return SASL_FAIL;
	    }
	
	KRB_UNLOCK_MUTEX(cparams->utils);
	
	ret = _plug_buf_alloc(text->utils, &(text->out_buf),
			      &(text->out_buf_len), ticket.length);
	if (ret != SASL_OK) return ret;
	
	memcpy(text->out_buf, ticket.dat, ticket.length);
	
	*clientout = text->out_buf;
	*clientoutlen = ticket.length;
	
	text->state = 2;

	return SASL_CONTINUE;
    }
    
    /* challenge #2 */
    case 2: {
	int need = 0;
	int musthave = 0;
	int testnum;
	int nchal;    
	unsigned char *sout = NULL;
	unsigned len;
	unsigned char in[8];
	int result;
	int servermaxbuf;
	char *buf;
	int user_result = SASL_OK;
	
	/* try to get the authid */
	if (text->user == NULL) {
	    user_result = _plug_get_userid(cparams->utils, &text->user,
					   prompt_need);
	    
	    if (user_result != SASL_OK && user_result != SASL_INTERACT)
		return user_result;
	}
	
	/* free prompts we got */
	if (prompt_need && *prompt_need) {
	    cparams->utils->free(*prompt_need);
	    *prompt_need = NULL;
	}
	
	/* if there are prompts not filled in */
	if (user_result == SASL_INTERACT) {
	    /* make the prompt list */
	    int result =
		_plug_make_prompts(cparams->utils, prompt_need,
				   user_result == SASL_INTERACT ?
				   "Please enter your authorization name" : NULL, NULL,
				   NULL, NULL,
				   NULL, NULL,
				   NULL, NULL, NULL,
				   NULL, NULL, NULL);
	    if (result!=SASL_OK) return result;
	    
	    return SASL_INTERACT;
	}
	
	/* must be 8 octets */
	if (serverinlen!=8) {
	    SETERROR(cparams->utils,
		     "server response not 8 bytes long");
	    return SASL_BADAUTH;
	}
	
	memcpy(in, serverin, 8);
	
#ifndef macintosh
	/* get credentials */
	KRB_LOCK_MUTEX(cparams->utils);
	result = krb_get_cred((char *)cparams->service,
			      text->instance,
			      text->realm,
			      &text->credentials);
	KRB_UNLOCK_MUTEX(cparams->utils);
	
	if(result != 0) {
	    cparams->utils->log(NULL, SASL_LOG_ERR,
				"krb_get_cred() failed: %s (%d)",
				get_krb_err_txt(result), result);
	    SETERROR(cparams->utils, "krb_get_cred() failed");
	    return SASL_BADAUTH;
	}
#endif
	memcpy(text->session, text->credentials.session, 8);
	
	/* make key schedule for encryption and decryption */
	des_key_sched(&text->session, text->init_keysched);
	des_key_sched(&text->session, text->enc_keysched);
	des_key_sched(&text->session, text->dec_keysched);
	
	/* decrypt from server */
	des_ecb_encrypt((des_cblock *)in, (des_cblock *)in,
			text->init_keysched, DES_DECRYPT);
	
	/* convert to 32bit int */
	testnum = (in[0]*256*256*256)+(in[1]*256*256)+(in[2]*256)+in[3];
	
	/* verify data 1st 4 octets must be equal to chal+1 */
	if (testnum != text->challenge+1) {
	    SETERROR(cparams->utils,"server response incorrect");
	    return SASL_BADAUTH;
	}
	
	/* construct 8 octets
	 * 1-4 - original checksum
	 * 5 - bitmask of sec layer
	 * 6-8 max buffer size
	 */
	if (cparams->props.min_ssf > 
	    KRB_DES_SECURITY_BITS + cparams->external_ssf) {
	    SETERROR(cparams->utils,
		     "minimum ssf too strong for this mechanism");
	    return SASL_TOOWEAK;
	} else if (cparams->props.min_ssf > cparams->props.max_ssf) {
	    SETERROR(cparams->utils,
		     "minimum ssf larger than maximum ssf");
	    return SASL_BADPARAM;
	}
	
	/* create stuff to send to server */
	sout = (char *)
	    cparams->utils->malloc(9+(text->user ? strlen(text->user) : 0)+9);
	if (!sout) {
	    MEMERROR(cparams->utils);
	    return SASL_NOMEM;
	}
	
	nchal = htonl(text->challenge);
	memcpy(sout, &nchal, 4);
	
	/* need bits of layer */
	if(cparams->props.maxbufsize == 0) {
	    need = musthave = 0;
	} else {
	    need = cparams->props.max_ssf - cparams->external_ssf;
	    musthave = cparams->props.min_ssf - cparams->external_ssf;
	}
	
	oparams->decode = &kerberosv4_decode;
	oparams->encode = &kerberosv4_encode;
	
	if ((in[4] & KRB_SECFLAG_ENCRYPTION)
	    && (need>=56) && (musthave <= 56)) {
	    /* encryption */
	    text->sec_type = KRB_SEC_ENCRYPTION;
	    oparams->mech_ssf = 56;
	    sout[4] = KRB_SECFLAG_ENCRYPTION;
	    /* using encryption layer */
	} else if ((in[4] & KRB_SECFLAG_INTEGRITY)
		   && (need >= 1) && (musthave <= 1)) {
	    /* integrity */
	    text->sec_type = KRB_SEC_INTEGRITY;
	    oparams->mech_ssf=1;
	    sout[4] = KRB_SECFLAG_INTEGRITY;
	    /* using integrity layer */
	} else if ((in[4] & KRB_SECFLAG_NONE) && (musthave <= 0)) {
	    /* no layer */
	    text->sec_type = KRB_SEC_NONE;
	    oparams->encode=NULL;
	    oparams->decode=NULL;
	    oparams->mech_ssf=0;
	    sout[4] = KRB_SECFLAG_NONE;
	} else {
	    /* Mark that we tried */
	    oparams->mech_ssf=2;
	    SETERROR(cparams->utils,
		     "unable to agree on layers with server");
	    return SASL_BADPROT;
	}
	
	servermaxbuf = in[5]*256*256+in[6]*256+in[7];
	oparams->maxoutbuf = servermaxbuf;
	if (oparams->mech_ssf) {
	    /* FIXME: Likely to be too large */
	    oparams->maxoutbuf -= 50;
	}
	
	if(cparams->props.maxbufsize) {
	    int tmpmaxbuf = ( cparams->props.maxbufsize > 0xFFFFFF ) ? 0xFFFFFF : cparams->props.maxbufsize;

	    sout[5]=((tmpmaxbuf >> 16) & 0xFF);
	    sout[6]=((tmpmaxbuf >> 8) & 0xFF);
	    sout[7]=(tmpmaxbuf & 0xFF);
	} else {
            /* let's say we can support up to 64K */
	    /* no inherent inability with our layers to support more */

	    sout[5]=0x00;  /* max ciphertext buffer size */
	    sout[6]=0xFF;
	    sout[7]=0xFF;
	}
	
	sout[8] = 0x00; /* just to be safe */
	
	/* append userid */
	len = 9;			/* 8 + trailing NULL */
	if (text->user) {
	    strcpy((char *)sout + 8, text->user);
	    len += strlen(text->user);
	}
	
	/* append 0 based octets so is multiple of 8 */
	while(len % 8) {
	    sout[len]=0;
	    len++;
	}
	sout[len]=0;
	
	des_pcbc_encrypt((des_cblock *)sout,
			 (des_cblock *)sout,
			 len,
			 text->init_keysched,
			 (des_cblock *)text->session,
			 DES_ENCRYPT);
	
	result = _plug_buf_alloc(text->utils, &text->out_buf,
				 &text->out_buf_len, len);
	if (result != SASL_OK)  return result;
	
	memcpy(text->out_buf, sout, len);
	
	*clientout = text->out_buf;
	*clientoutlen = len;
	
	/* nothing more to do; should be authenticated */
	if(cparams->iplocalport) {   
	    result = ipv4_ipfromstring(cparams->utils,
				       cparams->iplocalport,
				       &(text->ip_local));
	    if (result != SASL_OK) {
		/* couldn't get local IP address */
		return result;
	    }
	}
	
	if (cparams->ipremoteport) {
	    result = ipv4_ipfromstring(cparams->utils,
				       cparams->ipremoteport,
				       &(text->ip_remote));
	    if (result != SASL_OK) {
		/* couldn't get local IP address */
		return result;
	    }
	}
	
	buf = cparams->utils->malloc(strlen(text->credentials.pname)
				     + strlen(text->credentials.pinst)
				     + 2);
	if (!buf) {
	    MEMERROR(cparams->utils);
	    return SASL_NOMEM;
	}
	strcpy(buf, text->credentials.pname);
	if (text->credentials.pinst[0]) {
	    strcat(buf, ".");
	    strcat(buf, text->credentials.pinst);
	}
	
	if (text->user && !text->user[0]) {
	    text->user = NULL;
	}
	
	ret = cparams->canon_user(cparams->utils->conn, buf, 0,
				  SASL_CU_AUTHID, oparams);
	if (ret != SASL_OK) {
	    cparams->utils->free(buf);
	    cparams->utils->free(sout);
	    return ret;
	}
	
	if (!text->user) {
	    /* 0 in length fields means use strlen() */
	    ret = cparams->canon_user(cparams->utils->conn, buf, 0,
				      SASL_CU_AUTHZID, oparams);
	} else {
	    ret = cparams->canon_user(cparams->utils->conn, text->user, 0,
				      SASL_CU_AUTHZID, oparams);
	}
	
	cparams->utils->free(buf);
	
	oparams->doneflag = 1;
	oparams->param_version = 0;
	
	/* used by layers */
	_plug_decode_init(&text->decode_context, text->utils,
			  (cparams->props.maxbufsize > 0xFFFFFF) ? 0xFFFFFF :
			  cparams->props.maxbufsize);
	
	if (sout) cparams->utils->free(sout);
	
	return SASL_OK;
    }
    
    default:
	cparams->utils->log(NULL, SASL_LOG_ERR,
			    "Invalid Kerberos client step %d\n", text->state);
	return SASL_FAIL;
    }

    return SASL_FAIL; /* should never get here */
}

static const long kerberosv4_required_prompts[] = {
    SASL_CB_LIST_END
};

static sasl_client_plug_t kerberosv4_client_plugins[] = 
{
    {
	"KERBEROS_V4",			/* mech_name */
	KRB_DES_SECURITY_BITS,		/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_NEEDSERVERFQDN
	| SASL_FEAT_SERVER_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	kerberosv4_required_prompts,	/* required_prompts */
	NULL,				/* glob_context */
	&kerberosv4_client_mech_new,	/* mech_new */
	&kerberosv4_client_mech_step,	/* mech_step */
	&kerberosv4_common_mech_dispose,/* mech_dispose */
	&kerberosv4_common_mech_free,	/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int kerberos4_client_plug_init(const sasl_utils_t *utils,
			       int maxversion,
			       int *out_version,
			       sasl_client_plug_t **pluglist,
			       int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR(utils, "Wrong KERBEROS_V4 version");
	return SASL_BADVERS;
    }
    
    if(!krb_mutex) {
	krb_mutex = utils->mutex_alloc();
	if(!krb_mutex) {
	    return SASL_FAIL;
	}
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = kerberosv4_client_plugins;
    *plugcount = 1;
    
    refcount++;
    
    return SASL_OK;
}
