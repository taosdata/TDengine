/* OTP SASL plugin
 * Ken Murchison
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
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#include <string.h> 
#include <ctype.h>
#include <assert.h>

#include <openssl/evp.h>
#include <openssl/md5.h> /* XXX hack for OpenBSD/OpenSSL cruftiness */

#include <sasl.h>
#define MD5_H  /* suppress internal MD5 */
#include <saslplug.h>

#include "plugin_common.h"

#ifdef macintosh 
#include <sasl_otp_plugin_decl.h> 
#endif 

/*****************************  Common Section  *****************************/

#define OTP_SEQUENCE_MAX	9999
#define OTP_SEQUENCE_DEFAULT	499
#define OTP_SEQUENCE_REINIT	490
#define OTP_SEED_MIN		1
#define OTP_SEED_MAX		16
#define OTP_HASH_SIZE		8		/* 64 bits */
#define OTP_CHALLENGE_MAX	100
#define OTP_RESPONSE_MAX	100
#define OTP_HEX_TYPE		"hex:"
#define OTP_WORD_TYPE		"word:"
#define OTP_INIT_HEX_TYPE	"init-hex:"
#define OTP_INIT_WORD_TYPE	"init-word:"

typedef struct algorithm_option_s {
    const char *name;		/* name used in challenge/response */
    int swab;			/* number of bytes to swab (0, 1, 2, 4, 8) */
    const char *evp_name;	/* name used for lookup in EVP table */
} algorithm_option_t;

static algorithm_option_t algorithm_options[] = {
    {"md4",	0,	"md4"},
    {"md5",	0,	"md5"},
    {"sha1",	4,	"sha1"},
    {NULL,	0,	NULL}
};

static EVP_MD_CTX *_plug_EVP_MD_CTX_new(const sasl_utils_t *utils)
{
    utils->log(NULL, SASL_LOG_DEBUG, "_plug_EVP_MD_CTX_new()");

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    return EVP_MD_CTX_new();
#else
    return utils->malloc(sizeof(EVP_MD_CTX));
#endif
}

static void _plug_EVP_MD_CTX_free(EVP_MD_CTX *ctx, const sasl_utils_t *utils)
{
    utils->log(NULL, SASL_LOG_DEBUG, "_plug_EVP_MD_CTX_free()");

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    EVP_MD_CTX_free(ctx);
#else
    utils->free(ctx);
#endif
}

/* Convert the binary data into ASCII hex */
void bin2hex(unsigned char *bin, int binlen, char *hex)
{
    int i;
    unsigned char c;
    
    for (i = 0; i < binlen; i++) {
	c = (bin[i] >> 4) & 0xf;
	hex[i*2] = (c > 9) ? ('a' + c - 10) : ('0' + c);
	c = bin[i] & 0xf;
	hex[i*2+1] = (c > 9) ? ('a' + c - 10) : ('0' + c);
    }
    hex[i*2] = '\0';
}

/*
 * Hash the data using the given algorithm and fold it into 64 bits,
 * swabbing bytes if necessary.
 */
static void otp_hash(const EVP_MD *md, char *in, size_t inlen,
		     unsigned char *out, int swab, EVP_MD_CTX *mdctx)
{
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int i;
    int j;
    unsigned hashlen;
    
    EVP_DigestInit(mdctx, md);
    EVP_DigestUpdate(mdctx, in, inlen);
    EVP_DigestFinal(mdctx, hash, &hashlen);
    
    /* Fold the result into 64 bits */
    for (i = OTP_HASH_SIZE; i < hashlen; i++) {
	hash[i % OTP_HASH_SIZE] ^= hash[i];
    }
    
    /* Swab bytes */
    if (swab) {
	for (i = 0; i < OTP_HASH_SIZE;) {
	    for (j = swab-1; j > -swab; i++, j-=2)
		out[i] = hash[i+j];
	}
    }
    else
	memcpy(out, hash, OTP_HASH_SIZE);
}

static int generate_otp(const sasl_utils_t *utils,
			algorithm_option_t *alg, unsigned seq, char *seed,
			unsigned char *secret, unsigned secret_len,
                        unsigned char *otp)
{
    const EVP_MD *md;
    EVP_MD_CTX *mdctx = NULL;
    char *key = NULL;
    int r = SASL_OK;
    
    if (!(md = EVP_get_digestbyname(alg->evp_name))) {
	utils->seterror(utils->conn, 0,
			"OTP algorithm %s is not available", alg->evp_name);
	return SASL_FAIL;
    }

    if ((mdctx = _plug_EVP_MD_CTX_new(utils)) == NULL) {
	SETERROR(utils, "cannot allocate MD CTX");
	r = SASL_NOMEM;
        goto done;
    }

    if ((key = utils->malloc(strlen(seed) + secret_len + 1)) == NULL) {
	SETERROR(utils, "cannot allocate OTP key");
	r = SASL_NOMEM;
        goto done;
    }
    
    /* initial step */
    sprintf(key, "%s%.*s", seed, secret_len, secret);
    otp_hash(md, key, strlen(key), otp, alg->swab, mdctx);
    
    /* computation step */
    while (seq-- > 0)
        otp_hash(md, (char *) otp, OTP_HASH_SIZE, otp, alg->swab, mdctx);

done:
    if (key) utils->free(key);
    if (mdctx) _plug_EVP_MD_CTX_free(mdctx, utils);

    return r;
}

static int parse_challenge(const sasl_utils_t *utils,
			   char *chal, algorithm_option_t **alg,
			   unsigned *seq, char *seed, int is_init)
{
    char *c;
    algorithm_option_t *opt;
    int n;
    
    c = chal;
    
    /* eat leading whitespace */
    while (*c && isspace((int) *c)) c++;
    
    if (!is_init) {
	/* check the prefix */
	if (!*c || strncmp(c, "otp-", 4)) {
	    SETERROR(utils, "not an OTP challenge");
	    return SASL_BADPROT;
	}
	
	/* skip the prefix */
	c += 4;
    }
    
    /* find the algorithm */
    opt = algorithm_options;
    while (opt->name) {
	if (!strncmp(c, opt->name, strlen(opt->name))) {
	    break;
	}
	opt++;
    }
    
    /* didn't find the algorithm in our list */
    if (!opt->name) {
	utils->seterror(utils->conn, 0, "OTP algorithm '%s' not supported", c);
	return SASL_BADPROT;
    }
    
    /* skip algorithm name */
    c += strlen(opt->name);
    *alg = opt;
    
    /* eat whitespace */
    if (!isspace((int) *c)) {
	SETERROR(utils, "no whitespace between OTP algorithm and sequence");
	return SASL_BADPROT;
    }
    while (*c && isspace((int) *c)) c++;
    
    /* grab the sequence */
    if ((*seq = strtoul(c, &c, 10)) > OTP_SEQUENCE_MAX) {
	utils->seterror(utils->conn, 0, "sequence > %u", OTP_SEQUENCE_MAX);
	return SASL_BADPROT;
    }
    
    /* eat whitespace */
    if (!isspace((int) *c)) {
	SETERROR(utils, "no whitespace between OTP sequence and seed");
	return SASL_BADPROT;
    }
    while (*c && isspace((int) *c)) c++;
    
    /* grab the seed, converting to lowercase as we go */
    n = 0;
    while (*c && isalnum((int) *c) && (n < OTP_SEED_MAX))
	seed[n++] = tolower((int) *c++);
    if (n > OTP_SEED_MAX) {
	utils->seterror(utils->conn, 0, "OTP seed length > %u", OTP_SEED_MAX);
	return SASL_BADPROT;
    }
    else if (n < OTP_SEED_MIN) {
	utils->seterror(utils->conn, 0, "OTP seed length < %u", OTP_SEED_MIN);
	return SASL_BADPROT;
    }
    seed[n] = '\0';
    
    if (!is_init) {
	/* eat whitespace */
	if (!isspace((int) *c)) {
	    SETERROR(utils, "no whitespace between OTP seed and extensions");
	    return SASL_BADPROT;
	}
	while (*c && isspace((int) *c)) c++;
	
	/* make sure this is an extended challenge */
	if (strncmp(c, "ext", 3) ||
	    (*(c+=3) &&
	     !(isspace((int) *c) || (*c == ',') ||
	       (*c == '\r') || (*c == '\n')))) {
	    SETERROR(utils, "not an OTP extended challenge");
	    return SASL_BADPROT;
	}
    }
    
    return SASL_OK;
}

static void
otp_common_mech_free(void *global_context __attribute__((unused)),
		     const sasl_utils_t *utils __attribute__((unused)))
{
    /* Don't call EVP_cleanup(); here, as this might confuse the calling
       application if it also uses OpenSSL */
}

/*****************************  Server Section  *****************************/

#ifdef  HAVE_OPIE
#include <opie.h>
#endif

typedef struct server_context {
    int state;

    char *authid;
    int locked;				/* is the user's secret locked? */
    algorithm_option_t *alg;
#ifdef HAVE_OPIE
    struct opie opie;
#else
    char *realm;
    unsigned seq;
    char seed[OTP_SEED_MAX+1];
    unsigned char otp[OTP_HASH_SIZE];
    time_t timestamp;			/* time we locked the secret */
#endif /* HAVE_OPIE */

    char *out_buf;
    unsigned out_buf_len;
} server_context_t;

static int otp_server_mech_new(void *glob_context __attribute__((unused)), 
			       sasl_server_params_t *sparams,
			       const char *challenge __attribute__((unused)),
			       unsigned challen __attribute__((unused)),
			       void **conn_context)
{
    server_context_t *text;
    
    /* holds state are in */
    text = sparams->utils->malloc(sizeof(server_context_t));
    if (text == NULL) {
	MEMERROR(sparams->utils);
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(server_context_t));
    
    text->state = 1;
    
    *conn_context = text;
    
    return SASL_OK;
}

#ifdef HAVE_OPIE

#ifndef OPIE_KEYFILE
#define OPIE_KEYFILE "/etc/opiekeys"
#endif

static int opie_server_mech_step(void *conn_context,
				 sasl_server_params_t *params,
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

    switch (text->state) {

    case 1: {
	const char *authzid;
	const char *authid;
	size_t authid_len;
	unsigned lup = 0;
	int result;
	
	/* should have received authzid NUL authid */
	
	/* get authzid */
	authzid = clientin;
	while ((lup < clientinlen) && (clientin[lup] != 0)) ++lup;
	
	if (lup >= clientinlen) {
	    SETERROR(params->utils, "Can only find OTP authzid (no authid)");
	    return SASL_BADPROT;
	}
	
	/* get authid */
	++lup;
	authid = clientin + lup;
	while ((lup < clientinlen) && (clientin[lup] != 0)) ++lup;
	
	authid_len = clientin + lup - authid;
	
	if (lup != clientinlen) {
	    SETERROR(params->utils,
		     "Got more data than we were expecting in the OTP plugin\n");
	    return SASL_BADPROT;
	}
	
	text->authid = params->utils->malloc(authid_len + 1);    
	if (text->authid == NULL) {
	    MEMERROR(params->utils);
	    return SASL_NOMEM;
	}
	
	/* we can't assume that authen is null-terminated */
	strncpy(text->authid, authid, authid_len);
	text->authid[authid_len] = '\0';
	
	result = params->canon_user(params->utils->conn, text->authid, 0,
				    SASL_CU_AUTHID, oparams);
	if (result != SASL_OK) return result;
	
	result = params->canon_user(params->utils->conn,
				    strlen(authzid) ? authzid : text->authid,
				    0, SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) return result;
	
	result = _plug_buf_alloc(params->utils, &(text->out_buf),
				 &(text->out_buf_len), OTP_CHALLENGE_MAX+1);
	if (result != SASL_OK) return result;

	/* create challenge - return sasl_continue on success */
	result = opiechallenge(&text->opie, text->authid, text->out_buf);
	
	switch (result) {
	case 0:
	    text->locked = 1;

	    *serverout = text->out_buf;
	    *serveroutlen = strlen(text->out_buf);

	    text->state = 2;
	    return SASL_CONTINUE;
	    
	case 1:
	    SETERROR(params->utils, "opiechallenge: user not found or locked");
	    return SASL_NOUSER;
	    
	default:
	    SETERROR(params->utils,
		     "opiechallenge: system error (file, memory, I/O)");
	    return SASL_FAIL;
	}
    }
    
    case 2: {
	char response[OPIE_RESPONSE_MAX+1];
	int result;
	
	/* should have received extended response,
	   but we'll take anything that we can verify */
	
	if (clientinlen > OPIE_RESPONSE_MAX) {
	    SETERROR(params->utils, "response too long");
	    return SASL_BADPROT;
	}
	
	/* we can't assume that the response is null-terminated */
	strncpy(response, clientin, clientinlen);
	response[clientinlen] = '\0';
	
	/* verify response */
	result = opieverify(&text->opie, response);
	text->locked = 0;
	
	switch (result) {
	case 0:
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
	    
	case 1:
	    SETERROR(params->utils, "opieverify: invalid/incorrect response");
	    return SASL_BADAUTH;
	    
	default:
	    SETERROR(params->utils,
		     "opieverify: system error (file, memory, I/O)");
	    return SASL_FAIL;
	}
    }
    
    default:
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid OTP server step %d\n", text->state);
	return SASL_FAIL;
    }

    return SASL_FAIL; /* should never get here */
}

static void opie_server_mech_dispose(void *conn_context,
				     const sasl_utils_t *utils)
{
    server_context_t *text = (server_context_t *) conn_context;
    
    if (!text) return;
    
    /* if we created a challenge, but bailed before the verification of the
       response, do a verify here to release the lock on the user key */
    if (text->locked) opieverify(&text->opie, "");
    
    if (text->authid) _plug_free_string(utils, &(text->authid));

    if (text->out_buf) utils->free(text->out_buf);
    
    utils->free(text);
}

static int opie_mech_avail(void *glob_context __attribute__((unused)),
			   sasl_server_params_t *sparams,
			   void **conn_context __attribute__((unused))) 
{
    const char *fname;
    unsigned int len;
    
    sparams->utils->getopt(sparams->utils->getopt_context,
			   "OTP", "opiekeys", &fname, &len);
    
    if (!fname) fname = OPIE_KEYFILE;
    
    if (access(fname, R_OK|W_OK) != 0) {
	sparams->utils->log(NULL, SASL_LOG_ERR,
			    "OTP unavailable because "
			    "can't read/write key database %s: %m",
			    fname, errno);
	return SASL_NOMECH;
    }
    
    return SASL_OK;
}

static sasl_server_plug_t otp_server_plugins[] = 
{
    {
	"OTP",
	0,
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_FORWARD_SECRECY,
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_DONTUSE_USERPASSWD
	| SASL_FEAT_ALLOWS_PROXY,
	NULL,
	&otp_server_mech_new,
	&opie_server_mech_step,
	&opie_server_mech_dispose,
	&otp_common_mech_free,
	NULL,
	NULL,
	NULL,
	&opie_mech_avail,
	NULL
    }
};
#else /* HAVE_OPIE */

#include "otp.h"

#define OTP_MDA_DEFAULT		"md5"
#define OTP_LOCK_TIMEOUT	5 * 60		/* 5 minutes */

/* Convert the ASCII hex into binary data */
int hex2bin(char *hex, unsigned char *bin, int binlen)
{
    int i;
    char *c;
    unsigned char msn, lsn;
    
    memset(bin, 0, binlen);
    
    for (c = hex, i = 0; i < binlen; c++) {
	/* whitespace */
	if (isspace((int) *c))
	    continue;
	/* end of string, or non-hex char */
	if (!*c || !*(c+1) || !isxdigit((int) *c))
	    break;
	
	msn = (*c > '9') ? tolower((int) *c) - 'a' + 10 : *c - '0';
	c++;
	lsn = (*c > '9') ? tolower((int) *c) - 'a' + 10 : *c - '0';
	
	bin[i++] = (unsigned char) (msn << 4) | lsn;
    }
    
    return (i < binlen) ? SASL_BADAUTH : SASL_OK;
}

static int make_secret(const sasl_utils_t *utils, const char *alg,
                       unsigned seq, char *seed, unsigned char *otp,
		       time_t timeout, sasl_secret_t **secret)
{
    size_t sec_len;
    char *data;
    char buf[2*OTP_HASH_SIZE+1];
    
    /*
     * secret is stored as:
     *
     * <alg> \t <seq> \t <seed> \t <otp> \t <timeout> \0
     *
     * <timeout> is used as a "lock" when an auth is in progress
     * we just set it to zero here (no lock)
     */
    sec_len = strlen(alg)+1+4+1+strlen(seed)+1+2*OTP_HASH_SIZE+1+20+1;
    *secret = utils->malloc(sizeof(sasl_secret_t)+sec_len);
    if (!*secret) {
	return SASL_NOMEM;
    }
    
    (*secret)->len = (unsigned) sec_len;
    data = (char *) (*secret)->data;

    bin2hex(otp, OTP_HASH_SIZE, buf);
    buf[2*OTP_HASH_SIZE] = '\0';
    
    sprintf(data, "%s\t%04d\t%s\t%s\t%020ld",
	    alg, seq, seed, buf, timeout);
    
    return SASL_OK;
}

static int parse_secret(const sasl_utils_t *utils,
			char *secret, size_t seclen,
			char *alg, unsigned *seq, char *seed,
			unsigned char *otp,
			time_t *timeout)
{
    if (strlen(secret) < seclen) {
        char *c;
	
	/*
	 * old-style (binary) secret is stored as:
	 *
	 * <alg> \0 <seq> \0 <seed> \0 <otp> <timeout>
	 *
	 */
	
	if (seclen < (3+1+1+1+OTP_SEED_MIN+1+OTP_HASH_SIZE+sizeof(time_t))) {
	    SETERROR(utils, "OTP secret too short");
	    return SASL_FAIL;
	}
	
	c = secret;
	
	strcpy(alg, (char*) c);
	c += strlen(alg)+1;
	
	*seq = strtoul(c, NULL, 10);
	c += 5;
	
	strcpy(seed, (char*) c);
	c += strlen(seed)+1;
	
	memcpy(otp, c, OTP_HASH_SIZE);
	c += OTP_HASH_SIZE;
	
	memcpy(timeout, c, sizeof(time_t));
	
	return SASL_OK;
    }

    else {
	char buf[2*OTP_HASH_SIZE+1];
	
	/*
	 * new-style (ASCII) secret is stored as:
	 *
	 * <alg> \t <seq> \t <seed> \t <otp> \t <timeout> \0
	 *
	 */
	
	if (seclen < (3+1+1+1+OTP_SEED_MIN+1+2*OTP_HASH_SIZE+1+20)) {
	    SETERROR(utils, "OTP secret too short");
	    return SASL_FAIL;
	}
	
	sscanf(secret, "%s\t%04d\t%s\t%s\t%020ld",
	       alg, seq, seed, buf, timeout);
	
	hex2bin(buf, otp, OTP_HASH_SIZE);
	
	return SASL_OK;
    }
}

/* Compare two string pointers */
static int strptrcasecmp(const void *arg1, const void *arg2)
{
    return (strcasecmp(*((char**) arg1), *((char**) arg2)));
}

/* Convert the 6 words into binary data */
static int word2bin(const sasl_utils_t *utils,
		    char *words, unsigned char *bin, const EVP_MD *md,
                    EVP_MD_CTX *mdctx)
{
    int i, j;
    char *c, *word, buf[OTP_RESPONSE_MAX+1];
    void *base;
    int nmemb;
    unsigned long x = 0;
    unsigned char bits[OTP_HASH_SIZE+1]; /* 1 for checksum */
    unsigned char chksum;
    int bit, fbyte, lbyte;
    const char **str_ptr;
    int alt_dict = 0;
    
    /* this is a destructive operation, so make a work copy */
    strcpy(buf, words);
    memset(bits, 0, 9);
    
    for (c = buf, bit = 0, i = 0; i < 6; i++, c++, bit+=11) {
	while (*c && isspace((int) *c)) c++;
	word = c;
	while (*c && isalpha((int) *c)) c++;
	if (!*c && i < 5) break;
	*c = '\0';
	if (strlen(word) < 1 || strlen(word) > 4) {
	    utils->log(NULL, SASL_LOG_DEBUG,
		       "incorrect word length '%s'", word);
	    return SASL_BADAUTH;
	}
	
	/* standard dictionary */
	if (!alt_dict) {
	    if (strlen(word) < 4) {
		base = otp_std_dict;
		nmemb = OTP_4LETTER_OFFSET;
	    }
	    else {
		base = otp_std_dict + OTP_4LETTER_OFFSET;
		nmemb = OTP_STD_DICT_SIZE - OTP_4LETTER_OFFSET;
	    }
	    
	    str_ptr = (const char**) bsearch((void*) &word, base, nmemb,
					     sizeof(const char*),
					     strptrcasecmp);
	    if (str_ptr) {
		x = (unsigned long) (str_ptr - otp_std_dict);
	    }
	    else if (i == 0) {
		/* couldn't find first word, try alternate dictionary */
		alt_dict = 1;
	    }
	    else {
		utils->log(NULL, SASL_LOG_DEBUG,
			   "word '%s' not found in dictionary", word);
		return SASL_BADAUTH;
	    }
	}
	
	/* alternate dictionary */
	if (alt_dict) {
	    unsigned char hash[EVP_MAX_MD_SIZE];
	    unsigned hashlen;
	    
	    EVP_DigestInit(mdctx, md);
	    EVP_DigestUpdate(mdctx, word, strlen(word));
	    EVP_DigestFinal(mdctx, hash, &hashlen);
	    
	    /* use lowest 11 bits */
	    x = ((hash[hashlen-2] & 0x7) << 8) | hash[hashlen-1];
	}
	
	/* left align 11 bits on byte boundary */
	x <<= (8 - ((bit+11) % 8));
	/* first output byte containing some of our 11 bits */
	fbyte = bit / 8;
	/* last output byte containing some of our 11 bits */
	lbyte = (bit+11) / 8;
	/* populate the output bytes with the 11 bits */
	for (j = lbyte; j >= fbyte; j--, x >>= 8)
	    bits[j] |= (unsigned char) (x & 0xff);
    }
    
    if (i < 6) {
	utils->log(NULL, SASL_LOG_DEBUG, "not enough words (%d)", i);
	return SASL_BADAUTH;
    }
    
    /* see if the 2-bit checksum is correct */
    for (chksum = 0, i = 0; i < 8; i++) {
	for (j = 0; j < 4; j++) {
	    chksum += ((bits[i] >> (2 * j)) & 0x3);
	}
    }
    chksum <<= 6;
    
    if (chksum != bits[8]) {
	utils->log(NULL, SASL_LOG_DEBUG, "incorrect parity");
	return SASL_BADAUTH;
    }
    
    memcpy(bin, bits, OTP_HASH_SIZE);
    
    return SASL_OK;
}

static int verify_response(server_context_t *text, const sasl_utils_t *utils,
			   char *response)
{
    const EVP_MD *md;
    EVP_MD_CTX *mdctx = NULL;
    char *c;
    int do_init = 0;
    unsigned char cur_otp[OTP_HASH_SIZE], prev_otp[OTP_HASH_SIZE];
    int r;
    
    /* find the MDA */
    if (!(md = EVP_get_digestbyname(text->alg->evp_name))) {
	utils->seterror(utils->conn, 0,
			"OTP algorithm %s is not available",
			text->alg->evp_name);
	return SASL_FAIL;
    }
    
    if ((mdctx = _plug_EVP_MD_CTX_new(utils)) == NULL) {
	SETERROR(utils, "cannot allocate MD CTX");
	return SASL_NOMEM;
    }
    
    /* eat leading whitespace */
    c = response;
    while (isspace((int) *c)) c++;
    
    if (strchr(c, ':')) {
	if (!strncasecmp(c, OTP_HEX_TYPE, strlen(OTP_HEX_TYPE))) {
	    r = hex2bin(c+strlen(OTP_HEX_TYPE), cur_otp, OTP_HASH_SIZE);
	}
	else if (!strncasecmp(c, OTP_WORD_TYPE, strlen(OTP_WORD_TYPE))) {
	    r = word2bin(utils, c+strlen(OTP_WORD_TYPE), cur_otp, md, mdctx);
	}
	else if (!strncasecmp(c, OTP_INIT_HEX_TYPE,
			      strlen(OTP_INIT_HEX_TYPE))) {
	    do_init = 1;
	    r = hex2bin(c+strlen(OTP_INIT_HEX_TYPE), cur_otp, OTP_HASH_SIZE);
	}
	else if (!strncasecmp(c, OTP_INIT_WORD_TYPE,
			      strlen(OTP_INIT_WORD_TYPE))) {
	    do_init = 1;
	    r = word2bin(utils, c+strlen(OTP_INIT_WORD_TYPE), cur_otp, md, mdctx);
	}
	else {
	    SETERROR(utils, "unknown OTP extended response type");
	    r = SASL_BADAUTH;
	}
    }
    else {
	/* standard response, try word first, and then hex */
	r = word2bin(utils, c, cur_otp, md, mdctx);
	if (r != SASL_OK)
	    r = hex2bin(c, cur_otp, OTP_HASH_SIZE);
    }
    
    if (r == SASL_OK) {
	/* do one more hash (previous otp) and compare to stored otp */
	otp_hash(md, (char *) cur_otp, OTP_HASH_SIZE,
                 prev_otp, text->alg->swab, mdctx);
	
	if (!memcmp(prev_otp, text->otp, OTP_HASH_SIZE)) {
	    /* update the secret with this seq/otp */
	    memcpy(text->otp, cur_otp, OTP_HASH_SIZE);
	    text->seq--;
	    r = SASL_OK;
	}
	else
	    r = SASL_BADAUTH;
    }
    
    /* if this is an init- attempt, let's check it out */
    if (r == SASL_OK && do_init) {
	char *new_chal = NULL, *new_resp = NULL;
	algorithm_option_t *alg;
	unsigned seq;
	char seed[OTP_SEED_MAX+1];
	unsigned char new_otp[OTP_HASH_SIZE];
	
	/* find the challenge and response fields */
	new_chal = strchr(c+strlen(OTP_INIT_WORD_TYPE), ':');
	if (new_chal) {
	    *new_chal++ = '\0';
	    new_resp = strchr(new_chal, ':');
	    if (new_resp)
		*new_resp++ = '\0';
	}
	
	if (!(new_chal && new_resp)) {
	    r = SASL_BADAUTH;
            goto done;
        }
	
	if ((r = parse_challenge(utils, new_chal, &alg, &seq, seed, 1))
	    != SASL_OK) {
            goto done;
	}
	
	if (seq < 1 || !strcasecmp(seed, text->seed)) {
	    r = SASL_BADAUTH;
            goto done;
        }
	
	/* find the MDA */
	if (!(md = EVP_get_digestbyname(alg->evp_name))) {
	    utils->seterror(utils->conn, 0,
			    "OTP algorithm %s is not available",
			    alg->evp_name);
	    r = SASL_BADAUTH;
            goto done;
	}
	
	if (!strncasecmp(c, OTP_INIT_HEX_TYPE, strlen(OTP_INIT_HEX_TYPE))) {
	    r = hex2bin(new_resp, new_otp, OTP_HASH_SIZE);
	}
	else if (!strncasecmp(c, OTP_INIT_WORD_TYPE,
			      strlen(OTP_INIT_WORD_TYPE))) {
	    r = word2bin(utils, new_resp, new_otp, md, mdctx);
	}
	
	if (r == SASL_OK) {
	    /* setup for new secret */
	    text->alg = alg;
	    text->seq = seq;
	    strcpy(text->seed, seed);
	    memcpy(text->otp, new_otp, OTP_HASH_SIZE);
	}
    }

  done:
    if (mdctx) _plug_EVP_MD_CTX_free(mdctx, utils);

    return r;
}

static int otp_server_mech_step1(server_context_t *text,
				 sasl_server_params_t *params,
				 const char *clientin,
				 unsigned clientinlen,
				 const char **serverout,
				 unsigned *serveroutlen,
				 sasl_out_params_t *oparams)
{
    const char *authzid;
    const char *authidp;
    size_t authid_len;
    unsigned lup = 0;
    int result, n;
    const char *lookup_request[] = { "*cmusaslsecretOTP",
				     NULL };
    const char *store_request[] = { "cmusaslsecretOTP",
				    NULL };
    struct propval auxprop_values[2];
    char mda[10];
    time_t timeout;
    sasl_secret_t *sec = NULL;
    struct propctx *propctx = NULL;
    
    /* should have received authzid NUL authid */
    
    /* get authzid */
    authzid = clientin;
    while ((lup < clientinlen) && (clientin[lup] != 0)) ++lup;
    
    if (lup >= clientinlen) {
	SETERROR(params->utils, "Can only find OTP authzid (no authid)");
	return SASL_BADPROT;
    }
    
    /* get authid */
    ++lup;
    authidp = clientin + lup;
    while ((lup < clientinlen) && (clientin[lup] != 0)) ++lup;
    
    authid_len = clientin + lup - authidp;
    
    if (lup != clientinlen) {
	SETERROR(params->utils,
		 "Got more data than we were expecting in the OTP plugin\n");
	return SASL_BADPROT;
    }
    
    text->authid = params->utils->malloc(authid_len + 1);    
    if (text->authid == NULL) {
	MEMERROR(params->utils);
	return SASL_NOMEM;
    }
    
    /* we can't assume that authid is null-terminated */
    strncpy(text->authid, authidp, authid_len);
    text->authid[authid_len] = '\0';

    n = 0;
    do {
	/* Get user secret */
	result = params->utils->prop_request(params->propctx,
					     lookup_request);
	if (result != SASL_OK) return result;

	/* this will trigger the getting of the aux properties.
	   Must use the fully qualified authid here */
	result = params->canon_user(params->utils->conn, text->authid, 0,
				    SASL_CU_AUTHID, oparams);
	if (result != SASL_OK) return result;
	
	result = params->canon_user(params->utils->conn,
				    strlen(authzid) ? authzid : text->authid,
				    0, SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) return result;
	
	result = params->utils->prop_getnames(params->propctx,
					      lookup_request,
					      auxprop_values);
	if (result < 0 ||
	    (!auxprop_values[0].name || !auxprop_values[0].values)) {
	    /* We didn't find this username */
	    SETERROR(params->utils, "no OTP secret in database");
	    result = params->transition ? SASL_TRANS : SASL_NOUSER;
	    return (result);
	}
	
	if (auxprop_values[0].name && auxprop_values[0].values) {
	    result = parse_secret(params->utils,
				  (char*) auxprop_values[0].values[0],
				  auxprop_values[0].valsize,
				  mda, &text->seq, text->seed, text->otp,
				  &timeout);
	    
	    if (result != SASL_OK) return result;
	} else {
	    SETERROR(params->utils, "don't have an OTP secret");
	    return SASL_FAIL;
	}
	
	text->timestamp = time(0);
    }
    /*
     * check lock timeout
     *
     * we try 10 times in 1 second intervals in order to give the other
     * auth attempt time to finish
     */
    while ((text->timestamp < timeout) && (n++ < 10) && !sleep(1));
    
    if (text->timestamp < timeout) {
	SETERROR(params->utils,
		 "simultaneous OTP authentications not permitted");
	return SASL_TRYAGAIN;
    }
    
    /* check sequence number */
    if (text->seq <= 1) {
	SETERROR(params->utils, "OTP has expired (sequence <= 1)");
	return SASL_EXPIRED;
    }
    
    /* find algorithm */
    text->alg = algorithm_options;
    while (text->alg->name) {
	if (!strcasecmp(text->alg->name, mda))
	    break;
	
	text->alg++;
    }
    
    if (!text->alg->name) {
	params->utils->seterror(params->utils->conn, 0,
				"unknown OTP algorithm '%s'", mda);
	return SASL_FAIL;
    }
    
    /* remake the secret with a timeout */
    result = make_secret(params->utils, text->alg->name, text->seq,
			 text->seed, text->otp,
			 text->timestamp + OTP_LOCK_TIMEOUT, &sec);
    if (result != SASL_OK) {
	SETERROR(params->utils, "error making OTP secret");
	return result;
    }
    
    /* do the store */
    propctx = params->utils->prop_new(0);
    if (!propctx)
	result = SASL_FAIL;
    if (result == SASL_OK)
	result = params->utils->prop_request(propctx, store_request);
    if (result == SASL_OK)
	result = params->utils->prop_set(propctx, "cmusaslsecretOTP",
					 (char *) sec->data, sec->len);
    if (result == SASL_OK)
	result = params->utils->auxprop_store(params->utils->conn,
					      propctx, text->authid);
    if (propctx)
	params->utils->prop_dispose(&propctx);

    if (sec) params->utils->free(sec);
    
    if (result != SASL_OK) {
	SETERROR(params->utils, "Error putting OTP secret");
	return result;
    }
    
    text->locked = 1;
    
    result = _plug_buf_alloc(params->utils, &(text->out_buf),
			     &(text->out_buf_len), OTP_CHALLENGE_MAX+1);
    if (result != SASL_OK) return result;
    
    /* create challenge */
    sprintf(text->out_buf, "otp-%s %u %s ext",
	    text->alg->name, text->seq-1, text->seed);
    
    *serverout = text->out_buf;
    *serveroutlen = (unsigned) strlen(text->out_buf);
    
    text->state = 2;
    
    return SASL_CONTINUE;
}

static int
otp_server_mech_step2(server_context_t *text,
		      sasl_server_params_t *params,
		      const char *clientin,
		      unsigned clientinlen,
		      const char **serverout __attribute__((unused)),
		      unsigned *serveroutlen __attribute__((unused)),
		      sasl_out_params_t *oparams)
{
    char response[OTP_RESPONSE_MAX+1];
    int result;
    sasl_secret_t *sec = NULL;
    struct propctx *propctx = NULL;
    const char *store_request[] = { "cmusaslsecretOTP",
				     NULL };
    
    if (clientinlen > OTP_RESPONSE_MAX) {
	SETERROR(params->utils, "OTP response too long");
	return SASL_BADPROT;
    }
    
    /* we can't assume that the response is null-terminated */
    strncpy(response, clientin, clientinlen);
    response[clientinlen] = '\0';
    
    /* check timeout */
    if (time(0) > text->timestamp + OTP_LOCK_TIMEOUT) {
	SETERROR(params->utils, "OTP: server timed out");
	return SASL_UNAVAIL;
    }
    
    /* verify response */
    result = verify_response(text, params->utils, response);
    if (result != SASL_OK) return result;
    
    /* make the new secret */
    result = make_secret(params->utils, text->alg->name, text->seq,
			 text->seed, text->otp, 0, &sec);
    if (result != SASL_OK) {
	SETERROR(params->utils, "error making OTP secret");
    }
    
    /* do the store */
    propctx = params->utils->prop_new(0);
    if (!propctx)
	result = SASL_FAIL;
    if (result == SASL_OK)
	result = params->utils->prop_request(propctx, store_request);
    if (result == SASL_OK)
	result = params->utils->prop_set(propctx, "cmusaslsecretOTP",
					 (char *) sec->data, sec->len);
    if (result == SASL_OK)
	result = params->utils->auxprop_store(params->utils->conn,
					      propctx, text->authid);
    if (propctx)
	params->utils->prop_dispose(&propctx);

    if (result) {
	SETERROR(params->utils, "Error putting OTP secret");
    }
    
    text->locked = 0;
    
    if (sec) _plug_free_secret(params->utils, &sec);
    
    /* set oparams */
    oparams->doneflag = 1;
    oparams->mech_ssf = 0;
    oparams->maxoutbuf = 0;
    oparams->encode_context = NULL;
    oparams->encode = NULL;
    oparams->decode_context = NULL;
    oparams->decode = NULL;
    oparams->param_version = 0;
    
    return result;
}

static int otp_server_mech_step(void *conn_context,
				sasl_server_params_t *params,
				const char *clientin,
				unsigned clientinlen,
				const char **serverout,
				unsigned *serveroutlen,
				sasl_out_params_t *oparams)
{
    server_context_t *text = (server_context_t *) conn_context;
    
    *serverout = NULL;
    *serveroutlen = 0;
    
    switch (text->state) {
	
    case 1:
	return otp_server_mech_step1(text, params, clientin, clientinlen,
				     serverout, serveroutlen, oparams);
	
    case 2:
	return otp_server_mech_step2(text, params, clientin, clientinlen,
				     serverout, serveroutlen, oparams);
	
    default:
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid OTP server step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static void otp_server_mech_dispose(void *conn_context,
				    const sasl_utils_t *utils)
{
    server_context_t *text = (server_context_t *) conn_context;
    sasl_secret_t *sec;
    struct propctx *propctx = NULL;
    const char *store_request[] = { "cmusaslsecretOTP",
				     NULL };
    int r;
    
    if (!text) return;
    
    /* if we created a challenge, but bailed before the verification of the
       response, release the lock on the user key */
    if (text->locked && (time(0) < text->timestamp + OTP_LOCK_TIMEOUT)) {
	r = make_secret(utils, text->alg->name, text->seq,
			text->seed, text->otp, 0, &sec);
	if (r != SASL_OK) {
	    SETERROR(utils, "error making OTP secret");
	    if (sec) utils->free(sec);
	    sec = NULL;
	}
	
	/* do the store */
	propctx = utils->prop_new(0);
	if (!propctx)
	    r = SASL_FAIL;
	if (!r)
	    r = utils->prop_request(propctx, store_request);
	if (!r)
	    r = utils->prop_set(propctx, "cmusaslsecretOTP",
				(sec ? (char *) sec->data : NULL),
				(sec ? sec->len : 0));
	if (!r)
	    r = utils->auxprop_store(utils->conn, propctx, text->authid);
	if (propctx)
	    utils->prop_dispose(&propctx);

	if (r) {
	    SETERROR(utils, "Error putting OTP secret");
	}
	
	if (sec) _plug_free_secret(utils, &sec);
    }
    
    if (text->authid) _plug_free_string(utils, &(text->authid));
    if (text->realm) _plug_free_string(utils, &(text->realm));
    
    if (text->out_buf) utils->free(text->out_buf);
    
    utils->free(text);
}

static int otp_setpass(void *glob_context __attribute__((unused)),
		       sasl_server_params_t *sparams,
		       const char *userstr,
		       const char *pass, unsigned passlen,
		       const char *oldpass __attribute__((unused)),
		       unsigned oldpasslen __attribute__((unused)),
		       unsigned flags)
{
    int r;
    char *user = NULL;
    char *user_only = NULL;
    char *realm = NULL;
    sasl_secret_t *sec;
    struct propctx *propctx = NULL;
    const char *store_request[] = { "cmusaslsecretOTP",
				     NULL };
    
    /* Do we have a backend that can store properties? */
    if (!sparams->utils->auxprop_store ||
	sparams->utils->auxprop_store(NULL, NULL, NULL) != SASL_OK) {
	SETERROR(sparams->utils, "OTP: auxprop backend can't store properties");
	return SASL_NOMECH;
    }
    
    r = _plug_parseuser(sparams->utils,
			&user_only,
			&realm,
			sparams->user_realm,
			sparams->serverFQDN,
			userstr);
    if (r) {
	SETERROR(sparams->utils, "OTP: Error parsing user");
	return r;
    }

    r = _plug_make_fulluser(sparams->utils, &user, user_only, realm);
    if (r) {
       goto cleanup;
    }

    if ((flags & SASL_SET_DISABLE) || pass == NULL) {
	sec = NULL;
    } else {
	algorithm_option_t *algs;
	const char *mda;
	unsigned int len;
	unsigned short randnum;
	char seed[OTP_SEED_MAX+1];
	unsigned char otp[OTP_HASH_SIZE];
	
	sparams->utils->getopt(sparams->utils->getopt_context,
			       "OTP", "otp_mda", &mda, &len);
	if (!mda) mda = OTP_MDA_DEFAULT;
	
	algs = algorithm_options;
	while (algs->name) {
	    if (!strcasecmp(algs->name, mda) ||
		!strcasecmp(algs->evp_name, mda))
		break;
	    
	    algs++;
	}
	
	if (!algs->name) {
	    sparams->utils->seterror(sparams->utils->conn, 0,
				     "unknown OTP algorithm '%s'", mda);
	    r = SASL_FAIL;
	    goto cleanup;
	}
	
	sparams->utils->rand(sparams->utils->rpool,
			     (char*) &randnum, sizeof(randnum));
	sprintf(seed, "%.2s%04u", sparams->serverFQDN, (randnum % 9999) + 1);
	
	r = generate_otp(sparams->utils, algs, OTP_SEQUENCE_DEFAULT,
			 seed, (unsigned char *) pass, passlen, otp);
	if (r != SASL_OK) {
	    /* generate_otp() takes care of error message */
	    goto cleanup;
	}
	
	r = make_secret(sparams->utils, algs->name, OTP_SEQUENCE_DEFAULT,
			seed, otp, 0, &sec);
	if (r != SASL_OK) {
	    SETERROR(sparams->utils, "error making OTP secret");
	    goto cleanup;
	}
    }
    
    /* do the store */
    propctx = sparams->utils->prop_new(0);
    if (!propctx)
	r = SASL_FAIL;
    if (!r)
	r = sparams->utils->prop_request(propctx, store_request);
    if (!r)
	r = sparams->utils->prop_set(propctx, "cmusaslsecretOTP",
				     (sec ? (char *) sec->data : NULL),
				     (sec ? sec->len : 0));
    if (!r)
	r = sparams->utils->auxprop_store(sparams->utils->conn, propctx, user);
    if (propctx)
	sparams->utils->prop_dispose(&propctx);
    
    if (r) {
	SETERROR(sparams->utils, "Error putting OTP secret");
	goto cleanup;
    }
    
    sparams->utils->log(NULL, SASL_LOG_DEBUG, "Setpass for OTP successful\n");
    
  cleanup:
    
    if (user) 	_plug_free_string(sparams->utils, &user);
    if (user_only)     _plug_free_string(sparams->utils, &user_only);
    if (realm) 	_plug_free_string(sparams->utils, &realm);
    if (sec)    _plug_free_secret(sparams->utils, &sec);
    
    return r;
}

static int otp_mech_avail(void *glob_context __attribute__((unused)),
	  	          sasl_server_params_t *sparams,
		          void **conn_context __attribute__((unused))) 
{
    /* Do we have a backend that can store properties? */
    if (!sparams->utils->auxprop_store ||
	sparams->utils->auxprop_store(NULL, NULL, NULL) != SASL_OK) {
	sparams->utils->log(NULL,
			    SASL_LOG_DEBUG,
			    "OTP: auxprop backend can't store properties");
	return SASL_NOMECH;
    }
    
    return SASL_OK;
}

static sasl_server_plug_t otp_server_plugins[] = 
{
    {
	"OTP",				/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_FORWARD_SECRECY,	/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	NULL,				/* glob_context */
	&otp_server_mech_new,		/* mech_new */
	&otp_server_mech_step,		/* mech_step */
	&otp_server_mech_dispose,	/* mech_dispose */
	&otp_common_mech_free,		/* mech_free */
	&otp_setpass,			/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	&otp_mech_avail,		/* mech avail */
	NULL				/* spare */
    }
};
#endif /* HAVE_OPIE */

int otp_server_plug_init(const sasl_utils_t *utils,
			 int maxversion,
			 int *out_version,
			 sasl_server_plug_t **pluglist,
			 int *plugcount)
{
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	SETERROR(utils, "OTP version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = otp_server_plugins;
    *plugcount = 1;  
    
    /* Add all digests */
    OpenSSL_add_all_digests();
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

typedef struct client_context {
    int state;

    sasl_secret_t *password;
    unsigned int free_password; /* set if we need to free password */

    const char *otpassword;

    char *out_buf;
    unsigned out_buf_len;

    char challenge[OTP_CHALLENGE_MAX+1];
} client_context_t;

static int otp_client_mech_new(void *glob_context __attribute__((unused)),
			       sasl_client_params_t *params,
			       void **conn_context)
{
    client_context_t *text;
    
    /* holds state are in */
    text = params->utils->malloc(sizeof(client_context_t));
    if (text == NULL) {
	MEMERROR( params->utils );
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(client_context_t));
    
    text->state = 1;
    
    *conn_context = text;
    
    return SASL_OK;
}

static int otp_client_mech_step1(client_context_t *text,
				 sasl_client_params_t *params,
				 const char *serverin __attribute__((unused)),
				 unsigned serverinlen __attribute__((unused)),
				 sasl_interact_t **prompt_need,
				 const char **clientout,
				 unsigned *clientoutlen,
				 sasl_out_params_t *oparams)
{
    const char *user = NULL, *authid = NULL;
    int user_result = SASL_OK;
    int auth_result = SASL_OK;
    int pass_result = SASL_OK;
    sasl_chalprompt_t *echo_cb;
    void *echo_context;
    int result;
    
    /* check if sec layer strong enough */
    if (params->props.min_ssf > params->external_ssf) {
	SETERROR( params->utils, "SSF requested of OTP plugin");
	return SASL_TOOWEAK;
    }
    
    /* try to get the authid */    
    if (oparams->authid == NULL) {
	auth_result = _plug_get_authid(params->utils, &authid, prompt_need);
	
	if ((auth_result != SASL_OK) && (auth_result != SASL_INTERACT))
	    return auth_result;
    }
    
    /* try to get the userid */
    if (oparams->user == NULL) {
	user_result = _plug_get_userid(params->utils, &user, prompt_need);
	
	if ((user_result != SASL_OK) && (user_result != SASL_INTERACT))
	    return user_result;
    }
    
    /* try to get the secret pass-phrase if we don't have a chalprompt */
    if ((params->utils->getcallback(params->utils->conn, SASL_CB_ECHOPROMPT,
				    (sasl_callback_ft *)&echo_cb, &echo_context) == SASL_FAIL) &&
	(text->password == NULL)) {
	pass_result = _plug_get_password(params->utils, &text->password,
					 &text->free_password, prompt_need);
	
	if ((pass_result != SASL_OK) && (pass_result != SASL_INTERACT))
	    return pass_result;
    }

    /* free prompts we got */
    if (prompt_need && *prompt_need) {
	params->utils->free(*prompt_need);
	*prompt_need = NULL;
    }
    
    /* if there are prompts not filled in */
    if ((user_result == SASL_INTERACT) || (auth_result == SASL_INTERACT) ||
	(pass_result == SASL_INTERACT)) {
	/* make the prompt list */
	result =
	    _plug_make_prompts(params->utils, prompt_need,
			       user_result == SASL_INTERACT ?
			       "Please enter your authorization name" : NULL,
			       NULL,
			       auth_result == SASL_INTERACT ?
			       "Please enter your authentication name" : NULL,
			       NULL,
			       pass_result == SASL_INTERACT ?
			       "Please enter your secret pass-phrase" : NULL,
			       NULL,
			       NULL, NULL, NULL,
			       NULL, NULL, NULL);
	if (result != SASL_OK) return result;
	
	return SASL_INTERACT;
    }
    
    if (!user || !*user) {
	result = params->canon_user(params->utils->conn, authid, 0,
				    SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
    }
    else {
	result = params->canon_user(params->utils->conn, user, 0,
				    SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) return result;

	result = params->canon_user(params->utils->conn, authid, 0,
				    SASL_CU_AUTHID, oparams);
    }
    if (result != SASL_OK) return result;
    
    /* send authorized id NUL authentication id */
    *clientoutlen = oparams->ulen + 1 + oparams->alen;
    
    /* remember the extra NUL on the end for stupid clients */
    result = _plug_buf_alloc(params->utils, &(text->out_buf),
			     &(text->out_buf_len), *clientoutlen + 1);
    if (result != SASL_OK) return result;
    
    memset(text->out_buf, 0, *clientoutlen + 1);
    memcpy(text->out_buf, oparams->user, oparams->ulen);
    memcpy(text->out_buf+oparams->ulen+1, oparams->authid, oparams->alen);
    *clientout = text->out_buf;
    
    text->state = 2;
    
    return SASL_CONTINUE;
}

static int otp_client_mech_step2(client_context_t *text,
				 sasl_client_params_t *params,
				 const char *serverin,
				 unsigned serverinlen,
				 sasl_interact_t **prompt_need,
				 const char **clientout,
				 unsigned *clientoutlen,
				 sasl_out_params_t *oparams)
{
    int echo_result = SASL_OK;
    int result;
    
    if (serverinlen > OTP_CHALLENGE_MAX) {
	SETERROR(params->utils, "OTP challenge too long");
	return SASL_BADPROT;
    }
    
    /* we can't assume that challenge is null-terminated */
    strncpy(text->challenge, serverin, serverinlen);
    text->challenge[serverinlen] = '\0';
    
    /* try to get the one-time password if we don't have the secret */
    if ((text->password == NULL) && (text->otpassword == NULL)) {
	echo_result = _plug_challenge_prompt(params->utils,
					     SASL_CB_ECHOPROMPT,
					     text->challenge,
					     "Please enter your one-time password",
					     &text->otpassword,
					     prompt_need);
	
	if ((echo_result != SASL_OK) && (echo_result != SASL_INTERACT))
	    return echo_result;
    }
    
    /* free prompts we got */
    if (prompt_need && *prompt_need) {
	params->utils->free(*prompt_need);
	*prompt_need = NULL;
    }
    
    /* if there are prompts not filled in */
    if (echo_result == SASL_INTERACT) {
	/* make the prompt list */
	result =
	    _plug_make_prompts(params->utils,
			       prompt_need,
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       text->challenge,
			       "Please enter your one-time password",
			       NULL,
			       NULL,
			       NULL,
			       NULL);
	if (result != SASL_OK) return result;
	
	return SASL_INTERACT;
    }
    
    /* the application provided us with a one-time password so use it */
    if (text->otpassword) {
	*clientout = text->otpassword;
	*clientoutlen = (unsigned) strlen(text->otpassword);
    }
    /* generate our own response using the user's secret pass-phrase */
    else {
	algorithm_option_t *alg;
	unsigned seq;
	char seed[OTP_SEED_MAX+1];
	unsigned char otp[OTP_HASH_SIZE];
	int init_done = 0;
	
	/* parse challenge */
	result = parse_challenge(params->utils,
				 text->challenge,
				 &alg,
				 &seq,
				 seed,
				 0);
	if (result != SASL_OK) return result;
	
	if (!text->password) {
	    PARAMERROR(params->utils);
	    return SASL_BADPARAM;
	}
	
	if (seq < 1) {
	    SETERROR(params->utils, "OTP has expired (sequence < 1)");
	    return SASL_EXPIRED;
	}
	
	/* generate otp */
	result = generate_otp(params->utils, alg, seq, seed,
			      text->password->data, text->password->len, otp);
	if (result != SASL_OK) return result;
	
	result = _plug_buf_alloc(params->utils, &(text->out_buf),
				 &(text->out_buf_len), OTP_RESPONSE_MAX+1);
	if (result != SASL_OK) return result;
	
	if (seq < OTP_SEQUENCE_REINIT) {
	    unsigned short randnum;
	    char new_seed[OTP_SEED_MAX+1];
	    unsigned char new_otp[OTP_HASH_SIZE];
	    
	    /* try to reinitialize */
	    
	    /* make sure we have a different seed */
	    do {
		params->utils->rand(params->utils->rpool,
				    (char*) &randnum, sizeof(randnum));
		sprintf(new_seed, "%.2s%04u", params->serverFQDN,
			(randnum % 9999) + 1);
	    } while (!strcasecmp(seed, new_seed));
	    
	    result = generate_otp(params->utils, alg, OTP_SEQUENCE_DEFAULT,
				  new_seed, text->password->data, text->password->len, new_otp);
	    
	    if (result == SASL_OK) {
		/* create an init-hex response */
		strcpy(text->out_buf, OTP_INIT_HEX_TYPE);
		bin2hex(otp, OTP_HASH_SIZE,
			text->out_buf+strlen(text->out_buf));
		sprintf(text->out_buf+strlen(text->out_buf), ":%s %u %s:",
			alg->name, OTP_SEQUENCE_DEFAULT, new_seed);
		bin2hex(new_otp, OTP_HASH_SIZE,
			text->out_buf+strlen(text->out_buf));
		init_done = 1;
	    }
	    else {
		/* just do a regular response */
	    }
	}
	
	if (!init_done) {
	    /* created hex response */
	    strcpy(text->out_buf, OTP_HEX_TYPE);
	    bin2hex(otp, OTP_HASH_SIZE, text->out_buf+strlen(text->out_buf));
	}
	
	*clientout = text->out_buf;
	*clientoutlen = (unsigned) strlen(text->out_buf);
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
    
    return SASL_OK;
}

static int otp_client_mech_step(void *conn_context,
				sasl_client_params_t *params,
				const char *serverin,
				unsigned serverinlen,
				sasl_interact_t **prompt_need,
				const char **clientout,
				unsigned *clientoutlen,
				sasl_out_params_t *oparams)
{
    client_context_t *text = (client_context_t *) conn_context;
    
    *clientout = NULL;
    *clientoutlen = 0;
    
    switch (text->state) {
	
    case 1:
	return otp_client_mech_step1(text, params, serverin, serverinlen,
				     prompt_need, clientout, clientoutlen,
				     oparams);
	
    case 2:
	return otp_client_mech_step2(text, params, serverin, serverinlen,
				     prompt_need, clientout, clientoutlen,
				     oparams);
	
    default:
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid OTP client step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static void otp_client_mech_dispose(void *conn_context,
				    const sasl_utils_t *utils)
{
    client_context_t *text = (client_context_t *) conn_context;
    
    if (!text) return;
    
    if (text->free_password) _plug_free_secret(utils, &(text->password));
    
    if (text->out_buf) utils->free(text->out_buf);
    
    utils->free(text);
}

static sasl_client_plug_t otp_client_plugins[] = 
{
    {
	"OTP",				/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_FORWARD_SECRECY,	/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	NULL,				/* required_prompts */
	NULL,				/* glob_context */
	&otp_client_mech_new,		/* mech_new */
	&otp_client_mech_step,		/* mech_step */
	&otp_client_mech_dispose,	/* mech_dispose */
	&otp_common_mech_free,		/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int otp_client_plug_init(sasl_utils_t *utils,
			 int maxversion,
			 int *out_version,
			 sasl_client_plug_t **pluglist,
			 int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR(utils, "OTP version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = otp_client_plugins;
    *plugcount = 1;
    
    /* Add all digests */
    OpenSSL_add_all_digests();
    
    return SASL_OK;
}
