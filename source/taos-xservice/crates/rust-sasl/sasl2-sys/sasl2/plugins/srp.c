/* SRP SASL plugin
 * Ken Murchison
 * Tim Martin  3/17/00
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

/*
 * Notes:
 *
 * - The authentication exchanges *should* be correct (per draft -08)
 *   but we won't know until we do some interop testing.
 *
 * - The security layers don't conform to draft -08:
 *    o  We don't use eos() and os() elements in an SRP buffer, we send
 *      just the bare octets.
 *    o  We don't yet use the PRNG() and KDF() primatives described in
 *       section 5.1.
 *
 * - Are we using cIV and sIV correctly for encrypt/decrypt?
 *
 * - We don't implement fast reauth.
 */

#include <config.h>
#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <limits.h>
#include <stdarg.h>

#ifndef UINT32_MAX
#define UINT32_MAX 4294967295U
#endif

#if UINT_MAX == UINT32_MAX
typedef unsigned int uint32;
#elif ULONG_MAX == UINT32_MAX
typedef unsigned long uint32;
#elif USHRT_MAX == UINT32_MAX
typedef unsigned short uint32;
#else
#error dont know what to use for uint32
#endif

/* for big number support */
#include <openssl/bn.h>

/* for digest and cipher support */
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>

/* for legacy libcrypto support */
#include "crypto-compat.h"

#include <sasl.h>
#define MD5_H  /* suppress internal MD5 */
#include <saslplug.h>

#include "plugin_common.h"

#ifdef macintosh
#include <sasl_srp_plugin_decl.h>
#endif 

/*****************************  Common Section  *****************************/

/* Size limit of cipher block size */
#define SRP_MAXBLOCKSIZE 16
/* Size limit of SRP buffer */
#define SRP_MAXBUFFERSIZE 2147483643UL

#define DEFAULT_MDA		"SHA-1"

#define OPTION_MDA		"mda="
#define OPTION_REPLAY_DETECTION	"replay_detection"
#define OPTION_INTEGRITY	"integrity="
#define OPTION_CONFIDENTIALITY	"confidentiality="
#define OPTION_MANDATORY	"mandatory="
#define OPTION_MAXBUFFERSIZE	"maxbuffersize="

/* Table of recommended Modulus (base 16) and Generator pairs */
struct Ng {
    char *N;
    unsigned long g;
} Ng_tab[] = {
    /* [264 bits] */
    { "115B8B692E0E045692CF280B436735C77A5A9E8A9E7ED56C965F87DB5B2A2ECE3",
      2
    },
    /* [384 bits] */
    { "8025363296FB943FCE54BE717E0E2958A02A9672EF561953B2BAA3BAACC3ED5754EB764C7AB7184578C57D5949CCB41B",
      2
    },
    /* [512 bits] */
    { "D4C7F8A2B32C11B8FBA9581EC4BA4F1B04215642EF7355E37C0FC0443EF756EA2C6B8EEB755A1C723027663CAA265EF785B8FF6A9B35227A52D86633DBDFCA43",
      2
    },
    /* [640 bits] */
    { "C94D67EB5B1A2346E8AB422FC6A0EDAEDA8C7F894C9EEEC42F9ED250FD7F0046E5AF2CF73D6B2FA26BB08033DA4DE322E144E7A8E9B12A0E4637F6371F34A2071C4B3836CBEEAB15034460FAA7ADF483",
      2
    },
    /* [768 bits] */
    { "B344C7C4F8C495031BB4E04FF8F84EE95008163940B9558276744D91F7CC9F402653BE7147F00F576B93754BCDDF71B636F2099E6FFF90E79575F3D0DE694AFF737D9BE9713CEF8D837ADA6380B1093E94B6A529A8C6C2BE33E0867C60C3262B",
      2
    },
    /* [1024 bits] */
    { "EEAF0AB9ADB38DD69C33F80AFA8FC5E86072618775FF3C0B9EA2314C9C256576D674DF7496EA81D3383B4813D692C6E0E0D5D8E250B98BE48E495C1D6089DAD15DC7D7B46154D6B6CE8EF4AD69B15D4982559B297BCF1885C529F566660E57EC68EDBC3C05726CC02FD4CBF4976EAA9AFD5138FE8376435B9FC61D2FC0EB06E3",
      2
    },
    /* [1280 bits] */
    { "D77946826E811914B39401D56A0A7843A8E7575D738C672A090AB1187D690DC43872FC06A7B6A43F3B95BEAEC7DF04B9D242EBDC481111283216CE816E004B786C5FCE856780D41837D95AD787A50BBE90BD3A9C98AC0F5FC0DE744B1CDE1891690894BC1F65E00DE15B4B2AA6D87100C9ECC2527E45EB849DEB14BB2049B163EA04187FD27C1BD9C7958CD40CE7067A9C024F9B7C5A0B4F5003686161F0605B",
      2
    },
    /* [1536 bits] */
    { "9DEF3CAFB939277AB1F12A8617A47BBBDBA51DF499AC4C80BEEEA9614B19CC4D5F4F5F556E27CBDE51C6A94BE4607A291558903BA0D0F84380B655BB9A22E8DCDF028A7CEC67F0D08134B1C8B97989149B609E0BE3BAB63D47548381DBC5B1FC764E3F4B53DD9DA1158BFD3E2B9C8CF56EDF019539349627DB2FD53D24B7C48665772E437D6C7F8CE442734AF7CCB7AE837C264AE3A9BEB87F8A2FE9B8B5292E5A021FFF5E91479E8CE7A28C2442C6F315180F93499A234DCF76E3FED135F9BB",
      2
    },
    /* [2048 bits] */
    { "AC6BDB41324A9A9BF166DE5E1389582FAF72B6651987EE07FC3192943DB56050A37329CBB4A099ED8193E0757767A13DD52312AB4B03310DCD7F48A9DA04FD50E8083969EDB767B0CF6095179A163AB3661A05FBD5FAAAE82918A9962F0B93B855F97993EC975EEAA80D740ADBF4FF747359D041D5C33EA71D281E446B14773BCA97B43A23FB801676BD207A436C6481F1D2B9078717461A5B9D32E688F87748544523B524B0D57D5EA77A2775D2ECFA032CFBDBF52FB3786160279004E57AE6AF874E7303CE53299CCC041C7BC308D82A5698F3A8D0C38271AE35F8E9DBFBB694B5C803D89F7AE435DE236D525F54759B65E372FCD68EF20FA7111F9E4AFF73",
      2
    }
};

#define NUM_Ng (sizeof(Ng_tab) / sizeof(struct Ng))


typedef struct layer_option_s {
    const char *name;		/* name used in option strings */
    unsigned enabled;		/* enabled?  determined at run-time */
    unsigned bit;		/* unique bit in bitmask */
    sasl_ssf_t ssf;		/* ssf of layer */
    const char *evp_name;	/* name used for lookup in EVP table */
} layer_option_t;

static layer_option_t digest_options[] = {
    { "SHA-1",		0, (1<<0), 1,	"sha1" },
    { "RIPEMD-160",	0, (1<<1), 1,	"rmd160" },
    { "MD5",		0, (1<<2), 1,	"md5" },
    { NULL,		0,      0, 0,	NULL }
};
static layer_option_t *default_digest = &digest_options[0];
static layer_option_t *server_mda = NULL;

static layer_option_t cipher_options[] = {
    { "DES",		0, (1<<0), 56,	"des-ofb" },
    { "3DES",		0, (1<<1), 112,	"des-ede-ofb" },
    { "AES",		0, (1<<2), 128,	"aes-128-ofb" },
    { "Blowfish",	0, (1<<3), 128,	"bf-ofb" },
    { "CAST-128",	0, (1<<4), 128,	"cast5-ofb" },
    { "IDEA",		0, (1<<5), 128,	"idea-ofb" },
    { NULL,		0,      0, 0,	NULL}
};
/* XXX Hack until OpenSSL 0.9.7 */
#if OPENSSL_VERSION_NUMBER < 0x00907000L
static layer_option_t *default_cipher = &cipher_options[0];
#else
static layer_option_t *default_cipher = &cipher_options[2];
#endif


enum {
    BIT_REPLAY_DETECTION=	(1<<0),
    BIT_INTEGRITY=		(1<<1),
    BIT_CONFIDENTIALITY=	(1<<2)
};

typedef struct srp_options_s {
    unsigned mda;		/* bitmask of MDAs */
    unsigned replay_detection;	/* replay detection on/off flag */
    unsigned integrity;		/* bitmask of integrity layers */
    unsigned confidentiality;	/* bitmask of confidentiality layers */
    unsigned mandatory;		/* bitmask of mandatory layers */
    unsigned long maxbufsize;	/* max # bytes processed by security layer */
} srp_options_t;

/* The main SRP context */
typedef struct context {
    int state;
    
    BIGNUM *N;			/* safe prime modulus */
    BIGNUM *g;			/* generator */
    
    BIGNUM *v;			/* password verifier */
    
    BIGNUM *b;			/* server private key */
    BIGNUM *B;			/* server public key */
    
    BIGNUM *a;			/* client private key */
    BIGNUM *A;			/* client public key */
    
    unsigned char K[EVP_MAX_MD_SIZE];	/* shared context key */
    unsigned int Klen;
    
    unsigned char M1[EVP_MAX_MD_SIZE];	/* client evidence */
    unsigned int M1len;
    
    char *authid;		/* authentication id (server) */
    char *userid;		/* authorization id (server) */
    sasl_secret_t *password;	/* user secret (client) */
    unsigned int free_password; /* set if we need to free password */
    
    char *client_options;
    char *server_options;
    
    srp_options_t client_opts;	/* cache between client steps */
    unsigned char cIV[SRP_MAXBLOCKSIZE];	/* cache between client steps */
    
    char *salt;			/* password salt */
    int saltlen;
    
    const EVP_MD *md;		/* underlying MDA */
    
    /* copy of utils from the params structures */
    const sasl_utils_t *utils;
    
    /* per-step mem management */
    char *out_buf;
    unsigned out_buf_len;
    
    /* Layer foo */
    unsigned layer;		/* bitmask of enabled layers */
    const EVP_MD *hmac_md;	/* HMAC for integrity */
    HMAC_CTX *hmac_send_ctx;
    HMAC_CTX *hmac_recv_ctx;

    const EVP_CIPHER *cipher;	/* cipher for confidentiality */
    EVP_CIPHER_CTX *cipher_enc_ctx;
    EVP_CIPHER_CTX *cipher_dec_ctx;
    
    /* replay detection sequence numbers */
    int seqnum_out;
    int seqnum_in;
    
    /* for encoding/decoding mem management */
    char           *encode_buf, *decode_buf, *decode_pkt_buf;
    unsigned       encode_buf_len, decode_buf_len, decode_pkt_buf_len;
    
    /* layers buffering */
    decode_context_t decode_context;
    
} context_t;

static int srp_encode(void *context,
		      const struct iovec *invec,
		      unsigned numiov,
		      const char **output,
		      unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    unsigned i;
    char *input;
    unsigned long inputlen, tmpnum;
    int ret;
    
    if (!context || !invec || !numiov || !output || !outputlen) {
	PARAMERROR( text->utils );
	return SASL_BADPARAM;
    }

    /* calculate total size of input */
    for (i = 0, inputlen = 0; i < numiov; i++)
	inputlen += invec[i].iov_len;

    /* allocate a buffer for the output */
    ret = _plug_buf_alloc(text->utils, &text->encode_buf,
			  &text->encode_buf_len,
			  4 +			/* for length */
			  inputlen +		/* for content */
			  SRP_MAXBLOCKSIZE +	/* for PKCS padding */
			  EVP_MAX_MD_SIZE);	/* for HMAC */
    if (ret != SASL_OK) return ret;

    *outputlen = 4; /* length */

    /* operate on each iovec */
    for (i = 0; i < numiov; i++) {
	input = invec[i].iov_base;
	inputlen = invec[i].iov_len;
    
	if (text->layer & BIT_CONFIDENTIALITY) {
	    int enclen;

	    /* encrypt the data into the output buffer */
	    EVP_EncryptUpdate(text->cipher_enc_ctx,
			      (unsigned char *) text->encode_buf + *outputlen,
                              &enclen, (unsigned char *) input, inputlen);
	    *outputlen += enclen;

	    /* switch the input to the encrypted data */
	    input = text->encode_buf + 4;
	    inputlen = *outputlen - 4;
	}
	else {
	    /* copy the raw input to the output */
	    memcpy(text->encode_buf + *outputlen, input, inputlen);
	    *outputlen += inputlen;
	}
    }
    
    if (text->layer & BIT_CONFIDENTIALITY) {
	int enclen;

	/* encrypt the last block of data into the output buffer */
	EVP_EncryptFinal(text->cipher_enc_ctx,
			 (unsigned char *) text->encode_buf + *outputlen,
                         &enclen);
	*outputlen += enclen;
    }

    if (text->layer & BIT_INTEGRITY) {
	unsigned hashlen;

	/* hash the content */
	HMAC_Update(text->hmac_send_ctx,
                    (unsigned char *) text->encode_buf+4, *outputlen-4);
	
	if (text->layer & BIT_REPLAY_DETECTION) {
	    /* hash the sequence number */
	    tmpnum = htonl(text->seqnum_out);
	    HMAC_Update(text->hmac_send_ctx, (unsigned char *) &tmpnum, 4);
	    
	    text->seqnum_out++;
	}

	/* append the HMAC into the output buffer */
	HMAC_Final(text->hmac_send_ctx,
                   (unsigned char *) text->encode_buf + *outputlen,
		   &hashlen);
	*outputlen += hashlen;
    }

    /* prepend the length of the output */
    tmpnum = *outputlen - 4;
    tmpnum = htonl(tmpnum);
    memcpy(text->encode_buf, &tmpnum, 4);

    *output = text->encode_buf;
    
    return SASL_OK;
}

/* decode a single SRP packet */
static int srp_decode_packet(void *context,
			     const char *input,
			     unsigned inputlen,
			     char **output,
			     unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    int ret;

    if (text->layer & BIT_INTEGRITY) {
	const char *hash;
	unsigned char myhash[EVP_MAX_MD_SIZE];
	unsigned hashlen;
	unsigned long tmpnum;

	hashlen = EVP_MD_size(text->hmac_md);

	if (inputlen < hashlen) {
	    text->utils->seterror(text->utils->conn, 0,
				  "SRP input is smaller "
				  "than hash length: %d vs %d\n",
				  inputlen, hashlen);
	    return SASL_BADPROT;
	}

	inputlen -= hashlen;
	hash = input + inputlen;

	/* create our own hash from the input */
	HMAC_Update(text->hmac_recv_ctx, (unsigned char *) input, inputlen);
	    
	if (text->layer & BIT_REPLAY_DETECTION) {
	    /* hash the sequence number */
	    tmpnum = htonl(text->seqnum_in);
	    HMAC_Update(text->hmac_recv_ctx, (unsigned char *) &tmpnum, 4);
		
	    text->seqnum_in++;
	}
	    
	HMAC_Final(text->hmac_recv_ctx, myhash, &hashlen);

	/* compare hashes */
        if (memcmp(hash, myhash, hashlen)) {
            SETERROR(text->utils, "Hash is incorrect\n");
            return SASL_BADMAC;
        }
    }
	
    ret = _plug_buf_alloc(text->utils, &(text->decode_pkt_buf),
			  &(text->decode_pkt_buf_len),
			  inputlen);
    if (ret != SASL_OK) return ret;
	
    if (text->layer & BIT_CONFIDENTIALITY) {
	int declen;

	/* decrypt the data into the output buffer */
	EVP_DecryptUpdate(text->cipher_dec_ctx,
			  (unsigned char *) text->decode_pkt_buf, &declen,
			  (unsigned char *) input, inputlen);
	*outputlen = declen;
	    
	EVP_DecryptFinal(text->cipher_dec_ctx,
			 (unsigned char *) text->decode_pkt_buf + declen,
                         &declen);
	*outputlen += declen;
    } else {
	/* copy the raw input to the output */
	memcpy(text->decode_pkt_buf, input, inputlen);
	*outputlen = inputlen;
    }

    *output = text->decode_pkt_buf;
    
    return SASL_OK;
}

/* decode and concatenate multiple SRP packets */
static int srp_decode(void *context,
		      const char *input, unsigned inputlen,
		      const char **output, unsigned *outputlen)
{
    context_t *text = (context_t *) context;
    int ret;
    
    ret = _plug_decode(&text->decode_context, input, inputlen,
		       &text->decode_buf, &text->decode_buf_len, outputlen,
		       srp_decode_packet, text);
    
    *output = text->decode_buf;
    
    return ret;
}

/*
 * Convert a big integer to it's byte representation
 */
static int BigIntToBytes(BIGNUM *num, unsigned char *out, int maxoutlen,
                         unsigned int *outlen)
{
    int len;
    
    len = BN_num_bytes(num);
    
    if (len > maxoutlen) return SASL_FAIL;
    
    *outlen = BN_bn2bin(num, out);
    
    return SASL_OK;    
}

/*
 * Compare a big integer against a word.
 */
static int BigIntCmpWord(BIGNUM *a, BN_ULONG w)
{
    BIGNUM *b = BN_new();
    int r;
    
    BN_set_word(b, w);
    r = BN_cmp(a, b);
    BN_free(b);
    return r;
}

/*
 * Generate a random big integer.
 */
static void GetRandBigInt(BIGNUM **out)
{
    *out = BN_new();
    
    /* xxx likely should use sasl random funcs */
    BN_rand(*out, SRP_MAXBLOCKSIZE*8, 0, 0);
}

#define MAX_BUFFER_LEN 2147483643
#define MAX_MPI_LEN 65535
#define MAX_UTF8_LEN 65535
#define MAX_OS_LEN 255

/*
 * Make an SRP buffer from the data specified by the fmt string.
 */
static int MakeBuffer(const sasl_utils_t *utils, char **buf, unsigned *buflen,
		      unsigned *outlen, const char *fmt, ...)
{
    va_list ap;
    char *p, *out = NULL;
    int r, alloclen, len;
    BIGNUM *mpi;
    char *os, *str, c;
    uint32 u;
    short ns;
    long totlen;

    /* first pass to calculate size of buffer */
    va_start(ap, fmt);
    for (p = (char *) fmt, alloclen = 0; *p; p++) {
	if (*p != '%') {
	    alloclen++;
	    continue;
	}

	switch (*++p) {
	case 'm':
	    /* MPI */
	    mpi = va_arg(ap, BIGNUM *);
	    len = BN_num_bytes(mpi);
	    if (len > MAX_MPI_LEN) {
		utils->log(NULL, SASL_LOG_ERR,
			   "String too long to create mpi string\n");
		r = SASL_FAIL;
		goto done;
	    }
	    alloclen += len + 2;
	    break;

	case 'o':
	    /* octet sequence (len followed by data) */
	    len = va_arg(ap, int);
	    if (len > MAX_OS_LEN) {
		utils->log(NULL, SASL_LOG_ERR,
			   "String too long to create os string\n");
		r = SASL_FAIL;
		goto done;
	    }
	    alloclen += len + 1;
	    os = va_arg(ap, char *);
	    break;

	case 's':
	    /* string */
	    str = va_arg(ap, char *);
	    len = strlen(str);
	    if (len > MAX_UTF8_LEN) {
		utils->log(NULL, SASL_LOG_ERR,
			   "String too long to create utf8 string\n");
		r = SASL_FAIL;
		goto done;
	    }
	    alloclen += len + 2;
	    break;

	case 'u':
	    /* unsigned int */
	    u = va_arg(ap, uint32);
	    alloclen += sizeof(uint32);
	    break;

	case 'c':
	    /* char */
	    c = va_arg(ap, int) & 0xFF;
	    alloclen += 1;
	    break;

	default:
	    alloclen += 1;
	    break;
	}
    }
    va_end(ap);

    if (alloclen > MAX_BUFFER_LEN) {
	utils->log(NULL, SASL_LOG_ERR,
		   "String too long to create SRP buffer string\n");
	return SASL_FAIL;
    }

    alloclen += 4;
    r = _plug_buf_alloc(utils, buf, buflen, alloclen);
    if (r != SASL_OK) return r;

    out = *buf + 4; /* skip size for now */

    /* second pass to fill buffer */
    va_start(ap, fmt);
    for (p = (char *) fmt; *p; p++) {
	if (*p != '%') {
	    *out = *p;
	    out++;
	    continue;
	}

	switch (*++p) {
	case 'm':
	    /* MPI */
	    mpi = va_arg(ap, BIGNUM *);
	    r = BigIntToBytes(mpi, (unsigned char *) out+2,
                              BN_num_bytes(mpi), (unsigned *) &len);
	    if (r) goto done;
	    ns = htons(len);
	    memcpy(out, &ns, 2);	/* add 2 byte len (network order) */
	    out += len + 2;
	    break;

	case 'o':
	    /* octet sequence (len followed by data) */
	    len = va_arg(ap, int);
	    os = va_arg(ap, char *);
	    *out = len & 0xFF;		/* add 1 byte len */
	    memcpy(out+1, os, len);	/* add data */
	    out += len+1;
	    break;

	case 's':
	    /* string */
	    str = va_arg(ap, char *);
	    /* xxx do actual utf8 conversion */
	    len = strlen(str);
	    ns = htons(len);
	    memcpy(out, &ns, 2);	/* add 2 byte len (network order) */
	    memcpy(out+2, str, len);	/* add string */
	    out += len + 2;
	    break;

	case 'u':
	    /* unsigned int */
	    u = va_arg(ap, uint32);
	    u = htonl(u);
	    memcpy(out, &u, sizeof(uint32));
	    out += sizeof(uint32);
	    break;

	case 'c':
	    /* char */
	    c = va_arg(ap, int) & 0xFF;
	    *out = c;
	    out++;
	    break;

	default:
	    *out = *p;
	    out++;
	    break;
	}
    }
  done:
    va_end(ap);

    *outlen = out - *buf;

    /* add 4 byte len (network order) */
    totlen = htonl(*outlen - 4);
    memcpy(*buf, &totlen, 4);

    return r;
}

/* 
 * Extract an SRP buffer into the data specified by the fmt string.
 *
 * A '-' flag means don't allocate memory for the data ('o' only).
 */
static int UnBuffer(const sasl_utils_t *utils, const char *buf,
		    unsigned buflen, const char *fmt, ...)
{
    va_list ap;
    char *p;
    int r = SASL_OK, noalloc;
    BIGNUM **mpi;
    char **os, **str;
    uint32 *u;
    unsigned short ns;
    unsigned len;

    if (!buf || buflen < 4) {
	utils->seterror(utils->conn, 0,
			"Buffer is not big enough to be SRP buffer: %d\n",
			buflen);
	return SASL_BADPROT;
    }
    
    /* get the length */
    memcpy(&len, buf, 4);
    len = ntohl(len);
    buf += 4;
    buflen -= 4;

    /* make sure it's right */
    if (len != buflen) {
	SETERROR(utils, "SRP Buffer isn't of the right length\n");
	return SASL_BADPROT;
    }
    
    va_start(ap, fmt);
    for (p = (char *) fmt; *p; p++) {
	if (*p != '%') {
	    if (*buf != *p) {
		r = SASL_BADPROT;
		goto done;
	    }
	    buf++;
	    buflen--;
	    continue;
	}

	/* check for noalloc flag */
	if ((noalloc = (*++p == '-'))) ++p;

	switch (*p) {
	case 'm':
	    /* MPI */
	    if (buflen < 2) {
		SETERROR(utils, "Buffer is not big enough to be SRP MPI\n");
		r = SASL_BADPROT;
		goto done;
	    }
    
	    /* get the length */
	    memcpy(&ns, buf, 2);
	    len = ntohs(ns);
	    buf += 2;
	    buflen -= 2;
    
	    /* make sure it's right */
	    if (len > buflen) {
		SETERROR(utils, "Not enough data for this SRP MPI\n");
		r = SASL_BADPROT;
		goto done;
	    }
	    
	    mpi = va_arg(ap, BIGNUM **);
            if (mpi) {
                if (!*mpi) *mpi = BN_new();
                else BN_clear(*mpi);
                BN_bin2bn((unsigned char *) buf, len, *mpi);
            }
	    break;

	case 'o':
	    /* octet sequence (len followed by data) */
	    if (buflen < 1) {
		SETERROR(utils, "Buffer is not big enough to be SRP os\n");
		r = SASL_BADPROT;
		goto done;
	    }

	    /* get the length */
	    len = (unsigned char) *buf;
	    buf++;
	    buflen--;

	    /* make sure it's right */
	    if (len > buflen) {
		SETERROR(utils, "Not enough data for this SRP os\n");
		r = SASL_BADPROT;
		goto done;
	    }
	    
	    *(va_arg(ap, int *)) = len;
	    os = va_arg(ap, char **);

	    if (noalloc)
		*os = (char *) buf;
	    else {
		*os = (char *) utils->malloc(len);
		if (!*os) {
		    r = SASL_NOMEM;
		    goto done;
		}
    
		memcpy(*os, buf, len);
	    }
	    break;

	case 's':
	    /* string */
	    if (buflen < 2) {
		SETERROR(utils, "Buffer is not big enough to be SRP UTF8\n");
		r = SASL_BADPROT;
		goto done;
	    }
    
	    /* get the length */
	    memcpy(&ns, buf, 2);
	    len = ntohs(ns);
	    buf += 2;
	    buflen -= 2;
    
	    /* make sure it's right */
	    if (len > buflen) {
		SETERROR(utils, "Not enough data for this SRP UTF8\n");
		r = SASL_BADPROT;
		goto done;
	    }
	    
	    str = va_arg(ap, char **);
	    *str = (char *) utils->malloc(len+1); /* +1 for NUL */
	    if (!*str) {
		r = SASL_NOMEM;
		goto done;
	    }
    
	    memcpy(*str, buf, len);
	    (*str)[len] = '\0';
	    break;

	case 'u':
	    /* unsigned int */
	    if (buflen < sizeof(uint32)) {
		SETERROR(utils, "Buffer is not big enough to be SRP uint\n");
		r = SASL_BADPROT;
		goto done;
	    }

	    len = sizeof(uint32);
	    u = va_arg(ap, uint32*);
	    memcpy(u, buf, len);
	    *u = ntohs(*u);
	    break;

	case 'c':
	    /* char */
	    if (buflen < 1) {
		SETERROR(utils, "Buffer is not big enough to be SRP char\n");
		r = SASL_BADPROT;
		goto done;
	    }

	    len = 1;
	    *(va_arg(ap, char *)) = *buf;
	    break;

	default:
	    len = 1;
	    if (*buf != *p) {
		r = SASL_BADPROT;
		goto done;
	    }
	    break;
	}

	buf += len;
	buflen -= len;
    }

  done:
    va_end(ap);

    if (buflen != 0) {
	SETERROR(utils, "Extra data in SRP buffer\n");
	r = SASL_BADPROT;
    }

    return r;
}

/*
 * Apply the hash function to the data specifed by the fmt string.
 */
static int MakeHash(const EVP_MD *md,
                    unsigned char hash[], unsigned int *hashlen,
		    const char *fmt, ...)
{
    va_list ap;
    char *p, buf[4096], *in;
    unsigned int inlen;
    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    int r = 0, hflag;

    EVP_DigestInit(mdctx, md);

    va_start(ap, fmt);
    for (p = (char *) fmt; *p; p++) {
	if (*p != '%') {
	    in = p;
	    inlen = 1;
	    hflag = 0;
	}
	else {
	    if ((hflag = (*++p == 'h'))) ++p;

	    switch (*p) {
	    case 'm': {
		/* MPI */
		BIGNUM *mval = va_arg(ap, BIGNUM *);

		in = buf;
		r = BigIntToBytes(mval, (unsigned char *) buf, sizeof(buf)-1, &inlen);
		if (r) goto done;
		break;
	    }

	    case 'o': {
		/* octet sequence (len followed by data) */
		inlen = va_arg(ap, int);
		in = va_arg(ap, char *);
		break;
	    }

	    case 's':
		/* string */
		in = va_arg(ap, char *);
		inlen = strlen(in);
		break;

	    case 'u': {
		/* unsigned int */
		uint32 uval = va_arg(ap, uint32);

		in = buf;
		inlen = sizeof(uint32);
		*((uint32 *) buf) = htonl(uval);
		break;
	    }

	    default:
		in = p;
		inlen = 1;
		break;
	    }
	}

	if (hflag) {
	    /* hash data separately before adding to current hash */
	    EVP_MD_CTX *tmpctx = EVP_MD_CTX_new();

	    EVP_DigestInit(tmpctx, md);
	    EVP_DigestUpdate(tmpctx, in, inlen);
	    EVP_DigestFinal(tmpctx, (unsigned char *) buf, &inlen);
            EVP_MD_CTX_free(tmpctx);
	    in = buf;
	}

	EVP_DigestUpdate(mdctx, in, inlen);
    }
  done:
    va_end(ap);

    EVP_DigestFinal(mdctx, hash, hashlen);
    EVP_MD_CTX_free(mdctx);

    return r;
}

static int CalculateX(context_t *text, const char *salt, int saltlen, 
		      const char *user, const unsigned char *pass, int passlen, 
		      BIGNUM **x)
{
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashlen;
    
    /* x = H(salt | H(user | ':' | pass)) */
    MakeHash(text->md, hash, &hashlen, "%s:%o", user, passlen, pass);
    MakeHash(text->md, hash, &hashlen, "%o%o", saltlen, salt, hashlen, hash);
    
    *x = BN_new();
    BN_bin2bn(hash, hashlen, *x);
    
    return SASL_OK;
}

static int CalculateM1(context_t *text, BIGNUM *N, BIGNUM *g,
		       char *U, char *salt, int saltlen,
		       BIGNUM *A, BIGNUM *B,
                       unsigned char *K, unsigned int Klen,
		       char *I, char *L,
                       unsigned char *M1, unsigned int *M1len)
{
    int r;
    unsigned int i, len;
    unsigned char Nhash[EVP_MAX_MD_SIZE];
    unsigned char ghash[EVP_MAX_MD_SIZE];
    unsigned char Ng[EVP_MAX_MD_SIZE];

    /* bytes(H( bytes(N) )) ^ bytes( H( bytes(g) ))
       ^ is the bitwise XOR operator. */
    r = MakeHash(text->md, Nhash, &len, "%m", N);
    if (r) return r;
    r = MakeHash(text->md, ghash, &len, "%m", g);
    if (r) return r;
    
    for (i = 0; i < len; i++) {
	Ng[i] = (Nhash[i] ^ ghash[i]);
    }

    r = MakeHash(text->md, M1, M1len, "%o%hs%o%m%m%o%hs%hs",
		 len, Ng, U, saltlen, salt, A, B, Klen, K, I, L);
    
    return r;
}

static int CalculateM2(context_t *text, BIGNUM *A,
		       unsigned char *M1, unsigned int M1len,
                       unsigned char *K, unsigned int Klen,
		       char *I, char *o, char *sid, uint32 ttl,
		       unsigned char *M2, unsigned int *M2len)
{
    int r;
    
    r = MakeHash(text->md, M2, M2len, "%m%o%o%hs%hs%s%u",
		 A, M1len, M1, Klen, K, I, o, sid, ttl);

    return r;
}

/* Parse an option out of an option string
 * Place found option in 'option'
 * 'nextptr' points to rest of string or NULL if at end
 */
static int ParseOption(const sasl_utils_t *utils,
		       char *in, char **option, char **nextptr)
{
    char *comma;
    int len;
    int i;
    
    if (strlen(in) == 0) {
	*option = NULL;
	return SASL_OK;
    }
    
    comma = strchr(in,',');    
    if (comma == NULL) comma = in + strlen(in);
    
    len = comma - in;
    
    *option = utils->malloc(len + 1);
    if (!*option) return SASL_NOMEM;
    
    /* lowercase string */
    for (i = 0; i < len; i++) {
	(*option)[i] = tolower((int)in[i]);
    }
    (*option)[len] = '\0';
    
    if (*comma) {
	*nextptr = comma+1;
    } else {
	*nextptr = NULL;
    }
    
    return SASL_OK;
}

static int FindBit(char *name, layer_option_t *opts)
{
    while (opts->name) {
	if (!strcasecmp(name, opts->name)) {
	    return opts->bit;
	}
	
	opts++;
    }
    
    return 0;
}

static layer_option_t *FindOptionFromBit(unsigned bit, layer_option_t *opts)
{
    while (opts->name) {
	if (opts->bit == bit) {
	    return opts;
	}
	
	opts++;
    }
    
    return NULL;
}

static int ParseOptionString(const sasl_utils_t *utils,
			     char *str, srp_options_t *opts, int isserver)
{
    if (!strncasecmp(str, OPTION_MDA, strlen(OPTION_MDA))) {
	
	int bit = FindBit(str+strlen(OPTION_MDA), digest_options);
	
	if (isserver && (!bit || opts->mda)) {
	    opts->mda = -1;
	    if (!bit)
		utils->seterror(utils->conn, 0,
				"SRP MDA %s not supported\n",
				str+strlen(OPTION_MDA));
	    else
		SETERROR(utils, "Multiple SRP MDAs given\n");
	    return SASL_BADPROT;
	}
	
	opts->mda |= bit;
	
    } else if (!strcasecmp(str, OPTION_REPLAY_DETECTION)) {
	if (opts->replay_detection) {
	    SETERROR(utils, "SRP Replay Detection option appears twice\n");
	    return SASL_BADPROT;
	}
	opts->replay_detection = 1;
	
    } else if (!strncasecmp(str, OPTION_INTEGRITY, strlen(OPTION_INTEGRITY)) &&
	       !strncasecmp(str+strlen(OPTION_INTEGRITY), "HMAC-", 5)) {
	
	int bit = FindBit(str+strlen(OPTION_INTEGRITY)+5, digest_options);
	
	if (isserver && (!bit || opts->integrity)) {
	    opts->integrity = -1;
	    if (!bit)
		utils->seterror(utils->conn, 0,
				"SRP Integrity option %s not supported\n",
				str+strlen(OPTION_INTEGRITY));
	    else
		SETERROR(utils, "Multiple SRP Integrity options given\n");
	    return SASL_BADPROT;
	}
	
	opts->integrity |= bit;
	
    } else if (!strncasecmp(str, OPTION_CONFIDENTIALITY,
			    strlen(OPTION_CONFIDENTIALITY))) {
	
	int bit = FindBit(str+strlen(OPTION_CONFIDENTIALITY),
			  cipher_options);
	
	if (isserver && (!bit || opts->confidentiality)) {
	    opts->confidentiality = -1;
	    if (!bit)
		utils->seterror(utils->conn, 0,
				"SRP Confidentiality option %s not supported\n",
				str+strlen(OPTION_CONFIDENTIALITY));
	    else
		SETERROR(utils,
			 "Multiple SRP Confidentiality options given\n");
	    return SASL_FAIL;
	}
	
	opts->confidentiality |= bit;
	
    } else if (!isserver && !strncasecmp(str, OPTION_MANDATORY,
					 strlen(OPTION_MANDATORY))) {
	
	char *layer = str+strlen(OPTION_MANDATORY);
	
	if (!strcasecmp(layer, OPTION_REPLAY_DETECTION))
	    opts->mandatory |= BIT_REPLAY_DETECTION;
	else if (!strncasecmp(layer, OPTION_INTEGRITY,
			      strlen(OPTION_INTEGRITY)-1))
	    opts->mandatory |= BIT_INTEGRITY;
	else if (!strncasecmp(layer, OPTION_CONFIDENTIALITY,
			      strlen(OPTION_CONFIDENTIALITY)-1))
	    opts->mandatory |= BIT_CONFIDENTIALITY;
	else {
	    utils->seterror(utils->conn, 0,
			    "Mandatory SRP option %s not supported\n", layer);
	    return SASL_BADPROT;
	}
	
    } else if (!strncasecmp(str, OPTION_MAXBUFFERSIZE,
			    strlen(OPTION_MAXBUFFERSIZE))) {
	
	opts->maxbufsize = strtoul(str+strlen(OPTION_MAXBUFFERSIZE), NULL, 10);
	
	if (opts->maxbufsize > SRP_MAXBUFFERSIZE) {
	    utils->seterror(utils->conn, 0,
			    "SRP Maxbuffersize %lu too big (> %lu)\n",
			    opts->maxbufsize, SRP_MAXBUFFERSIZE);
	    return SASL_BADPROT;
	}
	
    } else {
	/* Ignore unknown options */
    }
    
    return SASL_OK;
}

static int ParseOptions(const sasl_utils_t *utils,
			char *in, srp_options_t *out, int isserver)
{
    int r;
    
    memset(out, 0, sizeof(srp_options_t));
    out->maxbufsize = SRP_MAXBUFFERSIZE;
    
    while (in) {
	char *opt;
	
	r = ParseOption(utils, in, &opt, &in);
	if (r) return r;
	
	if (opt == NULL) return SASL_OK;
	
	utils->log(NULL, SASL_LOG_DEBUG, "Got option: [%s]\n",opt);
	
	r = ParseOptionString(utils, opt, out, isserver);
	utils->free(opt);
	
	if (r) return r;
    }
    
    return SASL_OK;
}

static layer_option_t *FindBest(int available, sasl_ssf_t min_ssf,
				sasl_ssf_t max_ssf, layer_option_t *opts)
{
    layer_option_t *best = NULL;
    
    if (!available) return NULL;
    
    while (opts->name) {
	if (opts->enabled && (available & opts->bit) &&
	    (opts->ssf >= min_ssf) && (opts->ssf <= max_ssf) &&
	    (!best || (opts->ssf > best->ssf))) {
	    best = opts;
	}
	
	opts++;
    }
    
    return best;
}

static int OptionsToString(const sasl_utils_t *utils,
			   srp_options_t *opts, char **out)
{
    char *ret = NULL;
    int alloced = 0;
    int first = 1;
    layer_option_t *optlist;
    
    ret = utils->malloc(1);
    if (!ret) return SASL_NOMEM;
    alloced = 1;
    ret[0] = '\0';
    
    optlist = digest_options;
    while(optlist->name) {
	if (opts->mda & optlist->bit) {
	    alloced += strlen(OPTION_MDA)+strlen(optlist->name)+1;
	    ret = utils->realloc(ret, alloced);
	    if (!ret) return SASL_NOMEM;
	    
	    if (!first) strcat(ret, ",");
	    strcat(ret, OPTION_MDA);
	    strcat(ret, optlist->name);
	    first = 0;
	}
	
	optlist++;
    }
    
    if (opts->replay_detection) {
	alloced += strlen(OPTION_REPLAY_DETECTION)+1;
	ret = utils->realloc(ret, alloced);
	if (!ret) return SASL_NOMEM;
	
	if (!first) strcat(ret, ",");
	strcat(ret, OPTION_REPLAY_DETECTION);
	first = 0;
    }
    
    optlist = digest_options;
    while(optlist->name) {
	if (opts->integrity & optlist->bit) {
	    alloced += strlen(OPTION_INTEGRITY)+5+strlen(optlist->name)+1;
	    ret = utils->realloc(ret, alloced);
	    if (!ret) return SASL_NOMEM;
	    
	    if (!first) strcat(ret, ",");
	    strcat(ret, OPTION_INTEGRITY);
	    strcat(ret, "HMAC-");
	    strcat(ret, optlist->name);
	    first = 0;
	}
	
	optlist++;
    }
    
    optlist = cipher_options;
    while(optlist->name) {
	if (opts->confidentiality & optlist->bit) {
	    alloced += strlen(OPTION_CONFIDENTIALITY)+strlen(optlist->name)+1;
	    ret = utils->realloc(ret, alloced);
	    if (!ret) return SASL_NOMEM;
	    
	    if (!first) strcat(ret, ",");
	    strcat(ret, OPTION_CONFIDENTIALITY);
	    strcat(ret, optlist->name);
	    first = 0;
	}
	
	optlist++;
    }
    
    if ((opts->integrity || opts->confidentiality) &&
	opts->maxbufsize < SRP_MAXBUFFERSIZE) {
	alloced += strlen(OPTION_MAXBUFFERSIZE)+10+1;
	ret = utils->realloc(ret, alloced);
	if (!ret) return SASL_NOMEM;
	
	if (!first) strcat(ret, ",");
	strcat(ret, OPTION_MAXBUFFERSIZE);
	sprintf(ret+strlen(ret), "%lu", opts->maxbufsize);
	first = 0;
    }
    
    if (opts->mandatory & BIT_REPLAY_DETECTION) {
	alloced += strlen(OPTION_MANDATORY)+strlen(OPTION_REPLAY_DETECTION)+1;
	ret = utils->realloc(ret, alloced);
	if (!ret) return SASL_NOMEM;
	
	if (!first) strcat(ret, ",");
	strcat(ret, OPTION_MANDATORY);
	strcat(ret, OPTION_REPLAY_DETECTION);
	first = 0;
    }
    
    if (opts->mandatory & BIT_INTEGRITY) {
	alloced += strlen(OPTION_MANDATORY)+strlen(OPTION_INTEGRITY)-1+1;
	ret = utils->realloc(ret, alloced);
	if (!ret) return SASL_NOMEM;
	
	if (!first) strcat(ret, ",");
	strcat(ret, OPTION_MANDATORY);
	strncat(ret, OPTION_INTEGRITY, strlen(OPTION_INTEGRITY)-1);
	/* terminate string */
	ret[alloced-1] = '\0';
	first = 0;
    }
    
    if (opts->mandatory & BIT_CONFIDENTIALITY) {
	alloced += strlen(OPTION_MANDATORY)+strlen(OPTION_CONFIDENTIALITY)-1+1;
	ret = utils->realloc(ret, alloced);
	if (!ret) return SASL_NOMEM;
	
	if (!first) strcat(ret, ",");
	strcat(ret, OPTION_MANDATORY);
	strncat(ret, OPTION_CONFIDENTIALITY, strlen(OPTION_CONFIDENTIALITY)-1);
	/* terminate string */
	ret[alloced-1] = '\0';
	first = 0;
    }
    
    *out = ret;
    return SASL_OK;
}


/*
 * Set the selected MDA.
 */
static int SetMDA(srp_options_t *opts, context_t *text)
{
    layer_option_t *opt;
    
    opt = FindOptionFromBit(opts->mda, digest_options);
    if (!opt) {
	text->utils->log(NULL, SASL_LOG_ERR,
			 "Unable to find SRP MDA option now\n");
	return SASL_FAIL;
    }
    
    text->md = EVP_get_digestbyname(opt->evp_name);
    
    return SASL_OK;
}

/*
 * Setup the selected security layer.
 */
static int LayerInit(srp_options_t *opts, context_t *text,
		     sasl_out_params_t *oparams,
                     unsigned char *enc_IV, unsigned char *dec_IV,
		     unsigned maxbufsize)
{
    layer_option_t *opt;
    
    if ((opts->integrity == 0) && (opts->confidentiality == 0)) {
	oparams->encode = NULL;
	oparams->decode = NULL;
	oparams->mech_ssf = 0;
	text->utils->log(NULL, SASL_LOG_DEBUG, "Using no protection\n");
	return SASL_OK;
    }
    
    oparams->encode = &srp_encode;
    oparams->decode = &srp_decode;
    oparams->maxoutbuf = opts->maxbufsize - 4; /* account for 4-byte length */

    _plug_decode_init(&text->decode_context, text->utils, maxbufsize);
    
    if (opts->replay_detection) {
	text->utils->log(NULL, SASL_LOG_DEBUG, "Using replay detection\n");

	text->layer |= BIT_REPLAY_DETECTION;
	
	/* If no integrity layer specified, use default */
	if (!opts->integrity)
	    opts->integrity = default_digest->bit;
    }
    
    if (opts->integrity) {
	text->utils->log(NULL, SASL_LOG_DEBUG, "Using integrity protection\n");
	
	text->layer |= BIT_INTEGRITY;
	
	opt = FindOptionFromBit(opts->integrity, digest_options);
	if (!opt) {
	    text->utils->log(NULL, SASL_LOG_ERR,
			     "Unable to find SRP integrity layer option\n");
	    return SASL_FAIL;
	}
	
	oparams->mech_ssf = opt->ssf;

	/* Initialize the HMACs */
	text->hmac_md = EVP_get_digestbyname(opt->evp_name);
        text->hmac_send_ctx = HMAC_CTX_new();
	HMAC_Init_ex(text->hmac_send_ctx, text->K, text->Klen, text->hmac_md, NULL);
        text->hmac_recv_ctx = HMAC_CTX_new();
	HMAC_Init_ex(text->hmac_recv_ctx, text->K, text->Klen, text->hmac_md, NULL);
	
	/* account for HMAC */
	oparams->maxoutbuf -= EVP_MD_size(text->hmac_md);
    }
    
    if (opts->confidentiality) {
	text->utils->log(NULL, SASL_LOG_DEBUG,
			 "Using confidentiality protection\n");
	
	text->layer |= BIT_CONFIDENTIALITY;
	
	opt = FindOptionFromBit(opts->confidentiality, cipher_options);
	if (!opt) {
	    text->utils->log(NULL, SASL_LOG_ERR,
			     "Unable to find SRP confidentiality layer option\n");
	    return SASL_FAIL;
	}
	
	oparams->mech_ssf = opt->ssf;

	/* Initialize the ciphers */
	text->cipher = EVP_get_cipherbyname(opt->evp_name);

        text->cipher_enc_ctx = EVP_CIPHER_CTX_new();
	EVP_CIPHER_CTX_init(text->cipher_enc_ctx);
	EVP_EncryptInit(text->cipher_enc_ctx, text->cipher, text->K, enc_IV);

        text->cipher_dec_ctx = EVP_CIPHER_CTX_new();
	EVP_CIPHER_CTX_init(text->cipher_dec_ctx);
	EVP_DecryptInit(text->cipher_dec_ctx, text->cipher, text->K, dec_IV);
    }
    
    return SASL_OK;
}

static void LayerCleanup(context_t *text)
{
    if (text->layer & BIT_INTEGRITY) {
	HMAC_CTX_free(text->hmac_send_ctx);
	HMAC_CTX_free(text->hmac_recv_ctx);
    }

    if (text->layer & BIT_CONFIDENTIALITY) {
	EVP_CIPHER_CTX_free(text->cipher_enc_ctx);
	EVP_CIPHER_CTX_free(text->cipher_dec_ctx);
    }
}
    

/*
 * Dispose of a SRP context (could be server or client)
 */ 
static void srp_common_mech_dispose(void *conn_context,
				    const sasl_utils_t *utils)
{
    context_t *text = (context_t *) conn_context;
    
    if (!text) return;
    
    BN_clear_free(text->N);
    BN_clear_free(text->g);
    BN_clear_free(text->v);
    BN_clear_free(text->b);
    BN_clear_free(text->B);
    BN_clear_free(text->a);
    BN_clear_free(text->A);
    
    if (text->authid)		utils->free(text->authid);
    if (text->userid)		utils->free(text->userid);
    if (text->free_password)	_plug_free_secret(utils, &(text->password));
    if (text->salt)		utils->free(text->salt);
    
    if (text->client_options)	utils->free(text->client_options);
    if (text->server_options)	utils->free(text->server_options);
 
    LayerCleanup(text);
    _plug_decode_free(&text->decode_context);

    if (text->encode_buf)	utils->free(text->encode_buf);
    if (text->decode_buf)	utils->free(text->decode_buf);
    if (text->decode_pkt_buf)	utils->free(text->decode_pkt_buf);
    if (text->out_buf)		utils->free(text->out_buf);
    
    utils->free(text);
}

static void
srp_common_mech_free(void *global_context __attribute__((unused)),
		     const sasl_utils_t *utils __attribute__((unused)))
{
    /* Don't call EVP_cleanup(); here, as this might confuse the calling
       application if it also uses OpenSSL */
}


/*****************************  Server Section  *****************************/

/* A large safe prime (N = 2q+1, where q is prime)
 *
 * Use N with the most bits from our table.
 *
 * All arithmetic is done modulo N
 */
static int generate_N_and_g(BIGNUM **N, BIGNUM **g)
{
    int result;

    *N = BN_new();
    result = BN_hex2bn(N, Ng_tab[NUM_Ng-1].N);
    if (!result) return SASL_FAIL;
    
    *g = BN_new();
    BN_set_word(*g, Ng_tab[NUM_Ng-1].g);
    
    return SASL_OK;
}

static int CalculateV(context_t *text,
		      BIGNUM *N, BIGNUM *g,
		      const char *user,
		      const unsigned char *pass, unsigned passlen,
		      BIGNUM **v, char **salt, int *saltlen)
{
    BIGNUM *x = NULL;
    BN_CTX *ctx = BN_CTX_new();
    int r;
    
    /* generate <salt> */    
    *saltlen = SRP_MAXBLOCKSIZE;
    *salt = (char *)text->utils->malloc(*saltlen);
    if (!*salt) return SASL_NOMEM;
    text->utils->rand(text->utils->rpool, *salt, *saltlen);
    
    r = CalculateX(text, *salt, *saltlen, user, pass, passlen, &x);
    if (r) {
	text->utils->seterror(text->utils->conn, 0, 
			      "Error calculating 'x'");
	return r;
    }
    
    /* v = g^x % N */
    *v = BN_new();
    BN_mod_exp(*v, g, x, N, ctx);
    
    BN_CTX_free(ctx);
    BN_clear_free(x);
    
    return r;   
}

static int CalculateB(context_t *text  __attribute__((unused)),
		      BIGNUM *v, BIGNUM *N, BIGNUM *g, BIGNUM **b, BIGNUM **B)
{
    BIGNUM *v3 = BN_new();
    BN_CTX *ctx = BN_CTX_new();
    
    /* Generate b */
    GetRandBigInt(b);
	
    /* Per [SRP]: make sure b > log[g](N) -- g is always 2 */
    BN_add_word(*b, BN_num_bits(N));
	
    /* B = (3v + g^b) % N */
    BN_set_word(v3, 3);
    BN_mod_mul(v3, v3, v, N, ctx);

    *B = BN_new();
    BN_mod_exp(*B, g, *b, N, ctx);
#if OPENSSL_VERSION_NUMBER >= 0x00907000L
    BN_mod_add(*B, *B, v3, N, ctx);
#else
    BN_add(*B, *B, v3);
    BN_mod(*B, *B, N, ctx);
#endif

    BN_clear_free(v3);
    BN_CTX_free(ctx);
    
    return SASL_OK;
}
	
static int ServerCalculateK(context_t *text, BIGNUM *v,
			    BIGNUM *N, BIGNUM *A, BIGNUM *b, BIGNUM *B,
			    unsigned char *K, unsigned int *Klen)
{
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashlen;
    BIGNUM *u = BN_new();
    BIGNUM *base = BN_new();
    BIGNUM *S = BN_new();
    BN_CTX *ctx = BN_CTX_new();
    int r;
    
    /* u = H(A | B) */
    r = MakeHash(text->md, hash, &hashlen, "%m%m", A, B);
    if (r) return r;
	
    BN_bin2bn(hash, hashlen, u);
	
    /* S = (Av^u) ^ b % N */
    BN_mod_exp(base, v, u, N, ctx);
    BN_mod_mul(base, base, A, N, ctx);
    
    BN_mod_exp(S, base, b, N, ctx);
    
    /* per Tom Wu: make sure Av^u != 1 (mod N) */
    if (BN_is_one(base)) {
	SETERROR(text->utils, "Unsafe SRP value for 'Av^u'\n");
	r = SASL_BADPROT;
	goto err;
    }
    
    /* per Tom Wu: make sure Av^u != -1 (mod N) */
    BN_add_word(base, 1);
    if (BN_cmp(S, N) == 0) {
	SETERROR(text->utils, "Unsafe SRP value for 'Av^u'\n");
	r = SASL_BADPROT;
	goto err;
    }
    
    /* K = H(S) */
    r = MakeHash(text->md, K, Klen, "%m", S);
    if (r) goto err;
    
    r = SASL_OK;
    
  err:
    BN_CTX_free(ctx);
    BN_clear_free(u);
    BN_clear_free(base);
    BN_clear_free(S);
    
    return r;
}

static int ParseUserSecret(const sasl_utils_t *utils,
			   char *secret, size_t seclen,
			   char **mda, BIGNUM **v, char **salt, int *saltlen)
{
    int r;
    
    /* The secret data is stored as suggested in RFC 2945:
     *
     *  { utf8(mda) mpi(v) os(salt) }  (base64 encoded)
     */
    r = utils->decode64(secret, seclen, secret, seclen, (unsigned *) &seclen);

    if (!r)
	r = UnBuffer(utils, secret, seclen, "%s%m%o", mda, v, saltlen, salt);
    if (r) {
	utils->seterror(utils->conn, 0, 
			"Error UnBuffering user secret");
    }

    return r;
}

static int CreateServerOptions(sasl_server_params_t *sparams, char **out)
{
    srp_options_t opts;
    sasl_ssf_t limitssf, requiressf;
    layer_option_t *optlist;
    
    /* zero out options */
    memset(&opts,0,sizeof(srp_options_t));
    
    /* Add mda */
    opts.mda = server_mda->bit;

    if(sparams->props.maxbufsize == 0) {
	limitssf = 0;
	requiressf = 0;
    } else {
	if (sparams->props.max_ssf < sparams->external_ssf) {
	    limitssf = 0;
	} else {
	    limitssf = sparams->props.max_ssf - sparams->external_ssf;
	}
	if (sparams->props.min_ssf < sparams->external_ssf) {
	    requiressf = 0;
	} else {
	    requiressf = sparams->props.min_ssf - sparams->external_ssf;
	}
    }
    
    /*
     * Add integrity options
     * Can't advertise integrity w/o support for default HMAC
     */
    if (default_digest->enabled) {
	optlist = digest_options;
	while(optlist->name) {
	    if (optlist->enabled &&
		/*(requiressf <= 1) &&*/ (limitssf >= 1)) {
		opts.integrity |= optlist->bit;
	    }
	    optlist++;
	}
    }
    
    /* if we set any integrity options we can advertise replay detection */
    if (opts.integrity) {
	opts.replay_detection = 1;
    }
    
    /*
     * Add confidentiality options
     * Can't advertise confidentiality w/o support for default cipher
     */
    if (default_cipher->enabled) {
	optlist = cipher_options;
	while(optlist->name) {
	    if (optlist->enabled &&
		(requiressf <= optlist->ssf) &&
		(limitssf >= optlist->ssf)) {
		opts.confidentiality |= optlist->bit;
	    }
	    optlist++;
	}
    }
    
    /* Add mandatory options */
    if (requiressf >= 1)
	opts.mandatory = BIT_REPLAY_DETECTION | BIT_INTEGRITY;
    if (requiressf > 1)
	opts.mandatory |= BIT_CONFIDENTIALITY;
    
    /* Add maxbuffersize */
    opts.maxbufsize = SRP_MAXBUFFERSIZE;
    if (sparams->props.maxbufsize &&
	sparams->props.maxbufsize < opts.maxbufsize)
	opts.maxbufsize = sparams->props.maxbufsize;
    
    return OptionsToString(sparams->utils, &opts, out);
}

static int
srp_server_mech_new(void *glob_context __attribute__((unused)),
		    sasl_server_params_t *params,
		    const char *challenge __attribute__((unused)),
		    unsigned challen __attribute__((unused)),
		    void **conn_context)
{
    context_t *text;
    
    /* holds state are in */
    text = params->utils->malloc(sizeof(context_t));
    if (text == NULL) {
	MEMERROR(params->utils);
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(context_t));
    
    text->state = 1;
    text->utils = params->utils;
    text->md = EVP_get_digestbyname(server_mda->evp_name);
    
    *conn_context = text;
    
    return SASL_OK;
}

static int srp_server_mech_step1(context_t *text,
				 sasl_server_params_t *params,
				 const char *clientin,
				 unsigned clientinlen,
				 const char **serverout,
				 unsigned *serveroutlen,
				 sasl_out_params_t *oparams)
{
    int result;
    char *sid = NULL;
    char *cn = NULL;
    int cnlen;
    char *realm = NULL;
    char *user = NULL;
    const char *password_request[] = { "*cmusaslsecretSRP",
				       SASL_AUX_PASSWORD,
				       NULL };
    struct propval auxprop_values[3];
    
    /* Expect:
     *
     * U - authentication identity
     * I - authorization identity
     * sid - session id
     * cn - client nonce
     *
     * { utf8(U) utf8(I) utf8(sid) os(cn) }
     *
     */
    result = UnBuffer(params->utils, clientin, clientinlen,
		      "%s%s%s%o", &text->authid, &text->userid, &sid,
		      &cnlen, &cn);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error UnBuffering input in step 1");
	return result;
    }
    /* Get the realm */
    result = _plug_parseuser(params->utils, &user, &realm, params->user_realm,
			     params->serverFQDN, text->authid);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error getting realm");
	goto cleanup;
    }
    
    /* Generate N and g */
    result = generate_N_and_g(&text->N, &text->g);
    if (result) {
	params->utils->seterror(text->utils->conn, 0, 
				"Error calculating N and g");
	return result;
    }
    
    /* Get user secret */
    result = params->utils->prop_request(params->propctx, password_request);
    if (result != SASL_OK) goto cleanup;
    
    /* this will trigger the getting of the aux properties */
    result = params->canon_user(params->utils->conn,
				text->authid, 0, SASL_CU_AUTHID, oparams);
    if (result != SASL_OK) goto cleanup;
    
    result = params->canon_user(params->utils->conn,
				text->userid, 0, SASL_CU_AUTHZID, oparams);
    if (result != SASL_OK) goto cleanup;
    
    result = params->utils->prop_getnames(params->propctx, password_request,
					  auxprop_values);
    if (result < 0 ||
	((!auxprop_values[0].name || !auxprop_values[0].values) &&
	 (!auxprop_values[1].name || !auxprop_values[1].values))) {
	/* We didn't find this username */
	params->utils->seterror(params->utils->conn,0,
				"no secret in database");
	result = params->transition ? SASL_TRANS : SASL_NOUSER;
	goto cleanup;
    }
    
    if (auxprop_values[0].name && auxprop_values[0].values) {
	char *mda = NULL;
	
	/* We have a precomputed verifier */
	result = ParseUserSecret(params->utils,
				 (char*) auxprop_values[0].values[0],
				 auxprop_values[0].valsize,
				 &mda, &text->v, &text->salt, &text->saltlen);
	
	if (result) {
	    /* ParseUserSecret sets error, if any */
	    if (mda) params->utils->free(mda);
	    goto cleanup;
	}
	
	/* find mda */
	server_mda = digest_options;
	while (server_mda->name) {
	    if (!strcasecmp(server_mda->name, mda))
		break;
	    
	    server_mda++;
	}
	
	if (!server_mda->name) {
	    params->utils->seterror(params->utils->conn, 0,
				    "unknown SRP mda '%s'", mda);
	    params->utils->free(mda);
	    result = SASL_FAIL;
	    goto cleanup;
	}
	params->utils->free(mda);
	
    } else if (auxprop_values[1].name && auxprop_values[1].values) {
	/* We only have the password -- calculate the verifier */
	int len = strlen(auxprop_values[1].values[0]);

	if (len == 0) {
	    params->utils->seterror(params->utils->conn,0,
				    "empty secret");
	    result = SASL_FAIL;
	    goto cleanup;
	}
	
	result = CalculateV(text, text->N, text->g, text->authid,
			    (unsigned char *) auxprop_values[1].values[0], len,
			    &text->v, &text->salt, &text->saltlen);
	if (result) {
	    params->utils->seterror(params->utils->conn, 0, 
				    "Error calculating v");
	    goto cleanup;
	}
    } else {
	params->utils->seterror(params->utils->conn, 0,
				"Have neither type of secret");
	result = SASL_FAIL;
	goto cleanup;
    }    
    
    /* erase the plaintext password */
    params->utils->prop_erase(params->propctx, password_request[1]);
    
    /* Calculate B */
    result = CalculateB(text, text->v, text->N, text->g, &text->b, &text->B);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error calculating B");
	return result;
    }

    /* Create L */
    result = CreateServerOptions(params, &text->server_options);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error creating server options");
	goto cleanup;
    }
    
    /* Send out:
     *
     * N - safe prime modulus
     * g - generator
     * s - salt
     * B - server's public key
     * L - server options (available layers etc)
     *
     * { 0x00 mpi(N) mpi(g) os(s) mpi(B) utf8(L) }
     *
     */
    result = MakeBuffer(text->utils, &text->out_buf, &text->out_buf_len,
			serveroutlen, "%c%m%m%o%m%s",
			0x00, text->N, text->g, text->saltlen, text->salt,
			text->B, text->server_options);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error creating SRP buffer from data in step 1");
	goto cleanup;
    }
    *serverout = text->out_buf;
    
    text->state = 2;
    result = SASL_CONTINUE;
    
  cleanup:
    if (sid) params->utils->free(sid);
    if (cn) params->utils->free(cn);
    if (user) params->utils->free(user);
    if (realm) params->utils->free(realm);
    
    return result;
}

static int srp_server_mech_step2(context_t *text,
			sasl_server_params_t *params,
			const char *clientin,
			unsigned clientinlen,
			const char **serverout,
			unsigned *serveroutlen,
			sasl_out_params_t *oparams)
{
    int result;    
    unsigned char *M1 = NULL, *cIV = NULL; /* don't free */
    unsigned int M1len, cIVlen;
    srp_options_t client_opts;
    unsigned char myM1[EVP_MAX_MD_SIZE];
    unsigned int myM1len;
    unsigned int i;
    unsigned char M2[EVP_MAX_MD_SIZE];
    unsigned int M2len;
    unsigned char sIV[SRP_MAXBLOCKSIZE];
    
    /* Expect:
     *
     * A - client's public key
     * M1 - client evidence
     * o - client option list
     * cIV - client's initial vector
     *
     * { mpi(A) os(M1) utf8(o) os(cIV) }
     *
     */
    result = UnBuffer(params->utils, clientin, clientinlen,
		      "%m%-o%s%-o", &text->A, &M1len, &M1,
		      &text->client_options, &cIVlen, &cIV);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error UnBuffering input in step 2");
	goto cleanup;
    }
    
    /* Per [SRP]: reject A <= 0 */
    if (BigIntCmpWord(text->A, 0) <= 0) {
	SETERROR(params->utils, "Illegal value for 'A'\n");
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* parse client options */
    result = ParseOptions(params->utils, text->client_options, &client_opts, 1);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error parsing user's options");
	
	if (client_opts.confidentiality) {
	    /* Mark that we attempted confidentiality layer negotiation */
	    oparams->mech_ssf = 2;
	}
	else if (client_opts.integrity || client_opts.replay_detection) {
	    /* Mark that we attempted integrity layer negotiation */
	    oparams->mech_ssf = 1;
	}
	return result;
    }

    result = SetMDA(&client_opts, text);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error setting options");
	return result;   
    }

    /* Calculate K */
    result = ServerCalculateK(text, text->v, text->N, text->A,
			      text->b, text->B, text->K, &text->Klen);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error calculating K");
	return result;
    }
    
    /* See if M1 is correct */
    result = CalculateM1(text, text->N, text->g, text->authid,
			 text->salt, text->saltlen, text->A, text->B,
			 text->K, text->Klen, text->userid,
			 text->server_options, myM1, &myM1len);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error calculating M1");
	goto cleanup;
    }
    
    if (myM1len != M1len) {
	params->utils->seterror(params->utils->conn, 0, 
				"SRP M1 lengths do not match");
	result = SASL_BADAUTH;
	goto cleanup;
    }
    
    for (i = 0; i < myM1len; i++) {
	if (myM1[i] != M1[i]) {
	    params->utils->seterror(params->utils->conn, 0, 
				    "client evidence does not match what we "
				    "calculated. Probably a password error");
	    result = SASL_BADAUTH;
	    goto cleanup;
	}
    }
    
    /* calculate M2 to send */
    result = CalculateM2(text, text->A, M1, M1len, text->K, text->Klen,
			 text->userid, text->client_options, "", 0,
			 M2, &M2len);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error calculating M2 (server evidence)");
	goto cleanup;
    }
    
    /* Create sIV (server initial vector) */
    text->utils->rand(text->utils->rpool, (char *) sIV, sizeof(sIV));
    
    /*
     * Send out:
     * M2 - server evidence
     * sIV - server's initial vector
     * sid - session id
     * ttl - time to live
     *
     * { os(M2) os(sIV) utf8(sid) uint(ttl) }
     */
    result = MakeBuffer(text->utils, &text->out_buf, &text->out_buf_len,
			serveroutlen, "%o%o%s%u", M2len, M2,
			sizeof(sIV), sIV, "", 0);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error making output buffer in SRP step 3");
	goto cleanup;
    }
    *serverout = text->out_buf;

    /* configure security layer */
    result = LayerInit(&client_opts, text, oparams, cIV, sIV,
		       params->props.maxbufsize);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error initializing security layer");
	return result;   
    }

    /* set oparams */
    oparams->doneflag = 1;
    oparams->param_version = 0;
    
    result = SASL_OK;
    
  cleanup:
    
    return result;
}

static int srp_server_mech_step(void *conn_context,
				sasl_server_params_t *sparams,
				const char *clientin,
				unsigned clientinlen,
				const char **serverout,
				unsigned *serveroutlen,
				sasl_out_params_t *oparams)
{
    context_t *text = (context_t *) conn_context;
    
    if (!sparams
	|| !serverout
	|| !serveroutlen
	|| !oparams)
	return SASL_BADPARAM;
    
    *serverout = NULL;
    *serveroutlen = 0;

    if (text == NULL) {
	return SASL_BADPROT;
    }

    sparams->utils->log(NULL, SASL_LOG_DEBUG,
			"SRP server step %d\n", text->state);

    switch (text->state) {

    case 1:
	return srp_server_mech_step1(text, sparams, clientin, clientinlen,
				     serverout, serveroutlen, oparams);

    case 2:
	return srp_server_mech_step2(text, sparams, clientin, clientinlen,
				     serverout, serveroutlen, oparams);

    default:
	sparams->utils->seterror(sparams->utils->conn, 0,
				 "Invalid SRP server step %d", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

#ifdef DO_SRP_SETPASS
static int srp_setpass(void *glob_context __attribute__((unused)),
		       sasl_server_params_t *sparams,
		       const char *userstr,
		       const char *pass,
		       unsigned passlen __attribute__((unused)),
		       const char *oldpass __attribute__((unused)),
		       unsigned oldpasslen __attribute__((unused)),
		       unsigned flags)
{
    int r;
    char *user = NULL;
    char *user_only = NULL;
    char *realm = NULL;
    sasl_secret_t *sec = NULL;
    struct propctx *propctx = NULL;
    const char *store_request[] = { "cmusaslsecretSRP",
				     NULL };
    
    /* Do we have a backend that can store properties? */
    if (!sparams->utils->auxprop_store ||
	sparams->utils->auxprop_store(NULL, NULL, NULL) != SASL_OK) {
	SETERROR(sparams->utils, "SRP: auxprop backend can't store properties");
	return SASL_NOMECH;
    }
    
    /* NB: Ideally we need to canonicalize userstr here */
    r = _plug_parseuser(sparams->utils, &user_only, &realm, sparams->user_realm,
			sparams->serverFQDN, userstr);

    if (r) {
	sparams->utils->seterror(sparams->utils->conn, 0, 
				 "Error parsing user");
	return r;
    }

    r = _plug_make_fulluser(sparams->utils, &user, user_only, realm);

    if (r) {
	goto cleanup;
    }

    if ((flags & SASL_SET_DISABLE) || pass == NULL) {
	sec = NULL;
    } else {
	context_t *text = NULL;
	BIGNUM *N = NULL;
	BIGNUM *g = NULL;
	BIGNUM *v = NULL;
	char *salt;
	int saltlen;
	char *buffer = NULL;
	unsigned int bufferlen, alloclen, encodelen;
	
	text = sparams->utils->malloc(sizeof(context_t));
	if (text == NULL) {
	    MEMERROR(sparams->utils);
	    return SASL_NOMEM;
	}
	
	memset(text, 0, sizeof(context_t));
	
	text->utils = sparams->utils;
	text->md = EVP_get_digestbyname(server_mda->evp_name);
	
	r = generate_N_and_g(&N, &g);
	if (r) {
	    sparams->utils->seterror(sparams->utils->conn, 0, 
				     "Error calculating N and g");
	    goto end;
	}

	/* user is a full username here */
	r = CalculateV(text, N, g, user,
                       (unsigned char *) pass, passlen, &v, &salt, &saltlen);
	if (r) {
	    sparams->utils->seterror(sparams->utils->conn, 0, 
				     "Error calculating v");
	    goto end;
	}
	
	/* The secret data is stored as suggested in RFC 2945:
	 *
	 *  { utf8(mda) mpi(v) os(salt) }  (base64 encoded)
	 */
	
	r = MakeBuffer(text->utils, &text->out_buf, &text->out_buf_len,
		       &bufferlen, "%s%m%o",
		       server_mda->name, &v, saltlen, salt);
	
	if (r) {
	    sparams->utils->seterror(sparams->utils->conn, 0, 
				     "Error making buffer for secret");
	    goto end;
	}
	buffer = text->out_buf;
	
	/* Put 'buffer' into sasl_secret_t.
	 * This will be base64 encoded, so make sure its big enough.
	 */
	alloclen = (bufferlen/3 + 1) * 4 + 1;
	sec = sparams->utils->malloc(sizeof(sasl_secret_t)+alloclen);
	if (!sec) {
	    r = SASL_NOMEM;
	    goto end;
	}
	sparams->utils->encode64(buffer, bufferlen, (char *) sec->data, alloclen,
				 &encodelen);
	sec->len = encodelen;
	
	/* Clean everything up */
      end:
	if (buffer) sparams->utils->free((void *) buffer);
	BN_clear_free(N);
	BN_clear_free(g);
	BN_clear_free(v);
	sparams->utils->free(text);
	
	if (r) return r;
    }
    
    /* do the store */
    propctx = sparams->utils->prop_new(0);
    if (!propctx)
	r = SASL_FAIL;
    if (!r)
	r = sparams->utils->prop_request(propctx, store_request);
    if (!r)
	r = sparams->utils->prop_set(propctx, "cmusaslsecretSRP",
				     (char *) (sec ? sec->data : NULL),
				     (sec ? sec->len : 0));
    if (!r)
	r = sparams->utils->auxprop_store(sparams->utils->conn, propctx, user);
    if (propctx)
	sparams->utils->prop_dispose(&propctx);
    
    if (r) {
	sparams->utils->seterror(sparams->utils->conn, 0, 
				 "Error putting SRP secret");
	goto cleanup;
    }
    
    sparams->utils->log(NULL, SASL_LOG_DEBUG, "Setpass for SRP successful\n");
    
  cleanup:
    
    if (user) 	_plug_free_string(sparams->utils, &user);
    if (user_only) 	_plug_free_string(sparams->utils, &user_only);
    if (realm) 	_plug_free_string(sparams->utils, &realm);
    if (sec)    _plug_free_secret(sparams->utils, &sec);
    
    return r;
}
#endif /* DO_SRP_SETPASS */

static int srp_mech_avail(void *glob_context __attribute__((unused)),
			  sasl_server_params_t *sparams,
			  void **conn_context __attribute__((unused))) 
{
    /* Do we have access to the selected MDA? */
    if (!server_mda || !server_mda->enabled) {
	SETERROR(sparams->utils,
		 "SRP unavailable due to selected MDA unavailable");
	return SASL_NOMECH;
    }
    
    return SASL_OK;
}

static sasl_server_plug_t srp_server_plugins[] = 
{
    {
	"SRP",				/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NODICTIONARY
	| SASL_SEC_FORWARD_SECRECY
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	NULL,				/* glob_context */
	&srp_server_mech_new,		/* mech_new */
	&srp_server_mech_step,		/* mech_step */
	&srp_common_mech_dispose,	/* mech_dispose */
	&srp_common_mech_free,		/* mech_free */
#ifdef DO_SRP_SETPASS
	&srp_setpass,			/* setpass */
#else
	NULL,
#endif
	NULL,				/* user_query */
	NULL,				/* idle */
	&srp_mech_avail,		/* mech avail */
	NULL				/* spare */
    }
};

int srp_server_plug_init(const sasl_utils_t *utils,
			 int maxversion,
			 int *out_version,
			 const sasl_server_plug_t **pluglist,
			 int *plugcount,
			 const char *plugname __attribute__((unused)))
{
    const char *mda;
    unsigned int len;
    layer_option_t *opts;
    
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	SETERROR(utils, "SRP version mismatch");
	return SASL_BADVERS;
    }
    
    utils->getopt(utils->getopt_context, "SRP", "srp_mda", &mda, &len);
    if (!mda) mda = DEFAULT_MDA;
    
    /* Add all digests and ciphers */
    OpenSSL_add_all_algorithms();
    
    /* See which digests we have available and set max_ssf accordingly */
    opts = digest_options;
    while (opts->name) {
	if (EVP_get_digestbyname(opts->evp_name)) {
	    opts->enabled = 1;
	    
	    srp_server_plugins[0].max_ssf = opts->ssf;
	}
	
	/* Locate the server MDA */
	if (!strcasecmp(opts->name, mda) || !strcasecmp(opts->evp_name, mda)) {
	    server_mda = opts;
	}
	
	opts++;
    }
    
    /* See which ciphers we have available and set max_ssf accordingly */
    opts = cipher_options;
    while (opts->name) {
	if (EVP_get_cipherbyname(opts->evp_name)) {
	    opts->enabled = 1;
	    
	    if (opts->ssf > srp_server_plugins[0].max_ssf) {
		srp_server_plugins[0].max_ssf = opts->ssf;
	    }
	}
	
	opts++;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = srp_server_plugins;
    *plugcount = 1;
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

/* Check to see if N,g is in the recommended list */
static int check_N_and_g(const sasl_utils_t *utils, BIGNUM *N, BIGNUM *g)
{
    char *N_prime;
    unsigned long g_prime;
    unsigned i;
    int r = SASL_FAIL;
    
    N_prime = BN_bn2hex(N);
    g_prime = BN_get_word(g);
    
    for (i = 0; i < NUM_Ng; i++) {
	if (!strcasecmp(N_prime, Ng_tab[i].N) && (g_prime == Ng_tab[i].g)) {
	    r = SASL_OK;
	    break;
	}
    }
    
    if (N_prime) utils->free(N_prime);
    
    return r;
}

static int CalculateA(context_t *text  __attribute__((unused)),
		      BIGNUM *N, BIGNUM *g, BIGNUM **a, BIGNUM **A)
{
    BN_CTX *ctx = BN_CTX_new();
    
    /* Generate a */
    GetRandBigInt(a);
	
    /* Per [SRP]: make sure a > log[g](N) -- g is always 2 */
    BN_add_word(*a, BN_num_bits(N));
	
    /* A = g^a % N */
    *A = BN_new();
    BN_mod_exp(*A, g, *a, N, ctx);

    BN_CTX_free(ctx);
    
    return SASL_OK;
}
	
static int ClientCalculateK(context_t *text, char *salt, int saltlen,
			    char *user, unsigned char *pass, int passlen,
			    BIGNUM *N, BIGNUM *g, BIGNUM *a, BIGNUM *A,
			    BIGNUM *B, unsigned char *K, unsigned int *Klen)
{
    int r;
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashlen;
    BIGNUM *x = NULL;
    BIGNUM *u = BN_new();
    BIGNUM *aux = BN_new();
    BIGNUM *gx = BN_new();
    BIGNUM *gx3 = BN_new();
    BIGNUM *base = BN_new();
    BIGNUM *S = BN_new();
    BN_CTX *ctx = BN_CTX_new();
    
    /* u = H(A | B) */
    r = MakeHash(text->md, hash, &hashlen, "%m%m", A, B);
    if (r) goto err;
    u = BN_new();
    BN_bin2bn(hash, hashlen, u);
    
    /* per Tom Wu: make sure u != 0 */
    if (BN_is_zero(u)) {
	SETERROR(text->utils, "SRP: Illegal value for 'u'\n");
	r = SASL_BADPROT;
	goto err;
    }
    
    /* S = (B - 3(g^x)) ^ (a + ux) % N */

    r = CalculateX(text, salt, saltlen, user, pass, passlen, &x);
    if (r) return r;
    
    /* a + ux */
    BN_mul(aux, u, x, ctx);
    BN_add(aux, aux, a);
    
    /* gx3 = 3(g^x) % N */
    BN_mod_exp(gx, g, x, N, ctx);
    BN_set_word(gx3, 3);
    BN_mod_mul(gx3, gx3, gx, N, ctx);
    
    /* base = (B - 3(g^x)) % N */
#if OPENSSL_VERSION_NUMBER >= 0x00907000L
    BN_mod_sub(base, B, gx3, N, ctx);
#else
    BN_sub(base, B, gx3);
    BN_mod(base, base, N, ctx);
    if (BigIntCmpWord(base, 0) < 0) {
	BN_add(base, base, N);
    }
#endif
    
    /* S = base^aux % N */
    BN_mod_exp(S, base, aux, N, ctx);
    
    /* K = H(S) */
    r = MakeHash(text->md, K, Klen, "%m", S);
    if (r) goto err;
    
    r = SASL_OK;
    
  err:
    BN_CTX_free(ctx);
    BN_clear_free(x);
    BN_clear_free(u);
    BN_clear_free(aux);
    BN_clear_free(gx);
    BN_clear_free(gx3);
    BN_clear_free(base);
    BN_clear_free(S);
    
    return r;
}

static int CreateClientOpts(sasl_client_params_t *params, 
			    srp_options_t *available, 
			    srp_options_t *out)
{
    layer_option_t *opt;
    sasl_ssf_t external;
    sasl_ssf_t limit;
    sasl_ssf_t musthave;
    
    /* zero out output */
    memset(out, 0, sizeof(srp_options_t));
    
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "Available MDA = %d\n", available->mda);
    
    /* mda */
    opt = FindBest(available->mda, 0, 256, digest_options);
    
    if (opt) {
	out->mda = opt->bit;
    }
    else {
	SETERROR(params->utils, "Can't find an acceptable SRP MDA\n");
	return SASL_BADAUTH;
    }
    
    /* get requested ssf */
    external = params->external_ssf;
    
    /* what do we _need_?  how much is too much? */
    if(params->props.maxbufsize == 0) {
	musthave = 0;
	limit = 0;
    } else {
	if (params->props.max_ssf > external) {
	    limit = params->props.max_ssf - external;
	} else {
	    limit = 0;
	}
	if (params->props.min_ssf > external) {
	    musthave = params->props.min_ssf - external;
	} else {
	    musthave = 0;
	}
    }
        
    /* we now go searching for an option that gives us at least "musthave"
       and at most "limit" bits of ssf. */
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "Available confidentiality = %d  "
		       "musthave = %d  limit = %d",
		       available->confidentiality, musthave, limit);
    
    /* confidentiality */
    if (limit > 1) {
	
	opt = FindBest(available->confidentiality, musthave, limit,
		       cipher_options);
	
	if (opt) {
	    out->confidentiality = opt->bit;
	    /* we've already satisfied the SSF with the confidentiality
	     * layer, but we'll also use an integrity layer if we can
	     */
	    musthave = 0;
	}
	else if (musthave > 1) {
	    SETERROR(params->utils,
		     "Can't find an acceptable SRP confidentiality layer\n");
	    return SASL_TOOWEAK;
	}
    }
    
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "Available integrity = %d "
		       "musthave = %d  limit = %d",
		       available->integrity, musthave, limit);
    
    /* integrity */
    if ((limit >= 1) && (musthave <= 1)) {
	
	opt = FindBest(available->integrity, musthave, limit,
		       digest_options);
	
	if (opt) {
	    out->integrity = opt->bit;
	    
	    /* if we set an integrity option we can set replay detection */
	    out->replay_detection = available->replay_detection;
	}
	else if (musthave > 0) {
	    SETERROR(params->utils,
		     "Can't find an acceptable SRP integrity layer\n");
	    return SASL_TOOWEAK;
	}
    }
    
    /* Check to see if we've satisfied all of the servers mandatory layers */
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "Mandatory layers = %d\n",available->mandatory);
    
    if ((!out->replay_detection &&
	 (available->mandatory & BIT_REPLAY_DETECTION)) ||
	(!out->integrity &&
	 (available->mandatory & BIT_INTEGRITY)) ||
	(!out->confidentiality &&
	 (available->mandatory & BIT_CONFIDENTIALITY))) {
	SETERROR(params->utils, "Mandatory SRP layer not supported\n");
	return SASL_BADAUTH;
    }
    
    /* Add maxbuffersize */
    out->maxbufsize = SRP_MAXBUFFERSIZE;
    if (params->props.maxbufsize && params->props.maxbufsize < out->maxbufsize)
	out->maxbufsize = params->props.maxbufsize;
    
    return SASL_OK;
}

static int srp_client_mech_new(void *glob_context __attribute__((unused)),
			       sasl_client_params_t *params,
			       void **conn_context)
{
    context_t *text;
    
    /* holds state are in */
    text = params->utils->malloc(sizeof(context_t));
    if (text == NULL) {
	MEMERROR( params->utils );
	return SASL_NOMEM;
    }
    
    memset(text, 0, sizeof(context_t));

    text->state = 1;
    text->utils = params->utils;

    *conn_context = text;
    
    return SASL_OK;
}

static int
srp_client_mech_step1(context_t *text,
		      sasl_client_params_t *params,
		      const char *serverin __attribute__((unused)),
		      unsigned serverinlen,
		      sasl_interact_t **prompt_need,
		      const char **clientout,
		      unsigned *clientoutlen,
		      sasl_out_params_t *oparams)
{
    const char *authid = NULL, *userid = NULL;
    int auth_result = SASL_OK;
    int pass_result = SASL_OK;
    int user_result = SASL_OK;
    int result;
    
    /* Expect: 
     *   absolutely nothing
     * 
     */
    if (serverinlen > 0) {
	SETERROR(params->utils, "Invalid input to first step of SRP\n");
	return SASL_BADPROT;
    }
    
    /* try to get the authid */
    if (oparams->authid==NULL) {
	auth_result = _plug_get_authid(params->utils, &authid, prompt_need);
	
	if ((auth_result != SASL_OK) && (auth_result != SASL_INTERACT))
	    return auth_result;
    }
    
    /* try to get the userid */
    if (oparams->user == NULL) {
	user_result = _plug_get_userid(params->utils, &userid, prompt_need);
	
	if ((user_result != SASL_OK) && (user_result != SASL_INTERACT))
	    return user_result;
    }
    
    /* try to get the password */
    if (text->password == NULL) {
	pass_result=_plug_get_password(params->utils, &text->password,
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
    if ((auth_result == SASL_INTERACT) || (user_result == SASL_INTERACT) ||
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
			       "Please enter your password" : NULL, NULL,
			       NULL, NULL, NULL,
			       NULL, NULL, NULL);
	if (result != SASL_OK) return result;
	    
	return SASL_INTERACT;
    }
    
    if (!userid || !*userid) {
	result = params->canon_user(params->utils->conn, authid, 0,
				    SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
    }
    else {
	result = params->canon_user(params->utils->conn, authid, 0,
				    SASL_CU_AUTHID, oparams);
	if (result != SASL_OK) return result;

	result = params->canon_user(params->utils->conn, userid, 0,
				    SASL_CU_AUTHZID, oparams);
    }
    if (result != SASL_OK) return result;
    
    /* Send out:
     *
     * U - authentication identity 
     * I - authorization identity
     * sid - previous session id
     * cn - client nonce
     *
     * { utf8(U) utf8(I) utf8(sid) os(cn) }
     */
    result = MakeBuffer(text->utils, &text->out_buf, &text->out_buf_len,
			clientoutlen, "%s%s%s%o",
			(char *) oparams->authid, (char *) oparams->user,
			"", 0, "");
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR, "Error making output buffer\n");
	goto cleanup;
    }
    *clientout = text->out_buf;
    
    text->state = 2;

    result = SASL_CONTINUE;
    
  cleanup:
    
    return result;
}

static int
srp_client_mech_step2(context_t *text,
		      sasl_client_params_t *params,
		      const char *serverin,
		      unsigned serverinlen,
		      sasl_interact_t **prompt_need __attribute__((unused)),
		      const char **clientout,
		      unsigned *clientoutlen,
		      sasl_out_params_t *oparams)
{
    int result;
    char reuse;
    srp_options_t server_opts;
    
    /* Expect:
     *
     *  { 0x00 mpi(N) mpi(g) os(s) mpi(B) utf8(L) }
     */
    result = UnBuffer(params->utils, serverin, serverinlen,
		      "%c%m%m%o%m%s", &reuse, &text->N, &text->g,
		      &text->saltlen, &text->salt, &text->B,
		      &text->server_options);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error UnBuffering input in step 2");
	goto cleanup;
    }

    /* Check N and g to see if they are one of the recommended pairs */
    result = check_N_and_g(params->utils, text->N, text->g);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Values of 'N' and 'g' are not recommended\n");
	goto cleanup;
    }
    
    /* Per [SRP]: reject B <= 0, B >= N */
    if (BigIntCmpWord(text->B, 0) <= 0 || BN_cmp(text->B, text->N) >= 0) {
	SETERROR(params->utils, "Illegal value for 'B'\n");
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* parse server options */
    memset(&server_opts, 0, sizeof(srp_options_t));
    result = ParseOptions(params->utils, text->server_options, &server_opts, 0);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Error parsing SRP server options\n");
	goto cleanup;
    }
    
    /* Create o */
    result = CreateClientOpts(params, &server_opts, &text->client_opts);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Error creating client options\n");
	goto cleanup;
    }
    
    result = OptionsToString(params->utils, &text->client_opts,
			     &text->client_options);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Error converting client options to an option string\n");
	goto cleanup;
    }

    result = SetMDA(&text->client_opts, text);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error setting MDA");
	goto cleanup;
    }

    /* Calculate A */
    result = CalculateA(text, text->N, text->g, &text->a, &text->A);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error calculating A");
	return result;
    }
    
    /* Calculate shared context key K */
    result = ClientCalculateK(text, text->salt, text->saltlen,
			      (char *) oparams->authid, 
			      text->password->data, text->password->len,
			      text->N, text->g, text->a, text->A, text->B,
			      text->K, &text->Klen);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Error creating K\n");
	goto cleanup;
    }
    
    /* Calculate M1 (client evidence) */
    result = CalculateM1(text, text->N, text->g, (char *) oparams->authid,
			 text->salt, text->saltlen, text->A, text->B,
			 text->K, text->Klen, (char *) oparams->user,
			 text->server_options, text->M1, &text->M1len);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Error creating M1\n");
	goto cleanup;
    }

    /* Create cIV (client initial vector) */
    text->utils->rand(text->utils->rpool, (char *) text->cIV, sizeof(text->cIV));
    
    /* Send out:
     *
     * A - client's public key
     * M1 - client evidence
     * o - client option list
     * cIV - client initial vector
     *
     * { mpi(A) os(M1) utf8(o) os(cIV) }
     */
    result = MakeBuffer(text->utils, &text->out_buf, &text->out_buf_len,
			clientoutlen, "%m%o%s%o",
			text->A, text->M1len, text->M1, text->client_options,
			sizeof(text->cIV), text->cIV);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR, "Error making output buffer\n");
	goto cleanup;
    }
    *clientout = text->out_buf;
    
    text->state = 3;

    result = SASL_CONTINUE;
    
  cleanup:
    
    return result;
}

static int
srp_client_mech_step3(context_t *text,
		      sasl_client_params_t *params,
		      const char *serverin,
		      unsigned serverinlen,
		      sasl_interact_t **prompt_need __attribute__((unused)),
		      const char **clientout __attribute__((unused)),
		      unsigned *clientoutlen __attribute__((unused)),
		      sasl_out_params_t *oparams)
{
    int result;    
    unsigned char *M2 = NULL, *sIV = NULL; /* don't free */
    char *sid = NULL;
    unsigned int M2len, sIVlen;
    uint32 ttl;
    unsigned int i;
    unsigned char myM2[EVP_MAX_MD_SIZE];
    unsigned int myM2len;
    
    /* Expect:
     *
     * M2 - server evidence
     * sIV - server initial vector
     * sid - session id
     * ttl - time to live
     *
     *   { os(M2) os(sIV) utf8(sid) uint(ttl) }
     */
    result = UnBuffer(params->utils, serverin, serverinlen,
		      "%-o%-o%s%u", &M2len, &M2, &sIVlen, &sIV,
		      &sid, &ttl);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error UnBuffering input in step 3");
	goto cleanup;
    }

    /* calculate our own M2 */
    result = CalculateM2(text, text->A, text->M1, text->M1len,
			 text->K, text->Klen, (char *) oparams->user,
			 text->client_options, "", 0,
			 myM2, &myM2len);
    if (result) {
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Error calculating our own M2 (server evidence)\n");
	goto cleanup;
    }
    
    /* compare to see if is server spoof */
    if (myM2len != M2len) {
	SETERROR(params->utils, "SRP Server M2 length wrong\n");
	result = SASL_BADSERV;
	goto cleanup;
    }
    
    
    for (i = 0; i < myM2len; i++) {
	if (M2[i] != myM2[i]) {
	    SETERROR(params->utils,
		     "SRP Server spoof detected. M2 incorrect\n");
	    result = SASL_BADSERV;
	    goto cleanup;
	}
    }
    
    /*
     * Send out: nothing
     */

    /* configure security layer */
    result = LayerInit(&text->client_opts, text, oparams, sIV, text->cIV,
		       params->props.maxbufsize);
    if (result) {
	params->utils->seterror(params->utils->conn, 0, 
				"Error initializing security layer");
	return result;   
    }

    /* set oparams */
    oparams->doneflag = 1;
    oparams->param_version = 0;

    result = SASL_OK;
    
  cleanup:
    if (sid) params->utils->free(sid);
    
    return result;
}

static int srp_client_mech_step(void *conn_context,
				sasl_client_params_t *params,
				const char *serverin,
				unsigned serverinlen,
				sasl_interact_t **prompt_need,
				const char **clientout,
				unsigned *clientoutlen,
				sasl_out_params_t *oparams)
{
    context_t *text = (context_t *) conn_context;
    
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "SRP client step %d\n", text->state);
    
    *clientout = NULL;
    *clientoutlen = 0;
    
    switch (text->state) {

    case 1:
	return srp_client_mech_step1(text, params, serverin, serverinlen, 
				     prompt_need, clientout, clientoutlen,
				     oparams);

    case 2:
	return srp_client_mech_step2(text, params, serverin, serverinlen, 
				     prompt_need, clientout, clientoutlen,
				     oparams);

    case 3:
	return srp_client_mech_step3(text, params, serverin, serverinlen, 
				     prompt_need, clientout, clientoutlen,
				     oparams);

    default:
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid SRP client step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}


static sasl_client_plug_t srp_client_plugins[] = 
{
    {
	"SRP",				/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NODICTIONARY
	| SASL_SEC_FORWARD_SECRECY
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_ALLOWS_PROXY,	/* features */
	NULL,				/* required_prompts */
	NULL,				/* glob_context */
	&srp_client_mech_new,		/* mech_new */
	&srp_client_mech_step,		/* mech_step */
	&srp_common_mech_dispose,	/* mech_dispose */
	&srp_common_mech_free,		/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int srp_client_plug_init(const sasl_utils_t *utils __attribute__((unused)),
			 int maxversion,
			 int *out_version,
			 const sasl_client_plug_t **pluglist,
			 int *plugcount,
			 const char *plugname __attribute__((unused)))
{
    layer_option_t *opts;
    
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR(utils, "SRP version mismatch");
	return SASL_BADVERS;
    }
    
    /* Add all digests and ciphers */
    OpenSSL_add_all_algorithms();
    
    /* See which digests we have available and set max_ssf accordingly */
    opts = digest_options;
    while (opts->name) {
	if (EVP_get_digestbyname(opts->evp_name)) {
	    opts->enabled = 1;
	    
	    srp_client_plugins[0].max_ssf = opts->ssf;
	}
	
	opts++;
    }
    
    /* See which ciphers we have available and set max_ssf accordingly */
    opts = cipher_options;
    while (opts->name) {
	if (EVP_get_cipherbyname(opts->evp_name)) {
	    opts->enabled = 1;
	    
	    if (opts->ssf > srp_client_plugins[0].max_ssf) {
		srp_client_plugins[0].max_ssf = opts->ssf;
	    }
	}
	
	opts++;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = srp_client_plugins;
    *plugcount=1;
    
    return SASL_OK;
}
