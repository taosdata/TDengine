/* NTLM SASL plugin
 * Ken Murchison
 *
 * References:
 *   http://www.innovation.ch/java/ntlm.html
 *   http://www.opengroup.org/comsource/techref2/NCH1222X.HTM
 *   http://www.ubiqx.org/cifs/rfc-draft/draft-leach-cifs-v1-spec-02.html
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
#include <ctype.h>
#include <errno.h>
#include <limits.h>

#ifdef WIN32
# include <process.h>	    /* for getpid */
  typedef int pid_t;
#else
# include <unistd.h>
# include <sys/types.h>
# include <sys/socket.h>
# include <sys/utsname.h>
# include <netdb.h>

#ifndef SYS_NMLN
  struct utsname dummy;
# define SYS_NMLN sizeof(dummy.sysname)
#endif

# define closesocket(sock)   close(sock)
  typedef int SOCKET;
#endif /* WIN32 */

#ifndef sasl_getpid /* for some reason VS doesn't like #define getpid */
# define sasl_getpid getpid
#endif

#include <openssl/md4.h>
#ifdef OPENSSL_NO_MD4
#error No MD4 support in OpenSSL
#endif
#include <openssl/md5.h>
#include <openssl/hmac.h>
#include <openssl/des.h>
#include <openssl/opensslv.h>
#if (OPENSSL_VERSION_NUMBER >= 0x0090700f) && \
     !defined(OPENSSL_ENABLE_OLD_DES_SUPPORT)
# define des_cblock DES_cblock
# define des_key_schedule DES_key_schedule
# define des_set_odd_parity(k) \
	 DES_set_odd_parity((k))
# define des_set_key(k,ks) \
	 DES_set_key((k),&(ks))
# define des_key_sched(k,ks) \
         DES_key_sched((k),&(ks))
# define des_ecb_encrypt(i,o,k,e) \
	 DES_ecb_encrypt((i),(o),&(k),(e))
#endif /* OpenSSL 0.9.7+ w/o old DES support */

/* for legacy libcrypto support */
#include "crypto-compat.h"

#include <sasl.h>
#define MD5_H  /* suppress internal MD5 */
#include <saslplug.h>

#include "plugin_common.h"

/*****************************  Common Section  *****************************/

#ifdef WIN32
static ssize_t writev (SOCKET fd, const struct iovec *iov, size_t iovcnt);

ssize_t writev (SOCKET fd, const struct iovec *iov, size_t iovcnt)
{
    ssize_t nwritten;		/* amount written */
    ssize_t nbytes;
    size_t i;
  
    nbytes = 0;

    for (i = 0; i < iovcnt; i++) {
	if ((nwritten = send (fd, iov[i].iov_base, iov[i].iov_len, 0)) == SOCKET_ERROR) {
/* Unless socket is nonblocking, we should always write everything */
	    return (-1);
	}

	nbytes += nwritten;
	  
	if (nwritten < iov[i].iov_len) {
	    break;
	}
    }
    return (nbytes);
}
#endif /* WIN32 */

#ifndef UINT16_MAX
#define UINT16_MAX 65535U
#endif

#if UINT_MAX == UINT16_MAX
typedef unsigned int uint16;
#elif USHRT_MAX == UINT16_MAX
typedef unsigned short uint16;
#else
#error dont know what to use for uint16
#endif

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

#define NTLM_SIGNATURE		"NTLMSSP"

enum {
    NTLM_TYPE_REQUEST		= 1,
    NTLM_TYPE_CHALLENGE		= 2,
    NTLM_TYPE_RESPONSE		= 3
};

enum {
    NTLM_USE_UNICODE		= 0x00001,
    NTLM_USE_ASCII		= 0x00002,
    NTLM_ASK_TARGET		= 0x00004,
    NTLM_AUTH_NTLM		= 0x00200,
    NTLM_ALWAYS_SIGN		= 0x08000,
    NTLM_TARGET_IS_DOMAIN	= 0x10000,
    NTLM_TARGET_IS_SERVER	= 0x20000,
    NTLM_FLAGS_MASK		= 0x0ffff
};

enum {
    NTLM_NONCE_LENGTH		= 8,
    NTLM_HASH_LENGTH		= 21,
    NTLM_RESP_LENGTH		= 24,
    NTLM_SESSKEY_LENGTH		= 16,
};

enum {
    NTLM_SIG_OFFSET		= 0,
    NTLM_TYPE_OFFSET		= 8,

    NTLM_TYPE1_FLAGS_OFFSET	= 12,
    NTLM_TYPE1_DOMAIN_OFFSET	= 16,
    NTLM_TYPE1_WORKSTN_OFFSET	= 24,
    NTLM_TYPE1_DATA_OFFSET	= 32,
    NTLM_TYPE1_MINSIZE		= 16,

    NTLM_TYPE2_TARGET_OFFSET	= 12,
    NTLM_TYPE2_FLAGS_OFFSET	= 20,
    NTLM_TYPE2_CHALLENGE_OFFSET	= 24,
    NTLM_TYPE2_CONTEXT_OFFSET	= 32,
    NTLM_TYPE2_TARGETINFO_OFFSET= 40,
    NTLM_TYPE2_DATA_OFFSET	= 48,
    NTLM_TYPE2_MINSIZE		= 32,

    NTLM_TYPE3_LMRESP_OFFSET	= 12,
    NTLM_TYPE3_NTRESP_OFFSET	= 20,
    NTLM_TYPE3_DOMAIN_OFFSET	= 28,
    NTLM_TYPE3_USER_OFFSET	= 36,
    NTLM_TYPE3_WORKSTN_OFFSET	= 44,
    NTLM_TYPE3_SESSIONKEY_OFFSET= 52,
    NTLM_TYPE3_FLAGS_OFFSET	= 60,
    NTLM_TYPE3_DATA_OFFSET	= 64,
    NTLM_TYPE3_MINSIZE		= 52,

    NTLM_BUFFER_LEN_OFFSET	= 0,
    NTLM_BUFFER_MAXLEN_OFFSET	= 2,
    NTLM_BUFFER_OFFSET_OFFSET	= 4,
    NTLM_BUFFER_SIZE		= 8
};

/* return the length of a string (even if it is NULL) */
#define xstrlen(s) (s ? strlen(s) : 0)

/* machine-independent routines to convert to/from Intel byte-order */
#define htois(is, hs) \
    (is)[0] = hs & 0xff; \
    (is)[1] = hs >> 8

#define itohs(is) \
    ((is)[0] | ((is)[1] << 8))

#define htoil(il, hl) \
    (il)[0] = hl & 0xff; \
    (il)[1] = (hl >> 8) & 0xff; \
    (il)[2] = (hl >> 16) & 0xff; \
    (il)[3] = hl >> 24

#define itohl(il) \
    ((il)[0] | ((il)[1] << 8) | ((il)[2] << 16) | ((il)[3] << 24))

/* convert string to all upper case */
static const char *ucase(const char *str, size_t len)
{
    char *cp = (char *) str;

    if (!len) len = xstrlen(str);
    
    while (len && cp && *cp) {
	*cp = toupper((int) *cp);
	cp++;
	len--;
    }

    return (str);
}

/* copy src to dst as unicode (in Intel byte-order) */
static void to_unicode(u_char *dst, const char *src, int len)
{
    for (; len; len--) {
	*dst++ = *src++;
	*dst++ = 0;
    }
}

/* copy unicode src (in Intel byte-order) to dst */
static void from_unicode(char *dst, u_char *src, int len)
{
    for (; len; len--) {
	*dst++ = *src & 0x7f;
	src += 2;
    }
}

/* load a string into an NTLM buffer */
static void load_buffer(u_char *buf, const u_char *str, uint16 len,
			int unicode, u_char *base, uint32 *offset)
{
    if (len) {
	if (unicode) {
	    to_unicode(base + *offset, (const char *) str, len);
	    len *= 2;
	}
	else {
	    memcpy(base + *offset, str, len);
	}
    }

    htois(buf + NTLM_BUFFER_LEN_OFFSET, len);
    htois(buf + NTLM_BUFFER_MAXLEN_OFFSET, len);
    htoil(buf + NTLM_BUFFER_OFFSET_OFFSET, *offset);
    *offset += len;
}

/* unload a string from an NTLM buffer */
static int unload_buffer(const sasl_utils_t *utils, const u_char *buf,
			 u_char **str, unsigned *outlen,
			 int unicode, const u_char *base, unsigned msglen)
{
    uint16 len = itohs(buf + NTLM_BUFFER_LEN_OFFSET);

    if (len) {
	uint32 offset;

	*str = utils->malloc(len + 1); /* add 1 for NUL */
	if (*str == NULL) {
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}

	offset = itohl(buf + NTLM_BUFFER_OFFSET_OFFSET);

	/* sanity check */
	if (offset > msglen || len > (msglen - offset)) return SASL_BADPROT;

	if (unicode) {
	    len /= 2;
	    from_unicode((char *) *str, (u_char *) base + offset, len);
	}
	else
	    memcpy(*str, base + offset, len);

	(*str)[len] = '\0'; /* add NUL */
    }
    else {
	*str = NULL;
    }

    if (outlen) *outlen = len;

    return SASL_OK;
}

/*
 * NTLM encryption/authentication routines per section 2.10 of
 * draft-leach-cifs-v1-spec-02
 */
static void E(unsigned char *out, unsigned char *K, unsigned Klen,
	      unsigned char *D, unsigned Dlen)
	      
{
    unsigned k, d;
    des_cblock K64;
    des_key_schedule ks;
    unsigned char *Dp;
#define KEY_SIZE   7
#define BLOCK_SIZE 8

    for (k = 0; k < Klen; k += KEY_SIZE, K += KEY_SIZE) {
	/* convert 56-bit key to 64-bit */
	K64[0] = K[0];
	K64[1] = ((K[0] << 7) & 0xFF) | (K[1] >> 1);
	K64[2] = ((K[1] << 6) & 0xFF) | (K[2] >> 2);
	K64[3] = ((K[2] << 5) & 0xFF) | (K[3] >> 3);
	K64[4] = ((K[3] << 4) & 0xFF) | (K[4] >> 4);
	K64[5] = ((K[4] << 3) & 0xFF) | (K[5] >> 5);
	K64[6] = ((K[5] << 2) & 0xFF) | (K[6] >> 6);
	K64[7] =  (K[6] << 1) & 0xFF;

 	des_set_odd_parity(&K64); /* XXX is this necessary? */
 	des_set_key(&K64, ks);

	for (d = 0, Dp = D; d < Dlen;
	     d += BLOCK_SIZE, Dp += BLOCK_SIZE, out += BLOCK_SIZE) {
 	    des_ecb_encrypt((void *) Dp, (void *) out, ks, DES_ENCRYPT);
	}
    }
}

static unsigned char *P16_lm(unsigned char *P16, sasl_secret_t *passwd,
			     const sasl_utils_t *utils __attribute__((unused)),
			     char **buf __attribute__((unused)),
			     unsigned *buflen __attribute__((unused)),
			     int *result)
{
    char P14[14];
    unsigned char S8[] = { 0x4b, 0x47, 0x53, 0x21, 0x40, 0x23, 0x24, 0x25 };

    strncpy(P14, (const char *) passwd->data, sizeof(P14));
    ucase(P14, sizeof(P14));

    E(P16, (unsigned char *) P14, sizeof(P14), S8, sizeof(S8));
    *result = SASL_OK;
    return P16;
}

static unsigned char *P16_nt(unsigned char *P16, sasl_secret_t *passwd,
			     const sasl_utils_t *utils,
			     char **buf, unsigned *buflen, int *result)
{
    if (_plug_buf_alloc(utils, buf, buflen, 2 * passwd->len) != SASL_OK) {
	SETERROR(utils, "cannot allocate P16_nt unicode buffer");
	*result = SASL_NOMEM;
    }
    else {
	to_unicode((unsigned char *) *buf, (const char *) passwd->data, passwd->len);
	MD4((unsigned char *) *buf, 2 * passwd->len, P16);
	*result = SASL_OK;
    }
    return P16;
}

static unsigned char *P21(unsigned char *P21, sasl_secret_t *passwd,
			  unsigned char * (*P16)(unsigned char *,
						 sasl_secret_t *,
						 const sasl_utils_t *,
						 char **, unsigned *, int *),
			  const sasl_utils_t *utils,
			  char **buf, unsigned *buflen, int *result)
{
    memset(P16(P21, passwd, utils, buf, buflen, result) + 16, 0, 5);
    return P21;
}

static unsigned char *P24(unsigned char *P24, unsigned char *P21,
			  unsigned char *C8)
		      
{
    E(P24, P21, NTLM_HASH_LENGTH, C8, NTLM_NONCE_LENGTH);
    return P24;
}

static HMAC_CTX *_plug_HMAC_CTX_new(const sasl_utils_t *utils)
{
    utils->log(NULL, SASL_LOG_DEBUG, "_plug_HMAC_CTX_new()");

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    return HMAC_CTX_new();
#else
    return utils->malloc(sizeof(HMAC_CTX));
#endif
}

static void _plug_HMAC_CTX_free(HMAC_CTX *ctx, const sasl_utils_t *utils)
{
    utils->log(NULL, SASL_LOG_DEBUG, "_plug_HMAC_CTX_free()");

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    HMAC_CTX_free(ctx);
#else
    HMAC_cleanup(ctx);
    utils->free(ctx);
#endif
}

static unsigned char *V2(unsigned char *V2, sasl_secret_t *passwd,
			 const char *authid, const char *target,
			 const unsigned char *challenge,
			 const unsigned char *blob, unsigned bloblen,
			 const sasl_utils_t *utils,
			 char **buf, unsigned *buflen, int *result)
{
    HMAC_CTX *ctx = NULL;
    unsigned char hash[EVP_MAX_MD_SIZE];
    char *upper;
    unsigned int len;

    /* Allocate enough space for the unicode target */
    len = (unsigned int) (strlen(authid) + xstrlen(target));
    if (_plug_buf_alloc(utils, buf, buflen, 2 * len + 1) != SASL_OK) {
	SETERROR(utils, "cannot allocate NTLMv2 hash");
	*result = SASL_NOMEM;
    }
    else if ((ctx = _plug_HMAC_CTX_new(utils)) == NULL) {
        SETERROR(utils, "cannot allocate HMAC CTX");
        *result = SASL_NOMEM;
    }
    else {
	/* NTLMv2hash = HMAC-MD5(NTLMhash, unicode(ucase(authid + domain))) */
	P16_nt(hash, passwd, utils, buf, buflen, result);

	/* Use the tail end of the buffer for ucase() conversion */
	upper = *buf + len;
	strcpy(upper, authid);
	if (target) strcat(upper, target);
	ucase(upper, len);
	to_unicode((unsigned char *) *buf, upper, len);

	HMAC(EVP_md5(), hash, MD5_DIGEST_LENGTH,
             (unsigned char *) *buf, 2 * len, hash, &len);

	/* V2 = HMAC-MD5(NTLMv2hash, challenge + blob) + blob */
        HMAC_CTX_reset(ctx);
	HMAC_Init_ex(ctx, hash, len, EVP_md5(), NULL);
	HMAC_Update(ctx, challenge, NTLM_NONCE_LENGTH);
	HMAC_Update(ctx, blob, bloblen);
	HMAC_Final(ctx, V2, &len);

	/* the blob is concatenated outside of this function */

	*result = SASL_OK;
    }

    if (ctx) _plug_HMAC_CTX_free(ctx, utils);

    return V2;
}

/*****************************  Server Section  *****************************/

typedef struct server_context {
    int state;

    uint32 flags;
    unsigned char nonce[NTLM_NONCE_LENGTH];

    /* per-step mem management */
    char *out_buf;
    unsigned out_buf_len;

    /* socket to remote authentication host */
    SOCKET sock;

} server_context_t;

#define	N(a)			(sizeof (a) / sizeof (a[0]))

#define SMB_HDR_PROTOCOL	"\xffSMB"

typedef struct {
    unsigned char protocol[4];
    unsigned char command;
    uint32 status;
    unsigned char flags;
    uint16 flags2;
    uint16 PidHigh;
    unsigned char extra[10];
    uint16 tid;
    uint16 pid;
    uint16 uid;
    uint16 mid;
} SMB_Header;

typedef struct {
    uint16 dialect_index;
    unsigned char security_mode;
    uint16 max_mpx_count;
    uint16 max_number_vcs;
    uint32 max_buffer_size;
    uint32 max_raw_size;
    uint32 session_key;
    uint32 capabilities;
    uint32 system_time_low;
    uint32 system_time_high;
    uint16 server_time_zone;
    unsigned char encryption_key_length;
} SMB_NegProt_Resp;

typedef struct {
    unsigned char andx_command;
    unsigned char andx_reserved;
    uint16 andx_offset;
    uint16 max_buffer_size;
    uint16 max_mpx_count;
    uint16 vc_number;
    uint32 session_key;
    uint16 case_insensitive_passwd_len;
    uint16 case_sensitive_passwd_len;
    uint32 reserved;
    uint32 capabilities;
} SMB_SessionSetup;

typedef struct {
    unsigned char andx_command;
    unsigned char andx_reserved;
    uint16 andx_offset;
    uint16 action;
} SMB_SessionSetup_Resp;

enum {
    NBT_SESSION_REQUEST		= 0x81,
    NBT_POSITIVE_SESSION_RESP	= 0x82,
    NBT_NEGATIVE_SESSION_RESP	= 0x83,
    NBT_ERR_NO_LISTEN_CALLED	= 0x80,
    NBT_ERR_NO_LISTEN_CALLING	= 0x81,
    NBT_ERR_CALLED_NOT_PRESENT	= 0x82,
    NBT_ERR_INSUFFICIENT_RESRC	= 0x83,
    NBT_ERR_UNSPECIFIED		= 0x8F,

    SMB_HDR_SIZE		= 32,

    SMB_COM_NEGOTIATE_PROTOCOL	= 0x72,
    SMB_COM_SESSION_SETUP_ANDX	= 0x73,
    SMB_COM_NONE		= 0xFF,

    SMB_FLAGS_SERVER_TO_REDIR	= 0x80,

    SMB_FLAGS2_ERR_STATUS	= 0x4000,
    SMB_FLAGS2_UNICODE		= 0x8000,

    SMB_NEGPROT_RESP_SIZE	= 34,

    SMB_SECURITY_MODE_USER	= 0x1,
    SMB_SECURITY_MODE_ENCRYPT	= 0x2,
    SMB_SECURITY_MODE_SIGN	= 0x4,
    SMB_SECURITY_MODE_SIGN_REQ	= 0x8,

    SMB_CAP_UNICODE		= 0x0004,
    SMB_CAP_STATUS32		= 0x0040,
    SMB_CAP_EXTENDED_SECURITY	= 0x80000000,

    SMB_SESSION_SETUP_SIZE	= 26,
    SMB_SESSION_SETUP_RESP_SIZE	= 6,

    SMB_REQUEST_MODE_GUEST	= 0x1
};

static const char *SMB_DIALECTS[] = {
#if 0
    "\x02PC NETWORK PROGRAM 1.0",
    "\x02PCLAN1.0",
    "\x02MICROSOFT NETWORKS 1.03",
    "\x02MICROSOFT NETWORKS 3.0",
    "\x02LANMAN1.0",
    "\x02Windows for Workgroups 3.1a",
    "\x02LM1.2X002",
    "\x02DOS LM1.2X002",
    "\x02DOS LANLAM2.1",
    "\x02LANMAN2.1",
#endif
    "\x02NT LM 0.12"
};

static void load_smb_header(unsigned char buf[], SMB_Header *hdr)
{
    unsigned char *p = buf;

    memcpy(p, SMB_HDR_PROTOCOL, 4); p += 4;
    *p++ = hdr->command;
    htoil(p, hdr->status); p += 4;
    *p++ = hdr->flags;
    htois(p, hdr->flags2); p += 2;
    htois(p, hdr->PidHigh); p += 2;
    memcpy(p, hdr->extra, 10); p += 10;
    htois(p, hdr->tid); p += 2;
    htois(p, hdr->pid); p += 2;
    htois(p, hdr->uid); p += 2;
    htois(p, hdr->mid);
}

static void unload_smb_header(unsigned char buf[], SMB_Header *hdr)
{
    unsigned char *p = buf;

    memcpy(hdr->protocol, p, 4); p += 4;
    hdr->command = *p++;
    hdr->status = itohl(p); p += 4;
    hdr->flags = *p++;
    hdr->flags2 = itohs(p); p += 2;
    hdr->PidHigh = itohs(p); p += 2;
    memcpy(hdr->extra, p, 10); p += 10;
    hdr->tid = itohs(p); p += 2;
    hdr->pid = itohs(p); p += 2;
    hdr->uid = itohs(p); p += 2;
    hdr->mid = itohs(p);
}

static void unload_negprot_resp(unsigned char buf[], SMB_NegProt_Resp *resp)
{
    unsigned char *p = buf;

    resp->dialect_index = itohs(p); p += 2;
    resp->security_mode = *p++;
    resp->max_mpx_count = itohs(p); p += 2;
    resp->max_number_vcs = itohs(p); p += 2;
    resp->max_buffer_size = itohl(p); p += 4;
    resp->max_raw_size = itohl(p); p += 4;
    resp->session_key = itohl(p); p += 4;
    resp->capabilities = itohl(p); p += 4;
    resp->system_time_low = itohl(p); p += 4;
    resp->system_time_high = itohl(p); p += 4;
    resp->server_time_zone = itohs(p); p += 2;
    resp->encryption_key_length = *p;
}

static void load_session_setup(unsigned char buf[], SMB_SessionSetup *setup)
{
    unsigned char *p = buf;

    *p++ = setup->andx_command;
    *p++ = setup->andx_reserved;
    htois(p, setup->andx_offset); p += 2;
    htois(p, setup->max_buffer_size); p += 2;
    htois(p, setup->max_mpx_count); p += 2;
    htois(p, setup->vc_number); p += 2;
    htoil(p, setup->session_key); p += 4;
    htois(p, setup->case_insensitive_passwd_len); p += 2;
    htois(p, setup->case_sensitive_passwd_len); p += 2;
    htoil(p, setup->reserved); p += 4;
    htoil(p, setup->capabilities); p += 4;
}

static void unload_session_setup_resp(unsigned char buf[],
				      SMB_SessionSetup_Resp *resp)
{
    unsigned char *p = buf;

    resp->andx_command = *p++;
    resp->andx_reserved = *p++;
    resp->andx_offset = itohs(p); p += 2;
    resp->action = itohs(p);
}

/*
 * Keep calling the writev() system call with 'fd', 'iov', and 'iovcnt'
 * until all the data is written out or an error occurs.
 */
static int retry_writev(SOCKET fd, struct iovec *iov, int iovcnt)
{
    int n;
    int i;
    int written = 0;
    static int iov_max =
#ifdef MAXIOV
	MAXIOV
#else
#ifdef IOV_MAX
	IOV_MAX
#else
	8192
#endif
#endif
	;
    
    for (;;) {
	while (iovcnt && iov[0].iov_len == 0) {
	    iov++;
	    iovcnt--;
	}

	if (!iovcnt) return written;

	n = (int) writev(fd, iov, iovcnt > iov_max ? iov_max : iovcnt);
	if (n == -1) {
#ifndef WIN32
	    if (errno == EINVAL && iov_max > 10) {
		iov_max /= 2;
		continue;
	    }
	    if (errno == EINTR) continue;
#endif
	    return -1;
	}

	written += n;

	for (i = 0; i < iovcnt; i++) {
	    if ((int) iov[i].iov_len > n) {
		iov[i].iov_base = (char *) iov[i].iov_base + n;
		iov[i].iov_len -= n;
		break;
	    }
	    n -= iov[i].iov_len;
	    iov[i].iov_len = 0;
	}

	if (i == iovcnt) return written;
    }
}

/*
 * Keep calling the read() system call with 'fd', 'buf', and 'nbyte'
 * until all the data is read in or an error occurs.
 */
static int retry_read(SOCKET fd, char *buf0, unsigned nbyte)
{
    int n;
    int nread = 0;
    char *buf = buf0;

    if (nbyte == 0) return 0;

    for (;;) {
/* Can't use read() on sockets on Windows, but recv works on all platforms */
	n = recv (fd, buf, nbyte, 0);
	if (n == -1 || n == 0) {
#ifndef WIN32
	    if (errno == EINTR || errno == EAGAIN) continue;
#endif
	    return -1;
	}

	nread += n;

	if (n >= (int) nbyte) return nread;

	buf += n;
	nbyte -= n;
    }
}

static void make_netbios_name(const char *in, unsigned char out[])
{
    size_t i, j = 0, n;

    /* create a NetBIOS name from the DNS name
     *
     * - use up to the first 16 chars of the first part of the hostname
     * - convert to all uppercase
     * - use the tail end of the output buffer as temp space
     */
    n = strcspn(in, ".");
    if (n > 16) n = 16;
    strncpy((char *) out+18, in, n);
    in = (char *) out+18;
    ucase(in, n);

    out[j++] = 0x20;
    for (i = 0; i < n; i++) {
	out[j++] = ((in[i] >> 4) & 0xf) + 0x41;
	out[j++] = (in[i] & 0xf) + 0x41;
    }
    for (; i < 16; i++) {
	out[j++] = ((0x20 >> 4) & 0xf) + 0x41;
	out[j++] = (0x20 & 0xf) + 0x41;
    }
    out[j] = 0;
}

static SOCKET smb_connect_server(const sasl_utils_t *utils, const char *client,
			      const char *server)
{
    struct addrinfo hints;
    struct addrinfo *ai = NULL, *r;
    SOCKET s = (SOCKET) -1;
    int err;
    char * error_str;
#ifdef WIN32
    DWORD saved_errno;
#else
    int saved_errno;
#endif
    int niflags;
    char *port = "139";
    char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];

    unsigned char called[34];
    unsigned char calling[34];
    struct iovec iov[3];
    uint32 pkt;
    int rc;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;
    if ((err = getaddrinfo(server, port, &hints, &ai)) != 0) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: getaddrinfo %s/%s: %s",
		   server, port, gai_strerror(err));
	return -1;
    }

    /* Make sure we have AF_INET or AF_INET6 addresses. */
    if (ai->ai_family != AF_INET && ai->ai_family != AF_INET6) {
	utils->log(NULL, SASL_LOG_ERR, "NTLM: no IP address info for %s",
		   ai->ai_canonname ? ai->ai_canonname : server);
	freeaddrinfo(ai);
	return -1;
    }

    /* establish connection to authentication server */
    for (r = ai; r; r = r->ai_next) {
	s = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
	if (s < 0)
	    continue;
	if (connect(s, r->ai_addr, r->ai_addrlen) >= 0)
	    break;
#ifdef WIN32
	saved_errno = WSAGetLastError();
#else
	saved_errno = errno;
#endif
	closesocket (s);
	s = -1;
	niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
	if (r->ai_family == AF_INET6)
	    niflags |= NI_WITHSCOPEID;
#endif
	if (getnameinfo(r->ai_addr, r->ai_addrlen, hbuf, sizeof(hbuf),
			pbuf, sizeof(pbuf), niflags) != 0) {
	    strcpy(hbuf, "unknown");
	    strcpy(pbuf, "unknown");
	}

        /* Can't use errno (and %m), as it doesn't contain
         * the socket error on Windows */
	error_str = _plug_get_error_message (utils, saved_errno);
	utils->log(NULL, SASL_LOG_WARN, "NTLM: connect %s[%s]/%s: %s",
		   ai->ai_canonname ? ai->ai_canonname : server,
		   hbuf,
		   pbuf,
		   error_str);
	utils->free (error_str);
    }
    if (s < 0) {
	if (getnameinfo(ai->ai_addr, ai->ai_addrlen, NULL, 0,
			pbuf, sizeof(pbuf), NI_NUMERICSERV) != 0) {
		strcpy(pbuf, "unknown");
	}
	utils->log(NULL, SASL_LOG_ERR, "NTLM: couldn't connect to %s/%s",
		   ai->ai_canonname ? ai->ai_canonname : server, pbuf);
	freeaddrinfo(ai);
	return -1;
    }

    freeaddrinfo(ai);

    /*** send NetBIOS session request ***/

    /* get length of data */
    pkt = sizeof(called) + sizeof(calling);

    /* make sure length is less than 17 bits */
    if (pkt >= (1 << 17)) {
	closesocket(s);
	return -1;
    }

    /* prepend the packet type */
    pkt |= (NBT_SESSION_REQUEST << 24);
    pkt = htonl(pkt);

    /* XXX should determine the real NetBIOS name */
    make_netbios_name(server, called);
    make_netbios_name(client, calling);

    iov[0].iov_base = (void *) &pkt;
    iov[0].iov_len = sizeof(pkt);
    iov[1].iov_base = called;
    iov[1].iov_len = sizeof(called);
    iov[2].iov_base = calling;
    iov[2].iov_len = sizeof(calling);

    rc = retry_writev(s, iov, N(iov));
    if (rc == -1) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error sending NetBIOS session request");
	closesocket(s);
	return -1;
    }

    rc = retry_read(s, (char *) &pkt, sizeof(pkt));
    pkt = ntohl(pkt);
    if (rc == -1 || pkt != (uint32) (NBT_POSITIVE_SESSION_RESP << 24)) {
	unsigned char ec = NBT_ERR_UNSPECIFIED;
	char *errstr;

	retry_read(s, (char *) &ec, sizeof(ec));
	switch (ec) {
	case NBT_ERR_NO_LISTEN_CALLED:
	    errstr = "Not listening on called name";
	    break;
	case NBT_ERR_NO_LISTEN_CALLING:
	    errstr = "Not listening for calling name";
	    break;
	case NBT_ERR_CALLED_NOT_PRESENT:
	    errstr = "Called name not present";
	    break;
	case NBT_ERR_INSUFFICIENT_RESRC:
	    errstr = "Called name present, but insufficient resources";
	    break;
	default:
	    errstr = "Unspecified error";
	}
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: negative NetBIOS session response: %s", errstr);
	closesocket(s);
	return -1;
    }

    return s;
}

static int smb_negotiate_protocol(const sasl_utils_t *utils,
				  server_context_t *text, char **domain)
{
    SMB_Header hdr;
    SMB_NegProt_Resp resp;
    unsigned char hbuf[SMB_HDR_SIZE], *p;
    unsigned char wordcount = 0;
    unsigned char bc[sizeof(uint16)];
    uint16 bytecount;
    uint32 len, nl;
    int n_dialects = N(SMB_DIALECTS);
    struct iovec iov[4+N(SMB_DIALECTS)];
    int i, n;
    int rc;
    pid_t current_pid;

    /*** create a negotiate protocol request ***/

    /* create a header */
    memset(&hdr, 0, sizeof(hdr));
    hdr.command = SMB_COM_NEGOTIATE_PROTOCOL;
#if 0
    hdr.flags2 = SMB_FLAGS2_ERR_STATUS;
    if (text->flags & NTLM_USE_UNICODE) hdr.flags2 |= SMB_FLAGS2_UNICODE;
#endif
    current_pid = sasl_getpid();
    if (sizeof(current_pid) <= 2) {
	hdr.pid = (uint16) current_pid;
	hdr.PidHigh = 0;
    } else {
	hdr.pid = (uint16) (((uint32) current_pid) & 0xFFFF);
	hdr.PidHigh = (uint16) (((uint32) current_pid) >> 16);
    }

    load_smb_header(hbuf, &hdr);

    /* put together all of the pieces of the request */
    n = 0;
    iov[n].iov_base = (void *) &nl;
    iov[n++].iov_len = sizeof(len);
    iov[n].iov_base = hbuf;
    iov[n++].iov_len = SMB_HDR_SIZE;
    iov[n].iov_base = &wordcount;
    iov[n++].iov_len = sizeof(wordcount);
    iov[n].iov_base = (void *) &bc;
    iov[n++].iov_len = sizeof(bc);

    /* add our supported dialects */
    for (i = 0; i < n_dialects; i++) {
	iov[n].iov_base = (char *) SMB_DIALECTS[i];
	iov[n++].iov_len = (long) strlen(SMB_DIALECTS[i]) + 1;
    }

    /* total up the lengths */
    len = bytecount = 0;
    for (i = 1; i < 4; i++) len += iov[i].iov_len;
    for (i = 4; i < n; i++) bytecount += (uint16) iov[i].iov_len;
    len += bytecount;
    nl = htonl(len);
    htois((char *) &bc, bytecount);

    /* send it */
    rc = retry_writev(text->sock, iov, n);
    if (rc == -1) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error sending NEGPROT request");
	return SASL_FAIL;
    }

    /*** read the negotiate protocol response ***/

    /* read the total length */
    rc = retry_read(text->sock, (char *) &nl, sizeof(nl));
    if (rc < (int) sizeof(nl)) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error reading NEGPROT response length");
	return SASL_FAIL;
    }

    /* read the data */
    len = ntohl(nl);
    if (_plug_buf_alloc(utils, &text->out_buf, &text->out_buf_len,
			len) != SASL_OK) {
	SETERROR(utils, "cannot allocate NTLM NEGPROT response buffer");
	return SASL_NOMEM;
    }

    rc = retry_read(text->sock, text->out_buf, len);
    if (rc < (int) len) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error reading NEGPROT response");
	return SASL_FAIL;
    }
    p = (unsigned char *) text->out_buf;

    /* parse the header */
    if (len < SMB_HDR_SIZE) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: not enough data for NEGPROT response header");
	return SASL_FAIL;
    }
    unload_smb_header(p, &hdr);
    p += SMB_HDR_SIZE;
    len -= SMB_HDR_SIZE;

    /* sanity check the header */
    if (memcmp(hdr.protocol, SMB_HDR_PROTOCOL, 4)	 /* correct protocol */
	|| hdr.command != SMB_COM_NEGOTIATE_PROTOCOL /* correct command */
	|| hdr.status				 /* no errors */
	|| !(hdr.flags & SMB_FLAGS_SERVER_TO_REDIR)) { /* response */
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error in NEGPROT response header: %u",
		   hdr.status);
	return SASL_FAIL;
    }

    /* get the wordcount */
    if (len < 1) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: not enough data for NEGPROT response wordcount");
	return SASL_FAIL;
    }
    wordcount = *p++;
    len--;

    /* parse the parameters */
    if (wordcount != SMB_NEGPROT_RESP_SIZE / sizeof(uint16)) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: incorrect NEGPROT wordcount for NT LM 0.12");
	return SASL_FAIL;
    }
    unload_negprot_resp(p, &resp);
    p += SMB_NEGPROT_RESP_SIZE;
    len -= SMB_NEGPROT_RESP_SIZE;

    /* sanity check the parameters */
    if (resp.dialect_index != 0
	|| !(resp.security_mode & SMB_SECURITY_MODE_USER)
	|| !(resp.security_mode & SMB_SECURITY_MODE_ENCRYPT)
	|| resp.security_mode & SMB_SECURITY_MODE_SIGN_REQ
	|| resp.capabilities & SMB_CAP_EXTENDED_SECURITY
	|| resp.encryption_key_length != NTLM_NONCE_LENGTH) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error in NEGPROT response parameters");
	return SASL_FAIL;
    }

    /* get the bytecount */
    if (len < 2) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: not enough data for NEGPROT response bytecount");
	return SASL_FAIL;
    }
    bytecount = itohs(p);
    p += 2;
    len -= 2;
    if (len != bytecount) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: incorrect bytecount for NEGPROT response data");
	return SASL_FAIL;
    }

    /* parse the data */
    memcpy(text->nonce, p, resp.encryption_key_length);
    p += resp.encryption_key_length;
    len -= resp.encryption_key_length;

    /* if client asked for target, send domain */
    if (text->flags & NTLM_ASK_TARGET) {
	*domain = utils->malloc(len);
	if (*domain == NULL) {
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}
	memcpy(*domain, p, len);
	from_unicode(*domain, (unsigned char *) *domain, len);

	text->flags |= NTLM_TARGET_IS_DOMAIN;
    }

    return SASL_OK;
}

static int smb_session_setup(const sasl_utils_t *utils, server_context_t *text,
			     const char *authid, char *domain,
			     unsigned char *lm_resp, unsigned lm_resp_len,
			     unsigned char *nt_resp, unsigned nt_resp_len)
{
    SMB_Header hdr;
    SMB_SessionSetup setup;
    SMB_SessionSetup_Resp resp;
    unsigned char hbuf[SMB_HDR_SIZE], sbuf[SMB_SESSION_SETUP_SIZE], *p;
    unsigned char wordcount = SMB_SESSION_SETUP_SIZE / sizeof(uint16);
    unsigned char bc[sizeof(uint16)];
    uint16 bytecount;
    uint32 len, nl;
    struct iovec iov[12];
    int i, n;
    int rc;
#ifdef WIN32
    char osbuf[80];
#else
    char osbuf[2*SYS_NMLN+2];
#endif
    char lanman[20];
    pid_t current_pid;

    /*** create a session setup request ***/

    /* create a header */
    memset(&hdr, 0, sizeof(hdr));
    hdr.command = SMB_COM_SESSION_SETUP_ANDX;
#if 0
    hdr.flags2 = SMB_FLAGS2_ERR_STATUS;
    if (text->flags & NTLM_USE_UNICODE) hdr.flags2 |= SMB_FLAGS2_UNICODE;
#endif
    current_pid = sasl_getpid();
    if (sizeof(current_pid) <= 2) {
	hdr.pid = (uint16) current_pid;
	hdr.PidHigh = 0;
    } else {
	hdr.pid = (uint16) (((uint32) current_pid) & 0xFFFF);
	hdr.PidHigh = (uint16) (((uint32) current_pid) >> 16);
    }

    load_smb_header(hbuf, &hdr);

    /* create a the setup parameters */
    memset(&setup, 0, sizeof(setup));
    setup.andx_command = SMB_COM_NONE;
    setup.max_buffer_size = 0xFFFF;
    if (lm_resp) setup.case_insensitive_passwd_len = lm_resp_len;
    if (nt_resp) setup.case_sensitive_passwd_len = nt_resp_len;
#if 0
    if (text->flags & NTLM_USE_UNICODE)
	setup.capabilities = SMB_CAP_UNICODE;
#endif
    load_session_setup(sbuf, &setup);

    _plug_snprintf_os_info (osbuf, sizeof(osbuf));

    snprintf(lanman, sizeof(lanman), "Cyrus SASL %u.%u.%u",
	     SASL_VERSION_MAJOR, SASL_VERSION_MINOR,
	     SASL_VERSION_STEP);

    /* put together all of the pieces of the request */
    n = 0;
    iov[n].iov_base = (void *) &nl;
    iov[n++].iov_len = sizeof(len);
    iov[n].iov_base = hbuf;
    iov[n++].iov_len = SMB_HDR_SIZE;
    iov[n].iov_base = &wordcount;
    iov[n++].iov_len = sizeof(wordcount);
    iov[n].iov_base = sbuf;
    iov[n++].iov_len = SMB_SESSION_SETUP_SIZE;
    iov[n].iov_base = (void *) &bc;
    iov[n++].iov_len = sizeof(bc);
    if (lm_resp) {
	iov[n].iov_base = lm_resp;
	iov[n++].iov_len = NTLM_RESP_LENGTH;
    }
    if (nt_resp) {
	iov[n].iov_base = nt_resp;
	iov[n++].iov_len = NTLM_RESP_LENGTH;
    }
    iov[n].iov_base = (char*) authid;
    iov[n++].iov_len = (long) strlen(authid) + 1;
    if (!domain) domain = "";
    iov[n].iov_base = domain;
    iov[n++].iov_len = (long) strlen(domain) + 1;
    iov[n].iov_base = osbuf;
    iov[n++].iov_len = (long) strlen(osbuf) + 1;
    iov[n].iov_base = lanman;
    iov[n++].iov_len = (long) strlen(lanman) + 1;

    /* total up the lengths */
    len = bytecount = 0;
    for (i = 1; i < 5; i++) len += iov[i].iov_len;
    for (i = 5; i < n; i++) bytecount += (uint16) iov[i].iov_len;
    len += bytecount;
    nl = htonl(len);
    htois((char *) &bc, bytecount);

    /* send it */
    rc = retry_writev(text->sock, iov, n);
    if (rc == -1) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error sending SESSIONSETUP request");
	return SASL_FAIL;
    }

    /*** read the session setup response ***/

    /* read the total length */
    rc = retry_read(text->sock, (char *) &nl, sizeof(nl));
    if (rc < (int) sizeof(nl)) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error reading SESSIONSETUP response length");
	return SASL_FAIL;
    }

    /* read the data */
    len = ntohl(nl);
    if (_plug_buf_alloc(utils, &text->out_buf, &text->out_buf_len,
			len) != SASL_OK) {
	SETERROR(utils,
		 "cannot allocate NTLM SESSIONSETUP response buffer");
	return SASL_NOMEM;
    }

    rc = retry_read(text->sock, text->out_buf, len);
    if (rc < (int) len) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error reading SESSIONSETUP response");
	return SASL_FAIL;
    }
    p = (unsigned char *) text->out_buf;

    /* parse the header */
    if (len < SMB_HDR_SIZE) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: not enough data for SESSIONSETUP response header");
	return SASL_FAIL;
    }
    unload_smb_header(p, &hdr);
    p += SMB_HDR_SIZE;
    len -= SMB_HDR_SIZE;

    /* sanity check the header */
    if (memcmp(hdr.protocol, SMB_HDR_PROTOCOL, 4)	/* correct protocol */
	|| hdr.command != SMB_COM_SESSION_SETUP_ANDX	/* correct command */
	|| !(hdr.flags & SMB_FLAGS_SERVER_TO_REDIR)) {	/* response */
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: error in SESSIONSETUP response header");
	return SASL_FAIL;
    }

    /* check auth success */
    if (hdr.status) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: auth failure: %u", hdr.status);
	return SASL_BADAUTH;
    }

    /* get the wordcount */
    if (len < 1) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: not enough data for SESSIONSETUP response wordcount");
	return SASL_FAIL;
    }
    wordcount = *p++;
    len--;

    /* parse the parameters */
    if (wordcount < SMB_SESSION_SETUP_RESP_SIZE / sizeof(uint16)) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: incorrect SESSIONSETUP wordcount");
	return SASL_FAIL;
    }
    unload_session_setup_resp(p, &resp);

    /* check auth success */
    if (resp.action & SMB_REQUEST_MODE_GUEST) {
	utils->log(NULL, SASL_LOG_ERR,
		   "NTLM: authenticated as guest");
	return SASL_BADAUTH;
    }

    return SASL_OK;
}

/*
 * Create a server challenge message (type 2) consisting of:
 *
 * signature (8 bytes)
 * message type (uint32)
 * target name (buffer)
 * flags (uint32)
 * challenge (8 bytes)
 * context (8 bytes)
 * target info (buffer)
 * data
 */
static int create_challenge(const sasl_utils_t *utils,
			    char **buf, unsigned *buflen,
			    const char *target, uint32 flags,
			    const u_char *nonce, unsigned *outlen)
{
    uint32 offset = NTLM_TYPE2_DATA_OFFSET;
    u_char *base;

    if (!nonce) {
	SETERROR(utils, "need nonce for NTLM challenge");
	return SASL_FAIL;
    }

    *outlen = offset + 2 * (unsigned) xstrlen(target);

    if (_plug_buf_alloc(utils, buf, buflen, *outlen) != SASL_OK) {
	SETERROR(utils, "cannot allocate NTLM challenge");
	return SASL_NOMEM;
    }

    base = (unsigned char *) *buf;
    memset(base, 0, *outlen);
    memcpy(base + NTLM_SIG_OFFSET, NTLM_SIGNATURE, sizeof(NTLM_SIGNATURE));
    htoil(base + NTLM_TYPE_OFFSET, NTLM_TYPE_CHALLENGE);
    load_buffer(base + NTLM_TYPE2_TARGET_OFFSET,
		(const unsigned char *) ucase(target, 0), (uint16) xstrlen(target), flags & NTLM_USE_UNICODE,
		base, &offset);
    htoil(base + NTLM_TYPE2_FLAGS_OFFSET, flags);
    memcpy(base + NTLM_TYPE2_CHALLENGE_OFFSET, nonce, NTLM_NONCE_LENGTH);

    return SASL_OK;
}

static int ntlm_server_mech_new(void *glob_context __attribute__((unused)), 
				sasl_server_params_t *sparams,
				const char *challenge __attribute__((unused)),
				unsigned challen __attribute__((unused)),
				void **conn_context)
{
    server_context_t *text;
    const char *serv;
    unsigned int len;
    SOCKET sock = (SOCKET) -1;

    /* holds state are in: allocate early */
    text = sparams->utils->malloc(sizeof(server_context_t));
    if (text == NULL) {
	MEMERROR( sparams->utils );
	return SASL_NOMEM;
    }

    sparams->utils->getopt(sparams->utils->getopt_context,
			   "NTLM", "ntlm_server", &serv, &len);
    if (serv) {
	unsigned int i,j;
	char *tmp, *next;

	/* strip any whitespace */
	if(_plug_strdup(sparams->utils, serv, &tmp, NULL) != SASL_OK) {
	    MEMERROR( sparams->utils );
	    return SASL_NOMEM;
	}
	for(i=0, j=0; i<len; i++) {
	    if(!isspace(tmp[i])) tmp[j++] = tmp[i];
	}
	tmp[j] = '\0';
	next = tmp;

	/* try to connect to a list of servers */
	do {
	    serv = next;
	    next = strchr(serv, ',');
	    if(next) *(next++) = '\0';
	    /* try to start a NetBIOS session with the server */
	    sock = smb_connect_server(sparams->utils, sparams->serverFQDN, serv);
	} while(sock == (SOCKET) -1 && next);

	sparams->utils->free(tmp);
	if (sock == (SOCKET) -1) return SASL_UNAVAIL;
    }
    
    memset(text, 0, sizeof(server_context_t));
    
    text->state = 1;
    text->sock = sock;
    
    *conn_context = text;
    
    return SASL_OK;
}

static int ntlm_server_mech_step1(server_context_t *text,
				  sasl_server_params_t *sparams,
				  const char *clientin,
				  unsigned clientinlen,
				  const char **serverout,
				  unsigned *serveroutlen,
				  sasl_out_params_t *oparams __attribute__((unused)))
{
    char *domain = NULL;
    int result;

    if (!clientin || clientinlen < NTLM_TYPE1_MINSIZE ||
	memcmp(clientin, NTLM_SIGNATURE, sizeof(NTLM_SIGNATURE)) ||
	itohl(clientin + NTLM_TYPE_OFFSET) != NTLM_TYPE_REQUEST) {
	SETERROR(sparams->utils, "client didn't issue valid NTLM request");
	return SASL_BADPROT;
    }

    text->flags = itohl(clientin + NTLM_TYPE1_FLAGS_OFFSET);
    sparams->utils->log(NULL, SASL_LOG_DEBUG,
			"client flags: %x", text->flags);

    text->flags &= NTLM_FLAGS_MASK; /* mask off the bits we don't support */

    /* if client can do Unicode, turn off ASCII */
    if (text->flags & NTLM_USE_UNICODE) text->flags &= ~NTLM_USE_ASCII;

    if (text->sock == -1) {
	/* generate challenge internally */

	/* if client asked for target, use FQDN as server target */
	if (text->flags & NTLM_ASK_TARGET) {
	    result = _plug_strdup(sparams->utils, sparams->serverFQDN,
			      &domain, NULL);
	    if (result != SASL_OK) return result;

	    text->flags |= NTLM_TARGET_IS_SERVER;
	}

	/* generate a nonce */
	sparams->utils->rand(sparams->utils->rpool,
			     (char *) text->nonce, NTLM_NONCE_LENGTH);
    }
    else {
	/* proxy the response/challenge */
	result = smb_negotiate_protocol(sparams->utils, text, &domain);
	if (result != SASL_OK) goto cleanup;
    }

    result = create_challenge(sparams->utils,
			      &text->out_buf, &text->out_buf_len,
			      domain, text->flags, text->nonce, serveroutlen);
    if (result != SASL_OK) goto cleanup;

    *serverout = text->out_buf;

    text->state = 2;
    
    result = SASL_CONTINUE;

  cleanup:
    if (domain) sparams->utils->free(domain);

    return result;
}

static int ntlm_server_mech_step2(server_context_t *text,
				  sasl_server_params_t *sparams,
				  const char *clientin,
				  unsigned clientinlen,
				  const char **serverout __attribute__((unused)),
				  unsigned *serveroutlen __attribute__((unused)),
				  sasl_out_params_t *oparams)
{
    unsigned char *lm_resp = NULL, *nt_resp = NULL;
    char *domain = NULL, *authid = NULL;
    unsigned lm_resp_len, nt_resp_len, domain_len, authid_len;
    int result;

    if (!clientin || clientinlen < NTLM_TYPE3_MINSIZE ||
	memcmp(clientin, NTLM_SIGNATURE, sizeof(NTLM_SIGNATURE)) ||
	itohl(clientin + NTLM_TYPE_OFFSET) != NTLM_TYPE_RESPONSE) {
	SETERROR(sparams->utils, "client didn't issue valid NTLM response");
	return SASL_BADPROT;
    }

    result = unload_buffer(sparams->utils,
			   (const unsigned char *) clientin + NTLM_TYPE3_LMRESP_OFFSET,
			   (u_char **) &lm_resp, &lm_resp_len, 0,
			   (const unsigned char *) clientin, clientinlen);
    if (result != SASL_OK) goto cleanup;

    result = unload_buffer(sparams->utils,
			   (const unsigned char *) clientin + NTLM_TYPE3_NTRESP_OFFSET,
			   (u_char **) &nt_resp, &nt_resp_len, 0,
			   (const unsigned char *) clientin, clientinlen);
    if (result != SASL_OK) goto cleanup;

    result = unload_buffer(sparams->utils,
			   (const unsigned char *) clientin + NTLM_TYPE3_DOMAIN_OFFSET,
			   (u_char **) &domain, &domain_len,
			   text->flags & NTLM_USE_UNICODE,
			   (const unsigned char *) clientin, clientinlen);
    if (result != SASL_OK) goto cleanup;

    result = unload_buffer(sparams->utils,
			   (const unsigned char *) clientin + NTLM_TYPE3_USER_OFFSET,
			   (u_char **) &authid, &authid_len,
			   text->flags & NTLM_USE_UNICODE,
			   (const unsigned char *) clientin, clientinlen);
    if (result != SASL_OK) goto cleanup;

    /* require at least one response and an authid */
    if ((!lm_resp && !nt_resp) ||
	(lm_resp && lm_resp_len < NTLM_RESP_LENGTH) ||
	(nt_resp && nt_resp_len < NTLM_RESP_LENGTH) ||
	!authid) {
	SETERROR(sparams->utils, "client issued incorrect/nonexistent responses");
	result = SASL_BADPROT;
	goto cleanup;
    }

    sparams->utils->log(NULL, SASL_LOG_DEBUG,
			"client user: %s", authid);
    if (domain) sparams->utils->log(NULL, SASL_LOG_DEBUG,
				    "client domain: %s", domain);

    if (text->sock == -1) {
	/* verify the response internally */

	sasl_secret_t *password = NULL;
	size_t pass_len;
	const char *password_request[] = { SASL_AUX_PASSWORD,
				       NULL };
	struct propval auxprop_values[2];
	unsigned char hash[NTLM_HASH_LENGTH];
	unsigned char resp[NTLM_RESP_LENGTH];

	/* fetch user's password */
	result = sparams->utils->prop_request(sparams->propctx, password_request);
	if (result != SASL_OK) goto cleanup;
    
	/* this will trigger the getting of the aux properties */
	result = sparams->canon_user(sparams->utils->conn, authid, authid_len,
				     SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) goto cleanup;

	result = sparams->utils->prop_getnames(sparams->propctx,
					       password_request,
					       auxprop_values);
	if (result < 0 ||
	    (!auxprop_values[0].name || !auxprop_values[0].values)) {
	    /* We didn't find this username */
	    SETERROR(sparams->utils, "no secret in database");
	    result = sparams->transition ? SASL_TRANS : SASL_NOUSER;
	    goto cleanup;
	}
    
	pass_len = strlen(auxprop_values[0].values[0]);
	if (pass_len == 0) {
	    SETERROR(sparams->utils, "empty secret");
	    result = SASL_FAIL;
	    goto cleanup;
	}

	password = sparams->utils->malloc(sizeof(sasl_secret_t) + pass_len);
	if (!password) {
	    result = SASL_NOMEM;
	    goto cleanup;
	}
	
	password->len = (unsigned) pass_len;
	strncpy((char *) password->data, auxprop_values[0].values[0], pass_len + 1);

	/* erase the plaintext password */
	sparams->utils->prop_erase(sparams->propctx, password_request[0]);

	/* calculate our own response(s) and compare with client's */
	result = SASL_OK;
	if (nt_resp && (nt_resp_len > NTLM_RESP_LENGTH)) {
	    /* Try NTv2 response */
	    sparams->utils->log(NULL, SASL_LOG_DEBUG,
				"calculating NTv2 response");
	    V2(resp, password, authid, domain, text->nonce,
	       nt_resp + MD5_DIGEST_LENGTH, nt_resp_len - MD5_DIGEST_LENGTH,
	       sparams->utils, &text->out_buf, &text->out_buf_len,
	       &result);

	    /* No need to compare the blob */
	    if (memcmp(nt_resp, resp, MD5_DIGEST_LENGTH)) {
		SETERROR(sparams->utils, "incorrect NTLMv2 response");
		result = SASL_BADAUTH;
	    }
	}
	else if (nt_resp) {
	    /* Try NT response */
	    sparams->utils->log(NULL, SASL_LOG_DEBUG,
				"calculating NT response");
	    P24(resp, P21(hash, password, P16_nt, sparams->utils,
			  &text->out_buf, &text->out_buf_len, &result),
		text->nonce);
	    if (memcmp(nt_resp, resp, NTLM_RESP_LENGTH)) {
		SETERROR(sparams->utils, "incorrect NTLM response");
		result = SASL_BADAUTH;
	    }
	}
	else if (lm_resp) {
	    /* Try LMv2 response */
	    sparams->utils->log(NULL, SASL_LOG_DEBUG,
				"calculating LMv2 response");
	    V2(resp, password, authid, domain, text->nonce,
	       lm_resp + MD5_DIGEST_LENGTH, lm_resp_len - MD5_DIGEST_LENGTH,
	       sparams->utils, &text->out_buf, &text->out_buf_len,
	       &result);
		
	    /* No need to compare the blob */
	    if (memcmp(lm_resp, resp, MD5_DIGEST_LENGTH)) {
		/* Try LM response */
		sparams->utils->log(NULL, SASL_LOG_DEBUG,
				    "calculating LM response");
		P24(resp, P21(hash, password, P16_lm, sparams->utils,
			      &text->out_buf, &text->out_buf_len, &result),
		    text->nonce);
		if (memcmp(lm_resp, resp, NTLM_RESP_LENGTH)) {
		    SETERROR(sparams->utils, "incorrect LMv1/v2 response");
		    result = SASL_BADAUTH;
		}
	    }
	}

	_plug_free_secret(sparams->utils, &password);

	if (result != SASL_OK) goto cleanup;
    }
    else {
	/* proxy the response */
	result = smb_session_setup(sparams->utils, text, authid, domain,
				   lm_resp, lm_resp_len, nt_resp, nt_resp_len);
	if (result != SASL_OK) goto cleanup;

	result = sparams->canon_user(sparams->utils->conn, authid, authid_len,
				     SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
	if (result != SASL_OK) goto cleanup;
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

  cleanup:
    if (lm_resp) sparams->utils->free(lm_resp);
    if (nt_resp) sparams->utils->free(nt_resp);
    if (domain) sparams->utils->free(domain);
    if (authid) sparams->utils->free(authid);

    return result;
}

static int ntlm_server_mech_step(void *conn_context,
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

    sparams->utils->log(NULL, SASL_LOG_DEBUG,
		       "NTLM server step %d\n", text->state);

    switch (text->state) {
	
    case 1:
	return ntlm_server_mech_step1(text, sparams, clientin, clientinlen,
				      serverout, serveroutlen, oparams);
	
    case 2:
	return ntlm_server_mech_step2(text, sparams, clientin, clientinlen,
				      serverout, serveroutlen, oparams);
	
    default:
	sparams->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid NTLM server step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static void ntlm_server_mech_dispose(void *conn_context,
				     const sasl_utils_t *utils)
{
    server_context_t *text = (server_context_t *) conn_context;
    
    if (!text) return;
    
    if (text->out_buf) utils->free(text->out_buf);
    if (text->sock != -1) closesocket(text->sock);

    utils->free(text);
}

static sasl_server_plug_t ntlm_server_plugins[] = 
{
    {
	"NTLM",				/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS,		/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_SUPPORTS_HTTP,	/* features */
	NULL,				/* glob_context */
	&ntlm_server_mech_new,		/* mech_new */
	&ntlm_server_mech_step,		/* mech_step */
	&ntlm_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech_avail */
	NULL				/* spare */
    }
};

int ntlm_server_plug_init(sasl_utils_t *utils,
			  int maxversion,
			  int *out_version,
			  sasl_server_plug_t **pluglist,
			  int *plugcount)
{
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	SETERROR(utils, "NTLM version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = ntlm_server_plugins;
    *plugcount = 1;
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

typedef struct client_context {
    int state;

    /* per-step mem management */
    char *out_buf;
    unsigned out_buf_len;

} client_context_t;

/*
 * Create a client request (type 1) consisting of:
 *
 * signature (8 bytes)
 * message type (uint32)
 * flags (uint32)
 * domain (buffer)
 * workstation (buffer)
 * data
 */
static int create_request(const sasl_utils_t *utils,
			  char **buf, unsigned *buflen,
			  const char *domain, const char *wkstn,
			  unsigned *outlen)
{
    uint32 flags = ( NTLM_USE_UNICODE | NTLM_USE_ASCII |
		     NTLM_ASK_TARGET | NTLM_AUTH_NTLM );
    uint32 offset = NTLM_TYPE1_DATA_OFFSET;
    u_char *base;

    *outlen = (unsigned) (offset + xstrlen(domain) + xstrlen(wkstn));
    if (_plug_buf_alloc(utils, buf, buflen, *outlen) != SASL_OK) {
	SETERROR(utils, "cannot allocate NTLM request");
	return SASL_NOMEM;
    }

    base = (unsigned char *) *buf;
    memset(base, 0, *outlen);
    memcpy(base + NTLM_SIG_OFFSET, NTLM_SIGNATURE, sizeof(NTLM_SIGNATURE));
    htoil(base + NTLM_TYPE_OFFSET, NTLM_TYPE_REQUEST);
    htoil(base + NTLM_TYPE1_FLAGS_OFFSET, flags);
    load_buffer(base + NTLM_TYPE1_DOMAIN_OFFSET,
		(const unsigned char *) domain, (uint16) xstrlen(domain), 0, base, &offset);
    load_buffer(base + NTLM_TYPE1_WORKSTN_OFFSET,
		(const unsigned char *) wkstn, (uint16) xstrlen(wkstn), 0, base, &offset);

    return SASL_OK;
}

/*
 * Create a client response (type 3) consisting of:
 *
 * signature (8 bytes)
 * message type (uint32)
 * LM/LMv2 response (buffer)
 * NTLM/NTLMv2 response (buffer)
 * domain (buffer)
 * user name (buffer)
 * workstation (buffer)
 * session key (buffer)
 * flags (uint32)
 * data
 */
static int create_response(const sasl_utils_t *utils,
			   char **buf, unsigned *buflen,
			   const u_char *lm_resp, const u_char *nt_resp,
			   const char *domain, const char *user,
			   const char *wkstn, const u_char *key,
			   uint32 flags, unsigned *outlen)
{
    uint32 offset = NTLM_TYPE3_DATA_OFFSET;
    u_char *base;

    if (!lm_resp && !nt_resp) {
	SETERROR(utils, "need at least one NT/LM response");
	return SASL_FAIL;
    }

    *outlen = (unsigned) (offset + (flags & NTLM_USE_UNICODE ? 2 : 1) * 
	(xstrlen(domain) + xstrlen(user) + xstrlen(wkstn)));
    if (lm_resp) *outlen += NTLM_RESP_LENGTH;
    if (nt_resp) *outlen += NTLM_RESP_LENGTH;
    if (key) *outlen += NTLM_SESSKEY_LENGTH;

    if (_plug_buf_alloc(utils, buf, buflen, *outlen) != SASL_OK) {
	SETERROR(utils, "cannot allocate NTLM response");
	return SASL_NOMEM;
    }

    base = (unsigned char *) *buf;
    memset(base, 0, *outlen);
    memcpy(base + NTLM_SIG_OFFSET, NTLM_SIGNATURE, sizeof(NTLM_SIGNATURE));
    htoil(base + NTLM_TYPE_OFFSET, NTLM_TYPE_RESPONSE);
    load_buffer(base + NTLM_TYPE3_LMRESP_OFFSET,
		lm_resp, lm_resp ? NTLM_RESP_LENGTH : 0, 0, base, &offset);
    load_buffer(base + NTLM_TYPE3_NTRESP_OFFSET,
		nt_resp, nt_resp ? NTLM_RESP_LENGTH : 0, 0, base, &offset);
    load_buffer(base + NTLM_TYPE3_DOMAIN_OFFSET,
		(const unsigned char *) ucase(domain, 0), (uint16) xstrlen(domain),
		flags & NTLM_USE_UNICODE,
		base, &offset);
    load_buffer(base + NTLM_TYPE3_USER_OFFSET,
		(const unsigned char *) user, (uint16) xstrlen(user),
		flags & NTLM_USE_UNICODE, base, &offset);
    load_buffer(base + NTLM_TYPE3_WORKSTN_OFFSET,
		(const unsigned char *) ucase(wkstn, 0), (uint16) xstrlen(wkstn),
		flags & NTLM_USE_UNICODE,
		base, &offset);
    load_buffer(base + NTLM_TYPE3_SESSIONKEY_OFFSET,
		key, key ? NTLM_SESSKEY_LENGTH : 0, 0, base, &offset);
    htoil(base + NTLM_TYPE3_FLAGS_OFFSET, flags);

    return SASL_OK;
}

static int ntlm_client_mech_new(void *glob_context __attribute__((unused)),
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

static int ntlm_client_mech_step1(client_context_t *text,
				  sasl_client_params_t *params,
				  const char *serverin __attribute__((unused)),
				  unsigned serverinlen __attribute__((unused)),
				  sasl_interact_t **prompt_need __attribute__((unused)),
				  const char **clientout,
				  unsigned *clientoutlen,
				  sasl_out_params_t *oparams __attribute__((unused)))
{
    int result;
    
    /* check if sec layer strong enough */
    if (params->props.min_ssf > params->external_ssf) {
	SETERROR(params->utils, "SSF requested of NTLM plugin");
	return SASL_TOOWEAK;
    }

    /* we don't care about domain or wkstn */
    result = create_request(params->utils, &text->out_buf, &text->out_buf_len,
			    NULL, NULL, clientoutlen);
    if (result != SASL_OK) return result;

    *clientout = text->out_buf;
    
    text->state = 2;
    
    return SASL_CONTINUE;
}

static int ntlm_client_mech_step2(client_context_t *text,
				  sasl_client_params_t *params,
				  const char *serverin,
				  unsigned serverinlen,
				  sasl_interact_t **prompt_need,
				  const char **clientout,
				  unsigned *clientoutlen,
				  sasl_out_params_t *oparams)
{
    const char *authid = NULL;
    sasl_secret_t *password = NULL;
    unsigned int free_password; /* set if we need to free password */
    char *domain = NULL;
    int auth_result = SASL_OK;
    int pass_result = SASL_OK;
    uint32 flags = 0;
    unsigned char hash[NTLM_HASH_LENGTH];
    unsigned char resp[NTLM_RESP_LENGTH], *lm_resp = NULL, *nt_resp = NULL;
    int result;
    const char *sendv2;

    if (!serverin || serverinlen < NTLM_TYPE2_MINSIZE ||
	memcmp(serverin, NTLM_SIGNATURE, sizeof(NTLM_SIGNATURE)) ||
	itohl(serverin + NTLM_TYPE_OFFSET) != NTLM_TYPE_CHALLENGE) {
	SETERROR(params->utils, "server didn't issue valid NTLM challenge");
	return SASL_BADPROT;
    }

    /* try to get the authid */
    if (oparams->authid == NULL) {
	auth_result = _plug_get_authid(params->utils, &authid, prompt_need);
	
	if ((auth_result != SASL_OK) && (auth_result != SASL_INTERACT))
	    return auth_result;
    }
    
    /* try to get the password */
    if (password == NULL) {
	pass_result = _plug_get_password(params->utils, &password,
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
    
    result = params->canon_user(params->utils->conn, authid, 0,
				SASL_CU_AUTHID | SASL_CU_AUTHZID, oparams);
    if (result != SASL_OK) goto cleanup;

    flags = itohl(serverin + NTLM_TYPE2_FLAGS_OFFSET);
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "server flags: %x", flags);

    flags &= NTLM_FLAGS_MASK; /* mask off the bits we don't support */

    result = unload_buffer(params->utils,
			   (const unsigned char *) serverin + NTLM_TYPE2_TARGET_OFFSET,
			   (u_char **) &domain, NULL,
			   flags & NTLM_USE_UNICODE,
			   (u_char *) serverin, serverinlen);
    if (result != SASL_OK) goto cleanup;
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "server domain: %s", domain);

    /* should we send a NTLMv2 response? */
    params->utils->getopt(params->utils->getopt_context,
			  "NTLM", "ntlm_v2", &sendv2, NULL);
    if (sendv2 &&
	(sendv2[0] == '1' || sendv2[0] == 'y' ||
	 (sendv2[0] == 'o' && sendv2[1] == 'n') || sendv2[0] == 't')) {

	/* put the cnonce in place after the LMv2 HMAC */
	char *cnonce = (char *) resp + MD5_DIGEST_LENGTH;

	params->utils->log(NULL, SASL_LOG_DEBUG,
			   "calculating LMv2 response");

	params->utils->rand(params->utils->rpool, cnonce, NTLM_NONCE_LENGTH);

	V2(resp, password, oparams->authid, domain,
	   (const unsigned char *) serverin + NTLM_TYPE2_CHALLENGE_OFFSET,
	   (const unsigned char *) cnonce, NTLM_NONCE_LENGTH,
	   params->utils, &text->out_buf, &text->out_buf_len, &result);

	lm_resp = resp;
    }
    else if (flags & NTLM_AUTH_NTLM) {
	params->utils->log(NULL, SASL_LOG_DEBUG,
			   "calculating NT response");
	P24(resp, P21(hash, password, P16_nt, params->utils,
		      &text->out_buf, &text->out_buf_len, &result),
	    (unsigned char *) serverin + NTLM_TYPE2_CHALLENGE_OFFSET);
	nt_resp = resp;
    }
    else {
	params->utils->log(NULL, SASL_LOG_DEBUG,
			   "calculating LM response");
	P24(resp, P21(hash, password, P16_lm, params->utils,
		      &text->out_buf, &text->out_buf_len, &result),
	    (unsigned char *) serverin + NTLM_TYPE2_CHALLENGE_OFFSET);
	lm_resp = resp;
    }
    if (result != SASL_OK) goto cleanup;

    /* we don't care about workstn or session key */
    result = create_response(params->utils, &text->out_buf, &text->out_buf_len,
			     lm_resp, nt_resp, domain, oparams->authid,
			     NULL, NULL, flags, clientoutlen);
    if (result != SASL_OK) goto cleanup;

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
    
    result = SASL_OK;

  cleanup:
    if (domain) params->utils->free(domain);
    if (free_password) _plug_free_secret(params->utils, &password);

    return result;
}

static int ntlm_client_mech_step(void *conn_context,
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
    
    params->utils->log(NULL, SASL_LOG_DEBUG,
		       "NTLM client step %d\n", text->state);

    switch (text->state) {
	
    case 1:
	return ntlm_client_mech_step1(text, params, serverin, serverinlen,
				      prompt_need, clientout, clientoutlen,
				      oparams);
	
    case 2:
	return ntlm_client_mech_step2(text, params, serverin, serverinlen,
				      prompt_need, clientout, clientoutlen,
				      oparams);
	
    default:
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid NTLM client step %d\n", text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static void ntlm_client_mech_dispose(void *conn_context,
				    const sasl_utils_t *utils)
{
    client_context_t *text = (client_context_t *) conn_context;
    
    if (!text) return;
    
    if (text->out_buf) utils->free(text->out_buf);
    
    utils->free(text);
}

static sasl_client_plug_t ntlm_client_plugins[] = 
{
    {
	"NTLM",				/* mech_name */
	0,				/* max_ssf */
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS,		/* security_flags */
	SASL_FEAT_WANT_CLIENT_FIRST
	| SASL_FEAT_SUPPORTS_HTTP,	/* features */
	NULL,				/* required_prompts */
	NULL,				/* glob_context */
	&ntlm_client_mech_new,		/* mech_new */
	&ntlm_client_mech_step,		/* mech_step */
	&ntlm_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int ntlm_client_plug_init(sasl_utils_t *utils,
			 int maxversion,
			 int *out_version,
			 sasl_client_plug_t **pluglist,
			 int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR(utils, "NTLM version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = ntlm_client_plugins;
    *plugcount = 1;
    
    return SASL_OK;
}
