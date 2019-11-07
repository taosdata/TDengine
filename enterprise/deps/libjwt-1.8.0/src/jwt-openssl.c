/* Copyright (C) 2015-2017 Ben Collins <ben@cyphre.com>
   This file is part of the JWT C Library

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the JWT Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/buffer.h>
#include <openssl/pem.h>

#include <jwt.h>

#include "jwt-private.h"
#include "config.h"

/* Routines to support crypto in LibJWT using OpenSSL. */

/* Functions to make libjwt backward compatible with OpenSSL version < 1.1.0
 * See https://wiki.openssl.org/index.php/1.1_API_Changes
 */
#if OPENSSL_VERSION_NUMBER < 0x10100000L

static void ECDSA_SIG_get0(const ECDSA_SIG *sig, const BIGNUM **pr, const BIGNUM **ps)
{
	if (pr != NULL)
		*pr = sig->r;
	if (ps != NULL)
		*ps = sig->s;
}

static int ECDSA_SIG_set0(ECDSA_SIG *sig, BIGNUM *r, BIGNUM *s)
{
	if (r == NULL || s == NULL)
		return 0;

	BN_clear_free(sig->r);
	BN_clear_free(sig->s);
	sig->r = r;
	sig->s = s;

	return 1;
}

#endif

int jwt_sign_sha_hmac(jwt_t *jwt, char **out, unsigned int *len,
		      const char *str)
{
	const EVP_MD *alg;

	switch (jwt->alg) {
        /* HMAC */
	case JWT_ALG_HS256:
		alg = EVP_sha256();
		break;
	case JWT_ALG_HS384:
		alg = EVP_sha384();
		break;
	case JWT_ALG_HS512:
		alg = EVP_sha512();
		break;
	default:
		return EINVAL;
	}

	*out = malloc(EVP_MAX_MD_SIZE);
	if (*out == NULL)
		return ENOMEM;

	HMAC(alg, jwt->key, jwt->key_len,
	     (const unsigned char *)str, strlen(str), (unsigned char *)*out,
	     len);

	return 0;
}

int jwt_verify_sha_hmac(jwt_t *jwt, const char *head, const char *sig)
{
	unsigned char res[EVP_MAX_MD_SIZE];
	BIO *bmem = NULL, *b64 = NULL;
	unsigned int res_len;
	const EVP_MD *alg;
	char *buf;
	int len, ret = EINVAL;

	switch (jwt->alg) {
	case JWT_ALG_HS256:
		alg = EVP_sha256();
		break;
	case JWT_ALG_HS384:
		alg = EVP_sha384();
		break;
	case JWT_ALG_HS512:
		alg = EVP_sha512();
		break;
	default:
		return EINVAL;
	}

	b64 = BIO_new(BIO_f_base64());
	if (b64 == NULL)
		return ENOMEM;

	bmem = BIO_new(BIO_s_mem());
	if (bmem == NULL) {
		BIO_free(b64);
		return ENOMEM;
	}

	BIO_push(b64, bmem);
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

	HMAC(alg, jwt->key, jwt->key_len,
	     (const unsigned char *)head, strlen(head), res, &res_len);

	BIO_write(b64, res, res_len);

	(void)BIO_flush(b64);

	len = BIO_pending(bmem);
	if (len < 0)
		goto jwt_verify_hmac_done;

	buf = alloca(len + 1);
	if (!buf) {
		ret = ENOMEM;
		goto jwt_verify_hmac_done;
	}

	len = BIO_read(bmem, buf, len);
	buf[len] = '\0';

	jwt_base64uri_encode(buf);

	/* And now... */
	ret = strcmp(buf, sig) ? EINVAL : 0;

jwt_verify_hmac_done:
	BIO_free_all(b64);

	return ret;
}

#define SIGN_ERROR(__err) ({ ret = __err; goto jwt_sign_sha_pem_done; })

int jwt_sign_sha_pem(jwt_t *jwt, char **out, unsigned int *len,
		     const char *str)
{
	EVP_MD_CTX *mdctx = NULL;
	ECDSA_SIG *ec_sig = NULL;
	const BIGNUM *ec_sig_r = NULL;
	const BIGNUM *ec_sig_s = NULL;
	BIO *bufkey = NULL;
	const EVP_MD *alg;
	int type;
	EVP_PKEY *pkey = NULL;
	int pkey_type;
	unsigned char *sig;
	int ret = 0;
	size_t slen;

	switch (jwt->alg) {
	/* RSA */
	case JWT_ALG_RS256:
		alg = EVP_sha256();
		type = EVP_PKEY_RSA;
		break;
	case JWT_ALG_RS384:
		alg = EVP_sha384();
		type = EVP_PKEY_RSA;
		break;
	case JWT_ALG_RS512:
		alg = EVP_sha512();
		type = EVP_PKEY_RSA;
		break;

	/* ECC */
	case JWT_ALG_ES256:
		alg = EVP_sha256();
		type = EVP_PKEY_EC;
		break;
	case JWT_ALG_ES384:
		alg = EVP_sha384();
		type = EVP_PKEY_EC;
		break;
	case JWT_ALG_ES512:
		alg = EVP_sha512();
		type = EVP_PKEY_EC;
		break;

	default:
		return EINVAL;
	}

	bufkey = BIO_new_mem_buf(jwt->key, jwt->key_len);
	if (bufkey == NULL)
		SIGN_ERROR(ENOMEM);

	/* This uses OpenSSL's default passphrase callback if needed. The
	 * library caller can override this in many ways, all of which are
	 * outside of the scope of LibJWT and this is documented in jwt.h. */
	pkey = PEM_read_bio_PrivateKey(bufkey, NULL, NULL, NULL);
	if (pkey == NULL)
		SIGN_ERROR(EINVAL);

	pkey_type = EVP_PKEY_id(pkey);
	if (pkey_type != type)
		SIGN_ERROR(EINVAL);

	mdctx = EVP_MD_CTX_create();
	if (mdctx == NULL)
		SIGN_ERROR(ENOMEM);

	/* Initialize the DigestSign operation using alg */
	if (EVP_DigestSignInit(mdctx, NULL, alg, NULL, pkey) != 1)
		SIGN_ERROR(EINVAL);

	/* Call update with the message */
	if (EVP_DigestSignUpdate(mdctx, str, strlen(str)) != 1)
		SIGN_ERROR(EINVAL);

	/* First, call EVP_DigestSignFinal with a NULL sig parameter to get length
	 * of sig. Length is returned in slen */
	if (EVP_DigestSignFinal(mdctx, NULL, &slen) != 1)
		SIGN_ERROR(EINVAL);

	/* Allocate memory for signature based on returned size */
	sig = alloca(slen);
	if (sig == NULL)
		SIGN_ERROR(ENOMEM);

	/* Get the signature */
	if (EVP_DigestSignFinal(mdctx, sig, &slen) != 1)
		SIGN_ERROR(EINVAL);

	if (pkey_type != EVP_PKEY_EC) {
		*out = malloc(slen);
		if (*out == NULL)
			SIGN_ERROR(ENOMEM);
		memcpy(*out, sig, slen);
		*len = slen;
	} else {
		unsigned int degree, bn_len, r_len, s_len, buf_len;
		unsigned char *raw_buf;
		EC_KEY *ec_key;

		/* For EC we need to convert to a raw format of R/S. */

		/* Get the actual ec_key */
		ec_key = EVP_PKEY_get1_EC_KEY(pkey);
		if (ec_key == NULL)
			SIGN_ERROR(ENOMEM);

		degree = EC_GROUP_get_degree(EC_KEY_get0_group(ec_key));

		EC_KEY_free(ec_key);

		/* Get the sig from the DER encoded version. */
		ec_sig = d2i_ECDSA_SIG(NULL, (const unsigned char **)&sig, slen);
		if (ec_sig == NULL)
			SIGN_ERROR(ENOMEM);

		ECDSA_SIG_get0(ec_sig, &ec_sig_r, &ec_sig_s);
		r_len = BN_num_bytes(ec_sig_r);
		s_len = BN_num_bytes(ec_sig_s);
		bn_len = (degree + 7) / 8;
		if ((r_len > bn_len) || (s_len > bn_len))
			SIGN_ERROR(EINVAL);

		buf_len = 2 * bn_len;
		raw_buf = alloca(buf_len);
		if (raw_buf == NULL)
			SIGN_ERROR(ENOMEM);

		/* Pad the bignums with leading zeroes. */
		memset(raw_buf, 0, buf_len);
		BN_bn2bin(ec_sig_r, raw_buf + bn_len - r_len);
		BN_bn2bin(ec_sig_s, raw_buf + buf_len - s_len);

		*out = malloc(buf_len);
		if (*out == NULL)
			SIGN_ERROR(ENOMEM);
		memcpy(*out, raw_buf, buf_len);
		*len = buf_len;
	}

jwt_sign_sha_pem_done:
	if (bufkey)
		BIO_free(bufkey);
	if (pkey)
		EVP_PKEY_free(pkey);
	if (mdctx)
		EVP_MD_CTX_destroy(mdctx);
	if (ec_sig)
		ECDSA_SIG_free(ec_sig);

	return ret;
}

#define VERIFY_ERROR(__err) ({ ret = __err; goto jwt_verify_sha_pem_done; })

int jwt_verify_sha_pem(jwt_t *jwt, const char *head, const char *sig_b64)
{
	unsigned char *sig = NULL;
	EVP_MD_CTX *mdctx = NULL;
	ECDSA_SIG *ec_sig = NULL;
	BIGNUM *ec_sig_r = NULL;
	BIGNUM *ec_sig_s = NULL;
	EVP_PKEY *pkey = NULL;
	const EVP_MD *alg;
	int type;
	int pkey_type;
	BIO *bufkey = NULL;
	int ret = 0;
	int slen;

	switch (jwt->alg) {
	/* RSA */
	case JWT_ALG_RS256:
		alg = EVP_sha256();
		type = EVP_PKEY_RSA;
		break;
	case JWT_ALG_RS384:
		alg = EVP_sha384();
		type = EVP_PKEY_RSA;
		break;
	case JWT_ALG_RS512:
		alg = EVP_sha512();
		type = EVP_PKEY_RSA;
		break;

	/* ECC */
	case JWT_ALG_ES256:
		alg = EVP_sha256();
		type = EVP_PKEY_EC;
		break;
	case JWT_ALG_ES384:
		alg = EVP_sha384();
		type = EVP_PKEY_EC;
		break;
	case JWT_ALG_ES512:
		alg = EVP_sha512();
		type = EVP_PKEY_EC;
		break;

	default:
		return EINVAL;
	}

	sig = jwt_b64_decode(sig_b64, &slen);
	if (sig == NULL)
		VERIFY_ERROR(EINVAL);

	bufkey = BIO_new_mem_buf(jwt->key, jwt->key_len);
	if (bufkey == NULL)
		VERIFY_ERROR(ENOMEM);

	/* This uses OpenSSL's default passphrase callback if needed. The
	 * library caller can override this in many ways, all of which are
	 * outside of the scope of LibJWT and this is documented in jwt.h. */
	pkey = PEM_read_bio_PUBKEY(bufkey, NULL, NULL, NULL);
	if (pkey == NULL)
		VERIFY_ERROR(EINVAL);

	pkey_type = EVP_PKEY_id(pkey);
	if (pkey_type != type)
		VERIFY_ERROR(EINVAL);

	/* Convert EC sigs back to ASN1. */
	if (pkey_type == EVP_PKEY_EC) {
		unsigned int degree, bn_len;
		unsigned char *p;
		EC_KEY *ec_key;

		ec_sig = ECDSA_SIG_new();
		if (ec_sig == NULL)
			VERIFY_ERROR(ENOMEM);

		/* Get the actual ec_key */
		ec_key = EVP_PKEY_get1_EC_KEY(pkey);
		if (ec_key == NULL)
			VERIFY_ERROR(ENOMEM);

		degree = EC_GROUP_get_degree(EC_KEY_get0_group(ec_key));

		EC_KEY_free(ec_key);

		bn_len = (degree + 7) / 8;
		if ((bn_len * 2) != slen)
			VERIFY_ERROR(EINVAL);

		ec_sig_r = BN_bin2bn(sig, bn_len, NULL);
		ec_sig_s = BN_bin2bn(sig + bn_len, bn_len, NULL);
		if (ec_sig_r  == NULL || ec_sig_s == NULL)
			VERIFY_ERROR(EINVAL);

		ECDSA_SIG_set0(ec_sig, ec_sig_r, ec_sig_s);
		free(sig);

		slen = i2d_ECDSA_SIG(ec_sig, NULL);
		sig = malloc(slen);
		if (sig == NULL)
			VERIFY_ERROR(ENOMEM);

		p = sig;
		slen = i2d_ECDSA_SIG(ec_sig, &p);

		if (slen == 0)
			VERIFY_ERROR(EINVAL);
	}

	mdctx = EVP_MD_CTX_create();
	if (mdctx == NULL)
		VERIFY_ERROR(ENOMEM);

	/* Initialize the DigestVerify operation using alg */
	if (EVP_DigestVerifyInit(mdctx, NULL, alg, NULL, pkey) != 1)
		VERIFY_ERROR(EINVAL);

	/* Call update with the message */
	if (EVP_DigestVerifyUpdate(mdctx, head, strlen(head)) != 1)
		VERIFY_ERROR(EINVAL);

	/* Now check the sig for validity. */
	if (EVP_DigestVerifyFinal(mdctx, sig, slen) != 1)
		VERIFY_ERROR(EINVAL);

jwt_verify_sha_pem_done:
	if (bufkey)
		BIO_free(bufkey);
	if (pkey)
		EVP_PKEY_free(pkey);
	if (mdctx)
		EVP_MD_CTX_destroy(mdctx);
	if (sig)
		free(sig);
	if (ec_sig)
		ECDSA_SIG_free(ec_sig);

	return ret;
}
