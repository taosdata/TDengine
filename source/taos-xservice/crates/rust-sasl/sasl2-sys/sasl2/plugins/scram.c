/* SCRAM-SHA-1/SHA-2 SASL plugin
 * Alexey Melnikov
 */
/* 
 * Copyright (c) 2009-2016 Carnegie Mellon University.  All rights reserved.
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
#include <errno.h>

#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>

#include "plugin_common.h"

#ifdef macintosh
#include <sasl_scram_plugin_decl.h>
#endif

#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

/*****************************  Common Section  *****************************/

#define NONCE_SIZE (32)		    /* arbitrary */
#define SALT_SIZE  (16)		    /* arbitrary */

/* TODO: make this a configurable option? */
#define DEFAULT_ITERATION_COUNTER   4096
#define MIN_ITERATION_COUNTER	    4096

#define MAX_ITERATION_COUNTER	    0x10000

/* maximum length of the iteration_counter (as a string). Assume it is 32bits */
#define ITERATION_COUNTER_BUF_LEN   20

#define BASE64_LEN(size)	    (((size) / 3 * 4) + (((size) % 3) ? 4 : 0))

#define MAX_CLIENTIN_LEN	    2048
#define MAX_SERVERIN_LEN	    2048

#define STRINGIZE(x)		    #x
#define MAX_CLIENTIN_LEN_STR	    STRINGIZE((MAX_CLIENTIN_LEN))
#define MAX_SERVERIN_LEN_STR	    STRINGIZE((MAX_SERVERIN_LEN))

#define CLIENT_KEY_CONSTANT	    "Client Key"
#define SERVER_KEY_CONSTANT	    "Server Key"
#define CLIENT_KEY_CONSTANT_LEN	    sizeof(CLIENT_KEY_CONSTANT)-1
#define SERVER_KEY_CONSTANT_LEN	    sizeof(SERVER_KEY_CONSTANT)-1

#define SCRAM_CB_FLAG_MASK    0x0F
#define SCRAM_CB_FLAG_N       0x00
#define SCRAM_CB_FLAG_P       0x01
#define SCRAM_CB_FLAG_Y       0x02

#ifdef SCRAM_DEBUG
#define PRINT_HASH(func,hash,size)  print_hash(func,hash,size)
#else
#define PRINT_HASH(func,hash,size)
#endif

/* NB: A temporary mapping for "internal errors". It would be better to add
   a new SASL error code for that */
#define SASL_SCRAM_INTERNAL	    SASL_NOMEM


/* Holds the core salt to avoid regenerating salt each auth. */
static unsigned char g_salt_key[SALT_SIZE];

/* Note that currently only SHA-* variants are supported! */
static const char *
scram_sasl_mech_name(size_t hash_size)
{
    switch (hash_size) {
    case 64:
	return "SCRAM-SHA-512";

    case 48:
	return "SCRAM-SHA-384";

    case 32:
	return "SCRAM-SHA-256";

    case 28:
	return "SCRAM-SHA-224";

    case 20:
	return "SCRAM-SHA-1";
    }

    return NULL;
}

/* Convert saslname = 1*(value-safe-char / "=2C" / "=3D") in place.
   Returns SASL_FAIL if the encoding is invalid, otherwise SASL_OK */
static int
decode_saslname (char *buf)
{
    char * inp;
    char * outp;

    inp = outp = buf;

    while (*inp) {
	if (*inp == '=') {
	    inp++;
	    if (*inp == '\0') {
		return SASL_FAIL;
	    }
	    if (inp[0] == '2' && inp[1] == 'C') {
		*outp = ',';
		inp += 2;
	    } else if (inp[0] == '3' && inp[1] == 'D') {
		*outp = '=';
		inp += 2;
	    } else {
		return SASL_FAIL;
	    }
	} else {
	    *outp = *inp;
	    inp++;
	}
	outp++;
    }

    *outp = '\0';

    return SASL_OK;
}

/* Convert a username to saslname = 1*(value-safe-char / "=2C" / "=3D")
   and return an allocated copy.
   "freeme" contains pointer to the allocated output, or NULL,
   if encoded_saslname just points to saslname.
   Returns SASL_NOMEM if can't allocate memory for the output, otherwise SASL_OK */
static int
encode_saslname (const char *saslname,
		 const char **encoded_saslname,
		 char **freeme)
{
    const char * inp;
    char * outp;
    int special_chars = 0;

    /* Found out if anything needs encoding */
    for (inp = saslname; *inp; inp++) {
	if (*inp == ',' || *inp == '=') {
	    special_chars++;
	}
    }

    if (special_chars == 0) {
	*encoded_saslname = saslname;
	*freeme = NULL;
	return SASL_OK;
    }

    outp = malloc(strlen(saslname) + special_chars * 2 + 1);
    *encoded_saslname = outp;
    *freeme = outp;
    if (outp == NULL) {
	return SASL_NOMEM;
    }

    for (inp = saslname; *inp; inp++) {
	switch (*inp) {
	case ',':
	    *outp++ = '=';
	    *outp++ = '2';
	    *outp++ = 'C';
	    break;

	case '=':
	    *outp++ = '=';
	    *outp++ = '3';
	    *outp++ = 'D';
	    break;

	default:
	    *outp++ = *inp;
	}
    }

    *outp = '\0';

    return SASL_OK;
}

static char *
create_nonce(const sasl_utils_t * utils,
	     char *buffer,
	     size_t buflen)	    /* Including the terminating NUL */
{
    char *intbuf;
    unsigned int estimated;

    if ((buflen - 1) % 4 != 0) {
	/* NB: the algorithm below doesn't work for such length.
	   It needs to be adjusted to allocate + 4 bytes,
	   encode the last 4 bytes to a separate buffer and
	   then copy the necessary number of bytes to the end of the output */
	return NULL;
    }

    estimated = (unsigned int)((buflen - 1) / 4 * 3);
    intbuf = (char *) utils->malloc(estimated + 1);
    if (intbuf == NULL) {
	return NULL;
    }

    utils->rand(utils->rpool, intbuf, estimated);
    
    /* base 64 encode it so it has valid chars */
    if (utils->encode64(intbuf,
			estimated,
			buffer,
			(unsigned int)buflen,
			NULL) != SASL_OK) {
	utils->free(intbuf);
	return NULL;
    }

    utils->free(intbuf);

    buffer[buflen-1] = '\0';

    return buffer;
}

#ifdef SCRAM_DEBUG
/* Useful for debugging interop issues */
static void
print_hash (const char * func, const char * hash, size_t hash_size)
{
    int i;

    printf (" HASH in %s:", func);
    for (i = 0; i < hash_size; i++) {
	printf (" %.2X", (unsigned char)hash[i]);
    }
    printf ("\n");
}
#endif


/* The result variable need to point to a buffer big enough for the [SHA-*] hash */
static void
Hi (const sasl_utils_t * utils,
    const EVP_MD *md,
    const char * str,
    size_t str_len,
    const char * salt,
    size_t salt_len,
    unsigned int iteration_count,
    char * result)
{
    char * initial_key = NULL;
    unsigned int i;
    char * temp_result;
    unsigned int hash_len = 0;
    size_t k, hash_size = EVP_MD_size(md);

    initial_key = utils->malloc(salt_len + 4);
    memcpy (initial_key, salt, salt_len);
    initial_key[salt_len] = 0;
    initial_key[salt_len+1] = 0;
    initial_key[salt_len+2] = 0;
    initial_key[salt_len+3] = 1;

    temp_result = utils->malloc(hash_size);

    /* U1   := HMAC(str, salt || INT(1)) */

    if (HMAC(md,
	     (const unsigned char *) str,
	     (int)str_len,
	     (const unsigned char *) initial_key,
	     (int)salt_len + 4,
             (unsigned char *)result,
	     &hash_len) == NULL) {
    }

    memcpy(temp_result, result, hash_size);

    PRINT_HASH ("first HMAC in Hi()", temp_result, hash_size);

    /* On each loop iteration j "temp_result" contains Uj,
       while "result" contains "U1 XOR ... XOR Uj" */
    for (i = 2; i <= iteration_count; i++) {
	if (HMAC(md,
		 (const unsigned char *) str,
		 (int)str_len,
		 (const unsigned char *) temp_result,
		 hash_size,
		 (unsigned char *)temp_result,
		 &hash_len) == NULL) {
	}

	PRINT_HASH ("Hi() HMAC inside loop", temp_result, hash_size);

	for (k = 0; k < hash_size; k++) {
	    result[k] ^= temp_result[k];
	}

	PRINT_HASH ("Hi() - accumulated result inside loop", result, hash_size);
    }

    utils->free(initial_key);
    utils->free(temp_result);
}

/**
 * User salt is Hi(username,salt_key);
 * This is fixed per reboot, to allow caching of SCRAM
 * SaltedPassword.
 */
static unsigned char *
scram_server_user_salt(const sasl_utils_t * utils,
                       const EVP_MD *md,
                       const char * username,
		       size_t * p_salt_len)
{
    size_t hash_size = EVP_MD_size(md);
    char * result = utils->malloc(hash_size);
    Hi(utils, md, username, strlen(username), (const char *) g_salt_key, SALT_SIZE,
        20 /* iterations */, result);
    *p_salt_len = hash_size;
    return (unsigned char *) result;
}

static int
GenerateScramSecrets (const sasl_utils_t * utils,
                      const EVP_MD *md,
		      const char * password,
		      size_t password_len,
		      char * salt,
		      size_t salt_len,
		      unsigned int iteration_count,
		      char * StoredKey,
		      char * ServerKey,
		      char ** error_text)
{
    char SaltedPassword[EVP_MAX_MD_SIZE];
    char ClientKey[EVP_MAX_MD_SIZE];
    sasl_secret_t *sec = NULL;
    unsigned int hash_len = 0;
    int result;
    size_t hash_size = EVP_MD_size(md);

    *error_text = NULL;

    if (password_len == 0) {
	*error_text = "empty secret";
	result = SASL_FAIL;
	goto cleanup;
    }

    sec = utils->malloc(sizeof(sasl_secret_t) + password_len);
    if (sec == NULL) {
	result = SASL_NOMEM;
	goto cleanup;
    }
	
    sec->len = (unsigned) password_len;
    strncpy((char *)sec->data, password, password_len + 1);

    /* SaltedPassword  := Hi(password, salt) */
    Hi (utils,
        md,
	(const char *) sec->data,
	sec->len,
	salt,
	salt_len,
	iteration_count,
	SaltedPassword);

    /* ClientKey       := HMAC(SaltedPassword, "Client Key") */
    if (HMAC(md,
	     (const unsigned char *) SaltedPassword,
	     hash_size,
	     (const unsigned char *) CLIENT_KEY_CONSTANT,
	     CLIENT_KEY_CONSTANT_LEN,
	     (unsigned char *)ClientKey,
	     &hash_len) == NULL) {
	*error_text = "HMAC call failed";
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    /* StoredKey       := H(ClientKey) */
    if (EVP_Digest((const unsigned char *) ClientKey, hash_size,
                   (unsigned char *) StoredKey, NULL, md, NULL) == 0) {
	*error_text = "Digest call failed";
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;

    }

    /* ServerKey       := HMAC(SaltedPassword, "Server Key") */
    if (HMAC(md,
	     (const unsigned char *) SaltedPassword,
	     hash_size,
	     (const unsigned char *) SERVER_KEY_CONSTANT,
	     SERVER_KEY_CONSTANT_LEN,
	     (unsigned char *)ServerKey,
	     &hash_len) == NULL) {
	*error_text = "HMAC call failed";
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    result = SASL_OK;

cleanup:
    if (sec) {
	_plug_free_secret(utils, &sec);
    }
    return result;
}

/*****************************  Server Section  *****************************/

typedef struct server_context {
    int state;

    const EVP_MD *md;		/* underlying MDA */

    char * authentication_id;
    char * authorization_id;

    char * out_buf;
    unsigned out_buf_len;
    char * auth_message;
    size_t auth_message_len;
    char * nonce;
    /* in binary form */
    char * salt;
    size_t salt_len;
    unsigned int iteration_count;
    char StoredKey[EVP_MAX_MD_SIZE + 1];
    char ServerKey[EVP_MAX_MD_SIZE + 1];

    int cb_flags;
    char *cbindingname;
    char *gs2_header;
    size_t gs2_header_length;
} server_context_t;

static int
scram_server_mech_new(void *glob_context,
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
    /* text->state = 0; */
    
    text->md = EVP_get_digestbyname((const char *) glob_context);

    *conn_context = text;
    
    return SASL_OK;
}

static int
scram_server_mech_step1(server_context_t *text,
			sasl_server_params_t *sparams,
			const char *clientin,
			unsigned clientinlen,
			const char **serverout,
			unsigned *serveroutlen,
			sasl_out_params_t *oparams __attribute__((unused)))
{
    char * authorization_id;
    char * authentication_id;
    char * p;
    char * nonce;
    size_t client_nonce_len;
    char * base64_salt = NULL;
    size_t base64len;
    size_t estimated_challenge_len;
    size_t pure_scram_length;
    char * inbuf = NULL;
    const char *password_request[] = { SASL_AUX_PASSWORD,
				       "*authPassword",
				       NULL };
    int canon_flags;
    struct propval auxprop_values[3];
    int result;
    size_t hash_size = EVP_MD_size(text->md);
    const char *scram_sasl_mech = scram_sasl_mech_name(hash_size);

    if (clientinlen == 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "%s input expected", scram_sasl_mech);
	return SASL_BADPROT;
    }

    /* Expecting: 'gs2-cbind-flag "," [ authzid ] "," [reserved-mext ","]
		   username "," nonce ["," extensions]' */

    if (clientinlen < 10) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid %s input", scram_sasl_mech);
	return SASL_BADPROT;
    }

    inbuf = sparams->utils->malloc (clientinlen + 1);

    if (inbuf == NULL) {
	MEMERROR( sparams->utils );
	return SASL_NOMEM;
    }

    memcpy(inbuf, clientin, clientinlen);
    inbuf[clientinlen] = 0;

    if (strlen(inbuf) != clientinlen) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "NULs found in %s input", scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    p = inbuf;

    /* gs2-cbind-flag  = "p=" cb-name / "n" / "y"
                         ;; "n" -> client doesn't support channel binding
                         ;; "y" -> client does support channel binding
                         ;;        but thinks the server does not.
                         ;; "p" -> client requires channel binding.
                         ;; The selected channel binding follows "p=". */
    switch (p[0]) {
	case 'p':
	    if (p[1] != '=') {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "The initial 'p' needs to be followed by '=' in %s input",
                                         scram_sasl_mech);
		result = SASL_BADPROT;
		goto cleanup;
	    }
	    p++;

	    text->cbindingname = p + 1;
	    p = strchr (p, ',');
	    if (p == NULL) {
		text->cbindingname = NULL;

		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Channel binding name must be terminated by a comma in %s input",
                                         scram_sasl_mech);
		result = SASL_BADPROT;
		goto cleanup;
	    }

	    *p = '\0';
	    _plug_strdup(sparams->utils, text->cbindingname, &text->cbindingname, NULL);
	    *p = ',';

	    text->cb_flags = SCRAM_CB_FLAG_P;
	    break;

	case 'n':
	    text->cb_flags = SCRAM_CB_FLAG_N;
	    /* We always have at least 10 bytes, so this is safe */
	    p++;
	    break;

	case 'y':
	    text->cb_flags = SCRAM_CB_FLAG_Y;
	    /* We always have at least 10 bytes, so this is safe */
	    p++;
	    break;

	default:
	    sparams->utils->seterror(sparams->utils->conn, 0,
                                     "The initial %s client response needs to start with 'y', 'n' or 'p'",
                                     scram_sasl_mech);
	    result = SASL_BADPROT;
	    goto cleanup;
    }

    if (p[0] != ',') {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "',' expected in %s input", scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }
    p++;

    if (p[0] == 'a' && p[1] == '=') {
        authorization_id = p + 2;

	p = strchr (authorization_id, ',');
	if (p == NULL) {
	    sparams->utils->seterror(sparams->utils->conn, 0,
                                     "At least nonce is expected in %s input",
                                     scram_sasl_mech);
	    result = SASL_BADPROT;
	    goto cleanup;
	}

	/* End of the GS2 header */
	p[0] = '\0';
	/* The GS2 header length DOES include the terminating comma */
	text->gs2_header_length = p - inbuf + 1;

	p++;

	/* Make a read-write copy we can modify */
	_plug_strdup(sparams->utils, authorization_id, &text->authorization_id, NULL);

	if (decode_saslname(text->authorization_id) != SASL_OK) {
	    sparams->utils->seterror(sparams->utils->conn, 0,
                                     "Invalid authorization identity encoding in %s input",
                                     scram_sasl_mech);
	    result = SASL_BADPROT;
	    goto cleanup;
	}
    } else if (p[0] != ',') {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "',' expected in %s input", scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    } else {
	/* End of the GS2 header */
	p[0] = '\0';
	/* The GS2 header length DOES include the terminating comma */
	text->gs2_header_length = p - inbuf + 1;

	p++;
    }

    text->gs2_header = sparams->utils->malloc (text->gs2_header_length + 1);
    if (text->gs2_header == NULL) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    memcpy(text->gs2_header, inbuf, text->gs2_header_length - 1);
    /* Remember the comma */
    text->gs2_header[text->gs2_header_length - 1] = ',';
    text->gs2_header[text->gs2_header_length] = 0;



    if (p[1] != '=') {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid %s input", scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (p[0] == 'm') {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Unsupported mandatory extension to %s",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (p[0] != 'n') {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Username (n=) expected in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    authentication_id = p + 2;
    p = strchr (authentication_id, ',');

    /* MUST be followed by a nonce */
    if (p == NULL) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Nonce expected after the username in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    *p = '\0';
    p++;

    if (decode_saslname(authentication_id) != SASL_OK) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid username encoding in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    _plug_strdup(sparams->utils, authentication_id, &text->authentication_id, NULL);

    if (strncmp(p, "r=", 2) != 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Nonce expected after the username in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    p += 2;
    nonce = p;
    p = strchr (nonce, ',');

    if (p == NULL) {
	p = nonce + strlen(nonce);
    } else {
	*p = '\0';
    }

    /* Generate server nonce, by appending some random stuff to the client nonce */
    client_nonce_len = strlen(nonce);
    text->nonce = sparams->utils->malloc (client_nonce_len + NONCE_SIZE + 1);

    if (text->nonce == NULL) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    strcpy (text->nonce, nonce);

    if (create_nonce(sparams->utils,
		     text->nonce + client_nonce_len,
		     NONCE_SIZE + 1) == NULL) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }



    /* Now we fetch user's password and calculate our secret */
    result = sparams->utils->prop_request(sparams->propctx, password_request);
    if (result != SASL_OK) {
	goto cleanup;
    }
    
    /* this will trigger the getting of the aux properties */
    canon_flags = SASL_CU_AUTHID;
    if (text->authorization_id == NULL || *text->authorization_id == '\0') {
	canon_flags |= SASL_CU_AUTHZID;
    }

    result = sparams->canon_user(sparams->utils->conn,
				 text->authentication_id,
				 0,
				 canon_flags,
				 oparams);
    if (result != SASL_OK) {
	SETERROR(sparams->utils, "unable to canonify user and get auxprops");
	goto cleanup;
    }
    
    if (text->authorization_id != NULL && *text->authorization_id != '\0') {
	result = sparams->canon_user(sparams->utils->conn,
				     text->authorization_id,
				     0,
				     SASL_CU_AUTHZID,
				     oparams);
    }
    if (result != SASL_OK) {
	SETERROR(sparams->utils, "unable to canonify authorization ID");
	goto cleanup;
    }

    result = sparams->utils->prop_getnames(sparams->propctx,
					   password_request,
					   auxprop_values);
    if (result < 0 ||
	((!auxprop_values[0].name || !auxprop_values[0].values) &&
	 (!auxprop_values[1].name || !auxprop_values[1].values))) {
	/* We didn't find this username */
	sparams->utils->seterror(sparams->utils->conn,0,
				 "no secret in database");
	result = sparams->transition ? SASL_TRANS : SASL_NOUSER;
	goto cleanup;
    }

    if (auxprop_values[0].name && auxprop_values[0].values) {
	char * error_text = NULL;
	char * s_iteration_count;
	char * end;

	text->salt = (char *) scram_server_user_salt(sparams->utils, text->md, text->authentication_id, &text->salt_len);

	sparams->utils->getopt(sparams->utils->getopt_context,
			       /* Different SCRAM hashes can have different strengh */
			       scram_sasl_mech,
			       "scram_iteration_counter",
			       (const char **) &s_iteration_count,
			       NULL);

	if (s_iteration_count != NULL) {
	    errno = 0;
	    text->iteration_count = strtoul(s_iteration_count, &end, 10);
	    if (s_iteration_count == end || *end != '\0' || errno != 0) {
		sparams->utils->log(NULL,
				    SASL_LOG_DEBUG,
				    "Invalid iteration-count in scram_iteration_count SASL option: not a number. Using the default instead.");
		s_iteration_count = NULL;
	    }
	}

	if (s_iteration_count == NULL) {
	    text->iteration_count = DEFAULT_ITERATION_COUNTER;
	}

	result = GenerateScramSecrets (sparams->utils,
                                       text->md,
				       auxprop_values[0].values[0],
				       strlen(auxprop_values[0].values[0]),
				       text->salt,
				       text->salt_len,
				       text->iteration_count,
				       text->StoredKey,
				       text->ServerKey,
				       &error_text);
	if (result != SASL_OK) {
	    if (error_text != NULL) {
		sparams->utils->seterror(sparams->utils->conn, 0, "%s",
					 error_text);
	    }
	    goto cleanup;
	}

    } else if (auxprop_values[1].name && auxprop_values[1].values) {
	char s_iteration_count[ITERATION_COUNTER_BUF_LEN+1];
	size_t base64_salt_len;
	unsigned int exact_key_len;
	const char * scram_hash;
	const char * p_field;
	char * end;
	int i;
        size_t scram_sasl_mech_len = strlen(scram_sasl_mech);

	result = SASL_SCRAM_INTERNAL;

	for (i = 0; auxprop_values[1].values[i] != NULL; i++) {
	    scram_hash = auxprop_values[1].values[i];

	    /* Skip the leading spaces */
	    while (*scram_hash == ' ') {
		scram_hash++;
	    }

	    if (strncmp(scram_hash, scram_sasl_mech, scram_sasl_mech_len) != 0) {
		continue;
	    }
	    scram_hash += scram_sasl_mech_len;

	    /* Skip spaces */
	    while (*scram_hash == ' ') {
		scram_hash++;
	    }

	    if (*scram_hash != '$') {
		/* syntax error, ignore the value */
		continue;
	    }
	    scram_hash++;

	    /* Skip spaces */
	    while (*scram_hash == ' ') {
		scram_hash++;
	    }

	    p_field = strchr(scram_hash, ':');
	    if (p_field == NULL || p_field == scram_hash) {
		/* syntax error, ignore the value */
		continue;
	    }

	    if ((p_field - scram_hash) > ITERATION_COUNTER_BUF_LEN) {
		/* The iteration counter is too big for us */
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid iteration-count in %s input: the value is too big",
                                         scram_sasl_mech);
		continue;
	    }

	    memcpy(s_iteration_count, scram_hash, p_field - scram_hash);
	    s_iteration_count[p_field - scram_hash] = '\0';

	    errno = 0;
	    text->iteration_count = strtoul(s_iteration_count, &end, 10);
	    if (s_iteration_count == end || *end != '\0' || errno != 0) {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid iteration-count in %s input: not a number",
                                         scram_sasl_mech);
		continue;
	    }

	    scram_hash = p_field + 1;

	    p_field = scram_hash + strcspn(scram_hash, "$ ");
	    if (p_field == scram_hash || *p_field == '\0') {
		/* syntax error, ignore the value */
		continue;
	    }

	    base64_salt_len = p_field - scram_hash;
	    text->salt = (char *) sparams->utils->malloc(base64_salt_len);
	    if (sparams->utils->decode64(scram_hash,
					 (unsigned int)base64_salt_len,
					 text->salt,
					 (unsigned int)base64_salt_len,
					 (unsigned int *) &text->salt_len) != SASL_OK) {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid base64 encoding of the salt in %s stored value",
                                         scram_sasl_mech);
		continue;
	    }

	    scram_hash = p_field;

	    /* Skip spaces */
	    while (*scram_hash == ' ') {
		scram_hash++;
	    }

	    if (*scram_hash != '$') {
		/* syntax error, ignore the value */
		sparams->utils->free(text->salt);
		text->salt = NULL;
		continue;
	    }
	    scram_hash++;

	    /* Skip spaces */
	    while (*scram_hash == ' ') {
		scram_hash++;
	    }

	    p_field = strchr(scram_hash, ':');
	    if (p_field == NULL || p_field == scram_hash) {
		/* syntax error, ignore the value */
		sparams->utils->free(text->salt);
		text->salt = NULL;
		continue;
	    }

	    if (sparams->utils->decode64(scram_hash,
					 (unsigned int)(p_field - scram_hash),
					 text->StoredKey,
					 hash_size + 1,
					 &exact_key_len) != SASL_OK) {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid base64 encoding of StoredKey in %s per-user storage",
                                         scram_sasl_mech);
		sparams->utils->free(text->salt);
		text->salt = NULL;
		continue;
	    }

	    if (exact_key_len != hash_size) {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid StoredKey in %s per-user storage",
                                         scram_sasl_mech);
		sparams->utils->free(text->salt);
		text->salt = NULL;
		continue;
	    }

	    scram_hash = p_field + 1;

	    p_field = strchr(scram_hash, ' ');
	    if (p_field == NULL) {
		p_field = scram_hash + strlen(scram_hash);
	    }


	    if (sparams->utils->decode64(scram_hash,
					 (unsigned int)(p_field - scram_hash),
					 text->ServerKey,
					 hash_size + 1,
					 &exact_key_len) != SASL_OK) {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid base64 encoding of ServerKey in %s per-user storage",
                                         scram_sasl_mech);
		sparams->utils->free(text->salt);
		text->salt = NULL;
		continue;
	    }

	    if (exact_key_len != hash_size) {
		sparams->utils->seterror(sparams->utils->conn, 0,
                                         "Invalid ServerKey in %s per-user storage", scram_sasl_mech);
		sparams->utils->free(text->salt);
		text->salt = NULL;
		continue;
	    }

	    result = SASL_OK;
	    break;
	}

	if (result != SASL_OK) {
	    sparams->utils->seterror(sparams->utils->conn,
				     0, "No valid %s secret found",
                                     scram_sasl_mech);
	    goto cleanup;
	}

    } else {
	sparams->utils->seterror(sparams->utils->conn,
				 0,
				 "Have neither type of secret");
	return SASL_FAIL;
    }
    
    /* erase the plaintext password */
    sparams->utils->prop_erase(sparams->propctx, password_request[0]);



    /* base 64 encode it so it has valid chars */
    base64len = (text->salt_len / 3 * 4) + ((text->salt_len % 3) ? 4 : 0);
    
    base64_salt = (char *) sparams->utils->malloc(base64len + 1);
    if (base64_salt == NULL) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }
    
    /*
     * Returns SASL_OK on success, SASL_BUFOVER if result won't fit
     */
    if (sparams->utils->encode64(text->salt,
				 (unsigned int)text->salt_len,
				 base64_salt,
				 (unsigned int)base64len + 1,
				 NULL) != SASL_OK) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    base64_salt[base64len] = '\0';

    /* Now we generate server challenge */
    estimated_challenge_len = client_nonce_len + NONCE_SIZE +
			      base64len +
			      ITERATION_COUNTER_BUF_LEN +
			      strlen("r=,s=,i=");
    result = _plug_buf_alloc(sparams->utils,
			     &(text->out_buf),
			     &(text->out_buf_len),
			     (unsigned) estimated_challenge_len + 1);
    if (result != SASL_OK) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    sprintf(text->out_buf,
	    "r=%s,s=%s,i=%u",
	    text->nonce,
	    base64_salt,
	    text->iteration_count);


    /* Save the (client response, ",", server challenge, ",").
       Note, we skip the GS2 prefix here */
    pure_scram_length = clientinlen - text->gs2_header_length;
    text->auth_message_len = pure_scram_length + 1 + estimated_challenge_len + 1;
    text->auth_message = sparams->utils->malloc (text->auth_message_len + 1);
    if (text->auth_message == NULL) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    memcpy(text->auth_message, clientin + text->gs2_header_length, pure_scram_length);
    text->auth_message[pure_scram_length] = ',';
    strcpy (text->auth_message + pure_scram_length + 1, text->out_buf);
    strcat (text->auth_message + pure_scram_length + 1, ",");

    /* Now remember the exact length, not the estimated one */
    text->auth_message_len = strlen(text->auth_message);

    *serverout = text->out_buf;
    *serveroutlen = (unsigned) strlen(text->out_buf);
    
    result = SASL_CONTINUE;
    text->state = 2;
    
cleanup:
    if (inbuf != NULL) {
	sparams->utils->free(inbuf);
    }
    if (base64_salt != NULL) {
	sparams->utils->free(base64_salt);
    }
    return result;
}
    
static int
scram_server_mech_step2(server_context_t *text,
			sasl_server_params_t *sparams,
			const char *clientin,
			unsigned clientinlen,
			const char **serverout,
			unsigned *serveroutlen,
			sasl_out_params_t *oparams)
{
    char *channel_binding = NULL;
    size_t channel_binding_len = 0;
    char *binary_channel_binding = NULL;
    unsigned binary_channel_binding_len = 0;
    char *client_proof = NULL;
    char *inbuf = NULL;
    char *p;
    int result = SASL_FAIL;
    size_t proof_offset;
    char * full_auth_message;
    char ReceivedClientKey[EVP_MAX_MD_SIZE];
    char DecodedClientProof[EVP_MAX_MD_SIZE + 1];
    char CalculatedStoredKey[EVP_MAX_MD_SIZE];
    char ClientSignature[EVP_MAX_MD_SIZE];
    char ServerSignature[EVP_MAX_MD_SIZE];
    char * nonce;
    size_t client_proof_len;
    size_t server_proof_len;
    unsigned exact_client_proof_len;
    unsigned int hash_len = 0;
    size_t k, hash_size = EVP_MD_size(text->md);
    const char *scram_sasl_mech = scram_sasl_mech_name(hash_size);

    if (clientinlen == 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "%s input expected", scram_sasl_mech);
	return SASL_BADPROT;
    }

    if (clientinlen < 3 || clientin[1] != '=') {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid %s input", scram_sasl_mech);
	return SASL_BADPROT;
    }

    inbuf = sparams->utils->malloc (clientinlen + 1);

    if (inbuf == NULL) {
	MEMERROR( sparams->utils );
	return SASL_NOMEM;
    }

    memcpy(inbuf, clientin, clientinlen);
    inbuf[clientinlen] = 0;

    if (strlen(inbuf) != clientinlen) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "NULs found in %s input", scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* Expecting: channel-binding "," nonce ["," extensions] "," proof */

    p = inbuf;

    if (strncmp(p, "c=", 2) != 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Channel binding expected in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    channel_binding = p + 2;

    p = strchr (channel_binding, ',');
    if (p == NULL) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "At least nonce is expected in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }
    *p = '\0';
    p++;

    channel_binding_len = strlen(channel_binding);

    /* We can calculate the exact length, but the decoded (binary) data
       is always shorter than its base64 version. */
    binary_channel_binding = (char *) sparams->utils->malloc(channel_binding_len + 1);

    if (sparams->utils->decode64(channel_binding,
				 (unsigned int)channel_binding_len,
				 binary_channel_binding,
				 (unsigned int)channel_binding_len,
				 &binary_channel_binding_len) != SASL_OK) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid base64 encoding of the channel bindings in %s",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (binary_channel_binding_len < text->gs2_header_length ||
	strncmp(binary_channel_binding, text->gs2_header, text->gs2_header_length) != 0) {
	sparams->utils->seterror (sparams->utils->conn,
				  0,
				  "Channel bindings prefix doesn't match the one received in the GS2 header of %s. Expected \"%s\"",
				  scram_sasl_mech, text->gs2_header);
	result = SASL_BADPROT;
	goto cleanup;
    }

    switch (text->cb_flags & SCRAM_CB_FLAG_MASK) {
    case SCRAM_CB_FLAG_P:
	binary_channel_binding_len -= (unsigned)text->gs2_header_length;
	if (binary_channel_binding_len == 0) {
	    sparams->utils->seterror(sparams->utils->conn, 0,
                                     "Channel bindings data expected in %s",
                                     scram_sasl_mech);
	    result = SASL_BADPROT;
	    goto cleanup;
	}

	if (sparams->cbinding == NULL) {
	    sparams->utils->seterror (sparams->utils->conn,
				      0,
				      "Server does not support channel binding type received in %s. Received: %s",
				      scram_sasl_mech,
				      text->cbindingname);
	    result = SASL_BADPROT;
	    goto cleanup;
	}

	if (strcmp(sparams->cbinding->name, text->cbindingname) != 0) {
		sparams->utils->seterror (sparams->utils->conn,
				      0,
				      "Unsupported channel bindings type received in %s. Expected: %s, received: %s",
                                      scram_sasl_mech,
				      sparams->cbinding->name,
				      text->cbindingname);
	    result = SASL_BADPROT;
	    goto cleanup;
	}

	if (binary_channel_binding_len != sparams->cbinding->len) {
	    sparams->utils->seterror (sparams->utils->conn,
				      0,
				      "Unsupported channel bindings length received in %s. Expected length: %lu, received: %d",
                                      scram_sasl_mech,
				      sparams->cbinding->len,
				      binary_channel_binding_len);
	    result = SASL_BADPROT;
	    goto cleanup;
	}

	if (memcmp(binary_channel_binding + text->gs2_header_length,
		   sparams->cbinding->data,
		   binary_channel_binding_len) != 0) {
	    sparams->utils->seterror(sparams->utils->conn, 0,
                                     "Channel bindings mismatch in %s",
                                     scram_sasl_mech);
	    result = SASL_BADPROT;
	    goto cleanup;
	}
        break;
    }

    if (strncmp(p, "r=", 2) != 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Nonce expected in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    nonce = p + 2;

    p = strchr (nonce, ',');
    if (p == NULL) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "At least proof is expected in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }
    *p = '\0';
    p++;

    if (strcmp(nonce, text->nonce) != 0) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Nonce mismatch %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    while (p[0] != '\0') {
	if (strncmp(p, "p=", 2) == 0) {
	    client_proof = p + 2;
	    proof_offset = p - inbuf - 1;
	    break;
	}

	p = strchr (p, ',');
	if (p == NULL) {
	    break;
	}
	p++;
    }

    if (client_proof == NULL) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Client proof is expected in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* Check that no extension data exists after the proof */
    p = strchr (client_proof, ',');
    if (p != NULL) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "No extension data is allowed after the client proof in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (strlen(client_proof) != (hash_size / 3 * 4 + (hash_size % 3 ? 4 : 0))) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid client proof length in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* Construct the full AuthMessage */
    full_auth_message = sparams->utils->realloc(text->auth_message,
						text->auth_message_len + proof_offset + 1);
    if (full_auth_message == NULL) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }
    text->auth_message = full_auth_message;

    memcpy(text->auth_message + text->auth_message_len, clientin, proof_offset);

    text->auth_message_len += proof_offset;
    text->auth_message[text->auth_message_len] = '\0';


    /* ClientSignature := HMAC(StoredKey, AuthMessage) */
    if (HMAC(text->md,
	     (const unsigned char *) text->StoredKey,
	     hash_size,
	     (const unsigned char *)text->auth_message,
	     (int)text->auth_message_len,
	     (unsigned char *)ClientSignature,
	     &hash_len) == NULL) {
	sparams->utils->seterror(sparams->utils->conn, 0,
				 "HMAC-%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    client_proof_len = strlen(client_proof);
    if (sparams->utils->decode64(client_proof,
				 (unsigned int)client_proof_len,
				 DecodedClientProof,
				 hash_size + 1,
				 &exact_client_proof_len) != SASL_OK) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid base64 encoding of the client proof in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (exact_client_proof_len != hash_size) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Invalid client proof (truncated) in %s input",
                                 scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    for (k = 0; k < hash_size; k++) {
	ReceivedClientKey[k] = DecodedClientProof[k] ^ ClientSignature[k];
    }

    /* StoredKey       := H(ClientKey) */
    if (EVP_Digest((const unsigned char *) ReceivedClientKey, hash_size,
                   (unsigned char *) CalculatedStoredKey, NULL, text->md, NULL) == 0) {
	sparams->utils->seterror(sparams->utils->conn,0,
				 "%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }
    
    for (k = 0; k < hash_size; k++) {
	if (CalculatedStoredKey[k] != text->StoredKey[k]) {
	    SETERROR(sparams->utils, "StoredKey mismatch");
	    result = SASL_BADAUTH;
	    goto cleanup;
	}
    }
    
    /* ServerSignature := HMAC(ServerKey, AuthMessage) */
    if (HMAC(text->md,
	     (const unsigned char *) text->ServerKey,
	     hash_size,
	     (unsigned char *) text->auth_message,
	     (int)text->auth_message_len,
	     (unsigned char *)ServerSignature,
	     &hash_len) == NULL) {
	sparams->utils->seterror(sparams->utils->conn,0,
				 "HMAC-%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    server_proof_len = (hash_size / 3 * 4 + (hash_size % 3 ? 4 : 0));
    result = _plug_buf_alloc(sparams->utils,
			     &(text->out_buf),
			     &(text->out_buf_len),
			     (unsigned) server_proof_len + strlen("v=") + 1);
    if (result != SASL_OK) {
	MEMERROR( sparams->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    text->out_buf[0] = 'v';
    text->out_buf[1] = '=';


    if (sparams->utils->encode64(ServerSignature,
				 hash_size,
				 text->out_buf+2,
				 (unsigned int)server_proof_len + 1,
				 NULL) != SASL_OK) {
	SETERROR(sparams->utils, "Internal error");
	/* This is not quite right, but better than alternatives */
	result = SASL_NOMEM;
	goto cleanup;
    }

    text->out_buf[server_proof_len + 2] = '\0';

    *serverout = text->out_buf;
    *serveroutlen = (unsigned) strlen(text->out_buf);


    /* set oparams */

    switch (text->cb_flags & SCRAM_CB_FLAG_MASK) {
    case SCRAM_CB_FLAG_N:
        oparams->cbindingdisp = SASL_CB_DISP_NONE;
        break;
    case SCRAM_CB_FLAG_P:
        oparams->cbindingdisp = SASL_CB_DISP_USED;
        oparams->cbindingname = text->cbindingname;
        break;
    case SCRAM_CB_FLAG_Y:
        oparams->cbindingdisp = SASL_CB_DISP_WANT;
        break;
    }

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
    if (inbuf != NULL) {
	sparams->utils->free(inbuf);
    }
    if (binary_channel_binding != NULL) {
	sparams->utils->free(binary_channel_binding);
    }

    return result;
}

static int scram_server_mech_step(void *conn_context,
				  sasl_server_params_t *sparams,
				  const char *clientin,
				  unsigned clientinlen,
				  const char **serverout,
				  unsigned *serveroutlen,
				  sasl_out_params_t *oparams)
{
    server_context_t *text = (server_context_t *) conn_context;
    const char *scram_sasl_mech = NULL;
    
    *serverout = NULL;
    *serveroutlen = 0;
    
    if (text == NULL) {
	return SASL_BADPROT;
    }

    scram_sasl_mech = scram_sasl_mech_name(EVP_MD_size(text->md));

    /* this should be well more than is ever needed */
    if (clientinlen > MAX_CLIENTIN_LEN) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "%s input longer than "
                                 STRINGIZE((MAX_CLIENTIN_LEN)) " bytes",
                                 scram_sasl_mech);
	return SASL_BADPROT;
    }
    
    switch (text->state) {
    case 0:
	text->state++;
	/* Assume the protocol doesn't support initial client response */
	if (clientinlen == 0) {
	    return SASL_CONTINUE;
	}
	/* fall through */

    case 1:
	return scram_server_mech_step1(text,
				       sparams,
				       clientin,
				       clientinlen,
				       serverout,
				       serveroutlen,
				       oparams);

    case 2:
	text->state++;
	return scram_server_mech_step2(text,
				       sparams,
				       clientin,
				       clientinlen,
				       serverout,
				       serveroutlen,
				       oparams);

    default: /* should never get here */
	sparams->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid %s server step %d\n",
                            scram_sasl_mech, text->state);
	return SASL_FAIL;
    }
    
    return SASL_FAIL; /* should never get here */
}

static int scram_setpass(void *glob_context,
			 sasl_server_params_t *sparams,
			 const char *userstr,
			 const char *pass,
			 unsigned passlen,
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
    const char *store_request[] = { "authPassword",
				    NULL };
    const char *generate_scram_secret;
    const EVP_MD *md = EVP_get_digestbyname((const char *) glob_context);
    size_t hash_size = EVP_MD_size(md);
    const char *scram_sasl_mech = scram_sasl_mech_name(hash_size);
    
    /* Do we have a backend that can store properties? */
    if (!sparams->utils->auxprop_store ||
	sparams->utils->auxprop_store(NULL, NULL, NULL) != SASL_OK) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "%s: auxprop backend can't store properties",
                                 scram_sasl_mech);
	return SASL_NOMECH;
    }

    sparams->utils->getopt(sparams->utils->getopt_context,
			   /* This affects all SCRAM plugins, not just SCRAM-SHA-1 */
			   "SCRAM",
			   "scram_secret_generate",
			   &generate_scram_secret,
			   NULL);

    /* NOTE: The default (when this option is not set) is NOT to generate authPassword secret */
    if (!(generate_scram_secret &&
	  (generate_scram_secret[0] == '1' || generate_scram_secret[0] == 'y' ||
	  (generate_scram_secret[0] == 'o' && generate_scram_secret[1] == 'n') ||
	  generate_scram_secret[0] == 't'))) {
	/* Pretend that everything is Ok, no need to generate noise in the logs */
	return SASL_OK;
    }

    r = _plug_parseuser(sparams->utils,
			&user_only,
			&realm,
			sparams->user_realm,
			sparams->serverFQDN,
			userstr);
    if (r) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "%s: Error parsing user", scram_sasl_mech);
	return r;
    }

    r = _plug_make_fulluser(sparams->utils, &user, user_only, realm);
    if (r) {
       goto cleanup;
    }

    if ((flags & SASL_SET_DISABLE) || pass == NULL) {
	sec = NULL;
    } else {
	char * error_text = NULL;
	char salt[SALT_SIZE + 1];
	char base64_salt[BASE64_LEN(SALT_SIZE) + 1];
	/* size_t salt_len = SALT_SIZE; */
	char StoredKey[EVP_MAX_MD_SIZE + 1];
	char ServerKey[EVP_MAX_MD_SIZE + 1];
	char base64_StoredKey[BASE64_LEN(EVP_MAX_MD_SIZE) + 1];
	char base64_ServerKey[BASE64_LEN(EVP_MAX_MD_SIZE) + 1];
	size_t secret_len;
	unsigned int iteration_count = DEFAULT_ITERATION_COUNTER;
	char * s_iteration_count;
	char * end;

	sparams->utils->getopt(sparams->utils->getopt_context,
			       /* Different SCRAM hashes can have different strengh */
			       scram_sasl_mech,
			       "scram_iteration_counter",
			       (const char **) &s_iteration_count,
			       NULL);

	if (s_iteration_count != NULL) {
	    errno = 0;
	    iteration_count = strtoul(s_iteration_count, &end, 10);
	    if (s_iteration_count == end || *end != '\0' || errno != 0) {
		sparams->utils->log(NULL,
				    SASL_LOG_DEBUG,
				    "Invalid iteration-count in scram_iteration_count SASL option: not a number. Using the default instead.");
		s_iteration_count = NULL;
	    }
	}

	if (s_iteration_count == NULL) {
	    iteration_count = DEFAULT_ITERATION_COUNTER;
	}

	sparams->utils->rand(sparams->utils->rpool, salt, SALT_SIZE);

	r = GenerateScramSecrets (sparams->utils,
                                  md,
				  pass,
				  passlen,
				  salt,
				  SALT_SIZE,
				  iteration_count,
				  StoredKey,
				  ServerKey,
				  &error_text);
	if (r != SASL_OK) {
	    if (error_text != NULL) {
		sparams->utils->seterror(sparams->utils->conn, 0, "%s",
					 error_text);
	    }
	    goto cleanup;
	}

	/* Returns SASL_OK on success, SASL_BUFOVER if result won't fit */
	if (sparams->utils->encode64(salt,
				     SALT_SIZE,
				     base64_salt,
				     BASE64_LEN(SALT_SIZE) + 1,
				     NULL) != SASL_OK) {
	    MEMERROR( sparams->utils );
	    r = SASL_NOMEM;
	    goto cleanup;
	}

	base64_salt[BASE64_LEN(SALT_SIZE)] = '\0';


	/* Returns SASL_OK on success, SASL_BUFOVER if result won't fit */
	if (sparams->utils->encode64(StoredKey,
				     hash_size,
				     base64_StoredKey,
				     BASE64_LEN(hash_size) + 1,
				     NULL) != SASL_OK) {
	    MEMERROR( sparams->utils );
	    r = SASL_NOMEM;
	    goto cleanup;
	}

	base64_StoredKey[BASE64_LEN(hash_size)] = '\0';



	/* Returns SASL_OK on success, SASL_BUFOVER if result won't fit */
	if (sparams->utils->encode64(ServerKey,
				     hash_size,
				     base64_ServerKey,
				     BASE64_LEN(hash_size) + 1,
				     NULL) != SASL_OK) {
	    MEMERROR( sparams->utils );
	    r = SASL_NOMEM;
	    goto cleanup;
	}

	base64_ServerKey[BASE64_LEN(hash_size)] = '\0';

	secret_len = strlen(scram_sasl_mech) + strlen("$:$:") +
		     ITERATION_COUNTER_BUF_LEN +
		     sizeof(base64_salt) +
		     sizeof(base64_StoredKey) +
		     sizeof(base64_ServerKey);

	sec = sparams->utils->malloc(sizeof(sasl_secret_t) + secret_len);
	if (sec == NULL) {
	    MEMERROR( sparams->utils );
	    r = SASL_NOMEM;
	    goto cleanup;
	}
    	
	sprintf((char *) sec->data,
		"%s$%u:%s$%s:%s",
		scram_sasl_mech,
		iteration_count,
		base64_salt,
		base64_StoredKey,
		base64_ServerKey);
	sec->len = (unsigned int) strlen((const char *) sec->data);
    }
    
    /* do the store */
    propctx = sparams->utils->prop_new(0);
    if (!propctx) {
	r = SASL_FAIL;
    }
    if (!r) {
	r = sparams->utils->prop_request(propctx, store_request);
    }
    if (!r) {
	r = sparams->utils->prop_set(propctx,
				     "authPassword",
				     (const char *) (sec ? sec->data : NULL),
				     (sec ? sec->len : 0));
    }
    if (!r) {
	r = sparams->utils->auxprop_store(sparams->utils->conn, propctx, user);
    }
    if (propctx) {
	sparams->utils->prop_dispose(&propctx);
    }
    
    if (r) {
	sparams->utils->seterror(sparams->utils->conn, 0,
                                 "Error putting %s secret",
                                 scram_sasl_mech);
	goto cleanup;
    }
    
    sparams->utils->log(NULL, SASL_LOG_DEBUG, "Setpass for %s successful\n",
                        scram_sasl_mech);
    
  cleanup:
    if (user) 	_plug_free_string(sparams->utils, &user);
    if (user_only)     _plug_free_string(sparams->utils, &user_only);
    if (realm) 	_plug_free_string(sparams->utils, &realm);
    if (sec)    _plug_free_secret(sparams->utils, &sec);
    
    return r;
}

static void scram_server_mech_dispose(void *conn_context,
				      const sasl_utils_t *utils)
{
    server_context_t *text = (server_context_t *) conn_context;
    
    if (!text) return;
    
    if (text->authentication_id) _plug_free_string(utils,&(text->authentication_id));
    if (text->authorization_id) _plug_free_string(utils,&(text->authorization_id));
    if (text->out_buf) _plug_free_string(utils,&(text->out_buf));
    if (text->auth_message) _plug_free_string(utils,&(text->auth_message));
    if (text->nonce) _plug_free_string(utils,&(text->nonce));
    if (text->salt) utils->free(text->salt);
    if (text->cbindingname != NULL) {
	utils->free(text->cbindingname);
	text->cbindingname = NULL;
    }
    if (text->gs2_header != NULL) {
	utils->free(text->gs2_header);
	text->gs2_header = NULL;
    }

    utils->free(text);
}

static sasl_server_plug_t scram_server_plugins[] = 
{
#ifdef HAVE_SHA512
    {
	"SCRAM-SHA-512",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(512) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING,	/* features */
	"SHA512",			/* glob_context */
	&scram_server_mech_new,		/* mech_new */
	&scram_server_mech_step,	/* mech_step */
	&scram_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	&scram_setpass,			/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech avail */
	NULL				/* spare */
    },
    {
	"SCRAM-SHA-384",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(384) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING,	/* features */
	"SHA384",			/* glob_context */
	&scram_server_mech_new,		/* mech_new */
	&scram_server_mech_step,	/* mech_step */
	&scram_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	&scram_setpass,			/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech avail */
	NULL				/* spare */
    },
    {
	"SCRAM-SHA-256",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(256) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING,	/* features */
	"SHA256",			/* glob_context */
	&scram_server_mech_new,		/* mech_new */
	&scram_server_mech_step,	/* mech_step */
	&scram_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	&scram_setpass,			/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech avail */
	NULL				/* spare */
    },
    {
	"SCRAM-SHA-224",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(224) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING,	/* features */
	"SHA224",			/* glob_context */
	&scram_server_mech_new,		/* mech_new */
	&scram_server_mech_step,	/* mech_step */
	&scram_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	&scram_setpass,			/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech avail */
	NULL				/* spare */
    },
#endif
    {
	"SCRAM-SHA-1",			/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(160) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOACTIVE
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING,	/* features */
	"SHA1",				/* glob_context */
	&scram_server_mech_new,		/* mech_new */
	&scram_server_mech_step,	/* mech_step */
	&scram_server_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	&scram_setpass,			/* setpass */
	NULL,				/* user_query */
	NULL,				/* idle */
	NULL,				/* mech avail */
	NULL				/* spare */
    }
};

int scram_server_plug_init(const sasl_utils_t *utils,
                           int maxversion,
                           int *out_version,
                           sasl_server_plug_t **pluglist,
                           int *plugcount)
{
    if (maxversion < SASL_SERVER_PLUG_VERSION) {
	SETERROR( utils, "SCRAM-SHA-* version mismatch");
	return SASL_BADVERS;
    }

    *out_version = SASL_SERVER_PLUG_VERSION;
    *pluglist = scram_server_plugins;
#ifdef HAVE_SHA512
    *plugcount = 5;
#else
    *plugcount = 1;
#endif
    utils->rand(utils->rpool, (char *)g_salt_key, SALT_SIZE);
    
    return SASL_OK;
}

/*****************************  Client Section  *****************************/

typedef struct client_context {
    int state;

    const EVP_MD *md;		/* underlying MDA */

    sasl_secret_t *password;	/* user password */
    unsigned int free_password; /* set if we need to free the password */

    char * gs2_header;
    size_t gs2_header_length;
    char * out_buf;
    unsigned out_buf_len;
    char * auth_message;
    size_t auth_message_len;
    char * nonce;
    /* in binary form */
    char * salt;
    size_t salt_len;
    unsigned int iteration_count;
    char SaltedPassword[EVP_MAX_MD_SIZE];

    int cb_flags;
} client_context_t;

static int scram_client_mech_new(void *glob_context,
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

    text->md = EVP_get_digestbyname((const char *) glob_context);

    *conn_context = text;
    
    return SASL_OK;
}

static int
scram_client_mech_step1(client_context_t *text,
			sasl_client_params_t *params,
			const char *serverin __attribute__((unused)), 
			unsigned serverinlen __attribute__((unused)), 
			sasl_interact_t **prompt_need,
			const char **clientout,
			unsigned *clientoutlen,
			sasl_out_params_t *oparams)
{
    const char *authid = NULL;
    const char *userid = NULL;
    int user_result = SASL_OK;
    int auth_result = SASL_OK;
    int pass_result = SASL_OK;
    int result;
    size_t maxsize;
    char * encoded_authcid;
    char * freeme = NULL;
    char * freeme2 = NULL;
    char channel_binding_state = 'n';
    const char * channel_binding_name = NULL;
    char * encoded_authorization_id = NULL;
    const char *scram_sasl_mech = scram_sasl_mech_name(EVP_MD_size(text->md));

    /* check if sec layer strong enough */
    if (params->props.min_ssf > params->external_ssf) {
	params->utils->seterror(params->utils->conn, 0,
                                "SSF requested of %s plugin",
                                scram_sasl_mech);
	return SASL_TOOWEAK;
    }
    
    /* try to get the userid */
    if (oparams->authid == NULL) {
	auth_result=_plug_get_authid(params->utils, &authid, prompt_need);
	
	if ((auth_result != SASL_OK) && (auth_result != SASL_INTERACT))
	    return auth_result;
    }
    
    /* try to get the userid */
    if (oparams->user == NULL) {
	user_result = _plug_get_userid(params->utils, &userid, prompt_need);
	
	if ((user_result != SASL_OK) && (user_result != SASL_INTERACT)) {
	    return user_result;
	}
    }
    
    /* try to get the password */
    if (text->password == NULL) {
	pass_result = _plug_get_password(params->utils,
					 &text->password,
					 &text->free_password,
					 prompt_need);
	if ((pass_result != SASL_OK) && (pass_result != SASL_INTERACT)) {
	    return pass_result;
	}
    }

    /* free prompts we got */
    if (prompt_need && *prompt_need) {
	params->utils->free(*prompt_need);
	*prompt_need = NULL;
    }
    
    /* if there are prompts not filled in */
    if ((auth_result == SASL_INTERACT) ||
	(user_result == SASL_INTERACT) ||
	(pass_result == SASL_INTERACT)) {
	/* make the prompt list */
	result =
	    _plug_make_prompts(params->utils,
			       prompt_need,
			       user_result == SASL_INTERACT ?
			       "Please enter your authorization name" : NULL,
			       NULL,
			       auth_result == SASL_INTERACT ?
			       "Please enter your authentication name" : NULL,
			       NULL,
			       pass_result == SASL_INTERACT ?
			       "Please enter your password" : NULL,
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       NULL,
			       NULL);

	if (result != SASL_OK) {
	    goto cleanup;
	}
	
	return SASL_INTERACT;
    }
    
    if (!text->password) {
	PARAMERROR(params->utils);
	return SASL_BADPARAM;
    }

    if (oparams->authid == NULL) {
	if (!userid || !*userid) {
	    result = params->canon_user(params->utils->conn,
					authid,
					0,
					SASL_CU_AUTHID | SASL_CU_AUTHZID,
					oparams);
	}
	else {
	    result = params->canon_user(params->utils->conn,
					authid,
					0,
					SASL_CU_AUTHID,
					oparams);
	    if (result != SASL_OK) {
		goto cleanup;
	    }

	    result = params->canon_user(params->utils->conn,
					userid,
					0,
					SASL_CU_AUTHZID,
					oparams);
	}
	if (result != SASL_OK) {
	    goto cleanup;
	}
    }

    switch (params->cbindingdisp) {
    case SASL_CB_DISP_NONE:
	text->cb_flags = SCRAM_CB_FLAG_N;
	channel_binding_state = 'n';
	break;
    case SASL_CB_DISP_USED:
	if (!SASL_CB_PRESENT(params)) {
	    result = SASL_BADPARAM;
	    goto cleanup;
	}
	channel_binding_name = params->cbinding->name;
	text->cb_flags = SCRAM_CB_FLAG_P;
	channel_binding_state = 'p';
	break;
    case SASL_CB_DISP_WANT:
	text->cb_flags = SCRAM_CB_FLAG_Y;
	channel_binding_state = 'y';
	break;
    }

    text->nonce = params->utils->malloc (NONCE_SIZE + 1);

    if (text->nonce == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    if (create_nonce(params->utils,
		     text->nonce,
		     NONCE_SIZE + 1) == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    if (userid != NULL && *userid != '\0') {
	result = encode_saslname (oparams->user,
				  (const char **) &encoded_authorization_id,
				  &freeme2);

	if (result != SASL_OK) {
	    MEMERROR( params->utils );
	    result = SASL_NOMEM;
	    goto cleanup;
	}
    }

    result = encode_saslname (oparams->authid,
			      (const char **) &encoded_authcid,
			      &freeme);
    if (result != SASL_OK) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    maxsize = strlen("p=,a=,n=,r=") +
	      ((channel_binding_name != NULL) ? strlen(channel_binding_name) : 0) +
	      ((encoded_authorization_id != NULL) ? strlen(encoded_authorization_id) : 0) +
	      strlen(encoded_authcid) +
	      strlen(text->nonce);
    result = _plug_buf_alloc(params->utils,
			     &(text->out_buf),
			     &(text->out_buf_len),
			     (unsigned) maxsize + 1);
    if (result != SASL_OK) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    snprintf(text->out_buf,
	     maxsize + 1,
	     "%c%s%s,%s%s,",
	     channel_binding_state,
	     (channel_binding_name != NULL) ? "=" : "",
	     (channel_binding_name != NULL) ? channel_binding_name : "",
	     (encoded_authorization_id != NULL) ? "a=" : "",
	     (encoded_authorization_id != NULL) ? encoded_authorization_id : "");

    text->gs2_header_length = strlen(text->out_buf);
    _plug_strdup(params->utils, text->out_buf, &text->gs2_header, NULL);

    sprintf(text->out_buf + text->gs2_header_length,
	    "n=%s,r=%s",
	    encoded_authcid,
	    text->nonce);

    /* Save the copy of the client-first-message */

    /* Need to skip the GS2 prefix here */
    _plug_strdup(params->utils,
		 text->out_buf + text->gs2_header_length,
		 &text->auth_message,
		 NULL);
    if (text->auth_message == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    text->auth_message_len = strlen(text->auth_message);

    *clientout = text->out_buf;
    *clientoutlen = (unsigned) strlen(*clientout);

    result = SASL_CONTINUE;

cleanup:
    if (freeme != NULL) _plug_free_string(params->utils, &freeme);
    if (freeme2 != NULL) _plug_free_string(params->utils, &freeme2);

    return result;
}

static int
scram_client_mech_step2(client_context_t *text,
			sasl_client_params_t *params,
			const char *serverin, 
			unsigned serverinlen, 
			sasl_interact_t **prompt_need __attribute__((unused)),
			const char **clientout,
			unsigned *clientoutlen,
			sasl_out_params_t *oparams __attribute__((unused)))
{
    char * p;
    char * nonce;
    size_t server_nonce_len;
    char * base64_salt = NULL;
    size_t base64_salt_len;
    unsigned exact_salt_len;
    char * counter;
    char * end;
    char * inbuf = NULL;
    size_t estimated_response_len;
    size_t length_no_proof;
    char * full_auth_message;
    size_t cb_bin_length;
    size_t channel_binding_data_len = 0;
    size_t cb_encoded_length;
    const char * channel_binding_data = NULL;
    char * cb_encoded = NULL;
    char * cb_bin = NULL;
    int result;
    char ClientKey[EVP_MAX_MD_SIZE];
    char StoredKey[EVP_MAX_MD_SIZE];
    char ClientSignature[EVP_MAX_MD_SIZE];
    char ClientProof[EVP_MAX_MD_SIZE];
    char * client_proof = NULL;
    size_t client_proof_len;
    unsigned int hash_len = 0;
    size_t k, hash_size = EVP_MD_size(text->md);
    const char *scram_sasl_mech = scram_sasl_mech_name(hash_size);

    if (serverinlen == 0) {
	params->utils->seterror(params->utils->conn, 0,
                                "%s input expected", scram_sasl_mech);
	return SASL_BADPROT;
    }

    /* [reserved-mext ","] nonce "," salt "," iteration-count ["," extensions] */

    if (serverinlen < 3 || serverin[1] != '=') {
	params->utils->seterror(params->utils->conn, 0,
                                "Invalid %s input", scram_sasl_mech);
	return SASL_BADPROT;
    }

    if (serverin[0] == 'm') {
	params->utils->seterror(params->utils->conn, 0,
                                "Unsupported mandatory extension to %s",
                                scram_sasl_mech);
	return SASL_BADPROT;
    }

    if (serverin[0] != 'r') {
	params->utils->seterror(params->utils->conn, 0,
                                "Nonce (r=) expected in %s input",
                                scram_sasl_mech);
	return SASL_BADPROT;
    }

    inbuf = params->utils->malloc (serverinlen + 1);
    if (inbuf == NULL) {
	MEMERROR( params->utils );
	return SASL_NOMEM;
    }

    memcpy(inbuf, serverin, serverinlen);
    inbuf[serverinlen] = 0;

    if (strlen(inbuf) != serverinlen) {
	params->utils->seterror(params->utils->conn, 0,
                                "NULs found in %s input", scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    nonce = inbuf + 2;
    p = strchr (nonce, ',');

    /* MUST be followed by a salt */
    if (p == NULL) {
	params->utils->seterror(params->utils->conn, 0,
                                "Salt expected after the nonce in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    *p = '\0';
    p++;

    if (strncmp(p, "s=", 2) != 0) {
	params->utils->seterror(params->utils->conn, 0,
                                "Salt expected after the nonce in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    p += 2;
    base64_salt = p;

    p = strchr (base64_salt, ',');

    /* MUST be followed by an iteration-count */
    if (p == NULL) {
	params->utils->seterror(params->utils->conn, 0,
                                "iteration-count expected after the salt in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    *p = '\0';
    p++;

    if (strncmp(p, "i=", 2) != 0) {
	params->utils->seterror(params->utils->conn, 0,
                                "iteration-count expected after the salt in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    p += 2;
    counter = p;
    p = strchr (counter, ',');

    if (p == NULL) {
	p = counter + strlen(counter);
    } else {
	*p = '\0';
    }

    errno = 0;
    text->iteration_count = strtoul(counter, &end, 10);
    if (counter == end || *end != '\0' || errno != 0) {
	params->utils->seterror(params->utils->conn, 0,
                                "Invalid iteration-count in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (text->iteration_count < MIN_ITERATION_COUNTER) {
    }
    
    if (text->iteration_count > MAX_ITERATION_COUNTER) {
	SETERROR(params->utils, "iteration-count is too big, refusing to compute");
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* The client MUST verify that the initial part of the nonce
       used in subsequent messages is the same as the nonce it
       initially specified. */
    server_nonce_len = strlen(nonce);

    if (server_nonce_len <= NONCE_SIZE ||
	strncmp(nonce, text->nonce, NONCE_SIZE) != 0) {
	SETERROR(params->utils, "The nonce received from the server doesn't start from the nonce sent by the client");
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* Now we can forget about our nonce */
    params->utils->free(text->nonce);

    _plug_strdup(params->utils, nonce, &text->nonce, NULL);

    if (text->nonce == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    /* base64 decode salt */
    base64_salt_len = strlen(base64_salt);

    if (base64_salt_len == 0) {
	SETERROR(params->utils, "The salt can't be empty");
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (base64_salt_len % 4 != 0) {
	SETERROR(params->utils, "Invalid base64 encoding of the salt");
	result = SASL_BADPROT;
	goto cleanup;
    }

    text->salt_len = base64_salt_len / 4 * 3;

    text->salt = (char *) params->utils->malloc(text->salt_len + 1);
    if (text->salt == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    if (params->utils->decode64(base64_salt,
				(unsigned int)base64_salt_len,
				text->salt,
				(unsigned int)text->salt_len + 1,
				&exact_salt_len) != SASL_OK) {
	params->utils->seterror(params->utils->conn, 0,
                                "Invalid base64 encoding of the salt in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    text->salt_len = exact_salt_len;

    /* Now we generate client response */

    if (text->gs2_header[0] == 'p') {

	if (params->cbinding == NULL) {
	    result = SASL_FAIL;
	    goto cleanup;
	}

	channel_binding_data = (const char *) params->cbinding->data;
	channel_binding_data_len = params->cbinding->len;
    }

    cb_bin_length = text->gs2_header_length + 
		    ((channel_binding_data != NULL) ? channel_binding_data_len : 0);
    cb_encoded_length = (cb_bin_length / 3 * 4) + ((cb_bin_length % 3) ? 4 : 0);
    
    if (channel_binding_data != NULL) {
	cb_bin = (char *) params->utils->malloc(cb_bin_length + 1);
	if (cb_bin == NULL) {
	    MEMERROR( params->utils );
	    result = SASL_NOMEM;
	    goto cleanup;
	}

	memcpy(cb_bin, text->gs2_header, text->gs2_header_length);
	memcpy(cb_bin + text->gs2_header_length, channel_binding_data, channel_binding_data_len);
    }

    cb_encoded = (char *) params->utils->malloc(cb_encoded_length + 1);
    if (cb_encoded == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }
    
    /*
     * Returns SASL_OK on success, SASL_BUFOVER if result won't fit
     */
    if (params->utils->encode64((cb_bin != NULL) ? cb_bin : text->gs2_header,
				(unsigned int)cb_bin_length,
				cb_encoded,
				(unsigned int)cb_encoded_length + 1,
				NULL) != SASL_OK) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    cb_encoded[cb_encoded_length] = '\0';

    client_proof_len = hash_size / 3 * 4 + ((hash_size % 3) ? 4 : 0);
    estimated_response_len = strlen(cb_encoded)+
			     strlen(text->nonce)+
			     client_proof_len +
			     strlen("c=,r=,p=");
    result = _plug_buf_alloc(params->utils,
			     &(text->out_buf),
			     &(text->out_buf_len),
			     (unsigned) estimated_response_len + 1);
    if (result != SASL_OK) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    /* channel-binding "," nonce ["," extensions] */
    sprintf(text->out_buf,
	    "c=%s,r=%s",
	    cb_encoded,
	    text->nonce);

    length_no_proof = strlen(text->out_buf);

    /* Build AuthMessage */
    full_auth_message = params->utils->realloc(text->auth_message,
					       text->auth_message_len + 1 +
					       serverinlen + 1 +
					       length_no_proof + 1);
    if (full_auth_message == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    text->auth_message = full_auth_message;

    text->auth_message[text->auth_message_len] = ',';
    memcpy(text->auth_message + text->auth_message_len + 1, serverin, serverinlen);
    text->auth_message[text->auth_message_len + 1 + serverinlen] = ',';
    memcpy(text->auth_message + text->auth_message_len + 1 + serverinlen + 1,
	   text->out_buf,
	   length_no_proof);
    text->auth_message_len += serverinlen + 2 + length_no_proof;
    text->auth_message[text->auth_message_len] = '\0';

    /* Calculate ClientProof */

    /* SaltedPassword  := Hi(password, salt) */
    Hi (params->utils,
        text->md,
	(const char *) text->password->data,
	text->password->len,
	text->salt,
	text->salt_len,
	text->iteration_count,
	text->SaltedPassword);

    PRINT_HASH ("SaltedPassword", text->SaltedPassword, hash_size);

    /* ClientKey       := HMAC(SaltedPassword, "Client Key") */
    if (HMAC(text->md,
	     (const unsigned char *) text->SaltedPassword,
	     hash_size,
	     (const unsigned char *) CLIENT_KEY_CONSTANT,
	     CLIENT_KEY_CONSTANT_LEN,
	     (unsigned char *)ClientKey,
	     &hash_len) == NULL) {
	params->utils->seterror(params->utils->conn,0,
				"HMAC-%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    PRINT_HASH ("ClientKey", ClientKey, hash_size);

    /* StoredKey       := H(ClientKey) */
    if (EVP_Digest((const unsigned char *) ClientKey, hash_size,
                   (unsigned char *) StoredKey, NULL, text->md, NULL) == 0) {
	params->utils->seterror(params->utils->conn,0,
				"%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    PRINT_HASH ("StoredKey", StoredKey, hash_size);

    /* ClientSignature := HMAC(StoredKey, AuthMessage) */
    if (HMAC(text->md,
	     (const unsigned char *)StoredKey,
	     hash_size,
	     (const unsigned char *) text->auth_message,
	     (int)text->auth_message_len,
	     (unsigned char *)ClientSignature,
	     &hash_len) == NULL) {
	params->utils->seterror(params->utils->conn,0,
				"HMAC-%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    PRINT_HASH ("ClientSignature", ClientSignature, hash_size);

    /* ClientProof     := ClientKey XOR ClientSignature */
    for (k = 0; k < hash_size; k++) {
	ClientProof[k] = ClientKey[k] ^ ClientSignature[k];
    }

    PRINT_HASH ("ClientProof", ClientProof, hash_size);

    /* base64-encode ClientProof */
    client_proof = (char *) params->utils->malloc(client_proof_len + 1);
    if (client_proof == NULL) {
	MEMERROR( params->utils );
	result = SASL_NOMEM;
	goto cleanup;
    }

    result = params->utils->encode64(ClientProof,
				     hash_size,
				     client_proof,
				     (unsigned int)client_proof_len + 1,
				     NULL);

    if (result != SASL_OK) {
	goto cleanup;
    }

    client_proof[client_proof_len] = '\0';

    sprintf(text->out_buf + length_no_proof,
	    ",p=%s",
	    client_proof);

    *clientout = text->out_buf;
    *clientoutlen = (unsigned) strlen(text->out_buf);

    result = SASL_CONTINUE;
    
cleanup:
    if (inbuf != NULL) {
	params->utils->free(inbuf);
    }

    if (client_proof != NULL) {
	params->utils->free(client_proof);
    }

    if (cb_encoded != NULL) {
	params->utils->free(cb_encoded);
    }

    if (cb_bin != NULL) {
	params->utils->free(cb_bin);
    }

    return result;
}


static int
scram_client_mech_step3(client_context_t *text,
			sasl_client_params_t *params,
			const char *serverin, 
			unsigned serverinlen, 
			sasl_interact_t **prompt_need __attribute__((unused)),
			const char **clientout __attribute__((unused)),
			unsigned *clientoutlen __attribute__((unused)),
			sasl_out_params_t *oparams)
{
    char * p;
    int result;
    size_t server_proof_len;
    unsigned exact_server_proof_len;
    char DecodedServerProof[EVP_MAX_MD_SIZE + 1];
    char ServerKey[EVP_MAX_MD_SIZE];
    char ServerSignature[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    size_t k, hash_size = EVP_MD_size(text->md);
    const char *scram_sasl_mech = scram_sasl_mech_name(hash_size);

    if (serverinlen < 3) {
	params->utils->seterror(params->utils->conn, 0,
                                "Invalid %s input expected",
                                scram_sasl_mech);
	return SASL_BADPROT;
    }

    /* Expecting: 'verifier ["," extensions]' */

    if (strncmp(serverin, "v=", 2) != 0) {
	params->utils->seterror(params->utils->conn, 0,
                                "ServerSignature expected in %s input",
                                scram_sasl_mech);
	return SASL_BADPROT;
    }

    /* Use memchr instead of the original strchr as there is no guarantee that
       the input data is NUL terminated */
    p = memchr (serverin + 2, ',', serverinlen - 2);
    if (p != NULL) {
	server_proof_len = p - (serverin + 2) - 1;
    } else {
	server_proof_len = serverinlen - 2;
    }

    if (params->utils->decode64(serverin + 2,	/* ServerProof */
				(unsigned int)server_proof_len,
				DecodedServerProof,
				hash_size + 1,
				&exact_server_proof_len) != SASL_OK) {
	params->utils->seterror(params->utils->conn, 0,
                                "Invalid base64 encoding of the server proof in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    if (exact_server_proof_len != hash_size) {
	params->utils->seterror(params->utils->conn, 0,
                                "Invalid server proof (truncated) in %s input",
                                scram_sasl_mech);
	result = SASL_BADPROT;
	goto cleanup;
    }

    /* ServerKey       := HMAC(SaltedPassword, "Server Key") */
    if (HMAC(text->md,
	     (const unsigned char *)text->SaltedPassword,
	     hash_size,
	     (const unsigned char *) SERVER_KEY_CONSTANT,
	     SERVER_KEY_CONSTANT_LEN,
	     (unsigned char *)ServerKey,
	     &hash_len) == NULL) {
	params->utils->seterror(params->utils->conn,0,
				"HMAC-%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    /* ServerSignature := HMAC(ServerKey, AuthMessage) */
    if (HMAC(text->md,
	     (const unsigned char *)ServerKey,
	     hash_size,
	     (const unsigned char *) text->auth_message,
	     (int)text->auth_message_len,
	     (unsigned char *)ServerSignature,
	     &hash_len) == NULL) {
	params->utils->seterror(params->utils->conn,0,
				"HMAC-%s call failed", scram_sasl_mech+6);
	result = SASL_SCRAM_INTERNAL;
	goto cleanup;
    }

    for (k = 0; k < hash_size; k++) {
	if (DecodedServerProof[k] != ServerSignature[k]) {
	    SETERROR(params->utils, "ServerSignature mismatch");
	    result = SASL_BADAUTH;
	    goto cleanup;
	}
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
    return result;
}

static int scram_client_mech_step(void *conn_context,
				  sasl_client_params_t *params,
				  const char *serverin,
				  unsigned serverinlen,
				  sasl_interact_t **prompt_need,
				  const char **clientout,
				  unsigned *clientoutlen,
				  sasl_out_params_t *oparams)
{
    int result = SASL_FAIL;
    client_context_t *text = (client_context_t *) conn_context;
    const char *scram_sasl_mech = scram_sasl_mech_name(EVP_MD_size(text->md));

    *clientout = NULL;
    *clientoutlen = 0;

    /* this should be well more than is ever needed */
    if (serverinlen > MAX_SERVERIN_LEN) {
	params->utils->seterror(params->utils->conn, 0,
                                "%s input longer than " STRINGIZE((MAX_SERVERIN_LEN)) " bytes",
                                scram_sasl_mech);
	return SASL_BADPROT;
    }
    
    switch (text->state) {
    case 0:
	result = scram_client_mech_step1(text,
				       params,
				       serverin,
				       serverinlen,
				       prompt_need,
				       clientout,
				       clientoutlen,
				       oparams);
	break;

    case 1:
	result = scram_client_mech_step2(text,
				       params,
				       serverin,
				       serverinlen,
				       prompt_need,
				       clientout,
				       clientoutlen,
				       oparams);
	break;

    case 2:
	result = scram_client_mech_step3(text,
				       params,
				       serverin,
				       serverinlen,
				       prompt_need,
				       clientout,
				       clientoutlen,
				       oparams);
	break;

    default: /* should never get here */
	params->utils->log(NULL, SASL_LOG_ERR,
			   "Invalid %s client step %d\n",
                           scram_sasl_mech, text->state);
	return SASL_FAIL;
    }
    
    if (result != SASL_INTERACT) {
	text->state++;
    }
    return result;
}

static void scram_client_mech_dispose(void *conn_context,
				      const sasl_utils_t *utils)
{
    client_context_t *text = (client_context_t *) conn_context;
    
    if (!text) return;
    
    /* get rid of all sensitive info */
    if (text->free_password) {
	_plug_free_secret(utils, &text->password);
	text->free_password = 0;
    }

    if (text->gs2_header) {
	utils->free(text->gs2_header);
	text->gs2_header = NULL;
    }

    if (text->out_buf) {
	utils->free(text->out_buf);
	text->out_buf = NULL;
    }
    
    if (text->auth_message) _plug_free_string(utils,&(text->auth_message));
    if (text->nonce) _plug_free_string(utils,&(text->nonce));
    if (text->salt) utils->free(text->salt);

    utils->free(text);
}

static sasl_client_plug_t scram_client_plugins[] = 
{
#ifdef HAVE_SHA512
    {
	"SCRAM-SHA-512",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(512) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING, 	/* features */
	NULL,				/* required_prompts */
	"SHA512",			/* glob_context */
	&scram_client_mech_new,		/* mech_new */
	&scram_client_mech_step,	/* mech_step */
	&scram_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    },
    {
	"SCRAM-SHA-384",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(384) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING, 	/* features */
	NULL,				/* required_prompts */
	"SHA384",			/* glob_context */
	&scram_client_mech_new,		/* mech_new */
	&scram_client_mech_step,	/* mech_step */
	&scram_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    },
    {
	"SCRAM-SHA-256",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(256) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING, 	/* features */
	NULL,				/* required_prompts */
	"SHA256",			/* glob_context */
	&scram_client_mech_new,		/* mech_new */
	&scram_client_mech_step,	/* mech_step */
	&scram_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    },
    {
	"SCRAM-SHA-224",		/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(224) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING, 	/* features */
	NULL,				/* required_prompts */
	"SHA224",			/* glob_context */
	&scram_client_mech_new,		/* mech_new */
	&scram_client_mech_step,	/* mech_step */
	&scram_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    },
#endif
    {
	"SCRAM-SHA-1",			/* mech_name */
	0,				/* max_ssf */
	SASL_SET_HASH_STRENGTH_BITS(160) |
	SASL_SEC_NOPLAINTEXT
	| SASL_SEC_NOANONYMOUS
	| SASL_SEC_NOACTIVE
	| SASL_SEC_MUTUAL_AUTH,		/* security_flags */
	SASL_FEAT_ALLOWS_PROXY
        | SASL_FEAT_SUPPORTS_HTTP
	| SASL_FEAT_CHANNEL_BINDING, 	/* features */
	NULL,				/* required_prompts */
	"SHA1",				/* glob_context */
	&scram_client_mech_new,		/* mech_new */
	&scram_client_mech_step,	/* mech_step */
	&scram_client_mech_dispose,	/* mech_dispose */
	NULL,				/* mech_free */
	NULL,				/* idle */
	NULL,				/* spare */
	NULL				/* spare */
    }
};

int scram_client_plug_init(const sasl_utils_t *utils,
			     int maxversion,
			     int *out_version,
			     sasl_client_plug_t **pluglist,
			     int *plugcount)
{
    if (maxversion < SASL_CLIENT_PLUG_VERSION) {
	SETERROR( utils, "SCRAM-SHA-* version mismatch");
	return SASL_BADVERS;
    }
    
    *out_version = SASL_CLIENT_PLUG_VERSION;
    *pluglist = scram_client_plugins;
#ifdef HAVE_SHA512
    *plugcount = 5;
#else
    *plugcount = 1;
#endif
    
    return SASL_OK;
}
