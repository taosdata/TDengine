/* db_berkeley.c--SASL berkeley db interface
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

#include <sys/stat.h>
#include <stdlib.h>
#include "sasldb.h"

/*
 * Construct a key
 *
 */
int _sasldb_alloc_key(const sasl_utils_t *utils,
		      const char *auth_identity,
		      const char *realm,
		      const char *propName,
		      char **key,
		      size_t *key_len)
{
  size_t auth_id_len, realm_len, prop_len;

  if(!utils || !auth_identity || !realm || !propName || !key || !key_len)
	return SASL_BADPARAM;

  auth_id_len = strlen(auth_identity);
  realm_len = strlen(realm);
  prop_len = strlen(propName);

  *key_len = auth_id_len + realm_len + prop_len + 2;
  *key = utils->malloc(*key_len);
  if (! *key)
    return SASL_NOMEM;
  memcpy(*key, auth_identity, auth_id_len);
  (*key)[auth_id_len] = '\0';
  memcpy(*key + auth_id_len + 1, realm, realm_len);
  (*key)[auth_id_len + realm_len + 1] = '\0';
  memcpy(*key + auth_id_len + realm_len + 2, propName, prop_len);

  return SASL_OK;
}

/*
 * decode a key
 */
int _sasldb_parse_key(const char *key, const size_t key_len,
		      char *authid, const size_t max_authid,
		      char *realm, const size_t max_realm,
		      char *propName, const size_t max_propname) 
{
    unsigned i = 0;
    unsigned numnulls = 0;
    size_t alen = 0, rlen = 0, pnlen = 0;

    if(!key || !key_len
       || (authid && !max_authid)
       || (realm && !max_realm)
       || (propName && !max_propname))
	return SASL_BADPARAM;

    for(i=0; i<key_len; i++) {
	if(key[i] == '\0') numnulls++;
    }

    if(numnulls != 2) return SASL_BADPARAM;

    alen = strlen(key);
    rlen = strlen(key + alen + 1);
    pnlen = key_len - alen - rlen - 2;
    

    if(authid) {
	if(alen >= max_authid)
	    return SASL_BUFOVER;
	strncpy(authid, key, max_authid);
    }

    if(realm) {
	if(rlen >= max_realm)
	    return SASL_BUFOVER;
	strncpy(realm, key + alen + 1, max_realm);
    }
    
    if(propName) {
	if(pnlen >= max_propname)
	    return SASL_BUFOVER;
	strncpy(propName, key + alen + rlen + 2, pnlen);

	/* Have to add the missing NULL */
	propName[pnlen] = '\0';
    }

    return SASL_OK;
}

/* These are more or less aliases to the correct functions */
int _sasldb_getsecret(const sasl_utils_t *utils,
		      sasl_conn_t *context,
		      const char *authid,
		      const char *realm,
		      sasl_secret_t ** secret) 
{
    char buf[8192];
    size_t len;
    sasl_secret_t *out;
    int ret;
    const char *param = SASL_AUX_PASSWORD;
    param++;
    
    if(!secret) {
	utils->seterror(context, 0, "No secret pointer in _sasldb_getsecret");
	return SASL_BADPARAM;
    }

    ret = _sasldb_getdata(utils, context, authid, realm, param,
			  buf, 8192, &len);
    
    if(ret != SASL_OK) {
	return ret;
    }
    
    out = utils->malloc(sizeof(sasl_secret_t) + len);
    if(!out) {
	utils->seterror(context, 0, "Out of Memory in _sasldb_getsecret");
	return SASL_NOMEM;
    }

    out->len = (unsigned) len;
    memcpy(out->data, buf, len);
    out->data[len]='\0';

    *secret = out;

    return SASL_OK;
}

int _sasldb_putsecret(const sasl_utils_t *utils,
		      sasl_conn_t *context,
		      const char *authid,
		      const char *realm,
		      const sasl_secret_t * secret) 
{
    const char *param = SASL_AUX_PASSWORD;
    param++; /* skip leading * */
    return _sasldb_putdata(utils, context, authid, realm, param,
			   (const char *) (secret ? secret->data : NULL),
			   (secret ? secret->len : 0));
}

int __sasldb_internal_list (const char *authid,
			    const char *realm,
			    const char *property,
			    void *rock __attribute__((unused)))
{
    printf("%s@%s: %s\n", authid, realm, property);

    return (SASL_OK);
}

/* List all users in database */
int _sasldb_listusers (const sasl_utils_t *utils,
		       sasl_conn_t *context,
		       sasldb_list_callback_t callback,
		       void *callback_rock)
{
    int result;
    char key_buf[32768];
    size_t key_len;
    sasldb_handle dbh;

    if (callback == NULL) {
	callback = &__sasldb_internal_list;
	callback_rock = NULL;
    }

    dbh = _sasldb_getkeyhandle(utils, context);

    if(!dbh) {
	utils->log (context, SASL_LOG_ERR, "_sasldb_getkeyhandle has failed");
	return SASL_FAIL;
    }

    result = _sasldb_getnextkey(utils,
			        dbh,
				key_buf,
				32768,
				&key_len);

    while (result == SASL_CONTINUE)
    {
	char authid_buf[16384];
	char realm_buf[16384];
	char property_buf[16384];
	int ret;

	ret = _sasldb_parse_key(key_buf, key_len,
				authid_buf, 16384,
				realm_buf, 16384,
				property_buf, 16384);

	if(ret == SASL_BUFOVER) {
	    utils->log (context, SASL_LOG_ERR, "Key is too large in _sasldb_parse_key");
	    continue;
	} else if(ret != SASL_OK) {
	    utils->log (context, SASL_LOG_ERR, "Bad Key in _sasldb_parse_key");
	    continue;
	}
	
	result = callback (authid_buf,
			   realm_buf,
			   property_buf,
			   callback_rock);

	if (result != SASL_OK && result != SASL_CONTINUE) {
	    break;
	}

	result = _sasldb_getnextkey(utils,
				    dbh,
				    key_buf,
				    32768,
				    &key_len);
    }

    if (result == SASL_BUFOVER) {
	utils->log (context, SASL_LOG_ERR, "Key is too large in _sasldb_getnextkey");
    } else if (result != SASL_OK) {
	utils->log (context, SASL_LOG_ERR, "DB failure in _sasldb_getnextkey");
    }

    return _sasldb_releasekeyhandle(utils, dbh);
}
