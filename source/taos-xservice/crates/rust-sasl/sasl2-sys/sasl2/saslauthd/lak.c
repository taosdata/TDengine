/* COPYRIGHT
 * Copyright (c) 2002-2003 Igor Brezac
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY IGOR BREZAC. ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL IGOR BREZAC OR
 * ITS EMPLOYEES OR AGENTS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 * END COPYRIGHT */

#ifndef AUTH_LDAP
	#include "mechanisms.h"
	#include "utils.h"
#endif

#ifdef AUTH_LDAP

#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <ctype.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef HAVE_CRYPT_H
#include <crypt.h>
#endif

#ifdef HAVE_OPENSSL
#ifndef OPENSSL_DISABLE_OLD_DES_SUPPORT
#define OPENSSL_DISABLE_OLD_DES_SUPPORT
#endif
#include <openssl/evp.h>
#include <openssl/des.h>

/* for legacy libcrypto support */
#include "crypto-compat.h"
#endif

#define LDAP_DEPRECATED 1
#include <ldap.h>
#include <lber.h>
#include <sasl.h>
#include "lak.h"

typedef struct lak_auth_method {
	int method;
	int (*check) (LAK *lak, const char *user, const char *service, const char *realm, const char *password) ;
} LAK_AUTH_METHOD;

typedef struct lak_hash_rock {
	const char *mda;
	int salted;
} LAK_HASH_ROCK;

typedef struct lak_password_scheme {
	char *hash;
	int (*check) (const char *cred, const char *passwd, void *rock);
	void *rock;
} LAK_PASSWORD_SCHEME;

static int lak_config_read(LAK_CONF *, const char *);
static int lak_config_int(const char *);
static int lak_config_switch(const char *);
static void lak_config_free(LAK_CONF *);
static int lak_config(const char *, LAK_CONF **);
static int lak_escape(const char *, const unsigned int, char **);
static int lak_tokenize_domains(const char *, int, char **);
static int lak_expand_tokens(const char *, const char *, const char *, const char *, const char *, char **);
static int lak_connect(LAK *);
static int lak_bind(LAK *, LAK_USER *);
static void lak_unbind(LAK *);
static int lak_auth_custom(LAK *, const char *, const char *, const char *, const char *);
static int lak_auth_bind(LAK *, const char *, const char *, const char *, const char *);
static int lak_auth_fastbind(LAK *, const char *, const char *, const char *, const char *);
static int lak_group_member(LAK *, const char *, const char *, const char *, const char *);
#if 0 /* unused */
static char *lak_result_get(const LAK_RESULT *, const char *);
#endif
static int lak_result_add(const char *, const char *, LAK_RESULT **);
static int lak_check_password(const char *, const char *, void *);
static int lak_check_crypt(const char *, const char *, void *);
#ifdef HAVE_OPENSSL
static int lak_base64_decode(const char *, char **, int *);
static int lak_check_hashed(const char *, const char *, void *);
#endif
static int lak_sasl_interact(LDAP *, unsigned, void *, void *);
static int lak_user(const char *, const char *, const char *, const char *, const char *, const char *, LAK_USER **);
static int lak_user_copy(LAK_USER **, const LAK_USER *);
static int lak_user_cmp(const LAK_USER *, const LAK_USER *);
static void lak_user_free(LAK_USER *);

static LAK_AUTH_METHOD authenticator[] = {
	{ LAK_AUTH_METHOD_BIND, lak_auth_bind },
	{ LAK_AUTH_METHOD_CUSTOM, lak_auth_custom },
	{ LAK_AUTH_METHOD_FASTBIND, lak_auth_fastbind },
	{ -1, NULL }
};

static LAK_HASH_ROCK hash_rock[] = {
	{ "md5", 0 },
	{ "md5", 1 },
	{ "sha1", 0 },
	{ "sha1", 1 }
};

static LAK_PASSWORD_SCHEME password_scheme[] = {
	{ "{CRYPT}", lak_check_crypt, NULL },
	{ "{UNIX}", lak_check_crypt, NULL },
#ifdef HAVE_OPENSSL
	{ "{MD5}", lak_check_hashed, &hash_rock[0] },
	{ "{SMD5}", lak_check_hashed, &hash_rock[1] },
	{ "{SHA}", lak_check_hashed, &hash_rock[2] },
	{ "{SSHA}", lak_check_hashed, &hash_rock[3] },
#endif
	{ NULL, NULL, NULL }
};

static const char *dn_attr = "dn";

#define ISSET(x)  ((x != NULL) && (*(x) != '\0'))
#define EMPTY(x)  ((x == NULL) || (*(x) == '\0'))

static int lak_config_read(
	LAK_CONF *conf,
	const char *configfile)
{
	FILE *infile;
	int lineno = 0;
	char buf[4096];
	char *p, *key;

	infile = fopen(configfile, "r");
	if (!infile) {
	    syslog(LOG_ERR|LOG_AUTH,
		   "Could not open saslauthd config file: %s (%m)",
		   configfile);
	    return LAK_FAIL;
	}
    
	while (fgets(buf, sizeof(buf), infile)) {
		lineno++;

		if (buf[strlen(buf)-1] == '\n') 
			buf[strlen(buf)-1] = '\0';
		for (p = buf; *p && isspace((int) *p); p++);
		if (!*p || *p == '#') 
		    continue;

		key = p;
		while (*p && (isalnum((int) *p) || *p == '-' || *p == '_')) {
			if (isupper((int) *p)) 
				*p = tolower(*p);
			p++;
		}
		if (*p != ':') {
			fclose(infile);
			return LAK_FAIL;
		}
		
		*p++ = '\0';

		while (*p && isspace((int) *p)) 
			p++;

		if (!*p) {
			fclose(infile);
			return LAK_FAIL;
		}

		if (!strcasecmp(key, "ldap_servers"))
			strlcpy(conf->servers, p, LAK_URL_LEN);

		else if (!strcasecmp(key, "ldap_bind_dn"))
			strlcpy(conf->bind_dn, p, LAK_DN_LEN);

		else if (!strcasecmp(key, "ldap_bind_pw") ||
		         !strcasecmp(key, "ldap_password"))
			strlcpy(conf->password, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_version"))
			conf->version = lak_config_int(p);

		else if (!strcasecmp(key, "ldap_search_base"))
			strlcpy(conf->search_base, p, LAK_DN_LEN);

		else if (!strcasecmp(key, "ldap_filter"))
			strlcpy(conf->filter, p, LAK_DN_LEN);
		
		else if (!strcasecmp(key, "ldap_password_attr"))
			strlcpy(conf->password_attr, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_group_dn"))
			strlcpy(conf->group_dn, p, LAK_DN_LEN);
		
		else if (!strcasecmp(key, "ldap_group_attr"))
			strlcpy(conf->group_attr, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_group_filter"))
			strlcpy(conf->group_filter, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_group_search_base"))
			strlcpy(conf->group_search_base, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_group_scope")) {
			if (!strcasecmp(p, "one")) {
				conf->group_scope = LDAP_SCOPE_ONELEVEL;
			} else if (!strcasecmp(p, "base")) {
				conf->group_scope = LDAP_SCOPE_BASE;
			}
		} else if (!strcasecmp(key, "ldap_group_match_method")) {
			if (!strcasecmp(p, "filter")) {
				conf->group_match_method = LAK_GROUP_MATCH_METHOD_FILTER;
			} else if (!strcasecmp(p, "attr")) {
				conf->group_match_method = LAK_GROUP_MATCH_METHOD_ATTR;
			}
		} else if (!strcasecmp(key, "ldap_default_realm") ||
		         !strcasecmp(key, "ldap_default_domain"))
			strlcpy(conf->default_realm, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_auth_method")) {
			if (!strcasecmp(p, "custom")) {
				conf->auth_method = LAK_AUTH_METHOD_CUSTOM;
			} else if (!strcasecmp(p, "fastbind")) {
				conf->auth_method = LAK_AUTH_METHOD_FASTBIND;
			}
		} else if (!strcasecmp(key, "ldap_timeout")) {
			conf->timeout.tv_sec = lak_config_int(p);
			conf->timeout.tv_usec = 0;
		} else if (!strcasecmp(key, "ldap_size_limit"))
			conf->size_limit = lak_config_int(p);

		else if (!strcasecmp(key, "ldap_time_limit"))
			conf->time_limit = lak_config_int(p);

		else if (!strcasecmp(key, "ldap_deref")) {
			if (!strcasecmp(p, "search")) {
				conf->deref = LDAP_DEREF_SEARCHING;
			} else if (!strcasecmp(p, "find")) {
				conf->deref = LDAP_DEREF_FINDING;
			} else if (!strcasecmp(p, "always")) {
				conf->deref = LDAP_DEREF_ALWAYS;
			} else if (!strcasecmp(p, "never")) {
				conf->deref = LDAP_DEREF_NEVER;
			}
		} else if (!strcasecmp(key, "ldap_referrals")) {
			conf->referrals = lak_config_switch(p);

		} else if (!strcasecmp(key, "ldap_restart")) {
			conf->restart = lak_config_switch(p);

		} else if (!strcasecmp(key, "ldap_scope")) {
			if (!strcasecmp(p, "one")) {
				conf->scope = LDAP_SCOPE_ONELEVEL;
			} else if (!strcasecmp(p, "base")) {
				conf->scope = LDAP_SCOPE_BASE;
			}
		} else if (!strcasecmp(key, "ldap_use_sasl")) {
			conf->use_sasl = lak_config_switch(p);

		} else if (!strcasecmp(key, "ldap_id") ||
                   !strcasecmp(key, "ldap_sasl_authc_id"))
			strlcpy(conf->id, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_authz_id") ||
                 !strcasecmp(key, "ldap_sasl_authz_id"))
			strlcpy(conf->authz_id, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_realm") ||
                 !strcasecmp(key, "ldap_sasl_realm"))
			strlcpy(conf->realm, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_mech") ||
                 !strcasecmp(key, "ldap_sasl_mech"))
			strlcpy(conf->mech, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_sasl_secprops"))
			strlcpy(conf->sasl_secprops, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_start_tls"))
			conf->start_tls = lak_config_switch(p);

		else if (!strcasecmp(key, "ldap_tls_check_peer"))
			conf->tls_check_peer = lak_config_switch(p);

		else if (!strcasecmp(key, "ldap_tls_cacert_file"))
			strlcpy(conf->tls_cacert_file, p, LAK_PATH_LEN);

		else if (!strcasecmp(key, "ldap_tls_cacert_dir"))
			strlcpy(conf->tls_cacert_dir, p, LAK_PATH_LEN);

		else if (!strcasecmp(key, "ldap_tls_ciphers"))
			strlcpy(conf->tls_ciphers, p, LAK_BUF_LEN);

		else if (!strcasecmp(key, "ldap_tls_cert"))
			strlcpy(conf->tls_cert, p, LAK_PATH_LEN);

		else if (!strcasecmp(key, "ldap_tls_key"))
			strlcpy(conf->tls_key, p, LAK_PATH_LEN);

		else if (!strcasecmp(key, "ldap_debug"))
			conf->debug = lak_config_int(p);
	}

	if (conf->version != LDAP_VERSION3 && 
	    (conf->use_sasl ||
	     conf->start_tls))
	    conf->version = LDAP_VERSION3;

    if (conf->use_sasl &&
        conf->auth_method == LAK_AUTH_METHOD_BIND)
        conf->auth_method = LAK_AUTH_METHOD_FASTBIND;

    if ( ISSET(conf->group_filter) &&
         ISSET(conf->search_base) &&
         EMPTY(conf->group_search_base) )
        strlcpy(conf->group_search_base, conf->search_base, LAK_DN_LEN);
        
	fclose(infile);

	return LAK_OK;
}

static int lak_config_int(
	const char *val)
{
    if (!val) return 0;

    if (!isdigit((int) *val) && (*val != '-' || !isdigit((int) val[1]))) return 0;

    return atoi(val);
}

static int lak_config_switch(
	const char *val)
{
    if (!val) return 0;
    
    if (*val == '0' || *val == 'n' ||
	(*val == 'o' && val[1] == 'f') || *val == 'f') {
	return 0;
    } else if (*val == '1' || *val == 'y' ||
	     (*val == 'o' && val[1] == 'n') || *val == 't') {
	return 1;
    }
    return 0;
}

static int lak_config(
	const char *configfile, 
	LAK_CONF **ret)
{
	LAK_CONF *conf;
	int rc = 0;

	conf = malloc( sizeof(LAK_CONF) );
	if (conf == NULL) {
		return LAK_NOMEM;
	}

	memset(conf, 0, sizeof(LAK_CONF));

	strlcpy(conf->servers, "ldap://localhost/", LAK_BUF_LEN);
	conf->version = LDAP_VERSION3;
	strlcpy(conf->filter, "(uid=%u)", LAK_DN_LEN);
	strlcpy(conf->password_attr, "userPassword", LAK_BUF_LEN);
	conf->scope = LDAP_SCOPE_SUBTREE;
	strlcpy(conf->group_attr, "uniqueMember", LAK_BUF_LEN);
	conf->group_scope = LDAP_SCOPE_SUBTREE;
    conf->group_match_method = LAK_GROUP_MATCH_METHOD_ATTR;
	conf->auth_method = LAK_AUTH_METHOD_BIND;
	conf->timeout.tv_sec = 5;
	conf->timeout.tv_usec = 0;
	conf->size_limit = 1;
	conf->time_limit = 5;
	conf->deref = LDAP_DEREF_NEVER;
	conf->restart = 1;
	conf->start_tls = 0;
	conf->use_sasl = 0;
	conf->tls_check_peer = -1;

	strlcpy(conf->path, configfile, LAK_PATH_LEN);

	rc = lak_config_read(conf, conf->path);
	if (rc != LAK_OK) {
		lak_config_free(conf);
		return rc;
	}

	*ret = conf;
	return LAK_OK;
}

static void lak_config_free(
	LAK_CONF *conf) 
{
	if (conf == NULL) {
		return;
	}

	memset(conf, 0, sizeof(LAK_CONF));

	free (conf);

	return;
}

/*
 * Note: calling function must free memory.
 */
static int lak_escape(
	const char *s, 
	const unsigned int n, 
	char **result) 
{
	char *buf;
	char *end, *ptr, *temp;

	if (n > strlen(s))  // Sanity check, just in case
		return LAK_FAIL;

	buf = malloc(n * 5 + 1);
	if (buf == NULL) {
		return LAK_NOMEM;
	}

	buf[0] = '\0';
	ptr = (char *)s;
	end = ptr + n;

	while (((temp = strpbrk(ptr, "*()\\\0"))!=NULL) && (temp<end)) {

		if (temp>ptr)
			strncat(buf, ptr, temp-ptr);

		switch (*temp) {
			case '*':
				strcat(buf, "\\2a");
				break;
			case '(':
				strcat(buf, "\\28");
				break;
			case ')':
				strcat(buf, "\\29");
				break;
			case '\\':
				strcat(buf, "\\5c");
				break;
			case '\0':
				strcat(buf, "\\00");
				break;
		}
		ptr=temp+1;
	}
	if (ptr<end)
		strncat(buf, ptr, end-ptr);

	*result = buf;

	return LAK_OK;
}

static int lak_tokenize_domains(
	const char *d, 
	int n, 
	char **result)
{
	char *s, *s1;
	char *lasts;
	int nt, i, rc;

	*result = NULL;

	if (d == NULL || n < 1 || n > 9)
		return LAK_FAIL;

	s = strdup(d);
	if (s == NULL)
		return LAK_NOMEM;

	for( nt=0, s1=s; *s1; s1++ )
		if( *s1 == '.' ) nt++;
	nt++;

	if (n > nt) {
		free(s);
		return LAK_FAIL;
	}

	i = nt - n;
	s1 = (char *)strtok_r(s, ".", &lasts);
	while(s1) {
		if (i == 0) {
			rc = lak_escape(s1, strlen(s1), result);
			free(s);
			return rc;
		}
		s1 = (char *)strtok_r(NULL, ".", &lasts);
		i--;
	}

	free(s);
	return LAK_FAIL;
}

#define LAK_MAX(a,b) (a>b?a:b)

/*
 * lak_expand_tokens
 * Parts with the strings provided.
 *   %%   = %
 *   %u   = user
 *   %U   = user part of %u
 *   %d   = domain part of %u if available, othwise same as %r
 *   %1-9 = domain if not available realm, token
 *          (%1 = tld, %2 = domain when %r = domain.tld)
 *   %s   = service
 *   %r   = realm
 *   %R   = prepend '@' to realm
 *   %D   = user DN
 * Note: calling function must free memory.
 */
static int lak_expand_tokens(
	const char *pattern,
	const char *username, 
	const char *service,
	const char *realm,
	const char *dn,
	char **result) 
{
	char *buf; 
	char *end, *ptr, *temp;
	char *ebuf, *user;
	char *domain;
	int rc;

	/* to permit multiple occurences of username and/or realm in filter */
	/* and avoid memory overflow in filter build [eg: (|(uid=%u)(userid=%u)) ] */
	int percents, service_len, realm_len, dn_len, user_len, maxparamlength;
	
	if (pattern == NULL) {
		syslog(LOG_ERR|LOG_AUTH, "filter pattern not setup");
		return LAK_FAIL;
	}

	/* find the longest param of username and realm, 
	   do not worry about domain because it is always shorter 
	   then username                                           */
	user_len=ISSET(username) ? strlen(username) : 0;
	service_len=ISSET(service) ? strlen(service) : 0;
	realm_len=ISSET(realm) ? strlen(realm) : 0;
	dn_len=ISSET(dn) ? strlen(dn) : 0;

	maxparamlength = LAK_MAX(user_len, service_len);
	maxparamlength = LAK_MAX(maxparamlength, realm_len + 1);  /* +1 for %R when '@' is prepended */
	maxparamlength = LAK_MAX(maxparamlength, dn_len);

	/* find the number of occurences of percent sign in filter */
	for( percents=0, buf=(char *)pattern; *buf; buf++ ) {
		if( *buf == '%' ) percents++;
	}

	/* percents * 3 * maxparamlength because we need to account for
         * an entirely-escaped worst-case-length parameter */
	buf=malloc(strlen(pattern) + (percents * 3 * maxparamlength) + 1);
	if(buf == NULL)
		return LAK_NOMEM;
	buf[0] = '\0';
	
	ptr = (char *)pattern;
	end = ptr + strlen(ptr);

	while ((temp=strchr(ptr,'%'))!=NULL ) {

		if ((temp-ptr) > 0)
			strncat(buf, ptr, temp-ptr);

		if ((temp+1) >= end) {
			syslog(LOG_DEBUG|LOG_AUTH, "Incomplete lookup substitution format");
			break;
		}

		switch (*(temp+1)) {
			case '%':
				strncat(buf,temp+1,1);
				break;
			case 'u':
				if (ISSET(username)) {
					rc=lak_escape(username, strlen(username), &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "Username not available.");
				break;
			case 'U':
				if (ISSET(username)) {
					user = strchr(username, '@');
					rc=lak_escape(username, (user ? user - username : (unsigned) strlen(username)), &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "Username not available.");
				break;
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				if (ISSET(username) && 
				    ((domain = strchr(username, '@')) && domain[1]!='\0')) {
					rc=lak_tokenize_domains(domain+1, (int) *(temp+1)-48, &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else if (ISSET(realm)) {
					rc=lak_tokenize_domains(realm, (int) *(temp+1)-48, &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "Domain/Realm not available.");
				break;
			case 'd':
				if (ISSET(username) && 
				    ((domain = strchr(username, '@')) && domain[1]!='\0')) {
					rc=lak_escape(domain+1, strlen(domain+1), &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "Domain/Realm not available.");
                                break;
			case 'R':
			case 'r':
				if (ISSET(realm)) {
					rc = lak_escape(realm, strlen(realm), &ebuf);
					if (rc == LAK_OK) {
						if (*(temp+1) == 'R')
							strcat(buf,"@");
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "Domain/Realm not available.");
				break;
			case 's':
				if (ISSET(service)) {
					rc = lak_escape(service, strlen(service), &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "Service not available.");
				break;
			case 'D':
				if (ISSET(dn)) {
					rc = lak_escape(dn, strlen(dn), &ebuf);
					if (rc == LAK_OK) {
						strcat(buf,ebuf);
						free(ebuf);
					}
				} else
					syslog(LOG_DEBUG|LOG_AUTH, "User DN not available.");
				break;
			default:
				break;
		}
		ptr=temp+2;
	}
	if (temp<end)
		strcat(buf, ptr);

	*result = buf;

	return LAK_OK;
}

int lak_init(
	const char *configfile, 
	LAK **ret) 
{
	LAK *lak;
	int rc;

	lak = *ret;

	if (lak != NULL) {
		return LAK_OK;
	}

	lak = (LAK *)malloc(sizeof(LAK));
	if (lak == NULL)
		return LAK_NOMEM;

	lak->status=LAK_NOT_BOUND;
	lak->ld=NULL;
	lak->conf=NULL;
	lak->user=NULL;

	rc = lak_config(configfile, &lak->conf);
	if (rc != LAK_OK) {
		free(lak);
		return rc;
	}

#ifdef HAVE_OPENSSL
	OpenSSL_add_all_digests();
#endif

	*ret=lak;
	return LAK_OK;
}

void lak_close(
	LAK *lak) 
{

	if (lak == NULL)
		return;

	lak_config_free(lak->conf);

	lak_unbind(lak);

	free(lak);

#ifdef HAVE_OPENSSL
	EVP_cleanup();
#endif

	return;
}

static int lak_connect(
	LAK *lak)
{
	int rc = 0;
	char *p = NULL;

	if (ISSET(lak->conf->tls_cacert_file)) {
		rc = ldap_set_option (NULL, LDAP_OPT_X_TLS_CACERTFILE, lak->conf->tls_cacert_file);
		if (rc != LDAP_SUCCESS) {
			syslog (LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_X_TLS_CACERTFILE (%s).", ldap_err2string (rc));
		}
	}

	if (ISSET(lak->conf->tls_cacert_dir)) {
		rc = ldap_set_option (NULL, LDAP_OPT_X_TLS_CACERTDIR, lak->conf->tls_cacert_dir);
		if (rc != LDAP_SUCCESS) {
			syslog (LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_X_TLS_CACERTDIR (%s).", ldap_err2string (rc));
		}
	}

	if (lak->conf->tls_check_peer != -1) {
		rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_REQUIRE_CERT, &lak->conf->tls_check_peer);
		if (rc != LDAP_SUCCESS) {
			syslog (LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_X_TLS_REQUIRE_CERT (%s).", ldap_err2string (rc));
		}
	}

	if (ISSET(lak->conf->tls_ciphers)) {
		/* set cipher suite, certificate and private key: */
		rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_CIPHER_SUITE, lak->conf->tls_ciphers);
		if (rc != LDAP_SUCCESS) {
			syslog (LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_X_TLS_CIPHER_SUITE (%s).", ldap_err2string (rc));
		}
	}

	if (ISSET(lak->conf->tls_cert)) {
		rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_CERTFILE, lak->conf->tls_cert);
		if (rc != LDAP_SUCCESS) {
			syslog (LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_X_TLS_CERTFILE (%s).", ldap_err2string (rc));
		}
	}

	if (ISSET(lak->conf->tls_key)) {
		rc = ldap_set_option(NULL, LDAP_OPT_X_TLS_KEYFILE, lak->conf->tls_key);
		if (rc != LDAP_SUCCESS) {
			syslog (LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_X_TLS_KEYFILE (%s).", ldap_err2string (rc));
		}
	}

	rc = ldap_initialize(&lak->ld, lak->conf->servers);
	if (rc != LDAP_SUCCESS) {
		syslog(LOG_ERR|LOG_AUTH, "ldap_initialize failed (%s)", lak->conf->servers);
		return LAK_CONNECT_FAIL;
	}

	if (lak->conf->debug) {
		rc = ldap_set_option(NULL, LDAP_OPT_DEBUG_LEVEL, &(lak->conf->debug));
		if (rc != LDAP_OPT_SUCCESS)
			syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_DEBUG_LEVEL %x.", lak->conf->debug);
	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_PROTOCOL_VERSION, &(lak->conf->version));
	if (rc != LDAP_OPT_SUCCESS) {

		if (lak->conf->use_sasl ||
		    lak->conf->start_tls) {
			syslog(LOG_ERR|LOG_AUTH, "Failed to set LDAP_OPT_PROTOCOL_VERSION %d, required for ldap_start_tls and ldap_use_sasl.", lak->conf->version);
			lak_unbind(lak);
			return LAK_CONNECT_FAIL;
		} else
			syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_PROTOCOL_VERSION %d.", lak->conf->version);

		lak->conf->version = LDAP_VERSION2;

	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_NETWORK_TIMEOUT, &(lak->conf->timeout));
	if (rc != LDAP_OPT_SUCCESS) {
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_NETWORK_TIMEOUT %ld.%ld.", lak->conf->timeout.tv_sec, lak->conf->timeout.tv_usec);
	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_TIMEOUT, &(lak->conf->timeout));
	if (rc != LDAP_OPT_SUCCESS) {
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_TIMEOUT %ld.%ld.", lak->conf->timeout.tv_sec, lak->conf->timeout.tv_usec);
	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_TIMELIMIT, &(lak->conf->time_limit));
	if (rc != LDAP_OPT_SUCCESS) {
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_TIMELIMIT %d.", lak->conf->time_limit);
	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_DEREF, &(lak->conf->deref));
	if (rc != LDAP_OPT_SUCCESS) {
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_DEREF %d.", lak->conf->deref);
	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_REFERRALS, lak->conf->referrals ? LDAP_OPT_ON : LDAP_OPT_OFF);
	if (rc != LDAP_OPT_SUCCESS) {
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_REFERRALS.");
	}

	rc = ldap_set_option(lak->ld, LDAP_OPT_SIZELIMIT, &(lak->conf->size_limit));
	if (rc != LDAP_OPT_SUCCESS)
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_SIZELIMIT %d.", lak->conf->size_limit);

	rc = ldap_set_option(lak->ld, LDAP_OPT_RESTART, lak->conf->restart ? LDAP_OPT_ON : LDAP_OPT_OFF);
	if (rc != LDAP_OPT_SUCCESS) {
		syslog(LOG_WARNING|LOG_AUTH, "Unable to set LDAP_OPT_RESTART.");
	}

	if (lak->conf->start_tls) {

		rc = ldap_start_tls_s(lak->ld, NULL, NULL);
		if (rc != LDAP_SUCCESS) {
			syslog(LOG_ERR|LOG_AUTH, "start tls failed (%s).", ldap_err2string(rc));
			lak_unbind(lak);
			return LAK_CONNECT_FAIL;
		}
	}
	
	if (lak->conf->use_sasl) {

		if (EMPTY(lak->conf->mech)) {
			ldap_get_option(lak->ld, LDAP_OPT_X_SASL_MECH, &p);
			if (p)
				strlcpy(lak->conf->mech, p, LAK_BUF_LEN);
		}

		if (EMPTY(lak->conf->realm)) {
			ldap_get_option(lak->ld, LDAP_OPT_X_SASL_REALM, &p);
			if (p)
				strlcpy(lak->conf->realm, p, LAK_BUF_LEN);
		}

		if (ISSET(lak->conf->sasl_secprops)) {
			rc = ldap_set_option(lak->ld, LDAP_OPT_X_SASL_SECPROPS, (void *) lak->conf->sasl_secprops);
			if( rc != LDAP_OPT_SUCCESS ) {
				syslog(LOG_ERR|LOG_AUTH, "Unable to set LDAP_OPT_X_SASL_SECPROPS.");
				lak_unbind(lak);
				return LAK_CONNECT_FAIL;
			}
		}
	}


	return LAK_OK;
}

static int lak_user(
	const char *bind_dn, 
	const char *id, 
	const char *authz_id, 
	const char *mech, 
	const char *realm, 
	const char *password, 
	LAK_USER **ret)
{
	LAK_USER *lu = NULL;
	
	*ret = NULL;

	lu = (LAK_USER *)malloc(sizeof(LAK_USER));
	if (lu == NULL)
		return LAK_NOMEM;

	memset(lu, 0, sizeof(LAK_USER));

	if (ISSET(bind_dn))
		strlcpy(lu->bind_dn, bind_dn, LAK_DN_LEN);

	if (ISSET(id))
		strlcpy(lu->id, id, LAK_BUF_LEN);

	if (ISSET(authz_id))
		strlcpy(lu->authz_id, authz_id, LAK_BUF_LEN);

	if (ISSET(mech))
		strlcpy(lu->mech, mech, LAK_BUF_LEN);

	if (ISSET(realm))
		strlcpy(lu->realm, realm, LAK_BUF_LEN);

	if (ISSET(password))
		strlcpy(lu->password, password, LAK_BUF_LEN);
	
	*ret = lu;
	return LAK_OK;
}

static int lak_user_cmp(
	const LAK_USER *lu1,
	const LAK_USER *lu2)
{

	if (lu1 == NULL ||
	    lu2 == NULL)
		return LAK_FAIL;
	
	if (memcmp(lu1, lu2, sizeof(LAK_USER)) == 0)
		return LAK_OK;

	return LAK_FAIL;
}

static int lak_user_copy(
	LAK_USER **lu1,
	const LAK_USER *lu2)
{
	LAK_USER *lu;

	lu = *lu1;

	if (lu2 == NULL)
		return LAK_FAIL;

	if (lu == NULL) {
		lu = (LAK_USER *)malloc(sizeof(LAK_USER));
		if (lu == NULL)
			return LAK_NOMEM;
		
		*lu1 = lu;
	}

	memcpy((void *)lu, (void *)lu2, sizeof(LAK_USER));

	return LAK_OK;
}

static void lak_user_free(
	LAK_USER *user)
{
	if (user == NULL) {
		return;
	}

	memset(user, 0, sizeof(LAK_USER));

	free(user);

	return;
}

static int lak_sasl_interact(
	LDAP *ld, 
	unsigned flags __attribute__((unused)), 
	void *def, 
	void *inter)
{
	sasl_interact_t *in = inter;
	const char *p;
	LAK_USER *lu = def;

	for (;in->id != SASL_CB_LIST_END;in++) {
		p = NULL;
		switch(in->id) {
			case SASL_CB_AUTHNAME:
				if (ISSET(lu->id))
					p = lu->id;
				if (!p)
					ldap_get_option( ld, LDAP_OPT_X_SASL_AUTHCID, &p);
				break;
			case SASL_CB_USER:
				if (ISSET(lu->authz_id))
					p = lu->authz_id;
				if (!p)
					ldap_get_option( ld, LDAP_OPT_X_SASL_AUTHZID, &p);
				break;
			case SASL_CB_GETREALM:
				if (ISSET(lu->realm))
					p = lu->realm;
				break;          
			case SASL_CB_PASS:
				if (ISSET(lu->password))
					p = lu->password;
				break;
		}

		in->result = ISSET(p) ? p : "";
		in->len = strlen(in->result);
	}

	return LDAP_SUCCESS;
}

static int lak_bind(
	LAK *lak, 
	LAK_USER *user)
{
	int rc;

	if (user == NULL)  // Sanity Check
		return LAK_FAIL;

	if ((lak->status == LAK_BOUND) &&
	    (lak_user_cmp(lak->user, user) == LAK_OK))
		return LAK_OK;

	lak_user_free(lak->user);
	lak->user = NULL;

	if (
#if LDAP_VENDOR_VERSION < 20204
        lak->conf->use_sasl ||
#endif
	    lak->conf->version == LDAP_VERSION2) 
		lak->status = LAK_NOT_BOUND;

	if (lak->status == LAK_NOT_BOUND) {
		lak_unbind(lak);
		rc = lak_connect(lak);
		if (rc != LAK_OK)
			return rc;
	}

	if (lak->conf->use_sasl)
		rc = ldap_sasl_interactive_bind_s(
			lak->ld, 
			user->bind_dn,
			user->mech, 
			NULL, 
			NULL, 
			LDAP_SASL_QUIET, 
			lak_sasl_interact, 
			user);
	else
		rc = ldap_simple_bind_s(lak->ld, user->bind_dn, user->password);

	switch (rc) {
		case LDAP_SUCCESS:
			break;
        case LDAP_INVALID_CREDENTIALS:
        case LDAP_INSUFFICIENT_ACCESS:
        case LDAP_INVALID_DN_SYNTAX:
        case LDAP_OTHER: 
			lak->status = LAK_NOT_BOUND;
            return LAK_BIND_FAIL;
		case LDAP_TIMEOUT:
		case LDAP_SERVER_DOWN:
		default:
			syslog(LOG_DEBUG|LOG_AUTH,
				   (lak->conf->use_sasl ? "ldap_sasl_interactive_bind() failed %d (%s)." : "ldap_simple_bind() failed %d (%s)."), rc, ldap_err2string(rc));
			lak->status = LAK_NOT_BOUND;
			return LAK_RETRY;
	}

	rc = lak_user_copy(&(lak->user), user);
	if (rc != LAK_OK)
		return rc;

	lak->status = LAK_BOUND;

	return LAK_OK;
}

static void lak_unbind(
	LAK *lak)
{
	if (!lak)
		return;

	lak_user_free(lak->user);

	if (lak->ld)
		ldap_unbind(lak->ld);

	lak->ld = NULL;
	lak->user = NULL;
	lak->status = LAK_NOT_BOUND;

	return;
}

/* 
 * lak_retrieve - retrieve user@realm values specified by 'attrs'
 */
int lak_retrieve(
	LAK *lak, 
	const char *user, 
	const char *service, 
	const char *realm, 
	const char **attrs, 
	LAK_RESULT **ret)
{
	int rc = 0, i;
	char *filter = NULL;
	char *search_base = NULL;
	LDAPMessage *res = NULL;
	LDAPMessage *entry = NULL;
	BerElement *ber = NULL;
	char *attr = NULL, **vals = NULL, *dn = NULL;
	LAK_USER *lu = NULL;
    
	*ret = NULL;

	if (lak == NULL) {
		syslog(LOG_ERR|LOG_AUTH, "lak_init did not run.");
		return LAK_FAIL;
	}

	if (EMPTY(user))
		return LAK_FAIL;

	if (EMPTY(realm))
		realm = lak->conf->default_realm;

	rc = lak_user(	
		lak->conf->bind_dn,
		lak->conf->id,
		lak->conf->authz_id,
		lak->conf->mech,
		lak->conf->realm,
		lak->conf->password,
		&lu);
	if (rc != LAK_OK)
		return rc;

	rc = lak_bind(lak, lu);
	if (rc != LAK_OK)
		goto done;

	rc = lak_expand_tokens(lak->conf->filter, user, service, realm, NULL, &filter);
	if (rc != LAK_OK)
        goto done;

	rc = lak_expand_tokens(lak->conf->search_base, user, service, realm, NULL, &search_base);
	if (rc != LAK_OK)
        goto done;

	rc = ldap_search_st(lak->ld, search_base, lak->conf->scope, filter, (char **) attrs, 0, &(lak->conf->timeout), &res);
	switch (rc) {
		case LDAP_SUCCESS:
		case LDAP_NO_SUCH_OBJECT:
			break;
		case LDAP_TIMELIMIT_EXCEEDED:
		case LDAP_BUSY:
		case LDAP_UNAVAILABLE:
		case LDAP_INSUFFICIENT_ACCESS:
			/*  We do not need to re-connect to the LDAP server 
			    under these conditions */
			syslog(LOG_ERR|LOG_AUTH, "user ldap_search_st() failed: %s", ldap_err2string(rc));
            rc = LAK_USER_NOT_FOUND;
			goto done;
		case LDAP_TIMEOUT:
		case LDAP_SERVER_DOWN:
		default:
			syslog(LOG_ERR|LOG_AUTH, "user ldap_search_st() failed: %s", ldap_err2string(rc));
            rc = LAK_RETRY;
			lak->status = LAK_NOT_BOUND;
			goto done;
	}

    i = ldap_count_entries(lak->ld, res);
    if (i != 1) {
        if (i == 0)
			syslog(LOG_DEBUG|LOG_AUTH, "Entry not found (%s).", filter);
        else
            syslog(LOG_DEBUG|LOG_AUTH, "Duplicate entries found (%s).", filter);
        rc = LAK_USER_NOT_FOUND;
        goto done;
    }
	
    rc = LAK_FAIL;

	if ((entry = ldap_first_entry(lak->ld, res)) != NULL)  {
        for (i=0; attrs[i] != NULL; i++) {
            
            if (!strcmp(attrs[i], dn_attr)) {
                dn = ldap_get_dn(lak->ld, entry);
                if (dn == NULL)
                    goto done;

                rc = lak_result_add(dn_attr, dn, ret);
                if (rc != LAK_OK) {
                    lak_result_free(*ret);
                    *ret = NULL;
                    goto done;
                }
            }
        }

        for (attr = ldap_first_attribute(lak->ld, entry, &ber); attr != NULL; 
            attr = ldap_next_attribute(lak->ld, entry, ber)) {

            vals = ldap_get_values(lak->ld, entry, attr);
            if (vals == NULL)
                continue;

            for (i = 0; vals[i] != NULL; i++) {
                rc = lak_result_add(attr, vals[i], ret);
                if (rc != LAK_OK) {
                    lak_result_free(*ret);
                    *ret = NULL;
                    goto done;
                }
            }

            ldap_value_free(vals);
            vals = NULL;
            ldap_memfree(attr);
            attr = NULL;
        }
    }

done:;
	if (res)
		ldap_msgfree(res);
	if (dn)
		ldap_memfree(dn);
	if (vals)
		ldap_value_free(vals);
	if (attr)
		ldap_memfree(attr);
	if (ber != NULL)
		ber_free(ber, 0);
	if (filter)
		free(filter);
	if (search_base)
		free(search_base);
	if (lu)
		lak_user_free(lu);

	return rc;
}

static int lak_group_member(
	LAK *lak, 
	const char *user, 
	const char *service, 
	const char *realm, 
	const char *dn)
{
	char *group_dn = NULL, *user_dn = NULL;
    char *group_filter = NULL;
    char *group_search_base = NULL;
	struct berval *dn_bv = NULL;
	int rc;
    LAK_RESULT *lres = NULL;
    const char *attrs[] = { dn_attr, NULL };
    const char *group_attrs[] = {"1.1", NULL};

	LDAPMessage *res = NULL;

    user_dn = (char *)dn;

    if (EMPTY(user_dn)) {
        if (lak->conf->use_sasl) {

#if LDAP_VENDOR_VERSION >= 20122
            if (ldap_whoami_s(lak->ld, &dn_bv, NULL, NULL) != LDAP_SUCCESS || !dn_bv) {
                syslog(LOG_ERR|LOG_AUTH, "ldap_whoami_s() failed.");
                rc =  LAK_NOT_GROUP_MEMBER;
                goto done;
            }

            user_dn = dn_bv->bv_val;
#else
            syslog(LOG_ERR|LOG_AUTH, "Your OpenLDAP API does not supported ldap_whoami().");
            rc =  LAK_NOT_GROUP_MEMBER;
            goto done;
#endif

        } else {
            
            rc = lak_retrieve(lak, user, service, realm, attrs, &lres);
            if (rc != LAK_OK)
                goto done;

            user_dn = lres->value;
        }
    }

    if (lak->conf->group_match_method == LAK_GROUP_MATCH_METHOD_ATTR) {

            rc = lak_expand_tokens(lak->conf->group_dn, user, service, realm, NULL, &group_dn);
            if (rc != LAK_OK)
                goto done;

            rc = ((ldap_compare_s(lak->ld, group_dn, lak->conf->group_attr, user_dn)) == LDAP_COMPARE_TRUE ? 
                     LAK_OK : LAK_NOT_GROUP_MEMBER);

    } else if (lak->conf->group_match_method == LAK_GROUP_MATCH_METHOD_FILTER) {

        rc = lak_expand_tokens(lak->conf->group_filter, user, service, realm, user_dn, &group_filter);
        if (rc != LAK_OK)
            goto done;

        rc = lak_expand_tokens(lak->conf->group_search_base, user, service, realm, user_dn, &group_search_base);
        if (rc != LAK_OK)
            goto done;

        rc = ldap_search_st(lak->ld, group_search_base, lak->conf->group_scope, group_filter, (char **) group_attrs, 0, &(lak->conf->timeout), &res);
        switch (rc) {
            case LDAP_SUCCESS:
            case LDAP_NO_SUCH_OBJECT:
                break;
            case LDAP_TIMELIMIT_EXCEEDED:
            case LDAP_BUSY:
            case LDAP_UNAVAILABLE:
            case LDAP_INSUFFICIENT_ACCESS:
                syslog(LOG_ERR|LOG_AUTH, "group ldap_search_st() failed: %s", ldap_err2string(rc));
                rc = LAK_NOT_GROUP_MEMBER;
                goto done;
            case LDAP_TIMEOUT:
            case LDAP_SERVER_DOWN:
            default:
                syslog(LOG_ERR|LOG_AUTH, "group ldap_search_st() failed: %s", ldap_err2string(rc));
                rc = LAK_RETRY;
                lak->status = LAK_NOT_BOUND;
                goto done;
        }

        rc = ( (ldap_count_entries(lak->ld, res) >= 1) ? LAK_OK : LAK_NOT_GROUP_MEMBER );

    } else {

            syslog(LOG_WARNING|LOG_AUTH, "Unknown ldap_group_match_method value.");
            rc = LAK_FAIL;

    }

done:;
    if (res)
        ldap_msgfree(res);
    if (group_dn)
        free(group_dn);
    if (group_filter)
        free(group_filter);
    if (group_search_base)
        free(group_search_base);
    if (lres)
        lak_result_free(lres);
    if (dn_bv)
        ber_bvfree(dn_bv);

    return rc;
}

static int lak_auth_custom(
	LAK *lak,
	const char *user,
	const char *service,
	const char *realm,
	const char *password) 
{
	LAK_RESULT *lres;
	int rc;
	const char *attrs[] = { lak->conf->password_attr, NULL};

	rc = lak_retrieve(lak, user, service, realm, attrs, &lres);
	if (rc != LAK_OK)
		return rc;

    rc = lak_check_password(lres->value, password, NULL);

	if ( rc == LAK_OK &&
	    (ISSET(lak->conf->group_dn) ||
         ISSET(lak->conf->group_filter)) )
        rc = lak_group_member(lak, user, service, realm, NULL);
	
	lak_result_free(lres);

	return(rc);
}

static int lak_auth_bind(
	LAK *lak,
	const char *user,
	const char *service,
	const char *realm,
	const char *password) 
{
    LAK_USER *lu = NULL;
	LAK_RESULT *dn = NULL;
	int rc;
	const char *attrs[] = {dn_attr, NULL};

	rc = lak_retrieve(lak, user, service, realm, attrs, &dn);
	if (rc != LAK_OK)
		goto done;

	rc = lak_user(	
		dn->value,
		NULL,
		NULL,
		NULL,
		NULL,
		password,
		&lu);
	if (rc != LAK_OK)
		goto done;

	rc = lak_bind(lak, lu);

	if ( rc == LAK_OK &&
	    (ISSET(lak->conf->group_dn) ||
         ISSET(lak->conf->group_filter)) )
	  {
		/* restore config bind */
                lak_unbind(lak);
		lak_user_free(lu);
		rc = lak_user(
		lak->conf->bind_dn,
		lak->conf->id,
		lak->conf->authz_id,
		lak->conf->mech,
		lak->conf->realm,
		lak->conf->password,
		&lu);
		if (rc != LAK_OK)
			goto done;
		rc = lak_bind(lak, lu);
		if (rc != LAK_OK)
			goto done;

		rc = lak_group_member(lak, user, service, realm, dn->value);
	    }
done:
	if (lu)
		lak_user_free(lu);
	if (dn)
		lak_result_free(dn);

	return rc;
}

static int lak_auth_fastbind(
	LAK *lak,
	const char *user,
	const char *service,
	const char *realm,
	const char *password) 
{
	int rc;
	LAK_USER *lu = NULL;
	char *dn = NULL;
	char id[LAK_BUF_LEN];

	*id = '\0';

	if (lak->conf->use_sasl) {
		strlcpy(id, user, LAK_BUF_LEN);
		if (!strchr(id, '@') &&
		    (ISSET(realm))) {
			strlcat(id, "@", LAK_BUF_LEN);
			strlcat(id, realm, LAK_BUF_LEN);
		}
	} else {
		rc = lak_expand_tokens(lak->conf->filter, user, service, realm, NULL, &dn);
		if (rc != LAK_OK || 
            EMPTY(dn))
			goto done;
	}
			
	rc = lak_user(	
		dn,
		id,
		NULL,
		lak->conf->mech,
		lak->conf->realm,
		password,
		&lu);
	if (rc != LAK_OK)
		goto done;

	rc = lak_bind(lak, lu);

	if ( rc == LAK_OK &&
	    (ISSET(lak->conf->group_dn) ||
         ISSET(lak->conf->group_filter)) )
            rc = lak_group_member(lak, user, service, realm, dn);

done:;
	if (lu)
		lak_user_free(lu);
	if (dn != NULL)
		free(dn);

	return rc;
}

int lak_authenticate(
	LAK *lak,
	const char *user,
	const char *service,
	const char *realm,
	const char *password) 
{
	int i;
	int rc;
    int retry = 2;

	if (lak == NULL) {
		syslog(LOG_ERR|LOG_AUTH, "lak_init did not run.");
		return LAK_FAIL;
	}

	if (EMPTY(user))
		return LAK_FAIL;

	if (EMPTY(realm))
		realm = lak->conf->default_realm;

	for (i = 0; authenticator[i].method != -1; i++) {
		if (authenticator[i].method == lak->conf->auth_method) {
			if (authenticator[i].check) {
                for (;retry > 0; retry--) {
                    rc = (authenticator[i].check)(lak, user, service, realm, password);
                    switch(rc) {
                        case LAK_OK:
                            return LAK_OK;
                        case LAK_RETRY:
                            if (retry > 1) {
                                syslog(LOG_INFO|LOG_AUTH, "Retrying authentication");
                                break;
                            }

                            GCC_FALLTHROUGH

                        default:
                            syslog(
                                LOG_DEBUG|LOG_AUTH, 
                                "Authentication failed for %s%s%s: %s (%d)", 
                                user, 
                                (ISSET(realm) ? "/" : ""), 
                                (ISSET(realm) ? realm : ""), 
                                lak_error(rc), 
                                rc);
                            return LAK_FAIL;
                    }
                }
            }
			break;
		}
	}

    /* Should not get here */
    syslog(LOG_DEBUG|LOG_AUTH, "Authentication method not setup properly (%d)", lak->conf->auth_method);

	return LAK_FAIL;
}

char *lak_error(
    const int errno)
{

    switch (errno) {
        case LAK_OK:
            return "Success";
        case LAK_FAIL:
            return "Generic error";
        case LAK_NOMEM:
            return "Out of memory";
        case LAK_RETRY:
            return "Retry condition (ldap server connection reset or broken)";
        case LAK_NOT_GROUP_MEMBER:
            return "Group member check failed";
        case LAK_INVALID_PASSWORD:
            return "Invalid password";
        case LAK_USER_NOT_FOUND:
            return "User not found";
        case LAK_BIND_FAIL:
            return "Bind to ldap server failed (invalid user/password or insufficient access)";
        case LAK_CONNECT_FAIL:
            return "Cannot connect to ldap server (configuration error)";
        default:
            return "Unknow error";
    }
}

#if 0 /* unused */
static char *lak_result_get(
    const LAK_RESULT *lres, 
    const char *attr) 
{
    LAK_RESULT *ptr;


    for (ptr = (LAK_RESULT *)lres; ptr != NULL; ptr = ptr->next)
        if (!strcasecmp(ptr->attribute, attr))
            return ptr->value;

    return NULL;
}
#endif

static int lak_result_add(
	const char *attr,
	const char *val,
	LAK_RESULT **ret)  
{
	LAK_RESULT *lres;
	
	lres = (LAK_RESULT *) malloc(sizeof(LAK_RESULT));
	if (lres == NULL) {
		return LAK_NOMEM;
	}

	lres->next = NULL;

	lres->attribute = strdup(attr);
	if (lres->attribute == NULL) {
		lak_result_free(lres);
		return LAK_NOMEM;
	}

	lres->value = strdup(val);
	if (lres->value == NULL) {
		lak_result_free(lres);
		return LAK_NOMEM;
	}
	lres->len = strlen(lres->value);

	lres->next = *ret;

	*ret = lres;
	return LAK_OK;
}

void lak_result_free(
	LAK_RESULT *res) 
{
	LAK_RESULT *lres, *ptr = res;

	if (ptr == NULL)
		return;

	for (lres = ptr; lres != NULL; lres = ptr) {

		ptr = lres->next;

		if (lres->attribute != NULL) {
			memset(lres->attribute, 0, strlen(lres->attribute));
			free(lres->attribute);	
		}

		if (lres->value != NULL) {
			memset(lres->value, 0, strlen(lres->value));
			free(lres->value);	
		}

		lres->next = NULL;

		free(lres);
	}

	return;
}

static int lak_check_password(
	const char *hash, 
	const char *passwd,
	void *rock __attribute__((unused))) 
{
	int i, hlen;

	if (EMPTY(hash))
		return LAK_INVALID_PASSWORD;

	if (EMPTY(passwd))
		return LAK_INVALID_PASSWORD;

	for (i = 0; password_scheme[i].hash != NULL; i++) {

		hlen = strlen(password_scheme[i].hash);
		if (!strncasecmp(password_scheme[i].hash, hash, hlen)) {
			if (password_scheme[i].check) {
				return (password_scheme[i].check)(hash+hlen, passwd,
								password_scheme[i].rock);
			}
			return LAK_FAIL;
		}
	}

	return strcmp(hash, passwd) ? LAK_INVALID_PASSWORD : LAK_OK;
}

#ifdef HAVE_OPENSSL

static int lak_base64_decode(
	const char *src,
	char **ret,
	int *rlen) 
{

	int rc, i, tlen = 0;
	char *text;
	EVP_ENCODE_CTX *enc_ctx = EVP_ENCODE_CTX_new();

	if (enc_ctx == NULL)
		return LAK_NOMEM;

	text = (char *)malloc(((strlen(src)+3)/4 * 3) + 1);
	if (text == NULL) {
		EVP_ENCODE_CTX_free(enc_ctx);
		return LAK_NOMEM;
	}

	EVP_DecodeInit(enc_ctx);
	rc = EVP_DecodeUpdate(enc_ctx, (unsigned char *) text, &i, (const unsigned char *)src, strlen(src));
	if (rc < 0) {
		EVP_ENCODE_CTX_free(enc_ctx);
		free(text);
		return LAK_FAIL;
	}
	tlen += i;
	EVP_DecodeFinal(enc_ctx, (unsigned char *) text, &i);

	EVP_ENCODE_CTX_free(enc_ctx);

	*ret = text;
	if (rlen != NULL)
		*rlen = tlen;

	return LAK_OK;
}

static int lak_check_hashed(
	const char *hash,
	const char *passwd,
	void *rock)
{
	int rc, clen;
	LAK_HASH_ROCK *hrock = (LAK_HASH_ROCK *) rock;
	EVP_MD_CTX *mdctx;
	const EVP_MD *md;
	unsigned char digest[EVP_MAX_MD_SIZE];
	char *cred;

	md = EVP_get_digestbyname(hrock->mda);
	if (!md)
		return LAK_FAIL;

	mdctx = EVP_MD_CTX_new();
	if (!mdctx)
		return LAK_NOMEM;

	rc = lak_base64_decode(hash, &cred, &clen);
	if (rc != LAK_OK) {
		EVP_MD_CTX_free(mdctx);
		return rc;
	}

	EVP_DigestInit(mdctx, md);
	EVP_DigestUpdate(mdctx, passwd, strlen(passwd));
	if (hrock->salted) {
		EVP_DigestUpdate(mdctx, &cred[EVP_MD_size(md)],
				 clen - EVP_MD_size(md));
	}
	EVP_DigestFinal(mdctx, digest, NULL);
	EVP_MD_CTX_free(mdctx);

	rc = memcmp((char *)cred, (char *)digest, EVP_MD_size(md));
	free(cred);
	return rc ? LAK_INVALID_PASSWORD : LAK_OK;
}

#endif /* HAVE_OPENSSL */

static int lak_check_crypt(
	const char *hash,
	const char *passwd,
	void *rock __attribute__((unused))) 
{
	char *cred;

	if (strlen(hash) < 2 )
		return LAK_INVALID_PASSWORD;

	cred = crypt(passwd, hash);
	if (EMPTY(cred))
		return LAK_INVALID_PASSWORD;

	return strcmp(hash, cred) ? LAK_INVALID_PASSWORD : LAK_OK;
}

#endif /* AUTH_LDAP */
