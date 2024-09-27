/* $OpenLDAP: pkg/ldap/contrib/ldapsasl/ldapdb.c,v 1.1.2.7 2003/11/29 22:10:03 hyc Exp $ */
/* SASL LDAP auxprop+canonuser implementation
 * Copyright (C) 2002-2007 Howard Chu, All rights reserved. <hyc@symas.com>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include <config.h>

#include <stdio.h>
#include <ctype.h>

#include "sasl.h"
#include "saslutil.h"
#include "saslplug.h"

#include "plugin_common.h"

#define LDAP_DEPRECATED 1
#include <ldap.h>

static char ldapdb[] = "ldapdb";

typedef struct ldapctx {
	int inited;		/* Have we already read the config? */
	const char *uri;	/* URI of LDAP server */
	struct berval id;	/* SASL authcid to bind as */
	struct berval pw;	/* password for bind */
	struct berval mech;	/* SASL mech */
	int use_tls;		/* Issue StartTLS request? */
	struct berval canon;	/* Use attr in user entry for canonical name */
} ldapctx;

static ldapctx ldapdb_ctx;

static int ldapdb_interact(LDAP *ld, unsigned flags __attribute__((unused)),
	void *def, void *inter)
{
	sasl_interact_t *in = inter;
	ldapctx *ctx = def;
	struct berval p;

	for (;in->id != SASL_CB_LIST_END;in++)
	{
		p.bv_val = NULL;
		switch(in->id)
		{
			case SASL_CB_GETREALM:
				ldap_get_option(ld, LDAP_OPT_X_SASL_REALM, &p.bv_val);
				if (p.bv_val) p.bv_len = strlen(p.bv_val);
				break;		
			case SASL_CB_AUTHNAME:
				p = ctx->id;
				break;
			case SASL_CB_PASS:
				p = ctx->pw;
				break;
		}
		if (p.bv_val)
		{
			in->result = p.bv_val;
			in->len = p.bv_len;
		}
	}
	return LDAP_SUCCESS;
}

typedef struct connparm {
	LDAP *ld;
	LDAPControl c;
	LDAPControl *ctrl[2];
	struct berval *dn;
} connparm;

static int ldapdb_connect(ldapctx *ctx, sasl_server_params_t *sparams,
	const char *user, unsigned ulen, connparm *cp)
{
    int i, rc;
    char *authzid;

    if((i=ldap_initialize(&cp->ld, ctx->uri))) {
	cp->ld = NULL;
	return i;
    }

    authzid = sparams->utils->malloc(ulen + sizeof("u:"));
    if (!authzid) {
    	return LDAP_NO_MEMORY;
    } 
    strcpy(authzid, "u:");
    strcpy(authzid+2, user);
    cp->c.ldctl_oid = LDAP_CONTROL_PROXY_AUTHZ;
    cp->c.ldctl_value.bv_val = authzid;
    cp->c.ldctl_value.bv_len = ulen + 2;
    cp->c.ldctl_iscritical = 1;

    i = LDAP_VERSION3;
    rc = ldap_set_option(cp->ld, LDAP_OPT_PROTOCOL_VERSION, &i);
    if (rc != 0) {
        sparams->utils->free(authzid);
        return rc;
    }

    /* If TLS is set and it fails, continue or bail out as requested */
    if (ctx->use_tls && (i=ldap_start_tls_s(cp->ld, NULL, NULL)) != LDAP_SUCCESS
    	&& ctx->use_tls > 1) {
    	sparams->utils->free(authzid);
	return i;
    }

    i = ldap_sasl_interactive_bind_s(cp->ld, NULL, ctx->mech.bv_val, NULL,
    	NULL, LDAP_SASL_QUIET, ldapdb_interact, ctx);
    if (i != LDAP_SUCCESS) {
    	sparams->utils->free(authzid);
	return i;
    }
    
    cp->ctrl[0] = &cp->c;
    cp->ctrl[1] = NULL;
    i = ldap_whoami_s(cp->ld, &cp->dn, cp->ctrl, NULL);
    if (i == LDAP_SUCCESS && cp->dn) {
    	if (!cp->dn->bv_val || strncmp(cp->dn->bv_val, "dn:", 3)) {
	    ber_bvfree(cp->dn);
	    cp->dn = NULL;
	    i = LDAP_INVALID_SYNTAX;
	} else {
    	    cp->c.ldctl_value = *(cp->dn);
	}
    }
    sparams->utils->free(authzid);
    return i;
}

static int ldapdb_auxprop_lookup(void *glob_context,
				  sasl_server_params_t *sparams,
				  unsigned flags,
				  const char *user,
				  unsigned ulen)
{
    ldapctx *ctx = glob_context;
    connparm cp;
    int ret, i, n, *aindx;
    int result;
    int j;
    const struct propval *pr;
    struct berval **bvals;
    LDAPMessage *msg, *res;
    char **attrs = NULL;
    
    if(!ctx || !sparams || !user) return SASL_BADPARAM;

    pr = sparams->utils->prop_get(sparams->propctx);
    if (!pr) return SASL_FAIL;

    /* count how many attrs to fetch */
    for(i = 0, n = 0; pr[i].name; i++) {
	if(pr[i].name[0] == '*' && (flags & SASL_AUXPROP_AUTHZID))
	    continue;
	if(pr[i].values && !(flags & SASL_AUXPROP_OVERRIDE))
	    continue;
	n++;
    }

    /* nothing to do, bail out */
    if (!n) return SASL_OK;

    /* alloc an array of attr names for search, and index to the props */
    attrs = sparams->utils->malloc((n+1)*sizeof(char *)*2);
    if (!attrs) {
	return SASL_NOMEM;
    }

    aindx = (int *)(attrs + n + 1);

    /* copy attr list */
    for (i=0, n=0; pr[i].name; i++) {
	if(pr[i].name[0] == '*' && (flags & SASL_AUXPROP_AUTHZID))
	    continue;
	if(pr[i].values && !(flags & SASL_AUXPROP_OVERRIDE))
	    continue;
    	attrs[n] = (char *)pr[i].name;
	if (pr[i].name[0] == '*') attrs[n]++;
	aindx[n] = i;
	n++;
    }
    attrs[n] = NULL;

    if ((ret = ldapdb_connect(ctx, sparams, user, ulen, &cp)) != LDAP_SUCCESS) {
	goto process_ldap_error;
    }

    ret = ldap_search_ext_s(cp.ld, cp.dn->bv_val+3, LDAP_SCOPE_BASE,
    	"(objectclass=*)", attrs, 0, cp.ctrl, NULL, NULL, 1, &res);
    ber_bvfree(cp.dn);

    if (ret != LDAP_SUCCESS) {
	goto process_ldap_error;
    }

    /* Assume no user by default */
    ret = LDAP_NO_SUCH_OBJECT;

    for (msg = ldap_first_message(cp.ld, res);
         msg;
         msg = ldap_next_message(cp.ld, msg)) {
    	if (ldap_msgtype(msg) != LDAP_RES_SEARCH_ENTRY) continue;

	/* Presence of a search result response indicates that the user exists */
	ret = LDAP_SUCCESS;

        for (i = 0; i < n; i++) {
	    bvals = ldap_get_values_len(cp.ld, msg, attrs[i]);
	    if (!bvals) continue;

	    if (pr[aindx[i]].values) {
	    	sparams->utils->prop_erase(sparams->propctx, pr[aindx[i]].name);
	    }

            for ( j = 0; bvals[j] != NULL; j++ ) {
	        sparams->utils->prop_set(sparams->propctx,
                                         pr[aindx[i]].name,
				         bvals[j]->bv_val,
                                         bvals[j]->bv_len);
            }
	    ber_bvecfree(bvals);
	}
    }
    ldap_msgfree(res);

 process_ldap_error:
    switch (ret) {
	case LDAP_SUCCESS:
            result = SASL_OK;
            break;

	case LDAP_NO_SUCH_OBJECT:
            result = SASL_NOUSER;
            break;

        case LDAP_NO_MEMORY:
            result = SASL_NOMEM;
            break;

	case LDAP_SERVER_DOWN:
	case LDAP_BUSY:
	case LDAP_UNAVAILABLE:
	case LDAP_CONNECT_ERROR:
	    result = SASL_UNAVAIL;
	    break;

#if defined(LDAP_PROXY_AUTHZ_FAILURE)
	case LDAP_PROXY_AUTHZ_FAILURE:
#elif defined(LDAP_X_PROXY_AUTHZ_FAILURE)
	case LDAP_X_PROXY_AUTHZ_FAILURE:
#endif
	case LDAP_INAPPROPRIATE_AUTH:
	case LDAP_INVALID_CREDENTIALS:
	case LDAP_INSUFFICIENT_ACCESS:
	    result = SASL_BADAUTH;
	    break;

        default:
            result = SASL_FAIL;
            break;
    }

    if(attrs) sparams->utils->free(attrs);
    if(cp.ld) ldap_unbind_ext(cp.ld, NULL, NULL);

    return result;
}

static int ldapdb_auxprop_store(void *glob_context,
				  sasl_server_params_t *sparams,
				  struct propctx *prctx,
				  const char *user,
				  unsigned ulen)
{
    ldapctx *ctx = glob_context;
    connparm cp;
    const struct propval *pr;
    int i, n;
    LDAPMod **mods;

    /* just checking if we are enabled */
    if (!prctx) return SASL_OK;

    if (!sparams || !user) return SASL_BADPARAM;

    pr = sparams->utils->prop_get(prctx);
    if (!pr) return SASL_BADPARAM;

    for (n=0; pr[n].name; n++);
    if (!n) return SASL_BADPARAM;

    mods = sparams->utils->malloc((n+1) * sizeof(LDAPMod*) + n * sizeof(LDAPMod));
    if (!mods) return SASL_NOMEM;

    if((i=ldapdb_connect(ctx, sparams, user, ulen, &cp)) == 0) {

	for (i=0; i<n; i++) {
	    mods[i] = (LDAPMod *)((char *)(mods+n+1) + i * sizeof(LDAPMod));
	    mods[i]->mod_op = LDAP_MOD_REPLACE;
	    mods[i]->mod_type = (char *)pr[i].name;
	    mods[i]->mod_values = (char **)pr[i].values;
	}
	mods[i] = NULL;

	i = ldap_modify_ext_s(cp.ld, cp.dn->bv_val+3, mods, cp.ctrl, NULL);
	ber_bvfree(cp.dn);
    }

    sparams->utils->free(mods);

    if (i) {
    	sparams->utils->seterror(sparams->utils->conn, 0, "%s",
	    ldap_err2string(i));
	if (i == LDAP_NO_MEMORY) i = SASL_NOMEM;
	else i = SASL_FAIL;
    }
    if(cp.ld) ldap_unbind_ext(cp.ld, NULL, NULL);
    return i;
}

static int
ldapdb_canon_server(void *glob_context,
		    sasl_server_params_t *sparams,
		    const char *user,
		    unsigned ulen,
		    unsigned flags __attribute__((unused)),
		    char *out,
		    unsigned out_max,
		    unsigned *out_ulen)
{
    ldapctx *ctx = glob_context;
    connparm cp;
    struct berval **bvals;
    LDAPMessage *msg, *res;
    char *rdn, *attrs[2];
    unsigned len;
    int ret;
    
    if(!ctx || !sparams || !user) return SASL_BADPARAM;

    /* If no canon attribute was configured, we can't do anything */
    if(!ctx->canon.bv_val) return SASL_BADPARAM;

    /* Trim whitespace */
    while(isspace(*(unsigned char *)user)) {
	user++;
	ulen--;
    }
    while(isspace((unsigned char)user[ulen-1])) {
    	ulen--;
    }
    
    if (!ulen) {
    	sparams->utils->seterror(sparams->utils->conn, 0,
	    "All-whitespace username.");
	return SASL_FAIL;
    }

    ret = ldapdb_connect(ctx, sparams, user, ulen, &cp);
    if ( ret ) goto done;

    /* See if the RDN uses the canon attr. If so, just use the RDN
     * value, we don't need to do a search.
     */
    rdn = cp.dn->bv_val+3;
    if (!strncasecmp(ctx->canon.bv_val, rdn, ctx->canon.bv_len) &&
    	rdn[ctx->canon.bv_len] == '=') {
	char *comma;
	rdn += ctx->canon.bv_len + 1;
	comma = strchr(rdn, ',');
	if ( comma )
	    len = comma - rdn;
	else 
	    len = cp.dn->bv_len - (rdn - cp.dn->bv_val);
	if ( len > out_max )
	    len = out_max;
	memcpy(out, rdn, len);
	out[len] = '\0';
	*out_ulen = len;
	ret = SASL_OK;
	ber_bvfree(cp.dn);
	goto done;
    }

    /* Have to read the user's entry */
    attrs[0] = ctx->canon.bv_val;
    attrs[1] = NULL;
    ret = ldap_search_ext_s(cp.ld, cp.dn->bv_val+3, LDAP_SCOPE_BASE,
    	"(objectclass=*)", attrs, 0, cp.ctrl, NULL, NULL, 1, &res);
    ber_bvfree(cp.dn);

    if (ret != LDAP_SUCCESS) goto done;

    for(msg=ldap_first_message(cp.ld, res); msg; msg=ldap_next_message(cp.ld, msg))
    {
    	if (ldap_msgtype(msg) != LDAP_RES_SEARCH_ENTRY) continue;
	bvals = ldap_get_values_len(cp.ld, msg, attrs[0]);
	if (!bvals) continue;
	len = bvals[0]->bv_len;
	if ( len > out_max )
	    len = out_max;
	memcpy(out, bvals[0]->bv_val, len);
	out[len] = '\0';
	*out_ulen = len;
	ber_bvecfree(bvals);
    }
    ldap_msgfree(res);

 done:
    if(cp.ld) ldap_unbind_ext(cp.ld, NULL, NULL);
    if (ret) {
    	sparams->utils->seterror(sparams->utils->conn, 0, "%s",
	    ldap_err2string(ret));
	if (ret == LDAP_NO_MEMORY) ret = SASL_NOMEM;
	else ret = SASL_FAIL;
    }
    return ret;
}

static int
ldapdb_canon_client(void *glob_context __attribute__((unused)),
		    sasl_client_params_t *cparams,
		    const char *user,
		    unsigned ulen,
		    unsigned flags __attribute__((unused)),
		    char *out,
		    unsigned out_max,
		    unsigned *out_ulen)
{
    if(!cparams || !user) return SASL_BADPARAM;

    /* Trim whitespace */
    while(isspace(*(unsigned char *)user)) {
	user++;
	ulen--;
    }
    while(isspace((unsigned char)user[ulen-1])) {
    	ulen--;
    }
    
    if (!ulen) {
    	cparams->utils->seterror(cparams->utils->conn, 0,
	    "All-whitespace username.");
	return SASL_FAIL;
    }

    if (ulen > out_max) return SASL_BUFOVER;

    memcpy(out, user, ulen);
    out[ulen] = '\0';
    *out_ulen = ulen;
    return SASL_OK;
}

static int
ldapdb_config(const sasl_utils_t *utils)
{
    ldapctx *p = &ldapdb_ctx;
    const char *s;
    unsigned len;

    if(p->inited) return SASL_OK;

    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_uri", &p->uri, NULL);
    if(!p->uri) return SASL_BADPARAM;

    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_id",
    	(const char **)&p->id.bv_val, &len);
    p->id.bv_len = len;
    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_pw",
    	(const char **)&p->pw.bv_val, &len);
    p->pw.bv_len = len;
    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_mech",
    	(const char **)&p->mech.bv_val, &len);
    p->mech.bv_len = len;
    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_starttls", &s, NULL);
    if (s)
    {
    	if (!strcasecmp(s, "demand")) p->use_tls = 2;
	else if (!strcasecmp(s, "try")) p->use_tls = 1;
    }
    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_rc", &s, &len);
    if (s)
    {
    	char *str = utils->malloc(sizeof("LDAPRC=")+len);
	if (!str) return SASL_NOMEM;
	strcpy( str, "LDAPRC=" );
	strcpy( str + sizeof("LDAPRC=")-1, s );
	if (putenv(str))
	{
	    utils->free(str);
	    return SASL_NOMEM;
	}
    }
    utils->getopt(utils->getopt_context, ldapdb, "ldapdb_canon_attr",
	(const char **)&p->canon.bv_val, &len);
    p->canon.bv_len = len;
    p->inited = 1;

    return SASL_OK;
}

static sasl_auxprop_plug_t ldapdb_auxprop_plugin = {
    0,				/* Features */
    0,				/* spare */
    &ldapdb_ctx,		/* glob_context */
    NULL,	/* auxprop_free */
    ldapdb_auxprop_lookup,	/* auxprop_lookup */
    ldapdb,			/* name */
    ldapdb_auxprop_store	/* auxprop store */
};

int ldapdb_auxprop_plug_init(const sasl_utils_t *utils,
                             int max_version,
                             int *out_version,
                             sasl_auxprop_plug_t **plug,
                             const char *plugname __attribute__((unused))) 
{
    int rc;

    if(!out_version || !plug) return SASL_BADPARAM;

    if(max_version < SASL_AUXPROP_PLUG_VERSION) return SASL_BADVERS;
    
    rc = ldapdb_config(utils);

    *out_version = SASL_AUXPROP_PLUG_VERSION;

    *plug = &ldapdb_auxprop_plugin;

    return rc;
}

static sasl_canonuser_plug_t ldapdb_canonuser_plugin = {
	0,	/* features */
	0,	/* spare */
	&ldapdb_ctx,	/* glob_context */
	ldapdb,	/* name */
	NULL,	/* canon_user_free */
	ldapdb_canon_server,	/* canon_user_server */
	ldapdb_canon_client,	/* canon_user_client */
	NULL,
	NULL,
	NULL
};

int ldapdb_canonuser_plug_init(const sasl_utils_t *utils,
                             int max_version,
                             int *out_version,
                             sasl_canonuser_plug_t **plug,
                             const char *plugname __attribute__((unused))) 
{
    int rc;

    if(!out_version || !plug) return SASL_BADPARAM;

    if(max_version < SASL_CANONUSER_PLUG_VERSION) return SASL_BADVERS;
    
    rc = ldapdb_config(utils);

    *out_version = SASL_CANONUSER_PLUG_VERSION;

    *plug = &ldapdb_canonuser_plugin;

    return rc;
}
