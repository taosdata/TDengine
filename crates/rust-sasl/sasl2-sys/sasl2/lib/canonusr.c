/* canonusr.c - user canonicalization support
 * Rob Siemborski
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
#include <sasl.h>
#include <string.h>
#include <ctype.h>
#include <prop.h>
#include <stdio.h>

#include "saslint.h"

typedef struct canonuser_plug_list 
{
    struct canonuser_plug_list *next;
    char name[PATH_MAX];
    const sasl_canonuser_plug_t *plug;
} canonuser_plug_list_t;

static canonuser_plug_list_t *canonuser_head = NULL;

/* default behavior:
 *                   eliminate leading & trailing whitespace,
 *                   null-terminate, and get into the outparams
 *                   (handled by INTERNAL plugin) */
/* a zero ulen or alen indicates that it is strlen(value) */
int _sasl_canon_user(sasl_conn_t *conn,
                     const char *user, unsigned ulen,
                     unsigned flags,
                     sasl_out_params_t *oparams)
{
    canonuser_plug_list_t *ptr;
    sasl_server_conn_t *sconn = NULL;
    sasl_client_conn_t *cconn = NULL;
    sasl_canon_user_t *cuser_cb;
    sasl_getopt_t *getopt;
    void *context;
    int result;
    const char *plugin_name = NULL;
    char *user_buf;
    unsigned *lenp;

    if(!conn) return SASL_BADPARAM;    
    if(!user || !oparams) return SASL_BADPARAM;

    if(flags & SASL_CU_AUTHID) {
	user_buf = conn->authid_buf;
	lenp = &(oparams->alen);
    } else if (flags & SASL_CU_AUTHZID) {
	user_buf = conn->user_buf;
	lenp = &(oparams->ulen);
    } else {
	return SASL_BADPARAM;
    }
    
    if (conn->type == SASL_CONN_SERVER)
      sconn = (sasl_server_conn_t *)conn;
    else if (conn->type == SASL_CONN_CLIENT)
      cconn = (sasl_client_conn_t *)conn;
    else return SASL_FAIL;
    
    if(!ulen) ulen = (unsigned int)strlen(user);
    
    /* check to see if we have a callback to make*/
    result = _sasl_getcallback(conn,
			       SASL_CB_CANON_USER,
			       (sasl_callback_ft *)&cuser_cb,
			       &context);
    if(result == SASL_OK && cuser_cb) {
	result = cuser_cb(conn,
			  context,
			  user,
			  ulen,
			  flags,
			  (sconn ?
				sconn->user_realm :
				NULL),
			  user_buf,
			  CANON_BUF_SIZE,
			  lenp);
	

	if (result != SASL_OK) return result;

	/* Point the input copy at the stored buffer */
	user = user_buf;
	ulen = *lenp;
    }

    /* which plugin are we supposed to use? */
    result = _sasl_getcallback(conn,
			       SASL_CB_GETOPT,
			       (sasl_callback_ft *)&getopt,
			       &context);
    if (result == SASL_OK && getopt) {
	getopt(context, NULL, "canon_user_plugin", &plugin_name, NULL);
    }

    if (!plugin_name) {
	/* Use Default */
	plugin_name = "INTERNAL";
    }
    
    for (ptr = canonuser_head; ptr; ptr = ptr->next) {
	/* A match is if we match the internal name of the plugin, or if
	 * we match the filename (old-style) */
	if ((ptr->plug->name && !strcmp(plugin_name, ptr->plug->name))
	   || !strcmp(plugin_name, ptr->name)) break;
    }

    /* We clearly don't have this one! */
    if (!ptr) {
	sasl_seterror(conn, 0, "desired canon_user plugin %s not found",
		      plugin_name);
	return SASL_NOMECH;
    }
    
    if (sconn) {
	/* we're a server */
	result = ptr->plug->canon_user_server(ptr->plug->glob_context,
					      sconn->sparams,
					      user, ulen,
					      flags,
					      user_buf,
					      CANON_BUF_SIZE, lenp);
    } else {
	/* we're a client */
	result = ptr->plug->canon_user_client(ptr->plug->glob_context,
					      cconn->cparams,
					      user, ulen,
					      flags,
					      user_buf,
					      CANON_BUF_SIZE, lenp);
    }

    if (result != SASL_OK) return result;

    if ((flags & SASL_CU_AUTHID) && (flags & SASL_CU_AUTHZID)) {
	/* We did both, so we need to copy the result into
	 * the buffer for the authzid from the buffer for the authid */
	memcpy(conn->user_buf, conn->authid_buf, CANON_BUF_SIZE);
	oparams->ulen = oparams->alen;
    }
	
    /* Set the appropriate oparams (lengths have already been set by lenp) */
    if (flags & SASL_CU_AUTHID) {
	oparams->authid = conn->authid_buf;
    }

    if (flags & SASL_CU_AUTHZID) {
	oparams->user = conn->user_buf;
    }

    RETURN(conn, result);
}

/* Lookup all properties for authentication and/or authorization identity. */
static int _sasl_auxprop_lookup_user_props (sasl_conn_t *conn,
					    unsigned flags,
					    sasl_out_params_t *oparams)
{
    sasl_server_conn_t *sconn = NULL;
    int result = SASL_OK;

    if (!conn) return SASL_BADPARAM;    
    if (!oparams) return SASL_BADPARAM;

#ifndef macintosh
    if (conn->type == SASL_CONN_SERVER) sconn = (sasl_server_conn_t *)conn;

    /* do auxprop lookups (server only) */
    if (sconn) {
	int authz_result;
	unsigned auxprop_lookup_flags = flags & SASL_CU_ASIS_MASK;

	if (flags & SASL_CU_OVERRIDE) {
	    auxprop_lookup_flags |= SASL_AUXPROP_OVERRIDE;
	}

	if (flags & SASL_CU_AUTHID) {
	    result = _sasl_auxprop_lookup(sconn->sparams,
					  auxprop_lookup_flags,
					  oparams->authid,
					  oparams->alen);
	} else {
	    result = SASL_CONTINUE;
	}
	if (flags & SASL_CU_AUTHZID) {
	    authz_result = _sasl_auxprop_lookup(sconn->sparams,
						auxprop_lookup_flags | SASL_AUXPROP_AUTHZID,
						oparams->user,
						oparams->ulen);

	    if (result == SASL_CONTINUE) {
		/* Only SASL_CU_AUTHZID was requested.
		   The authz_result value is authoritative. */
		result = authz_result;
	    } else if (result == SASL_OK && authz_result != SASL_NOUSER) {
		/* Use the authz_result value, unless "result"
		   already contains an error */
		result = authz_result;
	    }
	}

	if ((flags & SASL_CU_EXTERNALLY_VERIFIED) && (result == SASL_NOUSER || result == SASL_NOMECH)) {
	    /* The called has explicitly told us that the authentication identity
	       was already verified or will be verified independently.
	       So a failure to retrieve any associated properties
	       is not an error. For example the caller is using Kerberos to verify user,
	       but the LDAPDB/SASLDB auxprop plugin doesn't contain any auxprops for
	       the user.
	       Another case is PLAIN/LOGIN not using auxprop to verify user passwords. */
	    result = SASL_OK;
	}	
    }
#endif

    RETURN(conn, result);
}

/* default behavior:
 *                   Eliminate leading & trailing whitespace,
 *                   null-terminate, and get into the outparams
 *                   (handled by INTERNAL plugin).
 *
 *                   Server only: Also does auxprop lookups once username
 *                   is canonicalized. */
int _sasl_canon_user_lookup (sasl_conn_t *conn,
			     const char *user,
			     unsigned ulen,
			     unsigned flags,
			     sasl_out_params_t *oparams)
{
    int result;

    result = _sasl_canon_user (conn,
			       user,
			       ulen,
			       flags,
			       oparams);
    if (result == SASL_OK) {
	result = _sasl_auxprop_lookup_user_props (conn,
						  flags,
						  oparams);
    }

    RETURN(conn, result);
}

void _sasl_canonuser_free() 
{
    canonuser_plug_list_t *ptr, *ptr_next;
    
    for(ptr = canonuser_head; ptr; ptr = ptr_next) {
	ptr_next = ptr->next;
	if(ptr->plug->canon_user_free)
	    ptr->plug->canon_user_free(ptr->plug->glob_context,
				       sasl_global_utils);
	sasl_FREE(ptr);
    }

    canonuser_head = NULL;
}

int sasl_canonuser_add_plugin(const char *plugname,
			      sasl_canonuser_init_t *canonuserfunc) 
{
    int result, out_version;
    canonuser_plug_list_t *new_item;
    sasl_canonuser_plug_t *plug;

    if(!plugname || strlen(plugname) > (PATH_MAX - 1)) {
	sasl_seterror(NULL, 0,
		      "bad plugname passed to sasl_canonuser_add_plugin\n");
	return SASL_BADPARAM;
    }
    
    result = canonuserfunc(sasl_global_utils, SASL_CANONUSER_PLUG_VERSION,
			   &out_version, &plug, plugname);

    if(result != SASL_OK) {
	_sasl_log(NULL, SASL_LOG_ERR, "%s_canonuser_plug_init() failed in sasl_canonuser_add_plugin(): %z\n",
		  plugname, result);
	return result;
    }

    if(!plug->canon_user_server && !plug->canon_user_client) {
	/* We need at least one of these implemented */
	_sasl_log(NULL, SASL_LOG_ERR,
		  "canonuser plugin '%s' without either client or server side", plugname);
	return SASL_BADPROT;
    }
    
    new_item = sasl_ALLOC(sizeof(canonuser_plug_list_t));
    if(!new_item) return SASL_NOMEM;

    strncpy(new_item->name, plugname, PATH_MAX - 1);
    new_item->name[strlen(plugname)] = '\0';

    new_item->plug = plug;
    new_item->next = canonuser_head;
    canonuser_head = new_item;

    return SASL_OK;
}

#ifdef MIN
#undef MIN
#endif
#define MIN(a,b) (((a) < (b))? (a):(b))

static int _canonuser_internal(const sasl_utils_t *utils,
			       const char *user, unsigned ulen,
			       unsigned flags __attribute__((unused)),
			       char *out_user,
			       unsigned out_umax, unsigned *out_ulen) 
{
    unsigned i;
    char *in_buf, *userin;
    const char *begin_u;
    unsigned u_apprealm = 0;
    sasl_server_conn_t *sconn = NULL;

    if(!utils || !user) return SASL_BADPARAM;

    in_buf = sasl_ALLOC((ulen + 2) * sizeof(char));
    if(!in_buf) return SASL_NOMEM;

    userin = in_buf;

    memcpy(userin, user, ulen);
    userin[ulen] = '\0';
    
    /* Strip User ID */
    for(i=0;isspace((int)userin[i]) && i<ulen;i++);
    begin_u = &(userin[i]);
    if(i>0) ulen -= i;

    for(;ulen > 0 && isspace((int)begin_u[ulen-1]); ulen--);
    if(begin_u == &(userin[ulen])) {
	sasl_FREE(in_buf);
	utils->seterror(utils->conn, 0, "All-whitespace username.");
	return SASL_FAIL;
    }

    if(utils->conn && utils->conn->type == SASL_CONN_SERVER)
	sconn = (sasl_server_conn_t *)utils->conn;

    /* Need to append realm if necessary (see sasl.h) */
    if(sconn && sconn->user_realm && !strchr(user, '@')) {
	u_apprealm = (unsigned) strlen(sconn->user_realm) + 1;
    }
    
    /* Now Copy */
    memcpy(out_user, begin_u, MIN(ulen, out_umax));
    if(sconn && u_apprealm) {
	if(ulen >= out_umax) return SASL_BUFOVER;
	out_user[ulen] = '@';
	memcpy(&(out_user[ulen+1]), sconn->user_realm,
	       MIN(u_apprealm-1, out_umax-ulen-1));
    }
    out_user[MIN(ulen + u_apprealm,out_umax)] = '\0';

    if(ulen + u_apprealm > out_umax) return SASL_BUFOVER;

    if(out_ulen) *out_ulen = MIN(ulen + u_apprealm,out_umax);
    
    sasl_FREE(in_buf);
    return SASL_OK;
}

static int _cu_internal_server(void *glob_context __attribute__((unused)),
			       sasl_server_params_t *sparams,
			       const char *user, unsigned ulen,
			       unsigned flags,
			       char *out_user,
			       unsigned out_umax, unsigned *out_ulen) 
{
    return _canonuser_internal(sparams->utils,
			       user, ulen,
			       flags, out_user, out_umax, out_ulen);
}

static int _cu_internal_client(void *glob_context __attribute__((unused)),
			       sasl_client_params_t *cparams,
			       const char *user, unsigned ulen,
			       unsigned flags,
			       char *out_user,
			       unsigned out_umax, unsigned *out_ulen) 
{
    return _canonuser_internal(cparams->utils,
			       user, ulen,
			       flags, out_user, out_umax, out_ulen);
}

static sasl_canonuser_plug_t canonuser_internal_plugin = {
        0, /* features */
	0, /* spare */
	NULL, /* glob_context */
	"INTERNAL", /* name */
	NULL, /* canon_user_free */
	_cu_internal_server,
	_cu_internal_client,
	NULL,
	NULL,
	NULL
};

int internal_canonuser_init(const sasl_utils_t *utils __attribute__((unused)),
                            int max_version,
                            int *out_version,
                            sasl_canonuser_plug_t **plug,
                            const char *plugname __attribute__((unused))) 
{
    if(!out_version || !plug) return SASL_BADPARAM;

    if(max_version < SASL_CANONUSER_PLUG_VERSION) return SASL_BADVERS;
    
    *out_version = SASL_CANONUSER_PLUG_VERSION;

    *plug = &canonuser_internal_plugin;

    return SASL_OK;
}
