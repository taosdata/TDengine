/* SASL server API implementation
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

/* local functions/structs don't start with sasl
 */
#include <config.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#ifndef macintosh
#include <sys/types.h>
#include <sys/stat.h>
#endif
#include <fcntl.h>
#include <string.h>
#include <ctype.h>

#include "sasl.h"
#include "saslint.h"
#include "saslplug.h"
#include "saslutil.h"

#define DEFAULT_CHECKPASS_MECH "auxprop"

/* Contains functions:
 * 
 * sasl_server_init
 * sasl_server_new
 * sasl_listmech
 * sasl_server_start
 * sasl_server_step
 * sasl_checkpass
 * sasl_checkapop
 * sasl_user_exists
 * sasl_setpass
 */

/* if we've initialized the server sucessfully */
static int _sasl_server_active = 0;

/* For access by other modules */
int _is_sasl_server_active(void) { return _sasl_server_active; }

static int _sasl_checkpass(sasl_conn_t *conn, 
			   const char *user, unsigned userlen,
			   const char *pass, unsigned passlen);

static mech_list_t *mechlist = NULL; /* global var which holds the list */

static sasl_global_callbacks_t global_callbacks;

/* set the password for a user
 *  conn        -- SASL connection
 *  user        -- user name
 *  pass        -- plaintext password, may be NULL to remove user
 *  passlen     -- length of password, 0 = strlen(pass)
 *  oldpass     -- NULL will sometimes work
 *  oldpasslen  -- length of password, 0 = strlen(oldpass)
 *  flags       -- see flags below
 * 
 * returns:
 *  SASL_NOCHANGE  -- proper entry already exists
 *  SASL_NOMECH    -- no authdb supports password setting as configured
 *  SASL_NOVERIFY  -- user exists, but no settable password present
 *  SASL_DISABLED  -- account disabled
 *  SASL_PWLOCK    -- password locked
 *  SASL_WEAKPASS  -- password too weak for security policy
 *  SASL_NOUSERPASS -- user-supplied passwords not permitted
 *  SASL_FAIL      -- OS error
 *  SASL_BADPARAM  -- password too long
 *  SASL_OK        -- successful
 */

int sasl_setpass(sasl_conn_t *conn,
		 const char *user,
		 const char *pass,
		 unsigned passlen,
		 const char *oldpass,
		 unsigned oldpasslen,
		 unsigned flags)
{
    int result = SASL_OK, tmpresult;
    sasl_server_conn_t *s_conn = (sasl_server_conn_t *) conn;
    const char *password_request[] = { SASL_AUX_PASSWORD_PROP, NULL };
    const char *user_delete_request[] = { SASL_AUX_PASSWORD_PROP, SASL_AUX_ALL, NULL };
    sasl_server_userdb_setpass_t *setpass_cb = NULL;
    void *context = NULL;
    int tried_setpass = 0;
    int failed = 0;
    mechanism_t *sm;
    server_sasl_mechanism_t *m;
    char *current_mech;
     
    if (!_sasl_server_active || !mechlist) return SASL_NOTINIT;

    /* check params */
    if (!conn) return SASL_BADPARAM;
    if (conn->type != SASL_CONN_SERVER) PARAMERROR(conn);
     
    if ((!(flags & SASL_SET_DISABLE) && passlen == 0)
        || ((flags & SASL_SET_CREATE) && (flags & SASL_SET_DISABLE)))
	PARAMERROR(conn);

    /* Check that we have an active SASL mechanism */
    if (sasl_getprop (conn,
		      SASL_MECHNAME,
		      (const void **) &current_mech) != SASL_OK) {
	current_mech = NULL;
    }

    if ( (flags & SASL_SET_CURMECH_ONLY) &&
	 (current_mech == NULL) ) {
	sasl_seterror( conn, SASL_NOLOG,
                  "No current SASL mechanism available");
	RETURN(conn, SASL_BADPARAM);
    }

    /* Do we want to store SASL_AUX_PASSWORD_PROP (plain text)?  and
     * Do we have an auxprop backend that can store properties?
     */
    if ((flags & SASL_SET_DISABLE || !(flags & SASL_SET_NOPLAIN)) &&
	sasl_auxprop_store(NULL, NULL, NULL) == SASL_OK) {

	tried_setpass++;

	if (flags & SASL_SET_DISABLE) {
	    pass = NULL;
	    passlen = 0;
	    result = prop_request(s_conn->sparams->propctx, user_delete_request);
	} else {
	    result = prop_request(s_conn->sparams->propctx, password_request);
	}
	if (result == SASL_OK) {
	    /* NOTE: When deleting users, this will work in a backward compatible way */
	    result = prop_set(s_conn->sparams->propctx, SASL_AUX_PASSWORD_PROP,
			      pass, passlen);
	}
	if (result == SASL_OK && flags & SASL_SET_DISABLE) {
	    result = prop_set(s_conn->sparams->propctx, SASL_AUX_ALL,
			      NULL, 0);
	}
	if (result == SASL_OK) {
	    result = sasl_auxprop_store(conn, s_conn->sparams->propctx, user);
	}
	if (result != SASL_OK) {
	    _sasl_log(conn, SASL_LOG_ERR,
		      "setpass failed for %s: %z",
		      user, result);
	    failed++;
	} else {
	    _sasl_log(conn, SASL_LOG_NOTE,
		      "setpass succeeded for %s", user);
	}
    }

    /* We want to preserve the current value of result, so we use tmpresult below */

    /* call userdb callback function */
    tmpresult = _sasl_getcallback(conn, SASL_CB_SERVER_USERDB_SETPASS,
			       (sasl_callback_ft *)&setpass_cb, &context);
    if (tmpresult == SASL_OK && setpass_cb) {

	tried_setpass++;

	tmpresult = setpass_cb(conn, context, user, pass, passlen,
			    s_conn->sparams->propctx, flags);
	if(tmpresult != SASL_OK) {
	    if (tmpresult == SASL_CONSTRAINT_VIOLAT) {
		if (result == SASL_OK) {
		    result = tmpresult;
		}
	    } else {
		result = tmpresult;
	    }
	    _sasl_log(conn, SASL_LOG_ERR,
		      "setpass callback failed for %s: %z",
		      user, tmpresult);
	    failed++;
	} else {
	    _sasl_log(conn, SASL_LOG_NOTE,
		      "setpass callback succeeded for %s", user);
	}
    }

    /* now we let the mechanisms set their secrets */
    for (sm = s_conn->mech_list; sm; sm = sm->next) {
	m = &sm->m;

	if (!m->plug->setpass) {
	    /* can't set pass for this mech */
	    continue;
	}

	/* Invoke only one setpass for the currently selected mechanism,
	   if SASL_SET_CURMECH_ONLY is specified */
	if ((flags & SASL_SET_CURMECH_ONLY) &&
	    (strcmp(current_mech, m->plug->mech_name) != 0)) {
	    continue;
	}

	tried_setpass++;

	tmpresult = m->plug->setpass(m->plug->glob_context,
				     ((sasl_server_conn_t *)conn)->sparams,
				     user,
				     pass,
				     passlen,
				     oldpass, oldpasslen,
				     flags);
	if (tmpresult == SASL_OK) {
	    _sasl_log(conn, SASL_LOG_NOTE,
		      "%s: set secret for %s", m->plug->mech_name, user);

	    m->condition = SASL_OK; /* if we previously thought the
				       mechanism didn't have any user secrets 
				       we now think it does */

	} else if (tmpresult == SASL_NOCHANGE) {
	    _sasl_log(conn, SASL_LOG_NOTE,
		      "%s: secret not changed for %s", m->plug->mech_name, user);
	} else if (tmpresult == SASL_CONSTRAINT_VIOLAT) {
	    _sasl_log(conn, SASL_LOG_ERR,
		      "%s: failed to set secret for %s: constrain violation",
		      m->plug->mech_name, user);
	    if (result == SASL_OK) {
		result = tmpresult;
	    }
	    failed++;
	} else {
	    result = tmpresult;
	    _sasl_log(conn, SASL_LOG_ERR,
		      "%s: failed to set secret for %s: %z (%m)",
		      m->plug->mech_name, user, tmpresult,
#ifndef WIN32
		      errno
#else
		      GetLastError()
#endif
		      );
	    failed++;
	}
    }

    if (!tried_setpass) {
	_sasl_log(conn, SASL_LOG_WARN,
		  "secret not changed for %s: "
		  "no writable auxprop plugin or setpass callback found",
		  user);
    } else if (result == SASL_CONSTRAINT_VIOLAT) {
	/* If not all setpass failed with SASL_CONSTRAINT_VIOLAT - 
	   ignore SASL_CONSTRAINT_VIOLAT */
	if (failed < tried_setpass) {
	    result = SASL_OK;
	}
    }

    RETURN(conn, result);
}

/* local mechanism which disposes of server */
static void server_dispose(sasl_conn_t *pconn)
{
    sasl_server_conn_t *s_conn=  (sasl_server_conn_t *) pconn;
    context_list_t *cur, *cur_next;

    /* Just sanity check that sasl_server_done wasn't called yet */
    if (_sasl_server_active != 0) {
	if (s_conn->mech) {
	    void (*mech_dispose)(void *conn_context, const sasl_utils_t *utils);

	    mech_dispose = s_conn->mech->m.plug->mech_dispose;

	    if (mech_dispose) {
		mech_dispose(pconn->context, s_conn->sparams->utils);
	    }
	}
	pconn->context = NULL;

	for(cur = s_conn->mech_contexts; cur; cur=cur_next) {
	    cur_next = cur->next;
	    if (cur->context) {
		cur->mech->m.plug->mech_dispose(cur->context, s_conn->sparams->utils);
	    }
	    sasl_FREE(cur);
	}  
	s_conn->mech_contexts = NULL;
    }
  
    _sasl_free_utils(&s_conn->sparams->utils);

    if (s_conn->sparams->propctx) {
	prop_dispose(&s_conn->sparams->propctx);
    }

    if (s_conn->appname) {
	sasl_FREE(s_conn->appname);
    }

    if (s_conn->user_realm) {
	sasl_FREE(s_conn->user_realm);
    }

    if (s_conn->sparams) {
	sasl_FREE(s_conn->sparams);
    }

    if (s_conn->mech_list != mechlist->mech_list) {
	/* free connection-specific mech_list */
	mechanism_t *m, *prevm;

	m = s_conn->mech_list; /* m point to beginning of the list */

	while (m) {
	     prevm = m;
	     m = m->next;
	     sasl_FREE(prevm);
	}
    }

    _sasl_conn_dispose(pconn);
}

static int init_mechlist(void)
{
    sasl_utils_t *newutils = NULL;

    /* set util functions - need to do rest */
    newutils = _sasl_alloc_utils(NULL, &global_callbacks);
    if (newutils == NULL)
	return SASL_NOMEM;

    newutils->checkpass = &_sasl_checkpass;

    mechlist->utils = newutils;
    mechlist->mech_list = NULL;
    mechlist->mech_length = 0;

    return SASL_OK;
}

static int mech_compare(const sasl_server_plug_t *a,
			const sasl_server_plug_t *b)
{
    unsigned sec_diff;
    unsigned features_diff;

    /* XXX  the following is fairly arbitrary, but its independent
       of the order in which the plugins are loaded
    */
    sec_diff = a->security_flags ^ b->security_flags;
    if (sec_diff & a->security_flags & SASL_SEC_NOANONYMOUS) return 1;
    if (sec_diff & b->security_flags & SASL_SEC_NOANONYMOUS) return -1;
    if (sec_diff & a->security_flags & SASL_SEC_NOPLAINTEXT) return 1;
    if (sec_diff & b->security_flags & SASL_SEC_NOPLAINTEXT) return -1;
    if (sec_diff & a->security_flags & SASL_SEC_MUTUAL_AUTH) return 1;
    if (sec_diff & b->security_flags & SASL_SEC_MUTUAL_AUTH) return -1;
    if (sec_diff & a->security_flags & SASL_SEC_NOACTIVE) return 1;
    if (sec_diff & b->security_flags & SASL_SEC_NOACTIVE) return -1;
    if (sec_diff & a->security_flags & SASL_SEC_NODICTIONARY) return 1;
    if (sec_diff & b->security_flags & SASL_SEC_NODICTIONARY) return -1;
    if (sec_diff & a->security_flags & SASL_SEC_FORWARD_SECRECY) return 1;
    if (sec_diff & b->security_flags & SASL_SEC_FORWARD_SECRECY) return -1;

    features_diff = a->features ^ b->features;
    if (features_diff & a->features & SASL_FEAT_CHANNEL_BINDING) return 1;
    if (features_diff & b->features & SASL_FEAT_CHANNEL_BINDING) return -1;

    if (a->max_ssf > b->max_ssf) return 1;
    if (a->max_ssf < b->max_ssf) return -1;

    if (SASL_GET_HASH_STRENGTH(a->security_flags) > SASL_GET_HASH_STRENGTH(b->security_flags)) return 1;
    if (SASL_GET_HASH_STRENGTH(a->security_flags) < SASL_GET_HASH_STRENGTH(b->security_flags)) return -1;

    return 0;
}

/*
 * parameters:
 *  p - entry point
 */
int sasl_server_add_plugin(const char *plugname,
			   sasl_server_plug_init_t *p)
{
    int plugcount;
    sasl_server_plug_t *pluglist = NULL;
    sasl_server_plug_init_t *entry_point = NULL;
    int result;
    int version;
    int lupe;

    if(!plugname || !p) return SASL_BADPARAM;

    entry_point = (sasl_server_plug_init_t *)p;

    /* call into the shared library asking for information about it */
    /* version is filled in with the version of the plugin */
    result = entry_point(mechlist->utils, SASL_SERVER_PLUG_VERSION, &version,
			 &pluglist, &plugcount);

    if ((result != SASL_OK) && (result != SASL_NOUSER)
        && (result != SASL_CONTINUE)) {
	_sasl_log(NULL, SASL_LOG_DEBUG,
		  "%s_client_plug_init() failed in sasl_server_add_plugin(): %z\n",
		  plugname, result);
	return result;
    }

    /* Make sure plugin is using the same SASL version as us */
    if (version != SASL_SERVER_PLUG_VERSION)
    {
	_sasl_log(NULL,
		  SASL_LOG_ERR,
		  "version mismatch on  sasl_server_add_plugin for '%s': %d expected, but %d reported",
		  plugname,
		  SASL_SERVER_PLUG_VERSION,
		  version);
	return SASL_BADVERS;
    }

    for (lupe=0;lupe < plugcount ;lupe++, pluglist++)
    {
	mechanism_t *mech, *mp;

	mech = sasl_ALLOC(sizeof(mechanism_t));
	if (! mech) return SASL_NOMEM;
        memset (mech, 0, sizeof(mechanism_t));

	mech->m.plug = pluglist;
	if(_sasl_strdup(plugname, &mech->m.plugname, NULL) != SASL_OK) {
	    sasl_FREE(mech);
	    return SASL_NOMEM;
	}
	mech->m.version = version;

	/* whether this mech actually has any users in it's db */
	mech->m.condition = result; /* SASL_OK, SASL_CONTINUE or SASL_NOUSER */

        /* mech->m.f = NULL; */

	/* sort mech_list by relative "strength" */
	mp = mechlist->mech_list;
	if (!mp || mech_compare(pluglist, mp->m.plug) >= 0) {
	    /* add mech to head of list */
	    mech->next = mechlist->mech_list;
	    mechlist->mech_list = mech;
	} else {
	    /* find where to insert mech into list */
	    while (mp->next &&
		   mech_compare(pluglist, mp->next->m.plug) <= 0) mp = mp->next;
	    mech->next = mp->next;
	    mp->next = mech;
	}
	mechlist->mech_length++;
    }

    return SASL_OK;
}

int sasl_server_done(void)
{
    int result = SASL_CONTINUE;

    if (_sasl_server_cleanup_hook == NULL && _sasl_client_cleanup_hook == NULL) {
	return SASL_NOTINIT;
    }

    if (_sasl_server_cleanup_hook) {
	result = _sasl_server_cleanup_hook();
	
	if (result == SASL_OK) {
	    _sasl_server_idle_hook = NULL;
	    _sasl_server_cleanup_hook = NULL;
	} else {
	    return result;
	}
    }
    
    if (_sasl_server_cleanup_hook || _sasl_client_cleanup_hook) {
	return result;
    }
    
    sasl_common_done();

    return SASL_OK;
}

static int server_done(void) {
  mechanism_t *m;
  mechanism_t *prevm;

  if(_sasl_server_active == 0)
      return SASL_NOTINIT;
  else
      _sasl_server_active--;
  
  if(_sasl_server_active) {
      /* Don't de-init yet! Our refcount is nonzero. */
      return SASL_CONTINUE;
  }

  if (mechlist != NULL)
  {
      m=mechlist->mech_list; /* m point to beginning of the list */

      while (m!=NULL)
      {
	  prevm=m;
	  m=m->next;
    
	  if (prevm->m.plug->mech_free) {
	      prevm->m.plug->mech_free(prevm->m.plug->glob_context,
				     mechlist->utils);
	  }

	  sasl_FREE(prevm->m.plugname);
	  sasl_FREE(prevm);    
      }
      _sasl_free_utils(&mechlist->utils);
      sasl_FREE(mechlist);
      mechlist = NULL;
  }

  /* Free the auxprop plugins */
  _sasl_auxprop_free();

  global_callbacks.callbacks = NULL;
  global_callbacks.appname = NULL;

  sasl_config_done();

  return SASL_OK;
}

static int server_idle(sasl_conn_t *conn)
{
    sasl_server_conn_t *s_conn = NULL;
    mechanism_t *m;

    if (! mechlist) {
	return 0;
    }

    if (!conn)
        return 1;
    s_conn = (sasl_server_conn_t *) conn;

    for (m = s_conn->mech_list;
	 m != NULL;
	 m = m->next) {
	if (m->m.plug->idle
	    &&  m->m.plug->idle(m->m.plug->glob_context,
				conn,
				s_conn->sparams)) {
	    return 1;
	}
    }

    return 0;
}

static int load_config(const sasl_callback_t *verifyfile_cb)
{
    int result;
    const char *path_to_config = NULL;
    size_t path_len;
    char *config_filename = NULL;
    size_t len;
    const sasl_callback_t *getconfpath_cb = NULL;
    const char * next;

    /* If appname was not provided, behave as if there is no config file 
        (see also sasl_config_init() */
    if (global_callbacks.appname == NULL) {
        return SASL_CONTINUE;
    }

    /* get the path to the config file */
    getconfpath_cb = _sasl_find_getconfpath_callback( global_callbacks.callbacks );
    if (getconfpath_cb == NULL) return SASL_BADPARAM;

    /* getconfpath_cb->proc MUST be a sasl_getconfpath_t; if only C had a type
       system */
    result = ((sasl_getconfpath_t *)(getconfpath_cb->proc))(getconfpath_cb->context,
							    (char **) &path_to_config);
    if (result != SASL_OK) goto done;
    if (path_to_config == NULL) path_to_config = "";

    next = path_to_config;

    while (next != NULL) {
        next = strchr(path_to_config, PATHS_DELIMITER);

        /* length = length of path + '/' + length of appname + ".conf" + 1
            for '\0' */

        if (next != NULL) {
            path_len = next - path_to_config;
            next++; /* Skip to the next path */
        } else {
            path_len = strlen(path_to_config);
        }

        len = path_len + 2 + strlen(global_callbacks.appname) + 5 + 1;

        if (len > PATH_MAX ) {
            result = SASL_FAIL;
            goto done;
        }

        /* construct the filename for the config file */
        config_filename = sasl_ALLOC((unsigned)len);
        if (! config_filename) {
            result = SASL_NOMEM;
            goto done;
        }

        snprintf(config_filename, len, "%.*s%c%s.conf", (int)path_len, path_to_config,
	        HIER_DELIMITER, global_callbacks.appname);

        /* Ask the application if it's safe to use this file */
        result = ((sasl_verifyfile_t *)(verifyfile_cb->proc))(verifyfile_cb->context,
					        config_filename, SASL_VRFY_CONF);

        /* returns SASL_CONTINUE if the config file doesn't exist */
        if (result == SASL_OK) {
            result = sasl_config_init(config_filename);

            if (result != SASL_CONTINUE) {
                /* We are done */
                break;
            }
        }

        if (config_filename) {
            sasl_FREE(config_filename);
            config_filename = NULL;
        }

        path_to_config = next;
    }

 done:
    if (config_filename) sasl_FREE(config_filename);

    return result;
}

/*
 * Verify that all the callbacks are valid
 */
static int verify_server_callbacks(const sasl_callback_t *callbacks)
{
    if (callbacks == NULL) return SASL_OK;

    while (callbacks->id != SASL_CB_LIST_END) {
	if (callbacks->proc==NULL) return SASL_FAIL;

	callbacks++;
    }

    return SASL_OK;
}

static char *grab_field(char *line, char **eofield)
{
    int d = 0;
    char *field;

    while (isspace((int) *line)) line++;

    /* find end of field */
    while (line[d] && !isspace(((int) line[d]))) d++;
    field = sasl_ALLOC(d + 1);
    if (!field) { return NULL; }
    memcpy(field, line, d);
    field[d] = '\0';
    *eofield = line + d;
    
    return field;
}

struct secflag_map_s {
    char *name;
    int value;
};

struct secflag_map_s secflag_map[] = {
    { "noplaintext", SASL_SEC_NOPLAINTEXT },
    { "noactive", SASL_SEC_NOACTIVE },
    { "nodictionary", SASL_SEC_NODICTIONARY },
    { "forward_secrecy", SASL_SEC_FORWARD_SECRECY },
    { "noanonymous", SASL_SEC_NOANONYMOUS },
    { "pass_credentials", SASL_SEC_PASS_CREDENTIALS },
    { "mutual_auth", SASL_SEC_MUTUAL_AUTH },
    { NULL, 0x0 }
};

static int parse_mechlist_file(const char *mechlistfile)
{
    FILE *f;
    char buf[1024];
    char *t, *ptr;
    int r = 0;

    f = fopen(mechlistfile, "r");
    if (!f) return SASL_FAIL;

    r = SASL_OK;
    while (fgets(buf, sizeof(buf), f) != NULL) {
	mechanism_t *n = sasl_ALLOC(sizeof(mechanism_t));
	sasl_server_plug_t *nplug;

	if (n == NULL) { r = SASL_NOMEM; break; }
	n->m.version = SASL_SERVER_PLUG_VERSION;
	n->m.condition = SASL_CONTINUE;
	nplug = sasl_ALLOC(sizeof(sasl_server_plug_t));
	if (nplug == NULL) { r = SASL_NOMEM; break; }
	memset(nplug, 0, sizeof(sasl_server_plug_t));

	/* each line is:
	   plugin-file WS mech_name WS max_ssf *(WS security_flag) RET
	*/
	
	/* grab file */
	n->m.f = grab_field(buf, &ptr);

	/* grab mech_name */
	nplug->mech_name = grab_field(ptr, &ptr);

	/* grab max_ssf */
	nplug->max_ssf = strtol(ptr, &ptr, 10);

	/* grab security flags */
	while (*ptr != '\n') {
	    struct secflag_map_s *map;

	    /* read security flag */
	    t = grab_field(ptr, &ptr);
	    map = secflag_map;
	    while (map->name) {
		if (!strcasecmp(t, map->name)) {
		    nplug->security_flags |= map->value;
		    break;
		}
		map++;
	    }
	    if (!map->name) {
		_sasl_log(NULL, SASL_LOG_ERR,
			  "%s: couldn't identify flag '%s'",
			  nplug->mech_name, t);
	    }
	    free(t);
	}

	/* insert mechanism into mechlist */
	n->m.plug = nplug;
	n->next = mechlist->mech_list;
	mechlist->mech_list = n;
	mechlist->mech_length++;
    }

    fclose(f);
    return r;
}

/* initialize server drivers, done once per process
 *  callbacks      -- callbacks for all server connections; must include
 *                    getopt callback
 *  appname        -- name of calling application
 *                    (for lower level logging and reading of the configuration file)
 * results:
 *  state          -- server state
 * returns:
 *  SASL_OK        -- success
 *  SASL_BADPARAM  -- error in config file
 *  SASL_NOMEM     -- memory failure
 *  SASL_BADVERS   -- Mechanism version mismatch
 */

int sasl_server_init(const sasl_callback_t *callbacks,
		     const char *appname)
{
    int ret;
    const sasl_callback_t *vf;
    const char *pluginfile = NULL;
#if false
    sasl_getopt_t *getopt;
    void *context;
#endif

    const add_plugin_list_t ep_list[] = {
	{ "sasl_server_plug_init", (add_plugin_t *)sasl_server_add_plugin },
	{ "sasl_auxprop_plug_init", (add_plugin_t *)sasl_auxprop_add_plugin },
	{ "sasl_canonuser_init", (add_plugin_t *)sasl_canonuser_add_plugin },
	{ NULL, NULL }
    };

    /* lock allocation type */
    _sasl_allocation_locked++;

    /* we require the appname (if present) to be short enough to be a path */
    if (appname != NULL && strlen(appname) >= PATH_MAX)
	return SASL_BADPARAM;

    if (_sasl_server_active) {
	/* We're already active, just increase our refcount */
	/* xxx do something with the callback structure? */
	_sasl_server_active++;
	return SASL_OK;
    }
    
    ret = _sasl_common_init(&global_callbacks);
    if (ret != SASL_OK)
	return ret;
 
    /* verify that the callbacks look ok */
    ret = verify_server_callbacks(callbacks);
    if (ret != SASL_OK)
	return ret;

    global_callbacks.callbacks = callbacks;
    
    /* A shared library calling sasl_server_init will pass NULL as appname.
       This should retain the original appname. */
    if (appname != NULL) {
        global_callbacks.appname = appname;
    }

    /* If we fail now, we have to call server_done */
    _sasl_server_active = 1;

    /* allocate mechlist and set it to empty */
    mechlist = sasl_ALLOC(sizeof(mech_list_t));
    if (mechlist == NULL) {
	server_done();
	return SASL_NOMEM;
    }

    ret = init_mechlist();
    if (ret != SASL_OK) {
	server_done();
	return ret;
    }

    vf = _sasl_find_verifyfile_callback(callbacks);

    /* load config file if applicable */
    ret = load_config(vf);
    if ((ret != SASL_OK) && (ret != SASL_CONTINUE)) {
	server_done();
	return ret;
    }

    /* load internal plugins */
    sasl_server_add_plugin("EXTERNAL", &external_server_plug_init);

#if false
    /* delayed loading of plugins? (DSO only, as it doesn't
     * make much [any] sense to delay in the static library case) */
    if (_sasl_getcallback(NULL, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context) 
	   == SASL_OK) {
	/* No sasl_conn_t was given to getcallback, so we provide the
	 * global callbacks structure */
	ret = getopt(&global_callbacks, NULL, "plugin_list", &pluginfile, NULL);
    }
#endif
    
    if (pluginfile != NULL) {
	/* this file should contain a list of plugins available.
	   we'll load on demand. */

	/* Ask the application if it's safe to use this file */
	ret = ((sasl_verifyfile_t *)(vf->proc))(vf->context,
						pluginfile,
						SASL_VRFY_CONF);
	if (ret != SASL_OK) {
	    _sasl_log(NULL, SASL_LOG_ERR,
		      "unable to load plugin list %s: %z", pluginfile, ret);
	}
	
	if (ret == SASL_OK) {
	    ret = parse_mechlist_file(pluginfile);
	}
    } else {
	/* load all plugins now */
	ret = _sasl_load_plugins(ep_list,
				 _sasl_find_getpath_callback(callbacks),
				 _sasl_find_verifyfile_callback(callbacks));
    }

    if (ret == SASL_OK) {
	_sasl_server_cleanup_hook = &server_done;
	_sasl_server_idle_hook = &server_idle;

	ret = _sasl_build_mechlist();
    } else {
	server_done();
    }

    return ret;
}

/*
 * Once we have the users plaintext password we 
 * may want to transition them. That is put entries
 * for them in the passwd database for other
 * stronger mechanism
 *
 * for example PLAIN -> CRAM-MD5
 */
static int
_sasl_transition(sasl_conn_t * conn,
		 const char * pass,
		 unsigned passlen)
{
    const char *dotrans = "n";
    sasl_getopt_t *getopt;
    int result = SASL_OK;
    void *context;
    unsigned flags = 0;

    if (! conn)
	return SASL_BADPARAM;

    if (! conn->oparams.authid)
	PARAMERROR(conn);

    /* check if this is enabled: default to false */
    if (_sasl_getcallback(conn, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context) == SASL_OK)
    {
	getopt(context, NULL, "auto_transition", &dotrans, NULL);
	if (dotrans == NULL) dotrans = "n";
    }


    if (!strcmp(dotrans, "noplain")) flags |= SASL_SET_NOPLAIN;

    if (flags || *dotrans == '1' || *dotrans == 'y' ||
	(*dotrans == 'o' && dotrans[1] == 'n') || *dotrans == 't') {
	/* ok, it's on! */
	_sasl_log(conn, SASL_LOG_NOTE, 
		  "transitioning user %s to auxprop database",
		  conn->oparams.authid);
	result = sasl_setpass(conn,
			      conn->oparams.authid,
			      pass,
			      passlen,
			      NULL, 0, SASL_SET_CREATE | flags);
    }

    RETURN(conn,result);
}


/* create context for a single SASL connection
 *  service        -- registered name of the service using SASL (e.g. "imap")
 *  serverFQDN     -- Fully qualified domain name of server.  NULL means use
 *                    gethostname() or equivalent.
 *                    Useful for multi-homed servers.
 *  user_realm     -- permits multiple user realms on server, NULL = default
 *  iplocalport    -- server IPv4/IPv6 domain literal string with port
 *                    (if NULL, then mechanisms requiring IPaddr are disabled)
 *  ipremoteport   -- client IPv4/IPv6 domain literal string with port
 *                    (if NULL, then mechanisms requiring IPaddr are disabled)
 *  callbacks      -- callbacks (e.g., authorization, lang, new getopt context)
 *  flags          -- usage flags (see above)
 * returns:
 *  pconn          -- new connection context
 *
 * returns:
 *  SASL_OK        -- success
 *  SASL_NOMEM     -- not enough memory
 */

int sasl_server_new(const char *service,
		    const char *serverFQDN,
		    const char *user_realm,
		    const char *iplocalport,
		    const char *ipremoteport,
		    const sasl_callback_t *callbacks,
		    unsigned flags,
		    sasl_conn_t **pconn)
{
  int result;
  sasl_server_conn_t *serverconn;
  sasl_utils_t *utils;
  sasl_getopt_t *getopt;
  void *context;
  const char *log_level, *auto_trans;
  const char *mlist = NULL;
  int plus = 0;

  if (_sasl_server_active==0) return SASL_NOTINIT;
  if (! pconn) return SASL_FAIL;
  if (! service) return SASL_FAIL;

  *pconn=sasl_ALLOC(sizeof(sasl_server_conn_t));
  if (*pconn==NULL) return SASL_NOMEM;

  memset(*pconn, 0, sizeof(sasl_server_conn_t));

  serverconn = (sasl_server_conn_t *)*pconn;

  /* make sparams */
  serverconn->sparams=sasl_ALLOC(sizeof(sasl_server_params_t));
  if (serverconn->sparams==NULL)
      MEMERROR(*pconn);

  memset(serverconn->sparams, 0, sizeof(sasl_server_params_t));

  (*pconn)->destroy_conn = &server_dispose;
  result = _sasl_conn_init(*pconn, service, flags, SASL_CONN_SERVER,
			   &server_idle, serverFQDN,
			   iplocalport, ipremoteport,
			   callbacks, &global_callbacks);
  if (result != SASL_OK)
      goto done_error;


  /* set util functions - need to do rest */
  utils=_sasl_alloc_utils(*pconn, &global_callbacks);
  if (!utils) {
      result = SASL_NOMEM;
      goto done_error;
  }
  
  utils->checkpass = &_sasl_checkpass;

  /* Setup the propctx -> We'll assume the default size */
  serverconn->sparams->propctx=prop_new(0);
  if(!serverconn->sparams->propctx) {
      result = SASL_NOMEM;
      goto done_error;
  }

  serverconn->sparams->service = (*pconn)->service;
  serverconn->sparams->servicelen = (unsigned) strlen((*pconn)->service);

  if (global_callbacks.appname && global_callbacks.appname[0] != '\0') {
    result = _sasl_strdup (global_callbacks.appname,
			   &serverconn->appname,
			   NULL);
    if (result != SASL_OK) {
      result = SASL_NOMEM;
      goto done_error;
    }
    serverconn->sparams->appname = serverconn->appname;
    serverconn->sparams->applen = (unsigned) strlen(serverconn->sparams->appname);
  } else {
    serverconn->appname = NULL;
    serverconn->sparams->appname = NULL;
    serverconn->sparams->applen = 0;
  }

  serverconn->sparams->serverFQDN = (*pconn)->serverFQDN;
  serverconn->sparams->slen = (unsigned) strlen((*pconn)->serverFQDN);

  if (user_realm) {
      result = _sasl_strdup(user_realm, &serverconn->user_realm, NULL);
      serverconn->sparams->urlen = (unsigned) strlen(user_realm);
      serverconn->sparams->user_realm = serverconn->user_realm;
  } else {
      serverconn->user_realm = NULL;
      /* the sparams is already zeroed */
  }

  serverconn->sparams->callbacks = callbacks;

  log_level = auto_trans = NULL;
  if(_sasl_getcallback(*pconn, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context) == SASL_OK) {
    getopt(context, NULL, "log_level", &log_level, NULL);
    getopt(context, NULL, "auto_transition", &auto_trans, NULL);
    getopt(context, NULL, "mech_list", &mlist, NULL);
  }
  serverconn->sparams->log_level = log_level ? atoi(log_level) : SASL_LOG_ERR;

  serverconn->sparams->utils = utils;

  if (auto_trans &&
      (*auto_trans == '1' || *auto_trans == 'y' || *auto_trans == 't' ||
       (*auto_trans == 'o' && auto_trans[1] == 'n') ||
       !strcmp(auto_trans, "noplain")) &&
      sasl_auxprop_store(NULL, NULL, NULL) == SASL_OK) {
      serverconn->sparams->transition = &_sasl_transition;
  }

  /* if we have a mech_list, create ordered list of avail mechs for this conn */
  if (mlist) {
      const char *cp;
      mechanism_t *mptr, *tail = NULL;

      while (*mlist) {
	  /* find end of current mech name */
	  for (cp = mlist; *cp && !isspace((int) *cp); cp++);

	  /* search for mech name in loaded plugins */
	  for (mptr = mechlist->mech_list; mptr; mptr = mptr->next) {
	      const sasl_server_plug_t *plug = mptr->m.plug;

	      if (_sasl_is_equal_mech(mlist, plug->mech_name, (size_t) (cp - mlist), &plus)) {
		  /* found a match */
		  break;
	      }
	  }
	  if (mptr) {
	      mechanism_t *new = sasl_ALLOC(sizeof(mechanism_t));
	      if (!new) return SASL_NOMEM;

	      memcpy(&new->m, &mptr->m, sizeof(server_sasl_mechanism_t));
	      new->next = NULL;

	      if (!serverconn->mech_list) {
		  serverconn->mech_list = new;
		  tail = serverconn->mech_list;
	      }
	      else {
		  if (tail)
                     tail->next = new;
		  tail = new;
	      }
	      serverconn->mech_length++;
	  }

	  /* find next mech name */
	  mlist = cp;
	  while (*mlist && isspace((int) *mlist)) mlist++;
      }
  }
  else {
      serverconn->mech_list = mechlist->mech_list;
      serverconn->mech_length = mechlist->mech_length;
  }

  serverconn->sparams->canon_user = &_sasl_canon_user_lookup;
  serverconn->sparams->props = serverconn->base.props;
  serverconn->sparams->flags = flags;

  if(result == SASL_OK) return SASL_OK;

 done_error:
  _sasl_conn_dispose(*pconn);
  sasl_FREE(*pconn);
  *pconn = NULL;
  return result;
}

/*
 * The rule is:
 * IF mech strength + external strength < min ssf THEN FAIL.
 * We also have to look at the security properties and make sure
 * that this mechanism has everything we want.
 */
static int mech_permitted(sasl_conn_t *conn,
			  mechanism_t *mech)
{
    sasl_server_conn_t *s_conn = (sasl_server_conn_t *)conn;
    const sasl_server_plug_t *plug;
    int ret;
    int myflags;
    context_list_t *cur;
    context_list_t *mech_context_list_entry = NULL;
    void *context = NULL;
    sasl_ssf_t minssf = 0;

    if(!conn) return SASL_NOMECH;

    if(! mech || ! mech->m.plug) {
	PARAMERROR(conn);
	return SASL_NOMECH;
    }
    
    plug = mech->m.plug;

    /* setup parameters for the call to mech_avail */
    s_conn->sparams->serverFQDN=conn->serverFQDN;
    s_conn->sparams->service=conn->service;
    s_conn->sparams->user_realm=s_conn->user_realm;
    s_conn->sparams->props=conn->props;
    s_conn->sparams->external_ssf=conn->external.ssf;

    /* Check if we have banished this one already */
    for (cur = s_conn->mech_contexts; cur; cur=cur->next) {
	if (cur->mech == mech) {
	    /* If it's not mech_avail'd, then stop now */
	    if (!cur->context) {
		return SASL_NOMECH;
	    } else {
		context = cur->context;
		mech_context_list_entry = cur;
	    }
	    break;
	}
    }
    
    if (conn->props.min_ssf < conn->external.ssf) {
	minssf = 0;
    } else {
	minssf = conn->props.min_ssf - conn->external.ssf;
    }
    
    /* Generic mechanism */
    if (plug->max_ssf < minssf) {
	sasl_seterror(conn, SASL_NOLOG,
		      "mech %s is too weak", plug->mech_name);
	return SASL_TOOWEAK; /* too weak */
    }

    if (plug->mech_avail
        && (ret = plug->mech_avail(plug->glob_context,
				   s_conn->sparams,
				   (void **)&context)) != SASL_OK ) {
	if (ret == SASL_NOMECH) {
	    /* Mark this mech as no good for this connection */
	    cur = sasl_ALLOC(sizeof(context_list_t));
	    if (!cur) {
		MEMERROR(conn);
		return SASL_NOMECH;
	    }
	    cur->context = NULL;
	    cur->mech = mech;
	    cur->next = s_conn->mech_contexts;
	    s_conn->mech_contexts = cur;
	}
	
	/* SASL_NOTDONE might also get us here */

	/* Error should be set by mech_avail call */
	return SASL_NOMECH;
    } else if (context) {
	if (mech_context_list_entry != NULL) {
	    /* Update the context. It shouldn't have changed, but who knows */
	    mech_context_list_entry->context = context;
	} else {
	    /* Save this context */
	    cur = sasl_ALLOC(sizeof(context_list_t));
	    if (!cur) {
		MEMERROR(conn);
		return SASL_NOMECH;
	    }
	    cur->context = context;
	    cur->mech = mech;
	    cur->next = s_conn->mech_contexts;
	    s_conn->mech_contexts = cur;
	}
    }
    
    /* Generic mechanism */
    if (plug->max_ssf < minssf) {
	sasl_seterror(conn, SASL_NOLOG, "too weak");
	return SASL_TOOWEAK; /* too weak */
    }

    /* if there are no users in the secrets database we can't use this 
       mechanism */
    if (mech->m.condition == SASL_NOUSER) {
	sasl_seterror(conn, 0, "no users in secrets db");
	return SASL_NOMECH;
    }

    /* Can it meet our features? */
    if ((conn->flags & SASL_NEED_PROXY) &&
	!(plug->features & SASL_FEAT_ALLOWS_PROXY)) {
	return SASL_NOMECH;
    }
    if ((conn->flags & SASL_NEED_HTTP) &&
	!(plug->features & SASL_FEAT_SUPPORTS_HTTP)) {
	return SASL_NOMECH;
    }
    
    /* security properties---if there are any flags that differ and are
       in what the connection are requesting, then fail */
    
    /* special case plaintext */
    myflags = conn->props.security_flags;

    /* if there's an external layer this is no longer plaintext */
    if ((conn->props.min_ssf <= conn->external.ssf) && 
	(conn->external.ssf > 1)) {
	myflags &= ~SASL_SEC_NOPLAINTEXT;
    }

    /* do we want to special case SASL_SEC_PASS_CREDENTIALS? nah.. */
    if ((myflags &= (myflags ^ plug->security_flags)) != 0) {
	sasl_seterror(conn, SASL_NOLOG,
		      "security flags do not match required");
	return (myflags & SASL_SEC_NOPLAINTEXT) ? SASL_ENCRYPT : SASL_NOMECH;
    }

    /* Check Features */
    if (plug->features & SASL_FEAT_GETSECRET) {
	/* We no longer support sasl_server_{get,put}secret */
	sasl_seterror(conn, 0,
		      "mech %s requires unprovided secret facility",
		      plug->mech_name);
	return SASL_NOMECH;
    }

    return SASL_OK;
}

/*
 * make the authorization 
 *
 */

static int do_authorization(sasl_server_conn_t *s_conn)
{
    int ret;
    sasl_authorize_t *authproc;
    void *auth_context;
    
    /* now let's see if authname is allowed to proxy for username! */
    
    /* check the proxy callback */
    if (_sasl_getcallback(&s_conn->base, SASL_CB_PROXY_POLICY,
			  (sasl_callback_ft *)&authproc, &auth_context) != SASL_OK) {
	INTERROR(&s_conn->base, SASL_NOAUTHZ);
    }

    ret = authproc(&(s_conn->base), auth_context,
		   s_conn->base.oparams.user, s_conn->base.oparams.ulen,
		   s_conn->base.oparams.authid, s_conn->base.oparams.alen,
		   s_conn->user_realm,
		   (s_conn->user_realm ? (unsigned) strlen(s_conn->user_realm) : 0),
		   s_conn->sparams->propctx);

    RETURN(&s_conn->base, ret);
}


/* start a mechanism exchange within a connection context
 *  mech           -- the mechanism name client requested
 *  clientin       -- client initial response (NUL terminated), NULL if empty
 *  clientinlen    -- length of initial response
 *  serverout      -- initial server challenge, NULL if done 
 *                    (library handles freeing this string)
 *  serveroutlen   -- length of initial server challenge
 * output:
 *  pconn          -- the connection negotiation state on success
 *
 * Same returns as sasl_server_step() or
 * SASL_NOMECH if mechanism not available.
 */
int sasl_server_start(sasl_conn_t *conn,
		      const char *mech,
		      const char *clientin,
		      unsigned clientinlen,
		      const char **serverout,
		      unsigned *serveroutlen)
{
    sasl_server_conn_t *s_conn=(sasl_server_conn_t *) conn;
    int result;
    context_list_t *cur, **prev;
    mechanism_t *m;
    size_t mech_len;
    int plus = 0;

    if (_sasl_server_active==0) return SASL_NOTINIT;

    /* check parameters */
    if(!conn) return SASL_BADPARAM;
    
    if (!mech || ((clientin == NULL) && (clientinlen > 0)))
	PARAMERROR(conn);

    if (serverout) *serverout = NULL;
    if (serveroutlen) *serveroutlen = 0;

    /* make sure mech is valid mechanism
       if not return appropriate error */
    m = s_conn->mech_list;
    mech_len = strlen(mech);

    while (m != NULL) {
	if (_sasl_is_equal_mech(mech, m->m.plug->mech_name, mech_len, &plus)) {
	    break;
	}

	m = m->next;
    }
  
    if (m == NULL) {
	sasl_seterror(conn, 0, "Couldn't find mech %s", mech);
	result = SASL_NOMECH;
	goto done;
    }

    /* Make sure that we're willing to use this mech */
    if ((result = mech_permitted(conn, m)) != SASL_OK) {
	goto done;
    }

    if (m->m.condition == SASL_CONTINUE) {
	sasl_server_plug_init_t *entry_point = NULL;
	void *library = NULL;
	sasl_server_plug_t *pluglist = NULL;
	int version, plugcount;
	int l = 0;

	/* need to load this plugin */
	result = _sasl_get_plugin(m->m.f,
		    _sasl_find_verifyfile_callback(global_callbacks.callbacks),
				  &library);

	if (result == SASL_OK) {
	    result = _sasl_locate_entry(library, "sasl_server_plug_init",
					(void **)&entry_point);
	}

	if (result == SASL_OK) {
	    result = entry_point(mechlist->utils, SASL_SERVER_PLUG_VERSION,
				 &version, &pluglist, &plugcount);
	}

	if (result == SASL_OK) {
	    /* find the correct mechanism in this plugin */
	    for (l = 0; l < plugcount; l++) {
		if (!strcasecmp(pluglist[l].mech_name, 
				m->m.plug->mech_name)) break;
	    }
	    if (l == plugcount) {
		result = SASL_NOMECH;
	    }
	}
	if (result == SASL_OK) {
	    /* check that the parameters are the same */
	    if ((pluglist[l].max_ssf != m->m.plug->max_ssf) ||
		(pluglist[l].security_flags != m->m.plug->security_flags)) {
		_sasl_log(conn, SASL_LOG_ERR, 
			  "%s: security parameters don't match mechlist file",
			  pluglist[l].mech_name);
		result = SASL_NOMECH;
	    }
	}
	if (result == SASL_OK) {
	    /* copy mechlist over */
	    sasl_FREE((sasl_server_plug_t *) m->m.plug);
	    m->m.plug = &pluglist[l];
	    m->m.condition = SASL_OK;
	}

	if (result != SASL_OK) {
	    /* The library will eventually be freed, don't sweat it */
	    RETURN(conn, result);
	}
    }

    if (conn->context) {
	s_conn->mech->m.plug->mech_dispose(conn->context,
					   s_conn->sparams->utils);
	conn->context = NULL;
    }

    /* We used to setup sparams HERE, but now it's done
       inside of mech_permitted (which is called above) */
    prev = &s_conn->mech_contexts;
    for (cur = *prev; cur; prev=&cur->next,cur=cur->next) {
	if (cur->mech == m) {
	    if (!cur->context) {
		sasl_seterror(conn, 0,
			      "Got past mech_permitted with a disallowed mech!");
		return SASL_NOMECH;
	    }
	    /* If we find it, we need to pull cur out of the
	       list so it won't be freed later! */
	    *prev = cur->next;
	    conn->context = cur->context;
	    sasl_FREE(cur);
	    break;
	}
    }

    s_conn->mech = m;
    
    if (!conn->context) {
	/* Note that we don't hand over a new challenge */
	result = s_conn->mech->m.plug->mech_new(s_conn->mech->m.plug->glob_context,
						s_conn->sparams,
						NULL,
						0,
						&(conn->context));
    } else {
	/* the work was already done by mech_avail! */
	result = SASL_OK;
    }
    
    if (result == SASL_OK) {
         if (clientin) {
            if (s_conn->mech->m.plug->features & SASL_FEAT_SERVER_FIRST) {
                /* Remote sent first, but mechanism does not support it.
                 * RFC 2222 says we fail at this point. */
                sasl_seterror(conn,
			      0,
                              "Remote sent first but mech does not allow it.");
                result = SASL_BADPROT;
            } else {
                /* Mech wants client-first, so let them have it */
                result = sasl_server_step(conn,
                                          clientin,
					  clientinlen,
                                          serverout,
					  serveroutlen);
            }
        } else {
            if (s_conn->mech->m.plug->features & SASL_FEAT_WANT_CLIENT_FIRST) {
                /* Mech wants client first anyway, so we should do that */
		if (serverout) *serverout = "";
		if (serveroutlen) *serveroutlen = 0;
                result = SASL_CONTINUE;
            } else {
                /* Mech wants server-first, so let them have it */
                result = sasl_server_step(conn,
                                          clientin,
					  clientinlen,
                                          serverout,
					  serveroutlen);
            }
	}
    }

 done:
    if (  result != SASL_OK
       && result != SASL_CONTINUE
       && result != SASL_INTERACT) {
	if (conn->context) {
	    s_conn->mech->m.plug->mech_dispose(conn->context,
					       s_conn->sparams->utils);
	    conn->context = NULL;
	}
	conn->oparams.doneflag = 0;
    }
    
    RETURN(conn,result);
}


/* perform one step of the SASL exchange
 *  clientinlen & clientin -- client data
 *                      NULL on first step if no optional client step
 *  serveroutlen & serverout -- set to the server data to transmit
 *                        to the client in the next step
 *                        (library handles freeing this)
 *
 * returns:
 *  SASL_OK        -- exchange is complete.
 *  SASL_CONTINUE  -- indicates another step is necessary.
 *  SASL_TRANS     -- entry for user exists, but not for mechanism
 *                    and transition is possible
 *  SASL_BADPARAM  -- service name needed
 *  SASL_BADPROT   -- invalid input from client
 *  ...
 */

int sasl_server_step(sasl_conn_t *conn,
		     const char *clientin,
		     unsigned clientinlen,
		     const char **serverout,
		     unsigned *serveroutlen)
{
    int ret;
    sasl_server_conn_t *s_conn = (sasl_server_conn_t *) conn;  /* cast */

    /* check parameters */
    if (_sasl_server_active==0) return SASL_NOTINIT;
    if (!conn) return SASL_BADPARAM;
    if ((clientin==NULL) && (clientinlen>0))
	PARAMERROR(conn);

    /* If we've already done the last send, return! */
    if (s_conn->sent_last == 1) {
	return SASL_OK;
    }

    /* Don't do another step if the plugin told us that we're done */
    if (conn->oparams.doneflag) {
	_sasl_log(conn, SASL_LOG_ERR, "attempting server step after doneflag");
	return SASL_FAIL;
    }

    if (serverout) *serverout = NULL;
    if (serveroutlen) *serveroutlen = 0;

    ret = s_conn->mech->m.plug->mech_step(conn->context,
					s_conn->sparams,
					clientin,
					clientinlen,
					serverout,
					serveroutlen,
					&conn->oparams);

    if (ret == SASL_OK) {
	ret = do_authorization(s_conn);
    }

    if (ret == SASL_OK) {
	/* if we're done, we need to watch out for the following:
	 * 1. the mech does server-send-last
	 * 2. the protocol does not
	 *
	 * in this case, return SASL_CONTINUE and remember we are done.
	 */
	if(*serverout && !(conn->flags & SASL_SUCCESS_DATA)) {
	    s_conn->sent_last = 1;
	    ret = SASL_CONTINUE;
	}
	if(!conn->oparams.maxoutbuf) {
	    conn->oparams.maxoutbuf = conn->props.maxbufsize;
	}

        /* Validate channel bindings */
	switch (conn->oparams.cbindingdisp) {
	case SASL_CB_DISP_NONE:
	    if (SASL_CB_CRITICAL(s_conn->sparams)) {
		sasl_seterror(conn, 0,
			      "server requires channel binding but client provided none");
		ret = SASL_BADBINDING;
	    }
	    break;
	case SASL_CB_DISP_WANT:
	    if (SASL_CB_PRESENT(s_conn->sparams)) {
		sasl_seterror(conn, 0,
			      "client incorrectly assumed server had no channel binding");
		ret = SASL_BADAUTH;
	    }
	    break;
	case SASL_CB_DISP_USED:
	    if (!SASL_CB_PRESENT(s_conn->sparams)) {
		sasl_seterror(conn, 0,
			      "client provided channel binding but server had none");
		ret = SASL_BADBINDING;
	    } else if (strcmp(conn->oparams.cbindingname,
		       s_conn->sparams->cbinding->name) != 0) {
		sasl_seterror(conn, 0,
			      "client channel binding %s does not match server %s",
			      conn->oparams.cbindingname, s_conn->sparams->cbinding->name);
		ret = SASL_BADBINDING;
	    }
	    break;
	}

        if (ret == SASL_OK &&
	    (conn->oparams.user == NULL || conn->oparams.authid == NULL)) {
	    sasl_seterror(conn, 0,
			  "mech did not call canon_user for both authzid " \
			  "and authid");
	    ret = SASL_BADPROT;
	}	
    }
    
    if (  ret != SASL_OK
       && ret != SASL_CONTINUE
       && ret != SASL_INTERACT) {
	if (conn->context) {
	    s_conn->mech->m.plug->mech_dispose(conn->context,
					     s_conn->sparams->utils);
	    conn->context = NULL;
	}
	conn->oparams.doneflag = 0;
    }

    RETURN(conn, ret);
}

/* returns the length of all the mechanisms
 * added up 
 */

static unsigned mech_names_len(mechanism_t *mech_list)
{
  mechanism_t *listptr;
  unsigned result = 0;

  for (listptr = mech_list;
       listptr;
       listptr = listptr->next)
    result += (unsigned) strlen(listptr->m.plug->mech_name);

  return result;
}

/* This returns a list of mechanisms in a NUL-terminated string
 *
 * The default behavior is to separate with spaces if sep == NULL
 */
int _sasl_server_listmech(sasl_conn_t *conn,
			  const char *user __attribute__((unused)),
			  const char *prefix,
			  const char *sep,
			  const char *suffix,
			  const char **result,
			  unsigned *plen,
			  int *pcount)
{
  sasl_server_conn_t *s_conn = (sasl_server_conn_t *) conn;  /* cast */
  int lup;
  mechanism_t *listptr;
  int ret;
  size_t resultlen;
  int flag;
  const char *mysep;

  /* if there hasn't been a sasl_sever_init() fail */
  if (_sasl_server_active==0) return SASL_NOTINIT;
  if (!conn) return SASL_BADPARAM;
  if (conn->type != SASL_CONN_SERVER) PARAMERROR(conn);
  
  if (! result)
      PARAMERROR(conn);

  if (plen != NULL)
      *plen = 0;
  if (pcount != NULL)
      *pcount = 0;

  if (sep) {
      mysep = sep;
  } else {
      mysep = " ";
  }

  if (!s_conn->mech_list || s_conn->mech_length <= 0)
      INTERROR(conn, SASL_NOMECH);

  resultlen = (prefix ? strlen(prefix) : 0)
            + (strlen(mysep) * (s_conn->mech_length - 1) * 2)
	    + (mech_names_len(s_conn->mech_list) * 2) /* including -PLUS variant */
	    + (s_conn->mech_length * (sizeof("-PLUS") - 1))
            + (suffix ? strlen(suffix) : 0)
	    + 1;

  ret = _buf_alloc(&conn->mechlist_buf,
		   &conn->mechlist_buf_len, resultlen);
  if(ret != SASL_OK) MEMERROR(conn);

  if (prefix)
    strcpy (conn->mechlist_buf,prefix);
  else
    *(conn->mechlist_buf) = '\0';

  listptr = s_conn->mech_list;
   
  flag = 0;
  /* make list */
  for (lup = 0; lup < s_conn->mech_length; lup++) {
      /* currently, we don't use the "user" parameter for anything */
      if (mech_permitted(conn, listptr) == SASL_OK) {

          /*
           * If the server would never succeed in the authentication of
           * the non-PLUS-variant due to policy reasons, it MUST advertise
           * only the PLUS-variant.
           */
	  if ((listptr->m.plug->features & SASL_FEAT_CHANNEL_BINDING) &&
	      SASL_CB_PRESENT(s_conn->sparams)) {
	    if (pcount != NULL) {
		(*pcount)++;
	    }
	    if (flag) {
              strcat(conn->mechlist_buf, mysep);
	    } else {
              flag = 1;
	    }
	    strcat(conn->mechlist_buf, listptr->m.plug->mech_name);
	    strcat(conn->mechlist_buf, "-PLUS");
	  }

          /*
           * If the server cannot support channel binding, it SHOULD
           * advertise only the non-PLUS-variant. Here, supporting channel
           * binding means the underlying SASL mechanism supports it and
           * the application has set some channel binding data.
           */
          if (!SASL_CB_PRESENT(s_conn->sparams) ||
              !SASL_CB_CRITICAL(s_conn->sparams)) {
            if (pcount != NULL) {
	      (*pcount)++;
	    }
	    if (flag) {
              strcat(conn->mechlist_buf, mysep);
	    } else {
              flag = 1;
	    }
	    strcat(conn->mechlist_buf, listptr->m.plug->mech_name);
          }
      }

      listptr = listptr->next;
  }

  if (suffix)
      strcat(conn->mechlist_buf,suffix);

  if (plen!=NULL)
      *plen = (unsigned) strlen(conn->mechlist_buf);

  *result = conn->mechlist_buf;

  return SASL_OK;  
}

sasl_string_list_t *_sasl_server_mechs(void) 
{
  mechanism_t *listptr;
  sasl_string_list_t *retval = NULL, *next=NULL;

  if(!_sasl_server_active) return NULL;

  /* make list */
  for (listptr = mechlist->mech_list; listptr; listptr = listptr->next) {
      next = sasl_ALLOC(sizeof(sasl_string_list_t));

      if(!next && !retval) return NULL;
      else if(!next) {
	  next = retval->next;
	  do {
	      sasl_FREE(retval);
	      retval = next;
	      next = retval->next;
	  } while(next);
	  return NULL;
      }
      
      next->d = listptr->m.plug->mech_name;

      if(!retval) {
	  next->next = NULL;
	  retval = next;
      } else {
	  next->next = retval;
	  retval = next;
      }
  }

  return retval;
}

#define EOSTR(s,n) (((s)[n] == '\0') || ((s)[n] == ' ') || ((s)[n] == '\t'))
static int is_mech(const char *t, const char *m)
{
    size_t sl = strlen(m);
    return ((!strncasecmp(m, t, sl)) && EOSTR(t, sl));
}

/* returns OK if it's valid */
static int _sasl_checkpass(sasl_conn_t *conn,
			   const char *user,
			   unsigned userlen,
			   const char *pass,
			   unsigned passlen)
{
    sasl_server_conn_t *s_conn = (sasl_server_conn_t *) conn;
    int result;
    sasl_getopt_t *getopt;
    sasl_server_userdb_checkpass_t *checkpass_cb;
    void *context;
    const char *mlist = NULL, *mech = NULL;
    struct sasl_verify_password_s *v;
    const char *service = conn->service;

    if (!userlen) userlen = (unsigned) strlen(user);
    if (!passlen) passlen = (unsigned) strlen(pass);

    /* call userdb callback function, if available */
    result = _sasl_getcallback(conn, SASL_CB_SERVER_USERDB_CHECKPASS,
			       (sasl_callback_ft *)&checkpass_cb, &context);
    if(result == SASL_OK && checkpass_cb) {
	result = checkpass_cb(conn, context, user, pass, passlen,
			      s_conn->sparams->propctx);
	if(result == SASL_OK)
	    return SASL_OK;
    }

    /* figure out how to check (i.e. auxprop or saslauthd or pwcheck) */
    if (_sasl_getcallback(conn, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context)
            == SASL_OK) {
        getopt(context, NULL, "pwcheck_method", &mlist, NULL);
    }

    if(!mlist) mlist = DEFAULT_CHECKPASS_MECH;

    result = SASL_NOMECH;

    mech = mlist;
    while (*mech && result != SASL_OK) {
	for (v = _sasl_verify_password; v->name; v++) {
	    if(is_mech(mech, v->name)) {
		result = v->verify(conn, user, pass, service,
				   s_conn->user_realm);
		break;
	    }
	}
	if (result != SASL_OK) {
	    /* skip to next mech in list */
	    while (*mech && !isspace((int) *mech)) mech++;
	    while (*mech && isspace((int) *mech)) mech++;
	}
	else if (!is_mech(mech, "auxprop") && s_conn->sparams->transition) {
	    s_conn->sparams->transition(conn, pass, passlen);
	}
    }

    if (result == SASL_NOMECH) {
	/* no mechanism available ?!? */
	_sasl_log(conn, SASL_LOG_ERR, "unknown password verifier(s) %s", mlist);
    }

    if (result != SASL_OK)
	sasl_seterror(conn, SASL_NOLOG, "checkpass failed");

    RETURN(conn, result);
}

/* check if a plaintext password is valid
 *   if user is NULL, check if plaintext passwords are enabled
 * inputs:
 *  user          -- user to query in current user_domain
 *  userlen       -- length of username, 0 = strlen(user)
 *  pass          -- plaintext password to check
 *  passlen       -- length of password, 0 = strlen(pass)
 * returns 
 *  SASL_OK       -- success
 *  SASL_NOMECH   -- mechanism not supported
 *  SASL_NOVERIFY -- user found, but no verifier
 *  SASL_NOUSER   -- user not found
 */
int sasl_checkpass(sasl_conn_t *conn,
		   const char *user,
		   unsigned userlen,
		   const char *pass,
		   unsigned passlen)
{
    int result;
    
    if (_sasl_server_active==0) return SASL_NOTINIT;
    
    /* check if it's just a query if we are enabled */
    if (!user)
	return SASL_OK;

    if (!conn) return SASL_BADPARAM;
    
    /* check params */
    if (pass == NULL)
	PARAMERROR(conn);

    /* canonicalize the username */
    result = _sasl_canon_user(conn, user, userlen,
			      SASL_CU_AUTHID | SASL_CU_AUTHZID,
			      &(conn->oparams));
    if(result != SASL_OK) RETURN(conn, result);
    user = conn->oparams.user;

    /* Check the password and lookup additional properties */
    result = _sasl_checkpass(conn, user, userlen, pass, passlen);

    /* Do authorization */
    if(result == SASL_OK) {
      result = do_authorization((sasl_server_conn_t *)conn);
    }

    RETURN(conn,result);
}

/* check if a user exists on server
 *  conn          -- connection context (may be NULL, used to hold last error)
 *  service       -- registered name of the service using SASL (e.g. "imap")
 *  user_realm    -- permits multiple user realms on server, NULL = default
 *  user          -- NUL terminated user name
 *
 * returns:
 *  SASL_OK       -- success
 *  SASL_DISABLED -- account disabled [FIXME: currently not detected]
 *  SASL_NOUSER   -- user not found
 *  SASL_NOVERIFY -- user found, but no usable mechanism [FIXME: not supported]
 *  SASL_NOMECH   -- no mechanisms enabled
 *  SASL_UNAVAIL  -- remote authentication server unavailable, try again later
 */
int sasl_user_exists(sasl_conn_t *conn,
		     const char *service,
		     const char *user_realm,
		     const char *user) 
{
    int result=SASL_NOMECH;
    const char *mlist = NULL, *mech = NULL;
    void *context;
    sasl_getopt_t *getopt;
    struct sasl_verify_password_s *v;
    
    /* check params */
    if (_sasl_server_active==0) return SASL_NOTINIT;
    if (!conn) return SASL_BADPARAM;
    if (!user || conn->type != SASL_CONN_SERVER) 
	PARAMERROR(conn);

    if(!service) service = conn->service;
    
    /* figure out how to check (i.e. auxprop or saslauthd or pwcheck) */
    if (_sasl_getcallback(conn, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context)
            == SASL_OK) {
        getopt(context, NULL, "pwcheck_method", &mlist, NULL);
    }

    if(!mlist) mlist = DEFAULT_CHECKPASS_MECH;

    result = SASL_NOMECH;

    mech = mlist;
    while (*mech && result != SASL_OK) {
	for (v = _sasl_verify_password; v->name; v++) {
	    if(is_mech(mech, v->name)) {
		result = v->verify(conn, user, NULL, service, user_realm);
		break;
	    }
	}
	if (result != SASL_OK) {
	    /* skip to next mech in list */
	    while (*mech && !isspace((int) *mech)) mech++;
	    while (*mech && isspace((int) *mech)) mech++;
	}
    }

    /* Screen out the SASL_BADPARAM response
     * we'll get from not giving a password */
    if (result == SASL_BADPARAM) {
	result = SASL_OK;
    }

    if (result == SASL_NOMECH) {
	/* no mechanism available ?!? */
	_sasl_log(conn, SASL_LOG_ERR, "no plaintext password verifier?");
	sasl_seterror(conn, SASL_NOLOG, "no plaintext password verifier?");
    }

    RETURN(conn, result);
}

/* check if an apop exchange is valid
 *  (note this is an optional part of the SASL API)
 *  if challenge is NULL, just check if APOP is enabled
 * inputs:
 *  challenge     -- challenge which was sent to client
 *  challen       -- length of challenge, 0 = strlen(challenge)
 *  response      -- client response, "<user> <digest>" (RFC 1939)
 *  resplen       -- length of response, 0 = strlen(response)
 * returns 
 *  SASL_OK       -- success
 *  SASL_BADAUTH  -- authentication failed
 *  SASL_BADPARAM -- missing challenge
 *  SASL_BADPROT  -- protocol error (e.g., response in wrong format)
 *  SASL_NOVERIFY -- user found, but no verifier
 *  SASL_NOMECH   -- mechanism not supported
 *  SASL_NOUSER   -- user not found
 */
int sasl_checkapop(sasl_conn_t *conn,
#ifdef DO_SASL_CHECKAPOP
 		   const char *challenge,
 		   unsigned challen __attribute__((unused)),
 		   const char *response,
 		   unsigned resplen __attribute__((unused)))
#else
 		   const char *challenge __attribute__((unused)),
 		   unsigned challen __attribute__((unused)),
 		   const char *response __attribute__((unused)),
 		   unsigned resplen __attribute__((unused)))
#endif
{
#ifdef DO_SASL_CHECKAPOP
    sasl_server_conn_t *s_conn = (sasl_server_conn_t *) conn;
    char *user, *user_end;
    const char *password_request[] = { SASL_AUX_PASSWORD, NULL };
    size_t user_len;
    int result;

    if (_sasl_server_active==0)
	return SASL_NOTINIT;

    /* check if it's just a query if we are enabled */
    if(!challenge)
	return SASL_OK;

    /* check params */
    if (!conn) return SASL_BADPARAM;
    if (!response)
	PARAMERROR(conn);

    /* Parse out username and digest.
     *
     * Per RFC 1939, response must be "<user> <digest>", where
     * <digest> is a 16-octet value which is sent in hexadecimal
     * format, using lower-case ASCII characters.
     */
    user_end = strrchr(response, ' ');
    if (!user_end || strspn(user_end + 1, "0123456789abcdef") != 32) 
    {
        sasl_seterror(conn, 0, "Bad Digest");
        RETURN(conn,SASL_BADPROT);
    }
 
    user_len = (size_t)(user_end - response);
    user = sasl_ALLOC(user_len + 1);
    memcpy(user, response, user_len);
    user[user_len] = '\0';

    result = prop_request(s_conn->sparams->propctx, password_request);
    if(result != SASL_OK) 
    {
        sasl_FREE(user);
        RETURN(conn, result);
    }

    /* erase the plaintext password */
    s_conn->sparams->utils->prop_erase(s_conn->sparams->propctx,
				       password_request[0]);

    /* canonicalize the username and lookup any associated properties */
    result = _sasl_canon_user_lookup (conn,
				      user,
				      user_len,
				      SASL_CU_AUTHID | SASL_CU_AUTHZID,
				      &(conn->oparams));
    sasl_FREE(user);

    if(result != SASL_OK) RETURN(conn, result);

    /* Do APOP verification */
    result = _sasl_auxprop_verify_apop(conn, conn->oparams.authid,
	challenge, user_end + 1, s_conn->user_realm);

    /* Do authorization */
    if(result == SASL_OK) {
      result = do_authorization((sasl_server_conn_t *)conn);
    } else {
        /* If verification failed, we don't want to encourage getprop to work */
	conn->oparams.user = NULL;
	conn->oparams.authid = NULL;
    }

    RETURN(conn, result);
#else /* sasl_checkapop was disabled at compile time */
    sasl_seterror(conn, SASL_NOLOG,
	"sasl_checkapop called, but was disabled at compile time");
    RETURN(conn, SASL_NOMECH);
#endif /* DO_SASL_CHECKAPOP */
}

/* It would be nice if we can show other information like Author, Company, Year, plugin version */
static void
_sasl_print_mechanism (
  server_sasl_mechanism_t *m,
  sasl_info_callback_stage_t stage,
  void *rock __attribute__((unused))
)
{
    char delimiter;

    if (stage == SASL_INFO_LIST_START) {
	printf ("List of server plugins follows\n");
	return;
    } else if (stage == SASL_INFO_LIST_END) {
	return;
    }

    /* Process the mechanism */
    printf ("Plugin \"%s\" ", m->plugname);

    switch (m->condition) {
	case SASL_OK:
	    printf ("[loaded]");
	    break;

	case SASL_CONTINUE:
	    printf ("[delayed]");
	    break;

	case SASL_NOUSER:
	    printf ("[no users]");
	    break;

	default:
	    printf ("[unknown]");
	    break;
    }

    printf (", \tAPI version: %d\n", m->version);

    if (m->plug != NULL) {
	printf ("\tSASL mechanism: %s, best SSF: %d, supports setpass: %s\n",
		m->plug->mech_name,
		m->plug->max_ssf,
		(m->plug->setpass != NULL) ? "yes" : "no"
		);


	printf ("\tsecurity flags:");
	
	delimiter = ' ';
	if (m->plug->security_flags & SASL_SEC_NOANONYMOUS) {
	    printf ("%cNO_ANONYMOUS", delimiter);
	    delimiter = '|';
	}

	if (m->plug->security_flags & SASL_SEC_NOPLAINTEXT) {
	    printf ("%cNO_PLAINTEXT", delimiter);
	    delimiter = '|';
	}
	
	if (m->plug->security_flags & SASL_SEC_NOACTIVE) {
	    printf ("%cNO_ACTIVE", delimiter);
	    delimiter = '|';
	}

	if (m->plug->security_flags & SASL_SEC_NODICTIONARY) {
	    printf ("%cNO_DICTIONARY", delimiter);
	    delimiter = '|';
	}

	if (m->plug->security_flags & SASL_SEC_FORWARD_SECRECY) {
	    printf ("%cFORWARD_SECRECY", delimiter);
	    delimiter = '|';
	}

	if (m->plug->security_flags & SASL_SEC_PASS_CREDENTIALS) {
	    printf ("%cPASS_CREDENTIALS", delimiter);
	    delimiter = '|';
	}

	if (m->plug->security_flags & SASL_SEC_MUTUAL_AUTH) {
	    printf ("%cMUTUAL_AUTH", delimiter);
	    delimiter = '|';
	}



	printf ("\n\tfeatures:");

	delimiter = ' ';
	if (m->plug->features & SASL_FEAT_WANT_CLIENT_FIRST) {
	    printf ("%cWANT_CLIENT_FIRST", delimiter);
	    delimiter = '|';
	}

	if (m->plug->features & SASL_FEAT_SERVER_FIRST) {
	    printf ("%cSERVER_FIRST", delimiter);
	    delimiter = '|';
	}

	if (m->plug->features & SASL_FEAT_ALLOWS_PROXY) {
	    printf ("%cPROXY_AUTHENTICATION", delimiter);
	    delimiter = '|';
	}

	if (m->plug->features & SASL_FEAT_DONTUSE_USERPASSWD) {
	    printf ("%cDONTUSE_USERPASSWD", delimiter);
	    delimiter = '|';
	}

	if (m->plug->features & SASL_FEAT_NEEDSERVERFQDN) {
	    printf ("%cNEED_SERVER_FQDN", delimiter);
	    delimiter = '|';
	}

        /* Is this one used? */
        if (m->plug->features & SASL_FEAT_SERVICE) {
	    printf ("%cSERVICE", delimiter);
	    delimiter = '|';
	}

        if (m->plug->features & SASL_FEAT_GETSECRET) {
	    printf ("%cNEED_GETSECRET", delimiter);
	    delimiter = '|';
	}

        if (m->plug->features & SASL_FEAT_GSS_FRAMING) {
	    printf ("%cGSS_FRAMING", delimiter);
	    delimiter = '|';
	}

        if (m->plug->features & SASL_FEAT_CHANNEL_BINDING) {
	    printf ("%cCHANNEL_BINDING", delimiter);
	    delimiter = '|';
	}

        if (m->plug->features & SASL_FEAT_SUPPORTS_HTTP) {
	    printf ("%cSUPPORTS_HTTP", delimiter);
	    delimiter = '|';
	}
    }

    if (m->f) {
	printf ("\n\twill be loaded from \"%s\"", m->f);
    }

    printf ("\n");
}

/* Dump information about available server plugins (separate functions should be
   used for canon and auxprop plugins */
int sasl_server_plugin_info (
  const char *c_mech_list,		/* space separated mechanism list or NULL for ALL */
  sasl_server_info_callback_t *info_cb,
  void *info_cb_rock
)
{
    mechanism_t *m;
    server_sasl_mechanism_t plug_data;
    char * cur_mech;
    char *mech_list = NULL;
    char * p;

    if (info_cb == NULL) {
	info_cb = _sasl_print_mechanism;
    }

    if (mechlist != NULL) {
	info_cb (NULL, SASL_INFO_LIST_START, info_cb_rock);

	if (c_mech_list == NULL) {
	    m = mechlist->mech_list; /* m point to beginning of the list */

	    while (m != NULL) {
		memcpy (&plug_data, &m->m, sizeof(plug_data));

		info_cb (&plug_data, SASL_INFO_LIST_MECH, info_cb_rock);
	    
		m = m->next;
	    }
	} else {
            mech_list = strdup(c_mech_list);

	    cur_mech = mech_list;

	    while (cur_mech != NULL) {
		p = strchr (cur_mech, ' ');
		if (p != NULL) {
		    *p = '\0';
		    p++;
		}

		m = mechlist->mech_list; /* m point to beginning of the list */

		while (m != NULL) {
		    if (strcasecmp (cur_mech, m->m.plug->mech_name) == 0) {
			memcpy (&plug_data, &m->m, sizeof(plug_data));

			info_cb (&plug_data, SASL_INFO_LIST_MECH, info_cb_rock);
		    }
	    
		    m = m->next;
		}

		cur_mech = p;
	    }

            free (mech_list);
	}

	info_cb (NULL, SASL_INFO_LIST_END, info_cb_rock);

	return (SASL_OK);
    }

    return (SASL_NOTINIT);
}
