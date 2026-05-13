/* windlopen.c--Windows dynamic loader interface
 * Ryan Troll
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

#include <stdio.h>
#include <io.h>
#include <sys/stat.h>

#include <config.h>
#include <sasl.h>
#include "saslint.h"
#include "staticopen.h"

#define DLL_SUFFIX	_T(".dll")
#define DLL_MASK	_T("*") DLL_SUFFIX
#define DLL_MASK_LEN	5 /* in symbols */

const int _is_sasl_server_static = 0;

/* : inefficient representation, but works */
typedef struct lib_list 
{
    struct lib_list *next;
    HMODULE library;
} lib_list_t;

static lib_list_t *lib_list_head = NULL;

int _sasl_locate_entry(void *library,
		       const char *entryname,
		       void **entry_point) 
{
    if(entryname == NULL) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "no entryname in _sasl_locate_entry");
	return SASL_BADPARAM;
    }

    if(library == NULL) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "no library in _sasl_locate_entry");
	return SASL_BADPARAM;
    }

    if(entry_point == NULL) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "no entrypoint output pointer in _sasl_locate_entry");
	return SASL_BADPARAM;
    }

    *entry_point = GetProcAddress(library, entryname);

    if (*entry_point == NULL) {
#if 0 /* This message appears to confuse people */
	_sasl_log(NULL, SASL_LOG_DEBUG,
		  "unable to get entry point %s: %s", entryname,
		  GetLastError());
#endif
	return SASL_FAIL;
    }

    return SASL_OK;
}

static int _sasl_plugin_load(const char *plugin, void *library,
			     const char *entryname,
			     int (*add_plugin)(const char *, void *)) 
{
    void *entry_point;
    int result;
    
    result = _sasl_locate_entry(library, entryname, &entry_point);
    if(result == SASL_OK) {
	result = add_plugin(plugin, entry_point);
	if(result != SASL_OK)
	    _sasl_log(NULL, SASL_LOG_DEBUG,
		      "_sasl_plugin_load failed on %s for plugin: %s\n",
		      entryname, plugin);
    }

    return result;
}

/* loads a plugin library */
static int _tsasl_get_plugin(TCHAR *tfile,
    const sasl_callback_t *verifyfile_cb,
    void **libraryptr)
{
    HINSTANCE library = NULL;
    lib_list_t *newhead;
    char *file;
    int retCode = SASL_OK;

    if (sizeof(TCHAR) != sizeof(char)) {
        file = _sasl_wchar_to_utf8(tfile);
        if (!file) {
            retCode = SASL_NOMEM;
            goto cleanup;
        }
    }
    else {
        file = (char*)tfile;
    }
    retCode = ((sasl_verifyfile_t *)(verifyfile_cb->proc))
		    (verifyfile_cb->context, file, SASL_VRFY_PLUGIN);
    if (retCode != SASL_OK)
        goto cleanup;

    newhead = sasl_ALLOC(sizeof(lib_list_t));
    if (!newhead) {
        retCode = SASL_NOMEM;
        goto cleanup;
    }

    if (!(library = LoadLibrary(tfile))) {
	    _sasl_log(NULL, SASL_LOG_ERR,
		      "unable to LoadLibrary %s: %s", file, GetLastError());
	    sasl_FREE(newhead);
        retCode = SASL_FAIL;
        goto cleanup;
    }

    newhead->library = library;
    newhead->next = lib_list_head;
    lib_list_head = newhead;

    *libraryptr = library;
cleanup:
    if (sizeof(TCHAR) != sizeof(char)) {
        sasl_FREE(file);
    }
    return retCode;
}

int _sasl_get_plugin(const char *file,
    const sasl_callback_t *verifyfile_cb,
    void **libraryptr)
{
    if (sizeof(TCHAR) == sizeof(char)) {
        return _tsasl_get_plugin((TCHAR*)file, verifyfile_cb, libraryptr);
    }
    else {
        WCHAR *tfile = _sasl_utf8_to_wchar(file);
        int ret = SASL_NOMEM;

        if (tfile) {
            ret = _tsasl_get_plugin(tfile, verifyfile_cb, libraryptr);
            sasl_FREE(tfile);
        }

        return ret;
    }
}


/* undoes actions done by _sasl_get_plugin */
void _sasl_remove_last_plugin()
{
    lib_list_t *last_plugin = lib_list_head;
    lib_list_head = lib_list_head->next;
    if (last_plugin->library) {
	FreeLibrary(last_plugin->library);
    }
    sasl_FREE(last_plugin);
}

/* gets the list of mechanisms */
int _sasl_load_plugins(const add_plugin_list_t *entrypoints,
		       const sasl_callback_t *getpath_cb,
		       const sasl_callback_t *verifyfile_cb)
{
    int result;
    TCHAR cur_dir[PATH_MAX], full_name[PATH_MAX+2], prefix[PATH_MAX+2];
				/* 1 for '\\' 1 for trailing '\0' */
    TCHAR * pattern;
    TCHAR c;
    int pos;
    int retCode = SASL_OK;
    char *utf8path = NULL;
    TCHAR *path=NULL;
    int position;
    const add_plugin_list_t *cur_ep;
    struct _stat statbuf;		/* filesystem entry information */
    intptr_t fhandle;			/* file handle for _findnext function */
    struct _tfinddata_t finddata;	/* data returned by _findnext() */
    size_t prefix_len;
    
    /* for static plugins */
    add_plugin_t *add_plugin;
    _sasl_plug_type type;
    _sasl_plug_rec *p;

    if (! entrypoints
	|| ! getpath_cb
	|| getpath_cb->id != SASL_CB_GETPATH
	|| ! getpath_cb->proc
	|| ! verifyfile_cb
	|| verifyfile_cb->id != SASL_CB_VERIFYFILE
	|| ! verifyfile_cb->proc)
	return SASL_BADPARAM;

    /* do all the static plugins first */

    for (cur_ep = entrypoints; cur_ep->entryname; cur_ep++) {

        /* What type of plugin are we looking for? */
        if (!strcmp(cur_ep->entryname, "sasl_server_plug_init")) {
            type = SERVER;
            add_plugin = (add_plugin_t *)sasl_server_add_plugin;
        }
        else if (!strcmp(cur_ep->entryname, "sasl_client_plug_init")) {
            type = CLIENT;
            add_plugin = (add_plugin_t *)sasl_client_add_plugin;
        }
        else if (!strcmp(cur_ep->entryname, "sasl_auxprop_plug_init")) {
            type = AUXPROP;
            add_plugin = (add_plugin_t *)sasl_auxprop_add_plugin;
        }
        else if (!strcmp(cur_ep->entryname, "sasl_canonuser_init")) {
            type = CANONUSER;
            add_plugin = (add_plugin_t *)sasl_canonuser_add_plugin;
        }
        else {
            /* What are we looking for then? */
            return SASL_FAIL;
        }
        for (p = _sasl_static_plugins; p->type; p++) {
            if (type == p->type)
                result = add_plugin(p->name, p->plug);
        }
    }

    return SASL_OK;
}

int
_sasl_done_with_plugins(void)
{
    lib_list_t *libptr, *libptr_next;
    
    for(libptr = lib_list_head; libptr; libptr = libptr_next) {
	libptr_next = libptr->next;
	if (libptr->library != NULL) {
	    FreeLibrary(libptr->library);
	}
	sasl_FREE(libptr);
    }

    lib_list_head = NULL;

    return SASL_OK;
}
