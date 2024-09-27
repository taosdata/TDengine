/* dlopen.c--Unix dlopen() dynamic loader interface
 * Rob Siemborski
 * Rob Earhart
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
#ifdef HAVE_DLFCN_H
#include <dlfcn.h>
#endif

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <limits.h>

#include <sasl.h>
#include "saslint.h"

#include <saslplug.h>
#include "staticopen.h"

#ifdef DO_DLOPEN
#if HAVE_DIRENT_H
# include <dirent.h>
# define NAMLEN(dirent) strlen((dirent)->d_name)
#else /* HAVE_DIRENT_H */
# define dirent direct
# define NAMLEN(dirent) (dirent)->d_namlen
# if HAVE_SYS_NDIR_H
#  include <sys/ndir.h>
# endif
# if HAVE_SYS_DIR_H
#  include <sys/dir.h>
# endif
# if HAVE_NDIR_H
#  include <ndir.h>
# endif
#endif /* ! HAVE_DIRENT_H */

#ifndef NAME_MAX
# ifdef _POSIX_NAME_MAX
#  define NAME_MAX _POSIX_NAME_MAX
# else
#  define NAME_MAX 16
# endif
#endif
 
#if NAME_MAX < 8
#  define NAME_MAX 8
#endif

#ifdef __hpux
#ifndef HAVE_DLFCN_H
#include <dl.h>

typedef shl_t * dll_handle;
typedef void * dll_func;

dll_handle
dlopen(char *fname, int mode)
{
    shl_t h = shl_load(fname, BIND_DEFERRED, 0L);
    shl_t *hp = NULL;
    
    if (h) {
	hp = (shl_t *)malloc(sizeof (shl_t));
	if (!hp) {
	    shl_unload(h);
	} else {
	    *hp = h;
	}
    }

    return (dll_handle)hp;
}

int
dlclose(dll_handle hp)
{
    shl_t h;

    if (hp != NULL) {
	h = *((shl_t *)hp);
	free(hp);
	return shl_unload(h);
    } else {
	/* Return error */
	return -1;
    }
}

dll_func
dlsym(dll_handle h, char *n)
{
    dll_func handle;
    
    if (shl_findsym ((shl_t *)h, n, TYPE_PROCEDURE, &handle))
	return NULL;
    
    return (dll_func)handle;
}

char *dlerror()
{
    if (errno != 0) {
	return strerror(errno);
    }
    return "Generic shared library error";
}

#endif /* HAVE_DLFCN_H */

#ifdef __ia64
#define SO_SUFFIX       ".so"
#else
#define SO_SUFFIX	".sl"
#endif /* __ia64 */

#else /* __hpux */
#define SO_SUFFIX	".so"
#endif

#define LA_SUFFIX       ".la"

typedef struct lib_list 
{
    struct lib_list *next;
    void *library;
} lib_list_t;

static lib_list_t *lib_list_head = NULL;

#endif /* DO_DLOPEN */

int _sasl_locate_entry(void *library, const char *entryname,
		       void **entry_point) 
{
#ifdef DO_DLOPEN
/* note that we still check for known problem systems in
 * case we are cross-compiling */
#if defined(DLSYM_NEEDS_UNDERSCORE) || (defined(__OpenBSD__) && !defined(__ELF__))
    char adj_entryname[1024];
#else
#define adj_entryname entryname
#endif

    if(!entryname) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "no entryname in _sasl_locate_entry");
	return SASL_BADPARAM;
    }

    if(!library) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "no library in _sasl_locate_entry");
	return SASL_BADPARAM;
    }

    if(!entry_point) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "no entrypoint output pointer in _sasl_locate_entry");
	return SASL_BADPARAM;
    }

#if defined(DLSYM_NEEDS_UNDERSCORE) || (defined(__OpenBSD__) && !defined(__ELF__))
    snprintf(adj_entryname, sizeof adj_entryname, "_%s", entryname);
#endif

    *entry_point = NULL;
    *entry_point = dlsym(library, adj_entryname);
    if (*entry_point == NULL) {
#if 0 /* This message appears to confuse people */
	_sasl_log(NULL, SASL_LOG_DEBUG,
		  "unable to get entry point %s: %s", adj_entryname,
		  dlerror());
#endif
	return SASL_FAIL;
    }

    return SASL_OK;
#else
    return SASL_FAIL;
#endif /* DO_DLOPEN */
}

#ifdef DO_DLOPEN

static int _sasl_plugin_load(char *plugin, void *library,
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

/* this returns the file to actually open.
 *  out should be a buffer of size PATH_MAX
 *  and may be the same as in. */

/* We'll use a static buffer for speed unless someone complains */
#define MAX_LINE 2048

static int _parse_la(const char *prefix, const char *in, char *out) 
{
    FILE *file;
    size_t length;
    char line[MAX_LINE];
    char *ntmp = NULL;

    if(!in || !out || !prefix || out == in) return SASL_BADPARAM;

    /* Set this so we can detect failure */
    *out = '\0';

    length = strlen(in);

    if (strcmp(in + (length - strlen(LA_SUFFIX)), LA_SUFFIX)) {
	if(!strcmp(in + (length - strlen(SO_SUFFIX)),SO_SUFFIX)) {
	    /* check for a .la file */
	    if (strlen(prefix) + strlen(in) + strlen(LA_SUFFIX) + 1 >= MAX_LINE)
		return SASL_BADPARAM;
	    strcpy(line, prefix);
	    strcat(line, in);
	    length = strlen(line);
	    *(line + (length - strlen(SO_SUFFIX))) = '\0';
	    strcat(line, LA_SUFFIX);
	    file = fopen(line, "r");
	    if(file) {
		/* We'll get it on the .la open */
		fclose(file);
		return SASL_FAIL;
	    }
	}
        if (strlen(prefix) + strlen(in) + 1 >= PATH_MAX)
            return SASL_BADPARAM;
	strcpy(out, prefix);
	strcat(out, in);
	return SASL_OK;
    }

    if (strlen(prefix) + strlen(in) + 1 >= MAX_LINE)
        return SASL_BADPARAM;
    strcpy(line, prefix);
    strcat(line, in);

    file = fopen(line, "r");
    if(!file) {
	_sasl_log(NULL, SASL_LOG_WARN,
		  "unable to open LA file: %s", line);
	return SASL_FAIL;
    }
    
    while(!feof(file)) {
	if(!fgets(line, MAX_LINE, file)) break;
	if(line[strlen(line) - 1] != '\n') {
	    _sasl_log(NULL, SASL_LOG_WARN,
		      "LA file has too long of a line: %s", in);
	    fclose(file);
	    return SASL_BUFOVER;
	}
	if(line[0] == '\n' || line[0] == '#') continue;
	if(!strncmp(line, "dlname=", sizeof("dlname=") - 1)) {
	    /* We found the line with the name in it */
	    char *end;
	    char *start;
	    size_t len;
	    end = strrchr(line, '\'');
	    if(!end) continue;
	    start = &line[sizeof("dlname=")-1];
	    len = strlen(start);
	    if(len > 3 && start[0] == '\'') {
		ntmp=&start[1];
		*end='\0';
		/* Do we have dlname="" ? */
		if(ntmp == end) {
		    _sasl_log(NULL, SASL_LOG_DEBUG,
			      "dlname is empty in .la file: %s", in);
		    fclose(file);
		    return SASL_FAIL;
		}
		strcpy(out, prefix);
		strcat(out, ntmp);
	    }
	    break;
	}
    }
    if(ferror(file) || feof(file)) {
	_sasl_log(NULL, SASL_LOG_WARN,
		  "Error reading .la: %s\n", in);
	fclose(file);
	return SASL_FAIL;
    }
    fclose(file);

    if(!(*out)) {
	_sasl_log(NULL, SASL_LOG_WARN,
		  "Could not find a dlname line in .la file: %s", in);
	return SASL_FAIL;
    }

    return SASL_OK;
}
#endif /* DO_DLOPEN */

/* loads a plugin library */
int _sasl_get_plugin(const char *file,
		     const sasl_callback_t *verifyfile_cb,
		     void **libraryptr)
{
#ifdef DO_DLOPEN
    int r = 0;
    int flag;
    void *library;
    lib_list_t *newhead;
    
    r = ((sasl_verifyfile_t *)(verifyfile_cb->proc))
		    (verifyfile_cb->context, file, SASL_VRFY_PLUGIN);
    if (r != SASL_OK) return r;

#ifdef RTLD_NOW
    flag = RTLD_NOW;
#else
    flag = 0;
#endif

    newhead = sasl_ALLOC(sizeof(lib_list_t));
    if(!newhead) return SASL_NOMEM;

    if (!(library = dlopen(file, flag))) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "unable to dlopen %s: %s", file, dlerror());
	sasl_FREE(newhead);
	return SASL_FAIL;
    }

    newhead->library = library;
    newhead->next = lib_list_head;
    lib_list_head = newhead;

    *libraryptr = library;
    return SASL_OK;
#else
    return SASL_FAIL;
#endif /* DO_DLOPEN */
}

/* gets the list of mechanisms */
int _sasl_load_plugins(const add_plugin_list_t *entrypoints,
		       const sasl_callback_t *getpath_cb,
		       const sasl_callback_t *verifyfile_cb)
{
    int result;
    const add_plugin_list_t *cur_ep;
#ifdef DO_DLOPEN
    char str[PATH_MAX], tmp[PATH_MAX+2], prefix[PATH_MAX+2];
				/* 1 for '/' 1 for trailing '\0' */
    char c;
    int pos;
    const char *path=NULL;
    int position;
    DIR *dp;
    struct dirent *dir;
#endif
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

    for(cur_ep = entrypoints; cur_ep->entryname; cur_ep++) {

	/* What type of plugin are we looking for? */
	if(!strcmp(cur_ep->entryname, "sasl_server_plug_init")) {
	    type = SERVER;
	    add_plugin = (add_plugin_t *)sasl_server_add_plugin;
	} else if (!strcmp(cur_ep->entryname, "sasl_client_plug_init")) {
	    type = CLIENT;
	    add_plugin = (add_plugin_t *)sasl_client_add_plugin;
	} else if (!strcmp(cur_ep->entryname, "sasl_auxprop_plug_init")) {
	    type = AUXPROP;
	    add_plugin = (add_plugin_t *)sasl_auxprop_add_plugin;
	} else if (!strcmp(cur_ep->entryname, "sasl_canonuser_init")) {
	    type = CANONUSER;
	    add_plugin = (add_plugin_t *)sasl_canonuser_add_plugin;
	} else {
	    /* What are we looking for then? */
	    return SASL_FAIL;
	}
	for (p=_sasl_static_plugins; p->type; p++) {
	    if(type == p->type)
	    	result = add_plugin(p->name, p->plug);
	}
    }

#if defined(TRY_DLOPEN_WHEN_STATIC)
    /* get the path to the plugins */
    result = ((sasl_getpath_t *)(getpath_cb->proc))(getpath_cb->context,
						    &path);
    if (result != SASL_OK) return result;
    if (! path) return SASL_FAIL;

    if (strlen(path) >= PATH_MAX) { /* no you can't buffer overrun */
	return SASL_FAIL;
    }

    position=0;
    do {
	pos=0;
	do {
	    c=path[position];
	    position++;
	    str[pos]=c;
	    pos++;
	} while ((c!=':') && (c!='=') && (c!=0));
	str[pos-1]='\0';

	strcpy(prefix,str);
	strcat(prefix,"/");

	if ((dp=opendir(str)) !=NULL) /* ignore errors */    
	{
	    while ((dir=readdir(dp)) != NULL)
	    {
		size_t length;
		void *library;
		char *c;
		char plugname[PATH_MAX];
		char name[PATH_MAX];

		length = NAMLEN(dir);
		if (length < 4) 
		    continue; /* can not possibly be what we're looking for */

		if (length + pos>=PATH_MAX) continue; /* too big */

		if (strcmp(dir->d_name + (length - strlen(SO_SUFFIX)),
			   SO_SUFFIX)
		    && strcmp(dir->d_name + (length - strlen(LA_SUFFIX)),
			   LA_SUFFIX))
		    continue;

		memcpy(name,dir->d_name,length);
		name[length]='\0';

		result = _parse_la(prefix, name, tmp);
		if(result != SASL_OK)
		    continue;
		
		/* skip "lib" and cut off suffix --
		   this only need be approximate */
		strcpy(plugname, name + 3);
		c = strchr(plugname, (int)'.');
		if(c) *c = '\0';

		result = _sasl_get_plugin(tmp, verifyfile_cb, &library);

		if(result != SASL_OK)
		    continue;

		for(cur_ep = entrypoints; cur_ep->entryname; cur_ep++) {
			_sasl_plugin_load(plugname, library, cur_ep->entryname,
					  cur_ep->add_plugin);
			/* If this fails, it's not the end of the world */
		}
	    }

	    closedir(dp);
	} else {
	    _sasl_log(NULL, SASL_LOG_DEBUG,
		      "looking for plugins in '%s', failed to open directory, error: %s",
		      str,
		      strerror(errno));
	}

    } while ((c!='=') && (c!=0));
#endif

    return SASL_OK;
}

int
_sasl_done_with_plugins(void)
{
#ifdef DO_DLOPEN
    lib_list_t *libptr, *libptr_next;
    
    for(libptr = lib_list_head; libptr; libptr = libptr_next) {
	libptr_next = libptr->next;
	if(libptr->library)
	    dlclose(libptr->library);
	sasl_FREE(libptr);
    }

    lib_list_head = NULL;
#endif /* DO_DLOPEN */
    return SASL_OK;
}
