/*
 * load the sasl plugins
 * $Id: mac_dyn_dlopen.c,v 1.3 2003/02/13 19:55:59 rjs3 Exp $
 */
/* 
 * Copyright (c) 1998-2003 Carnegie Mellon University.  All rights reserved.
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
 *      Office of Technology Transfer
 *      Carnegie Mellon University
 *      5000 Forbes Avenue
 *      Pittsburgh, PA  15213-3890
 *      (412) 268-4387, fax: (412) 268-7395
 *      tech-transfer@andrew.cmu.edu
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
#include <stdlib.h>
#include <string.h>
#include <sasl.h>
#include "saslint.h"

#include <CodeFragments.h>
#include <Errors.h>
#include <Resources.h>
#include <Strings.h>
#include <Folders.h>

#ifdef RUBBISH
#include <FSpCompat.h>
#endif

/*
 * The following data structure defines the structure of a code fragment
 * resource.  We can cast the resource to be of this type to access
 * any fields we need to see.
 */
struct CfrgHeader {
    long 	res1;
    long 	res2;
    long 	version;
    long 	res3;
    long 	res4;
    long 	filler1;
    long 	filler2;
    long 	itemCount;
    char	arrayStart;	/* Array of externalItems begins here. */
};
typedef struct CfrgHeader CfrgHeader, *CfrgHeaderPtr, **CfrgHeaderPtrHand;

/*
 * The below structure defines a cfrag item within the cfrag resource.
 */
struct CfrgItem {
    OSType 	archType;
    long 	updateLevel;
    long	currVersion;
    long	oldDefVersion;
    long	appStackSize;
    short	appSubFolder;
    char	usage;
    char	location;
    long	codeOffset;
    long	codeLength;
    long	res1;
    long	res2;
    short	itemSize;
    Str255	name;		/* This is actually variable sized. */
};
typedef struct CfrgItem CfrgItem;

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#if TARGET_API_MAC_CARBON
#define SASL_PLUGIN_DIR "\p:sasl v2:carbon:biff"
#else
#define SASL_PLUGIN_DIR "\p:sasl v2:biff"
#endif

typedef struct lib_list 
{
    struct lib_list *next;
    void *library;
} lib_list_t;

static lib_list_t *lib_list_head = NULL;

/*
 * add the passed extension
 */
int _macsasl_get_fsspec(FSSpec *fspec,
	void **libraryptr)
{
	int rc;
    CFragConnectionID connID;
    Ptr dummy;
    unsigned long offset = 0;
    unsigned long length = kCFragGoesToEOF;
    unsigned char package_name[255];
   	Str255 error_text;
   	lib_list_t *newhead;

    newhead = sasl_ALLOC(sizeof(lib_list_t));
    if(!newhead) return SASL_NOMEM;

   	package_name[0] = 0;
    rc=GetDiskFragment(fspec,offset,length,package_name,
	    kLoadCFrag,&connID,&dummy,error_text);
	if(rc!=0) {
		sasl_FREE(newhead);
		return rc;
	}

    newhead->library = (void *)connID;
    newhead->next = lib_list_head;
    lib_list_head = newhead;

    *libraryptr = (void *)connID;
    return SASL_OK;
}

int _sasl_locate_entry(void *library, const char *entryname,
		       void **entry_point) 
{
	int result;
#if TARGET_API_MAC_CARBON
    char cstr[256];
#endif
	Str255 pentry;
    CFragSymbolClass symClass;
    OSErr rc;

    if(!entryname) {
	return SASL_BADPARAM;
    }

    if(!library) {
	return SASL_BADPARAM;
    }

    if(!entry_point) {
	return SASL_BADPARAM;
    }

#if TARGET_API_MAC_CARBON
	strcpy(cstr,entryname);
    CopyCStringToPascal(cstr, pentry);
#else
	strcpy(pentry,entryname);
    c2pstr(pentry);
#endif

    rc = FindSymbol((CFragConnectionID)library,pentry,entry_point, &symClass);
    if ((rc!=noErr) || (symClass==kDataCFragSymbol))
    	return SASL_FAIL;

	return SASL_OK;
}

static int _sasl_plugin_load(char *plugin, void *library,
			     const char *entryname,
			     int (*add_plugin)(const char *, void *)) 
{
    void *entry_point;
    int result;
    
    result = _sasl_locate_entry(library, entryname, &entry_point);
    if(result == SASL_OK) {
	result = add_plugin(plugin, entry_point);
//	if(result != SASL_OK)
//	    _sasl_log(NULL, SASL_LOG_ERR,
//		      "_sasl_plugin_load failed on %s for plugin: %s\n",
//		      entryname, plugin);
    }

    return result;
}

/*
 * does the passed string a occur and the end of string b?
 */
int _macsasl_ends_in(char *a, char *b)
{
	int alen=strlen(a);
	int blen=strlen(b);
	if(blen<alen)
		return FALSE;
	return (memcmp(a,b+(blen-alen),alen)==0);
}

/*
 * scan the passed directory loading sasl extensions
 */
int _macsasl_find_extensions_in_dir(short vref,long dir_id,
	const add_plugin_list_t *entrypoints)
{
	CInfoPBRec cinfo;
	unsigned char aname[300];
	char plugname[256];
	int findex=0;
	FSSpec a_plugin;
	lib_list_t *library;
	char *c;
	const add_plugin_list_t *cur_ep;

	while(TRUE) {
		int os;
		memset(&cinfo,0,sizeof(cinfo));
		aname[0] = 0;
		cinfo.hFileInfo.ioVRefNum=vref;
		cinfo.hFileInfo.ioNamePtr=aname;
		cinfo.hFileInfo.ioFDirIndex=findex++;
		cinfo.hFileInfo.ioDirID=dir_id;
		os=PBGetCatInfo(&cinfo,FALSE);
		if(os!=0)
			return SASL_OK;
		aname[aname[0]+1] = 0;

		/* skip over non shlb files */
		if(!_macsasl_ends_in(".shlb",aname+1))
			continue;
		os=FSMakeFSSpec(vref,dir_id,aname,&a_plugin);
		if(os!=0)
			continue;

		/* skip "lib" and cut off suffix --
		   this only need be approximate */
		strcpy(plugname, aname + 1);
		c = strchr(plugname, (int)'.');
		if(c) *c = '\0';

		if (!_macsasl_get_fsspec(&a_plugin,&library))
			for(cur_ep = entrypoints; cur_ep->entryname; cur_ep++) {
				_sasl_plugin_load(plugname, library, cur_ep->entryname,
						  cur_ep->add_plugin);
				/* If this fails, it's not the end of the world */
			}
	}
	return SASL_OK;
}

/* gets the list of mechanisms */
int _sasl_load_plugins(const add_plugin_list_t *entrypoints,
			const sasl_callback_t *getpath_cb,
			const sasl_callback_t *verifyfile_cb)
{
	int rc;
	short extensions_vref;
	long extensions_dirid;
	FSSpec sasl_dir;
	/* find the extensions folder */
	rc=FindFolder(kOnSystemDisk,kExtensionFolderType,FALSE,
		&extensions_vref,&extensions_dirid);
	if(rc!=0)
		return SASL_BADPARAM;
	rc=FSMakeFSSpec(extensions_vref,extensions_dirid,SASL_PLUGIN_DIR,&sasl_dir);
	/*
	 * if a plugin named biff exits or not we really dont care
	 * if it does get rc 0 if it does not get -43 (fnfErr)
	 * if the sasl dir doesnt exist we get -120 (dirNFFErr)
	 */
	if((rc!=0)&&(rc!=fnfErr))
		return SASL_BADPARAM;
	/*
	 * now extensions_vref is volume
	 * sasl_dir.parID is dirid for sasl plugins folder
	 */
	
	return _macsasl_find_extensions_in_dir(extensions_vref,sasl_dir.parID,entrypoints);
}

int
_sasl_done_with_plugins(void)
{
    lib_list_t *libptr, *libptr_next;
    
    for(libptr = lib_list_head; libptr; libptr = libptr_next) {
	libptr_next = libptr->next;
	if(libptr->library)
	    CloseConnection((CFragConnectionID*)&libptr->library);
	sasl_FREE(libptr);
    }

    lib_list_head = NULL;

    return SASL_OK;
}
