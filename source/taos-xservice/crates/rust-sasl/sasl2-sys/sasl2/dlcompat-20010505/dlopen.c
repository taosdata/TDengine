/*
 * This file was modified by Christoph Pfisterer <cp@chrisp.de>
 * on Tue, Jan 23 2001. See the file "ChangeLog" for details of what
 * was changed.
 *
 *
 * Copyright (c) 1999 Apple Computer, Inc. All rights reserved.
 *
 * @APPLE_LICENSE_HEADER_START@
 * 
 * Portions Copyright (c) 1999 Apple Computer, Inc.  All Rights
 * Reserved.  This file contains Original Code and/or Modifications of
 * Original Code as defined in and that are subject to the Apple Public
 * Source License Version 1.1 (the "License").  You may not use this file
 * except in compliance with the License.  Please obtain a copy of the
 * License at http://www.apple.com/publicsource and read it before using
 * this file.
 * 
 * The Original Code and all software distributed under the License are
 * distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, EITHER
 * EXPRESS OR IMPLIED, AND APPLE HEREBY DISCLAIMS ALL SUCH WARRANTIES,
 * INCLUDING WITHOUT LIMITATION, ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE OR NON- INFRINGEMENT.  Please see the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * @APPLE_LICENSE_HEADER_END@
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include "mach-o/dyld.h"
#include "dlfcn.h"

/*
 * debugging macros
 */
#if DEBUG > 0
#define DEBUG_PRINT(format) fprintf(stderr,(format));fflush(stderr)
#define DEBUG_PRINT1(format,arg1) fprintf(stderr,(format),(arg1));\
  fflush(stderr)
#define DEBUG_PRINT2(format,arg1,arg2) fprintf(stderr,(format),\
  (arg1),(arg2));fflush(stderr)
#define DEBUG_PRINT3(format,arg1,arg2,arg3) fprintf(stderr,(format),\
  (arg1),(arg2),(arg3));fflush(stderr)
#else
#define DEBUG_PRINT(format) /**/
#define DEBUG_PRINT1(format,arg1) /**/
#define DEBUG_PRINT2(format,arg1,arg2) /**/
#define DEBUG_PRINT3(format,arg1,arg2,arg3) /**/
#undef DEBUG
#endif

/*
 * The structure of a dlopen() handle.
 */
struct dlopen_handle {
    dev_t dev;		/* the path's device and inode number from stat(2) */
    ino_t ino; 
    int dlopen_mode;	/* current dlopen mode for this handle */
    int dlopen_count;	/* number of times dlopen() called on this handle */
    NSModule module;	/* the NSModule returned by NSLinkModule() */
    struct dlopen_handle *prev;
    struct dlopen_handle *next;
};
static struct dlopen_handle *dlopen_handles = NULL;
static const struct dlopen_handle main_program_handle = {NULL};
static char *dlerror_pointer = NULL;

/*
 * NSMakePrivateModulePublic() is not part of the public dyld API so we define
 * it here.  The internal dyld function pointer for
 * __dyld_NSMakePrivateModulePublic is returned so thats all that maters to get
 * the functionality need to implement the dlopen() interfaces.
 */
static
int
NSMakePrivateModulePublic(
NSModule module)
{
    static int (*p)(NSModule module) = NULL;

	if(p == NULL)
	    _dyld_func_lookup("__dyld_NSMakePrivateModulePublic",
			      (unsigned long *)&p);
	if(p == NULL){
#ifdef DEBUG
	    printf("_dyld_func_lookup of __dyld_NSMakePrivateModulePublic "
		   "failed\n");
#endif
	    return(FALSE);
	}
	return(p(module));
}

/*
 * helper routine: search for a named module in various locations
 */
static
int
_dl_search_paths(
const char *filename,
char *pathbuf,
struct stat *stat_buf)
{
    const char *pathspec;
    const char *element;
    const char *p;
    char *q;
    char *pathbuf_end;
    const char *envvars[] = {
        "$DYLD_LIBRARY_PATH",
        "$LD_LIBRARY_PATH",
        "/usr/lib:/lib",
        NULL };
    int envvar_index;

        pathbuf_end = pathbuf + PATH_MAX - 8;

	for(envvar_index = 0; envvars[envvar_index]; envvar_index++){
	    if(envvars[envvar_index][0] == '$'){
	        pathspec = getenv(envvars[envvar_index]+1);
	    }
	    else {
	        pathspec = envvars[envvar_index];
	    }

	    if(pathspec != NULL){
	        element = pathspec;
		while(*element){
	            /* extract path list element */
		    p = element;
		    q = pathbuf;
		    while(*p && *p != ':' && q < pathbuf_end)
                        *q++ = *p++;
		    if(q == pathbuf){  /* empty element */
		        if(*p){
		            element = p+1;
			    continue;
			}
			break;
		    }
		    if (*p){
		        element = p+1;
		    }
		    else{
		        element = p;  /* this terminates the loop */
		    }

		    /* add slash if neccessary */
		    if(*(q-1) != '/' && q < pathbuf_end){
		        *q++ = '/';
		    }

		    /* append module name */
		    p = filename;
		    while(*p && q < pathbuf_end) *q++ = *p++;
		    *q++ = 0;

		    if(q >= pathbuf_end){
		        /* maybe add an error message here */
		        break;
		    }

		    if(stat(pathbuf, stat_buf) == 0){
		        return 0;
		    }
		}
	    }
	}

	/* we have searched everywhere, now we give up */
	return -1;
}

/*
 * dlopen() the MacOS X version of the FreeBSD dlopen() interface.
 */
void *
dlopen(
const char *path,
int mode)
{
    const char *module_path;
    void *retval;
    struct stat stat_buf;
    NSObjectFileImage objectFileImage;
    NSObjectFileImageReturnCode ofile_result_code;
    NSModule module;
    struct dlopen_handle *p;
    unsigned long options;
    NSSymbol NSSymbol;
    void (*init)(void);
    char pathbuf[PATH_MAX];

        DEBUG_PRINT2("libdl: dlopen(%s,0x%x) -> ", path, (unsigned int)mode);

	dlerror_pointer = NULL;
	/*
	 * A NULL path is to indicate the caller wants a handle for the
	 * main program.
 	 */
	if(path == NULL){
	    retval = (void *)&main_program_handle;
	    DEBUG_PRINT1("main / %p\n", retval);
	    return(retval);
	}

	/* see if the path exists and if so get the device and inode number */
	if(stat(path, &stat_buf) == -1){
	    dlerror_pointer = strerror(errno);

	    if(path[0] == '/'){
	        DEBUG_PRINT1("ERROR (stat): %s\n", dlerror_pointer);
	        return(NULL);
	    }

	    /* search for the module in various places */
	    if(_dl_search_paths(path, pathbuf, &stat_buf)){
	        /* dlerror_pointer is unmodified */
	        DEBUG_PRINT1("ERROR (stat): %s\n", dlerror_pointer);
	        return(NULL);
	    }
	    DEBUG_PRINT1("found %s -> ", pathbuf);
	    module_path = pathbuf;
	    dlerror_pointer = NULL;
	}
	else{
	    module_path = path;
	}

	/*
	 * If we don't want an unshared handle see if we already have a handle
	 * for this path.
	 */
	if((mode & RTLD_UNSHARED) != RTLD_UNSHARED){
	    p = dlopen_handles;
	    while(p != NULL){
		if(p->dev == stat_buf.st_dev && p->ino == stat_buf.st_ino){
		    /* skip unshared handles */
		    if((p->dlopen_mode & RTLD_UNSHARED) == RTLD_UNSHARED)
			continue;
		    /*
		     * We have already created a handle for this path.  The
		     * caller might be trying to promote an RTLD_LOCAL handle
		     * to a RTLD_GLOBAL.  Or just looking it up with
		     * RTLD_NOLOAD.
		     */
		    if((p->dlopen_mode & RTLD_LOCAL) == RTLD_LOCAL &&
		       (mode & RTLD_GLOBAL) == RTLD_GLOBAL){
			/* promote the handle */
			if(NSMakePrivateModulePublic(p->module) == TRUE){
			    p->dlopen_mode &= ~RTLD_LOCAL;
			    p->dlopen_mode |= RTLD_GLOBAL;
			    p->dlopen_count++;
			    DEBUG_PRINT1("%p\n", p);
			    return(p);
			}
			else{
			    dlerror_pointer = "can't promote handle from "
					      "RTLD_LOCAL to RTLD_GLOBAL";
			    DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
			    return(NULL);
			}
		    }
		    p->dlopen_count++;
		    DEBUG_PRINT1("%p\n", p);
		    return(p);
		}
		p = p->next;
	    }
	}
	
	/*
	 * We do not have a handle for this path if we were just trying to
	 * look it up return NULL to indicate we don't have it.
	 */
	if((mode & RTLD_NOLOAD) == RTLD_NOLOAD){
	    dlerror_pointer = "no existing handle for path RTLD_NOLOAD test";
	    DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
	    return(NULL);
	}

	/* try to create an object file image from this path */
	ofile_result_code = NSCreateObjectFileImageFromFile(module_path,
							    &objectFileImage);
	if(ofile_result_code != NSObjectFileImageSuccess){
	    switch(ofile_result_code){
	    case NSObjectFileImageFailure:
		dlerror_pointer = "object file setup failure";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    case NSObjectFileImageInappropriateFile:
		dlerror_pointer = "not a Mach-O MH_BUNDLE file type";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    case NSObjectFileImageArch:
		dlerror_pointer = "no object for this architecture";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    case NSObjectFileImageFormat:
		dlerror_pointer = "bad object file format";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    case NSObjectFileImageAccess:
		dlerror_pointer = "can't read object file";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    default:
		dlerror_pointer = "unknown error from "
				  "NSCreateObjectFileImageFromFile()";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    }
	}

	/* try to link in this object file image */
	options = NSLINKMODULE_OPTION_PRIVATE;
	if((mode & RTLD_NOW) == RTLD_NOW)
	    options |= NSLINKMODULE_OPTION_BINDNOW;
	module = NSLinkModule(objectFileImage, module_path, options);
	NSDestroyObjectFileImage(objectFileImage) ;
	if(module == NULL){
	    dlerror_pointer = "NSLinkModule() failed for dlopen()";
	    DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
	    return(NULL);
	}

	/*
	 * If the handle is to be global promote the handle.  It is done this
	 * way to avoid multiply defined symbols.
	 */
	if((mode & RTLD_GLOBAL) == RTLD_GLOBAL){
	    if(NSMakePrivateModulePublic(module) == FALSE){
		dlerror_pointer = "can't promote handle from RTLD_LOCAL to "
				  "RTLD_GLOBAL";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    }
	}

	p = malloc(sizeof(struct dlopen_handle));
	if(p == NULL){
	    dlerror_pointer = "can't allocate memory for the dlopen handle";
	    DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
	    return(NULL);
	}

	/* fill in the handle */
	p->dev = stat_buf.st_dev;
        p->ino = stat_buf.st_ino;
	if(mode & RTLD_GLOBAL)
	    p->dlopen_mode = RTLD_GLOBAL;
	else
	    p->dlopen_mode = RTLD_LOCAL;
	p->dlopen_mode |= (mode & RTLD_UNSHARED) |
			  (mode & RTLD_NODELETE) |
			  (mode & RTLD_LAZY_UNDEF);
	p->dlopen_count = 1;
	p->module = module;
	p->prev = NULL;
	p->next = dlopen_handles;
	if(dlopen_handles != NULL)
	    dlopen_handles->prev = p;
	dlopen_handles = p;

	/* call the init function if one exists */
	NSSymbol = NSLookupSymbolInModule(p->module, "__init");
	if(NSSymbol != NULL){
	    init = NSAddressOfSymbol(NSSymbol);
	    init();
	}
	
	DEBUG_PRINT1("%p\n", p);
	return(p);
}

/*
 * dlsym() the MacOS X version of the FreeBSD dlopen() interface.
 */
void *
dlsym(
void * handle,
const char *symbol)
{
    struct dlopen_handle *dlopen_handle, *p;
    NSSymbol NSSymbol;
    void *address;

        DEBUG_PRINT2("libdl: dlsym(%p,%s) -> ", handle, symbol);

	dlopen_handle = (struct dlopen_handle *)handle;

	/*
	 * If this is the handle for the main program do a global lookup.
	 */
	if(dlopen_handle == (struct dlopen_handle *)&main_program_handle){
	    if(NSIsSymbolNameDefined(symbol) == TRUE){
		NSSymbol = NSLookupAndBindSymbol(symbol);
		address = NSAddressOfSymbol(NSSymbol);
		dlerror_pointer = NULL;
		DEBUG_PRINT1("%p\n", address);
		return(address);
	    }
	    else{
		dlerror_pointer = "symbol not found";
		DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		return(NULL);
	    }
	}

	/*
	 * Find this handle and do a lookup in just this module.
	 */
	p = dlopen_handles;
	while(p != NULL){
	    if(dlopen_handle == p){
		NSSymbol = NSLookupSymbolInModule(p->module, symbol);
		if(NSSymbol != NULL){
		    address = NSAddressOfSymbol(NSSymbol);
		    dlerror_pointer = NULL;
		    DEBUG_PRINT1("%p\n", address);
		    return(address);
		}
		else{
		    dlerror_pointer = "symbol not found";
		    DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		    return(NULL);
		}
	    }
	    p = p->next;
	}

	dlerror_pointer = "bad handle passed to dlsym()";
	DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
	return(NULL);
}

/*
 * dlerror() the MacOS X version of the FreeBSD dlopen() interface.
 */
const char *
dlerror(
void)
{
    const char *p;

	p = (const char *)dlerror_pointer;
	dlerror_pointer = NULL;
	return(p);
}

/*
 * dlclose() the MacOS X version of the FreeBSD dlopen() interface.
 */
int
dlclose(
void * handle)
{
    struct dlopen_handle *p, *q;
    unsigned long options;
    NSSymbol NSSymbol;
    void (*fini)(void);

        DEBUG_PRINT1("libdl: dlclose(%p) -> ", handle);

	dlerror_pointer = NULL;
	q = (struct dlopen_handle *)handle;
	p = dlopen_handles;
	while(p != NULL){
	    if(p == q){
		/* if the dlopen() count is not zero we are done */
		p->dlopen_count--;
		if(p->dlopen_count != 0){
		    DEBUG_PRINT("OK");
		    return(0);
		}

		/* call the fini function if one exists */
		NSSymbol = NSLookupSymbolInModule(p->module, "__fini");
		if(NSSymbol != NULL){
		    fini = NSAddressOfSymbol(NSSymbol);
		    fini();
		}

		/* unlink the module for this handle */
		options = 0;
		if(p->dlopen_mode & RTLD_NODELETE)
		    options |= NSUNLINKMODULE_OPTION_KEEP_MEMORY_MAPPED;
		if(p->dlopen_mode & RTLD_LAZY_UNDEF)
		    options |= NSUNLINKMODULE_OPTION_RESET_LAZY_REFERENCES;
		if(NSUnLinkModule(p->module, options) == FALSE){
		    dlerror_pointer = "NSUnLinkModule() failed for dlclose()";
		    DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
		    return(-1);
		}
		if(p->prev != NULL)
		    p->prev->next = p->next;
		if(p->next != NULL)
		    p->next->prev = p->prev;
		if(dlopen_handles == p)
		    dlopen_handles = p->next;
		free(p);
		DEBUG_PRINT("OK");
		return(0);
	    }
	    p = p->next;
	}
	dlerror_pointer = "invalid handle passed to dlclose()";
	DEBUG_PRINT1("ERROR: %s\n", dlerror_pointer);
	return(-1);
}
