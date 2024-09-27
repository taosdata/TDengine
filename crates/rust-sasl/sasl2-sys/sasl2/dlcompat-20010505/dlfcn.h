/*
 * This file was modified by Christoph Pfisterer <cp@chrisp.de>
 * on Sat, May 5 2001. See the file "ChangeLog" for details of what
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

#ifdef __cplusplus
extern "C" {
#endif

extern void * dlopen(
    const char *path,
    int mode);
extern void * dlsym(
    void * handle,
    const char *symbol);
extern const char * dlerror(
    void);
extern int dlclose(
    void * handle);

#define RTLD_LAZY	0x1
#define RTLD_NOW	0x2
#define RTLD_LOCAL	0x4
#define RTLD_GLOBAL	0x8
#define RTLD_NOLOAD	0x10
#define RTLD_SHARED	0x20	/* not used, the default */
#define RTLD_UNSHARED	0x40
#define RTLD_NODELETE	0x80
#define RTLD_LAZY_UNDEF	0x100

#ifdef __cplusplus
}
#endif
