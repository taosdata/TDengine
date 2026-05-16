/* $Id: mac_krb_lib1.c,v 1.3 2003/02/13 19:55:57 rjs3 Exp $
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
/*
 * library to emulate unix kerberos on a macintosh
 */
#include <config.h>
#include <krb.h>
#include <extra_krb.h>
#include <kcglue_krb.h>

#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#include <stdio.h>

/*
 * given a hostname return the kerberos realm
 * NOT thread safe....
 */
char *krb_realmofhost(const char *s)
{
	s=strchr(s,'.');
	if(s==0)
		return "ANDREW.CMU.EDU";
	return (char *)(s+1);
}

/*
 * return the default instance to use for a given hostname
 * NOT thread safe... but then neathoer is the real kerberos one
 */
char *krb_get_phost(const char *alias)
{
#define MAX_HOST_LEN (512)
    static char instance[MAX_HOST_LEN];
    char *dst=instance;
    int remaining=MAX_HOST_LEN-10;
    while(remaining-->0) {
    	char ch= *alias++;
    	if(ch==0) break;
    	if(isupper(ch))
    		ch=tolower(ch);
    	if(ch=='.')
    		break;
    	*dst++=ch;
    }
    *dst=0;
    return instance;
}
