/*
 * Mar  8, 2000 by Hajimu UMEMOTO <ume@mahoroba.org>
 *
 * This module is besed on ssh-1.2.27-IPv6-1.5 written by
 * KIKUCHI Takahiro <kick@kyoto.wide.ad.jp>
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
/*
 * fake library for ssh
 *
 * This file includes getnameinfo().
 * These funtions are defined in rfc2133.
 *
 * But these functions are not implemented correctly. The minimum subset
 * is implemented for ssh use only. For exapmle, this routine assumes
 * that ai_family is AF_INET. Don't use it for another purpose.
 * 
 * In the case not using 'configure --enable-ipv6', this getnameinfo.c
 * will be used if you have broken getnameinfo or no getnameinfo.
 */

#include "saslauthd.h"
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>

int
getnameinfo(const struct sockaddr *sa, socklen_t salen __attribute__((unused)),
	    char *host, size_t hostlen, char *serv, size_t servlen, int flags)
{
    struct sockaddr_in *sin = (struct sockaddr_in *)sa;
    struct hostent *hp;
    char tmpserv[16];
  
    if (serv) {
	sprintf(tmpserv, "%d", ntohs(sin->sin_port));
	if (strlen(tmpserv) > servlen)
	    return EAI_MEMORY;
	else
	    strcpy(serv, tmpserv);
    }
    if (host) {
	if (flags & NI_NUMERICHOST) {
	    if (strlen(inet_ntoa(sin->sin_addr)) >= hostlen)
		return EAI_MEMORY;
	    else {
		strcpy(host, inet_ntoa(sin->sin_addr));
		return 0;
	    }
	} else {
	    hp = gethostbyaddr((char *)&sin->sin_addr,
			       sizeof(struct in_addr), AF_INET);
	    if (hp)
		if (strlen(hp->h_name) >= hostlen)
		    return EAI_MEMORY;
		else {
		    strcpy(host, hp->h_name);
		    return 0;
		}
	    else
		return EAI_NODATA;
	}
    }
    
    return 0;
}
