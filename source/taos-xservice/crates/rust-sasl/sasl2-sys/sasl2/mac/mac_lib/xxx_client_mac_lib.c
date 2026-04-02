/* $Id: xxx_client_mac_lib.c,v 1.3 2003/02/13 19:55:59 rjs3 Exp $
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
 * routines used by the sasl test programs and not provided
 * by a mac.  see also xxx_mac_lib.c for routines needed by
 * the sasl library and not supplied by the system runtime
 */

#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdio.h>
 
#include <config.h>
#include <netinet/in.h>

char *__progname="mac";

struct hostent *gethostbyname(const char *hnam)
{
	static struct hostent result;
	int bytes[4];
	int i;
	unsigned int ip=0;
	if(sscanf(hnam,"%d.%d.%d.%d",bytes,bytes+1,bytes+2,bytes+3)!=4)
		return 0;
	for(i=0;i<4;i++) {
		ip<<=8;
		ip|=(bytes[i]&0x0ff);
	}
	memcpy(result.h_addr,&ip,4);
	return &result;
}

/*
 * ala perl chomp
 */
static void xxy_chomp(char *s,const char stop_here)
{
	char ch;
	while((ch= (*s++))!=0)
		if(ch==stop_here) {
			s[-1]=0;
			return;
		}
}

char* getpass(const char *prompt)
{
	const int max_buf=200;
	char *buf=malloc(max_buf);
	if(buf==0)
		return 0;
	memset(buf,0,max_buf);  /* not likely to be a performance issue eheh */
	printf("%s",prompt);
	fgets(buf,max_buf-1,stdin);
	xxy_chomp(buf,'\n');
	return buf;
}

#ifdef TARGET_API_MAC_CARBON
char *strdup(const char *str)
{
 	if(str==0)
 		return 0;
 	{
 		const int len=strlen(str);
 		char *result=malloc(len+1);
 		strcpy(result,str);
 		return result;
  	}
 	
}
#endif
