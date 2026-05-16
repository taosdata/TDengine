/*
 * internaly sasl or its test programs use some functions which are not availible
 * on the macintosh.  these have common names like strdrup gethostname etc.  defining
 * them as routines could make conflicts with clients of the library.  in config.h
 * we macro define such names to start with xxx_.  The implementation for them is
 * here.  The xxx_ is in hopes of not conflicting with a name in client program.
 */
/* $Id: xxx_mac_lib.c,v 1.3 2003/02/13 19:55:59 rjs3 Exp $
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

 #include <string.h>
 #include <stdlib.h>
 #include <ctype.h>
 
 #include <config.h>
// #include <netinet/in.h>

/*
 * return the smaller of two integers
 */
static int xxy_min(int a,int b)
{
	if(a<b)
		return a;
	return b;
}

static int limit_strcpy(char *dest,const char *src,int len)
{
	int slen=strlen(src);
	if(len<1)
		return 0;
	slen=xxy_min(slen,len-1);
	if(slen>0)
		memcpy(dest,src,slen);
	dest[slen]=0;
	return slen;	
}

int strcpy_truncate(char *dest,char *src,int len)
{
	return limit_strcpy(dest,src,len);
}

int gethostname(char *dest,int destlen)
{
	limit_strcpy(dest,"localhost",destlen);
	return 0;
}

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
 
int strncasecmp(const char *s1,const char *s2,int len)
{
	while(len-- >0) {
		char c1= *s1++;
		char c2= *s2++;
		if((c1==0)&&(c2==0))
			return 0;
		if(c1==0)
			return -1;
		if(c2==0)
			return 1;
		/* last ansi spec i read tolower was undefined for non uppercase chars
		 * but it works in most implementations
		 */
		if(isupper(c1))
			c1=tolower(c1);
		if(isupper(c2))
			c2=tolower(c2);
		if(c1<c2)
			return -1;
		if(c1>c2)
			return 1;
	}
	return 1;
}

int strcasecmp(const char *s1,const char *s2)
{
	while(1) {
		char c1= *s1++;
		char c2= *s2++;
		if((c1==0)&&(c2==0))
			return 0;
		if(c1==0)
			return -1;
		if(c2==0)
			return 1;
		/* last ansi spec i read tolower was undefined for non uppercase chars
		 * but it works in most implementations
		 */
		if(isupper(c1))
			c1=tolower(c1);
		if(isupper(c2))
			c2=tolower(c2);
		if(c1<c2)
			return -1;
		if(c1>c2)
			return 1;
	}
}

int inet_aton(const char *cp, struct in_addr *inp)
{
	char *cptr1, *cptr2, *cptr3;
	long u;
	char cptr0[256];
	strcpy(cptr0, cp);

	if (!(cptr1 = strchr(cptr0, '.'))) return 0;
	*cptr1++ = 0;
	if (!(cptr2 = strchr(cptr1, '.'))) return 0;
	*cptr2++ = 0;
	if (!(cptr3 = strchr(cptr2, '.'))) return 0;
	*cptr3++ = 0;
	if (!*cptr3) return 0;

	u = ((atoi(cptr0) << 8 + atoi(cptr1)) << 8 + atoi(cptr2)) << 8 + atoi(cptr3);
	inp->s_addr = htonl(u);
	return 1;
}
