/*	$NetBSD: getsubopt.c,v 1.4 1998/02/03 18:44:15 perry Exp $	*/

/*-
 * Copyright (c) 1990, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *	This product includes software developed by the University of
 *	California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#if ((!defined(WIN32))&&(!defined(macintosh)))
#include <sys/cdefs.h>
#endif /* WIN32 */
#if defined(LIBC_SCCS) && !defined(lint)
#if 0
static char sccsid[] = "@(#)getsubopt.c	8.1 (Berkeley) 6/4/93";
#else
__RCSID("$NetBSD: getsubopt.c,v 1.4 1998/02/03 18:44:15 perry Exp $");
#endif
#endif /* LIBC_SCCS and not lint */

#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNISTD_H */
#if (defined(WIN32)||(defined(macintosh)))
#include "sasl.h"
LIBSASL_API int getsubopt(char **optionp, char * const *tokens, char **valuep);
#endif /* WIN32 */
/*
 * The SVID interface to getsubopt provides no way of figuring out which
 * part of the suboptions list wasn't matched.  This makes error messages
 * tricky...  The extern variable suboptarg is a pointer to the token
 * which didn't match.
 */
char *suboptarg;

int
getsubopt(optionp, tokens, valuep)
	char **optionp, **valuep;
	char * const *tokens;
{
	int cnt;
	char *p;

	suboptarg = *valuep = NULL;

	if (!optionp || !*optionp)
		return(-1);

	/* skip leading white-space, commas */
	for (p = *optionp; *p && (*p == ',' || *p == ' ' || *p == '\t'); ++p);

	if (!*p) {
		*optionp = p;
		return(-1);
	}

	/* save the start of the token, and skip the rest of the token. */
	for (suboptarg = p;
	    *++p && *p != ',' && *p != '=' && *p != ' ' && *p != '\t';);

	if (*p) {
		/*
		 * If there's an equals sign, set the value pointer, and
		 * skip over the value part of the token.  Terminate the
		 * token.
		 */
		if (*p == '=') {
			*p = '\0';
			for (*valuep = ++p;
			    *p && *p != ',' && *p != ' ' && *p != '\t'; ++p);
			if (*p) 
				*p++ = '\0';
		} else
			*p++ = '\0';
		/* Skip any whitespace or commas after this token. */
		for (; *p && (*p == ',' || *p == ' ' || *p == '\t'); ++p);
	}

	/* set optionp for next round. */
	*optionp = p;

	for (cnt = 0; *tokens; ++tokens, ++cnt)
		if (!strcmp(suboptarg, *tokens))
			return(cnt);
	return(-1);
}
