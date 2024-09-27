/*
 * prompt for a command line
 */
/* $Id: parse_cmd_line.c,v 1.3 2003/02/13 19:55:59 rjs3 Exp $
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <parse_cmd_line.h>

static char *skip_blanks(char *s)
{
	while(isspace(*s))
		s++;
	return s;
}

static void chomp(char *dst,char ch)
{
	dst=strchr(dst,ch);
	if(dst!=0)
		*dst=0;
}

int parse_cmd_line(int max_argc,char **argv,int line_size,char *line)
{
	int argc=1;
	memset(line,0,line_size);
	fprintf(stdout,"cmd>");
	fflush(stdout);
	fgets(line,line_size-1,stdin);
	*argv++="prg";
	chomp(line,'\n');
	max_argc-=2;
	while(line[0]!=0) {
		line=skip_blanks(line);
		if(line[0]==0)
			break;
		if(argc>=max_argc)
			break;
		*argv++=line;
		argc++;
		line=strchr(line,' ');
		if(line==0)
			break;
		*line++=0;
	}
	*argv=0;
	return argc;
}
