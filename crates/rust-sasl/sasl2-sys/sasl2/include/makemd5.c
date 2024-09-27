/* creates the md5global.h file. 
 *  Derived from KTH kerberos library bits.c program
 * Tim Martin 
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
 * Copyright (c) 1997, 1998 Kungliga Tekniska Högskolan
 * (Royal Institute of Technology, Stockholm, Sweden). 
 * All rights reserved. 
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions 
 * are met: 
 *
 * 1. Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in the 
 *    documentation and/or other materials provided with the distribution. 
 *
 * 3. All advertising materials mentioning features or use of this software 
 *    must display the following acknowledgement: 
 *      This product includes software developed by Kungliga Tekniska 
 *      Högskolan and its contributors. 
 *
 * 4. Neither the name of the Institute nor the names of its contributors 
 *    may be used to endorse or promote products derived from this software 
 *    without specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS 
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF 
 * SUCH DAMAGE. 
 */



#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>


static void
my_strupr(char *s)
{
    char *p = s;
    while(*p){
	if(islower((int) *p))
	    *p = toupper((int) *p);
	p++;
    }	
}


#define BITSIZE(TYPE)						\
{								\
    int b = 0; TYPE x = 1, zero = 0; char *pre = "U";		\
    char tmp[128] = {0}, tmp2[128] = {0};			\
    while(x){ x <<= 1; b++; if(x < zero) pre=""; }		\
    if(b >= len){						\
        int tabs;						\
	snprintf(tmp, 127, "%sINT%d" , pre, len/8);	    	\
	snprintf(tmp2, 127, "typedef %s %s;", #TYPE, tmp);	\
	my_strupr(tmp);						\
	tabs = 5 - strlen(tmp2) / 8;				\
        fprintf(f, "%s", tmp2);					\
	while(tabs-- > 0) fprintf(f, "\t");			\
	fprintf(f, "/* %2d bits */\n", b);			\
        return;                                                 \
    }								\
}

static void
try_signed(FILE *f, int len)
{
    BITSIZE(signed char);
    BITSIZE(short);
    BITSIZE(int);
    BITSIZE(long);
#ifdef HAVE_LONG_LONG
    BITSIZE(long long);
#endif
    fprintf(f, "/* There is no %d bit type */\n", len);
}

static void
try_unsigned(FILE *f, int len)
{
    BITSIZE(unsigned char);
    BITSIZE(unsigned short);
    BITSIZE(unsigned int);
    BITSIZE(unsigned long);
#ifdef HAVE_LONG_LONG
    BITSIZE(unsigned long long);
#endif
    fprintf(f, "/* There is no %d bit type */\n", len);
}

static int print_pre(FILE *f)
{
  fprintf(f,
	  "/* GLOBAL.H - RSAREF types and constants\n"
	  " */\n"
          "#ifndef MD5GLOBAL_H\n"
          "#define MD5GLOBAL_H\n"
	  "\n"
	  "/* PROTOTYPES should be set to one if and only if the compiler supports\n"
	  "  function argument prototyping.\n"
	  "The following makes PROTOTYPES default to 0 if it has not already\n"
	  "  been defined with C compiler flags.\n"
	  " */\n"
	  "#ifndef PROTOTYPES\n"
	  "#define PROTOTYPES 0\n"
	  "#endif\n"
	  "\n"
	  "/* POINTER defines a generic pointer type */\n"
	  "typedef unsigned char *POINTER;\n"
	  "\n"
	  );
  return 1;
}

static int print_post(FILE *f)
{
  fprintf(f, "\n"
	  "/* PROTO_LIST is defined depending on how PROTOTYPES is defined above.\n"
	  "If using PROTOTYPES, then PROTO_LIST returns the list, otherwise it\n"
	  "returns an empty list.\n"
	  "*/\n"
	  "#if PROTOTYPES\n"
	  "#define PROTO_LIST(list) list\n"
	  "#else\n"
	  "#define PROTO_LIST(list) ()\n"
	  "#endif\n"
	  "\n"
	  "#endif /* MD5GLOBAL_H */\n\n"
	  );

  return 1;
}


int main(int argc, char **argv)
{
  FILE *f;
  char *fn, *hb;
    
  if(argc < 2){
    fn = "bits.h";
    hb = "__BITS_H__";
    f = stdout;
  } else {
    char *p;
    fn = argv[1];
    hb = malloc(strlen(fn) + 5);
    sprintf(hb, "__%s__", fn);
    for(p = hb; *p; p++){
      if(!isalnum((int) *p))
	*p = '_';
    }
    f = fopen(argv[1], "w");
    if (f == NULL)
      return 1;
  }

  print_pre(f);

#ifndef HAVE_INT8_T
    try_signed (f, 8);
#endif /* HAVE_INT8_T */
#ifndef HAVE_INT16_T
    try_signed (f, 16);
#endif /* HAVE_INT16_T */
#ifndef HAVE_INT32_T
    try_signed (f, 32);
#endif /* HAVE_INT32_T */
#ifndef HAVE_INT64_T
    try_signed (f, 64);
#endif /* HAVE_INT64_T */

#ifndef HAVE_U_INT8_T
    try_unsigned (f, 8);
#endif /* HAVE_INT8_T */
#ifndef HAVE_U_INT16_T
    try_unsigned (f, 16);
#endif /* HAVE_U_INT16_T */
#ifndef HAVE_U_INT32_T
    try_unsigned (f, 32);
#endif /* HAVE_U_INT32_T */
#ifndef HAVE_U_INT64_T
    try_unsigned (f, 64);
#endif /* HAVE_U_INT64_T */

    print_post(f);
  
    fclose(f);

    return 0;  
}
