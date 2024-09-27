/* Simple Config file API
 * Dave Eckhardt
 * Rob Siemborski
 * Tim Martin (originally in Cyrus distribution)
 */
/* 
 * Copyright (c) 2001-2016 Carnegie Mellon University.  All rights reserved.
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

/* cfile_read() has a clumsy error reporting path
 * so that it doesn't depend on any particular package's
 * return code space.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "cfile.h"

struct cf_keyval {
    char *key;
    char *value;
};

struct cfile {
    struct cf_keyval *kvlist;
    int n_kv;
};

#define CONFIGLISTGROWSIZE 100
#define BIG_ENOUGH 4096

cfile cfile_read(const char *filename, char *complaint, int complaint_len)
{
    FILE *infile;
    int lineno = 0;
    int alloced = 0;
    char buf[BIG_ENOUGH];
    char *p, *key;
    struct cfile *cf;

	if (complaint)
      complaint[0] = '\0';

    if (!(cf = malloc(sizeof (*cf)))) {
      /* then strdup() will probably fail, sigh */
      if (complaint)
        snprintf(complaint, complaint_len, "cfile_read: no memory");
      return 0;
    }

    cf->n_kv = 0;
    cf->kvlist = 0;

    infile = fopen(filename, "r");
    if (!infile) {
      if (complaint)
        snprintf(complaint, complaint_len, "cfile_read: cannot open %s", filename);
      cfile_free(cf);
      return 0;
    }
    
    while (fgets(buf, sizeof(buf), infile)) {
	lineno++;

	if (buf[strlen(buf)-1] == '\n') buf[strlen(buf)-1] = '\0';
	for (p = buf; *p && isspace((int) *p); p++);
	if (!*p || *p == '#') continue;

	key = p;
	while (*p && (isalnum((int) *p) || *p == '-' || *p == '_')) {
	    if (isupper((int) *p)) *p = tolower(*p);
	    p++;
	}
	if (*p != ':') {
	  if (complaint)
	    snprintf(complaint, complaint_len, "%s: line %d: no colon separator", filename, lineno);
	  cfile_free(cf);
	  fclose(infile);
	  return 0;
	}
	*p++ = '\0';

	while (*p && isspace((int) *p)) p++;
	
	if (!*p) {
	  if (complaint)
	    snprintf(complaint, complaint_len, "%s: line %d: keyword %s: no value", filename, lineno, key);
	  cfile_free(cf);
	  fclose(infile);
	  return 0;
	}

	if (cf->n_kv == alloced) {
	    alloced += CONFIGLISTGROWSIZE;
	    cf->kvlist=realloc((char *)cf->kvlist, 
				    alloced * sizeof(struct cf_keyval));
	    if (cf->kvlist==NULL) {
	      if (complaint)
	        snprintf(complaint, complaint_len, "cfile_read: no memory");
	      cfile_free(cf);
	      fclose(infile);
	      return 0;
	    }
	}

	if (!(cf->kvlist[cf->n_kv].key = strdup(key)) ||
	    !(cf->kvlist[cf->n_kv].value = strdup(p))) {
	      if (complaint)
	        snprintf(complaint, complaint_len, "cfile_read: no memory");
	      /* maybe one strdup() worked */
	      if (cf->kvlist[cf->n_kv].key)
	          free(cf->kvlist[cf->n_kv].key);
	      cfile_free(cf);
	      fclose(infile);
	      return 0;
	}

	cf->n_kv++;
    }
    fclose(infile);

    return cf;
}

const char *cfile_getstring(cfile cf,const char *key,const char *def)
{
    int opt;

    for (opt = 0; opt < cf->n_kv; opt++) {
	if (*key == cf->kvlist[opt].key[0] &&
	    !strcmp(key, cf->kvlist[opt].key))
	  return cf->kvlist[opt].value;
    }
    return def;
}

int cfile_getint(cfile cf,const char *key,int def)
{
    const char *val = cfile_getstring(cf, key, (char *)0);

    if (!val) return def;
    if (!isdigit((int) *val) && (*val != '-' || !isdigit((int) val[1]))) return def;
    return atoi(val);
}

int cfile_getswitch(cfile cf,const char *key,int def)
{
    const char *val = cfile_getstring(cf, key, (char *)0);

    if (!val) return def;

    if (*val == '0' || *val == 'n' ||
	(*val == 'o' && val[1] == 'f') || *val == 'f') {
	return 0;
    }
    else if (*val == '1' || *val == 'y' ||
	     (*val == 'o' && val[1] == 'n') || *val == 't') {
	return 1;
    }
    return def;
}

void cfile_free(cfile cf)
{
    int opt;

    if (cf->kvlist) {
	for (opt = 0; opt < cf->n_kv; opt++) {
	    if (cf->kvlist[opt].key)
	      free(cf->kvlist[opt].key);
	    if (cf->kvlist[opt].value)
	      free(cf->kvlist[opt].value);
	}
	free(cf->kvlist);
    }
    free(cf);
}
