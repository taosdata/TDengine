/* sasldblistusers.c -- list users in sasldb
 * Rob Siemborski
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

#include <config.h>

#include <stdio.h>
#include <stdlib.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <sasl.h>
#include "../sasldb/sasldb.h"

#ifdef WIN32
#include <saslutil.h>
#if defined(__MINGW64_VERSION_MAJOR)
#include <getopt.h>
#else
__declspec(dllimport) char *optarg;
__declspec(dllimport) int optind;
#endif
#endif

/* Cheating to make the utils work out right */
LIBSASL_VAR const sasl_utils_t *sasl_global_utils;

char *sasldb_path = SASL_DB_PATH;
const char *progname = NULL;

int good_getopt(void *context __attribute__((unused)), 
		const char *plugin_name __attribute__((unused)), 
		const char *option,
		const char **result,
		unsigned *len)
{
    if (sasldb_path && !strcmp(option, "sasldb_path")) {
	*result = sasldb_path;
	if (len)
	    *len = (unsigned) strlen(sasldb_path);
	return SASL_OK;
    }

    return SASL_FAIL;
}

static struct sasl_callback goodsasl_cb[] = {
    { SASL_CB_GETOPT, (sasl_callback_ft)&good_getopt, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};

int main(int argc, char **argv)
{
    int c;
    int result;
    sasl_conn_t *conn;
    int bad_option = 0;
    int display_usage = 0;
    const char *sasl_implementation;
    int libsasl_version;
    int libsasl_major;
    int libsasl_minor;
    int libsasl_step;

    if (! argv[0])
       progname = "sasldblistusers2";
    else {
       progname = strrchr(argv[0], HIER_DELIMITER);
       if (progname)
           progname++;
       else
           progname = argv[0];
    }

    /* A single parameter not starting with "-" denotes sasldb to use */
    if ((argc == 2) && argv[1][0] != '-') {
	sasldb_path = argv[1];
	goto START_WORK;
    }

    while ((c = getopt(argc, argv, "vf:h?")) != EOF) {
       switch (c) {
         case 'f':
	   sasldb_path = optarg;
	   break;
         case 'h':
           bad_option = 0;
            display_usage = 1;
           break;
	 case 'v':
	   sasl_version (&sasl_implementation, &libsasl_version);
	   libsasl_major = libsasl_version >> 24;
	   libsasl_minor = (libsasl_version >> 16) & 0xFF;
	   libsasl_step = libsasl_version & 0xFFFF;

	   (void)fprintf(stderr, "\nThis product includes software developed by Computing Services\n"
		"at Carnegie Mellon University (http://www.cmu.edu/computing/).\n\n"
		"Built against SASL API version %u.%u.%u\n"
		"LibSasl version %u.%u.%u by \"%s\"\n",
		SASL_VERSION_MAJOR, SASL_VERSION_MINOR, SASL_VERSION_STEP,
		libsasl_major, libsasl_minor, libsasl_step, sasl_implementation);
	   exit(0);
	   break;

         default:
           bad_option = 1;
            display_usage = 1;
            break;
       }
    }

    if (optind != argc)
       display_usage = 1;

    if (display_usage) {
           fprintf(stderr,
             "\nThis product includes software developed by Computing Services\n"
             "at Carnegie Mellon University (http://www.cmu.edu/computing/).\n\n");

           fprintf(stderr, "%s: usage: %s [-v] [[-f] sasldb]\n",
		   progname, progname);
           fprintf(stderr, "\t-f sasldb\tuse given file as sasldb\n"
                           "\t-v\tprint version numbers and exit\n");
       if (bad_option) {
           fprintf(stderr, "Unrecognized command line option\n");
       }
       return 1;
    }
 
START_WORK:
    result = sasl_server_init(goodsasl_cb, "sasldblistusers");
    if(result != SASL_OK) {
	fprintf(stderr, "Couldn't initialize server API\n");
	return 1;
    }
    
    result = sasl_server_new("sasldb",
			     "localhost",
			     NULL,
			     NULL,
			     NULL,
			     NULL,
			     0,
			     &conn);

    if(_sasl_check_db(sasl_global_utils, conn) != SASL_OK) {
	fprintf(stderr, "check_db unsuccessful\n");
	return 1;
    }

    if(_sasldb_listusers (sasl_global_utils, conn, NULL, NULL) != SASL_OK) {
	fprintf(stderr, "listusers failed\n");
    }

    sasl_dispose(&conn);
    sasl_done();

    return 0;
}
