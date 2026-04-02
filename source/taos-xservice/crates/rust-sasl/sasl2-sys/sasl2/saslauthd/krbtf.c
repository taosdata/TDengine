/* MODULE: krbtf */
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

/*
 * Dec  4, 2002 by Dave Eckhardt <davide+receptionist@cs.cmu.edu>
 * This is inspired by code which was identical in both
 * auth_krb4.c and auth_krb5.c. This code contains protection
 * against a race condition.
 */

/* PUBLIC DEPENDENCIES */
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#ifdef SASLAUTHD_THREADED /* is this really used??? */
#include <pthread.h>
#endif /* SASLAUTHD_THREADED */

#include "mechanisms.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <syslog.h>

#ifdef AUTH_KRB4
#include <auth_krb4.h>
#define WANT_KRBTF
#endif /* WANT_KRBTF */

#ifdef WANT_KRBTF

/* PRIVATE DEPENDENCIES */
/* globals */

/* privates */
static char tf_dir[] = PATH_SASLAUTHD_RUNDIR "/.tf";
static char pidstring[80];
size_t pidstring_len = 0;
/* END PRIVATE DEPENDENCIES */

#endif /* WANT_KRBTF */

/* FUNCTION: krbtf_init */

/* SYNOPSIS
 * Initialize the Kerberos IV/V ticket-file/credential-cache common code
 *
 * When possible, use Heimdal krb5's memory-only credential caches--
 * this saves a whole bunch of useless disk i/o's to create and destroy
 * a file which we don't want anybody to see anyway.
 *
 * If not, this function will create a private directory for ticket
 * files and cache getpid() for later use.  Therefore, we must be
 * called AFTER main() does whatever fork()ing it wants.
 *
 * END SYNOPSIS */

int					/* R: -1 on failure, else 0 */
krbtf_init (
  /* PARAMETERS */
  void					/* no parameters */
  /* END PARAMETERS */
  )
{
#ifdef WANT_KRBTF
    /* VARIABLES */
    int rc;				/* return code holder */
    struct stat sb;			/* stat() work area */
    /* END VARIABLES */
    authmech_t *authmech;

    if (((rc = mkdir(tf_dir, 0700)) == 0) || (errno == EEXIST)) {
	if ((rc = lstat(tf_dir, &sb)) == 0) {
	    if (sb.st_mode & S_IFLNK) {
		syslog(LOG_ERR, "krbtf_init: %s is a symbolic link", tf_dir);
		return -1;
	    }
	}
    }

    if (rc != 0) {
	syslog(LOG_ERR, "krbtf_init %s: %m", tf_dir);
	return -1;
    }

    /* cache getpid() for use in filenames */
    if ((pidstring_len = snprintf(pidstring, sizeof (pidstring), "%d", getpid())) >= sizeof (pidstring)) {
	    syslog(LOG_ERR, "krbtf_init pidstring too long(!?)");
	    return -1;
    }

    return 0;
#else /* WANT_KRBTF */
	syslog(LOG_ERR, "krbtf_init: not compiled!");
	return -1;
#endif /* WANT_KRBTF */
}

/* END FUNCTION: krbtf_init */

/* FUNCTION: krbtf_name */

/* SYNOPSIS
 * Spit a ticket-file/credentical-cache name into caller's array.
 *
 * END SYNOPSIS */

int					/* R: -1 on failure, else 0 */
krbtf_name (
  /* PARAMETERS */
  char *tfname,				/* O: where caller wants name */
  int len				/* I: available length */
  /* END PARAMETERS */
  )
{
#ifdef WANT_KRBTF
	int dir_len = sizeof (tf_dir) - 1; /* don't count the null */
	int want_len = dir_len + 1 + pidstring_len + 1;

	if (want_len > len) {
	    syslog(LOG_ERR, "krbtf_name: need room for %d bytes, got %d", want_len, len);
	    return -1;
	}

	strcpy(tfname, tf_dir);
	tfname += dir_len; len -= dir_len;

	*tfname++ = '/'; len--;

	strcpy(tfname, pidstring);

#ifdef SASLAUTHD_THREADED /* is this really used??? */
	tfname += pidstring_len;
	len -= pidstring_len;

	if (snprintf(tfname, len, "_%d", pthread_self() >= len)) {
	    syslog(LOG_ERR, "krbtf_name: no room for thread id");
	    return -1;
	}
#endif /* SASLAUTHD_THREADED */

    return 0;
#else /* WANT_KRBTF */
	syslog(LOG_ERR, "krbtf_name: not compiled!");
	return -1;
#endif /* WANT_KRBTF */
}
/* END FUNCTION: krbtf_name */

/* END MODULE: krbtf */
