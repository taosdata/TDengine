
/* MODULE: auth_rimap */

/* COPYRIGHT
 * Copyright (c) 1998 Messaging Direct Ltd.
 * Copyright (c) 2013 Sebastian Pipping <sebastian@pipping.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY MESSAGING DIRECT LTD. ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL MESSAGING DIRECT LTD. OR
 * ITS EMPLOYEES OR AGENTS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 * 
 * Copyright 1998, 1999 Carnegie Mellon University
 * 
 *                       All Rights Reserved
 * 
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose and without fee is hereby granted,
 * provided that the above copyright notice appear in all copies and that
 * both that copyright notice and this permission notice appear in
 * supporting documentation, and that the name of Carnegie Mellon
 * University not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission.
 * 
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE FOR
 * ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * END COPYRIGHT */

/* SYNOPSIS
 * Proxy authentication to a remote IMAP (or IMSP) server.
 * END SYNOPSIS */

/* PUBLIC DEPENDENCIES */
#include "mechanisms.h"

#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#ifdef _AIX
# include <strings.h>
#endif /* _AIX */
#include <syslog.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <netdb.h>
#if HAVE_SYS_TIME_H
#  include <sys/time.h>
#endif
# include <time.h>

#include "auth_rimap.h"
#include "utils.h"
#include "globals.h"
/* END PUBLIC DEPENDENCIES */

/* PRIVATE DEPENDENCIES */
static const char *r_host = NULL;       /* remote hostname (mech_option) */
static struct addrinfo *ai = NULL;	/* remote authentication host    */
/* END PRIVATE DEPENDENCIES */

#define DEFAULT_REMOTE_SERVICE "imap"	/* getservbyname() name for remote
					   service we connect to.	 */
#define TAG "saslauthd"			/* IMAP command tag */
#define LOGIN_CMD (TAG " LOGIN ")	/* IMAP login command (with tag) */
#define LOGOUT_CMD (TAG " LOGOUT")	/* IMAP logout command (with tag) */
#define LOGIN_REPLY_GOOD (TAG " OK")	/* Expected IMAP login reply, good edition (with tag) */
#define LOGIN_REPLY_BAD (TAG " NO")	/* Expected IMAP login reply, bad edition (with tag) */
#define LOGIN_REPLY_CAP "* CAPABILITY"	/* Expected IMAP login reply, capabilities edition */
#define NETWORK_IO_TIMEOUT 30		/* network I/O timeout (seconds) */
#define RESP_LEN 1000			/* size of read response buffer  */

/* Common failure response strings for auth_rimap() */

#define RESP_IERROR	"NO [ALERT] saslauthd internal error"
#define RESP_UNAVAILABLE "NO [ALERT] The remote authentication server is currently unavailable"
#define RESP_UNEXPECTED	"NO [ALERT] Unexpected response from remote authentication server"

/* FUNCTION: sig_null */

/* SYNOPSIS
 * Catch and ignore a signal.
 * END SYNOPSIS */

static RETSIGTYPE				/* R: OS dependent */
sig_null (
  /* PARAMETERS */
  int sig					/* I: signal being caught */
  /* END PARAMETERS */
  )
{

    switch (sig) {
	
      case SIGALRM:
	signal(SIGALRM, sig_null);
	break;

      case SIGPIPE:
	signal(SIGPIPE, sig_null);
	break;

      default:
	syslog(LOG_WARNING, "auth_rimap: unexpected signal %d", sig);
	break;
    }
#ifdef __APPLE__
    return;
#else /* __APPLE__ */
# if RETSIGTYPE == void
    return;
# else /* RETSIGTYPE */
    return 0;
# endif /* RETSIGTYPE */
#endif /* __APPLE__ */
}

/* END FUNCTION: sig_null */

/* FUNCTION: qstring */

/* SYNOPSIS
 * Quote a string for transmission over the IMAP protocol.
 * END SYNOPSIS */

static char *				/* R: the quoted string		*/
qstring (
  /* PARAMETERS */
  const char *s				/* I: string to quote		*/
  /* END PARAMETERS */
  )
{
    char *c;				/* pointer to returned string   */
    register const char *p1;		/* scratch pointers		*/
    register char *p2;			/* scratch pointers             */
    int len;				/* length of array to malloc    */

    /*
     * Ugh, we have to escape quoted-specials ...
     */
    len = 2*strlen(s) + 3;		/* assume all chars are quoted-special */
    c = malloc(len);
    if (c == NULL) {
	return NULL;
    }
    p1 = s;
    p2 = c;
    *p2++ = '"';
    while (*p1) {
	if (*p1 == '"' || *p1 == '\\') {
	    *p2++ = '\\';		/* escape the quoted-special */
	}
	*p2++ = *p1++;
    }
    strcpy(p2, "\"");
    return c;
}

/* END FUNCTION: qstring */

/* FUNCTION: auth_rimap_init */

/* SYNOPSIS
 * Validate the host and service names for the remote server.
 * END SYNOPSIS */

int
auth_rimap_init (
  /* PARAMETERS */
  void					/* no parameters */
  /* END PARAMETERS */
  )
{

    /* VARIABLES */
    struct addrinfo hints;
    int err;
    char *c;				/* scratch pointer               */
    /* END VARIABLES */

    if (mech_option == NULL) {
	syslog(LOG_ERR, "rimap_init: no hostname specified");
	return -1;
    } else {
	r_host = mech_option;
    }

    /* Determine the port number to connect to.
     *
     * r_host has already been initialized to the hostname and optional port
     * port name to connect to. The format of the string is:
     *
     *		hostname
     * or
     *		hostname/port
     */

    c = strchr(r_host, '/');		/* look for optional service  */
    
    if (c != NULL) {
	*c++ = '\0';			/* tie off hostname and point */
					/* to service string          */
    } else {
	c = DEFAULT_REMOTE_SERVICE;
    }

    if (ai)
	freeaddrinfo(ai);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;
    if ((err = getaddrinfo(r_host, c, &hints, &ai)) != 0) {
	syslog(LOG_ERR, "auth_rimap_init: getaddrinfo %s/%s: %s",
	       r_host, c, gai_strerror(err));
	return -1;
     }
    /* Make sure we have AF_INET or AF_INET6 addresses. */
    if (ai->ai_family != AF_INET && ai->ai_family != AF_INET6) {
	syslog(LOG_ERR, "auth_rimap_init: no IP address info for %s",
	       ai->ai_canonname ? ai->ai_canonname : r_host);
	freeaddrinfo(ai);
	ai = NULL;
	return -1;
    }

    return 0;
}

/* END FUNCTION: auth_rimap_init */

typedef enum _t_login_status {
	LOGIN_STATUS_UNKNOWN,

	LOGIN_STATUS_ACCEPTED,
	LOGIN_STATUS_REJECTED,
	LOGIN_STATUS_MALFORMED
} t_login_status;

/* FUNCTION: warn_malformed_imap_login_reply */
void
warn_malformed_imap_login_reply(
		/* PARAMETERS */
		const char * server_reply  /* I: plaintext server reply */
		/* END PARAMETERS */
		)
{
	syslog(LOG_WARNING, "auth_rimap: unexpected response to auth request: %s", server_reply);
}

/* END FUNCTION: warn_malformed_imap_login_reply */

/* FUNCTION: process_login_reply */

/* SYNOPSIS
 * Classify IMAP server reply into accepted, rejected or malformed.
 * END SYNOPSIS */

t_login_status
process_login_reply(
		/* PARAMETERS */
		char * server_reply,  /* I/O: plaintext server reply */
		const char * login    /* I  : plaintext authenticator */
		/* END PARAMETERS */
		)
{
	/* VARIABLES */
	t_login_status res = LOGIN_STATUS_UNKNOWN;
	char * line_first = server_reply;
	char * line_after_last;
	/* END VARIABLES */

	for (;;) {
		/* find line boundary */
		line_after_last = strpbrk(line_first, "\x0a\x0d");
		if (line_after_last == NULL) {
			warn_malformed_imap_login_reply(line_first);
			return LOGIN_STATUS_MALFORMED;
		}

		/* handle single line */
		{
			/* terminate line (reverted later) */
			const char backup = line_after_last[0];
			line_after_last[0] = '\0';

			/* classify current line */
			if (strncmp(line_first, LOGIN_REPLY_GOOD, sizeof(LOGIN_REPLY_GOOD) - 1) == 0) {
				res = LOGIN_STATUS_ACCEPTED;
			} else if (strncmp(line_first, LOGIN_REPLY_BAD, sizeof(LOGIN_REPLY_BAD) - 1) == 0) {
				res = LOGIN_STATUS_REJECTED;
			} else if (strncmp(line_first, LOGIN_REPLY_CAP, sizeof(LOGIN_REPLY_CAP) - 1) == 0) {
				/* keep looking for ".. OK" or ".. NO" */
			} else {
				res = LOGIN_STATUS_MALFORMED;
			}

			/* report current line */
			if (res == LOGIN_STATUS_MALFORMED) {
				warn_malformed_imap_login_reply(line_first);
			} else if (flags & VERBOSE) {
				syslog(LOG_DEBUG, "auth_rimap: [%s] %s", login, line_first);
			}

			/* revert termination */
			line_after_last[0] = backup;
		}

		/* are we done? */
		if (res != LOGIN_STATUS_UNKNOWN) {
			return res;
		}

		/* forward to next line */
		while ((line_after_last[0] == '\x0a')
				|| (line_after_last[0] == '\x0d')) {
			line_after_last++;
		}

		/* no more lines? */
		if (line_after_last[0] == '\0') {
			warn_malformed_imap_login_reply("");
			return LOGIN_STATUS_MALFORMED;
		}

		/* prepare for next round */
		line_first = line_after_last;
	}

	assert(! "cannot be reached");
}

/* END FUNCTION: process_login_reply */


#ifndef HAVE_MEMMEM
static void *memmem(
		const void *big, size_t big_len,
		const void *little, size_t little_len)
{
	const char *bp = (const char *)big;
	const char *lp = (const char *)little;
	size_t l;

	if (big_len < little_len || little_len == 0 || big_len == 0)
		return NULL;

    	size_t len_count = 0;
	while (len_count < big_len) {
		for (l = 0; l < little_len; l++) {
			if (bp[l] != lp[l])
				break;
		}
		if (l == little_len)
			return (void *)bp;
		bp++;
		len_count++;
	}

	return NULL;
}
#endif

static int read_response(int s, char *rbuf, int buflen, const char *tag)
{
    int rc = 0;

    do {
        /* check if there is more to read */
        fd_set         perm;
        int            fds, ret;
        struct timeval timeout;

        FD_ZERO(&perm);
        FD_SET(s, &perm);
        fds = s +1;

        timeout.tv_sec  = NETWORK_IO_TIMEOUT;
        timeout.tv_usec = 0;
        ret = select (fds, &perm, NULL, NULL, &timeout );
        if ( ret<=0 ) {
            rc = ret;
            break;
        }
        if ( FD_ISSET(s, &perm) ) {
            ret = read(s, rbuf+rc, buflen-rc);
            if ( ret<=0 ) {
                rc = ret;
                break;
            } else {
                rc += ret;
            }
        }
    } while (rc < buflen &&
             ( rbuf[rc-1] != '\n' || !memmem(rbuf, rc, tag, strlen(tag)) ));

    return rc;
}

/* FUNCTION: auth_rimap */

/* SYNOPSIS
 * Proxy authenticate to a remote IMAP server.
 *
 * This mechanism takes the plaintext authenticator and password, forms
 * them into an IMAP LOGIN command, then attempts to authenticate to
 * a remote IMAP server using those values. If the remote authentication
 * succeeds the credentials are considered valid.
 *
 * NOTE: since IMSP uses the same form of LOGIN command as IMAP does,
 * this driver will also work with IMSP servers.
 */

/* XXX This should be extended to support SASL PLAIN authentication */

char *					/* R: Allocated response string */
auth_rimap (
  /* PARAMETERS */
  const char *login,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm __attribute__((unused))
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    int	s=-1;				/* socket to remote auth host   */
    struct addrinfo *r;			/* remote socket address info   */
    struct iovec iov[5];		/* for sending IMAP commands    */
    char *qlogin;			/* pointer to "quoted" login    */
    char *qpass;			/* pointer to "quoted" password */
    char *c;				/* scratch pointer              */
    int rc, rc2;			/* return code scratch area     */
    char rbuf[RESP_LEN];		/* response read buffer         */
    char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];
    int saved_errno;
    int niflags;
    t_login_status login_status = LOGIN_STATUS_MALFORMED;
    /* END VARIABLES */

    /* sanity checks */
    assert(login != NULL);
    assert(password != NULL);

    /*establish connection to remote */
    for (r = ai; r; r = r->ai_next) {
	s = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
	if (s < 0)
	    continue;
	if (connect(s, r->ai_addr, r->ai_addrlen) >= 0)
	    break;
	close(s);
	s = -1;
	saved_errno = errno;
	niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
	if (r->ai_family == AF_INET6)
	    niflags |= NI_WITHSCOPEID;
#endif
	if (getnameinfo(r->ai_addr, r->ai_addrlen, hbuf, sizeof(hbuf),
			pbuf, sizeof(pbuf), niflags) != 0) {
	    strlcpy(hbuf, "unknown", sizeof(hbuf));
	    strlcpy(pbuf, "unknown", sizeof(pbuf));
	}
	errno = saved_errno;
	syslog(LOG_WARNING, "auth_rimap: connect %s[%s]/%s: %m",
	       ai->ai_canonname ? ai->ai_canonname : r_host, hbuf, pbuf);
    }
    if (s < 0) {
        if (!ai) {
            syslog(LOG_WARNING, "auth_httpform: no address given");
            return strdup("NO [ALERT] No address given");
        }

	if (getnameinfo(ai->ai_addr, ai->ai_addrlen, NULL, 0,
			pbuf, sizeof(pbuf), NI_NUMERICSERV) != 0)
	    strlcpy(pbuf, "unknown", sizeof(pbuf));
	syslog(LOG_WARNING, "auth_rimap: couldn't connect to %s/%s",
	       ai->ai_canonname ? ai->ai_canonname : r_host, pbuf);
	return strdup("NO [ALERT] Couldn't contact remote authentication server");
    }

    /* CLAIM: we now have a TCP connection to the remote IMAP server */

    /*
     * Install noop signal handlers. These just reinstall the handler
     * and return so that we take an EINTR during network I/O.
     */
    (void) signal(SIGALRM, sig_null);
    (void) signal(SIGPIPE, sig_null);
    
    /* read and parse the IMAP banner */

    rc = read_response(s, rbuf, RESP_LEN, "*");
    if (rc == -1) {
	syslog(LOG_WARNING, "auth_rimap: read (banner): %m");
	(void) close(s);
	return strdup("NO [ALERT] error synchronizing with remote authentication server");
    }
    else if (rc >= RESP_LEN) {
	syslog(LOG_WARNING, "auth_rimap: read (banner): buffer overflow");
	(void) close(s);
	return strdup("NO [ALERT] error synchronizing with remote authentication server");
    }

    rbuf[rc] = '\0';			/* tie off response */
    c = strpbrk(rbuf, "\r\n");
    if (c != NULL) {
	*c = '\0';			/* tie off line termination */
    }

    if (!strncmp(rbuf, "* NO", sizeof("* NO")-1)) {
	(void) close(s);
	return strdup(RESP_UNAVAILABLE);
    }
    if (!strncmp(rbuf, "* BYE", sizeof("* BYE")-1)) {
	(void) close(s);
	return strdup(RESP_UNAVAILABLE);
    }
    if (strncmp(rbuf, "* OK", sizeof("* OK")-1)) {
	syslog(LOG_WARNING,
	       "auth_rimap: unexpected response during initial handshake: %s",
	       rbuf);
	(void) close(s);
	return strdup(RESP_UNEXPECTED);
    }
    
    /* build the LOGIN command */

    qlogin = qstring(login);		/* quote login */
    qpass = qstring(password);		/* quote password */
    if (qlogin == NULL) {
	if (qpass != NULL) {
	    memset(qpass, 0, strlen(qpass));
	    free(qpass);
	}
	(void) close(s);
	syslog(LOG_WARNING, "auth_rimap: qstring(login) == NULL");
	return strdup(RESP_IERROR);
    }
    if (qpass == NULL) {
	if (qlogin != NULL) {
	    memset(qlogin, 0, strlen(qlogin));
	    free(qlogin);
	}
	(void) close(s);
	syslog(LOG_WARNING, "auth_rimap: qstring(password) == NULL");
	return strdup(RESP_IERROR);
    }

    iov[0].iov_base = LOGIN_CMD;
    iov[0].iov_len  = sizeof(LOGIN_CMD) - 1;
    iov[1].iov_base = qlogin;
    iov[1].iov_len  = strlen(qlogin);
    iov[2].iov_base = " ";
    iov[2].iov_len  = sizeof(" ") - 1;
    iov[3].iov_base = qpass;
    iov[3].iov_len  = strlen(qpass);
    iov[4].iov_base = "\r\n";
    iov[4].iov_len  = sizeof("\r\n") - 1;

    if (flags & VERBOSE) {
	syslog(LOG_DEBUG, "auth_rimap: sending %s%s %s",
	       LOGIN_CMD, qlogin, qpass);
    }
    alarm(NETWORK_IO_TIMEOUT);
    rc = retry_writev(s, iov, 5);
    alarm(0);

    /* don't need these any longer */
    memset(qlogin, 0, strlen(qlogin));
    free(qlogin);
    memset(qpass, 0, strlen(qpass));
    free(qpass);

    if (rc == -1) {
        syslog(LOG_WARNING, "auth_rimap: writev %s: %m", LOGIN_CMD);
	(void)close(s);
	return strdup(RESP_IERROR);
    }

    /* read and parse the LOGIN response */

    rc = read_response(s, rbuf, RESP_LEN, TAG);
    if (rc == -1) {
	(void) close(s);
	syslog(LOG_WARNING, "auth_rimap: read (response): %m");
	return strdup(RESP_IERROR);
    }
    else if (rc >= RESP_LEN) {
	(void) close(s);
	syslog(LOG_WARNING, "auth_rimap: read (response): buffer overflow");
	return strdup(RESP_IERROR);
    }

    /* build the LOGOUT command */

    iov[0].iov_base = LOGOUT_CMD;
    iov[0].iov_len  = sizeof(LOGOUT_CMD) - 1;
    iov[1].iov_base = "\r\n";
    iov[1].iov_len  = sizeof("\r\n") - 1;

    if (flags & VERBOSE) {
	syslog(LOG_DEBUG, "auth_rimap: sending %s", LOGOUT_CMD);
    }
    alarm(NETWORK_IO_TIMEOUT);
    rc2 = retry_writev(s, iov, 2);
    alarm(0);
    if (rc2 == -1) {
      syslog(LOG_WARNING, "auth_rimap: writev %s: %m", LOGOUT_CMD);
    }

    (void) close(s);			/* we're done with the remote */

    rbuf[rc] = '\0';			/* tie off response */
    login_status = process_login_reply(rbuf, login);

    if (login_status == LOGIN_STATUS_ACCEPTED) {
	return strdup("OK remote authentication successful");
    }
    if (login_status == LOGIN_STATUS_REJECTED) {
	return strdup("NO remote server rejected your credentials");
    }
    return strdup(RESP_UNEXPECTED);
    
}

/* END FUNCTION: auth_rimap */

/* END MODULE: auth_rimap */
