/* MODULE: auth_httpform */

/* COPYRIGHT
 * Copyright (c) 2005 Pyx Engineering AG
 * Copyright (c) 1998 Messaging Direct Ltd.
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
 * Proxy authentication to a remote HTTP server.
 * END SYNOPSIS */

#include <config.h>

/* PUBLIC DEPENDENCIES */
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
#include <stdio.h>

#include "mechanisms.h"
#include "utils.h"
#include "cfile.h"
#include "globals.h"
#include "auth_httpform.h"
/* END PUBLIC DEPENDENCIES */

#ifndef MAX
#define MAX(p,q) ((p >= q) ? p : q)
#endif

#ifndef MIN
#define MIN(p,q) ((p <= q) ? p : q)
#endif

/* PRIVATE DEPENDENCIES */
static cfile config = NULL;
static const char *r_host = "localhost";  /* remote host (mech_option) */
static const char *r_port = "80";       /* remote port (mech_option) */
static const char *r_uri = NULL;        /* URI to call (mech_option) */
static const char *formdata = NULL;     /* HTML form data (mech_option) */
static struct addrinfo *ai = NULL;      /* remote host, as looked up    */
/* END PRIVATE DEPENDENCIES */

#define NETWORK_IO_TIMEOUT 30		/* network I/O timeout (seconds) */
#define RESP_LEN 1000			/* size of read response buffer  */

#define TWO_CRLF "\r\n\r\n"
#define CRLF "\r\n"
#define SPACE " "

#define HTTP_STATUS_SUCCESS "200"
#define HTTP_STATUS_NOCONTENT "204"
#define HTTP_STATUS_REFUSE "403"

/* Common failure response strings for auth_httpform() */

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
	logger(L_INFO, "auth_httpform", "unexpected signal %d", sig);
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

/* FUNCTION: url_escape */

/* SYNOPSIS
 * URL-escapes the given string 
 * 
 * Note: calling function must free memory.
 * 
 * END SYNOPSIS */
static char *url_escape(
  /* PARAMETERS */
  const char *string
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    size_t length = strlen(string);
    size_t alloc = length+50;   /* add some reserve */
    char *out;
    size_t outidx=0, inidx=0;
    /* END VARIABLES */

    out = malloc(alloc);
    if (!out)
        return NULL;

    while (inidx < length) {
    	unsigned char in = (unsigned char)string[inidx];
        if (!(in >= 'a' && in <= 'z') &&
            !(in >= 'A' && in <= 'Z') &&
            !(in >= '0' && in <= '9')) {

            /* encode it */
            if (outidx+3 > alloc) {
                /* the size grows with two, since this'll become a %XX */
                char *tmp = NULL;
                alloc *= 2;
                tmp = realloc(out, alloc);
                if (!tmp) {
                    free(out);
                    return NULL;
                } else {
                    out = tmp;
                }
            }
            
            snprintf(&out[outidx], 4, "%%%02X", in);
            outidx += 3;
        } else {
            /* just copy this */
            out[outidx++] = in;
        }

        inidx++;
    }
    out[outidx] = 0; /* terminate it */
    return out;
}

/* END FUNCTION: url_escape */

/* FUNCTION: create_post_data */

/* SYNOPSIS
 * Replace %u, %p and %r in the form data read from the config file
 * with the actual username and password.
 * 
 * Large parts of this functions have been shamelessly copied from
 * the sql_create_statement() in the sql.c plugin code
 * 
 * Note: calling function must free memory.
 * 
 * END SYNOPSIS */

static char *create_post_data(
  /* PARAMETERS */
  const char *formdata, 
  const char *user,
  const char *password,
  const char *realm
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    const char *ptr, *line_ptr;
    char *esc_user = NULL, *esc_password = NULL, *esc_realm = NULL;
    char *buf = NULL, *buf_ptr;
    int filtersize;
    int ulen, plen, rlen;
    int numpercents=0;
    int biggest;
    size_t i;
    /* END VARIABLES */

    user = esc_user = url_escape(user);
    password = esc_password = url_escape(password);
    realm = esc_realm = url_escape(realm);
    if (!user || !password || !realm) {
        logger(LOG_ERR, "auth_httpform:create_post_data", "failed to allocate memory");
        goto CLEANUP;
    }
    
    /* calculate memory needed for creating the complete query string. */
    ulen = strlen(user);
    plen = strlen(password);
    rlen = strlen(realm);

    /* what if we have multiple %foo occurrences in the input query? */
    for (i = 0; i < strlen(formdata); i++) {
        if (formdata[i] == '%') {
            numpercents++;
        }
    }
    
    /* find the biggest of ulen, plen */
    biggest = MAX(MAX(ulen, plen), rlen);
    
    /* don't forget the trailing 0x0 */
    filtersize = strlen(formdata) + 1 + (numpercents*biggest)+1;
    
    /* ok, now try to allocate a chunk of that size */
    buf = (char *) malloc(filtersize);
    
    if (!buf) {
        logger(LOG_ERR, "auth_httpform:create_post_data", "failed to allocate memory");
        goto CLEANUP;
    }
    
    buf_ptr = buf;
    line_ptr = formdata;
    
    /* replace the strings */
    while ( (ptr = strchr(line_ptr, '%')) ) {
        /* copy up to but not including the next % */
        memcpy(buf_ptr, line_ptr, ptr - line_ptr); 
        buf_ptr += ptr - line_ptr;
        ptr++;
        switch (ptr[0]) {
        case '%':
            buf_ptr[0] = '%';
            buf_ptr++;
            break;
        case 'u':
            memcpy(buf_ptr, user, ulen);
            buf_ptr += ulen;
            break;
        case 'p':
            memcpy(buf_ptr, password, plen);
            buf_ptr += plen;
            break;
        case 'r':
            memcpy(buf_ptr, realm, rlen);
            buf_ptr += rlen;
            break;
        default:
            buf_ptr[0] = '%';
            buf_ptr[1] = ptr[0];
            buf_ptr += 2;
            break;
        }
        ptr++;
        line_ptr = ptr;
    }

    /* don't forget the rest */    
    memcpy(buf_ptr, line_ptr, strlen(line_ptr)+1);

CLEANUP:
    if (esc_user) {
        memset(esc_user, 0, strlen(esc_user));
        free(esc_user);
    }
    if (esc_password) {
        memset(esc_password, 0, strlen(esc_password));
        free(esc_password);
    }
    if (esc_realm) {
        memset(esc_realm, 0, strlen(realm));
        free(esc_realm);
    }

    return buf;
}

/* END FUNCTION: create_post_data */

/* FUNCTION: build_sasl_response */

/* SYNOPSIS
 * Build a SASL response out of the HTTP response
 * 
 * Note: The returned string is malloced and will be free'd by the 
 * saslauthd core
 * 
 * END SYNOPSIS */
static char *build_sasl_response(
  /* PARAMETERS */
  const char *http_response
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    size_t length = 0;
    char *c, *http_response_code, *http_response_string;
    char *sasl_response;
    /* END VARIABLES */
    
    /* parse the response, just the first line */
    /* e.g. HTTP/1.1 200 OK */
    /* e.g. HTTP/1.1 204 No Content */
    /* e.g. HTTP/1.1 403 User unknown */
    c = strpbrk(http_response, CRLF);
    if (c != NULL) {
        *c = '\0';                      /* tie off line termination */
    }

    /* isolate the HTTP response code and string */
    http_response_code = strpbrk(http_response, SPACE) + 1;
    http_response_string = strpbrk(http_response_code, SPACE) + 1;
    *(http_response_string-1) = '\0';  /* replace space after code with 0 */

    if (!strcmp(http_response_code, HTTP_STATUS_SUCCESS) ||
        !strcmp(http_response_code, HTTP_STATUS_NOCONTENT)) {
        return strdup("OK remote authentication successful");
    }
    if (!strcmp(http_response_code, HTTP_STATUS_REFUSE)) {
        /* return the HTTP response string as the SASL response */
        length = strlen(http_response_string) + 3 + 1;
        sasl_response = malloc(length);
        if (sasl_response == NULL)
            return NULL;
            
        snprintf(sasl_response, length, "NO %s", http_response_string);
        return sasl_response;
    }
    
    logger(L_INFO, "auth_httpform", "unexpected response to auth request: %s %s",
           http_response_code, http_response_string);

    return strdup(RESP_UNEXPECTED);
}

/* END FUNCTION: build_sasl_response */

/* FUNCTION: auth_httpform_init */

/* SYNOPSIS
 * Validate the host and service names for the remote server.
 * END SYNOPSIS */

int
auth_httpform_init (
  /* PARAMETERS */
  void					/* no parameters */
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    int rc;
    char *configname = NULL;
    struct addrinfo hints;
    /* END VARIABLES */

    /* name of config file may be given with -O option */
    if (mech_option)
        configname = mech_option;
    else if (access(SASLAUTHD_CONF_FILE_DEFAULT, F_OK) == 0)
        configname = SASLAUTHD_CONF_FILE_DEFAULT;
 
    /* open and read config file */
    if (configname) {
        char complaint[1024];

        if (!(config = cfile_read(configname, complaint, sizeof (complaint)))) {
            syslog(LOG_ERR, "auth_httpform_init %s", complaint);
            return -1;
        }
    }

    if (config) {
        r_host = cfile_getstring(config, "httpform_host", r_host);
        r_port = cfile_getstring(config, "httpform_port", r_port);
        r_uri = cfile_getstring(config, "httpform_uri", r_uri);
        formdata = cfile_getstring(config, "httpform_data", formdata);
    }
    
    if (formdata == NULL || r_uri == NULL) {
        syslog(LOG_ERR, "auth_httpform_init formdata and uri must be specified");
        return -1;
    }

    /* lookup the host/port - taken from auth_rimap */
    if (ai)
        freeaddrinfo(ai);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;
    if ((rc = getaddrinfo(r_host, r_port, &hints, &ai)) != 0) {
        syslog(LOG_ERR, "auth_httpform_init: getaddrinfo %s/%s: %s",
               r_host, r_port, gai_strerror(rc));
        return -1;
     }
     
    /* Make sure we have AF_INET or AF_INET6 addresses. */
    if (ai->ai_family != AF_INET && ai->ai_family != AF_INET6) {
        syslog(LOG_ERR, "auth_httpform_init: no IP address info for %s",
               ai->ai_canonname ? ai->ai_canonname : r_host);
        freeaddrinfo(ai);
        ai = NULL;
        return -1;
    }

    return 0;
}

/* END FUNCTION: auth_httpform_init */

/* FUNCTION: auth_httpform */

/* SYNOPSIS
 * Proxy authenticate to a remote HTTP server with a form POST.
 *
 * This mechanism takes the plaintext authenticator and password, forms
 * them into an HTTP POST request. If the HTTP server responds with a 200/204
 * status code, the credentials are considered valid. If it responds with
 * a 403 HTTP status code, the credentials are considered wrong. Any other
 * HTTP status code is treated like a network error.
 */

/* XXX This should be extended to support SASL PLAIN authentication */

char *					/* R: Allocated response string */
auth_httpform (
  /* PARAMETERS */
  const char *user,			/* I: plaintext authenticator */
  const char *password,			/* I: plaintext password */
  const char *service __attribute__((unused)),
  const char *realm                    /* I: user's realm */
  /* END PARAMETERS */
  )
{
    /* VARIABLES */
    int s=-1;                           /* socket to remote auth host   */
    struct addrinfo *r;                 /* remote socket address info   */
    char *req;                          /* request, with user and pw    */
    int rc;                             /* return code scratch area     */
    char postbuf[RESP_LEN];             /* request buffer               */
    int postlen;                        /* length of post request       */
    char rbuf[RESP_LEN];                /* response read buffer         */
    char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];
    int saved_errno;
    int niflags;
    /* END VARIABLES */

    /* sanity checks */
    assert(user != NULL);
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
        syslog(LOG_WARNING, "auth_httpform: connect %s[%s]/%s: %m",
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
        syslog(LOG_WARNING, "auth_httpform: couldn't connect to %s/%s",
               ai->ai_canonname ? ai->ai_canonname : r_host, pbuf);
        return strdup("NO [ALERT] Couldn't contact remote authentication server");
    }

    /* CLAIM: we now have a TCP connection to the remote HTTP server */

    /*
     * Install noop signal handlers. These just reinstall the handler
     * and return so that we take an EINTR during network I/O.
     */
    (void) signal(SIGALRM, sig_null);
    (void) signal(SIGPIPE, sig_null);
    
    /* build the HTTP request */
    req = create_post_data(formdata, user, password, realm);
    if (req == NULL) {
        close(s);
        syslog(LOG_WARNING, "auth_httpform: create_post_data == NULL");
        return strdup(RESP_IERROR);
    }

    postlen = snprintf(postbuf, RESP_LEN-1,
              "POST %s HTTP/1.1" CRLF
              "Host: %s:%s" CRLF
              "Connection: close" CRLF
              "User-Agent: saslauthd" CRLF
              "Accept: */*" CRLF
              "Content-Type: application/x-www-form-urlencoded" CRLF
              "Content-Length: %d" TWO_CRLF
              "%s",
              r_uri, r_host, r_port, (int)strlen(req), req);

    if (flags & VERBOSE) {
        syslog(LOG_DEBUG, "auth_httpform: sending %s %s %s",
               r_host, r_uri, req);
    }
    
    /* send it */
    alarm(NETWORK_IO_TIMEOUT);
    rc = tx_rec(s, postbuf, postlen);
    alarm(0);
    
    if (rc < postlen) {
        syslog(LOG_WARNING, "auth_httpform: failed to send request");
        memset(req, 0, strlen(req));
        free(req); 
        memset(postbuf, 0, postlen);
        close(s);
        return strdup(RESP_IERROR);
    }

    /* don't need these any longer */
    memset(req, 0, strlen(req));
    free(req); 
    memset(postbuf, 0, postlen);

    /* read and parse the response */
    alarm(NETWORK_IO_TIMEOUT);
    rc = read(s, rbuf, RESP_LEN-1);
    alarm(0);
    
    close(s);                    /* we're done with the remote */

    if (rc == -1) {
        syslog(LOG_WARNING, "auth_httpform: read (response): %m");
        return strdup(RESP_IERROR);
    }

    rc = MIN(rc, RESP_LEN - 1);  /* don't write past rbuf */
    rbuf[rc] = '\0';             /* make sure str-funcs find null */

    if (flags & VERBOSE) {
        syslog(LOG_DEBUG, "auth_httpform: [%s] %s", user, rbuf);
    }

    return build_sasl_response(rbuf);
}

/* END FUNCTION: auth_httpform */

/* END MODULE: auth_httpform */
