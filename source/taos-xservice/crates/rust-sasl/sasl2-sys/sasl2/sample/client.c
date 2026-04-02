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
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <assert.h>

#include <sasl.h>
#include <saslplug.h>

#include "common.h"

/* remove \r\n at end of the line */
static void chop(char *s)
{
    char *p;

    assert(s);
    p = s + strlen(s) - 1;
    if (p[0] == '\n') {
	*p-- = '\0';
    }
    if (p >= s && p[0] == '\r') {
	*p-- = '\0';
    }
}

static int getrealm(void *context __attribute__((unused)), 
		    int id,
		    const char **availrealms,
		    const char **result)
{
    static char buf[1024];

    /* paranoia check */
    if (id != SASL_CB_GETREALM) return SASL_BADPARAM;
    if (!result) return SASL_BADPARAM;

    printf("please choose a realm (available:");
    while (*availrealms) {
	printf(" %s", *availrealms);
	availrealms++;
    }
    printf("): ");

    fgets(buf, sizeof buf, stdin);
    chop(buf);
    *result = buf;
  
    return SASL_OK;
}

static int simple(void *context __attribute__((unused)),
		  int id,
		  const char **result,
		  unsigned *len)
{
    static char bufU[1024];
    static char bufA[1024];
    char *b;

    /* paranoia check */
    if (! result)
	return SASL_BADPARAM;

    switch (id) {
    case SASL_CB_USER:
	printf("please enter an authorization id: ");
        b = bufU;
	break;
    case SASL_CB_AUTHNAME:
	printf("please enter an authentication id: ");
        b = bufA;
	break;
    default:
	return SASL_BADPARAM;
    }

    fgets(b, 1024, stdin);
    chop(b);
    *result = b;
    if (len) *len = strlen(b);
  
    return SASL_OK;
}

#ifndef HAVE_GETPASSPHRASE
static char *
getpassphrase(const char *prompt)
{
  return getpass(prompt);
}
#endif /* ! HAVE_GETPASSPHRASE */

static int
getsecret(sasl_conn_t *conn,
	  void *context __attribute__((unused)),
	  int id,
	  sasl_secret_t **psecret)
{
    char *password;
    size_t len;
    static sasl_secret_t *x;

    /* paranoia check */
    if (! conn || ! psecret || id != SASL_CB_PASS)
	return SASL_BADPARAM;

    password = getpassphrase("Password: ");
    if (! password)
	return SASL_FAIL;

    len = strlen(password);

    x = (sasl_secret_t *) realloc(x, sizeof(sasl_secret_t) + len);
  
    if (!x) {
	memset(password, 0, len);
	return SASL_NOMEM;
    }

    x->len = len;
    strcpy((char *)x->data, password);
    memset(password, 0, len);
    
    *psecret = x;
    return SASL_OK;
}


/* callbacks we support */
static sasl_callback_t callbacks[] = {
  {
    SASL_CB_GETREALM, (sasl_callback_ft)&getrealm, NULL
  }, {
    SASL_CB_USER, (sasl_callback_ft)&simple, NULL
  }, {
    SASL_CB_AUTHNAME, (sasl_callback_ft)&simple, NULL
  }, {
    SASL_CB_PASS, (sasl_callback_ft)&getsecret, NULL
  }, {
    SASL_CB_LIST_END, NULL, NULL
  }
};

int getconn(const char *host, const char *port)
{
    struct addrinfo hints, *ai, *r;
    int err, sock = -1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((err = getaddrinfo(host, port, &hints, &ai)) != 0) {
	fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
	exit(EX_UNAVAILABLE);
    }

    for (r = ai; r; r = r->ai_next) {
	sock = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
	if (sock < 0)
	    continue;
	if (connect(sock, r->ai_addr, r->ai_addrlen) >= 0)
	    break;
	close(sock);
	sock = -1;
    }

    freeaddrinfo(ai);
    if (sock < 0) {
	perror("connect");
	exit(EX_UNAVAILABLE);
    }

    return sock;
}

char *mech;

int mysasl_negotiate(FILE *in, FILE *out, sasl_conn_t *conn)
{
    char buf[8192];
    const char *data;
    const char *chosenmech;
    int len;
    int r, c;

    /* get the capability list */
    dprintf(0, "receiving capability list... ");
    len = recv_string(in, buf, sizeof buf);
    dprintf(0, "%s\n", buf);

    if (mech) {
	/* make sure that 'mech' appears in 'buf' */
	if (!strstr(buf, mech)) {
	    printf("server doesn't offer mandatory mech '%s'\n", mech);
	    return -1;
	}
    } else {
	mech = buf;
    }

    r = sasl_client_start(conn, mech, NULL, &data, (unsigned int *) &len, &chosenmech);
    if (r != SASL_OK && r != SASL_CONTINUE) {
	saslerr(r, "starting SASL negotiation");
	printf("\n%s\n", sasl_errdetail(conn));
	return -1;
    }
    
    dprintf(1, "using mechanism %s\n", chosenmech);

    /* we send up to 3 strings;
       the mechanism chosen, the presence of initial response,
       and optionally the initial response */
    send_string(out, chosenmech, strlen(chosenmech));
    if(data) {
	send_string(out, "Y", 1);
	send_string(out, data, len);
    } else {
	send_string(out, "N", 1);
    }

    for (;;) {
	dprintf(2, "waiting for server reply...\n");

	c = fgetc(in);
	switch (c) {
	case 'O':
	    goto done_ok;

	case 'N':
	    goto done_no;

	case 'C': /* continue authentication */
	    break;

	default:
	    printf("bad protocol from server (%c %x)\n", c, c);
	    return -1;
	}
	len = recv_string(in, buf, sizeof buf);

	r = sasl_client_step(conn, buf, len, NULL, &data, (unsigned int *) &len);
	if (r != SASL_OK && r != SASL_CONTINUE) {
	    saslerr(r, "performing SASL negotiation");
	    printf("\n%s\n", sasl_errdetail(conn));
	    return -1;
	}

	if (data) {
	    dprintf(2, "sending response length %d...\n", len);
	    send_string(out, data, len);
	} else {
	    dprintf(2, "sending null response...\n");
	    send_string(out, "", 0);
	}
    }

 done_ok:
    printf("successful authentication\n");
    return 0;

 done_no:
    printf("authentication failed\n");
    return -1;
}

void usage(void)
{
    fprintf(stderr, "usage: client [-c|-C] [-p port] [-s service] [-m mech] host\n");
    exit(EX_USAGE);
}

int main(int argc, char *argv[])
{
    int c;
    char *host = "localhost";
    char *port = "12345";
    char localaddr[NI_MAXHOST + NI_MAXSERV],
	remoteaddr[NI_MAXHOST + NI_MAXSERV];
    char *service = "rcmd";
    char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];
    int r;
    sasl_conn_t *conn;
    FILE *in, *out;
    int fd;
    int salen;
    int niflags, error;
    struct sockaddr_storage local_ip, remote_ip;
    int cb_flag = 0;
    sasl_channel_binding_t cb;

    while ((c = getopt(argc, argv, "Ccp:s:m:")) != EOF) {
	switch(c) {
	case 'C':
	    cb_flag = 2;    /* channel bindings are critical */
	    break;

	case 'c':
	    cb_flag = 1;    /* channel bindings are optional */
	    break;

	case 'p':
	    port = optarg;
	    break;

	case 's':
	    service = optarg;
	    break;

	case 'm':
	    mech = optarg;
	    break;

	default:
	    usage();
	    break;
	}
    }

    if (optind > argc - 1) {
	usage();
    }
    if (optind == argc - 1) {
	host = argv[optind];
    }

    /* initialize the sasl library */
    r = sasl_client_init(callbacks);
    if (r != SASL_OK) saslfail(r, "initializing libsasl");

    /* connect to remote server */
    fd = getconn(host, port);

    /* set ip addresses */
    salen = sizeof(local_ip);
    if (getsockname(fd, (struct sockaddr *)&local_ip, (unsigned int*) &salen) < 0) {
	perror("getsockname");
    }

    niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
    if (((struct sockaddr *)&local_ip)->sa_family == AF_INET6)
      niflags |= NI_WITHSCOPEID;
#endif
    error = getnameinfo((struct sockaddr *)&local_ip, salen,
			hbuf, sizeof(hbuf), pbuf, sizeof(pbuf), niflags);
    if (error != 0) {
	fprintf(stderr, "getnameinfo: %s\n", gai_strerror(error));
	strcpy(hbuf, "unknown");
	strcpy(pbuf, "unknown");
    }
    snprintf(localaddr, sizeof(localaddr), "%s;%s", hbuf, pbuf);

    salen = sizeof(remote_ip);
    if (getpeername(fd, (struct sockaddr *)&remote_ip, (unsigned int *) &salen) < 0) {
	perror("getpeername");
    }

    niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
    if (((struct sockaddr *)&remote_ip)->sa_family == AF_INET6)
	niflags |= NI_WITHSCOPEID;
#endif
    error = getnameinfo((struct sockaddr *)&remote_ip, salen,
			hbuf, sizeof(hbuf), pbuf, sizeof(pbuf), niflags);
    if (error != 0) {
	fprintf(stderr, "getnameinfo: %s\n", gai_strerror(error));
	strcpy(hbuf, "unknown");
	strcpy(pbuf, "unknown");
    }
    snprintf(remoteaddr, sizeof(remoteaddr), "%s;%s", hbuf, pbuf);
    
    /* client new connection */
    r = sasl_client_new(service, host, localaddr, remoteaddr, NULL, 0, &conn);
    if (r != SASL_OK) saslfail(r, "allocating connection state");

    if (cb_flag) {
        cb.name = "sasl-sample";
        cb.critical = cb_flag > 1;
        cb.data = (unsigned char *) "this is a test of channel binding";
        cb.len = (unsigned int) strlen((const char *) cb.data);

        sasl_setprop(conn, SASL_CHANNEL_BINDING, &cb);
    }

    /* set external properties here
       sasl_setprop(conn, SASL_SSF_EXTERNAL, &extprops); */

    /* set required security properties here
       sasl_setprop(conn, SASL_SEC_PROPS, &secprops); */


    in = fdopen(fd, "r");
    out = fdopen(fd, "w");

    r = mysasl_negotiate(in, out, conn);
    if (r == SASL_OK) {
	/* send/receive data */
	
	
    }
    
    printf("closing connection\n");
    fclose(in);
    fclose(out);
    close(fd);
    sasl_dispose(&conn);

    sasl_done();

    return r;
}
