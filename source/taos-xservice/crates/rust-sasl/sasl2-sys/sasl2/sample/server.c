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
 * Copyright 2009  by the Massachusetts Institute of Technology.
 * All Rights Reserved.
 *
 * Export of this software from the United States of America may
 *   require a specific license from the United States Government.
 *   It is the responsibility of any person or organization contemplating
 *   export to obtain such a license before exporting.
 *
 * WITHIN THAT CONSTRAINT, permission to use, copy, modify, and
 * distribute this software and its documentation for any purpose and
 * without fee is hereby granted, provided that the above copyright
 * notice appear in all copies and that both that copyright notice and
 * this permission notice appear in supporting documentation, and that
 * the name of M.I.T. not be used in advertising or publicity pertaining
 * to distribution of the software without specific, written prior
 * permission.  Furthermore if you modify this software you must label
 * your software as modified software and not distribute it in such a
 * fashion that it might be confused with the original M.I.T. software.
 * M.I.T. makes no representations about the suitability of
 * this software for any purpose.  It is provided "as is" without express
 * or implied warranty.
 *
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

#include <sasl.h>

#ifdef HAVE_GSS_GET_NAME_ATTRIBUTE
#include <gssapi/gssapi.h>
#ifndef KRB5_HEIMDAL
#ifdef HAVE_GSSAPI_GSSAPI_EXT_H
#include <gssapi/gssapi_ext.h>
#endif
#endif
#endif

#include "common.h"

#if !defined(IPV6_BINDV6ONLY) && defined(IN6P_IPV6_V6ONLY)
#define IPV6_BINDV6ONLY IN6P_BINDV6ONLY
#endif
#if !defined(IPV6_V6ONLY) && defined(IPV6_BINDV6ONLY)
#define	IPV6_V6ONLY	IPV6_BINDV6ONLY
#endif
#ifndef IPV6_BINDV6ONLY
#undef      IPV6_V6ONLY
#endif

#ifdef HAVE_GSS_GET_NAME_ATTRIBUTE
static OM_uint32
enumerateAttributes(OM_uint32 *minor,
                    gss_name_t name,
                    int noisy);
#endif

/* create a socket listening on port 'port' */
/* if af is PF_UNSPEC more than one socket may be returned */
/* the returned list is dynamically allocated, so caller needs to free it */
int *listensock(const char *port, const int af)
{
    struct addrinfo hints, *ai, *r;
    int err, maxs, *sock, *socks;
    const int on = 1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = af;
    hints.ai_socktype = SOCK_STREAM;
    err = getaddrinfo(NULL, port, &hints, &ai);
    if (err) {
	fprintf(stderr, "%s\n", gai_strerror(err));
	exit(EX_USAGE);
    }

    /* Count max number of sockets we may open */
    for (maxs = 0, r = ai; r; r = r->ai_next, maxs++)
	;
    socks = malloc((maxs + 1) * sizeof(int));
    if (!socks) {
	fprintf(stderr, "couldn't allocate memory for sockets\n");
	freeaddrinfo(ai);
	exit(EX_OSERR);
    }

    socks[0] = 0;	/* num of sockets counter at start of array */
    sock = socks + 1;
    for (r = ai; r; r = r->ai_next) {
	fprintf(stderr, "trying %d, %d, %d\n",r->ai_family, r->ai_socktype, r->ai_protocol);
	*sock = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
	if (*sock < 0) {
	    perror("socket");
	    continue;
	}
	if (setsockopt(*sock, SOL_SOCKET, SO_REUSEADDR, 
		       (void *) &on, sizeof(on)) < 0) {
	    perror("setsockopt(SO_REUSEADDR)");
	    close(*sock);
	    continue;
	}
#if defined(IPV6_V6ONLY) && !(defined(__FreeBSD__) && __FreeBSD__ < 3)
	if (r->ai_family == AF_INET6) {
	    if (setsockopt(*sock, IPPROTO_IPV6, IPV6_BINDV6ONLY,
			   (void *) &on, sizeof(on)) < 0) {
		perror("setsockopt (IPV6_BINDV6ONLY)");
		close(*sock);
		continue;
	    }
	}
#endif
	if (bind(*sock, r->ai_addr, r->ai_addrlen) < 0) {
	    perror("bind");
	    close(*sock);
	    continue;
 	}

 	if (listen(*sock, 5) < 0) {
 	    perror("listen");
 	    close(*sock);
 	    continue;
 	}

 	socks[0]++;
 	sock++;
    }

    freeaddrinfo(ai);

    if (socks[0] == 0) {
 	fprintf(stderr, "Couldn't bind to any socket\n");
 	free(socks);
	exit(EX_OSERR);
    }

    return socks;
}

void usage(void)
{
    fprintf(stderr, "usage: server [-C|-c] [-h hostname] [-p port] [-s service] [-m mech]\n");
    exit(EX_USAGE);
}

/* globals because i'm lazy */
char *mech;

/* do the sasl negotiation; return -1 if it fails */
int mysasl_negotiate(FILE *in, FILE *out, sasl_conn_t *conn)
{
    char buf[8192];
    char chosenmech[128];
    const char *data;
    int len;
    int r = SASL_FAIL;
    const char *userid;
#ifdef HAVE_GSS_GET_NAME_ATTRIBUTE
    gss_name_t peer = GSS_C_NO_NAME;
#endif
    
    /* generate the capability list */
    if (mech) {
	dprintf(2, "forcing use of mechanism %s\n", mech);
	data = strdup(mech);
	len = strlen(data);
    } else {
	int count;

	dprintf(1, "generating client mechanism list... ");
	r = sasl_listmech(conn, NULL, NULL, " ", NULL,
			  &data, (unsigned int *) &len, &count);
	if (r != SASL_OK) saslfail(r, "generating mechanism list");
	dprintf(1, "%d mechanisms\n", count);
    }

    /* send capability list to client */
    send_string(out, data, len);
    if (mech)
	free((void *) data);

    dprintf(1, "waiting for client mechanism...\n");
    len = recv_string(in, chosenmech, sizeof chosenmech);
    if (len <= 0) {
	printf("client didn't choose mechanism\n");
	fputc('N', out); /* send NO to client */
	fflush(out);
	return -1;
    }

    if (mech && strcasecmp(mech, chosenmech)) {
	printf("client didn't choose mandatory mechanism\n");
	fputc('N', out); /* send NO to client */
	fflush(out);
	return -1;
    }

    len = recv_string(in, buf, sizeof(buf));
    if(len != 1) {
	saslerr(r, "didn't receive first-send parameter correctly");
	fputc('N', out);
	fflush(out);
	return -1;
    }

    if(buf[0] == 'Y') {
        /* receive initial response (if any) */
        len = recv_string(in, buf, sizeof(buf));

        /* start libsasl negotiation */
        r = sasl_server_start(conn, chosenmech, buf, len,
			      &data, (unsigned int *) &len);
    } else {
	r = sasl_server_start(conn, chosenmech, NULL, 0,
			      &data, (unsigned int *) &len);
    }
    
    if (r != SASL_OK && r != SASL_CONTINUE) {
	saslerr(r, "starting SASL negotiation");
	fputc('N', out); /* send NO to client */
	fflush(out);
	return -1;
    }

    while (r == SASL_CONTINUE) {
	if (data) {
	    dprintf(2, "sending response length %d...\n", len);
	    fputc('C', out); /* send CONTINUE to client */
	    send_string(out, data, len);
	} else {
	    dprintf(2, "sending null response...\n");
	    fputc('C', out); /* send CONTINUE to client */
	    send_string(out, "", 0);
	}

	dprintf(1, "waiting for client reply...\n");
	len = recv_string(in, buf, sizeof buf);
	if (len < 0) {
	    printf("client disconnected\n");
	    return -1;
	}

	r = sasl_server_step(conn, buf, len, &data, (unsigned int *) &len);
	if (r != SASL_OK && r != SASL_CONTINUE) {
	    saslerr(r, "performing SASL negotiation");
	    fputc('N', out); /* send NO to client */
	    fflush(out);
	    return -1;
	}
    }

    if (r != SASL_OK) {
	saslerr(r, "incorrect authentication");
	fputc('N', out); /* send NO to client */
	fflush(out);
	return -1;
    }

    fputc('O', out); /* send OK to client */
    fflush(out);
    dprintf(1, "negotiation complete\n");

    r = sasl_getprop(conn, SASL_USERNAME, (const void **) &userid);
    printf("successful authentication '%s'\n", userid);

#ifdef HAVE_GSS_GET_NAME_ATTRIBUTE
    r = sasl_getprop(conn, SASL_GSS_PEER_NAME, (const void **) &peer);
    if (peer != GSS_C_NO_NAME) {
        OM_uint32 minor;
        enumerateAttributes(&minor, peer, 1);
    }
#endif

    return 0;
}

int main(int argc, char *argv[])
{
    int c;
    char *port = "12345";
    char *service = "rcmd";
    char *hostname = NULL;
    int *l, maxfd=0;
    int r, i;
    sasl_conn_t *conn;
    int cb_flag = 0;

    while ((c = getopt(argc, argv, "Cch:p:s:m:")) != EOF) {
	switch(c) {
	case 'C':
	    cb_flag = 2;        /* channel bindings are critical */
	    break;

	case 'c':
	    cb_flag = 1;        /* channel bindings are present */
	    break;

	case 'h':
	    hostname = optarg;
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

    /* initialize the sasl library */
    r = sasl_server_init(NULL, "sample");
    if (r != SASL_OK) saslfail(r, "initializing libsasl");

    /* get a listening socket */
    if ((l = listensock(port, PF_UNSPEC)) == NULL) {
	saslfail(SASL_FAIL, "allocating listensock");
    }

    for (i = 1; i <= l[0]; i++) {
       if (l[i] > maxfd)
           maxfd = l[i];
    }

    for (;;) {
	char localaddr[NI_MAXHOST | NI_MAXSERV],
	     remoteaddr[NI_MAXHOST | NI_MAXSERV];
	char myhostname[1024+1];
	char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];
	struct sockaddr_storage local_ip, remote_ip;
	int niflags, error;
	int salen;
	int nfds, fd = -1;
	FILE *in, *out;
	fd_set readfds;
	sasl_channel_binding_t cb;

	FD_ZERO(&readfds);
	for (i = 1; i <= l[0]; i++)
	    FD_SET(l[i], &readfds);

	nfds = select(maxfd + 1, &readfds, 0, 0, 0);
	if (nfds <= 0) {
	    if (nfds < 0 && errno != EINTR)
		perror("select");
	    continue;
	}

       for (i = 1; i <= l[0]; i++) 
           if (FD_ISSET(l[i], &readfds)) {
               fd = accept(l[i], NULL, NULL);
               break;
           }

	if (fd < 0) {
	    if (errno != EINTR)
		perror("accept");
	    continue;
	}

	printf("accepted new connection\n");

	/* set ip addresses */
	salen = sizeof(local_ip);
	if (getsockname(fd, (struct sockaddr *)&local_ip, (unsigned int *) &salen) < 0) {
	    perror("getsockname");
	}
	niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
	if (((struct sockaddr *)&local_ip)->sa_family == AF_INET6)
	    niflags |= NI_WITHSCOPEID;
#endif
	error = getnameinfo((struct sockaddr *)&local_ip, salen, hbuf,
			    sizeof(hbuf), pbuf, sizeof(pbuf), niflags);
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
	error = getnameinfo((struct sockaddr *)&remote_ip, salen, hbuf,
			    sizeof(hbuf), pbuf, sizeof(pbuf), niflags);
	if (error != 0) {
	    fprintf(stderr, "getnameinfo: %s\n", gai_strerror(error));
	    strcpy(hbuf, "unknown");
	    strcpy(pbuf, "unknown");
	}
	snprintf(remoteaddr, sizeof(remoteaddr), "%s;%s", hbuf, pbuf);

	if (hostname == NULL) {
	    r = gethostname(myhostname, sizeof(myhostname)-1);
	    if(r == -1) saslfail(r, "getting hostname");
	    hostname = myhostname;
	}

	r = sasl_server_new(service, hostname, NULL, localaddr, remoteaddr,
			    NULL, 0, &conn);
	if (r != SASL_OK) saslfail(r, "allocating connection state");

	cb.name = "sasl-sample";
	cb.critical = cb_flag > 1;
	cb.data = (const unsigned char *) "this is a test of channel binding";
	cb.len = (unsigned int) strlen((const char *) cb.data);

	if (cb_flag) {
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
    }

    sasl_done();
}

#ifdef HAVE_GSS_GET_NAME_ATTRIBUTE
static void displayStatus_1(m, code, type)
    char *m;
    OM_uint32 code;
    int type;
{
    OM_uint32 maj_stat, min_stat;
    gss_buffer_desc msg;
    OM_uint32 msg_ctx;

    msg_ctx = 0;
    while (1) {
        maj_stat = gss_display_status(&min_stat, code,
                                      type, GSS_C_NULL_OID,
                                      &msg_ctx, &msg);
        fprintf(stderr, "%s (%u): %s\n", m, maj_stat, (char *)msg.value);
        (void) gss_release_buffer(&min_stat, &msg);

        if (!msg_ctx)
            break;
    }
}

static void displayStatus(msg, maj_stat, min_stat)
    char *msg;
    OM_uint32 maj_stat;
    OM_uint32 min_stat;
{
    displayStatus_1(msg, maj_stat, GSS_C_GSS_CODE);
    displayStatus_1(msg, min_stat, GSS_C_MECH_CODE);
}

static void
dumpAttribute(OM_uint32 *minor,
              gss_name_t name,
              gss_buffer_t attribute,
              int noisy)
{
    OM_uint32 major, tmp;
    gss_buffer_desc value;
    gss_buffer_desc display_value;
    int authenticated = 0;
    int complete = 0;
    int more = -1;
    unsigned int i;

    while (more != 0) {
        value.value = NULL;
        display_value.value = NULL;

        major = gss_get_name_attribute(minor,
                                       name,
                                       attribute,
                                       &authenticated,
                                       &complete,
                                       &value,
                                       &display_value,
                                       &more);
        if (GSS_ERROR(major)) {
            displayStatus("gss_get_name_attribute", major, *minor);
            break;
        }

        printf("Attribute %.*s %s %s\n\n%.*s\n",
               (int)attribute->length, (char *)attribute->value,
               authenticated ? "Authenticated" : "",
               complete ? "Complete" : "",
               (int)display_value.length, (char *)display_value.value);

        if (noisy) {
            for (i = 0; i < value.length; i++) {
                if ((i % 32) == 0)
                    printf("\n");
                printf("%02x", ((char *)value.value)[i] & 0xFF);
            }
            printf("\n\n");
        }

        gss_release_buffer(&tmp, &value);
        gss_release_buffer(&tmp, &display_value);
    }
}

static OM_uint32
enumerateAttributes(OM_uint32 *minor,
                    gss_name_t name,
                    int noisy)
{
    OM_uint32 major, tmp;
    int name_is_MN;
    gss_OID mech = GSS_C_NO_OID;
    gss_buffer_set_t attrs = GSS_C_NO_BUFFER_SET;
    unsigned int i;

    major = gss_inquire_name(minor,
                             name,
                             &name_is_MN,
                             &mech,
                             &attrs);
    if (GSS_ERROR(major)) {
        displayStatus("gss_inquire_name", major, *minor);
        return major;
    }

    if (attrs != GSS_C_NO_BUFFER_SET) {
        for (i = 0; i < attrs->count; i++)
            dumpAttribute(minor, name, &attrs->elements[i], noisy);
    }

    gss_release_oid(&tmp, &mech);
    gss_release_buffer_set(&tmp, &attrs);

    return major;
}
#endif
