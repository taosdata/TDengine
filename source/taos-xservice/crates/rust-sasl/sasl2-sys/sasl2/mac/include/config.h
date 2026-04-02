/* $Id: config.h,v 1.4 2004/03/08 16:57:27 rjs3 Exp $
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

#ifndef CONFIG_H
#define CONFIG_H

//#ifdef XSUS
//#include <windows.h>
//#endif

#include <string.h>

/*
 * funky names for functions so we dont conflict with possible
 * real ones in applications we load into
 */

#define htons
#define htonl
#define ntohl
#define ntohs
#define strdup xxx_sasl_strdup
#define strcasecmp xxx_sasl_strcasecmp
#define gethostname xxx_sasl_gethostname
#define inet_aton xxx_sasl_inetaton

/* Our package */
#define PACKAGE "cyrus-sasl"

/* Our version */
#define VERSION "2.0.4"

/* We only want minimal server functionality.  Cripple the server functionality when necessary to get
 * things to compile.
 *
 * Currently only cripples PLAIN.
 */
#define SASL_MINIMAL_SERVER 1

/* DB Type */
#undef SASL_DB_TYPE

int strcasecmp(const char *s1,const char *s2);
int strncasecmp(const char *s1,const char *s2,int len);

int strcpy_truncate(char *dest,char *src,int len);

#define gethostname xxx_sasl_gethostname
int gethostname(char *dest,int destlen);

#define SASL_PATH_ENV_VAR "foo"
#define PLUGINDIR "make_me_a_function_to_get_that_info"

typedef unsigned char u_char;
typedef unsigned long u_long;

char *strdup(const char *str);
 struct sockaddr_in {
	u_char  sin_len;
	u_char  sin_family;
   unsigned short sin_port;
   union {
   	 unsigned long s_addr;
   } sin_addr;
   char sin_zero[8];
 };

struct in_addr {
	unsigned long s_addr;
};

#ifndef HAVE_SOCKLEN_T
typedef unsigned int socklen_t;
#endif /* HAVE_SOCKLEN_T */

#include "gai.h"

#ifndef NULL
#define NULL (0L)
#endif

#ifdef RUBBISH
int snprintf (char *str,size_t count,const char *fmt,...);
int snprintf (char *str,int count,const char *fmt,...);
#endif

extern char *optarg;
extern int optind;
extern int getopt(
	int nargc,
	char * const *nargv,
	const char *ostr);

extern int getsubopt(char **optionp, const char * const *tokens, char **valuep);
extern char* getpass(const char *prompt);

/* ------------------------------------------------------------ */

/* Things that are fetched via autoconf under Unix
 */
#define HAVE_MEMCPY 1

#define MAXHOSTNAMELEN 1024

/* ------------------------------------------------------------ */

#define WITHOUT_NANA
#define L_DEFAULT_GUARD (0)
#define I_DEFAULT_GUARD (0)
#define I(foo)
#ifdef RUBBISH
//#define VL(foo)
#endif
#include <stdio.h>
#define XXVL(foo)  printf foo;
#define VL(foo)
#define VLP(foo,bar)

#define __attribute__(foo)

#include <netinet/in.h>

#define getservbyname(X,Y) NULL
struct servent {
	int s_port;
};

struct sockaddr {
        u_char  sa_len;                 /* total length */
        u_char  sa_family;              /* address family */
        char    sa_data[14];            /* address value */
}; 
#define SOCK_MAXADDRLEN 255             /* longest possible addresses */

#ifndef HAVE_SOCKLEN_T
typedef unsigned int socklen_t;
#endif /* HAVE_SOCKLEN_T */

#ifndef HAVE_STRUCT_SOCKADDR_STORAGE
#define        _SS_MAXSIZE     128     /* Implementation specific max size */
#define        _SS_PADSIZE     (_SS_MAXSIZE - sizeof (struct sockaddr))

struct sockaddr_storage {
       struct  sockaddr ss_sa;
       char            __ss_pad2[_SS_PADSIZE];
};
#define ss_family ss_sa.sa_family
#endif /* !HAVE_STRUCT_SOCKADDR_STORAGE */

#define get_krb_err_txt(X) (krb_err_txt[(X)])

/*
 * Address families.
 */
#define AF_UNSPEC       0               /* unspecified */
#define AF_UNIX         1               /* local to host (pipes, portals) */
#define AF_INET         2               /* internetwork: UDP, TCP, etc. */
#define AF_IMPLINK      3               /* arpanet imp addresses */
#define AF_PUP          4               /* pup protocols: e.g. BSP */
#define AF_CHAOS        5               /* mit CHAOS protocols */
#define AF_NS           6               /* XEROX NS protocols */
#define AF_NBS          7               /* nbs protocols */
#define AF_ECMA         8               /* european computer manufacturers */
#define AF_DATAKIT      9               /* datakit protocols */
#define AF_CCITT        10              /* CCITT protocols, X.25 etc */
#define AF_SNA          11              /* IBM SNA */
#define AF_DECnet       12              /* DECnet */
#define AF_DLI          13              /* Direct data link interface */
#define AF_LAT          14              /* LAT */
#define AF_HYLINK       15              /* NSC Hyperchannel */
#define AF_APPLETALK    16              /* Apple Talk */
#define AF_NIT          17              /* Network Interface Tap */
#define AF_802          18              /* IEEE 802.2, also ISO 8802 */
#define AF_OSI          19              /* umbrella for all families used */
#define AF_X25          20              /* CCITT X.25 in particular */
#define AF_OSINET       21              /* AFI = 47, IDI = 4 */
#define AF_GOSIP        22              /* U.S. Government OSI */
#define AF_IPX          23              /* Novell Internet Protocol */
#define AF_ROUTE        24              /* Internal Routing Protocol */
#define AF_LINK         25              /* Link-layer interface */
#define AF_INET6        26              /* Internet Protocol, Version 6 */
#define AF_KEY          27              /* Security Association DB socket */

#define AF_MAX          27

/*
 * Protocol families, same as address families for now.
 */
#define PF_UNSPEC       AF_UNSPEC
#define PF_UNIX         AF_UNIX
#define PF_INET         AF_INET
#define PF_IMPLINK      AF_IMPLINK
#define PF_PUP          AF_PUP
#define PF_CHAOS        AF_CHAOS
#define PF_NS           AF_NS
#define PF_NBS          AF_NBS
#define PF_ECMA         AF_ECMA
#define PF_DATAKIT      AF_DATAKIT
#define PF_CCITT        AF_CCITT
#define PF_SNA          AF_SNA
#define PF_DECnet       AF_DECnet
#define PF_DLI          AF_DLI
#define PF_LAT          AF_LAT
#define PF_HYLINK       AF_HYLINK
#define PF_APPLETALK    AF_APPLETALK
#define PF_NIT          AF_NIT
#define PF_802          AF_802
#define PF_OSI          AF_OSI
#define PF_X25          AF_X25
#define PF_OSINET       AF_OSINET
#define PF_GOSIP        AF_GOSIP
#define PF_IPX          AF_IPX
#define PF_ROUTE        AF_ROUTE
#define PF_LINK         AF_LINK
#define PF_INET6        AF_INET6
#define PF_KEY          AF_KEY

#define PF_MAX          AF_MAX

#define SOCK_STREAM		1
#define SOCK_DGRAM		2

struct iovec {
    char *iov_base; 
    long iov_len;
};      

#ifndef HAVE_GETADDRINFO
#define        getaddrinfo     sasl_getaddrinfo
#define        freeaddrinfo    sasl_freeaddrinfo
#define        getnameinfo     sasl_getnameinfo
#define        gai_strerror    sasl_gai_strerror
#include "gai.h"
#endif

#endif /* CONFIG_H */
