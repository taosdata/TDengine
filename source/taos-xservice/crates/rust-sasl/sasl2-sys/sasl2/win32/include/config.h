/* config.h--SASL configuration for win32
 * Ryan Troll
 */
/* 
 * Copyright (c) 1998-2004 Carnegie Mellon University.  All rights reserved.
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

#include <stddef.h>

/* winsock2 includes windows.h.
   Note that we can't include both winsock.h and winsock2.h as
   they conflict */
#include <winsock2.h>
#include <tchar.h>

/* Our package */
#define PACKAGE "cyrus-sasl"

/* Our version */
#define VERSION "2.1.28"

/* Visual Studio supports prototypes */
#define PROTOTYPES     1

#ifndef HAVE_CADDR_T
#ifndef	caddr_t
typedef unsigned char   *caddr_t;
#define HAVE_CADDR_T	1
#endif
#endif

#ifndef _INTPTR_T_DEFINED

#ifdef  _WIN64
typedef __int64             intptr_t;
#else
typedef int		    intptr_t;
#endif

#endif

/* Registry key that contains the locations of the plugins */
#define SASL_ROOT_KEY _T("SOFTWARE\\Carnegie Mellon\\Project Cyrus\\SASL Library")
#define SASL_PLUGIN_PATH_ATTR _T("SearchPath")
#define SASL_CONF_PATH_ATTR _T("ConfFile")

/* : This should probably be replaced with a call to a function
   : that gets the proper value from Registry */
#define SASL_DB_PATH "c:\\CMU\\sasldb2"

/* what db package are we using? */
/* #undef SASL_GDBM */
/* #undef SASL_NDBM */
#define SASL_BERKELEYDB 1

/* which mechs can we link statically? */
/* #define STATIC_ANONYMOUS 1 */
/* #define STATIC_CRAMMD5 1 */
/* #define STATIC_DIGESTMD5 1 */
/* #define STATIC_GSSAPIV2 1 */
/* #undef STATIC_KERBEROS4 */
/* #define STATIC_LOGIN 1 */
/* #undef STATIC_MYSQL */
/* #define STATIC_NTLM 1 */
/* #define STATIC_OTP 1 */
/* #define STATIC_PLAIN 1 */
/* #define STATIC_SASLDB 1 */
/* #define STATIC_SCRAM 1 */
/* #define STATIC_SRP 1 */

/* ------------------------------------------------------------ */

/* Things that are fetched via autoconf under Unix
 */
#define HAVE_MEMCPY 1
#define HAVE_OPENSSL 1

#define PLUGINDIR "C:\\CMU\\bin\\sasl2"
#define CONFIGDIR "C:\\CMU\\bin\\sasl2"

/* Windows calls these functions something else
 */
#define strcasecmp   _stricmp
#if defined (_MSC_VER) && (_MSC_VER < 1900)
#define snprintf    _snprintf
#endif
#define strncasecmp  _strnicmp
#define strdup  _strdup
#define sasl_getpid  GetCurrentThreadId

#define MAXHOSTNAMELEN 1024

/* ------------------------------------------------------------ */

#define WITHOUT_NANA
#define L_DEFAULT_GUARD (0)
#define I_DEFAULT_GUARD (0)
#define I(foo)
#define VL(foo)  printf foo;
#define VLP(foo,bar)

/* we're not gcc */
#define __attribute__(foo)

/* : Same as in tpipv6.h */
#ifndef HAVE_SOCKLEN_T
typedef int socklen_t;
#endif /* HAVE_SOCKLEN_T */

/* If we expect to run on XP and later, we have IPv6 support natively */
#if TARGET_WIN_SYSTEM >= 51
#if !defined(_WIN32_WINNT)
/* This forces the inclusion of OS supported functions, with no fallback */
#define _WIN32_WINNT	0x0510
#endif
#endif

#if defined(_MSC_VER) && (_MSC_VER >= 1300)
/* The following two defines will prevent our own definitions below */
#define HAVE_GETADDRINFO
#define HAVE_GETNAMEINFO
#define HAVE_STRUCT_SOCKADDR_STORAGE
/* Unless _WIN32_WINNT > 0x0500, Ws2tcpip.h will try to find OS provided
   getaddrinfo at runtime. It will fallback to Microsoft emulation,
   if not found */
#include <Ws2tcpip.h>
#endif

#if !defined(HAVE_STRUCT_SOCKADDR_STORAGE) && !defined(_SS_MAXSIZE)
#define	_SS_MAXSIZE	128	/* Implementation specific max size */
#define	_SS_PADSIZE	(_SS_MAXSIZE - sizeof (struct sockaddr))

struct sockaddr_storage {
	struct	sockaddr ss_sa;
	char		__ss_pad2[_SS_PADSIZE];
};
# define ss_family ss_sa.sa_family
#endif /* !HAVE_STRUCT_SOCKADDR_STORAGE */

#ifndef AF_INET6
/* Define it to something that should never appear */
#define	AF_INET6	AF_MAX
#endif

#ifndef HAVE_GETADDRINFO
#define	getaddrinfo	sasl_getaddrinfo
#define	freeaddrinfo	sasl_freeaddrinfo
#define	gai_strerror	sasl_gai_strerror
#endif

#ifndef HAVE_GETNAMEINFO
#define	getnameinfo	sasl_getnameinfo
#endif

#if !defined(HAVE_GETNAMEINFO) || !defined(HAVE_GETADDRINFO)
#include "gai.h"
#endif

#ifndef AI_NUMERICHOST   /* support glibc 2.0.x */
#define AI_NUMERICHOST  4
#define NI_NUMERICHOST  2
#define NI_NAMEREQD     4
#define NI_NUMERICSERV  8
#endif

#include <time.h>

/* Keep in sync with SleepyCat definitions */
typedef int int32_t;
typedef __int64 int64_t;
#ifdef _WIN64
typedef int64_t ssize_t;
#else
typedef int32_t ssize_t;
#endif

#define HIER_DELIMITER '\\'

#ifndef sleep
#define sleep(seconds)		plug_sleep(seconds)
unsigned int plug_sleep(unsigned int seconds);
#endif

#endif /* CONFIG_H */
