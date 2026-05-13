/* staticopen.h
 * Rob Siemborski
 * Howard Chu
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

typedef enum {
	UNKNOWN = 0, SERVER = 1, CLIENT = 2, AUXPROP = 3, CANONUSER = 4
} _sasl_plug_type;

typedef struct {
	_sasl_plug_type type;
	char *name;
	sasl_client_plug_init_t *plug;
} _sasl_plug_rec;

/* For static linking */
#define SPECIFIC_CLIENT_PLUG_INIT_PROTO( x ) \
sasl_client_plug_init_t x##_client_plug_init

#define SPECIFIC_SERVER_PLUG_INIT_PROTO( x ) \
sasl_server_plug_init_t x##_server_plug_init

#define SPECIFIC_AUXPROP_PLUG_INIT_PROTO( x ) \
sasl_auxprop_init_t x##_auxprop_plug_init

#define SPECIFIC_CANONUSER_PLUG_INIT_PROTO( x ) \
sasl_canonuser_init_t x##_canonuser_plug_init

/* Static Compillation Foo */
#define SPECIFIC_CLIENT_PLUG_INIT( x, n )\
	{ CLIENT, n, x##_client_plug_init }
#define SPECIFIC_SERVER_PLUG_INIT( x, n )\
	{ SERVER, n, (sasl_client_plug_init_t *)x##_server_plug_init }
#define SPECIFIC_AUXPROP_PLUG_INIT( x, n )\
	{ AUXPROP, n, (sasl_client_plug_init_t *)x##_auxprop_plug_init }
#define SPECIFIC_CANONUSER_PLUG_INIT( x, n )\
	{ CANONUSER, n, (sasl_client_plug_init_t *)x##_canonuser_plug_init }

#ifdef STATIC_ANONYMOUS
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( anonymous );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( anonymous );
#endif
#ifdef STATIC_CRAMMD5
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( crammd5 );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( crammd5 );
#endif
#ifdef STATIC_DIGESTMD5
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( digestmd5 );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( digestmd5 );
#endif
#ifdef STATIC_SCRAM
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( scram );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( scram );
#endif
#ifdef STATIC_GSSAPIV2
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( gssapiv2 );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( gssapiv2 );
#endif
#ifdef STATIC_KERBEROS4
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( kerberos4 );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( kerberos4 );
#endif
#ifdef STATIC_LOGIN
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( login );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( login );
#endif
#ifdef STATIC_NTLM
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( ntlm );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( ntlm );
#endif
#ifdef STATIC_OTP
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( otp );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( otp );
#endif
#ifdef STATIC_PLAIN
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( plain );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( plain );
#endif
#ifdef STATIC_SRP
extern SPECIFIC_SERVER_PLUG_INIT_PROTO( srp );
extern SPECIFIC_CLIENT_PLUG_INIT_PROTO( srp );
#endif
#ifdef STATIC_SASLDB
extern SPECIFIC_AUXPROP_PLUG_INIT_PROTO( sasldb );
#endif
#ifdef STATIC_SQL
extern SPECIFIC_AUXPROP_PLUG_INIT_PROTO( sql );
#endif
#ifdef STATIC_LDAPDB
extern SPECIFIC_AUXPROP_PLUG_INIT_PROTO( ldapdb );
#endif

_sasl_plug_rec _sasl_static_plugins[] = {
#ifdef STATIC_ANONYMOUS
	SPECIFIC_SERVER_PLUG_INIT( anonymous, "ANONYMOUS" ),
	SPECIFIC_CLIENT_PLUG_INIT( anonymous, "ANONYMOUS" ),
#endif
#ifdef STATIC_CRAMMD5
	SPECIFIC_SERVER_PLUG_INIT( crammd5, "CRAM-MD5" ),
	SPECIFIC_CLIENT_PLUG_INIT( crammd5, "CRAM-MD5" ),
#endif
#ifdef STATIC_DIGESTMD5
	SPECIFIC_SERVER_PLUG_INIT( digestmd5, "DIGEST-MD5" ),
	SPECIFIC_CLIENT_PLUG_INIT( digestmd5, "DIGEST-MD5" ),
#endif
#ifdef STATIC_GSSAPIV2
	SPECIFIC_SERVER_PLUG_INIT( gssapiv2, "GSSAPI" ),
	SPECIFIC_CLIENT_PLUG_INIT( gssapiv2, "GSSAPI" ),
#endif
#ifdef STATIC_KERBEROS4
	SPECIFIC_SERVER_PLUG_INIT( kerberos4, "KERBEROS_V4" ),
	SPECIFIC_CLIENT_PLUG_INIT( kerberos4, "KERBEROS_V4" ),
#endif
#ifdef STATIC_LOGIN
	SPECIFIC_SERVER_PLUG_INIT( login, "LOGIN" ),
	SPECIFIC_CLIENT_PLUG_INIT( login, "LOGIN" ),
#endif
#ifdef STATIC_NTLM
	SPECIFIC_SERVER_PLUG_INIT( ntlm, "NTLM" ),
	SPECIFIC_CLIENT_PLUG_INIT( ntlm, "NTLM" ),
#endif
#ifdef STATIC_OTP
	SPECIFIC_SERVER_PLUG_INIT( otp, "OTP" ),
	SPECIFIC_CLIENT_PLUG_INIT( otp, "OTP" ),
#endif
#ifdef STATIC_PLAIN
	SPECIFIC_SERVER_PLUG_INIT( plain, "PLAIN" ),
	SPECIFIC_CLIENT_PLUG_INIT( plain, "PLAIN" ),
#endif
#ifdef STATIC_SCRAM
        SPECIFIC_SERVER_PLUG_INIT( scram, "SCRAM" ),
        SPECIFIC_CLIENT_PLUG_INIT( scram, "SCRAM" ),
#endif
#ifdef STATIC_SRP
	SPECIFIC_SERVER_PLUG_INIT( srp, "SRP" ),
	SPECIFIC_CLIENT_PLUG_INIT( srp, "SRP" ),
#endif
#ifdef STATIC_SASLDB
	SPECIFIC_AUXPROP_PLUG_INIT( sasldb, "SASLDB" ),
#endif
#ifdef STATIC_SQL
	SPECIFIC_AUXPROP_PLUG_INIT( sql, "SQL" ),
#endif
#ifdef STATIC_LDAPDB
    SPECIFIC_AUXPROP_PLUG_INIT( ldapdb, "LDAPDB" ),
#endif
	{ UNKNOWN, NULL, NULL }
};
