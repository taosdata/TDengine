
/* Generic SASL plugin utility functions
 * Rob Siemborski
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

#ifndef _PLUGIN_COMMON_H_
#define _PLUGIN_COMMON_H_

#include <config.h>

#ifndef macintosh
#ifdef WIN32
# include <winsock2.h>
#else
# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>
# include <netdb.h>
#endif /* WIN32 */
#endif /* macintosh */

#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>

#define PLUG_API extern

#define SASL_CLIENT_PLUG_INIT( x ) \
extern sasl_client_plug_init_t x##_client_plug_init; \
PLUG_API int sasl_client_plug_init(const sasl_utils_t *utils, \
                         int maxversion, int *out_version, \
			 sasl_client_plug_t **pluglist, \
                         int *plugcount) { \
        return x##_client_plug_init(utils, maxversion, out_version, \
				     pluglist, plugcount); \
}

#define SASL_SERVER_PLUG_INIT( x ) \
extern sasl_server_plug_init_t x##_server_plug_init; \
PLUG_API int sasl_server_plug_init(const sasl_utils_t *utils, \
                         int maxversion, int *out_version, \
			 sasl_server_plug_t **pluglist, \
                         int *plugcount) { \
        return x##_server_plug_init(utils, maxversion, out_version, \
				     pluglist, plugcount); \
}

#define SASL_AUXPROP_PLUG_INIT( x ) \
extern sasl_auxprop_init_t x##_auxprop_plug_init; \
PLUG_API int sasl_auxprop_plug_init(const sasl_utils_t *utils, \
                           int maxversion, int *out_version, \
                           sasl_auxprop_plug_t **plug, \
                           const char *plugname) {\
        return x##_auxprop_plug_init(utils, maxversion, out_version, \
                                     plug, plugname); \
}

#define SASL_CANONUSER_PLUG_INIT( x ) \
extern sasl_canonuser_init_t x##_canonuser_plug_init; \
PLUG_API int sasl_canonuser_init(const sasl_utils_t *utils, \
                           int maxversion, int *out_version, \
                           sasl_canonuser_plug_t **plug, \
                           const char *plugname) {\
        return x##_canonuser_plug_init(utils, maxversion, out_version, \
                                     plug, plugname); \
}

/* note: msg cannot include additional variables, so if you want to
 * do a printf-format string, then you need to call seterror yourself */
#define SETERROR( utils, msg ) (utils)->seterror( (utils)->conn, 0, (msg) )

#ifndef MEMERROR
#define MEMERROR( utils ) \
    (utils)->seterror( (utils)->conn, 0, \
                       "Out of Memory in " __FILE__ " near line %d", __LINE__ )
#endif

#ifndef PARAMERROR
#define PARAMERROR( utils ) \
    (utils)->seterror( (utils)->conn, 0, \
                       "Parameter Error in " __FILE__ " near line %d", __LINE__ )
#endif

#ifndef SASLINT_H
typedef struct buffer_info 
{
    char *data;
    unsigned curlen;   /* Current length of data in buffer */
    unsigned reallen;  /* total length of buffer (>= curlen) */
} buffer_info_t;

#ifndef HAVE_GETHOSTNAME
#ifdef sun
/* gotta define gethostname ourselves on suns */
extern int gethostname(char *, int);
#endif
#endif /* HAVE_GETHOSTNAME */

#endif /* SASLINT_H */

#ifdef __cplusplus
extern "C" {
#endif

int _plug_ipfromstring(const sasl_utils_t *utils, const char *addr,
		       struct sockaddr *out, socklen_t outlen);
int _plug_iovec_to_buf(const sasl_utils_t *utils, const struct iovec *vec,
		       unsigned numiov, buffer_info_t **output);
int _plug_buf_alloc(const sasl_utils_t *utils, char **rwbuf,
		    unsigned *curlen, unsigned newlen);
int _plug_strdup(const sasl_utils_t * utils, const char *in,
	         char **out, int *outlen);
void _plug_free_string(const sasl_utils_t *utils, char **str);
void _plug_free_secret(const sasl_utils_t *utils, sasl_secret_t **secret);

#define _plug_get_userid(utils, result, prompt_need) \
	_plug_get_simple(utils, SASL_CB_USER, 0, result, prompt_need)
#define _plug_get_authid(utils, result, prompt_need) \
	_plug_get_simple(utils, SASL_CB_AUTHNAME, 1, result, prompt_need)
int _plug_get_simple(const sasl_utils_t *utils, unsigned int id, int required,
		     const char **result, sasl_interact_t **prompt_need);

int _plug_get_password(const sasl_utils_t *utils, sasl_secret_t **secret,
		       unsigned int *iscopy, sasl_interact_t **prompt_need);

int _plug_challenge_prompt(const sasl_utils_t *utils, unsigned int id,
			   const char *challenge, const char *promptstr,
			   const char **result, sasl_interact_t **prompt_need);

int _plug_get_realm(const sasl_utils_t *utils, const char **availrealms,
		    const char **realm, sasl_interact_t **prompt_need);

int _plug_make_prompts(const sasl_utils_t *utils,
		       sasl_interact_t **prompts_res,
		       const char *user_prompt, const char *user_def,
		       const char *auth_prompt, const char *auth_def,
		       const char *pass_prompt, const char *pass_def,
		       const char *echo_chal,
		       const char *echo_prompt, const char *echo_def,
		       const char *realm_chal,
		       const char *realm_prompt, const char *realm_def);

typedef struct decode_context {
    const sasl_utils_t *utils;
    unsigned int needsize;	/* How much of the 4-byte size do we need? */
    char sizebuf[4];		/* Buffer to accumulate the 4-byte size */
    unsigned int size;		/* Absolute size of the encoded packet */
    char *buffer;		/* Buffer to accumulate an encoded packet */
    unsigned int cursize;	/* Amount of packet data in the buffer */
    unsigned int in_maxbuf;	/* Maximum allowed size of an incoming encoded packet */
} decode_context_t;

void _plug_decode_init(decode_context_t *text,
		       const sasl_utils_t *utils, unsigned int in_maxbuf);

int _plug_decode(decode_context_t *text,
		 const char *input, unsigned inputlen,
		 char **output, unsigned *outputsize, unsigned *outputlen,
		 int (*decode_pkt)(void *rock,
				   const char *input, unsigned inputlen,
				   char **output, unsigned *outputlen),
		 void *rock);

void _plug_decode_free(decode_context_t *text);

int _plug_parseuser(const sasl_utils_t *utils,
		    char **user, char **realm, const char *user_realm, 
		    const char *serverFQDN, const char *input);

int _plug_make_fulluser(const sasl_utils_t *utils,
			char **fulluser, const char * useronly, const char *realm);

char * _plug_get_error_message (const sasl_utils_t *utils,
#ifdef WIN32
				DWORD error
#else
				int error
#endif
				);
void _plug_snprintf_os_info (char * osbuf, int osbuf_len);

#ifdef __cplusplus
}
#endif

#endif /* _PLUGIN_COMMON_H_ */
