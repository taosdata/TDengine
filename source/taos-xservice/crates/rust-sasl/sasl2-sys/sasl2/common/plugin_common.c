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

#include <config.h>
#ifndef macintosh
#ifdef WIN32
# include <winsock2.h>
# include <versionhelpers.h>
#else
# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>
# include <netdb.h>
# include <sys/utsname.h>
#endif /* WIN32 */
#endif /* macintosh */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <fcntl.h>
#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>

#include <errno.h>
#include <ctype.h>
#include <stdio.h>

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#include "plugin_common.h"

/* translate IPv4 mapped IPv6 address to IPv4 address */
static void sockaddr_unmapped(
#ifdef IN6_IS_ADDR_V4MAPPED
  struct sockaddr *sa, socklen_t *len
#else
  struct sockaddr *sa __attribute__((unused)),
  socklen_t *len __attribute__((unused))
#endif
)
{
#ifdef IN6_IS_ADDR_V4MAPPED
    struct sockaddr_in6 *sin6;
    struct sockaddr_in *sin4;
    uint32_t addr;
    int port;

    if (sa->sa_family != AF_INET6)
	return;
    sin6 = (struct sockaddr_in6 *)sa;
    if (!IN6_IS_ADDR_V4MAPPED((&sin6->sin6_addr)))
	return;
    sin4 = (struct sockaddr_in *)sa;
#ifdef s6_addr32
    addr = *(uint32_t *)&sin6->sin6_addr.s6_addr32[3];
#else
    memcpy(&addr, &sin6->sin6_addr.s6_addr[12], 4);
#endif
    port = sin6->sin6_port;
    memset(sin4, 0, sizeof(struct sockaddr_in));
    sin4->sin_addr.s_addr = addr;
    sin4->sin_port = port;
    sin4->sin_family = AF_INET;
#ifdef HAVE_SOCKADDR_SA_LEN
    sin4->sin_len = sizeof(struct sockaddr_in);
#endif
    *len = sizeof(struct sockaddr_in);
#else
    return;
#endif
}

int _plug_ipfromstring(const sasl_utils_t *utils, const char *addr,
		       struct sockaddr *out, socklen_t outlen) 
{
    int i, j;
    socklen_t len;
    struct sockaddr_storage ss;
    struct addrinfo hints, *ai = NULL;
    char hbuf[NI_MAXHOST];
    
    if(!utils || !addr || !out) {
	if(utils) PARAMERROR( utils );
	return SASL_BADPARAM;
    }

    /* Parse the address */
    for (i = 0; addr[i] != '\0' && addr[i] != ';'; i++) {
	if (i + 1 >= NI_MAXHOST) {
	    if(utils) PARAMERROR( utils );
	    return SASL_BADPARAM;
	}
	hbuf[i] = addr[i];
    }
    hbuf[i] = '\0';

    if (addr[i] == ';')
	i++;
    /* XXX/FIXME: Do we need this check? */
    for (j = i; addr[j] != '\0'; j++)
	if (!isdigit((int)(addr[j]))) {
	    PARAMERROR( utils );
	    return SASL_BADPARAM;
	}

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_NUMERICHOST;

    if (getaddrinfo(hbuf, &addr[i], &hints, &ai) != 0) {	
	PARAMERROR( utils );
	return SASL_BADPARAM;
    }

    len = (socklen_t) ai->ai_addrlen;
    memcpy(&ss, ai->ai_addr, len);
    freeaddrinfo(ai);
    sockaddr_unmapped((struct sockaddr *)&ss, &len);
    if (outlen < len) {
	PARAMERROR( utils );
	return SASL_BUFOVER;
    }

    memcpy(out, &ss, len);

    return SASL_OK;
}

int _plug_iovec_to_buf(const sasl_utils_t *utils, const struct iovec *vec,
		       unsigned numiov, buffer_info_t **output) 
{
    unsigned i;
    int ret;
    buffer_info_t *out;
    char *pos;

    if(!utils || !vec || !output) {
	if(utils) PARAMERROR( utils );
	return SASL_BADPARAM;
    }
    
    if(!(*output)) {
	*output = utils->malloc(sizeof(buffer_info_t));
	if(!*output) {
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}
	memset(*output,0,sizeof(buffer_info_t));
    }

    out = *output;
    
    out->curlen = 0;
    for(i=0; i<numiov; i++)
	out->curlen += vec[i].iov_len;

    ret = _plug_buf_alloc(utils, &out->data, &out->reallen, out->curlen);

    if(ret != SASL_OK) {
	MEMERROR(utils);
	return SASL_NOMEM;
    }
    
    memset(out->data, 0, out->reallen);
    pos = out->data;
    
    for(i=0; i<numiov; i++) {
	memcpy(pos, vec[i].iov_base, vec[i].iov_len);
	pos += vec[i].iov_len;
    }

    return SASL_OK;
}

/* Basically a conditional call to realloc(), if we need more */
int _plug_buf_alloc(const sasl_utils_t *utils, char **rwbuf,
		    unsigned *curlen, unsigned newlen) 
{
    if(!utils || !rwbuf || !curlen) {
	if (utils) PARAMERROR(utils);
	return SASL_BADPARAM;
    }

    if(!(*rwbuf)) {
	*rwbuf = utils->malloc(newlen);
	if (*rwbuf == NULL) {
	    *curlen = 0;
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}
	*curlen = newlen;
    } else if(*rwbuf && *curlen < newlen) {
	unsigned needed = 2*(*curlen);

	while(needed < newlen)
	    needed *= 2;

	*rwbuf = utils->realloc(*rwbuf, needed);
	if (*rwbuf == NULL) {
	    *curlen = 0;
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}
	*curlen = needed;
    } 

    return SASL_OK;
}

/* copy a string */
int _plug_strdup(const sasl_utils_t * utils, const char *in,
		 char **out, int *outlen)
{
  size_t len = 0;

  if(!utils || !in || !out) {
      if(utils) PARAMERROR(utils);
      return SASL_BADPARAM;
  }

  len = strlen(in);

  *out = utils->malloc(len + 1);
  if (!*out) {
      MEMERROR(utils);
      return SASL_NOMEM;
  }

  strcpy((char *) *out, in);

  if (outlen)
      *outlen = (int) len;

  return SASL_OK;
}

void _plug_free_string(const sasl_utils_t *utils, char **str)
{
  size_t len;

  if (!utils || !str || !(*str)) return;

  len = strlen(*str);

  utils->erasebuffer(*str, (unsigned int) len);
  utils->free(*str);

  *str=NULL;
}

void _plug_free_secret(const sasl_utils_t *utils, sasl_secret_t **secret) 
{
    if(!utils || !secret || !(*secret)) return;

    utils->erasebuffer((char *)(*secret)->data, (*secret)->len);
    utils->free(*secret);
    *secret = NULL;
}

/* 
 * Trys to find the prompt with the lookingfor id in the prompt list
 * Returns it if found. NULL otherwise
 */
sasl_interact_t *_plug_find_prompt(sasl_interact_t **promptlist,
				   unsigned int lookingfor)
{
    sasl_interact_t *prompt;

    if (promptlist && *promptlist) {
	for (prompt = *promptlist; prompt->id != SASL_CB_LIST_END; ++prompt) {
	    if (prompt->id==lookingfor)
		return prompt;
	}
    }

    return NULL;
}

/*
 * Retrieve the simple string given by the callback id.
 */
int _plug_get_simple(const sasl_utils_t *utils, unsigned int id, int required,
		     const char **result, sasl_interact_t **prompt_need)
{

    int ret = SASL_FAIL;
    sasl_getsimple_t *simple_cb;
    void *simple_context;
    sasl_interact_t *prompt;

    *result = NULL;

    /* see if we were given the result in the prompt */
    prompt = _plug_find_prompt(prompt_need, id);
    if (prompt != NULL) {
	/* We prompted, and got.*/
	
	if (required && !prompt->result) {
	    SETERROR(utils, "Unexpectedly missing a prompt result in _plug_get_simple");
	    return SASL_BADPARAM;
	}

	*result = prompt->result;
	return SASL_OK;
    }
  
    /* Try to get the callback... */
    ret = utils->getcallback(utils->conn, id, (sasl_callback_ft *)&simple_cb, &simple_context);

    if (ret == SASL_FAIL && !required)
	return SASL_OK;

    if (ret == SASL_OK && simple_cb) {
	ret = simple_cb(simple_context, id, result, NULL);
	if (ret != SASL_OK)
	    return ret;

	if (required && !*result) {
	    PARAMERROR(utils);
	    return SASL_BADPARAM;
	}
    }
  
    return ret;
}

/*
 * Retrieve the user password.
 */
int _plug_get_password(const sasl_utils_t *utils, sasl_secret_t **password,
		       unsigned int *iscopy, sasl_interact_t **prompt_need)
{
    int ret = SASL_FAIL;
    sasl_getsecret_t *pass_cb;
    void *pass_context;
    sasl_interact_t *prompt;

    *password = NULL;
    *iscopy = 0;

    /* see if we were given the password in the prompt */
    prompt = _plug_find_prompt(prompt_need, SASL_CB_PASS);
    if (prompt != NULL) {
	/* We prompted, and got.*/
	
	if (!prompt->result) {
	    SETERROR(utils, "Unexpectedly missing a prompt result in _plug_get_password");
	    return SASL_BADPARAM;
	}
      
	/* copy what we got into a secret_t */
	*password = (sasl_secret_t *) utils->malloc(sizeof(sasl_secret_t) +
						    prompt->len + 1);
	if (!*password) {
	    MEMERROR(utils);
	    return SASL_NOMEM;
	}
      
	(*password)->len=prompt->len;
	memcpy((*password)->data, prompt->result, prompt->len);
	(*password)->data[(*password)->len]=0;

	*iscopy = 1;

	return SASL_OK;
    }

    /* Try to get the callback... */
    ret = utils->getcallback(utils->conn, SASL_CB_PASS,
			     (sasl_callback_ft *)&pass_cb, &pass_context);

    if (ret == SASL_OK && pass_cb) {
	ret = pass_cb(utils->conn, pass_context, SASL_CB_PASS, password);
	if (ret != SASL_OK)
	    return ret;

	if (!*password) {
	    PARAMERROR(utils);
	    return SASL_BADPARAM;
	}
    }

    return ret;
}

/*
 * Retrieve the string given by the challenge prompt id.
 */
int _plug_challenge_prompt(const sasl_utils_t *utils, unsigned int id,
			   const char *challenge, const char *promptstr,
			   const char **result, sasl_interact_t **prompt_need)
{
    int ret = SASL_FAIL;
    sasl_chalprompt_t *chalprompt_cb;
    void *chalprompt_context;
    sasl_interact_t *prompt;

    *result = NULL;

    /* see if we were given the password in the prompt */
    prompt = _plug_find_prompt(prompt_need, id);
    if (prompt != NULL) {
	/* We prompted, and got.*/
	
	if (!prompt->result) {
	    SETERROR(utils, "Unexpectedly missing a prompt result in _plug_challenge_prompt");
	    return SASL_BADPARAM;
	}
      
	*result = prompt->result;
	return SASL_OK;
    }

    /* Try to get the callback... */
    ret = utils->getcallback(utils->conn, id,
			     (sasl_callback_ft *)&chalprompt_cb, &chalprompt_context);

    if (ret == SASL_OK && chalprompt_cb) {
	ret = chalprompt_cb(chalprompt_context, id,
			    challenge, promptstr, NULL, result, NULL);
	if (ret != SASL_OK)
	    return ret;

	if (!*result) {
	    PARAMERROR(utils);
	    return SASL_BADPARAM;
	}
    }

    return ret;
}

/*
 * Retrieve the client realm.
 */
int _plug_get_realm(const sasl_utils_t *utils, const char **availrealms,
		    const char **realm, sasl_interact_t **prompt_need)
{
    int ret = SASL_FAIL;
    sasl_getrealm_t *realm_cb;
    void *realm_context;
    sasl_interact_t *prompt;

    *realm = NULL;

    /* see if we were given the result in the prompt */
    prompt = _plug_find_prompt(prompt_need, SASL_CB_GETREALM);
    if (prompt != NULL) {
	/* We prompted, and got.*/
	
	if (!prompt->result) {
	    SETERROR(utils, "Unexpectedly missing a prompt result in _plug_get_realm");
	    return SASL_BADPARAM;
	}

	*realm = prompt->result;
	return SASL_OK;
    }

    /* Try to get the callback... */
    ret = utils->getcallback(utils->conn, SASL_CB_GETREALM,
			     (sasl_callback_ft *)&realm_cb, &realm_context);

    if (ret == SASL_OK && realm_cb) {
	ret = realm_cb(realm_context, SASL_CB_GETREALM, availrealms, realm);
	if (ret != SASL_OK)
	    return ret;

	if (!*realm) {
	    PARAMERROR(utils);
	    return SASL_BADPARAM;
	}
    }
  
    return ret;
}

/*
 * Make the requested prompts. (prompt==NULL means we don't want it)
 */
int _plug_make_prompts(const sasl_utils_t *utils,
		       sasl_interact_t **prompts_res,
		       const char *user_prompt, const char *user_def,
		       const char *auth_prompt, const char *auth_def,
		       const char *pass_prompt, const char *pass_def,
		       const char *echo_chal,
		       const char *echo_prompt, const char *echo_def,
		       const char *realm_chal,
		       const char *realm_prompt, const char *realm_def)
{
    int num = 1;
    int alloc_size;
    sasl_interact_t *prompts;

    if (user_prompt) num++;
    if (auth_prompt) num++;
    if (pass_prompt) num++;
    if (echo_prompt) num++;
    if (realm_prompt) num++;

    if (num == 1) {
	SETERROR( utils, "make_prompts() called with no actual prompts" );
	return SASL_FAIL;
    }

    alloc_size = sizeof(sasl_interact_t)*num;
    prompts = utils->malloc(alloc_size);
    if (!prompts) {
	MEMERROR( utils );
	return SASL_NOMEM;
    }
    memset(prompts, 0, alloc_size);
  
    *prompts_res = prompts;

    if (user_prompt) {
	(prompts)->id = SASL_CB_USER;
	(prompts)->challenge = "Authorization Name";
	(prompts)->prompt = user_prompt;
	(prompts)->defresult = user_def;

	prompts++;
    }

    if (auth_prompt) {
	(prompts)->id = SASL_CB_AUTHNAME;
	(prompts)->challenge = "Authentication Name";
	(prompts)->prompt = auth_prompt;
	(prompts)->defresult = auth_def;

	prompts++;
    }

    if (pass_prompt) {
	(prompts)->id = SASL_CB_PASS;
	(prompts)->challenge = "Password";
	(prompts)->prompt = pass_prompt;
	(prompts)->defresult = pass_def;

	prompts++;
    }

    if (echo_prompt) {
	(prompts)->id = SASL_CB_ECHOPROMPT;
	(prompts)->challenge = echo_chal;
	(prompts)->prompt = echo_prompt;
	(prompts)->defresult = echo_def;

	prompts++;
    }

    if (realm_prompt) {
	(prompts)->id = SASL_CB_GETREALM;
	(prompts)->challenge = realm_chal;
	(prompts)->prompt = realm_prompt;
	(prompts)->defresult = realm_def;

	prompts++;
    }

    /* add the ending one */
    (prompts)->id = SASL_CB_LIST_END;
    (prompts)->challenge = NULL;
    (prompts)->prompt = NULL;
    (prompts)->defresult = NULL;

    return SASL_OK;
}

void _plug_decode_init(decode_context_t *text,
		       const sasl_utils_t *utils, unsigned int in_maxbuf)
{
    memset(text, 0, sizeof(decode_context_t));

    text->utils = utils;
    text->needsize = 4;
    text->in_maxbuf = in_maxbuf;
}

/*
 * Decode as much of the input as possible (possibly none),
 * using decode_pkt() to decode individual packets.
 */
int _plug_decode(decode_context_t *text,
		 const char *input, unsigned inputlen,
		 char **output,		/* output buffer */
		 unsigned *outputsize,	/* current size of output buffer */
		 unsigned *outputlen,	/* length of data in output buffer */
		 int (*decode_pkt)(void *rock,
				   const char *input, unsigned inputlen,
				   char **output, unsigned *outputlen),
		 void *rock)
{
    unsigned int tocopy;
    unsigned diff;
    char *tmp;
    unsigned tmplen;
    int ret;
    
    *outputlen = 0;

    while (inputlen) { /* more input */
	if (text->needsize) { /* need to get the rest of the 4-byte size */

	    /* copy as many bytes (up to 4) as we have into size buffer */
	    tocopy = (inputlen > text->needsize) ? text->needsize : inputlen;
	    memcpy(text->sizebuf + 4 - text->needsize, input, tocopy);
	    text->needsize -= tocopy;
	
	    input += tocopy;
	    inputlen -= tocopy;
	
	    if (!text->needsize) { /* we have the entire 4-byte size */
		memcpy(&(text->size), text->sizebuf, 4);
		text->size = ntohl(text->size);
		text->cursize = 0;
	    } else {
		/* We do NOT have the entire 4-byte size...
		 * wait for more data */
		return SASL_OK;
	    }
	}

	if (!text->size) /* should never happen */
	    return SASL_FAIL;

	if (text->size > text->in_maxbuf) {
	    text->utils->log(NULL, SASL_LOG_ERR,
			     "encoded packet size too big (%d > %d)",
			     text->size, text->in_maxbuf);
	    return SASL_FAIL;
	}

	if (!text->buffer) {
	    text->buffer = text->utils->malloc(text->in_maxbuf);
	    if (text->buffer == NULL) return SASL_NOMEM;
	}

	diff = text->size - text->cursize; /* bytes needed for full packet */

	if (inputlen < diff) {	/* not a complete packet, need more input */
	    memcpy(text->buffer + text->cursize, input, inputlen);
	    text->cursize += inputlen;
	    return SASL_OK;
	}

	/* copy the rest of the packet */
	memcpy(text->buffer + text->cursize, input, diff);
	input += diff;
	inputlen -= diff;

	/* decode the packet (no need to free tmp) */
	ret = decode_pkt(rock, text->buffer, text->size, &tmp, &tmplen);
	if (ret != SASL_OK) return ret;

	/* append the decoded packet to the output */
	ret = _plug_buf_alloc(text->utils, output, outputsize,
			      *outputlen + tmplen + 1); /* +1 for NUL */
	if (ret != SASL_OK) return ret;

	memcpy(*output + *outputlen, tmp, tmplen);
	*outputlen += tmplen;

	/* protect stupid clients */
	*(*output + *outputlen) = '\0';

	/* reset for the next packet */
	text->needsize = 4;
    }

    return SASL_OK;    
}

void _plug_decode_free(decode_context_t *text)
{
    if (text->buffer) text->utils->free(text->buffer);
}

/* returns the realm we should pretend to be in */
int _plug_parseuser(const sasl_utils_t *utils,
		    char **user, char **realm, const char *user_realm, 
		    const char *serverFQDN, const char *input)
{
    int ret;
    char *r;

    if(!user || !serverFQDN) {
	PARAMERROR( utils );
	return SASL_BADPARAM;
    }

    r = strchr(input, '@');
    if (!r) {
	/* hmmm, the user didn't specify a realm */
	if(user_realm && user_realm[0]) {
	    ret = _plug_strdup(utils, user_realm, realm, NULL);
	} else {
	    /* Default to serverFQDN */
	    ret = _plug_strdup(utils, serverFQDN, realm, NULL);
	}
	
	if (ret == SASL_OK) {
	    ret = _plug_strdup(utils, input, user, NULL);
	}
    } else {
	r++;
	ret = _plug_strdup(utils, r, realm, NULL);
	*--r = '\0';
	*user = utils->malloc(r - input + 1);
	if (*user) {
	    strncpy(*user, input, r - input +1);
	} else {
	    MEMERROR( utils );
	    ret = SASL_NOMEM;
	}
	*r = '@';
    }

    return ret;
}

int _plug_make_fulluser(const sasl_utils_t *utils,
			char **fulluser,
			const char * useronly,
			const char *realm)
{
    if(!fulluser || !useronly || !realm) {
	PARAMERROR( utils );
	return (SASL_BADPARAM);
    }

    *fulluser = utils->malloc (strlen(useronly) + strlen(realm) + 2);
    if (*fulluser == NULL) {
	MEMERROR( utils );
	return (SASL_NOMEM);
    }

    strcpy (*fulluser, useronly);
    strcat (*fulluser, "@");
    strcat (*fulluser, realm);

    return (SASL_OK);
}

char * _plug_get_error_message (const sasl_utils_t *utils,
#ifdef WIN32
				DWORD error
#else
				int error
#endif
				)
{
    char * return_value;
#ifdef WIN32
    LPVOID lpMsgBuf;

    FormatMessage( 
	FORMAT_MESSAGE_ALLOCATE_BUFFER | 
	FORMAT_MESSAGE_FROM_SYSTEM | 
	FORMAT_MESSAGE_IGNORE_INSERTS,
	NULL,
	error,
	MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), /* Default language */
	(LPTSTR) &lpMsgBuf,
	0,
	NULL 
    );

    if (_plug_strdup (utils, lpMsgBuf, &return_value, NULL) != SASL_OK) {
	return_value = NULL;
    }

    LocalFree( lpMsgBuf );
#else /* !WIN32 */
    if (_plug_strdup (utils, strerror(error), &return_value, NULL) != SASL_OK) {
	return_value = NULL;
    }
#endif /* WIN32 */
    return (return_value);
}

void _plug_snprintf_os_info (char * osbuf, int osbuf_len)
{
#ifdef WIN32
    char *sysname;
    sysname = "Unknown Windows";

/* Let's suppose it's still compilable with win2k sdk. So define everythig missing */
#ifndef _WIN32_WINNT_WINXP
# define _WIN32_WINNT_WINXP                  0x0501
#endif
#ifndef _WIN32_WINNT_WS03
# define _WIN32_WINNT_WS03                   0x0502
#endif
#ifndef _WIN32_WINNT_WIN6
# define _WIN32_WINNT_WIN6                   0x0600
#endif
#ifndef _WIN32_WINNT_VISTA
# define _WIN32_WINNT_VISTA                  0x0600
#endif
#ifndef _WIN32_WINNT_WS08
# define _WIN32_WINNT_WS08                   0x0600
#endif
#ifndef _WIN32_WINNT_LONGHORN
# define _WIN32_WINNT_LONGHORN               0x0600
#endif
#ifndef _WIN32_WINNT_WIN7
# define _WIN32_WINNT_WIN7                   0x0601
#endif
#ifndef _WIN32_WINNT_WIN8
# define _WIN32_WINNT_WIN8                   0x0602
#endif
#ifndef _WIN32_WINNT_WINBLUE
# define _WIN32_WINNT_WINBLUE                0x0603
#endif
#ifndef _WIN32_WINNT_WIN10
# define _WIN32_WINNT_WIN10                  0x0A00
#endif

    /* and use IsWindowsVersionOrGreater instead of convenient wrappers by the same reason */
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WIN10), LOBYTE(_WIN32_WINNT_WIN10), 0)) {
        sysname = "Windows 10 or greater";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WINBLUE), LOBYTE(_WIN32_WINNT_WINBLUE), 0)) {
        sysname = "Windows 8.1";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WIN8), LOBYTE(_WIN32_WINNT_WIN8), 0)) {
        sysname = "Windows 8";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WIN7), LOBYTE(_WIN32_WINNT_WIN7), 1)) {
        sysname = "Windows 7 SP1";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WIN7), LOBYTE(_WIN32_WINNT_WIN7), 0)) {
        sysname = "Windows 7";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_VISTA), LOBYTE(_WIN32_WINNT_VISTA), 2)) {
        sysname = "Windows Vista SP2";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_VISTA), LOBYTE(_WIN32_WINNT_VISTA), 1)) {
        sysname = "Windows Vista SP1";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_VISTA), LOBYTE(_WIN32_WINNT_VISTA), 0)) {
        sysname = "Windows Vista";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WINXP), LOBYTE(_WIN32_WINNT_WINXP), 3)) {
        sysname = "Windows XP SP3";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WINXP), LOBYTE(_WIN32_WINNT_WINXP), 2)) {
        sysname = "Windows XP SP2";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WINXP), LOBYTE(_WIN32_WINNT_WINXP), 1)) {
        sysname = "Windows XP SP1";
    } else
    if (IsWindowsVersionOrGreater(HIBYTE(_WIN32_WINNT_WINXP), LOBYTE(_WIN32_WINNT_WINXP), 0)) {
        sysname = "Windows XP";
    }

	snprintf(osbuf, osbuf_len, "%s", sysname);

#else /* !WIN32 */
    struct utsname os;

    uname(&os);
    snprintf(osbuf, osbuf_len, "%s %s", os.sysname, os.release);
#endif /* WIN32 */
}

#if defined(WIN32)
unsigned int plug_sleep (unsigned int seconds)
{
    long dwSec = seconds*1000;
    Sleep (dwSec);
    return 0;
}
#endif
