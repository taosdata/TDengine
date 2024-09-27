/* common.c - Functions that are common to server and clinet
 * Rob Siemborski
 * Tim Martin
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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif
#include <stdarg.h>
#include <ctype.h>
#include <assert.h>

#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>
#include "saslint.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

static const char *implementation_string = "Cyrus SASL";

#define	VSTR0(maj, min, step)	#maj "." #min "." #step
#define	VSTR(maj, min, step)	VSTR0(maj, min, step)
#define	SASL_VERSION_STRING	VSTR(SASL_VERSION_MAJOR, SASL_VERSION_MINOR, \
				SASL_VERSION_STEP)

static int _sasl_getpath(void *context __attribute__((unused)), const char **path);
static int _sasl_getpath_simple(void *context __attribute__((unused)), const char **path);
static int _sasl_getconfpath(void *context __attribute__((unused)), char ** path);
static int _sasl_getconfpath_simple(void *context __attribute__((unused)), const char **path);

#if !defined(WIN32)
static char * _sasl_get_default_unix_path(void *context __attribute__((unused)),
                            char * env_var_name, char * default_value);
#else
/* NB: Always returned allocated value */
static char * _sasl_get_default_win_path(void *context __attribute__((unused)),
                            TCHAR * reg_attr_name, char * default_value);
#endif


/* It turns out to be convenient to have a shared sasl_utils_t */
const sasl_utils_t *sasl_global_utils = NULL;

/* Should be a null-terminated array that lists the available mechanisms */
static char **global_mech_list = NULL;

void *free_mutex = NULL;

int (*_sasl_client_cleanup_hook)(void) = NULL;
int (*_sasl_server_cleanup_hook)(void) = NULL;
int (*_sasl_client_idle_hook)(sasl_conn_t *conn) = NULL;
int (*_sasl_server_idle_hook)(sasl_conn_t *conn) = NULL;

sasl_allocation_utils_t _sasl_allocation_utils={
  (sasl_malloc_t *)  &malloc,
  (sasl_calloc_t *)  &calloc,
  (sasl_realloc_t *) &realloc,
  (sasl_free_t *) &free
};
int _sasl_allocation_locked = 0;

#define SASL_ENCODEV_EXTRA  4096

/* Default getpath/getconfpath callbacks. These can be edited by sasl_set_path(). */
static sasl_callback_t default_getpath_cb = {
    SASL_CB_GETPATH, (sasl_callback_ft)&_sasl_getpath, NULL
};
static sasl_callback_t default_getconfpath_cb = {
    SASL_CB_GETCONFPATH, (sasl_callback_ft)&_sasl_getconfpath, NULL
};

static char * default_plugin_path = NULL;
static char * default_conf_path = NULL;

static int _sasl_global_getopt(void *context,
			       const char *plugin_name,
			       const char *option,
			       const char ** result,
			       unsigned *len);
 
/* Intenal mutex functions do as little as possible (no thread protection) */
static void *sasl_mutex_alloc(void)
{
  return (void *)0x1;
}

static int sasl_mutex_lock(void *mutex __attribute__((unused)))
{
    return SASL_OK;
}

static int sasl_mutex_unlock(void *mutex __attribute__((unused)))
{
    return SASL_OK;
}

static void sasl_mutex_free(void *mutex __attribute__((unused)))
{
    return;
}

sasl_mutex_utils_t _sasl_mutex_utils={
  &sasl_mutex_alloc,
  &sasl_mutex_lock,
  &sasl_mutex_unlock,
  &sasl_mutex_free
};

void sasl_set_mutex(sasl_mutex_alloc_t *n,
		    sasl_mutex_lock_t *l,
		    sasl_mutex_unlock_t *u,
		    sasl_mutex_free_t *d)
{
    /* Disallow mutex function changes once sasl_client_init
       and/or sasl_server_init is called */
    if (_sasl_server_cleanup_hook || _sasl_client_cleanup_hook) {
	return;
    }

    _sasl_mutex_utils.alloc=n;
    _sasl_mutex_utils.lock=l;
    _sasl_mutex_utils.unlock=u;
    _sasl_mutex_utils.free=d;
}

/* copy a string to malloced memory */
int _sasl_strdup(const char *in, char **out, size_t *outlen)
{
  size_t len = strlen(in);
  if (outlen) *outlen = len;
  *out=sasl_ALLOC((unsigned) len + 1);
  if (! *out) return SASL_NOMEM;
  strcpy((char *) *out, in);
  return SASL_OK;
}

/* adds a string to the buffer; reallocing if need be */
int _sasl_add_string(char **out, size_t *alloclen,
		     size_t *outlen, const char *add)
{
  size_t addlen;

  if (add==NULL) add = "(null)";

  addlen=strlen(add); /* only compute once */
  if (_buf_alloc(out, alloclen, (*outlen)+addlen+1)!=SASL_OK)
    return SASL_NOMEM;

  strcpy(*out + *outlen, add);
  *outlen += addlen;

  return SASL_OK;
}

/* a simpler way to set plugin path or configuration file path
 * without the need to set sasl_getpath_t callback.
 *
 * This function can be called before sasl_server_init/sasl_client_init.
 *
 * Don't call this function without locking in a multithreaded application.
 */  
int sasl_set_path (int path_type, char * path)
{
    int result;

    if (path == NULL) {
        return (SASL_FAIL);
    }

    switch (path_type) {
        case SASL_PATH_TYPE_PLUGIN:
            if (default_plugin_path != NULL) {
                sasl_FREE (default_plugin_path);
                default_plugin_path = NULL;
            }
            result = _sasl_strdup (path, &default_plugin_path, NULL);
            if (result != SASL_OK) {
                return (result);
            }

            /* Update the default getpath_t callback */
            default_getpath_cb.proc = (sasl_callback_ft)&_sasl_getpath_simple;
            break;

        case SASL_PATH_TYPE_CONFIG:
            if (default_conf_path != NULL) {
                sasl_FREE (default_conf_path);
                default_conf_path = NULL;
            }
            result = _sasl_strdup (path, &default_conf_path, NULL);
            if (result != SASL_OK) {
                return (result);
            }

            /* Update the default getpath_t callback */
            default_getconfpath_cb.proc = (sasl_callback_ft)&_sasl_getconfpath_simple;
            break;

        default:
            return (SASL_FAIL);
    }

    return (SASL_OK);
}

/* return the version of the cyrus sasl library as compiled,
 * using 32 bits: high byte is major version, second byte is minor version,
 * low 16 bits are step #.
 * Patch version is not available using this function,
 * use sasl_version_info() instead.
 */
void sasl_version(const char **implementation, int *version) 
{
    if(implementation) *implementation = implementation_string;
    /* NB: the format is not the same as in SASL_VERSION_FULL */
    if(version) *version = (SASL_VERSION_MAJOR << 24) | 
		           (SASL_VERSION_MINOR << 16) |
		           (SASL_VERSION_STEP);
}

/* Extended version of sasl_version above */
void sasl_version_info (const char **implementation, const char **version_string,
		    int *version_major, int *version_minor, int *version_step,
		    int *version_patch)
{
    if (implementation) *implementation = implementation_string;
    if (version_string) *version_string = SASL_VERSION_STRING;
    if (version_major) *version_major = SASL_VERSION_MAJOR;
    if (version_minor) *version_minor = SASL_VERSION_MINOR;
    if (version_step) *version_step = SASL_VERSION_STEP;
    /* Version patch is always 0 for CMU SASL */
    if (version_patch) *version_patch = 0;
}

/* security-encode a regular string.  Mostly a wrapper for sasl_encodev */
/* output is only valid until next call to sasl_encode or sasl_encodev */
int sasl_encode(sasl_conn_t *conn, const char *input,
		unsigned inputlen,
		const char **output, unsigned *outputlen)
{
    int result;
    struct iovec tmp;

    if(!conn) return SASL_BADPARAM;
    if(!input || !inputlen || !output || !outputlen)
	PARAMERROR(conn);
    
    /* maxoutbuf checking is done in sasl_encodev */

    /* Note: We are casting a const pointer here, but it's okay
     * because we believe people downstream of us are well-behaved, and the
     * alternative is an absolute mess, performance-wise. */
    tmp.iov_base = (void *)input;
    tmp.iov_len = inputlen;
    
    result = sasl_encodev(conn, &tmp, 1, output, outputlen);

    RETURN(conn, result);
}

/* Internal function that doesn't do any verification */
static int
_sasl_encodev (sasl_conn_t *conn,
	       const struct iovec *invec,
               unsigned numiov,
               int * p_num_packets,     /* number of packets generated so far */
	       const char **output,     /* previous output, if *p_num_packets > 0 */
               unsigned *outputlen)
{
    int result;
    char * new_buf;

    assert (conn->oparams.encode != NULL);

    if (*p_num_packets == 1) {
        /* This is the second call to this function,
           so we need to allocate a new output buffer
           and copy existing data there. */
        conn->multipacket_encoded_data.curlen = *outputlen;
        if (conn->multipacket_encoded_data.data == NULL) {
            conn->multipacket_encoded_data.reallen = 
                 conn->multipacket_encoded_data.curlen + SASL_ENCODEV_EXTRA;
            conn->multipacket_encoded_data.data =
                 sasl_ALLOC(conn->multipacket_encoded_data.reallen + 1);

            if (conn->multipacket_encoded_data.data == NULL) {
                MEMERROR(conn);
            }
        } else {
            /* A buffer left from a previous sasl_encodev call.
               Make sure it is big enough. */
            if (conn->multipacket_encoded_data.curlen >
                conn->multipacket_encoded_data.reallen) {
                conn->multipacket_encoded_data.reallen = 
                    conn->multipacket_encoded_data.curlen + SASL_ENCODEV_EXTRA;

	        new_buf = sasl_REALLOC(conn->multipacket_encoded_data.data,
                            conn->multipacket_encoded_data.reallen + 1);
                if (new_buf == NULL) {
                    MEMERROR(conn);
                }
                conn->multipacket_encoded_data.data = new_buf;
            }
        }

        memcpy (conn->multipacket_encoded_data.data,
                *output,
                *outputlen);
    }

    result = conn->oparams.encode(conn->context,
                                  invec,
                                  numiov,
				  output,
                                  outputlen);

    if (*p_num_packets > 0 && result == SASL_OK) {
        /* Is the allocated buffer big enough? If not, grow it. */
        if ((conn->multipacket_encoded_data.curlen + *outputlen) >
             conn->multipacket_encoded_data.reallen) {
            conn->multipacket_encoded_data.reallen =
                conn->multipacket_encoded_data.curlen + *outputlen;
	    new_buf = sasl_REALLOC(conn->multipacket_encoded_data.data,
                        conn->multipacket_encoded_data.reallen + 1);
            if (new_buf == NULL) {
                MEMERROR(conn);
            }
            conn->multipacket_encoded_data.data = new_buf;
        }

        /* Append new data to the end of the buffer */
        memcpy (conn->multipacket_encoded_data.data +
                conn->multipacket_encoded_data.curlen,
                *output,
                *outputlen);
        conn->multipacket_encoded_data.curlen += *outputlen;

        *output = conn->multipacket_encoded_data.data;
        *outputlen = (unsigned)conn->multipacket_encoded_data.curlen;
    }

    (*p_num_packets)++;

    RETURN(conn, result);
}

/* security-encode an iovec */
/* output is only valid until the next call to sasl_encode or sasl_encodev */
int sasl_encodev(sasl_conn_t *conn,
		 const struct iovec *invec,
                 unsigned numiov,
		 const char **output,
                 unsigned *outputlen)
{
    int result = SASL_OK;
    unsigned i;
    unsigned j;
    size_t total_size = 0;
    struct iovec *cur_invec = NULL;
    struct iovec last_invec;
    unsigned cur_numiov;
    char * next_buf = NULL;
    size_t remainder_len;
    unsigned index_offset;
    unsigned allocated = 0;
    /* Number of generated SASL packets */
    int num_packets = 0;

    if (!conn) return SASL_BADPARAM;
    if (! invec || ! output || ! outputlen || numiov < 1) {
	PARAMERROR(conn);
    }

    if (!conn->props.maxbufsize) {
	sasl_seterror(conn, 0,
		      "called sasl_encode[v] with application that does not support security layers");
	return SASL_TOOWEAK;
    }

    /* If oparams.encode is NULL, this means there is no SASL security
       layer in effect, so no SASL framing is needed. */
    if (conn->oparams.encode == NULL)  {
	result = _iovec_to_buf(invec, numiov, &conn->encode_buf);
	if (result != SASL_OK) INTERROR(conn, result);
       
	*output = conn->encode_buf->data;
	*outputlen = (unsigned) conn->encode_buf->curlen;

        RETURN(conn, result);
    }

    /* This might be better to check on a per-plugin basis, but I think
     * it's cleaner and more effective here.  It also encourages plugins
     * to be honest about what they accept */

    last_invec.iov_base = NULL;
    remainder_len = 0;
    next_buf = NULL;
    i = 0;
    while (i < numiov) {
        if ((total_size + invec[i].iov_len) > conn->oparams.maxoutbuf) {

            /* CLAIM: total_size < conn->oparams.maxoutbuf */
            
            /* Fit as many bytes in last_invec, so that we have conn->oparams.maxoutbuf
               bytes in total. */
            last_invec.iov_len = conn->oparams.maxoutbuf - total_size;
            /* Point to the first byte of the current record. */
            last_invec.iov_base = invec[i].iov_base;

            /* Note that total_size < conn->oparams.maxoutbuf */
            /* The total size of the iov is bigger then the other end can accept.
               So we allocate a new iov that contains just enough. */

            /* +1 --- for the tail record */
            cur_numiov = i + 1;

            /* +1 --- just in case we need the head record */
            if ((cur_numiov + 1) > allocated) {
                struct iovec *new_invec;

                allocated = cur_numiov + 1;
                new_invec = sasl_REALLOC (cur_invec, sizeof(struct iovec) * allocated);
                if (new_invec == NULL) {
                    if (cur_invec != NULL) {
                        sasl_FREE(cur_invec);
                    }
                    MEMERROR(conn);
                }
                cur_invec = new_invec;
            }

            if (next_buf != NULL) {
                cur_invec[0].iov_base = next_buf;
                cur_invec[0].iov_len = (long)remainder_len;
                cur_numiov++;
                index_offset = 1;
            } else {
                index_offset = 0;
            }

            if (i > 0) {
                /* Copy all previous chunks */
                /* NOTE - The starting index in invec is always 0 */
                for (j = 0; j < i; j++) {
                    cur_invec[j + index_offset] = invec[j];
                }
            }

            /* Initialize the last record */
            cur_invec[i + index_offset] = last_invec;

            result = _sasl_encodev (conn,
	                            cur_invec,
                                    cur_numiov,
                                    &num_packets,
	                            output,
                                    outputlen);

            if (result != SASL_OK) {
                goto cleanup;
            }

            /* Point to the first byte that wouldn't fit into
               the conn->oparams.maxoutbuf buffer. */
            /* Note, if next_buf points to the very end of the IOV record,
               it will be reset to NULL below */
            /* Note, that some platforms define iov_base as "void *",
               thus the typecase below */
            next_buf = (char *) last_invec.iov_base + last_invec.iov_len;
            /* Note - remainder_len is how many bytes left to be encoded in
               the current IOV slot. */
            remainder_len = (total_size + invec[i].iov_len) - conn->oparams.maxoutbuf;

            /* Skip all consumed IOV records */
            invec += i + 1;
            numiov = numiov - (i + 1);
            i = 0;

            while (remainder_len > conn->oparams.maxoutbuf) {
                last_invec.iov_base = next_buf;
                last_invec.iov_len = conn->oparams.maxoutbuf;

                /* Note, if next_buf points to the very end of the IOV record,
                   it will be reset to NULL below */
                /* Note, that some platforms define iov_base as "void *",
                   thus the typecase below */
                next_buf = (char *) last_invec.iov_base + last_invec.iov_len;
                remainder_len = remainder_len - conn->oparams.maxoutbuf;

                result = _sasl_encodev (conn,
	                                &last_invec,
                                        1,
                                        &num_packets,
	                                output,
                                        outputlen);
                if (result != SASL_OK) {
                    goto cleanup;
                }
            }

	    total_size = remainder_len;

            if (remainder_len == 0) {
                /* Just clear next_buf */
                next_buf = NULL;
            }
        } else {
	    total_size += invec[i].iov_len;
            i++;
        }
    }

    /* CLAIM - The remaining data is shorter then conn->oparams.maxoutbuf. */

    /* Force encoding of any partial buffer. Might not be optimal on the wire. */
    if (next_buf != NULL) {
        last_invec.iov_base = next_buf;
        last_invec.iov_len = (long)remainder_len;

        result = _sasl_encodev (conn,
	                        &last_invec,
                                1,
                                &num_packets,
	                        output,
                                outputlen);

        if (result != SASL_OK) {
            goto cleanup;
        }
    }

    if (numiov > 0) {
        result = _sasl_encodev (conn,
	                        invec,
                                numiov,
                                &num_packets,
	                        output,
                                outputlen);
    }

cleanup:
    if (cur_invec != NULL) {
        sasl_FREE(cur_invec);
    }

    RETURN(conn, result);
}
 
/* output is only valid until next call to sasl_decode */
int sasl_decode(sasl_conn_t *conn,
		const char *input, unsigned inputlen,
		const char **output, unsigned *outputlen)
{
    int result;

    if(!conn) return SASL_BADPARAM;
    if(!input || !output || !outputlen)
	PARAMERROR(conn);

    if(!conn->props.maxbufsize) {
	sasl_seterror(conn, 0,
		      "called sasl_decode with application that does not support security layers");
	RETURN(conn, SASL_TOOWEAK);
    }

    if(conn->oparams.decode == NULL)
    {
	/* Since we know how long the output is maximally, we can
	 * just allocate it to begin with, and never need another
         * allocation! */

	/* However, if they pass us more than they actually can take,
	 * we cannot help them... */
	if(inputlen > conn->props.maxbufsize) {
	    sasl_seterror(conn, 0,
			  "input too large for default sasl_decode");
	    RETURN(conn,SASL_BUFOVER);
	}

	if(!conn->decode_buf)
	    conn->decode_buf = sasl_ALLOC(conn->props.maxbufsize + 1);
	if(!conn->decode_buf)	
	    MEMERROR(conn);
	
	memcpy(conn->decode_buf, input, inputlen);
	conn->decode_buf[inputlen] = '\0';
	*output = conn->decode_buf;
	*outputlen = inputlen;
	
        return SASL_OK;
    } else {
        result = conn->oparams.decode(conn->context, input, inputlen,
                                      output, outputlen);

	/* NULL an empty buffer (for misbehaved applications) */
	if (*outputlen == 0) *output = NULL;

        RETURN(conn, result);
    }

    INTERROR(conn, SASL_FAIL);
}


void
sasl_set_alloc(sasl_malloc_t *m,
	       sasl_calloc_t *c,
	       sasl_realloc_t *r,
	       sasl_free_t *f)
{
  if (_sasl_allocation_locked++)  return;

  _sasl_allocation_utils.malloc=m;
  _sasl_allocation_utils.calloc=c;
  _sasl_allocation_utils.realloc=r;
  _sasl_allocation_utils.free=f;
}

void sasl_common_done(void)
{
    /* NOTE - the caller will need to reinitialize the values,
       if it is going to call sasl_client_init/sasl_server_init again. */
    if (default_plugin_path != NULL) {
	sasl_FREE (default_plugin_path);
	default_plugin_path = NULL;
    }
    if (default_conf_path != NULL) {
	sasl_FREE (default_conf_path);
	default_conf_path = NULL;
    }

    _sasl_canonuser_free();
    _sasl_done_with_plugins();
    
    sasl_MUTEX_FREE(free_mutex);
    free_mutex = NULL;
    
    _sasl_free_utils(&sasl_global_utils);
    
    if (global_mech_list) {
	sasl_FREE(global_mech_list);
	global_mech_list = NULL;
    }
}

/* This function is for backward compatibility */
void sasl_done(void)
{
    if (_sasl_server_cleanup_hook && _sasl_server_cleanup_hook() == SASL_OK) {
	_sasl_server_idle_hook = NULL;
	_sasl_server_cleanup_hook = NULL;
    }
    
    if (_sasl_client_cleanup_hook && _sasl_client_cleanup_hook() == SASL_OK) {
	_sasl_client_idle_hook = NULL;	
	_sasl_client_cleanup_hook = NULL;
    }
    
    if (_sasl_server_cleanup_hook || _sasl_client_cleanup_hook) {
	return;
    }

    sasl_common_done();
}

/* fills in the base sasl_conn_t info */
int _sasl_conn_init(sasl_conn_t *conn,
		    const char *service,
		    unsigned int flags,
		    enum Sasl_conn_type type,
		    int (*idle_hook)(sasl_conn_t *conn),
		    const char *serverFQDN,
		    const char *iplocalport,
		    const char *ipremoteport,
		    const sasl_callback_t *callbacks,
		    const sasl_global_callbacks_t *global_callbacks) {
  int result = SASL_OK;

  conn->type = type;

  result = _sasl_strdup(service, &conn->service, NULL);
  if (result != SASL_OK) 
      MEMERROR(conn);

  memset(&conn->oparams, 0, sizeof(sasl_out_params_t));
  memset(&conn->external, 0, sizeof(_sasl_external_properties_t));

  conn->flags = flags;

  result = sasl_setprop(conn, SASL_IPLOCALPORT, iplocalport);
  if(result != SASL_OK)
      RETURN(conn, result);
  
  result = sasl_setprop(conn, SASL_IPREMOTEPORT, ipremoteport);
  if(result != SASL_OK)
      RETURN(conn, result);
  
  conn->encode_buf = NULL;
  conn->context = NULL;
  conn->secret = NULL;
  conn->idle_hook = idle_hook;
  conn->callbacks = callbacks;
  conn->global_callbacks = global_callbacks;

  memset(&conn->props, 0, sizeof(conn->props));

  /* Start this buffer out as an empty string */
  conn->error_code = SASL_OK;
  conn->errdetail_buf = conn->error_buf = NULL;
  conn->errdetail_buf_len = conn->error_buf_len = 150;

  result = _buf_alloc(&conn->error_buf, &conn->error_buf_len, 150);     
  if(result != SASL_OK) MEMERROR(conn);
  result = _buf_alloc(&conn->errdetail_buf, &conn->errdetail_buf_len, 150);
  if(result != SASL_OK) MEMERROR(conn);
  
  conn->error_buf[0] = '\0';
  conn->errdetail_buf[0] = '\0';
  
  conn->decode_buf = NULL;

  if(serverFQDN) {
      result = _sasl_strdup(serverFQDN, &conn->serverFQDN, NULL);
      sasl_strlower (conn->serverFQDN);
  } else if (conn->type == SASL_CONN_SERVER) {
      /* We can fake it because we *are* the server */
      char name[MAXFQDNLEN];
      memset(name, 0, sizeof(name));
      if (get_fqhostname (name, MAXFQDNLEN, 0) != 0) {
        return (SASL_FAIL);
      }
      
      result = _sasl_strdup(name, &conn->serverFQDN, NULL);
  } else {
      conn->serverFQDN = NULL;
  }
  

  if(result != SASL_OK) MEMERROR( conn );

  RETURN(conn, SASL_OK);
}

int _sasl_common_init(sasl_global_callbacks_t *global_callbacks)
{
    int result;

    /* The last specified global callback always wins */
    if (sasl_global_utils != NULL) {
	sasl_utils_t * global_utils = (sasl_utils_t *)sasl_global_utils;
	global_utils->getopt = &_sasl_global_getopt;
	global_utils->getopt_context = global_callbacks;
    }

    /* Do nothing if we are already initialized */
    if (free_mutex) {
	return SASL_OK;
    }

    /* Setup the global utilities */
    if(!sasl_global_utils) {
	sasl_global_utils = _sasl_alloc_utils(NULL, global_callbacks);
	if(sasl_global_utils == NULL) return SASL_NOMEM;
    }

    /* Init the canon_user plugin */
    result = sasl_canonuser_add_plugin("INTERNAL", internal_canonuser_init);
    if(result != SASL_OK) return result;    

    if (!free_mutex) {
	free_mutex = sasl_MUTEX_ALLOC();
    }
    if (!free_mutex) return SASL_FAIL;

    return SASL_OK;
}

/* dispose connection state, sets it to NULL
 *  checks for pointer to NULL
 */
void sasl_dispose(sasl_conn_t **pconn)
{
  int result;

  if (! pconn) return;
  if (! *pconn) return;

  /* serialize disposes. this is necessary because we can't
     dispose of conn->mutex if someone else is locked on it */
  if (!free_mutex) {
      free_mutex = sasl_MUTEX_ALLOC();
      if (!free_mutex) return;
  }

  result = sasl_MUTEX_LOCK(free_mutex);
  if (result!=SASL_OK) return;
  
  /* *pconn might have become NULL by now */
  if (*pconn) {
      (*pconn)->destroy_conn(*pconn);
      sasl_FREE(*pconn);
      *pconn=NULL;
  }

  sasl_MUTEX_UNLOCK(free_mutex);
}

void _sasl_conn_dispose(sasl_conn_t *conn) {
  if (conn->serverFQDN)
      sasl_FREE(conn->serverFQDN);

  if (conn->external.auth_id)
      sasl_FREE(conn->external.auth_id);

  if(conn->encode_buf) {
      if(conn->encode_buf->data) sasl_FREE(conn->encode_buf->data);
      sasl_FREE(conn->encode_buf);
  }

  if(conn->error_buf)
      sasl_FREE(conn->error_buf);
  
  if(conn->errdetail_buf)
      sasl_FREE(conn->errdetail_buf);

  if(conn->decode_buf)
      sasl_FREE(conn->decode_buf);

  if(conn->mechlist_buf)
      sasl_FREE(conn->mechlist_buf);

  if(conn->service)
      sasl_FREE(conn->service);

  if (conn->multipacket_encoded_data.data) {
      sasl_FREE(conn->multipacket_encoded_data.data);
  }

  /* oparams sub-members should be freed by the plugin, in so much
   * as they were allocated by the plugin */
}


/* get property from SASL connection state
 *  propnum       -- property number
 *  pvalue        -- pointer to value
 * returns:
 *  SASL_OK       -- no error
 *  SASL_NOTDONE  -- property not available yet
 *  SASL_BADPARAM -- bad property number or SASL context is NULL
 */
int sasl_getprop(sasl_conn_t *conn, int propnum, const void **pvalue)
{
  int result = SASL_OK;
  sasl_getopt_t *getopt;
  void *context;
  
  if (! conn) return SASL_BADPARAM;
  if (! pvalue) PARAMERROR(conn);

  switch(propnum)
  {
  case SASL_SSF:
      *(sasl_ssf_t **)pvalue= &conn->oparams.mech_ssf;
      break;      
  case SASL_MAXOUTBUF:
      *(unsigned **)pvalue = &conn->oparams.maxoutbuf;
      break;
  case SASL_GETOPTCTX:
      result = _sasl_getcallback(conn, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context);
      if(result != SASL_OK) break;
      
      *(void **)pvalue = context;
      break;
  case SASL_CALLBACK:
      *(const sasl_callback_t **)pvalue = conn->callbacks;
      break;
  case SASL_IPLOCALPORT:
      if(conn->got_ip_local)
	  *(const char **)pvalue = conn->iplocalport;
      else {
	  *(const char **)pvalue = NULL;
	  result = SASL_NOTDONE;
      }
      break;
  case SASL_IPREMOTEPORT:
      if(conn->got_ip_remote)
	  *(const char **)pvalue = conn->ipremoteport;
      else {
	  *(const char **)pvalue = NULL;
	  result = SASL_NOTDONE;
      }	  
      break;
  case SASL_USERNAME:
      if(! conn->oparams.user)
	  result = SASL_NOTDONE;
      else
	  *((const char **)pvalue) = conn->oparams.user;
      break;
  case SASL_AUTHUSER:
      if(! conn->oparams.authid)
	  result = SASL_NOTDONE;
      else
	  *((const char **)pvalue) = conn->oparams.authid;
      break;
  case SASL_APPNAME:
      /* Currently we only support server side contexts, but we should
         be able to extend this to support client side contexts as well */
      if(conn->type != SASL_CONN_SERVER) result = SASL_BADPROT;
      else
	  *((const char **)pvalue) = ((sasl_server_conn_t *)conn)->sparams->appname;
      break;
  case SASL_SERVERFQDN:
      *((const char **)pvalue) = conn->serverFQDN;
      break;
  case SASL_DEFUSERREALM:
      if(conn->type != SASL_CONN_SERVER) result = SASL_BADPROT;
      else
	  *((const char **)pvalue) = ((sasl_server_conn_t *)conn)->user_realm;
      break;
  case SASL_SERVICE:
      *((const char **)pvalue) = conn->service;
      break;
  case SASL_AUTHSOURCE: /* name of plugin (not name of mech) */
      if(conn->type == SASL_CONN_CLIENT) {
	  if(!((sasl_client_conn_t *)conn)->mech) {
	      result = SASL_NOTDONE;
	      break;
	  }
	  *((const char **)pvalue) =
	      ((sasl_client_conn_t *)conn)->mech->m.plugname;
      } else if (conn->type == SASL_CONN_SERVER) {
	  if(!((sasl_server_conn_t *)conn)->mech) {
	      result = SASL_NOTDONE;
	      break;
	  }
	  *((const char **)pvalue) =
	      ((sasl_server_conn_t *)conn)->mech->m.plugname;
      } else {
	  result = SASL_BADPARAM;
      }
      break;
  case SASL_MECHNAME: /* name of mech */
      if(conn->type == SASL_CONN_CLIENT) {
	  if(!((sasl_client_conn_t *)conn)->mech) {
	      result = SASL_NOTDONE;
	      break;
	  }
	  *((const char **)pvalue) =
	      ((sasl_client_conn_t *)conn)->mech->m.plug->mech_name;
      } else if (conn->type == SASL_CONN_SERVER) {
	  if(!((sasl_server_conn_t *)conn)->mech) {
	      result = SASL_NOTDONE;
	      break;
	  }
	  *((const char **)pvalue) =
	      ((sasl_server_conn_t *)conn)->mech->m.plug->mech_name;
      } else {
	  result = SASL_BADPARAM;
      }
      
      if(!(*pvalue) && result == SASL_OK) result = SASL_NOTDONE;
      break;
  case SASL_PLUGERR:
      *((const char **)pvalue) = conn->error_buf;
      break;
  case SASL_DELEGATEDCREDS:
      /* We can't really distinguish between "no delegated credentials"
         and "authentication not finished" */
      if(! conn->oparams.client_creds)
	  result = SASL_NOTDONE;
      else
	  *((const char **)pvalue) = conn->oparams.client_creds;
      break;
  case SASL_GSS_PEER_NAME:
      if(! conn->oparams.gss_peer_name)
	  result = SASL_NOTDONE;
      else
	  *((const char **)pvalue) = conn->oparams.gss_peer_name;
      break;
  case SASL_GSS_LOCAL_NAME:
      if(! conn->oparams.gss_local_name)
	  result = SASL_NOTDONE;
      else
	  *((const char **)pvalue) = conn->oparams.gss_local_name;
      break;
  case SASL_SSF_EXTERNAL:
      *((const sasl_ssf_t **)pvalue) = &conn->external.ssf;
      break;
  case SASL_AUTH_EXTERNAL:
      *((const char **)pvalue) = conn->external.auth_id;
      break;
  case SASL_SEC_PROPS:
      *((const sasl_security_properties_t **)pvalue) = &conn->props;
      break;
  case SASL_GSS_CREDS:
      if(conn->type == SASL_CONN_CLIENT)
	  *(const void **)pvalue = 
              ((sasl_client_conn_t *)conn)->cparams->gss_creds;
      else
	  *(const void **)pvalue = 
              ((sasl_server_conn_t *)conn)->sparams->gss_creds;
      break;
  case SASL_HTTP_REQUEST: {
      if (conn->type == SASL_CONN_SERVER)
	  *(const sasl_http_request_t **)pvalue =
	      ((sasl_server_conn_t *)conn)->sparams->http_request;
      else
	  *(const sasl_http_request_t **)pvalue =
	      ((sasl_client_conn_t *)conn)->cparams->http_request;
      break;
  }
  default: 
      result = SASL_BADPARAM;
  }

  if(result == SASL_BADPARAM) {
      PARAMERROR(conn);
  } else if(result == SASL_NOTDONE) {
      sasl_seterror(conn, SASL_NOLOG,
		    "Information that was requested is not yet available.");
      RETURN(conn, result);
  } else if(result != SASL_OK) {
      INTERROR(conn, result);
  } else
      RETURN(conn, result); 
}

/* set property in SASL connection state
 * returns:
 *  SASL_OK       -- value set
 *  SASL_BADPARAM -- invalid property or value
 */
int sasl_setprop(sasl_conn_t *conn, int propnum, const void *value)
{
  int result = SASL_OK;
  char *str;

  /* make sure the sasl context is valid */
  if (!conn)
    return SASL_BADPARAM;

  switch(propnum)
  {
  case SASL_SSF_EXTERNAL:
      conn->external.ssf = *((sasl_ssf_t *)value);
      if(conn->type == SASL_CONN_SERVER) {
	((sasl_server_conn_t*)conn)->sparams->external_ssf =
	  conn->external.ssf;
      } else {
	((sasl_client_conn_t*)conn)->cparams->external_ssf =
	  conn->external.ssf;
      }
      break;

  case SASL_AUTH_EXTERNAL:
      if(value && strlen(value)) {
	  result = _sasl_strdup(value, &str, NULL);
	  if(result != SASL_OK) MEMERROR(conn);
      } else {
	  str = NULL;
      }

      if(conn->external.auth_id)
	  sasl_FREE(conn->external.auth_id);

      conn->external.auth_id = str;

      break;

  case SASL_DEFUSERREALM:
      if(conn->type != SASL_CONN_SERVER) {
	sasl_seterror(conn, 0, "Tried to set realm on non-server connection");
	result = SASL_BADPROT;
	break;
      }

      if(value && strlen(value)) {
	  result = _sasl_strdup(value, &str, NULL);
	  if(result != SASL_OK) MEMERROR(conn);
      } else {
	  PARAMERROR(conn);
      }

      if(((sasl_server_conn_t *)conn)->user_realm)
      	  sasl_FREE(((sasl_server_conn_t *)conn)->user_realm);

      ((sasl_server_conn_t *)conn)->user_realm = str;
      ((sasl_server_conn_t *)conn)->sparams->user_realm = str;

      break;

  case SASL_SEC_PROPS:
  {
      sasl_security_properties_t *props = (sasl_security_properties_t *)value;

      if(props->maxbufsize == 0 && props->min_ssf != 0) {
	  sasl_seterror(conn, 0,
			"Attempt to disable security layers (maxoutbuf == 0) with min_ssf > 0");
	  RETURN(conn, SASL_TOOWEAK);
      }

      conn->props = *props;

      if(conn->type == SASL_CONN_SERVER) {
	((sasl_server_conn_t*)conn)->sparams->props = *props;
      } else {
	((sasl_client_conn_t*)conn)->cparams->props = *props;
      }

      break;
  }
      
  case SASL_IPREMOTEPORT:
  {
      const char *ipremoteport = (const char *)value;
      if(!value) {
	  conn->got_ip_remote = 0; 
      } else if (_sasl_ipfromstring(ipremoteport, NULL, 0)
		 != SASL_OK) {
	  sasl_seterror(conn, 0, "Bad IPREMOTEPORT value");
	  RETURN(conn, SASL_BADPARAM);
      } else {
	  strcpy(conn->ipremoteport, ipremoteport);
	  conn->got_ip_remote = 1;
      }
      
      if(conn->got_ip_remote) {
	  if(conn->type == SASL_CONN_CLIENT) {
	      ((sasl_client_conn_t *)conn)->cparams->ipremoteport
		  = conn->ipremoteport;
	      ((sasl_client_conn_t *)conn)->cparams->ipremlen =
		  (unsigned) strlen(conn->ipremoteport);
	  } else if (conn->type == SASL_CONN_SERVER) {
	      ((sasl_server_conn_t *)conn)->sparams->ipremoteport
		  = conn->ipremoteport;
	      ((sasl_server_conn_t *)conn)->sparams->ipremlen =
		  (unsigned) strlen(conn->ipremoteport);
	  }
      } else {
	  if(conn->type == SASL_CONN_CLIENT) {
	      ((sasl_client_conn_t *)conn)->cparams->ipremoteport
		  = NULL;
	      ((sasl_client_conn_t *)conn)->cparams->ipremlen = 0;
	  } else if (conn->type == SASL_CONN_SERVER) {
	      ((sasl_server_conn_t *)conn)->sparams->ipremoteport
		  = NULL;	      
	      ((sasl_server_conn_t *)conn)->sparams->ipremlen = 0;
	  }
      }

      break;
  }

  case SASL_IPLOCALPORT:
  {
      const char *iplocalport = (const char *)value;
      if(!value) {
	  conn->got_ip_local = 0;	  
      } else if (_sasl_ipfromstring(iplocalport, NULL, 0)
		 != SASL_OK) {
	  sasl_seterror(conn, 0, "Bad IPLOCALPORT value");
	  RETURN(conn, SASL_BADPARAM);
      } else {
	  strcpy(conn->iplocalport, iplocalport);
	  conn->got_ip_local = 1;
      }

      if(conn->got_ip_local) {
	  if(conn->type == SASL_CONN_CLIENT) {
	      ((sasl_client_conn_t *)conn)->cparams->iplocalport
		  = conn->iplocalport;
	      ((sasl_client_conn_t *)conn)->cparams->iploclen
		  = (unsigned) strlen(conn->iplocalport);
	  } else if (conn->type == SASL_CONN_SERVER) {
	      ((sasl_server_conn_t *)conn)->sparams->iplocalport
		  = conn->iplocalport;
	      ((sasl_server_conn_t *)conn)->sparams->iploclen
		  = (unsigned) strlen(conn->iplocalport);
	  }
      } else {
	  if(conn->type == SASL_CONN_CLIENT) {
	      ((sasl_client_conn_t *)conn)->cparams->iplocalport
		  = NULL;
	      ((sasl_client_conn_t *)conn)->cparams->iploclen = 0;
	  } else if (conn->type == SASL_CONN_SERVER) {
	      ((sasl_server_conn_t *)conn)->sparams->iplocalport
		  = NULL;
	      ((sasl_server_conn_t *)conn)->sparams->iploclen = 0;
	  }
      }
      break;
  }

  case SASL_APPNAME:
      /* Currently we only support server side contexts, but we should
         be able to extend this to support client side contexts as well */
      if(conn->type != SASL_CONN_SERVER) {
	sasl_seterror(conn, 0, "Tried to set application name on non-server connection");
	result = SASL_BADPROT;
	break;
      }

      if(((sasl_server_conn_t *)conn)->appname) {
      	  sasl_FREE(((sasl_server_conn_t *)conn)->appname);
	  ((sasl_server_conn_t *)conn)->appname = NULL;
      }

      if(value && strlen(value)) {
	  result = _sasl_strdup(value,
				&(((sasl_server_conn_t *)conn)->appname),
				NULL);
	  if(result != SASL_OK) MEMERROR(conn);
	  ((sasl_server_conn_t *)conn)->sparams->appname =
              ((sasl_server_conn_t *)conn)->appname;
	  ((sasl_server_conn_t *)conn)->sparams->applen =
	      (unsigned) strlen(((sasl_server_conn_t *)conn)->appname);
      } else {
	  ((sasl_server_conn_t *)conn)->sparams->appname = NULL;
	  ((sasl_server_conn_t *)conn)->sparams->applen = 0;
      }
      break;

  case SASL_GSS_CREDS:
      if(conn->type == SASL_CONN_CLIENT)
          ((sasl_client_conn_t *)conn)->cparams->gss_creds = value;
      else
          ((sasl_server_conn_t *)conn)->sparams->gss_creds = value;
      break;

  case SASL_CHANNEL_BINDING: {
    const struct sasl_channel_binding *cb = (const struct sasl_channel_binding *)value;

    if (conn->type == SASL_CONN_SERVER)
        ((sasl_server_conn_t *)conn)->sparams->cbinding = cb;
    else
        ((sasl_client_conn_t *)conn)->cparams->cbinding = cb;
    break;
  }

  case SASL_HTTP_REQUEST: {
      const sasl_http_request_t *req = (const sasl_http_request_t *)value;

      if (conn->type == SASL_CONN_SERVER)
	  ((sasl_server_conn_t *)conn)->sparams->http_request = req;
      else
	  ((sasl_client_conn_t *)conn)->cparams->http_request = req;
      break;
  }

  default:
      sasl_seterror(conn, 0, "Unknown parameter type");
      result = SASL_BADPARAM;
  }
  
  RETURN(conn, result);
}

/* this is apparently no longer a user function */
static int sasl_usererr(int saslerr)
{
    /* Hide the difference in a username failure and a password failure */
    if (saslerr == SASL_NOUSER)
	return SASL_BADAUTH;

    /* otherwise return the error given; no transform necessary */
    return saslerr;
}

const char *sasl_errstring(int saslerr,
			   const char *langlist __attribute__((unused)),
			   const char **outlang)
{
  if (outlang) *outlang="en-us";

  switch(saslerr)
    {
    case SASL_CONTINUE: return "another step is needed in authentication";
    case SASL_OK:       return "successful result";
    case SASL_FAIL:     return "generic failure";
    case SASL_NOMEM:    return "no memory available";
    case SASL_BUFOVER:  return "overflowed buffer";
    case SASL_NOMECH:   return "no mechanism available";
    case SASL_BADPROT:  return "bad protocol / cancel";
    case SASL_NOTDONE:  return "can't request information until later in exchange";
    case SASL_BADPARAM: return "invalid parameter supplied";
    case SASL_TRYAGAIN: return "transient failure (e.g., weak key)";
    case SASL_BADMAC:   return "integrity check failed";
    case SASL_NOTINIT:  return "SASL library is not initialized";
                             /* -- client only codes -- */
    case SASL_INTERACT:   return "needs user interaction";
    case SASL_BADSERV:    return "server failed mutual authentication step";
    case SASL_WRONGMECH:  return "mechanism doesn't support requested feature";
                             /* -- server only codes -- */
    case SASL_BADAUTH:    return "authentication failure";
    case SASL_NOAUTHZ:    return "authorization failure";
    case SASL_TOOWEAK:    return "mechanism too weak for this user";
    case SASL_ENCRYPT:    return "encryption needed to use mechanism";
    case SASL_TRANS:      return "One time use of a plaintext password will enable requested mechanism for user";
    case SASL_EXPIRED:    return "passphrase expired, has to be reset";
    case SASL_DISABLED:   return "account disabled";
    case SASL_NOUSER:     return "user not found";
    case SASL_BADVERS:    return "version mismatch with plug-in";
    case SASL_UNAVAIL:    return "remote authentication server unavailable";
    case SASL_NOVERIFY:   return "user exists, but no verifier for user";
    case SASL_PWLOCK:     return "passphrase locked";
    case SASL_NOCHANGE:   return "requested change was not needed";
    case SASL_WEAKPASS:   return "passphrase is too weak for security policy";
    case SASL_NOUSERPASS: return "user supplied passwords are not permitted";
    case SASL_NEED_OLD_PASSWD: return "sasl_setpass needs old password in order "
				"to perform password change";
    case SASL_CONSTRAINT_VIOLAT: return "sasl_setpass can't store a property because "
			        "of a constraint violation";
    case SASL_BADBINDING: return "channel binding failure";
    case SASL_CONFIGERR:  return "error when parsing configuration file";

    default:   return "undefined error!";
    }

}

/* Return the sanitized error detail about the last error that occured for 
 * a connection */
const char *sasl_errdetail(sasl_conn_t *conn) 
{
    unsigned need_len;
    const char *errstr;
    char leader[128];

    if(!conn) return NULL;
    
    errstr = sasl_errstring(conn->error_code, NULL, NULL);
    snprintf(leader,128,"SASL(%d): %s: ",
	     sasl_usererr(conn->error_code), errstr);
    
    need_len = (unsigned) (strlen(leader) + strlen(conn->error_buf) + 12);
    if (_buf_alloc(&conn->errdetail_buf, &conn->errdetail_buf_len, need_len) != SASL_OK) {
        return NULL;
    }

    snprintf(conn->errdetail_buf, need_len, "%s%s", leader, conn->error_buf);
   
    return conn->errdetail_buf;
}


/* Note that this needs the global callbacks, so if you don't give getcallbacks
 * a sasl_conn_t, you're going to need to pass it yourself (or else we couldn't
 * have client and server at the same time */
static int _sasl_global_getopt(void *context,
			       const char *plugin_name,
			       const char *option,
			       const char ** result,
			       unsigned *len)
{
  const sasl_global_callbacks_t * global_callbacks;
  const sasl_callback_t *callback;

  global_callbacks = (const sasl_global_callbacks_t *) context;

  if (global_callbacks && global_callbacks->callbacks) {
      for (callback = global_callbacks->callbacks;
	   callback->id != SASL_CB_LIST_END;
	   callback++) {
	if (callback->id == SASL_CB_GETOPT) {
	  if (!callback->proc) return SASL_FAIL;
	  if (((sasl_getopt_t *)(callback->proc))(callback->context,
						  plugin_name,
						  option,
						  result,
						  len)
	      == SASL_OK)
	    return SASL_OK;
	}
      }
  }
  
  /* look it up in our configuration file */
  *result = sasl_config_getstring(option, NULL);
  if (*result != NULL) {
      if (len) { *len = (unsigned) strlen(*result); }
      return SASL_OK;
  }

  return SASL_FAIL;
}

static int
_sasl_conn_getopt(void *context,
		  const char *plugin_name,
		  const char *option,
		  const char ** result,
		  unsigned *len)
{
  sasl_conn_t * conn;
  const sasl_callback_t *callback;

  if (! context)
    return SASL_BADPARAM;

  conn = (sasl_conn_t *) context;

  if (conn->callbacks)
    for (callback = conn->callbacks;
	 callback->id != SASL_CB_LIST_END;
	 callback++)
      if (callback->id == SASL_CB_GETOPT
	  && (((sasl_getopt_t *)(callback->proc))(callback->context,
						  plugin_name,
						  option,
						  result,
						  len)
	      == SASL_OK))
	return SASL_OK;

  /* If we made it here, we didn't find an appropriate callback
   * in the connection's callback list, or the callback we did
   * find didn't return SASL_OK.  So we attempt to use the
   * global callback for this connection... */
  return _sasl_global_getopt((void *)conn->global_callbacks,
			     plugin_name,
			     option,
			     result,
			     len);
}

#ifdef HAVE_SYSLOG
/* this is the default logging */
static int _sasl_syslog(void *context,
			int priority,
			const char *message)
{
    int syslog_priority;
    sasl_server_conn_t *sconn;

    if (context) {
	if (((sasl_conn_t *)context)->type == SASL_CONN_SERVER) {
	    sconn = (sasl_server_conn_t *)context;
	    if (sconn->sparams->log_level < priority) 
		return SASL_OK;
	}
    }

    /* set syslog priority */
    switch(priority) {
    case SASL_LOG_NONE:
	return SASL_OK;
	break;
    case SASL_LOG_ERR:
	syslog_priority = LOG_ERR;
	break;
    case SASL_LOG_WARN:
	syslog_priority = LOG_WARNING;
	break;
    case SASL_LOG_NOTE:
    case SASL_LOG_FAIL:
	syslog_priority = LOG_NOTICE;
	break;
    case SASL_LOG_PASS:
    case SASL_LOG_TRACE:
    case SASL_LOG_DEBUG:
    default:
	syslog_priority = LOG_DEBUG;
	break;
    }
    
    /* do the syslog call. Do not need to call openlog? */
    syslog(syslog_priority | LOG_AUTH, "%s", message);
    
    return SASL_OK;
}
#endif				/* HAVE_SYSLOG */

static int
_sasl_getsimple(void *context,
		int id,
		const char ** result,
		size_t *len)
{
  const char *userid;

  if (! context || ! result) return SASL_BADPARAM;

  switch(id) {
  case SASL_CB_AUTHNAME:
    userid = getenv("USER");
    if (userid != NULL) {
	*result = userid;
	if (len) *len = strlen(userid);
	return SASL_OK;
    }
    userid = getenv("USERNAME");
    if (userid != NULL) {
	*result = userid;
	if (len) *len = strlen(userid);
	return SASL_OK;
    }
#ifdef WIN32
    /* for win32, try using the GetUserName standard call */
    {
	DWORD i;
	BOOL rval;
	static char sender[128];

    TCHAR tsender[128];
	i = sizeof(tsender) / sizeof(tsender[0]);
	rval = GetUserName(tsender, &i);
	if ( rval) { /* got a userid */
        WideCharToMultiByte(CP_UTF8, 0, tsender, -1, sender, sizeof(sender), NULL, NULL); /* -1 ensures null-terminated utf8 */
		*result = sender;
		if (len) *len = strlen(sender);
		return SASL_OK;
	}
    }
#endif /* WIN32 */
    return SASL_FAIL;
  default:
    return SASL_BADPARAM;
  }
}

static int
_sasl_getpath(void *context __attribute__((unused)),
              const char ** path_dest)
{
#if !defined(WIN32)
    char *path;
#endif
    int res = SASL_OK;

    if (! path_dest) {
        return SASL_BADPARAM;
    }

    /* Only calculate the path once. */
    if (default_plugin_path == NULL) {

#if defined(WIN32)
        /* NB: On Windows platforms this value is always allocated */
        default_plugin_path = _sasl_get_default_win_path(context,
                                                         SASL_PLUGIN_PATH_ATTR,
                                                         PLUGINDIR);
#else
        /* NB: On Unix platforms this value is never allocated */
        path = _sasl_get_default_unix_path(context,
                                           SASL_PATH_ENV_VAR,
                                           PLUGINDIR);

        res = _sasl_strdup(path, &default_plugin_path, NULL);
#endif
    }

    if (res == SASL_OK) {
        *path_dest = default_plugin_path;
    }

    return res;
}

static int
_sasl_getpath_simple(void *context __attribute__((unused)),
                     const char **path)
{
    if (! path) {
        return SASL_BADPARAM;
    }

    if (default_plugin_path == NULL) {
        return SASL_FAIL;
    }

    *path = default_plugin_path;

    return SASL_OK;
}

static int
_sasl_getconfpath(void *context __attribute__((unused)),
                  char ** path_dest)
{
#if !defined(WIN32)
    char *path;
#endif
    int res = SASL_OK;

    if (! path_dest) {
        return SASL_BADPARAM;
    }

  /* Only calculate the path once. */
    if (default_conf_path == NULL) {

#if defined(WIN32)
        /* NB: On Windows platforms this value is always allocated */
        default_conf_path = _sasl_get_default_win_path(context,
                                                       SASL_CONF_PATH_ATTR,
                                                       CONFIGDIR);
#else
        /* NB: On Unix platforms this value is never allocated */
        path = _sasl_get_default_unix_path(context,
                                           SASL_CONF_PATH_ENV_VAR,
                                           CONFIGDIR);

        res = _sasl_strdup(path, &default_conf_path, NULL);
#endif
    }

    if (res == SASL_OK) {
        *path_dest = default_conf_path;
    }

    return res;
}

static int
_sasl_getconfpath_simple(void *context __attribute__((unused)),
                         const char **path)
{
    if (! path) {
        return SASL_BADPARAM;
    }

    if (default_conf_path == NULL) {
        return SASL_FAIL;
    }

    *path = default_conf_path;

    return SASL_OK;
}


static int
_sasl_verifyfile(void *context __attribute__((unused)),
		 char *file  __attribute__((unused)),
		 int type  __attribute__((unused)))
{
  /* always say ok */
  return SASL_OK;
}


static int
_sasl_proxy_policy(sasl_conn_t *conn,
		   void *context __attribute__((unused)),
		   const char *requested_user, unsigned rlen,
		   const char *auth_identity, unsigned alen,
		   const char *def_realm __attribute__((unused)),
		   unsigned urlen __attribute__((unused)),
		   struct propctx *propctx __attribute__((unused)))
{
    if (!conn)
	return SASL_BADPARAM;

    if (!requested_user || *requested_user == '\0')
	return SASL_OK;

    if (!auth_identity || !requested_user || rlen != alen ||
	(memcmp(auth_identity, requested_user, rlen) != 0)) {
	sasl_seterror(conn, 0,
		      "Requested identity not authenticated identity");
	RETURN(conn, SASL_BADAUTH);
    }

    return SASL_OK;
}

int _sasl_getcallback(sasl_conn_t * conn,
		      unsigned long callbackid,
		      sasl_callback_ft *pproc,
		      void **pcontext)
{
  const sasl_callback_t *callback;

  if (!pproc || !pcontext)
      PARAMERROR(conn);

  /* Some callbacks are always provided by the library */
  switch (callbackid) {
  case SASL_CB_LIST_END:
    /* Nothing ever gets to provide this */
      INTERROR(conn, SASL_FAIL);
  case SASL_CB_GETOPT:
      if (conn) {
	  *pproc = (sasl_callback_ft)&_sasl_conn_getopt;
	  *pcontext = conn;
      } else {
	  *pproc = (sasl_callback_ft)&_sasl_global_getopt;
	  *pcontext = NULL;
      }
      return SASL_OK;
  }

  /* If it's not always provided by the library, see if there's
   * a version provided by the application for this connection... */
  if (conn && conn->callbacks) {
    for (callback = conn->callbacks; callback->id != SASL_CB_LIST_END;
	 callback++) {
	if (callback->id == callbackid) {
	    *pproc = callback->proc;
	    *pcontext = callback->context;
	    if (callback->proc) {
		return SASL_OK;
	    } else {
		return SASL_INTERACT;
	    }
	}
    }
  }

  /* And, if not for this connection, see if there's one
   * for all {server,client} connections... */
  if (conn && conn->global_callbacks && conn->global_callbacks->callbacks) {
      for (callback = conn->global_callbacks->callbacks;
	   callback->id != SASL_CB_LIST_END;
	   callback++) {
	  if (callback->id == callbackid) {
	      *pproc = callback->proc;
	      *pcontext = callback->context;
	      if (callback->proc) {
		  return SASL_OK;
	      } else {
		  return SASL_INTERACT;
	      }
	  }
      }
  }

  /* Otherwise, see if the library provides a default callback. */
  switch (callbackid) {
#ifdef HAVE_SYSLOG
  case SASL_CB_LOG:
    *pproc = (sasl_callback_ft)&_sasl_syslog;
    *pcontext = conn;
    return SASL_OK;
#endif /* HAVE_SYSLOG */
  case SASL_CB_GETPATH:
    *pproc = default_getpath_cb.proc;
    *pcontext = default_getpath_cb.context;
    return SASL_OK;
  case SASL_CB_GETCONFPATH:
    *pproc = default_getconfpath_cb.proc;
    *pcontext = default_getconfpath_cb.context;
    return SASL_OK;
  case SASL_CB_AUTHNAME:
    *pproc = (sasl_callback_ft)&_sasl_getsimple;
    *pcontext = conn;
    return SASL_OK;
  case SASL_CB_VERIFYFILE:
    *pproc = (sasl_callback_ft)&_sasl_verifyfile;
    *pcontext = NULL;
    return SASL_OK;
  case SASL_CB_PROXY_POLICY:
    *pproc = (sasl_callback_ft)&_sasl_proxy_policy;
    *pcontext = NULL;
    return SASL_OK;
  }

  /* Unable to find a callback... */
  *pproc = NULL;
  *pcontext = NULL;
  sasl_seterror(conn, SASL_NOLOG, "Unable to find a callback: %d", callbackid);
  RETURN(conn,SASL_FAIL);
}


/*
 * This function is typically called from a plugin.
 * It creates a string from the formatting and varargs given
 * and calls the logging callback (syslog by default)
 *
 * %m will parse the value in the next argument as an errno string
 * %z will parse the next argument as a SASL error code.
 */

void
_sasl_log (sasl_conn_t *conn,
	   int level,
	   const char *fmt,
	   ...)
{
  char *out = NULL;
  size_t alloclen=100; /* current allocated length */
  size_t outlen=0; /* current length of output buffer */
  size_t formatlen;
  size_t pos=0; /* current position in format string */
  int result;
  sasl_log_t *log_cb;
  void *log_ctx;
  
  int ival;
  unsigned int uval;
  char *cval;
  va_list ap; /* varargs thing */

  if(!fmt) return;
  
  out = (char *) sasl_ALLOC(250);
  if(!out) return;

  formatlen = strlen(fmt);

  /* See if we have a logging callback... */
  result = _sasl_getcallback(conn, SASL_CB_LOG, (sasl_callback_ft *)&log_cb, &log_ctx);
  if (result == SASL_OK && ! log_cb)
    result = SASL_FAIL;
  if (result != SASL_OK) goto done;
  
  va_start(ap, fmt); /* start varargs */

  while(pos<formatlen)
  {
    if (fmt[pos]!='%') /* regular character */
    {
      result = _buf_alloc(&out, &alloclen, outlen+1);
      if (result != SASL_OK) goto done;
      out[outlen]=fmt[pos];
      outlen++;
      pos++;

    } else { /* formating thing */
      int done=0;
      char frmt[10];
      int frmtpos=1;
      char tempbuf[21];
      frmt[0]='%';
      pos++;

      while (done==0)
      {
	switch(fmt[pos])
	  {
	  case 's': /* need to handle this */
	    cval = va_arg(ap, char *); /* get the next arg */
	    result = _sasl_add_string(&out, &alloclen,
				&outlen, cval);
	      
	    if (result != SASL_OK) /* add the string */
		goto done;

	    done=1;
	    break;

	  case '%': /* double % output the '%' character */
	    result = _buf_alloc(&out,&alloclen,outlen+1);
	    if (result != SASL_OK)
		goto done;
	    
	    out[outlen]='%';
	    outlen++;
	    done=1;
	    break;

	  case 'm': /* insert the errno string */
	    result = _sasl_add_string(&out, &alloclen, &outlen,
				strerror(va_arg(ap, int)));
	    if (result != SASL_OK)
		goto done;
	    
	    done=1;
	    break;

	  case 'z': /* insert the sasl error string */
	    result = _sasl_add_string(&out, &alloclen, &outlen,
				(char *) sasl_errstring(va_arg(ap, int),NULL,NULL));
	    if (result != SASL_OK)
		goto done;
	    
	    done=1;
	    break;

	  case 'c':
	    frmt[frmtpos++]=fmt[pos];
	    frmt[frmtpos]=0;
	    tempbuf[0] = (char) va_arg(ap, int); /* get the next arg */
	    tempbuf[1]='\0';
	    
	    /* now add the character */
	    result = _sasl_add_string(&out, &alloclen, &outlen, tempbuf);
	    if (result != SASL_OK)
		goto done;
		
	    done=1;
	    break;

	  case 'd':
	  case 'i':
	    frmt[frmtpos++]=fmt[pos];
	    frmt[frmtpos]=0;
	    ival = va_arg(ap, int); /* get the next arg */

	    snprintf(tempbuf,20,frmt,ival); /* have snprintf do the work */
	    /* now add the string */
	    result = _sasl_add_string(&out, &alloclen, &outlen, tempbuf);
	    if (result != SASL_OK)
		goto done;

	    done=1;
	    break;

	  case 'o':
	  case 'u':
	  case 'x':
	  case 'X':
	    frmt[frmtpos++]=fmt[pos];
	    frmt[frmtpos]=0;
	    uval = va_arg(ap, unsigned int); /* get the next arg */

	    snprintf(tempbuf,20,frmt,uval); /* have snprintf do the work */
	    /* now add the string */
	    result = _sasl_add_string(&out, &alloclen, &outlen, tempbuf);
	    if (result != SASL_OK)
		goto done;

	    done=1;
	    break;

	  default: 
	    frmt[frmtpos++]=fmt[pos]; /* add to the formating */
	    frmt[frmtpos]=0;	    
	    if (frmtpos>9) 
	      done=1;
	  }
	pos++;
	if (pos>formatlen)
	  done=1;
      }

    }
  }

  /* put 0 at end */
  result = _buf_alloc(&out, &alloclen, outlen+1);
  if (result != SASL_OK) goto done;
  out[outlen]=0;

  /* send log message */
  result = log_cb(log_ctx, level, out);

 done:
  va_end(ap);
  if(out) sasl_FREE(out);
}



/* Allocate and Init a sasl_utils_t structure */
sasl_utils_t *
_sasl_alloc_utils(sasl_conn_t *conn,
		  sasl_global_callbacks_t *global_callbacks)
{
  sasl_utils_t *utils;
  /* set util functions - need to do rest*/
  utils=sasl_ALLOC(sizeof(sasl_utils_t));
  if (utils==NULL)
    return NULL;

  utils->conn = conn;

  sasl_randcreate(&utils->rpool);

  if (conn) {
    utils->getopt = &_sasl_conn_getopt;
    utils->getopt_context = conn;
  } else {
    utils->getopt = &_sasl_global_getopt;
    utils->getopt_context = global_callbacks;
  }

  utils->malloc=_sasl_allocation_utils.malloc;
  utils->calloc=_sasl_allocation_utils.calloc;
  utils->realloc=_sasl_allocation_utils.realloc;
  utils->free=_sasl_allocation_utils.free;

  utils->mutex_alloc = _sasl_mutex_utils.alloc;
  utils->mutex_lock = _sasl_mutex_utils.lock;
  utils->mutex_unlock = _sasl_mutex_utils.unlock;
  utils->mutex_free = _sasl_mutex_utils.free;
  
  utils->MD5Init  = &_sasl_MD5Init;
  utils->MD5Update= &_sasl_MD5Update;
  utils->MD5Final = &_sasl_MD5Final;
  utils->hmac_md5 = &_sasl_hmac_md5;
  utils->hmac_md5_init = &_sasl_hmac_md5_init;
  utils->hmac_md5_final = &_sasl_hmac_md5_final;
  utils->hmac_md5_precalc = &_sasl_hmac_md5_precalc;
  utils->hmac_md5_import = &_sasl_hmac_md5_import;
  utils->mkchal = &sasl_mkchal;
  utils->utf8verify = &sasl_utf8verify;
  utils->rand=&sasl_rand;
  utils->churn=&sasl_churn;  
  utils->checkpass=NULL;
  
  utils->encode64=&sasl_encode64;
  utils->decode64=&sasl_decode64;
  
  utils->erasebuffer=&sasl_erasebuffer;

  utils->getprop=&sasl_getprop;
  utils->setprop=&sasl_setprop;

  utils->getcallback=&_sasl_getcallback;

  utils->log=&_sasl_log;

  utils->seterror=&sasl_seterror;

#ifndef macintosh
  /* Aux Property Utilities */
  utils->prop_new=&prop_new;
  utils->prop_dup=&prop_dup;
  utils->prop_request=&prop_request;
  utils->prop_get=&prop_get;
  utils->prop_getnames=&prop_getnames;
  utils->prop_clear=&prop_clear;
  utils->prop_dispose=&prop_dispose;
  utils->prop_format=&prop_format;
  utils->prop_set=&prop_set;
  utils->prop_setvals=&prop_setvals;
  utils->prop_erase=&prop_erase;
  utils->auxprop_store=&sasl_auxprop_store;
#endif

  /* Spares */
  utils->spare_fptr = NULL;
  utils->spare_fptr1 = utils->spare_fptr2 = NULL;
  
  return utils;
}

int
_sasl_free_utils(const sasl_utils_t ** utils)
{
    sasl_utils_t *nonconst;

    if(!utils) return SASL_BADPARAM;
    if(!*utils) return SASL_OK;

    /* I wish we could avoid this cast, it's pretty gratuitous but it
     * does make life easier to have it const everywhere else. */
    nonconst = (sasl_utils_t *)(*utils);

    sasl_randfree(&(nonconst->rpool));
    sasl_FREE(nonconst);

    *utils = NULL;
    return SASL_OK;
}

int sasl_idle(sasl_conn_t *conn)
{
  if (! conn) {
    if (_sasl_server_idle_hook
	&& _sasl_server_idle_hook(NULL))
      return 1;
    if (_sasl_client_idle_hook
	&& _sasl_client_idle_hook(NULL))
      return 1;
    return 0;
  }

  if (conn->idle_hook)
    return conn->idle_hook(conn);

  return 0;
}

static const sasl_callback_t *
_sasl_find_callback_by_type (const sasl_callback_t *callbacks,
                             unsigned long id)
{
    if (callbacks) {
        while (callbacks->id != SASL_CB_LIST_END) {
            if (callbacks->id == id) {
	        return callbacks;
            } else {
	        ++callbacks;
            }
        }
    }
    return NULL;
}

const sasl_callback_t *
_sasl_find_getpath_callback(const sasl_callback_t *callbacks)
{
  callbacks = _sasl_find_callback_by_type (callbacks, SASL_CB_GETPATH);
  if (callbacks != NULL) {
    return callbacks;
  } else {
    return &default_getpath_cb;
  }
}

const sasl_callback_t *
_sasl_find_getconfpath_callback(const sasl_callback_t *callbacks)
{
  callbacks = _sasl_find_callback_by_type (callbacks, SASL_CB_GETCONFPATH);
  if (callbacks != NULL) {
    return callbacks;
  } else {
    return &default_getconfpath_cb;
  }
}

const sasl_callback_t *
_sasl_find_verifyfile_callback(const sasl_callback_t *callbacks)
{
  static const sasl_callback_t default_verifyfile_cb = {
    SASL_CB_VERIFYFILE,
    (sasl_callback_ft)&_sasl_verifyfile,
    NULL
  };

  callbacks = _sasl_find_callback_by_type (callbacks, SASL_CB_VERIFYFILE);
  if (callbacks != NULL) {
    return callbacks;
  } else {
    return &default_verifyfile_cb;
  }
}

/* Basically a conditional call to realloc(), if we need more */
int _buf_alloc(char **rwbuf, size_t *curlen, size_t newlen) 
{
    if(!(*rwbuf)) {
	*rwbuf = sasl_ALLOC((unsigned)newlen);
	if (*rwbuf == NULL) {
	    *curlen = 0;
	    return SASL_NOMEM;
	}
	*curlen = newlen;
    } else if(*rwbuf && *curlen < newlen) {
	size_t needed = 2*(*curlen);

	while(needed < newlen)
	    needed *= 2;

        /* WARN - We will leak the old buffer on failure */
	*rwbuf = sasl_REALLOC(*rwbuf, (unsigned)needed);
	
	if (*rwbuf == NULL) {
	    *curlen = 0;
	    return SASL_NOMEM;
	}
	*curlen = needed;
    } 

    return SASL_OK;
}

/* for the mac os x cfm glue: this lets the calling function
   get pointers to the error buffer without having to touch the sasl_conn_t struct */
void _sasl_get_errorbuf(sasl_conn_t *conn, char ***bufhdl, size_t **lenhdl)
{
	*bufhdl = &conn->error_buf;
	*lenhdl = &conn->error_buf_len;
}

/* convert an iovec to a single buffer */
int _iovec_to_buf(const struct iovec *vec,
		  unsigned numiov, buffer_info_t **output) 
{
    unsigned i;
    int ret;
    buffer_info_t *out;
    char *pos;

    if (!vec || !output) return SASL_BADPARAM;

    if (!(*output)) {
	*output = sasl_ALLOC(sizeof(buffer_info_t));
	if (!*output) return SASL_NOMEM;
	memset(*output,0,sizeof(buffer_info_t));
    }

    out = *output;
    
    out->curlen = 0;
    for (i = 0; i < numiov; i++) {
	out->curlen += vec[i].iov_len;
    }

    ret = _buf_alloc(&out->data, &out->reallen, out->curlen);

    if (ret != SASL_OK) return SASL_NOMEM;
    
    memset(out->data, 0, out->reallen);
    pos = out->data;
    
    for (i = 0; i < numiov; i++) {
	memcpy(pos, vec[i].iov_base, vec[i].iov_len);
	pos += vec[i].iov_len;
    }

    return SASL_OK;
}

/* This code might be useful in the future, but it isn't now, so.... */
#if 0
int _sasl_iptostring(const struct sockaddr *addr, socklen_t addrlen,
		     char *out, unsigned outlen) {
    char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];
    int niflags;

    if(!addr || !out) return SASL_BADPARAM;

    niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
    if (addr->sa_family == AF_INET6)
	niflags |= NI_WITHSCOPEID;
#endif
    if (getnameinfo(addr, addrlen, hbuf, sizeof(hbuf), pbuf, sizeof(pbuf),
		    niflags) != 0)
	return SASL_BADPARAM;

    if(outlen < strlen(hbuf) + strlen(pbuf) + 2)
	return SASL_BUFOVER;

    snprintf(out, outlen, "%s;%s", hbuf, pbuf);

    return SASL_OK;
}
#endif

int _sasl_ipfromstring(const char *addr,
		       struct sockaddr *out, socklen_t outlen) 
{
    int i, j;
    struct addrinfo hints, *ai = NULL;
    char hbuf[NI_MAXHOST];
    
    /* A NULL out pointer just implies we don't do a copy, just verify it */

    if(!addr) return SASL_BADPARAM;

    /* Parse the address */
    for (i = 0; addr[i] != '\0' && addr[i] != ';'; i++) {
	if (i >= NI_MAXHOST)
	    return SASL_BADPARAM;
	hbuf[i] = addr[i];
    }
    hbuf[i] = '\0';

    if (addr[i] == ';')
	i++;
    /* XXX: Do we need this check? */
    for (j = i; addr[j] != '\0'; j++)
	if (!isdigit((int)(addr[j])))
	    return SASL_BADPARAM;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_NUMERICHOST;
    if (getaddrinfo(hbuf, &addr[i], &hints, &ai) != 0)
	return SASL_BADPARAM;

    if (out) {
	if (outlen < (socklen_t)ai->ai_addrlen) {
	    freeaddrinfo(ai);
	    return SASL_BUFOVER;
	}
	memcpy(out, ai->ai_addr, ai->ai_addrlen);
    }

    freeaddrinfo(ai);

    return SASL_OK;
}

int _sasl_build_mechlist(void) 
{
    int count = 0;
    sasl_string_list_t *clist = NULL, *slist = NULL, *olist = NULL;
    sasl_string_list_t *p, *q, **last, *p_next;

    clist = _sasl_client_mechs();
    slist = _sasl_server_mechs();

    if(!clist) {
	olist = slist;
    } else {
	int flag;
	
	/* append slist to clist, and set olist to clist */
	for(p = slist; p; p = p_next) {
	    flag = 0;
	    p_next = p->next;

	    last = &clist;
	    for(q = clist; q; q = q->next) {
		if(!strcmp(q->d, p->d)) {
		    /* They match, set the flag */
		    flag = 1;
		    break;
		}
		last = &(q->next);
	    }

	    if(!flag) {
		*last = p;
		p->next = NULL;
	    } else {
		sasl_FREE(p);
	    }
	}

	olist = clist;
    }

    if(!olist) {
	/* This is not going to be very useful */
	printf ("no olist");
	return SASL_FAIL;
    }

    for (p = olist; p; p = p->next) count++;
    
    if(global_mech_list) {
	sasl_FREE(global_mech_list);
	global_mech_list = NULL;
    }
    
    global_mech_list = sasl_ALLOC((count + 1) * sizeof(char *));
    if(!global_mech_list) return SASL_NOMEM;
    
    memset(global_mech_list, 0, (count + 1) * sizeof(char *));
    
    count = 0;
    for (p = olist; p; p = p_next) {
	p_next = p->next;

	global_mech_list[count++] = (char *) p->d;

    	sasl_FREE(p);
    }

    return SASL_OK;
}

const char ** sasl_global_listmech(void) 
{
    return (const char **)global_mech_list;
}

int sasl_listmech(sasl_conn_t *conn,
		  const char *user,
		  const char *prefix,
		  const char *sep,
		  const char *suffix,
		  const char **result,
		  unsigned *plen,
		  int *pcount)
{
    if(!conn) {
	return SASL_BADPARAM;
    } else if(conn->type == SASL_CONN_SERVER) {
	RETURN(conn, _sasl_server_listmech(conn, user, prefix, sep, suffix,
					   result, plen, pcount));
    } else if (conn->type == SASL_CONN_CLIENT) {
	RETURN(conn, _sasl_client_listmech(conn, prefix, sep, suffix,
					   result, plen, pcount));
    }
    
    PARAMERROR(conn);
}

int _sasl_is_equal_mech(const char *req_mech,
                        const char *plug_mech,
			size_t req_mech_len,
                        int *plus)
{
    size_t n;

    if (req_mech_len > 5 &&
        strcasecmp(&req_mech[req_mech_len - 5], "-PLUS") == 0) {
        n = req_mech_len - 5;
        *plus = 1;
    } else {
        n = req_mech_len;
        *plus = 0;
    }

    if (n < strlen(plug_mech)) {
	/* Don't allow arbitrary prefix match */
	return 0;
    }

    return (strncasecmp(req_mech, plug_mech, n) == 0);
}

#ifndef WIN32
static char *
_sasl_get_default_unix_path(void *context __attribute__((unused)),
                            char * env_var_name,
                            char * default_value)
{
    char *path = NULL;

    /* Honor external variable only in a safe environment */
    if (getuid() == geteuid() && getgid() == getegid()) {
        path = getenv(env_var_name);
    }
    if (! path) {
        path = default_value;
    }

    return path;
}

#else /*WIN32*/
/* Return NULL on failure */
static char *
_sasl_get_default_win_path(void *context __attribute__((unused)),
                           TCHAR * reg_attr_name,
                           char * default_value)
{
    /* Open registry entry, and find all registered SASL libraries.
     *
     * Registry location:
     *
     *     SOFTWARE\\Carnegie Mellon\\Project Cyrus\\SASL Library
     *
     * Key - value:
     *
     *     "SearchPath" - value: PATH like (';' delimited) list
     *                    of directories where to search for plugins
     *                    The list may contain references to environment
     *                    variables (e.g. %PATH%).
     *
     */
    HKEY  hKey;
    DWORD ret;
    DWORD ValueType;		    /* value type */
    DWORD cbData;		    /* value size in bytes and later number of wchars */
    TCHAR * ValueData;		    /* value */
    DWORD cbExpandedData;	    /* "expanded" value size in wchars */
    TCHAR * ExpandedValueData;	    /* "expanded" value */
    TCHAR * return_value;	    /* function return value */
    TCHAR * tmp;

    /* Initialization */
    ExpandedValueData = NULL;
    ValueData = NULL;
    return_value = NULL;

    /* Open the registry */
    ret = RegOpenKeyEx(HKEY_LOCAL_MACHINE,
		       SASL_ROOT_KEY,
		       0,
		       KEY_READ,
		       &hKey);

    if (ret != ERROR_SUCCESS) { 
        /* no registry entry */
        char *ret;
        (void) _sasl_strdup (default_value, &ret, NULL);
        return ret;
    }

    /* figure out value type and required buffer size */
    /* the size will include space for terminating NUL if required */
    RegQueryValueEx (hKey,
		     reg_attr_name,
		     NULL,	    /* reserved */
		     &ValueType,
		     NULL,
		     &cbData);
 
    /* Only accept string related types */
    if (ValueType != REG_EXPAND_SZ &&
	ValueType != REG_MULTI_SZ &&
	ValueType != REG_SZ) {
	return_value = NULL;
	goto CLEANUP;
    }

    /* Any high water mark? */
    ValueData = sasl_ALLOC(cbData + 2 * sizeof(TCHAR)); /* extra bytes to insert null-terminator if it's missed */
    if (ValueData == NULL) {
	return_value = NULL;
	goto CLEANUP;
    };

    if (RegQueryValueEx(hKey,
        reg_attr_name,
        NULL,	    /* reserved */
        &ValueType,
        (LPBYTE)ValueData,
        &cbData) != ERROR_SUCCESS) {
        return_value = NULL;
        goto CLEANUP;
    }
    cbData /= sizeof(TCHAR); /* covert to number of symbols */
    ValueData[cbData] = '\0'; /* MS docs say we have to to that */
    ValueData[cbData + 1] = '\0'; /* for MULTI */

    switch (ValueType) {
    case REG_EXPAND_SZ:
        /* : A random starting guess */
        cbExpandedData = cbData + 1024;
        ExpandedValueData = (TCHAR*)sasl_ALLOC(cbExpandedData * sizeof(TCHAR));
        if (ExpandedValueData == NULL) {
            return_value = NULL;
            goto CLEANUP;
        };


        cbExpandedData = ExpandEnvironmentStrings(
                                                  ValueData,
                                                  ExpandedValueData,
                                                  cbExpandedData);

        if (cbExpandedData == 0) {
            /* : GetLastError() contains the reason for failure */
            return_value = NULL;
            goto CLEANUP;
        }

        /* : Must retry expansion with the bigger buffer */
        if (cbExpandedData > cbData + 1024) {
            /* : Memory leak here if can't realloc */
            ExpandedValueData = sasl_REALLOC(ExpandedValueData, cbExpandedData * sizeof(TCHAR));
            if (ExpandedValueData == NULL) {
                return_value = NULL;
                goto CLEANUP;
            };

            cbExpandedData = ExpandEnvironmentStrings(
                                                      ValueData,
                                                      ExpandedValueData,
                                                      cbExpandedData);

            /* : This should not happen */
            if (cbExpandedData == 0) {
                /* : GetLastError() contains the reason for failure */
                return_value = NULL;
                goto CLEANUP;
            }
        }

        sasl_FREE(ValueData);
        ValueData = ExpandedValueData;
        /* : This is to prevent automatical freeing of this block on cleanup */
        ExpandedValueData = NULL;

        break;

    case REG_MULTI_SZ:
        tmp = ValueData;

        /* : We shouldn't overflow here, as the buffer is guarantied
           : to contain at least two consequent NULs */
        while (1) {
            if (tmp[0] == '\0') {
                /* : Stop the process if we found the end of the string (two consequent NULs) */
                if (tmp[1] == '\0') {
                    break;
                }

                /* : Replace delimiting NUL with our delimiter characted */
                tmp[0] = PATHS_DELIMITER;
            }
            tmp += (_tcslen(tmp));
        }
        break;

    case REG_SZ:
        /* Do nothing, it is good as is */
        break;

    default:
        return_value = NULL;
        goto CLEANUP;
    }

    return_value = ValueData; /* just to flag we have a result */

CLEANUP:
    RegCloseKey(hKey);
    if (ExpandedValueData != NULL) sasl_FREE(ExpandedValueData);
    if (return_value == NULL) {
	    if (ValueData != NULL) sasl_FREE(ValueData);
        return NULL;
    }
    if (sizeof(TCHAR) == sizeof(char)) {
        return (char*)return_value;
    }

    /* convert to utf-8 for compatibility with other OS' */
    {
        char *tmp = _sasl_wchar_to_utf8(return_value);
        sasl_FREE(return_value);
        return tmp;
    }
}

char* _sasl_wchar_to_utf8(WCHAR *str)
{
    size_t bufLen = WideCharToMultiByte(CP_UTF8, 0, str, -1, NULL, 0, NULL, NULL);
    char *buf = sasl_ALLOC(bufLen);
    if (buf) {
        if (WideCharToMultiByte(CP_UTF8, 0, str, -1, buf, bufLen, NULL, NULL) == 0) { /* -1 ensures null-terminated utf8 */
            sasl_FREE(buf);
            buf = NULL;
        }
    }
    return buf;
}

WCHAR* _sasl_utf8_to_wchar(const char *str)
{
    size_t bufLen = MultiByteToWideChar(CP_UTF8, 0, str, -1, NULL, 0);
    WCHAR *buf = sasl_ALLOC(bufLen * sizeof(WCHAR));
    if (buf) {
        if (MultiByteToWideChar(CP_UTF8, 0, str, -1, buf, bufLen) == 0) { /* -1 ensures null-terminated utf8 */
            sasl_FREE(buf);
            buf = NULL;
        }
    }
    return buf;
}

#endif /*WIN32*/
