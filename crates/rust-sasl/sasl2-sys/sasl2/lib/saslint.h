/* saslint.h - internal SASL library definitions
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

#ifndef SASLINT_H
#define SASLINT_H

#include <config.h>
#include "sasl.h"
#include "saslplug.h"
#include "saslutil.h"
#include "prop.h"

#ifndef INLINE
#if defined (WIN32)
/* Visual Studio: "inline" keyword is not available in C, only in C++ */
#define INLINE __inline
#else
#define INLINE  inline
#endif
#endif

/* #define'd constants */
#define CANON_BUF_SIZE 1024

/* Error Handling Foo */
/* Helpful Hints:
 *  -Error strings are set as soon as possible (first function in stack trace
 *   with a pointer to the sasl_conn_t.
 *  -Error codes are set as late as possible (only in the sasl api functions),
 *   though "as often as possible" also comes to mind to ensure correctness
 *  -Errors from calls to _buf_alloc, _sasl_strdup, etc are assumed to be
 *   memory errors.
 *  -Only errors (error codes < SASL_OK) should be remembered
 */
#define RETURN(conn, val) { if(conn && (val) < SASL_OK) \
                               (conn)->error_code = (val); \
                            return (val); }
#define MEMERROR(conn) {\
    if(conn) sasl_seterror( (conn), 0, \
                   "Out of Memory in " __FILE__ " near line %d", __LINE__ ); \
    RETURN(conn, SASL_NOMEM) }
#define PARAMERROR(conn) {\
    if(conn) sasl_seterror( (conn), SASL_NOLOG, \
                  "Parameter error in " __FILE__ " near line %d", __LINE__ ); \
    RETURN(conn, SASL_BADPARAM) }
#define INTERROR(conn, val) {\
    if(conn) sasl_seterror( (conn), 0, \
                   "Internal Error %d in " __FILE__ " near line %d", (val),\
		   __LINE__ ); \
    RETURN(conn, (val)) }

#ifndef PATH_MAX
# ifdef WIN32
#  define PATH_MAX MAX_PATH
# else
#  ifdef _POSIX_PATH_MAX
#   define PATH_MAX _POSIX_PATH_MAX
#  else
#   define PATH_MAX 1024         /* arbitrary; probably big enough.
                                  * will probably only be 256+64 on
                                  * pre-posix machines */
#  endif /* _POSIX_PATH_MAX */
# endif /* WIN32 */
#endif

/* : Define directory delimiter in SASL_PATH/SASL_CONF_PATH variables */
#ifdef WIN32
#define PATHS_DELIMITER	';'
#else
#define PATHS_DELIMITER	':'
#endif

/* A FQDN max len is 255 per RFC 1035,
 * this means 253 chars max, we add one more for zero terminator */
#define MAXFQDNLEN 254

/* Datatype Definitions */
typedef struct {
  const sasl_callback_t *callbacks;
  const char *appname;
} sasl_global_callbacks_t;

typedef struct _sasl_external_properties 
{
    sasl_ssf_t ssf;
    char *auth_id;
} _sasl_external_properties_t;

typedef struct sasl_string_list
{
    const char *d;
    struct sasl_string_list *next;
} sasl_string_list_t;

typedef struct buffer_info
{ 
    char *data;
    size_t curlen;
    size_t reallen;
} buffer_info_t;

typedef int add_plugin_t(const char *, void *);

typedef struct add_plugin_list 
{
    const char *entryname;
    add_plugin_t *add_plugin;
} add_plugin_list_t;

enum Sasl_conn_type { SASL_CONN_UNKNOWN = 0,
		      SASL_CONN_SERVER = 1,
                      SASL_CONN_CLIENT = 2 };

struct sasl_conn {
  enum Sasl_conn_type type;

  void (*destroy_conn)(sasl_conn_t *); /* destroy function */

  char *service;

  unsigned int flags;  /* flags passed to sasl_*_new */

  /* IP information.  A buffer of size 52 is adequate for this in its
     longest format (see sasl.h) */
  int got_ip_local, got_ip_remote;
  char iplocalport[NI_MAXHOST + NI_MAXSERV];
  char ipremoteport[NI_MAXHOST + NI_MAXSERV];

  void *context;
  sasl_out_params_t oparams;

  sasl_security_properties_t props;
  _sasl_external_properties_t external;

  sasl_secret_t *secret;

  int (*idle_hook)(sasl_conn_t *conn);
  const sasl_callback_t *callbacks;
  const sasl_global_callbacks_t *global_callbacks; /* global callbacks
						    * connection */
  char *serverFQDN;

  /* Pointers to memory that we are responsible for */
  buffer_info_t *encode_buf;

  int error_code;
  char *error_buf, *errdetail_buf;
  size_t error_buf_len, errdetail_buf_len;
  char *mechlist_buf;
  size_t mechlist_buf_len;

  char *decode_buf;

  char user_buf[CANON_BUF_SIZE+1], authid_buf[CANON_BUF_SIZE+1];

  /* Allocated by sasl_encodev if the output contains multiple SASL packet. */
  buffer_info_t multipacket_encoded_data;
};

/* Server Conn Type Information */

typedef struct mechanism
{
    server_sasl_mechanism_t m;
    struct mechanism *next;
} mechanism_t;

typedef struct mech_list {
  const sasl_utils_t *utils;  /* gotten from plug_init */

  void *mutex;            /* mutex for this data */ 
  mechanism_t *mech_list; /* list of loaded mechanisms */
  int mech_length;        /* number of loaded mechanisms */
} mech_list_t;

typedef struct context_list 
{
    mechanism_t *mech;
    void *context;     /* if NULL, this mech is disabled for this connection
			* otherwise, use this context instead of a call
			* to mech_new */
    struct context_list *next;
} context_list_t;

typedef struct sasl_server_conn {
    sasl_conn_t base; /* parts common to server + client */

    char *appname; /* application name buffer (for sparams) */
    char *user_realm; /* domain the user authenticating is in */
    int sent_last; /* Have we already done the last send? */
    int authenticated;
    mechanism_t *mech; /* mechanism trying to use */
    sasl_server_params_t *sparams;
    context_list_t *mech_contexts;
    mechanism_t *mech_list; /* list of available mechanisms */
    int mech_length;        /* number of available mechanisms */
} sasl_server_conn_t;

/* Client Conn Type Information */

typedef struct cmechanism
{
    client_sasl_mechanism_t m;
    struct cmechanism *next;  
} cmechanism_t;

typedef struct cmech_list {
  const sasl_utils_t *utils; 

  void *mutex;            /* mutex for this data */ 
  cmechanism_t *mech_list; /* list of mechanisms */
  int mech_length;       /* number of mechanisms */

} cmech_list_t;

typedef struct sasl_client_conn {
  sasl_conn_t base; /* parts common to server + client */

  cmechanism_t *mech;
  sasl_client_params_t *cparams;

  char *clientFQDN;

  cmechanism_t *mech_list; /* list of available mechanisms */
  int mech_length;	   /* number of available mechanisms */
} sasl_client_conn_t;

typedef struct sasl_allocation_utils {
  sasl_malloc_t *malloc;
  sasl_calloc_t *calloc;
  sasl_realloc_t *realloc;
  sasl_free_t *free;
} sasl_allocation_utils_t;

typedef struct sasl_mutex_utils {
  sasl_mutex_alloc_t *alloc;
  sasl_mutex_lock_t *lock;
  sasl_mutex_unlock_t *unlock;
  sasl_mutex_free_t *free;
} sasl_mutex_utils_t;

typedef struct sasl_log_utils_s {
  sasl_log_t *log;
} sasl_log_utils_t;

typedef int sasl_plaintext_verifier(sasl_conn_t *conn,
				    const char *userid,
				    const char *passwd,
				    const char *service,
				    const char *user_realm);

struct sasl_verify_password_s {
    char *name;
    sasl_plaintext_verifier *verify;
};

/*
 * globals & constants
 */
/*
 * common.c
 */
LIBSASL_API const sasl_utils_t *sasl_global_utils;

extern int (*_sasl_client_idle_hook)(sasl_conn_t *conn);
extern int (*_sasl_server_idle_hook)(sasl_conn_t *conn);

/* These return SASL_OK if we've actually finished cleanup, 
 * SASL_NOTINIT if that part of the library isn't initialized, and
 * SASL_CONTINUE if we need to call them again */
extern int (*_sasl_client_cleanup_hook)(void);
extern int (*_sasl_server_cleanup_hook)(void);

extern sasl_allocation_utils_t _sasl_allocation_utils;
extern sasl_mutex_utils_t _sasl_mutex_utils;
extern int _sasl_allocation_locked;

void sasl_common_done(void);

extern int _sasl_is_equal_mech(const char *req_mech,
                               const char *plug_mech,
                               size_t req_mech_len,
                               int *plus);

/*
 * checkpw.c
 */
extern struct sasl_verify_password_s _sasl_verify_password[];

/*
 * server.c
 */
/* (this is a function call to ensure this is read-only to the outside) */
extern int _is_sasl_server_active(void);

/*
 * Allocation and Mutex utility macros
 */
#define sasl_ALLOC(__size__) (_sasl_allocation_utils.malloc((__size__)))
#define sasl_CALLOC(__nelem__, __size__) \
	(_sasl_allocation_utils.calloc((__nelem__), (__size__)))
#define sasl_REALLOC(__ptr__, __size__) \
	(_sasl_allocation_utils.realloc((__ptr__), (__size__)))
#define sasl_FREE(__ptr__) (_sasl_allocation_utils.free((__ptr__)))

#define sasl_MUTEX_ALLOC() (_sasl_mutex_utils.alloc())
#define sasl_MUTEX_LOCK(__mutex__) (_sasl_mutex_utils.lock((__mutex__)))
#define sasl_MUTEX_UNLOCK(__mutex__) (_sasl_mutex_utils.unlock((__mutex__)))
#define sasl_MUTEX_FREE(__mutex__) \
	(_sasl_mutex_utils.free((__mutex__)))

/* function prototypes */
/*
 * dlopen.c and staticopen.c
 */
/*
 * The differences here are:
 * _sasl_load_plugins loads all plugins from all files
 * _sasl_get_plugin loads the LIBRARY for an individual file
 * _sasl_done_with_plugins frees the LIBRARIES loaded by the above 2
 * _sasl_locate_entry locates an entrypoint in a given library
 */
extern int _sasl_load_plugins(const add_plugin_list_t *entrypoints,
			       const sasl_callback_t *getpath_callback,
			       const sasl_callback_t *verifyfile_callback);
extern int _sasl_get_plugin(const char *file,
			    const sasl_callback_t *verifyfile_cb,
			    void **libraryptr);
extern int _sasl_locate_entry(void *library, const char *entryname,
                              void **entry_point);
extern int _sasl_done_with_plugins();

/*
 * common.c
 */
extern const sasl_callback_t *
_sasl_find_getpath_callback(const sasl_callback_t *callbacks);

extern const sasl_callback_t *
_sasl_find_getconfpath_callback(const sasl_callback_t *callbacks);

extern const sasl_callback_t *
_sasl_find_verifyfile_callback(const sasl_callback_t *callbacks);

extern int _sasl_common_init(sasl_global_callbacks_t *global_callbacks);

extern int _sasl_conn_init(sasl_conn_t *conn,
			   const char *service,
			   unsigned int flags,
			   enum Sasl_conn_type type,
			   int (*idle_hook)(sasl_conn_t *conn),
			   const char *serverFQDN,
			   const char *iplocalport,
			   const char *ipremoteport,
			   const sasl_callback_t *callbacks,
			   const sasl_global_callbacks_t *global_callbacks);
extern void _sasl_conn_dispose(sasl_conn_t *conn);

extern sasl_utils_t *
_sasl_alloc_utils(sasl_conn_t *conn,
		  sasl_global_callbacks_t *global_callbacks);
extern int _sasl_free_utils(const sasl_utils_t ** utils);

extern int
_sasl_getcallback(sasl_conn_t * conn,
		  unsigned long callbackid,
		  sasl_callback_ft * pproc,
		  void **pcontext);

extern void
_sasl_log(sasl_conn_t *conn,
	  int level,
	  const char *fmt,
	  ...);

void _sasl_get_errorbuf(sasl_conn_t *conn, char ***bufhdl, size_t **lenhdl);
int _sasl_add_string(char **out, size_t *alloclen,
		     size_t *outlen, const char *add);

/* More Generic Utilities in common.c */
extern int _sasl_strdup(const char *in, char **out, size_t *outlen);

/* Basically a conditional call to realloc(), if we need more */
int _buf_alloc(char **rwbuf, size_t *curlen, size_t newlen);

/* convert an iovec to a single buffer */
int _iovec_to_buf(const struct iovec *vec,
		  unsigned numiov, buffer_info_t **output);

/* Convert between string formats and sockaddr formats */
int _sasl_iptostring(const struct sockaddr *addr, socklen_t addrlen,
		     char *out, unsigned outlen);
int _sasl_ipfromstring(const char *addr, struct sockaddr *out,
		       socklen_t outlen);

/*
 * external plugin (external.c)
 */
int external_client_plug_init(const sasl_utils_t *utils,
			      int max_version,
			      int *out_version,
			      sasl_client_plug_t **pluglist,
			      int *plugcount);
int external_server_plug_init(const sasl_utils_t *utils,
			      int max_version,
			      int *out_version,
			      sasl_server_plug_t **pluglist,
			      int *plugcount);

/* Mech Listing Functions */
int _sasl_build_mechlist(void);
int _sasl_server_listmech(sasl_conn_t *conn,
			  const char *user,
			  const char *prefix,
			  const char *sep,
			  const char *suffix,
			  const char **result,
			  unsigned *plen,
			  int *pcount);
int _sasl_client_listmech(sasl_conn_t *conn,
			  const char *prefix,
			  const char *sep,
			  const char *suffix,
			  const char **result,
			  unsigned *plen,
			  int *pcount);
/* Just create a straight list of them */
sasl_string_list_t *_sasl_client_mechs(void);
sasl_string_list_t *_sasl_server_mechs(void);

/*
 * config file declarations (config.c)
 */
extern const char *sasl_config_getstring(const char *key,const char *def);

/* checkpw.c */
#ifdef DO_SASL_CHECKAPOP
extern int _sasl_auxprop_verify_apop(sasl_conn_t *conn,
				     const char *userstr,
				     const char *challenge,
				     const char *response,
				     const char *user_realm);
#endif /* DO_SASL_CHECKAPOP */

/* Auxprop Plugin (sasldb.c) */
extern int sasldb_auxprop_plug_init(const sasl_utils_t *utils,
				    int max_version,
				    int *out_version,
				    sasl_auxprop_plug_t **plug,
				    const char *plugname);

/*
 * auxprop.c
 */
extern int _sasl_auxprop_add_plugin(void *p, void *library);
extern void _sasl_auxprop_free(void);
extern int _sasl_auxprop_lookup(sasl_server_params_t *sparams,
				 unsigned flags,
				 const char *user, unsigned ulen);

/*
 * canonusr.c
 */
void _sasl_canonuser_free();
extern int internal_canonuser_init(const sasl_utils_t *utils,
				   int max_version,
				   int *out_version,
				   sasl_canonuser_plug_t **plug,
				   const char *plugname);
extern int _sasl_canon_user(sasl_conn_t *conn,
			    const char *user,
			    unsigned ulen,
			    unsigned flags,
			    sasl_out_params_t *oparams);
int _sasl_canon_user_lookup (sasl_conn_t *conn,
			     const char *user,
			     unsigned ulen,
			     unsigned flags,
			     sasl_out_params_t *oparams);

/*
 * saslutil.c
 */
int get_fqhostname(
  char *name,  
  int namelen,
  int abort_if_no_fqdn
  );

#ifndef HAVE_GETHOSTNAME
#ifdef sun
/* gotta define gethostname ourselves on suns */
extern int gethostname(char *, int);
#endif
#endif /* HAVE_GETHOSTNAME */

#ifdef WIN32
char* _sasl_wchar_to_utf8(WCHAR *str);
WCHAR* _sasl_utf8_to_wchar(const char *str);
#endif

#endif /* SASLINT_H */
