/* saslplug.h --  API for SASL plug-ins
 */

#ifndef SASLPLUG_H
#define SASLPLUG_H 1

#ifndef MD5GLOBAL_H
#include "md5global.h"
#endif
#ifndef MD5_H
#include "md5.h"
#endif
#ifndef HMAC_MD5_H
#include "hmac-md5.h"
#endif
#ifndef PROP_H
#include "prop.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* callback to lookup a sasl_callback_t for a connection
 * input:
 *  conn        -- the connection to lookup a callback for
 *  callbacknum -- the number of the callback
 * output:
 *  pproc       -- pointer to the callback function (set to NULL on failure)
 *  pcontext    -- pointer to the callback context (set to NULL on failure)
 * returns:
 *  SASL_OK -- no error
 *  SASL_FAIL -- unable to find a callback of the requested type
 *  SASL_INTERACT -- caller must use interaction to get data
 */
typedef int (*sasl_callback_ft)(void);
typedef int sasl_getcallback_t(sasl_conn_t *conn,
			       unsigned long callbackid,
			       sasl_callback_ft * pproc,
			       void **pcontext);

/* The sasl_utils structure will remain backwards compatible unless
 * the SASL_*_PLUG_VERSION is changed incompatibly
 * higher SASL_UTILS_VERSION numbers indicate more functions are available
 */
#define SASL_UTILS_VERSION 4

/* utility function set for plug-ins
 */
typedef struct sasl_utils {
    int version;

    /* contexts */
    sasl_conn_t *conn;
    sasl_rand_t *rpool;
    void *getopt_context;

    /* option function */
    sasl_getopt_t *getopt;
    
    /* allocation functions: */
    sasl_malloc_t *malloc;
    sasl_calloc_t *calloc;
    sasl_realloc_t *realloc;
    sasl_free_t *free;

    /* mutex functions: */
    sasl_mutex_alloc_t *mutex_alloc;
    sasl_mutex_lock_t *mutex_lock;
    sasl_mutex_unlock_t *mutex_unlock;
    sasl_mutex_free_t *mutex_free;

    /* MD5 hash and HMAC functions */
    void (*MD5Init)(MD5_CTX *);
    void (*MD5Update)(MD5_CTX *, const unsigned char *text, unsigned int len);
    void (*MD5Final)(unsigned char [16], MD5_CTX *);
    void (*hmac_md5)(const unsigned char *text, int text_len,
		     const unsigned char *key, int key_len,
		     unsigned char [16]);
    void (*hmac_md5_init)(HMAC_MD5_CTX *, const unsigned char *key, int len);
    /* hmac_md5_update() is just a call to MD5Update on inner context */
    void (*hmac_md5_final)(unsigned char [16], HMAC_MD5_CTX *);
    void (*hmac_md5_precalc)(HMAC_MD5_STATE *,
			     const unsigned char *key, int len);
    void (*hmac_md5_import)(HMAC_MD5_CTX *, HMAC_MD5_STATE *);

    /* mechanism utility functions (same as above): */
    int (*mkchal)(sasl_conn_t *conn, char *buf, unsigned maxlen,
		  unsigned hostflag);
    int (*utf8verify)(const char *str, unsigned len);
    void (*rand)(sasl_rand_t *rpool, char *buf, unsigned len);
    void (*churn)(sasl_rand_t *rpool, const char *data, unsigned len);

    /* This allows recursive calls to the sasl_checkpass() routine from
     * within a SASL plug-in.  This MUST NOT be used in the PLAIN mechanism
     * as sasl_checkpass MAY be a front-end for the PLAIN mechanism.
     * This is intended for use by the non-standard LOGIN mechanism and
     * potentially by a future mechanism which uses public-key technology to
     * set up a lightweight encryption layer just for sending a password.
     */
    int (*checkpass)(sasl_conn_t *conn,
		     const char *user, unsigned userlen,
		     const char *pass, unsigned passlen);
    
    /* Access to base64 encode/decode routines */
    int (*decode64)(const char *in, unsigned inlen,
		    char *out, unsigned outmax, unsigned *outlen);
    int (*encode64)(const char *in, unsigned inlen,
		    char *out, unsigned outmax, unsigned *outlen);

    /* erase a buffer */
    void (*erasebuffer)(char *buf, unsigned len);

    /* callback to sasl_getprop() and sasl_setprop() */
    int (*getprop)(sasl_conn_t *conn, int propnum, const void **pvalue);
    int (*setprop)(sasl_conn_t *conn, int propnum, const void *value);

    /* callback function */
    sasl_getcallback_t *getcallback;

    /* format a message and then pass it to the SASL_CB_LOG callback
     *
     * use syslog()-style formatting (printf with %m as a human readable text
     * (strerror()) for the error specified as the parameter).
     * The implementation may use a fixed size buffer not smaller
     * than 512 octets if it securely truncates the message.
     *
     * level is a SASL_LOG_* level (see sasl.h)
     */
    void (*log)(sasl_conn_t *conn, int level, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

    /* callback to sasl_seterror() */
    void (*seterror)(sasl_conn_t *conn, unsigned flags, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

    /* spare function pointer */
    int *(*spare_fptr)(void);

    /* auxiliary property utilities */
    struct propctx *(*prop_new)(unsigned estimate);
    int (*prop_dup)(struct propctx *src_ctx, struct propctx **dst_ctx);
    int (*prop_request)(struct propctx *ctx, const char **names);
    const struct propval *(*prop_get)(struct propctx *ctx);
    int (*prop_getnames)(struct propctx *ctx, const char **names,
			 struct propval *vals);
    void (*prop_clear)(struct propctx *ctx, int requests);
    void (*prop_dispose)(struct propctx **ctx);
    int (*prop_format)(struct propctx *ctx, const char *sep, int seplen,
		       char *outbuf, unsigned outmax, unsigned *outlen);
    int (*prop_set)(struct propctx *ctx, const char *name,
		    const char *value, int vallen);
    int (*prop_setvals)(struct propctx *ctx, const char *name,
			const char **values);
    void (*prop_erase)(struct propctx *ctx, const char *name);
    int (*auxprop_store)(sasl_conn_t *conn,
			 struct propctx *ctx, const char *user);

    /* for additions which don't require a version upgrade; set to 0 */
    int (*spare_fptr1)(void);
    int (*spare_fptr2)(void);
} sasl_utils_t;

/*
 * output parameters from SASL API
 *
 * created / destroyed by the glue code, though probably filled in
 * by a combination of the plugin, the glue code, and the canon_user callback.
 *
 */
typedef struct sasl_out_params {
    unsigned doneflag;		/* exchange complete */

    const char *user;		/* canonicalized user name */
    const char *authid;		/* canonicalized authentication id */

    unsigned ulen;		/* length of canonicalized user name */
    unsigned alen;		/* length of canonicalized authid */

    /* security layer information */
    unsigned maxoutbuf;         /* Maximum buffer size, which will
                                   produce buffer no bigger than the
                                   negotiated SASL maximum buffer size */
    sasl_ssf_t mech_ssf;   /* Should be set non-zero if negotiation of a
	 		    * security layer was *attempted*, even if
			    * the negotiation failed */
    void *encode_context;
    int (*encode)(void *context, const struct iovec *invec, unsigned numiov,
		  const char **output, unsigned *outputlen);
    void *decode_context;
    int (*decode)(void *context, const char *input, unsigned inputlen,
		  const char **output, unsigned *outputlen);
    
    /* Pointer to delegated (client's) credentials, if supported by
       the SASL mechanism */
    void *client_creds;

    /* for additions which don't require a version upgrade; set to 0 */
    const void *gss_peer_name;
    const void *gss_local_name;
    const char *cbindingname;   /* channel binding name from packet */
    int (*spare_fptr1)(void);
    int (*spare_fptr2)(void);
    unsigned int cbindingdisp;  /* channel binding disposition from client */
    int spare_int2;
    int spare_int3;
    int spare_int4;

    /* set to 0 initially, this allows a plugin with extended parameters
     * to work with an older framework by updating version as parameters
     * are added.
     */
    int param_version;
} sasl_out_params_t;



/* Used by both client and server side plugins */
typedef enum  {
    SASL_INFO_LIST_START = 0,
    SASL_INFO_LIST_MECH,
    SASL_INFO_LIST_END
} sasl_info_callback_stage_t;

/******************************
 * Channel binding macros     **
 ******************************/

typedef enum {
    SASL_CB_DISP_NONE = 0,          /* client did not support CB */
    SASL_CB_DISP_WANT,              /* client supports CB, thinks server does not */
    SASL_CB_DISP_USED               /* client supports and used CB */
} sasl_cbinding_disp_t;

/* TRUE if channel binding is non-NULL */
#define SASL_CB_PRESENT(params)     ((params)->cbinding != NULL)
/* TRUE if channel binding is marked critical */
#define SASL_CB_CRITICAL(params)    (SASL_CB_PRESENT(params) && \
				     (params)->cbinding->critical)

/******************************
 * Client Mechanism Functions *
 ******************************/

/*
 * input parameters to client SASL plugin
 *
 * created / destroyed by the glue code
 *
 */
typedef struct sasl_client_params {
    const char *service;	/* service name */
    const char *serverFQDN;	/* server fully qualified domain name */
    const char *clientFQDN;	/* client's fully qualified domain name */
    const sasl_utils_t *utils;	/* SASL API utility routines --
				 * for a particular sasl_conn_t,
				 * MUST remain valid until mech_free is
				 * called */
    const sasl_callback_t *prompt_supp; /* client callback list */
    const char *iplocalport;	/* server IP domain literal & port */
    const char *ipremoteport;	/* client IP domain literal & port */

    unsigned servicelen;	/* length of service */
    unsigned slen;		/* length of serverFQDN */
    unsigned clen;		/* length of clientFQDN */
    unsigned iploclen;		/* length of iplocalport */
    unsigned ipremlen;		/* length of ipremoteport */

    /* application's security requirements & info */
    sasl_security_properties_t props;
    sasl_ssf_t external_ssf;	/* external SSF active */

    /* for additions which don't require a version upgrade; set to 0 */
    const void *gss_creds;                  /* GSS credential handle */
    const sasl_channel_binding_t *cbinding; /* client channel binding */
    const sasl_http_request_t *http_request;/* HTTP Digest request method */
    void *spare_ptr4;

    /* Canonicalize a user name from on-wire to internal format
     *  added rjs3 2001-05-23
     *  Must be called once user name aquired if canon_user is non-NULL.
     *  conn        connection context
     *  in          user name from wire protocol (need not be NUL terminated)
     *  len         length of user name from wire protocol (0 = strlen(user))
     *  flags       for SASL_CU_* flags
     *  oparams     the user, authid, ulen, alen, fields are
     *              set appropriately after canonicalization/copying and
     *              authorization of arguments
     *
     *  responsible for setting user, ulen, authid, and alen in the oparams
     *  structure
     *
     *  default behavior is to strip leading and trailing whitespace, as
     *  well as allocating space for and copying the parameters.
     *
     * results:
     *  SASL_OK       -- success
     *  SASL_NOMEM    -- out of memory
     *  SASL_BADPARAM -- invalid conn
     *  SASL_BADPROT  -- invalid user/authid
     */
    int (*canon_user)(sasl_conn_t *conn,
                    const char *in, unsigned len,
                    unsigned flags,
                    sasl_out_params_t *oparams);

    int (*spare_fptr1)(void);

    unsigned int cbindingdisp;
    int spare_int2;
    int spare_int3;

    /* flags field as passed to sasl_client_new */
    unsigned flags;

    /* set to 0 initially, this allows a plugin with extended parameters
     * to work with an older framework by updating version as parameters
     * are added.
     */
    int param_version;
} sasl_client_params_t;

/* features shared between client and server */
/* These allow the glue code to handle client-first and server-last issues */

/* This indicates that the mechanism prefers to do client-send-first
 * if the protocol allows it. */
#define SASL_FEAT_WANT_CLIENT_FIRST	0x0002

/* This feature is deprecated.  Instead, plugins should set *serverout to
 * non-NULL and return SASL_OK intelligently to allow flexible use of
 * server-last semantics
#define SASL_FEAT_WANT_SERVER_LAST	0x0004
*/

/* This feature is deprecated.  Instead, plugins should correctly set
 * SASL_FEAT_SERVER_FIRST as needed
#define SASL_FEAT_INTERNAL_CLIENT_FIRST	0x0008
*/

/* This indicates that the plugin is server-first only. 
 * Not defining either of SASL_FEAT_SERVER_FIRST or 
 * SASL_FEAT_WANT_CLIENT_FIRST indicates that the mechanism
 * will handle the client-first situation internally.
 */
#define SASL_FEAT_SERVER_FIRST		0x0010

/* This plugin allows proxying */
#define SASL_FEAT_ALLOWS_PROXY		0x0020

/* server plugin don't use cleartext userPassword attribute */
#define SASL_FEAT_DONTUSE_USERPASSWD 	0x0080

/* Underlying mechanism uses GSS framing */
#define SASL_FEAT_GSS_FRAMING	 	0x0100

/* Underlying mechanism supports channel binding */
#define SASL_FEAT_CHANNEL_BINDING	0x0800

/* This plugin can be used for HTTP authentication */
#define SASL_FEAT_SUPPORTS_HTTP	    	0x1000

/* client plug-in features */
#define SASL_FEAT_NEEDSERVERFQDN	0x0001

/* a C object for a client mechanism
 */
typedef struct sasl_client_plug {
    /* mechanism name */
    const char *mech_name;

    /* best mech additional security layer strength factor */
    sasl_ssf_t max_ssf;

    /* best security flags, as defined in sasl_security_properties_t */
    unsigned security_flags;

    /* features of plugin */
    unsigned features;

    /* required prompt ids, NULL = user/pass only */
    const unsigned long *required_prompts;
    
    /* global state for mechanism */
    void *glob_context;
    
    /* create context for mechanism, using params supplied
     *  glob_context   -- from above
     *  params         -- params from sasl_client_new
     *  conn_context   -- context for one connection
     * returns:
     *  SASL_OK        -- success
     *  SASL_NOMEM     -- not enough memory
     *  SASL_WRONGMECH -- mech doesn't support security params
     */
    int (*mech_new)(void *glob_context,
		    sasl_client_params_t *cparams,
		    void **conn_context);
    
    /* perform one step of exchange.  NULL is passed for serverin on
     * first step.
     * returns:
     *  SASL_OK        -- success
     *  SASL_INTERACT  -- user interaction needed to fill in prompts
     *  SASL_BADPROT   -- server protocol incorrect/cancelled
     *  SASL_BADSERV   -- server failed mutual auth
     */
    int (*mech_step)(void *conn_context,
		     sasl_client_params_t *cparams,
		     const char *serverin,
		     unsigned serverinlen,
		     sasl_interact_t **prompt_need,
		     const char **clientout,
		     unsigned *clientoutlen,
		     sasl_out_params_t *oparams);
    
    /* dispose of connection context from mech_new
     */
    void (*mech_dispose)(void *conn_context, const sasl_utils_t *utils);
    
    /* free all global space used by mechanism
     *  mech_dispose must be called on all mechanisms first
     */
    void (*mech_free)(void *glob_context, const sasl_utils_t *utils);
     
    /* perform precalculations during a network round-trip
     *  or idle period.  conn_context may be NULL
     *  returns 1 if action taken, 0 if no action taken
     */
    int (*idle)(void *glob_context,
		void *conn_context,
		sasl_client_params_t *cparams);

    /* for additions which don't require a version upgrade; set to 0 */
    int (*spare_fptr1)(void);
    int (*spare_fptr2)(void);
} sasl_client_plug_t;

#define SASL_CLIENT_PLUG_VERSION         4

/* plug-in entry point:
 *  utils       -- utility callback functions
 *  max_version -- highest client plug version supported
 * returns:
 *  out_version -- client plug version of result
 *  pluglist    -- list of mechanism plug-ins
 *  plugcount   -- number of mechanism plug-ins
 * results:
 *  SASL_OK       -- success
 *  SASL_NOMEM    -- failure
 *  SASL_BADVERS  -- max_version too small
 *  SASL_BADPARAM -- bad config string
 *  ...
 */
typedef int sasl_client_plug_init_t(const sasl_utils_t *utils,
				    int max_version,
				    int *out_version,
				    sasl_client_plug_t **pluglist,
				    int *plugcount);


/* add a client plug-in
 */
LIBSASL_API int sasl_client_add_plugin(const char *plugname,
				       sasl_client_plug_init_t *cplugfunc);

typedef struct client_sasl_mechanism
{
    int version;

    char *plugname;
    const sasl_client_plug_t *plug;
} client_sasl_mechanism_t;

typedef void sasl_client_info_callback_t (client_sasl_mechanism_t *m,
					  sasl_info_callback_stage_t stage,
					  void *rock);

/* Dump information about available client plugins */
LIBSASL_API int sasl_client_plugin_info (const char *mech_list,
	sasl_client_info_callback_t *info_cb,
	void *info_cb_rock);


/********************
 * Server Functions *
 ********************/

/* log message formatting routine */
typedef void sasl_logmsg_p(sasl_conn_t *conn, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

/*
 * input parameters to server SASL plugin
 *
 * created / destroyed by the glue code
 *
 */
typedef struct sasl_server_params {
    const char *service;	/* NULL = default service for user_exists
				   and setpass */
    const char *appname;	/* name of calling application */
    const char *serverFQDN;	/* server default fully qualified domain name
				 * (e.g., gethostname) */
    const char *user_realm;	/* realm for user (NULL = client supplied) */
    const char *iplocalport;	/* server IP domain literal & port */
    const char *ipremoteport;	/* client IP domain literal & port */

    unsigned servicelen;	/* length of service */
    unsigned applen;		/* length of appname */
    unsigned slen;		/* length of serverFQDN */
    unsigned urlen;		/* length of user_realm */
    unsigned iploclen;		/* length of iplocalport */
    unsigned ipremlen;		/* length of ipremoteport */

    /* This indicates the level of logging desired.  See SASL_LOG_*
     * in sasl.h
     *
     * Plug-ins can ignore this and just pass their desired level to
     * the log callback.  This is primarily used to eliminate logging which
     * might be a performance problem (e.g., full protocol trace) and
     * to select between SASL_LOG_TRACE and SASL_LOG_PASS alternatives
     */
    int log_level;

    const sasl_utils_t *utils;	/* SASL API utility routines --
				 * for a particular sasl_conn_t,
				 * MUST remain valid until mech_free is
				 * called */
    const sasl_callback_t *callbacks;	/* Callbacks from application */

    /* application's security requirements */
    sasl_security_properties_t props;
    sasl_ssf_t external_ssf;	/* external SSF active */

    /* Pointer to the function which takes the plaintext passphrase and
     *  transitions a user to non-plaintext mechanisms via setpass calls.
     *  (NULL = auto transition not enabled/supported)
     *
     *  If passlen is 0, it defaults to strlen(pass).
     *  returns 0 if no entry added, 1 if entry added
     */
    int (*transition)(sasl_conn_t *conn, const char *pass, unsigned passlen);

    /* Canonicalize a user name from on-wire to internal format
     *  added cjn 1999-09-21
     *  Must be called once user name acquired if canon_user is non-NULL.
     *  conn        connection context
     *  user        user name from wire protocol (need not be NUL terminated)
     *  ulen        length of user name from wire protocol (0 = strlen(user))
     *  flags       for SASL_CU_* flags
     *  oparams     the user, authid, ulen, alen, fields are
     *              set appropriately after canonicalization/copying and
     *              authorization of arguments
     *
     *  responsible for setting user, ulen, authid, and alen in the oparams
     *  structure
     *
     *  default behavior is to strip leading and trailing whitespace, as
     *  well as allocating space for and copying the parameters.
     *
     * results:
     *  SASL_OK       -- success
     *  SASL_NOMEM    -- out of memory
     *  SASL_BADPARAM -- invalid conn
     *  SASL_BADPROT  -- invalid user/authid
     */
    int (*canon_user)(sasl_conn_t *conn,
		      const char *user, unsigned ulen,
		      unsigned flags,
		      sasl_out_params_t *oparams);
    
    /* auxiliary property context (see definitions in prop.h)
     *  added cjn 2000-01-30
     *
     * NOTE: these properties are the ones associated with the
     * canonicalized "user" (user to login as / authorization id), not
     * the "authid" (user whose credentials are used / authentication id)
     * Prefix the property name with a "*" if a property associated with
     * the "authid" is interesting.
     */
    struct propctx *propctx;

    /* for additions which don't require a version upgrade; set to 0 */
    const void *gss_creds;                  /* GSS credential handle */
    const sasl_channel_binding_t *cbinding; /* server channel binding */
    const sasl_http_request_t *http_request;/* HTTP Digest request method */
    void *spare_ptr4;
    int (*spare_fptr1)(void);
    int (*spare_fptr2)(void);
    int spare_int1;
    int spare_int2;
    int spare_int3;

    /* flags field as passed to sasl_server_new */
    unsigned flags;

    /* set to 0 initially, this allows a plugin with extended parameters
     * to work with an older framework by updating version as parameters
     * are added.
     */
    int param_version;
} sasl_server_params_t;

/* logging levels (more levels may be added later, if necessary):
 */
#define SASL_LOG_NONE  0	/* don't log anything */
#define SASL_LOG_ERR   1	/* log unusual errors (default) */
#define SASL_LOG_FAIL  2	/* log all authentication failures */
#define SASL_LOG_WARN  3	/* log non-fatal warnings */
#define SASL_LOG_NOTE  4	/* more verbose than LOG_WARN */
#define SASL_LOG_DEBUG 5	/* more verbose than LOG_NOTE */
#define SASL_LOG_TRACE 6	/* traces of internal protocols */
#define SASL_LOG_PASS  7	/* traces of internal protocols, including
				 * passwords */

/* additional flags for setpass() function below:
 */
/*      SASL_SET_CREATE                     create user if pass non-NULL */
/*      SASL_SET_DISABLE                    disable user */
#define SASL_SET_REMOVE  SASL_SET_CREATE /* remove user if pass is NULL */

/* features for server plug-in
 */
#define SASL_FEAT_SERVICE    0x0200 /* service-specific passwords supported */
#define SASL_FEAT_GETSECRET  0x0400 /* sasl_server_{get,put}secret_t callbacks
				     * required by plug-in */

/* a C object for a server mechanism
 */
typedef struct sasl_server_plug {
    /* mechanism name */
    const char *mech_name;

    /* best mech additional security layer strength factor */
    sasl_ssf_t max_ssf;

    /* best security flags, as defined in sasl_security_properties_t */
    unsigned security_flags;

    /* features of plugin */
    unsigned features;
    
    /* global state for mechanism */
    void *glob_context;

    /* create a new mechanism handler
     *  glob_context  -- global context
     *  sparams       -- server config params
     *  challenge     -- server challenge from previous instance or NULL
     *  challen       -- length of challenge from previous instance or 0
     * out:
     *  conn_context  -- connection context
     *  errinfo       -- error information
     *
     * returns:
     *  SASL_OK       -- successfully created mech instance
     *  SASL_*        -- any other server error code
     */
    int (*mech_new)(void *glob_context,
		    sasl_server_params_t *sparams,
		    const char *challenge,
		    unsigned challen,
		    void **conn_context);
    
    /* perform one step in exchange
     *
     * returns:
     *  SASL_OK       -- success, all done
     *  SASL_CONTINUE -- success, one more round trip
     *  SASL_*        -- any other server error code
     */
    int (*mech_step)(void *conn_context,
		     sasl_server_params_t *sparams,
		     const char *clientin,
		     unsigned clientinlen,
		     const char **serverout,
		     unsigned *serveroutlen,
		     sasl_out_params_t *oparams);
    
    /* dispose of a connection state
     */
    void (*mech_dispose)(void *conn_context, const sasl_utils_t *utils);
    
    /* free global state for mechanism
     *  mech_dispose must be called on all mechanisms first
     */
    void (*mech_free)(void *glob_context, const sasl_utils_t *utils);
    
    /* set a password (optional)
     *  glob_context  -- global context
     *  sparams       -- service, middleware utilities, etc. props ignored
     *  user          -- user name
     *  pass          -- password/passphrase (NULL = disable/remove/delete)
     *  passlen       -- length of password/passphrase
     *  oldpass       -- old password/passphrase (NULL = transition)
     *  oldpasslen    -- length of password/passphrase
     *  flags         -- see above
     *
     * returns:
     *  SASL_NOCHANGE -- no change was needed
     *  SASL_NOUSER   -- no entry for user
     *  SASL_NOVERIFY -- no mechanism compatible entry for user
     *  SASL_PWLOCK   -- password locked
     *  SASL_DIABLED  -- account disabled
     *  etc.
     */
    int (*setpass)(void *glob_context,
		   sasl_server_params_t *sparams,
		   const char *user,
		   const char *pass, unsigned passlen,
		   const char *oldpass, unsigned oldpasslen,
		   unsigned flags);

    /* query which mechanisms are available for user
     *  glob_context  -- context
     *  sparams       -- service, middleware utilities, etc. props ignored
     *  user          -- NUL terminated user name
     *  maxmech       -- max number of strings in mechlist (0 = no output)
     * output:
     *  mechlist      -- an array of C string pointers, filled in with
     *                   mechanism names available to the user
     *
     * returns:
     *  SASL_OK       -- success
     *  SASL_NOMEM    -- not enough memory
     *  SASL_FAIL     -- lower level failure
     *  SASL_DISABLED -- account disabled
     *  SASL_NOUSER   -- user not found
     *  SASL_BUFOVER  -- maxmech is too small
     *  SASL_NOVERIFY -- user found, but no mechanisms available
     */
    int (*user_query)(void *glob_context,
		      sasl_server_params_t *sparams,
		      const char *user,
		      int maxmech,
		      const char **mechlist);
     
    /* perform precalculations during a network round-trip
     *  or idle period.  conn_context may be NULL (optional)
     *  returns 1 if action taken, 0 if no action taken
     */
    int (*idle)(void *glob_context,
		void *conn_context,
		sasl_server_params_t *sparams);

    /* check if mechanism is available
     *  optional--if NULL, mechanism is available based on ENABLE= in config
     *
     *  If this routine sets conn_context to a non-NULL value, then the call
     *  to mech_new will be skipped.  This should not be done unless
     *  there's a significant performance benefit, since it can cause
     *  additional memory allocation in SASL core code to keep track of
     *  contexts potentially for multiple mechanisms.
     *
     *  This is called by the first call to sasl_listmech() for a
     *  given connection context, thus for a given protocol it may
     *  never be called.  Note that if mech_avail returns SASL_NOMECH,
     *  then that mechanism is considered disabled for the remainder
     *  of the session.  If mech_avail returns SASL_NOTDONE, then a
     *  future call to mech_avail may still return either SASL_OK
     *  or SASL_NOMECH.
     *
     *  returns SASL_OK on success,
     *          SASL_NOTDONE if mech is not available now, but may be later
     *                       (e.g. EXTERNAL w/o auth_id)
     *          SASL_NOMECH if mech disabled
     */
    int (*mech_avail)(void *glob_context,
		      sasl_server_params_t *sparams,
		      void **conn_context);

    /* for additions which don't require a version upgrade; set to 0 */
    int (*spare_fptr2)(void);
} sasl_server_plug_t;

#define SASL_SERVER_PLUG_VERSION 4

/* plug-in entry point:
 *  utils         -- utility callback functions
 *  plugname      -- name of plug-in (may be NULL)
 *  max_version   -- highest server plug version supported
 * returns:
 *  out_version   -- server plug-in version of result
 *  pluglist      -- list of mechanism plug-ins
 *  plugcount     -- number of mechanism plug-ins
 * results:
 *  SASL_OK       -- success
 *  SASL_NOMEM    -- failure
 *  SASL_BADVERS  -- max_version too small
 *  SASL_BADPARAM -- bad config string
 *  ...
 */
typedef int sasl_server_plug_init_t(const sasl_utils_t *utils,
				    int max_version,
				    int *out_version,
				    sasl_server_plug_t **pluglist,
				    int *plugcount);

/* 
 * add a server plug-in
 */
LIBSASL_API int sasl_server_add_plugin(const char *plugname,
				       sasl_server_plug_init_t *splugfunc);


typedef struct server_sasl_mechanism
{
    int version;
    int condition; /* set to SASL_NOUSER if no available users;
		      set to SASL_CONTINUE if delayed plugin loading */
    char *plugname; /* for AUTHSOURCE tracking */
    const sasl_server_plug_t *plug;
    char *f;       /* where should i load the mechanism from? */
} server_sasl_mechanism_t;

typedef void sasl_server_info_callback_t (server_sasl_mechanism_t *m,
					  sasl_info_callback_stage_t stage,
					  void *rock);


/* Dump information about available server plugins (separate functions are
   used for canon and auxprop plugins) */
LIBSASL_API int sasl_server_plugin_info (const char *mech_list,
	sasl_server_info_callback_t *info_cb,
	void *info_cb_rock);


/*********************************************************
 * user canonicalization plug-in -- added cjn 1999-09-29 *
 *********************************************************/

typedef struct sasl_canonuser {
    /* optional features of plugin (set to 0) */
    int features;

    /* spare integer (set to 0) */
    int spare_int1;

    /* global state for plugin */
    void *glob_context;

    /* name of plugin */
    char *name;

    /* free global state for plugin */
    void (*canon_user_free)(void *glob_context, const sasl_utils_t *utils);

    /* canonicalize a username
     *  glob_context     -- global context from this structure
     *  sparams          -- server params, note user_realm&propctx elements
     *  user             -- user to login as (may not be NUL terminated)
     *  len              -- length of user name (0 = strlen(user))
     *  flags            -- for SASL_CU_* flags
     *  out              -- buffer to copy user name
     *  out_max          -- max length of user name
     *  out_len          -- set to length of user name
     *
     *  note that the output buffers MAY be the same as the input buffers.
     *
     * returns
     *  SASL_OK         on success
     *  SASL_BADPROT    username contains invalid character
     */
    int (*canon_user_server)(void *glob_context,
			     sasl_server_params_t *sparams,
			     const char *user, unsigned len,
			     unsigned flags,
			     char *out,
			     unsigned out_umax, unsigned *out_ulen);

    int (*canon_user_client)(void *glob_context,
			     sasl_client_params_t *cparams,
			     const char *user, unsigned len,
			     unsigned flags,
			     char *out,
			     unsigned out_max, unsigned *out_len);

    /* for additions which don't require a version upgrade; set to 0 */
    int (*spare_fptr1)(void);
    int (*spare_fptr2)(void);
    int (*spare_fptr3)(void);
} sasl_canonuser_plug_t;

#define SASL_CANONUSER_PLUG_VERSION 5

/* default name for canonuser plug-in entry point is "sasl_canonuser_init"
 *  similar to sasl_server_plug_init model, except only returns one
 *  sasl_canonuser_plug_t structure;
 */
typedef int sasl_canonuser_init_t(const sasl_utils_t *utils,
				  int max_version,
				  int *out_version,
				  sasl_canonuser_plug_t **plug,
				  const char *plugname);

/* add a canonuser plugin
 */
LIBSASL_API int sasl_canonuser_add_plugin(const char *plugname,
				  sasl_canonuser_init_t *canonuserfunc);

/******************************************************
 * auxiliary property plug-in -- added cjn 1999-09-29 *
 ******************************************************/

typedef struct sasl_auxprop_plug {
    /* optional features of plugin (none defined yet, set to 0) */
    int features;

    /* spare integer, must be set to 0 */
    int spare_int1;

    /* global state for plugin */
    void *glob_context;

    /* free global state for plugin (OPTIONAL) */
    void (*auxprop_free)(void *glob_context, const sasl_utils_t *utils);

    /* fill in fields of an auxiliary property context
     *  last element in array has id of SASL_AUX_END
     *  elements with non-0 len should be ignored.
     */
    int (*auxprop_lookup)(void *glob_context,
			   sasl_server_params_t *sparams,
			   unsigned flags,
			   const char *user, unsigned ulen);

    /* name of the auxprop plugin */
    char *name;

    /* store the fields/values of an auxiliary property context (OPTIONAL)
     *
     * if ctx is NULL, just check if storing properties is enabled
     *
     * returns
     *  SASL_OK         on success
     *  SASL_FAIL       on failure
     */
    int (*auxprop_store)(void *glob_context,
			 sasl_server_params_t *sparams,
			 struct propctx *ctx,
			 const char *user, unsigned ulen);
} sasl_auxprop_plug_t;

/* auxprop lookup flags */
#define SASL_AUXPROP_OVERRIDE 0x01 /* if clear, ignore auxiliary properties
				    * with non-zero len field.  If set,
				    * override value of those properties */
#define SASL_AUXPROP_AUTHZID  0x02 /* if clear, we are looking up the
				    * authid flags (prefixed with *), otherwise
				    * we are looking up the authzid flags
				    * (no prefix) */

/* NOTE: Keep in sync with SASL_CU_<XXX> flags */
#define SASL_AUXPROP_VERIFY_AGAINST_HASH 0x10


#define SASL_AUXPROP_PLUG_VERSION 8

/* default name for auxprop plug-in entry point is "sasl_auxprop_init"
 *  similar to sasl_server_plug_init model, except only returns one
 *  sasl_auxprop_plug_t structure;
 */
typedef int sasl_auxprop_init_t(const sasl_utils_t *utils,
				int max_version,
				int *out_version,
				sasl_auxprop_plug_t **plug,
				const char *plugname);

/* add an auxiliary property plug-in
 */
LIBSASL_API int sasl_auxprop_add_plugin(const char *plugname,
					sasl_auxprop_init_t *auxpropfunc);

typedef void auxprop_info_callback_t (sasl_auxprop_plug_t *m,
			              sasl_info_callback_stage_t stage,
				      void *rock);

/* Dump information about available auxprop plugins (separate functions are
   used for canon and server authentication plugins) */
LIBSASL_API int auxprop_plugin_info (const char *mech_list,
	auxprop_info_callback_t *info_cb,
	void *info_cb_rock);

#ifdef __cplusplus
}
#endif

#endif /* SASLPLUG_H */
