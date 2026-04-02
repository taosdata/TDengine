/* This is a proposed C API for support of SASL
 *
 *********************************IMPORTANT*******************************
 * send email to chris.newman@innosoft.com and cyrus-bugs@andrew.cmu.edu *
 * if you need to add new error codes, callback types, property values,  *
 * etc.   It is important to keep the multiple implementations of this   *
 * API from diverging.                                                   *
 *********************************IMPORTANT*******************************
 *
 * Basic Type Summary:
 *  sasl_conn_t       Context for a SASL connection negotiation
 *  sasl_ssf_t        Security layer Strength Factor
 *  sasl_callback_t   A typed client/server callback function and context
 *  sasl_interact_t   A client interaction descriptor
 *  sasl_secret_t     A client password
 *  sasl_rand_t       Random data context structure
 *  sasl_security_properties_t  An application's required security level
 *
 * Callbacks:
 *  sasl_getopt_t     client/server: Get an option value
 *  sasl_logmsg_t     client/server: Log message handler
 *  sasl_getsimple_t  client: Get user/language list
 *  sasl_getsecret_t  client: Get authentication secret
 *  sasl_chalprompt_t client: Display challenge and prompt for response
 *
 * Server only Callbacks:
 *  sasl_authorize_t             user authorization policy callback
 *  sasl_getconfpath_t           get path to search for config file
 *  sasl_server_userdb_checkpass check password and auxprops in userdb
 *  sasl_server_userdb_setpass   set password in userdb
 *  sasl_server_canon_user       canonicalize username routine
 *
 * Client/Server Function Summary:
 *  sasl_done         Release all SASL global state
 *  sasl_dispose      Connection done: Dispose of sasl_conn_t
 *  sasl_getprop      Get property (e.g., user name, security layer info)
 *  sasl_setprop      Set property (e.g., external ssf)
 *  sasl_errdetail    Generate string from last error on connection
 *  sasl_errstring    Translate sasl error code to a string
 *  sasl_encode       Encode data to send using security layer
 *  sasl_decode       Decode data received using security layer
 *  
 * Utility functions:
 *  sasl_encode64     Encode data to send using MIME base64 encoding
 *  sasl_decode64     Decode data received using MIME base64 encoding
 *  sasl_erasebuffer  Erase a buffer
 *
 * Client Function Summary:
 *  sasl_client_init  Load and initialize client plug-ins (call once)
 *  sasl_client_new   Initialize client connection context: sasl_conn_t
 *  sasl_client_start Select mechanism for connection
 *  sasl_client_step  Perform one authentication step
 *
 * Server Function Summary
 *  sasl_server_init  Load and initialize server plug-ins (call once)
 *  sasl_server_new   Initialize server connection context: sasl_conn_t
 *  sasl_listmech     Create list of available mechanisms
 *  sasl_server_start Begin an authentication exchange
 *  sasl_server_step  Perform one authentication exchange step
 *  sasl_checkpass    Check a plaintext passphrase
 *  sasl_checkapop    Check an APOP challenge/response (uses pseudo "APOP"
 *                    mechanism similar to CRAM-MD5 mechanism; optional)
 *  sasl_user_exists  Check if user exists
 *  sasl_setpass      Change a password or add a user entry
 *  sasl_auxprop_request  Request auxiliary properties
 *  sasl_auxprop_getctx   Get auxiliary property context for connection
 *  sasl_auxprop_store    Store a set of auxiliary properties
 *
 * Basic client model:
 *  1. client calls sasl_client_init() at startup to load plug-ins
 *  2. when connection formed, call sasl_client_new()
 *  3. once list of supported mechanisms received from server, client
 *     calls sasl_client_start().  goto 4a
 *  4. client calls sasl_client_step()
 * [4a. If SASL_INTERACT, fill in prompts and goto 4
 *      -- doesn't happen if callbacks provided]
 *  4b. If SASL error, goto 7 or 3
 *  4c. If SASL_OK, continue or goto 6 if last server response was success
 *  5. send message to server, wait for response
 *  5a. On data or success with server response, goto 4
 *  5b. On failure goto 7 or 3
 *  5c. On success with no server response continue
 *  6. continue with application protocol until connection closes
 *     call sasl_getprop/sasl_encode/sasl_decode() if using security layer
 *  7. call sasl_dispose(), may return to step 2
 *  8. call sasl_done() when program terminates
 *
 * Basic Server model:
 *  1. call sasl_server_init() at startup to load plug-ins
 *  2. On connection, call sasl_server_new()
 *  3. call sasl_listmech() and send list to client]
 *  4. after client AUTH command, call sasl_server_start(), goto 5a
 *  5. call sasl_server_step()
 *  5a. If SASL_CONTINUE, output to client, wait response, repeat 5
 *  5b. If SASL error, then goto 7
 *  5c. If SASL_OK, move on
 *  6. continue with application protocol until connection closes
 *     call sasl_getprop to get username
 *     call sasl_getprop/sasl_encode/sasl_decode() if using security layer
 *  7. call sasl_dispose(), may return to step 2
 *  8. call sasl_done() when program terminates
 *
 *************************************************
 * IMPORTANT NOTE: server realms / username syntax
 *
 * If a user name contains a "@", then the rightmost "@" in the user name
 * separates the account name from the realm in which this account is
 * located.  A single server may support multiple realms.  If the
 * server knows the realm at connection creation time (e.g., a server
 * with multiple IP addresses tightly binds one address to a specific
 * realm) then that realm must be passed in the user_realm field of
 * the sasl_server_new call.  If user_realm is non-empty and an
 * unqualified user name is supplied, then the canon_user facility is
 * expected to append "@" and user_realm to the user name.  The canon_user
 * facility may treat other characters such as "%" as equivalent to "@".
 *
 * If the server forbids the use of "@" in user names for other
 * purposes, this simplifies security validation.
 */

#ifndef SASL_H
#define SASL_H 1

#include <stddef.h>  /* For size_t */

/* Keep in sync with win32/common.mak */
#define SASL_VERSION_MAJOR 2
#define SASL_VERSION_MINOR 1
#define SASL_VERSION_STEP 28

/* A convenience macro: same as was defined in the OpenLDAP LDAPDB */
#define SASL_VERSION_FULL ((SASL_VERSION_MAJOR << 16) |\
      (SASL_VERSION_MINOR << 8) | SASL_VERSION_STEP)

#include "prop.h"

/*************
 * Basic API *
 *************/

/* SASL result codes: */
#define SASL_CONTINUE    1   /* another step is needed in authentication */
#define SASL_OK          0   /* successful result */
#define SASL_FAIL       -1   /* generic failure */
#define SASL_NOMEM      -2   /* memory shortage failure */
#define SASL_BUFOVER    -3   /* overflowed buffer */
#define SASL_NOMECH     -4   /* mechanism not supported */
#define SASL_BADPROT    -5   /* bad protocol / cancel */
#define SASL_NOTDONE    -6   /* can't request info until later in exchange */
#define SASL_BADPARAM   -7   /* invalid parameter supplied */
#define SASL_TRYAGAIN   -8   /* transient failure (e.g., weak key) */
#define SASL_BADMAC	-9   /* integrity check failed */
#define SASL_NOTINIT    -12  /* SASL library not initialized */
                             /* -- client only codes -- */
#define SASL_INTERACT    2   /* needs user interaction */
#define SASL_BADSERV    -10  /* server failed mutual authentication step */
#define SASL_WRONGMECH  -11  /* mechanism doesn't support requested feature */
                             /* -- server only codes -- */
#define SASL_BADAUTH    -13  /* authentication failure */
#define SASL_NOAUTHZ    -14  /* authorization failure */
#define SASL_TOOWEAK    -15  /* mechanism too weak for this user */
#define SASL_ENCRYPT    -16  /* encryption needed to use mechanism */
#define SASL_TRANS      -17  /* One time use of a plaintext password will
				enable requested mechanism for user */
#define SASL_EXPIRED    -18  /* passphrase expired, has to be reset */
#define SASL_DISABLED   -19  /* account disabled */
#define SASL_NOUSER     -20  /* user not found */
#define SASL_BADVERS    -23  /* version mismatch with plug-in */
#define SASL_UNAVAIL    -24  /* remote authentication server unavailable */
#define SASL_NOVERIFY   -26  /* user exists, but no verifier for user */
			     /* -- codes for password setting -- */
#define SASL_PWLOCK     -21  /* passphrase locked */
#define SASL_NOCHANGE   -22  /* requested change was not needed */
#define SASL_WEAKPASS   -27  /* passphrase is too weak for security policy */
#define SASL_NOUSERPASS -28  /* user supplied passwords not permitted */
#define SASL_NEED_OLD_PASSWD -29 /* sasl_setpass needs old password in order
				    to perform password change */
#define SASL_CONSTRAINT_VIOLAT	-30 /* a property can't be stored,
				       because of some constrains/policy violation */

#define SASL_BADBINDING -32  /* channel binding failure */
#define SASL_CONFIGERR  -100 /* error when parsing configuration file */

/* max size of a sasl mechanism name */
#define SASL_MECHNAMEMAX 20

#ifdef _WIN32
/* Define to have the same layout as a WSABUF */
#ifndef STRUCT_IOVEC_DEFINED
#define STRUCT_IOVEC_DEFINED 1
struct iovec {
    long iov_len;
    char *iov_base;
};
#endif
#else
struct iovec;				     /* Defined in OS headers */
#endif


/* per-connection SASL negotiation state for client or server
 */
typedef struct sasl_conn sasl_conn_t;

/* Plain text password structure.
 *  len is the length of the password, data is the text.
 */
typedef struct sasl_secret {
    unsigned long len;
    unsigned char data[1];		/* variable sized */
} sasl_secret_t;

/* random data context structure
 */
typedef struct sasl_rand_s sasl_rand_t;

#ifdef __cplusplus
extern "C" {
#endif

/****************************
 * Configure Basic Services *
 ****************************/

/* the following functions are used to adjust how allocation and mutexes work
 * they must be called before all other SASL functions:
 */

#include <sys/types.h>

/* memory allocation functions which may optionally be replaced:
 */
typedef void *sasl_malloc_t(size_t);
typedef void *sasl_calloc_t(size_t, size_t);
typedef void *sasl_realloc_t(void *, size_t);
typedef void sasl_free_t(void *);

LIBSASL_API void sasl_set_alloc(sasl_malloc_t *,
				sasl_calloc_t *,
				sasl_realloc_t *,
				sasl_free_t *);

/* mutex functions which may optionally be replaced:
 *  sasl_mutex_alloc allocates a mutex structure
 *  sasl_mutex_lock blocks until mutex locked
 *   returns -1 on deadlock or parameter error
 *   returns 0 on success
 *  sasl_mutex_unlock unlocks mutex if it's locked
 *   returns -1 if not locked or parameter error
 *   returns 0 on success
 *  sasl_mutex_free frees a mutex structure
 */
typedef void *sasl_mutex_alloc_t(void);
typedef int sasl_mutex_lock_t(void *mutex);
typedef int sasl_mutex_unlock_t(void *mutex);
typedef void sasl_mutex_free_t(void *mutex);
LIBSASL_API void sasl_set_mutex(sasl_mutex_alloc_t *, sasl_mutex_lock_t *,
				sasl_mutex_unlock_t *, sasl_mutex_free_t *);

/*****************************
 * Security preference types *
 *****************************/

/* security layer strength factor -- an unsigned integer usable by the caller
 *  to specify approximate security layer strength desired.  Roughly
 *  correlated to effective key length for encryption.
 * 0   = no protection
 * 1   = integrity protection only
 * 40  = 40-bit DES or 40-bit RC2/RC4
 * 56  = DES
 * 112 = triple-DES
 * 128 = 128-bit RC2/RC4/BLOWFISH
 * 256 = baseline AES
 */
typedef unsigned sasl_ssf_t;

/* usage flags provided to sasl_server_new and sasl_client_new:
 */
#define SASL_SUCCESS_DATA    0x0004 /* server supports data on success */
#define SASL_NEED_PROXY      0x0008 /* require a mech that allows proxying */
#define SASL_NEED_HTTP       0x0010 /* require a mech that can do HTTP auth */

/***************************
 * Security Property Types *
 ***************************/

/* Structure specifying the client or server's security policy
 * and optional additional properties.
 */

/* These are the various security flags apps can specify. */
/* NOPLAINTEXT          -- don't permit mechanisms susceptible to simple
 *                         passive attack (e.g., PLAIN, LOGIN)
 * NOACTIVE             -- protection from active (non-dictionary) attacks
 *                         during authentication exchange.
 *                         Authenticates server.
 * NODICTIONARY         -- don't permit mechanisms susceptible to passive
 *                         dictionary attack
 * FORWARD_SECRECY      -- require forward secrecy between sessions
 *                         (breaking one won't help break next)
 * NOANONYMOUS          -- don't permit mechanisms that allow anonymous login
 * PASS_CREDENTIALS     -- require mechanisms which pass client
 *			   credentials, and allow mechanisms which can pass
 *			   credentials to do so
 * MUTUAL_AUTH          -- require mechanisms which provide mutual
 *			   authentication
 */
#define SASL_SEC_NOPLAINTEXT      0x0001
#define SASL_SEC_NOACTIVE         0x0002
#define SASL_SEC_NODICTIONARY     0x0004
#define SASL_SEC_FORWARD_SECRECY  0x0008
#define SASL_SEC_NOANONYMOUS      0x0010
#define SASL_SEC_PASS_CREDENTIALS 0x0020
#define SASL_SEC_MUTUAL_AUTH      0x0040
#define SASL_SEC_MAXIMUM          0xFFFF

/* This is used when adding hash size to the security_flags field */
/* NB: hash size is in bits */
#define SASL_SET_HASH_STRENGTH_BITS(x)    (((x) / 8) << 16)

/* NB: This value is in bytes */
#define SASL_GET_HASH_STRENGTH(x)    ((x) >> 16)

typedef struct sasl_security_properties 
{ 
    /* security strength factor
     *  min_ssf      = minimum acceptable final level
     *  max_ssf      = maximum acceptable final level
     */ 
    sasl_ssf_t min_ssf;
    sasl_ssf_t max_ssf;

    /* Maximum security layer receive buffer size.
     *  0=security layer not supported
     */
    unsigned maxbufsize; 
    
    /* bitfield for attacks to protect against */
    unsigned security_flags;

    /* NULL terminated array of additional property names, values */ 
    const char **property_names;
    const char **property_values;
} sasl_security_properties_t; 

/******************
 * Callback types *
 ******************/

/*
 * Extensible type for a client/server callbacks
 *  id      -- identifies callback type
 *  proc    -- procedure call arguments vary based on id
 *  context -- context passed to procedure
 */
/* Note that any memory that is allocated by the callback needs to be
 * freed by the application, be it via function call or interaction.
 *
 * It may be freed after sasl_*_step returns SASL_OK.  if the mechanism
 * requires this information to persist (for a security layer, for example)
 * it must maintain a private copy.
 */
typedef struct sasl_callback {
    /* Identifies the type of the callback function.
     * Mechanisms must ignore callbacks with id's they don't recognize.
     */
    unsigned long id;
    int (*proc)(void);   /* Callback function.  Types of arguments vary by 'id' */
    void *context;
} sasl_callback_t;

/* callback ids & functions:
 */
#define SASL_CB_LIST_END   0  /* end of list */

/* option reading callback -- this allows a SASL configuration to be
 *  encapsulated in the caller's configuration system.  Some implementations
 *  may use default config file(s) if this is omitted.  Configuration items
 *  may be plugin-specific and are arbitrary strings.
 *
 * inputs:
 *  context     -- option context from callback record
 *  plugin_name -- name of plugin (NULL = general SASL option)
 *  option      -- name of option
 * output:
 *  result      -- set to result which persists until next getopt in
 *                 same thread, unchanged if option not found
 *  len         -- length of result (may be NULL)
 * returns:
 *  SASL_OK     -- no error
 *  SASL_FAIL   -- error
 */
typedef int sasl_getopt_t(void *context, const char *plugin_name,
			  const char *option,
			  const char **result, unsigned *len);
#define SASL_CB_GETOPT       1

/* Logging levels for use with the logging callback function. */
#define SASL_LOG_NONE  0	/* don't log anything */
#define SASL_LOG_ERR   1	/* log unusual errors (default) */
#define SASL_LOG_FAIL  2	/* log all authentication failures */
#define SASL_LOG_WARN  3	/* log non-fatal warnings */
#define SASL_LOG_NOTE  4	/* more verbose than LOG_WARN */
#define SASL_LOG_DEBUG 5	/* more verbose than LOG_NOTE */
#define SASL_LOG_TRACE 6	/* traces of internal protocols */
#define SASL_LOG_PASS  7	/* traces of internal protocols, including
				 * passwords */

/* logging callback -- this allows plugins and the middleware to
 *  log operations they perform.
 * inputs:
 *  context     -- logging context from the callback record
 *  level       -- logging level; see above
 *  message     -- message to log
 * returns:
 *  SASL_OK     -- no error
 *  SASL_FAIL   -- error
 */
typedef int sasl_log_t(void *context,
		       int level,
		       const char *message);
#define SASL_CB_LOG	    2

/* getpath callback -- this allows applications to specify the
 * colon-separated path to search for plugins (by default,
 * taken from an implementation-specific location).
 * inputs:
 *  context     -- getpath context from the callback record
 * outputs:
 *  path	-- colon seperated path
 * returns:
 *  SASL_OK     -- no error
 *  SASL_FAIL   -- error
 */
typedef int sasl_getpath_t(void *context,
			   const char **path);

#define SASL_CB_GETPATH	    3

/* verify file callback -- this allows applications to check if they
 * want SASL to use files, file by file.  This is intended to allow
 * applications to sanity check the environment to make sure plugins
 * or the configuration file can't be written to, etc.
 * inputs: 
 *  context     -- verifypath context from the callback record
 *  file        -- full path to file to verify
 *  type        -- type of file to verify (see below)

 * returns:
 *  SASL_OK        -- no error (file can safely be used)
 *  SASL_CONTINUE  -- continue WITHOUT using this file
 *  SASL_FAIL      -- error 
 */

/* these are the types of files libsasl will ask about */
typedef enum {
    SASL_VRFY_PLUGIN=0,		/* a DLL/shared library plug-in */
    SASL_VRFY_CONF=1,		/* a configuration file */
    SASL_VRFY_PASSWD=2,		/* a password storage file/db */
    SASL_VRFY_OTHER=3		/* some other file */
} sasl_verify_type_t;

typedef int sasl_verifyfile_t(void *context,
                              const char *file, sasl_verify_type_t type);
#define SASL_CB_VERIFYFILE  4

/* getconfpath callback -- this allows applications to specify the
 * colon-separated path to search for config files (by default,
 * taken from the SASL_CONF_PATH environment variable).
 * inputs:
 *  context     -- getconfpath context from the callback record
 * outputs:
 *  path        -- colon seperated path (allocated on the heap; the
 *                 library will free it using the sasl_free_t *
 *                 passed to sasl_set_callback, or the standard free()
 *                 library call).
 * returns:
 *  SASL_OK     -- no error
 *  SASL_FAIL   -- error
 */
typedef int sasl_getconfpath_t(void *context,
                               char **path);

#define SASL_CB_GETCONFPATH  5

/* client/user interaction callbacks:
 */
/* Simple prompt -- result must persist until next call to getsimple on
 *  same connection or until connection context is disposed
 * inputs:
 *  context       -- context from callback structure
 *  id            -- callback id
 * outputs:
 *  result        -- set to NUL terminated string
 *                   NULL = user cancel
 *  len           -- length of result
 * returns SASL_OK
 */
typedef int sasl_getsimple_t(void *context, int id,
			     const char **result, unsigned *len);
#define SASL_CB_USER         0x4001  /* client user identity to login as */
#define SASL_CB_AUTHNAME     0x4002  /* client authentication name */
#define SASL_CB_LANGUAGE     0x4003  /* comma separated list of RFC 1766
			              * language codes in order of preference
				      * to be used to localize client prompts
				      * or server error codes */
#define SASL_CB_CNONCE       0x4007  /* caller supplies client-nonce
				      * primarily for testing purposes */

/* get a sasl_secret_t (plaintext password with length)
 * inputs:
 *  conn          -- connection context
 *  context       -- context from callback structure
 *  id            -- callback id
 * outputs:
 *  psecret       -- set to NULL to cancel
 *                   set to password structure which must persist until
 *                   next call to getsecret in same connection, but middleware
 *                   will erase password data when it's done with it.
 * returns SASL_OK
 */
typedef int sasl_getsecret_t(sasl_conn_t *conn, void *context, int id,
			     sasl_secret_t **psecret);
#define SASL_CB_PASS         0x4004  /* client passphrase-based secret */


/* prompt for input in response to a challenge.
 * input:
 *  context   -- context from callback structure
 *  id        -- callback id
 *  challenge -- server challenge
 * output:
 *  result    -- NUL terminated result, NULL = user cancel
 *  len       -- length of result
 * returns SASL_OK
 */
typedef int sasl_chalprompt_t(void *context, int id,
			      const char *challenge,
			      const char *prompt, const char *defresult,
			      const char **result, unsigned *len);
#define SASL_CB_ECHOPROMPT   0x4005 /* challenge and client enterred result */
#define SASL_CB_NOECHOPROMPT 0x4006 /* challenge and client enterred result */

/* prompt (or autoselect) the realm to do authentication in.
 *  may get a list of valid realms.
 * input:
 *  context     -- context from callback structure
 *  id          -- callback id
 *  availrealms -- available realms; string list; NULL terminated
 *                 list may be empty.
 * output:
 *  result      -- NUL terminated realm; NULL is equivalent to ""
 * returns SASL_OK
 * result must persist until the next callback
 */
typedef int sasl_getrealm_t(void *context, int id,
			    const char **availrealms,
			    const char **result);
#define SASL_CB_GETREALM (0x4008) /* realm to attempt authentication in */

/* server callbacks:
 */

/* improved callback to verify authorization;
 *     canonicalization now handled elsewhere
 *  conn           -- connection context
 *  requested_user -- the identity/username to authorize (NUL terminated)
 *  rlen           -- length of requested_user
 *  auth_identity  -- the identity associated with the secret (NUL terminated)
 *  alen           -- length of auth_identity
 *  default_realm  -- default user realm, as passed to sasl_server_new if
 *  urlen          -- length of default realm
 *  propctx        -- auxiliary properties
 * returns SASL_OK on success,
 *         SASL_NOAUTHZ or other SASL response on failure
 */
typedef int sasl_authorize_t(sasl_conn_t *conn,
			     void *context,
			     const char *requested_user, unsigned rlen,
			     const char *auth_identity, unsigned alen,
			     const char *def_realm, unsigned urlen,
			     struct propctx *propctx);
#define SASL_CB_PROXY_POLICY 0x8001

/* functions for "userdb" based plugins to call to get/set passwords.
 * the location for the passwords is determined by the caller or middleware.
 * plug-ins may get passwords from other locations.
 */

/* callback to verify a plaintext password against the caller-supplied
 * user database.  This is necessary to allow additional <method>s for
 * encoding of the userPassword property.
 *  user          -- NUL terminated user name with user@realm syntax
 *  pass          -- password to check (may not be NUL terminated)
 *  passlen       -- length of password to check
 *  propctx       -- auxiliary properties for user
 */
typedef int sasl_server_userdb_checkpass_t(sasl_conn_t *conn,
					   void *context,
					   const char *user,
					   const char *pass,
					   unsigned passlen,
					   struct propctx *propctx);
#define SASL_CB_SERVER_USERDB_CHECKPASS (0x8005)

/* callback to store/change a plaintext password in the user database
 *  user          -- NUL terminated user name with user@realm syntax
 *  pass          -- password to store (may not be NUL terminated)
 *  passlen       -- length of password to store
 *  propctx       -- auxiliary properties (not stored)
 *  flags         -- see SASL_SET_* flags below (SASL_SET_CREATE optional)
 */
typedef int sasl_server_userdb_setpass_t(sasl_conn_t *conn,
					 void *context,
					 const char *user,
					 const char *pass,
					 unsigned passlen,
					 struct propctx *propctx,
					 unsigned flags);
#define SASL_CB_SERVER_USERDB_SETPASS (0x8006)

/* callback for a server-supplied user canonicalization function.
 *
 * This function is called directly after the mechanism has the
 * authentication and authorization IDs.  It is called before any
 * User Canonicalization plugin is called.  It has the responsibility
 * of copying its output into the provided output buffers.
 * 
 *  in, inlen     -- user name to canonicalize, may not be NUL terminated
 *                   may be same buffer as out
 *  flags         -- not currently used, supplied by auth mechanism
 *  user_realm    -- the user realm (may be NULL in case of client)
 *  out           -- buffer to copy user name
 *  out_max       -- max length of user name
 *  out_len       -- set to length of user name
 *
 * returns
 *  SASL_OK         on success
 *  SASL_BADPROT    username contains invalid character
 */

/* User Canonicalization Function Flags */

#define SASL_CU_NONE    0x00 /* Not a valid flag to pass */
/* One of the following two is required */
#define SASL_CU_AUTHID  0x01
#define SASL_CU_AUTHZID 0x02

/* Combine the following with SASL_CU_AUTHID, if you don't want
   to fail if auxprop returned SASL_NOUSER/SASL_NOMECH. */
#define SASL_CU_EXTERNALLY_VERIFIED 0x04

#define SASL_CU_OVERRIDE	    0x08    /* mapped to SASL_AUXPROP_OVERRIDE */

/* The following CU flags are passed "as is" down to auxprop lookup */
#define SASL_CU_ASIS_MASK	    0xFFF0
/* NOTE: Keep in sync with SASL_AUXPROP_<XXX> flags */
#define SASL_CU_VERIFY_AGAINST_HASH 0x10


typedef int sasl_canon_user_t(sasl_conn_t *conn,
			      void *context,
			      const char *in, unsigned inlen,
			      unsigned flags,
			      const char *user_realm,
			      char *out,
			      unsigned out_max, unsigned *out_len);

#define SASL_CB_CANON_USER (0x8007)

/**********************************
 * Common Client/server functions *
 **********************************/

/* Types of paths to set (see sasl_set_path below). */
#define SASL_PATH_TYPE_PLUGIN	0
#define SASL_PATH_TYPE_CONFIG	1

/* a simpler way to set plugin path or configuration file path
 * without the need to set sasl_getpath_t callback.
 *
 * This function can be called before sasl_server_init/sasl_client_init.
 */  
LIBSASL_API int sasl_set_path (int path_type, char * path);

/* get sasl library version information
 * implementation is a vendor-defined string
 * version is a vender-defined representation of the version #.
 *
 * This function is being deprecated in favor of sasl_version_info. */
LIBSASL_API void sasl_version(const char **implementation,
			      int *version);

/* Extended version of sasl_version().
 *
 * This function is to be used
 *  for library version display and logging
 *  for bug workarounds in old library versions
 *
 * The sasl_version_info is not to be used for API feature detection.
 *
 * All parameters are optional. If NULL is specified, the value is not returned.
 */
LIBSASL_API void sasl_version_info (const char **implementation,
				const char **version_string,
				int *version_major,
				int *version_minor,
				int *version_step,
				int *version_patch);

/* dispose of all SASL plugins.  Connection
 * states have to be disposed of before calling this.
 *
 * This function is DEPRECATED in favour of sasl_server_done/
 * sasl_client_done.
 */
LIBSASL_API void sasl_done(void);

/* dispose of all SASL plugins.  Connection
 * states have to be disposed of before calling this.
 * This function should be called instead of sasl_done(),
   whenever possible.
 */
LIBSASL_API int sasl_server_done(void);

/* dispose of all SASL plugins.  Connection
 * states have to be disposed of before calling this.
 * This function should be called instead of sasl_done(),
   whenever possible.
 */
LIBSASL_API int sasl_client_done(void);

/* dispose connection state, sets it to NULL
 *  checks for pointer to NULL
 */
LIBSASL_API void sasl_dispose(sasl_conn_t **pconn);

/* translate an error number into a string
 * input:
 *  saslerr  -- the error number
 *  langlist -- comma separated list of RFC 1766 languages (may be NULL)
 * results:
 *  outlang  -- the language actually used (may be NULL if don't care)
 * returns:
 *  the error message in UTF-8 (only the US-ASCII subset if langlist is NULL)
 */
LIBSASL_API const char *sasl_errstring(int saslerr,
				       const char *langlist,
				       const char **outlang);

/* get detail about the last error that occurred on a connection
 * text is sanitized so it's suitable to send over the wire
 * (e.g., no distinction between SASL_BADAUTH and SASL_NOUSER)
 * input:
 *  conn          -- mandatory connection context
 * returns:
 *  the error message in UTF-8 (only the US-ASCII subset permitted if no
 *  SASL_CB_LANGUAGE callback is present)
 */
LIBSASL_API const char *sasl_errdetail(sasl_conn_t *conn);

/* set the error string which will be returned by sasl_errdetail() using
 *  syslog()-style formatting (e.g. printf-style with %m as most recent
 *  errno error)
 *
 *  primarily for use by server callbacks such as the sasl_authorize_t
 *  callback and internally to plug-ins
 *
 * This will also trigger a call to the SASL logging callback (if any)
 * with a level of SASL_LOG_FAIL unless the SASL_NOLOG flag is set.
 *
 * Messages should be sensitive to the current language setting.  If there
 * is no SASL_CB_LANGUAGE callback messages MUST be US-ASCII otherwise UTF-8
 * is used and use of RFC 2482 for mixed-language text is encouraged.
 *
 * if conn is NULL, function does nothing
 */
LIBSASL_API void sasl_seterror(sasl_conn_t *conn, unsigned flags,
			       const char *fmt, ...);
#define SASL_NOLOG       0x01
			   
/* get property from SASL connection state
 *  propnum       -- property number
 *  pvalue        -- pointer to value
 * returns:
 *  SASL_OK       -- no error
 *  SASL_NOTDONE  -- property not available yet
 *  SASL_BADPARAM -- bad property number
 */
LIBSASL_API int sasl_getprop(sasl_conn_t *conn, int propnum,
			     const void **pvalue);
#define SASL_USERNAME     0	/* pointer to NUL terminated user name */
#define SASL_SSF          1	/* security layer security strength factor,
                                 * if 0, call to sasl_encode, sasl_decode
                                 * unnecessary */
#define SASL_MAXOUTBUF    2     /* security layer max output buf unsigned */  
#define SASL_DEFUSERREALM 3	/* default realm passed to server_new */
				/* or set with setprop */
#define SASL_GETOPTCTX    4	/* context for getopt callback */
#define SASL_CALLBACK     7	/* current callback function list */
#define SASL_IPLOCALPORT  8	/* iplocalport string passed to server_new */
#define SASL_IPREMOTEPORT 9	/* ipremoteport string passed to server_new */

/* This returns a string which is either empty or has an error message
 * from sasl_seterror (e.g., from a plug-in or callback).  It differs
 * from the result of sasl_errdetail() which also takes into account the
 * last return status code.
 */
#define SASL_PLUGERR     10

/* a handle to any delegated credentials or NULL if none is present 
 * is returned by the mechanism. The user will probably need to know
 * which mechanism was used to actually known how to make use of them
 * currently only implemented for the gssapi mechanism */
#define SASL_DELEGATEDCREDS 11

#define SASL_SERVICE      12	/* service passed to sasl_*_new */
#define SASL_SERVERFQDN   13	/* serverFQDN passed to sasl_*_new */
#define SASL_AUTHSOURCE   14	/* name of auth source last used, useful
				 * for failed authentication tracking */
#define SASL_MECHNAME     15    /* active mechanism name, if any */
#define SASL_AUTHUSER     16    /* authentication/admin user */
#define SASL_APPNAME	  17	/* application name (used for logging/
				   configuration), same as appname parameter
				   to sasl_server_init */

/* GSS-API credential handle for sasl_client_step() or sasl_server_step().
 * The application is responsible for releasing this credential handle. */
#define	SASL_GSS_CREDS	  18

/* GSS name (gss_name_t) of the peer, as output by gss_inquire_context()
 * or gss_accept_sec_context().
 * On server end this is similar to SASL_USERNAME, but the gss_name_t
 * structure can contain additional attributes associated with the peer.
 */
#define	SASL_GSS_PEER_NAME	19

/* Local GSS name (gss_name_t) as output by gss_inquire_context(). This
 * is particularly useful for servers that respond to multiple names. */
#define	SASL_GSS_LOCAL_NAME	20

/* Channel binding information. Memory is managed by the caller. */
typedef struct sasl_channel_binding {
    const char *name;
    int critical;
    unsigned long len;
    const unsigned char *data;
} sasl_channel_binding_t;

#define SASL_CHANNEL_BINDING    21

/* HTTP Request (RFC 2616) - ONLY used for HTTP Digest Auth (RFC 2617) */
typedef struct sasl_http_request {
    const char *method;			/* HTTP Method */
    const char *uri;			/* request-URI */
    const unsigned char *entity;	/* entity-body */
    unsigned long elen;			/* entity-body length */
    unsigned non_persist;		/* Is it a non-persistent connection? */
} sasl_http_request_t;

#define SASL_HTTP_REQUEST	22

/* set property in SASL connection state
 * returns:
 *  SASL_OK       -- value set
 *  SASL_BADPARAM -- invalid property or value
 */
LIBSASL_API int sasl_setprop(sasl_conn_t *conn,
			     int propnum,
			     const void *value);
#define SASL_SSF_EXTERNAL  100	/* external SSF active (sasl_ssf_t *) */
#define SASL_SEC_PROPS     101	/* sasl_security_properties_t */
#define SASL_AUTH_EXTERNAL 102	/* external authentication ID (const char *) */

/* If the SASL_AUTH_EXTERNAL value is non-NULL, then a special version of the
 * EXTERNAL mechanism is enabled (one for server-embedded EXTERNAL mechanisms).
 * Otherwise, the EXTERNAL mechanism will be absent unless a plug-in
 * including EXTERNAL is present.
 */

/* do precalculations during an idle period or network round trip
 *  may pass NULL to precompute for some mechanisms prior to connect
 *  returns 1 if action taken, 0 if no action taken
 */
LIBSASL_API int sasl_idle(sasl_conn_t *conn);

/**************
 * Client API *
 **************/

/* list of client interactions with user for caller to fill in
 */
typedef struct sasl_interact {
    unsigned long id;		/* same as client/user callback ID */
    const char *challenge;	/* presented to user (e.g. OTP challenge) */
    const char *prompt;		/* presented to user (e.g. "Username: ") */
    const char *defresult;	/* default result string */
    const void *result;		/* set to point to result */
    unsigned len;		/* set to length of result */
} sasl_interact_t;

/* initialize the SASL client drivers
 *  callbacks      -- base callbacks for all client connections;
 *                    must include getopt callback
 * returns:
 *  SASL_OK        -- Success
 *  SASL_NOMEM     -- Not enough memory
 *  SASL_BADVERS   -- Mechanism version mismatch
 *  SASL_BADPARAM  -- missing getopt callback or error in config file
 *  SASL_NOMECH    -- No mechanisms available
 *  ...
 */
LIBSASL_API int sasl_client_init(const sasl_callback_t *callbacks);

/* initialize a client exchange based on the specified mechanism
 *  service       -- registered name of the service using SASL (e.g. "imap")
 *  serverFQDN    -- the fully qualified domain name of the server
 *  iplocalport   -- client IPv4/IPv6 domain literal string with port
 *                    (if NULL, then mechanisms requiring IPaddr are disabled)
 *  ipremoteport  -- server IPv4/IPv6 domain literal string with port
 *                    (if NULL, then mechanisms requiring IPaddr are disabled)
 *  prompt_supp   -- list of client interactions supported
 *                   may also include sasl_getopt_t context & call
 *                   NULL prompt_supp = user/pass via SASL_INTERACT only
 *                   NULL proc = interaction supported via SASL_INTERACT
 *  flags         -- server usage flags (see above)
 * in/out:
 *  pconn         -- connection negotiation structure
 *                   pointer to NULL => allocate new
 *
 * Returns:
 *  SASL_OK       -- success
 *  SASL_NOMECH   -- no mechanism meets requested properties
 *  SASL_NOMEM    -- not enough memory
 */
LIBSASL_API int sasl_client_new(const char *service,
				const char *serverFQDN,
				const char *iplocalport,
				const char *ipremoteport,
				const sasl_callback_t *prompt_supp,
				unsigned flags,
				sasl_conn_t **pconn);

/* select a mechanism for a connection
 *  mechlist      -- mechanisms server has available (punctuation ignored)
 *                   if NULL, then discard cached info and retry last mech
 * output:
 *  prompt_need   -- on SASL_INTERACT, list of prompts needed to continue
 *                   may be NULL if callbacks provided
 *  clientout     -- the initial client response to send to the server
 *                   will be valid until next call to client_start/client_step
 *                   NULL if mech doesn't include initial client challenge
 *  mech          -- set to mechansm name of selected mechanism (may be NULL)
 *
 * Returns:
 *  SASL_OK       -- success
 *  SASL_NOMEM    -- not enough memory
 *  SASL_NOMECH   -- no mechanism meets requested properties
 *  SASL_INTERACT -- user interaction needed to fill in prompt_need list
 */
LIBSASL_API int sasl_client_start(sasl_conn_t *conn,
				  const char *mechlist,
				  sasl_interact_t **prompt_need,
				  const char **clientout,
				  unsigned *clientoutlen,
				  const char **mech);

/* do a single authentication step.
 *  serverin    -- the server message received by the client, MUST have a NUL
 *                 sentinel, not counted by serverinlen
 * output:
 *  prompt_need -- on SASL_INTERACT, list of prompts needed to continue
 *  clientout   -- the client response to send to the server
 *                 will be valid until next call to client_start/client_step
 *
 * returns:
 *  SASL_OK        -- success
 *  SASL_INTERACT  -- user interaction needed to fill in prompt_need list
 *  SASL_BADPROT   -- server protocol incorrect/cancelled
 *  SASL_BADSERV   -- server failed mutual auth
 */
LIBSASL_API int sasl_client_step(sasl_conn_t *conn,
				 const char *serverin,
				 unsigned serverinlen,
				 sasl_interact_t **prompt_need,
				 const char **clientout,
				 unsigned *clientoutlen);

/**************
 * Server API *
 **************/

/* initialize server drivers, done once per process
 *  callbacks      -- callbacks for all server connections; must include
 *                    getopt callback
 *  appname        -- name of calling application (for lower level logging)
 * results:
 *  state          -- server state
 * returns:
 *  SASL_OK        -- success
 *  SASL_BADPARAM  -- error in config file
 *  SASL_NOMEM     -- memory failure
 *  SASL_BADVERS   -- Mechanism version mismatch
 */
LIBSASL_API int sasl_server_init(const sasl_callback_t *callbacks,
				 const char *appname);

/* IP/port syntax:
 *  a.b.c.d;p              where a-d are 0-255 and p is 0-65535 port number.
 *  e:f:g:h:i:j:k:l;p      where e-l are 0000-ffff lower-case hexidecimal
 *  e:f:g:h:i:j:a.b.c.d;p  alternate syntax for previous
 *
 *  Note that one or more "0" fields in f-k can be replaced with "::"
 *  Thus:                 e:f:0000:0000:0000:j:k:l;p
 *  can be abbreviated:   e:f::j:k:l;p
 *
 * A buffer of size 52 is adequate for the longest format with NUL terminator.
 */

/* create context for a single SASL connection
 *  service        -- registered name of the service using SASL (e.g. "imap")
 *  serverFQDN     -- Fully qualified domain name of server.  NULL means use
 *                    gethostname() or equivalent.
 *                    Useful for multi-homed servers.
 *  user_realm     -- permits multiple user realms on server, NULL = default
 *  iplocalport    -- server IPv4/IPv6 domain literal string with port
 *                    (if NULL, then mechanisms requiring IPaddr are disabled)
 *  ipremoteport   -- client IPv4/IPv6 domain literal string with port
 *                    (if NULL, then mechanisms requiring IPaddr are disabled)
 *  callbacks      -- callbacks (e.g., authorization, lang, new getopt context)
 *  flags          -- usage flags (see above)
 * returns:
 *  pconn          -- new connection context
 *
 * returns:
 *  SASL_OK        -- success
 *  SASL_NOMEM     -- not enough memory
 */
LIBSASL_API int sasl_server_new(const char *service,
				const char *serverFQDN,
				const char *user_realm,
				const char *iplocalport,
				const char *ipremoteport,
				const sasl_callback_t *callbacks,
				unsigned flags,
				sasl_conn_t **pconn);

/* Return an array of NUL-terminated strings, terminated by a NULL pointer,
 * which lists all possible mechanisms that the library can supply
 *
 * Returns NULL on failure. */
LIBSASL_API const char ** sasl_global_listmech(void);

/* This returns a list of mechanisms in a NUL-terminated string
 *  conn          -- the connection to list mechanisms for (either client
 *                   or server)
 *  user          -- restricts mechanisms to those available to that user
 *                   (may be NULL, not used for client case)
 *  prefix        -- appended to beginning of result
 *  sep           -- appended between mechanisms
 *  suffix        -- appended to end of result
 * results:
 *  result        -- NUL terminated result which persists until next
 *                   call to sasl_listmech for this sasl_conn_t
 *  plen          -- gets length of result (excluding NUL), may be NULL
 *  pcount        -- gets number of mechanisms, may be NULL
 *
 * returns:
 *  SASL_OK        -- success
 *  SASL_NOMEM     -- not enough memory
 *  SASL_NOMECH    -- no enabled mechanisms
 */
LIBSASL_API int sasl_listmech(sasl_conn_t *conn,
			      const char *user,
			      const char *prefix,
			      const char *sep,
			      const char *suffix,
			      const char **result,
			      unsigned *plen,
			      int *pcount);

/* start a mechanism exchange within a connection context
 *  mech           -- the mechanism name client requested
 *  clientin       -- client initial response (NUL terminated), NULL if empty
 *  clientinlen    -- length of initial response
 *  serverout      -- initial server challenge, NULL if done 
 *                    (library handles freeing this string)
 *  serveroutlen   -- length of initial server challenge
 * output:
 *  pconn          -- the connection negotiation state on success
 *
 * Same returns as sasl_server_step() or
 * SASL_NOMECH if mechanism not available.
 */
LIBSASL_API int sasl_server_start(sasl_conn_t *conn,
				  const char *mech,
				  const char *clientin,
				  unsigned clientinlen,
				  const char **serverout,
				  unsigned *serveroutlen);

/* perform one step of the SASL exchange
 *  inputlen & input -- client data
 *                      NULL on first step if no optional client step
 *  outputlen & output -- set to the server data to transmit
 *                        to the client in the next step
 *                        (library handles freeing this)
 *
 * returns:
 *  SASL_OK        -- exchange is complete.
 *  SASL_CONTINUE  -- indicates another step is necessary.
 *  SASL_TRANS     -- entry for user exists, but not for mechanism
 *                    and transition is possible
 *  SASL_BADPARAM  -- service name needed
 *  SASL_BADPROT   -- invalid input from client
 *  ...
 */
LIBSASL_API int sasl_server_step(sasl_conn_t *conn,
				 const char *clientin,
				 unsigned clientinlen,
				 const char **serverout,
				 unsigned *serveroutlen);

/* check if an apop exchange is valid
 *  (note this is an optional part of the SASL API)
 *  if challenge is NULL, just check if APOP is enabled
 * inputs:
 *  challenge     -- challenge which was sent to client
 *  challen       -- length of challenge, 0 = strlen(challenge)
 *  response      -- client response, "<user> <digest>" (RFC 1939)
 *  resplen       -- length of response, 0 = strlen(response)
 * returns 
 *  SASL_OK       -- success
 *  SASL_BADAUTH  -- authentication failed
 *  SASL_BADPARAM -- missing challenge
 *  SASL_BADPROT  -- protocol error (e.g., response in wrong format)
 *  SASL_NOVERIFY -- user found, but no verifier
 *  SASL_NOMECH   -- mechanism not supported
 *  SASL_NOUSER   -- user not found
 */
LIBSASL_API int sasl_checkapop(sasl_conn_t *conn,
			       const char *challenge, unsigned challen,
			       const char *response, unsigned resplen);

/* check if a plaintext password is valid
 *   if user is NULL, check if plaintext passwords are enabled
 * inputs:
 *  user          -- user to query in current user_domain
 *  userlen       -- length of username, 0 = strlen(user)
 *  pass          -- plaintext password to check
 *  passlen       -- length of password, 0 = strlen(pass)
 * returns 
 *  SASL_OK       -- success
 *  SASL_NOMECH   -- mechanism not supported
 *  SASL_NOVERIFY -- user found, but no verifier
 *  SASL_NOUSER   -- user not found
 */
LIBSASL_API int sasl_checkpass(sasl_conn_t *conn,
			       const char *user, unsigned userlen,
			       const char *pass, unsigned passlen);

/* check if a user exists on server
 *  conn          -- connection context
 *  service       -- registered name of the service using SASL (e.g. "imap")
 *  user_realm    -- permits multiple user realms on server, NULL = default
 *  user          -- NUL terminated user name
 *
 * returns:
 *  SASL_OK       -- success
 *  SASL_DISABLED -- account disabled
 *  SASL_NOUSER   -- user not found
 *  SASL_NOVERIFY -- user found, but no usable mechanism
 *  SASL_NOMECH   -- no mechanisms enabled
 *  SASL_UNAVAIL  -- remote authentication server unavailable, try again later
 */
LIBSASL_API int sasl_user_exists(sasl_conn_t *conn,
				 const char *service,
				 const char *user_realm,
				 const char *user);

/* set the password for a user
 *  conn        -- SASL connection
 *  user        -- user name
 *  pass        -- plaintext password, may be NULL to remove user
 *  passlen     -- length of password, 0 = strlen(pass)
 *  oldpass     -- NULL will sometimes work
 *  oldpasslen  -- length of password, 0 = strlen(oldpass)
 *  flags       -- see flags below
 * 
 * returns:
 *  SASL_NOCHANGE  -- proper entry already exists
 *  SASL_NOMECH    -- no authdb supports password setting as configured
 *  SASL_NOVERIFY  -- user exists, but no settable password present
 *  SASL_DISABLED  -- account disabled
 *  SASL_PWLOCK    -- password locked
 *  SASL_WEAKPASS  -- password too weak for security policy
 *  SASL_NOUSERPASS -- user-supplied passwords not permitted
 *  SASL_FAIL      -- OS error
 *  SASL_BADPARAM  -- password too long
 *  SASL_OK        -- successful
 */
LIBSASL_API int sasl_setpass(sasl_conn_t *conn,
			     const char *user,
			     const char *pass, unsigned passlen,
			     const char *oldpass, unsigned oldpasslen,
			     unsigned flags);
#define SASL_SET_CREATE  0x01   /* create a new entry for user */
#define SASL_SET_DISABLE 0x02	/* disable user account */
#define SASL_SET_NOPLAIN 0x04	/* do not store secret in plain text */
#define SASL_SET_CURMECH_ONLY 0x08	/* set the mechanism specific password only.
					   fail if no current mechanism */

/*********************************************************
 * Auxiliary Property Support -- added by cjn 1999-09-29 *
 *********************************************************/

#define SASL_AUX_END      NULL	/* last auxiliary property */

#define SASL_AUX_ALL "*" /* A special flag to signal user deletion */

/* traditional Posix items (should be implemented on Posix systems) */
#define SASL_AUX_PASSWORD_PROP "userPassword" /* User Password */
#define SASL_AUX_PASSWORD "*" SASL_AUX_PASSWORD_PROP /* User Password (of authid) */
#define SASL_AUX_UIDNUM   "uidNumber"	/* UID number for the user */
#define SASL_AUX_GIDNUM   "gidNumber"	/* GID for the user */
#define SASL_AUX_FULLNAME "gecos"	/* full name of the user, unix-style */
#define SASL_AUX_HOMEDIR  "homeDirectory" /* home directory for user */
#define SASL_AUX_SHELL    "loginShell"	/* login shell for the user */

/* optional additional items (not necessarily implemented) */
/* single preferred mail address for user canonically-quoted
 * RFC821/822 syntax */
#define SASL_AUX_MAILADDR "mail"
/* path to unix-style mailbox for user */
#define SASL_AUX_UNIXMBX  "mailMessageStore"
/* SMTP mail channel name to use if user authenticates successfully */
#define SASL_AUX_MAILCHAN "mailSMTPSubmitChannel"

/* Request a set of auxiliary properties
 *  conn         connection context
 *  propnames    list of auxiliary property names to request ending with
 *               NULL.  
 *
 * Subsequent calls will add items to the request list.  Call with NULL
 * to clear the request list.
 *
 * errors
 *  SASL_OK       -- success
 *  SASL_BADPARAM -- bad count/conn parameter
 *  SASL_NOMEM    -- out of memory
 */
LIBSASL_API int sasl_auxprop_request(sasl_conn_t *conn,
				     const char **propnames);

/* Returns current auxiliary property context.
 * Use functions in prop.h to access content
 *
 *  if authentication hasn't completed, property values may be empty/NULL
 *
 *  properties not recognized by active plug-ins will be left empty/NULL
 *
 *  returns NULL if conn is invalid.
 */
LIBSASL_API struct propctx *sasl_auxprop_getctx(sasl_conn_t *conn);

/* Store the set of auxiliary properties for the given user.
 * Use functions in prop.h to set the content.
 *
 *  conn         connection context
 *  ctx          property context from prop_new()/prop_request()/prop_set()
 *  user         NUL terminated user
 *
 * Call with NULL 'ctx' to see if the backend allows storing properties.
 *
 * errors
 *  SASL_OK       -- success
 *  SASL_NOMECH   -- can not store some/all properties
 *  SASL_BADPARAM -- bad conn/ctx/user parameter
 *  SASL_NOMEM    -- out of memory
 *  SASL_FAIL     -- failed to store
 */
LIBSASL_API int sasl_auxprop_store(sasl_conn_t *conn,
				   struct propctx *ctx, const char *user);

/**********************
 * security layer API *
 **********************/

/* encode a block of data for transmission using security layer,
 *  returning the input buffer if there is no security layer.
 *  output is only valid until next call to sasl_encode or sasl_encodev
 * returns:
 *  SASL_OK      -- success (returns input if no layer negotiated)
 *  SASL_NOTDONE -- security layer negotiation not finished
 *  SASL_BADPARAM -- inputlen is greater than the SASL_MAXOUTBUF
 */
LIBSASL_API int sasl_encode(sasl_conn_t *conn,
			    const char *input, unsigned inputlen,
			    const char **output, unsigned *outputlen);

/* encode a block of data for transmission using security layer
 *  output is only valid until next call to sasl_encode or sasl_encodev
 * returns:
 *  SASL_OK      -- success (returns input if no layer negotiated)
 *  SASL_NOTDONE -- security layer negotiation not finished
 *  SASL_BADPARAM -- input length is greater than the SASL_MAXOUTBUF
 *		     or no security layer
 */
LIBSASL_API int sasl_encodev(sasl_conn_t *conn,
			     const struct iovec *invec, unsigned numiov,
			     const char **output, unsigned *outputlen);

/* decode a block of data received using security layer
 *  returning the input buffer if there is no security layer.
 *  output is only valid until next call to sasl_decode
 *
 *  if outputlen is 0 on return, than the value of output is undefined.
 *  
 * returns:
 *  SASL_OK      -- success (returns input if no layer negotiated)
 *  SASL_NOTDONE -- security layer negotiation not finished
 *  SASL_BADMAC  -- bad message integrity check
 */
LIBSASL_API int sasl_decode(sasl_conn_t *conn,
			    const char *input, unsigned inputlen,
			    const char **output, unsigned *outputlen);

#ifdef __cplusplus
}
#endif

#endif /* SASL_H */
