/* cfmglue.c
   by Rolf Braun, for CMU SASL on Mac OS X

   This file provides routines to allow CFM (os 9 linkage) Carbon applications
   to use the native Mach-O SASL libraries on Mac OS X, using CFBundle to
   load the backend libraries and automatically allocated assembly callbacks
   $Id: cfmglue.c,v 1.4 2003/06/12 00:33:05 rbraun Exp $
*/
/* 
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

#include <Carbon.h>
#include <string.h>
#include <stdlib.h>
#include "sasl.h"
#include "saslutil.h"
#include "prop.h"

/* prototypes for internal functions (from saslint.h) */
int _sasl_add_string(char **out, int *alloclen, int *outlen, const char *add);
int _buf_alloc(char **rwbuf, unsigned *curlen, unsigned newlen);
void _sasl_get_errorbuf(sasl_conn_t *conn, char ***bufhdl, unsigned **lenhdl);

void	*MachOFunctionPointerForCFMFunctionPointer( void *cfmfp );
sasl_callback_t *GetCFMCallbacks(const sasl_callback_t *callbacks);
void DisposeCFMCallbacks(sasl_callback_t *callbacks);

int _cfmsasl_haveCustomAlloc = 0;
int _cfmsasl_haveCustomMutex = 0;
int _cfmsasl_initted = 0;

/* DO NOT change the order of this struct! It MUST match that
   of the iovec struct in /usr/include/sys/uio.h on Mac OS X!
   It MUST also match that of the Mac OS 9 Carbon config.h since
   Carbon CFM stuff links at runtime against whatever it's running on! */
struct iovec {
    char *iov_base; 
    long iov_len;
};

typedef struct			
{
	CFURLRef		bundleURL;
	CFBundleRef		myBundle;
	sasl_callback_t *clientCallbacks;
	sasl_callback_t *serverCallbacks;
	int (*SASLClientNewPtr)(const char *service,
							const char *serverFQDN,
							const char *iplocalport,
							const char *ipremoteport,
							const sasl_callback_t *prompt_supp, 
							unsigned flags,
							sasl_conn_t **pconn);
	int (*SASLClientStartPtr)(sasl_conn_t *conn,
								const char *mechlist,
								sasl_interact_t **prompt_need,
								const char **clientout,
								unsigned *clientoutlen,
								const char **mech);
	int (*SASLClientStepPtr)(sasl_conn_t *conn,
								const char *serverin,
								unsigned serverinlen,
								sasl_interact_t **prompt_need,
								const char **clientout,
								unsigned *clientoutlen);
	const char * (*SASLErrStringPtr)(int saslerr,
							const char *langlist,
							const char **outlang);
	const char *(*sasl_errdetailPtr)(sasl_conn_t *conn);
	int (*SASLGetPropPtr)(sasl_conn_t *conn, int propnum, const void **pvalue);
	int (*SASLSetPropPtr)(sasl_conn_t *conn,
							int propnum,
							const void *value);
	int (*SASLIdlePtr)(sasl_conn_t *conn);
	int (*SASLEncodePtr)(sasl_conn_t *conn,
							const char *input, unsigned inputlen,
							const char **output, unsigned *outputlen);
	int (*SASLDecodePtr)(sasl_conn_t *conn,
							const char *input, unsigned inputlen,
							const char **output, unsigned *outputlen);
	int (*SASLEncodeVPtr)(sasl_conn_t *conn,
			     const struct iovec *invec, unsigned numiov,
			     const char **output, unsigned *outputlen);
	int (*SASLDisposePtr)(sasl_conn_t **pconn);
	int (*SASLDonePtr)();
	void (*SASLSetAllocPtr)(sasl_malloc_t *, sasl_calloc_t *, sasl_realloc_t *, sasl_free_t *);
	int (*sasl_decode64Ptr)(const char *in, unsigned inlen,
			      char *out, unsigned outmax, unsigned *outlen);
	int (*sasl_encode64Ptr)(const char *in, unsigned inlen,
			      char *out, unsigned outmax, unsigned *outlen);
	int (*sasl_mkchalPtr)(sasl_conn_t *conn, char *buf,
			    unsigned maxlen, unsigned hostflag);
	int (*sasl_utf8verifyPtr)(const char *str, unsigned len);
	void (*sasl_churnPtr)(sasl_rand_t *rpool, 
			    const char *data,
			    unsigned len);
	void (*sasl_randPtr)(sasl_rand_t *rpool,
			   char *buf,
			   unsigned len);
	void (*sasl_randseedPtr)(sasl_rand_t *rpool,
			       const char *seed,
			       unsigned len);
	void (*sasl_randfreePtr)(sasl_rand_t **rpool);
	int (*sasl_randcreatePtr)(sasl_rand_t **rpool);
	void (*SASLSetMutexPtr)(sasl_mutex_alloc_t *mn, sasl_mutex_lock_t *ml,
                                sasl_mutex_unlock_t *mu, sasl_mutex_free_t *md);
	int (*SASLServerNewPtr)(const char *service,
						const char *serverFQDN,
						const char *user_realm,
						const char *iplocalport,
						const char *ipremoteport,
						const sasl_callback_t *callbacks, 
						unsigned flags,
						sasl_conn_t **pconn);
	int (*sasl_listmechPtr)(sasl_conn_t *conn,
			      const char *user,
			      const char *prefix,
			      const char *sep,
			      const char *suffix,
			      const char **result,
			      unsigned *plen,
			      int *pcount);
	int (*SASLServerStartPtr)(sasl_conn_t *conn,
				  const char *mech,
				  const char *clientin,
				  unsigned clientinlen,
				  const char **serverout,
				  unsigned *serveroutlen);
	int (*SASLServerStepPtr)(sasl_conn_t *conn,
		     const char *clientin,
		     unsigned clientinlen,
		     const char **serverout,
		     unsigned *serveroutlen);
	int (*sasl_checkpassPtr)(sasl_conn_t *conn,
			       const char *user,
			       unsigned userlen,
			       const char *pass,
			       unsigned passlen);
	int (*sasl_user_existsPtr)(sasl_conn_t *conn,
			 const char *service,
		     const char *user_realm,
		     const char *user);
	int (*sasl_setpassPtr)(sasl_conn_t *conn,
		 const char *user,
		 const char *pass,
		 unsigned passlen,
		 const char *oldpass, unsigned oldpasslen,
		 unsigned flags);
	int (*sasl_checkapopPtr)(sasl_conn_t *conn,
			       const char *challenge, unsigned challen,
			       const char *response, unsigned resplen);
	int (*sasl_auxprop_requestPtr)(sasl_conn_t *conn,
				     const char **propnames);
	struct propctx *(*sasl_auxprop_getctxPtr)(sasl_conn_t *conn);
	void (*sasl_erasebufferPtr)(char *pass, unsigned len);
	struct propctx *(*prop_newPtr)(unsigned estimate);
	int (*prop_dupPtr)(struct propctx *src_ctx, struct propctx **dst_ctx);
	const struct propval *(*prop_getPtr)(struct propctx *ctx);
	int (*prop_getnamesPtr)(struct propctx *ctx, const char **names,
		  struct propval *vals);
	void (*prop_clearPtr)(struct propctx *ctx, int requests);
	void (*prop_erasePtr)(struct propctx *ctx, const char *name);
	void (*prop_disposePtr)(struct propctx **ctx);
	int (*prop_formatPtr)(struct propctx *ctx, const char *sep, int seplen,
		char *outbuf, unsigned outmax, unsigned *outlen);
	int (*prop_setPtr)(struct propctx *ctx, const char *name,
	     const char *value, int vallen);
	int (*prop_setvalsPtr)(struct propctx *ctx, const char *name,
		 const char **values);
	int (*_sasl_add_stringPtr)(char **out, int *alloclen, int *outlen, const char *add);
	int (*_buf_allocPtr)(char **rwbuf, unsigned *curlen, unsigned newlen);
	void (*_sasl_get_errorbufPtr)(sasl_conn_t *conn, char ***bufhdl, unsigned **lenhdl);

} GlobalsRec;

typedef struct
{
	sasl_malloc_t *custMalloc;
	sasl_calloc_t *custCalloc;
	sasl_realloc_t *custRealloc;
	sasl_free_t *custFree;

	sasl_mutex_alloc_t *custMutexNew;
	sasl_mutex_lock_t *custMutexLock;
	sasl_mutex_unlock_t *custMutexUnlock;
	sasl_mutex_free_t *custMutexDispose;
} GlobalParamsRec;

typedef struct
{
	sasl_conn_t			*ctx;
	sasl_callback_t		*cbk;
} cfm_sasl_conn_t;

GlobalsRec	saslcfmglob;						//	The globals
GlobalParamsRec saslcfmglobp;

// From Apple sample code... (CarbonLib SDK, CFM->MachO->CFM)
//
//	This function allocates a block of CFM glue code which contains the instructions to call CFM routines
//
void	*MachOFunctionPointerForCFMFunctionPointer( void *cfmfp )
{
	UInt32 template[6] = {0x3D800000, 0x618C0000, 0x800C0000, 0x804C0004, 0x7C0903A6, 0x4E800420};
    UInt32	*mfp = (UInt32*) NewPtr( sizeof(template) );		//	Must later dispose of allocated memory
    mfp[0] = template[0] | ((UInt32)cfmfp >> 16);
    mfp[1] = template[1] | ((UInt32)cfmfp & 0xFFFF);
    mfp[2] = template[2];
    mfp[3] = template[3];
    mfp[4] = template[4];
    mfp[5] = template[5];
    MakeDataExecutable( mfp, sizeof(template) );
    return( mfp );
}

sasl_callback_t *GetCFMCallbacks(const sasl_callback_t *callbacks)
{
	int cbksize = 0;
	const sasl_callback_t *cbk = callbacks;
	sasl_callback_t *ncbk, *new_callbacks;
	while (cbk->id) {
		cbksize++; cbk++;
	}
	cbksize++; cbk = callbacks;

	ncbk = new_callbacks = (sasl_callback_t *)NewPtr(cbksize * sizeof(sasl_callback_t));
	if (!ncbk) return nil;
	while (cbksize--) {
		ncbk->id = cbk->id;
		ncbk->context = cbk->context;
		if (cbk->proc)
			ncbk->proc = MachOFunctionPointerForCFMFunctionPointer(cbk->proc);
		else ncbk->proc = nil;
		ncbk++; cbk++;
	}
	return new_callbacks;
}

void DisposeCFMCallbacks(sasl_callback_t *callbacks)
{
	sasl_callback_t *cbk = callbacks;
	if (!cbk) return;
	while (cbk->id) {
		if (cbk->proc) DisposePtr((Ptr)cbk->proc);
		cbk++;
	}
	DisposePtr((Ptr)callbacks);
}

int _cfmsasl_common_init(const sasl_callback_t *callbacks, int isServer, const char *appname);

int _cfmsasl_common_init(const sasl_callback_t *callbacks, int isServer, const char *appname)
{
	int (*SASLClientInitPtr)(const sasl_callback_t *callbacks);
	int (*SASLServerInitPtr)(const sasl_callback_t *callbacks, const char *appname);
	int result = SASL_NOMEM;

  if (!_cfmsasl_initted) {
// The skeleton for this code originally came from the CFM->MachO->CFM sample
// provided by Aple with the Carbon SDK. It shows how to use CFBundle to call MachO
// libraries from CFM and how to encapsulate callbacks into CFM.
	memset( &saslcfmglob, 0, sizeof(GlobalsRec) );						//	Initialize the globals

	//	Make a CFURLRef from the CFString representation of the bundle's path.
	//	See the Core Foundation URL Services chapter for details.
	saslcfmglob.bundleURL	= CFURLCreateWithFileSystemPath( 
				nil, 										 //	workaround for Radar # 2452789
				CFSTR("/Library/Frameworks/SASL2.framework"), //	hard coded path for sample
				0,
				true );
	if ( saslcfmglob.bundleURL != NULL )

	// Make a bundle instance using the URLRef.
	saslcfmglob.myBundle	= CFBundleCreate(
				NULL /* kCFAllocatorDefault */, 			//	workaround for Radar # 2452789
				saslcfmglob.bundleURL );

	if ( saslcfmglob.myBundle && CFBundleLoadExecutable( saslcfmglob.myBundle )) {	//	Try to load the executable from my bundle.

		// Now that the code is loaded, search for the functions we want by name.
		saslcfmglob.SASLClientNewPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_client_new") );

		saslcfmglob.SASLClientStartPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_client_start") );

		saslcfmglob.SASLClientStepPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_client_step") );

		saslcfmglob.SASLErrStringPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_errstring") );

		saslcfmglob.sasl_errdetailPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_errdetail") );

		saslcfmglob.SASLGetPropPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_getprop") );

		saslcfmglob.SASLSetPropPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_setprop") );

		saslcfmglob.SASLIdlePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_idle") );

		saslcfmglob.SASLEncodePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_encode") );

		saslcfmglob.SASLEncodeVPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_encodev") );

		saslcfmglob.SASLDecodePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_decode") );

		saslcfmglob.SASLDisposePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_dispose") );

		saslcfmglob.SASLDonePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_done") );

		saslcfmglob.SASLSetAllocPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_set_alloc") );

		saslcfmglob.sasl_encode64Ptr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_encode64") );

		saslcfmglob.sasl_decode64Ptr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_decode64") );
		
		saslcfmglob.sasl_mkchalPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_mkchal") );

		saslcfmglob.sasl_utf8verifyPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_utf8verify") );

		saslcfmglob.sasl_churnPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_churn") );

		saslcfmglob.sasl_randPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_rand") );

		saslcfmglob.sasl_randseedPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_randseed") );

		saslcfmglob.sasl_randcreatePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_randcreate") );

		saslcfmglob.sasl_randfreePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_randfree") );

		saslcfmglob.SASLSetMutexPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_set_mutex") );

		saslcfmglob.SASLServerNewPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_server_new") );

		saslcfmglob.SASLServerStartPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_server_start") );

		saslcfmglob.SASLServerStepPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_server_step") );

		saslcfmglob.sasl_listmechPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_listmech") );

		saslcfmglob.sasl_checkpassPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_checkpass") );

		saslcfmglob.sasl_setpassPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_setpass") );

		saslcfmglob.sasl_user_existsPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_user_exists") );

		saslcfmglob.sasl_checkapopPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_checkapop") );

		saslcfmglob.sasl_auxprop_requestPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_auxprop_request") );

		saslcfmglob.sasl_auxprop_getctxPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_auxprop_getctx") );

		saslcfmglob.sasl_erasebufferPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_erasebuffer") );

		saslcfmglob.prop_newPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_new") );

		saslcfmglob.prop_dupPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_dup") );

		saslcfmglob.prop_getPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_get") );

		saslcfmglob.prop_getnamesPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_getnames") );

		saslcfmglob.prop_clearPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_clear") );

		saslcfmglob.prop_erasePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_erase") );

		saslcfmglob.prop_disposePtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_dispose") );

		saslcfmglob.prop_formatPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_format") );

		saslcfmglob.prop_setPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_set") );

		saslcfmglob.prop_setvalsPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("prop_setvals") );

/* These are internal functions used by our sasl_seterror */
		saslcfmglob._sasl_add_stringPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("_sasl_add_string") );

		saslcfmglob._buf_allocPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("_buf_alloc") );

		saslcfmglob._sasl_get_errorbufPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("_sasl_get_errorbuf") );

		if (!_cfmsasl_haveCustomAlloc) {
			saslcfmglobp.custMalloc = MachOFunctionPointerForCFMFunctionPointer(malloc);
			saslcfmglobp.custCalloc = MachOFunctionPointerForCFMFunctionPointer(calloc);
			saslcfmglobp.custRealloc = MachOFunctionPointerForCFMFunctionPointer(realloc);
			saslcfmglobp.custFree = MachOFunctionPointerForCFMFunctionPointer(free);
			
			_cfmsasl_haveCustomAlloc = 1;
		}

		saslcfmglob.SASLSetAllocPtr(saslcfmglobp.custMalloc, saslcfmglobp.custCalloc,
									saslcfmglobp.custRealloc, saslcfmglobp.custFree);

		if (_cfmsasl_haveCustomMutex) 
			saslcfmglob.SASLSetMutexPtr(saslcfmglobp.custMutexNew, saslcfmglobp.custMutexLock,
										saslcfmglobp.custMutexUnlock, saslcfmglobp.custMutexDispose);

	} else if (saslcfmglob.myBundle) {
		CFRelease(saslcfmglob.myBundle);
		saslcfmglob.myBundle = nil;
	}

	if (saslcfmglob.bundleURL && !saslcfmglob.myBundle) {
		CFRelease(saslcfmglob.bundleURL);
		saslcfmglob.bundleURL = nil;
	}

	if (saslcfmglob.myBundle)
		_cfmsasl_initted = 1;
  }

	if (_cfmsasl_initted) {
		if (isServer) {
			SASLServerInitPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_server_init") );
			if (SASLServerInitPtr != nil) {
	    		sasl_callback_t *new_callbacks = NULL;
				if (callbacks)
					new_callbacks = GetCFMCallbacks(callbacks);
				result = SASLServerInitPtr(new_callbacks, appname);
				saslcfmglob.serverCallbacks = new_callbacks;
			}
		} else {
			SASLClientInitPtr = (void*)CFBundleGetFunctionPointerForName(
						saslcfmglob.myBundle, CFSTR("sasl_client_init") );
			if (SASLClientInitPtr != nil) {
	    		sasl_callback_t *new_callbacks = NULL;
				if (callbacks)
					new_callbacks = GetCFMCallbacks(callbacks);
				result = SASLClientInitPtr(new_callbacks);
				saslcfmglob.clientCallbacks = new_callbacks;
			}
		}
	}
	return result;
}

int sasl_server_init(const sasl_callback_t *callbacks,
				 const char *appname)
{
	return _cfmsasl_common_init(callbacks, true, appname);
}

int sasl_client_init(const sasl_callback_t *callbacks)
{
	return _cfmsasl_common_init(callbacks, false, nil);
}

void sasl_done(void)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.SASLDonePtr) return;

	saslcfmglob.SASLDonePtr();
	DisposeCFMCallbacks(saslcfmglob.clientCallbacks);
	DisposeCFMCallbacks(saslcfmglob.serverCallbacks);
	CFBundleUnloadExecutable(saslcfmglob.myBundle);
	CFRelease(saslcfmglob.myBundle);
	CFRelease(saslcfmglob.bundleURL);
	saslcfmglob.myBundle = NULL;
	saslcfmglob.bundleURL = NULL;

        _cfmsasl_initted = 0;
}

int sasl_client_new (const char *service,
						const char *serverFQDN,
						const char *iplocalport,
						const char *ipremoteport,
						const sasl_callback_t *prompt_supp, 
						unsigned flags,
						sasl_conn_t **pconn)
{
	sasl_callback_t *new_ps = NULL;
	int result;
	cfm_sasl_conn_t *myconn;

	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLClientNewPtr) return SASL_NOMEM;

	if (prompt_supp)
		new_ps = GetCFMCallbacks(prompt_supp);

// this is commented out because sasl.h incorrectly described the api
//	if (*pconn)
//		DisposeCFMCallbacks(((cfm_sasl_conn_t *)*pconn)->cbk);
//	else {
	myconn = (cfm_sasl_conn_t *) NewPtr(sizeof(cfm_sasl_conn_t));
	if (myconn == NULL) {
		return SASL_NOMEM;
	}

	myconn->ctx = NULL;
//	}

	result = saslcfmglob.SASLClientNewPtr(service, serverFQDN, iplocalport,
				ipremoteport, new_ps, flags,
				&(myconn->ctx));
	myconn->cbk = new_ps;

	*pconn = (sasl_conn_t *)myconn;

	return result;
}

int sasl_server_new (const char *service,
						const char *serverFQDN,
						const char *user_realm,
						const char *iplocalport,
						const char *ipremoteport,
						const sasl_callback_t *callbacks, 
						unsigned flags,
						sasl_conn_t **pconn)
{
	sasl_callback_t *new_ps = NULL;
	int result;

	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLServerNewPtr) return SASL_NOMEM;

	if (callbacks)
		new_ps = GetCFMCallbacks(callbacks);
	*pconn = (sasl_conn_t *)NewPtr(sizeof(cfm_sasl_conn_t));
	((cfm_sasl_conn_t *)*pconn)->ctx = nil;

	result = saslcfmglob.SASLServerNewPtr(service, serverFQDN, user_realm,
				iplocalport, ipremoteport, new_ps, flags,
				&((cfm_sasl_conn_t *)*pconn)->ctx);
	((cfm_sasl_conn_t *)*pconn)->cbk = new_ps;

	return result;
}

void sasl_dispose(sasl_conn_t **pconn)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.SASLDisposePtr) return;

	if (!pconn) return;
	if (!*pconn) return;

	saslcfmglob.SASLDisposePtr(&((cfm_sasl_conn_t *)*pconn)->ctx);
	DisposeCFMCallbacks(((cfm_sasl_conn_t *)*pconn)->cbk);
	DisposePtr((Ptr)*pconn);
	*pconn = NULL;
}

int sasl_client_start (sasl_conn_t *conn,
						const char *mechlist,
						sasl_interact_t **prompt_need,
						const char **clientout,
						unsigned *clientoutlen,
						const char **mech)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLClientStartPtr) return SASL_NOMEM;

	return saslcfmglob.SASLClientStartPtr(((cfm_sasl_conn_t *)conn)->ctx, mechlist,
				prompt_need, clientout, clientoutlen, mech);
}

int sasl_client_step (sasl_conn_t *conn,
						const char *serverin,
						unsigned serverinlen,
						sasl_interact_t **prompt_need,
						const char **clientout,
						unsigned *clientoutlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLClientStepPtr) return SASL_NOMEM;

	return saslcfmglob.SASLClientStepPtr(((cfm_sasl_conn_t *)conn)->ctx, serverin,
							serverinlen, prompt_need, clientout, clientoutlen);
}

const char *sasl_errstring(int saslerr,
							const char *langlist,
							const char **outlang)
{
	if (!_cfmsasl_initted)
		return NULL;
	if (!saslcfmglob.SASLErrStringPtr) return NULL;

	return saslcfmglob.SASLErrStringPtr(saslerr, langlist, outlang);
}

const char *sasl_errdetail(sasl_conn_t *conn)
{
	if (!_cfmsasl_initted)
		return NULL;
	if (!saslcfmglob.sasl_errdetailPtr) return NULL;

	return saslcfmglob.sasl_errdetailPtr(((cfm_sasl_conn_t *)conn)->ctx);
}

int sasl_getprop(sasl_conn_t *conn, int propnum, const void **pvalue)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLGetPropPtr) return SASL_NOMEM;

	return saslcfmglob.SASLGetPropPtr(((cfm_sasl_conn_t *)conn)->ctx, propnum, pvalue);
}

int sasl_setprop(sasl_conn_t *conn,
					int propnum,
					const void *value)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLSetPropPtr) return SASL_NOMEM;

	return saslcfmglob.SASLSetPropPtr(((cfm_sasl_conn_t *)conn)->ctx, propnum, value);
}

int sasl_idle(sasl_conn_t *conn)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLIdlePtr) return SASL_NOMEM;

	return saslcfmglob.SASLIdlePtr(((cfm_sasl_conn_t *)conn)->ctx);
}

int sasl_encode(sasl_conn_t *conn,
				const char *input, unsigned inputlen,
				const char **output, unsigned *outputlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLEncodePtr) return SASL_NOMEM;

	return saslcfmglob.SASLEncodePtr(((cfm_sasl_conn_t *)conn)->ctx, input, inputlen,
											output, outputlen);
}

int sasl_encodev(sasl_conn_t *conn,
			     const struct iovec *invec, unsigned numiov,
			     const char **output, unsigned *outputlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLEncodeVPtr) return SASL_NOMEM;

	return saslcfmglob.SASLEncodeVPtr(((cfm_sasl_conn_t *)conn)->ctx, invec, numiov,
											output, outputlen);
}

int sasl_decode(sasl_conn_t *conn,
				const char *input, unsigned inputlen,
				const char **output, unsigned *outputlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLDecodePtr) return SASL_NOMEM;

	return saslcfmglob.SASLDecodePtr(((cfm_sasl_conn_t *)conn)->ctx, input, inputlen,
											output, outputlen);
}

int sasl_decode64(const char *in, unsigned inlen,
			      char *out, unsigned outmax, unsigned *outlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_decode64Ptr) return SASL_NOMEM;

	return saslcfmglob.sasl_decode64Ptr(in, inlen, out, outmax, outlen);
}

int sasl_encode64(const char *in, unsigned inlen,
			      char *out, unsigned outmax, unsigned *outlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_encode64Ptr) return SASL_NOMEM;

	return saslcfmglob.sasl_encode64Ptr(in, inlen, out, outmax, outlen);
}

void sasl_set_alloc(sasl_malloc_t *ma, sasl_calloc_t *ca, sasl_realloc_t *rea, sasl_free_t *fr)
{
	if (_cfmsasl_haveCustomAlloc) {
		DisposePtr((Ptr)saslcfmglobp.custMalloc);
		DisposePtr((Ptr)saslcfmglobp.custCalloc);
		DisposePtr((Ptr)saslcfmglobp.custRealloc);
		DisposePtr((Ptr)saslcfmglobp.custFree);
	}
	saslcfmglobp.custMalloc = MachOFunctionPointerForCFMFunctionPointer(ma);
	saslcfmglobp.custCalloc = MachOFunctionPointerForCFMFunctionPointer(ca);
	saslcfmglobp.custRealloc = MachOFunctionPointerForCFMFunctionPointer(rea);
	saslcfmglobp.custFree = MachOFunctionPointerForCFMFunctionPointer(fr);
	_cfmsasl_haveCustomAlloc = 1;
}

int sasl_mkchal(sasl_conn_t *conn, char *buf,
			    unsigned maxlen, unsigned hostflag)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_mkchalPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_mkchalPtr(((cfm_sasl_conn_t *)conn)->ctx, buf, maxlen, hostflag);
}

int sasl_utf8verify(const char *str, unsigned len)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_utf8verifyPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_utf8verifyPtr(str, len);
}

void sasl_churn(sasl_rand_t *rpool, 
			    const char *data,
			    unsigned len)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.sasl_churnPtr) return;

	saslcfmglob.sasl_churnPtr(rpool, data, len);
}

void sasl_rand(sasl_rand_t *rpool,
			   char *buf,
			   unsigned len)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.sasl_randPtr) return;

	saslcfmglob.sasl_randPtr(rpool, buf, len);
}

void sasl_randseed(sasl_rand_t *rpool,
			       const char *seed,
			       unsigned len)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.sasl_randseedPtr) return;

	saslcfmglob.sasl_randseedPtr(rpool, seed, len);
}

void sasl_randfree(sasl_rand_t **rpool)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.sasl_randfreePtr) return;

	saslcfmglob.sasl_randfreePtr(rpool);
}

int sasl_randcreate(sasl_rand_t **rpool)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_randcreatePtr) return SASL_NOMEM;

	return saslcfmglob.sasl_randcreatePtr(rpool);
}

void sasl_set_mutex(sasl_mutex_alloc_t *mn, sasl_mutex_lock_t *ml,
                                sasl_mutex_unlock_t *mu, sasl_mutex_free_t *md)
{
	if (_cfmsasl_haveCustomMutex) {
		DisposePtr((Ptr)saslcfmglobp.custMutexNew);
		DisposePtr((Ptr)saslcfmglobp.custMutexLock);
		DisposePtr((Ptr)saslcfmglobp.custMutexUnlock);
		DisposePtr((Ptr)saslcfmglobp.custMutexDispose);
	}
	saslcfmglobp.custMutexNew = MachOFunctionPointerForCFMFunctionPointer(mn);
	saslcfmglobp.custMutexLock = MachOFunctionPointerForCFMFunctionPointer(ml);
	saslcfmglobp.custMutexUnlock = MachOFunctionPointerForCFMFunctionPointer(mu);
	saslcfmglobp.custMutexDispose = MachOFunctionPointerForCFMFunctionPointer(md);
	_cfmsasl_haveCustomMutex = 1;
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
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_listmechPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_listmechPtr(((cfm_sasl_conn_t *)conn)->ctx, user, prefix, sep,
											suffix, result, plen, pcount);
}

int sasl_server_start(sasl_conn_t *conn,
				  const char *mech,
				  const char *clientin,
				  unsigned clientinlen,
				  const char **serverout,
				  unsigned *serveroutlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLServerStartPtr) return SASL_NOMEM;

	return saslcfmglob.SASLServerStartPtr(((cfm_sasl_conn_t *)conn)->ctx, mech, clientin,
											clientinlen, serverout, serveroutlen);
}

int sasl_server_step(sasl_conn_t *conn,
		     const char *clientin,
		     unsigned clientinlen,
		     const char **serverout,
		     unsigned *serveroutlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.SASLServerStepPtr) return SASL_NOMEM;

	return saslcfmglob.SASLServerStepPtr(((cfm_sasl_conn_t *)conn)->ctx, clientin, clientinlen,
											serverout, serveroutlen);
}

int sasl_checkpass(sasl_conn_t *conn,
			       const char *user,
			       unsigned userlen,
			       const char *pass,
			       unsigned passlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_checkpassPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_checkpassPtr(((cfm_sasl_conn_t *)conn)->ctx, user, userlen, pass,
											passlen);
}

int sasl_user_exists(sasl_conn_t *conn,
			 const char *service,
		     const char *user_realm,
		     const char *user)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_user_existsPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_user_existsPtr(((cfm_sasl_conn_t *)conn)->ctx,
												service, user_realm, user);
}

int sasl_setpass(sasl_conn_t *conn,
		 const char *user,
		 const char *pass,
		 unsigned passlen,
		 const char *oldpass, unsigned oldpasslen,
		 unsigned flags)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_setpassPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_setpassPtr(((cfm_sasl_conn_t *)conn)->ctx, user, pass,
											passlen, oldpass, oldpasslen, flags);
}

int sasl_checkapop(sasl_conn_t *conn,
			       const char *challenge, unsigned challen,
			       const char *response, unsigned resplen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_checkapopPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_checkapopPtr(((cfm_sasl_conn_t *)conn)->ctx, challenge, challen,
												response, resplen);
}

int sasl_auxprop_request(sasl_conn_t *conn,
				     const char **propnames)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.sasl_auxprop_requestPtr) return SASL_NOMEM;

	return saslcfmglob.sasl_auxprop_requestPtr(((cfm_sasl_conn_t *)conn)->ctx, propnames);
}

struct propctx *sasl_auxprop_getctx(sasl_conn_t *conn)
{
	if (!_cfmsasl_initted)
		return NULL;
	if (!saslcfmglob.sasl_auxprop_getctxPtr) return NULL;

	return saslcfmglob.sasl_auxprop_getctxPtr(((cfm_sasl_conn_t *)conn)->ctx);
}

void sasl_erasebuffer(char *pass, unsigned len)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.sasl_erasebufferPtr) return;

	saslcfmglob.sasl_erasebufferPtr(pass, len);
}

struct propctx *prop_new(unsigned estimate)
{
	if (!_cfmsasl_initted)
		return NULL;
	if (!saslcfmglob.prop_newPtr) return NULL;

	return saslcfmglob.prop_newPtr(estimate);
}

int prop_dup(struct propctx *src_ctx, struct propctx **dst_ctx)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.prop_dupPtr) return SASL_NOMEM;

	return saslcfmglob.prop_dupPtr(src_ctx, dst_ctx);
}

const struct propval *prop_get(struct propctx *ctx)
{
	if (!_cfmsasl_initted)
		return NULL;
	if (!saslcfmglob.prop_getPtr) return NULL;

	return saslcfmglob.prop_getPtr(ctx);
}

int prop_getnames(struct propctx *ctx, const char **names,
		  struct propval *vals)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.prop_getnamesPtr) return SASL_NOMEM;

	return saslcfmglob.prop_getnamesPtr(ctx, names, vals);
}

void prop_clear(struct propctx *ctx, int requests)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.prop_clearPtr) return;

	saslcfmglob.prop_clearPtr(ctx, requests);
}

void prop_erase(struct propctx *ctx, const char *name)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.prop_erasePtr) return;

	saslcfmglob.prop_erasePtr(ctx, name);
}

void prop_dispose(struct propctx **ctx)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob.prop_disposePtr) return;

	saslcfmglob.prop_disposePtr(ctx);
}

int prop_format(struct propctx *ctx, const char *sep, int seplen,
		char *outbuf, unsigned outmax, unsigned *outlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.prop_formatPtr) return SASL_NOMEM;

	return saslcfmglob.prop_formatPtr(ctx, sep, seplen, outbuf, outmax, outlen);
}

int prop_set(struct propctx *ctx, const char *name,
	     const char *value, int vallen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.prop_setPtr) return SASL_NOMEM;

	return saslcfmglob.prop_setPtr(ctx, name, value, vallen);
}

int prop_setvals(struct propctx *ctx, const char *name,
		 const char **values)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob.prop_setvalsPtr) return SASL_NOMEM;

	return saslcfmglob.prop_setvalsPtr(ctx, name, values);
}

/* internal functions used by sasl_seterror follow */
int _sasl_add_string(char **out, int *alloclen, int *outlen, const char *add)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob._sasl_add_stringPtr) return SASL_NOMEM;

	return saslcfmglob._sasl_add_stringPtr(out, alloclen, outlen, add);
}

int _buf_alloc(char **rwbuf, unsigned *curlen, unsigned newlen)
{
	if (!_cfmsasl_initted)
		return SASL_NOMEM;
	if (!saslcfmglob._buf_allocPtr) return SASL_NOMEM;

	return saslcfmglob._buf_allocPtr(rwbuf, curlen, newlen);
}

void _sasl_get_errorbuf(sasl_conn_t *conn, char ***bufhdl, unsigned **lenhdl)
{
	if (!_cfmsasl_initted)
		return;
	if (!saslcfmglob._sasl_add_stringPtr) return;

	saslcfmglob._sasl_get_errorbufPtr(((cfm_sasl_conn_t *)conn)->ctx, bufhdl, lenhdl);
}
