/*
	KClient.h -- Application interface for KClient

	© Copyright 1994,95 by Project Mandarin Inc.
	
	Initial coding 			8/94 Peter Bosanko.
	Added new routines		8/95 PCB
	Moved some constants
	from krbdriver.h
	
========================================================================
	DES and Kerberos portions of this file are...
========================================================================
	
	Copyright (C) 1989 by the Massachusetts Institute of Technology

	Export of this software from the United States of America is assumed
	to require a specific license from the United States Government.
	It is the responsibility of any person or organization contemplating
	export to obtain such a license before exporting.

WITHIN THAT CONSTRAINT, permission to use, copy, modify, and
distribute this software and its documentation for any purpose and
without fee is hereby granted, provided that the above copyright
notice appear in all copies and that both that copyright notice and
this permission notice appear in supporting documentation, and that
the name of M.I.T. not be used in advertising or publicity pertaining
to distribution of the software without specific, written prior
permission.  M.I.T. makes no representations about the suitability of
this software for any purpose.  It is provided "as is" without express
or implied warranty.

*/

#ifndef	_KCLIENT_
#define	_KCLIENT_

#ifndef _TYPES_
#include <Types.h>
#endif

/* Error codes */

enum {
	cKrbCorruptedFile = -1024,	/* couldn't find a needed resource */
	cKrbNoKillIO,				/* can't killIO because all calls sync */
	cKrbBadSelector,			/* csCode passed doesn't select a recognized function */
	cKrbCantClose,				/* we must always remain open */
	cKrbMapDoesntExist,			/* tried to access a map that doesn't exist (index too large,
									or criteria doesn't match anything) */
	cKrbSessDoesntExist,		/* tried to access a session that doesn't exist */
	cKrbCredsDontExist,			/* tried to access credentials that don't exist */
	cKrbTCPunavailable,			/* couldn't open MacTCP driver */
	cKrbUserCancelled,			/* user cancelled a log in operation */
	cKrbConfigurationErr,		/* Kerberos Preference file is not configured properly */
	cKrbServerRejected,			/* A server rejected our ticket */
	cKrbServerImposter,			/* Server appears to be a phoney */
	cKrbServerRespIncomplete,	/* Server response is not complete */
	cKrbNotLoggedIn,			/* Returned by cKrbGetUserName if user is not logged in */
	cKrbOldDriver,				/* old version of the driver */
	cKrbDriverInUse,			/* driver is not reentrant */
	cKrbAppInBkgnd,				/* driver won't put up password dialog when in background */
	cKrbInvalidSession,			/* invalid structure passed to KClient/KServer routine */
	cKrbOptionNotDefined,		/* returned from GetOption */
	
	cKrbKerberosErrBlock = -20000	/* start of block of 256 kerberos error numbers */
};

#define LARGEST_DRIVER_ERROR	cKrbOptionNotDefined

typedef char KClientErrString[64];

enum { KClientLoggedIn, KClientNotLoggedIn };

/* Different kerberos name formats (for KServerGetUserName) */
enum { 
	KClientLocalName,				/* Don't specify realm */
	KClientCommonName, 				/* Only specify realm if it isn't local */
	KClientFullName					/* Always specify realm */
};

/* Options */
enum {
	kclientOptionSaveName = 1,
	kclientOptionSynchTime,
	kclientOptionShowMenu,
	kclientOptionInstalled_1_6
};

struct KClientKey {
	unsigned char keyBytes[8];
};
typedef struct KClientKey KClientKey;

struct KClientSessionInfo {
	char sessionBytes[256];
};
typedef struct KClientSessionInfo KClientSessionInfo;
typedef KClientSessionInfo *KClientSessionPtr;

/* Defines for obsolete function names */
#define KClientInitSession		KClientNewSession
#define KClientVerifySendAuth	KClientVerifyReplyTicket

/************************************/
/* Some includes from des.h & krb.h */
/************************************/
#if defined(powerc) || defined(__powerc)
#pragma options align=mac68k
#endif

#ifndef DES_DEFS

typedef unsigned char des_cblock[8];	/* crypto-block size */

/* Key schedule */
typedef struct des_ks_struct { des_cblock _; } des_key_schedule[16];

#endif /* DES_DEFS */

#ifndef KRB_DEFS

#define C_Block des_cblock
#define Key_schedule des_key_schedule

/* The maximum sizes for aname, realm, sname, and instance +1 */
#define 	ANAME_SZ	40
#define		REALM_SZ	40
#define		SNAME_SZ	40
#define		INST_SZ		40

/* Definition of text structure used to pass text around */
#define		MAX_KTXT_LEN	1250

struct ktext {
    long     length;		/* Length of the text */
    unsigned char dat[MAX_KTXT_LEN];	/* The data itself */
    unsigned long mbz;		/* zero to catch runaway strings */
};

typedef struct ktext *KTEXT;
typedef struct ktext KTEXT_ST;

struct credentials {
    char    service[ANAME_SZ];	/* Service name */
    char    instance[INST_SZ];	/* Instance */
    char    realm[REALM_SZ];	/* Auth domain */
    C_Block session;		/* Session key */
    long     lifetime;		/* Lifetime */
    long     kvno;		/* Key version number */
    KTEXT_ST ticket_st;		/* The ticket itself */
    long    issue_date;		/* The issue time */
    char    pname[ANAME_SZ];	/* Principal's name */
    char    pinst[INST_SZ];	/* Principal's instance */
};

typedef struct credentials CREDENTIALS;

/* Structure definition for rd_private_msg and rd_safe_msg */

struct msg_dat {
    unsigned char *app_data;	/* pointer to appl data */
    unsigned long app_length;	/* length of appl data */
    unsigned long hash;		/* hash to lookup replay */
    long     swap;		/* swap bytes? */
    long    time_sec;		/* msg timestamp seconds */
    unsigned char time_5ms;	/* msg timestamp 5ms units */
};

typedef struct msg_dat MSG_DAT;

typedef unsigned long u_long;
typedef unsigned short u_short;

#define KRB_PASSWORD_SERVICE  "changepw.kerberos"

#endif	/* KRB_DEFS */

#if defined(powerc) || defined(__powerc)
#pragma options align=reset
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * call into des ecb_encrypt
 */
/* created by n3liw+@cmu.edu to support SASL, need to be able to specify checksum */
int KClient_des_ecb_encrypt(KClientSessionInfo  *session,des_cblock v1,des_cblock v2,int do_encrypt);

/*
 * call into des pcbc_encrypt
 */
/* created by n3liw+@cmu.edu to support SASL, need to be able to specify checksum */
int KClient_des_pcbc_encrypt(KClientSessionInfo  *session,des_cblock v1,des_cblock v2,long len,int do_encrypt);

OSErr KClientNewSession(KClientSessionInfo *session, unsigned long lAddr,unsigned short lPort,unsigned long fAddr,unsigned short fPort);

OSErr KClientDisposeSession(KClientSessionInfo  *session);

/* created by n3liw+@cmu.edu to support SASL, need to be able to specify checksum */
OSErr KClientGetTicketForServiceFull(KClientSessionInfo *session, char *service,void *buf,unsigned long *buflen,long cks);

OSErr KClientGetTicketForService(KClientSessionInfo *session, char *service,void *buf,unsigned long *buflen);

OSErr KClientLogin( KClientSessionInfo *session, KClientKey *privateKey );

OSErr KClientSetPrompt(  KClientSessionInfo *session, char *prompt );

OSErr KClientPasswordLogin( KClientSessionInfo *session, char *password, KClientKey *privateKey );

OSErr KClientPasswordToKey( char *password, KClientKey *privateKey );

OSErr KClientKeyLogin( KClientSessionInfo *session, KClientKey *privateKey );

OSErr KClientLogout( void );
		
short KClientStatus( void );

OSErr KClientVersion( short *majorVersion, short *minorVersion, char *versionString );

OSErr KClientGetUserName(char *user);

OSErr KClientGetSessionUserName(KClientSessionInfo *session, char *user, short nameType);

OSErr KClientSetUserName(char *user);

OSErr KClientCacheInitialTicket(KClientSessionInfo *session, char *service);

OSErr KClientGetSessionKey(KClientSessionInfo *session, KClientKey *sessionKey);

OSErr KClientMakeSendAuth(KClientSessionInfo *session, char *service,void *buf,unsigned long *buflen,long checksum, char *applicationVersion);
				
OSErr KClientVerifyReplyTicket(KClientSessionInfo *session, void *buf,unsigned long *buflen );
		
OSErr KClientEncrypt(KClientSessionInfo *session, void *buf,unsigned long buflen,void *encryptBuf,unsigned long *encryptLength);
		
OSErr KClientDecrypt(KClientSessionInfo *session, void *buf,unsigned long buflen,unsigned long *decryptOffset,unsigned long *decryptLength);

void KClientErrorText(OSErr err, char *text);


/* KServer calls */

OSErr KServerNewSession( KClientSessionInfo *session, char *service, 
						unsigned long lAddr,unsigned short lPort,unsigned long fAddr,unsigned short fPort);

OSErr KServerVerifyTicket( KClientSessionInfo *session, void *buf, char *keyFileName );

OSErr KServerGetReplyTicket( KClientSessionInfo *session, void *buf, unsigned long *buflen );

OSErr KServerGetKey( KClientSessionInfo *session, KClientKey *privateKey, char *service, long version, char *filename );

OSErr KServerAddKey( KClientSessionInfo *session, KClientKey *privateKey, char *service, long version, char *filename );

OSErr KServerGetSessionTimeRemaining( KClientSessionInfo *session, long *seconds );

/* Configuration routines */

OSErr KClientGetLocalRealm( char *realm );

OSErr KClientSetLocalRealm( char *realm );

OSErr KClientGetRealm( char *host, char *realm );

OSErr KClientAddRealmMap( char *host, char *realm );

OSErr KClientDeleteRealmMap( char *host );

OSErr KClientGetNthRealmMap( long n, char *host, char *realm );

OSErr KClientGetNthServer( long n, char *host, char *realm, Boolean admin );

OSErr KClientAddServerMap( char *host, char *realm, Boolean admin );

OSErr KClientDeleteServerMap( char *host, char *realm );

OSErr KClientGetNthServerMap( long n, char *host, char *realm, Boolean *admin );

OSErr KClientGetNthServerPort( long n, short *port );

OSErr KClientSetNthServerPort( long n, short port );

OSErr KClientGetNumSessions( long *n );

OSErr KClientGetNthSession( long n, char *name, char *instance, char *realm );

OSErr KClientDeleteSession( char *name, char *instance, char *realm );

OSErr KClientGetCredentials( char *name, char *instance, char *realm, CREDENTIALS *cred );

OSErr KClientAddCredentials( char *name, char *instance, char *realm, CREDENTIALS *cred );

OSErr KClientDeleteCredentials( char *name, char *instance, char *realm, 
								char *sname, char *sinstance, char *srealm );


OSErr KClientGetNumCredentials( long *n, char *name, char *instance, char *realm );

OSErr KClientGetNthCredential( long n, char *name, char *instance, char *realm,
								char *sname, char *sinstance, char *srealm );

OSErr KClientAddSpecial( char *service, char *name );

OSErr KClientDeleteSpecial( char *service );

OSErr KClientGetNumSpecials( long *n );

OSErr KClientGetNthSpecial( long n, char *name, char *service );

OSErr KClientSetOption( short option, void *value );

OSErr KClientGetOption( short option, void *value );

#ifdef __cplusplus
}
#endif

#endif