/*
	KrbDriver.h -- This is the KClient driver's direct call interface.
	This file defines all of the csCodes used by the driver and the three
	structures used for passing information to and from the driver.
		
	© Copyright 1992,95 by Cornell University
	
	Initial coding 						1/92 Peter Bosanko
	Moved some constants to kclient.h	8/95 PCB
*/

#ifndef _KrbDriver_	
#define	_KrbDriver_

#ifndef _KCLIENT_
#include "KClient.h"
#endif

/* csCodes for Control Calls */
enum {
	cKrbKillIO = 1,
	cKrbGetLocalRealm,
	cKrbSetLocalRealm,
	cKrbGetRealm,
	cKrbAddRealmMap,
	cKrbDeleteRealmMap,
	cKrbGetNthRealmMap,
	cKrbGetNthServer,
	cKrbAddServerMap,
	cKrbDeleteServerMap,
	cKrbGetNthServerMap,
	cKrbGetNumSessions,
	cKrbGetNthSession,
	cKrbDeleteSession,
	cKrbGetCredentials,
	cKrbAddCredentials,
	cKrbDeleteCredentials,
	cKrbGetNumCredentials,
	cKrbGetNthCredentials,
	cKrbDeleteAllSessions,
	cKrbGetTicketForService,
	cKrbGetAuthForService,
	cKrbCheckServiceResponse,
	cKrbEncrypt,
	cKrbDecrypt,
	cKrbCacheInitialTicket,
	cKrbGetUserName,
	cKrbSetUserName,
	cKrbSetPassword,
	cKrbGetDesPointers,
	cKrbGetErrorText,
	cKrbLogin,
	cKrbSetKey,
	cKrbKerberos,
	cKrbGetNthServerPort,
	cKrbSetNthServerPort,
	cKrbDriverVersion,
	cKrbPasswordToKey,
	cKrbNewClientSession,
	cKrbNewServerSession,
	cKrbDisposeSession,
	cKrbServerVerifyTicket,
	cKrbServerGetReplyTkt,
	cKrbGetServiceKey,
	cKrbAddServiceKey,
	cKrbGetOption,
	cKrbSetOption,
	cKrbAdditionalLogin,
	cKrbControlPanelEnter,
	cKrbControlPanelLeave,
	cKrbGetSessionTimeRemaining,
	cKrbGetSessionUserName,
	cKrbGetNumSpecials,
	cKrbGetNthSpecial,
	cKrbAddSpecial,
	cKrbDeleteSpecial
};

/* Need to switch to short word alignment on power pc */

#if defined(powerc) || defined(__powerc)
#pragma options align=mac68k
#endif

/* Parameter block for high level calls */

struct krbHiParmBlock	{
			char 			*service;		/* full name -- combined service, instance, realm */
			char  			*buf;
			unsigned long  	buflen;
			long 			checksum;
			unsigned long	lAddr;
			unsigned short	lPort;
			unsigned long	fAddr;
			unsigned short	fPort;
			unsigned long	decryptOffset;
			unsigned long	decryptLength;
			char 			*encryptBuf;
			unsigned long	encryptLength;
			char	 		*applicationVersion;	/* Version string must be 8 bytes long!	 */
			char	 		sessionKey[8];			/* for internal use                      */
			char			schedule[128];			/* for internal use                      */
			char 			*user;
};
typedef struct krbHiParmBlock krbHiParmBlock;
typedef krbHiParmBlock *KrbParmPtr;
typedef KrbParmPtr *KrbParmHandle;

/* New KClient record */

#define		NEW_KCLIENT_TAG 0xF7FAF7FA

struct KClientRec	{
			long			tag;
			krbHiParmBlock	hiParm;
			long			libVersion;
			void			*serverContext;
			char			*filename;
			long			keyVersion;
			char			serverKey[8];
			char			*prompt;
			short			nameType;
};
typedef struct KClientRec KClientRec;

/* ********************************************************* */
/* The rest of these defs are for low level calls            */
/* ********************************************************* */

/* Parameter block for low level calls */		
struct krbParmBlock	{
			char	*uName;
			char	*uInstance;
			char	*uRealm;			/* also where local realm or mapping realm passed */
			char	*sName;
			char	*sInstance;
			char	*sRealm;
			char	*host;				/* also netorhost */
			long	admin;				/* isadmin, mustadmin */
			long	*itemNumber;
			long	*adminReturn;		/* when it needs to be passed back */
			CREDENTIALS *cred;
			short	port;
};
typedef struct krbParmBlock krbParmBlock;

#if defined(powerc) || defined(__powerc)
#pragma options align=reset
#endif

#endif