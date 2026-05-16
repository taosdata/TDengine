/*
	KClient.c -- Application library for KClient

	© Copyright 1994,1995 by Cornell University
	
	Initial coding 8/94 by Peter Bosanko.
*/

#ifndef _KrbDriver_
#include "krbdriver.h"
#endif

#ifndef _DEVICES_
#include	<Devices.h>
#endif

#include "kcglue_des.h"

#define KC_SESSION ((KClientRec *)session)
#define KC_PB (&(((KClientRec *)session)->hiParm))
#define OLD_KC_PB ((krbHiParmBlock *)session)
#define PICK_PARM (kcRec ? (void*) kcRec : (void*) pb)
#define KCLIENTDRIVER "\p.Kerberos"

/* Forward Declarations */

OSErr KClientSendMessage(short msg, void *parm);
OSErr KClientSetPassword(  KClientSessionInfo *session, char *password  );
krbHiParmBlock *KClientSessionKind(  KClientSessionInfo *session, KClientRec **kcRec );
OSErr _KClientVersion( StringPtr driver, short *majorVersion, short *minorVersion, char *versionString );

/*
 * call into des ecb_encrypt
 */
/* created by n3liw+@cmu.edu to support SASL, need to be able to specify checksum */
int KClient_des_ecb_encrypt(KClientSessionInfo  *session,des_cblock v1,des_cblock v2,int do_encrypt)
{
	KClientKey sessionKey;
	Key_schedule schedule;
	
	int rc=KClientGetSessionKey(session,&sessionKey);
	if(rc!=0)
		return rc;
	rc=kcglue_des_key_sched(&sessionKey,schedule);
	if(rc!=0)
		return rc;
	kcglue_des_ecb_encrypt(v1,v2,schedule,do_encrypt);
	return rc;
}

/*
 * call into des pcbc_encrypt
 */
/* created by n3liw+@cmu.edu to support SASL, need to be able to specify checksum */
int KClient_des_pcbc_encrypt(KClientSessionInfo  *session,des_cblock v1,des_cblock v2,long len,int do_encrypt)
{
	KClientKey sessionKey;
	Key_schedule schedule;
	
	int rc=KClientGetSessionKey(session,&sessionKey);
	if(rc!=0)
		return rc;
	rc=kcglue_des_key_sched(&sessionKey,schedule);
	if(rc!=0)
		return rc;
	kcglue_des_pcbc_encrypt(v1,v2,len,schedule,&sessionKey,do_encrypt);
	return rc;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSendMessage(short msg, void *parm)
{
	ParamBlockRec aPBR;
	short refNum = 0;
	
/************************************************** 
	OK to "open" driver everytime because driver
	just returns if it's already open.
	This saves us from having to pass around refNum
	or store it in a global.                     
***************************************************/
	
	OSErr err = OpenDriver(KCLIENTDRIVER,&refNum);
	if (err) return err;

	aPBR.cntrlParam.ioCompletion = nil;
	aPBR.cntrlParam.ioVRefNum = 0;
	aPBR.cntrlParam.ioCRefNum = refNum;
	aPBR.cntrlParam.csCode = msg;
	BlockMove(&parm,aPBR.cntrlParam.csParam,sizeof(parm));
		
	(void) PBControlImmed( &aPBR );

	err = aPBR.cntrlParam.ioResult;
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
krbHiParmBlock *KClientSessionKind(  KClientSessionInfo *session, KClientRec **kcRec )
{
	if (KC_SESSION->tag==NEW_KCLIENT_TAG) {
		/* Newer driver, use newer session record */
		if (kcRec)
			*kcRec 		= KC_SESSION;
		return KC_PB;
	}
	else {
		if (kcRec)
			*kcRec 		= NULL;
		return OLD_KC_PB;
	}
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSetPassword(  KClientSessionInfo *session, char *password  )
{
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
	
	pb->user = password;
	return KClientSendMessage(cKrbSetPassword,PICK_PARM);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientNewSession(KClientSessionInfo *session, unsigned long lAddr,unsigned short lPort,unsigned long fAddr,unsigned short fPort)
{
	OSErr err;

	err = KClientSendMessage(cKrbNewClientSession,KC_SESSION);
	
	if (err==cKrbBadSelector) {
		/* old driver, so initialize by hand */
		short i,e = sizeof(KClientSessionInfo) / sizeof(long);
		long *s = (long *) session;
		for (i=0;i<e;i++) *s++ = 0;
		err = noErr;
	}

	KC_SESSION->libVersion		= 2;
	KC_PB->lAddr 				= lAddr;
	KC_PB->lPort 				= lPort;
	KC_PB->fAddr 				= fAddr;
	KC_PB->fPort 				= fPort;
		
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDisposeSession(KClientSessionInfo *session)
{
	KClientRec *kcRec;
	(void) KClientSessionKind(session,&kcRec);
	
	if (kcRec)
		return KClientSendMessage(cKrbDisposeSession,session);

	return noErr;
}

/*---------------------------------------------------------------------------------------------------*/
/*
 * modified by n3liw+@cmu.edu to support SASL, need to be able to specify checksum
 */
OSErr KClientGetTicketForService(KClientSessionInfo *session, char *service,void *buf,unsigned long *buflen)
{
	return KClientGetTicketForServiceFull(session,service,buf,buflen,0);
}

#include <stdio.h>
#include <string.h>

/*
 * store the passed long in network byte order
 */
static char *put_long(char *dst,long aval)
{
	*dst++=aval>>24;
	*dst++=aval>>16;
	*dst++=aval>>8;
	*dst++=aval;
	return dst;
}

/*
 * - int = 1 byte
 * - long = 4 bytes
 * long length of all the following [kclientism]
 * ticket format, from reading mk_req.c
 * int KRB_PROT_VERSION      
 * int AUTH_MSG_APPL_REQUEST
 * int key version numbner
 * string realm
 * int ticket length
 * int authenticator length
 * ticket
 * authenticator [
 *   string name
 *   string instance
 *   string realm
 *   long checksum
 *   byte GMT microseconds/5
 *   int GMT time
 * ] encrypted in session key
 */

/*---------------------------------------------------------------------------------------------------*/
/*
 * created by n3liw+@cmu.edu to support SASL, need to be able to specify checksum
 */
OSErr KClientGetTicketForServiceFull(KClientSessionInfo *session, char *service,void *buf,unsigned long *buflen,long cks)
{
	char *p=(char *)buf;
	long tkt_len;
	long auth_len;
	char wbuf[1500];

	OSErr err;	
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	pb->service = service;
	pb->buf = (char *) buf;
	pb->buflen = *buflen;
	pb->checksum = cks;
	err = KClientSendMessage(cKrbGetTicketForService,PICK_PARM);
	*buflen = pb->buflen;
	if(err!=0)
		return err;
	/*
	 * if checksum is zero, buth kclientman and kclient will correctly get the ticket
	 * if checksum is non zero, then kclientman will have incorrectly encoded 0 in the checksum
	 * field of the authenticator, kclient will have encoded the correct checksum...
	 * rather than check the underlying authentication package (kclient vs kclientman)
	 * we will go ahead and decrypt the authenticator and fix the checksum.  this is unessary but
	 * harmless for kclient.
     */
	if(cks==0)
		return 0;
	p+=4+3+strlen(p+7)+1; /*4 byte kclient len, vers,req, kvno*/
	tkt_len= (*p++)&0x0ff;
	auth_len= (*p++)&0x0ff;
	p+=tkt_len;
	err=KClient_des_pcbc_encrypt(session,(unsigned char *)p,(unsigned char *)wbuf,auth_len,0);
	if(err!=0)
		return err;
	{
		char *w=wbuf;
		/* printf("name='%s'\n",w); */
		w+=strlen(w)+1;	/*skip name */
		/* printf("instance='%s'\n",w); */
		w+=strlen(w)+1;	/*skip instance */
		/* printf("realm='%s'\n",w); */
		w+=strlen(w)+1; /*realm*/
		w=put_long(w,cks);
	}
	err=KClient_des_pcbc_encrypt(session,(unsigned char *)wbuf,(unsigned char *)wbuf,auth_len,1);
	memcpy(p,wbuf,auth_len);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientLogin(  KClientSessionInfo *session, KClientKey *privateKey )
{	
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
	
	pb->service = (char *) privateKey; /* pointer to private key in first 4 bytes */
	err = KClientSendMessage(cKrbLogin,PICK_PARM);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSetPrompt(  KClientSessionInfo *session, char *prompt )
{	
	KClientRec *kcRec;
	(void) KClientSessionKind(session,&kcRec);

	if (kcRec)
		kcRec->prompt = prompt;
	else return cKrbBadSelector;
	
	return noErr;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientPasswordLogin(  KClientSessionInfo *session, char *password, KClientKey *privateKey )
{
	OSErr err;
	
	if ( ( err = KClientSetPassword(session,password) ) != noErr )
		 return err;
	
	return KClientLogin(session,privateKey);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientPasswordToKey( char *password, KClientKey *privateKey )
{
	ParamBlockRec aPBR;
	short refNum;
	OSErr err;
	
	if ( (err = OpenDriver(KCLIENTDRIVER,&refNum)) != noErr)
		return err;

	aPBR.cntrlParam.ioCompletion = nil;
	aPBR.cntrlParam.ioVRefNum = 0;
	aPBR.cntrlParam.ioCRefNum = refNum;
	aPBR.cntrlParam.csCode = cKrbPasswordToKey;
	((long *)aPBR.cntrlParam.csParam)[0] = (long)password;
	((long *)aPBR.cntrlParam.csParam)[1] = (long)privateKey;
		
	(void) PBControl( &aPBR, false );
	return aPBR.cntrlParam.ioResult;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientKeyLogin( KClientSessionInfo *session, KClientKey *privateKey )
{
	OSErr err;

	err = KClientSendMessage(cKrbSetKey,privateKey);
	if (err) return err;
	
	return KClientLogin(session,privateKey);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientLogout( )
{
	krbHiParmBlock	cpb;

	return KClientSendMessage(cKrbDeleteAllSessions, &cpb);
}
		
/*---------------------------------------------------------------------------------------------------*/
short KClientStatus( )
{
	char user[40];
	
	user[0] = '\0';
	(void) KClientGetUserName(user);
	if (*user != 0)
		return KClientLoggedIn;
	return KClientNotLoggedIn;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr _KClientVersion( StringPtr driver, short *majorVersion, short *minorVersion, char *versionString )
{
	ParamBlockRec aPBR;
	short refNum;
	OSErr err;
	
	if ( (err = OpenDriver(driver,&refNum)) != noErr)
		return err;

	aPBR.cntrlParam.ioCompletion = nil;
	aPBR.cntrlParam.ioVRefNum = 0;
	aPBR.cntrlParam.ioCRefNum = refNum;
	aPBR.cntrlParam.csCode = cKrbDriverVersion;
	((long *)aPBR.cntrlParam.csParam)[1] = (long)versionString;
		
	(void) PBControl( &aPBR, false );
	err = aPBR.cntrlParam.ioResult;
	
	/* For pre-2.0, do some detective work */
	if (err==cKrbBadSelector) {
		*majorVersion = 1;
		aPBR.cntrlParam.csCode = cKrbGetDesPointers;
		((long *)aPBR.cntrlParam.csParam)[1] = 11; /* so it doesn't return anything */
		(void) PBControl( &aPBR, false );
		if (aPBR.cntrlParam.ioResult==cKrbOldDriver) {
			*minorVersion = 1;
			if (versionString)
				*((long *)versionString) = '1.1\0';
		}
		else {
			*minorVersion = 0;
			if (versionString)
				*((long *)versionString) = '1.0\0';
		}
		err = 0;
	}
	else {
		*majorVersion = aPBR.cntrlParam.csParam[0];
		*minorVersion = aPBR.cntrlParam.csParam[1];
	}
	
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientVersion( short *majorVersion, short *minorVersion, char *versionString )
{
	return _KClientVersion(KCLIENTDRIVER,majorVersion,minorVersion,versionString);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetUserName(char *user)
{
	OSErr err;
	KClientSessionInfo s,*session = &s;
	
	KC_SESSION->tag = 0;
	OLD_KC_PB->user = user;
	err = KClientSendMessage(cKrbGetUserName,session);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetSessionUserName(KClientSessionInfo *session, char *user, short nameType )
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	pb->user = user;
	kcRec->nameType = nameType;
	err = KClientSendMessage(cKrbGetSessionUserName,PICK_PARM);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSetUserName(char *user)
{
	OSErr err;
	KClientSessionInfo s,*session = &s;
	
	KC_SESSION->tag = 0;
	OLD_KC_PB->user = user;
	err = KClientSendMessage(cKrbSetUserName,session);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientCacheInitialTicket(KClientSessionInfo *session, char *service)
{
	OSErr err;	
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
		
	pb->service = service;
	err = KClientSendMessage(cKrbCacheInitialTicket,PICK_PARM);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetSessionKey(KClientSessionInfo *session, KClientKey *sessionKey)
{	
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	/* Not logged in and no server context */
	if ((!kcRec || !kcRec->serverContext) && KClientStatus()==KClientNotLoggedIn)
		return cKrbNotLoggedIn;
	
	BlockMove(&(pb->sessionKey),sessionKey,sizeof(KClientKey));
	return 0;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientMakeSendAuth(KClientSessionInfo *session, char *service,void *buf,unsigned long *buflen,long checksum, char *applicationVersion)
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
		
	pb->service = service;
	pb->buf = (char *) buf;
	pb->buflen = *buflen;
	pb->checksum = checksum;
	pb->applicationVersion = applicationVersion;
	err = KClientSendMessage(cKrbGetAuthForService,PICK_PARM);
	*buflen = pb->buflen;
	return err;
}				
/*---------------------------------------------------------------------------------------------------*/
OSErr KClientVerifyReplyTicket(KClientSessionInfo *session, void *buf,unsigned long *buflen )
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
	
	pb->buf = (char *) buf;
	pb->buflen = *buflen;
	err = KClientSendMessage(cKrbCheckServiceResponse,PICK_PARM);
	*buflen = pb->buflen;
	return err;
}
		
/*---------------------------------------------------------------------------------------------------*/
OSErr KClientEncrypt(KClientSessionInfo *session, void *buf,unsigned long buflen,void *encryptBuf,unsigned long *encryptLength)
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	pb->buf = (char *) buf;
	pb->buflen = buflen;
	pb->encryptBuf = (char *) encryptBuf;
	err = KClientSendMessage(cKrbEncrypt,PICK_PARM);
	*encryptLength = pb->encryptLength;
	return err;
}
		
/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDecrypt(KClientSessionInfo *session, void *buf,unsigned long buflen,
					unsigned long *decryptOffset,unsigned long *decryptLength)
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	pb->buf = (char *) buf;
	pb->buflen = buflen;
	err = KClientSendMessage(cKrbDecrypt,PICK_PARM);
	*decryptOffset = pb->decryptOffset;
	*decryptLength = pb->decryptLength;
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
void KClientErrorText(OSErr err, char *text)
{
	ParamBlockRec aPBR;
	short refNum;
	OSErr oerr;
	
	if ( (oerr = OpenDriver(KCLIENTDRIVER,&refNum)) != noErr)
		return;

	aPBR.cntrlParam.ioCompletion = nil;
	aPBR.cntrlParam.ioVRefNum = 0;
	aPBR.cntrlParam.ioCRefNum = refNum;
	aPBR.cntrlParam.csCode = cKrbGetErrorText;
	((long *)aPBR.cntrlParam.csParam)[0] = (long)err;
	((long *)aPBR.cntrlParam.csParam)[1] = (long)text;
		
	(void) PBControl( &aPBR, false );
	
	/* In case driver is old, at least return something */
	if (aPBR.cntrlParam.ioResult==cKrbBadSelector) {
		BlockMove("Kerberos error",text,15);		
	}
}

/*---------------------------------------------------------------------------------------------------*/
/* Kerberized Server routines                                                                        */
/*---------------------------------------------------------------------------------------------------*/
OSErr KServerNewSession( KClientSessionInfo *session, char *service,unsigned long lAddr, 
						unsigned short lPort,unsigned long fAddr,unsigned short fPort)
{
	OSErr err;

	KC_PB->service 				= service;

	err = KClientSendMessage(cKrbNewServerSession,KC_SESSION);
	
	if (err)
		return err;

	KC_SESSION->libVersion		= 2;
	KC_PB->lAddr 				= lAddr;
	KC_PB->lPort 				= lPort;
	KC_PB->fAddr 				= fAddr;
	KC_PB->fPort 				= fPort;
		
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KServerVerifyTicket( KClientSessionInfo *session, void *buf, char *filename )
{
	OSErr err;
	KC_PB->buf = (char *) buf;
	KC_SESSION->filename = filename;

	err = KClientSendMessage(cKrbServerVerifyTicket,KC_SESSION);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KServerGetReplyTicket( KClientSessionInfo *session, void *buf, unsigned long *buflen )
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
	
	pb->buf 		= (char *) buf;
	pb->buflen 	= *buflen;

	err = KClientSendMessage(cKrbServerGetReplyTkt,PICK_PARM);
	if (err) return err;
	
	*buflen = pb->buflen;
	return noErr;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KServerAddKey( KClientSessionInfo *session, KClientKey *privateKey, char *service, long version, char *filename )
{
	OSErr err;
	KClientKey key;
	char srv[128];
	char tkt[1250];
	unsigned long len;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	if (!kcRec)
		return cKrbBadSelector;	/* old driver */
	
	KC_SESSION->filename 	= filename;
	KC_PB->service 			= service;

	if (!service) {
		/* No service, build from scratch, prompt the user */
 
		/* Get the user to log in, using service principle and password */
		KClientLogout();
		err = KClientLogin( session, &key );
		if (err) return err;
		
		err = KClientGetUserName(srv);
		if (err) return err;

		/* Get a service ticket for the service so that we can obtain key version number */
		err = KClientGetTicketForService(session, srv,tkt,&len);
		if (err) return err;
				
		KC_PB->service = srv;
		BlockMove(&key,KC_SESSION->serverKey,8);
		KC_SESSION->keyVersion 	= tkt[6];		/* tkt contains private key's version in the seventh byte */
	}
	else {
		KC_SESSION->keyVersion 	= version;
		BlockMove(privateKey,KC_SESSION->serverKey,8);
		KC_PB->service 		= service;
	}
	
	return KClientSendMessage(cKrbAddServiceKey,session);

}

/*---------------------------------------------------------------------------------------------------*/
OSErr KServerGetKey( KClientSessionInfo *session, KClientKey *privateKey,char *service, long version, char *filename )
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);
		
	if (!kcRec)
		return cKrbBadSelector;	/* old driver */

	KC_SESSION->keyVersion 	= version;
	KC_SESSION->filename 	= filename;
	KC_PB->service 			= service;

	err = KClientSendMessage(cKrbGetServiceKey,KC_SESSION);
	if (err) return err;

	BlockMove(KC_SESSION->serverKey,privateKey,8);
	return noErr;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KServerGetSessionTimeRemaining( KClientSessionInfo *session, long *seconds )
{
	OSErr err;
	KClientRec *kcRec;
	krbHiParmBlock *pb = KClientSessionKind(session,&kcRec);

	err = KClientSendMessage(cKrbGetSessionTimeRemaining,PICK_PARM);
	*seconds = pb->checksum;
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
/* Configuration routines                                                                            */
/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetLocalRealm( char *realm )
{
	krbParmBlock pb;
	pb.uRealm = realm;
	return KClientSendMessage(cKrbGetLocalRealm,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSetLocalRealm( char *realm )
{
	krbParmBlock pb;
	pb.uRealm = realm;
	return KClientSendMessage(cKrbSetLocalRealm,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetRealm( char *host, char *realm )
{
	krbParmBlock pb;
	pb.uRealm = realm;
	pb.host = host;
	return KClientSendMessage(cKrbGetRealm,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientAddRealmMap( char *host, char *realm )
{
	krbParmBlock pb;
	pb.uRealm = realm;
	pb.host = host;
	return KClientSendMessage(cKrbAddRealmMap,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDeleteRealmMap( char *host )
{
	krbParmBlock pb;
	pb.host = host;
	return KClientSendMessage(cKrbDeleteRealmMap,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthRealmMap( long n, char *host, char *realm )
{
	krbParmBlock pb;	
	pb.host = host;
	pb.uRealm = realm;
	pb.itemNumber = &n;
	return KClientSendMessage(cKrbGetNthRealmMap,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthServer( long n, char *host, char *realm, Boolean admin )
{
	krbParmBlock pb;
	
	pb.host = host;
	pb.uRealm = realm;
	pb.itemNumber = &n;
	pb.admin = (long) admin;
	return KClientSendMessage(cKrbGetNthServer,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientAddServerMap( char *host, char *realm, Boolean admin )
{
	krbParmBlock pb;
	pb.uRealm = realm;
	pb.host = host;
	pb.admin = admin ? 1 : 0;
	return KClientSendMessage(cKrbAddServerMap,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDeleteServerMap( char *host, char *realm )
{
	krbParmBlock pb;
	pb.uRealm = realm;
	pb.host = host;
	return KClientSendMessage(cKrbDeleteServerMap,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthServerMap( long n, char *host, char *realm, Boolean *admin )
{
	OSErr err;
	long ladmin;
	krbParmBlock pb;
	
	pb.uRealm = realm;
	pb.host = host;
	pb.adminReturn = &ladmin;
	pb.itemNumber = &n;
	err = KClientSendMessage(cKrbGetNthServerMap,&pb);
	*admin = (ladmin==1);
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthServerPort( long n, short *port )
{
	OSErr err;
	krbParmBlock pb;
	pb.itemNumber = &n;
	err = KClientSendMessage(cKrbGetNthServerPort,&pb);
	*port = pb.port;
	return err;
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSetNthServerPort( long n, short port )
{
	krbParmBlock pb;
	pb.itemNumber = &n;
	pb.port = port;
	return KClientSendMessage(cKrbSetNthServerPort,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNumSessions( long *n )
{
	krbParmBlock pb;
	pb.itemNumber = n;
	return KClientSendMessage(cKrbGetNumSessions,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthSession( long n, char *name, char *instance, char *realm )
{
	krbParmBlock pb;
	pb.itemNumber = &n;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	return KClientSendMessage(cKrbGetNthSession,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDeleteSession( char *name, char *instance, char *realm )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	return KClientSendMessage(cKrbDeleteSession,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetCredentials( char *name, char *instance, char *realm, CREDENTIALS *cred )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	pb.cred = cred;
	return KClientSendMessage(cKrbGetCredentials,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientAddCredentials( char *name, char *instance, char *realm, CREDENTIALS *cred )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	pb.cred = cred;
	return KClientSendMessage(cKrbAddCredentials,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDeleteCredentials( char *name, char *instance, char *realm, 
								char *sname, char *sinstance, char *srealm )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	pb.sName = sname;
	pb.sInstance = sinstance;
	pb.sRealm = srealm;
	return KClientSendMessage(cKrbDeleteCredentials,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNumCredentials( long *n, char *name, char *instance, char *realm )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	pb.itemNumber = n;
	return KClientSendMessage(cKrbGetNumCredentials,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthCredential( long n, char *name, char *instance, char *realm,
								char *sname, char *sinstance, char *srealm )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.uInstance = instance;
	pb.uRealm = realm;
	pb.sName = sname;
	pb.sInstance = sinstance;
	pb.sRealm = srealm;
	pb.itemNumber = &n;
	return KClientSendMessage(cKrbGetNthCredentials,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientAddSpecial( char *service, char *name )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.sName = service;
	return KClientSendMessage(cKrbAddSpecial,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientDeleteSpecial( char *service )
{
	krbParmBlock pb;
	pb.sName = service;
	return KClientSendMessage(cKrbDeleteSpecial,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNumSpecials( long *n )
{
	krbParmBlock pb;
	pb.itemNumber = n;
	return KClientSendMessage(cKrbGetNumSpecials,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetNthSpecial( long n, char *name, char *service )
{
	krbParmBlock pb;
	pb.uName = name;
	pb.sName = service;
	pb.itemNumber = &n;
	return KClientSendMessage(cKrbGetNthSpecial,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientSetOption( short option, void *value )
{
	krbParmBlock pb;
	pb.uName = (char *) value;
	pb.port = option;
	return KClientSendMessage(cKrbSetOption,&pb);
}

/*---------------------------------------------------------------------------------------------------*/
OSErr KClientGetOption( short option, void *value )
{
	krbParmBlock pb;
	pb.uName = (char *) value;
	pb.port = option;
	return KClientSendMessage(cKrbGetOption,&pb);
}

