/* $Id: kcglue_krb.c,v 1.3 2003/02/13 19:55:57 rjs3 Exp $
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
#include <stdlib.h>
#include <string.h>
#include <kcglue_krb.h>
//#include "macKClientPublic.h"
#include "KClient.h"

#ifndef FALSE
#define FALSE 0
#endif
#ifndef TRUE
#define TRUE 1
#endif

#define SOME_KRB_ERR_NUMBER (70)
#define		MAX_KRB_ERRORS	256

const char *krb_err_txt[MAX_KRB_ERRORS]={
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err",
 "krb err","krb err","krb err","krb err","krb err","krb err","krb err","krb err"
};


/*
 * given a service instance and realm, combine them to foo.bar@REALM
 * return true upon success
 */
static int implode_krb_user_info(char *dst,const char *service,const char *instance,const char *realm)
{
  	if(strlen(service)>=KCGLUE_ITEM_SIZE)
  		return FALSE;
  	if(strlen(instance)>=KCGLUE_ITEM_SIZE)
  		return FALSE;
  	if(strlen(realm)>=KCGLUE_ITEM_SIZE)
  		return FALSE;
  	strcpy(dst,service);
  	dst+=strlen(dst);
  	if(instance[0]!=0) {
  		*dst++='.';
  		strcpy(dst,instance);
  		dst+=strlen(dst);
  	}
  	*dst++='@';
  	strcpy(dst,realm);
  	return TRUE;
}

int kcglue_krb_mk_req(void *dat,int *len, const char *service, char *instance, char *realm, 
	   long checksum,
	   void *des_key,
	   char *pname,
	   char *pinst)
{
	char tkt_buf[KCGLUE_MAX_KTXT_LEN+20];
	char user_id[KCGLUE_MAX_K_STR_LEN+1];
	char dummy1[KCGLUE_MAX_K_STR_LEN+1], dummy2[KCGLUE_MAX_K_STR_LEN+1];
  	KClientSession ses;
  	KClientPrincipal prin, srvp;
  	int have_session=FALSE;
  	int rc;

	if(!implode_krb_user_info(user_id,service,instance,realm))
		return SOME_KRB_ERR_NUMBER;

  	rc=KClientNewClientSession(&ses/*,0,0,0,0*/ );
  	if(rc!=0)
    	return SOME_KRB_ERR_NUMBER;
  	have_session=TRUE;
  	
    *len=sizeof(tkt_buf)-10;
  	//rc=KClientGetTicketForServiceFull(&ses,user_id,tkt_buf,len,checksum);
  	rc=KClientV4StringToPrincipal(user_id, &srvp);
  	if (rc==0)
  		rc=KClientSetServerPrincipal(ses,srvp);
	if (rc==0)
	  	rc=KClientGetTicketForService(ses,checksum,tkt_buf,len);
  	if(rc==0) {
		memcpy(dat,tkt_buf/*+4*/,*len);	/*kclient puts out a 4 byte length that mit doesnt*/
		rc=KClientGetSessionKey(ses,des_key);
	}
	if(rc==0) {
//		rc=KClientGetUserName(pname);
		rc=KClientGetClientPrincipal(ses, &prin);
		if (rc==0) {
			rc=KClientPrincipalToV4Triplet(prin, pname, dummy1, dummy2);
			KClientDisposePrincipal(prin);
		}
	}
	*pinst=0;
	if(have_session)
    	KClientDisposeSession(ses);
  
	if(rc!=0)
		return SOME_KRB_ERR_NUMBER;
	return 0;
}
