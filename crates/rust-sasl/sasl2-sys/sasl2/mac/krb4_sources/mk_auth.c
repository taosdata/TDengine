/*
 * Copyright (c) 1995, 1996, 1997, 1998 Kungliga Tekniska Hšgskolan
 * (Royal Institute of Technology, Stockholm, Sweden).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the Institute nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "krb_locl.h"

RCSID("$Id: mk_auth.c,v 1.2 2001/12/04 02:06:08 rjs3 Exp $");

/*
 * Generate an authenticator for service.instance@realm.
 * instance is canonicalized by `krb_get_phost'
 * realm is set to the local realm if realm == NULL
 * The ticket acquired by `krb_mk_req' is returned in `ticket' and the
 * authenticator in `buf'.  
 * Options control the behaviour (see krb_sendauth).
 */

int
krb_mk_auth(int32_t options,
	    KTEXT ticket,
	    char *service,
	    char *instance,
	    char *realm,
	    u_int32_t checksum,
	    char *version,
	    KTEXT buf)
{
  char realinst[INST_SZ];
  char realrealm[REALM_SZ];
  int ret;
  char *tmp;

  if (options & KOPT_DONT_CANON)
    tmp = instance;
  else
    tmp = krb_get_phost (instance);

  strlcpy(realinst, tmp, sizeof(realinst));

  if (realm == NULL) {
    ret = krb_get_lrealm (realrealm, 1);
    if (ret != KSUCCESS)
      return ret;
    realm = realrealm;
  }
  
  if(!(options & KOPT_DONT_MK_REQ)) {
    ret = krb_mk_req (ticket, service, realinst, realm, checksum);
    if (ret != KSUCCESS)
      return ret;
  }
    
  {
      int tmp;
      size_t rem = sizeof(buf->dat);
      unsigned char *p = buf->dat;

      p = buf->dat;

      if (rem < 2 * KRB_SENDAUTH_VLEN)
	  return KFAILURE;
      memcpy (p, KRB_SENDAUTH_VERS, KRB_SENDAUTH_VLEN);
      p += KRB_SENDAUTH_VLEN;
      rem -= KRB_SENDAUTH_VLEN;

      memcpy (p, version, KRB_SENDAUTH_VLEN);
      p += KRB_SENDAUTH_VLEN;
      rem -= KRB_SENDAUTH_VLEN;

      tmp = krb_put_int(ticket->length, p, rem, 4);
      if (tmp < 0)
	  return KFAILURE;
      p += tmp;
      rem -= tmp;

      if (rem < ticket->length)
	  return KFAILURE;
      memcpy(p, ticket->dat, ticket->length);
      p += ticket->length;
      rem -= ticket->length;
      buf->length = p - buf->dat;
  }
  return KSUCCESS;
}
