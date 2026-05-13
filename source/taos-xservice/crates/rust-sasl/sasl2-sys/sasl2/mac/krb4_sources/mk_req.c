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
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *      This product includes software developed by the Kungliga Tekniska
 *      Hšgskolan and its contributors.
 * 
 * 4. Neither the name of the Institute nor the names of its contributors
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

#include "sasl_mac_krb_locl.h"

RCSID("$Id: mk_req.c,v 1.2 2001/12/04 02:06:08 rjs3 Exp $");

static int lifetime = 255;	/* But no longer than TGT says. */


static int
build_request(KTEXT req, char *name, char *inst, char *realm, 
	      u_int32_t checksum)
{
    struct timeval tv;
    unsigned char *p = req->dat;
    int tmp;
    size_t rem = sizeof(req->dat);

    tmp = krb_put_nir(name, inst, realm, p, rem);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    tmp = krb_put_int(checksum, p, rem, 4);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    /* Fill in the times on the request id */
    krb_kdctimeofday(&tv);

    if (rem < 1)
	return KFAILURE;

    *p++ = tv.tv_usec / 5000; /* 5ms */
    --rem;
    
    tmp = krb_put_int(tv.tv_sec, p, rem, 4);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    /* Fill to a multiple of 8 bytes for DES */
    req->length = ((p - req->dat + 7)/8) * 8;
    return 0;
}


/*
 * krb_mk_req takes a text structure in which an authenticator is to
 * be built, the name of a service, an instance, a realm,
 * and a checksum.  It then retrieves a ticket for
 * the desired service and creates an authenticator in the text
 * structure passed as the first argument.  krb_mk_req returns
 * KSUCCESS on success and a Kerberos error code on failure.
 *
 * The peer procedure on the other end is krb_rd_req.  When making
 * any changes to this routine it is important to make corresponding
 * changes to krb_rd_req.
 *
 * The authenticator consists of the following:
 *
 * authent->dat
 *
 * unsigned char	KRB_PROT_VERSION	protocol version no.
 * unsigned char	AUTH_MSG_APPL_REQUEST	message type
 * (least significant
 * bit of above)	HOST_BYTE_ORDER		local byte ordering
 * unsigned char	kvno from ticket	server's key version
 * string		realm			server's realm
 * unsigned char	tl			ticket length
 * unsigned char	idl			request id length
 * text			ticket->dat		ticket for server
 * text			req_id->dat		request id
 *
 * The ticket information is retrieved from the ticket cache or
 * fetched from Kerberos.  The request id (called the "authenticator"
 * in the papers on Kerberos) contains the following:
 *
 * req_id->dat
 *
 * string		cr.pname		{name, instance, and
 * string		cr.pinst		realm of principal
 * string		myrealm			making this request}
 * 4 bytes		checksum		checksum argument given
 * unsigned char	tv_local.tf_usec	time (milliseconds)
 * 4 bytes		tv_local.tv_sec		time (seconds)
 *
 * req_id->length = 3 strings + 3 terminating nulls + 5 bytes for time,
 *                  all rounded up to multiple of 8.
 */

int
krb_mk_req(KTEXT authent, char *service, char *instance, char *realm, 
	   int32_t checksum)
{
    KTEXT_ST req_st;
    KTEXT req_id = &req_st;

    CREDENTIALS cr;             /* Credentials used by retr */
    KTEXT ticket = &(cr.ticket_st); /* Pointer to tkt_st */
    int retval;                 /* Returned by krb_get_cred */

    char myrealm[REALM_SZ];

    unsigned char *p = authent->dat;
    int rem = sizeof(authent->dat);
    int tmp;

    tmp = krb_put_int(KRB_PROT_VERSION, p, rem, 1);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    tmp = krb_put_int(AUTH_MSG_APPL_REQUEST, p, rem, 1);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    /* Get the ticket and move it into the authenticator */
    if (krb_ap_req_debug)
        krb_warning("Realm: %s\n", realm);

    retval = krb_get_cred(service,instance,realm,&cr);

    if (retval == RET_NOTKT) {
	retval = get_ad_tkt(service, instance, realm, lifetime);
	if (retval == KSUCCESS)
	    retval = krb_get_cred(service, instance, realm, &cr);
    }

    if (retval != KSUCCESS)
	return retval;


    /*
     * With multi realm ticket files either find a matching TGT or
     * else use the first TGT for inter-realm authentication.
     *
     * In myrealm hold the realm of the principal "owning" the
     * corresponding ticket-granting-ticket.
     */

    retval = krb_get_cred(KRB_TICKET_GRANTING_TICKET, realm, realm, 0);
    if (retval == KSUCCESS) {
      strcpy_truncate(myrealm, realm, REALM_SZ);
    } else
      retval = krb_get_tf_realm(TKT_FILE, myrealm);
    
    if (retval != KSUCCESS)
	return retval;
    
    if (krb_ap_req_debug)
        krb_warning("serv=%s.%s@%s princ=%s.%s@%s\n", service, instance, realm,
		    cr.pname, cr.pinst, myrealm);

    tmp = krb_put_int(cr.kvno, p, rem, 1);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    tmp = krb_put_string(realm, p, rem);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    tmp = krb_put_int(ticket->length, p, rem, 1);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    retval = build_request(req_id, cr.pname, cr.pinst, myrealm, checksum);
    if (retval != KSUCCESS)
	return retval;
    
    encrypt_ktext(req_id, &cr.session, DES_ENCRYPT);

    tmp = krb_put_int(req_id->length, p, rem, 1);
    if (tmp < 0)
	return KFAILURE;
    p += tmp;
    rem -= tmp;

    if (rem < ticket->length + req_id->length)
	return KFAILURE;

    memcpy(p, ticket->dat, ticket->length);
    p += ticket->length;
    rem -= ticket->length;
    memcpy(p, req_id->dat, req_id->length);
    p += req_id->length;
    rem -= req_id->length;

    authent->length = p - authent->dat;
    
    memset(&cr, 0, sizeof(cr));
    memset(&req_st, 0, sizeof(req_st));

    if (krb_ap_req_debug)
        krb_warning("Authent->length = %d\n", authent->length);

    return KSUCCESS;
}

/* 
 * krb_set_lifetime sets the default lifetime for additional tickets
 * obtained via krb_mk_req().
 * 
 * It returns the previous value of the default lifetime.
 */

int
krb_set_lifetime(int newval)
{
    int olife = lifetime;

    lifetime = newval;
    return(olife);
}
