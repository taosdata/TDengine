/*
 * $Source: /cvs/src/sasl/mac/kerberos_includes/prot.h,v $
 * $Author: rjs3 $
 * $Header: /cvs/src/sasl/mac/kerberos_includes/prot.h,v 1.2 2001/12/04 02:06:06 rjs3 Exp $
 *
 * Copyright 1985, 1986, 1987, 1988 by the Massachusetts Institute
 * of Technology.
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>.
 *
 * Include file with authentication protocol information.
 */
#ifndef	_KERBEROS_PROT_H
#define	_KERBEROS_PROT_H

#ifdef RUBBISH
#pragma ident	"@(#)prot.h	1.3	92/07/14 SMI"
#endif

//#include <kerberos/mit-copyright.h>
#ifdef RUBBISH
#include <kerberos/krb_conf.h>
#endif

#ifdef	__cplusplus
extern "C" {
#endif

#define		KRB_PORT		750	/* PC's don't have */
						/* /etc/services */
#define		KRB_PROT_VERSION 	4
#define		MAX_PKT_LEN		1000
#define		MAX_TXT_LEN		1000
#define		TICKET_GRANTING_TICKET	"krbtgt"

/* Macro's to obtain various fields from a packet */

#define	pkt_version(packet)  (unsigned int) *(packet->dat)
#define	pkt_msg_type(packet) (unsigned int) *(packet->dat+1)
#define	pkt_a_name(packet)   (packet->dat+2)
#define	pkt_a_inst(packet)   \
	(packet->dat+3+strlen((char *)pkt_a_name(packet)))
#define	pkt_a_realm(packet)  \
	(pkt_a_inst(packet)+1+strlen((char *)pkt_a_inst(packet)))

/* Macro to obtain realm from application request */
#define	apreq_realm(auth)	(auth->dat + 3)

#define	pkt_time_ws(packet) (char *) \
	(packet->dat+5+strlen((char *)pkt_a_name(packet)) + \
	    strlen((char *)pkt_a_inst(packet)) + \
	    strlen((char *)pkt_a_realm(packet)))

#define	pkt_no_req(packet) (unsigned short) \
	*(packet->dat+9+strlen((char *)pkt_a_name(packet)) + \
	    strlen((char *)pkt_a_inst(packet)) + \
	    strlen((char *)pkt_a_realm(packet)))
#define	pkt_x_date(packet) (char *) \
	(packet->dat+10+strlen((char *)pkt_a_name(packet)) + \
	    strlen((char *)pkt_a_inst(packet)) + \
	    strlen((char *)pkt_a_realm(packet)))
#define	pkt_err_code(packet) ((char *) \
	(packet->dat+9+strlen((char *)pkt_a_name(packet)) + \
	    strlen((char *)pkt_a_inst(packet)) + \
	    strlen((char *)pkt_a_realm(packet))))
#define	pkt_err_text(packet) \
	(packet->dat+13+strlen((char *)pkt_a_name(packet)) + \
	    strlen((char *)pkt_a_inst(packet)) + \
	    strlen((char *)pkt_a_realm(packet)))

/* Routines to create and read packets may be found in prot.c */

#ifdef RUBBISH
KTEXT create_auth_reply();
KTEXT create_death_packet();
KTEXT pkt_cipher();
#endif

/* Message types , always leave lsb for byte order */

#define		AUTH_MSG_KDC_REQUEST			 1<<1
#define		AUTH_MSG_KDC_REPLY			 2<<1
#define		AUTH_MSG_APPL_REQUEST			 3<<1
#define		AUTH_MSG_APPL_REQUEST_MUTUAL		 4<<1
#define		AUTH_MSG_ERR_REPLY			 5<<1
#define		AUTH_MSG_PRIVATE			 6<<1
#define		AUTH_MSG_SAFE				 7<<1
#define		AUTH_MSG_APPL_ERR			 8<<1
#define		AUTH_MSG_DIE				63<<1

/* values for kerb error codes */

#define		KERB_ERR_OK				 0
#define		KERB_ERR_NAME_EXP			 1
#define		KERB_ERR_SERVICE_EXP			 2
#define		KERB_ERR_AUTH_EXP			 3
#define		KERB_ERR_PKT_VER			 4
#define		KERB_ERR_NAME_MAST_KEY_VER		 5
#define		KERB_ERR_SERV_MAST_KEY_VER		 6
#define		KERB_ERR_BYTE_ORDER			 7
#define		KERB_ERR_PRINCIPAL_UNKNOWN		 8
#define		KERB_ERR_PRINCIPAL_NOT_UNIQUE		 9
#define		KERB_ERR_NULL_KEY			10

#ifdef	__cplusplus
}
#endif

#endif	/* _KERBEROS_PROT_H */
