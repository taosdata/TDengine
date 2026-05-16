/*
 * $Source: /cvs/src/sasl/mac/kerberos_includes/klog.h,v $
 * $Author: rjs3 $
 * $Header: /cvs/src/sasl/mac/kerberos_includes/klog.h,v 1.2 2001/12/04 02:06:05 rjs3 Exp $
 *
 * Copyright 1988 by the Massachusetts Institute of Technology.
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>.
 *
 * This file defines the types of log messages logged by klog.  Each
 * type of message may be selectively turned on or off.
 */

#ifndef	_KERBEROS_KLOG_H
#define	_KERBEROS_KLOG_H

#pragma ident	"@(#)klog.h	1.3	92/07/14 SMI"

#include <kerberos/mit-copyright.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define	KRBLOG 		"/kerberos/kerberos.log"  /* master server  */
#define	KRBSLAVELOG	"/kerberos/kerberos_slave.log"  /* master server  */
#define	NLOGTYPE	100	/* Maximum number of log msg types  */

#define	L_NET_ERR	  1	/* Error in network code	    */
#define	L_NET_INFO	  2	/* Info on network activity	    */
#define	L_KRB_PERR	  3	/* Kerberos protocol errors	    */
#define	L_KRB_PINFO	  4	/* Kerberos protocol info	    */
#define	L_INI_REQ	  5	/* Request for initial ticket	    */
#define	L_NTGT_INTK	  6	/* Initial request not for TGT	    */
#define	L_DEATH_REQ	  7	/* Request for server death	    */
#define	L_TKT_REQ	  8	/* All ticket requests using a tgt  */
#define	L_ERR_SEXP	  9	/* Service expired		    */
#define	L_ERR_MKV	 10	/* Master key version incorrect	    */
#define	L_ERR_NKY	 11	/* User's key is null		    */
#define	L_ERR_NUN	 12	/* Principal not unique		    */
#define	L_ERR_UNK	 13	/* Principal Unknown		    */
#define	L_ALL_REQ	 14	/* All requests			    */
#define	L_APPL_REQ	 15	/* Application requests (using tgt) */
#define	L_KRB_PWARN	 16	/* Protocol warning messages	    */

char   *klog();

#ifdef	__cplusplus
}
#endif

#endif	/* _KERBEROS_KLOG_H */
