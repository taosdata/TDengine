/*
 * $Source: /cvs/src/sasl/mac/kerberos_includes/lsb_addr_comp.h,v $
 * $Author: rjs3 $
 * $Header: /cvs/src/sasl/mac/kerberos_includes/lsb_addr_comp.h,v 1.2 2001/12/04 02:06:05 rjs3 Exp $
 *
 * Copyright 1988 by the Massachusetts Institute of Technology.
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>.
 *
 * Comparison macros to emulate LSBFIRST comparison results of network
 * byte-order quantities
 */

#ifndef	_KERBEROS_LSB_ADDR_COMP_H
#define	_KERBEROS_LSB_ADDR_COMP_H

#pragma ident	"@(#)lsb_addr_comp.h	1.4	93/02/04 SMI"

#include <kerberos/mit-copyright.h>
#include <kerberos/osconf.h>

#ifdef	__cplusplus
extern "C" {
#endif

#ifdef LSBFIRST
#define	lsb_net_ulong_less(x, y)	((x < y) ? -1 : ((x > y) ? 1 : 0))
#define	lsb_net_ushort_less(x, y)	((x < y) ? -1 : ((x > y) ? 1 : 0))
#else
/* MSBFIRST */
#define	u_char_comp(x, y) \
	(((x) > (y)) ? (1) : (((x) == (y)) ? (0) : (-1)))
/* This is gross, but... */
#define	lsb_net_ulong_less(x, y) long_less_than((u_char *)&x, (u_char *)&y)
#define	lsb_net_ushort_less(x, y) short_less_than((u_char *)&x, (u_char *)&y)

#define	long_less_than(x, y) \
	(u_char_comp((x)[3], (y)[3]) ? u_char_comp((x)[3], (y)[3]) : \
	    (u_char_comp((x)[2], (y)[2]) ? u_char_comp((x)[2], (y)[2]) : \
	    (u_char_comp((x)[1], (y)[1]) ? u_char_comp((x)[1], (y)[1]) : \
	    (u_char_comp((x)[0], (y)[0])))))
#define	short_less_than(x, y) \
	(u_char_comp((x)[1], (y)[1]) ? u_char_comp((x)[1], (y)[1]) : \
	    (u_char_comp((x)[0], (y)[0])))

#endif /* LSBFIRST */

#ifdef	__cplusplus
}
#endif

#endif	/* _KERBEROS_LSB_ADDR_COMP_H */
