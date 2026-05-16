/*
 * $Source: /cvs/src/sasl/mac/kerberos_includes/conf.h,v $
 * $Author: rjs3 $
 * $Header: /cvs/src/sasl/mac/kerberos_includes/conf.h,v 1.2 2001/12/04 02:06:05 rjs3 Exp $
 *
 * Copyright 1988 by the Massachusetts Institute of Technology.
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>.
 *
 * Configuration info for operating system, hardware description,
 * language implementation, C library, etc.
 *
 * This file should be included in (almost) every file in the Kerberos
 * sources, and probably should *not* be needed outside of those
 * sources.  (How do we deal with /usr/include/des.h and
 * /usr/include/krb.h?)
 */

#ifndef	_KERBEROS_CONF_H
#define	_KERBEROS_CONF_H

#pragma ident	"@(#)conf.h	1.5	93/02/04 SMI"

#include <kerberos/mit-copyright.h>

#include <kerberos/osconf.h>

#ifdef SHORTNAMES
#include <kerberos/names.h>
#endif

#ifdef	__cplusplus
extern "C" {
#endif

/*
 * Language implementation-specific definitions
 */

/* special cases */
#ifdef __HIGHC__
/* broken implementation of ANSI C */
#undef __STDC__
#endif

#ifndef __STDC__
#define	const
#define	volatile
#define	signed
typedef char *pointer;		/* pointer to generic data */
#define	PROTOTYPE(p) ()
#else
typedef void *pointer;
#define	PROTOTYPE(p) p
#endif

/* Does your compiler understand "void"? */
#ifdef notdef
#define	void int
#endif

/*
 * A few checks to see that necessary definitions are included.
 */

/* byte order */

#ifndef MSBFIRST
#ifndef LSBFIRST
/* #error byte order not defined */
Error: byte order not defined.
#endif
#endif

/* machine size */
#ifndef BITS16
#ifndef BITS32
Error: how big is this machine anyways?
#endif
#endif

/* end of checks */

#ifdef	__cplusplus
}
#endif

#endif	/* _KERBEROS_CONF_H */
