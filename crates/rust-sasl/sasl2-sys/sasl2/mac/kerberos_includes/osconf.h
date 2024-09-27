/*
 * $Source: /afs/athena.mit.edu/astaff/project/kerberos/src/include/RCS/osconf.h
 * $Author: rjs3 $
 * $Header: /afs/athena.mit.edu/astaff/project/kerberos/src/include/RCS/osconf.h
 *		4.4 89/12/19 13:26:27 jtkohl Exp $
 *
 * Copyright 1988 by the Massachusetts Institute of Technology.
 *
 * For copying and distribution information, please see the file
 * <mit-copyright.h>.
 *
 * Athena configuration.
 */

#ifndef	_KERBEROS_OSCONF_H
#define	_KERBEROS_OSCONF_H

#pragma ident	"@(#)osconf.h	1.7	94/07/29 SMI"

#include <kerberos/mit-copyright.h>

#ifdef tahoe
#include <kerberos/conf-bsdtahoe.h>
#else /* !tahoe */
#ifdef vax
#include <kerberos/conf-bsdvax.h>
#else /* !vax */
#if defined(mips) && defined(ultrix)
#include <kerberos/conf-ultmips2.h>
#else /* !Ultrix MIPS-2 */
#ifdef ibm032
#include <kerberos/conf-bsdibm032.h>
#else /* !ibm032 */
#ifdef apollo
#include <kerberos/conf-bsdapollo.h>
#else /* !apollo */
#ifdef sun
#ifdef sparc
#if defined(SunOS) && SunOS >= 50
#include <kerberos/conf-svsparc.h>
#else
#include <kerberos/conf-bsdsparc.h>
#endif
#else /* sun but not sparc */
#ifdef i386
#include <kerberos/conf-bsd386i.h>
#else /* sun but not sparc or i386 */
#ifdef __ppc
#include <kerberos/conf-svppc.h>
#else /* sun but not (sparc, i386, or ppc) */
#include <kerberos/conf-bsdm68k.h>
#endif /* ppc */
#endif /* i386 */
#endif /* sparc */
#else /* !sun */
#ifdef pyr
#include <kerberos/conf-pyr.h>
#endif /* pyr */
#endif /* sun */
#endif /* apollo */
#endif /* ibm032 */
#endif /* mips */
#endif /* vax */
#endif /* tahoe */

#endif	/* _KERBEROS_OSCONF_H */
