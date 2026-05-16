/*
 * Header file for common error description library.
 *
 * Copyright 1988, Student Information Processing Board of the
 * Massachusetts Institute of Technology.
 *
 * Copyright 1995 by Cygnus Support.
 *
 * For copyright and distribution info, see the documentation supplied
 * with this package.
 */

#ifndef __COM_ERR_H

#if defined(_MSDOS) || defined(_WIN32) || defined(macintosh)
#include <Kerberos5/win-mac.h>
#if defined(macintosh) && defined(__CFM68K__) && !defined(__USING_STATIC_LIBS__)
#pragma import on
#endif
#endif

#ifndef KRB5_CALLCONV
#define KRB5_CALLCONV
#define KRB5_CALLCONV_C
#define KRB5_DLLIMP
#define GSS_DLLIMP
#define KRB5_EXPORTVAR
#endif

#ifndef FAR
#define FAR
#define NEAR
#endif

#if defined(__STDC__) || defined(__cplusplus) || defined(_MSDOS) || defined(_WIN32) || defined(macintosh)

/* End-user programs may need this -- oh well */
#ifndef HAVE_STDARG_H
#define HAVE_STDARG_H 1
#endif

#define ET_P(x) x

#else
#define ET_P(x) ()
#endif /* __STDC__ */

#ifdef HAVE_STDARG_H
#include <stdarg.h>
#define	ET_STDARG_P(x) x
#else
#include <varargs.h>
#define ET_STDARG_P(x) ()
#define ET_VARARGS
#endif

typedef long errcode_t;
typedef void (*et_old_error_hook_func) ET_P((const char FAR *, errcode_t,
					     const char FAR *, va_list ap));
	
struct error_table {
	char const FAR * const FAR * msgs;
	unsigned long base;
	unsigned int n_msgs;
};

#ifdef __cplusplus
extern "C" {
#endif

KRB5_DLLIMP extern void KRB5_CALLCONV_C com_err
	ET_STDARG_P((const char FAR *, errcode_t, const char FAR *, ...));
KRB5_DLLIMP extern void KRB5_CALLCONV com_err_va
	ET_P((const char FAR *whoami, errcode_t code, const char FAR *fmt,
	      va_list ap));
KRB5_DLLIMP extern const char FAR * KRB5_CALLCONV error_message
	ET_P((errcode_t));
KRB5_DLLIMP extern errcode_t KRB5_CALLCONV add_error_table
	ET_P((const struct error_table FAR *));
KRB5_DLLIMP extern errcode_t KRB5_CALLCONV remove_error_table
	ET_P((const struct error_table FAR *));

#if !defined(_MSDOS) && !defined(_WIN32) && !defined(macintosh) && !defined(__MACH__)
/*
 * The display routine should be application specific.  A global hook,
 * may cause inappropriate display procedures to be called between
 * applications under non-Unix environments.
 */

extern et_old_error_hook_func set_com_err_hook
	ET_P((et_old_error_hook_func));
extern et_old_error_hook_func reset_com_err_hook
	ET_P((void));
#endif

#ifdef __cplusplus
}
#endif

#if defined(macintosh) && defined(__CFM68K__) && !defined(__USING_STATIC_LIBS__)
#pragma import reset
#endif

#define __COM_ERR_H
#endif /* ! defined(__COM_ERR_H) */
