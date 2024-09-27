/*
 * Copyright 1993 by OpenVision Technologies, Inc.
 * 
 * Permission to use, copy, modify, distribute, and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appears in all copies and
 * that both that copyright notice and this permission notice appear in
 * supporting documentation, and that the name of OpenVision not be used
 * in advertising or publicity pertaining to distribution of the software
 * without specific, written prior permission. OpenVision makes no
 * representations about the suitability of this software for any
 * purpose.  It is provided "as is" without express or implied warranty.
 * 
 * OPENVISION DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
 * INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO
 * EVENT SHALL OPENVISION BE LIABLE FOR ANY SPECIAL, INDIRECT OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
 * USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef _GSSAPI_KRB5_H_
#define _GSSAPI_KRB5_H_

#if defined(macintosh) || (defined(__MACH__) && defined(__APPLE__))
	#include <KerberosSupport/KerberosSupport.h>
#endif

#if TARGET_OS_MAC
#include <Kerberos5/Kerberos5.h>
#else
#include <krb5.h>
#endif

/* C++ friendlyness */
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/* Reserved static storage for GSS_oids.  See rfc 1964 for more details. */

/* 2.1.1. Kerberos Principal Name Form: */
GSS_DLLIMP extern const gss_OID_desc * const GSS_KRB5_NT_PRINCIPAL_NAME;
/* This name form shall be represented by the Object Identifier {iso(1)
 * member-body(2) United States(840) mit(113554) infosys(1) gssapi(2)
 * krb5(2) krb5_name(1)}.  The recommended symbolic name for this type
 * is "GSS_KRB5_NT_PRINCIPAL_NAME". */

/* 2.1.2. Host-Based Service Name Form */
#define GSS_KRB5_NT_HOSTBASED_SERVICE_NAME GSS_C_NT_HOSTBASED_SERVICE
/* This name form shall be represented by the Object Identifier {iso(1)
 * member-body(2) United States(840) mit(113554) infosys(1) gssapi(2)
 * generic(1) service_name(4)}.  The previously recommended symbolic
 * name for this type is "GSS_KRB5_NT_HOSTBASED_SERVICE_NAME".  The
 * currently preferred symbolic name for this type is
 * "GSS_C_NT_HOSTBASED_SERVICE". */

/* 2.2.1. User Name Form */
#define GSS_KRB5_NT_USER_NAME GSS_C_NT_USER_NAME    
/* This name form shall be represented by the Object Identifier {iso(1)
 * member-body(2) United States(840) mit(113554) infosys(1) gssapi(2)
 * generic(1) user_name(1)}.  The recommended symbolic name for this
 * type is "GSS_KRB5_NT_USER_NAME". */

/* 2.2.2. Machine UID Form */
#define GSS_KRB5_NT_MACHINE_UID_NAME GSS_C_NT_MACHINE_UID_NAME
/* This name form shall be represented by the Object Identifier {iso(1)
 * member-body(2) United States(840) mit(113554) infosys(1) gssapi(2)
 * generic(1) machine_uid_name(2)}.  The recommended symbolic name for
 * this type is "GSS_KRB5_NT_MACHINE_UID_NAME". */

/* 2.2.3. String UID Form */
#define GSS_KRB5_NT_STRING_UID_NAME GSS_C_NT_STRING_UID_NAME
/* This name form shall be represented by the Object Identifier {iso(1)
 * member-body(2) United States(840) mit(113554) infosys(1) gssapi(2)
 * generic(1) string_uid_name(3)}.  The recommended symbolic name for
 * this type is "GSS_KRB5_NT_STRING_UID_NAME". */ 

extern const gss_OID_desc * const gss_mech_krb5;
extern const gss_OID_desc * const gss_mech_krb5_old;
extern const gss_OID_desc * const gss_mech_krb5_v2;
extern const gss_OID_set_desc * const gss_mech_set_krb5;
extern const gss_OID_set_desc * const gss_mech_set_krb5_old;
extern const gss_OID_set_desc * const gss_mech_set_krb5_both;
extern const gss_OID_set_desc * const gss_mech_set_krb5_v2;
extern const gss_OID_set_desc * const gss_mech_set_krb5_v1v2;

extern const gss_OID_desc * const gss_nt_krb5_name;
extern const gss_OID_desc * const gss_nt_krb5_principal;

extern const gss_OID_desc krb5_gss_oid_array[];

#define gss_krb5_nt_general_name	gss_nt_krb5_name
#define gss_krb5_nt_principal		gss_nt_krb5_principal
#define gss_krb5_nt_service_name	gss_nt_service_name
#define gss_krb5_nt_user_name		gss_nt_user_name
#define gss_krb5_nt_machine_uid_name	gss_nt_machine_uid_name
#define gss_krb5_nt_string_uid_name	gss_nt_string_uid_name

GSS_DLLIMP OM_uint32 KRB5_CALLCONV gss_krb5_get_tkt_flags 
	PROTOTYPE((OM_uint32 *minor_status,
		   gss_ctx_id_t context_handle,
		   krb5_flags *ticket_flags));

GSS_DLLIMP OM_uint32 KRB5_CALLCONV gss_krb5_copy_ccache
	PROTOTYPE((OM_uint32 *minor_status,
		   gss_cred_id_t cred_handle,
		   krb5_ccache out_ccache));

GSS_DLLIMP OM_uint32 KRB5_CALLCONV gss_krb5_ccache_name
	PROTOTYPE((OM_uint32 *minor_status, const char *name,
		   const char **out_name));

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _GSSAPI_KRB5_H_ */
