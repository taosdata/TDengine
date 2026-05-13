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

#ifndef _GS2_TOKEN_H_
#define _GS2_TOKEN_H_ 1

#include <config.h>

#include <gssapi/gssapi.h>

#ifndef KRB5_HEIMDAL
#ifdef HAVE_GSSAPI_GSSAPI_EXT_H
#include <gssapi/gssapi_ext.h>
#endif
#endif

#ifndef HAVE_GSS_DECAPSULATE_TOKEN
OM_uint32
gs2_decapsulate_token(const gss_buffer_t input_token,
                      const gss_OID token_oid,
                      gss_buffer_t output_token);
#define gss_decapsulate_token gs2_decapsulate_token
#endif

#ifndef HAVE_GSS_ENCAPSULATE_TOKEN
OM_uint32
gs2_encapsulate_token(const gss_buffer_t input_token,
                      const gss_OID token_oid,
                      gss_buffer_t output_token);
#define gss_encapsulate_token gs2_encapsulate_token
#endif

#ifndef HAVE_GSS_OID_EQUAL
int
gs2_oid_equal(const gss_OID o1, const gss_OID o2);
#define gss_oid_equal gs2_oid_equal
#endif

#endif /* _GS2_TOKEN_H_ */
