/*
 * Copyright (c) 2011, PADL Software Pty Ltd.
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
 * 3. Neither the name of PADL Software nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY PADL SOFTWARE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL PADL SOFTWARE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
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

#include <config.h>
#include <string.h>
#include <stdlib.h>

#include "gs2_token.h"

#ifndef HAVE_GSS_ENCAPSULATE_TOKEN
/* XXXX this code currently makes the assumption that a mech oid will
   never be longer than 127 bytes.  This assumption is not inherent in
   the interfaces, so the code can be fixed if the OSI namespace
   balloons unexpectedly. */

/*
 * Each token looks like this:
 * 0x60                 tag for APPLICATION 0, SEQUENCE
 *                              (constructed, definite-length)
 * <length>             possible multiple bytes, need to parse/generate
 * 0x06                 tag for OBJECT IDENTIFIER
 * <moid_length>        compile-time constant string (assume 1 byte)
 * <moid_bytes>         compile-time constant string
 * <inner_bytes>        the ANY containing the application token
 * bytes 0,1 are the token type
 * bytes 2,n are the token data
 *
 * Note that the token type field is a feature of RFC 1964 mechanisms and
 * is not used by other GSSAPI mechanisms.  As such, a token type of -1
 * is interpreted to mean that no token type should be expected or
 * generated.
 *
 * For the purposes of this abstraction, the token "header" consists of
 * the sequence tag and length octets, the mech OID DER encoding, and the
 * first two inner bytes, which indicate the token type.  The token
 * "body" consists of everything else.
 */

static size_t
der_length_size(size_t length)
{
    if (length < (1<<7))
        return 1;
    else if (length < (1<<8))
        return 2;
#if INT_MAX == 0x7fff
    else
        return 3;
#else
    else if (length < (1<<16))
        return 3;
    else if (length < (1<<24))
        return 4;
    else
        return 5;
#endif
}

static void
der_write_length(unsigned char **buf, size_t length)
{
    if (length < (1<<7)) {
        *(*buf)++ = (unsigned char)length;
    } else {
        *(*buf)++ = (unsigned char)(der_length_size(length)+127);
#if INT_MAX > 0x7fff
        if (length >= (1<<24))
            *(*buf)++ = (unsigned char)(length>>24);
        if (length >= (1<<16))
            *(*buf)++ = (unsigned char)((length>>16)&0xff);
#endif
        if (length >= (1<<8))
            *(*buf)++ = (unsigned char)((length>>8)&0xff);
        *(*buf)++ = (unsigned char)(length&0xff);
    }
}

/* returns the length of a token, given the mech oid and the body size */

static size_t
token_size(const gss_OID_desc *mech, size_t body_size)
{
    /* set body_size to sequence contents size */
    body_size += 2 + (size_t) mech->length;         /* NEED overflow check */
    return 1 + der_length_size(body_size) + body_size;
}

/* fills in a buffer with the token header.  The buffer is assumed to
   be the right size.  buf is advanced past the token header */

static void
make_token_header(
    const gss_OID_desc *mech,
    size_t body_size,
    unsigned char **buf)
{
    *(*buf)++ = 0x60;
    der_write_length(buf, 2 + mech->length + body_size);
    *(*buf)++ = 0x06;
    *(*buf)++ = (unsigned char)mech->length;
    memcpy(*buf, mech->elements, mech->length);
    *buf += mech->length;
}

OM_uint32
gs2_encapsulate_token(const gss_buffer_t input_token,
                      const gss_OID token_oid,
                      gss_buffer_t output_token)
{
    size_t tokenSize;
    unsigned char *buf;

    if (input_token == GSS_C_NO_BUFFER || token_oid == GSS_C_NO_OID)
        return GSS_S_CALL_INACCESSIBLE_READ;

    if (output_token == GSS_C_NO_BUFFER)
        return GSS_S_CALL_INACCESSIBLE_WRITE;

    tokenSize = token_size(token_oid, input_token->length);

    output_token->value = malloc(tokenSize);
    if (output_token->value == NULL)
        return GSS_S_FAILURE;

    buf = output_token->value;

    make_token_header(token_oid, input_token->length, &buf);
    memcpy(buf, input_token->value, input_token->length);
    output_token->length = tokenSize;

    return GSS_S_COMPLETE;
}
#endif


#ifndef HAVE_GSS_DECAPSULATE_TOKEN
/* returns decoded length, or < 0 on failure.  Advances buf and
   decrements bufsize */

static int
der_read_length(unsigned char **buf, ssize_t *bufsize)
{
    unsigned char sf;
    int ret;

    if (*bufsize < 1)
        return -1;

    sf = *(*buf)++;
    (*bufsize)--;
    if (sf & 0x80) {
        if ((sf &= 0x7f) > ((*bufsize)-1))
            return -1;
        if (sf > sizeof(int))
            return -1;
        ret = 0;
        for (; sf; sf--) {
            ret = (ret<<8) + (*(*buf)++);
            (*bufsize)--;
        }
    } else {
        ret = sf;
    }

    return ret;
}

/*
 * Given a buffer containing a token, reads and verifies the token,
 * leaving buf advanced past the token header, and setting body_size
 * to the number of remaining bytes.  Returns 0 on success,
 * G_BAD_TOK_HEADER for a variety of errors, and G_WRONG_MECH if the
 * mechanism in the token does not match the mech argument.  buf and
 * *body_size are left unmodified on error.
 */

static OM_uint32
verify_token_header(OM_uint32 *minor,
                    const gss_OID mech,
                    size_t *body_size,
                    unsigned char **buf_in,
                    size_t toksize_in)
{
    unsigned char *buf = *buf_in;
    ssize_t seqsize;
    gss_OID_desc toid;
    ssize_t toksize = (ssize_t)toksize_in;

    *minor = 0;

    if ((toksize -= 1) < 0)
        return GSS_S_DEFECTIVE_TOKEN;

    if (*buf++ != 0x60)
        return GSS_S_DEFECTIVE_TOKEN;

    seqsize = der_read_length(&buf, &toksize);
    if (seqsize < 0)
        return GSS_S_DEFECTIVE_TOKEN;

    if (seqsize != toksize)
        return GSS_S_DEFECTIVE_TOKEN;

    if ((toksize -= 1) < 0)
        return GSS_S_DEFECTIVE_TOKEN;

    if (*buf++ != 0x06)
        return GSS_S_DEFECTIVE_TOKEN;

    if ((toksize -= 1) < 0)
        return GSS_S_DEFECTIVE_TOKEN;

    toid.length = *buf++;

    if ((toksize -= toid.length) < 0)
        return GSS_S_DEFECTIVE_TOKEN;

    toid.elements = buf;
    buf += toid.length;

    if (!gss_oid_equal(&toid, mech))
        return GSS_S_DEFECTIVE_TOKEN;

    *buf_in = buf;
    *body_size = toksize;

    return GSS_S_COMPLETE;
}

OM_uint32
gs2_decapsulate_token(const gss_buffer_t input_token,
                      const gss_OID token_oid,
                      gss_buffer_t output_token)
{
    OM_uint32 major, minor;
    size_t body_size = 0;
    unsigned char *buf_in;

    if (input_token == GSS_C_NO_BUFFER || token_oid == GSS_C_NO_OID)
        return GSS_S_CALL_INACCESSIBLE_READ;

    if (output_token == GSS_C_NO_BUFFER)
        return GSS_S_CALL_INACCESSIBLE_WRITE;

    buf_in = input_token->value;

    major = verify_token_header(&minor, token_oid, &body_size, &buf_in,
                                input_token->length);
    if (minor != 0)
        return GSS_S_DEFECTIVE_TOKEN;

    output_token->value = malloc(body_size);
    if (output_token->value == NULL)
        return GSS_S_FAILURE;

    memcpy(output_token->value, buf_in, body_size);
    output_token->length = body_size;

    return GSS_S_COMPLETE;
}
#endif

#ifndef HAVE_GSS_OID_EQUAL
int
gs2_oid_equal(const gss_OID o1, const gss_OID o2)
{
    return o1->length == o2->length &&
        (memcmp(o1->elements, o2->elements, o1->length) == 0);
}
#endif
