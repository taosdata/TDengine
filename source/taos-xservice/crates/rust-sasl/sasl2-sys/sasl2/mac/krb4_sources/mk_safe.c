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

RCSID("$Id: mk_safe.c,v 1.2 2001/12/04 02:06:08 rjs3 Exp $");

/* application include files */
#include "krb-archaeology.h"


/* from rd_safe.c */
extern int dqc_type;
void fixup_quad_cksum(void*, size_t, des_cblock*, void*, void*, int);

/*
 * krb_mk_safe() constructs an AUTH_MSG_SAFE message.  It takes some
 * user data "in" of "length" bytes and creates a packet in "out"
 * consisting of the user data, a timestamp, and the sender's network
 * address, followed by a checksum computed on the above, using the
 * given "key".  The length of the resulting packet is returned.
 *
 * The "out" packet consists of:
 *
 * Size			Variable		Field
 * ----			--------		-----
 *
 * 1 byte		KRB_PROT_VERSION	protocol version number
 * 1 byte		AUTH_MSG_SAFE |		message type plus local
 *			HOST_BYTE_ORDER		byte order in low bit
 *
 * ===================== begin checksum ================================
 * 
 * 4 bytes		length			length of user data
 * length		in			user data
 * 1 byte		msg_time_5ms		timestamp milliseconds
 * 4 bytes		sender->sin.addr.s_addr	sender's IP address
 *
 * 4 bytes		msg_time_sec or		timestamp seconds with
 *			-msg_time_sec		direction in sign bit
 *
 * ======================= end checksum ================================
 *
 * 16 bytes		big_cksum		quadratic checksum of
 *						above using "key"
 */

int32_t
krb_mk_safe(void *in, void *out, u_int32_t length, des_cblock *key, 
	    struct sockaddr_in *sender, struct sockaddr_in *receiver)
{
    unsigned char * p = (unsigned char*)out;
    struct timeval tv;
    unsigned char *start;
    u_int32_t src_addr;

    p += krb_put_int(KRB_PROT_VERSION, p, 1, 1);
    p += krb_put_int(AUTH_MSG_SAFE, p, 1, 1);
    
    start = p;

    p += krb_put_int(length, p, 4, 4);

    memcpy(p, in, length);
    p += length;
    
    krb_kdctimeofday(&tv);

    *p++ = tv.tv_usec/5000; /* 5ms */
    
    src_addr = sender->sin_addr.s_addr;
    p += krb_put_address(src_addr, p, 4);

    p += krb_put_int(lsb_time(tv.tv_sec, sender, receiver), p, 4, 4);

    {
	/* We are faking big endian mode, so we need to fix the
	 * checksum (that is byte order dependent). We always send a
	 * checksum of the new type, unless we know that we are
	 * talking to an old client (this requires a call to
	 * krb_rd_safe first).  
	 */
	unsigned char new_checksum[16];
	unsigned char old_checksum[16];
	fixup_quad_cksum(start, p - start, key, new_checksum, old_checksum, 0);
	
	if((dqc_type == DES_QUAD_GUESS && DES_QUAD_DEFAULT == DES_QUAD_OLD) || 
	   dqc_type == DES_QUAD_OLD)
	    memcpy(p, old_checksum, 16);
	else
	    memcpy(p, new_checksum, 16);
    }
    p += 16;

    return p - (unsigned char*)out;
}
