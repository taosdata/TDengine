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
 * 3. Neither the name of the Institute nor the names of its contributors
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

RCSID("$Id: mk_priv.c,v 1.2 2001/12/04 02:06:08 rjs3 Exp $");

/* application include files */
#include "krb-archaeology.h"

/*
 * krb_mk_priv() constructs an AUTH_MSG_PRIVATE message.  It takes
 * some user data "in" of "length" bytes and creates a packet in "out"
 * consisting of the user data, a timestamp, and the sender's network
 * address.
 * The packet is encrypted by pcbc_encrypt(), using the given
 * "key" and "schedule".
 * The length of the resulting packet "out" is
 * returned.
 *
 * It is similar to krb_mk_safe() except for the additional key
 * schedule argument "schedule" and the fact that the data is encrypted
 * rather than appended with a checksum.  The protocol version is
 * KRB_PROT_VERSION, defined in "krb.h".
 *
 * The "out" packet consists of:
 *
 * Size			Variable		Field
 * ----			--------		-----
 *
 * 1 byte		KRB_PROT_VERSION	protocol version number
 * 1 byte		AUTH_MSG_PRIVATE |	message type plus local
 *			HOST_BYTE_ORDER		byte order in low bit
 *
 * 4 bytes		c_length		length of data
 * we encrypt from here with pcbc_encrypt
 * 
 * 4 bytes		length			length of user data
 * length		in			user data
 * 1 byte		msg_time_5ms		timestamp milliseconds
 * 4 bytes		sender->sin.addr.s_addr	sender's IP address
 *
 * 4 bytes		msg_time_sec or		timestamp seconds with
 *			-msg_time_sec		direction in sign bit
 *
 * 0<=n<=7  bytes	pad to 8 byte multiple	zeroes
 */

int32_t
krb_mk_priv(void *in, void *out, u_int32_t length, 
	    struct des_ks_struct *schedule, des_cblock *key, 
	    struct sockaddr_in *sender, struct sockaddr_in *receiver)
{
    unsigned char *p = (unsigned char*)out;
    unsigned char *cipher;

    struct timeval tv;
    u_int32_t src_addr;
    u_int32_t len;

    p += krb_put_int(KRB_PROT_VERSION, p, 1, 1);
    p += krb_put_int(AUTH_MSG_PRIVATE, p, 1, 1);

    len = 4 + length + 1 + 4 + 4;
    len = (len + 7) & ~7;
    p += krb_put_int(len, p, 4, 4);
    
    cipher = p;

    p += krb_put_int(length, p, 4, 4);
    
    memcpy(p, in, length);
    p += length;
    
    krb_kdctimeofday(&tv);

    *p++ =tv.tv_usec / 5000;
    
    src_addr = sender->sin_addr.s_addr;
    p += krb_put_address(src_addr, p, 4);

    p += krb_put_int(lsb_time(tv.tv_sec, sender, receiver), p, 4, 4);
    
    memset(p, 0, 7);

    des_pcbc_encrypt((des_cblock *)cipher, (des_cblock *)cipher,
		     len, schedule, key, DES_ENCRYPT);

    return  (cipher - (unsigned char*)out) + len;
}
