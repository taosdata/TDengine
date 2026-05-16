/*
 * Copyright (c) 1995, 1996, 1997 Kungliga Tekniska Hšgskolan
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

RCSID("$Id: lsb_addr_comp.c,v 1.2 2001/12/04 02:06:08 rjs3 Exp $");

#include "krb-archaeology.h"

int
krb_lsb_antinet_ulong_cmp(u_int32_t x, u_int32_t y)
{
    int i;
    u_int32_t a = 0, b = 0;
    u_int8_t *p = (u_int8_t*) &x;
    u_int8_t *q = (u_int8_t*) &y;

    for(i = sizeof(u_int32_t) - 1; i >= 0; i--){
	a = (a << 8) | p[i];
	b = (b << 8) | q[i];
    }
    if(a > b)
	return 1;
    if(a < b)
	return -1;
    return 0;
}

int
krb_lsb_antinet_ushort_cmp(u_int16_t x, u_int16_t y)
{
    int i;
    u_int16_t a = 0, b = 0;
    u_int8_t *p = (u_int8_t*) &x;
    u_int8_t *q = (u_int8_t*) &y;

    for(i = sizeof(u_int16_t) - 1; i >= 0; i--){
	a = (a << 8) | p[i];
	b = (b << 8) | q[i];
    }
    if(a > b)
	return 1;
    if(a < b)
	return -1;
    return 0;
}

u_int32_t
lsb_time(time_t t, struct sockaddr_in *src, struct sockaddr_in *dst)
{
    int dir = 1;
    const char *fw;

    /*
     * direction bit is the sign bit of the timestamp.  Ok until
     * 2038??
     */
    if(krb_debug) {
	krb_warning("lsb_time: src = %s:%u\n", 
		    inet_ntoa(src->sin_addr.s_addr), ntohs(src->sin_port));
	krb_warning("lsb_time: dst = %s:%u\n", 
		    inet_ntoa(dst->sin_addr.s_addr), ntohs(dst->sin_port));
    }

    /* For compatibility with broken old code, compares are done in VAX 
       byte order (LSBFIRST) */ 
    if (krb_lsb_antinet_ulong_less(src->sin_addr.s_addr, /* src < recv */ 
				   dst->sin_addr.s_addr) < 0) 
        dir = -1;
    else if (krb_lsb_antinet_ulong_less(src->sin_addr.s_addr, 
					dst->sin_addr.s_addr)==0) 
        if (krb_lsb_antinet_ushort_less(src->sin_port, dst->sin_port) < 0)
            dir = -1;
    /*
     * all that for one tiny bit!  Heaven help those that talk to
     * themselves.
     */
    if(krb_get_config_bool("reverse_lsb_test")) {
	if(krb_debug) 
	    krb_warning("lsb_time: reversing direction: %d -> %d\n", dir, -dir);
	dir = -dir;
    }   
#ifdef RUBBISH   
    else if((fw = krb_get_config_string("firewall_address"))) {
	struct in_addr fw_addr;
	fw_addr.sin_addr.s_addr = inet_addr(fw);
	if(fw_addr.s_addr != INADDR_NONE) {
	    int s_lt_d, d_lt_f;
	    krb_warning("lsb_time: fw = %s\n", inet_ntoa(fw_addr));
	    /* negate if src < dst < fw || fw < dst < src */
	    s_lt_d = (krb_lsb_antinet_ulong_less(src->sin_addr.s_addr,
						 dst->sin_addr.s_addr) == -1);
	    d_lt_f = (krb_lsb_antinet_ulong_less(fw_addr.s_addr,
						 dst->sin_addr.s_addr) == 1);
	    if((s_lt_d ^ d_lt_f) == 0) {
		if(krb_debug) 
		    krb_warning("lsb_time: reversing direction: %d -> %d\n", 
				dir, -dir);
		dir = -dir;
	    }
	}
    }
#endif
    t = t * dir;
    t = t & 0xffffffff;
    return t;
}
