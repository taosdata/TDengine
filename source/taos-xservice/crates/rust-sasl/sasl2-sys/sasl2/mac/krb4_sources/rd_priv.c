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

RCSID("$Id: rd_priv.c,v 1.2 2001/12/04 02:06:09 rjs3 Exp $");

/* application include files */
#include "krb-archaeology.h"

/*
 * krb_rd_priv() decrypts and checks the integrity of an
 * AUTH_MSG_PRIVATE message.  Given the message received, "in",
 * the length of that message, "in_length", the key "schedule"
 * and "key", and the network addresses of the
 * "sender" and "receiver" of the message, krb_rd_safe() returns
 * RD_AP_OK if the message is okay, otherwise some error code.
 *
 * The message data retrieved from "in" are returned in the structure
 * "m_data".  The pointer to the application data
 * (m_data->app_data) refers back to the appropriate place in "in".
 *
 * See the file "mk_priv.c" for the format of the AUTH_MSG_PRIVATE
 * message.  The structure containing the extracted message
 * information, MSG_DAT, is defined in "krb.h".
 */

int32_t
krb_rd_priv(void *in, u_int32_t in_length, 
	    struct des_ks_struct *schedule, des_cblock *key, 
	    struct sockaddr_in *sender, struct sockaddr_in *receiver, 
	    MSG_DAT *m_data)
{
    unsigned char *p = (unsigned char*)in;
    int little_endian;
    u_int32_t clen;
    struct timeval tv;
    u_int32_t src_addr;
    int delta_t;

    unsigned char pvno, type;

    pvno = *p++;
    if(pvno != KRB_PROT_VERSION)
	return RD_AP_VERSION;
    
    type = *p++;
    little_endian = type & 1;
    type &= ~1;

    p += krb_get_int(p, &clen, 4, little_endian);
    
    if(clen + 2 > in_length)
	return RD_AP_MODIFIED;

    des_pcbc_encrypt((des_cblock*)p, (des_cblock*)p, clen, 
		     schedule, key, DES_DECRYPT);
    
    p += krb_get_int(p, &m_data->app_length, 4, little_endian);
    if(m_data->app_length + 17 > in_length)
	return RD_AP_MODIFIED;

    m_data->app_data = p;
    p += m_data->app_length;
    
    m_data->time_5ms = *p++;

    p += krb_get_address(p, &src_addr);

    if (!krb_equiv(src_addr, sender->sin_addr.s_addr))
	return RD_AP_BADD;

    p += krb_get_int(p, (u_int32_t *)&m_data->time_sec, 4, little_endian);

    m_data->time_sec = lsb_time(m_data->time_sec, sender, receiver);
    
    gettimeofday(&tv, NULL);

    /* check the time integrity of the msg */
    delta_t = abs((int)((long) tv.tv_sec - m_data->time_sec));
    if (delta_t > CLOCK_SKEW)
	return RD_AP_TIME;
    if (krb_debug)
	krb_warning("delta_t = %d\n", (int) delta_t);

    /*
     * caller must check timestamps for proper order and
     * replays, since server might have multiple clients
     * each with its own timestamps and we don't assume
     * tightly synchronized clocks.
     */

    return KSUCCESS;
}
