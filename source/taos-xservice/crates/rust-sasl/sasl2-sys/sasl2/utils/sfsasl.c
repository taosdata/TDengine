#include <config.h>
#include <stdlib.h>
#include <sasl.h>
#include <sfio.h>

/* 
 * Copyright (c) 1998-2016 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact  
 *      Carnegie Mellon University
 *      Center for Technology Transfer and Enterprise Creation
 *      4615 Forbes Avenue
 *      Suite 302
 *      Pittsburgh, PA  15213
 *      (412) 268-7393, fax: (412) 268-7395
 *      innovation@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */


/* sf discipline to add sasl
 */

typedef struct _sasldisc
{
    Sfdisc_t	disc;
    sasl_conn_t *conn;
} Sasldisc_t;

ssize_t sasl_read(Sfio_t *f, Void_t *buf, size_t size, Sfdisc_t *disc)
{
    int len, result;
    const char *outbuf;
    int outlen;
    Sasldisc_t *sd = (Sasldisc_t *) disc;

    len = sfrd(f, buf, size, disc);

    if (len <= 0)
	return len;

    result = sasl_decode(sd->conn, buf, len, &outbuf, &outlen);

    if (result != SASL_OK) {
	/* eventually, we'll want an exception here */
	return -1;
    }

    if (outbuf != NULL) {
	memcpy(buf, outbuf, outlen);
    }

    return outlen;
}

ssize_t sasl_write(Sfio_t *f, const Void_t *buf, size_t size, Sfdisc_t *disc)
{
    int result;
    const char *outbuf;
    int outlen;
    Sasldisc_t *sd = (Sasldisc_t *) disc;

    result = sasl_encode(sd->conn, buf, size, &outbuf, &outlen);

    if (result != SASL_OK) {
	return -1;
    }

    if (outbuf != NULL) {
	sfwr(f, outbuf, outlen, disc);
    }

    return size;
}

int sfdcsasl(Sfio_t *f, sasl_conn_t *conn)
{
    Sasldisc_t *sasl;
    
    if (conn == NULL) {
	/* no need to do anything */
	return 0;
    }

    if(!(sasl = (Sasldisc_t*)malloc(sizeof(Sasldisc_t))) )
	return -1;
    
    sasl->disc.readf = sasl_read;
    sasl->disc.writef = sasl_write;
    sasl->disc.seekf = NULL;
    sasl->disc.exceptf = NULL;

    sasl->conn = conn;

    if (sfdisc(f, (Sfdisc_t *) sasl) != (Sfdisc_t *) sasl) {
	free(sasl);
	return -1;
    }
    
    return 0;
}


