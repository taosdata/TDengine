/* SASL server API implementation
 * Rob Siemborski
 * Tim Martin
 */
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

#include <config.h>

/* checkpw stuff */

#include <stdio.h>
#include "sasl.h"
#include "saslutil.h"
#include "saslplug.h"
#include "saslint.h"

#include <assert.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <fcntl.h>
#ifdef USE_DOORS
#include <sys/mman.h>
#include <door.h>
#endif

#include <stdlib.h>

#ifndef WIN32
#include <strings.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/un.h>
#else
#include <string.h>
#endif

#include <limits.h>
#include <sys/types.h>
#include <ctype.h>

#ifdef HAVE_PWD_H
#include <pwd.h>
#endif /* HAVE_PWD_H */
#ifdef HAVE_SHADOW_H
#include <shadow.h>
#endif /* HAVE_SHADOW_H */

#if defined(HAVE_PWCHECK) || defined(HAVE_SASLAUTHD) || defined(HAVE_AUTHDAEMON)
# include <errno.h>
# include <sys/types.h>
# include <sys/socket.h>
# include <sys/un.h>
# ifdef HAVE_UNISTD_H
#  include <unistd.h>
# endif
#endif


/* we store the following secret to check plaintext passwords:
 *
 * <salt> \0 <secret>
 *
 * where <secret> = MD5(<salt>, "sasldb", <pass>)
 */
static int _sasl_make_plain_secret(const char *salt, 
				   const char *passwd, size_t passlen,
				   sasl_secret_t **secret)
{
    MD5_CTX ctx;
    unsigned sec_len = 16 + 1 + 16; /* salt + "\0" + hash */

    *secret = (sasl_secret_t *) sasl_ALLOC(sizeof(sasl_secret_t) +
					   sec_len * sizeof(char));
    if (*secret == NULL) {
	return SASL_NOMEM;
    }

    _sasl_MD5Init(&ctx);
    _sasl_MD5Update(&ctx, (const unsigned char *) salt, 16);
    _sasl_MD5Update(&ctx, (const unsigned char *) "sasldb", 6);
    _sasl_MD5Update(&ctx, (const unsigned char *) passwd, (unsigned int) passlen);
    memcpy((*secret)->data, salt, 16);
    (*secret)->data[16] = '\0';
    _sasl_MD5Final((*secret)->data + 17, &ctx);
    (*secret)->len = sec_len;
    
    return SASL_OK;
}

/* verify user password using auxprop plugins
 */
static int auxprop_verify_password(sasl_conn_t *conn,
				   const char *userstr,
				   const char *passwd,
				   const char *service __attribute__((unused)),
				   const char *user_realm __attribute__((unused)))
{
    int ret = SASL_FAIL;
    int result = SASL_OK;
    sasl_server_conn_t *sconn = (sasl_server_conn_t *)conn;
    const char *password_request[] = { SASL_AUX_PASSWORD,
				       "*cmusaslsecretPLAIN",
				       NULL };
    struct propval auxprop_values[3];
    
    if (!conn || !userstr)
	return SASL_BADPARAM;

    /* We need to clear any previous results and re-canonify to 
     * ensure correctness */

    prop_clear (sconn->sparams->propctx, 0);
	
    /* ensure its requested */
    result = prop_request(sconn->sparams->propctx, password_request);

    if(result != SASL_OK) return result;

    result = _sasl_canon_user_lookup (conn,
				      userstr,
				      0,
				      SASL_CU_AUTHID | SASL_CU_AUTHZID,
				      &(conn->oparams));
    if(result != SASL_OK) return result;
    
    result = prop_getnames(sconn->sparams->propctx, password_request,
			   auxprop_values);
    if (result < 0) {
	return result;
    }

    /* Verify that the returned <name>s are correct.
       But we defer checking for NULL values till after we verify
       that a passwd is specified. */
    if (!auxprop_values[0].name && !auxprop_values[1].name) {
	return SASL_NOUSER;
    }
        
    /* It is possible for us to get useful information out of just
     * the lookup, so we won't check that we have a password until now */
    if(!passwd) {
	ret = SASL_BADPARAM;
	goto done;
    }

    if ((!auxprop_values[0].values || !auxprop_values[0].values[0])
	&& (!auxprop_values[1].values || !auxprop_values[1].values[0])) {
	return SASL_NOUSER;
    }
        
    /* At the point this has been called, the username has been canonified
     * and we've done the auxprop lookup.  This should be easy. */
    if(auxprop_values[0].name
       && auxprop_values[0].values
       && auxprop_values[0].values[0]
       && !strcmp(auxprop_values[0].values[0], passwd)) {
	/* We have a plaintext version and it matched! */
	return SASL_OK;
    } else if(auxprop_values[1].name
	      && auxprop_values[1].values
	      && auxprop_values[1].values[0]) {
	const char *db_secret = auxprop_values[1].values[0];
	sasl_secret_t *construct;
	
	ret = _sasl_make_plain_secret(db_secret, passwd,
				      strlen(passwd),
				      &construct);
	if (ret != SASL_OK) {
	    goto done;
	}

	if (!memcmp(db_secret, construct->data, construct->len)) {
	    /* password verified! */
	    ret = SASL_OK;
	} else {
	    /* passwords do not match */
	    ret = SASL_BADAUTH;
	}

	sasl_FREE(construct);
    } else {
	/* passwords do not match */
	ret = SASL_BADAUTH;
    }

    /* erase the plaintext password */
    sconn->sparams->utils->prop_erase(sconn->sparams->propctx,
				      password_request[0]);

 done:
    /* We're not going to erase the property here because other people
     * may want it */
    return ret;
}

#if 0
/* Verify user password using auxprop plugins. Allow verification against a hashed password,
 * or non-retrievable password. Don't use cmusaslsecretPLAIN attribute.
 *
 * This function is similar to auxprop_verify_password().
 */
static int auxprop_verify_password_hashed(sasl_conn_t *conn,
					  const char *userstr,
					  const char *passwd,
					  const char *service __attribute__((unused)),
					  const char *user_realm __attribute__((unused)))
{
    int ret = SASL_FAIL;
    int result = SASL_OK;
    sasl_server_conn_t *sconn = (sasl_server_conn_t *)conn;
    const char *password_request[] = { SASL_AUX_PASSWORD,
				       NULL };
    struct propval auxprop_values[2];
    unsigned extra_cu_flags = 0;

    if (!conn || !userstr)
	return SASL_BADPARAM;

    /* We need to clear any previous results and re-canonify to 
     * ensure correctness */

    prop_clear(sconn->sparams->propctx, 0);
	
    /* ensure its requested */
    result = prop_request(sconn->sparams->propctx, password_request);

    if (result != SASL_OK) return result;

    /* We need to pass "password" down to the auxprop_lookup */
    /* NB: We don't support binary passwords */
    if (passwd != NULL) {
	prop_set (sconn->sparams->propctx,
		  SASL_AUX_PASSWORD,
		  passwd,
		  -1);
	extra_cu_flags = SASL_CU_VERIFY_AGAINST_HASH;
    }

    result = _sasl_canon_user_lookup (conn,
				      userstr,
				      0,
				      SASL_CU_AUTHID | SASL_CU_AUTHZID | extra_cu_flags,
				      &(conn->oparams));

    if (result != SASL_OK) return result;
    
    result = prop_getnames(sconn->sparams->propctx, password_request,
			   auxprop_values);
    if (result < 0) {
	return result;
    }

    /* Verify that the returned <name>s are correct.
       But we defer checking for NULL values till after we verify
       that a passwd is specified. */
    if (!auxprop_values[0].name && !auxprop_values[1].name) {
	return SASL_NOUSER;
    }
        
    /* It is possible for us to get useful information out of just
     * the lookup, so we won't check that we have a password until now */
    if (!passwd) {
	ret = SASL_BADPARAM;
	goto done;
    }

    if ((!auxprop_values[0].values || !auxprop_values[0].values[0])) {
	return SASL_NOUSER;
    }

    /* At the point this has been called, the username has been canonified
     * and we've done the auxprop lookup.  This should be easy. */

    /* NB: Note that if auxprop_lookup failed to verify the password,
       then the userPassword property value would be NULL */
    if (auxprop_values[0].name
        && auxprop_values[0].values
        && auxprop_values[0].values[0]
        && !strcmp(auxprop_values[0].values[0], passwd)) {
	/* We have a plaintext version and it matched! */
	return SASL_OK;
    } else {
	/* passwords do not match */
	ret = SASL_BADAUTH;
    }

 done:
    /* We're not going to erase the property here because other people
     * may want it */
    return ret;
}
#endif

#ifdef DO_SASL_CHECKAPOP
int _sasl_auxprop_verify_apop(sasl_conn_t *conn,
			      const char *userstr,
			      const char *challenge,
			      const char *response,
			      const char *user_realm __attribute__((unused)))
{
    int ret = SASL_BADAUTH;
    char *userid = NULL;
    char *realm = NULL;
    unsigned char digest[16];
    char digeststr[33];
    const char *password_request[] = { SASL_AUX_PASSWORD, NULL };
    struct propval auxprop_values[2];
    sasl_server_conn_t *sconn = (sasl_server_conn_t *)conn;
    MD5_CTX ctx;
    int i;

    if (!conn || !userstr || !challenge || !response)
       PARAMERROR(conn)

    /* We've done the auxprop lookup already (in our caller) */
    /* sadly, APOP has no provision for storing secrets */
    ret = prop_getnames(sconn->sparams->propctx, password_request,
			auxprop_values);
    if(ret < 0) {
	sasl_seterror(conn, 0, "could not perform password lookup");
	goto done;
    }
    
    if(!auxprop_values[0].name ||
       !auxprop_values[0].values ||
       !auxprop_values[0].values[0]) {
	sasl_seterror(conn, 0, "could not find password");
	ret = SASL_NOUSER;
	goto done;
    }
    
    _sasl_MD5Init(&ctx);
    _sasl_MD5Update(&ctx, (const unsigned char *) challenge, strlen(challenge));
    _sasl_MD5Update(&ctx, (const unsigned char *) auxprop_values[0].values[0],
		    strlen(auxprop_values[0].values[0]));
    _sasl_MD5Final(digest, &ctx);

    /* erase the plaintext password */
    sconn->sparams->utils->prop_erase(sconn->sparams->propctx,
				      password_request[0]);

    /* convert digest from binary to ASCII hex */
    for (i = 0; i < 16; i++)
      sprintf(digeststr + (i*2), "%02x", digest[i]);

    if (!strncasecmp(digeststr, response, 32)) {
      /* password verified! */
      ret = SASL_OK;
    } else {
      /* passwords do not match */
      ret = SASL_BADAUTH;
    }

 done:
    if (ret == SASL_BADAUTH) sasl_seterror(conn, SASL_NOLOG,
					   "login incorrect");
    if (userid) sasl_FREE(userid);
    if (realm)  sasl_FREE(realm);

    return ret;
}
#endif /* DO_SASL_CHECKAPOP */

#if defined(HAVE_PWCHECK) || defined(HAVE_SASLAUTHD) || defined(HAVE_AUTHDAEMON)
/*
 * Wait for file descriptor to be writable. Return with error if timeout.
 */
static int write_wait(int fd, unsigned delta)
{
    fd_set wfds;
    fd_set efds;
    struct timeval tv;

    /* 
     * Wait for file descriptor fd to be writable. Retry on
     * interruptions. Return with error upon timeout.
     */
    while (1) {
	FD_ZERO(&wfds);
	FD_ZERO(&efds);
	FD_SET(fd, &wfds);
	FD_SET(fd, &efds);
	tv.tv_sec = (long) delta;
	tv.tv_usec = 0;
	switch(select(fd + 1, 0, &wfds, &efds, &tv)) {
	case 0:
	    /* Timeout. */
	    errno = ETIMEDOUT;
	    return -1;
	case +1:
	    if (FD_ISSET(fd, &wfds)) {
		/* Success, file descriptor is writable. */
		return 0;
	    }
	    return -1;
	case -1:
	    if (errno == EINTR || errno == EAGAIN)
		continue;
            return -1;
	default:
	    /* Error catch-all. */
	    return -1;
	}
    }
    /* Not reached. */
    return -1;
}

/*
 * Keep calling the writev() system call with 'fd', 'iov', and 'iovcnt'
 * until all the data is written out or an error/timeout occurs.
 */
static int retry_writev(int fd, struct iovec *iov, int iovcnt, unsigned delta)
{
    int n;
    int i;
    int written = 0;
    static int iov_max =
#ifdef MAXIOV
	MAXIOV
#else
#ifdef IOV_MAX
	IOV_MAX
#else
	8192
#endif
#endif
	;
    
    for (;;) {
	while (iovcnt && iov[0].iov_len == 0) {
	    iov++;
	    iovcnt--;
	}

	if (!iovcnt) return written;

	if (delta > 0) {
	    if (write_wait(fd, delta))
		return -1;
	}
	n = writev(fd, iov, iovcnt > iov_max ? iov_max : iovcnt);
	if (n == -1) {
	    if (errno == EINVAL && iov_max > 10) {
		iov_max /= 2;
		continue;
	    }
	    if (errno == EINTR) continue;
	    return -1;
	}

	written += n;

	for (i = 0; i < iovcnt; i++) {
	    if ((int) iov[i].iov_len > n) {
		iov[i].iov_base = (char *)iov[i].iov_base + n;
		iov[i].iov_len -= n;
		break;
	    }
	    n -= iov[i].iov_len;
	    iov[i].iov_len = 0;
	}

	if (i == iovcnt) return written;
    }
}

#endif

#ifdef HAVE_PWCHECK
/* pwcheck daemon-authenticated login */
static int pwcheck_verify_password(sasl_conn_t *conn,
				   const char *userid, 
				   const char *passwd,
				   const char *service __attribute__((unused)),
				   const char *user_realm 
				               __attribute__((unused)))
{
    int s;
    struct sockaddr_un srvaddr;
    int r;
    struct iovec iov[10];
    static char response[1024];
    unsigned start, n;
    char pwpath[1024];

    if (strlen(PWCHECKDIR)+8+1 > sizeof(pwpath)) return SASL_FAIL;

    strcpy(pwpath, PWCHECKDIR);
    strcat(pwpath, "/pwcheck");

    s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s == -1) return errno;

    memset((char *)&srvaddr, 0, sizeof(srvaddr));
    srvaddr.sun_family = AF_UNIX;
    strncpy(srvaddr.sun_path, pwpath, sizeof(srvaddr.sun_path));
    r = connect(s, (struct sockaddr *)&srvaddr, sizeof(srvaddr));
    if (r == -1) {
	sasl_seterror(conn,0,"cannot connect to pwcheck server");
	return SASL_FAIL;
    }

    iov[0].iov_base = (char *)userid;
    iov[0].iov_len = strlen(userid)+1;
    iov[1].iov_base = (char *)passwd;
    iov[1].iov_len = strlen(passwd)+1;

    retry_writev(s, iov, 2, 0);

    start = 0;
    while (start < sizeof(response) - 1) {
	n = read(s, response+start, sizeof(response) - 1 - start);
	if (n < 1) break;
	start += n;
    }

    close(s);

    if (start > 1 && !strncmp(response, "OK", 2)) {
	return SASL_OK;
    }

    response[start] = '\0';
    sasl_seterror(conn,0,response);
    return SASL_BADAUTH;
}

#endif

#if defined(HAVE_SASLAUTHD) || defined(HAVE_AUTHDAEMON)
static int read_wait(int fd, unsigned delta)
{
    fd_set rfds;
    fd_set efds;
    struct timeval tv;
    /* 
     * Wait for file descriptor fd to be readable. Retry on 
     * interruptions. Return with error upon timeout.
     */
    while (1) {
	FD_ZERO(&rfds);
	FD_ZERO(&efds);
	FD_SET(fd, &rfds);
	FD_SET(fd, &efds);
	tv.tv_sec = (long) delta;
	tv.tv_usec = 0;
	switch(select(fd + 1, &rfds, 0, &efds, &tv)) {
	case 0:
	    /* Timeout. */
	    errno = ETIMEDOUT;
	    return -1;
	case +1:
	case +2:
	    if (FD_ISSET(fd, &rfds)) {
		/* Success, file descriptor is readable. */
		return 0;
	    }
	    return -1;
	case -1:
	    if (errno == EINTR || errno == EAGAIN)
		continue;
            return -1;
	default:
	    /* Error catch-all. */
	    return -1;
	}
    }
    /* Not reached. */
    return -1;
}

/*
 * Keep calling the read() system call until all the data is read in, 
 * timeout, EOF, or an error occurs. This function returns the number 
 * of useful bytes, or -1 if timeout/error.
 */
static int retry_read(int fd, void *buf0, unsigned nbyte, unsigned delta)
{
    int nr;
    unsigned nleft = nbyte;
    char *buf = (char*) buf0;
    
    while (nleft >= 1) {
	if (delta > 0) {
	    if (read_wait(fd, delta))
		return -1;
	}
	nr = read(fd, buf, nleft);
	if (nr < 0) {
	    if (errno == EINTR || errno == EAGAIN)
		continue;
	    return -1;
	} else if (nr == 0) {
	    break;
	}
      buf += nr;
      nleft -= nr;
    }
    return nbyte - nleft;
}
#endif

#ifdef HAVE_SASLAUTHD
/* saslauthd-authenticated login */
static int saslauthd_verify_password(sasl_conn_t *conn,
				     const char *userid, 
				     const char *passwd,
				     const char *service,
				     const char *user_realm)
{
    char response[1024];
    char query[8192];
    char *query_end = query;
    int s;
    struct sockaddr_un srvaddr;
    sasl_getopt_t *getopt;
    void *context;
    char pwpath[sizeof(srvaddr.sun_path)];
    const char *p = NULL;
    char *freeme = NULL;
#ifdef USE_DOORS
    door_arg_t arg;
#endif

    /* check to see if the user configured a rundir */
    if (_sasl_getcallback(conn, SASL_CB_GETOPT,
                          (sasl_callback_ft *)&getopt, &context) == SASL_OK) {
	getopt(context, NULL, "saslauthd_path", &p, NULL);
    }
    if (p) {
        if (strlen(p) >= sizeof(pwpath))
            return SASL_FAIL;

	strncpy(pwpath, p, sizeof(pwpath) - 1);
        pwpath[strlen(p)] = '\0';
    } else {
	if (strlen(PATH_SASLAUTHD_RUNDIR) + 4 + 1 > sizeof(pwpath))
	    return SASL_FAIL;

	strcpy(pwpath, PATH_SASLAUTHD_RUNDIR "/mux");
    }

    /* Split out username/realm if necessary */
    if(strrchr(userid,'@') != NULL) {
	char *rtmp;
	
	if(_sasl_strdup(userid, &freeme, NULL) != SASL_OK)
	    goto fail;

	userid = freeme;
	rtmp = strrchr(userid,'@');
	*rtmp = '\0';
	user_realm = rtmp + 1;
    }

    /*
     * build request of the form:
     *
     * count authid count password count service count realm
     */
    {
 	unsigned short max_len, req_len, u_len, p_len, s_len, r_len;
 
	max_len = (unsigned short) sizeof(query);

	/* prevent buffer overflow */
	if ((strlen(userid) > USHRT_MAX) ||
	    (strlen(passwd) > USHRT_MAX) ||
	    (strlen(service) > USHRT_MAX) ||
	    (user_realm && (strlen(user_realm) > USHRT_MAX))) {
	    goto toobig;
	}

 	u_len = (strlen(userid));
 	p_len = (strlen(passwd));
	s_len = (strlen(service));
	r_len = ((user_realm ? strlen(user_realm) : 0));

	/* prevent buffer overflow */
	req_len = 30;
	if (max_len - req_len < u_len) goto toobig;
	req_len += u_len;
	if (max_len - req_len < p_len) goto toobig;
	req_len += p_len;
	if (max_len - req_len < s_len) goto toobig;
	req_len += s_len;
	if (max_len - req_len < r_len) goto toobig;

	u_len = htons(u_len);
	p_len = htons(p_len);
	s_len = htons(s_len);
	r_len = htons(r_len);

	memcpy(query_end, &u_len, sizeof(unsigned short));
	query_end += sizeof(unsigned short);
	while (*userid) *query_end++ = *userid++;

	memcpy(query_end, &p_len, sizeof(unsigned short));
	query_end += sizeof(unsigned short);
	while (*passwd) *query_end++ = *passwd++;

	memcpy(query_end, &s_len, sizeof(unsigned short));
	query_end += sizeof(unsigned short);
	while (*service) *query_end++ = *service++;

	memcpy(query_end, &r_len, sizeof(unsigned short));
	query_end += sizeof(unsigned short);
	if (user_realm) while (*user_realm) *query_end++ = *user_realm++;
    }

#ifdef USE_DOORS
    s = open(pwpath, O_RDONLY);
    if (s < 0) {
	sasl_seterror(conn, 0, "cannot open door to saslauthd server: %m", errno);
	goto fail;
    }

    arg.data_ptr = query;
    arg.data_size = query_end - query;
    arg.desc_ptr = NULL;
    arg.desc_num = 0;
    arg.rbuf = response;
    arg.rsize = sizeof(response);

    if (door_call(s, &arg) < 0) {
      /* Parameters are undefined */
      close(s);
      sasl_seterror(conn, 0, "door call to saslauthd server failed: %m", errno);
      goto fail;
    }

    if (arg.data_ptr != response || arg.data_size >= sizeof(response)) {
	/* oh damn, we got back a really long response */
	munmap(arg.rbuf, arg.rsize);
	close(s);
	sasl_seterror(conn, 0, "saslauthd sent an overly long response");
	goto fail;
    }
    response[arg.data_size] = '\0';

    close(s);
#else
    /* unix sockets */

    s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s == -1) {
	sasl_seterror(conn, 0, "cannot create socket for saslauthd: %m", errno);
	goto fail;
    }

    memset((char *)&srvaddr, 0, sizeof(srvaddr));
    srvaddr.sun_family = AF_UNIX;
    strncpy(srvaddr.sun_path, pwpath, sizeof(srvaddr.sun_path) - 1);
    srvaddr.sun_path[strlen(pwpath)] = '\0';

    {
	int r = connect(s, (struct sockaddr *) &srvaddr, sizeof(srvaddr));
	if (r == -1) {
	    close(s);
	    sasl_seterror(conn, 0, "cannot connect to saslauthd server: %m", errno);
	    goto fail;
	}
    }

    {
 	struct iovec iov[8];
 
	iov[0].iov_len = query_end - query;
	iov[0].iov_base = query;

	if (retry_writev(s, iov, 1, 0) == -1) {
	    close(s);
            sasl_seterror(conn, 0, "write failed");
	    goto fail;
  	}
    }

    {
	unsigned short count = 0;

	/*
	 * read response of the form:
	 *
	 * count result
	 */
	if (retry_read(s, &count, sizeof(count), 0) < (int) sizeof(count)) {
	    sasl_seterror(conn, 0, "size read failed");
	    goto fail;
	}
	
	count = ntohs(count);
	if (count < 2) { /* MUST have at least "OK" or "NO" */
	    close(s);
	    sasl_seterror(conn, 0, "bad response from saslauthd");
	    goto fail;
	}
	
	count = (int)sizeof(response) <= count ? sizeof(response) - 1 : count;
	if (retry_read(s, response, count, 0) < count) {
	    close(s);
	    sasl_seterror(conn, 0, "read failed");
	    goto fail;
	}
	response[count] = '\0';
    }

    close(s);
#endif /* USE_DOORS */
  
    if(freeme) free(freeme);

    if (!strncmp(response, "OK", 2)) {
	return SASL_OK;
    }
  
    sasl_seterror(conn, SASL_NOLOG, "authentication failed");
    return SASL_BADAUTH;

 toobig:
    /* request just too damn big */
    sasl_seterror(conn, 0, "saslauthd request too large");

 fail:
    if (freeme) free(freeme);
    return SASL_FAIL;
}

#endif

#ifdef HAVE_AUTHDAEMON
/* 
 * Preliminary support for Courier's authdaemond.
 */
#define AUTHDAEMON_IO_TIMEOUT 30

static int authdaemon_blocking(int fd, int block)
{
    int f, r;

    /* Get the fd's blocking bit. */
    f = fcntl(fd, F_GETFL, 0);
    if (f == -1)
	return -1;

    /* Adjust the bitmap accordingly. */
#ifndef O_NONBLOCK
#define NB_BITMASK FNDELAY
#else
#define NB_BITMASK O_NONBLOCK
#endif
    if (block)
	f &= ~NB_BITMASK;
    else
	f |=  NB_BITMASK;
#undef NB_BITMASK

    /* Adjust the fd's blocking bit. */
    r = fcntl(fd, F_SETFL, f);
    if (r)
	return -1;

    /* Success. */
    return 0;
}

static int authdaemon_connect(sasl_conn_t *conn, const char *path)
{
    int r, s = -1;
    struct sockaddr_un srvaddr;

    if (strlen(path) >= sizeof(srvaddr.sun_path)) {
	sasl_seterror(conn, 0, "unix socket path too large", errno);
	goto fail;
    }

    s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s == -1) {
	sasl_seterror(conn, 0, "cannot create socket for connection to Courier authdaemond: %m", errno);
	goto fail;
    }

    memset((char *)&srvaddr, 0, sizeof(srvaddr));
    srvaddr.sun_family = AF_UNIX;
    strncpy(srvaddr.sun_path, path, sizeof(srvaddr.sun_path) - 1);

    /* Use nonblocking unix socket connect(2). */
    if (authdaemon_blocking(s, 0)) {
	sasl_seterror(conn, 0, "cannot set nonblocking bit: %m", errno);
	goto fail;
    }

    r = connect(s, (struct sockaddr *) &srvaddr, sizeof(srvaddr));
    if (r == -1) {
	sasl_seterror(conn, 0, "cannot connect to Courier authdaemond: %m", errno);
	goto fail;
    }

    if (authdaemon_blocking(s, 1)) {
	sasl_seterror(conn, 0, "cannot clear nonblocking bit: %m", errno);
	goto fail;
    }

    return s;
fail:
    if (s >= 0)
	close(s);
    return -1;
}

static char *authdaemon_build_query(const char *service,
				    const char *authtype,
				    const char *user,
				    const char *passwd)
{
    int sz;
    int l = strlen(service) 
            + 1
            + strlen(authtype) 
            + 1
            + strlen(user)
            + 1
            + strlen(passwd) 
            + 1;
    char *buf, n[5];
    if (snprintf(n, sizeof(n), "%d", l) >= (int)sizeof(n))
	return NULL;
    sz = strlen(n) + l + 20;
    if (!(buf = sasl_ALLOC(sz)))
	return NULL;
    snprintf(buf, 
             sz, 
             "AUTH %s\n%s\n%s\n%s\n%s\n\n",
             n,
             service,
             authtype,
             user,
             passwd);
    return buf;
}

static int authdaemon_read(int fd, void *buf0, unsigned sz)
{
    int nr;
    char *buf = (char*) buf0;
    if (sz <= 1)
	return -1;
    if ((nr = retry_read(fd, buf0, sz - 1, AUTHDAEMON_IO_TIMEOUT)) < 0)
	return -1;
    /* We need a null-terminated buffer. */
    buf[nr] = 0;
    /* Check for overflow condition. */
    return nr + 1 < (int)sz ? 0 : -1;
}

static int authdaemon_write(int fd, void *buf0, unsigned sz)
{
    int nw;
    struct iovec io;
    io.iov_len = sz;
    io.iov_base = buf0;
    nw = retry_writev(fd, &io, 1, AUTHDAEMON_IO_TIMEOUT);
    return nw == (int)sz ? 0 : -1;
}

static int authdaemon_talk(sasl_conn_t *conn, int sock, char *authreq)
{
    char *str;
    char buf[8192];

    if (authdaemon_write(sock, authreq, strlen(authreq)))
	goto _err_out;
    if (authdaemon_read(sock, buf, sizeof(buf)))
	goto _err_out;
    for (str = buf; *str; ) {
	char *sub;

	for (sub = str; *str; ++str) {
	    if (*str == '\n') {
		*str++ = 0;
		break;
	    }
	}
	if (strcmp(sub, ".") == 0) {
	    /* success */
	    return SASL_OK;
	}
	if (strcmp(sub, "FAIL") == 0) {
	    /* passwords do not match */
	    sasl_seterror(conn, SASL_NOLOG, "authentication failed");
	    return SASL_BADAUTH;
	}
    }
_err_out:
    /* catchall: authentication error */
    sasl_seterror(conn, 0, "could not verify password");
    return SASL_FAIL;
}

static int authdaemon_verify_password(sasl_conn_t *conn,
				      const char *userid, 
				      const char *passwd,
				      const char *service,
				      const char *user_realm __attribute__((unused)))
{
    const char *p = NULL;
    sasl_getopt_t *getopt;
    void *context;
    int result = SASL_FAIL;
    char *query = NULL;
    int sock = -1;

    /* check to see if the user configured a rundir */
    if (_sasl_getcallback(conn, SASL_CB_GETOPT,
                          (sasl_callback_ft *)&getopt, &context) == SASL_OK) {
	getopt(context, NULL, "authdaemond_path", &p, NULL);
    }
    if (!p) {
	/*
	 * XXX should we peek at Courier's build-time config ?
	 */
	p = PATH_AUTHDAEMON_SOCKET;
    }

    if ((sock = authdaemon_connect(conn, p)) < 0)
	goto out;
    if (!(query = authdaemon_build_query(service, "login", userid, passwd)))
	goto out;
    result = authdaemon_talk(conn, sock, query);
out:
    if (sock >= 0)
	close(sock), sock = -1;
    if (query)
	sasl_FREE(query), query = 0;
    return result;
}
#endif

#ifdef HAVE_ALWAYSTRUE
static int always_true(sasl_conn_t *conn,
		       const char *userstr,
		       const char *passwd __attribute__((unused)),
		       const char *service __attribute__((unused)),
		       const char *user_realm __attribute__((unused))) 
{
    _sasl_log(conn, SASL_LOG_WARN, "AlwaysTrue Password Verifier Verified: %s",
	      userstr);
    return SASL_OK;
}
#endif

struct sasl_verify_password_s _sasl_verify_password[] = {
    { "auxprop", &auxprop_verify_password },
#if 0	/* totally undocumented. wtf is this? */
    { "auxprop-hashed", &auxprop_verify_password_hashed },
#endif
#ifdef HAVE_PWCHECK
    { "pwcheck", &pwcheck_verify_password },
#endif
#ifdef HAVE_SASLAUTHD
    { "saslauthd", &saslauthd_verify_password },
#endif
#ifdef HAVE_AUTHDAEMON
    { "authdaemond", &authdaemon_verify_password },
#endif
#ifdef HAVE_ALWAYSTRUE
    { "alwaystrue", &always_true },
#endif
    { NULL, NULL }
};
