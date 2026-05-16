/* test-saslauthd.c: saslauthd test utility
 * Rob Siemborski
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
#include <stdio.h>

#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef USE_DOORS
#include <door.h>
#endif
#include <assert.h>

#include "globals.h"
#include "utils.h"

/* make utils.c happy */
int flags = LOG_USE_STDERR;

/*
 * Keep calling the read() system call with 'fd', 'buf', and 'nbyte'
 * until all the data is read in or an error occurs.
 */
int retry_read(int fd, void *inbuf, unsigned nbyte)
{
    int n;
    int nread = 0;
    char *buf = (char *)inbuf;

    if (nbyte == 0) return 0;

    for (;;) {
	n = read(fd, buf, nbyte);
	if (n == -1 || n == 0) {
	    if (errno == EINTR || errno == EAGAIN) continue;
	    return -1;
	}

	nread += n;

	if (n >= (int) nbyte) return nread;

	buf += n;
	nbyte -= n;
    }
}

/* saslauthd-authenticated login */
static int saslauthd_verify_password(const char *saslauthd_path,
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
    int r;
    unsigned short count;
    char pwpath[sizeof(srvaddr.sun_path)];
#ifdef USE_DOORS
    door_arg_t arg;
#endif

    if(!service) service = "imap";
    if(!user_realm) user_realm = "";
    if(!userid || !passwd) return -1;
    
    if (saslauthd_path) {
	strlcpy(pwpath, saslauthd_path, sizeof(pwpath));
    } else {
	if (strlen(PATH_SASLAUTHD_RUNDIR) + 4 + 1 > sizeof(pwpath))
	    return -1;

	strcpy(pwpath, PATH_SASLAUTHD_RUNDIR);
	strcat(pwpath, "/mux");
    }

    /*
     * build request of the form:
     *
     * count authid count password count service count realm
     */
    {
 	unsigned short u_len, p_len, s_len, r_len;
 
 	u_len = htons(strlen(userid));
 	p_len = htons(strlen(passwd));
	s_len = htons(strlen(service));
	r_len = htons((user_realm ? strlen(user_realm) : 0));

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
	perror("open");
	return -1;
    }

    arg.data_ptr = query;
    arg.data_size = query_end - query;
    arg.desc_ptr = NULL;
    arg.desc_num = 0;
    arg.rbuf = response;
    arg.rsize = sizeof(response);

    if(door_call(s, &arg) != 0) {
	printf("NO \"door_call failed\"\n");
	return -1;	
    }

    assert(arg.data_size < sizeof(response));
    response[arg.data_size] = '\0';

    close(s);
#else
    s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s == -1) {
	perror("socket() ");
	return -1;
    }

    memset((char *)&srvaddr, 0, sizeof(srvaddr));
    srvaddr.sun_family = AF_UNIX;
    strlcpy(srvaddr.sun_path, pwpath, sizeof(srvaddr.sun_path));

    r = connect(s, (struct sockaddr *) &srvaddr, sizeof(srvaddr));
    if (r == -1) {
        close(s);
        perror("connect() ");
	return -1;
    }

    {
 	struct iovec iov[8];
 
	iov[0].iov_len = query_end - query;
	iov[0].iov_base = query;

	if (retry_writev(s, iov, 1) == -1) {
	    close(s);
	    fprintf(stderr,"write failed\n");
	    return -1;
	}
    }
  
    /*
     * read response of the form:
     *
     * count result
     */
    if (retry_read(s, &count, sizeof(count)) < (int) sizeof(count)) {
        close(s);
        fprintf(stderr,"size read failed\n");
        return -1;
    }
  
    count = ntohs(count);
    if (count < 2) { /* MUST have at least "OK" or "NO" */
	close(s);
        fprintf(stderr,"bad response from saslauthd\n");
	return -1;
    }
  
    count = (int)sizeof(response) < count ? sizeof(response) : count;
    if (retry_read(s, response, count) < count) {
	close(s);
        fprintf(stderr,"read failed\n");
	return -1;
    }
    response[count] = '\0';
  
    close(s);
#endif /* USE_DOORS */
  
    if (!strncmp(response, "OK", 2)) {
	printf("OK \"Success.\"\n");
	return 0;
    }
  
    printf("NO \"authentication failed\"\n");
    return -1;
}

int
main(int argc, char *argv[])
{
  const char *user = NULL, *password = NULL;
  const char *realm = NULL, *service = NULL, *path = NULL;
  int c;
  int flag_error = 0;
  int result = 0;
  int repeat = 0;

  while ((c = getopt(argc, argv, "p:u:r:s:f:R:")) != EOF)
      switch (c) {
      case 'R':
	  repeat = atoi(optarg);
	  break;
      case 'f':
	  path = optarg;
	  break;
      case 's':
	  service = optarg;
	  break;
      case 'r':
	  realm = optarg;
	  break;
      case 'u':
	  user = optarg;
	  break;
      case 'p':
	  password = optarg;
	  break;
      default:
	  flag_error = 1;
	  break;
    }

  if (!user || !password)
    flag_error = 1;

  if (flag_error) {
    (void)fprintf(stderr,
		  "%s: usage: %s -u username -p password\n"
		  "              [-r realm] [-s servicename]\n"
		  "              [-f socket path] [-R repeatnum]\n",
		  argv[0], argv[0]);
    exit(1);
  }

  if (!repeat) repeat = 1;
  for (c = 0; c < repeat; c++) {
      /* saslauthd-authenticated login */
      printf("%d: ", c);
      result = saslauthd_verify_password(path, user, password, service, realm);
  }
  return result;
}


