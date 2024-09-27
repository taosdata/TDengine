/* pwcheck.c -- Unix pwcheck daemon
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

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef HAVE_PATHS_H
#include <paths.h>
#endif
#include <syslog.h>

#if !defined(_PATH_PWCHECKPID)
#ifdef _PATH_VARRUN
# define _PATH_PWCHECKPID (_PATH_VARRUN "pwcheck.pid")
#else
# define _PATH_PWCHECKPID (NULL)
#endif
#endif

void newclient(int);
int retry_write(int, const char *, unsigned int);

/*
 * Unix pwcheck daemon-authenticated login (shadow password)
 */

int
main()
{
    char fnamebuf[MAXPATHLEN];
    int s;
    int c;
    int count;
    int rc;
    struct sockaddr_un srvaddr;
    struct sockaddr_un clientaddr;
    int r;
    int len;
    mode_t oldumask;
    char *pid_file = _PATH_PWCHECKPID;
    FILE *fp = NULL;
    pid_t pid;

    openlog("pwcheck", LOG_NDELAY, LOG_AUTH);

    /* Daemonize. */
    count = 5;
    while (count--) {
	pid = fork();
            
	if (pid > 0)
	    _exit(0);               /* parent dies */
            
	if ((pid == -1) && (errno == EAGAIN)) {
	    syslog(LOG_WARNING, "master fork failed (sleeping): %m");
	    sleep(5);
	    continue;
	}
    }
    if (pid == -1) {
	rc = errno;
	syslog(LOG_ERR, "FATAL: master fork failed: %m");
	fprintf(stderr, "pwcheck: ");
	errno = rc;
	perror("fork");
	exit(1);
    }

    /*
     * We're now running in the child. Lose our controlling terminal
     * and obtain a new process group.
     */
    if (setsid() == -1) {
	rc = errno;
	syslog(LOG_ERR, "FATAL: setsid: %m");
	fprintf(stderr, "pwcheck: ");
	errno = rc;
	perror("setsid");
	exit(1);
    }
        
    s = open("/dev/null", O_RDWR, 0);
    if (s == -1) {
	rc = errno;
	syslog(LOG_ERR, "FATAL: /dev/null: %m");
	fprintf(stderr, "pwcheck: ");
	errno = rc;
	perror("/dev/null");
	exit(1);
            
    }
    dup2(s, fileno(stdin));
    dup2(s, fileno(stdout));
    dup2(s, fileno(stderr));
    if (s > 2) {
	close(s);
    }

    /*
     *   Record process ID - shamelessly stolen from inetd (I.V.)
     */
    pid = getpid();
    if (pid_file) {
	fp = fopen(pid_file, "w");
    }
    if (fp) {
        fprintf(fp, "%ld\n", (long)pid);
        fclose(fp);
    } else if (pid_file) {
        syslog(LOG_WARNING, "%s: %m", pid_file);
    }

    s = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s == -1) {
	perror("socket");
	exit(1);
    }

    strncpy(fnamebuf, PWCHECKDIR, sizeof(fnamebuf));
    strncpy(fnamebuf + sizeof(PWCHECKDIR)-1, "/pwcheck",
	    sizeof(fnamebuf) - sizeof(PWCHECKDIR));
    fnamebuf[MAXPATHLEN-1] = '\0';

    (void) unlink(fnamebuf);

    memset((char *)&srvaddr, 0, sizeof(srvaddr));
    srvaddr.sun_family = AF_UNIX;
    strncpy(srvaddr.sun_path, fnamebuf, sizeof(srvaddr.sun_path));
    /* Most systems make sockets 0777 no matter what you ask for.
       Known exceptions are Linux and DUX. */
    oldumask = umask((mode_t) 0); /* for Linux, which observes the umask when
			    setting up the socket */
    r = bind(s, (struct sockaddr *)&srvaddr, sizeof(srvaddr));
    if (r == -1) {
	syslog(LOG_ERR, "%.*s: %m",
	       sizeof(srvaddr.sun_path), srvaddr.sun_path);
	exit(1);
    }
    umask(oldumask); /* for Linux */
    chmod(fnamebuf, (mode_t) 0777); /* for DUX, where this isn't the default.
				    (harmlessly fails on some systems) */	
    r = listen(s, 5);
    if (r == -1) {
	syslog(LOG_ERR, "listen: %m");
	exit(1);
    }

    for (;;) {
	len = sizeof(clientaddr);
	c = accept(s, (struct sockaddr *)&clientaddr, &len);
	if (c == -1 && errno != EINTR) {
	    syslog(LOG_WARNING, "accept: %m");
	    continue;
	}

	newclient(c);
    }
}

void newclient(int c)
{
    char request[1024];
    int n;
    unsigned int start;
    char *reply;
    extern char *pwcheck();
    
    start = 0;
    while (start < sizeof(request) - 1) {
	n = read(c, request+start, sizeof(request) - 1 - start);
	if (n < 1) {
	    reply = "Error reading request";
	    goto sendreply;
	}
		
	start += n;

	if (request[start-1] == '\0' && strlen(request) < start) {
	    break;
	}
    }

    if (start >= sizeof(request) - 1) {
	reply = "Request too big";
    }
    else {
	reply = pwcheck(request, request + strlen(request) + 1);
    }

sendreply:

    retry_write(c, reply, strlen(reply));
    close(c);
}
  
/*
 * Keep calling the write() system call with 'fd', 'buf', and 'nbyte'
 * until all the data is written out or an error occurs.
 */
int retry_write(int fd, const char *buf, unsigned int nbyte)
{
    int n;
    int written = 0;

    if (nbyte == 0)
	return 0;

    for (;;) {
        n = write(fd, buf, nbyte);
        if (n == -1) {
            if (errno == EINTR)
		continue;
            return -1;
        }

        written += n;

        if ((unsigned int) n >= nbyte)
	    return written;

        buf += n;
        nbyte -= n;
    }
}
