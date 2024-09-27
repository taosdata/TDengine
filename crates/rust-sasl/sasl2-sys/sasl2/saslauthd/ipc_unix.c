/*******************************************************************************
 *
 * ipc_unix.c
 *
 * Description:  Implements the AF_UNIX IPC method.
 *
 * Copyright (c) 1997-2000 Messaging Direct Ltd.
 * All rights reserved.
 *
 * Portions Copyright (c) 2003 Jeremy Rumpf
 * jrumpf@heavyload.net
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY MESSAGING DIRECT LTD. ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL MESSAGING DIRECT LTD. OR
 * ITS EMPLOYEES OR AGENTS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 *
 *
 * HISTORY
 *
 * 
 * This source file created using 8 space tabs.
 *
 ********************************************************************************/

/****************************************
 * enable/disable ifdef
*****************************************/
#include "saslauthd-main.h"

#ifdef USE_UNIX_IPC
/****************************************/

/****************************************
 * includes
*****************************************/
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <errno.h>
#include <netinet/in.h>

#include "globals.h"
#include "utils.h"

/****************************************
 * declarations/protos
 *****************************************/
static void	do_request(int);
static void	send_no(int, char *);
static int	rel_accept_lock();
static int	get_accept_lock();

/****************************************
 * module globals
 *****************************************/
static int			sock_fd;     /* descriptor for the socket          */
static int			accept_fd;   /* descriptor for the accept lock     */
static struct sockaddr_un	server;      /* domain socket control, server side */
static struct sockaddr_un	client;      /* domain socket control, client side */
static SALEN_TYPE		len;         /* length for the client sockaddr_un  */
static char			*sock_file;  /* path to the AF_UNIX socket         */
static char			*accept_file;/* path to the accept() lock file     */

/****************************************
 * flags       	global from saslauthd-main.c
 * run_path    	global from saslauthd-main.c
 * num_procs   	global from saslauthd-main.c
 * detach_tty()	function from saslauthd-main.c
 * rx_rec()		function from utils.c
 * tx_rec()		function from utils.c
 * logger()		function from utils.c
 *****************************************/


/*************************************************************
 * IPC init. Initialize the environment specific to the 
 * AF_UNIX IPC method.
 *
 * __Required Function__
 **************************************************************/
void ipc_init() {
	int	rc;
	size_t  sock_file_len;
	
        /*********************************************************
	 * When we're not preforking, using an accept lock is a
	 * waste of resources. Otherwise, setup the accept lock
	 * file.
	 **********************************************************/
	if (num_procs == 0) 
		flags &= ~USE_ACCEPT_LOCK;
	
	if (flags & USE_ACCEPT_LOCK) {
		size_t accept_file_len;

		accept_file_len = strlen(run_path) + sizeof(ACCEPT_LOCK_FILE) + 1;
		if ((accept_file = malloc(accept_file_len)) == NULL) {
			logger(L_ERR, L_FUNC, "could not allocate memory");
			exit(1);
		}

		strlcpy(accept_file, run_path, accept_file_len);
		strlcat(accept_file, ACCEPT_LOCK_FILE, accept_file_len);

		if ((accept_fd = open(accept_file, O_RDWR|O_CREAT|O_TRUNC, S_IWUSR|S_IRUSR)) == -1) {
			rc = errno;
			logger(L_ERR, L_FUNC, "could not open accept lock file: %s", accept_file);
                	logger(L_ERR, L_FUNC, "open: %s", strerror(rc));
			exit(1);
		}

		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, "using accept lock file: %s", accept_file);
	}

	/**************************************************************
	 * We're at the point where we can't really do anything else
	 * until we attempt to detach or daemonize.
	 **************************************************************/
	detach_tty();

	/**************************************************************
	 * Setup the UNIX domain socket
	 **************************************************************/
	sock_file_len = strlen(run_path) + sizeof(SOCKET_FILE) + 1;
	if ((sock_file = malloc(sock_file_len)) == NULL) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		exit(1);
	}

	strlcpy(sock_file, run_path, sock_file_len);
	strlcat(sock_file, SOCKET_FILE, sock_file_len);

	unlink(sock_file);
	memset(&server, 0, sizeof(server));
	strlcpy(server.sun_path, sock_file, sizeof(server.sun_path));	
	server.sun_family = AF_UNIX;
	
	if ((sock_fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not create socket");
		logger(L_ERR, L_FUNC, "socket: %s", strerror(rc));
		exit(1);
	}

	umask(0);

	if (bind(sock_fd, (struct sockaddr *)&server, sizeof(server)) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not bind to socket: %s", sock_file);
		logger(L_ERR, L_FUNC, "bind: %s", strerror(rc));
		exit(1);
	}

	if (chmod(sock_file, S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not chmod socket: %s", sock_file);
		logger(L_ERR, L_FUNC, "chmod: %s", strerror(rc));
		exit(1);
	}

	fchmod(sock_fd, S_IRWXU|S_IRWXG|S_IRWXO);

	umask(077);

	if (listen(sock_fd, SOCKET_BACKLOG) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not listen on socket: %s", sock_file);
		logger(L_ERR, L_FUNC, "listen: %s", strerror(rc));
		exit(1);
	}


	logger(L_INFO, L_FUNC, "listening on socket: %s", sock_file);

	/**************************************************************
	 * Ok boys... Let's procreate... If necessary of course...
	 * Num_procs == 0 means we're running one shot per process. In
	 * that case, we'll handle forking on a per connection basis.
	 **************************************************************/
	if (num_procs != 0)
		flags |= USE_PROCESS_MODEL;

	return;
}

/*************************************************************
 * Main IPC loop. Handle all the socket accept stuff, fork if 
 * needed, then pass things off to do_request().
 *
 * __Required Function__
 **************************************************************/
void ipc_loop() {

	int		rc;
	int		conn_fd;
	unsigned char	dummy;


	while(1) {

		len = sizeof(client);

		/**************************************************************
		 * First, if needed, get the accept lock. If it fails, take a
		 * nap and go to the top of the loop. (or should we just die?)
		 *************************************************************/
		if (get_accept_lock() != 0) {
			sleep(5);
			continue;
		}

        	conn_fd = accept(sock_fd, (struct sockaddr *)&client, (unsigned int *) &len);
		rc = errno;

		rel_accept_lock();

		if (conn_fd == -1) {
			if (rc != EINTR) {
				logger(L_ERR, L_FUNC, "socket accept failure");
				logger(L_ERR, L_FUNC, "accept: %s", strerror(rc));
				sleep(5);
			}
			continue;
		}

		/**************************************************************
		 * If we're running one shot, drop off a kid to handle the
		 * connection.
		 *************************************************************/
		if (num_procs == 0) {
		    if(flags & DETACH_TTY) {
			if (have_baby() > 0) {	/* parent */
			    close(conn_fd);
			    continue;
			}
			
			close(sock_fd);         /* child  */
		    }
		    
		    do_request(conn_fd);
		    shutdown(conn_fd, SHUT_WR);
		    while (read(conn_fd, &dummy, 1) > 0) { }
		    close(conn_fd);

		    if(flags & DETACH_TTY) {
			exit(0);	
		    } else {
			continue;
		    }
		    
		}

		/**************************************************************
		 * Normal prefork mode.
		 *************************************************************/
		do_request(conn_fd);
		shutdown(conn_fd, SHUT_WR);
		while (read(conn_fd, &dummy, 1) > 0) { }
		close(conn_fd);
	}

	return;
}


/*************************************************************
 * General cleanup. Unlock, close, and unlink our files.
 *
 * __Required Function__
 **************************************************************/
void ipc_cleanup() {

	struct flock    lock_st;

	if (flags & USE_ACCEPT_LOCK) {

		lock_st.l_type = F_UNLCK;
		lock_st.l_start = 0;
		lock_st.l_whence = SEEK_SET;
		lock_st.l_len = 1;

		fcntl(accept_fd, F_SETLK, &lock_st);

		close(accept_fd);
		unlink(accept_file);

		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, "accept lock file removed: %s", accept_file);
	}

	close(sock_fd);
	unlink(sock_file);

	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "socket removed: %s", sock_file);
}


/*************************************************************
 * Handle the comms on the socket, pass the request off to
 * do_auth() back in saslauthd-main.c, then transmit the
 * result back out on the socket.
 **************************************************************/
void do_request(int conn_fd) {

	unsigned short		count;                     /* input/output data byte count           */
	unsigned short		ncount;                    /* input/output data byte count, network  */ 
	char			*response;                 /* response to send to the client         */
	char			login[MAX_REQ_LEN + 1];    /* account name to authenticate           */
	char			password[MAX_REQ_LEN + 1]; /* password for authentication            */
	char			service[MAX_REQ_LEN + 1];  /* service name for authentication        */
	char			realm[MAX_REQ_LEN + 1];    /* user realm for authentication          */


	/**************************************************************
	 * The input data stream consists of the login id, password,
	 * service name and user realm as counted length strings.
	 * We read in each string, then dispatch the data.
	 **************************************************************/

	/* login id */
	if (rx_rec(conn_fd, (void *)&count, (size_t)sizeof(count)) != (ssize_t)sizeof(count)) 
		return;

	count = ntohs(count);

	if (count > MAX_REQ_LEN) {
		logger(L_ERR, L_FUNC, "login exceeded MAX_REQ_LEN: %d", MAX_REQ_LEN);
		send_no(conn_fd, "");
		return;
	}	

	if (rx_rec(conn_fd, (void *)login, (size_t)count) != (ssize_t)count) 
		return;
	
	login[count] = '\0';

	/* password */
	if (rx_rec(conn_fd, (void *)&count, (size_t)sizeof(count)) != (ssize_t)sizeof(count)) 
		return;

	count = ntohs(count);

	if (count > MAX_REQ_LEN) {
		logger(L_ERR, L_FUNC, "password exceeded MAX_REQ_LEN: %d", MAX_REQ_LEN);
		send_no(conn_fd, "");
		return;
	}	

	if (rx_rec(conn_fd, (void *)password, (size_t)count) != (ssize_t)count) 
		return;
		
	password[count] = '\0';

	/* service */
	if (rx_rec(conn_fd, (void *)&count, (size_t)sizeof(count)) != (ssize_t)sizeof(count)) 
		return;

	count = ntohs(count);

	if (count > MAX_REQ_LEN) {
		logger(L_ERR, L_FUNC, "service exceeded MAX_REQ_LEN: %d", MAX_REQ_LEN);
		send_no(conn_fd, "");
		return;
	}	

	if (rx_rec(conn_fd, (void *)service, (size_t)count) != (ssize_t)count) 
		return;

	service[count] = '\0';

	/* realm */
	if (rx_rec(conn_fd, (void *)&count, (size_t)sizeof(count)) != (ssize_t)sizeof(count)) 
		return;

	count = ntohs(count);

	if (count > MAX_REQ_LEN) {
		logger(L_ERR, L_FUNC, "realm exceeded MAX_REQ_LEN: %d", MAX_REQ_LEN);
		send_no(conn_fd, "");
		return;
	}	

	if (rx_rec(conn_fd, (void *)realm, (size_t)count) != (ssize_t)count) 
		return;

	realm[count] = '\0';

	/**************************************************************
 	 * We don't allow NULL passwords or login names
	 **************************************************************/
	if (*login == '\0') {
		logger(L_ERR, L_FUNC, "NULL login received");
		send_no(conn_fd, "NULL login received");
		return;
	}	
	
	if (*password == '\0') {
		logger(L_ERR, L_FUNC, "NULL password received");
		send_no(conn_fd, "NULL password received");
		return;
	}	

	/**************************************************************
	 * Get the mechanism response from do_auth() and send it back.
	 **************************************************************/
	response = do_auth(login, password, service, realm);

	memset(password, 0, strlen(password));

	if (response == NULL) {
		send_no(conn_fd, "NULL response from mechanism");
		return;
	}	

	count = strlen(response);
	ncount = htons(count);

	if (tx_rec(conn_fd, (void *)&ncount, (size_t)sizeof(ncount)) != (ssize_t)sizeof(ncount)) {
		free(response);
		return;
	}

	if (tx_rec(conn_fd, (void *)response, (size_t)count) != (ssize_t)sizeof(count)) {
		free(response);
		return;
	}

	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "response: %s", response);

	free(response);

	return;
}


/*************************************************************
 * In case something went out to lunch while reading in the
 * request data, we may want to attempt to send out a default 
 * "NO" response on the socket. The mesg is optional.
 **************************************************************/
void send_no(int conn_fd, char *mesg) {
	char		buff[1024];
	unsigned short	count; 
	unsigned short	ncount; 

	buff[0] = 'N';
	buff[1] = 'O';
	buff[2] = ' ';

	/* buff, except for the trailing NUL and 'NO ' */
	strncpy(buff + 3, mesg, sizeof(buff) - 1 - 3);
	buff[1023] = '\0';

	count = strlen(buff);
	ncount = htons(count);

	if (tx_rec(conn_fd, (void *)&ncount, (size_t)sizeof(ncount)) != (ssize_t)sizeof(ncount))
		return;

	if (tx_rec(conn_fd, (void *)buff, (size_t)count) != (ssize_t)sizeof(count))
		return;

	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "response: %s", buff);
	
	return;	
}


/*************************************************************
 * Attempt to get a write lock on the accept lock file.
 * Return 0 if everything went ok, return -1 if something bad
 * happened. This function is expected to block.
 **************************************************************/
int get_accept_lock() {

	struct flock    lock_st;
	int             rc;


	if (!(flags & USE_ACCEPT_LOCK))
		return 0;

	lock_st.l_type = F_WRLCK;
	lock_st.l_start = 0;
	lock_st.l_whence = SEEK_SET;
	lock_st.l_len = 1;

	errno = 0;

	do {
		rc = fcntl(accept_fd, F_SETLKW, &lock_st);
	} while (rc != 0 && errno == EINTR);

	if (rc != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not acquire accept lock");
		logger(L_ERR, L_FUNC, "fcntl: %s", strerror(rc));
		return -1;
	}

	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "acquired accept lock");

	return 0;
}


/*************************************************************
 * Attempt to release the write lock on the accept lock file.
 * Return 0 if everything went ok, return -1 if something bad
 * happened.
 **************************************************************/
int rel_accept_lock() {

	struct flock    lock_st;
	int             rc;


	if (!(flags & USE_ACCEPT_LOCK))
		return 0;

	lock_st.l_type = F_UNLCK;
	lock_st.l_start = 0;
	lock_st.l_whence = SEEK_SET;
	lock_st.l_len = 1;

	errno = 0;

	do {
		rc = fcntl(accept_fd, F_SETLKW, &lock_st);
	} while (rc != 0 && errno == EINTR);

	if (rc != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not release accept lock");
		logger(L_ERR, L_FUNC, "fcntl: %s", strerror(rc));
		return -1;
	}

	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "released accept lock");

	return 0;
}



#endif /* USE_UNIX_IPC */
