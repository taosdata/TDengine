/*****************************************************************************
 *
 * saslauthd-main.c
 *
 * Description:  Main program source.
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
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
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
 * saslauthd is a re-implementation of the pwcheck utility included
 * with the CMU Cyrus IMAP server circa 1997. This implementation
 * was written by Lyndon Nerenberg of Messaging Direct Inc. (which
 * at that time was the Esys Corporation) and was included in the
 * company's IMAP message store product (Simeon Message Service) as
 * the smsauthd utility.
 *
 * This implementation was contributed to CMU by Messaging Direct Ltd.
 * in September 2000.
 *
 * September 2001 (Ken Murchison of Oceana Matrix Ltd.):
 * - Modified the protocol to use counted length strings instead of
 *   nul delimited strings.
 * - Augmented the protocol to accept the service name and user realm.
 * 
 * Feb 2003: Partial rewrite and cleanup  by Jeremy Rumpf jrumpf@heavyload.net
 * - Merge the doors and unix IPC methods under a common framework.
 *
 *   OVERVIEW
 *
 * saslauthd provides an interface between the SASL library and various
 * external authentication mechanisms. The primary goal is to isolate
 * code that requires superuser privileges (for example, access to
 * the shadow password file) into a single easily audited module. It
 * can also act as an authentication proxy between plaintext-equivelent
 * authentication schemes (i.e. CRAM-MD5) and more secure authentication
 * services such as Kerberos, although such usage is STRONGLY discouraged
 * because it exposes the strong credentials via the insecure plaintext
 * mechanisms.
 *
 * The program listens for connections on a UNIX domain socket. Access to
 * the service is controlled by the UNIX filesystem permissions on the
 * socket.
 *
 * The service speaks a very simple protocol. The client connects and
 * sends the authentication identifier, the plaintext password, the
 * service name and user realm as counted length strings (a 16-bit
 * unsigned integer in network byte order followed by the string
 * itself). The server returns a single response as a counted length
 * string. The response begins with "OK" or "NO", and is followed by
 * an optional text string (separated from the OK/NO by a single space
 * character), and a NUL. The server then closes the connection.
 *
 * An "OK" response indicates the authentication credentials are valid.
 * A "NO" response indicates the authentication failed.
 *
 * The optional text string may be used to indicate an exceptional
 * condition in the authentication environment that should be communicated
 * to the client.
 *
 *****************************************************************************/

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#ifdef _AIX
# include <strings.h>
#endif /* _AIX */

#include <syslog.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/uio.h>

#include "globals.h"
#include "saslauthd-main.h"
#include "cache.h"
#include "utils.h"

/* max login + max realm + '@' */
#define MAX_LOGIN_REALM_LEN (MAX_REQ_LEN * 2) + 1

/****************************************
 * declarations/protos
 *****************************************/
static void	show_version();
static void	show_usage();

/****************************************
 * application globals
 *****************************************/
int		flags = 0;		/* Runtime flags                     */
int		g_argc;			/* Copy of argc for those who need it*/
char		**g_argv;		/* Copy of argv for those who need it*/
char		*run_path = NULL;	/* path to our working directory     */
authmech_t	*auth_mech = NULL;	/* Authentication mechanism to use   */
char		*mech_option = NULL;	/* mechanism-specific option	     */
int		num_procs = 5;		/* The max number of worker processes*/


/****************************************
 * module globals
*****************************************/
extern char 	*optarg;		/* For getopt()                          */
static int     	master_pid;		/* Pid of the master process             */
static int 	pid_fd;                 /* Descriptor to the open pid file       */
static int 	pid_file_lock_fd; 		/* Descriptor to the open pid lock file  */
static char	*pid_file;		/* Pid file name                         */
static char	*pid_file_lock;		/* Pid lock file name                    */
static int       startup_pipe[2] = { -1, -1 };

int main(int argc, char **argv) {
	int		option;
	int 		rc;
	int 		x;
	struct flock	lockinfo;
	char            *auth_mech_name = NULL;
	size_t		pid_file_size;

	/* XXX  force openlog() before any of our mechs try syslog() */
	logger(L_INFO, L_FUNC, "starting %s", argv[0]);

	SET_AUTH_PARAMETERS(argc, argv);

	g_argc = argc;
	g_argv = argv;

	/* default flags */
	flags |= USE_ACCEPT_LOCK;
	flags |= DETACH_TTY;
	flags |= LOG_USE_SYSLOG;
	flags |= LOG_USE_STDERR;
	flags |= AM_MASTER;

	while ((option = getopt(argc, argv, "a:cdhO:lm:n:rs:t:vV")) != -1) {
		switch(option) {
			case 'a':
			        /* Only one at a time, please! */
			        if(auth_mech_name) {
				    show_usage();
				    break;
				}

				auth_mech_name = strdup(optarg);
				if (!auth_mech_name) {
				    logger(L_ERR, L_FUNC,
					   "could not allocate memory");
				    exit(1);
				}
				break;

			case 'c':
				flags |= CACHE_ENABLED;
				break;

			case 'd':
				flags |= VERBOSE;
				flags &= ~DETACH_TTY;
				break;

			case 'h':
				show_usage();
				break;
				
			case 'O':
				set_mech_option(optarg);
				break;

			case 'l':
				flags &= ~USE_ACCEPT_LOCK;
				break;

			case 'm':
				set_run_path(optarg);
				break;

			case 'n':
				set_max_procs(optarg);
				break;

			case 'r':
				flags |= CONCAT_LOGIN_REALM;
				break;

			case 's':
				cache_set_table_size(optarg);
				break;

			case 't':
				cache_set_timeout(optarg);
				break;

		        case 'V':
				flags |= VERBOSE;		    
				break;

			case 'v':
				show_version();
				break;
				
			default:
				show_usage();
				break;
		}
	}

	if (run_path == NULL)
    		run_path = PATH_SASLAUTHD_RUNDIR;

    	if (auth_mech_name == NULL) {
		logger(L_ERR, L_FUNC, "no authentication mechanism specified");
		show_usage();
		exit(1);
	}

	/* Create our working directory */
	if (mkdir(run_path, 0755) == -1 && errno != EEXIST) {
		logger(L_ERR, L_FUNC, "can not mkdir: %s", run_path);
		logger(L_ERR, L_FUNC, "Check to make sure the parent directory exists and is");
		logger(L_ERR, L_FUNC, "writeable by the user this process runs as.");
		exit(1);
	}

	set_auth_mech(auth_mech_name);

	if (flags & VERBOSE)  {
		logger(L_DEBUG, L_FUNC, "num_procs  : %d", num_procs);

		if (mech_option == NULL)
			logger(L_DEBUG, L_FUNC, "mech_option: NULL");
		else
			logger(L_DEBUG, L_FUNC, "mech_option: %s", mech_option);

		logger(L_DEBUG, L_FUNC, "run_path   : %s", run_path);
		logger(L_DEBUG, L_FUNC, "auth_mech  : %s", auth_mech->name);
	}

	/*********************************************************
	 * Change our working directory to the dir where the
	 * run path is set to, core dumps will go there to keep
	 * them intact.
	 **********************************************************/
	if (chdir(run_path) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not chdir to: %s", run_path);
		logger(L_ERR, L_FUNC, "chdir: %s", strerror(rc));
		logger(L_ERR, L_FUNC, "Check to make sure the directory exists and is");
		logger(L_ERR, L_FUNC, "writeable by the user this process runs as.");
		exit(1);
	}

	umask(0077);

	pid_file_size = strlen(run_path) + sizeof(PID_FILE_LOCK) + 1;
	if ((pid_file_lock = malloc(pid_file_size)) == NULL) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		exit(1);
	}
    
	strlcpy(pid_file_lock, run_path, pid_file_size);
	strlcat(pid_file_lock, PID_FILE_LOCK, pid_file_size);

	if ((pid_file_lock_fd = open(pid_file_lock, O_CREAT|O_TRUNC|O_RDWR, 0644)) < 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not open pid lock file: %s", pid_file_lock);
		logger(L_ERR, L_FUNC, "open: %s", strerror(rc));
		logger(L_ERR, L_FUNC,
		       "Check to make sure the directory exists and is");
		logger(L_ERR, L_FUNC, "writeable by the user this process runs as.");
		exit(1);
	}

	lockinfo.l_type = F_WRLCK;
	lockinfo.l_start = 0;
	lockinfo.l_len = 0;
	lockinfo.l_whence = SEEK_SET;

	if (fcntl(pid_file_lock_fd, F_SETLK, &lockinfo) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not lock pid lock file: %s", pid_file_lock);
		logger(L_ERR, L_FUNC, "fcntl: %s", strerror(rc));
		exit(1);
	}
    
	if(pipe(startup_pipe) == -1) {
		logger(L_ERR, L_FUNC, "can't create startup pipe");
		exit(1);
	}

	/*********************************************************
	 * Enable signal handlers.
	 **********************************************************/
	signal_setup();

	/*********************************************************
	 * Cache setup, exit if it doesn't succeed (optional would
	 * be to disable the cache and log a warning).
	 **********************************************************/
	if (cache_init() != 0)
		exit(1);

	/*********************************************************
	 * Call the ipc specific initializer. This should also
	 * call detach_tty() at the appropriate point.
	 **********************************************************/
	ipc_init();

	/*********************************************************
	 * Enable general cleanup.
	 **********************************************************/
	atexit(server_exit);

	/*********************************************************
	 * If required, enable the process model.
	 **********************************************************/
	if (flags & USE_PROCESS_MODEL) {
        	if (flags & VERBOSE)
                	logger(L_DEBUG, L_FUNC, "using process model");

        	for (x = 1; x < num_procs; x++) {
			if (have_baby() != 0)
				continue;		/* parent */

                	break;				/* child  */
        	}
	}

	/*********************************************************
	 * Enter the ipc loop, we should never return.
	 **********************************************************/
	ipc_loop();

	exit(0);
}


/*************************************************************
 * Performs all authentication centric duties. We should be
 * getting callbacks from the ipc method here. We'll simply 
 * return a pointer to a string to send back to the client.
 * The caller is responsible for freeing the pointer. 
 **************************************************************/
char *do_auth(const char *_login, const char *password, const char *service, const char *realm) {

	struct cache_result	lkup_result;
	char			*response = NULL;
	int			cached = 0;
	char			login_buff[MAX_LOGIN_REALM_LEN];
	char			*login;


	/***********************************************************
	 * Check to concat the login and realm into a single login.
	 * Aka, login: foo realm: bar becomes login: foo@bar.
	 * We do this because some mechs have no concept of a realm.
	 * Ie. auth_pam and friends.
	 ***********************************************************/
	if ((flags & CONCAT_LOGIN_REALM) && realm && realm[0] != '\0') {
	    strlcpy(login_buff, _login, sizeof(login_buff));
	    strlcat(login_buff, "@", sizeof(login_buff));
	    strlcat(login_buff, realm, sizeof(login_buff));

	    login = login_buff;
	} else {
	    login = (char *)_login;
	}

	if (cache_lookup(login, realm, service, password, &lkup_result) == CACHE_OK) {	
		response = strdup("OK");
		cached = 1;
	} else {
		response = auth_mech->authenticate(login, password, service, realm);

		if (response == NULL) {
			logger(L_ERR, L_FUNC, "internal mechanism failure: %s", auth_mech->name);
			response = strdup("NO internal mechanism failure");
		}
	}

	if (strncmp(response, "OK", 2) == 0) {
		cache_commit(&lkup_result);

		if (flags & VERBOSE) {
			if (cached) 
				logger(L_DEBUG, L_FUNC, "auth success (cached): [user=%s] [service=%s] [realm=%s]", \
					login, service, realm);
			else
				logger(L_DEBUG, L_FUNC, "auth success: [user=%s] [service=%s] [realm=%s] [mech=%s]", \
					login, service, realm, auth_mech->name);
		}
		return response;
	}

	if (strncmp(response, "NO", 2) == 0) {
		logger(L_INFO, L_FUNC, "auth failure: [user=%s] [service=%s] [realm=%s] [mech=%s] [reason=%s]", \
			login, service, realm, auth_mech->name,
		        strlen(response) >= 4 ? response+3 : "Unknown");

		return response;
	}

	logger(L_ERR, L_FUNC, "mechanism returned unknown response: %s", auth_mech->name);
	free(response);
	response = strdup("NO internal mechanism failure");

	return response;
}


/*************************************************************
 * Allow someone to set the auth mech to use
 **************************************************************/
void set_auth_mech(const char *mech) {
	for (auth_mech = mechanisms; auth_mech->name != NULL; auth_mech++) {
		if (strcasecmp(auth_mech->name, mech) == 0)
			break;
	}

	if (auth_mech->name == NULL) {
		logger(L_ERR, L_FUNC, "unknown authentication mechanism: %s", mech);
		exit(1);
	}

	if (auth_mech->initialize) {
		if(auth_mech->initialize() != 0) {
		    logger(L_ERR, L_FUNC, "failed to initialize mechanism %s",
			   auth_mech->name);
		    exit(1);
		}
	}
}


/*************************************************************
 * Allow someone to set the number of worker processes we
 * will use. Only applicable to unix ipc.
 **************************************************************/
void set_max_procs(const char *procs) {
	num_procs = atoi(procs);

	if(num_procs < 0) {
		logger(L_ERR, L_FUNC, "invalid number of worker processes defined");
		exit(1);
	}

	return;
}


/*************************************************************
 * Allow someone to set the mechanism specific option
 **************************************************************/
void set_mech_option(const char *option) {

	free(mech_option);
	mech_option = NULL;

	if ((mech_option = strdup(option)) == NULL) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		exit(1);
	}

	return;
}


/*************************************************************
 * Allow someone to set the path to our working directory
 **************************************************************/
void set_run_path(const char *path) {

	if (*path != '/') {
		logger(L_ERR, L_FUNC, "-m requires an absolute pathname");
		exit(1);
	}

	free(run_path);
	run_path = NULL;

	if ((run_path = strdup(path)) == NULL) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		exit(1);
	}

	return;
}



/*************************************************************
 * Setup all the proper signal masks. 
 **************************************************************/
void signal_setup() {

	static struct sigaction act_sigchld;
	static struct sigaction act_sigalrm;
	static struct sigaction act_sigterm;
	static struct sigaction act_sigpipe;
	static struct sigaction act_sighup;
	static struct sigaction act_sigint;
	int			rc;

	/**************************************************************
	 * Handler for SIGCHLD
	 **************************************************************/
	act_sigchld.sa_handler = handle_sigchld;
	sigemptyset(&act_sigchld.sa_mask);

	if (sigaction(SIGCHLD, &act_sigchld, NULL) != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "failed to set sigaction for SIGCHLD");
		logger(L_ERR, L_FUNC, "sigaction: %s", strerror(rc));
		exit(1);
	}

	/**************************************************************
	 * Handler for SIGALRM  (IGNORE)
	 **************************************************************/
	act_sigalrm.sa_handler = SIG_IGN;
	sigemptyset(&act_sigalrm.sa_mask);

	if (sigaction(SIGALRM, &act_sigalrm, NULL) != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "failed to set sigaction for SIGALRM");
		logger(L_ERR, L_FUNC, "sigaction: %s", strerror(rc));
		exit(1);
	}

	/**************************************************************
	 * Handler for SIGPIPE  (IGNORE)
	 **************************************************************/
	act_sigpipe.sa_handler = SIG_IGN;
	sigemptyset(&act_sigpipe.sa_mask);

	if (sigaction(SIGPIPE, &act_sigpipe, NULL) != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "failed to set sigaction for SIGPIPE");
		logger(L_ERR, L_FUNC, "sigaction: %s", strerror(rc));
		exit(1);
	}

	/**************************************************************
	 * Handler for SIGHUP  (IGNORE)
	 **************************************************************/
	act_sighup.sa_handler = SIG_IGN;
	sigemptyset(&act_sighup.sa_mask);

	if (sigaction(SIGHUP, &act_sighup, NULL) != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "failed to set sigaction for SIGHUP");
		logger(L_ERR, L_FUNC, "sigaction: %s", strerror(rc));
		exit(1);
	}

	/**************************************************************
	 * Handler for SIGTERM
	 **************************************************************/
	act_sigterm.sa_handler = server_exit;
	sigemptyset(&act_sigterm.sa_mask);

	if (sigaction(SIGTERM, &act_sigterm, NULL) != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "failed to set sigaction for SIGTERM");
		logger(L_ERR, L_FUNC, "sigaction: %s", strerror(rc));
		exit(1);
	}

	/**************************************************************
	 * Handler for SIGINT
	 **************************************************************/
	act_sigint.sa_handler = server_exit;
	sigemptyset(&act_sigint.sa_mask);

	if (sigaction(SIGINT, &act_sigint, NULL) != 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "failed to set sigaction for SIGINT");
		logger(L_ERR, L_FUNC, "sigaction: %s", strerror(rc));
		exit(1);
	}

	return;
}


/*************************************************************
 * Detaches us from the controlling tty (aka daemonize). 
 * More than likely this will be called from an ipc_init()
 * function as we want to stay in the foreground for as long
 * as possible.
 **************************************************************/
void detach_tty() {
    int		x;
    int		rc;
    int		null_fd;
    int         exit_result;
    pid_t      	pid;
    struct flock	lockinfo;
    
    /**************************************************************
     * Make sure we're supposed to do this, the user may have 
     * requested us to stay in the foreground.
     **************************************************************/
    if (flags & DETACH_TTY) {
	for(x=5; x; x--) {
	    pid = fork();
	    
	    if ((pid == -1) && (errno == EAGAIN)) {
		logger(L_ERR, L_FUNC,
		       "fork failed, retrying");
		sleep(5);
		continue;
	    }
	    
	    break;
	}
	
	if (pid == -1) {
	    /* Non retryable error. */
	    rc = errno;
	    logger(L_ERR, L_FUNC, "Cannot start saslauthd");
	    logger(L_ERR, L_FUNC, "saslauthd master fork failed: %s",
		   strerror(rc));
	    exit(1);
	} else if (pid != 0) {
	    int exit_code;
	    
	    /* Parent, wait for child */
	    if(read(startup_pipe[0], &exit_code, sizeof(exit_code)) == -1) {
		logger(L_ERR, L_FUNC,
		       "Cannot start saslauthd");
		logger(L_ERR, L_FUNC,
		       "could not read from startup_pipe");
		unlink(pid_file_lock);
		exit(1);
	    } else {
		if (exit_code != 0) {
		    logger(L_ERR, L_FUNC, "Cannot start saslauthd");
		    if (exit_code == 2) {
			logger(L_ERR, L_FUNC,
			       "Another instance of saslauthd is currently running");
		    } else {
			logger(L_ERR, L_FUNC, "Check syslog for errors");
		    }
		}
		unlink(pid_file_lock);
		exit(exit_code);
	    }
	}
	
	/* Child! */
	close(startup_pipe[0]);
	
	free(pid_file_lock);
	
	if (setsid() == -1) {
	    exit_result = 1;
	    rc = errno;
	    
	    logger(L_ERR, L_FUNC, "failed to set session id: %s",
		   strerror(rc));
	    
	    /* Tell our parent that we failed. */
	    write(startup_pipe[1], &exit_result, sizeof(exit_result));
	    
	    exit(1);
	}
	
	if ((null_fd = open("/dev/null", O_RDWR, 0)) == -1) {
	    exit_result = 1;
	    rc = errno;
	    
	    logger(L_ERR, L_FUNC, "failed to open /dev/null: %s",
		   strerror(rc));
	    
	    /* Tell our parent that we failed. */
	    write(startup_pipe[1], &exit_result, sizeof(exit_result));
	    
	    exit(1);
	}
	
	/*********************************************************
	 * From this point on, stop printing errors out to stderr.
	 **********************************************************/
	flags &= ~LOG_USE_STDERR;

	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);
	
	dup2(null_fd, STDIN_FILENO);
	dup2(null_fd, STDOUT_FILENO);
	dup2(null_fd, STDERR_FILENO);

	if (null_fd > 2)
	    close(null_fd);
		
	/*********************************************************
	 * Locks don't persist across forks. Relock the pid file
	 * to keep folks from having duplicate copies running...
	 *********************************************************/
	if (!(pid_file = malloc(strlen(run_path) + sizeof(PID_FILE) + 1))) {
	    exit_result = 1;
	    logger(L_ERR, L_FUNC, "could not allocate memory");
	    write(startup_pipe[1], &exit_result, sizeof(exit_result));
	    exit(1);
	}
	
	strcpy(pid_file, run_path);
	strcat(pid_file, PID_FILE);
	
	/* Write out the pidfile */
	pid_fd = open(pid_file, O_CREAT|O_RDWR, 0644);
	if(pid_fd == -1) {
	    rc = errno;
	    exit_result = 1;

	    logger(L_ERR, L_FUNC, "could not open pid file %s: %s",
		   pid_file, strerror(rc));
	    
	    /* Tell our parent that we failed. */
	    write(startup_pipe[1], &exit_result, sizeof(exit_result));
	    
	    exit(1);
	} else {
	    char buf[100];
	    
	    lockinfo.l_type = F_WRLCK;
	    lockinfo.l_start = 0;
	    lockinfo.l_len = 0;
	    lockinfo.l_whence = SEEK_SET;
	    
	    if (fcntl(pid_fd, F_SETLK, &lockinfo) == -1) {
		exit_result = 2;
		rc = errno;
		
		logger(L_ERR, L_FUNC, "could not lock pid file %s: %s",
		       pid_file, strerror(rc));
		
		/* Tell our parent that we failed. */
		write(startup_pipe[1], &exit_result, sizeof(exit_result));
		
		exit(2);
	    } else {
		int pid_fd_flags = fcntl(pid_fd, F_GETFD, 0);
		
		if (pid_fd_flags != -1) {
		    pid_fd_flags =
			fcntl(pid_fd, F_SETFD, pid_fd_flags | FD_CLOEXEC);
		}
		
		if (pid_fd_flags == -1) {
		    int exit_result = 1;
		    
		    logger(L_ERR, L_FUNC, "unable to set close-on-exec for pidfile");
		    
		    /* Tell our parent that we failed. */
		    write(startup_pipe[1], &exit_result, sizeof(exit_result));
		    
		    exit(1);
		}
		
		/* Write PID */
		master_pid = getpid();
		snprintf(buf, sizeof(buf), "%lu\n", (unsigned long)master_pid);
		if (lseek(pid_fd, 0, SEEK_SET) == -1 ||
		    ftruncate(pid_fd, 0) == -1 ||
		    write(pid_fd, buf, strlen(buf)) == -1) {
		    int exit_result = 1;
		    rc = errno;
		    
		    logger(L_ERR, L_FUNC, "could not write to pid file %s: %s", pid_file, strerror(rc));
		    
		    /* Tell our parent that we failed. */
		    write(startup_pipe[1], &exit_result, sizeof(exit_result));
		    
		    exit(1);
		}
		fsync(pid_fd);
	    }
	}
	
	{
	    int exit_result = 0;
	    
	    /* success! */
	    if(write(startup_pipe[1], &exit_result, sizeof(exit_result)) == -1) {
		logger(L_ERR, L_FUNC,
		       "could not write success result to startup pipe");
		exit(1);
	    }
	}
	
	close(startup_pipe[1]);
	if(pid_file_lock_fd != -1) close(pid_file_lock_fd);
    }
    
    logger(L_INFO, L_FUNC, "master pid is: %lu", (unsigned long)master_pid);
    
    return;
}


/*************************************************************
 * Fork off a copy of ourselves. Return 0 if we're the child,
 * > 0 for the parent. Die if we can't fork (the environment
 * is probably unstable?).
 **************************************************************/
pid_t have_baby() {
        pid_t   pid;
        int     rc;

        pid = fork();

        if (pid < 0) {
                rc = errno;
                logger(L_ERR, L_FUNC, "could not fork child process");
                logger(L_ERR, L_FUNC, "fork: %s", strerror(rc));
                exit(1);
        }

	/*********************************************************
	 * If we're the child, clear the AM_MASTER flag.
	 **********************************************************/
	if (pid == 0) {
		flags &= ~AM_MASTER;
        	return pid;
	}

        if (flags & VERBOSE) {
                logger(L_DEBUG, L_FUNC, "forked child: %lu",
		       (unsigned long)pid);
	}

        return pid;
}


/*************************************************************
 * Reap in all the dead children
 **************************************************************/
void handle_sigchld() {
	pid_t pid;

	while ((pid = waitpid(-1, 0, WNOHANG)) > 0) {
		if (flags & VERBOSE) 
			logger(L_DEBUG, L_FUNC, "child exited: %lu", (unsigned long)pid);

	}

	return;
}


/*************************************************************
 * Do some final cleanup here.
 **************************************************************/
void server_exit() {

	/*********************************************************
	 * If we're not the master process, don't do anything
	 **********************************************************/
	if (!(flags & AM_MASTER)) {
		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, "child exited: %d", getpid());

		_exit(0);
	}

	kill(-master_pid, SIGTERM);

	/*********************************************************
	 * Tidy up and delete the pid_file. (close will release the lock)
         * besides, we want to unlink it first anyway to avoid a race.
	 * Note that only one process (the master, in our case) should
	 * unlink it.
	 **********************************************************/
	if(flags & DETACH_TTY) {
	    if(getpid() == master_pid) unlink(pid_file);
	    close(pid_fd);

	    if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "pid file removed: %s", pid_file);

	    free(pid_file);
	} else {
	    /* Tidy up and delete the pid_file_lock. (in the detached
	       case this is covered by the parent process already */

	    unlink(pid_file_lock);
	    close(pid_file_lock_fd);
	    
	    if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "pid file lock removed: %s",
		       pid_file_lock);
	    free(pid_file_lock);
	}

	

	/*********************************************************
 	 * Cleanup the cache, if it's enabled
	 **********************************************************/
	if (flags & CACHE_ENABLED) {
		cache_cleanup_lock();
		cache_cleanup_mm();
	}

	/*********************************************************
	 * Tell the IPC method to clean its room. 
	 **********************************************************/
	ipc_cleanup();

	/*********************************************************
	 * Any other cleanup should go here
	 **********************************************************/

	logger(L_INFO, L_FUNC, "master exited: %d", master_pid);

	_exit(0);
}


/*************************************************************
 * Dump out our version and all the auth mechs we support
 **************************************************************/
void show_version() {
    authmech_t *authmech;
    
    fprintf(stderr, "saslauthd %s\nauthentication mechanisms:", VERSION);

    for (authmech = mechanisms; authmech->name != NULL; authmech++) {
	fprintf(stderr, " %s", authmech->name);
    }

    fprintf(stderr, "\n\n");
    exit(0);
}


/*************************************************************
 * Dump out our usage info and tag a show_version after it
 **************************************************************/
void show_usage() {
    fprintf(stderr, "usage: saslauthd [options]\n\n");
    fprintf(stderr, "option information:\n");
    fprintf(stderr, "  -a <authmech>  Selects the authentication mechanism to use.\n");
    fprintf(stderr, "  -c             Enable credential caching.\n");
    fprintf(stderr, "  -d             Debugging (don't detach from tty, implies -V)\n");
    fprintf(stderr, "  -r             Combine the realm with the login before passing to authentication mechanism\n");
    fprintf(stderr, "                 Ex. login: \"foo\" realm: \"bar\" will get passed as login: \"foo@bar\"\n");
    fprintf(stderr, "                 The realm name is passed untouched.\n");
    fprintf(stderr, "  -O <option>    Optional argument to pass to the authentication\n");
    fprintf(stderr, "                 mechanism.\n");
    fprintf(stderr, "  -l             Disable accept() locking. Increases performance, but\n");
    fprintf(stderr, "                 may not be compatible with some operating systems.\n");
    fprintf(stderr, "  -m <path>      Alternate path for the saslauthd working directory,\n");
    fprintf(stderr, "                 must be absolute.\n"); 
    fprintf(stderr, "  -n <procs>     Number of worker processes to create.\n");
    fprintf(stderr, "  -s <kilobytes> Size of the credential cache (in kilobytes)\n");
    fprintf(stderr, "  -t <seconds>   Timeout for items in the credential cache (in seconds)\n");
    fprintf(stderr, "  -v             Display version information and available mechs\n");
    fprintf(stderr, "  -V             Enable verbose logging\n");
    fprintf(stderr, "  -h             Display this message.\n\n");

    show_version();
    exit(0);
}

