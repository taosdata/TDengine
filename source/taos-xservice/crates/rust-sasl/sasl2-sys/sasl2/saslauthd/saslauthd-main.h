/****************************************************************************
 *
 * saslauthd-main.h
 *
 * Description:  Header file for saslauthd-main.c
 *               
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
 * THIS SOFTWARE IS PROVIDED ``AS IS''. ANY EXPRESS OR IMPLIED WARRANTIES,
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL JEREMY RUMPF OR ANY CONTRIBUTER TO THIS SOFTWARE BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE
 *
 * HISTORY
 * 
 * Feb 2004: Partial rewrite and cleanup  by Jeremy Rumpf jrumpf@heavyload.net
 * - Merge the doors and unix IPC methods under a common framework.
 *
 * This source file created using 8 space tabs.
 *
 ****************************************************************************/

#ifndef _SASLAUTHDMAIN_H
#define _SASLAUTHDMAIN_H

#include <config.h>

#include <sys/types.h>

/****************************************************************
 * Plug in some autoconf magic to determine what IPC method
 * to use.
 ****************************************************************/
#ifdef USE_DOORS
# define USE_DOORS_IPC
#else
# define USE_UNIX_IPC
#endif

/* AIX uses a slight variant of this */
#ifdef _AIX
# define SALEN_TYPE size_t
#else 
# define SALEN_TYPE int
#endif 

/* Define some macros. These help keep the ifdefs out of the
 * mainline code. */
#ifdef AUTH_SIA
#define SET_AUTH_PARAMETERS(argc, argv) set_auth_parameters(argc, argv)
#else
#define SET_AUTH_PARAMETERS(argc, argv)
#endif

/* file name defines - don't forget the '/' in these! */
#define PID_FILE		"/saslauthd.pid"    
#define PID_FILE_LOCK		"/saslauthd.pid.lock"
#define ACCEPT_LOCK_FILE	"/mux.accept"       
#define SOCKET_FILE		"/mux"              
#define DOOR_FILE		"/mux"              

/* login, pw, service, realm buffer size */
#define MAX_REQ_LEN		256     

/* socket backlog when supported */
#define SOCKET_BACKLOG  	32

/* saslauthd-main.c */
extern char	*do_auth(const char *, const char *,
			 const char *, const char *);
extern void	set_auth_mech(const char *);
extern void	set_max_procs(const char *);
extern void	set_mech_option(const char *);
extern void	set_run_path(const char *);
extern void	signal_setup();
extern void	detach_tty();
extern void	handle_sigchld();
extern void	server_exit();
extern pid_t	have_baby();

/* ipc api delcarations */
extern void	ipc_init();
extern void	ipc_loop();
extern void	ipc_cleanup();

#endif  /* _SASLAUTHDMAIN_H */
