/*******************************************************************************
 * *****************************************************************************
 * *
 * * globals.h
 * *
 * * Description:  Header file for all application wide globale variables.
 * *
 * * Copyright (c) 1997-2000 Messaging Direct Ltd.
 * * All rights reserved.
 * *
 * * Redistribution and use in source and binary forms, with or without
 * * modification, are permitted provided that the following conditions
 * * are met:
 * *
 * * 1. Redistributions of source code must retain the above copyright
 * *    notice, this list of conditions and the following disclaimer.
 * *
 * * 2. Redistributions in binary form must reproduce the above copyright
 * *    notice, this list of conditions and the following disclaimer in the
 * *    documentation and/or other materials provided with the distribution.
 * *
 * * THIS SOFTWARE IS PROVIDED ``AS IS''. ANY EXPRESS OR IMPLIED WARRANTIES,
 * * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * * IN NO EVENT SHALL JEREMY RUMPF OR ANY CONTRIBUTER TO THIS SOFTWARE BE
 * * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * * THE POSSIBILITY OF SUCH DAMAGE
 * *
 * * HISTORY
 * * 
 * * This source file created using 8 space tabs.
 * *
 * ******************************************************************************
 ********************************************************************************/

#ifndef _GLOBALS_H
#define _GLOBALS_H

#include "mechanisms.h"


/* saslauthd-main.c */
extern int              g_argc;
extern char             **g_argv;
extern int              flags;
extern int              num_procs;
extern char             *mech_option;
extern char             *run_path;
extern authmech_t       *auth_mech;


/* flags bits */
#define VERBOSE                 (1 << 1)
#define LOG_USE_SYSLOG          (1 << 2)
#define LOG_USE_STDERR          (1 << 3)
#define AM_MASTER               (1 << 4)
#define USE_ACCEPT_LOCK         (1 << 5)
#define DETACH_TTY              (1 << 6)
#define CACHE_ENABLED           (1 << 7)
#define USE_PROCESS_MODEL       (1 << 8)
#define CONCAT_LOGIN_REALM      (1 << 9)


#endif  /* _GLOBALS_H */
