/*******************************************************************************
 * *****************************************************************************
 * *
 * * utils.h
 * *
 * * Description:  Header file for utils.c
 * *               
 * *
 * * Copyright (c) 1997-2000 Messaging Direct Ltd.
 * * All rights reserved.
 * *
 * * Portions Copyright (c) 2003 Jeremy Rumpf
 * * jrumpf@heavyload.net
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
 * *
 * * This source file created using 8 space tabs.
 * *
 * ******************************************************************************
 ********************************************************************************/

#ifndef _UTILS_H
#define _UTILS_H

#include <config.h>

#include <syslog.h>
#include <sys/types.h>
#include <sys/uio.h>

/* log prioities */
#define L_ERR			LOG_ERR
#define L_INFO			LOG_INFO
#define L_DEBUG			LOG_DEBUG


/* some magic to grab function names */
#ifdef HAVE_FUNC
# define L_FUNC __func__
# define HAVE_L_FUNC 1
#elif defined(HAVE_PRETTY_FUNCTION)
# define L_FUNC __PRETTY_FUNCTION__
# define HAVE_L_FUNC 1
#elif defined(HAVE_FUNCTION)
# define L_FUNC __FUNCTION__
# define HAVE_L_FUNC 1
#else
# define L_FUNC ""
# undef HAVE_L_FUNC
#endif

#ifdef HAVE_L_FUNC
# define L_STDERR_FORMAT        "saslauthd[%d] :%-16s: %s\n"
#else
# define L_STDERR_FORMAT        "saslauthd[%d] :%s%s\n"
#endif 


/* utils.c */
extern void	logger(int, const char *, const char *, ...);
extern ssize_t	tx_rec(int filefd, void *, size_t);
extern ssize_t	rx_rec(int , void *, size_t);
extern int	retry_writev(int, struct iovec *, int);


#endif  /* _UTILS_H */
