/*******************************************************************************
 * *****************************************************************************
 * *
 * * cache.h
 * *
 * * Description:  Header file for cache.c
 * *               
 * *
 * * Copyright (C) 2003 Jeremy Rumpf
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
 * * Jeremy Rumpf
 * * jrumpf@heavyload.net
 * *
 * ******************************************************************************
 ********************************************************************************/

#ifndef _CACHE_H
#define _CACHE_H


/* constant includes */
#include <config.h>


/****************************************************************
* * Plug in some autoconf magic to determine what implementation
* * to use for the table slot (row) locking.
****************************************************************/
#ifdef USE_DOORS
# define CACHE_USE_PTHREAD_RWLOCK
#else
# define CACHE_USE_FCNTL
#endif



/************************************************/
#ifdef CACHE_USE_FCNTL
	/* FCNTL Impl */

struct lock_ctl {
	char			*flock_file;
	int			flock_fd;
};

#endif  /* CACHE_USE_FCNTL */
/************************************************/



/************************************************/
#ifdef CACHE_USE_PTHREAD_RWLOCK
	/* RWLock Impl */

#include <pthread.h>

struct lock_ctl {
	pthread_rwlock_t	*rwlock;
};

#endif  /* CACHE_USE_PTHREAD_RWLOCK */
/************************************************/



/* defaults */
#define CACHE_DEFAULT_TIMEOUT		28800
#define CACHE_DEFAULT_TABLE_SIZE	1711
#define CACHE_DEFAULT_FLAGS		0
#define CACHE_MAX_BUCKETS_PER		6
#define CACHE_MMAP_FILE			"/cache.mmap"  /* don't forget the "/" */
#define CACHE_FLOCK_FILE		"/cache.flock" /* don't forget the "/" */



/* If debugging uncomment this for always verbose  */
/* #define CACHE_DEFAULT_FLAGS		CACHE_VERBOSE */



/* max length for cached credential values */
#define CACHE_MAX_CREDS_LENGTH		60



/* magic values (must be less than 63 chars!) */
#define CACHE_CACHE_MAGIC		"SASLAUTHD_CACHE_MAGIC"



/* return values */
#define CACHE_OK			0
#define CACHE_FAIL			1
#define CACHE_TOO_BIG			2	



/* cache_result status values */
#define CACHE_NO_FLUSH			0
#define CACHE_FLUSH			1
#define CACHE_FLUSH_WITH_RESCAN		2	



/* declarations */
struct bucket {
        char            	creds[CACHE_MAX_CREDS_LENGTH];
        unsigned int		user_offt;
        unsigned int		realm_offt;
        unsigned int		service_offt;
        unsigned char   	pwd_digest[16];
        time_t          	created;
};

struct stats {
        volatile unsigned int   hits;
        volatile unsigned int   misses;
        volatile unsigned int   lock_failures;
        volatile unsigned int   attempts;
        unsigned int            table_size;
        unsigned int            max_buckets_per;
        unsigned int            sizeof_bucket;
        unsigned int            bytes;
        unsigned int            timeout;
};

struct mm_ctl {
	void			*base;
	unsigned int		bytes;
	char			*file;
};

struct cache_result {
	struct bucket		bucket;
	struct bucket   	*read_bucket;
	unsigned int		hash_offset;
	int			status;
};


/* cache.c */
extern int cache_init(void);
extern int cache_lookup(const char *, const char *, const char *, const char *, struct cache_result *);
extern void cache_commit(struct cache_result *);
extern int cache_pjwhash(char *);
extern void cache_set_table_size(const char *);
extern void cache_set_timeout(const char *);
extern unsigned int cache_get_next_prime(unsigned int);
extern void *cache_alloc_mm(unsigned int);
extern void cache_cleanup_mm(void);
extern void cache_cleanup_lock(void);
extern int cache_init_lock(void);
extern int cache_get_wlock(unsigned int);
extern int cache_get_rlock(unsigned int);
extern int cache_un_lock(unsigned int);

#endif  /* _CACHE_H */

