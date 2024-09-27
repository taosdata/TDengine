/*****************************************************************************
 *
 * cache.c
 *
 * Description:  Implements a credentail caching layer to ease the loading 
 *               on the authentication mechanisms.
 *
 * Copyright (C) 2003 Jeremy Rumpf
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
 * Jeremy Rumpf
 * jrumpf@heavyload.net
 *
 *****************************************************************************/

/****************************************
 * includes
 *****************************************/
#include <config.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>

#include "cache.h"
#include "utils.h"
#include "globals.h"
#include "md5global.h"
#include "saslauthd_md5.h"

/****************************************
 * module globals
 *****************************************/
static  struct mm_ctl	mm;
static  struct lock_ctl	lock;
static  struct bucket	*table = NULL;
static  struct stats	*table_stats = NULL;
static  unsigned int	table_size = 0;
static  unsigned int	table_timeout = 0;

/****************************************
 * flags               global from saslauthd-main.c
 * run_path            global from saslauthd-main.c
 * tx_rec()            function from utils.c
 * logger()            function from utils.c
 *****************************************/

/*************************************************************
 * The initialization function. This function will setup
 * the hash table's memory region, initialize the table, etc.
 **************************************************************/
int cache_init(void) {
	int		bytes;
	char		cache_magic[64];
	void		*base;

	if (!(flags & CACHE_ENABLED))
		return 0;

	memset(cache_magic, 0, sizeof(cache_magic));
	strlcpy(cache_magic, CACHE_CACHE_MAGIC, sizeof(cache_magic));

	/**************************************************************
	 * Compute the size of the hash table. This and a stats 
	 * struct will make up the memory region. 
	 **************************************************************/

	if (table_size == 0)
		table_size = CACHE_DEFAULT_TABLE_SIZE;

	bytes = (table_size * CACHE_MAX_BUCKETS_PER * sizeof(struct bucket))
		+ sizeof(struct stats) + 256;


	if ((base = cache_alloc_mm(bytes)) == NULL)
		return -1;

	if (table_timeout == 0)
		table_timeout = CACHE_DEFAULT_TIMEOUT;

	if (flags & VERBOSE) {
		logger(L_DEBUG, L_FUNC, "bucket size: %d bytes",
		       sizeof(struct bucket));
		logger(L_DEBUG, L_FUNC, "stats size : %d bytes",
		       sizeof(struct stats));
		logger(L_DEBUG, L_FUNC, "timeout    : %d seconds",
		       table_timeout);
		logger(L_DEBUG, L_FUNC, "cache table: %d total bytes",
		       bytes);
		logger(L_DEBUG, L_FUNC, "cache table: %d slots",
		       table_size);
		logger(L_DEBUG, L_FUNC, "cache table: %d buckets",
		       table_size * CACHE_MAX_BUCKETS_PER);
	} 

	/**************************************************************
	 * At the top of the region is the magic and stats struct. The
	 * slots follow. Due to locking, the counters in the stats
	 * struct will not be entirely accurate.
	 **************************************************************/

	memset(base, 0, bytes);

	memcpy(base, cache_magic, 64);
	table_stats = (void *)((char *)base + 64);
	table_stats->table_size = table_size;
	table_stats->max_buckets_per = CACHE_MAX_BUCKETS_PER;
	table_stats->sizeof_bucket = sizeof(struct bucket);
	table_stats->timeout = table_timeout;
	table_stats->bytes = bytes;

	table = (void *)((char *)table_stats + 128);

	/**************************************************************
	 * Last, initialize the hash table locking.
	 **************************************************************/

	if (cache_init_lock() != 0)
		return -1;

	return 0;
}	

/*************************************************************
 * Here we'll take some credentials and run them through
 * the hash table. If we have a valid hit then all is good
 * return CACHE_OK. If we don't get a hit, write the entry to
 * the result pointer and expect a later call to
 * cache_commit() to flush the bucket into the table.
 **************************************************************/
int cache_lookup(const char *user, const char *realm, const char *service, const char *password, struct cache_result *result) {

	int			user_length = 0;
	int			realm_length = 0;
	int			service_length = 0;
	int			hash_offset;
	unsigned char		pwd_digest[16];
	MD5_CTX			md5_context;
	time_t			epoch;
	time_t			epoch_timeout;
	struct bucket		*ref_bucket;
	struct bucket		*low_bucket;
	struct bucket		*high_bucket;
	struct bucket		*read_bucket = NULL;
	char			userrealmserv[CACHE_MAX_CREDS_LENGTH];
	static char		*debug = "[login=%s] [service=%s] [realm=%s]: %s";


	if (!(flags & CACHE_ENABLED))
		return CACHE_FAIL;

	memset((void *)result, 0, sizeof(struct cache_result));
	result->status = CACHE_NO_FLUSH;
	
	/**************************************************************
	 * Initial length checks
	 **************************************************************/

	user_length = strlen(user) + 1;
	realm_length = strlen(realm) + 1;
	service_length = strlen(service) + 1;

	if ((user_length + realm_length + service_length) > CACHE_MAX_CREDS_LENGTH) {
		return CACHE_TOO_BIG;
	}

	/**************************************************************
	 * Any ideas on how not to call time() for every lookup?
	 **************************************************************/

	epoch = time(NULL);
	epoch_timeout = epoch - table_timeout;

	/**************************************************************
	 * Get the offset into the hash table and the md5 sum of
	 * the password.
	 **************************************************************/

	strlcpy(userrealmserv, user, sizeof(userrealmserv));
	strlcat(userrealmserv, realm, sizeof(userrealmserv));
	strlcat(userrealmserv, service, sizeof(userrealmserv));

	hash_offset = cache_pjwhash(userrealmserv);

	_saslauthd_MD5Init(&md5_context);
	_saslauthd_MD5Update(&md5_context, password, strlen(password));
	_saslauthd_MD5Final(pwd_digest, &md5_context);

	/**************************************************************
	 * Loop through the bucket chain to try and find a hit.
	 *
	 * low_bucket = bucket at the start of the slot.
	 *
	 * high_bucket = last bucket in the slot.
	 * 
	 * read_bucket = Contains the matched bucket if found. 
	 *               Otherwise is NULL.
	 *
	 * Also, lock the slot first to avoid contention in the 
	 * bucket chain.
	 *
	 **************************************************************/

	table_stats->attempts++;

	if (cache_get_rlock(hash_offset) != 0) {
		table_stats->misses++;
		table_stats->lock_failures++;
		return CACHE_FAIL;
	}	

	low_bucket = table + (CACHE_MAX_BUCKETS_PER * hash_offset);
	high_bucket = low_bucket + CACHE_MAX_BUCKETS_PER;

	for (ref_bucket = low_bucket; ref_bucket < high_bucket; ref_bucket++) {
		if (strcmp(user, ref_bucket->creds + ref_bucket->user_offt) == 0 &&
		    strcmp (realm, ref_bucket->creds + ref_bucket->realm_offt) == 0 &&
		    strcmp(service, ref_bucket->creds + ref_bucket->service_offt) == 0 &&
                    ref_bucket->created > epoch_timeout) {
			read_bucket = ref_bucket;
			break;
		}
	}

	/**************************************************************
	 * If we have our fish, check the password. If it's good,
	 * release the slot (row) lock and return CACHE_OK. Else,
	 * we'll write the entry to the result pointer. If we have a
	 * read_bucket, then tell cache_commit() to not rescan the 
	 * chain (CACHE_FLUSH). Else, have cache_commit() determine the
	 * best bucket to place the new entry (CACHE_FLUSH_WITH_RESCAN).
	 **************************************************************/

	if (read_bucket != NULL) {

		if (memcmp(pwd_digest, read_bucket->pwd_digest, 16) == 0) {

			if (flags & VERBOSE)
				logger(L_DEBUG, L_FUNC, debug, user, service, realm, "found with valid passwd");

			cache_un_lock(hash_offset);
			table_stats->hits++;
			return CACHE_OK;
		}

		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, debug, user, service, realm, "found with invalid passwd, update pending");

		result->status = CACHE_FLUSH;

	} else {

		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, debug, user, service, realm, "not found, update pending");

		result->status = CACHE_FLUSH_WITH_RESCAN;
	}

	result->hash_offset = hash_offset;
	result->read_bucket = read_bucket;
	
	result->bucket.user_offt = 0;
	result->bucket.realm_offt = user_length;
	result->bucket.service_offt = user_length + realm_length;

	strcpy(result->bucket.creds + result->bucket.user_offt, user);	
	strcpy(result->bucket.creds + result->bucket.realm_offt, realm);	
	strcpy(result->bucket.creds + result->bucket.service_offt, service);	

	memcpy(result->bucket.pwd_digest, pwd_digest, 16);
	result->bucket.created = epoch;

	cache_un_lock(hash_offset);
	table_stats->misses++;
	return CACHE_FAIL;
}


/*************************************************************
 * If it was later determined that the previous failed lookup
 * is ok, flush the result->bucket out to it's permanent home
 * in the hash table. 
 **************************************************************/
void cache_commit(struct cache_result *result) {
	struct bucket           *write_bucket;
	struct bucket		*ref_bucket;
	struct bucket		*low_bucket;
	struct bucket		*high_bucket;

	if (!(flags & CACHE_ENABLED))
		return;

	if (result->status == CACHE_NO_FLUSH)
		return;

	if (cache_get_wlock(result->hash_offset) != 0) {
		table_stats->lock_failures++;
		return;
	}	

	if (result->status == CACHE_FLUSH) {
		write_bucket = result->read_bucket;
	} else {
		/*********************************************************
		 * CACHE_FLUSH_WITH_RESCAN is the default action to take.
	 	 * Simply traverse the slot looking for the oldest bucket
		 * and mark it for writing.
	 	 **********************************************************/
		low_bucket = table + (CACHE_MAX_BUCKETS_PER * result->hash_offset);
		high_bucket = low_bucket + CACHE_MAX_BUCKETS_PER;
		write_bucket = low_bucket;

		for (ref_bucket = low_bucket; ref_bucket < high_bucket; ref_bucket++) {
			if (ref_bucket->created < write_bucket->created) 
				write_bucket = ref_bucket;
		}
	}

	memcpy((void *)write_bucket, (void *)&(result->bucket), sizeof(struct bucket));

	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "lookup committed");

	cache_un_lock(result->hash_offset);
	return;
}


/*************************************************************
 * Hashing function. Algorithm is an adaptation of Peter
 * Weinberger's (PJW) generic hashing algorithm, which
 * is based on Allen Holub's version. 
 **************************************************************/
int cache_pjwhash(char *datum ) {
    const int BITS_IN_int = ( sizeof(int) * CHAR_BIT );
    const int THREE_QUARTERS = ((int) ((BITS_IN_int * 3) / 4));
    const int ONE_EIGHTH = ((int) (BITS_IN_int / 8));
    const int HIGH_BITS = ( ~((unsigned int)(~0) >> ONE_EIGHTH ));
    
    unsigned int            hash_value, i;
    
    for (hash_value = 0; *datum; ++datum) {
	hash_value = (hash_value << ONE_EIGHTH) + *datum;
	if ((i = hash_value & HIGH_BITS) != 0)
	    hash_value = (hash_value ^ (i >> THREE_QUARTERS)) & ~HIGH_BITS;
    }
    
    return (hash_value % table_size);
}

/*************************************************************
 * Allow someone to set the hash table size (in kilobytes).
 * Since the hash table has to be prime, this won't be exact.
 **************************************************************/
void cache_set_table_size(const char *size) {
	unsigned int	kilobytes;
	unsigned int	bytes;
	unsigned int	calc_bytes = 0;
	unsigned int	calc_table_size = 1;

	kilobytes = strtol(size, (char **)NULL, 10);

	if (kilobytes <= 0) {
		logger(L_ERR, L_FUNC,
		       "cache size must be positive and non zero");
		exit(1);
	}

	bytes = kilobytes * 1024;

	calc_table_size =
	    bytes / (sizeof(struct bucket) * CACHE_MAX_BUCKETS_PER);

	do {
	    calc_table_size = cache_get_next_prime(calc_table_size);
	    calc_bytes = calc_table_size *
		sizeof(struct bucket) * CACHE_MAX_BUCKETS_PER; 
	} while (calc_bytes < bytes);

	table_size = calc_table_size;

	return;
}


/*************************************************************
 * Allow someone to set the table timeout (in seconds)
 **************************************************************/
void cache_set_timeout(const char *time) {
	table_timeout = strtol(time, (char **)NULL, 10);

	if (table_timeout <= 0) {
		logger(L_ERR, L_FUNC, "cache timeout must be positive");
		exit(1);
	}

	return;
}


/*************************************************************
 * Find the next closest prime relative to the number given.
 * This is a variation of an implementation of the 
 * Sieve of Erastothenes by Frank Pilhofer,
 * http://www.fpx.de/fp/Software/Sieve.html. 
 **************************************************************/
unsigned int cache_get_next_prime(unsigned int number) {

#define TEST(f,x)	(*(f+((x)>>4))&(1<<(((x)&15L)>>1)))
#define SET(f,x)        *(f+((x)>>4))|=1<<(((x)&15)>>1)

	unsigned char	*feld = NULL;
	unsigned int	teste = 1;
	unsigned int	max;
	unsigned int	mom;
	unsigned int	alloc;

	max = number + 20000;

	feld = malloc(alloc=(((max-=10000)>>4)+1));

	if (feld == NULL) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		exit(1);
	}

	memset(feld, 0, alloc);

	while ((teste += 2) < max) {
		if (!TEST(feld, teste)) {
			if (teste > number) {
				free(feld);
				return teste;
			}

			for (mom=3*teste; mom<max; mom+=teste<<1) SET (feld, mom);
		}
	}

	/******************************************************
	 * A prime wasn't found in the maximum search range.
	 * Just return the original number.
	 ******************************************************/
	
	free(feld);
	return number;
}


/*************************************************************
 * Open the file that we'll mmap in as the shared memory
 * segment. If something fails, return NULL.
 **************************************************************/
void *cache_alloc_mm(unsigned int bytes) {
	int		file_fd;
	int		rc;
	int		chunk_count;
	char		null_buff[1024];
	size_t          mm_file_len;
	
	mm.bytes = bytes;

	mm_file_len = strlen(run_path) + sizeof(CACHE_MMAP_FILE) + 1;
	if (!(mm.file =
	     (char *)malloc(mm_file_len))) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		return NULL;
	}

	strlcpy(mm.file, run_path, mm_file_len);
	strlcat(mm.file, CACHE_MMAP_FILE, mm_file_len);
	
	if ((file_fd =
	     open(mm.file, O_RDWR|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR)) < 0) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not open mmap file: %s", mm.file);
		logger(L_ERR, L_FUNC, "open: %s", strerror(rc));
		return NULL;
	}

	memset(null_buff, 0, sizeof(null_buff));

	chunk_count = (bytes / sizeof(null_buff)) + 1;

	while (chunk_count > 0) {
	    if (tx_rec(file_fd, null_buff, sizeof(null_buff))
		!= (ssize_t)sizeof(null_buff)) {
		rc = errno;
		logger(L_ERR, L_FUNC,
		       "failed while writing to mmap file: %s",
		       mm.file);
		close(file_fd);
		return NULL;
	    }
	    
	    chunk_count--;
	}	
	
	if ((mm.base = mmap(NULL, bytes, PROT_READ|PROT_WRITE,
			    MAP_SHARED, file_fd, 0))== (void *)-1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not mmap shared memory segment");
		logger(L_ERR, L_FUNC, "mmap: %s", strerror(rc));
		close(file_fd);
		return NULL;
	}

	close(file_fd);

	if (flags & VERBOSE) {
		logger(L_DEBUG, L_FUNC,
		       "mmaped shared memory segment on file: %s", mm.file);
	}

	return mm.base;
}


/*************************************************************
 * When we die we may need to perform some cleanup on the
 * mmaped region. We assume we're the last process out here.
 * Otherwise, deleting the file may cause SIGBUS signals to
 * be generated for other processes.
 **************************************************************/
void cache_cleanup_mm(void) {
	if (mm.base != NULL) {
		munmap(mm.base, mm.bytes);
		unlink(mm.file);

		if (flags & VERBOSE) {
			logger(L_DEBUG, L_FUNC,
			       "cache mmap file removed: %s", mm.file);
		}
	}

	return;
}

/*****************************************************************
 * The following is relative to the fcntl() locking method. Probably
 * used when the Sys IV SHM Implementation is in effect.
 ****************************************************************/
#ifdef CACHE_USE_FCNTL

/*************************************************************
 * Setup the locking stuff required to implement the fcntl()
 * style record locking of the hash table. Return 0 if
 * everything is peachy, otherwise -1.
 * __FCNTL Impl__
 **************************************************************/
int cache_init_lock(void) {
	int	rc;
	size_t  flock_file_len;

	flock_file_len = strlen(run_path) + sizeof(CACHE_FLOCK_FILE) + 1;
	if ((lock.flock_file = (char *)malloc(flock_file_len)) == NULL) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		return -1;
	}

	strlcpy(lock.flock_file, run_path, flock_file_len);
	strlcat(lock.flock_file, CACHE_FLOCK_FILE, flock_file_len);

	if ((lock.flock_fd = open(lock.flock_file, O_RDWR|O_CREAT|O_TRUNC, S_IWUSR|S_IRUSR)) == -1) {
		rc = errno;
		logger(L_ERR, L_FUNC, "could not open flock file: %s", lock.flock_file);
		logger(L_ERR, L_FUNC, "open: %s", strerror(rc));
		return -1;
	}

	if (flags & VERBOSE) 
		logger(L_DEBUG, L_FUNC, "flock file opened at %s", lock.flock_file);

	return 0;
}


/*************************************************************
 * When the processes die we'll need to cleanup/delete
 * the flock_file. More for correctness than anything.
 * __FCNTL Impl__
 **************************************************************/
void cache_cleanup_lock(void) {


	if (lock.flock_file != NULL) {
		unlink(lock.flock_file);

		if (flags & VERBOSE) 
			logger(L_DEBUG, L_FUNC, "flock file removed: %s", lock.flock_file);

	}

	return;
}


/*************************************************************
 * Attempt to get a write lock on a slot. Return 0 if 
 * everything went ok, return -1 if something bad happened.
 * This function is expected to block.
 * __FCNTL Impl__
 **************************************************************/
int cache_get_wlock(unsigned int slot) {
	struct flock	lock_st;
	int		rc;

	lock_st.l_type = F_WRLCK;
	lock_st.l_start = slot;
	lock_st.l_whence = SEEK_SET;
	lock_st.l_len = 1;

	errno = 0;

	do {
		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, "attempting a write lock on slot: %d", slot);

		rc = fcntl(lock.flock_fd, F_SETLKW, &lock_st);
	} while (rc != 0 && errno == EINTR);

	if (rc != 0) {	
		rc = errno;
		logger(L_ERR, L_FUNC, "could not acquire a write lock on slot: %d\n", slot);
		logger(L_ERR, L_FUNC, "fcntl: %s", strerror(rc));
		return -1;
	}

	return 0;
}


/*************************************************************
 * Attempt to get a read lock on a slot. Return 0 if 
 * everything went ok, return -1 if something bad happened.
 * This function is expected to block.
 * __FCNTL Impl__
 **************************************************************/
int cache_get_rlock(unsigned int slot) {

	struct flock	lock_st;
	int		rc;


	lock_st.l_type = F_RDLCK;
	lock_st.l_start = slot;
	lock_st.l_whence = SEEK_SET;
	lock_st.l_len = 1;

	errno = 0;

	do {
		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, "attempting a read lock on slot: %d", slot);

		rc = fcntl(lock.flock_fd, F_SETLKW, &lock_st);
	} while (rc != 0 && errno == EINTR);

	if (rc != 0) {	
		rc = errno;
		logger(L_ERR, L_FUNC, "could not acquire a read lock on slot: %d\n", slot);
		logger(L_ERR, L_FUNC, "fcntl: %s", strerror(rc));
		return -1;
	}

	return 0;
}


/*************************************************************
 * Releases a previously acquired lock on a slot.
 * __FCNTL Impl__
 **************************************************************/
int cache_un_lock(unsigned int slot) {

	struct flock	lock_st;
	int		rc;


	lock_st.l_type = F_UNLCK;
	lock_st.l_start = slot;
	lock_st.l_whence = SEEK_SET;
	lock_st.l_len = 1;

	errno = 0;

	do {
		if (flags & VERBOSE)
			logger(L_DEBUG, L_FUNC, "attempting to release lock on slot: %d", slot);

		rc = fcntl(lock.flock_fd, F_SETLKW, &lock_st);
	} while (rc != 0 && errno == EINTR);

	if (rc != 0) {	
		rc = errno;
		logger(L_ERR, L_FUNC, "could not release lock on slot: %d\n", slot);
		logger(L_ERR, L_FUNC, "fcntl: %s", strerror(rc));
		return -1;
	}

	return 0;
}


#endif  /* CACHE_USE_FCNTL */

/**********************************************************************
 * The following is relative to the POSIX threads rwlock method of locking
 * slots in the hash table. Used when the Doors IPC is in effect, thus
 * -lpthreads is evident.
 ***********************************************************************/

#ifdef CACHE_USE_PTHREAD_RWLOCK

/*************************************************************
 * Initialize a pthread_rwlock_t for every slot (row) in the
 * hash table. Return 0 if everything went ok, -1 if we bomb.
 * __RWLock Impl__
 **************************************************************/
int cache_init_lock(void) {
	unsigned int		x;
	pthread_rwlock_t	*rwlock;

	if (!(lock.rwlock =
	     (pthread_rwlock_t *)malloc(sizeof(pthread_rwlock_t) * table_size))) {
		logger(L_ERR, L_FUNC, "could not allocate memory");
		return -1;
	}

	for (x = 0; x < table_size; x++) {
		rwlock = lock.rwlock + x;

		if (pthread_rwlock_init(rwlock, NULL) != 0) {
			logger(L_ERR, L_FUNC, "failed to initialize lock %d", x);
			return -1;
		}
	}

	if (flags & VERBOSE) 
		logger(L_DEBUG, L_FUNC, "%d rwlocks initialized", table_size);

	return 0;
}


/*************************************************************
 * Destroy all of the rwlocks, free the buffer.
 * __RWLock Impl__
 **************************************************************/
void cache_cleanup_lock(void) {
    unsigned int x;
    pthread_rwlock_t	*rwlock;

    if(!lock.rwlock) return;
    
    for(x=0; x<table_size; x++) {
	rwlock = lock.rwlock + x;
	pthread_rwlock_destroy(rwlock);
    }
    
    free(lock.rwlock);

    return;
}


/*************************************************************
 * Attempt to get a write lock on a slot. Return 0 if 
 * everything went ok, return -1 if something bad happened.
 * This function is expected to block the current thread.
 * __RWLock Impl__
**************************************************************/
int cache_get_wlock(unsigned int slot) {

	int		rc = 0;


	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "attempting a write lock on slot: %d", slot);

	rc = pthread_rwlock_wrlock(lock.rwlock + slot);

	if (rc != 0) {	
		logger(L_ERR, L_FUNC, "could not acquire a write lock on slot: %d\n", slot);
		return -1;
	}

	return 0;
}


/*************************************************************
 * Attempt to get a read lock on a slot. Return 0 if 
 * everything went ok, return -1 if something bad happened.
 * This function is expected to block the current thread.
 * __RWLock Impl__
 **************************************************************/
int cache_get_rlock(unsigned int slot) {

	int		rc = 0;


	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "attempting a read lock on slot: %d", slot);

	rc = pthread_rwlock_rdlock(lock.rwlock + slot);

	if (rc != 0) {	
		logger(L_ERR, L_FUNC, "could not acquire a read lock on slot: %d\n", slot);
		return -1;
	}

	return 0;
}


/*************************************************************
 * Releases a previously acquired lock on a slot.
 * __RWLock Impl__
 **************************************************************/
int cache_un_lock(unsigned int slot) {

	int		rc = 0;


	if (flags & VERBOSE)
		logger(L_DEBUG, L_FUNC, "attempting to release lock on slot: %d", slot);

	rc = pthread_rwlock_unlock(lock.rwlock + slot);

	if (rc != 0) {	
		logger(L_ERR, L_FUNC, "could not release lock on slot: %d\n", slot);
		return -1;
	}

	return 0;
}


#endif  /* CACHE_USE_PTHREAD_RWLOCK */
/***************************************************************************************/
/***************************************************************************************/
