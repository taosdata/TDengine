/*******************************************************************************
 * *****************************************************************************
 * *
 * * saslcache.c
 * *
 * * Description:  A small utility that can attach to saslauthd's shared
 * *               memory region and display/dump information in the cache.
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

/****************************************
* * includes
*****************************************/
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "cache.h"


/****************************************
* * declarations/protos
*****************************************/
void	show_usage(void);
void	dump_cache_stats(void);
void	dump_cache_users(void);
char	*make_time(time_t);

/****************************************
* * module globals
*****************************************/
static  void            *shm_base = NULL;
static  struct bucket   *table = NULL;
static  struct stats    *table_stats = NULL;

/****************************************
*****************************************/


/*************************************************************
* * Main
**************************************************************/
int main(int argc, char **argv) {

	int		option;
	int		dump_user_info = 0;
	int		dump_stat_info = 0;
	char   		*file = NULL;
	int		file_fd;
	char		cache_magic[64];
	struct stat 	stat_buff;

	while ((option = getopt(argc, argv, "dm:s")) != -1) {
		switch(option) {

			case 'd':
				dump_user_info = 1;
				break;

			case 's':
				dump_stat_info = 1;
				break;

			case 'm':
				file = strdup(optarg);
				break;

			default:
				show_usage();
		}
	}

	if (file == NULL)
		file = PATH_SASLAUTHD_RUNDIR "/cache.mmap";

	if (stat(file, &stat_buff) == -1) {
		fprintf(stderr, "could not stat mmap file: %s\n", file);
		fprintf(stderr, "stat: %s\n", strerror(errno));
		exit(1);
	}

	if ((file_fd = open(file, O_RDONLY)) < 0) {
		fprintf(stderr, "could not open mmap file: %s\n", file);
		fprintf(stderr, "open: %s\n", strerror(errno));
		fprintf(stderr, "perhaps saslcache -m <path>\n");
		exit(1);
	}

	if ((shm_base = mmap(NULL, stat_buff.st_size, PROT_READ, MAP_SHARED, file_fd, 0))== (void *)-1) {
		fprintf(stderr, "could not mmap shared memory file: %s\n", file);
		fprintf(stderr, "mmap: %s\n", strerror(errno));
		exit(1);
	}	

	memcpy(cache_magic, shm_base, 64);
	cache_magic[63] = '\0';

	if (strcmp(cache_magic, CACHE_CACHE_MAGIC) != 0) {
		fprintf(stderr, "mmap file [%s] is not a valid saslauthd cache\n", file);
		exit(1);
	}

	table_stats = shm_base + 64;
	table = (void *)((char *)table_stats + 128);

	if (dump_stat_info == 0 && dump_user_info == 0)
		dump_stat_info = 1;

	if (dump_stat_info)
		dump_cache_stats();

	if (dump_user_info)
		dump_cache_users();

	exit(0);	
}


/****************************************************
* * Dump a delimited record for each item in the
* * cache to stdout.
****************************************************/
void dump_cache_users(void) {

	unsigned int		x;
	struct bucket		*ref_bucket;
        time_t			epoch_to;

	epoch_to = time(NULL) - table_stats->timeout;

	fprintf(stdout, "\"user\",\"realm\",\"service\",\"created\",\"created_localtime\"\n");

	for (x = 0; x < (table_stats->table_size * table_stats->max_buckets_per); x++) {

		ref_bucket = table + x;

		if (ref_bucket->created > epoch_to && *(ref_bucket->creds) != '\0') {
			fprintf(stderr, "\"%s\",", ref_bucket->creds + ref_bucket->user_offt);
			fprintf(stderr, "\"%s\",", ref_bucket->creds + ref_bucket->realm_offt);
			fprintf(stderr, "\"%s\",", ref_bucket->creds + ref_bucket->service_offt);
			fprintf(stderr, "\"%lu\",", ref_bucket->created);
			fprintf(stderr, "\"%s\"\n", make_time(ref_bucket->created));
		}
	}
}

/****************************************************
* * Dump some usage statistics about the cred cache.
* * (clean this up someday)
****************************************************/
void dump_cache_stats(void) {

        unsigned int		x, y, z;
        float			a;
        unsigned int		max_chain_length = 0;
        unsigned int		min_chain_length = 0;
        unsigned int		buckets_in_use = 0;
        unsigned int		slots_in_use = 0;
        unsigned int		slots_max_chain = 0;
        unsigned int		slots_min_chain = 0;
        time_t			epoch_to;


	min_chain_length = table_stats->max_buckets_per;
	epoch_to = time(NULL) - table_stats->timeout;

	for (x = 0; x < table_stats->table_size; x++) {

		z = 0;

		for (y = (x * table_stats->max_buckets_per); y < ((x + 1) * table_stats->max_buckets_per); y++) { 
			if (table[y].created > epoch_to) {
				buckets_in_use++;
				z++;
			}
		}

		if (z == min_chain_length)
			slots_min_chain++;

		if (z == max_chain_length)
			slots_max_chain++;

		if (z > 0)
			slots_in_use++;

		if (z > max_chain_length) {
			max_chain_length = z;
			slots_max_chain = 1;
		}

		if (z < min_chain_length) {
			min_chain_length = z;
			slots_min_chain = 1;
		}
	}

	fprintf(stdout, "----------------------------------------\n");
	fprintf(stdout, "Saslauthd Cache Detail:\n");
	fprintf(stdout, "\n");
	fprintf(stdout, "  timeout (seconds)           :  %d\n", table_stats->timeout);
	fprintf(stdout, "  total slots allocated       :  %d\n", table_stats->table_size);
	fprintf(stdout, "  slots in use                :  %d\n", slots_in_use);
	fprintf(stdout, "  total buckets               :  %d\n", (table_stats->max_buckets_per * table_stats->table_size));
	fprintf(stdout, "  buckets per slot            :  %d\n", table_stats->max_buckets_per);
	fprintf(stdout, "  buckets in use              :  %d\n", buckets_in_use);
	fprintf(stdout, "  hash table size (bytes)     :  %d\n", table_stats->bytes);
	fprintf(stdout, "  bucket size (bytes)         :  %d\n", table_stats->sizeof_bucket);
	fprintf(stdout, "  minimum slot allocation     :  %d\n", min_chain_length);
	fprintf(stdout, "  maximum slot allocation     :  %d\n", max_chain_length);
	fprintf(stdout, "  slots at maximum allocation :  %d\n", slots_max_chain);
	fprintf(stdout, "  slots at minimum allocation :  %d\n", slots_min_chain);

	if (table_stats->table_size == 0)
		a = 0;
	else
		a = slots_in_use / (float)table_stats->table_size;

	fprintf(stdout, "  overall hash table load     :  %0.2f\n", a);
	fprintf(stdout, "\n");
	fprintf(stdout, "  hits*                       :  %d\n", table_stats->hits);
	fprintf(stdout, "  misses*                     :  %d\n", table_stats->misses);
	fprintf(stdout, "  total lookup attempts*      :  %d\n", table_stats->attempts);

	if (table_stats->attempts == 0)
		a = 0;
	else
		a = (table_stats->hits / (float)table_stats->attempts) * 100;

	fprintf(stdout, "  hit ratio*                  :  %0.2f\n", a);
	fprintf(stdout, "  flock failures*             :  %d\n", table_stats->lock_failures);
	fprintf(stdout, "----------------------------------------\n");
	fprintf(stdout, "* May not be completely accurate\n");
	fprintf(stdout, "----------------------------------------\n\n");
}


/**************************************************
* * Create a human readable time representation
****************************************************/
char	*make_time(time_t epoch) {

	static char	created_str[128];
	struct	tm	*tm_st = NULL;


	tm_st = localtime(&epoch);	

	if (tm_st == NULL) 
		return "unknown";

	strftime(created_str, 127, "%c", tm_st);
	created_str[127] = '\0';

	return created_str;
}


/**************************************************
* * Dump out the usage information and exit
****************************************************/
void show_usage(void) {

    fprintf(stderr, "usage: saslcache [options]\n\n");
    fprintf(stderr, "option information:\n");
    fprintf(stderr, "  -d             Dumps a csv list of information in the cache.\n");
    fprintf(stderr, "  -f             Purges all entries from the cache.\n");
    fprintf(stderr, "  -m <path>      Alternate path to the cache.mmap file.\n");
    fprintf(stderr, "                 Defaults to: %s\n", PATH_SASLAUTHD_RUNDIR "/cache.mmap");
    fprintf(stderr, "  -s             Dumps general statistic information about the cache.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "  All data is delivered to stdout.\n");

    exit(1);

}

