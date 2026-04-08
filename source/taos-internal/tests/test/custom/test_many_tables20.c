/* Compile: gcc -o test_many_tables test_many_tables.c -g -ltaos -lpthread -std=gnu99
 *
 * Format: ./test_many_tables ip db_name tb_prefix [ntables] [nconnections] [nrecords_per_table] [nrecords_per_request]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <semaphore.h>
#include "tutil.h"

#include "taos.h"

#define BUFFER_SIZE 8192
#define MAX_DB_NAME_SIZE 64
#define MAX_TB_NAME_SIZE 64

enum MODE{SYNC, ASYNC};
typedef struct {
    TAOS * taos;
    int threadID;
    char db_name[MAX_DB_NAME_SIZE];
    char tb_prefix[MAX_TB_NAME_SIZE];
    int start_table_id;
    int end_table_id;
    int nrecords_per_table;
    int nrecords_per_request;
    long start_time;
    long time_interval;
    sem_t mutex_sem;
    int notFinished;
    sem_t lock_sem;
} info;

typedef struct {
    TAOS * taos;
    char tb_name[MAX_TB_NAME_SIZE];
    long timestamp;
    int target;
    int counter;
    int nrecords_per_request;
    sem_t * mutex_sem;
    int * notFinished;
    sem_t * lock_sem;
} sTable;

void queryDB(TAOS * taos, char * command);
void * sync_write(void * sarg);
double getCurrentTime();
FILE  *logFp = NULL;

const char* getTimeString()
{
	int64_t timeMs = taosGetTimestampMs();
	time_t tt = timeMs / 1000;
	static char buf[25] = { 0 };
	struct tm  *ptm;
	ptm = localtime(&tt);
	strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);
	return buf;
}

int main(int argc, char * argv[]) {
    char * db_name = "db";
    char * tb_prefix = "card";
    char * ip_addr = "192.168.151.71";
    int    ntables = 2500;
    int    nrecords_per_table = 30;
    int    nrecords_per_request = 150;
    int    nconnections = 5;
    long   time_interval = 10000;
	int    blocks = 32368;
    char  command[BUFFER_SIZE] = "\0";
	logFp = fopen("result.log", "w");
	int replica = 2;

	if (argc == 1) {
		printf("usage: %s replica cacheSize rowNum\n", argv[0]);
		fprintf(logFp, "usage: %s replica cacheSize rowNum\n", argv[0]);
	}

	fflush(logFp);

	// a simple way to parse input parameters
	if (argc >= 2) replica = atoi(argv[1]);
	if (argc >= 3) blocks = atoi(argv[2]);
	if (argc >= 4) nrecords_per_table = atoi(argv[3]);

	printf("replica:%d cacheSize:%d rowNum:%d\n", replica, blocks, nrecords_per_table);
	fprintf(logFp, "replica:%d cacheSize:%d rowNum:%d\n", replica, blocks, nrecords_per_table);
	fflush(logFp);

    taos_init();
    TAOS * taos = taos_connect(ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        fprintf(logFp, "Failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
		printf("Failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    sprintf(command, "create database if not exists %s replica %d cache %d ablocks 4 tblocks 200 tables 500", db_name, replica, blocks);
    taos_query(taos, command);

    sprintf(command, "use %s", db_name);
    taos_query(taos, command);

    sprintf(command, "create table mt (ts timestamp, v1 int, v2 int, v3 int, v4 int, v5 int, v6 int, v7 int, v8 int, v9 int, v10 int, v11 int, v12 int, v13 int, v14 int, v15 int, v16 int, v17 int, v18 int, v19 int, v20 int) tags (t int)");
    taos_query(taos, command);
    
	fprintf(logFp, "%s Creating %d tables......\n", getTimeString(), ntables);
	printf("%s Creating %d tables......\n", getTimeString(), ntables);
    for (int i = 0; i < ntables; i++) {
        sprintf(command, "create table if not exists %s%d using mt tags (%d)", tb_prefix, i, i);
        queryDB(taos, command);
    }

    fprintf(logFp, "%s Tables created!\n", getTimeString());
	printf("%s Tables created!\n", getTimeString());
    taos_close(taos);
	fflush(logFp);

    /* exit(0); */

    /* Wait for data to create  */
    sleep(2);

    /* Insert data */
    double ts = getCurrentTime();
	fprintf(logFp, "%s Inserting data......\ \n", getTimeString());
	printf("%s Inserting data......\ \n", getTimeString());

    pthread_t * pids = taosMemoryMalloc(nconnections * sizeof(pthread_t));
    info * infos = taosMemoryMalloc(nconnections * sizeof(info));
    int a = ntables / nconnections;
    int b = ntables % nconnections;
    int last = 0;
    for (int i = 0; i < nconnections; i++) {
        info * t_info = infos+i;
        t_info->threadID = i;
        strcpy(t_info->db_name, db_name);
        strcpy(t_info->tb_prefix, tb_prefix);
        t_info->nrecords_per_table = nrecords_per_table;
        t_info->start_time = 1493568000000;
        t_info->time_interval = time_interval;
        t_info->taos = taos_connect(ip_addr, "root", "taosdata", db_name, 0);
        t_info->nrecords_per_request = nrecords_per_request;
        t_info->start_table_id = last;
        t_info->end_table_id = i < b? last + a: last + a - 1;
        last = t_info->end_table_id + 1;

        sem_init(&(t_info->mutex_sem), 0, 1);
        t_info->notFinished = t_info->end_table_id - t_info->start_table_id + 1;
        sem_init(&(t_info->lock_sem), 0, 0);

        /* if (query_mode == SYNC) { */
        pthread_create(pids+i, NULL, sync_write, t_info);
        /* } */
        /* else { */
        /*     pthread_create(pids+i, NULL, async_write, t_info); */
        /* } */
    }
    for (int i = 0; i < nconnections; i++){
        pthread_join(pids[i], NULL);
    }

    double t = getCurrentTime() - ts;
	fprintf(logFp, "%s Done! Spent %10.4f seconds to insert %ld records: %12.2f R/s\n", getTimeString(), t, 1L * ntables*nrecords_per_table, (1L * ntables*nrecords_per_table) / t);
    printf("%s Done! Spent %10.4f seconds to insert %ld records: %12.2f R/s\n", getTimeString(), t, 1L * ntables*nrecords_per_table, (1L*ntables*nrecords_per_table)/t);
	fflush(logFp);
	fclose(logFp);
    for (int i = 0; i < nconnections; i++){
        info * t_info = infos + i;

        sem_destroy(&(t_info->mutex_sem));
        sem_destroy(&(t_info->lock_sem));
        taos_close(t_info->taos);
    }

    taosMemoryFree(pids);
    taosMemoryFree(infos);

    return 0;
}

void queryDB(TAOS * taos, char * command) {
    if (taos_query(taos, command) != 0) {
        fprintf(logFp, "Failed to run %s, reason: %s\n", command, taos_errstr(taos) );
		printf("Failed to run %s, reason: %s\n", command, taos_errstr(taos));
        taos_close(taos);
        exit(EXIT_FAILURE);
    }
}

int randValue() {
    return rand() % 5;
}

// sync insertion
void * sync_write(void * sarg) {
    info * winfo = (info * )sarg;
    char * buffer = taosMemoryMalloc(65536);
    char *pStr = NULL;
    int   count = 0;
    double st = 0;
    double et = 0;

    srand(time(NULL));
    long time_counter=winfo->start_time;
	fprintf(logFp, "%s ThreadID: %d start table ID: %d end table ID: %d\n", getTimeString(), winfo->threadID, winfo->start_table_id, winfo->end_table_id);
    printf("%s ThreadID: %d start table ID: %d end table ID: %d\n", getTimeString(), winfo->threadID, winfo->start_table_id, winfo->end_table_id);

    pStr = buffer;
    pStr += sprintf(pStr, "insert into ");

    for (int i = 0; i < winfo->nrecords_per_table; i++){
        time_counter = winfo->start_time + i * winfo->time_interval;

        for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++){

            int val = randValue();
            pStr += sprintf(pStr, "%s%d values (%ld, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d) ", winfo->tb_prefix, tID, time_counter, val+0, val+10, val+20, val+30, val+40, val+50, val+60, val+70, val+90, val+100, val+110, val+120, val+130, val+140, val+150, val+160, val+170, val+180, val+190, val+200);

            count++;

            if (count >= winfo->nrecords_per_request) {
                queryDB(winfo->taos, buffer);
                
                count = 0;
                pStr = buffer;
                pStr += sprintf(pStr, "insert into ");
            }
        }
    }

	if (count > 0) {
		queryDB(winfo->taos, buffer);
	}

    taosMemoryFree(buffer);

    return NULL;
}

double getCurrentTime(){
    struct timeval tv;   
    if (gettimeofday(&tv, NULL) != 0) {
        perror("Failed to get current time in ms");
        exit(EXIT_FAILURE);  
    }           

    return tv.tv_sec + tv.tv_usec / 1E6;
}

