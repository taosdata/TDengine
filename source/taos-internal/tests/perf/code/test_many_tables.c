/* Compile: gcc -o test_many_tables test_many_tables.c -g -ltaos -lpthread -std=gnu99
 *
 * Format: ./test_many_tables ip db_name tb_prefix [ntables] [nconnections] [nrecords_per_table] [nrecords_per_request]
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <semaphore.h>
#include "taos.h"

#define BUFFER_SIZE 4096
#define MAX_DB_NAME_SIZE 64
#define MAX_TB_NAME_SIZE 64

/* ******************************* Structure definition*******************************  */
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


/* ******************************* Global variables*******************************  */



/* ******************************* Global functions*******************************  */
void queryDB(TAOS * taos, char * command);
void * sync_write(void * sarg);
void * async_write(void * sarg);
double getCurrentTime();
void call_back(void * param, TAOS_RES * res, int code);
void mySleep(unsigned int second);
extern void taosMsleep(int mseconds);



int main(int argc, char * argv[]) {
    if (argc < 5) {
        printf("\tFormat: ./test_many_tables configDir <0 SYNC|1 ASYNC> ip db_name tb_prefix ntables nconnections nrecords_per_table nrecords_per_request\n");
        exit(EXIT_FAILURE);
    }

    strcpy(configDir, argv[1]);
    enum MODE query_mode = atoi(argv[2]);
    char * ip_addr = argv[3];
    char * db_name = argv[4];
    char * tb_prefix = argv[5];

    int ntables = 1;
    if (argc >= 7) {
        ntables = atoi(argv[6]);
    }

    int nconnections = 1;
    if (argc >= 8) {
        nconnections = atoi(argv[7]);
    }
   
    int nrecords_per_table = 100000;
    if (argc >= 9) {
        nrecords_per_table = atoi(argv[8]);
    }

    int nrecords_per_request = 1;
    if (argc >= 10) {
        nrecords_per_request = atoi(argv[9]);
    }

    taos_init();
    TAOS * taos = taos_connect(ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        fprintf(stderr,"Failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
        taos_close(taos);
        return 1;
    }

    char command[BUFFER_SIZE] = "\0";
    sprintf(command, "create database %s", db_name);
    taos_query(taos, command);

    sprintf(command, "use %s", db_name);
    taos_query(taos, command);

    sprintf(command, "create table m (ts timestamp, age int) tags (tag1 int)");
    taos_query(taos, command);

    /* Create all the tables; */
    printf("Creating %d tables......\n", ntables);
    for (int i = 0; i < ntables; i++) {
        sprintf(command, "create table %s%d using m tags (%d)", tb_prefix, i, i);
        queryDB(taos, command);
    }

    printf("Tables created!\n");
    taos_close(taos);

    /* Wait for data to create  */
    mySleep(5);

    /* Insert data */
    double ts = getCurrentTime();
    printf("Inserting data......\n");
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
        t_info->start_time = 1500000000000;
        t_info->taos = taos_connect(ip_addr, "root", "taosdata", db_name, 0);
        t_info->nrecords_per_request = nrecords_per_request;
        t_info->start_table_id = last;
        t_info->end_table_id = i < b? last + a: last + a - 1;
        last = t_info->end_table_id + 1;

        sem_init(&(t_info->mutex_sem), 0, 1);
        t_info->notFinished = t_info->end_table_id - t_info->start_table_id + 1;
        sem_init(&(t_info->lock_sem), 0, 0);

        if (query_mode == SYNC) {
            pthread_create(pids+i, NULL, sync_write, t_info);
        }
        else {
            pthread_create(pids+i, NULL, async_write, t_info);
        }
    }
    for (int i = 0; i < nconnections; i++){
        pthread_join(pids[i], NULL);
    }

    double t = getCurrentTime() - ts;
    printf("Done! Spent %10.4f seconds to insert %d records: %12.2f R/s\n", t, ntables*nrecords_per_table, ntables*nrecords_per_table/t);

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
        fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(taos) );
        taos_close(taos);
        exit(EXIT_FAILURE);
    }
}

// sync insertion
void * sync_write(void * sarg) {
    info * winfo = (info * )sarg;
    char buffer[BUFFER_SIZE] = "\0";

    srand(time(NULL));
    long time_counter=winfo->start_time;
    for (int i = 0; i < winfo->nrecords_per_table;){

        for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++){
            int inserted = i;
            long tmp_time = time_counter;


            char * pstr = buffer;
            pstr += sprintf(pstr, "insert into %s.%s%d", winfo->db_name, winfo->tb_prefix, tID);
            int k;
            for(k = 0; k < winfo->nrecords_per_request;) {
                pstr += sprintf(pstr, " values (%ld, %d)", tmp_time++, rand()%10000);
                inserted++;
                k++;

                if (inserted >= winfo->nrecords_per_table) break;
            }

            /* puts(buffer); */
            queryDB(winfo->taos, buffer);

            if (tID == winfo->end_table_id) {
                i = inserted; 
                time_counter = tmp_time;
            }
        }
    }

    return NULL;
}

void * async_write(void * sarg) {
    info * winfo = (info *)sarg;

    sTable * tb_infos = (sTable *) taosMemoryMalloc(sizeof(sTable)*(winfo->end_table_id-winfo->start_table_id+1));

    for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {
        sTable * tb_info = tb_infos+tID-winfo->start_table_id;
        tb_info->taos = winfo->taos;
        sprintf(tb_info->tb_name, "%s.%s%d", winfo->db_name, winfo->tb_prefix, tID);
        tb_info->timestamp = winfo->start_time;
        tb_info->counter = 0;
        tb_info->target = winfo->nrecords_per_table;
        tb_info->nrecords_per_request = winfo->nrecords_per_request;
        tb_info->mutex_sem = &(winfo->mutex_sem);
        tb_info->notFinished = &(winfo->notFinished);
        tb_info->lock_sem = &(winfo->lock_sem);

        /* char buff[BUFFER_SIZE] = "\0"; */
        /* sprintf(buff, "insert into %s values (0, 0)", tb_info->tb_name); */
        /* queryDB(tb_info->taos,buff); */

        taos_query_a(winfo->taos, "show databases", call_back, tb_info);
    }

    sem_wait(&(winfo->lock_sem));
    taosMemoryFree(tb_infos);

    return NULL;
}

void call_back(void * param, TAOS_RES * res, int code){
    sTable * tb_info = (sTable *)param;

    if (code < 0) {
        fprintf(stderr, "failed to insert data %d:reason; %s\n", code, taos_errstr(tb_info->taos));
        exit(EXIT_FAILURE);
    }

    // If finished;
    if (tb_info->counter >= tb_info->target) {
        sem_wait(tb_info->mutex_sem);
        (*(tb_info->notFinished))--;
        if (*(tb_info->notFinished) == 0) sem_post(tb_info->lock_sem);
        sem_post(tb_info->mutex_sem);
        return;
    }

    char buffer[BUFFER_SIZE] = "\0";
    char * pstr = buffer;
    pstr += sprintf(pstr, "insert into %s", tb_info->tb_name);

    for (int i = 0; i < tb_info->nrecords_per_request; i++){
        pstr += sprintf(pstr, " values (%ld, %d)", tb_info->timestamp++, rand()%10000);
        tb_info->counter++;

        if (tb_info->counter >= tb_info->target) break;
    }

    taos_query_a(tb_info->taos, buffer, call_back, tb_info);

    taos_free_result(res);
}

void mySleep(unsigned int second) {
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    taosMsleep(1000*second);

    pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}


double getCurrentTime(){
    struct timeval tv;   
    if (gettimeofday(&tv, NULL) != 0) {
        perror("Failed to get current time in ms");
        exit(EXIT_FAILURE);  
    }           

    return tv.tv_sec + tv.tv_usec / 1E6;
}

