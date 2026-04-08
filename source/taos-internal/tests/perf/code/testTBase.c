/*
 * Usage:
 *
 * Drop Database : ./testTBase drop db <$(hostname) -I)>
 * Insert Data   : ./testTBase sync db ts1 10000000 15 <$(hostname -I)>
 * Read Data     : ./testTBase read db ts <$(hostname -I)>
*/


#include <sys/time.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>

#ifndef __LOCAL__
#include <taos/taos.h>
#else
#include "taos.h"
#endif


#define BUFFER_SIZE 65536    // 64K


typedef struct {
    char db_name[20];               // Database name
    char tb_name[20];               // Table name
    int  nrecords;                  // Records to insert
    int  nrecords_per_request;      // Records per request
    int  query_interval;            // Qeury interval
    int  query_size;                // Query size
    char ip_addr[64];
    long start_time;
    
} insert_info;

typedef struct{
    char db_name[20];
    char tb_name[20];
} query_info;

typedef struct {
    char ip_addr[64];
    char db_name[20];
    char tb_name[20];
    int  interval;
} monitorInfo;



double getCurrentTime();
void * syncWriteTable(void * vargs);
void rebuildDB(TAOS * taos, const char * const db_name);
void readTable(TAOS * taos, const char * const db_name, const char * const tb_name) ;
void asyncWriteTable(TAOS * taos, const char * const tb_name, const int ntables, const int nrecords_per_table);
void dropDB(TAOS * taos, const char * const db_name);
void * monitor(void * arg);




int main(int argc, char * argv[]) { 

    if (argc < 5) {
        printf("usage:\n");
        printf("\t%s\n","Format:  ./testTBase ip_addr nclients db_name table_prefix nrecords nrecords_per_request query_interval query_size is_query replica configDir");
        printf("\t%s\n","Example: ./testTBase $(hostname -I) 5 db t 50000000 100 10000000 100000 3 /etc/taos");
        /* printf("\t%s\n","Insert Data   : ./testTBase sync db ts1 10000000 15 $(hostname -I)"); */
        /* printf("\t%s\n","Read Data     : ./testTBase read db ts $(hostname -I)"); */
        exit(EXIT_FAILURE);
    }

    char * ip_addr = argv[1];
    int nclients   = atoi(argv[2]);
    char * db_name = argv[3];
    char * tb_prefix = argv[4];
    int  replica = 1;
    int  is_query = 0;

    int nrecords = 200000000;
    if (argc > 5) {
        nrecords = atoi(argv[5]);
    }

    int nrecords_per_request = 300;
    if (argc > 6) {
        nrecords_per_request = atoi(argv[6]);
    }

    int query_interval = nrecords;
    if (argc > 7) {
        query_interval = atoi(argv[7]);
    }

    int query_size = query_interval;
    if (argc > 8) {
        query_size = atoi(argv[8]);
    }
    if (argc > 9) {
        is_query = atoi(argv[9]);
    }

    if (argc > 10) {
        replica = atoi(argv[10]);
    }

    if (argc > 11) {
        strcpy(configDir, argv[11]);
    }
    

    TAOS * taos = taos_connect(ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL){
        fprintf(stderr, "failed to connect to TDengine");
        exit(EXIT_FAILURE);
    }

    char command[BUFFER_SIZE];
    sprintf(command, "create database %s replica %d keep 36500", db_name, replica);
    taos_query(taos, command);

    taos_close(taos);

    pthread_t * pids = (pthread_t *) taosMemoryMalloc(sizeof(pthread_t)*nclients);
    insert_info * infos = (insert_info *) taosMemoryMalloc(sizeof(insert_info)*nclients);

    for (int i = 0; i < nclients; i++){
        insert_info * info = infos + i;
        sprintf(info->db_name, "%s", db_name);
        sprintf(info->tb_name, "%s%d", tb_prefix, i);
        sprintf(info->ip_addr, "%s", ip_addr);
        info->nrecords = nrecords;
        info->nrecords_per_request = nrecords_per_request;
        info->query_interval = query_interval;
        info->query_size = query_size;
        info->start_time = 1514736000000;
        
        pthread_create(pids+i, NULL, syncWriteTable, info);
    }

    if (is_query) {
        pthread_t monitor_id;
        monitorInfo mInfo;
        strcpy(mInfo.ip_addr, ip_addr);
        strcpy(mInfo.db_name, db_name);
        sprintf(mInfo.tb_name, "%s%d", tb_prefix, 0);
        mInfo.interval = 3;
        pthread_create(&monitor_id, NULL, monitor, &mInfo);
        pthread_join(monitor_id, NULL);
    }

    for (int i = 0; i < nclients; i++){
        pthread_join(pids[i], NULL);
    }

    taosMemoryFree(pids);
    taosMemoryFree(infos);

    return 0;
}

void * monitor(void * arg) {
    monitorInfo * mInfo = (monitorInfo *) arg;

    TAOS * taos = taos_connect(mInfo->ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL){
        fprintf(stderr, "failed to connect to TDengine\n");
        exit(EXIT_FAILURE);
    }

    int nrecords = 0;
    char command[BUFFER_SIZE];
    sprintf(command, "select * from %s.%s", mInfo->db_name, mInfo->tb_name);
    while (1) {
        sleep(mInfo->interval);

        if (taos_query(taos, command) != 0) {
            fprintf(stderr, "Failed to query\n");
            taos_close(taos);
            exit(EXIT_FAILURE);
        }

        TAOS_RES * result = taos_use_result(taos);
        if (result == NULL) {
            fprintf(stderr, "Failed to retreive results:%s\n", taos_errstr(taos));
            taos_close(taos);
            exit(1);
        }

        int count = 0;
        TAOS_ROW row;
        while ((row = taos_fetch_row(result))) {
            count ++;
        }

        taos_free_result(result);

        if (count == nrecords) {
            printf("Monitor over! old: %d, new: %d\n", nrecords, count);
            break;
        }

        nrecords = count;
    }

    return NULL;
}

void readTable(TAOS * taos, const char * const db_name, const char * const tb_name) {
    char buffer[BUFFER_SIZE] = "\0";

    /* Use the database */
    sprintf(buffer, "use %s", db_name);
    if(taos_query(taos, buffer) != 0) {
        fprintf(stderr, "Failed to use database: %s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    /* Start to read the data in the table */
    double t = getCurrentTime();
    int nrecords = 0;
    sprintf(buffer, "select * from %s", tb_name);
    if (taos_query(taos, buffer) != 0) {
        fprintf(stderr, "Failed to query data: %s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    TAOS_RES * result = taos_use_result(taos);
    if (result == NULL) {
        fprintf(stderr, "Failed to retreive results:%s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    TAOS_ROW row;

    while ((row = taos_fetch_row(result))) {
        nrecords++;
    }

    taos_free_result(result);
    t = getCurrentTime() - t;

    printf("%10.2f | %10d | %10.2f\n", t, nrecords, nrecords/t);
}


void * syncWriteTable(void * vargs) {

    insert_info * info = (insert_info *) vargs;

    long start_time = info-> start_time;

    /* Connect to the database */
    taos_init();
    TAOS* taos = taos_connect(info->ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(taos));
        taos_close(taos);
        return NULL;
    }

    char buffer[BUFFER_SIZE] = "\0";

    /* Use the database */
    sprintf(buffer, "use %s", info->db_name);
    if (taos_query(taos, buffer) != 0) {
        fprintf(stderr, "Failed to use database %s: %s\n", info->db_name, taos_errstr(taos));
        taos_close(taos);
        return NULL;
    }

    /* Drop the table */
    sprintf(buffer, "drop table %s", info->tb_name);
    taos_query(taos, buffer);
    sleep(1);

    /* Create the table */
    sprintf(buffer, "create table %s (ts timestamp, a float)", info->tb_name);
    if (taos_query(taos, buffer) != 0) {
        fprintf(stderr, "Failed to create table %s: %s\n", info->tb_name, taos_errstr(taos));
        taos_close(taos);
        return NULL;
    }

    /* Wait for the table to create */
    sleep(1);

    /* Insert data */
    double t_w = 0;
    srand(time(NULL));
    int next_goal = info->query_interval;
    for (int inserted = 0; inserted < info->nrecords;) {
        memset(buffer, 0, BUFFER_SIZE);
        char * pstr = buffer;

        pstr += sprintf(pstr, "insert into %s values", info->tb_name);
        for (int i = 0; i < info->nrecords_per_request; i++) {
            pstr += sprintf(pstr, "(%ld, %f)", start_time, (rand() * 1.0 / RAND_MAX) * 20);
            start_time += 100;

            inserted++;
            if (inserted >= info->nrecords){
                break;
            }
        }

        /* Insert the data */
        double ts = getCurrentTime();
        while (taos_query(taos, buffer) != 0){
            if (getCurrentTime() - ts > 120) {
                fprintf(stderr, "Wait too long to inert. Aborted!\n");
                taos_close(taos);
                return NULL;
            }
        }
        t_w += (getCurrentTime() - ts);


        /* Check if to read */
        if (inserted >= next_goal) {
            memset(buffer, 0, BUFFER_SIZE);
            sprintf(buffer, "select * from %s where ts>=%ld", info->tb_name, info->start_time + inserted - info->query_size);

            double t_r = getCurrentTime();
            if (taos_query(taos, buffer) != 0) {
                fprintf(stderr, "Failed to query data: %s\n", taos_errstr(taos));
                taos_close(taos);
                return NULL;
            }
            TAOS_RES * result = taos_use_result(taos);
            if (result == NULL) {
                fprintf(stderr, "Failed to retreive results:%s\n", taos_errstr(taos));
                taos_close(taos);
                return NULL;
            }
            TAOS_ROW row;
            int nread = 0;
            while ((row = taos_fetch_row(result))) {
                nread++;
            }
            taos_free_result(result);

            t_r = getCurrentTime() - t_r;
            printf("%s || %10.2f | %10d | %10.2f | %15.8f || %10.2f | %10d | %11.2f | %10.8f || %10s\n",
                    info->tb_name,
                    t_w, inserted, inserted/t_w, t_w*1000/inserted,
                    t_r, nread, nread/t_r, t_r*1000/nread,
                    nread==info->query_size? "Succeed":"Failed");
            next_goal += info->query_interval;
        }
    }

    return NULL;
}


void rebuildDB(TAOS * taos, const char * const db_name) {
    char buffer[BUFFER_SIZE] = "\0";

    /* Create the new database */
    sprintf(buffer, "create database %s", db_name);
    if (taos_query(taos, buffer) == 0){
        sleep(1);  // wait for database to create.
    }

    /* Use the database */
    sprintf(buffer, "use %s", db_name);
    if(taos_query(taos, buffer) != 0) {
        fprintf(stderr, "Failed to use database: %s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }
}

double getCurrentTime(){
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        perror("Failed to get current time in ms");
        exit(EXIT_FAILURE);
    }

    return tv.tv_sec + tv.tv_usec / 1E6;
}

void dropDB(TAOS * taos, const char * const db_name) {
    char buffer[128] = "\0";

    sprintf(buffer, "drop database %s", db_name);
    taos_query(taos, buffer);
}
