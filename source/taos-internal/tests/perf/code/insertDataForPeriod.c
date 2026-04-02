/* Compile:
 *      gcc -o insertDataForPeriod insertDataForPeriod.c -ltaos -lpthread
 * Usage:
 *      ./insertDataForPeriod db_name table_name [records_per_request] [insert_time] [request_interval_time] [ip_addr]
 * Example:
 *      ./insertDataForPeriod    db1       t1              10                5               10              168.192.0.1
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <string.h>
#include <signal.h>

#ifndef __LOCAL__
#include <taos/taos.h>
#else
#include "taos.h"
#endif

#define BUFFER_SIZE 65536    // 64K

long start_time = 1430000000000;

extern void taosMsleep(int mseconds);
double getCurrentTime();
void mysleep(unsigned int second);

int main(int argc, char * argv[]){

    if (argc <=3) {
        printf("Usage:\n");
        printf("./insertDataForPeriod db_name table_name [records_per_request] [insert_time] [request_interval_time] [ip_addr]\n");
        printf("Example:\n");
        printf("./insertDataForPeriod    db1       t1              10                300               10              168.192.0.1\n");
        exit(0);
    }

    char * db_name = argv[1];
    char * tb_name = argv[2];

    int nrecords_per_request = 1;
    if (argc >=4) {
        nrecords_per_request = atoi(argv[3]);
    }

    int seconds = 300;
    if (argc >= 5) {
        seconds = atoi(argv[4]);
    }

    int request_interval = 0;
    if (argc >= 6) {
        request_interval = atoi(argv[5]);
    }

    if (request_interval > seconds) {
        fprintf(stderr, "Reqest interval is too long");
        exit(EXIT_FAILURE);
    }

    char * ip_addr = NULL;
    if (argc >= 7) {
        ip_addr = argv[6];
    }


    taos_init();
    TAOS * taos = taos_connect(ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(taos));
        exit(1);
    }

    char buffer[BUFFER_SIZE] = "\0";
    /* #<{(| Drop database |)}># */
    /* sprintf(buffer, "drop database %s", db_name); */
    /* taos_query(taos, buffer); */
    /* mysleep(1); */

    /* Create database */
    sprintf(buffer, "create database %s", db_name);
    taos_query(taos, buffer);
    /* if (taos_query(taos, buffer)) { */
    /*     fprintf(stderr, "Failed to create database:%s\n", taos_errstr(taos)); */
    /*     exit(1); */
    /* } */
    mysleep(1);

    /* Use database */
    sprintf(buffer, "use %s", db_name);
    if (taos_query(taos, buffer)) {
        fprintf(stderr, "failed to use database:%s\n", taos_errstr(taos));
        exit(1);
    }

    /* Drop table */
    sprintf(buffer, "drop table %s", tb_name);
    taos_query(taos, buffer);

    /* Create table */
    sprintf(buffer, "create table %s (ts timestamp, tag binary(12), lat float, lon float, direction int)", tb_name);
    if (taos_query(taos, buffer)) {
        fprintf(stderr, "Failed to create table:%s\n", taos_errstr(taos));
        exit(1);
    }
    mysleep(1);

    double t1 = getCurrentTime();
    while (1) {
        if (getCurrentTime()-t1 > seconds) break;

        /* Wrtie data */
        memset(buffer, 0, BUFFER_SIZE);
        char * pstr = buffer;

        pstr += sprintf(pstr, "insert into %s", tb_name);
        for (int i = 0; i < nrecords_per_request; i++){
            pstr += sprintf(pstr, " values (%ld, %s, %f, %f, %d)", 
                    start_time++,
                    "abcd", 
                    rand() * 1.0 / RAND_MAX * 180 -90, 
                    rand() * 1.0 / RAND_MAX * 360, 
                    rand()%100000);
        }

        while (taos_query(taos, buffer) != 0){
            if (getCurrentTime()-t1 > seconds) break;
        }
        mysleep(request_interval);
    }

    taos_close(taos);
    return 0;
}


double getCurrentTime(){
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        perror("Failed to get current time in ms");
        exit(EXIT_FAILURE);
    }

    return tv.tv_sec + tv.tv_usec / 1E6;
}

void mysleep(unsigned int second) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    taosMsleep(1000*second);
}
