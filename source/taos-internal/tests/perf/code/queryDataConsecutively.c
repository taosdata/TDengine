/* Compile:
 *      gcc -o queryDataConsecutively queryDataConsecutively.c -ltaos -lpthread -g
 * Usage:
 *      ./queryDataConsecutively    db_name    table_name [query_interval] [ip_addr]
 * Example:
 *      ./queryDataConsecutively      db1         t1             0         192.168.0.1
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

#ifndef __LOCAL__
#include <taos/taos.h>
#else
#include "taos.h"
#endif

#define BUFFER_SIZE 65536    // 64K

int queryDB(TAOS * taos, char * sqlcmd);
void mysleep(unsigned int second);
extern void taosMsleep(int mseconds);

int query_interval = 0;

int main(int argc, char * argv[]) {
    if (argc < 3) {
        printf("Usage:\n");
        printf("    ./queryDataConsecutively    db_name    table_name [query_interval] [ip_addr]\n");
        printf("Example:\n");
        printf("    ./queryDataConsecutively      db1         t1             0         192.168.0.1\n");
        exit(0);
    }

    char * db_name = argv[1];
    char * tb_name = argv[2];

    if (argc >= 4) query_interval = atoi(argv[3]);
    char * ip_addr = NULL;
    if (argc >= 5) ip_addr = argv[4];

    taos_init();
    TAOS * taos = taos_connect(ip_addr, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "Failed to connect to DB, reason:%s\n", taos_errstr(taos));
        exit(-1);
    }

    char buffer[BUFFER_SIZE] = "\0";

    /* Use database */
    sprintf(buffer, "use %s", db_name);
    if (taos_query(taos, buffer)) {
        fprintf(stderr, "failed to use database:%s\n", taos_errstr(taos));
        exit(1);
    }

    sprintf(buffer, "select * from %s", tb_name);

    int last = 0;
    printf("%d\n", query_interval);
    while (1) {
        int now = queryDB(taos, buffer);
        printf("Last: %d,  Now: %d\n", last, now);
        if (now < last) break;
        last = now;
        mysleep(query_interval);
    }
}

int queryDB(TAOS * taos, char * sqlcmd) {
    while (taos_query(taos, sqlcmd)) {
        mysleep(1);
    }

    /* Fetch the result */
    TAOS_RES * result = taos_use_result(taos);
    if (result == NULL) {
        fprintf(stderr, "Failed to retreive results:%s\n", taos_errstr(taos));
        exit(EXIT_FAILURE);
    }

    TAOS_ROW row;
    int res = 0;
    while ((row = taos_fetch_row(result))){
        res += 1;
    }

    taos_free_result(result);

    return res;
}

void mysleep(unsigned int second) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    taosMsleep(1000*second);
}
