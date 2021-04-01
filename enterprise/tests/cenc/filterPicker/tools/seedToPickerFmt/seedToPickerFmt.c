#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "libmseed.h"
#include "taos.h"


#define  MAX_TSQL_LEN  65535


int quit = 0;


void handler(int sig) {
   fprintf(stderr, "SIGINT received, quit\r\n");
   quit = 1;
}


int main(int argc, char *argv[])
{
    int               opt, np;
    const char       *host      = "localhost";
    const char       *user      = "root";
    const char       *passwd    = "taosdata";
    const char       *port      = "6030";
    const char       *db_name   = "detail";
    const char       *tb_name;
    const char       *of_name   = NULL;
    FILE             *fp        = NULL;
    TAOS             *taos      = NULL;
    TAOS_RES         *res       = NULL;
    TAOS_ROW          row       = NULL;
    char              cmd[MAX_TSQL_LEN];
    char              timestr[64];
    int64_t           samples, time;
    struct sigaction  act;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s -t tb_name[ -h host -u user -p passwd -P port -d db_name] -o outfile\r\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "t:h:u:p:P:o:")) != -1) {   
        switch (opt) {
            case 't':
                tb_name = strdup(optarg);
                break;
            case 'h':
                host = strdup(optarg);
                break;
            case 'u':
                user = strdup(optarg);
                break;
            case 'p':
                passwd = strdup(optarg);
                break;
            case 'P':
                port = strdup(optarg);
                break;
            case 'd':
                db_name = strdup(optarg);
                break;
            case 'o':
                of_name = strdup(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s -i url -o outfile\r\n", argv[0]);
                exit(1);
        }
    }

    if (tb_name == NULL || tb_name[0] == '\0') {
        fprintf(stderr, "the option -t was missing!\r\n");
        exit(1);
    }

    if (of_name == NULL || of_name[0] == '\0') {
        fprintf(stderr, "the option -o was missing!\r\n");
        exit(1);
    }

    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);

    usleep(500000);

    fp = fopen(of_name, "w");
    if (fp == NULL) {
        fprintf(stderr, "failed to open %s\r\n", of_name);
        exit(1);
    }

    fprintf(stderr, "running extract data from %s to %s, please wait...\r\n", tb_name, of_name);

    // init TAOS
    taos_init();

    taos = taos_connect(host, user, passwd, "", atoi(port));
    if (taos == NULL) {
        fprintf(stderr, "failed to connect to db, reason:%s\r\n", taos_errstr(taos));
        goto failed;
    }

    taos_select_db(taos, db_name);

    // get data
    np = snprintf(cmd, sizeof(cmd), "select * from %s;", tb_name);
    if (np <= 0) {
        fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
        goto failed;
    }

    cmd[np] = '\0';

    res = taos_query(taos, cmd);
    if (res == NULL || taos_errno(res) != 0) {
        fprintf(stderr, "taos_query error\r\n");
        goto failed;
    }

    samples = 0;

    while ((row = taos_fetch_row(res))) {
        time = *((int64_t *) row[0]) * 1000 * 1000;
        ms_nstime2timestrz(time, timestr, ISOMONTHDAY, MICRO);
        fprintf(fp, "%s %.1f\n", timestr, (float) (*((int *) row[1])));
        samples++;
    }

    taos_free_result(res);

failed:
    fprintf(stderr, "done, inserted %ld record(s)\r\n", samples);
 
    if (taos) {
        taos_close(taos);
    }

    if (fp) {
        fclose(fp);
    }

    return 0;
}
