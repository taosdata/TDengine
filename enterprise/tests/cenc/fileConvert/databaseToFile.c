#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "taos.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  65535


int quit = 0;


void handler(int sig) {
    fprintf(stderr, "SIGINT received, quit\r\n");
    quit = 1;
}


int taos_check_res(TAOS_RES *res, const char *cmd) {
    int code = 0;

    if (res == NULL) {
        fprintf(stderr, "NULL res\r\n");
        code = -1;
    } else {
        if (taos_errno(res) != 0) {
            fprintf(stderr, "failed to execute: \"%s\", reason: %s\r\n", cmd, taos_errstr(res));
            code = -2;
        }
    }

    return code;
}


int main(int argc, char *argv[])
{
    int               opt, i, np, nfields, rv;
    int               status       = 0;
    int               verbose      = 0;
    int               reclen       = 512;
    int               data[500];
    uint32_t          flags        = MSF_PACKVER2;
    MS3Record        *msr          = NULL;
    const char       *file_name    = NULL;
    const char       *tsdb_server  = "localhost";
    const char       *tsdb_usrname = "root";
    const char       *tsdb_passwd  = "taosdata";
    const char       *tsdb_port    = "6030";
    const char       *db_name      = "detail";
    const char       *tb_name      = NULL;
    const char       *stb_name     = "ms";
    const char       *begin_time   = NULL;
    const char       *end_time     = NULL;
    TAOS             *taos         = NULL;
    TAOS_RES         *res          = NULL;
    TAOS_ROW          row          = NULL;
    TAOS_FIELD       *fields       = NULL;
    char              sid[LM_SIDLEN];
    char              net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
    char              cmd[MAX_TSQL_LEN];
    int64_t           records;
    nstime_t          nsbegin_time; 
    nstime_t          nsend_time; 
    int64_t           prev_time = 0, delta_time;
    struct sigaction  act;
    enum              {
        neither = 0,
        begin,
        end,
        both
    } time_present;


    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -o filename -t tb_name[ -b begin_time(YYYY-MM-DD hh:mm:ss.sss) "
                "-e end_time(YYYY-MM-DD hh:mm:ss.sss) -s tsdb_server -u user "
                "-p password -P port -d db_name]\r\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "o:t:b:e:s:u:p:P:d:")) != -1) {
        switch (opt) {
            case 'o':
                file_name = strdup(optarg);
                break;
            case 't':
                tb_name = strdup(optarg);
                break;
            case 's':
                tsdb_server = strdup(optarg);
                break;
            case 'u':
                tsdb_usrname = strdup(optarg);
                break;
            case 'p':
                tsdb_passwd = strdup(optarg);
                break;
            case 'P':
                tsdb_port = strdup(optarg);
                break;
            case 'd':
                db_name = strdup(optarg);
                break;
	    case 'b':
		begin_time = strdup(optarg);
		break;
	    case 'e':
		end_time = strdup(optarg);
		break;
            default:
                fprintf(stderr,
                        "Usage: %s -o filename -t tb_name[ -b begin_time(YYYY-MM-DD hh:mm:ss.sss) "
                        "-e end_time(YYYY-MM-DD hh:mm:ss.sss) -s tsdb_server -u user "
                        "-p password -P port -d db_name]\r\n", argv[0]);
                exit(1);
        }
    }

    if (file_name == NULL || file_name[0] == '\0') {
        fprintf(stderr, "the option -o was missing!\r\n");
        exit(1);
    }

    if (tb_name == NULL || tb_name[0] == '\0') {
        fprintf(stderr, "the option -t was missing!\r\n");
        exit(1);
    }

    if (strlen(tb_name) >= LM_SIDLEN - (sizeof("XFDSN:") - 1)) {
        fprintf(stderr, "too long table name: %s!\r\n", tb_name);
        exit(1);
    }

    memset(sid, 0, LM_SIDLEN);

    (void) snprintf(sid, LM_SIDLEN, "XFDSN:%s", tb_name);
    if (ms_sid2nslc(sid, net, stat, loc, chan)) {
        fprintf(stderr, "ms_sid2nslc() error\r\n");
        exit(1);
    }

    time_present = neither;

    if (begin_time && begin_time[0]) {
        time_present = begin;

        nsbegin_time = ms_timestr2nstime(begin_time);
        if (nsbegin_time == NSTERROR) {
            fprintf(stderr, "invalid begin time: %s!\r\n", begin_time);
            exit(1);
        }
    }

    if (end_time && end_time[0]) {
        if (time_present == begin) {
            time_present = both;
        } else {
            time_present = end;
        }

        nsend_time = ms_timestr2nstime(end_time);
        if (nsend_time == NSTERROR) {
            fprintf(stderr, "invalid end time: %s!\r\n", end_time);
            exit(1);
        }
    }

    if (time_present == both) {
        if (nsbegin_time > nsend_time) {
            fprintf(stderr, "begin time MUST be less or eqaul to end time!\r\n");
            exit(1);
        }
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# File Name:                       %s\r\n", file_name);
    fprintf(stderr, "# Server:                          %s\r\n", tsdb_server);
    fprintf(stderr, "# User:                            %s\r\n", tsdb_usrname);
    fprintf(stderr, "# Server Port:                     %s\r\n", tsdb_port);
    fprintf(stderr, "# Database Name:                   %s\r\n", db_name);
    fprintf(stderr, "# Super Table Name:                %s\r\n", stb_name);
    fprintf(stderr, "# Table Name:                      %s\r\n", tb_name);
    fprintf(stderr, "# Begin time:                      %s\r\n", begin_time);
    fprintf(stderr, "# End time:                        %s\r\n", end_time);
    fprintf(stderr, "################################################################\r\n");

    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);

    usleep(500000);

    // init MSR
    msr = msr3_init(NULL);
    if (msr == NULL) {
        fprintf(stderr, "failed to init msr\r\n");
        exit(1);
    }

    // init TAOS
    // taos_init();

    // connect
    taos = taos_connect(tsdb_server, tsdb_usrname, tsdb_passwd, NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "failed to connet to server: %s\r\n", tsdb_server);
        exit(1);
    }
 
    // change to database
    np = snprintf(cmd, sizeof(cmd), "use %s;", db_name);
    if (np <= 0) {
        fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
        status = 1;
        goto failed;
    }

    cmd[np] = '\0';

    res = taos_query(taos, cmd);
    if (taos_check_res(res, cmd) != 0) {
        status = 1;
        goto failed;
    }

    taos_free_result(res);

    fprintf(stderr, "running database -> file, please wait...\r\n");

    records = 0;
    np = 0;
    flags |= MSF_FLUSHDATA;

    switch (time_present) {
    case begin:
        np = snprintf(cmd, sizeof(cmd), "select * from %s where ts >= \"%s\";", tb_name, begin_time);
        break;
    case end:
        np = snprintf(cmd, sizeof(cmd), "select * from %s where ts <= \"%s\";", tb_name, end_time);
        break;
    case both:
        np = snprintf(cmd, sizeof(cmd), "select * from %s where ts >= \"%s\" and ts <= \"%s\";",
                      tb_name, begin_time, end_time);
        break;
    default:
        np = snprintf(cmd, sizeof(cmd), "select * from %s;", tb_name);
    }

    if (np <= 0) {
        fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
        status = 1;
        goto failed;
    }

    cmd[np] = '\0';

    res = taos_query(taos, cmd);
    if (taos_check_res(res, cmd) != 0) {
        status = 1;
        goto failed;
    }

    // construct msr
    memset(msr->sid, 0, sizeof(msr->sid));
    strncpy(msr->sid, sid, strlen(sid));
    msr->reclen = reclen;
    msr->pubversion = 2;
    msr->samprate = 100;
    msr->encoding = DE_STEIM2;
    msr->sampletype = 'i';

    nfields = taos_num_fields(res);
    msr->datasamples = data;

    delta_time = (int64_t) (1000 * 1000 * 1000 * 1.0 / msr->samprate);
    prev_time = -1;

    while ((row = taos_fetch_row(res))) {
        if (records == 0) {
            msr->starttime = (*((int64_t *) row[0])) * 1000 * 1000;
        }

        fields = taos_fetch_fields(res);

        for (i = 0; i < nfields; i++) {
            if (strncasecmp(fields[i].name, "data", sizeof("data") - 1) == 0 &&
                fields[i].type == TSDB_DATA_TYPE_INT)
            {
                data[records++] = *((int *) row[i]);
            }
        }

        if (prev_time == -1) {
            prev_time = (*((int64_t *) row[0])) * 1000 * 1000;
        } else {
            if (prev_time + delta_time != (*((int64_t *) row[0])) * 1000 * 1000) {
                fprintf(stderr, "non-successive timestamp, previous: %ld, now: %ld\r\n",
                        prev_time, (*((int64_t *) row[0])) * 1000 * 1000);

                records--;

                msr->numsamples = records;
                msr->samplecnt = msr->numsamples;

                rv = msr3_writemseed(msr, file_name, 0, flags, verbose);
                if (rv < 0) {
                    fprintf(stderr, "1 msr3_writemseed error (%d)\r\n", rv);
                    break;
                }

                prev_time = (*((int64_t *) row[0])) * 1000 * 1000;
                msr->starttime = prev_time;

                data[0] = data[records];
                records = 1;

                continue;
            } else {
                prev_time += delta_time;
            }
        }

        if (records >= 400) {
            records = 0;

            msr->numsamples = 400;
            msr->samplecnt = msr->numsamples;

            rv = msr3_writemseed(msr, file_name, 0, flags, verbose);
            if (rv < 0) {
                fprintf(stderr, "2 msr3_writemseed error (%d)\r\n", rv);
                break;
            }
        }
    }

    if (records > 0) {
        msr->numsamples = records;

        rv = msr3_writemseed(msr, file_name, 0, flags, verbose);
        if (rv < 0) {
            fprintf(stderr, "3 msr3_writemseed error (%d)\r\n", rv);
        }
    }

    msr->datasamples = NULL;

    taos_free_result(res);

    if (status == 0) {
        fprintf(stderr, "done\r\n");
    }
 
failed:

    if (msr) {
        msr3_free(&msr);
    }

    if (taos) {
        taos_close(taos);
    }
 
    return status;
}
