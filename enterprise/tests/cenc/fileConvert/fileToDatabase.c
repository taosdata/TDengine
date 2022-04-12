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
    int               opt, np, n, i, retcode;
    int               status       = 0;
    int               verbose      = 0;
    uint32_t          flags        = 0;
    MS3Record        *msr          = NULL;
    const char       *file_name    = NULL;
    const char       *tsdb_server  = "localhost";
    const char       *tsdb_usrname = "root";
    const char       *tsdb_passwd  = "taosdata";
    const char       *tsdb_port    = "6030";
    const char       *db_name      = "detail";
    const char       *stable_name  = "ms";
    TAOS             *taos         = NULL;
    TAOS_RES         *res          = NULL;
    char             *cp;
    char              net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
    char              cmd[MAX_TSQL_LEN];
    int32_t          *idata;
    int64_t           samples, npts, start_time;
    struct sigaction  act;
#if 0
    int64_t         ingest_time;
    struct timeval  sys_time;
#endif

    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -i filename[ -s tsdb_server "
                "-u user -p password -P port "
                "-d db_name -S stable_name]\r\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "i:s:u:p:P:d:S:")) != -1) {   
        switch (opt) {
            case 'i':
                file_name = strdup(optarg);
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
	    case 'S':
		stable_name = strdup(optarg);
		break;
            default:
                fprintf(stderr,
                        "Usage: %s -i filename[ -s tsdb_server "
                        "-u user -p password -P port "
                        "-d db_name -S stable_name]\r\n", argv[0]);
                exit(1);
        }
    }

    if (file_name == NULL || file_name[0] == '\0') {
        fprintf(stderr, "the option -i was missing!\r\n");
        exit(1);
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# File Name:                       %s\r\n", file_name);
    fprintf(stderr, "# Server:                          %s\r\n", tsdb_server);
    fprintf(stderr, "# User:                            %s\r\n", tsdb_usrname);
    fprintf(stderr, "# Server Port:                     %s\r\n", tsdb_port);
    fprintf(stderr, "# Database Name:                   %s\r\n", db_name);
    fprintf(stderr, "# Super Table Name:                %s\r\n", stable_name);
    fprintf(stderr, "################################################################\r\n");

    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);

    usleep(500000);

    // init TAOS
    // taos_init();

    // connect
    taos = taos_connect(tsdb_server, tsdb_usrname, tsdb_passwd, NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "failed to connet to server: %s\r\n", tsdb_server);
        exit(1);
    }
 
    // create database
    np = snprintf(cmd, sizeof(cmd), "create database if not exists %s;", db_name);
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

    fprintf(stderr, "running file -> database, please wait...\r\n");

    samples = 0;
    flags = MSF_UNPACKDATA | MSF_VALIDATECRC |  MSF_PNAMERANGE;

    np = snprintf(cmd, sizeof(cmd),
                       "create stable if not exists %s (ts timestamp, data int) "
                       "tags (network binary(20), station binary(20), location binary(20), channel binary(20));",
                       stable_name);
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

    // read from miniseed and inert into database
    while ((retcode = ms3_readmsr(&msr, file_name, NULL, NULL, flags, verbose)) == MS_NOERROR) {
        if (quit == 1) {
            break;
        }

        npts = msr->numsamples;
        if (npts <= 0) {
            continue;
        }

        samples += npts;

        start_time = (int64_t) round(msr->starttime * 0.001 * 0.001);
        idata = (int32_t *) msr->datasamples;
        n = (int) round(1000.0 / msr->samprate);

#if 0
        gettimeofday(&sys_time, NULL);
        ingest_time = sys_time.tv_sec * 1000L + (int64_t) round(sys_time.tv_usec * 0.001 * 0.001);

        memset(net, 0, LM_SIDLEN);
        memset(stat, 0, LM_SIDLEN);
        memset(loc, 0, LM_SIDLEN);
        memset(chan, 0, LM_SIDLEN);

        if (ms_sid2nslc(msr->sid, net, stat, loc, chan)) {
            sprintf(stderr, "failed to parse sid: %s\r\n", msr->sid);
            status = 1;
            break;
        }

        np = snprintf(cmd, sizeof(cmd),
                           "insert into %s_%s_%s_%s_md "
                           "using %s_md "
                           "tags ('%s', '%s', '%s', '%s') "
                           "values (%ld, %d, %ld, %ld, %ld, %f);",
                           net, stat, loc, chan, 
                           stable_name,
                           net, stat, loc, chan,
                           ingest_time, msr->sequence_number, start_time, start_time + (npts - 1) * n,
                           npts, msr->samprate);
        if (np <= 0) {
            fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
            break;
        }

        cmd[np] = '\0';

        res = taos_query(taos, cmd);
        if (taos_check_res(res, cmd) != 0) {
            status = 1;
            goto failed;
        }

        taos_free_result(res);
#endif

        memset(net, 0, LM_SIDLEN);
        memset(stat, 0, LM_SIDLEN);
        memset(loc, 0, LM_SIDLEN);
        memset(chan, 0, LM_SIDLEN);

        if (ms_sid2nslc(msr->sid, net, stat, loc, chan)) {
            fprintf(stderr, "failed to parse sid: %s\r\n", msr->sid);
            break;
        }

        np = snprintf(cmd, sizeof(cmd),
                           "insert into %s_%s_%s_%s "
                           "using %s tags ('%s', '%s', '%s', '%s') values ",
                            net, stat, loc, chan,
                            stable_name,
                            net, stat, loc, chan);
        if (np <= 0) {
            fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
            break;
        }

        cmd[np] = '\0';
        cp = cmd + np;

        for (i = 0; i < npts; i++) {
            if (i != npts - 1) {
                np = snprintf(cp, cmd + MAX_TSQL_LEN - cp, "(%ld, %d) ", start_time + i * n, idata[i]);
            } else {
                np = snprintf(cp, cmd + MAX_TSQL_LEN - cp, "(%ld, %d)", start_time + i * n, idata[i]);
            }

            if (np <= 0) {
                fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
                status = 1;
                goto failed;
            }

            cp += np;
        }

        *cp++ = ';';
        *cp = '\0';

        res = taos_query(taos, cmd);
        if (taos_check_res(res, cmd) != 0) {
            status = 1;
            goto failed;
        }

        taos_free_result(res);
    }

    if (retcode != MS_ENDOFFILE) {
        ms_log(2, "cannot read %s: %s\r\n", file_name, ms_errorstr (retcode));
    }

    if (status == 0) {
        fprintf(stderr, "done, inserted %ld record(s)\r\n", samples);
    }
 
failed:

    /* cleanup memory and close file */
    if (msr) {
        ms3_readmsr(&msr, NULL, NULL, NULL, flags, verbose);
    }

    if (taos) {
        taos_close(taos);
    }
 
    return status;
}
