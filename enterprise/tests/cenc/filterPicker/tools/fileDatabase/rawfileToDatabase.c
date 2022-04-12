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
#define  hash(key, c)  ((uint64_t) key * 31 + c)


int quit = 0;


static char basis_64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";


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


uint64_t hash_key(char *data, size_t len)
{
    uint64_t  i, key;

    key = 0;

    for (i = 0; i < len; i++) {
        key = hash(key, data[i]);
    }

    return key;
}


// base64 encode
char *base64_encode(const unsigned char *value, int vlen) {
  unsigned char oval = 0;
  char *        result = (char *)taosMemoryMalloc((size_t)(vlen * 4) / 3 + 10);
  char *        out = result;

  while (vlen >= 3) {
    *out++ = basis_64[value[0] >> 2];
    *out++ = basis_64[((value[0] << 4) & 0x30) | (value[1] >> 4)];
    *out++ = basis_64[((value[1] << 2) & 0x3C) | (value[2] >> 6)];
    *out++ = basis_64[value[2] & 0x3F];
    value += 3;
    vlen -= 3;
  }

  if (vlen > 0) {
    *out++ = basis_64[value[0] >> 2];
    oval = (value[0] << 4) & 0x30;
    if (vlen > 1) oval |= value[1] >> 4;
    *out++ = basis_64[oval];
    *out++ = (vlen < 2) ? '=' : basis_64[(value[1] << 2) & 0x3C];
    *out++ = '=';
  }

  *out = '\0';
  return result;
}


int main(int argc, char *argv[])
{
    int                  opt, np, retcode;
    int                  status       = 0;
    int                  verbose      = 0;
    uint32_t             flags        = 0;
    MS3Record           *msr          = NULL;
    const char          *file_name    = NULL;
    const char          *tsdb_server  = "localhost";
    const char          *tsdb_usrname = "root";
    const char          *tsdb_passwd  = "taosdata";
    const char          *tsdb_port    = "6030";
    const char          *tp_name      = "packet";
    TAOS                *taos         = NULL;
    TAOS_RES            *res          = NULL;
    char                 cmd[MAX_TSQL_LEN];
    int64_t              packets, start_time;
    uint64_t             hash         = 0;
    int                  id;
    size_t               cnt, len;
    struct sigaction     act;
    char                *base64       = NULL;
    FILE                *fp           = NULL;
    struct MS3FileParam *pmsfp        = NULL;

    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -i filename[ -s tsdb_server "
                "-u user -p password -P port -t tp_name]\r\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "i:s:u:p:P:t:")) != -1) {   
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
            case 't':
                tp_name = strdup(optarg);
                break;
            default:
                fprintf(stderr,
                        "Usage: %s -i filename[ -s tsdb_server "
                        "-u user -p password -P port -t tp_name]\r\n", argv[0]);
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
    fprintf(stderr, "# Topic Name:                      %s\r\n", tp_name);
    fprintf(stderr, "################################################################\r\n");

    act.sa_handler = handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);

    usleep(500000);

    // init TAOS
    taos_init();

    // connect
    taos = taos_connect(tsdb_server, tsdb_usrname, tsdb_passwd, NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "failed to connet to server: %s\r\n", tsdb_server);
        exit(1);
    }
 
    // create database
    np = snprintf(cmd, sizeof(cmd), "create topic if not exists %s partitions 4;", tp_name);
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
    np = snprintf(cmd, sizeof(cmd), "use %s;", tp_name);
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

    fprintf(stderr, "running file (rawdata) -> database, please wait...\r\n");

    packets = 0;
    flags = MSF_SKIPNOTDATA & MSF_UNPACKDATA;

    fp = fopen(file_name, "rb");
    if (fp == NULL) {
        fprintf(stderr, "running file (rawdata) -> database, please wait...\r\n");
        status = 1;
        goto failed;
    }

    // read from miniseed and inert into database
    while ((retcode = ms3_readmsr_r(&pmsfp, &msr, file_name, NULL, NULL, flags, verbose)) == MS_NOERROR) {
        if (quit == 1) {
            break;
        }

        start_time = (int64_t) round(msr->starttime);

        hash = hash_key(msr->sid, strlen(msr->sid));
        id = (int) (hash % 4) + 1;

        cnt = fread(cmd + np - 3, 1, msr->reclen, fp);
        if (cnt != msr->reclen) {
            fprintf(stderr, "fread error\r\n");
            break;
        }

        base64 = base64_encode((const unsigned char *) (cmd + np - 3), msr->reclen);
        len = strlen(base64);
        if (MAX_TSQL_LEN - 1 - np < len) {
            fprintf(stderr, "not space in cmd for base64: len(%ld)\r\n", len);
            continue;
        }

        np = snprintf(cmd, sizeof(cmd),
                           "insert into p%d using ps tags (%d) values (%ld, now, '%s');",
                            id, id, start_time, base64);

        taosMemoryFree(base64);

        if (np <= 0) {
            fprintf(stderr, "fprintf error cmd: %s\r\n", cmd);
            break;
        }

        cmd[np] = '\0';

        packets++;

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
        fprintf(stderr, "done, inserted %ld packet(s)\r\n", packets);
    }
 
failed:

    /* cleanup memory and close file */
    if (msr) {
        ms3_readmsr(&msr, NULL, NULL, NULL, flags, verbose);
    }

    if (taos) {
        taos_close(taos);
    }

    if (fp) {
        fclose(fp);
    }
 
    return status;
}
