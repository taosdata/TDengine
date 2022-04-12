#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "taos.h"


#define  MAX_TSQL_LEN  65535


static signed char index_64[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32,
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1};

#define  CHAR64(c)     (((c) < 0 || (c) > 127) ? -1 : index_64[(c)])


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


// base64 decode
unsigned char *base64_decode(const char *value, int inlen, int *outlen) {
  int            c1, c2, c3, c4;
  unsigned char *result = (unsigned char *)taosMemoryMalloc((size_t)(inlen * 3) / 4 + 1);
  unsigned char *out = result;

  *outlen = 0;

  while (1) {
    if (value[0] == 0) {
      *out = '\0';
      return result;
    }

    // skip \r\n
    if (value[0] == '\n' || value[0] == '\r') {
      value += 1;
      continue;
    }

    c1 = value[0];
    if (CHAR64(c1) == -1) goto base64_decode_error;
    c2 = value[1];
    if (CHAR64(c2) == -1) goto base64_decode_error;
    c3 = value[2];
    if ((c3 != '=') && (CHAR64(c3) == -1)) goto base64_decode_error;
    c4 = value[3];
    if ((c4 != '=') && (CHAR64(c4) == -1)) goto base64_decode_error;

    value += 4;
    *out++ = (unsigned char)((CHAR64(c1) << 2) | (CHAR64(c2) >> 4));
    *outlen += 1;
    if (c3 != '=') {
      *out++ = (unsigned char)(((CHAR64(c2) << 4) & 0xf0) | (CHAR64(c3) >> 2));
      *outlen += 1;
      if (c4 != '=') {
        *out++ = (unsigned char)(((CHAR64(c3) << 6) & 0xc0) | CHAR64(c4));
        *outlen += 1;
      }
    }
  }

base64_decode_error:
  taosMemoryFree(result);
  result = 0;
  *outlen = 0;

  return result;
}


int main(int argc, char *argv[])
{
    int                  opt, nfields, i, len, np;
    int                  status       = 0;
    const char          *file_name    = NULL;
    const char          *tsdb_server  = "localhost";
    const char          *tsdb_usrname = "root";
    const char          *tsdb_passwd  = "taosdata";
    const char          *tsdb_port    = "6030";
    const char          *tp_name      = "packet";
    const char          *tb_name      = NULL;
    TAOS                *taos         = NULL;
    TAOS_RES            *res          = NULL;
    TAOS_ROW             row          = NULL;
    TAOS_FIELD          *fields       = NULL;
    char                 cmd[MAX_TSQL_LEN];
    int64_t              packets;
    struct sigaction     act;
    unsigned char       *p       = NULL;
    FILE                *fp           = NULL;

    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -o filename -t table_name[ -s tsdb_server "
                "-u user -p password -P port -T tp_name]\r\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "o:s:u:p:P:t:T:")) != -1) {   
        switch (opt) {
            case 'o':
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
                tb_name = strdup(optarg);
                break;
            case 'T':
                tp_name = strdup(optarg);
                break;
            default:
                fprintf(stderr,
                        "Usage: %s -o filename -t table_name[ -s tsdb_server "
                        "-u user -p password -P port -T tp_name]\r\n", argv[0]);
                exit(1);
        }
    }

    if (file_name == NULL || file_name[0] == '\0') {
        fprintf(stderr, "the option -i was missing!\r\n");
        exit(1);
    }

    if (tb_name == NULL || tb_name[0] == '\0') {
        fprintf(stderr, "the option -t was missing!\r\n");
        exit(1);
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# File Name:                       %s\r\n", file_name);
    fprintf(stderr, "# Server:                          %s\r\n", tsdb_server);
    fprintf(stderr, "# User:                            %s\r\n", tsdb_usrname);
    fprintf(stderr, "# Server Port:                     %s\r\n", tsdb_port);
    fprintf(stderr, "# Topic Name:                      %s\r\n", tp_name);
    fprintf(stderr, "# Table Name:                      %s\r\n", tb_name);
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

    fprintf(stderr, "running database -> file (rawdata), please wait...\r\n");

    packets = 0;

    np = snprintf(cmd, sizeof(cmd), "select * from %s;", tb_name);
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

    fp = fopen(file_name, "wb");
    if (fp == NULL) {
        fprintf(stderr, "running database -> file (rawdata), please wait...\r\n");
        status = 1;
        goto failed;
    }

    nfields = taos_num_fields(res);

    while ((row = taos_fetch_row(res))) {
        packets++;

        fields = taos_fetch_fields(res);

        for (i = 0; i < nfields; i++) {
            if (strncasecmp(fields[i].name, "content", sizeof("content") - 1) == 0 &&
                fields[i].type == TSDB_DATA_TYPE_BINARY)
            {
                p = base64_decode((const char *) row[i], strlen((char *) row[i]), &len);
                if (p == NULL) {
                    fprintf(stderr, "base64_decode error\r\n");
                    status = 1;
                    goto failed;
                }

                fwrite(p, len, 1, fp);
                taosMemoryFree((void *) p);
            }
        }
    }

    if (status == 0) {
        fprintf(stderr, "done, wrote %ld packet(s)\r\n", packets);
    }
 
failed:
    if (taos) {
        taos_close(taos);
    }

    if (fp) {
        fclose(fp);
    }
 
    return status;
}
