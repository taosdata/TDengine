#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "taos.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  1048576
#define  TQ_CHAN_NUM   10
#define  SEED_BUF_LEN  256
#define  hash(key, c)  ((uint64_t) key * 31 + c)


#define pMS2FSDH_STATION(record)    ((char *)((uint8_t *) record + 8))
#define pMS2FSDH_LOCATION(record)   ((char *)((uint8_t *) record + 13))
#define pMS2FSDH_CHANNEL(record)    ((char *)((uint8_t *) record + 15))
#define pMS2FSDH_NETWORK(record)    ((char *)((uint8_t *) record + 18))
#define pMS3FSDH_SIDLENGTH(record)  ((uint8_t *)((uint8_t *) record + 33))
#define pMS3FSDH_SID(record)        ((char *)((uint8_t *) record + 40))


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


// base64 encode
static char basis_64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

char *base64_encode(const unsigned char *value, int vlen)
{
    unsigned char oval = 0;
    char *        result = (char *)malloc((size_t)(vlen * 4) / 3 + 10);
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
        if (vlen > 1) {
            oval |= value[1] >> 4;
        }

        *out++ = basis_64[oval];
        *out++ = (vlen < 2) ? '=' : basis_64[(value[1] << 2) & 0x3C];
        *out++ = '=';
    }

    *out = '\0';

    return result;
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


char *seed_recordsid(char *record, char *sid, int sidlen)
{
    char net[3] = {0};
    char sta[6] = {0};
    char loc[3] = {0};
    char chan[6] = {0};
  
    if (record == NULL || sid == NULL) {
        return NULL;
    }
  
    ms_strncpclean(net, pMS2FSDH_NETWORK(record), 2);
    ms_strncpclean(sta, pMS2FSDH_STATION(record), 5);
    ms_strncpclean(loc, pMS2FSDH_LOCATION(record), 2);

    /* Map 3 channel codes to BAND_SOURCE_POSITION */
    chan[0] = *pMS2FSDH_CHANNEL(record);
    chan[1] = '_';
    chan[2] = *(pMS2FSDH_CHANNEL(record) + 1);
    chan[3] = '_';
    chan[4] = *(pMS2FSDH_CHANNEL(record) + 2);
  
    if (ms_nslc2sid (sid, sidlen, 0, net, sta, loc, chan) < 0) {
        return NULL;
    }

    return sid;
}


int seed_detect(char *record, int recbuflen, uint32_t flags, char *sid, int sidlen, int *readlen)
{
    int      reclen;
    uint8_t  recsidlen;
    uint8_t  fmt_ver = 0;

    reclen = ms3_detect(record, recbuflen, &fmt_ver);
    if (fmt_ver == 2 &&
        reclen == 0 &&
        (flags & MSF_ATENDOFFILE) &&
        ((recbuflen & (recbuflen - 1)) == 0) &&
        recbuflen < MAXRECLEN)
    {
        reclen = (int) recbuflen;
    }

    /* No seed record detected */
    if (reclen < 0) {
        return MS_NOTSEED;
    } else if (reclen == 0) {
        /* Found record but could not determine length */
        return MINRECLEN;
    } else if (reclen < MINRECLEN || reclen > MAXRECLEN) {
        return MS_OUTOFRANGE;
    } else if (reclen > recbuflen) {
        return (int) (reclen - recbuflen);
    }

    if (fmt_ver == 2) {
        if (!MS2_ISVALIDHEADER(record)) {
            goto invalid;
        }

        if (sid) {
            seed_recordsid(record, sid, LM_SIDLEN);
        }
    } else if (fmt_ver == 3) {
        if (!MS3_ISVALIDHEADER(record)) {
            goto invalid;
        }

        recsidlen = *pMS3FSDH_SIDLENGTH(record);
        if (recsidlen > (uint8_t) (sidlen - 1)) {
            fprintf(stderr, "too long sid length: %d\r\n", recsidlen);
            return MS_GENERROR;
        }

        if (sid) {
            memmove(sid, pMS3FSDH_SID(record), recsidlen);
            sid[recsidlen] = '\0';
        }
    }

    if (readlen) {
        *readlen = reclen;
    }

    return MS_NOERROR;

invalid:

    fprintf(stderr, "unrecognized header\r\n");

    return MS_NOTSEED;
}


char  *default_tsdb_server  = "localhost";
char  *default_tsdb_usrname = "root";
char  *default_tsdb_passwd  = "taosdata";
char  *default_tsdb_port    = "6030";
char  *default_topic        = "rewrite";


int main(int argc, char *argv[])
{
    int               opt, np;
    int               id;
    int               begin;
    int               offset;
    char             *file_name    = NULL;
    char             *tsdb_server  = NULL;
    char             *tsdb_usrname = NULL;
    char             *tsdb_passwd  = NULL;
    char             *tsdb_port    = NULL;
    char             *topic        = NULL;
    TAOS             *taos         = NULL;
    TAOS_RES         *res          = NULL;
    FILE             *fp           = NULL;
    char              buf[SEED_BUF_LEN];
    char              cmd[MAX_TSQL_LEN];
    char              sid[LM_SIDLEN];
    const char       *prefix  = "insert into";
    const int         pfxlen  = sizeof("insert into") - 1;
    char             *base64;
    uint64_t          hash;
    int64_t           ts;
    struct timeval    now;
    struct sigaction  act;

    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -i filename[ -s tsdb_server "
                "-u user -p password -P port -t topic]\r\n", argv[0]);
        goto failed;
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
                topic = strdup(optarg);
                break;
            default:
                fprintf(stderr,
                        "Usage: %s -i filename[ -s tsdb_server "
                        "-u user -p password -P port -t topic]\r\n", argv[0]);
                goto failed;
        }
    }

    if (file_name == NULL || file_name[0] == '\0') {
        fprintf(stderr, "Usage: %s -i filename[ -s tsdb_server "
                        "-u user -p password -P port -t topic]\r\n", argv[0]);
        goto failed;
    }

    if (tsdb_server == NULL) {
        tsdb_server = default_tsdb_server;
    }

    if (tsdb_usrname == NULL) {
        tsdb_usrname = default_tsdb_usrname;
    }

    if (tsdb_passwd == NULL) {
        tsdb_passwd = default_tsdb_passwd;
    }

    if (tsdb_port == NULL) {
        tsdb_port = default_tsdb_port;
    }

    if (topic == NULL) {
        topic = default_topic;
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# File Name:                       %s\r\n", file_name);
    fprintf(stderr, "# Server:                          %s\r\n", tsdb_server);
    fprintf(stderr, "# User:                            %s\r\n", tsdb_usrname);
    fprintf(stderr, "# Server Port:                     %s\r\n", tsdb_port);
    fprintf(stderr, "# Topic:                           %s\r\n", topic);
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
 
    // create topic
    np = snprintf(cmd, sizeof(cmd), "create topic if not exists %s partitions %d;", topic, TQ_CHAN_NUM);
    if (np <= 0) {
        fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
        goto failed;
    }

    cmd[np] = '\0';

    res = taos_query(taos, cmd);
    if (taos_check_res(res, cmd) != 0) {
        goto failed;
    }

    // change to database
    taos_select_db(taos, topic);

    fp = fopen(file_name, "r");
    if (fp == NULL) {
        fprintf(stderr, "failed to open %s\r\n", file_name);
        goto failed;
    }

    np = 0;
    begin = 1;
    offset = 0;
    memset(sid, 0, LM_SIDLEN);

    fprintf(stderr, "running file -> database(raw), please wait...\r\n");

    while (fread(buf, 1, SEED_BUF_LEN, fp) == SEED_BUF_LEN) {
        if (quit == 1) {
            break;
        }

        if (seed_detect(buf, SEED_BUF_LEN, MSF_SKIPNOTDATA, sid, LM_SIDLEN, NULL) != MS_NOERROR) {
            fprintf(stderr, "seed_detect error\r\n");
            break;
        }

        base64 = base64_encode((const unsigned char *) buf, SEED_BUF_LEN);
        if (base64 == NULL) {
            fprintf(stderr, "base64 error\r\n");
            break;
        }

remain_data:

        /* TODO */
        hash = hash_key(sid, strlen(sid));
        id = (int) (hash % TQ_CHAN_NUM) + 1;

        if (begin == 1) {
            begin = 0;
            memset(cmd, 0, sizeof(cmd));
            memcpy(cmd, prefix, pfxlen);
            offset += pfxlen;
        }

        if (offset + strlen(base64) < MAX_TSQL_LEN - 256) {
            gettimeofday(&now, NULL);
            ts = (int64_t) (now.tv_sec * 1000000 + now.tv_usec);
            np = snprintf(cmd + offset, sizeof(cmd) - offset, " p%d using ps tags (%d) values (%ld, %ld, '%s')", id, id, ts, ts, base64);

            free(base64);

            if (np <= 0) {
                fprintf(stderr, "fprintf error cmd for preparing data: %s\r\n", cmd);
                continue;
            }

            offset += np;
        } else {
            if (offset == 0) {
                free(base64);

                fprintf(stderr, "too long record length: %d\r\n", SEED_BUF_LEN);
                goto failed;
            }

            cmd[offset] = ';';

            res = taos_query(taos, cmd);
            if (taos_check_res(&res, cmd) != 0) {
                goto failed;
            }

            begin = 1;
            offset = 0;
            goto remain_data;
        }
    }

    if (offset) {
        cmd[offset++] = ';';
        cmd[offset] = '\0';

        res = taos_query(taos, cmd);
        taos_check_res(&res, cmd);
    }

failed:

    if (taos) {
        taos_close(taos);
    }

    if (fp) {
        fclose(fp);
    }
 
    if (file_name) {
        free(file_name);
    }

    if (tsdb_server != default_tsdb_server) {
        free(tsdb_server);
    }

    if (tsdb_usrname != default_tsdb_usrname) {
        free(tsdb_usrname);
    }

    if (tsdb_passwd != default_tsdb_passwd) {
        free(tsdb_passwd);
    }

    if (tsdb_port != default_tsdb_port) {
        free(tsdb_port);
    }

    if (topic != default_topic) {
        free(topic);
    }

    return 0;
}
