#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <signal.h>
#include "taos.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  1024
#define  TQ_CHAN_NUM   16
#define  SEED_BUF_LEN  256
#define  MAX_BATCH_NUM 2048
#define  MAX_SEED_LEN  512
#define  hash(key, c)  ((uint64_t) key * 31 + c)


#define pMS2FSDH_STATION(record)    ((char *)((uint8_t *) record + 8))
#define pMS2FSDH_LOCATION(record)   ((char *)((uint8_t *) record + 13))
#define pMS2FSDH_CHANNEL(record)    ((char *)((uint8_t *) record + 15))
#define pMS2FSDH_NETWORK(record)    ((char *)((uint8_t *) record + 18))
#define pMS3FSDH_SIDLENGTH(record)  ((uint8_t *)((uint8_t *) record + 33))
#define pMS3FSDH_SID(record)        ((char *)((uint8_t *) record + 40))


int quit = 0;
int64_t total = 0;


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
        if (readlen) {
          *readlen = reclen;
        }

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
    int               opt, np, code;
    int               id;
    int               n = 0;
    int               readlen;
    char             *file_name    = NULL;
    char             *tsdb_server  = NULL;
    char             *tsdb_usrname = NULL;
    char             *tsdb_passwd  = NULL;
    char             *tsdb_port    = NULL;
    char             *topic        = NULL;
    TAOS             *taos         = NULL;
    TAOS_RES         *res          = NULL;
    TAOS_STMT        *stmt         = NULL;
    TAOS_BIND         tags[1];
    FILE             *fp           = NULL;
    char              tbbuf[32];
    char              buf[MAXRECLEN];
    char              cmd[MAX_TSQL_LEN];
    char              sid[LM_SIDLEN];
    uint64_t          hash;
    int64_t           prv, ts;
    struct timeval    now;
    struct sigaction  act;
    TAOS_MULTI_BIND   params[3];
    int32_t           off_len[MAX_BATCH_NUM];
    int32_t           ts_len[MAX_BATCH_NUM];
    int32_t           bin_len[MAX_BATCH_NUM];
    char              is_null[MAX_BATCH_NUM];
    struct {
        int64_t       off[MAX_BATCH_NUM];
        int64_t       ts[MAX_BATCH_NUM];
        char          bin[MAX_BATCH_NUM][MAX_SEED_LEN];
    } val;

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
    prv = -1;
    memset(sid, 0, LM_SIDLEN);

    fprintf(stderr, "running file -> database(raw), please wait...\r\n");

    memset(is_null, 0, MAX_BATCH_NUM);

    params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[0].buffer_length = sizeof(val.off[0]);
    params[0].buffer = val.off;
    params[0].length = off_len;
    params[0].is_null = is_null;
    params[0].num = MAX_BATCH_NUM;

    params[1].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[1].buffer_length = sizeof(val.ts[0]);
    params[1].buffer = val.ts;
    params[1].length = ts_len;
    params[1].is_null = is_null;
    params[1].num = MAX_BATCH_NUM;

    params[2].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[2].buffer_length = sizeof(val.bin[0]);
    params[2].buffer = val.bin;
    params[2].length = bin_len;
    params[2].is_null = is_null;
    params[2].num = MAX_BATCH_NUM;

    tags[0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[0].buffer_length = sizeof(int);
    tags[0].buffer = &id;
    tags[0].length = NULL;
    tags[0].is_null = NULL;

    stmt = taos_stmt_init(taos);
    if (stmt == NULL) {
        fprintf(stderr, "failed to init stmt\r\n");
        goto failed;
    }

    memset(cmd, 0, sizeof(cmd));
    np = snprintf(cmd, sizeof(cmd) - 1, "insert into ? using ps tags (?) values (?, ?, ?)");

    if (np <= 0) {
        fprintf(stderr, "fprintf error cmd for preparing data\r\n");
        goto failed;
    }

    code = taos_stmt_prepare(stmt, cmd, 0);
    if (code != 0) {
        fprintf(stderr, "failed to stmt prepare for cmd(%s), code: 0x%x\r\n", cmd, code);
        goto failed;
    }

    while (fread(buf, 1, SEED_BUF_LEN, fp) == SEED_BUF_LEN) {
        if (quit == 1) {
            break;
        }

        readlen = 0;

        if (seed_detect(buf, SEED_BUF_LEN, MSF_SKIPNOTDATA, sid, LM_SIDLEN, &readlen) != MS_NOERROR) {
            if (readlen > SEED_BUF_LEN) {
                if (fread(buf + SEED_BUF_LEN, 1, readlen - SEED_BUF_LEN, fp) != readlen - SEED_BUF_LEN) {
                    fprintf(stderr, "seed_detect error\r\n");
                    break;
                }
	    } else {
                break;
            }
        }

        total++;

        hash = hash_key(sid, strlen(sid));
        id = (int) (hash % TQ_CHAN_NUM) + 1;

        if (n < MAX_BATCH_NUM) {
            gettimeofday(&now, NULL);
            ts = (int64_t) (now.tv_sec * 1000000 + now.tv_usec);

            /* so fast */
            if (prv == ts) {
                usleep(5);
                gettimeofday(&now, NULL);
                ts = (int64_t) (now.tv_sec * 1000000 + now.tv_usec);
            }

            prv = ts;

            val.off[n] = ts;
            val.ts[n] = ts;

            memset(val.bin[n], 0, MAX_SEED_LEN);
            memcpy(val.bin[n], buf, SEED_BUF_LEN);

	    off_len[n] = sizeof(val.off[0]);
            ts_len[n] = sizeof(val.ts[0]);
            bin_len[n] = SEED_BUF_LEN;

	    n++;

            memset(tbbuf, 0, sizeof(tbbuf));
            sprintf(tbbuf, "p%d", id);

            code = taos_stmt_set_tbname_tags(stmt, tbbuf, tags);
            if (code != 0) {
                fprintf(stderr, "failed to taos_stmt_set_tbname_tags, code: 0x%x\r\n", code);
                break;
            }
        } else {
            taos_stmt_bind_param_batch(stmt, params);
            taos_stmt_add_batch(stmt);

            n = 0;

            if (taos_stmt_execute(stmt) != 0) {
                fprintf(stderr, "failed to execute insert statement\r\n");
                break;
            }
        }
    }

    if (n > 0) {
        params[0].num = n;
        params[1].num = n;
        params[2].num = n;

        taos_stmt_bind_param_batch(stmt, params);
        taos_stmt_add_batch(stmt);

        if (taos_stmt_execute(stmt) != 0) {
            fprintf(stderr, "failed to execute the last insert statement\r\n");
        }
    }

    fprintf(stdout, "total: %ld\r\n", total);

failed:

    if (stmt) {
        taos_stmt_close(stmt);
    }

    if (taos) {
        taos_close(taos);
    }

    if (fp) {
        fclose(fp);
    }
 
    if (file_name) {
        free(file_name);
    }

    if (tsdb_server && tsdb_server != default_tsdb_server) {
        free(tsdb_server);
    }

    if (tsdb_usrname && tsdb_usrname != default_tsdb_usrname) {
        free(tsdb_usrname);
    }

    if (tsdb_passwd && tsdb_passwd != default_tsdb_passwd) {
        free(tsdb_passwd);
    }

    if (tsdb_port && tsdb_port != default_tsdb_port) {
        free(tsdb_port);
    }

    if (topic && topic != default_topic) {
        free(topic);
    }

    return 0;
}
