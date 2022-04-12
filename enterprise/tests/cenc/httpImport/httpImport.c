#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "curl/curl.h"
#include "libmseed.h"
#if !defined(HTTP_IMPORT_DEBUG)
#include <errno.h>
#include "taos.h"
#include "base64.h"
#endif


typedef struct SNETIO
{
    CURL   *curl;
    CURLM  *curlm;
    int     still_running;
} SNETIO;


/* Receving callback parameters */
typedef struct recv_cb_params_s
{
    size_t  size;
    char   *buffer;
    int     is_paused;
} recv_cb_params_t;


typedef struct recv_buf_info_s
{
    char  readbuffer[MAXRECLEN];
    int   readlength;
    int   readoffset;
} recv_buf_info_t;


#define BUF_SKIP_LEN                1
#define MAX_TSQL_LEN                65535
#define hash(key, c)                ((uint64_t) key * 31 + c)
#define BUF_UNPROC_LEN(info)        (info)->readlength - (info)->readoffset
#define BUF_TOREAD_PTR(info)        (info)->readbuffer + (info)->readoffset


#define pMS2FSDH_STATION(record)    ((char *)((uint8_t *) record + 8))
#define pMS2FSDH_LOCATION(record)   ((char *)((uint8_t *) record + 13))
#define pMS2FSDH_CHANNEL(record)    ((char *)((uint8_t *) record + 15))
#define pMS2FSDH_NETWORK(record)    ((char *)((uint8_t *) record + 18))
#define pMS3FSDH_SIDLENGTH(record)  ((uint8_t *)((uint8_t *) record + 33))
#define pMS3FSDH_SID(record)        ((char *)((uint8_t *) record + 40))


int seed_net_open(SNETIO *io, const char *url);
size_t seed_net_read(SNETIO *io, void *buffer, size_t size);
void seed_net_close(SNETIO *io);
int seed_net_eof(SNETIO *io);
size_t recv_callback(char *buffer, size_t size, size_t num, void *userdata);
void shift_buffer(recv_buf_info_t *info, int shift);
int seed_read_from(SNETIO *io, recv_buf_info_t *info, uint32_t flags, char *sid, int sidlen, int *reclen);
int seed_detect(char *record, int recbuflen, uint32_t flags, char *sid, int sidlen, int *readlen);
char *seed_recordsid(char *record, char *sid, int sidlen);
#if !defined(HTTP_IMPORT_DEBUG)
int check_and_free_res(TAOS_RES **res, const char *cmd);
uint64_t hash_key(char *data, size_t len);
#endif


int main(int argc, char *argv[])
{
    int              ret;
    int              opt;
    int              reclen;
    int64_t          packets;
    time_t           start, end;
    SNETIO           io;
    recv_buf_info_t  info;
    uint32_t         flags   = MSF_SKIPNOTDATA;
    const char      *url     = NULL;
    const char      *bpos    = NULL;
#if !defined(HTTP_IMPORT_DEBUG)
    int              np;
    int              id;
    TAOS            *taos    = NULL;
    TAOS_RES        *res     = NULL;
    char             cmd[MAX_TSQL_LEN];
    char             sid[LM_SIDLEN];
    uint64_t         hash    = 0;
    long             numport;
    const char      *host    = "localhost";
    const char      *user    = "root";
    const char      *passwd  = "taosdata";
    const char      *port    = "6030";
    const char      *topic   = "packet";
    char            *base64  = NULL;
#else
    FILE            *fp      = NULL;
    const char      *ofile   = NULL;
#endif 

#if !defined(HTTP_IMPORT_DEBUG)
    while ((opt = getopt(argc, argv, "i:h:u:p:P:t:")) != -1) {   
        switch (opt) {
        case 'i':
            url = strdup(optarg);
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
        case 't':
            topic = strdup(optarg);
            break;
        default:
	    fprintf(stderr, "Usage: %s -i url[ -h host -u user -P port -t topic]\r\n", argv[0]);
	    exit(1);
        }
    }

    if (url == NULL || url[0] == '\0') {
        fprintf(stderr, "the option -i was missing!\r\n");
        exit(1);
    }

    numport = strtol(port, NULL, 10);
    if (errno == EINVAL || errno == ERANGE || (numport < 0 || numport > 65535)) {
        fprintf(stderr, "the option -p with invalid number!\r\n");
        exit(1);
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# URL:                                   %s\r\n", url);
    fprintf(stderr, "# Host:                                  %s\r\n", host);
    fprintf(stderr, "# User:                                  %s\r\n", user);
    fprintf(stderr, "# Port:                                  %s\r\n", port);
    fprintf(stderr, "# Topic:                                 %s\r\n", topic);
    fprintf(stderr, "################################################################\r\n");
#else
    while ((opt = getopt(argc, argv, "i:o:")) != -1) {   
        switch (opt) {
        case 'i':
            url = strdup(optarg);
            break;
        case 'o':
            ofile = strdup(optarg);
            break;
        default:
	    fprintf(stderr, "Usage: %s -i url -o filename\r\n", argv[0]);
	    exit(1);
        }
    }

    if (url == NULL || url[0] == '\0') {
        fprintf(stderr, "the option -i was missing!\r\n");
        exit(1);
    }

    if (ofile == NULL || ofile[0] == '\0') {
        fprintf(stderr, "the option -o was missing!\r\n");
        exit(1);
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# URL:                                   %s\r\n", url);
    fprintf(stderr, "# Output:                                %s\r\n", ofile);
    fprintf(stderr, "################################################################\r\n");
#endif

    packets = 0;
    memset(&io, 0, sizeof(SNETIO));
    memset(&info, 0, sizeof(info));

    start = time(NULL);

    ret = seed_net_open(&io, url);
    if (ret != 0) {
        fprintf(stderr, "failed to open %s\r\n", url);
        exit(1);
    }

    fprintf(stderr, "opened %s ok\r\n", url);

#if !defined(HTTP_IMPORT_DEBUG)
    taos_init();

    taos = taos_connect(host, user, passwd, NULL, numport);
    if (taos == NULL) {
        fprintf(stderr, "failed to connect to server: %s\r\n", host);
        goto failed;
    }

    // create topic if not exists
    np = snprintf(cmd, sizeof(cmd), "create topic if not exists %s partitions 4;", topic);
    if (np <= 0) {
        fprintf(stderr, "fprintf error cmd: %s\r\n", cmd);
        goto failed;
    }

    cmd[np] = '\0';

    res = taos_query(taos, cmd);
    if (check_and_free_res(&res, cmd) != 0) {
        goto failed;
    }

    // use database
    taos_select_db(taos, topic);

    fprintf(stderr, "running write data in stream to database, please wait...\n");
#else
    fp = fopen(ofile, "w");
    if (fp == NULL) {
        fprintf(stderr, "failed to open %s\r\n", ofile);
        goto failed;
    }
#endif

    for ( ;; ) {
        ret = seed_read_from(&io, &info, flags,
#if !defined(HTTP_IMPORT_DEBUG)
                             sid, LM_SIDLEN,
#else
                             NULL, 0,
#endif
                             &reclen);
        if (ret != MS_NOERROR) {
            break;
        }

        bpos = info.readbuffer + info.readoffset - reclen;

#if !defined(HTTP_IMPORT_DEBUG)
        hash = hash_key(sid, strlen(sid));
        id = (int) (hash % 4) + 1;

        base64 = base64_encode((const unsigned char *) bpos, reclen);

        np = snprintf(cmd, sizeof(cmd),
                      "insert into p%d using ps tags (%d) values (now, now, '%s');",
                      id, id, base64);

        taosMemoryFree(base64);

        if (np < 0) {
            fprintf(stderr, "fprintf error cmd in loop: %s\r\n", cmd);
            goto failed;
        }

        cmd[np] = '\0';

        res = taos_query(taos, cmd);
        if (check_and_free_res(&res, cmd) != 0) {
            break;
        }
#else
        if (fwrite(bpos, 1, reclen, fp) != reclen) {
            fprintf(stderr, "failed to write to %s\r\n", ofile);
            break;
        }
#endif

        packets++;
        if (packets % 100 == 0) {
            fprintf(stderr, "%ld packet(s) transfered\r\n", packets);
        }
    }

    end = time(NULL);

    if (ret != MS_ENDOFFILE) {
        fprintf(stderr, "terminated prematurely, %ld packet(s) transfered, %lds elapsed\r\n",
                packets, end - start);
    } else {
        fprintf(stderr, "done, %ld packet(s) transfered, %lds elapsed\r\n", packets, end - start);
    }

failed:

    seed_net_close(&io);

#if !defined(HTTP_IMPORT_DEBUG)
    if (taos) {
        taos_close(taos);
    }
#else
    if (fp) {
        fclose(fp);
    }
#endif

    return 0;
}


int
seed_net_open(SNETIO *io, const char *url)
{
    long  http_code;

    if (io == NULL || url == NULL) {
        return -1;
    }

    io->curl = curl_easy_init();
    if (io->curl == NULL) {
        fprintf(stderr, "failed to initialize curl\r\n");
        exit(1);
    }

    /* Set URL */
    if (curl_easy_setopt(io->curl, CURLOPT_URL, url) != CURLE_OK) {
        fprintf(stderr, "could not set CURLOPT_URL: %s\r\n", url);
        goto failed;
    }

    /* Disable signals */
    if (curl_easy_setopt (io->curl, CURLOPT_NOSIGNAL, 1L) != CURLE_OK) {
        fprintf(stderr, "could not set CURLOPT_NOSIGNAL\r\n");
        goto failed;
    }

    /* Return failure codes on errors */
    if (curl_easy_setopt (io->curl, CURLOPT_FAILONERROR, 1L) != CURLE_OK) {
        fprintf(stderr, "Cannot set CURLOPT_FAILONERROR\r\n");
        goto failed;
    }

    /* Follow HTTP redirects */
    if (curl_easy_setopt (io->curl, CURLOPT_FOLLOWLOCATION, 1L) != CURLE_OK) {   
        fprintf(stderr, "could set CURLOPT_FOLLOWLOCATION\r\n");
        goto failed;
    }

    /* Configure write callback for recv'ed data */
    if (curl_easy_setopt (io->curl, CURLOPT_WRITEFUNCTION, recv_callback) != CURLE_OK) {   
        fprintf(stderr, "could not set CURLOPT_WRITEFUNCTION\r\n");
        goto failed; 
    }

    io->curlm = curl_multi_init();
    if (io->curlm == NULL) {
        fprintf(stderr, "curl_multi_init error\r\n");
        goto failed;
    }

    if (curl_multi_add_handle(io->curlm, io->curl) != CURLM_OK) {
        fprintf(stderr, "could not add CURL handle to multi handle\r\n");
        goto failed;
    }

    /* No header callback */

    io->still_running = 1;

    seed_net_read(io, NULL, 0);

    curl_easy_getinfo(io->curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code == 404) {
        fprintf(stderr, "could not open %s, Not Found\r\n", url);
        goto failed;
    } else if (http_code >= 400 && http_code < 600) {
        fprintf(stderr, "could not open %s, response code: %ld\r\n", url, http_code);
        goto failed;
    }

    return 0;

failed:
    return -1;
}


size_t
seed_net_read(SNETIO *io, void *buffer, size_t size)
{
    int              ret;
    size_t           read = 0;
    struct timeval   timeout;
    fd_set           fdread;
    fd_set           fdwrite;
    long             curl_timeo = -1;
    int              maxfd = -1;
    recv_cb_params_t rcp;

    if (io == NULL) {
        return -1;
    }

    if (buffer == NULL && size > 0) {
        return -1;
    }

    if (!io->still_running) {
        return 0;
    }

    rcp.buffer = buffer;
    rcp.size   = size;

    if (curl_easy_setopt(io->curl, CURLOPT_WRITEDATA, (void *) &rcp) != CURLE_OK) {
        fprintf(stderr, "could not set CURLOPT_WRITEDATA\r\n");
        return -1;
    }

    /* Unpause connection */
    curl_easy_pause(io->curl, CURLPAUSE_CONT);
    rcp.is_paused = 0;

    /* Receive data while connection running, destination space available
     * and connection is not paused. */
    do {
        /* Default timeout for read failure */
        timeout.tv_sec  = 15;
        timeout.tv_usec = 0;

        curl_multi_timeout (io->curl, &curl_timeo);

        /* Tailor timeout based on maximum suggested by libcurl */
        if (curl_timeo >= 0) {
            timeout.tv_sec = curl_timeo / 1000;
            if (timeout.tv_sec > 1) {
                timeout.tv_sec = 1;
            } else {
                timeout.tv_usec = (curl_timeo % 1000) * 1000;
            }
        }

        FD_ZERO (&fdread);
        FD_ZERO (&fdwrite);

        if (curl_multi_fdset(io->curlm, &fdread, &fdwrite, NULL, &maxfd) != CURLM_OK) {
            fprintf(stderr, "curl_multi_fdset() error\r\n");
            return -1;
        }

        /* libcurl/system needs time to work, sleep 100 milliseconds */
        if (maxfd == -1) {
            usleep(100000);
            ret = 0;
        } else {
            ret = select(maxfd + 1, &fdread, &fdwrite, NULL, &timeout);
        }

        /* Receive data */
        if (ret >= 0) {
            curl_multi_perform(io->curlm, &io->still_running);
        }
    } while (io->still_running > 0 &&
             !rcp.is_paused &&
             (rcp.size > 0 || rcp.buffer == NULL));

    read = size - rcp.size;

    return read;
}


void
seed_net_close(SNETIO *io)
{
    if (io && io->curl) {
        if (io->curlm) {
            curl_multi_remove_handle (io->curlm, io->curl);
            curl_multi_cleanup(io->curlm);
        }

        curl_easy_cleanup(io->curl);
    }
}


int seed_net_eof(SNETIO *io)
{
    if (io == NULL || io->curl == NULL) {
        return 0;
    }

    if (!io->still_running) {
        return 1;
    }

    return 0;
}


size_t
recv_callback(char *buffer, size_t size, size_t num, void *userdata)
{
    recv_cb_params_t  *rcp = (recv_cb_params_t *) userdata;

    if (buffer == NULL || userdata == NULL) {
        return 0;
    }

    size *= num;

    if (size > rcp->size) {
        rcp->is_paused = 1;
        return CURL_WRITEFUNC_PAUSE;
    } else {
        memcpy(rcp->buffer, buffer, size);
        rcp->buffer += size;
        rcp->size -= size;
    }

    return size;
}


void
shift_buffer(recv_buf_info_t *info, int shift)
{
    if (info == NULL || (shift <= 0 && shift > info->readlength)) {
        return;
    }                                                                                                   

    memmove(info->readbuffer, info->readbuffer + shift, info->readlength - shift);
    info->readlength -= shift;

    if (shift < info->readoffset) {
        info->readoffset -= shift;
    } else { 
        info->readoffset = 0;
    }
}


int
seed_read_from(SNETIO *io, recv_buf_info_t *info, uint32_t flags, char *sid, int sidlen, int *reclen)
{
    int       readcount;
    int       detectlen;
    int       parseval;
    int       recbuflen;
    int       retcode;
    uint32_t  lflags;

    parseval = 0;
    retcode = MS_NOERROR;
    lflags = flags;

    for ( ;; ) {
        if (!seed_net_eof(io) && (BUF_UNPROC_LEN(info) < MINRECLEN ||
                                  parseval > 0))
        {
            if (BUF_UNPROC_LEN(info) <= 0) {
                info->readlength = 0;
                info->readoffset = 0;
            } else if (info->readoffset > 0) {
                shift_buffer(info, info->readoffset);
            }

            readcount = (int) seed_net_read(io, info->readbuffer + info->readlength,
                                            MAXRECLEN - info->readlength);
            if (readcount <= 0 && !seed_net_eof(io)) {
                retcode = MS_GENERROR;
                break;
            }

            info->readlength += readcount;
        }

        if (BUF_UNPROC_LEN(info) >= MINRECLEN) {
            if (seed_net_eof(io)) {
                lflags |= MSF_ATENDOFFILE;
            }

            recbuflen = BUF_UNPROC_LEN(info);

            parseval = seed_detect(BUF_TOREAD_PTR(info),
                                   recbuflen, lflags, sid, sidlen, &detectlen);
            if (parseval == MS_NOERROR) {
                info->readoffset += detectlen;
                if (reclen) {
                    *reclen = detectlen;
                }

                retcode = MS_NOERROR;
                break;
            } else if (parseval < 0) {
                if (flags & MSF_SKIPNOTDATA) {
                    info->readoffset += BUF_SKIP_LEN;
                } else {
                    retcode = parseval;
                    break;
                }
            } else {
                if ((BUF_UNPROC_LEN(info) + parseval) > MAXRECLEN) {
                    if (flags & MSF_SKIPNOTDATA) {
                        info->readoffset += BUF_SKIP_LEN;
                    } else {
                        retcode = MS_OUTOFRANGE;
                        break;
                    }
                } else if (seed_net_eof(io)) {
                    retcode = MS_ENDOFFILE;
                    break;
                }
            }
        }

        /* Finished when at end-of-stream and
         * buffer contains less than MINRECLEN */
        if (seed_net_eof(io) && BUF_UNPROC_LEN(info) < MAXRECLEN) {
            retcode = MS_ENDOFFILE;
            break;
        }
    }

    return retcode;
}


int
seed_detect(char *record, int recbuflen, uint32_t flags, char *sid, int sidlen, int *readlen)
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


char *
seed_recordsid(char *record, char *sid, int sidlen)
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


#if !defined(HTTP_IMPORT_DEBUG)
int
check_and_free_res(TAOS_RES **res, const char *cmd)
{
    int code = 0;

    if (res == NULL) {
        return code;
    }

    if (*res == NULL) {
        fprintf(stderr, "NULL res\r\n");
        code = -1;
    } else {
        if (taos_errno(*res) != 0) {
            fprintf(stderr, "failed to execute: \"%s\", reason: %s\r\n",
                    cmd, taos_errstr(*res));
            code = -2;
        }

        taos_free_result(*res);
        *res = NULL;
    }

    return code;
}


uint64_t
hash_key(char *data, size_t len)
{
    uint64_t  i, key;

    key = 0;

    for (i = 0; i < len; i++) {
        key = hash(key, data[i]);
    }

    return key;
}
#endif
