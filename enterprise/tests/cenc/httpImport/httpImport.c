#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include "curl/curl.h"
#include "libmseed.h"
#if !defined(HTTP_IMPORT_DEBUG)
#include <errno.h>
#include "taos.h"
#include "base64.h"
#endif


#define CIR_BUF_NUM   8192
#define CIR_BUF_SIZE  1536


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


typedef struct circular_buf_s
{
    char             data[CIR_BUF_NUM][CIR_BUF_SIZE];
    int              pos;
    int              last;

    pthread_mutex_t  mutex;
} circular_buf_t;


typedef struct thread_arg_s
{
    int              index;
    circular_buf_t   buffer;
    TAOS            *taos;
} thread_arg_t;


#define BUF_SKIP_LEN                1
#define TQ_CHAN_NUM                 16
#define MAX_TSQL_LEN                ((MAXRECLEN - 100) << 3)
#define MAX_BUFF_LEN                (MAXRECLEN - 100)
#define MAX_DB_ROWS                 32767
#define hash(key, c)                ((uint64_t) key * 31 + c)
#define BUF_UNPROC_LEN(info)        (info)->readlength - (info)->readoffset
#define BUF_TOREAD_PTR(info)        (info)->readbuffer + (info)->readoffset


#define pMS2FSDH_STATION(record)    ((char *)((uint8_t *) record + 8))
#define pMS2FSDH_LOCATION(record)   ((char *)((uint8_t *) record + 13))
#define pMS2FSDH_CHANNEL(record)    ((char *)((uint8_t *) record + 15))
#define pMS2FSDH_NETWORK(record)    ((char *)((uint8_t *) record + 18))
#define pMS3FSDH_SIDLENGTH(record)  ((uint8_t *)((uint8_t *) record + 33))
#define pMS3FSDH_SID(record)        ((char *)((uint8_t *) record + 40))


int seed_net_open(SNETIO *io, const char *login, const char *url);
size_t seed_net_read(SNETIO *io, void *buffer, size_t size);
void seed_net_close(SNETIO *io, const char *logout);
int seed_net_eof(SNETIO *io);
size_t recv_callback(char *buffer, size_t size, size_t num, void *userdata);
void shift_buffer(recv_buf_info_t *info, int shift);
int seed_read_from(SNETIO *io, recv_buf_info_t *info, uint32_t flags, char *sid, int sidlen, int *reclen);
int seed_detect(char *record, int recbuflen, uint32_t flags, char *sid, int sidlen, int *readlen);
char *seed_recordsid(char *record, char *sid, int sidlen);
#if !defined(HTTP_IMPORT_DEBUG)
void signal_handler(int sig);
void *seed_write_routine(void *arg);
int check_and_free_res(TAOS_RES **res, const char *cmd);
uint64_t hash_key(char *data, size_t len);
#endif


int    running         = 1;
int    quit            = 0;
char  *default_host    = "localhost";
char  *default_user    = "root";
char  *default_passwd  = "taosdata";
char  *default_port    = "6030";
char  *default_topic   = "packet";



int main(int argc, char *argv[])
{
    int              i, ret;
    int              opt;
    int              reclen;
    int64_t          packets;
    time_t           start, end;
    SNETIO           io;
    recv_buf_info_t  info;
    uint32_t         flags   = MSF_SKIPNOTDATA;
    char            *login   = NULL;
    char            *logout  = NULL;
    char            *url     = NULL;
    struct sigaction act;
#if !defined(HTTP_IMPORT_DEBUG)
    int              np;
    int              id;
    int              iretry  = 0;
    int              icount  = 0;
    TAOS            *taos    = NULL;
    TAOS_RES        *res     = NULL;
    char             cmd[MAX_TSQL_LEN];
    char             sid[LM_SIDLEN];
    long             numport;
    char            *host    = NULL;
    char            *user    = NULL;
    char            *passwd  = NULL;
    char            *port    = NULL;
    char            *topic   = NULL;
    char            *retry   = NULL;
    char            *count   = NULL;
    int              last;
    pthread_t        tid[TQ_CHAN_NUM];
    thread_arg_t    *arg     = NULL;
    char            *base64;
    uint64_t         hash;
    struct timeval   now;
#else
    FILE            *fp      = NULL;
    char            *ofile   = NULL;
#endif 

#if !defined(HTTP_IMPORT_DEBUG)
    while ((opt = getopt(argc, argv, "l:L:i:h:u:p:P:t:r:c:")) != -1) {   
        switch (opt) {
        case 'l':
            login = strdup(optarg);
            break;
        case 'L':
            logout = strdup(optarg);
            break;
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
        case 'r':
            retry = strdup(optarg);
            break;
        case 'c':
            count = strdup(optarg);
            break;
        default:
	    fprintf(stderr, "Usage: %s -i url[ -l login -L logout -h host -u user -P port -t topic -r <on|off> -c count]\r\n", argv[0]);
	    goto failed;
        }
    }

    if (url == NULL || url[0] == '\0') {
        fprintf(stderr, "Usage: %s -i url[ -l login -L logout -h host -u user -P port -t topic -r <on|off> -c count]\r\n", argv[0]);
        goto failed;
    }

    if (retry) {
        if (strncasecmp(retry, "on", strlen(retry)) && strncasecmp(retry, "off", strlen(retry))) {
          fprintf(stderr, "invalid retry option: %s!\r\n", retry);
          goto failed;
        }

        if (strncasecmp(retry, "on", strlen(retry)) == 0) {
          iretry = 1;
        }
    }

    if (iretry == 0) {
        if (count) {
            fprintf(stderr, "-r option not specified or retry specified as \"off\", -c option ignored!\r\n");
        }
    } else {
        if (count) {
            icount = (int) strtol(count, NULL, 10);
            if (errno == EINVAL || errno == ERANGE || icount < 0) {
                fprintf(stderr, "the option -c with invalid number!\r\n");
                goto failed;
            }
        }
    }

    if (host == NULL) {
        host = default_host;
    }

    if (user == NULL) {
        user = default_user;
    }

    if (passwd == NULL) {
        passwd = default_passwd;
    }

    if (port == NULL) {
        port = default_port;
    }

    if (topic == NULL) {
        topic = default_topic;
    }

    numport = strtol(port, NULL, 10);
    if (errno == EINVAL || errno == ERANGE || (numport < 0 || numport > 65535)) {
        fprintf(stderr, "the option -p with invalid number!\r\n");
        goto failed;
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# Login URL:                             %s\r\n", login);
    fprintf(stderr, "# Logout URL:                            %s\r\n", logout);
    fprintf(stderr, "# URL:                                   %s\r\n", url);
    fprintf(stderr, "# Host:                                  %s\r\n", host);
    fprintf(stderr, "# User:                                  %s\r\n", user);
    fprintf(stderr, "# Port:                                  %s\r\n", port);
    fprintf(stderr, "# Topic:                                 %s\r\n", topic);
    fprintf(stderr, "# Retry:                                 %s\r\n", retry);
    fprintf(stderr, "# Retry Count:                           %s\r\n", count);
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
	    goto failed;
        }
    }

    if (url == NULL || url[0] == '\0') {
        fprintf(stderr, "the option -i was missing!\r\n");
        goto failed;
    }

    if (ofile == NULL || ofile[0] == '\0') {
        fprintf(stderr, "the option -o was missing!\r\n");
        goto failed;
    }

    fprintf(stderr, "################################################################\r\n");
    fprintf(stderr, "# URL:                                   %s\r\n", url);
    fprintf(stderr, "# Output:                                %s\r\n", ofile);
    fprintf(stderr, "################################################################\r\n");
#endif

    act.sa_handler = signal_handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);

    packets = 0;

#if !defined(HTTP_IMPORT_DEBUG)
retry:
#endif

    memset(&io, 0, sizeof(SNETIO));
    memset(&info, 0, sizeof(info));

    start = time(NULL);

    ret = seed_net_open(&io, login, url);
    if (ret != 0) {
        fprintf(stderr, "failed to open %s\r\n", url);
        goto failed;
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
    np = snprintf(cmd, sizeof(cmd), "create topic if not exists %s partitions %d;", topic, TQ_CHAN_NUM);
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

#if !defined(HTTP_IMPORT_DEBUG)
    np = 0;
    id = 0;
    reclen = 0;
    memset(sid, 0, LM_SIDLEN);

    if (arg == NULL) {
        arg = (thread_arg_t *) malloc(TQ_CHAN_NUM * sizeof(thread_arg_t));
        if (arg == NULL) {
            goto failed;
        }
    }

    memset(arg, 0, TQ_CHAN_NUM * sizeof(thread_arg_t));

    for (i = 0; i < TQ_CHAN_NUM; i++) {
        arg[i].index = i;
        arg[i].taos = taos;

        pthread_create(&tid[i], NULL, seed_write_routine, (void *) &arg[i]);
    }
#endif

    while (running) {
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

        packets++;

#if !defined(HTTP_IMPORT_DEBUG)
        base64 = base64_encode((const unsigned char *) info.readbuffer + info.readoffset - reclen, reclen);
        if (base64 == NULL) {
            fprintf(stderr, "base64 error\r\n");
            continue;
        }

        hash = hash_key(sid, strlen(sid));
        id = (int) (hash % TQ_CHAN_NUM);

        //pthread_mutex_lock(&arg[id].buffer.mutex);

        last = (arg[id].buffer.last + 1) % CIR_BUF_NUM;

        if (last == arg[id].buffer.pos) {
            fprintf(stderr, "buffer(%d) was full\r\n", id);
            //pthread_mutex_unlock(&arg[id].buffer.mutex);
            continue;
        }

	reclen = strlen(base64);

        if (reclen > CIR_BUF_SIZE - 1) {
            fprintf(stderr, "too long(%d) packet\r\n", reclen);
            //pthread_mutex_unlock(&arg[id].buffer.mutex);
            continue;
        }

        arg[id].buffer.data[arg[id].buffer.last][reclen] = '\0';
        memcpy(arg[id].buffer.data[arg[id].buffer.last], base64, reclen);

        arg[id].buffer.last = last;

        //pthread_mutex_unlock(&arg[id].buffer.mutex);

        free(base64);
#else
        if (running && fwrite(info.readbuffer + info.readoffset - reclen, 1, reclen, fp) != reclen) {
            fprintf(stderr, "failed to write to %s\r\n", ofile);
            goto failed;
        }
#endif

        if (packets % 50000 == 0) {
            fprintf(stdout, "%ld packet(s) transfered\r\n", packets);
        }
    }

    end = time(NULL);

    gettimeofday(&now, NULL);

    if (ret != MS_ENDOFFILE) {
        fprintf(stderr, "terminated prematurely, %ld packet(s) transfered, %lds elapsed, now: %ld\r\n",
                packets, end - start, now.tv_sec * 1000000 + now.tv_usec);
    } else {
        fprintf(stdout, "done, %ld packet(s) transfered, %lds elapsed, now: %ld\r\n",
                packets, end - start, now.tv_sec * 1000000 + now.tv_usec);
    }

failed:

    seed_net_close(&io, logout);

#if !defined(HTTP_IMPORT_DEBUG)
    for (i = 0; i < TQ_CHAN_NUM; i++) {
        pthread_join(tid[i], NULL);
    }

    if (taos) {
        taos_close(taos);
    }

    if (iretry == 1 && quit == 0) {
        if (count) {
            if (icount > 0) {
                icount--;
                running = 1;
                goto retry;
            }
        } else {
            running = 1;
            goto retry;
        }
    }

    if (arg) {
        free(arg);
    }

    if (url) {
        free(url);
    }

    if (login) {
        free(login);
    }

    if (logout) {
        free(logout);
    }

    if (retry) {
        free(retry);
    }

    if (count) {
        free(count);
    }

    if (host != default_host) {
        free(host);
    }

    if (user != default_user) {
        free(user);
    }

    if (passwd != default_passwd) {
        free(passwd);
    }

    if (port != default_port) {
        free(port);
    }

    if (topic != default_topic) {
        free(topic);
    }
#else
    if (fp) {
        fclose(fp);
    }

    if (url) {
        free(url);
    }

    if (ofile) {
        free(ofile);
    }
#endif

    return 0;
}


int
seed_net_open(SNETIO *io, const char *login, const char *url)
{
    long     http_code;
    CURLcode res;

    if (io == NULL || url == NULL) {
        return -1;
    }

    if (login) {
        io->curl = curl_easy_init();
        if (io->curl == NULL) {
            fprintf(stderr, "failed to initialize curl for login\r\n");
            exit(1);
        }

        /* Set Login URL */
        if (curl_easy_setopt(io->curl, CURLOPT_URL, login) != CURLE_OK) {
            fprintf(stderr, "could not set CURLOPT_URL: %s\r\n", login);
            goto failed;
        }

        if (curl_easy_setopt(io->curl, CURLOPT_COOKIEJAR, "/tmp/cookies.txt") != CURLE_OK) {
            fprintf(stderr, "could not set CURLOPT_COOKIEJAR\r\n");
            goto failed;
        }

        res = curl_easy_perform(io->curl);
        if (res != CURLE_OK) {
            fprintf(stderr, "curl perform failed for login: %s\n", curl_easy_strerror(res));
            goto failed;
        }

        curl_easy_cleanup(io->curl);
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

    if (login) {
        if (curl_easy_setopt(io->curl, CURLOPT_COOKIEFILE, "/tmp/cookies.txt") != CURLE_OK) {
            fprintf(stderr, "could not set CURLOPT_COOKIEFILE\r\n");
            goto failed;
        }
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
    if (io->curl) {
        if (io->curlm) {
            curl_multi_remove_handle(io->curlm, io->curl);
            curl_multi_cleanup(io->curlm);
        }

        curl_easy_cleanup(io->curl);
    }

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
seed_net_close(SNETIO *io, const char *logout)
{
    CURLcode res;

    if (io && io->curl) {
        if (io->curlm) {
            curl_multi_remove_handle(io->curlm, io->curl);
            curl_multi_cleanup(io->curlm);
        }

        curl_easy_cleanup(io->curl);
    }

    if (logout) {
        /* Set Logout URL */
        io->curl = curl_easy_init();

        if (curl_easy_setopt(io->curl, CURLOPT_URL, logout) != CURLE_OK) {
            fprintf(stderr, "could not set CURLOPT_URL: %s\r\n", logout);
        }

        res = curl_easy_perform(io->curl);
        if (res != CURLE_OK) {
            fprintf(stderr, "curl perform failed for logout: %s\n", curl_easy_strerror(res));
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
void signal_handler(int sig)
{
    quit = 1;
    running = 0;
}


void *seed_write_routine(void *arg)
{
    int             np, offset, cursor, rows;
    int64_t         total_rows, prv, ts;
    char            cmd[MAX_TSQL_LEN];
    const char     *prefix  = "insert into";
    const int       pfxlen  = sizeof("insert into") - 1;
    struct timeval  now;
    thread_arg_t   *p;
    TAOS_RES       *res;

    p = (thread_arg_t *) arg;

    offset = 0;
    rows = 0;
    total_rows = 0;
    prv = -1;

    memset(cmd, 0, sizeof(cmd));
    memcpy(cmd, prefix, pfxlen);
    offset += pfxlen;

    np = snprintf(cmd + offset, sizeof(cmd) - offset,
                  " p%d using ps tags (%d) values", p->index + 1, p->index + 1);
    if (np <= 0) {
        fprintf(stderr, "thread(%d): fprintf error cmd for preparing data prefix: %s\r\n", p->index + 1, cmd);
        running = 0;
        return NULL;
    }

    offset += np;
    cursor = offset;

    while (running) {
        //pthread_mutex_lock(&p->buffer.mutex);

        if (p->buffer.pos == p->buffer.last) {
            //fprintf(stderr, "thread(%d): buffer(%d) was empty\r\n", p->index + 1, p->index);
            //pthread_mutex_unlock(&p->buffer.mutex);
            usleep(10);
            continue;
        }

        //pthread_mutex_unlock(&p->buffer.mutex);

        if (offset + strlen(p->buffer.data[p->buffer.pos]) < (MAX_TSQL_LEN - 256) && rows < MAX_DB_ROWS) {
            gettimeofday(&now, NULL);
            ts = (int64_t) (now.tv_sec * 1000000 + now.tv_usec);

            /* so fast */
            if (prv == ts) {
                usleep(5);
                gettimeofday(&now, NULL);
                ts = (int64_t) (now.tv_sec * 1000000 + now.tv_usec);
            }

            prv = ts;

            np = snprintf(cmd + offset, sizeof(cmd) - offset, " (%ld, %ld, '%s')",
                          ts, ts, p->buffer.data[p->buffer.pos]);
            if (np <= 0) {
                fprintf(stderr, "thread(%d): fprintf error cmd for preparing data: %s\r\n", p->index + 1, cmd);
                continue;
            }

            offset += np;
            rows++;
            total_rows++;
            p->buffer.pos++;
	    p->buffer.pos %= CIR_BUF_NUM;
        } else {
            if (rows >= MAX_DB_ROWS) {
                fprintf(stderr, "thread(%d): too many rows: %d\r\n", p->index + 1, rows);
            } else {
                cmd[offset] = ';';

                res = taos_query(p->taos, cmd);
                if (check_and_free_res(&res, cmd) != 0) {
                    running = 0;
                    break;
                }
            }

            //fprintf(stderr, "thread(%d): rows: %d\r\n", p->index + 1, rows);
            memset(cmd + cursor, 0, sizeof(cmd) - cursor);
            offset = cursor;
            rows = 0;
        }
    }

    if (offset) {
        cmd[offset] = ';';

        res = taos_query(p->taos, cmd);
        check_and_free_res(&res, cmd);
    }

    //fprintf(stderr, "thread(%d): total rows: %ld\r\n", p->index + 1, total_rows);

    return NULL;
}


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
