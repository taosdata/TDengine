#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <errno.h>
#include <signal.h>


#define RECV_MAX_LINE 2048
#define ITEM_MAX_LINE 128
#define REQ_MAX_LINE  2048
#define REQ_CLI_COUNT 100


typedef enum
{
    uninited,
    connecting,
    connected,
    datasent
} conn_stat;


typedef enum
{
    false,
    true
} bool;


typedef unsigned short u16_t;
typedef unsigned int u32_t;


typedef struct
{
    int                 sockfd;
    int                 index;
    conn_stat           state;
    size_t              nsent;
    size_t              nrecv;
    size_t              nlen;
    bool                error;
    bool                success;
    struct sockaddr_in  serv_addr;
} socket_ctx;


int set_nonblocking(int sockfd)
{
    int ret;

    ret = fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) | O_NONBLOCK);
    if (ret == -1) {
        printf("failed to fcntl for %d\r\n", sockfd);
        return ret;
    }

    return ret;
}


int create_socket(const char *ip, const u16_t port, socket_ctx *pctx)
{
    int                 ret;

    if (ip == NULL || port == 0 || pctx == NULL) {
        printf("invalid parameter\r\n");
        return -1;
    }

    pctx->sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (pctx->sockfd == -1) {
        printf("failed to create socket\r\n");
        return -1;
    }

    bzero(&pctx->serv_addr, sizeof(struct sockaddr_in));

    pctx->serv_addr.sin_family = AF_INET;
    pctx->serv_addr.sin_port = htons(port);

    ret = inet_pton(AF_INET, ip, &pctx->serv_addr.sin_addr);
    if (ret <= 0) {
        printf("inet_pton error, ip: %s\r\n", ip);
        return -1;
    }

    ret = set_nonblocking(pctx->sockfd);
    if (ret == -1) {
        printf("failed to set %d as nonblocking\r\n", pctx->sockfd);
        return -1;
    }

    return pctx->sockfd;
}


void close_sockets(socket_ctx *pctx, int cnt)
{
    int  i;

    if (pctx == NULL) {
        return;
    }

    for (i = 0; i < cnt; i++) {
        if (pctx[i].sockfd > 0) {
            close(pctx[i].sockfd);
            pctx[i].sockfd = -1;
        }
    }
}


int proc_pending_error(socket_ctx *ctx)
{
    int       ret;
    int       err;
    socklen_t len;

    if (ctx == NULL) {
        return 0;
    }

    err = 0;
    len = sizeof(int);

    ret = getsockopt(ctx->sockfd, SOL_SOCKET, SO_ERROR, (void *)&err, &len);
    if (ret == -1) {
        err = errno;
    }

    if (err) {
        printf("failed to connect at index: %d\r\n", ctx->index);

        close(ctx->sockfd);
        ctx->sockfd = -1;

        return -1;
    }

    return 0;
}


void build_http_request(char *ip, u16_t port, char *url, char *sql, char *req_buf, int len)
{
    char         req_line[ITEM_MAX_LINE];
    char         req_host[ITEM_MAX_LINE];
    char         req_cont_type[ITEM_MAX_LINE];
    char         req_cont_len[ITEM_MAX_LINE];
    const char*  req_auth = "Authorization: Basic cm9vdDp0YW9zZGF0YQ==\r\n";

    if (ip == NULL || port == 0 ||
        url == NULL || url[0] == '\0' ||
        sql == NULL || sql[0] == '\0' ||
        req_buf == NULL || len <= 0)
    {
        return;
    }

    snprintf(req_line, ITEM_MAX_LINE, "POST %s HTTP/1.1\r\n", url);
    snprintf(req_host, ITEM_MAX_LINE, "HOST: %s:%d\r\n", ip, port);
    snprintf(req_cont_type, ITEM_MAX_LINE, "%s\r\n", "Content-Type: text/plain");
    snprintf(req_cont_len, ITEM_MAX_LINE, "Content-Length: %ld\r\n\r\n", strlen(sql));

    snprintf(req_buf, len, "%s%s%s%s%s%s", req_line, req_host, req_auth, req_cont_type, req_cont_len, sql);
}


int add_event(int epfd, int sockfd, u32_t events, void *data)
{
    struct epoll_event evs_op;

    evs_op.data.ptr = data;
    evs_op.events = events;

    return epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &evs_op);
}


int mod_event(int epfd, int sockfd, u32_t events, void *data)
{
    struct epoll_event evs_op;

    evs_op.data.ptr = data;
    evs_op.events = events;

    return epoll_ctl(epfd, EPOLL_CTL_MOD, sockfd, &evs_op);
}


int del_event(int epfd, int sockfd)
{
    struct epoll_event evs_op;

    evs_op.events = 0;
    evs_op.data.ptr = NULL;

    return epoll_ctl(epfd, EPOLL_CTL_DEL, sockfd, &evs_op);
}


int main()
{
    int                  i;
    int                  ret, n, nsent, nrecv;
    int                  epfd;
    u32_t                events;
    char                *str;
    socket_ctx          *pctx, ctx[REQ_CLI_COUNT];
    char                *ip = "127.0.0.1";
    char                *url = "/rest/sql";
    u16_t                port = 6041;
    struct epoll_event   evs[REQ_CLI_COUNT];
    char                 sql[REQ_MAX_LINE];
    char                 send_buf[REQ_CLI_COUNT][REQ_MAX_LINE + 5 * ITEM_MAX_LINE];
    char                 recv_buf[REQ_CLI_COUNT][RECV_MAX_LINE];
    int                  count;

    signal(SIGPIPE, SIG_IGN);

    for (i = 0; i < REQ_CLI_COUNT; i++) {
        ctx[i].sockfd = -1;
        ctx[i].index = i;
        ctx[i].state = uninited;
        ctx[i].nsent = 0;
        ctx[i].nrecv = 0;
        ctx[i].error = false;
        ctx[i].success = false;

        memset(sql, 0, REQ_MAX_LINE);
        memset(send_buf[i], 0, REQ_MAX_LINE + 5 * ITEM_MAX_LINE);
        memset(recv_buf[i], 0, RECV_MAX_LINE);

        snprintf(sql, REQ_MAX_LINE, "use db%d", i);

        build_http_request(ip, port, url, sql, send_buf[i], REQ_MAX_LINE + 5 * ITEM_MAX_LINE);

        ctx[i].nlen = strlen(send_buf[i]);
    }

    epfd = epoll_create(REQ_CLI_COUNT);
    if (epfd <= 0) {
        printf("failed to create epoll\r\n");
        goto failed;
    }

    for (i = 0; i < REQ_CLI_COUNT; i++) {
        ret = create_socket(ip, port, &ctx[i]);
        if (ret == -1) {
            printf("failed to create socket, index: %d\r\n", i);
            goto failed;
        }
    }

    for (i = 0; i < REQ_CLI_COUNT; i++) {
        events = EPOLLET | EPOLLIN | EPOLLOUT;
        ret = add_event(epfd, ctx[i].sockfd, events, (void *) &ctx[i]);
        if (ret == -1) {
            printf("failed to add sockfd to epoll, index: %d\r\n", i);
            goto failed;
        }
    }

    count = 0;

    for (i = 0; i < REQ_CLI_COUNT; i++) {
        ret = connect(ctx[i].sockfd, (struct sockaddr *) &ctx[i].serv_addr, sizeof(ctx[i].serv_addr));
        if (ret == -1) {
            if (errno != EINPROGRESS) {
                printf("connect error, index: %d\r\n", ctx[i].index);
                (void) del_event(epfd, ctx[i].sockfd);
                close(ctx[i].sockfd);
                ctx[i].sockfd = -1;
            } else {
                ctx[i].state = connecting;
                count++;
            }

            continue;
        }

        ctx[i].state = connected;
        count++;
    }

    printf("clients: %d\r\n", count);

    while (count > 0) {
        n = epoll_wait(epfd, evs, REQ_CLI_COUNT, 2);
        if (n == -1) {
            if (errno != EINTR) {
                printf("epoll_wait error, reason: %s\r\n", strerror(errno));
                break;
            }
        } else {
            for (i = 0; i < n; i++) {
                if (evs[i].events & EPOLLERR) {
                    pctx = (socket_ctx *) evs[i].data.ptr;
                    printf("event error, index: %d\r\n", pctx->index);
                    close(pctx->sockfd);
                    pctx->sockfd = -1;
                    count--;
                } else if (evs[i].events & EPOLLIN) {
                    pctx = (socket_ctx *) evs[i].data.ptr;
                    if (pctx->state == connecting) {
                        ret = proc_pending_error(pctx);
                        if (ret == 0) {
                            printf("client connected, index: %d\r\n", pctx->index);
                            pctx->state = connected;
                        } else {
                            printf("client connect failed, index: %d\r\n", pctx->index);
                            (void) del_event(epfd, pctx->sockfd);
                            close(pctx->sockfd);
                            pctx->sockfd = -1;
                            count--;

                            continue;
                        }
                    }

                    for ( ;; ) {
                        nrecv = recv(pctx->sockfd, recv_buf[pctx->index] + pctx->nrecv, RECV_MAX_LINE, 0);
                        if (nrecv == -1) {
                            if (errno != EAGAIN && errno != EINTR) {
                                printf("failed to recv, index: %d, reason: %s\r\n", pctx->index, strerror(errno));
                                (void) del_event(epfd, pctx->sockfd);
                                close(pctx->sockfd);
                                pctx->sockfd = -1;
                                count--;
                            }

                            break;
                        } else if (nrecv == 0) {
                            printf("peer closed connection, index: %d\r\n", pctx->index);
                            (void) del_event(epfd, pctx->sockfd);
                            close(pctx->sockfd);
                            pctx->sockfd = -1;
                            count--;
                            break;
                        }

                        pctx->nrecv += nrecv;
                        if (pctx->nrecv > 12) {
                            if (pctx->error == false && pctx->success == false) {
                                str = recv_buf[pctx->index] + 9;
                                if (str[0] != '2' || str[1] != '0' || str[2] != '0') {
                                    printf("response error, index: %d, recv: %s\r\n", pctx->index, recv_buf[pctx->index]);
                                    pctx->error = true;
                                } else {
                                    printf("response ok, index: %d\r\n", pctx->index);
                                    pctx->success = true;
                                }
                            }
                        }
                    }
                } else if (evs[i].events & EPOLLOUT) {
                    pctx = (socket_ctx *) evs[i].data.ptr;
                    if (pctx->state == connecting) {
                        ret = proc_pending_error(pctx);
                        if (ret == 0) {
                            printf("client connected, index: %d\r\n", pctx->index);
                            pctx->state = connected;
                        } else {
                            printf("client connect failed, index: %d\r\n", pctx->index);
                            (void) del_event(epfd, pctx->sockfd);
                            close(pctx->sockfd);
                            pctx->sockfd = -1;
                            count--;

                            continue;
                        }
                    }

                    for ( ;; ) {
                        nsent = send(pctx->sockfd, send_buf[pctx->index] + pctx->nsent, pctx->nlen - pctx->nsent, 0);
                        if (nsent == -1) {
                            if (errno != EAGAIN && errno != EINTR) {
                                printf("failed to send, index: %d\r\n", pctx->index);
                                (void) del_event(epfd, pctx->sockfd);
                                close(pctx->sockfd);
                                pctx->sockfd = -1;
                                count--;
                            }

                            break;
                        }

                        if (nsent == (int) (pctx->nlen - pctx->nsent)) {
                            printf("request done, request: %s, index: %d\r\n", send_buf[pctx->index], pctx->index);

                            pctx->state = datasent;

                            events = EPOLLET | EPOLLIN;
                            (void) mod_event(epfd, pctx->sockfd, events, (void *)pctx);

                            break;
                        } else {
                            pctx->nsent += nsent;
                        }
                    }
                } else {
                    pctx = (socket_ctx *) evs[i].data.ptr;
                    printf("unknown event(%u), index: %d\r\n", evs[i].events, pctx->index);
                    (void) del_event(epfd, pctx->sockfd);
                    close(pctx->sockfd);
                    pctx->sockfd = -1;
                    count--;
                }
            }
        }
    }

failed:

    if (epfd > 0) {
        close(epfd);
    }

    close_sockets(ctx, REQ_CLI_COUNT);

    return 0;
}
