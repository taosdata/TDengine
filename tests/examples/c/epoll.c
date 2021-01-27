/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// how to use to do a pressure-test upon eok
// tester:               cat /dev/urandom | nc -c <ip> <port>
// testee:               ./debug/build/bin/epoll -l <port> > /dev/null
// compare against:      nc -l <port> > /dev/null
// monitor and compare : glances

#ifdef __APPLE__
#include "eok.h"
#else // __APPLE__
#include <sys/epoll.h>
#endif // __APPLE__
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <libgen.h>

#define D(fmt, ...) fprintf(stderr, "%s[%d]%s(): " fmt "\n", basename(__FILE__), __LINE__, __func__, ##__VA_ARGS__)
#define A(statement, fmt, ...) do {                                     \
  if (statement) break;                                                 \
  fprintf(stderr, "%s[%d]%s(): assert [%s] failed: %d[%s]: " fmt "\n",  \
          basename(__FILE__), __LINE__, __func__,                       \
          #statement, errno, strerror(errno),                           \
          ##__VA_ARGS__);                                               \
  abort();                                                              \
} while (0)

#define E(fmt, ...) do {                                                \
  fprintf(stderr, "%s[%d]%s(): %d[%s]: " fmt "\n",                      \
          basename(__FILE__), __LINE__, __func__,                       \
          errno, strerror(errno),                                       \
          ##__VA_ARGS__);                                               \
} while (0)

typedef struct ep_s            ep_t;
struct ep_s {
  int                    ep;

  pthread_mutex_t        lock;
  int                    sv[2];  // 0 for read, 1 for write;
  pthread_t              thread;

  volatile unsigned int  stopping:1;
  volatile unsigned int  waiting:1;
  volatile unsigned int  wakenup:1;
};

static int ep_dummy = 0;

static ep_t* ep_create(void);
static void  ep_destroy(ep_t *ep);
static void* routine(void* arg);
static int open_listen(unsigned short port);

typedef struct fde_s          fde_t;
struct fde_s {
  int                          skt;
  void (*on_event)(ep_t *ep, struct epoll_event *events, fde_t *client);
};

static void listen_event(ep_t *ep, struct epoll_event *ev, fde_t *client);
static void null_event(ep_t *ep, struct epoll_event *ev, fde_t *client);

#define usage(arg0, fmt, ...)       do {                                               \
  if (fmt[0]) {                                                                        \
    fprintf(stderr, "" fmt "\n", ##__VA_ARGS__);                                       \
  }                                                                                    \
  fprintf(stderr, "usage:\n");                                                         \
  fprintf(stderr, "  %s -l <port>             : specify listenning port\n", arg0);     \
} while (0)

int main(int argc, char *argv[]) {
  char *prg = basename(argv[0]);
  if (argc==1) {
    usage(prg, "");
    return 0;
  }
  ep_t* ep = ep_create();
  A(ep, "failed");
  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (0==strcmp(arg, "-l")) {
      ++i;
      if (i>=argc) {
        usage(prg, "expecting <port> after -l, but got nothing");
        return 1; // confirmed potential leakage
      }
      arg = argv[i];
      int port = atoi(arg);
      int skt = open_listen(port);
      if (skt==-1) continue;
      fde_t *client = (fde_t*)calloc(1, sizeof(*client));
      if (!client) {
        E("out of memory");
        close(skt);
        continue;
      }
      client->skt = skt;
      client->on_event = listen_event;
      struct epoll_event ev = {0};
      ev.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
      ev.data.ptr = client;
      A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, skt, &ev), "");
      continue;
    }
    usage(prg, "unknown argument: [%s]", arg);
    return 1;
  }
  char *line = NULL;
  size_t linecap = 0;
  ssize_t linelen;
  while ((linelen = getline(&line, &linecap, stdin)) > 0) {
    line[strlen(line)-1] = '\0';
    if (0==strcmp(line, "exit")) break;
    if (0==strcmp(line, "quit")) break;
    if (line==strstr(line, "close")) {
      int fd = 0;
      sscanf(line, "close %d", &fd);
      if (fd<=2) {
        fprintf(stderr, "fd [%d] invalid\n", fd);
        continue;
      }
      A(0==epoll_ctl(ep->ep, EPOLL_CTL_DEL, fd, NULL), "");
      continue;
    }
    if (strlen(line)==0) continue;
    fprintf(stderr, "unknown cmd:[%s]\n", line);
  }
  ep_destroy(ep);
  D("");
  return 0;
}

ep_t* ep_create(void) {
  ep_t *ep = (ep_t*)calloc(1, sizeof(*ep));
  A(ep, "out of memory");
  A(-1!=(ep->ep = epoll_create(1)), "");
  ep->sv[0] = -1;
  ep->sv[1] = -1;
  A(0==socketpair(AF_LOCAL, SOCK_STREAM, 0, ep->sv), "");
  A(0==pthread_mutex_init(&ep->lock, NULL), "");
  A(0==pthread_mutex_lock(&ep->lock), "");
  struct epoll_event ev = {0};
  ev.events   = EPOLLIN;
  ev.data.ptr = &ep_dummy;
  A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, ep->sv[0], &ev), "");
  A(0==pthread_create(&ep->thread, NULL, routine, ep), "");
  A(0==pthread_mutex_unlock(&ep->lock), "");
  return ep;
}

static void ep_destroy(ep_t *ep) {
  A(ep, "invalid argument");
  ep->stopping = 1;
  A(1==send(ep->sv[1], "1", 1, 0), "");
  A(0==pthread_join(ep->thread, NULL), "");
  A(0==pthread_mutex_destroy(&ep->lock), "");
  A(0==close(ep->sv[0]), "");
  A(0==close(ep->sv[1]), "");
  A(0==close(ep->ep), "");
  free(ep);
}

static void* routine(void* arg) {
  A(arg, "invalid argument");
  ep_t *ep = (ep_t*)arg;

  while (!ep->stopping) {
    struct epoll_event evs[10];
    memset(evs, 0, sizeof(evs));

    A(0==pthread_mutex_lock(&ep->lock), "");
    A(ep->waiting==0, "internal logic error");
    ep->waiting = 1;
    A(0==pthread_mutex_unlock(&ep->lock), "");

    int r = epoll_wait(ep->ep, evs, sizeof(evs)/sizeof(evs[0]), -1);
    A(r>0, "indefinite epoll_wait shall not timeout:[%d]", r);

    A(0==pthread_mutex_lock(&ep->lock), "");
    A(ep->waiting==1, "internal logic error");
    ep->waiting = 0;
    A(0==pthread_mutex_unlock(&ep->lock), "");

    for (int i=0; i<r; ++i) {
      struct epoll_event *ev = evs + i;
      if (ev->data.ptr == &ep_dummy) {
        char c = '\0';
        A(1==recv(ep->sv[0], &c, 1, 0), "internal logic error");
        A(0==pthread_mutex_lock(&ep->lock), "");
        ep->wakenup = 0;
        A(0==pthread_mutex_unlock(&ep->lock), "");
        D("........");
        continue;
      }
      A(ev->data.ptr, "internal logic error");
      fde_t *client = (fde_t*)ev->data.ptr;
      client->on_event(ep, ev, client);
      continue;
    }
  }
  return NULL;
}

static int open_listen(unsigned short port) {
  int r = 0;
  int skt = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (skt==-1) {
    E("socket() failed");
    return -1;
  }
  do {
    struct sockaddr_in si = {0};
    si.sin_family = AF_INET;
    si.sin_addr.s_addr = inet_addr("0.0.0.0");
    si.sin_port = htons(port);
    r = bind(skt, (struct sockaddr*)&si, sizeof(si));
    if (r) {
      E("bind(%u) failed", port);
      break;
    }
    r = listen(skt, 100);
    if (r) {
      E("listen() failed");
      break;
    }
    memset(&si, 0, sizeof(si));
    socklen_t len = sizeof(si);
    r = getsockname(skt, (struct sockaddr *)&si, &len);
    if (r) {
      E("getsockname() failed");
    }
    A(len==sizeof(si), "internal logic error");
    D("listenning at: %d", ntohs(si.sin_port));
    return skt;
  } while (0);
  close(skt);
  return -1;
}

static void listen_event(ep_t *ep, struct epoll_event *ev, fde_t *client) {
  A(ev->events & EPOLLIN, "internal logic error");
  struct sockaddr_in si = {0};
  socklen_t silen = sizeof(si);
  int skt = accept(client->skt, (struct sockaddr*)&si, &silen);
  A(skt!=-1, "internal logic error");
  fde_t *server = (fde_t*)calloc(1, sizeof(*server));
  if (!server) {
    close(skt);
    return;
  }
  server->skt = skt;
  server->on_event = null_event;
  struct epoll_event ee = {0};
  ee.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
  ee.data.ptr = server;
  A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, skt, &ee), "");
}

static void null_event(ep_t *ep, struct epoll_event *ev, fde_t *client) {
  if (ev->events & EPOLLIN) {
    char buf[8192];
    int n = recv(client->skt, buf, sizeof(buf), 0);
    A(n>=0 && n<=sizeof(buf), "internal logic error:[%d]", n);
    A(n==fwrite(buf, 1, n, stdout), "internal logic error");
  }
  if (ev->events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
    A(0==pthread_mutex_lock(&ep->lock), "");
    A(0==epoll_ctl(ep->ep, EPOLL_CTL_DEL, client->skt, NULL), "");
    A(0==pthread_mutex_unlock(&ep->lock), "");
    close(client->skt);
    client->skt = -1;
    client->on_event = NULL;
    free(client);
  }
}

