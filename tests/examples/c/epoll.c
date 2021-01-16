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

#ifdef __APPLE__
#include "eok.h"
#else
#include <sys/epoll.h>
#endif
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
static int open_connect(unsigned short port);
static int open_listen(unsigned short port);

typedef struct client_s          client_t;
struct client_s {
  int                          skt;
  void (*on_event)(ep_t *ep, struct epoll_event *events, client_t *client);
  volatile unsigned int        state; // 1: listenning; 2: connected
};

static void echo_event(ep_t *ep, struct epoll_event *ev, client_t *client);

int main(int argc, char *argv[]) {
  ep_t* ep = ep_create();
  A(ep, "failed");
  int skt = open_connect(6789);
  if (skt!=-1) {
    client_t *client = (client_t*)calloc(1, sizeof(*client));
    if (client) {
      client->skt = skt;
      client->on_event = echo_event;
      client->state = 2;
      struct epoll_event ev = {0};
      ev.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
      ev.data.ptr = client;
      A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, skt, &ev), "");
    }
  }
  skt = open_listen(0);
  if (skt!=-1) {
    client_t *client = (client_t*)calloc(1, sizeof(*client));
    if (client) {
      client->skt = skt;
      client->on_event = echo_event;
      client->state = 1;
      struct epoll_event ev = {0};
      ev.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
      ev.data.ptr = client;
      A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, skt, &ev), "");
    }
  }
  // char c = '\0';
  // while ((c=getchar())!=EOF) {
  //   switch (c) {
  //     case 'q': break;
  //     default: continue;
  //   }
  // }
  // getchar();
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
      client_t *client = (client_t*)ev->data.ptr;
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
    si.sin_addr.s_addr = inet_addr("127.0.0.1");
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

static int open_connect(unsigned short port) {
  int r = 0;
  int skt = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (skt==-1) {
    E("socket() failed");
    return -1;
  }
  do {
    struct sockaddr_in si = {0};
    si.sin_family = AF_INET;
    si.sin_addr.s_addr = inet_addr("127.0.0.1");
    si.sin_port = htons(port);
    r = connect(skt, (struct sockaddr*)&si, sizeof(si));
    if (r) {
      E("connect(%u) failed", port);
      break;
    }
    memset(&si, 0, sizeof(si));
    socklen_t len = sizeof(si);
    r = getsockname(skt, (struct sockaddr *)&si, &len);
    if (r) {
      E("getsockname() failed");
    }
    A(len==sizeof(si), "internal logic error");
    D("connected: %d", ntohs(si.sin_port));
    return skt;
  } while (0);
  close(skt);
  return -1;
}

static void echo_event(ep_t *ep, struct epoll_event *ev, client_t *client) {
  if (ev->events & EPOLLIN) {
    if (client->state==1) {
      struct sockaddr_in si = {0};
      socklen_t silen = sizeof(si);
      int skt = accept(client->skt, (struct sockaddr*)&si, &silen);
      if (skt!=-1) {
        client_t *server = (client_t*)calloc(1, sizeof(*server));
        if (server) {
          server->skt = skt;
          server->on_event = echo_event;
          server->state = 2;
          struct epoll_event ev = {0};
          ev.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
          ev.data.ptr = server;
          A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, skt, &ev), "");
        }
      }
    }
    if (client->state==2) {
      char buf[4];
      int n = recv(client->skt, buf, sizeof(buf)-1, 0);
      A(n>=0 && n<sizeof(buf), "internal logic error:[%d]", n);
      buf[n] = '\0';
      fprintf(stderr, "events[%x]:%s\n", ev->events, buf);
    }
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

