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

int main(int argc, char *argv[]) {
  ep_t* ep = ep_create();
  A(ep, "failed");
  int skt = open_connect(6789);
  if (skt!=-1) {
    struct epoll_event ev = {0};
    ev.events   = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    ev.data.ptr = &skt;
    A(0==epoll_ctl(ep->ep, EPOLL_CTL_ADD, skt, &ev), "");
  }
  getchar();
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
    struct epoll_event evs[10] = {0};

    A(0==pthread_mutex_lock(&ep->lock), "");
    A(ep->waiting==0, "internal logic error");
    ep->waiting = 1;
    A(0==pthread_mutex_unlock(&ep->lock), "");

    int r = epoll_wait(ep->ep, evs, sizeof(evs)/sizeof(evs[0]), -1);
    A(r>0, "indefinite epoll_wait shall not timeout");

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
      int skt = *(int*)ev->data.ptr;
      if (ev->events & EPOLLIN) {
        char buf[4];
        int n = recv(skt, buf, sizeof(buf)-1, 0);
        A(n>=0 && n<sizeof(buf), "internal logic error");
        buf[n] = '\0';
        fprintf(stderr, "events[%x]:%s\n", ev->events, buf);
      }
      if (ev->events & EPOLLRDHUP) {
        A(0==epoll_ctl(ep->ep, EPOLL_CTL_DEL, skt, NULL), "");
      }
      continue;
    }
  }
  return NULL;
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

