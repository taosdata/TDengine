#ifdef WITH_EPOLL

#define _GNU_SOURCE

#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "ttqMemory.h"
#include "ttqPacket.h"
#include "ttqSend.h"
#include "ttqSystree.h"
#include "ttqTime.h"
#include "ttqUtil.h"

#define MAX_EVENTS 1000

static sigset_t           my_sigblock;
static struct epoll_event ep_events[MAX_EVENTS];

#endif

int ttqMuxInit(struct tmqtt__listener_sock *listensock, int listensock_count) {
#ifndef WITH_EPOLL
  UNUSED(listensock);
  UNUSED(listensock_count);
  return -1;
#else

  struct epoll_event ev;
  int                i;

  sigemptyset(&my_sigblock);
  sigaddset(&my_sigblock, SIGINT);
  sigaddset(&my_sigblock, SIGTERM);
  sigaddset(&my_sigblock, SIGUSR1);
  sigaddset(&my_sigblock, SIGUSR2);
  sigaddset(&my_sigblock, SIGHUP);

  memset(&ep_events, 0, sizeof(struct epoll_event) * MAX_EVENTS);

  db.epollfd = 0;
  if ((db.epollfd = epoll_create(MAX_EVENTS)) == -1) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error in epoll creating: %s", strerror(errno));
    return TTQ_ERR_UNKNOWN;
  }
  memset(&ev, 0, sizeof(struct epoll_event));
  for (i = 0; i < listensock_count; i++) {
    ev.data.ptr = &listensock[i];
    ev.events = EPOLLIN;
    if (epoll_ctl(db.epollfd, EPOLL_CTL_ADD, listensock[i].sock, &ev) == -1) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error in epoll initial registering: %s", strerror(errno));
      (void)close(db.epollfd);
      db.epollfd = 0;
      return TTQ_ERR_UNKNOWN;
    }
  }

  return TTQ_ERR_SUCCESS;

#endif
}

int ttqMuxCleanup(void) {
#ifndef WITH_EPOLL
  return -1;
#else

  (void)close(db.epollfd);
  db.epollfd = 0;
  return TTQ_ERR_SUCCESS;

#endif
}

int ttqMuxAddOut(struct tmqtt *context) {
#ifndef WITH_EPOLL
  UNUSED(context);
  return -1;
#else

  struct epoll_event ev;

  if (!(context->events & EPOLLOUT)) {
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.data.ptr = context;
    ev.events = EPOLLIN | EPOLLOUT;
    if (epoll_ctl(db.epollfd, EPOLL_CTL_ADD, context->sock, &ev) == -1) {
      if ((errno != EEXIST) || (epoll_ctl(db.epollfd, EPOLL_CTL_MOD, context->sock, &ev) == -1)) {
        ttq_log(NULL, TTQ_LOG_DEBUG, "Error in epoll re-registering to EPOLLOUT: %s", strerror(errno));
      }
    }
    context->events = EPOLLIN | EPOLLOUT;
  }

  return TTQ_ERR_SUCCESS;

#endif
}

int ttqMuxRemoveOut(struct tmqtt *context) {
#ifndef WITH_EPOLL
  UNUSED(context);
  return -1;
#else

  struct epoll_event ev;

  if (context->events & EPOLLOUT) {
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.data.ptr = context;
    ev.events = EPOLLIN;
    if (epoll_ctl(db.epollfd, EPOLL_CTL_ADD, context->sock, &ev) == -1) {
      if ((errno != EEXIST) || (epoll_ctl(db.epollfd, EPOLL_CTL_MOD, context->sock, &ev) == -1)) {
        ttq_log(NULL, TTQ_LOG_DEBUG, "Error in epoll re-registering to EPOLLIN: %s", strerror(errno));
      }
    }
    context->events = EPOLLIN;
  }

  return TTQ_ERR_SUCCESS;

#endif
}

int ttqMuxDelete(struct tmqtt *context) {
#ifndef WITH_EPOLL
  UNUSED(context);
  return -1;
#else

  struct epoll_event ev;

  memset(&ev, 0, sizeof(struct epoll_event));
  if (context->sock != INVALID_SOCKET) {
    if (epoll_ctl(db.epollfd, EPOLL_CTL_DEL, context->sock, &ev) == -1) {
      return 1;
    }
  }
  return 0;

#endif
}

#ifdef WITH_EPOLL

static int ttq_mux_add_in(struct tmqtt *context) {
  struct epoll_event ev;

  memset(&ev, 0, sizeof(struct epoll_event));
  ev.events = EPOLLIN;
  ev.data.ptr = context;
  if (epoll_ctl(db.epollfd, EPOLL_CTL_ADD, context->sock, &ev) == -1) {
    if (errno != EEXIST) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error in epoll accepting: %s", strerror(errno));
    }
  }
  context->events = EPOLLIN;
  return TTQ_ERR_SUCCESS;
}

static void ttq_handle_rw(struct tmqtt *context, uint32_t events) {
  int       err;
  socklen_t len;
  int       rc;

  if (context->sock == INVALID_SOCKET) {
    return;
  }

  if (events & EPOLLOUT) {
    if (context->state == ttq_cs_connect_pending) {
      len = sizeof(int);
      if (!getsockopt(context->sock, SOL_SOCKET, SO_ERROR, (char *)&err, &len)) {
        if (err == 0) {
          tmqtt__set_state(context, ttq_cs_new);
        }
      } else {
        ttqDisconnect(context, TTQ_ERR_CONN_LOST);
        return;
      }
    }
    rc = packet__write(context);
    if (rc) {
      ttqDisconnect(context, rc);
      return;
    }
  }

  if (events & EPOLLIN) {
    do {
      rc = packet__read(context);
      if (rc) {
        ttqDisconnect(context, rc);
        return;
      }
    } while (SSL_DATA_PENDING(context));
  } else {
    if (events & (EPOLLERR | EPOLLHUP)) {
      ttqDisconnect(context, TTQ_ERR_CONN_LOST);
      return;
    }
  }
}

#endif

int ttqMuxHandle(struct tmqtt__listener_sock *listensock, int listensock_count) {
  UNUSED(listensock_count);

#ifndef WITH_EPOLL
  UNUSED(listensock);
  return -1;
#else

  int                i;
  int                event_count;
  sigset_t           origsig;
  struct epoll_event ev;
  struct tmqtt      *context;

  memset(&ev, 0, sizeof(struct epoll_event));
  sigprocmask(SIG_SETMASK, &my_sigblock, &origsig);
  event_count = epoll_wait(db.epollfd, ep_events, MAX_EVENTS, 100);
  sigprocmask(SIG_SETMASK, &origsig, NULL);

  db.now_s = tmqtt_time();
  db.now_real_s = time(NULL);

  switch (event_count) {
    case -1:
      if (errno != EINTR) {
        ttq_log(NULL, TTQ_LOG_ERR, "Error in epoll waiting: %s.", strerror(errno));
      }
      break;
    case 0:
      break;
    default:
      for (i = 0; i < event_count; i++) {
        context = ep_events[i].data.ptr;
        if (context->ident == id_client) {
          ttq_handle_rw(context, ep_events[i].events);
        } else if (context->ident == id_listener) {
          listensock = ep_events[i].data.ptr;

          if (ep_events[i].events & (EPOLLIN | EPOLLPRI)) {
            while ((context = ttqNetSocketAccept(listensock)) != NULL) {
              context->events = EPOLLIN;
              ttq_mux_add_in(context);
            }
          }
        }
      }
  }

  return TTQ_ERR_SUCCESS;

#endif
}
