#define _GNU_SOURCE

#ifdef WITH_EPOLL

#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#else /* select-based multiplexing (e.g. Windows) */

#ifdef WIN32
#ifndef FD_SETSIZE
#define FD_SETSIZE 1024
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

/* epoll event flags are only used as interest/result bitmasks by this file. */
#ifndef EPOLLIN
#define EPOLLIN  0x001
#endif
#ifndef EPOLLPRI
#define EPOLLPRI 0x002
#endif
#ifndef EPOLLOUT
#define EPOLLOUT 0x004
#endif
#ifndef EPOLLERR
#define EPOLLERR 0x008
#endif
#ifndef EPOLLHUP
#define EPOLLHUP 0x010
#endif

#endif /* WITH_EPOLL */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "ttqMemory.h"
#include "ttqNet.h"
#include "ttqPacket.h"
#include "ttqSend.h"
#include "ttqSystree.h"
#include "ttqTime.h"
#include "ttqUtil.h"

#ifdef WITH_EPOLL

#define MAX_EVENTS 1000

static sigset_t           my_sigblock;
static struct epoll_event ep_events[MAX_EVENTS];

#endif

/*
 * Handle read/write readiness for a single client connection. Shared by both the
 * epoll and the select based multiplexers, so that the connection state machine
 * stays in one place.
 */
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

int ttqMuxInit(struct tmqtt__listener_sock *listensock, int listensock_count) {
#ifndef WITH_EPOLL
  UNUSED(listensock);
  UNUSED(listensock_count);
  return TTQ_ERR_SUCCESS;
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
  return TTQ_ERR_SUCCESS;
#else

  (void)close(db.epollfd);
  db.epollfd = 0;
  return TTQ_ERR_SUCCESS;

#endif
}

int ttqMuxAddOut(struct tmqtt *context) {
#ifndef WITH_EPOLL

  context->events = EPOLLIN | EPOLLOUT;
  return TTQ_ERR_SUCCESS;

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

  context->events = EPOLLIN;
  return TTQ_ERR_SUCCESS;

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

  /*
   * The select based multiplexer rebuilds its fd sets from db.contexts_by_sock on
   * every iteration, so there is nothing to unregister here. The context is removed
   * from that hash by the networking layer when the socket is closed.
   */
  UNUSED(context);
  return 0;

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

#endif

int ttqMuxHandle(struct tmqtt__listener_sock *listensock, int listensock_count) {
#ifndef WITH_EPOLL

  int            i;
  int            event_count;
  fd_set         readfds;
  fd_set         writefds;
  fd_set         exceptfds;
  struct timeval tv;
  ttq_sock_t     maxfd = 0;
  struct tmqtt  *context;
  struct tmqtt  *ctxt_tmp;

  FD_ZERO(&readfds);
  FD_ZERO(&writefds);
  FD_ZERO(&exceptfds);

#ifdef WIN32
  int fd_count = 0;
  static bool fd_limit_warned = false;
#endif

  for (i = 0; i < listensock_count; i++) {
    if (listensock[i].sock == INVALID_SOCKET) {
      continue;
    }
#ifdef WIN32
    if (fd_count >= FD_SETSIZE) {
      if (!fd_limit_warned) {
        ttq_log(NULL, TTQ_LOG_WARNING,
                "Warning: FD_SETSIZE (%d) exceeded, some sockets dropped from select. "
                "Max concurrent connections on Windows is limited to %d.",
                FD_SETSIZE, FD_SETSIZE);
        fd_limit_warned = true;
      }
      continue;
    }
    fd_count++;
#endif
    FD_SET(listensock[i].sock, &readfds);
    if (listensock[i].sock > maxfd) {
      maxfd = listensock[i].sock;
    }
  }

  HASH_ITER(hh_sock, db.contexts_by_sock, context, ctxt_tmp) {
    if (context->sock == INVALID_SOCKET) {
      continue;
    }
#ifdef WIN32
    if (fd_count >= FD_SETSIZE) {
      continue;
    }
    fd_count++;
#endif
    FD_SET(context->sock, &readfds);
    if (context->events & EPOLLOUT) {
      FD_SET(context->sock, &writefds);
    }
    FD_SET(context->sock, &exceptfds);
    if (context->sock > maxfd) {
      maxfd = context->sock;
    }
  }

  tv.tv_sec = 0;
  tv.tv_usec = 100 * 1000;
  event_count = select((int)(maxfd + 1), &readfds, &writefds, &exceptfds, &tv);

  db.now_s = tmqtt_time();
  db.now_real_s = time(NULL);

  if (event_count == -1) {
#ifdef WIN32
    errno = WSAGetLastError();
#endif
    if (errno != COMPAT_EINTR) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error in select waiting: %s.", strerror(errno));
    }
    return TTQ_ERR_SUCCESS;
  }

  if (event_count == 0) {
    return TTQ_ERR_SUCCESS;
  }

  HASH_ITER(hh_sock, db.contexts_by_sock, context, ctxt_tmp) {
    uint32_t events = 0;

    if (context->sock == INVALID_SOCKET) {
      continue;
    }
    if (FD_ISSET(context->sock, &readfds)) {
      events |= EPOLLIN;
    }
    if (FD_ISSET(context->sock, &writefds)) {
      events |= EPOLLOUT;
    }
    if (FD_ISSET(context->sock, &exceptfds)) {
      events |= EPOLLERR;
    }
    if (events) {
      ttq_handle_rw(context, events);
    }
  }

  for (i = 0; i < listensock_count; i++) {
    if (listensock[i].sock == INVALID_SOCKET) {
      continue;
    }
    if (FD_ISSET(listensock[i].sock, &readfds)) {
      while ((context = ttqNetSocketAccept(&listensock[i])) != NULL) {
        context->events = EPOLLIN;
      }
    }
  }

  return TTQ_ERR_SUCCESS;

#else

  UNUSED(listensock_count);

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
