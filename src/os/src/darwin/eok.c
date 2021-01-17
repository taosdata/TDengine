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

#include "eok.h"

#include <errno.h>
#include <libgen.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef ENABLE_LOG
#define D(fmt, ...) fprintf(stderr, "%s[%d]%s(): " fmt "\n", basename(__FILE__), __LINE__, __func__, ##__VA_ARGS__)
#define E(fmt, ...) do {                                                \
  fprintf(stderr, "%s[%d]%s(): %d[%s]: " fmt "\n",                      \
          basename(__FILE__), __LINE__, __func__,                       \
          errno, strerror(errno),                                       \
          ##__VA_ARGS__);                                               \
} while (0)
#else // !ENABLE_LOG
#define D(fmt, ...) (void)fmt
#define E(fmt, ...) (void)fmt
#endif // ENABLE_LOG

#define A(statement, fmt, ...) do {                                     \
  if (statement) break;                                                 \
  fprintf(stderr, "%s[%d]%s(): assert [%s] failed: %d[%s]: " fmt "\n",  \
          basename(__FILE__), __LINE__, __func__,                       \
          #statement, errno, strerror(errno),                           \
          ##__VA_ARGS__);                                               \
  abort();                                                              \
} while (0)

static int eok_dummy = 0;

typedef struct ep_over_kq_s               ep_over_kq_t;
typedef struct eok_event_s                eok_event_t;

struct ep_over_kq_s {
  int                    kq;

  // !!!
  // idx in the eoks list
  // used as pseudo-file-desciptor
  // must be 'closed' with epoll_close
  int                    idx;

  ep_over_kq_t          *next;

  int                    sv[2]; // 0 for read, 1 for write

  // all registered 'epoll events, key by fd'
  int                   evs_count;
  eok_event_t           *evs_head;
  eok_event_t           *evs_tail;
  eok_event_t           *evs_free;

  // all kev changes list pending to be processed by kevent64
  // key by tuple (ident,filter), ident === fd in this case
  struct kevent64_s     *kchanges;
  int                    nchanges;
  int                    ichanges;

  // kev eventslist for kevent64 to store active events
  // they remain alive among kevent64 calls
  struct kevent64_s     *kevslist;
  int                    nevslist;

  pthread_mutex_t        lock;

  volatile unsigned int  lock_valid:1;
  volatile unsigned int  waiting:1;
  volatile unsigned int  changed:1;
  volatile unsigned int  wakenup:1;
  volatile unsigned int  stopping:1;
};

struct eok_event_s {
  int                        fd;
  struct epoll_event         epev;
  volatile unsigned int      changed; // 0:registered;1:add;2:mod;3:del

  eok_event_t               *next;
  eok_event_t               *prev;
};

typedef struct eoks_s                      eoks_t;
struct eoks_s {
  pthread_mutex_t      lock;
  ep_over_kq_t        *eoks;
  int                  neoks;
  ep_over_kq_t        *eoks_free;
};

static eoks_t          eoks = {
  .lock           = PTHREAD_MUTEX_INITIALIZER,
  .eoks           = NULL,
  .neoks          = 0,
  .eoks_free      = NULL,
};

#ifdef ENABLE_LOG
static const char* op_str(int op) {
  switch (op) {
    case EPOLL_CTL_ADD: return "EPOLL_CTL_ADD";
    case EPOLL_CTL_MOD: return "EPOLL_CTL_MOD";
    case EPOLL_CTL_DEL: return "EPOLL_CTL_DEL";
    default: return "UNKNOWN";
  }
}

static __thread char buf_slots[10][1024] = {0};
static __thread int  buf_slots_linelen   = sizeof(buf_slots[0])/sizeof(buf_slots[0][0]);
static __thread int  buf_slots_count     = sizeof(buf_slots)/(sizeof(buf_slots[0])/sizeof(buf_slots[0][0]));

static const char* events_str(uint32_t events, int slots) {
  A(slots>=0 && slots<buf_slots_count, "internal logic error");
  char     *buf = buf_slots[slots];
  char     *p   = buf;
  size_t    len = buf_slots_linelen;
  int       n   = 0;
  buf[0] = '\0';
  // copied from <sys/epoll.h> on linux
  //     EPOLLIN = 0x001,
  // #define EPOLLIN EPOLLIN
  //     EPOLLPRI = 0x002,
  // #define EPOLLPRI EPOLLPRI
  //     EPOLLOUT = 0x004,
  // #define EPOLLOUT EPOLLOUT
  //     EPOLLRDNORM = 0x040,
  // #define EPOLLRDNORM EPOLLRDNORM
  //     EPOLLRDBAND = 0x080,
  // #define EPOLLRDBAND EPOLLRDBAND
  //     EPOLLWRNORM = 0x100,
  // #define EPOLLWRNORM EPOLLWRNORM
  //     EPOLLWRBAND = 0x200,
  // #define EPOLLWRBAND EPOLLWRBAND
  //     EPOLLMSG = 0x400,
  // #define EPOLLMSG EPOLLMSG
  //     EPOLLERR = 0x008,
  // #define EPOLLERR EPOLLERR
  //     EPOLLHUP = 0x010,
  // #define EPOLLHUP EPOLLHUP
  //     EPOLLRDHUP = 0x2000,
  // #define EPOLLRDHUP EPOLLRDHUP
  //     EPOLLEXCLUSIVE = 1u << 28,
  // #define EPOLLEXCLUSIVE EPOLLEXCLUSIVE
  //     EPOLLWAKEUP = 1u << 29,
  // #define EPOLLWAKEUP EPOLLWAKEUP
  //     EPOLLONESHOT = 1u << 30,
  // #define EPOLLONESHOT EPOLLONESHOT
  //     EPOLLET = 1u << 31
  // #define EPOLLET EPOLLET
#define CHK_EV(ev)                                              \
  if (len>0 && (events & (ev))==(ev)) {                         \
    n   = snprintf(p, len, "%s%s", p!=buf ? "|" : "", #ev);     \
    p   += n;                                                   \
    len -= n;                                                   \
  }
  CHK_EV(EPOLLIN);
  CHK_EV(EPOLLPRI);
  CHK_EV(EPOLLOUT);
  CHK_EV(EPOLLRDNORM);
  CHK_EV(EPOLLRDBAND);
  CHK_EV(EPOLLWRNORM);
  CHK_EV(EPOLLWRBAND);
  CHK_EV(EPOLLMSG);
  CHK_EV(EPOLLERR);
  CHK_EV(EPOLLHUP);
  CHK_EV(EPOLLRDHUP);
  CHK_EV(EPOLLEXCLUSIVE);
  CHK_EV(EPOLLWAKEUP);
  CHK_EV(EPOLLONESHOT);
  CHK_EV(EPOLLET);
#undef CHK_EV
  return buf;
}

static const char* kev_flags_str(uint16_t flags, int slots) {
  A(slots>=0 && slots<buf_slots_count, "internal logic error");
  char     *buf = buf_slots[slots];
  char     *p   = buf;
  size_t    len = buf_slots_linelen;
  int       n   = 0;
  buf[0] = '\0';
  // copied to <sys/event.h>
  // #define EV_ADD              0x0001      /* add event to kq (implies enable) */
  // #define EV_DELETE           0x0002      /* delete event from kq */
  // #define EV_ENABLE           0x0004      /* enable event */
  // #define EV_DISABLE          0x0008      /* disable event (not reported) */
  //     /* flags */
  // #define EV_ONESHOT          0x0010      /* only report one occurrence */
  // #define EV_CLEAR            0x0020      /* clear event state after reporting */
  // #define EV_RECEIPT          0x0040      /* force immediate event output */
  //                                             /* ... with or without EV_ERROR */
  //                                             /* ... use KEVENT_FLAG_ERROR_EVENTS */
  //                                             /*     on syscalls supporting flags */
  // #define EV_DISPATCH         0x0080      /* disable event after reporting */
  // #define EV_UDATA_SPECIFIC   0x0100      /* unique kevent per udata value */
  // #define EV_DISPATCH2        (EV_DISPATCH | EV_UDATA_SPECIFIC)
  //     /* ... in combination with EV_DELETE */
  //     /* will defer delete until udata-specific */
  //     /* event enabled. EINPROGRESS will be */
  //     /* returned to indicate the deferral */
  // #define EV_VANISHED         0x0200      /* report that source has vanished  */
  //                                             /* ... only valid with EV_DISPATCH2 */
  // #define EV_SYSFLAGS         0xF000      /* reserved by system */
  // #define EV_FLAG0            0x1000      /* filter-specific flag */
  // #define EV_FLAG1            0x2000      /* filter-specific flag */
  //     /* returned values */
  // #define EV_EOF              0x8000      /* EOF detected */
  // #define EV_ERROR            0x4000      /* error, data contains errno */
#define CHK_EV(ev)                                              \
  if (len>0 && (flags & (ev))==(ev)) {                          \
    n   = snprintf(p, len, "%s%s", p!=buf ? "|" : "", #ev);     \
    p   += n;                                                   \
    len -= n;                                                   \
  }
  CHK_EV(EV_ADD);
  CHK_EV(EV_DELETE);
  CHK_EV(EV_ENABLE);
  CHK_EV(EV_DISABLE);
  CHK_EV(EV_ONESHOT);
  CHK_EV(EV_CLEAR);
  CHK_EV(EV_RECEIPT);
  CHK_EV(EV_DISPATCH);
  CHK_EV(EV_UDATA_SPECIFIC);
  CHK_EV(EV_DISPATCH2);
  CHK_EV(EV_VANISHED);
  CHK_EV(EV_SYSFLAGS);
  CHK_EV(EV_FLAG0);
  CHK_EV(EV_FLAG1);
  CHK_EV(EV_EOF);
  CHK_EV(EV_ERROR);
#undef CHK_EV
  return buf;
}
#endif // ENABLE_LOG

static ep_over_kq_t* eoks_alloc(void);
static void          eoks_free(ep_over_kq_t *eok);
static ep_over_kq_t* eoks_find(int epfd);

static eok_event_t* eok_find_ev(ep_over_kq_t *eok, int fd);
static eok_event_t* eok_calloc_ev(ep_over_kq_t *eok);
static void         eok_free_ev(ep_over_kq_t *eok, eok_event_t *ev);
static void         eok_wakeup(ep_over_kq_t *eok);

static int eok_chgs_refresh(ep_over_kq_t *eok, eok_event_t *oev, eok_event_t *ev, struct kevent64_s *krev, struct kevent64_s *kwev);

static struct kevent64_s* eok_alloc_eventslist(ep_over_kq_t *eok, int maxevents);

int epoll_create(int size) {
  (void)size;
  int e = 0;
  ep_over_kq_t *eok = eoks_alloc();
  if (!eok) return -1;

  A(eok->kq==-1, "internal logic error");
  A(eok->lock_valid, "internal logic error");
  A(eok->idx>=0 && eok->idx<eoks.neoks, "internal logic error");
  A(eok->next==NULL, "internal logic error");
  A(eok->sv[0]==-1, "internal logic error");
  A(eok->sv[1]==-1, "internal logic error");

  eok->kq = kqueue();
  if (eok->kq==-1) {
    e = errno;
    eoks_free(eok);
    errno = e;
    return -1;
  }

  if (socketpair(AF_LOCAL, SOCK_STREAM, 0, eok->sv)) {
    e = errno;
    eoks_free(eok);
    errno = e;
    return -1;
  }

  struct epoll_event ev = {0};
  ev.events = EPOLLIN;
  ev.data.ptr = &eok_dummy;
  D("epoll_create epfd:[%d]", eok->idx);
  if (epoll_ctl(eok->idx, EPOLL_CTL_ADD, eok->sv[0], &ev)) {
    e = errno;
    epoll_close(eok->idx);
    errno = e;
    return -1;
  }

  return eok->idx;
}

int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event) {
  D("epoll_ctling epfd:[%d], op:[%s], fd:[%d], events:[%04x:%s]",
     epfd, op_str(op), fd,
     event ? event->events : 0,
     event ? events_str(event->events, 0) : "NULL");
  int e = 0;
  if (epfd<0 || epfd>=eoks.neoks) {
    errno = EBADF;
    return -1;
  }
  if (fd==-1) {
    errno = EBADF;
    return -1;
  }
  if (event && !(event->events & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP | EPOLLOUT))) {
    e = ENOTSUP;
    return -1;
  }

  ep_over_kq_t *eok = eoks_find(epfd);
  if (!eok) {
    errno = EBADF;
    return -1;
  }

  A(0==pthread_mutex_lock(&eok->lock), "");
  do {
    eok_event_t* oev = eok_find_ev(eok, fd);
    if (op==EPOLL_CTL_ADD && oev) {
      e = EEXIST;
      break;
    }
    if (op!=EPOLL_CTL_ADD && !oev) {
      e = ENOENT;
      break;
    }
    if (op!=EPOLL_CTL_DEL && !event) {
      e = EINVAL;
      break;
    }

    // prepare krev/kwev
    struct kevent64_s krev = {0};
    struct kevent64_s kwev = {0};
    krev.ident = -1;
    kwev.ident = -1;
    uint16_t flags = 0;
    // prepare internal eok event
    eok_event_t ev = {0};
    ev.fd = fd;
    if (event) ev.epev = *event;
    struct epoll_event *pev = event;
    switch (op) {
      case EPOLL_CTL_ADD: {
        flags = EV_ADD;
        ev.changed = 1;
      } break;
      case EPOLL_CTL_MOD: {
        flags = EV_ADD;
        ev.changed = 2;
      } break;
      case EPOLL_CTL_DEL: {
        // event is ignored
        // pev points to registered epoll_event
        pev = &oev->epev;
        flags = EV_DELETE;
        ev.changed = 3;
      } break;
      default: {
        e = ENOTSUP;
      } break;
    }

    if (e) break;

    // udata will be delayed to be set
    if (pev->events & (EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
      flags |= EV_EOF;
      EV_SET64(&krev, ev.fd, EVFILT_READ, flags, 0, 0, -1, 0, 0);
    }
    if (pev->events & EPOLLOUT) {
      EV_SET64(&kwev, ev.fd, EVFILT_WRITE, flags, 0, 0, -1, 0, 0);
    }

    // refresh registered evlist and changelist in a transaction way
    if (eok_chgs_refresh(eok, oev, &ev, &krev, &kwev)) {
      e = errno;
      A(e, "internal logic error");
      break;
    }
    eok->changed = 1;
    eok_wakeup(eok);
  } while (0);
  A(0==pthread_mutex_unlock(&eok->lock), "");

  if (e) {
    errno = e;
    return -1;
  }

  return 0;
}

static struct timespec do_timespec_diff(struct timespec *from, struct timespec *to);

int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout) {
  int e = 0;
  if (!events) {
    errno = EINVAL;
    E("epoll_waiting epfd:[%d], maxevents:[%d], timeout:[%d] failed", epfd, maxevents, timeout);
    return -1;
  }
  if (maxevents<=0) {
    errno = EINVAL;
    E("epoll_waiting epfd:[%d], maxevents:[%d], timeout:[%d] failed", epfd, maxevents, timeout);
    return -1;
  }

  struct timespec abstime = {0};
  A(TIME_UTC==timespec_get(&abstime, TIME_UTC), "internal logic error");

  if (timeout!=-1) {
    if (timeout<0) timeout = 0;
    int64_t t = abstime.tv_nsec + timeout * 1000000;
    abstime.tv_sec += t / 1000000000;
    abstime.tv_nsec = t % 1000000000;
  }

  int r = 0;

  ep_over_kq_t *eok = eoks_find(epfd);
  if (!eok) {
    errno = EBADF;
    E("epoll_waiting epfd:[%d], maxevents:[%d], timeout:[%d] failed", epfd, maxevents, timeout);
    errno = EBADF;
    return -1;
  }

  int cnts = 0;
  A(0==pthread_mutex_lock(&eok->lock), "");
  do {
    cnts = 0;
    A(eok->waiting==0, "internal logic error");
    struct kevent64_s *eventslist = eok_alloc_eventslist(eok, maxevents);
    if (!eventslist) {
      e = ENOMEM;
      E("epoll_waiting epfd:[%d], maxevents:[%d], timeout:[%d] failed", epfd, maxevents, timeout);
      break;
    }
    memset(eventslist, 0, maxevents * sizeof(*eventslist));

    struct timespec now = {0};
    A(TIME_UTC==timespec_get(&now, TIME_UTC), "internal logic error");
    struct timespec to = do_timespec_diff(&now, &abstime);
    struct timespec *pto = &to;
    if (timeout==-1) {
      pto = NULL;
    }

    // taking the changelist
    struct kevent64_s *kchanges = eok->kchanges;
    int                nchanges = eok->nchanges;
    int                ichanges = eok->ichanges;
    // let outside world to add changes
    eok->kchanges = NULL;
    eok->nchanges = 0;
    eok->ichanges = 0;

    eok->changed = 0;
    eok->wakenup = 0;
    eok->waiting = 1;

    A(0==pthread_mutex_unlock(&eok->lock), "");
    if (ichanges>0) {
      D("kevent64 epfd[%d] changing [%d] changes and waiting...", eok->idx, ichanges);
    }
    errno = 0;
    r = kevent64(eok->kq, kchanges, ichanges, eventslist, maxevents, 0, pto);
    e = errno;
    if (e) {
      E("kevent64 epfd[%d] waiting done, with r[%d]", eok->idx, r);
    }
    A(0==pthread_mutex_lock(&eok->lock), "");

    eok->waiting = 0;

    if (kchanges) {
      if (eok->kchanges==NULL) {
        // reuse
        A(eok->nchanges==0 && eok->ichanges==0, "internal logic error");
        eok->kchanges = kchanges;
        eok->nchanges = nchanges;
      } else {
        free(kchanges);
        kchanges = NULL;
      }
      nchanges = 0;
      ichanges = 0;
    }

    if (r==0) {
      A(timeout!=-1, "internal logic error");
    }
    for (int i=0; i<r; ++i) {
      struct kevent64_s *kev = eventslist + i;
      A(kev->udata && eok->evs_head && eok->evs_tail, "internal logic error");
      eok_event_t *ev = (eok_event_t*)kev->udata;
      A(kev->ident == ev->fd, "internal logic error");
      if (kev->flags & EV_ERROR) {
        D("error when processing change list for fd[%d], error[%s], kev_flags:[%04x:%s]",
           ev->fd, strerror(kev->data), kev->flags, kev_flags_str(kev->flags, 0));
      }
      switch (kev->filter) {
        case EVFILT_READ: {
          A((ev->epev.events & EPOLLIN), "internal logic errro");
          if (ev->epev.data.ptr==&eok_dummy) {
            // it's coming from wakeup socket pair
            char c = '\0';
            A(1==recv(kev->ident, &c, 1, 0), "internal logic error");
            A(0==memcmp(&c, "1", 1), "internal logic error");
            D("wokenup");
            continue;
          } else {
            if (ev->changed==3) {
              D("already requested to delete for fd[%d]", ev->fd);
              // TODO: write a unit test for this case
              // EV_DELETE?
              continue;
            }
            // converting to epoll_event
            // we shall collect all kevents for the uniq fd into one epoll_evnt
            // but currently, taos never use EPOLLOUT
            // just let it this way for the moment
            struct epoll_event pev = {0};
            pev.data.ptr = ev->epev.data.ptr;
            pev.events = EPOLLIN;
            if (kev->flags & EV_EOF) {
              // take all these as EOF for the moment
              pev.events |= (EPOLLHUP | EPOLLERR | EPOLLRDHUP);
            }
            // rounded to what user care
            pev.events = pev.events & ev->epev.events;
            D("events found for fd[%d]: [%04x:%s], which was registered: [%04x:%s], kev_flags: [%04x:%s]",
               ev->fd, pev.events, events_str(pev.events, 0),
               ev->epev.events, events_str(ev->epev.events, 1),
               kev->flags, kev_flags_str(kev->flags, 2));
            // now we get ev and store it
            events[cnts++] = pev;
          }
        } break;
        case EVFILT_WRITE: {
          A(0, "not implemented yet");
        } break;
        default: {
          A(0, "internal logic error");
        } break;
      }
    }
    if (r>=0) {
      // we can safely rule out delete-requested-events from the regitered evlists
      // if only changelist are correctly registered
      eok_event_t *p = eok->evs_head;
      while (p) {
        eok_event_t *next = p->next;
        if (p->changed==3) {
          D("removing registered event for fd[%d]: [%04x:%s]", p->fd, p->epev.events, events_str(p->epev.events, 0));
          eok_free_ev(eok, p);
        }
        p = next;
      }
    }
    if (cnts==0) {
      // if no user-cared-events is up
      // we check to see if time is up
      if (timeout!=-1) {
        A(TIME_UTC==timespec_get(&now, TIME_UTC), "internal logic error");
        to = do_timespec_diff(&now, &abstime);
        if (to.tv_sec==0 && to.tv_nsec==0) break;
      }
      // time is not up yet, continue loop
    }
  } while (cnts==0);
  if (cnts>0) {
    D("kevent64 waiting done with [%d] events", cnts);
  }
  A(0==pthread_mutex_unlock(&eok->lock), "");

  if (e) {
    errno = e;
    E("epoll_wait failed");
    return -1;
  }

  // tell user how many events are valid
  return cnts;
}

static struct timespec do_timespec_diff(struct timespec *from, struct timespec *to) {
  struct timespec delta;
  delta.tv_sec  = to->tv_sec - from->tv_sec;
  delta.tv_nsec = to->tv_nsec - from->tv_nsec;
  // norm and round up
  while (delta.tv_nsec<0) {
    delta.tv_sec -= 1;
    delta.tv_nsec += 1000000000;
  }
  if (delta.tv_sec < 0) {
    delta.tv_sec = 0;
    delta.tv_nsec = 0;
  }
  return delta;
}

int epoll_close(int epfd) {
  D("epoll_closing epfd: [%d]", epfd);
  ep_over_kq_t *eok = eoks_find(epfd);
  if (!eok) {
    errno = EBADF;
    return -1;
  }

  A(0==pthread_mutex_lock(&eok->lock), "");
  do {
    // panic if it would be double-closed
    A(eok->stopping==0, "internal logic error");
    eok->stopping = 1;
    // panic if epoll_wait is pending
    A(eok->waiting==0, "internal logic error");

    if (eok->kq!=-1) {
      close(eok->kq);
      eok->kq = -1;
    }
  } while (0);
  A(0==pthread_mutex_unlock(&eok->lock), "");
  eoks_free(eok);

  return 0;
}

static struct kevent64_s* eok_alloc_eventslist(ep_over_kq_t *eok, int maxevents) {
  if (maxevents<=eok->nevslist) return eok->kevslist;
  struct kevent64_s *p = (struct kevent64_s*)realloc(eok->kevslist, sizeof(*p)*maxevents);
  if (!p) return NULL;
  eok->kevslist = p;
  eok->nevslist = maxevents;
  return p;
}

static eok_event_t* eok_find_ev(ep_over_kq_t *eok, int fd) {
  eok_event_t *p = eok->evs_head;
  while (p) {
    if (p->fd == fd) return p;
    p = p->next;
  }
  errno = ENOENT;
  return NULL;
}

static eok_event_t* eok_calloc_ev(ep_over_kq_t *eok) {
  eok_event_t *p = NULL;
  if (eok->evs_free) {
    p = eok->evs_free;
    eok->evs_free = p->next;
    p->next = NULL;
    A(p->prev==NULL, "internal logic error");
  } else {
    p = (eok_event_t*)calloc(1, sizeof(*p));
    if (!p) return NULL;
    A(p->next==NULL, "internal logic error");
    A(p->prev==NULL, "internal logic error");
  }
  // dirty link
  p->prev = eok->evs_tail;
  if (eok->evs_tail) eok->evs_tail->next = p;
  else               eok->evs_head = p;
  eok->evs_tail = p;

  eok->evs_count += 1;

  return p;
}

static void eok_free_ev(ep_over_kq_t *eok, eok_event_t *ev) {
  if (ev->prev) ev->prev->next = ev->next;
  else          eok->evs_head  = ev->next;
  if (ev->next) ev->next->prev = ev->prev;
  else          eok->evs_tail  = ev->prev;
  ev->prev = NULL;
  ev->next = eok->evs_free;
  eok->evs_free = ev->next;

  eok->evs_count -= 1;
}

static void eok_wakeup(ep_over_kq_t *eok) {
  if (!eok->waiting) return;
  if (eok->wakenup) return;
  eok->wakenup = 1;
  A(1==send(eok->sv[1], "1", 1, 0), "");
}

static int eok_chgs_refresh(ep_over_kq_t *eok, eok_event_t *oev, eok_event_t *ev, struct kevent64_s *krev, struct kevent64_s *kwev) {
  if (!oev) oev = eok_calloc_ev(eok);
  if (!oev) return -1;
  int n = 0;
  if (krev->ident==ev->fd) ++n;
  if (kwev->ident==ev->fd) ++n;
  A(n, "internal logic error");
  if (eok->ichanges+n>eok->nchanges) {
    struct kevent64_s *p = (struct kevent64_s*)realloc(eok->kchanges, sizeof(*p) * (eok->nchanges + 10));
    if (!p) {
      if (ev->changed==1) {
        // roll back
        A(oev, "internal logic error");
        eok_free_ev(eok, oev);
      }
      errno = ENOMEM;
      return -1;
    }
    eok->kchanges  = p;
    eok->nchanges += 10;
  }

  // copy to registered event slot
  oev->fd      = ev->fd;
  if (ev->changed!=3) {
    // if add/mod, copy epoll_event
    oev->epev    = ev->epev;
  }
  oev->changed = ev->changed;

  // copy to changes list
  n = 0;
  if (krev->ident==ev->fd) {
    krev->udata = (uint64_t)oev;
    eok->kchanges[eok->ichanges++] = *krev;
    ++n;
  }
  if (kwev->ident==ev->fd) {
    kwev->udata = (uint64_t)oev;
    eok->kchanges[eok->ichanges++] = *kwev;
    ++n;
  }
  D("add #changes[%d] for fd[%d], and now #changes/registers [%d/%d]", n, ev->fd, eok->ichanges, eok->evs_count);
  return 0;
}

static ep_over_kq_t* eoks_alloc(void) {
  ep_over_kq_t *eok = NULL;

  A(0==pthread_mutex_lock(&eoks.lock), "");
  do {
    if (eoks.eoks_free) {
      eok = eoks.eoks_free;
      eoks.eoks_free = eok->next;
      eok->next = NULL;
      break;
    }
    ep_over_kq_t *p = (ep_over_kq_t*)realloc(eoks.eoks, sizeof(*p) * (eoks.neoks+1));
    if (!p) break;
    eoks.eoks = p;
    eok = eoks.eoks + eoks.neoks;
    memset(eok, 0, sizeof(*eok));
    eok->idx = eoks.neoks;
    eok->kq  = -1;
    eok->sv[0] = -1;
    eok->sv[1] = -1;
    eoks.neoks += 1;
  } while (0);
  A(0==pthread_mutex_unlock(&eoks.lock), "");

  if (!eok) {
    errno = ENOMEM;
    return NULL;
  }
  if (eok->lock_valid) {
    return eok;
  }

  if (0==pthread_mutex_init(&eok->lock, NULL)) {
    eok->lock_valid = 1;
    return eok;
  }

  eoks_free(eok);
  errno = ENOMEM;
  return NULL;
}

static void eoks_free(ep_over_kq_t *eok) {
  A(0==pthread_mutex_lock(&eoks.lock), "");
  do {
    A(eok->next==NULL, "internal logic error");

    // leave eok->kchanges as is
    A(eok->ichanges==0, "internal logic error");

    A(eok->waiting==0, "internal logic error");
    if (eok->evs_count==1) {
      A(eok->evs_head && eok->evs_tail && eok->evs_head==eok->evs_tail, "internal logic error");
      A(eok->evs_head->fd==eok->sv[0] && eok->sv[0]!=-1 && eok->sv[1]!=-1, "internal logic error");
      // fd is critical system resource
      close(eok->sv[0]);
      eok->sv[0] = -1;
      close(eok->sv[1]);
      eok->sv[1] = -1;
      eok_free_ev(eok, eok->evs_head);
    }
    A(eok->evs_head==NULL && eok->evs_tail==NULL && eok->evs_count==0, "internal logic error");
    A(eok->sv[0]==-1 && eok->sv[1]==-1, "internal logic error");
    if (eok->kq!=-1) {
      close(eok->kq);
      eok->kq = -1;
    }
    eok->next = eoks.eoks_free;
    eoks.eoks_free = eok;
  } while (0);
  A(0==pthread_mutex_unlock(&eoks.lock), "");
}

static ep_over_kq_t* eoks_find(int epfd) {
  ep_over_kq_t *eok = NULL;
  A(0==pthread_mutex_lock(&eoks.lock), "");
  do {
    if (epfd<0 || epfd>=eoks.neoks) {
      break;
    }
    A(eoks.eoks, "internal logic error");
    eok = eoks.eoks + epfd;
    A(eok->next==NULL, "internal logic error");
    A(eok->lock_valid, "internal logic error");
  } while (0);
  A(0==pthread_mutex_unlock(&eoks.lock), "");
  return eok;
}

