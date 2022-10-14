#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "task.h"

#define NUM_OF_THREAD 1
#define TIMEOUT       10000

typedef struct SThreadObj {
  TdThread    thread;
  uv_pipe_t  *pipe;
  uv_loop_t  *loop;
  uv_async_t *workerAsync;  //
  int         fd;
} SThreadObj;

typedef struct SServerObj {
  uv_tcp_t     server;
  uv_loop_t   *loop;
  int          workerIdx;
  int          numOfThread;
  SThreadObj **pThreadObj;
  uv_pipe_t  **pipe;
} SServerObj;

typedef struct SConnCtx {
  uv_tcp_t   *pClient;
  uv_timer_t *pTimer;
  uv_async_t *pWorkerAsync;
  int         ref;
} SConnCtx;

void echo_write(uv_write_t *req, int status) {
  if (status < 0) {
    fprintf(stderr, "Write error %s\n", uv_err_name(status));
  }
  printf("write data to client\n");
  taosMemoryFree(req);
}

void echo_read(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  SConnCtx *pConn = container_of(client, SConnCtx, pClient);
  pConn->ref += 1;
  printf("read data %d\n", nread, buf->base, buf->len);
  if (nread > 0) {
    uv_write_t *req = (uv_write_t *)taosMemoryMalloc(sizeof(uv_write_t));
    // dispatch request to database other process thread
    // just write out
    uv_buf_t write_out;
    write_out.base = buf->base;
    write_out.len = nread;
    uv_write((uv_write_t *)req, client, &write_out, 1, echo_write);
    taosMemoryFree(buf->base);
    return;
  }

  if (nread < 0) {
    if (nread != UV_EOF) fprintf(stderr, "Read error %s\n", uv_err_name(nread));
    uv_close((uv_handle_t *)client, NULL);
  }
  taosMemoryFree(buf->base);
}

void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  buf->base = taosMemoryMalloc(suggested_size);
  buf->len = suggested_size;
}

void on_new_connection(uv_stream_t *s, int status) {
  if (status == -1) {
    // error!
    return;
  }
  SServerObj *pObj = container_of(s, SServerObj, server);
  printf("new_connection from client\n");

  uv_tcp_t *client = (uv_tcp_t *)taosMemoryMalloc(sizeof(uv_tcp_t));
  uv_tcp_init(pObj->loop, client);
  if (uv_accept(s, (uv_stream_t *)client) == 0) {
    uv_write_t *write_req = (uv_write_t *)taosMemoryMalloc(sizeof(uv_write_t));
    uv_buf_t    dummy_buf = uv_buf_init("a", 1);
    // despatch to worker thread
    pObj->workerIdx = (pObj->workerIdx + 1) % pObj->numOfThread;
    uv_write2(write_req, (uv_stream_t *)&(pObj->pipe[pObj->workerIdx][0]), &dummy_buf, 1, (uv_stream_t *)client,
              echo_write);
  } else {
    uv_close((uv_handle_t *)client, NULL);
  }
}
void child_on_new_connection(uv_stream_t *q, ssize_t nread, const uv_buf_t *buf) {
  printf("x child_on_new_connection \n");
  if (nread < 0) {
    if (nread != UV_EOF) fprintf(stderr, "Read error %s\n", uv_err_name(nread));
    uv_close((uv_handle_t *)q, NULL);
    return;
  }
  SThreadObj *pObj = (SThreadObj *)container_of(q, struct SThreadObj, pipe);

  uv_pipe_t *pipe = (uv_pipe_t *)q;
  if (!uv_pipe_pending_count(pipe)) {
    fprintf(stderr, "No pending count\n");
    return;
  }

  uv_handle_type pending = uv_pipe_pending_type(pipe);
  assert(pending == UV_TCP);

  SConnCtx *pConn = taosMemoryMalloc(sizeof(SConnCtx));

  /* init conn timer*/
  pConn->pTimer = taosMemoryMalloc(sizeof(uv_timer_t));
  uv_timer_init(pObj->loop, pConn->pTimer);

  pConn->pClient = (uv_tcp_t *)taosMemoryMalloc(sizeof(uv_tcp_t));
  pConn->pWorkerAsync = pObj->workerAsync;  // thread safty
  uv_tcp_init(pObj->loop, pConn->pClient);

  if (uv_accept(q, (uv_stream_t *)(pConn->pClient)) == 0) {
    uv_os_fd_t fd;
    uv_fileno((const uv_handle_t *)pConn->pClient, &fd);
    fprintf(stderr, "Worker Accepted fd %d\n", fd);
    uv_timer_start(pConn->pTimer, timeOutCallBack, TIMEOUT, 0);
    uv_read_start((uv_stream_t *)(pConn->pClient), alloc_buffer, echo_read);
  } else {
    uv_timer_stop(pConn->pTimer);
    taosMemoryFree(pConn->pTimer);
    uv_close((uv_handle_t *)pConn->pClient, NULL);
    taosMemoryFree(pConn->pClient);
    taosMemoryFree(pConn);
  }
}

static void workerAsyncCallback(uv_async_t *handle) {
  SThreadObj *pObj = container_of(handle, SThreadObj, workerAsync);
  // do nothing
}
void *worker_thread(void *arg) {
  SThreadObj *pObj = (SThreadObj *)arg;
  int         fd = pObj->fd;
  pObj->loop = (uv_loop_t *)taosMemoryMalloc(sizeof(uv_loop_t));
  uv_loop_init(pObj->loop);

  uv_pipe_init(pObj->loop, pObj->pipe, 1);
  uv_pipe_open(pObj->pipe, fd);

  pObj->workerAsync = taosMemoryMalloc(sizeof(uv_async_t));
  uv_async_init(pObj->loop, pObj->workerAsync, workerAsyncCallback);
  uv_read_start((uv_stream_t *)pObj->pipe, alloc_buffer, child_on_new_connection);

  uv_run(pObj->loop, UV_RUN_DEFAULT);
}
int main() {
  SServerObj *server = taosMemoryCalloc(1, sizeof(SServerObj));
  server->loop = (uv_loop_t *)taosMemoryMalloc(sizeof(uv_loop_t));
  server->numOfThread = NUM_OF_THREAD;
  server->workerIdx = 0;
  server->pThreadObj = (SThreadObj **)taosMemoryCalloc(server->numOfThread, sizeof(SThreadObj *));
  server->pipe = (uv_pipe_t **)taosMemoryCalloc(server->numOfThread, sizeof(uv_pipe_t *));

  uv_loop_init(server->loop);

  for (int i = 0; i < server->numOfThread; i++) {
    server->pThreadObj[i] = (SThreadObj *)taosMemoryCalloc(1, sizeof(SThreadObj));
    server->pipe[i] = (uv_pipe_t *)taosMemoryCalloc(2, sizeof(uv_pipe_t));
    int fds[2];
    if (uv_socketpair(AF_UNIX, SOCK_STREAM, fds, UV_NONBLOCK_PIPE, UV_NONBLOCK_PIPE) != 0) {
      return -1;
    }
    uv_pipe_init(server->loop, &(server->pipe[i][0]), 1);
    uv_pipe_open(&(server->pipe[i][0]), fds[1]);  // init write

    server->pThreadObj[i]->fd = fds[0];
    server->pThreadObj[i]->pipe = &(server->pipe[i][1]);  // init read
    int err = taosThreadCreate(&(server->pThreadObj[i]->thread), NULL, worker_thread, (void *)(server->pThreadObj[i]));
    if (err == 0) {
      printf("thread %d create\n", i);
    } else {
      printf("thread %d create failed", i);
    }

    uv_tcp_init(server->loop, &server->server);
    struct sockaddr_in bind_addr;
    uv_ip4_addr("0.0.0.0", 7000, &bind_addr);
    uv_tcp_bind(&server->server, (const struct sockaddr *)&bind_addr, 0);
    int err = 0;
    if ((err = uv_listen((uv_stream_t *)&server->server, 128, on_new_connection)) != 0) {
      fprintf(stderr, "Listen error %s\n", uv_err_name(err));
      return 2;
    }
    uv_run(server->loop, UV_RUN_DEFAULT);
    return 0;
  }
}
