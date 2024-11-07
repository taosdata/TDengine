/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef USE_UV

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "transComm.h"
#include "transportInt.h"
#include "trpc.h"

using namespace std;

struct QueueElem {
  queue q;
  int   val;
};
class QueueObj {
 public:
  QueueObj() {
    // avoid formate
    QUEUE_INIT(&head);
  }
  void Push(QueueElem *el) {
    // avoid formate
    QUEUE_PUSH(&head, &el->q);
  }
  QueueElem *Pop() {
    QueueElem *el = NULL;
    if (!IsEmpty()) {
      queue *h = QUEUE_HEAD(&head);
      el = QUEUE_DATA(h, QueueElem, q);
      QUEUE_REMOVE(h);
    }
    return el;
  }
  bool IsEmpty() {
    // avoid formate
    return QUEUE_IS_EMPTY(&head);
  }
  void RmElem(QueueElem *el) {
    // impl
    QUEUE_REMOVE(&el->q);
  }
  void ForEach(std::vector<int> &result) {
    queue *h;
    QUEUE_FOREACH(h, &head) {
      // add more
      QueueElem *el = QUEUE_DATA(h, QueueElem, q);
      result.push_back(el->val);
    }
  }

 private:
  queue head;
};

class QueueEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // TODO
    q = new QueueObj();
  }
  virtual void TearDown() {
    delete q;
    // formate
  }
  QueueObj *q;
};

TEST_F(QueueEnv, testPushAndPop) {
  // add more test
  assert(q->IsEmpty());

  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
  }
  int i = 0;
  while (!q->IsEmpty()) {
    QueueElem *el = q->Pop();
    assert(el->val == i++);
    taosMemoryFree(el);
  }
  assert(q->IsEmpty());
}
TEST_F(QueueEnv, testRm) {
  // add more test

  std::vector<QueueElem *> set;
  assert(q->IsEmpty());

  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
    set.push_back(el);
  }
  for (int i = set.size() - 1; i >= 0; i--) {
    QueueElem *el = set[i];
    q->RmElem(el);
    taosMemoryFree(el);
  }
  assert(q->IsEmpty());
}
TEST_F(QueueEnv, testIter) {
  // add more test
  assert(q->IsEmpty());
  std::vector<int> vals;
  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
    vals.push_back(i);
  }
  std::vector<int> result;
  q->ForEach(result);
  assert(result.size() == vals.size());
}

class TransCtxEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    ctx = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
    transCtxInit(ctx);
    // TODO
  }
  virtual void TearDown() {
    transCtxCleanup(ctx);
    // formate
  }
  STransCtx *ctx;
};

int32_t cloneVal(void *src, void **dst) {
  int sz = (int)strlen((char *)src);
  *dst = taosMemoryCalloc(1, sz + 1);
  memcpy(*dst, src, sz);
  return 0;
}
// TEST_F(TransCtxEnv, mergeTest) {
//  int key = 1;
//  {
//    STransCtx *src = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
//    transCtxInit(src);
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    transCtxMerge(ctx, src);
//    taosMemoryFree(src);
//  }
//  EXPECT_EQ(2, taosHashGetSize(ctx->args));
//  {
//    STransCtx *src = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
//    transCtxInit(src);
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    transCtxMerge(ctx, src);
//    taosMemoryFree(src);
//  }
//  std::string val("Hello");
//  EXPECT_EQ(4, taosHashGetSize(ctx->args));
//  {
//    key = 1;
//    STransCtx *src = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
//    transCtxInit(src);
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryCalloc(1, 11);
//      val1.clone = cloneVal;
//      memcpy(val1.val, val.c_str(), val.size());
//
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryCalloc(1, 11);
//      val1.clone = cloneVal;
//      memcpy(val1.val, val.c_str(), val.size());
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    transCtxMerge(ctx, src);
//    taosMemoryFree(src);
//  }
//  EXPECT_EQ(4, taosHashGetSize(ctx->args));
//
//  char *skey = (char *)transCtxDumpVal(ctx, 1);
//  EXPECT_EQ(0, strcmp(skey, val.c_str()));
//  taosMemoryFree(skey);
//
//  skey = (char *)transCtxDumpVal(ctx, 2);
//  EXPECT_EQ(0, strcmp(skey, val.c_str()));
//}
typedef struct {
  int32_t err;
  char *str;
} SErrorCode;
TEST(uvTest, testCodeError) {
  int32_t error = UV_EINVAL;

  SErrorCode codeDict[] = {
   {E2BIG, "argument list too long"},                                         
   {EACCES, "permission denied"},
  {EADDRINUSE, "address already in use"},
  {EADDRINUSE, "address already in use"                                    },
  {EADDRNOTAVAIL, "address not available"                                  },
  {EAFNOSUPPORT, "address family not supported"                            },
  {EAGAIN, "resource temporarily unavailable"                              },
  {EAI_ADDRFAMILY, "address family not supported"                          },
  {EAI_AGAIN, "temporary failure"                                          },
  {EAI_BADFLAGS, "bad ai_flags value"                                      },
  {EAI_CANCELED, "request canceled"                                        },
  {EAI_FAIL, "permanent failure"                                           },
  {EAI_FAMILY, "ai_family not supported"                                   },
  {EAI_MEMORY, "out of memory"                                             },
  {EAI_NODATA, "no address"                                                },
  {EAI_NONAME, "unknown node or service"                                   },
  {EAI_OVERFLOW, "argument buffer overflow"                                },
  {EAI_SERVICE, "service not available for socket type"                    },
  {EAI_SOCKTYPE, "socket type not supported"                               },
  {EALREADY, "connection already in progress"                              },
  {EBADF, "bad file descriptor"                                            }, 
  {EBUSY, "resource busy or locked"                                        },
  {ECANCELED, "operation canceled"                                         },
  {ECONNABORTED, "software caused connection abort"                        },
  {ECONNREFUSED, "connection refused"                                      },
  {ECONNRESET, "connection reset by peer"                                  },
  {EDESTADDRREQ, "destination address required"                            },
  {EEXIST, "file already exists"                                           },
  {EFAULT, "bad address in system call argument"                           },
  {EFBIG, "file too large"                                                 },
  {EHOSTUNREACH, "host is unreachable"                                     },
  {EINTR, "interrupted system call"                                        },
  {EINVAL, "invalid argument"                                              },
  {EIO, "i/o error"                                                        },
  {EISCONN, "socket is already connected"                                  },
  {EISDIR, "illegal operation on a directory"                              },
  {ELOOP, "too many symbolic links encountered"                            },
  {EMFILE, "too many open files"                                           },
  {EMSGSIZE, "message too long"                                            },
  {ENAMETOOLONG, "name too long"                                           },
  {ENETDOWN, "network is down"                                             },
  {ENETUNREACH, "network is unreachable"                                   },
  {ENFILE, "file table overflow"                                           },
  {ENOBUFS, "no buffer space available"                                    },
  {ENODEV, "no such device"                                                },
  {ENOENT, "no such file or directory"                                     },
  {ENOMEM, "not enough memory"                                             },
  {ENONET, "machine is not on the network"                                 },
  {ENOPROTOOPT, "protocol not available"                                   },
  {ENOSPC, "no space left on device"                                       },
  {ENOSYS, "function not implemented"                                      },
  {ENOTCONN, "socket is not connected"                                     },
  {ENOTDIR, "not a directory"                                              },
  {ENOTEMPTY, "directory not empty"                                        },
  {ENOTSOCK, "socket operation on non-socket"                              },
  {ENOTSUP, "operation not supported on socket"                            },
  {EOVERFLOW, "value too large for defined data type"                      },
  {EPERM, "operation not permitted"                                        },
  {EPIPE, "broken pipe"                                                    },
  {EPROTO, "protocol error"                                                },
  {EPROTONOSUPPORT, "protocol not supported"                               },
  {EPROTOTYPE, "protocol wrong type for socket"                            },
  {ERANGE, "result too large"                                              },
  {EROFS, "read-only file system"                                          },
  {ESHUTDOWN, "cannot send after transport endpoint shutdown"              },
  {ESPIPE, "invalid seek"                                                  },
  {ESRCH, "no such process"                                                },
  {ETIMEDOUT, "connection timed out"                                       },
  {ETXTBSY, "text file is busy"                                            },
  {EXDEV, "cross-device link not permitted"                                },
  {EOF, "end of file"                                                      },
  {ENXIO, "no such device or address"                                      },
  {EMLINK, "too many links"                                                },
  {EHOSTDOWN, "host is down"                                               },
  {EREMOTEIO, "remote I/O error"                                           },
  {ENOTTY, "inappropriate ioctl for device"                                },
  {EILSEQ, "illegal byte sequence"                                         },
  {ESOCKTNOSUPPORT, "socket type not supported"                            },
  {ENODATA, "no data available"                                            },
  {EUNATCH, "protocol driver not attached"                                 }
 };

  
  for (int i = 0; i < sizeof(codeDict) / sizeof(codeDict[0]); i++) {
    int32_t code = transCvtUvErrno(-codeDict[i].err);
    const char *taosErr = tstrerror(code);
    int32_t len = strlen(taosErr);
    const char *uvErr = uv_strerror(-codeDict[i].err);
    int32_t cmp = strncasecmp(taosErr, uvErr, len);
    printf("%s\t \t%s %d\n", taosErr, uvErr, cmp);
    EXPECT_EQ(0, cmp);
  } 
   
}

#endif
