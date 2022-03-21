/**
 * @file queue.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief UTIL module queue tests
 * @version 1.0
 * @date 2022-01-27
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>

#include "os.h"
#include "tqueue.h"

#include <sys/shm.h>
#include <sys/wait.h>

class UtilTestQueue : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 public:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}
};

#if 0
TEST_F(UtilTestQueue, 01_fork) {
  pid_t pid;
  int   shmid;
  int*  shmptr;
  int*  tmp;

  int                 err;
  pthread_mutexattr_t mattr;
  if ((err = pthread_mutexattr_init(&mattr)) < 0) {
    printf("mutex addr init error:%s\n", strerror(err));
    exit(1);
  }

  if ((err = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED)) < 0) {
    printf("mutex addr get shared error:%s\n", strerror(err));
    exit(1);
  }

  pthread_mutex_t* m;
  int              mid = shmget(IPC_PRIVATE, sizeof(pthread_mutex_t), 0600);
  m = (pthread_mutex_t*)shmat(mid, NULL, 0);

  if ((err = pthread_mutex_init(m, &mattr)) < 0) {
    printf("mutex mutex init error:%s\n", strerror(err));
    exit(1);
  }

  if ((shmid = shmget(IPC_PRIVATE, 1000, IPC_CREAT | 0600)) < 0) {
    perror("shmget error");
    exit(1);
  }

  if ((shmptr = (int*)shmat(shmid, 0, 0)) == (void*)-1) {
    perror("shmat error");
    exit(1);
  }

  tmp = shmptr;

  int   shmid2;
  int** shmptr2;
  if ((shmid2 = shmget(IPC_PRIVATE, 20, IPC_CREAT | 0600)) < 0) {
    perror("shmget2 error");
    exit(1);
  }

  if ((shmptr2 = (int**)shmat(shmid2, 0, 0)) == (void*)-1) {
    perror("shmat2 error");
    exit(1);
  }

  *shmptr2 = shmptr;

  if ((pid = fork()) < 0) {
    perror("fork error");
    exit(1);
  } else if (pid == 0) {
    if ((err = taosThreadMutexLock(m)) < 0) {
      printf("lock error:%s\n", strerror(err));
      exit(1);
    }
    for (int i = 0; i < 30; ++i) {
      **shmptr2 = i;
      (*shmptr2)++;
    }

    if ((err = taosThreadMutexUnlock(m)) < 0) {
      printf("unlock error:%s\n", strerror(err));
      exit(1);
    }
    exit(0);

  } else {
    if ((err = taosThreadMutexLock(m)) < 0) {
      printf("lock error:%s\n", strerror(err));
      exit(1);
    }
    for (int i = 10; i < 42; ++i) {
      **shmptr2 = i;
      (*shmptr2)++;
    }
    if ((err = taosThreadMutexUnlock(m)) < 0) {
      printf("unlock error:%s\n", strerror(err));
      exit(1);
    }
  }

  wait(NULL);

  for (int i = 0; i < 70; ++i) {
    printf("%d ", tmp[i]);
  }

  printf("\n");

  taosThreadAttrDestroy(&mattr);
  //销毁mutex
  pthread_mutex_destroy(m);

  exit(0);
}

#endif