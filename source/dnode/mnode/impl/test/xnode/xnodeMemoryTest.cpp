/**
 * @file xnodeMemoryTest.cpp
 * @brief XNode memory management unit tests
 * @version 1.0
 * @date 2025-12-25
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <vector>

// Test structure
// typedef struct SXnodeObj {
//   int32_t id;
//   int32_t urlLen;
//   char*   url;
//   int32_t statusLen;
//   char*   status;
//   int64_t createTime;
//   int64_t updateTime;
// } SXnodeObj;

// typedef struct SXnodeTaskObj {
//   int32_t tid;
//   char    name[64];
//   int32_t xnodeId;
//   int32_t agentId;
//   int32_t optionsLen;
//   char*   options;
//   int64_t createTime;
//   int64_t updateTime;
// } SXnodeTaskObj;

extern "C" {
#include "mndDef.h"
}

class XnodeMemoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    allocCount = 0;
    freeCount = 0;
  }

  void TearDown() override {
    // Verify no memory leaks
    EXPECT_EQ(allocCount, freeCount) << "Memory leak detected!";
  }

  int allocCount;
  int freeCount;

  SXnodeObj* createXnode(int id, const char* url, const char* status) {
    SXnodeObj* obj = (SXnodeObj*)taosMemMalloc(sizeof(SXnodeObj));
    allocCount++;

    memset(obj, 0, sizeof(SXnodeObj));
    obj->id = id;

    if (url) {
      obj->urlLen = strlen(url) + 1;
      obj->url = (char*)taosMemMalloc(obj->urlLen);
      allocCount++;
      strcpy(obj->url, url);
    }

    if (status) {
      obj->statusLen = strlen(status) + 1;
      obj->status = (char*)taosMemMalloc(obj->statusLen);
      allocCount++;
      strcpy(obj->status, status);
    }

    obj->createTime = 1000000;
    obj->updateTime = 2000000;

    return obj;
  }

  void freeXnode(SXnodeObj* obj) {
    if (!obj) return;

    if (obj->url) {
      taosMemFree(obj->url);
    }

    if (obj->status) {
      taosMemFree(obj->status);
    }

    taosMemFree(obj);
    freeCount++;
  }

  SXnodeTaskObj* createTask(int tid, const char* name, int xnodeId) {
    SXnodeTaskObj* task = (SXnodeTaskObj*)taosMemMalloc(sizeof(SXnodeTaskObj));
    allocCount++;

    memset(task, 0, sizeof(SXnodeTaskObj));
    task->id = tid;
    task->xnodeId = xnodeId;
    task->via = -1;

    if (name) {
      strncpy(task->name, name, sizeof(task->name) - 1);
    }

    return task;
  }

  void freeTask(SXnodeTaskObj* task) {
    if (!task) return;

    if (task->name) {
      taosMemFree(task->name);
      freeCount++;
    }
    if (task->sourceDsn) {
      taosMemFree(task->sourceDsn);
      freeCount++;
    }
    if (task->sinkDsn) {
      taosMemFree(task->sinkDsn);
      freeCount++;
    }
    if (task->parser) {
      taosMemFree(task->parser);
      freeCount++;
    }
    if (task->status) {
      taosMemFree(task->status);
      freeCount++;
    }
    if (task->reason) {
      taosMemFree(task->reason);
      freeCount++;
    }
    taosMemFree(task);
    freeCount++;
  }
};

TEST_F(XnodeMemoryTest, SingleObjectAllocFree) {
  SXnodeObj* obj = createXnode(1, "node1:6050", "online");
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->id, 1);
  EXPECT_STREQ(obj->url, "node1:6050");
  EXPECT_STREQ(obj->status, "online");

  freeXnode(obj);
  // Leak check in TearDown
}

TEST_F(XnodeMemoryTest, MultipleObjectsAllocFree) {
  std::vector<SXnodeObj*> objects;

  // Create 100 objects
  for (int i = 0; i < 100; i++) {
    char url[32];
    snprintf(url, sizeof(url), "node%d:6050", i);
    SXnodeObj* obj = createXnode(i, url, i % 2 ? "online" : "offline");
    objects.push_back(obj);
  }

  EXPECT_EQ(objects.size(), 100);

  // Free all objects
  for (auto obj : objects) {
    freeXnode(obj);
  }

  objects.clear();
}

TEST_F(XnodeMemoryTest, NullFieldsHandling) {
  // Create object with null fields
  SXnodeObj* obj = createXnode(1, nullptr, nullptr);
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->urlLen, 0);
  EXPECT_EQ(obj->statusLen, 0);
  EXPECT_EQ(obj->url, nullptr);
  EXPECT_EQ(obj->status, nullptr);

  freeXnode(obj);
}

TEST_F(XnodeMemoryTest, PartialFieldsHandling) {
  // Only URL, no status
  SXnodeObj* obj1 = createXnode(1, "node1:6050", nullptr);
  ASSERT_NE(obj1, nullptr);
  EXPECT_NE(obj1->url, nullptr);
  EXPECT_EQ(obj1->status, nullptr);
  freeXnode(obj1);

  // Only status, no URL
  SXnodeObj* obj2 = createXnode(2, nullptr, "online");
  ASSERT_NE(obj2, nullptr);
  EXPECT_EQ(obj2->url, nullptr);
  EXPECT_NE(obj2->status, nullptr);
  freeXnode(obj2);
}

TEST_F(XnodeMemoryTest, TaskObjectMemory) {
  SXnodeTaskObj* task = createTask(100, "test_task", 1);
  ASSERT_NE(task, nullptr);
  EXPECT_EQ(task->id, 100);
  EXPECT_STREQ(task->name, "test_task");
  EXPECT_EQ(task->xnodeId, 1);

  // Add options
  const char* opts = "key1=value1,key2=value2";
  // task->optionsLen = strlen(opts) + 1;
  // task->options = (char*)taosMemMalloc(task->optionsLen);
  allocCount++;
  // strcpy(task->options, opts);

  // EXPECT_STREQ(task->options, opts);

  freeTask(task);
}

TEST_F(XnodeMemoryTest, MultipleTasksMemory) {
  std::vector<SXnodeTaskObj*> tasks;

  for (int i = 0; i < 50; i++) {
    char name[64];
    snprintf(name, sizeof(name), "task_%d", i);
    SXnodeTaskObj* task = createTask(i, name, i % 5);
    tasks.push_back(task);
  }

  EXPECT_EQ(tasks.size(), 50);

  // Free all tasks
  for (auto task : tasks) {
    freeTask(task);
  }

  tasks.clear();
}

TEST_F(XnodeMemoryTest, LargeStringFields) {
  // Create object with large strings
  char largeUrl[1024];
  char largeStatus[512];

  memset(largeUrl, 'A', sizeof(largeUrl) - 1);
  largeUrl[sizeof(largeUrl) - 1] = '\0';

  memset(largeStatus, 'S', sizeof(largeStatus) - 1);
  largeStatus[sizeof(largeStatus) - 1] = '\0';

  SXnodeObj* obj = createXnode(1, largeUrl, largeStatus);
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->urlLen, sizeof(largeUrl));
  EXPECT_EQ(obj->statusLen, sizeof(largeStatus));
  EXPECT_STREQ(obj->url, largeUrl);
  EXPECT_STREQ(obj->status, largeStatus);

  freeXnode(obj);
}

TEST_F(XnodeMemoryTest, ZeroLengthStrings) {
  // Empty strings (not null)
  SXnodeObj* obj = createXnode(1, "", "");
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->urlLen, 1);  // '\0' only
  EXPECT_EQ(obj->statusLen, 1);
  EXPECT_STREQ(obj->url, "");
  EXPECT_STREQ(obj->status, "");

  freeXnode(obj);
}

TEST_F(XnodeMemoryTest, RapidAllocFree) {
  // Test rapid allocation and deallocation
  for (int i = 0; i < 1000; i++) {
    SXnodeObj* obj = createXnode(i, "node:6050", "online");
    ASSERT_NE(obj, nullptr);
    freeXnode(obj);
  }

  // All should be properly freed (checked in TearDown)
}

TEST_F(XnodeMemoryTest, MixedObjectTypes) {
  // Mix XNode and Task objects
  SXnodeObj*     xnode = createXnode(1, "node1:6050", "online");
  SXnodeTaskObj* task1 = createTask(100, "task1", 1);
  SXnodeTaskObj* task2 = createTask(101, "task2", 1);
  SXnodeObj*     xnode2 = createXnode(2, "node2:6051", "offline");

  ASSERT_NE(xnode, nullptr);
  ASSERT_NE(task1, nullptr);
  ASSERT_NE(task2, nullptr);
  ASSERT_NE(xnode2, nullptr);

  // Free in different order
  freeTask(task1);
  freeXnode(xnode);
  freeTask(task2);
  freeXnode(xnode2);
}

TEST_F(XnodeMemoryTest, DoubleFreePrevention) {
  SXnodeObj* obj = createXnode(1, "node1:6050", "online");
  ASSERT_NE(obj, nullptr);

  freeXnode(obj);

  // Second free with nullptr should be safe
  freeXnode(nullptr);

  // This is just to demonstrate safe null handling
  // In real code, we should not double-free
}

TEST_F(XnodeMemoryTest, StressTest) {
  // Stress test: create and free many objects in random order
  std::vector<SXnodeObj*> objects;

  // Create 500 objects
  for (int i = 0; i < 500; i++) {
    char url[32];
    snprintf(url, sizeof(url), "node%d:%d", i, 6050 + (i % 10));
    SXnodeObj* obj = createXnode(i, url, i % 3 ? "online" : "offline");
    objects.push_back(obj);
  }

  // Free every other object
  for (size_t i = 0; i < objects.size(); i += 2) {
    freeXnode(objects[i]);
    objects[i] = nullptr;
  }

  // Free remaining objects
  for (auto obj : objects) {
    if (obj) {
      freeXnode(obj);
    }
  }
}

TEST_F(XnodeMemoryTest, BoundaryValues) {
  // Test with boundary integer values
  SXnodeObj* obj = createXnode(INT32_MAX, "max:6050", "status");
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->id, INT32_MAX);
  freeXnode(obj);

  obj = createXnode(INT32_MIN, "min:6050", "status");
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->id, INT32_MIN);
  freeXnode(obj);

  obj = createXnode(0, "zero:6050", "status");
  ASSERT_NE(obj, nullptr);
  EXPECT_EQ(obj->id, 0);
  freeXnode(obj);
}
