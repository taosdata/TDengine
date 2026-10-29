/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/*
 * Workload Recorder (WLR) in-process behaviour unit tests.
 *
 * Covers selected CFG021-CFG026, ORD001/005/008/009/013-016, FO006 and
 * RE014-RE016 cases from the DS 11.4 in-process, concurrency and fault-test
 * buckets. Individual tests cite the exact clause they enforce.
 *
 * Unlike wlrFormatTest.cpp, these tests exercise the implemented Recorder
 * Manager, queue and writer directly through clientWlr.h and clientWlrInt.h.
 * If the public or internal interfaces change, update the tests rather than
 * weakening their assertions. Each one cites the DS clause it enforces.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <mutex>
#include <vector>

#if defined(__has_include)
#if __has_include("clientWlr.h")
#define WLR_HAVE_RECORDER 1
#endif
#endif

#ifndef WLR_HAVE_RECORDER

// --------------------------------------------------------------------------
// Recorder not implemented. Keep the target buildable and make the absence
// visible in CI output rather than silently reporting an empty pass.
// --------------------------------------------------------------------------
TEST(wlrRecorder, NotImplementedYet) {
  FAIL() << "clientWlr.h absent: recorder behaviour tests must not silently disappear";
}

#else  // WLR_HAVE_RECORDER

extern "C" {
#include "clientInt.h"
#include "clientWlr.h"
#include "clientWlrInt.h"
}

#ifdef WINDOWS
#include <process.h>
#include <windows.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {

struct AsyncFailureCallbackProbe {
  bool     childRecorded;
  int32_t  callbackDepth;
  uint64_t parentCallId;
};

void asyncFailureCallbackProbe(void *param, TAOS_RES *, int32_t) {
  AsyncFailureCallbackProbe *probe = static_cast<AsyncFailureCallbackProbe *>(param);
  probe->callbackDepth = wlrTlsDepth;
  probe->parentCallId = wlrGetCallbackParent();

  SWorkloadCallToken child = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select recovery";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  probe->childRecorded = wlrApiBegin(&args, &child) == 0 && child.recorded;
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&child, &ret);
}

std::string makeTempDir() {
#ifdef WINDOWS
  char root[MAX_PATH] = {0};
  DWORD rootLen = GetTempPathA((DWORD)sizeof(root), root);
  if (rootLen == 0 || rootLen >= sizeof(root)) return "";
  if (root[rootLen - 1] != '\\' && root[rootLen - 1] != '/') {
    if (rootLen + 1 >= sizeof(root)) return "";
    root[rootLen++] = '\\';
    root[rootLen] = '\0';
  }
  for (int attempt = 0; attempt < 100; ++attempt) {
    char path[MAX_PATH] = {0};
    int n = snprintf(path, sizeof(path), "%swlr-ut-%d-%d", root, _getpid(), attempt);
    if (n <= 0 || (size_t)n >= sizeof(path)) return "";
    if (CreateDirectoryA(path, NULL) != 0) return path;
    if (GetLastError() != ERROR_ALREADY_EXISTS) return "";
  }
  return "";
#else
  const char* tempRoot = std::getenv("TMPDIR");
  std::string tmpl = std::string(tempRoot != nullptr && tempRoot[0] != '\0' ? tempRoot : "/tmp") +
                     "/wlr-ut-XXXXXX";
  char* p = mkdtemp(&tmpl[0]);
  if (p == nullptr) {
    int errorCode = errno;
    GTEST_LOG_(ERROR) << "mkdtemp(" << tmpl << ") failed: errno=" << errorCode << " ("
                      << std::strerror(errorCode) << ")";
    return "";
  }
  return p;
#endif
}

using FrameVisitor = std::function<bool(const SWlrFrameHeader&, const uint8_t*)>;

bool visitDecodedFrames(const std::string& dir, const FrameVisitor& visitor) {
  std::vector<std::string> paths;
  TdDirPtr d = taosOpenDir(dir.c_str());
  if (d == nullptr) return false;
  for (TdDirEntryPtr de = taosReadDir(d); de != nullptr; de = taosReadDir(d)) {
    const char* name = taosGetDirEntryName(de);
    if (name == nullptr) continue;
    std::string segmentName(name);
    const std::string suffix = WLR_SEGMENT_SUFFIX;
    if (segmentName.size() > suffix.size() &&
        segmentName.compare(segmentName.size() - suffix.size(), suffix.size(), suffix) == 0) {
      paths.push_back(dir + TD_DIRSEP + segmentName);
    }
  }
  (void)taosCloseDir(&d);
  if (paths.empty()) {
    ADD_FAILURE() << "no recording segments matching " << WLR_SEGMENT_SUFFIX << " found in " << dir;
    return false;
  }

  for (const std::string& path : paths) {
    int64_t fileSize = 0;
    if (taosStatFile(path.c_str(), &fileSize, nullptr, nullptr) != 0 || fileSize <= 0) {
      ADD_FAILURE() << "cannot stat recording segment " << path;
      continue;
    }
    constexpr int64_t kMaxBufferedSegmentSize = 64ll * 1024 * 1024;
    if (fileSize > kMaxBufferedSegmentSize) {
      ADD_FAILURE() << "recording segment exceeds whole-file test limit (" << kMaxBufferedSegmentSize
                    << " bytes): " << path;
      continue;
    }
    std::vector<uint8_t> buf;
    try {
      buf.resize(static_cast<size_t>(fileSize));
    } catch (const std::bad_alloc&) {
      ADD_FAILURE() << "cannot allocate " << fileSize << " bytes for recording segment " << path;
      continue;
    } catch (const std::length_error&) {
      ADD_FAILURE() << "recording segment size is not representable by vector: " << path;
      continue;
    }
    TdFilePtr file = taosOpenFile(path.c_str(), TD_FILE_READ);
    if (file == nullptr) {
      ADD_FAILURE() << "cannot open recording segment " << path;
      continue;
    }
    int64_t readBytes = taosReadFile(file, buf.data(), fileSize);
    (void)taosCloseFile(&file);
    if (readBytes != fileSize) {
      ADD_FAILURE() << "short read for recording segment " << path;
      continue;
    }

    SWlrFileHeader header;
    EWlrDecodeResult headerResult = wlrDecodeFileHeader(buf.data(), buf.size(), &header);
    if (headerResult != WLR_DECODE_OK) {
      ADD_FAILURE() << "failed to decode file header for " << path << ": " << headerResult;
      continue;
    }
    size_t offset = header.headerLength;
    while (offset < buf.size()) {
      SWlrFrameHeader frame;
      const uint8_t* payload = nullptr;
      uint32_t consumed = 0;
      EWlrDecodeResult frameResult =
          wlrDecodeFrame(buf.data() + offset, buf.size() - offset, &frame, &payload, &consumed);
      if (frameResult != WLR_DECODE_OK) {
        ADD_FAILURE() << "failed to decode frame in " << path << " at offset " << offset << ": "
                      << frameResult;
        break;
      }
      if (visitor(frame, payload)) {
        return true;
      }
      offset += consumed;
    }
  }
  return false;
}

/*
 * Return the payload length of the earliest event across all segments, or -1
 * if none decodes. A RETURN is small and well formed even if the BEGIN payload
 * was lost, so choosing the last event would not detect that failure.
 */
int32_t firstEventPayloadLen(const std::string& dir) {
  uint64_t earliestSeq = UINT64_MAX;
  int32_t  firstLen = -1;
  (void)visitDecodedFrames(dir, [&](const SWlrFrameHeader& frame, const uint8_t*) {
    if (frame.type == WLR_FRAME_EVENT && frame.eventSequence < earliestSeq) {
      earliestSeq = frame.eventSequence;
      firstLen = static_cast<int32_t>(frame.payloadLength);
    }
    return false;
  });
  EXPECT_GE(firstLen, 0) << "failed to decode any event frame";
  return firstLen;
}

bool firstBeginHasTagValue(const std::string& dir, uint16_t apiType, uint16_t tag,
                           const std::string& expected) {
  uint64_t earliestSeq = UINT64_MAX;
  bool     matches = false;
  (void)visitDecodedFrames(dir, [&](const SWlrFrameHeader& frame, const uint8_t* payload) {
    if (frame.type == WLR_FRAME_EVENT && frame.eventSequence < earliestSeq) {
      earliestSeq = frame.eventSequence;
      matches = false;
      int32_t count = 0;
      EWlrDecodeResult decodeResult =
          wlrDecodeTlvs(payload, frame.payloadLength, nullptr, 0, &count);
      EXPECT_EQ(decodeResult, WLR_DECODE_OK) << "failed to count TLVs in the first event";
      if (decodeResult != WLR_DECODE_OK) return false;
      const int32_t capacity = count;
      std::vector<SWlrTlv> tlvs(static_cast<size_t>(capacity));
      int32_t decodedCount = 0;
      decodeResult = wlrDecodeTlvs(payload, frame.payloadLength, tlvs.data(), capacity, &decodedCount);
      EXPECT_EQ(decodeResult, WLR_DECODE_OK) << "failed to decode TLVs in the first event";
      EWlrDecodeResult schemaResult =
          decodeResult == WLR_DECODE_OK && decodedCount == capacity
              ? wlrValidateRequiredTags(WLR_FRAME_EVENT, apiType, WLR_PHASE_BEGIN, tlvs.data(), decodedCount)
              : WLR_DECODE_BAD_LENGTH;
      EXPECT_EQ(schemaResult, WLR_DECODE_OK) << "first event does not satisfy its BEGIN schema";
      if (schemaResult == WLR_DECODE_OK) {
        for (int32_t i = 0; i < decodedCount; ++i) {
          if (WLR_TAG_BASE(tlvs[i].tag) != tag || tlvs[i].length != expected.size()) continue;
          if (expected.empty()) {
            matches = true;
            break;
          }
          if (std::memcmp(tlvs[i].value, expected.data(), expected.size()) == 0) {
            matches = true;
            break;
          }
        }
      }
    }
    return false;
  });
  EXPECT_NE(earliestSeq, UINT64_MAX) << "failed to decode any event frame";
  return matches;
}

bool sessionStopHasActiveCallCount(const std::string& dir, uint64_t expected) {
  return visitDecodedFrames(dir, [&](const SWlrFrameHeader& frame, const uint8_t* payload) {
    if (frame.type != WLR_FRAME_SESSION_STOP) return false;
    int32_t count = 0;
    if (wlrDecodeTlvs(payload, frame.payloadLength, nullptr, 0, &count) != WLR_DECODE_OK) {
      return false;
    }
    const int32_t capacity = count;
    std::vector<SWlrTlv> tlvs(static_cast<size_t>(capacity));
    int32_t decodedCount = 0;
    if (wlrDecodeTlvs(payload, frame.payloadLength, tlvs.data(), capacity, &decodedCount) != WLR_DECODE_OK ||
        decodedCount != capacity) {
      return false;
    }
    const SWlrTlv* active = nullptr;
    for (int32_t i = 0; i < decodedCount; ++i) {
      if (WLR_TAG_BASE(tlvs[static_cast<size_t>(i)].tag) == WLR_TAG_ACTIVE_CALL_COUNT)
        active = &tlvs[static_cast<size_t>(i)];
    }
    return active != nullptr && active->length == sizeof(uint64_t) &&
           wlrGetU64(active->value) == expected;
  });
}

bool hasCleanSessionStop(const std::string& dir) {
  return sessionStopHasActiveCallCount(dir, 0);
}

std::string findManifest(const std::string& dir);
std::string readWholeFile(const std::string& path);

/* Read the session manifest, or "" when absent. */
std::string readManifestJson(const std::string& dir) {
  return readWholeFile(findManifest(dir));
}

void removeTree(const std::string& dir) {
  if (dir.empty()) return;
  taosRemoveDir(dir.c_str());
}

bool jsonHasBool(const std::string& json, const char* key, bool expected) {
  std::string needle = std::string("\"") + key + "\"";
  size_t pos = 0;
  do {
    pos = json.find(needle, pos);
    if (pos == std::string::npos) return false;
    size_t after = pos + needle.size();
    while (after < json.size() && std::isspace(static_cast<unsigned char>(json[after]))) ++after;
    if (after < json.size() && json[after] == ':') {
      pos = after;
      break;
    }
    pos = after;
  } while (pos < json.size());
  pos = json.find_first_not_of(" \t\r\n", pos + 1);
  const char* value = expected ? "true" : "false";
  if (pos == std::string::npos || json.compare(pos, std::strlen(value), value) != 0) return false;
  size_t end = pos + std::strlen(value);
  return end == json.size() || std::isspace(static_cast<unsigned char>(json[end])) ||
         json[end] == ',' || json[end] == '}' || json[end] == ']';
}

bool jsonHasInteger(const std::string& json, const char* key, int64_t expected) {
  std::string needle = std::string("\"") + key + "\"";
  size_t pos = 0;
  do {
    pos = json.find(needle, pos);
    if (pos == std::string::npos) return false;
    size_t after = pos + needle.size();
    while (after < json.size() && std::isspace(static_cast<unsigned char>(json[after]))) ++after;
    if (after < json.size() && json[after] == ':') {
      pos = after;
      break;
    }
    pos = after;
  } while (pos < json.size());
  pos = json.find_first_not_of(" \t\r\n", pos + 1);
  std::string value = std::to_string(expected);
  bool quoted = pos != std::string::npos && json[pos] == '"';
  if (quoted) ++pos;
  if (pos == std::string::npos || json.compare(pos, value.size(), value) != 0) return false;
  size_t end = pos + value.size();
  if (quoted) {
    if (end >= json.size() || json[end] != '"') return false;
    ++end;
  }
  return end == json.size() || std::isspace(static_cast<unsigned char>(json[end])) ||
         json[end] == ',' || json[end] == '}' || json[end] == ']';
}

// Fixture: one recorder session per test, always torn down.
class WlrRecorderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = makeTempDir();
    ASSERT_FALSE(dir_.empty()) << "mkdtemp failed";
  }

  void TearDown() override {
    // Fault injection is process-wide state on a single shared recorder, so a
    // stalled writer left set here would hang the NEXT test rather than this one.
    // Clear both flags before closing.
    if (wlrRecorderState() != WLR_STATE_OFF) {
      (void)wlrTestResumeWriter();
      (void)wlrTestResumeWrite();
      atomic_store_32(&wlrMgr.stallFinalIo, 0);
      EXPECT_EQ(wlrTestClearWriterFailure(), 0);
    }
    /* Reset both public-API nesting and pending async handoffs so an intentionally
     * unfinished call cannot make the next test order-dependent. */
    wlrTestResetDepth();
    wlrRecorderClose();
    for (int attempt = 0; attempt < 1000 && wlrRecorderState() != WLR_STATE_OFF; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ASSERT_EQ(wlrRecorderState(), WLR_STATE_OFF);
    removeTree(dir_);
  }

  int32_t startRecorder(int32_t mode = 1, int32_t bufferMb = 256) {
    SWorkloadRecorderCfg cfg;
    std::memset(&cfg, 0, sizeof(cfg));
    cfg.mode = mode;
    cfg.bufferSizeMb = bufferMb;
    std::snprintf(cfg.recordDir, sizeof(cfg.recordDir), "%s", dir_.c_str());
    return wlrRecorderInit(&cfg);
  }

  // A minimal recorded call: BEGIN then RETURN, as a query would do.
  int32_t recordCall(uint64_t connId, const char* sql, SWorkloadCallToken* tokenOut = nullptr) {
    SWorkloadCallToken token;
    std::memset(&token, 0, sizeof(token));
    SWorkloadBeginArgs args;
    std::memset(&args, 0, sizeof(args));
    args.apiType = WLR_API_QUERY;
    args.logicalConnId = connId;
    args.sql = sql;
    args.sqlLen = static_cast<int32_t>(std::strlen(sql));

    // Returns 0 when the call completed without blocking, regardless of whether
    // it was recorded. wlrApiBegin's non-zero codes mean "not recorded", which is
    // the expected state when the recorder is off or has failed open.
    int32_t beginCode = wlrApiBegin(&args, &token);
    if (beginCode != 0) return beginCode;
    if (!token.recorded) return 0;

    SWorkloadReturnArgs ret;
    std::memset(&ret, 0, sizeof(ret));
    ret.code = 0;
    wlrApiReturn(&token, &ret);
    if (tokenOut) *tokenOut = token;
    return 0;
  }

  void checkBlockedFinalIo(int32_t stage) {
    ASSERT_EQ(startRecorder(1), 0);
    ASSERT_EQ(recordCall(1, "select 1"), 0);
    atomic_store_32(&wlrMgr.stallFinalIo, stage);

    auto started = std::chrono::steady_clock::now();
    wlrRecorderClose();
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(2));
    EXPECT_EQ(atomic_load_32(&wlrMgr.finalIoStalled), stage);
    EXPECT_EQ(atomic_load_32(&wlrMgr.writerDeferred), 1);
    EXPECT_EQ(atomic_load_32(&wlrMgr.captureIncomplete), 1);
    EXPECT_EQ(wlrRecorderState(), WLR_STATE_STOPPING);
    EXPECT_NE(wlrMgr.pool, nullptr);
    EXPECT_TRUE(wlrMgr.lockReady);

    atomic_store_32(&wlrMgr.stallFinalIo, 0);
    for (int attempt = 0; attempt < 1000 && wlrRecorderState() != WLR_STATE_OFF; ++attempt) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ASSERT_EQ(wlrRecorderState(), WLR_STATE_OFF);
    EXPECT_EQ(wlrMgr.pool, nullptr);
    EXPECT_FALSE(wlrMgr.lockReady);
    std::string manifest = readManifestJson(dir_);
    EXPECT_TRUE(jsonHasBool(manifest, "captureComplete", false)) << manifest;
    EXPECT_TRUE(jsonHasBool(manifest, "gracefullyClosed", false)) << manifest;
  }

  std::string dir_;
};

}  // namespace

// ==================================================== CFG021-CFG026 state machine

// TS CFG021 -- OFF -> STARTING -> RUNNING once the first segment header is out.
TEST_F(WlrRecorderTest, StateOffToRunningOnInit) {
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
  ASSERT_EQ(startRecorder(1), 0);
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_RUNNING)
      << "DS 6.3: RUNNING is entered only after the first segment header is written";
}

TEST_F(WlrRecorderTest, CloseWhileOffDoesNotPoisonNextGeneration) {
  ASSERT_EQ(wlrRecorderState(), WLR_STATE_OFF);
  wlrRecorderClose();
  EXPECT_EQ(atomic_load_32(&wlrMgr.closeRequested), 0);
  ASSERT_EQ(startRecorder(1), 0);
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_RUNNING);
}

TEST_F(WlrRecorderTest, ReinitializationResetsSessionStatistics) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select first_session"), 0);
  wlrRecorderClose();
  ASSERT_GT(wlrTestEventsWritten(), 0);

  ASSERT_EQ(startRecorder(1), 0);
  EXPECT_EQ(wlrTestEventsWritten(), 0)
      << "a new session must not inherit the previous session's event count";
}

TEST_F(WlrRecorderTest, ConcurrentCloseTearsDownOnce) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select concurrent_close"), 0);

  constexpr int kClosers = 8;
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> closers;
  for (int i = 0; i < kClosers; ++i) {
    closers.emplace_back([&] {
      ready.fetch_add(1);
      while (!go.load()) std::this_thread::yield();
      wlrRecorderClose();
    });
  }
  while (ready.load() != kClosers) std::this_thread::yield();
  go.store(true);
  for (auto& closer : closers) closer.join();

  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
  EXPECT_FALSE(readManifestJson(dir_).empty());
}

// TS CFG002/CFG004/CFG005 -- mode 0 and reserved modes never allocate.
TEST_F(WlrRecorderTest, ReservedAndOffModesStayOff) {
  for (int32_t mode : {0, 2, 7, 255}) {
    wlrRecorderClose();
    int32_t code = startRecorder(mode);
    EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF) << "mode " << mode << " must not record";
    if (mode == 0) {
      EXPECT_EQ(code, 0) << "OFF mode initialization must succeed without allocating";
    } else {
      EXPECT_EQ(code, WLR_E_CONFIG) << "reserved mode should report WLR_E_CONFIG";
    }
  }
}

// TS CFG022 -- a failure during startup lands in FAILED_OPEN, never stalls in
// STARTING, and never aborts the caller.
TEST_F(WlrRecorderTest, StartingToFailedOpenOnBadDir) {
  SWorkloadRecorderCfg cfg;
  std::memset(&cfg, 0, sizeof(cfg));
  cfg.mode = 1;
  cfg.bufferSizeMb = 256;
  std::string badDir = dir_ + "/not-a-directory";
  TdFilePtr marker = taosOpenFile(badDir.c_str(), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(marker, nullptr);
  ASSERT_EQ(taosCloseFile(&marker), 0);
  std::snprintf(cfg.recordDir, sizeof(cfg.recordDir), "%s", badDir.c_str());

  int32_t code = wlrRecorderInit(&cfg);
  EXPECT_EQ(code, WLR_E_INIT_DIR) << "unwritable dir must report WLR_E_INIT_DIR";
  int32_t st = wlrRecorderState();
  EXPECT_TRUE(st == WLR_STATE_OFF || st == WLR_STATE_FAILED_OPEN)
      << "DS 6.3: must not remain in STARTING; got state " << st;
}

// TS CFG023 -- RUNNING -> STOPPING -> OFF drains and closes.
TEST_F(WlrRecorderTest, RunningToOffOnClose) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select 1"), 0);
  wlrRecorderClose();
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
}

TEST_F(WlrRecorderTest, CloseWaitsForReservedProducerBeforeDestroyingQueue) {
  ASSERT_EQ(startRecorder(1), 0);

  SWlrSlot *slot = nullptr;
  uint8_t *payload = nullptr;
  ASSERT_EQ(wlrReserve(&wlrMgr, 1, WLR_FRAME_EVENT, 0, &slot, &payload), 0);
  payload[0] = 0;

  std::atomic<bool> closeFinished{false};
  std::thread closer([&] {
    wlrRecorderClose();
    closeFinished.store(true);
  });

  for (int i = 0; i < 100 && wlrRecorderState() == WLR_STATE_RUNNING; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool stopping = wlrRecorderState() == WLR_STATE_STOPPING;
  EXPECT_TRUE(stopping);
  EXPECT_FALSE(closeFinished.load()) << "queue storage was destroyed while a producer still owned it";

  if (stopping && !closeFinished.load()) {
    EXPECT_EQ(wlrPublish(&wlrMgr, slot, false), 0);
  }
  closer.join();
  EXPECT_TRUE(closeFinished.load());
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
}

TEST_F(WlrRecorderTest, CloseWaitsForEntrantBeforeQueueLock) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(wlrTestEnterLifecycle(), 0);

  std::atomic<bool> closeFinished{false};
  std::thread closer([&] {
    wlrRecorderClose();
    closeFinished.store(true);
  });

  for (int i = 0; i < 100 && wlrRecorderState() == WLR_STATE_RUNNING; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_STOPPING);
  EXPECT_FALSE(closeFinished.load()) << "queue synchronization was destroyed before an entrant acquired it";

  wlrTestLeaveLifecycle();
  closer.join();
  EXPECT_TRUE(closeFinished.load());
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
}

TEST_F(WlrRecorderTest, ClosedLifecycleAdmissionRejectsLateEntrants) {
  ASSERT_EQ(startRecorder(1), 0);
  wlrTestCloseLifecycleAdmission();

  EXPECT_EQ(wlrTestEnterLifecycle(), -1);
  wlrRecorderClose();
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
}

// TS CFG024 -- a permanent writer error moves RUNNING -> FAILED_OPEN and
// subsequent hooks bypass without blocking. DS 6.16.
TEST_F(WlrRecorderTest, RunningToFailedOpenBypassesHooks) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select 1"), 0);

  ASSERT_EQ(wlrTestInjectWriterFailure(), 0) << "test hook required to simulate writer death";
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_FAILED_OPEN);

  // FO008 -- business calls keep working; recording is simply skipped.
  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select 2";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  EXPECT_NE(wlrApiBegin(&args, &token), 0);
  EXPECT_FALSE(token.recorded) << "fail-open must skip recording without affecting the business call";
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_FAILED_OPEN);
}

// TS CFG025 -- FAILED_OPEN -> OFF on cleanup, no double-free / second fault.
TEST_F(WlrRecorderTest, FailedOpenToOffOnClose) {
  ASSERT_EQ(startRecorder(1), 0);
  EXPECT_EQ(wlrTestInjectWriterFailure(), 0) << "test hook required to fail the writer";
  ASSERT_EQ(wlrRecorderState(), WLR_STATE_FAILED_OPEN);
  wlrRecorderClose();
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
}

// TS FO006 -- no auto-recovery inside the same process: a healed disk must not
// silently resume and produce a file that looks continuous but is not.
TEST_F(WlrRecorderTest, NoAutoRecoveryAfterFailOpen) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(wlrTestInjectWriterFailure(), 0);
  for (int i = 0; i < 10; ++i) {
    SWorkloadCallToken token;
    std::memset(&token, 0, sizeof(token));
    SWorkloadBeginArgs args;
    std::memset(&args, 0, sizeof(args));
    args.apiType = WLR_API_QUERY;
    args.logicalConnId = 1;
    args.sql = "select 3";
    args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
    EXPECT_NE(wlrApiBegin(&args, &token), 0);
    EXPECT_FALSE(token.recorded);
  }
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_FAILED_OPEN)
      << "DS 6.16: recording must not resume in the same process";
}

// TS CFG026 -- during STOPPING no new call acquires a session token, so no
// half-written event is produced. DS 6.4.
TEST_F(WlrRecorderTest, StoppingRejectsNewTokens) {
  ASSERT_EQ(startRecorder(1), 0);

  std::atomic<bool> verifyStopping{false};
  std::atomic<bool> observedStopping{false};
  std::atomic<int>  accepted{0}, rejected{0};

  std::thread worker([&] {
    auto attemptBegin = [&] {
      SWorkloadCallToken token;
      std::memset(&token, 0, sizeof(token));
      SWorkloadBeginArgs args;
      std::memset(&args, 0, sizeof(args));
      args.apiType = WLR_API_QUERY;
      args.logicalConnId = 1;
      args.sql = "select 1";
      args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
      int32_t stateBefore = wlrRecorderState();
      if (wlrApiBegin(&args, &token) == 0) {
        accepted.fetch_add(1);
        SWorkloadReturnArgs ret;
        std::memset(&ret, 0, sizeof(ret));
        wlrApiReturn(&token, &ret);
      } else {
        rejected.fetch_add(1);
        if (stateBefore != WLR_STATE_RUNNING) EXPECT_FALSE(token.recorded);
      }
    };

    while (!verifyStopping.load()) {
      attemptBegin();
    }
    while (wlrRecorderState() != WLR_STATE_STOPPING) std::this_thread::yield();
    observedStopping.store(true);
    attemptBegin();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(wlrTestSetStopping(), 0);
  verifyStopping.store(true);
  for (int i = 0; i < 1000 && !observedStopping.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(observedStopping.load());
  worker.join();
  wlrRecorderClose();

  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
  EXPECT_GT(accepted.load() + rejected.load(), 0);
  EXPECT_GT(rejected.load(), 0) << "shutdown race never observed a rejected token";

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select 1";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  EXPECT_NE(wlrApiBegin(&args, &token), 0) << "OFF recorder accepted a new token";
}

// ============================================================ ORD001-ORD016

// TS ORD001 -- single-thread event sequence is monotonic and matches call order.
TEST_F(WlrRecorderTest, SingleThreadSequenceIsMonotonic) {
  ASSERT_EQ(startRecorder(1), 0);
  std::vector<uint64_t> seqs;
  for (int i = 0; i < 64; ++i) {
    SWorkloadCallToken token;
    std::memset(&token, 0, sizeof(token));
    SWorkloadBeginArgs args;
    std::memset(&args, 0, sizeof(args));
    args.apiType = WLR_API_QUERY;
    args.logicalConnId = 1;
    args.sql = "select 1";
    args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
    ASSERT_EQ(wlrApiBegin(&args, &token), 0);
    seqs.push_back(wlrTokenEventSeq(&token));
    SWorkloadReturnArgs ret;
    std::memset(&ret, 0, sizeof(ret));
    wlrApiReturn(&token, &ret);
  }
  for (size_t i = 1; i < seqs.size(); ++i) {
    EXPECT_GT(seqs[i], seqs[i - 1]) << "eventSeq must strictly increase at index " << i;
  }
}

// TS ORD014 -- one thread across many connections: each connection's causal
// order holds and the global sequence is still strictly increasing.
TEST_F(WlrRecorderTest, SingleThreadMultipleConnections) {
  ASSERT_EQ(startRecorder(1), 0);
  constexpr uint64_t kConns = 4;
  std::vector<std::vector<uint64_t>> perConn(kConns);
  std::vector<uint64_t>              global;

  for (int round = 0; round < 16; ++round) {
    for (uint64_t c = 0; c < kConns; ++c) {
      SWorkloadCallToken token;
      std::memset(&token, 0, sizeof(token));
      SWorkloadBeginArgs args;
      std::memset(&args, 0, sizeof(args));
      args.apiType = WLR_API_QUERY;
      args.logicalConnId = c + 1;
      args.sql = "select 1";
      args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
      ASSERT_EQ(wlrApiBegin(&args, &token), 0);
      uint64_t s = wlrTokenEventSeq(&token);
      perConn[c].push_back(s);
      global.push_back(s);
      SWorkloadReturnArgs ret;
      std::memset(&ret, 0, sizeof(ret));
      wlrApiReturn(&token, &ret);
    }
  }

  for (uint64_t c = 0; c < kConns; ++c) {
    for (size_t i = 1; i < perConn[c].size(); ++i) {
      EXPECT_GT(perConn[c][i], perConn[c][i - 1]) << "conn " << c << " causal order broken";
    }
  }
  for (size_t i = 1; i < global.size(); ++i) {
    EXPECT_GT(global[i], global[i - 1]) << "global eventSeq not strictly increasing";
  }
}

// TS ORD005 -- cross-thread sequences are unique with no duplicates.
TEST_F(WlrRecorderTest, CrossThreadSequenceIsUnique) {
  ASSERT_EQ(startRecorder(1), 0);
  constexpr int kThreads = 8;
  constexpr int kPerThread = 200;

  std::vector<std::vector<uint64_t>> got(kThreads);
  std::atomic<int> beginFailures{0};
  std::vector<std::thread>           ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t] {
      for (int i = 0; i < kPerThread; ++i) {
        SWorkloadCallToken token;
        std::memset(&token, 0, sizeof(token));
        SWorkloadBeginArgs args;
        std::memset(&args, 0, sizeof(args));
        args.apiType = WLR_API_QUERY;
        args.logicalConnId = static_cast<uint64_t>(t + 1);
        args.sql = "select 1";
        args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
        if (wlrApiBegin(&args, &token) != 0) {
          beginFailures.fetch_add(1);
          continue;
        }
        got[t].push_back(wlrTokenEventSeq(&token));
        SWorkloadReturnArgs ret;
        std::memset(&ret, 0, sizeof(ret));
        wlrApiReturn(&token, &ret);
      }
    });
  }
  for (auto& th : ts) th.join();

  std::vector<uint64_t> all;
  for (auto& v : got) {
    // Per-thread order must be preserved even though threads interleave.
    for (size_t i = 1; i < v.size(); ++i) {
      EXPECT_GT(v[i], v[i - 1]) << "per-thread order violated";
    }
    all.insert(all.end(), v.begin(), v.end());
  }
  EXPECT_EQ(beginFailures.load(), 0) << "recorder rejected a token during ordering test";
  std::sort(all.begin(), all.end());
  EXPECT_EQ(std::adjacent_find(all.begin(), all.end()), all.end())
      << "duplicate eventSeq assigned across threads";
}

// TS ORD015 -- sync and async interleaved: the async three-phase sequence
// (BEGIN, API_RETURN, REQUEST_DONE) is ordered correctly against sync calls.
TEST_F(WlrRecorderTest, SyncAsyncMixedPhases) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken asyncTok;
  std::memset(&asyncTok, 0, sizeof(asyncTok));
  SWorkloadBeginArgs aArgs;
  std::memset(&aArgs, 0, sizeof(aArgs));
  aArgs.apiType = WLR_API_QUERY_A;
  aArgs.logicalConnId = 1;
  aArgs.sql = "select async";
  aArgs.sqlLen = 12;
  ASSERT_EQ(wlrApiBegin(&aArgs, &asyncTok), 0);
  uint64_t beginSeq = wlrTokenEventSeq(&asyncTok);

  // The async API returns to the user before completion.
  SWorkloadReturnArgs aRet;
  std::memset(&aRet, 0, sizeof(aRet));
  wlrApiReturn(&asyncTok, &aRet);

  // A sync call happens while the async request is still outstanding.
  ASSERT_EQ(recordCall(1, "select sync"), 0);

  // Completion arrives last, on a driver thread.
  SWorkloadDoneArgs done;
  std::memset(&done, 0, sizeof(done));
  done.code = 0;
  wlrAsyncDone(&asyncTok, &done);

  EXPECT_GT(wlrTestLastEventSeq(), beginSeq)
      << "REQUEST_DONE must be sequenced after the originating BEGIN";
}

// TS ORD016 -- concurrent STMT handles on the recording side do not interleave
// each other's state machine events.
TEST_F(WlrRecorderTest, ConcurrentStmtHandlesDoNotCrossTalk) {
  ASSERT_EQ(startRecorder(1), 0);
  constexpr int kStmts = 4;

  std::vector<std::thread> ts;
  std::atomic<int>         failures{0};
  for (int s = 0; s < kStmts; ++s) {
    ts.emplace_back([&, s] {
      uint64_t stmtId = static_cast<uint64_t>(s + 1);
      for (int round = 0; round < 25; ++round) {
        for (int32_t api : {(int32_t)WLR_API_STMT_PREPARE, (int32_t)WLR_API_STMT_BIND_PARAM,
                            (int32_t)WLR_API_STMT_EXECUTE}) {
          SWorkloadCallToken token;
          std::memset(&token, 0, sizeof(token));
          SWorkloadBeginArgs args;
          std::memset(&args, 0, sizeof(args));
          args.apiType = api;
          args.logicalConnId = 1;
          args.logicalStmtId = stmtId;
          if (wlrApiBegin(&args, &token) != 0) {
            failures.fetch_add(1);
            continue;
          }
          SWorkloadReturnArgs ret;
          std::memset(&ret, 0, sizeof(ret));
          wlrApiReturn(&token, &ret);
        }
      }
    });
  }
  for (auto& th : ts) th.join();
  EXPECT_EQ(failures.load(), 0) << "concurrent STMT recording produced begin failures";
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_RUNNING);
}

// TS ORD008/ORD009 -- the smallest buffer forces backpressure but never drops.
TEST_F(WlrRecorderTest, BackpressureBlocksButNeverDrops) {
  ASSERT_EQ(startRecorder(1, /*bufferMb=*/16), 0);

  const std::string big(64 * 1024, 'x');
  constexpr int     kCalls = 2000;
  int               ok = 0;
  int32_t           failureCode = 0;
  for (int i = 0; i < kCalls; ++i) {
    SWorkloadCallToken token;
    std::memset(&token, 0, sizeof(token));
    SWorkloadBeginArgs args;
    std::memset(&args, 0, sizeof(args));
    args.apiType = WLR_API_QUERY;
    args.logicalConnId = 1;
    args.sql = big.c_str();
    args.sqlLen = static_cast<int32_t>(big.size());
    failureCode = wlrApiBegin(&args, &token);
    if (failureCode != 0) break;
    ++ok;
    SWorkloadReturnArgs ret;
    std::memset(&ret, 0, sizeof(ret));
    wlrApiReturn(&token, &ret);
  }

  EXPECT_EQ(ok, kCalls) << "recording stopped at call " << ok << " with code " << failureCode;
  EXPECT_EQ(wlrTestDroppedEventCount(), 0u)
      << "DS 6.13: there is no drop branch under normal storage conditions";
  // Deliberately NOT asserting that backpressure occurred: whether a producer
  // ever waits depends on whether the writer keeps up, which varies with disk
  // speed. The contract is that no event is lost, not that blocking happens.
  int64_t backpressureNs = wlrTestBackpressureNs();
  if (backpressureNs > 0) GTEST_LOG_(INFO) << "diagnostic backpressure: " << backpressureNs << " ns";
}

// TS ORD013 -- a producer waiting for pool space must be woken by fail-open.
TEST_F(WlrRecorderTest, BackpressureWaiterWokenByFailOpen) {
  ASSERT_EQ(startRecorder(1, /*bufferMb=*/16), 0);
  ASSERT_EQ(wlrTestStallWriter(), 0) << "test hook required to stall the writer";

  std::atomic<int>  published{0};
  std::atomic<bool> started{false};
  std::atomic<bool> finished{false};
  std::mutex progressMutex;
  std::condition_variable progressCv;
  std::thread       producer([&] {
    started.store(true);
    progressCv.notify_all();
    for (int i = 0; i < 32; ++i) {
      SWlrSlot *slot = nullptr;
      uint8_t *payload = nullptr;
      if (wlrReserve(&wlrMgr, WLR_MAX_FRAME_PAYLOAD, WLR_FRAME_EVENT, 0, &slot, &payload) != 0) break;
      std::memset(payload, 0, WLR_MAX_FRAME_PAYLOAD);
      (void)wlrPublish(&wlrMgr, slot, /*waitAck=*/false);
      published.fetch_add(1);
      progressCv.notify_all();
    }
    finished.store(true);
    progressCv.notify_all();
  });

  std::unique_lock<std::mutex> progressLock(progressMutex);
  progressCv.wait_for(progressLock, std::chrono::seconds(5), [&] {
    return started.load() && (published.load() > 0 || finished.load());
  });
  EXPECT_TRUE(started.load()) << "producer has not entered admission";
  progressLock.unlock();
  for (int i = 0; i < 500 && !finished.load() && wlrTestAdmissionWaiters() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_FALSE(finished.load()) << "producer exited before backpressure could block admission";
  EXPECT_GT(wlrTestAdmissionWaiters(), 0) << "producer never entered the backpressure wait";
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_RUNNING);
  EXPECT_EQ(wlrTestInjectWriterFailure(), 0) << "test hook required to fail the writer";
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_FAILED_OPEN);

  for (int i = 0; i < 100 && !finished.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const bool releasedByFailOpen = finished.load();
  if (!finished.load()) {
    EXPECT_EQ(wlrTestResumeWriter(), 0);
    /* Emergency cleanup is deliberately independent of the fail-open path under
     * test: explicitly wake every queue waiter so a missing production broadcast
     * reports an assertion failure instead of stranding producer.join(). */
    wlrQueueWakeAll(&wlrMgr);
    for (int i = 0; i < 100 && !finished.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  EXPECT_TRUE(releasedByFailOpen) << "DS 6.13: fail-open did not release the blocked producer";
  producer.join();
  EXPECT_GT(wlrTestBackpressureNs(), 0) << "the producer never entered the backpressure wait";
}

// ====================================================== SESSION_STOP / FATAL

// A BEGIN with no RETURN must be reported as unfinished at stop time. That is the
// signal a hung query leaves behind, and it must not be mistaken for corruption
// (DS 6.4, FS 4.7).
TEST_F(WlrRecorderTest, UnfinishedCallIsTrackedAtStop) {
  ASSERT_EQ(startRecorder(1), 0);

  // Begin without ever calling wlrApiReturn -- exactly what a hung API leaves.
  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select never_returns";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);

  EXPECT_EQ(wlrTestActiveCallCount(), 1) << "an in-flight call must be tracked";

  // Stopping must not block on it: a hung call holds a Call ID forever, so waiting
  // would hang process exit (DS 6.4).
  wlrRecorderClose();
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
  EXPECT_TRUE(sessionStopHasActiveCallCount(dir_, 1));
}

// A completed call must leave the active list, or every session would look like it
// ended with everything outstanding.
TEST_F(WlrRecorderTest, CompletedCallLeavesActiveList) {
  ASSERT_EQ(startRecorder(1), 0);
  EXPECT_EQ(wlrTestActiveCallCount(), 0);

  ASSERT_EQ(recordCall(1, "select 1"), 0);
  EXPECT_EQ(wlrTestActiveCallCount(), 0) << "a returned call must be removed from the list";

  // And many calls in sequence must not accumulate.
  for (int i = 0; i < 50; ++i) {
    ASSERT_EQ(recordCall(1, "select 2"), 0);
  }
  EXPECT_EQ(wlrTestActiveCallCount(), 0);
}

// Interleaved begins and returns across threads must balance out.
TEST_F(WlrRecorderTest, ActiveListBalancesUnderConcurrency) {
  ASSERT_EQ(startRecorder(1), 0);

  constexpr int kThreads = 6;
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t] {
      for (int i = 0; i < 100; ++i) {
        (void)recordCall(static_cast<uint64_t>(t + 1), "select 1");
      }
    });
  }
  for (auto &th : ts) th.join();

  EXPECT_EQ(wlrTestActiveCallCount(), 0) << "begins and returns must balance";
}

// ================================================================== manifest

namespace {

std::string readWholeFile(const std::string &path) {
  int64_t size = 0;
  if (taosStatFile(path.c_str(), &size, nullptr, nullptr) != 0 || size <= 0) return std::string();
  std::string out;
  try {
    out.resize(static_cast<size_t>(size));
  } catch (const std::bad_alloc &) {
    return std::string();
  } catch (const std::length_error &) {
    return std::string();
  }
  TdFilePtr f = taosOpenFile(path.c_str(), TD_FILE_READ);
  if (f == nullptr) return std::string();
  int64_t got = taosReadFile(f, &out[0], size);
  (void)taosCloseFile(&f);
  if (got != size) return std::string();
  return out;
}

// Find the manifest in a record dir without hardcoding the generated name.
std::string findManifest(const std::string &dir) {
  TdDirPtr d = taosOpenDir(dir.c_str());
  if (d == nullptr) return std::string();
  std::string found;
  TdDirEntryPtr e;
  while ((e = taosReadDir(d)) != nullptr) {
    const char *name = taosGetDirEntryName(e);
    if (name == nullptr) continue;
    std::string manifestName(name);
    const std::string suffix = WLR_MANIFEST_SUFFIX;
    if (manifestName.size() >= suffix.size() &&
        manifestName.compare(manifestName.size() - suffix.size(), suffix.size(), suffix) == 0) {
      found = dir + TD_DIRSEP + name;
      break;
    }
  }
  (void)taosCloseDir(&d);
  return found;
}

}  // namespace

// A clean stop must report all four completeness flags truthfully (DS 6.15.1).
TEST_F(WlrRecorderTest, ManifestReportsCleanSession) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select 1"), 0);
  wlrRecorderClose();

  std::string path = findManifest(dir_);
  ASSERT_FALSE(path.empty()) << "no manifest was written";
  std::string js = readWholeFile(path);
  ASSERT_FALSE(js.empty());

  EXPECT_TRUE(jsonHasBool(js, "structurallyComplete", true)) << js;
  EXPECT_TRUE(jsonHasBool(js, "captureComplete", true)) << js;
  EXPECT_TRUE(jsonHasBool(js, "gracefullyClosed", true)) << js;
  EXPECT_TRUE(jsonHasBool(js, "hasUnfinishedCalls", false)) << js;

  // Check credential fields, not credential-like values that may legitimately occur in
  // the recordDir path or as the default username.
  EXPECT_EQ(js.find("\"password\""), std::string::npos) << js;
  EXPECT_EQ(js.find("\"secret\""), std::string::npos) << js;
  EXPECT_EQ(js.find("\"credential\""), std::string::npos) << js;
}

// After fail-open, captureComplete must be false and gracefullyClosed must not be
// claimed: events are known to be missing, and replay needs --allow-incomplete.
TEST_F(WlrRecorderTest, ManifestReportsFailOpen) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select 1"), 0);
  ASSERT_EQ(wlrTestInjectWriterFailure(), 0);
  ASSERT_EQ(wlrRecorderState(), WLR_STATE_FAILED_OPEN);
  wlrRecorderClose();

  std::string path = findManifest(dir_);
  ASSERT_FALSE(path.empty()) << "no manifest was written after fail-open";
  std::string js = readWholeFile(path);
  ASSERT_FALSE(js.empty());

  EXPECT_TRUE(jsonHasBool(js, "captureComplete", false))
      << "fail-open must mark the capture incomplete: " << js;
  EXPECT_TRUE(jsonHasBool(js, "gracefullyClosed", false))
      << "a failed session must not claim a graceful close: " << js;
}

TEST_F(WlrRecorderTest, ManifestRejectsGracefulCloseAfterFsyncFailure) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select 1"), 0);
  ASSERT_EQ(wlrTestInjectFsyncFailure(), 0);
  ASSERT_EQ(wlrRecorderState(), WLR_STATE_RUNNING);
  wlrRecorderClose();

  std::string path = findManifest(dir_);
  ASSERT_FALSE(path.empty()) << "no manifest was written after fsync failure";
  std::string js = readWholeFile(path);
  ASSERT_FALSE(js.empty());
  EXPECT_TRUE(jsonHasBool(js, "captureComplete", false)) << js;
  EXPECT_TRUE(jsonHasBool(js, "gracefullyClosed", false)) << js;
}

TEST_F(WlrRecorderTest, ManifestReportsIncompleteDrain) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(wlrTestStallWriter(), 0);

  SWlrSlot *slot = nullptr;
  uint8_t *payload = nullptr;
  ASSERT_EQ(wlrReserve(&wlrMgr, 1, WLR_FRAME_EVENT, 0, &slot, &payload), 0);
  payload[0] = 0;
  ASSERT_EQ(wlrPublish(&wlrMgr, slot, /*waitAck=*/false), 0);

  wlrRecorderClose();

  std::string path = findManifest(dir_);
  ASSERT_FALSE(path.empty()) << "no manifest was written after an incomplete drain";
  std::string js = readWholeFile(path);
  ASSERT_FALSE(js.empty());
  EXPECT_TRUE(jsonHasBool(js, "captureComplete", false)) << js;
  EXPECT_TRUE(jsonHasBool(js, "gracefullyClosed", false)) << js;
  EXPECT_FALSE(hasCleanSessionStop(dir_))
      << "an incomplete drain must not write a clean SESSION_STOP marker";
}

TEST_F(WlrRecorderTest, CleanupDefersBlockedTerminalFrame) {
  checkBlockedFinalIo(WLR_TEST_FINAL_FRAME);
}

TEST_F(WlrRecorderTest, CleanupDefersBlockedFinalFlush) {
  checkBlockedFinalIo(WLR_TEST_FINAL_FLUSH);
}

TEST_F(WlrRecorderTest, CleanupDefersBlockedFinalClose) {
  checkBlockedFinalIo(WLR_TEST_FINAL_CLOSE);
}

TEST_F(WlrRecorderTest, CleanupDefersBlockedFinalManifest) {
  checkBlockedFinalIo(WLR_TEST_FINAL_MANIFEST);
}

TEST_F(WlrRecorderTest, CleanupDefersBlockedWrite) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(wlrTestStallWrite(), 0);

  SWlrSlot *slot = nullptr;
  uint8_t *payload = nullptr;
  ASSERT_EQ(wlrReserve(&wlrMgr, 1, WLR_FRAME_EVENT, 0, &slot, &payload), 0);
  payload[0] = 0;
  ASSERT_EQ(wlrPublish(&wlrMgr, slot, false), 0);
  for (int attempt = 0; attempt < 500 && !wlrTestWriteStalled(); ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_EQ(wlrTestWriteStalled(), 1);

  auto start = std::chrono::steady_clock::now();
  wlrRecorderClose();
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, std::chrono::seconds(2));
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_STOPPING);
  EXPECT_EQ(wlrMgr.captureIncomplete, 1);

  ASSERT_EQ(wlrTestResumeWrite(), 0);
  for (int attempt = 0; attempt < 500 && wlrRecorderState() != WLR_STATE_OFF; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_EQ(wlrRecorderState(), WLR_STATE_OFF);
}

// An unfinished call is reported but is NOT a defect: replay accepts such a session
// without --allow-incomplete (DS 6.15.1).
TEST_F(WlrRecorderTest, ManifestSeparatesUnfinishedFromIncomplete) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select never_returns";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);

  wlrRecorderClose();

  std::string js = readWholeFile(findManifest(dir_));
  ASSERT_FALSE(js.empty());
  EXPECT_TRUE(jsonHasBool(js, "hasUnfinishedCalls", true)) << js;
  // The key distinction: unfinished does not imply incomplete.
  EXPECT_TRUE(jsonHasBool(js, "captureComplete", true))
      << "an unfinished call must not mark the capture incomplete: " << js;
}

// ================================================================ RE014-RE016

// TS RE016 -- depth returns to zero even when the inner path errors out early.
TEST_F(WlrRecorderTest, TlsDepthRestoredOnErrorPath) {
  ASSERT_EQ(startRecorder(1), 0);
  EXPECT_EQ(wlrTestCurrentDepth(), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select 1";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  EXPECT_EQ(wlrTestCurrentDepth(), 1);

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  ret.code = -1;  // the wrapped call failed
  wlrApiReturn(&token, &ret);
  EXPECT_EQ(wlrTestCurrentDepth(), 0) << "depth must unwind on the error path too";

  // A subsequent call on the same thread must still be recorded.
  EXPECT_EQ(recordCall(1, "select 2"), 0);
}

TEST_F(WlrRecorderTest, InternalScopeSuppressesCleanupButRestoresCapture) {
  ASSERT_EQ(startRecorder(1), 0);

  int32_t previousDepth = wlrInternalBegin();
  EXPECT_EQ(wlrTestCurrentDepth(), 1);
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_FREE_RESULT;
  args.logicalResultId = 1;
  SWorkloadCallToken token = {};
  EXPECT_EQ(wlrApiBegin(&args, &token), 0);
  EXPECT_FALSE(token.recorded);
  wlrApiReturn(&token, nullptr);
  wlrInternalEnd(previousDepth);

  EXPECT_EQ(wlrTestCurrentDepth(), 0);
  EXPECT_EQ(recordCall(1, "select 1"), 0);
}

// TS RE014 -- depth is per thread, so a driver thread sits at 0. This is why
// TLS depth alone cannot suppress the internal fetch in postSubQueryFetchCb:
// that path must be split into an internal impl instead.
// This test pins the property that makes the split mandatory.
TEST_F(WlrRecorderTest, DepthIsPerThreadSoDriverThreadSeesZero) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = "select 1";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_EQ(wlrTestCurrentDepth(), 1);

  int32_t observed = -1;
  std::thread other([&] { observed = wlrTestCurrentDepth(); });
  other.join();

  EXPECT_EQ(observed, 0)
      << "API nesting depth must be thread-local";

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  wlrApiReturn(&token, &ret);
}

// TS RE015/API027b -- callback-parent state can be explicitly cleared on a
// reused driver thread. The actual driver callback wrapper is integration-only.
TEST_F(WlrRecorderTest, CallbackParentSlotExplicitClear) {
  ASSERT_EQ(startRecorder(1), 0);

  wlrSetCallbackParent(4242);
  EXPECT_EQ(wlrGetCallbackParent(), 4242u);
  wlrSetCallbackParent(0);
  EXPECT_EQ(wlrGetCallbackParent(), 0u)
      << "callback-parent state must support explicit clearing";
}

// ------------------------------------------- async token handoff (REQUEST_DONE)

// TS API027 -- the whole token reaches request creation, not just the Call ID.
// REQUEST_DONE carries apiType and a duration measured from beginMonoNs, so a
// handoff that dropped either would emit a DONE with no api identity and a
// duration measured from zero.
TEST_F(WlrRecorderTest, PendingAsyncCallCarriesWholeToken) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY_A;
  args.logicalConnId = 1;
  args.sql = "select handoff";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  ASSERT_NE(token.beginMonoNs, 0);

  wlrSetPendingAsyncCall(&token);

  uint64_t callId = 0;
  int64_t  beginMonoNs = 0;
  uint16_t apiType = 0;
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);

  EXPECT_EQ(callId, token.callId);
  EXPECT_EQ(beginMonoNs, token.beginMonoNs)
      << "DS 6.8: without beginMonoNs the DONE event's duration is measured from zero";
  EXPECT_EQ(apiType, static_cast<uint16_t>(WLR_API_QUERY_A))
      << "DS 6.8: the DONE event must carry the originating API's identity";

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  wlrApiReturn(&token, &ret);
}

TEST_F(WlrRecorderTest, SynchronousAsyncFailureCallbackCanRecordChildCall) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_QUERY_A;
  args.logicalConnId = 1;
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  wlrSetPendingAsyncCall(&token);

  AsyncFailureCallbackProbe probe = {};
  taosAsyncQueryImpl(0, nullptr, asyncFailureCallbackProbe, &probe, false, TD_REQ_FROM_APP, true);

  EXPECT_EQ(probe.callbackDepth, 0);
  EXPECT_EQ(probe.parentCallId, token.callId);
  EXPECT_TRUE(probe.childRecorded)
      << "a public API issued by a synchronous failure callback was suppressed as nested internals";

  SWorkloadReturnArgs ret = {};
  ret.code = TSDB_CODE_INVALID_PARA;
  wlrApiReturn(&token, &ret);
}

TEST_F(WlrRecorderTest, VariableLengthSingleColumnBindUsesBufferStride) {
  char values[3][8] = {{'a'}, {'b', 'c'}, {'x', 'x', 'x'}};
  int32_t lengths[3] = {1, 2, 3};
  char isNull[3] = {0, 0, 1};
  TAOS_MULTI_BIND bind = {};
  bind.buffer_type = TSDB_DATA_TYPE_BINARY;
  bind.buffer = values;
  bind.buffer_length = sizeof(values[0]);
  bind.length = lengths;
  bind.is_null = isNull;
  bind.num = 3;

  uint8_t encoded[256] = {};
  int32_t encodedLen = wlrTestEncodeSingleMultiBind(&bind, encoded, sizeof(encoded));
  ASSERT_GT(encodedLen, 0);

  SWlrBindCol decoded = {};
  int32_t columnCount = 0;
  ASSERT_EQ(wlrDecodeBind(encoded, static_cast<uint32_t>(encodedLen), &decoded, 1, &columnCount),
            WLR_DECODE_OK);
  ASSERT_EQ(columnCount, 1);
  ASSERT_EQ(decoded.valueBytes, 3u);
  EXPECT_EQ(std::memcmp(decoded.values, "abc", 3), 0)
      << "variable-length values must be packed from each buffer_length stride";
}

TEST_F(WlrRecorderTest, SingleColumnBindExceedsOldCaptureCapacity) {
  std::vector<int64_t> timestamps(1000, 1700000000000);
  TAOS_MULTI_BIND bind = {};
  bind.buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  bind.buffer = timestamps.data();
  bind.num = static_cast<int32_t>(timestamps.size());
  uint32_t size = wlrTestSingleMultiBindSize(&bind);
  ASSERT_GT(size, 8192u);
  ASSERT_LT(size, WLR_MAX_FRAME_PAYLOAD);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrTestEncodeSingleMultiBind(&bind, encoded.data(), encoded.size()), size);
}

TEST_F(WlrRecorderTest, FixedWidthSingleColumnBindWithoutBufferRecordsNulls) {
  char isNull[2] = {1, 2};
  TAOS_MULTI_BIND bind = {};
  bind.buffer_type = TSDB_DATA_TYPE_INT;
  bind.is_null = isNull;
  bind.num = 2;

  uint8_t encoded[256] = {};
  int32_t encodedLen = wlrTestEncodeSingleMultiBind(&bind, encoded, sizeof(encoded));
  ASSERT_GT(encodedLen, 0);

  SWlrBindCol decoded = {};
  int32_t columnCount = 0;
  ASSERT_EQ(wlrDecodeBind(encoded, static_cast<uint32_t>(encodedLen), &decoded, 1, &columnCount),
            WLR_DECODE_OK);
  ASSERT_EQ(columnCount, 1);
  ASSERT_EQ(decoded.valueBytes, 2u * sizeof(int32_t));
  EXPECT_EQ(decoded.isNull[0], 1);
  EXPECT_EQ(decoded.isNull[1], 1);
  for (uint32_t offset = 0; offset < decoded.valueBytes; ++offset) {
    EXPECT_EQ(decoded.values[offset], 0);
  }

  isNull[1] = 0;
  EXPECT_EQ(wlrTestEncodeSingleMultiBind(&bind, encoded, sizeof(encoded)), 0);
}

TEST_F(WlrRecorderTest, VariableLengthSingleColumnBindWithoutLengthsRecordsNulls) {
  char isNull[2] = {1, 1};
  TAOS_MULTI_BIND bind = {};
  bind.buffer_type = TSDB_DATA_TYPE_VARCHAR;
  bind.is_null = isNull;
  bind.num = 2;

  uint8_t encoded[256] = {};
  int32_t encodedLen = wlrTestEncodeSingleMultiBind(&bind, encoded, sizeof(encoded));
  ASSERT_GT(encodedLen, 0);

  SWlrBindCol decoded = {};
  int32_t columnCount = 0;
  ASSERT_EQ(wlrDecodeBind(encoded, static_cast<uint32_t>(encodedLen), &decoded, 1, &columnCount),
            WLR_DECODE_OK);
  ASSERT_EQ(columnCount, 1);
  EXPECT_EQ(decoded.valueBytes, 0u);
  EXPECT_EQ(decoded.isNull[0], 1);
  EXPECT_EQ(decoded.isNull[1], 1);
  EXPECT_EQ(wlrBindColLength(&decoded, 0), 0u);
  EXPECT_EQ(wlrBindColLength(&decoded, 1), 0u);

  isNull[1] = 0;
  EXPECT_EQ(wlrTestEncodeSingleMultiBind(&bind, encoded, sizeof(encoded)), 0);
}

TEST_F(WlrRecorderTest, VariableLengthColumnBindSkipsNullStorage) {
  char values[] = {'a', 'b', 'c'};
  int32_t lengths[3] = {1, 2, 2};
  char isNull[3] = {0, 1, 0};
  TAOS_STMT2_COLUMN_BIND column = {};
  column.buffer_type = TSDB_DATA_TYPE_BINARY;
  column.buffer = values;
  column.length = lengths;
  column.is_null = isNull;
  TAOS_STMT2_COLUMN_BINDV bindv = {};
  bindv.num_columns = 1;
  bindv.num_rows = 3;
  bindv.columns = &column;

  for (char nullKind : {1, 2}) {
    isNull[1] = nullKind;
    uint8_t encoded[256] = {};
    int32_t encodedLen = wlrTestEncodeStmt2ColumnBindv(&bindv, encoded, sizeof(encoded));
    ASSERT_GT(encodedLen, 0);

    SWlrBindCol decoded = {};
    int32_t columnCount = 0;
    ASSERT_EQ(wlrDecodeBind(encoded, static_cast<uint32_t>(encodedLen), &decoded, 1, &columnCount),
              WLR_DECODE_OK);
    ASSERT_EQ(columnCount, 1);
    ASSERT_EQ(decoded.valueBytes, 3u);
    EXPECT_EQ(wlrBindColLength(&decoded, 0), 1u);
    EXPECT_EQ(wlrBindColLength(&decoded, 1), 0u);
    EXPECT_EQ(wlrBindColLength(&decoded, 2), 2u);
    EXPECT_EQ(std::memcmp(decoded.values, "abc", 3), 0)
        << "NULL/NONE rows must not advance the source buffer";
  }
}

TEST_F(WlrRecorderTest, ColumnBindExceedsOldCaptureCapacity) {
  std::vector<int64_t> timestamps(2000, 1700000000000);
  TAOS_STMT2_COLUMN_BIND column = {};
  column.buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  column.buffer = timestamps.data();
  TAOS_STMT2_COLUMN_BINDV bindv = {};
  bindv.num_columns = 1;
  bindv.num_rows = static_cast<int32_t>(timestamps.size());
  bindv.columns = &column;
  uint32_t size = wlrTestStmt2ColumnBindvSize(&bindv);
  ASSERT_GT(size, 16384u);
  ASSERT_LT(size, WLR_MAX_FRAME_PAYLOAD);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrTestEncodeStmt2ColumnBindv(&bindv, encoded.data(), encoded.size()), size);
}

TEST_F(WlrRecorderTest, FixedWidthColumnBindWithoutBufferRecordsNulls) {
  int64_t timestamps[2] = {1, 2};
  char isNull[2] = {1, 2};
  TAOS_STMT2_COLUMN_BIND columns[2] = {};
  columns[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  columns[0].buffer = timestamps;
  columns[1].buffer_type = TSDB_DATA_TYPE_INT;
  columns[1].is_null = isNull;
  TAOS_STMT2_COLUMN_BINDV bindv = {};
  bindv.num_columns = 2;
  bindv.num_rows = 2;
  bindv.columns = columns;

  uint8_t encoded[256] = {};
  int32_t encodedLen = wlrTestEncodeStmt2ColumnBindv(&bindv, encoded, sizeof(encoded));
  ASSERT_GT(encodedLen, 0);

  SWlrBindCol decoded[2] = {};
  int32_t columnCount = 0;
  ASSERT_EQ(wlrDecodeBind(encoded, static_cast<uint32_t>(encodedLen), decoded, 2, &columnCount),
            WLR_DECODE_OK);
  ASSERT_EQ(columnCount, 2);
  ASSERT_EQ(decoded[1].valueBytes, 2u * sizeof(int32_t));
  EXPECT_EQ(decoded[1].isNull[0], 1);
  EXPECT_EQ(decoded[1].isNull[1], 2);
  for (uint32_t offset = 0; offset < decoded[1].valueBytes; ++offset) {
    EXPECT_EQ(decoded[1].values[offset], 0);
  }
}

TEST_F(WlrRecorderTest, RowBindRecordsAllNullColumnsAndTag) {
  int64_t timestamps[2] = {1700000000000, 1700000001000};
  char intNulls[2] = {1, 1};
  char textNulls[2] = {1, 1};
  TAOS_STMT2_BIND columns[3] = {};
  columns[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  columns[0].buffer = timestamps;
  columns[0].num = 2;
  columns[1].buffer_type = TSDB_DATA_TYPE_INT;
  columns[1].is_null = intNulls;
  columns[1].num = 2;
  columns[2].buffer_type = TSDB_DATA_TYPE_BINARY;
  columns[2].is_null = textNulls;
  columns[2].num = 2;
  TAOS_STMT2_BIND *tableColumns[] = {columns};
  TAOS_STMT2_BINDV bindv = {};
  bindv.count = 1;
  bindv.bind_cols = tableColumns;

  uint32_t size = wlrTestEncodeStmt2RowBindv(&bindv, 3, NULL, 0);
  ASSERT_GT(size, 0u);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrTestEncodeStmt2RowBindv(&bindv, 3, encoded.data(), size), size);
  SWlrBindCol decoded[3] = {};
  int32_t decodedCount = 0;
  ASSERT_EQ(wlrDecodeBind(encoded.data(), size, decoded, 3, &decodedCount), WLR_DECODE_OK);
  ASSERT_EQ(decodedCount, 3);
  EXPECT_EQ(decoded[1].valueBytes, 2u * sizeof(int32_t));
  EXPECT_EQ(decoded[1].isNull[0], 1);
  EXPECT_EQ(decoded[1].isNull[1], 1);
  EXPECT_EQ(decoded[2].valueBytes, 0u);
  EXPECT_EQ(decoded[2].isNull[0], 1);
  EXPECT_EQ(decoded[2].isNull[1], 1);

  TAOS_STMT2_BIND tag = {};
  char tagNull = 1;
  tag.buffer_type = TSDB_DATA_TYPE_INT;
  tag.is_null = &tagNull;
  tag.num = 1;
  TAOS_STMT2_BIND *tagRows[] = {&tag};
  bindv.tags = tagRows;
  uint32_t tagSize = wlrTestEncodeStmt2Tags(&bindv, 1, NULL, 0);
  ASSERT_GT(tagSize, 8u);
  std::vector<uint8_t> encodedTags(tagSize);
  ASSERT_EQ(wlrTestEncodeStmt2Tags(&bindv, 1, encodedTags.data(), tagSize), tagSize);
  SWlrBindCol decodedTag = {};
  ASSERT_EQ(wlrDecodeBind(encodedTags.data() + 8, tagSize - 8, &decodedTag, 1, &decodedCount), WLR_DECODE_OK);
  ASSERT_EQ(decodedCount, 1);
  EXPECT_EQ(decodedTag.isNull[0], 1);
}

TEST_F(WlrRecorderTest, RowBindRecordsUntypedSingleNull) {
  char nullValue = 1;
  TAOS_STMT2_BIND column = {};
  column.is_null = &nullValue;
  column.num = 1;
  TAOS_STMT2_BIND *rows[] = {&column};
  TAOS_STMT2_BINDV bindv = {};
  bindv.count = 1;
  bindv.bind_cols = rows;

  uint32_t size = wlrTestEncodeStmt2RowBindv(&bindv, 1, NULL, 0);
  ASSERT_GT(size, 0u);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrTestEncodeStmt2RowBindv(&bindv, 1, encoded.data(), size), size);
  SWlrBindCol decoded = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_OK);
  EXPECT_EQ(count, 1);
  EXPECT_EQ(decoded.type, 0);
  EXPECT_EQ(decoded.elemCount, 1);
  EXPECT_EQ(decoded.isNull[0], 1);

  nullValue = 2;
  ASSERT_EQ(wlrTestEncodeStmt2RowBindv(&bindv, 1, NULL, 0), size);
  ASSERT_EQ(wlrTestEncodeStmt2RowBindv(&bindv, 1, encoded.data(), size), size);
  ASSERT_EQ(wlrDecodeBind(encoded.data(), size, &decoded, 1, &count), WLR_DECODE_OK);
  EXPECT_EQ(decoded.type, 0);
  EXPECT_EQ(decoded.elemCount, 1);
  EXPECT_EQ(decoded.isNull[0], 2);
  EXPECT_EQ(decoded.valueBytes, 0u);
}

TEST_F(WlrRecorderTest, RowBindPreservesMissingTableEntry) {
  int64_t timestamp = 1700000000000;
  int32_t value = 42;
  TAOS_STMT2_BIND columns[2] = {};
  columns[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  columns[0].buffer = &timestamp;
  columns[0].num = 1;
  columns[1].buffer_type = TSDB_DATA_TYPE_INT;
  columns[1].buffer = &value;
  columns[1].num = 1;
  TAOS_STMT2_BIND *rows[] = {NULL, columns};
  TAOS_STMT2_BINDV bindv = {};
  bindv.count = 2;
  bindv.bind_cols = rows;

  uint32_t size = wlrTestEncodeStmt2RowBindv(&bindv, 2, NULL, 0);
  ASSERT_GT(size, 0u);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrTestEncodeStmt2RowBindv(&bindv, 2, encoded.data(), size), size);
  SWlrBindCol decoded[4] = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeBind(encoded.data(), size, decoded, 4, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 4);
  EXPECT_EQ(decoded[0].flags & WLR_BIND_FLAG_ABSENT_TABLE, WLR_BIND_FLAG_ABSENT_TABLE);
  EXPECT_EQ(decoded[1].elemCount, 0);
  EXPECT_EQ(decoded[2].type, TSDB_DATA_TYPE_TIMESTAMP);
  EXPECT_EQ(decoded[3].type, TSDB_DATA_TYPE_INT);
}

TEST_F(WlrRecorderTest, ScalarTagsAllowZeroNum) {
  int32_t site = 7;
  char region[] = "north";
  int32_t regionLength = 5;
  TAOS_STMT2_BIND tags[2] = {};
  tags[0].buffer_type = TSDB_DATA_TYPE_INT;
  tags[0].buffer = &site;
  tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[1].buffer = region;
  tags[1].length = &regionLength;
  TAOS_STMT2_BIND *tagRows[] = {tags};
  TAOS_STMT2_BINDV bindv = {};
  bindv.count = 1;
  bindv.tags = tagRows;

  uint32_t size = wlrTestEncodeStmt2Tags(&bindv, 2, NULL, 0);
  ASSERT_GT(size, 8u);
  std::vector<uint8_t> encoded(size);
  ASSERT_EQ(wlrTestEncodeStmt2Tags(&bindv, 2, encoded.data(), size), size);
  SWlrBindCol decoded[2] = {};
  int32_t count = 0;
  ASSERT_EQ(wlrDecodeBind(encoded.data() + 8, size - 8, decoded, 2, &count), WLR_DECODE_OK);
  ASSERT_EQ(count, 2);
  EXPECT_EQ(decoded[0].elemCount, 1u);
  EXPECT_EQ(decoded[1].elemCount, 1u);
  EXPECT_EQ(wlrGetU32(decoded[0].values), 7u);
  EXPECT_EQ(decoded[1].valueBytes, 5u);
  EXPECT_EQ(std::memcmp(decoded[1].values, region, 5), 0);
  EXPECT_EQ(tags[0].num, 0);
  EXPECT_EQ(tags[1].num, 0);
}

TEST_F(WlrRecorderTest, AsyncStmt2ExecRecordsCompletionModeOnBegin) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_STMT2_EXEC;
  args.logicalStmtId = 1;
  args.stmt2Async = true;
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  uint8_t encoded[sizeof(uint64_t)] = {};
  wlrPutU64(encoded, 1);
  EXPECT_TRUE(firstBeginHasTagValue(dir_, WLR_API_STMT2_EXEC, WLR_TAG_STMT2_ASYNC,
                                    std::string(reinterpret_cast<const char *>(encoded), sizeof(encoded))));
}

TEST_F(WlrRecorderTest, StmtInitRecordsReplayableOptions) {
  ASSERT_EQ(startRecorder(1), 0);
  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_STMT_INIT_WITH_OPTIONS;
  args.logicalStmtId = 1;
  args.reqid = 42;
  args.stmtInitOptionsSet = true;
  args.stmtInitOptions = WLR_STMT_OPT_SINGLE_STB_INSERT | WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE;
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  uint8_t encoded[sizeof(uint64_t)] = {};
  wlrPutU64(encoded, 3);
  EXPECT_TRUE(firstBeginHasTagValue(dir_, args.apiType, WLR_TAG_STMT_INIT_OPTIONS,
                                    std::string(reinterpret_cast<const char *>(encoded), sizeof(encoded))));
  wlrPutU64(encoded, 42);
  EXPECT_TRUE(firstBeginHasTagValue(dir_, args.apiType, WLR_TAG_REQID,
                                    std::string(reinterpret_cast<const char *>(encoded), sizeof(encoded))));
}

TEST_F(WlrRecorderTest, Stmt2InitRecordsCallbackPresenceWithoutPointers) {
  ASSERT_EQ(startRecorder(1), 0);
  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_STMT2_INIT;
  args.logicalStmtId = 1;
  args.reqid = 43;
  args.stmtInitOptionsSet = true;
  args.stmtInitOptions = WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE | WLR_STMT_OPT_ASYNC_CALLBACK |
                         WLR_STMT_OPT_USERDATA;
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  uint8_t encoded[sizeof(uint64_t)] = {};
  wlrPutU64(encoded, 14);
  EXPECT_TRUE(firstBeginHasTagValue(dir_, args.apiType, WLR_TAG_STMT_INIT_OPTIONS,
                                    std::string(reinterpret_cast<const char *>(encoded), sizeof(encoded))));
  wlrPutU64(encoded, 43);
  EXPECT_TRUE(firstBeginHasTagValue(dir_, args.apiType, WLR_TAG_REQID,
                                    std::string(reinterpret_cast<const char *>(encoded), sizeof(encoded))));
}

// TS API027 -- taking consumes all three fields together. A request built on a
// path that was not recorded must not inherit a previous call's token, or its
// completion would emit a DONE naming an unrelated call.
TEST_F(WlrRecorderTest, PendingAsyncCallIsConsumedOnce) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY_A;
  args.logicalConnId = 1;
  args.sql = "select once";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);

  wlrSetPendingAsyncCall(&token);

  uint64_t callId = 0;
  int64_t  beginMonoNs = 0;
  uint16_t apiType = 0;
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  ASSERT_EQ(callId, token.callId);

  // Second take: the slot must be empty, not still holding the first call.
  callId = 0xdead;
  beginMonoNs = 0xdead;
  apiType = 0xbe;
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  EXPECT_EQ(callId, 0u) << "DS 6.8: a second request must not inherit the first call's ID";
  EXPECT_EQ(beginMonoNs, 0);
  EXPECT_EQ(apiType, 0);

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  wlrApiReturn(&token, &ret);
}

TEST_F(WlrRecorderTest, NestedPendingAsyncCallsPreserveOuterToken) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken outer = {};
  outer.recorded = true;
  outer.callId = 101;
  outer.beginMonoNs = 1001;
  outer.apiType = WLR_API_QUERY_A;
  SWorkloadCallToken inner = {};
  inner.recorded = true;
  inner.callId = 202;
  inner.beginMonoNs = 2002;
  inner.apiType = WLR_API_QUERY_A_WITH_REQID;

  wlrSetPendingAsyncCall(&outer);
  wlrSetPendingAsyncCall(&inner);

  uint64_t callId = 0;
  int64_t beginMonoNs = 0;
  uint16_t apiType = 0;
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  EXPECT_EQ(callId, inner.callId);
  EXPECT_EQ(beginMonoNs, inner.beginMonoNs);
  EXPECT_EQ(apiType, inner.apiType);
  wlrSetPendingAsyncCall(NULL);

  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  EXPECT_EQ(callId, outer.callId);
  EXPECT_EQ(beginMonoNs, outer.beginMonoNs);
  EXPECT_EQ(apiType, outer.apiType);
  wlrSetPendingAsyncCall(NULL);
}

// TS API027 -- an unrecorded call (or a driver early-return) must leave nothing
// pending. The public wrapper clears with NULL after the driver call precisely
// because the driver's error paths never reach the take.
TEST_F(WlrRecorderTest, PendingAsyncCallClearedByNullAndUnrecordedToken) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY_A;
  args.logicalConnId = 1;
  args.sql = "select cleared";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);

  uint64_t callId = 0xdead;
  int64_t  beginMonoNs = 0;
  uint16_t apiType = 0;

  wlrSetPendingAsyncCall(&token);
  wlrSetPendingAsyncCall(NULL); /* what the wrapper does after the driver returns */
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  EXPECT_EQ(callId, 0u) << "DS 6.8: NULL must clear the pending token, not preserve it";

  // An unrecorded nested call pushes an empty frame rather than overwriting the
  // outer recorded token. Its request sees no token, then paired cleanup exposes
  // the outer frame again.
  SWorkloadCallToken notRecorded;
  std::memset(&notRecorded, 0, sizeof(notRecorded));
  notRecorded.callId = 777;
  notRecorded.recorded = false;

  wlrSetPendingAsyncCall(&token);
  wlrSetPendingAsyncCall(&notRecorded);
  callId = 0xdead;
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  EXPECT_EQ(callId, 0u)
      << "DS 6.8: an unrecorded request must not inherit the outer call's token";
  wlrSetPendingAsyncCall(NULL);
  wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
  EXPECT_EQ(callId, token.callId)
      << "the outer pending token was not restored after clearing the inner unrecorded call";
  wlrSetPendingAsyncCall(NULL);

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  wlrApiReturn(&token, &ret);
}

// TS API027 -- out params are optional; callers that want only the ID must still
// consume the whole slot.
TEST_F(WlrRecorderTest, PendingAsyncCallToleratesNullOutParams) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY_A;
  args.logicalConnId = 1;
  args.sql = "select partial";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);

  wlrSetPendingAsyncCall(&token);
  wlrTakePendingAsyncCall(NULL, NULL, NULL); /* must not crash, must still consume */

  uint64_t callId = 0xdead;
  wlrTakePendingAsyncCall(&callId, NULL, NULL);
  EXPECT_EQ(callId, 0u) << "a take with NULL out params must still clear the slot";

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  wlrApiReturn(&token, &ret);
}

// TS API027 -- the pending slot is thread-local. Two threads issuing async calls
// concurrently must not see each other's token, which is why a per-connection
// slot was rejected: several async queries share one connection.
TEST_F(WlrRecorderTest, PendingAsyncCallIsThreadLocal) {
  ASSERT_EQ(startRecorder(1), 0);

  constexpr int    kThreads = 4;
  std::atomic<int> mismatches{0};

  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) {
    ts.emplace_back([&, t] {
      for (int round = 0; round < 50; ++round) {
        SWorkloadCallToken token;
        std::memset(&token, 0, sizeof(token));
        SWorkloadBeginArgs args;
        std::memset(&args, 0, sizeof(args));
        args.apiType = WLR_API_QUERY_A;
        args.logicalConnId = static_cast<uint64_t>(t + 1);
        args.sql = "select tls";
        args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
        if (wlrApiBegin(&args, &token) != 0 || !token.recorded) continue;

        wlrSetPendingAsyncCall(&token);

        uint64_t callId = 0;
        int64_t  beginMonoNs = 0;
        uint16_t apiType = 0;
        wlrTakePendingAsyncCall(&callId, &beginMonoNs, &apiType);
        if (callId != token.callId || beginMonoNs != token.beginMonoNs) {
          mismatches.fetch_add(1);
        }

        SWorkloadReturnArgs ret;
        std::memset(&ret, 0, sizeof(ret));
        wlrApiReturn(&token, &ret);
      }
    });
  }
  for (auto& th : ts) th.join();

  EXPECT_EQ(mismatches.load(), 0)
      << "DS 6.8: the pending slot must be thread-local, otherwise concurrent async calls on one "
         "connection would tie DONE events to each other's calls";
}

/*
 * A payload too large for one frame must still record the call's metadata.
 *
 * The oversize path shrinks its reservation to the fixed-tag budget. If it then
 * also tried to encode the variable-length fields, the first would not fit, the
 * offset would collapse, and an EMPTY payload would be published -- failing the
 * schema check on every required tag and telling replay nothing at all. The
 * observable contract is that the call is still captured, not dropped.
 */
TEST_F(WlrRecorderTest, OversizePayloadStillRecordsMetadata) {
  ASSERT_EQ(startRecorder(1), 0);

  // Comfortably past WLR_MAX_FRAME_PAYLOAD (1 MiB).
  std::string huge(2u << 20, 'x');

  SWorkloadCallToken token;
  std::memset(&token, 0, sizeof(token));
  SWorkloadBeginArgs args;
  std::memset(&args, 0, sizeof(args));
  args.apiType = WLR_API_QUERY;
  args.logicalConnId = 1;
  args.sql = huge.c_str();
  args.sqlLen = static_cast<int32_t>(huge.size());

  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  EXPECT_TRUE(token.recorded)
      << "an oversize payload must degrade to metadata-only, not drop the event";

  SWorkloadReturnArgs ret;
  std::memset(&ret, 0, sizeof(ret));
  wlrApiReturn(&token, &ret);

  // Close so the segment is flushed, then read the BEGIN frame that was actually
  // written. Its payload must retain a conservative subset of the common required
  // TLVs. Zero means the encode collapsed and the event says nothing.
  wlrRecorderClose();
  int32_t len = firstEventPayloadLen(dir_);
  ASSERT_NE(len, -1) << "no event frame could be decoded from the session segments";
  constexpr int32_t kRequiredBeginPayloadMin = 5 * (WLR_TLV_HEADER_SIZE + sizeof(uint64_t));
  EXPECT_GE(len, kRequiredBeginPayloadMin) << "oversize BEGIN published a " << len
                     << "-byte payload; the required metadata was dropped";
  EXPECT_LE(len, (int32_t)WLR_MAX_FRAME_PAYLOAD);
}

TEST_F(WlrRecorderTest, SchemalessPreservesEmptyTableNameKey) {
  ASSERT_EQ(startRecorder(1), 0);
  const char *keys[] = {nullptr, "", "id"};
  const uint16_t apis[] = {WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY,
                           WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY};
  const char *line = "meters,id=chosen,host=a value=1i 1700000000000";
  for (uint16_t api : apis) {
    for (const char *key : keys) {
      SWorkloadBeginArgs args = {};
      args.apiType = api;
      args.logicalConnId = 1;
      args.smlProtocol = 1;
      args.smlPrecision = 4;
      args.smlTbnameKey = key;
      args.smlCount = 1;
      if (api == WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY) {
        args.smlLines = &line;
      } else {
        args.smlRaw = line;
        args.smlRawLen = static_cast<int32_t>(strlen(line));
      }
      SWorkloadCallToken token = {};
      ASSERT_EQ(wlrApiBegin(&args, &token), 0);
      ASSERT_TRUE(token.recorded);
      SWorkloadReturnArgs result = {};
      wlrApiReturn(&token, &result);
    }
  }
  wlrRecorderClose();
  int32_t begins = 0;
  visitDecodedFrames(dir_, [&](const SWlrFrameHeader& frame, const uint8_t* payload) {
    if (frame.type != WLR_FRAME_EVENT) return false;
    SWlrTlv fields[32] = {};
    int32_t count = 0;
    EXPECT_EQ(wlrDecodeTlvs(payload, frame.payloadLength, fields, 32, &count), WLR_DECODE_OK);
    bool begin = false;
    const SWlrTlv *keyField = nullptr;
    for (int32_t index = 0; index < count; ++index) {
      if (WLR_TAG_BASE(fields[index].tag) == WLR_TAG_EVENT_PHASE) {
        begin = fields[index].length == 8 && wlrGetU64(fields[index].value) == WLR_PHASE_BEGIN;
      }
      if (WLR_TAG_BASE(fields[index].tag) == WLR_TAG_SML_TBNAME_KEY) keyField = &fields[index];
    }
    if (begin) {
      const char *expected = keys[begins++ % 3];
      EXPECT_EQ(keyField != nullptr, expected != nullptr);
      if (keyField != nullptr && expected != nullptr) {
        EXPECT_EQ(keyField->length, strlen(expected));
        if (keyField->length > 0) EXPECT_EQ(memcmp(keyField->value, expected, keyField->length), 0);
      }
    }
    return false;
  });
  EXPECT_EQ(begins, 6);
}

TEST_F(WlrRecorderTest, Stmt2PayloadRequiresReaderSupport) {
  ASSERT_EQ(startRecorder(1), 0);

  uint8_t bindDesc[] = {1};
  uint8_t tagDesc[] = {1, 0, 0, 0, 0, 0, 0, 0};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_STMT2_BIND_PARAM;
  args.logicalStmtId = 42;
  args.bindDesc = bindDesc;
  args.bindDescLen = sizeof(bindDesc);
  args.stmt2Tags = tagDesc;
  args.stmt2TagsLen = sizeof(tagDesc);
  const char *names[] = {"t1", nullptr, ""};
  std::vector<uint8_t> nameDesc(wlrStmt2NamesEncodedSize(names, 3));
  ASSERT_GT(wlrEncodeStmt2Names(nameDesc.data(), nameDesc.size(), names, 3), 0);
  args.tbnameList = nameDesc.data();
  args.tbnameListLen = static_cast<int32_t>(nameDesc.size());

  SWorkloadCallToken token = {};
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  ret.logicalStmtId = args.logicalStmtId;
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  bool found = visitDecodedFrames(dir_, [&](const SWlrFrameHeader& frame, const uint8_t* payload) {
    if (frame.type != WLR_FRAME_EVENT) return false;
    SWlrTlv tlvs[32] = {};
    int32_t count = 0;
    if (wlrDecodeTlvs(payload, frame.payloadLength, tlvs, 32, &count) != WLR_DECODE_OK) return false;
    int32_t requiredPayloads = 0;
    for (int32_t index = 0; index < count; ++index) {
      uint16_t baseTag = WLR_TAG_BASE(tlvs[index].tag);
      if (baseTag != WLR_TAG_STMT2_TAGS && baseTag != WLR_TAG_STMT2_NULLABLE_TBNAMES) continue;
      ++requiredPayloads;
      if (baseTag == WLR_TAG_STMT2_NULLABLE_TBNAMES) {
        SWlrSmlLine decodedNames[3] = {};
        int32_t nameCount = 0;
        EXPECT_EQ(wlrDecodeStmt2Names(tlvs[index].value, tlvs[index].length,
                                      decodedNames, 3, &nameCount), WLR_DECODE_OK);
        EXPECT_EQ(nameCount, 3);
        EXPECT_EQ(decodedNames[1].bytes, nullptr);
        EXPECT_NE(decodedNames[2].bytes, nullptr);
        EXPECT_EQ(decodedNames[2].length, 0u);
      }
      EXPECT_TRUE(WLR_TAG_IS_REQUIRED(tlvs[index].tag));
      EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, args.apiType, WLR_PHASE_BEGIN, tlvs, count),
                WLR_DECODE_OK);
      tlvs[index].tag = (uint16_t)(999 | WLR_TAG_REQUIRED_BIT);
      EXPECT_EQ(wlrValidateRequiredTags(WLR_FRAME_EVENT, args.apiType, WLR_PHASE_BEGIN, tlvs, count),
                WLR_DECODE_UNKNOWN_REQUIRED_TAG);
      tlvs[index].tag = (uint16_t)(baseTag | WLR_TAG_REQUIRED_BIT);
    }
    return requiredPayloads == 2;
  });
  EXPECT_TRUE(found);
}

TEST_F(WlrRecorderTest, OversizeConnectionOptionStillSatisfiesSchema) {
  ASSERT_EQ(startRecorder(1), 0);

  std::string huge(2u << 20, 'x');
  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_OPTIONS_CONNECTION;
  args.connOptSet = true;
  args.connOptType = 0;
  args.connOptName = "charset";
  args.connOptValue = huge.c_str();

  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  EXPECT_TRUE(firstBeginHasTagValue(dir_, WLR_API_OPTIONS_CONNECTION, WLR_TAG_CONN_OPT_NAME, ""))
      << "metadata-only connection option must not look like a NULL clear";
}

TEST_F(WlrRecorderTest, NullConnectionOptionKeepsReplayableName) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_OPTIONS_CONNECTION;
  args.connOptSet = true;
  args.connOptType = TSDB_OPTION_CONNECTION_USER_APP;
  args.connOptName = "user_app";

  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  EXPECT_TRUE(firstBeginHasTagValue(dir_, WLR_API_OPTIONS_CONNECTION, WLR_TAG_CONN_OPT_NAME, "user_app"));
}

TEST_F(WlrRecorderTest, SchemalessTbnameKeyIsRecorded) {
  ASSERT_EQ(startRecorder(1), 0);

  const char *lines[] = {"meters,id=a value=1i 1"};
  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY;
  args.logicalConnId = 1;
  args.smlProtocol = 0;
  args.smlPrecision = 0;
  args.smlTbnameKey = "id";
  args.smlLines = lines;
  args.smlCount = 1;

  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  EXPECT_TRUE(firstBeginHasTagValue(dir_, args.apiType, WLR_TAG_SML_TBNAME_KEY, "id"));
}

TEST_F(WlrRecorderTest, StatementPrepareRecordsSql) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token = {};
  SWorkloadBeginArgs args = {};
  args.apiType = WLR_API_STMT_PREPARE;
  args.logicalStmtId = 7;
  args.sql = "select ?";
  args.sqlLen = static_cast<int32_t>(std::strlen(args.sql));
  ASSERT_EQ(wlrApiBegin(&args, &token), 0);
  ASSERT_TRUE(token.recorded);
  SWorkloadReturnArgs ret = {};
  ret.logicalStmtId = 7;
  wlrApiReturn(&token, &ret);
  wlrRecorderClose();

  EXPECT_TRUE(firstBeginHasTagValue(dir_, args.apiType, WLR_TAG_SQL, "select ?"));
}

TEST_F(WlrRecorderTest, StatementPrepareZeroLengthRecordsSql) {
  ASSERT_EQ(startRecorder(1), 0);

  SWorkloadCallToken token = {};
  WLR_API_BEGIN_STMT(&token, WLR_API_STMT2_PREPARE, 7, "select ?", 0);
  ASSERT_TRUE(token.recorded);
  WLR_API_END_STMT(&token, 0, 7);
  wlrRecorderClose();

  EXPECT_TRUE(firstBeginHasTagValue(dir_, WLR_API_STMT2_PREPARE, WLR_TAG_SQL, "select ?"));
}

/*
 * A completed call must never be left on the unfinished list.
 *
 * wlrApiBegin registers the call before waiting for its BEGIN frame to be acked, so
 * that a call hanging inside the real API shows up as unfinished. The registration
 * has to be undone on every exit -- including the ack-failure path, which clears
 * `recorded` and so makes wlrApiReturn skip the removal it would normally do. A leak
 * there strands the entry in the fixed ring and reports a finished call as unfinished.
 *
 * NOTE: this covers the normal path only. The ack-failure branch cannot be reached
 * from here without injecting a writer failure, so this test guards the invariant
 * rather than pinning that specific fix.
 */
TEST_F(WlrRecorderTest, CompletedCallsLeaveNoUnfinishedEntries) {
  ASSERT_EQ(startRecorder(1), 0);

  for (int i = 0; i < 64; ++i) {
    ASSERT_EQ(recordCall(1, "select 1"), 0);
  }

  /* Assert through the manifest because that is where unfinished-call state is
   * published to readers, rather than coupling this check to the manager's layout. */
  wlrRecorderClose();

  std::string manifest = readManifestJson(dir_);
  ASSERT_FALSE(manifest.empty()) << "no manifest written";
  EXPECT_TRUE(jsonHasBool(manifest, "hasUnfinishedCalls", false))
      << "every call completed, so none may be reported unfinished; manifest: " << manifest;
  EXPECT_TRUE(jsonHasInteger(manifest, "unfinishedCallCount", 0)) << "manifest: " << manifest;
  EXPECT_TRUE(jsonHasBool(manifest, "segmentNamesPartial", false)) << "manifest: " << manifest;
}

TEST_F(WlrRecorderTest, SessionStopRotatesBeforeSegmentLimit) {
  ASSERT_EQ(startRecorder(1), 0);
  ASSERT_EQ(recordCall(1, "select 1"), 0);

  for (int attempt = 0; attempt < 1000 &&
       atomic_load_64(&wlrMgr.commitSeq) < atomic_load_64(&wlrMgr.nextEventSeq); ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(atomic_load_64(&wlrMgr.commitSeq), atomic_load_64(&wlrMgr.nextEventSeq));
  wlrMgr.segBytes = static_cast<int64_t>(WLR_SEGMENT_ROTATE_SIZE) - 40;
  wlrRecorderClose();

  EXPECT_EQ(wlrMgr.segIndex, 1u);
  EXPECT_TRUE(hasCleanSessionStop(dir_));
}

#endif  // WLR_HAVE_RECORDER
