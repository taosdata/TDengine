#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "replayLane.h"

class ReplayLaneTest : public testing::Test {
 protected:
  std::unique_ptr<SReplayShared> shared{new SReplayShared{}};
  SReplaySession session{};
  std::vector<SReplayEvent> events;

  void TearDown() override { replayLaneFreeDeps(shared.get()); }

  void append(uint64_t callId, uint16_t apiType, uint16_t phase,
              uint64_t parentCallId = 0, uint64_t stmtId = 0) {
    SReplayEvent event{};
    event.callId = callId;
    event.apiType = apiType;
    event.phase = phase;
    event.parentCallId = parentCallId;
    event.stmtId = stmtId;
    event.connId = 1;
    event.stmt2Async = apiType == WLR_API_STMT2_EXEC;
    events.push_back(event);
  }

  int32_t initialize() {
    session.events = events.data();
    session.eventCount = static_cast<int32_t>(events.size());
    shared->session = &session;
    return replayLaneInitDeps(shared.get());
  }
};

TEST_F(ReplayLaneTest, LongQueryCallbackChain) {
  const int32_t queryCount = 40000;
  events.reserve(queryCount * 5);
  for (int32_t index = 0; index < queryCount; ++index) {
    uint64_t callId = static_cast<uint64_t>(index) * 2 + 1;
    uint64_t parentCallId = index > 0 ? callId - 2 : 0;
    append(callId, WLR_API_QUERY_A, WLR_PHASE_BEGIN, parentCallId);
    append(callId, WLR_API_QUERY_A, WLR_PHASE_RETURN);
    append(callId, WLR_API_QUERY_A, WLR_PHASE_DONE);
    events.back().resultId = callId;
    append(callId + 1, WLR_API_FREE_RESULT, WLR_PHASE_BEGIN, callId);
    events.back().resultId = callId;
    append(callId + 1, WLR_API_FREE_RESULT, WLR_PHASE_RETURN);
  }
  auto started = std::chrono::steady_clock::now();
  ASSERT_EQ(initialize(), REPLAY_EXIT_OK);
  auto elapsed = std::chrono::steady_clock::now() - started;
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 10000);
  EXPECT_EQ(shared->returnCount, queryCount * 2);
  EXPECT_EQ(shared->terminalByBegin[0], 2);
  EXPECT_EQ(shared->terminalByBegin[(queryCount - 1) * 5], (queryCount - 1) * 5 + 2);
}

TEST_F(ReplayLaneTest, InheritedStmt2AncestorRejectsLifecycleOverlap) {
  const uint16_t operations[] = {WLR_API_STMT2_EXEC, WLR_API_STMT2_CLOSE, WLR_API_STMT2_PREPARE};
  for (uint16_t operation : operations) {
    for (bool overlaps : {false, true}) {
      SCOPED_TRACE(testing::Message() << operation << ": " << overlaps);
      events.clear();
      append(1, WLR_API_STMT2_BIND_PARAM_A, WLR_PHASE_BEGIN, 0, 7);
      append(1, WLR_API_STMT2_BIND_PARAM_A, WLR_PHASE_DONE);
      append(2, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 1);
      append(2, WLR_API_QUERY_A, WLR_PHASE_DONE);
      append(3, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 2);
      if (!overlaps) append(3, WLR_API_QUERY_A, WLR_PHASE_DONE);
      append(4, operation, WLR_PHASE_BEGIN, 0, 7);
      append(4, operation, WLR_PHASE_DONE);
      if (overlaps) append(3, WLR_API_QUERY_A, WLR_PHASE_DONE);
      EXPECT_EQ(initialize(), overlaps ? REPLAY_EXIT_INPUT : REPLAY_EXIT_OK);
    }
  }
}

TEST_F(ReplayLaneTest, BindWaitsForOverlappingAsyncExecute) {
  const uint16_t bindApis[] = {WLR_API_STMT2_BIND_PARAM, WLR_API_STMT2_BIND_PARAM_A,
                               WLR_API_STMT2_BIND_PARAM_COLUMN, WLR_API_STMT2_BIND_PARAM_COLUMN_A};
  for (uint16_t bindApi : bindApis) {
    SCOPED_TRACE(bindApi);
    uint16_t bindTerminal = (bindApi == WLR_API_STMT2_BIND_PARAM_A ||
                             bindApi == WLR_API_STMT2_BIND_PARAM_COLUMN_A)
                                ? WLR_PHASE_DONE : WLR_PHASE_RETURN;
    events.clear();
    append(1, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 0, 7);
    append(1, WLR_API_STMT2_EXEC, WLR_PHASE_RETURN, 0, 7);
    append(2, bindApi, WLR_PHASE_BEGIN, 0, 7);
    events.back().originTid = 2;
    append(1, WLR_API_STMT2_EXEC, WLR_PHASE_DONE, 0, 7);
    append(2, bindApi, bindTerminal, 0, 7);
    events.back().originTid = 2;
    ASSERT_EQ(initialize(), REPLAY_EXIT_OK);
    EXPECT_EQ(shared->closePending[2], 1);
    EXPECT_EQ(shared->dependentClose[shared->dependentHead[0]], 2);

    events.clear();
    append(1, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 0, 7);
    append(1, WLR_API_STMT2_EXEC, WLR_PHASE_RETURN, 0, 7);
    append(1, WLR_API_STMT2_EXEC, WLR_PHASE_DONE, 0, 7);
    append(2, bindApi, WLR_PHASE_BEGIN, 0, 7);
    append(2, bindApi, bindTerminal, 0, 7);
    ASSERT_EQ(initialize(), REPLAY_EXIT_OK);
    EXPECT_EQ(shared->closePending[3], 0);
  }
}

TEST_F(ReplayLaneTest, PrepareWaitsForOverlappingAsyncExecute) {
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 0, 7);
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_RETURN, 0, 7);
  append(2, WLR_API_STMT2_PREPARE, WLR_PHASE_BEGIN, 0, 7);
  events.back().originTid = 2;
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_DONE, 0, 7);
  append(2, WLR_API_STMT2_PREPARE, WLR_PHASE_RETURN, 0, 7);
  events.back().originTid = 2;
  ASSERT_EQ(initialize(), REPLAY_EXIT_OK);
  EXPECT_EQ(shared->closePending[2], 1);
  EXPECT_EQ(shared->dependentClose[shared->dependentHead[0]], 2);
}

TEST_F(ReplayLaneTest, RejectsBindOverlappingExecuteCallback) {
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 0, 7);
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_RETURN, 0, 7);
  append(2, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 1);
  append(3, WLR_API_STMT2_BIND_PARAM, WLR_PHASE_BEGIN, 0, 7);
  append(2, WLR_API_QUERY_A, WLR_PHASE_DONE, 0, 0);
  append(3, WLR_API_STMT2_BIND_PARAM, WLR_PHASE_RETURN, 0, 7);
  EXPECT_EQ(initialize(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayLaneTest, RejectsBindStartedBeforeExecuteCallback) {
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 0, 7);
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_RETURN, 0, 7);
  append(3, WLR_API_STMT2_BIND_PARAM, WLR_PHASE_BEGIN, 0, 7);
  events.back().originTid = 3;
  append(1, WLR_API_STMT2_EXEC, WLR_PHASE_DONE, 0, 7);
  append(2, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 1);
  events.back().originTid = 2;
  append(2, WLR_API_QUERY_A, WLR_PHASE_DONE, 0, 0);
  append(3, WLR_API_STMT2_BIND_PARAM, WLR_PHASE_RETURN, 0, 7);
  events.back().originTid = 3;
  EXPECT_EQ(initialize(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayLaneTest, RejectsUnfinishedBindAndCallbackOverlap) {
  for (bool finishBind : {false, true}) {
    for (bool finishCallback : {false, true}) {
      if (finishBind && finishCallback) continue;
      SCOPED_TRACE(testing::Message() << "finishBind=" << finishBind
                                      << " finishCallback=" << finishCallback);
      events.clear();
      append(1, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 0, 7);
      append(1, WLR_API_STMT2_EXEC, WLR_PHASE_RETURN, 0, 7);
      append(1, WLR_API_STMT2_EXEC, WLR_PHASE_DONE, 0, 7);
      append(2, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 1);
      events.back().originTid = 2;
      append(3, WLR_API_STMT2_BIND_PARAM, WLR_PHASE_BEGIN, 0, 7);
      events.back().originTid = 3;
      if (finishCallback) {
        append(2, WLR_API_QUERY_A, WLR_PHASE_DONE);
        events.back().originTid = 2;
      }
      if (finishBind) {
        append(3, WLR_API_STMT2_BIND_PARAM, WLR_PHASE_RETURN, 0, 7);
        events.back().originTid = 3;
      }
      EXPECT_EQ(initialize(), REPLAY_EXIT_INPUT);
    }
  }
}

TEST_F(ReplayLaneTest, NestedStmt2AncestorsAreRejected) {
  append(1, WLR_API_STMT2_BIND_PARAM_COLUMN_A, WLR_PHASE_BEGIN, 0, 7);
  append(1, WLR_API_STMT2_BIND_PARAM_COLUMN_A, WLR_PHASE_DONE);
  append(2, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 1);
  append(2, WLR_API_QUERY_A, WLR_PHASE_DONE);
  append(3, WLR_API_STMT2_EXEC, WLR_PHASE_BEGIN, 2, 8);
  append(3, WLR_API_STMT2_EXEC, WLR_PHASE_DONE);
  append(4, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 3);
  append(4, WLR_API_QUERY_A, WLR_PHASE_DONE);
  EXPECT_EQ(initialize(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayLaneTest, MissingParentIsAllowedButForwardAndSelfParentsAreRejected) {
  append(1, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 99);
  append(1, WLR_API_QUERY_A, WLR_PHASE_DONE);
  append(2, WLR_API_QUERY_A, WLR_PHASE_BEGIN, 1);
  append(2, WLR_API_QUERY_A, WLR_PHASE_DONE);
  EXPECT_EQ(initialize(), REPLAY_EXIT_OK);
  events[0].parentCallId = 1;
  EXPECT_EQ(initialize(), REPLAY_EXIT_INPUT);
  events[0].parentCallId = 2;
  EXPECT_EQ(initialize(), REPLAY_EXIT_INPUT);
}

class ReplayInputTest : public testing::Test {
 protected:
  SReplaySession session{};
  std::vector<std::string> paths;

  void TearDown() override {
    replayFreeSession(&session);
    for (const auto& path : paths) EXPECT_EQ(taosRemoveFile(path.c_str()), 0);
  }

  void writeInput(uint8_t sessionId, const char *clockDomain, int64_t wallNs, int64_t startMonoNs,
                  int64_t eventMonoNs, bool legacy = false, const char *key = nullptr,
                  uint16_t api = WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY,
                  uint32_t segmentIndex = 0) {
    SWlrFileHeader header{};
    header.sessionId[0] = sessionId;
    header.pid = sessionId;
    header.processStartWallNs = wallNs;
    header.processStartMonoNs = startMonoNs;
    header.segmentIndex = segmentIndex;
    header.firstEventSeq = segmentIndex + 1;
    if (clockDomain != nullptr) tstrncpy(header.clockDomainId, clockDomain, sizeof(header.clockDomainId));
    std::vector<uint8_t> bytes(WLR_FILE_HEADER_SIZE);
    ASSERT_EQ(wlrEncodeFileHeader(bytes.data(), bytes.size(), &header), WLR_FILE_HEADER_SIZE);
    if (legacy) {
      wlrPutU16(bytes.data() + WLR_HDR_OFF_MINOR, 0);
      wlrPutU16(bytes.data() + WLR_HDR_OFF_HDRLEN, WLR_FILE_HEADER_V0_SIZE);
      bytes.resize(WLR_FILE_HEADER_V0_SIZE);
    }

    uint8_t payload[512]{};
    size_t offset = 0;
    const uint16_t tags[] = {WLR_TAG_WALL_NS, WLR_TAG_MONO_NS, WLR_TAG_ORIGIN_TID,
                             WLR_TAG_EMITTER_TID, WLR_TAG_API_TYPE, WLR_TAG_EVENT_PHASE,
                             WLR_TAG_CALL_ID, WLR_TAG_CONN_ID, WLR_TAG_SML_PROTOCOL, WLR_TAG_SML_PRECISION};
    const uint64_t values[] = {static_cast<uint64_t>(wallNs), static_cast<uint64_t>(eventMonoNs),
                               1, 1, api, WLR_PHASE_BEGIN, segmentIndex + 1, 1, 1, 4};
    for (size_t index = 0; index < sizeof(tags) / sizeof(tags[0]); ++index) {
      int32_t count = wlrEncodeTlvU64(payload + offset, sizeof(payload) - offset,
                                     tags[index], true, values[index]);
      ASSERT_GT(count, 0);
      offset += count;
    }
    if (key != nullptr) {
      int32_t count = wlrEncodeTlv(payload + offset, sizeof(payload) - offset,
                                  WLR_TAG_SML_TBNAME_KEY, false, 0, key, strlen(key));
      ASSERT_GT(count, 0);
      offset += count;
    }
    SWlrFrameHeader frame{};
    frame.type = WLR_FRAME_EVENT;
    frame.eventSequence = header.firstEventSeq;
    frame.logicalEventId = header.firstEventSeq;
    frame.payloadLength = static_cast<uint32_t>(offset);
    size_t headerSize = bytes.size();
    bytes.resize(headerSize + wlrFrameEncodedSize(frame.payloadLength));
    ASSERT_GT(wlrEncodeFrame(bytes.data() + headerSize, bytes.size() - headerSize, &frame, payload), 0);

    char temporary[PATH_MAX]{};
    taosGetTmpfilePath(testing::TempDir().c_str(), "replay-input", temporary);
    paths.push_back(std::string(temporary) + ".wlr");
    TdFilePtr file = taosOpenFile(paths.back().c_str(), TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_EXCL);
    ASSERT_NE(file, nullptr);
    EXPECT_EQ(taosWriteFile(file, bytes.data(), bytes.size()), static_cast<int64_t>(bytes.size()));
    EXPECT_EQ(taosCloseFile(&file), 0);
  }

  int32_t load(bool allowIncomplete = false) {
    replayFreeSession(&session);
    SReplayOpts options{};
    options.mode = REPLAY_MODE_REPLAY;
    options.allowIncomplete = allowIncomplete;
    options.inputCount = static_cast<int32_t>(paths.size());
    for (size_t index = 0; index < paths.size(); ++index) options.inputs[index] = paths[index].c_str();
    return replayLoadInputs(&options, &session);
  }
};

TEST_F(ReplayInputTest, RejectsDifferentBootsWithSimilarWallOffsets) {
  writeInput(1, "linux:boot-a:0:0", 1000000000000LL, 1000000000LL, 1000000000000LL);
  writeInput(2, "linux:boot-b:0:0", 1000000000000LL, 3000000000LL, 999000000000LL);
  EXPECT_EQ(load(), REPLAY_EXIT_INPUT);
  EXPECT_EQ(load(true), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayInputTest, RejectsDifferentMonotonicNamespaceOffsets) {
  writeInput(1, "linux:boot-a:0:0", 1000000000000LL, 1000000000LL, 1000000000000LL);
  writeInput(2, "linux:boot-a:2:0", 1000000000000LL, 3000000000LL, 999000000000LL);
  EXPECT_EQ(load(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayInputTest, SameDomainPreservesOrderAcrossWallClockChanges) {
  writeInput(1, "linux:boot-a:0:0", 1000000000000LL, 1000000000LL, 1000000000000LL);
  writeInput(2, "linux:boot-a:0:0", 5000000000000LL, 3000000000LL, 1001000000000LL);
  ASSERT_EQ(load(), REPLAY_EXIT_OK);
  ASSERT_EQ(session.eventCount, 2);
  EXPECT_EQ(session.events[0].sessionIdx, 0);
  EXPECT_EQ(session.events[0].monoNs, 0);
  EXPECT_EQ(session.events[1].monoNs, 1000000000LL);
}

TEST_F(ReplayInputTest, RejectsMissingClockIdentityInMergedInputs) {
  writeInput(1, "linux:boot-a:0:0", 1000, 100, 200);
  writeInput(2, nullptr, 1000, 100, 201);
  EXPECT_EQ(load(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayInputTest, LegacySessionRemainsReadableButCannotBeMerged) {
  writeInput(1, nullptr, 1000, 100, 200, true);
  ASSERT_EQ(load(), REPLAY_EXIT_OK);
  EXPECT_EQ(session.eventCount, 1);
  writeInput(2, nullptr, 1000, 100, 201, true);
  EXPECT_EQ(load(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayInputTest, RejectsClockIdentityChangesWithinSession) {
  writeInput(1, "linux:boot-a:0:0", 1000, 100, 200);
  writeInput(1, "linux:boot-b:0:0", 1000, 100, 201, false, nullptr,
             WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY, 1);
  EXPECT_EQ(load(), REPLAY_EXIT_INPUT);
}

TEST_F(ReplayInputTest, RetainsSchemalessKeyPresenceForLineAndRawCalls) {
  const char *keys[] = {nullptr, "", "id"};
  const uint16_t apis[] = {WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY,
                           WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY};
  uint8_t sessionId = 0;
  for (uint16_t api : apis) {
    for (const char *key : keys) {
      ++sessionId;
      writeInput(sessionId, "linux:boot-a:0:0", 1000, 100, 200 + sessionId, false, key, api);
    }
  }
  ASSERT_EQ(load(), REPLAY_EXIT_OK);
  ASSERT_EQ(session.eventCount, 6);
  for (int32_t index = 0; index < session.eventCount; ++index) {
    const SReplayEvent& event = session.events[index];
    const char *expected = keys[index % 3];
    EXPECT_EQ(event.smlTbnameKeySet, expected != nullptr);
    EXPECT_EQ(event.smlTbnameKeyLen, expected == nullptr ? 0u : strlen(expected));
    if (event.smlTbnameKeyLen > 0) EXPECT_EQ(memcmp(event.smlTbnameKey, expected, event.smlTbnameKeyLen), 0);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
