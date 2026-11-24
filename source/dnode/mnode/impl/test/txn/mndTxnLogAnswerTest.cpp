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

// Batch meta txn: the answer to a COMMIT / ROLLBACK that reaches the mnode for a txn which is no longer in SDB_TXN
// (mndTxnAnswerFromLog), the case of a client that resends its request after a leader switch. The record of a
// finished txn lives in SDB_TXN_LOG; the answer must be idempotent for the same request and truthful for the other.
//
//   COMMIT   on COMMITTED  -> success          COMMIT   on ROLLEDBACK -> TXN_ROLLEDBACK
//   ROLLBACK on ROLLEDBACK -> success          ROLLBACK on COMMITTED  -> TXN_COMMITTED
//   another user's record -> NO_RIGHTS         a stage that is not terminal -> TXN_NOT_EXIST
//   no record -> not found (the caller decides)

#include <gtest/gtest.h>

#include "mndDef.h"
#include "mndInt.h"
#include "mndTxn.h"
#include "sdb.h"
#include "tglobal.h"

namespace {

// The sdb table of the txn log, reduced to what a lookup needs: the real encode / decode / insert functions of the
// mnode are static, and the answer function only reads the object back by its key.
SdbInsertFp gInsert = nullptr;

SSdbRaw* logEncode(STxnLogObj* pLog) {
  SSdbRaw* pRaw = sdbAllocRaw(SDB_TXN_LOG, 1, sizeof(STxnLogObj));
  if (pRaw == nullptr) return nullptr;
  int32_t dataPos = 0;
  (void)sdbSetRawInt64(pRaw, dataPos, pLog->id);
  dataPos += sizeof(int64_t);
  (void)sdbSetRawInt8(pRaw, dataPos, pLog->stage);
  dataPos += sizeof(int8_t);
  (void)sdbSetRawBinary(pRaw, dataPos, pLog->createUser, TSDB_USER_LEN);
  dataPos += TSDB_USER_LEN;
  (void)sdbSetRawDataLen(pRaw, dataPos);
  return pRaw;
}

SSdbRow* logDecode(SSdbRaw* pRaw) {
  SSdbRow* pRow = sdbAllocRow(sizeof(STxnLogObj));
  if (pRow == nullptr) return nullptr;
  STxnLogObj* pLog = static_cast<STxnLogObj*>(sdbGetRowObj(pRow));
  int32_t     dataPos = 0;
  (void)sdbGetRawInt64(pRaw, dataPos, &pLog->id);
  dataPos += sizeof(int64_t);
  (void)sdbGetRawInt8(pRaw, dataPos, &pLog->stage);
  dataPos += sizeof(int8_t);
  (void)sdbGetRawBinary(pRaw, dataPos, pLog->createUser, TSDB_USER_LEN);
  return pRow;
}

int32_t logInsert(SSdb*, STxnLogObj*) { return 0; }
int32_t logUpdate(SSdb*, STxnLogObj*, STxnLogObj*) { return 0; }
int32_t logDelete(SSdb*, STxnLogObj*, bool) { return 0; }

class MndTxnLogAnswerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = TD_TMP_DIR_PATH "mnode_test_txn_log_answer";
    taosRemoveDir(path_);
    SSdbOpt opt = {};
    opt.pMnode = &mnode_;
    opt.path = path_;
    mnode_.pSdb = sdbInit(&opt);
    ASSERT_NE(nullptr, mnode_.pSdb);

    SSdbTable table = {};
    table.sdbType = SDB_TXN_LOG;
    table.keyType = SDB_KEY_INT64;
    table.encodeFp = (SdbEncodeFp)logEncode;
    table.decodeFp = (SdbDecodeFp)logDecode;
    table.insertFp = (SdbInsertFp)logInsert;
    table.updateFp = (SdbUpdateFp)logUpdate;
    table.deleteFp = (SdbDeleteFp)logDelete;
    ASSERT_EQ(0, sdbSetTable(mnode_.pSdb, table));
  }

  void TearDown() override {
    sdbCleanup(mnode_.pSdb);
    taosRemoveDir(path_);
  }

  void putLog(txn_id_t id, int8_t stage, const char* user) {
    STxnLogObj log = {};
    log.id = id;
    log.stage = stage;
    tstrncpy(log.createUser, user, sizeof(log.createUser));
    SSdbRaw* pRaw = logEncode(&log);
    ASSERT_NE(nullptr, pRaw);
    ASSERT_EQ(0, sdbSetRawStatus(pRaw, SDB_STATUS_READY));
    ASSERT_EQ(0, sdbWrite(mnode_.pSdb, pRaw));
  }

  // returns the code; *pFound as reported
  int32_t ask(txn_id_t id, bool isCommit, const char* user, bool* pFound) {
    SRpcMsg req = {};
    tstrncpy(req.info.conn.user, user, sizeof(req.info.conn.user));
    return mndTxnAnswerFromLog(&mnode_, &req, id, isCommit, pFound);
  }

  const char* path_ = nullptr;
  SMnode      mnode_ = {};
};

}  // namespace

TEST_F(MndTxnLogAnswerTest, committedTxn) {
  putLog(1001, UTXN_STAGE_COMMITTED, "alice");
  bool found = false;
  EXPECT_EQ(TSDB_CODE_SUCCESS, ask(1001, true, "alice", &found));  // a resent COMMIT is idempotent
  EXPECT_TRUE(found);
  EXPECT_EQ(TSDB_CODE_TXN_COMMITTED, ask(1001, false, "alice", &found));  // ROLLBACK must not claim success
  EXPECT_TRUE(found);
}

TEST_F(MndTxnLogAnswerTest, rolledBackTxn) {
  putLog(1002, UTXN_STAGE_ROLLEDBACK, "alice");
  bool found = false;
  EXPECT_EQ(TSDB_CODE_SUCCESS, ask(1002, false, "alice", &found));  // a resent ROLLBACK is idempotent
  EXPECT_TRUE(found);
  EXPECT_EQ(TSDB_CODE_TXN_ROLLEDBACK, ask(1002, true, "alice", &found));  // COMMIT must not claim success
  EXPECT_TRUE(found);
}

TEST_F(MndTxnLogAnswerTest, anotherUsersRecordIsNotAnswered) {
  putLog(1003, UTXN_STAGE_COMMITTED, "alice");
  putLog(1004, UTXN_STAGE_ROLLEDBACK, "alice");
  bool found = false;
  EXPECT_EQ(TSDB_CODE_MND_NO_RIGHTS, ask(1003, true, "bob", &found));
  EXPECT_TRUE(found);
  EXPECT_EQ(TSDB_CODE_MND_NO_RIGHTS, ask(1003, false, "bob", &found));
  EXPECT_EQ(TSDB_CODE_MND_NO_RIGHTS, ask(1004, false, "bob", &found));
  EXPECT_EQ(TSDB_CODE_MND_NO_RIGHTS, ask(1004, true, "bob", &found));
}

TEST_F(MndTxnLogAnswerTest, nonTerminalStageIsNotExist) {
  // only COMMITTED / ROLLEDBACK are ever written; stage 9 (once planned as ZOMBIE) and the like are not answered
  for (int8_t stage : {(int8_t)UTXN_STAGE_ACTIVE, (int8_t)UTXN_STAGE_COMMITTING, (int8_t)UTXN_STAGE_TIMEOUT_KILLED,
                       (int8_t)42}) {
    putLog(2000 + stage, stage, "alice");
    bool found = false;
    EXPECT_EQ(TSDB_CODE_TXN_NOT_EXIST, ask(2000 + stage, true, "alice", &found)) << "stage " << (int)stage;
    EXPECT_TRUE(found);
    EXPECT_EQ(TSDB_CODE_TXN_NOT_EXIST, ask(2000 + stage, false, "alice", &found)) << "stage " << (int)stage;
  }
}

TEST_F(MndTxnLogAnswerTest, noRecordIsNotFound) {
  bool found = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS, ask(9999, true, "alice", &found));
  EXPECT_FALSE(found);  // the caller decides (COMMIT: not exist, ROLLBACK: nothing to undo)
  found = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS, ask(9999, false, "alice", &found));
  EXPECT_FALSE(found);
}
