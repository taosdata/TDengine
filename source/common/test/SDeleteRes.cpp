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

// SDeleteRes 的编解码测试。DELETE 支持普通列过滤走的是"leader 在前置处理阶段把命中行拆成 N 个
// 独立的老格式点删除、各自走一次 Raft 提交"，SDeleteRes 本身的线上格式不做任何扩展，
// 与本特性之前完全一致——这里只验证这份线上格式的编解码往返正确。

static void encodeDeleteRes(const SDeleteRes *pRes, std::vector<char> *pBuf) {
  int32_t size = 0;
  int32_t ret = 0;
  tEncodeSize(tEncodeDeleteRes, pRes, size, ret);
  ASSERT_EQ(ret, 0);
  ASSERT_GT(size, 0);

  pBuf->resize(size);
  SEncoder encoder = {0};
  tEncoderInit(&encoder, (uint8_t *)pBuf->data(), size);
  ASSERT_EQ(tEncodeDeleteRes(&encoder, pRes), 0);
  tEncoderClear(&encoder);
}

TEST(td_msg_test, s_delete_res_codec) {
  SDeleteRes src = {0};
  src.suid = 1001;
  src.uidList = taosArrayInit(2, sizeof(tb_uid_t));
  ASSERT_NE(src.uidList, nullptr);
  tb_uid_t uid1 = 2001, uid2 = 2002;
  ASSERT_NE(taosArrayPush(src.uidList, &uid1), nullptr);
  ASSERT_NE(taosArrayPush(src.uidList, &uid2), nullptr);
  src.skey = 1700000000000LL;
  src.ekey = 1700000009999LL;
  src.affectedRows = 42;
  tstrncpy(src.tableFName, "db.stb", sizeof(src.tableFName));
  tstrncpy(src.tsColName, "ts", sizeof(src.tsColName));
  src.ctimeMs = 1700000010000LL;
  src.source = 1;
  src.secureDelete = 1;

  std::vector<char> buf;
  encodeDeleteRes(&src, &buf);

  SDeleteRes dst = {0};
  dst.uidList = taosArrayInit(0, sizeof(tb_uid_t));
  ASSERT_NE(dst.uidList, nullptr);

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t *)buf.data(), buf.size());
  ASSERT_EQ(tDecodeDeleteRes(&decoder, &dst), 0);
  tDecoderClear(&decoder);

  ASSERT_EQ(dst.suid, src.suid);
  ASSERT_EQ(taosArrayGetSize(dst.uidList), 2);
  ASSERT_EQ(*(tb_uid_t *)taosArrayGet(dst.uidList, 0), uid1);
  ASSERT_EQ(*(tb_uid_t *)taosArrayGet(dst.uidList, 1), uid2);
  ASSERT_EQ(dst.skey, src.skey);
  ASSERT_EQ(dst.ekey, src.ekey);
  ASSERT_EQ(dst.affectedRows, src.affectedRows);
  ASSERT_STREQ(dst.tableFName, src.tableFName);
  ASSERT_STREQ(dst.tsColName, src.tsColName);
  ASSERT_EQ(dst.ctimeMs, src.ctimeMs);
  ASSERT_EQ(dst.source, src.source);
  ASSERT_EQ(dst.secureDelete, src.secureDelete);

  taosArrayDestroy(src.uidList);
  taosArrayDestroy(dst.uidList);
}
