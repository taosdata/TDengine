#include <iostream>

#include <gtest/gtest.h>

#include "tmsg.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#define TD_MSG_TYPE_INFO_
#undef TD_MSG_RANGE_CODE_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

TEST(td_msg_test, simple_msg_test) {
  // std::cout << TMSG_INFO(TDMT_VND_DROP_TABLE) << std::endl;
  // std::cout << TMSG_INFO(TDMT_MND_DROP_SUPER_TABLE) << std::endl;
  // std::cout << TMSG_INFO(TDMT_MND_CREATE_SUPER_TABLE) << std::endl;

  int32_t msgSize = sizeof(tMsgTypeInfo) / sizeof(SMsgTypeInfo);
  for (size_t i = 0; i < msgSize; ++i) {
    SMsgTypeInfo *pInfo = &tMsgTypeInfo[i];
    std::cout << i * 2 + 1 << " " << pInfo->name << " " << pInfo->type << std::endl;
    std::cout << i * 2 + 2 << " " << pInfo->rspName << " " << pInfo->rspType << std::endl;
  }
}