#include <gtest/gtest.h>

#include "trow.h"

TEST(td_row_test, build_row_to_target) {
#if 0
  char        dst[1024];
  SRow*       pRow = (SRow*)dst;
  int         ncols = 10;
  col_id_t    cid;
  void*       pData;
  SRowBuilder rb = trbInit(TD_OR_ROW_BUILDER, NULL, 0, pRow, 1024);

  trbSetRowInfo(&rb, false, 0);
  trbSetRowTS(&rb, 1637550210000);
  for (int c = 0; c < ncols; c++) {
    cid = c;
    if (trbWriteCol(&rb, pData, cid) < 0) {
      // TODO
    }
  }
#endif
}