#include "taosdef.h"
#include "tcompare.h"

int32_t compareInt32Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT32_VAL(pLeft) - GET_INT32_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareInt64Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT64_VAL(pLeft) - GET_INT64_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareInt16Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT16_VAL(pLeft) - GET_INT16_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareInt8Val(const void *pLeft, const void *pRight) {
  int32_t ret = GET_INT8_VAL(pLeft) - GET_INT8_VAL(pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareIntDoubleVal(const void *pLeft, const void *pRight) {
  //  int64_t lhs = ((SSkipListKey *)pLeft)->i64Key;
  //  double  rhs = ((SSkipListKey *)pRight)->dKey;
  //  if (fabs(lhs - rhs) < FLT_EPSILON) {
  //    return 0;
  //  } else {
  //    return (lhs > rhs) ? 1 : -1;
  //  }
  return 0;
}

int32_t compareDoubleIntVal(const void *pLeft, const void *pRight) {
  //  double  lhs = ((SSkipListKey *)pLeft)->dKey;
  //  int64_t rhs = ((SSkipListKey *)pRight)->i64Key;
  //  if (fabs(lhs - rhs) < FLT_EPSILON) {
  //    return 0;
  //  } else {
  //    return (lhs > rhs) ? 1 : -1;
  //  }
  return 0;
}

int32_t compareDoubleVal(const void *pLeft, const void *pRight) {
  double ret = GET_DOUBLE_VAL(pLeft) - GET_DOUBLE_VAL(pRight);
  if (fabs(ret) < FLT_EPSILON) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareStrVal(const void *pLeft, const void *pRight) {
  int32_t ret = strcmp(pLeft, pRight);
  if (ret == 0) {
    return 0;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

int32_t compareWStrVal(const void *pLeft, const void *pRight) {
  //  SSkipListKey *pL = (SSkipListKey *)pLeft;
  //  SSkipListKey *pR = (SSkipListKey *)pRight;
  //
  //  if (pL->nLen == 0 && pR->nLen == 0) {
  //    return 0;
  //  }
  //
  //  // handle only one-side bound compare situation, there is only lower bound or only upper bound
  //  if (pL->nLen == -1) {
  //    return 1;  // no lower bound, lower bound is minimum, always return -1;
  //  } else if (pR->nLen == -1) {
  //    return -1;  // no upper bound, upper bound is maximum situation, always return 1;
  //  }
  //
  //  int32_t ret = wcscmp(((SSkipListKey *)pLeft)->wpz, ((SSkipListKey *)pRight)->wpz);
  //
  //  if (ret == 0) {
  //    return 0;
  //  } else {
  //    return ret > 0 ? 1 : -1;
  //  }
  return 0;
}

__compar_fn_t getComparFunc(int32_t type, int32_t filterDataType) {
  __compar_fn_t comparFn = NULL;
  
  switch (type) {
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT: {
      if (filterDataType == TSDB_DATA_TYPE_BIGINT) {
        comparFn = compareInt64Val;
        break;
      }
    }
    case TSDB_DATA_TYPE_BOOL: {
      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
        comparFn = compareInt32Val;
      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareIntDoubleVal;
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
//      if (filterDataType >= TSDB_DATA_TYPE_BOOL && filterDataType <= TSDB_DATA_TYPE_BIGINT) {
//        comparFn = compareDoubleIntVal;
//      } else if (filterDataType >= TSDB_DATA_TYPE_FLOAT && filterDataType <= TSDB_DATA_TYPE_DOUBLE) {
//        comparFn = compareDoubleVal;
//      }
      if (filterDataType == TSDB_DATA_TYPE_DOUBLE) {
        comparFn = compareDoubleVal;
      }
      break;
    }
    case TSDB_DATA_TYPE_BINARY:
      comparFn = compareStrVal;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      comparFn = compareWStrVal;
      break;
    default:
      comparFn = compareInt32Val;
      break;
  }
  
  return comparFn;
}

__compar_fn_t getKeyComparFunc(int32_t keyType) {
  __compar_fn_t comparFn = NULL;
  
  switch (keyType) {
    case TSDB_DATA_TYPE_TINYINT:
      comparFn = compareInt8Val;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      comparFn = compareInt16Val;
      break;
    case TSDB_DATA_TYPE_INT:
      comparFn = compareInt32Val;
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      comparFn = compareInt64Val;
      break;
    case TSDB_DATA_TYPE_BOOL:
      comparFn = compareInt32Val;
      break;
    
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      comparFn = compareDoubleVal;
      break;
    
    case TSDB_DATA_TYPE_BINARY:
      comparFn = compareStrVal;
      break;
    
    case TSDB_DATA_TYPE_NCHAR:
      comparFn = compareWStrVal;
      break;
    
    default:
      comparFn = compareInt32Val;
      break;
  }
  
  return comparFn;
}
