#include "tunaryoperator.h"

static void assignBasicParaInfo(struct SScalarFuncParam* dst, const struct SScalarFuncParam* src) {
  dst->type = src->type;
  dst->bytes = src->bytes;
  dst->num = src->num;
}

static void tceil(SScalarFuncParam *pLeft, SScalarFuncParam* pOutput) {
  assignBasicParaInfo(pOutput, pLeft);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = ceilf(p[i]);
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*)pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = ceil(p[i]);
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void tfloor(SScalarFuncParam *pLeft, SScalarFuncParam* pOutput) {
  assignBasicParaInfo(pOutput, pLeft);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;

      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = floorf(p[i]);
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*) pOutput->data;

      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = floor(p[i]);
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void _tabs(SScalarFuncParam *pLeft, SScalarFuncParam* pOutput) {
  assignBasicParaInfo(pOutput, pLeft);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t* p = (int8_t*) pLeft->data;
      int8_t* out = (int8_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t* p = (int16_t*) pLeft->data;
      int16_t* out = (int16_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t* p = (int32_t*) pLeft->data;
      int32_t* out = (int32_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t* p = (int64_t*) pLeft->data;
      int64_t* out = (int64_t*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = (p[i] > 0)? p[i]:-p[i];
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void tround(SScalarFuncParam *pLeft, SScalarFuncParam* pOutput) {
  assignBasicParaInfo(pOutput, pLeft);

  switch (pLeft->bytes) {
    case TSDB_DATA_TYPE_FLOAT: {
      float* p = (float*) pLeft->data;
      float* out = (float*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = roundf(p[i]);
      }
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double* p = (double*) pLeft->data;
      double* out = (double*) pOutput->data;
      for (int32_t i = 0; i < pLeft->num; ++i) {
        out[i] = round(p[i]);
      }
    }

    default:
      memcpy(pOutput->data, pLeft->data, pLeft->num* pLeft->bytes);
  }
}

static void tlen(SScalarFuncParam *pLeft, SScalarFuncParam* pOutput) {
  int64_t* out = (int64_t*) pOutput->data;
  char* s = pLeft->data;

  for(int32_t i = 0; i < pLeft->num; ++i) {
    out[i] = varDataLen(POINTER_SHIFT(s, i * pLeft->bytes));
  }
}

_unary_scalar_fn_t getUnaryScalarOperatorFn(int32_t operator) {
  switch(operator) {
    case TSDB_UNARY_OP_CEIL:
      return tceil;
    case TSDB_UNARY_OP_FLOOR:
      return tfloor;
    case TSDB_UNARY_OP_ROUND:
      return tround;
    case TSDB_UNARY_OP_ABS:
      return _tabs;
    case TSDB_UNARY_OP_LEN:
      return tlen;
    default:
      assert(0);
  }
}

bool isStringOperatorFn(int32_t op) {
  return op == TSDB_UNARY_OP_LEN;
}
