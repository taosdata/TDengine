#define Py_LIMITED_API 0x030A0000
#include <Python.h>

#include <taospyudf.h>

#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#ifndef _WIN32
#include <dlfcn.h>
#endif

#include <filesystem>
#include <regex>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

static PyObject *gPyUdfDataBlockType = nullptr;

struct UdfDataBlockPyObject {
  PyObject_HEAD
  SUdfDataBlock *dataBlock;
};

static std::string pyObjToString(PyObject *obj) {
  if (obj == nullptr) {
    return "<null>";
  }
  PyObject *s = PyObject_Str(obj);
  if (s == nullptr) {
    PyErr_Clear();
    return "<str failed>";
  }
  PyObject *bytes = PyUnicode_AsEncodedString(s, "utf-8", "replace");
  std::string out = "<utf8 failed>";
  if (bytes != nullptr) {
    const char *c = nullptr;
    Py_ssize_t n = 0;
    if (PyBytes_AsStringAndSize(bytes, const_cast<char **>(&c), &n) == 0 && c != nullptr) {
      out.assign(c, static_cast<size_t>(n));
    }
    Py_DECREF(bytes);
  } else {
    PyErr_Clear();
  }
  Py_DECREF(s);
  return out;
}

static std::string fetchPyError() {
  if (!PyErr_Occurred()) {
    return "<no python error>";
  }

  PyObject *ptype = nullptr;
  PyObject *pvalue = nullptr;
  PyObject *ptraceback = nullptr;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);

  std::string typeStr = pyObjToString(ptype);
  std::string valueStr = pyObjToString(pvalue);
  std::string out = typeStr + ": " + valueStr;

  Py_XDECREF(ptype);
  Py_XDECREF(pvalue);
  Py_XDECREF(ptraceback);
  return out;
}

static PyObject *UdfDataBlock_shape(PyObject *self, PyObject *) {
  auto *obj = reinterpret_cast<UdfDataBlockPyObject *>(self);
  if (obj->dataBlock == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "invalid UdfDataBlock");
    return nullptr;
  }
  return Py_BuildValue("(ii)", obj->dataBlock->numOfRows, obj->dataBlock->numOfCols);
}

static PyObject *UdfDataBlock_meta(PyObject *self, PyObject *args) {
  auto *obj = reinterpret_cast<UdfDataBlockPyObject *>(self);
  if (obj->dataBlock == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "invalid UdfDataBlock");
    return nullptr;
  }

  int col = 0;
  if (!PyArg_ParseTuple(args, "i", &col)) {
    return nullptr;
  }
  if (col < 0 || col >= obj->dataBlock->numOfCols) {
    PyErr_SetString(PyExc_IndexError, "column index out of range");
    return nullptr;
  }

  SUdfColumn *c = obj->dataBlock->udfCols[col];
  return Py_BuildValue("(iiii)", c->colMeta.type, c->colMeta.bytes, c->colMeta.scale, c->colMeta.precision);
}

static PyObject *UdfDataBlock_data(PyObject *self, PyObject *args) {
  auto *obj = reinterpret_cast<UdfDataBlockPyObject *>(self);
  if (obj->dataBlock == nullptr) {
    PyErr_SetString(PyExc_RuntimeError, "invalid UdfDataBlock");
    return nullptr;
  }

  int row = 0;
  int col = 0;
  if (!PyArg_ParseTuple(args, "ii", &row, &col)) {
    return nullptr;
  }

  if (row < 0 || row >= obj->dataBlock->numOfRows || col < 0 || col >= obj->dataBlock->numOfCols) {
    PyErr_SetString(PyExc_IndexError, "index out of range for data call");
    return nullptr;
  }

  SUdfColumn *c = obj->dataBlock->udfCols[col];
  if (udfColDataIsNull(c, row)) {
    Py_RETURN_NONE;
  }

  char *data = udfColDataGetData(c, row);
  switch (c->colMeta.type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return PyLong_FromLongLong(*(int64_t *)data);
    case TSDB_DATA_TYPE_TINYINT:
      return PyLong_FromLong(*(int8_t *)data);
    case TSDB_DATA_TYPE_UTINYINT:
      return PyLong_FromUnsignedLong(*(uint8_t *)data);
    case TSDB_DATA_TYPE_SMALLINT:
      return PyLong_FromLong(*(int16_t *)data);
    case TSDB_DATA_TYPE_USMALLINT:
      return PyLong_FromUnsignedLong(*(uint16_t *)data);
    case TSDB_DATA_TYPE_INT:
      return PyLong_FromLong(*(int32_t *)data);
    case TSDB_DATA_TYPE_UINT:
      return PyLong_FromUnsignedLong(*(uint32_t *)data);
    case TSDB_DATA_TYPE_BIGINT:
      return PyLong_FromLongLong(*(int64_t *)data);
    case TSDB_DATA_TYPE_UBIGINT:
      return PyLong_FromUnsignedLongLong(*(uint64_t *)data);
    case TSDB_DATA_TYPE_BOOL:
      return PyBool_FromLong(*(int8_t *)data ? 1 : 0);
    case TSDB_DATA_TYPE_FLOAT:
      return PyFloat_FromDouble(*(float *)data);
    case TSDB_DATA_TYPE_DOUBLE:
      return PyFloat_FromDouble(*(double *)data);
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_BINARY:
      return PyBytes_FromStringAndSize(varDataVal(data), varDataLen(data));
    default:
      PyErr_SetString(PyExc_TypeError, "unsupported python udf type");
      return nullptr;
  }
}

static PyMethodDef gUdfDataBlockMethods[] = {
    {"shape", UdfDataBlock_shape, METH_NOARGS, "Return block shape"},
    {"data", UdfDataBlock_data, METH_VARARGS, "Return one cell value"},
    {"meta", UdfDataBlock_meta, METH_VARARGS, "Return column meta"},
    {nullptr, nullptr, 0, nullptr},
};

static PyType_Slot gUdfDataBlockTypeSlots[] = {
    {Py_tp_methods, gUdfDataBlockMethods},
    {0, nullptr},
};

static PyType_Spec gUdfDataBlockTypeSpec = {
    "taospyudf.UdfDataBlock",
    sizeof(UdfDataBlockPyObject),
    0,
    Py_TPFLAGS_DEFAULT,
    gUdfDataBlockTypeSlots,
};

static int ensureUdfDataBlockType() {
  if (gPyUdfDataBlockType != nullptr) {
    return 0;
  }

  gPyUdfDataBlockType = PyType_FromSpec(&gUdfDataBlockTypeSpec);
  if (gPyUdfDataBlockType == nullptr) {
    return -1;
  }
  return 0;
}

static PyObject *newUdfDataBlockObject(SUdfDataBlock *block) {
  if (ensureUdfDataBlockType() != 0) {
    return nullptr;
  }

  PyObject *obj = PyObject_CallNoArgs(gPyUdfDataBlockType);
  if (obj == nullptr) {
    return nullptr;
  }
  auto *typed = reinterpret_cast<UdfDataBlockPyObject *>(obj);
  typed->dataBlock = block;
  return obj;
}

static int copyPyObjToVarColumn(SUdfColumn *col, int row, PyObject *obj, int8_t outputType) {
  const char *raw = nullptr;
  Py_ssize_t rawLen = 0;
  PyObject *tmpBytes = nullptr;

  if (PyBytes_Check(obj)) {
    if (PyBytes_AsStringAndSize(obj, const_cast<char **>(&raw), &rawLen) != 0) {
      return -1;
    }
  } else if (PyByteArray_Check(obj)) {
    raw = PyByteArray_AsString(obj);
    rawLen = PyByteArray_Size(obj);
  } else if (PyUnicode_Check(obj)) {
    const char *enc = (outputType == TSDB_DATA_TYPE_NCHAR) ? "utf-32-le" : "utf-8";
    tmpBytes = PyUnicode_AsEncodedString(obj, enc, "strict");
    if (tmpBytes == nullptr) {
      return -1;
    }
    if (PyBytes_AsStringAndSize(tmpBytes, const_cast<char **>(&raw), &rawLen) != 0) {
      Py_DECREF(tmpBytes);
      return -1;
    }
  } else {
    PyErr_SetString(PyExc_TypeError, "string output requires bytes/bytearray/str");
    return -1;
  }

  std::unique_ptr<char[]> var{new char[VARSTR_HEADER_SIZE + static_cast<size_t>(rawLen)]};
  varDataSetLen(var.get(), rawLen);
  memcpy(varDataVal(var.get()), raw, rawLen);
  udfColDataSet(col, row, var.get(), false);

  Py_XDECREF(tmpBytes);
  return 0;
}

static int copyPyObjToColumnRow(SUdfColumn *col, int row, PyObject *obj) {
  if (obj == Py_None) {
    udfColDataSetNull(col, row);
    return 0;
  }

  switch (col->colMeta.type) {
    case TSDB_DATA_TYPE_TIMESTAMP: {
      int64_t v = PyLong_AsLongLong(obj);
      if (PyErr_Occurred()) return -1;
      udfColDataSet(col, row, reinterpret_cast<char *>(&v), false);
      return 0;
    }
    case TSDB_DATA_TYPE_TINYINT: {
      long v = PyLong_AsLong(obj);
      if (PyErr_Occurred()) return -1;
      int8_t c = static_cast<int8_t>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      unsigned long v = PyLong_AsUnsignedLong(obj);
      if (PyErr_Occurred()) return -1;
      uint8_t c = static_cast<uint8_t>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      long v = PyLong_AsLong(obj);
      if (PyErr_Occurred()) return -1;
      int16_t c = static_cast<int16_t>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      unsigned long v = PyLong_AsUnsignedLong(obj);
      if (PyErr_Occurred()) return -1;
      uint16_t c = static_cast<uint16_t>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_INT: {
      long v = PyLong_AsLong(obj);
      if (PyErr_Occurred()) return -1;
      int32_t c = static_cast<int32_t>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_UINT: {
      unsigned long v = PyLong_AsUnsignedLong(obj);
      if (PyErr_Occurred()) return -1;
      uint32_t c = static_cast<uint32_t>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = PyLong_AsLongLong(obj);
      if (PyErr_Occurred()) return -1;
      udfColDataSet(col, row, reinterpret_cast<char *>(&v), false);
      return 0;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t v = PyLong_AsUnsignedLongLong(obj);
      if (PyErr_Occurred()) return -1;
      udfColDataSet(col, row, reinterpret_cast<char *>(&v), false);
      return 0;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      double v = PyFloat_AsDouble(obj);
      if (PyErr_Occurred()) return -1;
      float c = static_cast<float>(v);
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = PyFloat_AsDouble(obj);
      if (PyErr_Occurred()) return -1;
      udfColDataSet(col, row, reinterpret_cast<char *>(&v), false);
      return 0;
    }
    case TSDB_DATA_TYPE_BOOL: {
      int v = PyObject_IsTrue(obj);
      if (v < 0) return -1;
      int8_t c = v ? 1 : 0;
      udfColDataSet(col, row, reinterpret_cast<char *>(&c), false);
      return 0;
    }
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      return copyPyObjToVarColumn(col, row, obj, col->colMeta.type);
    default:
      PyErr_SetString(PyExc_TypeError, "unsupported python udf type");
      return -1;
  }
}

class PyUdf {
 public:
  explicit PyUdf(const SScriptUdfInfo *udfInfo)
      : name_(udfInfo->name),
        path_(udfInfo->path),
        funcType_(udfInfo->funcType),
        outputType_(udfInfo->outputType),
        outputLen_(udfInfo->outputLen),
        bufSize_(udfInfo->bufSize) {
    std::string baseFilename = path_.substr(path_.find_last_of("/\\") + 1);
    std::size_t p = baseFilename.find_last_of('.');
    std::string stem = baseFilename.substr(0, p);

    bool failed = false;
    int tryTimes = 0;
    do {
      failed = false;
      module_ = PyImport_ImportModule(stem.c_str());
      if (module_ == nullptr) {
        std::string err = fetchPyError();
        PLOGE << "py udf load module failure. udf=" << name_ << ", module=" << stem << ", err=" << err;
        failed = true;
        if (tryTimes++ <= 10) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } else {
          throw std::runtime_error("python import failed: " + err);
        }
      }
    } while (failed);

    PLOGI << "py udf import module succeeded, udf=" << name_ << ", module=" << stem;
  }

  virtual ~PyUdf() {
    Py_XDECREF(destroy_);
    Py_XDECREF(init_);
    Py_XDECREF(module_);
  }

  virtual void loadFunctions() {
    init_ = PyObject_GetAttrString(module_, "init");
    destroy_ = PyObject_GetAttrString(module_, "destroy");
    if (init_ == nullptr || destroy_ == nullptr) {
      throw std::runtime_error("missing init/destroy");
    }
  }

  virtual void init() {
    PyObject *ret = PyObject_CallNoArgs(init_);
    if (ret == nullptr) {
      throw std::runtime_error(fetchPyError());
    }
    Py_DECREF(ret);
  }

  virtual void destroy() {
    PyObject *ret = PyObject_CallNoArgs(destroy_);
    if (ret == nullptr) {
      throw std::runtime_error(fetchPyError());
    }
    Py_DECREF(ret);
  }

  static PyUdf *create(const SScriptUdfInfo *udfInfo);

 protected:
  PyObject *module_ = nullptr;
  PyObject *init_ = nullptr;
  PyObject *destroy_ = nullptr;

  std::string name_;
  std::string path_;
  EUdfFuncType funcType_;
  int8_t outputType_;
  int32_t outputLen_;
  int32_t bufSize_;
};

class PyScalarUdf : public PyUdf {
 public:
  explicit PyScalarUdf(const SScriptUdfInfo *udfInfo) : PyUdf(udfInfo) {}

  ~PyScalarUdf() override { Py_XDECREF(process_); }

  void loadFunctions() override {
    PyUdf::loadFunctions();
    process_ = PyObject_GetAttrString(module_, "process");
    if (process_ == nullptr) {
      throw std::runtime_error("missing process");
    }
  }

  int32_t scalarProc(SUdfDataBlock *block, SUdfColumn *resultCol) {
    PyObject *blockObj = newUdfDataBlockObject(block);
    if (blockObj == nullptr) {
      throw std::runtime_error(fetchPyError());
    }

    PyObject *ret = PyObject_CallFunctionObjArgs(process_, blockObj, nullptr);
    Py_DECREF(blockObj);
    if (ret == nullptr) {
      throw std::runtime_error(fetchPyError());
    }

    PyObject *seq = PySequence_Fast(ret, "scalar process must return sequence");
    Py_DECREF(ret);
    if (seq == nullptr) {
      throw std::runtime_error(fetchPyError());
    }

    const Py_ssize_t n = PySequence_Size(seq);
    if (n != block->numOfRows) {
      Py_DECREF(seq);
      throw std::runtime_error("python udf scalar function shall return each result for each row");
    }

    for (Py_ssize_t i = 0; i < n; ++i) {
      PyObject *item = PySequence_GetItem(seq, i);
      if (item == nullptr) {
        std::string err = fetchPyError();
        Py_DECREF(seq);
        throw std::runtime_error(err);
      }
      if (copyPyObjToColumnRow(resultCol, static_cast<int>(i), item) != 0) {
        std::string err = fetchPyError();
        Py_DECREF(item);
        Py_DECREF(seq);
        throw std::runtime_error(err);
      }
      Py_DECREF(item);
    }

    Py_DECREF(seq);
    resultCol->colData.numOfRows = block->numOfRows;
    return 0;
  }

 private:
  PyObject *process_ = nullptr;
};

static int setInterBufBytes(SUdfInterBuf *dst, PyObject *obj) {
  if (obj == Py_None) {
    dst->numOfResult = 0;
    return 0;
  }

  const char *raw = nullptr;
  Py_ssize_t rawLen = 0;
  PyObject *tmp = nullptr;

  if (PyBytes_Check(obj)) {
    if (PyBytes_AsStringAndSize(obj, const_cast<char **>(&raw), &rawLen) != 0) {
      return -1;
    }
  } else if (PyByteArray_Check(obj)) {
    raw = PyByteArray_AsString(obj);
    rawLen = PyByteArray_Size(obj);
  } else {
    PyErr_SetString(PyExc_TypeError, "aggregate state must be bytes/bytearray/None");
    return -1;
  }

  if (rawLen > dst->bufLen) {
    PyErr_SetString(PyExc_BufferError, "aggregate state exceeds udf buf size");
    Py_XDECREF(tmp);
    return -1;
  }
  memcpy(dst->buf, raw, static_cast<size_t>(rawLen));
  dst->bufLen = static_cast<int32_t>(rawLen);
  dst->numOfResult = 1;

  Py_XDECREF(tmp);
  return 0;
}

class PyAggUdf : public PyUdf {
 public:
  explicit PyAggUdf(const SScriptUdfInfo *udfInfo) : PyUdf(udfInfo) {}

  ~PyAggUdf() override {
    Py_XDECREF(start_);
    Py_XDECREF(reduce_);
    Py_XDECREF(merge_);
    Py_XDECREF(finish_);
  }

  void loadFunctions() override {
    PyUdf::loadFunctions();
    start_ = PyObject_GetAttrString(module_, "start");
    reduce_ = PyObject_GetAttrString(module_, "reduce");
    finish_ = PyObject_GetAttrString(module_, "finish");
    if (start_ == nullptr || reduce_ == nullptr || finish_ == nullptr) {
      throw std::runtime_error("missing aggregate functions");
    }

    merge_ = PyObject_GetAttrString(module_, "merge");
    if (merge_ == nullptr) {
      PyErr_Clear();
    }
  }

  void aggStart(SUdfInterBuf *buf) {
    PyObject *ret = PyObject_CallNoArgs(start_);
    if (ret == nullptr) {
      throw std::runtime_error(fetchPyError());
    }
    if (setInterBufBytes(buf, ret) != 0) {
      std::string err = fetchPyError();
      Py_DECREF(ret);
      throw std::runtime_error(err);
    }
    Py_DECREF(ret);
  }

  void aggProc(SUdfDataBlock *dataBlock, SUdfInterBuf *buf, SUdfInterBuf *newBuf) {
    PyObject *blockObj = newUdfDataBlockObject(dataBlock);
    if (blockObj == nullptr) {
      throw std::runtime_error(fetchPyError());
    }

    PyObject *bufObj = nullptr;
    if (buf->numOfResult == 0) {
      Py_INCREF(Py_None);
      bufObj = Py_None;
    } else {
      bufObj = PyBytes_FromStringAndSize(buf->buf, buf->bufLen);
      if (bufObj == nullptr) {
        Py_DECREF(blockObj);
        throw std::runtime_error(fetchPyError());
      }
    }

    PyObject *ret = PyObject_CallFunctionObjArgs(reduce_, blockObj, bufObj, nullptr);
    Py_DECREF(blockObj);
    Py_DECREF(bufObj);
    if (ret == nullptr) {
      throw std::runtime_error(fetchPyError());
    }

    if (setInterBufBytes(newBuf, ret) != 0) {
      std::string err = fetchPyError();
      Py_DECREF(ret);
      throw std::runtime_error(err);
    }
    Py_DECREF(ret);
  }

  void aggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData) {
    PyObject *bufObj = nullptr;
    if (buf->numOfResult == 0) {
      Py_INCREF(Py_None);
      bufObj = Py_None;
    } else {
      bufObj = PyBytes_FromStringAndSize(buf->buf, buf->bufLen);
      if (bufObj == nullptr) {
        throw std::runtime_error(fetchPyError());
      }
    }

    PyObject *ret = PyObject_CallFunctionObjArgs(finish_, bufObj, nullptr);
    Py_DECREF(bufObj);
    if (ret == nullptr) {
      throw std::runtime_error(fetchPyError());
    }

    if (ret == Py_None) {
      resultData->numOfResult = 0;
      Py_DECREF(ret);
      return;
    }

    switch (outputType_) {
      case TSDB_DATA_TYPE_TIMESTAMP: {
        int64_t v = PyLong_AsLongLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        memcpy(resultData->buf, &v, sizeof(v));
        resultData->bufLen = sizeof(v);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        long v = PyLong_AsLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        int8_t c = static_cast<int8_t>(v);
        memcpy(resultData->buf, &c, sizeof(c));
        resultData->bufLen = sizeof(c);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        unsigned long v = PyLong_AsUnsignedLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        uint8_t c = static_cast<uint8_t>(v);
        memcpy(resultData->buf, &c, sizeof(c));
        resultData->bufLen = sizeof(c);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        long v = PyLong_AsLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        int16_t c = static_cast<int16_t>(v);
        memcpy(resultData->buf, &c, sizeof(c));
        resultData->bufLen = sizeof(c);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        unsigned long v = PyLong_AsUnsignedLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        uint16_t c = static_cast<uint16_t>(v);
        memcpy(resultData->buf, &c, sizeof(c));
        resultData->bufLen = sizeof(c);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        long v = PyLong_AsLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        int32_t c = static_cast<int32_t>(v);
        memcpy(resultData->buf, &c, sizeof(c));
        resultData->bufLen = sizeof(c);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        unsigned long v = PyLong_AsUnsignedLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        uint32_t c = static_cast<uint32_t>(v);
        memcpy(resultData->buf, &c, sizeof(c));
        resultData->bufLen = sizeof(c);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t v = PyLong_AsLongLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        memcpy(resultData->buf, &v, sizeof(v));
        resultData->bufLen = sizeof(v);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t v = PyLong_AsUnsignedLongLong(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        memcpy(resultData->buf, &v, sizeof(v));
        resultData->bufLen = sizeof(v);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        double v = PyFloat_AsDouble(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        float f = static_cast<float>(v);
        memcpy(resultData->buf, &f, sizeof(f));
        resultData->bufLen = sizeof(f);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double v = PyFloat_AsDouble(ret);
        if (PyErr_Occurred()) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        memcpy(resultData->buf, &v, sizeof(v));
        resultData->bufLen = sizeof(v);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_BOOL: {
        int v = PyObject_IsTrue(ret);
        if (v < 0) {
          std::string err = fetchPyError();
          Py_DECREF(ret);
          throw std::runtime_error(err);
        }
        int8_t b = v ? 1 : 0;
        memcpy(resultData->buf, &b, sizeof(b));
        resultData->bufLen = sizeof(b);
        resultData->numOfResult = 1;
        break;
      }
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR: {
        const char *raw = nullptr;
        Py_ssize_t rawLen = 0;
        PyObject *tmpBytes = nullptr;

        if (PyBytes_Check(ret)) {
          if (PyBytes_AsStringAndSize(ret, const_cast<char **>(&raw), &rawLen) != 0) {
            std::string err = fetchPyError();
            Py_DECREF(ret);
            throw std::runtime_error(err);
          }
        } else if (PyUnicode_Check(ret)) {
          const char *enc = (outputType_ == TSDB_DATA_TYPE_NCHAR) ? "utf-32-le" : "utf-8";
          tmpBytes = PyUnicode_AsEncodedString(ret, enc, "strict");
          if (tmpBytes == nullptr ||
              PyBytes_AsStringAndSize(tmpBytes, const_cast<char **>(&raw), &rawLen) != 0) {
            std::string err = fetchPyError();
            Py_XDECREF(tmpBytes);
            Py_DECREF(ret);
            throw std::runtime_error(err);
          }
        } else {
          Py_DECREF(ret);
          throw std::runtime_error("string output requires bytes or str");
        }

        varDataSetLen(resultData->buf, rawLen);
        memcpy(varDataVal(resultData->buf), raw, rawLen);
        resultData->bufLen = static_cast<int32_t>(rawLen) + VARSTR_HEADER_SIZE;
        resultData->numOfResult = 1;
        Py_XDECREF(tmpBytes);
        break;
      }
      default:
        Py_DECREF(ret);
        throw std::runtime_error("unsupported python udf output type");
    }

    Py_DECREF(ret);
  }

 private:
  PyObject *start_ = nullptr;
  PyObject *reduce_ = nullptr;
  PyObject *merge_ = nullptr;
  PyObject *finish_ = nullptr;
};

PyUdf *PyUdf::create(const SScriptUdfInfo *udfInfo) {
  if (udfInfo->funcType == UDF_FUNC_TYPE_AGG) {
    return new PyAggUdf(udfInfo);
  }
  if (udfInfo->funcType == UDF_FUNC_TYPE_SCALAR) {
    return new PyScalarUdf(udfInfo);
  }
  throw std::invalid_argument("udf type not supported");
}

int32_t doPyUdfInit(SScriptUdfInfo *udf, void **pUdfCtx) {
  PLOGI << "python udf init begin. name=" << udf->name << ", path=" << udf->path;
  try {
    PyUdf *pyUdf = PyUdf::create(udf);
    pyUdf->loadFunctions();
    pyUdf->init();
    *pUdfCtx = pyUdf;
  } catch (std::exception &e) {
    PLOGE << "call pyUdf init function failed. name=" << udf->name << ", err=" << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfDestroy(void *udfCtx) {
  try {
    PyUdf *pyUdf = static_cast<PyUdf *>(udfCtx);
    pyUdf->destroy();
    delete pyUdf;
  } catch (std::exception &e) {
    PLOGE << "call pyUdf destroy function failed. err=" << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfScalarProc(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx) {
  try {
    auto *pyScalarUdf = dynamic_cast<PyScalarUdf *>(static_cast<PyUdf *>(udfCtx));
    if (pyScalarUdf == nullptr) {
      return TSDB_UDF_PYTHON_EXEC_FAILURE;
    }
    return pyScalarUdf->scalarProc(block, resultCol);
  } catch (std::exception &e) {
    PLOGE << "call pyUdfScalar proc function failed. err=" << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
}

int32_t doPyUdfAggStart(SUdfInterBuf *buf, void *udfCtx) {
  try {
    auto *pyAggUdf = dynamic_cast<PyAggUdf *>(static_cast<PyUdf *>(udfCtx));
    if (pyAggUdf == nullptr) {
      return TSDB_UDF_PYTHON_EXEC_FAILURE;
    }
    pyAggUdf->aggStart(buf);
  } catch (std::exception &e) {
    PLOGE << "call pyAggUdf start function failed. err=" << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfAggProc(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf, void *udfCtx) {
  try {
    auto *pyAggUdf = dynamic_cast<PyAggUdf *>(static_cast<PyUdf *>(udfCtx));
    if (pyAggUdf == nullptr) {
      return TSDB_UDF_PYTHON_EXEC_FAILURE;
    }
    pyAggUdf->aggProc(block, interBuf, newInterBuf);
  } catch (std::exception &e) {
    PLOGE << "call pyAggUdf proc function failed. err=" << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfAggMerge(SUdfInterBuf *, SUdfInterBuf *, SUdfInterBuf *, void *) { return 0; }

int32_t doPyUdfAggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx) {
  try {
    auto *pyAggUdf = dynamic_cast<PyAggUdf *>(static_cast<PyUdf *>(udfCtx));
    if (pyAggUdf == nullptr) {
      return TSDB_UDF_PYTHON_EXEC_FAILURE;
    }
    pyAggUdf->aggFinish(buf, resultData);
  } catch (std::exception &e) {
    PLOGE << "call pyAggUdf finish function failed. err=" << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

std::vector<std::string> resplit(const std::string &s, const std::regex &sep_regex = std::regex{"\\s+"}) {
  std::sregex_token_iterator iter(s.begin(), s.end(), sep_regex, -1);
  std::sregex_token_iterator end;
  return {iter, end};
}

int32_t doPyOpen(SScriptUdfEnvItem *items, int numItems) {
  PLOGI << "python udf plugin open. numItems=" << numItems;
  if (Py_IsInitialized() == 1) {
    PLOGE << "python udf plugin open rejected: interpreter already initialized";
    return TSDB_UDF_PYTHON_WRONG_STATE;
  }

  Py_Initialize();
  if (Py_IsInitialized() == 0) {
    PLOGE << "python udf plugin open failed: Py_Initialize failed";
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }

  if (ensureUdfDataBlockType() != 0) {
    PLOGE << "python udf plugin open failed to prepare UdfDataBlock type: " << fetchPyError();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }

  PyObject *pySys = PyImport_ImportModule("sys");
  if (pySys == nullptr) {
    PLOGE << "python udf plugin open failed importing sys: " << fetchPyError();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }

  PyObject *sysPath = PyObject_GetAttrString(pySys, "path");
  if (sysPath == nullptr || !PyList_Check(sysPath)) {
    Py_XDECREF(sysPath);
    Py_DECREF(pySys);
    PLOGE << "python udf plugin open failed to get sys.path: " << fetchPyError();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }

  for (int i = 0; i < numItems; ++i) {
    if (items[i].name != nullptr && items[i].value != nullptr &&
        std::string_view(items[i].name) == std::string_view("PYTHONPATH")) {
#ifdef _WIN32
      static const std::regex pathSep(";");
#else
      static const std::regex pathSep(":");
#endif
      auto paths = resplit(std::string(items[i].value), pathSep);
      for (const auto &p : paths) {
        PyObject *v = PyUnicode_FromString(p.c_str());
        if (v == nullptr || PyList_Append(sysPath, v) != 0) {
          Py_XDECREF(v);
          Py_DECREF(sysPath);
          Py_DECREF(pySys);
          PLOGE << "python udf plugin open failed to append sys.path: " << fetchPyError();
          return TSDB_UDF_PYTHON_EXEC_FAILURE;
        }
        Py_DECREF(v);
      }
    }
  }

  Py_DECREF(sysPath);
  Py_DECREF(pySys);
  return 0;
}

int32_t doPyClose() {
  if (Py_IsInitialized() == 0) {
    return TSDB_UDF_PYTHON_WRONG_STATE;
  }

  Py_XDECREF(gPyUdfDataBlockType);
  gPyUdfDataBlockType = nullptr;
  Py_Finalize();
  return 0;
}

class ThreadPool {
  using task_type = std::function<void()>;

 public:
  explicit ThreadPool(size_t num = std::thread::hardware_concurrency()) {
    for (size_t i = 0; i < num; ++i) {
      workers_.emplace_back(std::thread([this] {
        while (true) {
          task_type task;
          {
            std::unique_lock<std::mutex> lock(task_mutex_);
            task_cond_.wait(lock, [this] { return !tasks_.empty(); });
            task = std::move(tasks_.front());
            tasks_.pop();
          }
          if (!task) {
            PLOGI << "worker #" << std::this_thread::get_id() << " exited";
            push_stop_task();
            return;
          }
          task();
        }
      }));
      PLOGI << "python udf worker #" << workers_.back().get_id() << " started";
    }
  }

  ~ThreadPool() { stop(); }

  void stop() {
    push_stop_task();
    for (auto &worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }

    std::queue<task_type> empty{};
    std::swap(tasks_, empty);
  }

  template <typename F, typename... Args>
  auto enqueue(F &&f, Args &&...args) {
    using return_type = std::invoke_result_t<F, Args...>;
    auto task =
        std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    auto res = task->get_future();

    {
      std::lock_guard<std::mutex> lock(task_mutex_);
      tasks_.emplace([task]() { (*task)(); });
    }
    task_cond_.notify_one();

    return res;
  }

 private:
  void push_stop_task() {
    std::lock_guard<std::mutex> lock(task_mutex_);
    tasks_.push(task_type{});
    task_cond_.notify_one();
  }

  std::vector<std::thread> workers_;
  std::queue<task_type> tasks_;
  std::mutex task_mutex_;
  std::condition_variable task_cond_;
};

static ThreadPool *pythonCaller = nullptr;

int32_t pyUdfInit(SScriptUdfInfo *udf, void **pUdfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfInit, udf, pUdfCtx);
  return f.get();
}

int32_t pyUdfDestroy(void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfDestroy, udfCtx);
  return f.get();
}

int32_t pyUdfScalarProc(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfScalarProc, block, resultCol, udfCtx);
  return f.get();
}

int32_t pyUdfAggStart(SUdfInterBuf *buf, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggStart, buf, udfCtx);
  return f.get();
}

int32_t pyUdfAggProc(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggProc, block, interBuf, newInterBuf, udfCtx);
  return f.get();
}

int32_t pyUdfAggMerge(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggMerge, inputBuf1, inputBuf2, outputBuf, udfCtx);
  return f.get();
}

int32_t pyUdfAggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggFinish, buf, resultData, udfCtx);
  return f.get();
}

#if defined(__APPLE__)
#  define TAOSPYUDF_SELF_DSO "libtaospyudf.dylib"
#elif !defined(_WIN32)
#  define TAOSPYUDF_SELF_DSO "libtaospyudf.so"
#endif

int32_t pyOpen(SScriptUdfEnvItem *items, int numItems) {
#ifdef TAOSPYUDF_SELF_DSO
  std::string selfDsoErr;
  const char *selfDsoTarget = TAOSPYUDF_SELF_DSO;
  Dl_info dlInfo{};
  static const char selfDsoAnchor = 0;
  if (dladdr(const_cast<void *>(static_cast<const void *>(&selfDsoAnchor)), &dlInfo) != 0 &&
      dlInfo.dli_fname != nullptr) {
    selfDsoTarget = dlInfo.dli_fname;
  }
  if (dlopen(selfDsoTarget, RTLD_LAZY | RTLD_GLOBAL) == nullptr) {
    const char *e = dlerror();
    selfDsoErr = std::string("dlopen(") + selfDsoTarget + "): " + (e ? e : "unknown error");
  }
#endif

  std::error_code ec;
  std::filesystem::path logDir = std::filesystem::temp_directory_path(ec);
  if (ec || logDir.empty()) {
#ifdef _WIN32
    logDir = ".";
#else
    logDir = "/tmp";
#endif
  }
  for (int i = 0; i < numItems; ++i) {
    if (std::string_view(items[i].name) == std::string_view("LOGDIR")) {
      if (items[i].value) {
        logDir = items[i].value;
        break;
      }
    }
  }

  std::filesystem::path logPath = logDir / "taospyudf.log";
  plog::init(plog::info, logPath.c_str(), 50 * 1024 * 1024, 5);

#ifdef TAOSPYUDF_SELF_DSO
  if (!selfDsoErr.empty()) {
    PLOGE << selfDsoErr << " - Python C extensions may fail to resolve our symbols";
  } else {
    PLOGI << "taospyudf self-dlopen succeeded: " << selfDsoTarget;
  }
#endif

  if (pythonCaller == nullptr) {
    pythonCaller = new ThreadPool(1);
  }

  auto f = pythonCaller->enqueue(doPyOpen, items, numItems);
  return f.get();
}

int32_t pyClose() {
  auto f = pythonCaller->enqueue(doPyClose);
  int32_t ret = f.get();
  delete pythonCaller;
  pythonCaller = nullptr;
  PLOGI << "taos python udf plugin close";
  return ret;
}
