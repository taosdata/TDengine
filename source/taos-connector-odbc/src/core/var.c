/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "internal.h"

#include "var.h"

#include "log.h"


static var_t* _var__add(var_t *args)
{
  (void)args;
  return NULL;
}

var_eval_f var_get_eval(const char *name, size_t nr)
{
#define RECORD(x) {#x, _var__##x}
  static const struct {
    const char        *name;
    var_eval_f         eval;
  } _evals [] = {
    RECORD(add),
  };
  size_t _nr_evals = sizeof(_evals) / sizeof(_evals[0]);
#undef RECORD

  for (size_t i=0; i<_nr_evals; ++i) {
    const char       *_name   = _evals[i].name;
    var_eval_f        _eval   = _evals[i].eval;
    if (nr == strlen(_name) && 0 == tod_strncasecmp(_name, name, nr)) {
      return _eval;
    }
  }

  return NULL;
}

static void _var_release_arr(var_t *v)
{
  for (size_t i=0; i<v->arr.nr; ++i) {
    var_t *val = v->arr.vals[i];
    var_release(val);
    TOD_SAFE_FREE(v->arr.vals[i]);
  }
  TOD_SAFE_FREE(v->arr.vals);
  v->arr.cap = 0;
  v->arr.nr  = 0;
}

static void _var_release_eval(var_t *v)
{
  var_release(v->exp.args);
  TOD_SAFE_FREE(v->exp.args);
  v->exp.eval = NULL;
}

void var_release(var_t *v)
{
  if (!v) return;
  switch (v->type) {
    case VAR_NULL:
    case VAR_BOOL:
    case VAR_INT8:
    case VAR_INT16:
    case VAR_INT32:
    case VAR_INT64:
    case VAR_UINT8:
    case VAR_UINT16:
    case VAR_UINT32:
    case VAR_UINT64:
      break;
    case VAR_FLOAT:
      str_release(&v->flt.s);
      break;
    case VAR_DOUBLE:
      str_release(&v->dbl.s);
      break;
    case VAR_ID:
    case VAR_STRING:
      str_release(&v->str.s);
      break;
    case VAR_PARAM:
    case VAR_ARR:
      _var_release_arr(v);
      break;
    case VAR_EVAL:
      _var_release_eval(v);
      break;
    default:
      OA_NIY(0);
      break;
  }
  v->type = VAR_NULL;
}

var_t* var_add(var_t *args)
{
  (void)args;
  return NULL;
}

var_t* var_sub(var_t *args)
{
  (void)args;
  return NULL;
}

var_t* var_mul(var_t *args)
{
  (void)args;
  return NULL;
}

var_t* var_div(var_t *args)
{
  (void)args;
  return NULL;
}

var_t* var_neg(var_t *args)
{
  (void)args;
  return NULL;
}

