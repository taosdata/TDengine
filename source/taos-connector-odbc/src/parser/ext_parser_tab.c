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

#include "ext_parser.h"

#include "../core/internal.h"        // FIXME:
#include "insert_eval.h"
#include "log.h"
#include "parser.h"
#include "topic.h"
#include "var.h"

#include "ext_parser.tab.h"
#include "ext_parser.lex.c"

#include "ext_parser.lex.h"
#undef yylloc
#undef yylval
#include "ext_parser.tab.c"

void insert_eval_release(insert_eval_t *eval)
{
  if (!eval) return;
  _var_destroy(eval->table_db);      eval->table_db       = NULL;
  _var_destroy(eval->table_tbl);     eval->table_tbl      = NULL;
  _var_destroy(eval->super_db);      eval->super_db       = NULL;
  _var_destroy(eval->super_tbl);     eval->super_tbl      = NULL;
  _var_destroy(eval->tag_names);     eval->tag_names      = NULL;
  _var_destroy(eval->tag_vals);      eval->tag_vals       = NULL;
  _var_destroy(eval->col_names);     eval->col_names      = NULL;
  _var_destroy(eval->col_vals);      eval->col_vals       = NULL;
}

int8_t insert_eval_nr_tags(insert_eval_t *eval)
{
  if (!eval->tag_names) return 0;
  size_t nr = eval->tag_names->arr.nr;
  OA_ILE(nr <= INT8_MAX);
  return (int8_t)nr;
}

int8_t insert_eval_nr_cols(insert_eval_t *eval)
{
  if (!eval->col_names) return 0;
  size_t nr = eval->col_names->arr.nr;
  OA_ILE(nr <= INT8_MAX);
  return (int8_t)nr;
}

