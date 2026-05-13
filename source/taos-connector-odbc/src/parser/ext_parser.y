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

%code top {
}

%code top {
    // here to include header files required for generated code
}

%code requires {
    #define YYSTYPE       EXT_PARSER_YYSTYPE
    #define YYLTYPE       EXT_PARSER_YYLTYPE
    #ifndef YY_TYPEDEF_YY_SCANNER_T
    #define YY_TYPEDEF_YY_SCANNER_T
    typedef void* yyscan_t;
    #endif
}

%code provides {
}

%code {
    // generated header from flex
    // introduce yylex decl for later use
    static void yyerror(
        YYLTYPE *yylloc,                   // match %define locations
        yyscan_t arg,                      // match %param
        ext_parser_param_t *param,         // match %parse-param
        const char *errsg
    );

    static int ext_parser_param_append_topic_name(ext_parser_param_t *param, const char *name, size_t len);
    static int ext_parser_param_append_topic_conf(ext_parser_param_t *param, const char *k, size_t kn, const char *v, size_t vn);

    #define SET_TOPIC(_v, _loc) do {                                           \
      if (!param) break;                                                       \
      if (ext_parser_param_append_topic_name(param, _v.text, _v.leng)) {       \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                  \
        YYABORT;                                                               \
      }                                                                        \
      param->is_topic = 1;                                                     \
    } while (0)

    #define SET_TOPIC_KEY(_k, _loc) do {                                                        \
      if (!param) break;                                                                        \
      if (ext_parser_param_append_topic_conf(param, _k.text, _k.leng, NULL, 0)) {               \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    #define SET_TOPIC_KEY_VAL(_k, _v, _loc) do {                                                \
      if (!param) break;                                                                        \
      if (ext_parser_param_append_topic_conf(param, _k.text, _k.leng, _v.text, _v.leng)) {      \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                                   \
        YYABORT;                                                                                \
      }                                                                                         \
    } while (0)

    static inline void _var_destroy(var_t *v)
    {
      var_release(v);
      TOD_SAFE_FREE(v);
    }

    void ext_parser_param_release(ext_parser_param_t *param)
    {
      if (!param) return;
      if (param->is_topic) topic_cfg_release(&param->topic_cfg);
      else                 insert_eval_release(&param->insert_eval);
      param->ctx.err_msg[0] = '\0';
    }

    static inline var_t* _create_var(var_e type, LOG_ARGS)
    {
      var_t *v = (var_t*)calloc(1, sizeof(*v));
      if (v) {
        v->type = type;
        return v;
      }
      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      return NULL;
    }

    static inline int _insert_eval_set_table_qm(insert_eval_t *insert_eval, LOG_ARGS)
    {
      var_t *v = _create_var(VAR_PARAM, LOG_VALS, yylloc);
      if (!v) return -1;
      insert_eval->table_tbl = v;
      return 0;
    }

    #define SET_INSERT_TBL_QM(_loc) do {                                       \
      int r = 0;                                                               \
      r = _insert_eval_set_table_qm(&param->insert_eval, LOG_MALS, &_loc);     \
      if (r) {                                                                 \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                  \
        YYABORT;                                                               \
      }                                                                        \
      param->insert_eval.tbl_param = 1;                                        \
      param->is_topic  = 0;                                                    \
    } while (0)

    #define SET_INSERT_TBL(_tbl, _loc) do {                                    \
      param->insert_eval.table_tbl = _tbl;                                     \
      param->insert_eval.tbl_param = 0;                                        \
      param->is_topic  = 0;                                                    \
    } while (0)

    #define SET_INSERT_DB_TBL(_db, _tbl, _loc) do {                            \
      param->insert_eval.table_db  = _db;                                      \
      param->insert_eval.table_tbl = _tbl;                                     \
      param->insert_eval.tbl_param = 0;                                        \
      param->is_topic  = 0;                                                    \
    } while (0)

    static inline var_t* _create_id(const char *s, size_t n, LOG_ARGS)
    {
      var_t *v = _create_var(VAR_ID, LOG_VALS, yylloc);
      if (!v) return NULL;
      int r = str_append(&v->str.s, s, n);
      if (r) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        return NULL;
      }
      return v;
    }

    #define CREATE_ID(_r, _ID, _loc) do {                                      \
      _r = _create_id(_ID.text, _ID.leng, LOG_MALS, &_loc);                    \
      if (!_r) YYABORT;                                                        \
    } while (0)

    #define SET_INSERT_SUPER_TBL(_tbl, _loc) do {                              \
      param->insert_eval.super_tbl = _tbl;                                     \
    } while (0)

    #define SET_INSERT_SUPER_DB_TBL(_db, _tbl, _loc) do {                      \
      param->insert_eval.super_db  = _db;                                      \
      param->insert_eval.super_tbl = _tbl;                                     \
    } while (0)

    #define SET_INSERT_TAGS(_names, _vals, _loc) do {                          \
      param->insert_eval.tag_names = _names;                                   \
      param->insert_eval.tag_vals  = _vals;                                    \
      if (!_names) break;                                                      \
      if (_names->arr.nr == _vals->arr.nr) break;                              \
      YLOG(LOG_MALS, &_loc,                                                    \
          "syntax error:"                                                      \
          "# of tags [%zd] does not match # of tagvals [%zd]",                 \
          _names->arr.nr, _vals->arr.nr);                                      \
      YYABORT;                                                                 \
    } while (0)

    #define SET_INSERT_COLS(_names, _vals, _loc) do {                          \
      param->insert_eval.col_names = _names;                                   \
      param->insert_eval.col_vals  = _vals;                                    \
      if (!_names) break;                                                      \
      if (_names->arr.nr == _vals->arr.nr) break;                              \
      YLOG(LOG_MALS, &_loc,                                                    \
          "syntax error:"                                                      \
          "# of cols [%zd] does not match # of colvals [%zd]",                 \
          _names->arr.nr, _vals->arr.nr);                                      \
      YYABORT;                                                                 \
    } while (0)

    static inline int _arr_append_v(var_t *arr, var_t *v, LOG_ARGS)
    {
      OA_NIY(arr->type == VAR_ARR);
      if (arr->arr.nr + 1 > arr->arr.cap) {
        size_t cap = (arr->arr.nr + 1 + 15) / 16 * 16;
        var_t **p = (var_t**)realloc(arr->arr.vals, cap * sizeof(*p));
        if (!p) {
          YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
          return -1;
        }
        arr->arr.vals      = p;
        arr->arr.cap       = cap;
      }
      arr->arr.vals[arr->arr.nr] = v;
      arr->arr.nr += 1;
      return 0;
    }

    static inline int _ids_append(var_t *ids, var_t *id, LOG_ARGS)
    {
      int r = 0;
      r = _arr_append_v(ids, id, LOG_VALS, yylloc);
      if (r == 0) return 0;
      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      _var_destroy(id);
      return -1;
    }

    static inline var_t* _create_ids(var_t *id, LOG_ARGS)
    {
      var_t *ids = _create_var(VAR_ARR, LOG_VALS, yylloc);
      if (!ids) return NULL;

      int r = _ids_append(ids, id, LOG_VALS, yylloc);
      if (r == 0) return ids;

      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      _var_destroy(ids);
      _var_destroy(id);
      return NULL;
    }

    #define CREATE_IDS(_r, _id, _loc) do {                                     \
      _r = _create_ids(_id, LOG_MALS, &_loc);                                  \
      if (!_r) YYABORT;                                                        \
    } while (0)

    #define IDS_APPEND(_r, _ids, _id, _loc) do {                               \
      int r = _ids_append(_ids, _id, LOG_MALS, &_loc);                         \
      if (r) {                                                                 \
        _var_destroy(_ids);                                                    \
        YYABORT;                                                               \
      }                                                                        \
      _r = _ids;                                                               \
    } while (0)

    static inline var_t* _create_exps(var_t *exp, LOG_ARGS)
    {
      var_t *exps = _create_var(VAR_ARR, LOG_VALS, yylloc);
      if (!exps) return NULL;

      int r = _arr_append_v(exps, exp, LOG_VALS, yylloc);
      if (r == 0) return exps;

      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      _var_destroy(exps);
      return NULL;
    }

    #define CREATE_EXPS(_r, _exp, _loc) do {                                   \
      _r = _create_exps(_exp, LOG_MALS, &_loc);                                \
      if (!_r) {                                                               \
        _var_destroy(_exp);                                                    \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                  \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)

    #define EXPS_APPEND(_r, _exps, _exp, _loc) do {                            \
      int r = _arr_append_v(_exps, _exp, LOG_MALS, &_loc);                     \
      if (r) {                                                                 \
        _var_destroy(_exps);                                                   \
        _var_destroy(_exp);                                                    \
        YYABORT;                                                               \
      }                                                                        \
      _r = _exps;                                                              \
    } while (0)

    static inline var_t* _create_exp(var_eval_f eval, var_t *e1, var_t *e2, LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_EVAL, LOG_VALS, yylloc);
      if (!exp) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        return NULL;
      }
      exp->exp.eval = eval;
      int r = 0;
      exp->exp.args = _create_var(VAR_ARR, LOG_VALS, yylloc);
      if (!exp->exp.args) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        r = -1;
      }
      if (r == 0) r = _arr_append_v(exp->exp.args, e1, LOG_VALS, yylloc);
      if (r == 0) r = _arr_append_v(exp->exp.args, e2, LOG_VALS, yylloc);
      if (r == 0) return exp;
      _var_destroy(exp);
      return NULL;
    }

    #define CREATE_EXP(_r, _f, _e1, _e2, _loc) do {                            \
      _r = _create_exp(_f, _e1, _e2, LOG_MALS, &_loc);                         \
      if (!_r) {                                                               \
        _var_destroy(_e1);                                                     \
        _var_destroy(_e2);                                                     \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                  \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)

    static inline var_t* _create_exp_neg(var_eval_f eval, var_t *e, LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_EVAL, LOG_VALS, yylloc);
      if (!exp) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        return NULL;
      }
      exp->exp.eval = eval;
      int r = 0;
      exp->exp.args = _create_var(VAR_ARR, LOG_VALS, yylloc);
      if (!exp->exp.args) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        r = -1;
      }
      if (r == 0) r = _arr_append_v(exp->exp.args, e, LOG_VALS, yylloc);
      if (r == 0) return exp;
      _var_destroy(exp);
      return NULL;
    }

    #define CREATE_EXP_NEG(_r, _f, _e, _loc) do {                              \
      _r = _create_exp_neg(_f, _e, LOG_MALS, &_loc);                           \
      if (!_r) {                                                               \
        _var_destroy(_e);                                                      \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)

    static inline var_t* _create_exp_by_name(var_eval_f eval, var_t *args, LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_EVAL, LOG_VALS, yylloc);
      if (!exp) return NULL;
      exp->exp.eval = eval;
      exp->exp.args = args;
      return exp;
    }

    #define CREATE_EXP_BY_NAME(_r, _f, _args, _loc) do {                       \
      _r = _create_exp_by_name(_f, _args, LOG_MALS, &_loc);                    \
      if (!_r) {                                                               \
        _var_destroy(_args);                                                   \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)

    static inline var_t* _create_exp_qm(LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_PARAM, LOG_VALS, yylloc);
      if (!exp) return NULL;
      return exp;
    }

    #define CREATE_EXP_QM(_r, _loc) do {                                       \
      _r = _create_exp_qm(LOG_MALS, &_loc);                                    \
      if (!_r) YYABORT;                                                        \
      param->insert_eval.nr_params += 1;                                       \
    } while (0)

    static inline var_t* _create_exp_id(const char *s, size_t n, LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_ID, LOG_VALS, yylloc);
      if (!exp) return NULL;
      int r = str_append(&exp->str.s, s, n);
      if (r == 0) return exp;
      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      _var_destroy(exp);
      return NULL;
    }

    #define CREATE_EXP_ID(_r, _ID, _loc) do {                                  \
      _r = _create_exp_id(_ID.text, _ID.leng, LOG_MALS, &_loc);                \
      if (!_r) YYABORT;                                                        \
    } while (0)

    static inline int _var_set_integral(var_t *exp, const char *s, size_t n, LOG_ARGS)
    {
      n = strnlen(s, n);
      char buf[1024]; buf[0] = '\0';
      int r = snprintf(buf, sizeof(buf), "%.*s", (int)n, s);
      if (r < 0 || (size_t)r >= sizeof(buf)) {
        YLOG(LOG_VALS, yylloc, "runtime error:buffer too small or internal logic error");
        return -1;
      }

      char *end = NULL;
      long long ll = strtoll(buf, &end, 0);
      int e = errno;
      if (*end) {
        YLOG(LOG_VALS, yylloc, "runtime error:conversion failure");
        return -1;
      }
      if (e == ERANGE) {
        end = NULL;
        unsigned long long ull = strtoull(buf, &end, 0);
        e = errno;
        if (*end || e == ERANGE) {
          YLOG(LOG_VALS, yylloc, "runtime error:conversion failure");
          return -1;
        }
        var_release(exp);
        exp->type = VAR_UINT64;
        exp->u64  = ull;
        return 0;
      }
      var_release(exp);
      exp->type = VAR_INT64;
      exp->u64  = ll;
      return 0;
    }

    static inline var_t* _create_exp_integral(const char *s, size_t n, LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_NULL, LOG_VALS, yylloc);
      if (!exp) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        return NULL;
      }
      int r = _var_set_integral(exp, s, n, LOG_VALS, yylloc);
      if (r == 0) return exp;
      _var_destroy(exp);
      return NULL;
    }

    #define CREATE_EXP_INTEGRAL(_r, _I, _loc) do {                             \
      _r = _create_exp_integral(_I.text, _I.leng, LOG_MALS, &_loc);            \
      if (!_r) YYABORT;                                                        \
    } while (0)

    static inline int _var_set_number(var_t *exp, const char *s, size_t n, LOG_ARGS)
    {
      n = strnlen(s, n);
      char buf[1024]; buf[0] = '\0';
      int r = snprintf(buf, sizeof(buf), "%.*s", (int)n, s);
      if (r < 0 || (size_t)r >= sizeof(buf)) {
        YLOG(LOG_VALS, yylloc, "runtime error:buffer too small or internal logic error");
        return -1;
      }

      double dbl = 0.;
      int    end = 0;
      r = sscanf(buf, "%lg%n", &dbl, &end);
      if (r != 1 || (size_t)end != n) {
        YLOG(LOG_VALS, yylloc, "runtime error:conversion failure");
        return -1;
      }
      var_release(exp);
      exp->type   = VAR_DOUBLE;
      exp->dbl.v  = dbl;
      return 0;
    }

    static inline var_t* _create_exp_number(const char *s, size_t n, LOG_ARGS)
    {
      var_t *exp = _create_var(VAR_NULL, LOG_VALS, yylloc);
      if (!exp) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        return NULL;
      }
      int r = _var_set_number(exp, s, n, LOG_VALS, yylloc);
      if (r == 0) return exp;
      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      _var_destroy(exp);
      return NULL;
    }

    #define CREATE_EXP_NUMBER(_r, _N, _loc) do {                               \
      _r = _create_exp_number(_N.text, _N.leng, LOG_MALS, &_loc);              \
      if (!_r) {                                                               \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                  \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)

    static inline var_t* _str_empty(var_quote_e qt, LOG_ARGS)
    {
      var_t *v = _create_var(VAR_STRING, LOG_VALS, yylloc);
      if (!v) return NULL;
      v->str.q = qt;
      return v;
    }

    static inline var_t* _strs_flat(var_t *ss, var_quote_e qt, LOG_ARGS)
    {
      var_t *v = _create_var(VAR_STRING, LOG_VALS, yylloc);
      if (!v) return NULL;
      v->str.q = qt;
      size_t n = 0;
      for (size_t i=0; i<ss->arr.nr; ++i) {
        str_t *str = &ss->arr.vals[i]->str.s;
        n += str->nr;
      }
      int r = str_keep(&v->str.s, n + 1);

      if (r == 0) {
        for (size_t i=0; i<ss->arr.nr; ++i) {
          str_t *str = &ss->arr.vals[i]->str.s;
          r = str_append(&v->str.s, str->str, str->nr);
          if (r) break;
        }
      }

      if (r == 0) return v;

      var_release(v);
      TOD_SAFE_FREE(v);

      YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
      return NULL;
    }

    #define STR_EMPTY(_r, _qt, _loc) do {                                      \
      _r = _str_empty(_qt, LOG_MALS, &_loc);                                   \
      if (!_r) YYABORT;                                                        \
    } while (0)

    #define STRS_FLAT(_r, _ss, _qt, _loc) do {                                 \
      _r = _strs_flat(_ss, _qt, LOG_MALS, &_loc);                              \
      var_release(_ss);                                                        \
      TOD_SAFE_FREE(_ss);                                                      \
      if (!_r) YYABORT;                                                        \
    } while (0)

    static inline int _strs_append(var_t *ss, var_t *s, LOG_ARGS)
    {
      if (ss->arr.nr + 1 > ss->arr.cap) {
        size_t cap = (ss->arr.nr + 1 + 15) / 16 * 16;
        var_t **p = (var_t**)realloc(ss->arr.vals, cap * sizeof(*p));
        if (!p) {
          YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
          return -1;
        }
        ss->arr.vals    = p;
        ss->arr.cap     = cap;
      }

      ss->arr.vals[ss->arr.nr]   = s;
      ss->arr.nr                += 1;

      return 0;
    }

    #define CREATE_STRS(_r, _s, _loc) do {                                     \
      _r = _create_var(VAR_ARR, LOG_MALS, &_loc);                              \
      if (_r) {                                                                \
        int r = _strs_append(_r, _s, LOG_MALS, &_loc);                         \
        if (r == 0) break;                                                     \
      }                                                                        \
      var_release(_s);                                                         \
      TOD_SAFE_FREE(_s);                                                       \
      var_release(_r);                                                         \
      TOD_SAFE_FREE(_r);                                                       \
      YYABORT;                                                                 \
    } while (0)

    #define STRS_APPEND(_r, _ss, _s, _loc) do {                                \
      int r = _strs_append(_ss, _s, LOG_MALS, &_loc);                          \
      if (r) {                                                                 \
        var_release(_ss);                                                      \
        TOD_SAFE_FREE(_ss);                                                    \
        var_release(_s);                                                       \
        TOD_SAFE_FREE(_s);                                                     \
        YYABORT;                                                               \
      }                                                                        \
      _r = _ss;                                                                \
    } while (0)

    static inline var_t* _create_str(const char *s, size_t n, LOG_ARGS)
    {
      var_t *v = _create_var(VAR_STRING, LOG_VALS, yylloc);
      if (!v) return NULL;
      int r = str_append(&v->str.s, s, n);
      if (r) {
        YLOG(LOG_VALS, yylloc, "runtime error:out of memory");
        return NULL;
      }
      return v;
    }

    #define CREATE_STR(_r, _s, _n, _loc) do {                                  \
      _r = _create_str(_s, _n, LOG_MALS, &_loc);                               \
      if (!_r) {                                                               \
        YLOG(LOG_MALS, &_loc, "runtime error:out of memory");                  \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)

    #define CREATE_FUNC(_r, _n, _loc) do {                                     \
      _r = var_get_eval(_n.text, _n.leng);                                     \
      if (!_r) {                                                               \
        YLOG(LOG_MALS, &_loc, "runtime error:unknown func");                   \
        YYABORT;                                                               \
      }                                                                        \
    } while (0)
}

/* Bison declarations. */
%require "3.0.4"
%define api.pure full
%define api.token.prefix {TOK_}
%define locations
%define api.location.type {parser_loc_t}
%define parse.error verbose
%define parse.lac full
%define parse.trace true
%defines
%verbose

%param { yyscan_t arg }
%parse-param { ext_parser_param_t *param }

// union members
%union { parser_token_t token; }
%union { char c; }
%union { var_t *var; }
%union { var_eval_f f; }

%destructor { var_release($$); free($$); } <var>

%token TOPIC UNEXP
%token <token> DIGITS ID INTEGRAL NUMBER QSTR SSTR TSTR
%token <token> TNAME TKEY TVAL

%nterm <var> id ids exps exp term qstrs sstrs tstrs qstr sstr tstr
%nterm <f> func

%token INSERT INTO USING WITH TAGS VALUES
%left '+' '-'
%left '*' '/'
%left UMINUS

 /* %nterm <str>   args */ // non-terminal `input` use `str` to store
                           // token value as well
 /* %destructor { free($$); } <str> */

%% /* The grammar follows. */

input:
  %empty
| '!' topic                     { (void)yynerrs; }
| '!' insert                    { (void)yynerrs; }
;

insert:
  INSERT INTO table_name values_clause
| INSERT INTO table_name using_clause values_clause
;

table_name:
  '?'                           { SET_INSERT_TBL_QM(@$); }
| id                            { SET_INSERT_TBL($1, @$); }
| id '.' id                     { SET_INSERT_DB_TBL($1, $3, @$); }
;

id:
  ID                            { CREATE_ID($$, $1, @$); }
| '`' '`'                       { STR_EMPTY($$, QUOTE_BQ, @$); }
| '`' tstrs '`'                 { STRS_FLAT($$, $2, QUOTE_BQ, @$); }
;

using_clause:
  USING super_table_name
| USING super_table_name WITH tags_clause
;

super_table_name:
  id                            { SET_INSERT_SUPER_TBL($1, @$); }
| id '.' id                     { SET_INSERT_SUPER_DB_TBL($1, $3, @$); }
;

tags_clause:
  '(' ids ')' TAGS '(' exps ')' { SET_INSERT_TAGS($2,   $6, @$); }
| TAGS '(' exps ')'             { SET_INSERT_TAGS(((var_t*)NULL), $3, @$); }
;

values_clause:
  '(' ids ')' VALUES '(' exps ')'  { SET_INSERT_COLS($2,   $6, @$); }
| VALUES '(' exps ')'              { SET_INSERT_COLS(((var_t*)NULL), $3, @$); }
;

ids:
  id                               { CREATE_IDS($$, $1, @$); }
| ids ',' id                       { IDS_APPEND($$, $1, $3, @$); }
;

exps:
  exp                              { CREATE_EXPS($$, $1, @$); }
| exps ',' exp                     { EXPS_APPEND($$, $1, $3, @$); }
;

exp:
  term                             { $$ = $1; }
| exp '+' exp                      { CREATE_EXP($$, var_add, $1, $3, @$); }
| exp '-' exp                      { CREATE_EXP($$, var_sub, $1, $3, @$); }
| exp '*' exp                      { CREATE_EXP($$, var_mul, $1, $3, @$); }
| exp '/' exp                      { CREATE_EXP($$, var_div, $1, $3, @$); }
| '-' exp           %prec UMINUS   { CREATE_EXP_NEG($$, var_neg, $2, @$); }
| '(' exp ')'                      { $$ = $2; }
| func '(' exps ')'                { CREATE_EXP_BY_NAME($$, $1, $3, @$); }
;

term:
  '?'                              { CREATE_EXP_QM($$, @$); }
| ID                               { CREATE_EXP_ID($$, $1, @$); }
| INTEGRAL                         { CREATE_EXP_INTEGRAL($$, $1, @$); }
| NUMBER                           { CREATE_EXP_NUMBER($$, $1, @$); }
| '"' '"'                          { STR_EMPTY($$, QUOTE_DQ, @$); }
| '"' qstrs '"'                    { STRS_FLAT($$, $2, QUOTE_DQ, @$); }
| '\'' '\''                        { STR_EMPTY($$, QUOTE_SQ, @$); }
| '\'' sstrs '\''                  { STRS_FLAT($$, $2, QUOTE_SQ, @$); }
;

qstrs:
  qstr                             { CREATE_STRS($$, $1, @$); }
| qstrs qstr                       { STRS_APPEND($$, $1, $2, @$); }
;

qstr:
  QSTR                             { CREATE_STR($$, $1.text, $1.leng, @$); }
| '"' '"'                          { CREATE_STR($$, "\"", 1, @$); }
;

sstrs:
  sstr                             { CREATE_STRS($$, $1, @$); }
| sstrs sstr                       { STRS_APPEND($$, $1, $2, @$); }
;

sstr:
  SSTR                             { CREATE_STR($$, $1.text, $1.leng, @$); }
| '\'' '\''                        { CREATE_STR($$, "'", 1, @$); }
;

tstrs:
  tstr                             { CREATE_STRS($$, $1, @$); }
| tstrs tstr                       { STRS_APPEND($$, $1, $2, @$); }
;

tstr:
  TSTR                             { CREATE_STR($$, $1.text, $1.leng, @$); }
| '`' '`'                          { CREATE_STR($$, "`", 1, @$); }
;

func:
  ID                               { CREATE_FUNC($$, $1, @$); }
;


topic:
  TOPIC names
| TOPIC names '{' tconfs '}'
;

names:
  TNAME                     { SET_TOPIC($1, @$); }
| names TNAME               { SET_TOPIC($2, @$); }
;

tconfs:
  %empty
| tconf
| tconfs delimits tconf
;

tconf:
  TKEY                           { SET_TOPIC_KEY($1, @$); }
| TKEY '=' TVAL                  { SET_TOPIC_KEY_VAL($1, $3, @$); }
;

delimits:
  ';'
| delimits ';'
;

%%

/* Called by yyparse on error. */
static void yyerror(
    YYLTYPE *yylloc,                   // match %define locations
    yyscan_t arg,                      // match %param
    ext_parser_param_t *param,         // match %parse-param
    const char *errmsg
)
{
  parser_ctx_t *ctx = param ? &param->ctx : NULL;
  parser_yyerror(__FILE__, __LINE__, __func__, yylloc, arg, ctx, errmsg);
}

static int ext_parser_param_append_topic_name(ext_parser_param_t *param, const char *name, size_t len)
{
  topic_cfg_t *cfg = &param->topic_cfg;
  if (cfg->names_nr == cfg->names_cap) {
    size_t cap = cfg->names_cap + 16;
    char **names = (char**)realloc(cfg->names, sizeof(*names) * cap);
    if (!names) return -1;
    cfg->names = names;
    cfg->names_cap = cap;
  }
  cfg->names[cfg->names_nr] = strndup(name, len);
  if (!cfg->names[cfg->names_nr]) return -1;
  cfg->names_nr += 1;
  return 0;
}

static int ext_parser_param_append_topic_conf(ext_parser_param_t *param, const char *k, size_t kn, const char *v, size_t vn)
{
  topic_cfg_t *cfg = &param->topic_cfg;
  return topic_cfg_append_kv(cfg, k, kn, v, vn);
}

int ext_parser_parse(const char *input, size_t len, ext_parser_param_t *param)
{
  yyscan_t arg = {0};
  yylex_init(&arg);
  // yyset_in(in, arg);
  int debug_flex = param ? param->ctx.debug_flex : 0;
  int debug_bison = param ? param->ctx.debug_bison: 0;
  yyset_debug(debug_flex, arg);
  yydebug = debug_bison;
  yyset_extra(&param->ctx, arg);
  param->ctx.input = input;
  param->ctx.len   = len;
  param->ctx.prev  = 0;
  param->ctx.pres  = 0;
  yy_scan_bytes(input ? input : "", input ? (int)len : 0, arg);
  int ret =yyparse(arg, param);
  yylex_destroy(arg);
  return ret ? -1 : 0;
}

