/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2001       Anil Kumar
 *  Copyright (C) 2004-2006  Anil Kumar, Jerry St.Clair
 *  Copyright (C) 2019-      Ian Norton
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public
 *  License as published by the Free Software Foundation; either
 *  version 2 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/*
 * Internal functions for rendering a JUnit XML report file once all tests are completed
 */

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "CUnit/Automated.h"
#include "CUnit/AutomatedJUnitXml.h"
#include "CUnit/TestDB.h"
#include "CUnit/Util.h"
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#define _DEFAULT_REPORT_SIZE 10
#define _REPORT_REALLOC_SIZE 8192


/* crude dynamic string buffer */
typedef struct {
    char *buf;
    unsigned end; /* the offset in buf of the nul terminator */
    size_t size;
} cu_dstr;

/* make a dstr */
static void _dstr_init(cu_dstr *dst) {
  assert(dst);
  memset(dst, 0, sizeof(*dst));
  dst->buf = (char*) malloc(_DEFAULT_REPORT_SIZE);
  assert(dst->buf && "dynamic string buffer allocation failed");
  memset(dst->buf, 0, _DEFAULT_REPORT_SIZE);
  dst->end = 0;
  dst->size = _DEFAULT_REPORT_SIZE;
}

/* make sure there is enough space left to add count bytes */
static void _dstr_ensure(cu_dstr *dst, size_t count) {
  size_t newsize = 0;
  if (!dst->buf) {
    _dstr_init(dst);
  }
  count++;
  if (dst->end + count >= dst->size) {
    if (count < _REPORT_REALLOC_SIZE) {
      count = _REPORT_REALLOC_SIZE;
    }
    newsize = dst->size + count;
    dst->buf = (char*) realloc(dst->buf, newsize);
    assert(dst->buf && "dynamic string buffer realloc failed");
    dst->size = newsize;
  }
}

/* append str to dst */
static void _dstr_snputs(cu_dstr *dst, const char* str, size_t count) {
  unsigned i;
  _dstr_ensure(dst, count);

  for (i = 0; i < count; i++) {
    dst->buf[dst->end++] = str[i];
  }
  dst->buf[dst->end] = 0;
}

static void _dstr_puts(cu_dstr *dst, char *str) {
  size_t len = strlen(str);
  if (len) {
    _dstr_snputs(dst, str, len);
  }
}

static void _dstr_reset(cu_dstr *dst) {
  assert(dst);
  free(dst->buf);
  memset(dst, 0, sizeof(*dst));
}

static char* _dstr_release(cu_dstr *dst) {
  char *retval;
  assert(dst);
  retval = dst->buf;
  dst->buf = NULL;
  _dstr_reset(dst);
  return retval;
}

static int _dstr_putf(cu_dstr *dst, const char* format, ...) {
  va_list args;
  va_list args_copy;
#if !(defined(_WIN32) || defined(__CYGWIN__))
  char tmp_buf[2];
#endif
  int need = 0;
  int rv;
  va_start(args, format);
  va_copy(args_copy, args);

#if defined(_WIN32) || defined(__CYGWIN__)
  need = _vscprintf(format, args);
#else
  need = vsnprintf(tmp_buf, 2, format, args);
#endif

  _dstr_ensure(dst, need + 1);
  rv = vsnprintf(dst->buf + dst->end, dst->size - dst->end, format, args_copy);
  dst->end = dst->end + need;
  va_end(args);
  return rv;
}


static char* _escape_string(const char* instr) {
  size_t len = CU_translated_strlen(instr) + 1;
  char *sztmp = (char *)malloc(len);
  assert (sztmp);
  CU_translate_special_characters(instr, sztmp, len);
  return sztmp;
}

static int count_suite_init_errors(CU_pSuite pSuite) {
  return CU_count_test_failures(pSuite->pCleanupFuncTest) + CU_count_test_failures(pSuite->pInitializeFuncTest);
}

static int count_all_init_errors(CU_pTestRegistry pTestRegistry) {
  CU_pSuite s = pTestRegistry->pSuite;
  int n = 0;
  while (s) {
    n += count_suite_init_errors(s);
    s = s->pNext;
  }
  return n;
}

static int count_suite_init_funcs(CU_pSuite pSuite) {
  int n = 0;
  if (pSuite) {
    if (pSuite->pInitializeFuncTest) n++;
    if (pSuite->pCleanupFuncTest) n++;
  }
  return n;
}

static int count_all_init_funcs(CU_pTestRegistry pTestRegistry) {
  int n = 0;
  if (pTestRegistry) {
    CU_pSuite s = pTestRegistry->pSuite;
    while (s) {
      n += count_suite_init_funcs(s);
      s = s->pNext;
    }
  }
  return n;
}

static void format_testcase(cu_dstr *dst, const char *esc_name, CU_pSuite suite, CU_pTest test) {
  char *esc_test_name = _escape_string(test->pName);
  CU_pFailureRecord failure = NULL;
  _dstr_putf(dst,
             "    <testcase classname=\"%s.%s\" name=\"%s\" time=\"%f\">\n",
             CU_automated_package_name_get(),
             esc_name,
             esc_test_name,
             CU_get_test_duration(test)
  );

  if (test->fSkipped || suite->fSkipped) {
    char *skipreason = NULL;
    if (suite->fSkipped) {
      if (suite->pSkipReason)
        skipreason = _escape_string(suite->pSkipReason);
      _dstr_puts(dst, "      <skipped reason=\"suite skipped\">");
      _dstr_putf(dst,
                 "Function: %s\n"
                 "File: %s\n"
                 "Line: %u\n",
                 suite->pSkipFunction,
                 suite->pSkipFile,
                 suite->uiSkipLine
      );
    } else {
      if (test->pSkipReason)
        skipreason = _escape_string(test->pSkipReason);
      _dstr_puts(dst, "      <skipped reason=\"test skipped\">");
      _dstr_putf(dst,
                 "Function: %s\n"
                 "File: %s\n"
                 "Line: %u\n",
                 test->pSkipFunction,
                 test->pSkipFile,
                 test->uiSkipLine
      );
    }
    if (skipreason)
      _dstr_puts(dst, skipreason);
    free(skipreason);
    _dstr_puts(dst, "      </skipped>\n");
  }

  while ((failure = CU_iterate_test_failures(test, failure))) {
    /* add each failure */
    cu_dstr failure_msg = {0};
    char *tmp_failure = NULL;
    _dstr_init(&failure_msg);
    assert(failure);
    switch (failure->type) {
      case CUF_SuiteCleanupFailed:
        _dstr_putf(&failure_msg, "%s Suite-wide cleanup failed", suite->pName);
        break;
      case CUF_SuiteInitFailed:
        _dstr_putf(&failure_msg, "%s Suite-wide setup failed", suite->pName);
        break;
      case CUF_AssertFailed:
        _dstr_putf(&failure_msg,
                   "Function: %s\n"
                   "File: %s\n"
                   "Line: %u\n"
                   "Assertion Failed:\n"
                   "Condition: '%s'\n\n",
                   failure->strFunction,
                   failure->strFileName,
                   failure->uiLineNumber,
                   failure->strCondition
        );
        break;
      default:
        break;
    }
    tmp_failure = _escape_string(failure_msg.buf);
    _dstr_putf(dst, "      <failure>%s</failure>\n", tmp_failure);
    free(tmp_failure);
    _dstr_reset(&failure_msg);
  }

  _dstr_puts(dst, "    </testcase>\n");
  free(esc_test_name);

}

CU_EXPORT void CU_automated_render_junit(char** outstr, const char* filename) {
  assert(outstr && "No destination pointer");
  *outstr = NULL;
  CU_pRunSummary summary = CU_get_run_summary();
  CU_pTestRegistry registry = CU_get_registry();
  CU_pSuite suite = registry->pSuite;
  if (summary) {
    cu_dstr dst = {0};
    char *esc_name = _escape_string(filename);
    int all_errors = count_all_init_errors(registry);
    int all_failures = all_errors + CU_count_all_failures(registry);
    int all_tests = CU_count_all_tests(registry) + count_all_init_funcs(registry);

    _dstr_init(&dst);
    _dstr_puts(&dst, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    _dstr_putf(&dst,
            "<testsuites errors=\"%d\" failures=\"%d\" tests=\"%d\" name=\"%s\" time=\"%f\">\n",
            all_errors,
            all_failures,
            all_tests,
            esc_name,
            summary->ElapsedTime
            );
    free(esc_name);

    /* iterate through all suites */
    while (suite) {
      CU_pTest test = suite->pTest;
      int errors = count_suite_init_errors(suite);
      int test_count = CU_count_suite_tests(suite) + count_suite_init_funcs(suite);
      int test_failures = errors + CU_count_suite_failures(suite);
      esc_name = _escape_string(suite->pName);
      _dstr_putf(&dst,
              "  <testsuite errors=\"%d\" failures=\"%d\" tests=\"%d\" name=\"%s\" time=\"%f\">\n",
              errors,
              test_failures,
              test_count,
              suite->pName,
              CU_get_suite_duration(suite)
              );

      /* record suite setup */
      if (suite->pInitializeFuncTest) format_testcase(&dst, esc_name, suite, suite->pInitializeFuncTest);

      /* iterate through all the tests */
      while (test) {
        format_testcase(&dst, esc_name, suite, test);
        test = test->pNext;
      }

      /* record suite cleanup */
      if (suite->pCleanupFuncTest) format_testcase(&dst, esc_name, suite, suite->pCleanupFuncTest);

      _dstr_puts(&dst, "  </testsuite>\n");
      free(esc_name);
      suite = suite->pNext;
    }

    _dstr_puts(&dst, "</testsuites>\n");
    *outstr = _dstr_release(&dst);
  }
}


CU_EXPORT void CU_automated_finish_junit(const char* filename)
{
  char* reportXml = NULL;
  FILE* outfile = fopen(filename, "w");
  if (!outfile) {
    CU_set_error(CUE_FOPEN_FAILED);
  } else {
    CU_automated_render_junit(&reportXml, filename);

    fputs(reportXml, outfile);

    if (fclose(outfile)) {
      CU_set_error(CUE_FCLOSE_FAILED);
    }
    free(reportXml);
  }
}
