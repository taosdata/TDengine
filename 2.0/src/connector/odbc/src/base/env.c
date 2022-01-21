/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "env.h"

#include <pthread.h>

static pthread_once_t          init_once           = PTHREAD_ONCE_INIT;
static char                    default_charset[64] = {0};

static void init_routine(void) {
  OD("compiled with ODBCVER:[0x%04x]", ODBCVER);

  const char *charset = NULL;
  setlocale(LC_ALL, "");
  const char *locale  = setlocale(LC_CTYPE, NULL);
  if (locale) {
    const char *dot  = strrchr(locale, '.');
    if (dot) charset = dot + 1;
  }
  if (!charset) {
#ifdef _MSC_VER
    charset = "CP936";
#else
    charset = "UTF-8";
#endif
    OD("failed to find original locale, fall back to [%s]", charset);
  } else {
#ifdef _MSC_VER
    char buf[64];
    snprintf(buf, sizeof(buf), "CP%s", charset);
    charset = buf;
#endif
    OD("system default charset: [%s]", charset);
  }

  snprintf(default_charset, sizeof(default_charset), "%s", charset);
}


static void env_release(env_t *env) {
  if (!env) return;
  OILE(env->refcount==0, "");

  env_clr_errs(env);
}

int env_init(env_t *env) {
  OILE(env, "");
  OILE(env->refcount==0, "");

  pthread_once(&init_once, init_routine);

  int r = errs_init(&env->errs);
  if (r) return -1;

  snprintf(env->enc_charset, sizeof(env->enc_charset), "%s", default_charset);
  env->odbc_ver = SQL_OV_ODBC3;

  env->refcount = 1;

  return 0;
}

// public
errs_t* env_get_errs(env_t *env) {
  OILE(env, "");

  return &env->errs;
}

void env_clr_errs(env_t *env) {
  if (!env) return;

  errs_reclaim(&env->errs);
}

void env_inc_ref(env_t *env) {
  OILE(env, "");
  int64_t rc = INC_REF(env);
  OILE(rc>=2, "");
}

void env_dec_ref(env_t *env) {
  OILE(env, "");
  int64_t rc = DEC_REF(env);
  if (rc>0) return;
  OILE(rc==0, "");

  env_release(env);
  free(env);
}

env_t* env_create(void) {
  env_t *env = (env_t*)calloc(1, sizeof(*env));
  if (!env) return NULL;

  if (env_init(env)) {
    free(env);
    return NULL;
  }

  return env;
}

