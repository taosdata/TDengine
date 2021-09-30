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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tulog.h"
#include "tglobal.h"
#include "tconfig.h"
#include "tutil.h"

/**
 * In some Linux systems, setLocale(LC_CTYPE, "") may return NULL, in which case the launch of
 * both the TDengine Server and the Client may be interrupted.
 *
 * In case that the setLocale failed to be executed, the right charset needs to be set.
 */
void tsSetLocale() {
  char *locale = setlocale(LC_CTYPE, tsLocale);

  // default locale or user specified locale is not valid, abort launch
  if (locale == NULL) {
    uError("Invalid locale:%s, please set the valid locale in config file", tsLocale);
  }

  if (strlen(tsCharset) == 0) {
    uError("failed to get charset, please set the valid charset in config file");
    exit(-1);
  }

  if (!taosValidateEncodec(tsCharset)) {
    uError("Invalid charset:%s, please set the valid charset in config file", tsCharset);
    exit(-1);
  }
}