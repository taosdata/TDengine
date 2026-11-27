// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#include "private.h"

static void copyCrtFields(struct tm *out, const TaosCrtTm *in) {
  out->tm_sec = in->tm_sec;
  out->tm_min = in->tm_min;
  out->tm_hour = in->tm_hour;
  out->tm_mday = in->tm_mday;
  out->tm_mon = in->tm_mon;
  out->tm_year = in->tm_year;
  out->tm_wday = in->tm_wday;
  out->tm_yday = in->tm_yday;
  out->tm_isdst = in->tm_isdst;
}

static void fromCrt(struct tm *out, const TaosCrtTm *in) {
  memset(out, 0, sizeof(*out));
  copyCrtFields(out, in);
  // The CRT has no offset hint. Zero would incorrectly suggest UTC.
  out->tm_gmtoff = LONG_MAX;
  // tzcode fills wday only after a successful conversion, including -1.
  out->tm_wday = -1;
}

static void toCrt(TaosCrtTm *out, const struct tm *in) {
  out->tm_sec = in->tm_sec;
  out->tm_min = in->tm_min;
  out->tm_hour = in->tm_hour;
  out->tm_mday = in->tm_mday;
  out->tm_mon = in->tm_mon;
  out->tm_year = in->tm_year;
  out->tm_wday = in->tm_wday;
  out->tm_yday = in->tm_yday;
  out->tm_isdst = in->tm_isdst;
}

void *taosTzAlloc(const char *name) {
  return tzalloc(name);
}

void taosTzFree(void *tz) {
  tzfree(tz);
}

TaosCrtTm *taosTzLocaltime(void *tz, const time_t *value,
                          TaosCrtTm *result, long *offset) {
  struct tm tmp;
  errno = 0;
  if (!localtime_rz(tz, value, &tmp)) {
    // Some tzcode extrapolation failures leave errno unset.
    if (errno == 0) errno = EOVERFLOW;
    return NULL;
  }
  toCrt(result, &tmp);
  if (offset) *offset = tmp.tm_gmtoff;
  return result;
}

time_t taosTzMktime(void *tz, TaosCrtTm *value, long *offset) {
  struct tm tmp;
  fromCrt(&tmp, value);
  time_t result = mktime_z(tz, &tmp);
  if (tmp.tm_wday < 0) {
    errno = EINVAL;
    return -1;
  }
  errno = 0;
  toCrt(value, &tmp);
  if (offset) *offset = tmp.tm_gmtoff;
  return result;
}

time_t taosTzTimegm(TaosCrtTm *value) {
  struct tm tmp;
  fromCrt(&tmp, value);
  time_t result = timegm(&tmp);
  if (tmp.tm_wday < 0) {
    errno = EOVERFLOW;
    return -1;
  }
  errno = 0;
  toCrt(value, &tmp);
  return result;
}

TaosCrtTm *taosTzGmtime(const time_t *value, TaosCrtTm *result) {
  struct tm tmp;
  if (!gmtime_r(value, &tmp)) return NULL;
  toCrt(result, &tmp);
  return result;
}

size_t taosTzStrftime(char *s, size_t size, const char *format,
                      const TaosCrtTm *value, void *tz) {
  struct tm tmp;
  fromCrt(&tmp, value);
  if (tz != NULL) {
    errno = 0;
    (void)mktime_z(tz, &tmp);
    if (tmp.tm_wday < 0) return 0;
    // Metadata follows the resolved instant even if the DST hint normalized.
    // Preserve supplied fields; use StrftimeAt for an unambiguous instant.
    copyCrtFields(&tmp, value);
  } else {
    tmp.tm_wday = value->tm_wday;
    tmp.tm_gmtoff = 0;
    tmp.tm_zone = "UTC";
  }
  return strftime(s, size, format, &tmp);
}

size_t taosTzStrftimeAt(char *s, size_t size, const char *format,
                        const time_t *stamp, void *tz) {
  struct tm tmp;
  if (!localtime_rz(tz, stamp, &tmp)) return 0;
  return strftime(s, size, format, &tmp);
}
