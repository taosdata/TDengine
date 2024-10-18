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

#include "tcs.h"
#include "tcsInt.h"

STcs tcs;

int32_t tcsPutObjectFromFile2(const char* file, const char* object, int8_t withcp) {
  return tcs.PutObjectFromFile2(file, object, withcp);
}

int32_t tcsGetObjectsByPrefix(const char* prefix, const char* path) { return tcs.GetObjectsByPrefix(prefix, path); }

int32_t tcsDeleteObjects(const char* object_name[], int nobject) { return tcs.DeleteObjects(object_name, nobject); }

int32_t tcsGetObjectToFile(const char* object_name, const char* fileName) {
  return tcs.GetObjectToFile(object_name, fileName);
}
