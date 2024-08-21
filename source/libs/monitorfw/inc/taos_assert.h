/**
 * Copyright 2019-2020 DigitalOcean Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <assert.h>

#ifndef TAOS_ASSERT_H
#define TAOS_ASSERT_H

#ifdef TAOS_ASSERT_ENABLE
#define TAOS_ASSERT(i) assert(i);
#else
#define TAOS_ASSERT(i) \
  if (i) return 1;
#define TAOS_ASSERT_NULL(i) \
  if (i) return NULL;
#endif  // TAOS_TEST

#endif  // TAOS_ASSERT_H
