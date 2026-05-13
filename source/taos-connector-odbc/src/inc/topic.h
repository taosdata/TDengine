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

#ifndef _topic_h_
#define _topic_h_

#include "macros.h"
#include "typedefs.h"

#include <taos.h>

EXTERN_C_BEGIN

void topic_cfg_release(topic_cfg_t *cfg) FA_HIDDEN;
void topic_cfg_transfer(topic_cfg_t *from, topic_cfg_t *to) FA_HIDDEN;
int topic_cfg_append_name(topic_cfg_t *cfg, const char *name, size_t len) FA_HIDDEN;
int topic_cfg_append_kv(topic_cfg_t *cfg, const char *k, size_t kn, const char *v, size_t vn) FA_HIDDEN;

void topic_reset(topic_t *topic) FA_HIDDEN;
void topic_release(topic_t *topic) FA_HIDDEN;

void topic_init(topic_t *topic, stmt_t *stmt) FA_HIDDEN;

SQLRETURN topic_open(
    topic_t             *topic,
    const sqlc_tsdb_t   *sql,
    topic_cfg_t         *cfg) FA_HIDDEN;

EXTERN_C_END

#endif //  _topic_h_

