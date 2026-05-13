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

#ifndef _url_parser_h_
#define _url_parser_h_

// https://datatracker.ietf.org/doc/html/rfc3986

#include "macros.h"
#include "typedefs.h"

#include <stddef.h>
#include <stdint.h>

EXTERN_C_BEGIN

void url_parser_param_reset(url_parser_param_t *param) FA_HIDDEN;
void url_parser_param_release(url_parser_param_t *param) FA_HIDDEN;

int url_parser_parse(const char *input, size_t len,
    url_parser_param_t *param) FA_HIDDEN;

void url_release(url_t *url) FA_HIDDEN;

int url_encode(url_t *url, char **out) FA_HIDDEN;
int url_encode_with_database(url_t *url, const char *db, char **out) FA_HIDDEN;
int url_parse_and_encode(const conn_cfg_t*, char **out, url_parser_param_t *param) FA_HIDDEN;

int url_set_scheme(url_t *url, const char *s, size_t n) FA_HIDDEN;
int url_set_user_pass(url_t *url, const char *u, size_t un, const char *p, size_t pn) FA_HIDDEN;
int url_set_host_port(url_t *url, const char *host, uint16_t port) FA_HIDDEN;

EXTERN_C_END

#endif // _url_parser_h_


