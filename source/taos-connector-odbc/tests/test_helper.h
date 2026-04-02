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

#ifndef _test_helper_h_
#define _test_helper_h_

#include "os_port.h"
#include "cjson/cJSON.h"
#include "ejson_parser.h"

EXTERN_C_BEGIN

int json_get_by_path(cJSON *root, const char *path, cJSON **json) FA_HIDDEN;
const char* json_object_get_string(cJSON *json, const char *key) FA_HIDDEN;
int json_object_get_number(cJSON *json, const char *key, double *v) FA_HIDDEN;
int json_get_number(cJSON *json, double *v) FA_HIDDEN;
cJSON* json_object_get_array(cJSON *json, const char *key) FA_HIDDEN;
int json_get_by_item(cJSON *root, int item, cJSON **json) FA_HIDDEN;
cJSON* load_json_file(const char *json_file, char *buf, size_t bytes, const char *fromcode, const char *tocode) FA_HIDDEN;
const char* json_to_string(cJSON *json) FA_HIDDEN;



int ejson_get_by_path(ejson_t *ejson, const char *path, ejson_t **v) FA_HIDDEN;
const char* ejson_object_get_string(ejson_t *ejson, const char *key) FA_HIDDEN;
int ejson_object_get_number(ejson_t *ejson, const char *key, double *v) FA_HIDDEN;
int ejson_get_number(ejson_t *ejson, double *v) FA_HIDDEN;
ejson_t* ejson_object_get_array(ejson_t *ejson, const char *key) FA_HIDDEN;
int ejson_get_by_item(ejson_t *ejson, int item, ejson_t **v) FA_HIDDEN;
ejson_t* load_ejson_file(const char *ejson_file, char *buf, size_t bytes, const char *fromcode, const char *tocode) FA_HIDDEN;
const char* ejson_to_string(ejson_t *ejson) FA_HIDDEN;



EXTERN_C_END

#endif // _test_helper_h_

