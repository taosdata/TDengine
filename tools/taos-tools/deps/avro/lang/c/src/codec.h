/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#ifndef AVRO_CODEC_H
#define	AVRO_CODEC_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <avro/platform.h>

enum avro_codec_type_t {
	AVRO_CODEC_NULL,
	AVRO_CODEC_DEFLATE,
	AVRO_CODEC_LZMA,
	AVRO_CODEC_SNAPPY
};
typedef enum avro_codec_type_t avro_codec_type_t;

struct avro_codec_t_ {
	const char * name;
	avro_codec_type_t type;
	int64_t block_size;
	int64_t used_size;
	void * block_data;
	void * codec_data;
};
typedef struct avro_codec_t_* avro_codec_t;

int avro_codec(avro_codec_t c, const char *type);
int avro_codec_reset(avro_codec_t c);
int avro_codec_encode(avro_codec_t c, void * data, int64_t len);
int avro_codec_decode(avro_codec_t c, void * data, int64_t len);

CLOSE_EXTERN
#endif
