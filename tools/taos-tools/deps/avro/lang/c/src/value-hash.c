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

#include <avro/platform.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro/value.h"
#include "avro_private.h"

#define check_return(retval, call) \
	do { \
		int  rval = call; \
		if (rval != 0) { return (retval); } \
	} while (0)

/*
 * We currently use MurmurHash3 [1], which is public domain, as our hash
 * implementation.
 *
 * [1] https://code.google.com/p/smhasher/
 */

/* Our seed is the MurmurHash3 of the string "avro.value" */
#define SEED  0xaf4c78df

#define ROTL32(a,b) (((a) << ((b) & 0x1f)) | ((a) >> (32 - ((b) & 0x1f))))

static inline uint32_t
fmix(uint32_t h)
{
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

static const uint32_t  c1 = 0xcc9e2d51;
static const uint32_t  c2 = 0x1b873593;

static inline uint32_t
add_hash(uint32_t start, uint32_t current)
{
	current *= c1;
	current = ROTL32(current, 15);
	current *= c2;

	start ^= current;
	start = ROTL32(start, 13);
	start = start * 5 + 0xe6546b64;

	return start;
}

static inline uint32_t
hash_buffer(uint32_t start, const void *src, size_t len)
{
	const uint8_t  *data = (const uint8_t *) src;
	const int  nblocks = len / 4;

	uint32_t  h1 = start;

	//----------
	// body

	const uint32_t  *blocks = (const uint32_t *) (data + nblocks*4);
	int  i;

	for (i = -nblocks; i != 0; i++) {
		uint32_t  k1 = blocks[i];

		k1 *= c1;
		k1 = ROTL32(k1,15);
		k1 *= c2;

		h1 ^= k1;
		h1 = ROTL32(h1,13);
		h1 = h1*5+0xe6546b64;
	}

	//----------
	// tail

	const uint8_t  *tail = (const uint8_t *) (data + nblocks*4);

	uint32_t  k1 = 0;

	switch (len & 3)
	{
		case 3: k1 ^= tail[2] << 16;
		case 2: k1 ^= tail[1] << 8;
		case 1: k1 ^= tail[0];
			k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
	};

	//----------
	// finalization

	h1 ^= len;
	return h1;
}

static uint32_t
avro_value_hash_fast(avro_value_t *value, uint32_t start)
{
	avro_type_t  type = avro_value_get_type(value);

	switch (type) {
		case AVRO_BOOLEAN:
		{
			int  v;
			check_return(0, avro_value_get_boolean(value, &v));
			return add_hash(start, v);
		}

		case AVRO_BYTES:
		{
			const void  *buf;
			size_t  size;
			check_return(0, avro_value_get_bytes(value, &buf, &size));
			return hash_buffer(start, buf, size);
		}

		case AVRO_DOUBLE:
		{
			union {
				double  d;
				uint32_t  u32[2];
			} v;
			check_return(0, avro_value_get_double(value, &v.d));
			return add_hash(add_hash(start, v.u32[0]), v.u32[1]);
		}

		case AVRO_FLOAT:
		{
			union {
				float  f;
				uint32_t  u32;
			} v;
			check_return(0, avro_value_get_float(value, &v.f));
			return add_hash(start, v.u32);
		}

		case AVRO_INT32:
		{
			int32_t  v;
			check_return(0, avro_value_get_int(value, &v));
			return add_hash(start, v);
		}

		case AVRO_INT64:
		{
			union {
				int64_t  u64;
				uint32_t  u32[2];
			} v;
			check_return(0, avro_value_get_long(value, &v.u64));
			return add_hash(add_hash(start, v.u32[0]), v.u32[1]);
		}

		case AVRO_NULL:
		{
			check_return(0, avro_value_get_null(value));
			return add_hash(start, 0);
		}

		case AVRO_STRING:
		{
			const char  *buf;
			size_t  size;
			check_return(0, avro_value_get_string(value, &buf, &size));
			return hash_buffer(start, buf, size);
		}

		case AVRO_ARRAY:
		{
			size_t  count;
			size_t  i;
			check_return(0, avro_value_get_size(value, &count));

			for (i = 0; i < count; i++) {
				avro_value_t  child;
				check_return(0, avro_value_get_by_index
					     (value, i, &child, NULL));
				start = avro_value_hash_fast(&child, start);
			}

			start ^= count;
			return start;
		}

		case AVRO_ENUM:
		{
			int  v;
			check_return(0, avro_value_get_enum(value, &v));
			return add_hash(start, v);
		}

		case AVRO_FIXED:
		{
			const void  *buf;
			size_t  size;
			check_return(0, avro_value_get_fixed(value, &buf, &size));
			return hash_buffer(start, buf, size);
		}

		case AVRO_MAP:
		{
			size_t  count;
			size_t  i;
			check_return(0, avro_value_get_size(value, &count));

			/*
			 * The hash for a map must be built up without
			 * taking into account the order of the elements
			 */
			uint32_t  map_hash = 0;
			for (i = 0; i < count; i++) {
				avro_value_t  child;
				const char  *key;
				check_return(0, avro_value_get_by_index
					     (value, i, &child, &key));

				uint32_t  element = SEED;
				element = hash_buffer(element, key, strlen(key));
				element = avro_value_hash_fast(&child, element);
				element = fmix(element);

				map_hash ^= element;
			}
			map_hash ^= count;

			return add_hash(start, map_hash);
		}

		case AVRO_RECORD:
		{
			size_t  count;
			size_t  i;
			check_return(0, avro_value_get_size(value, &count));

			for (i = 0; i < count; i++) {
				avro_value_t  child;
				check_return(0, avro_value_get_by_index
					     (value, i, &child, NULL));
				start = avro_value_hash_fast(&child, start);
			}

			start ^= count;
			return start;
		}

		case AVRO_UNION:
		{
			int  disc;
			avro_value_t  branch;
			check_return(0, avro_value_get_discriminant(value, &disc));
			check_return(0, avro_value_get_current_branch(value, &branch));

			start = add_hash(start, disc);
			start = avro_value_hash_fast(&branch, start);
			return start;
		}

		default:
			return 0;
	}
}

uint32_t
avro_value_hash(avro_value_t *value)
{
	uint32_t  hash = avro_value_hash_fast(value, SEED);
	return (hash == 0)? hash: fmix(hash);
}
