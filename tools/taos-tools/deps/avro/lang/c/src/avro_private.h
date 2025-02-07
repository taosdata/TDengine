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
#ifndef AVRO_PRIVATE_H
#define AVRO_PRIVATE_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <errno.h>

#include "avro/errors.h"
#include "avro/platform.h"

#ifdef HAVE_CONFIG_H
/* This is only true for now in the autotools build */
#include "config.h"
#endif

#ifdef _WIN32
#define snprintf _snprintf
#endif

/* Note that AVRO_PLATFORM_IS_BIG_ENDIAN is *always* defined. It is
 * either TRUE (1) or FALSE (0).
 */
#ifdef _WIN32
  #define AVRO_PLATFORM_IS_BIG_ENDIAN (0)
#else // UNIX
  #include <sys/param.h>
  #if BYTE_ORDER == BIG_ENDIAN
    #define AVRO_PLATFORM_IS_BIG_ENDIAN (1)
  #else
    #define AVRO_PLATFORM_IS_BIG_ENDIAN (0)
  #endif
#endif

/* Add definition of EILSEQ if it is not defined in errno.h. */
#include <errno.h>
#ifndef EILSEQ
#define EILSEQ 138
#endif


#define check(rval, call) { rval = call; if(rval) return rval; }

#define check_set(rval, call, ...)			\
	{						\
		rval = call;				\
		if (rval) {				\
			avro_set_error(__VA_ARGS__);	\
			return rval;			\
		}					\
	}

#define check_prefix(rval, call, ...)			\
	{						\
		rval = call;				\
		if (rval) {				\
			avro_prefix_error(__VA_ARGS__);	\
			return rval;			\
		}					\
	}

#define check_param(result, test, name)					\
	{								\
		if (!(test)) {						\
			avro_set_error("Invalid " name " in %s",	\
				       __FUNCTION__);			\
			return result;					\
		}							\
	}

#define AVRO_UNUSED(var) (void)var;

#define container_of(ptr_, type_, member_)  \
    ((type_ *)((char *)ptr_ - (size_t)&((type_ *)0)->member_))

#define nullstrcmp(s1, s2) \
    (((s1) && (s2)) ? strcmp(s1, s2) : ((s1) || (s2)))

CLOSE_EXTERN
#endif
