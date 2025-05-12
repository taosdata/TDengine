/*
          Copyright (c) 2013 - 2014, 2016 Mark Adler, Robert Vazan, Max Vysokikh

          This software is provided 'as-is', without any express or implied
          warranty.  In no event will the author be held liable for any damages
          arising from the use of this software.

          Permission is granted to anyone to use this software for any purpose,
          including commercial applications, and to alter it and redistribute it
          freely, subject to the following restrictions:

          1. The origin of this software must not be misrepresented; you must not
          claim that you wrote the original software. If you use this software
          in a product, an acknowledgment in the product documentation would be
          appreciated but is not required.
          2. Altered source versions must be plainly marked as such, and must not be
          misrepresented as being the original software.
          3. This notice may not be removed or altered from any source distribution.
        */

#ifndef _TD_UTIL_CRC32_H_
#define _TD_UTIL_CRC32_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef const uint8_t *crc_stream;

extern uint32_t (*crc32c)(uint32_t crci, crc_stream bytes, size_t len);

uint32_t crc32c_sf(uint32_t crci, crc_stream input, size_t length);

uint32_t crc32c_hw(uint32_t crc, crc_stream buf, size_t len);

void taosResolveCRC();

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_CRC32_H_*/
