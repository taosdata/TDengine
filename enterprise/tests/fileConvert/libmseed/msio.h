/***************************************************************************
 * Interface declarations for routines in fio.c
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

#ifndef MSIO_H
#define MSIO_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include "libmseed.h"

extern int msio_fopen (LMIO *io, const char *path, const char *mode,
                       int64_t *startoffset, int64_t *endoffset);
extern int msio_fclose (LMIO *io);
extern size_t msio_fread (LMIO *io, void *buffer, size_t size);
extern int msio_feof (LMIO *io);
extern int msio_url_useragent (const char *program, const char *version);
extern int msio_url_userpassword (const char *userpassword);
extern int msio_url_addheader (const char *header);
extern void msio_url_freeheaders (void);

#ifdef __cplusplus
}
#endif

#endif
