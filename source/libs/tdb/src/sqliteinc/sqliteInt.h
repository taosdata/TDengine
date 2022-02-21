/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Internal interface definitions for SQLite.
**
*/

#include <stdint.h>

#ifndef SQLITEINT_H
#define SQLITEINT_H

typedef int8_t   i8;
typedef int16_t  i16;
typedef int32_t  i32;
typedef int64_t  i64;
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef struct sqlite3_pcache_page {
  void *pBuf;   /* The content of the page */
  void *pExtra; /* Extra information associated with the page */
} sqlite3_pcache_page;

typedef u32 Pgno;

typedef struct Pager Pager;

#include "pcache.h"

#endif /* SQLITEINT_H */
