/*
 * Copyright (C) 1999-2008 Free Software Foundation, Inc.
 * This file is part of the GNU LIBICONV Library.
 *
 * The GNU LIBICONV Library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * The GNU LIBICONV Library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with the GNU LIBICONV Library; see the file COPYING.LIB.
 * If not, write to the Free Software Foundation, Inc., 51 Franklin Street,
 * Fifth Floor, Boston, MA 02110-1301, USA.
 */

#include <iconv.h>

#include <stdlib.h>
#include <string.h>
#include "config.h"
#include "localcharset.h"

#if ENABLE_EXTRA
/*
 * Consider all system dependent encodings, for any system,
 * and the extra encodings.
 */
#define USE_AIX
#define USE_OSF1
#define USE_DOS
#define USE_EXTRA
#else
/*
 * Consider those system dependent encodings that are needed for the
 * current system.
 */
#ifdef _AIX
#define USE_AIX
#endif
#if defined(__osf__) || defined(VMS)
#define USE_OSF1
#endif
#if defined(__DJGPP__) || (defined(_WIN32) && (defined(_MSC_VER) || defined(__MINGW32__)))
#define USE_DOS
#endif
#endif

/*
 * Data type for general conversion loop.
 */
struct loop_funcs {
  size_t (*loop_convert) (iconv_t icd,
                          const char* * inbuf, size_t *inbytesleft,
                          char* * outbuf, size_t *outbytesleft);
  size_t (*loop_reset) (iconv_t icd,
                        char* * outbuf, size_t *outbytesleft);
};

/*
 * Converters.
 */
#include "converters.h"

/*
 * Transliteration tables.
 */
#include "cjk_variants.h"
#include "translit.h"

/*
 * Table of all supported encodings.
 */
struct encoding {
  struct mbtowc_funcs ifuncs; /* conversion multibyte -> unicode */
  struct wctomb_funcs ofuncs; /* conversion unicode -> multibyte */
  int oflags;                 /* flags for unicode -> multibyte conversion */
};
#define DEFALIAS(xxx_alias,xxx) /* nothing */
enum {
#define DEFENCODING(xxx_names,xxx,xxx_ifuncs1,xxx_ifuncs2,xxx_ofuncs1,xxx_ofuncs2) \
  ei_##xxx ,
#include "encodings.def"
#ifdef USE_AIX
# include "encodings_aix.def"
#endif
#ifdef USE_OSF1
# include "encodings_osf1.def"
#endif
#ifdef USE_DOS
# include "encodings_dos.def"
#endif
#ifdef USE_EXTRA
# include "encodings_extra.def"
#endif
#include "encodings_local.def"
#undef DEFENCODING
ei_for_broken_compilers_that_dont_like_trailing_commas
};
#include "flags.h"
static struct encoding const all_encodings[] = {
#define DEFENCODING(xxx_names,xxx,xxx_ifuncs1,xxx_ifuncs2,xxx_ofuncs1,xxx_ofuncs2) \
  { xxx_ifuncs1,xxx_ifuncs2, xxx_ofuncs1,xxx_ofuncs2, ei_##xxx##_oflags },
#include "encodings.def"
#ifdef USE_AIX
# include "encodings_aix.def"
#endif
#ifdef USE_OSF1
# include "encodings_osf1.def"
#endif
#ifdef USE_DOS
# include "encodings_dos.def"
#endif
#ifdef USE_EXTRA
# include "encodings_extra.def"
#endif
#undef DEFENCODING
#define DEFENCODING(xxx_names,xxx,xxx_ifuncs1,xxx_ifuncs2,xxx_ofuncs1,xxx_ofuncs2) \
  { xxx_ifuncs1,xxx_ifuncs2, xxx_ofuncs1,xxx_ofuncs2, 0 },
#include "encodings_local.def"
#undef DEFENCODING
};
#undef DEFALIAS

/*
 * Conversion loops.
 */
#include "loops.h"

/*
 * Alias lookup function.
 * Defines
 *   struct alias { int name; unsigned int encoding_index; };
 *   const struct alias * aliases_lookup (const char *str, unsigned int len);
 *   #define MAX_WORD_LENGTH ...
 */
#if defined _AIX
# include "aliases_sysaix.h"
#elif defined hpux || defined __hpux
# include "aliases_syshpux.h"
#elif defined __osf__
# include "aliases_sysosf1.h"
#elif defined __sun
# include "aliases_syssolaris.h"
#else
# include "aliases.h"
#endif

/*
 * System dependent alias lookup function.
 * Defines
 *   const struct alias * aliases2_lookup (const char *str);
 */
#if defined(USE_AIX) || defined(USE_OSF1) || defined(USE_DOS) || defined(USE_EXTRA) /* || ... */
struct stringpool2_t {
#define S(tag,name,encoding_index) char stringpool_##tag[sizeof(name)];
#include "aliases2.h"
#undef S
};
static const struct stringpool2_t stringpool2_contents = {
#define S(tag,name,encoding_index) name,
#include "aliases2.h"
#undef S
};
#define stringpool2 ((const char *) &stringpool2_contents)
static const struct alias sysdep_aliases[] = {
#define S(tag,name,encoding_index) { (int)(long)&((struct stringpool2_t *)0)->stringpool_##tag, encoding_index },
#include "aliases2.h"
#undef S
};
#ifdef __GNUC__
__inline
#endif
// gcc -o0 bug fix 
// see http://git.savannah.gnu.org/gitweb/?p=libiconv.git;a=blobdiff;f=lib/iconv.c;h=31853a7f1c47871221189dbf597473a16d8a8da7;hp=5a1a32597fa3efc5f69624d37a2eb96f308cd241;hb=b29089d8b43abc8fba073da7e6dccaeba56b2b70;hpb=0a04404c90d6a725b8b6bbcd65e10c5fcf5993e9

static const struct alias *
aliases2_lookup (register const char *str)
{
  const struct alias * ptr;
  unsigned int count;
  for (ptr = sysdep_aliases, count = sizeof(sysdep_aliases)/sizeof(sysdep_aliases[0]); count > 0; ptr++, count--)
    if (!strcmp(str, stringpool2 + ptr->name))
      return ptr;
  return NULL;
}
#else
#define aliases2_lookup(str)  NULL
#define stringpool2  NULL
#endif

#if 0
/* Like !strcasecmp, except that the both strings can be assumed to be ASCII
   and the first string can be assumed to be in uppercase. */
static int strequal (const char* str1, const char* str2)
{
  unsigned char c1;
  unsigned char c2;
  for (;;) {
    c1 = * (unsigned char *) str1++;
    c2 = * (unsigned char *) str2++;
    if (c1 == 0)
      break;
    if (c2 >= 'a' && c2 <= 'z')
      c2 -= 'a'-'A';
    if (c1 != c2)
      break;
  }
  return (c1 == c2);
}
#endif

iconv_t iconv_open (const char* tocode, const char* fromcode)
{
  struct conv_struct * cd;
  unsigned int from_index;
  int from_wchar;
  unsigned int to_index;
  int to_wchar;
  int transliterate;
  int discard_ilseq;

#include "iconv_open1.h"

  cd = (struct conv_struct *) malloc(from_wchar != to_wchar
                                     ? sizeof(struct wchar_conv_struct)
                                     : sizeof(struct conv_struct));
  if (cd == NULL) {
    errno = ENOMEM;
    return (iconv_t)(-1);
  }

#include "iconv_open2.h"

  return (iconv_t)cd;
invalid:
  errno = EINVAL;
  return (iconv_t)(-1);
}

size_t iconv (iconv_t icd,
              ICONV_CONST char* * inbuf, size_t *inbytesleft,
              char* * outbuf, size_t *outbytesleft)
{
  conv_t cd = (conv_t) icd;
  if (inbuf == NULL || *inbuf == NULL)
    return cd->lfuncs.loop_reset(icd,outbuf,outbytesleft);
  else
    return cd->lfuncs.loop_convert(icd,
                                   (const char* *)inbuf,inbytesleft,
                                   outbuf,outbytesleft);
}

int iconv_close (iconv_t icd)
{
  conv_t cd = (conv_t) icd;
  free(cd);
  return 0;
}

#ifndef LIBICONV_PLUG

/*
 * Verify that a 'struct conv_struct' and a 'struct wchar_conv_struct' each
 * fit in an iconv_allocation_t.
 * If this verification fails, iconv_allocation_t must be made larger and
 * the major version in LIBICONV_VERSION_INFO must be bumped.
 * Currently 'struct conv_struct' has 21 integer/pointer fields, and
 * 'struct wchar_conv_struct' additionally has an 'mbstate_t' field.
 */
typedef int verify_size_1[2 * (sizeof (struct conv_struct) <= sizeof (iconv_allocation_t)) - 1];
typedef int verify_size_2[2 * (sizeof (struct wchar_conv_struct) <= sizeof (iconv_allocation_t)) - 1];

int iconv_open_into (const char* tocode, const char* fromcode,
                     iconv_allocation_t* resultp)
{
  struct conv_struct * cd;
  unsigned int from_index;
  int from_wchar;
  unsigned int to_index;
  int to_wchar;
  int transliterate;
  int discard_ilseq;

#include "iconv_open1.h"

  cd = (struct conv_struct *) resultp;

#include "iconv_open2.h"

  return 0;
invalid:
  errno = EINVAL;
  return -1;
}

int iconvctl (iconv_t icd, int request, void* argument)
{
  conv_t cd = (conv_t) icd;
  switch (request) {
    case ICONV_TRIVIALP:
      *(int *)argument =
        ((cd->lfuncs.loop_convert == unicode_loop_convert
          && cd->iindex == cd->oindex)
         || cd->lfuncs.loop_convert == wchar_id_loop_convert
         ? 1 : 0);
      return 0;
    case ICONV_GET_TRANSLITERATE:
      *(int *)argument = cd->transliterate;
      return 0;
    case ICONV_SET_TRANSLITERATE:
      cd->transliterate = (*(const int *)argument ? 1 : 0);
      return 0;
    case ICONV_GET_DISCARD_ILSEQ:
      *(int *)argument = cd->discard_ilseq;
      return 0;
    case ICONV_SET_DISCARD_ILSEQ:
      cd->discard_ilseq = (*(const int *)argument ? 1 : 0);
      return 0;
    case ICONV_SET_HOOKS:
      if (argument != NULL) {
        cd->hooks = *(const struct iconv_hooks *)argument;
      } else {
        cd->hooks.uc_hook = NULL;
        cd->hooks.wc_hook = NULL;
        cd->hooks.data = NULL;
      }
      return 0;
    case ICONV_SET_FALLBACKS:
      if (argument != NULL) {
        cd->fallbacks = *(const struct iconv_fallbacks *)argument;
      } else {
        cd->fallbacks.mb_to_uc_fallback = NULL;
        cd->fallbacks.uc_to_mb_fallback = NULL;
        cd->fallbacks.mb_to_wc_fallback = NULL;
        cd->fallbacks.wc_to_mb_fallback = NULL;
        cd->fallbacks.data = NULL;
      }
      return 0;
    default:
      errno = EINVAL;
      return -1;
  }
}

/* An alias after its name has been converted from 'int' to 'const char*'. */
struct nalias { const char* name; unsigned int encoding_index; };

static int compare_by_index (const void * arg1, const void * arg2)
{
  const struct nalias * alias1 = (const struct nalias *) arg1;
  const struct nalias * alias2 = (const struct nalias *) arg2;
  return (int)alias1->encoding_index - (int)alias2->encoding_index;
}

static int compare_by_name (const void * arg1, const void * arg2)
{
  const char * name1 = *(const char **)arg1;
  const char * name2 = *(const char **)arg2;
  /* Compare alphabetically, but put "CS" names at the end. */
  int sign = strcmp(name1,name2);
  if (sign != 0) {
    sign = ((name1[0]=='C' && name1[1]=='S') - (name2[0]=='C' && name2[1]=='S'))
           * 4 + (sign >= 0 ? 1 : -1);
  }
  return sign;
}

void iconvlist (int (*do_one) (unsigned int namescount,
                               const char * const * names,
                               void* data),
                void* data)
{
#define aliascount1  sizeof(aliases)/sizeof(aliases[0])
#ifndef aliases2_lookup
#define aliascount2  sizeof(sysdep_aliases)/sizeof(sysdep_aliases[0])
#else
#define aliascount2  0
#endif
#define aliascount  (aliascount1+aliascount2)
  struct nalias aliasbuf[aliascount];
  const char * namesbuf[aliascount];
  size_t num_aliases;
  {
    /* Put all existing aliases into a buffer. */
    size_t i;
    size_t j;
    j = 0;
    for (i = 0; i < aliascount1; i++) {
      const struct alias * p = &aliases[i];
      if (p->name >= 0
          && p->encoding_index != ei_local_char
          && p->encoding_index != ei_local_wchar_t) {
        aliasbuf[j].name = stringpool + p->name;
        aliasbuf[j].encoding_index = p->encoding_index;
        j++;
      }
    }
#ifndef aliases2_lookup
    for (i = 0; i < aliascount2; i++) {
      aliasbuf[j].name = stringpool2 + sysdep_aliases[i].name;
      aliasbuf[j].encoding_index = sysdep_aliases[i].encoding_index;
      j++;
    }
#endif
    num_aliases = j;
  }
  /* Sort by encoding_index. */
  if (num_aliases > 1)
    qsort(aliasbuf, num_aliases, sizeof(struct nalias), compare_by_index);
  {
    /* Process all aliases with the same encoding_index together. */
    size_t j;
    j = 0;
    while (j < num_aliases) {
      unsigned int ei = aliasbuf[j].encoding_index;
      size_t i = 0;
      do
        namesbuf[i++] = aliasbuf[j++].name;
      while (j < num_aliases && aliasbuf[j].encoding_index == ei);
      if (i > 1)
        qsort(namesbuf, i, sizeof(const char *), compare_by_name);
      /* Call the callback. */
      if (do_one(i,namesbuf,data))
        break;
    }
  }
#undef aliascount
#undef aliascount2
#undef aliascount1
}

/*
 * Table of canonical names of encodings.
 * Instead of strings, it contains offsets into stringpool and stringpool2.
 */
static const unsigned short all_canonical[] = {
#if defined _AIX
# include "canonical_sysaix.h"
#elif defined hpux || defined __hpux
# include "canonical_syshpux.h"
#elif defined __osf__
# include "canonical_sysosf1.h"
#elif defined __sun
# include "canonical_syssolaris.h"
#else
# include "canonical.h"
#endif
#ifdef USE_AIX
# if defined _AIX
#  include "canonical_aix_sysaix.h"
# else
#  include "canonical_aix.h"
# endif
#endif
#ifdef USE_OSF1
# if defined __osf__
#  include "canonical_osf1_sysosf1.h"
# else
#  include "canonical_osf1.h"
# endif
#endif
#ifdef USE_DOS
# include "canonical_dos.h"
#endif
#ifdef USE_EXTRA
# include "canonical_extra.h"
#endif
#if defined _AIX
# include "canonical_local_sysaix.h"
#elif defined hpux || defined __hpux
# include "canonical_local_syshpux.h"
#elif defined __osf__
# include "canonical_local_sysosf1.h"
#elif defined __sun
# include "canonical_local_syssolaris.h"
#else
# include "canonical_local.h"
#endif
};

const char * iconv_canonicalize (const char * name)
{
  const char* code;
  char buf[MAX_WORD_LENGTH+10+1];
  const char* cp;
  char* bp;
  const struct alias * ap;
  unsigned int count;
  unsigned int deps_index;
  const char* pool;

  /* Before calling aliases_lookup, convert the input string to upper case,
   * and check whether it's entirely ASCII (we call gperf with option "-7"
   * to achieve a smaller table) and non-empty. If it's not entirely ASCII,
   * or if it's too long, it is not a valid encoding name.
   */
  for (code = name;;) {
    /* Search code in the table. */
    for (cp = code, bp = buf, count = MAX_WORD_LENGTH+10+1; ; cp++, bp++) {
      unsigned char c = * (unsigned char *) cp;
      if (c >= 0x80)
        goto invalid;
      if (c >= 'a' && c <= 'z')
        c -= 'a'-'A';
      *bp = c;
      if (c == '\0')
        break;
      if (--count == 0)
        goto invalid;
    }
    for (;;) {
      if (bp-buf >= 10 && memcmp(bp-10,"//TRANSLIT",10)==0) {
        bp -= 10;
        *bp = '\0';
        continue;
      }
      if (bp-buf >= 8 && memcmp(bp-8,"//IGNORE",8)==0) {
        bp -= 8;
        *bp = '\0';
        continue;
      }
      break;
    }
    if (buf[0] == '\0') {
      code = locale_charset();
      /* Avoid an endless loop that could occur when using an older version
         of localcharset.c. */
      if (code[0] == '\0')
        goto invalid;
      continue;
    }
    pool = stringpool;
    ap = aliases_lookup(buf,bp-buf);
    if (ap == NULL) {
      pool = stringpool2;
      ap = aliases2_lookup(buf);
      if (ap == NULL)
        goto invalid;
    }
    if (ap->encoding_index == ei_local_char) {
      code = locale_charset();
      /* Avoid an endless loop that could occur when using an older version
         of localcharset.c. */
      if (code[0] == '\0')
        goto invalid;
      continue;
    }
    if (ap->encoding_index == ei_local_wchar_t) {
      /* On systems which define __STDC_ISO_10646__, wchar_t is Unicode.
         This is also the case on native Woe32 systems.  */
#if __STDC_ISO_10646__ || ((defined _WIN32 || defined __WIN32__) && !defined __CYGWIN__)
      if (sizeof(wchar_t) == 4) {
        deps_index = ei_ucs4internal;
        break;
      }
      if (sizeof(wchar_t) == 2) {
        deps_index = ei_ucs2internal;
        break;
      }
      if (sizeof(wchar_t) == 1) {
        deps_index = ei_iso8859_1;
        break;
      }
#endif
    }
    deps_index = ap->encoding_index;
    break;
  }
  return all_canonical[deps_index] + pool;
 invalid:
  return name;
}

int _libiconv_version = _LIBICONV_VERSION;

#if defined __FreeBSD__ && !defined __gnu_freebsd__
/* GNU libiconv is the native FreeBSD iconv implementation since 2002.
   It wants to define the symbols 'iconv_open', 'iconv', 'iconv_close'.  */
#define strong_alias(name, aliasname) _strong_alias(name, aliasname)
#define _strong_alias(name, aliasname) \
  extern __typeof (name) aliasname __attribute__ ((alias (#name)));
#undef iconv_open
#undef iconv
#undef iconv_close
strong_alias (libiconv_open, iconv_open)
strong_alias (libiconv, iconv)
strong_alias (libiconv_close, iconv_close)
#endif

#endif
