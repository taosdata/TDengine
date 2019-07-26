/* Copyright (C) 1999-2003, 2005, 2007 Free Software Foundation, Inc.
   This file is part of the GNU LIBICONV Library.

   The GNU LIBICONV Library is free software; you can redistribute it
   and/or modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   The GNU LIBICONV Library is distributed in the hope that it will be
   useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with the GNU LIBICONV Library; see the file COPYING.LIB.
   If not, write to the Free Software Foundation, Inc., 51 Franklin Street,
   Fifth Floor, Boston, MA 02110-1301, USA.  */


/* Define to 1 to enable a few rarely used encodings. */
#undef ENABLE_EXTRA

/* Define to 1 if the package shall run at any location in the filesystem. */
#undef ENABLE_RELOCATABLE

/* Define to a type if <wchar.h> does not define. */
#undef mbstate_t

/* Define if you have <iconv.h>, the iconv_t type, and the
   iconv_open, iconv, iconv_close functions. */
#undef HAVE_ICONV
/* Define as const if the declaration of iconv() needs const. */
#define ICONV_CONST /* empty by default */

/* Define to 1 if you have the getc_unlocked() function. */
#undef HAVE_GETC_UNLOCKED

/* Define if you have <langinfo.h> and nl_langinfo(CODESET). */
#undef HAVE_LANGINFO_CODESET

/* Define if you have the mbrtowc() function. */
#undef HAVE_MBRTOWC

/* Define to 1 if you have the setlocale() function. */
#undef HAVE_SETLOCALE

/* Define to 1 if you have the <stddef.h> header file. */
#undef HAVE_STDDEF_H

/* Define to 1 if you have the <stdlib.h> header file. */
#undef HAVE_STDLIB_H

/* Define to 1 if you have the <string.h> header file. */
#undef HAVE_STRING_H

/* Define to 1 or 0, depending whether the compiler supports simple visibility
   declarations. */
#undef HAVE_VISIBILITY

/* Define if you have the wcrtomb() function. */
#undef HAVE_WCRTOMB

/* Define if the machine's byte ordering is little endian. */
#undef WORDS_LITTLEENDIAN

/* Define to the value of ${prefix}, as a string. */
#undef INSTALLPREFIX

