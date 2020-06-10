/*
 * Copyright (C) 1999-2009 Free Software Foundation, Inc.
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

/* Part 2 of iconv_open.
   Input:
     struct conv_struct * cd;
     unsigned int from_index;
     int from_wchar;
     unsigned int to_index;
     int to_wchar;
     int transliterate;
     int discard_ilseq;
   Output: none.
   Side effects: Fills cd.
 */

  cd->iindex = from_index;
  cd->ifuncs = all_encodings[from_index].ifuncs;
  cd->oindex = to_index;
  cd->ofuncs = all_encodings[to_index].ofuncs;
  cd->oflags = all_encodings[to_index].oflags;
  /* Initialize the loop functions. */
#if HAVE_MBRTOWC
  if (to_wchar) {
#if HAVE_WCRTOMB
    if (from_wchar) {
      cd->lfuncs.loop_convert = wchar_id_loop_convert;
      cd->lfuncs.loop_reset = wchar_id_loop_reset;
    } else
#endif
    {
      cd->lfuncs.loop_convert = wchar_to_loop_convert;
      cd->lfuncs.loop_reset = wchar_to_loop_reset;
    }
  } else
#endif
  {
#if HAVE_WCRTOMB
    if (from_wchar) {
      cd->lfuncs.loop_convert = wchar_from_loop_convert;
      cd->lfuncs.loop_reset = wchar_from_loop_reset;
    } else
#endif
    {
      cd->lfuncs.loop_convert = unicode_loop_convert;
      cd->lfuncs.loop_reset = unicode_loop_reset;
    }
  }
  /* Initialize the states. */
  memset(&cd->istate,'\0',sizeof(state_t));
  memset(&cd->ostate,'\0',sizeof(state_t));
  /* Initialize the operation flags. */
  cd->transliterate = transliterate;
  cd->discard_ilseq = discard_ilseq;
  #ifndef LIBICONV_PLUG
  cd->fallbacks.mb_to_uc_fallback = NULL;
  cd->fallbacks.uc_to_mb_fallback = NULL;
  cd->fallbacks.mb_to_wc_fallback = NULL;
  cd->fallbacks.wc_to_mb_fallback = NULL;
  cd->fallbacks.data = NULL;
  cd->hooks.uc_hook = NULL;
  cd->hooks.wc_hook = NULL;
  cd->hooks.data = NULL;
  #endif
  /* Initialize additional fields. */
  if (from_wchar != to_wchar) {
    struct wchar_conv_struct * wcd = (struct wchar_conv_struct *) cd;
#if HAVE_WCRTOMB || HAVE_MBRTOWC
    memset(&wcd->state,'\0',sizeof(mbstate_t));
#endif
  }
  /* Done. */
