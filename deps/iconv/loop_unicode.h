/*
 * Copyright (C) 1999-2003, 2005-2006, 2008 Free Software Foundation, Inc.
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

/* This file defines the conversion loop via Unicode as a pivot encoding. */

/* Attempt to transliterate wc. Return code as in xxx_wctomb. */
static int unicode_transliterate (conv_t cd, ucs4_t wc,
                                  unsigned char* outptr, size_t outleft)
{
  if (cd->oflags & HAVE_HANGUL_JAMO) {
    /* Decompose Hangul into Jamo. Use double-width Jamo (contained
       in all Korean encodings and ISO-2022-JP-2), not half-width Jamo
       (contained in Unicode only). */
    ucs4_t buf[3];
    int ret = johab_hangul_decompose(cd,buf,wc);
    if (ret != RET_ILUNI) {
      /* we know 1 <= ret <= 3 */
      state_t backup_state = cd->ostate;
      unsigned char* backup_outptr = outptr;
      size_t backup_outleft = outleft;
      int i, sub_outcount;
      for (i = 0; i < ret; i++) {
        if (outleft == 0) {
          sub_outcount = RET_TOOSMALL;
          goto johab_hangul_failed;
        }
        sub_outcount = cd->ofuncs.xxx_wctomb(cd,outptr,buf[i],outleft);
        if (sub_outcount <= RET_ILUNI)
          goto johab_hangul_failed;
        if (!(sub_outcount <= outleft)) abort();
        outptr += sub_outcount; outleft -= sub_outcount;
      }
      return outptr-backup_outptr;
    johab_hangul_failed:
      cd->ostate = backup_state;
      outptr = backup_outptr;
      outleft = backup_outleft;
      if (sub_outcount != RET_ILUNI)
        return RET_TOOSMALL;
    }
  }
  {
    /* Try to use a variant, but postfix it with
       U+303E IDEOGRAPHIC VARIATION INDICATOR
       (cf. Ken Lunde's "CJKV information processing", p. 188). */
    int indx = -1;
    if (wc == 0x3006)
      indx = 0;
    else if (wc == 0x30f6)
      indx = 1;
    else if (wc >= 0x4e00 && wc < 0xa000)
      indx = cjk_variants_indx[wc-0x4e00];
    if (indx >= 0) {
      for (;; indx++) {
        ucs4_t buf[2];
        unsigned short variant = cjk_variants[indx];
        unsigned short last = variant & 0x8000;
        variant &= 0x7fff;
        variant += 0x3000;
        buf[0] = variant; buf[1] = 0x303e;
        {
          state_t backup_state = cd->ostate;
          unsigned char* backup_outptr = outptr;
          size_t backup_outleft = outleft;
          int i, sub_outcount;
          for (i = 0; i < 2; i++) {
            if (outleft == 0) {
              sub_outcount = RET_TOOSMALL;
              goto variant_failed;
            }
            sub_outcount = cd->ofuncs.xxx_wctomb(cd,outptr,buf[i],outleft);
            if (sub_outcount <= RET_ILUNI)
              goto variant_failed;
            if (!(sub_outcount <= outleft)) abort();
            outptr += sub_outcount; outleft -= sub_outcount;
          }
          return outptr-backup_outptr;
        variant_failed:
          cd->ostate = backup_state;
          outptr = backup_outptr;
          outleft = backup_outleft;
          if (sub_outcount != RET_ILUNI)
            return RET_TOOSMALL;
        }
        if (last)
          break;
      }
    }
  }
  if (wc >= 0x2018 && wc <= 0x201a) {
    /* Special case for quotation marks 0x2018, 0x2019, 0x201a */
    ucs4_t substitute =
      (cd->oflags & HAVE_QUOTATION_MARKS
       ? (wc == 0x201a ? 0x2018 : wc)
       : (cd->oflags & HAVE_ACCENTS
          ? (wc==0x2019 ? 0x00b4 : 0x0060) /* use accents */
          : 0x0027 /* use apostrophe */
      )  );
    int outcount = cd->ofuncs.xxx_wctomb(cd,outptr,substitute,outleft);
    if (outcount != RET_ILUNI)
      return outcount;
  }
  {
    /* Use the transliteration table. */
    int indx = translit_index(wc);
    if (indx >= 0) {
      const unsigned int * cp = &translit_data[indx];
      unsigned int num = *cp++;
      state_t backup_state = cd->ostate;
      unsigned char* backup_outptr = outptr;
      size_t backup_outleft = outleft;
      unsigned int i;
      int sub_outcount;
      for (i = 0; i < num; i++) {
        if (outleft == 0) {
          sub_outcount = RET_TOOSMALL;
          goto translit_failed;
        }
        sub_outcount = cd->ofuncs.xxx_wctomb(cd,outptr,cp[i],outleft);
        if (sub_outcount == RET_ILUNI)
          /* Recursive transliteration. */
          sub_outcount = unicode_transliterate(cd,cp[i],outptr,outleft);
        if (sub_outcount <= RET_ILUNI)
          goto translit_failed;
        if (!(sub_outcount <= outleft)) abort();
        outptr += sub_outcount; outleft -= sub_outcount;
      }
      return outptr-backup_outptr;
    translit_failed:
      cd->ostate = backup_state;
      outptr = backup_outptr;
      outleft = backup_outleft;
      if (sub_outcount != RET_ILUNI)
        return RET_TOOSMALL;
    }
  }
  return RET_ILUNI;
}

#ifndef LIBICONV_PLUG

struct uc_to_mb_fallback_locals {
  unsigned char* l_outbuf;
  size_t l_outbytesleft;
  int l_errno;
};

static void uc_to_mb_write_replacement (const char *buf, size_t buflen,
                                        void* callback_arg)
{
  struct uc_to_mb_fallback_locals * plocals =
    (struct uc_to_mb_fallback_locals *) callback_arg;
  /* Do nothing if already encountered an error in a previous call. */
  if (plocals->l_errno == 0) {
    /* Attempt to copy the passed buffer to the output buffer. */
    if (plocals->l_outbytesleft < buflen)
      plocals->l_errno = E2BIG;
    else {
      memcpy(plocals->l_outbuf, buf, buflen);
      plocals->l_outbuf += buflen;
      plocals->l_outbytesleft -= buflen;
    }
  }
}

struct mb_to_uc_fallback_locals {
  conv_t l_cd;
  unsigned char* l_outbuf;
  size_t l_outbytesleft;
  int l_errno;
};

static void mb_to_uc_write_replacement (const unsigned int *buf, size_t buflen,
                                        void* callback_arg)
{
  struct mb_to_uc_fallback_locals * plocals =
    (struct mb_to_uc_fallback_locals *) callback_arg;
  /* Do nothing if already encountered an error in a previous call. */
  if (plocals->l_errno == 0) {
    /* Attempt to convert the passed buffer to the target encoding. */
    conv_t cd = plocals->l_cd;
    unsigned char* outptr = plocals->l_outbuf;
    size_t outleft = plocals->l_outbytesleft;
    for (; buflen > 0; buf++, buflen--) {
      ucs4_t wc = *buf;
      int outcount;
      if (outleft == 0) {
        plocals->l_errno = E2BIG;
        break;
      }
      outcount = cd->ofuncs.xxx_wctomb(cd,outptr,wc,outleft);
      if (outcount != RET_ILUNI)
        goto outcount_ok;
      /* Handle Unicode tag characters (range U+E0000..U+E007F). */
      if ((wc >> 7) == (0xe0000 >> 7))
        goto outcount_zero;
      /* Try transliteration. */
      if (cd->transliterate) {
        outcount = unicode_transliterate(cd,wc,outptr,outleft);
        if (outcount != RET_ILUNI)
          goto outcount_ok;
      }
      if (cd->discard_ilseq) {
        outcount = 0;
        goto outcount_ok;
      }
      #ifndef LIBICONV_PLUG
      else if (cd->fallbacks.uc_to_mb_fallback != NULL) {
        struct uc_to_mb_fallback_locals locals;
        locals.l_outbuf = outptr;
        locals.l_outbytesleft = outleft;
        locals.l_errno = 0;
        cd->fallbacks.uc_to_mb_fallback(wc,
                                        uc_to_mb_write_replacement,
                                        &locals,
                                        cd->fallbacks.data);
        if (locals.l_errno != 0) {
          plocals->l_errno = locals.l_errno;
          break;
        }
        outptr = locals.l_outbuf;
        outleft = locals.l_outbytesleft;
        outcount = 0;
        goto outcount_ok;
      }
      #endif
      outcount = cd->ofuncs.xxx_wctomb(cd,outptr,0xFFFD,outleft);
      if (outcount != RET_ILUNI)
        goto outcount_ok;
      plocals->l_errno = EILSEQ;
      break;
    outcount_ok:
      if (outcount < 0) {
        plocals->l_errno = E2BIG;
        break;
      }
      #ifndef LIBICONV_PLUG
      if (cd->hooks.uc_hook)
        (*cd->hooks.uc_hook)(wc, cd->hooks.data);
      #endif
      if (!(outcount <= outleft)) abort();
      outptr += outcount; outleft -= outcount;
    outcount_zero: ;
    }
    plocals->l_outbuf = outptr;
    plocals->l_outbytesleft = outleft;
  }
}

#endif /* !LIBICONV_PLUG */

static size_t unicode_loop_convert (iconv_t icd,
                                    const char* * inbuf, size_t *inbytesleft,
                                    char* * outbuf, size_t *outbytesleft)
{
  conv_t cd = (conv_t) icd;
  size_t result = 0;
  const unsigned char* inptr = (const unsigned char*) *inbuf;
  size_t inleft = *inbytesleft;
  unsigned char* outptr = (unsigned char*) *outbuf;
  size_t outleft = *outbytesleft;
  while (inleft > 0) {
    state_t last_istate = cd->istate;
    ucs4_t wc;
    int incount;
    int outcount;
    incount = cd->ifuncs.xxx_mbtowc(cd,&wc,inptr,inleft);
    if (incount < 0) {
      if ((unsigned int)(-1-incount) % 2 == (unsigned int)(-1-RET_ILSEQ) % 2) {
        /* Case 1: invalid input, possibly after a shift sequence */
        incount = DECODE_SHIFT_ILSEQ(incount);
        if (cd->discard_ilseq) {
          switch (cd->iindex) {
            case ei_ucs4: case ei_ucs4be: case ei_ucs4le:
            case ei_utf32: case ei_utf32be: case ei_utf32le:
            case ei_ucs4internal: case ei_ucs4swapped:
              incount += 4; break;
            case ei_ucs2: case ei_ucs2be: case ei_ucs2le:
            case ei_utf16: case ei_utf16be: case ei_utf16le:
            case ei_ucs2internal: case ei_ucs2swapped:
              incount += 2; break;
            default:
              incount += 1; break;
          }
          goto outcount_zero;
        }
        #ifndef LIBICONV_PLUG
        else if (cd->fallbacks.mb_to_uc_fallback != NULL) {
          unsigned int incount2;
          struct mb_to_uc_fallback_locals locals;
          switch (cd->iindex) {
            case ei_ucs4: case ei_ucs4be: case ei_ucs4le:
            case ei_utf32: case ei_utf32be: case ei_utf32le:
            case ei_ucs4internal: case ei_ucs4swapped:
              incount2 = 4; break;
            case ei_ucs2: case ei_ucs2be: case ei_ucs2le:
            case ei_utf16: case ei_utf16be: case ei_utf16le:
            case ei_ucs2internal: case ei_ucs2swapped:
              incount2 = 2; break;
            default:
              incount2 = 1; break;
          }
          locals.l_cd = cd;
          locals.l_outbuf = outptr;
          locals.l_outbytesleft = outleft;
          locals.l_errno = 0;
          cd->fallbacks.mb_to_uc_fallback((const char*)inptr+incount, incount2,
                                          mb_to_uc_write_replacement,
                                          &locals,
                                          cd->fallbacks.data);
          if (locals.l_errno != 0) {
            inptr += incount; inleft -= incount;
            errno = locals.l_errno;
            result = -1;
            break;
          }
          incount += incount2;
          outptr = locals.l_outbuf;
          outleft = locals.l_outbytesleft;
          result += 1;
          goto outcount_zero;
        }
        #endif
        inptr += incount; inleft -= incount;
        errno = EILSEQ;
        result = -1;
        break;
      }
      if (incount == RET_TOOFEW(0)) {
        /* Case 2: not enough bytes available to detect anything */
        errno = EINVAL;
        result = -1;
        break;
      }
      /* Case 3: k bytes read, but only a shift sequence */
      incount = DECODE_TOOFEW(incount);
    } else {
      /* Case 4: k bytes read, making up a wide character */
      if (outleft == 0) {
        cd->istate = last_istate;
        errno = E2BIG;
        result = -1;
        break;
      }
      outcount = cd->ofuncs.xxx_wctomb(cd,outptr,wc,outleft);
      if (outcount != RET_ILUNI)
        goto outcount_ok;
      /* Handle Unicode tag characters (range U+E0000..U+E007F). */
      if ((wc >> 7) == (0xe0000 >> 7))
        goto outcount_zero;
      /* Try transliteration. */
      result++;
      if (cd->transliterate) {
        outcount = unicode_transliterate(cd,wc,outptr,outleft);
        if (outcount != RET_ILUNI)
          goto outcount_ok;
      }
      if (cd->discard_ilseq) {
        outcount = 0;
        goto outcount_ok;
      }
      #ifndef LIBICONV_PLUG
      else if (cd->fallbacks.uc_to_mb_fallback != NULL) {
        struct uc_to_mb_fallback_locals locals;
        locals.l_outbuf = outptr;
        locals.l_outbytesleft = outleft;
        locals.l_errno = 0;
        cd->fallbacks.uc_to_mb_fallback(wc,
                                        uc_to_mb_write_replacement,
                                        &locals,
                                        cd->fallbacks.data);
        if (locals.l_errno != 0) {
          cd->istate = last_istate;
          errno = locals.l_errno;
          return -1;
        }
        outptr = locals.l_outbuf;
        outleft = locals.l_outbytesleft;
        outcount = 0;
        goto outcount_ok;
      }
      #endif
      outcount = cd->ofuncs.xxx_wctomb(cd,outptr,0xFFFD,outleft);
      if (outcount != RET_ILUNI)
        goto outcount_ok;
      cd->istate = last_istate;
      errno = EILSEQ;
      result = -1;
      break;
    outcount_ok:
      if (outcount < 0) {
        cd->istate = last_istate;
        errno = E2BIG;
        result = -1;
        break;
      }
      #ifndef LIBICONV_PLUG
      if (cd->hooks.uc_hook)
        (*cd->hooks.uc_hook)(wc, cd->hooks.data);
      #endif
      if (!(outcount <= outleft)) abort();
      outptr += outcount; outleft -= outcount;
    }
  outcount_zero:
    if (!(incount <= inleft)) abort();
    inptr += incount; inleft -= incount;
  }
  *inbuf = (const char*) inptr;
  *inbytesleft = inleft;
  *outbuf = (char*) outptr;
  *outbytesleft = outleft;
  return result;
}

static size_t unicode_loop_reset (iconv_t icd,
                                  char* * outbuf, size_t *outbytesleft)
{
  conv_t cd = (conv_t) icd;
  if (outbuf == NULL || *outbuf == NULL) {
    /* Reset the states. */
    memset(&cd->istate,'\0',sizeof(state_t));
    memset(&cd->ostate,'\0',sizeof(state_t));
    return 0;
  } else {
    size_t result = 0;
    if (cd->ifuncs.xxx_flushwc) {
      state_t last_istate = cd->istate;
      ucs4_t wc;
      if (cd->ifuncs.xxx_flushwc(cd, &wc)) {
        unsigned char* outptr = (unsigned char*) *outbuf;
        size_t outleft = *outbytesleft;
        int outcount = cd->ofuncs.xxx_wctomb(cd,outptr,wc,outleft);
        if (outcount != RET_ILUNI)
          goto outcount_ok;
        /* Handle Unicode tag characters (range U+E0000..U+E007F). */
        if ((wc >> 7) == (0xe0000 >> 7))
          goto outcount_zero;
        /* Try transliteration. */
        result++;
        if (cd->transliterate) {
          outcount = unicode_transliterate(cd,wc,outptr,outleft);
          if (outcount != RET_ILUNI)
            goto outcount_ok;
        }
        if (cd->discard_ilseq) {
          outcount = 0;
          goto outcount_ok;
        }
        #ifndef LIBICONV_PLUG
        else if (cd->fallbacks.uc_to_mb_fallback != NULL) {
          struct uc_to_mb_fallback_locals locals;
          locals.l_outbuf = outptr;
          locals.l_outbytesleft = outleft;
          locals.l_errno = 0;
          cd->fallbacks.uc_to_mb_fallback(wc,
                                          uc_to_mb_write_replacement,
                                          &locals,
                                          cd->fallbacks.data);
          if (locals.l_errno != 0) {
            cd->istate = last_istate;
            errno = locals.l_errno;
            return -1;
          }
          outptr = locals.l_outbuf;
          outleft = locals.l_outbytesleft;
          outcount = 0;
          goto outcount_ok;
        }
        #endif
        outcount = cd->ofuncs.xxx_wctomb(cd,outptr,0xFFFD,outleft);
        if (outcount != RET_ILUNI)
          goto outcount_ok;
        cd->istate = last_istate;
        errno = EILSEQ;
        return -1;
      outcount_ok:
        if (outcount < 0) {
          cd->istate = last_istate;
          errno = E2BIG;
          return -1;
        }
        #ifndef LIBICONV_PLUG
        if (cd->hooks.uc_hook)
          (*cd->hooks.uc_hook)(wc, cd->hooks.data);
        #endif
        if (!(outcount <= outleft)) abort();
        outptr += outcount;
        outleft -= outcount;
      outcount_zero:
        *outbuf = (char*) outptr;
        *outbytesleft = outleft;
      }
    }
    if (cd->ofuncs.xxx_reset) {
      unsigned char* outptr = (unsigned char*) *outbuf;
      size_t outleft = *outbytesleft;
      int outcount = cd->ofuncs.xxx_reset(cd,outptr,outleft);
      if (outcount < 0) {
        errno = E2BIG;
        return -1;
      }
      if (!(outcount <= outleft)) abort();
      *outbuf = (char*) (outptr + outcount);
      *outbytesleft = outleft - outcount;
    }
    memset(&cd->istate,'\0',sizeof(state_t));
    memset(&cd->ostate,'\0',sizeof(state_t));
    return result;
  }
}
