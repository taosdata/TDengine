/*
 * Copyright (C) 2000-2002, 2005-2006, 2008 Free Software Foundation, Inc.
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

/* This file defines three conversion loops:
     - from wchar_t to anything else,
     - from anything else to wchar_t,
     - from wchar_t to wchar_t.
 */

#if HAVE_WCRTOMB || HAVE_MBRTOWC
# include <wchar.h>
# define BUF_SIZE 64  /* assume MB_LEN_MAX <= 64 */
  /* Some systems, like BeOS, have multibyte encodings but lack mbstate_t.  */
  extern size_t mbrtowc ();
# ifdef mbstate_t
#  define mbrtowc(pwc, s, n, ps) (mbrtowc)(pwc, s, n, 0)
#  define mbsinit(ps) 1
# endif
# ifndef mbsinit
#  if !HAVE_MBSINIT
#   define mbsinit(ps) 1
#  endif
# endif
#endif

/*
 * The first two conversion loops have an extended conversion descriptor.
 */
struct wchar_conv_struct {
  struct conv_struct parent;
#if HAVE_WCRTOMB || HAVE_MBRTOWC
  mbstate_t state;
#endif
};


#if HAVE_WCRTOMB

/* From wchar_t to anything else. */

#ifndef LIBICONV_PLUG

#if 0

struct wc_to_mb_fallback_locals {
  struct wchar_conv_struct * l_wcd;
  char* l_outbuf;
  size_t l_outbytesleft;
  int l_errno;
};

/* A callback that writes a string given in the locale encoding. */
static void wc_to_mb_write_replacement (const char *buf, size_t buflen,
                                        void* callback_arg)
{
  struct wc_to_mb_fallback_locals * plocals =
    (struct wc_to_mb_fallback_locals *) callback_arg;
  /* Do nothing if already encountered an error in a previous call. */
  if (plocals->l_errno == 0) {
    /* Attempt to convert the passed buffer to the target encoding.
       Here we don't support characters split across multiple calls. */
    const char* bufptr = buf;
    size_t bufleft = buflen;
    size_t res = unicode_loop_convert(&plocals->l_wcd->parent,
                                      &bufptr,&bufleft,
                                      &plocals->l_outbuf,&plocals->l_outbytesleft);
    if (res == (size_t)(-1)) {
      if (errno == EILSEQ || errno == EINVAL)
        /* Invalid buf contents. */
        plocals->l_errno = EILSEQ;
      else if (errno == E2BIG)
        /* Output buffer too small. */
        plocals->l_errno = E2BIG;
      else 
        abort();
    } else {
      /* Successful conversion. */
      if (bufleft > 0)
        abort();
    }
  }
}

#else

struct wc_to_mb_fallback_locals {
  char* l_outbuf;
  size_t l_outbytesleft;
  int l_errno;
};

/* A callback that writes a string given in the target encoding. */
static void wc_to_mb_write_replacement (const char *buf, size_t buflen,
                                        void* callback_arg)
{
  struct wc_to_mb_fallback_locals * plocals =
    (struct wc_to_mb_fallback_locals *) callback_arg;
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

#endif

#endif /* !LIBICONV_PLUG */

static size_t wchar_from_loop_convert (iconv_t icd,
                                       const char* * inbuf, size_t *inbytesleft,
                                       char* * outbuf, size_t *outbytesleft)
{
  struct wchar_conv_struct * wcd = (struct wchar_conv_struct *) icd;
  size_t result = 0;
  while (*inbytesleft >= sizeof(wchar_t)) {
    const wchar_t * inptr = (const wchar_t *) *inbuf;
    size_t inleft = *inbytesleft;
    char buf[BUF_SIZE];
    mbstate_t state = wcd->state;
    size_t bufcount = 0;
    while (inleft >= sizeof(wchar_t)) {
      /* Convert one wchar_t to multibyte representation. */
      size_t count = wcrtomb(buf+bufcount,*inptr,&state);
      if (count == (size_t)(-1)) {
        /* Invalid input. */
        if (wcd->parent.discard_ilseq) {
          count = 0;
        }
        #ifndef LIBICONV_PLUG
        else if (wcd->parent.fallbacks.wc_to_mb_fallback != NULL) {
          /* Drop the contents of buf[] accumulated so far, and instead
             pass all queued wide characters to the fallback handler. */
          struct wc_to_mb_fallback_locals locals;
          const wchar_t * fallback_inptr;
          #if 0
          locals.l_wcd = wcd;
          #endif
          locals.l_outbuf = *outbuf;
          locals.l_outbytesleft = *outbytesleft;
          locals.l_errno = 0;
          for (fallback_inptr = (const wchar_t *) *inbuf;
               fallback_inptr <= inptr;
               fallback_inptr++)
            wcd->parent.fallbacks.wc_to_mb_fallback(*fallback_inptr,
                                                    wc_to_mb_write_replacement,
                                                    &locals,
                                                    wcd->parent.fallbacks.data);
          if (locals.l_errno != 0) {
            errno = locals.l_errno;
            return -1;
          }
          wcd->state = state;
          *inbuf = (const char *) (inptr + 1);
          *inbytesleft = inleft - sizeof(wchar_t);
          *outbuf = locals.l_outbuf;
          *outbytesleft = locals.l_outbytesleft;
          result += 1;
          break;
        }
        #endif
        else {
          errno = EILSEQ;
          return -1;
        }
      }
      inptr++;
      inleft -= sizeof(wchar_t);
      bufcount += count;
      if (count == 0) {
        /* Continue, append next wchar_t. */
      } else {
        /* Attempt to convert the accumulated multibyte representations
           to the target encoding. */
        const char* bufptr = buf;
        size_t bufleft = bufcount;
        char* outptr = *outbuf;
        size_t outleft = *outbytesleft;
        size_t res = unicode_loop_convert(&wcd->parent,
                                          &bufptr,&bufleft,
                                          &outptr,&outleft);
        if (res == (size_t)(-1)) {
          if (errno == EILSEQ)
            /* Invalid input. */
            return -1;
          else if (errno == E2BIG)
            /* Output buffer too small. */
            return -1;
          else if (errno == EINVAL) {
            /* Continue, append next wchar_t, but avoid buffer overrun. */
            if (bufcount + MB_CUR_MAX > BUF_SIZE)
              abort();
          } else
            abort();
        } else {
          /* Successful conversion. */
          wcd->state = state;
          *inbuf = (const char *) inptr;
          *inbytesleft = inleft;
          *outbuf = outptr;
          *outbytesleft = outleft;
          result += res;
          break;
        }
      }
    }
  }
  return result;
}

static size_t wchar_from_loop_reset (iconv_t icd,
                                     char* * outbuf, size_t *outbytesleft)
{
  struct wchar_conv_struct * wcd = (struct wchar_conv_struct *) icd;
  if (outbuf == NULL || *outbuf == NULL) {
    /* Reset the states. */
    memset(&wcd->state,'\0',sizeof(mbstate_t));
    return unicode_loop_reset(&wcd->parent,NULL,NULL);
  } else {
    if (!mbsinit(&wcd->state)) {
      mbstate_t state = wcd->state;
      char buf[BUF_SIZE];
      size_t bufcount = wcrtomb(buf,(wchar_t)0,&state);
      if (bufcount == (size_t)(-1) || bufcount == 0 || buf[bufcount-1] != '\0')
        abort();
      else {
        const char* bufptr = buf;
        size_t bufleft = bufcount-1;
        char* outptr = *outbuf;
        size_t outleft = *outbytesleft;
        size_t res = unicode_loop_convert(&wcd->parent,
                                          &bufptr,&bufleft,
                                          &outptr,&outleft);
        if (res == (size_t)(-1)) {
          if (errno == E2BIG)
            return -1;
          else
            abort();
        } else {
          res = unicode_loop_reset(&wcd->parent,&outptr,&outleft);
          if (res == (size_t)(-1))
            return res;
          else {
            /* Successful. */
            wcd->state = state;
            *outbuf = outptr;
            *outbytesleft = outleft;
            return 0;
          }
        }
      }
    } else
      return unicode_loop_reset(&wcd->parent,outbuf,outbytesleft);
  }
}

#endif


#if HAVE_MBRTOWC

/* From anything else to wchar_t. */

#ifndef LIBICONV_PLUG

struct mb_to_wc_fallback_locals {
  char* l_outbuf;
  size_t l_outbytesleft;
  int l_errno;
};

static void mb_to_wc_write_replacement (const wchar_t *buf, size_t buflen,
                                        void* callback_arg)
{
  struct mb_to_wc_fallback_locals * plocals =
    (struct mb_to_wc_fallback_locals *) callback_arg;
  /* Do nothing if already encountered an error in a previous call. */
  if (plocals->l_errno == 0) {
    /* Attempt to copy the passed buffer to the output buffer. */
    if (plocals->l_outbytesleft < sizeof(wchar_t)*buflen)
      plocals->l_errno = E2BIG;
    else {
      for (; buflen > 0; buf++, buflen--) {
        *(wchar_t*) plocals->l_outbuf = *buf;
        plocals->l_outbuf += sizeof(wchar_t);
        plocals->l_outbytesleft -= sizeof(wchar_t);
      }
    }
  }
}

#endif /* !LIBICONV_PLUG */

static size_t wchar_to_loop_convert (iconv_t icd,
                                     const char* * inbuf, size_t *inbytesleft,
                                     char* * outbuf, size_t *outbytesleft)
{
  struct wchar_conv_struct * wcd = (struct wchar_conv_struct *) icd;
  size_t result = 0;
  while (*inbytesleft > 0) {
    size_t incount;
    for (incount = 1; incount <= *inbytesleft; incount++) {
      char buf[BUF_SIZE];
      const char* inptr = *inbuf;
      size_t inleft = incount;
      char* bufptr = buf;
      size_t bufleft = BUF_SIZE;
      size_t res = unicode_loop_convert(&wcd->parent,
                                        &inptr,&inleft,
                                        &bufptr,&bufleft);
      if (res == (size_t)(-1)) {
        if (errno == EILSEQ)
          /* Invalid input. */
          return -1;
        else if (errno == EINVAL) {
          /* Incomplete input. Next try with one more input byte. */
        } else
          /* E2BIG shouldn't occur. */
          abort();
      } else {
        /* Successful conversion. */
        size_t bufcount = bufptr-buf; /* = BUF_SIZE-bufleft */
        mbstate_t state = wcd->state;
        wchar_t wc;
        res = mbrtowc(&wc,buf,bufcount,&state);
        if (res == (size_t)(-2)) {
          /* Next try with one more input byte. */
        } else {
          if (res == (size_t)(-1)) {
            /* Invalid input. */
            if (wcd->parent.discard_ilseq) {
            }
            #ifndef LIBICONV_PLUG
            else if (wcd->parent.fallbacks.mb_to_wc_fallback != NULL) {
              /* Drop the contents of buf[] accumulated so far, and instead
                 pass all queued chars to the fallback handler. */
              struct mb_to_wc_fallback_locals locals;
              locals.l_outbuf = *outbuf;
              locals.l_outbytesleft = *outbytesleft;
              locals.l_errno = 0;
              wcd->parent.fallbacks.mb_to_wc_fallback(*inbuf, incount,
                                                      mb_to_wc_write_replacement,
                                                      &locals,
                                                      wcd->parent.fallbacks.data);
              if (locals.l_errno != 0) {
                errno = locals.l_errno;
                return -1;
              }
              /* Restoring the state is not needed because it is the initial
                 state anyway: For all known locale encodings, the multibyte
                 to wchar_t conversion doesn't have shift state, and we have
                 excluded partial accumulated characters. */
              /* wcd->state = state; */
              *inbuf += incount;
              *inbytesleft -= incount;
              *outbuf = locals.l_outbuf;
              *outbytesleft = locals.l_outbytesleft;
              result += 1;
              break;
            }
            #endif
            else
              return -1;
          } else {
            if (*outbytesleft < sizeof(wchar_t)) {
              errno = E2BIG;
              return -1;
            }
            *(wchar_t*) *outbuf = wc;
            /* Restoring the state is not needed because it is the initial
               state anyway: For all known locale encodings, the multibyte
               to wchar_t conversion doesn't have shift state, and we have
               excluded partial accumulated characters. */
            /* wcd->state = state; */
            *outbuf += sizeof(wchar_t);
            *outbytesleft -= sizeof(wchar_t);
          }
          *inbuf += incount;
          *inbytesleft -= incount;
          result += res;
          break;
        }
      }
    }
  }
  return result;
}

static size_t wchar_to_loop_reset (iconv_t icd,
                                   char* * outbuf, size_t *outbytesleft)
{
  struct wchar_conv_struct * wcd = (struct wchar_conv_struct *) icd;
  size_t res = unicode_loop_reset(&wcd->parent,outbuf,outbytesleft);
  if (res == (size_t)(-1))
    return res;
  memset(&wcd->state,0,sizeof(mbstate_t));
  return 0;
}

#endif


/* From wchar_t to wchar_t. */

static size_t wchar_id_loop_convert (iconv_t icd,
                                     const char* * inbuf, size_t *inbytesleft,
                                     char* * outbuf, size_t *outbytesleft)
{
  struct conv_struct * cd = (struct conv_struct *) icd;
  const wchar_t* inptr = (const wchar_t*) *inbuf;
  size_t inleft = *inbytesleft / sizeof(wchar_t);
  wchar_t* outptr = (wchar_t*) *outbuf;
  size_t outleft = *outbytesleft / sizeof(wchar_t);
  size_t count = (inleft <= outleft ? inleft : outleft);
  if (count > 0) {
    *inbytesleft -= count * sizeof(wchar_t);
    *outbytesleft -= count * sizeof(wchar_t);
    do {
      wchar_t wc = *inptr++;
      *outptr++ = wc;
      #ifndef LIBICONV_PLUG
      if (cd->hooks.wc_hook)
        (*cd->hooks.wc_hook)(wc, cd->hooks.data);
      #endif
    } while (--count > 0);
    *inbuf = (const char*) inptr;
    *outbuf = (char*) outptr;
  }
  return 0;
}

static size_t wchar_id_loop_reset (iconv_t icd,
                                   char* * outbuf, size_t *outbytesleft)
{
  return 0;
}
