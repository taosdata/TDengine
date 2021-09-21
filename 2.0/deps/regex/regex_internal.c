/* Extended regular expression matching and search library.
   Copyright (C) 2002, 2003 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Isamu Hasegawa <isamu@yamato.ibm.com>.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301 USA.  */

static void re_string_construct_common (const char *str, int len,
					re_string_t *pstr,
					RE_TRANSLATE_TYPE trans, int icase);
#ifdef RE_ENABLE_I18N
static int re_string_skip_chars (re_string_t *pstr, int new_raw_idx,
				 wint_t *last_wc);
#endif /* RE_ENABLE_I18N */
static re_dfastate_t *create_newstate_common (re_dfa_t *dfa,
					      const re_node_set *nodes,
					      unsigned int hash);
static reg_errcode_t register_state (re_dfa_t *dfa, re_dfastate_t *newstate,
				     unsigned int hash);
static re_dfastate_t *create_ci_newstate (re_dfa_t *dfa,
					  const re_node_set *nodes,
					  unsigned int hash);
static re_dfastate_t *create_cd_newstate (re_dfa_t *dfa,
					  const re_node_set *nodes,
					  unsigned int context,
					  unsigned int hash);
static unsigned int inline calc_state_hash (const re_node_set *nodes,
					    unsigned int context);

/* Functions for string operation.  */

/* This function allocate the buffers.  It is necessary to call
   re_string_reconstruct before using the object.  */

static reg_errcode_t
re_string_allocate (pstr, str, len, init_len, trans, icase)
     re_string_t *pstr;
     const char *str;
     int len, init_len, icase;
     RE_TRANSLATE_TYPE trans;
{
  reg_errcode_t ret;
  int init_buf_len = (len + 1 < init_len) ? len + 1: init_len;
  re_string_construct_common (str, len, pstr, trans, icase);
  pstr->stop = pstr->len;

  ret = re_string_realloc_buffers (pstr, init_buf_len);
  if (BE (ret != REG_NOERROR, 0))
    return ret;

  pstr->mbs_case = (MBS_CASE_ALLOCATED (pstr) ? pstr->mbs_case
		    : (unsigned char *) str);
  pstr->mbs = MBS_ALLOCATED (pstr) ? pstr->mbs : pstr->mbs_case;
  pstr->valid_len = (MBS_CASE_ALLOCATED (pstr) || MBS_ALLOCATED (pstr)
		     || MB_CUR_MAX > 1) ? pstr->valid_len : len;
  return REG_NOERROR;
}

/* This function allocate the buffers, and initialize them.  */

static reg_errcode_t
re_string_construct (pstr, str, len, trans, icase)
     re_string_t *pstr;
     const char *str;
     int len, icase;
     RE_TRANSLATE_TYPE trans;
{
  reg_errcode_t ret;
  re_string_construct_common (str, len, pstr, trans, icase);
  pstr->stop = pstr->len;
  /* Set 0 so that this function can initialize whole buffers.  */
  pstr->valid_len = 0;

  if (len > 0)
    {
      ret = re_string_realloc_buffers (pstr, len + 1);
      if (BE (ret != REG_NOERROR, 0))
	return ret;
    }
  pstr->mbs_case = (MBS_CASE_ALLOCATED (pstr) ? pstr->mbs_case
		    : (unsigned char *) str);
  pstr->mbs = MBS_ALLOCATED (pstr) ? pstr->mbs : pstr->mbs_case;

  if (icase)
    {
#ifdef RE_ENABLE_I18N
      if (MB_CUR_MAX > 1)
	build_wcs_upper_buffer (pstr);
      else
#endif /* RE_ENABLE_I18N  */
	build_upper_buffer (pstr);
    }
  else
    {
#ifdef RE_ENABLE_I18N
      if (MB_CUR_MAX > 1)
	build_wcs_buffer (pstr);
      else
#endif /* RE_ENABLE_I18N  */
	{
	  if (trans != NULL)
	    re_string_translate_buffer (pstr);
	  else
	    pstr->valid_len = len;
	}
    }

  /* Initialized whole buffers, then valid_len == bufs_len.  */
  pstr->valid_len = pstr->bufs_len;
  return REG_NOERROR;
}

/* Helper functions for re_string_allocate, and re_string_construct.  */

static reg_errcode_t
re_string_realloc_buffers (pstr, new_buf_len)
     re_string_t *pstr;
     int new_buf_len;
{
#ifdef RE_ENABLE_I18N
  if (MB_CUR_MAX > 1)
    {
      wint_t *new_array = re_realloc (pstr->wcs, wint_t, new_buf_len);
      if (BE (new_array == NULL, 0))
	return REG_ESPACE;
      pstr->wcs = new_array;
    }
#endif /* RE_ENABLE_I18N  */
  if (MBS_ALLOCATED (pstr))
    {
      unsigned char *new_array = re_realloc (pstr->mbs, unsigned char,
					     new_buf_len);
      if (BE (new_array == NULL, 0))
	return REG_ESPACE;
      pstr->mbs = new_array;
    }
  if (MBS_CASE_ALLOCATED (pstr))
    {
      unsigned char *new_array = re_realloc (pstr->mbs_case, unsigned char,
					     new_buf_len);
      if (BE (new_array == NULL, 0))
	return REG_ESPACE;
      pstr->mbs_case = new_array;
      if (!MBS_ALLOCATED (pstr))
	pstr->mbs = pstr->mbs_case;
    }
  pstr->bufs_len = new_buf_len;
  return REG_NOERROR;
}


static void
re_string_construct_common (str, len, pstr, trans, icase)
     const char *str;
     int len;
     re_string_t *pstr;
     RE_TRANSLATE_TYPE trans;
     int icase;
{
  memset (pstr, '\0', sizeof (re_string_t));
  pstr->raw_mbs = (const unsigned char *) str;
  pstr->len = len;
  pstr->trans = trans;
  pstr->icase = icase ? 1 : 0;
}

#ifdef RE_ENABLE_I18N

/* Build wide character buffer PSTR->WCS.
   If the byte sequence of the string are:
     <mb1>(0), <mb1>(1), <mb2>(0), <mb2>(1), <sb3>
   Then wide character buffer will be:
     <wc1>   , WEOF    , <wc2>   , WEOF    , <wc3>
   We use WEOF for padding, they indicate that the position isn't
   a first byte of a multibyte character.

   Note that this function assumes PSTR->VALID_LEN elements are already
   built and starts from PSTR->VALID_LEN.  */

static void
build_wcs_buffer (pstr)
     re_string_t *pstr;
{
  mbstate_t prev_st;
  int byte_idx, end_idx, mbclen, remain_len;
  /* Build the buffers from pstr->valid_len to either pstr->len or
     pstr->bufs_len.  */
  end_idx = (pstr->bufs_len > pstr->len)? pstr->len : pstr->bufs_len;
  for (byte_idx = pstr->valid_len; byte_idx < end_idx;)
    {
      wchar_t wc;
      remain_len = end_idx - byte_idx;
      prev_st = pstr->cur_state;
      mbclen = mbrtowc (&wc, ((const char *) pstr->raw_mbs + pstr->raw_mbs_idx
			      + byte_idx), remain_len, &pstr->cur_state);
      if (BE (mbclen == (size_t) -2, 0))
	{
	  /* The buffer doesn't have enough space, finish to build.  */
	  pstr->cur_state = prev_st;
	  break;
	}
      else if (BE (mbclen == (size_t) -1 || mbclen == 0, 0))
	{
	  /* We treat these cases as a singlebyte character.  */
	  mbclen = 1;
	  wc = (wchar_t) pstr->raw_mbs[pstr->raw_mbs_idx + byte_idx];
	  pstr->cur_state = prev_st;
	}

      /* Apply the translateion if we need.  */
      if (pstr->trans != NULL && mbclen == 1)
	{
	  int ch = pstr->trans[pstr->raw_mbs[pstr->raw_mbs_idx + byte_idx]];
	  pstr->mbs_case[byte_idx] = ch;
	}
      /* Write wide character and padding.  */
      pstr->wcs[byte_idx++] = wc;
      /* Write paddings.  */
      for (remain_len = byte_idx + mbclen - 1; byte_idx < remain_len ;)
	pstr->wcs[byte_idx++] = WEOF;
    }
  pstr->valid_len = byte_idx;
}

/* Build wide character buffer PSTR->WCS like build_wcs_buffer,
   but for REG_ICASE.  */

static void
build_wcs_upper_buffer (pstr)
     re_string_t *pstr;
{
  mbstate_t prev_st;
  int byte_idx, end_idx, mbclen, remain_len;
  /* Build the buffers from pstr->valid_len to either pstr->len or
     pstr->bufs_len.  */
  end_idx = (pstr->bufs_len > pstr->len)? pstr->len : pstr->bufs_len;
  for (byte_idx = pstr->valid_len; byte_idx < end_idx;)
    {
      wchar_t wc;
      remain_len = end_idx - byte_idx;
      prev_st = pstr->cur_state;
      mbclen = mbrtowc (&wc, ((const char *) pstr->raw_mbs + pstr->raw_mbs_idx
			      + byte_idx), remain_len, &pstr->cur_state);
      if (BE (mbclen == (size_t) -2, 0))
	{
	  /* The buffer doesn't have enough space, finish to build.  */
	  pstr->cur_state = prev_st;
	  break;
	}
      else if (mbclen == 1 || mbclen == (size_t) -1 || mbclen == 0)
	{
	  /* In case of a singlebyte character.  */
	  int ch = pstr->raw_mbs[pstr->raw_mbs_idx + byte_idx];
	  /* Apply the translateion if we need.  */
	  if (pstr->trans != NULL && mbclen == 1)
	    {
	      ch = pstr->trans[ch];
	      pstr->mbs_case[byte_idx] = ch;
	    }
	  pstr->wcs[byte_idx] = iswlower (wc) ? toupper (wc) : wc;
	  pstr->mbs[byte_idx++] = islower (ch) ? toupper (ch) : ch;
	  if (BE (mbclen == (size_t) -1, 0))
	    pstr->cur_state = prev_st;
	}
      else /* mbclen > 1 */
	{
	  if (iswlower (wc))
	    wcrtomb ((char *) pstr->mbs + byte_idx, towupper (wc), &prev_st);
	  else
	    memcpy (pstr->mbs + byte_idx,
		    pstr->raw_mbs + pstr->raw_mbs_idx + byte_idx, mbclen);
	  pstr->wcs[byte_idx++] = iswlower (wc) ? toupper (wc) : wc;
	  /* Write paddings.  */
	  for (remain_len = byte_idx + mbclen - 1; byte_idx < remain_len ;)
	    pstr->wcs[byte_idx++] = WEOF;
	}
    }
  pstr->valid_len = byte_idx;
}

/* Skip characters until the index becomes greater than NEW_RAW_IDX.
   Return the index.  */

static int
re_string_skip_chars (pstr, new_raw_idx, last_wc)
     re_string_t *pstr;
     int new_raw_idx;
     wint_t *last_wc;
{
  mbstate_t prev_st;
  int rawbuf_idx, mbclen;
  wchar_t wc = 0;

  /* Skip the characters which are not necessary to check.  */
  for (rawbuf_idx = pstr->raw_mbs_idx + pstr->valid_len;
       rawbuf_idx < new_raw_idx;)
    {
      int remain_len;
      remain_len = pstr->len - rawbuf_idx;
      prev_st = pstr->cur_state;
      mbclen = mbrtowc (&wc, (const char *) pstr->raw_mbs + rawbuf_idx,
			remain_len, &pstr->cur_state);
      if (BE (mbclen == (size_t) -2 || mbclen == (size_t) -1 || mbclen == 0, 0))
	{
	  /* We treat these cases as a singlebyte character.  */
	  mbclen = 1;
	  pstr->cur_state = prev_st;
	}
      /* Then proceed the next character.  */
      rawbuf_idx += mbclen;
    }
  *last_wc = (wint_t) wc;
  return rawbuf_idx;
}
#endif /* RE_ENABLE_I18N  */

/* Build the buffer PSTR->MBS, and apply the translation if we need.
   This function is used in case of REG_ICASE.  */

static void
build_upper_buffer (pstr)
     re_string_t *pstr;
{
  int char_idx, end_idx;
  end_idx = (pstr->bufs_len > pstr->len) ? pstr->len : pstr->bufs_len;

  for (char_idx = pstr->valid_len; char_idx < end_idx; ++char_idx)
    {
      int ch = pstr->raw_mbs[pstr->raw_mbs_idx + char_idx];
      if (pstr->trans != NULL)
	{
	  ch =  pstr->trans[ch];
	  pstr->mbs_case[char_idx] = ch;
	}
      if (islower (ch))
	pstr->mbs[char_idx] = toupper (ch);
      else
	pstr->mbs[char_idx] = ch;
    }
  pstr->valid_len = char_idx;
}

/* Apply TRANS to the buffer in PSTR.  */

static void
re_string_translate_buffer (pstr)
     re_string_t *pstr;
{
  int buf_idx, end_idx;
  end_idx = (pstr->bufs_len > pstr->len) ? pstr->len : pstr->bufs_len;

  for (buf_idx = pstr->valid_len; buf_idx < end_idx; ++buf_idx)
    {
      int ch = pstr->raw_mbs[pstr->raw_mbs_idx + buf_idx];
      pstr->mbs_case[buf_idx] = pstr->trans[ch];
    }

  pstr->valid_len = buf_idx;
}

/* This function re-construct the buffers.
   Concretely, convert to wide character in case of MB_CUR_MAX > 1,
   convert to upper case in case of REG_ICASE, apply translation.  */

static reg_errcode_t
re_string_reconstruct (pstr, idx, eflags, newline)
     re_string_t *pstr;
     int idx, eflags, newline;
{
  int offset = idx - pstr->raw_mbs_idx;
  if (offset < 0)
    {
      /* Reset buffer.  */
#ifdef RE_ENABLE_I18N
      if (MB_CUR_MAX > 1)
	memset (&pstr->cur_state, '\0', sizeof (mbstate_t));
#endif /* RE_ENABLE_I18N */
      pstr->len += pstr->raw_mbs_idx;
      pstr->stop += pstr->raw_mbs_idx;
      pstr->valid_len = pstr->raw_mbs_idx = 0;
      pstr->tip_context = ((eflags & REG_NOTBOL) ? CONTEXT_BEGBUF
			   : CONTEXT_NEWLINE | CONTEXT_BEGBUF);
      if (!MBS_CASE_ALLOCATED (pstr))
	pstr->mbs_case = (unsigned char *) pstr->raw_mbs;
      if (!MBS_ALLOCATED (pstr) && !MBS_CASE_ALLOCATED (pstr))
	pstr->mbs = (unsigned char *) pstr->raw_mbs;
      offset = idx;
    }

  if (offset != 0)
    {
      /* Are the characters which are already checked remain?  */
      if (offset < pstr->valid_len)
	{
	  /* Yes, move them to the front of the buffer.  */
	  pstr->tip_context = re_string_context_at (pstr, offset - 1, eflags,
						    newline);
#ifdef RE_ENABLE_I18N
	  if (MB_CUR_MAX > 1)
	    memmove (pstr->wcs, pstr->wcs + offset,
		     (pstr->valid_len - offset) * sizeof (wint_t));
#endif /* RE_ENABLE_I18N */
	  if (MBS_ALLOCATED (pstr))
	    memmove (pstr->mbs, pstr->mbs + offset,
		     pstr->valid_len - offset);
	  if (MBS_CASE_ALLOCATED (pstr))
	    memmove (pstr->mbs_case, pstr->mbs_case + offset,
		     pstr->valid_len - offset);
	  pstr->valid_len -= offset;
#if DEBUG
	  assert (pstr->valid_len > 0);
#endif
	}
      else
	{
	  /* No, skip all characters until IDX.  */
	  pstr->valid_len = 0;
#ifdef RE_ENABLE_I18N
	  if (MB_CUR_MAX > 1)
	    {
	      int wcs_idx;
	      wint_t wc;
	      pstr->valid_len = re_string_skip_chars (pstr, idx, &wc) - idx;
	      for (wcs_idx = 0; wcs_idx < pstr->valid_len; ++wcs_idx)
		pstr->wcs[wcs_idx] = WEOF;
	      if (pstr->trans && wc <= 0xff)
		wc = pstr->trans[wc];
	      pstr->tip_context = (IS_WIDE_WORD_CHAR (wc) ? CONTEXT_WORD
				   : ((newline && IS_WIDE_NEWLINE (wc))
				      ? CONTEXT_NEWLINE : 0));
	    }
	  else
#endif /* RE_ENABLE_I18N */
	    {
	      int c = pstr->raw_mbs[pstr->raw_mbs_idx + offset - 1];
	      if (pstr->trans)
		c = pstr->trans[c];
	      pstr->tip_context = (IS_WORD_CHAR (c) ? CONTEXT_WORD
				   : ((newline && IS_NEWLINE (c))
				      ? CONTEXT_NEWLINE : 0));
	    }
	}
      if (!MBS_CASE_ALLOCATED (pstr))
	{
	  pstr->mbs_case += offset;
	  /* In case of !MBS_ALLOCATED && !MBS_CASE_ALLOCATED.  */
	  if (!MBS_ALLOCATED (pstr))
	    pstr->mbs += offset;
	}
    }
  pstr->raw_mbs_idx = idx;
  pstr->len -= offset;
  pstr->stop -= offset;

  /* Then build the buffers.  */
#ifdef RE_ENABLE_I18N
  if (MB_CUR_MAX > 1)
    {
      if (pstr->icase)
	build_wcs_upper_buffer (pstr);
      else
	build_wcs_buffer (pstr);
    }
  else
#endif /* RE_ENABLE_I18N */
    {
      if (pstr->icase)
	build_upper_buffer (pstr);
      else if (pstr->trans != NULL)
	re_string_translate_buffer (pstr);
    }
  pstr->cur_idx = 0;

  return REG_NOERROR;
}

static void
re_string_destruct (pstr)
     re_string_t *pstr;
{
#ifdef RE_ENABLE_I18N
  re_free (pstr->wcs);
#endif /* RE_ENABLE_I18N  */
  if (MBS_ALLOCATED (pstr))
    re_free (pstr->mbs);
  if (MBS_CASE_ALLOCATED (pstr))
    re_free (pstr->mbs_case);
}

/* Return the context at IDX in INPUT.  */

static unsigned int
re_string_context_at (input, idx, eflags, newline_anchor)
     const re_string_t *input;
     int idx, eflags, newline_anchor;
{
  int c;
  if (idx < 0 || idx == input->len)
    {
      if (idx < 0)
	/* In this case, we use the value stored in input->tip_context,
	   since we can't know the character in input->mbs[-1] here.  */
	return input->tip_context;
      else /* (idx == input->len) */
	return ((eflags & REG_NOTEOL) ? CONTEXT_ENDBUF
		: CONTEXT_NEWLINE | CONTEXT_ENDBUF);
    }
#ifdef RE_ENABLE_I18N
  if (MB_CUR_MAX > 1)
    {
      wint_t wc;
      int wc_idx = idx;
      while(input->wcs[wc_idx] == WEOF)
	{
#ifdef DEBUG
	  /* It must not happen.  */
	  assert (wc_idx >= 0);
#endif
	  --wc_idx;
	  if (wc_idx < 0)
	    return input->tip_context;
	}
      wc = input->wcs[wc_idx];
      if (IS_WIDE_WORD_CHAR (wc))
	return CONTEXT_WORD;
      return (newline_anchor && IS_WIDE_NEWLINE (wc)) ? CONTEXT_NEWLINE : 0;
    }
  else
#endif
    {
      c = re_string_byte_at (input, idx);
      if (IS_WORD_CHAR (c))
	return CONTEXT_WORD;
      return (newline_anchor && IS_NEWLINE (c)) ? CONTEXT_NEWLINE : 0;
    }
}

/* Functions for set operation.  */

static reg_errcode_t
re_node_set_alloc (set, size)
     re_node_set *set;
     int size;
{
  set->alloc = size;
  set->nelem = 0;
  set->elems = re_malloc (int, size);
  if (BE (set->elems == NULL, 0))
    return REG_ESPACE;
  return REG_NOERROR;
}

static reg_errcode_t
re_node_set_init_1 (set, elem)
     re_node_set *set;
     int elem;
{
  set->alloc = 1;
  set->nelem = 1;
  set->elems = re_malloc (int, 1);
  if (BE (set->elems == NULL, 0))
    {
      set->alloc = set->nelem = 0;
      return REG_ESPACE;
    }
  set->elems[0] = elem;
  return REG_NOERROR;
}

static reg_errcode_t
re_node_set_init_2 (set, elem1, elem2)
     re_node_set *set;
     int elem1, elem2;
{
  set->alloc = 2;
  set->elems = re_malloc (int, 2);
  if (BE (set->elems == NULL, 0))
    return REG_ESPACE;
  if (elem1 == elem2)
    {
      set->nelem = 1;
      set->elems[0] = elem1;
    }
  else
    {
      set->nelem = 2;
      if (elem1 < elem2)
	{
	  set->elems[0] = elem1;
	  set->elems[1] = elem2;
	}
      else
	{
	  set->elems[0] = elem2;
	  set->elems[1] = elem1;
	}
    }
  return REG_NOERROR;
}

static reg_errcode_t
re_node_set_init_copy (dest, src)
     re_node_set *dest;
     const re_node_set *src;
{
  dest->nelem = src->nelem;
  if (src->nelem > 0)
    {
      dest->alloc = dest->nelem;
      dest->elems = re_malloc (int, dest->alloc);
      if (BE (dest->elems == NULL, 0))
	{
	  dest->alloc = dest->nelem = 0;
	  return REG_ESPACE;
	}
      memcpy (dest->elems, src->elems, src->nelem * sizeof (int));
    }
  else
    re_node_set_init_empty (dest);
  return REG_NOERROR;
}

/* Calculate the intersection of the sets SRC1 and SRC2. And merge it to
   DEST. Return value indicate the error code or REG_NOERROR if succeeded.
   Note: We assume dest->elems is NULL, when dest->alloc is 0.  */

static reg_errcode_t
re_node_set_add_intersect (dest, src1, src2)
     re_node_set *dest;
     const re_node_set *src1, *src2;
{
  int i1, i2, id;
  if (src1->nelem > 0 && src2->nelem > 0)
    {
      if (src1->nelem + src2->nelem + dest->nelem > dest->alloc)
	{
	  dest->alloc = src1->nelem + src2->nelem + dest->nelem;
	  dest->elems = re_realloc (dest->elems, int, dest->alloc);
	  if (BE (dest->elems == NULL, 0))
	    return REG_ESPACE;
	}
    }
  else
    return REG_NOERROR;

  for (i1 = i2 = id = 0 ; i1 < src1->nelem && i2 < src2->nelem ;)
    {
      if (src1->elems[i1] > src2->elems[i2])
	{
	  ++i2;
	  continue;
	}
      if (src1->elems[i1] == src2->elems[i2])
	{
	  while (id < dest->nelem && dest->elems[id] < src2->elems[i2])
	    ++id;
	  if (id < dest->nelem && dest->elems[id] == src2->elems[i2])
	    ++id;
	  else
	    {
	      memmove (dest->elems + id + 1, dest->elems + id,
		       sizeof (int) * (dest->nelem - id));
	      dest->elems[id++] = src2->elems[i2++];
	      ++dest->nelem;
	    }
	}
      ++i1;
    }
  return REG_NOERROR;
}

/* Calculate the union set of the sets SRC1 and SRC2. And store it to
   DEST. Return value indicate the error code or REG_NOERROR if succeeded.  */

static reg_errcode_t
re_node_set_init_union (dest, src1, src2)
     re_node_set *dest;
     const re_node_set *src1, *src2;
{
  int i1, i2, id;
  if (src1 != NULL && src1->nelem > 0 && src2 != NULL && src2->nelem > 0)
    {
      dest->alloc = src1->nelem + src2->nelem;
      dest->elems = re_malloc (int, dest->alloc);
      if (BE (dest->elems == NULL, 0))
	return REG_ESPACE;
    }
  else
    {
      if (src1 != NULL && src1->nelem > 0)
	return re_node_set_init_copy (dest, src1);
      else if (src2 != NULL && src2->nelem > 0)
	return re_node_set_init_copy (dest, src2);
      else
	re_node_set_init_empty (dest);
      return REG_NOERROR;
    }
  for (i1 = i2 = id = 0 ; i1 < src1->nelem && i2 < src2->nelem ;)
    {
      if (src1->elems[i1] > src2->elems[i2])
	{
	  dest->elems[id++] = src2->elems[i2++];
	  continue;
	}
      if (src1->elems[i1] == src2->elems[i2])
	++i2;
      dest->elems[id++] = src1->elems[i1++];
    }
  if (i1 < src1->nelem)
    {
      memcpy (dest->elems + id, src1->elems + i1,
	     (src1->nelem - i1) * sizeof (int));
      id += src1->nelem - i1;
    }
  else if (i2 < src2->nelem)
    {
      memcpy (dest->elems + id, src2->elems + i2,
	     (src2->nelem - i2) * sizeof (int));
      id += src2->nelem - i2;
    }
  dest->nelem = id;
  return REG_NOERROR;
}

/* Calculate the union set of the sets DEST and SRC. And store it to
   DEST. Return value indicate the error code or REG_NOERROR if succeeded.  */

static reg_errcode_t
re_node_set_merge (dest, src)
     re_node_set *dest;
     const re_node_set *src;
{
  int si, di;
  if (src == NULL || src->nelem == 0)
    return REG_NOERROR;
  if (dest->alloc < src->nelem + dest->nelem)
    {
      int *new_buffer;
      dest->alloc = 2 * (src->nelem + dest->alloc);
      new_buffer = re_realloc (dest->elems, int, dest->alloc);
      if (BE (new_buffer == NULL, 0))
	return REG_ESPACE;
      dest->elems = new_buffer;
    }

  for (si = 0, di = 0 ; si < src->nelem && di < dest->nelem ;)
    {
      int cp_from, ncp, mid, right, src_elem = src->elems[si];
      /* Binary search the spot we will add the new element.  */
      right = dest->nelem;
      while (di < right)
	{
	  mid = (di + right) / 2;
	  if (dest->elems[mid] < src_elem)
	    di = mid + 1;
	  else
	    right = mid;
	}
      if (di >= dest->nelem)
	break;

      if (dest->elems[di] == src_elem)
	{
	  /* Skip since, DEST already has the element.  */
	  ++di;
	  ++si;
	  continue;
	}

      /* Skip the src elements which are less than dest->elems[di].  */
      cp_from = si;
      while (si < src->nelem && src->elems[si] < dest->elems[di])
	++si;
      /* Copy these src elements.  */
      ncp = si - cp_from;
      memmove (dest->elems + di + ncp, dest->elems + di,
	       sizeof (int) * (dest->nelem - di));
      memcpy (dest->elems + di, src->elems + cp_from,
	      sizeof (int) * ncp);
      /* Update counters.  */
      di += ncp;
      dest->nelem += ncp;
    }

  /* Copy remaining src elements.  */
  if (si < src->nelem)
    {
      memcpy (dest->elems + di, src->elems + si,
	      sizeof (int) * (src->nelem - si));
      dest->nelem += src->nelem - si;
    }
  return REG_NOERROR;
}

/* Insert the new element ELEM to the re_node_set* SET.
   return 0 if SET already has ELEM,
   return -1 if an error is occured, return 1 otherwise.  */

static int
re_node_set_insert (set, elem)
     re_node_set *set;
     int elem;
{
  int idx, right, mid;
  /* In case of the set is empty.  */
  if (set->elems == NULL || set->alloc == 0)
    {
      if (BE (re_node_set_init_1 (set, elem) == REG_NOERROR, 1))
	return 1;
      else
	return -1;
    }

  /* Binary search the spot we will add the new element.  */
  idx = 0;
  right = set->nelem;
  while (idx < right)
    {
      mid = (idx + right) / 2;
      if (set->elems[mid] < elem)
	idx = mid + 1;
      else
	right = mid;
    }

  /* Realloc if we need.  */
  if (set->alloc < set->nelem + 1)
    {
      int *new_array;
      set->alloc = set->alloc * 2;
      new_array = re_malloc (int, set->alloc);
      if (BE (new_array == NULL, 0))
	return -1;
      /* Copy the elements they are followed by the new element.  */
      if (idx > 0)
	memcpy (new_array, set->elems, sizeof (int) * (idx));
      /* Copy the elements which follows the new element.  */
      if (set->nelem - idx > 0)
	memcpy (new_array + idx + 1, set->elems + idx,
		sizeof (int) * (set->nelem - idx));
      re_free (set->elems);
      set->elems = new_array;
    }
  else
    {
      /* Move the elements which follows the new element.  */
      if (set->nelem - idx > 0)
	memmove (set->elems + idx + 1, set->elems + idx,
		 sizeof (int) * (set->nelem - idx));
    }
  /* Insert the new element.  */
  set->elems[idx] = elem;
  ++set->nelem;
  return 1;
}

/* Compare two node sets SET1 and SET2.
   return 1 if SET1 and SET2 are equivalent, retrun 0 otherwise.  */

static int
re_node_set_compare (set1, set2)
     const re_node_set *set1, *set2;
{
  int i;
  if (set1 == NULL || set2 == NULL || set1->nelem != set2->nelem)
    return 0;
  for (i = 0 ; i < set1->nelem ; i++)
    if (set1->elems[i] != set2->elems[i])
      return 0;
  return 1;
}

/* Return (idx + 1) if SET contains the element ELEM, return 0 otherwise.  */

static int
re_node_set_contains (set, elem)
     const re_node_set *set;
     int elem;
{
  int idx, right, mid;
  if (set->nelem <= 0)
    return 0;

  /* Binary search the element.  */
  idx = 0;
  right = set->nelem - 1;
  while (idx < right)
    {
      mid = (idx + right) / 2;
      if (set->elems[mid] < elem)
	idx = mid + 1;
      else
	right = mid;
    }
  return set->elems[idx] == elem ? idx + 1 : 0;
}

static void
re_node_set_remove_at (set, idx)
     re_node_set *set;
     int idx;
{
  if (idx < 0 || idx >= set->nelem)
    return;
  if (idx < set->nelem - 1)
    memmove (set->elems + idx, set->elems + idx + 1,
	     sizeof (int) * (set->nelem - idx - 1));
  --set->nelem;
}


/* Add the token TOKEN to dfa->nodes, and return the index of the token.
   Or return -1, if an error will be occured.  */

static int
re_dfa_add_node (dfa, token, mode)
     re_dfa_t *dfa;
     re_token_t token;
     int mode;
{
  if (dfa->nodes_len >= dfa->nodes_alloc)
    {
      re_token_t *new_array;
      dfa->nodes_alloc *= 2;
      new_array = re_realloc (dfa->nodes, re_token_t, dfa->nodes_alloc);
      if (BE (new_array == NULL, 0))
	return -1;
      else
	dfa->nodes = new_array;
      if (mode)
	{
	  int *new_nexts, *new_indices;
	  re_node_set *new_edests, *new_eclosures, *new_inveclosures;

	  new_nexts = re_realloc (dfa->nexts, int, dfa->nodes_alloc);
	  new_indices = re_realloc (dfa->org_indices, int, dfa->nodes_alloc);
	  new_edests = re_realloc (dfa->edests, re_node_set, dfa->nodes_alloc);
	  new_eclosures = re_realloc (dfa->eclosures, re_node_set,
				      dfa->nodes_alloc);
	  new_inveclosures = re_realloc (dfa->inveclosures, re_node_set,
					 dfa->nodes_alloc);
	  if (BE (new_nexts == NULL || new_indices == NULL
		  || new_edests == NULL || new_eclosures == NULL
		  || new_inveclosures == NULL, 0))
	    return -1;
	  dfa->nexts = new_nexts;
	  dfa->org_indices = new_indices;
	  dfa->edests = new_edests;
	  dfa->eclosures = new_eclosures;
	  dfa->inveclosures = new_inveclosures;
	}
    }
  dfa->nodes[dfa->nodes_len] = token;
  dfa->nodes[dfa->nodes_len].duplicated = 0;
  dfa->nodes[dfa->nodes_len].constraint = 0;
  return dfa->nodes_len++;
}

static unsigned int inline
calc_state_hash (nodes, context)
     const re_node_set *nodes;
     unsigned int context;
{
  unsigned int hash = nodes->nelem + context;
  int i;
  for (i = 0 ; i < nodes->nelem ; i++)
    hash += nodes->elems[i];
  return hash;
}

/* Search for the state whose node_set is equivalent to NODES.
   Return the pointer to the state, if we found it in the DFA.
   Otherwise create the new one and return it.  In case of an error
   return NULL and set the error code in ERR.
   Note: - We assume NULL as the invalid state, then it is possible that
	   return value is NULL and ERR is REG_NOERROR.
	 - We never return non-NULL value in case of any errors, it is for
	   optimization.  */

static re_dfastate_t*
re_acquire_state (err, dfa, nodes)
     reg_errcode_t *err;
     re_dfa_t *dfa;
     const re_node_set *nodes;
{
  unsigned int hash;
  re_dfastate_t *new_state;
  struct re_state_table_entry *spot;
  int i;
  if (BE (nodes->nelem == 0, 0))
    {
      *err = REG_NOERROR;
      return NULL;
    }
  hash = calc_state_hash (nodes, 0);
  spot = dfa->state_table + (hash & dfa->state_hash_mask);

  for (i = 0 ; i < spot->num ; i++)
    {
      re_dfastate_t *state = spot->array[i];
      if (hash != state->hash)
	continue;
      if (re_node_set_compare (&state->nodes, nodes))
	return state;
    }

  /* There are no appropriate state in the dfa, create the new one.  */
  new_state = create_ci_newstate (dfa, nodes, hash);
  if (BE (new_state != NULL, 1))
    return new_state;
  else
    {
      *err = REG_ESPACE;
      return NULL;
    }
}

/* Search for the state whose node_set is equivalent to NODES and
   whose context is equivalent to CONTEXT.
   Return the pointer to the state, if we found it in the DFA.
   Otherwise create the new one and return it.  In case of an error
   return NULL and set the error code in ERR.
   Note: - We assume NULL as the invalid state, then it is possible that
	   return value is NULL and ERR is REG_NOERROR.
	 - We never return non-NULL value in case of any errors, it is for
	   optimization.  */

static re_dfastate_t*
re_acquire_state_context (err, dfa, nodes, context)
     reg_errcode_t *err;
     re_dfa_t *dfa;
     const re_node_set *nodes;
     unsigned int context;
{
  unsigned int hash;
  re_dfastate_t *new_state;
  struct re_state_table_entry *spot;
  int i;
  if (nodes->nelem == 0)
    {
      *err = REG_NOERROR;
      return NULL;
    }
  hash = calc_state_hash (nodes, context);
  spot = dfa->state_table + (hash & dfa->state_hash_mask);

  for (i = 0 ; i < spot->num ; i++)
    {
      re_dfastate_t *state = spot->array[i];
      if (hash != state->hash)
	continue;
      if (re_node_set_compare (state->entrance_nodes, nodes)
	  && state->context == context)
	return state;
    }
  /* There are no appropriate state in `dfa', create the new one.  */
  new_state = create_cd_newstate (dfa, nodes, context, hash);
  if (BE (new_state != NULL, 1))
    return new_state;
  else
    {
      *err = REG_ESPACE;
      return NULL;
    }
}

/* Allocate memory for DFA state and initialize common properties.
   Return the new state if succeeded, otherwise return NULL.  */

static re_dfastate_t *
create_newstate_common (dfa, nodes, hash)
     re_dfa_t *dfa;
     const re_node_set *nodes;
     unsigned int hash;
{
  re_dfastate_t *newstate;
  reg_errcode_t err;
  newstate = (re_dfastate_t *) calloc (sizeof (re_dfastate_t), 1);
  if (BE (newstate == NULL, 0))
    return NULL;
  err = re_node_set_init_copy (&newstate->nodes, nodes);
  if (BE (err != REG_NOERROR, 0))
    {
      re_free (newstate);
      return NULL;
    }
  newstate->trtable = NULL;
  newstate->trtable_search = NULL;
  newstate->hash = hash;
  return newstate;
}

/* Store the new state NEWSTATE whose hash value is HASH in appropriate
   position.  Return value indicate the error code if failed.  */

static reg_errcode_t
register_state (dfa, newstate, hash)
     re_dfa_t *dfa;
     re_dfastate_t *newstate;
     unsigned int hash;
{
  struct re_state_table_entry *spot;
  spot = dfa->state_table + (hash & dfa->state_hash_mask);

  if (spot->alloc <= spot->num)
    {
      re_dfastate_t **new_array;
      spot->alloc = 2 * spot->num + 2;
      new_array = re_realloc (spot->array, re_dfastate_t *, spot->alloc);
      if (BE (new_array == NULL, 0))
	return REG_ESPACE;
      spot->array = new_array;
    }
  spot->array[spot->num++] = newstate;
  return REG_NOERROR;
}

/* Create the new state which is independ of contexts.
   Return the new state if succeeded, otherwise return NULL.  */

static re_dfastate_t *
create_ci_newstate (dfa, nodes, hash)
     re_dfa_t *dfa;
     const re_node_set *nodes;
     unsigned int hash;
{
  int i;
  reg_errcode_t err;
  re_dfastate_t *newstate;
  newstate = create_newstate_common (dfa, nodes, hash);
  if (BE (newstate == NULL, 0))
    return NULL;
  newstate->entrance_nodes = &newstate->nodes;

  for (i = 0 ; i < nodes->nelem ; i++)
    {
      re_token_t *node = dfa->nodes + nodes->elems[i];
      re_token_type_t type = node->type;
      if (type == CHARACTER && !node->constraint)
	continue;

      /* If the state has the halt node, the state is a halt state.  */
      else if (type == END_OF_RE)
	newstate->halt = 1;
#ifdef RE_ENABLE_I18N
      else if (type == COMPLEX_BRACKET
	       || (type == OP_PERIOD && MB_CUR_MAX > 1))
	newstate->accept_mb = 1;
#endif /* RE_ENABLE_I18N */
      else if (type == OP_BACK_REF)
	newstate->has_backref = 1;
      else if (type == ANCHOR || node->constraint)
	newstate->has_constraint = 1;
    }
  err = register_state (dfa, newstate, hash);
  if (BE (err != REG_NOERROR, 0))
    {
      free_state (newstate);
      newstate = NULL;
    }
  return newstate;
}

/* Create the new state which is depend on the context CONTEXT.
   Return the new state if succeeded, otherwise return NULL.  */

static re_dfastate_t *
create_cd_newstate (dfa, nodes, context, hash)
     re_dfa_t *dfa;
     const re_node_set *nodes;
     unsigned int context, hash;
{
  int i, nctx_nodes = 0;
  reg_errcode_t err;
  re_dfastate_t *newstate;

  newstate = create_newstate_common (dfa, nodes, hash);
  if (BE (newstate == NULL, 0))
    return NULL;
  newstate->context = context;
  newstate->entrance_nodes = &newstate->nodes;

  for (i = 0 ; i < nodes->nelem ; i++)
    {
      unsigned int constraint = 0;
      re_token_t *node = dfa->nodes + nodes->elems[i];
      re_token_type_t type = node->type;
      if (node->constraint)
	constraint = node->constraint;

      if (type == CHARACTER && !constraint)
	continue;
      /* If the state has the halt node, the state is a halt state.  */
      else if (type == END_OF_RE)
	newstate->halt = 1;
#ifdef RE_ENABLE_I18N
      else if (type == COMPLEX_BRACKET
	       || (type == OP_PERIOD && MB_CUR_MAX > 1))
	newstate->accept_mb = 1;
#endif /* RE_ENABLE_I18N */
      else if (type == OP_BACK_REF)
	newstate->has_backref = 1;
      else if (type == ANCHOR)
	constraint = node->opr.ctx_type;

      if (constraint)
	{
	  if (newstate->entrance_nodes == &newstate->nodes)
	    {
	      newstate->entrance_nodes = re_malloc (re_node_set, 1);
	      if (BE (newstate->entrance_nodes == NULL, 0))
		{
		  free_state (newstate);
		  return NULL;
		}
	      re_node_set_init_copy (newstate->entrance_nodes, nodes);
	      nctx_nodes = 0;
	      newstate->has_constraint = 1;
	    }

	  if (NOT_SATISFY_PREV_CONSTRAINT (constraint,context))
	    {
	      re_node_set_remove_at (&newstate->nodes, i - nctx_nodes);
	      ++nctx_nodes;
	    }
	}
    }
  err = register_state (dfa, newstate, hash);
  if (BE (err != REG_NOERROR, 0))
    {
      free_state (newstate);
      newstate = NULL;
    }
  return  newstate;
}

static void
free_state (state)
     re_dfastate_t *state;
{
  if (state->entrance_nodes != &state->nodes)
    {
      re_node_set_free (state->entrance_nodes);
      re_free (state->entrance_nodes);
    }
  re_node_set_free (&state->nodes);
  re_free (state->trtable);
  re_free (state->trtable_search);
  re_free (state);
}
