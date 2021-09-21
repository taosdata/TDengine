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

#pragma warning( disable : 4047 )

static reg_errcode_t match_ctx_init (re_match_context_t *cache, int eflags,
				     re_string_t *input, int n);
static void match_ctx_clean (re_match_context_t *mctx);
static void match_ctx_free (re_match_context_t *cache);
static void match_ctx_free_subtops (re_match_context_t *mctx);
static reg_errcode_t match_ctx_add_entry (re_match_context_t *cache, int node,
					  int str_idx, int from, int to);
static int search_cur_bkref_entry (re_match_context_t *mctx, int str_idx);
static void match_ctx_clear_flag (re_match_context_t *mctx);
static reg_errcode_t match_ctx_add_subtop (re_match_context_t *mctx, int node,
					   int str_idx);
static re_sub_match_last_t * match_ctx_add_sublast (re_sub_match_top_t *subtop,
						   int node, int str_idx);
static void sift_ctx_init (re_sift_context_t *sctx, re_dfastate_t **sifted_sts,
			   re_dfastate_t **limited_sts, int last_node,
			   int last_str_idx, int check_subexp);
static reg_errcode_t re_search_internal (const regex_t *preg,
					 const char *string, int length,
					 int start, int range, int stop,
					 size_t nmatch, regmatch_t pmatch[],
					 int eflags);
static int re_search_2_stub (struct re_pattern_buffer *bufp,
			     const char *string1, int length1,
			     const char *string2, int length2,
			     int start, int range, struct re_registers *regs,
			     int stop, int ret_len);
static int re_search_stub (struct re_pattern_buffer *bufp,
			   const char *string, int length, int start,
			   int range, int stop, struct re_registers *regs,
			   int ret_len);
static unsigned re_copy_regs (struct re_registers *regs, regmatch_t *pmatch,
			      int nregs, int regs_allocated);
static inline re_dfastate_t *acquire_init_state_context (reg_errcode_t *err,
							 const regex_t *preg,
							 const re_match_context_t *mctx,
							 int idx);
static reg_errcode_t prune_impossible_nodes (const regex_t *preg,
					     re_match_context_t *mctx);
static int check_matching (const regex_t *preg, re_match_context_t *mctx,
			   int fl_search, int fl_longest_match);
static int check_halt_node_context (const re_dfa_t *dfa, int node,
				    unsigned int context);
static int check_halt_state_context (const regex_t *preg,
				     const re_dfastate_t *state,
				     const re_match_context_t *mctx, int idx);
static void update_regs (re_dfa_t *dfa, regmatch_t *pmatch, int cur_node,
			 int cur_idx, int nmatch);
static int proceed_next_node (const regex_t *preg, int nregs, regmatch_t *regs,
			      const re_match_context_t *mctx,
			      int *pidx, int node, re_node_set *eps_via_nodes,
			      struct re_fail_stack_t *fs);
static reg_errcode_t push_fail_stack (struct re_fail_stack_t *fs,
				      int str_idx, int *dests, int nregs,
				      regmatch_t *regs,
				      re_node_set *eps_via_nodes);
static int pop_fail_stack (struct re_fail_stack_t *fs, int *pidx, int nregs,
			   regmatch_t *regs, re_node_set *eps_via_nodes);
static reg_errcode_t set_regs (const regex_t *preg,
			       const re_match_context_t *mctx,
			       size_t nmatch, regmatch_t *pmatch,
			       int fl_backtrack);
static reg_errcode_t free_fail_stack_return (struct re_fail_stack_t *fs);

#ifdef RE_ENABLE_I18N
static int sift_states_iter_mb (const regex_t *preg,
				const re_match_context_t *mctx,
				re_sift_context_t *sctx,
				int node_idx, int str_idx, int max_str_idx);
#endif /* RE_ENABLE_I18N */
static reg_errcode_t sift_states_backward (const regex_t *preg,
					   re_match_context_t *mctx,
					   re_sift_context_t *sctx);
static reg_errcode_t update_cur_sifted_state (const regex_t *preg,
					      re_match_context_t *mctx,
					      re_sift_context_t *sctx,
					      int str_idx,
					      re_node_set *dest_nodes);
static reg_errcode_t add_epsilon_src_nodes (re_dfa_t *dfa,
					    re_node_set *dest_nodes,
					    const re_node_set *candidates);
static reg_errcode_t sub_epsilon_src_nodes (re_dfa_t *dfa, int node,
					    re_node_set *dest_nodes,
					    const re_node_set *and_nodes);
static int check_dst_limits (re_dfa_t *dfa, re_node_set *limits,
			     re_match_context_t *mctx, int dst_node,
			     int dst_idx, int src_node, int src_idx);
static int check_dst_limits_calc_pos (re_dfa_t *dfa, re_match_context_t *mctx,
				      int limit, re_node_set *eclosures,
				      int subexp_idx, int node, int str_idx);
static reg_errcode_t check_subexp_limits (re_dfa_t *dfa,
					  re_node_set *dest_nodes,
					  const re_node_set *candidates,
					  re_node_set *limits,
					  struct re_backref_cache_entry *bkref_ents,
					  int str_idx);
static reg_errcode_t sift_states_bkref (const regex_t *preg,
					re_match_context_t *mctx,
					re_sift_context_t *sctx,
					int str_idx, re_node_set *dest_nodes);
static reg_errcode_t clean_state_log_if_need (re_match_context_t *mctx,
					      int next_state_log_idx);
static reg_errcode_t merge_state_array (re_dfa_t *dfa, re_dfastate_t **dst,
					re_dfastate_t **src, int num);
static re_dfastate_t *transit_state (reg_errcode_t *err, const regex_t *preg,
				     re_match_context_t *mctx,
				     re_dfastate_t *state, int fl_search);
static reg_errcode_t check_subexp_matching_top (re_dfa_t *dfa,
						re_match_context_t *mctx,
						re_node_set *cur_nodes,
						int str_idx);
static re_dfastate_t *transit_state_sb (reg_errcode_t *err, const regex_t *preg,
					re_dfastate_t *pstate,
					int fl_search,
					re_match_context_t *mctx);
#ifdef RE_ENABLE_I18N
static reg_errcode_t transit_state_mb (const regex_t *preg,
				       re_dfastate_t *pstate,
				       re_match_context_t *mctx);
#endif /* RE_ENABLE_I18N */
static reg_errcode_t transit_state_bkref (const regex_t *preg,
					  re_node_set *nodes,
					  re_match_context_t *mctx);
static reg_errcode_t get_subexp (const regex_t *preg, re_match_context_t *mctx,
				 int bkref_node, int bkref_str_idx);
static reg_errcode_t get_subexp_sub (const regex_t *preg,
				     re_match_context_t *mctx,
				     re_sub_match_top_t *sub_top,
				     re_sub_match_last_t *sub_last,
				     int bkref_node, int bkref_str);
static int find_subexp_node (re_dfa_t *dfa, re_node_set *nodes,
			     int subexp_idx, int fl_open);
static reg_errcode_t check_arrival (const regex_t *preg,
				    re_match_context_t *mctx,
				    state_array_t *path, int top_node,
				    int top_str, int last_node, int last_str,
				    int fl_open);
static reg_errcode_t check_arrival_add_next_nodes (const regex_t *preg,
						   re_dfa_t *dfa,
						   re_match_context_t *mctx,
						   int str_idx,
						   re_node_set *cur_nodes,
						   re_node_set *next_nodes);
static reg_errcode_t check_arrival_expand_ecl (re_dfa_t *dfa,
					       re_node_set *cur_nodes,
					       int ex_subexp, int fl_open);
static reg_errcode_t check_arrival_expand_ecl_sub (re_dfa_t *dfa,
						   re_node_set *dst_nodes,
						   int target, int ex_subexp,
						   int fl_open);
static reg_errcode_t expand_bkref_cache (const regex_t *preg,
					 re_match_context_t *mctx,
					 re_node_set *cur_nodes, int cur_str,
					 int last_str, int subexp_num,
					 int fl_open);
static re_dfastate_t **build_trtable (const regex_t *dfa,
				      const re_dfastate_t *state,
				      int fl_search);
#ifdef RE_ENABLE_I18N
static int check_node_accept_bytes (const regex_t *preg, int node_idx,
				    const re_string_t *input, int idx);
# ifdef _LIBC
static unsigned int find_collation_sequence_value (const unsigned char *mbs,
						   size_t name_len);
# endif /* _LIBC */
#endif /* RE_ENABLE_I18N */
static int group_nodes_into_DFAstates (const regex_t *dfa,
				       const re_dfastate_t *state,
				       re_node_set *states_node,
				       bitset *states_ch);
static int check_node_accept (const regex_t *preg, const re_token_t *node,
			      const re_match_context_t *mctx, int idx);
static reg_errcode_t extend_buffers (re_match_context_t *mctx);

/* Entry point for POSIX code.  */

/* regexec searches for a given pattern, specified by PREG, in the
   string STRING.

   If NMATCH is zero or REG_NOSUB was set in the cflags argument to
   `regcomp', we ignore PMATCH.  Otherwise, we assume PMATCH has at
   least NMATCH elements, and we set them to the offsets of the
   corresponding matched substrings.

   EFLAGS specifies `execution flags' which affect matching: if
   REG_NOTBOL is set, then ^ does not match at the beginning of the
   string; if REG_NOTEOL is set, then $ does not match at the end.

   We return 0 if we find a match and REG_NOMATCH if not.  */

int
regexec (preg, string, nmatch, pmatch, eflags)
    const regex_t *__restrict preg;
    const char *__restrict string;
    size_t nmatch;
    regmatch_t pmatch[];
    int eflags;
{
  reg_errcode_t err;
  int length = (int)strlen(string);
  if (preg->no_sub)
    err = re_search_internal (preg, string, length, 0, length, length, 0,
			      NULL, eflags);
  else
    err = re_search_internal (preg, string, length, 0, length, length, nmatch,
			      pmatch, eflags);
  return err != REG_NOERROR;
}
#ifdef _LIBC
weak_alias (__regexec, regexec)
#endif

/* Entry points for GNU code.  */

/* re_match, re_search, re_match_2, re_search_2

   The former two functions operate on STRING with length LENGTH,
   while the later two operate on concatenation of STRING1 and STRING2
   with lengths LENGTH1 and LENGTH2, respectively.

   re_match() matches the compiled pattern in BUFP against the string,
   starting at index START.

   re_search() first tries matching at index START, then it tries to match
   starting from index START + 1, and so on.  The last start position tried
   is START + RANGE.  (Thus RANGE = 0 forces re_search to operate the same
   way as re_match().)

   The parameter STOP of re_{match,search}_2 specifies that no match exceeding
   the first STOP characters of the concatenation of the strings should be
   concerned.

   If REGS is not NULL, and BUFP->no_sub is not set, the offsets of the match
   and all groups is stroed in REGS.  (For the "_2" variants, the offsets are
   computed relative to the concatenation, not relative to the individual
   strings.)

   On success, re_match* functions return the length of the match, re_search*
   return the position of the start of the match.  Return value -1 means no
   match was found and -2 indicates an internal error.  */

int
re_match (bufp, string, length, start, regs)
    struct re_pattern_buffer *bufp;
    const char *string;
    int length, start;
    struct re_registers *regs;
{
  return re_search_stub (bufp, string, length, start, 0, length, regs, 1);
}
#ifdef _LIBC
weak_alias (__re_match, re_match)
#endif

int
re_search (bufp, string, length, start, range, regs)
    struct re_pattern_buffer *bufp;
    const char *string;
    int length, start, range;
    struct re_registers *regs;
{
  return re_search_stub (bufp, string, length, start, range, length, regs, 0);
}
#ifdef _LIBC
weak_alias (__re_search, re_search)
#endif

int
re_match_2 (bufp, string1, length1, string2, length2, start, regs, stop)
    struct re_pattern_buffer *bufp;
    const char *string1, *string2;
    int length1, length2, start, stop;
    struct re_registers *regs;
{
  return re_search_2_stub (bufp, string1, length1, string2, length2,
			   start, 0, regs, stop, 1);
}
#ifdef _LIBC
weak_alias (__re_match_2, re_match_2)
#endif

int
re_search_2 (bufp, string1, length1, string2, length2, start, range, regs, stop)
    struct re_pattern_buffer *bufp;
    const char *string1, *string2;
    int length1, length2, start, range, stop;
    struct re_registers *regs;
{
  return re_search_2_stub (bufp, string1, length1, string2, length2,
			   start, range, regs, stop, 0);
}
#ifdef _LIBC
weak_alias (__re_search_2, re_search_2)
#endif

static int
re_search_2_stub (bufp, string1, length1, string2, length2, start, range, regs,
		  stop, ret_len)
    struct re_pattern_buffer *bufp;
    const char *string1, *string2;
    int length1, length2, start, range, stop, ret_len;
    struct re_registers *regs;
{
  const char *str;
  int rval;
  int len = length1 + length2;
  int free_str = 0;

  if (BE (length1 < 0 || length2 < 0 || stop < 0, 0))
    return -2;

  /* Concatenate the strings.  */
  if (length2 > 0)
    if (length1 > 0)
      {
	char *s = re_malloc (char, len);

	if (BE (s == NULL, 0))
	  return -2;
	memcpy (s, string1, length1);
	memcpy (s + length1, string2, length2);
	str = s;
	free_str = 1;
      }
    else
      str = string2;
  else
    str = string1;

  rval = re_search_stub (bufp, str, len, start, range, stop, regs,
			 ret_len);
  if (free_str)
    re_free ((char *) str);
  return rval;
}

/* The parameters have the same meaning as those of re_search.
   Additional parameters:
   If RET_LEN is nonzero the length of the match is returned (re_match style);
   otherwise the position of the match is returned.  */

static int
re_search_stub (bufp, string, length, start, range, stop, regs, ret_len)
    struct re_pattern_buffer *bufp;
    const char *string;
    int length, start, range, stop, ret_len;
    struct re_registers *regs;
{
  reg_errcode_t result;
  regmatch_t *pmatch;
  int nregs, rval;
  int eflags = 0;

  /* Check for out-of-range.  */
  if (BE (start < 0 || start > length, 0))
    return -1;
  if (BE (start + range > length, 0))
    range = length - start;
  else if (BE (start + range < 0, 0))
    range = -start;

  eflags |= (bufp->not_bol) ? REG_NOTBOL : 0;
  eflags |= (bufp->not_eol) ? REG_NOTEOL : 0;

  /* Compile fastmap if we haven't yet.  */
  if (range > 0 && bufp->fastmap != NULL && !bufp->fastmap_accurate)
    re_compile_fastmap (bufp);

  if (BE (bufp->no_sub, 0))
    regs = NULL;

  /* We need at least 1 register.  */
  if (regs == NULL)
    nregs = 1;
  else if (BE (bufp->regs_allocated == REGS_FIXED &&
	       regs->num_regs < bufp->re_nsub + 1, 0))
    {
      nregs = regs->num_regs;
      if (BE (nregs < 1, 0))
	{
	  /* Nothing can be copied to regs.  */
	  regs = NULL;
	  nregs = 1;
	}
    }
  else
    nregs = (int)bufp->re_nsub + 1;
  pmatch = re_malloc (regmatch_t, nregs);
  if (BE (pmatch == NULL, 0))
    return -2;

  result = re_search_internal (bufp, string, length, start, range, stop,
			       nregs, pmatch, eflags);

  rval = 0;

  /* I hope we needn't fill ther regs with -1's when no match was found.  */
  if (result != REG_NOERROR)
    rval = -1;
  else if (regs != NULL)
    {
      /* If caller wants register contents data back, copy them.  */
      bufp->regs_allocated = re_copy_regs (regs, pmatch, nregs,
					   bufp->regs_allocated);
      if (BE (bufp->regs_allocated == REGS_UNALLOCATED, 0))
	rval = -2;
    }

  if (BE (rval == 0, 1))
    {
      if (ret_len)
	{
	  assert (pmatch[0].rm_so == start);
	  rval = pmatch[0].rm_eo - start;
	}
      else
	rval = pmatch[0].rm_so;
    }
  re_free (pmatch);
  return rval;
}

static unsigned
re_copy_regs (regs, pmatch, nregs, regs_allocated)
    struct re_registers *regs;
    regmatch_t *pmatch;
    int nregs, regs_allocated;
{
  int rval = REGS_REALLOCATE;
  int i;
  int need_regs = nregs + 1;
  /* We need one extra element beyond `num_regs' for the `-1' marker GNU code
     uses.  */

  /* Have the register data arrays been allocated?  */
  if (regs_allocated == REGS_UNALLOCATED)
    { /* No.  So allocate them with malloc.  */
      regs->start = re_malloc (regoff_t, need_regs);
      if (BE (regs->start == NULL, 0))
	return REGS_UNALLOCATED;
      regs->end = re_malloc (regoff_t, need_regs);
      if (BE (regs->end == NULL, 0))
	{
	  re_free (regs->start);
	  return REGS_UNALLOCATED;
	}
      regs->num_regs = need_regs;
    }
  else if (regs_allocated == REGS_REALLOCATE)
    { /* Yes.  If we need more elements than were already
	 allocated, reallocate them.  If we need fewer, just
	 leave it alone.  */
      if (need_regs > regs->num_regs)
	{
	  regs->start = re_realloc (regs->start, regoff_t, need_regs);
	  if (BE (regs->start == NULL, 0))
	    {
	      if (regs->end != NULL)
		re_free (regs->end);
	      return REGS_UNALLOCATED;
	    }
	  regs->end = re_realloc (regs->end, regoff_t, need_regs);
	  if (BE (regs->end == NULL, 0))
	    {
	      re_free (regs->start);
	      return REGS_UNALLOCATED;
	    }
	  regs->num_regs = need_regs;
	}
    }
  else
    {
      assert (regs_allocated == REGS_FIXED);
      /* This function may not be called with REGS_FIXED and nregs too big.  */
      assert (regs->num_regs >= nregs);
      rval = REGS_FIXED;
    }

  /* Copy the regs.  */
  for (i = 0; i < nregs; ++i)
    {
      regs->start[i] = pmatch[i].rm_so;
      regs->end[i] = pmatch[i].rm_eo;
    }
  for ( ; i < regs->num_regs; ++i)
    regs->start[i] = regs->end[i] = -1;

  return rval;
}

/* Set REGS to hold NUM_REGS registers, storing them in STARTS and
   ENDS.  Subsequent matches using PATTERN_BUFFER and REGS will use
   this memory for recording register information.  STARTS and ENDS
   must be allocated using the malloc library routine, and must each
   be at least NUM_REGS * sizeof (regoff_t) bytes long.

   If NUM_REGS == 0, then subsequent matches should allocate their own
   register data.

   Unless this function is called, the first search or match using
   PATTERN_BUFFER will allocate its own register data, without
   freeing the old data.  */

void
re_set_registers (bufp, regs, num_regs, starts, ends)
    struct re_pattern_buffer *bufp;
    struct re_registers *regs;
    unsigned num_regs;
    regoff_t *starts, *ends;
{
  if (num_regs)
    {
      bufp->regs_allocated = REGS_REALLOCATE;
      regs->num_regs = num_regs;
      regs->start = starts;
      regs->end = ends;
    }
  else
    {
      bufp->regs_allocated = REGS_UNALLOCATED;
      regs->num_regs = 0;
      regs->start = regs->end = (regoff_t *) 0;
    }
}
#ifdef _LIBC
weak_alias (__re_set_registers, re_set_registers)
#endif

/* Entry points compatible with 4.2 BSD regex library.  We don't define
   them unless specifically requested.  */

#if defined _REGEX_RE_COMP || defined _LIBC
int
# ifdef _LIBC
weak_function
# endif
re_exec (s)
     const char *s;
{
  return 0 == regexec (&re_comp_buf, s, 0, NULL, 0);
}
#endif /* _REGEX_RE_COMP */

static re_node_set empty_set;

/* Internal entry point.  */

/* Searches for a compiled pattern PREG in the string STRING, whose
   length is LENGTH.  NMATCH, PMATCH, and EFLAGS have the same
   mingings with regexec.  START, and RANGE have the same meanings
   with re_search.
   Return REG_NOERROR if we find a match, and REG_NOMATCH if not,
   otherwise return the error code.
   Note: We assume front end functions already check ranges.
   (START + RANGE >= 0 && START + RANGE <= LENGTH)  */

static reg_errcode_t
re_search_internal (preg, string, length, start, range, stop, nmatch, pmatch,
		    eflags)
    const regex_t *preg;
    const char *string;
    int length, start, range, stop, eflags;
    size_t nmatch;
    regmatch_t pmatch[];
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  re_string_t input;
  int left_lim, right_lim, incr;
  int fl_longest_match, match_first, match_last = -1;
  int fast_translate, sb;
  re_match_context_t mctx;
  char *fastmap = ((preg->fastmap != NULL && preg->fastmap_accurate
		    && range && !preg->can_be_null) ? preg->fastmap : NULL);

  /* Check if the DFA haven't been compiled.  */
  if (BE (preg->used == 0 || dfa->init_state == NULL
	  || dfa->init_state_word == NULL || dfa->init_state_nl == NULL
	  || dfa->init_state_begbuf == NULL, 0))
    return REG_NOMATCH;

  re_node_set_init_empty (&empty_set);
  memset (&mctx, '\0', sizeof (re_match_context_t));

  /* We must check the longest matching, if nmatch > 0.  */
  fl_longest_match = (nmatch != 0 || dfa->nbackref);

  err = re_string_allocate (&input, string, length, dfa->nodes_len + 1,
			    preg->translate, preg->syntax & RE_ICASE);
  if (BE (err != REG_NOERROR, 0))
    goto free_return;
  input.stop = stop;

  err = match_ctx_init (&mctx, eflags, &input, dfa->nbackref * 2);
  if (BE (err != REG_NOERROR, 0))
    goto free_return;

  /* We will log all the DFA states through which the dfa pass,
     if nmatch > 1, or this dfa has "multibyte node", which is a
     back-reference or a node which can accept multibyte character or
     multi character collating element.  */
  if (nmatch > 1 || dfa->has_mb_node)
    {
      mctx.state_log = re_malloc (re_dfastate_t *, dfa->nodes_len + 1);
      if (BE (mctx.state_log == NULL, 0))
	{
	  err = REG_ESPACE;
	  goto free_return;
	}
    }
  else
    mctx.state_log = NULL;

#ifdef DEBUG
  /* We assume front-end functions already check them.  */
  assert (start + range >= 0 && start + range <= length);
#endif

  match_first = start;
  input.tip_context = ((eflags & REG_NOTBOL) ? CONTEXT_BEGBUF
		       : CONTEXT_NEWLINE | CONTEXT_BEGBUF);

  /* Check incrementally whether of not the input string match.  */
  incr = (range < 0) ? -1 : 1;
  left_lim = (range < 0) ? start + range : start;
  right_lim = (range < 0) ? start : start + range;
  sb = MB_CUR_MAX == 1;
  fast_translate = sb || !(preg->syntax & RE_ICASE || preg->translate);

  for (;;)
    {
      /* At first get the current byte from input string.  */
      if (fastmap)
	{
	  if (BE (fast_translate, 1))
	    {
	      unsigned RE_TRANSLATE_TYPE t
		= (unsigned RE_TRANSLATE_TYPE) preg->translate;
	      if (BE (range >= 0, 1))
		{
		  if (BE (t != NULL, 0))
		    {
		      while (BE (match_first < right_lim, 1)
			     && !fastmap[t[(unsigned char) string[match_first]]])
			++match_first;
		    }
		  else
		    {
		      while (BE (match_first < right_lim, 1)
			     && !fastmap[(unsigned char) string[match_first]])
			++match_first;
		    }
		  if (BE (match_first == right_lim, 0))
		    {
		      int ch = match_first >= length
			       ? 0 : (unsigned char) string[match_first];
		      if (!fastmap[t ? t[ch] : ch])
			break;
		    }
		}
	      else
		{
		  while (match_first >= left_lim)
		    {
		      int ch = match_first >= length
			       ? 0 : (unsigned char) string[match_first];
		      if (fastmap[t ? t[ch] : ch])
			break;
		      --match_first;
		    }
		  if (match_first < left_lim)
		    break;
		}
	    }
	  else
	    {
	      int ch;

	      do
		{
		  /* In this case, we can't determine easily the current byte,
		     since it might be a component byte of a multibyte
		     character.  Then we use the constructed buffer
		     instead.  */
		  /* If MATCH_FIRST is out of the valid range, reconstruct the
		     buffers.  */
		  if (input.raw_mbs_idx + input.valid_len <= match_first
		      || match_first < input.raw_mbs_idx)
		    {
		      err = re_string_reconstruct (&input, match_first, eflags,
						   preg->newline_anchor);
		      if (BE (err != REG_NOERROR, 0))
			goto free_return;
		    }
		  /* If MATCH_FIRST is out of the buffer, leave it as '\0'.
		     Note that MATCH_FIRST must not be smaller than 0.  */
		  ch = ((match_first >= length) ? 0
		       : re_string_byte_at (&input,
					    match_first - input.raw_mbs_idx));
		  if (fastmap[ch])
		    break;
		  match_first += incr;
		}
	      while (match_first >= left_lim && match_first <= right_lim);
	      if (! fastmap[ch])
		break;
	    }
	}

      /* Reconstruct the buffers so that the matcher can assume that
	 the matching starts from the begining of the buffer.  */
      err = re_string_reconstruct (&input, match_first, eflags,
				   preg->newline_anchor);
      if (BE (err != REG_NOERROR, 0))
	goto free_return;
#ifdef RE_ENABLE_I18N
     /* Eliminate it when it is a component of a multibyte character
	 and isn't the head of a multibyte character.  */
      if (sb || re_string_first_byte (&input, 0))
#endif
	{
	  /* It seems to be appropriate one, then use the matcher.  */
	  /* We assume that the matching starts from 0.  */
	  mctx.state_log_top = mctx.nbkref_ents = mctx.max_mb_elem_len = 0;
	  match_last = check_matching (preg, &mctx, 0, fl_longest_match);
	  if (match_last != -1)
	    {
	      if (BE (match_last == -2, 0))
		{
		  err = REG_ESPACE;
		  goto free_return;
		}
	      else
		{
		  mctx.match_last = match_last;
		  if ((!preg->no_sub && nmatch > 1) || dfa->nbackref)
		    {
		      re_dfastate_t *pstate = mctx.state_log[match_last];
		      mctx.last_node = check_halt_state_context (preg, pstate,
								 &mctx, match_last);
		    }
		  if ((!preg->no_sub && nmatch > 1 && dfa->has_plural_match)
		      || dfa->nbackref)
		    {
		      err = prune_impossible_nodes (preg, &mctx);
		      if (err == REG_NOERROR)
			break;
		      if (BE (err != REG_NOMATCH, 0))
			goto free_return;
		    }
		  else
		    break; /* We found a matching.  */
		}
	    }
	  match_ctx_clean (&mctx);
	}
      /* Update counter.  */
      match_first += incr;
      if (match_first < left_lim || right_lim < match_first)
	break;
    }

  /* Set pmatch[] if we need.  */
  if (match_last != -1 && nmatch > 0)
    {
      int reg_idx;

      /* Initialize registers.  */
      for (reg_idx = 0; reg_idx < nmatch; ++reg_idx)
	pmatch[reg_idx].rm_so = pmatch[reg_idx].rm_eo = -1;

      /* Set the points where matching start/end.  */
      pmatch[0].rm_so = 0;
      pmatch[0].rm_eo = mctx.match_last;

      if (!preg->no_sub && nmatch > 1)
	{
	  err = set_regs (preg, &mctx, nmatch, pmatch,
			  dfa->has_plural_match && dfa->nbackref > 0);
	  if (BE (err != REG_NOERROR, 0))
	    goto free_return;
	}

      /* At last, add the offset to the each registers, since we slided
	 the buffers so that We can assume that the matching starts from 0.  */
      for (reg_idx = 0; reg_idx < nmatch; ++reg_idx)
	if (pmatch[reg_idx].rm_so != -1)
	  {
	    pmatch[reg_idx].rm_so += match_first;
	    pmatch[reg_idx].rm_eo += match_first;
	  }
    }
  err = (match_last == -1) ? REG_NOMATCH : REG_NOERROR;
 free_return:
  re_free (mctx.state_log);
  if (dfa->nbackref)
    match_ctx_free (&mctx);
  re_string_destruct (&input);
  return err;
}

static reg_errcode_t
prune_impossible_nodes (preg, mctx)
     const regex_t *preg;
     re_match_context_t *mctx;
{
  int halt_node, match_last;
  reg_errcode_t ret;
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  re_dfastate_t **sifted_states;
  re_dfastate_t **lim_states = NULL;
  re_sift_context_t sctx;
#ifdef DEBUG
  assert (mctx->state_log != NULL);
#endif
  match_last = mctx->match_last;
  halt_node = mctx->last_node;
  sifted_states = re_malloc (re_dfastate_t *, match_last + 1);
  if (BE (sifted_states == NULL, 0))
    {
      ret = REG_ESPACE;
      goto free_return;
    }
  if (dfa->nbackref)
    {
      lim_states = re_malloc (re_dfastate_t *, match_last + 1);
      if (BE (lim_states == NULL, 0))
	{
	  ret = REG_ESPACE;
	  goto free_return;
	}
      while (1)
	{
	  memset (lim_states, '\0',
		  sizeof (re_dfastate_t *) * (match_last + 1));
	  match_ctx_clear_flag (mctx);
	  sift_ctx_init (&sctx, sifted_states, lim_states, halt_node,
			 match_last, 0);
	  ret = sift_states_backward (preg, mctx, &sctx);
	  re_node_set_free (&sctx.limits);
	  if (BE (ret != REG_NOERROR, 0))
	      goto free_return;
	  if (sifted_states[0] != NULL || lim_states[0] != NULL)
	    break;
	  do
	    {
	      --match_last;
	      if (match_last < 0)
		{
		  ret = REG_NOMATCH;
		  goto free_return;
		}
	    } while (!mctx->state_log[match_last]->halt);
	  halt_node = check_halt_state_context (preg,
						mctx->state_log[match_last],
						mctx, match_last);
	}
      ret = merge_state_array (dfa, sifted_states, lim_states,
			       match_last + 1);
      re_free (lim_states);
      lim_states = NULL;
      if (BE (ret != REG_NOERROR, 0))
	goto free_return;
    }
  else
    {
      sift_ctx_init (&sctx, sifted_states, lim_states, halt_node,
		     match_last, 0);
      ret = sift_states_backward (preg, mctx, &sctx);
      re_node_set_free (&sctx.limits);
      if (BE (ret != REG_NOERROR, 0))
	goto free_return;
    }
  re_free (mctx->state_log);
  mctx->state_log = sifted_states;
  sifted_states = NULL;
  mctx->last_node = halt_node;
  mctx->match_last = match_last;
  ret = REG_NOERROR;
 free_return:
  re_free (sifted_states);
  re_free (lim_states);
  return ret;
}

/* Acquire an initial state and return it.
   We must select appropriate initial state depending on the context,
   since initial states may have constraints like "\<", "^", etc..  */

static inline re_dfastate_t *
acquire_init_state_context (err, preg, mctx, idx)
     reg_errcode_t *err;
     const regex_t *preg;
     const re_match_context_t *mctx;
     int idx;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;

  *err = REG_NOERROR;
  if (dfa->init_state->has_constraint)
    {
      unsigned int context;
      context =  re_string_context_at (mctx->input, idx - 1, mctx->eflags,
				       preg->newline_anchor);
      if (IS_WORD_CONTEXT (context))
	return dfa->init_state_word;
      else if (IS_ORDINARY_CONTEXT (context))
	return dfa->init_state;
      else if (IS_BEGBUF_CONTEXT (context) && IS_NEWLINE_CONTEXT (context))
	return dfa->init_state_begbuf;
      else if (IS_NEWLINE_CONTEXT (context))
	return dfa->init_state_nl;
      else if (IS_BEGBUF_CONTEXT (context))
	{
	  /* It is relatively rare case, then calculate on demand.  */
	  return  re_acquire_state_context (err, dfa,
					    dfa->init_state->entrance_nodes,
					    context);
	}
      else
	/* Must not happen?  */
	return dfa->init_state;
    }
  else
    return dfa->init_state;
}

/* Check whether the regular expression match input string INPUT or not,
   and return the index where the matching end, return -1 if not match,
   or return -2 in case of an error.
   FL_SEARCH means we must search where the matching starts,
   FL_LONGEST_MATCH means we want the POSIX longest matching.
   Note that the matcher assume that the maching starts from the current
   index of the buffer.  */

static int
check_matching (preg, mctx, fl_search, fl_longest_match)
    const regex_t *preg;
    re_match_context_t *mctx;
    int fl_search, fl_longest_match;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  reg_errcode_t err;
  int match = 0;
  int match_last = -1;
  int cur_str_idx = re_string_cur_idx (mctx->input);
  re_dfastate_t *cur_state;

  cur_state = acquire_init_state_context (&err, preg, mctx, cur_str_idx);
  /* An initial state must not be NULL(invalid state).  */
  if (BE (cur_state == NULL, 0))
    return -2;
  if (mctx->state_log != NULL)
    mctx->state_log[cur_str_idx] = cur_state;

  /* Check OP_OPEN_SUBEXP in the initial state in case that we use them
     later.  E.g. Processing back references.  */
  if (dfa->nbackref)
    {
      err = check_subexp_matching_top (dfa, mctx, &cur_state->nodes, 0);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }

  if (cur_state->has_backref)
    {
      err = transit_state_bkref (preg, &cur_state->nodes, mctx);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }

  /* If the RE accepts NULL string.  */
  if (cur_state->halt)
    {
      if (!cur_state->has_constraint
	  || check_halt_state_context (preg, cur_state, mctx, cur_str_idx))
	{
	  if (!fl_longest_match)
	    return cur_str_idx;
	  else
	    {
	      match_last = cur_str_idx;
	      match = 1;
	    }
	}
    }

  while (!re_string_eoi (mctx->input))
    {
      cur_state = transit_state (&err, preg, mctx, cur_state,
				 fl_search && !match);
      if (cur_state == NULL) /* Reached at the invalid state or an error.  */
	{
	  cur_str_idx = re_string_cur_idx (mctx->input);
	  if (BE (err != REG_NOERROR, 0))
	    return -2;
	  if (fl_search && !match)
	    {
	      /* Restart from initial state, since we are searching
		 the point from where matching start.  */
#ifdef RE_ENABLE_I18N
	      if (MB_CUR_MAX == 1
		  || re_string_first_byte (mctx->input, cur_str_idx))
#endif /* RE_ENABLE_I18N */
		cur_state = acquire_init_state_context (&err, preg, mctx,
							cur_str_idx);
	      if (BE (cur_state == NULL && err != REG_NOERROR, 0))
		return -2;
	      if (mctx->state_log != NULL)
		mctx->state_log[cur_str_idx] = cur_state;
	    }
	  else if (!fl_longest_match && match)
	    break;
	  else /* (fl_longest_match && match) || (!fl_search && !match)  */
	    {
	      if (mctx->state_log == NULL)
		break;
	      else
		{
		  int max = mctx->state_log_top;
		  for (; cur_str_idx <= max; ++cur_str_idx)
		    if (mctx->state_log[cur_str_idx] != NULL)
		      break;
		  if (cur_str_idx > max)
		    break;
		}
	    }
	}

      if (cur_state != NULL && cur_state->halt)
	{
	  /* Reached at a halt state.
	     Check the halt state can satisfy the current context.  */
	  if (!cur_state->has_constraint
	      || check_halt_state_context (preg, cur_state, mctx,
					   re_string_cur_idx (mctx->input)))
	    {
	      /* We found an appropriate halt state.  */
	      match_last = re_string_cur_idx (mctx->input);
	      match = 1;
	      if (!fl_longest_match)
		break;
	    }
	}
   }
  return match_last;
}

/* Check NODE match the current context.  */

static int check_halt_node_context (dfa, node, context)
    const re_dfa_t *dfa;
    int node;
    unsigned int context;
{
  re_token_type_t type = dfa->nodes[node].type;
  unsigned int constraint = dfa->nodes[node].constraint;
  if (type != END_OF_RE)
    return 0;
  if (!constraint)
    return 1;
  if (NOT_SATISFY_NEXT_CONSTRAINT (constraint, context))
    return 0;
  return 1;
}

/* Check the halt state STATE match the current context.
   Return 0 if not match, if the node, STATE has, is a halt node and
   match the context, return the node.  */

static int
check_halt_state_context (preg, state, mctx, idx)
    const regex_t *preg;
    const re_dfastate_t *state;
    const re_match_context_t *mctx;
    int idx;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int i;
  unsigned int context;
#ifdef DEBUG
  assert (state->halt);
#endif
  context = re_string_context_at (mctx->input, idx, mctx->eflags,
				  preg->newline_anchor);
  for (i = 0; i < state->nodes.nelem; ++i)
    if (check_halt_node_context (dfa, state->nodes.elems[i], context))
      return state->nodes.elems[i];
  return 0;
}

/* Compute the next node to which "NFA" transit from NODE("NFA" is a NFA
   corresponding to the DFA).
   Return the destination node, and update EPS_VIA_NODES, return -1 in case
   of errors.  */

static int
proceed_next_node (preg, nregs, regs, mctx, pidx, node, eps_via_nodes, fs)
    const regex_t *preg;
    regmatch_t *regs;
    const re_match_context_t *mctx;
    int nregs, *pidx, node;
    re_node_set *eps_via_nodes;
    struct re_fail_stack_t *fs;
{
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  int i, err, dest_node;
  dest_node = -1;
  if (IS_EPSILON_NODE (dfa->nodes[node].type))
    {
      re_node_set *cur_nodes = &mctx->state_log[*pidx]->nodes;
      int ndest, dest_nodes[2];
      err = re_node_set_insert (eps_via_nodes, node);
      if (BE (err < 0, 0))
	return -1;
      /* Pick up valid destinations.  */
      for (ndest = 0, i = 0; i < dfa->edests[node].nelem; ++i)
	{
	  int candidate = dfa->edests[node].elems[i];
	  if (!re_node_set_contains (cur_nodes, candidate))
	    continue;
	  dest_nodes[0] = (ndest == 0) ? candidate : dest_nodes[0];
	  dest_nodes[1] = (ndest == 1) ? candidate : dest_nodes[1];
	  ++ndest;
	}
      if (ndest <= 1)
	return ndest == 0 ? -1 : (ndest == 1 ? dest_nodes[0] : 0);
      /* In order to avoid infinite loop like "(a*)*".  */
      if (re_node_set_contains (eps_via_nodes, dest_nodes[0]))
	return dest_nodes[1];
      if (fs != NULL)
	push_fail_stack (fs, *pidx, dest_nodes, nregs, regs, eps_via_nodes);
      return dest_nodes[0];
    }
  else
    {
      int naccepted = 0;
      re_token_type_t type = dfa->nodes[node].type;

#ifdef RE_ENABLE_I18N
      if (ACCEPT_MB_NODE (type))
	naccepted = check_node_accept_bytes (preg, node, mctx->input, *pidx);
      else
#endif /* RE_ENABLE_I18N */
      if (type == OP_BACK_REF)
	{
	  int subexp_idx = dfa->nodes[node].opr.idx;
	  naccepted = regs[subexp_idx].rm_eo - regs[subexp_idx].rm_so;
	  if (fs != NULL)
	    {
	      if (regs[subexp_idx].rm_so == -1 || regs[subexp_idx].rm_eo == -1)
		return -1;
	      else if (naccepted)
		{
		  char *buf = (char *) re_string_get_buffer (mctx->input);
		  if (memcmp (buf + regs[subexp_idx].rm_so, buf + *pidx,
			      naccepted) != 0)
		    return -1;
		}
	    }

	  if (naccepted == 0)
	    {
	      err = re_node_set_insert (eps_via_nodes, node);
	      if (BE (err < 0, 0))
		return -2;
	      dest_node = dfa->edests[node].elems[0];
	      if (re_node_set_contains (&mctx->state_log[*pidx]->nodes,
					dest_node))
		return dest_node;
	    }
	}

      if (naccepted != 0
	  || check_node_accept (preg, dfa->nodes + node, mctx, *pidx))
	{
	  dest_node = dfa->nexts[node];
	  *pidx = (naccepted == 0) ? *pidx + 1 : *pidx + naccepted;
	  if (fs && (*pidx > mctx->match_last || mctx->state_log[*pidx] == NULL
		     || !re_node_set_contains (&mctx->state_log[*pidx]->nodes,
					       dest_node)))
	    return -1;
	  re_node_set_empty (eps_via_nodes);
	  return dest_node;
	}
    }
  return -1;
}

static reg_errcode_t
push_fail_stack (fs, str_idx, dests, nregs, regs, eps_via_nodes)
     struct re_fail_stack_t *fs;
     int str_idx, *dests, nregs;
     regmatch_t *regs;
     re_node_set *eps_via_nodes;
{
  reg_errcode_t err;
  int num = fs->num++;
  if (fs->num == fs->alloc)
    {
      struct re_fail_stack_ent_t *new_array;
      fs->alloc *= 2;
      new_array = realloc (fs->stack, (sizeof (struct re_fail_stack_ent_t)
				       * fs->alloc));
      if (new_array == NULL)
	return REG_ESPACE;
      fs->stack = new_array;
    }
  fs->stack[num].idx = str_idx;
  fs->stack[num].node = dests[1];
  fs->stack[num].regs = re_malloc (regmatch_t, nregs);
  memcpy (fs->stack[num].regs, regs, sizeof (regmatch_t) * nregs);
  err = re_node_set_init_copy (&fs->stack[num].eps_via_nodes, eps_via_nodes);
  return err;
}

static int
pop_fail_stack (fs, pidx, nregs, regs, eps_via_nodes)
     struct re_fail_stack_t *fs;
     int *pidx, nregs;
     regmatch_t *regs;
     re_node_set *eps_via_nodes;
{
  int num = --fs->num;
  assert (num >= 0);
 *pidx = fs->stack[num].idx;
  memcpy (regs, fs->stack[num].regs, sizeof (regmatch_t) * nregs);
  re_node_set_free (eps_via_nodes);
  re_free (fs->stack[num].regs);
  *eps_via_nodes = fs->stack[num].eps_via_nodes;
  return fs->stack[num].node;
}

/* Set the positions where the subexpressions are starts/ends to registers
   PMATCH.
   Note: We assume that pmatch[0] is already set, and
   pmatch[i].rm_so == pmatch[i].rm_eo == -1 (i > 1).  */

static reg_errcode_t
set_regs (preg, mctx, nmatch, pmatch, fl_backtrack)
     const regex_t *preg;
     const re_match_context_t *mctx;
     size_t nmatch;
     regmatch_t *pmatch;
     int fl_backtrack;
{
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  int idx, cur_node, real_nmatch;
  re_node_set eps_via_nodes;
  struct re_fail_stack_t *fs;
  struct re_fail_stack_t fs_body = {0, 2, NULL};
#ifdef DEBUG
  assert (nmatch > 1);
  assert (mctx->state_log != NULL);
#endif
  if (fl_backtrack)
    {
      fs = &fs_body;
      fs->stack = re_malloc (struct re_fail_stack_ent_t, fs->alloc);
    }
  else
    fs = NULL;
  cur_node = dfa->init_node;
  real_nmatch = (int)((nmatch <= preg->re_nsub) ? nmatch : preg->re_nsub + 1);
  re_node_set_init_empty (&eps_via_nodes);
  for (idx = pmatch[0].rm_so; idx <= pmatch[0].rm_eo ;)
    {
      update_regs (dfa, pmatch, cur_node, idx, real_nmatch);
      if (idx == pmatch[0].rm_eo && cur_node == mctx->last_node)
	{
	  int reg_idx;
	  if (fs)
	    {
	      for (reg_idx = 0; reg_idx < nmatch; ++reg_idx)
		if (pmatch[reg_idx].rm_so > -1 && pmatch[reg_idx].rm_eo == -1)
		  break;
	      if (reg_idx == nmatch)
		{
		  re_node_set_free (&eps_via_nodes);
		  return free_fail_stack_return (fs);
		}
        cur_node = pop_fail_stack(fs, &idx, (int)nmatch, pmatch,
					 &eps_via_nodes);
	    }
	  else
	    {
	      re_node_set_free (&eps_via_nodes);
	      return REG_NOERROR;
	    }
	}

      /* Proceed to next node.  */
      cur_node = proceed_next_node(preg, (int)nmatch, pmatch, mctx, &idx, cur_node,
				    &eps_via_nodes, fs);

      if (BE (cur_node < 0, 0))
	{
	  if (cur_node == -2)
	    return REG_ESPACE;
	  if (fs)
      cur_node = pop_fail_stack(fs, &idx, (int)nmatch, pmatch,
				       &eps_via_nodes);
	  else
	    {
	      re_node_set_free (&eps_via_nodes);
	      return REG_NOMATCH;
	    }
	}
    }
  re_node_set_free (&eps_via_nodes);
  return free_fail_stack_return (fs);
}

static reg_errcode_t
free_fail_stack_return (fs)
     struct re_fail_stack_t *fs;
{
  if (fs)
    {
      int fs_idx;
      for (fs_idx = 0; fs_idx < fs->num; ++fs_idx)
	{
	  re_node_set_free (&fs->stack[fs_idx].eps_via_nodes);
	  re_free (fs->stack[fs_idx].regs);
	}
      re_free (fs->stack);
    }
  return REG_NOERROR;
}

static void
update_regs (dfa, pmatch, cur_node, cur_idx, nmatch)
     re_dfa_t *dfa;
     regmatch_t *pmatch;
     int cur_node, cur_idx, nmatch;
{
  int type = dfa->nodes[cur_node].type;
  int reg_num;
  if (type != OP_OPEN_SUBEXP && type != OP_CLOSE_SUBEXP)
    return;
  reg_num = dfa->nodes[cur_node].opr.idx + 1;
  if (reg_num >= nmatch)
    return;
  if (type == OP_OPEN_SUBEXP)
    {
      /* We are at the first node of this sub expression.  */
      pmatch[reg_num].rm_so = cur_idx;
      pmatch[reg_num].rm_eo = -1;
    }
  else if (type == OP_CLOSE_SUBEXP)
    /* We are at the first node of this sub expression.  */
    pmatch[reg_num].rm_eo = cur_idx;
}

#define NUMBER_OF_STATE 1

/* This function checks the STATE_LOG from the SCTX->last_str_idx to 0
   and sift the nodes in each states according to the following rules.
   Updated state_log will be wrote to STATE_LOG.

   Rules: We throw away the Node `a' in the STATE_LOG[STR_IDX] if...
     1. When STR_IDX == MATCH_LAST(the last index in the state_log):
	If `a' isn't the LAST_NODE and `a' can't epsilon transit to
	the LAST_NODE, we throw away the node `a'.
     2. When 0 <= STR_IDX < MATCH_LAST and `a' accepts
	string `s' and transit to `b':
	i. If 'b' isn't in the STATE_LOG[STR_IDX+strlen('s')], we throw
	   away the node `a'.
	ii. If 'b' is in the STATE_LOG[STR_IDX+strlen('s')] but 'b' is
	    throwed away, we throw away the node `a'.
     3. When 0 <= STR_IDX < n and 'a' epsilon transit to 'b':
	i. If 'b' isn't in the STATE_LOG[STR_IDX], we throw away the
	   node `a'.
	ii. If 'b' is in the STATE_LOG[STR_IDX] but 'b' is throwed away,
	    we throw away the node `a'.  */

#define STATE_NODE_CONTAINS(state,node) \
  ((state) != NULL && re_node_set_contains (&(state)->nodes, node))

static reg_errcode_t
sift_states_backward (preg, mctx, sctx)
     const regex_t *preg;
     re_match_context_t *mctx;
     re_sift_context_t *sctx;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  int null_cnt = 0;
  int str_idx = sctx->last_str_idx;
  re_node_set cur_dest;
  re_node_set *cur_src; /* Points the state_log[str_idx]->nodes  */

#ifdef DEBUG
  assert (mctx->state_log != NULL && mctx->state_log[str_idx] != NULL);
#endif
  cur_src = &mctx->state_log[str_idx]->nodes;

  /* Build sifted state_log[str_idx].  It has the nodes which can epsilon
     transit to the last_node and the last_node itself.  */
  err = re_node_set_init_1 (&cur_dest, sctx->last_node);
  if (BE (err != REG_NOERROR, 0))
    return err;
  err = update_cur_sifted_state (preg, mctx, sctx, str_idx, &cur_dest);
  if (BE (err != REG_NOERROR, 0))
    goto free_return;

  /* Then check each states in the state_log.  */
  while (str_idx > 0)
    {
      int i, ret;
      /* Update counters.  */
      null_cnt = (sctx->sifted_states[str_idx] == NULL) ? null_cnt + 1 : 0;
      if (null_cnt > mctx->max_mb_elem_len)
	{
	  memset (sctx->sifted_states, '\0',
		  sizeof (re_dfastate_t *) * str_idx);
	  re_node_set_free (&cur_dest);
	  return REG_NOERROR;
	}
      re_node_set_empty (&cur_dest);
      --str_idx;
      cur_src = ((mctx->state_log[str_idx] == NULL) ? &empty_set
		 : &mctx->state_log[str_idx]->nodes);

      /* Then build the next sifted state.
	 We build the next sifted state on `cur_dest', and update
	 `sifted_states[str_idx]' with `cur_dest'.
	 Note:
	 `cur_dest' is the sifted state from `state_log[str_idx + 1]'.
	 `cur_src' points the node_set of the old `state_log[str_idx]'.  */
      for (i = 0; i < cur_src->nelem; i++)
	{
	  int prev_node = cur_src->elems[i];
	  int naccepted = 0;
	  re_token_type_t type = dfa->nodes[prev_node].type;

	  if (IS_EPSILON_NODE(type))
	    continue;
#ifdef RE_ENABLE_I18N
	  /* If the node may accept `multi byte'.  */
	  if (ACCEPT_MB_NODE (type))
	    naccepted = sift_states_iter_mb (preg, mctx, sctx, prev_node,
					     str_idx, sctx->last_str_idx);

#endif /* RE_ENABLE_I18N */
	  /* We don't check backreferences here.
	     See update_cur_sifted_state().  */

	  if (!naccepted
	      && check_node_accept (preg, dfa->nodes + prev_node, mctx,
				    str_idx)
	      && STATE_NODE_CONTAINS (sctx->sifted_states[str_idx + 1],
				      dfa->nexts[prev_node]))
	    naccepted = 1;

	  if (naccepted == 0)
	    continue;

	  if (sctx->limits.nelem)
	    {
	      int to_idx = str_idx + naccepted;
	      if (check_dst_limits (dfa, &sctx->limits, mctx,
				    dfa->nexts[prev_node], to_idx,
				    prev_node, str_idx))
		continue;
	    }
	  ret = re_node_set_insert (&cur_dest, prev_node);
	  if (BE (ret == -1, 0))
	    {
	      err = REG_ESPACE;
	      goto free_return;
	    }
	}

      /* Add all the nodes which satisfy the following conditions:
	 - It can epsilon transit to a node in CUR_DEST.
	 - It is in CUR_SRC.
	 And update state_log.  */
      err = update_cur_sifted_state (preg, mctx, sctx, str_idx, &cur_dest);
      if (BE (err != REG_NOERROR, 0))
	goto free_return;
    }
  err = REG_NOERROR;
 free_return:
  re_node_set_free (&cur_dest);
  return err;
}

/* Helper functions.  */

static inline reg_errcode_t
clean_state_log_if_need (mctx, next_state_log_idx)
    re_match_context_t *mctx;
    int next_state_log_idx;
{
  int top = mctx->state_log_top;

  if (next_state_log_idx >= mctx->input->bufs_len
      || (next_state_log_idx >= mctx->input->valid_len
	  && mctx->input->valid_len < mctx->input->len))
    {
      reg_errcode_t err;
      err = extend_buffers (mctx);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }

  if (top < next_state_log_idx)
    {
      memset (mctx->state_log + top + 1, '\0',
	      sizeof (re_dfastate_t *) * (next_state_log_idx - top));
      mctx->state_log_top = next_state_log_idx;
    }
  return REG_NOERROR;
}

static reg_errcode_t
merge_state_array (dfa, dst, src, num)
     re_dfa_t *dfa;
     re_dfastate_t **dst;
     re_dfastate_t **src;
     int num;
{
  int st_idx;
  reg_errcode_t err;
  for (st_idx = 0; st_idx < num; ++st_idx)
    {
      if (dst[st_idx] == NULL)
	dst[st_idx] = src[st_idx];
      else if (src[st_idx] != NULL)
	{
	  re_node_set merged_set;
	  err = re_node_set_init_union (&merged_set, &dst[st_idx]->nodes,
					&src[st_idx]->nodes);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	  dst[st_idx] = re_acquire_state (&err, dfa, &merged_set);
	  re_node_set_free (&merged_set);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	}
    }
  return REG_NOERROR;
}

static reg_errcode_t
update_cur_sifted_state (preg, mctx, sctx, str_idx, dest_nodes)
     const regex_t *preg;
     re_match_context_t *mctx;
     re_sift_context_t *sctx;
     int str_idx;
     re_node_set *dest_nodes;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  const re_node_set *candidates;
  candidates = ((mctx->state_log[str_idx] == NULL) ? &empty_set
		: &mctx->state_log[str_idx]->nodes);

  /* At first, add the nodes which can epsilon transit to a node in
     DEST_NODE.  */
  if (dest_nodes->nelem)
    {
      err = add_epsilon_src_nodes (dfa, dest_nodes, candidates);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }

  /* Then, check the limitations in the current sift_context.  */
  if (dest_nodes->nelem && sctx->limits.nelem)
    {
      err = check_subexp_limits (dfa, dest_nodes, candidates, &sctx->limits,
				 mctx->bkref_ents, str_idx);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }

  /* Update state_log.  */
  sctx->sifted_states[str_idx] = re_acquire_state (&err, dfa, dest_nodes);
  if (BE (sctx->sifted_states[str_idx] == NULL && err != REG_NOERROR, 0))
    return err;

  if ((mctx->state_log[str_idx] != NULL
       && mctx->state_log[str_idx]->has_backref))
    {
      err = sift_states_bkref (preg, mctx, sctx, str_idx, dest_nodes);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }
  return REG_NOERROR;
}

static reg_errcode_t
add_epsilon_src_nodes (dfa, dest_nodes, candidates)
     re_dfa_t *dfa;
     re_node_set *dest_nodes;
     const re_node_set *candidates;
{
  reg_errcode_t err;
  int src_idx;
  re_node_set src_copy;

  err = re_node_set_init_copy (&src_copy, dest_nodes);
  if (BE (err != REG_NOERROR, 0))
    return err;
  for (src_idx = 0; src_idx < src_copy.nelem; ++src_idx)
    {
      err = re_node_set_add_intersect (dest_nodes, candidates,
				       dfa->inveclosures
				       + src_copy.elems[src_idx]);
      if (BE (err != REG_NOERROR, 0))
	{
	  re_node_set_free (&src_copy);
	  return err;
	}
    }
  re_node_set_free (&src_copy);
  return REG_NOERROR;
}

static reg_errcode_t
sub_epsilon_src_nodes (dfa, node, dest_nodes, candidates)
     re_dfa_t *dfa;
     int node;
     re_node_set *dest_nodes;
     const re_node_set *candidates;
{
    int ecl_idx;
    reg_errcode_t err;
    re_node_set *inv_eclosure = dfa->inveclosures + node;
    re_node_set except_nodes;
    re_node_set_init_empty (&except_nodes);
    for (ecl_idx = 0; ecl_idx < inv_eclosure->nelem; ++ecl_idx)
      {
	int cur_node = inv_eclosure->elems[ecl_idx];
	if (cur_node == node)
	  continue;
	if (IS_EPSILON_NODE (dfa->nodes[cur_node].type))
	  {
	    int edst1 = dfa->edests[cur_node].elems[0];
	    int edst2 = ((dfa->edests[cur_node].nelem > 1)
			 ? dfa->edests[cur_node].elems[1] : -1);
	    if ((!re_node_set_contains (inv_eclosure, edst1)
		 && re_node_set_contains (dest_nodes, edst1))
		|| (edst2 > 0
		    && !re_node_set_contains (inv_eclosure, edst2)
		    && re_node_set_contains (dest_nodes, edst2)))
	      {
		err = re_node_set_add_intersect (&except_nodes, candidates,
						 dfa->inveclosures + cur_node);
		if (BE (err != REG_NOERROR, 0))
		  {
		    re_node_set_free (&except_nodes);
		    return err;
		  }
	      }
	  }
      }
    for (ecl_idx = 0; ecl_idx < inv_eclosure->nelem; ++ecl_idx)
      {
	int cur_node = inv_eclosure->elems[ecl_idx];
	if (!re_node_set_contains (&except_nodes, cur_node))
	  {
	    int idx = re_node_set_contains (dest_nodes, cur_node) - 1;
	    re_node_set_remove_at (dest_nodes, idx);
	  }
      }
    re_node_set_free (&except_nodes);
    return REG_NOERROR;
}

static int
check_dst_limits (dfa, limits, mctx, dst_node, dst_idx, src_node, src_idx)
     re_dfa_t *dfa;
     re_node_set *limits;
     re_match_context_t *mctx;
     int dst_node, dst_idx, src_node, src_idx;
{
  int lim_idx, src_pos, dst_pos;

  for (lim_idx = 0; lim_idx < limits->nelem; ++lim_idx)
    {
      int subexp_idx;
      struct re_backref_cache_entry *ent;
      ent = mctx->bkref_ents + limits->elems[lim_idx];
      subexp_idx = dfa->nodes[ent->node].opr.idx - 1;

      dst_pos = check_dst_limits_calc_pos (dfa, mctx, limits->elems[lim_idx],
					   dfa->eclosures + dst_node,
					   subexp_idx, dst_node, dst_idx);
      src_pos = check_dst_limits_calc_pos (dfa, mctx, limits->elems[lim_idx],
					   dfa->eclosures + src_node,
					   subexp_idx, src_node, src_idx);

      /* In case of:
	 <src> <dst> ( <subexp> )
	 ( <subexp> ) <src> <dst>
	 ( <subexp1> <src> <subexp2> <dst> <subexp3> )  */
      if (src_pos == dst_pos)
	continue; /* This is unrelated limitation.  */
      else
	return 1;
    }
  return 0;
}

static int
check_dst_limits_calc_pos (dfa, mctx, limit, eclosures, subexp_idx, node,
			   str_idx)
     re_dfa_t *dfa;
     re_match_context_t *mctx;
     re_node_set *eclosures;
     int limit, subexp_idx, node, str_idx;
{
  struct re_backref_cache_entry *lim = mctx->bkref_ents + limit;
  int pos = (str_idx < lim->subexp_from ? -1
	     : (lim->subexp_to < str_idx ? 1 : 0));
  if (pos == 0
      && (str_idx == lim->subexp_from || str_idx == lim->subexp_to))
    {
      int node_idx;
      for (node_idx = 0; node_idx < eclosures->nelem; ++node_idx)
	{
	  int node = eclosures->elems[node_idx];
	  re_token_type_t type= dfa->nodes[node].type;
	  if (type == OP_BACK_REF)
	    {
	      int bi = search_cur_bkref_entry (mctx, str_idx);
	      for (; bi < mctx->nbkref_ents; ++bi)
		{
		  struct re_backref_cache_entry *ent = mctx->bkref_ents + bi;
		  if (ent->str_idx > str_idx)
		    break;
		  if (ent->node == node && ent->subexp_from == ent->subexp_to)
		    {
		      int cpos, dst;
		      dst = dfa->edests[node].elems[0];
		      cpos = check_dst_limits_calc_pos (dfa, mctx, limit,
							dfa->eclosures + dst,
							subexp_idx, dst,
							str_idx);
		      if ((str_idx == lim->subexp_from && cpos == -1)
			  || (str_idx == lim->subexp_to && cpos == 0))
			return cpos;
		    }
		}
	    }
	  if (type == OP_OPEN_SUBEXP && subexp_idx == dfa->nodes[node].opr.idx
	      && str_idx == lim->subexp_from)
	    {
	      pos = -1;
	      break;
	    }
	  if (type == OP_CLOSE_SUBEXP && subexp_idx == dfa->nodes[node].opr.idx
	      && str_idx == lim->subexp_to)
	    break;
	}
      if (node_idx == eclosures->nelem && str_idx == lim->subexp_to)
	pos = 1;
    }
  return pos;
}

/* Check the limitations of sub expressions LIMITS, and remove the nodes
   which are against limitations from DEST_NODES. */

static reg_errcode_t
check_subexp_limits (dfa, dest_nodes, candidates, limits, bkref_ents, str_idx)
     re_dfa_t *dfa;
     re_node_set *dest_nodes;
     const re_node_set *candidates;
     re_node_set *limits;
     struct re_backref_cache_entry *bkref_ents;
     int str_idx;
{
  reg_errcode_t err;
  int node_idx, lim_idx;

  for (lim_idx = 0; lim_idx < limits->nelem; ++lim_idx)
    {
      int subexp_idx;
      struct re_backref_cache_entry *ent;
      ent = bkref_ents + limits->elems[lim_idx];

      if (str_idx <= ent->subexp_from || ent->str_idx < str_idx)
	continue; /* This is unrelated limitation.  */

      subexp_idx = dfa->nodes[ent->node].opr.idx - 1;
      if (ent->subexp_to == str_idx)
	{
	  int ops_node = -1;
	  int cls_node = -1;
	  for (node_idx = 0; node_idx < dest_nodes->nelem; ++node_idx)
	    {
	      int node = dest_nodes->elems[node_idx];
	      re_token_type_t type= dfa->nodes[node].type;
	      if (type == OP_OPEN_SUBEXP
		  && subexp_idx == dfa->nodes[node].opr.idx)
		ops_node = node;
	      else if (type == OP_CLOSE_SUBEXP
		       && subexp_idx == dfa->nodes[node].opr.idx)
		cls_node = node;
	    }

	  /* Check the limitation of the open subexpression.  */
	  /* Note that (ent->subexp_to = str_idx != ent->subexp_from).  */
	  if (ops_node >= 0)
	    {
	      err = sub_epsilon_src_nodes(dfa, ops_node, dest_nodes,
					  candidates);
	      if (BE (err != REG_NOERROR, 0))
		return err;
	    }
	  /* Check the limitation of the close subexpression.  */
	  for (node_idx = 0; node_idx < dest_nodes->nelem; ++node_idx)
	    {
	      int node = dest_nodes->elems[node_idx];
	      if (!re_node_set_contains (dfa->inveclosures + node, cls_node)
		  && !re_node_set_contains (dfa->eclosures + node, cls_node))
		{
		  /* It is against this limitation.
		     Remove it form the current sifted state.  */
		  err = sub_epsilon_src_nodes(dfa, node, dest_nodes,
					      candidates);
		  if (BE (err != REG_NOERROR, 0))
		    return err;
		  --node_idx;
		}
	    }
	}
      else /* (ent->subexp_to != str_idx)  */
	{
	  for (node_idx = 0; node_idx < dest_nodes->nelem; ++node_idx)
	    {
	      int node = dest_nodes->elems[node_idx];
	      re_token_type_t type= dfa->nodes[node].type;
	      if (type == OP_CLOSE_SUBEXP || type == OP_OPEN_SUBEXP)
		{
		  if (subexp_idx != dfa->nodes[node].opr.idx)
		    continue;
		  if ((type == OP_CLOSE_SUBEXP && ent->subexp_to != str_idx)
		      || (type == OP_OPEN_SUBEXP))
		    {
		      /* It is against this limitation.
			 Remove it form the current sifted state.  */
		      err = sub_epsilon_src_nodes(dfa, node, dest_nodes,
						  candidates);
		      if (BE (err != REG_NOERROR, 0))
			return err;
		    }
		}
	    }
	}
    }
  return REG_NOERROR;
}

static reg_errcode_t
sift_states_bkref (preg, mctx, sctx, str_idx, dest_nodes)
     const regex_t *preg;
     re_match_context_t *mctx;
     re_sift_context_t *sctx;
     int str_idx;
     re_node_set *dest_nodes;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *)preg->buffer;
  int node_idx, node;
  re_sift_context_t local_sctx;
  const re_node_set *candidates;
  candidates = ((mctx->state_log[str_idx] == NULL) ? &empty_set
		: &mctx->state_log[str_idx]->nodes);
  local_sctx.sifted_states = NULL; /* Mark that it hasn't been initialized.  */

  for (node_idx = 0; node_idx < candidates->nelem; ++node_idx)
    {
      int cur_bkref_idx = re_string_cur_idx (mctx->input);
      re_token_type_t type;
      node = candidates->elems[node_idx];
      type = dfa->nodes[node].type;
      if (node == sctx->cur_bkref && str_idx == cur_bkref_idx)
	continue;
      /* Avoid infinite loop for the REs like "()\1+".  */
      if (node == sctx->last_node && str_idx == sctx->last_str_idx)
	continue;
      if (type == OP_BACK_REF)
	{
	  int enabled_idx = search_cur_bkref_entry (mctx, str_idx);
	  for (; enabled_idx < mctx->nbkref_ents; ++enabled_idx)
	    {
	      int disabled_idx, subexp_len, to_idx, dst_node;
	      struct re_backref_cache_entry *entry;
	      entry = mctx->bkref_ents + enabled_idx;
	      if (entry->str_idx > str_idx)
		break;
	      if (entry->node != node)
		  continue;
	      subexp_len = entry->subexp_to - entry->subexp_from;
	      to_idx = str_idx + subexp_len;
	      dst_node = (subexp_len ? dfa->nexts[node]
			  : dfa->edests[node].elems[0]);

	      if (to_idx > sctx->last_str_idx
		  || sctx->sifted_states[to_idx] == NULL
		  || !STATE_NODE_CONTAINS (sctx->sifted_states[to_idx],
					   dst_node)
		  || check_dst_limits (dfa, &sctx->limits, mctx, node,
				       str_idx, dst_node, to_idx))
		continue;
		{
		  re_dfastate_t *cur_state;
		  entry->flag = 0;
		  for (disabled_idx = enabled_idx + 1;
		       disabled_idx < mctx->nbkref_ents; ++disabled_idx)
		    {
		      struct re_backref_cache_entry *entry2;
		      entry2 = mctx->bkref_ents + disabled_idx;
		      if (entry2->str_idx > str_idx)
			break;
		      entry2->flag = (entry2->node == node) ? 1 : entry2->flag;
		    }

		  if (local_sctx.sifted_states == NULL)
		    {
		      local_sctx = *sctx;
		      err = re_node_set_init_copy (&local_sctx.limits,
						   &sctx->limits);
		      if (BE (err != REG_NOERROR, 0))
			goto free_return;
		    }
		  local_sctx.last_node = node;
		  local_sctx.last_str_idx = str_idx;
		  err = re_node_set_insert (&local_sctx.limits, enabled_idx);
		  if (BE (err < 0, 0))
		    {
		      err = REG_ESPACE;
		      goto free_return;
		    }
		  cur_state = local_sctx.sifted_states[str_idx];
		  err = sift_states_backward (preg, mctx, &local_sctx);
		  if (BE (err != REG_NOERROR, 0))
		    goto free_return;
		  if (sctx->limited_states != NULL)
		    {
		      err = merge_state_array (dfa, sctx->limited_states,
					       local_sctx.sifted_states,
					       str_idx + 1);
		      if (BE (err != REG_NOERROR, 0))
			goto free_return;
		    }
		  local_sctx.sifted_states[str_idx] = cur_state;
		  re_node_set_remove (&local_sctx.limits, enabled_idx);
		  /* We must not use the variable entry here, since
		     mctx->bkref_ents might be realloced.  */
		  mctx->bkref_ents[enabled_idx].flag = 1;
		}
	    }
	  enabled_idx = search_cur_bkref_entry (mctx, str_idx);
	  for (; enabled_idx < mctx->nbkref_ents; ++enabled_idx)
	    {
	      struct re_backref_cache_entry *entry;
	      entry = mctx->bkref_ents + enabled_idx;
	      if (entry->str_idx > str_idx)
		break;
	      if (entry->node == node)
		entry->flag = 0;
	    }
	}
    }
  err = REG_NOERROR;
 free_return:
  if (local_sctx.sifted_states != NULL)
    {
      re_node_set_free (&local_sctx.limits);
    }

  return err;
}


#ifdef RE_ENABLE_I18N
static int
sift_states_iter_mb (preg, mctx, sctx, node_idx, str_idx, max_str_idx)
    const regex_t *preg;
    const re_match_context_t *mctx;
    re_sift_context_t *sctx;
    int node_idx, str_idx, max_str_idx;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int naccepted;
  /* Check the node can accept `multi byte'.  */
  naccepted = check_node_accept_bytes (preg, node_idx, mctx->input, str_idx);
  if (naccepted > 0 && str_idx + naccepted <= max_str_idx &&
      !STATE_NODE_CONTAINS (sctx->sifted_states[str_idx + naccepted],
			    dfa->nexts[node_idx]))
    /* The node can't accept the `multi byte', or the
       destination was already throwed away, then the node
       could't accept the current input `multi byte'.   */
    naccepted = 0;
  /* Otherwise, it is sure that the node could accept
     `naccepted' bytes input.  */
  return naccepted;
}
#endif /* RE_ENABLE_I18N */


/* Functions for state transition.  */

/* Return the next state to which the current state STATE will transit by
   accepting the current input byte, and update STATE_LOG if necessary.
   If STATE can accept a multibyte char/collating element/back reference
   update the destination of STATE_LOG.  */

static re_dfastate_t *
transit_state (err, preg, mctx, state, fl_search)
     reg_errcode_t *err;
     const regex_t *preg;
     re_match_context_t *mctx;
     re_dfastate_t *state;
     int fl_search;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  re_dfastate_t **trtable, *next_state;
  unsigned char ch;
  int cur_idx;

  if (re_string_cur_idx (mctx->input) + 1 >= mctx->input->bufs_len
      || (re_string_cur_idx (mctx->input) + 1 >= mctx->input->valid_len
	  && mctx->input->valid_len < mctx->input->len))
    {
      *err = extend_buffers (mctx);
      if (BE (*err != REG_NOERROR, 0))
	return NULL;
    }

  *err = REG_NOERROR;
  if (state == NULL)
    {
      next_state = state;
      re_string_skip_bytes (mctx->input, 1);
    }
  else
    {
#ifdef RE_ENABLE_I18N
      /* If the current state can accept multibyte.  */
      if (state->accept_mb)
	{
	  *err = transit_state_mb (preg, state, mctx);
	  if (BE (*err != REG_NOERROR, 0))
	    return NULL;
	}
#endif /* RE_ENABLE_I18N */

      /* Then decide the next state with the single byte.  */
      if (1)
	{
	  /* Use transition table  */
	  ch = re_string_fetch_byte (mctx->input);
	  trtable = fl_search ? state->trtable_search : state->trtable;
	  if (trtable == NULL)
	    {
	      trtable = build_trtable (preg, state, fl_search);
	      if (fl_search)
		state->trtable_search = trtable;
	      else
		state->trtable = trtable;
	    }
	  next_state = trtable[ch];
	}
      else
	{
	  /* don't use transition table  */
	  next_state = transit_state_sb (err, preg, state, fl_search, mctx);
	  if (BE (next_state == NULL && err != REG_NOERROR, 0))
	    return NULL;
	}
    }

  cur_idx = re_string_cur_idx (mctx->input);
  /* Update the state_log if we need.  */
  if (mctx->state_log != NULL)
    {
      if (cur_idx > mctx->state_log_top)
	{
	  mctx->state_log[cur_idx] = next_state;
	  mctx->state_log_top = cur_idx;
	}
      else if (mctx->state_log[cur_idx] == 0)
	{
	  mctx->state_log[cur_idx] = next_state;
	}
      else
	{
	  re_dfastate_t *pstate;
	  unsigned int context;
	  re_node_set next_nodes, *log_nodes, *table_nodes = NULL;
	  /* If (state_log[cur_idx] != 0), it implies that cur_idx is
	     the destination of a multibyte char/collating element/
	     back reference.  Then the next state is the union set of
	     these destinations and the results of the transition table.  */
	  pstate = mctx->state_log[cur_idx];
	  log_nodes = pstate->entrance_nodes;
	  if (next_state != NULL)
	    {
	      table_nodes = next_state->entrance_nodes;
	      *err = re_node_set_init_union (&next_nodes, table_nodes,
					     log_nodes);
	      if (BE (*err != REG_NOERROR, 0))
		return NULL;
	    }
	  else
	    next_nodes = *log_nodes;
	  /* Note: We already add the nodes of the initial state,
		   then we don't need to add them here.  */

	  context = re_string_context_at (mctx->input,
					  re_string_cur_idx (mctx->input) - 1,
					  mctx->eflags, preg->newline_anchor);
	  next_state = mctx->state_log[cur_idx]
	    = re_acquire_state_context (err, dfa, &next_nodes, context);
	  /* We don't need to check errors here, since the return value of
	     this function is next_state and ERR is already set.  */

	  if (table_nodes != NULL)
	    re_node_set_free (&next_nodes);
	}
    }

  /* Check OP_OPEN_SUBEXP in the current state in case that we use them
     later.  We must check them here, since the back references in the
     next state might use them.  */
  if (dfa->nbackref && next_state/* && fl_process_bkref */)
    {
      *err = check_subexp_matching_top (dfa, mctx, &next_state->nodes,
					cur_idx);
      if (BE (*err != REG_NOERROR, 0))
	return NULL;
    }

  /* If the next state has back references.  */
  if (next_state != NULL && next_state->has_backref)
    {
      *err = transit_state_bkref (preg, &next_state->nodes, mctx);
      if (BE (*err != REG_NOERROR, 0))
	return NULL;
      next_state = mctx->state_log[cur_idx];
    }
  return next_state;
}

/* Helper functions for transit_state.  */

/* From the node set CUR_NODES, pick up the nodes whose types are
   OP_OPEN_SUBEXP and which have corresponding back references in the regular
   expression. And register them to use them later for evaluating the
   correspoding back references.  */

static reg_errcode_t
check_subexp_matching_top (dfa, mctx, cur_nodes, str_idx)
     re_dfa_t *dfa;
     re_match_context_t *mctx;
     re_node_set *cur_nodes;
     int str_idx;
{
  int node_idx;
  reg_errcode_t err;

  /* TODO: This isn't efficient.
	   Because there might be more than one nodes whose types are
	   OP_OPEN_SUBEXP and whose index is SUBEXP_IDX, we must check all
	   nodes.
	   E.g. RE: (a){2}  */
  for (node_idx = 0; node_idx < cur_nodes->nelem; ++node_idx)
    {
      int node = cur_nodes->elems[node_idx];
      if (dfa->nodes[node].type == OP_OPEN_SUBEXP
	  && dfa->used_bkref_map & (1 << dfa->nodes[node].opr.idx))
	{
	  err = match_ctx_add_subtop (mctx, node, str_idx);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	}
    }
  return REG_NOERROR;
}

/* Return the next state to which the current state STATE will transit by
   accepting the current input byte.  */

static re_dfastate_t *
transit_state_sb (err, preg, state, fl_search, mctx)
     reg_errcode_t *err;
     const regex_t *preg;
     re_dfastate_t *state;
     int fl_search;
     re_match_context_t *mctx;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  re_node_set next_nodes;
  re_dfastate_t *next_state;
  int node_cnt, cur_str_idx = re_string_cur_idx (mctx->input);
  unsigned int context;

  *err = re_node_set_alloc (&next_nodes, state->nodes.nelem + 1);
  if (BE (*err != REG_NOERROR, 0))
    return NULL;
  for (node_cnt = 0; node_cnt < state->nodes.nelem; ++node_cnt)
    {
      int cur_node = state->nodes.elems[node_cnt];
      if (check_node_accept (preg, dfa->nodes + cur_node, mctx, cur_str_idx))
	{
	  *err = re_node_set_merge (&next_nodes,
				    dfa->eclosures + dfa->nexts[cur_node]);
	  if (BE (*err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return NULL;
	    }
	}
    }
  if (fl_search)
    {
#ifdef RE_ENABLE_I18N
      int not_initial = 0;
      if (MB_CUR_MAX > 1)
	for (node_cnt = 0; node_cnt < next_nodes.nelem; ++node_cnt)
	  if (dfa->nodes[next_nodes.elems[node_cnt]].type == CHARACTER)
	    {
	      not_initial = dfa->nodes[next_nodes.elems[node_cnt]].mb_partial;
	      break;
	    }
      if (!not_initial)
#endif
	{
	  *err = re_node_set_merge (&next_nodes,
				    dfa->init_state->entrance_nodes);
	  if (BE (*err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return NULL;
	    }
	}
    }
  context = re_string_context_at (mctx->input, cur_str_idx, mctx->eflags,
				  preg->newline_anchor);
  next_state = re_acquire_state_context (err, dfa, &next_nodes, context);
  /* We don't need to check errors here, since the return value of
     this function is next_state and ERR is already set.  */

  re_node_set_free (&next_nodes);
  re_string_skip_bytes (mctx->input, 1);
  return next_state;
}

#ifdef RE_ENABLE_I18N
static reg_errcode_t
transit_state_mb (preg, pstate, mctx)
    const regex_t *preg;
    re_dfastate_t *pstate;
    re_match_context_t *mctx;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int i;

  for (i = 0; i < pstate->nodes.nelem; ++i)
    {
      re_node_set dest_nodes, *new_nodes;
      int cur_node_idx = pstate->nodes.elems[i];
      int naccepted = 0, dest_idx;
      unsigned int context;
      re_dfastate_t *dest_state;

      if (dfa->nodes[cur_node_idx].constraint)
	{
	  context = re_string_context_at (mctx->input,
					  re_string_cur_idx (mctx->input),
					  mctx->eflags, preg->newline_anchor);
	  if (NOT_SATISFY_NEXT_CONSTRAINT (dfa->nodes[cur_node_idx].constraint,
					   context))
	    continue;
	}

      /* How many bytes the node can accepts?  */
      if (ACCEPT_MB_NODE (dfa->nodes[cur_node_idx].type))
	naccepted = check_node_accept_bytes (preg, cur_node_idx, mctx->input,
					     re_string_cur_idx (mctx->input));
      if (naccepted == 0)
	continue;

      /* The node can accepts `naccepted' bytes.  */
      dest_idx = re_string_cur_idx (mctx->input) + naccepted;
      mctx->max_mb_elem_len = ((mctx->max_mb_elem_len < naccepted) ? naccepted
			       : mctx->max_mb_elem_len);
      err = clean_state_log_if_need (mctx, dest_idx);
      if (BE (err != REG_NOERROR, 0))
	return err;
#ifdef DEBUG
      assert (dfa->nexts[cur_node_idx] != -1);
#endif
      /* `cur_node_idx' may point the entity of the OP_CONTEXT_NODE,
	 then we use pstate->nodes.elems[i] instead.  */
      new_nodes = dfa->eclosures + dfa->nexts[pstate->nodes.elems[i]];

      dest_state = mctx->state_log[dest_idx];
      if (dest_state == NULL)
	dest_nodes = *new_nodes;
      else
	{
	  err = re_node_set_init_union (&dest_nodes,
					dest_state->entrance_nodes, new_nodes);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	}
      context = re_string_context_at (mctx->input, dest_idx - 1, mctx->eflags,
				      preg->newline_anchor);
      mctx->state_log[dest_idx]
	= re_acquire_state_context (&err, dfa, &dest_nodes, context);
      if (dest_state != NULL)
	re_node_set_free (&dest_nodes);
      if (BE (mctx->state_log[dest_idx] == NULL && err != REG_NOERROR, 0))
	return err;
    }
  return REG_NOERROR;
}
#endif /* RE_ENABLE_I18N */

static reg_errcode_t
transit_state_bkref (preg, nodes, mctx)
    const regex_t *preg;
    re_node_set *nodes;
    re_match_context_t *mctx;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int i;
  int cur_str_idx = re_string_cur_idx (mctx->input);

  for (i = 0; i < nodes->nelem; ++i)
    {
      int dest_str_idx, prev_nelem, bkc_idx;
      int node_idx = nodes->elems[i];
      unsigned int context;
      re_token_t *node = dfa->nodes + node_idx;
      re_node_set *new_dest_nodes;

      /* Check whether `node' is a backreference or not.  */
      if (node->type != OP_BACK_REF)
	continue;

      if (node->constraint)
	{
	  context = re_string_context_at (mctx->input, cur_str_idx,
					  mctx->eflags, preg->newline_anchor);
	  if (NOT_SATISFY_NEXT_CONSTRAINT (node->constraint, context))
	    continue;
	}

      /* `node' is a backreference.
	 Check the substring which the substring matched.  */
      bkc_idx = mctx->nbkref_ents;
      err = get_subexp (preg, mctx, node_idx, cur_str_idx);
      if (BE (err != REG_NOERROR, 0))
	goto free_return;

      /* And add the epsilon closures (which is `new_dest_nodes') of
	 the backreference to appropriate state_log.  */
#ifdef DEBUG
      assert (dfa->nexts[node_idx] != -1);
#endif
      for (; bkc_idx < mctx->nbkref_ents; ++bkc_idx)
	{
	  int subexp_len;
	  re_dfastate_t *dest_state;
	  struct re_backref_cache_entry *bkref_ent;
	  bkref_ent = mctx->bkref_ents + bkc_idx;
	  if (bkref_ent->node != node_idx || bkref_ent->str_idx != cur_str_idx)
	    continue;
	  subexp_len = bkref_ent->subexp_to - bkref_ent->subexp_from;
	  new_dest_nodes = (subexp_len == 0
			    ? dfa->eclosures + dfa->edests[node_idx].elems[0]
			    : dfa->eclosures + dfa->nexts[node_idx]);
	  dest_str_idx = (cur_str_idx + bkref_ent->subexp_to
			  - bkref_ent->subexp_from);
	  context = re_string_context_at (mctx->input, dest_str_idx - 1,
					  mctx->eflags, preg->newline_anchor);
	  dest_state = mctx->state_log[dest_str_idx];
	  prev_nelem = ((mctx->state_log[cur_str_idx] == NULL) ? 0
			: mctx->state_log[cur_str_idx]->nodes.nelem);
	  /* Add `new_dest_node' to state_log.  */
	  if (dest_state == NULL)
	    {
	      mctx->state_log[dest_str_idx]
		= re_acquire_state_context (&err, dfa, new_dest_nodes,
					    context);
	      if (BE (mctx->state_log[dest_str_idx] == NULL
		      && err != REG_NOERROR, 0))
		goto free_return;
	    }
	  else
	    {
	      re_node_set dest_nodes;
	      err = re_node_set_init_union (&dest_nodes,
					    dest_state->entrance_nodes,
					    new_dest_nodes);
	      if (BE (err != REG_NOERROR, 0))
		{
		  re_node_set_free (&dest_nodes);
		  goto free_return;
		}
	      mctx->state_log[dest_str_idx]
		= re_acquire_state_context (&err, dfa, &dest_nodes, context);
	      re_node_set_free (&dest_nodes);
	      if (BE (mctx->state_log[dest_str_idx] == NULL
		      && err != REG_NOERROR, 0))
		goto free_return;
	    }
	  /* We need to check recursively if the backreference can epsilon
	     transit.  */
	  if (subexp_len == 0
	      && mctx->state_log[cur_str_idx]->nodes.nelem > prev_nelem)
	    {
	      err = check_subexp_matching_top (dfa, mctx, new_dest_nodes,
					       cur_str_idx);
	      if (BE (err != REG_NOERROR, 0))
		goto free_return;
	      err = transit_state_bkref (preg, new_dest_nodes, mctx);
	      if (BE (err != REG_NOERROR, 0))
		goto free_return;
	    }
	}
    }
  err = REG_NOERROR;
 free_return:
  return err;
}

/* Enumerate all the candidates which the backreference BKREF_NODE can match
   at BKREF_STR_IDX, and register them by match_ctx_add_entry().
   Note that we might collect inappropriate candidates here.
   However, the cost of checking them strictly here is too high, then we
   delay these checking for prune_impossible_nodes().  */

static reg_errcode_t
get_subexp (preg, mctx, bkref_node, bkref_str_idx)
     const regex_t *preg;
     re_match_context_t *mctx;
     int bkref_node, bkref_str_idx;
{
  int subexp_num, sub_top_idx;
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  char *buf = (char *) re_string_get_buffer (mctx->input);
  /* Return if we have already checked BKREF_NODE at BKREF_STR_IDX.  */
  int cache_idx = search_cur_bkref_entry (mctx, bkref_str_idx);
  for (; cache_idx < mctx->nbkref_ents; ++cache_idx)
    {
      struct re_backref_cache_entry *entry = mctx->bkref_ents + cache_idx;
      if (entry->str_idx > bkref_str_idx)
	break;
      if (entry->node == bkref_node)
	return REG_NOERROR; /* We already checked it.  */
    }
  subexp_num = dfa->nodes[bkref_node].opr.idx - 1;

  /* For each sub expression  */
  for (sub_top_idx = 0; sub_top_idx < mctx->nsub_tops; ++sub_top_idx)
    {
      reg_errcode_t err;
      re_sub_match_top_t *sub_top = mctx->sub_tops[sub_top_idx];
      re_sub_match_last_t *sub_last;
      int sub_last_idx, sl_str;
      char *bkref_str;

      if (dfa->nodes[sub_top->node].opr.idx != subexp_num)
	continue; /* It isn't related.  */

      sl_str = sub_top->str_idx;
      bkref_str = buf + bkref_str_idx;
      /* At first, check the last node of sub expressions we already
	 evaluated.  */
      for (sub_last_idx = 0; sub_last_idx < sub_top->nlasts; ++sub_last_idx)
	{
	  int sl_str_diff;
	  sub_last = sub_top->lasts[sub_last_idx];
	  sl_str_diff = sub_last->str_idx - sl_str;
	  /* The matched string by the sub expression match with the substring
	     at the back reference?  */
	  if (sl_str_diff > 0
	      && memcmp (bkref_str, buf + sl_str, sl_str_diff) != 0)
	    break; /* We don't need to search this sub expression any more.  */
	  bkref_str += sl_str_diff;
	  sl_str += sl_str_diff;
	  err = get_subexp_sub (preg, mctx, sub_top, sub_last, bkref_node,
				bkref_str_idx);
	  if (err == REG_NOMATCH)
	    continue;
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	}
      if (sub_last_idx < sub_top->nlasts)
	continue;
      if (sub_last_idx > 0)
	++sl_str;
      /* Then, search for the other last nodes of the sub expression.  */
      for (; sl_str <= bkref_str_idx; ++sl_str)
	{
	  int cls_node, sl_str_off;
	  re_node_set *nodes;
	  sl_str_off = sl_str - sub_top->str_idx;
	  /* The matched string by the sub expression match with the substring
	     at the back reference?  */
	  if (sl_str_off > 0
	      && memcmp (bkref_str++, buf + sl_str - 1, 1) != 0)
	    break; /* We don't need to search this sub expression any more.  */
	  if (mctx->state_log[sl_str] == NULL)
	    continue;
	  /* Does this state have a ')' of the sub expression?  */
	  nodes = &mctx->state_log[sl_str]->nodes;
	  cls_node = find_subexp_node (dfa, nodes, subexp_num, 0);
	  if (cls_node == -1)
	    continue; /* No.  */
	  if (sub_top->path == NULL)
	    {
	      sub_top->path = calloc (sizeof (state_array_t),
				      sl_str - sub_top->str_idx + 1);
	      if (sub_top->path == NULL)
		return REG_ESPACE;
	    }
	  /* Can the OP_OPEN_SUBEXP node arrive the OP_CLOSE_SUBEXP node
	     in the current context?  */
	  err = check_arrival (preg, mctx, sub_top->path, sub_top->node,
			       sub_top->str_idx, cls_node, sl_str, 0);
	  if (err == REG_NOMATCH)
	      continue;
	  if (BE (err != REG_NOERROR, 0))
	      return err;
	  sub_last = match_ctx_add_sublast (sub_top, cls_node, sl_str);
	  if (BE (sub_last == NULL, 0))
	    return REG_ESPACE;
	  err = get_subexp_sub (preg, mctx, sub_top, sub_last, bkref_node,
				bkref_str_idx);
	  if (err == REG_NOMATCH)
	    continue;
	}
    }
  return REG_NOERROR;
}

/* Helper functions for get_subexp().  */

/* Check SUB_LAST can arrive to the back reference BKREF_NODE at BKREF_STR.
   If it can arrive, register the sub expression expressed with SUB_TOP
   and SUB_LAST.  */

static reg_errcode_t
get_subexp_sub (preg, mctx, sub_top, sub_last, bkref_node, bkref_str)
     const regex_t *preg;
     re_match_context_t *mctx;
     re_sub_match_top_t *sub_top;
     re_sub_match_last_t *sub_last;
     int bkref_node, bkref_str;
{
  reg_errcode_t err;
  int to_idx;
  /* Can the subexpression arrive the back reference?  */
  err = check_arrival (preg, mctx, &sub_last->path, sub_last->node,
		       sub_last->str_idx, bkref_node, bkref_str, 1);
  if (err != REG_NOERROR)
    return err;
  err = match_ctx_add_entry (mctx, bkref_node, bkref_str, sub_top->str_idx,
			     sub_last->str_idx);
  if (BE (err != REG_NOERROR, 0))
    return err;
  to_idx = bkref_str + sub_last->str_idx - sub_top->str_idx;
  clean_state_log_if_need (mctx, to_idx);
  return REG_NOERROR;
}

/* Find the first node which is '(' or ')' and whose index is SUBEXP_IDX.
   Search '(' if FL_OPEN, or search ')' otherwise.
   TODO: This function isn't efficient...
	 Because there might be more than one nodes whose types are
	 OP_OPEN_SUBEXP and whose index is SUBEXP_IDX, we must check all
	 nodes.
	 E.g. RE: (a){2}  */

static int
find_subexp_node (dfa, nodes, subexp_idx, fl_open)
     re_dfa_t *dfa;
     re_node_set *nodes;
     int subexp_idx, fl_open;
{
  int cls_idx;
  for (cls_idx = 0; cls_idx < nodes->nelem; ++cls_idx)
    {
      int cls_node = nodes->elems[cls_idx];
      re_token_t *node = dfa->nodes + cls_node;
      if (((fl_open && node->type == OP_OPEN_SUBEXP)
	  || (!fl_open && node->type == OP_CLOSE_SUBEXP))
	  && node->opr.idx == subexp_idx)
	return cls_node;
    }
  return -1;
}

/* Check whether the node TOP_NODE at TOP_STR can arrive to the node
   LAST_NODE at LAST_STR.  We record the path onto PATH since it will be
   heavily reused.
   Return REG_NOERROR if it can arrive, or REG_NOMATCH otherwise.  */

static reg_errcode_t
check_arrival (preg, mctx, path, top_node, top_str, last_node, last_str,
	       fl_open)
     const regex_t *preg;
     re_match_context_t *mctx;
     state_array_t *path;
     int top_node, top_str, last_node, last_str, fl_open;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  reg_errcode_t err;
  int subexp_num, backup_cur_idx, str_idx, null_cnt;
  re_dfastate_t *cur_state = NULL;
  re_node_set *cur_nodes, next_nodes;
  re_dfastate_t **backup_state_log;
  unsigned int context;

  subexp_num = dfa->nodes[top_node].opr.idx;
  /* Extend the buffer if we need.  */
  if (path->alloc < last_str + mctx->max_mb_elem_len + 1)
    {
      re_dfastate_t **new_array;
      int old_alloc = path->alloc;
      path->alloc += last_str + mctx->max_mb_elem_len + 1;
      new_array = re_realloc (path->array, re_dfastate_t *, path->alloc);
      if (new_array == NULL)
	return REG_ESPACE;
      path->array = new_array;
      memset (new_array + old_alloc, '\0',
	      sizeof (re_dfastate_t *) * (path->alloc - old_alloc));
    }

  str_idx = path->next_idx == 0 ? top_str : path->next_idx;

  /* Temporary modify MCTX.  */
  backup_state_log = mctx->state_log;
  backup_cur_idx = mctx->input->cur_idx;
  mctx->state_log = path->array;
  mctx->input->cur_idx = str_idx;

  /* Setup initial node set.  */
  context = re_string_context_at (mctx->input, str_idx - 1, mctx->eflags,
				  preg->newline_anchor);
  if (str_idx == top_str)
    {
      err = re_node_set_init_1 (&next_nodes, top_node);
      if (BE (err != REG_NOERROR, 0))
	return err;
      err = check_arrival_expand_ecl (dfa, &next_nodes, subexp_num, fl_open);
      if (BE (err != REG_NOERROR, 0))
	{
	  re_node_set_free (&next_nodes);
	  return err;
	}
    }
  else
    {
      cur_state = mctx->state_log[str_idx];
      if (cur_state && cur_state->has_backref)
	{
	  err = re_node_set_init_copy (&next_nodes, &cur_state->nodes);
	  if (BE ( err != REG_NOERROR, 0))
	    return err;
	}
      else
	re_node_set_init_empty (&next_nodes);
    }
  if (str_idx == top_str || (cur_state && cur_state->has_backref))
    {
      if (next_nodes.nelem)
	{
	  err = expand_bkref_cache (preg, mctx, &next_nodes, str_idx, last_str,
				    subexp_num, fl_open);
	  if (BE ( err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return err;
	    }
	}
      cur_state = re_acquire_state_context (&err, dfa, &next_nodes, context);
      if (BE (cur_state == NULL && err != REG_NOERROR, 0))
	{
	  re_node_set_free (&next_nodes);
	  return err;
	}
      mctx->state_log[str_idx] = cur_state;
    }

  for (null_cnt = 0; str_idx < last_str && null_cnt <= mctx->max_mb_elem_len;)
    {
      re_node_set_empty (&next_nodes);
      if (mctx->state_log[str_idx + 1])
	{
	  err = re_node_set_merge (&next_nodes,
				   &mctx->state_log[str_idx + 1]->nodes);
	  if (BE (err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return err;
	    }
	}
      if (cur_state)
	{
	  err = check_arrival_add_next_nodes(preg, dfa, mctx, str_idx,
					     &cur_state->nodes, &next_nodes);
	  if (BE (err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return err;
	    }
	}
      ++str_idx;
      if (next_nodes.nelem)
	{
	  err = check_arrival_expand_ecl (dfa, &next_nodes, subexp_num,
					  fl_open);
	  if (BE (err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return err;
	    }
	  err = expand_bkref_cache (preg, mctx, &next_nodes, str_idx, last_str,
				    subexp_num, fl_open);
	  if (BE ( err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&next_nodes);
	      return err;
	    }
	}
      context = re_string_context_at (mctx->input, str_idx - 1, mctx->eflags,
				      preg->newline_anchor);
      cur_state = re_acquire_state_context (&err, dfa, &next_nodes, context);
      if (BE (cur_state == NULL && err != REG_NOERROR, 0))
	{
	  re_node_set_free (&next_nodes);
	  return err;
	}
      mctx->state_log[str_idx] = cur_state;
      null_cnt = cur_state == NULL ? null_cnt + 1 : 0;
    }
  re_node_set_free (&next_nodes);
  cur_nodes = (mctx->state_log[last_str] == NULL ? NULL
	       : &mctx->state_log[last_str]->nodes);
  path->next_idx = str_idx;

  /* Fix MCTX.  */
  mctx->state_log = backup_state_log;
  mctx->input->cur_idx = backup_cur_idx;

  if (cur_nodes == NULL)
    return REG_NOMATCH;
  /* Then check the current node set has the node LAST_NODE.  */
  return (re_node_set_contains (cur_nodes, last_node)
	  || re_node_set_contains (cur_nodes, last_node) ? REG_NOERROR
	  : REG_NOMATCH);
}

/* Helper functions for check_arrival.  */

/* Calculate the destination nodes of CUR_NODES at STR_IDX, and append them
   to NEXT_NODES.
   TODO: This function is similar to the functions transit_state*(),
	 however this function has many additional works.
	 Can't we unify them?  */

static reg_errcode_t
check_arrival_add_next_nodes (preg, dfa, mctx, str_idx, cur_nodes, next_nodes)
     const regex_t *preg;
     re_dfa_t *dfa;
     re_match_context_t *mctx;
     int str_idx;
     re_node_set *cur_nodes, *next_nodes;
{
  int cur_idx;
  reg_errcode_t err;
  re_node_set union_set;
  re_node_set_init_empty (&union_set);
  for (cur_idx = 0; cur_idx < cur_nodes->nelem; ++cur_idx)
    {
      int naccepted = 0;
      int cur_node = cur_nodes->elems[cur_idx];
      re_token_type_t type = dfa->nodes[cur_node].type;
      if (IS_EPSILON_NODE(type))
	continue;
#ifdef RE_ENABLE_I18N
      /* If the node may accept `multi byte'.  */
      if (ACCEPT_MB_NODE (type))
	{
	  naccepted = check_node_accept_bytes (preg, cur_node, mctx->input,
					       str_idx);
	  if (naccepted > 1)
	    {
	      re_dfastate_t *dest_state;
	      int next_node = dfa->nexts[cur_node];
	      int next_idx = str_idx + naccepted;
	      dest_state = mctx->state_log[next_idx];
	      re_node_set_empty (&union_set);
	      if (dest_state)
		{
		  err = re_node_set_merge (&union_set, &dest_state->nodes);
		  if (BE (err != REG_NOERROR, 0))
		    {
		      re_node_set_free (&union_set);
		      return err;
		    }
		  err = re_node_set_insert (&union_set, next_node);
		  if (BE (err < 0, 0))
		    {
		      re_node_set_free (&union_set);
		      return REG_ESPACE;
		    }
		}
	      else
		{
		  err = re_node_set_insert (&union_set, next_node);
		  if (BE (err < 0, 0))
		    {
		      re_node_set_free (&union_set);
		      return REG_ESPACE;
		    }
		}
	      mctx->state_log[next_idx] = re_acquire_state (&err, dfa,
							    &union_set);
	      if (BE (mctx->state_log[next_idx] == NULL
		      && err != REG_NOERROR, 0))
		{
		  re_node_set_free (&union_set);
		  return err;
		}
	    }
	}
#endif /* RE_ENABLE_I18N */
      if (naccepted
	  || check_node_accept (preg, dfa->nodes + cur_node, mctx,
				str_idx))
	{
	  err = re_node_set_insert (next_nodes, dfa->nexts[cur_node]);
	  if (BE (err < 0, 0))
	    {
	      re_node_set_free (&union_set);
	      return REG_ESPACE;
	    }
	}
    }
  re_node_set_free (&union_set);
  return REG_NOERROR;
}

/* For all the nodes in CUR_NODES, add the epsilon closures of them to
   CUR_NODES, however exclude the nodes which are:
    - inside the sub expression whose number is EX_SUBEXP, if FL_OPEN.
    - out of the sub expression whose number is EX_SUBEXP, if !FL_OPEN.
*/

static reg_errcode_t
check_arrival_expand_ecl (dfa, cur_nodes, ex_subexp, fl_open)
     re_dfa_t *dfa;
     re_node_set *cur_nodes;
     int ex_subexp, fl_open;
{
  reg_errcode_t err;
  int idx, outside_node;
  re_node_set new_nodes;
#ifdef DEBUG
  assert (cur_nodes->nelem);
#endif
  err = re_node_set_alloc (&new_nodes, cur_nodes->nelem);
  if (BE (err != REG_NOERROR, 0))
    return err;
  /* Create a new node set NEW_NODES with the nodes which are epsilon
     closures of the node in CUR_NODES.  */

  for (idx = 0; idx < cur_nodes->nelem; ++idx)
    {
      int cur_node = cur_nodes->elems[idx];
      re_node_set *eclosure = dfa->eclosures + cur_node;
      outside_node = find_subexp_node (dfa, eclosure, ex_subexp, fl_open);
      if (outside_node == -1)
	{
	  /* There are no problematic nodes, just merge them.  */
	  err = re_node_set_merge (&new_nodes, eclosure);
	  if (BE (err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&new_nodes);
	      return err;
	    }
	}
      else
	{
	  /* There are problematic nodes, re-calculate incrementally.  */
	  err = check_arrival_expand_ecl_sub (dfa, &new_nodes, cur_node,
					      ex_subexp, fl_open);
	  if (BE (err != REG_NOERROR, 0))
	    {
	      re_node_set_free (&new_nodes);
	      return err;
	    }
	}
    }
  re_node_set_free (cur_nodes);
  *cur_nodes = new_nodes;
  return REG_NOERROR;
}

/* Helper function for check_arrival_expand_ecl.
   Check incrementally the epsilon closure of TARGET, and if it isn't
   problematic append it to DST_NODES.  */

static reg_errcode_t
check_arrival_expand_ecl_sub (dfa, dst_nodes, target, ex_subexp, fl_open)
     re_dfa_t *dfa;
     int target, ex_subexp, fl_open;
     re_node_set *dst_nodes;
{
  int cur_node, type;
  for (cur_node = target; !re_node_set_contains (dst_nodes, cur_node);)
    {
      int err;
      type = dfa->nodes[cur_node].type;

      if (((type == OP_OPEN_SUBEXP && fl_open)
	   || (type == OP_CLOSE_SUBEXP && !fl_open))
	  && dfa->nodes[cur_node].opr.idx == ex_subexp)
	{
	  if (!fl_open)
	    {
	      err = re_node_set_insert (dst_nodes, cur_node);
	      if (BE (err == -1, 0))
		return REG_ESPACE;
	    }
	  break;
	}
      err = re_node_set_insert (dst_nodes, cur_node);
      if (BE (err == -1, 0))
	return REG_ESPACE;
      if (dfa->edests[cur_node].nelem == 0)
	break;
      if (dfa->edests[cur_node].nelem == 2)
	{
	  err = check_arrival_expand_ecl_sub (dfa, dst_nodes,
					      dfa->edests[cur_node].elems[1],
					      ex_subexp, fl_open);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	}
      cur_node = dfa->edests[cur_node].elems[0];
    }
  return REG_NOERROR;
}


/* For all the back references in the current state, calculate the
   destination of the back references by the appropriate entry
   in MCTX->BKREF_ENTS.  */

static reg_errcode_t
expand_bkref_cache (preg, mctx, cur_nodes, cur_str, last_str, subexp_num,
		    fl_open)
     const regex_t *preg;
     re_match_context_t *mctx;
     int cur_str, last_str, subexp_num, fl_open;
     re_node_set *cur_nodes;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int cache_idx, cache_idx_start;
  /* The current state.  */

  cache_idx_start = search_cur_bkref_entry (mctx, cur_str);
  for (cache_idx = cache_idx_start; cache_idx < mctx->nbkref_ents; ++cache_idx)
    {
      int to_idx, next_node;
      struct re_backref_cache_entry *ent = mctx->bkref_ents + cache_idx;
      if (ent->str_idx > cur_str)
	break;
      /* Is this entry ENT is appropriate?  */
      if (!re_node_set_contains (cur_nodes, ent->node))
	continue; /* No.  */

      to_idx = cur_str + ent->subexp_to - ent->subexp_from;
      /* Calculate the destination of the back reference, and append it
	 to MCTX->STATE_LOG.  */
      if (to_idx == cur_str)
	{
	  /* The backreference did epsilon transit, we must re-check all the
	     node in the current state.  */
	  re_node_set new_dests;
	  reg_errcode_t err2, err3;
	  next_node = dfa->edests[ent->node].elems[0];
	  if (re_node_set_contains (cur_nodes, next_node))
	    continue;
	  err = re_node_set_init_1 (&new_dests, next_node);
	  err2 = check_arrival_expand_ecl (dfa, &new_dests, subexp_num,
					   fl_open);
	  err3 = re_node_set_merge (cur_nodes, &new_dests);
	  re_node_set_free (&new_dests);
	  if (BE (err != REG_NOERROR || err2 != REG_NOERROR
		  || err3 != REG_NOERROR, 0))
	    {
	      err = (err != REG_NOERROR ? err
		     : (err2 != REG_NOERROR ? err2 : err3));
	      return err;
	    }
	  /* TODO: It is still inefficient...  */
	  cache_idx = cache_idx_start - 1;
	  continue;
	}
      else
	{
	  re_node_set union_set;
	  next_node = dfa->nexts[ent->node];
	  if (mctx->state_log[to_idx])
	    {
	      int ret;
	      if (re_node_set_contains (&mctx->state_log[to_idx]->nodes,
					next_node))
		continue;
	      err = re_node_set_init_copy (&union_set,
					   &mctx->state_log[to_idx]->nodes);
	      ret = re_node_set_insert (&union_set, next_node);
	      if (BE (err != REG_NOERROR || ret < 0, 0))
		{
		  re_node_set_free (&union_set);
		  err = err != REG_NOERROR ? err : REG_ESPACE;
		  return err;
		}
	    }
	  else
	    {
	      err = re_node_set_init_1 (&union_set, next_node);
	      if (BE (err != REG_NOERROR, 0))
		return err;
	    }
	  mctx->state_log[to_idx] = re_acquire_state (&err, dfa, &union_set);
	  re_node_set_free (&union_set);
	  if (BE (mctx->state_log[to_idx] == NULL
		  && err != REG_NOERROR, 0))
	    return err;
	}
    }
  return REG_NOERROR;
}

/* Build transition table for the state.
   Return the new table if succeeded, otherwise return NULL.  */

static re_dfastate_t **
build_trtable (preg, state, fl_search)
    const regex_t *preg;
    const re_dfastate_t *state;
    int fl_search;
{
  reg_errcode_t err;
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int i, j, k, ch;
  int dests_node_malloced = 0, dest_states_malloced = 0;
  int ndests; /* Number of the destination states from `state'.  */
  re_dfastate_t **trtable;
  re_dfastate_t **dest_states = NULL, **dest_states_word, **dest_states_nl;
  re_node_set follows, *dests_node;
  bitset *dests_ch;
  bitset acceptable;

  /* We build DFA states which corresponds to the destination nodes
     from `state'.  `dests_node[i]' represents the nodes which i-th
     destination state contains, and `dests_ch[i]' represents the
     characters which i-th destination state accepts.  */
#ifdef _LIBC
  if (__libc_use_alloca ((sizeof (re_node_set) + sizeof (bitset)) * SBC_MAX))
    dests_node = (re_node_set *)
		 alloca ((sizeof (re_node_set) + sizeof (bitset)) * SBC_MAX);
  else
#endif
    {
      dests_node = (re_node_set *)
		   malloc ((sizeof (re_node_set) + sizeof (bitset)) * SBC_MAX);
      if (BE (dests_node == NULL, 0))
	return NULL;
      dests_node_malloced = 1;
    }
  dests_ch = (bitset *) (dests_node + SBC_MAX);

  /* Initialize transiton table.  */
  trtable = (re_dfastate_t **) calloc (sizeof (re_dfastate_t *), SBC_MAX);
  if (BE (trtable == NULL, 0))
    {
      if (dests_node_malloced)
	free (dests_node);
      return NULL;
    }

  /* At first, group all nodes belonging to `state' into several
     destinations.  */
  ndests = group_nodes_into_DFAstates (preg, state, dests_node, dests_ch);
  if (BE (ndests <= 0, 0))
    {
      if (dests_node_malloced)
	free (dests_node);
      /* Return NULL in case of an error, trtable otherwise.  */
      if (ndests == 0)
	return trtable;
      free (trtable);
      return NULL;
    }

  err = re_node_set_alloc (&follows, ndests + 1);
  if (BE (err != REG_NOERROR, 0))
    goto out_free;

#ifdef _LIBC
  if (__libc_use_alloca ((sizeof (re_node_set) + sizeof (bitset)) * SBC_MAX
			 + ndests * 3 * sizeof (re_dfastate_t *)))
    dest_states = (re_dfastate_t **)
		  alloca (ndests * 3 * sizeof (re_dfastate_t *));
  else
#endif
    {
      dest_states = (re_dfastate_t **)
		    malloc (ndests * 3 * sizeof (re_dfastate_t *));
      if (BE (dest_states == NULL, 0))
	{
out_free:
	  if (dest_states_malloced)
	    free (dest_states);
	  re_node_set_free (&follows);
	  for (i = 0; i < ndests; ++i)
	    re_node_set_free (dests_node + i);
	  free (trtable);
	  if (dests_node_malloced)
	    free (dests_node);
	  return NULL;
	}
      dest_states_malloced = 1;
    }
  dest_states_word = dest_states + ndests;
  dest_states_nl = dest_states_word + ndests;
  bitset_empty (acceptable);

  /* Then build the states for all destinations.  */
  for (i = 0; i < ndests; ++i)
    {
      int next_node;
      re_node_set_empty (&follows);
      /* Merge the follows of this destination states.  */
      for (j = 0; j < dests_node[i].nelem; ++j)
	{
	  next_node = dfa->nexts[dests_node[i].elems[j]];
	  if (next_node != -1)
	    {
	      err = re_node_set_merge (&follows, dfa->eclosures + next_node);
	      if (BE (err != REG_NOERROR, 0))
		goto out_free;
	    }
	}
      /* If search flag is set, merge the initial state.  */
      if (fl_search)
	{
#ifdef RE_ENABLE_I18N
	  int not_initial = 0;
	  for (j = 0; j < follows.nelem; ++j)
	    if (dfa->nodes[follows.elems[j]].type == CHARACTER)
	      {
		not_initial = dfa->nodes[follows.elems[j]].mb_partial;
		break;
	      }
	  if (!not_initial)
#endif
	    {
	      err = re_node_set_merge (&follows,
				       dfa->init_state->entrance_nodes);
	      if (BE (err != REG_NOERROR, 0))
		goto out_free;
	    }
	}
      dest_states[i] = re_acquire_state_context (&err, dfa, &follows, 0);
      if (BE (dest_states[i] == NULL && err != REG_NOERROR, 0))
	goto out_free;
      /* If the new state has context constraint,
	 build appropriate states for these contexts.  */
      if (dest_states[i]->has_constraint)
	{
	  dest_states_word[i] = re_acquire_state_context (&err, dfa, &follows,
							  CONTEXT_WORD);
	  if (BE (dest_states_word[i] == NULL && err != REG_NOERROR, 0))
	    goto out_free;
	  dest_states_nl[i] = re_acquire_state_context (&err, dfa, &follows,
							CONTEXT_NEWLINE);
	  if (BE (dest_states_nl[i] == NULL && err != REG_NOERROR, 0))
	    goto out_free;
	}
      else
	{
	  dest_states_word[i] = dest_states[i];
	  dest_states_nl[i] = dest_states[i];
	}
      bitset_merge (acceptable, dests_ch[i]);
    }

  /* Update the transition table.  */
  /* For all characters ch...:  */
  for (i = 0, ch = 0; i < BITSET_UINTS; ++i)
    for (j = 0; j < UINT_BITS; ++j, ++ch)
      if ((acceptable[i] >> j) & 1)
	{
	  /* The current state accepts the character ch.  */
	  if (IS_WORD_CHAR (ch))
	    {
	      for (k = 0; k < ndests; ++k)
		if ((dests_ch[k][i] >> j) & 1)
		  {
		    /* k-th destination accepts the word character ch.  */
		    trtable[ch] = dest_states_word[k];
		    /* There must be only one destination which accepts
		       character ch.  See group_nodes_into_DFAstates.  */
		    break;
		  }
	    }
	  else /* not WORD_CHAR */
	    {
	      for (k = 0; k < ndests; ++k)
		if ((dests_ch[k][i] >> j) & 1)
		  {
		    /* k-th destination accepts the non-word character ch.  */
		    trtable[ch] = dest_states[k];
		    /* There must be only one destination which accepts
		       character ch.  See group_nodes_into_DFAstates.  */
		    break;
		  }
	    }
	}
  /* new line */
  if (bitset_contain (acceptable, NEWLINE_CHAR))
    {
      /* The current state accepts newline character.  */
      for (k = 0; k < ndests; ++k)
	if (bitset_contain (dests_ch[k], NEWLINE_CHAR))
	  {
	    /* k-th destination accepts newline character.  */
	    trtable[NEWLINE_CHAR] = dest_states_nl[k];
	    /* There must be only one destination which accepts
	       newline.  See group_nodes_into_DFAstates.  */
	    break;
	  }
    }

  if (dest_states_malloced)
    free (dest_states);

  re_node_set_free (&follows);
  for (i = 0; i < ndests; ++i)
    re_node_set_free (dests_node + i);

  if (dests_node_malloced)
    free (dests_node);

  return trtable;
}

/* Group all nodes belonging to STATE into several destinations.
   Then for all destinations, set the nodes belonging to the destination
   to DESTS_NODE[i] and set the characters accepted by the destination
   to DEST_CH[i].  This function return the number of destinations.  */

static int
group_nodes_into_DFAstates (preg, state, dests_node, dests_ch)
    const regex_t *preg;
    const re_dfastate_t *state;
    re_node_set *dests_node;
    bitset *dests_ch;
{
  reg_errcode_t err;
  const re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  int i, j, k;
  int ndests; /* Number of the destinations from `state'.  */
  bitset accepts; /* Characters a node can accept.  */
  const re_node_set *cur_nodes = &state->nodes;
  bitset_empty (accepts);
  ndests = 0;

  /* For all the nodes belonging to `state',  */
  for (i = 0; i < cur_nodes->nelem; ++i)
    {
      re_token_t *node = &dfa->nodes[cur_nodes->elems[i]];
      re_token_type_t type = node->type;
      unsigned int constraint = node->constraint;

      /* Enumerate all single byte character this node can accept.  */
      if (type == CHARACTER)
	bitset_set (accepts, node->opr.c);
      else if (type == SIMPLE_BRACKET)
	{
	  bitset_merge (accepts, node->opr.sbcset);
	}
      else if (type == OP_PERIOD)
	{
	  bitset_set_all (accepts);
	  if (!(preg->syntax & RE_DOT_NEWLINE))
	    bitset_clear (accepts, '\n');
	  if (preg->syntax & RE_DOT_NOT_NULL)
	    bitset_clear (accepts, '\0');
	}
      else
	continue;

      /* Check the `accepts' and sift the characters which are not
	 match it the context.  */
      if (constraint)
	{
	  if (constraint & NEXT_WORD_CONSTRAINT)
	    for (j = 0; j < BITSET_UINTS; ++j)
	      accepts[j] &= dfa->word_char[j];
	  if (constraint & NEXT_NOTWORD_CONSTRAINT)
	    for (j = 0; j < BITSET_UINTS; ++j)
	      accepts[j] &= ~dfa->word_char[j];
	  if (constraint & NEXT_NEWLINE_CONSTRAINT)
	    {
	      int accepts_newline = bitset_contain (accepts, NEWLINE_CHAR);
	      bitset_empty (accepts);
	      if (accepts_newline)
		bitset_set (accepts, NEWLINE_CHAR);
	      else
		continue;
	    }
	}

      /* Then divide `accepts' into DFA states, or create a new
	 state.  */
      for (j = 0; j < ndests; ++j)
	{
	  bitset intersec; /* Intersection sets, see below.  */
	  bitset remains;
	  /* Flags, see below.  */
	  int has_intersec, not_subset, not_consumed;

	  /* Optimization, skip if this state doesn't accept the character.  */
	  if (type == CHARACTER && !bitset_contain (dests_ch[j], node->opr.c))
	    continue;

	  /* Enumerate the intersection set of this state and `accepts'.  */
	  has_intersec = 0;
	  for (k = 0; k < BITSET_UINTS; ++k)
	    has_intersec |= intersec[k] = accepts[k] & dests_ch[j][k];
	  /* And skip if the intersection set is empty.  */
	  if (!has_intersec)
	    continue;

	  /* Then check if this state is a subset of `accepts'.  */
	  not_subset = not_consumed = 0;
	  for (k = 0; k < BITSET_UINTS; ++k)
	    {
	      not_subset |= remains[k] = ~accepts[k] & dests_ch[j][k];
	      not_consumed |= accepts[k] = accepts[k] & ~dests_ch[j][k];
	    }

	  /* If this state isn't a subset of `accepts', create a
	     new group state, which has the `remains'. */
	  if (not_subset)
	    {
	      bitset_copy (dests_ch[ndests], remains);
	      bitset_copy (dests_ch[j], intersec);
	      err = re_node_set_init_copy (dests_node + ndests, &dests_node[j]);
	      if (BE (err != REG_NOERROR, 0))
		goto error_return;
	      ++ndests;
	    }

	  /* Put the position in the current group. */
	  err = re_node_set_insert (&dests_node[j], cur_nodes->elems[i]);
	  if (BE (err < 0, 0))
	    goto error_return;

	  /* If all characters are consumed, go to next node. */
	  if (!not_consumed)
	    break;
	}
      /* Some characters remain, create a new group. */
      if (j == ndests)
	{
	  bitset_copy (dests_ch[ndests], accepts);
	  err = re_node_set_init_1 (dests_node + ndests, cur_nodes->elems[i]);
	  if (BE (err != REG_NOERROR, 0))
	    goto error_return;
	  ++ndests;
	  bitset_empty (accepts);
	}
    }
  return ndests;
 error_return:
  for (j = 0; j < ndests; ++j)
    re_node_set_free (dests_node + j);
  return -1;
}

#ifdef RE_ENABLE_I18N
/* Check how many bytes the node `dfa->nodes[node_idx]' accepts.
   Return the number of the bytes the node accepts.
   STR_IDX is the current index of the input string.

   This function handles the nodes which can accept one character, or
   one collating element like '.', '[a-z]', opposite to the other nodes
   can only accept one byte.  */

static int
check_node_accept_bytes (preg, node_idx, input, str_idx)
    const regex_t *preg;
    int node_idx, str_idx;
    const re_string_t *input;
{
  const re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  const re_token_t *node = dfa->nodes + node_idx;
  int elem_len = re_string_elem_size_at (input, str_idx);
  int char_len = re_string_char_size_at (input, str_idx);
  int i;
# ifdef _LIBC
  int j;
  uint32_t nrules = _NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_NRULES);
# endif /* _LIBC */
  if (elem_len <= 1 && char_len <= 1)
    return 0;
  if (node->type == OP_PERIOD)
    {
      /* '.' accepts any one character except the following two cases.  */
      if ((!(preg->syntax & RE_DOT_NEWLINE) &&
	   re_string_byte_at (input, str_idx) == '\n') ||
	  ((preg->syntax & RE_DOT_NOT_NULL) &&
	   re_string_byte_at (input, str_idx) == '\0'))
	return 0;
      return char_len;
    }
  else if (node->type == COMPLEX_BRACKET)
    {
      const re_charset_t *cset = node->opr.mbcset;
# ifdef _LIBC
      const unsigned char *pin = ((char *) re_string_get_buffer (input)
				  + str_idx);
# endif /* _LIBC */
      int match_len = 0;
      wchar_t wc = ((cset->nranges || cset->nchar_classes || cset->nmbchars)
		    ? re_string_wchar_at (input, str_idx) : 0);

      /* match with multibyte character?  */
      for (i = 0; i < cset->nmbchars; ++i)
	if (wc == cset->mbchars[i])
	  {
	    match_len = char_len;
	    goto check_node_accept_bytes_match;
	  }
      /* match with character_class?  */
      for (i = 0; i < cset->nchar_classes; ++i)
	{
	  wctype_t wt = cset->char_classes[i];
	  if (__iswctype (wc, wt))
	    {
	      match_len = char_len;
	      goto check_node_accept_bytes_match;
	    }
	}

# ifdef _LIBC
      if (nrules != 0)
	{
	  unsigned int in_collseq = 0;
	  const int32_t *table, *indirect;
	  const unsigned char *weights, *extra;
	  const char *collseqwc;
	  int32_t idx;
	  /* This #include defines a local function!  */
#  include <locale/weight.h>

	  /* match with collating_symbol?  */
	  if (cset->ncoll_syms)
	    extra = (const unsigned char *)
	      _NL_CURRENT (LC_COLLATE, _NL_COLLATE_SYMB_EXTRAMB);
	  for (i = 0; i < cset->ncoll_syms; ++i)
	    {
	      const unsigned char *coll_sym = extra + cset->coll_syms[i];
	      /* Compare the length of input collating element and
		 the length of current collating element.  */
	      if (*coll_sym != elem_len)
		continue;
	      /* Compare each bytes.  */
	      for (j = 0; j < *coll_sym; j++)
		if (pin[j] != coll_sym[1 + j])
		  break;
	      if (j == *coll_sym)
		{
		  /* Match if every bytes is equal.  */
		  match_len = j;
		  goto check_node_accept_bytes_match;
		}
	    }

	  if (cset->nranges)
	    {
	      if (elem_len <= char_len)
		{
		  collseqwc = _NL_CURRENT (LC_COLLATE, _NL_COLLATE_COLLSEQWC);
		  in_collseq = collseq_table_lookup (collseqwc, wc);
		}
	      else
		in_collseq = find_collation_sequence_value (pin, elem_len);
	    }
	  /* match with range expression?  */
	  for (i = 0; i < cset->nranges; ++i)
	    if (cset->range_starts[i] <= in_collseq
		&& in_collseq <= cset->range_ends[i])
	      {
		match_len = elem_len;
		goto check_node_accept_bytes_match;
	      }

	  /* match with equivalence_class?  */
	  if (cset->nequiv_classes)
	    {
	      const unsigned char *cp = pin;
	      table = (const int32_t *)
		_NL_CURRENT (LC_COLLATE, _NL_COLLATE_TABLEMB);
	      weights = (const unsigned char *)
		_NL_CURRENT (LC_COLLATE, _NL_COLLATE_WEIGHTMB);
	      extra = (const unsigned char *)
		_NL_CURRENT (LC_COLLATE, _NL_COLLATE_EXTRAMB);
	      indirect = (const int32_t *)
		_NL_CURRENT (LC_COLLATE, _NL_COLLATE_INDIRECTMB);
	      idx = findidx (&cp);
	      if (idx > 0)
		for (i = 0; i < cset->nequiv_classes; ++i)
		  {
		    int32_t equiv_class_idx = cset->equiv_classes[i];
		    size_t weight_len = weights[idx];
		    if (weight_len == weights[equiv_class_idx])
		      {
			int cnt = 0;
			while (cnt <= weight_len
			       && (weights[equiv_class_idx + 1 + cnt]
				   == weights[idx + 1 + cnt]))
			  ++cnt;
			if (cnt > weight_len)
			  {
			    match_len = elem_len;
			    goto check_node_accept_bytes_match;
			  }
		      }
		  }
	    }
	}
      else
# endif /* _LIBC */
	{
	  /* match with range expression?  */
#if __GNUC__ >= 2
	  wchar_t cmp_buf[] = {L'\0', L'\0', wc, L'\0', L'\0', L'\0'};
#else
	  wchar_t cmp_buf[] = {L'\0', L'\0', L'\0', L'\0', L'\0', L'\0'};
	  cmp_buf[2] = wc;
#endif
	  for (i = 0; i < cset->nranges; ++i)
	    {
	      cmp_buf[0] = cset->range_starts[i];
	      cmp_buf[4] = cset->range_ends[i];
	      if (wcscoll (cmp_buf, cmp_buf + 2) <= 0
		  && wcscoll (cmp_buf + 2, cmp_buf + 4) <= 0)
		{
		  match_len = char_len;
		  goto check_node_accept_bytes_match;
		}
	    }
	}
    check_node_accept_bytes_match:
      if (!cset->non_match)
	return match_len;
      else
	{
	  if (match_len > 0)
	    return 0;
	  else
	    return (elem_len > char_len) ? elem_len : char_len;
	}
    }
  return 0;
}

# ifdef _LIBC
static unsigned int
find_collation_sequence_value (mbs, mbs_len)
    const unsigned char *mbs;
    size_t mbs_len;
{
  uint32_t nrules = _NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_NRULES);
  if (nrules == 0)
    {
      if (mbs_len == 1)
	{
	  /* No valid character.  Match it as a single byte character.  */
	  const unsigned char *collseq = (const unsigned char *)
	    _NL_CURRENT (LC_COLLATE, _NL_COLLATE_COLLSEQMB);
	  return collseq[mbs[0]];
	}
      return UINT_MAX;
    }
  else
    {
      int32_t idx;
      const unsigned char *extra = (const unsigned char *)
	_NL_CURRENT (LC_COLLATE, _NL_COLLATE_SYMB_EXTRAMB);

      for (idx = 0; ;)
	{
	  int mbs_cnt, found = 0;
	  int32_t elem_mbs_len;
	  /* Skip the name of collating element name.  */
	  idx = idx + extra[idx] + 1;
	  elem_mbs_len = extra[idx++];
	  if (mbs_len == elem_mbs_len)
	    {
	      for (mbs_cnt = 0; mbs_cnt < elem_mbs_len; ++mbs_cnt)
		if (extra[idx + mbs_cnt] != mbs[mbs_cnt])
		  break;
	      if (mbs_cnt == elem_mbs_len)
		/* Found the entry.  */
		found = 1;
	    }
	  /* Skip the byte sequence of the collating element.  */
	  idx += elem_mbs_len;
	  /* Adjust for the alignment.  */
	  idx = (idx + 3) & ~3;
	  /* Skip the collation sequence value.  */
	  idx += sizeof (uint32_t);
	  /* Skip the wide char sequence of the collating element.  */
	  idx = idx + sizeof (uint32_t) * (extra[idx] + 1);
	  /* If we found the entry, return the sequence value.  */
	  if (found)
	    return *(uint32_t *) (extra + idx);
	  /* Skip the collation sequence value.  */
	  idx += sizeof (uint32_t);
	}
    }
}
# endif /* _LIBC */
#endif /* RE_ENABLE_I18N */

/* Check whether the node accepts the byte which is IDX-th
   byte of the INPUT.  */

static int
check_node_accept (preg, node, mctx, idx)
    const regex_t *preg;
    const re_token_t *node;
    const re_match_context_t *mctx;
    int idx;
{
  unsigned char ch;
  if (node->constraint)
    {
      /* The node has constraints.  Check whether the current context
	 satisfies the constraints.  */
      unsigned int context = re_string_context_at (mctx->input, idx,
						   mctx->eflags,
						   preg->newline_anchor);
      if (NOT_SATISFY_NEXT_CONSTRAINT (node->constraint, context))
	return 0;
    }
  ch = re_string_byte_at (mctx->input, idx);
  if (node->type == CHARACTER)
    return node->opr.c == ch;
  else if (node->type == SIMPLE_BRACKET)
    return bitset_contain (node->opr.sbcset, ch);
  else if (node->type == OP_PERIOD)
    return !((ch == '\n' && !(preg->syntax & RE_DOT_NEWLINE))
	     || (ch == '\0' && (preg->syntax & RE_DOT_NOT_NULL)));
  else
    return 0;
}

/* Extend the buffers, if the buffers have run out.  */

static reg_errcode_t
extend_buffers (mctx)
     re_match_context_t *mctx;
{
  reg_errcode_t ret;
  re_string_t *pstr = mctx->input;

  /* Double the lengthes of the buffers.  */
  ret = re_string_realloc_buffers (pstr, pstr->bufs_len * 2);
  if (BE (ret != REG_NOERROR, 0))
    return ret;

  if (mctx->state_log != NULL)
    {
      /* And double the length of state_log.  */
      re_dfastate_t **new_array;
      new_array = re_realloc (mctx->state_log, re_dfastate_t *,
			      pstr->bufs_len * 2);
      if (BE (new_array == NULL, 0))
	return REG_ESPACE;
      mctx->state_log = new_array;
    }

  /* Then reconstruct the buffers.  */
  if (pstr->icase)
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
	  if (pstr->trans != NULL)
	    re_string_translate_buffer (pstr);
	  else
	    pstr->valid_len = pstr->bufs_len;
	}
    }
  return REG_NOERROR;
}


/* Functions for matching context.  */

/* Initialize MCTX.  */

static reg_errcode_t
match_ctx_init (mctx, eflags, input, n)
    re_match_context_t *mctx;
    int eflags, n;
    re_string_t *input;
{
  mctx->eflags = eflags;
  mctx->input = input;
  mctx->match_last = -1;
  if (n > 0)
    {
      mctx->bkref_ents = re_malloc (struct re_backref_cache_entry, n);
      mctx->sub_tops = re_malloc (re_sub_match_top_t *, n);
      if (BE (mctx->bkref_ents == NULL || mctx->sub_tops == NULL, 0))
	return REG_ESPACE;
    }
  else
    mctx->bkref_ents = NULL;
  mctx->nbkref_ents = 0;
  mctx->abkref_ents = n;
  mctx->max_mb_elem_len = 1;
  mctx->nsub_tops = 0;
  mctx->asub_tops = n;
  return REG_NOERROR;
}

/* Clean the entries which depend on the current input in MCTX.
   This function must be invoked when the matcher changes the start index
   of the input, or changes the input string.  */

static void
match_ctx_clean (mctx)
    re_match_context_t *mctx;
{
  match_ctx_free_subtops (mctx);
  mctx->nsub_tops = 0;
  mctx->nbkref_ents = 0;
}

/* Free all the memory associated with MCTX.  */

static void
match_ctx_free (mctx)
    re_match_context_t *mctx;
{
  match_ctx_free_subtops (mctx);
  re_free (mctx->sub_tops);
  re_free (mctx->bkref_ents);
}

/* Free all the memory associated with MCTX->SUB_TOPS.  */

static void
match_ctx_free_subtops (mctx)
     re_match_context_t *mctx;
{
  int st_idx;
  for (st_idx = 0; st_idx < mctx->nsub_tops; ++st_idx)
    {
      int sl_idx;
      re_sub_match_top_t *top = mctx->sub_tops[st_idx];
      for (sl_idx = 0; sl_idx < top->nlasts; ++sl_idx)
	{
	  re_sub_match_last_t *last = top->lasts[sl_idx];
	  re_free (last->path.array);
	  re_free (last);
	}
      re_free (top->lasts);
      if (top->path)
	{
	  re_free (top->path->array);
	  re_free (top->path);
	}
      free (top);
    }
}

/* Add a new backreference entry to MCTX.
   Note that we assume that caller never call this function with duplicate
   entry, and call with STR_IDX which isn't smaller than any existing entry.
*/

static reg_errcode_t
match_ctx_add_entry (mctx, node, str_idx, from, to)
     re_match_context_t *mctx;
     int node, str_idx, from, to;
{
  if (mctx->nbkref_ents >= mctx->abkref_ents)
    {
      struct re_backref_cache_entry* new_entry;
      new_entry = re_realloc (mctx->bkref_ents, struct re_backref_cache_entry,
			      mctx->abkref_ents * 2);
      if (BE (new_entry == NULL, 0))
	{
	  re_free (mctx->bkref_ents);
	  return REG_ESPACE;
	}
      mctx->bkref_ents = new_entry;
      memset (mctx->bkref_ents + mctx->nbkref_ents, '\0',
	      sizeof (struct re_backref_cache_entry) * mctx->abkref_ents);
      mctx->abkref_ents *= 2;
    }
  mctx->bkref_ents[mctx->nbkref_ents].node = node;
  mctx->bkref_ents[mctx->nbkref_ents].str_idx = str_idx;
  mctx->bkref_ents[mctx->nbkref_ents].subexp_from = from;
  mctx->bkref_ents[mctx->nbkref_ents].subexp_to = to;
  mctx->bkref_ents[mctx->nbkref_ents++].flag = 0;
  if (mctx->max_mb_elem_len < to - from)
    mctx->max_mb_elem_len = to - from;
  return REG_NOERROR;
}

/* Search for the first entry which has the same str_idx.
   Note that MCTX->BKREF_ENTS is already sorted by MCTX->STR_IDX.  */

static int
search_cur_bkref_entry (mctx, str_idx)
     re_match_context_t *mctx;
     int str_idx;
{
  int left, right, mid;
  right = mctx->nbkref_ents;
  for (left = 0; left < right;)
    {
      mid = (left + right) / 2;
      if (mctx->bkref_ents[mid].str_idx < str_idx)
	left = mid + 1;
      else
	right = mid;
    }
  return left;
}

static void
match_ctx_clear_flag (mctx)
     re_match_context_t *mctx;
{
  int i;
  for (i = 0; i < mctx->nbkref_ents; ++i)
    {
      mctx->bkref_ents[i].flag = 0;
    }
}

/* Register the node NODE, whose type is OP_OPEN_SUBEXP, and which matches
   at STR_IDX.  */

static reg_errcode_t
match_ctx_add_subtop (mctx, node, str_idx)
     re_match_context_t *mctx;
     int node, str_idx;
{
#ifdef DEBUG
  assert (mctx->sub_tops != NULL);
  assert (mctx->asub_tops > 0);
#endif
  if (mctx->nsub_tops == mctx->asub_tops)
    {
      re_sub_match_top_t **new_array;
      mctx->asub_tops *= 2;
      new_array = re_realloc (mctx->sub_tops, re_sub_match_top_t *,
			      mctx->asub_tops);
      if (BE (new_array == NULL, 0))
	return REG_ESPACE;
      mctx->sub_tops = new_array;
    }
  mctx->sub_tops[mctx->nsub_tops] = calloc (1, sizeof (re_sub_match_top_t));
  if (mctx->sub_tops[mctx->nsub_tops] == NULL)
    return REG_ESPACE;
  mctx->sub_tops[mctx->nsub_tops]->node = node;
  mctx->sub_tops[mctx->nsub_tops++]->str_idx = str_idx;
  return REG_NOERROR;
}

/* Register the node NODE, whose type is OP_CLOSE_SUBEXP, and which matches
   at STR_IDX, whose corresponding OP_OPEN_SUBEXP is SUB_TOP.  */

static re_sub_match_last_t *
match_ctx_add_sublast (subtop, node, str_idx)
     re_sub_match_top_t *subtop;
     int node, str_idx;
{
  re_sub_match_last_t *new_entry;
  if (subtop->nlasts == subtop->alasts)
    {
      re_sub_match_last_t **new_array;
      subtop->alasts = 2 * subtop->alasts + 1;
      new_array = re_realloc (subtop->lasts, re_sub_match_last_t *,
			      subtop->alasts);
      if (BE (new_array == NULL, 0))
	return NULL;
      subtop->lasts = new_array;
    }
  new_entry = calloc (1, sizeof (re_sub_match_last_t));
  if (BE (new_entry == NULL, 0))
    return NULL;
  subtop->lasts[subtop->nlasts] = new_entry;
  new_entry->node = node;
  new_entry->str_idx = str_idx;
  ++subtop->nlasts;
  return new_entry;
}

static void
sift_ctx_init (sctx, sifted_sts, limited_sts, last_node, last_str_idx,
	       check_subexp)
    re_sift_context_t *sctx;
    re_dfastate_t **sifted_sts, **limited_sts;
    int last_node, last_str_idx, check_subexp;
{
  sctx->sifted_states = sifted_sts;
  sctx->limited_states = limited_sts;
  sctx->last_node = last_node;
  sctx->last_str_idx = last_str_idx;
  sctx->check_subexp = check_subexp;
  sctx->cur_bkref = -1;
  sctx->cls_subexp_idx = -1;
  re_node_set_init_empty (&sctx->limits);
}
