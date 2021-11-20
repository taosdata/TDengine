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
#pragma warning( disable : 4018 )

static reg_errcode_t re_compile_internal (regex_t *preg, const char * pattern,
					  int length, reg_syntax_t syntax);
static void re_compile_fastmap_iter (regex_t *bufp,
				     const re_dfastate_t *init_state,
				     char *fastmap);
static reg_errcode_t init_dfa (re_dfa_t *dfa, int pat_len);
static reg_errcode_t init_word_char (re_dfa_t *dfa);
#ifdef RE_ENABLE_I18N
static void free_charset (re_charset_t *cset);
#endif /* RE_ENABLE_I18N */
static void free_workarea_compile (regex_t *preg);
static reg_errcode_t create_initial_state (re_dfa_t *dfa);
static reg_errcode_t analyze (re_dfa_t *dfa);
static reg_errcode_t analyze_tree (re_dfa_t *dfa, bin_tree_t *node);
static void calc_first (re_dfa_t *dfa, bin_tree_t *node);
static void calc_next (re_dfa_t *dfa, bin_tree_t *node);
static void calc_epsdest (re_dfa_t *dfa, bin_tree_t *node);
static reg_errcode_t duplicate_node_closure (re_dfa_t *dfa, int top_org_node,
					     int top_clone_node, int root_node,
					     unsigned int constraint);
static reg_errcode_t duplicate_node (int *new_idx, re_dfa_t *dfa, int org_idx,
				     unsigned int constraint);
static int search_duplicated_node (re_dfa_t *dfa, int org_node,
				   unsigned int constraint);
static reg_errcode_t calc_eclosure (re_dfa_t *dfa);
static reg_errcode_t calc_eclosure_iter (re_node_set *new_set, re_dfa_t *dfa,
					 int node, int root);
static void calc_inveclosure (re_dfa_t *dfa);
static int fetch_number (re_string_t *input, re_token_t *token,
			 reg_syntax_t syntax);
static re_token_t fetch_token (re_string_t *input, reg_syntax_t syntax);
static int peek_token (re_token_t *token, re_string_t *input,
			reg_syntax_t syntax);
static int peek_token_bracket (re_token_t *token, re_string_t *input,
			       reg_syntax_t syntax);
static bin_tree_t *parse (re_string_t *regexp, regex_t *preg,
			  reg_syntax_t syntax, reg_errcode_t *err);
static bin_tree_t *parse_reg_exp (re_string_t *regexp, regex_t *preg,
				  re_token_t *token, reg_syntax_t syntax,
				  int nest, reg_errcode_t *err);
static bin_tree_t *parse_branch (re_string_t *regexp, regex_t *preg,
				 re_token_t *token, reg_syntax_t syntax,
				 int nest, reg_errcode_t *err);
static bin_tree_t *parse_expression (re_string_t *regexp, regex_t *preg,
				     re_token_t *token, reg_syntax_t syntax,
				     int nest, reg_errcode_t *err);
static bin_tree_t *parse_sub_exp (re_string_t *regexp, regex_t *preg,
				  re_token_t *token, reg_syntax_t syntax,
				  int nest, reg_errcode_t *err);
static bin_tree_t *parse_dup_op (bin_tree_t *dup_elem, re_string_t *regexp,
				 re_dfa_t *dfa, re_token_t *token,
				 reg_syntax_t syntax, reg_errcode_t *err);
static bin_tree_t *parse_bracket_exp (re_string_t *regexp, re_dfa_t *dfa,
				      re_token_t *token, reg_syntax_t syntax,
				      reg_errcode_t *err);
static reg_errcode_t parse_bracket_element (bracket_elem_t *elem,
					    re_string_t *regexp,
					    re_token_t *token, int token_len,
					    re_dfa_t *dfa,
					    reg_syntax_t syntax);
static reg_errcode_t parse_bracket_symbol (bracket_elem_t *elem,
					  re_string_t *regexp,
					  re_token_t *token);
#ifndef _LIBC
# ifdef RE_ENABLE_I18N
static reg_errcode_t build_range_exp (re_bitset_ptr_t sbcset,
				      re_charset_t *mbcset, int *range_alloc,
				      bracket_elem_t *start_elem,
				      bracket_elem_t *end_elem);
static reg_errcode_t build_collating_symbol (re_bitset_ptr_t sbcset,
					     re_charset_t *mbcset,
					     int *coll_sym_alloc,
					     const unsigned char *name);
# else /* not RE_ENABLE_I18N */
static reg_errcode_t build_range_exp (re_bitset_ptr_t sbcset,
				      bracket_elem_t *start_elem,
				      bracket_elem_t *end_elem);
static reg_errcode_t build_collating_symbol (re_bitset_ptr_t sbcset,
					     const unsigned char *name);
# endif /* not RE_ENABLE_I18N */
#endif /* not _LIBC */
#ifdef RE_ENABLE_I18N
static reg_errcode_t build_equiv_class (re_bitset_ptr_t sbcset,
					re_charset_t *mbcset,
					int *equiv_class_alloc,
					const unsigned char *name);
static reg_errcode_t build_charclass (re_bitset_ptr_t sbcset,
				      re_charset_t *mbcset,
				      int *char_class_alloc,
				      const unsigned char *class_name,
				      reg_syntax_t syntax);
#else  /* not RE_ENABLE_I18N */
static reg_errcode_t build_equiv_class (re_bitset_ptr_t sbcset,
					const unsigned char *name);
static reg_errcode_t build_charclass (re_bitset_ptr_t sbcset,
				      const unsigned char *class_name,
				      reg_syntax_t syntax);
#endif /* not RE_ENABLE_I18N */
static bin_tree_t *build_word_op (re_dfa_t *dfa, int not, reg_errcode_t *err);
static void free_bin_tree (bin_tree_t *tree);
static bin_tree_t *create_tree (bin_tree_t *left, bin_tree_t *right,
				re_token_type_t type, int deps_index);
static bin_tree_t *duplicate_tree (const bin_tree_t *src, re_dfa_t *dfa);

/* This table gives an error message for each of the error codes listed
   in regex.h.  Obviously the order here has to be same as there.
   POSIX doesn't require that we do anything for REG_NOERROR,
   but why not be nice?  */

const char __re_error_msgid[] attribute_hidden =
  {
#define REG_NOERROR_IDX	0
    gettext_noop ("Success")	/* REG_NOERROR */
    "\0"
#define REG_NOMATCH_IDX (REG_NOERROR_IDX + sizeof "Success")
    gettext_noop ("No match")	/* REG_NOMATCH */
    "\0"
#define REG_BADPAT_IDX	(REG_NOMATCH_IDX + sizeof "No match")
    gettext_noop ("Invalid regular expression") /* REG_BADPAT */
    "\0"
#define REG_ECOLLATE_IDX (REG_BADPAT_IDX + sizeof "Invalid regular expression")
    gettext_noop ("Invalid collation character") /* REG_ECOLLATE */
    "\0"
#define REG_ECTYPE_IDX	(REG_ECOLLATE_IDX + sizeof "Invalid collation character")
    gettext_noop ("Invalid character class name") /* REG_ECTYPE */
    "\0"
#define REG_EESCAPE_IDX	(REG_ECTYPE_IDX + sizeof "Invalid character class name")
    gettext_noop ("Trailing backslash") /* REG_EESCAPE */
    "\0"
#define REG_ESUBREG_IDX	(REG_EESCAPE_IDX + sizeof "Trailing backslash")
    gettext_noop ("Invalid back reference") /* REG_ESUBREG */
    "\0"
#define REG_EBRACK_IDX	(REG_ESUBREG_IDX + sizeof "Invalid back reference")
    gettext_noop ("Unmatched [ or [^")	/* REG_EBRACK */
    "\0"
#define REG_EPAREN_IDX	(REG_EBRACK_IDX + sizeof "Unmatched [ or [^")
    gettext_noop ("Unmatched ( or \\(") /* REG_EPAREN */
    "\0"
#define REG_EBRACE_IDX	(REG_EPAREN_IDX + sizeof "Unmatched ( or \\(")
    gettext_noop ("Unmatched \\{") /* REG_EBRACE */
    "\0"
#define REG_BADBR_IDX	(REG_EBRACE_IDX + sizeof "Unmatched \\{")
    gettext_noop ("Invalid content of \\{\\}") /* REG_BADBR */
    "\0"
#define REG_ERANGE_IDX	(REG_BADBR_IDX + sizeof "Invalid content of \\{\\}")
    gettext_noop ("Invalid range end")	/* REG_ERANGE */
    "\0"
#define REG_ESPACE_IDX	(REG_ERANGE_IDX + sizeof "Invalid range end")
    gettext_noop ("Memory exhausted") /* REG_ESPACE */
    "\0"
#define REG_BADRPT_IDX	(REG_ESPACE_IDX + sizeof "Memory exhausted")
    gettext_noop ("Invalid preceding regular expression") /* REG_BADRPT */
    "\0"
#define REG_EEND_IDX	(REG_BADRPT_IDX + sizeof "Invalid preceding regular expression")
    gettext_noop ("Premature end of regular expression") /* REG_EEND */
    "\0"
#define REG_ESIZE_IDX	(REG_EEND_IDX + sizeof "Premature end of regular expression")
    gettext_noop ("Regular expression too big") /* REG_ESIZE */
    "\0"
#define REG_ERPAREN_IDX	(REG_ESIZE_IDX + sizeof "Regular expression too big")
    gettext_noop ("Unmatched ) or \\)") /* REG_ERPAREN */
  };

const size_t __re_error_msgid_idx[] attribute_hidden =
  {
    REG_NOERROR_IDX,
    REG_NOMATCH_IDX,
    REG_BADPAT_IDX,
    REG_ECOLLATE_IDX,
    REG_ECTYPE_IDX,
    REG_EESCAPE_IDX,
    REG_ESUBREG_IDX,
    REG_EBRACK_IDX,
    REG_EPAREN_IDX,
    REG_EBRACE_IDX,
    REG_BADBR_IDX,
    REG_ERANGE_IDX,
    REG_ESPACE_IDX,
    REG_BADRPT_IDX,
    REG_EEND_IDX,
    REG_ESIZE_IDX,
    REG_ERPAREN_IDX
  };

/* Entry points for GNU code.  */

/* re_compile_pattern is the GNU regular expression compiler: it
   compiles PATTERN (of length LENGTH) and puts the result in BUFP.
   Returns 0 if the pattern was valid, otherwise an error string.

   Assumes the `allocated' (and perhaps `buffer') and `translate' fields
   are set in BUFP on entry.  */

const char *
re_compile_pattern (pattern, length, bufp)
    const char *pattern;
    size_t length;
    struct re_pattern_buffer *bufp;
{
  reg_errcode_t ret;

  /* And GNU code determines whether or not to get register information
     by passing null for the REGS argument to re_match, etc., not by
     setting no_sub.  */
  bufp->no_sub = 0;

  /* Match anchors at newline.  */
  bufp->newline_anchor = 1;

  ret = re_compile_internal (bufp, pattern, (int)length, re_syntax_options);

  if (!ret)
    return NULL;
  return gettext (__re_error_msgid + __re_error_msgid_idx[(int) ret]);
}
#ifdef _LIBC
weak_alias (__re_compile_pattern, re_compile_pattern)
#endif

/* Set by `re_set_syntax' to the current regexp syntax to recognize.  Can
   also be assigned to arbitrarily: each pattern buffer stores its own
   syntax, so it can be changed between regex compilations.  */
/* This has no initializer because initialized variables in Emacs
   become read-only after dumping.  */
reg_syntax_t re_syntax_options;


/* Specify the precise syntax of regexps for compilation.  This provides
   for compatibility for various utilities which historically have
   different, incompatible syntaxes.

   The argument SYNTAX is a bit mask comprised of the various bits
   defined in regex.h.  We return the old syntax.  */

reg_syntax_t
re_set_syntax (syntax)
    reg_syntax_t syntax;
{
  reg_syntax_t ret = re_syntax_options;

  re_syntax_options = syntax;
  return ret;
}
#ifdef _LIBC
weak_alias (__re_set_syntax, re_set_syntax)
#endif

int
re_compile_fastmap (bufp)
    struct re_pattern_buffer *bufp;
{
  re_dfa_t *dfa = (re_dfa_t *) bufp->buffer;
  char *fastmap = bufp->fastmap;

  memset (fastmap, '\0', sizeof (char) * SBC_MAX);
  re_compile_fastmap_iter (bufp, dfa->init_state, fastmap);
  if (dfa->init_state != dfa->init_state_word)
    re_compile_fastmap_iter (bufp, dfa->init_state_word, fastmap);
  if (dfa->init_state != dfa->init_state_nl)
    re_compile_fastmap_iter (bufp, dfa->init_state_nl, fastmap);
  if (dfa->init_state != dfa->init_state_begbuf)
    re_compile_fastmap_iter (bufp, dfa->init_state_begbuf, fastmap);
  bufp->fastmap_accurate = 1;
  return 0;
}
#ifdef _LIBC
weak_alias (__re_compile_fastmap, re_compile_fastmap)
#endif

static inline void
re_set_fastmap (char *fastmap, int icase, int ch)
{
  fastmap[ch] = 1;
  if (icase)
    fastmap[tolower (ch)] = 1;
}

/* Helper function for re_compile_fastmap.
   Compile fastmap for the initial_state INIT_STATE.  */

static void
re_compile_fastmap_iter (bufp, init_state, fastmap)
     regex_t *bufp;
     const re_dfastate_t *init_state;
     char *fastmap;
{
  re_dfa_t *dfa = (re_dfa_t *) bufp->buffer;
  int node_cnt;
  int icase = (MB_CUR_MAX == 1 && (bufp->syntax & RE_ICASE));
  for (node_cnt = 0; node_cnt < init_state->nodes.nelem; ++node_cnt)
    {
      int node = init_state->nodes.elems[node_cnt];
      re_token_type_t type = dfa->nodes[node].type;

      if (type == CHARACTER)
	re_set_fastmap (fastmap, icase, dfa->nodes[node].opr.c);
      else if (type == SIMPLE_BRACKET)
	{
	  int i, j, ch;
	  for (i = 0, ch = 0; i < BITSET_UINTS; ++i)
	    for (j = 0; j < UINT_BITS; ++j, ++ch)
	      if (dfa->nodes[node].opr.sbcset[i] & (1 << j))
		re_set_fastmap (fastmap, icase, ch);
	}
#ifdef RE_ENABLE_I18N
      else if (type == COMPLEX_BRACKET)
	{
	  int i;
	  re_charset_t *cset = dfa->nodes[node].opr.mbcset;
	  if (cset->non_match || cset->ncoll_syms || cset->nequiv_classes
	      || cset->nranges || cset->nchar_classes)
	    {
# ifdef _LIBC
	      if (_NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_NRULES) != 0)
		{
		  /* In this case we want to catch the bytes which are
		     the first byte of any collation elements.
		     e.g. In da_DK, we want to catch 'a' since "aa"
			  is a valid collation element, and don't catch
			  'b' since 'b' is the only collation element
			  which starts from 'b'.  */
		  int j, ch;
		  const int32_t *table = (const int32_t *)
		    _NL_CURRENT (LC_COLLATE, _NL_COLLATE_TABLEMB);
		  for (i = 0, ch = 0; i < BITSET_UINTS; ++i)
		    for (j = 0; j < UINT_BITS; ++j, ++ch)
		      if (table[ch] < 0)
			re_set_fastmap (fastmap, icase, ch);
		}
# else
	      if (MB_CUR_MAX > 1)
		for (i = 0; i < SBC_MAX; ++i)
		  if (__btowc (i) == WEOF)
		    re_set_fastmap (fastmap, icase, i);
# endif /* not _LIBC */
	    }
	  for (i = 0; i < cset->nmbchars; ++i)
	    {
	      char buf[256];
	      mbstate_t state;
	      memset (&state, '\0', sizeof (state));
	      __wcrtomb (buf, cset->mbchars[i], &state);
	      re_set_fastmap (fastmap, icase, *(unsigned char *) buf);
	    }
	}
#endif /* RE_ENABLE_I18N */
      else if (type == END_OF_RE || type == OP_PERIOD)
	{
	  memset (fastmap, '\1', sizeof (char) * SBC_MAX);
	  if (type == END_OF_RE)
	    bufp->can_be_null = 1;
	  return;
	}
    }
}

/* Entry point for POSIX code.  */
/* regcomp takes a regular expression as a string and compiles it.

   PREG is a regex_t *.  We do not expect any fields to be initialized,
   since POSIX says we shouldn't.  Thus, we set

     `buffer' to the compiled pattern;
     `used' to the length of the compiled pattern;
     `syntax' to RE_SYNTAX_POSIX_EXTENDED if the
       REG_EXTENDED bit in CFLAGS is set; otherwise, to
       RE_SYNTAX_POSIX_BASIC;
     `newline_anchor' to REG_NEWLINE being set in CFLAGS;
     `fastmap' to an allocated space for the fastmap;
     `fastmap_accurate' to zero;
     `re_nsub' to the number of subexpressions in PATTERN.

   PATTERN is the address of the pattern string.

   CFLAGS is a series of bits which affect compilation.

     If REG_EXTENDED is set, we use POSIX extended syntax; otherwise, we
     use POSIX basic syntax.

     If REG_NEWLINE is set, then . and [^...] don't match newline.
     Also, regexec will try a match beginning after every newline.

     If REG_ICASE is set, then we considers upper- and lowercase
     versions of letters to be equivalent when matching.

     If REG_NOSUB is set, then when PREG is passed to regexec, that
     routine will report only success or failure, and nothing about the
     registers.

   It returns 0 if it succeeds, nonzero if it doesn't.  (See regex.h for
   the return codes and their meanings.)  */

int
regcomp (preg, pattern, cflags)
    regex_t *__restrict preg;
    const char *__restrict pattern;
    int cflags;
{
  reg_errcode_t ret;
  reg_syntax_t syntax = ((cflags & REG_EXTENDED) ? RE_SYNTAX_POSIX_EXTENDED
			 : RE_SYNTAX_POSIX_BASIC);

  preg->buffer = NULL;
  preg->allocated = 0;
  preg->used = 0;

  /* Try to allocate space for the fastmap.  */
  preg->fastmap = re_malloc (char, SBC_MAX);
  if (BE (preg->fastmap == NULL, 0))
    return REG_ESPACE;

  syntax |= (cflags & REG_ICASE) ? RE_ICASE : 0;

  /* If REG_NEWLINE is set, newlines are treated differently.  */
  if (cflags & REG_NEWLINE)
    { /* REG_NEWLINE implies neither . nor [^...] match newline.  */
      syntax &= ~RE_DOT_NEWLINE;
      syntax |= RE_HAT_LISTS_NOT_NEWLINE;
      /* It also changes the matching behavior.  */
      preg->newline_anchor = 1;
    }
  else
    preg->newline_anchor = 0;
  preg->no_sub = !!(cflags & REG_NOSUB);
  preg->translate = NULL;

  ret = re_compile_internal(preg, pattern, (int)strlen(pattern), syntax);

  /* POSIX doesn't distinguish between an unmatched open-group and an
     unmatched close-group: both are REG_EPAREN.  */
  if (ret == REG_ERPAREN)
    ret = REG_EPAREN;

  /* We have already checked preg->fastmap != NULL.  */
  if (BE (ret == REG_NOERROR, 1))
    /* Compute the fastmap now, since regexec cannot modify the pattern
       buffer.  This function nevers fails in this implementation.  */
    (void) re_compile_fastmap (preg);
  else
    {
      /* Some error occurred while compiling the expression.  */
      re_free (preg->fastmap);
      preg->fastmap = NULL;
    }

  return (int) ret;
}
#ifdef _LIBC
weak_alias (__regcomp, regcomp)
#endif

/* Returns a message corresponding to an error code, ERRCODE, returned
   from either regcomp or regexec.   We don't use PREG here.  */

size_t
regerror (errcode, preg, errbuf, errbuf_size)
    int errcode;
    const regex_t *preg;
    char *errbuf;
    size_t errbuf_size;
{
  const char *msg;
  size_t msg_size;

  if (BE (errcode < 0
	  || errcode >= (int) (sizeof (__re_error_msgid_idx)
			       / sizeof (__re_error_msgid_idx[0])), 0))
    /* Only error codes returned by the rest of the code should be passed
       to this routine.  If we are given anything else, or if other regex
       code generates an invalid error code, then the program has a bug.
       Dump core so we can fix it.  */
    abort ();

  msg = gettext (__re_error_msgid + __re_error_msgid_idx[errcode]);

  msg_size = strlen (msg) + 1; /* Includes the null.  */

  if (BE (errbuf_size != 0, 1))
    {
      if (BE (msg_size > errbuf_size, 0))
	{
#if defined HAVE_MEMPCPY || defined _LIBC
	  *((char *) __mempcpy (errbuf, msg, errbuf_size - 1)) = '\0';
#else
	  memcpy (errbuf, msg, errbuf_size - 1);
	  errbuf[errbuf_size - 1] = 0;
#endif
	}
      else
	memcpy (errbuf, msg, msg_size);
    }

  return msg_size;
}
#ifdef _LIBC
weak_alias (__regerror, regerror)
#endif


static void
free_dfa_content (re_dfa_t *dfa)
{
  int i, j;

  re_free (dfa->subexps);

  for (i = 0; i < dfa->nodes_len; ++i)
    {
      re_token_t *node = dfa->nodes + i;
#ifdef RE_ENABLE_I18N
      if (node->type == COMPLEX_BRACKET && node->duplicated == 0)
	free_charset (node->opr.mbcset);
      else
#endif /* RE_ENABLE_I18N */
	if (node->type == SIMPLE_BRACKET && node->duplicated == 0)
	  re_free (node->opr.sbcset);
    }
  re_free (dfa->nexts);
  for (i = 0; i < dfa->nodes_len; ++i)
    {
      if (dfa->eclosures != NULL)
	re_node_set_free (dfa->eclosures + i);
      if (dfa->inveclosures != NULL)
	re_node_set_free (dfa->inveclosures + i);
      if (dfa->edests != NULL)
	re_node_set_free (dfa->edests + i);
    }
  re_free (dfa->edests);
  re_free (dfa->eclosures);
  re_free (dfa->inveclosures);
  re_free (dfa->nodes);

  for (i = 0; i <= dfa->state_hash_mask; ++i)
    {
      struct re_state_table_entry *entry = dfa->state_table + i;
      for (j = 0; j < entry->num; ++j)
	{
	  re_dfastate_t *state = entry->array[j];
	  free_state (state);
	}
      re_free (entry->array);
    }
  re_free (dfa->state_table);

  if (dfa->word_char != NULL)
    re_free (dfa->word_char);
#ifdef DEBUG
  re_free (dfa->re_str);
#endif

  re_free (dfa);
}


/* Free dynamically allocated space used by PREG.  */

void
regfree (preg)
    regex_t *preg;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  if (BE (dfa != NULL, 1))
    free_dfa_content (dfa);

  re_free (preg->fastmap);
}
#ifdef _LIBC
weak_alias (__regfree, regfree)
#endif

/* Entry points compatible with 4.2 BSD regex library.  We don't define
   them unless specifically requested.  */

#if defined _REGEX_RE_COMP || defined _LIBC

/* BSD has one and only one pattern buffer.  */
static struct re_pattern_buffer re_comp_buf;

char *
# ifdef _LIBC
/* Make these definitions weak in libc, so POSIX programs can redefine
   these names if they don't use our functions, and still use
   regcomp/regexec above without link errors.  */
weak_function
# endif
re_comp (s)
     const char *s;
{
  reg_errcode_t ret;
  char *fastmap;

  if (!s)
    {
      if (!re_comp_buf.buffer)
	return gettext ("No previous regular expression");
      return 0;
    }

  if (re_comp_buf.buffer)
    {
      fastmap = re_comp_buf.fastmap;
      re_comp_buf.fastmap = NULL;
      __regfree (&re_comp_buf);
      memset (&re_comp_buf, '\0', sizeof (re_comp_buf));
      re_comp_buf.fastmap = fastmap;
    }

  if (re_comp_buf.fastmap == NULL)
    {
      re_comp_buf.fastmap = (char *) malloc (SBC_MAX);
      if (re_comp_buf.fastmap == NULL)
	return (char *) gettext (__re_error_msgid
				 + __re_error_msgid_idx[(int) REG_ESPACE]);
    }

  /* Since `re_exec' always passes NULL for the `regs' argument, we
     don't need to initialize the pattern buffer fields which affect it.  */

  /* Match anchors at newlines.  */
  re_comp_buf.newline_anchor = 1;

  ret = re_compile_internal (&re_comp_buf, s, strlen (s), re_syntax_options);

  if (!ret)
    return NULL;

  /* Yes, we're discarding `const' here if !HAVE_LIBINTL.  */
  return (char *) gettext (__re_error_msgid + __re_error_msgid_idx[(int) ret]);
}

#ifdef _LIBC
libc_freeres_fn (free_mem)
{
  __regfree (&re_comp_buf);
}
#endif

#endif /* _REGEX_RE_COMP */

/* Internal entry point.
   Compile the regular expression PATTERN, whose length is LENGTH.
   SYNTAX indicate regular expression's syntax.  */

static reg_errcode_t
re_compile_internal (preg, pattern, length, syntax)
     regex_t *preg;
     const char * pattern;
     int length;
     reg_syntax_t syntax;
{
  reg_errcode_t err = REG_NOERROR;
  re_dfa_t *dfa;
  re_string_t regexp;

  /* Initialize the pattern buffer.  */
  preg->fastmap_accurate = 0;
  preg->syntax = syntax;
  preg->not_bol = preg->not_eol = 0;
  preg->used = 0;
  preg->re_nsub = 0;
  preg->can_be_null = 0;
  preg->regs_allocated = REGS_UNALLOCATED;

  /* Initialize the dfa.  */
  dfa = (re_dfa_t *) preg->buffer;
  if (preg->allocated < sizeof (re_dfa_t))
    {
      /* If zero allocated, but buffer is non-null, try to realloc
	 enough space.  This loses if buffer's address is bogus, but
	 that is the user's responsibility.  If ->buffer is NULL this
	 is a simple allocation.  */
      dfa = re_realloc (preg->buffer, re_dfa_t, 1);
      if (dfa == NULL)
	return REG_ESPACE;
      preg->allocated = sizeof (re_dfa_t);
    }
  preg->buffer = (unsigned char *) dfa;
  preg->used = sizeof (re_dfa_t);

  err = init_dfa (dfa, length);
  if (BE (err != REG_NOERROR, 0))
    {
      re_free (dfa);
      preg->buffer = NULL;
      preg->allocated = 0;
      return err;
    }
#ifdef DEBUG
  dfa->re_str = re_malloc (char, length + 1);
  strncpy (dfa->re_str, pattern, length + 1);
#endif

  err = re_string_construct (&regexp, pattern, length, preg->translate,
			     syntax & RE_ICASE);
  if (BE (err != REG_NOERROR, 0))
    {
      re_free (dfa);
      preg->buffer = NULL;
      preg->allocated = 0;
      return err;
    }

  /* Parse the regular expression, and build a structure tree.  */
  preg->re_nsub = 0;
  dfa->str_tree = parse (&regexp, preg, syntax, &err);
  if (BE (dfa->str_tree == NULL, 0))
    goto re_compile_internal_free_return;

  /* Analyze the tree and collect information which is necessary to
     create the dfa.  */
  err = analyze (dfa);
  if (BE (err != REG_NOERROR, 0))
    goto re_compile_internal_free_return;

  /* Then create the initial state of the dfa.  */
  err = create_initial_state (dfa);

  /* Release work areas.  */
  free_workarea_compile (preg);
  re_string_destruct (&regexp);

  if (BE (err != REG_NOERROR, 0))
    {
    re_compile_internal_free_return:
      free_dfa_content (dfa);
      preg->buffer = NULL;
      preg->allocated = 0;
    }

  return err;
}

/* Initialize DFA.  We use the length of the regular expression PAT_LEN
   as the initial length of some arrays.  */

static reg_errcode_t
init_dfa (dfa, pat_len)
     re_dfa_t *dfa;
     int pat_len;
{
  int table_size;

  memset (dfa, '\0', sizeof (re_dfa_t));

  dfa->nodes_alloc = pat_len + 1;
  dfa->nodes = re_malloc (re_token_t, dfa->nodes_alloc);

  dfa->states_alloc = pat_len + 1;

  /*  table_size = 2 ^ ceil(log pat_len) */
  for (table_size = 1; table_size > 0; table_size <<= 1)
    if (table_size > pat_len)
      break;

  dfa->state_table = calloc (sizeof (struct re_state_table_entry), table_size);
  dfa->state_hash_mask = table_size - 1;

  dfa->subexps_alloc = 1;
  dfa->subexps = re_malloc (re_subexp_t, dfa->subexps_alloc);
  dfa->word_char = NULL;

  if (BE (dfa->nodes == NULL || dfa->state_table == NULL
	  || dfa->subexps == NULL, 0))
    {
      /* We don't bother to free anything which was allocated.  Very
	 soon the process will go down anyway.  */
      dfa->subexps = NULL;
      dfa->state_table = NULL;
      dfa->nodes = NULL;
      return REG_ESPACE;
    }
  return REG_NOERROR;
}

/* Initialize WORD_CHAR table, which indicate which character is
   "word".  In this case "word" means that it is the word construction
   character used by some operators like "\<", "\>", etc.  */

static reg_errcode_t
init_word_char (dfa)
     re_dfa_t *dfa;
{
  int i, j, ch;
  dfa->word_char = (re_bitset_ptr_t) calloc (sizeof (bitset), 1);
  if (BE (dfa->word_char == NULL, 0))
    return REG_ESPACE;
  for (i = 0, ch = 0; i < BITSET_UINTS; ++i)
    for (j = 0; j < UINT_BITS; ++j, ++ch)
      if (isalnum (ch) || ch == '_')
	dfa->word_char[i] |= 1 << j;
  return REG_NOERROR;
}

/* Free the work area which are only used while compiling.  */

static void
free_workarea_compile (preg)
     regex_t *preg;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  free_bin_tree (dfa->str_tree);
  dfa->str_tree = NULL;
  re_free (dfa->org_indices);
  dfa->org_indices = NULL;
}

/* Create initial states for all contexts.  */

static reg_errcode_t
create_initial_state (dfa)
     re_dfa_t *dfa;
{
  int first, i;
  reg_errcode_t err;
  re_node_set init_nodes;

  /* Initial states have the epsilon closure of the node which is
     the first node of the regular expression.  */
  first = dfa->str_tree->first;
  dfa->init_node = first;
  err = re_node_set_init_copy (&init_nodes, dfa->eclosures + first);
  if (BE (err != REG_NOERROR, 0))
    return err;

  /* The back-references which are in initial states can epsilon transit,
     since in this case all of the subexpressions can be null.
     Then we add epsilon closures of the nodes which are the next nodes of
     the back-references.  */
  if (dfa->nbackref > 0)
    for (i = 0; i < init_nodes.nelem; ++i)
      {
	int node_idx = init_nodes.elems[i];
	re_token_type_t type = dfa->nodes[node_idx].type;

	int clexp_idx;
	if (type != OP_BACK_REF)
	  continue;
	for (clexp_idx = 0; clexp_idx < init_nodes.nelem; ++clexp_idx)
	  {
	    re_token_t *clexp_node;
	    clexp_node = dfa->nodes + init_nodes.elems[clexp_idx];
	    if (clexp_node->type == OP_CLOSE_SUBEXP
		&& clexp_node->opr.idx + 1 == dfa->nodes[node_idx].opr.idx)
	      break;
	  }
	if (clexp_idx == init_nodes.nelem)
	  continue;

	if (type == OP_BACK_REF)
	  {
	    int dest_idx = dfa->edests[node_idx].elems[0];
	    if (!re_node_set_contains (&init_nodes, dest_idx))
	      {
		re_node_set_merge (&init_nodes, dfa->eclosures + dest_idx);
		i = 0;
	      }
	  }
      }

  /* It must be the first time to invoke acquire_state.  */
  dfa->init_state = re_acquire_state_context (&err, dfa, &init_nodes, 0);
  /* We don't check ERR here, since the initial state must not be NULL.  */
  if (BE (dfa->init_state == NULL, 0))
    return err;
  if (dfa->init_state->has_constraint)
    {
      dfa->init_state_word = re_acquire_state_context (&err, dfa, &init_nodes,
						       CONTEXT_WORD);
      dfa->init_state_nl = re_acquire_state_context (&err, dfa, &init_nodes,
						     CONTEXT_NEWLINE);
      dfa->init_state_begbuf = re_acquire_state_context (&err, dfa,
							 &init_nodes,
							 CONTEXT_NEWLINE
							 | CONTEXT_BEGBUF);
      if (BE (dfa->init_state_word == NULL || dfa->init_state_nl == NULL
	      || dfa->init_state_begbuf == NULL, 0))
	return err;
    }
  else
    dfa->init_state_word = dfa->init_state_nl
      = dfa->init_state_begbuf = dfa->init_state;

  re_node_set_free (&init_nodes);
  return REG_NOERROR;
}

/* Analyze the structure tree, and calculate "first", "next", "edest",
   "eclosure", and "inveclosure".  */

static reg_errcode_t
analyze (dfa)
     re_dfa_t *dfa;
{
  int i;
  reg_errcode_t ret;

  /* Allocate arrays.  */
  dfa->nexts = re_malloc (int, dfa->nodes_alloc);
  dfa->org_indices = re_malloc (int, dfa->nodes_alloc);
  dfa->edests = re_malloc (re_node_set, dfa->nodes_alloc);
  dfa->eclosures = re_malloc (re_node_set, dfa->nodes_alloc);
  dfa->inveclosures = re_malloc (re_node_set, dfa->nodes_alloc);
  if (BE (dfa->nexts == NULL || dfa->org_indices == NULL || dfa->edests == NULL
	  || dfa->eclosures == NULL || dfa->inveclosures == NULL, 0))
    return REG_ESPACE;
  /* Initialize them.  */
  for (i = 0; i < dfa->nodes_len; ++i)
    {
      dfa->nexts[i] = -1;
      re_node_set_init_empty (dfa->edests + i);
      re_node_set_init_empty (dfa->eclosures + i);
      re_node_set_init_empty (dfa->inveclosures + i);
    }

  ret = analyze_tree (dfa, dfa->str_tree);
  if (BE (ret == REG_NOERROR, 1))
    {
      ret = calc_eclosure (dfa);
      if (ret == REG_NOERROR)
	calc_inveclosure (dfa);
    }
  return ret;
}

/* Helper functions for analyze.
   This function calculate "first", "next", and "edest" for the subtree
   whose root is NODE.  */

static reg_errcode_t
analyze_tree (dfa, node)
     re_dfa_t *dfa;
     bin_tree_t *node;
{
  reg_errcode_t ret;
  if (node->first == -1)
    calc_first (dfa, node);
  if (node->next == -1)
    calc_next (dfa, node);
  if (node->eclosure.nelem == 0)
    calc_epsdest (dfa, node);
  /* Calculate "first" etc. for the left child.  */
  if (node->left != NULL)
    {
      ret = analyze_tree (dfa, node->left);
      if (BE (ret != REG_NOERROR, 0))
	return ret;
    }
  /* Calculate "first" etc. for the right child.  */
  if (node->right != NULL)
    {
      ret = analyze_tree (dfa, node->right);
      if (BE (ret != REG_NOERROR, 0))
	return ret;
    }
  return REG_NOERROR;
}

/* Calculate "first" for the node NODE.  */
static void
calc_first (dfa, node)
     re_dfa_t *dfa;
     bin_tree_t *node;
{
  int idx, type;
  idx = node->node_idx;
  type = (node->type == 0) ? dfa->nodes[idx].type : node->type;

  switch (type)
    {
#ifdef DEBUG
    case OP_OPEN_BRACKET:
    case OP_CLOSE_BRACKET:
    case OP_OPEN_DUP_NUM:
    case OP_CLOSE_DUP_NUM:
    case OP_NON_MATCH_LIST:
    case OP_OPEN_COLL_ELEM:
    case OP_CLOSE_COLL_ELEM:
    case OP_OPEN_EQUIV_CLASS:
    case OP_CLOSE_EQUIV_CLASS:
    case OP_OPEN_CHAR_CLASS:
    case OP_CLOSE_CHAR_CLASS:
      /* These must not be appeared here.  */
      assert (0);
#endif
    case END_OF_RE:
    case CHARACTER:
    case OP_PERIOD:
    case OP_DUP_ASTERISK:
    case OP_DUP_QUESTION:
#ifdef RE_ENABLE_I18N
    case COMPLEX_BRACKET:
#endif /* RE_ENABLE_I18N */
    case SIMPLE_BRACKET:
    case OP_BACK_REF:
    case ANCHOR:
    case OP_OPEN_SUBEXP:
    case OP_CLOSE_SUBEXP:
      node->first = idx;
      break;
    case OP_DUP_PLUS:
#ifdef DEBUG
      assert (node->left != NULL);
#endif
      if (node->left->first == -1)
	calc_first (dfa, node->left);
      node->first = node->left->first;
      break;
    case OP_ALT:
      node->first = idx;
      break;
      /* else fall through */
    default:
#ifdef DEBUG
      assert (node->left != NULL);
#endif
      if (node->left->first == -1)
	calc_first (dfa, node->left);
      node->first = node->left->first;
      break;
    }
}

/* Calculate "next" for the node NODE.  */

static void
calc_next (dfa, node)
     re_dfa_t *dfa;
     bin_tree_t *node;
{
  int idx, type;
  bin_tree_t *parent = node->parent;
  if (parent == NULL)
    {
      node->next = -1;
      idx = node->node_idx;
      if (node->type == 0)
	dfa->nexts[idx] = node->next;
      return;
    }

  idx = parent->node_idx;
  type = (parent->type == 0) ? dfa->nodes[idx].type : parent->type;

  switch (type)
    {
    case OP_DUP_ASTERISK:
    case OP_DUP_PLUS:
      node->next = idx;
      break;
    case CONCAT:
      if (parent->left == node)
	{
	  if (parent->right->first == -1)
	    calc_first (dfa, parent->right);
	  node->next = parent->right->first;
	  break;
	}
      /* else fall through */
    default:
      if (parent->next == -1)
	calc_next (dfa, parent);
      node->next = parent->next;
      break;
    }
  idx = node->node_idx;
  if (node->type == 0)
    dfa->nexts[idx] = node->next;
}

/* Calculate "edest" for the node NODE.  */

static void
calc_epsdest (dfa, node)
     re_dfa_t *dfa;
     bin_tree_t *node;
{
  int idx;
  idx = node->node_idx;
  if (node->type == 0)
    {
      if (dfa->nodes[idx].type == OP_DUP_ASTERISK
	  || dfa->nodes[idx].type == OP_DUP_PLUS
	  || dfa->nodes[idx].type == OP_DUP_QUESTION)
	{
	  if (node->left->first == -1)
	    calc_first (dfa, node->left);
	  if (node->next == -1)
	    calc_next (dfa, node);
	  re_node_set_init_2 (dfa->edests + idx, node->left->first,
			      node->next);
	}
      else if (dfa->nodes[idx].type == OP_ALT)
	{
	  int left, right;
	  if (node->left != NULL)
	    {
	      if (node->left->first == -1)
		calc_first (dfa, node->left);
	      left = node->left->first;
	    }
	  else
	    {
	      if (node->next == -1)
		calc_next (dfa, node);
	      left = node->next;
	    }
	  if (node->right != NULL)
	    {
	      if (node->right->first == -1)
		calc_first (dfa, node->right);
	      right = node->right->first;
	    }
	  else
	    {
	      if (node->next == -1)
		calc_next (dfa, node);
	      right = node->next;
	    }
	  re_node_set_init_2 (dfa->edests + idx, left, right);
	}
      else if (dfa->nodes[idx].type == ANCHOR
	       || dfa->nodes[idx].type == OP_OPEN_SUBEXP
	       || dfa->nodes[idx].type == OP_CLOSE_SUBEXP
	       || dfa->nodes[idx].type == OP_BACK_REF)
	re_node_set_init_1 (dfa->edests + idx, node->next);
    }
}

/* Duplicate the epsilon closure of the node ROOT_NODE.
   Note that duplicated nodes have constraint INIT_CONSTRAINT in addition
   to their own constraint.  */

static reg_errcode_t
duplicate_node_closure (dfa, top_org_node, top_clone_node, root_node,
			init_constraint)
     re_dfa_t *dfa;
     int top_org_node, top_clone_node, root_node;
     unsigned int init_constraint;
{
  reg_errcode_t err;
  int org_node, clone_node, ret;
  unsigned int constraint = init_constraint;
  for (org_node = top_org_node, clone_node = top_clone_node;;)
    {
      int org_dest, clone_dest;
      if (dfa->nodes[org_node].type == OP_BACK_REF)
	{
	  /* If the back reference epsilon-transit, its destination must
	     also have the constraint.  Then duplicate the epsilon closure
	     of the destination of the back reference, and store it in
	     edests of the back reference.  */
	  org_dest = dfa->nexts[org_node];
	  re_node_set_empty (dfa->edests + clone_node);
	  err = duplicate_node (&clone_dest, dfa, org_dest, constraint);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	  dfa->nexts[clone_node] = dfa->nexts[org_node];
	  ret = re_node_set_insert (dfa->edests + clone_node, clone_dest);
	  if (BE (ret < 0, 0))
	    return REG_ESPACE;
	}
      else if (dfa->edests[org_node].nelem == 0)
	{
	  /* In case of the node can't epsilon-transit, don't duplicate the
	     destination and store the original destination as the
	     destination of the node.  */
	  dfa->nexts[clone_node] = dfa->nexts[org_node];
	  break;
	}
      else if (dfa->edests[org_node].nelem == 1)
	{
	  /* In case of the node can epsilon-transit, and it has only one
	     destination.  */
	  org_dest = dfa->edests[org_node].elems[0];
	  re_node_set_empty (dfa->edests + clone_node);
	  if (dfa->nodes[org_node].type == ANCHOR)
	    {
	      /* In case of the node has another constraint, append it.  */
	      if (org_node == root_node && clone_node != org_node)
		{
		  /* ...but if the node is root_node itself, it means the
		     epsilon closure have a loop, then tie it to the
		     destination of the root_node.  */
		  ret = re_node_set_insert (dfa->edests + clone_node,
					    org_dest);
		  if (BE (ret < 0, 0))
		    return REG_ESPACE;
		  break;
		}
	      constraint |= dfa->nodes[org_node].opr.ctx_type;
	    }
	  err = duplicate_node (&clone_dest, dfa, org_dest, constraint);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	  ret = re_node_set_insert (dfa->edests + clone_node, clone_dest);
	  if (BE (ret < 0, 0))
	    return REG_ESPACE;
	}
      else /* dfa->edests[org_node].nelem == 2 */
	{
	  /* In case of the node can epsilon-transit, and it has two
	     destinations. E.g. '|', '*', '+', '?'.   */
	  org_dest = dfa->edests[org_node].elems[0];
	  re_node_set_empty (dfa->edests + clone_node);
	  /* Search for a duplicated node which satisfies the constraint.  */
	  clone_dest = search_duplicated_node (dfa, org_dest, constraint);
	  if (clone_dest == -1)
	    {
	      /* There are no such a duplicated node, create a new one.  */
	      err = duplicate_node (&clone_dest, dfa, org_dest, constraint);
	      if (BE (err != REG_NOERROR, 0))
		return err;
	      ret = re_node_set_insert (dfa->edests + clone_node, clone_dest);
	      if (BE (ret < 0, 0))
		return REG_ESPACE;
	      err = duplicate_node_closure (dfa, org_dest, clone_dest,
					    root_node, constraint);
	      if (BE (err != REG_NOERROR, 0))
		return err;
	    }
	  else
	    {
	      /* There are a duplicated node which satisfy the constraint,
		 use it to avoid infinite loop.  */
	      ret = re_node_set_insert (dfa->edests + clone_node, clone_dest);
	      if (BE (ret < 0, 0))
		return REG_ESPACE;
	    }

	  org_dest = dfa->edests[org_node].elems[1];
	  err = duplicate_node (&clone_dest, dfa, org_dest, constraint);
	  if (BE (err != REG_NOERROR, 0))
	    return err;
	  ret = re_node_set_insert (dfa->edests + clone_node, clone_dest);
	  if (BE (ret < 0, 0))
	    return REG_ESPACE;
	}
      org_node = org_dest;
      clone_node = clone_dest;
    }
  return REG_NOERROR;
}

/* Search for a node which is duplicated from the node ORG_NODE, and
   satisfies the constraint CONSTRAINT.  */

static int
search_duplicated_node (dfa, org_node, constraint)
     re_dfa_t *dfa;
     int org_node;
     unsigned int constraint;
{
  int idx;
  for (idx = dfa->nodes_len - 1; dfa->nodes[idx].duplicated && idx > 0; --idx)
    {
      if (org_node == dfa->org_indices[idx]
	  && constraint == dfa->nodes[idx].constraint)
	return idx; /* Found.  */
    }
  return -1; /* Not found.  */
}

/* Duplicate the node whose index is ORG_IDX and set the constraint CONSTRAINT.
   The new index will be stored in NEW_IDX and return REG_NOERROR if succeeded,
   otherwise return the error code.  */

static reg_errcode_t
duplicate_node (new_idx, dfa, org_idx, constraint)
     re_dfa_t *dfa;
     int *new_idx, org_idx;
     unsigned int constraint;
{
  re_token_t dup;
  int dup_idx;

  dup = dfa->nodes[org_idx];
  dup_idx = re_dfa_add_node (dfa, dup, 1);
  if (BE (dup_idx == -1, 0))
    return REG_ESPACE;
  dfa->nodes[dup_idx].constraint = constraint;
  if (dfa->nodes[org_idx].type == ANCHOR)
    dfa->nodes[dup_idx].constraint |= dfa->nodes[org_idx].opr.ctx_type;
  dfa->nodes[dup_idx].duplicated = 1;
  re_node_set_init_empty (dfa->edests + dup_idx);
  re_node_set_init_empty (dfa->eclosures + dup_idx);
  re_node_set_init_empty (dfa->inveclosures + dup_idx);

  /* Store the index of the original node.  */
  dfa->org_indices[dup_idx] = org_idx;
  *new_idx = dup_idx;
  return REG_NOERROR;
}

static void
calc_inveclosure (dfa)
     re_dfa_t *dfa;
{
  int src, idx, dest;
  for (src = 0; src < dfa->nodes_len; ++src)
    {
      for (idx = 0; idx < dfa->eclosures[src].nelem; ++idx)
	{
	  dest = dfa->eclosures[src].elems[idx];
	  re_node_set_insert (dfa->inveclosures + dest, src);
	}
    }
}

/* Calculate "eclosure" for all the node in DFA.  */

static reg_errcode_t
calc_eclosure (dfa)
     re_dfa_t *dfa;
{
  int node_idx, incomplete;
#ifdef DEBUG
  assert (dfa->nodes_len > 0);
#endif
  incomplete = 0;
  /* For each nodes, calculate epsilon closure.  */
  for (node_idx = 0; ; ++node_idx)
    {
      reg_errcode_t err;
      re_node_set eclosure_elem;
      if (node_idx == dfa->nodes_len)
	{
	  if (!incomplete)
	    break;
	  incomplete = 0;
	  node_idx = 0;
	}

#ifdef DEBUG
      assert (dfa->eclosures[node_idx].nelem != -1);
#endif
      /* If we have already calculated, skip it.  */
      if (dfa->eclosures[node_idx].nelem != 0)
	continue;
      /* Calculate epsilon closure of `node_idx'.  */
      err = calc_eclosure_iter (&eclosure_elem, dfa, node_idx, 1);
      if (BE (err != REG_NOERROR, 0))
	return err;

      if (dfa->eclosures[node_idx].nelem == 0)
	{
	  incomplete = 1;
	  re_node_set_free (&eclosure_elem);
	}
    }
  return REG_NOERROR;
}

/* Calculate epsilon closure of NODE.  */

static reg_errcode_t
calc_eclosure_iter (new_set, dfa, node, root)
     re_node_set *new_set;
     re_dfa_t *dfa;
     int node, root;
{
  reg_errcode_t err;
  unsigned int constraint;
  int i, incomplete;
  re_node_set eclosure;
  incomplete = 0;
  err = re_node_set_alloc (&eclosure, dfa->edests[node].nelem + 1);
  if (BE (err != REG_NOERROR, 0))
    return err;

  /* This indicates that we are calculating this node now.
     We reference this value to avoid infinite loop.  */
  dfa->eclosures[node].nelem = -1;

  constraint = ((dfa->nodes[node].type == ANCHOR)
		? dfa->nodes[node].opr.ctx_type : 0);
  /* If the current node has constraints, duplicate all nodes.
     Since they must inherit the constraints.  */
  if (constraint && !dfa->nodes[dfa->edests[node].elems[0]].duplicated)
    {
      int org_node, cur_node;
      org_node = cur_node = node;
      err = duplicate_node_closure (dfa, node, node, node, constraint);
      if (BE (err != REG_NOERROR, 0))
	return err;
    }

  /* Expand each epsilon destination nodes.  */
  if (IS_EPSILON_NODE(dfa->nodes[node].type))
    for (i = 0; i < dfa->edests[node].nelem; ++i)
      {
	re_node_set eclosure_elem;
	int edest = dfa->edests[node].elems[i];
	/* If calculating the epsilon closure of `edest' is in progress,
	   return intermediate result.  */
	if (dfa->eclosures[edest].nelem == -1)
	  {
	    incomplete = 1;
	    continue;
	  }
	/* If we haven't calculated the epsilon closure of `edest' yet,
	   calculate now. Otherwise use calculated epsilon closure.  */
	if (dfa->eclosures[edest].nelem == 0)
	  {
	    err = calc_eclosure_iter (&eclosure_elem, dfa, edest, 0);
	    if (BE (err != REG_NOERROR, 0))
	      return err;
	  }
	else
	  eclosure_elem = dfa->eclosures[edest];
	/* Merge the epsilon closure of `edest'.  */
	re_node_set_merge (&eclosure, &eclosure_elem);
	/* If the epsilon closure of `edest' is incomplete,
	   the epsilon closure of this node is also incomplete.  */
	if (dfa->eclosures[edest].nelem == 0)
	  {
	    incomplete = 1;
	    re_node_set_free (&eclosure_elem);
	  }
      }

  /* Epsilon closures include itself.  */
  re_node_set_insert (&eclosure, node);
  if (incomplete && !root)
    dfa->eclosures[node].nelem = 0;
  else
    dfa->eclosures[node] = eclosure;
  *new_set = eclosure;
  return REG_NOERROR;
}

/* Functions for token which are used in the parser.  */

/* Fetch a token from INPUT.
   We must not use this function inside bracket expressions.  */

static re_token_t
fetch_token (input, syntax)
     re_string_t *input;
     reg_syntax_t syntax;
{
  re_token_t token;
  int consumed_byte;
  consumed_byte = peek_token (&token, input, syntax);
  re_string_skip_bytes (input, consumed_byte);
  return token;
}

/* Peek a token from INPUT, and return the length of the token.
   We must not use this function inside bracket expressions.  */

static int
peek_token (token, input, syntax)
     re_token_t *token;
     re_string_t *input;
     reg_syntax_t syntax;
{
  unsigned char c;

  if (re_string_eoi (input))
    {
      token->type = END_OF_RE;
      return 0;
    }

  c = re_string_peek_byte (input, 0);
  token->opr.c = c;

#ifdef RE_ENABLE_I18N
  token->mb_partial = 0;
  if (MB_CUR_MAX > 1 &&
      !re_string_first_byte (input, re_string_cur_idx (input)))
    {
      token->type = CHARACTER;
      token->mb_partial = 1;
      return 1;
    }
#endif
  if (c == '\\')
    {
      unsigned char c2;
      if (re_string_cur_idx (input) + 1 >= re_string_length (input))
	{
	  token->type = BACK_SLASH;
	  return 1;
	}

      c2 = re_string_peek_byte_case (input, 1);
      token->opr.c = c2;
      token->type = CHARACTER;
      switch (c2)
	{
	case '|':
	  if (!(syntax & RE_LIMITED_OPS) && !(syntax & RE_NO_BK_VBAR))
	    token->type = OP_ALT;
	  break;
	case '1': case '2': case '3': case '4': case '5':
	case '6': case '7': case '8': case '9':
	  if (!(syntax & RE_NO_BK_REFS))
	    {
	      token->type = OP_BACK_REF;
	      token->opr.idx = c2 - '0';
	    }
	  break;
	case '<':
	  if (!(syntax & RE_NO_GNU_OPS))
	    {
	      token->type = ANCHOR;
	      token->opr.idx = WORD_FIRST;
	    }
	  break;
	case '>':
	  if (!(syntax & RE_NO_GNU_OPS))
	    {
	      token->type = ANCHOR;
	      token->opr.idx = WORD_LAST;
	    }
	  break;
	case 'b':
	  if (!(syntax & RE_NO_GNU_OPS))
	    {
	      token->type = ANCHOR;
	      token->opr.idx = WORD_DELIM;
	    }
	  break;
	case 'B':
	  if (!(syntax & RE_NO_GNU_OPS))
	    {
	      token->type = ANCHOR;
	      token->opr.idx = INSIDE_WORD;
	    }
	  break;
	case 'w':
	  if (!(syntax & RE_NO_GNU_OPS))
	    token->type = OP_WORD;
	  break;
	case 'W':
	  if (!(syntax & RE_NO_GNU_OPS))
	    token->type = OP_NOTWORD;
	  break;
	case '`':
	  if (!(syntax & RE_NO_GNU_OPS))
	    {
	      token->type = ANCHOR;
	      token->opr.idx = BUF_FIRST;
	    }
	  break;
	case '\'':
	  if (!(syntax & RE_NO_GNU_OPS))
	    {
	      token->type = ANCHOR;
	      token->opr.idx = BUF_LAST;
	    }
	  break;
	case '(':
	  if (!(syntax & RE_NO_BK_PARENS))
	    token->type = OP_OPEN_SUBEXP;
	  break;
	case ')':
	  if (!(syntax & RE_NO_BK_PARENS))
	    token->type = OP_CLOSE_SUBEXP;
	  break;
	case '+':
	  if (!(syntax & RE_LIMITED_OPS) && (syntax & RE_BK_PLUS_QM))
	    token->type = OP_DUP_PLUS;
	  break;
	case '?':
	  if (!(syntax & RE_LIMITED_OPS) && (syntax & RE_BK_PLUS_QM))
	    token->type = OP_DUP_QUESTION;
	  break;
	case '{':
	  if ((syntax & RE_INTERVALS) && (!(syntax & RE_NO_BK_BRACES)))
	    token->type = OP_OPEN_DUP_NUM;
	  break;
	case '}':
	  if ((syntax & RE_INTERVALS) && (!(syntax & RE_NO_BK_BRACES)))
	    token->type = OP_CLOSE_DUP_NUM;
	  break;
	default:
	  break;
	}
      return 2;
    }

  token->type = CHARACTER;
  switch (c)
    {
    case '\n':
      if (syntax & RE_NEWLINE_ALT)
	token->type = OP_ALT;
      break;
    case '|':
      if (!(syntax & RE_LIMITED_OPS) && (syntax & RE_NO_BK_VBAR))
	token->type = OP_ALT;
      break;
    case '*':
      token->type = OP_DUP_ASTERISK;
      break;
    case '+':
      if (!(syntax & RE_LIMITED_OPS) && !(syntax & RE_BK_PLUS_QM))
	token->type = OP_DUP_PLUS;
      break;
    case '?':
      if (!(syntax & RE_LIMITED_OPS) && !(syntax & RE_BK_PLUS_QM))
	token->type = OP_DUP_QUESTION;
      break;
    case '{':
      if ((syntax & RE_INTERVALS) && (syntax & RE_NO_BK_BRACES))
	token->type = OP_OPEN_DUP_NUM;
      break;
    case '}':
      if ((syntax & RE_INTERVALS) && (syntax & RE_NO_BK_BRACES))
	token->type = OP_CLOSE_DUP_NUM;
      break;
    case '(':
      if (syntax & RE_NO_BK_PARENS)
	token->type = OP_OPEN_SUBEXP;
      break;
    case ')':
      if (syntax & RE_NO_BK_PARENS)
	token->type = OP_CLOSE_SUBEXP;
      break;
    case '[':
      token->type = OP_OPEN_BRACKET;
      break;
    case '.':
      token->type = OP_PERIOD;
      break;
    case '^':
      if (!(syntax & RE_CONTEXT_INDEP_ANCHORS) &&
	  re_string_cur_idx (input) != 0)
	{
	  char prev = re_string_peek_byte (input, -1);
	  if (prev != '|' && prev != '(' &&
	      (!(syntax & RE_NEWLINE_ALT) || prev != '\n'))
	    break;
	}
      token->type = ANCHOR;
      token->opr.idx = LINE_FIRST;
      break;
    case '$':
      if (!(syntax & RE_CONTEXT_INDEP_ANCHORS) &&
	  re_string_cur_idx (input) + 1 != re_string_length (input))
	{
	  re_token_t next;
	  re_string_skip_bytes (input, 1);
	  peek_token (&next, input, syntax);
	  re_string_skip_bytes (input, -1);
	  if (next.type != OP_ALT && next.type != OP_CLOSE_SUBEXP)
	    break;
	}
      token->type = ANCHOR;
      token->opr.idx = LINE_LAST;
      break;
    default:
      break;
    }
  return 1;
}

/* Peek a token from INPUT, and return the length of the token.
   We must not use this function out of bracket expressions.  */

static int
peek_token_bracket (token, input, syntax)
     re_token_t *token;
     re_string_t *input;
     reg_syntax_t syntax;
{
  unsigned char c;
  if (re_string_eoi (input))
    {
      token->type = END_OF_RE;
      return 0;
    }
  c = re_string_peek_byte (input, 0);
  token->opr.c = c;

#ifdef RE_ENABLE_I18N
  if (MB_CUR_MAX > 1 &&
      !re_string_first_byte (input, re_string_cur_idx (input)))
    {
      token->type = CHARACTER;
      return 1;
    }
#endif /* RE_ENABLE_I18N */

  if (c == '\\' && (syntax & RE_BACKSLASH_ESCAPE_IN_LISTS))
    {
      /* In this case, '\' escape a character.  */
      unsigned char c2;
      re_string_skip_bytes (input, 1);
      c2 = re_string_peek_byte (input, 0);
      token->opr.c = c2;
      token->type = CHARACTER;
      return 1;
    }
  if (c == '[') /* '[' is a special char in a bracket exps.  */
    {
      unsigned char c2;
      int token_len;
      c2 = re_string_peek_byte (input, 1);
      token->opr.c = c2;
      token_len = 2;
      switch (c2)
	{
	case '.':
	  token->type = OP_OPEN_COLL_ELEM;
	  break;
	case '=':
	  token->type = OP_OPEN_EQUIV_CLASS;
	  break;
	case ':':
	  if (syntax & RE_CHAR_CLASSES)
	    {
	      token->type = OP_OPEN_CHAR_CLASS;
	      break;
	    }
	  /* else fall through.  */
	default:
	  token->type = CHARACTER;
	  token->opr.c = c;
	  token_len = 1;
	  break;
	}
      return token_len;
    }
  switch (c)
    {
    case '-':
      token->type = OP_CHARSET_RANGE;
      break;
    case ']':
      token->type = OP_CLOSE_BRACKET;
      break;
    case '^':
      token->type = OP_NON_MATCH_LIST;
      break;
    default:
      token->type = CHARACTER;
    }
  return 1;
}

/* Functions for parser.  */

/* Entry point of the parser.
   Parse the regular expression REGEXP and return the structure tree.
   If an error is occured, ERR is set by error code, and return NULL.
   This function build the following tree, from regular expression <reg_exp>:
	   CAT
	   / \
	  /   \
   <reg_exp>  EOR

   CAT means concatenation.
   EOR means end of regular expression.  */

static bin_tree_t *
parse (regexp, preg, syntax, err)
     re_string_t *regexp;
     regex_t *preg;
     reg_syntax_t syntax;
     reg_errcode_t *err;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  bin_tree_t *tree, *eor, *root;
  re_token_t current_token;
  int new_idx;
  current_token = fetch_token (regexp, syntax);
  tree = parse_reg_exp (regexp, preg, &current_token, syntax, 0, err);
  if (BE (*err != REG_NOERROR && tree == NULL, 0))
    return NULL;
  new_idx = re_dfa_add_node (dfa, current_token, 0);
  eor = create_tree (NULL, NULL, 0, new_idx);
  if (tree != NULL)
    root = create_tree (tree, eor, CONCAT, 0);
  else
    root = eor;
  if (BE (new_idx == -1 || eor == NULL || root == NULL, 0))
    {
      *err = REG_ESPACE;
      return NULL;
    }
  return root;
}

/* This function build the following tree, from regular expression
   <branch1>|<branch2>:
	   ALT
	   / \
	  /   \
   <branch1> <branch2>

   ALT means alternative, which represents the operator `|'.  */

static bin_tree_t *
parse_reg_exp (regexp, preg, token, syntax, nest, err)
     re_string_t *regexp;
     regex_t *preg;
     re_token_t *token;
     reg_syntax_t syntax;
     int nest;
     reg_errcode_t *err;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  bin_tree_t *tree, *branch = NULL;
  int new_idx;
  tree = parse_branch (regexp, preg, token, syntax, nest, err);
  if (BE (*err != REG_NOERROR && tree == NULL, 0))
    return NULL;

  while (token->type == OP_ALT)
    {
      re_token_t alt_token = *token;
      new_idx = re_dfa_add_node (dfa, alt_token, 0);
      *token = fetch_token (regexp, syntax);
      if (token->type != OP_ALT && token->type != END_OF_RE
	  && (nest == 0 || token->type != OP_CLOSE_SUBEXP))
	{
	  branch = parse_branch (regexp, preg, token, syntax, nest, err);
	  if (BE (*err != REG_NOERROR && branch == NULL, 0))
	    {
	      free_bin_tree (tree);
	      return NULL;
	    }
	}
      else
	branch = NULL;
      tree = create_tree (tree, branch, 0, new_idx);
      if (BE (new_idx == -1 || tree == NULL, 0))
	{
	  *err = REG_ESPACE;
	  return NULL;
	}
      dfa->has_plural_match = 1;
    }
  return tree;
}

/* This function build the following tree, from regular expression
   <exp1><exp2>:
	CAT
	/ \
       /   \
   <exp1> <exp2>

   CAT means concatenation.  */

static bin_tree_t *
parse_branch (regexp, preg, token, syntax, nest, err)
     re_string_t *regexp;
     regex_t *preg;
     re_token_t *token;
     reg_syntax_t syntax;
     int nest;
     reg_errcode_t *err;
{
  bin_tree_t *tree, *exp;
  tree = parse_expression (regexp, preg, token, syntax, nest, err);
  if (BE (*err != REG_NOERROR && tree == NULL, 0))
    return NULL;

  while (token->type != OP_ALT && token->type != END_OF_RE
	 && (nest == 0 || token->type != OP_CLOSE_SUBEXP))
    {
      exp = parse_expression (regexp, preg, token, syntax, nest, err);
      if (BE (*err != REG_NOERROR && exp == NULL, 0))
	{
	  free_bin_tree (tree);
	  return NULL;
	}
      if (tree != NULL && exp != NULL)
	{
	  tree = create_tree (tree, exp, CONCAT, 0);
	  if (tree == NULL)
	    {
	      *err = REG_ESPACE;
	      return NULL;
	    }
	}
      else if (tree == NULL)
	tree = exp;
      /* Otherwise exp == NULL, we don't need to create new tree.  */
    }
  return tree;
}

/* This function build the following tree, from regular expression a*:
	 *
	 |
	 a
*/

static bin_tree_t *
parse_expression (regexp, preg, token, syntax, nest, err)
     re_string_t *regexp;
     regex_t *preg;
     re_token_t *token;
     reg_syntax_t syntax;
     int nest;
     reg_errcode_t *err;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  bin_tree_t *tree;
  int new_idx;
  switch (token->type)
    {
    case CHARACTER:
      new_idx = re_dfa_add_node (dfa, *token, 0);
      tree = create_tree (NULL, NULL, 0, new_idx);
      if (BE (new_idx == -1 || tree == NULL, 0))
	{
	  *err = REG_ESPACE;
	  return NULL;
	}
#ifdef RE_ENABLE_I18N
      if (MB_CUR_MAX > 1)
	{
	  while (!re_string_eoi (regexp)
		 && !re_string_first_byte (regexp, re_string_cur_idx (regexp)))
	    {
	      bin_tree_t *mbc_remain;
	      *token = fetch_token (regexp, syntax);
	      new_idx = re_dfa_add_node (dfa, *token, 0);
	      mbc_remain = create_tree (NULL, NULL, 0, new_idx);
	      tree = create_tree (tree, mbc_remain, CONCAT, 0);
	      if (BE (new_idx == -1 || mbc_remain == NULL || tree == NULL, 0))
		{
		  *err = REG_ESPACE;
		  return NULL;
		}
	    }
	}
#endif
      break;
    case OP_OPEN_SUBEXP:
      tree = parse_sub_exp (regexp, preg, token, syntax, nest + 1, err);
      if (BE (*err != REG_NOERROR && tree == NULL, 0))
	return NULL;
      break;
    case OP_OPEN_BRACKET:
      tree = parse_bracket_exp (regexp, dfa, token, syntax, err);
      if (BE (*err != REG_NOERROR && tree == NULL, 0))
	return NULL;
      break;
    case OP_BACK_REF:
      if (BE (preg->re_nsub < token->opr.idx
	      || dfa->subexps[token->opr.idx - 1].end == -1, 0))
	{
	  *err = REG_ESUBREG;
	  return NULL;
	}
      dfa->used_bkref_map |= 1 << (token->opr.idx - 1);
      new_idx = re_dfa_add_node (dfa, *token, 0);
      tree = create_tree (NULL, NULL, 0, new_idx);
      if (BE (new_idx == -1 || tree == NULL, 0))
	{
	  *err = REG_ESPACE;
	  return NULL;
	}
      ++dfa->nbackref;
      dfa->has_mb_node = 1;
      break;
    case OP_DUP_ASTERISK:
    case OP_DUP_PLUS:
    case OP_DUP_QUESTION:
    case OP_OPEN_DUP_NUM:
      if (syntax & RE_CONTEXT_INVALID_OPS)
	{
	  *err = REG_BADRPT;
	  return NULL;
	}
      else if (syntax & RE_CONTEXT_INDEP_OPS)
	{
	  *token = fetch_token (regexp, syntax);
	  return parse_expression (regexp, preg, token, syntax, nest, err);
	}
      /* else fall through  */
    case OP_CLOSE_SUBEXP:
      if ((token->type == OP_CLOSE_SUBEXP) &&
	  !(syntax & RE_UNMATCHED_RIGHT_PAREN_ORD))
	{
	  *err = REG_ERPAREN;
	  return NULL;
	}
      /* else fall through  */
    case OP_CLOSE_DUP_NUM:
      /* We treat it as a normal character.  */

      /* Then we can these characters as normal characters.  */
      token->type = CHARACTER;
      new_idx = re_dfa_add_node (dfa, *token, 0);
      tree = create_tree (NULL, NULL, 0, new_idx);
      if (BE (new_idx == -1 || tree == NULL, 0))
	{
	  *err = REG_ESPACE;
	  return NULL;
	}
      break;
    case ANCHOR:
      if (dfa->word_char == NULL)
	{
	  *err = init_word_char (dfa);
	  if (BE (*err != REG_NOERROR, 0))
	    return NULL;
	}
      if (token->opr.ctx_type == WORD_DELIM)
	{
	  bin_tree_t *tree_first, *tree_last;
	  int idx_first, idx_last;
	  token->opr.ctx_type = WORD_FIRST;
	  idx_first = re_dfa_add_node (dfa, *token, 0);
	  tree_first = create_tree (NULL, NULL, 0, idx_first);
	  token->opr.ctx_type = WORD_LAST;
	  idx_last = re_dfa_add_node (dfa, *token, 0);
	  tree_last = create_tree (NULL, NULL, 0, idx_last);
	  token->type = OP_ALT;
	  new_idx = re_dfa_add_node (dfa, *token, 0);
	  tree = create_tree (tree_first, tree_last, 0, new_idx);
	  if (BE (idx_first == -1 || idx_last == -1 || new_idx == -1
		  || tree_first == NULL || tree_last == NULL
		  || tree == NULL, 0))
	    {
	      *err = REG_ESPACE;
	      return NULL;
	    }
	}
      else
	{
	  new_idx = re_dfa_add_node (dfa, *token, 0);
	  tree = create_tree (NULL, NULL, 0, new_idx);
	  if (BE (new_idx == -1 || tree == NULL, 0))
	    {
	      *err = REG_ESPACE;
	      return NULL;
	    }
	}
      /* We must return here, since ANCHORs can't be followed
	 by repetition operators.
	 eg. RE"^*" is invalid or "<ANCHOR(^)><CHAR(*)>",
	     it must not be "<ANCHOR(^)><REPEAT(*)>".  */
      *token = fetch_token (regexp, syntax);
      return tree;
    case OP_PERIOD:
      new_idx = re_dfa_add_node (dfa, *token, 0);
      tree = create_tree (NULL, NULL, 0, new_idx);
      if (BE (new_idx == -1 || tree == NULL, 0))
	{
	  *err = REG_ESPACE;
	  return NULL;
	}
      if (MB_CUR_MAX > 1)
	dfa->has_mb_node = 1;
      break;
    case OP_WORD:
      tree = build_word_op (dfa, 0, err);
      if (BE (*err != REG_NOERROR && tree == NULL, 0))
	return NULL;
      break;
    case OP_NOTWORD:
      tree = build_word_op (dfa, 1, err);
      if (BE (*err != REG_NOERROR && tree == NULL, 0))
	return NULL;
      break;
    case OP_ALT:
    case END_OF_RE:
      return NULL;
    case BACK_SLASH:
      *err = REG_EESCAPE;
      return NULL;
    default:
      /* Must not happen?  */
#ifdef DEBUG
      assert (0);
#endif
      return NULL;
    }
  *token = fetch_token (regexp, syntax);

  while (token->type == OP_DUP_ASTERISK || token->type == OP_DUP_PLUS
	 || token->type == OP_DUP_QUESTION || token->type == OP_OPEN_DUP_NUM)
    {
      tree = parse_dup_op (tree, regexp, dfa, token, syntax, err);
      if (BE (*err != REG_NOERROR && tree == NULL, 0))
	return NULL;
      dfa->has_plural_match = 1;
    }

  return tree;
}

/* This function build the following tree, from regular expression
   (<reg_exp>):
	 SUBEXP
	    |
	<reg_exp>
*/

static bin_tree_t *
parse_sub_exp (regexp, preg, token, syntax, nest, err)
     re_string_t *regexp;
     regex_t *preg;
     re_token_t *token;
     reg_syntax_t syntax;
     int nest;
     reg_errcode_t *err;
{
  re_dfa_t *dfa = (re_dfa_t *) preg->buffer;
  bin_tree_t *tree, *left_par, *right_par;
  size_t cur_nsub;
  int new_idx;
  cur_nsub = preg->re_nsub++;
  if (dfa->subexps_alloc < preg->re_nsub)
    {
      re_subexp_t *new_array;
      dfa->subexps_alloc *= 2;
      new_array = re_realloc (dfa->subexps, re_subexp_t, dfa->subexps_alloc);
      if (BE (new_array == NULL, 0))
	{
	  dfa->subexps_alloc /= 2;
	  *err = REG_ESPACE;
	  return NULL;
	}
      dfa->subexps = new_array;
    }
  dfa->subexps[cur_nsub].start = dfa->nodes_len;
  dfa->subexps[cur_nsub].end = -1;

  new_idx = re_dfa_add_node (dfa, *token, 0);
  left_par = create_tree (NULL, NULL, 0, new_idx);
  if (BE (new_idx == -1 || left_par == NULL, 0))
    {
      *err = REG_ESPACE;
      return NULL;
    }
  dfa->nodes[new_idx].opr.idx = (int)cur_nsub;
  *token = fetch_token (regexp, syntax);

  /* The subexpression may be a null string.  */
  if (token->type == OP_CLOSE_SUBEXP)
    tree = NULL;
  else
    {
      tree = parse_reg_exp (regexp, preg, token, syntax, nest, err);
      if (BE (*err != REG_NOERROR && tree == NULL, 0))
	return NULL;
    }
  if (BE (token->type != OP_CLOSE_SUBEXP, 0))
    {
      free_bin_tree (tree);
      *err = REG_BADPAT;
      return NULL;
    }
  new_idx = re_dfa_add_node (dfa, *token, 0);
  dfa->subexps[cur_nsub].end = dfa->nodes_len;
  right_par = create_tree (NULL, NULL, 0, new_idx);
  tree = ((tree == NULL) ? right_par
	  : create_tree (tree, right_par, CONCAT, 0));
  tree = create_tree (left_par, tree, CONCAT, 0);
  if (BE (new_idx == -1 || right_par == NULL || tree == NULL, 0))
    {
      *err = REG_ESPACE;
      return NULL;
    }
  dfa->nodes[new_idx].opr.idx = (int)cur_nsub;

  return tree;
}

/* This function parse repetition operators like "*", "+", "{1,3}" etc.  */

static bin_tree_t *
parse_dup_op (dup_elem, regexp, dfa, token, syntax, err)
     bin_tree_t *dup_elem;
     re_string_t *regexp;
     re_dfa_t *dfa;
     re_token_t *token;
     reg_syntax_t syntax;
     reg_errcode_t *err;
{
  re_token_t dup_token;
  bin_tree_t *tree = dup_elem, *work_tree;
  int new_idx, start_idx = re_string_cur_idx (regexp);
  re_token_t start_token = *token;
  if (token->type == OP_OPEN_DUP_NUM)
    {
      int i;
      int end = 0;
      int start = fetch_number (regexp, token, syntax);
      bin_tree_t *elem;
      if (start == -1)
	{
	  if (token->type == CHARACTER && token->opr.c == ',')
	    start = 0; /* We treat "{,m}" as "{0,m}".  */
	  else
	    {
	      *err = REG_BADBR; /* <re>{} is invalid.  */
	      return NULL;
	    }
	}
      if (BE (start != -2, 1))
	{
	  /* We treat "{n}" as "{n,n}".  */
	  end = ((token->type == OP_CLOSE_DUP_NUM) ? start
		 : ((token->type == CHARACTER && token->opr.c == ',')
		    ? fetch_number (regexp, token, syntax) : -2));
	}
      if (BE (start == -2 || end == -2, 0))
	{
	  /* Invalid sequence.  */
	  if (token->type == OP_CLOSE_DUP_NUM)
	    goto parse_dup_op_invalid_interval;
	  else
	    goto parse_dup_op_ebrace;
	}
      if (BE (start == 0 && end == 0, 0))
	{
	  /* We treat "<re>{0}" and "<re>{0,0}" as null string.  */
	  *token = fetch_token (regexp, syntax);
	  free_bin_tree (dup_elem);
	  return NULL;
	}

      /* Extract "<re>{n,m}" to "<re><re>...<re><re>{0,<m-n>}".  */
      elem = tree;
      for (i = 0; i < start; ++i)
	if (i != 0)
	  {
	    work_tree = duplicate_tree (elem, dfa);
	    tree = create_tree (tree, work_tree, CONCAT, 0);
	    if (BE (work_tree == NULL || tree == NULL, 0))
	      goto parse_dup_op_espace;
	  }

      if (end == -1)
	{
	  /* We treat "<re>{0,}" as "<re>*".  */
	  dup_token.type = OP_DUP_ASTERISK;
	  if (start > 0)
	    {
	      elem = duplicate_tree (elem, dfa);
	      new_idx = re_dfa_add_node (dfa, dup_token, 0);
	      work_tree = create_tree (elem, NULL, 0, new_idx);
	      tree = create_tree (tree, work_tree, CONCAT, 0);
	      if (BE (elem == NULL || new_idx == -1 || work_tree == NULL
		      || tree == NULL, 0))
		goto parse_dup_op_espace;
	    }
	  else
	    {
	      new_idx = re_dfa_add_node (dfa, dup_token, 0);
	      tree = create_tree (elem, NULL, 0, new_idx);
	      if (BE (new_idx == -1 || tree == NULL, 0))
		goto parse_dup_op_espace;
	    }
	}
      else if (end - start > 0)
	{
	  /* Then extract "<re>{0,m}" to "<re>?<re>?...<re>?".  */
	  dup_token.type = OP_DUP_QUESTION;
	  if (start > 0)
	    {
	      elem = duplicate_tree (elem, dfa);
	      new_idx = re_dfa_add_node (dfa, dup_token, 0);
	      elem = create_tree (elem, NULL, 0, new_idx);
	      tree = create_tree (tree, elem, CONCAT, 0);
	      if (BE (elem == NULL || new_idx == -1 || tree == NULL, 0))
		goto parse_dup_op_espace;
	    }
	  else
	    {
	      new_idx = re_dfa_add_node (dfa, dup_token, 0);
	      tree = elem = create_tree (elem, NULL, 0, new_idx);
	      if (BE (new_idx == -1 || tree == NULL, 0))
		goto parse_dup_op_espace;
	    }
	  for (i = 1; i < end - start; ++i)
	    {
	      work_tree = duplicate_tree (elem, dfa);
	      tree = create_tree (tree, work_tree, CONCAT, 0);
	      if (BE (work_tree == NULL || tree == NULL, 0))
		{
		  *err = REG_ESPACE;
		  return NULL;
		}
	    }
	}
    }
  else
    {
      new_idx = re_dfa_add_node (dfa, *token, 0);
      tree = create_tree (tree, NULL, 0, new_idx);
      if (BE (new_idx == -1 || tree == NULL, 0))
	{
	  *err = REG_ESPACE;
	  return NULL;
	}
    }
  *token = fetch_token (regexp, syntax);
  return tree;

 parse_dup_op_espace:
  free_bin_tree (tree);
  *err = REG_ESPACE;
  return NULL;

 parse_dup_op_ebrace:
  if (BE (!(syntax & RE_INVALID_INTERVAL_ORD), 0))
    {
      *err = REG_EBRACE;
      return NULL;
    }
  goto parse_dup_op_rollback;
 parse_dup_op_invalid_interval:
  if (BE (!(syntax & RE_INVALID_INTERVAL_ORD), 0))
    {
      *err = REG_BADBR;
      return NULL;
    }
 parse_dup_op_rollback:
  re_string_set_index (regexp, start_idx);
  *token = start_token;
  token->type = CHARACTER;
  return dup_elem;
}

/* Size of the names for collating symbol/equivalence_class/character_class.
   I'm not sure, but maybe enough.  */
#define BRACKET_NAME_BUF_SIZE 32

#ifndef _LIBC
  /* Local function for parse_bracket_exp only used in case of NOT _LIBC.
     Build the range expression which starts from START_ELEM, and ends
     at END_ELEM.  The result are written to MBCSET and SBCSET.
     RANGE_ALLOC is the allocated size of mbcset->range_starts, and
     mbcset->range_ends, is a pointer argument sinse we may
     update it.  */

static reg_errcode_t
# ifdef RE_ENABLE_I18N
build_range_exp (sbcset, mbcset, range_alloc, start_elem, end_elem)
     re_charset_t *mbcset;
     int *range_alloc;
# else /* not RE_ENABLE_I18N */
build_range_exp (sbcset, start_elem, end_elem)
# endif /* not RE_ENABLE_I18N */
     re_bitset_ptr_t sbcset;
     bracket_elem_t *start_elem, *end_elem;
{
  unsigned int start_ch, end_ch;
  /* Equivalence Classes and Character Classes can't be a range start/end.  */
  if (BE (start_elem->type == EQUIV_CLASS || start_elem->type == CHAR_CLASS
	  || end_elem->type == EQUIV_CLASS || end_elem->type == CHAR_CLASS,
	  0))
    return REG_ERANGE;

  /* We can handle no multi character collating elements without libc
     support.  */
  if (BE ((start_elem->type == COLL_SYM
	   && strlen ((char *) start_elem->opr.name) > 1)
	  || (end_elem->type == COLL_SYM
	      && strlen ((char *) end_elem->opr.name) > 1), 0))
    return REG_ECOLLATE;

# ifdef RE_ENABLE_I18N
  {
    wchar_t wc, start_wc, end_wc;
    wchar_t cmp_buf[6] = {L'\0', L'\0', L'\0', L'\0', L'\0', L'\0'};

    start_ch = ((start_elem->type == SB_CHAR) ? start_elem->opr.ch
		: ((start_elem->type == COLL_SYM) ? start_elem->opr.name[0]
		   : 0));
    end_ch = ((end_elem->type == SB_CHAR) ? end_elem->opr.ch
	      : ((end_elem->type == COLL_SYM) ? end_elem->opr.name[0]
		 : 0));
    start_wc = ((start_elem->type == SB_CHAR || start_elem->type == COLL_SYM)
		? __btowc (start_ch) : start_elem->opr.wch);
    end_wc = ((end_elem->type == SB_CHAR || end_elem->type == COLL_SYM)
	      ? __btowc (end_ch) : end_elem->opr.wch);
    cmp_buf[0] = start_wc;
    cmp_buf[4] = end_wc;
    if (wcscoll (cmp_buf, cmp_buf + 4) > 0)
      return REG_ERANGE;

    /* Check the space of the arrays.  */
    if (*range_alloc == mbcset->nranges)
      {
	/* There are not enough space, need realloc.  */
	wchar_t *new_array_start, *new_array_end;
	int new_nranges;

	/* +1 in case of mbcset->nranges is 0.  */
	new_nranges = 2 * mbcset->nranges + 1;
	/* Use realloc since mbcset->range_starts and mbcset->range_ends
	   are NULL if *range_alloc == 0.  */
	new_array_start = re_realloc (mbcset->range_starts, wchar_t,
				      new_nranges);
	new_array_end = re_realloc (mbcset->range_ends, wchar_t,
				    new_nranges);

	if (BE (new_array_start == NULL || new_array_end == NULL, 0))
	  return REG_ESPACE;

	mbcset->range_starts = new_array_start;
	mbcset->range_ends = new_array_end;
	*range_alloc = new_nranges;
      }

    mbcset->range_starts[mbcset->nranges] = start_wc;
    mbcset->range_ends[mbcset->nranges++] = end_wc;

    /* Build the table for single byte characters.  */
    for (wc = 0; wc <= SBC_MAX; ++wc)
      {
	cmp_buf[2] = wc;
	if (wcscoll (cmp_buf, cmp_buf + 2) <= 0
	    && wcscoll (cmp_buf + 2, cmp_buf + 4) <= 0)
	  bitset_set (sbcset, wc);
      }
  }
# else /* not RE_ENABLE_I18N */
  {
    unsigned int ch;
    start_ch = ((start_elem->type == SB_CHAR ) ? start_elem->opr.ch
		: ((start_elem->type == COLL_SYM) ? start_elem->opr.name[0]
		   : 0));
    end_ch = ((end_elem->type == SB_CHAR ) ? end_elem->opr.ch
	      : ((end_elem->type == COLL_SYM) ? end_elem->opr.name[0]
		 : 0));
    if (start_ch > end_ch)
      return REG_ERANGE;
    /* Build the table for single byte characters.  */
    for (ch = 0; ch <= SBC_MAX; ++ch)
      if (start_ch <= ch  && ch <= end_ch)
	bitset_set (sbcset, ch);
  }
# endif /* not RE_ENABLE_I18N */
  return REG_NOERROR;
}
#endif /* not _LIBC */

#ifndef _LIBC
/* Helper function for parse_bracket_exp only used in case of NOT _LIBC..
   Build the collating element which is represented by NAME.
   The result are written to MBCSET and SBCSET.
   COLL_SYM_ALLOC is the allocated size of mbcset->coll_sym, is a
   pointer argument since we may update it.  */

static reg_errcode_t
# ifdef RE_ENABLE_I18N
build_collating_symbol (sbcset, mbcset, coll_sym_alloc, name)
     re_charset_t *mbcset;
     int *coll_sym_alloc;
# else /* not RE_ENABLE_I18N */
build_collating_symbol (sbcset, name)
# endif /* not RE_ENABLE_I18N */
     re_bitset_ptr_t sbcset;
     const unsigned char *name;
{
  size_t name_len = strlen ((const char *) name);
  if (BE (name_len != 1, 0))
    return REG_ECOLLATE;
  else
    {
      bitset_set (sbcset, name[0]);
      return REG_NOERROR;
    }
}
#endif /* not _LIBC */

/* This function parse bracket expression like "[abc]", "[a-c]",
   "[[.a-a.]]" etc.  */

static bin_tree_t *
parse_bracket_exp (regexp, dfa, token, syntax, err)
     re_string_t *regexp;
     re_dfa_t *dfa;
     re_token_t *token;
     reg_syntax_t syntax;
     reg_errcode_t *err;
{
#ifdef _LIBC
  const unsigned char *collseqmb;
  const char *collseqwc;
  uint32_t nrules;
  int32_t table_size;
  const int32_t *symb_table;
  const unsigned char *extra;

  /* Local function for parse_bracket_exp used in _LIBC environement.
     Seek the collating symbol entry correspondings to NAME.
     Return the index of the symbol in the SYMB_TABLE.  */

  static inline int32_t
  seek_collating_symbol_entry (name, name_len)
	 const unsigned char *name;
	 size_t name_len;
    {
      int32_t hash = elem_hash ((const char *) name, name_len);
      int32_t elem = hash % table_size;
      int32_t second = hash % (table_size - 2);
      while (symb_table[2 * elem] != 0)
	{
	  /* First compare the hashing value.  */
	  if (symb_table[2 * elem] == hash
	      /* Compare the length of the name.  */
	      && name_len == extra[symb_table[2 * elem + 1]]
	      /* Compare the name.  */
	      && memcmp (name, &extra[symb_table[2 * elem + 1] + 1],
			 name_len) == 0)
	    {
	      /* Yep, this is the entry.  */
	      break;
	    }

	  /* Next entry.  */
	  elem += second;
	}
      return elem;
    }

  /* Local function for parse_bracket_exp used in _LIBC environement.
     Look up the collation sequence value of BR_ELEM.
     Return the value if succeeded, UINT_MAX otherwise.  */

  static inline unsigned int
  lookup_collation_sequence_value (br_elem)
	 bracket_elem_t *br_elem;
    {
      if (br_elem->type == SB_CHAR)
	{
	  /*
	  if (MB_CUR_MAX == 1)
	  */
	  if (nrules == 0)
	    return collseqmb[br_elem->opr.ch];
	  else
	    {
	      wint_t wc = __btowc (br_elem->opr.ch);
	      return collseq_table_lookup (collseqwc, wc);
	    }
	}
      else if (br_elem->type == MB_CHAR)
	{
	  return collseq_table_lookup (collseqwc, br_elem->opr.wch);
	}
      else if (br_elem->type == COLL_SYM)
	{
	  size_t sym_name_len = strlen ((char *) br_elem->opr.name);
	  if (nrules != 0)
	    {
	      int32_t elem, idx;
	      elem = seek_collating_symbol_entry (br_elem->opr.name,
						  sym_name_len);
	      if (symb_table[2 * elem] != 0)
		{
		  /* We found the entry.  */
		  idx = symb_table[2 * elem + 1];
		  /* Skip the name of collating element name.  */
		  idx += 1 + extra[idx];
		  /* Skip the byte sequence of the collating element.  */
		  idx += 1 + extra[idx];
		  /* Adjust for the alignment.  */
		  idx = (idx + 3) & ~3;
		  /* Skip the multibyte collation sequence value.  */
		  idx += sizeof (unsigned int);
		  /* Skip the wide char sequence of the collating element.  */
		  idx += sizeof (unsigned int) *
		    (1 + *(unsigned int *) (extra + idx));
		  /* Return the collation sequence value.  */
		  return *(unsigned int *) (extra + idx);
		}
	      else if (symb_table[2 * elem] == 0 && sym_name_len == 1)
		{
		  /* No valid character.  Match it as a single byte
		     character.  */
		  return collseqmb[br_elem->opr.name[0]];
		}
	    }
	  else if (sym_name_len == 1)
	    return collseqmb[br_elem->opr.name[0]];
	}
      return UINT_MAX;
    }

  /* Local function for parse_bracket_exp used in _LIBC environement.
     Build the range expression which starts from START_ELEM, and ends
     at END_ELEM.  The result are written to MBCSET and SBCSET.
     RANGE_ALLOC is the allocated size of mbcset->range_starts, and
     mbcset->range_ends, is a pointer argument sinse we may
     update it.  */

  static inline reg_errcode_t
# ifdef RE_ENABLE_I18N
  build_range_exp (sbcset, mbcset, range_alloc, start_elem, end_elem)
	 re_charset_t *mbcset;
	 int *range_alloc;
# else /* not RE_ENABLE_I18N */
  build_range_exp (sbcset, start_elem, end_elem)
# endif /* not RE_ENABLE_I18N */
	 re_bitset_ptr_t sbcset;
	 bracket_elem_t *start_elem, *end_elem;
    {
      unsigned int ch;
      uint32_t start_collseq;
      uint32_t end_collseq;

# ifdef RE_ENABLE_I18N
      /* Check the space of the arrays.  */
      if (*range_alloc == mbcset->nranges)
	{
	  /* There are not enough space, need realloc.  */
	  uint32_t *new_array_start;
	  uint32_t *new_array_end;
	  int new_nranges;

	  /* +1 in case of mbcset->nranges is 0.  */
	  new_nranges = 2 * mbcset->nranges + 1;
	  /* Use realloc since mbcset->range_starts and mbcset->range_ends
	     are NULL if *range_alloc == 0.  */
	  new_array_start = re_realloc (mbcset->range_starts, uint32_t,
					new_nranges);
	  new_array_end = re_realloc (mbcset->range_ends, uint32_t,
				      new_nranges);

	  if (BE (new_array_start == NULL || new_array_end == NULL, 0))
	    return REG_ESPACE;

	  mbcset->range_starts = new_array_start;
	  mbcset->range_ends = new_array_end;
	  *range_alloc = new_nranges;
	}
# endif /* RE_ENABLE_I18N */

      /* Equivalence Classes and Character Classes can't be a range
	 start/end.  */
      if (BE (start_elem->type == EQUIV_CLASS || start_elem->type == CHAR_CLASS
	      || end_elem->type == EQUIV_CLASS || end_elem->type == CHAR_CLASS,
	      0))
	return REG_ERANGE;

      start_collseq = lookup_collation_sequence_value (start_elem);
      end_collseq = lookup_collation_sequence_value (end_elem);
      /* Check start/end collation sequence values.  */
      if (BE (start_collseq == UINT_MAX || end_collseq == UINT_MAX, 0))
	return REG_ECOLLATE;
      if (BE ((syntax & RE_NO_EMPTY_RANGES) && start_collseq > end_collseq, 0))
	return REG_ERANGE;

# ifdef RE_ENABLE_I18N
      /* Got valid collation sequence values, add them as a new entry.  */
      mbcset->range_starts[mbcset->nranges] = start_collseq;
      mbcset->range_ends[mbcset->nranges++] = end_collseq;
# endif /* RE_ENABLE_I18N */

      /* Build the table for single byte characters.  */
      for (ch = 0; ch <= SBC_MAX; ch++)
	{
	  uint32_t ch_collseq;
	  /*
	  if (MB_CUR_MAX == 1)
	  */
	  if (nrules == 0)
	    ch_collseq = collseqmb[ch];
	  else
	    ch_collseq = collseq_table_lookup (collseqwc, __btowc (ch));
	  if (start_collseq <= ch_collseq && ch_collseq <= end_collseq)
	    bitset_set (sbcset, ch);
	}
      return REG_NOERROR;
    }

  /* Local function for parse_bracket_exp used in _LIBC environement.
     Build the collating element which is represented by NAME.
     The result are written to MBCSET and SBCSET.
     COLL_SYM_ALLOC is the allocated size of mbcset->coll_sym, is a
     pointer argument sinse we may update it.  */

  static inline reg_errcode_t
# ifdef RE_ENABLE_I18N
  build_collating_symbol (sbcset, mbcset, coll_sym_alloc, name)
	 re_charset_t *mbcset;
	 int *coll_sym_alloc;
# else /* not RE_ENABLE_I18N */
  build_collating_symbol (sbcset, name)
# endif /* not RE_ENABLE_I18N */
	 re_bitset_ptr_t sbcset;
	 const unsigned char *name;
    {
      int32_t elem, idx;
      size_t name_len = strlen ((const char *) name);
      if (nrules != 0)
	{
	  elem = seek_collating_symbol_entry (name, name_len);
	  if (symb_table[2 * elem] != 0)
	    {
	      /* We found the entry.  */
	      idx = symb_table[2 * elem + 1];
	      /* Skip the name of collating element name.  */
	      idx += 1 + extra[idx];
	    }
	  else if (symb_table[2 * elem] == 0 && name_len == 1)
	    {
	      /* No valid character, treat it as a normal
		 character.  */
	      bitset_set (sbcset, name[0]);
	      return REG_NOERROR;
	    }
	  else
	    return REG_ECOLLATE;

# ifdef RE_ENABLE_I18N
	  /* Got valid collation sequence, add it as a new entry.  */
	  /* Check the space of the arrays.  */
	  if (*coll_sym_alloc == mbcset->ncoll_syms)
	    {
	      /* Not enough, realloc it.  */
	      /* +1 in case of mbcset->ncoll_syms is 0.  */
	      *coll_sym_alloc = 2 * mbcset->ncoll_syms + 1;
	      /* Use realloc since mbcset->coll_syms is NULL
		 if *alloc == 0.  */
	      mbcset->coll_syms = re_realloc (mbcset->coll_syms, int32_t,
					      *coll_sym_alloc);
	      if (BE (mbcset->coll_syms == NULL, 0))
		return REG_ESPACE;
	    }
	  mbcset->coll_syms[mbcset->ncoll_syms++] = idx;
# endif /* RE_ENABLE_I18N */
	  return REG_NOERROR;
	}
      else
	{
	  if (BE (name_len != 1, 0))
	    return REG_ECOLLATE;
	  else
	    {
	      bitset_set (sbcset, name[0]);
	      return REG_NOERROR;
	    }
	}
    }
#endif

  re_token_t br_token;
  re_bitset_ptr_t sbcset;
#ifdef RE_ENABLE_I18N
  re_charset_t *mbcset;
  int coll_sym_alloc = 0, range_alloc = 0, mbchar_alloc = 0;
  int equiv_class_alloc = 0, char_class_alloc = 0;
#else /* not RE_ENABLE_I18N */
  int non_match = 0;
#endif /* not RE_ENABLE_I18N */
  bin_tree_t *work_tree;
  int token_len, new_idx;
#ifdef _LIBC
  collseqmb = (const unsigned char *)
    _NL_CURRENT (LC_COLLATE, _NL_COLLATE_COLLSEQMB);
  nrules = _NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_NRULES);
  if (nrules)
    {
      /*
      if (MB_CUR_MAX > 1)
      */
	collseqwc = _NL_CURRENT (LC_COLLATE, _NL_COLLATE_COLLSEQWC);
      table_size = _NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_SYMB_HASH_SIZEMB);
      symb_table = (const int32_t *) _NL_CURRENT (LC_COLLATE,
						  _NL_COLLATE_SYMB_TABLEMB);
      extra = (const unsigned char *) _NL_CURRENT (LC_COLLATE,
						   _NL_COLLATE_SYMB_EXTRAMB);
    }
#endif
  sbcset = (re_bitset_ptr_t) calloc (sizeof (unsigned int), BITSET_UINTS);
#ifdef RE_ENABLE_I18N
  mbcset = (re_charset_t *) calloc (sizeof (re_charset_t), 1);
#endif /* RE_ENABLE_I18N */
#ifdef RE_ENABLE_I18N
  if (BE (sbcset == NULL || mbcset == NULL, 0))
#else
  if (BE (sbcset == NULL, 0))
#endif /* RE_ENABLE_I18N */
    {
      *err = REG_ESPACE;
      return NULL;
    }

  token_len = peek_token_bracket (token, regexp, syntax);
  if (BE (token->type == END_OF_RE, 0))
    {
      *err = REG_BADPAT;
      goto parse_bracket_exp_free_return;
    }
  if (token->type == OP_NON_MATCH_LIST)
    {
#ifdef RE_ENABLE_I18N
      int i;
      mbcset->non_match = 1;
#else /* not RE_ENABLE_I18N */
      non_match = 1;
#endif /* not RE_ENABLE_I18N */
      if (syntax & RE_HAT_LISTS_NOT_NEWLINE)
	bitset_set (sbcset, '\0');
      re_string_skip_bytes (regexp, token_len); /* Skip a token.  */
      token_len = peek_token_bracket (token, regexp, syntax);
      if (BE (token->type == END_OF_RE, 0))
	{
	  *err = REG_BADPAT;
	  goto parse_bracket_exp_free_return;
	}
#ifdef RE_ENABLE_I18N
      if (MB_CUR_MAX > 1)
	for (i = 0; i < SBC_MAX; ++i)
	  if (__btowc (i) == WEOF)
	    bitset_set (sbcset, i);
#endif /* RE_ENABLE_I18N */
    }

  /* We treat the first ']' as a normal character.  */
  if (token->type == OP_CLOSE_BRACKET)
    token->type = CHARACTER;

  while (1)
    {
      bracket_elem_t start_elem, end_elem;
      unsigned char start_name_buf[BRACKET_NAME_BUF_SIZE];
      unsigned char end_name_buf[BRACKET_NAME_BUF_SIZE];
      reg_errcode_t ret;
      int token_len2 = 0, is_range_exp = 0;
      re_token_t token2;

      start_elem.opr.name = start_name_buf;
      ret = parse_bracket_element (&start_elem, regexp, token, token_len, dfa,
				   syntax);
      if (BE (ret != REG_NOERROR, 0))
	{
	  *err = ret;
	  goto parse_bracket_exp_free_return;
	}

      token_len = peek_token_bracket (token, regexp, syntax);
      if (BE (token->type == END_OF_RE, 0))
	{
	  *err = REG_BADPAT;
	  goto parse_bracket_exp_free_return;
	}
      if (token->type == OP_CHARSET_RANGE)
	{
	  re_string_skip_bytes (regexp, token_len); /* Skip '-'.  */
	  token_len2 = peek_token_bracket (&token2, regexp, syntax);
	  if (BE (token->type == END_OF_RE, 0))
	    {
	      *err = REG_BADPAT;
	      goto parse_bracket_exp_free_return;
	    }
	  if (token2.type == OP_CLOSE_BRACKET)
	    {
	      /* We treat the last '-' as a normal character.  */
	      re_string_skip_bytes (regexp, -token_len);
	      token->type = CHARACTER;
	    }
	  else
	    is_range_exp = 1;
	}

      if (is_range_exp == 1)
	{
	  end_elem.opr.name = end_name_buf;
	  ret = parse_bracket_element (&end_elem, regexp, &token2, token_len2,
				       dfa, syntax);
	  if (BE (ret != REG_NOERROR, 0))
	    {
	      *err = ret;
	      goto parse_bracket_exp_free_return;
	    }

	  token_len = peek_token_bracket (token, regexp, syntax);
	  if (BE (token->type == END_OF_RE, 0))
	    {
	      *err = REG_BADPAT;
	      goto parse_bracket_exp_free_return;
	    }
	  *err = build_range_exp (sbcset,
#ifdef RE_ENABLE_I18N
				  mbcset, &range_alloc,
#endif /* RE_ENABLE_I18N */
				  &start_elem, &end_elem);
	  if (BE (*err != REG_NOERROR, 0))
	    goto parse_bracket_exp_free_return;
	}
      else
	{
	  switch (start_elem.type)
	    {
	    case SB_CHAR:
	      bitset_set (sbcset, start_elem.opr.ch);
	      break;
#ifdef RE_ENABLE_I18N
	    case MB_CHAR:
	      /* Check whether the array has enough space.  */
	      if (mbchar_alloc == mbcset->nmbchars)
		{
		  /* Not enough, realloc it.  */
		  /* +1 in case of mbcset->nmbchars is 0.  */
		  mbchar_alloc = 2 * mbcset->nmbchars + 1;
		  /* Use realloc since array is NULL if *alloc == 0.  */
		  mbcset->mbchars = re_realloc (mbcset->mbchars, wchar_t,
						mbchar_alloc);
		  if (BE (mbcset->mbchars == NULL, 0))
		    goto parse_bracket_exp_espace;
		}
	      mbcset->mbchars[mbcset->nmbchars++] = start_elem.opr.wch;
	      break;
#endif /* RE_ENABLE_I18N */
	    case EQUIV_CLASS:
	      *err = build_equiv_class (sbcset,
#ifdef RE_ENABLE_I18N
					mbcset, &equiv_class_alloc,
#endif /* RE_ENABLE_I18N */
					start_elem.opr.name);
	      if (BE (*err != REG_NOERROR, 0))
		goto parse_bracket_exp_free_return;
	      break;
	    case COLL_SYM:
	      *err = build_collating_symbol (sbcset,
#ifdef RE_ENABLE_I18N
					     mbcset, &coll_sym_alloc,
#endif /* RE_ENABLE_I18N */
					     start_elem.opr.name);
	      if (BE (*err != REG_NOERROR, 0))
		goto parse_bracket_exp_free_return;
	      break;
	    case CHAR_CLASS:
	      *err = build_charclass (sbcset,
#ifdef RE_ENABLE_I18N
				      mbcset, &char_class_alloc,
#endif /* RE_ENABLE_I18N */
				      start_elem.opr.name, syntax);
	      if (BE (*err != REG_NOERROR, 0))
	       goto parse_bracket_exp_free_return;
	      break;
	    default:
	      assert (0);
	      break;
	    }
	}
      if (token->type == OP_CLOSE_BRACKET)
	break;
    }

  re_string_skip_bytes (regexp, token_len); /* Skip a token.  */

  /* If it is non-matching list.  */
#ifdef RE_ENABLE_I18N
  if (mbcset->non_match)
#else /* not RE_ENABLE_I18N */
  if (non_match)
#endif /* not RE_ENABLE_I18N */
    bitset_not (sbcset);

  /* Build a tree for simple bracket.  */
  br_token.type = SIMPLE_BRACKET;
  br_token.opr.sbcset = sbcset;
  new_idx = re_dfa_add_node (dfa, br_token, 0);
  work_tree = create_tree (NULL, NULL, 0, new_idx);
  if (BE (new_idx == -1 || work_tree == NULL, 0))
    goto parse_bracket_exp_espace;

#ifdef RE_ENABLE_I18N
  if (mbcset->nmbchars || mbcset->ncoll_syms || mbcset->nequiv_classes
      || mbcset->nranges || (MB_CUR_MAX > 1 && (mbcset->nchar_classes
						|| mbcset->non_match)))
    {
      re_token_t alt_token;
      bin_tree_t *mbc_tree;
      /* Build a tree for complex bracket.  */
      br_token.type = COMPLEX_BRACKET;
      br_token.opr.mbcset = mbcset;
      dfa->has_mb_node = 1;
      new_idx = re_dfa_add_node (dfa, br_token, 0);
      mbc_tree = create_tree (NULL, NULL, 0, new_idx);
      if (BE (new_idx == -1 || mbc_tree == NULL, 0))
	goto parse_bracket_exp_espace;
      /* Then join them by ALT node.  */
      dfa->has_plural_match = 1;
      alt_token.type = OP_ALT;
      new_idx = re_dfa_add_node (dfa, alt_token, 0);
      work_tree = create_tree (work_tree, mbc_tree, 0, new_idx);
      if (BE (new_idx != -1 && mbc_tree != NULL, 1))
	return work_tree;
    }
  else
    {
      free_charset (mbcset);
      return work_tree;
    }
#else /* not RE_ENABLE_I18N */
  return work_tree;
#endif /* not RE_ENABLE_I18N */

 parse_bracket_exp_espace:
  *err = REG_ESPACE;
 parse_bracket_exp_free_return:
  re_free (sbcset);
#ifdef RE_ENABLE_I18N
  free_charset (mbcset);
#endif /* RE_ENABLE_I18N */
  return NULL;
}

/* Parse an element in the bracket expression.  */

static reg_errcode_t
parse_bracket_element (elem, regexp, token, token_len, dfa, syntax)
     bracket_elem_t *elem;
     re_string_t *regexp;
     re_token_t *token;
     int token_len;
     re_dfa_t *dfa;
     reg_syntax_t syntax;
{
#ifdef RE_ENABLE_I18N
  int cur_char_size;
  cur_char_size = re_string_char_size_at (regexp, re_string_cur_idx (regexp));
  if (cur_char_size > 1)
    {
      elem->type = MB_CHAR;
      elem->opr.wch = re_string_wchar_at (regexp, re_string_cur_idx (regexp));
      re_string_skip_bytes (regexp, cur_char_size);
      return REG_NOERROR;
    }
#endif /* RE_ENABLE_I18N */
  re_string_skip_bytes (regexp, token_len); /* Skip a token.  */
  if (token->type == OP_OPEN_COLL_ELEM || token->type == OP_OPEN_CHAR_CLASS
      || token->type == OP_OPEN_EQUIV_CLASS)
    return parse_bracket_symbol (elem, regexp, token);
  elem->type = SB_CHAR;
  elem->opr.ch = token->opr.c;
  return REG_NOERROR;
}

/* Parse a bracket symbol in the bracket expression.  Bracket symbols are
   such as [:<character_class>:], [.<collating_element>.], and
   [=<equivalent_class>=].  */

static reg_errcode_t
parse_bracket_symbol (elem, regexp, token)
     bracket_elem_t *elem;
     re_string_t *regexp;
     re_token_t *token;
{
  unsigned char ch, delim = token->opr.c;
  int i = 0;
  for (;; ++i)
    {
      if (re_string_eoi(regexp) || i >= BRACKET_NAME_BUF_SIZE)
	return REG_EBRACK;
      if (token->type == OP_OPEN_CHAR_CLASS)
	ch = re_string_fetch_byte_case (regexp);
      else
	ch = re_string_fetch_byte (regexp);
      if (ch == delim && re_string_peek_byte (regexp, 0) == ']')
	break;
      elem->opr.name[i] = ch;
    }
  re_string_skip_bytes (regexp, 1);
  elem->opr.name[i] = '\0';
  switch (token->type)
    {
    case OP_OPEN_COLL_ELEM:
      elem->type = COLL_SYM;
      break;
    case OP_OPEN_EQUIV_CLASS:
      elem->type = EQUIV_CLASS;
      break;
    case OP_OPEN_CHAR_CLASS:
      elem->type = CHAR_CLASS;
      break;
    default:
      break;
    }
  return REG_NOERROR;
}

  /* Helper function for parse_bracket_exp.
     Build the equivalence class which is represented by NAME.
     The result are written to MBCSET and SBCSET.
     EQUIV_CLASS_ALLOC is the allocated size of mbcset->equiv_classes,
     is a pointer argument sinse we may update it.  */

static reg_errcode_t
#ifdef RE_ENABLE_I18N
build_equiv_class (sbcset, mbcset, equiv_class_alloc, name)
     re_charset_t *mbcset;
     int *equiv_class_alloc;
#else /* not RE_ENABLE_I18N */
build_equiv_class (sbcset, name)
#endif /* not RE_ENABLE_I18N */
     re_bitset_ptr_t sbcset;
     const unsigned char *name;
{
#if defined _LIBC && defined RE_ENABLE_I18N
  uint32_t nrules = _NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_NRULES);
  if (nrules != 0)
    {
      const int32_t *table, *indirect;
      const unsigned char *weights, *extra, *cp;
      unsigned char char_buf[2];
      int32_t idx1, idx2;
      unsigned int ch;
      size_t len;
      /* This #include defines a local function!  */
# include <locale/weight.h>
      /* Calculate the index for equivalence class.  */
      cp = name;
      table = (const int32_t *) _NL_CURRENT (LC_COLLATE, _NL_COLLATE_TABLEMB);
      weights = (const unsigned char *) _NL_CURRENT (LC_COLLATE,
					       _NL_COLLATE_WEIGHTMB);
      extra = (const unsigned char *) _NL_CURRENT (LC_COLLATE,
						   _NL_COLLATE_EXTRAMB);
      indirect = (const int32_t *) _NL_CURRENT (LC_COLLATE,
						_NL_COLLATE_INDIRECTMB);
      idx1 = findidx (&cp);
      if (BE (idx1 == 0 || cp < name + strlen ((const char *) name), 0))
	/* This isn't a valid character.  */
	return REG_ECOLLATE;

      /* Build single byte matcing table for this equivalence class.  */
      char_buf[1] = (unsigned char) '\0';
      len = weights[idx1];
      for (ch = 0; ch < SBC_MAX; ++ch)
	{
	  char_buf[0] = ch;
	  cp = char_buf;
	  idx2 = findidx (&cp);
/*
	  idx2 = table[ch];
*/
	  if (idx2 == 0)
	    /* This isn't a valid character.  */
	    continue;
	  if (len == weights[idx2])
	    {
	      int cnt = 0;
	      while (cnt <= len &&
		     weights[idx1 + 1 + cnt] == weights[idx2 + 1 + cnt])
		++cnt;

	      if (cnt > len)
		bitset_set (sbcset, ch);
	    }
	}
      /* Check whether the array has enough space.  */
      if (*equiv_class_alloc == mbcset->nequiv_classes)
	{
	  /* Not enough, realloc it.  */
	  /* +1 in case of mbcset->nequiv_classes is 0.  */
	  *equiv_class_alloc = 2 * mbcset->nequiv_classes + 1;
	  /* Use realloc since the array is NULL if *alloc == 0.  */
	  mbcset->equiv_classes = re_realloc (mbcset->equiv_classes, int32_t,
					      *equiv_class_alloc);
	  if (BE (mbcset->equiv_classes == NULL, 0))
	    return REG_ESPACE;
	}
      mbcset->equiv_classes[mbcset->nequiv_classes++] = idx1;
    }
  else
#endif /* _LIBC && RE_ENABLE_I18N */
    {
      if (BE (strlen ((const char *) name) != 1, 0))
	return REG_ECOLLATE;
      bitset_set (sbcset, *name);
    }
  return REG_NOERROR;
}

  /* Helper function for parse_bracket_exp.
     Build the character class which is represented by NAME.
     The result are written to MBCSET and SBCSET.
     CHAR_CLASS_ALLOC is the allocated size of mbcset->char_classes,
     is a pointer argument sinse we may update it.  */

static reg_errcode_t
#ifdef RE_ENABLE_I18N
build_charclass (sbcset, mbcset, char_class_alloc, class_name, syntax)
     re_charset_t *mbcset;
     int *char_class_alloc;
#else /* not RE_ENABLE_I18N */
build_charclass (sbcset, class_name, syntax)
#endif /* not RE_ENABLE_I18N */
     re_bitset_ptr_t sbcset;
     const unsigned char *class_name;
     reg_syntax_t syntax;
{
  int i;
  const char *name = (const char *) class_name;

  /* In case of REG_ICASE "upper" and "lower" match the both of
     upper and lower cases.  */
  if ((syntax & RE_ICASE)
      && (strcmp (name, "upper") == 0 || strcmp (name, "lower") == 0))
    name = "alpha";

#ifdef RE_ENABLE_I18N
  /* Check the space of the arrays.  */
  if (*char_class_alloc == mbcset->nchar_classes)
    {
      /* Not enough, realloc it.  */
      /* +1 in case of mbcset->nchar_classes is 0.  */
      *char_class_alloc = 2 * mbcset->nchar_classes + 1;
      /* Use realloc since array is NULL if *alloc == 0.  */
      mbcset->char_classes = re_realloc (mbcset->char_classes, wctype_t,
					 *char_class_alloc);
      if (BE (mbcset->char_classes == NULL, 0))
	return REG_ESPACE;
    }
  mbcset->char_classes[mbcset->nchar_classes++] = __wctype (name);
#endif /* RE_ENABLE_I18N */

#define BUILD_CHARCLASS_LOOP(ctype_func)\
    for (i = 0; i < SBC_MAX; ++i)	\
      {					\
	if (ctype_func (i))		\
	  bitset_set (sbcset, i);	\
      }

  if (strcmp (name, "alnum") == 0)
    BUILD_CHARCLASS_LOOP (isalnum)
  else if (strcmp (name, "cntrl") == 0)
    BUILD_CHARCLASS_LOOP (iscntrl)
  else if (strcmp (name, "lower") == 0)
    BUILD_CHARCLASS_LOOP (islower)
  else if (strcmp (name, "space") == 0)
    BUILD_CHARCLASS_LOOP (isspace)
  else if (strcmp (name, "alpha") == 0)
    BUILD_CHARCLASS_LOOP (isalpha)
  else if (strcmp (name, "digit") == 0)
    BUILD_CHARCLASS_LOOP (isdigit)
  else if (strcmp (name, "print") == 0)
    BUILD_CHARCLASS_LOOP (isprint)
  else if (strcmp (name, "upper") == 0)
    BUILD_CHARCLASS_LOOP (isupper)
  else if (strcmp (name, "blank") == 0)
    BUILD_CHARCLASS_LOOP (isblank)
  else if (strcmp (name, "graph") == 0)
    BUILD_CHARCLASS_LOOP (isgraph)
  else if (strcmp (name, "punct") == 0)
    BUILD_CHARCLASS_LOOP (ispunct)
  else if (strcmp (name, "xdigit") == 0)
    BUILD_CHARCLASS_LOOP (isxdigit)
  else
    return REG_ECTYPE;

  return REG_NOERROR;
}

static bin_tree_t *
build_word_op (dfa, not, err)
     re_dfa_t *dfa;
     int not;
     reg_errcode_t *err;
{
  re_bitset_ptr_t sbcset;
#ifdef RE_ENABLE_I18N
  re_charset_t *mbcset;
  int alloc = 0;
#else /* not RE_ENABLE_I18N */
  int non_match = 0;
#endif /* not RE_ENABLE_I18N */
  reg_errcode_t ret;
  re_token_t br_token;
  bin_tree_t *tree;
  int new_idx;

  sbcset = (re_bitset_ptr_t) calloc (sizeof (unsigned int), BITSET_UINTS);
#ifdef RE_ENABLE_I18N
  mbcset = (re_charset_t *) calloc (sizeof (re_charset_t), 1);
#endif /* RE_ENABLE_I18N */

#ifdef RE_ENABLE_I18N
  if (BE (sbcset == NULL || mbcset == NULL, 0))
#else /* not RE_ENABLE_I18N */
  if (BE (sbcset == NULL, 0))
#endif /* not RE_ENABLE_I18N */
    {
      *err = REG_ESPACE;
      return NULL;
    }

  if (not)
    {
#ifdef RE_ENABLE_I18N
      int i;
      /*
      if (syntax & RE_HAT_LISTS_NOT_NEWLINE)
	bitset_set(cset->sbcset, '\0');
      */
      mbcset->non_match = 1;
      if (MB_CUR_MAX > 1)
	for (i = 0; i < SBC_MAX; ++i)
	  if (__btowc (i) == WEOF)
	    bitset_set (sbcset, i);
#else /* not RE_ENABLE_I18N */
      non_match = 1;
#endif /* not RE_ENABLE_I18N */
    }

  /* We don't care the syntax in this case.  */
  ret = build_charclass (sbcset,
#ifdef RE_ENABLE_I18N
			 mbcset, &alloc,
#endif /* RE_ENABLE_I18N */
			 (const unsigned char *) "alpha", 0);

  if (BE (ret != REG_NOERROR, 0))
    {
      re_free (sbcset);
#ifdef RE_ENABLE_I18N
      free_charset (mbcset);
#endif /* RE_ENABLE_I18N */
      *err = ret;
      return NULL;
    }
  /* \w match '_' also.  */
  bitset_set (sbcset, '_');

  /* If it is non-matching list.  */
#ifdef RE_ENABLE_I18N
  if (mbcset->non_match)
#else /* not RE_ENABLE_I18N */
  if (non_match)
#endif /* not RE_ENABLE_I18N */
    bitset_not (sbcset);

  /* Build a tree for simple bracket.  */
  br_token.type = SIMPLE_BRACKET;
  br_token.opr.sbcset = sbcset;
  new_idx = re_dfa_add_node (dfa, br_token, 0);
  tree = create_tree (NULL, NULL, 0, new_idx);
  if (BE (new_idx == -1 || tree == NULL, 0))
    goto build_word_op_espace;

#ifdef RE_ENABLE_I18N
  if (MB_CUR_MAX > 1)
    {
      re_token_t alt_token;
      bin_tree_t *mbc_tree;
      /* Build a tree for complex bracket.  */
      br_token.type = COMPLEX_BRACKET;
      br_token.opr.mbcset = mbcset;
      dfa->has_mb_node = 1;
      new_idx = re_dfa_add_node (dfa, br_token, 0);
      mbc_tree = create_tree (NULL, NULL, 0, new_idx);
      if (BE (new_idx == -1 || mbc_tree == NULL, 0))
	goto build_word_op_espace;
      /* Then join them by ALT node.  */
      alt_token.type = OP_ALT;
      new_idx = re_dfa_add_node (dfa, alt_token, 0);
      tree = create_tree (tree, mbc_tree, 0, new_idx);
      if (BE (new_idx != -1 && mbc_tree != NULL, 1))
	return tree;
    }
  else
    {
      free_charset (mbcset);
      return tree;
    }
#else /* not RE_ENABLE_I18N */
  return tree;
#endif /* not RE_ENABLE_I18N */

 build_word_op_espace:
  re_free (sbcset);
#ifdef RE_ENABLE_I18N
  free_charset (mbcset);
#endif /* RE_ENABLE_I18N */
  *err = REG_ESPACE;
  return NULL;
}

/* This is intended for the expressions like "a{1,3}".
   Fetch a number from `input', and return the number.
   Return -1, if the number field is empty like "{,1}".
   Return -2, If an error is occured.  */

static int
fetch_number (input, token, syntax)
     re_string_t *input;
     re_token_t *token;
     reg_syntax_t syntax;
{
  int num = -1;
  unsigned char c;
  while (1)
    {
      *token = fetch_token (input, syntax);
      c = token->opr.c;
      if (BE (token->type == END_OF_RE, 0))
	return -2;
      if (token->type == OP_CLOSE_DUP_NUM || c == ',')
	break;
      num = ((token->type != CHARACTER || c < '0' || '9' < c || num == -2)
	     ? -2 : ((num == -1) ? c - '0' : num * 10 + c - '0'));
      num = (num > RE_DUP_MAX) ? -2 : num;
    }
  return num;
}

#ifdef RE_ENABLE_I18N
static void
free_charset (re_charset_t *cset)
{
  re_free (cset->mbchars);
# ifdef _LIBC
  re_free (cset->coll_syms);
  re_free (cset->equiv_classes);
  re_free (cset->range_starts);
  re_free (cset->range_ends);
# endif
  re_free (cset->char_classes);
  re_free (cset);
}
#endif /* RE_ENABLE_I18N */

/* Functions for binary tree operation.  */

/* Create a node of tree.
   Note: This function automatically free left and right if malloc fails.  */

static bin_tree_t *
create_tree (left, right, type, deps_index)
     bin_tree_t *left;
     bin_tree_t *right;
     re_token_type_t type;
     int deps_index;
{
  bin_tree_t *tree;
  tree = re_malloc (bin_tree_t, 1);
  if (BE (tree == NULL, 0))
    {
      free_bin_tree (left);
      free_bin_tree (right);
      return NULL;
    }
  tree->parent = NULL;
  tree->left = left;
  tree->right = right;
  tree->type = type;
  tree->node_idx = deps_index;
  tree->first = -1;
  tree->next = -1;
  re_node_set_init_empty (&tree->eclosure);

  if (left != NULL)
    left->parent = tree;
  if (right != NULL)
    right->parent = tree;
  return tree;
}

/* Free the sub tree pointed by TREE.  */

static void
free_bin_tree (tree)
     bin_tree_t *tree;
{
  if (tree == NULL)
    return;
  /*re_node_set_free (&tree->eclosure);*/
  free_bin_tree (tree->left);
  free_bin_tree (tree->right);
  re_free (tree);
}

/* Duplicate the node SRC, and return new node.  */

static bin_tree_t *
duplicate_tree (src, dfa)
     const bin_tree_t *src;
     re_dfa_t *dfa;
{
  bin_tree_t *left = NULL, *right = NULL, *new_tree;
  int new_node_idx;
  /* Since node indies must be according to Post-order of the tree,
     we must duplicate the left at first.  */
  if (src->left != NULL)
    {
      left = duplicate_tree (src->left, dfa);
      if (left == NULL)
	return NULL;
    }

  /* Secondaly, duplicate the right.  */
  if (src->right != NULL)
    {
      right = duplicate_tree (src->right, dfa);
      if (right == NULL)
	{
	  free_bin_tree (left);
	  return NULL;
	}
    }

  /* At last, duplicate itself.  */
  if (src->type == NON_TYPE)
    {
      new_node_idx = re_dfa_add_node (dfa, dfa->nodes[src->node_idx], 0);
      dfa->nodes[new_node_idx].duplicated = 1;
      if (BE (new_node_idx == -1, 0))
	{
	  free_bin_tree (left);
	  free_bin_tree (right);
	  return NULL;
	}
    }
  else
    new_node_idx = src->type;

  new_tree = create_tree (left, right, src->type, new_node_idx);
  if (BE (new_tree == NULL, 0))
    {
      free_bin_tree (left);
      free_bin_tree (right);
    }
  return new_tree;
}
