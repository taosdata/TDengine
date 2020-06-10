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

#ifndef _REGEX_INTERNAL_H
#define _REGEX_INTERNAL_H 1

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined HAVE_LOCALE_H || defined _LIBC
# include <locale.h>
#endif
#if defined HAVE_WCHAR_H || defined _LIBC
# include <wchar.h>
#endif /* HAVE_WCHAR_H || _LIBC */
#if defined HAVE_WCTYPE_H || defined _LIBC
# include <wctype.h>
#endif /* HAVE_WCTYPE_H || _LIBC */

/* In case that the system doesn't have isblank().  */
#if !defined _LIBC && !defined HAVE_ISBLANK && !defined isblank
# define isblank(ch) ((ch) == ' ' || (ch) == '\t')
#endif

#ifdef _LIBC
# ifndef _RE_DEFINE_LOCALE_FUNCTIONS
#  define _RE_DEFINE_LOCALE_FUNCTIONS 1
#   include <locale/localeinfo.h>
#   include <locale/elem-hash.h>
#   include <locale/coll-lookup.h>
# endif
#endif

/* This is for other GNU distributions with internationalized messages.  */
#if HAVE_LIBINTL_H || defined _LIBC
# include <libintl.h>
# ifdef _LIBC
#  undef gettext
#  define gettext(msgid) \
  INTUSE(__dcgettext) (INTUSE(_libc_intl_domainname), msgid, LC_MESSAGES)
# endif
#else
# define gettext(msgid) (msgid)
#endif

#ifndef gettext_noop
/* This define is so xgettext can find the internationalizable
   strings.  */
# define gettext_noop(String) String
#endif

#if (defined MB_CUR_MAX && HAVE_LOCALE_H && HAVE_WCTYPE_H && HAVE_WCHAR_H && HAVE_WCRTOMB && HAVE_MBRTOWC && HAVE_WCSCOLL) || _LIBC
# define RE_ENABLE_I18N
#endif

#if __GNUC__ >= 3
# define BE(expr, val) __builtin_expect (expr, val)
#else
# define BE(expr, val) (expr)
# define inline
#endif

/* Number of bits in a byte.  */
#define BYTE_BITS 8
/* Number of single byte character.  */
#define SBC_MAX 256

#define COLL_ELEM_LEN_MAX 8

/* The character which represents newline.  */
#define NEWLINE_CHAR '\n'
#define WIDE_NEWLINE_CHAR L'\n'

/* Rename to standard API for using out of glibc.  */
#ifndef _LIBC
# define __wctype wctype
# define __iswctype iswctype
# define __btowc btowc
# define __mempcpy mempcpy
# define __wcrtomb wcrtomb
# define attribute_hidden
#endif /* not _LIBC */

extern const char __re_error_msgid[] attribute_hidden;
extern const size_t __re_error_msgid_idx[] attribute_hidden;

/* Number of bits in an unsinged int.  */
#define UINT_BITS (sizeof (unsigned int) * BYTE_BITS)
/* Number of unsigned int in an bit_set.  */
#define BITSET_UINTS ((SBC_MAX + UINT_BITS - 1) / UINT_BITS)
typedef unsigned int bitset[BITSET_UINTS];
typedef unsigned int *re_bitset_ptr_t;

#define bitset_set(set,i) (set[i / UINT_BITS] |= 1 << i % UINT_BITS)
#define bitset_clear(set,i) (set[i / UINT_BITS] &= ~(1 << i % UINT_BITS))
#define bitset_contain(set,i) (set[i / UINT_BITS] & (1 << i % UINT_BITS))
#define bitset_empty(set) memset (set, 0, sizeof (unsigned int) * BITSET_UINTS)
#define bitset_set_all(set) \
  memset (set, 255, sizeof (unsigned int) * BITSET_UINTS)
#define bitset_copy(dest,src) \
  memcpy (dest, src, sizeof (unsigned int) * BITSET_UINTS)
static inline void bitset_not (bitset set);
static inline void bitset_merge (bitset dest, const bitset src);
static inline void bitset_not_merge (bitset dest, const bitset src);

#define PREV_WORD_CONSTRAINT 0x0001
#define PREV_NOTWORD_CONSTRAINT 0x0002
#define NEXT_WORD_CONSTRAINT 0x0004
#define NEXT_NOTWORD_CONSTRAINT 0x0008
#define PREV_NEWLINE_CONSTRAINT 0x0010
#define NEXT_NEWLINE_CONSTRAINT 0x0020
#define PREV_BEGBUF_CONSTRAINT 0x0040
#define NEXT_ENDBUF_CONSTRAINT 0x0080
#define DUMMY_CONSTRAINT 0x0100

typedef enum
{
  INSIDE_WORD = PREV_WORD_CONSTRAINT | NEXT_WORD_CONSTRAINT,
  WORD_FIRST = PREV_NOTWORD_CONSTRAINT | NEXT_WORD_CONSTRAINT,
  WORD_LAST = PREV_WORD_CONSTRAINT | NEXT_NOTWORD_CONSTRAINT,
  LINE_FIRST = PREV_NEWLINE_CONSTRAINT,
  LINE_LAST = NEXT_NEWLINE_CONSTRAINT,
  BUF_FIRST = PREV_BEGBUF_CONSTRAINT,
  BUF_LAST = NEXT_ENDBUF_CONSTRAINT,
  WORD_DELIM = DUMMY_CONSTRAINT
} re_context_type;

typedef struct
{
  int alloc;
  int nelem;
  int *elems;
} re_node_set;

typedef enum
{
  NON_TYPE = 0,

  /* Token type, these are used only by token.  */
  OP_OPEN_BRACKET,
  OP_CLOSE_BRACKET,
  OP_CHARSET_RANGE,
  OP_OPEN_DUP_NUM,
  OP_CLOSE_DUP_NUM,
  OP_NON_MATCH_LIST,
  OP_OPEN_COLL_ELEM,
  OP_CLOSE_COLL_ELEM,
  OP_OPEN_EQUIV_CLASS,
  OP_CLOSE_EQUIV_CLASS,
  OP_OPEN_CHAR_CLASS,
  OP_CLOSE_CHAR_CLASS,
  OP_WORD,
  OP_NOTWORD,
  BACK_SLASH,

  /* Tree type, these are used only by tree. */
  CONCAT,
  ALT,
  SUBEXP,
  SIMPLE_BRACKET,
#ifdef RE_ENABLE_I18N
  COMPLEX_BRACKET,
#endif /* RE_ENABLE_I18N */

  /* Node type, These are used by token, node, tree.  */
  OP_OPEN_SUBEXP,
  OP_CLOSE_SUBEXP,
  OP_PERIOD,
  CHARACTER,
  END_OF_RE,
  OP_ALT,
  OP_DUP_ASTERISK,
  OP_DUP_PLUS,
  OP_DUP_QUESTION,
  OP_BACK_REF,
  ANCHOR,

  /* Dummy marker.  */
  END_OF_RE_TOKEN_T
} re_token_type_t;

#ifdef RE_ENABLE_I18N
typedef struct
{
  /* Multibyte characters.  */
  wchar_t *mbchars;

  /* Collating symbols.  */
# ifdef _LIBC
  int32_t *coll_syms;
# endif

  /* Equivalence classes. */
# ifdef _LIBC
  int32_t *equiv_classes;
# endif

  /* Range expressions. */
# ifdef _LIBC
  uint32_t *range_starts;
  uint32_t *range_ends;
# else /* not _LIBC */
  wchar_t *range_starts;
  wchar_t *range_ends;
# endif /* not _LIBC */

  /* Character classes. */
  wctype_t *char_classes;

  /* If this character set is the non-matching list.  */
  unsigned int non_match : 1;

  /* # of multibyte characters.  */
  int nmbchars;

  /* # of collating symbols.  */
  int ncoll_syms;

  /* # of equivalence classes. */
  int nequiv_classes;

  /* # of range expressions. */
  int nranges;

  /* # of character classes. */
  int nchar_classes;
} re_charset_t;
#endif /* RE_ENABLE_I18N */

typedef struct
{
  union
  {
    unsigned char c;		/* for CHARACTER */
    re_bitset_ptr_t sbcset;	/* for SIMPLE_BRACKET */
#ifdef RE_ENABLE_I18N
    re_charset_t *mbcset;	/* for COMPLEX_BRACKET */
#endif /* RE_ENABLE_I18N */
    int idx;			/* for BACK_REF */
    re_context_type ctx_type;	/* for ANCHOR */
  } opr;
#if __GNUC__ >= 2
  re_token_type_t type : 8;
#else
  re_token_type_t type;
#endif
  unsigned int constraint : 10;	/* context constraint */
  unsigned int duplicated : 1;
#ifdef RE_ENABLE_I18N
  unsigned int mb_partial : 1;
#endif
} re_token_t;

#define IS_EPSILON_NODE(type) \
  ((type) == OP_ALT || (type) == OP_DUP_ASTERISK || (type) == OP_DUP_PLUS \
   || (type) == OP_DUP_QUESTION || (type) == ANCHOR \
   || (type) == OP_OPEN_SUBEXP || (type) == OP_CLOSE_SUBEXP)

#define ACCEPT_MB_NODE(type) \
  ((type) == COMPLEX_BRACKET || (type) == OP_PERIOD)

struct re_string_t
{
  /* Indicate the raw buffer which is the original string passed as an
     argument of regexec(), re_search(), etc..  */
  const unsigned char *raw_mbs;
  /* Store the multibyte string.  In case of "case insensitive mode" like
     REG_ICASE, upper cases of the string are stored, otherwise MBS points
     the same address that RAW_MBS points.  */
  unsigned char *mbs;
  /* Store the case sensitive multibyte string.  In case of
     "case insensitive mode", the original string are stored,
     otherwise MBS_CASE points the same address that MBS points.  */
  unsigned char *mbs_case;
#ifdef RE_ENABLE_I18N
  /* Store the wide character string which is corresponding to MBS.  */
  wint_t *wcs;
  mbstate_t cur_state;
#endif
  /* Index in RAW_MBS.  Each character mbs[i] corresponds to
     raw_mbs[raw_mbs_idx + i].  */
  int raw_mbs_idx;
  /* The length of the valid characters in the buffers.  */
  int valid_len;
  /* The length of the buffers MBS, MBS_CASE, and WCS.  */
  int bufs_len;
  /* The index in MBS, which is updated by re_string_fetch_byte.  */
  int cur_idx;
  /* This is length_of_RAW_MBS - RAW_MBS_IDX.  */
  int len;
  /* End of the buffer may be shorter than its length in the cases such
     as re_match_2, re_search_2.  Then, we use STOP for end of the buffer
     instead of LEN.  */
  int stop;

  /* The context of mbs[0].  We store the context independently, since
     the context of mbs[0] may be different from raw_mbs[0], which is
     the beginning of the input string.  */
  unsigned int tip_context;
  /* The translation passed as a part of an argument of re_compile_pattern.  */
  RE_TRANSLATE_TYPE trans;
  /* 1 if REG_ICASE.  */
  unsigned int icase : 1;
};
typedef struct re_string_t re_string_t;
/* In case of REG_ICASE, we allocate the buffer dynamically for mbs.  */
#define MBS_ALLOCATED(pstr) (pstr->icase)
/* In case that we need translation, we allocate the buffer dynamically
   for mbs_case.  Note that mbs == mbs_case if not REG_ICASE.  */
#define MBS_CASE_ALLOCATED(pstr) (pstr->trans != NULL)


static reg_errcode_t re_string_allocate (re_string_t *pstr, const char *str,
					 int len, int init_len,
					 RE_TRANSLATE_TYPE trans, int icase);
static reg_errcode_t re_string_construct (re_string_t *pstr, const char *str,
					  int len, RE_TRANSLATE_TYPE trans,
					  int icase);
static reg_errcode_t re_string_reconstruct (re_string_t *pstr, int idx,
					    int eflags, int newline);
static reg_errcode_t re_string_realloc_buffers (re_string_t *pstr,
						int new_buf_len);
#ifdef RE_ENABLE_I18N
static void build_wcs_buffer (re_string_t *pstr);
static void build_wcs_upper_buffer (re_string_t *pstr);
#endif /* RE_ENABLE_I18N */
static void build_upper_buffer (re_string_t *pstr);
static void re_string_translate_buffer (re_string_t *pstr);
static void re_string_destruct (re_string_t *pstr);
#ifdef RE_ENABLE_I18N
static int re_string_elem_size_at (const re_string_t *pstr, int idx);
static inline int re_string_char_size_at (const re_string_t *pstr, int idx);
static inline wint_t re_string_wchar_at (const re_string_t *pstr, int idx);
#endif /* RE_ENABLE_I18N */
static unsigned int re_string_context_at (const re_string_t *input, int idx,
					  int eflags, int newline_anchor);
#define re_string_peek_byte(pstr, offset) \
  ((pstr)->mbs[(pstr)->cur_idx + offset])
#define re_string_peek_byte_case(pstr, offset) \
  ((pstr)->mbs_case[(pstr)->cur_idx + offset])
#define re_string_fetch_byte(pstr) \
  ((pstr)->mbs[(pstr)->cur_idx++])
#define re_string_fetch_byte_case(pstr) \
  ((pstr)->mbs_case[(pstr)->cur_idx++])
#define re_string_first_byte(pstr, idx) \
  ((idx) == (pstr)->len || (pstr)->wcs[idx] != WEOF)
#define re_string_is_single_byte_char(pstr, idx) \
  ((pstr)->wcs[idx] != WEOF && ((pstr)->len == (idx) \
				|| (pstr)->wcs[(idx) + 1] != WEOF))
#define re_string_eoi(pstr) ((pstr)->stop <= (pstr)->cur_idx)
#define re_string_cur_idx(pstr) ((pstr)->cur_idx)
#define re_string_get_buffer(pstr) ((pstr)->mbs)
#define re_string_length(pstr) ((pstr)->len)
#define re_string_byte_at(pstr,idx) ((pstr)->mbs[idx])
#define re_string_skip_bytes(pstr,idx) ((pstr)->cur_idx += (idx))
#define re_string_set_index(pstr,idx) ((pstr)->cur_idx = (idx))

#define re_malloc(t,n) ((t *) malloc ((n) * sizeof (t)))
#define re_realloc(p,t,n) ((t *) realloc (p, (n) * sizeof (t)))
#define re_free(p) free (p)

struct bin_tree_t
{
  struct bin_tree_t *parent;
  struct bin_tree_t *left;
  struct bin_tree_t *right;

  /* `node_idx' is the index in dfa->nodes, if `type' == 0.
     Otherwise `type' indicate the type of this node.  */
  re_token_type_t type;
  int node_idx;

  int first;
  int next;
  re_node_set eclosure;
};
typedef struct bin_tree_t bin_tree_t;


#define CONTEXT_WORD 1
#define CONTEXT_NEWLINE (CONTEXT_WORD << 1)
#define CONTEXT_BEGBUF (CONTEXT_NEWLINE << 1)
#define CONTEXT_ENDBUF (CONTEXT_BEGBUF << 1)

#define IS_WORD_CONTEXT(c) ((c) & CONTEXT_WORD)
#define IS_NEWLINE_CONTEXT(c) ((c) & CONTEXT_NEWLINE)
#define IS_BEGBUF_CONTEXT(c) ((c) & CONTEXT_BEGBUF)
#define IS_ENDBUF_CONTEXT(c) ((c) & CONTEXT_ENDBUF)
#define IS_ORDINARY_CONTEXT(c) ((c) == 0)

#define IS_WORD_CHAR(ch) (isalnum (ch) || (ch) == '_')
#define IS_NEWLINE(ch) ((ch) == NEWLINE_CHAR)
#define IS_WIDE_WORD_CHAR(ch) (iswalnum (ch) || (ch) == L'_')
#define IS_WIDE_NEWLINE(ch) ((ch) == WIDE_NEWLINE_CHAR)

#define NOT_SATISFY_PREV_CONSTRAINT(constraint,context) \
 ((((constraint) & PREV_WORD_CONSTRAINT) && !IS_WORD_CONTEXT (context)) \
  || ((constraint & PREV_NOTWORD_CONSTRAINT) && IS_WORD_CONTEXT (context)) \
  || ((constraint & PREV_NEWLINE_CONSTRAINT) && !IS_NEWLINE_CONTEXT (context))\
  || ((constraint & PREV_BEGBUF_CONSTRAINT) && !IS_BEGBUF_CONTEXT (context)))

#define NOT_SATISFY_NEXT_CONSTRAINT(constraint,context) \
 ((((constraint) & NEXT_WORD_CONSTRAINT) && !IS_WORD_CONTEXT (context)) \
  || (((constraint) & NEXT_NOTWORD_CONSTRAINT) && IS_WORD_CONTEXT (context)) \
  || (((constraint) & NEXT_NEWLINE_CONSTRAINT) && !IS_NEWLINE_CONTEXT (context)) \
  || (((constraint) & NEXT_ENDBUF_CONSTRAINT) && !IS_ENDBUF_CONTEXT (context)))

struct re_dfastate_t
{
  unsigned int hash;
  re_node_set nodes;
  re_node_set *entrance_nodes;
  struct re_dfastate_t **trtable;
  struct re_dfastate_t **trtable_search;
  /* If this state is a special state.
     A state is a special state if the state is the halt state, or
     a anchor.  */
  unsigned int context : 2;
  unsigned int halt : 1;
  /* If this state can accept `multi byte'.
     Note that we refer to multibyte characters, and multi character
     collating elements as `multi byte'.  */
  unsigned int accept_mb : 1;
  /* If this state has backreference node(s).  */
  unsigned int has_backref : 1;
  unsigned int has_constraint : 1;
};
typedef struct re_dfastate_t re_dfastate_t;

typedef struct
{
  /* start <= node < end  */
  int start;
  int end;
} re_subexp_t;

struct re_state_table_entry
{
  int num;
  int alloc;
  re_dfastate_t **array;
};

/* Array type used in re_sub_match_last_t and re_sub_match_top_t.  */

typedef struct
{
  int next_idx;
  int alloc;
  re_dfastate_t **array;
} state_array_t;

/* Store information about the node NODE whose type is OP_CLOSE_SUBEXP.  */

typedef struct
{
  int node;
  int str_idx; /* The position NODE match at.  */
  state_array_t path;
} re_sub_match_last_t;

/* Store information about the node NODE whose type is OP_OPEN_SUBEXP.
   And information about the node, whose type is OP_CLOSE_SUBEXP,
   corresponding to NODE is stored in LASTS.  */

typedef struct
{
  int str_idx;
  int node;
  int next_last_offset;
  state_array_t *path;
  int alasts; /* Allocation size of LASTS.  */
  int nlasts; /* The number of LASTS.  */
  re_sub_match_last_t **lasts;
} re_sub_match_top_t;

struct re_backref_cache_entry
{
  int node;
  int str_idx;
  int subexp_from;
  int subexp_to;
  int flag;
};

typedef struct
{
  /* EFLAGS of the argument of regexec.  */
  int eflags;
  /* Where the matching ends.  */
  int match_last;
  int last_node;
  /* The string object corresponding to the input string.  */
  re_string_t *input;
  /* The state log used by the matcher.  */
  re_dfastate_t **state_log;
  int state_log_top;
  /* Back reference cache.  */
  int nbkref_ents;
  int abkref_ents;
  struct re_backref_cache_entry *bkref_ents;
  int max_mb_elem_len;
  int nsub_tops;
  int asub_tops;
  re_sub_match_top_t **sub_tops;
} re_match_context_t;

typedef struct
{
  int cur_bkref;
  int cls_subexp_idx;

  re_dfastate_t **sifted_states;
  re_dfastate_t **limited_states;

  re_node_set limits;

  int last_node;
  int last_str_idx;
  int check_subexp;
} re_sift_context_t;

struct re_fail_stack_ent_t
{
  int idx;
  int node;
  regmatch_t *regs;
  re_node_set eps_via_nodes;
};

struct re_fail_stack_t
{
  int num;
  int alloc;
  struct re_fail_stack_ent_t *stack;
};

struct re_dfa_t
{
  re_bitset_ptr_t word_char;

  /* number of subexpressions `re_nsub' is in regex_t.  */
  int subexps_alloc;
  re_subexp_t *subexps;

  re_token_t *nodes;
  int nodes_alloc;
  int nodes_len;
  bin_tree_t *str_tree;
  int *nexts;
  int *org_indices;
  re_node_set *edests;
  re_node_set *eclosures;
  re_node_set *inveclosures;
  struct re_state_table_entry *state_table;
  unsigned int state_hash_mask;
  re_dfastate_t *init_state;
  re_dfastate_t *init_state_word;
  re_dfastate_t *init_state_nl;
  re_dfastate_t *init_state_begbuf;
  int states_alloc;
  int init_node;
  int nbackref; /* The number of backreference in this dfa.  */
  /* Bitmap expressing which backreference is used.  */
  unsigned int used_bkref_map;
#ifdef DEBUG
  char* re_str;
#endif
  unsigned int has_plural_match : 1;
  /* If this dfa has "multibyte node", which is a backreference or
     a node which can accept multibyte character or multi character
     collating element.  */
  unsigned int has_mb_node : 1;
};
typedef struct re_dfa_t re_dfa_t;

static reg_errcode_t re_node_set_alloc (re_node_set *set, int size);
static reg_errcode_t re_node_set_init_1 (re_node_set *set, int elem);
static reg_errcode_t re_node_set_init_2 (re_node_set *set, int elem1,
					 int elem2);
#define re_node_set_init_empty(set) memset (set, '\0', sizeof (re_node_set))
static reg_errcode_t re_node_set_init_copy (re_node_set *dest,
					    const re_node_set *src);
static reg_errcode_t re_node_set_add_intersect (re_node_set *dest,
						const re_node_set *src1,
						const re_node_set *src2);
static reg_errcode_t re_node_set_init_union (re_node_set *dest,
					     const re_node_set *src1,
					     const re_node_set *src2);
static reg_errcode_t re_node_set_merge (re_node_set *dest,
					const re_node_set *src);
static int re_node_set_insert (re_node_set *set, int elem);
static int re_node_set_compare (const re_node_set *set1,
				const re_node_set *set2);
static int re_node_set_contains (const re_node_set *set, int elem);
static void re_node_set_remove_at (re_node_set *set, int idx);
#define re_node_set_remove(set,id) \
  (re_node_set_remove_at (set, re_node_set_contains (set, id) - 1))
#define re_node_set_empty(p) ((p)->nelem = 0)
#define re_node_set_free(set) re_free ((set)->elems)
static int re_dfa_add_node (re_dfa_t *dfa, re_token_t token, int mode);
static re_dfastate_t *re_acquire_state (reg_errcode_t *err, re_dfa_t *dfa,
					const re_node_set *nodes);
static re_dfastate_t *re_acquire_state_context (reg_errcode_t *err,
						re_dfa_t *dfa,
						const re_node_set *nodes,
						unsigned int context);
static void free_state (re_dfastate_t *state);


typedef enum
{
  SB_CHAR,
  MB_CHAR,
  EQUIV_CLASS,
  COLL_SYM,
  CHAR_CLASS
} bracket_elem_type;

typedef struct
{
  bracket_elem_type type;
  union
  {
    unsigned char ch;
    unsigned char *name;
    wchar_t wch;
  } opr;
} bracket_elem_t;


/* Inline functions for bitset operation.  */
static inline void
bitset_not (set)
     bitset set;
{
  int bitset_i;
  for (bitset_i = 0; bitset_i < BITSET_UINTS; ++bitset_i)
    set[bitset_i] = ~set[bitset_i];
}

static inline void
bitset_merge (dest, src)
     bitset dest;
     const bitset src;
{
  int bitset_i;
  for (bitset_i = 0; bitset_i < BITSET_UINTS; ++bitset_i)
    dest[bitset_i] |= src[bitset_i];
}

static inline void
bitset_not_merge (dest, src)
     bitset dest;
     const bitset src;
{
  int i;
  for (i = 0; i < BITSET_UINTS; ++i)
    dest[i] |= ~src[i];
}

#ifdef RE_ENABLE_I18N
/* Inline functions for re_string.  */
static inline int
re_string_char_size_at (pstr, idx)
     const re_string_t *pstr;
     int idx;
{
  int byte_idx;
  if (MB_CUR_MAX == 1)
    return 1;
  for (byte_idx = 1; idx + byte_idx < pstr->len; ++byte_idx)
    if (pstr->wcs[idx + byte_idx] != WEOF)
      break;
  return byte_idx;
}

static inline wint_t
re_string_wchar_at (pstr, idx)
     const re_string_t *pstr;
     int idx;
{
  if (MB_CUR_MAX == 1)
    return (wint_t) pstr->mbs[idx];
  return (wint_t) pstr->wcs[idx];
}

static int
re_string_elem_size_at (pstr, idx)
     const re_string_t *pstr;
     int idx;
{
#ifdef _LIBC
  const unsigned char *p, *extra;
  const int32_t *table, *indirect;
  int32_t tmp;
# include <locale/weight.h>
  uint_fast32_t nrules = _NL_CURRENT_WORD (LC_COLLATE, _NL_COLLATE_NRULES);

  if (nrules != 0)
    {
      table = (const int32_t *) _NL_CURRENT (LC_COLLATE, _NL_COLLATE_TABLEMB);
      extra = (const unsigned char *)
	_NL_CURRENT (LC_COLLATE, _NL_COLLATE_EXTRAMB);
      indirect = (const int32_t *) _NL_CURRENT (LC_COLLATE,
						_NL_COLLATE_INDIRECTMB);
      p = pstr->mbs + idx;
      tmp = findidx (&p);
      return p - pstr->mbs - idx;
    }
  else
#endif /* _LIBC */
    return 1;
}
#endif /* RE_ENABLE_I18N */

#endif /*  _REGEX_INTERNAL_H */
