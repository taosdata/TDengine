/*
** 2000-05-29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Driver template for the LEMON parser generator.
**
** The "lemon" program processes an LALR(1) input grammar file, then uses
** this template to construct a parser.  The "lemon" program inserts text
** at each "%%" line.  Also, any "P-a-r-s-e" identifer prefix (without the
** interstitial "-" characters) contained in this template is changed into
** the value of the %name directive from the grammar.  Otherwise, the content
** of this template is copied straight through into the generate parser
** source file.
**
** The following is the concatenation of all %include directives from the
** input grammar file:
*/
#include <stdio.h>
#include <assert.h>
/************ Begin %include sections from the grammar ************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#include "nodes.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "astCreateFuncs.h"

#define PARSER_TRACE printf("rule = %s\n", yyRuleName[yyruleno])
/**************** End of %include directives **********************************/
/* These constants specify the various numeric values for terminal symbols
** in a format understandable to "makeheaders".  This section is blank unless
** "lemon" is run with the "-m" command-line option.
***************** Begin makeheaders token definitions *************************/
/**************** End makeheaders token definitions ***************************/

/* The next sections is a series of control #defines.
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used to store the integer codes
**                       that represent terminal and non-terminal symbols.
**                       "unsigned char" is used if there are fewer than
**                       256 symbols.  Larger types otherwise.
**    YYNOCODE           is a number of type YYCODETYPE that is not used for
**                       any terminal or nonterminal symbol.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       (also known as: "terminal symbols") have fall-back
**                       values which should be used if the original symbol
**                       would not parse.  This permits keywords to sometimes
**                       be used as identifiers, for example.
**    YYACTIONTYPE       is the data type used for "action codes" - numbers
**                       that indicate what to do in response to the next
**                       token.
**    NewParseTOKENTYPE     is the data type used for minor type for terminal
**                       symbols.  Background: A "minor type" is a semantic
**                       value associated with a terminal or non-terminal
**                       symbols.  For example, for an "ID" terminal symbol,
**                       the minor type might be the name of the identifier.
**                       Each non-terminal can have a different minor type.
**                       Terminal symbols all have the same minor type, though.
**                       This macros defines the minor type for terminal 
**                       symbols.
**    YYMINORTYPE        is the data type used for all minor types.
**                       This is typically a union of many types, one of
**                       which is NewParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    NewParseARG_SDECL     A static variable declaration for the %extra_argument
**    NewParseARG_PDECL     A parameter declaration for the %extra_argument
**    NewParseARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    NewParseARG_STORE     Code to store %extra_argument into yypParser
**    NewParseARG_FETCH     Code to extract %extra_argument from yypParser
**    NewParseCTX_*         As NewParseARG_ except for %extra_context
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYNTOKEN           Number of terminal symbols
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
**    YY_MIN_REDUCE      Minimum value for reduce actions
**    YY_MAX_REDUCE      Maximum value for reduce actions
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/************* Begin control #defines *****************************************/
#define YYCODETYPE unsigned char
#define YYNOCODE 71
#define YYACTIONTYPE unsigned char
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  bool yy9;
  SNodeList* yy30;
  SToken yy67;
  ENullOrder yy68;
  EOrder yy108;
  SNode* yy130;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define NewParseARG_SDECL  SAstCreateContext* pCxt ;
#define NewParseARG_PDECL , SAstCreateContext* pCxt 
#define NewParseARG_PARAM ,pCxt 
#define NewParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define NewParseARG_STORE yypParser->pCxt =pCxt ;
#define NewParseCTX_SDECL
#define NewParseCTX_PDECL
#define NewParseCTX_PARAM
#define NewParseCTX_FETCH
#define NewParseCTX_STORE
#define YYNSTATE             63
#define YYNRULE              68
#define YYNTOKEN             35
#define YY_MAX_SHIFT         62
#define YY_MIN_SHIFTREDUCE   114
#define YY_MAX_SHIFTREDUCE   181
#define YY_ERROR_ACTION      182
#define YY_ACCEPT_ACTION     183
#define YY_NO_ACTION         184
#define YY_MIN_REDUCE        185
#define YY_MAX_REDUCE        252
/************* End control #defines *******************************************/
#define YY_NLOOKAHEAD ((int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])))

/* Define the yytestcase() macro to be a no-op if is not already defined
** otherwise.
**
** Applications can choose to define yytestcase() in the %include section
** to a macro that can assist in verifying code coverage.  For production
** code the yytestcase() macro should be turned off.  But it is useful
** for testing.
*/
#ifndef yytestcase
# define yytestcase(X)
#endif


/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.  
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N <= YY_MAX_SHIFT             Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   N between YY_MIN_SHIFTREDUCE       Shift to an arbitrary state then
**     and YY_MAX_SHIFTREDUCE           reduce by rule N-YY_MIN_SHIFTREDUCE.
**
**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as either:
**
**    (A)   N = yy_action[ yy_shift_ofst[S] + X ]
**    (B)   N = yy_default[S]
**
** The (A) formula is preferred.  The B formula is used instead if
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X.
**
** The formulas above are for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array.
**
** The following are the tables generated in this section:
**
**  yy_action[]        A single table containing all actions.
**  yy_lookahead[]     A table containing the lookahead for each entry in
**                     yy_action.  Used to detect hash collisions.
**  yy_shift_ofst[]    For each state, the offset into yy_action for
**                     shifting terminals.
**  yy_reduce_ofst[]   For each state, the offset into yy_action for
**                     shifting non-terminals after a reduce.
**  yy_default[]       Default action for each state.
**
*********** Begin parsing tables **********************************************/
#define YY_ACTTAB_COUNT (229)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    30,   50,   30,   30,   30,   30,   55,   50,   30,   30,
 /*    10 */    10,    9,   29,   62,   51,  195,   30,   19,   30,   30,
 /*    20 */    30,   30,   57,   19,   30,   30,   30,   19,   30,   30,
 /*    30 */    30,   30,   57,   19,   30,   30,   30,   50,   30,   30,
 /*    40 */    30,   30,   55,   50,   30,   30,   39,  222,  155,  156,
 /*    50 */    23,  196,   12,   11,   10,    9,   25,  223,   30,   58,
 /*    60 */    30,   30,   30,   30,   57,   58,   30,   30,   30,   59,
 /*    70 */    30,   30,   30,   30,   57,   59,   30,   30,   30,   37,
 /*    80 */    30,   30,   30,   30,   57,   37,   30,   30,  204,   38,
 /*    90 */     5,  183,   60,  166,  158,  159,  121,   53,  200,  201,
 /*   100 */   202,  203,   54,  203,  203,  203,  246,   18,  246,  246,
 /*   110 */   246,  246,   57,  120,  246,  246,  245,   17,  245,  245,
 /*   120 */   245,  245,   57,   46,  245,  245,   35,   27,   35,   35,
 /*   130 */    35,   35,   57,   31,   35,   35,   36,   47,   36,   36,
 /*   140 */    36,   36,   57,  128,   36,   36,  242,  117,  242,  242,
 /*   150 */   242,  242,   57,   42,  241,  242,  241,  241,  241,  241,
 /*   160 */    57,   52,  212,  241,   14,   13,  122,    1,  212,  212,
 /*   170 */    38,    5,   14,   13,  166,   40,  224,   20,   38,    5,
 /*   180 */    15,  210,  166,  211,   21,   22,   17,  210,  210,   43,
 /*   190 */   160,    6,    7,   45,   41,   26,  207,   48,   24,  208,
 /*   200 */   136,   44,  205,    8,  189,    3,  142,    2,   16,  127,
 /*   210 */   147,  146,   32,  150,   49,  149,    4,   33,  180,  206,
 /*   220 */    28,  117,   34,  186,   56,  162,  161,  185,   61,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    37,   38,   39,   40,   41,   42,   43,   44,   45,   46,
 /*    10 */     8,    9,   49,   10,   51,   52,   37,   38,   39,   40,
 /*    20 */    41,   42,   43,   44,   45,   46,   37,   38,   39,   40,
 /*    30 */    41,   42,   43,   44,   45,   46,   37,   38,   39,   40,
 /*    40 */    41,   42,   43,   44,   45,   46,   67,   68,   30,   31,
 /*    50 */    69,   52,    6,    7,    8,    9,   63,   68,   37,   38,
 /*    60 */    39,   40,   41,   42,   43,   44,   45,   46,   37,   38,
 /*    70 */    39,   40,   41,   42,   43,   44,   45,   46,   37,   38,
 /*    80 */    39,   40,   41,   42,   43,   44,   45,   46,   43,   12,
 /*    90 */    13,   35,   36,   16,   33,   34,    2,   12,   53,   54,
 /*   100 */    55,   56,   57,   58,   59,   60,   37,   22,   39,   40,
 /*   110 */    41,   42,   43,   19,   45,   46,   37,   61,   39,   40,
 /*   120 */    41,   42,   43,   15,   45,   46,   37,   63,   39,   40,
 /*   130 */    41,   42,   43,   32,   45,   46,   37,   29,   39,   40,
 /*   140 */    41,   42,   43,    8,   45,   46,   37,   12,   39,   40,
 /*   150 */    41,   42,   43,    1,   37,   46,   39,   40,   41,   42,
 /*   160 */    43,   36,   47,   46,    6,    7,    8,   48,   47,   47,
 /*   170 */    12,   13,    6,    7,   16,   23,   70,   62,   12,   13,
 /*   180 */    13,   66,   16,   62,   62,   18,   61,   66,   66,   15,
 /*   190 */    14,   15,   13,   25,   64,   64,   17,   28,   65,   65,
 /*   200 */    12,   27,   43,   21,   50,   15,   14,   24,    2,   12,
 /*   210 */    26,   26,   26,   26,   20,   26,   15,   26,   14,   17,
 /*   220 */    17,   12,   17,    0,   17,   14,   14,    0,   11,   71,
 /*   230 */    71,   71,   71,   71,   71,   71,   71,   71,   71,   71,
 /*   240 */    71,   71,   71,   71,   71,   71,   71,   71,   71,   71,
 /*   250 */    71,   71,   71,   71,   71,   71,   71,   71,   71,   71,
 /*   260 */    71,   71,   71,   71,
};
#define YY_SHIFT_COUNT    (62)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (227)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */     3,  158,  166,  166,  166,  166,  166,  166,   85,  166,
 /*    10 */   166,  166,  166,   77,   77,  167,  167,  167,  229,   18,
 /*    20 */   152,  152,   94,  101,  168,  169,  169,  168,  188,  182,
 /*    30 */    46,   61,  174,  108,  135,    2,    2,  176,  179,  190,
 /*    40 */   183,  192,  206,  184,  185,  186,  187,  189,  191,  197,
 /*    50 */   194,  201,  204,  202,  203,  205,  209,  207,  211,  212,
 /*    60 */   223,  227,  217,
};
#define YY_REDUCE_COUNT (29)
#define YY_REDUCE_MIN   (-37)
#define YY_REDUCE_MAX   (159)
static const short yy_reduce_ofst[] = {
 /*     0 */    56,  -37,  -21,  -11,   -1,   21,   31,   41,   45,   69,
 /*    10 */    79,   89,   99,  109,  117,  115,  121,  122,  125,  -19,
 /*    20 */    -7,   64,  119,  106,  130,  133,  134,  131,  159,  154,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   209,  182,  182,  182,  182,  182,  182,  182,  182,  182,
 /*    10 */   182,  182,  182,  182,  182,  182,  182,  182,  209,  225,
 /*    20 */   214,  214,  190,  228,  216,  219,  219,  216,  182,  182,
 /*    30 */   239,  182,  182,  182,  182,  244,  243,  182,  187,  215,
 /*    40 */   182,  182,  182,  182,  182,  182,  182,  182,  182,  182,
 /*    50 */   197,  194,  182,  207,  182,  182,  182,  182,  182,  182,
 /*    60 */   182,  182,  182,
};
/********** End of lemon-generated parsing tables *****************************/

/* The next table maps tokens (terminal symbols) into fallback tokens.  
** If a construct like the following:
** 
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
**
** This feature can be used, for example, to cause some keywords in a language
** to revert to identifiers if they keyword does not apply in the context where
** it appears.
*/
#ifdef YYFALLBACK
static const YYCODETYPE yyFallback[] = {
};
#endif /* YYFALLBACK */

/* The following structure represents a single element of the
** parser's stack.  Information stored includes:
**
**   +  The state number for the parser at this level of the stack.
**
**   +  The value of the token stored at this level of the stack.
**      (In other words, the "major" token.)
**
**   +  The semantic value stored at this level of the stack.  This is
**      the information used by the action routines in the grammar.
**      It is sometimes called the "minor" token.
**
** After the "shift" half of a SHIFTREDUCE action, the stateno field
** actually contains the reduce action for the second half of the
** SHIFTREDUCE.
*/
struct yyStackEntry {
  YYACTIONTYPE stateno;  /* The state-number, or reduce action in SHIFTREDUCE */
  YYCODETYPE major;      /* The major token value.  This is the code
                         ** number for the token at this stack level */
  YYMINORTYPE minor;     /* The user-supplied minor token value.  This
                         ** is the value of the token  */
};
typedef struct yyStackEntry yyStackEntry;

/* The state of the parser is completely contained in an instance of
** the following structure */
struct yyParser {
  yyStackEntry *yytos;          /* Pointer to top element of the stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyhwm;                    /* High-water mark of the stack */
#endif
#ifndef YYNOERRORRECOVERY
  int yyerrcnt;                 /* Shifts left before out of the error */
#endif
  NewParseARG_SDECL                /* A place to hold %extra_argument */
  NewParseCTX_SDECL                /* A place to hold %extra_context */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
  yyStackEntry yystk0;          /* First stack entry */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
  yyStackEntry *yystackEnd;            /* Last entry in the stack */
#endif
};
typedef struct yyParser yyParser;

#ifndef NDEBUG
#include <stdio.h>
static FILE *yyTraceFILE = 0;
static char *yyTracePrompt = 0;
#endif /* NDEBUG */

#ifndef NDEBUG
/* 
** Turn parser tracing on by giving a stream to which to write the trace
** and a prompt to preface each trace message.  Tracing is turned off
** by making either argument NULL 
**
** Inputs:
** <ul>
** <li> A FILE* to which trace output should be written.
**      If NULL, then tracing is turned off.
** <li> A prefix string written at the beginning of every
**      line of trace output.  If NULL, then tracing is
**      turned off.
** </ul>
**
** Outputs:
** None.
*/
void NewParseTrace(FILE *TraceFILE, char *zTracePrompt){
  yyTraceFILE = TraceFILE;
  yyTracePrompt = zTracePrompt;
  if( yyTraceFILE==0 ) yyTracePrompt = 0;
  else if( yyTracePrompt==0 ) yyTraceFILE = 0;
}
#endif /* NDEBUG */

#if defined(YYCOVERAGE) || !defined(NDEBUG)
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  /*    0 */ "$",
  /*    1 */ "UNION",
  /*    2 */ "ALL",
  /*    3 */ "MINUS",
  /*    4 */ "EXCEPT",
  /*    5 */ "INTERSECT",
  /*    6 */ "NK_PLUS",
  /*    7 */ "NK_MINUS",
  /*    8 */ "NK_STAR",
  /*    9 */ "NK_SLASH",
  /*   10 */ "SHOW",
  /*   11 */ "DATABASES",
  /*   12 */ "NK_ID",
  /*   13 */ "NK_LP",
  /*   14 */ "NK_RP",
  /*   15 */ "NK_COMMA",
  /*   16 */ "NK_LITERAL",
  /*   17 */ "NK_DOT",
  /*   18 */ "SELECT",
  /*   19 */ "DISTINCT",
  /*   20 */ "AS",
  /*   21 */ "FROM",
  /*   22 */ "NK_LR",
  /*   23 */ "ORDER",
  /*   24 */ "BY",
  /*   25 */ "SLIMIT",
  /*   26 */ "NK_INTEGER",
  /*   27 */ "SOFFSET",
  /*   28 */ "LIMIT",
  /*   29 */ "OFFSET",
  /*   30 */ "ASC",
  /*   31 */ "DESC",
  /*   32 */ "NULLS",
  /*   33 */ "FIRST",
  /*   34 */ "LAST",
  /*   35 */ "cmd",
  /*   36 */ "query_expression",
  /*   37 */ "value_function",
  /*   38 */ "value_expression",
  /*   39 */ "value_expression_primary",
  /*   40 */ "nonparenthesized_value_expression_primary",
  /*   41 */ "literal",
  /*   42 */ "column_reference",
  /*   43 */ "table_name",
  /*   44 */ "common_value_expression",
  /*   45 */ "numeric_value_expression",
  /*   46 */ "numeric_primary",
  /*   47 */ "query_specification",
  /*   48 */ "set_quantifier_opt",
  /*   49 */ "select_list",
  /*   50 */ "from_clause",
  /*   51 */ "select_sublist",
  /*   52 */ "select_item",
  /*   53 */ "table_reference_list",
  /*   54 */ "table_reference",
  /*   55 */ "table_factor",
  /*   56 */ "table_primary",
  /*   57 */ "db_name",
  /*   58 */ "derived_table",
  /*   59 */ "table_subquery",
  /*   60 */ "subquery",
  /*   61 */ "with_clause_opt",
  /*   62 */ "query_expression_body",
  /*   63 */ "order_by_clause_opt",
  /*   64 */ "slimit_clause_opt",
  /*   65 */ "limit_clause_opt",
  /*   66 */ "query_primary",
  /*   67 */ "sort_specification_list",
  /*   68 */ "sort_specification",
  /*   69 */ "ordering_specification_opt",
  /*   70 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= SHOW DATABASES",
 /*   1 */ "cmd ::= query_expression",
 /*   2 */ "column_reference ::= NK_ID",
 /*   3 */ "column_reference ::= table_name NK_DOT NK_ID",
 /*   4 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause",
 /*   5 */ "set_quantifier_opt ::=",
 /*   6 */ "set_quantifier_opt ::= DISTINCT",
 /*   7 */ "set_quantifier_opt ::= ALL",
 /*   8 */ "select_list ::= NK_STAR",
 /*   9 */ "select_list ::= select_sublist",
 /*  10 */ "select_sublist ::= select_item",
 /*  11 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /*  12 */ "select_item ::= value_expression",
 /*  13 */ "select_item ::= value_expression AS NK_ID",
 /*  14 */ "select_item ::= table_name NK_DOT NK_STAR",
 /*  15 */ "from_clause ::= FROM table_reference_list",
 /*  16 */ "table_reference_list ::= table_reference",
 /*  17 */ "table_reference ::= table_factor",
 /*  18 */ "table_factor ::= table_primary",
 /*  19 */ "table_primary ::= table_name",
 /*  20 */ "table_primary ::= db_name NK_DOT table_name",
 /*  21 */ "db_name ::= NK_ID",
 /*  22 */ "table_name ::= NK_ID",
 /*  23 */ "query_expression ::= with_clause_opt query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /*  24 */ "with_clause_opt ::=",
 /*  25 */ "query_expression_body ::= query_primary",
 /*  26 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /*  27 */ "query_primary ::= query_specification",
 /*  28 */ "query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP",
 /*  29 */ "order_by_clause_opt ::=",
 /*  30 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /*  31 */ "slimit_clause_opt ::=",
 /*  32 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /*  33 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /*  34 */ "limit_clause_opt ::=",
 /*  35 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /*  36 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /*  37 */ "sort_specification_list ::= sort_specification",
 /*  38 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /*  39 */ "sort_specification ::= value_expression ordering_specification_opt null_ordering_opt",
 /*  40 */ "ordering_specification_opt ::=",
 /*  41 */ "ordering_specification_opt ::= ASC",
 /*  42 */ "ordering_specification_opt ::= DESC",
 /*  43 */ "null_ordering_opt ::=",
 /*  44 */ "null_ordering_opt ::= NULLS FIRST",
 /*  45 */ "null_ordering_opt ::= NULLS LAST",
 /*  46 */ "value_function ::= NK_ID NK_LP value_expression NK_RP",
 /*  47 */ "value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP",
 /*  48 */ "value_expression_primary ::= NK_LP value_expression NK_RP",
 /*  49 */ "value_expression_primary ::= nonparenthesized_value_expression_primary",
 /*  50 */ "nonparenthesized_value_expression_primary ::= literal",
 /*  51 */ "nonparenthesized_value_expression_primary ::= column_reference",
 /*  52 */ "literal ::= NK_LITERAL",
 /*  53 */ "value_expression ::= common_value_expression",
 /*  54 */ "common_value_expression ::= numeric_value_expression",
 /*  55 */ "numeric_value_expression ::= numeric_primary",
 /*  56 */ "numeric_value_expression ::= NK_PLUS numeric_primary",
 /*  57 */ "numeric_value_expression ::= NK_MINUS numeric_primary",
 /*  58 */ "numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression",
 /*  59 */ "numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression",
 /*  60 */ "numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression",
 /*  61 */ "numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression",
 /*  62 */ "numeric_primary ::= value_expression_primary",
 /*  63 */ "numeric_primary ::= value_function",
 /*  64 */ "table_primary ::= derived_table",
 /*  65 */ "derived_table ::= table_subquery",
 /*  66 */ "subquery ::= NK_LR query_expression NK_RP",
 /*  67 */ "table_subquery ::= subquery",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.  Return the number
** of errors.  Return 0 on success.
*/
static int yyGrowStack(yyParser *p){
  int newSize;
  int idx;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  idx = p->yytos ? (int)(p->yytos - p->yystack) : 0;
  if( p->yystack==&p->yystk0 ){
    pNew = malloc(newSize*sizeof(pNew[0]));
    if( pNew ) pNew[0] = p->yystk0;
  }else{
    pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  }
  if( pNew ){
    p->yystack = pNew;
    p->yytos = &p->yystack[idx];
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sStack grows from %d to %d entries.\n",
              yyTracePrompt, p->yystksz, newSize);
    }
#endif
    p->yystksz = newSize;
  }
  return pNew==0; 
}
#endif

/* Datatype of the argument to the memory allocated passed as the
** second argument to NewParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void NewParseInit(void *yypRawParser NewParseCTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  NewParseCTX_STORE
#ifdef YYTRACKMAXSTACKDEPTH
  yypParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  yypParser->yytos = NULL;
  yypParser->yystack = NULL;
  yypParser->yystksz = 0;
  if( yyGrowStack(yypParser) ){
    yypParser->yystack = &yypParser->yystk0;
    yypParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  yypParser->yytos = yypParser->yystack;
  yypParser->yystack[0].stateno = 0;
  yypParser->yystack[0].major = 0;
#if YYSTACKDEPTH>0
  yypParser->yystackEnd = &yypParser->yystack[YYSTACKDEPTH-1];
#endif
}

#ifndef NewParse_ENGINEALWAYSONSTACK
/* 
** This function allocates a new parser.
** The only argument is a pointer to a function which works like
** malloc.
**
** Inputs:
** A pointer to the function used to allocate memory.
**
** Outputs:
** A pointer to a parser.  This pointer is used in subsequent calls
** to NewParse and NewParseFree.
*/
void *NewParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE) NewParseCTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    NewParseCTX_STORE
    NewParseInit(yypParser NewParseCTX_PARAM);
  }
  return (void*)yypParser;
}
#endif /* NewParse_ENGINEALWAYSONSTACK */


/* The following function deletes the "minor type" or semantic value
** associated with a symbol.  The symbol can be either a terminal
** or nonterminal. "yymajor" is the symbol code, and "yypminor" is
** a pointer to the value to be deleted.  The code used to do the 
** deletions is derived from the %destructor and/or %token_destructor
** directives of the input grammar.
*/
static void yy_destructor(
  yyParser *yypParser,    /* The parser */
  YYCODETYPE yymajor,     /* Type code for object to destroy */
  YYMINORTYPE *yypminor   /* The object to be destroyed */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
  switch( yymajor ){
    /* Here is inserted the actions which take place when a
    ** terminal or non-terminal is destroyed.  This can happen
    ** when the symbol is popped from the stack during a
    ** reduce or during error processing or when a parser is 
    ** being destroyed before it is finished parsing.
    **
    ** Note: during a reduce, the only symbols destroyed are those
    ** which appear on the RHS of the rule, but which are *not* used
    ** inside the C code.
    */
/********* Begin destructor definitions ***************************************/
      /* Default NON-TERMINAL Destructor */
    case 35: /* cmd */
    case 36: /* query_expression */
    case 37: /* value_function */
    case 38: /* value_expression */
    case 39: /* value_expression_primary */
    case 40: /* nonparenthesized_value_expression_primary */
    case 41: /* literal */
    case 42: /* column_reference */
    case 43: /* table_name */
    case 44: /* common_value_expression */
    case 45: /* numeric_value_expression */
    case 46: /* numeric_primary */
    case 47: /* query_specification */
    case 50: /* from_clause */
    case 52: /* select_item */
    case 53: /* table_reference_list */
    case 54: /* table_reference */
    case 55: /* table_factor */
    case 56: /* table_primary */
    case 57: /* db_name */
    case 58: /* derived_table */
    case 59: /* table_subquery */
    case 60: /* subquery */
    case 61: /* with_clause_opt */
    case 62: /* query_expression_body */
    case 64: /* slimit_clause_opt */
    case 65: /* limit_clause_opt */
    case 66: /* query_primary */
    case 68: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy130)); 
}
      break;
    case 48: /* set_quantifier_opt */
{

}
      break;
    case 49: /* select_list */
    case 51: /* select_sublist */
    case 63: /* order_by_clause_opt */
    case 67: /* sort_specification_list */
{
 nodesDestroyNodeList((yypminor->yy30)); 
}
      break;
    case 69: /* ordering_specification_opt */
{

}
      break;
    case 70: /* null_ordering_opt */
{

}
      break;
/********* End destructor definitions *****************************************/
    default:  break;   /* If no destructor action specified: do nothing */
  }
}

/*
** Pop the parser's stack once.
**
** If there is a destructor routine associated with the token which
** is popped from the stack, then call it.
*/
static void yy_pop_parser_stack(yyParser *pParser){
  yyStackEntry *yytos;
  assert( pParser->yytos!=0 );
  assert( pParser->yytos > pParser->yystack );
  yytos = pParser->yytos--;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yy_destructor(pParser, yytos->major, &yytos->minor);
}

/*
** Clear all secondary memory allocations from the parser
*/
void NewParseFinalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef NewParse_ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void NewParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  NewParseFinalize(p);
  (*freeProc)(p);
}
#endif /* NewParse_ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int NewParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyhwm;
}
#endif

/* This array of booleans keeps track of the parser statement
** coverage.  The element yycoverage[X][Y] is set when the parser
** is in state X and has a lookahead token Y.  In a well-tested
** systems, every element of this matrix should end up being set.
*/
#if defined(YYCOVERAGE)
static unsigned char yycoverage[YYNSTATE][YYNTOKEN];
#endif

/*
** Write into out a description of every state/lookahead combination that
**
**   (1)  has not been used by the parser, and
**   (2)  is not a syntax error.
**
** Return the number of missed state/lookahead combinations.
*/
#if defined(YYCOVERAGE)
int NewParseCoverage(FILE *out){
  int stateno, iLookAhead, i;
  int nMissed = 0;
  for(stateno=0; stateno<YYNSTATE; stateno++){
    i = yy_shift_ofst[stateno];
    for(iLookAhead=0; iLookAhead<YYNTOKEN; iLookAhead++){
      if( yy_lookahead[i+iLookAhead]!=iLookAhead ) continue;
      if( yycoverage[stateno][iLookAhead]==0 ) nMissed++;
      if( out ){
        fprintf(out,"State %d lookahead %s %s\n", stateno,
                yyTokenName[iLookAhead],
                yycoverage[stateno][iLookAhead] ? "ok" : "missed");
      }
    }
  }
  return nMissed;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
static YYACTIONTYPE yy_find_shift_action(
  YYCODETYPE iLookAhead,    /* The look-ahead token */
  YYACTIONTYPE stateno      /* Current state number */
){
  int i;

  if( stateno>YY_MAX_SHIFT ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
#if defined(YYCOVERAGE)
  yycoverage[stateno][iLookAhead] = 1;
#endif
  do{
    i = yy_shift_ofst[stateno];
    assert( i>=0 );
    /* assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD ); */
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( i>=YY_NLOOKAHEAD || yy_lookahead[i]!=iLookAhead ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      if( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0])
             && (iFallback = yyFallback[iLookAhead])!=0 ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE, "%sFALLBACK %s => %s\n",
             yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[iFallback]);
        }
#endif
        assert( yyFallback[iFallback]==0 ); /* Fallback loop must terminate */
        iLookAhead = iFallback;
        continue;
      }
#endif
#ifdef YYWILDCARD
      {
        int j = i - iLookAhead + YYWILDCARD;
        if( 
#if YY_SHIFT_MIN+YYWILDCARD<0
          j>=0 &&
#endif
#if YY_SHIFT_MAX+YYWILDCARD>=YY_ACTTAB_COUNT
          j<YY_ACTTAB_COUNT &&
#endif
          j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) &&
          yy_lookahead[j]==YYWILDCARD && iLookAhead>0
        ){
#ifndef NDEBUG
          if( yyTraceFILE ){
            fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead],
               yyTokenName[YYWILDCARD]);
          }
#endif /* NDEBUG */
          return yy_action[j];
        }
      }
#endif /* YYWILDCARD */
      return yy_default[stateno];
    }else{
      return yy_action[i];
    }
  }while(1);
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
*/
static YYACTIONTYPE yy_find_reduce_action(
  YYACTIONTYPE stateno,     /* Current state number */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
#ifdef YYERRORSYMBOL
  if( stateno>YY_REDUCE_COUNT ){
    return yy_default[stateno];
  }
#else
  assert( stateno<=YY_REDUCE_COUNT );
#endif
  i = yy_reduce_ofst[stateno];
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
#ifdef YYERRORSYMBOL
  if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
    return yy_default[stateno];
  }
#else
  assert( i>=0 && i<YY_ACTTAB_COUNT );
  assert( yy_lookahead[i]==iLookAhead );
#endif
  return yy_action[i];
}

/*
** The following routine is called if the stack overflows.
*/
static void yyStackOverflow(yyParser *yypParser){
   NewParseARG_FETCH
   NewParseCTX_FETCH
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yytos>yypParser->yystack ) yy_pop_parser_stack(yypParser);
   /* Here code is inserted which will execute if the parser
   ** stack every overflows */
/******** Begin %stack_overflow code ******************************************/
/******** End %stack_overflow code ********************************************/
   NewParseARG_STORE /* Suppress warning about unused %extra_argument var */
   NewParseCTX_STORE
}

/*
** Print tracing information for a SHIFT action
*/
#ifndef NDEBUG
static void yyTraceShift(yyParser *yypParser, int yyNewState, const char *zTag){
  if( yyTraceFILE ){
    if( yyNewState<YYNSTATE ){
      fprintf(yyTraceFILE,"%s%s '%s', go to state %d\n",
         yyTracePrompt, zTag, yyTokenName[yypParser->yytos->major],
         yyNewState);
    }else{
      fprintf(yyTraceFILE,"%s%s '%s', pending reduce %d\n",
         yyTracePrompt, zTag, yyTokenName[yypParser->yytos->major],
         yyNewState - YY_MIN_REDUCE);
    }
  }
}
#else
# define yyTraceShift(X,Y,Z)
#endif

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  YYACTIONTYPE yyNewState,      /* The new state to shift in */
  YYCODETYPE yyMajor,           /* The major token to shift in */
  NewParseTOKENTYPE yyMinor        /* The minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yytos++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
    yypParser->yyhwm++;
    assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack) );
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yytos>yypParser->yystackEnd ){
    yypParser->yytos--;
    yyStackOverflow(yypParser);
    return;
  }
#else
  if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz] ){
    if( yyGrowStack(yypParser) ){
      yypParser->yytos--;
      yyStackOverflow(yypParser);
      return;
    }
  }
#endif
  if( yyNewState > YY_MAX_SHIFT ){
    yyNewState += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
  }
  yytos = yypParser->yytos;
  yytos->stateno = yyNewState;
  yytos->major = yyMajor;
  yytos->minor.yy0 = yyMinor;
  yyTraceShift(yypParser, yyNewState, "Shift");
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;       /* Symbol on the left-hand side of the rule */
  signed char nrhs;     /* Negative of the number of RHS symbols in the rule */
} yyRuleInfo[] = {
  {   35,   -2 }, /* (0) cmd ::= SHOW DATABASES */
  {   35,   -1 }, /* (1) cmd ::= query_expression */
  {   42,   -1 }, /* (2) column_reference ::= NK_ID */
  {   42,   -3 }, /* (3) column_reference ::= table_name NK_DOT NK_ID */
  {   47,   -4 }, /* (4) query_specification ::= SELECT set_quantifier_opt select_list from_clause */
  {   48,    0 }, /* (5) set_quantifier_opt ::= */
  {   48,   -1 }, /* (6) set_quantifier_opt ::= DISTINCT */
  {   48,   -1 }, /* (7) set_quantifier_opt ::= ALL */
  {   49,   -1 }, /* (8) select_list ::= NK_STAR */
  {   49,   -1 }, /* (9) select_list ::= select_sublist */
  {   51,   -1 }, /* (10) select_sublist ::= select_item */
  {   51,   -3 }, /* (11) select_sublist ::= select_sublist NK_COMMA select_item */
  {   52,   -1 }, /* (12) select_item ::= value_expression */
  {   52,   -3 }, /* (13) select_item ::= value_expression AS NK_ID */
  {   52,   -3 }, /* (14) select_item ::= table_name NK_DOT NK_STAR */
  {   50,   -2 }, /* (15) from_clause ::= FROM table_reference_list */
  {   53,   -1 }, /* (16) table_reference_list ::= table_reference */
  {   54,   -1 }, /* (17) table_reference ::= table_factor */
  {   55,   -1 }, /* (18) table_factor ::= table_primary */
  {   56,   -1 }, /* (19) table_primary ::= table_name */
  {   56,   -3 }, /* (20) table_primary ::= db_name NK_DOT table_name */
  {   57,   -1 }, /* (21) db_name ::= NK_ID */
  {   43,   -1 }, /* (22) table_name ::= NK_ID */
  {   36,   -5 }, /* (23) query_expression ::= with_clause_opt query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {   61,    0 }, /* (24) with_clause_opt ::= */
  {   62,   -1 }, /* (25) query_expression_body ::= query_primary */
  {   62,   -4 }, /* (26) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {   66,   -1 }, /* (27) query_primary ::= query_specification */
  {   66,   -6 }, /* (28) query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
  {   63,    0 }, /* (29) order_by_clause_opt ::= */
  {   63,   -3 }, /* (30) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {   64,    0 }, /* (31) slimit_clause_opt ::= */
  {   64,   -4 }, /* (32) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {   64,   -4 }, /* (33) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {   65,    0 }, /* (34) limit_clause_opt ::= */
  {   65,   -4 }, /* (35) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {   65,   -4 }, /* (36) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {   67,   -1 }, /* (37) sort_specification_list ::= sort_specification */
  {   67,   -3 }, /* (38) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {   68,   -3 }, /* (39) sort_specification ::= value_expression ordering_specification_opt null_ordering_opt */
  {   69,    0 }, /* (40) ordering_specification_opt ::= */
  {   69,   -1 }, /* (41) ordering_specification_opt ::= ASC */
  {   69,   -1 }, /* (42) ordering_specification_opt ::= DESC */
  {   70,    0 }, /* (43) null_ordering_opt ::= */
  {   70,   -2 }, /* (44) null_ordering_opt ::= NULLS FIRST */
  {   70,   -2 }, /* (45) null_ordering_opt ::= NULLS LAST */
  {   37,   -4 }, /* (46) value_function ::= NK_ID NK_LP value_expression NK_RP */
  {   37,   -6 }, /* (47) value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP */
  {   39,   -3 }, /* (48) value_expression_primary ::= NK_LP value_expression NK_RP */
  {   39,   -1 }, /* (49) value_expression_primary ::= nonparenthesized_value_expression_primary */
  {   40,   -1 }, /* (50) nonparenthesized_value_expression_primary ::= literal */
  {   40,   -1 }, /* (51) nonparenthesized_value_expression_primary ::= column_reference */
  {   41,   -1 }, /* (52) literal ::= NK_LITERAL */
  {   38,   -1 }, /* (53) value_expression ::= common_value_expression */
  {   44,   -1 }, /* (54) common_value_expression ::= numeric_value_expression */
  {   45,   -1 }, /* (55) numeric_value_expression ::= numeric_primary */
  {   45,   -2 }, /* (56) numeric_value_expression ::= NK_PLUS numeric_primary */
  {   45,   -2 }, /* (57) numeric_value_expression ::= NK_MINUS numeric_primary */
  {   45,   -3 }, /* (58) numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression */
  {   45,   -3 }, /* (59) numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression */
  {   45,   -3 }, /* (60) numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression */
  {   45,   -3 }, /* (61) numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression */
  {   46,   -1 }, /* (62) numeric_primary ::= value_expression_primary */
  {   46,   -1 }, /* (63) numeric_primary ::= value_function */
  {   56,   -1 }, /* (64) table_primary ::= derived_table */
  {   58,   -1 }, /* (65) derived_table ::= table_subquery */
  {   60,   -3 }, /* (66) subquery ::= NK_LR query_expression NK_RP */
  {   59,   -1 }, /* (67) table_subquery ::= subquery */
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
**
** The yyLookahead and yyLookaheadToken parameters provide reduce actions
** access to the lookahead token (if any).  The yyLookahead will be YYNOCODE
** if the lookahead token has already been consumed.  As this procedure is
** only called from one place, optimizing compilers will in-line it, which
** means that the extra parameters have no performance impact.
*/
static YYACTIONTYPE yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno,       /* Number of the rule by which to reduce */
  int yyLookahead,             /* Lookahead token, or YYNOCODE if none */
  NewParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  NewParseCTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  YYACTIONTYPE yyact;             /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  NewParseARG_FETCH
  (void)yyLookahead;
  (void)yyLookaheadToken;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfo[yyruleno].nrhs;
    if( yysize ){
      fprintf(yyTraceFILE, "%sReduce %d [%s], go to state %d.\n",
        yyTracePrompt,
        yyruleno, yyRuleName[yyruleno], yymsp[yysize].stateno);
    }else{
      fprintf(yyTraceFILE, "%sReduce %d [%s].\n",
        yyTracePrompt, yyruleno, yyRuleName[yyruleno]);
    }
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if( yyRuleInfo[yyruleno].nrhs==0 ){
#ifdef YYTRACKMAXSTACKDEPTH
    if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
      yypParser->yyhwm++;
      assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack));
    }
#endif
#if YYSTACKDEPTH>0 
    if( yypParser->yytos>=yypParser->yystackEnd ){
      yyStackOverflow(yypParser);
      /* The call to yyStackOverflow() above pops the stack until it is
      ** empty, causing the main parser loop to exit.  So the return value
      ** is never used and does not matter. */
      return 0;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        /* The call to yyStackOverflow() above pops the stack until it is
        ** empty, causing the main parser loop to exit.  So the return value
        ** is never used and does not matter. */
        return 0;
      }
      yymsp = yypParser->yytos;
    }
#endif
  }

  switch( yyruleno ){
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
/********** Begin reduce actions **********************************************/
        YYMINORTYPE yylhsminor;
      case 0: /* cmd ::= SHOW DATABASES */
{ PARSER_TRACE; createShowStmt(pCxt, SHOW_TYPE_DATABASE); }
        break;
      case 1: /* cmd ::= query_expression */
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy130; }
        break;
      case 2: /* column_reference ::= NK_ID */
{ PARSER_TRACE; yylhsminor.yy130 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy130 = yylhsminor.yy130;
        break;
      case 3: /* column_reference ::= table_name NK_DOT NK_ID */
      case 14: /* select_item ::= table_name NK_DOT NK_STAR */ yytestcase(yyruleno==14);
{ PARSER_TRACE; yylhsminor.yy130 = createColumnNode(pCxt, &yymsp[-2].minor.yy67, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy130 = yylhsminor.yy130;
        break;
      case 4: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause */
{ PARSER_TRACE; yymsp[-3].minor.yy130 = createSelectStmt(pCxt, yymsp[-2].minor.yy9, yymsp[-1].minor.yy30, yymsp[0].minor.yy130); }
        break;
      case 5: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy9 = false; }
        break;
      case 6: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy9 = true; }
        break;
      case 7: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy9 = false; }
        break;
      case 8: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy30 = NULL; }
        break;
      case 9: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy30 = yymsp[0].minor.yy30; }
  yymsp[0].minor.yy30 = yylhsminor.yy30;
        break;
      case 10: /* select_sublist ::= select_item */
      case 37: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==37);
{ PARSER_TRACE; yylhsminor.yy30 = createNodeList(pCxt, yymsp[0].minor.yy130); }
  yymsp[0].minor.yy30 = yylhsminor.yy30;
        break;
      case 11: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 38: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==38);
{ PARSER_TRACE; yylhsminor.yy30 = addNodeToList(pCxt, yymsp[-2].minor.yy30, yymsp[0].minor.yy130); }
  yymsp[-2].minor.yy30 = yylhsminor.yy30;
        break;
      case 12: /* select_item ::= value_expression */
      case 16: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==16);
      case 17: /* table_reference ::= table_factor */ yytestcase(yyruleno==17);
      case 18: /* table_factor ::= table_primary */ yytestcase(yyruleno==18);
      case 25: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==25);
      case 27: /* query_primary ::= query_specification */ yytestcase(yyruleno==27);
{ PARSER_TRACE; yylhsminor.yy130 = yymsp[0].minor.yy130; }
  yymsp[0].minor.yy130 = yylhsminor.yy130;
        break;
      case 13: /* select_item ::= value_expression AS NK_ID */
{ PARSER_TRACE; yylhsminor.yy130 = setProjectionAlias(pCxt, yymsp[-2].minor.yy130, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy130 = yylhsminor.yy130;
        break;
      case 15: /* from_clause ::= FROM table_reference_list */
{ PARSER_TRACE; yymsp[-1].minor.yy130 = yymsp[0].minor.yy130; }
        break;
      case 19: /* table_primary ::= table_name */
{ PARSER_TRACE; yylhsminor.yy130 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy67); }
  yymsp[0].minor.yy130 = yylhsminor.yy130;
        break;
      case 20: /* table_primary ::= db_name NK_DOT table_name */
{ PARSER_TRACE; yylhsminor.yy130 = createRealTableNode(pCxt, &yymsp[-2].minor.yy67, &yymsp[0].minor.yy67); }
  yymsp[-2].minor.yy130 = yylhsminor.yy130;
        break;
      case 21: /* db_name ::= NK_ID */
      case 22: /* table_name ::= NK_ID */ yytestcase(yyruleno==22);
{ PARSER_TRACE; yylhsminor.yy67 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy67 = yylhsminor.yy67;
        break;
      case 23: /* query_expression ::= with_clause_opt query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{  yy_destructor(yypParser,61,&yymsp[-4].minor);
{ 
                                                                                                                                    PARSER_TRACE;
                                                                                                                                    addOrderByList(pCxt, yymsp[-3].minor.yy130, yymsp[-2].minor.yy30);
                                                                                                                                    addSlimit(pCxt, yymsp[-3].minor.yy130, yymsp[-1].minor.yy130);
                                                                                                                                    addLimit(pCxt, yymsp[-3].minor.yy130, yymsp[0].minor.yy130);
                                                                                                                                    yymsp[-4].minor.yy130 = yymsp[-3].minor.yy130;  
                                                                                                                                  }
}
        break;
      case 24: /* with_clause_opt ::= */
{}
        break;
      case 26: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy130 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy130, yymsp[0].minor.yy130); }
  yymsp[-3].minor.yy130 = yylhsminor.yy130;
        break;
      case 28: /* query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy130 = yymsp[-4].minor.yy130;}
  yy_destructor(yypParser,63,&yymsp[-3].minor);
  yy_destructor(yypParser,65,&yymsp[-2].minor);
  yy_destructor(yypParser,64,&yymsp[-1].minor);
        break;
      case 29: /* order_by_clause_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy30 = NULL; }
        break;
      case 30: /* order_by_clause_opt ::= ORDER BY sort_specification_list */
{ PARSER_TRACE; yymsp[-2].minor.yy30 = yymsp[0].minor.yy30; }
        break;
      case 31: /* slimit_clause_opt ::= */
      case 34: /* limit_clause_opt ::= */ yytestcase(yyruleno==34);
{ yymsp[1].minor.yy130 = NULL; }
        break;
      case 32: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 35: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==35);
{ yymsp[-3].minor.yy130 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 33: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 36: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==36);
{ yymsp[-3].minor.yy130 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 39: /* sort_specification ::= value_expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy130 = createOrderByExprNode(pCxt, yymsp[-2].minor.yy130, yymsp[-1].minor.yy108, yymsp[0].minor.yy68); }
  yymsp[-2].minor.yy130 = yylhsminor.yy130;
        break;
      case 40: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy108 = ORDER_ASC; }
        break;
      case 41: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy108 = ORDER_ASC; }
        break;
      case 42: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy108 = ORDER_DESC; }
        break;
      case 43: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy68 = NULL_ORDER_DEFAULT; }
        break;
      case 44: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy68 = NULL_ORDER_FIRST; }
        break;
      case 45: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy68 = NULL_ORDER_LAST; }
        break;
      case 46: /* value_function ::= NK_ID NK_LP value_expression NK_RP */
      case 48: /* value_expression_primary ::= NK_LP value_expression NK_RP */ yytestcase(yyruleno==48);
{
}
  yy_destructor(yypParser,38,&yymsp[-1].minor);
        break;
      case 47: /* value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP */
{
}
  yy_destructor(yypParser,38,&yymsp[-3].minor);
  yy_destructor(yypParser,38,&yymsp[-1].minor);
        break;
      case 49: /* value_expression_primary ::= nonparenthesized_value_expression_primary */
{  yy_destructor(yypParser,40,&yymsp[0].minor);
{
}
}
        break;
      case 50: /* nonparenthesized_value_expression_primary ::= literal */
{  yy_destructor(yypParser,41,&yymsp[0].minor);
{
}
}
        break;
      case 51: /* nonparenthesized_value_expression_primary ::= column_reference */
{  yy_destructor(yypParser,42,&yymsp[0].minor);
{
}
}
        break;
      case 53: /* value_expression ::= common_value_expression */
{  yy_destructor(yypParser,44,&yymsp[0].minor);
{
}
}
        break;
      case 54: /* common_value_expression ::= numeric_value_expression */
{  yy_destructor(yypParser,45,&yymsp[0].minor);
{
}
}
        break;
      case 55: /* numeric_value_expression ::= numeric_primary */
{  yy_destructor(yypParser,46,&yymsp[0].minor);
{
}
}
        break;
      case 56: /* numeric_value_expression ::= NK_PLUS numeric_primary */
      case 57: /* numeric_value_expression ::= NK_MINUS numeric_primary */ yytestcase(yyruleno==57);
{
}
  yy_destructor(yypParser,46,&yymsp[0].minor);
        break;
      case 58: /* numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression */
      case 59: /* numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression */ yytestcase(yyruleno==59);
      case 60: /* numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression */ yytestcase(yyruleno==60);
      case 61: /* numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression */ yytestcase(yyruleno==61);
{  yy_destructor(yypParser,45,&yymsp[-2].minor);
{
}
  yy_destructor(yypParser,45,&yymsp[0].minor);
}
        break;
      case 62: /* numeric_primary ::= value_expression_primary */
{  yy_destructor(yypParser,39,&yymsp[0].minor);
{
}
}
        break;
      case 63: /* numeric_primary ::= value_function */
{  yy_destructor(yypParser,37,&yymsp[0].minor);
{
}
}
        break;
      case 64: /* table_primary ::= derived_table */
{  yy_destructor(yypParser,58,&yymsp[0].minor);
{
}
}
        break;
      case 65: /* derived_table ::= table_subquery */
{  yy_destructor(yypParser,59,&yymsp[0].minor);
{
}
}
        break;
      case 66: /* subquery ::= NK_LR query_expression NK_RP */
{
}
  yy_destructor(yypParser,36,&yymsp[-1].minor);
        break;
      case 67: /* table_subquery ::= subquery */
{  yy_destructor(yypParser,60,&yymsp[0].minor);
{
}
}
        break;
      default:
      /* (52) literal ::= NK_LITERAL */ yytestcase(yyruleno==52);
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yyact = yy_find_reduce_action(yymsp[yysize].stateno,(YYCODETYPE)yygoto);

  /* There are no SHIFTREDUCE actions on nonterminals because the table
  ** generator has simplified them to pure REDUCE actions. */
  assert( !(yyact>YY_MAX_SHIFT && yyact<=YY_MAX_SHIFTREDUCE) );

  /* It is not possible for a REDUCE to be followed by an error */
  assert( yyact!=YY_ERROR_ACTION );

  yymsp += yysize+1;
  yypParser->yytos = yymsp;
  yymsp->stateno = (YYACTIONTYPE)yyact;
  yymsp->major = (YYCODETYPE)yygoto;
  yyTraceShift(yypParser, yyact, "... then shift");
  return yyact;
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sFail!\n",yyTracePrompt);
  }
#endif
  while( yypParser->yytos>yypParser->yystack ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser fails */
/************ Begin %parse_failure code ***************************************/
/************ End %parse_failure code *****************************************/
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  NewParseTOKENTYPE yyminor         /* The minor type of the error token */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
  
  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > pCxt->pQueryCxt->msgLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        sprintf(pCxt->pQueryCxt->pMsg, msg, tmpstr);
    } else {
        sprintf(pCxt->pQueryCxt->pMsg, msg, &TOKEN.z[0]);
    }
  } else {
    sprintf(pCxt->pQueryCxt->pMsg, "Incomplete SQL statement");
  }
  pCxt->valid = false;
/************ End %syntax_error code ******************************************/
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sAccept!\n",yyTracePrompt);
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  assert( yypParser->yytos==yypParser->yystack );
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */
/*********** Begin %parse_accept code *****************************************/
 printf("parsing complete!\n" );
/*********** End %parse_accept code *******************************************/
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "NewParseAlloc" which describes the current state of the parser.
** The second argument is the major token number.  The third is
** the minor token.  The fourth optional argument is whatever the
** user wants (and specified in the grammar) and is available for
** use by the action routines.
**
** Inputs:
** <ul>
** <li> A pointer to the parser (an opaque structure.)
** <li> The major token number.
** <li> The minor token number.
** <li> An option argument of a grammar-specified type.
** </ul>
**
** Outputs:
** None.
*/
void NewParse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  NewParseTOKENTYPE yyminor       /* The value for the token */
  NewParseARG_PDECL               /* Optional %extra_argument parameter */
){
  YYMINORTYPE yyminorunion;
  YYACTIONTYPE yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser = (yyParser*)yyp;  /* The parser */
  NewParseCTX_FETCH
  NewParseARG_STORE

  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif

  yyact = yypParser->yytos->stateno;
#ifndef NDEBUG
  if( yyTraceFILE ){
    if( yyact < YY_MIN_REDUCE ){
      fprintf(yyTraceFILE,"%sInput '%s' in state %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact);
    }else{
      fprintf(yyTraceFILE,"%sInput '%s' with pending reduce %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact-YY_MIN_REDUCE);
    }
  }
#endif

  do{
    assert( yyact==yypParser->yytos->stateno );
    yyact = yy_find_shift_action((YYCODETYPE)yymajor,yyact);
    if( yyact >= YY_MIN_REDUCE ){
      yyact = yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,
                        yyminor NewParseCTX_PARAM);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,(YYCODETYPE)yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      break;
    }else if( yyact==YY_ACCEPT_ACTION ){
      yypParser->yytos--;
      yy_accept(yypParser);
      return;
    }else{
      assert( yyact == YY_ERROR_ACTION );
      yyminorunion.yy0 = yyminor;
#ifdef YYERRORSYMBOL
      int yymx;
#endif
#ifndef NDEBUG
      if( yyTraceFILE ){
        fprintf(yyTraceFILE,"%sSyntax Error!\n",yyTracePrompt);
      }
#endif
#ifdef YYERRORSYMBOL
      /* A syntax error has occurred.
      ** The response to an error depends upon whether or not the
      ** grammar defines an error token "ERROR".  
      **
      ** This is what we do if the grammar does define ERROR:
      **
      **  * Call the %syntax_error function.
      **
      **  * Begin popping the stack until we enter a state where
      **    it is legal to shift the error symbol, then shift
      **    the error symbol.
      **
      **  * Set the error count to three.
      **
      **  * Begin accepting and shifting new tokens.  No new error
      **    processing will occur until three tokens have been
      **    shifted successfully.
      **
      */
      if( yypParser->yyerrcnt<0 ){
        yy_syntax_error(yypParser,yymajor,yyminor);
      }
      yymx = yypParser->yytos->major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor, &yyminorunion);
        yymajor = YYNOCODE;
      }else{
        while( yypParser->yytos >= yypParser->yystack
            && (yyact = yy_find_reduce_action(
                        yypParser->yytos->stateno,
                        YYERRORSYMBOL)) > YY_MAX_SHIFTREDUCE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yytos < yypParser->yystack || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
#ifndef YYNOERRORRECOVERY
          yypParser->yyerrcnt = -1;
#endif
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          yy_shift(yypParser,yyact,YYERRORSYMBOL,yyminor);
        }
      }
      yypParser->yyerrcnt = 3;
      yyerrorhit = 1;
      if( yymajor==YYNOCODE ) break;
      yyact = yypParser->yytos->stateno;
#elif defined(YYNOERRORRECOVERY)
      /* If the YYNOERRORRECOVERY macro is defined, then do not attempt to
      ** do any kind of error recovery.  Instead, simply invoke the syntax
      ** error routine and continue going as if nothing had happened.
      **
      ** Applications can set this macro (for example inside %include) if
      ** they intend to abandon the parse upon the first syntax error seen.
      */
      yy_syntax_error(yypParser,yymajor, yyminor);
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      break;
#else  /* YYERRORSYMBOL is not defined */
      /* This is what we do if the grammar does not define ERROR:
      **
      **  * Report an error message, and throw away the input token.
      **
      **  * If the input token is $, then fail the parse.
      **
      ** As before, subsequent error messages are suppressed until
      ** three input tokens have been successfully shifted.
      */
      if( yypParser->yyerrcnt<=0 ){
        yy_syntax_error(yypParser,yymajor, yyminor);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
#ifndef YYNOERRORRECOVERY
        yypParser->yyerrcnt = -1;
#endif
      }
      break;
#endif
    }
  }while( yypParser->yytos>yypParser->yystack );
#ifndef NDEBUG
  if( yyTraceFILE ){
    yyStackEntry *i;
    char cDiv = '[';
    fprintf(yyTraceFILE,"%sReturn. Stack=",yyTracePrompt);
    for(i=&yypParser->yystack[1]; i<=yypParser->yytos; i++){
      fprintf(yyTraceFILE,"%c%s", cDiv, yyTokenName[i->major]);
      cDiv = ' ';
    }
    fprintf(yyTraceFILE,"]\n");
  }
#endif
  return;
}

/*
** Return the fallback token corresponding to canonical token iToken, or
** 0 if iToken has no fallback.
*/
int NewParseFallback(int iToken){
#ifdef YYFALLBACK
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
