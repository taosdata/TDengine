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
#define YYNOCODE 70
#define YYACTIONTYPE unsigned char
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  SToken yy29;
  EOrder yy78;
  SNode* yy112;
  bool yy117;
  SNodeList* yy124;
  ENullOrder yy137;
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
#define YYNSTATE             62
#define YYNRULE              70
#define YYNTOKEN             32
#define YY_MAX_SHIFT         61
#define YY_MIN_SHIFTREDUCE   112
#define YY_MAX_SHIFTREDUCE   181
#define YY_ERROR_ACTION      182
#define YY_ACCEPT_ACTION     183
#define YY_NO_ACTION         184
#define YY_MIN_REDUCE        185
#define YY_MAX_REDUCE        254
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
#define YY_ACTTAB_COUNT (232)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    34,   46,   34,   34,   34,   34,   54,   46,   34,   34,
 /*    10 */    50,  212,   29,   49,   47,  195,   34,   22,   34,   34,
 /*    20 */    34,   34,   56,   22,   34,   34,   34,   22,   34,   34,
 /*    30 */    34,   34,   56,   22,   34,   34,   34,   46,   34,   34,
 /*    40 */    34,   34,   54,   46,   34,   34,   14,   13,   41,  222,
 /*    50 */    21,  196,   40,    5,   40,    5,  164,   26,  164,  223,
 /*    60 */    34,   57,   34,   34,   34,   34,   56,   57,   34,   34,
 /*    70 */    34,   58,   34,   34,   34,   34,   56,   58,   34,   34,
 /*    80 */    34,   39,   34,   34,   34,   34,   56,   39,   34,   34,
 /*    90 */   204,   12,   11,   10,    9,    7,   61,   48,  212,  207,
 /*   100 */   200,  201,  202,  203,   53,  203,  203,  203,  246,   18,
 /*   110 */   246,  246,  246,  246,   56,  218,  246,  246,  245,   52,
 /*   120 */   245,  245,  245,  245,   56,   18,  245,  245,   15,   21,
 /*   130 */    23,  218,   44,   25,    1,   37,  216,   37,   37,   37,
 /*   140 */    37,   56,   51,   37,   37,   38,  217,   38,   38,   38,
 /*   150 */    38,   56,  216,   38,   38,  242,   42,  242,  242,  242,
 /*   160 */   242,   56,  214,  214,  242,  153,  154,   17,  218,   35,
 /*   170 */   241,  119,  241,  241,  241,  241,   56,  183,   59,  241,
 /*   180 */    14,   13,  120,   24,  213,   49,   40,    5,  118,  216,
 /*   190 */   164,  156,  157,   31,  126,   10,    9,   19,  115,   33,
 /*   200 */   134,  224,  205,   17,  158,    6,    8,  189,   43,   30,
 /*   210 */     3,  208,   32,  146,   16,    2,  125,   45,    4,  206,
 /*   220 */    27,   20,  178,   28,   36,  115,   55,  186,  160,  159,
 /*   230 */   185,   60,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    34,   35,   36,   37,   38,   39,   40,   41,   42,   43,
 /*    10 */    63,   64,   46,   12,   48,   49,   34,   35,   36,   37,
 /*    20 */    38,   39,   40,   41,   42,   43,   34,   35,   36,   37,
 /*    30 */    38,   39,   40,   41,   42,   43,   34,   35,   36,   37,
 /*    40 */    38,   39,   40,   41,   42,   43,    6,    7,   66,   67,
 /*    50 */    22,   49,   12,   13,   12,   13,   16,   68,   16,   67,
 /*    60 */    34,   35,   36,   37,   38,   39,   40,   41,   42,   43,
 /*    70 */    34,   35,   36,   37,   38,   39,   40,   41,   42,   43,
 /*    80 */    34,   35,   36,   37,   38,   39,   40,   41,   42,   43,
 /*    90 */    40,    6,    7,    8,    9,   13,   10,   63,   64,   17,
 /*   100 */    50,   51,   52,   53,   54,   55,   56,   57,   34,   23,
 /*   110 */    36,   37,   38,   39,   40,   44,   42,   43,   34,   12,
 /*   120 */    36,   37,   38,   39,   40,   23,   42,   43,   13,   22,
 /*   130 */    59,   44,    1,   18,   45,   34,   65,   36,   37,   38,
 /*   140 */    39,   40,   33,   42,   43,   34,   59,   36,   37,   38,
 /*   150 */    39,   40,   65,   42,   43,   34,   25,   36,   37,   38,
 /*   160 */    39,   40,   56,   57,   43,   27,   28,   58,   44,   29,
 /*   170 */    34,    2,   36,   37,   38,   39,   40,   32,   33,   43,
 /*   180 */     6,    7,    8,   59,   64,   12,   12,   13,   19,   65,
 /*   190 */    16,   30,   31,   60,    8,    8,    9,   24,   12,   60,
 /*   200 */    12,   69,   40,   58,   14,   15,   21,   47,   62,   61,
 /*   210 */    15,   62,   61,   14,    2,   26,   12,   20,   15,   17,
 /*   220 */    15,   20,   14,   17,   17,   12,   17,    0,   14,   14,
 /*   230 */     0,   11,   70,   70,   70,   70,   70,   70,   70,   70,
 /*   240 */    70,   70,   70,   70,   70,   70,   70,   70,   70,   70,
 /*   250 */    70,   70,   70,   70,   70,   70,   70,   70,   70,   70,
 /*   260 */    70,   70,   70,   70,
};
#define YY_SHIFT_COUNT    (61)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (230)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    86,  174,   40,   40,   40,   40,   40,   40,  107,   40,
 /*    10 */    40,   40,   40,   42,   42,  115,  115,  115,  173,    1,
 /*    20 */    28,  102,  138,  131,  131,  169,  140,    1,  188,  185,
 /*    30 */   232,  232,  232,  232,   85,  161,  186,  187,  187,  190,
 /*    40 */    82,  195,  189,  199,  212,  204,  197,  203,  205,  201,
 /*    50 */   205,  208,  202,  206,  207,  213,  209,  214,  215,  227,
 /*    60 */   230,  220,
};
#define YY_REDUCE_COUNT (33)
#define YY_REDUCE_MIN   (-53)
#define YY_REDUCE_MAX   (162)
static const short yy_reduce_ofst[] = {
 /*     0 */   145,  -34,  -18,   -8,    2,   26,   36,   46,   50,   74,
 /*    10 */    84,  101,  111,  121,  136,   71,   87,  124,  -53,   34,
 /*    20 */   106,  109,  -11,  133,  139,   89,  132,  120,  162,  160,
 /*    30 */   146,  148,  149,  151,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   209,  182,  182,  182,  182,  182,  182,  182,  215,  182,
 /*    10 */   182,  182,  182,  182,  182,  182,  182,  182,  182,  182,
 /*    20 */   215,  209,  225,  220,  220,  190,  228,  182,  182,  182,
 /*    30 */   254,  253,  254,  253,  239,  182,  182,  244,  243,  182,
 /*    40 */   187,  221,  182,  182,  182,  182,  197,  194,  211,  182,
 /*    50 */   210,  182,  207,  182,  182,  182,  182,  182,  182,  182,
 /*    60 */   182,  182,
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
  /*   23 */ "WITH",
  /*   24 */ "RECURSIVE",
  /*   25 */ "ORDER",
  /*   26 */ "BY",
  /*   27 */ "ASC",
  /*   28 */ "DESC",
  /*   29 */ "NULLS",
  /*   30 */ "FIRST",
  /*   31 */ "LAST",
  /*   32 */ "cmd",
  /*   33 */ "query_expression",
  /*   34 */ "value_function",
  /*   35 */ "value_expression",
  /*   36 */ "value_expression_primary",
  /*   37 */ "nonparenthesized_value_expression_primary",
  /*   38 */ "literal",
  /*   39 */ "column_reference",
  /*   40 */ "table_name",
  /*   41 */ "common_value_expression",
  /*   42 */ "numeric_value_expression",
  /*   43 */ "numeric_primary",
  /*   44 */ "query_specification",
  /*   45 */ "set_quantifier_opt",
  /*   46 */ "select_list",
  /*   47 */ "from_clause",
  /*   48 */ "select_sublist",
  /*   49 */ "select_item",
  /*   50 */ "table_reference_list",
  /*   51 */ "table_reference",
  /*   52 */ "table_factor",
  /*   53 */ "table_primary",
  /*   54 */ "db_name",
  /*   55 */ "derived_table",
  /*   56 */ "table_subquery",
  /*   57 */ "subquery",
  /*   58 */ "with_clause_opt",
  /*   59 */ "query_expression_body",
  /*   60 */ "order_by_clause_opt",
  /*   61 */ "limit_clause_opt",
  /*   62 */ "slimit_clause_opt",
  /*   63 */ "with_list",
  /*   64 */ "with_list_element",
  /*   65 */ "query_primary",
  /*   66 */ "sort_specification_list",
  /*   67 */ "sort_specification",
  /*   68 */ "ordering_specification_opt",
  /*   69 */ "null_ordering_opt",
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
 /*  23 */ "query_expression ::= with_clause_opt query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt",
 /*  24 */ "with_clause_opt ::=",
 /*  25 */ "with_clause_opt ::= WITH with_list",
 /*  26 */ "with_clause_opt ::= WITH RECURSIVE with_list",
 /*  27 */ "with_list ::= with_list_element",
 /*  28 */ "with_list ::= with_list NK_COMMA with_list_element",
 /*  29 */ "with_list_element ::= NK_ID AS table_subquery",
 /*  30 */ "table_subquery ::=",
 /*  31 */ "query_expression_body ::= query_primary",
 /*  32 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /*  33 */ "query_primary ::= query_specification",
 /*  34 */ "query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP",
 /*  35 */ "order_by_clause_opt ::=",
 /*  36 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
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
 /*  68 */ "limit_clause_opt ::=",
 /*  69 */ "slimit_clause_opt ::=",
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
    case 32: /* cmd */
    case 33: /* query_expression */
    case 34: /* value_function */
    case 35: /* value_expression */
    case 36: /* value_expression_primary */
    case 37: /* nonparenthesized_value_expression_primary */
    case 38: /* literal */
    case 39: /* column_reference */
    case 40: /* table_name */
    case 41: /* common_value_expression */
    case 42: /* numeric_value_expression */
    case 43: /* numeric_primary */
    case 44: /* query_specification */
    case 47: /* from_clause */
    case 49: /* select_item */
    case 50: /* table_reference_list */
    case 51: /* table_reference */
    case 52: /* table_factor */
    case 53: /* table_primary */
    case 54: /* db_name */
    case 55: /* derived_table */
    case 56: /* table_subquery */
    case 57: /* subquery */
    case 58: /* with_clause_opt */
    case 59: /* query_expression_body */
    case 61: /* limit_clause_opt */
    case 62: /* slimit_clause_opt */
    case 63: /* with_list */
    case 64: /* with_list_element */
    case 65: /* query_primary */
    case 67: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy112)); 
}
      break;
    case 45: /* set_quantifier_opt */
{

}
      break;
    case 46: /* select_list */
    case 48: /* select_sublist */
    case 60: /* order_by_clause_opt */
    case 66: /* sort_specification_list */
{
 nodesDestroyNodeList((yypminor->yy124)); 
}
      break;
    case 68: /* ordering_specification_opt */
{

}
      break;
    case 69: /* null_ordering_opt */
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
  {   32,   -2 }, /* (0) cmd ::= SHOW DATABASES */
  {   32,   -1 }, /* (1) cmd ::= query_expression */
  {   39,   -1 }, /* (2) column_reference ::= NK_ID */
  {   39,   -3 }, /* (3) column_reference ::= table_name NK_DOT NK_ID */
  {   44,   -4 }, /* (4) query_specification ::= SELECT set_quantifier_opt select_list from_clause */
  {   45,    0 }, /* (5) set_quantifier_opt ::= */
  {   45,   -1 }, /* (6) set_quantifier_opt ::= DISTINCT */
  {   45,   -1 }, /* (7) set_quantifier_opt ::= ALL */
  {   46,   -1 }, /* (8) select_list ::= NK_STAR */
  {   46,   -1 }, /* (9) select_list ::= select_sublist */
  {   48,   -1 }, /* (10) select_sublist ::= select_item */
  {   48,   -3 }, /* (11) select_sublist ::= select_sublist NK_COMMA select_item */
  {   49,   -1 }, /* (12) select_item ::= value_expression */
  {   49,   -3 }, /* (13) select_item ::= value_expression AS NK_ID */
  {   49,   -3 }, /* (14) select_item ::= table_name NK_DOT NK_STAR */
  {   47,   -2 }, /* (15) from_clause ::= FROM table_reference_list */
  {   50,   -1 }, /* (16) table_reference_list ::= table_reference */
  {   51,   -1 }, /* (17) table_reference ::= table_factor */
  {   52,   -1 }, /* (18) table_factor ::= table_primary */
  {   53,   -1 }, /* (19) table_primary ::= table_name */
  {   53,   -3 }, /* (20) table_primary ::= db_name NK_DOT table_name */
  {   54,   -1 }, /* (21) db_name ::= NK_ID */
  {   40,   -1 }, /* (22) table_name ::= NK_ID */
  {   33,   -5 }, /* (23) query_expression ::= with_clause_opt query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt */
  {   58,    0 }, /* (24) with_clause_opt ::= */
  {   58,   -2 }, /* (25) with_clause_opt ::= WITH with_list */
  {   58,   -3 }, /* (26) with_clause_opt ::= WITH RECURSIVE with_list */
  {   63,   -1 }, /* (27) with_list ::= with_list_element */
  {   63,   -3 }, /* (28) with_list ::= with_list NK_COMMA with_list_element */
  {   64,   -3 }, /* (29) with_list_element ::= NK_ID AS table_subquery */
  {   56,    0 }, /* (30) table_subquery ::= */
  {   59,   -1 }, /* (31) query_expression_body ::= query_primary */
  {   59,   -4 }, /* (32) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {   65,   -1 }, /* (33) query_primary ::= query_specification */
  {   65,   -6 }, /* (34) query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
  {   60,    0 }, /* (35) order_by_clause_opt ::= */
  {   60,   -3 }, /* (36) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {   66,   -1 }, /* (37) sort_specification_list ::= sort_specification */
  {   66,   -3 }, /* (38) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {   67,   -3 }, /* (39) sort_specification ::= value_expression ordering_specification_opt null_ordering_opt */
  {   68,    0 }, /* (40) ordering_specification_opt ::= */
  {   68,   -1 }, /* (41) ordering_specification_opt ::= ASC */
  {   68,   -1 }, /* (42) ordering_specification_opt ::= DESC */
  {   69,    0 }, /* (43) null_ordering_opt ::= */
  {   69,   -2 }, /* (44) null_ordering_opt ::= NULLS FIRST */
  {   69,   -2 }, /* (45) null_ordering_opt ::= NULLS LAST */
  {   34,   -4 }, /* (46) value_function ::= NK_ID NK_LP value_expression NK_RP */
  {   34,   -6 }, /* (47) value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP */
  {   36,   -3 }, /* (48) value_expression_primary ::= NK_LP value_expression NK_RP */
  {   36,   -1 }, /* (49) value_expression_primary ::= nonparenthesized_value_expression_primary */
  {   37,   -1 }, /* (50) nonparenthesized_value_expression_primary ::= literal */
  {   37,   -1 }, /* (51) nonparenthesized_value_expression_primary ::= column_reference */
  {   38,   -1 }, /* (52) literal ::= NK_LITERAL */
  {   35,   -1 }, /* (53) value_expression ::= common_value_expression */
  {   41,   -1 }, /* (54) common_value_expression ::= numeric_value_expression */
  {   42,   -1 }, /* (55) numeric_value_expression ::= numeric_primary */
  {   42,   -2 }, /* (56) numeric_value_expression ::= NK_PLUS numeric_primary */
  {   42,   -2 }, /* (57) numeric_value_expression ::= NK_MINUS numeric_primary */
  {   42,   -3 }, /* (58) numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression */
  {   42,   -3 }, /* (59) numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression */
  {   42,   -3 }, /* (60) numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression */
  {   42,   -3 }, /* (61) numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression */
  {   43,   -1 }, /* (62) numeric_primary ::= value_expression_primary */
  {   43,   -1 }, /* (63) numeric_primary ::= value_function */
  {   53,   -1 }, /* (64) table_primary ::= derived_table */
  {   55,   -1 }, /* (65) derived_table ::= table_subquery */
  {   57,   -3 }, /* (66) subquery ::= NK_LR query_expression NK_RP */
  {   56,   -1 }, /* (67) table_subquery ::= subquery */
  {   61,    0 }, /* (68) limit_clause_opt ::= */
  {   62,    0 }, /* (69) slimit_clause_opt ::= */
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
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy112; }
        break;
      case 2: /* column_reference ::= NK_ID */
{ PARSER_TRACE; yylhsminor.yy112 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 3: /* column_reference ::= table_name NK_DOT NK_ID */
      case 14: /* select_item ::= table_name NK_DOT NK_STAR */ yytestcase(yyruleno==14);
{ PARSER_TRACE; yylhsminor.yy112 = createColumnNode(pCxt, &yymsp[-2].minor.yy29, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy112 = yylhsminor.yy112;
        break;
      case 4: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause */
{ PARSER_TRACE; yymsp[-3].minor.yy112 = createSelectStmt(pCxt, yymsp[-2].minor.yy117, yymsp[-1].minor.yy124, yymsp[0].minor.yy112); }
        break;
      case 5: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy117 = false; }
        break;
      case 6: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy117 = true; }
        break;
      case 7: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy117 = false; }
        break;
      case 8: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy124 = NULL; }
        break;
      case 9: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy124 = yymsp[0].minor.yy124; }
  yymsp[0].minor.yy124 = yylhsminor.yy124;
        break;
      case 10: /* select_sublist ::= select_item */
      case 37: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==37);
{ PARSER_TRACE; yylhsminor.yy124 = createNodeList(pCxt, yymsp[0].minor.yy112); }
  yymsp[0].minor.yy124 = yylhsminor.yy124;
        break;
      case 11: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 38: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==38);
{ PARSER_TRACE; yylhsminor.yy124 = addNodeToList(pCxt, yymsp[-2].minor.yy124, yymsp[0].minor.yy112); }
  yymsp[-2].minor.yy124 = yylhsminor.yy124;
        break;
      case 12: /* select_item ::= value_expression */
      case 16: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==16);
      case 17: /* table_reference ::= table_factor */ yytestcase(yyruleno==17);
      case 18: /* table_factor ::= table_primary */ yytestcase(yyruleno==18);
      case 31: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==31);
      case 33: /* query_primary ::= query_specification */ yytestcase(yyruleno==33);
{ PARSER_TRACE; yylhsminor.yy112 = yymsp[0].minor.yy112; }
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 13: /* select_item ::= value_expression AS NK_ID */
{ PARSER_TRACE; yylhsminor.yy112 = setProjectionAlias(pCxt, yymsp[-2].minor.yy112, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy112 = yylhsminor.yy112;
        break;
      case 15: /* from_clause ::= FROM table_reference_list */
{ PARSER_TRACE; yymsp[-1].minor.yy112 = yymsp[0].minor.yy112; }
        break;
      case 19: /* table_primary ::= table_name */
{ PARSER_TRACE; yylhsminor.yy112 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy29); }
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 20: /* table_primary ::= db_name NK_DOT table_name */
{ PARSER_TRACE; yylhsminor.yy112 = createRealTableNode(pCxt, &yymsp[-2].minor.yy29, &yymsp[0].minor.yy29); }
  yymsp[-2].minor.yy112 = yylhsminor.yy112;
        break;
      case 21: /* db_name ::= NK_ID */
      case 22: /* table_name ::= NK_ID */ yytestcase(yyruleno==22);
{ PARSER_TRACE; yylhsminor.yy29 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy29 = yylhsminor.yy29;
        break;
      case 23: /* query_expression ::= with_clause_opt query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt */
{  yy_destructor(yypParser,58,&yymsp[-4].minor);
{ PARSER_TRACE; yymsp[-4].minor.yy112 = yymsp[-3].minor.yy112; }
  yy_destructor(yypParser,60,&yymsp[-2].minor);
  yy_destructor(yypParser,61,&yymsp[-1].minor);
  yy_destructor(yypParser,62,&yymsp[0].minor);
}
        break;
      case 24: /* with_clause_opt ::= */
      case 30: /* table_subquery ::= */ yytestcase(yyruleno==30);
{}
        break;
      case 25: /* with_clause_opt ::= WITH with_list */
      case 26: /* with_clause_opt ::= WITH RECURSIVE with_list */ yytestcase(yyruleno==26);
{ PARSER_TRACE; pCxt->notSupport = true; pCxt->valid = false; }
  yy_destructor(yypParser,63,&yymsp[0].minor);
        break;
      case 27: /* with_list ::= with_list_element */
{  yy_destructor(yypParser,64,&yymsp[0].minor);
{}
}
        break;
      case 28: /* with_list ::= with_list NK_COMMA with_list_element */
{  yy_destructor(yypParser,63,&yymsp[-2].minor);
{}
  yy_destructor(yypParser,64,&yymsp[0].minor);
}
        break;
      case 29: /* with_list_element ::= NK_ID AS table_subquery */
{}
  yy_destructor(yypParser,56,&yymsp[0].minor);
        break;
      case 32: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy112 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy112, yymsp[0].minor.yy112); }
  yymsp[-3].minor.yy112 = yylhsminor.yy112;
        break;
      case 34: /* query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy112 = yymsp[-4].minor.yy112;}
  yy_destructor(yypParser,60,&yymsp[-3].minor);
  yy_destructor(yypParser,61,&yymsp[-2].minor);
  yy_destructor(yypParser,62,&yymsp[-1].minor);
        break;
      case 35: /* order_by_clause_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy124 = NULL; }
        break;
      case 36: /* order_by_clause_opt ::= ORDER BY sort_specification_list */
{ PARSER_TRACE; yymsp[-2].minor.yy124 = yymsp[0].minor.yy124; }
        break;
      case 39: /* sort_specification ::= value_expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy112 = createOrderByExprNode(pCxt, yymsp[-2].minor.yy112, yymsp[-1].minor.yy78, yymsp[0].minor.yy137); }
  yymsp[-2].minor.yy112 = yylhsminor.yy112;
        break;
      case 40: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy78 = ORDER_ASC; }
        break;
      case 41: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy78 = ORDER_ASC; }
        break;
      case 42: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy78 = ORDER_DESC; }
        break;
      case 43: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy137 = NULL_ORDER_DEFAULT; }
        break;
      case 44: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy137 = NULL_ORDER_FIRST; }
        break;
      case 45: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy137 = NULL_ORDER_LAST; }
        break;
      case 46: /* value_function ::= NK_ID NK_LP value_expression NK_RP */
      case 48: /* value_expression_primary ::= NK_LP value_expression NK_RP */ yytestcase(yyruleno==48);
{
}
  yy_destructor(yypParser,35,&yymsp[-1].minor);
        break;
      case 47: /* value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP */
{
}
  yy_destructor(yypParser,35,&yymsp[-3].minor);
  yy_destructor(yypParser,35,&yymsp[-1].minor);
        break;
      case 49: /* value_expression_primary ::= nonparenthesized_value_expression_primary */
{  yy_destructor(yypParser,37,&yymsp[0].minor);
{
}
}
        break;
      case 50: /* nonparenthesized_value_expression_primary ::= literal */
{  yy_destructor(yypParser,38,&yymsp[0].minor);
{
}
}
        break;
      case 51: /* nonparenthesized_value_expression_primary ::= column_reference */
{  yy_destructor(yypParser,39,&yymsp[0].minor);
{
}
}
        break;
      case 53: /* value_expression ::= common_value_expression */
{  yy_destructor(yypParser,41,&yymsp[0].minor);
{
}
}
        break;
      case 54: /* common_value_expression ::= numeric_value_expression */
{  yy_destructor(yypParser,42,&yymsp[0].minor);
{
}
}
        break;
      case 55: /* numeric_value_expression ::= numeric_primary */
{  yy_destructor(yypParser,43,&yymsp[0].minor);
{
}
}
        break;
      case 56: /* numeric_value_expression ::= NK_PLUS numeric_primary */
      case 57: /* numeric_value_expression ::= NK_MINUS numeric_primary */ yytestcase(yyruleno==57);
{
}
  yy_destructor(yypParser,43,&yymsp[0].minor);
        break;
      case 58: /* numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression */
      case 59: /* numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression */ yytestcase(yyruleno==59);
      case 60: /* numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression */ yytestcase(yyruleno==60);
      case 61: /* numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression */ yytestcase(yyruleno==61);
{  yy_destructor(yypParser,42,&yymsp[-2].minor);
{
}
  yy_destructor(yypParser,42,&yymsp[0].minor);
}
        break;
      case 62: /* numeric_primary ::= value_expression_primary */
{  yy_destructor(yypParser,36,&yymsp[0].minor);
{
}
}
        break;
      case 63: /* numeric_primary ::= value_function */
{  yy_destructor(yypParser,34,&yymsp[0].minor);
{
}
}
        break;
      case 64: /* table_primary ::= derived_table */
{  yy_destructor(yypParser,55,&yymsp[0].minor);
{
}
}
        break;
      case 65: /* derived_table ::= table_subquery */
{  yy_destructor(yypParser,56,&yymsp[0].minor);
{
}
}
        break;
      case 66: /* subquery ::= NK_LR query_expression NK_RP */
{
}
  yy_destructor(yypParser,33,&yymsp[-1].minor);
        break;
      case 67: /* table_subquery ::= subquery */
{  yy_destructor(yypParser,57,&yymsp[0].minor);
{
}
}
        break;
      default:
      /* (52) literal ::= NK_LITERAL */ yytestcase(yyruleno==52);
      /* (68) limit_clause_opt ::= */ yytestcase(yyruleno==68);
      /* (69) slimit_clause_opt ::= */ yytestcase(yyruleno==69);
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
