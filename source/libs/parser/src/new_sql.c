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

#if 0
#define PARSER_TRACE printf("lemon rule = %s\n", yyRuleName[yyruleno])
#define PARSER_DESTRUCTOR_TRACE printf("lemon destroy token = %s\n", yyTokenName[yymajor])
#define PARSER_COMPLETE printf("parsing complete!\n" )
#else
#define PARSER_TRACE
#define PARSER_DESTRUCTOR_TRACE
#define PARSER_COMPLETE
#endif
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
#define YYNOCODE 125
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  EOperatorType yy40;
  EFillMode yy44;
  SToken yy79;
  ENullOrder yy107;
  EJoinType yy162;
  SNodeList* yy174;
  EOrder yy188;
  SNode* yy212;
  bool yy237;
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
#define YYNSTATE             149
#define YYNRULE              135
#define YYNTOKEN             73
#define YY_MAX_SHIFT         148
#define YY_MIN_SHIFTREDUCE   245
#define YY_MAX_SHIFTREDUCE   379
#define YY_ERROR_ACTION      380
#define YY_ACCEPT_ACTION     381
#define YY_NO_ACTION         382
#define YY_MIN_REDUCE        383
#define YY_MAX_REDUCE        517
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
#define YY_ACTTAB_COUNT (656)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   392,  390,   94,  494,   20,   67,  393,  390,   27,   25,
 /*    10 */    23,   22,   21,  400,  390,  321,   52,  131,  414,  144,
 /*    20 */   492,   74,   46,  401,  266,  403,   19,   89,   98,  119,
 /*    30 */   284,  285,  286,  287,  288,  289,  290,  292,  293,  294,
 /*    40 */    27,   25,   23,   22,   21,   23,   22,   21,  346,  465,
 /*    50 */   463,  247,  248,  249,  250,  145,  253,  320,   19,   89,
 /*    60 */    98,  132,  284,  285,  286,  287,  288,  289,  290,  292,
 /*    70 */   293,  294,   26,   24,  110,  344,  345,  347,  348,  247,
 /*    80 */   248,  249,  250,  145,  253,  260,  103,    6,  400,  390,
 /*    90 */   494,    8,  143,  414,  144,    3,  317,   41,  401,  111,
 /*   100 */   403,  439,   36,   53,   70,   96,  435,  492,  400,  390,
 /*   110 */   143,  414,  143,  414,  144,  469,  114,   41,  401,  128,
 /*   120 */   403,  439,  450,  148,   54,   96,  435,  450,  143,  414,
 /*   130 */   494,   34,   63,   36,  118,  490,  400,  390,  137,  448,
 /*   140 */   143,  414,  144,  493,  447,   41,  401,  492,  403,  439,
 /*   150 */    26,   24,  322,   96,  435,   54,   17,  247,  248,  249,
 /*   160 */   250,  145,  253,  454,  103,   28,  291,  400,  390,  295,
 /*   170 */   136,  143,  414,  144,    5,    4,   41,  401,  329,  403,
 /*   180 */   439,  138,  400,  390,  438,  435,  143,  414,  144,  415,
 /*   190 */   258,   41,  401,   76,  403,  439,  120,  115,  113,  123,
 /*   200 */   435,  400,  390,  377,  378,  143,  414,  144,   78,   34,
 /*   210 */    40,  401,   42,  403,  439,  343,  400,  390,   88,  435,
 /*   220 */   131,  414,  144,   26,   24,   46,  401,    7,  403,  134,
 /*   230 */   247,  248,  249,  250,  145,  253,  112,  103,    6,  476,
 /*   240 */    27,   25,   23,   22,   21,  109,   72,   26,   24,  130,
 /*   250 */    31,  140,  104,  462,  247,  248,  249,  250,  145,  253,
 /*   260 */   450,  103,   28,  400,  390,   55,  253,  143,  414,  144,
 /*   270 */   107,   57,   41,  401,   60,  403,  439,  446,  124,  108,
 /*   280 */   281,  436,   34,  400,  390,  475,   95,  143,  414,  144,
 /*   290 */     5,    4,   47,  401,   34,  403,  141,   59,  400,  390,
 /*   300 */   374,  375,  143,  414,  144,    2,   34,   86,  401,  105,
 /*   310 */   403,  400,  390,  302,  456,  143,  414,  144,   16,  122,
 /*   320 */    86,  401,  121,  403,   27,   25,   23,   22,   21,  133,
 /*   330 */   508,  400,  390,   62,  106,  143,  414,  144,   49,    1,
 /*   340 */    86,  401,   97,  403,  400,  390,   64,  317,  143,  414,
 /*   350 */   144,   13,   29,   47,  401,  296,  403,  400,  390,  421,
 /*   360 */   257,  143,  414,  144,   44,  260,   86,  401,  102,  403,
 /*   370 */   451,  400,  390,   30,   65,  143,  414,  144,  261,   29,
 /*   380 */    83,  401,  264,  403,  400,  390,  510,  466,  143,  414,
 /*   390 */   144,  509,  139,   80,  401,   99,  403,  400,  390,  135,
 /*   400 */   142,  143,  414,  144,  258,   77,   84,  401,  397,  403,
 /*   410 */   395,  400,  390,   75,  491,  143,  414,  144,   10,   29,
 /*   420 */    81,  401,   11,  403,  400,  390,   35,   56,  143,  414,
 /*   430 */   144,  340,   58,   85,  401,  342,  403,  400,  390,  336,
 /*   440 */    48,  143,  414,  144,   61,   38,  411,  401,  335,  403,
 /*   450 */   116,  400,  390,  117,   39,  143,  414,  144,  395,   12,
 /*   460 */   410,  401,    4,  403,  400,  390,  282,  315,  143,  414,
 /*   470 */   144,  314,   32,  409,  401,   69,  403,  400,  390,   33,
 /*   480 */   394,  143,  414,  144,   51,  368,   92,  401,   14,  403,
 /*   490 */     9,  400,  390,   37,  357,  143,  414,  144,  363,   79,
 /*   500 */    91,  401,  362,  403,  400,  390,  100,  367,  143,  414,
 /*   510 */   144,  366,  101,   93,  401,  251,  403,  400,  390,  384,
 /*   520 */   383,  143,  414,  144,   15,  147,   90,  401,  382,  403,
 /*   530 */   382,  400,  390,  382,  382,  143,  414,  144,  382,  382,
 /*   540 */    82,  401,  382,  403,  400,  390,  382,  382,  143,  414,
 /*   550 */   144,  382,  382,   87,  401,  382,  403,  127,   45,  382,
 /*   560 */   382,  382,  127,   45,  382,  382,   43,  382,  382,  382,
 /*   570 */   382,   43,  382,  382,  129,   66,  444,  445,  382,  444,
 /*   580 */    68,  444,  126,  382,  125,  382,  382,  127,   45,  382,
 /*   590 */   382,  382,  127,   45,  382,  382,   43,  382,  382,  382,
 /*   600 */   382,   43,  382,  381,  146,   50,  444,  445,  382,  444,
 /*   610 */    71,  444,  445,  382,  444,   27,   25,   23,   22,   21,
 /*   620 */    27,   25,   23,   22,   21,  382,  382,  382,  382,  261,
 /*   630 */   382,  382,   18,  494,  382,  382,  266,  382,   27,   25,
 /*   640 */    23,   22,   21,  382,  382,  382,   53,  382,   73,  382,
 /*   650 */   492,   27,   25,   23,   22,   21,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    75,   76,   77,  103,   89,   90,   75,   76,    8,    9,
 /*    10 */    10,   11,   12,   75,   76,    4,  116,   79,   80,   81,
 /*    20 */   120,  123,   84,   85,   24,   87,   26,   27,   28,   22,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */     8,    9,   10,   11,   12,   10,   11,   12,   29,   83,
 /*    50 */   112,   15,   16,   17,   18,   19,   20,   46,   26,   27,
 /*    60 */    28,   74,   30,   31,   32,   33,   34,   35,   36,   37,
 /*    70 */    38,   39,    8,    9,   55,   56,   57,   58,   59,   15,
 /*    80 */    16,   17,   18,   19,   20,   22,   22,   23,   75,   76,
 /*    90 */   103,   27,   79,   80,   81,   43,   44,   84,   85,  115,
 /*   100 */    87,   88,   23,  116,   41,   92,   93,  120,   75,   76,
 /*   110 */    79,   80,   79,   80,   81,  102,   85,   84,   85,  101,
 /*   120 */    87,   88,   82,   13,   45,   92,   93,   82,   79,   80,
 /*   130 */   103,   67,  108,   23,   85,  102,   75,   76,   21,   99,
 /*   140 */    79,   80,   81,  116,   99,   84,   85,  120,   87,   88,
 /*   150 */     8,    9,   10,   92,   93,   45,   26,   15,   16,   17,
 /*   160 */    18,   19,   20,  102,   22,   23,   36,   75,   76,   39,
 /*   170 */     3,   79,   80,   81,    1,    2,   84,   85,   10,   87,
 /*   180 */    88,   64,   75,   76,   92,   93,   79,   80,   81,   80,
 /*   190 */    22,   84,   85,  117,   87,   88,   50,   51,   52,   92,
 /*   200 */    93,   75,   76,   71,   72,   79,   80,   81,  117,   67,
 /*   210 */    84,   85,   21,   87,   88,   24,   75,   76,   92,   93,
 /*   220 */    79,   80,   81,    8,    9,   84,   85,  104,   87,   62,
 /*   230 */    15,   16,   17,   18,   19,   20,   54,   22,   23,  114,
 /*   240 */     8,    9,   10,   11,   12,   53,  105,    8,    9,   22,
 /*   250 */    23,   21,  111,  112,   15,   16,   17,   18,   19,   20,
 /*   260 */    82,   22,   23,   75,   76,  113,   20,   79,   80,   81,
 /*   270 */    76,   21,   84,   85,   24,   87,   88,   99,   27,   76,
 /*   280 */    29,   93,   67,   75,   76,  114,   76,   79,   80,   81,
 /*   290 */     1,    2,   84,   85,   67,   87,   66,  113,   75,   76,
 /*   300 */    68,   69,   79,   80,   81,   61,   67,   84,   85,   86,
 /*   310 */    87,   75,   76,   24,  110,   79,   80,   81,    2,   60,
 /*   320 */    84,   85,   86,   87,    8,    9,   10,   11,   12,  121,
 /*   330 */   122,   75,   76,  109,   48,   79,   80,   81,  107,   47,
 /*   340 */    84,   85,   86,   87,   75,   76,  106,   44,   79,   80,
 /*   350 */    81,   23,   21,   84,   85,   24,   87,   75,   76,   91,
 /*   360 */    22,   79,   80,   81,   79,   22,   84,   85,   86,   87,
 /*   370 */    82,   75,   76,   40,   94,   79,   80,   81,   22,   21,
 /*   380 */    84,   85,   24,   87,   75,   76,  124,   83,   79,   80,
 /*   390 */    81,  122,   63,   84,   85,   70,   87,   75,   76,  118,
 /*   400 */    65,   79,   80,   81,   22,  118,   84,   85,   23,   87,
 /*   410 */    25,   75,   76,  119,  119,   79,   80,   81,   21,   21,
 /*   420 */    84,   85,   49,   87,   75,   76,   21,   24,   79,   80,
 /*   430 */    81,   24,   23,   84,   85,   24,   87,   75,   76,   24,
 /*   440 */    23,   79,   80,   81,   23,   23,   84,   85,   24,   87,
 /*   450 */    15,   75,   76,   21,   23,   79,   80,   81,   25,   49,
 /*   460 */    84,   85,    2,   87,   75,   76,   29,   24,   79,   80,
 /*   470 */    81,   24,   42,   84,   85,   25,   87,   75,   76,   21,
 /*   480 */    25,   79,   80,   81,   25,   24,   84,   85,   21,   87,
 /*   490 */    49,   75,   76,    4,   24,   79,   80,   81,   15,   25,
 /*   500 */    84,   85,   15,   87,   75,   76,   15,   15,   79,   80,
 /*   510 */    81,   15,   15,   84,   85,   17,   87,   75,   76,    0,
 /*   520 */     0,   79,   80,   81,   23,   14,   84,   85,  125,   87,
 /*   530 */   125,   75,   76,  125,  125,   79,   80,   81,  125,  125,
 /*   540 */    84,   85,  125,   87,   75,   76,  125,  125,   79,   80,
 /*   550 */    81,  125,  125,   84,   85,  125,   87,   78,   79,  125,
 /*   560 */   125,  125,   78,   79,  125,  125,   87,  125,  125,  125,
 /*   570 */   125,   87,  125,  125,   95,   96,   97,   98,  125,  100,
 /*   580 */    96,   97,   98,  125,  100,  125,  125,   78,   79,  125,
 /*   590 */   125,  125,   78,   79,  125,  125,   87,  125,  125,  125,
 /*   600 */   125,   87,  125,   73,   74,   96,   97,   98,  125,  100,
 /*   610 */    96,   97,   98,  125,  100,    8,    9,   10,   11,   12,
 /*   620 */     8,    9,   10,   11,   12,  125,  125,  125,  125,   22,
 /*   630 */   125,  125,    2,  103,  125,  125,   24,  125,    8,    9,
 /*   640 */    10,   11,   12,  125,  125,  125,  116,  125,   41,  125,
 /*   650 */   120,    8,    9,   10,   11,   12,  125,  125,  125,  125,
 /*   660 */   125,  125,  125,  125,  125,  125,  125,  125,  125,  125,
 /*   670 */   125,  125,  125,  125,  125,  125,  125,  125,  125,  125,
 /*   680 */   125,  125,  125,  125,  125,  125,  125,  125,  125,  125,
 /*   690 */   125,  125,  125,  125,  125,  125,  125,  125,  125,  125,
 /*   700 */   125,  125,  125,  125,  125,  125,  125,  125,  125,  125,
 /*   710 */   125,  125,  125,  125,  125,  125,  125,  125,  125,  125,
 /*   720 */   125,  125,  125,  125,  125,
};
#define YY_SHIFT_COUNT    (148)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (643)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   110,   64,   64,   64,   64,   64,   64,  142,  215,  239,
 /*    10 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    20 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    30 */   227,  227,  227,  227,   79,   36,   79,   79,    7,    7,
 /*    40 */     0,   32,   36,   63,   63,   63,  607,  232,   19,  146,
 /*    50 */    52,  168,  167,  167,   11,  182,  192,  246,  246,  182,
 /*    60 */   192,  246,  244,  259,  286,  292,  303,  328,  303,  338,
 /*    70 */   343,  303,  333,  356,  325,  329,  335,  335,  329,  382,
 /*    80 */   316,  630,  612,  643,  643,  643,  643,  643,  289,  130,
 /*    90 */    35,   35,   35,   35,  191,  250,  173,  331,  251,  132,
 /*   100 */   117,  230,  358,  385,  397,  398,  373,  403,  407,  409,
 /*   110 */   405,  411,  417,  421,  415,  422,  424,  435,  432,  433,
 /*   120 */   431,  398,  410,  460,  437,  443,  447,  450,  430,  458,
 /*   130 */   455,  459,  461,  467,  441,  470,  489,  483,  487,  491,
 /*   140 */   492,  496,  497,  474,  501,  498,  519,  520,  511,
};
#define YY_REDUCE_COUNT (79)
#define YY_REDUCE_MIN   (-102)
#define YY_REDUCE_MAX   (530)
static const short yy_reduce_ofst[] = {
 /*     0 */   530,   13,   33,   61,   92,  107,  126,  141,  188,  208,
 /*    10 */   -62,  223,  236,  256,  269,  282,  296,  309,  322,  336,
 /*    20 */   349,  362,  376,  389,  402,  416,  429,  442,  456,  469,
 /*    30 */   479,  484,  509,  514,  -13,  -75, -100,   27,   31,   49,
 /*    40 */   -85,  -85,  -69,   40,   45,  178,  -34, -102,  -16,   24,
 /*    50 */    18,  109,   76,   91,  123,  125,  152,  194,  203,  171,
 /*    60 */   184,  210,  204,  224,  231,  240,   18,  268,   18,  285,
 /*    70 */   288,   18,  280,  304,  262,  281,  294,  295,  287,  109,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   380,  380,  380,  380,  380,  380,  380,  380,  380,  380,
 /*    10 */   380,  380,  380,  380,  380,  380,  380,  380,  380,  380,
 /*    20 */   380,  380,  380,  380,  380,  380,  380,  380,  380,  380,
 /*    30 */   380,  380,  380,  380,  380,  380,  380,  380,  380,  380,
 /*    40 */   380,  380,  380,  449,  449,  449,  464,  511,  380,  472,
 /*    50 */   380,  380,  496,  496,  457,  479,  477,  380,  380,  479,
 /*    60 */   477,  380,  489,  487,  470,  468,  442,  380,  380,  380,
 /*    70 */   380,  443,  380,  380,  514,  498,  502,  502,  498,  380,
 /*    80 */   380,  380,  380,  418,  417,  416,  412,  413,  380,  380,
 /*    90 */   407,  408,  406,  405,  380,  380,  507,  380,  380,  380,
 /*   100 */   499,  503,  380,  396,  461,  471,  380,  380,  380,  380,
 /*   110 */   380,  380,  380,  380,  380,  380,  380,  380,  380,  396,
 /*   120 */   380,  488,  380,  437,  380,  517,  445,  380,  380,  441,
 /*   130 */   395,  380,  380,  497,  380,  380,  380,  380,  380,  380,
 /*   140 */   380,  380,  380,  380,  380,  380,  380,  380,  380,
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
  /*    1 */ "OR",
  /*    2 */ "AND",
  /*    3 */ "UNION",
  /*    4 */ "ALL",
  /*    5 */ "MINUS",
  /*    6 */ "EXCEPT",
  /*    7 */ "INTERSECT",
  /*    8 */ "NK_PLUS",
  /*    9 */ "NK_MINUS",
  /*   10 */ "NK_STAR",
  /*   11 */ "NK_SLASH",
  /*   12 */ "NK_REM",
  /*   13 */ "SHOW",
  /*   14 */ "DATABASES",
  /*   15 */ "NK_INTEGER",
  /*   16 */ "NK_FLOAT",
  /*   17 */ "NK_STRING",
  /*   18 */ "NK_BOOL",
  /*   19 */ "TIMESTAMP",
  /*   20 */ "NK_VARIABLE",
  /*   21 */ "NK_COMMA",
  /*   22 */ "NK_ID",
  /*   23 */ "NK_LP",
  /*   24 */ "NK_RP",
  /*   25 */ "NK_DOT",
  /*   26 */ "BETWEEN",
  /*   27 */ "NOT",
  /*   28 */ "IS",
  /*   29 */ "NULL",
  /*   30 */ "NK_LT",
  /*   31 */ "NK_GT",
  /*   32 */ "NK_LE",
  /*   33 */ "NK_GE",
  /*   34 */ "NK_NE",
  /*   35 */ "NK_EQ",
  /*   36 */ "LIKE",
  /*   37 */ "MATCH",
  /*   38 */ "NMATCH",
  /*   39 */ "IN",
  /*   40 */ "FROM",
  /*   41 */ "AS",
  /*   42 */ "JOIN",
  /*   43 */ "ON",
  /*   44 */ "INNER",
  /*   45 */ "SELECT",
  /*   46 */ "DISTINCT",
  /*   47 */ "WHERE",
  /*   48 */ "PARTITION",
  /*   49 */ "BY",
  /*   50 */ "SESSION",
  /*   51 */ "STATE_WINDOW",
  /*   52 */ "INTERVAL",
  /*   53 */ "SLIDING",
  /*   54 */ "FILL",
  /*   55 */ "VALUE",
  /*   56 */ "NONE",
  /*   57 */ "PREV",
  /*   58 */ "LINEAR",
  /*   59 */ "NEXT",
  /*   60 */ "GROUP",
  /*   61 */ "HAVING",
  /*   62 */ "ORDER",
  /*   63 */ "SLIMIT",
  /*   64 */ "SOFFSET",
  /*   65 */ "LIMIT",
  /*   66 */ "OFFSET",
  /*   67 */ "NK_LR",
  /*   68 */ "ASC",
  /*   69 */ "DESC",
  /*   70 */ "NULLS",
  /*   71 */ "FIRST",
  /*   72 */ "LAST",
  /*   73 */ "cmd",
  /*   74 */ "query_expression",
  /*   75 */ "literal",
  /*   76 */ "duration_literal",
  /*   77 */ "literal_list",
  /*   78 */ "db_name",
  /*   79 */ "table_name",
  /*   80 */ "column_name",
  /*   81 */ "function_name",
  /*   82 */ "table_alias",
  /*   83 */ "column_alias",
  /*   84 */ "expression",
  /*   85 */ "column_reference",
  /*   86 */ "expression_list",
  /*   87 */ "subquery",
  /*   88 */ "predicate",
  /*   89 */ "compare_op",
  /*   90 */ "in_op",
  /*   91 */ "in_predicate_value",
  /*   92 */ "boolean_value_expression",
  /*   93 */ "boolean_primary",
  /*   94 */ "from_clause",
  /*   95 */ "table_reference_list",
  /*   96 */ "table_reference",
  /*   97 */ "table_primary",
  /*   98 */ "joined_table",
  /*   99 */ "alias_opt",
  /*  100 */ "parenthesized_joined_table",
  /*  101 */ "join_type",
  /*  102 */ "search_condition",
  /*  103 */ "query_specification",
  /*  104 */ "set_quantifier_opt",
  /*  105 */ "select_list",
  /*  106 */ "where_clause_opt",
  /*  107 */ "partition_by_clause_opt",
  /*  108 */ "twindow_clause_opt",
  /*  109 */ "group_by_clause_opt",
  /*  110 */ "having_clause_opt",
  /*  111 */ "select_sublist",
  /*  112 */ "select_item",
  /*  113 */ "sliding_opt",
  /*  114 */ "fill_opt",
  /*  115 */ "fill_mode",
  /*  116 */ "query_expression_body",
  /*  117 */ "order_by_clause_opt",
  /*  118 */ "slimit_clause_opt",
  /*  119 */ "limit_clause_opt",
  /*  120 */ "query_primary",
  /*  121 */ "sort_specification_list",
  /*  122 */ "sort_specification",
  /*  123 */ "ordering_specification_opt",
  /*  124 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= SHOW DATABASES",
 /*   1 */ "cmd ::= query_expression",
 /*   2 */ "literal ::= NK_INTEGER",
 /*   3 */ "literal ::= NK_FLOAT",
 /*   4 */ "literal ::= NK_STRING",
 /*   5 */ "literal ::= NK_BOOL",
 /*   6 */ "literal ::= TIMESTAMP NK_STRING",
 /*   7 */ "literal ::= duration_literal",
 /*   8 */ "duration_literal ::= NK_VARIABLE",
 /*   9 */ "literal_list ::= literal",
 /*  10 */ "literal_list ::= literal_list NK_COMMA literal",
 /*  11 */ "db_name ::= NK_ID",
 /*  12 */ "table_name ::= NK_ID",
 /*  13 */ "column_name ::= NK_ID",
 /*  14 */ "function_name ::= NK_ID",
 /*  15 */ "table_alias ::= NK_ID",
 /*  16 */ "column_alias ::= NK_ID",
 /*  17 */ "expression ::= literal",
 /*  18 */ "expression ::= column_reference",
 /*  19 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /*  20 */ "expression ::= subquery",
 /*  21 */ "expression ::= NK_LP expression NK_RP",
 /*  22 */ "expression ::= NK_PLUS expression",
 /*  23 */ "expression ::= NK_MINUS expression",
 /*  24 */ "expression ::= expression NK_PLUS expression",
 /*  25 */ "expression ::= expression NK_MINUS expression",
 /*  26 */ "expression ::= expression NK_STAR expression",
 /*  27 */ "expression ::= expression NK_SLASH expression",
 /*  28 */ "expression ::= expression NK_REM expression",
 /*  29 */ "expression_list ::= expression",
 /*  30 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  31 */ "column_reference ::= column_name",
 /*  32 */ "column_reference ::= table_name NK_DOT column_name",
 /*  33 */ "predicate ::= expression compare_op expression",
 /*  34 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  35 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  36 */ "predicate ::= expression IS NULL",
 /*  37 */ "predicate ::= expression IS NOT NULL",
 /*  38 */ "predicate ::= expression in_op in_predicate_value",
 /*  39 */ "compare_op ::= NK_LT",
 /*  40 */ "compare_op ::= NK_GT",
 /*  41 */ "compare_op ::= NK_LE",
 /*  42 */ "compare_op ::= NK_GE",
 /*  43 */ "compare_op ::= NK_NE",
 /*  44 */ "compare_op ::= NK_EQ",
 /*  45 */ "compare_op ::= LIKE",
 /*  46 */ "compare_op ::= NOT LIKE",
 /*  47 */ "compare_op ::= MATCH",
 /*  48 */ "compare_op ::= NMATCH",
 /*  49 */ "in_op ::= IN",
 /*  50 */ "in_op ::= NOT IN",
 /*  51 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /*  52 */ "boolean_value_expression ::= boolean_primary",
 /*  53 */ "boolean_value_expression ::= NOT boolean_primary",
 /*  54 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /*  55 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /*  56 */ "boolean_primary ::= predicate",
 /*  57 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /*  58 */ "from_clause ::= FROM table_reference_list",
 /*  59 */ "table_reference_list ::= table_reference",
 /*  60 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /*  61 */ "table_reference ::= table_primary",
 /*  62 */ "table_reference ::= joined_table",
 /*  63 */ "table_primary ::= table_name alias_opt",
 /*  64 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /*  65 */ "table_primary ::= subquery alias_opt",
 /*  66 */ "alias_opt ::=",
 /*  67 */ "alias_opt ::= table_alias",
 /*  68 */ "alias_opt ::= AS table_alias",
 /*  69 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /*  70 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /*  71 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /*  72 */ "join_type ::= INNER",
 /*  73 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /*  74 */ "set_quantifier_opt ::=",
 /*  75 */ "set_quantifier_opt ::= DISTINCT",
 /*  76 */ "set_quantifier_opt ::= ALL",
 /*  77 */ "select_list ::= NK_STAR",
 /*  78 */ "select_list ::= select_sublist",
 /*  79 */ "select_sublist ::= select_item",
 /*  80 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /*  81 */ "select_item ::= expression",
 /*  82 */ "select_item ::= expression column_alias",
 /*  83 */ "select_item ::= expression AS column_alias",
 /*  84 */ "select_item ::= table_name NK_DOT NK_STAR",
 /*  85 */ "where_clause_opt ::=",
 /*  86 */ "where_clause_opt ::= WHERE search_condition",
 /*  87 */ "partition_by_clause_opt ::=",
 /*  88 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /*  89 */ "twindow_clause_opt ::=",
 /*  90 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /*  91 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /*  92 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /*  93 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /*  94 */ "sliding_opt ::=",
 /*  95 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /*  96 */ "fill_opt ::=",
 /*  97 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /*  98 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /*  99 */ "fill_mode ::= NONE",
 /* 100 */ "fill_mode ::= PREV",
 /* 101 */ "fill_mode ::= NULL",
 /* 102 */ "fill_mode ::= LINEAR",
 /* 103 */ "fill_mode ::= NEXT",
 /* 104 */ "group_by_clause_opt ::=",
 /* 105 */ "group_by_clause_opt ::= GROUP BY expression_list",
 /* 106 */ "having_clause_opt ::=",
 /* 107 */ "having_clause_opt ::= HAVING search_condition",
 /* 108 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 109 */ "query_expression_body ::= query_primary",
 /* 110 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 111 */ "query_primary ::= query_specification",
 /* 112 */ "query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP",
 /* 113 */ "order_by_clause_opt ::=",
 /* 114 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 115 */ "slimit_clause_opt ::=",
 /* 116 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 117 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 118 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 119 */ "limit_clause_opt ::=",
 /* 120 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 121 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 122 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 123 */ "subquery ::= NK_LR query_expression NK_RP",
 /* 124 */ "search_condition ::= boolean_value_expression",
 /* 125 */ "sort_specification_list ::= sort_specification",
 /* 126 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 127 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 128 */ "ordering_specification_opt ::=",
 /* 129 */ "ordering_specification_opt ::= ASC",
 /* 130 */ "ordering_specification_opt ::= DESC",
 /* 131 */ "null_ordering_opt ::=",
 /* 132 */ "null_ordering_opt ::= NULLS FIRST",
 /* 133 */ "null_ordering_opt ::= NULLS LAST",
 /* 134 */ "table_primary ::= parenthesized_joined_table",
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
    case 73: /* cmd */
    case 74: /* query_expression */
    case 75: /* literal */
    case 76: /* duration_literal */
    case 84: /* expression */
    case 85: /* column_reference */
    case 87: /* subquery */
    case 88: /* predicate */
    case 91: /* in_predicate_value */
    case 92: /* boolean_value_expression */
    case 93: /* boolean_primary */
    case 94: /* from_clause */
    case 95: /* table_reference_list */
    case 96: /* table_reference */
    case 97: /* table_primary */
    case 98: /* joined_table */
    case 100: /* parenthesized_joined_table */
    case 102: /* search_condition */
    case 103: /* query_specification */
    case 106: /* where_clause_opt */
    case 108: /* twindow_clause_opt */
    case 110: /* having_clause_opt */
    case 112: /* select_item */
    case 113: /* sliding_opt */
    case 114: /* fill_opt */
    case 116: /* query_expression_body */
    case 118: /* slimit_clause_opt */
    case 119: /* limit_clause_opt */
    case 120: /* query_primary */
    case 122: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy212)); 
}
      break;
    case 77: /* literal_list */
    case 86: /* expression_list */
    case 105: /* select_list */
    case 107: /* partition_by_clause_opt */
    case 109: /* group_by_clause_opt */
    case 111: /* select_sublist */
    case 117: /* order_by_clause_opt */
    case 121: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy174)); 
}
      break;
    case 78: /* db_name */
    case 79: /* table_name */
    case 80: /* column_name */
    case 81: /* function_name */
    case 82: /* table_alias */
    case 83: /* column_alias */
    case 99: /* alias_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 89: /* compare_op */
    case 90: /* in_op */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 101: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 104: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 115: /* fill_mode */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 123: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 124: /* null_ordering_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
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
  {   73,   -2 }, /* (0) cmd ::= SHOW DATABASES */
  {   73,   -1 }, /* (1) cmd ::= query_expression */
  {   75,   -1 }, /* (2) literal ::= NK_INTEGER */
  {   75,   -1 }, /* (3) literal ::= NK_FLOAT */
  {   75,   -1 }, /* (4) literal ::= NK_STRING */
  {   75,   -1 }, /* (5) literal ::= NK_BOOL */
  {   75,   -2 }, /* (6) literal ::= TIMESTAMP NK_STRING */
  {   75,   -1 }, /* (7) literal ::= duration_literal */
  {   76,   -1 }, /* (8) duration_literal ::= NK_VARIABLE */
  {   77,   -1 }, /* (9) literal_list ::= literal */
  {   77,   -3 }, /* (10) literal_list ::= literal_list NK_COMMA literal */
  {   78,   -1 }, /* (11) db_name ::= NK_ID */
  {   79,   -1 }, /* (12) table_name ::= NK_ID */
  {   80,   -1 }, /* (13) column_name ::= NK_ID */
  {   81,   -1 }, /* (14) function_name ::= NK_ID */
  {   82,   -1 }, /* (15) table_alias ::= NK_ID */
  {   83,   -1 }, /* (16) column_alias ::= NK_ID */
  {   84,   -1 }, /* (17) expression ::= literal */
  {   84,   -1 }, /* (18) expression ::= column_reference */
  {   84,   -4 }, /* (19) expression ::= function_name NK_LP expression_list NK_RP */
  {   84,   -1 }, /* (20) expression ::= subquery */
  {   84,   -3 }, /* (21) expression ::= NK_LP expression NK_RP */
  {   84,   -2 }, /* (22) expression ::= NK_PLUS expression */
  {   84,   -2 }, /* (23) expression ::= NK_MINUS expression */
  {   84,   -3 }, /* (24) expression ::= expression NK_PLUS expression */
  {   84,   -3 }, /* (25) expression ::= expression NK_MINUS expression */
  {   84,   -3 }, /* (26) expression ::= expression NK_STAR expression */
  {   84,   -3 }, /* (27) expression ::= expression NK_SLASH expression */
  {   84,   -3 }, /* (28) expression ::= expression NK_REM expression */
  {   86,   -1 }, /* (29) expression_list ::= expression */
  {   86,   -3 }, /* (30) expression_list ::= expression_list NK_COMMA expression */
  {   85,   -1 }, /* (31) column_reference ::= column_name */
  {   85,   -3 }, /* (32) column_reference ::= table_name NK_DOT column_name */
  {   88,   -3 }, /* (33) predicate ::= expression compare_op expression */
  {   88,   -5 }, /* (34) predicate ::= expression BETWEEN expression AND expression */
  {   88,   -6 }, /* (35) predicate ::= expression NOT BETWEEN expression AND expression */
  {   88,   -3 }, /* (36) predicate ::= expression IS NULL */
  {   88,   -4 }, /* (37) predicate ::= expression IS NOT NULL */
  {   88,   -3 }, /* (38) predicate ::= expression in_op in_predicate_value */
  {   89,   -1 }, /* (39) compare_op ::= NK_LT */
  {   89,   -1 }, /* (40) compare_op ::= NK_GT */
  {   89,   -1 }, /* (41) compare_op ::= NK_LE */
  {   89,   -1 }, /* (42) compare_op ::= NK_GE */
  {   89,   -1 }, /* (43) compare_op ::= NK_NE */
  {   89,   -1 }, /* (44) compare_op ::= NK_EQ */
  {   89,   -1 }, /* (45) compare_op ::= LIKE */
  {   89,   -2 }, /* (46) compare_op ::= NOT LIKE */
  {   89,   -1 }, /* (47) compare_op ::= MATCH */
  {   89,   -1 }, /* (48) compare_op ::= NMATCH */
  {   90,   -1 }, /* (49) in_op ::= IN */
  {   90,   -2 }, /* (50) in_op ::= NOT IN */
  {   91,   -3 }, /* (51) in_predicate_value ::= NK_LP expression_list NK_RP */
  {   92,   -1 }, /* (52) boolean_value_expression ::= boolean_primary */
  {   92,   -2 }, /* (53) boolean_value_expression ::= NOT boolean_primary */
  {   92,   -3 }, /* (54) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {   92,   -3 }, /* (55) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {   93,   -1 }, /* (56) boolean_primary ::= predicate */
  {   93,   -3 }, /* (57) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {   94,   -2 }, /* (58) from_clause ::= FROM table_reference_list */
  {   95,   -1 }, /* (59) table_reference_list ::= table_reference */
  {   95,   -3 }, /* (60) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {   96,   -1 }, /* (61) table_reference ::= table_primary */
  {   96,   -1 }, /* (62) table_reference ::= joined_table */
  {   97,   -2 }, /* (63) table_primary ::= table_name alias_opt */
  {   97,   -4 }, /* (64) table_primary ::= db_name NK_DOT table_name alias_opt */
  {   97,   -2 }, /* (65) table_primary ::= subquery alias_opt */
  {   99,    0 }, /* (66) alias_opt ::= */
  {   99,   -1 }, /* (67) alias_opt ::= table_alias */
  {   99,   -2 }, /* (68) alias_opt ::= AS table_alias */
  {  100,   -3 }, /* (69) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  100,   -3 }, /* (70) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {   98,   -6 }, /* (71) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  101,   -1 }, /* (72) join_type ::= INNER */
  {  103,   -9 }, /* (73) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  104,    0 }, /* (74) set_quantifier_opt ::= */
  {  104,   -1 }, /* (75) set_quantifier_opt ::= DISTINCT */
  {  104,   -1 }, /* (76) set_quantifier_opt ::= ALL */
  {  105,   -1 }, /* (77) select_list ::= NK_STAR */
  {  105,   -1 }, /* (78) select_list ::= select_sublist */
  {  111,   -1 }, /* (79) select_sublist ::= select_item */
  {  111,   -3 }, /* (80) select_sublist ::= select_sublist NK_COMMA select_item */
  {  112,   -1 }, /* (81) select_item ::= expression */
  {  112,   -2 }, /* (82) select_item ::= expression column_alias */
  {  112,   -3 }, /* (83) select_item ::= expression AS column_alias */
  {  112,   -3 }, /* (84) select_item ::= table_name NK_DOT NK_STAR */
  {  106,    0 }, /* (85) where_clause_opt ::= */
  {  106,   -2 }, /* (86) where_clause_opt ::= WHERE search_condition */
  {  107,    0 }, /* (87) partition_by_clause_opt ::= */
  {  107,   -3 }, /* (88) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  108,    0 }, /* (89) twindow_clause_opt ::= */
  {  108,   -6 }, /* (90) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  108,   -4 }, /* (91) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  108,   -6 }, /* (92) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  108,   -8 }, /* (93) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  113,    0 }, /* (94) sliding_opt ::= */
  {  113,   -4 }, /* (95) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  114,    0 }, /* (96) fill_opt ::= */
  {  114,   -4 }, /* (97) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  114,   -6 }, /* (98) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  115,   -1 }, /* (99) fill_mode ::= NONE */
  {  115,   -1 }, /* (100) fill_mode ::= PREV */
  {  115,   -1 }, /* (101) fill_mode ::= NULL */
  {  115,   -1 }, /* (102) fill_mode ::= LINEAR */
  {  115,   -1 }, /* (103) fill_mode ::= NEXT */
  {  109,    0 }, /* (104) group_by_clause_opt ::= */
  {  109,   -3 }, /* (105) group_by_clause_opt ::= GROUP BY expression_list */
  {  110,    0 }, /* (106) having_clause_opt ::= */
  {  110,   -2 }, /* (107) having_clause_opt ::= HAVING search_condition */
  {   74,   -4 }, /* (108) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  116,   -1 }, /* (109) query_expression_body ::= query_primary */
  {  116,   -4 }, /* (110) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  120,   -1 }, /* (111) query_primary ::= query_specification */
  {  120,   -6 }, /* (112) query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
  {  117,    0 }, /* (113) order_by_clause_opt ::= */
  {  117,   -3 }, /* (114) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  118,    0 }, /* (115) slimit_clause_opt ::= */
  {  118,   -2 }, /* (116) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  118,   -4 }, /* (117) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  118,   -4 }, /* (118) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  119,    0 }, /* (119) limit_clause_opt ::= */
  {  119,   -2 }, /* (120) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  119,   -4 }, /* (121) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  119,   -4 }, /* (122) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {   87,   -3 }, /* (123) subquery ::= NK_LR query_expression NK_RP */
  {  102,   -1 }, /* (124) search_condition ::= boolean_value_expression */
  {  121,   -1 }, /* (125) sort_specification_list ::= sort_specification */
  {  121,   -3 }, /* (126) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  122,   -3 }, /* (127) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  123,    0 }, /* (128) ordering_specification_opt ::= */
  {  123,   -1 }, /* (129) ordering_specification_opt ::= ASC */
  {  123,   -1 }, /* (130) ordering_specification_opt ::= DESC */
  {  124,    0 }, /* (131) null_ordering_opt ::= */
  {  124,   -2 }, /* (132) null_ordering_opt ::= NULLS FIRST */
  {  124,   -2 }, /* (133) null_ordering_opt ::= NULLS LAST */
  {   97,   -1 }, /* (134) table_primary ::= parenthesized_joined_table */
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
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy212; }
        break;
      case 2: /* literal ::= NK_INTEGER */
{ PARSER_TRACE; yylhsminor.yy212 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 3: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy212 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 4: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy212 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 5: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy212 = createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 6: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yymsp[-1].minor.yy212 = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0); }
        break;
      case 7: /* literal ::= duration_literal */
      case 17: /* expression ::= literal */ yytestcase(yyruleno==17);
      case 18: /* expression ::= column_reference */ yytestcase(yyruleno==18);
      case 20: /* expression ::= subquery */ yytestcase(yyruleno==20);
      case 52: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==52);
      case 56: /* boolean_primary ::= predicate */ yytestcase(yyruleno==56);
      case 59: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==59);
      case 61: /* table_reference ::= table_primary */ yytestcase(yyruleno==61);
      case 62: /* table_reference ::= joined_table */ yytestcase(yyruleno==62);
      case 81: /* select_item ::= expression */ yytestcase(yyruleno==81);
      case 109: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==109);
      case 111: /* query_primary ::= query_specification */ yytestcase(yyruleno==111);
      case 124: /* search_condition ::= boolean_value_expression */ yytestcase(yyruleno==124);
{ PARSER_TRACE; yylhsminor.yy212 = yymsp[0].minor.yy212; }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 8: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy212 = createDurationValueNode(pCxt, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 9: /* literal_list ::= literal */
      case 29: /* expression_list ::= expression */ yytestcase(yyruleno==29);
      case 79: /* select_sublist ::= select_item */ yytestcase(yyruleno==79);
      case 125: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==125);
{ PARSER_TRACE; yylhsminor.yy174 = createNodeList(pCxt, yymsp[0].minor.yy212); }
  yymsp[0].minor.yy174 = yylhsminor.yy174;
        break;
      case 10: /* literal_list ::= literal_list NK_COMMA literal */
      case 30: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==30);
      case 80: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==80);
      case 126: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==126);
{ PARSER_TRACE; yylhsminor.yy174 = addNodeToList(pCxt, yymsp[-2].minor.yy174, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy174 = yylhsminor.yy174;
        break;
      case 11: /* db_name ::= NK_ID */
      case 12: /* table_name ::= NK_ID */ yytestcase(yyruleno==12);
      case 13: /* column_name ::= NK_ID */ yytestcase(yyruleno==13);
      case 14: /* function_name ::= NK_ID */ yytestcase(yyruleno==14);
      case 15: /* table_alias ::= NK_ID */ yytestcase(yyruleno==15);
      case 16: /* column_alias ::= NK_ID */ yytestcase(yyruleno==16);
{ PARSER_TRACE; yylhsminor.yy79 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy79 = yylhsminor.yy79;
        break;
      case 19: /* expression ::= function_name NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy212 = createFunctionNode(pCxt, &yymsp[-3].minor.yy79, yymsp[-1].minor.yy174); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 21: /* expression ::= NK_LP expression NK_RP */
      case 57: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==57);
      case 69: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */ yytestcase(yyruleno==69);
      case 70: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==70);
      case 123: /* subquery ::= NK_LR query_expression NK_RP */ yytestcase(yyruleno==123);
{ PARSER_TRACE; yymsp[-2].minor.yy212 = yymsp[-1].minor.yy212; }
        break;
      case 22: /* expression ::= NK_PLUS expression */
      case 58: /* from_clause ::= FROM table_reference_list */ yytestcase(yyruleno==58);
      case 86: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==86);
      case 107: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==107);
{ PARSER_TRACE; yymsp[-1].minor.yy212 = yymsp[0].minor.yy212; }
        break;
      case 23: /* expression ::= NK_MINUS expression */
{ PARSER_TRACE; yymsp[-1].minor.yy212 = createOperatorNode(pCxt, OP_TYPE_SUB, yymsp[0].minor.yy212, NULL); }
        break;
      case 24: /* expression ::= expression NK_PLUS expression */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, OP_TYPE_ADD, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 25: /* expression ::= expression NK_MINUS expression */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, OP_TYPE_SUB, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 26: /* expression ::= expression NK_STAR expression */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, OP_TYPE_MULTI, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 27: /* expression ::= expression NK_SLASH expression */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, OP_TYPE_DIV, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 28: /* expression ::= expression NK_REM expression */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, OP_TYPE_MOD, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 31: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy212 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy79); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 32: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy212 = createColumnNode(pCxt, &yymsp[-2].minor.yy79, &yymsp[0].minor.yy79); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 33: /* predicate ::= expression compare_op expression */
      case 38: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==38);
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, yymsp[-1].minor.yy40, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 34: /* predicate ::= expression BETWEEN expression AND expression */
{ PARSER_TRACE; yylhsminor.yy212 = createBetweenAnd(pCxt, yymsp[-4].minor.yy212, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-4].minor.yy212 = yylhsminor.yy212;
        break;
      case 35: /* predicate ::= expression NOT BETWEEN expression AND expression */
{ PARSER_TRACE; yylhsminor.yy212 = createNotBetweenAnd(pCxt, yymsp[-2].minor.yy212, yymsp[-5].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-5].minor.yy212 = yylhsminor.yy212;
        break;
      case 36: /* predicate ::= expression IS NULL */
{ PARSER_TRACE; yylhsminor.yy212 = createIsNullCondNode(pCxt, yymsp[-2].minor.yy212, true); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 37: /* predicate ::= expression IS NOT NULL */
{ PARSER_TRACE; yylhsminor.yy212 = createIsNullCondNode(pCxt, yymsp[-3].minor.yy212, false); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 39: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_LOWER_THAN; }
        break;
      case 40: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_GREATER_THAN; }
        break;
      case 41: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 42: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 43: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_NOT_EQUAL; }
        break;
      case 44: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_EQUAL; }
        break;
      case 45: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_LIKE; }
        break;
      case 46: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy40 = OP_TYPE_NOT_LIKE; }
        break;
      case 47: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_MATCH; }
        break;
      case 48: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_NMATCH; }
        break;
      case 49: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_IN; }
        break;
      case 50: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy40 = OP_TYPE_NOT_IN; }
        break;
      case 51: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yymsp[-2].minor.yy212 = createNodeListNode(pCxt, yymsp[-1].minor.yy174); }
        break;
      case 53: /* boolean_value_expression ::= NOT boolean_primary */
{ PARSER_TRACE; yymsp[-1].minor.yy212 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, yymsp[0].minor.yy212, NULL); }
        break;
      case 54: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy212 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 55: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy212 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 60: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy212 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy212, yymsp[0].minor.yy212, NULL); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 63: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy79, &yymsp[0].minor.yy79); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 64: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createRealTableNode(pCxt, &yymsp[-3].minor.yy79, &yymsp[-1].minor.yy79, &yymsp[0].minor.yy79); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 65: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createTempTableNode(pCxt, yymsp[-1].minor.yy212, &yymsp[0].minor.yy79); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 66: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy79 = nil_token;  }
        break;
      case 67: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy79 = yymsp[0].minor.yy79; }
  yymsp[0].minor.yy79 = yylhsminor.yy79;
        break;
      case 68: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy79 = yymsp[0].minor.yy79; }
        break;
      case 71: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy212 = createJoinTableNode(pCxt, yymsp[-4].minor.yy162, yymsp[-5].minor.yy212, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-5].minor.yy212 = yylhsminor.yy212;
        break;
      case 72: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy162 = JOIN_TYPE_INNER; }
        break;
      case 73: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy212 = createSelectStmt(pCxt, yymsp[-7].minor.yy237, yymsp[-6].minor.yy174, yymsp[-5].minor.yy212);
                                                                                    yymsp[-8].minor.yy212 = addWhereClause(pCxt, yymsp[-8].minor.yy212, yymsp[-4].minor.yy212);
                                                                                    yymsp[-8].minor.yy212 = addPartitionByClause(pCxt, yymsp[-8].minor.yy212, yymsp[-3].minor.yy174);
                                                                                    yymsp[-8].minor.yy212 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy212, yymsp[-2].minor.yy212);
                                                                                    yymsp[-8].minor.yy212 = addGroupByClause(pCxt, yymsp[-8].minor.yy212, yymsp[-1].minor.yy174);
                                                                                    yymsp[-8].minor.yy212 = addHavingClause(pCxt, yymsp[-8].minor.yy212, yymsp[0].minor.yy212);
                                                                                  }
        break;
      case 74: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy237 = false; }
        break;
      case 75: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy237 = true; }
        break;
      case 76: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy237 = false; }
        break;
      case 77: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy174 = NULL; }
        break;
      case 78: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy174 = yymsp[0].minor.yy174; }
  yymsp[0].minor.yy174 = yylhsminor.yy174;
        break;
      case 82: /* select_item ::= expression column_alias */
{ PARSER_TRACE; yylhsminor.yy212 = setProjectionAlias(pCxt, yymsp[-1].minor.yy212, &yymsp[0].minor.yy79); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 83: /* select_item ::= expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy212 = setProjectionAlias(pCxt, yymsp[-2].minor.yy212, &yymsp[0].minor.yy79); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 84: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy212 = createColumnNode(pCxt, &yymsp[-2].minor.yy79, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 85: /* where_clause_opt ::= */
      case 89: /* twindow_clause_opt ::= */ yytestcase(yyruleno==89);
      case 94: /* sliding_opt ::= */ yytestcase(yyruleno==94);
      case 96: /* fill_opt ::= */ yytestcase(yyruleno==96);
      case 106: /* having_clause_opt ::= */ yytestcase(yyruleno==106);
      case 115: /* slimit_clause_opt ::= */ yytestcase(yyruleno==115);
      case 119: /* limit_clause_opt ::= */ yytestcase(yyruleno==119);
{ PARSER_TRACE; yymsp[1].minor.yy212 = NULL; }
        break;
      case 87: /* partition_by_clause_opt ::= */
      case 104: /* group_by_clause_opt ::= */ yytestcase(yyruleno==104);
      case 113: /* order_by_clause_opt ::= */ yytestcase(yyruleno==113);
{ PARSER_TRACE; yymsp[1].minor.yy174 = NULL; }
        break;
      case 88: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 105: /* group_by_clause_opt ::= GROUP BY expression_list */ yytestcase(yyruleno==105);
      case 114: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==114);
{ PARSER_TRACE; yymsp[-2].minor.yy174 = yymsp[0].minor.yy174; }
        break;
      case 90: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = createSessionWindowNode(pCxt, yymsp[-3].minor.yy212, &yymsp[-1].minor.yy0); }
        break;
      case 91: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createStateWindowNode(pCxt, yymsp[-1].minor.yy212); }
        break;
      case 92: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy212, NULL, yymsp[-1].minor.yy212, yymsp[0].minor.yy212); }
        break;
      case 93: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy212 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy212, yymsp[-3].minor.yy212, yymsp[-1].minor.yy212, yymsp[0].minor.yy212); }
        break;
      case 95: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy212 = yymsp[-1].minor.yy212; }
        break;
      case 97: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createFillNode(pCxt, yymsp[-1].minor.yy44, NULL); }
        break;
      case 98: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy174)); }
        break;
      case 99: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_NONE; }
        break;
      case 100: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_PREV; }
        break;
      case 101: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_NULL; }
        break;
      case 102: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_LINEAR; }
        break;
      case 103: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_NEXT; }
        break;
      case 108: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy212 = addOrderByClause(pCxt, yymsp[-3].minor.yy212, yymsp[-2].minor.yy174);
                                                                                    yylhsminor.yy212 = addSlimitClause(pCxt, yylhsminor.yy212, yymsp[-1].minor.yy212);
                                                                                    yylhsminor.yy212 = addLimitClause(pCxt, yylhsminor.yy212, yymsp[0].minor.yy212);
                                                                                  }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 110: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy212 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 112: /* query_primary ::= NK_LP query_expression_body order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = yymsp[-4].minor.yy212;}
  yy_destructor(yypParser,117,&yymsp[-3].minor);
  yy_destructor(yypParser,119,&yymsp[-2].minor);
  yy_destructor(yypParser,118,&yymsp[-1].minor);
        break;
      case 116: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 120: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==120);
{ PARSER_TRACE; yymsp[-1].minor.yy212 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 117: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 121: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==121);
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 118: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 122: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==122);
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 127: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createOrderByExprNode(pCxt, yymsp[-2].minor.yy212, yymsp[-1].minor.yy188, yymsp[0].minor.yy107); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 128: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy188 = ORDER_ASC; }
        break;
      case 129: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy188 = ORDER_ASC; }
        break;
      case 130: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy188 = ORDER_DESC; }
        break;
      case 131: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy107 = NULL_ORDER_DEFAULT; }
        break;
      case 132: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy107 = NULL_ORDER_FIRST; }
        break;
      case 133: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy107 = NULL_ORDER_LAST; }
        break;
      case 134: /* table_primary ::= parenthesized_joined_table */
{  yy_destructor(yypParser,100,&yymsp[0].minor);
{
}
}
        break;
      default:
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
 PARSER_COMPLETE; 
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
