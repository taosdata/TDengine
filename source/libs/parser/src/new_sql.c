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
#define YYNSTATE             147
#define YYNRULE              138
#define YYNTOKEN             72
#define YY_MAX_SHIFT         146
#define YY_MIN_SHIFTREDUCE   243
#define YY_MAX_SHIFTREDUCE   380
#define YY_ERROR_ACTION      381
#define YY_ACCEPT_ACTION     382
#define YY_NO_ACTION         383
#define YY_MIN_REDUCE        384
#define YY_MAX_REDUCE        521
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
#define YY_ACTTAB_COUNT (737)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   393,  391,   92,   23,   66,  394,  391,  469,   30,   28,
 /*    10 */    26,   25,   24,  401,  391,  141,  416,  141,  416,  142,
 /*    20 */    72,  112,   46,  402,  265,  405,   22,   87,   96,  117,
 /*    30 */   283,  284,  285,  286,  287,  288,  289,  291,  292,  293,
 /*    40 */    30,   28,   26,   25,   24,  401,  391,  141,  416,  141,
 /*    50 */   416,  142,  146,  116,   46,  402,   54,  405,   22,   87,
 /*    60 */    96,  514,  283,  284,  285,  286,  287,  288,  289,  291,
 /*    70 */   292,  293,  131,  401,  391,  128,    9,  141,  416,  142,
 /*    80 */     4,  318,   39,  402,   54,  405,  441,  109,   29,   27,
 /*    90 */    86,  437,  132,  513,   63,  245,  246,  247,  248,  143,
 /*   100 */   251,  500,  101,    1,  118,  113,  111,   10,  245,  246,
 /*   110 */   247,  248,  143,  251,  500,   53,  453,  401,  391,  498,
 /*   120 */   258,  141,  416,  142,  126,   54,   40,  402,  499,  405,
 /*   130 */   441,  453,  498,  450,   94,  437,  453,  401,  391,   69,
 /*   140 */   330,  141,  416,  142,  473,  417,   40,  402,  449,  405,
 /*   150 */   441,  134,  256,  448,   94,  437,   41,  401,  391,  344,
 /*   160 */     8,  141,  416,  142,  496,   74,   40,  402,  347,  405,
 /*   170 */   441,  458,  110,  318,   94,  437,  131,  401,  391,  107,
 /*   180 */   322,  141,  416,  142,  457,   57,   78,  402,   60,  405,
 /*   190 */   128,    9,   29,   27,  108,  345,  346,  348,  349,  245,
 /*   200 */   246,  247,  248,  143,  251,  500,  101,    1,   19,   31,
 /*   210 */   133,   10,  295,   54,   30,   28,   26,   25,   24,   53,
 /*   220 */    29,   27,  321,  498,   26,   25,   24,  245,  246,  247,
 /*   230 */   248,  143,  251,  480,  101,    5,  401,  391,    7,    6,
 /*   240 */   141,  416,  142,    7,    6,   40,  402,  251,  405,  441,
 /*   250 */   122,  138,  280,  440,  437,   55,  398,   54,  396,  401,
 /*   260 */   391,  301,  105,  141,  416,  142,  379,  380,   40,  402,
 /*   270 */   106,  405,  441,   29,   27,  323,  121,  437,  479,   59,
 /*   280 */   245,  246,  247,  248,  143,  251,   93,  101,    5,   30,
 /*   290 */    28,   26,   25,   24,  401,  391,  139,  131,  129,  416,
 /*   300 */   142,  125,   44,   45,  402,  265,  405,    3,  460,  135,
 /*   310 */    42,  120,   20,   30,   28,   26,   25,   24,   62,   67,
 /*   320 */   446,  124,  290,  123,   70,  294,  500,  259,  104,   21,
 /*   330 */   102,  466,   48,   64,    2,   30,   28,   26,   25,   24,
 /*   340 */    53,   16,   31,  423,  498,  262,   71,  318,  255,   29,
 /*   350 */    27,  130,  136,   43,  258,  454,  245,  246,  247,  248,
 /*   360 */   143,  251,   32,  101,    5,   29,   27,   30,   28,   26,
 /*   370 */    25,   24,  245,  246,  247,  248,  143,  251,   65,  101,
 /*   380 */     1,  401,  391,  259,  470,  141,  416,  142,   97,  140,
 /*   390 */    40,  402,  515,  405,  441,   29,   27,  497,  137,  438,
 /*   400 */    73,  256,  245,  246,  247,  248,  143,  251,   56,  101,
 /*   410 */     5,   13,   30,   28,   26,   25,   24,   31,   14,  401,
 /*   420 */   391,  341,   58,  141,  416,  142,  376,  377,   84,  402,
 /*   430 */   100,  405,  401,  391,   35,  343,  129,  416,  142,   47,
 /*   440 */    61,   45,  402,  337,  405,   36,  114,  401,  391,  382,
 /*   450 */   144,  141,  416,  142,  336,   37,   84,  402,  103,  405,
 /*   460 */   115,  396,  401,  391,   18,  315,  141,  416,  142,  467,
 /*   470 */     6,   80,  402,   15,  405,  281,  314,  401,  391,  500,
 /*   480 */    68,  141,  416,  142,   33,   34,   84,  402,   95,  405,
 /*   490 */   395,   52,   17,   53,  263,  401,  391,  498,  370,  141,
 /*   500 */   416,  142,   11,  119,   79,  402,   38,  405,  401,  391,
 /*   510 */   365,   75,  141,  416,  142,  364,   98,   81,  402,  369,
 /*   520 */   405,  368,  401,  391,  385,   99,  141,  416,  142,  249,
 /*   530 */    12,   76,  402,  384,  405,  145,  383,  401,  391,  383,
 /*   540 */   383,  141,  416,  142,  383,  383,   82,  402,  383,  405,
 /*   550 */   383,  383,  401,  391,  383,  383,  141,  416,  142,  383,
 /*   560 */   383,   77,  402,  383,  405,  383,  383,  383,  383,  383,
 /*   570 */   401,  391,  383,  383,  141,  416,  142,  383,  383,   83,
 /*   580 */   402,  383,  405,  401,  391,  383,  383,  141,  416,  142,
 /*   590 */   383,  383,  413,  402,  383,  405,  383,  401,  391,  383,
 /*   600 */   383,  141,  416,  142,  383,  383,  412,  402,  383,  405,
 /*   610 */   383,  383,  401,  391,  383,  383,  141,  416,  142,  383,
 /*   620 */   383,  411,  402,  383,  405,  383,  383,  401,  391,  383,
 /*   630 */   383,  141,  416,  142,  383,  383,   90,  402,  383,  405,
 /*   640 */   383,  383,  383,  383,  383,  401,  391,  383,  383,  141,
 /*   650 */   416,  142,  383,  383,   89,  402,  383,  405,  401,  391,
 /*   660 */   383,  383,  141,  416,  142,  383,  383,   91,  402,  383,
 /*   670 */   405,  383,  401,  391,  383,  383,  141,  416,  142,  383,
 /*   680 */   383,   88,  402,  383,  405,  125,   44,  401,  391,  383,
 /*   690 */   383,  141,  416,  142,   42,  383,   85,  402,  383,  405,
 /*   700 */   125,   44,  127,   49,  446,  447,  383,  451,  383,   42,
 /*   710 */   383,  383,  383,  383,  125,   44,  383,  383,   50,  446,
 /*   720 */   447,  383,  451,   42,  383,  383,  383,  383,  383,  383,
 /*   730 */   383,  383,   51,  446,  447,  383,  451,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */    74,   75,   76,   88,   89,   74,   75,   82,    8,    9,
 /*    10 */    10,   11,   12,   74,   75,   78,   79,   78,   79,   80,
 /*    20 */   123,   84,   83,   84,   24,   86,   26,   27,   28,   22,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */     8,    9,   10,   11,   12,   74,   75,   78,   79,   78,
 /*    50 */    79,   80,   13,   84,   83,   84,   45,   86,   26,   27,
 /*    60 */    28,  122,   30,   31,   32,   33,   34,   35,   36,   37,
 /*    70 */    38,   39,   73,   74,   75,   22,   23,   78,   79,   80,
 /*    80 */    43,   44,   83,   84,   45,   86,   87,  114,    8,    9,
 /*    90 */    91,   92,  121,  122,  107,   15,   16,   17,   18,   19,
 /*   100 */    20,  102,   22,   23,   50,   51,   52,   27,   15,   16,
 /*   110 */    17,   18,   19,   20,  102,  116,   81,   74,   75,  120,
 /*   120 */    22,   78,   79,   80,  100,   45,   83,   84,  116,   86,
 /*   130 */    87,   81,  120,   98,   91,   92,   81,   74,   75,   41,
 /*   140 */    10,   78,   79,   80,  101,   79,   83,   84,   98,   86,
 /*   150 */    87,    3,   22,   98,   91,   92,   21,   74,   75,   24,
 /*   160 */   103,   78,   79,   80,  101,  117,   83,   84,   29,   86,
 /*   170 */    87,   42,   54,   44,   91,   92,   73,   74,   75,   53,
 /*   180 */     4,   78,   79,   80,  101,   21,   83,   84,   24,   86,
 /*   190 */    22,   23,    8,    9,   55,   56,   57,   58,   59,   15,
 /*   200 */    16,   17,   18,   19,   20,  102,   22,   23,    2,   21,
 /*   210 */    62,   27,   24,   45,    8,    9,   10,   11,   12,  116,
 /*   220 */     8,    9,   46,  120,   10,   11,   12,   15,   16,   17,
 /*   230 */    18,   19,   20,  113,   22,   23,   74,   75,    1,    2,
 /*   240 */    78,   79,   80,    1,    2,   83,   84,   20,   86,   87,
 /*   250 */    27,   21,   29,   91,   92,  112,   23,   45,   25,   74,
 /*   260 */    75,   24,   75,   78,   79,   80,   70,   71,   83,   84,
 /*   270 */    75,   86,   87,    8,    9,   10,   91,   92,  113,  112,
 /*   280 */    15,   16,   17,   18,   19,   20,   75,   22,   23,    8,
 /*   290 */     9,   10,   11,   12,   74,   75,   66,   73,   78,   79,
 /*   300 */    80,   77,   78,   83,   84,   24,   86,   61,  109,   21,
 /*   310 */    86,   60,   26,    8,    9,   10,   11,   12,  108,   95,
 /*   320 */    96,   97,   36,   99,  104,   39,  102,   22,   48,    2,
 /*   330 */   110,  111,  106,  105,   47,    8,    9,   10,   11,   12,
 /*   340 */   116,   23,   21,   90,  120,   24,   41,   44,   22,    8,
 /*   350 */     9,   10,   64,   78,   22,   81,   15,   16,   17,   18,
 /*   360 */    19,   20,   40,   22,   23,    8,    9,    8,    9,   10,
 /*   370 */    11,   12,   15,   16,   17,   18,   19,   20,   93,   22,
 /*   380 */    23,   74,   75,   22,   82,   78,   79,   80,   69,   65,
 /*   390 */    83,   84,  124,   86,   87,    8,    9,  119,   63,   92,
 /*   400 */   118,   22,   15,   16,   17,   18,   19,   20,   24,   22,
 /*   410 */    23,   21,    8,    9,   10,   11,   12,   21,   49,   74,
 /*   420 */    75,   24,   23,   78,   79,   80,   67,   68,   83,   84,
 /*   430 */    85,   86,   74,   75,   21,   24,   78,   79,   80,   23,
 /*   440 */    23,   83,   84,   24,   86,   23,   15,   74,   75,   72,
 /*   450 */    73,   78,   79,   80,   24,   23,   83,   84,   85,   86,
 /*   460 */    21,   25,   74,   75,   21,   24,   78,   79,   80,  111,
 /*   470 */     2,   83,   84,   49,   86,   29,   24,   74,   75,  102,
 /*   480 */    25,   78,   79,   80,   42,   21,   83,   84,   85,   86,
 /*   490 */    25,   25,   21,  116,   24,   74,   75,  120,   24,   78,
 /*   500 */    79,   80,   49,  115,   83,   84,    4,   86,   74,   75,
 /*   510 */    15,   25,   78,   79,   80,   15,   15,   83,   84,   15,
 /*   520 */    86,   15,   74,   75,    0,   15,   78,   79,   80,   17,
 /*   530 */    23,   83,   84,    0,   86,   14,  125,   74,   75,  125,
 /*   540 */   125,   78,   79,   80,  125,  125,   83,   84,  125,   86,
 /*   550 */   125,  125,   74,   75,  125,  125,   78,   79,   80,  125,
 /*   560 */   125,   83,   84,  125,   86,  125,  125,  125,  125,  125,
 /*   570 */    74,   75,  125,  125,   78,   79,   80,  125,  125,   83,
 /*   580 */    84,  125,   86,   74,   75,  125,  125,   78,   79,   80,
 /*   590 */   125,  125,   83,   84,  125,   86,  125,   74,   75,  125,
 /*   600 */   125,   78,   79,   80,  125,  125,   83,   84,  125,   86,
 /*   610 */   125,  125,   74,   75,  125,  125,   78,   79,   80,  125,
 /*   620 */   125,   83,   84,  125,   86,  125,  125,   74,   75,  125,
 /*   630 */   125,   78,   79,   80,  125,  125,   83,   84,  125,   86,
 /*   640 */   125,  125,  125,  125,  125,   74,   75,  125,  125,   78,
 /*   650 */    79,   80,  125,  125,   83,   84,  125,   86,   74,   75,
 /*   660 */   125,  125,   78,   79,   80,  125,  125,   83,   84,  125,
 /*   670 */    86,  125,   74,   75,  125,  125,   78,   79,   80,  125,
 /*   680 */   125,   83,   84,  125,   86,   77,   78,   74,   75,  125,
 /*   690 */   125,   78,   79,   80,   86,  125,   83,   84,  125,   86,
 /*   700 */    77,   78,   94,   95,   96,   97,  125,   99,  125,   86,
 /*   710 */   125,  125,  125,  125,   77,   78,  125,  125,   95,   96,
 /*   720 */    97,  125,   99,   86,  125,  125,  125,  125,  125,  125,
 /*   730 */   125,  125,   95,   96,   97,  125,   99,
};
#define YY_SHIFT_COUNT    (146)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (533)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    39,   80,  184,  184,  184,  212,  184,  184,  265,  168,
 /*    10 */   357,  387,  341,  387,  387,  387,  387,  387,  387,  387,
 /*    20 */   387,  387,  387,  387,  387,  387,  387,  387,  387,  387,
 /*    30 */   387,  387,   53,   53,   53,   93,    7,    7,   11,    0,
 /*    40 */    32,   93,   98,   98,   98,  305,  359,  139,   54,  129,
 /*    50 */    37,  129,  130,  148,  176,  118,  126,  227,  227,  118,
 /*    60 */   126,  227,  246,  251,  280,  287,  318,  303,  326,  332,
 /*    70 */   322,  361,  319,  324,  335,  379,  206,  327,  281,  404,
 /*    80 */   404,  404,  404,  404,  404,  404,  237,  286,  214,  214,
 /*    90 */   214,  214,  135,  164,  242,  188,  223,  196,  288,  230,
 /*   100 */   321,  233,  390,  396,  369,  384,  397,  399,  413,  411,
 /*   110 */   416,  417,  419,  422,  430,  431,  439,  436,  432,  443,
 /*   120 */   424,  468,  446,  441,  452,  455,  442,  464,  465,  466,
 /*   130 */   470,  474,  471,  453,  502,  495,  500,  501,  504,  506,
 /*   140 */   510,  486,  507,  512,  524,  533,  521,
};
#define YY_REDUCE_COUNT (75)
#define YY_REDUCE_MIN   (-103)
#define YY_REDUCE_MAX   (637)
static const short yy_reduce_ofst[] = {
 /*     0 */   377,   -1,   43,   63,   83,  103,  162,  185,  220,  224,
 /*    10 */   307,  -29,  345,  358,  373,  388,  403,  -61,  421,  434,
 /*    20 */   448,  463,  478,  496,  509,  523,  538,  553,  571,  584,
 /*    30 */   598,  613,  608,  623,  637,  -74,  -63,  -31,   12,  -85,
 /*    40 */   -85,  -69,   35,   50,   55,  -75, -103,  -27,  -13,   24,
 /*    50 */    24,   24,   66,   48,   57,  120,  143,  187,  195,  165,
 /*    60 */   167,  211,  199,  210,  226,  228,  253,   24,  275,  274,
 /*    70 */   285,  302,  268,  278,  282,   66,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   381,  381,  381,  381,  381,  381,  381,  381,  381,  381,
 /*    10 */   381,  381,  381,  381,  381,  381,  381,  381,  381,  381,
 /*    20 */   381,  381,  381,  381,  381,  381,  381,  381,  381,  381,
 /*    30 */   381,  381,  381,  381,  381,  381,  381,  381,  381,  381,
 /*    40 */   381,  381,  452,  452,  452,  468,  516,  381,  476,  444,
 /*    50 */   458,  445,  381,  501,  461,  483,  481,  381,  381,  483,
 /*    60 */   481,  381,  495,  491,  474,  472,  381,  458,  381,  381,
 /*    70 */   381,  381,  519,  507,  503,  381,  381,  381,  381,  494,
 /*    80 */   493,  420,  419,  418,  414,  415,  381,  381,  409,  410,
 /*    90 */   408,  407,  381,  381,  512,  381,  381,  381,  504,  508,
 /*   100 */   381,  397,  465,  475,  381,  381,  381,  381,  381,  381,
 /*   110 */   381,  381,  381,  381,  381,  381,  381,  397,  381,  492,
 /*   120 */   381,  439,  381,  451,  447,  381,  381,  443,  396,  381,
 /*   130 */   381,  381,  502,  381,  381,  381,  381,  381,  381,  381,
 /*   140 */   381,  381,  381,  381,  381,  381,  381,
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
  /*   67 */ "ASC",
  /*   68 */ "DESC",
  /*   69 */ "NULLS",
  /*   70 */ "FIRST",
  /*   71 */ "LAST",
  /*   72 */ "cmd",
  /*   73 */ "query_expression",
  /*   74 */ "literal",
  /*   75 */ "duration_literal",
  /*   76 */ "literal_list",
  /*   77 */ "db_name",
  /*   78 */ "table_name",
  /*   79 */ "column_name",
  /*   80 */ "function_name",
  /*   81 */ "table_alias",
  /*   82 */ "column_alias",
  /*   83 */ "expression",
  /*   84 */ "column_reference",
  /*   85 */ "expression_list",
  /*   86 */ "subquery",
  /*   87 */ "predicate",
  /*   88 */ "compare_op",
  /*   89 */ "in_op",
  /*   90 */ "in_predicate_value",
  /*   91 */ "boolean_value_expression",
  /*   92 */ "boolean_primary",
  /*   93 */ "from_clause",
  /*   94 */ "table_reference_list",
  /*   95 */ "table_reference",
  /*   96 */ "table_primary",
  /*   97 */ "joined_table",
  /*   98 */ "alias_opt",
  /*   99 */ "parenthesized_joined_table",
  /*  100 */ "join_type",
  /*  101 */ "search_condition",
  /*  102 */ "query_specification",
  /*  103 */ "set_quantifier_opt",
  /*  104 */ "select_list",
  /*  105 */ "where_clause_opt",
  /*  106 */ "partition_by_clause_opt",
  /*  107 */ "twindow_clause_opt",
  /*  108 */ "group_by_clause_opt",
  /*  109 */ "having_clause_opt",
  /*  110 */ "select_sublist",
  /*  111 */ "select_item",
  /*  112 */ "sliding_opt",
  /*  113 */ "fill_opt",
  /*  114 */ "fill_mode",
  /*  115 */ "group_by_list",
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
 /*  20 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /*  21 */ "expression ::= subquery",
 /*  22 */ "expression ::= NK_LP expression NK_RP",
 /*  23 */ "expression ::= NK_PLUS expression",
 /*  24 */ "expression ::= NK_MINUS expression",
 /*  25 */ "expression ::= expression NK_PLUS expression",
 /*  26 */ "expression ::= expression NK_MINUS expression",
 /*  27 */ "expression ::= expression NK_STAR expression",
 /*  28 */ "expression ::= expression NK_SLASH expression",
 /*  29 */ "expression ::= expression NK_REM expression",
 /*  30 */ "expression_list ::= expression",
 /*  31 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  32 */ "column_reference ::= column_name",
 /*  33 */ "column_reference ::= table_name NK_DOT column_name",
 /*  34 */ "predicate ::= expression compare_op expression",
 /*  35 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  36 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  37 */ "predicate ::= expression IS NULL",
 /*  38 */ "predicate ::= expression IS NOT NULL",
 /*  39 */ "predicate ::= expression in_op in_predicate_value",
 /*  40 */ "compare_op ::= NK_LT",
 /*  41 */ "compare_op ::= NK_GT",
 /*  42 */ "compare_op ::= NK_LE",
 /*  43 */ "compare_op ::= NK_GE",
 /*  44 */ "compare_op ::= NK_NE",
 /*  45 */ "compare_op ::= NK_EQ",
 /*  46 */ "compare_op ::= LIKE",
 /*  47 */ "compare_op ::= NOT LIKE",
 /*  48 */ "compare_op ::= MATCH",
 /*  49 */ "compare_op ::= NMATCH",
 /*  50 */ "in_op ::= IN",
 /*  51 */ "in_op ::= NOT IN",
 /*  52 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /*  53 */ "boolean_value_expression ::= boolean_primary",
 /*  54 */ "boolean_value_expression ::= NOT boolean_primary",
 /*  55 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /*  56 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /*  57 */ "boolean_primary ::= predicate",
 /*  58 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /*  59 */ "from_clause ::= FROM table_reference_list",
 /*  60 */ "table_reference_list ::= table_reference",
 /*  61 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /*  62 */ "table_reference ::= table_primary",
 /*  63 */ "table_reference ::= joined_table",
 /*  64 */ "table_primary ::= table_name alias_opt",
 /*  65 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /*  66 */ "table_primary ::= subquery alias_opt",
 /*  67 */ "table_primary ::= parenthesized_joined_table",
 /*  68 */ "alias_opt ::=",
 /*  69 */ "alias_opt ::= table_alias",
 /*  70 */ "alias_opt ::= AS table_alias",
 /*  71 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /*  72 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /*  73 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /*  74 */ "join_type ::=",
 /*  75 */ "join_type ::= INNER",
 /*  76 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /*  77 */ "set_quantifier_opt ::=",
 /*  78 */ "set_quantifier_opt ::= DISTINCT",
 /*  79 */ "set_quantifier_opt ::= ALL",
 /*  80 */ "select_list ::= NK_STAR",
 /*  81 */ "select_list ::= select_sublist",
 /*  82 */ "select_sublist ::= select_item",
 /*  83 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /*  84 */ "select_item ::= expression",
 /*  85 */ "select_item ::= expression column_alias",
 /*  86 */ "select_item ::= expression AS column_alias",
 /*  87 */ "select_item ::= table_name NK_DOT NK_STAR",
 /*  88 */ "where_clause_opt ::=",
 /*  89 */ "where_clause_opt ::= WHERE search_condition",
 /*  90 */ "partition_by_clause_opt ::=",
 /*  91 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /*  92 */ "twindow_clause_opt ::=",
 /*  93 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /*  94 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /*  95 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /*  96 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /*  97 */ "sliding_opt ::=",
 /*  98 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /*  99 */ "fill_opt ::=",
 /* 100 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 101 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 102 */ "fill_mode ::= NONE",
 /* 103 */ "fill_mode ::= PREV",
 /* 104 */ "fill_mode ::= NULL",
 /* 105 */ "fill_mode ::= LINEAR",
 /* 106 */ "fill_mode ::= NEXT",
 /* 107 */ "group_by_clause_opt ::=",
 /* 108 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 109 */ "group_by_list ::= expression",
 /* 110 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 111 */ "having_clause_opt ::=",
 /* 112 */ "having_clause_opt ::= HAVING search_condition",
 /* 113 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 114 */ "query_expression_body ::= query_primary",
 /* 115 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 116 */ "query_primary ::= query_specification",
 /* 117 */ "order_by_clause_opt ::=",
 /* 118 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 119 */ "slimit_clause_opt ::=",
 /* 120 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 121 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 122 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 123 */ "limit_clause_opt ::=",
 /* 124 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 125 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 126 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 127 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 128 */ "search_condition ::= boolean_value_expression",
 /* 129 */ "sort_specification_list ::= sort_specification",
 /* 130 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 131 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 132 */ "ordering_specification_opt ::=",
 /* 133 */ "ordering_specification_opt ::= ASC",
 /* 134 */ "ordering_specification_opt ::= DESC",
 /* 135 */ "null_ordering_opt ::=",
 /* 136 */ "null_ordering_opt ::= NULLS FIRST",
 /* 137 */ "null_ordering_opt ::= NULLS LAST",
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
    case 72: /* cmd */
    case 73: /* query_expression */
    case 74: /* literal */
    case 75: /* duration_literal */
    case 83: /* expression */
    case 84: /* column_reference */
    case 86: /* subquery */
    case 87: /* predicate */
    case 90: /* in_predicate_value */
    case 91: /* boolean_value_expression */
    case 92: /* boolean_primary */
    case 93: /* from_clause */
    case 94: /* table_reference_list */
    case 95: /* table_reference */
    case 96: /* table_primary */
    case 97: /* joined_table */
    case 99: /* parenthesized_joined_table */
    case 101: /* search_condition */
    case 102: /* query_specification */
    case 105: /* where_clause_opt */
    case 107: /* twindow_clause_opt */
    case 109: /* having_clause_opt */
    case 111: /* select_item */
    case 112: /* sliding_opt */
    case 113: /* fill_opt */
    case 116: /* query_expression_body */
    case 118: /* slimit_clause_opt */
    case 119: /* limit_clause_opt */
    case 120: /* query_primary */
    case 122: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy212)); 
}
      break;
    case 76: /* literal_list */
    case 85: /* expression_list */
    case 104: /* select_list */
    case 106: /* partition_by_clause_opt */
    case 108: /* group_by_clause_opt */
    case 110: /* select_sublist */
    case 115: /* group_by_list */
    case 117: /* order_by_clause_opt */
    case 121: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy174)); 
}
      break;
    case 77: /* db_name */
    case 78: /* table_name */
    case 79: /* column_name */
    case 80: /* function_name */
    case 81: /* table_alias */
    case 82: /* column_alias */
    case 98: /* alias_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 88: /* compare_op */
    case 89: /* in_op */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 100: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 103: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 114: /* fill_mode */
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
  {   72,   -2 }, /* (0) cmd ::= SHOW DATABASES */
  {   72,   -1 }, /* (1) cmd ::= query_expression */
  {   74,   -1 }, /* (2) literal ::= NK_INTEGER */
  {   74,   -1 }, /* (3) literal ::= NK_FLOAT */
  {   74,   -1 }, /* (4) literal ::= NK_STRING */
  {   74,   -1 }, /* (5) literal ::= NK_BOOL */
  {   74,   -2 }, /* (6) literal ::= TIMESTAMP NK_STRING */
  {   74,   -1 }, /* (7) literal ::= duration_literal */
  {   75,   -1 }, /* (8) duration_literal ::= NK_VARIABLE */
  {   76,   -1 }, /* (9) literal_list ::= literal */
  {   76,   -3 }, /* (10) literal_list ::= literal_list NK_COMMA literal */
  {   77,   -1 }, /* (11) db_name ::= NK_ID */
  {   78,   -1 }, /* (12) table_name ::= NK_ID */
  {   79,   -1 }, /* (13) column_name ::= NK_ID */
  {   80,   -1 }, /* (14) function_name ::= NK_ID */
  {   81,   -1 }, /* (15) table_alias ::= NK_ID */
  {   82,   -1 }, /* (16) column_alias ::= NK_ID */
  {   83,   -1 }, /* (17) expression ::= literal */
  {   83,   -1 }, /* (18) expression ::= column_reference */
  {   83,   -4 }, /* (19) expression ::= function_name NK_LP expression_list NK_RP */
  {   83,   -4 }, /* (20) expression ::= function_name NK_LP NK_STAR NK_RP */
  {   83,   -1 }, /* (21) expression ::= subquery */
  {   83,   -3 }, /* (22) expression ::= NK_LP expression NK_RP */
  {   83,   -2 }, /* (23) expression ::= NK_PLUS expression */
  {   83,   -2 }, /* (24) expression ::= NK_MINUS expression */
  {   83,   -3 }, /* (25) expression ::= expression NK_PLUS expression */
  {   83,   -3 }, /* (26) expression ::= expression NK_MINUS expression */
  {   83,   -3 }, /* (27) expression ::= expression NK_STAR expression */
  {   83,   -3 }, /* (28) expression ::= expression NK_SLASH expression */
  {   83,   -3 }, /* (29) expression ::= expression NK_REM expression */
  {   85,   -1 }, /* (30) expression_list ::= expression */
  {   85,   -3 }, /* (31) expression_list ::= expression_list NK_COMMA expression */
  {   84,   -1 }, /* (32) column_reference ::= column_name */
  {   84,   -3 }, /* (33) column_reference ::= table_name NK_DOT column_name */
  {   87,   -3 }, /* (34) predicate ::= expression compare_op expression */
  {   87,   -5 }, /* (35) predicate ::= expression BETWEEN expression AND expression */
  {   87,   -6 }, /* (36) predicate ::= expression NOT BETWEEN expression AND expression */
  {   87,   -3 }, /* (37) predicate ::= expression IS NULL */
  {   87,   -4 }, /* (38) predicate ::= expression IS NOT NULL */
  {   87,   -3 }, /* (39) predicate ::= expression in_op in_predicate_value */
  {   88,   -1 }, /* (40) compare_op ::= NK_LT */
  {   88,   -1 }, /* (41) compare_op ::= NK_GT */
  {   88,   -1 }, /* (42) compare_op ::= NK_LE */
  {   88,   -1 }, /* (43) compare_op ::= NK_GE */
  {   88,   -1 }, /* (44) compare_op ::= NK_NE */
  {   88,   -1 }, /* (45) compare_op ::= NK_EQ */
  {   88,   -1 }, /* (46) compare_op ::= LIKE */
  {   88,   -2 }, /* (47) compare_op ::= NOT LIKE */
  {   88,   -1 }, /* (48) compare_op ::= MATCH */
  {   88,   -1 }, /* (49) compare_op ::= NMATCH */
  {   89,   -1 }, /* (50) in_op ::= IN */
  {   89,   -2 }, /* (51) in_op ::= NOT IN */
  {   90,   -3 }, /* (52) in_predicate_value ::= NK_LP expression_list NK_RP */
  {   91,   -1 }, /* (53) boolean_value_expression ::= boolean_primary */
  {   91,   -2 }, /* (54) boolean_value_expression ::= NOT boolean_primary */
  {   91,   -3 }, /* (55) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {   91,   -3 }, /* (56) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {   92,   -1 }, /* (57) boolean_primary ::= predicate */
  {   92,   -3 }, /* (58) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {   93,   -2 }, /* (59) from_clause ::= FROM table_reference_list */
  {   94,   -1 }, /* (60) table_reference_list ::= table_reference */
  {   94,   -3 }, /* (61) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {   95,   -1 }, /* (62) table_reference ::= table_primary */
  {   95,   -1 }, /* (63) table_reference ::= joined_table */
  {   96,   -2 }, /* (64) table_primary ::= table_name alias_opt */
  {   96,   -4 }, /* (65) table_primary ::= db_name NK_DOT table_name alias_opt */
  {   96,   -2 }, /* (66) table_primary ::= subquery alias_opt */
  {   96,   -1 }, /* (67) table_primary ::= parenthesized_joined_table */
  {   98,    0 }, /* (68) alias_opt ::= */
  {   98,   -1 }, /* (69) alias_opt ::= table_alias */
  {   98,   -2 }, /* (70) alias_opt ::= AS table_alias */
  {   99,   -3 }, /* (71) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {   99,   -3 }, /* (72) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {   97,   -6 }, /* (73) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  100,    0 }, /* (74) join_type ::= */
  {  100,   -1 }, /* (75) join_type ::= INNER */
  {  102,   -9 }, /* (76) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  103,    0 }, /* (77) set_quantifier_opt ::= */
  {  103,   -1 }, /* (78) set_quantifier_opt ::= DISTINCT */
  {  103,   -1 }, /* (79) set_quantifier_opt ::= ALL */
  {  104,   -1 }, /* (80) select_list ::= NK_STAR */
  {  104,   -1 }, /* (81) select_list ::= select_sublist */
  {  110,   -1 }, /* (82) select_sublist ::= select_item */
  {  110,   -3 }, /* (83) select_sublist ::= select_sublist NK_COMMA select_item */
  {  111,   -1 }, /* (84) select_item ::= expression */
  {  111,   -2 }, /* (85) select_item ::= expression column_alias */
  {  111,   -3 }, /* (86) select_item ::= expression AS column_alias */
  {  111,   -3 }, /* (87) select_item ::= table_name NK_DOT NK_STAR */
  {  105,    0 }, /* (88) where_clause_opt ::= */
  {  105,   -2 }, /* (89) where_clause_opt ::= WHERE search_condition */
  {  106,    0 }, /* (90) partition_by_clause_opt ::= */
  {  106,   -3 }, /* (91) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  107,    0 }, /* (92) twindow_clause_opt ::= */
  {  107,   -6 }, /* (93) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  107,   -4 }, /* (94) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  107,   -6 }, /* (95) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  107,   -8 }, /* (96) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  112,    0 }, /* (97) sliding_opt ::= */
  {  112,   -4 }, /* (98) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  113,    0 }, /* (99) fill_opt ::= */
  {  113,   -4 }, /* (100) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  113,   -6 }, /* (101) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  114,   -1 }, /* (102) fill_mode ::= NONE */
  {  114,   -1 }, /* (103) fill_mode ::= PREV */
  {  114,   -1 }, /* (104) fill_mode ::= NULL */
  {  114,   -1 }, /* (105) fill_mode ::= LINEAR */
  {  114,   -1 }, /* (106) fill_mode ::= NEXT */
  {  108,    0 }, /* (107) group_by_clause_opt ::= */
  {  108,   -3 }, /* (108) group_by_clause_opt ::= GROUP BY group_by_list */
  {  115,   -1 }, /* (109) group_by_list ::= expression */
  {  115,   -3 }, /* (110) group_by_list ::= group_by_list NK_COMMA expression */
  {  109,    0 }, /* (111) having_clause_opt ::= */
  {  109,   -2 }, /* (112) having_clause_opt ::= HAVING search_condition */
  {   73,   -4 }, /* (113) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  116,   -1 }, /* (114) query_expression_body ::= query_primary */
  {  116,   -4 }, /* (115) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  120,   -1 }, /* (116) query_primary ::= query_specification */
  {  117,    0 }, /* (117) order_by_clause_opt ::= */
  {  117,   -3 }, /* (118) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  118,    0 }, /* (119) slimit_clause_opt ::= */
  {  118,   -2 }, /* (120) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  118,   -4 }, /* (121) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  118,   -4 }, /* (122) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  119,    0 }, /* (123) limit_clause_opt ::= */
  {  119,   -2 }, /* (124) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  119,   -4 }, /* (125) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  119,   -4 }, /* (126) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {   86,   -3 }, /* (127) subquery ::= NK_LP query_expression NK_RP */
  {  101,   -1 }, /* (128) search_condition ::= boolean_value_expression */
  {  121,   -1 }, /* (129) sort_specification_list ::= sort_specification */
  {  121,   -3 }, /* (130) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  122,   -3 }, /* (131) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  123,    0 }, /* (132) ordering_specification_opt ::= */
  {  123,   -1 }, /* (133) ordering_specification_opt ::= ASC */
  {  123,   -1 }, /* (134) ordering_specification_opt ::= DESC */
  {  124,    0 }, /* (135) null_ordering_opt ::= */
  {  124,   -2 }, /* (136) null_ordering_opt ::= NULLS FIRST */
  {  124,   -2 }, /* (137) null_ordering_opt ::= NULLS LAST */
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
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 3: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 4: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 5: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 6: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 7: /* literal ::= duration_literal */
      case 17: /* expression ::= literal */ yytestcase(yyruleno==17);
      case 18: /* expression ::= column_reference */ yytestcase(yyruleno==18);
      case 21: /* expression ::= subquery */ yytestcase(yyruleno==21);
      case 53: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==53);
      case 57: /* boolean_primary ::= predicate */ yytestcase(yyruleno==57);
      case 60: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==60);
      case 62: /* table_reference ::= table_primary */ yytestcase(yyruleno==62);
      case 63: /* table_reference ::= joined_table */ yytestcase(yyruleno==63);
      case 67: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==67);
      case 114: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==114);
      case 116: /* query_primary ::= query_specification */ yytestcase(yyruleno==116);
      case 128: /* search_condition ::= boolean_value_expression */ yytestcase(yyruleno==128);
{ PARSER_TRACE; yylhsminor.yy212 = yymsp[0].minor.yy212; }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 8: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 9: /* literal_list ::= literal */
      case 30: /* expression_list ::= expression */ yytestcase(yyruleno==30);
{ PARSER_TRACE; yylhsminor.yy174 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy212)); }
  yymsp[0].minor.yy174 = yylhsminor.yy174;
        break;
      case 10: /* literal_list ::= literal_list NK_COMMA literal */
      case 31: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==31);
{ PARSER_TRACE; yylhsminor.yy174 = addNodeToList(pCxt, yymsp[-2].minor.yy174, releaseRawExprNode(pCxt, yymsp[0].minor.yy212)); }
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
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy79, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy79, yymsp[-1].minor.yy174)); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 20: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy79, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy79, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 22: /* expression ::= NK_LP expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy212)); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 23: /* expression ::= NK_PLUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy212));
                                                                                  }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 24: /* expression ::= NK_MINUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy212), NULL));
                                                                                  }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 25: /* expression ::= expression NK_PLUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy212);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); 
                                                                                  }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 26: /* expression ::= expression NK_MINUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy212);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); 
                                                                                  }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 27: /* expression ::= expression NK_STAR expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy212);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); 
                                                                                  }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 28: /* expression ::= expression NK_SLASH expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy212);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); 
                                                                                  }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 29: /* expression ::= expression NK_REM expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy212);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); 
                                                                                  }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 32: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNode(pCxt, &yymsp[0].minor.yy79, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy79)); }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 33: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy79, &yymsp[0].minor.yy79, createColumnNode(pCxt, &yymsp[-2].minor.yy79, &yymsp[0].minor.yy79)); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 34: /* predicate ::= expression compare_op expression */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, yymsp[-1].minor.yy40, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212)); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 35: /* predicate ::= expression BETWEEN expression AND expression */
{ PARSER_TRACE; yylhsminor.yy212 = createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy212), releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212)); }
  yymsp[-4].minor.yy212 = yylhsminor.yy212;
        break;
      case 36: /* predicate ::= expression NOT BETWEEN expression AND expression */
{ PARSER_TRACE; yylhsminor.yy212 = createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), releaseRawExprNode(pCxt, yymsp[-5].minor.yy212), releaseRawExprNode(pCxt, yymsp[0].minor.yy212)); }
  yymsp[-5].minor.yy212 = yylhsminor.yy212;
        break;
      case 37: /* predicate ::= expression IS NULL */
{ PARSER_TRACE; yylhsminor.yy212 = createIsNullCondNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), true); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 38: /* predicate ::= expression IS NOT NULL */
{ PARSER_TRACE; yylhsminor.yy212 = createIsNullCondNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy212), false); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 39: /* predicate ::= expression in_op in_predicate_value */
{ PARSER_TRACE; yylhsminor.yy212 = createOperatorNode(pCxt, yymsp[-1].minor.yy40, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 40: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_LOWER_THAN; }
        break;
      case 41: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_GREATER_THAN; }
        break;
      case 42: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 43: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 44: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_NOT_EQUAL; }
        break;
      case 45: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_EQUAL; }
        break;
      case 46: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_LIKE; }
        break;
      case 47: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy40 = OP_TYPE_NOT_LIKE; }
        break;
      case 48: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_MATCH; }
        break;
      case 49: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_NMATCH; }
        break;
      case 50: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy40 = OP_TYPE_IN; }
        break;
      case 51: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy40 = OP_TYPE_NOT_IN; }
        break;
      case 52: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yymsp[-2].minor.yy212 = createNodeListNode(pCxt, yymsp[-1].minor.yy174); }
        break;
      case 54: /* boolean_value_expression ::= NOT boolean_primary */
{ PARSER_TRACE; yymsp[-1].minor.yy212 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, yymsp[0].minor.yy212, NULL); }
        break;
      case 55: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy212 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 56: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{ PARSER_TRACE; yylhsminor.yy212 = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 58: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */
      case 71: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */ yytestcase(yyruleno==71);
      case 72: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==72);
{ PARSER_TRACE; yymsp[-2].minor.yy212 = yymsp[-1].minor.yy212; }
        break;
      case 59: /* from_clause ::= FROM table_reference_list */
      case 89: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==89);
      case 112: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==112);
{ PARSER_TRACE; yymsp[-1].minor.yy212 = yymsp[0].minor.yy212; }
        break;
      case 61: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy212 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy212, yymsp[0].minor.yy212, NULL); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 64: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy79, &yymsp[0].minor.yy79); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 65: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createRealTableNode(pCxt, &yymsp[-3].minor.yy79, &yymsp[-1].minor.yy79, &yymsp[0].minor.yy79); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 66: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy212), &yymsp[0].minor.yy79); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 68: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy79 = nil_token;  }
        break;
      case 69: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy79 = yymsp[0].minor.yy79; }
  yymsp[0].minor.yy79 = yylhsminor.yy79;
        break;
      case 70: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy79 = yymsp[0].minor.yy79; }
        break;
      case 73: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy212 = createJoinTableNode(pCxt, yymsp[-4].minor.yy162, yymsp[-5].minor.yy212, yymsp[-2].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-5].minor.yy212 = yylhsminor.yy212;
        break;
      case 74: /* join_type ::= */
{ PARSER_TRACE; yymsp[1].minor.yy162 = JOIN_TYPE_INNER; }
        break;
      case 75: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy162 = JOIN_TYPE_INNER; }
        break;
      case 76: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
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
      case 77: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy237 = false; }
        break;
      case 78: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy237 = true; }
        break;
      case 79: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy237 = false; }
        break;
      case 80: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy174 = NULL; }
        break;
      case 81: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy174 = yymsp[0].minor.yy174; }
  yymsp[0].minor.yy174 = yylhsminor.yy174;
        break;
      case 82: /* select_sublist ::= select_item */
      case 129: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==129);
{ PARSER_TRACE; yylhsminor.yy174 = createNodeList(pCxt, yymsp[0].minor.yy212); }
  yymsp[0].minor.yy174 = yylhsminor.yy174;
        break;
      case 83: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 130: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==130);
{ PARSER_TRACE; yylhsminor.yy174 = addNodeToList(pCxt, yymsp[-2].minor.yy174, yymsp[0].minor.yy212); }
  yymsp[-2].minor.yy174 = yylhsminor.yy174;
        break;
      case 84: /* select_item ::= expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy212);
                                                                                    yylhsminor.yy212 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy212), &t);
                                                                                  }
  yymsp[0].minor.yy212 = yylhsminor.yy212;
        break;
      case 85: /* select_item ::= expression column_alias */
{ PARSER_TRACE; yylhsminor.yy212 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy212), &yymsp[0].minor.yy79); }
  yymsp[-1].minor.yy212 = yylhsminor.yy212;
        break;
      case 86: /* select_item ::= expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy212 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), &yymsp[0].minor.yy79); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 87: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy212 = createColumnNode(pCxt, &yymsp[-2].minor.yy79, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 88: /* where_clause_opt ::= */
      case 92: /* twindow_clause_opt ::= */ yytestcase(yyruleno==92);
      case 97: /* sliding_opt ::= */ yytestcase(yyruleno==97);
      case 99: /* fill_opt ::= */ yytestcase(yyruleno==99);
      case 111: /* having_clause_opt ::= */ yytestcase(yyruleno==111);
      case 119: /* slimit_clause_opt ::= */ yytestcase(yyruleno==119);
      case 123: /* limit_clause_opt ::= */ yytestcase(yyruleno==123);
{ PARSER_TRACE; yymsp[1].minor.yy212 = NULL; }
        break;
      case 90: /* partition_by_clause_opt ::= */
      case 107: /* group_by_clause_opt ::= */ yytestcase(yyruleno==107);
      case 117: /* order_by_clause_opt ::= */ yytestcase(yyruleno==117);
{ PARSER_TRACE; yymsp[1].minor.yy174 = NULL; }
        break;
      case 91: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 108: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==108);
      case 118: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==118);
{ PARSER_TRACE; yymsp[-2].minor.yy174 = yymsp[0].minor.yy174; }
        break;
      case 93: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy212), &yymsp[-1].minor.yy0); }
        break;
      case 94: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy212)); }
        break;
      case 95: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy212, NULL, yymsp[-1].minor.yy212, yymsp[0].minor.yy212); }
        break;
      case 96: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy212 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy212, yymsp[-3].minor.yy212, yymsp[-1].minor.yy212, yymsp[0].minor.yy212); }
        break;
      case 98: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy212 = yymsp[-1].minor.yy212; }
        break;
      case 100: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createFillNode(pCxt, yymsp[-1].minor.yy44, NULL); }
        break;
      case 101: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy212 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy174)); }
        break;
      case 102: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_NONE; }
        break;
      case 103: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_PREV; }
        break;
      case 104: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_NULL; }
        break;
      case 105: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_LINEAR; }
        break;
      case 106: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy44 = FILL_MODE_NEXT; }
        break;
      case 109: /* group_by_list ::= expression */
{ PARSER_TRACE; yylhsminor.yy174 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); }
  yymsp[0].minor.yy174 = yylhsminor.yy174;
        break;
      case 110: /* group_by_list ::= group_by_list NK_COMMA expression */
{ PARSER_TRACE; yylhsminor.yy174 = addNodeToList(pCxt, yymsp[-2].minor.yy174, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy212))); }
  yymsp[-2].minor.yy174 = yylhsminor.yy174;
        break;
      case 113: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy212 = addOrderByClause(pCxt, yymsp[-3].minor.yy212, yymsp[-2].minor.yy174);
                                                                                    yylhsminor.yy212 = addSlimitClause(pCxt, yylhsminor.yy212, yymsp[-1].minor.yy212);
                                                                                    yylhsminor.yy212 = addLimitClause(pCxt, yylhsminor.yy212, yymsp[0].minor.yy212);
                                                                                  }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 115: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy212 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy212, yymsp[0].minor.yy212); }
  yymsp[-3].minor.yy212 = yylhsminor.yy212;
        break;
      case 120: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 124: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==124);
{ PARSER_TRACE; yymsp[-1].minor.yy212 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 121: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 125: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==125);
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 122: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 126: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==126);
{ PARSER_TRACE; yymsp[-3].minor.yy212 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 127: /* subquery ::= NK_LP query_expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy212 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy212); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 131: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy212 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy212), yymsp[-1].minor.yy188, yymsp[0].minor.yy107); }
  yymsp[-2].minor.yy212 = yylhsminor.yy212;
        break;
      case 132: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy188 = ORDER_ASC; }
        break;
      case 133: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy188 = ORDER_ASC; }
        break;
      case 134: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy188 = ORDER_DESC; }
        break;
      case 135: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy107 = NULL_ORDER_DEFAULT; }
        break;
      case 136: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy107 = NULL_ORDER_FIRST; }
        break;
      case 137: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy107 = NULL_ORDER_LAST; }
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
