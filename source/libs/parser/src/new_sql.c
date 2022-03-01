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
#define YYNOCODE 152
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  SNodeList* yy8;
  EOrder yy50;
  EOperatorType yy60;
  SDatabaseOptions* yy87;
  SNode* yy104;
  SToken yy129;
  bool yy185;
  ENullOrder yy186;
  EJoinType yy228;
  EFillMode yy246;
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
#define YYNSTATE             170
#define YYNRULE              160
#define YYNTOKEN             96
#define YY_MAX_SHIFT         169
#define YY_MIN_SHIFTREDUCE   286
#define YY_MAX_SHIFTREDUCE   445
#define YY_ERROR_ACTION      446
#define YY_ACCEPT_ACTION     447
#define YY_NO_ACTION         448
#define YY_MIN_REDUCE        449
#define YY_MAX_REDUCE        608
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
#define YY_ACTTAB_COUNT (757)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   486,  476,   73,  146,  501,  147,  587,  114,   89,  487,
 /*    10 */   105,  490,   30,   28,   26,   25,   24,  478,  476,   97,
 /*    20 */   586,   92,   23,   72,  585,   30,   28,   26,   25,   24,
 /*    30 */   479,  476,  146,  501,   92,   65,  486,  476,  117,  146,
 /*    40 */   501,  147,    9,    8,   41,  487,  556,  490,  526,  328,
 /*    50 */   129,   22,  101,  523,  346,  347,  348,  349,  350,  351,
 /*    60 */   352,  354,  355,  356,   22,  101,  449,  346,  347,  348,
 /*    70 */   349,  350,  351,  352,  354,  355,  356,  486,  476,  169,
 /*    80 */   146,  501,  147,  412,  395,   89,  487,  108,  490,  168,
 /*    90 */   364,  167,  166,  165,  164,  163,  162,  161,  160,  159,
 /*   100 */   140,  158,  157,  156,  155,  154,  153,  152,  387,  113,
 /*   110 */   410,  411,  413,  414,  136,  486,  476,  319,  146,  501,
 /*   120 */   147,  131,   10,   39,  487,  122,  490,  526,  131,   10,
 /*   130 */    55,   91,  522,  123,  118,  116,  486,  476,  321,  132,
 /*   140 */   501,  147,  141,  587,   40,  487,   20,  490,  526,   55,
 /*   150 */    55,  540,   99,  522,   49,  353,   70,   54,  357,  486,
 /*   160 */   476,  585,  132,  501,  147,  139,   71,   40,  487,  537,
 /*   170 */   490,  526,  107,  553,  386,   99,  522,   49,  486,  476,
 /*   180 */   143,  146,  501,  147,    6,  383,   85,  487,  134,  490,
 /*   190 */    26,   25,   24,  502,  486,  476,  554,  146,  501,  147,
 /*   200 */    75,   42,   40,  487,  409,  490,  526,  146,  501,    2,
 /*   210 */    99,  522,  599,  121,  545,  322,  383,  540,  540,  124,
 /*   220 */   343,  560,  486,  476,  144,  146,  501,  147,  447,  115,
 /*   230 */    40,  487,  149,  490,  526,  536,  535,  557,   99,  522,
 /*   240 */   599,  567,  486,  476,  322,  146,  501,  147,  138,  583,
 /*   250 */    40,  487,  112,  490,  526,   29,   27,   57,   99,  522,
 /*   260 */   599,  587,   56,   59,   11,   31,   62,  308,  358,  544,
 /*   270 */   128,   29,   27,  388,  314,   54,   45,  310,  566,  585,
 /*   280 */    11,    9,    8,  308,   43,  309,  311,  148,  314,  110,
 /*   290 */   106,    1,  111,  310,   51,  533,  534,   61,  538,   29,
 /*   300 */    27,  309,  311,  148,  314,   98,  106,    1,   11,  444,
 /*   310 */   445,  308,   55,  136,  486,  476,    5,  146,  501,  147,
 /*   320 */   547,  310,   83,  487,   31,  490,  125,  325,   64,  309,
 /*   330 */   311,  148,  314,  109,  106,    1,   29,   27,   48,  483,
 /*   340 */     4,  481,  587,   66,  308,  383,  318,  321,  308,   30,
 /*   350 */    28,   26,   25,   24,  310,   44,   54,  541,  310,   32,
 /*   360 */   585,   67,  309,  311,  148,  314,  309,  311,  148,  314,
 /*   370 */    16,  106,    7,  486,  476,  102,  146,  501,  147,  508,
 /*   380 */   584,   41,  487,  602,  490,  526,  145,  319,  142,  525,
 /*   390 */   522,   74,  317,   55,   79,  486,  476,  151,  146,  501,
 /*   400 */   147,   77,   80,   41,  487,    3,  490,  526,   31,   14,
 /*   410 */    58,  133,  522,  128,  406,  136,   60,   35,  408,   45,
 /*   420 */    29,   27,  135,   47,   63,  119,   36,   43,  441,  442,
 /*   430 */   402,  401,  308,  120,  481,   29,   27,   68,  533,  127,
 /*   440 */    37,  126,  310,   18,  587,   15,   33,  308,  380,  379,
 /*   450 */   309,  311,  148,  314,   69,  106,    7,  310,   54,   34,
 /*   460 */    29,   27,  585,    8,  480,  309,  311,  148,  314,   53,
 /*   470 */   106,    1,  308,  344,  326,  486,  476,   17,  146,  501,
 /*   480 */   147,  435,  310,   46,  487,   12,  490,   38,  430,  429,
 /*   490 */   309,  311,  148,  314,  103,  106,    7,  486,  476,   19,
 /*   500 */   146,  501,  147,   76,   21,   89,  487,  100,  490,   30,
 /*   510 */    28,   26,   25,   24,   30,   28,   26,   25,   24,  434,
 /*   520 */   433,   13,  137,  600,   30,   28,   26,   25,   24,  486,
 /*   530 */   476,  104,  146,  501,  147,  312,  470,   46,  487,  287,
 /*   540 */   490,  486,  476,  150,  146,  501,  147,  299,  306,   84,
 /*   550 */   487,  305,  490,  486,  476,  304,  146,  501,  147,   78,
 /*   560 */   448,   86,  487,  303,  490,  302,  301,  486,  476,  300,
 /*   570 */   146,  501,  147,  298,  297,   81,  487,  601,  490,  296,
 /*   580 */   486,  476,  295,  146,  501,  147,  294,  293,   87,  487,
 /*   590 */   292,  490,  486,  476,  291,  146,  501,  147,  290,  448,
 /*   600 */    82,  487,  448,  490,  486,  476,  448,  146,  501,  147,
 /*   610 */   448,  448,   88,  487,  448,  490,  448,   30,   28,   26,
 /*   620 */    25,   24,  448,  448,  486,  476,  448,  146,  501,  147,
 /*   630 */   448,  448,  498,  487,  448,  490,  486,  476,  448,  146,
 /*   640 */   501,  147,  448,  448,  497,  487,  448,  490,  486,  476,
 /*   650 */   448,  146,  501,  147,  328,  448,  496,  487,  448,  490,
 /*   660 */   486,  476,  448,  146,  501,  147,  448,  448,   95,  487,
 /*   670 */   448,  490,  486,  476,  448,  146,  501,  147,  448,  448,
 /*   680 */    94,  487,  448,  490,  448,  486,  476,  448,  146,  501,
 /*   690 */   147,  448,  448,   96,  487,  448,  490,  486,  476,  448,
 /*   700 */   146,  501,  147,  448,  448,   93,  487,  448,  490,  486,
 /*   710 */   476,  448,  146,  501,  147,  448,  448,   90,  487,  448,
 /*   720 */   490,  448,  448,  128,  448,  448,  448,  448,  128,   45,
 /*   730 */   448,  448,  448,  448,   45,  448,  448,   43,  448,  448,
 /*   740 */   448,  448,   43,  448,  448,  448,  130,   50,  533,  534,
 /*   750 */   448,  538,   52,  533,  534,  448,  538,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   101,  102,  150,  104,  105,  106,  129,  141,  109,  110,
 /*    10 */   111,  112,   12,   13,   14,   15,   16,  101,  102,  103,
 /*    20 */   143,   21,  114,  115,  147,   12,   13,   14,   15,   16,
 /*    30 */   101,  102,  104,  105,   21,  134,  101,  102,  110,  104,
 /*    40 */   105,  106,    1,    2,  109,  110,  108,  112,  113,   49,
 /*    50 */   127,   51,   52,  118,   54,   55,   56,   57,   58,   59,
 /*    60 */    60,   61,   62,   63,   51,   52,    0,   54,   55,   56,
 /*    70 */    57,   58,   59,   60,   61,   62,   63,  101,  102,   18,
 /*    80 */   104,  105,  106,   53,   14,  109,  110,  111,  112,   23,
 /*    90 */    49,   25,   26,   27,   28,   29,   30,   31,   32,   33,
 /*   100 */    46,   35,   36,   37,   38,   39,   40,   41,    4,   79,
 /*   110 */    80,   81,   82,   83,  100,  101,  102,   47,  104,  105,
 /*   120 */   106,   47,   48,  109,  110,   47,  112,  113,   47,   48,
 /*   130 */    69,  117,  118,   74,   75,   76,  101,  102,   47,  104,
 /*   140 */   105,  106,   88,  129,  109,  110,   51,  112,  113,   69,
 /*   150 */    69,  107,  117,  118,  119,   60,   65,  143,   63,  101,
 /*   160 */   102,  147,  104,  105,  106,    3,  131,  109,  110,  125,
 /*   170 */   112,  113,  137,  138,   70,  117,  118,  119,  101,  102,
 /*   180 */    46,  104,  105,  106,   67,   68,  109,  110,   21,  112,
 /*   190 */    14,   15,   16,  105,  101,  102,  138,  104,  105,  106,
 /*   200 */   144,   46,  109,  110,   49,  112,  113,  104,  105,  130,
 /*   210 */   117,  118,  119,  110,   66,   47,   68,  107,  107,  142,
 /*   220 */    53,  128,  101,  102,   90,  104,  105,  106,   96,   78,
 /*   230 */   109,  110,  100,  112,  113,  125,  125,  108,  117,  118,
 /*   240 */   119,  140,  101,  102,   47,  104,  105,  106,   86,  128,
 /*   250 */   109,  110,   77,  112,  113,   12,   13,  139,  117,  118,
 /*   260 */   119,  129,   65,   46,   21,   46,   49,   24,   49,  128,
 /*   270 */    98,   12,   13,   14,   45,  143,  104,   34,  140,  147,
 /*   280 */    21,    1,    2,   24,  112,   42,   43,   44,   45,  102,
 /*   290 */    47,   48,  102,   34,  122,  123,  124,  139,  126,   12,
 /*   300 */    13,   42,   43,   44,   45,  102,   47,   48,   21,   94,
 /*   310 */    95,   24,   69,  100,  101,  102,   85,  104,  105,  106,
 /*   320 */   136,   34,  109,  110,   46,  112,   84,   49,  135,   42,
 /*   330 */    43,   44,   45,   72,   47,   48,   12,   13,  133,   48,
 /*   340 */    71,   50,  129,  132,   24,   68,   47,   47,   24,   12,
 /*   350 */    13,   14,   15,   16,   34,  104,  143,  107,   34,   64,
 /*   360 */   147,  120,   42,   43,   44,   45,   42,   43,   44,   45,
 /*   370 */    48,   47,   48,  101,  102,   93,  104,  105,  106,  116,
 /*   380 */   146,  109,  110,  151,  112,  113,   89,   47,   87,  117,
 /*   390 */   118,  145,   47,   69,   98,  101,  102,   20,  104,  105,
 /*   400 */   106,   97,   99,  109,  110,   46,  112,  113,   46,   73,
 /*   410 */    49,  117,  118,   98,   49,  100,   48,   46,   49,  104,
 /*   420 */    12,   13,   14,   48,   48,   24,   48,  112,   91,   92,
 /*   430 */    49,   49,   24,   46,   50,   12,   13,  122,  123,  124,
 /*   440 */    48,  126,   34,   46,  129,   73,   66,   24,   49,   49,
 /*   450 */    42,   43,   44,   45,   50,   47,   48,   34,  143,   46,
 /*   460 */    12,   13,  147,    2,   50,   42,   43,   44,   45,   50,
 /*   470 */    47,   48,   24,   53,   49,  101,  102,   46,  104,  105,
 /*   480 */   106,   49,   34,  109,  110,   73,  112,    4,   24,   24,
 /*   490 */    42,   43,   44,   45,   24,   47,   48,  101,  102,    2,
 /*   500 */   104,  105,  106,   50,    2,  109,  110,  111,  112,   12,
 /*   510 */    13,   14,   15,   16,   12,   13,   14,   15,   16,   24,
 /*   520 */    24,   48,  148,  149,   12,   13,   14,   15,   16,  101,
 /*   530 */   102,   24,  104,  105,  106,   34,    0,  109,  110,   22,
 /*   540 */   112,  101,  102,   21,  104,  105,  106,   34,   24,  109,
 /*   550 */   110,   24,  112,  101,  102,   24,  104,  105,  106,   19,
 /*   560 */   152,  109,  110,   24,  112,   24,   24,  101,  102,   24,
 /*   570 */   104,  105,  106,   24,   24,  109,  110,  149,  112,   24,
 /*   580 */   101,  102,   24,  104,  105,  106,   24,   24,  109,  110,
 /*   590 */    24,  112,  101,  102,   24,  104,  105,  106,   24,  152,
 /*   600 */   109,  110,  152,  112,  101,  102,  152,  104,  105,  106,
 /*   610 */   152,  152,  109,  110,  152,  112,  152,   12,   13,   14,
 /*   620 */    15,   16,  152,  152,  101,  102,  152,  104,  105,  106,
 /*   630 */   152,  152,  109,  110,  152,  112,  101,  102,  152,  104,
 /*   640 */   105,  106,  152,  152,  109,  110,  152,  112,  101,  102,
 /*   650 */   152,  104,  105,  106,   49,  152,  109,  110,  152,  112,
 /*   660 */   101,  102,  152,  104,  105,  106,  152,  152,  109,  110,
 /*   670 */   152,  112,  101,  102,  152,  104,  105,  106,  152,  152,
 /*   680 */   109,  110,  152,  112,  152,  101,  102,  152,  104,  105,
 /*   690 */   106,  152,  152,  109,  110,  152,  112,  101,  102,  152,
 /*   700 */   104,  105,  106,  152,  152,  109,  110,  152,  112,  101,
 /*   710 */   102,  152,  104,  105,  106,  152,  152,  109,  110,  152,
 /*   720 */   112,  152,  152,   98,  152,  152,  152,  152,   98,  104,
 /*   730 */   152,  152,  152,  152,  104,  152,  152,  112,  152,  152,
 /*   740 */   152,  152,  112,  152,  152,  152,  121,  122,  123,  124,
 /*   750 */   152,  126,  122,  123,  124,  152,  126,
};
#define YY_SHIFT_COUNT    (169)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (605)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    61,  243,  259,  287,  287,  287,  287,  324,  287,  287,
 /*    10 */    81,  423,  448,  408,  448,  448,  448,  448,  448,  448,
 /*    20 */   448,  448,  448,  448,  448,  448,  448,  448,  448,  448,
 /*    30 */   448,  448,   74,   74,   74,  320,   78,   78,   80,    0,
 /*    40 */    13,   13,  320,   91,   91,   91,  337,   30,   59,  197,
 /*    50 */   148,  117,  148,   70,  162,  104,  168,  151,  175,  229,
 /*    60 */   229,  151,  175,  229,  231,  242,  261,  269,  277,  299,
 /*    70 */   300,  295,  322,  282,  297,  301,  340,  345,  377,  757,
 /*    80 */    66,  497,  502,  605,  512,  512,  512,  512,  512,  512,
 /*    90 */   512,   41,   95,  176,  176,  176,  176,  155,  217,  280,
 /*   100 */   219,  167,  215,   54,  134,  278,  291,  359,  362,  336,
 /*   110 */   361,  365,  368,  371,  369,  375,  376,  381,  378,  382,
 /*   120 */   401,  387,  384,  392,  397,  372,  399,  400,  404,  380,
 /*   130 */   413,  414,  419,  461,  420,  425,  432,  431,  412,  483,
 /*   140 */   464,  465,  470,  495,  496,  507,  453,  473,  501,  536,
 /*   150 */   517,  522,  524,  527,  531,  539,  541,  542,  545,  513,
 /*   160 */   549,  550,  555,  558,  562,  563,  566,  570,  574,  540,
};
#define YY_REDUCE_COUNT (79)
#define YY_REDUCE_MIN   (-148)
#define YY_REDUCE_MAX   (630)
static const short yy_reduce_ofst[] = {
 /*     0 */   132,   14,   35,   58,   93,  121,  141,  213,  272,  294,
 /*    10 */   315,  -65,  374, -101,  -24,   77,  396,  428,  440,  452,
 /*    20 */   466,  479,  491,  503,  523,  535,  547,  559,  571,  584,
 /*    30 */   596,  608,  625,  172,  630,  -84,  -72,  103, -123,  -92,
 /*    40 */   -92,  -92,  -71,   44,  110,  111, -148, -134,  -99,  -62,
 /*    50 */   -77,  -77,  -77,   88,   56,   79,  129,  101,  118,  187,
 /*    60 */   190,  138,  158,  203,  184,  193,  205,  211,  -77,  251,
 /*    70 */   250,  241,  263,  232,  234,  246,   88,  296,  304,  303,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*    10 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*    20 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*    30 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*    40 */   528,  446,  446,  539,  539,  539,  603,  446,  563,  555,
 /*    50 */   531,  545,  532,  446,  588,  548,  446,  570,  568,  446,
 /*    60 */   446,  570,  568,  446,  582,  578,  561,  559,  545,  446,
 /*    70 */   446,  446,  446,  606,  594,  590,  446,  446,  451,  452,
 /*    80 */   446,  446,  446,  446,  581,  580,  505,  504,  503,  499,
 /*    90 */   500,  446,  446,  494,  495,  493,  492,  446,  446,  529,
 /*   100 */   446,  446,  446,  591,  595,  446,  482,  552,  562,  446,
 /*   110 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*   120 */   446,  446,  482,  446,  579,  446,  538,  534,  446,  446,
 /*   130 */   530,  481,  446,  524,  446,  446,  446,  589,  446,  446,
 /*   140 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*   150 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
 /*   160 */   446,  446,  446,  446,  446,  446,  446,  446,  446,  446,
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
  /*    8 */ "NK_BITAND",
  /*    9 */ "NK_BITOR",
  /*   10 */ "NK_LSHIFT",
  /*   11 */ "NK_RSHIFT",
  /*   12 */ "NK_PLUS",
  /*   13 */ "NK_MINUS",
  /*   14 */ "NK_STAR",
  /*   15 */ "NK_SLASH",
  /*   16 */ "NK_REM",
  /*   17 */ "NK_CONCAT",
  /*   18 */ "CREATE",
  /*   19 */ "DATABASE",
  /*   20 */ "IF",
  /*   21 */ "NOT",
  /*   22 */ "EXISTS",
  /*   23 */ "BLOCKS",
  /*   24 */ "NK_INTEGER",
  /*   25 */ "CACHE",
  /*   26 */ "CACHELAST",
  /*   27 */ "COMP",
  /*   28 */ "DAYS",
  /*   29 */ "FSYNC",
  /*   30 */ "MAXROWS",
  /*   31 */ "MINROWS",
  /*   32 */ "KEEP",
  /*   33 */ "PRECISION",
  /*   34 */ "NK_STRING",
  /*   35 */ "QUORUM",
  /*   36 */ "REPLICA",
  /*   37 */ "TTL",
  /*   38 */ "WAL",
  /*   39 */ "VGROUPS",
  /*   40 */ "SINGLESTABLE",
  /*   41 */ "STREAMMODE",
  /*   42 */ "NK_FLOAT",
  /*   43 */ "NK_BOOL",
  /*   44 */ "TIMESTAMP",
  /*   45 */ "NK_VARIABLE",
  /*   46 */ "NK_COMMA",
  /*   47 */ "NK_ID",
  /*   48 */ "NK_LP",
  /*   49 */ "NK_RP",
  /*   50 */ "NK_DOT",
  /*   51 */ "BETWEEN",
  /*   52 */ "IS",
  /*   53 */ "NULL",
  /*   54 */ "NK_LT",
  /*   55 */ "NK_GT",
  /*   56 */ "NK_LE",
  /*   57 */ "NK_GE",
  /*   58 */ "NK_NE",
  /*   59 */ "NK_EQ",
  /*   60 */ "LIKE",
  /*   61 */ "MATCH",
  /*   62 */ "NMATCH",
  /*   63 */ "IN",
  /*   64 */ "FROM",
  /*   65 */ "AS",
  /*   66 */ "JOIN",
  /*   67 */ "ON",
  /*   68 */ "INNER",
  /*   69 */ "SELECT",
  /*   70 */ "DISTINCT",
  /*   71 */ "WHERE",
  /*   72 */ "PARTITION",
  /*   73 */ "BY",
  /*   74 */ "SESSION",
  /*   75 */ "STATE_WINDOW",
  /*   76 */ "INTERVAL",
  /*   77 */ "SLIDING",
  /*   78 */ "FILL",
  /*   79 */ "VALUE",
  /*   80 */ "NONE",
  /*   81 */ "PREV",
  /*   82 */ "LINEAR",
  /*   83 */ "NEXT",
  /*   84 */ "GROUP",
  /*   85 */ "HAVING",
  /*   86 */ "ORDER",
  /*   87 */ "SLIMIT",
  /*   88 */ "SOFFSET",
  /*   89 */ "LIMIT",
  /*   90 */ "OFFSET",
  /*   91 */ "ASC",
  /*   92 */ "DESC",
  /*   93 */ "NULLS",
  /*   94 */ "FIRST",
  /*   95 */ "LAST",
  /*   96 */ "cmd",
  /*   97 */ "exists_opt",
  /*   98 */ "db_name",
  /*   99 */ "db_options",
  /*  100 */ "query_expression",
  /*  101 */ "literal",
  /*  102 */ "duration_literal",
  /*  103 */ "literal_list",
  /*  104 */ "table_name",
  /*  105 */ "column_name",
  /*  106 */ "function_name",
  /*  107 */ "table_alias",
  /*  108 */ "column_alias",
  /*  109 */ "expression",
  /*  110 */ "column_reference",
  /*  111 */ "expression_list",
  /*  112 */ "subquery",
  /*  113 */ "predicate",
  /*  114 */ "compare_op",
  /*  115 */ "in_op",
  /*  116 */ "in_predicate_value",
  /*  117 */ "boolean_value_expression",
  /*  118 */ "boolean_primary",
  /*  119 */ "common_expression",
  /*  120 */ "from_clause",
  /*  121 */ "table_reference_list",
  /*  122 */ "table_reference",
  /*  123 */ "table_primary",
  /*  124 */ "joined_table",
  /*  125 */ "alias_opt",
  /*  126 */ "parenthesized_joined_table",
  /*  127 */ "join_type",
  /*  128 */ "search_condition",
  /*  129 */ "query_specification",
  /*  130 */ "set_quantifier_opt",
  /*  131 */ "select_list",
  /*  132 */ "where_clause_opt",
  /*  133 */ "partition_by_clause_opt",
  /*  134 */ "twindow_clause_opt",
  /*  135 */ "group_by_clause_opt",
  /*  136 */ "having_clause_opt",
  /*  137 */ "select_sublist",
  /*  138 */ "select_item",
  /*  139 */ "sliding_opt",
  /*  140 */ "fill_opt",
  /*  141 */ "fill_mode",
  /*  142 */ "group_by_list",
  /*  143 */ "query_expression_body",
  /*  144 */ "order_by_clause_opt",
  /*  145 */ "slimit_clause_opt",
  /*  146 */ "limit_clause_opt",
  /*  147 */ "query_primary",
  /*  148 */ "sort_specification_list",
  /*  149 */ "sort_specification",
  /*  150 */ "ordering_specification_opt",
  /*  151 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE DATABASE exists_opt db_name db_options",
 /*   1 */ "exists_opt ::= IF NOT EXISTS",
 /*   2 */ "exists_opt ::=",
 /*   3 */ "db_options ::=",
 /*   4 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*   5 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*   6 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*   7 */ "db_options ::= db_options COMP NK_INTEGER",
 /*   8 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*   9 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  10 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  11 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  12 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  13 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  14 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  15 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  16 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  17 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  18 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  19 */ "db_options ::= db_options SINGLESTABLE NK_INTEGER",
 /*  20 */ "db_options ::= db_options STREAMMODE NK_INTEGER",
 /*  21 */ "cmd ::= query_expression",
 /*  22 */ "literal ::= NK_INTEGER",
 /*  23 */ "literal ::= NK_FLOAT",
 /*  24 */ "literal ::= NK_STRING",
 /*  25 */ "literal ::= NK_BOOL",
 /*  26 */ "literal ::= TIMESTAMP NK_STRING",
 /*  27 */ "literal ::= duration_literal",
 /*  28 */ "duration_literal ::= NK_VARIABLE",
 /*  29 */ "literal_list ::= literal",
 /*  30 */ "literal_list ::= literal_list NK_COMMA literal",
 /*  31 */ "db_name ::= NK_ID",
 /*  32 */ "table_name ::= NK_ID",
 /*  33 */ "column_name ::= NK_ID",
 /*  34 */ "function_name ::= NK_ID",
 /*  35 */ "table_alias ::= NK_ID",
 /*  36 */ "column_alias ::= NK_ID",
 /*  37 */ "expression ::= literal",
 /*  38 */ "expression ::= column_reference",
 /*  39 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /*  40 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /*  41 */ "expression ::= subquery",
 /*  42 */ "expression ::= NK_LP expression NK_RP",
 /*  43 */ "expression ::= NK_PLUS expression",
 /*  44 */ "expression ::= NK_MINUS expression",
 /*  45 */ "expression ::= expression NK_PLUS expression",
 /*  46 */ "expression ::= expression NK_MINUS expression",
 /*  47 */ "expression ::= expression NK_STAR expression",
 /*  48 */ "expression ::= expression NK_SLASH expression",
 /*  49 */ "expression ::= expression NK_REM expression",
 /*  50 */ "expression_list ::= expression",
 /*  51 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  52 */ "column_reference ::= column_name",
 /*  53 */ "column_reference ::= table_name NK_DOT column_name",
 /*  54 */ "predicate ::= expression compare_op expression",
 /*  55 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  56 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  57 */ "predicate ::= expression IS NULL",
 /*  58 */ "predicate ::= expression IS NOT NULL",
 /*  59 */ "predicate ::= expression in_op in_predicate_value",
 /*  60 */ "compare_op ::= NK_LT",
 /*  61 */ "compare_op ::= NK_GT",
 /*  62 */ "compare_op ::= NK_LE",
 /*  63 */ "compare_op ::= NK_GE",
 /*  64 */ "compare_op ::= NK_NE",
 /*  65 */ "compare_op ::= NK_EQ",
 /*  66 */ "compare_op ::= LIKE",
 /*  67 */ "compare_op ::= NOT LIKE",
 /*  68 */ "compare_op ::= MATCH",
 /*  69 */ "compare_op ::= NMATCH",
 /*  70 */ "in_op ::= IN",
 /*  71 */ "in_op ::= NOT IN",
 /*  72 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /*  73 */ "boolean_value_expression ::= boolean_primary",
 /*  74 */ "boolean_value_expression ::= NOT boolean_primary",
 /*  75 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /*  76 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /*  77 */ "boolean_primary ::= predicate",
 /*  78 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /*  79 */ "common_expression ::= expression",
 /*  80 */ "common_expression ::= boolean_value_expression",
 /*  81 */ "from_clause ::= FROM table_reference_list",
 /*  82 */ "table_reference_list ::= table_reference",
 /*  83 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /*  84 */ "table_reference ::= table_primary",
 /*  85 */ "table_reference ::= joined_table",
 /*  86 */ "table_primary ::= table_name alias_opt",
 /*  87 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /*  88 */ "table_primary ::= subquery alias_opt",
 /*  89 */ "table_primary ::= parenthesized_joined_table",
 /*  90 */ "alias_opt ::=",
 /*  91 */ "alias_opt ::= table_alias",
 /*  92 */ "alias_opt ::= AS table_alias",
 /*  93 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /*  94 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /*  95 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /*  96 */ "join_type ::=",
 /*  97 */ "join_type ::= INNER",
 /*  98 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /*  99 */ "set_quantifier_opt ::=",
 /* 100 */ "set_quantifier_opt ::= DISTINCT",
 /* 101 */ "set_quantifier_opt ::= ALL",
 /* 102 */ "select_list ::= NK_STAR",
 /* 103 */ "select_list ::= select_sublist",
 /* 104 */ "select_sublist ::= select_item",
 /* 105 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 106 */ "select_item ::= common_expression",
 /* 107 */ "select_item ::= common_expression column_alias",
 /* 108 */ "select_item ::= common_expression AS column_alias",
 /* 109 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 110 */ "where_clause_opt ::=",
 /* 111 */ "where_clause_opt ::= WHERE search_condition",
 /* 112 */ "partition_by_clause_opt ::=",
 /* 113 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 114 */ "twindow_clause_opt ::=",
 /* 115 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 116 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 117 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 118 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 119 */ "sliding_opt ::=",
 /* 120 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 121 */ "fill_opt ::=",
 /* 122 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 123 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 124 */ "fill_mode ::= NONE",
 /* 125 */ "fill_mode ::= PREV",
 /* 126 */ "fill_mode ::= NULL",
 /* 127 */ "fill_mode ::= LINEAR",
 /* 128 */ "fill_mode ::= NEXT",
 /* 129 */ "group_by_clause_opt ::=",
 /* 130 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 131 */ "group_by_list ::= expression",
 /* 132 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 133 */ "having_clause_opt ::=",
 /* 134 */ "having_clause_opt ::= HAVING search_condition",
 /* 135 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 136 */ "query_expression_body ::= query_primary",
 /* 137 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 138 */ "query_primary ::= query_specification",
 /* 139 */ "order_by_clause_opt ::=",
 /* 140 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 141 */ "slimit_clause_opt ::=",
 /* 142 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 143 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 144 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 145 */ "limit_clause_opt ::=",
 /* 146 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 147 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 148 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 149 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 150 */ "search_condition ::= common_expression",
 /* 151 */ "sort_specification_list ::= sort_specification",
 /* 152 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 153 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 154 */ "ordering_specification_opt ::=",
 /* 155 */ "ordering_specification_opt ::= ASC",
 /* 156 */ "ordering_specification_opt ::= DESC",
 /* 157 */ "null_ordering_opt ::=",
 /* 158 */ "null_ordering_opt ::= NULLS FIRST",
 /* 159 */ "null_ordering_opt ::= NULLS LAST",
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
    case 96: /* cmd */
    case 97: /* exists_opt */
    case 100: /* query_expression */
    case 101: /* literal */
    case 102: /* duration_literal */
    case 109: /* expression */
    case 110: /* column_reference */
    case 112: /* subquery */
    case 113: /* predicate */
    case 116: /* in_predicate_value */
    case 117: /* boolean_value_expression */
    case 118: /* boolean_primary */
    case 119: /* common_expression */
    case 120: /* from_clause */
    case 121: /* table_reference_list */
    case 122: /* table_reference */
    case 123: /* table_primary */
    case 124: /* joined_table */
    case 126: /* parenthesized_joined_table */
    case 128: /* search_condition */
    case 129: /* query_specification */
    case 132: /* where_clause_opt */
    case 134: /* twindow_clause_opt */
    case 136: /* having_clause_opt */
    case 138: /* select_item */
    case 139: /* sliding_opt */
    case 140: /* fill_opt */
    case 143: /* query_expression_body */
    case 145: /* slimit_clause_opt */
    case 146: /* limit_clause_opt */
    case 147: /* query_primary */
    case 149: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy104)); 
}
      break;
    case 98: /* db_name */
    case 104: /* table_name */
    case 105: /* column_name */
    case 106: /* function_name */
    case 107: /* table_alias */
    case 108: /* column_alias */
    case 125: /* alias_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 99: /* db_options */
{
 tfree((yypminor->yy87)); 
}
      break;
    case 103: /* literal_list */
    case 111: /* expression_list */
    case 131: /* select_list */
    case 133: /* partition_by_clause_opt */
    case 135: /* group_by_clause_opt */
    case 137: /* select_sublist */
    case 142: /* group_by_list */
    case 144: /* order_by_clause_opt */
    case 148: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy8)); 
}
      break;
    case 114: /* compare_op */
    case 115: /* in_op */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 127: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 130: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 141: /* fill_mode */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 150: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 151: /* null_ordering_opt */
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
  {   96,   -5 }, /* (0) cmd ::= CREATE DATABASE exists_opt db_name db_options */
  {   97,   -3 }, /* (1) exists_opt ::= IF NOT EXISTS */
  {   97,    0 }, /* (2) exists_opt ::= */
  {   99,    0 }, /* (3) db_options ::= */
  {   99,   -3 }, /* (4) db_options ::= db_options BLOCKS NK_INTEGER */
  {   99,   -3 }, /* (5) db_options ::= db_options CACHE NK_INTEGER */
  {   99,   -3 }, /* (6) db_options ::= db_options CACHELAST NK_INTEGER */
  {   99,   -3 }, /* (7) db_options ::= db_options COMP NK_INTEGER */
  {   99,   -3 }, /* (8) db_options ::= db_options DAYS NK_INTEGER */
  {   99,   -3 }, /* (9) db_options ::= db_options FSYNC NK_INTEGER */
  {   99,   -3 }, /* (10) db_options ::= db_options MAXROWS NK_INTEGER */
  {   99,   -3 }, /* (11) db_options ::= db_options MINROWS NK_INTEGER */
  {   99,   -3 }, /* (12) db_options ::= db_options KEEP NK_INTEGER */
  {   99,   -3 }, /* (13) db_options ::= db_options PRECISION NK_STRING */
  {   99,   -3 }, /* (14) db_options ::= db_options QUORUM NK_INTEGER */
  {   99,   -3 }, /* (15) db_options ::= db_options REPLICA NK_INTEGER */
  {   99,   -3 }, /* (16) db_options ::= db_options TTL NK_INTEGER */
  {   99,   -3 }, /* (17) db_options ::= db_options WAL NK_INTEGER */
  {   99,   -3 }, /* (18) db_options ::= db_options VGROUPS NK_INTEGER */
  {   99,   -3 }, /* (19) db_options ::= db_options SINGLESTABLE NK_INTEGER */
  {   99,   -3 }, /* (20) db_options ::= db_options STREAMMODE NK_INTEGER */
  {   96,   -1 }, /* (21) cmd ::= query_expression */
  {  101,   -1 }, /* (22) literal ::= NK_INTEGER */
  {  101,   -1 }, /* (23) literal ::= NK_FLOAT */
  {  101,   -1 }, /* (24) literal ::= NK_STRING */
  {  101,   -1 }, /* (25) literal ::= NK_BOOL */
  {  101,   -2 }, /* (26) literal ::= TIMESTAMP NK_STRING */
  {  101,   -1 }, /* (27) literal ::= duration_literal */
  {  102,   -1 }, /* (28) duration_literal ::= NK_VARIABLE */
  {  103,   -1 }, /* (29) literal_list ::= literal */
  {  103,   -3 }, /* (30) literal_list ::= literal_list NK_COMMA literal */
  {   98,   -1 }, /* (31) db_name ::= NK_ID */
  {  104,   -1 }, /* (32) table_name ::= NK_ID */
  {  105,   -1 }, /* (33) column_name ::= NK_ID */
  {  106,   -1 }, /* (34) function_name ::= NK_ID */
  {  107,   -1 }, /* (35) table_alias ::= NK_ID */
  {  108,   -1 }, /* (36) column_alias ::= NK_ID */
  {  109,   -1 }, /* (37) expression ::= literal */
  {  109,   -1 }, /* (38) expression ::= column_reference */
  {  109,   -4 }, /* (39) expression ::= function_name NK_LP expression_list NK_RP */
  {  109,   -4 }, /* (40) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  109,   -1 }, /* (41) expression ::= subquery */
  {  109,   -3 }, /* (42) expression ::= NK_LP expression NK_RP */
  {  109,   -2 }, /* (43) expression ::= NK_PLUS expression */
  {  109,   -2 }, /* (44) expression ::= NK_MINUS expression */
  {  109,   -3 }, /* (45) expression ::= expression NK_PLUS expression */
  {  109,   -3 }, /* (46) expression ::= expression NK_MINUS expression */
  {  109,   -3 }, /* (47) expression ::= expression NK_STAR expression */
  {  109,   -3 }, /* (48) expression ::= expression NK_SLASH expression */
  {  109,   -3 }, /* (49) expression ::= expression NK_REM expression */
  {  111,   -1 }, /* (50) expression_list ::= expression */
  {  111,   -3 }, /* (51) expression_list ::= expression_list NK_COMMA expression */
  {  110,   -1 }, /* (52) column_reference ::= column_name */
  {  110,   -3 }, /* (53) column_reference ::= table_name NK_DOT column_name */
  {  113,   -3 }, /* (54) predicate ::= expression compare_op expression */
  {  113,   -5 }, /* (55) predicate ::= expression BETWEEN expression AND expression */
  {  113,   -6 }, /* (56) predicate ::= expression NOT BETWEEN expression AND expression */
  {  113,   -3 }, /* (57) predicate ::= expression IS NULL */
  {  113,   -4 }, /* (58) predicate ::= expression IS NOT NULL */
  {  113,   -3 }, /* (59) predicate ::= expression in_op in_predicate_value */
  {  114,   -1 }, /* (60) compare_op ::= NK_LT */
  {  114,   -1 }, /* (61) compare_op ::= NK_GT */
  {  114,   -1 }, /* (62) compare_op ::= NK_LE */
  {  114,   -1 }, /* (63) compare_op ::= NK_GE */
  {  114,   -1 }, /* (64) compare_op ::= NK_NE */
  {  114,   -1 }, /* (65) compare_op ::= NK_EQ */
  {  114,   -1 }, /* (66) compare_op ::= LIKE */
  {  114,   -2 }, /* (67) compare_op ::= NOT LIKE */
  {  114,   -1 }, /* (68) compare_op ::= MATCH */
  {  114,   -1 }, /* (69) compare_op ::= NMATCH */
  {  115,   -1 }, /* (70) in_op ::= IN */
  {  115,   -2 }, /* (71) in_op ::= NOT IN */
  {  116,   -3 }, /* (72) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  117,   -1 }, /* (73) boolean_value_expression ::= boolean_primary */
  {  117,   -2 }, /* (74) boolean_value_expression ::= NOT boolean_primary */
  {  117,   -3 }, /* (75) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  117,   -3 }, /* (76) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  118,   -1 }, /* (77) boolean_primary ::= predicate */
  {  118,   -3 }, /* (78) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  119,   -1 }, /* (79) common_expression ::= expression */
  {  119,   -1 }, /* (80) common_expression ::= boolean_value_expression */
  {  120,   -2 }, /* (81) from_clause ::= FROM table_reference_list */
  {  121,   -1 }, /* (82) table_reference_list ::= table_reference */
  {  121,   -3 }, /* (83) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  122,   -1 }, /* (84) table_reference ::= table_primary */
  {  122,   -1 }, /* (85) table_reference ::= joined_table */
  {  123,   -2 }, /* (86) table_primary ::= table_name alias_opt */
  {  123,   -4 }, /* (87) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  123,   -2 }, /* (88) table_primary ::= subquery alias_opt */
  {  123,   -1 }, /* (89) table_primary ::= parenthesized_joined_table */
  {  125,    0 }, /* (90) alias_opt ::= */
  {  125,   -1 }, /* (91) alias_opt ::= table_alias */
  {  125,   -2 }, /* (92) alias_opt ::= AS table_alias */
  {  126,   -3 }, /* (93) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  126,   -3 }, /* (94) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  124,   -6 }, /* (95) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  127,    0 }, /* (96) join_type ::= */
  {  127,   -1 }, /* (97) join_type ::= INNER */
  {  129,   -9 }, /* (98) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  130,    0 }, /* (99) set_quantifier_opt ::= */
  {  130,   -1 }, /* (100) set_quantifier_opt ::= DISTINCT */
  {  130,   -1 }, /* (101) set_quantifier_opt ::= ALL */
  {  131,   -1 }, /* (102) select_list ::= NK_STAR */
  {  131,   -1 }, /* (103) select_list ::= select_sublist */
  {  137,   -1 }, /* (104) select_sublist ::= select_item */
  {  137,   -3 }, /* (105) select_sublist ::= select_sublist NK_COMMA select_item */
  {  138,   -1 }, /* (106) select_item ::= common_expression */
  {  138,   -2 }, /* (107) select_item ::= common_expression column_alias */
  {  138,   -3 }, /* (108) select_item ::= common_expression AS column_alias */
  {  138,   -3 }, /* (109) select_item ::= table_name NK_DOT NK_STAR */
  {  132,    0 }, /* (110) where_clause_opt ::= */
  {  132,   -2 }, /* (111) where_clause_opt ::= WHERE search_condition */
  {  133,    0 }, /* (112) partition_by_clause_opt ::= */
  {  133,   -3 }, /* (113) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  134,    0 }, /* (114) twindow_clause_opt ::= */
  {  134,   -6 }, /* (115) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  134,   -4 }, /* (116) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  134,   -6 }, /* (117) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  134,   -8 }, /* (118) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  139,    0 }, /* (119) sliding_opt ::= */
  {  139,   -4 }, /* (120) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  140,    0 }, /* (121) fill_opt ::= */
  {  140,   -4 }, /* (122) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  140,   -6 }, /* (123) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  141,   -1 }, /* (124) fill_mode ::= NONE */
  {  141,   -1 }, /* (125) fill_mode ::= PREV */
  {  141,   -1 }, /* (126) fill_mode ::= NULL */
  {  141,   -1 }, /* (127) fill_mode ::= LINEAR */
  {  141,   -1 }, /* (128) fill_mode ::= NEXT */
  {  135,    0 }, /* (129) group_by_clause_opt ::= */
  {  135,   -3 }, /* (130) group_by_clause_opt ::= GROUP BY group_by_list */
  {  142,   -1 }, /* (131) group_by_list ::= expression */
  {  142,   -3 }, /* (132) group_by_list ::= group_by_list NK_COMMA expression */
  {  136,    0 }, /* (133) having_clause_opt ::= */
  {  136,   -2 }, /* (134) having_clause_opt ::= HAVING search_condition */
  {  100,   -4 }, /* (135) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  143,   -1 }, /* (136) query_expression_body ::= query_primary */
  {  143,   -4 }, /* (137) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  147,   -1 }, /* (138) query_primary ::= query_specification */
  {  144,    0 }, /* (139) order_by_clause_opt ::= */
  {  144,   -3 }, /* (140) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  145,    0 }, /* (141) slimit_clause_opt ::= */
  {  145,   -2 }, /* (142) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  145,   -4 }, /* (143) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  145,   -4 }, /* (144) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  146,    0 }, /* (145) limit_clause_opt ::= */
  {  146,   -2 }, /* (146) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  146,   -4 }, /* (147) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  146,   -4 }, /* (148) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  112,   -3 }, /* (149) subquery ::= NK_LP query_expression NK_RP */
  {  128,   -1 }, /* (150) search_condition ::= common_expression */
  {  148,   -1 }, /* (151) sort_specification_list ::= sort_specification */
  {  148,   -3 }, /* (152) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  149,   -3 }, /* (153) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  150,    0 }, /* (154) ordering_specification_opt ::= */
  {  150,   -1 }, /* (155) ordering_specification_opt ::= ASC */
  {  150,   -1 }, /* (156) ordering_specification_opt ::= DESC */
  {  151,    0 }, /* (157) null_ordering_opt ::= */
  {  151,   -2 }, /* (158) null_ordering_opt ::= NULLS FIRST */
  {  151,   -2 }, /* (159) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= CREATE DATABASE exists_opt db_name db_options */
{ PARSER_TRACE; pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy185, &yymsp[-1].minor.yy129, yymsp[0].minor.yy87);}
        break;
      case 1: /* exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy185 = true; }
        break;
      case 2: /* exists_opt ::= */
{ yymsp[1].minor.yy185 = false; }
        break;
      case 3: /* db_options ::= */
{ yymsp[1].minor.yy87 = createDefaultDatabaseOptions(pCxt);}
        break;
      case 4: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 5: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 6: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 7: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 8: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 9: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 10: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 11: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 12: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 13: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 14: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 15: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 16: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 17: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 18: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 19: /* db_options ::= db_options SINGLESTABLE NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 20: /* db_options ::= db_options STREAMMODE NK_INTEGER */
{ yylhsminor.yy87 = setDatabaseOption(pCxt, yymsp[-2].minor.yy87, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy87 = yylhsminor.yy87;
        break;
      case 21: /* cmd ::= query_expression */
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy104; }
        break;
      case 22: /* literal ::= NK_INTEGER */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 23: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 24: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 25: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 26: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 27: /* literal ::= duration_literal */
      case 37: /* expression ::= literal */ yytestcase(yyruleno==37);
      case 38: /* expression ::= column_reference */ yytestcase(yyruleno==38);
      case 41: /* expression ::= subquery */ yytestcase(yyruleno==41);
      case 73: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==73);
      case 77: /* boolean_primary ::= predicate */ yytestcase(yyruleno==77);
      case 82: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==82);
      case 84: /* table_reference ::= table_primary */ yytestcase(yyruleno==84);
      case 85: /* table_reference ::= joined_table */ yytestcase(yyruleno==85);
      case 89: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==89);
      case 136: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==136);
      case 138: /* query_primary ::= query_specification */ yytestcase(yyruleno==138);
{ PARSER_TRACE; yylhsminor.yy104 = yymsp[0].minor.yy104; }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 28: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 29: /* literal_list ::= literal */
      case 50: /* expression_list ::= expression */ yytestcase(yyruleno==50);
{ PARSER_TRACE; yylhsminor.yy8 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy104)); }
  yymsp[0].minor.yy8 = yylhsminor.yy8;
        break;
      case 30: /* literal_list ::= literal_list NK_COMMA literal */
      case 51: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==51);
{ PARSER_TRACE; yylhsminor.yy8 = addNodeToList(pCxt, yymsp[-2].minor.yy8, releaseRawExprNode(pCxt, yymsp[0].minor.yy104)); }
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 31: /* db_name ::= NK_ID */
      case 32: /* table_name ::= NK_ID */ yytestcase(yyruleno==32);
      case 33: /* column_name ::= NK_ID */ yytestcase(yyruleno==33);
      case 34: /* function_name ::= NK_ID */ yytestcase(yyruleno==34);
      case 35: /* table_alias ::= NK_ID */ yytestcase(yyruleno==35);
      case 36: /* column_alias ::= NK_ID */ yytestcase(yyruleno==36);
{ PARSER_TRACE; yylhsminor.yy129 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 39: /* expression ::= function_name NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy129, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy129, yymsp[-1].minor.yy8)); }
  yymsp[-3].minor.yy104 = yylhsminor.yy104;
        break;
      case 40: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy129, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy129, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy104 = yylhsminor.yy104;
        break;
      case 42: /* expression ::= NK_LP expression NK_RP */
      case 78: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==78);
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy104)); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 43: /* expression ::= NK_PLUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy104));
                                                                                  }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 44: /* expression ::= NK_MINUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy104), NULL));
                                                                                  }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 45: /* expression ::= expression NK_PLUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); 
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 46: /* expression ::= expression NK_MINUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); 
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 47: /* expression ::= expression NK_STAR expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); 
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 48: /* expression ::= expression NK_SLASH expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); 
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 49: /* expression ::= expression NK_REM expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); 
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 52: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNode(pCxt, &yymsp[0].minor.yy129, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy129)); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 53: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129, createColumnNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy129)); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 54: /* predicate ::= expression compare_op expression */
      case 59: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==59);
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy60, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104)));
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 55: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy104), releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104)));
                                                                                  }
  yymsp[-4].minor.yy104 = yylhsminor.yy104;
        break;
      case 56: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[-5].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104)));
                                                                                  }
  yymsp[-5].minor.yy104 = yylhsminor.yy104;
        break;
      case 57: /* predicate ::= expression IS NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), NULL));
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 58: /* predicate ::= expression IS NOT NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy104), NULL));
                                                                                  }
  yymsp[-3].minor.yy104 = yylhsminor.yy104;
        break;
      case 60: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_LOWER_THAN; }
        break;
      case 61: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_GREATER_THAN; }
        break;
      case 62: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 63: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 64: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_NOT_EQUAL; }
        break;
      case 65: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_EQUAL; }
        break;
      case 66: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_LIKE; }
        break;
      case 67: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy60 = OP_TYPE_NOT_LIKE; }
        break;
      case 68: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_MATCH; }
        break;
      case 69: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_NMATCH; }
        break;
      case 70: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy60 = OP_TYPE_IN; }
        break;
      case 71: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy60 = OP_TYPE_NOT_IN; }
        break;
      case 72: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy8)); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 74: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy104), NULL));
                                                                                  }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 75: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104)));
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 76: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy104);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), releaseRawExprNode(pCxt, yymsp[0].minor.yy104)));
                                                                                  }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 79: /* common_expression ::= expression */
      case 80: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==80);
{ yylhsminor.yy104 = yymsp[0].minor.yy104; }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 81: /* from_clause ::= FROM table_reference_list */
      case 111: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==111);
      case 134: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==134);
{ PARSER_TRACE; yymsp[-1].minor.yy104 = yymsp[0].minor.yy104; }
        break;
      case 83: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy104 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy104, yymsp[0].minor.yy104, NULL); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 86: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy104 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy129, &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 87: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy104 = createRealTableNode(pCxt, &yymsp[-3].minor.yy129, &yymsp[-1].minor.yy129, &yymsp[0].minor.yy129); }
  yymsp[-3].minor.yy104 = yylhsminor.yy104;
        break;
      case 88: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy104 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy104), &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 90: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy129 = nil_token;  }
        break;
      case 91: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy129 = yymsp[0].minor.yy129; }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 92: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy129 = yymsp[0].minor.yy129; }
        break;
      case 93: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 94: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==94);
{ PARSER_TRACE; yymsp[-2].minor.yy104 = yymsp[-1].minor.yy104; }
        break;
      case 95: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy104 = createJoinTableNode(pCxt, yymsp[-4].minor.yy228, yymsp[-5].minor.yy104, yymsp[-2].minor.yy104, yymsp[0].minor.yy104); }
  yymsp[-5].minor.yy104 = yylhsminor.yy104;
        break;
      case 96: /* join_type ::= */
{ PARSER_TRACE; yymsp[1].minor.yy228 = JOIN_TYPE_INNER; }
        break;
      case 97: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy228 = JOIN_TYPE_INNER; }
        break;
      case 98: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy104 = createSelectStmt(pCxt, yymsp[-7].minor.yy185, yymsp[-6].minor.yy8, yymsp[-5].minor.yy104);
                                                                                    yymsp[-8].minor.yy104 = addWhereClause(pCxt, yymsp[-8].minor.yy104, yymsp[-4].minor.yy104);
                                                                                    yymsp[-8].minor.yy104 = addPartitionByClause(pCxt, yymsp[-8].minor.yy104, yymsp[-3].minor.yy8);
                                                                                    yymsp[-8].minor.yy104 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy104, yymsp[-2].minor.yy104);
                                                                                    yymsp[-8].minor.yy104 = addGroupByClause(pCxt, yymsp[-8].minor.yy104, yymsp[-1].minor.yy8);
                                                                                    yymsp[-8].minor.yy104 = addHavingClause(pCxt, yymsp[-8].minor.yy104, yymsp[0].minor.yy104);
                                                                                  }
        break;
      case 99: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy185 = false; }
        break;
      case 100: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy185 = true; }
        break;
      case 101: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy185 = false; }
        break;
      case 102: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy8 = NULL; }
        break;
      case 103: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy8 = yymsp[0].minor.yy8; }
  yymsp[0].minor.yy8 = yylhsminor.yy8;
        break;
      case 104: /* select_sublist ::= select_item */
      case 151: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==151);
{ PARSER_TRACE; yylhsminor.yy8 = createNodeList(pCxt, yymsp[0].minor.yy104); }
  yymsp[0].minor.yy8 = yylhsminor.yy8;
        break;
      case 105: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 152: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==152);
{ PARSER_TRACE; yylhsminor.yy8 = addNodeToList(pCxt, yymsp[-2].minor.yy8, yymsp[0].minor.yy104); }
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 106: /* select_item ::= common_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy104);
                                                                                    yylhsminor.yy104 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy104), &t);
                                                                                  }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 107: /* select_item ::= common_expression column_alias */
{ PARSER_TRACE; yylhsminor.yy104 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy104), &yymsp[0].minor.yy129); }
  yymsp[-1].minor.yy104 = yylhsminor.yy104;
        break;
      case 108: /* select_item ::= common_expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy104 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), &yymsp[0].minor.yy129); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 109: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy104 = createColumnNode(pCxt, &yymsp[-2].minor.yy129, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 110: /* where_clause_opt ::= */
      case 114: /* twindow_clause_opt ::= */ yytestcase(yyruleno==114);
      case 119: /* sliding_opt ::= */ yytestcase(yyruleno==119);
      case 121: /* fill_opt ::= */ yytestcase(yyruleno==121);
      case 133: /* having_clause_opt ::= */ yytestcase(yyruleno==133);
      case 141: /* slimit_clause_opt ::= */ yytestcase(yyruleno==141);
      case 145: /* limit_clause_opt ::= */ yytestcase(yyruleno==145);
{ PARSER_TRACE; yymsp[1].minor.yy104 = NULL; }
        break;
      case 112: /* partition_by_clause_opt ::= */
      case 129: /* group_by_clause_opt ::= */ yytestcase(yyruleno==129);
      case 139: /* order_by_clause_opt ::= */ yytestcase(yyruleno==139);
{ PARSER_TRACE; yymsp[1].minor.yy8 = NULL; }
        break;
      case 113: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 130: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==130);
      case 140: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==140);
{ PARSER_TRACE; yymsp[-2].minor.yy8 = yymsp[0].minor.yy8; }
        break;
      case 115: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy104 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy104), &yymsp[-1].minor.yy0); }
        break;
      case 116: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy104 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy104)); }
        break;
      case 117: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy104 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy104, NULL, yymsp[-1].minor.yy104, yymsp[0].minor.yy104); }
        break;
      case 118: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy104 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy104, yymsp[-3].minor.yy104, yymsp[-1].minor.yy104, yymsp[0].minor.yy104); }
        break;
      case 120: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy104 = yymsp[-1].minor.yy104; }
        break;
      case 122: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy104 = createFillNode(pCxt, yymsp[-1].minor.yy246, NULL); }
        break;
      case 123: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy104 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy8)); }
        break;
      case 124: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy246 = FILL_MODE_NONE; }
        break;
      case 125: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy246 = FILL_MODE_PREV; }
        break;
      case 126: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy246 = FILL_MODE_NULL; }
        break;
      case 127: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy246 = FILL_MODE_LINEAR; }
        break;
      case 128: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy246 = FILL_MODE_NEXT; }
        break;
      case 131: /* group_by_list ::= expression */
{ PARSER_TRACE; yylhsminor.yy8 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); }
  yymsp[0].minor.yy8 = yylhsminor.yy8;
        break;
      case 132: /* group_by_list ::= group_by_list NK_COMMA expression */
{ PARSER_TRACE; yylhsminor.yy8 = addNodeToList(pCxt, yymsp[-2].minor.yy8, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy104))); }
  yymsp[-2].minor.yy8 = yylhsminor.yy8;
        break;
      case 135: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy104 = addOrderByClause(pCxt, yymsp[-3].minor.yy104, yymsp[-2].minor.yy8);
                                                                                    yylhsminor.yy104 = addSlimitClause(pCxt, yylhsminor.yy104, yymsp[-1].minor.yy104);
                                                                                    yylhsminor.yy104 = addLimitClause(pCxt, yylhsminor.yy104, yymsp[0].minor.yy104);
                                                                                  }
  yymsp[-3].minor.yy104 = yylhsminor.yy104;
        break;
      case 137: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy104 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy104, yymsp[0].minor.yy104); }
  yymsp[-3].minor.yy104 = yylhsminor.yy104;
        break;
      case 142: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 146: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==146);
{ PARSER_TRACE; yymsp[-1].minor.yy104 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 143: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 147: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==147);
{ PARSER_TRACE; yymsp[-3].minor.yy104 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 144: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 148: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==148);
{ PARSER_TRACE; yymsp[-3].minor.yy104 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 149: /* subquery ::= NK_LP query_expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy104 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy104); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 150: /* search_condition ::= common_expression */
{ PARSER_TRACE; yylhsminor.yy104 = releaseRawExprNode(pCxt, yymsp[0].minor.yy104); }
  yymsp[0].minor.yy104 = yylhsminor.yy104;
        break;
      case 153: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy104 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy104), yymsp[-1].minor.yy50, yymsp[0].minor.yy186); }
  yymsp[-2].minor.yy104 = yylhsminor.yy104;
        break;
      case 154: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy50 = ORDER_ASC; }
        break;
      case 155: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy50 = ORDER_ASC; }
        break;
      case 156: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy50 = ORDER_DESC; }
        break;
      case 157: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy186 = NULL_ORDER_DEFAULT; }
        break;
      case 158: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy186 = NULL_ORDER_FIRST; }
        break;
      case 159: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy186 = NULL_ORDER_LAST; }
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
