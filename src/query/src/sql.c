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
#include "qSqlparser.h"
#include "tcmdtype.h"
#include "tstoken.h"
#include "ttokendef.h"
#include "tutil.h"
#include "tvariant.h"
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
**    ParseTOKENTYPE     is the data type used for minor type for terminal
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
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    ParseARG_SDECL     A static variable declaration for the %extra_argument
**    ParseARG_PDECL     A parameter declaration for the %extra_argument
**    ParseARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    ParseCTX_*         As ParseARG_ except for %extra_context
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
#define YYCODETYPE unsigned short int
#define YYNOCODE 281
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy42;
  SCreateAcctInfo yy47;
  SQuerySQL* yy114;
  TAOS_FIELD yy179;
  SLimitVal yy204;
  SSubclauseInfo* yy219;
  int yy222;
  SArray* yy247;
  SCreateDbInfo yy262;
  tSQLExpr* yy326;
  SCreateTableSQL* yy358;
  tVariant yy378;
  int64_t yy403;
  SIntervalVal yy430;
  tSQLExprList* yy522;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_PARAM ,pInfo
#define ParseARG_FETCH SSqlInfo* pInfo=yypParser->pInfo;
#define ParseARG_STORE yypParser->pInfo=pInfo;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYFALLBACK 1
#define YYNSTATE             294
#define YYNRULE              254
#define YYNRULE_WITH_ACTION  254
#define YYNTOKEN             210
#define YY_MAX_SHIFT         293
#define YY_MIN_SHIFTREDUCE   477
#define YY_MAX_SHIFTREDUCE   730
#define YY_ERROR_ACTION      731
#define YY_ACCEPT_ACTION     732
#define YY_NO_ACTION         733
#define YY_MIN_REDUCE        734
#define YY_MAX_REDUCE        987
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
#define YY_ACTTAB_COUNT (651)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    74,  521,  732,  293,  521,  165,  186,  291,   28,  522,
 /*    10 */   190,  893,  522,   43,   44,  969,   47,   48,   15,  776,
 /*    20 */   198,   37,  152,   46,  242,   51,   49,   53,   50,  854,
 /*    30 */   855,   27,  858,   42,   41,  871,  128,   40,   39,   38,
 /*    40 */    43,   44,  882,   47,   48,  882,  188,  198,   37,  868,
 /*    50 */    46,  242,   51,   49,   53,   50,  187,  128,  203,  225,
 /*    60 */    42,   41,  979,  165,   40,   39,   38,   43,   44,  890,
 /*    70 */    47,   48,  193,  970,  198,   37,  165,   46,  242,   51,
 /*    80 */    49,   53,   50,  871,  128,  192,  970,   42,   41,  258,
 /*    90 */   521,   40,   39,   38,  290,  289,  115,  239,  522,   71,
 /*   100 */    77,   43,   45,  128,   47,   48,  205,   66,  198,   37,
 /*   110 */    28,   46,  242,   51,   49,   53,   50,   40,   39,   38,
 /*   120 */   921,   42,   41,  278,   65,   40,   39,   38,  865,  678,
 /*   130 */   287,  871,  859,  210,  478,  479,  480,  481,  482,  483,
 /*   140 */   484,  485,  486,  487,  488,  489,  292,   72,  201,  215,
 /*   150 */    44,  868,   47,   48,  856,  871,  198,   37,  209,   46,
 /*   160 */   242,   51,   49,   53,   50,  869,  922,  204,  237,   42,
 /*   170 */    41,   96,  163,   40,   39,   38,  278,   21,  256,  286,
 /*   180 */   285,  255,  254,  253,  284,  252,  283,  282,  281,  251,
 /*   190 */   280,  279,  835,  594,  823,  824,  825,  826,  827,  828,
 /*   200 */   829,  830,  831,  832,  833,  834,  836,  837,   47,   48,
 /*   210 */    87,   86,  198,   37,   28,   46,  242,   51,   49,   53,
 /*   220 */    50,  268,  267,   16,  211,   42,   41,  265,  264,   40,
 /*   230 */    39,   38,  197,  691,   28,  634,  682,  207,  685,  174,
 /*   240 */   688,   22,   42,   41,   73,  175,   40,   39,   38,   34,
 /*   250 */   108,  107,  173,  197,  691,  867,   67,  682,   28,  685,
 /*   260 */    21,  688,  286,  285,  194,  195,  169,  284,  241,  283,
 /*   270 */   282,  281,  202,  280,  279,  868,  618,   28,   60,  615,
 /*   280 */    22,  616,  631,  617,  218,  194,  195,  123,   34,   23,
 /*   290 */   841,  222,  221,  839,  840,  857,  261,   61,  842,  868,
 /*   300 */   844,  845,  843,  208,  846,  847,  260,  212,  213,  224,
 /*   310 */   638,   51,   49,   53,   50,  262,  181,   28,  868,   42,
 /*   320 */    41,   94,   98,   40,   39,   38,   28,   88,  103,  106,
 /*   330 */    97,   10,   52,    3,  142,   76,  100,  138,  680,   31,
 /*   340 */    83,   79,   82,  158,  154,  690,  230,  659,  660,  156,
 /*   350 */   111,  110,  109,   52,  785,  266,  777,  152,  868,  152,
 /*   360 */   689,  626,  121,  684,  270,  687,  690,  868,  196,  227,
 /*   370 */    34,  228,  258,  646,  681,   29,  683,  125,  686,  650,
 /*   380 */   651,  689,  619,   56,   18,  711,  692,  243,  966,   17,
 /*   390 */    17,   57,  604,  245,  606,  247,   29,   29,   56,   75,
 /*   400 */   605,   63,   26,  593,   56,  248,   12,   11,   93,   92,
 /*   410 */     4,  965,   58,   14,   13,  622,  620,  623,  621,  105,
 /*   420 */   104,  120,  118,  932,  964,  182,  183,  167,  168,  170,
 /*   430 */   164,  171,  172,  178,  179,  177,  162,  176,  166,  870,
 /*   440 */   931,  199,  928,  927,  884,  200,  269,  122,  892,   35,
 /*   450 */   899,  901,  124,  139,  864,  914,  140,  141,  913,  137,
 /*   460 */   787,  250,  160,   32,  259,   34,  784,  984,   84,  983,
 /*   470 */   981,  143,  226,  119,  231,  263,  978,   90,  977,  975,
 /*   480 */   694,  144,  645,  805,  189,  235,   62,  881,  129,   33,
 /*   490 */    59,  240,   30,   54,  161,  132,  130,  238,  236,  131,
 /*   500 */   774,   99,  772,  133,  234,  134,  232,  101,   36,  102,
 /*   510 */   770,   95,  769,  271,  272,  214,  153,  767,  766,  765,
 /*   520 */   764,  763,  273,  155,  157,  760,  758,  756,  754,  752,
 /*   530 */   159,  274,  229,   68,   69,  915,  275,  276,  277,  184,
 /*   540 */   206,  249,  730,  185,  180,  288,   80,  216,  217,  768,
 /*   550 */   729,  220,  219,  112,  728,  762,  147,  761,  146,  806,
 /*   560 */   145,  148,  149,  151,  150,  113,  114,  753,    1,  716,
 /*   570 */     2,  223,  227,  628,   64,    6,  866,  244,   70,  647,
 /*   580 */   126,  135,  136,  191,   24,  233,    7,  652,  127,    8,
 /*   590 */   693,    5,   25,    9,   19,  246,   20,  695,   78,   76,
 /*   600 */   562,  558,  556,  555,  554,  551,  525,  257,   81,   85,
 /*   610 */    29,   55,  596,  595,   89,   91,  592,  546,  544,  536,
 /*   620 */   542,  538,  540,  534,  532,  564,  563,  561,  560,  559,
 /*   630 */   557,  553,  552,   56,  523,  493,  491,  734,  733,  733,
 /*   640 */   733,  733,  733,  733,  733,  733,  733,  733,  733,  116,
 /*   650 */   117,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   218,    1,  210,  211,    1,  270,  212,  213,  213,    9,
 /*    10 */   230,  213,    9,   13,   14,  280,   16,   17,  270,  217,
 /*    20 */    20,   21,  220,   23,   24,   25,   26,   27,   28,  247,
 /*    30 */   248,  249,  250,   33,   34,  255,  213,   37,   38,   39,
 /*    40 */    13,   14,  253,   16,   17,  253,  251,   20,   21,  254,
 /*    50 */    23,   24,   25,   26,   27,   28,  267,  213,  230,  267,
 /*    60 */    33,   34,  255,  270,   37,   38,   39,   13,   14,  271,
 /*    70 */    16,   17,  279,  280,   20,   21,  270,   23,   24,   25,
 /*    80 */    26,   27,   28,  255,  213,  279,  280,   33,   34,   77,
 /*    90 */     1,   37,   38,   39,   63,   64,   65,  274,    9,  276,
 /*   100 */   218,   13,   14,  213,   16,   17,  230,  107,   20,   21,
 /*   110 */   213,   23,   24,   25,   26,   27,   28,   37,   38,   39,
 /*   120 */   276,   33,   34,   79,  218,   37,   38,   39,  213,  102,
 /*   130 */   230,  255,  250,  213,   45,   46,   47,   48,   49,   50,
 /*   140 */    51,   52,   53,   54,   55,   56,   57,  276,  251,   60,
 /*   150 */    14,  254,   16,   17,  248,  255,   20,   21,   66,   23,
 /*   160 */    24,   25,   26,   27,   28,  245,  276,  252,  278,   33,
 /*   170 */    34,   74,  270,   37,   38,   39,   79,   86,   87,   88,
 /*   180 */    89,   90,   91,   92,   93,   94,   95,   96,   97,   98,
 /*   190 */    99,  100,  229,    5,  231,  232,  233,  234,  235,  236,
 /*   200 */   237,  238,  239,  240,  241,  242,  243,  244,   16,   17,
 /*   210 */   133,  134,   20,   21,  213,   23,   24,   25,   26,   27,
 /*   220 */    28,   33,   34,   44,  132,   33,   34,  135,  136,   37,
 /*   230 */    38,   39,    1,    2,  213,   37,    5,   66,    7,   60,
 /*   240 */     9,  101,   33,   34,  256,   66,   37,   38,   39,  109,
 /*   250 */    71,   72,   73,    1,    2,  254,  268,    5,  213,    7,
 /*   260 */    86,    9,   88,   89,   33,   34,  270,   93,   37,   95,
 /*   270 */    96,   97,  251,   99,  100,  254,    2,  213,  106,    5,
 /*   280 */   101,    7,  106,    9,  131,   33,   34,  213,  109,  113,
 /*   290 */   229,  138,  139,  232,  233,    0,  251,  125,  237,  254,
 /*   300 */   239,  240,  241,  132,  243,  244,  135,   33,   34,  130,
 /*   310 */   112,   25,   26,   27,   28,  251,  137,  213,  254,   33,
 /*   320 */    34,   61,   62,   37,   38,   39,  213,   67,   68,   69,
 /*   330 */    70,  101,  101,   61,   62,  105,   76,  107,    1,   67,
 /*   340 */    68,   69,   70,   61,   62,  114,  272,  120,  121,   67,
 /*   350 */    68,   69,   70,  101,  217,  251,  217,  220,  254,  220,
 /*   360 */   129,  102,  101,    5,  251,    7,  114,  254,   59,  110,
 /*   370 */   109,  102,   77,  102,   37,  106,    5,  106,    7,  102,
 /*   380 */   102,  129,  108,  106,  106,  102,  102,   15,  270,  106,
 /*   390 */   106,  106,  102,  102,  102,  102,  106,  106,  106,  106,
 /*   400 */   102,  101,  101,  103,  106,  104,  133,  134,  133,  134,
 /*   410 */   101,  270,  127,  133,  134,    5,    5,    7,    7,   74,
 /*   420 */    75,   61,   62,  246,  270,  270,  270,  270,  270,  270,
 /*   430 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  255,
 /*   440 */   246,  246,  246,  246,  253,  246,  246,  213,  213,  269,
 /*   450 */   213,  213,  213,  213,  213,  277,  213,  213,  277,  257,
 /*   460 */   213,  213,  213,  213,  213,  109,  213,  213,  213,  213,
 /*   470 */   213,  213,  253,   59,  273,  213,  213,  213,  213,  213,
 /*   480 */   108,  213,  114,  213,  273,  273,  124,  266,  265,  213,
 /*   490 */   126,  118,  213,  123,  213,  262,  264,  122,  117,  263,
 /*   500 */   213,  213,  213,  261,  116,  260,  115,  213,  128,  213,
 /*   510 */   213,   85,  213,   84,   49,  213,  213,  213,  213,  213,
 /*   520 */   213,  213,   81,  213,  213,  213,  213,  213,  213,  213,
 /*   530 */   213,   83,  214,  214,  214,  214,   53,   82,   80,  214,
 /*   540 */   214,  214,    5,  214,  214,   77,  218,  140,    5,  214,
 /*   550 */     5,    5,  140,  215,    5,  214,  222,  214,  226,  228,
 /*   560 */   227,  225,  223,  221,  224,  215,  215,  214,  219,   87,
 /*   570 */   216,  131,  110,  102,  111,  101,  253,  104,  106,  102,
 /*   580 */   101,  259,  258,    1,  106,  101,  119,  102,  101,  119,
 /*   590 */   102,  101,  106,  101,  101,  104,  101,  108,   74,  105,
 /*   600 */     9,    5,    5,    5,    5,    5,   78,   15,   74,  134,
 /*   610 */   106,   16,    5,    5,  134,  134,  102,    5,    5,    5,
 /*   620 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   630 */     5,    5,    5,  106,   78,   59,   58,    0,  281,  281,
 /*   640 */   281,  281,  281,  281,  281,  281,  281,  281,  281,   21,
 /*   650 */    21,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   660 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   670 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   680 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   690 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   700 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   710 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   720 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   730 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   740 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   750 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   760 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   770 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   780 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   790 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   800 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   810 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   820 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   830 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   840 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   850 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   860 */   281,
};
#define YY_SHIFT_COUNT    (293)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (637)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   179,   91,  174,   12,  231,  252,    3,    3,    3,    3,
 /*    10 */     3,    3,    3,    3,    3,    0,   89,  252,  274,  274,
 /*    20 */   274,  274,  140,    3,    3,    3,    3,  295,    3,    3,
 /*    30 */    97,   12,   44,   44,  651,  252,  252,  252,  252,  252,
 /*    40 */   252,  252,  252,  252,  252,  252,  252,  252,  252,  252,
 /*    50 */   252,  252,  252,  252,  252,  274,  274,  188,  188,  188,
 /*    60 */   188,  188,  188,  188,  261,    3,    3,  198,    3,    3,
 /*    70 */     3,  227,  227,  176,    3,    3,    3,    3,    3,    3,
 /*    80 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    90 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   100 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   110 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   120 */     3,  356,  414,  414,  414,  368,  368,  368,  414,  362,
 /*   130 */   364,  370,  373,  375,  381,  388,  391,  380,  356,  414,
 /*   140 */   414,  414,   12,  414,  414,  426,  429,  465,  441,  448,
 /*   150 */   483,  455,  458,  414,  468,  414,  468,  414,  468,  414,
 /*   160 */   651,  651,   27,   54,   88,   54,   54,  136,  192,  286,
 /*   170 */   286,  286,  286,  260,  272,  282,  209,  209,  209,  209,
 /*   180 */    92,  153,   80,   80,  230,  171,   31,  259,  269,  271,
 /*   190 */   277,  278,  283,  284,  358,  371,  337,  309,  372,  285,
 /*   200 */   172,  290,  291,  292,  293,  298,  301,   77,  273,  275,
 /*   210 */   300,  280,  410,  411,  345,  360,  537,  407,  543,  545,
 /*   220 */   412,  546,  549,  482,  440,  462,  471,  463,  473,  474,
 /*   230 */   472,  477,  479,  582,  484,  485,  487,  478,  467,  486,
 /*   240 */   470,  488,  490,  489,  492,  473,  493,  491,  495,  494,
 /*   250 */   524,  591,  596,  597,  598,  599,  600,  528,  592,  534,
 /*   260 */   475,  504,  504,  595,  480,  481,  504,  607,  608,  514,
 /*   270 */   504,  612,  613,  614,  615,  616,  617,  618,  619,  620,
 /*   280 */   621,  622,  623,  624,  625,  626,  627,  527,  556,  628,
 /*   290 */   629,  576,  578,  637,
};
#define YY_REDUCE_COUNT (161)
#define YY_REDUCE_MIN   (-265)
#define YY_REDUCE_MAX   (354)
static const short yy_reduce_ofst[] = {
 /*     0 */  -208,  -37,   61, -218, -207, -194, -205, -110, -177, -103,
 /*    10 */    21,   45,   64,  104,  113, -202, -206, -265, -220, -172,
 /*    20 */  -124, -100, -211,   74, -156, -129,  -85, -118,  -80,    1,
 /*    30 */  -198,  -94,  137,  139,  -12, -252,  -98,   -4,  118,  141,
 /*    40 */   154,  155,  156,  157,  158,  159,  160,  161,  162,  163,
 /*    50 */   164,  165,  166,  167,  168, -193,  184,  177,  194,  195,
 /*    60 */   196,  197,  199,  200,  191,  234,  235,  180,  237,  238,
 /*    70 */   239,  178,  181,  202,  240,  241,  243,  244,  247,  248,
 /*    80 */   249,  250,  251,  253,  254,  255,  256,  257,  258,  262,
 /*    90 */   263,  264,  265,  266,  268,  270,  276,  279,  281,  287,
 /*   100 */   288,  289,  294,  296,  297,  299,  302,  303,  304,  305,
 /*   110 */   306,  307,  308,  310,  311,  312,  313,  314,  315,  316,
 /*   120 */   317,  219,  318,  319,  320,  201,  211,  212,  321,  221,
 /*   130 */   223,  232,  236,  233,  242,  245,  322,  324,  323,  325,
 /*   140 */   326,  327,  328,  329,  330,  331,  333,  332,  334,  336,
 /*   150 */   339,  340,  342,  335,  338,  341,  350,  343,  351,  353,
 /*   160 */   349,  354,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   731,  786,  775,  783,  972,  972,  731,  731,  731,  731,
 /*    10 */   731,  731,  731,  731,  731,  894,  749,  972,  731,  731,
 /*    20 */   731,  731,  731,  731,  731,  731,  731,  783,  731,  731,
 /*    30 */   788,  783,  788,  788,  889,  731,  731,  731,  731,  731,
 /*    40 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*    50 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*    60 */   731,  731,  731,  731,  731,  731,  731,  896,  898,  900,
 /*    70 */   731,  918,  918,  887,  731,  731,  731,  731,  731,  731,
 /*    80 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*    90 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  773,
 /*   100 */   731,  771,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   110 */   731,  731,  731,  731,  731,  759,  731,  731,  731,  731,
 /*   120 */   731,  731,  751,  751,  751,  731,  731,  731,  751,  925,
 /*   130 */   929,  923,  911,  919,  910,  906,  905,  933,  731,  751,
 /*   140 */   751,  751,  783,  751,  751,  804,  802,  800,  792,  798,
 /*   150 */   794,  796,  790,  751,  781,  751,  781,  751,  781,  751,
 /*   160 */   822,  838,  731,  934,  731,  971,  924,  961,  960,  967,
 /*   170 */   959,  958,  957,  731,  731,  731,  953,  954,  956,  955,
 /*   180 */   731,  731,  963,  962,  731,  731,  731,  731,  731,  731,
 /*   190 */   731,  731,  731,  731,  731,  731,  731,  936,  731,  930,
 /*   200 */   926,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   210 */   848,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   220 */   731,  731,  731,  731,  731,  886,  731,  731,  731,  731,
 /*   230 */   897,  731,  731,  731,  731,  731,  731,  920,  731,  912,
 /*   240 */   731,  731,  731,  731,  731,  860,  731,  731,  731,  731,
 /*   250 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   260 */   731,  982,  980,  731,  731,  731,  976,  731,  731,  731,
 /*   270 */   974,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   280 */   731,  731,  731,  731,  731,  731,  731,  807,  731,  757,
 /*   290 */   755,  731,  747,  731,
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
    0,  /*          $ => nothing */
    0,  /*         ID => nothing */
    1,  /*       BOOL => ID */
    1,  /*    TINYINT => ID */
    1,  /*   SMALLINT => ID */
    1,  /*    INTEGER => ID */
    1,  /*     BIGINT => ID */
    1,  /*      FLOAT => ID */
    1,  /*     DOUBLE => ID */
    1,  /*     STRING => ID */
    1,  /*  TIMESTAMP => ID */
    1,  /*     BINARY => ID */
    1,  /*      NCHAR => ID */
    0,  /*         OR => nothing */
    0,  /*        AND => nothing */
    0,  /*        NOT => nothing */
    0,  /*         EQ => nothing */
    0,  /*         NE => nothing */
    0,  /*     ISNULL => nothing */
    0,  /*    NOTNULL => nothing */
    0,  /*         IS => nothing */
    1,  /*       LIKE => ID */
    1,  /*       GLOB => ID */
    0,  /*    BETWEEN => nothing */
    0,  /*         IN => nothing */
    0,  /*         GT => nothing */
    0,  /*         GE => nothing */
    0,  /*         LT => nothing */
    0,  /*         LE => nothing */
    0,  /*     BITAND => nothing */
    0,  /*      BITOR => nothing */
    0,  /*     LSHIFT => nothing */
    0,  /*     RSHIFT => nothing */
    0,  /*       PLUS => nothing */
    0,  /*      MINUS => nothing */
    0,  /*     DIVIDE => nothing */
    0,  /*      TIMES => nothing */
    0,  /*       STAR => nothing */
    0,  /*      SLASH => nothing */
    0,  /*        REM => nothing */
    0,  /*     CONCAT => nothing */
    0,  /*     UMINUS => nothing */
    0,  /*      UPLUS => nothing */
    0,  /*     BITNOT => nothing */
    0,  /*       SHOW => nothing */
    0,  /*  DATABASES => nothing */
    0,  /*     MNODES => nothing */
    0,  /*     DNODES => nothing */
    0,  /*   ACCOUNTS => nothing */
    0,  /*      USERS => nothing */
    0,  /*    MODULES => nothing */
    0,  /*    QUERIES => nothing */
    0,  /* CONNECTIONS => nothing */
    0,  /*    STREAMS => nothing */
    0,  /*  VARIABLES => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*     VNODES => nothing */
    1,  /*    IPTOKEN => ID */
    0,  /*        DOT => nothing */
    0,  /*     CREATE => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    1,  /*     STABLE => ID */
    0,  /*      DNODE => nothing */
    0,  /*       USER => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*       KEEP => nothing */
    0,  /*      CACHE => nothing */
    0,  /*    REPLICA => nothing */
    0,  /*     QUORUM => nothing */
    0,  /*       DAYS => nothing */
    0,  /*    MINROWS => nothing */
    0,  /*    MAXROWS => nothing */
    0,  /*     BLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*        WAL => nothing */
    0,  /*      FSYNC => nothing */
    0,  /*       COMP => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*     UPDATE => nothing */
    0,  /*  CACHELAST => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*      COMMA => nothing */
    0,  /*         AS => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    1,  /*       DESC => ID */
    0,  /*      GROUP => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      LIMIT => nothing */
    1,  /*     OFFSET => ID */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      WHERE => nothing */
    1,  /*        NOW => ID */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*        TAG => nothing */
    0,  /*     CHANGE => nothing */
    0,  /*        SET => nothing */
    0,  /*       KILL => nothing */
    0,  /* CONNECTION => nothing */
    0,  /*     STREAM => nothing */
    0,  /*      COLON => nothing */
    1,  /*      ABORT => ID */
    1,  /*      AFTER => ID */
    1,  /*     ATTACH => ID */
    1,  /*     BEFORE => ID */
    1,  /*      BEGIN => ID */
    1,  /*    CASCADE => ID */
    1,  /*    CLUSTER => ID */
    1,  /*   CONFLICT => ID */
    1,  /*       COPY => ID */
    1,  /*   DEFERRED => ID */
    1,  /* DELIMITERS => ID */
    1,  /*     DETACH => ID */
    1,  /*       EACH => ID */
    1,  /*        END => ID */
    1,  /*    EXPLAIN => ID */
    1,  /*       FAIL => ID */
    1,  /*        FOR => ID */
    1,  /*     IGNORE => ID */
    1,  /*  IMMEDIATE => ID */
    1,  /*  INITIALLY => ID */
    1,  /*    INSTEAD => ID */
    1,  /*      MATCH => ID */
    1,  /*        KEY => ID */
    1,  /*         OF => ID */
    1,  /*      RAISE => ID */
    1,  /*    REPLACE => ID */
    1,  /*   RESTRICT => ID */
    1,  /*        ROW => ID */
    1,  /*  STATEMENT => ID */
    1,  /*    TRIGGER => ID */
    1,  /*       VIEW => ID */
    1,  /*      COUNT => ID */
    1,  /*        SUM => ID */
    1,  /*        AVG => ID */
    1,  /*        MIN => ID */
    1,  /*        MAX => ID */
    1,  /*      FIRST => ID */
    1,  /*       LAST => ID */
    1,  /*        TOP => ID */
    1,  /*     BOTTOM => ID */
    1,  /*     STDDEV => ID */
    1,  /* PERCENTILE => ID */
    1,  /* APERCENTILE => ID */
    1,  /* LEASTSQUARES => ID */
    1,  /*  HISTOGRAM => ID */
    1,  /*       DIFF => ID */
    1,  /*     SPREAD => ID */
    1,  /*        TWA => ID */
    1,  /*     INTERP => ID */
    1,  /*   LAST_ROW => ID */
    1,  /*       RATE => ID */
    1,  /*      IRATE => ID */
    1,  /*   SUM_RATE => ID */
    1,  /*  SUM_IRATE => ID */
    1,  /*   AVG_RATE => ID */
    1,  /*  AVG_IRATE => ID */
    1,  /*       TBID => ID */
    1,  /*       SEMI => ID */
    1,  /*       NONE => ID */
    1,  /*       PREV => ID */
    1,  /*     LINEAR => ID */
    1,  /*     IMPORT => ID */
    1,  /*     METRIC => ID */
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
    1,  /*    METRICS => ID */
    1,  /*     INSERT => ID */
    1,  /*       INTO => ID */
    1,  /*     VALUES => ID */
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
  ParseARG_SDECL                /* A place to hold %extra_argument */
  ParseCTX_SDECL                /* A place to hold %extra_context */
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
void ParseTrace(FILE *TraceFILE, char *zTracePrompt){
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
  /*    1 */ "ID",
  /*    2 */ "BOOL",
  /*    3 */ "TINYINT",
  /*    4 */ "SMALLINT",
  /*    5 */ "INTEGER",
  /*    6 */ "BIGINT",
  /*    7 */ "FLOAT",
  /*    8 */ "DOUBLE",
  /*    9 */ "STRING",
  /*   10 */ "TIMESTAMP",
  /*   11 */ "BINARY",
  /*   12 */ "NCHAR",
  /*   13 */ "OR",
  /*   14 */ "AND",
  /*   15 */ "NOT",
  /*   16 */ "EQ",
  /*   17 */ "NE",
  /*   18 */ "ISNULL",
  /*   19 */ "NOTNULL",
  /*   20 */ "IS",
  /*   21 */ "LIKE",
  /*   22 */ "GLOB",
  /*   23 */ "BETWEEN",
  /*   24 */ "IN",
  /*   25 */ "GT",
  /*   26 */ "GE",
  /*   27 */ "LT",
  /*   28 */ "LE",
  /*   29 */ "BITAND",
  /*   30 */ "BITOR",
  /*   31 */ "LSHIFT",
  /*   32 */ "RSHIFT",
  /*   33 */ "PLUS",
  /*   34 */ "MINUS",
  /*   35 */ "DIVIDE",
  /*   36 */ "TIMES",
  /*   37 */ "STAR",
  /*   38 */ "SLASH",
  /*   39 */ "REM",
  /*   40 */ "CONCAT",
  /*   41 */ "UMINUS",
  /*   42 */ "UPLUS",
  /*   43 */ "BITNOT",
  /*   44 */ "SHOW",
  /*   45 */ "DATABASES",
  /*   46 */ "MNODES",
  /*   47 */ "DNODES",
  /*   48 */ "ACCOUNTS",
  /*   49 */ "USERS",
  /*   50 */ "MODULES",
  /*   51 */ "QUERIES",
  /*   52 */ "CONNECTIONS",
  /*   53 */ "STREAMS",
  /*   54 */ "VARIABLES",
  /*   55 */ "SCORES",
  /*   56 */ "GRANTS",
  /*   57 */ "VNODES",
  /*   58 */ "IPTOKEN",
  /*   59 */ "DOT",
  /*   60 */ "CREATE",
  /*   61 */ "TABLE",
  /*   62 */ "DATABASE",
  /*   63 */ "TABLES",
  /*   64 */ "STABLES",
  /*   65 */ "VGROUPS",
  /*   66 */ "DROP",
  /*   67 */ "STABLE",
  /*   68 */ "DNODE",
  /*   69 */ "USER",
  /*   70 */ "ACCOUNT",
  /*   71 */ "USE",
  /*   72 */ "DESCRIBE",
  /*   73 */ "ALTER",
  /*   74 */ "PASS",
  /*   75 */ "PRIVILEGE",
  /*   76 */ "LOCAL",
  /*   77 */ "IF",
  /*   78 */ "EXISTS",
  /*   79 */ "PPS",
  /*   80 */ "TSERIES",
  /*   81 */ "DBS",
  /*   82 */ "STORAGE",
  /*   83 */ "QTIME",
  /*   84 */ "CONNS",
  /*   85 */ "STATE",
  /*   86 */ "KEEP",
  /*   87 */ "CACHE",
  /*   88 */ "REPLICA",
  /*   89 */ "QUORUM",
  /*   90 */ "DAYS",
  /*   91 */ "MINROWS",
  /*   92 */ "MAXROWS",
  /*   93 */ "BLOCKS",
  /*   94 */ "CTIME",
  /*   95 */ "WAL",
  /*   96 */ "FSYNC",
  /*   97 */ "COMP",
  /*   98 */ "PRECISION",
  /*   99 */ "UPDATE",
  /*  100 */ "CACHELAST",
  /*  101 */ "LP",
  /*  102 */ "RP",
  /*  103 */ "UNSIGNED",
  /*  104 */ "TAGS",
  /*  105 */ "USING",
  /*  106 */ "COMMA",
  /*  107 */ "AS",
  /*  108 */ "NULL",
  /*  109 */ "SELECT",
  /*  110 */ "UNION",
  /*  111 */ "ALL",
  /*  112 */ "DISTINCT",
  /*  113 */ "FROM",
  /*  114 */ "VARIABLE",
  /*  115 */ "INTERVAL",
  /*  116 */ "FILL",
  /*  117 */ "SLIDING",
  /*  118 */ "ORDER",
  /*  119 */ "BY",
  /*  120 */ "ASC",
  /*  121 */ "DESC",
  /*  122 */ "GROUP",
  /*  123 */ "HAVING",
  /*  124 */ "LIMIT",
  /*  125 */ "OFFSET",
  /*  126 */ "SLIMIT",
  /*  127 */ "SOFFSET",
  /*  128 */ "WHERE",
  /*  129 */ "NOW",
  /*  130 */ "RESET",
  /*  131 */ "QUERY",
  /*  132 */ "ADD",
  /*  133 */ "COLUMN",
  /*  134 */ "TAG",
  /*  135 */ "CHANGE",
  /*  136 */ "SET",
  /*  137 */ "KILL",
  /*  138 */ "CONNECTION",
  /*  139 */ "STREAM",
  /*  140 */ "COLON",
  /*  141 */ "ABORT",
  /*  142 */ "AFTER",
  /*  143 */ "ATTACH",
  /*  144 */ "BEFORE",
  /*  145 */ "BEGIN",
  /*  146 */ "CASCADE",
  /*  147 */ "CLUSTER",
  /*  148 */ "CONFLICT",
  /*  149 */ "COPY",
  /*  150 */ "DEFERRED",
  /*  151 */ "DELIMITERS",
  /*  152 */ "DETACH",
  /*  153 */ "EACH",
  /*  154 */ "END",
  /*  155 */ "EXPLAIN",
  /*  156 */ "FAIL",
  /*  157 */ "FOR",
  /*  158 */ "IGNORE",
  /*  159 */ "IMMEDIATE",
  /*  160 */ "INITIALLY",
  /*  161 */ "INSTEAD",
  /*  162 */ "MATCH",
  /*  163 */ "KEY",
  /*  164 */ "OF",
  /*  165 */ "RAISE",
  /*  166 */ "REPLACE",
  /*  167 */ "RESTRICT",
  /*  168 */ "ROW",
  /*  169 */ "STATEMENT",
  /*  170 */ "TRIGGER",
  /*  171 */ "VIEW",
  /*  172 */ "COUNT",
  /*  173 */ "SUM",
  /*  174 */ "AVG",
  /*  175 */ "MIN",
  /*  176 */ "MAX",
  /*  177 */ "FIRST",
  /*  178 */ "LAST",
  /*  179 */ "TOP",
  /*  180 */ "BOTTOM",
  /*  181 */ "STDDEV",
  /*  182 */ "PERCENTILE",
  /*  183 */ "APERCENTILE",
  /*  184 */ "LEASTSQUARES",
  /*  185 */ "HISTOGRAM",
  /*  186 */ "DIFF",
  /*  187 */ "SPREAD",
  /*  188 */ "TWA",
  /*  189 */ "INTERP",
  /*  190 */ "LAST_ROW",
  /*  191 */ "RATE",
  /*  192 */ "IRATE",
  /*  193 */ "SUM_RATE",
  /*  194 */ "SUM_IRATE",
  /*  195 */ "AVG_RATE",
  /*  196 */ "AVG_IRATE",
  /*  197 */ "TBID",
  /*  198 */ "SEMI",
  /*  199 */ "NONE",
  /*  200 */ "PREV",
  /*  201 */ "LINEAR",
  /*  202 */ "IMPORT",
  /*  203 */ "METRIC",
  /*  204 */ "TBNAME",
  /*  205 */ "JOIN",
  /*  206 */ "METRICS",
  /*  207 */ "INSERT",
  /*  208 */ "INTO",
  /*  209 */ "VALUES",
  /*  210 */ "program",
  /*  211 */ "cmd",
  /*  212 */ "dbPrefix",
  /*  213 */ "ids",
  /*  214 */ "cpxName",
  /*  215 */ "ifexists",
  /*  216 */ "alter_db_optr",
  /*  217 */ "acct_optr",
  /*  218 */ "ifnotexists",
  /*  219 */ "db_optr",
  /*  220 */ "pps",
  /*  221 */ "tseries",
  /*  222 */ "dbs",
  /*  223 */ "streams",
  /*  224 */ "storage",
  /*  225 */ "qtime",
  /*  226 */ "users",
  /*  227 */ "conns",
  /*  228 */ "state",
  /*  229 */ "keep",
  /*  230 */ "tagitemlist",
  /*  231 */ "cache",
  /*  232 */ "replica",
  /*  233 */ "quorum",
  /*  234 */ "days",
  /*  235 */ "minrows",
  /*  236 */ "maxrows",
  /*  237 */ "blocks",
  /*  238 */ "ctime",
  /*  239 */ "wal",
  /*  240 */ "fsync",
  /*  241 */ "comp",
  /*  242 */ "prec",
  /*  243 */ "update",
  /*  244 */ "cachelast",
  /*  245 */ "typename",
  /*  246 */ "signed",
  /*  247 */ "create_table_args",
  /*  248 */ "create_stable_args",
  /*  249 */ "create_table_list",
  /*  250 */ "create_from_stable",
  /*  251 */ "columnlist",
  /*  252 */ "tagNamelist",
  /*  253 */ "select",
  /*  254 */ "column",
  /*  255 */ "tagitem",
  /*  256 */ "selcollist",
  /*  257 */ "from",
  /*  258 */ "where_opt",
  /*  259 */ "interval_opt",
  /*  260 */ "fill_opt",
  /*  261 */ "sliding_opt",
  /*  262 */ "groupby_opt",
  /*  263 */ "orderby_opt",
  /*  264 */ "having_opt",
  /*  265 */ "slimit_opt",
  /*  266 */ "limit_opt",
  /*  267 */ "union",
  /*  268 */ "sclp",
  /*  269 */ "distinct",
  /*  270 */ "expr",
  /*  271 */ "as",
  /*  272 */ "tablelist",
  /*  273 */ "tmvar",
  /*  274 */ "sortlist",
  /*  275 */ "sortitem",
  /*  276 */ "item",
  /*  277 */ "sortorder",
  /*  278 */ "grouplist",
  /*  279 */ "exprlist",
  /*  280 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW MNODES",
 /*   3 */ "cmd ::= SHOW DNODES",
 /*   4 */ "cmd ::= SHOW ACCOUNTS",
 /*   5 */ "cmd ::= SHOW USERS",
 /*   6 */ "cmd ::= SHOW MODULES",
 /*   7 */ "cmd ::= SHOW QUERIES",
 /*   8 */ "cmd ::= SHOW CONNECTIONS",
 /*   9 */ "cmd ::= SHOW STREAMS",
 /*  10 */ "cmd ::= SHOW VARIABLES",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "cmd ::= SHOW VNODES",
 /*  14 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  15 */ "dbPrefix ::=",
 /*  16 */ "dbPrefix ::= ids DOT",
 /*  17 */ "cpxName ::=",
 /*  18 */ "cpxName ::= DOT ids",
 /*  19 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  20 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  27 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  28 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  29 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  30 */ "cmd ::= DROP DNODE ids",
 /*  31 */ "cmd ::= DROP USER ids",
 /*  32 */ "cmd ::= DROP ACCOUNT ids",
 /*  33 */ "cmd ::= USE ids",
 /*  34 */ "cmd ::= DESCRIBE ids cpxName",
 /*  35 */ "cmd ::= ALTER USER ids PASS ids",
 /*  36 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  37 */ "cmd ::= ALTER DNODE ids ids",
 /*  38 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  39 */ "cmd ::= ALTER LOCAL ids",
 /*  40 */ "cmd ::= ALTER LOCAL ids ids",
 /*  41 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  42 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  43 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  44 */ "ids ::= ID",
 /*  45 */ "ids ::= STRING",
 /*  46 */ "ifexists ::= IF EXISTS",
 /*  47 */ "ifexists ::=",
 /*  48 */ "ifnotexists ::= IF NOT EXISTS",
 /*  49 */ "ifnotexists ::=",
 /*  50 */ "cmd ::= CREATE DNODE ids",
 /*  51 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  52 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  53 */ "cmd ::= CREATE USER ids PASS ids",
 /*  54 */ "pps ::=",
 /*  55 */ "pps ::= PPS INTEGER",
 /*  56 */ "tseries ::=",
 /*  57 */ "tseries ::= TSERIES INTEGER",
 /*  58 */ "dbs ::=",
 /*  59 */ "dbs ::= DBS INTEGER",
 /*  60 */ "streams ::=",
 /*  61 */ "streams ::= STREAMS INTEGER",
 /*  62 */ "storage ::=",
 /*  63 */ "storage ::= STORAGE INTEGER",
 /*  64 */ "qtime ::=",
 /*  65 */ "qtime ::= QTIME INTEGER",
 /*  66 */ "users ::=",
 /*  67 */ "users ::= USERS INTEGER",
 /*  68 */ "conns ::=",
 /*  69 */ "conns ::= CONNS INTEGER",
 /*  70 */ "state ::=",
 /*  71 */ "state ::= STATE ids",
 /*  72 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  73 */ "keep ::= KEEP tagitemlist",
 /*  74 */ "cache ::= CACHE INTEGER",
 /*  75 */ "replica ::= REPLICA INTEGER",
 /*  76 */ "quorum ::= QUORUM INTEGER",
 /*  77 */ "days ::= DAYS INTEGER",
 /*  78 */ "minrows ::= MINROWS INTEGER",
 /*  79 */ "maxrows ::= MAXROWS INTEGER",
 /*  80 */ "blocks ::= BLOCKS INTEGER",
 /*  81 */ "ctime ::= CTIME INTEGER",
 /*  82 */ "wal ::= WAL INTEGER",
 /*  83 */ "fsync ::= FSYNC INTEGER",
 /*  84 */ "comp ::= COMP INTEGER",
 /*  85 */ "prec ::= PRECISION STRING",
 /*  86 */ "update ::= UPDATE INTEGER",
 /*  87 */ "cachelast ::= CACHELAST INTEGER",
 /*  88 */ "db_optr ::=",
 /*  89 */ "db_optr ::= db_optr cache",
 /*  90 */ "db_optr ::= db_optr replica",
 /*  91 */ "db_optr ::= db_optr quorum",
 /*  92 */ "db_optr ::= db_optr days",
 /*  93 */ "db_optr ::= db_optr minrows",
 /*  94 */ "db_optr ::= db_optr maxrows",
 /*  95 */ "db_optr ::= db_optr blocks",
 /*  96 */ "db_optr ::= db_optr ctime",
 /*  97 */ "db_optr ::= db_optr wal",
 /*  98 */ "db_optr ::= db_optr fsync",
 /*  99 */ "db_optr ::= db_optr comp",
 /* 100 */ "db_optr ::= db_optr prec",
 /* 101 */ "db_optr ::= db_optr keep",
 /* 102 */ "db_optr ::= db_optr update",
 /* 103 */ "db_optr ::= db_optr cachelast",
 /* 104 */ "alter_db_optr ::=",
 /* 105 */ "alter_db_optr ::= alter_db_optr replica",
 /* 106 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 107 */ "alter_db_optr ::= alter_db_optr keep",
 /* 108 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 109 */ "alter_db_optr ::= alter_db_optr comp",
 /* 110 */ "alter_db_optr ::= alter_db_optr wal",
 /* 111 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 112 */ "alter_db_optr ::= alter_db_optr update",
 /* 113 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 114 */ "typename ::= ids",
 /* 115 */ "typename ::= ids LP signed RP",
 /* 116 */ "typename ::= ids UNSIGNED",
 /* 117 */ "signed ::= INTEGER",
 /* 118 */ "signed ::= PLUS INTEGER",
 /* 119 */ "signed ::= MINUS INTEGER",
 /* 120 */ "cmd ::= CREATE TABLE create_table_args",
 /* 121 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 122 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 123 */ "cmd ::= CREATE TABLE create_table_list",
 /* 124 */ "create_table_list ::= create_from_stable",
 /* 125 */ "create_table_list ::= create_table_list create_from_stable",
 /* 126 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 127 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 128 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 129 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 130 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 131 */ "tagNamelist ::= ids",
 /* 132 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 133 */ "columnlist ::= columnlist COMMA column",
 /* 134 */ "columnlist ::= column",
 /* 135 */ "column ::= ids typename",
 /* 136 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 137 */ "tagitemlist ::= tagitem",
 /* 138 */ "tagitem ::= INTEGER",
 /* 139 */ "tagitem ::= FLOAT",
 /* 140 */ "tagitem ::= STRING",
 /* 141 */ "tagitem ::= BOOL",
 /* 142 */ "tagitem ::= NULL",
 /* 143 */ "tagitem ::= MINUS INTEGER",
 /* 144 */ "tagitem ::= MINUS FLOAT",
 /* 145 */ "tagitem ::= PLUS INTEGER",
 /* 146 */ "tagitem ::= PLUS FLOAT",
 /* 147 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 148 */ "union ::= select",
 /* 149 */ "union ::= LP union RP",
 /* 150 */ "union ::= union UNION ALL select",
 /* 151 */ "union ::= union UNION ALL LP select RP",
 /* 152 */ "cmd ::= union",
 /* 153 */ "select ::= SELECT selcollist",
 /* 154 */ "sclp ::= selcollist COMMA",
 /* 155 */ "sclp ::=",
 /* 156 */ "selcollist ::= sclp distinct expr as",
 /* 157 */ "selcollist ::= sclp STAR",
 /* 158 */ "as ::= AS ids",
 /* 159 */ "as ::= ids",
 /* 160 */ "as ::=",
 /* 161 */ "distinct ::= DISTINCT",
 /* 162 */ "distinct ::=",
 /* 163 */ "from ::= FROM tablelist",
 /* 164 */ "tablelist ::= ids cpxName",
 /* 165 */ "tablelist ::= ids cpxName ids",
 /* 166 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 167 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 168 */ "tmvar ::= VARIABLE",
 /* 169 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 170 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 171 */ "interval_opt ::=",
 /* 172 */ "fill_opt ::=",
 /* 173 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 174 */ "fill_opt ::= FILL LP ID RP",
 /* 175 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 176 */ "sliding_opt ::=",
 /* 177 */ "orderby_opt ::=",
 /* 178 */ "orderby_opt ::= ORDER BY sortlist",
 /* 179 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 180 */ "sortlist ::= item sortorder",
 /* 181 */ "item ::= ids cpxName",
 /* 182 */ "sortorder ::= ASC",
 /* 183 */ "sortorder ::= DESC",
 /* 184 */ "sortorder ::=",
 /* 185 */ "groupby_opt ::=",
 /* 186 */ "groupby_opt ::= GROUP BY grouplist",
 /* 187 */ "grouplist ::= grouplist COMMA item",
 /* 188 */ "grouplist ::= item",
 /* 189 */ "having_opt ::=",
 /* 190 */ "having_opt ::= HAVING expr",
 /* 191 */ "limit_opt ::=",
 /* 192 */ "limit_opt ::= LIMIT signed",
 /* 193 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 194 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 195 */ "slimit_opt ::=",
 /* 196 */ "slimit_opt ::= SLIMIT signed",
 /* 197 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 198 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 199 */ "where_opt ::=",
 /* 200 */ "where_opt ::= WHERE expr",
 /* 201 */ "expr ::= LP expr RP",
 /* 202 */ "expr ::= ID",
 /* 203 */ "expr ::= ID DOT ID",
 /* 204 */ "expr ::= ID DOT STAR",
 /* 205 */ "expr ::= INTEGER",
 /* 206 */ "expr ::= MINUS INTEGER",
 /* 207 */ "expr ::= PLUS INTEGER",
 /* 208 */ "expr ::= FLOAT",
 /* 209 */ "expr ::= MINUS FLOAT",
 /* 210 */ "expr ::= PLUS FLOAT",
 /* 211 */ "expr ::= STRING",
 /* 212 */ "expr ::= NOW",
 /* 213 */ "expr ::= VARIABLE",
 /* 214 */ "expr ::= BOOL",
 /* 215 */ "expr ::= ID LP exprlist RP",
 /* 216 */ "expr ::= ID LP STAR RP",
 /* 217 */ "expr ::= expr IS NULL",
 /* 218 */ "expr ::= expr IS NOT NULL",
 /* 219 */ "expr ::= expr LT expr",
 /* 220 */ "expr ::= expr GT expr",
 /* 221 */ "expr ::= expr LE expr",
 /* 222 */ "expr ::= expr GE expr",
 /* 223 */ "expr ::= expr NE expr",
 /* 224 */ "expr ::= expr EQ expr",
 /* 225 */ "expr ::= expr BETWEEN expr AND expr",
 /* 226 */ "expr ::= expr AND expr",
 /* 227 */ "expr ::= expr OR expr",
 /* 228 */ "expr ::= expr PLUS expr",
 /* 229 */ "expr ::= expr MINUS expr",
 /* 230 */ "expr ::= expr STAR expr",
 /* 231 */ "expr ::= expr SLASH expr",
 /* 232 */ "expr ::= expr REM expr",
 /* 233 */ "expr ::= expr LIKE expr",
 /* 234 */ "expr ::= expr IN LP exprlist RP",
 /* 235 */ "exprlist ::= exprlist COMMA expritem",
 /* 236 */ "exprlist ::= expritem",
 /* 237 */ "expritem ::= expr",
 /* 238 */ "expritem ::=",
 /* 239 */ "cmd ::= RESET QUERY CACHE",
 /* 240 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 241 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 242 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 243 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 244 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 245 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 246 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 247 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 248 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 249 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 250 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 251 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 252 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 253 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
** second argument to ParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void ParseInit(void *yypRawParser ParseCTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  ParseCTX_STORE
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

#ifndef Parse_ENGINEALWAYSONSTACK
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
** to Parse and ParseFree.
*/
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE) ParseCTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    ParseCTX_STORE
    ParseInit(yypParser ParseCTX_PARAM);
  }
  return (void*)yypParser;
}
#endif /* Parse_ENGINEALWAYSONSTACK */


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
  ParseARG_FETCH
  ParseCTX_FETCH
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
    case 229: /* keep */
    case 230: /* tagitemlist */
    case 251: /* columnlist */
    case 252: /* tagNamelist */
    case 260: /* fill_opt */
    case 262: /* groupby_opt */
    case 263: /* orderby_opt */
    case 274: /* sortlist */
    case 278: /* grouplist */
{
taosArrayDestroy((yypminor->yy247));
}
      break;
    case 249: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy358));
}
      break;
    case 253: /* select */
{
doDestroyQuerySql((yypminor->yy114));
}
      break;
    case 256: /* selcollist */
    case 268: /* sclp */
    case 279: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy522));
}
      break;
    case 258: /* where_opt */
    case 264: /* having_opt */
    case 270: /* expr */
    case 280: /* expritem */
{
tSqlExprDestroy((yypminor->yy326));
}
      break;
    case 267: /* union */
{
destroyAllSelectClause((yypminor->yy219));
}
      break;
    case 275: /* sortitem */
{
tVariantDestroy(&(yypminor->yy378));
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
void ParseFinalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef Parse_ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void ParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  ParseFinalize(p);
  (*freeProc)(p);
}
#endif /* Parse_ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p){
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
int ParseCoverage(FILE *out){
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
    assert( i<=YY_ACTTAB_COUNT );
    assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD );
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    assert( i<(int)YY_NLOOKAHEAD );
    if( yy_lookahead[i]!=iLookAhead ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      assert( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0]) );
      iFallback = yyFallback[iLookAhead];
      if( iFallback!=0 ){
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
        assert( j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) );
        if( yy_lookahead[j]==YYWILDCARD && iLookAhead>0 ){
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
      assert( i>=0 && i<sizeof(yy_action)/sizeof(yy_action[0]) );
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
   ParseARG_FETCH
   ParseCTX_FETCH
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
   ParseARG_STORE /* Suppress warning about unused %extra_argument var */
   ParseCTX_STORE
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
  ParseTOKENTYPE yyMinor        /* The minor token to shift in */
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

/* For rule J, yyRuleInfoLhs[J] contains the symbol on the left-hand side
** of that rule */
static const YYCODETYPE yyRuleInfoLhs[] = {
   210,  /* (0) program ::= cmd */
   211,  /* (1) cmd ::= SHOW DATABASES */
   211,  /* (2) cmd ::= SHOW MNODES */
   211,  /* (3) cmd ::= SHOW DNODES */
   211,  /* (4) cmd ::= SHOW ACCOUNTS */
   211,  /* (5) cmd ::= SHOW USERS */
   211,  /* (6) cmd ::= SHOW MODULES */
   211,  /* (7) cmd ::= SHOW QUERIES */
   211,  /* (8) cmd ::= SHOW CONNECTIONS */
   211,  /* (9) cmd ::= SHOW STREAMS */
   211,  /* (10) cmd ::= SHOW VARIABLES */
   211,  /* (11) cmd ::= SHOW SCORES */
   211,  /* (12) cmd ::= SHOW GRANTS */
   211,  /* (13) cmd ::= SHOW VNODES */
   211,  /* (14) cmd ::= SHOW VNODES IPTOKEN */
   212,  /* (15) dbPrefix ::= */
   212,  /* (16) dbPrefix ::= ids DOT */
   214,  /* (17) cpxName ::= */
   214,  /* (18) cpxName ::= DOT ids */
   211,  /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
   211,  /* (20) cmd ::= SHOW CREATE DATABASE ids */
   211,  /* (21) cmd ::= SHOW dbPrefix TABLES */
   211,  /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   211,  /* (23) cmd ::= SHOW dbPrefix STABLES */
   211,  /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   211,  /* (25) cmd ::= SHOW dbPrefix VGROUPS */
   211,  /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
   211,  /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
   211,  /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
   211,  /* (29) cmd ::= DROP DATABASE ifexists ids */
   211,  /* (30) cmd ::= DROP DNODE ids */
   211,  /* (31) cmd ::= DROP USER ids */
   211,  /* (32) cmd ::= DROP ACCOUNT ids */
   211,  /* (33) cmd ::= USE ids */
   211,  /* (34) cmd ::= DESCRIBE ids cpxName */
   211,  /* (35) cmd ::= ALTER USER ids PASS ids */
   211,  /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
   211,  /* (37) cmd ::= ALTER DNODE ids ids */
   211,  /* (38) cmd ::= ALTER DNODE ids ids ids */
   211,  /* (39) cmd ::= ALTER LOCAL ids */
   211,  /* (40) cmd ::= ALTER LOCAL ids ids */
   211,  /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
   211,  /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
   211,  /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   213,  /* (44) ids ::= ID */
   213,  /* (45) ids ::= STRING */
   215,  /* (46) ifexists ::= IF EXISTS */
   215,  /* (47) ifexists ::= */
   218,  /* (48) ifnotexists ::= IF NOT EXISTS */
   218,  /* (49) ifnotexists ::= */
   211,  /* (50) cmd ::= CREATE DNODE ids */
   211,  /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   211,  /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   211,  /* (53) cmd ::= CREATE USER ids PASS ids */
   220,  /* (54) pps ::= */
   220,  /* (55) pps ::= PPS INTEGER */
   221,  /* (56) tseries ::= */
   221,  /* (57) tseries ::= TSERIES INTEGER */
   222,  /* (58) dbs ::= */
   222,  /* (59) dbs ::= DBS INTEGER */
   223,  /* (60) streams ::= */
   223,  /* (61) streams ::= STREAMS INTEGER */
   224,  /* (62) storage ::= */
   224,  /* (63) storage ::= STORAGE INTEGER */
   225,  /* (64) qtime ::= */
   225,  /* (65) qtime ::= QTIME INTEGER */
   226,  /* (66) users ::= */
   226,  /* (67) users ::= USERS INTEGER */
   227,  /* (68) conns ::= */
   227,  /* (69) conns ::= CONNS INTEGER */
   228,  /* (70) state ::= */
   228,  /* (71) state ::= STATE ids */
   217,  /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   229,  /* (73) keep ::= KEEP tagitemlist */
   231,  /* (74) cache ::= CACHE INTEGER */
   232,  /* (75) replica ::= REPLICA INTEGER */
   233,  /* (76) quorum ::= QUORUM INTEGER */
   234,  /* (77) days ::= DAYS INTEGER */
   235,  /* (78) minrows ::= MINROWS INTEGER */
   236,  /* (79) maxrows ::= MAXROWS INTEGER */
   237,  /* (80) blocks ::= BLOCKS INTEGER */
   238,  /* (81) ctime ::= CTIME INTEGER */
   239,  /* (82) wal ::= WAL INTEGER */
   240,  /* (83) fsync ::= FSYNC INTEGER */
   241,  /* (84) comp ::= COMP INTEGER */
   242,  /* (85) prec ::= PRECISION STRING */
   243,  /* (86) update ::= UPDATE INTEGER */
   244,  /* (87) cachelast ::= CACHELAST INTEGER */
   219,  /* (88) db_optr ::= */
   219,  /* (89) db_optr ::= db_optr cache */
   219,  /* (90) db_optr ::= db_optr replica */
   219,  /* (91) db_optr ::= db_optr quorum */
   219,  /* (92) db_optr ::= db_optr days */
   219,  /* (93) db_optr ::= db_optr minrows */
   219,  /* (94) db_optr ::= db_optr maxrows */
   219,  /* (95) db_optr ::= db_optr blocks */
   219,  /* (96) db_optr ::= db_optr ctime */
   219,  /* (97) db_optr ::= db_optr wal */
   219,  /* (98) db_optr ::= db_optr fsync */
   219,  /* (99) db_optr ::= db_optr comp */
   219,  /* (100) db_optr ::= db_optr prec */
   219,  /* (101) db_optr ::= db_optr keep */
   219,  /* (102) db_optr ::= db_optr update */
   219,  /* (103) db_optr ::= db_optr cachelast */
   216,  /* (104) alter_db_optr ::= */
   216,  /* (105) alter_db_optr ::= alter_db_optr replica */
   216,  /* (106) alter_db_optr ::= alter_db_optr quorum */
   216,  /* (107) alter_db_optr ::= alter_db_optr keep */
   216,  /* (108) alter_db_optr ::= alter_db_optr blocks */
   216,  /* (109) alter_db_optr ::= alter_db_optr comp */
   216,  /* (110) alter_db_optr ::= alter_db_optr wal */
   216,  /* (111) alter_db_optr ::= alter_db_optr fsync */
   216,  /* (112) alter_db_optr ::= alter_db_optr update */
   216,  /* (113) alter_db_optr ::= alter_db_optr cachelast */
   245,  /* (114) typename ::= ids */
   245,  /* (115) typename ::= ids LP signed RP */
   245,  /* (116) typename ::= ids UNSIGNED */
   246,  /* (117) signed ::= INTEGER */
   246,  /* (118) signed ::= PLUS INTEGER */
   246,  /* (119) signed ::= MINUS INTEGER */
   211,  /* (120) cmd ::= CREATE TABLE create_table_args */
   211,  /* (121) cmd ::= CREATE TABLE create_stable_args */
   211,  /* (122) cmd ::= CREATE STABLE create_stable_args */
   211,  /* (123) cmd ::= CREATE TABLE create_table_list */
   249,  /* (124) create_table_list ::= create_from_stable */
   249,  /* (125) create_table_list ::= create_table_list create_from_stable */
   247,  /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   248,  /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   250,  /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   250,  /* (129) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   252,  /* (130) tagNamelist ::= tagNamelist COMMA ids */
   252,  /* (131) tagNamelist ::= ids */
   247,  /* (132) create_table_args ::= ifnotexists ids cpxName AS select */
   251,  /* (133) columnlist ::= columnlist COMMA column */
   251,  /* (134) columnlist ::= column */
   254,  /* (135) column ::= ids typename */
   230,  /* (136) tagitemlist ::= tagitemlist COMMA tagitem */
   230,  /* (137) tagitemlist ::= tagitem */
   255,  /* (138) tagitem ::= INTEGER */
   255,  /* (139) tagitem ::= FLOAT */
   255,  /* (140) tagitem ::= STRING */
   255,  /* (141) tagitem ::= BOOL */
   255,  /* (142) tagitem ::= NULL */
   255,  /* (143) tagitem ::= MINUS INTEGER */
   255,  /* (144) tagitem ::= MINUS FLOAT */
   255,  /* (145) tagitem ::= PLUS INTEGER */
   255,  /* (146) tagitem ::= PLUS FLOAT */
   253,  /* (147) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   267,  /* (148) union ::= select */
   267,  /* (149) union ::= LP union RP */
   267,  /* (150) union ::= union UNION ALL select */
   267,  /* (151) union ::= union UNION ALL LP select RP */
   211,  /* (152) cmd ::= union */
   253,  /* (153) select ::= SELECT selcollist */
   268,  /* (154) sclp ::= selcollist COMMA */
   268,  /* (155) sclp ::= */
   256,  /* (156) selcollist ::= sclp distinct expr as */
   256,  /* (157) selcollist ::= sclp STAR */
   271,  /* (158) as ::= AS ids */
   271,  /* (159) as ::= ids */
   271,  /* (160) as ::= */
   269,  /* (161) distinct ::= DISTINCT */
   269,  /* (162) distinct ::= */
   257,  /* (163) from ::= FROM tablelist */
   272,  /* (164) tablelist ::= ids cpxName */
   272,  /* (165) tablelist ::= ids cpxName ids */
   272,  /* (166) tablelist ::= tablelist COMMA ids cpxName */
   272,  /* (167) tablelist ::= tablelist COMMA ids cpxName ids */
   273,  /* (168) tmvar ::= VARIABLE */
   259,  /* (169) interval_opt ::= INTERVAL LP tmvar RP */
   259,  /* (170) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   259,  /* (171) interval_opt ::= */
   260,  /* (172) fill_opt ::= */
   260,  /* (173) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   260,  /* (174) fill_opt ::= FILL LP ID RP */
   261,  /* (175) sliding_opt ::= SLIDING LP tmvar RP */
   261,  /* (176) sliding_opt ::= */
   263,  /* (177) orderby_opt ::= */
   263,  /* (178) orderby_opt ::= ORDER BY sortlist */
   274,  /* (179) sortlist ::= sortlist COMMA item sortorder */
   274,  /* (180) sortlist ::= item sortorder */
   276,  /* (181) item ::= ids cpxName */
   277,  /* (182) sortorder ::= ASC */
   277,  /* (183) sortorder ::= DESC */
   277,  /* (184) sortorder ::= */
   262,  /* (185) groupby_opt ::= */
   262,  /* (186) groupby_opt ::= GROUP BY grouplist */
   278,  /* (187) grouplist ::= grouplist COMMA item */
   278,  /* (188) grouplist ::= item */
   264,  /* (189) having_opt ::= */
   264,  /* (190) having_opt ::= HAVING expr */
   266,  /* (191) limit_opt ::= */
   266,  /* (192) limit_opt ::= LIMIT signed */
   266,  /* (193) limit_opt ::= LIMIT signed OFFSET signed */
   266,  /* (194) limit_opt ::= LIMIT signed COMMA signed */
   265,  /* (195) slimit_opt ::= */
   265,  /* (196) slimit_opt ::= SLIMIT signed */
   265,  /* (197) slimit_opt ::= SLIMIT signed SOFFSET signed */
   265,  /* (198) slimit_opt ::= SLIMIT signed COMMA signed */
   258,  /* (199) where_opt ::= */
   258,  /* (200) where_opt ::= WHERE expr */
   270,  /* (201) expr ::= LP expr RP */
   270,  /* (202) expr ::= ID */
   270,  /* (203) expr ::= ID DOT ID */
   270,  /* (204) expr ::= ID DOT STAR */
   270,  /* (205) expr ::= INTEGER */
   270,  /* (206) expr ::= MINUS INTEGER */
   270,  /* (207) expr ::= PLUS INTEGER */
   270,  /* (208) expr ::= FLOAT */
   270,  /* (209) expr ::= MINUS FLOAT */
   270,  /* (210) expr ::= PLUS FLOAT */
   270,  /* (211) expr ::= STRING */
   270,  /* (212) expr ::= NOW */
   270,  /* (213) expr ::= VARIABLE */
   270,  /* (214) expr ::= BOOL */
   270,  /* (215) expr ::= ID LP exprlist RP */
   270,  /* (216) expr ::= ID LP STAR RP */
   270,  /* (217) expr ::= expr IS NULL */
   270,  /* (218) expr ::= expr IS NOT NULL */
   270,  /* (219) expr ::= expr LT expr */
   270,  /* (220) expr ::= expr GT expr */
   270,  /* (221) expr ::= expr LE expr */
   270,  /* (222) expr ::= expr GE expr */
   270,  /* (223) expr ::= expr NE expr */
   270,  /* (224) expr ::= expr EQ expr */
   270,  /* (225) expr ::= expr BETWEEN expr AND expr */
   270,  /* (226) expr ::= expr AND expr */
   270,  /* (227) expr ::= expr OR expr */
   270,  /* (228) expr ::= expr PLUS expr */
   270,  /* (229) expr ::= expr MINUS expr */
   270,  /* (230) expr ::= expr STAR expr */
   270,  /* (231) expr ::= expr SLASH expr */
   270,  /* (232) expr ::= expr REM expr */
   270,  /* (233) expr ::= expr LIKE expr */
   270,  /* (234) expr ::= expr IN LP exprlist RP */
   279,  /* (235) exprlist ::= exprlist COMMA expritem */
   279,  /* (236) exprlist ::= expritem */
   280,  /* (237) expritem ::= expr */
   280,  /* (238) expritem ::= */
   211,  /* (239) cmd ::= RESET QUERY CACHE */
   211,  /* (240) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   211,  /* (241) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   211,  /* (242) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   211,  /* (243) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   211,  /* (244) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   211,  /* (245) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   211,  /* (246) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   211,  /* (247) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   211,  /* (248) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   211,  /* (249) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   211,  /* (250) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   211,  /* (251) cmd ::= KILL CONNECTION INTEGER */
   211,  /* (252) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   211,  /* (253) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW MNODES */
   -2,  /* (3) cmd ::= SHOW DNODES */
   -2,  /* (4) cmd ::= SHOW ACCOUNTS */
   -2,  /* (5) cmd ::= SHOW USERS */
   -2,  /* (6) cmd ::= SHOW MODULES */
   -2,  /* (7) cmd ::= SHOW QUERIES */
   -2,  /* (8) cmd ::= SHOW CONNECTIONS */
   -2,  /* (9) cmd ::= SHOW STREAMS */
   -2,  /* (10) cmd ::= SHOW VARIABLES */
   -2,  /* (11) cmd ::= SHOW SCORES */
   -2,  /* (12) cmd ::= SHOW GRANTS */
   -2,  /* (13) cmd ::= SHOW VNODES */
   -3,  /* (14) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (15) dbPrefix ::= */
   -2,  /* (16) dbPrefix ::= ids DOT */
    0,  /* (17) cpxName ::= */
   -2,  /* (18) cpxName ::= DOT ids */
   -5,  /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
   -4,  /* (20) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (21) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (23) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (25) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (29) cmd ::= DROP DATABASE ifexists ids */
   -3,  /* (30) cmd ::= DROP DNODE ids */
   -3,  /* (31) cmd ::= DROP USER ids */
   -3,  /* (32) cmd ::= DROP ACCOUNT ids */
   -2,  /* (33) cmd ::= USE ids */
   -3,  /* (34) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (35) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (37) cmd ::= ALTER DNODE ids ids */
   -5,  /* (38) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (39) cmd ::= ALTER LOCAL ids */
   -4,  /* (40) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (44) ids ::= ID */
   -1,  /* (45) ids ::= STRING */
   -2,  /* (46) ifexists ::= IF EXISTS */
    0,  /* (47) ifexists ::= */
   -3,  /* (48) ifnotexists ::= IF NOT EXISTS */
    0,  /* (49) ifnotexists ::= */
   -3,  /* (50) cmd ::= CREATE DNODE ids */
   -6,  /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (53) cmd ::= CREATE USER ids PASS ids */
    0,  /* (54) pps ::= */
   -2,  /* (55) pps ::= PPS INTEGER */
    0,  /* (56) tseries ::= */
   -2,  /* (57) tseries ::= TSERIES INTEGER */
    0,  /* (58) dbs ::= */
   -2,  /* (59) dbs ::= DBS INTEGER */
    0,  /* (60) streams ::= */
   -2,  /* (61) streams ::= STREAMS INTEGER */
    0,  /* (62) storage ::= */
   -2,  /* (63) storage ::= STORAGE INTEGER */
    0,  /* (64) qtime ::= */
   -2,  /* (65) qtime ::= QTIME INTEGER */
    0,  /* (66) users ::= */
   -2,  /* (67) users ::= USERS INTEGER */
    0,  /* (68) conns ::= */
   -2,  /* (69) conns ::= CONNS INTEGER */
    0,  /* (70) state ::= */
   -2,  /* (71) state ::= STATE ids */
   -9,  /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (73) keep ::= KEEP tagitemlist */
   -2,  /* (74) cache ::= CACHE INTEGER */
   -2,  /* (75) replica ::= REPLICA INTEGER */
   -2,  /* (76) quorum ::= QUORUM INTEGER */
   -2,  /* (77) days ::= DAYS INTEGER */
   -2,  /* (78) minrows ::= MINROWS INTEGER */
   -2,  /* (79) maxrows ::= MAXROWS INTEGER */
   -2,  /* (80) blocks ::= BLOCKS INTEGER */
   -2,  /* (81) ctime ::= CTIME INTEGER */
   -2,  /* (82) wal ::= WAL INTEGER */
   -2,  /* (83) fsync ::= FSYNC INTEGER */
   -2,  /* (84) comp ::= COMP INTEGER */
   -2,  /* (85) prec ::= PRECISION STRING */
   -2,  /* (86) update ::= UPDATE INTEGER */
   -2,  /* (87) cachelast ::= CACHELAST INTEGER */
    0,  /* (88) db_optr ::= */
   -2,  /* (89) db_optr ::= db_optr cache */
   -2,  /* (90) db_optr ::= db_optr replica */
   -2,  /* (91) db_optr ::= db_optr quorum */
   -2,  /* (92) db_optr ::= db_optr days */
   -2,  /* (93) db_optr ::= db_optr minrows */
   -2,  /* (94) db_optr ::= db_optr maxrows */
   -2,  /* (95) db_optr ::= db_optr blocks */
   -2,  /* (96) db_optr ::= db_optr ctime */
   -2,  /* (97) db_optr ::= db_optr wal */
   -2,  /* (98) db_optr ::= db_optr fsync */
   -2,  /* (99) db_optr ::= db_optr comp */
   -2,  /* (100) db_optr ::= db_optr prec */
   -2,  /* (101) db_optr ::= db_optr keep */
   -2,  /* (102) db_optr ::= db_optr update */
   -2,  /* (103) db_optr ::= db_optr cachelast */
    0,  /* (104) alter_db_optr ::= */
   -2,  /* (105) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (106) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (107) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (108) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (109) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (110) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (111) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (112) alter_db_optr ::= alter_db_optr update */
   -2,  /* (113) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (114) typename ::= ids */
   -4,  /* (115) typename ::= ids LP signed RP */
   -2,  /* (116) typename ::= ids UNSIGNED */
   -1,  /* (117) signed ::= INTEGER */
   -2,  /* (118) signed ::= PLUS INTEGER */
   -2,  /* (119) signed ::= MINUS INTEGER */
   -3,  /* (120) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (121) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (122) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (123) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (124) create_table_list ::= create_from_stable */
   -2,  /* (125) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (129) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (130) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (131) tagNamelist ::= ids */
   -5,  /* (132) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (133) columnlist ::= columnlist COMMA column */
   -1,  /* (134) columnlist ::= column */
   -2,  /* (135) column ::= ids typename */
   -3,  /* (136) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (137) tagitemlist ::= tagitem */
   -1,  /* (138) tagitem ::= INTEGER */
   -1,  /* (139) tagitem ::= FLOAT */
   -1,  /* (140) tagitem ::= STRING */
   -1,  /* (141) tagitem ::= BOOL */
   -1,  /* (142) tagitem ::= NULL */
   -2,  /* (143) tagitem ::= MINUS INTEGER */
   -2,  /* (144) tagitem ::= MINUS FLOAT */
   -2,  /* (145) tagitem ::= PLUS INTEGER */
   -2,  /* (146) tagitem ::= PLUS FLOAT */
  -12,  /* (147) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -1,  /* (148) union ::= select */
   -3,  /* (149) union ::= LP union RP */
   -4,  /* (150) union ::= union UNION ALL select */
   -6,  /* (151) union ::= union UNION ALL LP select RP */
   -1,  /* (152) cmd ::= union */
   -2,  /* (153) select ::= SELECT selcollist */
   -2,  /* (154) sclp ::= selcollist COMMA */
    0,  /* (155) sclp ::= */
   -4,  /* (156) selcollist ::= sclp distinct expr as */
   -2,  /* (157) selcollist ::= sclp STAR */
   -2,  /* (158) as ::= AS ids */
   -1,  /* (159) as ::= ids */
    0,  /* (160) as ::= */
   -1,  /* (161) distinct ::= DISTINCT */
    0,  /* (162) distinct ::= */
   -2,  /* (163) from ::= FROM tablelist */
   -2,  /* (164) tablelist ::= ids cpxName */
   -3,  /* (165) tablelist ::= ids cpxName ids */
   -4,  /* (166) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (167) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (168) tmvar ::= VARIABLE */
   -4,  /* (169) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (170) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (171) interval_opt ::= */
    0,  /* (172) fill_opt ::= */
   -6,  /* (173) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (174) fill_opt ::= FILL LP ID RP */
   -4,  /* (175) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (176) sliding_opt ::= */
    0,  /* (177) orderby_opt ::= */
   -3,  /* (178) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (179) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (180) sortlist ::= item sortorder */
   -2,  /* (181) item ::= ids cpxName */
   -1,  /* (182) sortorder ::= ASC */
   -1,  /* (183) sortorder ::= DESC */
    0,  /* (184) sortorder ::= */
    0,  /* (185) groupby_opt ::= */
   -3,  /* (186) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (187) grouplist ::= grouplist COMMA item */
   -1,  /* (188) grouplist ::= item */
    0,  /* (189) having_opt ::= */
   -2,  /* (190) having_opt ::= HAVING expr */
    0,  /* (191) limit_opt ::= */
   -2,  /* (192) limit_opt ::= LIMIT signed */
   -4,  /* (193) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (194) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (195) slimit_opt ::= */
   -2,  /* (196) slimit_opt ::= SLIMIT signed */
   -4,  /* (197) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (198) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (199) where_opt ::= */
   -2,  /* (200) where_opt ::= WHERE expr */
   -3,  /* (201) expr ::= LP expr RP */
   -1,  /* (202) expr ::= ID */
   -3,  /* (203) expr ::= ID DOT ID */
   -3,  /* (204) expr ::= ID DOT STAR */
   -1,  /* (205) expr ::= INTEGER */
   -2,  /* (206) expr ::= MINUS INTEGER */
   -2,  /* (207) expr ::= PLUS INTEGER */
   -1,  /* (208) expr ::= FLOAT */
   -2,  /* (209) expr ::= MINUS FLOAT */
   -2,  /* (210) expr ::= PLUS FLOAT */
   -1,  /* (211) expr ::= STRING */
   -1,  /* (212) expr ::= NOW */
   -1,  /* (213) expr ::= VARIABLE */
   -1,  /* (214) expr ::= BOOL */
   -4,  /* (215) expr ::= ID LP exprlist RP */
   -4,  /* (216) expr ::= ID LP STAR RP */
   -3,  /* (217) expr ::= expr IS NULL */
   -4,  /* (218) expr ::= expr IS NOT NULL */
   -3,  /* (219) expr ::= expr LT expr */
   -3,  /* (220) expr ::= expr GT expr */
   -3,  /* (221) expr ::= expr LE expr */
   -3,  /* (222) expr ::= expr GE expr */
   -3,  /* (223) expr ::= expr NE expr */
   -3,  /* (224) expr ::= expr EQ expr */
   -5,  /* (225) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (226) expr ::= expr AND expr */
   -3,  /* (227) expr ::= expr OR expr */
   -3,  /* (228) expr ::= expr PLUS expr */
   -3,  /* (229) expr ::= expr MINUS expr */
   -3,  /* (230) expr ::= expr STAR expr */
   -3,  /* (231) expr ::= expr SLASH expr */
   -3,  /* (232) expr ::= expr REM expr */
   -3,  /* (233) expr ::= expr LIKE expr */
   -5,  /* (234) expr ::= expr IN LP exprlist RP */
   -3,  /* (235) exprlist ::= exprlist COMMA expritem */
   -1,  /* (236) exprlist ::= expritem */
   -1,  /* (237) expritem ::= expr */
    0,  /* (238) expritem ::= */
   -3,  /* (239) cmd ::= RESET QUERY CACHE */
   -7,  /* (240) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (241) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (242) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (243) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (244) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (245) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (246) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (247) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (248) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (249) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (250) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (251) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (252) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (253) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  ParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  ParseCTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  YYACTIONTYPE yyact;             /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH
  (void)yyLookahead;
  (void)yyLookaheadToken;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfoNRhs[yyruleno];
    if( yysize ){
      fprintf(yyTraceFILE, "%sReduce %d [%s]%s, pop back to state %d.\n",
        yyTracePrompt,
        yyruleno, yyRuleName[yyruleno],
        yyruleno<YYNRULE_WITH_ACTION ? "" : " without external action",
        yymsp[yysize].stateno);
    }else{
      fprintf(yyTraceFILE, "%sReduce %d [%s]%s.\n",
        yyTracePrompt, yyruleno, yyRuleName[yyruleno],
        yyruleno<YYNRULE_WITH_ACTION ? "" : " without external action");
    }
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if( yyRuleInfoNRhs[yyruleno]==0 ){
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
      case 0: /* program ::= cmd */
      case 120: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==120);
      case 121: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==121);
      case 122: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==122);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 7: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 9: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 10: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 12: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 14: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 15: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 16: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 17: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 18: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 19: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1);
}
        break;
      case 28: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, TSDB_SUPER_TABLE);
}
        break;
      case 29: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, -1); }
        break;
      case 30: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 31: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 32: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 33: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 34: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 35: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 36: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 38: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 39: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 40: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy47);}
        break;
      case 43: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 44: /* ids ::= ID */
      case 45: /* ids ::= STRING */ yytestcase(yyruleno==45);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 46: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 47: /* ifexists ::= */
      case 49: /* ifnotexists ::= */ yytestcase(yyruleno==49);
      case 162: /* distinct ::= */ yytestcase(yyruleno==162);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 48: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 50: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 51: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 52: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
        break;
      case 53: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 54: /* pps ::= */
      case 56: /* tseries ::= */ yytestcase(yyruleno==56);
      case 58: /* dbs ::= */ yytestcase(yyruleno==58);
      case 60: /* streams ::= */ yytestcase(yyruleno==60);
      case 62: /* storage ::= */ yytestcase(yyruleno==62);
      case 64: /* qtime ::= */ yytestcase(yyruleno==64);
      case 66: /* users ::= */ yytestcase(yyruleno==66);
      case 68: /* conns ::= */ yytestcase(yyruleno==68);
      case 70: /* state ::= */ yytestcase(yyruleno==70);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 55: /* pps ::= PPS INTEGER */
      case 57: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==57);
      case 59: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==59);
      case 61: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==61);
      case 63: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==63);
      case 65: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==65);
      case 67: /* users ::= USERS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==69);
      case 71: /* state ::= STATE ids */ yytestcase(yyruleno==71);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 72: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy47.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy47.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy47.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy47.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy47.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy47.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy47.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy47 = yylhsminor.yy47;
        break;
      case 73: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy247 = yymsp[0].minor.yy247; }
        break;
      case 74: /* cache ::= CACHE INTEGER */
      case 75: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==75);
      case 76: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==76);
      case 77: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==79);
      case 80: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==80);
      case 81: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==81);
      case 82: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==82);
      case 83: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==83);
      case 84: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==84);
      case 85: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==85);
      case 86: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==86);
      case 87: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==87);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 88: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
        break;
      case 89: /* db_optr ::= db_optr cache */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 90: /* db_optr ::= db_optr replica */
      case 105: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==105);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 91: /* db_optr ::= db_optr quorum */
      case 106: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==106);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 92: /* db_optr ::= db_optr days */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 93: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 94: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 95: /* db_optr ::= db_optr blocks */
      case 108: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==108);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 96: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 97: /* db_optr ::= db_optr wal */
      case 110: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==110);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 98: /* db_optr ::= db_optr fsync */
      case 111: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==111);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 99: /* db_optr ::= db_optr comp */
      case 109: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==109);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 100: /* db_optr ::= db_optr prec */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 101: /* db_optr ::= db_optr keep */
      case 107: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==107);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.keep = yymsp[0].minor.yy247; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 102: /* db_optr ::= db_optr update */
      case 112: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==112);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 103: /* db_optr ::= db_optr cachelast */
      case 113: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==113);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 104: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
        break;
      case 114: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy179, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy179 = yylhsminor.yy179;
        break;
      case 115: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy403 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy403;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy179 = yylhsminor.yy179;
        break;
      case 116: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy179, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 117: /* signed ::= INTEGER */
{ yylhsminor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 118: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 119: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy403 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 123: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy358;}
        break;
      case 124: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy42);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy358 = pCreateTable;
}
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 125: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy358->childTableInfo, &yymsp[0].minor.yy42);
  yylhsminor.yy358 = yymsp[-1].minor.yy358;
}
  yymsp[-1].minor.yy358 = yylhsminor.yy358;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy358 = tSetCreateSqlElems(yymsp[-1].minor.yy247, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy358 = yylhsminor.yy358;
        break;
      case 127: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy358 = tSetCreateSqlElems(yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy358 = yylhsminor.yy358;
        break;
      case 128: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy247, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy42 = yylhsminor.yy42;
        break;
      case 129: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy42 = yylhsminor.yy42;
        break;
      case 130: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy0); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 131: /* tagNamelist ::= ids */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 132: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy358 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy114, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy358 = yylhsminor.yy358;
        break;
      case 133: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy179); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 134: /* columnlist ::= column */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy179);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 135: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy179, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy179);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 136: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);    }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 137: /* tagitemlist ::= tagitem */
{ yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1); }
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 138: /* tagitem ::= INTEGER */
      case 139: /* tagitem ::= FLOAT */ yytestcase(yyruleno==139);
      case 140: /* tagitem ::= STRING */ yytestcase(yyruleno==140);
      case 141: /* tagitem ::= BOOL */ yytestcase(yyruleno==141);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 142: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 143: /* tagitem ::= MINUS INTEGER */
      case 144: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==144);
      case 145: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==145);
      case 146: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==146);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 147: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy114 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy522, yymsp[-9].minor.yy247, yymsp[-8].minor.yy326, yymsp[-4].minor.yy247, yymsp[-3].minor.yy247, &yymsp[-7].minor.yy430, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy247, &yymsp[0].minor.yy204, &yymsp[-1].minor.yy204, yymsp[-2].minor.yy326);
}
  yymsp[-11].minor.yy114 = yylhsminor.yy114;
        break;
      case 148: /* union ::= select */
{ yylhsminor.yy219 = setSubclause(NULL, yymsp[0].minor.yy114); }
  yymsp[0].minor.yy219 = yylhsminor.yy219;
        break;
      case 149: /* union ::= LP union RP */
{ yymsp[-2].minor.yy219 = yymsp[-1].minor.yy219; }
        break;
      case 150: /* union ::= union UNION ALL select */
{ yylhsminor.yy219 = appendSelectClause(yymsp[-3].minor.yy219, yymsp[0].minor.yy114); }
  yymsp[-3].minor.yy219 = yylhsminor.yy219;
        break;
      case 151: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy219 = appendSelectClause(yymsp[-5].minor.yy219, yymsp[-1].minor.yy114); }
  yymsp[-5].minor.yy219 = yylhsminor.yy219;
        break;
      case 152: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy219, NULL, TSDB_SQL_SELECT); }
        break;
      case 153: /* select ::= SELECT selcollist */
{
  yylhsminor.yy114 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy522, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 154: /* sclp ::= selcollist COMMA */
{yylhsminor.yy522 = yymsp[-1].minor.yy522;}
  yymsp[-1].minor.yy522 = yylhsminor.yy522;
        break;
      case 155: /* sclp ::= */
{yymsp[1].minor.yy522 = 0;}
        break;
      case 156: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy522 = tSqlExprListAppend(yymsp[-3].minor.yy522, yymsp[-1].minor.yy326,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy522 = yylhsminor.yy522;
        break;
      case 157: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy522 = tSqlExprListAppend(yymsp[-1].minor.yy522, pNode, 0, 0);
}
  yymsp[-1].minor.yy522 = yylhsminor.yy522;
        break;
      case 158: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 159: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 160: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 161: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 163: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 164: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 165: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 166: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(yymsp[-3].minor.yy247, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 167: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(yymsp[-4].minor.yy247, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy247 = yylhsminor.yy247;
        break;
      case 168: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy430.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy430.offset.n = 0; yymsp[-3].minor.yy430.offset.z = NULL; yymsp[-3].minor.yy430.offset.type = 0;}
        break;
      case 170: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy430.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy430.offset = yymsp[-1].minor.yy0;}
        break;
      case 171: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy430, 0, sizeof(yymsp[1].minor.yy430));}
        break;
      case 172: /* fill_opt ::= */
{yymsp[1].minor.yy247 = 0;     }
        break;
      case 173: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy247, &A, -1, 0);
    yymsp[-5].minor.yy247 = yymsp[-1].minor.yy247;
}
        break;
      case 174: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy247 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 175: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 176: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 177: /* orderby_opt ::= */
{yymsp[1].minor.yy247 = 0;}
        break;
      case 178: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 179: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy247 = tVariantListAppend(yymsp[-3].minor.yy247, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 180: /* sortlist ::= item sortorder */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 181: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 182: /* sortorder ::= ASC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 183: /* sortorder ::= DESC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_DESC;}
        break;
      case 184: /* sortorder ::= */
{ yymsp[1].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 185: /* groupby_opt ::= */
{ yymsp[1].minor.yy247 = 0;}
        break;
      case 186: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 187: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);
}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 188: /* grouplist ::= item */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1);
}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 189: /* having_opt ::= */
      case 199: /* where_opt ::= */ yytestcase(yyruleno==199);
      case 238: /* expritem ::= */ yytestcase(yyruleno==238);
{yymsp[1].minor.yy326 = 0;}
        break;
      case 190: /* having_opt ::= HAVING expr */
      case 200: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==200);
{yymsp[-1].minor.yy326 = yymsp[0].minor.yy326;}
        break;
      case 191: /* limit_opt ::= */
      case 195: /* slimit_opt ::= */ yytestcase(yyruleno==195);
{yymsp[1].minor.yy204.limit = -1; yymsp[1].minor.yy204.offset = 0;}
        break;
      case 192: /* limit_opt ::= LIMIT signed */
      case 196: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==196);
{yymsp[-1].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-1].minor.yy204.offset = 0;}
        break;
      case 193: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 194: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 197: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 198: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 201: /* expr ::= LP expr RP */
{yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy326->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 202: /* expr ::= ID */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 203: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 204: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 205: /* expr ::= INTEGER */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 206: /* expr ::= MINUS INTEGER */
      case 207: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==207);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 208: /* expr ::= FLOAT */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 209: /* expr ::= MINUS FLOAT */
      case 210: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==210);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 211: /* expr ::= STRING */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 212: /* expr ::= NOW */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 213: /* expr ::= VARIABLE */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 214: /* expr ::= BOOL */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 215: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy326 = tSqlExprCreateFunction(yymsp[-1].minor.yy522, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy326 = yylhsminor.yy326;
        break;
      case 216: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy326 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy326 = yylhsminor.yy326;
        break;
      case 217: /* expr ::= expr IS NULL */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 218: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-3].minor.yy326, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy326 = yylhsminor.yy326;
        break;
      case 219: /* expr ::= expr LT expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_LT);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 220: /* expr ::= expr GT expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_GT);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 221: /* expr ::= expr LE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_LE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 222: /* expr ::= expr GE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_GE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 223: /* expr ::= expr NE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_NE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 224: /* expr ::= expr EQ expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_EQ);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 225: /* expr ::= expr BETWEEN expr AND expr */
{ tSQLExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy326); yylhsminor.yy326 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy326, yymsp[-2].minor.yy326, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy326, TK_LE), TK_AND);}
  yymsp[-4].minor.yy326 = yylhsminor.yy326;
        break;
      case 226: /* expr ::= expr AND expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_AND);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 227: /* expr ::= expr OR expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_OR); }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 228: /* expr ::= expr PLUS expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_PLUS);  }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 229: /* expr ::= expr MINUS expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_MINUS); }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 230: /* expr ::= expr STAR expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_STAR);  }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 231: /* expr ::= expr SLASH expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_DIVIDE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 232: /* expr ::= expr REM expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_REM);   }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 233: /* expr ::= expr LIKE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_LIKE);  }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 234: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-4].minor.yy326, (tSQLExpr*)yymsp[-1].minor.yy522, TK_IN); }
  yymsp[-4].minor.yy326 = yylhsminor.yy326;
        break;
      case 235: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy522 = tSqlExprListAppend(yymsp[-2].minor.yy522,yymsp[0].minor.yy326,0, 0);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 236: /* exprlist ::= expritem */
{yylhsminor.yy522 = tSqlExprListAppend(0,yymsp[0].minor.yy326,0, 0);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 237: /* expritem ::= expr */
{yylhsminor.yy326 = yymsp[0].minor.yy326;}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 239: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 240: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 241: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 242: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 243: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 244: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 245: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy378, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 246: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 247: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 248: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 249: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 250: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 251: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 252: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 253: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfoLhs)/sizeof(yyRuleInfoLhs[0]) );
  yygoto = yyRuleInfoLhs[yyruleno];
  yysize = yyRuleInfoNRhs[yyruleno];
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
  ParseARG_FETCH
  ParseCTX_FETCH
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
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  ParseTOKENTYPE yyminor         /* The minor type of the error token */
){
  ParseARG_FETCH
  ParseCTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/

  pInfo->valid = false;
  int32_t outputBufLen = tListLen(pInfo->msg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->msg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->msg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->msg, "Incomplete SQL statement");
  }

  assert(len <= outputBufLen);
/************ End %syntax_error code ******************************************/
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH
  ParseCTX_FETCH
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

/*********** End %parse_accept code *******************************************/
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "ParseAlloc" which describes the current state of the parser.
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
void Parse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  ParseTOKENTYPE yyminor       /* The value for the token */
  ParseARG_PDECL               /* Optional %extra_argument parameter */
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
  ParseCTX_FETCH
  ParseARG_STORE

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
                        yyminor ParseCTX_PARAM);
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
int ParseFallback(int iToken){
#ifdef YYFALLBACK
  assert( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) );
  return yyFallback[iToken];
#else
  (void)iToken;
  return 0;
#endif
}
