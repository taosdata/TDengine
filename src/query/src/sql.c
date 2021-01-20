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
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
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
#define YYNOCODE 279
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateTableSQL* yy38;
  SCreateAcctInfo yy71;
  tSQLExpr* yy78;
  int yy96;
  SQuerySQL* yy148;
  SCreatedTableInfo yy152;
  SSubclauseInfo* yy153;
  tSQLExprList* yy166;
  SLimitVal yy167;
  TAOS_FIELD yy183;
  SCreateDbInfo yy234;
  int64_t yy325;
  SIntervalVal yy400;
  SArray* yy421;
  tVariant yy430;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             258
#define YYNRULE              240
#define YYNTOKEN             209
#define YY_MAX_SHIFT         257
#define YY_MIN_SHIFTREDUCE   431
#define YY_MAX_SHIFTREDUCE   670
#define YY_ERROR_ACTION      671
#define YY_ACCEPT_ACTION     672
#define YY_NO_ACTION         673
#define YY_MIN_REDUCE        674
#define YY_MAX_REDUCE        913
/************* End control #defines *******************************************/

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
#define YY_ACTTAB_COUNT (586)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   143,  474,  143,   23,  672,  257,  165,  547,  827,  475,
 /*    10 */   900,  168,  901,   37,   38,   12,   39,   40,  816,   23,
 /*    20 */   173,   31,  474,  474,  210,   43,   41,   45,   42,  805,
 /*    30 */   475,  475,  163,   36,   35,  232,  231,   34,   33,   32,
 /*    40 */    37,   38,  801,   39,   40,  816,  110,  173,   31,  162,
 /*    50 */   255,  210,   43,   41,   45,   42,  176,   66,  802,  195,
 /*    60 */    36,   35,  178,  824,   34,   33,   32,  432,  433,  434,
 /*    70 */   435,  436,  437,  438,  439,  440,  441,  442,  443,  256,
 /*    80 */   179,  225,  185,   37,   38,  805,   39,   40,  796,  242,
 /*    90 */   173,   31,  143,  180,  210,   43,   41,   45,   42,  110,
 /*   100 */   110,  167,  901,   36,   35,   57,  853,   34,   33,   32,
 /*   110 */    17,  223,  250,  249,  222,  221,  220,  248,  219,  247,
 /*   120 */   246,  245,  218,  244,  243,  803,  142,  774,  624,  762,
 /*   130 */   763,  764,  765,  766,  767,  768,  769,  770,  771,  772,
 /*   140 */   773,  775,  776,   38,  181,   39,   40,  229,  228,  173,
 /*   150 */    31,  605,  606,  210,   43,   41,   45,   42,  207,  854,
 /*   160 */    61,  205,   36,   35,   23,  110,   34,   33,   32,  188,
 /*   170 */    39,   40,   23,  251,  173,   31,  192,  191,  210,   43,
 /*   180 */    41,   45,   42,   34,   33,   32,  105,   36,   35,  104,
 /*   190 */   147,   34,   33,   32,  172,  637,  805,   28,  628,  897,
 /*   200 */   631,  177,  634,  802,  172,  637,  896,   13,  628,  230,
 /*   210 */   631,  802,  634,   18,  172,  637,  794,  895,  628,   63,
 /*   220 */   631,   28,  634,  155,  574,   62,  169,  170,   23,  156,
 /*   230 */   209,   29,  197,   92,   91,  150,  169,  170,   77,   76,
 /*   240 */   582,   17,  198,  250,  249,  159,  169,  170,  248,  626,
 /*   250 */   247,  246,  245,  715,  244,  243,  133,  211,  780,   80,
 /*   260 */   160,  778,  779,   18,  242,  234,  781,  802,  783,  784,
 /*   270 */   782,   28,  785,  786,   43,   41,   45,   42,  724,  579,
 /*   280 */    64,  133,   36,   35,   19,  627,   34,   33,   32,    3,
 /*   290 */   124,  194,  225,   44,  910,   72,   68,   71,  158,   11,
 /*   300 */    10,  566,  592,   44,  563,  636,  564,  107,  565,  793,
 /*   310 */    22,  795,  630,   44,  633,  636,  716,   36,   35,  133,
 /*   320 */   635,   34,   33,   32,  171,  636,  596,   49,   78,   82,
 /*   330 */   635,   48,  182,  183,   87,   90,   81,  137,  135,  629,
 /*   340 */   635,  632,   84,   95,   94,   93,   50,    9,  145,  640,
 /*   350 */    52,   65,  120,  254,  253,   98,  597,  656,  638,  555,
 /*   360 */   146,   15,   14,   14,   24,    4,   55,   53,  546,  213,
 /*   370 */   556,  570,  148,  571,   24,   48,  568,  149,  569,   89,
 /*   380 */    88,  103,  101,  153,  154,  152,  141,  151,  144,  804,
 /*   390 */   864,  863,  818,  174,  860,  859,  175,  233,  826,  846,
 /*   400 */   831,  833,  106,  121,  845,  122,  567,  119,  123,  726,
 /*   410 */   217,  139,   26,  226,  102,  723,   28,  227,  909,   74,
 /*   420 */   908,  906,  125,  744,   27,   25,  196,  140,  713,  591,
 /*   430 */    83,  711,   85,   86,  199,  709,  708,  184,   54,  134,
 /*   440 */   706,  164,  705,  704,  703,  702,  136,  700,  698,  696,
 /*   450 */   694,  692,  138,  203,   58,   59,   51,  847,  815,   46,
 /*   460 */   208,  206,  204,  202,  200,   30,   79,  235,  236,  237,
 /*   470 */   238,  239,  240,  241,  161,  215,  216,  252,  670,  187,
 /*   480 */   186,  669,   69,  189,  157,  190,  668,  193,  661,  707,
 /*   490 */   197,  576,   60,   56,  593,   96,  128,   97,  127,  745,
 /*   500 */   126,  130,  129,  131,  132,  701,  693,  113,  111,  118,
 /*   510 */   116,  114,  112,  115,  800,    1,  117,    2,  166,   20,
 /*   520 */   108,  201,    6,  598,  109,    7,  639,    5,    8,   21,
 /*   530 */    16,   67,  212,  641,  214,  515,   65,  511,  509,  508,
 /*   540 */   507,  504,  478,  224,   70,   47,   73,   75,   24,  549,
 /*   550 */   548,  545,  499,  497,  489,  495,  491,  493,  487,  485,
 /*   560 */   517,  516,  514,  513,  512,  510,  506,  505,   48,  476,
 /*   570 */   447,  445,  674,  673,  673,  673,  673,  673,  673,  673,
 /*   580 */   673,  673,  673,  673,   99,  100,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   267,    1,  267,  213,  210,  211,  230,    5,  213,    9,
 /*    10 */   277,  276,  277,   13,   14,  267,   16,   17,  251,  213,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  253,
 /*    30 */     9,    9,  265,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  252,   16,   17,  251,  213,   20,   21,  212,
 /*    50 */   213,   24,   25,   26,   27,   28,  250,  218,  252,  265,
 /*    60 */    33,   34,  230,  268,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    66,   76,   60,   13,   14,  253,   16,   17,  249,   78,
 /*    90 */    20,   21,  267,  213,   24,   25,   26,   27,   28,  213,
 /*   100 */   213,  276,  277,   33,   34,  105,  273,   37,   38,   39,
 /*   110 */    85,   86,   87,   88,   89,   90,   91,   92,   93,   94,
 /*   120 */    95,   96,   97,   98,   99,  245,  267,  229,  101,  231,
 /*   130 */   232,  233,  234,  235,  236,  237,  238,  239,  240,  241,
 /*   140 */   242,  243,  244,   14,  130,   16,   17,  133,  134,   20,
 /*   150 */    21,  118,  119,   24,   25,   26,   27,   28,  271,  273,
 /*   160 */   273,  275,   33,   34,  213,  213,   37,   38,   39,  129,
 /*   170 */    16,   17,  213,  230,   20,   21,  136,  137,   24,   25,
 /*   180 */    26,   27,   28,   37,   38,   39,  213,   33,   34,  100,
 /*   190 */   267,   37,   38,   39,    1,    2,  253,  108,    5,  267,
 /*   200 */     7,  250,    9,  252,    1,    2,  267,   44,    5,  250,
 /*   210 */     7,  252,    9,  100,    1,    2,    0,  267,    5,  254,
 /*   220 */     7,  108,    9,   60,  101,  273,   33,   34,  213,   66,
 /*   230 */    37,  266,  109,   70,   71,   72,   33,   34,  131,  132,
 /*   240 */    37,   85,  269,   87,   88,  267,   33,   34,   92,    1,
 /*   250 */    94,   95,   96,  217,   98,   99,  220,   15,  229,   73,
 /*   260 */   267,  232,  233,  100,   78,  250,  237,  252,  239,  240,
 /*   270 */   241,  108,  243,  244,   25,   26,   27,   28,  217,  106,
 /*   280 */   218,  220,   33,   34,  111,   37,   37,   38,   39,   61,
 /*   290 */    62,  128,   76,  100,  253,   67,   68,   69,  135,  131,
 /*   300 */   132,    2,  101,  100,    5,  112,    7,  106,    9,  247,
 /*   310 */   248,  249,    5,  100,    7,  112,  217,   33,   34,  220,
 /*   320 */   127,   37,   38,   39,   59,  112,  101,  106,   61,   62,
 /*   330 */   127,  106,   33,   34,   67,   68,   69,   61,   62,    5,
 /*   340 */   127,    7,   75,   67,   68,   69,  125,  100,  267,  107,
 /*   350 */   106,  104,  105,   63,   64,   65,  101,  101,  101,  101,
 /*   360 */   267,  106,  106,  106,  106,  100,  100,  123,  102,  101,
 /*   370 */   101,    5,  267,    7,  106,  106,    5,  267,    7,   73,
 /*   380 */    74,   61,   62,  267,  267,  267,  267,  267,  267,  253,
 /*   390 */   246,  246,  251,  246,  246,  246,  246,  246,  213,  274,
 /*   400 */   213,  213,  213,  213,  274,  213,  107,  255,  213,  213,
 /*   410 */   213,  213,  213,  213,   59,  213,  108,  213,  213,  213,
 /*   420 */   213,  213,  213,  213,  213,  213,  251,  213,  213,  112,
 /*   430 */   213,  213,  213,  213,  270,  213,  213,  213,  122,  213,
 /*   440 */   213,  270,  213,  213,  213,  213,  213,  213,  213,  213,
 /*   450 */   213,  213,  213,  270,  214,  214,  124,  214,  264,  121,
 /*   460 */   116,  120,  115,  114,  113,  126,   84,   83,   49,   80,
 /*   470 */    82,   53,   81,   79,  214,  214,  214,   76,    5,    5,
 /*   480 */   138,    5,  218,  138,  214,    5,    5,  129,   86,  214,
 /*   490 */   109,  101,  106,  110,  101,  215,  222,  215,  226,  228,
 /*   500 */   227,  223,  225,  224,  221,  214,  214,  261,  263,  256,
 /*   510 */   258,  260,  262,  259,  251,  219,  257,  216,    1,  106,
 /*   520 */   100,  100,  117,  101,  100,  117,  101,  100,  100,  106,
 /*   530 */   100,   73,  103,  107,  103,    9,  104,    5,    5,    5,
 /*   540 */     5,    5,   77,   15,   73,   16,  132,  132,  106,    5,
 /*   550 */     5,  101,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   560 */     5,    5,    5,    5,    5,    5,    5,    5,  106,   77,
 /*   570 */    59,   58,    0,  278,  278,  278,  278,  278,  278,  278,
 /*   580 */   278,  278,  278,  278,   21,   21,  278,  278,  278,  278,
 /*   590 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   600 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   610 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   620 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   630 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   640 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   650 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   660 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   670 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   680 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   690 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   700 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   710 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   720 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   730 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   740 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   750 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   760 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   770 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   780 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   790 */   278,  278,  278,  278,  278,
};
#define YY_SHIFT_COUNT    (257)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (572)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   163,   25,  156,    5,  193,  213,   21,   21,   21,   21,
 /*    10 */    21,   21,    0,   22,  213,  299,  299,  299,  113,   21,
 /*    20 */    21,   21,  216,   21,   21,  186,   11,   11,  586,  203,
 /*    30 */   213,  213,  213,  213,  213,  213,  213,  213,  213,  213,
 /*    40 */   213,  213,  213,  213,  213,  213,  213,  299,  299,    2,
 /*    50 */     2,    2,    2,    2,    2,    2,   89,   21,   21,   21,
 /*    60 */    21,   33,   33,  173,   21,   21,   21,   21,   21,   21,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,  308,  355,  355,  317,  317,  317,
 /*   110 */   355,  316,  332,  338,  344,  341,  347,  349,  351,  339,
 /*   120 */   308,  355,  355,  355,    5,  355,  382,  384,  419,  389,
 /*   130 */   388,  418,  391,  394,  355,  401,  355,  401,  355,  586,
 /*   140 */   586,   27,   70,   70,   70,  129,  154,  249,  249,  249,
 /*   150 */   267,  284,  284,  284,  284,  228,  276,   14,   40,  146,
 /*   160 */   146,  247,  290,  123,  201,  225,  255,  256,  257,  307,
 /*   170 */   334,  248,  265,  242,  221,  244,  258,  268,  269,  107,
 /*   180 */   266,  168,  366,  371,  306,  320,  473,  342,  474,  476,
 /*   190 */   345,  480,  481,  402,  358,  381,  390,  383,  386,  393,
 /*   200 */   420,  517,  421,  422,  424,  413,  405,  423,  408,  425,
 /*   210 */   427,  426,  428,  429,  430,  431,  432,  458,  526,  532,
 /*   220 */   533,  534,  535,  536,  465,  528,  471,  529,  414,  415,
 /*   230 */   442,  544,  545,  450,  442,  547,  548,  549,  550,  551,
 /*   240 */   552,  553,  554,  555,  556,  557,  558,  559,  560,  561,
 /*   250 */   562,  462,  492,  563,  564,  511,  513,  572,
};
#define YY_REDUCE_COUNT (140)
#define YY_REDUCE_MIN   (-267)
#define YY_REDUCE_MAX   (301)
static const short yy_reduce_ofst[] = {
 /*     0 */  -206, -102,   29,   62, -265, -175, -114, -113, -194,  -49,
 /*    10 */   -41,   15, -205, -163, -267, -224, -168,  -57, -233,  -27,
 /*    20 */  -167,  -48, -161, -120, -210,   36,   61,   99,  -35, -252,
 /*    30 */  -141,  -77,  -68,  -61,  -50,  -22,   -7,   81,   93,  105,
 /*    40 */   110,  116,  117,  118,  119,  120,  121,   41,  136,  144,
 /*    50 */   145,  147,  148,  149,  150,  151,  141,  185,  187,  188,
 /*    60 */   189,  125,  130,  152,  190,  192,  195,  196,  197,  198,
 /*    70 */   199,  200,  202,  204,  205,  206,  207,  208,  209,  210,
 /*    80 */   211,  212,  214,  215,  217,  218,  219,  220,  222,  223,
 /*    90 */   224,  226,  227,  229,  230,  231,  232,  233,  234,  235,
 /*   100 */   236,  237,  238,  239,  175,  240,  241,  164,  171,  183,
 /*   110 */   243,  194,  245,  250,  246,  251,  254,  252,  259,  253,
 /*   120 */   263,  260,  261,  262,  264,  270,  271,  273,  272,  274,
 /*   130 */   277,  278,  279,  283,  275,  280,  291,  282,  292,  296,
 /*   140 */   301,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   671,  725,  714,  722,  903,  903,  671,  671,  671,  671,
 /*    10 */   671,  671,  828,  689,  903,  671,  671,  671,  671,  671,
 /*    20 */   671,  671,  722,  671,  671,  727,  727,  727,  823,  671,
 /*    30 */   671,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*    40 */   671,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*    50 */   671,  671,  671,  671,  671,  671,  671,  671,  830,  832,
 /*    60 */   671,  850,  850,  821,  671,  671,  671,  671,  671,  671,
 /*    70 */   671,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*    80 */   671,  671,  671,  712,  671,  710,  671,  671,  671,  671,
 /*    90 */   671,  671,  671,  671,  671,  671,  671,  671,  699,  671,
 /*   100 */   671,  671,  671,  671,  671,  691,  691,  671,  671,  671,
 /*   110 */   691,  857,  861,  855,  843,  851,  842,  838,  837,  865,
 /*   120 */   671,  691,  691,  691,  722,  691,  743,  741,  739,  731,
 /*   130 */   737,  733,  735,  729,  691,  720,  691,  720,  691,  761,
 /*   140 */   777,  671,  866,  902,  856,  892,  891,  898,  890,  889,
 /*   150 */   671,  885,  886,  888,  887,  671,  671,  671,  671,  894,
 /*   160 */   893,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*   170 */   671,  671,  868,  671,  862,  858,  671,  671,  671,  671,
 /*   180 */   787,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*   190 */   671,  671,  671,  671,  671,  820,  671,  671,  829,  671,
 /*   200 */   671,  671,  671,  671,  671,  852,  671,  844,  671,  671,
 /*   210 */   671,  671,  671,  797,  671,  671,  671,  671,  671,  671,
 /*   220 */   671,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*   230 */   907,  671,  671,  671,  905,  671,  671,  671,  671,  671,
 /*   240 */   671,  671,  671,  671,  671,  671,  671,  671,  671,  671,
 /*   250 */   671,  746,  671,  697,  695,  671,  687,  671,
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
    0,  /*         AS => nothing */
    0,  /*      COMMA => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
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
    1,  /*     STABLE => ID */
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
  /*   67 */ "DNODE",
  /*   68 */ "USER",
  /*   69 */ "ACCOUNT",
  /*   70 */ "USE",
  /*   71 */ "DESCRIBE",
  /*   72 */ "ALTER",
  /*   73 */ "PASS",
  /*   74 */ "PRIVILEGE",
  /*   75 */ "LOCAL",
  /*   76 */ "IF",
  /*   77 */ "EXISTS",
  /*   78 */ "PPS",
  /*   79 */ "TSERIES",
  /*   80 */ "DBS",
  /*   81 */ "STORAGE",
  /*   82 */ "QTIME",
  /*   83 */ "CONNS",
  /*   84 */ "STATE",
  /*   85 */ "KEEP",
  /*   86 */ "CACHE",
  /*   87 */ "REPLICA",
  /*   88 */ "QUORUM",
  /*   89 */ "DAYS",
  /*   90 */ "MINROWS",
  /*   91 */ "MAXROWS",
  /*   92 */ "BLOCKS",
  /*   93 */ "CTIME",
  /*   94 */ "WAL",
  /*   95 */ "FSYNC",
  /*   96 */ "COMP",
  /*   97 */ "PRECISION",
  /*   98 */ "UPDATE",
  /*   99 */ "CACHELAST",
  /*  100 */ "LP",
  /*  101 */ "RP",
  /*  102 */ "UNSIGNED",
  /*  103 */ "TAGS",
  /*  104 */ "USING",
  /*  105 */ "AS",
  /*  106 */ "COMMA",
  /*  107 */ "NULL",
  /*  108 */ "SELECT",
  /*  109 */ "UNION",
  /*  110 */ "ALL",
  /*  111 */ "FROM",
  /*  112 */ "VARIABLE",
  /*  113 */ "INTERVAL",
  /*  114 */ "FILL",
  /*  115 */ "SLIDING",
  /*  116 */ "ORDER",
  /*  117 */ "BY",
  /*  118 */ "ASC",
  /*  119 */ "DESC",
  /*  120 */ "GROUP",
  /*  121 */ "HAVING",
  /*  122 */ "LIMIT",
  /*  123 */ "OFFSET",
  /*  124 */ "SLIMIT",
  /*  125 */ "SOFFSET",
  /*  126 */ "WHERE",
  /*  127 */ "NOW",
  /*  128 */ "RESET",
  /*  129 */ "QUERY",
  /*  130 */ "ADD",
  /*  131 */ "COLUMN",
  /*  132 */ "TAG",
  /*  133 */ "CHANGE",
  /*  134 */ "SET",
  /*  135 */ "KILL",
  /*  136 */ "CONNECTION",
  /*  137 */ "STREAM",
  /*  138 */ "COLON",
  /*  139 */ "ABORT",
  /*  140 */ "AFTER",
  /*  141 */ "ATTACH",
  /*  142 */ "BEFORE",
  /*  143 */ "BEGIN",
  /*  144 */ "CASCADE",
  /*  145 */ "CLUSTER",
  /*  146 */ "CONFLICT",
  /*  147 */ "COPY",
  /*  148 */ "DEFERRED",
  /*  149 */ "DELIMITERS",
  /*  150 */ "DETACH",
  /*  151 */ "EACH",
  /*  152 */ "END",
  /*  153 */ "EXPLAIN",
  /*  154 */ "FAIL",
  /*  155 */ "FOR",
  /*  156 */ "IGNORE",
  /*  157 */ "IMMEDIATE",
  /*  158 */ "INITIALLY",
  /*  159 */ "INSTEAD",
  /*  160 */ "MATCH",
  /*  161 */ "KEY",
  /*  162 */ "OF",
  /*  163 */ "RAISE",
  /*  164 */ "REPLACE",
  /*  165 */ "RESTRICT",
  /*  166 */ "ROW",
  /*  167 */ "STATEMENT",
  /*  168 */ "TRIGGER",
  /*  169 */ "VIEW",
  /*  170 */ "COUNT",
  /*  171 */ "SUM",
  /*  172 */ "AVG",
  /*  173 */ "MIN",
  /*  174 */ "MAX",
  /*  175 */ "FIRST",
  /*  176 */ "LAST",
  /*  177 */ "TOP",
  /*  178 */ "BOTTOM",
  /*  179 */ "STDDEV",
  /*  180 */ "PERCENTILE",
  /*  181 */ "APERCENTILE",
  /*  182 */ "LEASTSQUARES",
  /*  183 */ "HISTOGRAM",
  /*  184 */ "DIFF",
  /*  185 */ "SPREAD",
  /*  186 */ "TWA",
  /*  187 */ "INTERP",
  /*  188 */ "LAST_ROW",
  /*  189 */ "RATE",
  /*  190 */ "IRATE",
  /*  191 */ "SUM_RATE",
  /*  192 */ "SUM_IRATE",
  /*  193 */ "AVG_RATE",
  /*  194 */ "AVG_IRATE",
  /*  195 */ "TBID",
  /*  196 */ "SEMI",
  /*  197 */ "NONE",
  /*  198 */ "PREV",
  /*  199 */ "LINEAR",
  /*  200 */ "IMPORT",
  /*  201 */ "METRIC",
  /*  202 */ "TBNAME",
  /*  203 */ "JOIN",
  /*  204 */ "METRICS",
  /*  205 */ "STABLE",
  /*  206 */ "INSERT",
  /*  207 */ "INTO",
  /*  208 */ "VALUES",
  /*  209 */ "error",
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
  /*  248 */ "create_table_list",
  /*  249 */ "create_from_stable",
  /*  250 */ "columnlist",
  /*  251 */ "select",
  /*  252 */ "column",
  /*  253 */ "tagitem",
  /*  254 */ "selcollist",
  /*  255 */ "from",
  /*  256 */ "where_opt",
  /*  257 */ "interval_opt",
  /*  258 */ "fill_opt",
  /*  259 */ "sliding_opt",
  /*  260 */ "groupby_opt",
  /*  261 */ "orderby_opt",
  /*  262 */ "having_opt",
  /*  263 */ "slimit_opt",
  /*  264 */ "limit_opt",
  /*  265 */ "union",
  /*  266 */ "sclp",
  /*  267 */ "expr",
  /*  268 */ "as",
  /*  269 */ "tablelist",
  /*  270 */ "tmvar",
  /*  271 */ "sortlist",
  /*  272 */ "sortitem",
  /*  273 */ "item",
  /*  274 */ "sortorder",
  /*  275 */ "grouplist",
  /*  276 */ "exprlist",
  /*  277 */ "expritem",
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
 /*  28 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  29 */ "cmd ::= DROP DNODE ids",
 /*  30 */ "cmd ::= DROP USER ids",
 /*  31 */ "cmd ::= DROP ACCOUNT ids",
 /*  32 */ "cmd ::= USE ids",
 /*  33 */ "cmd ::= DESCRIBE ids cpxName",
 /*  34 */ "cmd ::= ALTER USER ids PASS ids",
 /*  35 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  36 */ "cmd ::= ALTER DNODE ids ids",
 /*  37 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  38 */ "cmd ::= ALTER LOCAL ids",
 /*  39 */ "cmd ::= ALTER LOCAL ids ids",
 /*  40 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  41 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  42 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  43 */ "ids ::= ID",
 /*  44 */ "ids ::= STRING",
 /*  45 */ "ifexists ::= IF EXISTS",
 /*  46 */ "ifexists ::=",
 /*  47 */ "ifnotexists ::= IF NOT EXISTS",
 /*  48 */ "ifnotexists ::=",
 /*  49 */ "cmd ::= CREATE DNODE ids",
 /*  50 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  51 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  52 */ "cmd ::= CREATE USER ids PASS ids",
 /*  53 */ "pps ::=",
 /*  54 */ "pps ::= PPS INTEGER",
 /*  55 */ "tseries ::=",
 /*  56 */ "tseries ::= TSERIES INTEGER",
 /*  57 */ "dbs ::=",
 /*  58 */ "dbs ::= DBS INTEGER",
 /*  59 */ "streams ::=",
 /*  60 */ "streams ::= STREAMS INTEGER",
 /*  61 */ "storage ::=",
 /*  62 */ "storage ::= STORAGE INTEGER",
 /*  63 */ "qtime ::=",
 /*  64 */ "qtime ::= QTIME INTEGER",
 /*  65 */ "users ::=",
 /*  66 */ "users ::= USERS INTEGER",
 /*  67 */ "conns ::=",
 /*  68 */ "conns ::= CONNS INTEGER",
 /*  69 */ "state ::=",
 /*  70 */ "state ::= STATE ids",
 /*  71 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  72 */ "keep ::= KEEP tagitemlist",
 /*  73 */ "cache ::= CACHE INTEGER",
 /*  74 */ "replica ::= REPLICA INTEGER",
 /*  75 */ "quorum ::= QUORUM INTEGER",
 /*  76 */ "days ::= DAYS INTEGER",
 /*  77 */ "minrows ::= MINROWS INTEGER",
 /*  78 */ "maxrows ::= MAXROWS INTEGER",
 /*  79 */ "blocks ::= BLOCKS INTEGER",
 /*  80 */ "ctime ::= CTIME INTEGER",
 /*  81 */ "wal ::= WAL INTEGER",
 /*  82 */ "fsync ::= FSYNC INTEGER",
 /*  83 */ "comp ::= COMP INTEGER",
 /*  84 */ "prec ::= PRECISION STRING",
 /*  85 */ "update ::= UPDATE INTEGER",
 /*  86 */ "cachelast ::= CACHELAST INTEGER",
 /*  87 */ "db_optr ::=",
 /*  88 */ "db_optr ::= db_optr cache",
 /*  89 */ "db_optr ::= db_optr replica",
 /*  90 */ "db_optr ::= db_optr quorum",
 /*  91 */ "db_optr ::= db_optr days",
 /*  92 */ "db_optr ::= db_optr minrows",
 /*  93 */ "db_optr ::= db_optr maxrows",
 /*  94 */ "db_optr ::= db_optr blocks",
 /*  95 */ "db_optr ::= db_optr ctime",
 /*  96 */ "db_optr ::= db_optr wal",
 /*  97 */ "db_optr ::= db_optr fsync",
 /*  98 */ "db_optr ::= db_optr comp",
 /*  99 */ "db_optr ::= db_optr prec",
 /* 100 */ "db_optr ::= db_optr keep",
 /* 101 */ "db_optr ::= db_optr update",
 /* 102 */ "db_optr ::= db_optr cachelast",
 /* 103 */ "alter_db_optr ::=",
 /* 104 */ "alter_db_optr ::= alter_db_optr replica",
 /* 105 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 106 */ "alter_db_optr ::= alter_db_optr keep",
 /* 107 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 108 */ "alter_db_optr ::= alter_db_optr comp",
 /* 109 */ "alter_db_optr ::= alter_db_optr wal",
 /* 110 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 111 */ "alter_db_optr ::= alter_db_optr update",
 /* 112 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 113 */ "typename ::= ids",
 /* 114 */ "typename ::= ids LP signed RP",
 /* 115 */ "typename ::= ids UNSIGNED",
 /* 116 */ "signed ::= INTEGER",
 /* 117 */ "signed ::= PLUS INTEGER",
 /* 118 */ "signed ::= MINUS INTEGER",
 /* 119 */ "cmd ::= CREATE TABLE create_table_args",
 /* 120 */ "cmd ::= CREATE TABLE create_table_list",
 /* 121 */ "create_table_list ::= create_from_stable",
 /* 122 */ "create_table_list ::= create_table_list create_from_stable",
 /* 123 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 124 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 125 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 126 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 127 */ "columnlist ::= columnlist COMMA column",
 /* 128 */ "columnlist ::= column",
 /* 129 */ "column ::= ids typename",
 /* 130 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 131 */ "tagitemlist ::= tagitem",
 /* 132 */ "tagitem ::= INTEGER",
 /* 133 */ "tagitem ::= FLOAT",
 /* 134 */ "tagitem ::= STRING",
 /* 135 */ "tagitem ::= BOOL",
 /* 136 */ "tagitem ::= NULL",
 /* 137 */ "tagitem ::= MINUS INTEGER",
 /* 138 */ "tagitem ::= MINUS FLOAT",
 /* 139 */ "tagitem ::= PLUS INTEGER",
 /* 140 */ "tagitem ::= PLUS FLOAT",
 /* 141 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 142 */ "union ::= select",
 /* 143 */ "union ::= LP union RP",
 /* 144 */ "union ::= union UNION ALL select",
 /* 145 */ "union ::= union UNION ALL LP select RP",
 /* 146 */ "cmd ::= union",
 /* 147 */ "select ::= SELECT selcollist",
 /* 148 */ "sclp ::= selcollist COMMA",
 /* 149 */ "sclp ::=",
 /* 150 */ "selcollist ::= sclp expr as",
 /* 151 */ "selcollist ::= sclp STAR",
 /* 152 */ "as ::= AS ids",
 /* 153 */ "as ::= ids",
 /* 154 */ "as ::=",
 /* 155 */ "from ::= FROM tablelist",
 /* 156 */ "tablelist ::= ids cpxName",
 /* 157 */ "tablelist ::= ids cpxName ids",
 /* 158 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 159 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 160 */ "tmvar ::= VARIABLE",
 /* 161 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 162 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 163 */ "interval_opt ::=",
 /* 164 */ "fill_opt ::=",
 /* 165 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 166 */ "fill_opt ::= FILL LP ID RP",
 /* 167 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 168 */ "sliding_opt ::=",
 /* 169 */ "orderby_opt ::=",
 /* 170 */ "orderby_opt ::= ORDER BY sortlist",
 /* 171 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 172 */ "sortlist ::= item sortorder",
 /* 173 */ "item ::= ids cpxName",
 /* 174 */ "sortorder ::= ASC",
 /* 175 */ "sortorder ::= DESC",
 /* 176 */ "sortorder ::=",
 /* 177 */ "groupby_opt ::=",
 /* 178 */ "groupby_opt ::= GROUP BY grouplist",
 /* 179 */ "grouplist ::= grouplist COMMA item",
 /* 180 */ "grouplist ::= item",
 /* 181 */ "having_opt ::=",
 /* 182 */ "having_opt ::= HAVING expr",
 /* 183 */ "limit_opt ::=",
 /* 184 */ "limit_opt ::= LIMIT signed",
 /* 185 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 186 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 187 */ "slimit_opt ::=",
 /* 188 */ "slimit_opt ::= SLIMIT signed",
 /* 189 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 190 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 191 */ "where_opt ::=",
 /* 192 */ "where_opt ::= WHERE expr",
 /* 193 */ "expr ::= LP expr RP",
 /* 194 */ "expr ::= ID",
 /* 195 */ "expr ::= ID DOT ID",
 /* 196 */ "expr ::= ID DOT STAR",
 /* 197 */ "expr ::= INTEGER",
 /* 198 */ "expr ::= MINUS INTEGER",
 /* 199 */ "expr ::= PLUS INTEGER",
 /* 200 */ "expr ::= FLOAT",
 /* 201 */ "expr ::= MINUS FLOAT",
 /* 202 */ "expr ::= PLUS FLOAT",
 /* 203 */ "expr ::= STRING",
 /* 204 */ "expr ::= NOW",
 /* 205 */ "expr ::= VARIABLE",
 /* 206 */ "expr ::= BOOL",
 /* 207 */ "expr ::= ID LP exprlist RP",
 /* 208 */ "expr ::= ID LP STAR RP",
 /* 209 */ "expr ::= expr IS NULL",
 /* 210 */ "expr ::= expr IS NOT NULL",
 /* 211 */ "expr ::= expr LT expr",
 /* 212 */ "expr ::= expr GT expr",
 /* 213 */ "expr ::= expr LE expr",
 /* 214 */ "expr ::= expr GE expr",
 /* 215 */ "expr ::= expr NE expr",
 /* 216 */ "expr ::= expr EQ expr",
 /* 217 */ "expr ::= expr AND expr",
 /* 218 */ "expr ::= expr OR expr",
 /* 219 */ "expr ::= expr PLUS expr",
 /* 220 */ "expr ::= expr MINUS expr",
 /* 221 */ "expr ::= expr STAR expr",
 /* 222 */ "expr ::= expr SLASH expr",
 /* 223 */ "expr ::= expr REM expr",
 /* 224 */ "expr ::= expr LIKE expr",
 /* 225 */ "expr ::= expr IN LP exprlist RP",
 /* 226 */ "exprlist ::= exprlist COMMA expritem",
 /* 227 */ "exprlist ::= expritem",
 /* 228 */ "expritem ::= expr",
 /* 229 */ "expritem ::=",
 /* 230 */ "cmd ::= RESET QUERY CACHE",
 /* 231 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 232 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 233 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 234 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 235 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 236 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 237 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 238 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 239 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
void ParseInit(void *yypParser){
  yyParser *pParser = (yyParser*)yypParser;
#ifdef YYTRACKMAXSTACKDEPTH
  pParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  pParser->yytos = NULL;
  pParser->yystack = NULL;
  pParser->yystksz = 0;
  if( yyGrowStack(pParser) ){
    pParser->yystack = &pParser->yystk0;
    pParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  pParser->yyerrcnt = -1;
#endif
  pParser->yytos = pParser->yystack;
  pParser->yystack[0].stateno = 0;
  pParser->yystack[0].major = 0;
#if YYSTACKDEPTH>0
  pParser->yystackEnd = &pParser->yystack[YYSTACKDEPTH-1];
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
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( pParser ) ParseInit(pParser);
  return pParser;
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
  ParseARG_FETCH;
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
    case 250: /* columnlist */
    case 258: /* fill_opt */
    case 260: /* groupby_opt */
    case 261: /* orderby_opt */
    case 271: /* sortlist */
    case 275: /* grouplist */
{
taosArrayDestroy((yypminor->yy421));
}
      break;
    case 248: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy38));
}
      break;
    case 251: /* select */
{
doDestroyQuerySql((yypminor->yy148));
}
      break;
    case 254: /* selcollist */
    case 266: /* sclp */
    case 276: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy166));
}
      break;
    case 256: /* where_opt */
    case 262: /* having_opt */
    case 267: /* expr */
    case 277: /* expritem */
{
tSqlExprDestroy((yypminor->yy78));
}
      break;
    case 265: /* union */
{
destroyAllSelectClause((yypminor->yy153));
}
      break;
    case 272: /* sortitem */
{
tVariantDestroy(&(yypminor->yy430));
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
static unsigned int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yytos->stateno;
 
  if( stateno>YY_MAX_SHIFT ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
#if defined(YYCOVERAGE)
  yycoverage[stateno][iLookAhead] = 1;
#endif
  do{
    i = yy_shift_ofst[stateno];
    assert( i>=0 && i+YYNTOKEN<=sizeof(yy_lookahead)/sizeof(yy_lookahead[0]) );
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( yy_lookahead[i]!=iLookAhead ){
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
static int yy_find_reduce_action(
  int stateno,              /* Current state number */
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
   ParseARG_FETCH;
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
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
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
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
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
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
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
  {  210,   -1 }, /* (0) program ::= cmd */
  {  211,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  211,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  211,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  211,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  211,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  211,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  211,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  211,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  211,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  211,   -2 }, /* (10) cmd ::= SHOW VARIABLES */
  {  211,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  211,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  211,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  211,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  212,    0 }, /* (15) dbPrefix ::= */
  {  212,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  214,    0 }, /* (17) cpxName ::= */
  {  214,   -2 }, /* (18) cpxName ::= DOT ids */
  {  211,   -5 }, /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  211,   -4 }, /* (20) cmd ::= SHOW CREATE DATABASE ids */
  {  211,   -3 }, /* (21) cmd ::= SHOW dbPrefix TABLES */
  {  211,   -5 }, /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  211,   -3 }, /* (23) cmd ::= SHOW dbPrefix STABLES */
  {  211,   -5 }, /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  211,   -3 }, /* (25) cmd ::= SHOW dbPrefix VGROUPS */
  {  211,   -4 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  211,   -5 }, /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
  {  211,   -4 }, /* (28) cmd ::= DROP DATABASE ifexists ids */
  {  211,   -3 }, /* (29) cmd ::= DROP DNODE ids */
  {  211,   -3 }, /* (30) cmd ::= DROP USER ids */
  {  211,   -3 }, /* (31) cmd ::= DROP ACCOUNT ids */
  {  211,   -2 }, /* (32) cmd ::= USE ids */
  {  211,   -3 }, /* (33) cmd ::= DESCRIBE ids cpxName */
  {  211,   -5 }, /* (34) cmd ::= ALTER USER ids PASS ids */
  {  211,   -5 }, /* (35) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  211,   -4 }, /* (36) cmd ::= ALTER DNODE ids ids */
  {  211,   -5 }, /* (37) cmd ::= ALTER DNODE ids ids ids */
  {  211,   -3 }, /* (38) cmd ::= ALTER LOCAL ids */
  {  211,   -4 }, /* (39) cmd ::= ALTER LOCAL ids ids */
  {  211,   -4 }, /* (40) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  211,   -4 }, /* (41) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  211,   -6 }, /* (42) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  213,   -1 }, /* (43) ids ::= ID */
  {  213,   -1 }, /* (44) ids ::= STRING */
  {  215,   -2 }, /* (45) ifexists ::= IF EXISTS */
  {  215,    0 }, /* (46) ifexists ::= */
  {  218,   -3 }, /* (47) ifnotexists ::= IF NOT EXISTS */
  {  218,    0 }, /* (48) ifnotexists ::= */
  {  211,   -3 }, /* (49) cmd ::= CREATE DNODE ids */
  {  211,   -6 }, /* (50) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  211,   -5 }, /* (51) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  211,   -5 }, /* (52) cmd ::= CREATE USER ids PASS ids */
  {  220,    0 }, /* (53) pps ::= */
  {  220,   -2 }, /* (54) pps ::= PPS INTEGER */
  {  221,    0 }, /* (55) tseries ::= */
  {  221,   -2 }, /* (56) tseries ::= TSERIES INTEGER */
  {  222,    0 }, /* (57) dbs ::= */
  {  222,   -2 }, /* (58) dbs ::= DBS INTEGER */
  {  223,    0 }, /* (59) streams ::= */
  {  223,   -2 }, /* (60) streams ::= STREAMS INTEGER */
  {  224,    0 }, /* (61) storage ::= */
  {  224,   -2 }, /* (62) storage ::= STORAGE INTEGER */
  {  225,    0 }, /* (63) qtime ::= */
  {  225,   -2 }, /* (64) qtime ::= QTIME INTEGER */
  {  226,    0 }, /* (65) users ::= */
  {  226,   -2 }, /* (66) users ::= USERS INTEGER */
  {  227,    0 }, /* (67) conns ::= */
  {  227,   -2 }, /* (68) conns ::= CONNS INTEGER */
  {  228,    0 }, /* (69) state ::= */
  {  228,   -2 }, /* (70) state ::= STATE ids */
  {  217,   -9 }, /* (71) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  229,   -2 }, /* (72) keep ::= KEEP tagitemlist */
  {  231,   -2 }, /* (73) cache ::= CACHE INTEGER */
  {  232,   -2 }, /* (74) replica ::= REPLICA INTEGER */
  {  233,   -2 }, /* (75) quorum ::= QUORUM INTEGER */
  {  234,   -2 }, /* (76) days ::= DAYS INTEGER */
  {  235,   -2 }, /* (77) minrows ::= MINROWS INTEGER */
  {  236,   -2 }, /* (78) maxrows ::= MAXROWS INTEGER */
  {  237,   -2 }, /* (79) blocks ::= BLOCKS INTEGER */
  {  238,   -2 }, /* (80) ctime ::= CTIME INTEGER */
  {  239,   -2 }, /* (81) wal ::= WAL INTEGER */
  {  240,   -2 }, /* (82) fsync ::= FSYNC INTEGER */
  {  241,   -2 }, /* (83) comp ::= COMP INTEGER */
  {  242,   -2 }, /* (84) prec ::= PRECISION STRING */
  {  243,   -2 }, /* (85) update ::= UPDATE INTEGER */
  {  244,   -2 }, /* (86) cachelast ::= CACHELAST INTEGER */
  {  219,    0 }, /* (87) db_optr ::= */
  {  219,   -2 }, /* (88) db_optr ::= db_optr cache */
  {  219,   -2 }, /* (89) db_optr ::= db_optr replica */
  {  219,   -2 }, /* (90) db_optr ::= db_optr quorum */
  {  219,   -2 }, /* (91) db_optr ::= db_optr days */
  {  219,   -2 }, /* (92) db_optr ::= db_optr minrows */
  {  219,   -2 }, /* (93) db_optr ::= db_optr maxrows */
  {  219,   -2 }, /* (94) db_optr ::= db_optr blocks */
  {  219,   -2 }, /* (95) db_optr ::= db_optr ctime */
  {  219,   -2 }, /* (96) db_optr ::= db_optr wal */
  {  219,   -2 }, /* (97) db_optr ::= db_optr fsync */
  {  219,   -2 }, /* (98) db_optr ::= db_optr comp */
  {  219,   -2 }, /* (99) db_optr ::= db_optr prec */
  {  219,   -2 }, /* (100) db_optr ::= db_optr keep */
  {  219,   -2 }, /* (101) db_optr ::= db_optr update */
  {  219,   -2 }, /* (102) db_optr ::= db_optr cachelast */
  {  216,    0 }, /* (103) alter_db_optr ::= */
  {  216,   -2 }, /* (104) alter_db_optr ::= alter_db_optr replica */
  {  216,   -2 }, /* (105) alter_db_optr ::= alter_db_optr quorum */
  {  216,   -2 }, /* (106) alter_db_optr ::= alter_db_optr keep */
  {  216,   -2 }, /* (107) alter_db_optr ::= alter_db_optr blocks */
  {  216,   -2 }, /* (108) alter_db_optr ::= alter_db_optr comp */
  {  216,   -2 }, /* (109) alter_db_optr ::= alter_db_optr wal */
  {  216,   -2 }, /* (110) alter_db_optr ::= alter_db_optr fsync */
  {  216,   -2 }, /* (111) alter_db_optr ::= alter_db_optr update */
  {  216,   -2 }, /* (112) alter_db_optr ::= alter_db_optr cachelast */
  {  245,   -1 }, /* (113) typename ::= ids */
  {  245,   -4 }, /* (114) typename ::= ids LP signed RP */
  {  245,   -2 }, /* (115) typename ::= ids UNSIGNED */
  {  246,   -1 }, /* (116) signed ::= INTEGER */
  {  246,   -2 }, /* (117) signed ::= PLUS INTEGER */
  {  246,   -2 }, /* (118) signed ::= MINUS INTEGER */
  {  211,   -3 }, /* (119) cmd ::= CREATE TABLE create_table_args */
  {  211,   -3 }, /* (120) cmd ::= CREATE TABLE create_table_list */
  {  248,   -1 }, /* (121) create_table_list ::= create_from_stable */
  {  248,   -2 }, /* (122) create_table_list ::= create_table_list create_from_stable */
  {  247,   -6 }, /* (123) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  247,  -10 }, /* (124) create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  249,  -10 }, /* (125) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  247,   -5 }, /* (126) create_table_args ::= ifnotexists ids cpxName AS select */
  {  250,   -3 }, /* (127) columnlist ::= columnlist COMMA column */
  {  250,   -1 }, /* (128) columnlist ::= column */
  {  252,   -2 }, /* (129) column ::= ids typename */
  {  230,   -3 }, /* (130) tagitemlist ::= tagitemlist COMMA tagitem */
  {  230,   -1 }, /* (131) tagitemlist ::= tagitem */
  {  253,   -1 }, /* (132) tagitem ::= INTEGER */
  {  253,   -1 }, /* (133) tagitem ::= FLOAT */
  {  253,   -1 }, /* (134) tagitem ::= STRING */
  {  253,   -1 }, /* (135) tagitem ::= BOOL */
  {  253,   -1 }, /* (136) tagitem ::= NULL */
  {  253,   -2 }, /* (137) tagitem ::= MINUS INTEGER */
  {  253,   -2 }, /* (138) tagitem ::= MINUS FLOAT */
  {  253,   -2 }, /* (139) tagitem ::= PLUS INTEGER */
  {  253,   -2 }, /* (140) tagitem ::= PLUS FLOAT */
  {  251,  -12 }, /* (141) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  265,   -1 }, /* (142) union ::= select */
  {  265,   -3 }, /* (143) union ::= LP union RP */
  {  265,   -4 }, /* (144) union ::= union UNION ALL select */
  {  265,   -6 }, /* (145) union ::= union UNION ALL LP select RP */
  {  211,   -1 }, /* (146) cmd ::= union */
  {  251,   -2 }, /* (147) select ::= SELECT selcollist */
  {  266,   -2 }, /* (148) sclp ::= selcollist COMMA */
  {  266,    0 }, /* (149) sclp ::= */
  {  254,   -3 }, /* (150) selcollist ::= sclp expr as */
  {  254,   -2 }, /* (151) selcollist ::= sclp STAR */
  {  268,   -2 }, /* (152) as ::= AS ids */
  {  268,   -1 }, /* (153) as ::= ids */
  {  268,    0 }, /* (154) as ::= */
  {  255,   -2 }, /* (155) from ::= FROM tablelist */
  {  269,   -2 }, /* (156) tablelist ::= ids cpxName */
  {  269,   -3 }, /* (157) tablelist ::= ids cpxName ids */
  {  269,   -4 }, /* (158) tablelist ::= tablelist COMMA ids cpxName */
  {  269,   -5 }, /* (159) tablelist ::= tablelist COMMA ids cpxName ids */
  {  270,   -1 }, /* (160) tmvar ::= VARIABLE */
  {  257,   -4 }, /* (161) interval_opt ::= INTERVAL LP tmvar RP */
  {  257,   -6 }, /* (162) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  257,    0 }, /* (163) interval_opt ::= */
  {  258,    0 }, /* (164) fill_opt ::= */
  {  258,   -6 }, /* (165) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  258,   -4 }, /* (166) fill_opt ::= FILL LP ID RP */
  {  259,   -4 }, /* (167) sliding_opt ::= SLIDING LP tmvar RP */
  {  259,    0 }, /* (168) sliding_opt ::= */
  {  261,    0 }, /* (169) orderby_opt ::= */
  {  261,   -3 }, /* (170) orderby_opt ::= ORDER BY sortlist */
  {  271,   -4 }, /* (171) sortlist ::= sortlist COMMA item sortorder */
  {  271,   -2 }, /* (172) sortlist ::= item sortorder */
  {  273,   -2 }, /* (173) item ::= ids cpxName */
  {  274,   -1 }, /* (174) sortorder ::= ASC */
  {  274,   -1 }, /* (175) sortorder ::= DESC */
  {  274,    0 }, /* (176) sortorder ::= */
  {  260,    0 }, /* (177) groupby_opt ::= */
  {  260,   -3 }, /* (178) groupby_opt ::= GROUP BY grouplist */
  {  275,   -3 }, /* (179) grouplist ::= grouplist COMMA item */
  {  275,   -1 }, /* (180) grouplist ::= item */
  {  262,    0 }, /* (181) having_opt ::= */
  {  262,   -2 }, /* (182) having_opt ::= HAVING expr */
  {  264,    0 }, /* (183) limit_opt ::= */
  {  264,   -2 }, /* (184) limit_opt ::= LIMIT signed */
  {  264,   -4 }, /* (185) limit_opt ::= LIMIT signed OFFSET signed */
  {  264,   -4 }, /* (186) limit_opt ::= LIMIT signed COMMA signed */
  {  263,    0 }, /* (187) slimit_opt ::= */
  {  263,   -2 }, /* (188) slimit_opt ::= SLIMIT signed */
  {  263,   -4 }, /* (189) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  263,   -4 }, /* (190) slimit_opt ::= SLIMIT signed COMMA signed */
  {  256,    0 }, /* (191) where_opt ::= */
  {  256,   -2 }, /* (192) where_opt ::= WHERE expr */
  {  267,   -3 }, /* (193) expr ::= LP expr RP */
  {  267,   -1 }, /* (194) expr ::= ID */
  {  267,   -3 }, /* (195) expr ::= ID DOT ID */
  {  267,   -3 }, /* (196) expr ::= ID DOT STAR */
  {  267,   -1 }, /* (197) expr ::= INTEGER */
  {  267,   -2 }, /* (198) expr ::= MINUS INTEGER */
  {  267,   -2 }, /* (199) expr ::= PLUS INTEGER */
  {  267,   -1 }, /* (200) expr ::= FLOAT */
  {  267,   -2 }, /* (201) expr ::= MINUS FLOAT */
  {  267,   -2 }, /* (202) expr ::= PLUS FLOAT */
  {  267,   -1 }, /* (203) expr ::= STRING */
  {  267,   -1 }, /* (204) expr ::= NOW */
  {  267,   -1 }, /* (205) expr ::= VARIABLE */
  {  267,   -1 }, /* (206) expr ::= BOOL */
  {  267,   -4 }, /* (207) expr ::= ID LP exprlist RP */
  {  267,   -4 }, /* (208) expr ::= ID LP STAR RP */
  {  267,   -3 }, /* (209) expr ::= expr IS NULL */
  {  267,   -4 }, /* (210) expr ::= expr IS NOT NULL */
  {  267,   -3 }, /* (211) expr ::= expr LT expr */
  {  267,   -3 }, /* (212) expr ::= expr GT expr */
  {  267,   -3 }, /* (213) expr ::= expr LE expr */
  {  267,   -3 }, /* (214) expr ::= expr GE expr */
  {  267,   -3 }, /* (215) expr ::= expr NE expr */
  {  267,   -3 }, /* (216) expr ::= expr EQ expr */
  {  267,   -3 }, /* (217) expr ::= expr AND expr */
  {  267,   -3 }, /* (218) expr ::= expr OR expr */
  {  267,   -3 }, /* (219) expr ::= expr PLUS expr */
  {  267,   -3 }, /* (220) expr ::= expr MINUS expr */
  {  267,   -3 }, /* (221) expr ::= expr STAR expr */
  {  267,   -3 }, /* (222) expr ::= expr SLASH expr */
  {  267,   -3 }, /* (223) expr ::= expr REM expr */
  {  267,   -3 }, /* (224) expr ::= expr LIKE expr */
  {  267,   -5 }, /* (225) expr ::= expr IN LP exprlist RP */
  {  276,   -3 }, /* (226) exprlist ::= exprlist COMMA expritem */
  {  276,   -1 }, /* (227) exprlist ::= expritem */
  {  277,   -1 }, /* (228) expritem ::= expr */
  {  277,    0 }, /* (229) expritem ::= */
  {  211,   -3 }, /* (230) cmd ::= RESET QUERY CACHE */
  {  211,   -7 }, /* (231) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  211,   -7 }, /* (232) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  211,   -7 }, /* (233) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  211,   -7 }, /* (234) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  211,   -8 }, /* (235) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  211,   -9 }, /* (236) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  211,   -3 }, /* (237) cmd ::= KILL CONNECTION INTEGER */
  {  211,   -5 }, /* (238) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  211,   -5 }, /* (239) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno,       /* Number of the rule by which to reduce */
  int yyLookahead,             /* Lookahead token, or YYNOCODE if none */
  ParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
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
      return;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        return;
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
      case 119: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==119);
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
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 29: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 30: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 31: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 32: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 33: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 34: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 35: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 38: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 39: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy234, &t);}
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy71);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy71);}
        break;
      case 43: /* ids ::= ID */
      case 44: /* ids ::= STRING */ yytestcase(yyruleno==44);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 45: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 46: /* ifexists ::= */
      case 48: /* ifnotexists ::= */ yytestcase(yyruleno==48);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 47: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 49: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 50: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy71);}
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy234, &yymsp[-2].minor.yy0);}
        break;
      case 52: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 53: /* pps ::= */
      case 55: /* tseries ::= */ yytestcase(yyruleno==55);
      case 57: /* dbs ::= */ yytestcase(yyruleno==57);
      case 59: /* streams ::= */ yytestcase(yyruleno==59);
      case 61: /* storage ::= */ yytestcase(yyruleno==61);
      case 63: /* qtime ::= */ yytestcase(yyruleno==63);
      case 65: /* users ::= */ yytestcase(yyruleno==65);
      case 67: /* conns ::= */ yytestcase(yyruleno==67);
      case 69: /* state ::= */ yytestcase(yyruleno==69);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 54: /* pps ::= PPS INTEGER */
      case 56: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==56);
      case 58: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==58);
      case 60: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==60);
      case 62: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==62);
      case 64: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==64);
      case 66: /* users ::= USERS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* state ::= STATE ids */ yytestcase(yyruleno==70);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 71: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy71.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy71.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy71.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy71.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy71.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy71.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy71.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy71.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy71.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy71 = yylhsminor.yy71;
        break;
      case 72: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy421 = yymsp[0].minor.yy421; }
        break;
      case 73: /* cache ::= CACHE INTEGER */
      case 74: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==74);
      case 75: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==75);
      case 76: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==79);
      case 80: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==80);
      case 81: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==81);
      case 82: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==82);
      case 83: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==83);
      case 84: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==84);
      case 85: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==85);
      case 86: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==86);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 87: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy234);}
        break;
      case 88: /* db_optr ::= db_optr cache */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 89: /* db_optr ::= db_optr replica */
      case 104: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==104);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 90: /* db_optr ::= db_optr quorum */
      case 105: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==105);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 91: /* db_optr ::= db_optr days */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 92: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 93: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 94: /* db_optr ::= db_optr blocks */
      case 107: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==107);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 95: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 96: /* db_optr ::= db_optr wal */
      case 109: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==109);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 97: /* db_optr ::= db_optr fsync */
      case 110: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==110);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 98: /* db_optr ::= db_optr comp */
      case 108: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==108);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 99: /* db_optr ::= db_optr prec */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 100: /* db_optr ::= db_optr keep */
      case 106: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==106);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.keep = yymsp[0].minor.yy421; }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 101: /* db_optr ::= db_optr update */
      case 111: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==111);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 102: /* db_optr ::= db_optr cachelast */
      case 112: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==112);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 103: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy234);}
        break;
      case 113: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy183, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy183 = yylhsminor.yy183;
        break;
      case 114: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy325 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy325;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy183 = yylhsminor.yy183;
        break;
      case 115: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy183, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 116: /* signed ::= INTEGER */
{ yylhsminor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 117: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 118: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy325 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 120: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy38;}
        break;
      case 121: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy152);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy38 = pCreateTable;
}
  yymsp[0].minor.yy38 = yylhsminor.yy38;
        break;
      case 122: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy38->childTableInfo, &yymsp[0].minor.yy152);
  yylhsminor.yy38 = yymsp[-1].minor.yy38;
}
  yymsp[-1].minor.yy38 = yylhsminor.yy38;
        break;
      case 123: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy38 = tSetCreateSqlElems(yymsp[-1].minor.yy421, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy38 = yylhsminor.yy38;
        break;
      case 124: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy38 = tSetCreateSqlElems(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy38 = yylhsminor.yy38;
        break;
      case 125: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy421, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy152 = yylhsminor.yy152;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy38 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy148, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy38 = yylhsminor.yy38;
        break;
      case 127: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy183); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 128: /* columnlist ::= column */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy183);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 129: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy183, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);
}
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 130: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);    }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 131: /* tagitemlist ::= tagitem */
{ yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 132: /* tagitem ::= INTEGER */
      case 133: /* tagitem ::= FLOAT */ yytestcase(yyruleno==133);
      case 134: /* tagitem ::= STRING */ yytestcase(yyruleno==134);
      case 135: /* tagitem ::= BOOL */ yytestcase(yyruleno==135);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 136: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 137: /* tagitem ::= MINUS INTEGER */
      case 138: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==138);
      case 139: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==139);
      case 140: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==140);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 141: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy148 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy166, yymsp[-9].minor.yy421, yymsp[-8].minor.yy78, yymsp[-4].minor.yy421, yymsp[-3].minor.yy421, &yymsp[-7].minor.yy400, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy421, &yymsp[0].minor.yy167, &yymsp[-1].minor.yy167);
}
  yymsp[-11].minor.yy148 = yylhsminor.yy148;
        break;
      case 142: /* union ::= select */
{ yylhsminor.yy153 = setSubclause(NULL, yymsp[0].minor.yy148); }
  yymsp[0].minor.yy153 = yylhsminor.yy153;
        break;
      case 143: /* union ::= LP union RP */
{ yymsp[-2].minor.yy153 = yymsp[-1].minor.yy153; }
        break;
      case 144: /* union ::= union UNION ALL select */
{ yylhsminor.yy153 = appendSelectClause(yymsp[-3].minor.yy153, yymsp[0].minor.yy148); }
  yymsp[-3].minor.yy153 = yylhsminor.yy153;
        break;
      case 145: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy153 = appendSelectClause(yymsp[-5].minor.yy153, yymsp[-1].minor.yy148); }
  yymsp[-5].minor.yy153 = yylhsminor.yy153;
        break;
      case 146: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy153, NULL, TSDB_SQL_SELECT); }
        break;
      case 147: /* select ::= SELECT selcollist */
{
  yylhsminor.yy148 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy166, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 148: /* sclp ::= selcollist COMMA */
{yylhsminor.yy166 = yymsp[-1].minor.yy166;}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 149: /* sclp ::= */
{yymsp[1].minor.yy166 = 0;}
        break;
      case 150: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy166 = tSqlExprListAppend(yymsp[-2].minor.yy166, yymsp[-1].minor.yy78, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 151: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy166 = tSqlExprListAppend(yymsp[-1].minor.yy166, pNode, 0);
}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 152: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 153: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 154: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 155: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 156: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 157: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 158: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 159: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(yymsp[-4].minor.yy421, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy421 = yylhsminor.yy421;
        break;
      case 160: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 161: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy400.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy400.offset.n = 0; yymsp[-3].minor.yy400.offset.z = NULL; yymsp[-3].minor.yy400.offset.type = 0;}
        break;
      case 162: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy400.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy400.offset = yymsp[-1].minor.yy0;}
        break;
      case 163: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 164: /* fill_opt ::= */
{yymsp[1].minor.yy421 = 0;     }
        break;
      case 165: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy421, &A, -1, 0);
    yymsp[-5].minor.yy421 = yymsp[-1].minor.yy421;
}
        break;
      case 166: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy421 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 167: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 168: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 169: /* orderby_opt ::= */
{yymsp[1].minor.yy421 = 0;}
        break;
      case 170: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 171: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy421 = tVariantListAppend(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 172: /* sortlist ::= item sortorder */
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 173: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 174: /* sortorder ::= ASC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 175: /* sortorder ::= DESC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_DESC;}
        break;
      case 176: /* sortorder ::= */
{ yymsp[1].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 177: /* groupby_opt ::= */
{ yymsp[1].minor.yy421 = 0;}
        break;
      case 178: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 179: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 180: /* grouplist ::= item */
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1);
}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 181: /* having_opt ::= */
      case 191: /* where_opt ::= */ yytestcase(yyruleno==191);
      case 229: /* expritem ::= */ yytestcase(yyruleno==229);
{yymsp[1].minor.yy78 = 0;}
        break;
      case 182: /* having_opt ::= HAVING expr */
      case 192: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==192);
{yymsp[-1].minor.yy78 = yymsp[0].minor.yy78;}
        break;
      case 183: /* limit_opt ::= */
      case 187: /* slimit_opt ::= */ yytestcase(yyruleno==187);
{yymsp[1].minor.yy167.limit = -1; yymsp[1].minor.yy167.offset = 0;}
        break;
      case 184: /* limit_opt ::= LIMIT signed */
      case 188: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==188);
{yymsp[-1].minor.yy167.limit = yymsp[0].minor.yy325;  yymsp[-1].minor.yy167.offset = 0;}
        break;
      case 185: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy167.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[0].minor.yy325;}
        break;
      case 186: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy167.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[-2].minor.yy325;}
        break;
      case 189: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy167.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[0].minor.yy325;}
        break;
      case 190: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy167.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[-2].minor.yy325;}
        break;
      case 193: /* expr ::= LP expr RP */
{yylhsminor.yy78 = yymsp[-1].minor.yy78; yylhsminor.yy78->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy78->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 194: /* expr ::= ID */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 195: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 196: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 197: /* expr ::= INTEGER */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 198: /* expr ::= MINUS INTEGER */
      case 199: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==199);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy78 = yylhsminor.yy78;
        break;
      case 200: /* expr ::= FLOAT */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 201: /* expr ::= MINUS FLOAT */
      case 202: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==202);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy78 = yylhsminor.yy78;
        break;
      case 203: /* expr ::= STRING */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 204: /* expr ::= NOW */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 205: /* expr ::= VARIABLE */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 206: /* expr ::= BOOL */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 207: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy78 = tSqlExprCreateFunction(yymsp[-1].minor.yy166, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy78 = yylhsminor.yy78;
        break;
      case 208: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy78 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy78 = yylhsminor.yy78;
        break;
      case 209: /* expr ::= expr IS NULL */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 210: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-3].minor.yy78, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy78 = yylhsminor.yy78;
        break;
      case 211: /* expr ::= expr LT expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_LT);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 212: /* expr ::= expr GT expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_GT);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 213: /* expr ::= expr LE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_LE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 214: /* expr ::= expr GE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_GE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 215: /* expr ::= expr NE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_NE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 216: /* expr ::= expr EQ expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_EQ);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 217: /* expr ::= expr AND expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_AND);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 218: /* expr ::= expr OR expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_OR); }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 219: /* expr ::= expr PLUS expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_PLUS);  }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 220: /* expr ::= expr MINUS expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_MINUS); }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 221: /* expr ::= expr STAR expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_STAR);  }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 222: /* expr ::= expr SLASH expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_DIVIDE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 223: /* expr ::= expr REM expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_REM);   }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 224: /* expr ::= expr LIKE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_LIKE);  }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 225: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-4].minor.yy78, (tSQLExpr*)yymsp[-1].minor.yy166, TK_IN); }
  yymsp[-4].minor.yy78 = yylhsminor.yy78;
        break;
      case 226: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy166 = tSqlExprListAppend(yymsp[-2].minor.yy166,yymsp[0].minor.yy78,0);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 227: /* exprlist ::= expritem */
{yylhsminor.yy166 = tSqlExprListAppend(0,yymsp[0].minor.yy78,0);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 228: /* expritem ::= expr */
{yylhsminor.yy78 = yymsp[0].minor.yy78;}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 230: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 231: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 232: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 233: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 234: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 235: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy430, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 237: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 238: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 239: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
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
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
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
  ParseARG_FETCH;
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
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
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
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
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
  unsigned int yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  yypParser = (yyParser*)yyp;
  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    int stateno = yypParser->yytos->stateno;
    if( stateno < YY_MIN_REDUCE ){
      fprintf(yyTraceFILE,"%sInput '%s' in state %d\n",
              yyTracePrompt,yyTokenName[yymajor],stateno);
    }else{
      fprintf(yyTraceFILE,"%sInput '%s' with pending reduce %d\n",
              yyTracePrompt,yyTokenName[yymajor],stateno-YY_MIN_REDUCE);
    }
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact >= YY_MIN_REDUCE ){
      yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,yyminor);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      yymajor = YYNOCODE;
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
            && yymx != YYERRORSYMBOL
            && (yyact = yy_find_reduce_action(
                        yypParser->yytos->stateno,
                        YYERRORSYMBOL)) >= YY_MIN_REDUCE
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
      yymajor = YYNOCODE;
      
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
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yytos>yypParser->yystack );
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
