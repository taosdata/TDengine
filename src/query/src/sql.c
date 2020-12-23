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
#define YYNOCODE 278
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SQuerySQL* yy4;
  SSubclauseInfo* yy13;
  int yy70;
  SCreatedTableInfo yy84;
  SIntervalVal yy222;
  TAOS_FIELD yy363;
  tSQLExprList* yy382;
  int64_t yy387;
  SArray* yy403;
  SLimitVal yy404;
  SCreateTableSQL* yy436;
  SCreateAcctSQL yy463;
  SCreateDBInfo yy478;
  tVariant yy488;
  tSQLExpr* yy522;
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
#define YYNRULE              239
#define YYNTOKEN             208
#define YY_MAX_SHIFT         257
#define YY_MIN_SHIFTREDUCE   430
#define YY_MAX_SHIFTREDUCE   668
#define YY_ERROR_ACTION      669
#define YY_ACCEPT_ACTION     670
#define YY_NO_ACTION         671
#define YY_MIN_REDUCE        672
#define YY_MAX_REDUCE        910
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
#define YY_ACTTAB_COUNT (585)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   143,  473,  143,   23,  670,  257,  165,  545,  824,  474,
 /*    10 */   897,  168,  898,   37,   38,   12,   39,   40,  813,   23,
 /*    20 */   173,   31,  473,  473,  209,   43,   41,   45,   42,  802,
 /*    30 */   474,  474,  163,   36,   35,  231,  230,   34,   33,   32,
 /*    40 */    37,   38,  798,   39,   40,  813,  105,  173,   31,  162,
 /*    50 */   255,  209,   43,   41,   45,   42,  176,  178,  799,  194,
 /*    60 */    36,   35,  233,  821,   34,   33,   32,  431,  432,  433,
 /*    70 */   434,  435,  436,  437,  438,  439,  440,  441,  442,  256,
 /*    80 */   802,  143,  184,   63,  179,   37,   38,  224,   39,   40,
 /*    90 */   167,  898,  173,   31,  800,   29,  209,   43,   41,   45,
 /*   100 */    42,  110,  197,  791,   57,   36,   35,  251,  210,   34,
 /*   110 */    33,   32,  110,   17,  222,  250,  249,  221,  220,  219,
 /*   120 */   248,  218,  247,  246,  245,  217,  244,  243,  622,  772,
 /*   130 */   802,  760,  761,  762,  763,  764,  765,  766,  767,  768,
 /*   140 */   769,  770,  771,  773,  774,  242,   38,  180,   39,   40,
 /*   150 */   228,  227,  173,   31,  110,   18,  209,   43,   41,   45,
 /*   160 */    42,  851,   28,  204,  110,   36,   35,   23,  187,   34,
 /*   170 */    33,   32,  850,   39,   40,  191,  190,  173,   31,  224,
 /*   180 */   624,  209,   43,   41,   45,   42,   34,   33,   32,    9,
 /*   190 */    36,   35,   65,  120,   34,   33,   32,  172,  635,  638,
 /*   200 */    66,  626,  104,  629,  177,  632,  799,  172,  635,   28,
 /*   210 */    13,  626,  206,  629,   61,  632,  625,  172,  635,  142,
 /*   220 */   572,  626,   23,  629,   62,  632,  155,  196,  147,  169,
 /*   230 */   170,  793,  156,  208,  603,  604,   92,   91,  150,  169,
 /*   240 */   170,  894,  713,  580,   17,  133,  250,  249,  893,  169,
 /*   250 */   170,  248,   64,  247,  246,  245,  892,  244,  243,  229,
 /*   260 */   778,  799,   80,  776,  777,  590,   18,  242,  779,  107,
 /*   270 */   781,  782,  780,   28,  783,  784,   43,   41,   45,   42,
 /*   280 */   159,  790,   22,  792,   36,   35,  160,  171,   34,   33,
 /*   290 */    32,  722,  564,  193,  133,  561,   44,  562,   52,  563,
 /*   300 */   158,  254,  253,   98,   36,   35,   44,  634,   34,   33,
 /*   310 */    32,   23,  145,    3,  124,   53,   44,  634,  146,   72,
 /*   320 */    68,   71,  633,  181,  182,  148,  714,  634,    4,  133,
 /*   330 */    78,   82,  633,  137,  135,  149,   87,   90,   81,   95,
 /*   340 */    94,   93,  633,  153,   84,  594,  577,  154,  234,   48,
 /*   350 */   799,   19,   49,  152,  595,  654,  636,  141,   15,   14,
 /*   360 */    14,  628,  627,  631,  630,  553,  212,  151,  554,   24,
 /*   370 */    24,   50,   48,   77,   76,   11,   10,  568,  566,  569,
 /*   380 */   567,   89,   88,  103,  101,  907,  144,  801,  861,  860,
 /*   390 */   174,  857,  856,  175,  823,  232,  565,  828,  830,  106,
 /*   400 */   843,  815,  842,  121,  122,   28,  119,  123,  195,  724,
 /*   410 */   216,  139,   26,  225,  721,  226,  906,   74,  102,  905,
 /*   420 */   903,  125,  742,   27,   25,  140,  711,   83,  589,  709,
 /*   430 */    85,   86,  707,  706,  183,  134,  704,  703,  702,  701,
 /*   440 */   198,  700,  136,  698,  696,  694,  692,  690,  138,  164,
 /*   450 */    58,   54,   59,  202,   51,  844,   46,  812,  207,  205,
 /*   460 */   203,  201,  199,   30,   79,  235,  236,  237,  238,  239,
 /*   470 */   240,  161,  214,  241,  252,  215,  668,  186,  185,  157,
 /*   480 */    69,  667,  188,  189,  666,  659,  192,  196,  166,  574,
 /*   490 */   705,  591,   56,   96,  132,  743,  126,  128,  127,  129,
 /*   500 */   130,  699,  111,  131,    1,   97,  116,  112,  113,  114,
 /*   510 */   691,  797,   60,  117,  115,  118,    2,   20,  108,  200,
 /*   520 */     6,  596,  109,    5,    7,  637,   21,    8,  211,   16,
 /*   530 */   213,  639,   67,   65,  514,  510,  508,  507,  506,  503,
 /*   540 */   477,  223,   70,   47,   73,   75,   24,  547,  546,  544,
 /*   550 */    55,  498,  496,  488,  494,  490,  492,  486,  484,  516,
 /*   560 */   515,  513,  512,  511,  509,  505,  504,   48,  475,  446,
 /*   570 */   444,  672,  671,  671,  671,  671,  671,  671,  671,  671,
 /*   580 */   671,  671,  671,   99,  100,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   266,    1,  266,  212,  209,  210,  229,    5,  212,    9,
 /*    10 */   276,  275,  276,   13,   14,  266,   16,   17,  250,  212,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  252,
 /*    30 */     9,    9,  264,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  251,   16,   17,  250,  212,   20,   21,  211,
 /*    50 */   212,   24,   25,   26,   27,   28,  249,  229,  251,  264,
 /*    60 */    33,   34,  212,  267,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */   252,  266,   60,  253,   66,   13,   14,   76,   16,   17,
 /*    90 */   275,  276,   20,   21,  244,  265,   24,   25,   26,   27,
 /*   100 */    28,  212,  268,    0,  104,   33,   34,  229,   15,   37,
 /*   110 */    38,   39,  212,   85,   86,   87,   88,   89,   90,   91,
 /*   120 */    92,   93,   94,   95,   96,   97,   98,   99,  101,  228,
 /*   130 */   252,  230,  231,  232,  233,  234,  235,  236,  237,  238,
 /*   140 */   239,  240,  241,  242,  243,   78,   14,  129,   16,   17,
 /*   150 */   132,  133,   20,   21,  212,  100,   24,   25,   26,   27,
 /*   160 */    28,  272,  107,  274,  212,   33,   34,  212,  128,   37,
 /*   170 */    38,   39,  272,   16,   17,  135,  136,   20,   21,   76,
 /*   180 */     1,   24,   25,   26,   27,   28,   37,   38,   39,  100,
 /*   190 */    33,   34,  103,  104,   37,   38,   39,    1,    2,  106,
 /*   200 */   217,    5,  100,    7,  249,    9,  251,    1,    2,  107,
 /*   210 */    44,    5,  270,    7,  272,    9,   37,    1,    2,  266,
 /*   220 */   101,    5,  212,    7,  272,    9,   60,  108,  266,   33,
 /*   230 */    34,  248,   66,   37,  117,  118,   70,   71,   72,   33,
 /*   240 */    34,  266,  216,   37,   85,  219,   87,   88,  266,   33,
 /*   250 */    34,   92,  217,   94,   95,   96,  266,   98,   99,  249,
 /*   260 */   228,  251,   73,  231,  232,  101,  100,   78,  236,  105,
 /*   270 */   238,  239,  240,  107,  242,  243,   25,   26,   27,   28,
 /*   280 */   266,  246,  247,  248,   33,   34,  266,   59,   37,   38,
 /*   290 */    39,  216,    2,  127,  219,    5,  100,    7,  105,    9,
 /*   300 */   134,   63,   64,   65,   33,   34,  100,  111,   37,   38,
 /*   310 */    39,  212,  266,   61,   62,  122,  100,  111,  266,   67,
 /*   320 */    68,   69,  126,   33,   34,  266,  216,  111,  100,  219,
 /*   330 */    61,   62,  126,   61,   62,  266,   67,   68,   69,   67,
 /*   340 */    68,   69,  126,  266,   75,  101,  105,  266,  249,  105,
 /*   350 */   251,  110,  105,  266,  101,  101,  101,  266,  105,  105,
 /*   360 */   105,    5,    5,    7,    7,  101,  101,  266,  101,  105,
 /*   370 */   105,  124,  105,  130,  131,  130,  131,    5,    5,    7,
 /*   380 */     7,   73,   74,   61,   62,  252,  266,  252,  245,  245,
 /*   390 */   245,  245,  245,  245,  212,  245,  106,  212,  212,  212,
 /*   400 */   273,  250,  273,  212,  212,  107,  254,  212,  250,  212,
 /*   410 */   212,  212,  212,  212,  212,  212,  212,  212,   59,  212,
 /*   420 */   212,  212,  212,  212,  212,  212,  212,  212,  111,  212,
 /*   430 */   212,  212,  212,  212,  212,  212,  212,  212,  212,  212,
 /*   440 */   269,  212,  212,  212,  212,  212,  212,  212,  212,  269,
 /*   450 */   213,  121,  213,  269,  123,  213,  120,  263,  115,  119,
 /*   460 */   114,  113,  112,  125,   84,   83,   49,   80,   82,   53,
 /*   470 */    81,  213,  213,   79,   76,  213,    5,    5,  137,  213,
 /*   480 */   217,    5,  137,    5,    5,   86,  128,  108,    1,  101,
 /*   490 */   213,  101,  109,  214,  220,  227,  226,  221,  225,  224,
 /*   500 */   222,  213,  262,  223,  218,  214,  257,  261,  260,  259,
 /*   510 */   213,  250,  105,  256,  258,  255,  215,  105,  100,  100,
 /*   520 */   116,  101,  100,  100,  116,  101,  105,  100,  102,  100,
 /*   530 */   102,  106,   73,  103,    9,    5,    5,    5,    5,    5,
 /*   540 */    77,   15,   73,   16,  131,  131,  105,    5,    5,  101,
 /*   550 */   100,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   560 */     5,    5,    5,    5,    5,    5,    5,  105,   77,   59,
 /*   570 */    58,    0,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   580 */   277,  277,  277,   21,   21,  277,  277,  277,  277,  277,
 /*   590 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   600 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   610 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   620 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   630 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   640 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   650 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   660 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   670 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   680 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   690 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   700 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   710 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   720 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   730 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   740 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   750 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   760 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   770 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   780 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   790 */   277,  277,  277,
};
#define YY_SHIFT_COUNT    (257)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (571)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   166,   28,  159,   11,  196,  216,   21,   21,   21,   21,
 /*    10 */    21,   21,    0,   22,  216,  290,  290,  290,   55,   21,
 /*    20 */    21,   21,  103,   21,   21,  189,   67,   67,  585,  206,
 /*    30 */   216,  216,  216,  216,  216,  216,  216,  216,  216,  216,
 /*    40 */   216,  216,  216,  216,  216,  216,  216,  290,  290,    2,
 /*    50 */     2,    2,    2,    2,    2,    2,  102,   21,   21,   21,
 /*    60 */    21,  117,  117,  241,   21,   21,   21,   21,   21,   21,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,  298,  359,  359,  317,  317,  317,
 /*   110 */   359,  330,  331,  336,  343,  340,  346,  348,  350,  338,
 /*   120 */   298,  359,  359,  359,   11,  359,  380,  382,  417,  387,
 /*   130 */   386,  416,  389,  394,  359,  398,  359,  398,  359,  585,
 /*   140 */   585,   27,   72,   72,   72,  132,  157,  251,  251,  251,
 /*   150 */   269,  271,  271,  271,  271,  252,  272,   18,   40,  149,
 /*   160 */   149,   89,  238,  119,  164,  244,  253,  254,  255,  356,
 /*   170 */   357,  179,  228,   93,  247,  193,  264,  265,  267,  243,
 /*   180 */   245,  372,  373,  308,  322,  471,  341,  472,  476,  345,
 /*   190 */   478,  479,  399,  358,  379,  388,  383,  407,  390,  418,
 /*   200 */   487,  419,  420,  422,  412,  404,  421,  408,  424,  423,
 /*   210 */   425,  427,  426,  429,  428,  430,  459,  525,  530,  531,
 /*   220 */   532,  533,  534,  463,  526,  469,  527,  413,  414,  441,
 /*   230 */   542,  543,  448,  450,  441,  546,  547,  548,  549,  550,
 /*   240 */   551,  552,  553,  554,  555,  556,  557,  558,  559,  560,
 /*   250 */   561,  462,  491,  562,  563,  510,  512,  571,
};
#define YY_REDUCE_COUNT (140)
#define YY_REDUCE_MIN   (-266)
#define YY_REDUCE_MAX   (301)
static const short yy_reduce_ofst[] = {
 /*     0 */  -205,  -99,   32,   35, -264, -185, -111,  -58, -193,  -45,
 /*    10 */    10,   99, -204, -162, -266, -223, -172, -122, -232, -166,
 /*    20 */  -100,  -48,  -17, -150, -209,   26,   75,  110, -170, -251,
 /*    30 */   -47,  -38,  -25,  -18,  -10,   14,   20,   46,   52,   59,
 /*    40 */    69,   77,   81,   87,   91,  101,  120,  133,  135,  143,
 /*    50 */   144,  145,  146,  147,  148,  150,  151,  182,  185,  186,
 /*    60 */   187,  127,  129,  152,  191,  192,  195,  197,  198,  199,
 /*    70 */   200,  201,  202,  203,  204,  205,  207,  208,  209,  210,
 /*    80 */   211,  212,  213,  214,  215,  217,  218,  219,  220,  221,
 /*    90 */   222,  223,  224,  225,  226,  227,  229,  230,  231,  232,
 /*   100 */   233,  234,  235,  236,  158,  237,  239,  171,  180,  184,
 /*   110 */   242,  194,  240,  246,  248,  250,  256,  249,  257,  260,
 /*   120 */   261,  258,  259,  262,  263,  266,  268,  270,  273,  276,
 /*   130 */   275,  278,  280,  274,  277,  279,  288,  291,  297,  286,
 /*   140 */   301,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   669,  723,  712,  720,  900,  900,  669,  669,  669,  669,
 /*    10 */   669,  669,  825,  687,  900,  669,  669,  669,  669,  669,
 /*    20 */   669,  669,  720,  669,  669,  725,  725,  725,  820,  669,
 /*    30 */   669,  669,  669,  669,  669,  669,  669,  669,  669,  669,
 /*    40 */   669,  669,  669,  669,  669,  669,  669,  669,  669,  669,
 /*    50 */   669,  669,  669,  669,  669,  669,  669,  669,  827,  829,
 /*    60 */   669,  847,  847,  818,  669,  669,  669,  669,  669,  669,
 /*    70 */   669,  669,  669,  669,  669,  669,  669,  669,  669,  669,
 /*    80 */   669,  669,  669,  710,  669,  708,  669,  669,  669,  669,
 /*    90 */   669,  669,  669,  669,  669,  669,  669,  669,  697,  669,
 /*   100 */   669,  669,  669,  669,  669,  689,  689,  669,  669,  669,
 /*   110 */   689,  854,  858,  852,  840,  848,  839,  835,  834,  862,
 /*   120 */   669,  689,  689,  689,  720,  689,  741,  739,  737,  729,
 /*   130 */   735,  731,  733,  727,  689,  718,  689,  718,  689,  759,
 /*   140 */   775,  669,  863,  899,  853,  889,  888,  895,  887,  886,
 /*   150 */   669,  882,  883,  885,  884,  669,  669,  669,  669,  891,
 /*   160 */   890,  669,  669,  669,  669,  669,  669,  669,  669,  669,
 /*   170 */   669,  669,  865,  669,  859,  855,  669,  669,  669,  669,
 /*   180 */   669,  669,  669,  669,  669,  669,  669,  669,  669,  669,
 /*   190 */   669,  669,  669,  669,  817,  669,  669,  826,  669,  669,
 /*   200 */   669,  669,  669,  669,  849,  669,  841,  669,  669,  669,
 /*   210 */   669,  669,  794,  669,  669,  669,  669,  669,  669,  669,
 /*   220 */   669,  669,  669,  669,  669,  669,  669,  669,  669,  904,
 /*   230 */   669,  669,  669,  785,  902,  669,  669,  669,  669,  669,
 /*   240 */   669,  669,  669,  669,  669,  669,  669,  669,  669,  669,
 /*   250 */   669,  744,  669,  695,  693,  669,  685,  669,
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
  /*  102 */ "TAGS",
  /*  103 */ "USING",
  /*  104 */ "AS",
  /*  105 */ "COMMA",
  /*  106 */ "NULL",
  /*  107 */ "SELECT",
  /*  108 */ "UNION",
  /*  109 */ "ALL",
  /*  110 */ "FROM",
  /*  111 */ "VARIABLE",
  /*  112 */ "INTERVAL",
  /*  113 */ "FILL",
  /*  114 */ "SLIDING",
  /*  115 */ "ORDER",
  /*  116 */ "BY",
  /*  117 */ "ASC",
  /*  118 */ "DESC",
  /*  119 */ "GROUP",
  /*  120 */ "HAVING",
  /*  121 */ "LIMIT",
  /*  122 */ "OFFSET",
  /*  123 */ "SLIMIT",
  /*  124 */ "SOFFSET",
  /*  125 */ "WHERE",
  /*  126 */ "NOW",
  /*  127 */ "RESET",
  /*  128 */ "QUERY",
  /*  129 */ "ADD",
  /*  130 */ "COLUMN",
  /*  131 */ "TAG",
  /*  132 */ "CHANGE",
  /*  133 */ "SET",
  /*  134 */ "KILL",
  /*  135 */ "CONNECTION",
  /*  136 */ "STREAM",
  /*  137 */ "COLON",
  /*  138 */ "ABORT",
  /*  139 */ "AFTER",
  /*  140 */ "ATTACH",
  /*  141 */ "BEFORE",
  /*  142 */ "BEGIN",
  /*  143 */ "CASCADE",
  /*  144 */ "CLUSTER",
  /*  145 */ "CONFLICT",
  /*  146 */ "COPY",
  /*  147 */ "DEFERRED",
  /*  148 */ "DELIMITERS",
  /*  149 */ "DETACH",
  /*  150 */ "EACH",
  /*  151 */ "END",
  /*  152 */ "EXPLAIN",
  /*  153 */ "FAIL",
  /*  154 */ "FOR",
  /*  155 */ "IGNORE",
  /*  156 */ "IMMEDIATE",
  /*  157 */ "INITIALLY",
  /*  158 */ "INSTEAD",
  /*  159 */ "MATCH",
  /*  160 */ "KEY",
  /*  161 */ "OF",
  /*  162 */ "RAISE",
  /*  163 */ "REPLACE",
  /*  164 */ "RESTRICT",
  /*  165 */ "ROW",
  /*  166 */ "STATEMENT",
  /*  167 */ "TRIGGER",
  /*  168 */ "VIEW",
  /*  169 */ "COUNT",
  /*  170 */ "SUM",
  /*  171 */ "AVG",
  /*  172 */ "MIN",
  /*  173 */ "MAX",
  /*  174 */ "FIRST",
  /*  175 */ "LAST",
  /*  176 */ "TOP",
  /*  177 */ "BOTTOM",
  /*  178 */ "STDDEV",
  /*  179 */ "PERCENTILE",
  /*  180 */ "APERCENTILE",
  /*  181 */ "LEASTSQUARES",
  /*  182 */ "HISTOGRAM",
  /*  183 */ "DIFF",
  /*  184 */ "SPREAD",
  /*  185 */ "TWA",
  /*  186 */ "INTERP",
  /*  187 */ "LAST_ROW",
  /*  188 */ "RATE",
  /*  189 */ "IRATE",
  /*  190 */ "SUM_RATE",
  /*  191 */ "SUM_IRATE",
  /*  192 */ "AVG_RATE",
  /*  193 */ "AVG_IRATE",
  /*  194 */ "TBID",
  /*  195 */ "SEMI",
  /*  196 */ "NONE",
  /*  197 */ "PREV",
  /*  198 */ "LINEAR",
  /*  199 */ "IMPORT",
  /*  200 */ "METRIC",
  /*  201 */ "TBNAME",
  /*  202 */ "JOIN",
  /*  203 */ "METRICS",
  /*  204 */ "STABLE",
  /*  205 */ "INSERT",
  /*  206 */ "INTO",
  /*  207 */ "VALUES",
  /*  208 */ "error",
  /*  209 */ "program",
  /*  210 */ "cmd",
  /*  211 */ "dbPrefix",
  /*  212 */ "ids",
  /*  213 */ "cpxName",
  /*  214 */ "ifexists",
  /*  215 */ "alter_db_optr",
  /*  216 */ "acct_optr",
  /*  217 */ "ifnotexists",
  /*  218 */ "db_optr",
  /*  219 */ "pps",
  /*  220 */ "tseries",
  /*  221 */ "dbs",
  /*  222 */ "streams",
  /*  223 */ "storage",
  /*  224 */ "qtime",
  /*  225 */ "users",
  /*  226 */ "conns",
  /*  227 */ "state",
  /*  228 */ "keep",
  /*  229 */ "tagitemlist",
  /*  230 */ "cache",
  /*  231 */ "replica",
  /*  232 */ "quorum",
  /*  233 */ "days",
  /*  234 */ "minrows",
  /*  235 */ "maxrows",
  /*  236 */ "blocks",
  /*  237 */ "ctime",
  /*  238 */ "wal",
  /*  239 */ "fsync",
  /*  240 */ "comp",
  /*  241 */ "prec",
  /*  242 */ "update",
  /*  243 */ "cachelast",
  /*  244 */ "typename",
  /*  245 */ "signed",
  /*  246 */ "create_table_args",
  /*  247 */ "create_table_list",
  /*  248 */ "create_from_stable",
  /*  249 */ "columnlist",
  /*  250 */ "select",
  /*  251 */ "column",
  /*  252 */ "tagitem",
  /*  253 */ "selcollist",
  /*  254 */ "from",
  /*  255 */ "where_opt",
  /*  256 */ "interval_opt",
  /*  257 */ "fill_opt",
  /*  258 */ "sliding_opt",
  /*  259 */ "groupby_opt",
  /*  260 */ "orderby_opt",
  /*  261 */ "having_opt",
  /*  262 */ "slimit_opt",
  /*  263 */ "limit_opt",
  /*  264 */ "union",
  /*  265 */ "sclp",
  /*  266 */ "expr",
  /*  267 */ "as",
  /*  268 */ "tablelist",
  /*  269 */ "tmvar",
  /*  270 */ "sortlist",
  /*  271 */ "sortitem",
  /*  272 */ "item",
  /*  273 */ "sortorder",
  /*  274 */ "grouplist",
  /*  275 */ "exprlist",
  /*  276 */ "expritem",
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
 /* 115 */ "signed ::= INTEGER",
 /* 116 */ "signed ::= PLUS INTEGER",
 /* 117 */ "signed ::= MINUS INTEGER",
 /* 118 */ "cmd ::= CREATE TABLE create_table_args",
 /* 119 */ "cmd ::= CREATE TABLE create_table_list",
 /* 120 */ "create_table_list ::= create_from_stable",
 /* 121 */ "create_table_list ::= create_table_list create_from_stable",
 /* 122 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 123 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 124 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 125 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 126 */ "columnlist ::= columnlist COMMA column",
 /* 127 */ "columnlist ::= column",
 /* 128 */ "column ::= ids typename",
 /* 129 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 130 */ "tagitemlist ::= tagitem",
 /* 131 */ "tagitem ::= INTEGER",
 /* 132 */ "tagitem ::= FLOAT",
 /* 133 */ "tagitem ::= STRING",
 /* 134 */ "tagitem ::= BOOL",
 /* 135 */ "tagitem ::= NULL",
 /* 136 */ "tagitem ::= MINUS INTEGER",
 /* 137 */ "tagitem ::= MINUS FLOAT",
 /* 138 */ "tagitem ::= PLUS INTEGER",
 /* 139 */ "tagitem ::= PLUS FLOAT",
 /* 140 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 141 */ "union ::= select",
 /* 142 */ "union ::= LP union RP",
 /* 143 */ "union ::= union UNION ALL select",
 /* 144 */ "union ::= union UNION ALL LP select RP",
 /* 145 */ "cmd ::= union",
 /* 146 */ "select ::= SELECT selcollist",
 /* 147 */ "sclp ::= selcollist COMMA",
 /* 148 */ "sclp ::=",
 /* 149 */ "selcollist ::= sclp expr as",
 /* 150 */ "selcollist ::= sclp STAR",
 /* 151 */ "as ::= AS ids",
 /* 152 */ "as ::= ids",
 /* 153 */ "as ::=",
 /* 154 */ "from ::= FROM tablelist",
 /* 155 */ "tablelist ::= ids cpxName",
 /* 156 */ "tablelist ::= ids cpxName ids",
 /* 157 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 158 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 159 */ "tmvar ::= VARIABLE",
 /* 160 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 161 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 162 */ "interval_opt ::=",
 /* 163 */ "fill_opt ::=",
 /* 164 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 165 */ "fill_opt ::= FILL LP ID RP",
 /* 166 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 167 */ "sliding_opt ::=",
 /* 168 */ "orderby_opt ::=",
 /* 169 */ "orderby_opt ::= ORDER BY sortlist",
 /* 170 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 171 */ "sortlist ::= item sortorder",
 /* 172 */ "item ::= ids cpxName",
 /* 173 */ "sortorder ::= ASC",
 /* 174 */ "sortorder ::= DESC",
 /* 175 */ "sortorder ::=",
 /* 176 */ "groupby_opt ::=",
 /* 177 */ "groupby_opt ::= GROUP BY grouplist",
 /* 178 */ "grouplist ::= grouplist COMMA item",
 /* 179 */ "grouplist ::= item",
 /* 180 */ "having_opt ::=",
 /* 181 */ "having_opt ::= HAVING expr",
 /* 182 */ "limit_opt ::=",
 /* 183 */ "limit_opt ::= LIMIT signed",
 /* 184 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 185 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 186 */ "slimit_opt ::=",
 /* 187 */ "slimit_opt ::= SLIMIT signed",
 /* 188 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 189 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 190 */ "where_opt ::=",
 /* 191 */ "where_opt ::= WHERE expr",
 /* 192 */ "expr ::= LP expr RP",
 /* 193 */ "expr ::= ID",
 /* 194 */ "expr ::= ID DOT ID",
 /* 195 */ "expr ::= ID DOT STAR",
 /* 196 */ "expr ::= INTEGER",
 /* 197 */ "expr ::= MINUS INTEGER",
 /* 198 */ "expr ::= PLUS INTEGER",
 /* 199 */ "expr ::= FLOAT",
 /* 200 */ "expr ::= MINUS FLOAT",
 /* 201 */ "expr ::= PLUS FLOAT",
 /* 202 */ "expr ::= STRING",
 /* 203 */ "expr ::= NOW",
 /* 204 */ "expr ::= VARIABLE",
 /* 205 */ "expr ::= BOOL",
 /* 206 */ "expr ::= ID LP exprlist RP",
 /* 207 */ "expr ::= ID LP STAR RP",
 /* 208 */ "expr ::= expr IS NULL",
 /* 209 */ "expr ::= expr IS NOT NULL",
 /* 210 */ "expr ::= expr LT expr",
 /* 211 */ "expr ::= expr GT expr",
 /* 212 */ "expr ::= expr LE expr",
 /* 213 */ "expr ::= expr GE expr",
 /* 214 */ "expr ::= expr NE expr",
 /* 215 */ "expr ::= expr EQ expr",
 /* 216 */ "expr ::= expr AND expr",
 /* 217 */ "expr ::= expr OR expr",
 /* 218 */ "expr ::= expr PLUS expr",
 /* 219 */ "expr ::= expr MINUS expr",
 /* 220 */ "expr ::= expr STAR expr",
 /* 221 */ "expr ::= expr SLASH expr",
 /* 222 */ "expr ::= expr REM expr",
 /* 223 */ "expr ::= expr LIKE expr",
 /* 224 */ "expr ::= expr IN LP exprlist RP",
 /* 225 */ "exprlist ::= exprlist COMMA expritem",
 /* 226 */ "exprlist ::= expritem",
 /* 227 */ "expritem ::= expr",
 /* 228 */ "expritem ::=",
 /* 229 */ "cmd ::= RESET QUERY CACHE",
 /* 230 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 231 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 232 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 233 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 234 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 235 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 236 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 237 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 238 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 228: /* keep */
    case 229: /* tagitemlist */
    case 249: /* columnlist */
    case 257: /* fill_opt */
    case 259: /* groupby_opt */
    case 260: /* orderby_opt */
    case 270: /* sortlist */
    case 274: /* grouplist */
{
taosArrayDestroy((yypminor->yy403));
}
      break;
    case 247: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy436));
}
      break;
    case 250: /* select */
{
doDestroyQuerySql((yypminor->yy4));
}
      break;
    case 253: /* selcollist */
    case 265: /* sclp */
    case 275: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy382));
}
      break;
    case 255: /* where_opt */
    case 261: /* having_opt */
    case 266: /* expr */
    case 276: /* expritem */
{
tSqlExprDestroy((yypminor->yy522));
}
      break;
    case 264: /* union */
{
destroyAllSelectClause((yypminor->yy13));
}
      break;
    case 271: /* sortitem */
{
tVariantDestroy(&(yypminor->yy488));
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
  {  209,   -1 }, /* (0) program ::= cmd */
  {  210,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  210,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  210,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  210,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  210,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  210,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  210,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  210,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  210,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  210,   -2 }, /* (10) cmd ::= SHOW VARIABLES */
  {  210,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  210,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  210,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  210,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  211,    0 }, /* (15) dbPrefix ::= */
  {  211,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  213,    0 }, /* (17) cpxName ::= */
  {  213,   -2 }, /* (18) cpxName ::= DOT ids */
  {  210,   -5 }, /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  210,   -4 }, /* (20) cmd ::= SHOW CREATE DATABASE ids */
  {  210,   -3 }, /* (21) cmd ::= SHOW dbPrefix TABLES */
  {  210,   -5 }, /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  210,   -3 }, /* (23) cmd ::= SHOW dbPrefix STABLES */
  {  210,   -5 }, /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  210,   -3 }, /* (25) cmd ::= SHOW dbPrefix VGROUPS */
  {  210,   -4 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  210,   -5 }, /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
  {  210,   -4 }, /* (28) cmd ::= DROP DATABASE ifexists ids */
  {  210,   -3 }, /* (29) cmd ::= DROP DNODE ids */
  {  210,   -3 }, /* (30) cmd ::= DROP USER ids */
  {  210,   -3 }, /* (31) cmd ::= DROP ACCOUNT ids */
  {  210,   -2 }, /* (32) cmd ::= USE ids */
  {  210,   -3 }, /* (33) cmd ::= DESCRIBE ids cpxName */
  {  210,   -5 }, /* (34) cmd ::= ALTER USER ids PASS ids */
  {  210,   -5 }, /* (35) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  210,   -4 }, /* (36) cmd ::= ALTER DNODE ids ids */
  {  210,   -5 }, /* (37) cmd ::= ALTER DNODE ids ids ids */
  {  210,   -3 }, /* (38) cmd ::= ALTER LOCAL ids */
  {  210,   -4 }, /* (39) cmd ::= ALTER LOCAL ids ids */
  {  210,   -4 }, /* (40) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  210,   -4 }, /* (41) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  210,   -6 }, /* (42) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  212,   -1 }, /* (43) ids ::= ID */
  {  212,   -1 }, /* (44) ids ::= STRING */
  {  214,   -2 }, /* (45) ifexists ::= IF EXISTS */
  {  214,    0 }, /* (46) ifexists ::= */
  {  217,   -3 }, /* (47) ifnotexists ::= IF NOT EXISTS */
  {  217,    0 }, /* (48) ifnotexists ::= */
  {  210,   -3 }, /* (49) cmd ::= CREATE DNODE ids */
  {  210,   -6 }, /* (50) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  210,   -5 }, /* (51) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  210,   -5 }, /* (52) cmd ::= CREATE USER ids PASS ids */
  {  219,    0 }, /* (53) pps ::= */
  {  219,   -2 }, /* (54) pps ::= PPS INTEGER */
  {  220,    0 }, /* (55) tseries ::= */
  {  220,   -2 }, /* (56) tseries ::= TSERIES INTEGER */
  {  221,    0 }, /* (57) dbs ::= */
  {  221,   -2 }, /* (58) dbs ::= DBS INTEGER */
  {  222,    0 }, /* (59) streams ::= */
  {  222,   -2 }, /* (60) streams ::= STREAMS INTEGER */
  {  223,    0 }, /* (61) storage ::= */
  {  223,   -2 }, /* (62) storage ::= STORAGE INTEGER */
  {  224,    0 }, /* (63) qtime ::= */
  {  224,   -2 }, /* (64) qtime ::= QTIME INTEGER */
  {  225,    0 }, /* (65) users ::= */
  {  225,   -2 }, /* (66) users ::= USERS INTEGER */
  {  226,    0 }, /* (67) conns ::= */
  {  226,   -2 }, /* (68) conns ::= CONNS INTEGER */
  {  227,    0 }, /* (69) state ::= */
  {  227,   -2 }, /* (70) state ::= STATE ids */
  {  216,   -9 }, /* (71) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  228,   -2 }, /* (72) keep ::= KEEP tagitemlist */
  {  230,   -2 }, /* (73) cache ::= CACHE INTEGER */
  {  231,   -2 }, /* (74) replica ::= REPLICA INTEGER */
  {  232,   -2 }, /* (75) quorum ::= QUORUM INTEGER */
  {  233,   -2 }, /* (76) days ::= DAYS INTEGER */
  {  234,   -2 }, /* (77) minrows ::= MINROWS INTEGER */
  {  235,   -2 }, /* (78) maxrows ::= MAXROWS INTEGER */
  {  236,   -2 }, /* (79) blocks ::= BLOCKS INTEGER */
  {  237,   -2 }, /* (80) ctime ::= CTIME INTEGER */
  {  238,   -2 }, /* (81) wal ::= WAL INTEGER */
  {  239,   -2 }, /* (82) fsync ::= FSYNC INTEGER */
  {  240,   -2 }, /* (83) comp ::= COMP INTEGER */
  {  241,   -2 }, /* (84) prec ::= PRECISION STRING */
  {  242,   -2 }, /* (85) update ::= UPDATE INTEGER */
  {  243,   -2 }, /* (86) cachelast ::= CACHELAST INTEGER */
  {  218,    0 }, /* (87) db_optr ::= */
  {  218,   -2 }, /* (88) db_optr ::= db_optr cache */
  {  218,   -2 }, /* (89) db_optr ::= db_optr replica */
  {  218,   -2 }, /* (90) db_optr ::= db_optr quorum */
  {  218,   -2 }, /* (91) db_optr ::= db_optr days */
  {  218,   -2 }, /* (92) db_optr ::= db_optr minrows */
  {  218,   -2 }, /* (93) db_optr ::= db_optr maxrows */
  {  218,   -2 }, /* (94) db_optr ::= db_optr blocks */
  {  218,   -2 }, /* (95) db_optr ::= db_optr ctime */
  {  218,   -2 }, /* (96) db_optr ::= db_optr wal */
  {  218,   -2 }, /* (97) db_optr ::= db_optr fsync */
  {  218,   -2 }, /* (98) db_optr ::= db_optr comp */
  {  218,   -2 }, /* (99) db_optr ::= db_optr prec */
  {  218,   -2 }, /* (100) db_optr ::= db_optr keep */
  {  218,   -2 }, /* (101) db_optr ::= db_optr update */
  {  218,   -2 }, /* (102) db_optr ::= db_optr cachelast */
  {  215,    0 }, /* (103) alter_db_optr ::= */
  {  215,   -2 }, /* (104) alter_db_optr ::= alter_db_optr replica */
  {  215,   -2 }, /* (105) alter_db_optr ::= alter_db_optr quorum */
  {  215,   -2 }, /* (106) alter_db_optr ::= alter_db_optr keep */
  {  215,   -2 }, /* (107) alter_db_optr ::= alter_db_optr blocks */
  {  215,   -2 }, /* (108) alter_db_optr ::= alter_db_optr comp */
  {  215,   -2 }, /* (109) alter_db_optr ::= alter_db_optr wal */
  {  215,   -2 }, /* (110) alter_db_optr ::= alter_db_optr fsync */
  {  215,   -2 }, /* (111) alter_db_optr ::= alter_db_optr update */
  {  215,   -2 }, /* (112) alter_db_optr ::= alter_db_optr cachelast */
  {  244,   -1 }, /* (113) typename ::= ids */
  {  244,   -4 }, /* (114) typename ::= ids LP signed RP */
  {  245,   -1 }, /* (115) signed ::= INTEGER */
  {  245,   -2 }, /* (116) signed ::= PLUS INTEGER */
  {  245,   -2 }, /* (117) signed ::= MINUS INTEGER */
  {  210,   -3 }, /* (118) cmd ::= CREATE TABLE create_table_args */
  {  210,   -3 }, /* (119) cmd ::= CREATE TABLE create_table_list */
  {  247,   -1 }, /* (120) create_table_list ::= create_from_stable */
  {  247,   -2 }, /* (121) create_table_list ::= create_table_list create_from_stable */
  {  246,   -6 }, /* (122) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  246,  -10 }, /* (123) create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  248,  -10 }, /* (124) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  246,   -5 }, /* (125) create_table_args ::= ifnotexists ids cpxName AS select */
  {  249,   -3 }, /* (126) columnlist ::= columnlist COMMA column */
  {  249,   -1 }, /* (127) columnlist ::= column */
  {  251,   -2 }, /* (128) column ::= ids typename */
  {  229,   -3 }, /* (129) tagitemlist ::= tagitemlist COMMA tagitem */
  {  229,   -1 }, /* (130) tagitemlist ::= tagitem */
  {  252,   -1 }, /* (131) tagitem ::= INTEGER */
  {  252,   -1 }, /* (132) tagitem ::= FLOAT */
  {  252,   -1 }, /* (133) tagitem ::= STRING */
  {  252,   -1 }, /* (134) tagitem ::= BOOL */
  {  252,   -1 }, /* (135) tagitem ::= NULL */
  {  252,   -2 }, /* (136) tagitem ::= MINUS INTEGER */
  {  252,   -2 }, /* (137) tagitem ::= MINUS FLOAT */
  {  252,   -2 }, /* (138) tagitem ::= PLUS INTEGER */
  {  252,   -2 }, /* (139) tagitem ::= PLUS FLOAT */
  {  250,  -12 }, /* (140) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  264,   -1 }, /* (141) union ::= select */
  {  264,   -3 }, /* (142) union ::= LP union RP */
  {  264,   -4 }, /* (143) union ::= union UNION ALL select */
  {  264,   -6 }, /* (144) union ::= union UNION ALL LP select RP */
  {  210,   -1 }, /* (145) cmd ::= union */
  {  250,   -2 }, /* (146) select ::= SELECT selcollist */
  {  265,   -2 }, /* (147) sclp ::= selcollist COMMA */
  {  265,    0 }, /* (148) sclp ::= */
  {  253,   -3 }, /* (149) selcollist ::= sclp expr as */
  {  253,   -2 }, /* (150) selcollist ::= sclp STAR */
  {  267,   -2 }, /* (151) as ::= AS ids */
  {  267,   -1 }, /* (152) as ::= ids */
  {  267,    0 }, /* (153) as ::= */
  {  254,   -2 }, /* (154) from ::= FROM tablelist */
  {  268,   -2 }, /* (155) tablelist ::= ids cpxName */
  {  268,   -3 }, /* (156) tablelist ::= ids cpxName ids */
  {  268,   -4 }, /* (157) tablelist ::= tablelist COMMA ids cpxName */
  {  268,   -5 }, /* (158) tablelist ::= tablelist COMMA ids cpxName ids */
  {  269,   -1 }, /* (159) tmvar ::= VARIABLE */
  {  256,   -4 }, /* (160) interval_opt ::= INTERVAL LP tmvar RP */
  {  256,   -6 }, /* (161) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  256,    0 }, /* (162) interval_opt ::= */
  {  257,    0 }, /* (163) fill_opt ::= */
  {  257,   -6 }, /* (164) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  257,   -4 }, /* (165) fill_opt ::= FILL LP ID RP */
  {  258,   -4 }, /* (166) sliding_opt ::= SLIDING LP tmvar RP */
  {  258,    0 }, /* (167) sliding_opt ::= */
  {  260,    0 }, /* (168) orderby_opt ::= */
  {  260,   -3 }, /* (169) orderby_opt ::= ORDER BY sortlist */
  {  270,   -4 }, /* (170) sortlist ::= sortlist COMMA item sortorder */
  {  270,   -2 }, /* (171) sortlist ::= item sortorder */
  {  272,   -2 }, /* (172) item ::= ids cpxName */
  {  273,   -1 }, /* (173) sortorder ::= ASC */
  {  273,   -1 }, /* (174) sortorder ::= DESC */
  {  273,    0 }, /* (175) sortorder ::= */
  {  259,    0 }, /* (176) groupby_opt ::= */
  {  259,   -3 }, /* (177) groupby_opt ::= GROUP BY grouplist */
  {  274,   -3 }, /* (178) grouplist ::= grouplist COMMA item */
  {  274,   -1 }, /* (179) grouplist ::= item */
  {  261,    0 }, /* (180) having_opt ::= */
  {  261,   -2 }, /* (181) having_opt ::= HAVING expr */
  {  263,    0 }, /* (182) limit_opt ::= */
  {  263,   -2 }, /* (183) limit_opt ::= LIMIT signed */
  {  263,   -4 }, /* (184) limit_opt ::= LIMIT signed OFFSET signed */
  {  263,   -4 }, /* (185) limit_opt ::= LIMIT signed COMMA signed */
  {  262,    0 }, /* (186) slimit_opt ::= */
  {  262,   -2 }, /* (187) slimit_opt ::= SLIMIT signed */
  {  262,   -4 }, /* (188) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  262,   -4 }, /* (189) slimit_opt ::= SLIMIT signed COMMA signed */
  {  255,    0 }, /* (190) where_opt ::= */
  {  255,   -2 }, /* (191) where_opt ::= WHERE expr */
  {  266,   -3 }, /* (192) expr ::= LP expr RP */
  {  266,   -1 }, /* (193) expr ::= ID */
  {  266,   -3 }, /* (194) expr ::= ID DOT ID */
  {  266,   -3 }, /* (195) expr ::= ID DOT STAR */
  {  266,   -1 }, /* (196) expr ::= INTEGER */
  {  266,   -2 }, /* (197) expr ::= MINUS INTEGER */
  {  266,   -2 }, /* (198) expr ::= PLUS INTEGER */
  {  266,   -1 }, /* (199) expr ::= FLOAT */
  {  266,   -2 }, /* (200) expr ::= MINUS FLOAT */
  {  266,   -2 }, /* (201) expr ::= PLUS FLOAT */
  {  266,   -1 }, /* (202) expr ::= STRING */
  {  266,   -1 }, /* (203) expr ::= NOW */
  {  266,   -1 }, /* (204) expr ::= VARIABLE */
  {  266,   -1 }, /* (205) expr ::= BOOL */
  {  266,   -4 }, /* (206) expr ::= ID LP exprlist RP */
  {  266,   -4 }, /* (207) expr ::= ID LP STAR RP */
  {  266,   -3 }, /* (208) expr ::= expr IS NULL */
  {  266,   -4 }, /* (209) expr ::= expr IS NOT NULL */
  {  266,   -3 }, /* (210) expr ::= expr LT expr */
  {  266,   -3 }, /* (211) expr ::= expr GT expr */
  {  266,   -3 }, /* (212) expr ::= expr LE expr */
  {  266,   -3 }, /* (213) expr ::= expr GE expr */
  {  266,   -3 }, /* (214) expr ::= expr NE expr */
  {  266,   -3 }, /* (215) expr ::= expr EQ expr */
  {  266,   -3 }, /* (216) expr ::= expr AND expr */
  {  266,   -3 }, /* (217) expr ::= expr OR expr */
  {  266,   -3 }, /* (218) expr ::= expr PLUS expr */
  {  266,   -3 }, /* (219) expr ::= expr MINUS expr */
  {  266,   -3 }, /* (220) expr ::= expr STAR expr */
  {  266,   -3 }, /* (221) expr ::= expr SLASH expr */
  {  266,   -3 }, /* (222) expr ::= expr REM expr */
  {  266,   -3 }, /* (223) expr ::= expr LIKE expr */
  {  266,   -5 }, /* (224) expr ::= expr IN LP exprlist RP */
  {  275,   -3 }, /* (225) exprlist ::= exprlist COMMA expritem */
  {  275,   -1 }, /* (226) exprlist ::= expritem */
  {  276,   -1 }, /* (227) expritem ::= expr */
  {  276,    0 }, /* (228) expritem ::= */
  {  210,   -3 }, /* (229) cmd ::= RESET QUERY CACHE */
  {  210,   -7 }, /* (230) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  210,   -7 }, /* (231) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  210,   -7 }, /* (232) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  210,   -7 }, /* (233) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  210,   -8 }, /* (234) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  210,   -9 }, /* (235) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  210,   -3 }, /* (236) cmd ::= KILL CONNECTION INTEGER */
  {  210,   -5 }, /* (237) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  210,   -5 }, /* (238) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 118: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==118);
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
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy463);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy463);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy463);}
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy463.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy463.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy463.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy463.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy463.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy463.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy463.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy463.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy463.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy463 = yylhsminor.yy463;
        break;
      case 72: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy403 = yymsp[0].minor.yy403; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy478);}
        break;
      case 88: /* db_optr ::= db_optr cache */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 89: /* db_optr ::= db_optr replica */
      case 104: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==104);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 90: /* db_optr ::= db_optr quorum */
      case 105: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==105);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 91: /* db_optr ::= db_optr days */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 92: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 93: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 94: /* db_optr ::= db_optr blocks */
      case 107: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==107);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 95: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 96: /* db_optr ::= db_optr wal */
      case 109: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==109);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 97: /* db_optr ::= db_optr fsync */
      case 110: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==110);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 98: /* db_optr ::= db_optr comp */
      case 108: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==108);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 99: /* db_optr ::= db_optr prec */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 100: /* db_optr ::= db_optr keep */
      case 106: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==106);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.keep = yymsp[0].minor.yy403; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 101: /* db_optr ::= db_optr update */
      case 111: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==111);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 102: /* db_optr ::= db_optr cachelast */
      case 112: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==112);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 103: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy478);}
        break;
      case 113: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy363, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy363 = yylhsminor.yy363;
        break;
      case 114: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy387 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy363, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy387;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy363, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy363 = yylhsminor.yy363;
        break;
      case 115: /* signed ::= INTEGER */
{ yylhsminor.yy387 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy387 = yylhsminor.yy387;
        break;
      case 116: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy387 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 117: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy387 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 119: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy436;}
        break;
      case 120: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy84);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy436 = pCreateTable;
}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 121: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy436->childTableInfo, &yymsp[0].minor.yy84);
  yylhsminor.yy436 = yymsp[-1].minor.yy436;
}
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 122: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy436 = tSetCreateSqlElems(yymsp[-1].minor.yy403, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy436, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy436 = yylhsminor.yy436;
        break;
      case 123: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy436 = tSetCreateSqlElems(yymsp[-5].minor.yy403, yymsp[-1].minor.yy403, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy436, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy436 = yylhsminor.yy436;
        break;
      case 124: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy84 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy403, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy84 = yylhsminor.yy84;
        break;
      case 125: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy436 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy4, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy436, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy436 = yylhsminor.yy436;
        break;
      case 126: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy403, &yymsp[0].minor.yy363); yylhsminor.yy403 = yymsp[-2].minor.yy403;  }
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 127: /* columnlist ::= column */
{yylhsminor.yy403 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy403, &yymsp[0].minor.yy363);}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 128: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy363, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy363);
}
  yymsp[-1].minor.yy363 = yylhsminor.yy363;
        break;
      case 129: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy403 = tVariantListAppend(yymsp[-2].minor.yy403, &yymsp[0].minor.yy488, -1);    }
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 130: /* tagitemlist ::= tagitem */
{ yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 131: /* tagitem ::= INTEGER */
      case 132: /* tagitem ::= FLOAT */ yytestcase(yyruleno==132);
      case 133: /* tagitem ::= STRING */ yytestcase(yyruleno==133);
      case 134: /* tagitem ::= BOOL */ yytestcase(yyruleno==134);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 135: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 136: /* tagitem ::= MINUS INTEGER */
      case 137: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==137);
      case 138: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==138);
      case 139: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==139);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 140: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy4 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy382, yymsp[-9].minor.yy403, yymsp[-8].minor.yy522, yymsp[-4].minor.yy403, yymsp[-3].minor.yy403, &yymsp[-7].minor.yy222, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy403, &yymsp[0].minor.yy404, &yymsp[-1].minor.yy404);
}
  yymsp[-11].minor.yy4 = yylhsminor.yy4;
        break;
      case 141: /* union ::= select */
{ yylhsminor.yy13 = setSubclause(NULL, yymsp[0].minor.yy4); }
  yymsp[0].minor.yy13 = yylhsminor.yy13;
        break;
      case 142: /* union ::= LP union RP */
{ yymsp[-2].minor.yy13 = yymsp[-1].minor.yy13; }
        break;
      case 143: /* union ::= union UNION ALL select */
{ yylhsminor.yy13 = appendSelectClause(yymsp[-3].minor.yy13, yymsp[0].minor.yy4); }
  yymsp[-3].minor.yy13 = yylhsminor.yy13;
        break;
      case 144: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy13 = appendSelectClause(yymsp[-5].minor.yy13, yymsp[-1].minor.yy4); }
  yymsp[-5].minor.yy13 = yylhsminor.yy13;
        break;
      case 145: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy13, NULL, TSDB_SQL_SELECT); }
        break;
      case 146: /* select ::= SELECT selcollist */
{
  yylhsminor.yy4 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy382, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy4 = yylhsminor.yy4;
        break;
      case 147: /* sclp ::= selcollist COMMA */
{yylhsminor.yy382 = yymsp[-1].minor.yy382;}
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 148: /* sclp ::= */
{yymsp[1].minor.yy382 = 0;}
        break;
      case 149: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy382 = tSqlExprListAppend(yymsp[-2].minor.yy382, yymsp[-1].minor.yy522, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy382 = yylhsminor.yy382;
        break;
      case 150: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy382 = tSqlExprListAppend(yymsp[-1].minor.yy382, pNode, 0);
}
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 151: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 152: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 153: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 154: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy403 = yymsp[0].minor.yy403;}
        break;
      case 155: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy403 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy403 = tVariantListAppendToken(yylhsminor.yy403, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 156: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy403 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy403 = tVariantListAppendToken(yylhsminor.yy403, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 157: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy403 = tVariantListAppendToken(yymsp[-3].minor.yy403, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy403 = tVariantListAppendToken(yylhsminor.yy403, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 158: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy403 = tVariantListAppendToken(yymsp[-4].minor.yy403, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy403 = tVariantListAppendToken(yylhsminor.yy403, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy403 = yylhsminor.yy403;
        break;
      case 159: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 160: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy222.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy222.offset.n = 0; yymsp[-3].minor.yy222.offset.z = NULL; yymsp[-3].minor.yy222.offset.type = 0;}
        break;
      case 161: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy222.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy222.offset = yymsp[-1].minor.yy0;}
        break;
      case 162: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy222, 0, sizeof(yymsp[1].minor.yy222));}
        break;
      case 163: /* fill_opt ::= */
{yymsp[1].minor.yy403 = 0;     }
        break;
      case 164: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy403, &A, -1, 0);
    yymsp[-5].minor.yy403 = yymsp[-1].minor.yy403;
}
        break;
      case 165: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy403 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 166: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 167: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 168: /* orderby_opt ::= */
{yymsp[1].minor.yy403 = 0;}
        break;
      case 169: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy403 = yymsp[0].minor.yy403;}
        break;
      case 170: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy403 = tVariantListAppend(yymsp[-3].minor.yy403, &yymsp[-1].minor.yy488, yymsp[0].minor.yy70);
}
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 171: /* sortlist ::= item sortorder */
{
  yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[-1].minor.yy488, yymsp[0].minor.yy70);
}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 172: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 173: /* sortorder ::= ASC */
{ yymsp[0].minor.yy70 = TSDB_ORDER_ASC; }
        break;
      case 174: /* sortorder ::= DESC */
{ yymsp[0].minor.yy70 = TSDB_ORDER_DESC;}
        break;
      case 175: /* sortorder ::= */
{ yymsp[1].minor.yy70 = TSDB_ORDER_ASC; }
        break;
      case 176: /* groupby_opt ::= */
{ yymsp[1].minor.yy403 = 0;}
        break;
      case 177: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy403 = yymsp[0].minor.yy403;}
        break;
      case 178: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy403 = tVariantListAppend(yymsp[-2].minor.yy403, &yymsp[0].minor.yy488, -1);
}
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 179: /* grouplist ::= item */
{
  yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1);
}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 180: /* having_opt ::= */
      case 190: /* where_opt ::= */ yytestcase(yyruleno==190);
      case 228: /* expritem ::= */ yytestcase(yyruleno==228);
{yymsp[1].minor.yy522 = 0;}
        break;
      case 181: /* having_opt ::= HAVING expr */
      case 191: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==191);
{yymsp[-1].minor.yy522 = yymsp[0].minor.yy522;}
        break;
      case 182: /* limit_opt ::= */
      case 186: /* slimit_opt ::= */ yytestcase(yyruleno==186);
{yymsp[1].minor.yy404.limit = -1; yymsp[1].minor.yy404.offset = 0;}
        break;
      case 183: /* limit_opt ::= LIMIT signed */
      case 187: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==187);
{yymsp[-1].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-1].minor.yy404.offset = 0;}
        break;
      case 184: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy404.limit = yymsp[-2].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[0].minor.yy387;}
        break;
      case 185: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[-2].minor.yy387;}
        break;
      case 188: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy404.limit = yymsp[-2].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[0].minor.yy387;}
        break;
      case 189: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[-2].minor.yy387;}
        break;
      case 192: /* expr ::= LP expr RP */
{yylhsminor.yy522 = yymsp[-1].minor.yy522; yylhsminor.yy522->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy522->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 193: /* expr ::= ID */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 194: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 195: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 196: /* expr ::= INTEGER */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 197: /* expr ::= MINUS INTEGER */
      case 198: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==198);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy522 = yylhsminor.yy522;
        break;
      case 199: /* expr ::= FLOAT */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 200: /* expr ::= MINUS FLOAT */
      case 201: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==201);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy522 = yylhsminor.yy522;
        break;
      case 202: /* expr ::= STRING */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 203: /* expr ::= NOW */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 204: /* expr ::= VARIABLE */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 205: /* expr ::= BOOL */
{ yylhsminor.yy522 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 206: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy522 = tSqlExprCreateFunction(yymsp[-1].minor.yy382, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy522 = yylhsminor.yy522;
        break;
      case 207: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy522 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy522 = yylhsminor.yy522;
        break;
      case 208: /* expr ::= expr IS NULL */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 209: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-3].minor.yy522, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy522 = yylhsminor.yy522;
        break;
      case 210: /* expr ::= expr LT expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_LT);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 211: /* expr ::= expr GT expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_GT);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 212: /* expr ::= expr LE expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_LE);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 213: /* expr ::= expr GE expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_GE);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 214: /* expr ::= expr NE expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_NE);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 215: /* expr ::= expr EQ expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_EQ);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 216: /* expr ::= expr AND expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_AND);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 217: /* expr ::= expr OR expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_OR); }
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 218: /* expr ::= expr PLUS expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_PLUS);  }
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 219: /* expr ::= expr MINUS expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_MINUS); }
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 220: /* expr ::= expr STAR expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_STAR);  }
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 221: /* expr ::= expr SLASH expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_DIVIDE);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 222: /* expr ::= expr REM expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_REM);   }
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 223: /* expr ::= expr LIKE expr */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-2].minor.yy522, yymsp[0].minor.yy522, TK_LIKE);  }
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 224: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy522 = tSqlExprCreate(yymsp[-4].minor.yy522, (tSQLExpr*)yymsp[-1].minor.yy382, TK_IN); }
  yymsp[-4].minor.yy522 = yylhsminor.yy522;
        break;
      case 225: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy382 = tSqlExprListAppend(yymsp[-2].minor.yy382,yymsp[0].minor.yy522,0);}
  yymsp[-2].minor.yy382 = yylhsminor.yy382;
        break;
      case 226: /* exprlist ::= expritem */
{yylhsminor.yy382 = tSqlExprListAppend(0,yymsp[0].minor.yy522,0);}
  yymsp[0].minor.yy382 = yylhsminor.yy382;
        break;
      case 227: /* expritem ::= expr */
{yylhsminor.yy522 = yymsp[0].minor.yy522;}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 229: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 230: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 231: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 232: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 233: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 234: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 235: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 236: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 237: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 238: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  int32_t outputBufLen = tListLen(pInfo->pzErrMsg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->pzErrMsg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->pzErrMsg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->pzErrMsg, "Incomplete SQL statement");
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
