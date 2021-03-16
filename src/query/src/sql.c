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
#define YYNOCODE 265
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy96;
  tSqlExpr* yy178;
  SCreateAcctInfo yy187;
  SCreateTableSql* yy230;
  SArray* yy285;
  TAOS_FIELD yy295;
  SQuerySqlNode* yy342;
  tVariant yy362;
  SIntervalVal yy376;
  SLimitVal yy438;
  int yy460;
  SSubclauseInfo* yy513;
  SSessionWindowVal yy523;
  int64_t yy525;
  SCreateDbInfo yy526;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             313
#define YYNRULE              265
#define YYNTOKEN             188
#define YY_MAX_SHIFT         312
#define YY_MIN_SHIFTREDUCE   502
#define YY_MAX_SHIFTREDUCE   766
#define YY_ERROR_ACTION      767
#define YY_ACCEPT_ACTION     768
#define YY_NO_ACTION         769
#define YY_MIN_REDUCE        770
#define YY_MAX_REDUCE        1034
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
#define YY_ACTTAB_COUNT (675)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   910,  549,  201,  310,  205,  138,  938,    3,  166,  550,
 /*    10 */   768,  312,   17,   47,   48,  138,   51,   52,   30,  180,
 /*    20 */   213,   41,  180,   50,  260,   55,   53,   57,   54, 1016,
 /*    30 */   916,  208, 1017,   46,   45,  178,  180,   44,   43,   42,
 /*    40 */    47,   48,  219,   51,   52,  207, 1017,  213,   41,  549,
 /*    50 */    50,  260,   55,   53,   57,   54,  927,  550,  184,  203,
 /*    60 */    46,   45,  913,  218,   44,   43,   42,   48,  935,   51,
 /*    70 */    52,  240,  968,  213,   41,  549,   50,  260,   55,   53,
 /*    80 */    57,   54,  969,  550,  255,  220,   46,   45,   82,  916,
 /*    90 */    44,   43,   42,  503,  504,  505,  506,  507,  508,  509,
 /*   100 */   510,  511,  512,  513,  514,  515,  311,  628,  815,  230,
 /*   110 */    70,  916,  165,   47,   48,   30,   51,   52,  276,   30,
 /*   120 */   213,   41,  904,   50,  260,   55,   53,   57,   54,   44,
 /*   130 */    43,   42,  714,   46,   45,  286,  285,   44,   43,   42,
 /*   140 */    47,   49,  824,   51,   52,  224,  165,  213,   41,   24,
 /*   150 */    50,  260,   55,   53,   57,   54,  216,   36,  902,  913,
 /*   160 */    46,   45,  222,  912,   44,   43,   42,   23,  274,  305,
 /*   170 */   304,  273,  272,  271,  303,  270,  302,  301,  300,  269,
 /*   180 */   299,  298,  876,  138,  864,  865,  866,  867,  868,  869,
 /*   190 */   870,  871,  872,  873,  874,  875,  877,  878,   51,   52,
 /*   200 */   816,  306,  213,   41,  165,   50,  260,   55,   53,   57,
 /*   210 */    54,  296,   18,   79,  226,   46,   45,  283,  282,   44,
 /*   220 */    43,   42,  212,  727,  927,  131,  718,  916,  721,  189,
 /*   230 */   724,  223,  212,  727,  278,  190,  718,  276,  721,  202,
 /*   240 */   724,  115,  114,  188,  899,  900,   29,  903,  257,  233,
 /*   250 */    76,  309,  308,  123,  209,  210,  237,  236,  259,  138,
 /*   260 */    23,  225,  305,  304,  209,  210,   69,  303,  979,  302,
 /*   270 */   301,  300,   24,  299,  298,  884,  102,   30,  882,  883,
 /*   280 */    36,  296,   78,  885, 1013,  887,  888,  886,  245,  889,
 /*   290 */   890,   55,   53,   57,   54,   71,  914,  261,  901,   46,
 /*   300 */    45,  668,  239,   44,   43,   42,  100,  105,   30,  196,
 /*   310 */     1,  153,   94,  104,  110,  113,  103,  720,  217,  723,
 /*   320 */   129,  913,  107,    5,  155,   56,   77,   30,   36,   33,
 /*   330 */   154,   89,   84,   88,   30,   56,  173,  169,  726,  719,
 /*   340 */    30,  722,  171,  168,  118,  117,  116,   12,  726,  279,
 /*   350 */   211,   81,  913,  149,  725,  660,   46,   45,  695,  696,
 /*   360 */    44,   43,   42,  242,  725,  243,  665,  652,  280,   31,
 /*   370 */   649,  913,  650,   25,  651,  284,  680,  716,  913,  672,
 /*   380 */   133,  288,  686,  687,  913,  747,   60,   20,  728,   19,
 /*   390 */    61, 1012,   19,  730,    6,   64,  638,  263,  227,  228,
 /*   400 */    31,   31,  640,  265,  639, 1011,   60,   80,   60,   93,
 /*   410 */    92,   28,   62,  717,  266,   65,   14,   13,   67,  197,
 /*   420 */   627,   99,   98,  198,   16,   15,  656,  654,  657,  655,
 /*   430 */   112,  111,  128,  126,  182, 1026,  183,  185,  179,  186,
 /*   440 */   915,  187,  193,  194,  192,  177,  191,  181,  978,  929,
 /*   450 */   214,  975,  974,  215,  287,  130,   39,  937,  148,  944,
 /*   460 */   946,  132,  136,   36,  241,  961,  960,  150,  909,  127,
 /*   470 */   679,  246,  911,  204,  248,  140,  653,  151,  141,  926,
 /*   480 */   152,  827,   63,  253,   66,  268,   37,  175,   34,  139,
 /*   490 */    58,  258,  277,  823, 1031,   90, 1030, 1028,  156,  256,
 /*   500 */   281, 1025,   96, 1024,  142, 1022,  254,  157,  845,   35,
 /*   510 */    32,   38,  176,  812,  106,  252,  810,  108,  109,  808,
 /*   520 */   807,  229,  167,  805,  804,  803,  250,  802,  801,  800,
 /*   530 */   170,  172,  797,  795,  793,  791,  789,  174,  247,  244,
 /*   540 */    72,   73,  249,  962,   40,  297,  101,  289,  290,  291,
 /*   550 */   292,  293,  294,  199,  295,  221,  307,  267,  766,  231,
 /*   560 */   232,  765,  234,  235,  200,   85,   86,  195,  764,  752,
 /*   570 */   238,  242,  662,   74,   68,    8,  262,  806,  681,  799,
 /*   580 */   164,  846,  160,  158,  159,  162,  161,  163,  119,    2,
 /*   590 */   120,  121,  880,  798,    4,  122,  790,  134,  147,  143,
 /*   600 */   144,  145,  146,  135,  684,   75,  206,  251,  137,  892,
 /*   610 */   688,   26,    9,   10,  729,   27,    7,   11,  731,   21,
 /*   620 */    22,  264,   83,  591,  587,   81,  585,  584,  583,  580,
 /*   630 */   553,  275,   31,   87,   91,  630,   59,  629,   95,  626,
 /*   640 */   575,  573,  565,  571,  567,  569,   97,  563,  561,  594,
 /*   650 */   593,  592,  590,  589,  588,  586,  582,  581,   60,  551,
 /*   660 */   519,  517,  124,  770,  769,  769,  769,  769,  769,  769,
 /*   670 */   769,  769,  769,  769,  125,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   192,    1,  191,  192,  211,  192,  192,  195,  196,    9,
 /*    10 */   189,  190,  253,   13,   14,  192,   16,   17,  192,  253,
 /*    20 */    20,   21,  253,   23,   24,   25,   26,   27,   28,  263,
 /*    30 */   237,  262,  263,   33,   34,  253,  253,   37,   38,   39,
 /*    40 */    13,   14,  234,   16,   17,  262,  263,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  235,    9,  253,  233,
 /*    60 */    33,   34,  236,  211,   37,   38,   39,   14,  254,   16,
 /*    70 */    17,  250,  259,   20,   21,    1,   23,   24,   25,   26,
 /*    80 */    27,   28,  259,    9,  261,  211,   33,   34,  198,  237,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,    5,  197,   61,
 /*   110 */   110,  237,  201,   13,   14,  192,   16,   17,   79,  192,
 /*   120 */    20,   21,  232,   23,   24,   25,   26,   27,   28,   37,
 /*   130 */    38,   39,  105,   33,   34,   33,   34,   37,   38,   39,
 /*   140 */    13,   14,  197,   16,   17,   67,  201,   20,   21,  104,
 /*   150 */    23,   24,   25,   26,   27,   28,  233,  112,    0,  236,
 /*   160 */    33,   34,   67,  236,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  210,  192,  212,  213,  214,  215,  216,  217,
 /*   190 */   218,  219,  220,  221,  222,  223,  224,  225,   16,   17,
 /*   200 */   197,  211,   20,   21,  201,   23,   24,   25,   26,   27,
 /*   210 */    28,   81,   44,  198,  136,   33,   34,  139,  140,   37,
 /*   220 */    38,   39,    1,    2,  235,  192,    5,  237,    7,   61,
 /*   230 */     9,  136,    1,    2,  139,   67,    5,   79,    7,  250,
 /*   240 */     9,   73,   74,   75,  229,  230,  231,  232,  257,  135,
 /*   250 */   259,   64,   65,   66,   33,   34,  142,  143,   37,  192,
 /*   260 */    88,  192,   90,   91,   33,   34,  198,   95,  228,   97,
 /*   270 */    98,   99,  104,  101,  102,  210,   76,  192,  213,  214,
 /*   280 */   112,   81,  238,  218,  253,  220,  221,  222,  255,  224,
 /*   290 */   225,   25,   26,   27,   28,  251,  227,   15,  230,   33,
 /*   300 */    34,   37,  134,   37,   38,   39,   62,   63,  192,  141,
 /*   310 */   199,  200,   68,   69,   70,   71,   72,    5,  233,    7,
 /*   320 */   104,  236,   78,   62,   63,  104,  259,  192,  112,   68,
 /*   330 */    69,   70,   71,   72,  192,  104,   62,   63,  117,    5,
 /*   340 */   192,    7,   68,   69,   70,   71,   72,  104,  117,  233,
 /*   350 */    60,  108,  236,  110,  133,  105,   33,   34,  124,  125,
 /*   360 */    37,   38,   39,  113,  133,  105,  109,    2,  233,  109,
 /*   370 */     5,  236,    7,  116,    9,  233,  105,    1,  236,  115,
 /*   380 */   109,  233,  105,  105,  236,  105,  109,  109,  105,  109,
 /*   390 */   109,  253,  109,  111,  104,  109,  105,  105,   33,   34,
 /*   400 */   109,  109,  105,  105,  105,  253,  109,  109,  109,  137,
 /*   410 */   138,  104,  131,   37,  107,  129,  137,  138,  104,  253,
 /*   420 */   106,  137,  138,  253,  137,  138,    5,    5,    7,    7,
 /*   430 */    76,   77,   62,   63,  253,  237,  253,  253,  253,  253,
 /*   440 */   237,  253,  253,  253,  253,  253,  253,  253,  228,  235,
 /*   450 */   228,  228,  228,  228,  228,  192,  252,  192,  239,  192,
 /*   460 */   192,  192,  192,  112,  235,  260,  260,  192,  192,   60,
 /*   470 */   117,  256,  235,  256,  256,  247,  111,  192,  246,  249,
 /*   480 */   192,  192,  130,  256,  128,  192,  192,  192,  192,  248,
 /*   490 */   127,  122,  192,  192,  192,  192,  192,  192,  192,  126,
 /*   500 */   192,  192,  192,  192,  245,  192,  121,  192,  192,  192,
 /*   510 */   192,  192,  192,  192,  192,  120,  192,  192,  192,  192,
 /*   520 */   192,  192,  192,  192,  192,  192,  119,  192,  192,  192,
 /*   530 */   192,  192,  192,  192,  192,  192,  192,  192,  118,  193,
 /*   540 */   193,  193,  193,  193,  132,  103,   87,   86,   50,   83,
 /*   550 */    85,   54,   84,  193,   82,  193,   79,  193,    5,  144,
 /*   560 */     5,    5,  144,    5,  193,  198,  198,  193,    5,   89,
 /*   570 */   135,  113,  105,  109,  114,  104,  107,  193,  105,  193,
 /*   580 */   202,  209,  203,  208,  207,  204,  206,  205,  194,  199,
 /*   590 */   194,  194,  226,  193,  195,  194,  193,  104,  240,  244,
 /*   600 */   243,  242,  241,  109,  105,  104,    1,  104,  104,  226,
 /*   610 */   105,  109,  123,  123,  105,  109,  104,  104,  111,  104,
 /*   620 */   104,  107,   76,    9,    5,  108,    5,    5,    5,    5,
 /*   630 */    80,   15,  109,   76,  138,    5,   16,    5,  138,  105,
 /*   640 */     5,    5,    5,    5,    5,    5,  138,    5,    5,    5,
 /*   650 */     5,    5,    5,    5,    5,    5,    5,    5,  109,   80,
 /*   660 */    60,   59,   21,    0,  264,  264,  264,  264,  264,  264,
 /*   670 */   264,  264,  264,  264,   21,  264,  264,  264,  264,  264,
 /*   680 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   690 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   700 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   710 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   720 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   730 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   740 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   750 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   760 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   770 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   780 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   790 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   800 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   810 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   820 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   830 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   840 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   850 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   860 */   264,  264,  264,
};
#define YY_SHIFT_COUNT    (312)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (663)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  172,  172,   39,  221,  231,   74,   74,
 /*    10 */    74,   74,   74,   74,   74,   74,   74,    0,   48,  231,
 /*    20 */   365,  365,  365,  365,   45,   74,   74,   74,   74,  158,
 /*    30 */    74,   74,  200,   39,  130,  130,  675,  675,  675,  231,
 /*    40 */   231,  231,  231,  231,  231,  231,  231,  231,  231,  231,
 /*    50 */   231,  231,  231,  231,  231,  231,  231,  231,  231,  365,
 /*    60 */   365,  102,  102,  102,  102,  102,  102,  102,  216,   74,
 /*    70 */    74,  264,   74,   74,   74,   74,  234,  234,  257,   74,
 /*    80 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*    90 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   100 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   110 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   120 */    74,   74,   74,   74,   74,   74,   74,   74,   74,  351,
 /*   130 */   409,  409,  409,  353,  353,  353,  409,  353,  409,  356,
 /*   140 */   352,  363,  369,  373,  385,  395,  407,  420,  412,  351,
 /*   150 */   409,  409,  409,  442,   39,   39,  409,  409,  459,  461,
 /*   160 */   498,  466,  465,  497,  468,  472,  442,  409,  477,  477,
 /*   170 */   409,  477,  409,  477,  409,  675,  675,   27,  100,  127,
 /*   180 */   100,  100,   53,  182,  266,  266,  266,  266,  244,  261,
 /*   190 */   274,  323,  323,  323,  323,   78,  114,   92,   92,  243,
 /*   200 */    95,  187,  250,  260,  271,  277,  278,  280,  283,  312,
 /*   210 */   334,  376,  290,  282,  281,  286,  291,  292,  297,  298,
 /*   220 */   299,  307,  272,  279,  284,  314,  287,  421,  422,  354,
 /*   230 */   370,  553,  415,  555,  556,  418,  558,  563,  480,  435,
 /*   240 */   458,  467,  460,  469,  471,  464,  473,  493,  499,  494,
 /*   250 */   501,  605,  503,  505,  504,  502,  489,  506,  490,  509,
 /*   260 */   512,  507,  513,  469,  515,  514,  516,  517,  546,  614,
 /*   270 */   619,  621,  622,  623,  624,  550,  616,  557,  496,  523,
 /*   280 */   523,  620,  500,  508,  523,  630,  632,  534,  523,  635,
 /*   290 */   636,  637,  638,  639,  640,  642,  643,  644,  645,  646,
 /*   300 */   647,  648,  649,  650,  651,  652,  549,  579,  641,  653,
 /*   310 */   600,  602,  663,
};
#define YY_REDUCE_COUNT (176)
#define YY_REDUCE_MIN   (-241)
#define YY_REDUCE_MAX   (403)
static const short yy_reduce_ofst[] = {
 /*     0 */  -179,  -28,  -28,   65,   65,   15, -231, -217, -174, -177,
 /*    10 */    -9,  -77,   85,  116,  135,  142,  148, -186, -189, -234,
 /*    20 */  -207, -148, -126,  -10,  -11,   33, -187,   67, -192, -110,
 /*    30 */    69,  -73,  -89,   68,  -55,    3,   44,  111, -188, -241,
 /*    40 */  -218, -195,   31,  138,  152,  166,  170,  181,  183,  184,
 /*    50 */   185,  186,  188,  189,  190,  191,  192,  193,  194,  198,
 /*    60 */   203,   40,  220,  222,  223,  224,  225,  226,  214,  263,
 /*    70 */   265,  204,  267,  268,  269,  270,  205,  206,  219,  275,
 /*    80 */   276,  285,  288,  289,  293,  294,  295,  296,  300,  301,
 /*    90 */   302,  303,  304,  305,  306,  308,  309,  310,  311,  313,
 /*   100 */   315,  316,  317,  318,  319,  320,  321,  322,  324,  325,
 /*   110 */   326,  327,  328,  329,  330,  331,  332,  333,  335,  336,
 /*   120 */   337,  338,  339,  340,  341,  342,  343,  344,  345,  229,
 /*   130 */   346,  347,  348,  215,  217,  218,  349,  227,  350,  230,
 /*   140 */   241,  228,  232,  259,  355,  357,  359,  361,  358,  237,
 /*   150 */   360,  362,  364,  366,  367,  368,  371,  374,  372,  375,
 /*   160 */   377,  379,  380,  381,  382,  378,  383,  384,  394,  396,
 /*   170 */   386,  397,  400,  401,  403,  390,  399,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   767,  879,  825,  891,  813,  822, 1019, 1019,  767,  767,
 /*    10 */   767,  767,  767,  767,  767,  767,  767,  939,  786, 1019,
 /*    20 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  822,
 /*    30 */   767,  767,  828,  822,  828,  828,  934,  863,  881,  767,
 /*    40 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    50 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    60 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    70 */   767,  941,  943,  945,  767,  767,  965,  965,  932,  767,
 /*    80 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    90 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   100 */   767,  767,  767,  767,  767,  767,  811,  767,  809,  767,
 /*   110 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   120 */   767,  767,  767,  796,  767,  767,  767,  767,  767,  767,
 /*   130 */   788,  788,  788,  767,  767,  767,  788,  767,  788,  972,
 /*   140 */   976,  970,  958,  966,  957,  953,  951,  950,  980,  767,
 /*   150 */   788,  788,  788,  826,  822,  822,  788,  788,  844,  842,
 /*   160 */   840,  832,  838,  834,  836,  830,  814,  788,  820,  820,
 /*   170 */   788,  820,  788,  820,  788,  863,  881,  767,  981,  767,
 /*   180 */  1018,  971, 1008, 1007, 1014, 1006, 1005, 1004,  767,  767,
 /*   190 */   767, 1000, 1001, 1003, 1002,  767,  767, 1010, 1009,  767,
 /*   200 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   210 */   767,  767,  983,  767,  977,  973,  767,  767,  767,  767,
 /*   220 */   767,  767,  767,  767,  767,  893,  767,  767,  767,  767,
 /*   230 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   240 */   931,  767,  767,  767,  767,  942,  767,  767,  767,  767,
 /*   250 */   767,  767,  767,  767,  767,  967,  767,  959,  767,  767,
 /*   260 */   767,  767,  767,  905,  767,  767,  767,  767,  767,  767,
 /*   270 */   767,  767,  767,  767,  767,  767,  767,  767,  767, 1029,
 /*   280 */  1027,  767,  767,  767, 1023,  767,  767,  767, 1021,  767,
 /*   290 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   300 */   767,  767,  767,  767,  767,  767,  847,  767,  794,  792,
 /*   310 */   767,  784,  767,
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
    0,  /*     TOPICS => nothing */
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
    0,  /*      TOPIC => nothing */
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
    0,  /* PARTITIONS => nothing */
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
    0,  /*    SESSION => nothing */
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
  /*   46 */ "TOPICS",
  /*   47 */ "MNODES",
  /*   48 */ "DNODES",
  /*   49 */ "ACCOUNTS",
  /*   50 */ "USERS",
  /*   51 */ "MODULES",
  /*   52 */ "QUERIES",
  /*   53 */ "CONNECTIONS",
  /*   54 */ "STREAMS",
  /*   55 */ "VARIABLES",
  /*   56 */ "SCORES",
  /*   57 */ "GRANTS",
  /*   58 */ "VNODES",
  /*   59 */ "IPTOKEN",
  /*   60 */ "DOT",
  /*   61 */ "CREATE",
  /*   62 */ "TABLE",
  /*   63 */ "DATABASE",
  /*   64 */ "TABLES",
  /*   65 */ "STABLES",
  /*   66 */ "VGROUPS",
  /*   67 */ "DROP",
  /*   68 */ "STABLE",
  /*   69 */ "TOPIC",
  /*   70 */ "DNODE",
  /*   71 */ "USER",
  /*   72 */ "ACCOUNT",
  /*   73 */ "USE",
  /*   74 */ "DESCRIBE",
  /*   75 */ "ALTER",
  /*   76 */ "PASS",
  /*   77 */ "PRIVILEGE",
  /*   78 */ "LOCAL",
  /*   79 */ "IF",
  /*   80 */ "EXISTS",
  /*   81 */ "PPS",
  /*   82 */ "TSERIES",
  /*   83 */ "DBS",
  /*   84 */ "STORAGE",
  /*   85 */ "QTIME",
  /*   86 */ "CONNS",
  /*   87 */ "STATE",
  /*   88 */ "KEEP",
  /*   89 */ "CACHE",
  /*   90 */ "REPLICA",
  /*   91 */ "QUORUM",
  /*   92 */ "DAYS",
  /*   93 */ "MINROWS",
  /*   94 */ "MAXROWS",
  /*   95 */ "BLOCKS",
  /*   96 */ "CTIME",
  /*   97 */ "WAL",
  /*   98 */ "FSYNC",
  /*   99 */ "COMP",
  /*  100 */ "PRECISION",
  /*  101 */ "UPDATE",
  /*  102 */ "CACHELAST",
  /*  103 */ "PARTITIONS",
  /*  104 */ "LP",
  /*  105 */ "RP",
  /*  106 */ "UNSIGNED",
  /*  107 */ "TAGS",
  /*  108 */ "USING",
  /*  109 */ "COMMA",
  /*  110 */ "AS",
  /*  111 */ "NULL",
  /*  112 */ "SELECT",
  /*  113 */ "UNION",
  /*  114 */ "ALL",
  /*  115 */ "DISTINCT",
  /*  116 */ "FROM",
  /*  117 */ "VARIABLE",
  /*  118 */ "INTERVAL",
  /*  119 */ "SESSION",
  /*  120 */ "FILL",
  /*  121 */ "SLIDING",
  /*  122 */ "ORDER",
  /*  123 */ "BY",
  /*  124 */ "ASC",
  /*  125 */ "DESC",
  /*  126 */ "GROUP",
  /*  127 */ "HAVING",
  /*  128 */ "LIMIT",
  /*  129 */ "OFFSET",
  /*  130 */ "SLIMIT",
  /*  131 */ "SOFFSET",
  /*  132 */ "WHERE",
  /*  133 */ "NOW",
  /*  134 */ "RESET",
  /*  135 */ "QUERY",
  /*  136 */ "ADD",
  /*  137 */ "COLUMN",
  /*  138 */ "TAG",
  /*  139 */ "CHANGE",
  /*  140 */ "SET",
  /*  141 */ "KILL",
  /*  142 */ "CONNECTION",
  /*  143 */ "STREAM",
  /*  144 */ "COLON",
  /*  145 */ "ABORT",
  /*  146 */ "AFTER",
  /*  147 */ "ATTACH",
  /*  148 */ "BEFORE",
  /*  149 */ "BEGIN",
  /*  150 */ "CASCADE",
  /*  151 */ "CLUSTER",
  /*  152 */ "CONFLICT",
  /*  153 */ "COPY",
  /*  154 */ "DEFERRED",
  /*  155 */ "DELIMITERS",
  /*  156 */ "DETACH",
  /*  157 */ "EACH",
  /*  158 */ "END",
  /*  159 */ "EXPLAIN",
  /*  160 */ "FAIL",
  /*  161 */ "FOR",
  /*  162 */ "IGNORE",
  /*  163 */ "IMMEDIATE",
  /*  164 */ "INITIALLY",
  /*  165 */ "INSTEAD",
  /*  166 */ "MATCH",
  /*  167 */ "KEY",
  /*  168 */ "OF",
  /*  169 */ "RAISE",
  /*  170 */ "REPLACE",
  /*  171 */ "RESTRICT",
  /*  172 */ "ROW",
  /*  173 */ "STATEMENT",
  /*  174 */ "TRIGGER",
  /*  175 */ "VIEW",
  /*  176 */ "SEMI",
  /*  177 */ "NONE",
  /*  178 */ "PREV",
  /*  179 */ "LINEAR",
  /*  180 */ "IMPORT",
  /*  181 */ "METRIC",
  /*  182 */ "TBNAME",
  /*  183 */ "JOIN",
  /*  184 */ "METRICS",
  /*  185 */ "INSERT",
  /*  186 */ "INTO",
  /*  187 */ "VALUES",
  /*  188 */ "error",
  /*  189 */ "program",
  /*  190 */ "cmd",
  /*  191 */ "dbPrefix",
  /*  192 */ "ids",
  /*  193 */ "cpxName",
  /*  194 */ "ifexists",
  /*  195 */ "alter_db_optr",
  /*  196 */ "alter_topic_optr",
  /*  197 */ "acct_optr",
  /*  198 */ "ifnotexists",
  /*  199 */ "db_optr",
  /*  200 */ "topic_optr",
  /*  201 */ "pps",
  /*  202 */ "tseries",
  /*  203 */ "dbs",
  /*  204 */ "streams",
  /*  205 */ "storage",
  /*  206 */ "qtime",
  /*  207 */ "users",
  /*  208 */ "conns",
  /*  209 */ "state",
  /*  210 */ "keep",
  /*  211 */ "tagitemlist",
  /*  212 */ "cache",
  /*  213 */ "replica",
  /*  214 */ "quorum",
  /*  215 */ "days",
  /*  216 */ "minrows",
  /*  217 */ "maxrows",
  /*  218 */ "blocks",
  /*  219 */ "ctime",
  /*  220 */ "wal",
  /*  221 */ "fsync",
  /*  222 */ "comp",
  /*  223 */ "prec",
  /*  224 */ "update",
  /*  225 */ "cachelast",
  /*  226 */ "partitions",
  /*  227 */ "typename",
  /*  228 */ "signed",
  /*  229 */ "create_table_args",
  /*  230 */ "create_stable_args",
  /*  231 */ "create_table_list",
  /*  232 */ "create_from_stable",
  /*  233 */ "columnlist",
  /*  234 */ "tagNamelist",
  /*  235 */ "select",
  /*  236 */ "column",
  /*  237 */ "tagitem",
  /*  238 */ "selcollist",
  /*  239 */ "from",
  /*  240 */ "where_opt",
  /*  241 */ "interval_opt",
  /*  242 */ "session_option",
  /*  243 */ "fill_opt",
  /*  244 */ "sliding_opt",
  /*  245 */ "groupby_opt",
  /*  246 */ "orderby_opt",
  /*  247 */ "having_opt",
  /*  248 */ "slimit_opt",
  /*  249 */ "limit_opt",
  /*  250 */ "union",
  /*  251 */ "sclp",
  /*  252 */ "distinct",
  /*  253 */ "expr",
  /*  254 */ "as",
  /*  255 */ "tablelist",
  /*  256 */ "tmvar",
  /*  257 */ "sortlist",
  /*  258 */ "sortitem",
  /*  259 */ "item",
  /*  260 */ "sortorder",
  /*  261 */ "grouplist",
  /*  262 */ "exprlist",
  /*  263 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW MNODES",
 /*   4 */ "cmd ::= SHOW DNODES",
 /*   5 */ "cmd ::= SHOW ACCOUNTS",
 /*   6 */ "cmd ::= SHOW USERS",
 /*   7 */ "cmd ::= SHOW MODULES",
 /*   8 */ "cmd ::= SHOW QUERIES",
 /*   9 */ "cmd ::= SHOW CONNECTIONS",
 /*  10 */ "cmd ::= SHOW STREAMS",
 /*  11 */ "cmd ::= SHOW VARIABLES",
 /*  12 */ "cmd ::= SHOW SCORES",
 /*  13 */ "cmd ::= SHOW GRANTS",
 /*  14 */ "cmd ::= SHOW VNODES",
 /*  15 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  16 */ "dbPrefix ::=",
 /*  17 */ "dbPrefix ::= ids DOT",
 /*  18 */ "cpxName ::=",
 /*  19 */ "cpxName ::= DOT ids",
 /*  20 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  21 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  28 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  29 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  31 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  32 */ "cmd ::= DROP DNODE ids",
 /*  33 */ "cmd ::= DROP USER ids",
 /*  34 */ "cmd ::= DROP ACCOUNT ids",
 /*  35 */ "cmd ::= USE ids",
 /*  36 */ "cmd ::= DESCRIBE ids cpxName",
 /*  37 */ "cmd ::= ALTER USER ids PASS ids",
 /*  38 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  39 */ "cmd ::= ALTER DNODE ids ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  41 */ "cmd ::= ALTER LOCAL ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids ids",
 /*  43 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  44 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  45 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  47 */ "ids ::= ID",
 /*  48 */ "ids ::= STRING",
 /*  49 */ "ifexists ::= IF EXISTS",
 /*  50 */ "ifexists ::=",
 /*  51 */ "ifnotexists ::= IF NOT EXISTS",
 /*  52 */ "ifnotexists ::=",
 /*  53 */ "cmd ::= CREATE DNODE ids",
 /*  54 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  55 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  56 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  57 */ "cmd ::= CREATE USER ids PASS ids",
 /*  58 */ "pps ::=",
 /*  59 */ "pps ::= PPS INTEGER",
 /*  60 */ "tseries ::=",
 /*  61 */ "tseries ::= TSERIES INTEGER",
 /*  62 */ "dbs ::=",
 /*  63 */ "dbs ::= DBS INTEGER",
 /*  64 */ "streams ::=",
 /*  65 */ "streams ::= STREAMS INTEGER",
 /*  66 */ "storage ::=",
 /*  67 */ "storage ::= STORAGE INTEGER",
 /*  68 */ "qtime ::=",
 /*  69 */ "qtime ::= QTIME INTEGER",
 /*  70 */ "users ::=",
 /*  71 */ "users ::= USERS INTEGER",
 /*  72 */ "conns ::=",
 /*  73 */ "conns ::= CONNS INTEGER",
 /*  74 */ "state ::=",
 /*  75 */ "state ::= STATE ids",
 /*  76 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  77 */ "keep ::= KEEP tagitemlist",
 /*  78 */ "cache ::= CACHE INTEGER",
 /*  79 */ "replica ::= REPLICA INTEGER",
 /*  80 */ "quorum ::= QUORUM INTEGER",
 /*  81 */ "days ::= DAYS INTEGER",
 /*  82 */ "minrows ::= MINROWS INTEGER",
 /*  83 */ "maxrows ::= MAXROWS INTEGER",
 /*  84 */ "blocks ::= BLOCKS INTEGER",
 /*  85 */ "ctime ::= CTIME INTEGER",
 /*  86 */ "wal ::= WAL INTEGER",
 /*  87 */ "fsync ::= FSYNC INTEGER",
 /*  88 */ "comp ::= COMP INTEGER",
 /*  89 */ "prec ::= PRECISION STRING",
 /*  90 */ "update ::= UPDATE INTEGER",
 /*  91 */ "cachelast ::= CACHELAST INTEGER",
 /*  92 */ "partitions ::= PARTITIONS INTEGER",
 /*  93 */ "db_optr ::=",
 /*  94 */ "db_optr ::= db_optr cache",
 /*  95 */ "db_optr ::= db_optr replica",
 /*  96 */ "db_optr ::= db_optr quorum",
 /*  97 */ "db_optr ::= db_optr days",
 /*  98 */ "db_optr ::= db_optr minrows",
 /*  99 */ "db_optr ::= db_optr maxrows",
 /* 100 */ "db_optr ::= db_optr blocks",
 /* 101 */ "db_optr ::= db_optr ctime",
 /* 102 */ "db_optr ::= db_optr wal",
 /* 103 */ "db_optr ::= db_optr fsync",
 /* 104 */ "db_optr ::= db_optr comp",
 /* 105 */ "db_optr ::= db_optr prec",
 /* 106 */ "db_optr ::= db_optr keep",
 /* 107 */ "db_optr ::= db_optr update",
 /* 108 */ "db_optr ::= db_optr cachelast",
 /* 109 */ "topic_optr ::= db_optr",
 /* 110 */ "topic_optr ::= topic_optr partitions",
 /* 111 */ "alter_db_optr ::=",
 /* 112 */ "alter_db_optr ::= alter_db_optr replica",
 /* 113 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 114 */ "alter_db_optr ::= alter_db_optr keep",
 /* 115 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 116 */ "alter_db_optr ::= alter_db_optr comp",
 /* 117 */ "alter_db_optr ::= alter_db_optr wal",
 /* 118 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 119 */ "alter_db_optr ::= alter_db_optr update",
 /* 120 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 121 */ "alter_topic_optr ::= alter_db_optr",
 /* 122 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 123 */ "typename ::= ids",
 /* 124 */ "typename ::= ids LP signed RP",
 /* 125 */ "typename ::= ids UNSIGNED",
 /* 126 */ "signed ::= INTEGER",
 /* 127 */ "signed ::= PLUS INTEGER",
 /* 128 */ "signed ::= MINUS INTEGER",
 /* 129 */ "cmd ::= CREATE TABLE create_table_args",
 /* 130 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 131 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE TABLE create_table_list",
 /* 133 */ "create_table_list ::= create_from_stable",
 /* 134 */ "create_table_list ::= create_table_list create_from_stable",
 /* 135 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 136 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 137 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 139 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 140 */ "tagNamelist ::= ids",
 /* 141 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 142 */ "columnlist ::= columnlist COMMA column",
 /* 143 */ "columnlist ::= column",
 /* 144 */ "column ::= ids typename",
 /* 145 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 146 */ "tagitemlist ::= tagitem",
 /* 147 */ "tagitem ::= INTEGER",
 /* 148 */ "tagitem ::= FLOAT",
 /* 149 */ "tagitem ::= STRING",
 /* 150 */ "tagitem ::= BOOL",
 /* 151 */ "tagitem ::= NULL",
 /* 152 */ "tagitem ::= MINUS INTEGER",
 /* 153 */ "tagitem ::= MINUS FLOAT",
 /* 154 */ "tagitem ::= PLUS INTEGER",
 /* 155 */ "tagitem ::= PLUS FLOAT",
 /* 156 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 157 */ "union ::= select",
 /* 158 */ "union ::= LP union RP",
 /* 159 */ "union ::= union UNION ALL select",
 /* 160 */ "union ::= union UNION ALL LP select RP",
 /* 161 */ "cmd ::= union",
 /* 162 */ "select ::= SELECT selcollist",
 /* 163 */ "sclp ::= selcollist COMMA",
 /* 164 */ "sclp ::=",
 /* 165 */ "selcollist ::= sclp distinct expr as",
 /* 166 */ "selcollist ::= sclp STAR",
 /* 167 */ "as ::= AS ids",
 /* 168 */ "as ::= ids",
 /* 169 */ "as ::=",
 /* 170 */ "distinct ::= DISTINCT",
 /* 171 */ "distinct ::=",
 /* 172 */ "from ::= FROM tablelist",
 /* 173 */ "tablelist ::= ids cpxName",
 /* 174 */ "tablelist ::= ids cpxName ids",
 /* 175 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 176 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 177 */ "tmvar ::= VARIABLE",
 /* 178 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 179 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 180 */ "interval_opt ::=",
 /* 181 */ "session_option ::=",
 /* 182 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 183 */ "fill_opt ::=",
 /* 184 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 185 */ "fill_opt ::= FILL LP ID RP",
 /* 186 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 187 */ "sliding_opt ::=",
 /* 188 */ "orderby_opt ::=",
 /* 189 */ "orderby_opt ::= ORDER BY sortlist",
 /* 190 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 191 */ "sortlist ::= item sortorder",
 /* 192 */ "item ::= ids cpxName",
 /* 193 */ "sortorder ::= ASC",
 /* 194 */ "sortorder ::= DESC",
 /* 195 */ "sortorder ::=",
 /* 196 */ "groupby_opt ::=",
 /* 197 */ "groupby_opt ::= GROUP BY grouplist",
 /* 198 */ "grouplist ::= grouplist COMMA item",
 /* 199 */ "grouplist ::= item",
 /* 200 */ "having_opt ::=",
 /* 201 */ "having_opt ::= HAVING expr",
 /* 202 */ "limit_opt ::=",
 /* 203 */ "limit_opt ::= LIMIT signed",
 /* 204 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 205 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 206 */ "slimit_opt ::=",
 /* 207 */ "slimit_opt ::= SLIMIT signed",
 /* 208 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 209 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 210 */ "where_opt ::=",
 /* 211 */ "where_opt ::= WHERE expr",
 /* 212 */ "expr ::= LP expr RP",
 /* 213 */ "expr ::= ID",
 /* 214 */ "expr ::= ID DOT ID",
 /* 215 */ "expr ::= ID DOT STAR",
 /* 216 */ "expr ::= INTEGER",
 /* 217 */ "expr ::= MINUS INTEGER",
 /* 218 */ "expr ::= PLUS INTEGER",
 /* 219 */ "expr ::= FLOAT",
 /* 220 */ "expr ::= MINUS FLOAT",
 /* 221 */ "expr ::= PLUS FLOAT",
 /* 222 */ "expr ::= STRING",
 /* 223 */ "expr ::= NOW",
 /* 224 */ "expr ::= VARIABLE",
 /* 225 */ "expr ::= BOOL",
 /* 226 */ "expr ::= ID LP exprlist RP",
 /* 227 */ "expr ::= ID LP STAR RP",
 /* 228 */ "expr ::= expr IS NULL",
 /* 229 */ "expr ::= expr IS NOT NULL",
 /* 230 */ "expr ::= expr LT expr",
 /* 231 */ "expr ::= expr GT expr",
 /* 232 */ "expr ::= expr LE expr",
 /* 233 */ "expr ::= expr GE expr",
 /* 234 */ "expr ::= expr NE expr",
 /* 235 */ "expr ::= expr EQ expr",
 /* 236 */ "expr ::= expr BETWEEN expr AND expr",
 /* 237 */ "expr ::= expr AND expr",
 /* 238 */ "expr ::= expr OR expr",
 /* 239 */ "expr ::= expr PLUS expr",
 /* 240 */ "expr ::= expr MINUS expr",
 /* 241 */ "expr ::= expr STAR expr",
 /* 242 */ "expr ::= expr SLASH expr",
 /* 243 */ "expr ::= expr REM expr",
 /* 244 */ "expr ::= expr LIKE expr",
 /* 245 */ "expr ::= expr IN LP exprlist RP",
 /* 246 */ "exprlist ::= exprlist COMMA expritem",
 /* 247 */ "exprlist ::= expritem",
 /* 248 */ "expritem ::= expr",
 /* 249 */ "expritem ::=",
 /* 250 */ "cmd ::= RESET QUERY CACHE",
 /* 251 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 252 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 253 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 254 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 255 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 257 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 258 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 259 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 260 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 261 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 262 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 263 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 264 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 210: /* keep */
    case 211: /* tagitemlist */
    case 233: /* columnlist */
    case 234: /* tagNamelist */
    case 243: /* fill_opt */
    case 245: /* groupby_opt */
    case 246: /* orderby_opt */
    case 257: /* sortlist */
    case 261: /* grouplist */
{
taosArrayDestroy((yypminor->yy285));
}
      break;
    case 231: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy230));
}
      break;
    case 235: /* select */
{
destroyQuerySqlNode((yypminor->yy342));
}
      break;
    case 238: /* selcollist */
    case 251: /* sclp */
    case 262: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy285));
}
      break;
    case 240: /* where_opt */
    case 247: /* having_opt */
    case 253: /* expr */
    case 263: /* expritem */
{
tSqlExprDestroy((yypminor->yy178));
}
      break;
    case 250: /* union */
{
destroyAllSelectClause((yypminor->yy513));
}
      break;
    case 258: /* sortitem */
{
tVariantDestroy(&(yypminor->yy362));
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
  {  189,   -1 }, /* (0) program ::= cmd */
  {  190,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  190,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  190,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  190,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  190,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  190,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  190,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  190,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  190,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  190,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  190,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  190,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  190,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  190,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  190,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  191,    0 }, /* (16) dbPrefix ::= */
  {  191,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  193,    0 }, /* (18) cpxName ::= */
  {  193,   -2 }, /* (19) cpxName ::= DOT ids */
  {  190,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  190,   -4 }, /* (21) cmd ::= SHOW CREATE DATABASE ids */
  {  190,   -3 }, /* (22) cmd ::= SHOW dbPrefix TABLES */
  {  190,   -5 }, /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  190,   -3 }, /* (24) cmd ::= SHOW dbPrefix STABLES */
  {  190,   -5 }, /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  190,   -3 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS */
  {  190,   -4 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  190,   -5 }, /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
  {  190,   -5 }, /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
  {  190,   -4 }, /* (30) cmd ::= DROP DATABASE ifexists ids */
  {  190,   -4 }, /* (31) cmd ::= DROP TOPIC ifexists ids */
  {  190,   -3 }, /* (32) cmd ::= DROP DNODE ids */
  {  190,   -3 }, /* (33) cmd ::= DROP USER ids */
  {  190,   -3 }, /* (34) cmd ::= DROP ACCOUNT ids */
  {  190,   -2 }, /* (35) cmd ::= USE ids */
  {  190,   -3 }, /* (36) cmd ::= DESCRIBE ids cpxName */
  {  190,   -5 }, /* (37) cmd ::= ALTER USER ids PASS ids */
  {  190,   -5 }, /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  190,   -4 }, /* (39) cmd ::= ALTER DNODE ids ids */
  {  190,   -5 }, /* (40) cmd ::= ALTER DNODE ids ids ids */
  {  190,   -3 }, /* (41) cmd ::= ALTER LOCAL ids */
  {  190,   -4 }, /* (42) cmd ::= ALTER LOCAL ids ids */
  {  190,   -4 }, /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  190,   -4 }, /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  190,   -4 }, /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  190,   -6 }, /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  192,   -1 }, /* (47) ids ::= ID */
  {  192,   -1 }, /* (48) ids ::= STRING */
  {  194,   -2 }, /* (49) ifexists ::= IF EXISTS */
  {  194,    0 }, /* (50) ifexists ::= */
  {  198,   -3 }, /* (51) ifnotexists ::= IF NOT EXISTS */
  {  198,    0 }, /* (52) ifnotexists ::= */
  {  190,   -3 }, /* (53) cmd ::= CREATE DNODE ids */
  {  190,   -6 }, /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  190,   -5 }, /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  190,   -5 }, /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  190,   -5 }, /* (57) cmd ::= CREATE USER ids PASS ids */
  {  201,    0 }, /* (58) pps ::= */
  {  201,   -2 }, /* (59) pps ::= PPS INTEGER */
  {  202,    0 }, /* (60) tseries ::= */
  {  202,   -2 }, /* (61) tseries ::= TSERIES INTEGER */
  {  203,    0 }, /* (62) dbs ::= */
  {  203,   -2 }, /* (63) dbs ::= DBS INTEGER */
  {  204,    0 }, /* (64) streams ::= */
  {  204,   -2 }, /* (65) streams ::= STREAMS INTEGER */
  {  205,    0 }, /* (66) storage ::= */
  {  205,   -2 }, /* (67) storage ::= STORAGE INTEGER */
  {  206,    0 }, /* (68) qtime ::= */
  {  206,   -2 }, /* (69) qtime ::= QTIME INTEGER */
  {  207,    0 }, /* (70) users ::= */
  {  207,   -2 }, /* (71) users ::= USERS INTEGER */
  {  208,    0 }, /* (72) conns ::= */
  {  208,   -2 }, /* (73) conns ::= CONNS INTEGER */
  {  209,    0 }, /* (74) state ::= */
  {  209,   -2 }, /* (75) state ::= STATE ids */
  {  197,   -9 }, /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  210,   -2 }, /* (77) keep ::= KEEP tagitemlist */
  {  212,   -2 }, /* (78) cache ::= CACHE INTEGER */
  {  213,   -2 }, /* (79) replica ::= REPLICA INTEGER */
  {  214,   -2 }, /* (80) quorum ::= QUORUM INTEGER */
  {  215,   -2 }, /* (81) days ::= DAYS INTEGER */
  {  216,   -2 }, /* (82) minrows ::= MINROWS INTEGER */
  {  217,   -2 }, /* (83) maxrows ::= MAXROWS INTEGER */
  {  218,   -2 }, /* (84) blocks ::= BLOCKS INTEGER */
  {  219,   -2 }, /* (85) ctime ::= CTIME INTEGER */
  {  220,   -2 }, /* (86) wal ::= WAL INTEGER */
  {  221,   -2 }, /* (87) fsync ::= FSYNC INTEGER */
  {  222,   -2 }, /* (88) comp ::= COMP INTEGER */
  {  223,   -2 }, /* (89) prec ::= PRECISION STRING */
  {  224,   -2 }, /* (90) update ::= UPDATE INTEGER */
  {  225,   -2 }, /* (91) cachelast ::= CACHELAST INTEGER */
  {  226,   -2 }, /* (92) partitions ::= PARTITIONS INTEGER */
  {  199,    0 }, /* (93) db_optr ::= */
  {  199,   -2 }, /* (94) db_optr ::= db_optr cache */
  {  199,   -2 }, /* (95) db_optr ::= db_optr replica */
  {  199,   -2 }, /* (96) db_optr ::= db_optr quorum */
  {  199,   -2 }, /* (97) db_optr ::= db_optr days */
  {  199,   -2 }, /* (98) db_optr ::= db_optr minrows */
  {  199,   -2 }, /* (99) db_optr ::= db_optr maxrows */
  {  199,   -2 }, /* (100) db_optr ::= db_optr blocks */
  {  199,   -2 }, /* (101) db_optr ::= db_optr ctime */
  {  199,   -2 }, /* (102) db_optr ::= db_optr wal */
  {  199,   -2 }, /* (103) db_optr ::= db_optr fsync */
  {  199,   -2 }, /* (104) db_optr ::= db_optr comp */
  {  199,   -2 }, /* (105) db_optr ::= db_optr prec */
  {  199,   -2 }, /* (106) db_optr ::= db_optr keep */
  {  199,   -2 }, /* (107) db_optr ::= db_optr update */
  {  199,   -2 }, /* (108) db_optr ::= db_optr cachelast */
  {  200,   -1 }, /* (109) topic_optr ::= db_optr */
  {  200,   -2 }, /* (110) topic_optr ::= topic_optr partitions */
  {  195,    0 }, /* (111) alter_db_optr ::= */
  {  195,   -2 }, /* (112) alter_db_optr ::= alter_db_optr replica */
  {  195,   -2 }, /* (113) alter_db_optr ::= alter_db_optr quorum */
  {  195,   -2 }, /* (114) alter_db_optr ::= alter_db_optr keep */
  {  195,   -2 }, /* (115) alter_db_optr ::= alter_db_optr blocks */
  {  195,   -2 }, /* (116) alter_db_optr ::= alter_db_optr comp */
  {  195,   -2 }, /* (117) alter_db_optr ::= alter_db_optr wal */
  {  195,   -2 }, /* (118) alter_db_optr ::= alter_db_optr fsync */
  {  195,   -2 }, /* (119) alter_db_optr ::= alter_db_optr update */
  {  195,   -2 }, /* (120) alter_db_optr ::= alter_db_optr cachelast */
  {  196,   -1 }, /* (121) alter_topic_optr ::= alter_db_optr */
  {  196,   -2 }, /* (122) alter_topic_optr ::= alter_topic_optr partitions */
  {  227,   -1 }, /* (123) typename ::= ids */
  {  227,   -4 }, /* (124) typename ::= ids LP signed RP */
  {  227,   -2 }, /* (125) typename ::= ids UNSIGNED */
  {  228,   -1 }, /* (126) signed ::= INTEGER */
  {  228,   -2 }, /* (127) signed ::= PLUS INTEGER */
  {  228,   -2 }, /* (128) signed ::= MINUS INTEGER */
  {  190,   -3 }, /* (129) cmd ::= CREATE TABLE create_table_args */
  {  190,   -3 }, /* (130) cmd ::= CREATE TABLE create_stable_args */
  {  190,   -3 }, /* (131) cmd ::= CREATE STABLE create_stable_args */
  {  190,   -3 }, /* (132) cmd ::= CREATE TABLE create_table_list */
  {  231,   -1 }, /* (133) create_table_list ::= create_from_stable */
  {  231,   -2 }, /* (134) create_table_list ::= create_table_list create_from_stable */
  {  229,   -6 }, /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  230,  -10 }, /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  232,  -10 }, /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  232,  -13 }, /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  234,   -3 }, /* (139) tagNamelist ::= tagNamelist COMMA ids */
  {  234,   -1 }, /* (140) tagNamelist ::= ids */
  {  229,   -5 }, /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
  {  233,   -3 }, /* (142) columnlist ::= columnlist COMMA column */
  {  233,   -1 }, /* (143) columnlist ::= column */
  {  236,   -2 }, /* (144) column ::= ids typename */
  {  211,   -3 }, /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
  {  211,   -1 }, /* (146) tagitemlist ::= tagitem */
  {  237,   -1 }, /* (147) tagitem ::= INTEGER */
  {  237,   -1 }, /* (148) tagitem ::= FLOAT */
  {  237,   -1 }, /* (149) tagitem ::= STRING */
  {  237,   -1 }, /* (150) tagitem ::= BOOL */
  {  237,   -1 }, /* (151) tagitem ::= NULL */
  {  237,   -2 }, /* (152) tagitem ::= MINUS INTEGER */
  {  237,   -2 }, /* (153) tagitem ::= MINUS FLOAT */
  {  237,   -2 }, /* (154) tagitem ::= PLUS INTEGER */
  {  237,   -2 }, /* (155) tagitem ::= PLUS FLOAT */
  {  235,  -13 }, /* (156) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  250,   -1 }, /* (157) union ::= select */
  {  250,   -3 }, /* (158) union ::= LP union RP */
  {  250,   -4 }, /* (159) union ::= union UNION ALL select */
  {  250,   -6 }, /* (160) union ::= union UNION ALL LP select RP */
  {  190,   -1 }, /* (161) cmd ::= union */
  {  235,   -2 }, /* (162) select ::= SELECT selcollist */
  {  251,   -2 }, /* (163) sclp ::= selcollist COMMA */
  {  251,    0 }, /* (164) sclp ::= */
  {  238,   -4 }, /* (165) selcollist ::= sclp distinct expr as */
  {  238,   -2 }, /* (166) selcollist ::= sclp STAR */
  {  254,   -2 }, /* (167) as ::= AS ids */
  {  254,   -1 }, /* (168) as ::= ids */
  {  254,    0 }, /* (169) as ::= */
  {  252,   -1 }, /* (170) distinct ::= DISTINCT */
  {  252,    0 }, /* (171) distinct ::= */
  {  239,   -2 }, /* (172) from ::= FROM tablelist */
  {  255,   -2 }, /* (173) tablelist ::= ids cpxName */
  {  255,   -3 }, /* (174) tablelist ::= ids cpxName ids */
  {  255,   -4 }, /* (175) tablelist ::= tablelist COMMA ids cpxName */
  {  255,   -5 }, /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
  {  256,   -1 }, /* (177) tmvar ::= VARIABLE */
  {  241,   -4 }, /* (178) interval_opt ::= INTERVAL LP tmvar RP */
  {  241,   -6 }, /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  241,    0 }, /* (180) interval_opt ::= */
  {  242,    0 }, /* (181) session_option ::= */
  {  242,   -7 }, /* (182) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  243,    0 }, /* (183) fill_opt ::= */
  {  243,   -6 }, /* (184) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  243,   -4 }, /* (185) fill_opt ::= FILL LP ID RP */
  {  244,   -4 }, /* (186) sliding_opt ::= SLIDING LP tmvar RP */
  {  244,    0 }, /* (187) sliding_opt ::= */
  {  246,    0 }, /* (188) orderby_opt ::= */
  {  246,   -3 }, /* (189) orderby_opt ::= ORDER BY sortlist */
  {  257,   -4 }, /* (190) sortlist ::= sortlist COMMA item sortorder */
  {  257,   -2 }, /* (191) sortlist ::= item sortorder */
  {  259,   -2 }, /* (192) item ::= ids cpxName */
  {  260,   -1 }, /* (193) sortorder ::= ASC */
  {  260,   -1 }, /* (194) sortorder ::= DESC */
  {  260,    0 }, /* (195) sortorder ::= */
  {  245,    0 }, /* (196) groupby_opt ::= */
  {  245,   -3 }, /* (197) groupby_opt ::= GROUP BY grouplist */
  {  261,   -3 }, /* (198) grouplist ::= grouplist COMMA item */
  {  261,   -1 }, /* (199) grouplist ::= item */
  {  247,    0 }, /* (200) having_opt ::= */
  {  247,   -2 }, /* (201) having_opt ::= HAVING expr */
  {  249,    0 }, /* (202) limit_opt ::= */
  {  249,   -2 }, /* (203) limit_opt ::= LIMIT signed */
  {  249,   -4 }, /* (204) limit_opt ::= LIMIT signed OFFSET signed */
  {  249,   -4 }, /* (205) limit_opt ::= LIMIT signed COMMA signed */
  {  248,    0 }, /* (206) slimit_opt ::= */
  {  248,   -2 }, /* (207) slimit_opt ::= SLIMIT signed */
  {  248,   -4 }, /* (208) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  248,   -4 }, /* (209) slimit_opt ::= SLIMIT signed COMMA signed */
  {  240,    0 }, /* (210) where_opt ::= */
  {  240,   -2 }, /* (211) where_opt ::= WHERE expr */
  {  253,   -3 }, /* (212) expr ::= LP expr RP */
  {  253,   -1 }, /* (213) expr ::= ID */
  {  253,   -3 }, /* (214) expr ::= ID DOT ID */
  {  253,   -3 }, /* (215) expr ::= ID DOT STAR */
  {  253,   -1 }, /* (216) expr ::= INTEGER */
  {  253,   -2 }, /* (217) expr ::= MINUS INTEGER */
  {  253,   -2 }, /* (218) expr ::= PLUS INTEGER */
  {  253,   -1 }, /* (219) expr ::= FLOAT */
  {  253,   -2 }, /* (220) expr ::= MINUS FLOAT */
  {  253,   -2 }, /* (221) expr ::= PLUS FLOAT */
  {  253,   -1 }, /* (222) expr ::= STRING */
  {  253,   -1 }, /* (223) expr ::= NOW */
  {  253,   -1 }, /* (224) expr ::= VARIABLE */
  {  253,   -1 }, /* (225) expr ::= BOOL */
  {  253,   -4 }, /* (226) expr ::= ID LP exprlist RP */
  {  253,   -4 }, /* (227) expr ::= ID LP STAR RP */
  {  253,   -3 }, /* (228) expr ::= expr IS NULL */
  {  253,   -4 }, /* (229) expr ::= expr IS NOT NULL */
  {  253,   -3 }, /* (230) expr ::= expr LT expr */
  {  253,   -3 }, /* (231) expr ::= expr GT expr */
  {  253,   -3 }, /* (232) expr ::= expr LE expr */
  {  253,   -3 }, /* (233) expr ::= expr GE expr */
  {  253,   -3 }, /* (234) expr ::= expr NE expr */
  {  253,   -3 }, /* (235) expr ::= expr EQ expr */
  {  253,   -5 }, /* (236) expr ::= expr BETWEEN expr AND expr */
  {  253,   -3 }, /* (237) expr ::= expr AND expr */
  {  253,   -3 }, /* (238) expr ::= expr OR expr */
  {  253,   -3 }, /* (239) expr ::= expr PLUS expr */
  {  253,   -3 }, /* (240) expr ::= expr MINUS expr */
  {  253,   -3 }, /* (241) expr ::= expr STAR expr */
  {  253,   -3 }, /* (242) expr ::= expr SLASH expr */
  {  253,   -3 }, /* (243) expr ::= expr REM expr */
  {  253,   -3 }, /* (244) expr ::= expr LIKE expr */
  {  253,   -5 }, /* (245) expr ::= expr IN LP exprlist RP */
  {  262,   -3 }, /* (246) exprlist ::= exprlist COMMA expritem */
  {  262,   -1 }, /* (247) exprlist ::= expritem */
  {  263,   -1 }, /* (248) expritem ::= expr */
  {  263,    0 }, /* (249) expritem ::= */
  {  190,   -3 }, /* (250) cmd ::= RESET QUERY CACHE */
  {  190,   -7 }, /* (251) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  190,   -7 }, /* (252) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  190,   -7 }, /* (253) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  190,   -7 }, /* (254) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  190,   -8 }, /* (255) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  190,   -9 }, /* (256) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  190,   -7 }, /* (257) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  190,   -7 }, /* (258) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  190,   -7 }, /* (259) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  190,   -7 }, /* (260) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  190,   -8 }, /* (261) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  190,   -3 }, /* (262) cmd ::= KILL CONNECTION INTEGER */
  {  190,   -5 }, /* (263) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  190,   -5 }, /* (264) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 129: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==129);
      case 130: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==131);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 10: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 15: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 16: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 17: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 18: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 19: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 20: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 29: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 30: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 31: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 32: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 33: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 34: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 35: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 37: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 38: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 41: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 44: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==44);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &t);}
        break;
      case 45: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy187);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 47: /* ids ::= ID */
      case 48: /* ids ::= STRING */ yytestcase(yyruleno==48);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 49: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 50: /* ifexists ::= */
      case 52: /* ifnotexists ::= */ yytestcase(yyruleno==52);
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 51: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 53: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 54: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 55: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 56: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==56);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &yymsp[-2].minor.yy0);}
        break;
      case 57: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 58: /* pps ::= */
      case 60: /* tseries ::= */ yytestcase(yyruleno==60);
      case 62: /* dbs ::= */ yytestcase(yyruleno==62);
      case 64: /* streams ::= */ yytestcase(yyruleno==64);
      case 66: /* storage ::= */ yytestcase(yyruleno==66);
      case 68: /* qtime ::= */ yytestcase(yyruleno==68);
      case 70: /* users ::= */ yytestcase(yyruleno==70);
      case 72: /* conns ::= */ yytestcase(yyruleno==72);
      case 74: /* state ::= */ yytestcase(yyruleno==74);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 59: /* pps ::= PPS INTEGER */
      case 61: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==63);
      case 65: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==69);
      case 71: /* users ::= USERS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* state ::= STATE ids */ yytestcase(yyruleno==75);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 76: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy187.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy187.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy187.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy187.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy187.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy187.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy187.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy187.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy187.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy187 = yylhsminor.yy187;
        break;
      case 77: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy285 = yymsp[0].minor.yy285; }
        break;
      case 78: /* cache ::= CACHE INTEGER */
      case 79: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==79);
      case 80: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==80);
      case 81: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==81);
      case 82: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==85);
      case 86: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==86);
      case 87: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==87);
      case 88: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==88);
      case 89: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==89);
      case 90: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==90);
      case 91: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==91);
      case 92: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==92);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 93: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy526); yymsp[1].minor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 94: /* db_optr ::= db_optr cache */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 95: /* db_optr ::= db_optr replica */
      case 112: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==112);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 96: /* db_optr ::= db_optr quorum */
      case 113: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==113);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 97: /* db_optr ::= db_optr days */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 98: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 99: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 100: /* db_optr ::= db_optr blocks */
      case 115: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==115);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 101: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 102: /* db_optr ::= db_optr wal */
      case 117: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==117);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 103: /* db_optr ::= db_optr fsync */
      case 118: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==118);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 104: /* db_optr ::= db_optr comp */
      case 116: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==116);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 105: /* db_optr ::= db_optr prec */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 106: /* db_optr ::= db_optr keep */
      case 114: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==114);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.keep = yymsp[0].minor.yy285; }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 107: /* db_optr ::= db_optr update */
      case 119: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==119);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 108: /* db_optr ::= db_optr cachelast */
      case 120: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==120);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 109: /* topic_optr ::= db_optr */
      case 121: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==121);
{ yylhsminor.yy526 = yymsp[0].minor.yy526; yylhsminor.yy526.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 110: /* topic_optr ::= topic_optr partitions */
      case 122: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==122);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 111: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy526); yymsp[1].minor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 123: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy295, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy295 = yylhsminor.yy295;
        break;
      case 124: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy525 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy295, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy525;  // negative value of name length
    tSetColumnType(&yylhsminor.yy295, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy295 = yylhsminor.yy295;
        break;
      case 125: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy295, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy295 = yylhsminor.yy295;
        break;
      case 126: /* signed ::= INTEGER */
{ yylhsminor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 127: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 128: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy525 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 132: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy230;}
        break;
      case 133: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy230 = pCreateTable;
}
  yymsp[0].minor.yy230 = yylhsminor.yy230;
        break;
      case 134: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy230->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy230 = yymsp[-1].minor.yy230;
}
  yymsp[-1].minor.yy230 = yylhsminor.yy230;
        break;
      case 135: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy230 = tSetCreateTableInfo(yymsp[-1].minor.yy285, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy230, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy230 = yylhsminor.yy230;
        break;
      case 136: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy230 = tSetCreateTableInfo(yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy230, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy230 = yylhsminor.yy230;
        break;
      case 137: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy285, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy96 = yylhsminor.yy96;
        break;
      case 139: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy0); yylhsminor.yy285 = yymsp[-2].minor.yy285;  }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 140: /* tagNamelist ::= ids */
{yylhsminor.yy285 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy285, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy230 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy342, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy230, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy230 = yylhsminor.yy230;
        break;
      case 142: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy295); yylhsminor.yy285 = yymsp[-2].minor.yy285;  }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 143: /* columnlist ::= column */
{yylhsminor.yy285 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy285, &yymsp[0].minor.yy295);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 144: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy295, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy295);
}
  yymsp[-1].minor.yy295 = yylhsminor.yy295;
        break;
      case 145: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);    }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 146: /* tagitemlist ::= tagitem */
{ yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1); }
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 147: /* tagitem ::= INTEGER */
      case 148: /* tagitem ::= FLOAT */ yytestcase(yyruleno==148);
      case 149: /* tagitem ::= STRING */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= BOOL */ yytestcase(yyruleno==150);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy362, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy362 = yylhsminor.yy362;
        break;
      case 151: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy362, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy362 = yylhsminor.yy362;
        break;
      case 152: /* tagitem ::= MINUS INTEGER */
      case 153: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==155);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy362, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy362 = yylhsminor.yy362;
        break;
      case 156: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy342 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy285, yymsp[-10].minor.yy285, yymsp[-9].minor.yy178, yymsp[-4].minor.yy285, yymsp[-3].minor.yy285, &yymsp[-8].minor.yy376, &yymsp[-7].minor.yy523, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy285, &yymsp[0].minor.yy438, &yymsp[-1].minor.yy438);
}
  yymsp[-12].minor.yy342 = yylhsminor.yy342;
        break;
      case 157: /* union ::= select */
{ yylhsminor.yy513 = setSubclause(NULL, yymsp[0].minor.yy342); }
  yymsp[0].minor.yy513 = yylhsminor.yy513;
        break;
      case 158: /* union ::= LP union RP */
{ yymsp[-2].minor.yy513 = yymsp[-1].minor.yy513; }
        break;
      case 159: /* union ::= union UNION ALL select */
{ yylhsminor.yy513 = appendSelectClause(yymsp[-3].minor.yy513, yymsp[0].minor.yy342); }
  yymsp[-3].minor.yy513 = yylhsminor.yy513;
        break;
      case 160: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy513 = appendSelectClause(yymsp[-5].minor.yy513, yymsp[-1].minor.yy342); }
  yymsp[-5].minor.yy513 = yylhsminor.yy513;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy513, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy342 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy285, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy285 = yymsp[-1].minor.yy285;}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 164: /* sclp ::= */
      case 188: /* orderby_opt ::= */ yytestcase(yyruleno==188);
{yymsp[1].minor.yy285 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy285 = tSqlExprListAppend(yymsp[-3].minor.yy285, yymsp[-1].minor.yy178,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy285 = tSqlExprListAppend(yymsp[-1].minor.yy285, pNode, 0, 0);
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 167: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 168: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 170: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 173: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy285 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy285 = tVariantListAppendToken(yylhsminor.yy285, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 174: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy285 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy285 = tVariantListAppendToken(yylhsminor.yy285, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 175: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy285 = tVariantListAppendToken(yymsp[-3].minor.yy285, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy285 = tVariantListAppendToken(yylhsminor.yy285, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy285 = tVariantListAppendToken(yymsp[-4].minor.yy285, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy285 = tVariantListAppendToken(yylhsminor.yy285, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy285 = yylhsminor.yy285;
        break;
      case 177: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy376.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy376.offset.n = 0;}
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy376.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy376.offset = yymsp[-1].minor.yy0;}
        break;
      case 180: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy376, 0, sizeof(yymsp[1].minor.yy376));}
        break;
      case 181: /* session_option ::= */
{yymsp[1].minor.yy523.col.n = 0; yymsp[1].minor.yy523.gap.n = 0;}
        break;
      case 182: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy523.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy523.gap = yymsp[-1].minor.yy0;
}
        break;
      case 183: /* fill_opt ::= */
{ yymsp[1].minor.yy285 = 0;     }
        break;
      case 184: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy285, &A, -1, 0);
    yymsp[-5].minor.yy285 = yymsp[-1].minor.yy285;
}
        break;
      case 185: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy285 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 186: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 187: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 189: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 190: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy285 = tVariantListAppend(yymsp[-3].minor.yy285, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 191: /* sortlist ::= item sortorder */
{
  yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 192: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy362, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy362 = yylhsminor.yy362;
        break;
      case 193: /* sortorder ::= ASC */
{ yymsp[0].minor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 194: /* sortorder ::= DESC */
{ yymsp[0].minor.yy460 = TSDB_ORDER_DESC;}
        break;
      case 195: /* sortorder ::= */
{ yymsp[1].minor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 196: /* groupby_opt ::= */
{ yymsp[1].minor.yy285 = 0;}
        break;
      case 197: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 198: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);
}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 199: /* grouplist ::= item */
{
  yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1);
}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 200: /* having_opt ::= */
      case 210: /* where_opt ::= */ yytestcase(yyruleno==210);
      case 249: /* expritem ::= */ yytestcase(yyruleno==249);
{yymsp[1].minor.yy178 = 0;}
        break;
      case 201: /* having_opt ::= HAVING expr */
      case 211: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==211);
{yymsp[-1].minor.yy178 = yymsp[0].minor.yy178;}
        break;
      case 202: /* limit_opt ::= */
      case 206: /* slimit_opt ::= */ yytestcase(yyruleno==206);
{yymsp[1].minor.yy438.limit = -1; yymsp[1].minor.yy438.offset = 0;}
        break;
      case 203: /* limit_opt ::= LIMIT signed */
      case 207: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==207);
{yymsp[-1].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-1].minor.yy438.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy438.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 205: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 208: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy438.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 212: /* expr ::= LP expr RP */
{yylhsminor.yy178 = yymsp[-1].minor.yy178; yylhsminor.yy178->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy178->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 213: /* expr ::= ID */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 214: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 215: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 216: /* expr ::= INTEGER */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 217: /* expr ::= MINUS INTEGER */
      case 218: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==218);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 219: /* expr ::= FLOAT */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 220: /* expr ::= MINUS FLOAT */
      case 221: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==221);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 222: /* expr ::= STRING */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 223: /* expr ::= NOW */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 224: /* expr ::= VARIABLE */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 225: /* expr ::= BOOL */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 226: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy178 = tSqlExprCreateFunction(yymsp[-1].minor.yy285, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 227: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy178 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 228: /* expr ::= expr IS NULL */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 229: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-3].minor.yy178, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 230: /* expr ::= expr LT expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LT);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 231: /* expr ::= expr GT expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GT);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 232: /* expr ::= expr LE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 233: /* expr ::= expr GE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 234: /* expr ::= expr NE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_NE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 235: /* expr ::= expr EQ expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_EQ);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 236: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy178); yylhsminor.yy178 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy178, yymsp[-2].minor.yy178, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy178, TK_LE), TK_AND);}
  yymsp[-4].minor.yy178 = yylhsminor.yy178;
        break;
      case 237: /* expr ::= expr AND expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_AND);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 238: /* expr ::= expr OR expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_OR); }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 239: /* expr ::= expr PLUS expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_PLUS);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 240: /* expr ::= expr MINUS expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_MINUS); }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 241: /* expr ::= expr STAR expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_STAR);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 242: /* expr ::= expr SLASH expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_DIVIDE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 243: /* expr ::= expr REM expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_REM);   }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 244: /* expr ::= expr LIKE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LIKE);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 245: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-4].minor.yy178, (tSqlExpr*)yymsp[-1].minor.yy285, TK_IN); }
  yymsp[-4].minor.yy178 = yylhsminor.yy178;
        break;
      case 246: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy285 = tSqlExprListAppend(yymsp[-2].minor.yy285,yymsp[0].minor.yy178,0, 0);}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 247: /* exprlist ::= expritem */
{yylhsminor.yy285 = tSqlExprListAppend(0,yymsp[0].minor.yy178,0, 0);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 248: /* expritem ::= expr */
{yylhsminor.yy178 = yymsp[0].minor.yy178;}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 250: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 251: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 252: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 253: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 254: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy362, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 263: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 264: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
