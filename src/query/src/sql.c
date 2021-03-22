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
#define YYNOCODE 263
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SLimitVal yy18;
  SFromInfo* yy70;
  SSessionWindowVal yy87;
  SCreateDbInfo yy94;
  int yy116;
  SSubclauseInfo* yy141;
  tSqlExpr* yy170;
  SCreateTableSql* yy194;
  tVariant yy218;
  SIntervalVal yy220;
  SCreatedTableInfo yy252;
  SQuerySqlNode* yy254;
  SCreateAcctInfo yy419;
  SArray* yy429;
  TAOS_FIELD yy451;
  int64_t yy481;
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
#define YYNTOKEN             186
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
#define YY_ACTTAB_COUNT (676)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   910,  549,  201,  310,  205,  139,  937,    3,  166,  550,
 /*    10 */   768,  312,   17,   47,   48,  139,   51,   52,   30,  180,
 /*    20 */   213,   41,  180,   50,  260,   55,   53,   57,   54, 1016,
 /*    30 */   916,  208, 1017,   46,   45,  178,  180,   44,   43,   42,
 /*    40 */    47,   48,  219,   51,   52,  207, 1017,  213,   41,  549,
 /*    50 */    50,  260,   55,   53,   57,   54,  928,  550,  184,  202,
 /*    60 */    46,   45,  913,  218,   44,   43,   42,   48,  934,   51,
 /*    70 */    52,  240,  968,  213,   41,  549,   50,  260,   55,   53,
 /*    80 */    57,   54,  969,  550,  255,  220,   46,   45,  276,  916,
 /*    90 */    44,   43,   42,  503,  504,  505,  506,  507,  508,  509,
 /*   100 */   510,  511,  512,  513,  514,  515,  311,  628,   84,  230,
 /*   110 */    69,  916, 1013,   47,   48,   30,   51,   52,  296,   30,
 /*   120 */   213,   41,  549,   50,  260,   55,   53,   57,   54,   64,
 /*   130 */   550,  306,  714,   46,   45,  286,  285,   44,   43,   42,
 /*   140 */    47,   49,  904,   51,   52,  224, 1012,  213,   41,   65,
 /*   150 */    50,  260,   55,   53,   57,   54,  216,  916,  902,  913,
 /*   160 */    46,   45,  222,  912,   44,   43,   42,   23,  274,  305,
 /*   170 */   304,  273,  272,  271,  303,  270,  302,  301,  300,  269,
 /*   180 */   299,  298,  876,  139,  864,  865,  866,  867,  868,  869,
 /*   190 */   870,  871,  872,  873,  874,  875,  877,  878,   51,   52,
 /*   200 */   815,  139,  213,   41,  165,   50,  260,   55,   53,   57,
 /*   210 */    54, 1011,   18,   81,  226,   46,   45,  283,  282,   44,
 /*   220 */    43,   42,  212,  727,  928,   25,  718,   68,  721,  189,
 /*   230 */   724,  223,  212,  727,  278,  190,  718,  276,  721,  203,
 /*   240 */   724,  117,  116,  188,  899,  900,   29,  903,  257,  233,
 /*   250 */    77,   44,   43,   42,  209,  210,  237,  236,  259,  901,
 /*   260 */    23,  197,  305,  304,  209,  210,  225,  303,   78,  302,
 /*   270 */   301,  300,   73,  299,  298,  884,  104,   30,  882,  883,
 /*   280 */    36,  296,  720,  885,  723,  887,  888,  886,  667,  889,
 /*   290 */   890,   55,   53,   57,   54,  132,  309,  308,  125,   46,
 /*   300 */    45,  914,  239,   44,   43,   42,  102,  107,   30,  196,
 /*   310 */   664,   73,   96,  106,  112,  115,  105,   24,  217,   36,
 /*   320 */   674,  913,  109,    5,  155,   56,  261,   79,  243,   33,
 /*   330 */   154,   91,   86,   90,   30,   56,  173,  169,  726,   30,
 /*   340 */    70,   30,  171,  168,  120,  119,  118,   12,  726,  279,
 /*   350 */   211,   83,  913,   80,  725,  824,   46,   45,  245,  165,
 /*   360 */    44,   43,   42,  198,  725,  816,  671,  652,  182,  165,
 /*   370 */   649,  719,  650,  722,  651,  280,    1,  153,  913,  716,
 /*   380 */   284,   61,  288,  913,  183,  913,  241,  695,  696,  680,
 /*   390 */    31,  686,  687,  134,    6,   60,   20,  747,  227,  228,
 /*   400 */   728,   19,  638,   62,   19,  263,   31,  640,  265,   31,
 /*   410 */   639,   60,   82,   28,   60,  717,  266,   95,   94,   14,
 /*   420 */    13,   67,  730,  627,  185,  101,  100,  179,   16,   15,
 /*   430 */   979,  656,  654,  657,  655,  114,  113,  130,  128,  186,
 /*   440 */   187,  193,  194,  192,  177,  191,  181, 1026,  915,  978,
 /*   450 */   214,  975,  974,  215,  287,  131,   39,  936,  944,  946,
 /*   460 */   133,  137,  929,  244,  129,  150,  961,  960,  911,  909,
 /*   470 */   149,  679,  246,  151,  204,  152,  653,  250,  258,  827,
 /*   480 */   140,   66,  141,  268,   37,   63,  175,  926,   34,  277,
 /*   490 */   248,  823,  253,  142, 1031,   58,   92, 1030, 1028,  256,
 /*   500 */   156,  143,  281, 1025,   98, 1024, 1022,  254,  157,  845,
 /*   510 */    35,   32,   38,  252,  176,  812,  108,  810,  110,  111,
 /*   520 */   808,  807,  229,  167,  805,  804,  803,  802,  801,  800,
 /*   530 */   170,  172,  797,  795,  793,  791,  789,  174,  247,  242,
 /*   540 */    71,   74,  249,  962,   40,  297,  103,  289,  290,  291,
 /*   550 */   292,  293,  294,  295,  199,  221,  307,  766,  231,  232,
 /*   560 */   267,  765,  234,  235,  764,  200,  238,   87,   88,  752,
 /*   570 */   195,  243,   75,    8,  262,  806,   72,  659,  681,  135,
 /*   580 */    76,  121,  159,  846,  160,  161,  158,  162,  164,  122,
 /*   590 */   163,  799,    2,  123,  880,  124,  798,  790,  684,  144,
 /*   600 */   147,  145,  146,    4,  136,  148,  892,  206,  251,   26,
 /*   610 */   688,  138,    9,   10,  729,   27,    7,   11,   21,  731,
 /*   620 */    22,   85,  264,  591,  587,   83,  585,  584,  583,  580,
 /*   630 */   553,  275,   93,   89,   31,  630,   59,   97,  629,   99,
 /*   640 */   626,  575,  573,  565,  571,  567,  569,  563,  561,  594,
 /*   650 */   593,  592,  590,  589,  588,  586,  582,  581,   60,  551,
 /*   660 */   519,  517,  770,  769,  769,  769,  769,  769,  769,  769,
 /*   670 */   769,  769,  769,  769,  126,  127,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   190,    1,  189,  190,  209,  190,  190,  193,  194,    9,
 /*    10 */   187,  188,  251,   13,   14,  190,   16,   17,  190,  251,
 /*    20 */    20,   21,  251,   23,   24,   25,   26,   27,   28,  261,
 /*    30 */   235,  260,  261,   33,   34,  251,  251,   37,   38,   39,
 /*    40 */    13,   14,  232,   16,   17,  260,  261,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  233,    9,  251,  231,
 /*    60 */    33,   34,  234,  209,   37,   38,   39,   14,  252,   16,
 /*    70 */    17,  248,  257,   20,   21,    1,   23,   24,   25,   26,
 /*    80 */    27,   28,  257,    9,  259,  209,   33,   34,   79,  235,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,    5,  196,   61,
 /*   110 */   110,  235,  251,   13,   14,  190,   16,   17,   81,  190,
 /*   120 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  109,
 /*   130 */     9,  209,  105,   33,   34,   33,   34,   37,   38,   39,
 /*   140 */    13,   14,  230,   16,   17,   67,  251,   20,   21,  129,
 /*   150 */    23,   24,   25,   26,   27,   28,  231,  235,    0,  234,
 /*   160 */    33,   34,   67,  234,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  208,  190,  210,  211,  212,  213,  214,  215,
 /*   190 */   216,  217,  218,  219,  220,  221,  222,  223,   16,   17,
 /*   200 */   195,  190,   20,   21,  199,   23,   24,   25,   26,   27,
 /*   210 */    28,  251,   44,  196,  136,   33,   34,  139,  140,   37,
 /*   220 */    38,   39,    1,    2,  233,  104,    5,  196,    7,   61,
 /*   230 */     9,  136,    1,    2,  139,   67,    5,   79,    7,  248,
 /*   240 */     9,   73,   74,   75,  227,  228,  229,  230,  255,  135,
 /*   250 */   257,   37,   38,   39,   33,   34,  142,  143,   37,  228,
 /*   260 */    88,  251,   90,   91,   33,   34,  190,   95,  257,   97,
 /*   270 */    98,   99,  104,  101,  102,  208,   76,  190,  211,  212,
 /*   280 */   112,   81,    5,  216,    7,  218,  219,  220,   37,  222,
 /*   290 */   223,   25,   26,   27,   28,  190,   64,   65,   66,   33,
 /*   300 */    34,  225,  134,   37,   38,   39,   62,   63,  190,  141,
 /*   310 */   109,  104,   68,   69,   70,   71,   72,  116,  231,  112,
 /*   320 */   105,  234,   78,   62,   63,  104,   15,  236,  113,   68,
 /*   330 */    69,   70,   71,   72,  190,  104,   62,   63,  117,  190,
 /*   340 */   249,  190,   68,   69,   70,   71,   72,  104,  117,  231,
 /*   350 */    60,  108,  234,  110,  133,  195,   33,   34,  253,  199,
 /*   360 */    37,   38,   39,  251,  133,  195,  115,    2,  251,  199,
 /*   370 */     5,    5,    7,    7,    9,  231,  197,  198,  234,    1,
 /*   380 */   231,  109,  231,  234,  251,  234,  105,  124,  125,  105,
 /*   390 */   109,  105,  105,  109,  104,  109,  109,  105,   33,   34,
 /*   400 */   105,  109,  105,  131,  109,  105,  109,  105,  105,  109,
 /*   410 */   105,  109,  109,  104,  109,   37,  107,  137,  138,  137,
 /*   420 */   138,  104,  111,  106,  251,  137,  138,  251,  137,  138,
 /*   430 */   226,    5,    5,    7,    7,   76,   77,   62,   63,  251,
 /*   440 */   251,  251,  251,  251,  251,  251,  251,  235,  235,  226,
 /*   450 */   226,  226,  226,  226,  226,  190,  250,  190,  190,  190,
 /*   460 */   190,  190,  233,  233,   60,  190,  258,  258,  233,  190,
 /*   470 */   237,  117,  254,  190,  254,  190,  111,  119,  122,  190,
 /*   480 */   246,  128,  245,  190,  190,  130,  190,  247,  190,  190,
 /*   490 */   254,  190,  254,  244,  190,  127,  190,  190,  190,  126,
 /*   500 */   190,  243,  190,  190,  190,  190,  190,  121,  190,  190,
 /*   510 */   190,  190,  190,  120,  190,  190,  190,  190,  190,  190,
 /*   520 */   190,  190,  190,  190,  190,  190,  190,  190,  190,  190,
 /*   530 */   190,  190,  190,  190,  190,  190,  190,  190,  118,  191,
 /*   540 */   191,  191,  191,  191,  132,  103,   87,   86,   50,   83,
 /*   550 */    85,   54,   84,   82,  191,  191,   79,    5,  144,    5,
 /*   560 */   191,    5,  144,    5,    5,  191,  135,  196,  196,   89,
 /*   570 */   191,  113,  109,  104,  107,  191,  114,  105,  105,  104,
 /*   580 */   104,  192,  205,  207,  201,  204,  206,  202,  200,  192,
 /*   590 */   203,  191,  197,  192,  224,  192,  191,  191,  105,  242,
 /*   600 */   239,  241,  240,  193,  109,  238,  224,    1,  104,  109,
 /*   610 */   105,  104,  123,  123,  105,  109,  104,  104,  104,  111,
 /*   620 */   104,   76,  107,    9,    5,  108,    5,    5,    5,    5,
 /*   630 */    80,   15,  138,   76,  109,    5,   16,  138,    5,  138,
 /*   640 */   105,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   650 */     5,    5,    5,    5,    5,    5,    5,    5,  109,   80,
 /*   660 */    60,   59,    0,  262,  262,  262,  262,  262,  262,  262,
 /*   670 */   262,  262,  262,  262,   21,   21,  262,  262,  262,  262,
 /*   680 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   690 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   700 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   710 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   720 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   730 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   740 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   750 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   760 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   770 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   780 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   790 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   800 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   810 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   820 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   830 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   840 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   850 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   860 */   262,  262,
};
#define YY_SHIFT_COUNT    (312)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (662)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  172,  172,    9,  221,  231,   74,   74,
 /*    10 */    74,   74,   74,   74,   74,   74,   74,    0,   48,  231,
 /*    20 */   365,  365,  365,  365,  121,  207,   74,   74,   74,  158,
 /*    30 */    74,   74,  200,    9,   37,   37,  676,  676,  676,  231,
 /*    40 */   231,  231,  231,  231,  231,  231,  231,  231,  231,  231,
 /*    50 */   231,  231,  231,  231,  231,  231,  231,  231,  231,  365,
 /*    60 */   365,  102,  102,  102,  102,  102,  102,  102,   74,   74,
 /*    70 */   251,   74,  207,  207,   74,   74,   74,  263,  263,  201,
 /*    80 */   207,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*    90 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   100 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   110 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   120 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   130 */    74,  404,  404,  404,  354,  354,  354,  404,  354,  404,
 /*   140 */   353,  355,  368,  356,  373,  386,  393,  358,  420,  412,
 /*   150 */   404,  404,  404,  442,    9,    9,  404,  404,  459,  461,
 /*   160 */   498,  466,  465,  497,  468,  471,  442,  404,  477,  477,
 /*   170 */   404,  477,  404,  477,  404,  676,  676,   27,  100,  127,
 /*   180 */   100,  100,   53,  182,  266,  266,  266,  266,  244,  261,
 /*   190 */   274,  323,  323,  323,  323,   78,  114,  214,  214,  243,
 /*   200 */    95,  232,  281,  215,  284,  286,  287,  292,  295,  277,
 /*   210 */   366,  378,  290,  311,  272,   20,  297,  300,  302,  303,
 /*   220 */   305,  309,  280,  282,  288,  317,  291,  426,  427,  359,
 /*   230 */   375,  552,  414,  554,  556,  418,  558,  559,  480,  431,
 /*   240 */   458,  467,  469,  462,  472,  463,  473,  475,  493,  495,
 /*   250 */   476,  606,  504,  505,  507,  500,  489,  506,  490,  509,
 /*   260 */   512,  508,  513,  467,  514,  515,  516,  517,  545,  614,
 /*   270 */   619,  621,  622,  623,  624,  550,  616,  557,  494,  525,
 /*   280 */   525,  620,  499,  501,  525,  630,  633,  535,  525,  636,
 /*   290 */   637,  638,  639,  640,  641,  642,  643,  644,  645,  646,
 /*   300 */   647,  648,  649,  650,  651,  652,  549,  579,  653,  654,
 /*   310 */   600,  602,  662,
};
#define YY_REDUCE_COUNT (176)
#define YY_REDUCE_MIN   (-239)
#define YY_REDUCE_MAX   (410)
static const short yy_reduce_ofst[] = {
 /*     0 */  -177,  -26,  -26,   67,   67,   17, -229, -215, -172, -175,
 /*    10 */    -7,  -75,   87,  118,  144,  149,  151, -184, -187, -232,
 /*    20 */  -205, -146, -124,  -78,  105,   -9, -185,   11, -190,  -88,
 /*    30 */    76,  -71,    5,   31,  160,  170,   91,  179, -186, -239,
 /*    40 */  -216, -193, -139, -105,  -40,   10,  112,  117,  133,  173,
 /*    50 */   176,  188,  189,  190,  191,  192,  193,  194,  195,  212,
 /*    60 */   213,  204,  223,  224,  225,  226,  227,  228,  265,  267,
 /*    70 */   206,  268,  229,  230,  269,  270,  271,  208,  209,  233,
 /*    80 */   235,  275,  279,  283,  285,  289,  293,  294,  296,  298,
 /*    90 */   299,  301,  304,  306,  307,  308,  310,  312,  313,  314,
 /*   100 */   315,  316,  318,  319,  320,  321,  322,  324,  325,  326,
 /*   110 */   327,  328,  329,  330,  331,  332,  333,  334,  335,  336,
 /*   120 */   337,  338,  339,  340,  341,  342,  343,  344,  345,  346,
 /*   130 */   347,  348,  349,  350,  218,  220,  236,  351,  238,  352,
 /*   140 */   240,  234,  237,  249,  258,  357,  360,  362,  361,  367,
 /*   150 */   363,  364,  369,  370,  371,  372,  374,  379,  376,  380,
 /*   160 */   377,  383,  381,  385,  387,  388,  382,  384,  389,  397,
 /*   170 */   400,  401,  405,  403,  406,  395,  410,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   767,  879,  825,  891,  813,  822, 1019, 1019,  767,  767,
 /*    10 */   767,  767,  767,  767,  767,  767,  767,  938,  786, 1019,
 /*    20 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  822,
 /*    30 */   767,  767,  828,  822,  828,  828,  933,  863,  881,  767,
 /*    40 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    50 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    60 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    70 */   940,  943,  767,  767,  945,  767,  767,  965,  965,  931,
 /*    80 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    90 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   100 */   767,  767,  767,  767,  767,  767,  767,  767,  811,  767,
 /*   110 */   809,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   120 */   767,  767,  767,  767,  767,  796,  767,  767,  767,  767,
 /*   130 */   767,  788,  788,  788,  767,  767,  767,  788,  767,  788,
 /*   140 */   972,  976,  970,  958,  966,  957,  953,  951,  950,  980,
 /*   150 */   788,  788,  788,  826,  822,  822,  788,  788,  844,  842,
 /*   160 */   840,  832,  838,  834,  836,  830,  814,  788,  820,  820,
 /*   170 */   788,  820,  788,  820,  788,  863,  881,  767,  981,  767,
 /*   180 */  1018,  971, 1008, 1007, 1014, 1006, 1005, 1004,  767,  767,
 /*   190 */   767, 1000, 1001, 1003, 1002,  767,  767, 1010, 1009,  767,
 /*   200 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   210 */   767,  767,  983,  767,  977,  973,  767,  767,  767,  767,
 /*   220 */   767,  767,  767,  767,  767,  893,  767,  767,  767,  767,
 /*   230 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   240 */   930,  767,  767,  767,  767,  941,  767,  767,  767,  767,
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
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
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
  /*  181 */ "TBNAME",
  /*  182 */ "JOIN",
  /*  183 */ "INSERT",
  /*  184 */ "INTO",
  /*  185 */ "VALUES",
  /*  186 */ "error",
  /*  187 */ "program",
  /*  188 */ "cmd",
  /*  189 */ "dbPrefix",
  /*  190 */ "ids",
  /*  191 */ "cpxName",
  /*  192 */ "ifexists",
  /*  193 */ "alter_db_optr",
  /*  194 */ "alter_topic_optr",
  /*  195 */ "acct_optr",
  /*  196 */ "ifnotexists",
  /*  197 */ "db_optr",
  /*  198 */ "topic_optr",
  /*  199 */ "pps",
  /*  200 */ "tseries",
  /*  201 */ "dbs",
  /*  202 */ "streams",
  /*  203 */ "storage",
  /*  204 */ "qtime",
  /*  205 */ "users",
  /*  206 */ "conns",
  /*  207 */ "state",
  /*  208 */ "keep",
  /*  209 */ "tagitemlist",
  /*  210 */ "cache",
  /*  211 */ "replica",
  /*  212 */ "quorum",
  /*  213 */ "days",
  /*  214 */ "minrows",
  /*  215 */ "maxrows",
  /*  216 */ "blocks",
  /*  217 */ "ctime",
  /*  218 */ "wal",
  /*  219 */ "fsync",
  /*  220 */ "comp",
  /*  221 */ "prec",
  /*  222 */ "update",
  /*  223 */ "cachelast",
  /*  224 */ "partitions",
  /*  225 */ "typename",
  /*  226 */ "signed",
  /*  227 */ "create_table_args",
  /*  228 */ "create_stable_args",
  /*  229 */ "create_table_list",
  /*  230 */ "create_from_stable",
  /*  231 */ "columnlist",
  /*  232 */ "tagNamelist",
  /*  233 */ "select",
  /*  234 */ "column",
  /*  235 */ "tagitem",
  /*  236 */ "selcollist",
  /*  237 */ "from",
  /*  238 */ "where_opt",
  /*  239 */ "interval_opt",
  /*  240 */ "session_option",
  /*  241 */ "fill_opt",
  /*  242 */ "sliding_opt",
  /*  243 */ "groupby_opt",
  /*  244 */ "orderby_opt",
  /*  245 */ "having_opt",
  /*  246 */ "slimit_opt",
  /*  247 */ "limit_opt",
  /*  248 */ "union",
  /*  249 */ "sclp",
  /*  250 */ "distinct",
  /*  251 */ "expr",
  /*  252 */ "as",
  /*  253 */ "tablelist",
  /*  254 */ "tmvar",
  /*  255 */ "sortlist",
  /*  256 */ "sortitem",
  /*  257 */ "item",
  /*  258 */ "sortorder",
  /*  259 */ "grouplist",
  /*  260 */ "exprlist",
  /*  261 */ "expritem",
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
 /* 157 */ "select ::= LP select RP",
 /* 158 */ "union ::= select",
 /* 159 */ "union ::= union UNION ALL select",
 /* 160 */ "cmd ::= union",
 /* 161 */ "select ::= SELECT selcollist",
 /* 162 */ "sclp ::= selcollist COMMA",
 /* 163 */ "sclp ::=",
 /* 164 */ "selcollist ::= sclp distinct expr as",
 /* 165 */ "selcollist ::= sclp STAR",
 /* 166 */ "as ::= AS ids",
 /* 167 */ "as ::= ids",
 /* 168 */ "as ::=",
 /* 169 */ "distinct ::= DISTINCT",
 /* 170 */ "distinct ::=",
 /* 171 */ "from ::= FROM tablelist",
 /* 172 */ "from ::= FROM LP union RP",
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
    case 208: /* keep */
    case 209: /* tagitemlist */
    case 231: /* columnlist */
    case 232: /* tagNamelist */
    case 241: /* fill_opt */
    case 243: /* groupby_opt */
    case 244: /* orderby_opt */
    case 255: /* sortlist */
    case 259: /* grouplist */
{
taosArrayDestroy((yypminor->yy429));
}
      break;
    case 229: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy194));
}
      break;
    case 233: /* select */
{
destroyQuerySqlNode((yypminor->yy254));
}
      break;
    case 236: /* selcollist */
    case 249: /* sclp */
    case 260: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy429));
}
      break;
    case 238: /* where_opt */
    case 245: /* having_opt */
    case 251: /* expr */
    case 261: /* expritem */
{
tSqlExprDestroy((yypminor->yy170));
}
      break;
    case 248: /* union */
{
destroyAllSelectClause((yypminor->yy141));
}
      break;
    case 256: /* sortitem */
{
tVariantDestroy(&(yypminor->yy218));
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
  {  187,   -1 }, /* (0) program ::= cmd */
  {  188,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  188,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  188,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  188,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  188,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  188,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  188,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  188,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  188,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  188,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  188,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  188,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  188,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  188,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  188,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  189,    0 }, /* (16) dbPrefix ::= */
  {  189,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  191,    0 }, /* (18) cpxName ::= */
  {  191,   -2 }, /* (19) cpxName ::= DOT ids */
  {  188,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  188,   -4 }, /* (21) cmd ::= SHOW CREATE DATABASE ids */
  {  188,   -3 }, /* (22) cmd ::= SHOW dbPrefix TABLES */
  {  188,   -5 }, /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  188,   -3 }, /* (24) cmd ::= SHOW dbPrefix STABLES */
  {  188,   -5 }, /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  188,   -3 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS */
  {  188,   -4 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  188,   -5 }, /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
  {  188,   -5 }, /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
  {  188,   -4 }, /* (30) cmd ::= DROP DATABASE ifexists ids */
  {  188,   -4 }, /* (31) cmd ::= DROP TOPIC ifexists ids */
  {  188,   -3 }, /* (32) cmd ::= DROP DNODE ids */
  {  188,   -3 }, /* (33) cmd ::= DROP USER ids */
  {  188,   -3 }, /* (34) cmd ::= DROP ACCOUNT ids */
  {  188,   -2 }, /* (35) cmd ::= USE ids */
  {  188,   -3 }, /* (36) cmd ::= DESCRIBE ids cpxName */
  {  188,   -5 }, /* (37) cmd ::= ALTER USER ids PASS ids */
  {  188,   -5 }, /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  188,   -4 }, /* (39) cmd ::= ALTER DNODE ids ids */
  {  188,   -5 }, /* (40) cmd ::= ALTER DNODE ids ids ids */
  {  188,   -3 }, /* (41) cmd ::= ALTER LOCAL ids */
  {  188,   -4 }, /* (42) cmd ::= ALTER LOCAL ids ids */
  {  188,   -4 }, /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  188,   -4 }, /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  188,   -4 }, /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  188,   -6 }, /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  190,   -1 }, /* (47) ids ::= ID */
  {  190,   -1 }, /* (48) ids ::= STRING */
  {  192,   -2 }, /* (49) ifexists ::= IF EXISTS */
  {  192,    0 }, /* (50) ifexists ::= */
  {  196,   -3 }, /* (51) ifnotexists ::= IF NOT EXISTS */
  {  196,    0 }, /* (52) ifnotexists ::= */
  {  188,   -3 }, /* (53) cmd ::= CREATE DNODE ids */
  {  188,   -6 }, /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  188,   -5 }, /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  188,   -5 }, /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  188,   -5 }, /* (57) cmd ::= CREATE USER ids PASS ids */
  {  199,    0 }, /* (58) pps ::= */
  {  199,   -2 }, /* (59) pps ::= PPS INTEGER */
  {  200,    0 }, /* (60) tseries ::= */
  {  200,   -2 }, /* (61) tseries ::= TSERIES INTEGER */
  {  201,    0 }, /* (62) dbs ::= */
  {  201,   -2 }, /* (63) dbs ::= DBS INTEGER */
  {  202,    0 }, /* (64) streams ::= */
  {  202,   -2 }, /* (65) streams ::= STREAMS INTEGER */
  {  203,    0 }, /* (66) storage ::= */
  {  203,   -2 }, /* (67) storage ::= STORAGE INTEGER */
  {  204,    0 }, /* (68) qtime ::= */
  {  204,   -2 }, /* (69) qtime ::= QTIME INTEGER */
  {  205,    0 }, /* (70) users ::= */
  {  205,   -2 }, /* (71) users ::= USERS INTEGER */
  {  206,    0 }, /* (72) conns ::= */
  {  206,   -2 }, /* (73) conns ::= CONNS INTEGER */
  {  207,    0 }, /* (74) state ::= */
  {  207,   -2 }, /* (75) state ::= STATE ids */
  {  195,   -9 }, /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  208,   -2 }, /* (77) keep ::= KEEP tagitemlist */
  {  210,   -2 }, /* (78) cache ::= CACHE INTEGER */
  {  211,   -2 }, /* (79) replica ::= REPLICA INTEGER */
  {  212,   -2 }, /* (80) quorum ::= QUORUM INTEGER */
  {  213,   -2 }, /* (81) days ::= DAYS INTEGER */
  {  214,   -2 }, /* (82) minrows ::= MINROWS INTEGER */
  {  215,   -2 }, /* (83) maxrows ::= MAXROWS INTEGER */
  {  216,   -2 }, /* (84) blocks ::= BLOCKS INTEGER */
  {  217,   -2 }, /* (85) ctime ::= CTIME INTEGER */
  {  218,   -2 }, /* (86) wal ::= WAL INTEGER */
  {  219,   -2 }, /* (87) fsync ::= FSYNC INTEGER */
  {  220,   -2 }, /* (88) comp ::= COMP INTEGER */
  {  221,   -2 }, /* (89) prec ::= PRECISION STRING */
  {  222,   -2 }, /* (90) update ::= UPDATE INTEGER */
  {  223,   -2 }, /* (91) cachelast ::= CACHELAST INTEGER */
  {  224,   -2 }, /* (92) partitions ::= PARTITIONS INTEGER */
  {  197,    0 }, /* (93) db_optr ::= */
  {  197,   -2 }, /* (94) db_optr ::= db_optr cache */
  {  197,   -2 }, /* (95) db_optr ::= db_optr replica */
  {  197,   -2 }, /* (96) db_optr ::= db_optr quorum */
  {  197,   -2 }, /* (97) db_optr ::= db_optr days */
  {  197,   -2 }, /* (98) db_optr ::= db_optr minrows */
  {  197,   -2 }, /* (99) db_optr ::= db_optr maxrows */
  {  197,   -2 }, /* (100) db_optr ::= db_optr blocks */
  {  197,   -2 }, /* (101) db_optr ::= db_optr ctime */
  {  197,   -2 }, /* (102) db_optr ::= db_optr wal */
  {  197,   -2 }, /* (103) db_optr ::= db_optr fsync */
  {  197,   -2 }, /* (104) db_optr ::= db_optr comp */
  {  197,   -2 }, /* (105) db_optr ::= db_optr prec */
  {  197,   -2 }, /* (106) db_optr ::= db_optr keep */
  {  197,   -2 }, /* (107) db_optr ::= db_optr update */
  {  197,   -2 }, /* (108) db_optr ::= db_optr cachelast */
  {  198,   -1 }, /* (109) topic_optr ::= db_optr */
  {  198,   -2 }, /* (110) topic_optr ::= topic_optr partitions */
  {  193,    0 }, /* (111) alter_db_optr ::= */
  {  193,   -2 }, /* (112) alter_db_optr ::= alter_db_optr replica */
  {  193,   -2 }, /* (113) alter_db_optr ::= alter_db_optr quorum */
  {  193,   -2 }, /* (114) alter_db_optr ::= alter_db_optr keep */
  {  193,   -2 }, /* (115) alter_db_optr ::= alter_db_optr blocks */
  {  193,   -2 }, /* (116) alter_db_optr ::= alter_db_optr comp */
  {  193,   -2 }, /* (117) alter_db_optr ::= alter_db_optr wal */
  {  193,   -2 }, /* (118) alter_db_optr ::= alter_db_optr fsync */
  {  193,   -2 }, /* (119) alter_db_optr ::= alter_db_optr update */
  {  193,   -2 }, /* (120) alter_db_optr ::= alter_db_optr cachelast */
  {  194,   -1 }, /* (121) alter_topic_optr ::= alter_db_optr */
  {  194,   -2 }, /* (122) alter_topic_optr ::= alter_topic_optr partitions */
  {  225,   -1 }, /* (123) typename ::= ids */
  {  225,   -4 }, /* (124) typename ::= ids LP signed RP */
  {  225,   -2 }, /* (125) typename ::= ids UNSIGNED */
  {  226,   -1 }, /* (126) signed ::= INTEGER */
  {  226,   -2 }, /* (127) signed ::= PLUS INTEGER */
  {  226,   -2 }, /* (128) signed ::= MINUS INTEGER */
  {  188,   -3 }, /* (129) cmd ::= CREATE TABLE create_table_args */
  {  188,   -3 }, /* (130) cmd ::= CREATE TABLE create_stable_args */
  {  188,   -3 }, /* (131) cmd ::= CREATE STABLE create_stable_args */
  {  188,   -3 }, /* (132) cmd ::= CREATE TABLE create_table_list */
  {  229,   -1 }, /* (133) create_table_list ::= create_from_stable */
  {  229,   -2 }, /* (134) create_table_list ::= create_table_list create_from_stable */
  {  227,   -6 }, /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  228,  -10 }, /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  230,  -10 }, /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  230,  -13 }, /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  232,   -3 }, /* (139) tagNamelist ::= tagNamelist COMMA ids */
  {  232,   -1 }, /* (140) tagNamelist ::= ids */
  {  227,   -5 }, /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
  {  231,   -3 }, /* (142) columnlist ::= columnlist COMMA column */
  {  231,   -1 }, /* (143) columnlist ::= column */
  {  234,   -2 }, /* (144) column ::= ids typename */
  {  209,   -3 }, /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
  {  209,   -1 }, /* (146) tagitemlist ::= tagitem */
  {  235,   -1 }, /* (147) tagitem ::= INTEGER */
  {  235,   -1 }, /* (148) tagitem ::= FLOAT */
  {  235,   -1 }, /* (149) tagitem ::= STRING */
  {  235,   -1 }, /* (150) tagitem ::= BOOL */
  {  235,   -1 }, /* (151) tagitem ::= NULL */
  {  235,   -2 }, /* (152) tagitem ::= MINUS INTEGER */
  {  235,   -2 }, /* (153) tagitem ::= MINUS FLOAT */
  {  235,   -2 }, /* (154) tagitem ::= PLUS INTEGER */
  {  235,   -2 }, /* (155) tagitem ::= PLUS FLOAT */
  {  233,  -13 }, /* (156) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  233,   -3 }, /* (157) select ::= LP select RP */
  {  248,   -1 }, /* (158) union ::= select */
  {  248,   -4 }, /* (159) union ::= union UNION ALL select */
  {  188,   -1 }, /* (160) cmd ::= union */
  {  233,   -2 }, /* (161) select ::= SELECT selcollist */
  {  249,   -2 }, /* (162) sclp ::= selcollist COMMA */
  {  249,    0 }, /* (163) sclp ::= */
  {  236,   -4 }, /* (164) selcollist ::= sclp distinct expr as */
  {  236,   -2 }, /* (165) selcollist ::= sclp STAR */
  {  252,   -2 }, /* (166) as ::= AS ids */
  {  252,   -1 }, /* (167) as ::= ids */
  {  252,    0 }, /* (168) as ::= */
  {  250,   -1 }, /* (169) distinct ::= DISTINCT */
  {  250,    0 }, /* (170) distinct ::= */
  {  237,   -2 }, /* (171) from ::= FROM tablelist */
  {  237,   -4 }, /* (172) from ::= FROM LP union RP */
  {  253,   -2 }, /* (173) tablelist ::= ids cpxName */
  {  253,   -3 }, /* (174) tablelist ::= ids cpxName ids */
  {  253,   -4 }, /* (175) tablelist ::= tablelist COMMA ids cpxName */
  {  253,   -5 }, /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
  {  254,   -1 }, /* (177) tmvar ::= VARIABLE */
  {  239,   -4 }, /* (178) interval_opt ::= INTERVAL LP tmvar RP */
  {  239,   -6 }, /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  239,    0 }, /* (180) interval_opt ::= */
  {  240,    0 }, /* (181) session_option ::= */
  {  240,   -7 }, /* (182) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  241,    0 }, /* (183) fill_opt ::= */
  {  241,   -6 }, /* (184) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  241,   -4 }, /* (185) fill_opt ::= FILL LP ID RP */
  {  242,   -4 }, /* (186) sliding_opt ::= SLIDING LP tmvar RP */
  {  242,    0 }, /* (187) sliding_opt ::= */
  {  244,    0 }, /* (188) orderby_opt ::= */
  {  244,   -3 }, /* (189) orderby_opt ::= ORDER BY sortlist */
  {  255,   -4 }, /* (190) sortlist ::= sortlist COMMA item sortorder */
  {  255,   -2 }, /* (191) sortlist ::= item sortorder */
  {  257,   -2 }, /* (192) item ::= ids cpxName */
  {  258,   -1 }, /* (193) sortorder ::= ASC */
  {  258,   -1 }, /* (194) sortorder ::= DESC */
  {  258,    0 }, /* (195) sortorder ::= */
  {  243,    0 }, /* (196) groupby_opt ::= */
  {  243,   -3 }, /* (197) groupby_opt ::= GROUP BY grouplist */
  {  259,   -3 }, /* (198) grouplist ::= grouplist COMMA item */
  {  259,   -1 }, /* (199) grouplist ::= item */
  {  245,    0 }, /* (200) having_opt ::= */
  {  245,   -2 }, /* (201) having_opt ::= HAVING expr */
  {  247,    0 }, /* (202) limit_opt ::= */
  {  247,   -2 }, /* (203) limit_opt ::= LIMIT signed */
  {  247,   -4 }, /* (204) limit_opt ::= LIMIT signed OFFSET signed */
  {  247,   -4 }, /* (205) limit_opt ::= LIMIT signed COMMA signed */
  {  246,    0 }, /* (206) slimit_opt ::= */
  {  246,   -2 }, /* (207) slimit_opt ::= SLIMIT signed */
  {  246,   -4 }, /* (208) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  246,   -4 }, /* (209) slimit_opt ::= SLIMIT signed COMMA signed */
  {  238,    0 }, /* (210) where_opt ::= */
  {  238,   -2 }, /* (211) where_opt ::= WHERE expr */
  {  251,   -3 }, /* (212) expr ::= LP expr RP */
  {  251,   -1 }, /* (213) expr ::= ID */
  {  251,   -3 }, /* (214) expr ::= ID DOT ID */
  {  251,   -3 }, /* (215) expr ::= ID DOT STAR */
  {  251,   -1 }, /* (216) expr ::= INTEGER */
  {  251,   -2 }, /* (217) expr ::= MINUS INTEGER */
  {  251,   -2 }, /* (218) expr ::= PLUS INTEGER */
  {  251,   -1 }, /* (219) expr ::= FLOAT */
  {  251,   -2 }, /* (220) expr ::= MINUS FLOAT */
  {  251,   -2 }, /* (221) expr ::= PLUS FLOAT */
  {  251,   -1 }, /* (222) expr ::= STRING */
  {  251,   -1 }, /* (223) expr ::= NOW */
  {  251,   -1 }, /* (224) expr ::= VARIABLE */
  {  251,   -1 }, /* (225) expr ::= BOOL */
  {  251,   -4 }, /* (226) expr ::= ID LP exprlist RP */
  {  251,   -4 }, /* (227) expr ::= ID LP STAR RP */
  {  251,   -3 }, /* (228) expr ::= expr IS NULL */
  {  251,   -4 }, /* (229) expr ::= expr IS NOT NULL */
  {  251,   -3 }, /* (230) expr ::= expr LT expr */
  {  251,   -3 }, /* (231) expr ::= expr GT expr */
  {  251,   -3 }, /* (232) expr ::= expr LE expr */
  {  251,   -3 }, /* (233) expr ::= expr GE expr */
  {  251,   -3 }, /* (234) expr ::= expr NE expr */
  {  251,   -3 }, /* (235) expr ::= expr EQ expr */
  {  251,   -5 }, /* (236) expr ::= expr BETWEEN expr AND expr */
  {  251,   -3 }, /* (237) expr ::= expr AND expr */
  {  251,   -3 }, /* (238) expr ::= expr OR expr */
  {  251,   -3 }, /* (239) expr ::= expr PLUS expr */
  {  251,   -3 }, /* (240) expr ::= expr MINUS expr */
  {  251,   -3 }, /* (241) expr ::= expr STAR expr */
  {  251,   -3 }, /* (242) expr ::= expr SLASH expr */
  {  251,   -3 }, /* (243) expr ::= expr REM expr */
  {  251,   -3 }, /* (244) expr ::= expr LIKE expr */
  {  251,   -5 }, /* (245) expr ::= expr IN LP exprlist RP */
  {  260,   -3 }, /* (246) exprlist ::= exprlist COMMA expritem */
  {  260,   -1 }, /* (247) exprlist ::= expritem */
  {  261,   -1 }, /* (248) expritem ::= expr */
  {  261,    0 }, /* (249) expritem ::= */
  {  188,   -3 }, /* (250) cmd ::= RESET QUERY CACHE */
  {  188,   -7 }, /* (251) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  188,   -7 }, /* (252) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  188,   -7 }, /* (253) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  188,   -7 }, /* (254) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  188,   -8 }, /* (255) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  188,   -9 }, /* (256) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  188,   -7 }, /* (257) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  188,   -7 }, /* (258) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  188,   -7 }, /* (259) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  188,   -7 }, /* (260) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  188,   -8 }, /* (261) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  188,   -3 }, /* (262) cmd ::= KILL CONNECTION INTEGER */
  {  188,   -5 }, /* (263) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  188,   -5 }, /* (264) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy94, &t);}
        break;
      case 45: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy419);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy419);}
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
      case 170: /* distinct ::= */ yytestcase(yyruleno==170);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 51: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 53: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 54: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy419);}
        break;
      case 55: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 56: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==56);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy94, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy419.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy419.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy419.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy419.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy419.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy419.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy419.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy419.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy419.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy419 = yylhsminor.yy419;
        break;
      case 77: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy429 = yymsp[0].minor.yy429; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy94); yymsp[1].minor.yy94.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 94: /* db_optr ::= db_optr cache */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 95: /* db_optr ::= db_optr replica */
      case 112: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==112);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 96: /* db_optr ::= db_optr quorum */
      case 113: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==113);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 97: /* db_optr ::= db_optr days */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 98: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 99: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 100: /* db_optr ::= db_optr blocks */
      case 115: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==115);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 101: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 102: /* db_optr ::= db_optr wal */
      case 117: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==117);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 103: /* db_optr ::= db_optr fsync */
      case 118: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==118);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 104: /* db_optr ::= db_optr comp */
      case 116: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==116);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 105: /* db_optr ::= db_optr prec */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 106: /* db_optr ::= db_optr keep */
      case 114: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==114);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.keep = yymsp[0].minor.yy429; }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 107: /* db_optr ::= db_optr update */
      case 119: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==119);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 108: /* db_optr ::= db_optr cachelast */
      case 120: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==120);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 109: /* topic_optr ::= db_optr */
      case 121: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==121);
{ yylhsminor.yy94 = yymsp[0].minor.yy94; yylhsminor.yy94.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy94 = yylhsminor.yy94;
        break;
      case 110: /* topic_optr ::= topic_optr partitions */
      case 122: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==122);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 111: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy94); yymsp[1].minor.yy94.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 123: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy451, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy451 = yylhsminor.yy451;
        break;
      case 124: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy481 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy451, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy481;  // negative value of name length
    tSetColumnType(&yylhsminor.yy451, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy451 = yylhsminor.yy451;
        break;
      case 125: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy451, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy451 = yylhsminor.yy451;
        break;
      case 126: /* signed ::= INTEGER */
{ yylhsminor.yy481 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy481 = yylhsminor.yy481;
        break;
      case 127: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy481 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 128: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy481 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 132: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy194;}
        break;
      case 133: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy252);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy194 = pCreateTable;
}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 134: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy194->childTableInfo, &yymsp[0].minor.yy252);
  yylhsminor.yy194 = yymsp[-1].minor.yy194;
}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 135: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy194 = tSetCreateTableInfo(yymsp[-1].minor.yy429, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy194, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy194 = yylhsminor.yy194;
        break;
      case 136: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy194 = tSetCreateTableInfo(yymsp[-5].minor.yy429, yymsp[-1].minor.yy429, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy194, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy194 = yylhsminor.yy194;
        break;
      case 137: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy252 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy429, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy252 = yylhsminor.yy252;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy252 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy429, yymsp[-1].minor.yy429, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy252 = yylhsminor.yy252;
        break;
      case 139: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy429, &yymsp[0].minor.yy0); yylhsminor.yy429 = yymsp[-2].minor.yy429;  }
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 140: /* tagNamelist ::= ids */
{yylhsminor.yy429 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy429, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy194 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy254, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy194, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 142: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy429, &yymsp[0].minor.yy451); yylhsminor.yy429 = yymsp[-2].minor.yy429;  }
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 143: /* columnlist ::= column */
{yylhsminor.yy429 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy429, &yymsp[0].minor.yy451);}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 144: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy451, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy451);
}
  yymsp[-1].minor.yy451 = yylhsminor.yy451;
        break;
      case 145: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy429 = tVariantListAppend(yymsp[-2].minor.yy429, &yymsp[0].minor.yy218, -1);    }
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 146: /* tagitemlist ::= tagitem */
{ yylhsminor.yy429 = tVariantListAppend(NULL, &yymsp[0].minor.yy218, -1); }
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 147: /* tagitem ::= INTEGER */
      case 148: /* tagitem ::= FLOAT */ yytestcase(yyruleno==148);
      case 149: /* tagitem ::= STRING */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= BOOL */ yytestcase(yyruleno==150);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy218, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy218 = yylhsminor.yy218;
        break;
      case 151: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy218, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy218 = yylhsminor.yy218;
        break;
      case 152: /* tagitem ::= MINUS INTEGER */
      case 153: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==155);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy218, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy218 = yylhsminor.yy218;
        break;
      case 156: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy254 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy429, yymsp[-10].minor.yy70, yymsp[-9].minor.yy170, yymsp[-4].minor.yy429, yymsp[-3].minor.yy429, &yymsp[-8].minor.yy220, &yymsp[-7].minor.yy87, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy429, &yymsp[0].minor.yy18, &yymsp[-1].minor.yy18);
}
  yymsp[-12].minor.yy254 = yylhsminor.yy254;
        break;
      case 157: /* select ::= LP select RP */
{yymsp[-2].minor.yy254 = yymsp[-1].minor.yy254;}
        break;
      case 158: /* union ::= select */
{ yylhsminor.yy141 = setSubclause(NULL, yymsp[0].minor.yy254); }
  yymsp[0].minor.yy141 = yylhsminor.yy141;
        break;
      case 159: /* union ::= union UNION ALL select */
{ yylhsminor.yy141 = appendSelectClause(yymsp[-3].minor.yy141, yymsp[0].minor.yy254); }
  yymsp[-3].minor.yy141 = yylhsminor.yy141;
        break;
      case 160: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy141, NULL, TSDB_SQL_SELECT); }
        break;
      case 161: /* select ::= SELECT selcollist */
{
  yylhsminor.yy254 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy429, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 162: /* sclp ::= selcollist COMMA */
{yylhsminor.yy429 = yymsp[-1].minor.yy429;}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 163: /* sclp ::= */
      case 188: /* orderby_opt ::= */ yytestcase(yyruleno==188);
{yymsp[1].minor.yy429 = 0;}
        break;
      case 164: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy429 = tSqlExprListAppend(yymsp[-3].minor.yy429, yymsp[-1].minor.yy170,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy429 = yylhsminor.yy429;
        break;
      case 165: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy429 = tSqlExprListAppend(yymsp[-1].minor.yy429, pNode, 0, 0);
}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 166: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 167: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 168: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 169: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 171: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy70 = yymsp[0].minor.yy70;}
        break;
      case 172: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy70 = setSubquery(NULL, yymsp[-1].minor.yy141);}
        break;
      case 173: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy70 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy70 = yylhsminor.yy70;
        break;
      case 174: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy70 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 175: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy70 = setTableNameList(yymsp[-3].minor.yy70, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy70 = yylhsminor.yy70;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy70 = setTableNameList(yymsp[-4].minor.yy70, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy70 = yylhsminor.yy70;
        break;
      case 177: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy220.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy220.offset.n = 0;}
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy220.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy220.offset = yymsp[-1].minor.yy0;}
        break;
      case 180: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy220, 0, sizeof(yymsp[1].minor.yy220));}
        break;
      case 181: /* session_option ::= */
{yymsp[1].minor.yy87.col.n = 0; yymsp[1].minor.yy87.gap.n = 0;}
        break;
      case 182: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy87.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy87.gap = yymsp[-1].minor.yy0;
}
        break;
      case 183: /* fill_opt ::= */
{ yymsp[1].minor.yy429 = 0;     }
        break;
      case 184: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy429, &A, -1, 0);
    yymsp[-5].minor.yy429 = yymsp[-1].minor.yy429;
}
        break;
      case 185: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy429 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 186: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 187: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 189: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy429 = yymsp[0].minor.yy429;}
        break;
      case 190: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy429 = tVariantListAppend(yymsp[-3].minor.yy429, &yymsp[-1].minor.yy218, yymsp[0].minor.yy116);
}
  yymsp[-3].minor.yy429 = yylhsminor.yy429;
        break;
      case 191: /* sortlist ::= item sortorder */
{
  yylhsminor.yy429 = tVariantListAppend(NULL, &yymsp[-1].minor.yy218, yymsp[0].minor.yy116);
}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 192: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy218, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy218 = yylhsminor.yy218;
        break;
      case 193: /* sortorder ::= ASC */
{ yymsp[0].minor.yy116 = TSDB_ORDER_ASC; }
        break;
      case 194: /* sortorder ::= DESC */
{ yymsp[0].minor.yy116 = TSDB_ORDER_DESC;}
        break;
      case 195: /* sortorder ::= */
{ yymsp[1].minor.yy116 = TSDB_ORDER_ASC; }
        break;
      case 196: /* groupby_opt ::= */
{ yymsp[1].minor.yy429 = 0;}
        break;
      case 197: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy429 = yymsp[0].minor.yy429;}
        break;
      case 198: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy429 = tVariantListAppend(yymsp[-2].minor.yy429, &yymsp[0].minor.yy218, -1);
}
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 199: /* grouplist ::= item */
{
  yylhsminor.yy429 = tVariantListAppend(NULL, &yymsp[0].minor.yy218, -1);
}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 200: /* having_opt ::= */
      case 210: /* where_opt ::= */ yytestcase(yyruleno==210);
      case 249: /* expritem ::= */ yytestcase(yyruleno==249);
{yymsp[1].minor.yy170 = 0;}
        break;
      case 201: /* having_opt ::= HAVING expr */
      case 211: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==211);
{yymsp[-1].minor.yy170 = yymsp[0].minor.yy170;}
        break;
      case 202: /* limit_opt ::= */
      case 206: /* slimit_opt ::= */ yytestcase(yyruleno==206);
{yymsp[1].minor.yy18.limit = -1; yymsp[1].minor.yy18.offset = 0;}
        break;
      case 203: /* limit_opt ::= LIMIT signed */
      case 207: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==207);
{yymsp[-1].minor.yy18.limit = yymsp[0].minor.yy481;  yymsp[-1].minor.yy18.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy18.limit = yymsp[-2].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[0].minor.yy481;}
        break;
      case 205: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy18.limit = yymsp[0].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[-2].minor.yy481;}
        break;
      case 208: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy18.limit = yymsp[-2].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[0].minor.yy481;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy18.limit = yymsp[0].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[-2].minor.yy481;}
        break;
      case 212: /* expr ::= LP expr RP */
{yylhsminor.yy170 = yymsp[-1].minor.yy170; yylhsminor.yy170->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy170->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 213: /* expr ::= ID */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 214: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 215: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 216: /* expr ::= INTEGER */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 217: /* expr ::= MINUS INTEGER */
      case 218: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==218);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy170 = yylhsminor.yy170;
        break;
      case 219: /* expr ::= FLOAT */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 220: /* expr ::= MINUS FLOAT */
      case 221: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==221);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy170 = yylhsminor.yy170;
        break;
      case 222: /* expr ::= STRING */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 223: /* expr ::= NOW */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 224: /* expr ::= VARIABLE */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 225: /* expr ::= BOOL */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 226: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy170 = tSqlExprCreateFunction(yymsp[-1].minor.yy429, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy170 = yylhsminor.yy170;
        break;
      case 227: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy170 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy170 = yylhsminor.yy170;
        break;
      case 228: /* expr ::= expr IS NULL */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 229: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-3].minor.yy170, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy170 = yylhsminor.yy170;
        break;
      case 230: /* expr ::= expr LT expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_LT);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 231: /* expr ::= expr GT expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_GT);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 232: /* expr ::= expr LE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_LE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 233: /* expr ::= expr GE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_GE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 234: /* expr ::= expr NE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_NE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 235: /* expr ::= expr EQ expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_EQ);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 236: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy170); yylhsminor.yy170 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy170, yymsp[-2].minor.yy170, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy170, TK_LE), TK_AND);}
  yymsp[-4].minor.yy170 = yylhsminor.yy170;
        break;
      case 237: /* expr ::= expr AND expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_AND);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 238: /* expr ::= expr OR expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_OR); }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 239: /* expr ::= expr PLUS expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_PLUS);  }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 240: /* expr ::= expr MINUS expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_MINUS); }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 241: /* expr ::= expr STAR expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_STAR);  }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 242: /* expr ::= expr SLASH expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_DIVIDE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 243: /* expr ::= expr REM expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_REM);   }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 244: /* expr ::= expr LIKE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_LIKE);  }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 245: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-4].minor.yy170, (tSqlExpr*)yymsp[-1].minor.yy429, TK_IN); }
  yymsp[-4].minor.yy170 = yylhsminor.yy170;
        break;
      case 246: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy429 = tSqlExprListAppend(yymsp[-2].minor.yy429,yymsp[0].minor.yy170,0, 0);}
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 247: /* exprlist ::= expritem */
{yylhsminor.yy429 = tSqlExprListAppend(0,yymsp[0].minor.yy170,0, 0);}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 248: /* expritem ::= expr */
{yylhsminor.yy170 = yymsp[0].minor.yy170;}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 250: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 251: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy218, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
