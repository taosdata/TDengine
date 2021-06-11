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
#include "ttoken.h"
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
#define YYNOCODE 271
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy112;
  SCreateAcctInfo yy151;
  tSqlExpr* yy166;
  SCreateTableSql* yy182;
  SSqlNode* yy236;
  SRelationInfo* yy244;
  SSessionWindowVal yy259;
  SIntervalVal yy340;
  TAOS_FIELD yy343;
  SWindowStateVal yy348;
  int64_t yy369;
  SCreateDbInfo yy382;
  SLimitVal yy414;
  SArray* yy441;
  SCreatedTableInfo yy456;
  tVariant yy506;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             347
#define YYNRULE              284
#define YYNTOKEN             190
#define YY_MAX_SHIFT         346
#define YY_MIN_SHIFTREDUCE   548
#define YY_MAX_SHIFTREDUCE   831
#define YY_ERROR_ACTION      832
#define YY_ACCEPT_ACTION     833
#define YY_NO_ACTION         834
#define YY_MIN_REDUCE        835
#define YY_MAX_REDUCE        1118
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
#define YY_ACTTAB_COUNT (734)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    23,  597, 1007,  597,  219,  344,  194,  833,  346,  598,
 /*    10 */   597,  598,  197,   54,   55,  225,   58,   59,  598,  986,
 /*    20 */   239,   48, 1094,   57,  300,   62,   60,   63,   61,  998,
 /*    30 */   998,  231,  233,   53,   52,  986,  986,   51,   50,   49,
 /*    40 */    54,   55,   35,   58,   59,  222,  223,  239,   48,  597,
 /*    50 */    57,  300,   62,   60,   63,   61,  998,  598,  152,  236,
 /*    60 */    53,   52,  235,  152,   51,   50,   49,   55, 1004,   58,
 /*    70 */    59,  772,  261,  239,   48,  240,   57,  300,   62,   60,
 /*    80 */    63,   61,   29,  195,   83,  221,   53,   52,  145,  983,
 /*    90 */    51,   50,   49,  549,  550,  551,  552,  553,  554,  555,
 /*   100 */   556,  557,  558,  559,  560,  561,  345,  773,  770,  220,
 /*   110 */    95,   77,   54,   55,   35,   58,   59,   42,  197,  239,
 /*   120 */    48,  197,   57,  300,   62,   60,   63,   61, 1095,  298,
 /*   130 */  1043, 1095,   53,   52,  197,   89,   51,   50,   49,   54,
 /*   140 */    56,  330,   58,   59, 1095,  974,  239,   48,  629,   57,
 /*   150 */   300,   62,   60,   63,   61,  268,  267,  229,  152,   53,
 /*   160 */    52,  983,  248,   51,   50,   49,   41,  296,  339,  338,
 /*   170 */   295,  294,  293,  337,  292,  336,  335,  334,  291,  333,
 /*   180 */   332,  946,  934,  935,  936,  937,  938,  939,  940,  941,
 /*   190 */   942,  943,  944,  945,  947,  948,   58,   59,   24,  984,
 /*   200 */   239,   48,   35,   57,  300,   62,   60,   63,   61,   51,
 /*   210 */    50,   49,  972,   53,   52,  205,  881,   51,   50,   49,
 /*   220 */    76,  179,  206,   35,  340,  915,   92,  129,  128,  204,
 /*   230 */  1044,  237,  280,  305,   83,  200,  238,  785,   35,  776,
 /*   240 */   774,  779,  777,  980,  780,  230,  238,  785,  715,  983,
 /*   250 */   774,    6,  777,  971,  780,  114,  108,  119,  969,  970,
 /*   260 */    34,  973,  118,  124,  127,  117,  309,   42,  217,  218,
 /*   270 */   983,  121,  301,   41,    9,  339,  338,   35,  217,  218,
 /*   280 */   337,  310,  336,  335,  334,  983,  333,  332,  232,  116,
 /*   290 */   260,  262,   75,  954,  298,  952,  953,  330,  242,  213,
 /*   300 */   955,   36,  957,  958,  956,  718,  959,  960,   62,   60,
 /*   310 */    63,   61,  775,  152,  778,   64,   53,   52,   14,  247,
 /*   320 */    51,   50,   49,  703,  982,   64,  700, 1091,  701,   35,
 /*   330 */   702,    5,   38,  169,   35,  188,  186,  184,  168,  102,
 /*   340 */    97,  101,  183,  132,  131,  130,   35,  786,   94,   91,
 /*   350 */   679, 1090,  783,  782,  244,  245,   35,  786,   35,   35,
 /*   360 */    53,   52,  891,  782,   51,   50,   49,  179,   90,  243,
 /*   370 */   781,  241,  311,  308,  307, 1089,  983,  312,  320,  319,
 /*   380 */   781,  983,   78,  282,  722,   88,  882,  253,   71,  316,
 /*   390 */   249,  179,  246,  983,  315,  314,  257,  256,   80,  317,
 /*   400 */    68,  318,  322,  983,   81,  983,  983,  343,  342,  137,
 /*   410 */   143,  141,  140,    1,  167,    3,  180,  751,  752,  734,
 /*   420 */   742,  302,  743, 1054,  689,  784,   33,  215,   72,  147,
 /*   430 */    65,  264,   26,  704,   36,  285,  691,  264,  287,  690,
 /*   440 */   806,  787,   69,  596,   74,   36,   65,  216,   93,   65,
 /*   450 */    25,   25,   16,   25,   15,  288,  107,  198,  106,   18,
 /*   460 */   707,   17,  708,  199,  705,   20,  706,   19,  201,  113,
 /*   470 */   985,  112,  678,   22,  196,   21,  126,  125,  202,  203,
 /*   480 */   208,  209,  210,  207,  193, 1114, 1106, 1053,  227, 1050,
 /*   490 */  1049,  228,  321,  258,  144, 1006, 1017,   45, 1014, 1015,
 /*   500 */  1019,  999,  265, 1036,  146,  150,  981,  274, 1035,  163,
 /*   510 */   142,  269,  164,  157,  979,  733,  165,  224,  789,  263,
 /*   520 */   166,  153,  894,  283,  290,   43,  191,  271,   39,  299,
 /*   530 */   890,  306,   73,  278, 1113,  996,   70,   47,  104,  154,
 /*   540 */   155, 1112,  281, 1109,  170,  313, 1105,  110,  279,  156,
 /*   550 */  1104,  277,  158,  275,  273, 1101,  159,  171,  270,  912,
 /*   560 */    40,   37,   44,  192,  878,  120,  876,  122,  123,  874,
 /*   570 */   873,  250,  182,  871,  870,  869,  868,  867,  866,  185,
 /*   580 */   187,  863,  861,  859,  857,  189,  854,  190,   46,   79,
 /*   590 */    84,  272,  331, 1037,  115,  323,  324,  325,  326,  327,
 /*   600 */   328,  329,  214,  341,  234,  289,  831,  252,  251,  830,
 /*   610 */   211,  212,  254,   98,   99,  255,  829,  812,  811,  259,
 /*   620 */    10,  264,  872,  284,  133,  710,  174,  134,  173,  913,
 /*   630 */   172,  175,  177,  176,  135,  178,  865,  914,  864,    2,
 /*   640 */   136,  950,  856,  855,   82,   30,    4,  266,  160,  161,
 /*   650 */   162,  962,   85,  735,  148,  149,  738,   86,  226,  740,
 /*   660 */    87,  276,   31,  744,  151,   13,   11,   32,   12,   27,
 /*   670 */    28,  286,   96,   94,  642,  638,  636,  635,  634,  631,
 /*   680 */   601,  297,  100,    7,  303,  790,  788,    8,  304,  103,
 /*   690 */   105,   66,   67,  109,  111,  681,  680,  677,  623,   36,
 /*   700 */   621,  613,  619,  615,  617,  611,  609,  645,  644,  643,
 /*   710 */   641,  640,  639,  637,  633,  632,  181,  599,  565,  835,
 /*   720 */   563,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   730 */   834,  834,  138,  139,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   259,    1,  194,    1,  193,  194,  259,  191,  192,    9,
 /*    10 */     1,    9,  259,   13,   14,  238,   16,   17,    9,  242,
 /*    20 */    20,   21,  269,   23,   24,   25,   26,   27,   28,  240,
 /*    30 */   240,  238,  238,   33,   34,  242,  242,   37,   38,   39,
 /*    40 */    13,   14,  194,   16,   17,  256,  256,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  240,    9,  194,  200,
 /*    60 */    33,   34,  200,  194,   37,   38,   39,   14,  260,   16,
 /*    70 */    17,    1,  256,   20,   21,  200,   23,   24,   25,   26,
 /*    80 */    27,   28,   80,  259,   80,  237,   33,   34,  194,  241,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,   37,   81,   61,
 /*   110 */   201,  111,   13,   14,  194,   16,   17,  113,  259,   20,
 /*   120 */    21,  259,   23,   24,   25,   26,   27,   28,  269,   82,
 /*   130 */   266,  269,   33,   34,  259,  266,   37,   38,   39,   13,
 /*   140 */    14,   84,   16,   17,  269,  236,   20,   21,    5,   23,
 /*   150 */    24,   25,   26,   27,   28,  261,  262,  237,  194,   33,
 /*   160 */    34,  241,  194,   37,   38,   39,   92,   93,   94,   95,
 /*   170 */    96,   97,   98,   99,  100,  101,  102,  103,  104,  105,
 /*   180 */   106,  215,  216,  217,  218,  219,  220,  221,  222,  223,
 /*   190 */   224,  225,  226,  227,  228,  229,   16,   17,   44,  231,
 /*   200 */    20,   21,  194,   23,   24,   25,   26,   27,   28,   37,
 /*   210 */    38,   39,    0,   33,   34,   61,  199,   37,   38,   39,
 /*   220 */   201,  204,   68,  194,  213,  214,  201,   73,   74,   75,
 /*   230 */   266,   60,  268,   79,   80,  259,    1,    2,  194,    5,
 /*   240 */     5,    7,    7,  194,    9,  237,    1,    2,   91,  241,
 /*   250 */     5,   80,    7,  234,    9,   62,   63,   64,  233,  234,
 /*   260 */   235,  236,   69,   70,   71,   72,  237,  113,   33,   34,
 /*   270 */   241,   78,   37,   92,  117,   94,   95,  194,   33,   34,
 /*   280 */    99,  237,  101,  102,  103,  241,  105,  106,  239,   76,
 /*   290 */   136,   81,  138,  215,   82,  217,  218,   84,   68,  145,
 /*   300 */   222,   91,  224,  225,  226,   37,  228,  229,   25,   26,
 /*   310 */    27,   28,    5,  194,    7,   80,   33,   34,   80,   68,
 /*   320 */    37,   38,   39,    2,  241,   80,    5,  259,    7,  194,
 /*   330 */     9,   62,   63,   64,  194,   62,   63,   64,   69,   70,
 /*   340 */    71,   72,   69,   70,   71,   72,  194,  112,  110,  111,
 /*   350 */     5,  259,  118,  118,   33,   34,  194,  112,  194,  194,
 /*   360 */    33,   34,  199,  118,   37,   38,   39,  204,  243,  139,
 /*   370 */   135,  141,  237,  143,  144,  259,  241,  237,   33,   34,
 /*   380 */   135,  241,  257,  264,  116,  266,  199,  137,   91,  237,
 /*   390 */   139,  204,  141,  241,  143,  144,  146,  147,   81,  237,
 /*   400 */    91,  237,  237,  241,   81,  241,  241,   65,   66,   67,
 /*   410 */    62,   63,   64,  202,  203,  197,  198,  126,  127,   81,
 /*   420 */    81,   15,   81,  232,   81,  118,   80,  259,  131,   91,
 /*   430 */    91,  114,   91,  112,   91,   81,   81,  114,   81,   81,
 /*   440 */    81,   81,  133,   81,   80,   91,   91,  259,   91,   91,
 /*   450 */    91,   91,  140,   91,  142,  109,  140,  259,  142,  140,
 /*   460 */     5,  142,    7,  259,    5,  140,    7,  142,  259,  140,
 /*   470 */   242,  142,  108,  140,  259,  142,   76,   77,  259,  259,
 /*   480 */   259,  259,  259,  259,  259,  242,  242,  232,  232,  232,
 /*   490 */   232,  232,  232,  194,  194,  194,  194,  258,  194,  194,
 /*   500 */   194,  240,  240,  267,  194,  194,  240,  194,  267,  244,
 /*   510 */    60,  263,  194,  250,  194,  118,  194,  263,  112,  195,
 /*   520 */   194,  254,  194,  124,  194,  194,  194,  263,  194,  194,
 /*   530 */   194,  194,  130,  263,  194,  255,  132,  129,  194,  253,
 /*   540 */   252,  194,  128,  194,  194,  194,  194,  194,  123,  251,
 /*   550 */   194,  122,  249,  121,  120,  194,  248,  194,  119,  194,
 /*   560 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   570 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   580 */   194,  194,  194,  194,  194,  194,  194,  194,  134,  195,
 /*   590 */   195,  195,  107,  195,   90,   89,   50,   86,   88,   54,
 /*   600 */    87,   85,  195,   82,  195,  195,    5,    5,  148,    5,
 /*   610 */   195,  195,  148,  201,  201,    5,    5,   94,   93,  137,
 /*   620 */    80,  114,  195,  109,  196,   81,  206,  196,  210,  212,
 /*   630 */   211,  209,  208,  207,  196,  205,  195,  214,  195,  202,
 /*   640 */   196,  230,  195,  195,  115,   80,  197,   91,  247,  246,
 /*   650 */   245,  230,   91,   81,   80,   91,   81,   80,    1,   81,
 /*   660 */    80,   80,   91,   81,   80,   80,  125,   91,  125,   80,
 /*   670 */    80,  109,   76,  110,    9,    5,    5,    5,    5,    5,
 /*   680 */    83,   15,   76,   80,   24,  112,   81,   80,   58,  142,
 /*   690 */   142,   16,   16,  142,  142,    5,    5,   81,    5,   91,
 /*   700 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   710 */     5,    5,    5,    5,    5,    5,   91,   83,   60,    0,
 /*   720 */    59,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   730 */   270,  270,   21,   21,  270,  270,  270,  270,  270,  270,
 /*   740 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   750 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   760 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   770 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   780 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   790 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   800 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   810 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   820 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   830 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   840 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   850 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   860 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   870 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   880 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   890 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   900 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   910 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   920 */   270,  270,  270,  270,
};
#define YY_SHIFT_COUNT    (346)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (719)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   154,   74,   74,  181,  181,   47,  235,  245,  245,    2,
 /*    10 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*    20 */     9,    9,    9,    0,   48,  245,  321,  321,  321,    4,
 /*    30 */     4,    9,    9,    9,  212,    9,    9,  213,   47,   57,
 /*    40 */    57,  143,  734,  734,  734,  245,  245,  245,  245,  245,
 /*    50 */   245,  245,  245,  245,  245,  245,  245,  245,  245,  245,
 /*    60 */   245,  245,  245,  245,  245,  321,  321,  321,  345,  345,
 /*    70 */   345,  345,  345,  345,  345,    9,    9,    9,  268,    9,
 /*    80 */     9,    9,    4,    4,    9,    9,    9,    9,  291,  291,
 /*    90 */   157,    4,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   100 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   110 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   120 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   130 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   140 */     9,    9,    9,    9,  450,  450,  450,  397,  397,  397,
 /*   150 */   450,  397,  450,  402,  404,  408,  399,  414,  425,  429,
 /*   160 */   432,  434,  439,  454,  450,  450,  450,  485,   47,   47,
 /*   170 */   450,  450,  504,  506,  546,  511,  510,  545,  513,  516,
 /*   180 */   485,  143,  450,  521,  521,  450,  521,  450,  521,  450,
 /*   190 */   450,  734,  734,   27,   99,   99,  126,   99,   53,  180,
 /*   200 */   283,  283,  283,  283,  193,  269,  273,  327,  327,  327,
 /*   210 */   327,  230,  251,  250,  238,  172,  172,  234,  307,  342,
 /*   220 */   348,  210,  317,  323,  338,  339,  341,  309,  297,  343,
 /*   230 */   354,  355,  357,  358,  346,  359,  360,   70,  171,  406,
 /*   240 */   362,  312,  316,  319,  455,  459,  325,  329,  364,  333,
 /*   250 */   400,  601,  460,  602,  604,  464,  610,  611,  523,  525,
 /*   260 */   482,  507,  514,  540,  529,  544,  565,  556,  561,  572,
 /*   270 */   574,  575,  564,  577,  578,  580,  657,  581,  582,  584,
 /*   280 */   571,  541,  576,  543,  585,  514,  589,  562,  590,  563,
 /*   290 */   596,  665,  670,  671,  672,  673,  674,  597,  666,  606,
 /*   300 */   603,  605,  573,  607,  660,  630,  675,  547,  548,  608,
 /*   310 */   608,  608,  608,  676,  551,  552,  608,  608,  608,  690,
 /*   320 */   691,  616,  608,  693,  695,  696,  697,  698,  699,  700,
 /*   330 */   701,  702,  703,  704,  705,  706,  707,  708,  709,  710,
 /*   340 */   625,  634,  711,  712,  658,  661,  719,
};
#define YY_REDUCE_COUNT (192)
#define YY_REDUCE_MIN   (-259)
#define YY_REDUCE_MAX   (449)
static const short yy_reduce_ofst[] = {
 /*     0 */  -184,  -34,  -34,   78,   78,   25, -141, -138, -125, -106,
 /*    10 */  -152,  -36,  119,  -80,    8,   29,   44,  135,  140,  152,
 /*    20 */   162,  164,  165, -192, -189, -247, -223, -207, -206, -211,
 /*    30 */  -210, -136, -131,   49,  -91,  -32,   83,   17,   19,  163,
 /*    40 */   187,   11,  125,  211,  218, -259, -253, -176,  -24,   68,
 /*    50 */    92,  116,  168,  188,  198,  204,  209,  215,  219,  220,
 /*    60 */   221,  222,  223,  224,  225,  228,  243,  244,  191,  255,
 /*    70 */   256,  257,  258,  259,  260,  299,  300,  301,  239,  302,
 /*    80 */   304,  305,  261,  262,  306,  310,  311,  313,  236,  241,
 /*    90 */   265,  266,  318,  320,  322,  326,  328,  330,  331,  332,
 /*   100 */   334,  335,  336,  337,  340,  344,  347,  349,  350,  351,
 /*   110 */   352,  353,  356,  361,  363,  365,  366,  367,  368,  369,
 /*   120 */   370,  371,  372,  373,  374,  375,  376,  377,  378,  379,
 /*   130 */   380,  381,  382,  383,  384,  385,  386,  387,  388,  389,
 /*   140 */   390,  391,  392,  393,  324,  394,  395,  248,  254,  264,
 /*   150 */   396,  270,  398,  280,  267,  286,  288,  298,  263,  303,
 /*   160 */   308,  401,  403,  405,  407,  409,  410,  411,  412,  413,
 /*   170 */   415,  416,  417,  419,  418,  420,  422,  426,  424,  430,
 /*   180 */   421,  423,  427,  428,  431,  441,  438,  443,  444,  447,
 /*   190 */   448,  437,  449,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   832,  949,  892,  961,  879,  889, 1097, 1097, 1097,  832,
 /*    10 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*    20 */   832,  832,  832, 1008,  851, 1097,  832,  832,  832,  832,
 /*    30 */   832,  832,  832,  832,  889,  832,  832,  895,  889,  895,
 /*    40 */   895,  832, 1003,  933,  951,  832,  832,  832,  832,  832,
 /*    50 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*    60 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*    70 */   832,  832,  832,  832,  832,  832,  832,  832, 1010, 1016,
 /*    80 */  1013,  832,  832,  832, 1018,  832,  832,  832, 1040, 1040,
 /*    90 */  1001,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   100 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   110 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   120 */   877,  832,  875,  832,  832,  832,  832,  832,  832,  832,
 /*   130 */   832,  832,  832,  832,  832,  832,  832,  862,  832,  832,
 /*   140 */   832,  832,  832,  832,  853,  853,  853,  832,  832,  832,
 /*   150 */   853,  832,  853, 1047, 1051, 1045, 1033, 1041, 1032, 1028,
 /*   160 */  1026, 1024, 1023, 1055,  853,  853,  853,  893,  889,  889,
 /*   170 */   853,  853,  911,  909,  907,  899,  905,  901,  903,  897,
 /*   180 */   880,  832,  853,  887,  887,  853,  887,  853,  887,  853,
 /*   190 */   853,  933,  951,  832, 1056, 1046,  832, 1096, 1086, 1085,
 /*   200 */  1092, 1084, 1083, 1082,  832,  832,  832, 1078, 1081, 1080,
 /*   210 */  1079,  832,  832,  832,  832, 1088, 1087,  832,  832,  832,
 /*   220 */   832,  832,  832,  832,  832,  832,  832, 1052, 1048,  832,
 /*   230 */   832,  832,  832,  832,  832,  832,  832,  832, 1058,  832,
 /*   240 */   832,  832,  832,  832,  832,  832,  832,  832,  963,  832,
 /*   250 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   260 */   832, 1000,  832,  832,  832,  832,  832, 1012, 1011,  832,
 /*   270 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   280 */  1042,  832, 1034,  832,  832,  975,  832,  832,  832,  832,
 /*   290 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   300 */   832,  832,  832,  832,  832,  832,  832,  832,  832, 1115,
 /*   310 */  1110, 1111, 1108,  832,  832,  832, 1107, 1102, 1103,  832,
 /*   320 */   832,  832, 1100,  832,  832,  832,  832,  832,  832,  832,
 /*   330 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   340 */   917,  832,  860,  858,  832,  849,  832,
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
    1,  /*     STABLE => ID */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
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
    0,  /*    COMPACT => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*      COMMA => nothing */
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
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
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
    0,  /* STATE_WINDOW => nothing */
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
    0,  /*     SYNCDB => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*     MODIFY => nothing */
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
  /*   63 */ "STABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
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
  /*   79 */ "COMPACT",
  /*   80 */ "LP",
  /*   81 */ "RP",
  /*   82 */ "IF",
  /*   83 */ "EXISTS",
  /*   84 */ "PPS",
  /*   85 */ "TSERIES",
  /*   86 */ "DBS",
  /*   87 */ "STORAGE",
  /*   88 */ "QTIME",
  /*   89 */ "CONNS",
  /*   90 */ "STATE",
  /*   91 */ "COMMA",
  /*   92 */ "KEEP",
  /*   93 */ "CACHE",
  /*   94 */ "REPLICA",
  /*   95 */ "QUORUM",
  /*   96 */ "DAYS",
  /*   97 */ "MINROWS",
  /*   98 */ "MAXROWS",
  /*   99 */ "BLOCKS",
  /*  100 */ "CTIME",
  /*  101 */ "WAL",
  /*  102 */ "FSYNC",
  /*  103 */ "COMP",
  /*  104 */ "PRECISION",
  /*  105 */ "UPDATE",
  /*  106 */ "CACHELAST",
  /*  107 */ "PARTITIONS",
  /*  108 */ "UNSIGNED",
  /*  109 */ "TAGS",
  /*  110 */ "USING",
  /*  111 */ "AS",
  /*  112 */ "NULL",
  /*  113 */ "SELECT",
  /*  114 */ "UNION",
  /*  115 */ "ALL",
  /*  116 */ "DISTINCT",
  /*  117 */ "FROM",
  /*  118 */ "VARIABLE",
  /*  119 */ "INTERVAL",
  /*  120 */ "SESSION",
  /*  121 */ "STATE_WINDOW",
  /*  122 */ "FILL",
  /*  123 */ "SLIDING",
  /*  124 */ "ORDER",
  /*  125 */ "BY",
  /*  126 */ "ASC",
  /*  127 */ "DESC",
  /*  128 */ "GROUP",
  /*  129 */ "HAVING",
  /*  130 */ "LIMIT",
  /*  131 */ "OFFSET",
  /*  132 */ "SLIMIT",
  /*  133 */ "SOFFSET",
  /*  134 */ "WHERE",
  /*  135 */ "NOW",
  /*  136 */ "RESET",
  /*  137 */ "QUERY",
  /*  138 */ "SYNCDB",
  /*  139 */ "ADD",
  /*  140 */ "COLUMN",
  /*  141 */ "MODIFY",
  /*  142 */ "TAG",
  /*  143 */ "CHANGE",
  /*  144 */ "SET",
  /*  145 */ "KILL",
  /*  146 */ "CONNECTION",
  /*  147 */ "STREAM",
  /*  148 */ "COLON",
  /*  149 */ "ABORT",
  /*  150 */ "AFTER",
  /*  151 */ "ATTACH",
  /*  152 */ "BEFORE",
  /*  153 */ "BEGIN",
  /*  154 */ "CASCADE",
  /*  155 */ "CLUSTER",
  /*  156 */ "CONFLICT",
  /*  157 */ "COPY",
  /*  158 */ "DEFERRED",
  /*  159 */ "DELIMITERS",
  /*  160 */ "DETACH",
  /*  161 */ "EACH",
  /*  162 */ "END",
  /*  163 */ "EXPLAIN",
  /*  164 */ "FAIL",
  /*  165 */ "FOR",
  /*  166 */ "IGNORE",
  /*  167 */ "IMMEDIATE",
  /*  168 */ "INITIALLY",
  /*  169 */ "INSTEAD",
  /*  170 */ "MATCH",
  /*  171 */ "KEY",
  /*  172 */ "OF",
  /*  173 */ "RAISE",
  /*  174 */ "REPLACE",
  /*  175 */ "RESTRICT",
  /*  176 */ "ROW",
  /*  177 */ "STATEMENT",
  /*  178 */ "TRIGGER",
  /*  179 */ "VIEW",
  /*  180 */ "SEMI",
  /*  181 */ "NONE",
  /*  182 */ "PREV",
  /*  183 */ "LINEAR",
  /*  184 */ "IMPORT",
  /*  185 */ "TBNAME",
  /*  186 */ "JOIN",
  /*  187 */ "INSERT",
  /*  188 */ "INTO",
  /*  189 */ "VALUES",
  /*  190 */ "error",
  /*  191 */ "program",
  /*  192 */ "cmd",
  /*  193 */ "dbPrefix",
  /*  194 */ "ids",
  /*  195 */ "cpxName",
  /*  196 */ "ifexists",
  /*  197 */ "alter_db_optr",
  /*  198 */ "alter_topic_optr",
  /*  199 */ "acct_optr",
  /*  200 */ "exprlist",
  /*  201 */ "ifnotexists",
  /*  202 */ "db_optr",
  /*  203 */ "topic_optr",
  /*  204 */ "pps",
  /*  205 */ "tseries",
  /*  206 */ "dbs",
  /*  207 */ "streams",
  /*  208 */ "storage",
  /*  209 */ "qtime",
  /*  210 */ "users",
  /*  211 */ "conns",
  /*  212 */ "state",
  /*  213 */ "intitemlist",
  /*  214 */ "intitem",
  /*  215 */ "keep",
  /*  216 */ "cache",
  /*  217 */ "replica",
  /*  218 */ "quorum",
  /*  219 */ "days",
  /*  220 */ "minrows",
  /*  221 */ "maxrows",
  /*  222 */ "blocks",
  /*  223 */ "ctime",
  /*  224 */ "wal",
  /*  225 */ "fsync",
  /*  226 */ "comp",
  /*  227 */ "prec",
  /*  228 */ "update",
  /*  229 */ "cachelast",
  /*  230 */ "partitions",
  /*  231 */ "typename",
  /*  232 */ "signed",
  /*  233 */ "create_table_args",
  /*  234 */ "create_stable_args",
  /*  235 */ "create_table_list",
  /*  236 */ "create_from_stable",
  /*  237 */ "columnlist",
  /*  238 */ "tagitemlist",
  /*  239 */ "tagNamelist",
  /*  240 */ "select",
  /*  241 */ "column",
  /*  242 */ "tagitem",
  /*  243 */ "selcollist",
  /*  244 */ "from",
  /*  245 */ "where_opt",
  /*  246 */ "interval_opt",
  /*  247 */ "session_option",
  /*  248 */ "windowstate_option",
  /*  249 */ "fill_opt",
  /*  250 */ "sliding_opt",
  /*  251 */ "groupby_opt",
  /*  252 */ "orderby_opt",
  /*  253 */ "having_opt",
  /*  254 */ "slimit_opt",
  /*  255 */ "limit_opt",
  /*  256 */ "union",
  /*  257 */ "sclp",
  /*  258 */ "distinct",
  /*  259 */ "expr",
  /*  260 */ "as",
  /*  261 */ "tablelist",
  /*  262 */ "sub",
  /*  263 */ "tmvar",
  /*  264 */ "sortlist",
  /*  265 */ "sortitem",
  /*  266 */ "item",
  /*  267 */ "sortorder",
  /*  268 */ "grouplist",
  /*  269 */ "expritem",
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
 /*  21 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
 /*  22 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  29 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  32 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  33 */ "cmd ::= DROP DNODE ids",
 /*  34 */ "cmd ::= DROP USER ids",
 /*  35 */ "cmd ::= DROP ACCOUNT ids",
 /*  36 */ "cmd ::= USE ids",
 /*  37 */ "cmd ::= DESCRIBE ids cpxName",
 /*  38 */ "cmd ::= ALTER USER ids PASS ids",
 /*  39 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids ids",
 /*  44 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  45 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  49 */ "ids ::= ID",
 /*  50 */ "ids ::= STRING",
 /*  51 */ "ifexists ::= IF EXISTS",
 /*  52 */ "ifexists ::=",
 /*  53 */ "ifnotexists ::= IF NOT EXISTS",
 /*  54 */ "ifnotexists ::=",
 /*  55 */ "cmd ::= CREATE DNODE ids",
 /*  56 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  57 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  58 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  59 */ "cmd ::= CREATE USER ids PASS ids",
 /*  60 */ "pps ::=",
 /*  61 */ "pps ::= PPS INTEGER",
 /*  62 */ "tseries ::=",
 /*  63 */ "tseries ::= TSERIES INTEGER",
 /*  64 */ "dbs ::=",
 /*  65 */ "dbs ::= DBS INTEGER",
 /*  66 */ "streams ::=",
 /*  67 */ "streams ::= STREAMS INTEGER",
 /*  68 */ "storage ::=",
 /*  69 */ "storage ::= STORAGE INTEGER",
 /*  70 */ "qtime ::=",
 /*  71 */ "qtime ::= QTIME INTEGER",
 /*  72 */ "users ::=",
 /*  73 */ "users ::= USERS INTEGER",
 /*  74 */ "conns ::=",
 /*  75 */ "conns ::= CONNS INTEGER",
 /*  76 */ "state ::=",
 /*  77 */ "state ::= STATE ids",
 /*  78 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  79 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  80 */ "intitemlist ::= intitem",
 /*  81 */ "intitem ::= INTEGER",
 /*  82 */ "keep ::= KEEP intitemlist",
 /*  83 */ "cache ::= CACHE INTEGER",
 /*  84 */ "replica ::= REPLICA INTEGER",
 /*  85 */ "quorum ::= QUORUM INTEGER",
 /*  86 */ "days ::= DAYS INTEGER",
 /*  87 */ "minrows ::= MINROWS INTEGER",
 /*  88 */ "maxrows ::= MAXROWS INTEGER",
 /*  89 */ "blocks ::= BLOCKS INTEGER",
 /*  90 */ "ctime ::= CTIME INTEGER",
 /*  91 */ "wal ::= WAL INTEGER",
 /*  92 */ "fsync ::= FSYNC INTEGER",
 /*  93 */ "comp ::= COMP INTEGER",
 /*  94 */ "prec ::= PRECISION STRING",
 /*  95 */ "update ::= UPDATE INTEGER",
 /*  96 */ "cachelast ::= CACHELAST INTEGER",
 /*  97 */ "partitions ::= PARTITIONS INTEGER",
 /*  98 */ "db_optr ::=",
 /*  99 */ "db_optr ::= db_optr cache",
 /* 100 */ "db_optr ::= db_optr replica",
 /* 101 */ "db_optr ::= db_optr quorum",
 /* 102 */ "db_optr ::= db_optr days",
 /* 103 */ "db_optr ::= db_optr minrows",
 /* 104 */ "db_optr ::= db_optr maxrows",
 /* 105 */ "db_optr ::= db_optr blocks",
 /* 106 */ "db_optr ::= db_optr ctime",
 /* 107 */ "db_optr ::= db_optr wal",
 /* 108 */ "db_optr ::= db_optr fsync",
 /* 109 */ "db_optr ::= db_optr comp",
 /* 110 */ "db_optr ::= db_optr prec",
 /* 111 */ "db_optr ::= db_optr keep",
 /* 112 */ "db_optr ::= db_optr update",
 /* 113 */ "db_optr ::= db_optr cachelast",
 /* 114 */ "topic_optr ::= db_optr",
 /* 115 */ "topic_optr ::= topic_optr partitions",
 /* 116 */ "alter_db_optr ::=",
 /* 117 */ "alter_db_optr ::= alter_db_optr replica",
 /* 118 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 119 */ "alter_db_optr ::= alter_db_optr keep",
 /* 120 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 121 */ "alter_db_optr ::= alter_db_optr comp",
 /* 122 */ "alter_db_optr ::= alter_db_optr wal",
 /* 123 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 124 */ "alter_db_optr ::= alter_db_optr update",
 /* 125 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 126 */ "alter_topic_optr ::= alter_db_optr",
 /* 127 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 128 */ "typename ::= ids",
 /* 129 */ "typename ::= ids LP signed RP",
 /* 130 */ "typename ::= ids UNSIGNED",
 /* 131 */ "signed ::= INTEGER",
 /* 132 */ "signed ::= PLUS INTEGER",
 /* 133 */ "signed ::= MINUS INTEGER",
 /* 134 */ "cmd ::= CREATE TABLE create_table_args",
 /* 135 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 137 */ "cmd ::= CREATE TABLE create_table_list",
 /* 138 */ "create_table_list ::= create_from_stable",
 /* 139 */ "create_table_list ::= create_table_list create_from_stable",
 /* 140 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 141 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 143 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 144 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 145 */ "tagNamelist ::= ids",
 /* 146 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 147 */ "columnlist ::= columnlist COMMA column",
 /* 148 */ "columnlist ::= column",
 /* 149 */ "column ::= ids typename",
 /* 150 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 151 */ "tagitemlist ::= tagitem",
 /* 152 */ "tagitem ::= INTEGER",
 /* 153 */ "tagitem ::= FLOAT",
 /* 154 */ "tagitem ::= STRING",
 /* 155 */ "tagitem ::= BOOL",
 /* 156 */ "tagitem ::= NULL",
 /* 157 */ "tagitem ::= MINUS INTEGER",
 /* 158 */ "tagitem ::= MINUS FLOAT",
 /* 159 */ "tagitem ::= PLUS INTEGER",
 /* 160 */ "tagitem ::= PLUS FLOAT",
 /* 161 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 162 */ "select ::= LP select RP",
 /* 163 */ "union ::= select",
 /* 164 */ "union ::= union UNION ALL select",
 /* 165 */ "cmd ::= union",
 /* 166 */ "select ::= SELECT selcollist",
 /* 167 */ "sclp ::= selcollist COMMA",
 /* 168 */ "sclp ::=",
 /* 169 */ "selcollist ::= sclp distinct expr as",
 /* 170 */ "selcollist ::= sclp STAR",
 /* 171 */ "as ::= AS ids",
 /* 172 */ "as ::= ids",
 /* 173 */ "as ::=",
 /* 174 */ "distinct ::= DISTINCT",
 /* 175 */ "distinct ::=",
 /* 176 */ "from ::= FROM tablelist",
 /* 177 */ "from ::= FROM sub",
 /* 178 */ "sub ::= LP union RP",
 /* 179 */ "sub ::= LP union RP ids",
 /* 180 */ "sub ::= sub COMMA LP union RP ids",
 /* 181 */ "tablelist ::= ids cpxName",
 /* 182 */ "tablelist ::= ids cpxName ids",
 /* 183 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 184 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 185 */ "tmvar ::= VARIABLE",
 /* 186 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 187 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 188 */ "interval_opt ::=",
 /* 189 */ "session_option ::=",
 /* 190 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 191 */ "windowstate_option ::=",
 /* 192 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 193 */ "fill_opt ::=",
 /* 194 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 195 */ "fill_opt ::= FILL LP ID RP",
 /* 196 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 197 */ "sliding_opt ::=",
 /* 198 */ "orderby_opt ::=",
 /* 199 */ "orderby_opt ::= ORDER BY sortlist",
 /* 200 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 201 */ "sortlist ::= item sortorder",
 /* 202 */ "item ::= ids cpxName",
 /* 203 */ "sortorder ::= ASC",
 /* 204 */ "sortorder ::= DESC",
 /* 205 */ "sortorder ::=",
 /* 206 */ "groupby_opt ::=",
 /* 207 */ "groupby_opt ::= GROUP BY grouplist",
 /* 208 */ "grouplist ::= grouplist COMMA item",
 /* 209 */ "grouplist ::= item",
 /* 210 */ "having_opt ::=",
 /* 211 */ "having_opt ::= HAVING expr",
 /* 212 */ "limit_opt ::=",
 /* 213 */ "limit_opt ::= LIMIT signed",
 /* 214 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 215 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 216 */ "slimit_opt ::=",
 /* 217 */ "slimit_opt ::= SLIMIT signed",
 /* 218 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 219 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 220 */ "where_opt ::=",
 /* 221 */ "where_opt ::= WHERE expr",
 /* 222 */ "expr ::= LP expr RP",
 /* 223 */ "expr ::= ID",
 /* 224 */ "expr ::= ID DOT ID",
 /* 225 */ "expr ::= ID DOT STAR",
 /* 226 */ "expr ::= INTEGER",
 /* 227 */ "expr ::= MINUS INTEGER",
 /* 228 */ "expr ::= PLUS INTEGER",
 /* 229 */ "expr ::= FLOAT",
 /* 230 */ "expr ::= MINUS FLOAT",
 /* 231 */ "expr ::= PLUS FLOAT",
 /* 232 */ "expr ::= STRING",
 /* 233 */ "expr ::= NOW",
 /* 234 */ "expr ::= VARIABLE",
 /* 235 */ "expr ::= PLUS VARIABLE",
 /* 236 */ "expr ::= MINUS VARIABLE",
 /* 237 */ "expr ::= BOOL",
 /* 238 */ "expr ::= NULL",
 /* 239 */ "expr ::= ID LP exprlist RP",
 /* 240 */ "expr ::= ID LP STAR RP",
 /* 241 */ "expr ::= expr IS NULL",
 /* 242 */ "expr ::= expr IS NOT NULL",
 /* 243 */ "expr ::= expr LT expr",
 /* 244 */ "expr ::= expr GT expr",
 /* 245 */ "expr ::= expr LE expr",
 /* 246 */ "expr ::= expr GE expr",
 /* 247 */ "expr ::= expr NE expr",
 /* 248 */ "expr ::= expr EQ expr",
 /* 249 */ "expr ::= expr BETWEEN expr AND expr",
 /* 250 */ "expr ::= expr AND expr",
 /* 251 */ "expr ::= expr OR expr",
 /* 252 */ "expr ::= expr PLUS expr",
 /* 253 */ "expr ::= expr MINUS expr",
 /* 254 */ "expr ::= expr STAR expr",
 /* 255 */ "expr ::= expr SLASH expr",
 /* 256 */ "expr ::= expr REM expr",
 /* 257 */ "expr ::= expr LIKE expr",
 /* 258 */ "expr ::= expr IN LP exprlist RP",
 /* 259 */ "exprlist ::= exprlist COMMA expritem",
 /* 260 */ "exprlist ::= expritem",
 /* 261 */ "expritem ::= expr",
 /* 262 */ "expritem ::=",
 /* 263 */ "cmd ::= RESET QUERY CACHE",
 /* 264 */ "cmd ::= SYNCDB ids REPLICA",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 281 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 282 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 283 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 200: /* exprlist */
    case 243: /* selcollist */
    case 257: /* sclp */
{
tSqlExprListDestroy((yypminor->yy441));
}
      break;
    case 213: /* intitemlist */
    case 215: /* keep */
    case 237: /* columnlist */
    case 238: /* tagitemlist */
    case 239: /* tagNamelist */
    case 249: /* fill_opt */
    case 251: /* groupby_opt */
    case 252: /* orderby_opt */
    case 264: /* sortlist */
    case 268: /* grouplist */
{
taosArrayDestroy((yypminor->yy441));
}
      break;
    case 235: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy182));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy236));
}
      break;
    case 244: /* from */
    case 261: /* tablelist */
    case 262: /* sub */
{
destroyRelationInfo((yypminor->yy244));
}
      break;
    case 245: /* where_opt */
    case 253: /* having_opt */
    case 259: /* expr */
    case 269: /* expritem */
{
tSqlExprDestroy((yypminor->yy166));
}
      break;
    case 256: /* union */
{
destroyAllSqlNode((yypminor->yy441));
}
      break;
    case 265: /* sortitem */
{
tVariantDestroy(&(yypminor->yy506));
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
  {  191,   -1 }, /* (0) program ::= cmd */
  {  192,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  192,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  192,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  192,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  192,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  192,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  192,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  192,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  192,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  192,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  192,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  192,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  192,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  192,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  192,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  193,    0 }, /* (16) dbPrefix ::= */
  {  193,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  195,    0 }, /* (18) cpxName ::= */
  {  195,   -2 }, /* (19) cpxName ::= DOT ids */
  {  192,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  192,   -5 }, /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  192,   -4 }, /* (22) cmd ::= SHOW CREATE DATABASE ids */
  {  192,   -3 }, /* (23) cmd ::= SHOW dbPrefix TABLES */
  {  192,   -5 }, /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  192,   -3 }, /* (25) cmd ::= SHOW dbPrefix STABLES */
  {  192,   -5 }, /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  192,   -3 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS */
  {  192,   -4 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  192,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  192,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  192,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  192,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  192,   -3 }, /* (33) cmd ::= DROP DNODE ids */
  {  192,   -3 }, /* (34) cmd ::= DROP USER ids */
  {  192,   -3 }, /* (35) cmd ::= DROP ACCOUNT ids */
  {  192,   -2 }, /* (36) cmd ::= USE ids */
  {  192,   -3 }, /* (37) cmd ::= DESCRIBE ids cpxName */
  {  192,   -5 }, /* (38) cmd ::= ALTER USER ids PASS ids */
  {  192,   -5 }, /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  192,   -4 }, /* (40) cmd ::= ALTER DNODE ids ids */
  {  192,   -5 }, /* (41) cmd ::= ALTER DNODE ids ids ids */
  {  192,   -3 }, /* (42) cmd ::= ALTER LOCAL ids */
  {  192,   -4 }, /* (43) cmd ::= ALTER LOCAL ids ids */
  {  192,   -4 }, /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  192,   -4 }, /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  192,   -4 }, /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  192,   -6 }, /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  192,   -6 }, /* (48) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  194,   -1 }, /* (49) ids ::= ID */
  {  194,   -1 }, /* (50) ids ::= STRING */
  {  196,   -2 }, /* (51) ifexists ::= IF EXISTS */
  {  196,    0 }, /* (52) ifexists ::= */
  {  201,   -3 }, /* (53) ifnotexists ::= IF NOT EXISTS */
  {  201,    0 }, /* (54) ifnotexists ::= */
  {  192,   -3 }, /* (55) cmd ::= CREATE DNODE ids */
  {  192,   -6 }, /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  192,   -5 }, /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  192,   -5 }, /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  192,   -5 }, /* (59) cmd ::= CREATE USER ids PASS ids */
  {  204,    0 }, /* (60) pps ::= */
  {  204,   -2 }, /* (61) pps ::= PPS INTEGER */
  {  205,    0 }, /* (62) tseries ::= */
  {  205,   -2 }, /* (63) tseries ::= TSERIES INTEGER */
  {  206,    0 }, /* (64) dbs ::= */
  {  206,   -2 }, /* (65) dbs ::= DBS INTEGER */
  {  207,    0 }, /* (66) streams ::= */
  {  207,   -2 }, /* (67) streams ::= STREAMS INTEGER */
  {  208,    0 }, /* (68) storage ::= */
  {  208,   -2 }, /* (69) storage ::= STORAGE INTEGER */
  {  209,    0 }, /* (70) qtime ::= */
  {  209,   -2 }, /* (71) qtime ::= QTIME INTEGER */
  {  210,    0 }, /* (72) users ::= */
  {  210,   -2 }, /* (73) users ::= USERS INTEGER */
  {  211,    0 }, /* (74) conns ::= */
  {  211,   -2 }, /* (75) conns ::= CONNS INTEGER */
  {  212,    0 }, /* (76) state ::= */
  {  212,   -2 }, /* (77) state ::= STATE ids */
  {  199,   -9 }, /* (78) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  213,   -3 }, /* (79) intitemlist ::= intitemlist COMMA intitem */
  {  213,   -1 }, /* (80) intitemlist ::= intitem */
  {  214,   -1 }, /* (81) intitem ::= INTEGER */
  {  215,   -2 }, /* (82) keep ::= KEEP intitemlist */
  {  216,   -2 }, /* (83) cache ::= CACHE INTEGER */
  {  217,   -2 }, /* (84) replica ::= REPLICA INTEGER */
  {  218,   -2 }, /* (85) quorum ::= QUORUM INTEGER */
  {  219,   -2 }, /* (86) days ::= DAYS INTEGER */
  {  220,   -2 }, /* (87) minrows ::= MINROWS INTEGER */
  {  221,   -2 }, /* (88) maxrows ::= MAXROWS INTEGER */
  {  222,   -2 }, /* (89) blocks ::= BLOCKS INTEGER */
  {  223,   -2 }, /* (90) ctime ::= CTIME INTEGER */
  {  224,   -2 }, /* (91) wal ::= WAL INTEGER */
  {  225,   -2 }, /* (92) fsync ::= FSYNC INTEGER */
  {  226,   -2 }, /* (93) comp ::= COMP INTEGER */
  {  227,   -2 }, /* (94) prec ::= PRECISION STRING */
  {  228,   -2 }, /* (95) update ::= UPDATE INTEGER */
  {  229,   -2 }, /* (96) cachelast ::= CACHELAST INTEGER */
  {  230,   -2 }, /* (97) partitions ::= PARTITIONS INTEGER */
  {  202,    0 }, /* (98) db_optr ::= */
  {  202,   -2 }, /* (99) db_optr ::= db_optr cache */
  {  202,   -2 }, /* (100) db_optr ::= db_optr replica */
  {  202,   -2 }, /* (101) db_optr ::= db_optr quorum */
  {  202,   -2 }, /* (102) db_optr ::= db_optr days */
  {  202,   -2 }, /* (103) db_optr ::= db_optr minrows */
  {  202,   -2 }, /* (104) db_optr ::= db_optr maxrows */
  {  202,   -2 }, /* (105) db_optr ::= db_optr blocks */
  {  202,   -2 }, /* (106) db_optr ::= db_optr ctime */
  {  202,   -2 }, /* (107) db_optr ::= db_optr wal */
  {  202,   -2 }, /* (108) db_optr ::= db_optr fsync */
  {  202,   -2 }, /* (109) db_optr ::= db_optr comp */
  {  202,   -2 }, /* (110) db_optr ::= db_optr prec */
  {  202,   -2 }, /* (111) db_optr ::= db_optr keep */
  {  202,   -2 }, /* (112) db_optr ::= db_optr update */
  {  202,   -2 }, /* (113) db_optr ::= db_optr cachelast */
  {  203,   -1 }, /* (114) topic_optr ::= db_optr */
  {  203,   -2 }, /* (115) topic_optr ::= topic_optr partitions */
  {  197,    0 }, /* (116) alter_db_optr ::= */
  {  197,   -2 }, /* (117) alter_db_optr ::= alter_db_optr replica */
  {  197,   -2 }, /* (118) alter_db_optr ::= alter_db_optr quorum */
  {  197,   -2 }, /* (119) alter_db_optr ::= alter_db_optr keep */
  {  197,   -2 }, /* (120) alter_db_optr ::= alter_db_optr blocks */
  {  197,   -2 }, /* (121) alter_db_optr ::= alter_db_optr comp */
  {  197,   -2 }, /* (122) alter_db_optr ::= alter_db_optr wal */
  {  197,   -2 }, /* (123) alter_db_optr ::= alter_db_optr fsync */
  {  197,   -2 }, /* (124) alter_db_optr ::= alter_db_optr update */
  {  197,   -2 }, /* (125) alter_db_optr ::= alter_db_optr cachelast */
  {  198,   -1 }, /* (126) alter_topic_optr ::= alter_db_optr */
  {  198,   -2 }, /* (127) alter_topic_optr ::= alter_topic_optr partitions */
  {  231,   -1 }, /* (128) typename ::= ids */
  {  231,   -4 }, /* (129) typename ::= ids LP signed RP */
  {  231,   -2 }, /* (130) typename ::= ids UNSIGNED */
  {  232,   -1 }, /* (131) signed ::= INTEGER */
  {  232,   -2 }, /* (132) signed ::= PLUS INTEGER */
  {  232,   -2 }, /* (133) signed ::= MINUS INTEGER */
  {  192,   -3 }, /* (134) cmd ::= CREATE TABLE create_table_args */
  {  192,   -3 }, /* (135) cmd ::= CREATE TABLE create_stable_args */
  {  192,   -3 }, /* (136) cmd ::= CREATE STABLE create_stable_args */
  {  192,   -3 }, /* (137) cmd ::= CREATE TABLE create_table_list */
  {  235,   -1 }, /* (138) create_table_list ::= create_from_stable */
  {  235,   -2 }, /* (139) create_table_list ::= create_table_list create_from_stable */
  {  233,   -6 }, /* (140) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  234,  -10 }, /* (141) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  236,  -10 }, /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  236,  -13 }, /* (143) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  239,   -3 }, /* (144) tagNamelist ::= tagNamelist COMMA ids */
  {  239,   -1 }, /* (145) tagNamelist ::= ids */
  {  233,   -5 }, /* (146) create_table_args ::= ifnotexists ids cpxName AS select */
  {  237,   -3 }, /* (147) columnlist ::= columnlist COMMA column */
  {  237,   -1 }, /* (148) columnlist ::= column */
  {  241,   -2 }, /* (149) column ::= ids typename */
  {  238,   -3 }, /* (150) tagitemlist ::= tagitemlist COMMA tagitem */
  {  238,   -1 }, /* (151) tagitemlist ::= tagitem */
  {  242,   -1 }, /* (152) tagitem ::= INTEGER */
  {  242,   -1 }, /* (153) tagitem ::= FLOAT */
  {  242,   -1 }, /* (154) tagitem ::= STRING */
  {  242,   -1 }, /* (155) tagitem ::= BOOL */
  {  242,   -1 }, /* (156) tagitem ::= NULL */
  {  242,   -2 }, /* (157) tagitem ::= MINUS INTEGER */
  {  242,   -2 }, /* (158) tagitem ::= MINUS FLOAT */
  {  242,   -2 }, /* (159) tagitem ::= PLUS INTEGER */
  {  242,   -2 }, /* (160) tagitem ::= PLUS FLOAT */
  {  240,  -14 }, /* (161) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  240,   -3 }, /* (162) select ::= LP select RP */
  {  256,   -1 }, /* (163) union ::= select */
  {  256,   -4 }, /* (164) union ::= union UNION ALL select */
  {  192,   -1 }, /* (165) cmd ::= union */
  {  240,   -2 }, /* (166) select ::= SELECT selcollist */
  {  257,   -2 }, /* (167) sclp ::= selcollist COMMA */
  {  257,    0 }, /* (168) sclp ::= */
  {  243,   -4 }, /* (169) selcollist ::= sclp distinct expr as */
  {  243,   -2 }, /* (170) selcollist ::= sclp STAR */
  {  260,   -2 }, /* (171) as ::= AS ids */
  {  260,   -1 }, /* (172) as ::= ids */
  {  260,    0 }, /* (173) as ::= */
  {  258,   -1 }, /* (174) distinct ::= DISTINCT */
  {  258,    0 }, /* (175) distinct ::= */
  {  244,   -2 }, /* (176) from ::= FROM tablelist */
  {  244,   -2 }, /* (177) from ::= FROM sub */
  {  262,   -3 }, /* (178) sub ::= LP union RP */
  {  262,   -4 }, /* (179) sub ::= LP union RP ids */
  {  262,   -6 }, /* (180) sub ::= sub COMMA LP union RP ids */
  {  261,   -2 }, /* (181) tablelist ::= ids cpxName */
  {  261,   -3 }, /* (182) tablelist ::= ids cpxName ids */
  {  261,   -4 }, /* (183) tablelist ::= tablelist COMMA ids cpxName */
  {  261,   -5 }, /* (184) tablelist ::= tablelist COMMA ids cpxName ids */
  {  263,   -1 }, /* (185) tmvar ::= VARIABLE */
  {  246,   -4 }, /* (186) interval_opt ::= INTERVAL LP tmvar RP */
  {  246,   -6 }, /* (187) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  246,    0 }, /* (188) interval_opt ::= */
  {  247,    0 }, /* (189) session_option ::= */
  {  247,   -7 }, /* (190) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  248,    0 }, /* (191) windowstate_option ::= */
  {  248,   -4 }, /* (192) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  249,    0 }, /* (193) fill_opt ::= */
  {  249,   -6 }, /* (194) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  249,   -4 }, /* (195) fill_opt ::= FILL LP ID RP */
  {  250,   -4 }, /* (196) sliding_opt ::= SLIDING LP tmvar RP */
  {  250,    0 }, /* (197) sliding_opt ::= */
  {  252,    0 }, /* (198) orderby_opt ::= */
  {  252,   -3 }, /* (199) orderby_opt ::= ORDER BY sortlist */
  {  264,   -4 }, /* (200) sortlist ::= sortlist COMMA item sortorder */
  {  264,   -2 }, /* (201) sortlist ::= item sortorder */
  {  266,   -2 }, /* (202) item ::= ids cpxName */
  {  267,   -1 }, /* (203) sortorder ::= ASC */
  {  267,   -1 }, /* (204) sortorder ::= DESC */
  {  267,    0 }, /* (205) sortorder ::= */
  {  251,    0 }, /* (206) groupby_opt ::= */
  {  251,   -3 }, /* (207) groupby_opt ::= GROUP BY grouplist */
  {  268,   -3 }, /* (208) grouplist ::= grouplist COMMA item */
  {  268,   -1 }, /* (209) grouplist ::= item */
  {  253,    0 }, /* (210) having_opt ::= */
  {  253,   -2 }, /* (211) having_opt ::= HAVING expr */
  {  255,    0 }, /* (212) limit_opt ::= */
  {  255,   -2 }, /* (213) limit_opt ::= LIMIT signed */
  {  255,   -4 }, /* (214) limit_opt ::= LIMIT signed OFFSET signed */
  {  255,   -4 }, /* (215) limit_opt ::= LIMIT signed COMMA signed */
  {  254,    0 }, /* (216) slimit_opt ::= */
  {  254,   -2 }, /* (217) slimit_opt ::= SLIMIT signed */
  {  254,   -4 }, /* (218) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  254,   -4 }, /* (219) slimit_opt ::= SLIMIT signed COMMA signed */
  {  245,    0 }, /* (220) where_opt ::= */
  {  245,   -2 }, /* (221) where_opt ::= WHERE expr */
  {  259,   -3 }, /* (222) expr ::= LP expr RP */
  {  259,   -1 }, /* (223) expr ::= ID */
  {  259,   -3 }, /* (224) expr ::= ID DOT ID */
  {  259,   -3 }, /* (225) expr ::= ID DOT STAR */
  {  259,   -1 }, /* (226) expr ::= INTEGER */
  {  259,   -2 }, /* (227) expr ::= MINUS INTEGER */
  {  259,   -2 }, /* (228) expr ::= PLUS INTEGER */
  {  259,   -1 }, /* (229) expr ::= FLOAT */
  {  259,   -2 }, /* (230) expr ::= MINUS FLOAT */
  {  259,   -2 }, /* (231) expr ::= PLUS FLOAT */
  {  259,   -1 }, /* (232) expr ::= STRING */
  {  259,   -1 }, /* (233) expr ::= NOW */
  {  259,   -1 }, /* (234) expr ::= VARIABLE */
  {  259,   -2 }, /* (235) expr ::= PLUS VARIABLE */
  {  259,   -2 }, /* (236) expr ::= MINUS VARIABLE */
  {  259,   -1 }, /* (237) expr ::= BOOL */
  {  259,   -1 }, /* (238) expr ::= NULL */
  {  259,   -4 }, /* (239) expr ::= ID LP exprlist RP */
  {  259,   -4 }, /* (240) expr ::= ID LP STAR RP */
  {  259,   -3 }, /* (241) expr ::= expr IS NULL */
  {  259,   -4 }, /* (242) expr ::= expr IS NOT NULL */
  {  259,   -3 }, /* (243) expr ::= expr LT expr */
  {  259,   -3 }, /* (244) expr ::= expr GT expr */
  {  259,   -3 }, /* (245) expr ::= expr LE expr */
  {  259,   -3 }, /* (246) expr ::= expr GE expr */
  {  259,   -3 }, /* (247) expr ::= expr NE expr */
  {  259,   -3 }, /* (248) expr ::= expr EQ expr */
  {  259,   -5 }, /* (249) expr ::= expr BETWEEN expr AND expr */
  {  259,   -3 }, /* (250) expr ::= expr AND expr */
  {  259,   -3 }, /* (251) expr ::= expr OR expr */
  {  259,   -3 }, /* (252) expr ::= expr PLUS expr */
  {  259,   -3 }, /* (253) expr ::= expr MINUS expr */
  {  259,   -3 }, /* (254) expr ::= expr STAR expr */
  {  259,   -3 }, /* (255) expr ::= expr SLASH expr */
  {  259,   -3 }, /* (256) expr ::= expr REM expr */
  {  259,   -3 }, /* (257) expr ::= expr LIKE expr */
  {  259,   -5 }, /* (258) expr ::= expr IN LP exprlist RP */
  {  200,   -3 }, /* (259) exprlist ::= exprlist COMMA expritem */
  {  200,   -1 }, /* (260) exprlist ::= expritem */
  {  269,   -1 }, /* (261) expritem ::= expr */
  {  269,    0 }, /* (262) expritem ::= */
  {  192,   -3 }, /* (263) cmd ::= RESET QUERY CACHE */
  {  192,   -3 }, /* (264) cmd ::= SYNCDB ids REPLICA */
  {  192,   -7 }, /* (265) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  192,   -7 }, /* (266) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  192,   -7 }, /* (267) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  192,   -7 }, /* (268) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  192,   -7 }, /* (269) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  192,   -8 }, /* (270) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  192,   -9 }, /* (271) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  192,   -7 }, /* (272) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  192,   -7 }, /* (273) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  192,   -7 }, /* (274) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  192,   -7 }, /* (275) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  192,   -7 }, /* (276) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  192,   -7 }, /* (277) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  192,   -8 }, /* (278) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  192,   -9 }, /* (279) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  192,   -7 }, /* (280) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  192,   -3 }, /* (281) cmd ::= KILL CONNECTION INTEGER */
  {  192,   -5 }, /* (282) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  192,   -5 }, /* (283) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 134: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==135);
      case 136: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==136);
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
      case 21: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 29: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 30: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 31: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 32: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 33: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 34: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 35: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 36: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 38: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 39: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 45: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==45);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy382, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy151);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy151);}
        break;
      case 48: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy441);}
        break;
      case 49: /* ids ::= ID */
      case 50: /* ids ::= STRING */ yytestcase(yyruleno==50);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 51: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 52: /* ifexists ::= */
      case 54: /* ifnotexists ::= */ yytestcase(yyruleno==54);
      case 175: /* distinct ::= */ yytestcase(yyruleno==175);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 53: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy151);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy382, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 60: /* pps ::= */
      case 62: /* tseries ::= */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= */ yytestcase(yyruleno==64);
      case 66: /* streams ::= */ yytestcase(yyruleno==66);
      case 68: /* storage ::= */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= */ yytestcase(yyruleno==70);
      case 72: /* users ::= */ yytestcase(yyruleno==72);
      case 74: /* conns ::= */ yytestcase(yyruleno==74);
      case 76: /* state ::= */ yytestcase(yyruleno==76);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 61: /* pps ::= PPS INTEGER */
      case 63: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==63);
      case 65: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==69);
      case 71: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==71);
      case 73: /* users ::= USERS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==75);
      case 77: /* state ::= STATE ids */ yytestcase(yyruleno==77);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 78: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy151.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy151.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy151.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy151.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy151.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy151.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy151.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy151.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy151.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy151 = yylhsminor.yy151;
        break;
      case 79: /* intitemlist ::= intitemlist COMMA intitem */
      case 150: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==150);
{ yylhsminor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);    }
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 80: /* intitemlist ::= intitem */
      case 151: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==151);
{ yylhsminor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1); }
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 81: /* intitem ::= INTEGER */
      case 152: /* tagitem ::= INTEGER */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= STRING */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= BOOL */ yytestcase(yyruleno==155);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 82: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy441 = yymsp[0].minor.yy441; }
        break;
      case 83: /* cache ::= CACHE INTEGER */
      case 84: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==84);
      case 85: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==85);
      case 86: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==89);
      case 90: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==90);
      case 91: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==91);
      case 92: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==92);
      case 93: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==93);
      case 94: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==94);
      case 95: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==95);
      case 96: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==96);
      case 97: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==97);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 98: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy382); yymsp[1].minor.yy382.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 99: /* db_optr ::= db_optr cache */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 100: /* db_optr ::= db_optr replica */
      case 117: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==117);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 101: /* db_optr ::= db_optr quorum */
      case 118: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==118);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 102: /* db_optr ::= db_optr days */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 103: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 104: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 105: /* db_optr ::= db_optr blocks */
      case 120: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==120);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 106: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 107: /* db_optr ::= db_optr wal */
      case 122: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==122);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 108: /* db_optr ::= db_optr fsync */
      case 123: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==123);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 109: /* db_optr ::= db_optr comp */
      case 121: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==121);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 110: /* db_optr ::= db_optr prec */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 111: /* db_optr ::= db_optr keep */
      case 119: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==119);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.keep = yymsp[0].minor.yy441; }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 112: /* db_optr ::= db_optr update */
      case 124: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==124);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 113: /* db_optr ::= db_optr cachelast */
      case 125: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==125);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 114: /* topic_optr ::= db_optr */
      case 126: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==126);
{ yylhsminor.yy382 = yymsp[0].minor.yy382; yylhsminor.yy382.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy382 = yylhsminor.yy382;
        break;
      case 115: /* topic_optr ::= topic_optr partitions */
      case 127: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==127);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 116: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy382); yymsp[1].minor.yy382.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 128: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy343, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy343 = yylhsminor.yy343;
        break;
      case 129: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy369 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy343, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;  // negative value of name length
    tSetColumnType(&yylhsminor.yy343, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy343 = yylhsminor.yy343;
        break;
      case 130: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy343, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy343 = yylhsminor.yy343;
        break;
      case 131: /* signed ::= INTEGER */
{ yylhsminor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 132: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 133: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 137: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy182;}
        break;
      case 138: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy456);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy182 = pCreateTable;
}
  yymsp[0].minor.yy182 = yylhsminor.yy182;
        break;
      case 139: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy182->childTableInfo, &yymsp[0].minor.yy456);
  yylhsminor.yy182 = yymsp[-1].minor.yy182;
}
  yymsp[-1].minor.yy182 = yylhsminor.yy182;
        break;
      case 140: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy182 = tSetCreateTableInfo(yymsp[-1].minor.yy441, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy182 = yylhsminor.yy182;
        break;
      case 141: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy182 = tSetCreateTableInfo(yymsp[-5].minor.yy441, yymsp[-1].minor.yy441, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy182 = yylhsminor.yy182;
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy456 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy441, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy456 = yylhsminor.yy456;
        break;
      case 143: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy456 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy441, yymsp[-1].minor.yy441, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy456 = yylhsminor.yy456;
        break;
      case 144: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy441, &yymsp[0].minor.yy0); yylhsminor.yy441 = yymsp[-2].minor.yy441;  }
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 145: /* tagNamelist ::= ids */
{yylhsminor.yy441 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy441, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 146: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy182 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy236, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy182 = yylhsminor.yy182;
        break;
      case 147: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy441, &yymsp[0].minor.yy343); yylhsminor.yy441 = yymsp[-2].minor.yy441;  }
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 148: /* columnlist ::= column */
{yylhsminor.yy441 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy441, &yymsp[0].minor.yy343);}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 149: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy343, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy343);
}
  yymsp[-1].minor.yy343 = yylhsminor.yy343;
        break;
      case 156: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 157: /* tagitem ::= MINUS INTEGER */
      case 158: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==160);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy506, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 161: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy236 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy441, yymsp[-11].minor.yy244, yymsp[-10].minor.yy166, yymsp[-4].minor.yy441, yymsp[-3].minor.yy441, &yymsp[-9].minor.yy340, &yymsp[-8].minor.yy259, &yymsp[-7].minor.yy348, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy441, &yymsp[0].minor.yy414, &yymsp[-1].minor.yy414, yymsp[-2].minor.yy166);
}
  yymsp[-13].minor.yy236 = yylhsminor.yy236;
        break;
      case 162: /* select ::= LP select RP */
{yymsp[-2].minor.yy236 = yymsp[-1].minor.yy236;}
        break;
      case 163: /* union ::= select */
{ yylhsminor.yy441 = setSubclause(NULL, yymsp[0].minor.yy236); }
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 164: /* union ::= union UNION ALL select */
{ yylhsminor.yy441 = appendSelectClause(yymsp[-3].minor.yy441, yymsp[0].minor.yy236); }
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 165: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy441, NULL, TSDB_SQL_SELECT); }
        break;
      case 166: /* select ::= SELECT selcollist */
{
  yylhsminor.yy236 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy441, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 167: /* sclp ::= selcollist COMMA */
{yylhsminor.yy441 = yymsp[-1].minor.yy441;}
  yymsp[-1].minor.yy441 = yylhsminor.yy441;
        break;
      case 168: /* sclp ::= */
      case 198: /* orderby_opt ::= */ yytestcase(yyruleno==198);
{yymsp[1].minor.yy441 = 0;}
        break;
      case 169: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy441 = tSqlExprListAppend(yymsp[-3].minor.yy441, yymsp[-1].minor.yy166,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 170: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy441 = tSqlExprListAppend(yymsp[-1].minor.yy441, pNode, 0, 0);
}
  yymsp[-1].minor.yy441 = yylhsminor.yy441;
        break;
      case 171: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 172: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 173: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 174: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 176: /* from ::= FROM tablelist */
      case 177: /* from ::= FROM sub */ yytestcase(yyruleno==177);
{yymsp[-1].minor.yy244 = yymsp[0].minor.yy244;}
        break;
      case 178: /* sub ::= LP union RP */
{yymsp[-2].minor.yy244 = addSubqueryElem(NULL, yymsp[-1].minor.yy441, NULL);}
        break;
      case 179: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy244 = addSubqueryElem(NULL, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 180: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy244 = addSubqueryElem(yymsp[-5].minor.yy244, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy244 = yylhsminor.yy244;
        break;
      case 181: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy244 = yylhsminor.yy244;
        break;
      case 182: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 183: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(yymsp[-3].minor.yy244, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy244 = yylhsminor.yy244;
        break;
      case 184: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(yymsp[-4].minor.yy244, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy244 = yylhsminor.yy244;
        break;
      case 185: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 186: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy340.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy340.offset.n = 0;}
        break;
      case 187: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy340.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy340.offset = yymsp[-1].minor.yy0;}
        break;
      case 188: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy340, 0, sizeof(yymsp[1].minor.yy340));}
        break;
      case 189: /* session_option ::= */
{yymsp[1].minor.yy259.col.n = 0; yymsp[1].minor.yy259.gap.n = 0;}
        break;
      case 190: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy259.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy259.gap = yymsp[-1].minor.yy0;
}
        break;
      case 191: /* windowstate_option ::= */
{ yymsp[1].minor.yy348.col.n = 0; yymsp[1].minor.yy348.col.z = NULL;}
        break;
      case 192: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy348.col = yymsp[-1].minor.yy0; }
        break;
      case 193: /* fill_opt ::= */
{ yymsp[1].minor.yy441 = 0;     }
        break;
      case 194: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy441, &A, -1, 0);
    yymsp[-5].minor.yy441 = yymsp[-1].minor.yy441;
}
        break;
      case 195: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy441 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 196: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 197: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 199: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 200: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy441 = tVariantListAppend(yymsp[-3].minor.yy441, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 201: /* sortlist ::= item sortorder */
{
  yylhsminor.yy441 = tVariantListAppend(NULL, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
  yymsp[-1].minor.yy441 = yylhsminor.yy441;
        break;
      case 202: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy506, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 203: /* sortorder ::= ASC */
{ yymsp[0].minor.yy112 = TSDB_ORDER_ASC; }
        break;
      case 204: /* sortorder ::= DESC */
{ yymsp[0].minor.yy112 = TSDB_ORDER_DESC;}
        break;
      case 205: /* sortorder ::= */
{ yymsp[1].minor.yy112 = TSDB_ORDER_ASC; }
        break;
      case 206: /* groupby_opt ::= */
{ yymsp[1].minor.yy441 = 0;}
        break;
      case 207: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 208: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);
}
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 209: /* grouplist ::= item */
{
  yylhsminor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1);
}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 210: /* having_opt ::= */
      case 220: /* where_opt ::= */ yytestcase(yyruleno==220);
      case 262: /* expritem ::= */ yytestcase(yyruleno==262);
{yymsp[1].minor.yy166 = 0;}
        break;
      case 211: /* having_opt ::= HAVING expr */
      case 221: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==221);
{yymsp[-1].minor.yy166 = yymsp[0].minor.yy166;}
        break;
      case 212: /* limit_opt ::= */
      case 216: /* slimit_opt ::= */ yytestcase(yyruleno==216);
{yymsp[1].minor.yy414.limit = -1; yymsp[1].minor.yy414.offset = 0;}
        break;
      case 213: /* limit_opt ::= LIMIT signed */
      case 217: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==217);
{yymsp[-1].minor.yy414.limit = yymsp[0].minor.yy369;  yymsp[-1].minor.yy414.offset = 0;}
        break;
      case 214: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy414.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 215: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy414.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 218: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy414.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 219: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy414.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 222: /* expr ::= LP expr RP */
{yylhsminor.yy166 = yymsp[-1].minor.yy166; yylhsminor.yy166->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy166->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 223: /* expr ::= ID */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 224: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 225: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 226: /* expr ::= INTEGER */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 227: /* expr ::= MINUS INTEGER */
      case 228: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 229: /* expr ::= FLOAT */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 230: /* expr ::= MINUS FLOAT */
      case 231: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==231);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 232: /* expr ::= STRING */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 233: /* expr ::= NOW */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 234: /* expr ::= VARIABLE */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 235: /* expr ::= PLUS VARIABLE */
      case 236: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 237: /* expr ::= BOOL */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 238: /* expr ::= NULL */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 239: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy166 = tSqlExprCreateFunction(yymsp[-1].minor.yy441, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 240: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy166 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 241: /* expr ::= expr IS NULL */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 242: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-3].minor.yy166, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 243: /* expr ::= expr LT expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LT);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 244: /* expr ::= expr GT expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GT);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 245: /* expr ::= expr LE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 246: /* expr ::= expr GE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 247: /* expr ::= expr NE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_NE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 248: /* expr ::= expr EQ expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_EQ);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 249: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy166); yylhsminor.yy166 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy166, yymsp[-2].minor.yy166, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy166, TK_LE), TK_AND);}
  yymsp[-4].minor.yy166 = yylhsminor.yy166;
        break;
      case 250: /* expr ::= expr AND expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_AND);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 251: /* expr ::= expr OR expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_OR); }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 252: /* expr ::= expr PLUS expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_PLUS);  }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 253: /* expr ::= expr MINUS expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_MINUS); }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 254: /* expr ::= expr STAR expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_STAR);  }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 255: /* expr ::= expr SLASH expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_DIVIDE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 256: /* expr ::= expr REM expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_REM);   }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 257: /* expr ::= expr LIKE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LIKE);  }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 258: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-4].minor.yy166, (tSqlExpr*)yymsp[-1].minor.yy441, TK_IN); }
  yymsp[-4].minor.yy166 = yylhsminor.yy166;
        break;
      case 259: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy441 = tSqlExprListAppend(yymsp[-2].minor.yy441,yymsp[0].minor.yy166,0, 0);}
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 260: /* exprlist ::= expritem */
{yylhsminor.yy441 = tSqlExprListAppend(0,yymsp[0].minor.yy166,0, 0);}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 261: /* expritem ::= expr */
{yylhsminor.yy166 = yymsp[0].minor.yy166;}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 263: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 264: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 271: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 279: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 282: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 283: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
