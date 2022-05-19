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
#define YYNOCODE 295
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SRelationInfo* yy52;
  int32_t yy88;
  tVariant yy134;
  int64_t yy165;
  SCreatedTableInfo yy192;
  SIntervalVal yy196;
  SArray* yy249;
  SSqlNode* yy320;
  SLimitVal yy342;
  tSqlExpr* yy370;
  SRangeVal yy384;
  SWindowStateVal yy385;
  int yy424;
  TAOS_FIELD yy475;
  SCreateDbInfo yy478;
  SCreateTableSql* yy494;
  SCreateAcctInfo yy547;
  SSessionWindowVal yy559;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             408
#define YYNRULE              324
#define YYNTOKEN             206
#define YY_MAX_SHIFT         407
#define YY_MIN_SHIFTREDUCE   637
#define YY_MAX_SHIFTREDUCE   960
#define YY_ERROR_ACTION      961
#define YY_ACCEPT_ACTION     962
#define YY_NO_ACTION         963
#define YY_MIN_REDUCE        964
#define YY_MAX_REDUCE        1287
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
#define YY_ACTTAB_COUNT (893)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */  1201,  688, 1202,  323,  327,  102,  260,  101,  894,  689,
 /*    10 */   897,  688,  962,  407,   38,   39, 1149,   42,   43,  689,
 /*    20 */    87,  273,   31,   30,   29,  168,   24,   41,  357,   46,
 /*    30 */    44,   47,   45,   32,  406,  251, 1260,   37,   36,  223,
 /*    40 */   107,   35,   34,   33,   38,   39, 1107,   42,   43, 1260,
 /*    50 */   183,  273,   31,   30,   29, 1106,  688,   41,  357,   46,
 /*    60 */    44,   47,   45,   32,  689, 1140, 1140,   37,   36,  313,
 /*    70 */   224,   35,   34,   33, 1104, 1105,   56, 1108,   38,   39,
 /*    80 */  1260,   42,   43,  297,  254,  273,   31,   30,   29, 1146,
 /*    90 */    90,   41,  357,   46,   44,   47,   45,   32,  868,  304,
 /*   100 */   303,   37,   36,  229,  353,   35,   34,   33,   38,   39,
 /*   110 */   271,   42,   43, 1260,  392,  273,   31,   30,   29,  283,
 /*   120 */    59,   41,  357,   46,   44,   47,   45,   32,  774,  902,
 /*   130 */   191,   37,   36,  110,  353,   35,   34,   33,  258,   38,
 /*   140 */    40,   52,   42,   43,   13, 1125,  273,   31,   30,   29,
 /*   150 */   891,  888,   41,  357,   46,   44,   47,   45,   32,  867,
 /*   160 */   382,  381,   37,   36,  230,  688,   35,   34,   33,   39,
 /*   170 */  1109,   42,   43,  689, 1260,  273,   31,   30,   29,  109,
 /*   180 */    88,   41,  357,   46,   44,   47,   45,   32,   35,   34,
 /*   190 */    33,   37,   36,   96,   55,   35,   34,   33,   68,  351,
 /*   200 */   399,  398,  350,  349,  348,  397,  347,  346,  345,  396,
 /*   210 */   344,  395,  394,  638,  639,  640,  641,  642,  643,  644,
 /*   220 */   645,  646,  647,  648,  649,  650,  651,  162,  334,  252,
 /*   230 */   289,   42,   43, 1124,   69,  273,   31,   30,   29,  293,
 /*   240 */   292,   41,  357,   46,   44,   47,   45,   32,  270,  848,
 /*   250 */   849,   37,   36, 1282, 1115,   35,   34,   33, 1083, 1071,
 /*   260 */  1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080, 1081,
 /*   270 */  1082, 1084, 1085,   25,   46,   44,   47,   45,   32,  161,
 /*   280 */   159,  158,   37,   36,  276,  135,   35,   34,   33, 1140,
 /*   290 */   235,   32,  405,  403,  665,   37,   36,  237,  392,   35,
 /*   300 */    34,   33,  266,  149,  148,  147,  236,  255,  245,  904,
 /*   310 */   365,   96,  892,  221,  895,  893,  898,  896,  245,  904,
 /*   320 */   225,  231,  892, 1260,  895, 1263,  898,    5,   63,  196,
 /*   330 */  1260, 1260, 1262,  242,  195,  116,  121,  112,  120,  282,
 /*   340 */   133,  127,  138, 1260,  249,  250,  283,  137,  359,  143,
 /*   350 */   146,  136,   69,  339,  249,  250,  802,  193,  140,  799,
 /*   360 */  1199,  800, 1200,  801,  836,  277,  726,  275,  839,  368,
 /*   370 */   367,  216,  214,  212,  269,  296, 1274,   86,  211,  153,
 /*   380 */   152,  151,  150,  274,  246, 1251,  305,   68,  286,  399,
 /*   390 */   398,  278,  279,   48,  397, 1260,  309,  310,  396,  105,
 /*   400 */   395,  394, 1091,   48, 1089, 1090,   37,   36,  356, 1092,
 /*   410 */    35,   34,   33, 1093,   91, 1094, 1095,   60,   60,   60,
 /*   420 */   284,  104,  281,  103,  377,  376,  306,   60,   60,   60,
 /*   430 */   355,  905,  899,  901, 1250, 1212,  903,   60, 1211,  225,
 /*   440 */  1249,  905,  899,  901, 1260,   60,   60,  272,  225, 1260,
 /*   450 */  1260, 1263,  265,   60,  267,   60,  900,   60, 1260, 1125,
 /*   460 */  1263, 1125,  820,  253,  263,  264,  900,  247, 1012,    6,
 /*   470 */  1122, 1122, 1122,  369,  370,  371,  206, 1260,  803,  280,
 /*   480 */  1122, 1122, 1122,  372,  256,  248,  227,  228,  362,  283,
 /*   490 */  1122,  378,  379,    1,  194, 1260, 1260, 1260, 1122, 1122,
 /*   500 */   358,  380,  232,  384,   77,  226, 1121,  233, 1122,   93,
 /*   510 */  1122,  234, 1260,  239,  240, 1260,  817, 1260, 1022,  241,
 /*   520 */   238, 1260,  222, 1260, 1260,  283,  206, 1013,  298, 1260,
 /*   530 */  1260,   94, 1260,  400, 1052,  206, 1123,    3,  207,  308,
 /*   540 */   307,  845,  855,   61,  856,   10,  355,   80,   84,   78,
 /*   550 */   300,  824,  784,  331,  786,  333,  170,   72,  785,   49,
 /*   560 */   934,  906,  361,  326,  687,  374,  373,   61,   61,   72,
 /*   570 */   108,  261,  300,   72,  164,    9,    9, 1208,   15,    9,
 /*   580 */    14,  773, 1207,  126,  360,  125,   17,  262,   16,  809,
 /*   590 */    81,  810,  807,  186,  808,  383,  909,   19,  294,   18,
 /*   600 */   132,   21,  131,   20,  145,  144,  166,  167, 1120, 1148,
 /*   610 */    26, 1141, 1159, 1156, 1157,  301, 1161,  169,  174,  319,
 /*   620 */  1116, 1191,  185, 1190, 1189,  187, 1188, 1114,  188,  189,
 /*   630 */  1287,  160, 1027,  401,  336,  337,  338,  342,  835,  343,
 /*   640 */    70,  312,  219,   66,  354, 1021,  366, 1281,  123, 1280,
 /*   650 */  1138, 1277,   27,  257,   82,  314,  316,  197,  375, 1273,
 /*   660 */   129, 1272, 1269,   79,  175,  198,  328, 1049,   67,   62,
 /*   670 */    71,  176,  220, 1009,   28,  177,  324,  322,  139, 1007,
 /*   680 */   320,  318,  181,  179,  141,  142,  178, 1005, 1004,  285,
 /*   690 */   209,  210, 1001, 1000,  999,  998,  997,  996,  995,  213,
 /*   700 */   215,  987,  217,  984,  218,  980,  315,  311,  163,   89,
 /*   710 */    85,  299, 1118,  341,   92,  340,   97,  317,  393,  134,
 /*   720 */   385,  386,  387,  388,  389,   83,  268,  335,  390,  391,
 /*   730 */   165,  959,  288,  287,  958,  243,  291,  244,  290,  957,
 /*   740 */  1026,  190,  117, 1025,  192,  940,  118,  939,  295,  300,
 /*   750 */    11,  330,   95,  302,  812,   53,   98,  844,  201, 1003,
 /*   760 */  1002, 1050,  199,  204,  200,  203,  202,  205, 1087,  154,
 /*   770 */   329,  155,  994,    2,  156, 1051,   54,  182,  180,  184,
 /*   780 */   993,   75,  157,  986,  985,  842,  841,    4,  838,  837,
 /*   790 */  1097,   76,  173,  846,  171,  259,  857,  172,   64,  851,
 /*   800 */    99,   22,  853,  100,  321,  360,  325,   12,   23,   65,
 /*   810 */   106,   50,  332,   57,   51,  109,  114,  111,  702,  704,
 /*   820 */   113,  739,  737,   58,  736,  735,  115,  733,  732,  731,
 /*   830 */   728,  352,  692,  119,    7,  931,  929,  908,  932,  907,
 /*   840 */   930,    8,  364,  910,  363,   73,   61,  122,  776,  124,
 /*   850 */   806,   74,  775,  128,  130,  772,  720,  718,  710,  805,
 /*   860 */   716,  712,  714,  708,  706,  742,  741,  740,  738,  734,
 /*   870 */   730,  729,  208,  655,  690,  964,  664,  963,  662,  963,
 /*   880 */   963,  963,  963,  963,  963,  963,  963,  963,  963,  963,
 /*   890 */   402,  963,  404,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   289,    1,  291,  292,  288,  289,    1,  291,    5,    9,
 /*    10 */     7,    1,  207,  208,   14,   15,  209,   17,   18,    9,
 /*    20 */   217,   21,   22,   23,   24,  209,  281,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,  209,  210,  291,   37,   38,  281,
 /*    40 */   217,   41,   42,   43,   14,   15,    0,   17,   18,  291,
 /*    50 */   268,   21,   22,   23,   24,  252,    1,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,    9,  260,  260,   37,   38,  287,
 /*    70 */   281,   41,   42,   43,  251,  252,  253,  254,   14,   15,
 /*    80 */   291,   17,   18,  278,  278,   21,   22,   23,   24,  282,
 /*    90 */    90,   27,   28,   29,   30,   31,   32,   33,   80,  283,
 /*   100 */   284,   37,   38,  281,   88,   41,   42,   43,   14,   15,
 /*   110 */     1,   17,   18,  291,   95,   21,   22,   23,   24,  209,
 /*   120 */    90,   27,   28,   29,   30,   31,   32,   33,    5,  126,
 /*   130 */   220,   37,   38,  217,   88,   41,   42,   43,  256,   14,
 /*   140 */    15,   86,   17,   18,   86,  263,   21,   22,   23,   24,
 /*   150 */    41,   87,   27,   28,   29,   30,   31,   32,   33,  141,
 /*   160 */    37,   38,   37,   38,  281,    1,   41,   42,   43,   15,
 /*   170 */   254,   17,   18,    9,  291,   21,   22,   23,   24,  121,
 /*   180 */   122,   27,   28,   29,   30,   31,   32,   33,   41,   42,
 /*   190 */    43,   37,   38,   86,   86,   41,   42,   43,  103,  104,
 /*   200 */   105,  106,  107,  108,  109,  110,  111,  112,  113,  114,
 /*   210 */   115,  116,  117,   49,   50,   51,   52,   53,   54,   55,
 /*   220 */    56,   57,   58,   59,   60,   61,   62,   63,  120,   65,
 /*   230 */   151,   17,   18,  263,  127,   21,   22,   23,   24,  160,
 /*   240 */   161,   27,   28,   29,   30,   31,   32,   33,  216,  133,
 /*   250 */   134,   37,   38,  263,  209,   41,   42,   43,  234,  235,
 /*   260 */   236,  237,  238,  239,  240,  241,  242,  243,  244,  245,
 /*   270 */   246,  247,  248,   48,   29,   30,   31,   32,   33,   66,
 /*   280 */    67,   68,   37,   38,   72,   82,   41,   42,   43,  260,
 /*   290 */    65,   33,   69,   70,   71,   37,   38,   72,   95,   41,
 /*   300 */    42,   43,  257,   78,   79,   80,   81,  278,    1,    2,
 /*   310 */    85,   86,    5,  281,    7,    5,    9,    7,    1,    2,
 /*   320 */   281,  281,    5,  291,    7,  293,    9,   66,   67,   68,
 /*   330 */   291,  291,  293,  281,   73,   74,   75,   76,   77,   72,
 /*   340 */    66,   67,   68,  291,   37,   38,  209,   73,   41,   75,
 /*   350 */    76,   77,  127,   92,   37,   38,    2,  220,   84,    5,
 /*   360 */   289,    7,  291,    9,    5,  153,    5,  155,    9,  157,
 /*   370 */   158,   66,   67,   68,  216,  150,  263,  152,   73,   74,
 /*   380 */    75,   76,   77,  216,  159,  281,  286,  103,  163,  105,
 /*   390 */   106,   37,   38,   86,  110,  291,   37,   38,  114,  264,
 /*   400 */   116,  117,  234,   86,  236,  237,   37,   38,   25,  241,
 /*   410 */    41,   42,   43,  245,  279,  247,  248,  209,  209,  209,
 /*   420 */   153,  289,  155,  291,  157,  158,  286,  209,  209,  209,
 /*   430 */    47,  124,  125,  126,  281,  250,  126,  209,  250,  281,
 /*   440 */   281,  124,  125,  126,  291,  209,  209,   64,  281,  291,
 /*   450 */   291,  293,  256,  209,  256,  209,  149,  209,  291,  263,
 /*   460 */   293,  263,   41,  255,  255,  255,  149,  281,  215,   86,
 /*   470 */   262,  262,  262,  255,  255,  255,  223,  291,  124,  125,
 /*   480 */   262,  262,  262,  255,  125,  281,  281,  281,   16,  209,
 /*   490 */   262,  255,  255,  218,  219,  291,  291,  291,  262,  262,
 /*   500 */   220,  255,  281,  255,  102,  281,  262,  281,  262,   87,
 /*   510 */   262,  281,  291,  281,  281,  291,  102,  291,  215,  281,
 /*   520 */   281,  291,  281,  291,  291,  209,  223,  215,   87,  291,
 /*   530 */   291,   87,  291,  232,  233,  223,  220,  213,  214,   37,
 /*   540 */    38,   87,   87,  102,   87,  131,   47,  102,   86,  147,
 /*   550 */   128,  130,   87,   87,   87,   87,  102,  102,   87,  102,
 /*   560 */    87,   87,   25,   64,   87,   37,   38,  102,  102,  102,
 /*   570 */   102,  250,  128,  102,  209,  102,  102,  250,  154,  102,
 /*   580 */   156,  119,  250,  154,   47,  156,  154,  250,  156,    5,
 /*   590 */   145,    7,    5,  258,    7,  250,  124,  154,  209,  156,
 /*   600 */   154,  154,  156,  156,   82,   83,  209,  209,  209,  209,
 /*   610 */   280,  260,  209,  209,  209,  260,  209,  209,  209,  209,
 /*   620 */   260,  290,  265,  290,  290,  209,  290,  209,  209,  209,
 /*   630 */   266,   64,  209,   88,  209,  209,  209,  209,  126,  209,
 /*   640 */   209,  285,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   650 */   277,  209,  148,  285,  144,  285,  285,  209,  209,  209,
 /*   660 */   209,  209,  209,  146,  276,  209,  139,  209,  209,  209,
 /*   670 */   209,  275,  209,  209,  143,  274,  142,  137,  209,  209,
 /*   680 */   136,  135,  270,  272,  209,  209,  273,  209,  209,  209,
 /*   690 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   700 */   209,  209,  209,  209,  209,  209,  138,  132,  211,  123,
 /*   710 */   212,  211,  211,   94,  211,   93,  211,  211,  118,  101,
 /*   720 */   100,   55,   97,   99,   59,  211,  211,  211,   98,   96,
 /*   730 */   131,    5,    5,  162,    5,  211,    5,  211,  162,    5,
 /*   740 */   222,  221,  217,  222,  221,  105,  217,  104,  151,  128,
 /*   750 */    86,  120,  129,  102,   87,   86,  102,   87,  225,  211,
 /*   760 */   211,  231,  230,  227,  229,  226,  228,  224,  249,  212,
 /*   770 */   259,  212,  211,  218,  212,  233,  267,  269,  271,  266,
 /*   780 */   211,  102,  212,  211,  211,  126,  126,  213,    5,    5,
 /*   790 */   249,   86,  102,   87,   86,    1,   87,   86,  102,   87,
 /*   800 */    86,  140,   87,   86,   86,   47,    1,   86,  140,  102,
 /*   810 */    90,   86,  120,   91,   86,  121,   74,   82,    5,    5,
 /*   820 */    90,    9,    5,   91,    5,    5,   90,    5,    5,    5,
 /*   830 */     5,   16,   89,   82,   86,    9,    9,   87,    9,   87,
 /*   840 */     9,   86,   63,  124,   28,   17,  102,  156,    5,  156,
 /*   850 */   126,   17,    5,  156,  156,   87,    5,    5,    5,  126,
 /*   860 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   870 */     5,    5,  102,   64,   89,    0,    9,  294,    9,  294,
 /*   880 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   890 */    22,  294,   22,  294,  294,  294,  294,  294,  294,  294,
 /*   900 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   910 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   920 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   930 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   940 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   950 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   960 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   970 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   980 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*   990 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1000 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1010 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1020 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1030 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1040 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1050 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1060 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1070 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1080 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1090 */   294,  294,  294,  294,  294,  294,  294,  294,  294,
};
#define YY_SHIFT_COUNT    (407)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (875)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   225,   95,   95,  284,  284,   16,  307,  317,  317,  317,
 /*    10 */    55,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*    20 */    10,   10,    5,    5,    0,  164,  317,  317,  317,  317,
 /*    30 */   317,  317,  317,  317,  317,  317,  317,  317,  317,  317,
 /*    40 */   317,  317,  317,  317,  317,  317,  317,  317,  317,  354,
 /*    50 */   354,  354,  107,  107,  116,   10,   46,   10,   10,   10,
 /*    60 */    10,   10,  203,   16,    5,    5,   19,   19,  361,  893,
 /*    70 */   893,  893,  354,  354,  354,  359,  359,  123,  123,  123,
 /*    80 */   123,  123,  123,   58,  123,   10,   10,   10,   10,   10,
 /*    90 */    10,  421,   10,   10,   10,  107,  107,   10,   10,   10,
 /*   100 */    10,   18,   18,   18,   18,  414,  107,   10,   10,   10,
 /*   110 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   120 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   130 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   140 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   150 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   160 */    10,   10,   10,  504,  567,  545,  567,  567,  567,  567,
 /*   170 */   512,  512,  512,  512,  567,  510,  517,  527,  531,  534,
 /*   180 */   540,  544,  546,  568,  575,  504,  586,  567,  567,  567,
 /*   190 */   619,  622,  619,  622,  600,   16,   16,  567,  567,  618,
 /*   200 */   620,  666,  625,  624,  665,  630,  633,  600,  361,  567,
 /*   210 */   567,  545,  545,  567,  545,  567,  545,  567,  567,  893,
 /*   220 */   893,   30,   64,   94,   94,   94,  125,  154,  214,  245,
 /*   230 */   245,  245,  245,  245,  245,  261,  274,  305,  258,  258,
 /*   240 */   258,  258,  369,  212,  267,  383,   79,  147,  147,    3,
 /*   250 */   310,  223,  213,  441,  422,  444,  502,  454,  455,  457,
 /*   260 */   499,  402,  445,  465,  466,  467,  468,  471,  108,  473,
 /*   270 */   474,  537,  109,  472,  477,  424,  429,  432,  584,  587,
 /*   280 */   528,  443,  446,  462,  447,  522,  599,  726,  571,  727,
 /*   290 */   729,  576,  731,  734,  640,  643,  597,  621,  631,  664,
 /*   300 */   623,  667,  669,  651,  654,  670,  679,  659,  660,  783,
 /*   310 */   784,  705,  706,  708,  709,  711,  712,  690,  714,  715,
 /*   320 */   717,  794,  718,  696,  661,  758,  805,  707,  668,  720,
 /*   330 */   721,  631,  725,  692,  728,  694,  735,  722,  730,  742,
 /*   340 */   813,  814,  732,  736,  812,  817,  819,  820,  822,  823,
 /*   350 */   824,  825,  743,  815,  751,  826,  827,  748,  750,  752,
 /*   360 */   829,  831,  719,  755,  816,  779,  828,  691,  693,  744,
 /*   370 */   744,  744,  744,  724,  733,  834,  697,  698,  744,  744,
 /*   380 */   744,  843,  847,  768,  744,  851,  852,  853,  855,  856,
 /*   390 */   857,  858,  859,  860,  861,  862,  863,  864,  865,  866,
 /*   400 */   770,  785,  867,  868,  869,  870,  809,  875,
};
#define YY_REDUCE_COUNT (220)
#define YY_REDUCE_MIN   (-289)
#define YY_REDUCE_MAX   (574)
static const short yy_reduce_ofst[] = {
 /*     0 */  -195,   24,   24,  168,  168, -177,   32,  158,  167,   39,
 /*    10 */  -184,  208,  209,  210,  218,  219,  220,  228,  236,  237,
 /*    20 */   246,  248, -289, -284, -193, -175, -255, -242, -211, -178,
 /*    30 */  -117,   40,   52,  104,  153,  159,  186,  204,  205,  206,
 /*    40 */   221,  224,  226,  230,  232,  233,  238,  239,  241, -118,
 /*    50 */   196,  198, -194,   29, -218,   45,  -84,  -90,  137,  280,
 /*    60 */   316,  244,  253, -197,   71,  132,  303,  312,  301,  135,
 /*    70 */   275,  324,  -30,  -10,  113,  100,  140,  185,  188,  321,
 /*    80 */   327,  332,  337,  335,  345,  365,  389,  397,  398,  399,
 /*    90 */   400,  330,  403,  404,  405,  351,  355,  407,  408,  409,
 /*   100 */   410,  331,  333,  334,  336,  357,  360,  416,  418,  419,
 /*   110 */   420,  423,  425,  426,  427,  428,  430,  431,  433,  434,
 /*   120 */   435,  436,  437,  438,  439,  440,  442,  448,  449,  450,
 /*   130 */   451,  452,  453,  456,  458,  459,  460,  461,  463,  464,
 /*   140 */   469,  470,  475,  476,  478,  479,  480,  481,  482,  483,
 /*   150 */   484,  485,  486,  487,  488,  489,  490,  491,  492,  493,
 /*   160 */   494,  495,  496,  364,  497,  498,  500,  501,  503,  505,
 /*   170 */   356,  368,  370,  371,  506,  373,  388,  396,  401,  413,
 /*   180 */   411,  507,  412,  508,  509,  513,  511,  514,  515,  516,
 /*   190 */   518,  520,  521,  523,  519,  525,  529,  524,  526,  530,
 /*   200 */   532,  535,  533,  538,  539,  536,  543,  541,  542,  548,
 /*   210 */   549,  557,  559,  561,  562,  569,  570,  572,  573,  555,
 /*   220 */   574,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   961, 1086, 1023, 1096, 1010, 1020, 1265, 1265, 1265, 1265,
 /*    10 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*    20 */   961,  961,  961,  961, 1150,  981,  961,  961,  961,  961,
 /*    30 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*    40 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*    50 */   961,  961,  961,  961, 1174,  961, 1020,  961,  961,  961,
 /*    60 */   961,  961, 1032, 1020,  961,  961, 1032, 1032,  961, 1145,
 /*    70 */  1070, 1088,  961,  961,  961,  961,  961,  961,  961,  961,
 /*    80 */   961,  961,  961, 1117,  961,  961,  961,  961,  961,  961,
 /*    90 */   961, 1152, 1158, 1155,  961,  961,  961, 1160,  961,  961,
 /*   100 */   961, 1196, 1196, 1196, 1196, 1143,  961,  961,  961,  961,
 /*   110 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   120 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   130 */   961,  961,  961,  961,  961,  961,  961,  961,  961, 1008,
 /*   140 */   961, 1006,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   150 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   160 */   961,  961,  979, 1213,  983, 1018,  983,  983,  983,  983,
 /*   170 */   961,  961,  961,  961,  983, 1205, 1209, 1186, 1203, 1197,
 /*   180 */  1181, 1179, 1177, 1185, 1170, 1213, 1119,  983,  983,  983,
 /*   190 */  1030, 1028, 1030, 1028, 1024, 1020, 1020,  983,  983, 1048,
 /*   200 */  1046, 1044, 1036, 1042, 1038, 1040, 1034, 1011,  961,  983,
 /*   210 */   983, 1018, 1018,  983, 1018,  983, 1018,  983,  983, 1070,
 /*   220 */  1088, 1264,  961, 1214, 1204, 1264,  961, 1246, 1245, 1255,
 /*   230 */  1254, 1253, 1244, 1243, 1242,  961,  961,  961, 1238, 1241,
 /*   240 */  1240, 1239, 1252,  961,  961, 1216,  961, 1248, 1247,  961,
 /*   250 */   961,  961,  961,  961,  961,  961, 1167,  961,  961,  961,
 /*   260 */  1192, 1210, 1206,  961,  961,  961,  961,  961,  961,  961,
 /*   270 */   961, 1217,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   280 */  1131,  961,  961, 1098,  961,  961,  961,  961,  961,  961,
 /*   290 */   961,  961,  961,  961,  961,  961,  961, 1142,  961,  961,
 /*   300 */   961,  961,  961, 1154, 1153,  961,  961,  961,  961,  961,
 /*   310 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   320 */   961,  961,  961, 1198,  961, 1193,  961, 1187,  961,  961,
 /*   330 */   961, 1110,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   340 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   350 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   360 */   961,  961,  961,  961,  961,  961,  961,  961,  961, 1283,
 /*   370 */  1278, 1279, 1276,  961,  961,  961,  961,  961, 1275, 1270,
 /*   380 */  1271,  961,  961,  961, 1268,  961,  961,  961,  961,  961,
 /*   390 */   961,  961,  961,  961,  961,  961,  961,  961,  961,  961,
 /*   400 */  1054,  961,  961,  990,  961,  988,  961,  961,
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
    1,  /*       JSON => ID */
    0,  /*         OR => nothing */
    0,  /*        AND => nothing */
    0,  /*        NOT => nothing */
    0,  /*         EQ => nothing */
    0,  /*         NE => nothing */
    0,  /*     ISNULL => nothing */
    0,  /*    NOTNULL => nothing */
    0,  /*         IS => nothing */
    1,  /*       LIKE => ID */
    1,  /*      MATCH => ID */
    1,  /*     NMATCH => ID */
    0,  /*   CONTAINS => nothing */
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
    0,  /*     UMINUS => nothing */
    0,  /*      UPLUS => nothing */
    0,  /*     BITNOT => nothing */
    0,  /*      ARROW => nothing */
    0,  /*       SHOW => nothing */
    0,  /*  DATABASES => nothing */
    0,  /*     TOPICS => nothing */
    0,  /*  FUNCTIONS => nothing */
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
    0,  /*   FUNCTION => nothing */
    0,  /*      DNODE => nothing */
    0,  /*       USER => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    1,  /*       DESC => ID */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*    COMPACT => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*         AS => nothing */
    0,  /* OUTPUTTYPE => nothing */
    0,  /*  AGGREGATE => nothing */
    0,  /*    BUFSIZE => nothing */
    1,  /*     PARAMS => ID */
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
    0,  /*         TO => nothing */
    0,  /*      SPLIT => nothing */
    1,  /*       NULL => ID */
    1,  /*        NOW => ID */
    0,  /*   VARIABLE => nothing */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*      RANGE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*      EVERY => nothing */
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    0,  /*      GROUP => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      LIMIT => nothing */
    1,  /*     OFFSET => ID */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      WHERE => nothing */
    1,  /*      TODAY => ID */
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
    0,  /*     DELETE => nothing */
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
    1,  /*        KEY => ID */
    1,  /*         OF => ID */
    1,  /*      RAISE => ID */
    1,  /*    REPLACE => ID */
    1,  /*   RESTRICT => ID */
    1,  /*        ROW => ID */
    1,  /*  STATEMENT => ID */
    1,  /*    TRIGGER => ID */
    1,  /*       VIEW => ID */
    1,  /*    IPTOKEN => ID */
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
    1,  /*       FILE => ID */
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
  /*   13 */ "JSON",
  /*   14 */ "OR",
  /*   15 */ "AND",
  /*   16 */ "NOT",
  /*   17 */ "EQ",
  /*   18 */ "NE",
  /*   19 */ "ISNULL",
  /*   20 */ "NOTNULL",
  /*   21 */ "IS",
  /*   22 */ "LIKE",
  /*   23 */ "MATCH",
  /*   24 */ "NMATCH",
  /*   25 */ "CONTAINS",
  /*   26 */ "GLOB",
  /*   27 */ "BETWEEN",
  /*   28 */ "IN",
  /*   29 */ "GT",
  /*   30 */ "GE",
  /*   31 */ "LT",
  /*   32 */ "LE",
  /*   33 */ "BITAND",
  /*   34 */ "BITOR",
  /*   35 */ "LSHIFT",
  /*   36 */ "RSHIFT",
  /*   37 */ "PLUS",
  /*   38 */ "MINUS",
  /*   39 */ "DIVIDE",
  /*   40 */ "TIMES",
  /*   41 */ "STAR",
  /*   42 */ "SLASH",
  /*   43 */ "REM",
  /*   44 */ "UMINUS",
  /*   45 */ "UPLUS",
  /*   46 */ "BITNOT",
  /*   47 */ "ARROW",
  /*   48 */ "SHOW",
  /*   49 */ "DATABASES",
  /*   50 */ "TOPICS",
  /*   51 */ "FUNCTIONS",
  /*   52 */ "MNODES",
  /*   53 */ "DNODES",
  /*   54 */ "ACCOUNTS",
  /*   55 */ "USERS",
  /*   56 */ "MODULES",
  /*   57 */ "QUERIES",
  /*   58 */ "CONNECTIONS",
  /*   59 */ "STREAMS",
  /*   60 */ "VARIABLES",
  /*   61 */ "SCORES",
  /*   62 */ "GRANTS",
  /*   63 */ "VNODES",
  /*   64 */ "DOT",
  /*   65 */ "CREATE",
  /*   66 */ "TABLE",
  /*   67 */ "STABLE",
  /*   68 */ "DATABASE",
  /*   69 */ "TABLES",
  /*   70 */ "STABLES",
  /*   71 */ "VGROUPS",
  /*   72 */ "DROP",
  /*   73 */ "TOPIC",
  /*   74 */ "FUNCTION",
  /*   75 */ "DNODE",
  /*   76 */ "USER",
  /*   77 */ "ACCOUNT",
  /*   78 */ "USE",
  /*   79 */ "DESCRIBE",
  /*   80 */ "DESC",
  /*   81 */ "ALTER",
  /*   82 */ "PASS",
  /*   83 */ "PRIVILEGE",
  /*   84 */ "LOCAL",
  /*   85 */ "COMPACT",
  /*   86 */ "LP",
  /*   87 */ "RP",
  /*   88 */ "IF",
  /*   89 */ "EXISTS",
  /*   90 */ "AS",
  /*   91 */ "OUTPUTTYPE",
  /*   92 */ "AGGREGATE",
  /*   93 */ "BUFSIZE",
  /*   94 */ "PARAMS",
  /*   95 */ "PPS",
  /*   96 */ "TSERIES",
  /*   97 */ "DBS",
  /*   98 */ "STORAGE",
  /*   99 */ "QTIME",
  /*  100 */ "CONNS",
  /*  101 */ "STATE",
  /*  102 */ "COMMA",
  /*  103 */ "KEEP",
  /*  104 */ "CACHE",
  /*  105 */ "REPLICA",
  /*  106 */ "QUORUM",
  /*  107 */ "DAYS",
  /*  108 */ "MINROWS",
  /*  109 */ "MAXROWS",
  /*  110 */ "BLOCKS",
  /*  111 */ "CTIME",
  /*  112 */ "WAL",
  /*  113 */ "FSYNC",
  /*  114 */ "COMP",
  /*  115 */ "PRECISION",
  /*  116 */ "UPDATE",
  /*  117 */ "CACHELAST",
  /*  118 */ "PARTITIONS",
  /*  119 */ "UNSIGNED",
  /*  120 */ "TAGS",
  /*  121 */ "USING",
  /*  122 */ "TO",
  /*  123 */ "SPLIT",
  /*  124 */ "NULL",
  /*  125 */ "NOW",
  /*  126 */ "VARIABLE",
  /*  127 */ "SELECT",
  /*  128 */ "UNION",
  /*  129 */ "ALL",
  /*  130 */ "DISTINCT",
  /*  131 */ "FROM",
  /*  132 */ "RANGE",
  /*  133 */ "INTERVAL",
  /*  134 */ "EVERY",
  /*  135 */ "SESSION",
  /*  136 */ "STATE_WINDOW",
  /*  137 */ "FILL",
  /*  138 */ "SLIDING",
  /*  139 */ "ORDER",
  /*  140 */ "BY",
  /*  141 */ "ASC",
  /*  142 */ "GROUP",
  /*  143 */ "HAVING",
  /*  144 */ "LIMIT",
  /*  145 */ "OFFSET",
  /*  146 */ "SLIMIT",
  /*  147 */ "SOFFSET",
  /*  148 */ "WHERE",
  /*  149 */ "TODAY",
  /*  150 */ "RESET",
  /*  151 */ "QUERY",
  /*  152 */ "SYNCDB",
  /*  153 */ "ADD",
  /*  154 */ "COLUMN",
  /*  155 */ "MODIFY",
  /*  156 */ "TAG",
  /*  157 */ "CHANGE",
  /*  158 */ "SET",
  /*  159 */ "KILL",
  /*  160 */ "CONNECTION",
  /*  161 */ "STREAM",
  /*  162 */ "COLON",
  /*  163 */ "DELETE",
  /*  164 */ "ABORT",
  /*  165 */ "AFTER",
  /*  166 */ "ATTACH",
  /*  167 */ "BEFORE",
  /*  168 */ "BEGIN",
  /*  169 */ "CASCADE",
  /*  170 */ "CLUSTER",
  /*  171 */ "CONFLICT",
  /*  172 */ "COPY",
  /*  173 */ "DEFERRED",
  /*  174 */ "DELIMITERS",
  /*  175 */ "DETACH",
  /*  176 */ "EACH",
  /*  177 */ "END",
  /*  178 */ "EXPLAIN",
  /*  179 */ "FAIL",
  /*  180 */ "FOR",
  /*  181 */ "IGNORE",
  /*  182 */ "IMMEDIATE",
  /*  183 */ "INITIALLY",
  /*  184 */ "INSTEAD",
  /*  185 */ "KEY",
  /*  186 */ "OF",
  /*  187 */ "RAISE",
  /*  188 */ "REPLACE",
  /*  189 */ "RESTRICT",
  /*  190 */ "ROW",
  /*  191 */ "STATEMENT",
  /*  192 */ "TRIGGER",
  /*  193 */ "VIEW",
  /*  194 */ "IPTOKEN",
  /*  195 */ "SEMI",
  /*  196 */ "NONE",
  /*  197 */ "PREV",
  /*  198 */ "LINEAR",
  /*  199 */ "IMPORT",
  /*  200 */ "TBNAME",
  /*  201 */ "JOIN",
  /*  202 */ "INSERT",
  /*  203 */ "INTO",
  /*  204 */ "VALUES",
  /*  205 */ "FILE",
  /*  206 */ "error",
  /*  207 */ "program",
  /*  208 */ "cmd",
  /*  209 */ "ids",
  /*  210 */ "dbPrefix",
  /*  211 */ "cpxName",
  /*  212 */ "ifexists",
  /*  213 */ "alter_db_optr",
  /*  214 */ "alter_topic_optr",
  /*  215 */ "acct_optr",
  /*  216 */ "exprlist",
  /*  217 */ "ifnotexists",
  /*  218 */ "db_optr",
  /*  219 */ "topic_optr",
  /*  220 */ "typename",
  /*  221 */ "bufsize",
  /*  222 */ "params",
  /*  223 */ "pps",
  /*  224 */ "tseries",
  /*  225 */ "dbs",
  /*  226 */ "streams",
  /*  227 */ "storage",
  /*  228 */ "qtime",
  /*  229 */ "users",
  /*  230 */ "conns",
  /*  231 */ "state",
  /*  232 */ "intitemlist",
  /*  233 */ "intitem",
  /*  234 */ "keep",
  /*  235 */ "cache",
  /*  236 */ "replica",
  /*  237 */ "quorum",
  /*  238 */ "days",
  /*  239 */ "minrows",
  /*  240 */ "maxrows",
  /*  241 */ "blocks",
  /*  242 */ "ctime",
  /*  243 */ "wal",
  /*  244 */ "fsync",
  /*  245 */ "comp",
  /*  246 */ "prec",
  /*  247 */ "update",
  /*  248 */ "cachelast",
  /*  249 */ "partitions",
  /*  250 */ "signed",
  /*  251 */ "create_table_args",
  /*  252 */ "create_stable_args",
  /*  253 */ "create_table_list",
  /*  254 */ "create_from_stable",
  /*  255 */ "columnlist",
  /*  256 */ "tagitemlist",
  /*  257 */ "tagNamelist",
  /*  258 */ "to_opt",
  /*  259 */ "split_opt",
  /*  260 */ "select",
  /*  261 */ "to_split",
  /*  262 */ "column",
  /*  263 */ "tagitem",
  /*  264 */ "selcollist",
  /*  265 */ "from",
  /*  266 */ "where_opt",
  /*  267 */ "range_option",
  /*  268 */ "interval_option",
  /*  269 */ "sliding_opt",
  /*  270 */ "session_option",
  /*  271 */ "windowstate_option",
  /*  272 */ "fill_opt",
  /*  273 */ "groupby_opt",
  /*  274 */ "having_opt",
  /*  275 */ "orderby_opt",
  /*  276 */ "slimit_opt",
  /*  277 */ "limit_opt",
  /*  278 */ "union",
  /*  279 */ "sclp",
  /*  280 */ "distinct",
  /*  281 */ "expr",
  /*  282 */ "as",
  /*  283 */ "tablelist",
  /*  284 */ "sub",
  /*  285 */ "tmvar",
  /*  286 */ "timestamp",
  /*  287 */ "intervalKey",
  /*  288 */ "sortlist",
  /*  289 */ "item",
  /*  290 */ "sortorder",
  /*  291 */ "arrow",
  /*  292 */ "grouplist",
  /*  293 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW FUNCTIONS",
 /*   4 */ "cmd ::= SHOW MNODES",
 /*   5 */ "cmd ::= SHOW DNODES",
 /*   6 */ "cmd ::= SHOW ACCOUNTS",
 /*   7 */ "cmd ::= SHOW USERS",
 /*   8 */ "cmd ::= SHOW MODULES",
 /*   9 */ "cmd ::= SHOW QUERIES",
 /*  10 */ "cmd ::= SHOW CONNECTIONS",
 /*  11 */ "cmd ::= SHOW STREAMS",
 /*  12 */ "cmd ::= SHOW VARIABLES",
 /*  13 */ "cmd ::= SHOW SCORES",
 /*  14 */ "cmd ::= SHOW GRANTS",
 /*  15 */ "cmd ::= SHOW VNODES",
 /*  16 */ "cmd ::= SHOW VNODES ids",
 /*  17 */ "dbPrefix ::=",
 /*  18 */ "dbPrefix ::= ids DOT",
 /*  19 */ "cpxName ::=",
 /*  20 */ "cpxName ::= DOT ids",
 /*  21 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  22 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
 /*  23 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  25 */ "cmd ::= SHOW dbPrefix TABLES LIKE STRING",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  27 */ "cmd ::= SHOW dbPrefix STABLES LIKE STRING",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  29 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  32 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  33 */ "cmd ::= DROP FUNCTION ids",
 /*  34 */ "cmd ::= DROP DNODE ids",
 /*  35 */ "cmd ::= DROP USER ids",
 /*  36 */ "cmd ::= DROP ACCOUNT ids",
 /*  37 */ "cmd ::= USE ids",
 /*  38 */ "cmd ::= DESCRIBE ids cpxName",
 /*  39 */ "cmd ::= DESC ids cpxName",
 /*  40 */ "cmd ::= ALTER USER ids PASS ids",
 /*  41 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  42 */ "cmd ::= ALTER DNODE ids ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  44 */ "cmd ::= ALTER LOCAL ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids ids",
 /*  46 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  47 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  50 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  51 */ "ids ::= ID",
 /*  52 */ "ids ::= STRING",
 /*  53 */ "ifexists ::= IF EXISTS",
 /*  54 */ "ifexists ::=",
 /*  55 */ "ifnotexists ::= IF NOT EXISTS",
 /*  56 */ "ifnotexists ::=",
 /*  57 */ "cmd ::= CREATE DNODE ids",
 /*  58 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  59 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  60 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  61 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params",
 /*  62 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params",
 /*  63 */ "cmd ::= CREATE USER ids PASS ids",
 /*  64 */ "bufsize ::=",
 /*  65 */ "bufsize ::= BUFSIZE INTEGER",
 /*  66 */ "params ::=",
 /*  67 */ "params ::= PARAMS INTEGER",
 /*  68 */ "pps ::=",
 /*  69 */ "pps ::= PPS INTEGER",
 /*  70 */ "tseries ::=",
 /*  71 */ "tseries ::= TSERIES INTEGER",
 /*  72 */ "dbs ::=",
 /*  73 */ "dbs ::= DBS INTEGER",
 /*  74 */ "streams ::=",
 /*  75 */ "streams ::= STREAMS INTEGER",
 /*  76 */ "storage ::=",
 /*  77 */ "storage ::= STORAGE INTEGER",
 /*  78 */ "qtime ::=",
 /*  79 */ "qtime ::= QTIME INTEGER",
 /*  80 */ "users ::=",
 /*  81 */ "users ::= USERS INTEGER",
 /*  82 */ "conns ::=",
 /*  83 */ "conns ::= CONNS INTEGER",
 /*  84 */ "state ::=",
 /*  85 */ "state ::= STATE ids",
 /*  86 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  87 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  88 */ "intitemlist ::= intitem",
 /*  89 */ "intitem ::= INTEGER",
 /*  90 */ "keep ::= KEEP intitemlist",
 /*  91 */ "cache ::= CACHE INTEGER",
 /*  92 */ "replica ::= REPLICA INTEGER",
 /*  93 */ "quorum ::= QUORUM INTEGER",
 /*  94 */ "days ::= DAYS INTEGER",
 /*  95 */ "minrows ::= MINROWS INTEGER",
 /*  96 */ "maxrows ::= MAXROWS INTEGER",
 /*  97 */ "blocks ::= BLOCKS INTEGER",
 /*  98 */ "ctime ::= CTIME INTEGER",
 /*  99 */ "wal ::= WAL INTEGER",
 /* 100 */ "fsync ::= FSYNC INTEGER",
 /* 101 */ "comp ::= COMP INTEGER",
 /* 102 */ "prec ::= PRECISION STRING",
 /* 103 */ "update ::= UPDATE INTEGER",
 /* 104 */ "cachelast ::= CACHELAST INTEGER",
 /* 105 */ "partitions ::= PARTITIONS INTEGER",
 /* 106 */ "db_optr ::=",
 /* 107 */ "db_optr ::= db_optr cache",
 /* 108 */ "db_optr ::= db_optr replica",
 /* 109 */ "db_optr ::= db_optr quorum",
 /* 110 */ "db_optr ::= db_optr days",
 /* 111 */ "db_optr ::= db_optr minrows",
 /* 112 */ "db_optr ::= db_optr maxrows",
 /* 113 */ "db_optr ::= db_optr blocks",
 /* 114 */ "db_optr ::= db_optr ctime",
 /* 115 */ "db_optr ::= db_optr wal",
 /* 116 */ "db_optr ::= db_optr fsync",
 /* 117 */ "db_optr ::= db_optr comp",
 /* 118 */ "db_optr ::= db_optr prec",
 /* 119 */ "db_optr ::= db_optr keep",
 /* 120 */ "db_optr ::= db_optr update",
 /* 121 */ "db_optr ::= db_optr cachelast",
 /* 122 */ "topic_optr ::= db_optr",
 /* 123 */ "topic_optr ::= topic_optr partitions",
 /* 124 */ "alter_db_optr ::=",
 /* 125 */ "alter_db_optr ::= alter_db_optr replica",
 /* 126 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 127 */ "alter_db_optr ::= alter_db_optr keep",
 /* 128 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 129 */ "alter_db_optr ::= alter_db_optr comp",
 /* 130 */ "alter_db_optr ::= alter_db_optr update",
 /* 131 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 132 */ "alter_topic_optr ::= alter_db_optr",
 /* 133 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 134 */ "typename ::= ids",
 /* 135 */ "typename ::= ids LP signed RP",
 /* 136 */ "typename ::= ids UNSIGNED",
 /* 137 */ "signed ::= INTEGER",
 /* 138 */ "signed ::= PLUS INTEGER",
 /* 139 */ "signed ::= MINUS INTEGER",
 /* 140 */ "cmd ::= CREATE TABLE create_table_args",
 /* 141 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 142 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 143 */ "cmd ::= CREATE TABLE create_table_list",
 /* 144 */ "create_table_list ::= create_from_stable",
 /* 145 */ "create_table_list ::= create_table_list create_from_stable",
 /* 146 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 147 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 148 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 149 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 150 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 151 */ "tagNamelist ::= ids",
 /* 152 */ "create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select",
 /* 153 */ "to_opt ::=",
 /* 154 */ "to_opt ::= TO ids cpxName",
 /* 155 */ "split_opt ::=",
 /* 156 */ "split_opt ::= SPLIT ids",
 /* 157 */ "columnlist ::= columnlist COMMA column",
 /* 158 */ "columnlist ::= column",
 /* 159 */ "column ::= ids typename",
 /* 160 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 161 */ "tagitemlist ::= tagitem",
 /* 162 */ "tagitem ::= INTEGER",
 /* 163 */ "tagitem ::= FLOAT",
 /* 164 */ "tagitem ::= STRING",
 /* 165 */ "tagitem ::= BOOL",
 /* 166 */ "tagitem ::= NULL",
 /* 167 */ "tagitem ::= NOW",
 /* 168 */ "tagitem ::= NOW PLUS VARIABLE",
 /* 169 */ "tagitem ::= NOW MINUS VARIABLE",
 /* 170 */ "tagitem ::= MINUS INTEGER",
 /* 171 */ "tagitem ::= MINUS FLOAT",
 /* 172 */ "tagitem ::= PLUS INTEGER",
 /* 173 */ "tagitem ::= PLUS FLOAT",
 /* 174 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 175 */ "select ::= LP select RP",
 /* 176 */ "union ::= select",
 /* 177 */ "union ::= union UNION ALL select",
 /* 178 */ "cmd ::= union",
 /* 179 */ "select ::= SELECT selcollist",
 /* 180 */ "sclp ::= selcollist COMMA",
 /* 181 */ "sclp ::=",
 /* 182 */ "selcollist ::= sclp distinct expr as",
 /* 183 */ "selcollist ::= sclp STAR",
 /* 184 */ "as ::= AS ids",
 /* 185 */ "as ::= ids",
 /* 186 */ "as ::=",
 /* 187 */ "distinct ::= DISTINCT",
 /* 188 */ "distinct ::=",
 /* 189 */ "from ::= FROM tablelist",
 /* 190 */ "from ::= FROM sub",
 /* 191 */ "sub ::= LP union RP",
 /* 192 */ "sub ::= LP union RP ids",
 /* 193 */ "sub ::= sub COMMA LP union RP ids",
 /* 194 */ "tablelist ::= ids cpxName",
 /* 195 */ "tablelist ::= ids cpxName ids",
 /* 196 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 197 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 198 */ "tmvar ::= VARIABLE",
 /* 199 */ "timestamp ::= INTEGER",
 /* 200 */ "timestamp ::= MINUS INTEGER",
 /* 201 */ "timestamp ::= PLUS INTEGER",
 /* 202 */ "timestamp ::= STRING",
 /* 203 */ "timestamp ::= NOW",
 /* 204 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 205 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 206 */ "range_option ::=",
 /* 207 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 208 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 209 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 210 */ "interval_option ::=",
 /* 211 */ "intervalKey ::= INTERVAL",
 /* 212 */ "intervalKey ::= EVERY",
 /* 213 */ "session_option ::=",
 /* 214 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 215 */ "windowstate_option ::=",
 /* 216 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 217 */ "fill_opt ::=",
 /* 218 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 219 */ "fill_opt ::= FILL LP ID RP",
 /* 220 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 221 */ "sliding_opt ::=",
 /* 222 */ "orderby_opt ::=",
 /* 223 */ "orderby_opt ::= ORDER BY sortlist",
 /* 224 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 225 */ "sortlist ::= sortlist COMMA arrow sortorder",
 /* 226 */ "sortlist ::= item sortorder",
 /* 227 */ "sortlist ::= arrow sortorder",
 /* 228 */ "item ::= ID",
 /* 229 */ "item ::= ID DOT ID",
 /* 230 */ "sortorder ::= ASC",
 /* 231 */ "sortorder ::= DESC",
 /* 232 */ "sortorder ::=",
 /* 233 */ "groupby_opt ::=",
 /* 234 */ "groupby_opt ::= GROUP BY grouplist",
 /* 235 */ "grouplist ::= grouplist COMMA item",
 /* 236 */ "grouplist ::= grouplist COMMA arrow",
 /* 237 */ "grouplist ::= item",
 /* 238 */ "grouplist ::= arrow",
 /* 239 */ "having_opt ::=",
 /* 240 */ "having_opt ::= HAVING expr",
 /* 241 */ "limit_opt ::=",
 /* 242 */ "limit_opt ::= LIMIT signed",
 /* 243 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 244 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 245 */ "slimit_opt ::=",
 /* 246 */ "slimit_opt ::= SLIMIT signed",
 /* 247 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 248 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 249 */ "where_opt ::=",
 /* 250 */ "where_opt ::= WHERE expr",
 /* 251 */ "expr ::= LP expr RP",
 /* 252 */ "expr ::= ID",
 /* 253 */ "expr ::= ID DOT ID",
 /* 254 */ "expr ::= ID DOT STAR",
 /* 255 */ "expr ::= INTEGER",
 /* 256 */ "expr ::= MINUS INTEGER",
 /* 257 */ "expr ::= PLUS INTEGER",
 /* 258 */ "expr ::= FLOAT",
 /* 259 */ "expr ::= MINUS FLOAT",
 /* 260 */ "expr ::= PLUS FLOAT",
 /* 261 */ "expr ::= STRING",
 /* 262 */ "expr ::= NOW",
 /* 263 */ "expr ::= TODAY",
 /* 264 */ "expr ::= VARIABLE",
 /* 265 */ "expr ::= PLUS VARIABLE",
 /* 266 */ "expr ::= MINUS VARIABLE",
 /* 267 */ "expr ::= BOOL",
 /* 268 */ "expr ::= NULL",
 /* 269 */ "expr ::= ID LP exprlist RP",
 /* 270 */ "expr ::= ID LP STAR RP",
 /* 271 */ "expr ::= ID LP expr AS typename RP",
 /* 272 */ "expr ::= expr IS NULL",
 /* 273 */ "expr ::= expr IS NOT NULL",
 /* 274 */ "expr ::= expr LT expr",
 /* 275 */ "expr ::= expr GT expr",
 /* 276 */ "expr ::= expr LE expr",
 /* 277 */ "expr ::= expr GE expr",
 /* 278 */ "expr ::= expr NE expr",
 /* 279 */ "expr ::= expr EQ expr",
 /* 280 */ "expr ::= expr BETWEEN expr AND expr",
 /* 281 */ "expr ::= expr AND expr",
 /* 282 */ "expr ::= expr OR expr",
 /* 283 */ "expr ::= expr PLUS expr",
 /* 284 */ "expr ::= expr MINUS expr",
 /* 285 */ "expr ::= expr STAR expr",
 /* 286 */ "expr ::= expr SLASH expr",
 /* 287 */ "expr ::= expr REM expr",
 /* 288 */ "expr ::= expr BITAND expr",
 /* 289 */ "expr ::= expr LIKE expr",
 /* 290 */ "expr ::= expr MATCH expr",
 /* 291 */ "expr ::= expr NMATCH expr",
 /* 292 */ "expr ::= ID CONTAINS STRING",
 /* 293 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 294 */ "arrow ::= ID ARROW STRING",
 /* 295 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 296 */ "expr ::= arrow",
 /* 297 */ "expr ::= expr IN LP exprlist RP",
 /* 298 */ "exprlist ::= exprlist COMMA expritem",
 /* 299 */ "exprlist ::= expritem",
 /* 300 */ "expritem ::= expr",
 /* 301 */ "expritem ::=",
 /* 302 */ "cmd ::= RESET QUERY CACHE",
 /* 303 */ "cmd ::= SYNCDB ids REPLICA",
 /* 304 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 305 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 306 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 307 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 308 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 309 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 310 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 311 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 312 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 313 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 314 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 315 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 316 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 317 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 318 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 319 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 320 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 321 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 322 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
 /* 323 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
    case 216: /* exprlist */
    case 264: /* selcollist */
    case 279: /* sclp */
{
tSqlExprListDestroy((yypminor->yy249));
}
      break;
    case 232: /* intitemlist */
    case 234: /* keep */
    case 255: /* columnlist */
    case 256: /* tagitemlist */
    case 257: /* tagNamelist */
    case 272: /* fill_opt */
    case 273: /* groupby_opt */
    case 275: /* orderby_opt */
    case 288: /* sortlist */
    case 292: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy249));
}
      break;
    case 253: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy494));
}
      break;
    case 260: /* select */
{
destroySqlNode((yypminor->yy320));
}
      break;
    case 265: /* from */
    case 283: /* tablelist */
    case 284: /* sub */
{
destroyRelationInfo((yypminor->yy52));
}
      break;
    case 266: /* where_opt */
    case 274: /* having_opt */
    case 281: /* expr */
    case 286: /* timestamp */
    case 291: /* arrow */
    case 293: /* expritem */
{
tSqlExprDestroy((yypminor->yy370));
}
      break;
    case 278: /* union */
{
destroyAllSqlNode((yypminor->yy249));
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
  {  207,   -1 }, /* (0) program ::= cmd */
  {  208,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  208,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  208,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  208,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  208,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  208,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  208,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  208,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  208,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  208,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  208,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  208,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  208,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  208,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  208,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  208,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  210,    0 }, /* (17) dbPrefix ::= */
  {  210,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  211,    0 }, /* (19) cpxName ::= */
  {  211,   -2 }, /* (20) cpxName ::= DOT ids */
  {  208,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  208,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  208,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  208,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  208,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
  {  208,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  208,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
  {  208,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  208,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  208,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  208,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  208,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  208,   -3 }, /* (33) cmd ::= DROP FUNCTION ids */
  {  208,   -3 }, /* (34) cmd ::= DROP DNODE ids */
  {  208,   -3 }, /* (35) cmd ::= DROP USER ids */
  {  208,   -3 }, /* (36) cmd ::= DROP ACCOUNT ids */
  {  208,   -2 }, /* (37) cmd ::= USE ids */
  {  208,   -3 }, /* (38) cmd ::= DESCRIBE ids cpxName */
  {  208,   -3 }, /* (39) cmd ::= DESC ids cpxName */
  {  208,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  208,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  208,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  208,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  208,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  208,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  208,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  208,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  208,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  208,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  208,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  209,   -1 }, /* (51) ids ::= ID */
  {  209,   -1 }, /* (52) ids ::= STRING */
  {  212,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  212,    0 }, /* (54) ifexists ::= */
  {  217,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  217,    0 }, /* (56) ifnotexists ::= */
  {  208,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  208,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  208,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  208,   -5 }, /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  208,   -9 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
  {  208,  -10 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
  {  208,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  221,    0 }, /* (64) bufsize ::= */
  {  221,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  222,    0 }, /* (66) params ::= */
  {  222,   -2 }, /* (67) params ::= PARAMS INTEGER */
  {  223,    0 }, /* (68) pps ::= */
  {  223,   -2 }, /* (69) pps ::= PPS INTEGER */
  {  224,    0 }, /* (70) tseries ::= */
  {  224,   -2 }, /* (71) tseries ::= TSERIES INTEGER */
  {  225,    0 }, /* (72) dbs ::= */
  {  225,   -2 }, /* (73) dbs ::= DBS INTEGER */
  {  226,    0 }, /* (74) streams ::= */
  {  226,   -2 }, /* (75) streams ::= STREAMS INTEGER */
  {  227,    0 }, /* (76) storage ::= */
  {  227,   -2 }, /* (77) storage ::= STORAGE INTEGER */
  {  228,    0 }, /* (78) qtime ::= */
  {  228,   -2 }, /* (79) qtime ::= QTIME INTEGER */
  {  229,    0 }, /* (80) users ::= */
  {  229,   -2 }, /* (81) users ::= USERS INTEGER */
  {  230,    0 }, /* (82) conns ::= */
  {  230,   -2 }, /* (83) conns ::= CONNS INTEGER */
  {  231,    0 }, /* (84) state ::= */
  {  231,   -2 }, /* (85) state ::= STATE ids */
  {  215,   -9 }, /* (86) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  232,   -3 }, /* (87) intitemlist ::= intitemlist COMMA intitem */
  {  232,   -1 }, /* (88) intitemlist ::= intitem */
  {  233,   -1 }, /* (89) intitem ::= INTEGER */
  {  234,   -2 }, /* (90) keep ::= KEEP intitemlist */
  {  235,   -2 }, /* (91) cache ::= CACHE INTEGER */
  {  236,   -2 }, /* (92) replica ::= REPLICA INTEGER */
  {  237,   -2 }, /* (93) quorum ::= QUORUM INTEGER */
  {  238,   -2 }, /* (94) days ::= DAYS INTEGER */
  {  239,   -2 }, /* (95) minrows ::= MINROWS INTEGER */
  {  240,   -2 }, /* (96) maxrows ::= MAXROWS INTEGER */
  {  241,   -2 }, /* (97) blocks ::= BLOCKS INTEGER */
  {  242,   -2 }, /* (98) ctime ::= CTIME INTEGER */
  {  243,   -2 }, /* (99) wal ::= WAL INTEGER */
  {  244,   -2 }, /* (100) fsync ::= FSYNC INTEGER */
  {  245,   -2 }, /* (101) comp ::= COMP INTEGER */
  {  246,   -2 }, /* (102) prec ::= PRECISION STRING */
  {  247,   -2 }, /* (103) update ::= UPDATE INTEGER */
  {  248,   -2 }, /* (104) cachelast ::= CACHELAST INTEGER */
  {  249,   -2 }, /* (105) partitions ::= PARTITIONS INTEGER */
  {  218,    0 }, /* (106) db_optr ::= */
  {  218,   -2 }, /* (107) db_optr ::= db_optr cache */
  {  218,   -2 }, /* (108) db_optr ::= db_optr replica */
  {  218,   -2 }, /* (109) db_optr ::= db_optr quorum */
  {  218,   -2 }, /* (110) db_optr ::= db_optr days */
  {  218,   -2 }, /* (111) db_optr ::= db_optr minrows */
  {  218,   -2 }, /* (112) db_optr ::= db_optr maxrows */
  {  218,   -2 }, /* (113) db_optr ::= db_optr blocks */
  {  218,   -2 }, /* (114) db_optr ::= db_optr ctime */
  {  218,   -2 }, /* (115) db_optr ::= db_optr wal */
  {  218,   -2 }, /* (116) db_optr ::= db_optr fsync */
  {  218,   -2 }, /* (117) db_optr ::= db_optr comp */
  {  218,   -2 }, /* (118) db_optr ::= db_optr prec */
  {  218,   -2 }, /* (119) db_optr ::= db_optr keep */
  {  218,   -2 }, /* (120) db_optr ::= db_optr update */
  {  218,   -2 }, /* (121) db_optr ::= db_optr cachelast */
  {  219,   -1 }, /* (122) topic_optr ::= db_optr */
  {  219,   -2 }, /* (123) topic_optr ::= topic_optr partitions */
  {  213,    0 }, /* (124) alter_db_optr ::= */
  {  213,   -2 }, /* (125) alter_db_optr ::= alter_db_optr replica */
  {  213,   -2 }, /* (126) alter_db_optr ::= alter_db_optr quorum */
  {  213,   -2 }, /* (127) alter_db_optr ::= alter_db_optr keep */
  {  213,   -2 }, /* (128) alter_db_optr ::= alter_db_optr blocks */
  {  213,   -2 }, /* (129) alter_db_optr ::= alter_db_optr comp */
  {  213,   -2 }, /* (130) alter_db_optr ::= alter_db_optr update */
  {  213,   -2 }, /* (131) alter_db_optr ::= alter_db_optr cachelast */
  {  214,   -1 }, /* (132) alter_topic_optr ::= alter_db_optr */
  {  214,   -2 }, /* (133) alter_topic_optr ::= alter_topic_optr partitions */
  {  220,   -1 }, /* (134) typename ::= ids */
  {  220,   -4 }, /* (135) typename ::= ids LP signed RP */
  {  220,   -2 }, /* (136) typename ::= ids UNSIGNED */
  {  250,   -1 }, /* (137) signed ::= INTEGER */
  {  250,   -2 }, /* (138) signed ::= PLUS INTEGER */
  {  250,   -2 }, /* (139) signed ::= MINUS INTEGER */
  {  208,   -3 }, /* (140) cmd ::= CREATE TABLE create_table_args */
  {  208,   -3 }, /* (141) cmd ::= CREATE TABLE create_stable_args */
  {  208,   -3 }, /* (142) cmd ::= CREATE STABLE create_stable_args */
  {  208,   -3 }, /* (143) cmd ::= CREATE TABLE create_table_list */
  {  253,   -1 }, /* (144) create_table_list ::= create_from_stable */
  {  253,   -2 }, /* (145) create_table_list ::= create_table_list create_from_stable */
  {  251,   -6 }, /* (146) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  252,  -10 }, /* (147) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  254,  -10 }, /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  254,  -13 }, /* (149) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  257,   -3 }, /* (150) tagNamelist ::= tagNamelist COMMA ids */
  {  257,   -1 }, /* (151) tagNamelist ::= ids */
  {  251,   -7 }, /* (152) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  258,    0 }, /* (153) to_opt ::= */
  {  258,   -3 }, /* (154) to_opt ::= TO ids cpxName */
  {  259,    0 }, /* (155) split_opt ::= */
  {  259,   -2 }, /* (156) split_opt ::= SPLIT ids */
  {  255,   -3 }, /* (157) columnlist ::= columnlist COMMA column */
  {  255,   -1 }, /* (158) columnlist ::= column */
  {  262,   -2 }, /* (159) column ::= ids typename */
  {  256,   -3 }, /* (160) tagitemlist ::= tagitemlist COMMA tagitem */
  {  256,   -1 }, /* (161) tagitemlist ::= tagitem */
  {  263,   -1 }, /* (162) tagitem ::= INTEGER */
  {  263,   -1 }, /* (163) tagitem ::= FLOAT */
  {  263,   -1 }, /* (164) tagitem ::= STRING */
  {  263,   -1 }, /* (165) tagitem ::= BOOL */
  {  263,   -1 }, /* (166) tagitem ::= NULL */
  {  263,   -1 }, /* (167) tagitem ::= NOW */
  {  263,   -3 }, /* (168) tagitem ::= NOW PLUS VARIABLE */
  {  263,   -3 }, /* (169) tagitem ::= NOW MINUS VARIABLE */
  {  263,   -2 }, /* (170) tagitem ::= MINUS INTEGER */
  {  263,   -2 }, /* (171) tagitem ::= MINUS FLOAT */
  {  263,   -2 }, /* (172) tagitem ::= PLUS INTEGER */
  {  263,   -2 }, /* (173) tagitem ::= PLUS FLOAT */
  {  260,  -15 }, /* (174) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  260,   -3 }, /* (175) select ::= LP select RP */
  {  278,   -1 }, /* (176) union ::= select */
  {  278,   -4 }, /* (177) union ::= union UNION ALL select */
  {  208,   -1 }, /* (178) cmd ::= union */
  {  260,   -2 }, /* (179) select ::= SELECT selcollist */
  {  279,   -2 }, /* (180) sclp ::= selcollist COMMA */
  {  279,    0 }, /* (181) sclp ::= */
  {  264,   -4 }, /* (182) selcollist ::= sclp distinct expr as */
  {  264,   -2 }, /* (183) selcollist ::= sclp STAR */
  {  282,   -2 }, /* (184) as ::= AS ids */
  {  282,   -1 }, /* (185) as ::= ids */
  {  282,    0 }, /* (186) as ::= */
  {  280,   -1 }, /* (187) distinct ::= DISTINCT */
  {  280,    0 }, /* (188) distinct ::= */
  {  265,   -2 }, /* (189) from ::= FROM tablelist */
  {  265,   -2 }, /* (190) from ::= FROM sub */
  {  284,   -3 }, /* (191) sub ::= LP union RP */
  {  284,   -4 }, /* (192) sub ::= LP union RP ids */
  {  284,   -6 }, /* (193) sub ::= sub COMMA LP union RP ids */
  {  283,   -2 }, /* (194) tablelist ::= ids cpxName */
  {  283,   -3 }, /* (195) tablelist ::= ids cpxName ids */
  {  283,   -4 }, /* (196) tablelist ::= tablelist COMMA ids cpxName */
  {  283,   -5 }, /* (197) tablelist ::= tablelist COMMA ids cpxName ids */
  {  285,   -1 }, /* (198) tmvar ::= VARIABLE */
  {  286,   -1 }, /* (199) timestamp ::= INTEGER */
  {  286,   -2 }, /* (200) timestamp ::= MINUS INTEGER */
  {  286,   -2 }, /* (201) timestamp ::= PLUS INTEGER */
  {  286,   -1 }, /* (202) timestamp ::= STRING */
  {  286,   -1 }, /* (203) timestamp ::= NOW */
  {  286,   -3 }, /* (204) timestamp ::= NOW PLUS VARIABLE */
  {  286,   -3 }, /* (205) timestamp ::= NOW MINUS VARIABLE */
  {  267,    0 }, /* (206) range_option ::= */
  {  267,   -6 }, /* (207) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  268,   -4 }, /* (208) interval_option ::= intervalKey LP tmvar RP */
  {  268,   -6 }, /* (209) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  268,    0 }, /* (210) interval_option ::= */
  {  287,   -1 }, /* (211) intervalKey ::= INTERVAL */
  {  287,   -1 }, /* (212) intervalKey ::= EVERY */
  {  270,    0 }, /* (213) session_option ::= */
  {  270,   -7 }, /* (214) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  271,    0 }, /* (215) windowstate_option ::= */
  {  271,   -4 }, /* (216) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  272,    0 }, /* (217) fill_opt ::= */
  {  272,   -6 }, /* (218) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  272,   -4 }, /* (219) fill_opt ::= FILL LP ID RP */
  {  269,   -4 }, /* (220) sliding_opt ::= SLIDING LP tmvar RP */
  {  269,    0 }, /* (221) sliding_opt ::= */
  {  275,    0 }, /* (222) orderby_opt ::= */
  {  275,   -3 }, /* (223) orderby_opt ::= ORDER BY sortlist */
  {  288,   -4 }, /* (224) sortlist ::= sortlist COMMA item sortorder */
  {  288,   -4 }, /* (225) sortlist ::= sortlist COMMA arrow sortorder */
  {  288,   -2 }, /* (226) sortlist ::= item sortorder */
  {  288,   -2 }, /* (227) sortlist ::= arrow sortorder */
  {  289,   -1 }, /* (228) item ::= ID */
  {  289,   -3 }, /* (229) item ::= ID DOT ID */
  {  290,   -1 }, /* (230) sortorder ::= ASC */
  {  290,   -1 }, /* (231) sortorder ::= DESC */
  {  290,    0 }, /* (232) sortorder ::= */
  {  273,    0 }, /* (233) groupby_opt ::= */
  {  273,   -3 }, /* (234) groupby_opt ::= GROUP BY grouplist */
  {  292,   -3 }, /* (235) grouplist ::= grouplist COMMA item */
  {  292,   -3 }, /* (236) grouplist ::= grouplist COMMA arrow */
  {  292,   -1 }, /* (237) grouplist ::= item */
  {  292,   -1 }, /* (238) grouplist ::= arrow */
  {  274,    0 }, /* (239) having_opt ::= */
  {  274,   -2 }, /* (240) having_opt ::= HAVING expr */
  {  277,    0 }, /* (241) limit_opt ::= */
  {  277,   -2 }, /* (242) limit_opt ::= LIMIT signed */
  {  277,   -4 }, /* (243) limit_opt ::= LIMIT signed OFFSET signed */
  {  277,   -4 }, /* (244) limit_opt ::= LIMIT signed COMMA signed */
  {  276,    0 }, /* (245) slimit_opt ::= */
  {  276,   -2 }, /* (246) slimit_opt ::= SLIMIT signed */
  {  276,   -4 }, /* (247) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  276,   -4 }, /* (248) slimit_opt ::= SLIMIT signed COMMA signed */
  {  266,    0 }, /* (249) where_opt ::= */
  {  266,   -2 }, /* (250) where_opt ::= WHERE expr */
  {  281,   -3 }, /* (251) expr ::= LP expr RP */
  {  281,   -1 }, /* (252) expr ::= ID */
  {  281,   -3 }, /* (253) expr ::= ID DOT ID */
  {  281,   -3 }, /* (254) expr ::= ID DOT STAR */
  {  281,   -1 }, /* (255) expr ::= INTEGER */
  {  281,   -2 }, /* (256) expr ::= MINUS INTEGER */
  {  281,   -2 }, /* (257) expr ::= PLUS INTEGER */
  {  281,   -1 }, /* (258) expr ::= FLOAT */
  {  281,   -2 }, /* (259) expr ::= MINUS FLOAT */
  {  281,   -2 }, /* (260) expr ::= PLUS FLOAT */
  {  281,   -1 }, /* (261) expr ::= STRING */
  {  281,   -1 }, /* (262) expr ::= NOW */
  {  281,   -1 }, /* (263) expr ::= TODAY */
  {  281,   -1 }, /* (264) expr ::= VARIABLE */
  {  281,   -2 }, /* (265) expr ::= PLUS VARIABLE */
  {  281,   -2 }, /* (266) expr ::= MINUS VARIABLE */
  {  281,   -1 }, /* (267) expr ::= BOOL */
  {  281,   -1 }, /* (268) expr ::= NULL */
  {  281,   -4 }, /* (269) expr ::= ID LP exprlist RP */
  {  281,   -4 }, /* (270) expr ::= ID LP STAR RP */
  {  281,   -6 }, /* (271) expr ::= ID LP expr AS typename RP */
  {  281,   -3 }, /* (272) expr ::= expr IS NULL */
  {  281,   -4 }, /* (273) expr ::= expr IS NOT NULL */
  {  281,   -3 }, /* (274) expr ::= expr LT expr */
  {  281,   -3 }, /* (275) expr ::= expr GT expr */
  {  281,   -3 }, /* (276) expr ::= expr LE expr */
  {  281,   -3 }, /* (277) expr ::= expr GE expr */
  {  281,   -3 }, /* (278) expr ::= expr NE expr */
  {  281,   -3 }, /* (279) expr ::= expr EQ expr */
  {  281,   -5 }, /* (280) expr ::= expr BETWEEN expr AND expr */
  {  281,   -3 }, /* (281) expr ::= expr AND expr */
  {  281,   -3 }, /* (282) expr ::= expr OR expr */
  {  281,   -3 }, /* (283) expr ::= expr PLUS expr */
  {  281,   -3 }, /* (284) expr ::= expr MINUS expr */
  {  281,   -3 }, /* (285) expr ::= expr STAR expr */
  {  281,   -3 }, /* (286) expr ::= expr SLASH expr */
  {  281,   -3 }, /* (287) expr ::= expr REM expr */
  {  281,   -3 }, /* (288) expr ::= expr BITAND expr */
  {  281,   -3 }, /* (289) expr ::= expr LIKE expr */
  {  281,   -3 }, /* (290) expr ::= expr MATCH expr */
  {  281,   -3 }, /* (291) expr ::= expr NMATCH expr */
  {  281,   -3 }, /* (292) expr ::= ID CONTAINS STRING */
  {  281,   -5 }, /* (293) expr ::= ID DOT ID CONTAINS STRING */
  {  291,   -3 }, /* (294) arrow ::= ID ARROW STRING */
  {  291,   -5 }, /* (295) arrow ::= ID DOT ID ARROW STRING */
  {  281,   -1 }, /* (296) expr ::= arrow */
  {  281,   -5 }, /* (297) expr ::= expr IN LP exprlist RP */
  {  216,   -3 }, /* (298) exprlist ::= exprlist COMMA expritem */
  {  216,   -1 }, /* (299) exprlist ::= expritem */
  {  293,   -1 }, /* (300) expritem ::= expr */
  {  293,    0 }, /* (301) expritem ::= */
  {  208,   -3 }, /* (302) cmd ::= RESET QUERY CACHE */
  {  208,   -3 }, /* (303) cmd ::= SYNCDB ids REPLICA */
  {  208,   -7 }, /* (304) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (305) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (306) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (307) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (308) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (309) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (310) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (311) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -7 }, /* (312) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (313) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (314) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (315) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (316) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (317) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (318) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (319) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -3 }, /* (320) cmd ::= KILL CONNECTION INTEGER */
  {  208,   -5 }, /* (321) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  208,   -5 }, /* (322) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  208,   -6 }, /* (323) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
      case 140: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==140);
      case 141: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==141);
      case 142: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==142);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNCTION, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 8: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 10: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 11: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 13: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 15: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 16: /* cmd ::= SHOW VNODES ids */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 17: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 18: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 20: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 21: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE STRING */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE STRING */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
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
      case 33: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 34: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 35: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 36: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 37: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 38: /* cmd ::= DESCRIBE ids cpxName */
      case 39: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==39);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 40: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 41: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 44: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 46: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 47: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==47);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy547);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy547);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy249);}
        break;
      case 51: /* ids ::= ID */
      case 52: /* ids ::= STRING */ yytestcase(yyruleno==52);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 53: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 54: /* ifexists ::= */
      case 56: /* ifnotexists ::= */ yytestcase(yyruleno==56);
      case 188: /* distinct ::= */ yytestcase(yyruleno==188);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy547);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-6].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy475, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-6].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy475, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 2);}
        break;
      case 63: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 64: /* bufsize ::= */
      case 66: /* params ::= */ yytestcase(yyruleno==66);
      case 68: /* pps ::= */ yytestcase(yyruleno==68);
      case 70: /* tseries ::= */ yytestcase(yyruleno==70);
      case 72: /* dbs ::= */ yytestcase(yyruleno==72);
      case 74: /* streams ::= */ yytestcase(yyruleno==74);
      case 76: /* storage ::= */ yytestcase(yyruleno==76);
      case 78: /* qtime ::= */ yytestcase(yyruleno==78);
      case 80: /* users ::= */ yytestcase(yyruleno==80);
      case 82: /* conns ::= */ yytestcase(yyruleno==82);
      case 84: /* state ::= */ yytestcase(yyruleno==84);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 65: /* bufsize ::= BUFSIZE INTEGER */
      case 67: /* params ::= PARAMS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==69);
      case 71: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==71);
      case 73: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==75);
      case 77: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==77);
      case 79: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==79);
      case 81: /* users ::= USERS INTEGER */ yytestcase(yyruleno==81);
      case 83: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==83);
      case 85: /* state ::= STATE ids */ yytestcase(yyruleno==85);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 86: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy547.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy547.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy547.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy547.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy547.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy547.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy547.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy547.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy547.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy547 = yylhsminor.yy547;
        break;
      case 87: /* intitemlist ::= intitemlist COMMA intitem */
      case 160: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==160);
{ yylhsminor.yy249 = tVariantListAppend(yymsp[-2].minor.yy249, &yymsp[0].minor.yy134, -1);    }
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 88: /* intitemlist ::= intitem */
      case 161: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==161);
{ yylhsminor.yy249 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1); }
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 89: /* intitem ::= INTEGER */
      case 162: /* tagitem ::= INTEGER */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= STRING */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= BOOL */ yytestcase(yyruleno==165);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 90: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy249 = yymsp[0].minor.yy249; }
        break;
      case 91: /* cache ::= CACHE INTEGER */
      case 92: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==92);
      case 93: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==93);
      case 94: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==96);
      case 97: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==97);
      case 98: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==98);
      case 99: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==99);
      case 100: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==100);
      case 101: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==101);
      case 102: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==102);
      case 103: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==103);
      case 104: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==104);
      case 105: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==105);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 106: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy478); yymsp[1].minor.yy478.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 107: /* db_optr ::= db_optr cache */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 108: /* db_optr ::= db_optr replica */
      case 125: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==125);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 109: /* db_optr ::= db_optr quorum */
      case 126: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==126);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 110: /* db_optr ::= db_optr days */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 111: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 112: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 113: /* db_optr ::= db_optr blocks */
      case 128: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==128);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 114: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 115: /* db_optr ::= db_optr wal */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 116: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 117: /* db_optr ::= db_optr comp */
      case 129: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==129);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 118: /* db_optr ::= db_optr prec */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 119: /* db_optr ::= db_optr keep */
      case 127: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==127);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.keep = yymsp[0].minor.yy249; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 120: /* db_optr ::= db_optr update */
      case 130: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==130);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 121: /* db_optr ::= db_optr cachelast */
      case 131: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==131);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 122: /* topic_optr ::= db_optr */
      case 132: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==132);
{ yylhsminor.yy478 = yymsp[0].minor.yy478; yylhsminor.yy478.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy478 = yylhsminor.yy478;
        break;
      case 123: /* topic_optr ::= topic_optr partitions */
      case 133: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==133);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 124: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy478); yymsp[1].minor.yy478.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 134: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy475, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy475 = yylhsminor.yy475;
        break;
      case 135: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy165 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy475, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy165;  // negative value of name length
    tSetColumnType(&yylhsminor.yy475, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy475 = yylhsminor.yy475;
        break;
      case 136: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy475, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy475 = yylhsminor.yy475;
        break;
      case 137: /* signed ::= INTEGER */
{ yylhsminor.yy165 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 138: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy165 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 139: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy165 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 143: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy494;}
        break;
      case 144: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy192);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy494 = pCreateTable;
}
  yymsp[0].minor.yy494 = yylhsminor.yy494;
        break;
      case 145: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy494->childTableInfo, &yymsp[0].minor.yy192);
  yylhsminor.yy494 = yymsp[-1].minor.yy494;
}
  yymsp[-1].minor.yy494 = yylhsminor.yy494;
        break;
      case 146: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy494 = tSetCreateTableInfo(yymsp[-1].minor.yy249, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy494, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy494 = yylhsminor.yy494;
        break;
      case 147: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy494 = tSetCreateTableInfo(yymsp[-5].minor.yy249, yymsp[-1].minor.yy249, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy494, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy494 = yylhsminor.yy494;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy249, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy192 = yylhsminor.yy192;
        break;
      case 149: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy249, yymsp[-1].minor.yy249, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy192 = yylhsminor.yy192;
        break;
      case 150: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy249, &yymsp[0].minor.yy0); yylhsminor.yy249 = yymsp[-2].minor.yy249;  }
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 151: /* tagNamelist ::= ids */
{yylhsminor.yy249 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy249, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 152: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy494 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy320, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy494, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy494 = yylhsminor.yy494;
        break;
      case 153: /* to_opt ::= */
      case 155: /* split_opt ::= */ yytestcase(yyruleno==155);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 154: /* to_opt ::= TO ids cpxName */
{
   yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
   yymsp[-2].minor.yy0.n += yymsp[0].minor.yy0.n;
}
        break;
      case 156: /* split_opt ::= SPLIT ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 157: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy249, &yymsp[0].minor.yy475); yylhsminor.yy249 = yymsp[-2].minor.yy249;  }
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 158: /* columnlist ::= column */
{yylhsminor.yy249 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy249, &yymsp[0].minor.yy475);}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 159: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy475, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy475);
}
  yymsp[-1].minor.yy475 = yylhsminor.yy475;
        break;
      case 166: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 167: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy134, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 168: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy134, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 169: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy134, &yymsp[0].minor.yy0, TK_MINUS, true);
}
        break;
      case 170: /* tagitem ::= MINUS INTEGER */
      case 171: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==171);
      case 172: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==172);
      case 173: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==173);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy134, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 174: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy320 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy249, yymsp[-12].minor.yy52, yymsp[-11].minor.yy370, yymsp[-4].minor.yy249, yymsp[-2].minor.yy249, &yymsp[-9].minor.yy196, &yymsp[-7].minor.yy559, &yymsp[-6].minor.yy385, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy249, &yymsp[0].minor.yy342, &yymsp[-1].minor.yy342, yymsp[-3].minor.yy370, &yymsp[-10].minor.yy384);
}
  yymsp[-14].minor.yy320 = yylhsminor.yy320;
        break;
      case 175: /* select ::= LP select RP */
{yymsp[-2].minor.yy320 = yymsp[-1].minor.yy320;}
        break;
      case 176: /* union ::= select */
{ yylhsminor.yy249 = setSubclause(NULL, yymsp[0].minor.yy320); }
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 177: /* union ::= union UNION ALL select */
{ yylhsminor.yy249 = appendSelectClause(yymsp[-3].minor.yy249, yymsp[0].minor.yy320); }
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 178: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy249, NULL, TSDB_SQL_SELECT); }
        break;
      case 179: /* select ::= SELECT selcollist */
{
  yylhsminor.yy320 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy249, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy320 = yylhsminor.yy320;
        break;
      case 180: /* sclp ::= selcollist COMMA */
{yylhsminor.yy249 = yymsp[-1].minor.yy249;}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 181: /* sclp ::= */
      case 222: /* orderby_opt ::= */ yytestcase(yyruleno==222);
{yymsp[1].minor.yy249 = 0;}
        break;
      case 182: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy249 = tSqlExprListAppend(yymsp[-3].minor.yy249, yymsp[-1].minor.yy370,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 183: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy249 = tSqlExprListAppend(yymsp[-1].minor.yy249, pNode, 0, 0);
}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 184: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 185: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 186: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 187: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 189: /* from ::= FROM tablelist */
      case 190: /* from ::= FROM sub */ yytestcase(yyruleno==190);
{yymsp[-1].minor.yy52 = yymsp[0].minor.yy52;}
        break;
      case 191: /* sub ::= LP union RP */
{yymsp[-2].minor.yy52 = addSubqueryElem(NULL, yymsp[-1].minor.yy249, NULL);}
        break;
      case 192: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy52 = addSubqueryElem(NULL, yymsp[-2].minor.yy249, &yymsp[0].minor.yy0);}
        break;
      case 193: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy52 = addSubqueryElem(yymsp[-5].minor.yy52, yymsp[-2].minor.yy249, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy52 = yylhsminor.yy52;
        break;
      case 194: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy52 = yylhsminor.yy52;
        break;
      case 195: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy52 = yylhsminor.yy52;
        break;
      case 196: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(yymsp[-3].minor.yy52, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy52 = yylhsminor.yy52;
        break;
      case 197: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(yymsp[-4].minor.yy52, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy52 = yylhsminor.yy52;
        break;
      case 198: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 199: /* timestamp ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 200: /* timestamp ::= MINUS INTEGER */
      case 201: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==201);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 202: /* timestamp ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 203: /* timestamp ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 204: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 205: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 206: /* range_option ::= */
{yymsp[1].minor.yy384.start = 0; yymsp[1].minor.yy384.end = 0;}
        break;
      case 207: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy384.start = yymsp[-3].minor.yy370; yymsp[-5].minor.yy384.end = yymsp[-1].minor.yy370;}
        break;
      case 208: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy196.interval = yymsp[-1].minor.yy0; yylhsminor.yy196.offset.n = 0; yylhsminor.yy196.token = yymsp[-3].minor.yy88;}
  yymsp[-3].minor.yy196 = yylhsminor.yy196;
        break;
      case 209: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy196.interval = yymsp[-3].minor.yy0; yylhsminor.yy196.offset = yymsp[-1].minor.yy0;   yylhsminor.yy196.token = yymsp[-5].minor.yy88;}
  yymsp[-5].minor.yy196 = yylhsminor.yy196;
        break;
      case 210: /* interval_option ::= */
{memset(&yymsp[1].minor.yy196, 0, sizeof(yymsp[1].minor.yy196));}
        break;
      case 211: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy88 = TK_INTERVAL;}
        break;
      case 212: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy88 = TK_EVERY;   }
        break;
      case 213: /* session_option ::= */
{yymsp[1].minor.yy559.col.n = 0; yymsp[1].minor.yy559.gap.n = 0;}
        break;
      case 214: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy559.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy559.gap = yymsp[-1].minor.yy0;
}
        break;
      case 215: /* windowstate_option ::= */
{ yymsp[1].minor.yy385.col.n = 0; yymsp[1].minor.yy385.col.z = NULL;}
        break;
      case 216: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy385.col = yymsp[-1].minor.yy0; }
        break;
      case 217: /* fill_opt ::= */
{ yymsp[1].minor.yy249 = 0;     }
        break;
      case 218: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy249, &A, -1, 0);
    yymsp[-5].minor.yy249 = yymsp[-1].minor.yy249;
}
        break;
      case 219: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy249 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 220: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 221: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 223: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy249 = yymsp[0].minor.yy249;}
        break;
      case 224: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-3].minor.yy249, &yymsp[-1].minor.yy134, NULL, false, yymsp[0].minor.yy424);
}
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 225: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-3].minor.yy249, NULL, yymsp[-1].minor.yy370, true, yymsp[0].minor.yy424);
}
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 226: /* sortlist ::= item sortorder */
{
  yylhsminor.yy249 = commonItemAppend(NULL, &yymsp[-1].minor.yy134, NULL, false, yymsp[0].minor.yy424);
}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 227: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy249 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy370, true, yymsp[0].minor.yy424);
}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 228: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 229: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy134, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 230: /* sortorder ::= ASC */
{ yymsp[0].minor.yy424 = TSDB_ORDER_ASC; }
        break;
      case 231: /* sortorder ::= DESC */
{ yymsp[0].minor.yy424 = TSDB_ORDER_DESC;}
        break;
      case 232: /* sortorder ::= */
{ yymsp[1].minor.yy424 = TSDB_ORDER_ASC; }
        break;
      case 233: /* groupby_opt ::= */
{ yymsp[1].minor.yy249 = 0;}
        break;
      case 234: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy249 = yymsp[0].minor.yy249;}
        break;
      case 235: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-2].minor.yy249, &yymsp[0].minor.yy134, NULL, false, -1);
}
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 236: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-2].minor.yy249, NULL, yymsp[0].minor.yy370, true, -1);
}
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 237: /* grouplist ::= item */
{
  yylhsminor.yy249 = commonItemAppend(NULL, &yymsp[0].minor.yy134, NULL, false, -1);
}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 238: /* grouplist ::= arrow */
{
  yylhsminor.yy249 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy370, true, -1);
}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 239: /* having_opt ::= */
      case 249: /* where_opt ::= */ yytestcase(yyruleno==249);
      case 301: /* expritem ::= */ yytestcase(yyruleno==301);
{yymsp[1].minor.yy370 = 0;}
        break;
      case 240: /* having_opt ::= HAVING expr */
      case 250: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==250);
{yymsp[-1].minor.yy370 = yymsp[0].minor.yy370;}
        break;
      case 241: /* limit_opt ::= */
      case 245: /* slimit_opt ::= */ yytestcase(yyruleno==245);
{yymsp[1].minor.yy342.limit = -1; yymsp[1].minor.yy342.offset = 0;}
        break;
      case 242: /* limit_opt ::= LIMIT signed */
      case 246: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==246);
{yymsp[-1].minor.yy342.limit = yymsp[0].minor.yy165;  yymsp[-1].minor.yy342.offset = 0;}
        break;
      case 243: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy342.limit = yymsp[-2].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[0].minor.yy165;}
        break;
      case 244: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy342.limit = yymsp[0].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[-2].minor.yy165;}
        break;
      case 247: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy342.limit = yymsp[-2].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[0].minor.yy165;}
        break;
      case 248: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy342.limit = yymsp[0].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[-2].minor.yy165;}
        break;
      case 251: /* expr ::= LP expr RP */
{yylhsminor.yy370 = yymsp[-1].minor.yy370; yylhsminor.yy370->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy370->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 252: /* expr ::= ID */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 253: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 254: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 255: /* expr ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 256: /* expr ::= MINUS INTEGER */
      case 257: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==257);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 258: /* expr ::= FLOAT */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 259: /* expr ::= MINUS FLOAT */
      case 260: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==260);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 261: /* expr ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 262: /* expr ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 263: /* expr ::= TODAY */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 264: /* expr ::= VARIABLE */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 265: /* expr ::= PLUS VARIABLE */
      case 266: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==266);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 267: /* expr ::= BOOL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 268: /* expr ::= NULL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 269: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(yymsp[-1].minor.yy249, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 270: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 271: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy370, &yymsp[-1].minor.yy475, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy370 = yylhsminor.yy370;
        break;
      case 272: /* expr ::= expr IS NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 273: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-3].minor.yy370, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 274: /* expr ::= expr LT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 275: /* expr ::= expr GT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 276: /* expr ::= expr LE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 277: /* expr ::= expr GE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 278: /* expr ::= expr NE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 279: /* expr ::= expr EQ expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_EQ);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 280: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy370); yylhsminor.yy370 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy370, yymsp[-2].minor.yy370, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy370, TK_LE), TK_AND);}
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 281: /* expr ::= expr AND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_AND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 282: /* expr ::= expr OR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_OR); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 283: /* expr ::= expr PLUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_PLUS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 284: /* expr ::= expr MINUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MINUS); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 285: /* expr ::= expr STAR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_STAR);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 286: /* expr ::= expr SLASH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_DIVIDE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 287: /* expr ::= expr REM expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_REM);   }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 288: /* expr ::= expr BITAND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_BITAND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 289: /* expr ::= expr LIKE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LIKE);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 290: /* expr ::= expr MATCH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MATCH);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 291: /* expr ::= expr NMATCH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NMATCH);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 292: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 293: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 294: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 295: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 296: /* expr ::= arrow */
      case 300: /* expritem ::= expr */ yytestcase(yyruleno==300);
{yylhsminor.yy370 = yymsp[0].minor.yy370;}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 297: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-4].minor.yy370, (tSqlExpr*)yymsp[-1].minor.yy249, TK_IN); }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 298: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy249 = tSqlExprListAppend(yymsp[-2].minor.yy249,yymsp[0].minor.yy370,0, 0);}
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 299: /* exprlist ::= expritem */
{yylhsminor.yy249 = tSqlExprListAppend(0,yymsp[0].minor.yy370,0, 0);}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 302: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 303: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 306: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 307: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, false);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, false);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 318: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 321: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 322: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      case 323: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  SDelData * pDelData = tGetDelData(&yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0, yymsp[0].minor.yy370);
  setSqlInfo(pInfo, pDelData, NULL, TSDB_SQL_DELETE_DATA);
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
