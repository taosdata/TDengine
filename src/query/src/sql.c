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
#define YYNSTATE             418
#define YYNRULE              332
#define YYNTOKEN             207
#define YY_MAX_SHIFT         417
#define YY_MIN_SHIFTREDUCE   651
#define YY_MAX_SHIFTREDUCE   982
#define YY_ERROR_ACTION      983
#define YY_ACCEPT_ACTION     984
#define YY_NO_ACTION         985
#define YY_MIN_REDUCE        986
#define YY_MAX_REDUCE        1317
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
#define YY_ACTTAB_COUNT (944)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   230,  705,  293,  175, 1226,   65, 1227,  332,  705,  706,
 /*    10 */  1290,  270, 1292,  197,   43,   44,  706,   47,   48,  416,
 /*    20 */   255,  283,   32,   31,   30, 1140,   65,   46,  365,   51,
 /*    30 */    49,   52,   50,   37,   36,   35,   34,   33,   42,   41,
 /*    40 */   742,   24,   40,   39,   38,   43,   44, 1174,   47,   48,
 /*    50 */   263, 1290,  283,   32,   31,   30,  314, 1147,   46,  365,
 /*    60 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*    70 */    41,  273,  276,   40,   39,   38,  313,  312, 1147,   40,
 /*    80 */    39,   38,   43,   44, 1149,   47,   48,  409, 1076,  283,
 /*    90 */    32,   31,   30,  361,   95,   46,  365,   51,   49,   52,
 /*   100 */    50,   37,   36,   35,   34,   33,   42,   41,  228,  400,
 /*   110 */    40,   39,   38,   43,   44, 1312,   47,   48, 1290, 1171,
 /*   120 */   283,   32,   31,   30, 1132,   64,   46,  365,   51,   49,
 /*   130 */    52,   50,   37,   36,   35,   34,   33,   42,   41,  190,
 /*   140 */   791,   40,   39,   38,  911,  268,  914,   43,   45,  853,
 /*   150 */    47,   48, 1150,  856,  283,   32,   31,   30,  322,  905,
 /*   160 */    46,  365,   51,   49,   52,   50,   37,   36,   35,   34,
 /*   170 */    33,   42,   41,  390,  389,   40,   39,   38,   44,  229,
 /*   180 */    47,   48,  318,  319,  283,   32,   31,   30,   82, 1290,
 /*   190 */    46,  365,   51,   49,   52,   50,   37,   36,   35,   34,
 /*   200 */    33,   42,   41,  705, 1304,   40,   39,   38,   47,   48,
 /*   210 */   275,  706,  283,   32,   31,   30,  361, 1150,   46,  365,
 /*   220 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*   230 */    41,  101,   83,   40,   39,   38,   73,  359,  408,  407,
 /*   240 */   358,  406,  357,  405,  356,  355,  354,  404,  353,  403,
 /*   250 */   402,  315,  652,  653,  654,  655,  656,  657,  658,  659,
 /*   260 */   660,  661,  662,  663,  664,  665,  169,  919,  262,  286,
 /*   270 */    74,  266,   13,  984,  417,   25,  411, 1107, 1095, 1096,
 /*   280 */  1097, 1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105, 1106,
 /*   290 */  1108, 1109,  244, 1165,  254,  921,  705,  115,  909, 1237,
 /*   300 */   912,  246,  915,  298,  706,  114,   93,  156,  155,  154,
 /*   310 */   245,  264,  302,  301,  292,  373,  101,   37,   36,   35,
 /*   320 */    34,   33,   42,   41,  112, 1165,   40,   39,   38,    3,
 /*   330 */   212,  259,  260, 1134,   73,  367,  408,  407, 1165,  406,
 /*   340 */    29,  405, 1280,  306, 1224,  404, 1225,  403,  402,  287,
 /*   350 */  1236,  285, 1290,  376,  375,   74,  265, 1129, 1130,   61,
 /*   360 */  1133,   51,   49,   52,   50,   37,   36,   35,   34,   33,
 /*   370 */    42,   41,  865,  866,   40,   39,   38,  293,  305,  819,
 /*   380 */    91,  293,  816,   53,  817,   57,  818,  256,  198,  254,
 /*   390 */   921,  295,  366,  909,  294,  912,  291,  915,  385,  384,
 /*   400 */     5,   68,  201,   65,  234,  415,  413,  679,  680,  200,
 /*   410 */   122,  127,  118,  126, 1290,  288,  289,  364,  235,  922,
 /*   420 */   916,  918,  910,  280,  913,  141,  259,  260, 1290,  349,
 /*   430 */    92,  279,  284,  236, 1115,   29, 1113, 1114,  400, 1120,
 /*   440 */   363, 1116,  277, 1290,  917, 1117,  271, 1118, 1119, 1150,
 /*   450 */   139,  133,  144,  336,  107, 1146,  106,  282, 1233,  143,
 /*   460 */   247,  149,  153,  142, 1131,   65,   65,   65,   42,   41,
 /*   470 */  1290,  146,   40,   39,   38,   65,   65,   65,   53,  221,
 /*   480 */   219,  217,    6, 1232,  248,   65,   65,  226,  216,  160,
 /*   490 */   159,  158,  157,  272, 1290,  230,  230, 1290,   65, 1293,
 /*   500 */   168,  166,  165,  820,  290, 1290, 1290, 1293, 1293,  837,
 /*   510 */   274,  377,  378,  391,  922,  916,  918, 1147, 1147, 1147,
 /*   520 */   379,  380,  386,  152,  151,  150,  249, 1147, 1147, 1147,
 /*   530 */   387,  388,  250, 1037,  251,  293, 1290, 1147, 1147,  917,
 /*   540 */   211, 1276, 1290,  392, 1290,  920, 1148,    1,  199, 1275,
 /*   550 */  1147, 1290,  885, 1274,  257,  258,   98,  370,  232, 1290,
 /*   560 */   233,  237,  231, 1290, 1290, 1290,  238,  239, 1290,  834,
 /*   570 */  1290, 1290, 1290,  241,   99,   85, 1290, 1290,  242,  109,
 /*   580 */   243,  108,  240, 1290,  227,  110, 1047,  307, 1290, 1038,
 /*   590 */  1290,  862, 1290,  211, 1290,  309,  211,   10,  363,  841,
 /*   600 */    96,   66,  317,  316,  872,  177,  873,  801,  340,  803,
 /*   610 */   342,  802,  884,  309,  956,  335,  343,   86,   77,   60,
 /*   620 */    54,   66,   66,   77,  113,   77,  923,  369,    9,  281,
 /*   630 */   704,   15,  132,   14,  131,   17,  826,   16,  827,  824,
 /*   640 */     9,  825,  382,  381,    9,   19,  138,   18,  137,   89,
 /*   650 */   368,   21,  193,   20,  171,  303,  173,  174, 1145, 1173,
 /*   660 */    26, 1184, 1181, 1182, 1166,  310, 1186,  926,  176,  181,
 /*   670 */   908,  328, 1216,  192, 1215, 1214, 1213, 1317, 1141,  167,
 /*   680 */   170,  790,  194, 1139,  195,  410,  321,  196, 1053,  852,
 /*   690 */   345,  346,  347, 1163,  348,  351,  352,   75,  224,  267,
 /*   700 */   323,   71,  362, 1046,  374,   27,  325, 1311,  129, 1310,
 /*   710 */    84, 1307,  202,  383, 1303,  135, 1302, 1299,  203, 1073,
 /*   720 */    72,   67,   76,   87,  225,  337,  182,   28,  185,  183,
 /*   730 */   333, 1034,  145,  331, 1032,  147,  148, 1030, 1029,  329,
 /*   740 */  1028,  261,  214,  215, 1025,  184, 1024, 1023, 1022,  327,
 /*   750 */  1021,  324, 1020, 1019,  218,  220, 1009,  222, 1006,  223,
 /*   760 */   320, 1002,   94,  350,  401,   90,  308,  140, 1143,   97,
 /*   770 */   102,  393,  326,  394,  395,  396,  397,  398,  399,  172,
 /*   780 */   981,   88,  278,  296,  344,  297,  980,  299,  300,  979,
 /*   790 */   252,  962,  253,  961,  123, 1051, 1050,  124,  304,  309,
 /*   800 */   339,   11,  100,  829,   58,  311,  103,   80, 1027, 1026,
 /*   810 */  1018,  210, 1074,  204,  205,  206,  207,  208,  161,  209,
 /*   820 */   162,  163, 1017,    4, 1111,  186,  164,  338, 1075, 1008,
 /*   830 */  1007,  861,  859,   59,  187,  188,  189,  191, 1122,    2,
 /*   840 */   855,  854,   81,  858,  863,  178,  180,  874,  179,  269,
 /*   850 */   868,  104,   69,  870,  105,  330,  368,  334,  341,   70,
 /*   860 */    22,   23,  111,   12,   55,  114,   56,   62,  120,  116,
 /*   870 */   117,  119,  720,   63,  755,  753,  752,  751,  749,  747,
 /*   880 */   121,  744,  709,  125,  360,    7,  953,  951,  925,  954,
 /*   890 */   924,  952,  927,    8,  371,  372,   78,  128,   66,  793,
 /*   900 */    79,  130,  134,  136,  792,  789,  736,  823,  734,  726,
 /*   910 */   732,  728,  730,  724,  722,  822,  758,  757,  756,  754,
 /*   920 */   750,  748,  746,  745,  213,  707,  681,  669,  678,  676,
 /*   930 */   986,  985,  412,  985,  985,  985,  985,  985,  985,  985,
 /*   940 */   985,  985,  985,  414,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   281,    1,  210,  210,  289,  210,  291,  292,    1,    9,
 /*    10 */   291,    1,  293,  221,   14,   15,    9,   17,   18,  210,
 /*    20 */   211,   21,   22,   23,   24,  210,  210,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */     5,  281,   42,   43,   44,   14,   15,  210,   17,   18,
 /*    50 */   255,  291,   21,   22,   23,   24,  286,  262,   27,   28,
 /*    60 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*    70 */    39,  255,  257,   42,   43,   44,  283,  284,  262,   42,
 /*    80 */    43,   44,   14,   15,  263,   17,   18,  232,  233,   21,
 /*    90 */    22,   23,   24,   92,   94,   27,   28,   29,   30,   31,
 /*   100 */    32,   33,   34,   35,   36,   37,   38,   39,  281,   98,
 /*   110 */    42,   43,   44,   14,   15,  263,   17,   18,  291,  282,
 /*   120 */    21,   22,   23,   24,    0,   94,   27,   28,   29,   30,
 /*   130 */    31,   32,   33,   34,   35,   36,   37,   38,   39,  268,
 /*   140 */     5,   42,   43,   44,    5,  256,    7,   14,   15,    5,
 /*   150 */    17,   18,  263,    9,   21,   22,   23,   24,  287,   91,
 /*   160 */    27,   28,   29,   30,   31,   32,   33,   34,   35,   36,
 /*   170 */    37,   38,   39,   38,   39,   42,   43,   44,   15,  281,
 /*   180 */    17,   18,   38,   39,   21,   22,   23,   24,  105,  291,
 /*   190 */    27,   28,   29,   30,   31,   32,   33,   34,   35,   36,
 /*   200 */    37,   38,   39,    1,  263,   42,   43,   44,   17,   18,
 /*   210 */   256,    9,   21,   22,   23,   24,   92,  263,   27,   28,
 /*   220 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*   230 */    39,   90,  149,   42,   43,   44,  106,  107,  108,  109,
 /*   240 */   110,  111,  112,  113,  114,  115,  116,  117,  118,  119,
 /*   250 */   120,  286,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   260 */    58,   59,   60,   61,   62,   63,   64,  128,   66,   75,
 /*   270 */   129,  127,   90,  208,  209,   49,   74,  234,  235,  236,
 /*   280 */   237,  238,  239,  240,  241,  242,  243,  244,  245,  246,
 /*   290 */   247,  248,   66,  260,    1,    2,    1,  218,    5,  250,
 /*   300 */     7,   75,    9,  153,    9,  123,  124,   81,   82,   83,
 /*   310 */    84,  278,  162,  163,   75,   89,   90,   33,   34,   35,
 /*   320 */    36,   37,   38,   39,  218,  260,   42,   43,   44,  214,
 /*   330 */   215,   38,   39,  254,  106,   42,  108,  109,  260,  111,
 /*   340 */    47,  113,  281,  278,  289,  117,  291,  119,  120,  155,
 /*   350 */   250,  157,  291,  159,  160,  129,  278,  251,  252,  253,
 /*   360 */   254,   29,   30,   31,   32,   33,   34,   35,   36,   37,
 /*   370 */    38,   39,  135,  136,   42,   43,   44,  210,  152,    2,
 /*   380 */   154,  210,    5,   90,    7,   90,    9,  161,  221,    1,
 /*   390 */     2,  165,  221,    5,  155,    7,  157,    9,  159,  160,
 /*   400 */    67,   68,   69,  210,  281,   70,   71,   72,   73,   76,
 /*   410 */    77,   78,   79,   80,  291,   38,   39,   25,  281,  126,
 /*   420 */   127,  128,    5,  217,    7,   85,   38,   39,  291,   96,
 /*   430 */   218,  217,  217,  281,  234,   47,  236,  237,   98,  239,
 /*   440 */    48,  241,  256,  291,  151,  245,  250,  247,  248,  263,
 /*   450 */    67,   68,   69,  288,  289,  262,  291,   65,  250,   76,
 /*   460 */   281,   78,   79,   80,  252,  210,  210,  210,   38,   39,
 /*   470 */   291,   88,   42,   43,   44,  210,  210,  210,   90,   67,
 /*   480 */    68,   69,   90,  250,  281,  210,  210,  281,   76,   77,
 /*   490 */    78,   79,   80,  250,  291,  281,  281,  291,  210,  293,
 /*   500 */    67,   68,   69,  126,  127,  291,  291,  293,  293,   42,
 /*   510 */   255,  255,  255,  250,  126,  127,  128,  262,  262,  262,
 /*   520 */   255,  255,  255,   85,   86,   87,  281,  262,  262,  262,
 /*   530 */   255,  255,  281,  216,  281,  210,  291,  262,  262,  151,
 /*   540 */   223,  281,  291,  255,  291,  128,  221,  219,  220,  281,
 /*   550 */   262,  291,   83,  281,  281,  281,   91,   16,  281,  291,
 /*   560 */   281,  281,  281,  291,  291,  291,  281,  281,  291,  105,
 /*   570 */   291,  291,  291,  281,   91,  105,  291,  291,  281,  289,
 /*   580 */   281,  291,  281,  291,  281,  264,  216,   91,  291,  216,
 /*   590 */   291,   91,  291,  223,  291,  130,  223,  133,   48,  132,
 /*   600 */   279,  105,   38,   39,   91,  105,   91,   91,   91,   91,
 /*   610 */    91,   91,  143,  130,   91,   65,   87,  147,  105,   90,
 /*   620 */   105,  105,  105,  105,  105,  105,   91,   25,  105,    1,
 /*   630 */    91,  156,  156,  158,  158,  156,    5,  158,    7,    5,
 /*   640 */   105,    7,   38,   39,  105,  156,  156,  158,  158,   90,
 /*   650 */    48,  156,  258,  158,  210,  210,  210,  210,  210,  210,
 /*   660 */   280,  210,  210,  210,  260,  260,  210,  126,  210,  210,
 /*   670 */    42,  210,  290,  265,  290,  290,  290,  266,  260,   65,
 /*   680 */   212,  122,  210,  210,  210,   92,  285,  210,  210,  128,
 /*   690 */   210,  210,  210,  277,  210,  210,  210,  210,  210,  285,
 /*   700 */   285,  210,  210,  210,  210,  150,  285,  210,  210,  210,
 /*   710 */   148,  210,  210,  210,  210,  210,  210,  210,  210,  210,
 /*   720 */   210,  210,  210,  146,  210,  141,  276,  145,  273,  275,
 /*   730 */   144,  210,  210,  139,  210,  210,  210,  210,  210,  138,
 /*   740 */   210,  210,  210,  210,  210,  274,  210,  210,  210,  137,
 /*   750 */   210,  140,  210,  210,  210,  210,  210,  210,  210,  210,
 /*   760 */   134,  210,  125,   97,  121,  213,  212,  104,  212,  212,
 /*   770 */   212,  103,  212,   56,  100,  102,   60,  101,   99,  133,
 /*   780 */     5,  212,  212,  164,  212,    5,    5,  164,    5,    5,
 /*   790 */   212,  108,  212,  107,  218,  222,  222,  218,  153,  130,
 /*   800 */    87,   90,  131,   91,   90,  105,  105,  105,  212,  212,
 /*   810 */   212,  224,  231,  230,  229,  225,  228,  226,  213,  227,
 /*   820 */   213,  213,  212,  214,  249,  272,  213,  259,  233,  212,
 /*   830 */   212,   91,  128,  267,  271,  270,  269,  266,  249,  219,
 /*   840 */     5,    5,   90,  128,   91,   90,  105,   91,   90,    1,
 /*   850 */    91,   90,  105,   91,   90,   90,   48,    1,   87,  105,
 /*   860 */   142,  142,   94,   90,   90,  123,   90,   95,   77,   87,
 /*   870 */    85,   94,    5,   95,    9,    5,    5,    5,    5,    5,
 /*   880 */    94,    5,   93,   85,   16,   90,    9,    9,   91,    9,
 /*   890 */    91,    9,  126,   90,   28,   64,   17,  158,  105,    5,
 /*   900 */    17,  158,  158,  158,    5,   91,    5,  128,    5,    5,
 /*   910 */     5,    5,    5,    5,    5,  128,    5,    5,    5,    5,
 /*   920 */     5,    5,    5,    5,  105,   93,   73,   65,    9,    9,
 /*   930 */     0,  294,   22,  294,  294,  294,  294,  294,  294,  294,
 /*   940 */   294,  294,  294,   22,  294,  294,  294,  294,  294,  294,
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
 /*  1090 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1100 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1110 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1120 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1130 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1140 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*  1150 */   294,
};
#define YY_SHIFT_COUNT    (417)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (930)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   226,  130,  130,  228,  228,    1,  293,  388,  388,  388,
 /*    10 */   295,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*    20 */     7,    7,   10,   10,    0,  202,  388,  388,  388,  388,
 /*    30 */   388,  388,  388,  388,  388,  388,  388,  388,  388,  388,
 /*    40 */   388,  388,  388,  388,  388,  388,  388,  388,  388,  388,
 /*    50 */   388,  388,  388,  388,  377,  377,  377,  141,  141,  237,
 /*    60 */     7,  124,    7,    7,    7,    7,    7,  340,    1,   10,
 /*    70 */    10,   11,   11,   35,  944,  944,  944,  377,  377,  377,
 /*    80 */   144,  144,  135,  135,  135,  135,  135,  135,  182,  135,
 /*    90 */     7,    7,    7,    7,    7,    7,  467,    7,    7,    7,
 /*   100 */   141,  141,    7,    7,    7,    7,  469,  469,  469,  469,
 /*   110 */   464,  141,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   120 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   130 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   140 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   150 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   160 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   170 */   555,  614,  593,  614,  614,  614,  614,  561,  561,  561,
 /*   180 */   561,  614,  577,  562,  584,  582,  586,  594,  601,  612,
 /*   190 */   611,  626,  555,  637,  614,  614,  614,  666,  666,  643,
 /*   200 */     1,    1,  614,  614,  663,  668,  717,  674,  673,  716,
 /*   210 */   676,  679,  643,   35,  614,  614,  593,  593,  614,  593,
 /*   220 */   614,  593,  614,  614,  944,  944,   31,   68,   99,   99,
 /*   230 */    99,  133,  163,  191,  332,  332,  332,  332,  332,  332,
 /*   240 */   284,  284,  284,  284,  333,  383,  412,  430,  430,  430,
 /*   250 */   430,  430,  194,  239,  392,  335,  150,   37,   37,  139,
 /*   260 */   417,  438,  433,  496,  465,  483,  564,  500,  513,  515,
 /*   270 */   550,   83,  470,  516,  517,  518,  519,  520,  529,  523,
 /*   280 */   535,  602,  628,  541,  539,  475,  476,  479,  631,  634,
 /*   290 */   604,  489,  490,  559,  495,  646,  775,  619,  780,  781,
 /*   300 */   623,  783,  784,  683,  686,  645,  669,  713,  711,  671,
 /*   310 */   712,  714,  700,  701,  740,  702,  704,  715,  835,  836,
 /*   320 */   752,  753,  755,  756,  758,  759,  741,  761,  762,  764,
 /*   330 */   848,  765,  747,  718,  808,  856,  754,  719,  768,  773,
 /*   340 */   713,  774,  771,  776,  742,  782,  785,  772,  777,  791,
 /*   350 */   867,  778,  786,  865,  870,  871,  872,  873,  874,  876,
 /*   360 */   789,  868,  798,  877,  878,  795,  797,  799,  880,  882,
 /*   370 */   766,  803,  866,  831,  879,  739,  743,  793,  793,  793,
 /*   380 */   793,  779,  787,  883,  744,  745,  793,  793,  793,  894,
 /*   390 */   899,  814,  793,  901,  903,  904,  905,  906,  907,  908,
 /*   400 */   909,  911,  912,  913,  914,  915,  916,  917,  918,  819,
 /*   410 */   832,  853,  919,  910,  920,  921,  862,  930,
};
#define YY_REDUCE_COUNT (225)
#define YY_REDUCE_MIN   (-285)
#define YY_REDUCE_MAX   (620)
static const short yy_reduce_ofst[] = {
 /*     0 */    65,   43,   43,  200,  200,  106,  206,  214,  215, -281,
 /*    10 */  -207, -205, -184,  255,  256,  257,  265,  266,  267,  275,
 /*    20 */   276,  288, -285,  165, -163, -191, -240, -173, -102,   61,
 /*    30 */   123,  137,  152,  179,  203,  245,  251,  253,  260,  268,
 /*    40 */   272,  273,  274,  277,  279,  280,  281,  285,  286,  292,
 /*    50 */   297,  299,  301,  303, -111,  -46,  186,   33,   78, -129,
 /*    60 */  -185,   79, -208,  167,  171,  325,  193,  317,  212,   55,
 /*    70 */   290,  370,  373, -145,  321,  328,  115, -179, -148,  -59,
 /*    80 */  -230,  -35,   49,  100,  196,  208,  233,  243,  394,  263,
 /*    90 */   444,  445,  446,  447,  448,  449,  380,  451,  452,  453,
 /*   100 */   404,  405,  456,  458,  459,  461,  382,  384,  385,  386,
 /*   110 */   408,  418,  472,  473,  474,  477,  478,  480,  481,  482,
 /*   120 */   484,  485,  486,  487,  488,  491,  492,  493,  494,  497,
 /*   130 */   498,  499,  501,  502,  503,  504,  505,  506,  507,  508,
 /*   140 */   509,  510,  511,  512,  514,  521,  522,  524,  525,  526,
 /*   150 */   527,  528,  530,  531,  532,  533,  534,  536,  537,  538,
 /*   160 */   540,  542,  543,  544,  545,  546,  547,  548,  549,  551,
 /*   170 */   411,  468,  552,  554,  556,  557,  558,  401,  414,  415,
 /*   180 */   421,  560,  416,  450,  454,  471,  455,  553,  563,  565,
 /*   190 */   567,  566,  571,  568,  569,  570,  572,  573,  574,  575,
 /*   200 */   576,  579,  578,  580,  581,  583,  585,  590,  588,  591,
 /*   210 */   592,  587,  589,  595,  596,  597,  605,  607,  598,  608,
 /*   220 */   610,  613,  617,  618,  620,  609,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   983, 1110, 1048, 1121, 1035, 1045, 1295, 1295, 1295, 1295,
 /*    10 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*    20 */   983,  983,  983,  983, 1175, 1003,  983,  983,  983,  983,
 /*    30 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*    40 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*    50 */   983,  983,  983,  983,  983,  983,  983,  983,  983, 1199,
 /*    60 */   983, 1045,  983,  983,  983,  983,  983, 1056, 1045,  983,
 /*    70 */   983, 1056, 1056,  983, 1170, 1094, 1112,  983,  983,  983,
 /*    80 */   983,  983,  983,  983,  983,  983,  983,  983, 1142,  983,
 /*    90 */   983,  983,  983,  983,  983,  983, 1177, 1183, 1180,  983,
 /*   100 */   983,  983, 1185,  983,  983,  983, 1221, 1221, 1221, 1221,
 /*   110 */  1168,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   120 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   130 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   140 */   983,  983,  983,  983,  983, 1033,  983, 1031,  983,  983,
 /*   150 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   160 */   983,  983,  983,  983,  983,  983,  983,  983,  983, 1001,
 /*   170 */  1238, 1005, 1043, 1005, 1005, 1005, 1005,  983,  983,  983,
 /*   180 */   983, 1005, 1230, 1234, 1211, 1228, 1222, 1206, 1204, 1202,
 /*   190 */  1210, 1195, 1238, 1144, 1005, 1005, 1005, 1054, 1054, 1049,
 /*   200 */  1045, 1045, 1005, 1005, 1072, 1070, 1068, 1060, 1066, 1062,
 /*   210 */  1064, 1058, 1036,  983, 1005, 1005, 1043, 1043, 1005, 1043,
 /*   220 */  1005, 1043, 1005, 1005, 1094, 1112, 1294,  983, 1239, 1229,
 /*   230 */  1294,  983, 1271, 1270, 1285, 1284, 1283, 1269, 1268, 1267,
 /*   240 */  1263, 1266, 1265, 1264,  983,  983,  983, 1282, 1281, 1279,
 /*   250 */  1278, 1277,  983,  983, 1241,  983,  983, 1273, 1272,  983,
 /*   260 */   983,  983,  983,  983,  983,  983, 1192,  983,  983,  983,
 /*   270 */  1217, 1235, 1231,  983,  983,  983,  983,  983,  983,  983,
 /*   280 */   983, 1242,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   290 */  1156,  983,  983, 1123,  983,  983,  983,  983,  983,  983,
 /*   300 */   983,  983,  983,  983,  983,  983, 1167,  983,  983,  983,
 /*   310 */   983,  983, 1179, 1178,  983,  983,  983,  983,  983,  983,
 /*   320 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   330 */   983,  983, 1223,  983, 1218,  983, 1212,  983,  983,  983,
 /*   340 */  1135,  983,  983,  983,  983, 1052,  983,  983,  983,  983,
 /*   350 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   360 */   983,  983,  983,  983,  983,  983,  983,  983,  983,  983,
 /*   370 */   983,  983,  983,  983,  983,  983,  983, 1313, 1308, 1309,
 /*   380 */  1306,  983,  983,  983,  983,  983, 1305, 1300, 1301,  983,
 /*   390 */   983,  983, 1298,  983,  983,  983,  983,  983,  983,  983,
 /*   400 */   983,  983,  983,  983,  983,  983,  983,  983,  983, 1078,
 /*   410 */   983,  983,  983, 1012,  983, 1010,  983,  983,
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
    0,  /*     BITXOR => nothing */
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
    0,  /*      ALIVE => nothing */
    1,  /*    CLUSTER => ID */
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
    0,  /*       TAGS => nothing */
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
  /*   35 */ "BITXOR",
  /*   36 */ "LSHIFT",
  /*   37 */ "RSHIFT",
  /*   38 */ "PLUS",
  /*   39 */ "MINUS",
  /*   40 */ "DIVIDE",
  /*   41 */ "TIMES",
  /*   42 */ "STAR",
  /*   43 */ "SLASH",
  /*   44 */ "REM",
  /*   45 */ "UMINUS",
  /*   46 */ "UPLUS",
  /*   47 */ "BITNOT",
  /*   48 */ "ARROW",
  /*   49 */ "SHOW",
  /*   50 */ "DATABASES",
  /*   51 */ "TOPICS",
  /*   52 */ "FUNCTIONS",
  /*   53 */ "MNODES",
  /*   54 */ "DNODES",
  /*   55 */ "ACCOUNTS",
  /*   56 */ "USERS",
  /*   57 */ "MODULES",
  /*   58 */ "QUERIES",
  /*   59 */ "CONNECTIONS",
  /*   60 */ "STREAMS",
  /*   61 */ "VARIABLES",
  /*   62 */ "SCORES",
  /*   63 */ "GRANTS",
  /*   64 */ "VNODES",
  /*   65 */ "DOT",
  /*   66 */ "CREATE",
  /*   67 */ "TABLE",
  /*   68 */ "STABLE",
  /*   69 */ "DATABASE",
  /*   70 */ "TABLES",
  /*   71 */ "STABLES",
  /*   72 */ "VGROUPS",
  /*   73 */ "ALIVE",
  /*   74 */ "CLUSTER",
  /*   75 */ "DROP",
  /*   76 */ "TOPIC",
  /*   77 */ "FUNCTION",
  /*   78 */ "DNODE",
  /*   79 */ "USER",
  /*   80 */ "ACCOUNT",
  /*   81 */ "USE",
  /*   82 */ "DESCRIBE",
  /*   83 */ "DESC",
  /*   84 */ "ALTER",
  /*   85 */ "PASS",
  /*   86 */ "PRIVILEGE",
  /*   87 */ "TAGS",
  /*   88 */ "LOCAL",
  /*   89 */ "COMPACT",
  /*   90 */ "LP",
  /*   91 */ "RP",
  /*   92 */ "IF",
  /*   93 */ "EXISTS",
  /*   94 */ "AS",
  /*   95 */ "OUTPUTTYPE",
  /*   96 */ "AGGREGATE",
  /*   97 */ "BUFSIZE",
  /*   98 */ "PPS",
  /*   99 */ "TSERIES",
  /*  100 */ "DBS",
  /*  101 */ "STORAGE",
  /*  102 */ "QTIME",
  /*  103 */ "CONNS",
  /*  104 */ "STATE",
  /*  105 */ "COMMA",
  /*  106 */ "KEEP",
  /*  107 */ "CACHE",
  /*  108 */ "REPLICA",
  /*  109 */ "QUORUM",
  /*  110 */ "DAYS",
  /*  111 */ "MINROWS",
  /*  112 */ "MAXROWS",
  /*  113 */ "BLOCKS",
  /*  114 */ "CTIME",
  /*  115 */ "WAL",
  /*  116 */ "FSYNC",
  /*  117 */ "COMP",
  /*  118 */ "PRECISION",
  /*  119 */ "UPDATE",
  /*  120 */ "CACHELAST",
  /*  121 */ "PARTITIONS",
  /*  122 */ "UNSIGNED",
  /*  123 */ "USING",
  /*  124 */ "TO",
  /*  125 */ "SPLIT",
  /*  126 */ "NULL",
  /*  127 */ "NOW",
  /*  128 */ "VARIABLE",
  /*  129 */ "SELECT",
  /*  130 */ "UNION",
  /*  131 */ "ALL",
  /*  132 */ "DISTINCT",
  /*  133 */ "FROM",
  /*  134 */ "RANGE",
  /*  135 */ "INTERVAL",
  /*  136 */ "EVERY",
  /*  137 */ "SESSION",
  /*  138 */ "STATE_WINDOW",
  /*  139 */ "FILL",
  /*  140 */ "SLIDING",
  /*  141 */ "ORDER",
  /*  142 */ "BY",
  /*  143 */ "ASC",
  /*  144 */ "GROUP",
  /*  145 */ "HAVING",
  /*  146 */ "LIMIT",
  /*  147 */ "OFFSET",
  /*  148 */ "SLIMIT",
  /*  149 */ "SOFFSET",
  /*  150 */ "WHERE",
  /*  151 */ "TODAY",
  /*  152 */ "RESET",
  /*  153 */ "QUERY",
  /*  154 */ "SYNCDB",
  /*  155 */ "ADD",
  /*  156 */ "COLUMN",
  /*  157 */ "MODIFY",
  /*  158 */ "TAG",
  /*  159 */ "CHANGE",
  /*  160 */ "SET",
  /*  161 */ "KILL",
  /*  162 */ "CONNECTION",
  /*  163 */ "STREAM",
  /*  164 */ "COLON",
  /*  165 */ "DELETE",
  /*  166 */ "ABORT",
  /*  167 */ "AFTER",
  /*  168 */ "ATTACH",
  /*  169 */ "BEFORE",
  /*  170 */ "BEGIN",
  /*  171 */ "CASCADE",
  /*  172 */ "CONFLICT",
  /*  173 */ "COPY",
  /*  174 */ "DEFERRED",
  /*  175 */ "DELIMITERS",
  /*  176 */ "DETACH",
  /*  177 */ "EACH",
  /*  178 */ "END",
  /*  179 */ "EXPLAIN",
  /*  180 */ "FAIL",
  /*  181 */ "FOR",
  /*  182 */ "IGNORE",
  /*  183 */ "IMMEDIATE",
  /*  184 */ "INITIALLY",
  /*  185 */ "INSTEAD",
  /*  186 */ "KEY",
  /*  187 */ "OF",
  /*  188 */ "RAISE",
  /*  189 */ "REPLACE",
  /*  190 */ "RESTRICT",
  /*  191 */ "ROW",
  /*  192 */ "STATEMENT",
  /*  193 */ "TRIGGER",
  /*  194 */ "VIEW",
  /*  195 */ "IPTOKEN",
  /*  196 */ "SEMI",
  /*  197 */ "NONE",
  /*  198 */ "PREV",
  /*  199 */ "LINEAR",
  /*  200 */ "IMPORT",
  /*  201 */ "TBNAME",
  /*  202 */ "JOIN",
  /*  203 */ "INSERT",
  /*  204 */ "INTO",
  /*  205 */ "VALUES",
  /*  206 */ "FILE",
  /*  207 */ "error",
  /*  208 */ "program",
  /*  209 */ "cmd",
  /*  210 */ "ids",
  /*  211 */ "dbPrefix",
  /*  212 */ "cpxName",
  /*  213 */ "ifexists",
  /*  214 */ "alter_db_optr",
  /*  215 */ "alter_topic_optr",
  /*  216 */ "acct_optr",
  /*  217 */ "exprlist",
  /*  218 */ "ifnotexists",
  /*  219 */ "db_optr",
  /*  220 */ "topic_optr",
  /*  221 */ "typename",
  /*  222 */ "bufsize",
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
 /*  29 */ "cmd ::= SHOW dbPrefix ALIVE",
 /*  30 */ "cmd ::= SHOW CLUSTER ALIVE",
 /*  31 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  32 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  33 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  34 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  35 */ "cmd ::= DROP FUNCTION ids",
 /*  36 */ "cmd ::= DROP DNODE ids",
 /*  37 */ "cmd ::= DROP USER ids",
 /*  38 */ "cmd ::= DROP ACCOUNT ids",
 /*  39 */ "cmd ::= USE ids",
 /*  40 */ "cmd ::= DESCRIBE ids cpxName",
 /*  41 */ "cmd ::= DESC ids cpxName",
 /*  42 */ "cmd ::= ALTER USER ids PASS ids",
 /*  43 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  44 */ "cmd ::= ALTER USER ids TAGS ids",
 /*  45 */ "cmd ::= ALTER DNODE ids ids",
 /*  46 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  47 */ "cmd ::= ALTER LOCAL ids",
 /*  48 */ "cmd ::= ALTER LOCAL ids ids",
 /*  49 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  50 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  51 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  52 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  53 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  54 */ "ids ::= ID",
 /*  55 */ "ids ::= STRING",
 /*  56 */ "ifexists ::= IF EXISTS",
 /*  57 */ "ifexists ::=",
 /*  58 */ "ifnotexists ::= IF NOT EXISTS",
 /*  59 */ "ifnotexists ::=",
 /*  60 */ "cmd ::= CREATE DNODE ids",
 /*  61 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  62 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  63 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  64 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  65 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  66 */ "cmd ::= CREATE USER ids PASS ids",
 /*  67 */ "cmd ::= CREATE USER ids PASS ids TAGS ids",
 /*  68 */ "bufsize ::=",
 /*  69 */ "bufsize ::= BUFSIZE INTEGER",
 /*  70 */ "pps ::=",
 /*  71 */ "pps ::= PPS INTEGER",
 /*  72 */ "tseries ::=",
 /*  73 */ "tseries ::= TSERIES INTEGER",
 /*  74 */ "dbs ::=",
 /*  75 */ "dbs ::= DBS INTEGER",
 /*  76 */ "streams ::=",
 /*  77 */ "streams ::= STREAMS INTEGER",
 /*  78 */ "storage ::=",
 /*  79 */ "storage ::= STORAGE INTEGER",
 /*  80 */ "qtime ::=",
 /*  81 */ "qtime ::= QTIME INTEGER",
 /*  82 */ "users ::=",
 /*  83 */ "users ::= USERS INTEGER",
 /*  84 */ "conns ::=",
 /*  85 */ "conns ::= CONNS INTEGER",
 /*  86 */ "state ::=",
 /*  87 */ "state ::= STATE ids",
 /*  88 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  89 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  90 */ "intitemlist ::= intitem",
 /*  91 */ "intitem ::= INTEGER",
 /*  92 */ "keep ::= KEEP intitemlist",
 /*  93 */ "cache ::= CACHE INTEGER",
 /*  94 */ "replica ::= REPLICA INTEGER",
 /*  95 */ "quorum ::= QUORUM INTEGER",
 /*  96 */ "days ::= DAYS INTEGER",
 /*  97 */ "minrows ::= MINROWS INTEGER",
 /*  98 */ "maxrows ::= MAXROWS INTEGER",
 /*  99 */ "blocks ::= BLOCKS INTEGER",
 /* 100 */ "ctime ::= CTIME INTEGER",
 /* 101 */ "wal ::= WAL INTEGER",
 /* 102 */ "fsync ::= FSYNC INTEGER",
 /* 103 */ "comp ::= COMP INTEGER",
 /* 104 */ "prec ::= PRECISION STRING",
 /* 105 */ "update ::= UPDATE INTEGER",
 /* 106 */ "cachelast ::= CACHELAST INTEGER",
 /* 107 */ "partitions ::= PARTITIONS INTEGER",
 /* 108 */ "db_optr ::=",
 /* 109 */ "db_optr ::= db_optr cache",
 /* 110 */ "db_optr ::= db_optr replica",
 /* 111 */ "db_optr ::= db_optr quorum",
 /* 112 */ "db_optr ::= db_optr days",
 /* 113 */ "db_optr ::= db_optr minrows",
 /* 114 */ "db_optr ::= db_optr maxrows",
 /* 115 */ "db_optr ::= db_optr blocks",
 /* 116 */ "db_optr ::= db_optr ctime",
 /* 117 */ "db_optr ::= db_optr wal",
 /* 118 */ "db_optr ::= db_optr fsync",
 /* 119 */ "db_optr ::= db_optr comp",
 /* 120 */ "db_optr ::= db_optr prec",
 /* 121 */ "db_optr ::= db_optr keep",
 /* 122 */ "db_optr ::= db_optr update",
 /* 123 */ "db_optr ::= db_optr cachelast",
 /* 124 */ "topic_optr ::= db_optr",
 /* 125 */ "topic_optr ::= topic_optr partitions",
 /* 126 */ "alter_db_optr ::=",
 /* 127 */ "alter_db_optr ::= alter_db_optr replica",
 /* 128 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 129 */ "alter_db_optr ::= alter_db_optr keep",
 /* 130 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 131 */ "alter_db_optr ::= alter_db_optr comp",
 /* 132 */ "alter_db_optr ::= alter_db_optr update",
 /* 133 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 134 */ "alter_db_optr ::= alter_db_optr minrows",
 /* 135 */ "alter_topic_optr ::= alter_db_optr",
 /* 136 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 137 */ "typename ::= ids",
 /* 138 */ "typename ::= ids LP signed RP",
 /* 139 */ "typename ::= ids UNSIGNED",
 /* 140 */ "signed ::= INTEGER",
 /* 141 */ "signed ::= PLUS INTEGER",
 /* 142 */ "signed ::= MINUS INTEGER",
 /* 143 */ "cmd ::= CREATE TABLE create_table_args",
 /* 144 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 145 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 146 */ "cmd ::= CREATE TABLE create_table_list",
 /* 147 */ "create_table_list ::= create_from_stable",
 /* 148 */ "create_table_list ::= create_table_list create_from_stable",
 /* 149 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 150 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 151 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 152 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 153 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 154 */ "tagNamelist ::= ids",
 /* 155 */ "create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select",
 /* 156 */ "to_opt ::=",
 /* 157 */ "to_opt ::= TO ids cpxName",
 /* 158 */ "split_opt ::=",
 /* 159 */ "split_opt ::= SPLIT ids",
 /* 160 */ "columnlist ::= columnlist COMMA column",
 /* 161 */ "columnlist ::= column",
 /* 162 */ "column ::= ids typename",
 /* 163 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 164 */ "tagitemlist ::= tagitem",
 /* 165 */ "tagitem ::= INTEGER",
 /* 166 */ "tagitem ::= FLOAT",
 /* 167 */ "tagitem ::= STRING",
 /* 168 */ "tagitem ::= BOOL",
 /* 169 */ "tagitem ::= NULL",
 /* 170 */ "tagitem ::= NOW",
 /* 171 */ "tagitem ::= NOW PLUS VARIABLE",
 /* 172 */ "tagitem ::= NOW MINUS VARIABLE",
 /* 173 */ "tagitem ::= MINUS INTEGER",
 /* 174 */ "tagitem ::= MINUS FLOAT",
 /* 175 */ "tagitem ::= PLUS INTEGER",
 /* 176 */ "tagitem ::= PLUS FLOAT",
 /* 177 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 178 */ "select ::= LP select RP",
 /* 179 */ "union ::= select",
 /* 180 */ "union ::= union UNION ALL select",
 /* 181 */ "cmd ::= union",
 /* 182 */ "select ::= SELECT selcollist",
 /* 183 */ "sclp ::= selcollist COMMA",
 /* 184 */ "sclp ::=",
 /* 185 */ "selcollist ::= sclp distinct expr as",
 /* 186 */ "selcollist ::= sclp STAR",
 /* 187 */ "as ::= AS ids",
 /* 188 */ "as ::= ids",
 /* 189 */ "as ::=",
 /* 190 */ "distinct ::= DISTINCT",
 /* 191 */ "distinct ::=",
 /* 192 */ "from ::= FROM tablelist",
 /* 193 */ "from ::= FROM sub",
 /* 194 */ "sub ::= LP union RP",
 /* 195 */ "sub ::= LP union RP ids",
 /* 196 */ "sub ::= sub COMMA LP union RP ids",
 /* 197 */ "tablelist ::= ids cpxName",
 /* 198 */ "tablelist ::= ids cpxName ids",
 /* 199 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 200 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 201 */ "tmvar ::= VARIABLE",
 /* 202 */ "timestamp ::= INTEGER",
 /* 203 */ "timestamp ::= MINUS INTEGER",
 /* 204 */ "timestamp ::= PLUS INTEGER",
 /* 205 */ "timestamp ::= STRING",
 /* 206 */ "timestamp ::= NOW",
 /* 207 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 208 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 209 */ "range_option ::=",
 /* 210 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 211 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 212 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 213 */ "interval_option ::=",
 /* 214 */ "intervalKey ::= INTERVAL",
 /* 215 */ "intervalKey ::= EVERY",
 /* 216 */ "session_option ::=",
 /* 217 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 218 */ "windowstate_option ::=",
 /* 219 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 220 */ "fill_opt ::=",
 /* 221 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 222 */ "fill_opt ::= FILL LP ID RP",
 /* 223 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 224 */ "sliding_opt ::=",
 /* 225 */ "orderby_opt ::=",
 /* 226 */ "orderby_opt ::= ORDER BY sortlist",
 /* 227 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 228 */ "sortlist ::= sortlist COMMA arrow sortorder",
 /* 229 */ "sortlist ::= item sortorder",
 /* 230 */ "sortlist ::= arrow sortorder",
 /* 231 */ "item ::= ID",
 /* 232 */ "item ::= ID DOT ID",
 /* 233 */ "sortorder ::= ASC",
 /* 234 */ "sortorder ::= DESC",
 /* 235 */ "sortorder ::=",
 /* 236 */ "groupby_opt ::=",
 /* 237 */ "groupby_opt ::= GROUP BY grouplist",
 /* 238 */ "grouplist ::= grouplist COMMA item",
 /* 239 */ "grouplist ::= grouplist COMMA arrow",
 /* 240 */ "grouplist ::= item",
 /* 241 */ "grouplist ::= arrow",
 /* 242 */ "having_opt ::=",
 /* 243 */ "having_opt ::= HAVING expr",
 /* 244 */ "limit_opt ::=",
 /* 245 */ "limit_opt ::= LIMIT signed",
 /* 246 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 247 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 248 */ "slimit_opt ::=",
 /* 249 */ "slimit_opt ::= SLIMIT signed",
 /* 250 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 251 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 252 */ "where_opt ::=",
 /* 253 */ "where_opt ::= WHERE expr",
 /* 254 */ "expr ::= LP expr RP",
 /* 255 */ "expr ::= ID",
 /* 256 */ "expr ::= ID DOT ID",
 /* 257 */ "expr ::= ID DOT STAR",
 /* 258 */ "expr ::= INTEGER",
 /* 259 */ "expr ::= MINUS INTEGER",
 /* 260 */ "expr ::= PLUS INTEGER",
 /* 261 */ "expr ::= FLOAT",
 /* 262 */ "expr ::= MINUS FLOAT",
 /* 263 */ "expr ::= PLUS FLOAT",
 /* 264 */ "expr ::= STRING",
 /* 265 */ "expr ::= NOW",
 /* 266 */ "expr ::= TODAY",
 /* 267 */ "expr ::= VARIABLE",
 /* 268 */ "expr ::= PLUS VARIABLE",
 /* 269 */ "expr ::= MINUS VARIABLE",
 /* 270 */ "expr ::= BOOL",
 /* 271 */ "expr ::= NULL",
 /* 272 */ "expr ::= ID LP exprlist RP",
 /* 273 */ "expr ::= ID LP STAR RP",
 /* 274 */ "expr ::= ID LP expr AS typename RP",
 /* 275 */ "expr ::= expr IS NULL",
 /* 276 */ "expr ::= expr IS NOT NULL",
 /* 277 */ "expr ::= expr LT expr",
 /* 278 */ "expr ::= expr GT expr",
 /* 279 */ "expr ::= expr LE expr",
 /* 280 */ "expr ::= expr GE expr",
 /* 281 */ "expr ::= expr NE expr",
 /* 282 */ "expr ::= expr EQ expr",
 /* 283 */ "expr ::= expr BETWEEN expr AND expr",
 /* 284 */ "expr ::= expr AND expr",
 /* 285 */ "expr ::= expr OR expr",
 /* 286 */ "expr ::= expr PLUS expr",
 /* 287 */ "expr ::= expr MINUS expr",
 /* 288 */ "expr ::= expr STAR expr",
 /* 289 */ "expr ::= expr SLASH expr",
 /* 290 */ "expr ::= expr REM expr",
 /* 291 */ "expr ::= expr BITAND expr",
 /* 292 */ "expr ::= expr BITOR expr",
 /* 293 */ "expr ::= expr BITXOR expr",
 /* 294 */ "expr ::= BITNOT expr",
 /* 295 */ "expr ::= expr LSHIFT expr",
 /* 296 */ "expr ::= expr RSHIFT expr",
 /* 297 */ "expr ::= expr LIKE expr",
 /* 298 */ "expr ::= expr MATCH expr",
 /* 299 */ "expr ::= expr NMATCH expr",
 /* 300 */ "expr ::= ID CONTAINS STRING",
 /* 301 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 302 */ "arrow ::= ID ARROW STRING",
 /* 303 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 304 */ "expr ::= arrow",
 /* 305 */ "expr ::= expr IN LP exprlist RP",
 /* 306 */ "exprlist ::= exprlist COMMA expritem",
 /* 307 */ "exprlist ::= expritem",
 /* 308 */ "expritem ::= expr",
 /* 309 */ "expritem ::=",
 /* 310 */ "cmd ::= RESET QUERY CACHE",
 /* 311 */ "cmd ::= SYNCDB ids REPLICA",
 /* 312 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 313 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 314 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 315 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 316 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 317 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 318 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 319 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 320 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 321 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 322 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 323 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 324 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 325 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 326 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 327 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 328 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 329 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 330 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
 /* 331 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
    case 217: /* exprlist */
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
  {  208,   -1 }, /* (0) program ::= cmd */
  {  209,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  209,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  209,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  209,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  209,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  209,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  209,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  209,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  209,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  209,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  209,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  209,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  209,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  209,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  209,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  209,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  211,    0 }, /* (17) dbPrefix ::= */
  {  211,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  212,    0 }, /* (19) cpxName ::= */
  {  212,   -2 }, /* (20) cpxName ::= DOT ids */
  {  209,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  209,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  209,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  209,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  209,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
  {  209,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  209,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
  {  209,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  209,   -3 }, /* (29) cmd ::= SHOW dbPrefix ALIVE */
  {  209,   -3 }, /* (30) cmd ::= SHOW CLUSTER ALIVE */
  {  209,   -5 }, /* (31) cmd ::= DROP TABLE ifexists ids cpxName */
  {  209,   -5 }, /* (32) cmd ::= DROP STABLE ifexists ids cpxName */
  {  209,   -4 }, /* (33) cmd ::= DROP DATABASE ifexists ids */
  {  209,   -4 }, /* (34) cmd ::= DROP TOPIC ifexists ids */
  {  209,   -3 }, /* (35) cmd ::= DROP FUNCTION ids */
  {  209,   -3 }, /* (36) cmd ::= DROP DNODE ids */
  {  209,   -3 }, /* (37) cmd ::= DROP USER ids */
  {  209,   -3 }, /* (38) cmd ::= DROP ACCOUNT ids */
  {  209,   -2 }, /* (39) cmd ::= USE ids */
  {  209,   -3 }, /* (40) cmd ::= DESCRIBE ids cpxName */
  {  209,   -3 }, /* (41) cmd ::= DESC ids cpxName */
  {  209,   -5 }, /* (42) cmd ::= ALTER USER ids PASS ids */
  {  209,   -5 }, /* (43) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  209,   -5 }, /* (44) cmd ::= ALTER USER ids TAGS ids */
  {  209,   -4 }, /* (45) cmd ::= ALTER DNODE ids ids */
  {  209,   -5 }, /* (46) cmd ::= ALTER DNODE ids ids ids */
  {  209,   -3 }, /* (47) cmd ::= ALTER LOCAL ids */
  {  209,   -4 }, /* (48) cmd ::= ALTER LOCAL ids ids */
  {  209,   -4 }, /* (49) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  209,   -4 }, /* (50) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  209,   -4 }, /* (51) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  209,   -6 }, /* (52) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  209,   -6 }, /* (53) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  210,   -1 }, /* (54) ids ::= ID */
  {  210,   -1 }, /* (55) ids ::= STRING */
  {  213,   -2 }, /* (56) ifexists ::= IF EXISTS */
  {  213,    0 }, /* (57) ifexists ::= */
  {  218,   -3 }, /* (58) ifnotexists ::= IF NOT EXISTS */
  {  218,    0 }, /* (59) ifnotexists ::= */
  {  209,   -3 }, /* (60) cmd ::= CREATE DNODE ids */
  {  209,   -6 }, /* (61) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  209,   -5 }, /* (62) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  209,   -5 }, /* (63) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  209,   -8 }, /* (64) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  209,   -9 }, /* (65) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  209,   -5 }, /* (66) cmd ::= CREATE USER ids PASS ids */
  {  209,   -7 }, /* (67) cmd ::= CREATE USER ids PASS ids TAGS ids */
  {  222,    0 }, /* (68) bufsize ::= */
  {  222,   -2 }, /* (69) bufsize ::= BUFSIZE INTEGER */
  {  223,    0 }, /* (70) pps ::= */
  {  223,   -2 }, /* (71) pps ::= PPS INTEGER */
  {  224,    0 }, /* (72) tseries ::= */
  {  224,   -2 }, /* (73) tseries ::= TSERIES INTEGER */
  {  225,    0 }, /* (74) dbs ::= */
  {  225,   -2 }, /* (75) dbs ::= DBS INTEGER */
  {  226,    0 }, /* (76) streams ::= */
  {  226,   -2 }, /* (77) streams ::= STREAMS INTEGER */
  {  227,    0 }, /* (78) storage ::= */
  {  227,   -2 }, /* (79) storage ::= STORAGE INTEGER */
  {  228,    0 }, /* (80) qtime ::= */
  {  228,   -2 }, /* (81) qtime ::= QTIME INTEGER */
  {  229,    0 }, /* (82) users ::= */
  {  229,   -2 }, /* (83) users ::= USERS INTEGER */
  {  230,    0 }, /* (84) conns ::= */
  {  230,   -2 }, /* (85) conns ::= CONNS INTEGER */
  {  231,    0 }, /* (86) state ::= */
  {  231,   -2 }, /* (87) state ::= STATE ids */
  {  216,   -9 }, /* (88) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  232,   -3 }, /* (89) intitemlist ::= intitemlist COMMA intitem */
  {  232,   -1 }, /* (90) intitemlist ::= intitem */
  {  233,   -1 }, /* (91) intitem ::= INTEGER */
  {  234,   -2 }, /* (92) keep ::= KEEP intitemlist */
  {  235,   -2 }, /* (93) cache ::= CACHE INTEGER */
  {  236,   -2 }, /* (94) replica ::= REPLICA INTEGER */
  {  237,   -2 }, /* (95) quorum ::= QUORUM INTEGER */
  {  238,   -2 }, /* (96) days ::= DAYS INTEGER */
  {  239,   -2 }, /* (97) minrows ::= MINROWS INTEGER */
  {  240,   -2 }, /* (98) maxrows ::= MAXROWS INTEGER */
  {  241,   -2 }, /* (99) blocks ::= BLOCKS INTEGER */
  {  242,   -2 }, /* (100) ctime ::= CTIME INTEGER */
  {  243,   -2 }, /* (101) wal ::= WAL INTEGER */
  {  244,   -2 }, /* (102) fsync ::= FSYNC INTEGER */
  {  245,   -2 }, /* (103) comp ::= COMP INTEGER */
  {  246,   -2 }, /* (104) prec ::= PRECISION STRING */
  {  247,   -2 }, /* (105) update ::= UPDATE INTEGER */
  {  248,   -2 }, /* (106) cachelast ::= CACHELAST INTEGER */
  {  249,   -2 }, /* (107) partitions ::= PARTITIONS INTEGER */
  {  219,    0 }, /* (108) db_optr ::= */
  {  219,   -2 }, /* (109) db_optr ::= db_optr cache */
  {  219,   -2 }, /* (110) db_optr ::= db_optr replica */
  {  219,   -2 }, /* (111) db_optr ::= db_optr quorum */
  {  219,   -2 }, /* (112) db_optr ::= db_optr days */
  {  219,   -2 }, /* (113) db_optr ::= db_optr minrows */
  {  219,   -2 }, /* (114) db_optr ::= db_optr maxrows */
  {  219,   -2 }, /* (115) db_optr ::= db_optr blocks */
  {  219,   -2 }, /* (116) db_optr ::= db_optr ctime */
  {  219,   -2 }, /* (117) db_optr ::= db_optr wal */
  {  219,   -2 }, /* (118) db_optr ::= db_optr fsync */
  {  219,   -2 }, /* (119) db_optr ::= db_optr comp */
  {  219,   -2 }, /* (120) db_optr ::= db_optr prec */
  {  219,   -2 }, /* (121) db_optr ::= db_optr keep */
  {  219,   -2 }, /* (122) db_optr ::= db_optr update */
  {  219,   -2 }, /* (123) db_optr ::= db_optr cachelast */
  {  220,   -1 }, /* (124) topic_optr ::= db_optr */
  {  220,   -2 }, /* (125) topic_optr ::= topic_optr partitions */
  {  214,    0 }, /* (126) alter_db_optr ::= */
  {  214,   -2 }, /* (127) alter_db_optr ::= alter_db_optr replica */
  {  214,   -2 }, /* (128) alter_db_optr ::= alter_db_optr quorum */
  {  214,   -2 }, /* (129) alter_db_optr ::= alter_db_optr keep */
  {  214,   -2 }, /* (130) alter_db_optr ::= alter_db_optr blocks */
  {  214,   -2 }, /* (131) alter_db_optr ::= alter_db_optr comp */
  {  214,   -2 }, /* (132) alter_db_optr ::= alter_db_optr update */
  {  214,   -2 }, /* (133) alter_db_optr ::= alter_db_optr cachelast */
  {  214,   -2 }, /* (134) alter_db_optr ::= alter_db_optr minrows */
  {  215,   -1 }, /* (135) alter_topic_optr ::= alter_db_optr */
  {  215,   -2 }, /* (136) alter_topic_optr ::= alter_topic_optr partitions */
  {  221,   -1 }, /* (137) typename ::= ids */
  {  221,   -4 }, /* (138) typename ::= ids LP signed RP */
  {  221,   -2 }, /* (139) typename ::= ids UNSIGNED */
  {  250,   -1 }, /* (140) signed ::= INTEGER */
  {  250,   -2 }, /* (141) signed ::= PLUS INTEGER */
  {  250,   -2 }, /* (142) signed ::= MINUS INTEGER */
  {  209,   -3 }, /* (143) cmd ::= CREATE TABLE create_table_args */
  {  209,   -3 }, /* (144) cmd ::= CREATE TABLE create_stable_args */
  {  209,   -3 }, /* (145) cmd ::= CREATE STABLE create_stable_args */
  {  209,   -3 }, /* (146) cmd ::= CREATE TABLE create_table_list */
  {  253,   -1 }, /* (147) create_table_list ::= create_from_stable */
  {  253,   -2 }, /* (148) create_table_list ::= create_table_list create_from_stable */
  {  251,   -6 }, /* (149) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  252,  -10 }, /* (150) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  254,  -10 }, /* (151) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  254,  -13 }, /* (152) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  257,   -3 }, /* (153) tagNamelist ::= tagNamelist COMMA ids */
  {  257,   -1 }, /* (154) tagNamelist ::= ids */
  {  251,   -7 }, /* (155) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  258,    0 }, /* (156) to_opt ::= */
  {  258,   -3 }, /* (157) to_opt ::= TO ids cpxName */
  {  259,    0 }, /* (158) split_opt ::= */
  {  259,   -2 }, /* (159) split_opt ::= SPLIT ids */
  {  255,   -3 }, /* (160) columnlist ::= columnlist COMMA column */
  {  255,   -1 }, /* (161) columnlist ::= column */
  {  262,   -2 }, /* (162) column ::= ids typename */
  {  256,   -3 }, /* (163) tagitemlist ::= tagitemlist COMMA tagitem */
  {  256,   -1 }, /* (164) tagitemlist ::= tagitem */
  {  263,   -1 }, /* (165) tagitem ::= INTEGER */
  {  263,   -1 }, /* (166) tagitem ::= FLOAT */
  {  263,   -1 }, /* (167) tagitem ::= STRING */
  {  263,   -1 }, /* (168) tagitem ::= BOOL */
  {  263,   -1 }, /* (169) tagitem ::= NULL */
  {  263,   -1 }, /* (170) tagitem ::= NOW */
  {  263,   -3 }, /* (171) tagitem ::= NOW PLUS VARIABLE */
  {  263,   -3 }, /* (172) tagitem ::= NOW MINUS VARIABLE */
  {  263,   -2 }, /* (173) tagitem ::= MINUS INTEGER */
  {  263,   -2 }, /* (174) tagitem ::= MINUS FLOAT */
  {  263,   -2 }, /* (175) tagitem ::= PLUS INTEGER */
  {  263,   -2 }, /* (176) tagitem ::= PLUS FLOAT */
  {  260,  -15 }, /* (177) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  260,   -3 }, /* (178) select ::= LP select RP */
  {  278,   -1 }, /* (179) union ::= select */
  {  278,   -4 }, /* (180) union ::= union UNION ALL select */
  {  209,   -1 }, /* (181) cmd ::= union */
  {  260,   -2 }, /* (182) select ::= SELECT selcollist */
  {  279,   -2 }, /* (183) sclp ::= selcollist COMMA */
  {  279,    0 }, /* (184) sclp ::= */
  {  264,   -4 }, /* (185) selcollist ::= sclp distinct expr as */
  {  264,   -2 }, /* (186) selcollist ::= sclp STAR */
  {  282,   -2 }, /* (187) as ::= AS ids */
  {  282,   -1 }, /* (188) as ::= ids */
  {  282,    0 }, /* (189) as ::= */
  {  280,   -1 }, /* (190) distinct ::= DISTINCT */
  {  280,    0 }, /* (191) distinct ::= */
  {  265,   -2 }, /* (192) from ::= FROM tablelist */
  {  265,   -2 }, /* (193) from ::= FROM sub */
  {  284,   -3 }, /* (194) sub ::= LP union RP */
  {  284,   -4 }, /* (195) sub ::= LP union RP ids */
  {  284,   -6 }, /* (196) sub ::= sub COMMA LP union RP ids */
  {  283,   -2 }, /* (197) tablelist ::= ids cpxName */
  {  283,   -3 }, /* (198) tablelist ::= ids cpxName ids */
  {  283,   -4 }, /* (199) tablelist ::= tablelist COMMA ids cpxName */
  {  283,   -5 }, /* (200) tablelist ::= tablelist COMMA ids cpxName ids */
  {  285,   -1 }, /* (201) tmvar ::= VARIABLE */
  {  286,   -1 }, /* (202) timestamp ::= INTEGER */
  {  286,   -2 }, /* (203) timestamp ::= MINUS INTEGER */
  {  286,   -2 }, /* (204) timestamp ::= PLUS INTEGER */
  {  286,   -1 }, /* (205) timestamp ::= STRING */
  {  286,   -1 }, /* (206) timestamp ::= NOW */
  {  286,   -3 }, /* (207) timestamp ::= NOW PLUS VARIABLE */
  {  286,   -3 }, /* (208) timestamp ::= NOW MINUS VARIABLE */
  {  267,    0 }, /* (209) range_option ::= */
  {  267,   -6 }, /* (210) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  268,   -4 }, /* (211) interval_option ::= intervalKey LP tmvar RP */
  {  268,   -6 }, /* (212) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  268,    0 }, /* (213) interval_option ::= */
  {  287,   -1 }, /* (214) intervalKey ::= INTERVAL */
  {  287,   -1 }, /* (215) intervalKey ::= EVERY */
  {  270,    0 }, /* (216) session_option ::= */
  {  270,   -7 }, /* (217) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  271,    0 }, /* (218) windowstate_option ::= */
  {  271,   -4 }, /* (219) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  272,    0 }, /* (220) fill_opt ::= */
  {  272,   -6 }, /* (221) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  272,   -4 }, /* (222) fill_opt ::= FILL LP ID RP */
  {  269,   -4 }, /* (223) sliding_opt ::= SLIDING LP tmvar RP */
  {  269,    0 }, /* (224) sliding_opt ::= */
  {  275,    0 }, /* (225) orderby_opt ::= */
  {  275,   -3 }, /* (226) orderby_opt ::= ORDER BY sortlist */
  {  288,   -4 }, /* (227) sortlist ::= sortlist COMMA item sortorder */
  {  288,   -4 }, /* (228) sortlist ::= sortlist COMMA arrow sortorder */
  {  288,   -2 }, /* (229) sortlist ::= item sortorder */
  {  288,   -2 }, /* (230) sortlist ::= arrow sortorder */
  {  289,   -1 }, /* (231) item ::= ID */
  {  289,   -3 }, /* (232) item ::= ID DOT ID */
  {  290,   -1 }, /* (233) sortorder ::= ASC */
  {  290,   -1 }, /* (234) sortorder ::= DESC */
  {  290,    0 }, /* (235) sortorder ::= */
  {  273,    0 }, /* (236) groupby_opt ::= */
  {  273,   -3 }, /* (237) groupby_opt ::= GROUP BY grouplist */
  {  292,   -3 }, /* (238) grouplist ::= grouplist COMMA item */
  {  292,   -3 }, /* (239) grouplist ::= grouplist COMMA arrow */
  {  292,   -1 }, /* (240) grouplist ::= item */
  {  292,   -1 }, /* (241) grouplist ::= arrow */
  {  274,    0 }, /* (242) having_opt ::= */
  {  274,   -2 }, /* (243) having_opt ::= HAVING expr */
  {  277,    0 }, /* (244) limit_opt ::= */
  {  277,   -2 }, /* (245) limit_opt ::= LIMIT signed */
  {  277,   -4 }, /* (246) limit_opt ::= LIMIT signed OFFSET signed */
  {  277,   -4 }, /* (247) limit_opt ::= LIMIT signed COMMA signed */
  {  276,    0 }, /* (248) slimit_opt ::= */
  {  276,   -2 }, /* (249) slimit_opt ::= SLIMIT signed */
  {  276,   -4 }, /* (250) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  276,   -4 }, /* (251) slimit_opt ::= SLIMIT signed COMMA signed */
  {  266,    0 }, /* (252) where_opt ::= */
  {  266,   -2 }, /* (253) where_opt ::= WHERE expr */
  {  281,   -3 }, /* (254) expr ::= LP expr RP */
  {  281,   -1 }, /* (255) expr ::= ID */
  {  281,   -3 }, /* (256) expr ::= ID DOT ID */
  {  281,   -3 }, /* (257) expr ::= ID DOT STAR */
  {  281,   -1 }, /* (258) expr ::= INTEGER */
  {  281,   -2 }, /* (259) expr ::= MINUS INTEGER */
  {  281,   -2 }, /* (260) expr ::= PLUS INTEGER */
  {  281,   -1 }, /* (261) expr ::= FLOAT */
  {  281,   -2 }, /* (262) expr ::= MINUS FLOAT */
  {  281,   -2 }, /* (263) expr ::= PLUS FLOAT */
  {  281,   -1 }, /* (264) expr ::= STRING */
  {  281,   -1 }, /* (265) expr ::= NOW */
  {  281,   -1 }, /* (266) expr ::= TODAY */
  {  281,   -1 }, /* (267) expr ::= VARIABLE */
  {  281,   -2 }, /* (268) expr ::= PLUS VARIABLE */
  {  281,   -2 }, /* (269) expr ::= MINUS VARIABLE */
  {  281,   -1 }, /* (270) expr ::= BOOL */
  {  281,   -1 }, /* (271) expr ::= NULL */
  {  281,   -4 }, /* (272) expr ::= ID LP exprlist RP */
  {  281,   -4 }, /* (273) expr ::= ID LP STAR RP */
  {  281,   -6 }, /* (274) expr ::= ID LP expr AS typename RP */
  {  281,   -3 }, /* (275) expr ::= expr IS NULL */
  {  281,   -4 }, /* (276) expr ::= expr IS NOT NULL */
  {  281,   -3 }, /* (277) expr ::= expr LT expr */
  {  281,   -3 }, /* (278) expr ::= expr GT expr */
  {  281,   -3 }, /* (279) expr ::= expr LE expr */
  {  281,   -3 }, /* (280) expr ::= expr GE expr */
  {  281,   -3 }, /* (281) expr ::= expr NE expr */
  {  281,   -3 }, /* (282) expr ::= expr EQ expr */
  {  281,   -5 }, /* (283) expr ::= expr BETWEEN expr AND expr */
  {  281,   -3 }, /* (284) expr ::= expr AND expr */
  {  281,   -3 }, /* (285) expr ::= expr OR expr */
  {  281,   -3 }, /* (286) expr ::= expr PLUS expr */
  {  281,   -3 }, /* (287) expr ::= expr MINUS expr */
  {  281,   -3 }, /* (288) expr ::= expr STAR expr */
  {  281,   -3 }, /* (289) expr ::= expr SLASH expr */
  {  281,   -3 }, /* (290) expr ::= expr REM expr */
  {  281,   -3 }, /* (291) expr ::= expr BITAND expr */
  {  281,   -3 }, /* (292) expr ::= expr BITOR expr */
  {  281,   -3 }, /* (293) expr ::= expr BITXOR expr */
  {  281,   -2 }, /* (294) expr ::= BITNOT expr */
  {  281,   -3 }, /* (295) expr ::= expr LSHIFT expr */
  {  281,   -3 }, /* (296) expr ::= expr RSHIFT expr */
  {  281,   -3 }, /* (297) expr ::= expr LIKE expr */
  {  281,   -3 }, /* (298) expr ::= expr MATCH expr */
  {  281,   -3 }, /* (299) expr ::= expr NMATCH expr */
  {  281,   -3 }, /* (300) expr ::= ID CONTAINS STRING */
  {  281,   -5 }, /* (301) expr ::= ID DOT ID CONTAINS STRING */
  {  291,   -3 }, /* (302) arrow ::= ID ARROW STRING */
  {  291,   -5 }, /* (303) arrow ::= ID DOT ID ARROW STRING */
  {  281,   -1 }, /* (304) expr ::= arrow */
  {  281,   -5 }, /* (305) expr ::= expr IN LP exprlist RP */
  {  217,   -3 }, /* (306) exprlist ::= exprlist COMMA expritem */
  {  217,   -1 }, /* (307) exprlist ::= expritem */
  {  293,   -1 }, /* (308) expritem ::= expr */
  {  293,    0 }, /* (309) expritem ::= */
  {  209,   -3 }, /* (310) cmd ::= RESET QUERY CACHE */
  {  209,   -3 }, /* (311) cmd ::= SYNCDB ids REPLICA */
  {  209,   -7 }, /* (312) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  209,   -7 }, /* (313) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  209,   -7 }, /* (314) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  209,   -7 }, /* (315) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  209,   -7 }, /* (316) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  209,   -8 }, /* (317) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  209,   -9 }, /* (318) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  209,   -7 }, /* (319) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  209,   -7 }, /* (320) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  209,   -7 }, /* (321) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  209,   -7 }, /* (322) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  209,   -7 }, /* (323) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  209,   -7 }, /* (324) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  209,   -8 }, /* (325) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  209,   -9 }, /* (326) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  209,   -7 }, /* (327) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  209,   -3 }, /* (328) cmd ::= KILL CONNECTION INTEGER */
  {  209,   -5 }, /* (329) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  209,   -5 }, /* (330) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  209,   -6 }, /* (331) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
      case 143: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==143);
      case 144: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==144);
      case 145: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==145);
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
      case 29: /* cmd ::= SHOW dbPrefix ALIVE */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_ALIVE_DB, &token, 0);
}
        break;
      case 30: /* cmd ::= SHOW CLUSTER ALIVE */
{
    SStrToken token;
    setShowOptions(pInfo, TSDB_MGMT_ALIVE_CLUSTER, &token, 0);
}
        break;
      case 31: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 32: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 33: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 34: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 35: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 36: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 37: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 38: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 39: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= DESCRIBE ids cpxName */
      case 41: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==41);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 42: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD,     &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0,   NULL, NULL);}
        break;
      case 43: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0,   NULL);}
        break;
      case 44: /* cmd ::= ALTER USER ids TAGS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_TAGS,       &yymsp[-2].minor.yy0, NULL, NULL, &yymsp[0].minor.yy0);}
        break;
      case 45: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 46: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 47: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 48: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 49: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 50: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==50);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
        break;
      case 51: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy547);}
        break;
      case 52: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy547);}
        break;
      case 53: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy249);}
        break;
      case 54: /* ids ::= ID */
      case 55: /* ids ::= STRING */ yytestcase(yyruleno==55);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 56: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 57: /* ifexists ::= */
      case 59: /* ifnotexists ::= */ yytestcase(yyruleno==59);
      case 191: /* distinct ::= */ yytestcase(yyruleno==191);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 58: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 60: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy547);}
        break;
      case 62: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 63: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==63);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
        break;
      case 64: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy475, &yymsp[0].minor.yy0, 1);}
        break;
      case 65: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy475, &yymsp[0].minor.yy0, 2);}
        break;
      case 66: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);}
        break;
      case 67: /* cmd ::= CREATE USER ids PASS ids TAGS ids */
{ setCreateUserSql(pInfo, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 68: /* bufsize ::= */
      case 70: /* pps ::= */ yytestcase(yyruleno==70);
      case 72: /* tseries ::= */ yytestcase(yyruleno==72);
      case 74: /* dbs ::= */ yytestcase(yyruleno==74);
      case 76: /* streams ::= */ yytestcase(yyruleno==76);
      case 78: /* storage ::= */ yytestcase(yyruleno==78);
      case 80: /* qtime ::= */ yytestcase(yyruleno==80);
      case 82: /* users ::= */ yytestcase(yyruleno==82);
      case 84: /* conns ::= */ yytestcase(yyruleno==84);
      case 86: /* state ::= */ yytestcase(yyruleno==86);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 69: /* bufsize ::= BUFSIZE INTEGER */
      case 71: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==73);
      case 75: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==75);
      case 77: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==77);
      case 79: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==79);
      case 81: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==81);
      case 83: /* users ::= USERS INTEGER */ yytestcase(yyruleno==83);
      case 85: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==85);
      case 87: /* state ::= STATE ids */ yytestcase(yyruleno==87);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 88: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
      case 89: /* intitemlist ::= intitemlist COMMA intitem */
      case 163: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==163);
{ yylhsminor.yy249 = tVariantListAppend(yymsp[-2].minor.yy249, &yymsp[0].minor.yy134, -1);    }
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 90: /* intitemlist ::= intitem */
      case 164: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==164);
{ yylhsminor.yy249 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1); }
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 91: /* intitem ::= INTEGER */
      case 165: /* tagitem ::= INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= FLOAT */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= STRING */ yytestcase(yyruleno==167);
      case 168: /* tagitem ::= BOOL */ yytestcase(yyruleno==168);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 92: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy249 = yymsp[0].minor.yy249; }
        break;
      case 93: /* cache ::= CACHE INTEGER */
      case 94: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==94);
      case 95: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==95);
      case 96: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==96);
      case 97: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==97);
      case 98: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==98);
      case 99: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==99);
      case 100: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==100);
      case 101: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==101);
      case 102: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==102);
      case 103: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==103);
      case 104: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==104);
      case 105: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==105);
      case 106: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==106);
      case 107: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==107);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 108: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy478); yymsp[1].minor.yy478.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 109: /* db_optr ::= db_optr cache */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 110: /* db_optr ::= db_optr replica */
      case 127: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==127);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 111: /* db_optr ::= db_optr quorum */
      case 128: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==128);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 112: /* db_optr ::= db_optr days */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 113: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 114: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 115: /* db_optr ::= db_optr blocks */
      case 130: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==130);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 116: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 117: /* db_optr ::= db_optr wal */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 118: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 119: /* db_optr ::= db_optr comp */
      case 131: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==131);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 120: /* db_optr ::= db_optr prec */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 121: /* db_optr ::= db_optr keep */
      case 129: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==129);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.keep = yymsp[0].minor.yy249; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 122: /* db_optr ::= db_optr update */
      case 132: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==132);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 123: /* db_optr ::= db_optr cachelast */
      case 133: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==133);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 124: /* topic_optr ::= db_optr */
      case 135: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==135);
{ yylhsminor.yy478 = yymsp[0].minor.yy478; yylhsminor.yy478.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy478 = yylhsminor.yy478;
        break;
      case 125: /* topic_optr ::= topic_optr partitions */
      case 136: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==136);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 126: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy478); yymsp[1].minor.yy478.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 134: /* alter_db_optr ::= alter_db_optr minrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.minRowsPerBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 137: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy475, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy475 = yylhsminor.yy475;
        break;
      case 138: /* typename ::= ids LP signed RP */
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
      case 139: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy475, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy475 = yylhsminor.yy475;
        break;
      case 140: /* signed ::= INTEGER */
{ yylhsminor.yy165 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 141: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy165 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 142: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy165 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 146: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy494;}
        break;
      case 147: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy192);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy494 = pCreateTable;
}
  yymsp[0].minor.yy494 = yylhsminor.yy494;
        break;
      case 148: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy494->childTableInfo, &yymsp[0].minor.yy192);
  yylhsminor.yy494 = yymsp[-1].minor.yy494;
}
  yymsp[-1].minor.yy494 = yylhsminor.yy494;
        break;
      case 149: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy494 = tSetCreateTableInfo(yymsp[-1].minor.yy249, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy494, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy494 = yylhsminor.yy494;
        break;
      case 150: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy494 = tSetCreateTableInfo(yymsp[-5].minor.yy249, yymsp[-1].minor.yy249, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy494, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy494 = yylhsminor.yy494;
        break;
      case 151: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy249, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy192 = yylhsminor.yy192;
        break;
      case 152: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy249, yymsp[-1].minor.yy249, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy192 = yylhsminor.yy192;
        break;
      case 153: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy249, &yymsp[0].minor.yy0); yylhsminor.yy249 = yymsp[-2].minor.yy249;  }
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 154: /* tagNamelist ::= ids */
{yylhsminor.yy249 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy249, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 155: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy494 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy320, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy494, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy494 = yylhsminor.yy494;
        break;
      case 156: /* to_opt ::= */
      case 158: /* split_opt ::= */ yytestcase(yyruleno==158);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 157: /* to_opt ::= TO ids cpxName */
{
   yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
   yymsp[-2].minor.yy0.n += yymsp[0].minor.yy0.n;
}
        break;
      case 159: /* split_opt ::= SPLIT ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 160: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy249, &yymsp[0].minor.yy475); yylhsminor.yy249 = yymsp[-2].minor.yy249;  }
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 161: /* columnlist ::= column */
{yylhsminor.yy249 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy249, &yymsp[0].minor.yy475);}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 162: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy475, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy475);
}
  yymsp[-1].minor.yy475 = yylhsminor.yy475;
        break;
      case 169: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 170: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy134, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 171: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy134, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 172: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy134, &yymsp[0].minor.yy0, TK_MINUS, true);
}
        break;
      case 173: /* tagitem ::= MINUS INTEGER */
      case 174: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==174);
      case 175: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==175);
      case 176: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==176);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy134, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 177: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy320 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy249, yymsp[-12].minor.yy52, yymsp[-11].minor.yy370, yymsp[-4].minor.yy249, yymsp[-2].minor.yy249, &yymsp[-9].minor.yy196, &yymsp[-7].minor.yy559, &yymsp[-6].minor.yy385, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy249, &yymsp[0].minor.yy342, &yymsp[-1].minor.yy342, yymsp[-3].minor.yy370, &yymsp[-10].minor.yy384);
}
  yymsp[-14].minor.yy320 = yylhsminor.yy320;
        break;
      case 178: /* select ::= LP select RP */
{yymsp[-2].minor.yy320 = yymsp[-1].minor.yy320;}
        break;
      case 179: /* union ::= select */
{ yylhsminor.yy249 = setSubclause(NULL, yymsp[0].minor.yy320); }
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 180: /* union ::= union UNION ALL select */
{ yylhsminor.yy249 = appendSelectClause(yymsp[-3].minor.yy249, yymsp[0].minor.yy320); }
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 181: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy249, NULL, TSDB_SQL_SELECT); }
        break;
      case 182: /* select ::= SELECT selcollist */
{
  yylhsminor.yy320 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy249, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy320 = yylhsminor.yy320;
        break;
      case 183: /* sclp ::= selcollist COMMA */
{yylhsminor.yy249 = yymsp[-1].minor.yy249;}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 184: /* sclp ::= */
      case 225: /* orderby_opt ::= */ yytestcase(yyruleno==225);
{yymsp[1].minor.yy249 = 0;}
        break;
      case 185: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy249 = tSqlExprListAppend(yymsp[-3].minor.yy249, yymsp[-1].minor.yy370,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 186: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy249 = tSqlExprListAppend(yymsp[-1].minor.yy249, pNode, 0, 0);
}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 187: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 188: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 189: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 190: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* from ::= FROM tablelist */
      case 193: /* from ::= FROM sub */ yytestcase(yyruleno==193);
{yymsp[-1].minor.yy52 = yymsp[0].minor.yy52;}
        break;
      case 194: /* sub ::= LP union RP */
{yymsp[-2].minor.yy52 = addSubqueryElem(NULL, yymsp[-1].minor.yy249, NULL);}
        break;
      case 195: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy52 = addSubqueryElem(NULL, yymsp[-2].minor.yy249, &yymsp[0].minor.yy0);}
        break;
      case 196: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy52 = addSubqueryElem(yymsp[-5].minor.yy52, yymsp[-2].minor.yy249, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy52 = yylhsminor.yy52;
        break;
      case 197: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy52 = yylhsminor.yy52;
        break;
      case 198: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy52 = yylhsminor.yy52;
        break;
      case 199: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(yymsp[-3].minor.yy52, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy52 = yylhsminor.yy52;
        break;
      case 200: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(yymsp[-4].minor.yy52, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy52 = yylhsminor.yy52;
        break;
      case 201: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 202: /* timestamp ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 203: /* timestamp ::= MINUS INTEGER */
      case 204: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==204);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 205: /* timestamp ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 206: /* timestamp ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 207: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 208: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy370 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 209: /* range_option ::= */
{yymsp[1].minor.yy384.start = 0; yymsp[1].minor.yy384.end = 0;}
        break;
      case 210: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy384.start = yymsp[-3].minor.yy370; yymsp[-5].minor.yy384.end = yymsp[-1].minor.yy370;}
        break;
      case 211: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy196.interval = yymsp[-1].minor.yy0; yylhsminor.yy196.offset.n = 0; yylhsminor.yy196.token = yymsp[-3].minor.yy88;}
  yymsp[-3].minor.yy196 = yylhsminor.yy196;
        break;
      case 212: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy196.interval = yymsp[-3].minor.yy0; yylhsminor.yy196.offset = yymsp[-1].minor.yy0;   yylhsminor.yy196.token = yymsp[-5].minor.yy88;}
  yymsp[-5].minor.yy196 = yylhsminor.yy196;
        break;
      case 213: /* interval_option ::= */
{memset(&yymsp[1].minor.yy196, 0, sizeof(yymsp[1].minor.yy196));}
        break;
      case 214: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy88 = TK_INTERVAL;}
        break;
      case 215: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy88 = TK_EVERY;   }
        break;
      case 216: /* session_option ::= */
{yymsp[1].minor.yy559.col.n = 0; yymsp[1].minor.yy559.gap.n = 0;}
        break;
      case 217: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy559.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy559.gap = yymsp[-1].minor.yy0;
}
        break;
      case 218: /* windowstate_option ::= */
{ yymsp[1].minor.yy385.col.n = 0; yymsp[1].minor.yy385.col.z = NULL;}
        break;
      case 219: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy385.col = yymsp[-1].minor.yy0; }
        break;
      case 220: /* fill_opt ::= */
{ yymsp[1].minor.yy249 = 0;     }
        break;
      case 221: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy249, &A, -1, 0);
    yymsp[-5].minor.yy249 = yymsp[-1].minor.yy249;
}
        break;
      case 222: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy249 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 223: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 224: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 226: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy249 = yymsp[0].minor.yy249;}
        break;
      case 227: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-3].minor.yy249, &yymsp[-1].minor.yy134, NULL, false, yymsp[0].minor.yy424);
}
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 228: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-3].minor.yy249, NULL, yymsp[-1].minor.yy370, true, yymsp[0].minor.yy424);
}
  yymsp[-3].minor.yy249 = yylhsminor.yy249;
        break;
      case 229: /* sortlist ::= item sortorder */
{
  yylhsminor.yy249 = commonItemAppend(NULL, &yymsp[-1].minor.yy134, NULL, false, yymsp[0].minor.yy424);
}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 230: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy249 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy370, true, yymsp[0].minor.yy424);
}
  yymsp[-1].minor.yy249 = yylhsminor.yy249;
        break;
      case 231: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 232: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy134, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 233: /* sortorder ::= ASC */
{ yymsp[0].minor.yy424 = TSDB_ORDER_ASC; }
        break;
      case 234: /* sortorder ::= DESC */
{ yymsp[0].minor.yy424 = TSDB_ORDER_DESC;}
        break;
      case 235: /* sortorder ::= */
{ yymsp[1].minor.yy424 = TSDB_ORDER_ASC; }
        break;
      case 236: /* groupby_opt ::= */
{ yymsp[1].minor.yy249 = 0;}
        break;
      case 237: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy249 = yymsp[0].minor.yy249;}
        break;
      case 238: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-2].minor.yy249, &yymsp[0].minor.yy134, NULL, false, -1);
}
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 239: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy249 = commonItemAppend(yymsp[-2].minor.yy249, NULL, yymsp[0].minor.yy370, true, -1);
}
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 240: /* grouplist ::= item */
{
  yylhsminor.yy249 = commonItemAppend(NULL, &yymsp[0].minor.yy134, NULL, false, -1);
}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 241: /* grouplist ::= arrow */
{
  yylhsminor.yy249 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy370, true, -1);
}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 242: /* having_opt ::= */
      case 252: /* where_opt ::= */ yytestcase(yyruleno==252);
      case 309: /* expritem ::= */ yytestcase(yyruleno==309);
{yymsp[1].minor.yy370 = 0;}
        break;
      case 243: /* having_opt ::= HAVING expr */
      case 253: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==253);
{yymsp[-1].minor.yy370 = yymsp[0].minor.yy370;}
        break;
      case 244: /* limit_opt ::= */
      case 248: /* slimit_opt ::= */ yytestcase(yyruleno==248);
{yymsp[1].minor.yy342.limit = -1; yymsp[1].minor.yy342.offset = 0;}
        break;
      case 245: /* limit_opt ::= LIMIT signed */
      case 249: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==249);
{yymsp[-1].minor.yy342.limit = yymsp[0].minor.yy165;  yymsp[-1].minor.yy342.offset = 0;}
        break;
      case 246: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy342.limit = yymsp[-2].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[0].minor.yy165;}
        break;
      case 247: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy342.limit = yymsp[0].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[-2].minor.yy165;}
        break;
      case 250: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy342.limit = yymsp[-2].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[0].minor.yy165;}
        break;
      case 251: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy342.limit = yymsp[0].minor.yy165;  yymsp[-3].minor.yy342.offset = yymsp[-2].minor.yy165;}
        break;
      case 254: /* expr ::= LP expr RP */
{yylhsminor.yy370 = yymsp[-1].minor.yy370; yylhsminor.yy370->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy370->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 255: /* expr ::= ID */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 256: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 257: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 258: /* expr ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 259: /* expr ::= MINUS INTEGER */
      case 260: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==260);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 261: /* expr ::= FLOAT */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 262: /* expr ::= MINUS FLOAT */
      case 263: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==263);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 264: /* expr ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 265: /* expr ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 266: /* expr ::= TODAY */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 267: /* expr ::= VARIABLE */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 268: /* expr ::= PLUS VARIABLE */
      case 269: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==269);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 270: /* expr ::= BOOL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 271: /* expr ::= NULL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 272: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(yymsp[-1].minor.yy249, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 273: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 274: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy370, &yymsp[-1].minor.yy475, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy370 = yylhsminor.yy370;
        break;
      case 275: /* expr ::= expr IS NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 276: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-3].minor.yy370, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 277: /* expr ::= expr LT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 278: /* expr ::= expr GT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 279: /* expr ::= expr LE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 280: /* expr ::= expr GE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 281: /* expr ::= expr NE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 282: /* expr ::= expr EQ expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_EQ);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 283: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy370); yylhsminor.yy370 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy370, yymsp[-2].minor.yy370, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy370, TK_LE), TK_AND);}
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 284: /* expr ::= expr AND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_AND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 285: /* expr ::= expr OR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_OR); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 286: /* expr ::= expr PLUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_PLUS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 287: /* expr ::= expr MINUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MINUS); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 288: /* expr ::= expr STAR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_STAR);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 289: /* expr ::= expr SLASH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_DIVIDE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 290: /* expr ::= expr REM expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_REM);   }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 291: /* expr ::= expr BITAND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_BITAND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 292: /* expr ::= expr BITOR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_BITOR); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 293: /* expr ::= expr BITXOR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_BITXOR);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 294: /* expr ::= BITNOT expr */
{yymsp[-1].minor.yy370 = tSqlExprCreate(yymsp[0].minor.yy370, NULL, TK_BITNOT);}
        break;
      case 295: /* expr ::= expr LSHIFT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LSHIFT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 296: /* expr ::= expr RSHIFT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_RSHIFT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 297: /* expr ::= expr LIKE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LIKE);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 298: /* expr ::= expr MATCH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MATCH);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 299: /* expr ::= expr NMATCH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NMATCH);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 300: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 301: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 302: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 303: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy370 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 304: /* expr ::= arrow */
      case 308: /* expritem ::= expr */ yytestcase(yyruleno==308);
{yylhsminor.yy370 = yymsp[0].minor.yy370;}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 305: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-4].minor.yy370, (tSqlExpr*)yymsp[-1].minor.yy249, TK_IN); }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 306: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy249 = tSqlExprListAppend(yymsp[-2].minor.yy249,yymsp[0].minor.yy370,0, 0);}
  yymsp[-2].minor.yy249 = yylhsminor.yy249;
        break;
      case 307: /* exprlist ::= expritem */
{yylhsminor.yy249 = tSqlExprListAppend(0,yymsp[0].minor.yy370,0, 0);}
  yymsp[0].minor.yy249 = yylhsminor.yy249;
        break;
      case 310: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 311: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 312: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 318: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 321: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 322: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 323: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 324: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 325: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 326: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 327: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy249, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 328: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 329: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 330: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      case 331: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
