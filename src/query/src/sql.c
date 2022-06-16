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
#define YYNOCODE 294
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SLimitVal yy24;
  SCreateTableSql* yy74;
  SCreatedTableInfo yy110;
  SWindowStateVal yy204;
  SRangeVal yy214;
  int yy274;
  TAOS_FIELD yy307;
  SArray* yy367;
  SSessionWindowVal yy373;
  tSqlExpr* yy378;
  tVariant yy410;
  SSqlNode* yy426;
  int64_t yy443;
  SIntervalVal yy478;
  SRelationInfo* yy480;
  SCreateAcctInfo yy563;
  SCreateDbInfo yy564;
  int32_t yy586;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             414
#define YYNRULE              327
#define YYNTOKEN             206
#define YY_MAX_SHIFT         413
#define YY_MIN_SHIFTREDUCE   643
#define YY_MAX_SHIFTREDUCE   969
#define YY_ERROR_ACTION      970
#define YY_ACCEPT_ACTION     971
#define YY_NO_ACTION         972
#define YY_MIN_REDUCE        973
#define YY_MAX_REDUCE        1299
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
#define YY_ACTTAB_COUNT (935)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   228,  694,  290,  173, 1208,   65, 1209,  330,  694,  695,
 /*    10 */  1272,  267, 1274,  195,   43,   44,  695,   47,   48,  412,
 /*    20 */   258,  280,   32,   31,   30, 1131,   65,   46,  363,   51,
 /*    30 */    49,   52,   50,   37,   36,   35,   34,   33,   42,   41,
 /*    40 */   265,  110,   40,   39,   38,   43,   44, 1132,   47,   48,
 /*    50 */   260,  778,  280,   32,   31,   30,   96, 1129,   46,  363,
 /*    60 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*    70 */    41,  270, 1147,   40,   39,   38,  311,  310, 1129,   43,
 /*    80 */    44,  730,   47,   48,  388,  387,  280,   32,   31,   30,
 /*    90 */   261,   95,   46,  363,   51,   49,   52,   50,   37,   36,
 /*   100 */    35,   34,   33,   42,   41,   24,  226,   40,   39,   38,
 /*   110 */    43,   44,   13,   47,   48, 1272, 1272,  280,   32,   31,
 /*   120 */    30, 1122,   64,   46,  363,   51,   49,   52,   50,   37,
 /*   130 */    36,   35,   34,   33,   42,   41,  272,   82,   40,   39,
 /*   140 */    38,   43,   45, 1132,   47,   48,  114,   93,  280,   32,
 /*   150 */    31,   30,  359,  892,   46,  363,   51,   49,   52,   50,
 /*   160 */    37,   36,   35,   34,   33,   42,   41,  296,  273,   40,
 /*   170 */    39,   38,   44,  227,   47,   48,  300,  299,  280,   32,
 /*   180 */    31,   30,   83, 1272,   46,  363,   51,   49,   52,   50,
 /*   190 */    37,   36,   35,   34,   33,   42,   41,  694,  140,   40,
 /*   200 */    39,   38,   47,   48,  274,  695,  280,   32,   31,   30,
 /*   210 */   398, 1132,   46,  363,   51,   49,   52,   50,   37,   36,
 /*   220 */    35,   34,   33,   42,   41,  852,  853,   40,   39,   38,
 /*   230 */   398,   73,  357,  405,  404,  356,  355,  354,  403,  353,
 /*   240 */   352,  351,  402,  350,  401,  400,  644,  645,  646,  647,
 /*   250 */   648,  649,  650,  651,  652,  653,  654,  655,  656,  657,
 /*   260 */   167,  101,  259, 1090, 1078, 1079, 1080, 1081, 1082, 1083,
 /*   270 */  1084, 1085, 1086, 1087, 1088, 1089, 1091, 1092,   25,  840,
 /*   280 */   252,  908,  362,  843,  896,   85,  899,  188,  902,  138,
 /*   290 */   132,  143,  166,  164,  163,  242,  142,  312,  148,  151,
 /*   300 */   141,   74,  244,  290,  277,  361,  320,  145,  154,  153,
 /*   310 */   152,  243,  316,  317,  196,  371,  101,  256,  257, 1156,
 /*   320 */  1262,  365,  279,  252,  908, 1147,   29,  896,   86,  899,
 /*   330 */  1272,  902,   51,   49,   52,   50,   37,   36,   35,   34,
 /*   340 */    33,   42,   41,  262,    6,   40,   39,   38,   65,  112,
 /*   350 */   806,  694,  232,  803,  115,  804,   74,  805,  313,  695,
 /*   360 */   256,  257, 1272,    5,   68,  199,   53, 1294,  224,   29,
 /*   370 */   198,  121,  126,  117,  125,  411,  409,  671, 1272,  303,
 /*   380 */  1275,   91, 1111, 1112,   61, 1115,  285,  286,  253,  346,
 /*   390 */  1116, 1153,  293,  271, 1114,  334,  107,  283,  106,  263,
 /*   400 */  1129, 1021, 1219,  909,  903,  905,  289,   89,  209,   53,
 /*   410 */   898,  276,  901,   37,   36,   35,   34,   33,   42,   41,
 /*   420 */   233,  234,   40,   39,   38,  219,  217,  215,  904,  281,
 /*   430 */  1272, 1272,  214,  158,  157,  156,  155,   57,   73,  777,
 /*   440 */   405,  404,  971,  413,   98,  403,  909,  903,  905,  402,
 /*   450 */    65,  401,  400, 1098,   65, 1096, 1097,   42,   41,   65,
 /*   460 */  1099,   40,   39,   38, 1100,   65, 1101, 1102,   65,   65,
 /*   470 */    65,  904,  807,  287,   65,  228,  897,  284,  900,  282,
 /*   480 */   872,  374,  373,  359,  307, 1272,  291, 1275,  288,  290,
 /*   490 */   383,  382,  361,  228, 1147,  375,   40,   39,   38,  376,
 /*   500 */   364,  824, 1129, 1272,  377, 1275, 1129,   92,  245,  333,
 /*   510 */   378, 1129,  304,  384,  385,  386,   65, 1129, 1272,  390,
 /*   520 */  1129, 1129, 1129,  246,  247,  290, 1129,  248,  249, 1258,
 /*   530 */   368,  906, 1257, 1272, 1272, 1256, 1130, 1272, 1272, 1272,
 /*   540 */   871, 1113, 1272,  254,  255, 1272,  230,  231,  235,  229,
 /*   550 */   236,  237,  239, 1272, 1272,  240, 1272, 1272, 1272, 1272,
 /*   560 */  1272, 1272, 1272,  241,  238, 1272,  821,  225, 1128, 1206,
 /*   570 */   109, 1207,  108, 1272, 1272, 1031, 1022, 1272,  406, 1059,
 /*   580 */     1,  197,  209,  209,    3,  210,  305,   99,  849,  828,
 /*   590 */   315,  314,  859,  860,  788,   10,  338,  907,  790,  340,
 /*   600 */    66,  789,  175,   60,  278,  943,   77,   54,   66,  910,
 /*   610 */    66,  367,   77,  113,  693,   77,   15, 1286,   14,    9,
 /*   620 */   131, 1218,  130,    9,   17,  268,   16,  307,    9,  813,
 /*   630 */   811,  814,  812,   19,  366,   18,  341, 1215,  913,  380,
 /*   640 */   379,  137,   21,  136,   20,  895,  150,  149,  191, 1214,
 /*   650 */   269,  389,  169,   26,  301,  171,  172, 1127, 1155, 1166,
 /*   660 */  1163, 1164, 1148,  308, 1168,  174,  179,  326, 1198, 1123,
 /*   670 */  1197, 1196, 1195,  190,  192,  165, 1121,  193,  194, 1299,
 /*   680 */   407, 1036,  343,  344,  345,  348,  349,   75,  839,  222,
 /*   690 */    71,  360, 1030,  372, 1293,  128,   27,  319, 1292, 1289,
 /*   700 */   200,  381, 1285,  134, 1284, 1281,  201,  264,  321,  323,
 /*   710 */  1056,   72,   67,   87, 1145,  180,   76,   84,  223,  335,
 /*   720 */    28, 1018,  329,  331,  144,  327,  182, 1016,  146,  147,
 /*   730 */   185, 1014, 1013,  292,  212,  181,  213, 1010, 1009, 1008,
 /*   740 */  1007, 1006, 1005, 1004,  216,  218,  996,  220,  993,  221,
 /*   750 */   989,  325,  322,  168,  318,   90,  306, 1125,   97,   94,
 /*   760 */   102,  324,  347,  399,  139,  391,  392,  393,   88,  395,
 /*   770 */   275,  342,  394,  396,  397,  170,  968,  295,  967,  250,
 /*   780 */   294,  251, 1035,  122,  123, 1034,  298, 1012,  297,  966,
 /*   790 */   949,  948,  302,   11,  307,  816,  337,  309,  100,  204,
 /*   800 */   159,  203, 1057,  202,  206,  205,  208, 1011,  207,  160,
 /*   810 */  1003,    2,  161, 1094,  336, 1002,  183,  162,  995, 1058,
 /*   820 */   189,  187,  184,  186,   59,    4,  994,   58,  103,  848,
 /*   830 */    80,  846, 1104,  842,  845,  841,   81,  178,  850,  176,
 /*   840 */   266,  861,  177,   22,  855,  104,   69,  857,  105,  328,
 /*   850 */   366,  332,   23,   70,  111,   12,   55,  339,  116,  114,
 /*   860 */   119,   56,   62,  708,  743,  741,  740,  118,  739,   63,
 /*   870 */   120,  737,  736,  735,  732,  358,  698,  124,    7,  940,
 /*   880 */   938,  912,  941,  911,  939,    8,  370,  127,  914,   78,
 /*   890 */   369,   66,  129,  810,   79,  133,  780,  779,  135,  776,
 /*   900 */   724,  722,  714,  720,  809,  716,  718,  712,  710,  746,
 /*   910 */   745,  744,  742,  738,  734,  733,  211,  696,  661,  973,
 /*   920 */   670,  408,  668,  972,  972,  972,  972,  972,  972,  972,
 /*   930 */   972,  972,  972,  972,  410,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   280,    1,  209,  209,  288,  209,  290,  291,    1,    9,
 /*    10 */   290,    1,  292,  220,   14,   15,    9,   17,   18,  209,
 /*    20 */   210,   21,   22,   23,   24,  262,  209,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */   255,  263,   42,   43,   44,   14,   15,  262,   17,   18,
 /*    50 */   254,    5,   21,   22,   23,   24,  278,  261,   27,   28,
 /*    60 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*    70 */    39,  254,  259,   42,   43,   44,  282,  283,  261,   14,
 /*    80 */    15,    5,   17,   18,   38,   39,   21,   22,   23,   24,
 /*    90 */   277,   91,   27,   28,   29,   30,   31,   32,   33,   34,
 /*   100 */    35,   36,   37,   38,   39,  280,  280,   42,   43,   44,
 /*   110 */    14,   15,   87,   17,   18,  290,  290,   21,   22,   23,
 /*   120 */    24,  209,   91,   27,   28,   29,   30,   31,   32,   33,
 /*   130 */    34,   35,   36,   37,   38,   39,  255,  102,   42,   43,
 /*   140 */    44,   14,   15,  262,   17,   18,  121,  122,   21,   22,
 /*   150 */    23,   24,   89,   88,   27,   28,   29,   30,   31,   32,
 /*   160 */    33,   34,   35,   36,   37,   38,   39,  151,  256,   42,
 /*   170 */    43,   44,   15,  280,   17,   18,  160,  161,   21,   22,
 /*   180 */    23,   24,  147,  290,   27,   28,   29,   30,   31,   32,
 /*   190 */    33,   34,   35,   36,   37,   38,   39,    1,   83,   42,
 /*   200 */    43,   44,   17,   18,  255,    9,   21,   22,   23,   24,
 /*   210 */    95,  262,   27,   28,   29,   30,   31,   32,   33,   34,
 /*   220 */    35,   36,   37,   38,   39,  133,  134,   42,   43,   44,
 /*   230 */    95,  103,  104,  105,  106,  107,  108,  109,  110,  111,
 /*   240 */   112,  113,  114,  115,  116,  117,   50,   51,   52,   53,
 /*   250 */    54,   55,   56,   57,   58,   59,   60,   61,   62,   63,
 /*   260 */    64,   87,   66,  233,  234,  235,  236,  237,  238,  239,
 /*   270 */   240,  241,  242,  243,  244,  245,  246,  247,   49,    5,
 /*   280 */     1,    2,   25,    9,    5,  102,    7,  267,    9,   67,
 /*   290 */    68,   69,   67,   68,   69,   66,   74,  285,   76,   77,
 /*   300 */    78,  127,   73,  209,  216,   48,  286,   85,   79,   80,
 /*   310 */    81,   82,   38,   39,  220,   86,   87,   38,   39,  209,
 /*   320 */   280,   42,   65,    1,    2,  259,   47,    5,  145,    7,
 /*   330 */   290,    9,   29,   30,   31,   32,   33,   34,   35,   36,
 /*   340 */    37,   38,   39,  277,   87,   42,   43,   44,  209,  217,
 /*   350 */     2,    1,  280,    5,  217,    7,  127,    9,  285,    9,
 /*   360 */    38,   39,  290,   67,   68,   69,   87,  262,  280,   47,
 /*   370 */    74,   75,   76,   77,   78,   70,   71,   72,  290,  150,
 /*   380 */   292,  152,  250,  251,  252,  253,   38,   39,  159,   93,
 /*   390 */   253,  281,  163,  254,    0,  287,  288,   73,  290,  125,
 /*   400 */   261,  215,  249,  124,  125,  126,   73,   87,  222,   87,
 /*   410 */     5,  216,    7,   33,   34,   35,   36,   37,   38,   39,
 /*   420 */   280,  280,   42,   43,   44,   67,   68,   69,  149,  216,
 /*   430 */   290,  290,   74,   75,   76,   77,   78,   87,  103,  119,
 /*   440 */   105,  106,  207,  208,   88,  110,  124,  125,  126,  114,
 /*   450 */   209,  116,  117,  233,  209,  235,  236,   38,   39,  209,
 /*   460 */   240,   42,   43,   44,  244,  209,  246,  247,  209,  209,
 /*   470 */   209,  149,  124,  125,  209,  280,    5,  153,    7,  155,
 /*   480 */    81,  157,  158,   89,  128,  290,  153,  292,  155,  209,
 /*   490 */   157,  158,   48,  280,  259,  254,   42,   43,   44,  254,
 /*   500 */   220,   42,  261,  290,  254,  292,  261,  217,  280,   65,
 /*   510 */   254,  261,  277,  254,  254,  254,  209,  261,  290,  254,
 /*   520 */   261,  261,  261,  280,  280,  209,  261,  280,  280,  280,
 /*   530 */    16,  126,  280,  290,  290,  280,  220,  290,  290,  290,
 /*   540 */   141,  251,  290,  280,  280,  290,  280,  280,  280,  280,
 /*   550 */   280,  280,  280,  290,  290,  280,  290,  290,  290,  290,
 /*   560 */   290,  290,  290,  280,  280,  290,  102,  280,  261,  288,
 /*   570 */   288,  290,  290,  290,  290,  215,  215,  290,  231,  232,
 /*   580 */   218,  219,  222,  222,  213,  214,   88,   88,   88,  130,
 /*   590 */    38,   39,   88,   88,   88,  131,   88,  126,   88,   88,
 /*   600 */   102,   88,  102,   87,    1,   88,  102,  102,  102,   88,
 /*   610 */   102,   25,  102,  102,   88,  102,  154,  262,  156,  102,
 /*   620 */   154,  249,  156,  102,  154,  249,  156,  128,  102,    5,
 /*   630 */     5,    7,    7,  154,   48,  156,  120,  249,  124,   38,
 /*   640 */    39,  154,  154,  156,  156,   42,   83,   84,  257,  249,
 /*   650 */   249,  249,  209,  279,  209,  209,  209,  209,  209,  209,
 /*   660 */   209,  209,  259,  259,  209,  209,  209,  209,  289,  259,
 /*   670 */   289,  289,  289,  264,  209,   65,  209,  209,  209,  265,
 /*   680 */    89,  209,  209,  209,  209,  209,  209,  209,  126,  209,
 /*   690 */   209,  209,  209,  209,  209,  209,  148,  284,  209,  209,
 /*   700 */   209,  209,  209,  209,  209,  209,  209,  284,  284,  284,
 /*   710 */   209,  209,  209,  144,  276,  275,  209,  146,  209,  139,
 /*   720 */   143,  209,  137,  142,  209,  136,  273,  209,  209,  209,
 /*   730 */   270,  209,  209,  209,  209,  274,  209,  209,  209,  209,
 /*   740 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   750 */   209,  135,  138,  211,  132,  212,  211,  211,  211,  123,
 /*   760 */   211,  211,   94,  118,  101,  100,   56,   97,  211,   60,
 /*   770 */   211,  211,   99,   98,   96,  131,    5,    5,    5,  211,
 /*   780 */   162,  211,  221,  217,  217,  221,    5,  211,  162,    5,
 /*   790 */   105,  104,  151,   87,  128,   88,  120,  102,  129,  224,
 /*   800 */   212,  228,  230,  229,  225,  227,  223,  211,  226,  212,
 /*   810 */   211,  218,  212,  248,  258,  211,  272,  212,  211,  232,
 /*   820 */   265,  268,  271,  269,  266,  213,  211,   87,  102,   88,
 /*   830 */   102,  126,  248,    5,  126,    5,   87,  102,   88,   87,
 /*   840 */     1,   88,   87,  140,   88,   87,  102,   88,   87,   87,
 /*   850 */    48,    1,  140,  102,   91,   87,   87,  120,   83,  121,
 /*   860 */    75,   87,   92,    5,    9,    5,    5,   91,    5,   92,
 /*   870 */    91,    5,    5,    5,    5,   16,   90,   83,   87,    9,
 /*   880 */     9,   88,    9,   88,    9,   87,   64,  156,  124,   17,
 /*   890 */    28,  102,  156,  126,   17,  156,    5,    5,  156,   88,
 /*   900 */     5,    5,    5,    5,  126,    5,    5,    5,    5,    5,
 /*   910 */     5,    5,    5,    5,    5,    5,  102,   90,   65,    0,
 /*   920 */     9,   22,    9,  293,  293,  293,  293,  293,  293,  293,
 /*   930 */   293,  293,  293,  293,   22,  293,  293,  293,  293,  293,
 /*   940 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   950 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   960 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   970 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   980 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   990 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1000 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1010 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1020 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1030 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1040 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1050 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1060 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1070 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1080 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1090 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1100 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1110 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1120 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1130 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1140 */   293,
};
#define YY_SHIFT_COUNT    (413)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (919)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   229,  128,  128,  335,  335,   63,  279,  322,  322,  322,
 /*    10 */   350,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*    20 */     7,    7,   10,   10,    0,  196,  322,  322,  322,  322,
 /*    30 */   322,  322,  322,  322,  322,  322,  322,  322,  322,  322,
 /*    40 */   322,  322,  322,  322,  322,  322,  322,  322,  322,  322,
 /*    50 */   322,  322,  322,  322,  348,  348,  348,  174,  174,   92,
 /*    60 */     7,  394,    7,    7,    7,    7,    7,  115,   63,   10,
 /*    70 */    10,  135,  135,   76,  935,  935,  935,  348,  348,  348,
 /*    80 */   274,  274,   46,   46,   46,   46,   46,   46,   25,   46,
 /*    90 */     7,    7,    7,    7,    7,    7,  459,    7,    7,    7,
 /*   100 */   174,  174,    7,    7,    7,    7,  399,  399,  399,  399,
 /*   110 */   464,  174,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   120 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   130 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   140 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   150 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   160 */     7,    7,    7,    7,    7,    7,    7,    7,  548,  610,
 /*   170 */   591,  610,  610,  610,  610,  562,  562,  562,  562,  610,
 /*   180 */   569,  571,  580,  577,  581,  585,  589,  616,  614,  622,
 /*   190 */   548,  636,  610,  610,  610,  668,  668,  645,   63,   63,
 /*   200 */   610,  610,  663,  665,  710,  670,  673,  709,  675,  678,
 /*   210 */   645,   76,  610,  610,  591,  591,  610,  591,  610,  591,
 /*   220 */   610,  610,  935,  935,   31,   65,   96,   96,   96,  127,
 /*   230 */   157,  185,  303,  303,  303,  303,  303,  303,  380,  380,
 /*   240 */   380,  380,  296,  222,  358,  419,  419,  419,  419,  419,
 /*   250 */   324,  333,  257,   16,  454,  454,  405,  471,  305,  225,
 /*   260 */   498,  356,  499,  552,  500,  504,  505,  444,   35,  183,
 /*   270 */   506,  508,  510,  511,  513,  516,  517,  521,  586,  603,
 /*   280 */   514,  526,  462,  466,  470,  624,  625,  601,  479,  487,
 /*   290 */   320,  488,  563,  644,  771,  618,  772,  773,  626,  781,
 /*   300 */   784,  685,  687,  641,  666,  676,  706,  669,  707,  740,
 /*   310 */   695,  726,  741,  728,  705,  708,  828,  830,  749,  750,
 /*   320 */   752,  753,  755,  756,  735,  758,  759,  761,  839,  762,
 /*   330 */   744,  703,  802,  850,  751,  712,  763,  768,  676,  769,
 /*   340 */   737,  774,  738,  775,  770,  776,  785,  858,  777,  779,
 /*   350 */   855,  860,  861,  863,  866,  867,  868,  869,  786,  859,
 /*   360 */   794,  870,  871,  791,  793,  795,  873,  875,  764,  798,
 /*   370 */   862,  822,  872,  731,  736,  789,  789,  789,  789,  767,
 /*   380 */   778,  877,  739,  742,  789,  789,  789,  891,  892,  811,
 /*   390 */   789,  895,  896,  897,  898,  900,  901,  902,  903,  904,
 /*   400 */   905,  906,  907,  908,  909,  910,  814,  827,  911,  899,
 /*   410 */   913,  912,  853,  919,
};
#define YY_REDUCE_COUNT (223)
#define YY_REDUCE_MIN   (-284)
#define YY_REDUCE_MAX   (615)
static const short yy_reduce_ofst[] = {
 /*     0 */   235,   30,   30,  220,  220,  132,   88,  195,  213, -280,
 /*    10 */  -206, -204, -183,  139,  241,  245,  250,  256,  259,  260,
 /*    20 */   261,  265, -284,  108,  110, -190, -175, -174, -107,   40,
 /*    30 */    72,  140,  141,  228,  243,  244,  247,  248,  249,  252,
 /*    40 */   255,  263,  264,  266,  267,  268,  269,  270,  271,  272,
 /*    50 */   275,  283,  284,  287, -215, -119,  -51, -187,   66,   20,
 /*    60 */   -88,  137, -207,   94,  280,  316,  307,  186,  290,  281,
 /*    70 */   282,  360,  361,  347, -222,  362,  371, -237,  105,  355,
 /*    80 */    12,   73,  153,  372,  376,  388,  400,  401,  391,  402,
 /*    90 */   443,  445,  446,  447,  448,  449,  374,  450,  451,  452,
 /*   100 */   403,  404,  455,  456,  457,  458,  379,  381,  382,  383,
 /*   110 */   409,  410,  465,  467,  468,  469,  472,  473,  474,  475,
 /*   120 */   476,  477,  478,  480,  481,  482,  483,  484,  485,  486,
 /*   130 */   489,  490,  491,  492,  493,  494,  495,  496,  497,  501,
 /*   140 */   502,  503,  507,  509,  512,  515,  518,  519,  520,  522,
 /*   150 */   523,  524,  525,  527,  528,  529,  530,  531,  532,  533,
 /*   160 */   534,  535,  536,  537,  538,  539,  540,  541,  414,  542,
 /*   170 */   543,  545,  546,  547,  549,  413,  423,  424,  425,  550,
 /*   180 */   438,  440,  461,  453,  544,  551,  460,  554,  553,  558,
 /*   190 */   555,  556,  557,  559,  560,  561,  564,  565,  566,  567,
 /*   200 */   568,  570,  572,  574,  573,  575,  578,  579,  582,  583,
 /*   210 */   584,  587,  576,  596,  588,  597,  599,  600,  604,  605,
 /*   220 */   607,  615,  593,  612,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   970, 1093, 1032, 1103, 1019, 1029, 1277, 1277, 1277, 1277,
 /*    10 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*    20 */   970,  970,  970,  970, 1157,  990,  970,  970,  970,  970,
 /*    30 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*    40 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*    50 */   970,  970,  970,  970,  970,  970,  970,  970,  970, 1181,
 /*    60 */   970, 1029,  970,  970,  970,  970,  970, 1039, 1029,  970,
 /*    70 */   970, 1039, 1039,  970, 1152, 1077, 1095,  970,  970,  970,
 /*    80 */   970,  970,  970,  970,  970,  970,  970,  970, 1124,  970,
 /*    90 */   970,  970,  970,  970,  970,  970, 1159, 1165, 1162,  970,
 /*   100 */   970,  970, 1167,  970,  970,  970, 1203, 1203, 1203, 1203,
 /*   110 */  1150,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   120 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   130 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   140 */   970,  970,  970,  970, 1017,  970, 1015,  970,  970,  970,
 /*   150 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   160 */   970,  970,  970,  970,  970,  970,  970,  988, 1220,  992,
 /*   170 */  1027,  992,  992,  992,  992,  970,  970,  970,  970,  992,
 /*   180 */  1212, 1216, 1193, 1210, 1204, 1188, 1186, 1184, 1192, 1177,
 /*   190 */  1220, 1126,  992,  992,  992, 1037, 1037, 1033, 1029, 1029,
 /*   200 */   992,  992, 1055, 1053, 1051, 1043, 1049, 1045, 1047, 1041,
 /*   210 */  1020,  970,  992,  992, 1027, 1027,  992, 1027,  992, 1027,
 /*   220 */   992,  992, 1077, 1095, 1276,  970, 1221, 1211, 1276,  970,
 /*   230 */  1253, 1252, 1267, 1266, 1265, 1251, 1250, 1249, 1245, 1248,
 /*   240 */  1247, 1246,  970,  970,  970, 1264, 1263, 1261, 1260, 1259,
 /*   250 */   970,  970, 1223,  970, 1255, 1254,  970,  970,  970,  970,
 /*   260 */   970,  970,  970, 1174,  970,  970,  970, 1199, 1217, 1213,
 /*   270 */   970,  970,  970,  970,  970,  970,  970,  970, 1224,  970,
 /*   280 */   970,  970,  970,  970,  970,  970,  970, 1138,  970,  970,
 /*   290 */  1105,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   300 */   970,  970,  970,  970, 1149,  970,  970,  970,  970,  970,
 /*   310 */  1161, 1160,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   320 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   330 */  1205,  970, 1200,  970, 1194,  970,  970,  970, 1117,  970,
 /*   340 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   350 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   360 */   970,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   370 */   970,  970,  970,  970,  970, 1295, 1290, 1291, 1288,  970,
 /*   380 */   970,  970,  970,  970, 1287, 1282, 1283,  970,  970,  970,
 /*   390 */  1280,  970,  970,  970,  970,  970,  970,  970,  970,  970,
 /*   400 */   970,  970,  970,  970,  970,  970, 1061,  970,  970,  999,
 /*   410 */   970,  997,  970,  970,
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
  /*   73 */ "DROP",
  /*   74 */ "TOPIC",
  /*   75 */ "FUNCTION",
  /*   76 */ "DNODE",
  /*   77 */ "USER",
  /*   78 */ "ACCOUNT",
  /*   79 */ "USE",
  /*   80 */ "DESCRIBE",
  /*   81 */ "DESC",
  /*   82 */ "ALTER",
  /*   83 */ "PASS",
  /*   84 */ "PRIVILEGE",
  /*   85 */ "LOCAL",
  /*   86 */ "COMPACT",
  /*   87 */ "LP",
  /*   88 */ "RP",
  /*   89 */ "IF",
  /*   90 */ "EXISTS",
  /*   91 */ "AS",
  /*   92 */ "OUTPUTTYPE",
  /*   93 */ "AGGREGATE",
  /*   94 */ "BUFSIZE",
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
  /*  222 */ "pps",
  /*  223 */ "tseries",
  /*  224 */ "dbs",
  /*  225 */ "streams",
  /*  226 */ "storage",
  /*  227 */ "qtime",
  /*  228 */ "users",
  /*  229 */ "conns",
  /*  230 */ "state",
  /*  231 */ "intitemlist",
  /*  232 */ "intitem",
  /*  233 */ "keep",
  /*  234 */ "cache",
  /*  235 */ "replica",
  /*  236 */ "quorum",
  /*  237 */ "days",
  /*  238 */ "minrows",
  /*  239 */ "maxrows",
  /*  240 */ "blocks",
  /*  241 */ "ctime",
  /*  242 */ "wal",
  /*  243 */ "fsync",
  /*  244 */ "comp",
  /*  245 */ "prec",
  /*  246 */ "update",
  /*  247 */ "cachelast",
  /*  248 */ "partitions",
  /*  249 */ "signed",
  /*  250 */ "create_table_args",
  /*  251 */ "create_stable_args",
  /*  252 */ "create_table_list",
  /*  253 */ "create_from_stable",
  /*  254 */ "columnlist",
  /*  255 */ "tagitemlist",
  /*  256 */ "tagNamelist",
  /*  257 */ "to_opt",
  /*  258 */ "split_opt",
  /*  259 */ "select",
  /*  260 */ "to_split",
  /*  261 */ "column",
  /*  262 */ "tagitem",
  /*  263 */ "selcollist",
  /*  264 */ "from",
  /*  265 */ "where_opt",
  /*  266 */ "range_option",
  /*  267 */ "interval_option",
  /*  268 */ "sliding_opt",
  /*  269 */ "session_option",
  /*  270 */ "windowstate_option",
  /*  271 */ "fill_opt",
  /*  272 */ "groupby_opt",
  /*  273 */ "having_opt",
  /*  274 */ "orderby_opt",
  /*  275 */ "slimit_opt",
  /*  276 */ "limit_opt",
  /*  277 */ "union",
  /*  278 */ "sclp",
  /*  279 */ "distinct",
  /*  280 */ "expr",
  /*  281 */ "as",
  /*  282 */ "tablelist",
  /*  283 */ "sub",
  /*  284 */ "tmvar",
  /*  285 */ "timestamp",
  /*  286 */ "intervalKey",
  /*  287 */ "sortlist",
  /*  288 */ "item",
  /*  289 */ "sortorder",
  /*  290 */ "arrow",
  /*  291 */ "grouplist",
  /*  292 */ "expritem",
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
 /*  61 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  62 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  63 */ "cmd ::= CREATE USER ids PASS ids",
 /*  64 */ "bufsize ::=",
 /*  65 */ "bufsize ::= BUFSIZE INTEGER",
 /*  66 */ "pps ::=",
 /*  67 */ "pps ::= PPS INTEGER",
 /*  68 */ "tseries ::=",
 /*  69 */ "tseries ::= TSERIES INTEGER",
 /*  70 */ "dbs ::=",
 /*  71 */ "dbs ::= DBS INTEGER",
 /*  72 */ "streams ::=",
 /*  73 */ "streams ::= STREAMS INTEGER",
 /*  74 */ "storage ::=",
 /*  75 */ "storage ::= STORAGE INTEGER",
 /*  76 */ "qtime ::=",
 /*  77 */ "qtime ::= QTIME INTEGER",
 /*  78 */ "users ::=",
 /*  79 */ "users ::= USERS INTEGER",
 /*  80 */ "conns ::=",
 /*  81 */ "conns ::= CONNS INTEGER",
 /*  82 */ "state ::=",
 /*  83 */ "state ::= STATE ids",
 /*  84 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  85 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  86 */ "intitemlist ::= intitem",
 /*  87 */ "intitem ::= INTEGER",
 /*  88 */ "keep ::= KEEP intitemlist",
 /*  89 */ "cache ::= CACHE INTEGER",
 /*  90 */ "replica ::= REPLICA INTEGER",
 /*  91 */ "quorum ::= QUORUM INTEGER",
 /*  92 */ "days ::= DAYS INTEGER",
 /*  93 */ "minrows ::= MINROWS INTEGER",
 /*  94 */ "maxrows ::= MAXROWS INTEGER",
 /*  95 */ "blocks ::= BLOCKS INTEGER",
 /*  96 */ "ctime ::= CTIME INTEGER",
 /*  97 */ "wal ::= WAL INTEGER",
 /*  98 */ "fsync ::= FSYNC INTEGER",
 /*  99 */ "comp ::= COMP INTEGER",
 /* 100 */ "prec ::= PRECISION STRING",
 /* 101 */ "update ::= UPDATE INTEGER",
 /* 102 */ "cachelast ::= CACHELAST INTEGER",
 /* 103 */ "partitions ::= PARTITIONS INTEGER",
 /* 104 */ "db_optr ::=",
 /* 105 */ "db_optr ::= db_optr cache",
 /* 106 */ "db_optr ::= db_optr replica",
 /* 107 */ "db_optr ::= db_optr quorum",
 /* 108 */ "db_optr ::= db_optr days",
 /* 109 */ "db_optr ::= db_optr minrows",
 /* 110 */ "db_optr ::= db_optr maxrows",
 /* 111 */ "db_optr ::= db_optr blocks",
 /* 112 */ "db_optr ::= db_optr ctime",
 /* 113 */ "db_optr ::= db_optr wal",
 /* 114 */ "db_optr ::= db_optr fsync",
 /* 115 */ "db_optr ::= db_optr comp",
 /* 116 */ "db_optr ::= db_optr prec",
 /* 117 */ "db_optr ::= db_optr keep",
 /* 118 */ "db_optr ::= db_optr update",
 /* 119 */ "db_optr ::= db_optr cachelast",
 /* 120 */ "topic_optr ::= db_optr",
 /* 121 */ "topic_optr ::= topic_optr partitions",
 /* 122 */ "alter_db_optr ::=",
 /* 123 */ "alter_db_optr ::= alter_db_optr replica",
 /* 124 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 125 */ "alter_db_optr ::= alter_db_optr keep",
 /* 126 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 127 */ "alter_db_optr ::= alter_db_optr comp",
 /* 128 */ "alter_db_optr ::= alter_db_optr update",
 /* 129 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 130 */ "alter_topic_optr ::= alter_db_optr",
 /* 131 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 132 */ "typename ::= ids",
 /* 133 */ "typename ::= ids LP signed RP",
 /* 134 */ "typename ::= ids UNSIGNED",
 /* 135 */ "signed ::= INTEGER",
 /* 136 */ "signed ::= PLUS INTEGER",
 /* 137 */ "signed ::= MINUS INTEGER",
 /* 138 */ "cmd ::= CREATE TABLE create_table_args",
 /* 139 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 140 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 141 */ "cmd ::= CREATE TABLE create_table_list",
 /* 142 */ "create_table_list ::= create_from_stable",
 /* 143 */ "create_table_list ::= create_table_list create_from_stable",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 145 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 146 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 147 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 148 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 149 */ "tagNamelist ::= ids",
 /* 150 */ "create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select",
 /* 151 */ "to_opt ::=",
 /* 152 */ "to_opt ::= TO ids cpxName",
 /* 153 */ "split_opt ::=",
 /* 154 */ "split_opt ::= SPLIT ids",
 /* 155 */ "columnlist ::= columnlist COMMA column",
 /* 156 */ "columnlist ::= column",
 /* 157 */ "column ::= ids typename",
 /* 158 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 159 */ "tagitemlist ::= tagitem",
 /* 160 */ "tagitem ::= INTEGER",
 /* 161 */ "tagitem ::= FLOAT",
 /* 162 */ "tagitem ::= STRING",
 /* 163 */ "tagitem ::= BOOL",
 /* 164 */ "tagitem ::= NULL",
 /* 165 */ "tagitem ::= NOW",
 /* 166 */ "tagitem ::= NOW PLUS VARIABLE",
 /* 167 */ "tagitem ::= NOW MINUS VARIABLE",
 /* 168 */ "tagitem ::= MINUS INTEGER",
 /* 169 */ "tagitem ::= MINUS FLOAT",
 /* 170 */ "tagitem ::= PLUS INTEGER",
 /* 171 */ "tagitem ::= PLUS FLOAT",
 /* 172 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 173 */ "select ::= LP select RP",
 /* 174 */ "union ::= select",
 /* 175 */ "union ::= union UNION ALL select",
 /* 176 */ "cmd ::= union",
 /* 177 */ "select ::= SELECT selcollist",
 /* 178 */ "sclp ::= selcollist COMMA",
 /* 179 */ "sclp ::=",
 /* 180 */ "selcollist ::= sclp distinct expr as",
 /* 181 */ "selcollist ::= sclp STAR",
 /* 182 */ "as ::= AS ids",
 /* 183 */ "as ::= ids",
 /* 184 */ "as ::=",
 /* 185 */ "distinct ::= DISTINCT",
 /* 186 */ "distinct ::=",
 /* 187 */ "from ::= FROM tablelist",
 /* 188 */ "from ::= FROM sub",
 /* 189 */ "sub ::= LP union RP",
 /* 190 */ "sub ::= LP union RP ids",
 /* 191 */ "sub ::= sub COMMA LP union RP ids",
 /* 192 */ "tablelist ::= ids cpxName",
 /* 193 */ "tablelist ::= ids cpxName ids",
 /* 194 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 195 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 196 */ "tmvar ::= VARIABLE",
 /* 197 */ "timestamp ::= INTEGER",
 /* 198 */ "timestamp ::= MINUS INTEGER",
 /* 199 */ "timestamp ::= PLUS INTEGER",
 /* 200 */ "timestamp ::= STRING",
 /* 201 */ "timestamp ::= NOW",
 /* 202 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 203 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 204 */ "range_option ::=",
 /* 205 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 206 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 207 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 208 */ "interval_option ::=",
 /* 209 */ "intervalKey ::= INTERVAL",
 /* 210 */ "intervalKey ::= EVERY",
 /* 211 */ "session_option ::=",
 /* 212 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 213 */ "windowstate_option ::=",
 /* 214 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 215 */ "fill_opt ::=",
 /* 216 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 217 */ "fill_opt ::= FILL LP ID RP",
 /* 218 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 219 */ "sliding_opt ::=",
 /* 220 */ "orderby_opt ::=",
 /* 221 */ "orderby_opt ::= ORDER BY sortlist",
 /* 222 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 223 */ "sortlist ::= sortlist COMMA arrow sortorder",
 /* 224 */ "sortlist ::= item sortorder",
 /* 225 */ "sortlist ::= arrow sortorder",
 /* 226 */ "item ::= ID",
 /* 227 */ "item ::= ID DOT ID",
 /* 228 */ "sortorder ::= ASC",
 /* 229 */ "sortorder ::= DESC",
 /* 230 */ "sortorder ::=",
 /* 231 */ "groupby_opt ::=",
 /* 232 */ "groupby_opt ::= GROUP BY grouplist",
 /* 233 */ "grouplist ::= grouplist COMMA item",
 /* 234 */ "grouplist ::= grouplist COMMA arrow",
 /* 235 */ "grouplist ::= item",
 /* 236 */ "grouplist ::= arrow",
 /* 237 */ "having_opt ::=",
 /* 238 */ "having_opt ::= HAVING expr",
 /* 239 */ "limit_opt ::=",
 /* 240 */ "limit_opt ::= LIMIT signed",
 /* 241 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 242 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 243 */ "slimit_opt ::=",
 /* 244 */ "slimit_opt ::= SLIMIT signed",
 /* 245 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 246 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 247 */ "where_opt ::=",
 /* 248 */ "where_opt ::= WHERE expr",
 /* 249 */ "expr ::= LP expr RP",
 /* 250 */ "expr ::= ID",
 /* 251 */ "expr ::= ID DOT ID",
 /* 252 */ "expr ::= ID DOT STAR",
 /* 253 */ "expr ::= INTEGER",
 /* 254 */ "expr ::= MINUS INTEGER",
 /* 255 */ "expr ::= PLUS INTEGER",
 /* 256 */ "expr ::= FLOAT",
 /* 257 */ "expr ::= MINUS FLOAT",
 /* 258 */ "expr ::= PLUS FLOAT",
 /* 259 */ "expr ::= STRING",
 /* 260 */ "expr ::= NOW",
 /* 261 */ "expr ::= TODAY",
 /* 262 */ "expr ::= VARIABLE",
 /* 263 */ "expr ::= PLUS VARIABLE",
 /* 264 */ "expr ::= MINUS VARIABLE",
 /* 265 */ "expr ::= BOOL",
 /* 266 */ "expr ::= NULL",
 /* 267 */ "expr ::= ID LP exprlist RP",
 /* 268 */ "expr ::= ID LP STAR RP",
 /* 269 */ "expr ::= ID LP expr AS typename RP",
 /* 270 */ "expr ::= expr IS NULL",
 /* 271 */ "expr ::= expr IS NOT NULL",
 /* 272 */ "expr ::= expr LT expr",
 /* 273 */ "expr ::= expr GT expr",
 /* 274 */ "expr ::= expr LE expr",
 /* 275 */ "expr ::= expr GE expr",
 /* 276 */ "expr ::= expr NE expr",
 /* 277 */ "expr ::= expr EQ expr",
 /* 278 */ "expr ::= expr BETWEEN expr AND expr",
 /* 279 */ "expr ::= expr AND expr",
 /* 280 */ "expr ::= expr OR expr",
 /* 281 */ "expr ::= expr PLUS expr",
 /* 282 */ "expr ::= expr MINUS expr",
 /* 283 */ "expr ::= expr STAR expr",
 /* 284 */ "expr ::= expr SLASH expr",
 /* 285 */ "expr ::= expr REM expr",
 /* 286 */ "expr ::= expr BITAND expr",
 /* 287 */ "expr ::= expr BITOR expr",
 /* 288 */ "expr ::= expr BITXOR expr",
 /* 289 */ "expr ::= BITNOT expr",
 /* 290 */ "expr ::= expr LSHIFT expr",
 /* 291 */ "expr ::= expr RSHIFT expr",
 /* 292 */ "expr ::= expr LIKE expr",
 /* 293 */ "expr ::= expr MATCH expr",
 /* 294 */ "expr ::= expr NMATCH expr",
 /* 295 */ "expr ::= ID CONTAINS STRING",
 /* 296 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 297 */ "arrow ::= ID ARROW STRING",
 /* 298 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 299 */ "expr ::= arrow",
 /* 300 */ "expr ::= expr IN LP exprlist RP",
 /* 301 */ "exprlist ::= exprlist COMMA expritem",
 /* 302 */ "exprlist ::= expritem",
 /* 303 */ "expritem ::= expr",
 /* 304 */ "expritem ::=",
 /* 305 */ "cmd ::= RESET QUERY CACHE",
 /* 306 */ "cmd ::= SYNCDB ids REPLICA",
 /* 307 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 308 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 309 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 310 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 311 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 312 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 313 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 314 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 315 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 316 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 317 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 318 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 319 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 320 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 321 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 322 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 323 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 324 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 325 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
 /* 326 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
    case 263: /* selcollist */
    case 278: /* sclp */
{
tSqlExprListDestroy((yypminor->yy367));
}
      break;
    case 231: /* intitemlist */
    case 233: /* keep */
    case 254: /* columnlist */
    case 255: /* tagitemlist */
    case 256: /* tagNamelist */
    case 271: /* fill_opt */
    case 272: /* groupby_opt */
    case 274: /* orderby_opt */
    case 287: /* sortlist */
    case 291: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy367));
}
      break;
    case 252: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy74));
}
      break;
    case 259: /* select */
{
destroySqlNode((yypminor->yy426));
}
      break;
    case 264: /* from */
    case 282: /* tablelist */
    case 283: /* sub */
{
destroyRelationInfo((yypminor->yy480));
}
      break;
    case 265: /* where_opt */
    case 273: /* having_opt */
    case 280: /* expr */
    case 285: /* timestamp */
    case 290: /* arrow */
    case 292: /* expritem */
{
tSqlExprDestroy((yypminor->yy378));
}
      break;
    case 277: /* union */
{
destroyAllSqlNode((yypminor->yy367));
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
  {  208,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  208,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  208,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  221,    0 }, /* (64) bufsize ::= */
  {  221,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  222,    0 }, /* (66) pps ::= */
  {  222,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  223,    0 }, /* (68) tseries ::= */
  {  223,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  224,    0 }, /* (70) dbs ::= */
  {  224,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  225,    0 }, /* (72) streams ::= */
  {  225,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  226,    0 }, /* (74) storage ::= */
  {  226,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  227,    0 }, /* (76) qtime ::= */
  {  227,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  228,    0 }, /* (78) users ::= */
  {  228,   -2 }, /* (79) users ::= USERS INTEGER */
  {  229,    0 }, /* (80) conns ::= */
  {  229,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  230,    0 }, /* (82) state ::= */
  {  230,   -2 }, /* (83) state ::= STATE ids */
  {  215,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  231,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  231,   -1 }, /* (86) intitemlist ::= intitem */
  {  232,   -1 }, /* (87) intitem ::= INTEGER */
  {  233,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  234,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  235,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  236,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  237,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  238,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  239,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  240,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  241,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  242,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  243,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  244,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  245,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  246,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  247,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  248,   -2 }, /* (103) partitions ::= PARTITIONS INTEGER */
  {  218,    0 }, /* (104) db_optr ::= */
  {  218,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  218,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  218,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  218,   -2 }, /* (108) db_optr ::= db_optr days */
  {  218,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  218,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  218,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  218,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  218,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  218,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  218,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  218,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  218,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  218,   -2 }, /* (118) db_optr ::= db_optr update */
  {  218,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  219,   -1 }, /* (120) topic_optr ::= db_optr */
  {  219,   -2 }, /* (121) topic_optr ::= topic_optr partitions */
  {  213,    0 }, /* (122) alter_db_optr ::= */
  {  213,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  213,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  213,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  213,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  213,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  213,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  213,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  214,   -1 }, /* (130) alter_topic_optr ::= alter_db_optr */
  {  214,   -2 }, /* (131) alter_topic_optr ::= alter_topic_optr partitions */
  {  220,   -1 }, /* (132) typename ::= ids */
  {  220,   -4 }, /* (133) typename ::= ids LP signed RP */
  {  220,   -2 }, /* (134) typename ::= ids UNSIGNED */
  {  249,   -1 }, /* (135) signed ::= INTEGER */
  {  249,   -2 }, /* (136) signed ::= PLUS INTEGER */
  {  249,   -2 }, /* (137) signed ::= MINUS INTEGER */
  {  208,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_args */
  {  208,   -3 }, /* (139) cmd ::= CREATE TABLE create_stable_args */
  {  208,   -3 }, /* (140) cmd ::= CREATE STABLE create_stable_args */
  {  208,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_list */
  {  252,   -1 }, /* (142) create_table_list ::= create_from_stable */
  {  252,   -2 }, /* (143) create_table_list ::= create_table_list create_from_stable */
  {  250,   -6 }, /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  251,  -10 }, /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  253,  -10 }, /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  253,  -13 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  256,   -3 }, /* (148) tagNamelist ::= tagNamelist COMMA ids */
  {  256,   -1 }, /* (149) tagNamelist ::= ids */
  {  250,   -7 }, /* (150) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  257,    0 }, /* (151) to_opt ::= */
  {  257,   -3 }, /* (152) to_opt ::= TO ids cpxName */
  {  258,    0 }, /* (153) split_opt ::= */
  {  258,   -2 }, /* (154) split_opt ::= SPLIT ids */
  {  254,   -3 }, /* (155) columnlist ::= columnlist COMMA column */
  {  254,   -1 }, /* (156) columnlist ::= column */
  {  261,   -2 }, /* (157) column ::= ids typename */
  {  255,   -3 }, /* (158) tagitemlist ::= tagitemlist COMMA tagitem */
  {  255,   -1 }, /* (159) tagitemlist ::= tagitem */
  {  262,   -1 }, /* (160) tagitem ::= INTEGER */
  {  262,   -1 }, /* (161) tagitem ::= FLOAT */
  {  262,   -1 }, /* (162) tagitem ::= STRING */
  {  262,   -1 }, /* (163) tagitem ::= BOOL */
  {  262,   -1 }, /* (164) tagitem ::= NULL */
  {  262,   -1 }, /* (165) tagitem ::= NOW */
  {  262,   -3 }, /* (166) tagitem ::= NOW PLUS VARIABLE */
  {  262,   -3 }, /* (167) tagitem ::= NOW MINUS VARIABLE */
  {  262,   -2 }, /* (168) tagitem ::= MINUS INTEGER */
  {  262,   -2 }, /* (169) tagitem ::= MINUS FLOAT */
  {  262,   -2 }, /* (170) tagitem ::= PLUS INTEGER */
  {  262,   -2 }, /* (171) tagitem ::= PLUS FLOAT */
  {  259,  -15 }, /* (172) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  259,   -3 }, /* (173) select ::= LP select RP */
  {  277,   -1 }, /* (174) union ::= select */
  {  277,   -4 }, /* (175) union ::= union UNION ALL select */
  {  208,   -1 }, /* (176) cmd ::= union */
  {  259,   -2 }, /* (177) select ::= SELECT selcollist */
  {  278,   -2 }, /* (178) sclp ::= selcollist COMMA */
  {  278,    0 }, /* (179) sclp ::= */
  {  263,   -4 }, /* (180) selcollist ::= sclp distinct expr as */
  {  263,   -2 }, /* (181) selcollist ::= sclp STAR */
  {  281,   -2 }, /* (182) as ::= AS ids */
  {  281,   -1 }, /* (183) as ::= ids */
  {  281,    0 }, /* (184) as ::= */
  {  279,   -1 }, /* (185) distinct ::= DISTINCT */
  {  279,    0 }, /* (186) distinct ::= */
  {  264,   -2 }, /* (187) from ::= FROM tablelist */
  {  264,   -2 }, /* (188) from ::= FROM sub */
  {  283,   -3 }, /* (189) sub ::= LP union RP */
  {  283,   -4 }, /* (190) sub ::= LP union RP ids */
  {  283,   -6 }, /* (191) sub ::= sub COMMA LP union RP ids */
  {  282,   -2 }, /* (192) tablelist ::= ids cpxName */
  {  282,   -3 }, /* (193) tablelist ::= ids cpxName ids */
  {  282,   -4 }, /* (194) tablelist ::= tablelist COMMA ids cpxName */
  {  282,   -5 }, /* (195) tablelist ::= tablelist COMMA ids cpxName ids */
  {  284,   -1 }, /* (196) tmvar ::= VARIABLE */
  {  285,   -1 }, /* (197) timestamp ::= INTEGER */
  {  285,   -2 }, /* (198) timestamp ::= MINUS INTEGER */
  {  285,   -2 }, /* (199) timestamp ::= PLUS INTEGER */
  {  285,   -1 }, /* (200) timestamp ::= STRING */
  {  285,   -1 }, /* (201) timestamp ::= NOW */
  {  285,   -3 }, /* (202) timestamp ::= NOW PLUS VARIABLE */
  {  285,   -3 }, /* (203) timestamp ::= NOW MINUS VARIABLE */
  {  266,    0 }, /* (204) range_option ::= */
  {  266,   -6 }, /* (205) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  267,   -4 }, /* (206) interval_option ::= intervalKey LP tmvar RP */
  {  267,   -6 }, /* (207) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  267,    0 }, /* (208) interval_option ::= */
  {  286,   -1 }, /* (209) intervalKey ::= INTERVAL */
  {  286,   -1 }, /* (210) intervalKey ::= EVERY */
  {  269,    0 }, /* (211) session_option ::= */
  {  269,   -7 }, /* (212) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  270,    0 }, /* (213) windowstate_option ::= */
  {  270,   -4 }, /* (214) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  271,    0 }, /* (215) fill_opt ::= */
  {  271,   -6 }, /* (216) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  271,   -4 }, /* (217) fill_opt ::= FILL LP ID RP */
  {  268,   -4 }, /* (218) sliding_opt ::= SLIDING LP tmvar RP */
  {  268,    0 }, /* (219) sliding_opt ::= */
  {  274,    0 }, /* (220) orderby_opt ::= */
  {  274,   -3 }, /* (221) orderby_opt ::= ORDER BY sortlist */
  {  287,   -4 }, /* (222) sortlist ::= sortlist COMMA item sortorder */
  {  287,   -4 }, /* (223) sortlist ::= sortlist COMMA arrow sortorder */
  {  287,   -2 }, /* (224) sortlist ::= item sortorder */
  {  287,   -2 }, /* (225) sortlist ::= arrow sortorder */
  {  288,   -1 }, /* (226) item ::= ID */
  {  288,   -3 }, /* (227) item ::= ID DOT ID */
  {  289,   -1 }, /* (228) sortorder ::= ASC */
  {  289,   -1 }, /* (229) sortorder ::= DESC */
  {  289,    0 }, /* (230) sortorder ::= */
  {  272,    0 }, /* (231) groupby_opt ::= */
  {  272,   -3 }, /* (232) groupby_opt ::= GROUP BY grouplist */
  {  291,   -3 }, /* (233) grouplist ::= grouplist COMMA item */
  {  291,   -3 }, /* (234) grouplist ::= grouplist COMMA arrow */
  {  291,   -1 }, /* (235) grouplist ::= item */
  {  291,   -1 }, /* (236) grouplist ::= arrow */
  {  273,    0 }, /* (237) having_opt ::= */
  {  273,   -2 }, /* (238) having_opt ::= HAVING expr */
  {  276,    0 }, /* (239) limit_opt ::= */
  {  276,   -2 }, /* (240) limit_opt ::= LIMIT signed */
  {  276,   -4 }, /* (241) limit_opt ::= LIMIT signed OFFSET signed */
  {  276,   -4 }, /* (242) limit_opt ::= LIMIT signed COMMA signed */
  {  275,    0 }, /* (243) slimit_opt ::= */
  {  275,   -2 }, /* (244) slimit_opt ::= SLIMIT signed */
  {  275,   -4 }, /* (245) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  275,   -4 }, /* (246) slimit_opt ::= SLIMIT signed COMMA signed */
  {  265,    0 }, /* (247) where_opt ::= */
  {  265,   -2 }, /* (248) where_opt ::= WHERE expr */
  {  280,   -3 }, /* (249) expr ::= LP expr RP */
  {  280,   -1 }, /* (250) expr ::= ID */
  {  280,   -3 }, /* (251) expr ::= ID DOT ID */
  {  280,   -3 }, /* (252) expr ::= ID DOT STAR */
  {  280,   -1 }, /* (253) expr ::= INTEGER */
  {  280,   -2 }, /* (254) expr ::= MINUS INTEGER */
  {  280,   -2 }, /* (255) expr ::= PLUS INTEGER */
  {  280,   -1 }, /* (256) expr ::= FLOAT */
  {  280,   -2 }, /* (257) expr ::= MINUS FLOAT */
  {  280,   -2 }, /* (258) expr ::= PLUS FLOAT */
  {  280,   -1 }, /* (259) expr ::= STRING */
  {  280,   -1 }, /* (260) expr ::= NOW */
  {  280,   -1 }, /* (261) expr ::= TODAY */
  {  280,   -1 }, /* (262) expr ::= VARIABLE */
  {  280,   -2 }, /* (263) expr ::= PLUS VARIABLE */
  {  280,   -2 }, /* (264) expr ::= MINUS VARIABLE */
  {  280,   -1 }, /* (265) expr ::= BOOL */
  {  280,   -1 }, /* (266) expr ::= NULL */
  {  280,   -4 }, /* (267) expr ::= ID LP exprlist RP */
  {  280,   -4 }, /* (268) expr ::= ID LP STAR RP */
  {  280,   -6 }, /* (269) expr ::= ID LP expr AS typename RP */
  {  280,   -3 }, /* (270) expr ::= expr IS NULL */
  {  280,   -4 }, /* (271) expr ::= expr IS NOT NULL */
  {  280,   -3 }, /* (272) expr ::= expr LT expr */
  {  280,   -3 }, /* (273) expr ::= expr GT expr */
  {  280,   -3 }, /* (274) expr ::= expr LE expr */
  {  280,   -3 }, /* (275) expr ::= expr GE expr */
  {  280,   -3 }, /* (276) expr ::= expr NE expr */
  {  280,   -3 }, /* (277) expr ::= expr EQ expr */
  {  280,   -5 }, /* (278) expr ::= expr BETWEEN expr AND expr */
  {  280,   -3 }, /* (279) expr ::= expr AND expr */
  {  280,   -3 }, /* (280) expr ::= expr OR expr */
  {  280,   -3 }, /* (281) expr ::= expr PLUS expr */
  {  280,   -3 }, /* (282) expr ::= expr MINUS expr */
  {  280,   -3 }, /* (283) expr ::= expr STAR expr */
  {  280,   -3 }, /* (284) expr ::= expr SLASH expr */
  {  280,   -3 }, /* (285) expr ::= expr REM expr */
  {  280,   -3 }, /* (286) expr ::= expr BITAND expr */
  {  280,   -3 }, /* (287) expr ::= expr BITOR expr */
  {  280,   -3 }, /* (288) expr ::= expr BITXOR expr */
  {  280,   -2 }, /* (289) expr ::= BITNOT expr */
  {  280,   -3 }, /* (290) expr ::= expr LSHIFT expr */
  {  280,   -3 }, /* (291) expr ::= expr RSHIFT expr */
  {  280,   -3 }, /* (292) expr ::= expr LIKE expr */
  {  280,   -3 }, /* (293) expr ::= expr MATCH expr */
  {  280,   -3 }, /* (294) expr ::= expr NMATCH expr */
  {  280,   -3 }, /* (295) expr ::= ID CONTAINS STRING */
  {  280,   -5 }, /* (296) expr ::= ID DOT ID CONTAINS STRING */
  {  290,   -3 }, /* (297) arrow ::= ID ARROW STRING */
  {  290,   -5 }, /* (298) arrow ::= ID DOT ID ARROW STRING */
  {  280,   -1 }, /* (299) expr ::= arrow */
  {  280,   -5 }, /* (300) expr ::= expr IN LP exprlist RP */
  {  216,   -3 }, /* (301) exprlist ::= exprlist COMMA expritem */
  {  216,   -1 }, /* (302) exprlist ::= expritem */
  {  292,   -1 }, /* (303) expritem ::= expr */
  {  292,    0 }, /* (304) expritem ::= */
  {  208,   -3 }, /* (305) cmd ::= RESET QUERY CACHE */
  {  208,   -3 }, /* (306) cmd ::= SYNCDB ids REPLICA */
  {  208,   -7 }, /* (307) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (308) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (309) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (310) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (311) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (312) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (313) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (314) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -7 }, /* (315) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (316) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (317) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (318) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (319) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (320) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (321) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (322) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -3 }, /* (323) cmd ::= KILL CONNECTION INTEGER */
  {  208,   -5 }, /* (324) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  208,   -5 }, /* (325) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  208,   -6 }, /* (326) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
      case 138: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==138);
      case 139: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==139);
      case 140: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==140);
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy564, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy563);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy563);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy367);}
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
      case 186: /* distinct ::= */ yytestcase(yyruleno==186);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy563);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy564, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy307, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy307, &yymsp[0].minor.yy0, 2);}
        break;
      case 63: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 64: /* bufsize ::= */
      case 66: /* pps ::= */ yytestcase(yyruleno==66);
      case 68: /* tseries ::= */ yytestcase(yyruleno==68);
      case 70: /* dbs ::= */ yytestcase(yyruleno==70);
      case 72: /* streams ::= */ yytestcase(yyruleno==72);
      case 74: /* storage ::= */ yytestcase(yyruleno==74);
      case 76: /* qtime ::= */ yytestcase(yyruleno==76);
      case 78: /* users ::= */ yytestcase(yyruleno==78);
      case 80: /* conns ::= */ yytestcase(yyruleno==80);
      case 82: /* state ::= */ yytestcase(yyruleno==82);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 65: /* bufsize ::= BUFSIZE INTEGER */
      case 67: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==69);
      case 71: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==75);
      case 77: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==77);
      case 79: /* users ::= USERS INTEGER */ yytestcase(yyruleno==79);
      case 81: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==81);
      case 83: /* state ::= STATE ids */ yytestcase(yyruleno==83);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 84: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy563.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy563.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy563.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy563.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy563.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy563.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy563.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy563.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy563.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy563 = yylhsminor.yy563;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 158: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==158);
{ yylhsminor.yy367 = tVariantListAppend(yymsp[-2].minor.yy367, &yymsp[0].minor.yy410, -1);    }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 86: /* intitemlist ::= intitem */
      case 159: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==159);
{ yylhsminor.yy367 = tVariantListAppend(NULL, &yymsp[0].minor.yy410, -1); }
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 87: /* intitem ::= INTEGER */
      case 160: /* tagitem ::= INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= FLOAT */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= STRING */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= BOOL */ yytestcase(yyruleno==163);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy367 = yymsp[0].minor.yy367; }
        break;
      case 89: /* cache ::= CACHE INTEGER */
      case 90: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==90);
      case 91: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==91);
      case 92: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==92);
      case 93: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==96);
      case 97: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==97);
      case 98: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==98);
      case 99: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==99);
      case 100: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==100);
      case 101: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==101);
      case 102: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==102);
      case 103: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==103);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 104: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy564); yymsp[1].minor.yy564.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.keep = yymsp[0].minor.yy367; }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy564 = yymsp[0].minor.yy564; yylhsminor.yy564.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy564 = yylhsminor.yy564;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy564); yymsp[1].minor.yy564.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy307, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy307 = yylhsminor.yy307;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy443 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy307, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy443;  // negative value of name length
    tSetColumnType(&yylhsminor.yy307, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy307 = yylhsminor.yy307;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy307, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy307 = yylhsminor.yy307;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy443 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy443 = yylhsminor.yy443;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy443 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy443 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy74;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy110);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy74 = pCreateTable;
}
  yymsp[0].minor.yy74 = yylhsminor.yy74;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy74->childTableInfo, &yymsp[0].minor.yy110);
  yylhsminor.yy74 = yymsp[-1].minor.yy74;
}
  yymsp[-1].minor.yy74 = yylhsminor.yy74;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy74 = tSetCreateTableInfo(yymsp[-1].minor.yy367, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy74 = yylhsminor.yy74;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy74 = tSetCreateTableInfo(yymsp[-5].minor.yy367, yymsp[-1].minor.yy367, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy74 = yylhsminor.yy74;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy110 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy367, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy110 = yylhsminor.yy110;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy110 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy367, yymsp[-1].minor.yy367, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy110 = yylhsminor.yy110;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy367, &yymsp[0].minor.yy0); yylhsminor.yy367 = yymsp[-2].minor.yy367;  }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy367 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy367, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy74 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy426, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy74 = yylhsminor.yy74;
        break;
      case 151: /* to_opt ::= */
      case 153: /* split_opt ::= */ yytestcase(yyruleno==153);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 152: /* to_opt ::= TO ids cpxName */
{
   yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
   yymsp[-2].minor.yy0.n += yymsp[0].minor.yy0.n;
}
        break;
      case 154: /* split_opt ::= SPLIT ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 155: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy367, &yymsp[0].minor.yy307); yylhsminor.yy367 = yymsp[-2].minor.yy367;  }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 156: /* columnlist ::= column */
{yylhsminor.yy367 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy367, &yymsp[0].minor.yy307);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 157: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy307, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy307);
}
  yymsp[-1].minor.yy307 = yylhsminor.yy307;
        break;
      case 164: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 165: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy410, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 166: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy410, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 167: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy410, &yymsp[0].minor.yy0, TK_MINUS, true);
}
        break;
      case 168: /* tagitem ::= MINUS INTEGER */
      case 169: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==169);
      case 170: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==170);
      case 171: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==171);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy410, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy410 = yylhsminor.yy410;
        break;
      case 172: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy426 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy367, yymsp[-12].minor.yy480, yymsp[-11].minor.yy378, yymsp[-4].minor.yy367, yymsp[-2].minor.yy367, &yymsp[-9].minor.yy478, &yymsp[-7].minor.yy373, &yymsp[-6].minor.yy204, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy367, &yymsp[0].minor.yy24, &yymsp[-1].minor.yy24, yymsp[-3].minor.yy378, &yymsp[-10].minor.yy214);
}
  yymsp[-14].minor.yy426 = yylhsminor.yy426;
        break;
      case 173: /* select ::= LP select RP */
{yymsp[-2].minor.yy426 = yymsp[-1].minor.yy426;}
        break;
      case 174: /* union ::= select */
{ yylhsminor.yy367 = setSubclause(NULL, yymsp[0].minor.yy426); }
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 175: /* union ::= union UNION ALL select */
{ yylhsminor.yy367 = appendSelectClause(yymsp[-3].minor.yy367, yymsp[0].minor.yy426); }
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 176: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy367, NULL, TSDB_SQL_SELECT); }
        break;
      case 177: /* select ::= SELECT selcollist */
{
  yylhsminor.yy426 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy367, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy426 = yylhsminor.yy426;
        break;
      case 178: /* sclp ::= selcollist COMMA */
{yylhsminor.yy367 = yymsp[-1].minor.yy367;}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 179: /* sclp ::= */
      case 220: /* orderby_opt ::= */ yytestcase(yyruleno==220);
{yymsp[1].minor.yy367 = 0;}
        break;
      case 180: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy367 = tSqlExprListAppend(yymsp[-3].minor.yy367, yymsp[-1].minor.yy378,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 181: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy367 = tSqlExprListAppend(yymsp[-1].minor.yy367, pNode, 0, 0);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 182: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 183: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 184: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 185: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 187: /* from ::= FROM tablelist */
      case 188: /* from ::= FROM sub */ yytestcase(yyruleno==188);
{yymsp[-1].minor.yy480 = yymsp[0].minor.yy480;}
        break;
      case 189: /* sub ::= LP union RP */
{yymsp[-2].minor.yy480 = addSubqueryElem(NULL, yymsp[-1].minor.yy367, NULL);}
        break;
      case 190: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy480 = addSubqueryElem(NULL, yymsp[-2].minor.yy367, &yymsp[0].minor.yy0);}
        break;
      case 191: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy480 = addSubqueryElem(yymsp[-5].minor.yy480, yymsp[-2].minor.yy367, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy480 = yylhsminor.yy480;
        break;
      case 192: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 193: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 194: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 195: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(yymsp[-4].minor.yy480, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy480 = yylhsminor.yy480;
        break;
      case 196: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 197: /* timestamp ::= INTEGER */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 198: /* timestamp ::= MINUS INTEGER */
      case 199: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==199);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 200: /* timestamp ::= STRING */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 201: /* timestamp ::= NOW */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 202: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 203: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 204: /* range_option ::= */
{yymsp[1].minor.yy214.start = 0; yymsp[1].minor.yy214.end = 0;}
        break;
      case 205: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy214.start = yymsp[-3].minor.yy378; yymsp[-5].minor.yy214.end = yymsp[-1].minor.yy378;}
        break;
      case 206: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy478.interval = yymsp[-1].minor.yy0; yylhsminor.yy478.offset.n = 0; yylhsminor.yy478.token = yymsp[-3].minor.yy586;}
  yymsp[-3].minor.yy478 = yylhsminor.yy478;
        break;
      case 207: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy478.interval = yymsp[-3].minor.yy0; yylhsminor.yy478.offset = yymsp[-1].minor.yy0;   yylhsminor.yy478.token = yymsp[-5].minor.yy586;}
  yymsp[-5].minor.yy478 = yylhsminor.yy478;
        break;
      case 208: /* interval_option ::= */
{memset(&yymsp[1].minor.yy478, 0, sizeof(yymsp[1].minor.yy478));}
        break;
      case 209: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy586 = TK_INTERVAL;}
        break;
      case 210: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy586 = TK_EVERY;   }
        break;
      case 211: /* session_option ::= */
{yymsp[1].minor.yy373.col.n = 0; yymsp[1].minor.yy373.gap.n = 0;}
        break;
      case 212: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy373.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy373.gap = yymsp[-1].minor.yy0;
}
        break;
      case 213: /* windowstate_option ::= */
{ yymsp[1].minor.yy204.col.n = 0; yymsp[1].minor.yy204.col.z = NULL;}
        break;
      case 214: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy204.col = yymsp[-1].minor.yy0; }
        break;
      case 215: /* fill_opt ::= */
{ yymsp[1].minor.yy367 = 0;     }
        break;
      case 216: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy367, &A, -1, 0);
    yymsp[-5].minor.yy367 = yymsp[-1].minor.yy367;
}
        break;
      case 217: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy367 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 218: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 219: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 221: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy367 = yymsp[0].minor.yy367;}
        break;
      case 222: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-3].minor.yy367, &yymsp[-1].minor.yy410, NULL, false, yymsp[0].minor.yy274);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 223: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-3].minor.yy367, NULL, yymsp[-1].minor.yy378, true, yymsp[0].minor.yy274);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 224: /* sortlist ::= item sortorder */
{
  yylhsminor.yy367 = commonItemAppend(NULL, &yymsp[-1].minor.yy410, NULL, false, yymsp[0].minor.yy274);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 225: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy367 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy378, true, yymsp[0].minor.yy274);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 226: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 227: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy410, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy410 = yylhsminor.yy410;
        break;
      case 228: /* sortorder ::= ASC */
{ yymsp[0].minor.yy274 = TSDB_ORDER_ASC; }
        break;
      case 229: /* sortorder ::= DESC */
{ yymsp[0].minor.yy274 = TSDB_ORDER_DESC;}
        break;
      case 230: /* sortorder ::= */
{ yymsp[1].minor.yy274 = TSDB_ORDER_ASC; }
        break;
      case 231: /* groupby_opt ::= */
{ yymsp[1].minor.yy367 = 0;}
        break;
      case 232: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy367 = yymsp[0].minor.yy367;}
        break;
      case 233: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-2].minor.yy367, &yymsp[0].minor.yy410, NULL, false, -1);
}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 234: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-2].minor.yy367, NULL, yymsp[0].minor.yy378, true, -1);
}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 235: /* grouplist ::= item */
{
  yylhsminor.yy367 = commonItemAppend(NULL, &yymsp[0].minor.yy410, NULL, false, -1);
}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 236: /* grouplist ::= arrow */
{
  yylhsminor.yy367 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy378, true, -1);
}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 237: /* having_opt ::= */
      case 247: /* where_opt ::= */ yytestcase(yyruleno==247);
      case 304: /* expritem ::= */ yytestcase(yyruleno==304);
{yymsp[1].minor.yy378 = 0;}
        break;
      case 238: /* having_opt ::= HAVING expr */
      case 248: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==248);
{yymsp[-1].minor.yy378 = yymsp[0].minor.yy378;}
        break;
      case 239: /* limit_opt ::= */
      case 243: /* slimit_opt ::= */ yytestcase(yyruleno==243);
{yymsp[1].minor.yy24.limit = -1; yymsp[1].minor.yy24.offset = 0;}
        break;
      case 240: /* limit_opt ::= LIMIT signed */
      case 244: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==244);
{yymsp[-1].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-1].minor.yy24.offset = 0;}
        break;
      case 241: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy443;}
        break;
      case 242: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy443;}
        break;
      case 245: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy443;}
        break;
      case 246: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy443;}
        break;
      case 249: /* expr ::= LP expr RP */
{yylhsminor.yy378 = yymsp[-1].minor.yy378; yylhsminor.yy378->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy378->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 250: /* expr ::= ID */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 251: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 252: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 253: /* expr ::= INTEGER */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 254: /* expr ::= MINUS INTEGER */
      case 255: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==255);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 256: /* expr ::= FLOAT */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 257: /* expr ::= MINUS FLOAT */
      case 258: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==258);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 259: /* expr ::= STRING */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 260: /* expr ::= NOW */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 261: /* expr ::= TODAY */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 262: /* expr ::= VARIABLE */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 263: /* expr ::= PLUS VARIABLE */
      case 264: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==264);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 265: /* expr ::= BOOL */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 266: /* expr ::= NULL */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 267: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(yymsp[-1].minor.yy367, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 268: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 269: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy378, &yymsp[-1].minor.yy307, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy378 = yylhsminor.yy378;
        break;
      case 270: /* expr ::= expr IS NULL */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 271: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-3].minor.yy378, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 272: /* expr ::= expr LT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 273: /* expr ::= expr GT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_GT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 274: /* expr ::= expr LE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 275: /* expr ::= expr GE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_GE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 276: /* expr ::= expr NE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 277: /* expr ::= expr EQ expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_EQ);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 278: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy378); yylhsminor.yy378 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy378, yymsp[-2].minor.yy378, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy378, TK_LE), TK_AND);}
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 279: /* expr ::= expr AND expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_AND);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 280: /* expr ::= expr OR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_OR); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 281: /* expr ::= expr PLUS expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_PLUS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 282: /* expr ::= expr MINUS expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MINUS); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 283: /* expr ::= expr STAR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_STAR);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 284: /* expr ::= expr SLASH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_DIVIDE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 285: /* expr ::= expr REM expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_REM);   }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 286: /* expr ::= expr BITAND expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITAND);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 287: /* expr ::= expr BITOR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITOR); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 288: /* expr ::= expr BITXOR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITXOR);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 289: /* expr ::= BITNOT expr */
{yymsp[-1].minor.yy378 = tSqlExprCreate(yymsp[0].minor.yy378, NULL, TK_BITNOT);}
        break;
      case 290: /* expr ::= expr LSHIFT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LSHIFT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 291: /* expr ::= expr RSHIFT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_RSHIFT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 292: /* expr ::= expr LIKE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LIKE);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 293: /* expr ::= expr MATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 294: /* expr ::= expr NMATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NMATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 295: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 296: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 297: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 298: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 299: /* expr ::= arrow */
      case 303: /* expritem ::= expr */ yytestcase(yyruleno==303);
{yylhsminor.yy378 = yymsp[0].minor.yy378;}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 300: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-4].minor.yy378, (tSqlExpr*)yymsp[-1].minor.yy367, TK_IN); }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 301: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy367 = tSqlExprListAppend(yymsp[-2].minor.yy367,yymsp[0].minor.yy378,0, 0);}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 302: /* exprlist ::= expritem */
{yylhsminor.yy367 = tSqlExprListAppend(0,yymsp[0].minor.yy378,0, 0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 305: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 306: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 307: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 313: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 318: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 321: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 322: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 323: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 324: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 325: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      case 326: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n; 
  SDelData * pDelData = tGetDelData(&yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0, yymsp[0].minor.yy378);
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
