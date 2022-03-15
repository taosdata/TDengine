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
#define YYNOCODE 290
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SRelationInfo* yy84;
  SArray* yy161;
  SSessionWindowVal yy219;
  TAOS_FIELD yy223;
  SCreateAcctInfo yy231;
  SSqlNode* yy276;
  SIntervalVal yy300;
  SCreateDbInfo yy302;
  SCreatedTableInfo yy356;
  int64_t yy369;
  SLimitVal yy394;
  SRangeVal yy420;
  int yy452;
  SCreateTableSql* yy462;
  int32_t yy520;
  tVariant yy526;
  tSqlExpr* yy546;
  SWindowStateVal yy548;
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
#define YYNSTATE             398
#define YYNRULE              320
#define YYNRULE_WITH_ACTION  320
#define YYNTOKEN             204
#define YY_MAX_SHIFT         397
#define YY_MIN_SHIFTREDUCE   625
#define YY_MAX_SHIFTREDUCE   944
#define YY_ERROR_ACTION      945
#define YY_ACCEPT_ACTION     946
#define YY_NO_ACTION         947
#define YY_MIN_REDUCE        948
#define YY_MAX_REDUCE        1267
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
#define YY_ACTTAB_COUNT (865)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   105,  676, 1097, 1131,  946,  397,  262,  760,  676,  677,
 /*    10 */  1183,  712, 1184,  314,   37,   38,  677,   41,   42,  396,
 /*    20 */   243,  265,   31,   30,   29, 1089,  163,   40,  347,   45,
 /*    30 */    43,   46,   44, 1086, 1087,   55, 1090,   36,   35,  372,
 /*    40 */   371,   34,   33,   32,   37,   38,  252,   41,   42,  258,
 /*    50 */    85,  265,   31,   30,   29,   24, 1122,   40,  347,   45,
 /*    60 */    43,   46,   44,  318,  100, 1241,   99,   36,   35,  218,
 /*    70 */   214,   34,   33,   32,  288, 1128,  131,  125,  136, 1241,
 /*    80 */  1241, 1243, 1244,  135, 1088,  141,  144,  134,   37,   38,
 /*    90 */    88,   41,   42,   51,  138,  265,   31,   30,   29,  295,
 /*   100 */   294,   40,  347,   45,   43,   46,   44,  343,   34,   33,
 /*   110 */    32,   36,   35,  343,  216,   34,   33,   32,   37,   38,
 /*   120 */    58,   41,   42,   59, 1241,  265,   31,   30,   29,  275,
 /*   130 */   676,   40,  347,   45,   43,   46,   44,  880,  677,  883,
 /*   140 */   185,   36,   35,  676,  217,   34,   33,   32,   13,   37,
 /*   150 */    39,  677,   41,   42, 1241,  382,  265,   31,   30,   29,
 /*   160 */  1106,  874,   40,  347,   45,   43,   46,   44,  245,  395,
 /*   170 */   393,  653,   36,   35,   59, 1104,   34,   33,   32,  209,
 /*   180 */   207,  205,  107,   86,  390, 1034,  204,  151,  150,  149,
 /*   190 */   148,  626,  627,  628,  629,  630,  631,  632,  633,  634,
 /*   200 */   635,  636,  637,  638,  639,  160,  250,  244,   38, 1263,
 /*   210 */    41,   42,  345, 1107,  265,   31,   30,   29,  280,  255,
 /*   220 */    40,  347,   45,   43,   46,   44, 1104,  284,  283,  317,
 /*   230 */    36,   35,    1,  187,   34,   33,   32,  223,   41,   42,
 /*   240 */   268,  178,  265,   31,   30,   29, 1255, 1241,   40,  347,
 /*   250 */    45,   43,   46,   44,  879,  296,  882,  888,   36,   35,
 /*   260 */   304,   94,   34,   33,   32,   67,  341,  389,  388,  340,
 /*   270 */   339,  338,  387,  337,  336,  335,  386,  334,  385,  384,
 /*   280 */    25,   59, 1065, 1053, 1054, 1055, 1056, 1057, 1058, 1059,
 /*   290 */  1060, 1061, 1062, 1063, 1064, 1066, 1067,  222,  224,  237,
 /*   300 */   890,   68,  297,  878,  230,  881,  352,  884, 1241, 1122,
 /*   310 */   147,  146,  145,  229,  181,  237,  890,  355,   94,  878,
 /*   320 */   269,  881,  267,  884,  358,  357,  256,  246,   59,   45,
 /*   330 */    43,   46,   44, 1104,  225,  241,  242,   36,   35,  349,
 /*   340 */   261,   34,   33,   32, 1241,   59,    5,   62,  189, 1194,
 /*   350 */  1122,  241,  242,  188,  114,  119,  110,  118,   68,  788,
 /*   360 */   274, 1233,  785,  257,  786,  108,  787,  266,  247, 1232,
 /*   370 */  1107, 1241,  330,  359,  889,   67, 1193,  389,  388, 1241,
 /*   380 */  1104,  287,  387,   84,   47,  285,  386,  822,  385,  384,
 /*   390 */   238,  825, 1231,  346,  270,  271, 1073, 1103, 1071, 1072,
 /*   400 */    47, 1091, 1241, 1074,  218,   59,   59, 1075,   59, 1076,
 /*   410 */  1077,   59,  161,  895, 1241,  345, 1244,  239,   79,  300,
 /*   420 */   301,  891,  885,  887,   36,   35,   59, 1241,   34,   33,
 /*   430 */    32,  218,  264,  159,  157,  156,  133,  891,  885,  887,
 /*   440 */   276, 1241,  273, 1244,  367,  366,  886,   59,  382,   59,
 /*   450 */   360,  361,  806,  362,    6,  240,  368, 1104, 1104,  220,
 /*   460 */  1104,   80,  886, 1104,  221, 1241,  226,  219,  275, 1241,
 /*   470 */   854,  369,  227,  228, 1241,  232, 1241, 1241, 1104,  186,
 /*   480 */   789,  272, 1241, 1241,   91, 1241,  233,   92,  234,  275,
 /*   490 */   259,  275,  370,  231,  374,  215, 1241, 1107, 1241, 1104,
 /*   500 */   348, 1104, 1105, 1241,  996, 1241,  248,  834,  835, 1006,
 /*   510 */  1181,  199, 1182,  102,  997,  101,  199,  103,    3,  200,
 /*   520 */   289,  199,  803,  831,  291,  299,  298,  291,  841,  842,
 /*   530 */   853,   76,   89,  770,   60,  322,  772,  165,  324,  771,
 /*   540 */   810,   54,   71,   48,  919,  892,  351,   60,  263,   60,
 /*   550 */    71,   10,  106,   71,   15,  675,   14,   83,    9,    9,
 /*   560 */   124,   17,  123,   16,  795,  793,  796,  794,  350,    9,
 /*   570 */   364,  363,  253,   19,  325,   18,   77,  130,   21,  129,
 /*   580 */    20,  143,  142, 1190, 1189,  254,  373,  162,  877,  759,
 /*   590 */  1102, 1130,   26, 1173, 1141, 1138, 1172, 1139, 1123,  292,
 /*   600 */  1143,  164,  169, 1098,  310, 1171, 1170,  180,  182, 1096,
 /*   610 */   183,  184, 1011,  158,  821,  327,  328,  303,  329,  332,
 /*   620 */   333,   69,  212,   65,  344, 1005,  249,  170,  356, 1262,
 /*   630 */   305,  307,  121, 1261, 1258,  190,   81,  365, 1254, 1120,
 /*   640 */   127, 1253,   78, 1250,  191, 1031,   66,  319,  171,   61,
 /*   650 */    70,  213,   28,  993,  315,  173,  137,  309,  313,  991,
 /*   660 */   139,  311,  140,  172,  306,  989,  988,  277,  202,  203,
 /*   670 */   985,  984,  983,  982,  302,  981,  980,   27,  979,  206,
 /*   680 */   208,  971,  210,  968,  211,  964,   87,  331,  290, 1100,
 /*   690 */    90,   95,  308,  383,  376,  132,  375,  377,  378,  379,
 /*   700 */    82,  380,  381,  260,  391,  944,  326,  279,  943,  282,
 /*   710 */   942,  278,  235,  236,  281,  925,  924,  115, 1010, 1009,
 /*   720 */   116,  286,  321,  291,   11,  293,  987,   93,  798,   52,
 /*   730 */    96,  830,  986,  193, 1032,  194,  195,  192,  196,  198,
 /*   740 */   197,  152,  828,  153,  978,  977,  320, 1069,  154, 1033,
 /*   750 */   155,   74,  176,  174,  175,  177,  970,   53,  179,  969,
 /*   760 */  1079,    2,    4,  824,  823,   75,  166,  827,  832,  843,
 /*   770 */   167,  168,  837,   97,  251,  839,   98,  312,   63,  350,
 /*   780 */   316,   12,  104,   49,   22,   23,  323,   64,  107,  109,
 /*   790 */    56,  111,   50,  112,  690,  725,  723,  722,  721,   57,
 /*   800 */   113,  719,  718,  717,  714,  680,  342,  117,    7,  916,
 /*   810 */   914,  894,  917,  893,  915,    8,  896,  354,  120,   72,
 /*   820 */   122,   60,  353,  792,   73,  762,  126,  128,  761,  758,
 /*   830 */   706,  704,  696,  702,  791,  698,  700,  694,  692,  728,
 /*   840 */   727,  726,  724,  720,  716,  715,  201,  643,  948,  678,
 /*   850 */   652,  392,  650,  947,  947,  947,  947,  947,  947,  947,
 /*   860 */   947,  947,  947,  947,  394,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   214,    1,  206,  206,  204,  205,  213,    5,    1,    9,
 /*    10 */   285,    5,  287,  288,   14,   15,    9,   17,   18,  206,
 /*    20 */   207,   21,   22,   23,   24,    0,  206,   27,   28,   29,
 /*    30 */    30,   31,   32,  247,  248,  249,  250,   37,   38,   37,
 /*    40 */    38,   41,   42,   43,   14,   15,    1,   17,   18,  253,
 /*    50 */   214,   21,   22,   23,   24,  277,  256,   27,   28,   29,
 /*    60 */    30,   31,   32,  284,  285,  287,  287,   37,   38,  277,
 /*    70 */   277,   41,   42,   43,  274,  278,   66,   67,   68,  287,
 /*    80 */   287,  289,  289,   73,  248,   75,   76,   77,   14,   15,
 /*    90 */    90,   17,   18,   86,   84,   21,   22,   23,   24,  279,
 /*   100 */   280,   27,   28,   29,   30,   31,   32,   88,   41,   42,
 /*   110 */    43,   37,   38,   88,  277,   41,   42,   43,   14,   15,
 /*   120 */    90,   17,   18,  206,  287,   21,   22,   23,   24,  206,
 /*   130 */     1,   27,   28,   29,   30,   31,   32,    5,    9,    7,
 /*   140 */   217,   37,   38,    1,  277,   41,   42,   43,   86,   14,
 /*   150 */    15,    9,   17,   18,  287,   94,   21,   22,   23,   24,
 /*   160 */   259,   87,   27,   28,   29,   30,   31,   32,  251,   69,
 /*   170 */    70,   71,   37,   38,  206,  258,   41,   42,   43,   66,
 /*   180 */    67,   68,  120,  121,  228,  229,   73,   74,   75,   76,
 /*   190 */    77,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   200 */    58,   59,   60,   61,   62,   63,  252,   65,   15,  259,
 /*   210 */    17,   18,   47,  259,   21,   22,   23,   24,  150,  251,
 /*   220 */    27,   28,   29,   30,   31,   32,  258,  159,  160,   64,
 /*   230 */    37,   38,  215,  216,   41,   42,   43,  277,   17,   18,
 /*   240 */    72,  264,   21,   22,   23,   24,  259,  287,   27,   28,
 /*   250 */    29,   30,   31,   32,    5,  282,    7,  125,   37,   38,
 /*   260 */   283,   86,   41,   42,   43,  102,  103,  104,  105,  106,
 /*   270 */   107,  108,  109,  110,  111,  112,  113,  114,  115,  116,
 /*   280 */    48,  206,  230,  231,  232,  233,  234,  235,  236,  237,
 /*   290 */   238,  239,  240,  241,  242,  243,  244,   65,  277,    1,
 /*   300 */     2,  126,  282,    5,   72,    7,   16,    9,  287,  256,
 /*   310 */    78,   79,   80,   81,  254,    1,    2,   85,   86,    5,
 /*   320 */   152,    7,  154,    9,  156,  157,  251,  274,  206,   29,
 /*   330 */    30,   31,   32,  258,  277,   37,   38,   37,   38,   41,
 /*   340 */   213,   41,   42,   43,  287,  206,   66,   67,   68,  246,
 /*   350 */   256,   37,   38,   73,   74,   75,   76,   77,  126,    2,
 /*   360 */    72,  277,    5,  252,    7,  214,    9,  213,  274,  277,
 /*   370 */   259,  287,   92,  251,  125,  102,  246,  104,  105,  287,
 /*   380 */   258,  149,  109,  151,   86,  206,  113,    5,  115,  116,
 /*   390 */   158,    9,  277,   25,   37,   38,  230,  258,  232,  233,
 /*   400 */    86,  250,  287,  237,  277,  206,  206,  241,  206,  243,
 /*   410 */   244,  206,  206,  123,  287,   47,  289,  277,  101,   37,
 /*   420 */    38,  123,  124,  125,   37,   38,  206,  287,   41,   42,
 /*   430 */    43,  277,   64,   66,   67,   68,   82,  123,  124,  125,
 /*   440 */   152,  287,  154,  289,  156,  157,  148,  206,   94,  206,
 /*   450 */   251,  251,   41,  251,   86,  277,  251,  258,  258,  277,
 /*   460 */   258,  144,  148,  258,  277,  287,  277,  277,  206,  287,
 /*   470 */    80,  251,  277,  277,  287,  277,  287,  287,  258,  217,
 /*   480 */   123,  124,  287,  287,   87,  287,  277,   87,  277,  206,
 /*   490 */   252,  206,  251,  277,  251,  277,  287,  259,  287,  258,
 /*   500 */   217,  258,  217,  287,  212,  287,  124,  132,  133,  212,
 /*   510 */   285,  219,  287,  285,  212,  287,  219,  260,  210,  211,
 /*   520 */    87,  219,  101,   87,  127,   37,   38,  127,   87,   87,
 /*   530 */   140,  101,  275,   87,  101,   87,   87,  101,   87,   87,
 /*   540 */   129,   86,  101,  101,   87,   87,   25,  101,    1,  101,
 /*   550 */   101,  130,  101,  101,  153,   87,  155,   86,  101,  101,
 /*   560 */   153,  153,  155,  155,    5,    5,    7,    7,   47,  101,
 /*   570 */    37,   38,  246,  153,  119,  155,  146,  153,  153,  155,
 /*   580 */   155,   82,   83,  246,  246,  246,  246,  206,   41,  118,
 /*   590 */   206,  206,  276,  286,  206,  206,  286,  206,  256,  256,
 /*   600 */   206,  206,  206,  256,  206,  286,  286,  261,  206,  206,
 /*   610 */   206,  206,  206,   64,  125,  206,  206,  281,  206,  206,
 /*   620 */   206,  206,  206,  206,  206,  206,  281,  272,  206,  206,
 /*   630 */   281,  281,  206,  206,  206,  206,  143,  206,  206,  273,
 /*   640 */   206,  206,  145,  206,  206,  206,  206,  138,  271,  206,
 /*   650 */   206,  206,  142,  206,  141,  269,  206,  134,  136,  206,
 /*   660 */   206,  135,  206,  270,  137,  206,  206,  206,  206,  206,
 /*   670 */   206,  206,  206,  206,  131,  206,  206,  147,  206,  206,
 /*   680 */   206,  206,  206,  206,  206,  206,  122,   93,  208,  208,
 /*   690 */   208,  208,  208,  117,   55,  100,   99,   96,   98,   59,
 /*   700 */   208,   97,   95,  208,   88,    5,  208,    5,    5,    5,
 /*   710 */     5,  161,  208,  208,  161,  104,  103,  214,  218,  218,
 /*   720 */   214,  150,  119,  127,   86,  101,  208,  128,   87,   86,
 /*   730 */   101,   87,  208,  225,  227,  221,  224,  226,  222,  220,
 /*   740 */   223,  209,  125,  209,  208,  208,  255,  245,  209,  229,
 /*   750 */   209,  101,  266,  268,  267,  265,  208,  263,  262,  208,
 /*   760 */   245,  215,  210,    5,    5,   86,   86,  125,   87,   87,
 /*   770 */    86,  101,   87,   86,    1,   87,   86,   86,  101,   47,
 /*   780 */     1,   86,   90,   86,  139,  139,  119,  101,  120,   82,
 /*   790 */    91,   90,   86,   74,    5,    9,    5,    5,    5,   91,
 /*   800 */    90,    5,    5,    5,    5,   89,   16,   82,   86,    9,
 /*   810 */     9,   87,    9,   87,    9,   86,  123,   63,  155,   17,
 /*   820 */   155,  101,   28,  125,   17,    5,  155,  155,    5,   87,
 /*   830 */     5,    5,    5,    5,  125,    5,    5,    5,    5,    5,
 /*   840 */     5,    5,    5,    5,    5,    5,  101,   64,    0,   89,
 /*   850 */     9,   22,    9,  290,  290,  290,  290,  290,  290,  290,
 /*   860 */   290,  290,  290,  290,   22,  290,  290,  290,  290,  290,
 /*   870 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   880 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   890 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   900 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   910 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   920 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   930 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   940 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   950 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   960 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   970 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   980 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   990 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1000 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1010 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1020 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1030 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1040 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1050 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*  1060 */   290,  290,  290,  290,  290,  290,  290,  290,  290,
};
#define YY_SHIFT_COUNT    (397)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (848)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   232,  163,  163,  273,  273,   19,  298,  314,  314,  314,
 /*    10 */     7,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*    20 */   129,  129,   45,   45,    0,  142,  314,  314,  314,  314,
 /*    30 */   314,  314,  314,  314,  314,  314,  314,  314,  314,  314,
 /*    40 */   314,  314,  314,  314,  314,  314,  314,  314,  357,  357,
 /*    50 */   357,  175,  175,  375,  129,   25,  129,  129,  129,  129,
 /*    60 */   129,  354,   19,   45,   45,   61,   61,    6,  865,  865,
 /*    70 */   865,  357,  357,  357,  382,  382,    2,    2,    2,    2,
 /*    80 */     2,    2,   62,    2,  129,  129,  129,  129,  129,  411,
 /*    90 */   129,  129,  129,  175,  175,  129,  129,  129,  129,  390,
 /*   100 */   390,  390,  390,  421,  175,  129,  129,  129,  129,  129,
 /*   110 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   120 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   130 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   140 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   150 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   160 */   129,  549,  549,  549,  549,  489,  489,  489,  489,  549,
 /*   170 */   493,  497,  509,  510,  513,  522,  526,  523,  527,  543,
 /*   180 */   530,  564,  549,  549,  549,  594,  594,  576,   19,   19,
 /*   190 */   549,  549,  595,  597,  639,  601,  600,  640,  604,  607,
 /*   200 */   576,    6,  549,  549,  616,  616,  549,  616,  549,  616,
 /*   210 */   549,  549,  865,  865,   30,   74,  104,  104,  104,  135,
 /*   220 */   193,  221,  280,  300,  300,  300,  300,  300,  300,   10,
 /*   230 */   113,  387,  387,  387,  387,  168,  288,  368,   68,   67,
 /*   240 */    67,  132,  249,  100,  367,  433,  397,  400,  488,  436,
 /*   250 */   441,  442,  165,  430,  317,  446,  448,  449,  451,  452,
 /*   260 */   455,  457,  458,  521,  547,  290,  468,  401,  407,  408,
 /*   270 */   559,  560,  533,  420,  424,  471,  425,  499,  700,  550,
 /*   280 */   702,  703,  553,  704,  705,  611,  613,  571,  596,  603,
 /*   290 */   638,  599,  641,  643,  624,  629,  644,  650,  617,  642,
 /*   300 */   758,  759,  679,  681,  680,  682,  684,  685,  670,  687,
 /*   310 */   688,  690,  773,  691,  677,  645,  732,  779,  686,  646,
 /*   320 */   692,  695,  603,  697,  667,  706,  668,  707,  699,  701,
 /*   330 */   719,  789,  708,  710,  786,  791,  792,  793,  796,  797,
 /*   340 */   798,  799,  716,  790,  725,  800,  801,  722,  724,  726,
 /*   350 */   803,  805,  693,  729,  794,  754,  802,  663,  665,  720,
 /*   360 */   720,  720,  720,  698,  709,  807,  671,  672,  720,  720,
 /*   370 */   720,  820,  823,  742,  720,  825,  826,  827,  828,  830,
 /*   380 */   831,  832,  833,  834,  835,  836,  837,  838,  839,  840,
 /*   390 */   745,  760,  841,  829,  843,  842,  783,  848,
};
#define YY_REDUCE_COUNT (213)
#define YY_REDUCE_MIN   (-275)
#define YY_REDUCE_MAX   (552)
static const short yy_reduce_ofst[] = {
 /*     0 */  -200,   52,   52,  166,  166, -214, -207,  127,  154, -208,
 /*    10 */  -180,  -83,  -32,   75,  122,  199,  200,  202,  205,  220,
 /*    20 */   241,  243, -275, -221, -203, -187, -222, -163, -133,  -40,
 /*    30 */    21,   57,   84,   92,  115,  140,  178,  182,  187,  189,
 /*    40 */   190,  195,  196,  198,  209,  211,  216,  218,  -46,  111,
 /*    50 */   238,   53,   94,  -23, -204,  151,  -77,  262,  283,  285,
 /*    60 */   139,  292, -164,  225,  228,  297,  302,  -44,  257,   17,
 /*    70 */   308,  -99,  -50,  -13,  -27,   20,  103,  130,  326,  337,
 /*    80 */   338,  339,   60,  340,  179,  206,  381,  384,  385,  316,
 /*    90 */   388,  389,  391,  342,  343,  394,  395,  396,  398,  307,
 /*   100 */   310,  319,  320,  346,  347,  402,  403,  404,  405,  406,
 /*   110 */   409,  410,  412,  413,  414,  415,  416,  417,  418,  419,
 /*   120 */   422,  423,  426,  427,  428,  429,  431,  432,  434,  435,
 /*   130 */   437,  438,  439,  440,  443,  444,  445,  447,  450,  453,
 /*   140 */   454,  456,  459,  460,  461,  462,  463,  464,  465,  466,
 /*   150 */   467,  469,  470,  472,  473,  474,  475,  476,  477,  478,
 /*   160 */   479,  480,  481,  482,  483,  336,  345,  349,  350,  484,
 /*   170 */   366,  355,  377,  393,  386,  485,  487,  486,  490,  494,
 /*   180 */   496,  491,  492,  495,  498,  500,  501,  502,  503,  506,
 /*   190 */   504,  505,  507,  511,  508,  514,  512,  516,  517,  519,
 /*   200 */   515,  520,  518,  524,  532,  534,  536,  539,  537,  541,
 /*   210 */   548,  551,  546,  552,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   945, 1068, 1007, 1078,  994, 1004, 1246, 1246, 1246, 1246,
 /*    10 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*    20 */   945,  945,  945,  945, 1132,  965,  945,  945,  945,  945,
 /*    30 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*    40 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*    50 */   945,  945,  945, 1156,  945, 1004,  945,  945,  945,  945,
 /*    60 */   945, 1014, 1004,  945,  945, 1014, 1014,  945, 1127, 1052,
 /*    70 */  1070,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*    80 */   945,  945, 1099,  945,  945,  945,  945,  945,  945, 1134,
 /*    90 */  1140, 1137,  945,  945,  945, 1142,  945,  945,  945, 1178,
 /*   100 */  1178, 1178, 1178, 1125,  945,  945,  945,  945,  945,  945,
 /*   110 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   120 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   130 */   945,  945,  945,  945,  945,  945,  945,  992,  945,  990,
 /*   140 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   150 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   160 */   963,  967,  967,  967,  967,  945,  945,  945,  945,  967,
 /*   170 */  1187, 1191, 1168, 1185, 1179, 1163, 1161, 1159, 1167, 1152,
 /*   180 */  1195, 1101,  967,  967,  967, 1012, 1012, 1008, 1004, 1004,
 /*   190 */   967,  967, 1030, 1028, 1026, 1018, 1024, 1020, 1022, 1016,
 /*   200 */   995,  945,  967,  967, 1002, 1002,  967, 1002,  967, 1002,
 /*   210 */   967,  967, 1052, 1070, 1245,  945, 1196, 1186, 1245,  945,
 /*   220 */  1228, 1227,  945, 1236, 1235, 1234, 1226, 1225, 1224,  945,
 /*   230 */   945, 1220, 1223, 1222, 1221,  945,  945, 1198,  945, 1230,
 /*   240 */  1229,  945,  945,  945,  945,  945,  945,  945, 1149,  945,
 /*   250 */   945,  945, 1174, 1192, 1188,  945,  945,  945,  945,  945,
 /*   260 */   945,  945,  945, 1199,  945,  945,  945,  945,  945,  945,
 /*   270 */   945,  945, 1113,  945,  945, 1080,  945,  945,  945,  945,
 /*   280 */   945,  945,  945,  945,  945,  945,  945,  945, 1124,  945,
 /*   290 */   945,  945,  945,  945, 1136, 1135,  945,  945,  945,  945,
 /*   300 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   310 */   945,  945,  945,  945, 1180,  945, 1175,  945, 1169,  945,
 /*   320 */   945,  945, 1092,  945,  945,  945,  945,  945,  945,  945,
 /*   330 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   340 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   350 */   945,  945,  945,  945,  945,  945,  945,  945,  945, 1264,
 /*   360 */  1259, 1260, 1257,  945,  945,  945,  945,  945, 1256, 1251,
 /*   370 */  1252,  945,  945,  945, 1249,  945,  945,  945,  945,  945,
 /*   380 */   945,  945,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   390 */  1036,  945,  945,  974,  945,  972,  945,  945,
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
  /*   94 */ "PPS",
  /*   95 */ "TSERIES",
  /*   96 */ "DBS",
  /*   97 */ "STORAGE",
  /*   98 */ "QTIME",
  /*   99 */ "CONNS",
  /*  100 */ "STATE",
  /*  101 */ "COMMA",
  /*  102 */ "KEEP",
  /*  103 */ "CACHE",
  /*  104 */ "REPLICA",
  /*  105 */ "QUORUM",
  /*  106 */ "DAYS",
  /*  107 */ "MINROWS",
  /*  108 */ "MAXROWS",
  /*  109 */ "BLOCKS",
  /*  110 */ "CTIME",
  /*  111 */ "WAL",
  /*  112 */ "FSYNC",
  /*  113 */ "COMP",
  /*  114 */ "PRECISION",
  /*  115 */ "UPDATE",
  /*  116 */ "CACHELAST",
  /*  117 */ "PARTITIONS",
  /*  118 */ "UNSIGNED",
  /*  119 */ "TAGS",
  /*  120 */ "USING",
  /*  121 */ "TO",
  /*  122 */ "SPLIT",
  /*  123 */ "NULL",
  /*  124 */ "NOW",
  /*  125 */ "VARIABLE",
  /*  126 */ "SELECT",
  /*  127 */ "UNION",
  /*  128 */ "ALL",
  /*  129 */ "DISTINCT",
  /*  130 */ "FROM",
  /*  131 */ "RANGE",
  /*  132 */ "INTERVAL",
  /*  133 */ "EVERY",
  /*  134 */ "SESSION",
  /*  135 */ "STATE_WINDOW",
  /*  136 */ "FILL",
  /*  137 */ "SLIDING",
  /*  138 */ "ORDER",
  /*  139 */ "BY",
  /*  140 */ "ASC",
  /*  141 */ "GROUP",
  /*  142 */ "HAVING",
  /*  143 */ "LIMIT",
  /*  144 */ "OFFSET",
  /*  145 */ "SLIMIT",
  /*  146 */ "SOFFSET",
  /*  147 */ "WHERE",
  /*  148 */ "TODAY",
  /*  149 */ "RESET",
  /*  150 */ "QUERY",
  /*  151 */ "SYNCDB",
  /*  152 */ "ADD",
  /*  153 */ "COLUMN",
  /*  154 */ "MODIFY",
  /*  155 */ "TAG",
  /*  156 */ "CHANGE",
  /*  157 */ "SET",
  /*  158 */ "KILL",
  /*  159 */ "CONNECTION",
  /*  160 */ "STREAM",
  /*  161 */ "COLON",
  /*  162 */ "ABORT",
  /*  163 */ "AFTER",
  /*  164 */ "ATTACH",
  /*  165 */ "BEFORE",
  /*  166 */ "BEGIN",
  /*  167 */ "CASCADE",
  /*  168 */ "CLUSTER",
  /*  169 */ "CONFLICT",
  /*  170 */ "COPY",
  /*  171 */ "DEFERRED",
  /*  172 */ "DELIMITERS",
  /*  173 */ "DETACH",
  /*  174 */ "EACH",
  /*  175 */ "END",
  /*  176 */ "EXPLAIN",
  /*  177 */ "FAIL",
  /*  178 */ "FOR",
  /*  179 */ "IGNORE",
  /*  180 */ "IMMEDIATE",
  /*  181 */ "INITIALLY",
  /*  182 */ "INSTEAD",
  /*  183 */ "KEY",
  /*  184 */ "OF",
  /*  185 */ "RAISE",
  /*  186 */ "REPLACE",
  /*  187 */ "RESTRICT",
  /*  188 */ "ROW",
  /*  189 */ "STATEMENT",
  /*  190 */ "TRIGGER",
  /*  191 */ "VIEW",
  /*  192 */ "IPTOKEN",
  /*  193 */ "SEMI",
  /*  194 */ "NONE",
  /*  195 */ "PREV",
  /*  196 */ "LINEAR",
  /*  197 */ "IMPORT",
  /*  198 */ "TBNAME",
  /*  199 */ "JOIN",
  /*  200 */ "INSERT",
  /*  201 */ "INTO",
  /*  202 */ "VALUES",
  /*  203 */ "FILE",
  /*  204 */ "program",
  /*  205 */ "cmd",
  /*  206 */ "ids",
  /*  207 */ "dbPrefix",
  /*  208 */ "cpxName",
  /*  209 */ "ifexists",
  /*  210 */ "alter_db_optr",
  /*  211 */ "alter_topic_optr",
  /*  212 */ "acct_optr",
  /*  213 */ "exprlist",
  /*  214 */ "ifnotexists",
  /*  215 */ "db_optr",
  /*  216 */ "topic_optr",
  /*  217 */ "typename",
  /*  218 */ "bufsize",
  /*  219 */ "pps",
  /*  220 */ "tseries",
  /*  221 */ "dbs",
  /*  222 */ "streams",
  /*  223 */ "storage",
  /*  224 */ "qtime",
  /*  225 */ "users",
  /*  226 */ "conns",
  /*  227 */ "state",
  /*  228 */ "intitemlist",
  /*  229 */ "intitem",
  /*  230 */ "keep",
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
  /*  245 */ "partitions",
  /*  246 */ "signed",
  /*  247 */ "create_table_args",
  /*  248 */ "create_stable_args",
  /*  249 */ "create_table_list",
  /*  250 */ "create_from_stable",
  /*  251 */ "columnlist",
  /*  252 */ "tagitemlist",
  /*  253 */ "tagNamelist",
  /*  254 */ "to_opt",
  /*  255 */ "split_opt",
  /*  256 */ "select",
  /*  257 */ "to_split",
  /*  258 */ "column",
  /*  259 */ "tagitem",
  /*  260 */ "selcollist",
  /*  261 */ "from",
  /*  262 */ "where_opt",
  /*  263 */ "range_option",
  /*  264 */ "interval_option",
  /*  265 */ "sliding_opt",
  /*  266 */ "session_option",
  /*  267 */ "windowstate_option",
  /*  268 */ "fill_opt",
  /*  269 */ "groupby_opt",
  /*  270 */ "having_opt",
  /*  271 */ "orderby_opt",
  /*  272 */ "slimit_opt",
  /*  273 */ "limit_opt",
  /*  274 */ "union",
  /*  275 */ "sclp",
  /*  276 */ "distinct",
  /*  277 */ "expr",
  /*  278 */ "as",
  /*  279 */ "tablelist",
  /*  280 */ "sub",
  /*  281 */ "tmvar",
  /*  282 */ "timestamp",
  /*  283 */ "intervalKey",
  /*  284 */ "sortlist",
  /*  285 */ "item",
  /*  286 */ "sortorder",
  /*  287 */ "arrow",
  /*  288 */ "grouplist",
  /*  289 */ "expritem",
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
 /* 286 */ "expr ::= expr LIKE expr",
 /* 287 */ "expr ::= expr MATCH expr",
 /* 288 */ "expr ::= expr NMATCH expr",
 /* 289 */ "expr ::= ID CONTAINS STRING",
 /* 290 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 291 */ "arrow ::= ID ARROW STRING",
 /* 292 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 293 */ "expr ::= arrow",
 /* 294 */ "expr ::= expr IN LP exprlist RP",
 /* 295 */ "exprlist ::= exprlist COMMA expritem",
 /* 296 */ "exprlist ::= expritem",
 /* 297 */ "expritem ::= expr",
 /* 298 */ "expritem ::=",
 /* 299 */ "cmd ::= RESET QUERY CACHE",
 /* 300 */ "cmd ::= SYNCDB ids REPLICA",
 /* 301 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 302 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 303 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 304 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 305 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 306 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 307 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 308 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 309 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 310 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 311 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 312 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 313 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 314 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 315 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 316 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 317 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 318 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 319 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 213: /* exprlist */
    case 260: /* selcollist */
    case 275: /* sclp */
{
tSqlExprListDestroy((yypminor->yy161));
}
      break;
    case 228: /* intitemlist */
    case 230: /* keep */
    case 251: /* columnlist */
    case 252: /* tagitemlist */
    case 253: /* tagNamelist */
    case 268: /* fill_opt */
    case 269: /* groupby_opt */
    case 271: /* orderby_opt */
    case 284: /* sortlist */
    case 288: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy161));
}
      break;
    case 249: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy462));
}
      break;
    case 256: /* select */
{
destroySqlNode((yypminor->yy276));
}
      break;
    case 261: /* from */
    case 279: /* tablelist */
    case 280: /* sub */
{
destroyRelationInfo((yypminor->yy84));
}
      break;
    case 262: /* where_opt */
    case 270: /* having_opt */
    case 277: /* expr */
    case 282: /* timestamp */
    case 287: /* arrow */
    case 289: /* expritem */
{
tSqlExprDestroy((yypminor->yy546));
}
      break;
    case 274: /* union */
{
destroyAllSqlNode((yypminor->yy161));
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
   204,  /* (0) program ::= cmd */
   205,  /* (1) cmd ::= SHOW DATABASES */
   205,  /* (2) cmd ::= SHOW TOPICS */
   205,  /* (3) cmd ::= SHOW FUNCTIONS */
   205,  /* (4) cmd ::= SHOW MNODES */
   205,  /* (5) cmd ::= SHOW DNODES */
   205,  /* (6) cmd ::= SHOW ACCOUNTS */
   205,  /* (7) cmd ::= SHOW USERS */
   205,  /* (8) cmd ::= SHOW MODULES */
   205,  /* (9) cmd ::= SHOW QUERIES */
   205,  /* (10) cmd ::= SHOW CONNECTIONS */
   205,  /* (11) cmd ::= SHOW STREAMS */
   205,  /* (12) cmd ::= SHOW VARIABLES */
   205,  /* (13) cmd ::= SHOW SCORES */
   205,  /* (14) cmd ::= SHOW GRANTS */
   205,  /* (15) cmd ::= SHOW VNODES */
   205,  /* (16) cmd ::= SHOW VNODES ids */
   207,  /* (17) dbPrefix ::= */
   207,  /* (18) dbPrefix ::= ids DOT */
   208,  /* (19) cpxName ::= */
   208,  /* (20) cpxName ::= DOT ids */
   205,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   205,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   205,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   205,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   205,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
   205,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   205,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
   205,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   205,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   205,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   205,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   205,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   205,  /* (33) cmd ::= DROP FUNCTION ids */
   205,  /* (34) cmd ::= DROP DNODE ids */
   205,  /* (35) cmd ::= DROP USER ids */
   205,  /* (36) cmd ::= DROP ACCOUNT ids */
   205,  /* (37) cmd ::= USE ids */
   205,  /* (38) cmd ::= DESCRIBE ids cpxName */
   205,  /* (39) cmd ::= DESC ids cpxName */
   205,  /* (40) cmd ::= ALTER USER ids PASS ids */
   205,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   205,  /* (42) cmd ::= ALTER DNODE ids ids */
   205,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   205,  /* (44) cmd ::= ALTER LOCAL ids */
   205,  /* (45) cmd ::= ALTER LOCAL ids ids */
   205,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   205,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   205,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   205,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   205,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   206,  /* (51) ids ::= ID */
   206,  /* (52) ids ::= STRING */
   209,  /* (53) ifexists ::= IF EXISTS */
   209,  /* (54) ifexists ::= */
   214,  /* (55) ifnotexists ::= IF NOT EXISTS */
   214,  /* (56) ifnotexists ::= */
   205,  /* (57) cmd ::= CREATE DNODE ids */
   205,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   205,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   205,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   205,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   205,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   205,  /* (63) cmd ::= CREATE USER ids PASS ids */
   218,  /* (64) bufsize ::= */
   218,  /* (65) bufsize ::= BUFSIZE INTEGER */
   219,  /* (66) pps ::= */
   219,  /* (67) pps ::= PPS INTEGER */
   220,  /* (68) tseries ::= */
   220,  /* (69) tseries ::= TSERIES INTEGER */
   221,  /* (70) dbs ::= */
   221,  /* (71) dbs ::= DBS INTEGER */
   222,  /* (72) streams ::= */
   222,  /* (73) streams ::= STREAMS INTEGER */
   223,  /* (74) storage ::= */
   223,  /* (75) storage ::= STORAGE INTEGER */
   224,  /* (76) qtime ::= */
   224,  /* (77) qtime ::= QTIME INTEGER */
   225,  /* (78) users ::= */
   225,  /* (79) users ::= USERS INTEGER */
   226,  /* (80) conns ::= */
   226,  /* (81) conns ::= CONNS INTEGER */
   227,  /* (82) state ::= */
   227,  /* (83) state ::= STATE ids */
   212,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   228,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   228,  /* (86) intitemlist ::= intitem */
   229,  /* (87) intitem ::= INTEGER */
   230,  /* (88) keep ::= KEEP intitemlist */
   231,  /* (89) cache ::= CACHE INTEGER */
   232,  /* (90) replica ::= REPLICA INTEGER */
   233,  /* (91) quorum ::= QUORUM INTEGER */
   234,  /* (92) days ::= DAYS INTEGER */
   235,  /* (93) minrows ::= MINROWS INTEGER */
   236,  /* (94) maxrows ::= MAXROWS INTEGER */
   237,  /* (95) blocks ::= BLOCKS INTEGER */
   238,  /* (96) ctime ::= CTIME INTEGER */
   239,  /* (97) wal ::= WAL INTEGER */
   240,  /* (98) fsync ::= FSYNC INTEGER */
   241,  /* (99) comp ::= COMP INTEGER */
   242,  /* (100) prec ::= PRECISION STRING */
   243,  /* (101) update ::= UPDATE INTEGER */
   244,  /* (102) cachelast ::= CACHELAST INTEGER */
   245,  /* (103) partitions ::= PARTITIONS INTEGER */
   215,  /* (104) db_optr ::= */
   215,  /* (105) db_optr ::= db_optr cache */
   215,  /* (106) db_optr ::= db_optr replica */
   215,  /* (107) db_optr ::= db_optr quorum */
   215,  /* (108) db_optr ::= db_optr days */
   215,  /* (109) db_optr ::= db_optr minrows */
   215,  /* (110) db_optr ::= db_optr maxrows */
   215,  /* (111) db_optr ::= db_optr blocks */
   215,  /* (112) db_optr ::= db_optr ctime */
   215,  /* (113) db_optr ::= db_optr wal */
   215,  /* (114) db_optr ::= db_optr fsync */
   215,  /* (115) db_optr ::= db_optr comp */
   215,  /* (116) db_optr ::= db_optr prec */
   215,  /* (117) db_optr ::= db_optr keep */
   215,  /* (118) db_optr ::= db_optr update */
   215,  /* (119) db_optr ::= db_optr cachelast */
   216,  /* (120) topic_optr ::= db_optr */
   216,  /* (121) topic_optr ::= topic_optr partitions */
   210,  /* (122) alter_db_optr ::= */
   210,  /* (123) alter_db_optr ::= alter_db_optr replica */
   210,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   210,  /* (125) alter_db_optr ::= alter_db_optr keep */
   210,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   210,  /* (127) alter_db_optr ::= alter_db_optr comp */
   210,  /* (128) alter_db_optr ::= alter_db_optr update */
   210,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   211,  /* (130) alter_topic_optr ::= alter_db_optr */
   211,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   217,  /* (132) typename ::= ids */
   217,  /* (133) typename ::= ids LP signed RP */
   217,  /* (134) typename ::= ids UNSIGNED */
   246,  /* (135) signed ::= INTEGER */
   246,  /* (136) signed ::= PLUS INTEGER */
   246,  /* (137) signed ::= MINUS INTEGER */
   205,  /* (138) cmd ::= CREATE TABLE create_table_args */
   205,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   205,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   205,  /* (141) cmd ::= CREATE TABLE create_table_list */
   249,  /* (142) create_table_list ::= create_from_stable */
   249,  /* (143) create_table_list ::= create_table_list create_from_stable */
   247,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   248,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   250,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   250,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   253,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   253,  /* (149) tagNamelist ::= ids */
   247,  /* (150) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
   254,  /* (151) to_opt ::= */
   254,  /* (152) to_opt ::= TO ids cpxName */
   255,  /* (153) split_opt ::= */
   255,  /* (154) split_opt ::= SPLIT ids */
   251,  /* (155) columnlist ::= columnlist COMMA column */
   251,  /* (156) columnlist ::= column */
   258,  /* (157) column ::= ids typename */
   252,  /* (158) tagitemlist ::= tagitemlist COMMA tagitem */
   252,  /* (159) tagitemlist ::= tagitem */
   259,  /* (160) tagitem ::= INTEGER */
   259,  /* (161) tagitem ::= FLOAT */
   259,  /* (162) tagitem ::= STRING */
   259,  /* (163) tagitem ::= BOOL */
   259,  /* (164) tagitem ::= NULL */
   259,  /* (165) tagitem ::= NOW */
   259,  /* (166) tagitem ::= NOW PLUS VARIABLE */
   259,  /* (167) tagitem ::= NOW MINUS VARIABLE */
   259,  /* (168) tagitem ::= MINUS INTEGER */
   259,  /* (169) tagitem ::= MINUS FLOAT */
   259,  /* (170) tagitem ::= PLUS INTEGER */
   259,  /* (171) tagitem ::= PLUS FLOAT */
   256,  /* (172) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   256,  /* (173) select ::= LP select RP */
   274,  /* (174) union ::= select */
   274,  /* (175) union ::= union UNION ALL select */
   205,  /* (176) cmd ::= union */
   256,  /* (177) select ::= SELECT selcollist */
   275,  /* (178) sclp ::= selcollist COMMA */
   275,  /* (179) sclp ::= */
   260,  /* (180) selcollist ::= sclp distinct expr as */
   260,  /* (181) selcollist ::= sclp STAR */
   278,  /* (182) as ::= AS ids */
   278,  /* (183) as ::= ids */
   278,  /* (184) as ::= */
   276,  /* (185) distinct ::= DISTINCT */
   276,  /* (186) distinct ::= */
   261,  /* (187) from ::= FROM tablelist */
   261,  /* (188) from ::= FROM sub */
   280,  /* (189) sub ::= LP union RP */
   280,  /* (190) sub ::= LP union RP ids */
   280,  /* (191) sub ::= sub COMMA LP union RP ids */
   279,  /* (192) tablelist ::= ids cpxName */
   279,  /* (193) tablelist ::= ids cpxName ids */
   279,  /* (194) tablelist ::= tablelist COMMA ids cpxName */
   279,  /* (195) tablelist ::= tablelist COMMA ids cpxName ids */
   281,  /* (196) tmvar ::= VARIABLE */
   282,  /* (197) timestamp ::= INTEGER */
   282,  /* (198) timestamp ::= MINUS INTEGER */
   282,  /* (199) timestamp ::= PLUS INTEGER */
   282,  /* (200) timestamp ::= STRING */
   282,  /* (201) timestamp ::= NOW */
   282,  /* (202) timestamp ::= NOW PLUS VARIABLE */
   282,  /* (203) timestamp ::= NOW MINUS VARIABLE */
   263,  /* (204) range_option ::= */
   263,  /* (205) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   264,  /* (206) interval_option ::= intervalKey LP tmvar RP */
   264,  /* (207) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   264,  /* (208) interval_option ::= */
   283,  /* (209) intervalKey ::= INTERVAL */
   283,  /* (210) intervalKey ::= EVERY */
   266,  /* (211) session_option ::= */
   266,  /* (212) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   267,  /* (213) windowstate_option ::= */
   267,  /* (214) windowstate_option ::= STATE_WINDOW LP ids RP */
   268,  /* (215) fill_opt ::= */
   268,  /* (216) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   268,  /* (217) fill_opt ::= FILL LP ID RP */
   265,  /* (218) sliding_opt ::= SLIDING LP tmvar RP */
   265,  /* (219) sliding_opt ::= */
   271,  /* (220) orderby_opt ::= */
   271,  /* (221) orderby_opt ::= ORDER BY sortlist */
   284,  /* (222) sortlist ::= sortlist COMMA item sortorder */
   284,  /* (223) sortlist ::= sortlist COMMA arrow sortorder */
   284,  /* (224) sortlist ::= item sortorder */
   284,  /* (225) sortlist ::= arrow sortorder */
   285,  /* (226) item ::= ID */
   285,  /* (227) item ::= ID DOT ID */
   286,  /* (228) sortorder ::= ASC */
   286,  /* (229) sortorder ::= DESC */
   286,  /* (230) sortorder ::= */
   269,  /* (231) groupby_opt ::= */
   269,  /* (232) groupby_opt ::= GROUP BY grouplist */
   288,  /* (233) grouplist ::= grouplist COMMA item */
   288,  /* (234) grouplist ::= grouplist COMMA arrow */
   288,  /* (235) grouplist ::= item */
   288,  /* (236) grouplist ::= arrow */
   270,  /* (237) having_opt ::= */
   270,  /* (238) having_opt ::= HAVING expr */
   273,  /* (239) limit_opt ::= */
   273,  /* (240) limit_opt ::= LIMIT signed */
   273,  /* (241) limit_opt ::= LIMIT signed OFFSET signed */
   273,  /* (242) limit_opt ::= LIMIT signed COMMA signed */
   272,  /* (243) slimit_opt ::= */
   272,  /* (244) slimit_opt ::= SLIMIT signed */
   272,  /* (245) slimit_opt ::= SLIMIT signed SOFFSET signed */
   272,  /* (246) slimit_opt ::= SLIMIT signed COMMA signed */
   262,  /* (247) where_opt ::= */
   262,  /* (248) where_opt ::= WHERE expr */
   277,  /* (249) expr ::= LP expr RP */
   277,  /* (250) expr ::= ID */
   277,  /* (251) expr ::= ID DOT ID */
   277,  /* (252) expr ::= ID DOT STAR */
   277,  /* (253) expr ::= INTEGER */
   277,  /* (254) expr ::= MINUS INTEGER */
   277,  /* (255) expr ::= PLUS INTEGER */
   277,  /* (256) expr ::= FLOAT */
   277,  /* (257) expr ::= MINUS FLOAT */
   277,  /* (258) expr ::= PLUS FLOAT */
   277,  /* (259) expr ::= STRING */
   277,  /* (260) expr ::= NOW */
   277,  /* (261) expr ::= TODAY */
   277,  /* (262) expr ::= VARIABLE */
   277,  /* (263) expr ::= PLUS VARIABLE */
   277,  /* (264) expr ::= MINUS VARIABLE */
   277,  /* (265) expr ::= BOOL */
   277,  /* (266) expr ::= NULL */
   277,  /* (267) expr ::= ID LP exprlist RP */
   277,  /* (268) expr ::= ID LP STAR RP */
   277,  /* (269) expr ::= ID LP expr AS typename RP */
   277,  /* (270) expr ::= expr IS NULL */
   277,  /* (271) expr ::= expr IS NOT NULL */
   277,  /* (272) expr ::= expr LT expr */
   277,  /* (273) expr ::= expr GT expr */
   277,  /* (274) expr ::= expr LE expr */
   277,  /* (275) expr ::= expr GE expr */
   277,  /* (276) expr ::= expr NE expr */
   277,  /* (277) expr ::= expr EQ expr */
   277,  /* (278) expr ::= expr BETWEEN expr AND expr */
   277,  /* (279) expr ::= expr AND expr */
   277,  /* (280) expr ::= expr OR expr */
   277,  /* (281) expr ::= expr PLUS expr */
   277,  /* (282) expr ::= expr MINUS expr */
   277,  /* (283) expr ::= expr STAR expr */
   277,  /* (284) expr ::= expr SLASH expr */
   277,  /* (285) expr ::= expr REM expr */
   277,  /* (286) expr ::= expr LIKE expr */
   277,  /* (287) expr ::= expr MATCH expr */
   277,  /* (288) expr ::= expr NMATCH expr */
   277,  /* (289) expr ::= ID CONTAINS STRING */
   277,  /* (290) expr ::= ID DOT ID CONTAINS STRING */
   287,  /* (291) arrow ::= ID ARROW STRING */
   287,  /* (292) arrow ::= ID DOT ID ARROW STRING */
   277,  /* (293) expr ::= arrow */
   277,  /* (294) expr ::= expr IN LP exprlist RP */
   213,  /* (295) exprlist ::= exprlist COMMA expritem */
   213,  /* (296) exprlist ::= expritem */
   289,  /* (297) expritem ::= expr */
   289,  /* (298) expritem ::= */
   205,  /* (299) cmd ::= RESET QUERY CACHE */
   205,  /* (300) cmd ::= SYNCDB ids REPLICA */
   205,  /* (301) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   205,  /* (302) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   205,  /* (303) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   205,  /* (304) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   205,  /* (305) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   205,  /* (306) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   205,  /* (307) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   205,  /* (308) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   205,  /* (309) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   205,  /* (310) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   205,  /* (311) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   205,  /* (312) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   205,  /* (313) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   205,  /* (314) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   205,  /* (315) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   205,  /* (316) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   205,  /* (317) cmd ::= KILL CONNECTION INTEGER */
   205,  /* (318) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   205,  /* (319) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW TOPICS */
   -2,  /* (3) cmd ::= SHOW FUNCTIONS */
   -2,  /* (4) cmd ::= SHOW MNODES */
   -2,  /* (5) cmd ::= SHOW DNODES */
   -2,  /* (6) cmd ::= SHOW ACCOUNTS */
   -2,  /* (7) cmd ::= SHOW USERS */
   -2,  /* (8) cmd ::= SHOW MODULES */
   -2,  /* (9) cmd ::= SHOW QUERIES */
   -2,  /* (10) cmd ::= SHOW CONNECTIONS */
   -2,  /* (11) cmd ::= SHOW STREAMS */
   -2,  /* (12) cmd ::= SHOW VARIABLES */
   -2,  /* (13) cmd ::= SHOW SCORES */
   -2,  /* (14) cmd ::= SHOW GRANTS */
   -2,  /* (15) cmd ::= SHOW VNODES */
   -3,  /* (16) cmd ::= SHOW VNODES ids */
    0,  /* (17) dbPrefix ::= */
   -2,  /* (18) dbPrefix ::= ids DOT */
    0,  /* (19) cpxName ::= */
   -2,  /* (20) cpxName ::= DOT ids */
   -5,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   -5,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   -4,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
   -3,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
   -3,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   -5,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (33) cmd ::= DROP FUNCTION ids */
   -3,  /* (34) cmd ::= DROP DNODE ids */
   -3,  /* (35) cmd ::= DROP USER ids */
   -3,  /* (36) cmd ::= DROP ACCOUNT ids */
   -2,  /* (37) cmd ::= USE ids */
   -3,  /* (38) cmd ::= DESCRIBE ids cpxName */
   -3,  /* (39) cmd ::= DESC ids cpxName */
   -5,  /* (40) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (42) cmd ::= ALTER DNODE ids ids */
   -5,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (44) cmd ::= ALTER LOCAL ids */
   -4,  /* (45) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -6,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   -1,  /* (51) ids ::= ID */
   -1,  /* (52) ids ::= STRING */
   -2,  /* (53) ifexists ::= IF EXISTS */
    0,  /* (54) ifexists ::= */
   -3,  /* (55) ifnotexists ::= IF NOT EXISTS */
    0,  /* (56) ifnotexists ::= */
   -3,  /* (57) cmd ::= CREATE DNODE ids */
   -6,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -8,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -9,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -5,  /* (63) cmd ::= CREATE USER ids PASS ids */
    0,  /* (64) bufsize ::= */
   -2,  /* (65) bufsize ::= BUFSIZE INTEGER */
    0,  /* (66) pps ::= */
   -2,  /* (67) pps ::= PPS INTEGER */
    0,  /* (68) tseries ::= */
   -2,  /* (69) tseries ::= TSERIES INTEGER */
    0,  /* (70) dbs ::= */
   -2,  /* (71) dbs ::= DBS INTEGER */
    0,  /* (72) streams ::= */
   -2,  /* (73) streams ::= STREAMS INTEGER */
    0,  /* (74) storage ::= */
   -2,  /* (75) storage ::= STORAGE INTEGER */
    0,  /* (76) qtime ::= */
   -2,  /* (77) qtime ::= QTIME INTEGER */
    0,  /* (78) users ::= */
   -2,  /* (79) users ::= USERS INTEGER */
    0,  /* (80) conns ::= */
   -2,  /* (81) conns ::= CONNS INTEGER */
    0,  /* (82) state ::= */
   -2,  /* (83) state ::= STATE ids */
   -9,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -3,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   -1,  /* (86) intitemlist ::= intitem */
   -1,  /* (87) intitem ::= INTEGER */
   -2,  /* (88) keep ::= KEEP intitemlist */
   -2,  /* (89) cache ::= CACHE INTEGER */
   -2,  /* (90) replica ::= REPLICA INTEGER */
   -2,  /* (91) quorum ::= QUORUM INTEGER */
   -2,  /* (92) days ::= DAYS INTEGER */
   -2,  /* (93) minrows ::= MINROWS INTEGER */
   -2,  /* (94) maxrows ::= MAXROWS INTEGER */
   -2,  /* (95) blocks ::= BLOCKS INTEGER */
   -2,  /* (96) ctime ::= CTIME INTEGER */
   -2,  /* (97) wal ::= WAL INTEGER */
   -2,  /* (98) fsync ::= FSYNC INTEGER */
   -2,  /* (99) comp ::= COMP INTEGER */
   -2,  /* (100) prec ::= PRECISION STRING */
   -2,  /* (101) update ::= UPDATE INTEGER */
   -2,  /* (102) cachelast ::= CACHELAST INTEGER */
   -2,  /* (103) partitions ::= PARTITIONS INTEGER */
    0,  /* (104) db_optr ::= */
   -2,  /* (105) db_optr ::= db_optr cache */
   -2,  /* (106) db_optr ::= db_optr replica */
   -2,  /* (107) db_optr ::= db_optr quorum */
   -2,  /* (108) db_optr ::= db_optr days */
   -2,  /* (109) db_optr ::= db_optr minrows */
   -2,  /* (110) db_optr ::= db_optr maxrows */
   -2,  /* (111) db_optr ::= db_optr blocks */
   -2,  /* (112) db_optr ::= db_optr ctime */
   -2,  /* (113) db_optr ::= db_optr wal */
   -2,  /* (114) db_optr ::= db_optr fsync */
   -2,  /* (115) db_optr ::= db_optr comp */
   -2,  /* (116) db_optr ::= db_optr prec */
   -2,  /* (117) db_optr ::= db_optr keep */
   -2,  /* (118) db_optr ::= db_optr update */
   -2,  /* (119) db_optr ::= db_optr cachelast */
   -1,  /* (120) topic_optr ::= db_optr */
   -2,  /* (121) topic_optr ::= topic_optr partitions */
    0,  /* (122) alter_db_optr ::= */
   -2,  /* (123) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (125) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (127) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (128) alter_db_optr ::= alter_db_optr update */
   -2,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (130) alter_topic_optr ::= alter_db_optr */
   -2,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (132) typename ::= ids */
   -4,  /* (133) typename ::= ids LP signed RP */
   -2,  /* (134) typename ::= ids UNSIGNED */
   -1,  /* (135) signed ::= INTEGER */
   -2,  /* (136) signed ::= PLUS INTEGER */
   -2,  /* (137) signed ::= MINUS INTEGER */
   -3,  /* (138) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (141) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (142) create_table_list ::= create_from_stable */
   -2,  /* (143) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (149) tagNamelist ::= ids */
   -7,  /* (150) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
    0,  /* (151) to_opt ::= */
   -3,  /* (152) to_opt ::= TO ids cpxName */
    0,  /* (153) split_opt ::= */
   -2,  /* (154) split_opt ::= SPLIT ids */
   -3,  /* (155) columnlist ::= columnlist COMMA column */
   -1,  /* (156) columnlist ::= column */
   -2,  /* (157) column ::= ids typename */
   -3,  /* (158) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (159) tagitemlist ::= tagitem */
   -1,  /* (160) tagitem ::= INTEGER */
   -1,  /* (161) tagitem ::= FLOAT */
   -1,  /* (162) tagitem ::= STRING */
   -1,  /* (163) tagitem ::= BOOL */
   -1,  /* (164) tagitem ::= NULL */
   -1,  /* (165) tagitem ::= NOW */
   -3,  /* (166) tagitem ::= NOW PLUS VARIABLE */
   -3,  /* (167) tagitem ::= NOW MINUS VARIABLE */
   -2,  /* (168) tagitem ::= MINUS INTEGER */
   -2,  /* (169) tagitem ::= MINUS FLOAT */
   -2,  /* (170) tagitem ::= PLUS INTEGER */
   -2,  /* (171) tagitem ::= PLUS FLOAT */
  -15,  /* (172) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (173) select ::= LP select RP */
   -1,  /* (174) union ::= select */
   -4,  /* (175) union ::= union UNION ALL select */
   -1,  /* (176) cmd ::= union */
   -2,  /* (177) select ::= SELECT selcollist */
   -2,  /* (178) sclp ::= selcollist COMMA */
    0,  /* (179) sclp ::= */
   -4,  /* (180) selcollist ::= sclp distinct expr as */
   -2,  /* (181) selcollist ::= sclp STAR */
   -2,  /* (182) as ::= AS ids */
   -1,  /* (183) as ::= ids */
    0,  /* (184) as ::= */
   -1,  /* (185) distinct ::= DISTINCT */
    0,  /* (186) distinct ::= */
   -2,  /* (187) from ::= FROM tablelist */
   -2,  /* (188) from ::= FROM sub */
   -3,  /* (189) sub ::= LP union RP */
   -4,  /* (190) sub ::= LP union RP ids */
   -6,  /* (191) sub ::= sub COMMA LP union RP ids */
   -2,  /* (192) tablelist ::= ids cpxName */
   -3,  /* (193) tablelist ::= ids cpxName ids */
   -4,  /* (194) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (195) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (196) tmvar ::= VARIABLE */
   -1,  /* (197) timestamp ::= INTEGER */
   -2,  /* (198) timestamp ::= MINUS INTEGER */
   -2,  /* (199) timestamp ::= PLUS INTEGER */
   -1,  /* (200) timestamp ::= STRING */
   -1,  /* (201) timestamp ::= NOW */
   -3,  /* (202) timestamp ::= NOW PLUS VARIABLE */
   -3,  /* (203) timestamp ::= NOW MINUS VARIABLE */
    0,  /* (204) range_option ::= */
   -6,  /* (205) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   -4,  /* (206) interval_option ::= intervalKey LP tmvar RP */
   -6,  /* (207) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
    0,  /* (208) interval_option ::= */
   -1,  /* (209) intervalKey ::= INTERVAL */
   -1,  /* (210) intervalKey ::= EVERY */
    0,  /* (211) session_option ::= */
   -7,  /* (212) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (213) windowstate_option ::= */
   -4,  /* (214) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (215) fill_opt ::= */
   -6,  /* (216) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (217) fill_opt ::= FILL LP ID RP */
   -4,  /* (218) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (219) sliding_opt ::= */
    0,  /* (220) orderby_opt ::= */
   -3,  /* (221) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (222) sortlist ::= sortlist COMMA item sortorder */
   -4,  /* (223) sortlist ::= sortlist COMMA arrow sortorder */
   -2,  /* (224) sortlist ::= item sortorder */
   -2,  /* (225) sortlist ::= arrow sortorder */
   -1,  /* (226) item ::= ID */
   -3,  /* (227) item ::= ID DOT ID */
   -1,  /* (228) sortorder ::= ASC */
   -1,  /* (229) sortorder ::= DESC */
    0,  /* (230) sortorder ::= */
    0,  /* (231) groupby_opt ::= */
   -3,  /* (232) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (233) grouplist ::= grouplist COMMA item */
   -3,  /* (234) grouplist ::= grouplist COMMA arrow */
   -1,  /* (235) grouplist ::= item */
   -1,  /* (236) grouplist ::= arrow */
    0,  /* (237) having_opt ::= */
   -2,  /* (238) having_opt ::= HAVING expr */
    0,  /* (239) limit_opt ::= */
   -2,  /* (240) limit_opt ::= LIMIT signed */
   -4,  /* (241) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (242) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (243) slimit_opt ::= */
   -2,  /* (244) slimit_opt ::= SLIMIT signed */
   -4,  /* (245) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (246) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (247) where_opt ::= */
   -2,  /* (248) where_opt ::= WHERE expr */
   -3,  /* (249) expr ::= LP expr RP */
   -1,  /* (250) expr ::= ID */
   -3,  /* (251) expr ::= ID DOT ID */
   -3,  /* (252) expr ::= ID DOT STAR */
   -1,  /* (253) expr ::= INTEGER */
   -2,  /* (254) expr ::= MINUS INTEGER */
   -2,  /* (255) expr ::= PLUS INTEGER */
   -1,  /* (256) expr ::= FLOAT */
   -2,  /* (257) expr ::= MINUS FLOAT */
   -2,  /* (258) expr ::= PLUS FLOAT */
   -1,  /* (259) expr ::= STRING */
   -1,  /* (260) expr ::= NOW */
   -1,  /* (261) expr ::= TODAY */
   -1,  /* (262) expr ::= VARIABLE */
   -2,  /* (263) expr ::= PLUS VARIABLE */
   -2,  /* (264) expr ::= MINUS VARIABLE */
   -1,  /* (265) expr ::= BOOL */
   -1,  /* (266) expr ::= NULL */
   -4,  /* (267) expr ::= ID LP exprlist RP */
   -4,  /* (268) expr ::= ID LP STAR RP */
   -6,  /* (269) expr ::= ID LP expr AS typename RP */
   -3,  /* (270) expr ::= expr IS NULL */
   -4,  /* (271) expr ::= expr IS NOT NULL */
   -3,  /* (272) expr ::= expr LT expr */
   -3,  /* (273) expr ::= expr GT expr */
   -3,  /* (274) expr ::= expr LE expr */
   -3,  /* (275) expr ::= expr GE expr */
   -3,  /* (276) expr ::= expr NE expr */
   -3,  /* (277) expr ::= expr EQ expr */
   -5,  /* (278) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (279) expr ::= expr AND expr */
   -3,  /* (280) expr ::= expr OR expr */
   -3,  /* (281) expr ::= expr PLUS expr */
   -3,  /* (282) expr ::= expr MINUS expr */
   -3,  /* (283) expr ::= expr STAR expr */
   -3,  /* (284) expr ::= expr SLASH expr */
   -3,  /* (285) expr ::= expr REM expr */
   -3,  /* (286) expr ::= expr LIKE expr */
   -3,  /* (287) expr ::= expr MATCH expr */
   -3,  /* (288) expr ::= expr NMATCH expr */
   -3,  /* (289) expr ::= ID CONTAINS STRING */
   -5,  /* (290) expr ::= ID DOT ID CONTAINS STRING */
   -3,  /* (291) arrow ::= ID ARROW STRING */
   -5,  /* (292) arrow ::= ID DOT ID ARROW STRING */
   -1,  /* (293) expr ::= arrow */
   -5,  /* (294) expr ::= expr IN LP exprlist RP */
   -3,  /* (295) exprlist ::= exprlist COMMA expritem */
   -1,  /* (296) exprlist ::= expritem */
   -1,  /* (297) expritem ::= expr */
    0,  /* (298) expritem ::= */
   -3,  /* (299) cmd ::= RESET QUERY CACHE */
   -3,  /* (300) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (301) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (302) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (303) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (304) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (305) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (306) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (307) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (308) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (309) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (310) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (311) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (312) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (313) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (314) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (315) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (316) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (317) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (318) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (319) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy302, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy231);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy231);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy161);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy231);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy302, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy223, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy223, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy231.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy231.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy231.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy231.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy231.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy231.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy231.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy231.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy231.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy231 = yylhsminor.yy231;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 158: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==158);
{ yylhsminor.yy161 = tVariantListAppend(yymsp[-2].minor.yy161, &yymsp[0].minor.yy526, -1);    }
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 86: /* intitemlist ::= intitem */
      case 159: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==159);
{ yylhsminor.yy161 = tVariantListAppend(NULL, &yymsp[0].minor.yy526, -1); }
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 87: /* intitem ::= INTEGER */
      case 160: /* tagitem ::= INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= FLOAT */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= STRING */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= BOOL */ yytestcase(yyruleno==163);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy526, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy161 = yymsp[0].minor.yy161; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy302); yymsp[1].minor.yy302.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.keep = yymsp[0].minor.yy161; }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy302 = yymsp[0].minor.yy302; yylhsminor.yy302.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy302 = yylhsminor.yy302;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy302); yymsp[1].minor.yy302.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy223, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy223 = yylhsminor.yy223;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy369 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;  // negative value of name length
    tSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy223 = yylhsminor.yy223;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy223, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy462;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy356);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy462 = pCreateTable;
}
  yymsp[0].minor.yy462 = yylhsminor.yy462;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy462->childTableInfo, &yymsp[0].minor.yy356);
  yylhsminor.yy462 = yymsp[-1].minor.yy462;
}
  yymsp[-1].minor.yy462 = yylhsminor.yy462;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy462 = tSetCreateTableInfo(yymsp[-1].minor.yy161, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy462, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy462 = yylhsminor.yy462;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy462 = tSetCreateTableInfo(yymsp[-5].minor.yy161, yymsp[-1].minor.yy161, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy462, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy462 = yylhsminor.yy462;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy356 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy161, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy356 = yylhsminor.yy356;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy356 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy161, yymsp[-1].minor.yy161, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy356 = yylhsminor.yy356;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy161, &yymsp[0].minor.yy0); yylhsminor.yy161 = yymsp[-2].minor.yy161;  }
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy161 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy161, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy462 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy276, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy462, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy462 = yylhsminor.yy462;
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
{taosArrayPush(yymsp[-2].minor.yy161, &yymsp[0].minor.yy223); yylhsminor.yy161 = yymsp[-2].minor.yy161;  }
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 156: /* columnlist ::= column */
{yylhsminor.yy161 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy161, &yymsp[0].minor.yy223);}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 157: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 164: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy526, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 165: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy526, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 166: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy526, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 167: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy526, &yymsp[0].minor.yy0, TK_MINUS, true);
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
    tVariantCreate(&yylhsminor.yy526, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 172: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy276 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy161, yymsp[-12].minor.yy84, yymsp[-11].minor.yy546, yymsp[-4].minor.yy161, yymsp[-2].minor.yy161, &yymsp[-9].minor.yy300, &yymsp[-7].minor.yy219, &yymsp[-6].minor.yy548, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy161, &yymsp[0].minor.yy394, &yymsp[-1].minor.yy394, yymsp[-3].minor.yy546, &yymsp[-10].minor.yy420);
}
  yymsp[-14].minor.yy276 = yylhsminor.yy276;
        break;
      case 173: /* select ::= LP select RP */
{yymsp[-2].minor.yy276 = yymsp[-1].minor.yy276;}
        break;
      case 174: /* union ::= select */
{ yylhsminor.yy161 = setSubclause(NULL, yymsp[0].minor.yy276); }
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 175: /* union ::= union UNION ALL select */
{ yylhsminor.yy161 = appendSelectClause(yymsp[-3].minor.yy161, yymsp[0].minor.yy276); }
  yymsp[-3].minor.yy161 = yylhsminor.yy161;
        break;
      case 176: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy161, NULL, TSDB_SQL_SELECT); }
        break;
      case 177: /* select ::= SELECT selcollist */
{
  yylhsminor.yy276 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy161, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy276 = yylhsminor.yy276;
        break;
      case 178: /* sclp ::= selcollist COMMA */
{yylhsminor.yy161 = yymsp[-1].minor.yy161;}
  yymsp[-1].minor.yy161 = yylhsminor.yy161;
        break;
      case 179: /* sclp ::= */
      case 220: /* orderby_opt ::= */ yytestcase(yyruleno==220);
{yymsp[1].minor.yy161 = 0;}
        break;
      case 180: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy161 = tSqlExprListAppend(yymsp[-3].minor.yy161, yymsp[-1].minor.yy546,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy161 = yylhsminor.yy161;
        break;
      case 181: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy161 = tSqlExprListAppend(yymsp[-1].minor.yy161, pNode, 0, 0);
}
  yymsp[-1].minor.yy161 = yylhsminor.yy161;
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
{yymsp[-1].minor.yy84 = yymsp[0].minor.yy84;}
        break;
      case 189: /* sub ::= LP union RP */
{yymsp[-2].minor.yy84 = addSubqueryElem(NULL, yymsp[-1].minor.yy161, NULL);}
        break;
      case 190: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy84 = addSubqueryElem(NULL, yymsp[-2].minor.yy161, &yymsp[0].minor.yy0);}
        break;
      case 191: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy84 = addSubqueryElem(yymsp[-5].minor.yy84, yymsp[-2].minor.yy161, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy84 = yylhsminor.yy84;
        break;
      case 192: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy84 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy84 = yylhsminor.yy84;
        break;
      case 193: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy84 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy84 = yylhsminor.yy84;
        break;
      case 194: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy84 = setTableNameList(yymsp[-3].minor.yy84, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy84 = yylhsminor.yy84;
        break;
      case 195: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy84 = setTableNameList(yymsp[-4].minor.yy84, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy84 = yylhsminor.yy84;
        break;
      case 196: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 197: /* timestamp ::= INTEGER */
{ yylhsminor.yy546 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 198: /* timestamp ::= MINUS INTEGER */
      case 199: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==199);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy546 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy546 = yylhsminor.yy546;
        break;
      case 200: /* timestamp ::= STRING */
{ yylhsminor.yy546 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 201: /* timestamp ::= NOW */
{ yylhsminor.yy546 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 202: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy546 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 203: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy546 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 204: /* range_option ::= */
{yymsp[1].minor.yy420.start = 0; yymsp[1].minor.yy420.end = 0;}
        break;
      case 205: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy420.start = yymsp[-3].minor.yy546; yymsp[-5].minor.yy420.end = yymsp[-1].minor.yy546;}
        break;
      case 206: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy300.interval = yymsp[-1].minor.yy0; yylhsminor.yy300.offset.n = 0; yylhsminor.yy300.token = yymsp[-3].minor.yy520;}
  yymsp[-3].minor.yy300 = yylhsminor.yy300;
        break;
      case 207: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy300.interval = yymsp[-3].minor.yy0; yylhsminor.yy300.offset = yymsp[-1].minor.yy0;   yylhsminor.yy300.token = yymsp[-5].minor.yy520;}
  yymsp[-5].minor.yy300 = yylhsminor.yy300;
        break;
      case 208: /* interval_option ::= */
{memset(&yymsp[1].minor.yy300, 0, sizeof(yymsp[1].minor.yy300));}
        break;
      case 209: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy520 = TK_INTERVAL;}
        break;
      case 210: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy520 = TK_EVERY;   }
        break;
      case 211: /* session_option ::= */
{yymsp[1].minor.yy219.col.n = 0; yymsp[1].minor.yy219.gap.n = 0;}
        break;
      case 212: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy219.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy219.gap = yymsp[-1].minor.yy0;
}
        break;
      case 213: /* windowstate_option ::= */
{ yymsp[1].minor.yy548.col.n = 0; yymsp[1].minor.yy548.col.z = NULL;}
        break;
      case 214: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy548.col = yymsp[-1].minor.yy0; }
        break;
      case 215: /* fill_opt ::= */
{ yymsp[1].minor.yy161 = 0;     }
        break;
      case 216: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy161, &A, -1, 0);
    yymsp[-5].minor.yy161 = yymsp[-1].minor.yy161;
}
        break;
      case 217: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy161 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 218: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 219: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 221: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy161 = yymsp[0].minor.yy161;}
        break;
      case 222: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy161 = commonItemAppend(yymsp[-3].minor.yy161, &yymsp[-1].minor.yy526, NULL, false, yymsp[0].minor.yy452);
}
  yymsp[-3].minor.yy161 = yylhsminor.yy161;
        break;
      case 223: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy161 = commonItemAppend(yymsp[-3].minor.yy161, NULL, yymsp[-1].minor.yy546, true, yymsp[0].minor.yy452);
}
  yymsp[-3].minor.yy161 = yylhsminor.yy161;
        break;
      case 224: /* sortlist ::= item sortorder */
{
  yylhsminor.yy161 = commonItemAppend(NULL, &yymsp[-1].minor.yy526, NULL, false, yymsp[0].minor.yy452);
}
  yymsp[-1].minor.yy161 = yylhsminor.yy161;
        break;
      case 225: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy161 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy546, true, yymsp[0].minor.yy452);
}
  yymsp[-1].minor.yy161 = yylhsminor.yy161;
        break;
      case 226: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy526, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 227: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy526, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy526 = yylhsminor.yy526;
        break;
      case 228: /* sortorder ::= ASC */
{ yymsp[0].minor.yy452 = TSDB_ORDER_ASC; }
        break;
      case 229: /* sortorder ::= DESC */
{ yymsp[0].minor.yy452 = TSDB_ORDER_DESC;}
        break;
      case 230: /* sortorder ::= */
{ yymsp[1].minor.yy452 = TSDB_ORDER_ASC; }
        break;
      case 231: /* groupby_opt ::= */
{ yymsp[1].minor.yy161 = 0;}
        break;
      case 232: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy161 = yymsp[0].minor.yy161;}
        break;
      case 233: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy161 = commonItemAppend(yymsp[-2].minor.yy161, &yymsp[0].minor.yy526, NULL, false, -1);
}
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 234: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy161 = commonItemAppend(yymsp[-2].minor.yy161, NULL, yymsp[0].minor.yy546, true, -1);
}
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 235: /* grouplist ::= item */
{
  yylhsminor.yy161 = commonItemAppend(NULL, &yymsp[0].minor.yy526, NULL, false, -1);
}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 236: /* grouplist ::= arrow */
{
  yylhsminor.yy161 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy546, true, -1);
}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 237: /* having_opt ::= */
      case 247: /* where_opt ::= */ yytestcase(yyruleno==247);
      case 298: /* expritem ::= */ yytestcase(yyruleno==298);
{yymsp[1].minor.yy546 = 0;}
        break;
      case 238: /* having_opt ::= HAVING expr */
      case 248: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==248);
{yymsp[-1].minor.yy546 = yymsp[0].minor.yy546;}
        break;
      case 239: /* limit_opt ::= */
      case 243: /* slimit_opt ::= */ yytestcase(yyruleno==243);
{yymsp[1].minor.yy394.limit = -1; yymsp[1].minor.yy394.offset = 0;}
        break;
      case 240: /* limit_opt ::= LIMIT signed */
      case 244: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==244);
{yymsp[-1].minor.yy394.limit = yymsp[0].minor.yy369;  yymsp[-1].minor.yy394.offset = 0;}
        break;
      case 241: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy394.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[0].minor.yy369;}
        break;
      case 242: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy394.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[-2].minor.yy369;}
        break;
      case 245: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy394.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[0].minor.yy369;}
        break;
      case 246: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy394.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[-2].minor.yy369;}
        break;
      case 249: /* expr ::= LP expr RP */
{yylhsminor.yy546 = yymsp[-1].minor.yy546; yylhsminor.yy546->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy546->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 250: /* expr ::= ID */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 251: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 252: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 253: /* expr ::= INTEGER */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 254: /* expr ::= MINUS INTEGER */
      case 255: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==255);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy546 = yylhsminor.yy546;
        break;
      case 256: /* expr ::= FLOAT */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 257: /* expr ::= MINUS FLOAT */
      case 258: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==258);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy546 = yylhsminor.yy546;
        break;
      case 259: /* expr ::= STRING */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 260: /* expr ::= NOW */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 261: /* expr ::= TODAY */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 262: /* expr ::= VARIABLE */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 263: /* expr ::= PLUS VARIABLE */
      case 264: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==264);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy546 = yylhsminor.yy546;
        break;
      case 265: /* expr ::= BOOL */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 266: /* expr ::= NULL */
{ yylhsminor.yy546 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 267: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy546 = tSqlExprCreateFunction(yymsp[-1].minor.yy161, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy546 = yylhsminor.yy546;
        break;
      case 268: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy546 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy546 = yylhsminor.yy546;
        break;
      case 269: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy546 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy546, &yymsp[-1].minor.yy223, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy546 = yylhsminor.yy546;
        break;
      case 270: /* expr ::= expr IS NULL */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 271: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-3].minor.yy546, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy546 = yylhsminor.yy546;
        break;
      case 272: /* expr ::= expr LT expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_LT);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 273: /* expr ::= expr GT expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_GT);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 274: /* expr ::= expr LE expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_LE);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 275: /* expr ::= expr GE expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_GE);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 276: /* expr ::= expr NE expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_NE);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 277: /* expr ::= expr EQ expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_EQ);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 278: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy546); yylhsminor.yy546 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy546, yymsp[-2].minor.yy546, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy546, TK_LE), TK_AND);}
  yymsp[-4].minor.yy546 = yylhsminor.yy546;
        break;
      case 279: /* expr ::= expr AND expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_AND);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 280: /* expr ::= expr OR expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_OR); }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 281: /* expr ::= expr PLUS expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_PLUS);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 282: /* expr ::= expr MINUS expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_MINUS); }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 283: /* expr ::= expr STAR expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_STAR);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 284: /* expr ::= expr SLASH expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_DIVIDE);}
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 285: /* expr ::= expr REM expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_REM);   }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 286: /* expr ::= expr LIKE expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_LIKE);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 287: /* expr ::= expr MATCH expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_MATCH);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 288: /* expr ::= expr NMATCH expr */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-2].minor.yy546, yymsp[0].minor.yy546, TK_NMATCH);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 289: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy546 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 290: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy546 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy546 = yylhsminor.yy546;
        break;
      case 291: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy546 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy546 = yylhsminor.yy546;
        break;
      case 292: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy546 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy546 = yylhsminor.yy546;
        break;
      case 293: /* expr ::= arrow */
      case 297: /* expritem ::= expr */ yytestcase(yyruleno==297);
{yylhsminor.yy546 = yymsp[0].minor.yy546;}
  yymsp[0].minor.yy546 = yylhsminor.yy546;
        break;
      case 294: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy546 = tSqlExprCreate(yymsp[-4].minor.yy546, (tSqlExpr*)yymsp[-1].minor.yy161, TK_IN); }
  yymsp[-4].minor.yy546 = yylhsminor.yy546;
        break;
      case 295: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy161 = tSqlExprListAppend(yymsp[-2].minor.yy161,yymsp[0].minor.yy546,0, 0);}
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 296: /* exprlist ::= expritem */
{yylhsminor.yy161 = tSqlExprListAppend(0,yymsp[0].minor.yy546,0, 0);}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 299: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 300: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 301: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 302: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 303: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 306: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 307: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy526, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 315: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy526, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 318: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 319: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
