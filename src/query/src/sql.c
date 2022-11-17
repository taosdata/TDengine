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
#define YYNOCODE 292
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tVariant yy42;
  int32_t yy44;
  SCreateTableSql* yy78;
  SRangeVal yy132;
  int yy133;
  SSqlNode* yy144;
  SLimitVal yy190;
  tSqlExpr* yy194;
  SIntervalVal yy200;
  SSessionWindowVal yy235;
  SWindowStateVal yy248;
  TAOS_FIELD yy263;
  int64_t yy277;
  SCreateAcctInfo yy299;
  SArray* yy333;
  SCreateDbInfo yy342;
  SCreatedTableInfo yy400;
  SRelationInfo* yy516;
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
#define YYNSTATE             417
#define YYNRULE              330
#define YYNRULE_WITH_ACTION  330
#define YYNTOKEN             206
#define YY_MAX_SHIFT         416
#define YY_MIN_SHIFTREDUCE   648
#define YY_MAX_SHIFTREDUCE   977
#define YY_ERROR_ACTION      978
#define YY_ACCEPT_ACTION     979
#define YY_NO_ACTION         980
#define YY_MIN_REDUCE        981
#define YY_MAX_REDUCE        1310
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
#define YY_ACTTAB_COUNT (942)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   230,  700, 1142,  175, 1219,   65, 1220,  332,  700,  701,
 /*    10 */  1283,  270, 1285, 1167,   43,   44,  701,   47,   48,  415,
 /*    20 */   261,  283,   32,   31,   30,  737,   65,   46,  365,   51,
 /*    30 */    49,   52,   50,   37,   36,   35,   34,   33,   42,   41,
 /*    40 */   268,   24,   40,   39,   38,   43,   44, 1143,   47,   48,
 /*    50 */   263, 1283,  283,   32,   31,   30,  314, 1140,   46,  365,
 /*    60 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*    70 */    41,  273,  228,   40,   39,   38,  313,  312, 1140,  275,
 /*    80 */    43,   44, 1283,   47,   48, 1164, 1143,  283,   32,   31,
 /*    90 */    30,  361,   95,   46,  365,   51,   49,   52,   50,   37,
 /*   100 */    36,   35,   34,   33,   42,   41,  229, 1273,   40,   39,
 /*   110 */    38,   43,   44,  400,   47,   48, 1283, 1283,  283,   32,
 /*   120 */    31,   30, 1125,   64,   46,  365,   51,   49,   52,   50,
 /*   130 */    37,   36,   35,   34,   33,   42,   41, 1305,  234,   40,
 /*   140 */    39,   38,  277,   43,   45,  786,   47,   48, 1283, 1143,
 /*   150 */   283,   32,   31,   30,   65,  900,   46,  365,   51,   49,
 /*   160 */    52,   50,   37,   36,   35,   34,   33,   42,   41,  860,
 /*   170 */   861,   40,   39,   38,   44,  298,   47,   48,  390,  389,
 /*   180 */   283,   32,   31,   30,  302,  301,   46,  365,   51,   49,
 /*   190 */    52,   50,   37,   36,   35,   34,   33,   42,   41,  700,
 /*   200 */   141,   40,   39,   38,   47,   48, 1139,  701,  283,   32,
 /*   210 */    31,   30,  361,  400,   46,  365,   51,   49,   52,   50,
 /*   220 */    37,   36,   35,   34,   33,   42,   41,  115,  190,   40,
 /*   230 */    39,   38,   73,  359,  408,  407,  358,  406,  357,  405,
 /*   240 */   356,  355,  354,  404,  353,  403,  402,  322,  649,  650,
 /*   250 */   651,  652,  653,  654,  655,  656,  657,  658,  659,  660,
 /*   260 */   661,  662,  169, 1127,  262, 1100, 1088, 1089, 1090, 1091,
 /*   270 */  1092, 1093, 1094, 1095, 1096, 1097, 1098, 1099, 1101, 1102,
 /*   280 */    25,   51,   49,   52,   50,   37,   36,   35,   34,   33,
 /*   290 */    42,   41,  235,  700,   40,   39,   38,  244,  236,  254,
 /*   300 */   916,  701, 1283,  904,  246,  907, 1297,  910, 1283,   92,
 /*   310 */   156,  155,  154,  245,  112,  979,  416,  247,  373,  101,
 /*   320 */    37,   36,   35,   34,   33,   42,   41, 1283,  315,   40,
 /*   330 */    39,   38,  336,  107,  101,  106,  258,  259,  254,  916,
 /*   340 */   367,  248,  904, 1124,  907,   29,  910, 1122, 1123,   61,
 /*   350 */  1126, 1283,    5,   68,  201, 1217,  280, 1218,   74,  200,
 /*   360 */   122,  127,  118,  126,  249,  814, 1133, 1158,  811, 1158,
 /*   370 */   812, 1230,  813,   74, 1283,  258,  259,  293,  848,  349,
 /*   380 */    57,  305,  851,   91,   29,  306,   53,  264,  197,   65,
 /*   390 */   255,  369,  286,   73,  295,  408,  407,   65,  406,  292,
 /*   400 */   405,  288,  289,   65,  404,  293,  403,  402,  139,  133,
 /*   410 */   144,  318,  319,  276,  368,  143,  198,  149,  153,  142,
 /*   420 */   226,  279,  917,  911,  913,   53, 1108,  146, 1106, 1107,
 /*   430 */  1283, 1113, 1286, 1109,  274,  284,  906, 1110,  909, 1111,
 /*   440 */  1112, 1140,  377,  221,  219,  217,  250,  912,  378, 1140,
 /*   450 */   216,  160,  159,  158,  157, 1140, 1283, 1229,   65,   13,
 /*   460 */   271,  917,  911,  913,   42,   41,  364,   65,   40,   39,
 /*   470 */    38,   65,  287,   65,  285,   65,  376,  375,  905,  294,
 /*   480 */   908,  291, 1226,  385,  384,  230,  912,  815,  290,  363,
 /*   490 */    65,  363,  114,   93,  251, 1283,  832, 1286,  266,  230,
 /*   500 */   414,  412,  676,  379, 1283,  109,  282,  108,  335, 1283,
 /*   510 */  1140, 1286,  380,   40,   39,   38,  386,  293,  387, 1140,
 /*   520 */   388,    1,  199, 1140, 1269, 1140, 1158, 1140,  366,    6,
 /*   530 */  1268,  152,  151,  150, 1283,  392,  168,  166,  165, 1267,
 /*   540 */  1283,  256, 1140,  880,  265,  257,  409, 1069,  232, 1283,
 /*   550 */   233, 1283,  237,  231,  238, 1283,  239,  914, 1283,  241,
 /*   560 */  1283,   98, 1283, 1283, 1283,  242, 1283,  243,  293, 1283,
 /*   570 */   240, 1030,  227,  829,  110, 1283, 1040, 1283,  211, 1141,
 /*   580 */  1283, 1031, 1283,  211,  836,   99,    3,  212,  211,   96,
 /*   590 */   307,  317,  316,  857,  867,  868,   82,   85,  796,  915,
 /*   600 */   309,   10,  340,  879,   66,  798,  342,  177,   77,   54,
 /*   610 */   343,  797,   66,   60,  281,  951,   66,  918,  370,   77,
 /*   620 */   113,  699,  382,  381,  309,   77,   89, 1225,   15,    9,
 /*   630 */    14,    9,  132,  272,  131,    9,   17,  193,   16,   86,
 /*   640 */    83,  821,  819,  822,  820,   19,  138,   18,  137,   21,
 /*   650 */   391,   20,  171,  303,  173,  903,   26,  174,  785, 1138,
 /*   660 */  1166, 1177, 1159, 1174, 1175,  310, 1179,  176,  181,  328,
 /*   670 */  1209, 1208, 1207,  192, 1206, 1134,  194, 1310,  410,  167,
 /*   680 */   321,  267,  323, 1132,  195,  196,  325, 1046,  345,  346,
 /*   690 */   847,  347,  348,  351,  352,   75,  224,   71,  362, 1156,
 /*   700 */   184,  182,  183,   87,   27, 1039,  374, 1304,  129,  337,
 /*   710 */  1303, 1300,   84,  185,   28,  202,  383,  333, 1296,  135,
 /*   720 */  1295,  329, 1292,  203,  331,  327,  921, 1066,   72,   67,
 /*   730 */   324,   76,  225,  186,  187, 1027,  320,  145, 1025,  147,
 /*   740 */    94,  148, 1023, 1022, 1021,  260,  350,  214,  215, 1018,
 /*   750 */  1017, 1016, 1015, 1014, 1013, 1012,  218,  220, 1004,  222,
 /*   760 */  1001,  401,  223,  997,  140,  393,  394,  170,  395,   90,
 /*   770 */   308, 1136,   97,  102,  396,  326,  397,  398,  399,  172,
 /*   780 */   976,  297,  975,   88,  278,  296,  188,  344,  300,  299,
 /*   790 */   974,  957,  252,  253,  956,  123, 1044,  124, 1043,  309,
 /*   800 */   304,   11,  339,  824,  100,  311,   58,  103,  856, 1020,
 /*   810 */    80, 1019,  854,  853,  161, 1067,  206,  204,  205,  208,
 /*   820 */   207,  209,  210,  162,    2, 1011,  338, 1104, 1010, 1068,
 /*   830 */   163,  164, 1003,   59,  189, 1002,  191,  850,    4,  849,
 /*   840 */    81,  180,  858,  178, 1115,  869,  179,  269,  863,  104,
 /*   850 */    69,  865,  105,  330,  368,   22,  334,   70,  111,   12,
 /*   860 */    23,   55,   56,  341,  114,  116,  120,  117,  715,  750,
 /*   870 */   748,  747,   62,  119,  746,   63,  744,  742,  121,  739,
 /*   880 */   704,  125,  948,  360,  946,    7,  949,  947,  920,  919,
 /*   890 */     8,  922,  371,  372,  818,  128,   78,  130,   66,   79,
 /*   900 */   134,  136,  788,  787,  784,  731,  729,  721,  727,  723,
 /*   910 */   725,  719,  717,  817,  753,  752,  751,  749,  745,  743,
 /*   920 */   741,  740,  213,  666,  702,  981,  675,  673,  980,  980,
 /*   930 */   411,  980,  980,  980,  980,  980,  980,  980,  980,  980,
 /*   940 */   980,  413,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   279,    1,  261,  208,  287,  208,  289,  290,    1,    9,
 /*    10 */   289,    1,  291,  208,   14,   15,    9,   17,   18,  208,
 /*    20 */   209,   21,   22,   23,   24,    5,  208,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */   254,  279,   42,   43,   44,   14,   15,  261,   17,   18,
 /*    50 */   253,  289,   21,   22,   23,   24,  284,  260,   27,   28,
 /*    60 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*    70 */    39,  253,  279,   42,   43,   44,  281,  282,  260,  254,
 /*    80 */    14,   15,  289,   17,   18,  280,  261,   21,   22,   23,
 /*    90 */    24,   90,   92,   27,   28,   29,   30,   31,   32,   33,
 /*   100 */    34,   35,   36,   37,   38,   39,  279,  279,   42,   43,
 /*   110 */    44,   14,   15,   96,   17,   18,  289,  289,   21,   22,
 /*   120 */    23,   24,    0,   92,   27,   28,   29,   30,   31,   32,
 /*   130 */    33,   34,   35,   36,   37,   38,   39,  261,  279,   42,
 /*   140 */    43,   44,  254,   14,   15,    5,   17,   18,  289,  261,
 /*   150 */    21,   22,   23,   24,  208,   89,   27,   28,   29,   30,
 /*   160 */    31,   32,   33,   34,   35,   36,   37,   38,   39,  133,
 /*   170 */   134,   42,   43,   44,   15,  151,   17,   18,   38,   39,
 /*   180 */    21,   22,   23,   24,  160,  161,   27,   28,   29,   30,
 /*   190 */    31,   32,   33,   34,   35,   36,   37,   38,   39,    1,
 /*   200 */    83,   42,   43,   44,   17,   18,  260,    9,   21,   22,
 /*   210 */    23,   24,   90,   96,   27,   28,   29,   30,   31,   32,
 /*   220 */    33,   34,   35,   36,   37,   38,   39,  216,  266,   42,
 /*   230 */    43,   44,  104,  105,  106,  107,  108,  109,  110,  111,
 /*   240 */   112,  113,  114,  115,  116,  117,  118,  285,   50,   51,
 /*   250 */    52,   53,   54,   55,   56,   57,   58,   59,   60,   61,
 /*   260 */    62,   63,   64,  252,   66,  232,  233,  234,  235,  236,
 /*   270 */   237,  238,  239,  240,  241,  242,  243,  244,  245,  246,
 /*   280 */    49,   29,   30,   31,   32,   33,   34,   35,   36,   37,
 /*   290 */    38,   39,  279,    1,   42,   43,   44,   66,  279,    1,
 /*   300 */     2,    9,  289,    5,   73,    7,  261,    9,  289,  216,
 /*   310 */    79,   80,   81,   82,  216,  206,  207,  279,   87,   88,
 /*   320 */    33,   34,   35,   36,   37,   38,   39,  289,  284,   42,
 /*   330 */    43,   44,  286,  287,   88,  289,   38,   39,    1,    2,
 /*   340 */    42,  279,    5,  250,    7,   47,    9,  249,  250,  251,
 /*   350 */   252,  289,   67,   68,   69,  287,  215,  289,  127,   74,
 /*   360 */    75,   76,   77,   78,  279,    2,  208,  258,    5,  258,
 /*   370 */     7,  248,    9,  127,  289,   38,   39,  208,    5,   94,
 /*   380 */    88,  150,    9,  152,   47,  276,   88,  276,  219,  208,
 /*   390 */   159,   25,   73,  104,  163,  106,  107,  208,  109,   73,
 /*   400 */   111,   38,   39,  208,  115,  208,  117,  118,   67,   68,
 /*   410 */    69,   38,   39,  255,   48,   74,  219,   76,   77,   78,
 /*   420 */   279,  215,  124,  125,  126,   88,  232,   86,  234,  235,
 /*   430 */   289,  237,  291,  239,  253,  215,    5,  243,    7,  245,
 /*   440 */   246,  260,  253,   67,   68,   69,  279,  149,  253,  260,
 /*   450 */    74,   75,   76,   77,   78,  260,  289,  248,  208,   88,
 /*   460 */   248,  124,  125,  126,   38,   39,   25,  208,   42,   43,
 /*   470 */    44,  208,  153,  208,  155,  208,  157,  158,    5,  153,
 /*   480 */     7,  155,  248,  157,  158,  279,  149,  124,  125,   48,
 /*   490 */   208,   48,  121,  122,  279,  289,   42,  291,  125,  279,
 /*   500 */    70,   71,   72,  253,  289,  287,   65,  289,   65,  289,
 /*   510 */   260,  291,  253,   42,   43,   44,  253,  208,  253,  260,
 /*   520 */   253,  217,  218,  260,  279,  260,  258,  260,  219,   88,
 /*   530 */   279,   83,   84,   85,  289,  253,   67,   68,   69,  279,
 /*   540 */   289,  279,  260,   81,  276,  279,  230,  231,  279,  289,
 /*   550 */   279,  289,  279,  279,  279,  289,  279,  126,  289,  279,
 /*   560 */   289,   89,  289,  289,  289,  279,  289,  279,  208,  289,
 /*   570 */   279,  214,  279,  103,  262,  289,  214,  289,  221,  219,
 /*   580 */   289,  214,  289,  221,  130,   89,  212,  213,  221,  277,
 /*   590 */    89,   38,   39,   89,   89,   89,  103,  103,   89,  126,
 /*   600 */   128,  131,   89,  141,  103,   89,   89,  103,  103,  103,
 /*   610 */    85,   89,  103,   88,    1,   89,  103,   89,   16,  103,
 /*   620 */   103,   89,   38,   39,  128,  103,   88,  248,  154,  103,
 /*   630 */   156,  103,  154,  248,  156,  103,  154,  256,  156,  145,
 /*   640 */   147,    5,    5,    7,    7,  154,  154,  156,  156,  154,
 /*   650 */   248,  156,  208,  208,  208,   42,  278,  208,  120,  208,
 /*   660 */   208,  208,  258,  208,  208,  258,  208,  208,  208,  208,
 /*   670 */   288,  288,  288,  263,  288,  258,  208,  264,   90,   65,
 /*   680 */   283,  283,  283,  208,  208,  208,  283,  208,  208,  208,
 /*   690 */   126,  208,  208,  208,  208,  208,  208,  208,  208,  275,
 /*   700 */   272,  274,  273,  144,  148,  208,  208,  208,  208,  139,
 /*   710 */   208,  208,  146,  271,  143,  208,  208,  142,  208,  208,
 /*   720 */   208,  136,  208,  208,  137,  135,  124,  208,  208,  208,
 /*   730 */   138,  208,  208,  270,  269,  208,  132,  208,  208,  208,
 /*   740 */   123,  208,  208,  208,  208,  208,   95,  208,  208,  208,
 /*   750 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   760 */   208,  119,  208,  208,  102,  101,   56,  210,   98,  211,
 /*   770 */   210,  210,  210,  210,  100,  210,   60,   99,   97,  131,
 /*   780 */     5,    5,    5,  210,  210,  162,  268,  210,    5,  162,
 /*   790 */     5,  106,  210,  210,  105,  216,  220,  216,  220,  128,
 /*   800 */   151,   88,   85,   89,  129,  103,   88,  103,   89,  210,
 /*   810 */   103,  210,  126,  126,  211,  229,  223,  228,  227,  224,
 /*   820 */   226,  225,  222,  211,  217,  210,  257,  247,  210,  231,
 /*   830 */   211,  211,  210,  265,  267,  210,  264,    5,  212,    5,
 /*   840 */    88,  103,   89,   88,  247,   89,   88,    1,   89,   88,
 /*   850 */   103,   89,   88,   88,   48,  140,    1,  103,   92,   88,
 /*   860 */   140,   88,   88,   85,  121,   85,   75,   83,    5,    9,
 /*   870 */     5,    5,   93,   92,    5,   93,    5,    5,   92,    5,
 /*   880 */    91,   83,    9,   16,    9,   88,    9,    9,   89,   89,
 /*   890 */    88,  124,   28,   64,  126,  156,   17,  156,  103,   17,
 /*   900 */   156,  156,    5,    5,   89,    5,    5,    5,    5,    5,
 /*   910 */     5,    5,    5,  126,    5,    5,    5,    5,    5,    5,
 /*   920 */     5,    5,  103,   65,   91,    0,    9,    9,  292,  292,
 /*   930 */    22,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   940 */   292,   22,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   950 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   960 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   970 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   980 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   990 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1000 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1010 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1020 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1030 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1040 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1050 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1060 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1070 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1080 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1090 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1100 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1110 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1120 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1130 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*  1140 */   292,  292,  292,  292,  292,  292,  292,  292,
};
#define YY_SHIFT_COUNT    (416)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (925)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   231,  128,  128,  289,  289,    1,  298,  337,  337,  337,
 /*    10 */   292,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*    20 */     7,    7,   10,   10,    0,  198,  337,  337,  337,  337,
 /*    30 */   337,  337,  337,  337,  337,  337,  337,  337,  337,  337,
 /*    40 */   337,  337,  337,  337,  337,  337,  337,  337,  337,  337,
 /*    50 */   337,  337,  337,  337,  363,  363,  363,  246,  246,   36,
 /*    60 */     7,  122,    7,    7,    7,    7,    7,  117,    1,   10,
 /*    70 */    10,   17,   17,   20,  942,  942,  942,  363,  363,  363,
 /*    80 */   373,  373,  140,  140,  140,  140,  140,  140,  371,  140,
 /*    90 */     7,    7,    7,    7,    7,    7,  454,    7,    7,    7,
 /*   100 */   246,  246,    7,    7,    7,    7,  462,  462,  462,  462,
 /*   110 */   470,  246,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   120 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   130 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   140 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   150 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   160 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   170 */   556,  614,  588,  614,  614,  614,  614,  564,  564,  564,
 /*   180 */   564,  614,  559,  566,  570,  571,  575,  587,  585,  590,
 /*   190 */   592,  604,  556,  617,  614,  614,  614,  651,  651,  642,
 /*   200 */     1,    1,  614,  614,  662,  664,  710,  670,  674,  716,
 /*   210 */   678,  681,  642,   20,  614,  614,  588,  588,  614,  588,
 /*   220 */   614,  588,  614,  614,  942,  942,   31,   66,   97,   97,
 /*   230 */    97,  129,  159,  187,  252,  252,  252,  252,  252,  252,
 /*   240 */   287,  287,  287,  287,  285,  341,  376,  426,  426,  426,
 /*   250 */   426,  426,  319,  326,  441,   24,  471,  471,  431,  473,
 /*   260 */   448,  430,  469,  501,  472,  496,  553,  504,  505,  506,
 /*   270 */   443,  493,  494,  509,  513,  516,  517,  522,  525,  526,
 /*   280 */   528,  366,  613,  602,  532,  474,  478,  482,  636,  637,
 /*   290 */   584,  491,  492,  538,  495,  648,  775,  623,  776,  777,
 /*   300 */   627,  783,  785,  685,  689,  649,  671,  717,  713,  675,
 /*   310 */   714,  718,  702,  704,  719,  707,  686,  687,  832,  834,
 /*   320 */   752,  753,  755,  756,  758,  759,  738,  761,  762,  764,
 /*   330 */   846,  765,  747,  715,  806,  855,  754,  720,  766,  771,
 /*   340 */   717,  773,  778,  774,  743,  780,  784,  779,  781,  791,
 /*   350 */   863,  782,  786,  860,  865,  866,  869,  871,  872,  874,
 /*   360 */   789,  867,  798,  873,  875,  797,  799,  800,  877,  878,
 /*   370 */   767,  802,  864,  829,  879,  739,  741,  795,  795,  795,
 /*   380 */   795,  768,  787,  882,  744,  745,  795,  795,  795,  897,
 /*   390 */   898,  815,  795,  900,  901,  902,  903,  904,  905,  906,
 /*   400 */   907,  909,  910,  911,  912,  913,  914,  915,  916,  819,
 /*   410 */   833,  917,  908,  918,  919,  858,  925,
};
#define YY_REDUCE_COUNT (225)
#define YY_REDUCE_MIN   (-283)
#define YY_REDUCE_MAX   (626)
static const short yy_reduce_ofst[] = {
 /*     0 */   109,   33,   33,  194,  194,   98,  141,  206,  220, -279,
 /*    10 */  -205, -203, -182,  181,  189,  195,  250,  259,  263,  265,
 /*    20 */   267,  282, -283,   46, -195, -189, -238, -207, -173, -172,
 /*    30 */  -141,   13,   19,   38,   62,   85,  167,  215,  245,  251,
 /*    40 */   260,  262,  266,  269,  271,  273,  274,  275,  277,  280,
 /*    50 */   286,  288,  291,  293, -214, -175, -112,  111,  268,  -38,
 /*    60 */   158,   11,  169,  197,  309,  360,  -54,  357,   93,   68,
 /*    70 */   218,  362,  367,  316,  312,  304,  374, -259, -124,   45,
 /*    80 */  -228,   44,  123,  209,  212,  234,  379,  385,  381,  402,
 /*    90 */   444,  445,  446,  449,  451,  452,  378,  453,  455,  456,
 /*   100 */   404,  407,  458,  459,  460,  461,  382,  383,  384,  386,
 /*   110 */   410,  417,  468,  475,  476,  477,  479,  480,  481,  483,
 /*   120 */   484,  485,  486,  487,  488,  489,  490,  497,  498,  499,
 /*   130 */   500,  502,  503,  507,  508,  510,  511,  512,  514,  515,
 /*   140 */   519,  520,  521,  523,  524,  527,  529,  530,  531,  533,
 /*   150 */   534,  535,  536,  537,  539,  540,  541,  542,  543,  544,
 /*   160 */   545,  546,  547,  548,  549,  550,  551,  552,  554,  555,
 /*   170 */   413,  557,  558,  560,  561,  562,  563,  397,  398,  399,
 /*   180 */   403,  565,  424,  427,  429,  428,  442,  463,  465,  518,
 /*   190 */   567,  568,  572,  569,  573,  574,  577,  576,  578,  580,
 /*   200 */   579,  581,  582,  583,  586,  589,  591,  593,  594,  595,
 /*   210 */   596,  600,  597,  598,  599,  601,  603,  612,  615,  619,
 /*   220 */   618,  620,  622,  625,  607,  626,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   978, 1103, 1041, 1114, 1028, 1038, 1288, 1288, 1288, 1288,
 /*    10 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*    20 */   978,  978,  978,  978, 1168,  998,  978,  978,  978,  978,
 /*    30 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*    40 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*    50 */   978,  978,  978,  978,  978,  978,  978,  978,  978, 1192,
 /*    60 */   978, 1038,  978,  978,  978,  978,  978, 1049, 1038,  978,
 /*    70 */   978, 1049, 1049,  978, 1163, 1087, 1105,  978,  978,  978,
 /*    80 */   978,  978,  978,  978,  978,  978,  978,  978, 1135,  978,
 /*    90 */   978,  978,  978,  978,  978,  978, 1170, 1176, 1173,  978,
 /*   100 */   978,  978, 1178,  978,  978,  978, 1214, 1214, 1214, 1214,
 /*   110 */  1161,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   120 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   130 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   140 */   978,  978,  978,  978,  978, 1026,  978, 1024,  978,  978,
 /*   150 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   160 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  996,
 /*   170 */  1231, 1000, 1036, 1000, 1000, 1000, 1000,  978,  978,  978,
 /*   180 */   978, 1000, 1223, 1227, 1204, 1221, 1215, 1199, 1197, 1195,
 /*   190 */  1203, 1188, 1231, 1137, 1000, 1000, 1000, 1047, 1047, 1042,
 /*   200 */  1038, 1038, 1000, 1000, 1065, 1063, 1061, 1053, 1059, 1055,
 /*   210 */  1057, 1051, 1029,  978, 1000, 1000, 1036, 1036, 1000, 1036,
 /*   220 */  1000, 1036, 1000, 1000, 1087, 1105, 1287,  978, 1232, 1222,
 /*   230 */  1287,  978, 1264, 1263, 1278, 1277, 1276, 1262, 1261, 1260,
 /*   240 */  1256, 1259, 1258, 1257,  978,  978,  978, 1275, 1274, 1272,
 /*   250 */  1271, 1270,  978,  978, 1234,  978, 1266, 1265,  978,  978,
 /*   260 */   978,  978,  978,  978,  978,  978, 1185,  978,  978,  978,
 /*   270 */  1210, 1228, 1224,  978,  978,  978,  978,  978,  978,  978,
 /*   280 */   978, 1235,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   290 */  1149,  978,  978, 1116,  978,  978,  978,  978,  978,  978,
 /*   300 */   978,  978,  978,  978,  978,  978, 1160,  978,  978,  978,
 /*   310 */   978,  978, 1172, 1171,  978,  978,  978,  978,  978,  978,
 /*   320 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   330 */   978,  978, 1216,  978, 1211,  978, 1205,  978,  978,  978,
 /*   340 */  1128,  978,  978,  978,  978, 1045,  978,  978,  978,  978,
 /*   350 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   360 */   978,  978,  978,  978,  978,  978,  978,  978,  978,  978,
 /*   370 */   978,  978,  978,  978,  978,  978,  978, 1306, 1301, 1302,
 /*   380 */  1299,  978,  978,  978,  978,  978, 1298, 1293, 1294,  978,
 /*   390 */   978,  978, 1291,  978,  978,  978,  978,  978,  978,  978,
 /*   400 */   978,  978,  978,  978,  978,  978,  978,  978,  978, 1071,
 /*   410 */   978,  978, 1007,  978, 1005,  978,  978,
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
  /*   85 */ "TAGS",
  /*   86 */ "LOCAL",
  /*   87 */ "COMPACT",
  /*   88 */ "LP",
  /*   89 */ "RP",
  /*   90 */ "IF",
  /*   91 */ "EXISTS",
  /*   92 */ "AS",
  /*   93 */ "OUTPUTTYPE",
  /*   94 */ "AGGREGATE",
  /*   95 */ "BUFSIZE",
  /*   96 */ "PPS",
  /*   97 */ "TSERIES",
  /*   98 */ "DBS",
  /*   99 */ "STORAGE",
  /*  100 */ "QTIME",
  /*  101 */ "CONNS",
  /*  102 */ "STATE",
  /*  103 */ "COMMA",
  /*  104 */ "KEEP",
  /*  105 */ "CACHE",
  /*  106 */ "REPLICA",
  /*  107 */ "QUORUM",
  /*  108 */ "DAYS",
  /*  109 */ "MINROWS",
  /*  110 */ "MAXROWS",
  /*  111 */ "BLOCKS",
  /*  112 */ "CTIME",
  /*  113 */ "WAL",
  /*  114 */ "FSYNC",
  /*  115 */ "COMP",
  /*  116 */ "PRECISION",
  /*  117 */ "UPDATE",
  /*  118 */ "CACHELAST",
  /*  119 */ "PARTITIONS",
  /*  120 */ "UNSIGNED",
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
  /*  206 */ "program",
  /*  207 */ "cmd",
  /*  208 */ "ids",
  /*  209 */ "dbPrefix",
  /*  210 */ "cpxName",
  /*  211 */ "ifexists",
  /*  212 */ "alter_db_optr",
  /*  213 */ "alter_topic_optr",
  /*  214 */ "acct_optr",
  /*  215 */ "exprlist",
  /*  216 */ "ifnotexists",
  /*  217 */ "db_optr",
  /*  218 */ "topic_optr",
  /*  219 */ "typename",
  /*  220 */ "bufsize",
  /*  221 */ "pps",
  /*  222 */ "tseries",
  /*  223 */ "dbs",
  /*  224 */ "streams",
  /*  225 */ "storage",
  /*  226 */ "qtime",
  /*  227 */ "users",
  /*  228 */ "conns",
  /*  229 */ "state",
  /*  230 */ "intitemlist",
  /*  231 */ "intitem",
  /*  232 */ "keep",
  /*  233 */ "cache",
  /*  234 */ "replica",
  /*  235 */ "quorum",
  /*  236 */ "days",
  /*  237 */ "minrows",
  /*  238 */ "maxrows",
  /*  239 */ "blocks",
  /*  240 */ "ctime",
  /*  241 */ "wal",
  /*  242 */ "fsync",
  /*  243 */ "comp",
  /*  244 */ "prec",
  /*  245 */ "update",
  /*  246 */ "cachelast",
  /*  247 */ "partitions",
  /*  248 */ "signed",
  /*  249 */ "create_table_args",
  /*  250 */ "create_stable_args",
  /*  251 */ "create_table_list",
  /*  252 */ "create_from_stable",
  /*  253 */ "columnlist",
  /*  254 */ "tagitemlist",
  /*  255 */ "tagNamelist",
  /*  256 */ "to_opt",
  /*  257 */ "split_opt",
  /*  258 */ "select",
  /*  259 */ "to_split",
  /*  260 */ "column",
  /*  261 */ "tagitem",
  /*  262 */ "selcollist",
  /*  263 */ "from",
  /*  264 */ "where_opt",
  /*  265 */ "range_option",
  /*  266 */ "interval_option",
  /*  267 */ "sliding_opt",
  /*  268 */ "session_option",
  /*  269 */ "windowstate_option",
  /*  270 */ "fill_opt",
  /*  271 */ "groupby_opt",
  /*  272 */ "having_opt",
  /*  273 */ "orderby_opt",
  /*  274 */ "slimit_opt",
  /*  275 */ "limit_opt",
  /*  276 */ "union",
  /*  277 */ "sclp",
  /*  278 */ "distinct",
  /*  279 */ "expr",
  /*  280 */ "as",
  /*  281 */ "tablelist",
  /*  282 */ "sub",
  /*  283 */ "tmvar",
  /*  284 */ "timestamp",
  /*  285 */ "intervalKey",
  /*  286 */ "sortlist",
  /*  287 */ "item",
  /*  288 */ "sortorder",
  /*  289 */ "arrow",
  /*  290 */ "grouplist",
  /*  291 */ "expritem",
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
 /*  42 */ "cmd ::= ALTER USER ids TAGS ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids",
 /*  44 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids",
 /*  46 */ "cmd ::= ALTER LOCAL ids ids",
 /*  47 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  48 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  50 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  51 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  52 */ "ids ::= ID",
 /*  53 */ "ids ::= STRING",
 /*  54 */ "ifexists ::= IF EXISTS",
 /*  55 */ "ifexists ::=",
 /*  56 */ "ifnotexists ::= IF NOT EXISTS",
 /*  57 */ "ifnotexists ::=",
 /*  58 */ "cmd ::= CREATE DNODE ids",
 /*  59 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  60 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  61 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  62 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  63 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  64 */ "cmd ::= CREATE USER ids PASS ids",
 /*  65 */ "cmd ::= CREATE USER ids PASS ids TAGS ids",
 /*  66 */ "bufsize ::=",
 /*  67 */ "bufsize ::= BUFSIZE INTEGER",
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
 /* 132 */ "alter_db_optr ::= alter_db_optr minrows",
 /* 133 */ "alter_topic_optr ::= alter_db_optr",
 /* 134 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 135 */ "typename ::= ids",
 /* 136 */ "typename ::= ids LP signed RP",
 /* 137 */ "typename ::= ids UNSIGNED",
 /* 138 */ "signed ::= INTEGER",
 /* 139 */ "signed ::= PLUS INTEGER",
 /* 140 */ "signed ::= MINUS INTEGER",
 /* 141 */ "cmd ::= CREATE TABLE create_table_args",
 /* 142 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 143 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 144 */ "cmd ::= CREATE TABLE create_table_list",
 /* 145 */ "create_table_list ::= create_from_stable",
 /* 146 */ "create_table_list ::= create_table_list create_from_stable",
 /* 147 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 148 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 149 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 150 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 151 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 152 */ "tagNamelist ::= ids",
 /* 153 */ "create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select",
 /* 154 */ "to_opt ::=",
 /* 155 */ "to_opt ::= TO ids cpxName",
 /* 156 */ "split_opt ::=",
 /* 157 */ "split_opt ::= SPLIT ids",
 /* 158 */ "columnlist ::= columnlist COMMA column",
 /* 159 */ "columnlist ::= column",
 /* 160 */ "column ::= ids typename",
 /* 161 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 162 */ "tagitemlist ::= tagitem",
 /* 163 */ "tagitem ::= INTEGER",
 /* 164 */ "tagitem ::= FLOAT",
 /* 165 */ "tagitem ::= STRING",
 /* 166 */ "tagitem ::= BOOL",
 /* 167 */ "tagitem ::= NULL",
 /* 168 */ "tagitem ::= NOW",
 /* 169 */ "tagitem ::= NOW PLUS VARIABLE",
 /* 170 */ "tagitem ::= NOW MINUS VARIABLE",
 /* 171 */ "tagitem ::= MINUS INTEGER",
 /* 172 */ "tagitem ::= MINUS FLOAT",
 /* 173 */ "tagitem ::= PLUS INTEGER",
 /* 174 */ "tagitem ::= PLUS FLOAT",
 /* 175 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 176 */ "select ::= LP select RP",
 /* 177 */ "union ::= select",
 /* 178 */ "union ::= union UNION ALL select",
 /* 179 */ "cmd ::= union",
 /* 180 */ "select ::= SELECT selcollist",
 /* 181 */ "sclp ::= selcollist COMMA",
 /* 182 */ "sclp ::=",
 /* 183 */ "selcollist ::= sclp distinct expr as",
 /* 184 */ "selcollist ::= sclp STAR",
 /* 185 */ "as ::= AS ids",
 /* 186 */ "as ::= ids",
 /* 187 */ "as ::=",
 /* 188 */ "distinct ::= DISTINCT",
 /* 189 */ "distinct ::=",
 /* 190 */ "from ::= FROM tablelist",
 /* 191 */ "from ::= FROM sub",
 /* 192 */ "sub ::= LP union RP",
 /* 193 */ "sub ::= LP union RP ids",
 /* 194 */ "sub ::= sub COMMA LP union RP ids",
 /* 195 */ "tablelist ::= ids cpxName",
 /* 196 */ "tablelist ::= ids cpxName ids",
 /* 197 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 198 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 199 */ "tmvar ::= VARIABLE",
 /* 200 */ "timestamp ::= INTEGER",
 /* 201 */ "timestamp ::= MINUS INTEGER",
 /* 202 */ "timestamp ::= PLUS INTEGER",
 /* 203 */ "timestamp ::= STRING",
 /* 204 */ "timestamp ::= NOW",
 /* 205 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 206 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 207 */ "range_option ::=",
 /* 208 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 209 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 210 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 211 */ "interval_option ::=",
 /* 212 */ "intervalKey ::= INTERVAL",
 /* 213 */ "intervalKey ::= EVERY",
 /* 214 */ "session_option ::=",
 /* 215 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 216 */ "windowstate_option ::=",
 /* 217 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 218 */ "fill_opt ::=",
 /* 219 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 220 */ "fill_opt ::= FILL LP ID RP",
 /* 221 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 222 */ "sliding_opt ::=",
 /* 223 */ "orderby_opt ::=",
 /* 224 */ "orderby_opt ::= ORDER BY sortlist",
 /* 225 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 226 */ "sortlist ::= sortlist COMMA arrow sortorder",
 /* 227 */ "sortlist ::= item sortorder",
 /* 228 */ "sortlist ::= arrow sortorder",
 /* 229 */ "item ::= ID",
 /* 230 */ "item ::= ID DOT ID",
 /* 231 */ "sortorder ::= ASC",
 /* 232 */ "sortorder ::= DESC",
 /* 233 */ "sortorder ::=",
 /* 234 */ "groupby_opt ::=",
 /* 235 */ "groupby_opt ::= GROUP BY grouplist",
 /* 236 */ "grouplist ::= grouplist COMMA item",
 /* 237 */ "grouplist ::= grouplist COMMA arrow",
 /* 238 */ "grouplist ::= item",
 /* 239 */ "grouplist ::= arrow",
 /* 240 */ "having_opt ::=",
 /* 241 */ "having_opt ::= HAVING expr",
 /* 242 */ "limit_opt ::=",
 /* 243 */ "limit_opt ::= LIMIT signed",
 /* 244 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 245 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 246 */ "slimit_opt ::=",
 /* 247 */ "slimit_opt ::= SLIMIT signed",
 /* 248 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 249 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 250 */ "where_opt ::=",
 /* 251 */ "where_opt ::= WHERE expr",
 /* 252 */ "expr ::= LP expr RP",
 /* 253 */ "expr ::= ID",
 /* 254 */ "expr ::= ID DOT ID",
 /* 255 */ "expr ::= ID DOT STAR",
 /* 256 */ "expr ::= INTEGER",
 /* 257 */ "expr ::= MINUS INTEGER",
 /* 258 */ "expr ::= PLUS INTEGER",
 /* 259 */ "expr ::= FLOAT",
 /* 260 */ "expr ::= MINUS FLOAT",
 /* 261 */ "expr ::= PLUS FLOAT",
 /* 262 */ "expr ::= STRING",
 /* 263 */ "expr ::= NOW",
 /* 264 */ "expr ::= TODAY",
 /* 265 */ "expr ::= VARIABLE",
 /* 266 */ "expr ::= PLUS VARIABLE",
 /* 267 */ "expr ::= MINUS VARIABLE",
 /* 268 */ "expr ::= BOOL",
 /* 269 */ "expr ::= NULL",
 /* 270 */ "expr ::= ID LP exprlist RP",
 /* 271 */ "expr ::= ID LP STAR RP",
 /* 272 */ "expr ::= ID LP expr AS typename RP",
 /* 273 */ "expr ::= expr IS NULL",
 /* 274 */ "expr ::= expr IS NOT NULL",
 /* 275 */ "expr ::= expr LT expr",
 /* 276 */ "expr ::= expr GT expr",
 /* 277 */ "expr ::= expr LE expr",
 /* 278 */ "expr ::= expr GE expr",
 /* 279 */ "expr ::= expr NE expr",
 /* 280 */ "expr ::= expr EQ expr",
 /* 281 */ "expr ::= expr BETWEEN expr AND expr",
 /* 282 */ "expr ::= expr AND expr",
 /* 283 */ "expr ::= expr OR expr",
 /* 284 */ "expr ::= expr PLUS expr",
 /* 285 */ "expr ::= expr MINUS expr",
 /* 286 */ "expr ::= expr STAR expr",
 /* 287 */ "expr ::= expr SLASH expr",
 /* 288 */ "expr ::= expr REM expr",
 /* 289 */ "expr ::= expr BITAND expr",
 /* 290 */ "expr ::= expr BITOR expr",
 /* 291 */ "expr ::= expr BITXOR expr",
 /* 292 */ "expr ::= BITNOT expr",
 /* 293 */ "expr ::= expr LSHIFT expr",
 /* 294 */ "expr ::= expr RSHIFT expr",
 /* 295 */ "expr ::= expr LIKE expr",
 /* 296 */ "expr ::= expr MATCH expr",
 /* 297 */ "expr ::= expr NMATCH expr",
 /* 298 */ "expr ::= ID CONTAINS STRING",
 /* 299 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 300 */ "arrow ::= ID ARROW STRING",
 /* 301 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 302 */ "expr ::= arrow",
 /* 303 */ "expr ::= expr IN LP exprlist RP",
 /* 304 */ "exprlist ::= exprlist COMMA expritem",
 /* 305 */ "exprlist ::= expritem",
 /* 306 */ "expritem ::= expr",
 /* 307 */ "expritem ::=",
 /* 308 */ "cmd ::= RESET QUERY CACHE",
 /* 309 */ "cmd ::= SYNCDB ids REPLICA",
 /* 310 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 311 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 312 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 313 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 314 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 315 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 316 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 317 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 318 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 319 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 320 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 321 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 322 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 323 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 324 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 325 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 326 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 327 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 328 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
 /* 329 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
    case 215: /* exprlist */
    case 262: /* selcollist */
    case 277: /* sclp */
{
tSqlExprListDestroy((yypminor->yy333));
}
      break;
    case 230: /* intitemlist */
    case 232: /* keep */
    case 253: /* columnlist */
    case 254: /* tagitemlist */
    case 255: /* tagNamelist */
    case 270: /* fill_opt */
    case 271: /* groupby_opt */
    case 273: /* orderby_opt */
    case 286: /* sortlist */
    case 290: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy333));
}
      break;
    case 251: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy78));
}
      break;
    case 258: /* select */
{
destroySqlNode((yypminor->yy144));
}
      break;
    case 263: /* from */
    case 281: /* tablelist */
    case 282: /* sub */
{
destroyRelationInfo((yypminor->yy516));
}
      break;
    case 264: /* where_opt */
    case 272: /* having_opt */
    case 279: /* expr */
    case 284: /* timestamp */
    case 289: /* arrow */
    case 291: /* expritem */
{
tSqlExprDestroy((yypminor->yy194));
}
      break;
    case 276: /* union */
{
destroyAllSqlNode((yypminor->yy333));
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
   206,  /* (0) program ::= cmd */
   207,  /* (1) cmd ::= SHOW DATABASES */
   207,  /* (2) cmd ::= SHOW TOPICS */
   207,  /* (3) cmd ::= SHOW FUNCTIONS */
   207,  /* (4) cmd ::= SHOW MNODES */
   207,  /* (5) cmd ::= SHOW DNODES */
   207,  /* (6) cmd ::= SHOW ACCOUNTS */
   207,  /* (7) cmd ::= SHOW USERS */
   207,  /* (8) cmd ::= SHOW MODULES */
   207,  /* (9) cmd ::= SHOW QUERIES */
   207,  /* (10) cmd ::= SHOW CONNECTIONS */
   207,  /* (11) cmd ::= SHOW STREAMS */
   207,  /* (12) cmd ::= SHOW VARIABLES */
   207,  /* (13) cmd ::= SHOW SCORES */
   207,  /* (14) cmd ::= SHOW GRANTS */
   207,  /* (15) cmd ::= SHOW VNODES */
   207,  /* (16) cmd ::= SHOW VNODES ids */
   209,  /* (17) dbPrefix ::= */
   209,  /* (18) dbPrefix ::= ids DOT */
   210,  /* (19) cpxName ::= */
   210,  /* (20) cpxName ::= DOT ids */
   207,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   207,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   207,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   207,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   207,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
   207,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   207,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
   207,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   207,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   207,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   207,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   207,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   207,  /* (33) cmd ::= DROP FUNCTION ids */
   207,  /* (34) cmd ::= DROP DNODE ids */
   207,  /* (35) cmd ::= DROP USER ids */
   207,  /* (36) cmd ::= DROP ACCOUNT ids */
   207,  /* (37) cmd ::= USE ids */
   207,  /* (38) cmd ::= DESCRIBE ids cpxName */
   207,  /* (39) cmd ::= DESC ids cpxName */
   207,  /* (40) cmd ::= ALTER USER ids PASS ids */
   207,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   207,  /* (42) cmd ::= ALTER USER ids TAGS ids */
   207,  /* (43) cmd ::= ALTER DNODE ids ids */
   207,  /* (44) cmd ::= ALTER DNODE ids ids ids */
   207,  /* (45) cmd ::= ALTER LOCAL ids */
   207,  /* (46) cmd ::= ALTER LOCAL ids ids */
   207,  /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
   207,  /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
   207,  /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
   207,  /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   207,  /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
   208,  /* (52) ids ::= ID */
   208,  /* (53) ids ::= STRING */
   211,  /* (54) ifexists ::= IF EXISTS */
   211,  /* (55) ifexists ::= */
   216,  /* (56) ifnotexists ::= IF NOT EXISTS */
   216,  /* (57) ifnotexists ::= */
   207,  /* (58) cmd ::= CREATE DNODE ids */
   207,  /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   207,  /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   207,  /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   207,  /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   207,  /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   207,  /* (64) cmd ::= CREATE USER ids PASS ids */
   207,  /* (65) cmd ::= CREATE USER ids PASS ids TAGS ids */
   220,  /* (66) bufsize ::= */
   220,  /* (67) bufsize ::= BUFSIZE INTEGER */
   221,  /* (68) pps ::= */
   221,  /* (69) pps ::= PPS INTEGER */
   222,  /* (70) tseries ::= */
   222,  /* (71) tseries ::= TSERIES INTEGER */
   223,  /* (72) dbs ::= */
   223,  /* (73) dbs ::= DBS INTEGER */
   224,  /* (74) streams ::= */
   224,  /* (75) streams ::= STREAMS INTEGER */
   225,  /* (76) storage ::= */
   225,  /* (77) storage ::= STORAGE INTEGER */
   226,  /* (78) qtime ::= */
   226,  /* (79) qtime ::= QTIME INTEGER */
   227,  /* (80) users ::= */
   227,  /* (81) users ::= USERS INTEGER */
   228,  /* (82) conns ::= */
   228,  /* (83) conns ::= CONNS INTEGER */
   229,  /* (84) state ::= */
   229,  /* (85) state ::= STATE ids */
   214,  /* (86) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   230,  /* (87) intitemlist ::= intitemlist COMMA intitem */
   230,  /* (88) intitemlist ::= intitem */
   231,  /* (89) intitem ::= INTEGER */
   232,  /* (90) keep ::= KEEP intitemlist */
   233,  /* (91) cache ::= CACHE INTEGER */
   234,  /* (92) replica ::= REPLICA INTEGER */
   235,  /* (93) quorum ::= QUORUM INTEGER */
   236,  /* (94) days ::= DAYS INTEGER */
   237,  /* (95) minrows ::= MINROWS INTEGER */
   238,  /* (96) maxrows ::= MAXROWS INTEGER */
   239,  /* (97) blocks ::= BLOCKS INTEGER */
   240,  /* (98) ctime ::= CTIME INTEGER */
   241,  /* (99) wal ::= WAL INTEGER */
   242,  /* (100) fsync ::= FSYNC INTEGER */
   243,  /* (101) comp ::= COMP INTEGER */
   244,  /* (102) prec ::= PRECISION STRING */
   245,  /* (103) update ::= UPDATE INTEGER */
   246,  /* (104) cachelast ::= CACHELAST INTEGER */
   247,  /* (105) partitions ::= PARTITIONS INTEGER */
   217,  /* (106) db_optr ::= */
   217,  /* (107) db_optr ::= db_optr cache */
   217,  /* (108) db_optr ::= db_optr replica */
   217,  /* (109) db_optr ::= db_optr quorum */
   217,  /* (110) db_optr ::= db_optr days */
   217,  /* (111) db_optr ::= db_optr minrows */
   217,  /* (112) db_optr ::= db_optr maxrows */
   217,  /* (113) db_optr ::= db_optr blocks */
   217,  /* (114) db_optr ::= db_optr ctime */
   217,  /* (115) db_optr ::= db_optr wal */
   217,  /* (116) db_optr ::= db_optr fsync */
   217,  /* (117) db_optr ::= db_optr comp */
   217,  /* (118) db_optr ::= db_optr prec */
   217,  /* (119) db_optr ::= db_optr keep */
   217,  /* (120) db_optr ::= db_optr update */
   217,  /* (121) db_optr ::= db_optr cachelast */
   218,  /* (122) topic_optr ::= db_optr */
   218,  /* (123) topic_optr ::= topic_optr partitions */
   212,  /* (124) alter_db_optr ::= */
   212,  /* (125) alter_db_optr ::= alter_db_optr replica */
   212,  /* (126) alter_db_optr ::= alter_db_optr quorum */
   212,  /* (127) alter_db_optr ::= alter_db_optr keep */
   212,  /* (128) alter_db_optr ::= alter_db_optr blocks */
   212,  /* (129) alter_db_optr ::= alter_db_optr comp */
   212,  /* (130) alter_db_optr ::= alter_db_optr update */
   212,  /* (131) alter_db_optr ::= alter_db_optr cachelast */
   212,  /* (132) alter_db_optr ::= alter_db_optr minrows */
   213,  /* (133) alter_topic_optr ::= alter_db_optr */
   213,  /* (134) alter_topic_optr ::= alter_topic_optr partitions */
   219,  /* (135) typename ::= ids */
   219,  /* (136) typename ::= ids LP signed RP */
   219,  /* (137) typename ::= ids UNSIGNED */
   248,  /* (138) signed ::= INTEGER */
   248,  /* (139) signed ::= PLUS INTEGER */
   248,  /* (140) signed ::= MINUS INTEGER */
   207,  /* (141) cmd ::= CREATE TABLE create_table_args */
   207,  /* (142) cmd ::= CREATE TABLE create_stable_args */
   207,  /* (143) cmd ::= CREATE STABLE create_stable_args */
   207,  /* (144) cmd ::= CREATE TABLE create_table_list */
   251,  /* (145) create_table_list ::= create_from_stable */
   251,  /* (146) create_table_list ::= create_table_list create_from_stable */
   249,  /* (147) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   250,  /* (148) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   252,  /* (149) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   252,  /* (150) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   255,  /* (151) tagNamelist ::= tagNamelist COMMA ids */
   255,  /* (152) tagNamelist ::= ids */
   249,  /* (153) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
   256,  /* (154) to_opt ::= */
   256,  /* (155) to_opt ::= TO ids cpxName */
   257,  /* (156) split_opt ::= */
   257,  /* (157) split_opt ::= SPLIT ids */
   253,  /* (158) columnlist ::= columnlist COMMA column */
   253,  /* (159) columnlist ::= column */
   260,  /* (160) column ::= ids typename */
   254,  /* (161) tagitemlist ::= tagitemlist COMMA tagitem */
   254,  /* (162) tagitemlist ::= tagitem */
   261,  /* (163) tagitem ::= INTEGER */
   261,  /* (164) tagitem ::= FLOAT */
   261,  /* (165) tagitem ::= STRING */
   261,  /* (166) tagitem ::= BOOL */
   261,  /* (167) tagitem ::= NULL */
   261,  /* (168) tagitem ::= NOW */
   261,  /* (169) tagitem ::= NOW PLUS VARIABLE */
   261,  /* (170) tagitem ::= NOW MINUS VARIABLE */
   261,  /* (171) tagitem ::= MINUS INTEGER */
   261,  /* (172) tagitem ::= MINUS FLOAT */
   261,  /* (173) tagitem ::= PLUS INTEGER */
   261,  /* (174) tagitem ::= PLUS FLOAT */
   258,  /* (175) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   258,  /* (176) select ::= LP select RP */
   276,  /* (177) union ::= select */
   276,  /* (178) union ::= union UNION ALL select */
   207,  /* (179) cmd ::= union */
   258,  /* (180) select ::= SELECT selcollist */
   277,  /* (181) sclp ::= selcollist COMMA */
   277,  /* (182) sclp ::= */
   262,  /* (183) selcollist ::= sclp distinct expr as */
   262,  /* (184) selcollist ::= sclp STAR */
   280,  /* (185) as ::= AS ids */
   280,  /* (186) as ::= ids */
   280,  /* (187) as ::= */
   278,  /* (188) distinct ::= DISTINCT */
   278,  /* (189) distinct ::= */
   263,  /* (190) from ::= FROM tablelist */
   263,  /* (191) from ::= FROM sub */
   282,  /* (192) sub ::= LP union RP */
   282,  /* (193) sub ::= LP union RP ids */
   282,  /* (194) sub ::= sub COMMA LP union RP ids */
   281,  /* (195) tablelist ::= ids cpxName */
   281,  /* (196) tablelist ::= ids cpxName ids */
   281,  /* (197) tablelist ::= tablelist COMMA ids cpxName */
   281,  /* (198) tablelist ::= tablelist COMMA ids cpxName ids */
   283,  /* (199) tmvar ::= VARIABLE */
   284,  /* (200) timestamp ::= INTEGER */
   284,  /* (201) timestamp ::= MINUS INTEGER */
   284,  /* (202) timestamp ::= PLUS INTEGER */
   284,  /* (203) timestamp ::= STRING */
   284,  /* (204) timestamp ::= NOW */
   284,  /* (205) timestamp ::= NOW PLUS VARIABLE */
   284,  /* (206) timestamp ::= NOW MINUS VARIABLE */
   265,  /* (207) range_option ::= */
   265,  /* (208) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   266,  /* (209) interval_option ::= intervalKey LP tmvar RP */
   266,  /* (210) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   266,  /* (211) interval_option ::= */
   285,  /* (212) intervalKey ::= INTERVAL */
   285,  /* (213) intervalKey ::= EVERY */
   268,  /* (214) session_option ::= */
   268,  /* (215) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   269,  /* (216) windowstate_option ::= */
   269,  /* (217) windowstate_option ::= STATE_WINDOW LP ids RP */
   270,  /* (218) fill_opt ::= */
   270,  /* (219) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   270,  /* (220) fill_opt ::= FILL LP ID RP */
   267,  /* (221) sliding_opt ::= SLIDING LP tmvar RP */
   267,  /* (222) sliding_opt ::= */
   273,  /* (223) orderby_opt ::= */
   273,  /* (224) orderby_opt ::= ORDER BY sortlist */
   286,  /* (225) sortlist ::= sortlist COMMA item sortorder */
   286,  /* (226) sortlist ::= sortlist COMMA arrow sortorder */
   286,  /* (227) sortlist ::= item sortorder */
   286,  /* (228) sortlist ::= arrow sortorder */
   287,  /* (229) item ::= ID */
   287,  /* (230) item ::= ID DOT ID */
   288,  /* (231) sortorder ::= ASC */
   288,  /* (232) sortorder ::= DESC */
   288,  /* (233) sortorder ::= */
   271,  /* (234) groupby_opt ::= */
   271,  /* (235) groupby_opt ::= GROUP BY grouplist */
   290,  /* (236) grouplist ::= grouplist COMMA item */
   290,  /* (237) grouplist ::= grouplist COMMA arrow */
   290,  /* (238) grouplist ::= item */
   290,  /* (239) grouplist ::= arrow */
   272,  /* (240) having_opt ::= */
   272,  /* (241) having_opt ::= HAVING expr */
   275,  /* (242) limit_opt ::= */
   275,  /* (243) limit_opt ::= LIMIT signed */
   275,  /* (244) limit_opt ::= LIMIT signed OFFSET signed */
   275,  /* (245) limit_opt ::= LIMIT signed COMMA signed */
   274,  /* (246) slimit_opt ::= */
   274,  /* (247) slimit_opt ::= SLIMIT signed */
   274,  /* (248) slimit_opt ::= SLIMIT signed SOFFSET signed */
   274,  /* (249) slimit_opt ::= SLIMIT signed COMMA signed */
   264,  /* (250) where_opt ::= */
   264,  /* (251) where_opt ::= WHERE expr */
   279,  /* (252) expr ::= LP expr RP */
   279,  /* (253) expr ::= ID */
   279,  /* (254) expr ::= ID DOT ID */
   279,  /* (255) expr ::= ID DOT STAR */
   279,  /* (256) expr ::= INTEGER */
   279,  /* (257) expr ::= MINUS INTEGER */
   279,  /* (258) expr ::= PLUS INTEGER */
   279,  /* (259) expr ::= FLOAT */
   279,  /* (260) expr ::= MINUS FLOAT */
   279,  /* (261) expr ::= PLUS FLOAT */
   279,  /* (262) expr ::= STRING */
   279,  /* (263) expr ::= NOW */
   279,  /* (264) expr ::= TODAY */
   279,  /* (265) expr ::= VARIABLE */
   279,  /* (266) expr ::= PLUS VARIABLE */
   279,  /* (267) expr ::= MINUS VARIABLE */
   279,  /* (268) expr ::= BOOL */
   279,  /* (269) expr ::= NULL */
   279,  /* (270) expr ::= ID LP exprlist RP */
   279,  /* (271) expr ::= ID LP STAR RP */
   279,  /* (272) expr ::= ID LP expr AS typename RP */
   279,  /* (273) expr ::= expr IS NULL */
   279,  /* (274) expr ::= expr IS NOT NULL */
   279,  /* (275) expr ::= expr LT expr */
   279,  /* (276) expr ::= expr GT expr */
   279,  /* (277) expr ::= expr LE expr */
   279,  /* (278) expr ::= expr GE expr */
   279,  /* (279) expr ::= expr NE expr */
   279,  /* (280) expr ::= expr EQ expr */
   279,  /* (281) expr ::= expr BETWEEN expr AND expr */
   279,  /* (282) expr ::= expr AND expr */
   279,  /* (283) expr ::= expr OR expr */
   279,  /* (284) expr ::= expr PLUS expr */
   279,  /* (285) expr ::= expr MINUS expr */
   279,  /* (286) expr ::= expr STAR expr */
   279,  /* (287) expr ::= expr SLASH expr */
   279,  /* (288) expr ::= expr REM expr */
   279,  /* (289) expr ::= expr BITAND expr */
   279,  /* (290) expr ::= expr BITOR expr */
   279,  /* (291) expr ::= expr BITXOR expr */
   279,  /* (292) expr ::= BITNOT expr */
   279,  /* (293) expr ::= expr LSHIFT expr */
   279,  /* (294) expr ::= expr RSHIFT expr */
   279,  /* (295) expr ::= expr LIKE expr */
   279,  /* (296) expr ::= expr MATCH expr */
   279,  /* (297) expr ::= expr NMATCH expr */
   279,  /* (298) expr ::= ID CONTAINS STRING */
   279,  /* (299) expr ::= ID DOT ID CONTAINS STRING */
   289,  /* (300) arrow ::= ID ARROW STRING */
   289,  /* (301) arrow ::= ID DOT ID ARROW STRING */
   279,  /* (302) expr ::= arrow */
   279,  /* (303) expr ::= expr IN LP exprlist RP */
   215,  /* (304) exprlist ::= exprlist COMMA expritem */
   215,  /* (305) exprlist ::= expritem */
   291,  /* (306) expritem ::= expr */
   291,  /* (307) expritem ::= */
   207,  /* (308) cmd ::= RESET QUERY CACHE */
   207,  /* (309) cmd ::= SYNCDB ids REPLICA */
   207,  /* (310) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   207,  /* (311) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   207,  /* (312) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   207,  /* (313) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   207,  /* (314) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   207,  /* (315) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   207,  /* (316) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   207,  /* (317) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   207,  /* (318) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   207,  /* (319) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   207,  /* (320) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   207,  /* (321) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   207,  /* (322) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   207,  /* (323) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   207,  /* (324) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   207,  /* (325) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   207,  /* (326) cmd ::= KILL CONNECTION INTEGER */
   207,  /* (327) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   207,  /* (328) cmd ::= KILL QUERY INTEGER COLON INTEGER */
   207,  /* (329) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
   -5,  /* (42) cmd ::= ALTER USER ids TAGS ids */
   -4,  /* (43) cmd ::= ALTER DNODE ids ids */
   -5,  /* (44) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (45) cmd ::= ALTER LOCAL ids */
   -4,  /* (46) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -6,  /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
   -1,  /* (52) ids ::= ID */
   -1,  /* (53) ids ::= STRING */
   -2,  /* (54) ifexists ::= IF EXISTS */
    0,  /* (55) ifexists ::= */
   -3,  /* (56) ifnotexists ::= IF NOT EXISTS */
    0,  /* (57) ifnotexists ::= */
   -3,  /* (58) cmd ::= CREATE DNODE ids */
   -6,  /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -8,  /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -9,  /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -5,  /* (64) cmd ::= CREATE USER ids PASS ids */
   -7,  /* (65) cmd ::= CREATE USER ids PASS ids TAGS ids */
    0,  /* (66) bufsize ::= */
   -2,  /* (67) bufsize ::= BUFSIZE INTEGER */
    0,  /* (68) pps ::= */
   -2,  /* (69) pps ::= PPS INTEGER */
    0,  /* (70) tseries ::= */
   -2,  /* (71) tseries ::= TSERIES INTEGER */
    0,  /* (72) dbs ::= */
   -2,  /* (73) dbs ::= DBS INTEGER */
    0,  /* (74) streams ::= */
   -2,  /* (75) streams ::= STREAMS INTEGER */
    0,  /* (76) storage ::= */
   -2,  /* (77) storage ::= STORAGE INTEGER */
    0,  /* (78) qtime ::= */
   -2,  /* (79) qtime ::= QTIME INTEGER */
    0,  /* (80) users ::= */
   -2,  /* (81) users ::= USERS INTEGER */
    0,  /* (82) conns ::= */
   -2,  /* (83) conns ::= CONNS INTEGER */
    0,  /* (84) state ::= */
   -2,  /* (85) state ::= STATE ids */
   -9,  /* (86) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -3,  /* (87) intitemlist ::= intitemlist COMMA intitem */
   -1,  /* (88) intitemlist ::= intitem */
   -1,  /* (89) intitem ::= INTEGER */
   -2,  /* (90) keep ::= KEEP intitemlist */
   -2,  /* (91) cache ::= CACHE INTEGER */
   -2,  /* (92) replica ::= REPLICA INTEGER */
   -2,  /* (93) quorum ::= QUORUM INTEGER */
   -2,  /* (94) days ::= DAYS INTEGER */
   -2,  /* (95) minrows ::= MINROWS INTEGER */
   -2,  /* (96) maxrows ::= MAXROWS INTEGER */
   -2,  /* (97) blocks ::= BLOCKS INTEGER */
   -2,  /* (98) ctime ::= CTIME INTEGER */
   -2,  /* (99) wal ::= WAL INTEGER */
   -2,  /* (100) fsync ::= FSYNC INTEGER */
   -2,  /* (101) comp ::= COMP INTEGER */
   -2,  /* (102) prec ::= PRECISION STRING */
   -2,  /* (103) update ::= UPDATE INTEGER */
   -2,  /* (104) cachelast ::= CACHELAST INTEGER */
   -2,  /* (105) partitions ::= PARTITIONS INTEGER */
    0,  /* (106) db_optr ::= */
   -2,  /* (107) db_optr ::= db_optr cache */
   -2,  /* (108) db_optr ::= db_optr replica */
   -2,  /* (109) db_optr ::= db_optr quorum */
   -2,  /* (110) db_optr ::= db_optr days */
   -2,  /* (111) db_optr ::= db_optr minrows */
   -2,  /* (112) db_optr ::= db_optr maxrows */
   -2,  /* (113) db_optr ::= db_optr blocks */
   -2,  /* (114) db_optr ::= db_optr ctime */
   -2,  /* (115) db_optr ::= db_optr wal */
   -2,  /* (116) db_optr ::= db_optr fsync */
   -2,  /* (117) db_optr ::= db_optr comp */
   -2,  /* (118) db_optr ::= db_optr prec */
   -2,  /* (119) db_optr ::= db_optr keep */
   -2,  /* (120) db_optr ::= db_optr update */
   -2,  /* (121) db_optr ::= db_optr cachelast */
   -1,  /* (122) topic_optr ::= db_optr */
   -2,  /* (123) topic_optr ::= topic_optr partitions */
    0,  /* (124) alter_db_optr ::= */
   -2,  /* (125) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (126) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (127) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (128) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (129) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (130) alter_db_optr ::= alter_db_optr update */
   -2,  /* (131) alter_db_optr ::= alter_db_optr cachelast */
   -2,  /* (132) alter_db_optr ::= alter_db_optr minrows */
   -1,  /* (133) alter_topic_optr ::= alter_db_optr */
   -2,  /* (134) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (135) typename ::= ids */
   -4,  /* (136) typename ::= ids LP signed RP */
   -2,  /* (137) typename ::= ids UNSIGNED */
   -1,  /* (138) signed ::= INTEGER */
   -2,  /* (139) signed ::= PLUS INTEGER */
   -2,  /* (140) signed ::= MINUS INTEGER */
   -3,  /* (141) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (142) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (143) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (144) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (145) create_table_list ::= create_from_stable */
   -2,  /* (146) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (147) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (148) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (149) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (150) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (151) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (152) tagNamelist ::= ids */
   -7,  /* (153) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
    0,  /* (154) to_opt ::= */
   -3,  /* (155) to_opt ::= TO ids cpxName */
    0,  /* (156) split_opt ::= */
   -2,  /* (157) split_opt ::= SPLIT ids */
   -3,  /* (158) columnlist ::= columnlist COMMA column */
   -1,  /* (159) columnlist ::= column */
   -2,  /* (160) column ::= ids typename */
   -3,  /* (161) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (162) tagitemlist ::= tagitem */
   -1,  /* (163) tagitem ::= INTEGER */
   -1,  /* (164) tagitem ::= FLOAT */
   -1,  /* (165) tagitem ::= STRING */
   -1,  /* (166) tagitem ::= BOOL */
   -1,  /* (167) tagitem ::= NULL */
   -1,  /* (168) tagitem ::= NOW */
   -3,  /* (169) tagitem ::= NOW PLUS VARIABLE */
   -3,  /* (170) tagitem ::= NOW MINUS VARIABLE */
   -2,  /* (171) tagitem ::= MINUS INTEGER */
   -2,  /* (172) tagitem ::= MINUS FLOAT */
   -2,  /* (173) tagitem ::= PLUS INTEGER */
   -2,  /* (174) tagitem ::= PLUS FLOAT */
  -15,  /* (175) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (176) select ::= LP select RP */
   -1,  /* (177) union ::= select */
   -4,  /* (178) union ::= union UNION ALL select */
   -1,  /* (179) cmd ::= union */
   -2,  /* (180) select ::= SELECT selcollist */
   -2,  /* (181) sclp ::= selcollist COMMA */
    0,  /* (182) sclp ::= */
   -4,  /* (183) selcollist ::= sclp distinct expr as */
   -2,  /* (184) selcollist ::= sclp STAR */
   -2,  /* (185) as ::= AS ids */
   -1,  /* (186) as ::= ids */
    0,  /* (187) as ::= */
   -1,  /* (188) distinct ::= DISTINCT */
    0,  /* (189) distinct ::= */
   -2,  /* (190) from ::= FROM tablelist */
   -2,  /* (191) from ::= FROM sub */
   -3,  /* (192) sub ::= LP union RP */
   -4,  /* (193) sub ::= LP union RP ids */
   -6,  /* (194) sub ::= sub COMMA LP union RP ids */
   -2,  /* (195) tablelist ::= ids cpxName */
   -3,  /* (196) tablelist ::= ids cpxName ids */
   -4,  /* (197) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (198) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (199) tmvar ::= VARIABLE */
   -1,  /* (200) timestamp ::= INTEGER */
   -2,  /* (201) timestamp ::= MINUS INTEGER */
   -2,  /* (202) timestamp ::= PLUS INTEGER */
   -1,  /* (203) timestamp ::= STRING */
   -1,  /* (204) timestamp ::= NOW */
   -3,  /* (205) timestamp ::= NOW PLUS VARIABLE */
   -3,  /* (206) timestamp ::= NOW MINUS VARIABLE */
    0,  /* (207) range_option ::= */
   -6,  /* (208) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   -4,  /* (209) interval_option ::= intervalKey LP tmvar RP */
   -6,  /* (210) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
    0,  /* (211) interval_option ::= */
   -1,  /* (212) intervalKey ::= INTERVAL */
   -1,  /* (213) intervalKey ::= EVERY */
    0,  /* (214) session_option ::= */
   -7,  /* (215) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (216) windowstate_option ::= */
   -4,  /* (217) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (218) fill_opt ::= */
   -6,  /* (219) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (220) fill_opt ::= FILL LP ID RP */
   -4,  /* (221) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (222) sliding_opt ::= */
    0,  /* (223) orderby_opt ::= */
   -3,  /* (224) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (225) sortlist ::= sortlist COMMA item sortorder */
   -4,  /* (226) sortlist ::= sortlist COMMA arrow sortorder */
   -2,  /* (227) sortlist ::= item sortorder */
   -2,  /* (228) sortlist ::= arrow sortorder */
   -1,  /* (229) item ::= ID */
   -3,  /* (230) item ::= ID DOT ID */
   -1,  /* (231) sortorder ::= ASC */
   -1,  /* (232) sortorder ::= DESC */
    0,  /* (233) sortorder ::= */
    0,  /* (234) groupby_opt ::= */
   -3,  /* (235) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (236) grouplist ::= grouplist COMMA item */
   -3,  /* (237) grouplist ::= grouplist COMMA arrow */
   -1,  /* (238) grouplist ::= item */
   -1,  /* (239) grouplist ::= arrow */
    0,  /* (240) having_opt ::= */
   -2,  /* (241) having_opt ::= HAVING expr */
    0,  /* (242) limit_opt ::= */
   -2,  /* (243) limit_opt ::= LIMIT signed */
   -4,  /* (244) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (245) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (246) slimit_opt ::= */
   -2,  /* (247) slimit_opt ::= SLIMIT signed */
   -4,  /* (248) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (249) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (250) where_opt ::= */
   -2,  /* (251) where_opt ::= WHERE expr */
   -3,  /* (252) expr ::= LP expr RP */
   -1,  /* (253) expr ::= ID */
   -3,  /* (254) expr ::= ID DOT ID */
   -3,  /* (255) expr ::= ID DOT STAR */
   -1,  /* (256) expr ::= INTEGER */
   -2,  /* (257) expr ::= MINUS INTEGER */
   -2,  /* (258) expr ::= PLUS INTEGER */
   -1,  /* (259) expr ::= FLOAT */
   -2,  /* (260) expr ::= MINUS FLOAT */
   -2,  /* (261) expr ::= PLUS FLOAT */
   -1,  /* (262) expr ::= STRING */
   -1,  /* (263) expr ::= NOW */
   -1,  /* (264) expr ::= TODAY */
   -1,  /* (265) expr ::= VARIABLE */
   -2,  /* (266) expr ::= PLUS VARIABLE */
   -2,  /* (267) expr ::= MINUS VARIABLE */
   -1,  /* (268) expr ::= BOOL */
   -1,  /* (269) expr ::= NULL */
   -4,  /* (270) expr ::= ID LP exprlist RP */
   -4,  /* (271) expr ::= ID LP STAR RP */
   -6,  /* (272) expr ::= ID LP expr AS typename RP */
   -3,  /* (273) expr ::= expr IS NULL */
   -4,  /* (274) expr ::= expr IS NOT NULL */
   -3,  /* (275) expr ::= expr LT expr */
   -3,  /* (276) expr ::= expr GT expr */
   -3,  /* (277) expr ::= expr LE expr */
   -3,  /* (278) expr ::= expr GE expr */
   -3,  /* (279) expr ::= expr NE expr */
   -3,  /* (280) expr ::= expr EQ expr */
   -5,  /* (281) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (282) expr ::= expr AND expr */
   -3,  /* (283) expr ::= expr OR expr */
   -3,  /* (284) expr ::= expr PLUS expr */
   -3,  /* (285) expr ::= expr MINUS expr */
   -3,  /* (286) expr ::= expr STAR expr */
   -3,  /* (287) expr ::= expr SLASH expr */
   -3,  /* (288) expr ::= expr REM expr */
   -3,  /* (289) expr ::= expr BITAND expr */
   -3,  /* (290) expr ::= expr BITOR expr */
   -3,  /* (291) expr ::= expr BITXOR expr */
   -2,  /* (292) expr ::= BITNOT expr */
   -3,  /* (293) expr ::= expr LSHIFT expr */
   -3,  /* (294) expr ::= expr RSHIFT expr */
   -3,  /* (295) expr ::= expr LIKE expr */
   -3,  /* (296) expr ::= expr MATCH expr */
   -3,  /* (297) expr ::= expr NMATCH expr */
   -3,  /* (298) expr ::= ID CONTAINS STRING */
   -5,  /* (299) expr ::= ID DOT ID CONTAINS STRING */
   -3,  /* (300) arrow ::= ID ARROW STRING */
   -5,  /* (301) arrow ::= ID DOT ID ARROW STRING */
   -1,  /* (302) expr ::= arrow */
   -5,  /* (303) expr ::= expr IN LP exprlist RP */
   -3,  /* (304) exprlist ::= exprlist COMMA expritem */
   -1,  /* (305) exprlist ::= expritem */
   -1,  /* (306) expritem ::= expr */
    0,  /* (307) expritem ::= */
   -3,  /* (308) cmd ::= RESET QUERY CACHE */
   -3,  /* (309) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (310) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (311) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (312) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (313) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (314) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (315) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (316) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (317) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (318) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (319) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (320) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (321) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (322) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (323) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (324) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (325) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (326) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (327) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (328) cmd ::= KILL QUERY INTEGER COLON INTEGER */
   -6,  /* (329) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
      case 141: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==141);
      case 142: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==142);
      case 143: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==143);
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
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD,     &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0,   NULL, NULL);}
        break;
      case 41: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0,   NULL);}
        break;
      case 42: /* cmd ::= ALTER USER ids TAGS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_TAGS,       &yymsp[-2].minor.yy0, NULL, NULL, &yymsp[0].minor.yy0);}
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 46: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 47: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 48: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==48);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy342, &t);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy299);}
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy299);}
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy333);}
        break;
      case 52: /* ids ::= ID */
      case 53: /* ids ::= STRING */ yytestcase(yyruleno==53);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 54: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 55: /* ifexists ::= */
      case 57: /* ifnotexists ::= */ yytestcase(yyruleno==57);
      case 189: /* distinct ::= */ yytestcase(yyruleno==189);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 58: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy299);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy342, &yymsp[-2].minor.yy0);}
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy263, &yymsp[0].minor.yy0, 1);}
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy263, &yymsp[0].minor.yy0, 2);}
        break;
      case 64: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);}
        break;
      case 65: /* cmd ::= CREATE USER ids PASS ids TAGS ids */
{ setCreateUserSql(pInfo, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 66: /* bufsize ::= */
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
      case 67: /* bufsize ::= BUFSIZE INTEGER */
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
    yylhsminor.yy299.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy299.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy299.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy299.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy299.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy299.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy299.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy299.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy299.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy299 = yylhsminor.yy299;
        break;
      case 87: /* intitemlist ::= intitemlist COMMA intitem */
      case 161: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==161);
{ yylhsminor.yy333 = tVariantListAppend(yymsp[-2].minor.yy333, &yymsp[0].minor.yy42, -1);    }
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 88: /* intitemlist ::= intitem */
      case 162: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==162);
{ yylhsminor.yy333 = tVariantListAppend(NULL, &yymsp[0].minor.yy42, -1); }
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 89: /* intitem ::= INTEGER */
      case 163: /* tagitem ::= INTEGER */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= STRING */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= BOOL */ yytestcase(yyruleno==166);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy42, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 90: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy333 = yymsp[0].minor.yy333; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy342); yymsp[1].minor.yy342.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 107: /* db_optr ::= db_optr cache */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 108: /* db_optr ::= db_optr replica */
      case 125: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==125);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 109: /* db_optr ::= db_optr quorum */
      case 126: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==126);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 110: /* db_optr ::= db_optr days */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 111: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 112: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 113: /* db_optr ::= db_optr blocks */
      case 128: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==128);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 114: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 115: /* db_optr ::= db_optr wal */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 116: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 117: /* db_optr ::= db_optr comp */
      case 129: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==129);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 118: /* db_optr ::= db_optr prec */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 119: /* db_optr ::= db_optr keep */
      case 127: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==127);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.keep = yymsp[0].minor.yy333; }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 120: /* db_optr ::= db_optr update */
      case 130: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==130);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 121: /* db_optr ::= db_optr cachelast */
      case 131: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==131);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 122: /* topic_optr ::= db_optr */
      case 133: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==133);
{ yylhsminor.yy342 = yymsp[0].minor.yy342; yylhsminor.yy342.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 123: /* topic_optr ::= topic_optr partitions */
      case 134: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==134);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 124: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy342); yymsp[1].minor.yy342.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* alter_db_optr ::= alter_db_optr minrows */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.minRowsPerBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 135: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy263, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy263 = yylhsminor.yy263;
        break;
      case 136: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy277 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy263, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy277;  // negative value of name length
    tSetColumnType(&yylhsminor.yy263, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy263 = yylhsminor.yy263;
        break;
      case 137: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy263, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy263 = yylhsminor.yy263;
        break;
      case 138: /* signed ::= INTEGER */
{ yylhsminor.yy277 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy277 = yylhsminor.yy277;
        break;
      case 139: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy277 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 140: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy277 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 144: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy78;}
        break;
      case 145: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy400);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy78 = pCreateTable;
}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 146: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy78->childTableInfo, &yymsp[0].minor.yy400);
  yylhsminor.yy78 = yymsp[-1].minor.yy78;
}
  yymsp[-1].minor.yy78 = yylhsminor.yy78;
        break;
      case 147: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy78 = tSetCreateTableInfo(yymsp[-1].minor.yy333, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy78, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy78 = yylhsminor.yy78;
        break;
      case 148: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy78 = tSetCreateTableInfo(yymsp[-5].minor.yy333, yymsp[-1].minor.yy333, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy78, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy78 = yylhsminor.yy78;
        break;
      case 149: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy400 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy333, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy400 = yylhsminor.yy400;
        break;
      case 150: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy400 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy333, yymsp[-1].minor.yy333, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy400 = yylhsminor.yy400;
        break;
      case 151: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy333, &yymsp[0].minor.yy0); yylhsminor.yy333 = yymsp[-2].minor.yy333;  }
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 152: /* tagNamelist ::= ids */
{yylhsminor.yy333 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy333, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 153: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy78 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy144, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy78, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy78 = yylhsminor.yy78;
        break;
      case 154: /* to_opt ::= */
      case 156: /* split_opt ::= */ yytestcase(yyruleno==156);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 155: /* to_opt ::= TO ids cpxName */
{
   yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
   yymsp[-2].minor.yy0.n += yymsp[0].minor.yy0.n;
}
        break;
      case 157: /* split_opt ::= SPLIT ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 158: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy333, &yymsp[0].minor.yy263); yylhsminor.yy333 = yymsp[-2].minor.yy333;  }
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 159: /* columnlist ::= column */
{yylhsminor.yy333 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy333, &yymsp[0].minor.yy263);}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 160: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy263, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy263);
}
  yymsp[-1].minor.yy263 = yylhsminor.yy263;
        break;
      case 167: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy42, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 168: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy42, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 169: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy42, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 170: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy42, &yymsp[0].minor.yy0, TK_MINUS, true);
}
        break;
      case 171: /* tagitem ::= MINUS INTEGER */
      case 172: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==172);
      case 173: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==173);
      case 174: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==174);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy42, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 175: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy144 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy333, yymsp[-12].minor.yy516, yymsp[-11].minor.yy194, yymsp[-4].minor.yy333, yymsp[-2].minor.yy333, &yymsp[-9].minor.yy200, &yymsp[-7].minor.yy235, &yymsp[-6].minor.yy248, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy333, &yymsp[0].minor.yy190, &yymsp[-1].minor.yy190, yymsp[-3].minor.yy194, &yymsp[-10].minor.yy132);
}
  yymsp[-14].minor.yy144 = yylhsminor.yy144;
        break;
      case 176: /* select ::= LP select RP */
{yymsp[-2].minor.yy144 = yymsp[-1].minor.yy144;}
        break;
      case 177: /* union ::= select */
{ yylhsminor.yy333 = setSubclause(NULL, yymsp[0].minor.yy144); }
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 178: /* union ::= union UNION ALL select */
{ yylhsminor.yy333 = appendSelectClause(yymsp[-3].minor.yy333, yymsp[0].minor.yy144); }
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 179: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy333, NULL, TSDB_SQL_SELECT); }
        break;
      case 180: /* select ::= SELECT selcollist */
{
  yylhsminor.yy144 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy333, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy144 = yylhsminor.yy144;
        break;
      case 181: /* sclp ::= selcollist COMMA */
{yylhsminor.yy333 = yymsp[-1].minor.yy333;}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 182: /* sclp ::= */
      case 223: /* orderby_opt ::= */ yytestcase(yyruleno==223);
{yymsp[1].minor.yy333 = 0;}
        break;
      case 183: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy333 = tSqlExprListAppend(yymsp[-3].minor.yy333, yymsp[-1].minor.yy194,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 184: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy333 = tSqlExprListAppend(yymsp[-1].minor.yy333, pNode, 0, 0);
}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 185: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 186: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 187: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 188: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 190: /* from ::= FROM tablelist */
      case 191: /* from ::= FROM sub */ yytestcase(yyruleno==191);
{yymsp[-1].minor.yy516 = yymsp[0].minor.yy516;}
        break;
      case 192: /* sub ::= LP union RP */
{yymsp[-2].minor.yy516 = addSubqueryElem(NULL, yymsp[-1].minor.yy333, NULL);}
        break;
      case 193: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy516 = addSubqueryElem(NULL, yymsp[-2].minor.yy333, &yymsp[0].minor.yy0);}
        break;
      case 194: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy516 = addSubqueryElem(yymsp[-5].minor.yy516, yymsp[-2].minor.yy333, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy516 = yylhsminor.yy516;
        break;
      case 195: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 196: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy516 = yylhsminor.yy516;
        break;
      case 197: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(yymsp[-3].minor.yy516, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy516 = yylhsminor.yy516;
        break;
      case 198: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(yymsp[-4].minor.yy516, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy516 = yylhsminor.yy516;
        break;
      case 199: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 200: /* timestamp ::= INTEGER */
{ yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 201: /* timestamp ::= MINUS INTEGER */
      case 202: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==202);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 203: /* timestamp ::= STRING */
{ yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 204: /* timestamp ::= NOW */
{ yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 205: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 206: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 207: /* range_option ::= */
{yymsp[1].minor.yy132.start = 0; yymsp[1].minor.yy132.end = 0;}
        break;
      case 208: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy132.start = yymsp[-3].minor.yy194; yymsp[-5].minor.yy132.end = yymsp[-1].minor.yy194;}
        break;
      case 209: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy200.interval = yymsp[-1].minor.yy0; yylhsminor.yy200.offset.n = 0; yylhsminor.yy200.token = yymsp[-3].minor.yy44;}
  yymsp[-3].minor.yy200 = yylhsminor.yy200;
        break;
      case 210: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy200.interval = yymsp[-3].minor.yy0; yylhsminor.yy200.offset = yymsp[-1].minor.yy0;   yylhsminor.yy200.token = yymsp[-5].minor.yy44;}
  yymsp[-5].minor.yy200 = yylhsminor.yy200;
        break;
      case 211: /* interval_option ::= */
{memset(&yymsp[1].minor.yy200, 0, sizeof(yymsp[1].minor.yy200));}
        break;
      case 212: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy44 = TK_INTERVAL;}
        break;
      case 213: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy44 = TK_EVERY;   }
        break;
      case 214: /* session_option ::= */
{yymsp[1].minor.yy235.col.n = 0; yymsp[1].minor.yy235.gap.n = 0;}
        break;
      case 215: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy235.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy235.gap = yymsp[-1].minor.yy0;
}
        break;
      case 216: /* windowstate_option ::= */
{ yymsp[1].minor.yy248.col.n = 0; yymsp[1].minor.yy248.col.z = NULL;}
        break;
      case 217: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy248.col = yymsp[-1].minor.yy0; }
        break;
      case 218: /* fill_opt ::= */
{ yymsp[1].minor.yy333 = 0;     }
        break;
      case 219: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy333, &A, -1, 0);
    yymsp[-5].minor.yy333 = yymsp[-1].minor.yy333;
}
        break;
      case 220: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy333 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 221: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 222: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 224: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy333 = yymsp[0].minor.yy333;}
        break;
      case 225: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-3].minor.yy333, &yymsp[-1].minor.yy42, NULL, false, yymsp[0].minor.yy133);
}
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 226: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-3].minor.yy333, NULL, yymsp[-1].minor.yy194, true, yymsp[0].minor.yy133);
}
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 227: /* sortlist ::= item sortorder */
{
  yylhsminor.yy333 = commonItemAppend(NULL, &yymsp[-1].minor.yy42, NULL, false, yymsp[0].minor.yy133);
}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 228: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy333 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy194, true, yymsp[0].minor.yy133);
}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 229: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy42, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 230: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy42, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 231: /* sortorder ::= ASC */
{ yymsp[0].minor.yy133 = TSDB_ORDER_ASC; }
        break;
      case 232: /* sortorder ::= DESC */
{ yymsp[0].minor.yy133 = TSDB_ORDER_DESC;}
        break;
      case 233: /* sortorder ::= */
{ yymsp[1].minor.yy133 = TSDB_ORDER_ASC; }
        break;
      case 234: /* groupby_opt ::= */
{ yymsp[1].minor.yy333 = 0;}
        break;
      case 235: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy333 = yymsp[0].minor.yy333;}
        break;
      case 236: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-2].minor.yy333, &yymsp[0].minor.yy42, NULL, false, -1);
}
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 237: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-2].minor.yy333, NULL, yymsp[0].minor.yy194, true, -1);
}
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 238: /* grouplist ::= item */
{
  yylhsminor.yy333 = commonItemAppend(NULL, &yymsp[0].minor.yy42, NULL, false, -1);
}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 239: /* grouplist ::= arrow */
{
  yylhsminor.yy333 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy194, true, -1);
}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 240: /* having_opt ::= */
      case 250: /* where_opt ::= */ yytestcase(yyruleno==250);
      case 307: /* expritem ::= */ yytestcase(yyruleno==307);
{yymsp[1].minor.yy194 = 0;}
        break;
      case 241: /* having_opt ::= HAVING expr */
      case 251: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==251);
{yymsp[-1].minor.yy194 = yymsp[0].minor.yy194;}
        break;
      case 242: /* limit_opt ::= */
      case 246: /* slimit_opt ::= */ yytestcase(yyruleno==246);
{yymsp[1].minor.yy190.limit = -1; yymsp[1].minor.yy190.offset = 0;}
        break;
      case 243: /* limit_opt ::= LIMIT signed */
      case 247: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==247);
{yymsp[-1].minor.yy190.limit = yymsp[0].minor.yy277;  yymsp[-1].minor.yy190.offset = 0;}
        break;
      case 244: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy190.limit = yymsp[-2].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[0].minor.yy277;}
        break;
      case 245: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy190.limit = yymsp[0].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[-2].minor.yy277;}
        break;
      case 248: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy190.limit = yymsp[-2].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[0].minor.yy277;}
        break;
      case 249: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy190.limit = yymsp[0].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[-2].minor.yy277;}
        break;
      case 252: /* expr ::= LP expr RP */
{yylhsminor.yy194 = yymsp[-1].minor.yy194; yylhsminor.yy194->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy194->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 253: /* expr ::= ID */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 254: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 255: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 256: /* expr ::= INTEGER */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 257: /* expr ::= MINUS INTEGER */
      case 258: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==258);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 259: /* expr ::= FLOAT */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 260: /* expr ::= MINUS FLOAT */
      case 261: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==261);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 262: /* expr ::= STRING */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 263: /* expr ::= NOW */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 264: /* expr ::= TODAY */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 265: /* expr ::= VARIABLE */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 266: /* expr ::= PLUS VARIABLE */
      case 267: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==267);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 268: /* expr ::= BOOL */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 269: /* expr ::= NULL */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 270: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy194 = tSqlExprCreateFunction(yymsp[-1].minor.yy333, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy194 = yylhsminor.yy194;
        break;
      case 271: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy194 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy194 = yylhsminor.yy194;
        break;
      case 272: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy194 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy194, &yymsp[-1].minor.yy263, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy194 = yylhsminor.yy194;
        break;
      case 273: /* expr ::= expr IS NULL */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 274: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-3].minor.yy194, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy194 = yylhsminor.yy194;
        break;
      case 275: /* expr ::= expr LT expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LT);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 276: /* expr ::= expr GT expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_GT);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 277: /* expr ::= expr LE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 278: /* expr ::= expr GE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_GE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 279: /* expr ::= expr NE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_NE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 280: /* expr ::= expr EQ expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_EQ);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 281: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy194); yylhsminor.yy194 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy194, yymsp[-2].minor.yy194, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy194, TK_LE), TK_AND);}
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 282: /* expr ::= expr AND expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_AND);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 283: /* expr ::= expr OR expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_OR); }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 284: /* expr ::= expr PLUS expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_PLUS);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 285: /* expr ::= expr MINUS expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_MINUS); }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 286: /* expr ::= expr STAR expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_STAR);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 287: /* expr ::= expr SLASH expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_DIVIDE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 288: /* expr ::= expr REM expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_REM);   }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 289: /* expr ::= expr BITAND expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_BITAND);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 290: /* expr ::= expr BITOR expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_BITOR); }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 291: /* expr ::= expr BITXOR expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_BITXOR);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 292: /* expr ::= BITNOT expr */
{yymsp[-1].minor.yy194 = tSqlExprCreate(yymsp[0].minor.yy194, NULL, TK_BITNOT);}
        break;
      case 293: /* expr ::= expr LSHIFT expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LSHIFT);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 294: /* expr ::= expr RSHIFT expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_RSHIFT);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 295: /* expr ::= expr LIKE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LIKE);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 296: /* expr ::= expr MATCH expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_MATCH);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 297: /* expr ::= expr NMATCH expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_NMATCH);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 298: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 299: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 300: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 301: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 302: /* expr ::= arrow */
      case 306: /* expritem ::= expr */ yytestcase(yyruleno==306);
{yylhsminor.yy194 = yymsp[0].minor.yy194;}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 303: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-4].minor.yy194, (tSqlExpr*)yymsp[-1].minor.yy333, TK_IN); }
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 304: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy333 = tSqlExprListAppend(yymsp[-2].minor.yy333,yymsp[0].minor.yy194,0, 0);}
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 305: /* exprlist ::= expritem */
{yylhsminor.yy333 = tSqlExprListAppend(0,yymsp[0].minor.yy194,0, 0);}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 308: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 309: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 310: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 316: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy42, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 318: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 321: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 322: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 323: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 324: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy42, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 325: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 326: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 327: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 328: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      case 329: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n; 
  SDelData * pDelData = tGetDelData(&yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0, yymsp[0].minor.yy194);
  setSqlInfo(pInfo, pDelData, NULL, TSDB_SQL_DELETE_DATA);
}
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
