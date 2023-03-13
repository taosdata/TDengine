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
#define YYNOCODE 293
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
#define ParseARG_PARAM ,pInfo
#define ParseARG_FETCH SSqlInfo* pInfo=yypParser->pInfo;
#define ParseARG_STORE yypParser->pInfo=pInfo;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYFALLBACK 1
#define YYNSTATE             418
#define YYNRULE              332
#define YYNTOKEN             207
#define YY_MAX_SHIFT         417
#define YY_MIN_SHIFTREDUCE   652
#define YY_MAX_SHIFTREDUCE   983
#define YY_ERROR_ACTION      984
#define YY_ACCEPT_ACTION     985
#define YY_NO_ACTION         986
#define YY_MIN_REDUCE        987
#define YY_MAX_REDUCE        1318
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
#define YY_ACTTAB_COUNT (946)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   230,  706,  293,  175, 1227,   65, 1228,  332,  706,  707,
 /*    10 */  1292,  270, 1294,  197,   43,   44,  707,   47,   48,  416,
 /*    20 */   255,  283,   32,   31,   30, 1141,   65,   46,  365,   51,
 /*    30 */    49,   52,   50,   37,   36,   35,   34,   33,   42,   41,
 /*    40 */   743,   24,   40,   39,   38,   43,   44, 1175,   47,   48,
 /*    50 */   263, 1292,  283,   32,   31,   30,  314, 1148,   46,  365,
 /*    60 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*    70 */    41,  273,  276,   40,   39,   38,  313,  312, 1148,   40,
 /*    80 */    39,   38,   43,   44, 1150,   47,   48,  409, 1077,  283,
 /*    90 */    32,   31,   30,  361,   95,   46,  365,   51,   49,   52,
 /*   100 */    50,   37,   36,   35,   34,   33,   42,   41,  228,  400,
 /*   110 */    40,   39,   38,   43,   44, 1313,   47,   48, 1292, 1172,
 /*   120 */   283,   32,   31,   30, 1133,   64,   46,  365,   51,   49,
 /*   130 */    52,   50,   37,   36,   35,   34,   33,   42,   41,  190,
 /*   140 */   792,   40,   39,   38,  912,  268,  915,   43,   45,  854,
 /*   150 */    47,   48, 1151,  857,  283,   32,   31,   30,  322,  906,
 /*   160 */    46,  365,   51,   49,   52,   50,   37,   36,   35,   34,
 /*   170 */    33,   42,   41,  390,  389,   40,   39,   38,   44,  229,
 /*   180 */    47,   48,  318,  319,  283,   32,   31,   30, 1305, 1292,
 /*   190 */    46,  365,   51,   49,   52,   50,   37,   36,   35,   34,
 /*   200 */    33,   42,   41,  706,  315,   40,   39,   38,   47,   48,
 /*   210 */  1238,  707,  283,   32,   31,   30,  361,  286,   46,  365,
 /*   220 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*   230 */    41,  866,  867,   40,   39,   38,   73,  359,  408,  407,
 /*   240 */   358,  406,  357,  405,  356,  355,  354,  404,  353,  403,
 /*   250 */   402, 1166,  653,  654,  655,  656,  657,  658,  659,  660,
 /*   260 */   661,  662,  663,  664,  665,  666,  169,  920,  262,  264,
 /*   270 */   112,  266,    1,  199,  985,  417,  411, 1108, 1096, 1097,
 /*   280 */  1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105, 1106, 1107,
 /*   290 */  1109, 1110,  254,  922,  141, 1237,  910,  287,  913,  285,
 /*   300 */   916,  376,  375, 1130, 1131,   61, 1134,  400,   25,   51,
 /*   310 */    49,   52,   50,   37,   36,   35,   34,   33,   42,   41,
 /*   320 */   706,  271,   40,   39,   38,  244, 1166, 1234,  707,  259,
 /*   330 */   260,  280,  820,  367,  246,  817,  298,  818,   29,  819,
 /*   340 */   156,  155,  154,  245,  306,  302,  301,   65,  373,  101,
 /*   350 */    37,   36,   35,   34,   33,   42,   41, 1282, 1233,   40,
 /*   360 */    39,   38,  272,   92,    5,   68,  201, 1292,  288,  289,
 /*   370 */   152,  151,  150,  200,  122,  127,  118,  126,  193,   42,
 /*   380 */    41,   53,  926,   40,   39,   38,   65,  292,   74,  115,
 /*   390 */   254,  922,  274,  349,  910,  226,  913, 1132,  916, 1148,
 /*   400 */   234, 1116,   65, 1114, 1115, 1292, 1121, 1295, 1117,   57,
 /*   410 */  1292,  305, 1118,   91, 1119, 1120,  279,  923,  917,  919,
 /*   420 */   256,  911,  284,  914,  295, 1135,  171,  259,  260,  293,
 /*   430 */    73,  377,  408,  407,  886,  406,   29,  405, 1148,   65,
 /*   440 */   198,  404,  918,  403,  402,  364,  391,  378,  293,  139,
 /*   450 */   133,  144,   65,  235, 1148,   13,  821,  290,  143,  366,
 /*   460 */   149,  153,  142, 1292,  221,  219,  217,  294,  363,  291,
 /*   470 */   146,  385,  384,  216,  160,  159,  158,  157,   65,   53,
 /*   480 */   230,   65,  303,  101,  379,  282,  230,   65,  114,   93,
 /*   490 */  1292, 1148, 1295,   65,  885,  173, 1292,  380, 1295,  415,
 /*   500 */   413,  680,  681,  363, 1148,  336,  107,  838,  106,  236,
 /*   510 */     6,  168,  166,  165,  247,  923,  917,  919,  248, 1292,
 /*   520 */   335,  249,   74,  386, 1292,  250,  387,  251, 1292,  370,
 /*   530 */  1148, 1292,  388, 1148,   65, 1292, 1278, 1292,  392, 1148,
 /*   540 */   918, 1277, 1276,  257,  921, 1148, 1292,  258,  232,  293,
 /*   550 */    26, 1292, 1292, 1292,  174,  233,  237, 1292, 1292,  231,
 /*   560 */  1149,  238,  239,  241,  110, 1292, 1292,  242,  243, 1292,
 /*   570 */   240, 1292, 1292, 1292,  227,  275,  277, 1292, 1292,   96,
 /*   580 */  1292, 1038, 1151, 1151, 1292, 1166, 1147, 1225,  211, 1226,
 /*   590 */   109, 1048,  108, 1039,    3,  212,  835,  842,  211,  307,
 /*   600 */   211,   98,   99,  265,  317,  316,  863,  873,  874,   82,
 /*   610 */    85,  802,  340,   66,  804,  342,  803,  281,  958,  924,
 /*   620 */   177,   77,   54,  369,   10,   66,   66, 1146,   77,  113,
 /*   630 */    77,  343,    9,    9,   60,   15,  705,   14,   89,  928,
 /*   640 */   309,  309,  132,   17,  131,   16,  368,  827, 1174,  828,
 /*   650 */     9, 1185,   86,   83,   19,  825,   18,  826,  909,  382,
 /*   660 */   381, 1182,  138,   21,  137,   20, 1183, 1167,  310, 1187,
 /*   670 */   791,  176,  181,  328, 1217, 1216, 1215, 1214,  192,  194,
 /*   680 */  1142, 1140,  195,  196, 1054,  345,  346,  347, 1318,  348,
 /*   690 */   351,  352,   75,  224,   71,  167,  410,  362, 1047,  374,
 /*   700 */   853, 1312,  129, 1311, 1308,  202,  383,   27, 1304,  135,
 /*   710 */  1303, 1300,  203,   87,  321,  267,  323,  325, 1074, 1164,
 /*   720 */   182,   72,   84,   67,  337,  183,   28,   76,  225,  184,
 /*   730 */   333, 1035,  331,  185,  329,  145,  327,  186, 1033,  147,
 /*   740 */   148, 1031, 1030, 1029,  261,  214,  215, 1026, 1025, 1024,
 /*   750 */  1023, 1022, 1021,  324,  320, 1020,  218,   94,  220, 1010,
 /*   760 */   222, 1007,  223, 1003,  350,  401,  170,  140,   90,  308,
 /*   770 */  1144,   97,  102,  326,  393,  394,  395,  396,  397,  398,
 /*   780 */   399,   88,  172,  982,  278,  297,  344,  981,  296,  300,
 /*   790 */   980,  252,  299,  963,  962,  123,  253, 1052, 1051,  124,
 /*   800 */   304,  339,  309,   11,  100,   58, 1028,  830,  311,  103,
 /*   810 */    80,  862,  206, 1027, 1075,  208,  204,  205,  207,  161,
 /*   820 */   209,  162, 1019,  210,  163, 1018,  338, 1112, 1009, 1076,
 /*   830 */   164,  856,  189,  187,  188,   59,  191, 1008,    4,  860,
 /*   840 */  1123,    2,  855,   81,  178,  859,  864,  875,  179,  180,
 /*   850 */   869,  104,  269,  871,  105,  330,   69,  368,  334,   12,
 /*   860 */   111,   55,   70,   22,   23,   56,  341,   62,  114,  120,
 /*   870 */   116,  117,  721,  756,  754,  119,  753,  752,  750,  748,
 /*   880 */   745,  710,   63,  121,  360,  125,  955,  953,    7,  956,
 /*   890 */   927,  954,  925,  929,    8,  372,   78,  128,  371,  130,
 /*   900 */    66,  824,  823,   79,  134,  136,  794,  793,  790,  737,
 /*   910 */   735,  727,  733,  729,  731,  725,  723,  759,  758,  757,
 /*   920 */   755,  751,  749,  747,  746,  213,  708,  682,  670,  679,
 /*   930 */   987,  677,  986,  986,  986,  986,  986,  986,  986,  986,
 /*   940 */   986,  986,  986,  412,  986,  414,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   280,    1,  209,  209,  288,  209,  290,  291,    1,    9,
 /*    10 */   290,    1,  292,  220,   14,   15,    9,   17,   18,  209,
 /*    20 */   210,   21,   22,   23,   24,  209,  209,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */     5,  280,   42,   43,   44,   14,   15,  209,   17,   18,
 /*    50 */   254,  290,   21,   22,   23,   24,  285,  261,   27,   28,
 /*    60 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*    70 */    39,  254,  256,   42,   43,   44,  282,  283,  261,   42,
 /*    80 */    43,   44,   14,   15,  262,   17,   18,  231,  232,   21,
 /*    90 */    22,   23,   24,   92,   94,   27,   28,   29,   30,   31,
 /*   100 */    32,   33,   34,   35,   36,   37,   38,   39,  280,   98,
 /*   110 */    42,   43,   44,   14,   15,  262,   17,   18,  290,  281,
 /*   120 */    21,   22,   23,   24,    0,   94,   27,   28,   29,   30,
 /*   130 */    31,   32,   33,   34,   35,   36,   37,   38,   39,  267,
 /*   140 */     5,   42,   43,   44,    5,  255,    7,   14,   15,    5,
 /*   150 */    17,   18,  262,    9,   21,   22,   23,   24,  286,   91,
 /*   160 */    27,   28,   29,   30,   31,   32,   33,   34,   35,   36,
 /*   170 */    37,   38,   39,   38,   39,   42,   43,   44,   15,  280,
 /*   180 */    17,   18,   38,   39,   21,   22,   23,   24,  262,  290,
 /*   190 */    27,   28,   29,   30,   31,   32,   33,   34,   35,   36,
 /*   200 */    37,   38,   39,    1,  285,   42,   43,   44,   17,   18,
 /*   210 */   249,    9,   21,   22,   23,   24,   92,   75,   27,   28,
 /*   220 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*   230 */    39,  135,  136,   42,   43,   44,  106,  107,  108,  109,
 /*   240 */   110,  111,  112,  113,  114,  115,  116,  117,  118,  119,
 /*   250 */   120,  259,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   260 */    58,   59,   60,   61,   62,   63,   64,  128,   66,  277,
 /*   270 */   217,  127,  218,  219,  207,  208,   74,  233,  234,  235,
 /*   280 */   236,  237,  238,  239,  240,  241,  242,  243,  244,  245,
 /*   290 */   246,  247,    1,    2,   85,  249,    5,  155,    7,  157,
 /*   300 */     9,  159,  160,  250,  251,  252,  253,   98,   49,   29,
 /*   310 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*   320 */     1,  249,   42,   43,   44,   66,  259,  249,    9,   38,
 /*   330 */    39,  216,    2,   42,   75,    5,  153,    7,   47,    9,
 /*   340 */    81,   82,   83,   84,  277,  162,  163,  209,   89,   90,
 /*   350 */    33,   34,   35,   36,   37,   38,   39,  280,  249,   42,
 /*   360 */    43,   44,  249,  217,   67,   68,   69,  290,   38,   39,
 /*   370 */    85,   86,   87,   76,   77,   78,   79,   80,  257,   38,
 /*   380 */    39,   90,   91,   42,   43,   44,  209,   75,  129,  217,
 /*   390 */     1,    2,  254,   96,    5,  280,    7,  251,    9,  261,
 /*   400 */   280,  233,  209,  235,  236,  290,  238,  292,  240,   90,
 /*   410 */   290,  152,  244,  154,  246,  247,  216,  126,  127,  128,
 /*   420 */   161,    5,  216,    7,  165,  253,  209,   38,   39,  209,
 /*   430 */   106,  254,  108,  109,   83,  111,   47,  113,  261,  209,
 /*   440 */   220,  117,  151,  119,  120,   25,  249,  254,  209,   67,
 /*   450 */    68,   69,  209,  280,  261,   90,  126,  127,   76,  220,
 /*   460 */    78,   79,   80,  290,   67,   68,   69,  155,   48,  157,
 /*   470 */    88,  159,  160,   76,   77,   78,   79,   80,  209,   90,
 /*   480 */   280,  209,  209,   90,  254,   65,  280,  209,  123,  124,
 /*   490 */   290,  261,  292,  209,  143,  209,  290,  254,  292,   70,
 /*   500 */    71,   72,   73,   48,  261,  287,  288,   42,  290,  280,
 /*   510 */    90,   67,   68,   69,  280,  126,  127,  128,  280,  290,
 /*   520 */    65,  280,  129,  254,  290,  280,  254,  280,  290,   16,
 /*   530 */   261,  290,  254,  261,  209,  290,  280,  290,  254,  261,
 /*   540 */   151,  280,  280,  280,  128,  261,  290,  280,  280,  209,
 /*   550 */   279,  290,  290,  290,  209,  280,  280,  290,  290,  280,
 /*   560 */   220,  280,  280,  280,  263,  290,  290,  280,  280,  290,
 /*   570 */   280,  290,  290,  290,  280,  255,  255,  290,  290,  278,
 /*   580 */   290,  215,  262,  262,  290,  259,  261,  288,  222,  290,
 /*   590 */   288,  215,  290,  215,  213,  214,  105,  132,  222,   91,
 /*   600 */   222,   91,   91,  277,   38,   39,   91,   91,   91,  105,
 /*   610 */   105,   91,   91,  105,   91,   91,   91,    1,   91,   91,
 /*   620 */   105,  105,  105,   25,  133,  105,  105,  209,  105,  105,
 /*   630 */   105,   87,  105,  105,   90,  156,   91,  158,   90,  126,
 /*   640 */   130,  130,  156,  156,  158,  158,   48,    5,  209,    7,
 /*   650 */   105,  209,  147,  149,  156,    5,  158,    7,   42,   38,
 /*   660 */    39,  209,  156,  156,  158,  158,  209,  259,  259,  209,
 /*   670 */   122,  209,  209,  209,  289,  289,  289,  289,  264,  209,
 /*   680 */   259,  209,  209,  209,  209,  209,  209,  209,  265,  209,
 /*   690 */   209,  209,  209,  209,  209,   65,   92,  209,  209,  209,
 /*   700 */   128,  209,  209,  209,  209,  209,  209,  150,  209,  209,
 /*   710 */   209,  209,  209,  146,  284,  284,  284,  284,  209,  276,
 /*   720 */   275,  209,  148,  209,  141,  274,  145,  209,  209,  273,
 /*   730 */   144,  209,  139,  272,  138,  209,  137,  271,  209,  209,
 /*   740 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*   750 */   209,  209,  209,  140,  134,  209,  209,  125,  209,  209,
 /*   760 */   209,  209,  209,  209,   97,  121,  211,  104,  212,  211,
 /*   770 */   211,  211,  211,  211,  103,   56,  100,  102,   60,  101,
 /*   780 */    99,  211,  133,    5,  211,    5,  211,    5,  164,    5,
 /*   790 */     5,  211,  164,  108,  107,  217,  211,  221,  221,  217,
 /*   800 */   153,   87,  130,   90,  131,   90,  211,   91,  105,  105,
 /*   810 */   105,   91,  224,  211,  230,  225,  229,  228,  227,  212,
 /*   820 */   226,  212,  211,  223,  212,  211,  258,  248,  211,  232,
 /*   830 */   212,    5,  268,  270,  269,  266,  265,  211,  213,  128,
 /*   840 */   248,  218,    5,   90,   90,  128,   91,   91,   90,  105,
 /*   850 */    91,   90,    1,   91,   90,   90,  105,   48,    1,   90,
 /*   860 */    94,   90,  105,  142,  142,   90,   87,   95,  123,   77,
 /*   870 */    87,   85,    5,    9,    5,   94,    5,    5,    5,    5,
 /*   880 */     5,   93,   95,   94,   16,   85,    9,    9,   90,    9,
 /*   890 */    91,    9,   91,  126,   90,   64,   17,  158,   28,  158,
 /*   900 */   105,  128,  128,   17,  158,  158,    5,    5,   91,    5,
 /*   910 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   920 */     5,    5,    5,    5,    5,  105,   93,   73,   65,    9,
 /*   930 */     0,    9,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   940 */   293,  293,  293,   22,  293,   22,  293,  293,  293,  293,
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
 /*  1140 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*  1150 */   293,  293,  293,
};
#define YY_SHIFT_COUNT    (417)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (930)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   259,  130,  130,  324,  324,    1,  291,  389,  389,  389,
 /*    10 */   319,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*    20 */     7,    7,   10,   10,    0,  202,  389,  389,  389,  389,
 /*    30 */   389,  389,  389,  389,  389,  389,  389,  389,  389,  389,
 /*    40 */   389,  389,  389,  389,  389,  389,  389,  389,  389,  389,
 /*    50 */   389,  389,  389,  389,  330,  330,  330,  393,  393,   96,
 /*    60 */     7,  124,    7,    7,    7,    7,    7,  209,    1,   10,
 /*    70 */    10,   11,   11,   35,  946,  946,  946,  330,  330,  330,
 /*    80 */   144,  144,  135,  135,  135,  135,  135,  135,  365,  135,
 /*    90 */     7,    7,    7,    7,    7,    7,  465,    7,    7,    7,
 /*   100 */   393,  393,    7,    7,    7,    7,  351,  351,  351,  351,
 /*   110 */   491,  393,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   120 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   130 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   140 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   150 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   160 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   170 */   557,  630,  604,  630,  630,  630,  630,  572,  572,  572,
 /*   180 */   572,  630,  567,  574,  583,  581,  586,  593,  596,  599,
 /*   190 */   613,  620,  557,  632,  630,  630,  630,  667,  667,  644,
 /*   200 */     1,    1,  630,  630,  663,  671,  719,  676,  675,  718,
 /*   210 */   678,  681,  644,   35,  630,  630,  604,  604,  630,  604,
 /*   220 */   630,  604,  630,  630,  946,  946,   31,   68,   99,   99,
 /*   230 */    99,  133,  163,  191,  280,  280,  280,  280,  280,  280,
 /*   240 */   317,  317,  317,  317,  297,  382,  397,  341,  341,  341,
 /*   250 */   341,  341,  142,  312,  420,  429,  183,   37,   37,  139,
 /*   260 */   416,  285,  444,  508,  510,  511,  566,  515,  516,  517,
 /*   270 */   455,  504,  505,  520,  521,  523,  524,  525,  544,  527,
 /*   280 */   528,  598,  616,  513,  545,  479,  486,  487,  642,  650,
 /*   290 */   621,  498,  506,  548,  507,  649,  778,  624,  780,  782,
 /*   300 */   628,  784,  785,  685,  687,  647,  672,  714,  713,  673,
 /*   310 */   716,  715,  703,  704,  720,  705,  711,  717,  826,  837,
 /*   320 */   753,  755,  754,  756,  758,  759,  744,  761,  762,  764,
 /*   330 */   851,  765,  751,  721,  809,  857,  757,  722,  766,  769,
 /*   340 */   714,  771,  779,  775,  745,  783,  786,  772,  781,  792,
 /*   350 */   867,  787,  789,  864,  869,  871,  872,  873,  874,  875,
 /*   360 */   788,  868,  800,  877,  878,  798,  799,  801,  880,  882,
 /*   370 */   767,  804,  870,  831,  879,  739,  741,  795,  795,  795,
 /*   380 */   795,  773,  774,  886,  746,  747,  795,  795,  795,  901,
 /*   390 */   902,  817,  795,  904,  905,  906,  907,  908,  909,  910,
 /*   400 */   911,  912,  913,  914,  915,  916,  917,  918,  919,  820,
 /*   410 */   833,  854,  920,  921,  922,  923,  863,  930,
};
#define YY_REDUCE_COUNT (225)
#define YY_REDUCE_MIN   (-284)
#define YY_REDUCE_MAX   (626)
static const short yy_reduce_ofst[] = {
 /*     0 */    67,   44,   44,  168,  168,   53,  115,  200,  206, -280,
 /*    10 */  -206, -204, -183,  138,  177,  193,  230,  243,  269,  272,
 /*    20 */   278,  284, -284,  218, -162, -190, -239, -172, -101,   77,
 /*    30 */   120,  173,  229,  234,  238,  241,  245,  247,  256,  261,
 /*    40 */   262,  263,  267,  268,  275,  276,  279,  281,  282,  283,
 /*    50 */   287,  288,  290,  294, -110,  320,  321,   -8,  326, -128,
 /*    60 */  -184,  172, -207,  220,  239,  340,  325,  366,  146,  299,
 /*    70 */   302,  376,  378, -144,  301,   54,  381, -178, -147,  -74,
 /*    80 */  -229,  -81,  -39,   46,   72,   78,  109,  113,  121,  197,
 /*    90 */   217,  273,  286,  345,  418,  439,  271,  442,  452,  457,
 /*   100 */   408,  409,  460,  462,  463,  464,  385,  386,  387,  388,
 /*   110 */   414,  421,  470,  472,  473,  474,  475,  476,  477,  478,
 /*   120 */   480,  481,  482,  483,  484,  485,  488,  489,  490,  492,
 /*   130 */   493,  494,  495,  496,  497,  499,  500,  501,  502,  503,
 /*   140 */   509,  512,  514,  518,  519,  522,  526,  529,  530,  531,
 /*   150 */   532,  533,  534,  535,  536,  537,  538,  539,  540,  541,
 /*   160 */   542,  543,  546,  547,  549,  550,  551,  552,  553,  554,
 /*   170 */   423,  555,  556,  558,  559,  560,  561,  430,  431,  432,
 /*   180 */   433,  562,  443,  445,  451,  456,  461,  466,  563,  565,
 /*   190 */   564,  569,  571,  568,  570,  573,  575,  576,  577,  579,
 /*   200 */   578,  582,  580,  585,  584,  587,  589,  588,  591,  590,
 /*   210 */   594,  600,  592,  597,  595,  602,  607,  609,  611,  612,
 /*   220 */   614,  618,  617,  626,  623,  625,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   984, 1111, 1049, 1122, 1036, 1046,  984,  984,  984,  984,
 /*    10 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*    20 */   984,  984,  984,  984, 1176, 1004,  984,  984,  984,  984,
 /*    30 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*    40 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*    50 */   984,  984,  984,  984,  984,  984,  984,  984,  984, 1200,
 /*    60 */   984, 1046,  984,  984,  984,  984,  984, 1057, 1046,  984,
 /*    70 */   984, 1057, 1057,  984, 1171, 1095, 1113,  984,  984,  984,
 /*    80 */   984,  984,  984,  984,  984,  984,  984,  984, 1143,  984,
 /*    90 */   984,  984,  984,  984,  984,  984, 1178, 1184, 1181,  984,
 /*   100 */   984,  984, 1186,  984,  984,  984, 1222, 1222, 1222, 1222,
 /*   110 */  1169,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   120 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   130 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   140 */   984,  984,  984,  984,  984, 1034,  984, 1032,  984,  984,
 /*   150 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   160 */   984,  984,  984,  984,  984,  984,  984,  984,  984, 1002,
 /*   170 */  1239, 1006, 1044, 1006, 1006, 1006, 1006,  984,  984,  984,
 /*   180 */   984, 1006, 1231, 1235, 1212, 1229, 1223, 1207, 1205, 1203,
 /*   190 */  1211, 1196, 1239, 1145, 1006, 1006, 1006, 1055, 1055, 1050,
 /*   200 */  1046, 1046, 1006, 1006, 1073, 1071, 1069, 1061, 1067, 1063,
 /*   210 */  1065, 1059, 1037,  984, 1006, 1006, 1044, 1044, 1006, 1044,
 /*   220 */  1006, 1044, 1006, 1006, 1095, 1113, 1296,  984, 1240, 1230,
 /*   230 */  1296,  984, 1273, 1272, 1287, 1286, 1285, 1271, 1270, 1269,
 /*   240 */  1265, 1268, 1267, 1266,  984,  984,  984, 1284, 1283, 1281,
 /*   250 */  1280, 1279,  984,  984, 1242,  984,  984, 1275, 1274,  984,
 /*   260 */   984,  984,  984,  984,  984,  984, 1193,  984,  984,  984,
 /*   270 */  1218, 1236, 1232,  984,  984,  984,  984,  984,  984,  984,
 /*   280 */   984, 1243,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   290 */  1157,  984,  984, 1124,  984,  984,  984,  984,  984,  984,
 /*   300 */   984,  984,  984,  984,  984,  984, 1168,  984,  984,  984,
 /*   310 */   984,  984, 1180, 1179,  984,  984,  984,  984,  984,  984,
 /*   320 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   330 */   984,  984, 1224,  984, 1219,  984, 1213,  984,  984,  984,
 /*   340 */  1136,  984,  984,  984,  984, 1053,  984,  984,  984,  984,
 /*   350 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   360 */   984,  984,  984,  984,  984,  984,  984,  984,  984,  984,
 /*   370 */   984,  984,  984,  984,  984,  984,  984, 1314, 1309, 1310,
 /*   380 */  1307,  984,  984,  984,  984,  984, 1306, 1301, 1302,  984,
 /*   390 */   984,  984, 1299,  984,  984,  984,  984,  984,  984,  984,
 /*   400 */   984,  984,  984,  984,  984,  984,  984,  984,  984, 1079,
 /*   410 */   984,  984,  984, 1013,  984, 1011,  984,  984,
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
 /* 274 */ "expr ::= ID LP RP",
 /* 275 */ "expr ::= ID LP expr AS typename RP",
 /* 276 */ "expr ::= expr IS NULL",
 /* 277 */ "expr ::= expr IS NOT NULL",
 /* 278 */ "expr ::= expr LT expr",
 /* 279 */ "expr ::= expr GT expr",
 /* 280 */ "expr ::= expr LE expr",
 /* 281 */ "expr ::= expr GE expr",
 /* 282 */ "expr ::= expr NE expr",
 /* 283 */ "expr ::= expr EQ expr",
 /* 284 */ "expr ::= expr BETWEEN expr AND expr",
 /* 285 */ "expr ::= expr AND expr",
 /* 286 */ "expr ::= expr OR expr",
 /* 287 */ "expr ::= expr PLUS expr",
 /* 288 */ "expr ::= expr MINUS expr",
 /* 289 */ "expr ::= expr STAR expr",
 /* 290 */ "expr ::= expr SLASH expr",
 /* 291 */ "expr ::= expr REM expr",
 /* 292 */ "expr ::= expr BITAND expr",
 /* 293 */ "expr ::= expr BITOR expr",
 /* 294 */ "expr ::= expr BITXOR expr",
 /* 295 */ "expr ::= BITNOT expr",
 /* 296 */ "expr ::= expr LSHIFT expr",
 /* 297 */ "expr ::= expr RSHIFT expr",
 /* 298 */ "expr ::= expr LIKE expr",
 /* 299 */ "expr ::= expr MATCH expr",
 /* 300 */ "expr ::= expr NMATCH expr",
 /* 301 */ "expr ::= ID CONTAINS STRING",
 /* 302 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 303 */ "arrow ::= ID ARROW STRING",
 /* 304 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 305 */ "expr ::= arrow",
 /* 306 */ "expr ::= expr IN LP exprlist RP",
 /* 307 */ "exprlist ::= exprlist COMMA expritem",
 /* 308 */ "exprlist ::= expritem",
 /* 309 */ "expritem ::= expr",
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
    /* assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD ); */
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( i>=YY_NLOOKAHEAD || yy_lookahead[i]!=iLookAhead ){
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
          j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) &&
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
  {  208,   -3 }, /* (29) cmd ::= SHOW dbPrefix ALIVE */
  {  208,   -3 }, /* (30) cmd ::= SHOW CLUSTER ALIVE */
  {  208,   -5 }, /* (31) cmd ::= DROP TABLE ifexists ids cpxName */
  {  208,   -5 }, /* (32) cmd ::= DROP STABLE ifexists ids cpxName */
  {  208,   -4 }, /* (33) cmd ::= DROP DATABASE ifexists ids */
  {  208,   -4 }, /* (34) cmd ::= DROP TOPIC ifexists ids */
  {  208,   -3 }, /* (35) cmd ::= DROP FUNCTION ids */
  {  208,   -3 }, /* (36) cmd ::= DROP DNODE ids */
  {  208,   -3 }, /* (37) cmd ::= DROP USER ids */
  {  208,   -3 }, /* (38) cmd ::= DROP ACCOUNT ids */
  {  208,   -2 }, /* (39) cmd ::= USE ids */
  {  208,   -3 }, /* (40) cmd ::= DESCRIBE ids cpxName */
  {  208,   -3 }, /* (41) cmd ::= DESC ids cpxName */
  {  208,   -5 }, /* (42) cmd ::= ALTER USER ids PASS ids */
  {  208,   -5 }, /* (43) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  208,   -5 }, /* (44) cmd ::= ALTER USER ids TAGS ids */
  {  208,   -4 }, /* (45) cmd ::= ALTER DNODE ids ids */
  {  208,   -5 }, /* (46) cmd ::= ALTER DNODE ids ids ids */
  {  208,   -3 }, /* (47) cmd ::= ALTER LOCAL ids */
  {  208,   -4 }, /* (48) cmd ::= ALTER LOCAL ids ids */
  {  208,   -4 }, /* (49) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  208,   -4 }, /* (50) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  208,   -4 }, /* (51) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  208,   -6 }, /* (52) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  208,   -6 }, /* (53) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  209,   -1 }, /* (54) ids ::= ID */
  {  209,   -1 }, /* (55) ids ::= STRING */
  {  212,   -2 }, /* (56) ifexists ::= IF EXISTS */
  {  212,    0 }, /* (57) ifexists ::= */
  {  217,   -3 }, /* (58) ifnotexists ::= IF NOT EXISTS */
  {  217,    0 }, /* (59) ifnotexists ::= */
  {  208,   -3 }, /* (60) cmd ::= CREATE DNODE ids */
  {  208,   -6 }, /* (61) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  208,   -5 }, /* (62) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  208,   -5 }, /* (63) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  208,   -8 }, /* (64) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  208,   -9 }, /* (65) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  208,   -5 }, /* (66) cmd ::= CREATE USER ids PASS ids */
  {  208,   -7 }, /* (67) cmd ::= CREATE USER ids PASS ids TAGS ids */
  {  221,    0 }, /* (68) bufsize ::= */
  {  221,   -2 }, /* (69) bufsize ::= BUFSIZE INTEGER */
  {  222,    0 }, /* (70) pps ::= */
  {  222,   -2 }, /* (71) pps ::= PPS INTEGER */
  {  223,    0 }, /* (72) tseries ::= */
  {  223,   -2 }, /* (73) tseries ::= TSERIES INTEGER */
  {  224,    0 }, /* (74) dbs ::= */
  {  224,   -2 }, /* (75) dbs ::= DBS INTEGER */
  {  225,    0 }, /* (76) streams ::= */
  {  225,   -2 }, /* (77) streams ::= STREAMS INTEGER */
  {  226,    0 }, /* (78) storage ::= */
  {  226,   -2 }, /* (79) storage ::= STORAGE INTEGER */
  {  227,    0 }, /* (80) qtime ::= */
  {  227,   -2 }, /* (81) qtime ::= QTIME INTEGER */
  {  228,    0 }, /* (82) users ::= */
  {  228,   -2 }, /* (83) users ::= USERS INTEGER */
  {  229,    0 }, /* (84) conns ::= */
  {  229,   -2 }, /* (85) conns ::= CONNS INTEGER */
  {  230,    0 }, /* (86) state ::= */
  {  230,   -2 }, /* (87) state ::= STATE ids */
  {  215,   -9 }, /* (88) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  231,   -3 }, /* (89) intitemlist ::= intitemlist COMMA intitem */
  {  231,   -1 }, /* (90) intitemlist ::= intitem */
  {  232,   -1 }, /* (91) intitem ::= INTEGER */
  {  233,   -2 }, /* (92) keep ::= KEEP intitemlist */
  {  234,   -2 }, /* (93) cache ::= CACHE INTEGER */
  {  235,   -2 }, /* (94) replica ::= REPLICA INTEGER */
  {  236,   -2 }, /* (95) quorum ::= QUORUM INTEGER */
  {  237,   -2 }, /* (96) days ::= DAYS INTEGER */
  {  238,   -2 }, /* (97) minrows ::= MINROWS INTEGER */
  {  239,   -2 }, /* (98) maxrows ::= MAXROWS INTEGER */
  {  240,   -2 }, /* (99) blocks ::= BLOCKS INTEGER */
  {  241,   -2 }, /* (100) ctime ::= CTIME INTEGER */
  {  242,   -2 }, /* (101) wal ::= WAL INTEGER */
  {  243,   -2 }, /* (102) fsync ::= FSYNC INTEGER */
  {  244,   -2 }, /* (103) comp ::= COMP INTEGER */
  {  245,   -2 }, /* (104) prec ::= PRECISION STRING */
  {  246,   -2 }, /* (105) update ::= UPDATE INTEGER */
  {  247,   -2 }, /* (106) cachelast ::= CACHELAST INTEGER */
  {  248,   -2 }, /* (107) partitions ::= PARTITIONS INTEGER */
  {  218,    0 }, /* (108) db_optr ::= */
  {  218,   -2 }, /* (109) db_optr ::= db_optr cache */
  {  218,   -2 }, /* (110) db_optr ::= db_optr replica */
  {  218,   -2 }, /* (111) db_optr ::= db_optr quorum */
  {  218,   -2 }, /* (112) db_optr ::= db_optr days */
  {  218,   -2 }, /* (113) db_optr ::= db_optr minrows */
  {  218,   -2 }, /* (114) db_optr ::= db_optr maxrows */
  {  218,   -2 }, /* (115) db_optr ::= db_optr blocks */
  {  218,   -2 }, /* (116) db_optr ::= db_optr ctime */
  {  218,   -2 }, /* (117) db_optr ::= db_optr wal */
  {  218,   -2 }, /* (118) db_optr ::= db_optr fsync */
  {  218,   -2 }, /* (119) db_optr ::= db_optr comp */
  {  218,   -2 }, /* (120) db_optr ::= db_optr prec */
  {  218,   -2 }, /* (121) db_optr ::= db_optr keep */
  {  218,   -2 }, /* (122) db_optr ::= db_optr update */
  {  218,   -2 }, /* (123) db_optr ::= db_optr cachelast */
  {  219,   -1 }, /* (124) topic_optr ::= db_optr */
  {  219,   -2 }, /* (125) topic_optr ::= topic_optr partitions */
  {  213,    0 }, /* (126) alter_db_optr ::= */
  {  213,   -2 }, /* (127) alter_db_optr ::= alter_db_optr replica */
  {  213,   -2 }, /* (128) alter_db_optr ::= alter_db_optr quorum */
  {  213,   -2 }, /* (129) alter_db_optr ::= alter_db_optr keep */
  {  213,   -2 }, /* (130) alter_db_optr ::= alter_db_optr blocks */
  {  213,   -2 }, /* (131) alter_db_optr ::= alter_db_optr comp */
  {  213,   -2 }, /* (132) alter_db_optr ::= alter_db_optr update */
  {  213,   -2 }, /* (133) alter_db_optr ::= alter_db_optr cachelast */
  {  213,   -2 }, /* (134) alter_db_optr ::= alter_db_optr minrows */
  {  214,   -1 }, /* (135) alter_topic_optr ::= alter_db_optr */
  {  214,   -2 }, /* (136) alter_topic_optr ::= alter_topic_optr partitions */
  {  220,   -1 }, /* (137) typename ::= ids */
  {  220,   -4 }, /* (138) typename ::= ids LP signed RP */
  {  220,   -2 }, /* (139) typename ::= ids UNSIGNED */
  {  249,   -1 }, /* (140) signed ::= INTEGER */
  {  249,   -2 }, /* (141) signed ::= PLUS INTEGER */
  {  249,   -2 }, /* (142) signed ::= MINUS INTEGER */
  {  208,   -3 }, /* (143) cmd ::= CREATE TABLE create_table_args */
  {  208,   -3 }, /* (144) cmd ::= CREATE TABLE create_stable_args */
  {  208,   -3 }, /* (145) cmd ::= CREATE STABLE create_stable_args */
  {  208,   -3 }, /* (146) cmd ::= CREATE TABLE create_table_list */
  {  252,   -1 }, /* (147) create_table_list ::= create_from_stable */
  {  252,   -2 }, /* (148) create_table_list ::= create_table_list create_from_stable */
  {  250,   -6 }, /* (149) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  251,  -10 }, /* (150) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  253,  -10 }, /* (151) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  253,  -13 }, /* (152) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  256,   -3 }, /* (153) tagNamelist ::= tagNamelist COMMA ids */
  {  256,   -1 }, /* (154) tagNamelist ::= ids */
  {  250,   -7 }, /* (155) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  257,    0 }, /* (156) to_opt ::= */
  {  257,   -3 }, /* (157) to_opt ::= TO ids cpxName */
  {  258,    0 }, /* (158) split_opt ::= */
  {  258,   -2 }, /* (159) split_opt ::= SPLIT ids */
  {  254,   -3 }, /* (160) columnlist ::= columnlist COMMA column */
  {  254,   -1 }, /* (161) columnlist ::= column */
  {  261,   -2 }, /* (162) column ::= ids typename */
  {  255,   -3 }, /* (163) tagitemlist ::= tagitemlist COMMA tagitem */
  {  255,   -1 }, /* (164) tagitemlist ::= tagitem */
  {  262,   -1 }, /* (165) tagitem ::= INTEGER */
  {  262,   -1 }, /* (166) tagitem ::= FLOAT */
  {  262,   -1 }, /* (167) tagitem ::= STRING */
  {  262,   -1 }, /* (168) tagitem ::= BOOL */
  {  262,   -1 }, /* (169) tagitem ::= NULL */
  {  262,   -1 }, /* (170) tagitem ::= NOW */
  {  262,   -3 }, /* (171) tagitem ::= NOW PLUS VARIABLE */
  {  262,   -3 }, /* (172) tagitem ::= NOW MINUS VARIABLE */
  {  262,   -2 }, /* (173) tagitem ::= MINUS INTEGER */
  {  262,   -2 }, /* (174) tagitem ::= MINUS FLOAT */
  {  262,   -2 }, /* (175) tagitem ::= PLUS INTEGER */
  {  262,   -2 }, /* (176) tagitem ::= PLUS FLOAT */
  {  259,  -15 }, /* (177) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  259,   -3 }, /* (178) select ::= LP select RP */
  {  277,   -1 }, /* (179) union ::= select */
  {  277,   -4 }, /* (180) union ::= union UNION ALL select */
  {  208,   -1 }, /* (181) cmd ::= union */
  {  259,   -2 }, /* (182) select ::= SELECT selcollist */
  {  278,   -2 }, /* (183) sclp ::= selcollist COMMA */
  {  278,    0 }, /* (184) sclp ::= */
  {  263,   -4 }, /* (185) selcollist ::= sclp distinct expr as */
  {  263,   -2 }, /* (186) selcollist ::= sclp STAR */
  {  281,   -2 }, /* (187) as ::= AS ids */
  {  281,   -1 }, /* (188) as ::= ids */
  {  281,    0 }, /* (189) as ::= */
  {  279,   -1 }, /* (190) distinct ::= DISTINCT */
  {  279,    0 }, /* (191) distinct ::= */
  {  264,   -2 }, /* (192) from ::= FROM tablelist */
  {  264,   -2 }, /* (193) from ::= FROM sub */
  {  283,   -3 }, /* (194) sub ::= LP union RP */
  {  283,   -4 }, /* (195) sub ::= LP union RP ids */
  {  283,   -6 }, /* (196) sub ::= sub COMMA LP union RP ids */
  {  282,   -2 }, /* (197) tablelist ::= ids cpxName */
  {  282,   -3 }, /* (198) tablelist ::= ids cpxName ids */
  {  282,   -4 }, /* (199) tablelist ::= tablelist COMMA ids cpxName */
  {  282,   -5 }, /* (200) tablelist ::= tablelist COMMA ids cpxName ids */
  {  284,   -1 }, /* (201) tmvar ::= VARIABLE */
  {  285,   -1 }, /* (202) timestamp ::= INTEGER */
  {  285,   -2 }, /* (203) timestamp ::= MINUS INTEGER */
  {  285,   -2 }, /* (204) timestamp ::= PLUS INTEGER */
  {  285,   -1 }, /* (205) timestamp ::= STRING */
  {  285,   -1 }, /* (206) timestamp ::= NOW */
  {  285,   -3 }, /* (207) timestamp ::= NOW PLUS VARIABLE */
  {  285,   -3 }, /* (208) timestamp ::= NOW MINUS VARIABLE */
  {  266,    0 }, /* (209) range_option ::= */
  {  266,   -6 }, /* (210) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  267,   -4 }, /* (211) interval_option ::= intervalKey LP tmvar RP */
  {  267,   -6 }, /* (212) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  267,    0 }, /* (213) interval_option ::= */
  {  286,   -1 }, /* (214) intervalKey ::= INTERVAL */
  {  286,   -1 }, /* (215) intervalKey ::= EVERY */
  {  269,    0 }, /* (216) session_option ::= */
  {  269,   -7 }, /* (217) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  270,    0 }, /* (218) windowstate_option ::= */
  {  270,   -4 }, /* (219) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  271,    0 }, /* (220) fill_opt ::= */
  {  271,   -6 }, /* (221) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  271,   -4 }, /* (222) fill_opt ::= FILL LP ID RP */
  {  268,   -4 }, /* (223) sliding_opt ::= SLIDING LP tmvar RP */
  {  268,    0 }, /* (224) sliding_opt ::= */
  {  274,    0 }, /* (225) orderby_opt ::= */
  {  274,   -3 }, /* (226) orderby_opt ::= ORDER BY sortlist */
  {  287,   -4 }, /* (227) sortlist ::= sortlist COMMA item sortorder */
  {  287,   -4 }, /* (228) sortlist ::= sortlist COMMA arrow sortorder */
  {  287,   -2 }, /* (229) sortlist ::= item sortorder */
  {  287,   -2 }, /* (230) sortlist ::= arrow sortorder */
  {  288,   -1 }, /* (231) item ::= ID */
  {  288,   -3 }, /* (232) item ::= ID DOT ID */
  {  289,   -1 }, /* (233) sortorder ::= ASC */
  {  289,   -1 }, /* (234) sortorder ::= DESC */
  {  289,    0 }, /* (235) sortorder ::= */
  {  272,    0 }, /* (236) groupby_opt ::= */
  {  272,   -3 }, /* (237) groupby_opt ::= GROUP BY grouplist */
  {  291,   -3 }, /* (238) grouplist ::= grouplist COMMA item */
  {  291,   -3 }, /* (239) grouplist ::= grouplist COMMA arrow */
  {  291,   -1 }, /* (240) grouplist ::= item */
  {  291,   -1 }, /* (241) grouplist ::= arrow */
  {  273,    0 }, /* (242) having_opt ::= */
  {  273,   -2 }, /* (243) having_opt ::= HAVING expr */
  {  276,    0 }, /* (244) limit_opt ::= */
  {  276,   -2 }, /* (245) limit_opt ::= LIMIT signed */
  {  276,   -4 }, /* (246) limit_opt ::= LIMIT signed OFFSET signed */
  {  276,   -4 }, /* (247) limit_opt ::= LIMIT signed COMMA signed */
  {  275,    0 }, /* (248) slimit_opt ::= */
  {  275,   -2 }, /* (249) slimit_opt ::= SLIMIT signed */
  {  275,   -4 }, /* (250) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  275,   -4 }, /* (251) slimit_opt ::= SLIMIT signed COMMA signed */
  {  265,    0 }, /* (252) where_opt ::= */
  {  265,   -2 }, /* (253) where_opt ::= WHERE expr */
  {  280,   -3 }, /* (254) expr ::= LP expr RP */
  {  280,   -1 }, /* (255) expr ::= ID */
  {  280,   -3 }, /* (256) expr ::= ID DOT ID */
  {  280,   -3 }, /* (257) expr ::= ID DOT STAR */
  {  280,   -1 }, /* (258) expr ::= INTEGER */
  {  280,   -2 }, /* (259) expr ::= MINUS INTEGER */
  {  280,   -2 }, /* (260) expr ::= PLUS INTEGER */
  {  280,   -1 }, /* (261) expr ::= FLOAT */
  {  280,   -2 }, /* (262) expr ::= MINUS FLOAT */
  {  280,   -2 }, /* (263) expr ::= PLUS FLOAT */
  {  280,   -1 }, /* (264) expr ::= STRING */
  {  280,   -1 }, /* (265) expr ::= NOW */
  {  280,   -1 }, /* (266) expr ::= TODAY */
  {  280,   -1 }, /* (267) expr ::= VARIABLE */
  {  280,   -2 }, /* (268) expr ::= PLUS VARIABLE */
  {  280,   -2 }, /* (269) expr ::= MINUS VARIABLE */
  {  280,   -1 }, /* (270) expr ::= BOOL */
  {  280,   -1 }, /* (271) expr ::= NULL */
  {  280,   -4 }, /* (272) expr ::= ID LP exprlist RP */
  {  280,   -4 }, /* (273) expr ::= ID LP STAR RP */
  {  280,   -3 }, /* (274) expr ::= ID LP RP */
  {  280,   -6 }, /* (275) expr ::= ID LP expr AS typename RP */
  {  280,   -3 }, /* (276) expr ::= expr IS NULL */
  {  280,   -4 }, /* (277) expr ::= expr IS NOT NULL */
  {  280,   -3 }, /* (278) expr ::= expr LT expr */
  {  280,   -3 }, /* (279) expr ::= expr GT expr */
  {  280,   -3 }, /* (280) expr ::= expr LE expr */
  {  280,   -3 }, /* (281) expr ::= expr GE expr */
  {  280,   -3 }, /* (282) expr ::= expr NE expr */
  {  280,   -3 }, /* (283) expr ::= expr EQ expr */
  {  280,   -5 }, /* (284) expr ::= expr BETWEEN expr AND expr */
  {  280,   -3 }, /* (285) expr ::= expr AND expr */
  {  280,   -3 }, /* (286) expr ::= expr OR expr */
  {  280,   -3 }, /* (287) expr ::= expr PLUS expr */
  {  280,   -3 }, /* (288) expr ::= expr MINUS expr */
  {  280,   -3 }, /* (289) expr ::= expr STAR expr */
  {  280,   -3 }, /* (290) expr ::= expr SLASH expr */
  {  280,   -3 }, /* (291) expr ::= expr REM expr */
  {  280,   -3 }, /* (292) expr ::= expr BITAND expr */
  {  280,   -3 }, /* (293) expr ::= expr BITOR expr */
  {  280,   -3 }, /* (294) expr ::= expr BITXOR expr */
  {  280,   -2 }, /* (295) expr ::= BITNOT expr */
  {  280,   -3 }, /* (296) expr ::= expr LSHIFT expr */
  {  280,   -3 }, /* (297) expr ::= expr RSHIFT expr */
  {  280,   -3 }, /* (298) expr ::= expr LIKE expr */
  {  280,   -3 }, /* (299) expr ::= expr MATCH expr */
  {  280,   -3 }, /* (300) expr ::= expr NMATCH expr */
  {  280,   -3 }, /* (301) expr ::= ID CONTAINS STRING */
  {  280,   -5 }, /* (302) expr ::= ID DOT ID CONTAINS STRING */
  {  290,   -3 }, /* (303) arrow ::= ID ARROW STRING */
  {  290,   -5 }, /* (304) arrow ::= ID DOT ID ARROW STRING */
  {  280,   -1 }, /* (305) expr ::= arrow */
  {  280,   -5 }, /* (306) expr ::= expr IN LP exprlist RP */
  {  216,   -3 }, /* (307) exprlist ::= exprlist COMMA expritem */
  {  216,   -1 }, /* (308) exprlist ::= expritem */
  {  292,   -1 }, /* (309) expritem ::= expr */
  {  208,   -3 }, /* (310) cmd ::= RESET QUERY CACHE */
  {  208,   -3 }, /* (311) cmd ::= SYNCDB ids REPLICA */
  {  208,   -7 }, /* (312) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (313) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (314) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (315) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (316) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (317) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (318) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (319) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -7 }, /* (320) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (321) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (322) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (323) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (324) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (325) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (326) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (327) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -3 }, /* (328) cmd ::= KILL CONNECTION INTEGER */
  {  208,   -5 }, /* (329) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  208,   -5 }, /* (330) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  208,   -6 }, /* (331) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy564, &t);}
        break;
      case 51: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy563);}
        break;
      case 52: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy563);}
        break;
      case 53: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy367);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy563);}
        break;
      case 62: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 63: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==63);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy564, &yymsp[-2].minor.yy0);}
        break;
      case 64: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy307, &yymsp[0].minor.yy0, 1);}
        break;
      case 65: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy307, &yymsp[0].minor.yy0, 2);}
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
      case 89: /* intitemlist ::= intitemlist COMMA intitem */
      case 163: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==163);
{ yylhsminor.yy367 = tVariantListAppend(yymsp[-2].minor.yy367, &yymsp[0].minor.yy410, -1);    }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 90: /* intitemlist ::= intitem */
      case 164: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==164);
{ yylhsminor.yy367 = tVariantListAppend(NULL, &yymsp[0].minor.yy410, -1); }
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 91: /* intitem ::= INTEGER */
      case 165: /* tagitem ::= INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= FLOAT */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= STRING */ yytestcase(yyruleno==167);
      case 168: /* tagitem ::= BOOL */ yytestcase(yyruleno==168);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 92: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy367 = yymsp[0].minor.yy367; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy564); yymsp[1].minor.yy564.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 109: /* db_optr ::= db_optr cache */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 110: /* db_optr ::= db_optr replica */
      case 127: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==127);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 111: /* db_optr ::= db_optr quorum */
      case 128: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==128);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 112: /* db_optr ::= db_optr days */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 113: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 114: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 115: /* db_optr ::= db_optr blocks */
      case 130: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==130);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 116: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 117: /* db_optr ::= db_optr wal */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 118: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 119: /* db_optr ::= db_optr comp */
      case 131: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==131);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 120: /* db_optr ::= db_optr prec */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 121: /* db_optr ::= db_optr keep */
      case 129: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==129);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.keep = yymsp[0].minor.yy367; }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 122: /* db_optr ::= db_optr update */
      case 132: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==132);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 123: /* db_optr ::= db_optr cachelast */
      case 133: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==133);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 124: /* topic_optr ::= db_optr */
      case 135: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==135);
{ yylhsminor.yy564 = yymsp[0].minor.yy564; yylhsminor.yy564.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy564 = yylhsminor.yy564;
        break;
      case 125: /* topic_optr ::= topic_optr partitions */
      case 136: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==136);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 126: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy564); yymsp[1].minor.yy564.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 134: /* alter_db_optr ::= alter_db_optr minrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.minRowsPerBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 137: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy307, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy307 = yylhsminor.yy307;
        break;
      case 138: /* typename ::= ids LP signed RP */
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
      case 139: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy307, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy307 = yylhsminor.yy307;
        break;
      case 140: /* signed ::= INTEGER */
{ yylhsminor.yy443 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy443 = yylhsminor.yy443;
        break;
      case 141: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy443 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 142: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy443 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 146: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy74;}
        break;
      case 147: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy110);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy74 = pCreateTable;
}
  yymsp[0].minor.yy74 = yylhsminor.yy74;
        break;
      case 148: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy74->childTableInfo, &yymsp[0].minor.yy110);
  yylhsminor.yy74 = yymsp[-1].minor.yy74;
}
  yymsp[-1].minor.yy74 = yylhsminor.yy74;
        break;
      case 149: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy74 = tSetCreateTableInfo(yymsp[-1].minor.yy367, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy74 = yylhsminor.yy74;
        break;
      case 150: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy74 = tSetCreateTableInfo(yymsp[-5].minor.yy367, yymsp[-1].minor.yy367, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy74 = yylhsminor.yy74;
        break;
      case 151: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy110 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy367, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy110 = yylhsminor.yy110;
        break;
      case 152: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy110 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy367, yymsp[-1].minor.yy367, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy110 = yylhsminor.yy110;
        break;
      case 153: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy367, &yymsp[0].minor.yy0); yylhsminor.yy367 = yymsp[-2].minor.yy367;  }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 154: /* tagNamelist ::= ids */
{yylhsminor.yy367 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy367, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 155: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy74 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy426, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy74 = yylhsminor.yy74;
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
{taosArrayPush(yymsp[-2].minor.yy367, &yymsp[0].minor.yy307); yylhsminor.yy367 = yymsp[-2].minor.yy367;  }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 161: /* columnlist ::= column */
{yylhsminor.yy367 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy367, &yymsp[0].minor.yy307);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 162: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy307, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy307);
}
  yymsp[-1].minor.yy307 = yylhsminor.yy307;
        break;
      case 169: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 170: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy410, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 171: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy410, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 172: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy410, &yymsp[0].minor.yy0, TK_MINUS, true);
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
    tVariantCreate(&yylhsminor.yy410, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy410 = yylhsminor.yy410;
        break;
      case 177: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy426 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy367, yymsp[-12].minor.yy480, yymsp[-11].minor.yy378, yymsp[-4].minor.yy367, yymsp[-2].minor.yy367, &yymsp[-9].minor.yy478, &yymsp[-7].minor.yy373, &yymsp[-6].minor.yy204, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy367, &yymsp[0].minor.yy24, &yymsp[-1].minor.yy24, yymsp[-3].minor.yy378, &yymsp[-10].minor.yy214);
}
  yymsp[-14].minor.yy426 = yylhsminor.yy426;
        break;
      case 178: /* select ::= LP select RP */
{yymsp[-2].minor.yy426 = yymsp[-1].minor.yy426;}
        break;
      case 179: /* union ::= select */
{ yylhsminor.yy367 = setSubclause(NULL, yymsp[0].minor.yy426); }
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 180: /* union ::= union UNION ALL select */
{ yylhsminor.yy367 = appendSelectClause(yymsp[-3].minor.yy367, yymsp[0].minor.yy426); }
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 181: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy367, NULL, TSDB_SQL_SELECT); }
        break;
      case 182: /* select ::= SELECT selcollist */
{
  yylhsminor.yy426 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy367, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy426 = yylhsminor.yy426;
        break;
      case 183: /* sclp ::= selcollist COMMA */
{yylhsminor.yy367 = yymsp[-1].minor.yy367;}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 184: /* sclp ::= */
      case 225: /* orderby_opt ::= */ yytestcase(yyruleno==225);
{yymsp[1].minor.yy367 = 0;}
        break;
      case 185: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy367 = tSqlExprListAppend(yymsp[-3].minor.yy367, yymsp[-1].minor.yy378,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 186: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy367 = tSqlExprListAppend(yymsp[-1].minor.yy367, pNode, 0, 0);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
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
{yymsp[-1].minor.yy480 = yymsp[0].minor.yy480;}
        break;
      case 194: /* sub ::= LP union RP */
{yymsp[-2].minor.yy480 = addSubqueryElem(NULL, yymsp[-1].minor.yy367, NULL);}
        break;
      case 195: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy480 = addSubqueryElem(NULL, yymsp[-2].minor.yy367, &yymsp[0].minor.yy0);}
        break;
      case 196: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy480 = addSubqueryElem(yymsp[-5].minor.yy480, yymsp[-2].minor.yy367, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy480 = yylhsminor.yy480;
        break;
      case 197: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 198: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 199: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 200: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(yymsp[-4].minor.yy480, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy480 = yylhsminor.yy480;
        break;
      case 201: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 202: /* timestamp ::= INTEGER */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 203: /* timestamp ::= MINUS INTEGER */
      case 204: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==204);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 205: /* timestamp ::= STRING */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 206: /* timestamp ::= NOW */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 207: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 208: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 209: /* range_option ::= */
{yymsp[1].minor.yy214.start = 0; yymsp[1].minor.yy214.end = 0;}
        break;
      case 210: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy214.start = yymsp[-3].minor.yy378; yymsp[-5].minor.yy214.end = yymsp[-1].minor.yy378;}
        break;
      case 211: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy478.interval = yymsp[-1].minor.yy0; yylhsminor.yy478.offset.n = 0; yylhsminor.yy478.token = yymsp[-3].minor.yy586;}
  yymsp[-3].minor.yy478 = yylhsminor.yy478;
        break;
      case 212: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy478.interval = yymsp[-3].minor.yy0; yylhsminor.yy478.offset = yymsp[-1].minor.yy0;   yylhsminor.yy478.token = yymsp[-5].minor.yy586;}
  yymsp[-5].minor.yy478 = yylhsminor.yy478;
        break;
      case 213: /* interval_option ::= */
{memset(&yymsp[1].minor.yy478, 0, sizeof(yymsp[1].minor.yy478));}
        break;
      case 214: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy586 = TK_INTERVAL;}
        break;
      case 215: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy586 = TK_EVERY;   }
        break;
      case 216: /* session_option ::= */
{yymsp[1].minor.yy373.col.n = 0; yymsp[1].minor.yy373.gap.n = 0;}
        break;
      case 217: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy373.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy373.gap = yymsp[-1].minor.yy0;
}
        break;
      case 218: /* windowstate_option ::= */
{ yymsp[1].minor.yy204.col.n = 0; yymsp[1].minor.yy204.col.z = NULL;}
        break;
      case 219: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy204.col = yymsp[-1].minor.yy0; }
        break;
      case 220: /* fill_opt ::= */
{ yymsp[1].minor.yy367 = 0;     }
        break;
      case 221: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy367, &A, -1, 0);
    yymsp[-5].minor.yy367 = yymsp[-1].minor.yy367;
}
        break;
      case 222: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy367 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 223: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 224: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 226: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy367 = yymsp[0].minor.yy367;}
        break;
      case 227: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-3].minor.yy367, &yymsp[-1].minor.yy410, NULL, false, yymsp[0].minor.yy274);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 228: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-3].minor.yy367, NULL, yymsp[-1].minor.yy378, true, yymsp[0].minor.yy274);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 229: /* sortlist ::= item sortorder */
{
  yylhsminor.yy367 = commonItemAppend(NULL, &yymsp[-1].minor.yy410, NULL, false, yymsp[0].minor.yy274);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 230: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy367 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy378, true, yymsp[0].minor.yy274);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 231: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 232: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy410, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy410 = yylhsminor.yy410;
        break;
      case 233: /* sortorder ::= ASC */
{ yymsp[0].minor.yy274 = TSDB_ORDER_ASC; }
        break;
      case 234: /* sortorder ::= DESC */
{ yymsp[0].minor.yy274 = TSDB_ORDER_DESC;}
        break;
      case 235: /* sortorder ::= */
{ yymsp[1].minor.yy274 = TSDB_ORDER_ASC; }
        break;
      case 236: /* groupby_opt ::= */
{ yymsp[1].minor.yy367 = 0;}
        break;
      case 237: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy367 = yymsp[0].minor.yy367;}
        break;
      case 238: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-2].minor.yy367, &yymsp[0].minor.yy410, NULL, false, -1);
}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 239: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-2].minor.yy367, NULL, yymsp[0].minor.yy378, true, -1);
}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 240: /* grouplist ::= item */
{
  yylhsminor.yy367 = commonItemAppend(NULL, &yymsp[0].minor.yy410, NULL, false, -1);
}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 241: /* grouplist ::= arrow */
{
  yylhsminor.yy367 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy378, true, -1);
}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 242: /* having_opt ::= */
      case 252: /* where_opt ::= */ yytestcase(yyruleno==252);
{yymsp[1].minor.yy378 = 0;}
        break;
      case 243: /* having_opt ::= HAVING expr */
      case 253: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==253);
{yymsp[-1].minor.yy378 = yymsp[0].minor.yy378;}
        break;
      case 244: /* limit_opt ::= */
      case 248: /* slimit_opt ::= */ yytestcase(yyruleno==248);
{yymsp[1].minor.yy24.limit = -1; yymsp[1].minor.yy24.offset = 0;}
        break;
      case 245: /* limit_opt ::= LIMIT signed */
      case 249: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==249);
{yymsp[-1].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-1].minor.yy24.offset = 0;}
        break;
      case 246: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy443;}
        break;
      case 247: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy443;}
        break;
      case 250: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy443;}
        break;
      case 251: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy443;}
        break;
      case 254: /* expr ::= LP expr RP */
{yylhsminor.yy378 = yymsp[-1].minor.yy378; yylhsminor.yy378->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy378->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 255: /* expr ::= ID */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 256: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 257: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 258: /* expr ::= INTEGER */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 259: /* expr ::= MINUS INTEGER */
      case 260: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==260);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 261: /* expr ::= FLOAT */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 262: /* expr ::= MINUS FLOAT */
      case 263: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==263);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 264: /* expr ::= STRING */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 265: /* expr ::= NOW */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 266: /* expr ::= TODAY */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 267: /* expr ::= VARIABLE */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 268: /* expr ::= PLUS VARIABLE */
      case 269: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==269);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 270: /* expr ::= BOOL */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 271: /* expr ::= NULL */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 272: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(yymsp[-1].minor.yy367, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 273: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 274: /* expr ::= ID LP RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-2].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(tSqlExprListAppend(0, 0, 0, 0), &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-2].minor.yy0.type); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 275: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy378, &yymsp[-1].minor.yy307, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy378 = yylhsminor.yy378;
        break;
      case 276: /* expr ::= expr IS NULL */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 277: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-3].minor.yy378, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 278: /* expr ::= expr LT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 279: /* expr ::= expr GT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_GT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 280: /* expr ::= expr LE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 281: /* expr ::= expr GE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_GE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 282: /* expr ::= expr NE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 283: /* expr ::= expr EQ expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_EQ);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 284: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy378); yylhsminor.yy378 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy378, yymsp[-2].minor.yy378, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy378, TK_LE), TK_AND);}
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 285: /* expr ::= expr AND expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_AND);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 286: /* expr ::= expr OR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_OR); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 287: /* expr ::= expr PLUS expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_PLUS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 288: /* expr ::= expr MINUS expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MINUS); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 289: /* expr ::= expr STAR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_STAR);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 290: /* expr ::= expr SLASH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_DIVIDE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 291: /* expr ::= expr REM expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_REM);   }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 292: /* expr ::= expr BITAND expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITAND);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 293: /* expr ::= expr BITOR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITOR); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 294: /* expr ::= expr BITXOR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITXOR);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 295: /* expr ::= BITNOT expr */
{yymsp[-1].minor.yy378 = tSqlExprCreate(yymsp[0].minor.yy378, NULL, TK_BITNOT);}
        break;
      case 296: /* expr ::= expr LSHIFT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LSHIFT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 297: /* expr ::= expr RSHIFT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_RSHIFT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 298: /* expr ::= expr LIKE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LIKE);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 299: /* expr ::= expr MATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 300: /* expr ::= expr NMATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NMATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 301: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 302: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 303: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 304: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 305: /* expr ::= arrow */
      case 309: /* expritem ::= expr */ yytestcase(yyruleno==309);
{yylhsminor.yy378 = yymsp[0].minor.yy378;}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 306: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-4].minor.yy378, (tSqlExpr*)yymsp[-1].minor.yy367, TK_IN); }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 307: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy367 = tSqlExprListAppend(yymsp[-2].minor.yy367,yymsp[0].minor.yy378,0, 0);}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 308: /* exprlist ::= expritem */
{yylhsminor.yy367 = tSqlExprListAppend(0,yymsp[0].minor.yy378,0, 0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 323: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 327: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
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
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
