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
#define YYNSTATE             417
#define YYNRULE              329
#define YYNTOKEN             206
#define YY_MAX_SHIFT         416
#define YY_MIN_SHIFTREDUCE   647
#define YY_MAX_SHIFTREDUCE   975
#define YY_ERROR_ACTION      976
#define YY_ACCEPT_ACTION     977
#define YY_NO_ACTION         978
#define YY_MIN_REDUCE        979
#define YY_MAX_REDUCE        1307
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
#define YY_ACTTAB_COUNT (930)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   230,  699, 1139,  175, 1216,   65, 1217,  332,  699,  700,
 /*    10 */  1280,  270, 1282, 1164,   43,   44,  700,   47,   48,  415,
 /*    20 */   261,  283,   32,   31,   30,  736,   65,   46,  366,   51,
 /*    30 */    49,   52,   50,   37,   36,   35,   34,   33,   42,   41,
 /*    40 */   268,   24,   40,   39,   38,   43,   44, 1140,   47,   48,
 /*    50 */   263, 1280,  283,   32,   31,   30,  314, 1137,   46,  366,
 /*    60 */    51,   49,   52,   50,   37,   36,   35,   34,   33,   42,
 /*    70 */    41,  273,  228,   40,   39,   38,  313,  312, 1137,  275,
 /*    80 */    43,   44, 1280,   47,   48, 1161, 1140,  283,   32,   31,
 /*    90 */    30,  362,   95,   46,  366,   51,   49,   52,   50,   37,
 /*   100 */    36,   35,   34,   33,   42,   41,  229, 1270,   40,   39,
 /*   110 */    38,   43,   44,  401,   47,   48, 1280, 1280,  283,   32,
 /*   120 */    31,   30, 1122,   64,   46,  366,   51,   49,   52,   50,
 /*   130 */    37,   36,   35,   34,   33,   42,   41, 1302,  234,   40,
 /*   140 */    39,   38,  277,   43,   45,  784,   47,   48, 1280, 1140,
 /*   150 */   283,   32,   31,   30,   65,  898,   46,  366,   51,   49,
 /*   160 */    52,   50,   37,   36,   35,   34,   33,   42,   41,  858,
 /*   170 */   859,   40,   39,   38,   44,  298,   47,   48,  391,  390,
 /*   180 */   283,   32,   31,   30,  302,  301,   46,  366,   51,   49,
 /*   190 */    52,   50,   37,   36,   35,   34,   33,   42,   41,  699,
 /*   200 */   141,   40,   39,   38,   47,   48, 1136,  700,  283,   32,
 /*   210 */    31,   30,  362,  401,   46,  366,   51,   49,   52,   50,
 /*   220 */    37,   36,   35,   34,   33,   42,   41,  371,  190,   40,
 /*   230 */    39,   38,   73,  360,  408,  407,  359,  358,  357,  406,
 /*   240 */   356,  355,  354,  405,  353,  404,  403,  322,  648,  649,
 /*   250 */   650,  651,  652,  653,  654,  655,  656,  657,  658,  659,
 /*   260 */   660,  661,  169,  101,  262, 1098, 1086, 1087, 1088, 1089,
 /*   270 */  1090, 1091, 1092, 1093, 1094, 1095, 1096, 1097, 1099, 1100,
 /*   280 */    25,   51,   49,   52,   50,   37,   36,   35,   34,   33,
 /*   290 */    42,   41,  115,  699,   40,   39,   38,  244,  235,  254,
 /*   300 */   914,  700,   74,  902,  246,  905,  281,  908, 1280, 1028,
 /*   310 */   156,  155,  154,  245, 1155,  112,  211,  236,  374,  101,
 /*   320 */    37,   36,   35,   34,   33,   42,   41, 1280, 1124,   40,
 /*   330 */    39,   38,  264,  977,  416,  919,  258,  259,  254,  914,
 /*   340 */   368, 1214,  902, 1215,  905,   29,  908,  901, 1119, 1120,
 /*   350 */    61, 1123,    5,   68,  201,   40,   39,   38,   74,  200,
 /*   360 */   122,  127,  118,  126,  247,  812,  336,  107,  809,  106,
 /*   370 */   810,  904,  811,  907, 1280,  258,  259,  409, 1067,  349,
 /*   380 */    57,  305,  280,   91,   29, 1155,   53,   42,   41,  286,
 /*   390 */   255,   40,   39,   38,  295,  248,  292,  139,  133,  144,
 /*   400 */  1294,  288,  289,  306,  143, 1280,  149,  153,  142,  315,
 /*   410 */   221,  219,  217,   82,   65,  279,  146,  216,  160,  159,
 /*   420 */   158,  157,  915,  909,  911,   53,   73,  365,  408,  407,
 /*   430 */    65,  846,  284,  406,  903,  849,  906,  405, 1130,  404,
 /*   440 */   403, 1106, 1155, 1104, 1105, 1227,  226,  910, 1107,   65,
 /*   450 */   364,  830, 1108,   65, 1109, 1110, 1280,   83, 1283,  274,
 /*   460 */   265,  915,  909,  911,  318,  319, 1137,  282,   65,  287,
 /*   470 */    65,  285,   65,  377,  376,  378,  294,   13,  291,  230,
 /*   480 */   386,  385, 1137,   65,   65,  276,  910,  813,  290, 1280,
 /*   490 */     6, 1283,  912,  827,  379, 1226,  230,  249,  380,  250,
 /*   500 */   271, 1137,  152,  151,  150, 1137, 1280, 1280, 1283, 1280,
 /*   510 */   114,   93,  109,  381,  108,  387, 1223,  388,    1,  199,
 /*   520 */  1137,   10, 1137,  251, 1137,  414,  412,  675,  389,  393,
 /*   530 */   168,  166,  165, 1280, 1266, 1137, 1137, 1265, 1264,  834,
 /*   540 */   256,   92,  257,  293, 1280,  878,  232, 1280, 1280,  233,
 /*   550 */  1280,  266, 1280,  237,  197,  913, 1280,  231,  238, 1280,
 /*   560 */   239,  241,  242, 1280,  243,   98,  240, 1280, 1280,  227,
 /*   570 */  1280, 1280, 1280,  293, 1280, 1121, 1280,  293,  293, 1280,
 /*   580 */  1038,  110,   99,  307,  198, 1029,  855,  211,  367, 1138,
 /*   590 */     3,  212,  211,  317,  316,  865,   96,   66,  866,  364,
 /*   600 */   177,   85, 1222,  794,  309,  877,  370,  272,  340,   77,
 /*   610 */   796,  342,   54,  795,  949,  343,  335,   66,   60,  193,
 /*   620 */   916,  309,   66,  698,   77,  113,   89,   77,    9,  369,
 /*   630 */   819,   15,  820,   14,    9,  392,  132,    9,  131,   17,
 /*   640 */   817,   16,  818,   86,  383,  382,   19,  171,   18,  138,
 /*   650 */   303,  137,  173,   21,  174,   20, 1135, 1163,  783,   26,
 /*   660 */  1174, 1171, 1172, 1156,  310, 1176,  176,  181,  328,  194,
 /*   670 */  1206, 1205, 1204, 1203,  192, 1131, 1129,  195,  196, 1044,
 /*   680 */   345,  346,  347,  348,  351, 1307,  167,  352,   75,  224,
 /*   690 */    71,  410,  363, 1037,  845,  375, 1301,  129, 1300, 1297,
 /*   700 */   202,   27,  321,   87, 1153,  384,  267,  323,  325, 1293,
 /*   710 */   135, 1292, 1289,  203, 1064,   84,   72,   67,  337,   76,
 /*   720 */   225, 1025,  145,  183, 1023,  147,  148, 1021,   28, 1020,
 /*   730 */   333,  331, 1019,  186,  184,  260,  214,  215, 1016,  329,
 /*   740 */   182, 1015, 1014,  327, 1013, 1012, 1011,  324, 1010,  218,
 /*   750 */   220, 1002,  222,  999,  223,  995,  320,   94,  170,  350,
 /*   760 */    90,  308,  402, 1133,  140,   97,  394,  102,  326,  395,
 /*   770 */   396,  397,  398,  399,   88,  400,  278,  172,  344,  974,
 /*   780 */   296,  297,  973,  300,  299,  252,  253,  972,  955,  954,
 /*   790 */   123, 1042, 1041,  124,  309,  304,  339,   11,  100,  822,
 /*   800 */   311, 1018,   58, 1017, 1009,  210,  205, 1065,  206,  204,
 /*   810 */   207,  208,  161,  209,  162,    2,  163,  164, 1008,  103,
 /*   820 */  1102,  185,  338, 1066, 1001, 1000,  854,   59,  187,  188,
 /*   830 */   189,  191,    4,   80,  852,  848,  847, 1112,   81,  180,
 /*   840 */   856,  851,  178,  269,  867,  179,   69,  861,  104,  369,
 /*   850 */   863,  105,  330,  334,  111,   70,   12,   22,   23,   55,
 /*   860 */   341,   56,  114,  117,  116,   62,  120,  714,  749,  747,
 /*   870 */   746,  745,  743,  119,  742,  741,   63,  121,  738,  703,
 /*   880 */   125,  946,    7,  944,  361,  918,  947,  917,  945,  920,
 /*   890 */     8,  372,  373,   78,  128,  130,   66,   79,  134,  786,
 /*   900 */   136,  785,  782,  730,  728,  720,  726,  722,  724,  718,
 /*   910 */   816,  716,  815,  752,  751,  750,  748,  744,  740,  739,
 /*   920 */   213,  665,  979,  701,  674,  672,  978,  411,  978,  413,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   280,    1,  262,  209,  288,  209,  290,  291,    1,    9,
 /*    10 */   290,    1,  292,  209,   14,   15,    9,   17,   18,  209,
 /*    20 */   210,   21,   22,   23,   24,    5,  209,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,   34,   35,   36,   37,   38,   39,
 /*    40 */   255,  280,   42,   43,   44,   14,   15,  262,   17,   18,
 /*    50 */   254,  290,   21,   22,   23,   24,  285,  261,   27,   28,
 /*    60 */    29,   30,   31,   32,   33,   34,   35,   36,   37,   38,
 /*    70 */    39,  254,  280,   42,   43,   44,  282,  283,  261,  255,
 /*    80 */    14,   15,  290,   17,   18,  281,  262,   21,   22,   23,
 /*    90 */    24,   90,   92,   27,   28,   29,   30,   31,   32,   33,
 /*   100 */    34,   35,   36,   37,   38,   39,  280,  280,   42,   43,
 /*   110 */    44,   14,   15,   96,   17,   18,  290,  290,   21,   22,
 /*   120 */    23,   24,    0,   92,   27,   28,   29,   30,   31,   32,
 /*   130 */    33,   34,   35,   36,   37,   38,   39,  262,  280,   42,
 /*   140 */    43,   44,  255,   14,   15,    5,   17,   18,  290,  262,
 /*   150 */    21,   22,   23,   24,  209,   89,   27,   28,   29,   30,
 /*   160 */    31,   32,   33,   34,   35,   36,   37,   38,   39,  133,
 /*   170 */   134,   42,   43,   44,   15,  151,   17,   18,   38,   39,
 /*   180 */    21,   22,   23,   24,  160,  161,   27,   28,   29,   30,
 /*   190 */    31,   32,   33,   34,   35,   36,   37,   38,   39,    1,
 /*   200 */    83,   42,   43,   44,   17,   18,  261,    9,   21,   22,
 /*   210 */    23,   24,   90,   96,   27,   28,   29,   30,   31,   32,
 /*   220 */    33,   34,   35,   36,   37,   38,   39,   16,  267,   42,
 /*   230 */    43,   44,  104,  105,  106,  107,  108,  109,  110,  111,
 /*   240 */   112,  113,  114,  115,  116,  117,  118,  286,   50,   51,
 /*   250 */    52,   53,   54,   55,   56,   57,   58,   59,   60,   61,
 /*   260 */    62,   63,   64,   88,   66,  233,  234,  235,  236,  237,
 /*   270 */   238,  239,  240,  241,  242,  243,  244,  245,  246,  247,
 /*   280 */    49,   29,   30,   31,   32,   33,   34,   35,   36,   37,
 /*   290 */    38,   39,  217,    1,   42,   43,   44,   66,  280,    1,
 /*   300 */     2,    9,  127,    5,   73,    7,    1,    9,  290,  215,
 /*   310 */    79,   80,   81,   82,  259,  217,  222,  280,   87,   88,
 /*   320 */    33,   34,   35,   36,   37,   38,   39,  290,  253,   42,
 /*   330 */    43,   44,  277,  207,  208,  124,   38,   39,    1,    2,
 /*   340 */    42,  288,    5,  290,    7,   47,    9,   42,  250,  251,
 /*   350 */   252,  253,   67,   68,   69,   42,   43,   44,  127,   74,
 /*   360 */    75,   76,   77,   78,  280,    2,  287,  288,    5,  290,
 /*   370 */     7,    5,    9,    7,  290,   38,   39,  231,  232,   94,
 /*   380 */    88,  150,  216,  152,   47,  259,   88,   38,   39,   73,
 /*   390 */   159,   42,   43,   44,  163,  280,   73,   67,   68,   69,
 /*   400 */   262,   38,   39,  277,   74,  290,   76,   77,   78,  285,
 /*   410 */    67,   68,   69,  103,  209,  216,   86,   74,   75,   76,
 /*   420 */    77,   78,  124,  125,  126,   88,  104,   25,  106,  107,
 /*   430 */   209,    5,  216,  111,    5,    9,    7,  115,  209,  117,
 /*   440 */   118,  233,  259,  235,  236,  249,  280,  149,  240,  209,
 /*   450 */    48,   42,  244,  209,  246,  247,  290,  147,  292,  254,
 /*   460 */   277,  124,  125,  126,   38,   39,  261,   65,  209,  153,
 /*   470 */   209,  155,  209,  157,  158,  254,  153,   88,  155,  280,
 /*   480 */   157,  158,  261,  209,  209,  256,  149,  124,  125,  290,
 /*   490 */    88,  292,  126,  103,  254,  249,  280,  280,  254,  280,
 /*   500 */   249,  261,   83,   84,   85,  261,  290,  290,  292,  290,
 /*   510 */   121,  122,  288,  254,  290,  254,  249,  254,  218,  219,
 /*   520 */   261,  131,  261,  280,  261,   70,   71,   72,  254,  254,
 /*   530 */    67,   68,   69,  290,  280,  261,  261,  280,  280,  130,
 /*   540 */   280,  217,  280,  209,  290,   81,  280,  290,  290,  280,
 /*   550 */   290,  125,  290,  280,  220,  126,  290,  280,  280,  290,
 /*   560 */   280,  280,  280,  290,  280,   89,  280,  290,  290,  280,
 /*   570 */   290,  290,  290,  209,  290,  251,  290,  209,  209,  290,
 /*   580 */   215,  263,   89,   89,  220,  215,   89,  222,  220,  220,
 /*   590 */   213,  214,  222,   38,   39,   89,  278,  103,   89,   48,
 /*   600 */   103,  103,  249,   89,  128,  141,   25,  249,   89,  103,
 /*   610 */    89,   89,  103,   89,   89,   85,   65,  103,   88,  257,
 /*   620 */    89,  128,  103,   89,  103,  103,   88,  103,  103,   48,
 /*   630 */     5,  154,    7,  156,  103,  249,  154,  103,  156,  154,
 /*   640 */     5,  156,    7,  145,   38,   39,  154,  209,  156,  154,
 /*   650 */   209,  156,  209,  154,  209,  156,  209,  209,  120,  279,
 /*   660 */   209,  209,  209,  259,  259,  209,  209,  209,  209,  209,
 /*   670 */   289,  289,  289,  289,  264,  259,  209,  209,  209,  209,
 /*   680 */   209,  209,  209,  209,  209,  265,   65,  209,  209,  209,
 /*   690 */   209,   90,  209,  209,  126,  209,  209,  209,  209,  209,
 /*   700 */   209,  148,  284,  144,  276,  209,  284,  284,  284,  209,
 /*   710 */   209,  209,  209,  209,  209,  146,  209,  209,  139,  209,
 /*   720 */   209,  209,  209,  274,  209,  209,  209,  209,  143,  209,
 /*   730 */   142,  137,  209,  271,  273,  209,  209,  209,  209,  136,
 /*   740 */   275,  209,  209,  135,  209,  209,  209,  138,  209,  209,
 /*   750 */   209,  209,  209,  209,  209,  209,  132,  123,  211,   95,
 /*   760 */   212,  211,  119,  211,  102,  211,  101,  211,  211,   56,
 /*   770 */    98,  100,   60,   99,  211,   97,  211,  131,  211,    5,
 /*   780 */   162,    5,    5,    5,  162,  211,  211,    5,  106,  105,
 /*   790 */   217,  221,  221,  217,  128,  151,   85,   88,  129,   89,
 /*   800 */   103,  211,   88,  211,  211,  223,  228,  230,  224,  229,
 /*   810 */   227,  225,  212,  226,  212,  218,  212,  212,  211,  103,
 /*   820 */   248,  272,  258,  232,  211,  211,   89,  266,  270,  269,
 /*   830 */   268,  265,  213,  103,  126,    5,    5,  248,   88,  103,
 /*   840 */    89,  126,   88,    1,   89,   88,  103,   89,   88,   48,
 /*   850 */    89,   88,   88,    1,   92,  103,   88,  140,  140,   88,
 /*   860 */    85,   88,  121,   83,   85,   93,   75,    5,    9,    5,
 /*   870 */     5,    5,    5,   92,    5,    5,   93,   92,    5,   91,
 /*   880 */    83,    9,   88,    9,   16,   89,    9,   89,    9,  124,
 /*   890 */    88,   28,   64,   17,  156,  156,  103,   17,  156,    5,
 /*   900 */   156,    5,   89,    5,    5,    5,    5,    5,    5,    5,
 /*   910 */   126,    5,  126,    5,    5,    5,    5,    5,    5,    5,
 /*   920 */   103,   65,    0,   91,    9,    9,  293,   22,  293,   22,
 /*   930 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
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
 /*  1130 */   293,  293,  293,  293,  293,  293,
};
#define YY_SHIFT_COUNT    (416)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (922)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   231,  128,  128,  322,  322,    1,  298,  337,  337,  337,
 /*    10 */   292,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*    20 */     7,    7,   10,   10,    0,  198,  337,  337,  337,  337,
 /*    30 */   337,  337,  337,  337,  337,  337,  337,  337,  337,  337,
 /*    40 */   337,  337,  337,  337,  337,  337,  337,  337,  337,  337,
 /*    50 */   337,  337,  337,  337,  363,  363,  363,  175,  175,   36,
 /*    60 */     7,  122,    7,    7,    7,    7,    7,  117,    1,   10,
 /*    70 */    10,   17,   17,   20,  930,  930,  930,  363,  363,  363,
 /*    80 */   426,  426,  140,  140,  140,  140,  140,  140,  389,  140,
 /*    90 */     7,    7,    7,    7,    7,    7,  409,    7,    7,    7,
 /*   100 */   175,  175,    7,    7,    7,    7,  464,  464,  464,  464,
 /*   110 */   390,  175,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   120 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   130 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   140 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   150 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   160 */     7,    7,    7,    7,    7,    7,    7,    7,    7,    7,
 /*   170 */   553,  621,  601,  621,  621,  621,  621,  568,  568,  568,
 /*   180 */   568,  621,  559,  569,  579,  585,  588,  594,  603,  608,
 /*   190 */   609,  624,  553,  634,  621,  621,  621,  664,  664,  643,
 /*   200 */     1,    1,  621,  621,  662,  665,  713,  672,  671,  712,
 /*   210 */   674,  678,  643,   20,  621,  621,  601,  601,  621,  601,
 /*   220 */   621,  601,  621,  621,  930,  930,   31,   66,   97,   97,
 /*   230 */    97,  129,  159,  187,  252,  252,  252,  252,  252,  252,
 /*   240 */   287,  287,  287,  287,  285,  330,  343,  349,  349,  349,
 /*   250 */   349,  349,  316,  323,  402,   24,  313,  313,  366,  429,
 /*   260 */   419,  455,  463,  494,  476,  493,  555,  497,  506,  509,
 /*   270 */   551,  310,  498,  514,  519,  521,  522,  524,  530,  525,
 /*   280 */   531,  581,  305,  211,  534,  477,  482,  485,  625,  635,
 /*   290 */   606,  492,  495,  538,  499,  646,  774,  618,  776,  777,
 /*   300 */   622,  778,  782,  682,  684,  644,  666,  711,  709,  669,
 /*   310 */   710,  714,  697,  716,  737,  730,  708,  715,  830,  831,
 /*   320 */   750,  751,  754,  755,  757,  758,  736,  760,  761,  763,
 /*   330 */   842,  764,  743,  717,  801,  852,  752,  718,  762,  768,
 /*   340 */   711,  771,  775,  773,  741,  779,  780,  772,  781,  791,
 /*   350 */   862,  783,  785,  859,  864,  865,  866,  867,  869,  870,
 /*   360 */   873,  788,  868,  797,  872,  874,  794,  796,  798,  877,
 /*   370 */   879,  765,  802,  863,  828,  876,  738,  739,  793,  793,
 /*   380 */   793,  793,  784,  786,  880,  742,  744,  793,  793,  793,
 /*   390 */   894,  896,  813,  793,  898,  899,  900,  901,  902,  903,
 /*   400 */   904,  906,  908,  909,  910,  911,  912,  913,  914,  817,
 /*   410 */   832,  915,  905,  916,  907,  856,  922,
};
#define YY_REDUCE_COUNT (225)
#define YY_REDUCE_MIN   (-284)
#define YY_REDUCE_MAX   (619)
static const short yy_reduce_ofst[] = {
 /*     0 */   126,   32,   32,  208,  208,   98,  166,  199,  216, -280,
 /*    10 */  -206, -204, -183,  205,  221,  240,  244,  259,  261,  263,
 /*    20 */   274,  275, -284,   79, -196, -190, -239, -208, -174, -173,
 /*    30 */  -142,   18,   37,   84,  115,  217,  219,  243,  254,  257,
 /*    40 */   258,  260,  262,  266,  269,  273,  277,  278,  280,  281,
 /*    50 */   282,  284,  286,  289, -215, -176, -113,   55,  183,  -39,
 /*    60 */   229,   75,  334,  364,  368,  369,  -55,   94,  324,   53,
 /*    70 */   224,  365,  370,  146,  318,  300,  377, -260, -125,  138,
 /*    80 */  -229,  124,  196,  246,  251,  267,  353,  358,  362,  386,
 /*    90 */   438,  441,  443,  445,  447,  448,  380,  451,  452,  453,
 /*   100 */   404,  405,  456,  457,  458,  459,  381,  382,  383,  384,
 /*   110 */   410,  416,  460,  467,  468,  469,  470,  471,  472,  473,
 /*   120 */   474,  475,  478,  479,  480,  481,  483,  484,  486,  487,
 /*   130 */   488,  489,  490,  491,  496,  500,  501,  502,  503,  504,
 /*   140 */   505,  507,  508,  510,  511,  512,  513,  515,  516,  517,
 /*   150 */   518,  520,  523,  526,  527,  528,  529,  532,  533,  535,
 /*   160 */   536,  537,  539,  540,  541,  542,  543,  544,  545,  546,
 /*   170 */   420,  547,  548,  550,  552,  554,  556,  418,  422,  423,
 /*   180 */   424,  557,  428,  465,  449,  461,  549,  462,  558,  560,
 /*   190 */   562,  561,  566,  564,  563,  565,  567,  570,  571,  572,
 /*   200 */   573,  576,  574,  575,  577,  580,  578,  584,  583,  586,
 /*   210 */   587,  582,  589,  591,  590,  592,  600,  602,  593,  604,
 /*   220 */   607,  605,  613,  614,  597,  619,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   976, 1101, 1039, 1111, 1026, 1036, 1285, 1285, 1285, 1285,
 /*    10 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*    20 */   976,  976,  976,  976, 1165,  996,  976,  976,  976,  976,
 /*    30 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*    40 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*    50 */   976,  976,  976,  976,  976,  976,  976,  976,  976, 1189,
 /*    60 */   976, 1036,  976,  976,  976,  976,  976, 1047, 1036,  976,
 /*    70 */   976, 1047, 1047,  976, 1160, 1085, 1103,  976,  976,  976,
 /*    80 */   976,  976,  976,  976,  976,  976,  976,  976, 1132,  976,
 /*    90 */   976,  976,  976,  976,  976,  976, 1167, 1173, 1170,  976,
 /*   100 */   976,  976, 1175,  976,  976,  976, 1211, 1211, 1211, 1211,
 /*   110 */  1158,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   120 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   130 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   140 */   976,  976,  976,  976,  976, 1024,  976, 1022,  976,  976,
 /*   150 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   160 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  994,
 /*   170 */  1228,  998, 1034,  998,  998,  998,  998,  976,  976,  976,
 /*   180 */   976,  998, 1220, 1224, 1201, 1218, 1212, 1196, 1194, 1192,
 /*   190 */  1200, 1185, 1228, 1134,  998,  998,  998, 1045, 1045, 1040,
 /*   200 */  1036, 1036,  998,  998, 1063, 1061, 1059, 1051, 1057, 1053,
 /*   210 */  1055, 1049, 1027,  976,  998,  998, 1034, 1034,  998, 1034,
 /*   220 */   998, 1034,  998,  998, 1085, 1103, 1284,  976, 1229, 1219,
 /*   230 */  1284,  976, 1261, 1260, 1275, 1274, 1273, 1259, 1258, 1257,
 /*   240 */  1253, 1256, 1255, 1254,  976,  976,  976, 1272, 1271, 1269,
 /*   250 */  1268, 1267,  976,  976, 1231,  976, 1263, 1262,  976,  976,
 /*   260 */   976,  976,  976,  976,  976,  976, 1182,  976,  976,  976,
 /*   270 */  1207, 1225, 1221,  976,  976,  976,  976,  976,  976,  976,
 /*   280 */   976, 1232,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   290 */  1146,  976,  976, 1113,  976,  976,  976,  976,  976,  976,
 /*   300 */   976,  976,  976,  976,  976,  976, 1157,  976,  976,  976,
 /*   310 */   976,  976, 1169, 1168,  976,  976,  976,  976,  976,  976,
 /*   320 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   330 */   976,  976, 1213,  976, 1208,  976, 1202,  976,  976,  976,
 /*   340 */  1125,  976,  976,  976,  976, 1043,  976,  976,  976,  976,
 /*   350 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   360 */   976,  976,  976,  976,  976,  976,  976,  976,  976,  976,
 /*   370 */   976,  976,  976,  976,  976,  976,  976,  976, 1303, 1298,
 /*   380 */  1299, 1296,  976,  976,  976,  976,  976, 1295, 1290, 1291,
 /*   390 */   976,  976,  976, 1288,  976,  976,  976,  976,  976,  976,
 /*   400 */   976,  976,  976,  976,  976,  976,  976,  976,  976, 1069,
 /*   410 */   976,  976, 1005,  976, 1003,  976,  976,
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
 /* 289 */ "expr ::= expr BITOR expr",
 /* 290 */ "expr ::= expr BITXOR expr",
 /* 291 */ "expr ::= BITNOT expr",
 /* 292 */ "expr ::= expr LSHIFT expr",
 /* 293 */ "expr ::= expr RSHIFT expr",
 /* 294 */ "expr ::= expr LIKE expr",
 /* 295 */ "expr ::= expr MATCH expr",
 /* 296 */ "expr ::= expr NMATCH expr",
 /* 297 */ "expr ::= ID CONTAINS STRING",
 /* 298 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 299 */ "arrow ::= ID ARROW STRING",
 /* 300 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 301 */ "expr ::= arrow",
 /* 302 */ "expr ::= expr IN LP exprlist RP",
 /* 303 */ "exprlist ::= exprlist COMMA expritem",
 /* 304 */ "exprlist ::= expritem",
 /* 305 */ "expritem ::= expr",
 /* 306 */ "expritem ::=",
 /* 307 */ "cmd ::= RESET QUERY CACHE",
 /* 308 */ "cmd ::= SYNCDB ids REPLICA",
 /* 309 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 310 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 311 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 312 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 313 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 314 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 315 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 316 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 317 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 318 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 319 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 320 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 321 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 322 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 323 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 324 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 325 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 326 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 327 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
 /* 328 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
  {  208,   -5 }, /* (42) cmd ::= ALTER USER ids TAGS ids */
  {  208,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  208,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  208,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  208,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  208,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  208,   -4 }, /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  208,   -4 }, /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  208,   -6 }, /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  208,   -6 }, /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  209,   -1 }, /* (52) ids ::= ID */
  {  209,   -1 }, /* (53) ids ::= STRING */
  {  212,   -2 }, /* (54) ifexists ::= IF EXISTS */
  {  212,    0 }, /* (55) ifexists ::= */
  {  217,   -3 }, /* (56) ifnotexists ::= IF NOT EXISTS */
  {  217,    0 }, /* (57) ifnotexists ::= */
  {  208,   -3 }, /* (58) cmd ::= CREATE DNODE ids */
  {  208,   -6 }, /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  208,   -5 }, /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  208,   -5 }, /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  208,   -8 }, /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  208,   -9 }, /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  208,   -5 }, /* (64) cmd ::= CREATE USER ids PASS ids */
  {  208,   -7 }, /* (65) cmd ::= CREATE USER ids PASS ids TAGS ids */
  {  221,    0 }, /* (66) bufsize ::= */
  {  221,   -2 }, /* (67) bufsize ::= BUFSIZE INTEGER */
  {  222,    0 }, /* (68) pps ::= */
  {  222,   -2 }, /* (69) pps ::= PPS INTEGER */
  {  223,    0 }, /* (70) tseries ::= */
  {  223,   -2 }, /* (71) tseries ::= TSERIES INTEGER */
  {  224,    0 }, /* (72) dbs ::= */
  {  224,   -2 }, /* (73) dbs ::= DBS INTEGER */
  {  225,    0 }, /* (74) streams ::= */
  {  225,   -2 }, /* (75) streams ::= STREAMS INTEGER */
  {  226,    0 }, /* (76) storage ::= */
  {  226,   -2 }, /* (77) storage ::= STORAGE INTEGER */
  {  227,    0 }, /* (78) qtime ::= */
  {  227,   -2 }, /* (79) qtime ::= QTIME INTEGER */
  {  228,    0 }, /* (80) users ::= */
  {  228,   -2 }, /* (81) users ::= USERS INTEGER */
  {  229,    0 }, /* (82) conns ::= */
  {  229,   -2 }, /* (83) conns ::= CONNS INTEGER */
  {  230,    0 }, /* (84) state ::= */
  {  230,   -2 }, /* (85) state ::= STATE ids */
  {  215,   -9 }, /* (86) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  231,   -3 }, /* (87) intitemlist ::= intitemlist COMMA intitem */
  {  231,   -1 }, /* (88) intitemlist ::= intitem */
  {  232,   -1 }, /* (89) intitem ::= INTEGER */
  {  233,   -2 }, /* (90) keep ::= KEEP intitemlist */
  {  234,   -2 }, /* (91) cache ::= CACHE INTEGER */
  {  235,   -2 }, /* (92) replica ::= REPLICA INTEGER */
  {  236,   -2 }, /* (93) quorum ::= QUORUM INTEGER */
  {  237,   -2 }, /* (94) days ::= DAYS INTEGER */
  {  238,   -2 }, /* (95) minrows ::= MINROWS INTEGER */
  {  239,   -2 }, /* (96) maxrows ::= MAXROWS INTEGER */
  {  240,   -2 }, /* (97) blocks ::= BLOCKS INTEGER */
  {  241,   -2 }, /* (98) ctime ::= CTIME INTEGER */
  {  242,   -2 }, /* (99) wal ::= WAL INTEGER */
  {  243,   -2 }, /* (100) fsync ::= FSYNC INTEGER */
  {  244,   -2 }, /* (101) comp ::= COMP INTEGER */
  {  245,   -2 }, /* (102) prec ::= PRECISION STRING */
  {  246,   -2 }, /* (103) update ::= UPDATE INTEGER */
  {  247,   -2 }, /* (104) cachelast ::= CACHELAST INTEGER */
  {  248,   -2 }, /* (105) partitions ::= PARTITIONS INTEGER */
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
  {  249,   -1 }, /* (137) signed ::= INTEGER */
  {  249,   -2 }, /* (138) signed ::= PLUS INTEGER */
  {  249,   -2 }, /* (139) signed ::= MINUS INTEGER */
  {  208,   -3 }, /* (140) cmd ::= CREATE TABLE create_table_args */
  {  208,   -3 }, /* (141) cmd ::= CREATE TABLE create_stable_args */
  {  208,   -3 }, /* (142) cmd ::= CREATE STABLE create_stable_args */
  {  208,   -3 }, /* (143) cmd ::= CREATE TABLE create_table_list */
  {  252,   -1 }, /* (144) create_table_list ::= create_from_stable */
  {  252,   -2 }, /* (145) create_table_list ::= create_table_list create_from_stable */
  {  250,   -6 }, /* (146) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  251,  -10 }, /* (147) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  253,  -10 }, /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  253,  -13 }, /* (149) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  256,   -3 }, /* (150) tagNamelist ::= tagNamelist COMMA ids */
  {  256,   -1 }, /* (151) tagNamelist ::= ids */
  {  250,   -7 }, /* (152) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  257,    0 }, /* (153) to_opt ::= */
  {  257,   -3 }, /* (154) to_opt ::= TO ids cpxName */
  {  258,    0 }, /* (155) split_opt ::= */
  {  258,   -2 }, /* (156) split_opt ::= SPLIT ids */
  {  254,   -3 }, /* (157) columnlist ::= columnlist COMMA column */
  {  254,   -1 }, /* (158) columnlist ::= column */
  {  261,   -2 }, /* (159) column ::= ids typename */
  {  255,   -3 }, /* (160) tagitemlist ::= tagitemlist COMMA tagitem */
  {  255,   -1 }, /* (161) tagitemlist ::= tagitem */
  {  262,   -1 }, /* (162) tagitem ::= INTEGER */
  {  262,   -1 }, /* (163) tagitem ::= FLOAT */
  {  262,   -1 }, /* (164) tagitem ::= STRING */
  {  262,   -1 }, /* (165) tagitem ::= BOOL */
  {  262,   -1 }, /* (166) tagitem ::= NULL */
  {  262,   -1 }, /* (167) tagitem ::= NOW */
  {  262,   -3 }, /* (168) tagitem ::= NOW PLUS VARIABLE */
  {  262,   -3 }, /* (169) tagitem ::= NOW MINUS VARIABLE */
  {  262,   -2 }, /* (170) tagitem ::= MINUS INTEGER */
  {  262,   -2 }, /* (171) tagitem ::= MINUS FLOAT */
  {  262,   -2 }, /* (172) tagitem ::= PLUS INTEGER */
  {  262,   -2 }, /* (173) tagitem ::= PLUS FLOAT */
  {  259,  -15 }, /* (174) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  259,   -3 }, /* (175) select ::= LP select RP */
  {  277,   -1 }, /* (176) union ::= select */
  {  277,   -4 }, /* (177) union ::= union UNION ALL select */
  {  208,   -1 }, /* (178) cmd ::= union */
  {  259,   -2 }, /* (179) select ::= SELECT selcollist */
  {  278,   -2 }, /* (180) sclp ::= selcollist COMMA */
  {  278,    0 }, /* (181) sclp ::= */
  {  263,   -4 }, /* (182) selcollist ::= sclp distinct expr as */
  {  263,   -2 }, /* (183) selcollist ::= sclp STAR */
  {  281,   -2 }, /* (184) as ::= AS ids */
  {  281,   -1 }, /* (185) as ::= ids */
  {  281,    0 }, /* (186) as ::= */
  {  279,   -1 }, /* (187) distinct ::= DISTINCT */
  {  279,    0 }, /* (188) distinct ::= */
  {  264,   -2 }, /* (189) from ::= FROM tablelist */
  {  264,   -2 }, /* (190) from ::= FROM sub */
  {  283,   -3 }, /* (191) sub ::= LP union RP */
  {  283,   -4 }, /* (192) sub ::= LP union RP ids */
  {  283,   -6 }, /* (193) sub ::= sub COMMA LP union RP ids */
  {  282,   -2 }, /* (194) tablelist ::= ids cpxName */
  {  282,   -3 }, /* (195) tablelist ::= ids cpxName ids */
  {  282,   -4 }, /* (196) tablelist ::= tablelist COMMA ids cpxName */
  {  282,   -5 }, /* (197) tablelist ::= tablelist COMMA ids cpxName ids */
  {  284,   -1 }, /* (198) tmvar ::= VARIABLE */
  {  285,   -1 }, /* (199) timestamp ::= INTEGER */
  {  285,   -2 }, /* (200) timestamp ::= MINUS INTEGER */
  {  285,   -2 }, /* (201) timestamp ::= PLUS INTEGER */
  {  285,   -1 }, /* (202) timestamp ::= STRING */
  {  285,   -1 }, /* (203) timestamp ::= NOW */
  {  285,   -3 }, /* (204) timestamp ::= NOW PLUS VARIABLE */
  {  285,   -3 }, /* (205) timestamp ::= NOW MINUS VARIABLE */
  {  266,    0 }, /* (206) range_option ::= */
  {  266,   -6 }, /* (207) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  267,   -4 }, /* (208) interval_option ::= intervalKey LP tmvar RP */
  {  267,   -6 }, /* (209) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  267,    0 }, /* (210) interval_option ::= */
  {  286,   -1 }, /* (211) intervalKey ::= INTERVAL */
  {  286,   -1 }, /* (212) intervalKey ::= EVERY */
  {  269,    0 }, /* (213) session_option ::= */
  {  269,   -7 }, /* (214) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  270,    0 }, /* (215) windowstate_option ::= */
  {  270,   -4 }, /* (216) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  271,    0 }, /* (217) fill_opt ::= */
  {  271,   -6 }, /* (218) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  271,   -4 }, /* (219) fill_opt ::= FILL LP ID RP */
  {  268,   -4 }, /* (220) sliding_opt ::= SLIDING LP tmvar RP */
  {  268,    0 }, /* (221) sliding_opt ::= */
  {  274,    0 }, /* (222) orderby_opt ::= */
  {  274,   -3 }, /* (223) orderby_opt ::= ORDER BY sortlist */
  {  287,   -4 }, /* (224) sortlist ::= sortlist COMMA item sortorder */
  {  287,   -4 }, /* (225) sortlist ::= sortlist COMMA arrow sortorder */
  {  287,   -2 }, /* (226) sortlist ::= item sortorder */
  {  287,   -2 }, /* (227) sortlist ::= arrow sortorder */
  {  288,   -1 }, /* (228) item ::= ID */
  {  288,   -3 }, /* (229) item ::= ID DOT ID */
  {  289,   -1 }, /* (230) sortorder ::= ASC */
  {  289,   -1 }, /* (231) sortorder ::= DESC */
  {  289,    0 }, /* (232) sortorder ::= */
  {  272,    0 }, /* (233) groupby_opt ::= */
  {  272,   -3 }, /* (234) groupby_opt ::= GROUP BY grouplist */
  {  291,   -3 }, /* (235) grouplist ::= grouplist COMMA item */
  {  291,   -3 }, /* (236) grouplist ::= grouplist COMMA arrow */
  {  291,   -1 }, /* (237) grouplist ::= item */
  {  291,   -1 }, /* (238) grouplist ::= arrow */
  {  273,    0 }, /* (239) having_opt ::= */
  {  273,   -2 }, /* (240) having_opt ::= HAVING expr */
  {  276,    0 }, /* (241) limit_opt ::= */
  {  276,   -2 }, /* (242) limit_opt ::= LIMIT signed */
  {  276,   -4 }, /* (243) limit_opt ::= LIMIT signed OFFSET signed */
  {  276,   -4 }, /* (244) limit_opt ::= LIMIT signed COMMA signed */
  {  275,    0 }, /* (245) slimit_opt ::= */
  {  275,   -2 }, /* (246) slimit_opt ::= SLIMIT signed */
  {  275,   -4 }, /* (247) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  275,   -4 }, /* (248) slimit_opt ::= SLIMIT signed COMMA signed */
  {  265,    0 }, /* (249) where_opt ::= */
  {  265,   -2 }, /* (250) where_opt ::= WHERE expr */
  {  280,   -3 }, /* (251) expr ::= LP expr RP */
  {  280,   -1 }, /* (252) expr ::= ID */
  {  280,   -3 }, /* (253) expr ::= ID DOT ID */
  {  280,   -3 }, /* (254) expr ::= ID DOT STAR */
  {  280,   -1 }, /* (255) expr ::= INTEGER */
  {  280,   -2 }, /* (256) expr ::= MINUS INTEGER */
  {  280,   -2 }, /* (257) expr ::= PLUS INTEGER */
  {  280,   -1 }, /* (258) expr ::= FLOAT */
  {  280,   -2 }, /* (259) expr ::= MINUS FLOAT */
  {  280,   -2 }, /* (260) expr ::= PLUS FLOAT */
  {  280,   -1 }, /* (261) expr ::= STRING */
  {  280,   -1 }, /* (262) expr ::= NOW */
  {  280,   -1 }, /* (263) expr ::= TODAY */
  {  280,   -1 }, /* (264) expr ::= VARIABLE */
  {  280,   -2 }, /* (265) expr ::= PLUS VARIABLE */
  {  280,   -2 }, /* (266) expr ::= MINUS VARIABLE */
  {  280,   -1 }, /* (267) expr ::= BOOL */
  {  280,   -1 }, /* (268) expr ::= NULL */
  {  280,   -4 }, /* (269) expr ::= ID LP exprlist RP */
  {  280,   -4 }, /* (270) expr ::= ID LP STAR RP */
  {  280,   -6 }, /* (271) expr ::= ID LP expr AS typename RP */
  {  280,   -3 }, /* (272) expr ::= expr IS NULL */
  {  280,   -4 }, /* (273) expr ::= expr IS NOT NULL */
  {  280,   -3 }, /* (274) expr ::= expr LT expr */
  {  280,   -3 }, /* (275) expr ::= expr GT expr */
  {  280,   -3 }, /* (276) expr ::= expr LE expr */
  {  280,   -3 }, /* (277) expr ::= expr GE expr */
  {  280,   -3 }, /* (278) expr ::= expr NE expr */
  {  280,   -3 }, /* (279) expr ::= expr EQ expr */
  {  280,   -5 }, /* (280) expr ::= expr BETWEEN expr AND expr */
  {  280,   -3 }, /* (281) expr ::= expr AND expr */
  {  280,   -3 }, /* (282) expr ::= expr OR expr */
  {  280,   -3 }, /* (283) expr ::= expr PLUS expr */
  {  280,   -3 }, /* (284) expr ::= expr MINUS expr */
  {  280,   -3 }, /* (285) expr ::= expr STAR expr */
  {  280,   -3 }, /* (286) expr ::= expr SLASH expr */
  {  280,   -3 }, /* (287) expr ::= expr REM expr */
  {  280,   -3 }, /* (288) expr ::= expr BITAND expr */
  {  280,   -3 }, /* (289) expr ::= expr BITOR expr */
  {  280,   -3 }, /* (290) expr ::= expr BITXOR expr */
  {  280,   -2 }, /* (291) expr ::= BITNOT expr */
  {  280,   -3 }, /* (292) expr ::= expr LSHIFT expr */
  {  280,   -3 }, /* (293) expr ::= expr RSHIFT expr */
  {  280,   -3 }, /* (294) expr ::= expr LIKE expr */
  {  280,   -3 }, /* (295) expr ::= expr MATCH expr */
  {  280,   -3 }, /* (296) expr ::= expr NMATCH expr */
  {  280,   -3 }, /* (297) expr ::= ID CONTAINS STRING */
  {  280,   -5 }, /* (298) expr ::= ID DOT ID CONTAINS STRING */
  {  290,   -3 }, /* (299) arrow ::= ID ARROW STRING */
  {  290,   -5 }, /* (300) arrow ::= ID DOT ID ARROW STRING */
  {  280,   -1 }, /* (301) expr ::= arrow */
  {  280,   -5 }, /* (302) expr ::= expr IN LP exprlist RP */
  {  216,   -3 }, /* (303) exprlist ::= exprlist COMMA expritem */
  {  216,   -1 }, /* (304) exprlist ::= expritem */
  {  292,   -1 }, /* (305) expritem ::= expr */
  {  292,    0 }, /* (306) expritem ::= */
  {  208,   -3 }, /* (307) cmd ::= RESET QUERY CACHE */
  {  208,   -3 }, /* (308) cmd ::= SYNCDB ids REPLICA */
  {  208,   -7 }, /* (309) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (310) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (311) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (312) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (313) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (314) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (315) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (316) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -7 }, /* (317) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (318) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (319) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  208,   -7 }, /* (320) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (321) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (322) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (323) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -7 }, /* (324) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  208,   -3 }, /* (325) cmd ::= KILL CONNECTION INTEGER */
  {  208,   -5 }, /* (326) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  208,   -5 }, /* (327) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  208,   -6 }, /* (328) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy564, &t);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy563);}
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy563);}
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy367);}
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
      case 188: /* distinct ::= */ yytestcase(yyruleno==188);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 58: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy563);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy564, &yymsp[-2].minor.yy0);}
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy307, &yymsp[0].minor.yy0, 1);}
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy307, &yymsp[0].minor.yy0, 2);}
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
      case 87: /* intitemlist ::= intitemlist COMMA intitem */
      case 160: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==160);
{ yylhsminor.yy367 = tVariantListAppend(yymsp[-2].minor.yy367, &yymsp[0].minor.yy410, -1);    }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 88: /* intitemlist ::= intitem */
      case 161: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==161);
{ yylhsminor.yy367 = tVariantListAppend(NULL, &yymsp[0].minor.yy410, -1); }
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 89: /* intitem ::= INTEGER */
      case 162: /* tagitem ::= INTEGER */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= STRING */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= BOOL */ yytestcase(yyruleno==165);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 90: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy367 = yymsp[0].minor.yy367; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy564); yymsp[1].minor.yy564.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 107: /* db_optr ::= db_optr cache */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 108: /* db_optr ::= db_optr replica */
      case 125: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==125);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 109: /* db_optr ::= db_optr quorum */
      case 126: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==126);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 110: /* db_optr ::= db_optr days */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 111: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 112: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 113: /* db_optr ::= db_optr blocks */
      case 128: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==128);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 114: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 115: /* db_optr ::= db_optr wal */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 116: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 117: /* db_optr ::= db_optr comp */
      case 129: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==129);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 118: /* db_optr ::= db_optr prec */
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 119: /* db_optr ::= db_optr keep */
      case 127: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==127);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.keep = yymsp[0].minor.yy367; }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 120: /* db_optr ::= db_optr update */
      case 130: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==130);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 121: /* db_optr ::= db_optr cachelast */
      case 131: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==131);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 122: /* topic_optr ::= db_optr */
      case 132: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==132);
{ yylhsminor.yy564 = yymsp[0].minor.yy564; yylhsminor.yy564.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy564 = yylhsminor.yy564;
        break;
      case 123: /* topic_optr ::= topic_optr partitions */
      case 133: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==133);
{ yylhsminor.yy564 = yymsp[-1].minor.yy564; yylhsminor.yy564.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 124: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy564); yymsp[1].minor.yy564.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 134: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy307, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy307 = yylhsminor.yy307;
        break;
      case 135: /* typename ::= ids LP signed RP */
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
      case 136: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy307, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy307 = yylhsminor.yy307;
        break;
      case 137: /* signed ::= INTEGER */
{ yylhsminor.yy443 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy443 = yylhsminor.yy443;
        break;
      case 138: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy443 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 139: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy443 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 143: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy74;}
        break;
      case 144: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy110);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy74 = pCreateTable;
}
  yymsp[0].minor.yy74 = yylhsminor.yy74;
        break;
      case 145: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy74->childTableInfo, &yymsp[0].minor.yy110);
  yylhsminor.yy74 = yymsp[-1].minor.yy74;
}
  yymsp[-1].minor.yy74 = yylhsminor.yy74;
        break;
      case 146: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy74 = tSetCreateTableInfo(yymsp[-1].minor.yy367, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy74 = yylhsminor.yy74;
        break;
      case 147: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy74 = tSetCreateTableInfo(yymsp[-5].minor.yy367, yymsp[-1].minor.yy367, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy74 = yylhsminor.yy74;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy110 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy367, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy110 = yylhsminor.yy110;
        break;
      case 149: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy110 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy367, yymsp[-1].minor.yy367, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy110 = yylhsminor.yy110;
        break;
      case 150: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy367, &yymsp[0].minor.yy0); yylhsminor.yy367 = yymsp[-2].minor.yy367;  }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 151: /* tagNamelist ::= ids */
{yylhsminor.yy367 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy367, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 152: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy74 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy426, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy74, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy74 = yylhsminor.yy74;
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
{taosArrayPush(yymsp[-2].minor.yy367, &yymsp[0].minor.yy307); yylhsminor.yy367 = yymsp[-2].minor.yy367;  }
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 158: /* columnlist ::= column */
{yylhsminor.yy367 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy367, &yymsp[0].minor.yy307);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 159: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy307, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy307);
}
  yymsp[-1].minor.yy307 = yylhsminor.yy307;
        break;
      case 166: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 167: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy410, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 168: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy410, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 169: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy410, &yymsp[0].minor.yy0, TK_MINUS, true);
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
    tVariantCreate(&yylhsminor.yy410, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy410 = yylhsminor.yy410;
        break;
      case 174: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy426 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy367, yymsp[-12].minor.yy480, yymsp[-11].minor.yy378, yymsp[-4].minor.yy367, yymsp[-2].minor.yy367, &yymsp[-9].minor.yy478, &yymsp[-7].minor.yy373, &yymsp[-6].minor.yy204, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy367, &yymsp[0].minor.yy24, &yymsp[-1].minor.yy24, yymsp[-3].minor.yy378, &yymsp[-10].minor.yy214);
}
  yymsp[-14].minor.yy426 = yylhsminor.yy426;
        break;
      case 175: /* select ::= LP select RP */
{yymsp[-2].minor.yy426 = yymsp[-1].minor.yy426;}
        break;
      case 176: /* union ::= select */
{ yylhsminor.yy367 = setSubclause(NULL, yymsp[0].minor.yy426); }
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 177: /* union ::= union UNION ALL select */
{ yylhsminor.yy367 = appendSelectClause(yymsp[-3].minor.yy367, yymsp[0].minor.yy426); }
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 178: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy367, NULL, TSDB_SQL_SELECT); }
        break;
      case 179: /* select ::= SELECT selcollist */
{
  yylhsminor.yy426 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy367, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy426 = yylhsminor.yy426;
        break;
      case 180: /* sclp ::= selcollist COMMA */
{yylhsminor.yy367 = yymsp[-1].minor.yy367;}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 181: /* sclp ::= */
      case 222: /* orderby_opt ::= */ yytestcase(yyruleno==222);
{yymsp[1].minor.yy367 = 0;}
        break;
      case 182: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy367 = tSqlExprListAppend(yymsp[-3].minor.yy367, yymsp[-1].minor.yy378,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 183: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy367 = tSqlExprListAppend(yymsp[-1].minor.yy367, pNode, 0, 0);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
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
{yymsp[-1].minor.yy480 = yymsp[0].minor.yy480;}
        break;
      case 191: /* sub ::= LP union RP */
{yymsp[-2].minor.yy480 = addSubqueryElem(NULL, yymsp[-1].minor.yy367, NULL);}
        break;
      case 192: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy480 = addSubqueryElem(NULL, yymsp[-2].minor.yy367, &yymsp[0].minor.yy0);}
        break;
      case 193: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy480 = addSubqueryElem(yymsp[-5].minor.yy480, yymsp[-2].minor.yy367, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy480 = yylhsminor.yy480;
        break;
      case 194: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 195: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 196: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 197: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy480 = setTableNameList(yymsp[-4].minor.yy480, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy480 = yylhsminor.yy480;
        break;
      case 198: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 199: /* timestamp ::= INTEGER */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 200: /* timestamp ::= MINUS INTEGER */
      case 201: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==201);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 202: /* timestamp ::= STRING */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 203: /* timestamp ::= NOW */
{ yylhsminor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 204: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 205: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy378 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 206: /* range_option ::= */
{yymsp[1].minor.yy214.start = 0; yymsp[1].minor.yy214.end = 0;}
        break;
      case 207: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy214.start = yymsp[-3].minor.yy378; yymsp[-5].minor.yy214.end = yymsp[-1].minor.yy378;}
        break;
      case 208: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy478.interval = yymsp[-1].minor.yy0; yylhsminor.yy478.offset.n = 0; yylhsminor.yy478.token = yymsp[-3].minor.yy586;}
  yymsp[-3].minor.yy478 = yylhsminor.yy478;
        break;
      case 209: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy478.interval = yymsp[-3].minor.yy0; yylhsminor.yy478.offset = yymsp[-1].minor.yy0;   yylhsminor.yy478.token = yymsp[-5].minor.yy586;}
  yymsp[-5].minor.yy478 = yylhsminor.yy478;
        break;
      case 210: /* interval_option ::= */
{memset(&yymsp[1].minor.yy478, 0, sizeof(yymsp[1].minor.yy478));}
        break;
      case 211: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy586 = TK_INTERVAL;}
        break;
      case 212: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy586 = TK_EVERY;   }
        break;
      case 213: /* session_option ::= */
{yymsp[1].minor.yy373.col.n = 0; yymsp[1].minor.yy373.gap.n = 0;}
        break;
      case 214: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy373.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy373.gap = yymsp[-1].minor.yy0;
}
        break;
      case 215: /* windowstate_option ::= */
{ yymsp[1].minor.yy204.col.n = 0; yymsp[1].minor.yy204.col.z = NULL;}
        break;
      case 216: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy204.col = yymsp[-1].minor.yy0; }
        break;
      case 217: /* fill_opt ::= */
{ yymsp[1].minor.yy367 = 0;     }
        break;
      case 218: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy367, &A, -1, 0);
    yymsp[-5].minor.yy367 = yymsp[-1].minor.yy367;
}
        break;
      case 219: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy367 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 220: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 221: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 223: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy367 = yymsp[0].minor.yy367;}
        break;
      case 224: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-3].minor.yy367, &yymsp[-1].minor.yy410, NULL, false, yymsp[0].minor.yy274);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 225: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-3].minor.yy367, NULL, yymsp[-1].minor.yy378, true, yymsp[0].minor.yy274);
}
  yymsp[-3].minor.yy367 = yylhsminor.yy367;
        break;
      case 226: /* sortlist ::= item sortorder */
{
  yylhsminor.yy367 = commonItemAppend(NULL, &yymsp[-1].minor.yy410, NULL, false, yymsp[0].minor.yy274);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 227: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy367 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy378, true, yymsp[0].minor.yy274);
}
  yymsp[-1].minor.yy367 = yylhsminor.yy367;
        break;
      case 228: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy410, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy410 = yylhsminor.yy410;
        break;
      case 229: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy410, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy410 = yylhsminor.yy410;
        break;
      case 230: /* sortorder ::= ASC */
{ yymsp[0].minor.yy274 = TSDB_ORDER_ASC; }
        break;
      case 231: /* sortorder ::= DESC */
{ yymsp[0].minor.yy274 = TSDB_ORDER_DESC;}
        break;
      case 232: /* sortorder ::= */
{ yymsp[1].minor.yy274 = TSDB_ORDER_ASC; }
        break;
      case 233: /* groupby_opt ::= */
{ yymsp[1].minor.yy367 = 0;}
        break;
      case 234: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy367 = yymsp[0].minor.yy367;}
        break;
      case 235: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-2].minor.yy367, &yymsp[0].minor.yy410, NULL, false, -1);
}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 236: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy367 = commonItemAppend(yymsp[-2].minor.yy367, NULL, yymsp[0].minor.yy378, true, -1);
}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 237: /* grouplist ::= item */
{
  yylhsminor.yy367 = commonItemAppend(NULL, &yymsp[0].minor.yy410, NULL, false, -1);
}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 238: /* grouplist ::= arrow */
{
  yylhsminor.yy367 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy378, true, -1);
}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 239: /* having_opt ::= */
      case 249: /* where_opt ::= */ yytestcase(yyruleno==249);
      case 306: /* expritem ::= */ yytestcase(yyruleno==306);
{yymsp[1].minor.yy378 = 0;}
        break;
      case 240: /* having_opt ::= HAVING expr */
      case 250: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==250);
{yymsp[-1].minor.yy378 = yymsp[0].minor.yy378;}
        break;
      case 241: /* limit_opt ::= */
      case 245: /* slimit_opt ::= */ yytestcase(yyruleno==245);
{yymsp[1].minor.yy24.limit = -1; yymsp[1].minor.yy24.offset = 0;}
        break;
      case 242: /* limit_opt ::= LIMIT signed */
      case 246: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==246);
{yymsp[-1].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-1].minor.yy24.offset = 0;}
        break;
      case 243: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy443;}
        break;
      case 244: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy443;}
        break;
      case 247: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy443;}
        break;
      case 248: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy443;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy443;}
        break;
      case 251: /* expr ::= LP expr RP */
{yylhsminor.yy378 = yymsp[-1].minor.yy378; yylhsminor.yy378->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy378->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 252: /* expr ::= ID */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 253: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 254: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 255: /* expr ::= INTEGER */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 256: /* expr ::= MINUS INTEGER */
      case 257: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==257);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 258: /* expr ::= FLOAT */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 259: /* expr ::= MINUS FLOAT */
      case 260: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==260);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 261: /* expr ::= STRING */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 262: /* expr ::= NOW */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 263: /* expr ::= TODAY */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 264: /* expr ::= VARIABLE */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 265: /* expr ::= PLUS VARIABLE */
      case 266: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==266);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 267: /* expr ::= BOOL */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 268: /* expr ::= NULL */
{ yylhsminor.yy378 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 269: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(yymsp[-1].minor.yy367, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 270: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 271: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy378 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy378, &yymsp[-1].minor.yy307, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy378 = yylhsminor.yy378;
        break;
      case 272: /* expr ::= expr IS NULL */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 273: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-3].minor.yy378, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy378 = yylhsminor.yy378;
        break;
      case 274: /* expr ::= expr LT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 275: /* expr ::= expr GT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_GT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 276: /* expr ::= expr LE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 277: /* expr ::= expr GE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_GE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 278: /* expr ::= expr NE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 279: /* expr ::= expr EQ expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_EQ);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 280: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy378); yylhsminor.yy378 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy378, yymsp[-2].minor.yy378, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy378, TK_LE), TK_AND);}
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 281: /* expr ::= expr AND expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_AND);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 282: /* expr ::= expr OR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_OR); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 283: /* expr ::= expr PLUS expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_PLUS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 284: /* expr ::= expr MINUS expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MINUS); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 285: /* expr ::= expr STAR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_STAR);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 286: /* expr ::= expr SLASH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_DIVIDE);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 287: /* expr ::= expr REM expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_REM);   }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 288: /* expr ::= expr BITAND expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITAND);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 289: /* expr ::= expr BITOR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITOR); }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 290: /* expr ::= expr BITXOR expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_BITXOR);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 291: /* expr ::= BITNOT expr */
{yymsp[-1].minor.yy378 = tSqlExprCreate(yymsp[0].minor.yy378, NULL, TK_BITNOT);}
        break;
      case 292: /* expr ::= expr LSHIFT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LSHIFT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 293: /* expr ::= expr RSHIFT expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_RSHIFT);}
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 294: /* expr ::= expr LIKE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LIKE);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 295: /* expr ::= expr MATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 296: /* expr ::= expr NMATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NMATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 297: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 298: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 299: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 300: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 301: /* expr ::= arrow */
      case 305: /* expritem ::= expr */ yytestcase(yyruleno==305);
{yylhsminor.yy378 = yymsp[0].minor.yy378;}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 302: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-4].minor.yy378, (tSqlExpr*)yymsp[-1].minor.yy367, TK_IN); }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 303: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy367 = tSqlExprListAppend(yymsp[-2].minor.yy367,yymsp[0].minor.yy378,0, 0);}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 304: /* exprlist ::= expritem */
{yylhsminor.yy367 = tSqlExprListAppend(0,yymsp[0].minor.yy378,0, 0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 307: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 308: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 309: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 315: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 318: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 321: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 322: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 323: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 324: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 325: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 326: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 327: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      case 328: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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