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
#define YYNOCODE 282
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tVariant yy2;
  SCreateDbInfo yy10;
  int32_t yy40;
  SSqlNode* yy68;
  SCreatedTableInfo yy72;
  SLimitVal yy114;
  SRangeVal yy144;
  SCreateTableSql* yy170;
  SIntervalVal yy280;
  int yy281;
  SSessionWindowVal yy295;
  SArray* yy345;
  tSqlExpr* yy418;
  SCreateAcctInfo yy427;
  SWindowStateVal yy432;
  SRelationInfo* yy484;
  TAOS_FIELD yy487;
  int64_t yy525;
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
#define YYNSTATE             379
#define YYNRULE              303
#define YYNRULE_WITH_ACTION  303
#define YYNTOKEN             199
#define YY_MAX_SHIFT         378
#define YY_MIN_SHIFTREDUCE   594
#define YY_MAX_SHIFTREDUCE   896
#define YY_ERROR_ACTION      897
#define YY_ACCEPT_ACTION     898
#define YY_NO_ACTION         899
#define YY_MIN_REDUCE        900
#define YY_MAX_REDUCE        1202
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
#define YY_ACTTAB_COUNT (791)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   245,  646,  377,  238, 1056,   23,  214,  730, 1078,  647,
 /*    10 */   682,  898,  378,   59,   60,  251,   63,   64, 1178, 1056,
 /*    20 */   259,   53,   52,   51,  646,   62,  335,   67,   65,   68,
 /*    30 */    66,  159,  647,  337,  175,   58,   57,  355,  354,   56,
 /*    40 */    55,   54,   59,   60,  253,   63,   64, 1055, 1056,  259,
 /*    50 */    53,   52,   51,  297,   62,  335,   67,   65,   68,   66,
 /*    60 */  1026, 1069, 1024, 1025,   58,   57, 1198, 1027,   56,   55,
 /*    70 */    54, 1028,  256, 1029, 1030,   58,   57, 1075,  281,   56,
 /*    80 */    55,   54,   59,   60,  166,   63,   64,   38,   84,  259,
 /*    90 */    53,   52,   51,   90,   62,  335,   67,   65,   68,   66,
 /*   100 */  1069,  288,  287,  646,   58,   57,  333,   29,   56,   55,
 /*   110 */    54,  647,   59,   61,  833,   63,   64,  241, 1042,  259,
 /*   120 */    53,   52,   51,  646,   62,  335,   67,   65,   68,   66,
 /*   130 */    45,  647,  240,  214,   58,   57, 1053,  852,   56,   55,
 /*   140 */    54,   60, 1050,   63,   64, 1179,  282,  259,   53,   52,
 /*   150 */    51,  166,   62,  335,   67,   65,   68,   66,   38,  309,
 /*   160 */    39,   95,   58,   57,  798,  799,   56,   55,   54,  595,
 /*   170 */   596,  597,  598,  599,  600,  601,  602,  603,  604,  605,
 /*   180 */   606,  607,  608,  157, 1069,  239,   63,   64,  770,  252,
 /*   190 */   259,   53,   52,   51,  255,   62,  335,   67,   65,   68,
 /*   200 */    66,  242,  365,  249,  333,   58,   57, 1053,  211,   56,
 /*   210 */    55,   54,  257,   44,  331,  372,  371,  330,  329,  328,
 /*   220 */   370,  327,  326,  325,  369,  324,  368,  367, 1126,   16,
 /*   230 */   307,   15,  166,   24,    6, 1018, 1006, 1007, 1008, 1009,
 /*   240 */  1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1019, 1020,
 /*   250 */   217,  166,  258,  848,  212,  214,  837,  225,  840,  839,
 /*   260 */   843,  842,   99,  141,  140,  139,  224, 1179,  258,  848,
 /*   270 */   340,   90,  837,  774,  840,  273,  843,   56,   55,   54,
 /*   280 */    67,   65,   68,   66,  277,  276,  236,  237,   58,   57,
 /*   290 */   336,  767,   56,   55,   54, 1039, 1040,   35, 1043,  260,
 /*   300 */   373,  987,  236,  237,    5,   41,  185,  268,   45, 1125,
 /*   310 */    38,  184,  108,  113,  104,  112,  754,    9,  181,  751,
 /*   320 */   262,  752,  786,  753,   38,  102,  789,  267,   96,   38,
 /*   330 */   320,  280,  838,   82,  841,   69,  125,  119,  130,  218,
 /*   340 */   232,  949,  118,  129,  117,  135,  138,  128,  195,  264,
 /*   350 */   265,   69,  293,  294,  132,  205,  203,  201,   38, 1052,
 /*   360 */   214, 1044,  200,  145,  144,  143,  142,  127,   38,  250,
 /*   370 */   849,  844, 1179, 1053,  344,   38,   38,  845, 1053,  365,
 /*   380 */   846,   44,   38,  372,  371,   83,  849,  844,  370,  376,
 /*   390 */   375,  150,  369,  845,  368,  367,   38,  263,   38,  261,
 /*   400 */   268,  343,  342,  345,  269,  219,  266, 1053,  350,  349,
 /*   410 */   815,  182,   14,  346,  220,  268,   98, 1053,   87, 1041,
 /*   420 */   347,  351,   88,   97, 1053, 1053, 1054,  352,  156,  154,
 /*   430 */   153, 1053,  959,  755,  756,  950,   34,  243,   85,  195,
 /*   440 */   795,  353,  195,  357,  805, 1053,  101, 1053,  806,    1,
 /*   450 */   183,    3,  196,  847,  161,  284,  292,  291,   70,  284,
 /*   460 */    75,   78,   26,  740,  312,  742,  314,  741,  814,  315,
 /*   470 */   871,  850,  835,  645,   18,   81,   17,   39,   39,   70,
 /*   480 */   100,   70,  137,  136,   25,   25,  759,   25,  760,   20,
 /*   490 */   757,   19,  758,  124,   22,  123,   21,  289, 1173, 1172,
 /*   500 */  1171,  234,   79,   76,  235,  215,  216,  729,  290, 1190,
 /*   510 */   836,  221,  213,  222,  223, 1136,  227,  228,  229, 1135,
 /*   520 */   247,  226,  278, 1132,  210, 1131,  248,  356,   48, 1070,
 /*   530 */   158, 1077, 1088, 1067,  155, 1085, 1086,  285, 1118, 1090,
 /*   540 */   160,  165, 1117,  303, 1051,  177,  283,   86,  178, 1049,
 /*   550 */   179,  180,  964,  785,  317,  318,  296,  319,  322,  323,
 /*   560 */   167,   46,  244,  298,  310,  168,  208,   42,  334,  958,
 /*   570 */   341, 1197,  115, 1196, 1193,  186,  348, 1189,  121,  300,
 /*   580 */    80,   77,   50,  308, 1188, 1185,  169,  306,  187,  304,
 /*   590 */   984,   43,  302,   40,   47,  209,  946,  131,  944,  133,
 /*   600 */   134,  942,  941,  299,  270,  198,  199,  938,  937,  936,
 /*   610 */   935,  934,  933,  932,  202,  204,  929,  927,  925,  923,
 /*   620 */   206,  920,  295,  207,  916,   49,  321,   91,  301, 1119,
 /*   630 */   366,  126,  359,  358,  360,  361,  363,  233,  362,  254,
 /*   640 */   316,  364,  374,  896,  271,  272,  895,  275,  274,  894,
 /*   650 */   876,  230,  963,  231,  109,  962,  110,  877,  279,  284,
 /*   660 */   311,   10,  286,  762,   30,   89,  940,  939,  190,  194,
 /*   670 */   146,  985,  188,  189,  192,  191,  931,  193,  147,  148,
 /*   680 */   930,    4,  149, 1022,   92,  986,  922,  176,  172,  170,
 /*   690 */   173,  171,  174,   33,  921,  794,    2,   73,  792,  791,
 /*   700 */  1032,  788,  787,   74,  164,  796,  162,  246,  807,  163,
 /*   710 */    31,  801,   93,   32,  803,   94,  305,   11,   12,   13,
 /*   720 */    27,  313,   36,   28,  101,  103,  106,  660,  695,  693,
 /*   730 */   692,  105,  691,  689,  688,   37,  107,  687,  684,  650,
 /*   740 */   111,  332,    7,  853,  851,    8,  338,  339,   39,  114,
 /*   750 */    71,  116,   72,  120,  732,  731,  728,  122,  676,  674,
 /*   760 */   666,  672,  668,  670,  664,  662,  698,  697,  696,  694,
 /*   770 */   690,  686,  685,  197,  648,  612,  900,  899,  899,  899,
 /*   780 */   899,  899,  899,  899,  899,  899,  899,  899,  899,  151,
 /*   790 */   152,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   247,    1,  201,  202,  251,  269,  269,    5,  201,    9,
 /*    10 */     5,  199,  200,   13,   14,  247,   16,   17,  281,  251,
 /*    20 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*    30 */    30,  201,    9,   15,  256,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  247,   16,   17,  251,  251,   20,
 /*    50 */    21,   22,   23,  275,   25,   26,   27,   28,   29,   30,
 /*    60 */   225,  249,  227,  228,   35,   36,  251,  232,   39,   40,
 /*    70 */    41,  236,  208,  238,  239,   35,   36,  270,  266,   39,
 /*    80 */    40,   41,   13,   14,  201,   16,   17,  201,   88,   20,
 /*    90 */    21,   22,   23,   84,   25,   26,   27,   28,   29,   30,
 /*   100 */   249,  271,  272,    1,   35,   36,   86,   84,   39,   40,
 /*   110 */    41,    9,   13,   14,   85,   16,   17,  266,    0,   20,
 /*   120 */    21,   22,   23,    1,   25,   26,   27,   28,   29,   30,
 /*   130 */   121,    9,  246,  269,   35,   36,  250,  119,   39,   40,
 /*   140 */    41,   14,  201,   16,   17,  281,   85,   20,   21,   22,
 /*   150 */    23,  201,   25,   26,   27,   28,   29,   30,  201,  276,
 /*   160 */    99,  278,   35,   36,  128,  129,   39,   40,   41,   47,
 /*   170 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   180 */    58,   59,   60,   61,  249,   63,   16,   17,   39,  248,
 /*   190 */    20,   21,   22,   23,  208,   25,   26,   27,   28,   29,
 /*   200 */    30,  266,   92,  246,   86,   35,   36,  250,  269,   39,
 /*   210 */    40,   41,   62,  100,  101,  102,  103,  104,  105,  106,
 /*   220 */   107,  108,  109,  110,  111,  112,  113,  114,  278,  148,
 /*   230 */   280,  150,  201,   46,   84,  225,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,  236,  237,  238,  239,
 /*   250 */    63,  201,    1,    2,  269,  269,    5,   70,    7,    5,
 /*   260 */     9,    7,  209,   76,   77,   78,   79,  281,    1,    2,
 /*   270 */    83,   84,    5,  124,    7,  145,    9,   39,   40,   41,
 /*   280 */    27,   28,   29,   30,  154,  155,   35,   36,   35,   36,
 /*   290 */    39,   99,   39,   40,   41,  242,  243,  244,  245,  208,
 /*   300 */   223,  224,   35,   36,   64,   65,   66,  201,  121,  278,
 /*   310 */   201,   71,   72,   73,   74,   75,    2,  125,  212,    5,
 /*   320 */    70,    7,    5,    9,  201,  209,    9,   70,  278,  201,
 /*   330 */    90,  144,    5,  146,    7,   84,   64,   65,   66,  269,
 /*   340 */   153,  207,  148,   71,  150,   73,   74,   75,  214,   35,
 /*   350 */    36,   84,   35,   36,   82,   64,   65,   66,  201,  250,
 /*   360 */   269,  245,   71,   72,   73,   74,   75,   80,  201,  246,
 /*   370 */   119,  120,  281,  250,  246,  201,  201,  126,  250,   92,
 /*   380 */   126,  100,  201,  102,  103,  209,  119,  120,  107,   67,
 /*   390 */    68,   69,  111,  126,  113,  114,  201,  147,  201,  149,
 /*   400 */   201,  151,  152,  246,  147,  269,  149,  250,  151,  152,
 /*   410 */    78,  212,   84,  246,  269,  201,   88,  250,   85,  243,
 /*   420 */   246,  246,   85,  252,  250,  250,  212,  246,   64,   65,
 /*   430 */    66,  250,  207,  119,  120,  207,   84,  120,  267,  214,
 /*   440 */    85,  246,  214,  246,   85,  250,  118,  250,   85,  210,
 /*   450 */   211,  205,  206,  126,   99,  122,   35,   36,   99,  122,
 /*   460 */    99,   99,   99,   85,   85,   85,   85,   85,  136,  117,
 /*   470 */    85,   85,    1,   85,  148,   84,  150,   99,   99,   99,
 /*   480 */    99,   99,   80,   81,   99,   99,    5,   99,    7,  148,
 /*   490 */     5,  150,    7,  148,  148,  150,  150,  274,  269,  269,
 /*   500 */   269,  269,  140,  142,  269,  269,  269,  116,  274,  251,
 /*   510 */    39,  269,  269,  269,  269,  241,  269,  269,  269,  241,
 /*   520 */   241,  269,  201,  241,  269,  241,  241,  241,  268,  249,
 /*   530 */   201,  201,  201,  265,   62,  201,  201,  249,  279,  201,
 /*   540 */   201,  201,  279,  201,  249,  253,  203,  203,  201,  201,
 /*   550 */   201,  201,  201,  126,  201,  201,  273,  201,  201,  201,
 /*   560 */   264,  201,  273,  273,  134,  263,  201,  201,  201,  201,
 /*   570 */   201,  201,  201,  201,  201,  201,  201,  201,  201,  273,
 /*   580 */   139,  141,  138,  137,  201,  201,  262,  132,  201,  131,
 /*   590 */   201,  201,  130,  201,  201,  201,  201,  201,  201,  201,
 /*   600 */   201,  201,  201,  133,  201,  201,  201,  201,  201,  201,
 /*   610 */   201,  201,  201,  201,  201,  201,  201,  201,  201,  201,
 /*   620 */   201,  201,  127,  201,  201,  143,   91,  203,  203,  203,
 /*   630 */   115,   98,   53,   97,   94,   96,   95,  203,   57,  203,
 /*   640 */   203,   93,   86,    5,  156,    5,    5,    5,  156,    5,
 /*   650 */   101,  203,  213,  203,  209,  213,  209,  102,  145,  122,
 /*   660 */   117,   84,   99,   85,   84,  123,  203,  203,  216,  215,
 /*   670 */   204,  222,  221,  220,  217,  219,  203,  218,  204,  204,
 /*   680 */   203,  205,  204,  240,   99,  224,  203,  254,  259,  261,
 /*   690 */   258,  260,  257,  255,  203,   85,  210,   99,  126,  126,
 /*   700 */   240,    5,    5,   84,   99,   85,   84,    1,   85,   84,
 /*   710 */    99,   85,   84,   99,   85,   84,   84,  135,  135,   84,
 /*   720 */    84,  117,   89,   84,  118,   80,   72,    5,    9,    5,
 /*   730 */     5,   88,    5,    5,    5,   89,   88,    5,    5,   87,
 /*   740 */    80,   15,   84,  119,   85,   84,   26,   61,   99,  150,
 /*   750 */    16,  150,   16,  150,    5,    5,   85,  150,    5,    5,
 /*   760 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   770 */     5,    5,    5,   99,   87,   62,    0,  282,  282,  282,
 /*   780 */   282,  282,  282,  282,  282,  282,  282,  282,  282,   21,
 /*   790 */    21,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   800 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   810 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   820 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   830 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   840 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   850 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   860 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   870 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   880 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   890 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   900 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   910 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   920 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   930 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   940 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   950 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   960 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   970 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   980 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
};
#define YY_SHIFT_COUNT    (378)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (776)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   187,  113,  113,  281,  281,   20,  251,  267,  267,   23,
 /*    10 */   102,  102,  102,  102,  102,  102,  102,  102,  102,  102,
 /*    20 */   102,  102,  102,    0,  122,  267,  314,  314,  314,    9,
 /*    30 */     9,  102,  102,   36,  102,  118,  102,  102,  102,  102,
 /*    40 */   287,   20,  110,  110,    5,  791,  791,  791,  267,  267,
 /*    50 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*    60 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*    70 */   314,  314,  314,  317,  317,    2,    2,    2,    2,    2,
 /*    80 */     2,    2,  102,  102,  102,  149,  102,  102,  102,    9,
 /*    90 */     9,  102,  102,  102,  102,  332,  332,  192,    9,  102,
 /*   100 */   102,  102,  102,  102,  102,  102,  102,  102,  102,  102,
 /*   110 */   102,  102,  102,  102,  102,  102,  102,  102,  102,  102,
 /*   120 */   102,  102,  102,  102,  102,  102,  102,  102,  102,  102,
 /*   130 */   102,  102,  102,  102,  102,  102,  102,  102,  102,  102,
 /*   140 */   102,  102,  102,  102,  102,  102,  102,  102,  102,  102,
 /*   150 */   102,  102,  102,  102,  102,  102,  102,  102,  472,  472,
 /*   160 */   472,  427,  427,  427,  427,  472,  472,  441,  440,  430,
 /*   170 */   444,  446,  455,  458,  462,  470,  495,  482,  472,  472,
 /*   180 */   472,  535,  535,  515,   20,   20,  472,  472,  533,  536,
 /*   190 */   579,  540,  539,  581,  541,  548,  515,    5,  472,  472,
 /*   200 */   556,  556,  472,  556,  472,  556,  472,  472,  791,  791,
 /*   210 */    29,   69,   69,   99,   69,  127,  170,  240,  253,  253,
 /*   220 */   253,  253,  253,  253,  272,  291,   40,   40,   40,   40,
 /*   230 */   250,  257,  130,  328,  238,  238,  254,  327,  322,  364,
 /*   240 */    61,  333,  337,  421,  355,  359,  363,  361,  362,  378,
 /*   250 */   379,  380,  381,  382,  352,  385,  386,  471,  150,   18,
 /*   260 */   388,   81,  194,  326,  481,  485,  341,  345,  391,  346,
 /*   270 */   402,  638,  488,  640,  641,  492,  642,  644,  555,  549,
 /*   280 */   513,  537,  543,  577,  542,  578,  580,  563,  585,  610,
 /*   290 */   598,  572,  573,  696,  697,  619,  620,  622,  623,  625,
 /*   300 */   626,  605,  628,  629,  631,  706,  632,  611,  582,  614,
 /*   310 */   583,  635,  543,  636,  604,  639,  606,  645,  633,  643,
 /*   320 */   654,  722,  646,  648,  719,  724,  725,  727,  728,  729,
 /*   330 */   732,  733,  652,  726,  660,  658,  659,  624,  661,  720,
 /*   340 */   686,  734,  599,  601,  649,  649,  649,  649,  736,  603,
 /*   350 */   607,  649,  649,  649,  749,  750,  671,  649,  753,  754,
 /*   360 */   755,  756,  757,  758,  759,  760,  761,  762,  763,  764,
 /*   370 */   765,  766,  767,  674,  687,  768,  769,  713,  776,
};
#define YY_REDUCE_COUNT (209)
#define YY_REDUCE_MIN   (-264)
#define YY_REDUCE_MAX   (491)
static const short yy_reduce_ofst[] = {
 /*     0 */  -188,   10,   10, -165, -165,   53, -136,  -14,   91, -170,
 /*    10 */  -114,  -50, -117,  -43,  123,  128,  157,  167,  174,  175,
 /*    20 */   181,  195,  197, -193, -199, -263, -247, -232, -203, -149,
 /*    30 */   -65,   31,   50, -222,  -59,  116,  106,  199,  214,  109,
 /*    40 */   134,  176,  225,  228,   77,  171,  239,  246, -264,  -61,
 /*    50 */   -15,   70,  136,  145,  229,  230,  231,  232,  235,  236,
 /*    60 */   237,  242,  243,  244,  245,  247,  248,  249,  252,  255,
 /*    70 */  -204, -185,  258,  223,  234,  274,  278,  279,  282,  284,
 /*    80 */   285,  286,  321,  329,  330,  260,  331,  334,  335,  280,
 /*    90 */   288,  338,  339,  340,  342,  259,  263,  292,  295,  347,
 /*   100 */   348,  349,  350,  351,  353,  354,  356,  357,  358,  360,
 /*   110 */   365,  366,  367,  368,  369,  370,  371,  372,  373,  374,
 /*   120 */   375,  376,  377,  383,  384,  387,  389,  390,  392,  393,
 /*   130 */   394,  395,  396,  397,  398,  399,  400,  401,  403,  404,
 /*   140 */   405,  406,  407,  408,  409,  410,  411,  412,  413,  414,
 /*   150 */   415,  416,  417,  418,  419,  420,  422,  423,  343,  344,
 /*   160 */   424,  283,  289,  290,  306,  425,  426,  268,  296,  302,
 /*   170 */   324,  428,  431,  429,  432,  435,  438,  433,  434,  436,
 /*   180 */   437,  439,  442,  443,  445,  447,  448,  450,  449,  451,
 /*   190 */   453,  452,  456,  457,  459,  454,  460,  461,  463,  464,
 /*   200 */   466,  474,  473,  475,  477,  478,  483,  491,  486,  476,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   897, 1021,  960, 1031,  947,  957, 1181, 1181, 1181,  897,
 /*    10 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*    20 */   897,  897,  897, 1079,  917, 1181,  897,  897,  897,  897,
 /*    30 */   897,  897,  897, 1103,  897,  957,  897,  897,  897,  897,
 /*    40 */   967,  957,  967,  967,  897, 1074, 1005, 1023,  897,  897,
 /*    50 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*    60 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*    70 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*    80 */   897,  897,  897,  897,  897, 1081, 1087, 1084,  897,  897,
 /*    90 */   897, 1089,  897,  897,  897, 1122, 1122, 1072,  897,  897,
 /*   100 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   110 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   120 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   130 */   897,  945,  897,  943,  897,  897,  897,  897,  897,  897,
 /*   140 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   150 */   928,  897,  897,  897,  897,  897,  897,  915,  919,  919,
 /*   160 */   919,  897,  897,  897,  897,  919,  919, 1129, 1133, 1115,
 /*   170 */  1127, 1123, 1110, 1108, 1106, 1114, 1099, 1137,  919,  919,
 /*   180 */   919,  965,  965,  961,  957,  957,  919,  919,  983,  981,
 /*   190 */   979,  971,  977,  973,  975,  969,  948,  897,  919,  919,
 /*   200 */   955,  955,  919,  955,  919,  955,  919,  919, 1005, 1023,
 /*   210 */   897, 1138, 1128,  897, 1180, 1168, 1167,  897, 1176, 1175,
 /*   220 */  1174, 1166, 1165, 1164,  897,  897, 1160, 1163, 1162, 1161,
 /*   230 */   897,  897,  897,  897, 1170, 1169,  897,  897,  897,  897,
 /*   240 */   897,  897,  897, 1096,  897,  897,  897, 1134, 1130,  897,
 /*   250 */   897,  897,  897,  897,  897,  897,  897,  897, 1140,  897,
 /*   260 */   897,  897,  897,  897,  897,  897,  897,  897, 1033,  897,
 /*   270 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   280 */   897, 1071,  897,  897,  897,  897,  897, 1083, 1082,  897,
 /*   290 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   300 */   897,  897,  897,  897,  897,  897,  897, 1124,  897, 1116,
 /*   310 */   897,  897, 1045,  897,  897,  897,  897,  897,  897,  897,
 /*   320 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   330 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   340 */   897,  897,  897,  897, 1199, 1194, 1195, 1192,  897,  897,
 /*   350 */   897, 1191, 1186, 1187,  897,  897,  897, 1184,  897,  897,
 /*   360 */   897,  897,  897,  897,  897,  897,  897,  897,  897,  897,
 /*   370 */   897,  897,  897,  989,  897,  926,  924,  897,  897,
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
    1,  /*      MATCH => ID */
    1,  /*     NMATCH => ID */
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
    1,  /*       NULL => ID */
    1,  /*        NOW => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
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
  /*   13 */ "OR",
  /*   14 */ "AND",
  /*   15 */ "NOT",
  /*   16 */ "EQ",
  /*   17 */ "NE",
  /*   18 */ "ISNULL",
  /*   19 */ "NOTNULL",
  /*   20 */ "IS",
  /*   21 */ "LIKE",
  /*   22 */ "MATCH",
  /*   23 */ "NMATCH",
  /*   24 */ "GLOB",
  /*   25 */ "BETWEEN",
  /*   26 */ "IN",
  /*   27 */ "GT",
  /*   28 */ "GE",
  /*   29 */ "LT",
  /*   30 */ "LE",
  /*   31 */ "BITAND",
  /*   32 */ "BITOR",
  /*   33 */ "LSHIFT",
  /*   34 */ "RSHIFT",
  /*   35 */ "PLUS",
  /*   36 */ "MINUS",
  /*   37 */ "DIVIDE",
  /*   38 */ "TIMES",
  /*   39 */ "STAR",
  /*   40 */ "SLASH",
  /*   41 */ "REM",
  /*   42 */ "CONCAT",
  /*   43 */ "UMINUS",
  /*   44 */ "UPLUS",
  /*   45 */ "BITNOT",
  /*   46 */ "SHOW",
  /*   47 */ "DATABASES",
  /*   48 */ "TOPICS",
  /*   49 */ "FUNCTIONS",
  /*   50 */ "MNODES",
  /*   51 */ "DNODES",
  /*   52 */ "ACCOUNTS",
  /*   53 */ "USERS",
  /*   54 */ "MODULES",
  /*   55 */ "QUERIES",
  /*   56 */ "CONNECTIONS",
  /*   57 */ "STREAMS",
  /*   58 */ "VARIABLES",
  /*   59 */ "SCORES",
  /*   60 */ "GRANTS",
  /*   61 */ "VNODES",
  /*   62 */ "DOT",
  /*   63 */ "CREATE",
  /*   64 */ "TABLE",
  /*   65 */ "STABLE",
  /*   66 */ "DATABASE",
  /*   67 */ "TABLES",
  /*   68 */ "STABLES",
  /*   69 */ "VGROUPS",
  /*   70 */ "DROP",
  /*   71 */ "TOPIC",
  /*   72 */ "FUNCTION",
  /*   73 */ "DNODE",
  /*   74 */ "USER",
  /*   75 */ "ACCOUNT",
  /*   76 */ "USE",
  /*   77 */ "DESCRIBE",
  /*   78 */ "DESC",
  /*   79 */ "ALTER",
  /*   80 */ "PASS",
  /*   81 */ "PRIVILEGE",
  /*   82 */ "LOCAL",
  /*   83 */ "COMPACT",
  /*   84 */ "LP",
  /*   85 */ "RP",
  /*   86 */ "IF",
  /*   87 */ "EXISTS",
  /*   88 */ "AS",
  /*   89 */ "OUTPUTTYPE",
  /*   90 */ "AGGREGATE",
  /*   91 */ "BUFSIZE",
  /*   92 */ "PPS",
  /*   93 */ "TSERIES",
  /*   94 */ "DBS",
  /*   95 */ "STORAGE",
  /*   96 */ "QTIME",
  /*   97 */ "CONNS",
  /*   98 */ "STATE",
  /*   99 */ "COMMA",
  /*  100 */ "KEEP",
  /*  101 */ "CACHE",
  /*  102 */ "REPLICA",
  /*  103 */ "QUORUM",
  /*  104 */ "DAYS",
  /*  105 */ "MINROWS",
  /*  106 */ "MAXROWS",
  /*  107 */ "BLOCKS",
  /*  108 */ "CTIME",
  /*  109 */ "WAL",
  /*  110 */ "FSYNC",
  /*  111 */ "COMP",
  /*  112 */ "PRECISION",
  /*  113 */ "UPDATE",
  /*  114 */ "CACHELAST",
  /*  115 */ "PARTITIONS",
  /*  116 */ "UNSIGNED",
  /*  117 */ "TAGS",
  /*  118 */ "USING",
  /*  119 */ "NULL",
  /*  120 */ "NOW",
  /*  121 */ "SELECT",
  /*  122 */ "UNION",
  /*  123 */ "ALL",
  /*  124 */ "DISTINCT",
  /*  125 */ "FROM",
  /*  126 */ "VARIABLE",
  /*  127 */ "RANGE",
  /*  128 */ "INTERVAL",
  /*  129 */ "EVERY",
  /*  130 */ "SESSION",
  /*  131 */ "STATE_WINDOW",
  /*  132 */ "FILL",
  /*  133 */ "SLIDING",
  /*  134 */ "ORDER",
  /*  135 */ "BY",
  /*  136 */ "ASC",
  /*  137 */ "GROUP",
  /*  138 */ "HAVING",
  /*  139 */ "LIMIT",
  /*  140 */ "OFFSET",
  /*  141 */ "SLIMIT",
  /*  142 */ "SOFFSET",
  /*  143 */ "WHERE",
  /*  144 */ "RESET",
  /*  145 */ "QUERY",
  /*  146 */ "SYNCDB",
  /*  147 */ "ADD",
  /*  148 */ "COLUMN",
  /*  149 */ "MODIFY",
  /*  150 */ "TAG",
  /*  151 */ "CHANGE",
  /*  152 */ "SET",
  /*  153 */ "KILL",
  /*  154 */ "CONNECTION",
  /*  155 */ "STREAM",
  /*  156 */ "COLON",
  /*  157 */ "ABORT",
  /*  158 */ "AFTER",
  /*  159 */ "ATTACH",
  /*  160 */ "BEFORE",
  /*  161 */ "BEGIN",
  /*  162 */ "CASCADE",
  /*  163 */ "CLUSTER",
  /*  164 */ "CONFLICT",
  /*  165 */ "COPY",
  /*  166 */ "DEFERRED",
  /*  167 */ "DELIMITERS",
  /*  168 */ "DETACH",
  /*  169 */ "EACH",
  /*  170 */ "END",
  /*  171 */ "EXPLAIN",
  /*  172 */ "FAIL",
  /*  173 */ "FOR",
  /*  174 */ "IGNORE",
  /*  175 */ "IMMEDIATE",
  /*  176 */ "INITIALLY",
  /*  177 */ "INSTEAD",
  /*  178 */ "KEY",
  /*  179 */ "OF",
  /*  180 */ "RAISE",
  /*  181 */ "REPLACE",
  /*  182 */ "RESTRICT",
  /*  183 */ "ROW",
  /*  184 */ "STATEMENT",
  /*  185 */ "TRIGGER",
  /*  186 */ "VIEW",
  /*  187 */ "IPTOKEN",
  /*  188 */ "SEMI",
  /*  189 */ "NONE",
  /*  190 */ "PREV",
  /*  191 */ "LINEAR",
  /*  192 */ "IMPORT",
  /*  193 */ "TBNAME",
  /*  194 */ "JOIN",
  /*  195 */ "INSERT",
  /*  196 */ "INTO",
  /*  197 */ "VALUES",
  /*  198 */ "FILE",
  /*  199 */ "program",
  /*  200 */ "cmd",
  /*  201 */ "ids",
  /*  202 */ "dbPrefix",
  /*  203 */ "cpxName",
  /*  204 */ "ifexists",
  /*  205 */ "alter_db_optr",
  /*  206 */ "alter_topic_optr",
  /*  207 */ "acct_optr",
  /*  208 */ "exprlist",
  /*  209 */ "ifnotexists",
  /*  210 */ "db_optr",
  /*  211 */ "topic_optr",
  /*  212 */ "typename",
  /*  213 */ "bufsize",
  /*  214 */ "pps",
  /*  215 */ "tseries",
  /*  216 */ "dbs",
  /*  217 */ "streams",
  /*  218 */ "storage",
  /*  219 */ "qtime",
  /*  220 */ "users",
  /*  221 */ "conns",
  /*  222 */ "state",
  /*  223 */ "intitemlist",
  /*  224 */ "intitem",
  /*  225 */ "keep",
  /*  226 */ "cache",
  /*  227 */ "replica",
  /*  228 */ "quorum",
  /*  229 */ "days",
  /*  230 */ "minrows",
  /*  231 */ "maxrows",
  /*  232 */ "blocks",
  /*  233 */ "ctime",
  /*  234 */ "wal",
  /*  235 */ "fsync",
  /*  236 */ "comp",
  /*  237 */ "prec",
  /*  238 */ "update",
  /*  239 */ "cachelast",
  /*  240 */ "partitions",
  /*  241 */ "signed",
  /*  242 */ "create_table_args",
  /*  243 */ "create_stable_args",
  /*  244 */ "create_table_list",
  /*  245 */ "create_from_stable",
  /*  246 */ "columnlist",
  /*  247 */ "tagitemlist",
  /*  248 */ "tagNamelist",
  /*  249 */ "select",
  /*  250 */ "column",
  /*  251 */ "tagitem",
  /*  252 */ "selcollist",
  /*  253 */ "from",
  /*  254 */ "where_opt",
  /*  255 */ "range_option",
  /*  256 */ "interval_option",
  /*  257 */ "sliding_opt",
  /*  258 */ "session_option",
  /*  259 */ "windowstate_option",
  /*  260 */ "fill_opt",
  /*  261 */ "groupby_opt",
  /*  262 */ "having_opt",
  /*  263 */ "orderby_opt",
  /*  264 */ "slimit_opt",
  /*  265 */ "limit_opt",
  /*  266 */ "union",
  /*  267 */ "sclp",
  /*  268 */ "distinct",
  /*  269 */ "expr",
  /*  270 */ "as",
  /*  271 */ "tablelist",
  /*  272 */ "sub",
  /*  273 */ "tmvar",
  /*  274 */ "timestamp",
  /*  275 */ "intervalKey",
  /*  276 */ "sortlist",
  /*  277 */ "sortitem",
  /*  278 */ "item",
  /*  279 */ "sortorder",
  /*  280 */ "grouplist",
  /*  281 */ "expritem",
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
 /*  25 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  27 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  29 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  30 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  32 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  33 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  34 */ "cmd ::= DROP FUNCTION ids",
 /*  35 */ "cmd ::= DROP DNODE ids",
 /*  36 */ "cmd ::= DROP USER ids",
 /*  37 */ "cmd ::= DROP ACCOUNT ids",
 /*  38 */ "cmd ::= USE ids",
 /*  39 */ "cmd ::= DESCRIBE ids cpxName",
 /*  40 */ "cmd ::= DESC ids cpxName",
 /*  41 */ "cmd ::= ALTER USER ids PASS ids",
 /*  42 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
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
 /*  65 */ "bufsize ::=",
 /*  66 */ "bufsize ::= BUFSIZE INTEGER",
 /*  67 */ "pps ::=",
 /*  68 */ "pps ::= PPS INTEGER",
 /*  69 */ "tseries ::=",
 /*  70 */ "tseries ::= TSERIES INTEGER",
 /*  71 */ "dbs ::=",
 /*  72 */ "dbs ::= DBS INTEGER",
 /*  73 */ "streams ::=",
 /*  74 */ "streams ::= STREAMS INTEGER",
 /*  75 */ "storage ::=",
 /*  76 */ "storage ::= STORAGE INTEGER",
 /*  77 */ "qtime ::=",
 /*  78 */ "qtime ::= QTIME INTEGER",
 /*  79 */ "users ::=",
 /*  80 */ "users ::= USERS INTEGER",
 /*  81 */ "conns ::=",
 /*  82 */ "conns ::= CONNS INTEGER",
 /*  83 */ "state ::=",
 /*  84 */ "state ::= STATE ids",
 /*  85 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  86 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  87 */ "intitemlist ::= intitem",
 /*  88 */ "intitem ::= INTEGER",
 /*  89 */ "keep ::= KEEP intitemlist",
 /*  90 */ "cache ::= CACHE INTEGER",
 /*  91 */ "replica ::= REPLICA INTEGER",
 /*  92 */ "quorum ::= QUORUM INTEGER",
 /*  93 */ "days ::= DAYS INTEGER",
 /*  94 */ "minrows ::= MINROWS INTEGER",
 /*  95 */ "maxrows ::= MAXROWS INTEGER",
 /*  96 */ "blocks ::= BLOCKS INTEGER",
 /*  97 */ "ctime ::= CTIME INTEGER",
 /*  98 */ "wal ::= WAL INTEGER",
 /*  99 */ "fsync ::= FSYNC INTEGER",
 /* 100 */ "comp ::= COMP INTEGER",
 /* 101 */ "prec ::= PRECISION STRING",
 /* 102 */ "update ::= UPDATE INTEGER",
 /* 103 */ "cachelast ::= CACHELAST INTEGER",
 /* 104 */ "partitions ::= PARTITIONS INTEGER",
 /* 105 */ "db_optr ::=",
 /* 106 */ "db_optr ::= db_optr cache",
 /* 107 */ "db_optr ::= db_optr replica",
 /* 108 */ "db_optr ::= db_optr quorum",
 /* 109 */ "db_optr ::= db_optr days",
 /* 110 */ "db_optr ::= db_optr minrows",
 /* 111 */ "db_optr ::= db_optr maxrows",
 /* 112 */ "db_optr ::= db_optr blocks",
 /* 113 */ "db_optr ::= db_optr ctime",
 /* 114 */ "db_optr ::= db_optr wal",
 /* 115 */ "db_optr ::= db_optr fsync",
 /* 116 */ "db_optr ::= db_optr comp",
 /* 117 */ "db_optr ::= db_optr prec",
 /* 118 */ "db_optr ::= db_optr keep",
 /* 119 */ "db_optr ::= db_optr update",
 /* 120 */ "db_optr ::= db_optr cachelast",
 /* 121 */ "topic_optr ::= db_optr",
 /* 122 */ "topic_optr ::= topic_optr partitions",
 /* 123 */ "alter_db_optr ::=",
 /* 124 */ "alter_db_optr ::= alter_db_optr replica",
 /* 125 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 126 */ "alter_db_optr ::= alter_db_optr keep",
 /* 127 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 128 */ "alter_db_optr ::= alter_db_optr comp",
 /* 129 */ "alter_db_optr ::= alter_db_optr update",
 /* 130 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 131 */ "alter_topic_optr ::= alter_db_optr",
 /* 132 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 133 */ "typename ::= ids",
 /* 134 */ "typename ::= ids LP signed RP",
 /* 135 */ "typename ::= ids UNSIGNED",
 /* 136 */ "signed ::= INTEGER",
 /* 137 */ "signed ::= PLUS INTEGER",
 /* 138 */ "signed ::= MINUS INTEGER",
 /* 139 */ "cmd ::= CREATE TABLE create_table_args",
 /* 140 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 141 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 142 */ "cmd ::= CREATE TABLE create_table_list",
 /* 143 */ "create_table_list ::= create_from_stable",
 /* 144 */ "create_table_list ::= create_table_list create_from_stable",
 /* 145 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 146 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 147 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 148 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 149 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 150 */ "tagNamelist ::= ids",
 /* 151 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 152 */ "columnlist ::= columnlist COMMA column",
 /* 153 */ "columnlist ::= column",
 /* 154 */ "column ::= ids typename",
 /* 155 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 156 */ "tagitemlist ::= tagitem",
 /* 157 */ "tagitem ::= INTEGER",
 /* 158 */ "tagitem ::= FLOAT",
 /* 159 */ "tagitem ::= STRING",
 /* 160 */ "tagitem ::= BOOL",
 /* 161 */ "tagitem ::= NULL",
 /* 162 */ "tagitem ::= NOW",
 /* 163 */ "tagitem ::= MINUS INTEGER",
 /* 164 */ "tagitem ::= MINUS FLOAT",
 /* 165 */ "tagitem ::= PLUS INTEGER",
 /* 166 */ "tagitem ::= PLUS FLOAT",
 /* 167 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 168 */ "select ::= LP select RP",
 /* 169 */ "union ::= select",
 /* 170 */ "union ::= union UNION ALL select",
 /* 171 */ "cmd ::= union",
 /* 172 */ "select ::= SELECT selcollist",
 /* 173 */ "sclp ::= selcollist COMMA",
 /* 174 */ "sclp ::=",
 /* 175 */ "selcollist ::= sclp distinct expr as",
 /* 176 */ "selcollist ::= sclp STAR",
 /* 177 */ "as ::= AS ids",
 /* 178 */ "as ::= ids",
 /* 179 */ "as ::=",
 /* 180 */ "distinct ::= DISTINCT",
 /* 181 */ "distinct ::=",
 /* 182 */ "from ::= FROM tablelist",
 /* 183 */ "from ::= FROM sub",
 /* 184 */ "sub ::= LP union RP",
 /* 185 */ "sub ::= LP union RP ids",
 /* 186 */ "sub ::= sub COMMA LP union RP ids",
 /* 187 */ "tablelist ::= ids cpxName",
 /* 188 */ "tablelist ::= ids cpxName ids",
 /* 189 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 190 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 191 */ "tmvar ::= VARIABLE",
 /* 192 */ "timestamp ::= INTEGER",
 /* 193 */ "timestamp ::= MINUS INTEGER",
 /* 194 */ "timestamp ::= PLUS INTEGER",
 /* 195 */ "timestamp ::= STRING",
 /* 196 */ "timestamp ::= NOW",
 /* 197 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 198 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 199 */ "range_option ::=",
 /* 200 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 201 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 202 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 203 */ "interval_option ::=",
 /* 204 */ "intervalKey ::= INTERVAL",
 /* 205 */ "intervalKey ::= EVERY",
 /* 206 */ "session_option ::=",
 /* 207 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 208 */ "windowstate_option ::=",
 /* 209 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 210 */ "fill_opt ::=",
 /* 211 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 212 */ "fill_opt ::= FILL LP ID RP",
 /* 213 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 214 */ "sliding_opt ::=",
 /* 215 */ "orderby_opt ::=",
 /* 216 */ "orderby_opt ::= ORDER BY sortlist",
 /* 217 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 218 */ "sortlist ::= item sortorder",
 /* 219 */ "item ::= ids cpxName",
 /* 220 */ "sortorder ::= ASC",
 /* 221 */ "sortorder ::= DESC",
 /* 222 */ "sortorder ::=",
 /* 223 */ "groupby_opt ::=",
 /* 224 */ "groupby_opt ::= GROUP BY grouplist",
 /* 225 */ "grouplist ::= grouplist COMMA item",
 /* 226 */ "grouplist ::= item",
 /* 227 */ "having_opt ::=",
 /* 228 */ "having_opt ::= HAVING expr",
 /* 229 */ "limit_opt ::=",
 /* 230 */ "limit_opt ::= LIMIT signed",
 /* 231 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 232 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 233 */ "slimit_opt ::=",
 /* 234 */ "slimit_opt ::= SLIMIT signed",
 /* 235 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 236 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 237 */ "where_opt ::=",
 /* 238 */ "where_opt ::= WHERE expr",
 /* 239 */ "expr ::= LP expr RP",
 /* 240 */ "expr ::= ID",
 /* 241 */ "expr ::= ID DOT ID",
 /* 242 */ "expr ::= ID DOT STAR",
 /* 243 */ "expr ::= INTEGER",
 /* 244 */ "expr ::= MINUS INTEGER",
 /* 245 */ "expr ::= PLUS INTEGER",
 /* 246 */ "expr ::= FLOAT",
 /* 247 */ "expr ::= MINUS FLOAT",
 /* 248 */ "expr ::= PLUS FLOAT",
 /* 249 */ "expr ::= STRING",
 /* 250 */ "expr ::= NOW",
 /* 251 */ "expr ::= VARIABLE",
 /* 252 */ "expr ::= PLUS VARIABLE",
 /* 253 */ "expr ::= MINUS VARIABLE",
 /* 254 */ "expr ::= BOOL",
 /* 255 */ "expr ::= NULL",
 /* 256 */ "expr ::= ID LP exprlist RP",
 /* 257 */ "expr ::= ID LP STAR RP",
 /* 258 */ "expr ::= expr IS NULL",
 /* 259 */ "expr ::= expr IS NOT NULL",
 /* 260 */ "expr ::= expr LT expr",
 /* 261 */ "expr ::= expr GT expr",
 /* 262 */ "expr ::= expr LE expr",
 /* 263 */ "expr ::= expr GE expr",
 /* 264 */ "expr ::= expr NE expr",
 /* 265 */ "expr ::= expr EQ expr",
 /* 266 */ "expr ::= expr BETWEEN expr AND expr",
 /* 267 */ "expr ::= expr AND expr",
 /* 268 */ "expr ::= expr OR expr",
 /* 269 */ "expr ::= expr PLUS expr",
 /* 270 */ "expr ::= expr MINUS expr",
 /* 271 */ "expr ::= expr STAR expr",
 /* 272 */ "expr ::= expr SLASH expr",
 /* 273 */ "expr ::= expr REM expr",
 /* 274 */ "expr ::= expr LIKE expr",
 /* 275 */ "expr ::= expr MATCH expr",
 /* 276 */ "expr ::= expr NMATCH expr",
 /* 277 */ "expr ::= expr IN LP exprlist RP",
 /* 278 */ "exprlist ::= exprlist COMMA expritem",
 /* 279 */ "exprlist ::= expritem",
 /* 280 */ "expritem ::= expr",
 /* 281 */ "expritem ::=",
 /* 282 */ "cmd ::= RESET QUERY CACHE",
 /* 283 */ "cmd ::= SYNCDB ids REPLICA",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 289 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 290 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 291 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 297 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 298 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 299 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 300 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 301 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 302 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 208: /* exprlist */
    case 252: /* selcollist */
    case 267: /* sclp */
{
tSqlExprListDestroy((yypminor->yy345));
}
      break;
    case 223: /* intitemlist */
    case 225: /* keep */
    case 246: /* columnlist */
    case 247: /* tagitemlist */
    case 248: /* tagNamelist */
    case 260: /* fill_opt */
    case 261: /* groupby_opt */
    case 263: /* orderby_opt */
    case 276: /* sortlist */
    case 280: /* grouplist */
{
taosArrayDestroy((yypminor->yy345));
}
      break;
    case 244: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy170));
}
      break;
    case 249: /* select */
{
destroySqlNode((yypminor->yy68));
}
      break;
    case 253: /* from */
    case 271: /* tablelist */
    case 272: /* sub */
{
destroyRelationInfo((yypminor->yy484));
}
      break;
    case 254: /* where_opt */
    case 262: /* having_opt */
    case 269: /* expr */
    case 274: /* timestamp */
    case 281: /* expritem */
{
tSqlExprDestroy((yypminor->yy418));
}
      break;
    case 266: /* union */
{
destroyAllSqlNode((yypminor->yy345));
}
      break;
    case 277: /* sortitem */
{
tVariantDestroy(&(yypminor->yy2));
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
   199,  /* (0) program ::= cmd */
   200,  /* (1) cmd ::= SHOW DATABASES */
   200,  /* (2) cmd ::= SHOW TOPICS */
   200,  /* (3) cmd ::= SHOW FUNCTIONS */
   200,  /* (4) cmd ::= SHOW MNODES */
   200,  /* (5) cmd ::= SHOW DNODES */
   200,  /* (6) cmd ::= SHOW ACCOUNTS */
   200,  /* (7) cmd ::= SHOW USERS */
   200,  /* (8) cmd ::= SHOW MODULES */
   200,  /* (9) cmd ::= SHOW QUERIES */
   200,  /* (10) cmd ::= SHOW CONNECTIONS */
   200,  /* (11) cmd ::= SHOW STREAMS */
   200,  /* (12) cmd ::= SHOW VARIABLES */
   200,  /* (13) cmd ::= SHOW SCORES */
   200,  /* (14) cmd ::= SHOW GRANTS */
   200,  /* (15) cmd ::= SHOW VNODES */
   200,  /* (16) cmd ::= SHOW VNODES ids */
   202,  /* (17) dbPrefix ::= */
   202,  /* (18) dbPrefix ::= ids DOT */
   203,  /* (19) cpxName ::= */
   203,  /* (20) cpxName ::= DOT ids */
   200,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   200,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   200,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   200,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   200,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   200,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   200,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   200,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   200,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   200,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   200,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   200,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   200,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   200,  /* (34) cmd ::= DROP FUNCTION ids */
   200,  /* (35) cmd ::= DROP DNODE ids */
   200,  /* (36) cmd ::= DROP USER ids */
   200,  /* (37) cmd ::= DROP ACCOUNT ids */
   200,  /* (38) cmd ::= USE ids */
   200,  /* (39) cmd ::= DESCRIBE ids cpxName */
   200,  /* (40) cmd ::= DESC ids cpxName */
   200,  /* (41) cmd ::= ALTER USER ids PASS ids */
   200,  /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
   200,  /* (43) cmd ::= ALTER DNODE ids ids */
   200,  /* (44) cmd ::= ALTER DNODE ids ids ids */
   200,  /* (45) cmd ::= ALTER LOCAL ids */
   200,  /* (46) cmd ::= ALTER LOCAL ids ids */
   200,  /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
   200,  /* (48) cmd ::= ALTER TOPIC ids alter_topic_optr */
   200,  /* (49) cmd ::= ALTER ACCOUNT ids acct_optr */
   200,  /* (50) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   200,  /* (51) cmd ::= COMPACT VNODES IN LP exprlist RP */
   201,  /* (52) ids ::= ID */
   201,  /* (53) ids ::= STRING */
   204,  /* (54) ifexists ::= IF EXISTS */
   204,  /* (55) ifexists ::= */
   209,  /* (56) ifnotexists ::= IF NOT EXISTS */
   209,  /* (57) ifnotexists ::= */
   200,  /* (58) cmd ::= CREATE DNODE ids */
   200,  /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   200,  /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   200,  /* (61) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   200,  /* (62) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   200,  /* (63) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   200,  /* (64) cmd ::= CREATE USER ids PASS ids */
   213,  /* (65) bufsize ::= */
   213,  /* (66) bufsize ::= BUFSIZE INTEGER */
   214,  /* (67) pps ::= */
   214,  /* (68) pps ::= PPS INTEGER */
   215,  /* (69) tseries ::= */
   215,  /* (70) tseries ::= TSERIES INTEGER */
   216,  /* (71) dbs ::= */
   216,  /* (72) dbs ::= DBS INTEGER */
   217,  /* (73) streams ::= */
   217,  /* (74) streams ::= STREAMS INTEGER */
   218,  /* (75) storage ::= */
   218,  /* (76) storage ::= STORAGE INTEGER */
   219,  /* (77) qtime ::= */
   219,  /* (78) qtime ::= QTIME INTEGER */
   220,  /* (79) users ::= */
   220,  /* (80) users ::= USERS INTEGER */
   221,  /* (81) conns ::= */
   221,  /* (82) conns ::= CONNS INTEGER */
   222,  /* (83) state ::= */
   222,  /* (84) state ::= STATE ids */
   207,  /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   223,  /* (86) intitemlist ::= intitemlist COMMA intitem */
   223,  /* (87) intitemlist ::= intitem */
   224,  /* (88) intitem ::= INTEGER */
   225,  /* (89) keep ::= KEEP intitemlist */
   226,  /* (90) cache ::= CACHE INTEGER */
   227,  /* (91) replica ::= REPLICA INTEGER */
   228,  /* (92) quorum ::= QUORUM INTEGER */
   229,  /* (93) days ::= DAYS INTEGER */
   230,  /* (94) minrows ::= MINROWS INTEGER */
   231,  /* (95) maxrows ::= MAXROWS INTEGER */
   232,  /* (96) blocks ::= BLOCKS INTEGER */
   233,  /* (97) ctime ::= CTIME INTEGER */
   234,  /* (98) wal ::= WAL INTEGER */
   235,  /* (99) fsync ::= FSYNC INTEGER */
   236,  /* (100) comp ::= COMP INTEGER */
   237,  /* (101) prec ::= PRECISION STRING */
   238,  /* (102) update ::= UPDATE INTEGER */
   239,  /* (103) cachelast ::= CACHELAST INTEGER */
   240,  /* (104) partitions ::= PARTITIONS INTEGER */
   210,  /* (105) db_optr ::= */
   210,  /* (106) db_optr ::= db_optr cache */
   210,  /* (107) db_optr ::= db_optr replica */
   210,  /* (108) db_optr ::= db_optr quorum */
   210,  /* (109) db_optr ::= db_optr days */
   210,  /* (110) db_optr ::= db_optr minrows */
   210,  /* (111) db_optr ::= db_optr maxrows */
   210,  /* (112) db_optr ::= db_optr blocks */
   210,  /* (113) db_optr ::= db_optr ctime */
   210,  /* (114) db_optr ::= db_optr wal */
   210,  /* (115) db_optr ::= db_optr fsync */
   210,  /* (116) db_optr ::= db_optr comp */
   210,  /* (117) db_optr ::= db_optr prec */
   210,  /* (118) db_optr ::= db_optr keep */
   210,  /* (119) db_optr ::= db_optr update */
   210,  /* (120) db_optr ::= db_optr cachelast */
   211,  /* (121) topic_optr ::= db_optr */
   211,  /* (122) topic_optr ::= topic_optr partitions */
   205,  /* (123) alter_db_optr ::= */
   205,  /* (124) alter_db_optr ::= alter_db_optr replica */
   205,  /* (125) alter_db_optr ::= alter_db_optr quorum */
   205,  /* (126) alter_db_optr ::= alter_db_optr keep */
   205,  /* (127) alter_db_optr ::= alter_db_optr blocks */
   205,  /* (128) alter_db_optr ::= alter_db_optr comp */
   205,  /* (129) alter_db_optr ::= alter_db_optr update */
   205,  /* (130) alter_db_optr ::= alter_db_optr cachelast */
   206,  /* (131) alter_topic_optr ::= alter_db_optr */
   206,  /* (132) alter_topic_optr ::= alter_topic_optr partitions */
   212,  /* (133) typename ::= ids */
   212,  /* (134) typename ::= ids LP signed RP */
   212,  /* (135) typename ::= ids UNSIGNED */
   241,  /* (136) signed ::= INTEGER */
   241,  /* (137) signed ::= PLUS INTEGER */
   241,  /* (138) signed ::= MINUS INTEGER */
   200,  /* (139) cmd ::= CREATE TABLE create_table_args */
   200,  /* (140) cmd ::= CREATE TABLE create_stable_args */
   200,  /* (141) cmd ::= CREATE STABLE create_stable_args */
   200,  /* (142) cmd ::= CREATE TABLE create_table_list */
   244,  /* (143) create_table_list ::= create_from_stable */
   244,  /* (144) create_table_list ::= create_table_list create_from_stable */
   242,  /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   243,  /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   245,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   245,  /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   248,  /* (149) tagNamelist ::= tagNamelist COMMA ids */
   248,  /* (150) tagNamelist ::= ids */
   242,  /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
   246,  /* (152) columnlist ::= columnlist COMMA column */
   246,  /* (153) columnlist ::= column */
   250,  /* (154) column ::= ids typename */
   247,  /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
   247,  /* (156) tagitemlist ::= tagitem */
   251,  /* (157) tagitem ::= INTEGER */
   251,  /* (158) tagitem ::= FLOAT */
   251,  /* (159) tagitem ::= STRING */
   251,  /* (160) tagitem ::= BOOL */
   251,  /* (161) tagitem ::= NULL */
   251,  /* (162) tagitem ::= NOW */
   251,  /* (163) tagitem ::= MINUS INTEGER */
   251,  /* (164) tagitem ::= MINUS FLOAT */
   251,  /* (165) tagitem ::= PLUS INTEGER */
   251,  /* (166) tagitem ::= PLUS FLOAT */
   249,  /* (167) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   249,  /* (168) select ::= LP select RP */
   266,  /* (169) union ::= select */
   266,  /* (170) union ::= union UNION ALL select */
   200,  /* (171) cmd ::= union */
   249,  /* (172) select ::= SELECT selcollist */
   267,  /* (173) sclp ::= selcollist COMMA */
   267,  /* (174) sclp ::= */
   252,  /* (175) selcollist ::= sclp distinct expr as */
   252,  /* (176) selcollist ::= sclp STAR */
   270,  /* (177) as ::= AS ids */
   270,  /* (178) as ::= ids */
   270,  /* (179) as ::= */
   268,  /* (180) distinct ::= DISTINCT */
   268,  /* (181) distinct ::= */
   253,  /* (182) from ::= FROM tablelist */
   253,  /* (183) from ::= FROM sub */
   272,  /* (184) sub ::= LP union RP */
   272,  /* (185) sub ::= LP union RP ids */
   272,  /* (186) sub ::= sub COMMA LP union RP ids */
   271,  /* (187) tablelist ::= ids cpxName */
   271,  /* (188) tablelist ::= ids cpxName ids */
   271,  /* (189) tablelist ::= tablelist COMMA ids cpxName */
   271,  /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
   273,  /* (191) tmvar ::= VARIABLE */
   274,  /* (192) timestamp ::= INTEGER */
   274,  /* (193) timestamp ::= MINUS INTEGER */
   274,  /* (194) timestamp ::= PLUS INTEGER */
   274,  /* (195) timestamp ::= STRING */
   274,  /* (196) timestamp ::= NOW */
   274,  /* (197) timestamp ::= NOW PLUS VARIABLE */
   274,  /* (198) timestamp ::= NOW MINUS VARIABLE */
   255,  /* (199) range_option ::= */
   255,  /* (200) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   256,  /* (201) interval_option ::= intervalKey LP tmvar RP */
   256,  /* (202) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   256,  /* (203) interval_option ::= */
   275,  /* (204) intervalKey ::= INTERVAL */
   275,  /* (205) intervalKey ::= EVERY */
   258,  /* (206) session_option ::= */
   258,  /* (207) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   259,  /* (208) windowstate_option ::= */
   259,  /* (209) windowstate_option ::= STATE_WINDOW LP ids RP */
   260,  /* (210) fill_opt ::= */
   260,  /* (211) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   260,  /* (212) fill_opt ::= FILL LP ID RP */
   257,  /* (213) sliding_opt ::= SLIDING LP tmvar RP */
   257,  /* (214) sliding_opt ::= */
   263,  /* (215) orderby_opt ::= */
   263,  /* (216) orderby_opt ::= ORDER BY sortlist */
   276,  /* (217) sortlist ::= sortlist COMMA item sortorder */
   276,  /* (218) sortlist ::= item sortorder */
   278,  /* (219) item ::= ids cpxName */
   279,  /* (220) sortorder ::= ASC */
   279,  /* (221) sortorder ::= DESC */
   279,  /* (222) sortorder ::= */
   261,  /* (223) groupby_opt ::= */
   261,  /* (224) groupby_opt ::= GROUP BY grouplist */
   280,  /* (225) grouplist ::= grouplist COMMA item */
   280,  /* (226) grouplist ::= item */
   262,  /* (227) having_opt ::= */
   262,  /* (228) having_opt ::= HAVING expr */
   265,  /* (229) limit_opt ::= */
   265,  /* (230) limit_opt ::= LIMIT signed */
   265,  /* (231) limit_opt ::= LIMIT signed OFFSET signed */
   265,  /* (232) limit_opt ::= LIMIT signed COMMA signed */
   264,  /* (233) slimit_opt ::= */
   264,  /* (234) slimit_opt ::= SLIMIT signed */
   264,  /* (235) slimit_opt ::= SLIMIT signed SOFFSET signed */
   264,  /* (236) slimit_opt ::= SLIMIT signed COMMA signed */
   254,  /* (237) where_opt ::= */
   254,  /* (238) where_opt ::= WHERE expr */
   269,  /* (239) expr ::= LP expr RP */
   269,  /* (240) expr ::= ID */
   269,  /* (241) expr ::= ID DOT ID */
   269,  /* (242) expr ::= ID DOT STAR */
   269,  /* (243) expr ::= INTEGER */
   269,  /* (244) expr ::= MINUS INTEGER */
   269,  /* (245) expr ::= PLUS INTEGER */
   269,  /* (246) expr ::= FLOAT */
   269,  /* (247) expr ::= MINUS FLOAT */
   269,  /* (248) expr ::= PLUS FLOAT */
   269,  /* (249) expr ::= STRING */
   269,  /* (250) expr ::= NOW */
   269,  /* (251) expr ::= VARIABLE */
   269,  /* (252) expr ::= PLUS VARIABLE */
   269,  /* (253) expr ::= MINUS VARIABLE */
   269,  /* (254) expr ::= BOOL */
   269,  /* (255) expr ::= NULL */
   269,  /* (256) expr ::= ID LP exprlist RP */
   269,  /* (257) expr ::= ID LP STAR RP */
   269,  /* (258) expr ::= expr IS NULL */
   269,  /* (259) expr ::= expr IS NOT NULL */
   269,  /* (260) expr ::= expr LT expr */
   269,  /* (261) expr ::= expr GT expr */
   269,  /* (262) expr ::= expr LE expr */
   269,  /* (263) expr ::= expr GE expr */
   269,  /* (264) expr ::= expr NE expr */
   269,  /* (265) expr ::= expr EQ expr */
   269,  /* (266) expr ::= expr BETWEEN expr AND expr */
   269,  /* (267) expr ::= expr AND expr */
   269,  /* (268) expr ::= expr OR expr */
   269,  /* (269) expr ::= expr PLUS expr */
   269,  /* (270) expr ::= expr MINUS expr */
   269,  /* (271) expr ::= expr STAR expr */
   269,  /* (272) expr ::= expr SLASH expr */
   269,  /* (273) expr ::= expr REM expr */
   269,  /* (274) expr ::= expr LIKE expr */
   269,  /* (275) expr ::= expr MATCH expr */
   269,  /* (276) expr ::= expr NMATCH expr */
   269,  /* (277) expr ::= expr IN LP exprlist RP */
   208,  /* (278) exprlist ::= exprlist COMMA expritem */
   208,  /* (279) exprlist ::= expritem */
   281,  /* (280) expritem ::= expr */
   281,  /* (281) expritem ::= */
   200,  /* (282) cmd ::= RESET QUERY CACHE */
   200,  /* (283) cmd ::= SYNCDB ids REPLICA */
   200,  /* (284) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   200,  /* (285) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   200,  /* (286) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   200,  /* (287) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   200,  /* (288) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   200,  /* (289) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   200,  /* (290) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   200,  /* (291) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   200,  /* (292) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   200,  /* (293) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   200,  /* (294) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   200,  /* (295) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   200,  /* (296) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   200,  /* (297) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   200,  /* (298) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   200,  /* (299) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   200,  /* (300) cmd ::= KILL CONNECTION INTEGER */
   200,  /* (301) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   200,  /* (302) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (34) cmd ::= DROP FUNCTION ids */
   -3,  /* (35) cmd ::= DROP DNODE ids */
   -3,  /* (36) cmd ::= DROP USER ids */
   -3,  /* (37) cmd ::= DROP ACCOUNT ids */
   -2,  /* (38) cmd ::= USE ids */
   -3,  /* (39) cmd ::= DESCRIBE ids cpxName */
   -3,  /* (40) cmd ::= DESC ids cpxName */
   -5,  /* (41) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
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
    0,  /* (65) bufsize ::= */
   -2,  /* (66) bufsize ::= BUFSIZE INTEGER */
    0,  /* (67) pps ::= */
   -2,  /* (68) pps ::= PPS INTEGER */
    0,  /* (69) tseries ::= */
   -2,  /* (70) tseries ::= TSERIES INTEGER */
    0,  /* (71) dbs ::= */
   -2,  /* (72) dbs ::= DBS INTEGER */
    0,  /* (73) streams ::= */
   -2,  /* (74) streams ::= STREAMS INTEGER */
    0,  /* (75) storage ::= */
   -2,  /* (76) storage ::= STORAGE INTEGER */
    0,  /* (77) qtime ::= */
   -2,  /* (78) qtime ::= QTIME INTEGER */
    0,  /* (79) users ::= */
   -2,  /* (80) users ::= USERS INTEGER */
    0,  /* (81) conns ::= */
   -2,  /* (82) conns ::= CONNS INTEGER */
    0,  /* (83) state ::= */
   -2,  /* (84) state ::= STATE ids */
   -9,  /* (85) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -3,  /* (86) intitemlist ::= intitemlist COMMA intitem */
   -1,  /* (87) intitemlist ::= intitem */
   -1,  /* (88) intitem ::= INTEGER */
   -2,  /* (89) keep ::= KEEP intitemlist */
   -2,  /* (90) cache ::= CACHE INTEGER */
   -2,  /* (91) replica ::= REPLICA INTEGER */
   -2,  /* (92) quorum ::= QUORUM INTEGER */
   -2,  /* (93) days ::= DAYS INTEGER */
   -2,  /* (94) minrows ::= MINROWS INTEGER */
   -2,  /* (95) maxrows ::= MAXROWS INTEGER */
   -2,  /* (96) blocks ::= BLOCKS INTEGER */
   -2,  /* (97) ctime ::= CTIME INTEGER */
   -2,  /* (98) wal ::= WAL INTEGER */
   -2,  /* (99) fsync ::= FSYNC INTEGER */
   -2,  /* (100) comp ::= COMP INTEGER */
   -2,  /* (101) prec ::= PRECISION STRING */
   -2,  /* (102) update ::= UPDATE INTEGER */
   -2,  /* (103) cachelast ::= CACHELAST INTEGER */
   -2,  /* (104) partitions ::= PARTITIONS INTEGER */
    0,  /* (105) db_optr ::= */
   -2,  /* (106) db_optr ::= db_optr cache */
   -2,  /* (107) db_optr ::= db_optr replica */
   -2,  /* (108) db_optr ::= db_optr quorum */
   -2,  /* (109) db_optr ::= db_optr days */
   -2,  /* (110) db_optr ::= db_optr minrows */
   -2,  /* (111) db_optr ::= db_optr maxrows */
   -2,  /* (112) db_optr ::= db_optr blocks */
   -2,  /* (113) db_optr ::= db_optr ctime */
   -2,  /* (114) db_optr ::= db_optr wal */
   -2,  /* (115) db_optr ::= db_optr fsync */
   -2,  /* (116) db_optr ::= db_optr comp */
   -2,  /* (117) db_optr ::= db_optr prec */
   -2,  /* (118) db_optr ::= db_optr keep */
   -2,  /* (119) db_optr ::= db_optr update */
   -2,  /* (120) db_optr ::= db_optr cachelast */
   -1,  /* (121) topic_optr ::= db_optr */
   -2,  /* (122) topic_optr ::= topic_optr partitions */
    0,  /* (123) alter_db_optr ::= */
   -2,  /* (124) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (125) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (126) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (127) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (128) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (129) alter_db_optr ::= alter_db_optr update */
   -2,  /* (130) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (131) alter_topic_optr ::= alter_db_optr */
   -2,  /* (132) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (133) typename ::= ids */
   -4,  /* (134) typename ::= ids LP signed RP */
   -2,  /* (135) typename ::= ids UNSIGNED */
   -1,  /* (136) signed ::= INTEGER */
   -2,  /* (137) signed ::= PLUS INTEGER */
   -2,  /* (138) signed ::= MINUS INTEGER */
   -3,  /* (139) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (140) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (141) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (142) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (143) create_table_list ::= create_from_stable */
   -2,  /* (144) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (145) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (146) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (148) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (149) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (150) tagNamelist ::= ids */
   -5,  /* (151) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (152) columnlist ::= columnlist COMMA column */
   -1,  /* (153) columnlist ::= column */
   -2,  /* (154) column ::= ids typename */
   -3,  /* (155) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (156) tagitemlist ::= tagitem */
   -1,  /* (157) tagitem ::= INTEGER */
   -1,  /* (158) tagitem ::= FLOAT */
   -1,  /* (159) tagitem ::= STRING */
   -1,  /* (160) tagitem ::= BOOL */
   -1,  /* (161) tagitem ::= NULL */
   -1,  /* (162) tagitem ::= NOW */
   -2,  /* (163) tagitem ::= MINUS INTEGER */
   -2,  /* (164) tagitem ::= MINUS FLOAT */
   -2,  /* (165) tagitem ::= PLUS INTEGER */
   -2,  /* (166) tagitem ::= PLUS FLOAT */
  -15,  /* (167) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (168) select ::= LP select RP */
   -1,  /* (169) union ::= select */
   -4,  /* (170) union ::= union UNION ALL select */
   -1,  /* (171) cmd ::= union */
   -2,  /* (172) select ::= SELECT selcollist */
   -2,  /* (173) sclp ::= selcollist COMMA */
    0,  /* (174) sclp ::= */
   -4,  /* (175) selcollist ::= sclp distinct expr as */
   -2,  /* (176) selcollist ::= sclp STAR */
   -2,  /* (177) as ::= AS ids */
   -1,  /* (178) as ::= ids */
    0,  /* (179) as ::= */
   -1,  /* (180) distinct ::= DISTINCT */
    0,  /* (181) distinct ::= */
   -2,  /* (182) from ::= FROM tablelist */
   -2,  /* (183) from ::= FROM sub */
   -3,  /* (184) sub ::= LP union RP */
   -4,  /* (185) sub ::= LP union RP ids */
   -6,  /* (186) sub ::= sub COMMA LP union RP ids */
   -2,  /* (187) tablelist ::= ids cpxName */
   -3,  /* (188) tablelist ::= ids cpxName ids */
   -4,  /* (189) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (190) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (191) tmvar ::= VARIABLE */
   -1,  /* (192) timestamp ::= INTEGER */
   -2,  /* (193) timestamp ::= MINUS INTEGER */
   -2,  /* (194) timestamp ::= PLUS INTEGER */
   -1,  /* (195) timestamp ::= STRING */
   -1,  /* (196) timestamp ::= NOW */
   -3,  /* (197) timestamp ::= NOW PLUS VARIABLE */
   -3,  /* (198) timestamp ::= NOW MINUS VARIABLE */
    0,  /* (199) range_option ::= */
   -6,  /* (200) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   -4,  /* (201) interval_option ::= intervalKey LP tmvar RP */
   -6,  /* (202) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
    0,  /* (203) interval_option ::= */
   -1,  /* (204) intervalKey ::= INTERVAL */
   -1,  /* (205) intervalKey ::= EVERY */
    0,  /* (206) session_option ::= */
   -7,  /* (207) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (208) windowstate_option ::= */
   -4,  /* (209) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (210) fill_opt ::= */
   -6,  /* (211) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (212) fill_opt ::= FILL LP ID RP */
   -4,  /* (213) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (214) sliding_opt ::= */
    0,  /* (215) orderby_opt ::= */
   -3,  /* (216) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (217) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (218) sortlist ::= item sortorder */
   -2,  /* (219) item ::= ids cpxName */
   -1,  /* (220) sortorder ::= ASC */
   -1,  /* (221) sortorder ::= DESC */
    0,  /* (222) sortorder ::= */
    0,  /* (223) groupby_opt ::= */
   -3,  /* (224) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (225) grouplist ::= grouplist COMMA item */
   -1,  /* (226) grouplist ::= item */
    0,  /* (227) having_opt ::= */
   -2,  /* (228) having_opt ::= HAVING expr */
    0,  /* (229) limit_opt ::= */
   -2,  /* (230) limit_opt ::= LIMIT signed */
   -4,  /* (231) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (232) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (233) slimit_opt ::= */
   -2,  /* (234) slimit_opt ::= SLIMIT signed */
   -4,  /* (235) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (236) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (237) where_opt ::= */
   -2,  /* (238) where_opt ::= WHERE expr */
   -3,  /* (239) expr ::= LP expr RP */
   -1,  /* (240) expr ::= ID */
   -3,  /* (241) expr ::= ID DOT ID */
   -3,  /* (242) expr ::= ID DOT STAR */
   -1,  /* (243) expr ::= INTEGER */
   -2,  /* (244) expr ::= MINUS INTEGER */
   -2,  /* (245) expr ::= PLUS INTEGER */
   -1,  /* (246) expr ::= FLOAT */
   -2,  /* (247) expr ::= MINUS FLOAT */
   -2,  /* (248) expr ::= PLUS FLOAT */
   -1,  /* (249) expr ::= STRING */
   -1,  /* (250) expr ::= NOW */
   -1,  /* (251) expr ::= VARIABLE */
   -2,  /* (252) expr ::= PLUS VARIABLE */
   -2,  /* (253) expr ::= MINUS VARIABLE */
   -1,  /* (254) expr ::= BOOL */
   -1,  /* (255) expr ::= NULL */
   -4,  /* (256) expr ::= ID LP exprlist RP */
   -4,  /* (257) expr ::= ID LP STAR RP */
   -3,  /* (258) expr ::= expr IS NULL */
   -4,  /* (259) expr ::= expr IS NOT NULL */
   -3,  /* (260) expr ::= expr LT expr */
   -3,  /* (261) expr ::= expr GT expr */
   -3,  /* (262) expr ::= expr LE expr */
   -3,  /* (263) expr ::= expr GE expr */
   -3,  /* (264) expr ::= expr NE expr */
   -3,  /* (265) expr ::= expr EQ expr */
   -5,  /* (266) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (267) expr ::= expr AND expr */
   -3,  /* (268) expr ::= expr OR expr */
   -3,  /* (269) expr ::= expr PLUS expr */
   -3,  /* (270) expr ::= expr MINUS expr */
   -3,  /* (271) expr ::= expr STAR expr */
   -3,  /* (272) expr ::= expr SLASH expr */
   -3,  /* (273) expr ::= expr REM expr */
   -3,  /* (274) expr ::= expr LIKE expr */
   -3,  /* (275) expr ::= expr MATCH expr */
   -3,  /* (276) expr ::= expr NMATCH expr */
   -5,  /* (277) expr ::= expr IN LP exprlist RP */
   -3,  /* (278) exprlist ::= exprlist COMMA expritem */
   -1,  /* (279) exprlist ::= expritem */
   -1,  /* (280) expritem ::= expr */
    0,  /* (281) expritem ::= */
   -3,  /* (282) cmd ::= RESET QUERY CACHE */
   -3,  /* (283) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (284) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (285) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (286) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (287) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (288) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (289) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (290) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (291) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (292) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (293) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (294) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (295) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (296) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (297) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (298) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (299) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (300) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (301) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (302) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 139: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==139);
      case 140: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==140);
      case 141: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==141);
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
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
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
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 35: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 36: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 38: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
      case 40: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==40);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 41: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 42: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy10, &t);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy427);}
        break;
      case 50: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy427);}
        break;
      case 51: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy345);}
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
      case 181: /* distinct ::= */ yytestcase(yyruleno==181);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 56: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 58: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy427);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 61: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==61);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy10, &yymsp[-2].minor.yy0);}
        break;
      case 62: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy487, &yymsp[0].minor.yy0, 1);}
        break;
      case 63: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy487, &yymsp[0].minor.yy0, 2);}
        break;
      case 64: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 65: /* bufsize ::= */
      case 67: /* pps ::= */ yytestcase(yyruleno==67);
      case 69: /* tseries ::= */ yytestcase(yyruleno==69);
      case 71: /* dbs ::= */ yytestcase(yyruleno==71);
      case 73: /* streams ::= */ yytestcase(yyruleno==73);
      case 75: /* storage ::= */ yytestcase(yyruleno==75);
      case 77: /* qtime ::= */ yytestcase(yyruleno==77);
      case 79: /* users ::= */ yytestcase(yyruleno==79);
      case 81: /* conns ::= */ yytestcase(yyruleno==81);
      case 83: /* state ::= */ yytestcase(yyruleno==83);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 66: /* bufsize ::= BUFSIZE INTEGER */
      case 68: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==70);
      case 72: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==76);
      case 78: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==78);
      case 80: /* users ::= USERS INTEGER */ yytestcase(yyruleno==80);
      case 82: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==82);
      case 84: /* state ::= STATE ids */ yytestcase(yyruleno==84);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 85: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy427.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy427.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy427.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy427.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy427.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy427.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy427.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy427.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy427.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy427 = yylhsminor.yy427;
        break;
      case 86: /* intitemlist ::= intitemlist COMMA intitem */
      case 155: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy345 = tVariantListAppend(yymsp[-2].minor.yy345, &yymsp[0].minor.yy2, -1);    }
  yymsp[-2].minor.yy345 = yylhsminor.yy345;
        break;
      case 87: /* intitemlist ::= intitem */
      case 156: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==156);
{ yylhsminor.yy345 = tVariantListAppend(NULL, &yymsp[0].minor.yy2, -1); }
  yymsp[0].minor.yy345 = yylhsminor.yy345;
        break;
      case 88: /* intitem ::= INTEGER */
      case 157: /* tagitem ::= INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= STRING */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= BOOL */ yytestcase(yyruleno==160);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy2, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 89: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy345 = yymsp[0].minor.yy345; }
        break;
      case 90: /* cache ::= CACHE INTEGER */
      case 91: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==91);
      case 92: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==92);
      case 93: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==96);
      case 97: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==97);
      case 98: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==98);
      case 99: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==99);
      case 100: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==100);
      case 101: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==101);
      case 102: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==102);
      case 103: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==103);
      case 104: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==104);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 105: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy10); yymsp[1].minor.yy10.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 106: /* db_optr ::= db_optr cache */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 107: /* db_optr ::= db_optr replica */
      case 124: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==124);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 108: /* db_optr ::= db_optr quorum */
      case 125: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==125);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 109: /* db_optr ::= db_optr days */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 110: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 111: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 112: /* db_optr ::= db_optr blocks */
      case 127: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==127);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 113: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 114: /* db_optr ::= db_optr wal */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 115: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 116: /* db_optr ::= db_optr comp */
      case 128: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==128);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 117: /* db_optr ::= db_optr prec */
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 118: /* db_optr ::= db_optr keep */
      case 126: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==126);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.keep = yymsp[0].minor.yy345; }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 119: /* db_optr ::= db_optr update */
      case 129: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==129);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 120: /* db_optr ::= db_optr cachelast */
      case 130: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==130);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 121: /* topic_optr ::= db_optr */
      case 131: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==131);
{ yylhsminor.yy10 = yymsp[0].minor.yy10; yylhsminor.yy10.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy10 = yylhsminor.yy10;
        break;
      case 122: /* topic_optr ::= topic_optr partitions */
      case 132: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==132);
{ yylhsminor.yy10 = yymsp[-1].minor.yy10; yylhsminor.yy10.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 123: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy10); yymsp[1].minor.yy10.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 133: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy487, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy487 = yylhsminor.yy487;
        break;
      case 134: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy525 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy487, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy525;  // negative value of name length
    tSetColumnType(&yylhsminor.yy487, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy487 = yylhsminor.yy487;
        break;
      case 135: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy487, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy487 = yylhsminor.yy487;
        break;
      case 136: /* signed ::= INTEGER */
{ yylhsminor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 137: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 138: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy525 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 142: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy170;}
        break;
      case 143: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy72);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy170 = pCreateTable;
}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 144: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy170->childTableInfo, &yymsp[0].minor.yy72);
  yylhsminor.yy170 = yymsp[-1].minor.yy170;
}
  yymsp[-1].minor.yy170 = yylhsminor.yy170;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy170 = tSetCreateTableInfo(yymsp[-1].minor.yy345, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy170, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy170 = yylhsminor.yy170;
        break;
      case 146: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy170 = tSetCreateTableInfo(yymsp[-5].minor.yy345, yymsp[-1].minor.yy345, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy170, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy170 = yylhsminor.yy170;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy72 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy345, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy72 = yylhsminor.yy72;
        break;
      case 148: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy72 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy345, yymsp[-1].minor.yy345, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy72 = yylhsminor.yy72;
        break;
      case 149: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy345, &yymsp[0].minor.yy0); yylhsminor.yy345 = yymsp[-2].minor.yy345;  }
  yymsp[-2].minor.yy345 = yylhsminor.yy345;
        break;
      case 150: /* tagNamelist ::= ids */
{yylhsminor.yy345 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy345, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy345 = yylhsminor.yy345;
        break;
      case 151: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy170 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy68, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy170, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy170 = yylhsminor.yy170;
        break;
      case 152: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy345, &yymsp[0].minor.yy487); yylhsminor.yy345 = yymsp[-2].minor.yy345;  }
  yymsp[-2].minor.yy345 = yylhsminor.yy345;
        break;
      case 153: /* columnlist ::= column */
{yylhsminor.yy345 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy345, &yymsp[0].minor.yy487);}
  yymsp[0].minor.yy345 = yylhsminor.yy345;
        break;
      case 154: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy487, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy487);
}
  yymsp[-1].minor.yy487 = yylhsminor.yy487;
        break;
      case 161: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy2, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 162: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy2, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 163: /* tagitem ::= MINUS INTEGER */
      case 164: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==166);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy2, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy2 = yylhsminor.yy2;
        break;
      case 167: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy68 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy345, yymsp[-12].minor.yy484, yymsp[-11].minor.yy418, yymsp[-4].minor.yy345, yymsp[-2].minor.yy345, &yymsp[-9].minor.yy280, &yymsp[-7].minor.yy295, &yymsp[-6].minor.yy432, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy345, &yymsp[0].minor.yy114, &yymsp[-1].minor.yy114, yymsp[-3].minor.yy418, &yymsp[-10].minor.yy144);
}
  yymsp[-14].minor.yy68 = yylhsminor.yy68;
        break;
      case 168: /* select ::= LP select RP */
{yymsp[-2].minor.yy68 = yymsp[-1].minor.yy68;}
        break;
      case 169: /* union ::= select */
{ yylhsminor.yy345 = setSubclause(NULL, yymsp[0].minor.yy68); }
  yymsp[0].minor.yy345 = yylhsminor.yy345;
        break;
      case 170: /* union ::= union UNION ALL select */
{ yylhsminor.yy345 = appendSelectClause(yymsp[-3].minor.yy345, yymsp[0].minor.yy68); }
  yymsp[-3].minor.yy345 = yylhsminor.yy345;
        break;
      case 171: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy345, NULL, TSDB_SQL_SELECT); }
        break;
      case 172: /* select ::= SELECT selcollist */
{
  yylhsminor.yy68 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy345, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 173: /* sclp ::= selcollist COMMA */
{yylhsminor.yy345 = yymsp[-1].minor.yy345;}
  yymsp[-1].minor.yy345 = yylhsminor.yy345;
        break;
      case 174: /* sclp ::= */
      case 215: /* orderby_opt ::= */ yytestcase(yyruleno==215);
{yymsp[1].minor.yy345 = 0;}
        break;
      case 175: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy345 = tSqlExprListAppend(yymsp[-3].minor.yy345, yymsp[-1].minor.yy418,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy345 = yylhsminor.yy345;
        break;
      case 176: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy345 = tSqlExprListAppend(yymsp[-1].minor.yy345, pNode, 0, 0);
}
  yymsp[-1].minor.yy345 = yylhsminor.yy345;
        break;
      case 177: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 178: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 179: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 180: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* from ::= FROM tablelist */
      case 183: /* from ::= FROM sub */ yytestcase(yyruleno==183);
{yymsp[-1].minor.yy484 = yymsp[0].minor.yy484;}
        break;
      case 184: /* sub ::= LP union RP */
{yymsp[-2].minor.yy484 = addSubqueryElem(NULL, yymsp[-1].minor.yy345, NULL);}
        break;
      case 185: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy484 = addSubqueryElem(NULL, yymsp[-2].minor.yy345, &yymsp[0].minor.yy0);}
        break;
      case 186: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy484 = addSubqueryElem(yymsp[-5].minor.yy484, yymsp[-2].minor.yy345, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy484 = yylhsminor.yy484;
        break;
      case 187: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy484 = yylhsminor.yy484;
        break;
      case 188: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(yymsp[-3].minor.yy484, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy484 = yylhsminor.yy484;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(yymsp[-4].minor.yy484, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy484 = yylhsminor.yy484;
        break;
      case 191: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 192: /* timestamp ::= INTEGER */
{ yylhsminor.yy418 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 193: /* timestamp ::= MINUS INTEGER */
      case 194: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==194);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy418 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy418 = yylhsminor.yy418;
        break;
      case 195: /* timestamp ::= STRING */
{ yylhsminor.yy418 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 196: /* timestamp ::= NOW */
{ yylhsminor.yy418 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 197: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy418 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 198: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy418 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 199: /* range_option ::= */
{yymsp[1].minor.yy144.start = 0; yymsp[1].minor.yy144.end = 0;}
        break;
      case 200: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy144.start = yymsp[-3].minor.yy418; yymsp[-5].minor.yy144.end = yymsp[-1].minor.yy418;}
        break;
      case 201: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy280.interval = yymsp[-1].minor.yy0; yylhsminor.yy280.offset.n = 0; yylhsminor.yy280.token = yymsp[-3].minor.yy40;}
  yymsp[-3].minor.yy280 = yylhsminor.yy280;
        break;
      case 202: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy280.interval = yymsp[-3].minor.yy0; yylhsminor.yy280.offset = yymsp[-1].minor.yy0;   yylhsminor.yy280.token = yymsp[-5].minor.yy40;}
  yymsp[-5].minor.yy280 = yylhsminor.yy280;
        break;
      case 203: /* interval_option ::= */
{memset(&yymsp[1].minor.yy280, 0, sizeof(yymsp[1].minor.yy280));}
        break;
      case 204: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy40 = TK_INTERVAL;}
        break;
      case 205: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy40 = TK_EVERY;   }
        break;
      case 206: /* session_option ::= */
{yymsp[1].minor.yy295.col.n = 0; yymsp[1].minor.yy295.gap.n = 0;}
        break;
      case 207: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy295.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy295.gap = yymsp[-1].minor.yy0;
}
        break;
      case 208: /* windowstate_option ::= */
{ yymsp[1].minor.yy432.col.n = 0; yymsp[1].minor.yy432.col.z = NULL;}
        break;
      case 209: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy432.col = yymsp[-1].minor.yy0; }
        break;
      case 210: /* fill_opt ::= */
{ yymsp[1].minor.yy345 = 0;     }
        break;
      case 211: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy345, &A, -1, 0);
    yymsp[-5].minor.yy345 = yymsp[-1].minor.yy345;
}
        break;
      case 212: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy345 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 213: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 214: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 216: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy345 = yymsp[0].minor.yy345;}
        break;
      case 217: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy345 = tVariantListAppend(yymsp[-3].minor.yy345, &yymsp[-1].minor.yy2, yymsp[0].minor.yy281);
}
  yymsp[-3].minor.yy345 = yylhsminor.yy345;
        break;
      case 218: /* sortlist ::= item sortorder */
{
  yylhsminor.yy345 = tVariantListAppend(NULL, &yymsp[-1].minor.yy2, yymsp[0].minor.yy281);
}
  yymsp[-1].minor.yy345 = yylhsminor.yy345;
        break;
      case 219: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy2, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy2 = yylhsminor.yy2;
        break;
      case 220: /* sortorder ::= ASC */
{ yymsp[0].minor.yy281 = TSDB_ORDER_ASC; }
        break;
      case 221: /* sortorder ::= DESC */
{ yymsp[0].minor.yy281 = TSDB_ORDER_DESC;}
        break;
      case 222: /* sortorder ::= */
{ yymsp[1].minor.yy281 = TSDB_ORDER_ASC; }
        break;
      case 223: /* groupby_opt ::= */
{ yymsp[1].minor.yy345 = 0;}
        break;
      case 224: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy345 = yymsp[0].minor.yy345;}
        break;
      case 225: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy345 = tVariantListAppend(yymsp[-2].minor.yy345, &yymsp[0].minor.yy2, -1);
}
  yymsp[-2].minor.yy345 = yylhsminor.yy345;
        break;
      case 226: /* grouplist ::= item */
{
  yylhsminor.yy345 = tVariantListAppend(NULL, &yymsp[0].minor.yy2, -1);
}
  yymsp[0].minor.yy345 = yylhsminor.yy345;
        break;
      case 227: /* having_opt ::= */
      case 237: /* where_opt ::= */ yytestcase(yyruleno==237);
      case 281: /* expritem ::= */ yytestcase(yyruleno==281);
{yymsp[1].minor.yy418 = 0;}
        break;
      case 228: /* having_opt ::= HAVING expr */
      case 238: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==238);
{yymsp[-1].minor.yy418 = yymsp[0].minor.yy418;}
        break;
      case 229: /* limit_opt ::= */
      case 233: /* slimit_opt ::= */ yytestcase(yyruleno==233);
{yymsp[1].minor.yy114.limit = -1; yymsp[1].minor.yy114.offset = 0;}
        break;
      case 230: /* limit_opt ::= LIMIT signed */
      case 234: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==234);
{yymsp[-1].minor.yy114.limit = yymsp[0].minor.yy525;  yymsp[-1].minor.yy114.offset = 0;}
        break;
      case 231: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy114.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy114.offset = yymsp[0].minor.yy525;}
        break;
      case 232: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy114.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy114.offset = yymsp[-2].minor.yy525;}
        break;
      case 235: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy114.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy114.offset = yymsp[0].minor.yy525;}
        break;
      case 236: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy114.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy114.offset = yymsp[-2].minor.yy525;}
        break;
      case 239: /* expr ::= LP expr RP */
{yylhsminor.yy418 = yymsp[-1].minor.yy418; yylhsminor.yy418->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy418->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 240: /* expr ::= ID */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 241: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 242: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 243: /* expr ::= INTEGER */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 244: /* expr ::= MINUS INTEGER */
      case 245: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==245);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy418 = yylhsminor.yy418;
        break;
      case 246: /* expr ::= FLOAT */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 247: /* expr ::= MINUS FLOAT */
      case 248: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==248);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy418 = yylhsminor.yy418;
        break;
      case 249: /* expr ::= STRING */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 250: /* expr ::= NOW */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 251: /* expr ::= VARIABLE */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 252: /* expr ::= PLUS VARIABLE */
      case 253: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==253);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy418 = yylhsminor.yy418;
        break;
      case 254: /* expr ::= BOOL */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 255: /* expr ::= NULL */
{ yylhsminor.yy418 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 256: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy418 = tSqlExprCreateFunction(yymsp[-1].minor.yy345, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy418 = yylhsminor.yy418;
        break;
      case 257: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy418 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy418 = yylhsminor.yy418;
        break;
      case 258: /* expr ::= expr IS NULL */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 259: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-3].minor.yy418, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy418 = yylhsminor.yy418;
        break;
      case 260: /* expr ::= expr LT expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_LT);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 261: /* expr ::= expr GT expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_GT);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 262: /* expr ::= expr LE expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_LE);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 263: /* expr ::= expr GE expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_GE);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 264: /* expr ::= expr NE expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_NE);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 265: /* expr ::= expr EQ expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_EQ);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 266: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy418); yylhsminor.yy418 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy418, yymsp[-2].minor.yy418, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy418, TK_LE), TK_AND);}
  yymsp[-4].minor.yy418 = yylhsminor.yy418;
        break;
      case 267: /* expr ::= expr AND expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_AND);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 268: /* expr ::= expr OR expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_OR); }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 269: /* expr ::= expr PLUS expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_PLUS);  }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 270: /* expr ::= expr MINUS expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_MINUS); }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 271: /* expr ::= expr STAR expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_STAR);  }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 272: /* expr ::= expr SLASH expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_DIVIDE);}
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 273: /* expr ::= expr REM expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_REM);   }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 274: /* expr ::= expr LIKE expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_LIKE);  }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 275: /* expr ::= expr MATCH expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_MATCH);  }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 276: /* expr ::= expr NMATCH expr */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-2].minor.yy418, yymsp[0].minor.yy418, TK_NMATCH);  }
  yymsp[-2].minor.yy418 = yylhsminor.yy418;
        break;
      case 277: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy418 = tSqlExprCreate(yymsp[-4].minor.yy418, (tSqlExpr*)yymsp[-1].minor.yy345, TK_IN); }
  yymsp[-4].minor.yy418 = yylhsminor.yy418;
        break;
      case 278: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy345 = tSqlExprListAppend(yymsp[-2].minor.yy345,yymsp[0].minor.yy418,0, 0);}
  yymsp[-2].minor.yy345 = yylhsminor.yy345;
        break;
      case 279: /* exprlist ::= expritem */
{yylhsminor.yy345 = tSqlExprListAppend(0,yymsp[0].minor.yy418,0, 0);}
  yymsp[0].minor.yy345 = yylhsminor.yy345;
        break;
      case 280: /* expritem ::= expr */
{yylhsminor.yy418 = yymsp[0].minor.yy418;}
  yymsp[0].minor.yy418 = yylhsminor.yy418;
        break;
      case 282: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 283: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 290: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy2, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 298: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy2, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 299: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy345, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 300: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 301: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 302: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
