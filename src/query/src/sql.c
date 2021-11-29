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
#define YYNOCODE 281
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int32_t yy2;
  SCreatedTableInfo yy42;
  tSqlExpr* yy44;
  SRelationInfo* yy46;
  SCreateAcctInfo yy47;
  TAOS_FIELD yy179;
  SLimitVal yy204;
  int yy222;
  SSqlNode* yy246;
  SArray* yy247;
  SCreateDbInfo yy262;
  SCreateTableSql* yy336;
  tVariant yy378;
  SRangeVal yy379;
  int64_t yy403;
  SIntervalVal yy430;
  SWindowStateVal yy492;
  SSessionWindowVal yy507;
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
#define YYNSTATE             381
#define YYNRULE              303
#define YYNRULE_WITH_ACTION  303
#define YYNTOKEN             198
#define YY_MAX_SHIFT         380
#define YY_MIN_SHIFTREDUCE   597
#define YY_MAX_SHIFTREDUCE   899
#define YY_ERROR_ACTION      900
#define YY_ACCEPT_ACTION     901
#define YY_NO_ACTION         902
#define YY_MIN_REDUCE        903
#define YY_MAX_REDUCE        1205
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
#define YY_ACTTAB_COUNT (813)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   166,  648,   23, 1080,  379,  239,  214,  732, 1044,  649,
 /*    10 */   684,  901,  380,   60,   61,  246,   64,   65, 1181, 1058,
 /*    20 */   260,   54,   53,   52,  648,   63,  336,   68,   66,   69,
 /*    30 */    67,  159,  649,  375,  989,   59,   58,  357,  356,   57,
 /*    40 */    56,   55,   60,   61,  252,   64,   65,  175, 1058,  260,
 /*    50 */    54,   53,   52,  166,   63,  336,   68,   66,   69,   67,
 /*    60 */  1028, 1071, 1026, 1027,   59,   58,  298, 1029,   57,   56,
 /*    70 */    55, 1030, 1077, 1031, 1032,   59,   58, 1127,  282,   57,
 /*    80 */    56,   55,   60,   61,  254,   64,   65,   85, 1058,  260,
 /*    90 */    54,   53,   52,  334,   63,  336,   68,   66,   69,   67,
 /*   100 */   951,  289,  288,  166,   59,   58,  257,  195,   57,   56,
 /*   110 */    55,   60,   61,   14,   64,   65,   38,   99,  260,   54,
 /*   120 */    53,   52,  769,   63,  336,   68,   66,   69,   67,  128,
 /*   130 */  1128,  788,  308,   59,   58,  791,  648,   57,   56,   55,
 /*   140 */   100,  367,   60,   62,  649,   64,   65,  102,    9,  260,
 /*   150 */    54,   53,   52,  835,   63,  336,   68,   66,   69,   67,
 /*   160 */  1071,  294,  295,   91,   59,   58,  256,  210,   57,   56,
 /*   170 */    55,   39,  334, 1041, 1042,   35, 1045,  242,  310, 1182,
 /*   180 */    96,  598,  599,  600,  601,  602,  603,  604,  605,  606,
 /*   190 */   607,  608,  609,  610,  611,  157,   61,  240,   64,   65,
 /*   200 */    46,  367,  260,   54,   53,   52,  648,   63,  336,   68,
 /*   210 */    66,   69,   67,  263,  649,  269,  241,   59,   58,  166,
 /*   220 */  1055,   57,   56,   55,   64,   65,  181,  214,  260,   54,
 /*   230 */    53,   52,  274,   63,  336,   68,   66,   69,   67, 1182,
 /*   240 */   817,  278,  277,   59,   58,  244, 1052,   57,   56,   55,
 /*   250 */    45,  332,  374,  373,  331,  330,  329,  372,  328,  327,
 /*   260 */   326,  371,  325,  370,  369, 1020, 1008, 1009, 1010, 1011,
 /*   270 */  1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1021, 1022,
 /*   280 */    24,  259,  850,   39,  261,  839,  212,  842,   29,  845,
 /*   290 */   264, 1071,  262,  253,  345,  344,   97,  218,  816,  259,
 /*   300 */   850,  800,  801,  839,  226,  842,   34,  845,  243, 1057,
 /*   310 */   142,  141,  140,  225, 1201,  237,  238,  342,   91,  338,
 /*   320 */     5,   42,  185,  103,   57,   56,   55,  184,  109,  114,
 /*   330 */   105,  113, 1054,  237,  238,   68,   66,   69,   67,  316,
 /*   340 */   841,   39,  844,   59,   58,  214,  321,   57,   56,   55,
 /*   350 */   268,  756,    1,  183,  753,   46,  754, 1182,  755, 1046,
 /*   360 */   840,  269,  843,   70,  126,  120,  131,  156,  154,  153,
 /*   370 */    39,  130,  182,  136,  139,  129,    3,  196,  281,  339,
 /*   380 */    83,   70,  133, 1193,  265,  266,  250,  233,   39,   39,
 /*   390 */  1055,   39,   84,  205,  203,  201,   39,   39,  851,  846,
 /*   400 */   200,  146,  145,  144,  143,  847,   39,   39,  772,   39,
 /*   410 */   269,   45,   79,  374,  373,  251,  851,  846,  372, 1055,
 /*   420 */    98,  337,  371,  847,  370,  369, 1043,  270,  269,  267,
 /*   430 */   283,  352,  351,  346,  347,   86,  348, 1055, 1055, 1056,
 /*   440 */  1055,  349,  353,  961,   40, 1055, 1055,  378,  377,  625,
 /*   450 */   195,  354,  355,   80,  359, 1055, 1055,  952, 1055,   88,
 /*   460 */   848,   89,  293,  292,  195,   76,  837,  757,  758,  797,
 /*   470 */   807,  808,  742,  313,  744,  315,  743,  874,  258,  852,
 /*   480 */   849,  647,  855,  161,   71,   26,   40,   40,   71,  101,
 /*   490 */    71,   25,  776,   25,   82,   25,  285,   16,  285,   15,
 /*   500 */     6,  119,  213,  118,  838,   18,  219,   17,   77,  761,
 /*   510 */   759,  762,  760,   20,  220,   19,  221,  125,   22,  124,
 /*   520 */    21,  138,  137, 1176, 1175,  290,  731, 1174,  235,  236,
 /*   530 */   216,  217,  222,  215,  223,  224,  228,  229,  230,  227,
 /*   540 */   211,  279, 1138, 1137,  158,  248,  291, 1134, 1079, 1133,
 /*   550 */   249,  358, 1090,   49, 1072, 1120, 1087, 1088,  155, 1092,
 /*   560 */   160,  286,  165,  304, 1053, 1119,  297,  177,  178,  245,
 /*   570 */   322,  299, 1051,  179,  180,  787,  966,  172,  318,  319,
 /*   580 */   320,  167,  323,  324,   47, 1069,  168,  311,  169,  301,
 /*   590 */   307,   81,  170,   51,  208,   78,   43,  309,  335,  960,
 /*   600 */   343, 1200,  116, 1199, 1196,  186,  350, 1192,  122, 1191,
 /*   610 */  1188,  305,  187,  986,   44,   41,   48,  303,  209,  948,
 /*   620 */   132,  946,  134,  135,  944,  943,  271,  300,  198,  199,
 /*   630 */   940,  939,  938,  937,  936,  935,  934,  202,  204,  930,
 /*   640 */   928,  926,  296,  206,  923,  207,  919,  368,   50,  284,
 /*   650 */    87,   92,  127,  302, 1121,  360,  361,  362,  363,  364,
 /*   660 */   365,  366,  376,  899,  273,  234,  272,  898,  255,  317,
 /*   670 */   276,  897,  275,  880,  231,  232,  879,  285,  280,  110,
 /*   680 */   965,  964,  111,  312,   10,  287,  764,   90,  942,  941,
 /*   690 */    30,   93,  933,  190,  147,  189,  987,  188,  932,  192,
 /*   700 */   191,  193,  988,  194,  148,  149,  150,    2,  796, 1024,
 /*   710 */   925,  924,   74,  794,   33,  173,  171,  176,  174,    4,
 /*   720 */   793,  790,  789,   75, 1034,  798,  162,  164,  809,  163,
 /*   730 */   247,  803,   94,   31,  805,   95,  306,   32,   13,   11,
 /*   740 */    12,  314,  104,   27,   28,  102,  107,  662,  697,  695,
 /*   750 */   694,  693,   36,  691,  106,  690,   37,  108,  689,  686,
 /*   760 */   652,  112,  333,    7,  340,  854,  856,  853,    8,  341,
 /*   770 */   115,   72,  117,   73,   40,  121,  123,  734,  733,  730,
 /*   780 */   678,  676,  668,  674,  670,  672,  666,  664,  700,  699,
 /*   790 */   698,  696,  692,  688,  687,  197,  650,  615,  903,  902,
 /*   800 */   902,  902,  902,  902,  902,  902,  902,  902,  902,  902,
 /*   810 */   902,  151,  152,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   200,    1,  268,  200,  200,  201,  268,    5,    0,    9,
 /*    10 */     5,  198,  199,   13,   14,  246,   16,   17,  280,  250,
 /*    20 */    20,   21,   22,   23,    1,   25,   26,   27,   28,   29,
 /*    30 */    30,  200,    9,  222,  223,   35,   36,   35,   36,   39,
 /*    40 */    40,   41,   13,   14,  246,   16,   17,  255,  250,   20,
 /*    50 */    21,   22,   23,  200,   25,   26,   27,   28,   29,   30,
 /*    60 */   224,  248,  226,  227,   35,   36,  274,  231,   39,   40,
 /*    70 */    41,  235,  269,  237,  238,   35,   36,  277,  265,   39,
 /*    80 */    40,   41,   13,   14,  246,   16,   17,   87,  250,   20,
 /*    90 */    21,   22,   23,   85,   25,   26,   27,   28,   29,   30,
 /*   100 */   206,  270,  271,  200,   35,   36,  207,  213,   39,   40,
 /*   110 */    41,   13,   14,   83,   16,   17,   87,   87,   20,   21,
 /*   120 */    22,   23,   98,   25,   26,   27,   28,   29,   30,   79,
 /*   130 */   277,    5,  279,   35,   36,    9,    1,   39,   40,   41,
 /*   140 */   208,   91,   13,   14,    9,   16,   17,  117,  124,   20,
 /*   150 */    21,   22,   23,   84,   25,   26,   27,   28,   29,   30,
 /*   160 */   248,   35,   36,   83,   35,   36,  207,  268,   39,   40,
 /*   170 */    41,  200,   85,  241,  242,  243,  244,  265,  275,  280,
 /*   180 */   277,   46,   47,   48,   49,   50,   51,   52,   53,   54,
 /*   190 */    55,   56,   57,   58,   59,   60,   14,   62,   16,   17,
 /*   200 */   120,   91,   20,   21,   22,   23,    1,   25,   26,   27,
 /*   210 */    28,   29,   30,   69,    9,  200,  245,   35,   36,  200,
 /*   220 */   249,   39,   40,   41,   16,   17,  211,  268,   20,   21,
 /*   230 */    22,   23,  144,   25,   26,   27,   28,   29,   30,  280,
 /*   240 */    77,  153,  154,   35,   36,  119,  200,   39,   40,   41,
 /*   250 */    99,  100,  101,  102,  103,  104,  105,  106,  107,  108,
 /*   260 */   109,  110,  111,  112,  113,  224,  225,  226,  227,  228,
 /*   270 */   229,  230,  231,  232,  233,  234,  235,  236,  237,  238,
 /*   280 */    45,    1,    2,  200,  207,    5,  268,    7,   83,    9,
 /*   290 */   146,  248,  148,  247,  150,  151,  277,   62,  135,    1,
 /*   300 */     2,  127,  128,    5,   69,    7,   83,    9,  265,  250,
 /*   310 */    75,   76,   77,   78,  250,   35,   36,   82,   83,   39,
 /*   320 */    63,   64,   65,  208,   39,   40,   41,   70,   71,   72,
 /*   330 */    73,   74,  249,   35,   36,   27,   28,   29,   30,  116,
 /*   340 */     5,  200,    7,   35,   36,  268,   89,   39,   40,   41,
 /*   350 */    69,    2,  209,  210,    5,  120,    7,  280,    9,  244,
 /*   360 */     5,  200,    7,   83,   63,   64,   65,   63,   64,   65,
 /*   370 */   200,   70,  211,   72,   73,   74,  204,  205,  143,   15,
 /*   380 */   145,   83,   81,  250,   35,   36,  245,  152,  200,  200,
 /*   390 */   249,  200,  208,   63,   64,   65,  200,  200,  118,  119,
 /*   400 */    70,   71,   72,   73,   74,  125,  200,  200,   39,  200,
 /*   410 */   200,   99,   98,  101,  102,  245,  118,  119,  106,  249,
 /*   420 */   251,  211,  110,  125,  112,  113,  242,  146,  200,  148,
 /*   430 */    84,  150,  151,  245,  245,  266,  245,  249,  249,  211,
 /*   440 */   249,  245,  245,  206,   98,  249,  249,   66,   67,   68,
 /*   450 */   213,  245,  245,  139,  245,  249,  249,  206,  249,   84,
 /*   460 */   125,   84,   35,   36,  213,   98,    1,  118,  119,   84,
 /*   470 */    84,   84,   84,   84,   84,   84,   84,   84,   61,   84,
 /*   480 */   125,   84,  118,   98,   98,   98,   98,   98,   98,   98,
 /*   490 */    98,   98,  123,   98,   83,   98,  121,  147,  121,  149,
 /*   500 */    83,  147,  268,  149,   39,  147,  268,  149,  141,    5,
 /*   510 */     5,    7,    7,  147,  268,  149,  268,  147,  147,  149,
 /*   520 */   149,   79,   80,  268,  268,  273,  115,  268,  268,  268,
 /*   530 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   540 */   268,  200,  240,  240,  200,  240,  273,  240,  200,  240,
 /*   550 */   240,  240,  200,  267,  248,  278,  200,  200,   61,  200,
 /*   560 */   200,  248,  200,  200,  248,  278,  272,  252,  200,  272,
 /*   570 */    90,  272,  200,  200,  200,  125,  200,  258,  200,  200,
 /*   580 */   200,  263,  200,  200,  200,  264,  262,  133,  261,  272,
 /*   590 */   131,  138,  260,  137,  200,  140,  200,  136,  200,  200,
 /*   600 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   610 */   200,  130,  200,  200,  200,  200,  200,  129,  200,  200,
 /*   620 */   200,  200,  200,  200,  200,  200,  200,  132,  200,  200,
 /*   630 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   640 */   200,  200,  126,  200,  200,  200,  200,  114,  142,  202,
 /*   650 */   202,  202,   97,  202,  202,   96,   52,   93,   95,   56,
 /*   660 */    94,   92,   85,    5,    5,  202,  155,    5,  202,  202,
 /*   670 */     5,    5,  155,  101,  202,  202,  100,  121,  144,  208,
 /*   680 */   212,  212,  208,  116,   83,   98,   84,  122,  202,  202,
 /*   690 */    83,   98,  202,  215,  203,  219,  221,  220,  202,  216,
 /*   700 */   218,  217,  223,  214,  203,  203,  203,  209,   84,  239,
 /*   710 */   202,  202,   98,  125,  254,  257,  259,  253,  256,  204,
 /*   720 */   125,    5,    5,   83,  239,   84,   83,   98,   84,   83,
 /*   730 */     1,   84,   83,   98,   84,   83,   83,   98,   83,  134,
 /*   740 */   134,  116,   79,   83,   83,  117,   71,    5,    9,    5,
 /*   750 */     5,    5,   88,    5,   87,    5,   88,   87,    5,    5,
 /*   760 */    86,   79,   15,   83,   26,   84,  118,   84,   83,   60,
 /*   770 */   149,   16,  149,   16,   98,  149,  149,    5,    5,   84,
 /*   780 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   790 */     5,    5,    5,    5,    5,   98,   86,   61,    0,  281,
 /*   800 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   810 */   281,   21,   21,  281,  281,  281,  281,  281,  281,  281,
 /*   820 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   830 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   840 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   850 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   860 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   870 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   880 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   890 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   900 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   910 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   920 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   930 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   940 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   950 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   960 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   970 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   980 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   990 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*  1000 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*  1010 */   281,
};
#define YY_SHIFT_COUNT    (380)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (798)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   235,  151,  151,  312,  312,   87,  280,  298,  298,  205,
 /*    10 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*    20 */    23,   23,   23,    0,  135,  298,  349,  349,  349,   80,
 /*    30 */    80,   23,   23,  174,   23,    8,   23,   23,   23,   23,
 /*    40 */    23,   50,   87,  110,  110,    5,  813,  813,  813,  298,
 /*    50 */   298,  298,  298,  298,  298,  298,  298,  298,  298,  298,
 /*    60 */   298,  298,  298,  298,  298,  298,  298,  298,  298,  298,
 /*    70 */   298,  349,  349,  349,  126,  126,    2,    2,    2,    2,
 /*    80 */     2,    2,    2,   23,   23,   23,  369,   23,   23,   23,
 /*    90 */    80,   80,   23,   23,   23,   23,  163,  163,   24,   80,
 /*   100 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   110 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   120 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   130 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   140 */    23,   23,   23,   23,   23,   23,   23,   23,   23,   23,
 /*   150 */    23,   23,   23,   23,   23,   23,   23,   23,  497,  497,
 /*   160 */   497,  450,  450,  450,  450,  497,  497,  453,  455,  454,
 /*   170 */   456,  461,  459,  481,  488,  495,  516,  506,  497,  497,
 /*   180 */   497,  480,  480,  533,   87,   87,  497,  497,  555,  559,
 /*   190 */   604,  564,  563,  603,  566,  569,  533,    5,  497,  497,
 /*   200 */   577,  577,  497,  577,  497,  577,  497,  497,  813,  813,
 /*   210 */    29,   69,   98,   98,   98,  129,  182,  208,  257,  308,
 /*   220 */   308,  308,  308,  308,  308,  301,  330,   40,   40,   40,
 /*   230 */    40,  144,  281,   88,   30,  285,  285,  335,  355,  381,
 /*   240 */   304,  346,  375,  377,  427,  385,  386,  387,  367,  314,
 /*   250 */   388,  389,  390,  391,  392,  223,  393,  395,  465,  417,
 /*   260 */   364,  397,  350,  354,  358,  504,  505,  366,  370,  411,
 /*   270 */   371,  442,  658,  511,  659,  662,  517,  665,  666,  572,
 /*   280 */   576,  534,  556,  567,  601,  565,  602,  607,  587,  593,
 /*   290 */   624,  614,  588,  595,  716,  717,  640,  641,  643,  644,
 /*   300 */   646,  647,  629,  649,  650,  652,  729,  653,  635,  605,
 /*   310 */   639,  606,  655,  567,  660,  625,  661,  628,  663,  664,
 /*   320 */   667,  675,  742,  668,  670,  739,  744,  745,  746,  748,
 /*   330 */   750,  753,  754,  674,  747,  682,  680,  681,  683,  648,
 /*   340 */   685,  738,  709,  755,  621,  623,  676,  676,  676,  676,
 /*   350 */   757,  626,  627,  676,  676,  676,  772,  773,  695,  676,
 /*   360 */   775,  776,  777,  778,  779,  780,  781,  782,  783,  784,
 /*   370 */   785,  786,  787,  788,  789,  697,  710,  790,  791,  736,
 /*   380 */   798,
};
#define YY_REDUCE_COUNT (209)
#define YY_REDUCE_MIN   (-266)
#define YY_REDUCE_MAX   (515)
static const short yy_reduce_ofst[] = {
 /*     0 */  -187,   41,   41, -164, -164,  -68, -101,  -41,   77, -169,
 /*    10 */   -29, -147,  -97,  141,  170,  188,  189,  191,  196,  197,
 /*    20 */   206,  207,  209, -197, -196, -262, -231, -202, -162,  -88,
 /*    30 */    43, -200,   19, -208,   46,  115,   15,  161,  210,  228,
 /*    40 */    83, -106,  184,  237,  251, -189,  169,  143,  172, -266,
 /*    50 */    18,  234,  238,  246,  248,  255,  256,  259,  260,  261,
 /*    60 */   262,  263,  264,  265,  266,  267,  268,  269,  270,  271,
 /*    70 */   272,   59,   64,  133,  252,  273,  302,  303,  305,  307,
 /*    80 */   309,  310,  311,  341,  344,  348,  286,  352,  356,  357,
 /*    90 */   306,  313,  359,  360,  362,  363,  277,  287,  315,  316,
 /*   100 */   368,  372,  373,  374,  376,  378,  379,  380,  382,  383,
 /*   110 */   384,  394,  396,  398,  399,  400,  401,  402,  403,  404,
 /*   120 */   405,  406,  407,  408,  409,  410,  412,  413,  414,  415,
 /*   130 */   416,  418,  419,  420,  421,  422,  423,  424,  425,  426,
 /*   140 */   428,  429,  430,  431,  432,  433,  434,  435,  436,  437,
 /*   150 */   438,  439,  440,  441,  443,  444,  445,  446,  447,  448,
 /*   160 */   449,  294,  297,  299,  317,  451,  452,  321,  318,  324,
 /*   170 */   327,  332,  457,  319,  458,  462,  460,  464,  463,  466,
 /*   180 */   467,  468,  469,  470,  471,  474,  472,  473,  475,  477,
 /*   190 */   476,  478,  482,  483,  484,  489,  485,  479,  486,  487,
 /*   200 */   491,  501,  490,  502,  496,  503,  508,  509,  498,  515,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   900, 1023,  962, 1033,  949,  959, 1184, 1184, 1184,  900,
 /*    10 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*    20 */   900,  900,  900, 1081,  920, 1184,  900,  900,  900,  900,
 /*    30 */   900,  900,  900, 1105,  900,  959,  900,  900,  900,  900,
 /*    40 */   900,  969,  959,  969,  969,  900, 1076, 1007, 1025,  900,
 /*    50 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*    60 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*    70 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*    80 */   900,  900,  900,  900,  900,  900, 1083, 1089, 1086,  900,
 /*    90 */   900,  900, 1091,  900,  900,  900, 1124, 1124, 1074,  900,
 /*   100 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   110 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   120 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   130 */   900,  900,  947,  900,  945,  900,  900,  900,  900,  900,
 /*   140 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   150 */   900,  900,  900,  900,  900,  900,  900,  918,  922,  922,
 /*   160 */   922,  900,  900,  900,  900,  922,  922, 1131, 1135, 1117,
 /*   170 */  1129, 1125, 1112, 1110, 1108, 1116, 1101, 1139,  922,  922,
 /*   180 */   922,  967,  967,  963,  959,  959,  922,  922,  985,  983,
 /*   190 */   981,  973,  979,  975,  977,  971,  950,  900,  922,  922,
 /*   200 */   957,  957,  922,  957,  922,  957,  922,  922, 1007, 1025,
 /*   210 */  1183,  900, 1140, 1130, 1183,  900, 1171, 1170,  900, 1179,
 /*   220 */  1178, 1177, 1169, 1168, 1167,  900,  900, 1163, 1166, 1165,
 /*   230 */  1164,  900,  900,  900,  900, 1173, 1172,  900,  900,  900,
 /*   240 */   900,  900,  900,  900, 1098,  900,  900,  900, 1136, 1132,
 /*   250 */   900,  900,  900,  900,  900,  900,  900,  900,  900, 1142,
 /*   260 */   900,  900,  900,  900,  900,  900,  900,  900,  900, 1035,
 /*   270 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   280 */   900,  900, 1073,  900,  900,  900,  900,  900, 1085, 1084,
 /*   290 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   300 */   900,  900,  900,  900,  900,  900,  900,  900, 1126,  900,
 /*   310 */  1118,  900,  900, 1047,  900,  900,  900,  900,  900,  900,
 /*   320 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   330 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   340 */   900,  900,  900,  900,  900,  900, 1202, 1197, 1198, 1195,
 /*   350 */   900,  900,  900, 1194, 1189, 1190,  900,  900,  900, 1187,
 /*   360 */   900,  900,  900,  900,  900,  900,  900,  900,  900,  900,
 /*   370 */   900,  900,  900,  900,  900,  991,  900,  929,  927,  900,
 /*   380 */   900,
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
  /*   42 */ "UMINUS",
  /*   43 */ "UPLUS",
  /*   44 */ "BITNOT",
  /*   45 */ "SHOW",
  /*   46 */ "DATABASES",
  /*   47 */ "TOPICS",
  /*   48 */ "FUNCTIONS",
  /*   49 */ "MNODES",
  /*   50 */ "DNODES",
  /*   51 */ "ACCOUNTS",
  /*   52 */ "USERS",
  /*   53 */ "MODULES",
  /*   54 */ "QUERIES",
  /*   55 */ "CONNECTIONS",
  /*   56 */ "STREAMS",
  /*   57 */ "VARIABLES",
  /*   58 */ "SCORES",
  /*   59 */ "GRANTS",
  /*   60 */ "VNODES",
  /*   61 */ "DOT",
  /*   62 */ "CREATE",
  /*   63 */ "TABLE",
  /*   64 */ "STABLE",
  /*   65 */ "DATABASE",
  /*   66 */ "TABLES",
  /*   67 */ "STABLES",
  /*   68 */ "VGROUPS",
  /*   69 */ "DROP",
  /*   70 */ "TOPIC",
  /*   71 */ "FUNCTION",
  /*   72 */ "DNODE",
  /*   73 */ "USER",
  /*   74 */ "ACCOUNT",
  /*   75 */ "USE",
  /*   76 */ "DESCRIBE",
  /*   77 */ "DESC",
  /*   78 */ "ALTER",
  /*   79 */ "PASS",
  /*   80 */ "PRIVILEGE",
  /*   81 */ "LOCAL",
  /*   82 */ "COMPACT",
  /*   83 */ "LP",
  /*   84 */ "RP",
  /*   85 */ "IF",
  /*   86 */ "EXISTS",
  /*   87 */ "AS",
  /*   88 */ "OUTPUTTYPE",
  /*   89 */ "AGGREGATE",
  /*   90 */ "BUFSIZE",
  /*   91 */ "PPS",
  /*   92 */ "TSERIES",
  /*   93 */ "DBS",
  /*   94 */ "STORAGE",
  /*   95 */ "QTIME",
  /*   96 */ "CONNS",
  /*   97 */ "STATE",
  /*   98 */ "COMMA",
  /*   99 */ "KEEP",
  /*  100 */ "CACHE",
  /*  101 */ "REPLICA",
  /*  102 */ "QUORUM",
  /*  103 */ "DAYS",
  /*  104 */ "MINROWS",
  /*  105 */ "MAXROWS",
  /*  106 */ "BLOCKS",
  /*  107 */ "CTIME",
  /*  108 */ "WAL",
  /*  109 */ "FSYNC",
  /*  110 */ "COMP",
  /*  111 */ "PRECISION",
  /*  112 */ "UPDATE",
  /*  113 */ "CACHELAST",
  /*  114 */ "PARTITIONS",
  /*  115 */ "UNSIGNED",
  /*  116 */ "TAGS",
  /*  117 */ "USING",
  /*  118 */ "NULL",
  /*  119 */ "NOW",
  /*  120 */ "SELECT",
  /*  121 */ "UNION",
  /*  122 */ "ALL",
  /*  123 */ "DISTINCT",
  /*  124 */ "FROM",
  /*  125 */ "VARIABLE",
  /*  126 */ "RANGE",
  /*  127 */ "INTERVAL",
  /*  128 */ "EVERY",
  /*  129 */ "SESSION",
  /*  130 */ "STATE_WINDOW",
  /*  131 */ "FILL",
  /*  132 */ "SLIDING",
  /*  133 */ "ORDER",
  /*  134 */ "BY",
  /*  135 */ "ASC",
  /*  136 */ "GROUP",
  /*  137 */ "HAVING",
  /*  138 */ "LIMIT",
  /*  139 */ "OFFSET",
  /*  140 */ "SLIMIT",
  /*  141 */ "SOFFSET",
  /*  142 */ "WHERE",
  /*  143 */ "RESET",
  /*  144 */ "QUERY",
  /*  145 */ "SYNCDB",
  /*  146 */ "ADD",
  /*  147 */ "COLUMN",
  /*  148 */ "MODIFY",
  /*  149 */ "TAG",
  /*  150 */ "CHANGE",
  /*  151 */ "SET",
  /*  152 */ "KILL",
  /*  153 */ "CONNECTION",
  /*  154 */ "STREAM",
  /*  155 */ "COLON",
  /*  156 */ "ABORT",
  /*  157 */ "AFTER",
  /*  158 */ "ATTACH",
  /*  159 */ "BEFORE",
  /*  160 */ "BEGIN",
  /*  161 */ "CASCADE",
  /*  162 */ "CLUSTER",
  /*  163 */ "CONFLICT",
  /*  164 */ "COPY",
  /*  165 */ "DEFERRED",
  /*  166 */ "DELIMITERS",
  /*  167 */ "DETACH",
  /*  168 */ "EACH",
  /*  169 */ "END",
  /*  170 */ "EXPLAIN",
  /*  171 */ "FAIL",
  /*  172 */ "FOR",
  /*  173 */ "IGNORE",
  /*  174 */ "IMMEDIATE",
  /*  175 */ "INITIALLY",
  /*  176 */ "INSTEAD",
  /*  177 */ "KEY",
  /*  178 */ "OF",
  /*  179 */ "RAISE",
  /*  180 */ "REPLACE",
  /*  181 */ "RESTRICT",
  /*  182 */ "ROW",
  /*  183 */ "STATEMENT",
  /*  184 */ "TRIGGER",
  /*  185 */ "VIEW",
  /*  186 */ "IPTOKEN",
  /*  187 */ "SEMI",
  /*  188 */ "NONE",
  /*  189 */ "PREV",
  /*  190 */ "LINEAR",
  /*  191 */ "IMPORT",
  /*  192 */ "TBNAME",
  /*  193 */ "JOIN",
  /*  194 */ "INSERT",
  /*  195 */ "INTO",
  /*  196 */ "VALUES",
  /*  197 */ "FILE",
  /*  198 */ "program",
  /*  199 */ "cmd",
  /*  200 */ "ids",
  /*  201 */ "dbPrefix",
  /*  202 */ "cpxName",
  /*  203 */ "ifexists",
  /*  204 */ "alter_db_optr",
  /*  205 */ "alter_topic_optr",
  /*  206 */ "acct_optr",
  /*  207 */ "exprlist",
  /*  208 */ "ifnotexists",
  /*  209 */ "db_optr",
  /*  210 */ "topic_optr",
  /*  211 */ "typename",
  /*  212 */ "bufsize",
  /*  213 */ "pps",
  /*  214 */ "tseries",
  /*  215 */ "dbs",
  /*  216 */ "streams",
  /*  217 */ "storage",
  /*  218 */ "qtime",
  /*  219 */ "users",
  /*  220 */ "conns",
  /*  221 */ "state",
  /*  222 */ "intitemlist",
  /*  223 */ "intitem",
  /*  224 */ "keep",
  /*  225 */ "cache",
  /*  226 */ "replica",
  /*  227 */ "quorum",
  /*  228 */ "days",
  /*  229 */ "minrows",
  /*  230 */ "maxrows",
  /*  231 */ "blocks",
  /*  232 */ "ctime",
  /*  233 */ "wal",
  /*  234 */ "fsync",
  /*  235 */ "comp",
  /*  236 */ "prec",
  /*  237 */ "update",
  /*  238 */ "cachelast",
  /*  239 */ "partitions",
  /*  240 */ "signed",
  /*  241 */ "create_table_args",
  /*  242 */ "create_stable_args",
  /*  243 */ "create_table_list",
  /*  244 */ "create_from_stable",
  /*  245 */ "columnlist",
  /*  246 */ "tagitemlist",
  /*  247 */ "tagNamelist",
  /*  248 */ "select",
  /*  249 */ "column",
  /*  250 */ "tagitem",
  /*  251 */ "selcollist",
  /*  252 */ "from",
  /*  253 */ "where_opt",
  /*  254 */ "range_option",
  /*  255 */ "interval_option",
  /*  256 */ "sliding_opt",
  /*  257 */ "session_option",
  /*  258 */ "windowstate_option",
  /*  259 */ "fill_opt",
  /*  260 */ "groupby_opt",
  /*  261 */ "having_opt",
  /*  262 */ "orderby_opt",
  /*  263 */ "slimit_opt",
  /*  264 */ "limit_opt",
  /*  265 */ "union",
  /*  266 */ "sclp",
  /*  267 */ "distinct",
  /*  268 */ "expr",
  /*  269 */ "as",
  /*  270 */ "tablelist",
  /*  271 */ "sub",
  /*  272 */ "tmvar",
  /*  273 */ "timestamp",
  /*  274 */ "intervalKey",
  /*  275 */ "sortlist",
  /*  276 */ "sortitem",
  /*  277 */ "item",
  /*  278 */ "sortorder",
  /*  279 */ "grouplist",
  /*  280 */ "expritem",
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
 /* 150 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 151 */ "columnlist ::= columnlist COMMA column",
 /* 152 */ "columnlist ::= column",
 /* 153 */ "column ::= ids typename",
 /* 154 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 155 */ "tagitemlist ::= tagitem",
 /* 156 */ "tagitem ::= INTEGER",
 /* 157 */ "tagitem ::= FLOAT",
 /* 158 */ "tagitem ::= STRING",
 /* 159 */ "tagitem ::= BOOL",
 /* 160 */ "tagitem ::= NULL",
 /* 161 */ "tagitem ::= NOW",
 /* 162 */ "tagitem ::= MINUS INTEGER",
 /* 163 */ "tagitem ::= MINUS FLOAT",
 /* 164 */ "tagitem ::= PLUS INTEGER",
 /* 165 */ "tagitem ::= PLUS FLOAT",
 /* 166 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 167 */ "select ::= LP select RP",
 /* 168 */ "union ::= select",
 /* 169 */ "union ::= union UNION ALL select",
 /* 170 */ "cmd ::= union",
 /* 171 */ "select ::= SELECT selcollist",
 /* 172 */ "sclp ::= selcollist COMMA",
 /* 173 */ "sclp ::=",
 /* 174 */ "selcollist ::= sclp distinct expr as",
 /* 175 */ "selcollist ::= sclp STAR",
 /* 176 */ "as ::= AS ids",
 /* 177 */ "as ::= ids",
 /* 178 */ "as ::=",
 /* 179 */ "distinct ::= DISTINCT",
 /* 180 */ "distinct ::=",
 /* 181 */ "from ::= FROM tablelist",
 /* 182 */ "from ::= FROM sub",
 /* 183 */ "sub ::= LP union RP",
 /* 184 */ "sub ::= LP union RP ids",
 /* 185 */ "sub ::= sub COMMA LP union RP ids",
 /* 186 */ "tablelist ::= ids cpxName",
 /* 187 */ "tablelist ::= ids cpxName ids",
 /* 188 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 189 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 190 */ "tmvar ::= VARIABLE",
 /* 191 */ "timestamp ::= INTEGER",
 /* 192 */ "timestamp ::= MINUS INTEGER",
 /* 193 */ "timestamp ::= PLUS INTEGER",
 /* 194 */ "timestamp ::= STRING",
 /* 195 */ "timestamp ::= NOW",
 /* 196 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 197 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 198 */ "range_option ::=",
 /* 199 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 200 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 201 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 202 */ "interval_option ::=",
 /* 203 */ "intervalKey ::= INTERVAL",
 /* 204 */ "intervalKey ::= EVERY",
 /* 205 */ "session_option ::=",
 /* 206 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 207 */ "windowstate_option ::=",
 /* 208 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 209 */ "fill_opt ::=",
 /* 210 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 211 */ "fill_opt ::= FILL LP ID RP",
 /* 212 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 213 */ "sliding_opt ::=",
 /* 214 */ "orderby_opt ::=",
 /* 215 */ "orderby_opt ::= ORDER BY sortlist",
 /* 216 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 217 */ "sortlist ::= item sortorder",
 /* 218 */ "item ::= ids cpxName",
 /* 219 */ "sortorder ::= ASC",
 /* 220 */ "sortorder ::= DESC",
 /* 221 */ "sortorder ::=",
 /* 222 */ "groupby_opt ::=",
 /* 223 */ "groupby_opt ::= GROUP BY grouplist",
 /* 224 */ "grouplist ::= grouplist COMMA item",
 /* 225 */ "grouplist ::= item",
 /* 226 */ "having_opt ::=",
 /* 227 */ "having_opt ::= HAVING expr",
 /* 228 */ "limit_opt ::=",
 /* 229 */ "limit_opt ::= LIMIT signed",
 /* 230 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 231 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 232 */ "slimit_opt ::=",
 /* 233 */ "slimit_opt ::= SLIMIT signed",
 /* 234 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 235 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 236 */ "where_opt ::=",
 /* 237 */ "where_opt ::= WHERE expr",
 /* 238 */ "expr ::= LP expr RP",
 /* 239 */ "expr ::= ID",
 /* 240 */ "expr ::= ID DOT ID",
 /* 241 */ "expr ::= ID DOT STAR",
 /* 242 */ "expr ::= INTEGER",
 /* 243 */ "expr ::= MINUS INTEGER",
 /* 244 */ "expr ::= PLUS INTEGER",
 /* 245 */ "expr ::= FLOAT",
 /* 246 */ "expr ::= MINUS FLOAT",
 /* 247 */ "expr ::= PLUS FLOAT",
 /* 248 */ "expr ::= STRING",
 /* 249 */ "expr ::= NOW",
 /* 250 */ "expr ::= VARIABLE",
 /* 251 */ "expr ::= PLUS VARIABLE",
 /* 252 */ "expr ::= MINUS VARIABLE",
 /* 253 */ "expr ::= BOOL",
 /* 254 */ "expr ::= NULL",
 /* 255 */ "expr ::= ID LP exprlist RP",
 /* 256 */ "expr ::= ID LP STAR RP",
 /* 257 */ "expr ::= ID LP expr AS typename RP",
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
    case 207: /* exprlist */
    case 251: /* selcollist */
    case 266: /* sclp */
{
tSqlExprListDestroy((yypminor->yy247));
}
      break;
    case 222: /* intitemlist */
    case 224: /* keep */
    case 245: /* columnlist */
    case 246: /* tagitemlist */
    case 247: /* tagNamelist */
    case 259: /* fill_opt */
    case 260: /* groupby_opt */
    case 262: /* orderby_opt */
    case 275: /* sortlist */
    case 279: /* grouplist */
{
taosArrayDestroy((yypminor->yy247));
}
      break;
    case 243: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy336));
}
      break;
    case 248: /* select */
{
destroySqlNode((yypminor->yy246));
}
      break;
    case 252: /* from */
    case 270: /* tablelist */
    case 271: /* sub */
{
destroyRelationInfo((yypminor->yy46));
}
      break;
    case 253: /* where_opt */
    case 261: /* having_opt */
    case 268: /* expr */
    case 273: /* timestamp */
    case 280: /* expritem */
{
tSqlExprDestroy((yypminor->yy44));
}
      break;
    case 265: /* union */
{
destroyAllSqlNode((yypminor->yy247));
}
      break;
    case 276: /* sortitem */
{
tVariantDestroy(&(yypminor->yy378));
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
   198,  /* (0) program ::= cmd */
   199,  /* (1) cmd ::= SHOW DATABASES */
   199,  /* (2) cmd ::= SHOW TOPICS */
   199,  /* (3) cmd ::= SHOW FUNCTIONS */
   199,  /* (4) cmd ::= SHOW MNODES */
   199,  /* (5) cmd ::= SHOW DNODES */
   199,  /* (6) cmd ::= SHOW ACCOUNTS */
   199,  /* (7) cmd ::= SHOW USERS */
   199,  /* (8) cmd ::= SHOW MODULES */
   199,  /* (9) cmd ::= SHOW QUERIES */
   199,  /* (10) cmd ::= SHOW CONNECTIONS */
   199,  /* (11) cmd ::= SHOW STREAMS */
   199,  /* (12) cmd ::= SHOW VARIABLES */
   199,  /* (13) cmd ::= SHOW SCORES */
   199,  /* (14) cmd ::= SHOW GRANTS */
   199,  /* (15) cmd ::= SHOW VNODES */
   199,  /* (16) cmd ::= SHOW VNODES ids */
   201,  /* (17) dbPrefix ::= */
   201,  /* (18) dbPrefix ::= ids DOT */
   202,  /* (19) cpxName ::= */
   202,  /* (20) cpxName ::= DOT ids */
   199,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   199,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   199,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   199,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   199,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   199,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   199,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   199,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   199,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   199,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   199,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   199,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   199,  /* (33) cmd ::= DROP FUNCTION ids */
   199,  /* (34) cmd ::= DROP DNODE ids */
   199,  /* (35) cmd ::= DROP USER ids */
   199,  /* (36) cmd ::= DROP ACCOUNT ids */
   199,  /* (37) cmd ::= USE ids */
   199,  /* (38) cmd ::= DESCRIBE ids cpxName */
   199,  /* (39) cmd ::= DESC ids cpxName */
   199,  /* (40) cmd ::= ALTER USER ids PASS ids */
   199,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   199,  /* (42) cmd ::= ALTER DNODE ids ids */
   199,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   199,  /* (44) cmd ::= ALTER LOCAL ids */
   199,  /* (45) cmd ::= ALTER LOCAL ids ids */
   199,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   199,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   199,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   199,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   199,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   200,  /* (51) ids ::= ID */
   200,  /* (52) ids ::= STRING */
   203,  /* (53) ifexists ::= IF EXISTS */
   203,  /* (54) ifexists ::= */
   208,  /* (55) ifnotexists ::= IF NOT EXISTS */
   208,  /* (56) ifnotexists ::= */
   199,  /* (57) cmd ::= CREATE DNODE ids */
   199,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   199,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   199,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   199,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   199,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   199,  /* (63) cmd ::= CREATE USER ids PASS ids */
   212,  /* (64) bufsize ::= */
   212,  /* (65) bufsize ::= BUFSIZE INTEGER */
   213,  /* (66) pps ::= */
   213,  /* (67) pps ::= PPS INTEGER */
   214,  /* (68) tseries ::= */
   214,  /* (69) tseries ::= TSERIES INTEGER */
   215,  /* (70) dbs ::= */
   215,  /* (71) dbs ::= DBS INTEGER */
   216,  /* (72) streams ::= */
   216,  /* (73) streams ::= STREAMS INTEGER */
   217,  /* (74) storage ::= */
   217,  /* (75) storage ::= STORAGE INTEGER */
   218,  /* (76) qtime ::= */
   218,  /* (77) qtime ::= QTIME INTEGER */
   219,  /* (78) users ::= */
   219,  /* (79) users ::= USERS INTEGER */
   220,  /* (80) conns ::= */
   220,  /* (81) conns ::= CONNS INTEGER */
   221,  /* (82) state ::= */
   221,  /* (83) state ::= STATE ids */
   206,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   222,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   222,  /* (86) intitemlist ::= intitem */
   223,  /* (87) intitem ::= INTEGER */
   224,  /* (88) keep ::= KEEP intitemlist */
   225,  /* (89) cache ::= CACHE INTEGER */
   226,  /* (90) replica ::= REPLICA INTEGER */
   227,  /* (91) quorum ::= QUORUM INTEGER */
   228,  /* (92) days ::= DAYS INTEGER */
   229,  /* (93) minrows ::= MINROWS INTEGER */
   230,  /* (94) maxrows ::= MAXROWS INTEGER */
   231,  /* (95) blocks ::= BLOCKS INTEGER */
   232,  /* (96) ctime ::= CTIME INTEGER */
   233,  /* (97) wal ::= WAL INTEGER */
   234,  /* (98) fsync ::= FSYNC INTEGER */
   235,  /* (99) comp ::= COMP INTEGER */
   236,  /* (100) prec ::= PRECISION STRING */
   237,  /* (101) update ::= UPDATE INTEGER */
   238,  /* (102) cachelast ::= CACHELAST INTEGER */
   239,  /* (103) partitions ::= PARTITIONS INTEGER */
   209,  /* (104) db_optr ::= */
   209,  /* (105) db_optr ::= db_optr cache */
   209,  /* (106) db_optr ::= db_optr replica */
   209,  /* (107) db_optr ::= db_optr quorum */
   209,  /* (108) db_optr ::= db_optr days */
   209,  /* (109) db_optr ::= db_optr minrows */
   209,  /* (110) db_optr ::= db_optr maxrows */
   209,  /* (111) db_optr ::= db_optr blocks */
   209,  /* (112) db_optr ::= db_optr ctime */
   209,  /* (113) db_optr ::= db_optr wal */
   209,  /* (114) db_optr ::= db_optr fsync */
   209,  /* (115) db_optr ::= db_optr comp */
   209,  /* (116) db_optr ::= db_optr prec */
   209,  /* (117) db_optr ::= db_optr keep */
   209,  /* (118) db_optr ::= db_optr update */
   209,  /* (119) db_optr ::= db_optr cachelast */
   210,  /* (120) topic_optr ::= db_optr */
   210,  /* (121) topic_optr ::= topic_optr partitions */
   204,  /* (122) alter_db_optr ::= */
   204,  /* (123) alter_db_optr ::= alter_db_optr replica */
   204,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   204,  /* (125) alter_db_optr ::= alter_db_optr keep */
   204,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   204,  /* (127) alter_db_optr ::= alter_db_optr comp */
   204,  /* (128) alter_db_optr ::= alter_db_optr update */
   204,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   205,  /* (130) alter_topic_optr ::= alter_db_optr */
   205,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   211,  /* (132) typename ::= ids */
   211,  /* (133) typename ::= ids LP signed RP */
   211,  /* (134) typename ::= ids UNSIGNED */
   240,  /* (135) signed ::= INTEGER */
   240,  /* (136) signed ::= PLUS INTEGER */
   240,  /* (137) signed ::= MINUS INTEGER */
   199,  /* (138) cmd ::= CREATE TABLE create_table_args */
   199,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   199,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   199,  /* (141) cmd ::= CREATE TABLE create_table_list */
   243,  /* (142) create_table_list ::= create_from_stable */
   243,  /* (143) create_table_list ::= create_table_list create_from_stable */
   241,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   242,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   244,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   244,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   247,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   247,  /* (149) tagNamelist ::= ids */
   241,  /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
   245,  /* (151) columnlist ::= columnlist COMMA column */
   245,  /* (152) columnlist ::= column */
   249,  /* (153) column ::= ids typename */
   246,  /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
   246,  /* (155) tagitemlist ::= tagitem */
   250,  /* (156) tagitem ::= INTEGER */
   250,  /* (157) tagitem ::= FLOAT */
   250,  /* (158) tagitem ::= STRING */
   250,  /* (159) tagitem ::= BOOL */
   250,  /* (160) tagitem ::= NULL */
   250,  /* (161) tagitem ::= NOW */
   250,  /* (162) tagitem ::= MINUS INTEGER */
   250,  /* (163) tagitem ::= MINUS FLOAT */
   250,  /* (164) tagitem ::= PLUS INTEGER */
   250,  /* (165) tagitem ::= PLUS FLOAT */
   248,  /* (166) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   248,  /* (167) select ::= LP select RP */
   265,  /* (168) union ::= select */
   265,  /* (169) union ::= union UNION ALL select */
   199,  /* (170) cmd ::= union */
   248,  /* (171) select ::= SELECT selcollist */
   266,  /* (172) sclp ::= selcollist COMMA */
   266,  /* (173) sclp ::= */
   251,  /* (174) selcollist ::= sclp distinct expr as */
   251,  /* (175) selcollist ::= sclp STAR */
   269,  /* (176) as ::= AS ids */
   269,  /* (177) as ::= ids */
   269,  /* (178) as ::= */
   267,  /* (179) distinct ::= DISTINCT */
   267,  /* (180) distinct ::= */
   252,  /* (181) from ::= FROM tablelist */
   252,  /* (182) from ::= FROM sub */
   271,  /* (183) sub ::= LP union RP */
   271,  /* (184) sub ::= LP union RP ids */
   271,  /* (185) sub ::= sub COMMA LP union RP ids */
   270,  /* (186) tablelist ::= ids cpxName */
   270,  /* (187) tablelist ::= ids cpxName ids */
   270,  /* (188) tablelist ::= tablelist COMMA ids cpxName */
   270,  /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
   272,  /* (190) tmvar ::= VARIABLE */
   273,  /* (191) timestamp ::= INTEGER */
   273,  /* (192) timestamp ::= MINUS INTEGER */
   273,  /* (193) timestamp ::= PLUS INTEGER */
   273,  /* (194) timestamp ::= STRING */
   273,  /* (195) timestamp ::= NOW */
   273,  /* (196) timestamp ::= NOW PLUS VARIABLE */
   273,  /* (197) timestamp ::= NOW MINUS VARIABLE */
   254,  /* (198) range_option ::= */
   254,  /* (199) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   255,  /* (200) interval_option ::= intervalKey LP tmvar RP */
   255,  /* (201) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   255,  /* (202) interval_option ::= */
   274,  /* (203) intervalKey ::= INTERVAL */
   274,  /* (204) intervalKey ::= EVERY */
   257,  /* (205) session_option ::= */
   257,  /* (206) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   258,  /* (207) windowstate_option ::= */
   258,  /* (208) windowstate_option ::= STATE_WINDOW LP ids RP */
   259,  /* (209) fill_opt ::= */
   259,  /* (210) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   259,  /* (211) fill_opt ::= FILL LP ID RP */
   256,  /* (212) sliding_opt ::= SLIDING LP tmvar RP */
   256,  /* (213) sliding_opt ::= */
   262,  /* (214) orderby_opt ::= */
   262,  /* (215) orderby_opt ::= ORDER BY sortlist */
   275,  /* (216) sortlist ::= sortlist COMMA item sortorder */
   275,  /* (217) sortlist ::= item sortorder */
   277,  /* (218) item ::= ids cpxName */
   278,  /* (219) sortorder ::= ASC */
   278,  /* (220) sortorder ::= DESC */
   278,  /* (221) sortorder ::= */
   260,  /* (222) groupby_opt ::= */
   260,  /* (223) groupby_opt ::= GROUP BY grouplist */
   279,  /* (224) grouplist ::= grouplist COMMA item */
   279,  /* (225) grouplist ::= item */
   261,  /* (226) having_opt ::= */
   261,  /* (227) having_opt ::= HAVING expr */
   264,  /* (228) limit_opt ::= */
   264,  /* (229) limit_opt ::= LIMIT signed */
   264,  /* (230) limit_opt ::= LIMIT signed OFFSET signed */
   264,  /* (231) limit_opt ::= LIMIT signed COMMA signed */
   263,  /* (232) slimit_opt ::= */
   263,  /* (233) slimit_opt ::= SLIMIT signed */
   263,  /* (234) slimit_opt ::= SLIMIT signed SOFFSET signed */
   263,  /* (235) slimit_opt ::= SLIMIT signed COMMA signed */
   253,  /* (236) where_opt ::= */
   253,  /* (237) where_opt ::= WHERE expr */
   268,  /* (238) expr ::= LP expr RP */
   268,  /* (239) expr ::= ID */
   268,  /* (240) expr ::= ID DOT ID */
   268,  /* (241) expr ::= ID DOT STAR */
   268,  /* (242) expr ::= INTEGER */
   268,  /* (243) expr ::= MINUS INTEGER */
   268,  /* (244) expr ::= PLUS INTEGER */
   268,  /* (245) expr ::= FLOAT */
   268,  /* (246) expr ::= MINUS FLOAT */
   268,  /* (247) expr ::= PLUS FLOAT */
   268,  /* (248) expr ::= STRING */
   268,  /* (249) expr ::= NOW */
   268,  /* (250) expr ::= VARIABLE */
   268,  /* (251) expr ::= PLUS VARIABLE */
   268,  /* (252) expr ::= MINUS VARIABLE */
   268,  /* (253) expr ::= BOOL */
   268,  /* (254) expr ::= NULL */
   268,  /* (255) expr ::= ID LP exprlist RP */
   268,  /* (256) expr ::= ID LP STAR RP */
   268,  /* (257) expr ::= ID LP expr AS typename RP */
   268,  /* (258) expr ::= expr IS NULL */
   268,  /* (259) expr ::= expr IS NOT NULL */
   268,  /* (260) expr ::= expr LT expr */
   268,  /* (261) expr ::= expr GT expr */
   268,  /* (262) expr ::= expr LE expr */
   268,  /* (263) expr ::= expr GE expr */
   268,  /* (264) expr ::= expr NE expr */
   268,  /* (265) expr ::= expr EQ expr */
   268,  /* (266) expr ::= expr BETWEEN expr AND expr */
   268,  /* (267) expr ::= expr AND expr */
   268,  /* (268) expr ::= expr OR expr */
   268,  /* (269) expr ::= expr PLUS expr */
   268,  /* (270) expr ::= expr MINUS expr */
   268,  /* (271) expr ::= expr STAR expr */
   268,  /* (272) expr ::= expr SLASH expr */
   268,  /* (273) expr ::= expr REM expr */
   268,  /* (274) expr ::= expr LIKE expr */
   268,  /* (275) expr ::= expr MATCH expr */
   268,  /* (276) expr ::= expr NMATCH expr */
   268,  /* (277) expr ::= expr IN LP exprlist RP */
   207,  /* (278) exprlist ::= exprlist COMMA expritem */
   207,  /* (279) exprlist ::= expritem */
   280,  /* (280) expritem ::= expr */
   280,  /* (281) expritem ::= */
   199,  /* (282) cmd ::= RESET QUERY CACHE */
   199,  /* (283) cmd ::= SYNCDB ids REPLICA */
   199,  /* (284) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   199,  /* (285) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   199,  /* (286) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   199,  /* (287) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   199,  /* (288) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   199,  /* (289) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   199,  /* (290) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   199,  /* (291) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   199,  /* (292) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   199,  /* (293) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   199,  /* (294) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   199,  /* (295) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   199,  /* (296) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   199,  /* (297) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   199,  /* (298) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   199,  /* (299) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   199,  /* (300) cmd ::= KILL CONNECTION INTEGER */
   199,  /* (301) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   199,  /* (302) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (151) columnlist ::= columnlist COMMA column */
   -1,  /* (152) columnlist ::= column */
   -2,  /* (153) column ::= ids typename */
   -3,  /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (155) tagitemlist ::= tagitem */
   -1,  /* (156) tagitem ::= INTEGER */
   -1,  /* (157) tagitem ::= FLOAT */
   -1,  /* (158) tagitem ::= STRING */
   -1,  /* (159) tagitem ::= BOOL */
   -1,  /* (160) tagitem ::= NULL */
   -1,  /* (161) tagitem ::= NOW */
   -2,  /* (162) tagitem ::= MINUS INTEGER */
   -2,  /* (163) tagitem ::= MINUS FLOAT */
   -2,  /* (164) tagitem ::= PLUS INTEGER */
   -2,  /* (165) tagitem ::= PLUS FLOAT */
  -15,  /* (166) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (167) select ::= LP select RP */
   -1,  /* (168) union ::= select */
   -4,  /* (169) union ::= union UNION ALL select */
   -1,  /* (170) cmd ::= union */
   -2,  /* (171) select ::= SELECT selcollist */
   -2,  /* (172) sclp ::= selcollist COMMA */
    0,  /* (173) sclp ::= */
   -4,  /* (174) selcollist ::= sclp distinct expr as */
   -2,  /* (175) selcollist ::= sclp STAR */
   -2,  /* (176) as ::= AS ids */
   -1,  /* (177) as ::= ids */
    0,  /* (178) as ::= */
   -1,  /* (179) distinct ::= DISTINCT */
    0,  /* (180) distinct ::= */
   -2,  /* (181) from ::= FROM tablelist */
   -2,  /* (182) from ::= FROM sub */
   -3,  /* (183) sub ::= LP union RP */
   -4,  /* (184) sub ::= LP union RP ids */
   -6,  /* (185) sub ::= sub COMMA LP union RP ids */
   -2,  /* (186) tablelist ::= ids cpxName */
   -3,  /* (187) tablelist ::= ids cpxName ids */
   -4,  /* (188) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (190) tmvar ::= VARIABLE */
   -1,  /* (191) timestamp ::= INTEGER */
   -2,  /* (192) timestamp ::= MINUS INTEGER */
   -2,  /* (193) timestamp ::= PLUS INTEGER */
   -1,  /* (194) timestamp ::= STRING */
   -1,  /* (195) timestamp ::= NOW */
   -3,  /* (196) timestamp ::= NOW PLUS VARIABLE */
   -3,  /* (197) timestamp ::= NOW MINUS VARIABLE */
    0,  /* (198) range_option ::= */
   -6,  /* (199) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   -4,  /* (200) interval_option ::= intervalKey LP tmvar RP */
   -6,  /* (201) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
    0,  /* (202) interval_option ::= */
   -1,  /* (203) intervalKey ::= INTERVAL */
   -1,  /* (204) intervalKey ::= EVERY */
    0,  /* (205) session_option ::= */
   -7,  /* (206) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (207) windowstate_option ::= */
   -4,  /* (208) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (209) fill_opt ::= */
   -6,  /* (210) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (211) fill_opt ::= FILL LP ID RP */
   -4,  /* (212) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (213) sliding_opt ::= */
    0,  /* (214) orderby_opt ::= */
   -3,  /* (215) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (216) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (217) sortlist ::= item sortorder */
   -2,  /* (218) item ::= ids cpxName */
   -1,  /* (219) sortorder ::= ASC */
   -1,  /* (220) sortorder ::= DESC */
    0,  /* (221) sortorder ::= */
    0,  /* (222) groupby_opt ::= */
   -3,  /* (223) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (224) grouplist ::= grouplist COMMA item */
   -1,  /* (225) grouplist ::= item */
    0,  /* (226) having_opt ::= */
   -2,  /* (227) having_opt ::= HAVING expr */
    0,  /* (228) limit_opt ::= */
   -2,  /* (229) limit_opt ::= LIMIT signed */
   -4,  /* (230) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (231) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (232) slimit_opt ::= */
   -2,  /* (233) slimit_opt ::= SLIMIT signed */
   -4,  /* (234) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (235) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (236) where_opt ::= */
   -2,  /* (237) where_opt ::= WHERE expr */
   -3,  /* (238) expr ::= LP expr RP */
   -1,  /* (239) expr ::= ID */
   -3,  /* (240) expr ::= ID DOT ID */
   -3,  /* (241) expr ::= ID DOT STAR */
   -1,  /* (242) expr ::= INTEGER */
   -2,  /* (243) expr ::= MINUS INTEGER */
   -2,  /* (244) expr ::= PLUS INTEGER */
   -1,  /* (245) expr ::= FLOAT */
   -2,  /* (246) expr ::= MINUS FLOAT */
   -2,  /* (247) expr ::= PLUS FLOAT */
   -1,  /* (248) expr ::= STRING */
   -1,  /* (249) expr ::= NOW */
   -1,  /* (250) expr ::= VARIABLE */
   -2,  /* (251) expr ::= PLUS VARIABLE */
   -2,  /* (252) expr ::= MINUS VARIABLE */
   -1,  /* (253) expr ::= BOOL */
   -1,  /* (254) expr ::= NULL */
   -4,  /* (255) expr ::= ID LP exprlist RP */
   -4,  /* (256) expr ::= ID LP STAR RP */
   -6,  /* (257) expr ::= ID LP expr AS typename RP */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy47);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy247);}
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
      case 180: /* distinct ::= */ yytestcase(yyruleno==180);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy179, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy179, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy47.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy47.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy47.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy47.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy47.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy47.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy47.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy47 = yylhsminor.yy47;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
{ yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);    }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1); }
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0, true); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy247 = yymsp[0].minor.yy247; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy262); yymsp[1].minor.yy262.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.keep = yymsp[0].minor.yy247; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy262 = yymsp[0].minor.yy262; yylhsminor.yy262.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy262 = yylhsminor.yy262;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy262); yymsp[1].minor.yy262.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy179, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy179 = yylhsminor.yy179;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy403 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy403;  // negative value of name length
    tSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy179 = yylhsminor.yy179;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy179, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy403 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy336;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy42);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy336 = pCreateTable;
}
  yymsp[0].minor.yy336 = yylhsminor.yy336;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy336->childTableInfo, &yymsp[0].minor.yy42);
  yylhsminor.yy336 = yymsp[-1].minor.yy336;
}
  yymsp[-1].minor.yy336 = yylhsminor.yy336;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy336 = tSetCreateTableInfo(yymsp[-1].minor.yy247, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy336, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy336 = yylhsminor.yy336;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy336 = tSetCreateTableInfo(yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy336, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy336 = yylhsminor.yy336;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy247, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy42 = yylhsminor.yy42;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy42 = yylhsminor.yy42;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy0); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy336 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy246, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy336, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy336 = yylhsminor.yy336;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy179); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 152: /* columnlist ::= column */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy179);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 153: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy179, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy179);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 160: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0, true); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 161: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0, true);}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 162: /* tagitem ::= MINUS INTEGER */
      case 163: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==165);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0, true);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 166: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy246 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy247, yymsp[-12].minor.yy46, yymsp[-11].minor.yy44, yymsp[-4].minor.yy247, yymsp[-2].minor.yy247, &yymsp[-9].minor.yy430, &yymsp[-7].minor.yy507, &yymsp[-6].minor.yy492, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy247, &yymsp[0].minor.yy204, &yymsp[-1].minor.yy204, yymsp[-3].minor.yy44, &yymsp[-10].minor.yy379);
}
  yymsp[-14].minor.yy246 = yylhsminor.yy246;
        break;
      case 167: /* select ::= LP select RP */
{yymsp[-2].minor.yy246 = yymsp[-1].minor.yy246;}
        break;
      case 168: /* union ::= select */
{ yylhsminor.yy247 = setSubclause(NULL, yymsp[0].minor.yy246); }
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 169: /* union ::= union UNION ALL select */
{ yylhsminor.yy247 = appendSelectClause(yymsp[-3].minor.yy247, yymsp[0].minor.yy246); }
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 170: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy247, NULL, TSDB_SQL_SELECT); }
        break;
      case 171: /* select ::= SELECT selcollist */
{
  yylhsminor.yy246 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy247, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 172: /* sclp ::= selcollist COMMA */
{yylhsminor.yy247 = yymsp[-1].minor.yy247;}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 173: /* sclp ::= */
      case 214: /* orderby_opt ::= */ yytestcase(yyruleno==214);
{yymsp[1].minor.yy247 = 0;}
        break;
      case 174: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy247 = tSqlExprListAppend(yymsp[-3].minor.yy247, yymsp[-1].minor.yy44,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 175: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy247 = tSqlExprListAppend(yymsp[-1].minor.yy247, pNode, 0, 0);
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 176: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 177: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 179: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* from ::= FROM tablelist */
      case 182: /* from ::= FROM sub */ yytestcase(yyruleno==182);
{yymsp[-1].minor.yy46 = yymsp[0].minor.yy46;}
        break;
      case 183: /* sub ::= LP union RP */
{yymsp[-2].minor.yy46 = addSubqueryElem(NULL, yymsp[-1].minor.yy247, NULL);}
        break;
      case 184: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy46 = addSubqueryElem(NULL, yymsp[-2].minor.yy247, &yymsp[0].minor.yy0);}
        break;
      case 185: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy46 = addSubqueryElem(yymsp[-5].minor.yy46, yymsp[-2].minor.yy247, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy46 = yylhsminor.yy46;
        break;
      case 186: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 187: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 188: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(yymsp[-3].minor.yy46, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy46 = setTableNameList(yymsp[-4].minor.yy46, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 190: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 191: /* timestamp ::= INTEGER */
{ yylhsminor.yy44 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 192: /* timestamp ::= MINUS INTEGER */
      case 193: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==193);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy44 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 194: /* timestamp ::= STRING */
{ yylhsminor.yy44 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 195: /* timestamp ::= NOW */
{ yylhsminor.yy44 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 196: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy44 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 197: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy44 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 198: /* range_option ::= */
{yymsp[1].minor.yy379.start = 0; yymsp[1].minor.yy379.end = 0;}
        break;
      case 199: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy379.start = yymsp[-3].minor.yy44; yymsp[-5].minor.yy379.end = yymsp[-1].minor.yy44;}
        break;
      case 200: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy430.interval = yymsp[-1].minor.yy0; yylhsminor.yy430.offset.n = 0; yylhsminor.yy430.token = yymsp[-3].minor.yy2;}
  yymsp[-3].minor.yy430 = yylhsminor.yy430;
        break;
      case 201: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy430.interval = yymsp[-3].minor.yy0; yylhsminor.yy430.offset = yymsp[-1].minor.yy0;   yylhsminor.yy430.token = yymsp[-5].minor.yy2;}
  yymsp[-5].minor.yy430 = yylhsminor.yy430;
        break;
      case 202: /* interval_option ::= */
{memset(&yymsp[1].minor.yy430, 0, sizeof(yymsp[1].minor.yy430));}
        break;
      case 203: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy2 = TK_INTERVAL;}
        break;
      case 204: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy2 = TK_EVERY;   }
        break;
      case 205: /* session_option ::= */
{yymsp[1].minor.yy507.col.n = 0; yymsp[1].minor.yy507.gap.n = 0;}
        break;
      case 206: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy507.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy507.gap = yymsp[-1].minor.yy0;
}
        break;
      case 207: /* windowstate_option ::= */
{ yymsp[1].minor.yy492.col.n = 0; yymsp[1].minor.yy492.col.z = NULL;}
        break;
      case 208: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy492.col = yymsp[-1].minor.yy0; }
        break;
      case 209: /* fill_opt ::= */
{ yymsp[1].minor.yy247 = 0;     }
        break;
      case 210: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0, true);

    tVariantListInsert(yymsp[-1].minor.yy247, &A, -1, 0);
    yymsp[-5].minor.yy247 = yymsp[-1].minor.yy247;
}
        break;
      case 211: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy247 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 212: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 213: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 215: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 216: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy247 = tVariantListAppend(yymsp[-3].minor.yy247, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 217: /* sortlist ::= item sortorder */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 218: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0, true);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 219: /* sortorder ::= ASC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 220: /* sortorder ::= DESC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_DESC;}
        break;
      case 221: /* sortorder ::= */
{ yymsp[1].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 222: /* groupby_opt ::= */
{ yymsp[1].minor.yy247 = 0;}
        break;
      case 223: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 224: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);
}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 225: /* grouplist ::= item */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1);
}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 226: /* having_opt ::= */
      case 236: /* where_opt ::= */ yytestcase(yyruleno==236);
      case 281: /* expritem ::= */ yytestcase(yyruleno==281);
{yymsp[1].minor.yy44 = 0;}
        break;
      case 227: /* having_opt ::= HAVING expr */
      case 237: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==237);
{yymsp[-1].minor.yy44 = yymsp[0].minor.yy44;}
        break;
      case 228: /* limit_opt ::= */
      case 232: /* slimit_opt ::= */ yytestcase(yyruleno==232);
{yymsp[1].minor.yy204.limit = -1; yymsp[1].minor.yy204.offset = 0;}
        break;
      case 229: /* limit_opt ::= LIMIT signed */
      case 233: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==233);
{yymsp[-1].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-1].minor.yy204.offset = 0;}
        break;
      case 230: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 231: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 234: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 235: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 238: /* expr ::= LP expr RP */
{yylhsminor.yy44 = yymsp[-1].minor.yy44; yylhsminor.yy44->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy44->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 239: /* expr ::= ID */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 240: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 241: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 242: /* expr ::= INTEGER */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 243: /* expr ::= MINUS INTEGER */
      case 244: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==244);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 245: /* expr ::= FLOAT */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 246: /* expr ::= MINUS FLOAT */
      case 247: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==247);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 248: /* expr ::= STRING */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 249: /* expr ::= NOW */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 250: /* expr ::= VARIABLE */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 251: /* expr ::= PLUS VARIABLE */
      case 252: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==252);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy44 = yylhsminor.yy44;
        break;
      case 253: /* expr ::= BOOL */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 254: /* expr ::= NULL */
{ yylhsminor.yy44 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
        break;
      case 255: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy44 = tSqlExprCreateFunction(yymsp[-1].minor.yy247, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy44 = yylhsminor.yy44;
        break;
      case 256: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy44 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy44 = yylhsminor.yy44;
        break;
      case 257: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy44 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy44, &yymsp[-1].minor.yy179, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy44 = yylhsminor.yy44;
        break;
      case 258: /* expr ::= expr IS NULL */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 259: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-3].minor.yy44, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy44 = yylhsminor.yy44;
        break;
      case 260: /* expr ::= expr LT expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_LT);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 261: /* expr ::= expr GT expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_GT);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 262: /* expr ::= expr LE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_LE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 263: /* expr ::= expr GE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_GE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 264: /* expr ::= expr NE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_NE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 265: /* expr ::= expr EQ expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_EQ);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 266: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy44); yylhsminor.yy44 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy44, yymsp[-2].minor.yy44, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy44, TK_LE), TK_AND);}
  yymsp[-4].minor.yy44 = yylhsminor.yy44;
        break;
      case 267: /* expr ::= expr AND expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_AND);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 268: /* expr ::= expr OR expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_OR); }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 269: /* expr ::= expr PLUS expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_PLUS);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 270: /* expr ::= expr MINUS expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_MINUS); }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 271: /* expr ::= expr STAR expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_STAR);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 272: /* expr ::= expr SLASH expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_DIVIDE);}
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 273: /* expr ::= expr REM expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_REM);   }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 274: /* expr ::= expr LIKE expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_LIKE);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 275: /* expr ::= expr MATCH expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_MATCH);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 276: /* expr ::= expr NMATCH expr */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TK_NMATCH);  }
  yymsp[-2].minor.yy44 = yylhsminor.yy44;
        break;
      case 277: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy44 = tSqlExprCreate(yymsp[-4].minor.yy44, (tSqlExpr*)yymsp[-1].minor.yy247, TK_IN); }
  yymsp[-4].minor.yy44 = yylhsminor.yy44;
        break;
      case 278: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy247 = tSqlExprListAppend(yymsp[-2].minor.yy247,yymsp[0].minor.yy44,0, 0);}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 279: /* exprlist ::= expritem */
{yylhsminor.yy247 = tSqlExprListAppend(0,yymsp[0].minor.yy44,0, 0);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 280: /* expritem ::= expr */
{yylhsminor.yy44 = yymsp[0].minor.yy44;}
  yymsp[0].minor.yy44 = yylhsminor.yy44;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, true);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1, true);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, true);
    A = tVariantListAppend(A, &yymsp[0].minor.yy378, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, true);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, true);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1, true);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 298: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, true);
    A = tVariantListAppend(A, &yymsp[0].minor.yy378, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 299: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
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
