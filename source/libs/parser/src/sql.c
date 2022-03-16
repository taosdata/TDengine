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

#include "nodes.h"
#include "parToken.h"
#include "ttokendef.h"
#include "parAst.h"
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
#define YYCODETYPE unsigned char
#define YYNOCODE 248
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SNode* yy168;
  SDataType yy208;
  EJoinType yy228;
  SToken yy241;
  EOrder yy258;
  EFillMode yy262;
  int32_t yy324;
  SAlterOption yy349;
  ENullOrder yy425;
  SNodeList* yy440;
  bool yy457;
  EOperatorType yy476;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  SAstCreateContext* pCxt ;
#define ParseARG_PDECL , SAstCreateContext* pCxt 
#define ParseARG_PARAM ,pCxt 
#define ParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define ParseARG_STORE yypParser->pCxt =pCxt ;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYNSTATE             400
#define YYNRULE              305
#define YYNTOKEN             160
#define YY_MAX_SHIFT         399
#define YY_MIN_SHIFTREDUCE   608
#define YY_MAX_SHIFTREDUCE   912
#define YY_ERROR_ACTION      913
#define YY_ACCEPT_ACTION     914
#define YY_NO_ACTION         915
#define YY_MIN_REDUCE        916
#define YY_MAX_REDUCE        1220
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
#define YY_ACTTAB_COUNT (1150)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   951, 1087, 1087,   42,  297,  914, 1199,  208,   24,  140,
 /*    10 */  1070,  312,   79,   31,   29,   27,   26,   25,  916, 1198,
 /*    20 */  1010,   94,  917, 1197,  196,  211,   89,   31,   29,   27,
 /*    30 */    26,   25,  213,  352, 1018, 1061, 1063, 1085, 1085,   77,
 /*    40 */    76,   75,   74,   73,   72,   71,   70,   69,   68,  226,
 /*    50 */   991,  190,  339,  338,  337,  336,  335,  334,  333,  332,
 /*    60 */   331,  330,  329,  328,  327,  326,  325,  324,  323,  322,
 /*    70 */   321, 1199,  100,   30,   28,  795,   31,   29,   27,   26,
 /*    80 */    25,  205,  352, 1053,  107,  818,   10, 1060, 1197,  297,
 /*    90 */   781,  353,  278,  195,  774, 1071,  242,  989, 1058,  243,
 /*   100 */   772,   77,   76,   75,   74,   73,   72,   71,   70,   69,
 /*   110 */    68,   12, 1015,   82,  190,   31,   29,   27,   26,   25,
 /*   120 */    23,  203,  243,  813,  814,  815,  816,  817,  819,  821,
 /*   130 */   822,  823,  773,   80,    1,  774,   31,   29,   27,   26,
 /*   140 */    25,  772,  280,  103, 1145, 1146,  320, 1150,  818,  713,
 /*   150 */   375,  374,  373,  717,  372,  719,  720,  371,  722,  368,
 /*   160 */   398,  728,  365,  730,  731,  362,  359,   97,  980, 1060,
 /*   170 */   908,  909,  854,  773,  352,  210,  783, 1098,  775,  778,
 /*   180 */  1058,    9,    8,   23,  203,  272,  813,  814,  815,  816,
 /*   190 */   817,  819,  821,  822,  823,  108, 1113,  385,  795,  380,
 /*   200 */   209,  398,  384,  294,  108,  383, 1060,  381,   89,  296,
 /*   210 */   382,  785,  214, 1085,  252, 1098, 1017, 1058,  282,  775,
 /*   220 */   778,   60, 1099, 1102, 1138,  353,    6,   10,  189, 1134,
 /*   230 */    64,   30,   28,  855, 1113, 1088,  635,  351, 1060,  205,
 /*   240 */  1199,  281,  250,  353,  347,  346, 1015,  296,   64, 1062,
 /*   250 */   253, 1085,  774,  107,  831,  379,   42, 1197,  772,   61,
 /*   260 */  1099, 1102, 1138,  286, 1015,  155,  198, 1134,  102,   12,
 /*   270 */    78, 1085, 1199, 1011, 1113, 1098,  343,  353, 1098,  154,
 /*   280 */   136,  294, 1012, 1157,  850,  107,  259, 1165,  108, 1197,
 /*   290 */   773,   21,    1,  171, 1113,  786, 1045, 1113, 1015,  820,
 /*   300 */   267,  294,  824,  853,  281,   59,  135,  296,  151,  990,
 /*   310 */   296, 1085,  353, 1098, 1085,  850,  282,  307,  398,  179,
 /*   320 */  1099, 1102,   61, 1099, 1102, 1138,  212,  287, 1113,  198,
 /*   330 */  1134,  102, 1113, 1015,   89,  294,  775,  778, 1199,  294,
 /*   340 */   636,  306, 1017,  219,  258,  296,   58,  119, 1098, 1085,
 /*   350 */  1166,  107,  988,  635,  271, 1197,   83,   61, 1099, 1102,
 /*   360 */  1138,  637,  215, 1007,  198, 1134, 1211, 1113,  377,  317,
 /*   370 */    89, 1060,  376,  316,  294, 1172,  108, 1098, 1017,  377,
 /*   380 */   296,  862, 1059,  376, 1085,  285,  221,  783,  218,  217,
 /*   390 */   122,  355,   61, 1099, 1102, 1138, 1113,  378,  318,  198,
 /*   400 */  1134, 1211, 1152,  294, 1006,  946,   30,   28,  378,  296,
 /*   410 */  1195,  784,  317, 1085,  205,  289,  316,  315,  314, 1149,
 /*   420 */  1152,   61, 1099, 1102, 1138, 1152,  353,  774,  198, 1134,
 /*   430 */  1211,  308, 1098,  772,  928,   30,   28, 1148,  386, 1156,
 /*   440 */  1085,  318, 1147,  205,   12,  255,  927, 1015,  353,  926,
 /*   450 */   353, 1113,  925,  152, 1098,  216,  774,  320,  294,   87,
 /*   460 */   315,  314,  772,   57,  296,  773,  278,    1, 1085, 1015,
 /*   470 */  1085, 1015,   51, 1113,  278,   53,   62, 1099, 1102, 1138,
 /*   480 */   294,  290, 1085, 1137, 1134, 1085,  296,   82, 1085, 1008,
 /*   490 */  1085,  918,  278,  398,  773,   82,    7,  313,   62, 1099,
 /*   500 */  1102, 1138,   27,   26,   25,  292, 1134,   80, 1004,  117,
 /*   510 */   293,  775,  778,   82,  101,   80,  876,  104, 1145, 1146,
 /*   520 */   248, 1150,  398,  116,   20,  105, 1145, 1146,  282, 1150,
 /*   530 */    65, 1000,  284,   80,   31,   29,   27,   26,   25, 1002,
 /*   540 */   775,  778,  924,  133, 1145,  277,  266,  276,  923,   43,
 /*   550 */  1199,  998,  114,   30,   28,  295, 1098,  108,   30,   28,
 /*   560 */   231,  205,  922,  107,    9,    8,  205, 1197,   30,   28,
 /*   570 */   981,  264,  273,  268,  774, 1113,  205, 1098, 1085,  774,
 /*   580 */   772,  810,  294,  161, 1085,  772,  159,  113,  296,  774,
 /*   590 */   921,  111, 1085,  137,   22,  772, 1113,  920, 1085,  919,
 /*   600 */    96, 1099, 1102,  294,   31,   29,   27,   26,   25,  296,
 /*   610 */  1054,  127,  773, 1085,    7,  163,  204,  773,  162,    7,
 /*   620 */  1092,  185, 1099, 1102,  781,  125, 1085,  773,  825,    1,
 /*   630 */   911,  912,  238, 1085, 1090, 1085,  792,  283, 1212,  764,
 /*   640 */   398,  130,   32, 1098,  237,  398,  249,  145,  279,  236,
 /*   650 */    32,  235,  302,   32,  150,  398, 1168,  706,  775,  778,
 /*   660 */   941,  143, 1113,  775,  778, 1098,   84, 1114,   85,  294,
 /*   670 */   232,   87,  397,  775,  778,  296,    2,  139,  781, 1085,
 /*   680 */   234,  233,  701,  227, 1113,  395, 1098,   62, 1099, 1102,
 /*   690 */  1138,  294,  939,  388,  738, 1135,   65,  296,  789,  744,
 /*   700 */   743, 1085,   88,  165,  260, 1113,  164, 1098,   85,  185,
 /*   710 */  1099, 1102,  294,   86,   87,  734,   85,  239,  296,  110,
 /*   720 */   228,  782, 1085, 1098,  167,  391, 1113,  166,  176,  357,
 /*   730 */   184, 1099, 1102,  294,  240,  788,  241,   41, 1098,  296,
 /*   740 */   244,  174, 1113, 1085,  115,  175,   95,  787,  251,  294,
 /*   750 */   256,   96, 1099, 1102,  254,  296,  118, 1113,  109, 1085,
 /*   760 */  1098,  274,  202,  786,  294,  257, 1179,  185, 1099, 1102,
 /*   770 */   296,  265, 1169,  300, 1085,  778, 1098,  206,  262, 1113,
 /*   780 */   197, 1098,  185, 1099, 1102,  123,  294,  126, 1178, 1213,
 /*   790 */  1159,    5,  296,  261,   99, 1113, 1085,  275,    4,  131,
 /*   800 */  1113,  850,  294,  129,  183, 1099, 1102,  294,  296, 1098,
 /*   810 */    81,  785, 1085,  296, 1153,   33,  132, 1085,  199,  291,
 /*   820 */   186, 1099, 1102, 1214, 1196,  177, 1099, 1102, 1113,  138,
 /*   830 */   288,   17, 1120, 1098, 1069,  294,  298,  299, 1098, 1068,
 /*   840 */   303,  296,  207,  147,  108, 1085,   52,  304,  305,  170,
 /*   850 */    50,  350, 1113,  187, 1099, 1102,  172, 1113,  390,  294,
 /*   860 */   153, 1098,  310, 1016,  294,  296,  225,  354, 1005, 1085,
 /*   870 */   296,  955,  158,  399, 1085, 1079, 1098,  178, 1099, 1102,
 /*   880 */  1113, 1001,  188, 1099, 1102,  160,  169,  294,   90,   91,
 /*   890 */  1003,  393,   67,  296,  999, 1113,  387, 1085,   92,   93,
 /*   900 */   168,  180,  294,  173,  954, 1110, 1099, 1102,  296,  181,
 /*   910 */  1078, 1077, 1085, 1098,  229,  230, 1098, 1076,  994,  993,
 /*   920 */  1109, 1099, 1102,  385,  953,  380,   38,  950,  384,   37,
 /*   930 */   938,  383, 1113,  381,  933, 1113,  382, 1075, 1066,  294,
 /*   940 */   992,  112,  294,  650,  952,  296, 1098,  949,  296, 1085,
 /*   950 */   246,  245, 1085,  247,  937,  936,  932, 1108, 1099, 1102,
 /*   960 */   193, 1099, 1102, 1074, 1073, 1113,   36, 1065, 1098,  120,
 /*   970 */   121,   44,  294,    3,   32,   14,  124,   39,  296,  875,
 /*   980 */    98,  128, 1085,  270, 1098,  269, 1090, 1113,  869, 1098,
 /*   990 */   192, 1099, 1102,   45,  294,  868,   46,   15,   34,   11,
 /*  1000 */   296,   47,  847, 1113, 1085,  897,  846,  902, 1113,  134,
 /*  1010 */   294,  896,  194, 1099, 1102,  294,  296,  879,  200,   19,
 /*  1020 */  1085,  296,  901,   35,   16, 1085,  106,  900,  191, 1099,
 /*  1030 */  1102,  201,    8,  182, 1099, 1102,  811,  141,   13,  263,
 /*  1040 */   877,  878,  880,  881,  793,  142,   18, 1064,  873,  144,
 /*  1050 */   146,   48,  148,  149,  301,   49,  712,   40,  311,   54,
 /*  1060 */    53,  742,   55,   56,  741,  740,  309,  648,  319,  671,
 /*  1070 */   670,  948,  669,  668,  667,  666,  662,  665,  664,  663,
 /*  1080 */   661,  660,  935,  659,  658,  657,  934,  656,  655,  654,
 /*  1090 */   653,  340,  341,  344,  345,  929,  348,  342,  349,  996,
 /*  1100 */  1089,  156,  157,   66,  356,  735,  220,  360,  363,  366,
 /*  1110 */   727,  726,  369,  725,  749,  748,  724,  747,  995,  679,
 /*  1120 */   678,  947,  677,  222,  676,  942,  675,  674,  940,  223,
 /*  1130 */   358,  389,  732,  361,  224,  729,  723,  364,  931,  367,
 /*  1140 */   394,  392,  930,  396,  776,  915,  721,  370,  915,   63,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     0,  163,  163,  170,  195,  160,  226,  198,  211,  212,
 /*    10 */   201,  183,  179,   12,   13,   14,   15,   16,    0,  239,
 /*    20 */   187,  161,  162,  243,  186,  186,  182,   12,   13,   14,
 /*    30 */    15,   16,  191,   20,  190,  194,  195,  199,  199,   21,
 /*    40 */    22,   23,   24,   25,   26,   27,   28,   29,   30,  204,
 /*    50 */     0,   50,   52,   53,   54,   55,   56,   57,   58,   59,
 /*    60 */    60,   61,   62,   63,   64,   65,   66,   67,   68,   69,
 /*    70 */    70,  226,  181,   12,   13,   74,   12,   13,   14,   15,
 /*    80 */    16,   20,   20,  192,  239,   84,   73,  182,  243,  195,
 /*    90 */    20,  168,  168,  188,   33,  201,  173,    0,  193,   49,
 /*   100 */    39,   21,   22,   23,   24,   25,   26,   27,   28,   29,
 /*   110 */    30,   50,  189,  189,   50,   12,   13,   14,   15,   16,
 /*   120 */   119,  120,   49,  122,  123,  124,  125,  126,  127,  128,
 /*   130 */   129,  130,   71,  209,   73,   33,   12,   13,   14,   15,
 /*   140 */    16,   39,  218,  219,  220,  221,   49,  223,   84,   90,
 /*   150 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   160 */    99,  102,  103,  104,  105,  106,  107,  171,  172,  182,
 /*   170 */   155,  156,    4,   71,   20,  188,   20,  163,  117,  118,
 /*   180 */   193,    1,    2,  119,  120,   20,  122,  123,  124,  125,
 /*   190 */   126,  127,  128,  129,  130,  134,  182,   52,   74,   54,
 /*   200 */   174,   99,   57,  189,  134,   60,  182,   62,  182,  195,
 /*   210 */    65,   20,  188,  199,  168,  163,  190,  193,  204,  117,
 /*   220 */   118,  207,  208,  209,  210,  168,   44,   73,  214,  215,
 /*   230 */   173,   12,   13,   14,  182,  163,   33,  180,  182,   20,
 /*   240 */   226,  189,   39,  168,  165,  166,  189,  195,  173,  193,
 /*   250 */   204,  199,   33,  239,   74,  180,  170,  243,   39,  207,
 /*   260 */   208,  209,  210,   88,  189,   32,  214,  215,  216,   50,
 /*   270 */    37,  199,  226,  187,  182,  163,   43,  168,  163,   46,
 /*   280 */   228,  189,  173,  132,  133,  239,  234,  235,  134,  243,
 /*   290 */    71,  119,   73,  175,  182,   20,  178,  182,  189,  127,
 /*   300 */   208,  189,  130,  135,  189,   72,  115,  195,   75,    0,
 /*   310 */   195,  199,  168,  163,  199,  133,  204,  173,   99,  207,
 /*   320 */   208,  209,  207,  208,  209,  210,  174,  152,  182,  214,
 /*   330 */   215,  216,  182,  189,  182,  189,  117,  118,  226,  189,
 /*   340 */    20,  108,  190,   35,  111,  195,  167,  114,  163,  199,
 /*   350 */   235,  239,    0,   33,  208,  243,  177,  207,  208,  209,
 /*   360 */   210,   41,  174,  184,  214,  215,  216,  182,   60,   60,
 /*   370 */   182,  182,   64,   64,  189,  225,  134,  163,  190,   60,
 /*   380 */   195,   14,  193,   64,  199,    3,   78,   20,   80,   81,
 /*   390 */   115,   83,  207,  208,  209,  210,  182,   89,   89,  214,
 /*   400 */   215,  216,  205,  189,  163,    0,   12,   13,   89,  195,
 /*   410 */   225,   20,   60,  199,   20,   88,   64,  108,  109,  222,
 /*   420 */   205,  207,  208,  209,  210,  205,  168,   33,  214,  215,
 /*   430 */   216,  173,  163,   39,  163,   12,   13,  222,   33,  225,
 /*   440 */   199,   89,  222,   20,   50,   74,  163,  189,  168,  163,
 /*   450 */   168,  182,  163,  173,  163,  173,   33,   49,  189,   88,
 /*   460 */   108,  109,   39,   73,  195,   71,  168,   73,  199,  189,
 /*   470 */   199,  189,  167,  182,  168,   85,  207,  208,  209,  210,
 /*   480 */   189,  154,  199,  214,  215,  199,  195,  189,  199,  184,
 /*   490 */   199,  162,  168,   99,   71,  189,   73,   86,  207,  208,
 /*   500 */   209,  210,   14,   15,   16,  214,  215,  209,  183,   32,
 /*   510 */    50,  117,  118,  189,   37,  209,   74,  219,  220,  221,
 /*   520 */    43,  223,   99,   46,    2,  219,  220,  221,  204,  223,
 /*   530 */    88,  183,  150,  209,   12,   13,   14,   15,   16,  183,
 /*   540 */   117,  118,  163,  219,  220,  221,  113,  223,  163,   72,
 /*   550 */   226,  183,   75,   12,   13,   14,  163,  134,   12,   13,
 /*   560 */   168,   20,  163,  239,    1,    2,   20,  243,   12,   13,
 /*   570 */   172,  237,  139,  140,   33,  182,   20,  163,  199,   33,
 /*   580 */    39,  121,  189,   79,  199,   39,   82,  110,  195,   33,
 /*   590 */   163,  114,  199,  246,    2,   39,  182,  163,  199,  163,
 /*   600 */   207,  208,  209,  189,   12,   13,   14,   15,   16,  195,
 /*   610 */   192,   74,   71,  199,   73,   79,  202,   71,   82,   73,
 /*   620 */    73,  207,  208,  209,   20,   88,  199,   71,   74,   73,
 /*   630 */   158,  159,   28,  199,   87,  199,   74,  244,  245,   74,
 /*   640 */    99,  231,   88,  163,   40,   99,  165,   74,  224,   45,
 /*   650 */    88,   47,   74,   88,   74,   99,  206,   74,  117,  118,
 /*   660 */     0,   88,  182,  117,  118,  163,   88,  182,   88,  189,
 /*   670 */    66,   88,   21,  117,  118,  195,  227,  240,   20,  199,
 /*   680 */    76,   77,   74,  168,  182,   34,  163,  207,  208,  209,
 /*   690 */   210,  189,    0,   33,   74,  215,   88,  195,   20,   74,
 /*   700 */    74,  199,   74,   79,  202,  182,   82,  163,   88,  207,
 /*   710 */   208,  209,  189,   88,   88,   74,   88,  203,  195,  170,
 /*   720 */   116,   20,  199,  163,   79,   33,  182,   82,   18,   88,
 /*   730 */   207,  208,  209,  189,  189,   20,  196,  170,  163,  195,
 /*   740 */   168,   31,  182,  199,  170,   35,   36,   20,  164,  189,
 /*   750 */   189,  207,  208,  209,  203,  195,  167,  182,   48,  199,
 /*   760 */   163,  238,  202,   20,  189,  196,  236,  207,  208,  209,
 /*   770 */   195,  142,  206,  141,  199,  118,  163,  202,  199,  182,
 /*   780 */   199,  163,  207,  208,  209,  200,  189,  200,  236,  245,
 /*   790 */   233,  149,  195,  137,  230,  182,  199,  148,  136,  229,
 /*   800 */   182,  133,  189,  232,  207,  208,  209,  189,  195,  163,
 /*   810 */   189,   20,  199,  195,  205,  131,  217,  199,  157,  153,
 /*   820 */   207,  208,  209,  247,  242,  207,  208,  209,  182,  241,
 /*   830 */   151,   73,  213,  163,  200,  189,  199,  199,  163,  200,
 /*   840 */   112,  195,  199,  189,  134,  199,   73,  197,  196,  178,
 /*   850 */   167,  164,  182,  207,  208,  209,  168,  182,    4,  189,
 /*   860 */   167,  163,  185,  189,  189,  195,  164,  182,  182,  199,
 /*   870 */   195,    0,  182,   19,  199,    0,  163,  207,  208,  209,
 /*   880 */   182,  182,  207,  208,  209,  182,   32,  189,  182,  182,
 /*   890 */   182,   37,  168,  195,  182,  182,   42,  199,  182,  182,
 /*   900 */    46,  176,  189,  169,    0,  207,  208,  209,  195,  176,
 /*   910 */     0,    0,  199,  163,   66,   87,  163,    0,    0,    0,
 /*   920 */   207,  208,  209,   52,    0,   54,   72,    0,   57,   75,
 /*   930 */     0,   60,  182,   62,    0,  182,   65,    0,    0,  189,
 /*   940 */     0,   44,  189,   51,    0,  195,  163,    0,  195,  199,
 /*   950 */    37,   39,  199,   44,    0,    0,    0,  207,  208,  209,
 /*   960 */   207,  208,  209,    0,    0,  182,  115,    0,  163,   44,
 /*   970 */   110,   73,  189,   88,   88,  138,   74,   88,  195,   74,
 /*   980 */    73,   73,  199,   88,  163,   39,   87,  182,   74,  163,
 /*   990 */   207,  208,  209,   73,  189,   74,   73,  138,  132,  138,
 /*  1000 */   195,    4,   74,  182,  199,   39,   74,   74,  182,   87,
 /*  1010 */   189,   39,  207,  208,  209,  189,  195,  121,   39,   88,
 /*  1020 */   199,  195,   39,   88,   88,  199,   87,   39,  207,  208,
 /*  1030 */   209,   39,    2,  207,  208,  209,  121,   87,   73,  143,
 /*  1040 */   144,  145,  146,  147,   74,   74,   73,    0,   74,   73,
 /*  1050 */    73,   73,   44,  110,  113,   73,   33,   73,   89,   73,
 /*  1060 */    85,   39,   73,   73,   39,   33,   86,   51,   50,   71,
 /*  1070 */    33,    0,   39,   39,   39,   39,   33,   39,   39,   39,
 /*  1080 */    39,   39,    0,   39,   39,   39,    0,   39,   39,   39,
 /*  1090 */    39,   39,   37,   39,   38,    0,   33,   44,   21,    0,
 /*  1100 */    87,   87,   82,   84,   39,   74,   39,   39,   39,   39,
 /*  1110 */   101,  101,   39,  101,   39,   39,  101,   33,    0,   39,
 /*  1120 */    39,    0,   39,   33,   39,    0,   39,   39,    0,   33,
 /*  1130 */    73,   40,   74,   73,   33,   74,   74,   73,    0,   73,
 /*  1140 */    33,   39,    0,   33,   33,  248,   74,   73,  248,   20,
 /*  1150 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1160 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1170 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1180 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1190 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1200 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1210 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1220 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1230 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1240 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1250 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1260 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1270 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1280 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1290 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*  1300 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
};
#define YY_SHIFT_COUNT    (399)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (1142)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   710,   61,  219,  394,  394,  394,  394,  423,  394,  394,
 /*    10 */   154,  546,  556,  541,  546,  546,  546,  546,  546,  546,
 /*    20 */   546,  546,  546,  546,  546,  546,  546,  546,  546,  546,
 /*    30 */   546,  546,  546,   13,   13,   13,   70,   62,   62,  102,
 /*    40 */   102,   62,   62,   73,  156,  165,  165,  242,  391,  156,
 /*    50 */    62,   62,  156,   62,  156,  391,  156,  156,   62,  408,
 /*    60 */     1,   64,   64,   80,  308,  102,  102,  145,  102,  102,
 /*    70 */   102,  102,  102,  102,  102,  102,  102,  102,  320,   50,
 /*    80 */   191,  191,  191,   97,  391,  156,  156,  156,  411,   59,
 /*    90 */    59,   59,   59,   59,   18,  604,   15,  871,  896,  433,
 /*   100 */   319,  203,  275,  151,  182,  151,  367,  382,  168,  658,
 /*   110 */   678,   73,  701,  715,   73,  658,   73,  727,  678,  408,
 /*   120 */   701,  715,  743,  629,  632,  657,  629,  632,  657,  642,
 /*   130 */   649,  656,  662,  668,  701,  791,  684,  661,  666,  679,
 /*   140 */   758,  156,  632,  657,  657,  632,  657,  728,  701,  715,
 /*   150 */   411,  408,  773,  658,  408,  727,  701,  156,  156,  156,
 /*   160 */   156,  156,  156,  156,  156,  156,  156,  156,  658,  727,
 /*   170 */  1150, 1150, 1150,    0,  233,  477,  854,  522,  592,  124,
 /*   180 */   309,  352,  103,  103,  103,  103,  103,  103,  103,  180,
 /*   190 */   172,  488,  488,  488,  488,  371,  442,  537,  563,  472,
 /*   200 */   175,  327,  554,  460,  562,  547,  565,  573,  578,  580,
 /*   210 */   583,  608,  620,  625,  626,  628,  390,  504,  536,  624,
 /*   220 */   641,  645,  405,  660,  692,  651,  875,  904,  910,  911,
 /*   230 */   848,  828,  917,  918,  919,  924,  927,  930,  934,  937,
 /*   240 */   938,  897,  940,  892,  944,  947,  912,  913,  909,  954,
 /*   250 */   955,  956,  963,  964,  851,  967,  898,  925,  860,  885,
 /*   260 */   886,  837,  902,  889,  905,  907,  908,  914,  920,  921,
 /*   270 */   946,  895,  899,  923,  931,  859,  928,  932,  922,  866,
 /*   280 */   935,  939,  933,  936,  861,  997,  966,  972,  979,  983,
 /*   290 */   988,  992, 1030,  915,  950,  970,  965,  973,  971,  974,
 /*   300 */   976,  977,  941,  978, 1047, 1008,  943,  982,  975,  984,
 /*   310 */   980, 1023,  969,  986,  989,  990, 1022, 1025, 1032, 1016,
 /*   320 */  1018,  998, 1037, 1033, 1034, 1035, 1036, 1038, 1039, 1040,
 /*   330 */  1043, 1041, 1042, 1044, 1045, 1046, 1048, 1049, 1050, 1051,
 /*   340 */  1071, 1052, 1055, 1053, 1082, 1054, 1056, 1086, 1095, 1063,
 /*   350 */  1077, 1099, 1013, 1014, 1019, 1020, 1031, 1065, 1067, 1057,
 /*   360 */  1058, 1068, 1060, 1061, 1069, 1064, 1062, 1070, 1066, 1072,
 /*   370 */  1073, 1074, 1009, 1010, 1012, 1015, 1075, 1076, 1084, 1118,
 /*   380 */  1080, 1081, 1083, 1085, 1087, 1088, 1121, 1090, 1125, 1096,
 /*   390 */  1091, 1128, 1101, 1102, 1138, 1107, 1142, 1110, 1111, 1129,
};
#define YY_REDUCE_COUNT (172)
#define YY_REDUCE_MIN   (-220)
#define YY_REDUCE_MAX   (826)
static const short yy_reduce_ofst[] = {
 /*     0 */  -155,   14,   52,  115,  150,  185,  214,  112,  269,  291,
 /*    10 */   324,  393,  480,  414,  502,  523,  544,  560,  575,  597,
 /*    20 */   613,  618,  646,  670,  675,  698,  713,  750,  753,  783,
 /*    30 */   805,  821,  826,  -76,  298,  306,   46,   57,   75, -162,
 /*    40 */  -161,  -77,  109, -167,  -95,   92,  146, -220, -191,   26,
 /*    50 */   144,  258,  -13,  280,  152, -159,   24,  188,  282,  179,
 /*    60 */  -203, -203, -203, -140, -109,   72,  241,   -4,  271,  283,
 /*    70 */   286,  289,  379,  385,  399,  427,  434,  436,   79,   86,
 /*    80 */   197,  215,  220,  305, -106, -156,   56,  189,  118, -172,
 /*    90 */   325,  348,  356,  368,  329,  392,  347,  398,  334,  410,
 /*   100 */   418,  481,  450,  424,  424,  424,  485,  437,  449,  515,
 /*   110 */   514,  549,  545,  540,  567,  572,  574,  584,  551,  589,
 /*   120 */   561,  569,  566,  530,  585,  579,  552,  587,  581,  557,
 /*   130 */   571,  564,  570,  424,  621,  609,  599,  576,  582,  588,
 /*   140 */   619,  485,  634,  637,  638,  639,  643,  650,  654,  652,
 /*   150 */   671,  683,  677,  688,  693,  687,  674,  685,  686,  690,
 /*   160 */   699,  703,  706,  707,  708,  712,  716,  717,  724,  702,
 /*   170 */   725,  733,  734,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*    10 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*    20 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*    30 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*    40 */   913,  913,  913,  959,  913,  913,  913,  913,  913,  913,
 /*    50 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  957,
 /*    60 */   913, 1140,  913,  913,  913,  913,  913,  913,  913,  913,
 /*    70 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  959,
 /*    80 */  1151, 1151, 1151,  957,  913,  913,  913,  913, 1044,  913,
 /*    90 */   913,  913,  913,  913,  913,  913, 1215,  913,  913, 1175,
 /*   100 */   997,  913, 1167, 1143, 1157, 1144,  913, 1200, 1160,  913,
 /*   110 */   913,  959,  913,  913,  959,  913,  959,  913,  913,  957,
 /*   120 */   913,  913,  913, 1182, 1180,  913, 1182, 1180,  913, 1194,
 /*   130 */  1190, 1173, 1171, 1157,  913,  913,  913, 1218, 1206, 1202,
 /*   140 */   913,  913, 1180,  913,  913, 1180,  913, 1067,  913,  913,
 /*   150 */   913,  957, 1013,  913,  957,  913,  913,  913,  913,  913,
 /*   160 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   170 */  1047, 1047,  960,  913,  913,  913,  913,  913,  913,  913,
 /*   180 */   913,  913, 1112, 1193, 1192, 1111, 1117, 1116, 1115,  913,
 /*   190 */   913, 1106, 1107, 1105, 1104,  913,  913,  913, 1141,  913,
 /*   200 */  1203, 1207,  913,  913,  913, 1091,  913,  913,  913,  913,
 /*   210 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   220 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   230 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   240 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   250 */   913,  913,  913,  913,  913,  913,  913,  913,  913, 1164,
 /*   260 */  1174,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   270 */   913,  913, 1091,  913, 1191,  913, 1150, 1146,  913,  913,
 /*   280 */  1142,  913,  913, 1201,  913,  913,  913,  913,  913,  913,
 /*   290 */   913,  913, 1136,  913,  913,  913,  913,  913,  913,  913,
 /*   300 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   310 */   913,  913, 1019,  913,  913,  913,  913,  913,  913,  913,
 /*   320 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   330 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   340 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   350 */   913,  913, 1090,  913,  913,  913,  913,  913,  913, 1041,
 /*   360 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   370 */   913,  913, 1026, 1024, 1023, 1022,  913,  913,  913,  913,
 /*   380 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
 /*   390 */   913,  913,  913,  913,  913,  913,  913,  913,  913,  913,
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
  /*    1 */ "OR",
  /*    2 */ "AND",
  /*    3 */ "UNION",
  /*    4 */ "ALL",
  /*    5 */ "MINUS",
  /*    6 */ "EXCEPT",
  /*    7 */ "INTERSECT",
  /*    8 */ "NK_BITAND",
  /*    9 */ "NK_BITOR",
  /*   10 */ "NK_LSHIFT",
  /*   11 */ "NK_RSHIFT",
  /*   12 */ "NK_PLUS",
  /*   13 */ "NK_MINUS",
  /*   14 */ "NK_STAR",
  /*   15 */ "NK_SLASH",
  /*   16 */ "NK_REM",
  /*   17 */ "NK_CONCAT",
  /*   18 */ "ALTER",
  /*   19 */ "ACCOUNT",
  /*   20 */ "NK_ID",
  /*   21 */ "PASS",
  /*   22 */ "PPS",
  /*   23 */ "TSERIES",
  /*   24 */ "STORAGE",
  /*   25 */ "STREAMS",
  /*   26 */ "QTIME",
  /*   27 */ "DBS",
  /*   28 */ "USERS",
  /*   29 */ "CONNS",
  /*   30 */ "STATE",
  /*   31 */ "CREATE",
  /*   32 */ "USER",
  /*   33 */ "NK_STRING",
  /*   34 */ "PRIVILEGE",
  /*   35 */ "DROP",
  /*   36 */ "SHOW",
  /*   37 */ "DNODE",
  /*   38 */ "PORT",
  /*   39 */ "NK_INTEGER",
  /*   40 */ "DNODES",
  /*   41 */ "NK_IPTOKEN",
  /*   42 */ "LOCAL",
  /*   43 */ "QNODE",
  /*   44 */ "ON",
  /*   45 */ "QNODES",
  /*   46 */ "DATABASE",
  /*   47 */ "DATABASES",
  /*   48 */ "USE",
  /*   49 */ "IF",
  /*   50 */ "NOT",
  /*   51 */ "EXISTS",
  /*   52 */ "BLOCKS",
  /*   53 */ "CACHE",
  /*   54 */ "CACHELAST",
  /*   55 */ "COMP",
  /*   56 */ "DAYS",
  /*   57 */ "FSYNC",
  /*   58 */ "MAXROWS",
  /*   59 */ "MINROWS",
  /*   60 */ "KEEP",
  /*   61 */ "PRECISION",
  /*   62 */ "QUORUM",
  /*   63 */ "REPLICA",
  /*   64 */ "TTL",
  /*   65 */ "WAL",
  /*   66 */ "VGROUPS",
  /*   67 */ "SINGLE_STABLE",
  /*   68 */ "STREAM_MODE",
  /*   69 */ "RETENTIONS",
  /*   70 */ "FILE_FACTOR",
  /*   71 */ "NK_FLOAT",
  /*   72 */ "TABLE",
  /*   73 */ "NK_LP",
  /*   74 */ "NK_RP",
  /*   75 */ "STABLE",
  /*   76 */ "TABLES",
  /*   77 */ "STABLES",
  /*   78 */ "ADD",
  /*   79 */ "COLUMN",
  /*   80 */ "MODIFY",
  /*   81 */ "RENAME",
  /*   82 */ "TAG",
  /*   83 */ "SET",
  /*   84 */ "NK_EQ",
  /*   85 */ "USING",
  /*   86 */ "TAGS",
  /*   87 */ "NK_DOT",
  /*   88 */ "NK_COMMA",
  /*   89 */ "COMMENT",
  /*   90 */ "BOOL",
  /*   91 */ "TINYINT",
  /*   92 */ "SMALLINT",
  /*   93 */ "INT",
  /*   94 */ "INTEGER",
  /*   95 */ "BIGINT",
  /*   96 */ "FLOAT",
  /*   97 */ "DOUBLE",
  /*   98 */ "BINARY",
  /*   99 */ "TIMESTAMP",
  /*  100 */ "NCHAR",
  /*  101 */ "UNSIGNED",
  /*  102 */ "JSON",
  /*  103 */ "VARCHAR",
  /*  104 */ "MEDIUMBLOB",
  /*  105 */ "BLOB",
  /*  106 */ "VARBINARY",
  /*  107 */ "DECIMAL",
  /*  108 */ "SMA",
  /*  109 */ "ROLLUP",
  /*  110 */ "INDEX",
  /*  111 */ "FULLTEXT",
  /*  112 */ "FUNCTION",
  /*  113 */ "INTERVAL",
  /*  114 */ "TOPIC",
  /*  115 */ "AS",
  /*  116 */ "MNODES",
  /*  117 */ "NK_BOOL",
  /*  118 */ "NK_VARIABLE",
  /*  119 */ "BETWEEN",
  /*  120 */ "IS",
  /*  121 */ "NULL",
  /*  122 */ "NK_LT",
  /*  123 */ "NK_GT",
  /*  124 */ "NK_LE",
  /*  125 */ "NK_GE",
  /*  126 */ "NK_NE",
  /*  127 */ "LIKE",
  /*  128 */ "MATCH",
  /*  129 */ "NMATCH",
  /*  130 */ "IN",
  /*  131 */ "FROM",
  /*  132 */ "JOIN",
  /*  133 */ "INNER",
  /*  134 */ "SELECT",
  /*  135 */ "DISTINCT",
  /*  136 */ "WHERE",
  /*  137 */ "PARTITION",
  /*  138 */ "BY",
  /*  139 */ "SESSION",
  /*  140 */ "STATE_WINDOW",
  /*  141 */ "SLIDING",
  /*  142 */ "FILL",
  /*  143 */ "VALUE",
  /*  144 */ "NONE",
  /*  145 */ "PREV",
  /*  146 */ "LINEAR",
  /*  147 */ "NEXT",
  /*  148 */ "GROUP",
  /*  149 */ "HAVING",
  /*  150 */ "ORDER",
  /*  151 */ "SLIMIT",
  /*  152 */ "SOFFSET",
  /*  153 */ "LIMIT",
  /*  154 */ "OFFSET",
  /*  155 */ "ASC",
  /*  156 */ "DESC",
  /*  157 */ "NULLS",
  /*  158 */ "FIRST",
  /*  159 */ "LAST",
  /*  160 */ "cmd",
  /*  161 */ "account_options",
  /*  162 */ "account_option",
  /*  163 */ "literal",
  /*  164 */ "user_name",
  /*  165 */ "dnode_endpoint",
  /*  166 */ "dnode_host_name",
  /*  167 */ "not_exists_opt",
  /*  168 */ "db_name",
  /*  169 */ "db_options",
  /*  170 */ "exists_opt",
  /*  171 */ "alter_db_options",
  /*  172 */ "alter_db_option",
  /*  173 */ "full_table_name",
  /*  174 */ "column_def_list",
  /*  175 */ "tags_def_opt",
  /*  176 */ "table_options",
  /*  177 */ "multi_create_clause",
  /*  178 */ "tags_def",
  /*  179 */ "multi_drop_clause",
  /*  180 */ "alter_table_clause",
  /*  181 */ "alter_table_options",
  /*  182 */ "column_name",
  /*  183 */ "type_name",
  /*  184 */ "create_subtable_clause",
  /*  185 */ "specific_tags_opt",
  /*  186 */ "literal_list",
  /*  187 */ "drop_table_clause",
  /*  188 */ "col_name_list",
  /*  189 */ "table_name",
  /*  190 */ "column_def",
  /*  191 */ "func_name_list",
  /*  192 */ "alter_table_option",
  /*  193 */ "col_name",
  /*  194 */ "func_name",
  /*  195 */ "function_name",
  /*  196 */ "index_name",
  /*  197 */ "index_options",
  /*  198 */ "func_list",
  /*  199 */ "duration_literal",
  /*  200 */ "sliding_opt",
  /*  201 */ "func",
  /*  202 */ "expression_list",
  /*  203 */ "topic_name",
  /*  204 */ "query_expression",
  /*  205 */ "table_alias",
  /*  206 */ "column_alias",
  /*  207 */ "expression",
  /*  208 */ "column_reference",
  /*  209 */ "subquery",
  /*  210 */ "predicate",
  /*  211 */ "compare_op",
  /*  212 */ "in_op",
  /*  213 */ "in_predicate_value",
  /*  214 */ "boolean_value_expression",
  /*  215 */ "boolean_primary",
  /*  216 */ "common_expression",
  /*  217 */ "from_clause",
  /*  218 */ "table_reference_list",
  /*  219 */ "table_reference",
  /*  220 */ "table_primary",
  /*  221 */ "joined_table",
  /*  222 */ "alias_opt",
  /*  223 */ "parenthesized_joined_table",
  /*  224 */ "join_type",
  /*  225 */ "search_condition",
  /*  226 */ "query_specification",
  /*  227 */ "set_quantifier_opt",
  /*  228 */ "select_list",
  /*  229 */ "where_clause_opt",
  /*  230 */ "partition_by_clause_opt",
  /*  231 */ "twindow_clause_opt",
  /*  232 */ "group_by_clause_opt",
  /*  233 */ "having_clause_opt",
  /*  234 */ "select_sublist",
  /*  235 */ "select_item",
  /*  236 */ "fill_opt",
  /*  237 */ "fill_mode",
  /*  238 */ "group_by_list",
  /*  239 */ "query_expression_body",
  /*  240 */ "order_by_clause_opt",
  /*  241 */ "slimit_clause_opt",
  /*  242 */ "limit_clause_opt",
  /*  243 */ "query_primary",
  /*  244 */ "sort_specification_list",
  /*  245 */ "sort_specification",
  /*  246 */ "ordering_specification_opt",
  /*  247 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= ALTER ACCOUNT NK_ID account_options",
 /*   1 */ "account_options ::= account_option",
 /*   2 */ "account_options ::= account_options account_option",
 /*   3 */ "account_option ::= PASS literal",
 /*   4 */ "account_option ::= PPS literal",
 /*   5 */ "account_option ::= TSERIES literal",
 /*   6 */ "account_option ::= STORAGE literal",
 /*   7 */ "account_option ::= STREAMS literal",
 /*   8 */ "account_option ::= QTIME literal",
 /*   9 */ "account_option ::= DBS literal",
 /*  10 */ "account_option ::= USERS literal",
 /*  11 */ "account_option ::= CONNS literal",
 /*  12 */ "account_option ::= STATE literal",
 /*  13 */ "cmd ::= CREATE USER user_name PASS NK_STRING",
 /*  14 */ "cmd ::= ALTER USER user_name PASS NK_STRING",
 /*  15 */ "cmd ::= ALTER USER user_name PRIVILEGE NK_STRING",
 /*  16 */ "cmd ::= DROP USER user_name",
 /*  17 */ "cmd ::= SHOW USERS",
 /*  18 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*  19 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*  20 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*  21 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*  22 */ "cmd ::= SHOW DNODES",
 /*  23 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING",
 /*  24 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING",
 /*  25 */ "cmd ::= ALTER ALL DNODES NK_STRING",
 /*  26 */ "cmd ::= ALTER ALL DNODES NK_STRING NK_STRING",
 /*  27 */ "dnode_endpoint ::= NK_STRING",
 /*  28 */ "dnode_host_name ::= NK_ID",
 /*  29 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  30 */ "cmd ::= ALTER LOCAL NK_STRING",
 /*  31 */ "cmd ::= ALTER LOCAL NK_STRING NK_STRING",
 /*  32 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  33 */ "cmd ::= DROP QNODE ON DNODE NK_INTEGER",
 /*  34 */ "cmd ::= SHOW QNODES",
 /*  35 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  36 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  37 */ "cmd ::= SHOW DATABASES",
 /*  38 */ "cmd ::= USE db_name",
 /*  39 */ "cmd ::= ALTER DATABASE db_name alter_db_options",
 /*  40 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  41 */ "not_exists_opt ::=",
 /*  42 */ "exists_opt ::= IF EXISTS",
 /*  43 */ "exists_opt ::=",
 /*  44 */ "db_options ::=",
 /*  45 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  46 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  47 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  48 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  49 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  50 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  51 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  52 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  53 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  54 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  55 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  56 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  57 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  58 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  59 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  60 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  61 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  62 */ "db_options ::= db_options RETENTIONS NK_STRING",
 /*  63 */ "db_options ::= db_options FILE_FACTOR NK_FLOAT",
 /*  64 */ "alter_db_options ::= alter_db_option",
 /*  65 */ "alter_db_options ::= alter_db_options alter_db_option",
 /*  66 */ "alter_db_option ::= BLOCKS NK_INTEGER",
 /*  67 */ "alter_db_option ::= FSYNC NK_INTEGER",
 /*  68 */ "alter_db_option ::= KEEP NK_INTEGER",
 /*  69 */ "alter_db_option ::= WAL NK_INTEGER",
 /*  70 */ "alter_db_option ::= QUORUM NK_INTEGER",
 /*  71 */ "alter_db_option ::= CACHELAST NK_INTEGER",
 /*  72 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  73 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  74 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  75 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  76 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  77 */ "cmd ::= SHOW TABLES",
 /*  78 */ "cmd ::= SHOW STABLES",
 /*  79 */ "cmd ::= ALTER TABLE alter_table_clause",
 /*  80 */ "cmd ::= ALTER STABLE alter_table_clause",
 /*  81 */ "alter_table_clause ::= full_table_name alter_table_options",
 /*  82 */ "alter_table_clause ::= full_table_name ADD COLUMN column_name type_name",
 /*  83 */ "alter_table_clause ::= full_table_name DROP COLUMN column_name",
 /*  84 */ "alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name",
 /*  85 */ "alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name",
 /*  86 */ "alter_table_clause ::= full_table_name ADD TAG column_name type_name",
 /*  87 */ "alter_table_clause ::= full_table_name DROP TAG column_name",
 /*  88 */ "alter_table_clause ::= full_table_name MODIFY TAG column_name type_name",
 /*  89 */ "alter_table_clause ::= full_table_name RENAME TAG column_name column_name",
 /*  90 */ "alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal",
 /*  91 */ "multi_create_clause ::= create_subtable_clause",
 /*  92 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  93 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  94 */ "multi_drop_clause ::= drop_table_clause",
 /*  95 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  96 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  97 */ "specific_tags_opt ::=",
 /*  98 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  99 */ "full_table_name ::= table_name",
 /* 100 */ "full_table_name ::= db_name NK_DOT table_name",
 /* 101 */ "column_def_list ::= column_def",
 /* 102 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /* 103 */ "column_def ::= column_name type_name",
 /* 104 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /* 105 */ "type_name ::= BOOL",
 /* 106 */ "type_name ::= TINYINT",
 /* 107 */ "type_name ::= SMALLINT",
 /* 108 */ "type_name ::= INT",
 /* 109 */ "type_name ::= INTEGER",
 /* 110 */ "type_name ::= BIGINT",
 /* 111 */ "type_name ::= FLOAT",
 /* 112 */ "type_name ::= DOUBLE",
 /* 113 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /* 114 */ "type_name ::= TIMESTAMP",
 /* 115 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /* 116 */ "type_name ::= TINYINT UNSIGNED",
 /* 117 */ "type_name ::= SMALLINT UNSIGNED",
 /* 118 */ "type_name ::= INT UNSIGNED",
 /* 119 */ "type_name ::= BIGINT UNSIGNED",
 /* 120 */ "type_name ::= JSON",
 /* 121 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /* 122 */ "type_name ::= MEDIUMBLOB",
 /* 123 */ "type_name ::= BLOB",
 /* 124 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /* 125 */ "type_name ::= DECIMAL",
 /* 126 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /* 127 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /* 128 */ "tags_def_opt ::=",
 /* 129 */ "tags_def_opt ::= tags_def",
 /* 130 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /* 131 */ "table_options ::=",
 /* 132 */ "table_options ::= table_options COMMENT NK_STRING",
 /* 133 */ "table_options ::= table_options KEEP NK_INTEGER",
 /* 134 */ "table_options ::= table_options TTL NK_INTEGER",
 /* 135 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /* 136 */ "table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP",
 /* 137 */ "alter_table_options ::= alter_table_option",
 /* 138 */ "alter_table_options ::= alter_table_options alter_table_option",
 /* 139 */ "alter_table_option ::= COMMENT NK_STRING",
 /* 140 */ "alter_table_option ::= KEEP NK_INTEGER",
 /* 141 */ "alter_table_option ::= TTL NK_INTEGER",
 /* 142 */ "col_name_list ::= col_name",
 /* 143 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /* 144 */ "col_name ::= column_name",
 /* 145 */ "func_name_list ::= func_name",
 /* 146 */ "func_name_list ::= func_name_list NK_COMMA col_name",
 /* 147 */ "func_name ::= function_name",
 /* 148 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /* 149 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /* 150 */ "cmd ::= DROP INDEX index_name ON table_name",
 /* 151 */ "index_options ::=",
 /* 152 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /* 153 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /* 154 */ "func_list ::= func",
 /* 155 */ "func_list ::= func_list NK_COMMA func",
 /* 156 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 157 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression",
 /* 158 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name",
 /* 159 */ "cmd ::= DROP TOPIC exists_opt topic_name",
 /* 160 */ "cmd ::= SHOW VGROUPS",
 /* 161 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /* 162 */ "cmd ::= SHOW MNODES",
 /* 163 */ "cmd ::= query_expression",
 /* 164 */ "literal ::= NK_INTEGER",
 /* 165 */ "literal ::= NK_FLOAT",
 /* 166 */ "literal ::= NK_STRING",
 /* 167 */ "literal ::= NK_BOOL",
 /* 168 */ "literal ::= TIMESTAMP NK_STRING",
 /* 169 */ "literal ::= duration_literal",
 /* 170 */ "duration_literal ::= NK_VARIABLE",
 /* 171 */ "literal_list ::= literal",
 /* 172 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 173 */ "db_name ::= NK_ID",
 /* 174 */ "table_name ::= NK_ID",
 /* 175 */ "column_name ::= NK_ID",
 /* 176 */ "function_name ::= NK_ID",
 /* 177 */ "table_alias ::= NK_ID",
 /* 178 */ "column_alias ::= NK_ID",
 /* 179 */ "user_name ::= NK_ID",
 /* 180 */ "index_name ::= NK_ID",
 /* 181 */ "topic_name ::= NK_ID",
 /* 182 */ "expression ::= literal",
 /* 183 */ "expression ::= column_reference",
 /* 184 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 185 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 186 */ "expression ::= subquery",
 /* 187 */ "expression ::= NK_LP expression NK_RP",
 /* 188 */ "expression ::= NK_PLUS expression",
 /* 189 */ "expression ::= NK_MINUS expression",
 /* 190 */ "expression ::= expression NK_PLUS expression",
 /* 191 */ "expression ::= expression NK_MINUS expression",
 /* 192 */ "expression ::= expression NK_STAR expression",
 /* 193 */ "expression ::= expression NK_SLASH expression",
 /* 194 */ "expression ::= expression NK_REM expression",
 /* 195 */ "expression_list ::= expression",
 /* 196 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 197 */ "column_reference ::= column_name",
 /* 198 */ "column_reference ::= table_name NK_DOT column_name",
 /* 199 */ "predicate ::= expression compare_op expression",
 /* 200 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 201 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 202 */ "predicate ::= expression IS NULL",
 /* 203 */ "predicate ::= expression IS NOT NULL",
 /* 204 */ "predicate ::= expression in_op in_predicate_value",
 /* 205 */ "compare_op ::= NK_LT",
 /* 206 */ "compare_op ::= NK_GT",
 /* 207 */ "compare_op ::= NK_LE",
 /* 208 */ "compare_op ::= NK_GE",
 /* 209 */ "compare_op ::= NK_NE",
 /* 210 */ "compare_op ::= NK_EQ",
 /* 211 */ "compare_op ::= LIKE",
 /* 212 */ "compare_op ::= NOT LIKE",
 /* 213 */ "compare_op ::= MATCH",
 /* 214 */ "compare_op ::= NMATCH",
 /* 215 */ "in_op ::= IN",
 /* 216 */ "in_op ::= NOT IN",
 /* 217 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 218 */ "boolean_value_expression ::= boolean_primary",
 /* 219 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 220 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 221 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 222 */ "boolean_primary ::= predicate",
 /* 223 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 224 */ "common_expression ::= expression",
 /* 225 */ "common_expression ::= boolean_value_expression",
 /* 226 */ "from_clause ::= FROM table_reference_list",
 /* 227 */ "table_reference_list ::= table_reference",
 /* 228 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 229 */ "table_reference ::= table_primary",
 /* 230 */ "table_reference ::= joined_table",
 /* 231 */ "table_primary ::= table_name alias_opt",
 /* 232 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 233 */ "table_primary ::= subquery alias_opt",
 /* 234 */ "table_primary ::= parenthesized_joined_table",
 /* 235 */ "alias_opt ::=",
 /* 236 */ "alias_opt ::= table_alias",
 /* 237 */ "alias_opt ::= AS table_alias",
 /* 238 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 239 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 240 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 241 */ "join_type ::=",
 /* 242 */ "join_type ::= INNER",
 /* 243 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 244 */ "set_quantifier_opt ::=",
 /* 245 */ "set_quantifier_opt ::= DISTINCT",
 /* 246 */ "set_quantifier_opt ::= ALL",
 /* 247 */ "select_list ::= NK_STAR",
 /* 248 */ "select_list ::= select_sublist",
 /* 249 */ "select_sublist ::= select_item",
 /* 250 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 251 */ "select_item ::= common_expression",
 /* 252 */ "select_item ::= common_expression column_alias",
 /* 253 */ "select_item ::= common_expression AS column_alias",
 /* 254 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 255 */ "where_clause_opt ::=",
 /* 256 */ "where_clause_opt ::= WHERE search_condition",
 /* 257 */ "partition_by_clause_opt ::=",
 /* 258 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 259 */ "twindow_clause_opt ::=",
 /* 260 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 261 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 262 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 263 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 264 */ "sliding_opt ::=",
 /* 265 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 266 */ "fill_opt ::=",
 /* 267 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 268 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 269 */ "fill_mode ::= NONE",
 /* 270 */ "fill_mode ::= PREV",
 /* 271 */ "fill_mode ::= NULL",
 /* 272 */ "fill_mode ::= LINEAR",
 /* 273 */ "fill_mode ::= NEXT",
 /* 274 */ "group_by_clause_opt ::=",
 /* 275 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 276 */ "group_by_list ::= expression",
 /* 277 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 278 */ "having_clause_opt ::=",
 /* 279 */ "having_clause_opt ::= HAVING search_condition",
 /* 280 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 281 */ "query_expression_body ::= query_primary",
 /* 282 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 283 */ "query_primary ::= query_specification",
 /* 284 */ "order_by_clause_opt ::=",
 /* 285 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 286 */ "slimit_clause_opt ::=",
 /* 287 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 288 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 289 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 290 */ "limit_clause_opt ::=",
 /* 291 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 292 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 293 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 294 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 295 */ "search_condition ::= common_expression",
 /* 296 */ "sort_specification_list ::= sort_specification",
 /* 297 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 298 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 299 */ "ordering_specification_opt ::=",
 /* 300 */ "ordering_specification_opt ::= ASC",
 /* 301 */ "ordering_specification_opt ::= DESC",
 /* 302 */ "null_ordering_opt ::=",
 /* 303 */ "null_ordering_opt ::= NULLS FIRST",
 /* 304 */ "null_ordering_opt ::= NULLS LAST",
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
      /* Default NON-TERMINAL Destructor */
    case 160: /* cmd */
    case 163: /* literal */
    case 169: /* db_options */
    case 171: /* alter_db_options */
    case 173: /* full_table_name */
    case 176: /* table_options */
    case 180: /* alter_table_clause */
    case 181: /* alter_table_options */
    case 184: /* create_subtable_clause */
    case 187: /* drop_table_clause */
    case 190: /* column_def */
    case 193: /* col_name */
    case 194: /* func_name */
    case 197: /* index_options */
    case 199: /* duration_literal */
    case 200: /* sliding_opt */
    case 201: /* func */
    case 204: /* query_expression */
    case 207: /* expression */
    case 208: /* column_reference */
    case 209: /* subquery */
    case 210: /* predicate */
    case 213: /* in_predicate_value */
    case 214: /* boolean_value_expression */
    case 215: /* boolean_primary */
    case 216: /* common_expression */
    case 217: /* from_clause */
    case 218: /* table_reference_list */
    case 219: /* table_reference */
    case 220: /* table_primary */
    case 221: /* joined_table */
    case 223: /* parenthesized_joined_table */
    case 225: /* search_condition */
    case 226: /* query_specification */
    case 229: /* where_clause_opt */
    case 231: /* twindow_clause_opt */
    case 233: /* having_clause_opt */
    case 235: /* select_item */
    case 236: /* fill_opt */
    case 239: /* query_expression_body */
    case 241: /* slimit_clause_opt */
    case 242: /* limit_clause_opt */
    case 243: /* query_primary */
    case 245: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy168)); 
}
      break;
    case 161: /* account_options */
    case 162: /* account_option */
{
 
}
      break;
    case 164: /* user_name */
    case 165: /* dnode_endpoint */
    case 166: /* dnode_host_name */
    case 168: /* db_name */
    case 182: /* column_name */
    case 189: /* table_name */
    case 195: /* function_name */
    case 196: /* index_name */
    case 203: /* topic_name */
    case 205: /* table_alias */
    case 206: /* column_alias */
    case 222: /* alias_opt */
{
 
}
      break;
    case 167: /* not_exists_opt */
    case 170: /* exists_opt */
    case 227: /* set_quantifier_opt */
{
 
}
      break;
    case 172: /* alter_db_option */
    case 192: /* alter_table_option */
{
 
}
      break;
    case 174: /* column_def_list */
    case 175: /* tags_def_opt */
    case 177: /* multi_create_clause */
    case 178: /* tags_def */
    case 179: /* multi_drop_clause */
    case 185: /* specific_tags_opt */
    case 186: /* literal_list */
    case 188: /* col_name_list */
    case 191: /* func_name_list */
    case 198: /* func_list */
    case 202: /* expression_list */
    case 228: /* select_list */
    case 230: /* partition_by_clause_opt */
    case 232: /* group_by_clause_opt */
    case 234: /* select_sublist */
    case 238: /* group_by_list */
    case 240: /* order_by_clause_opt */
    case 244: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy440)); 
}
      break;
    case 183: /* type_name */
{
 
}
      break;
    case 211: /* compare_op */
    case 212: /* in_op */
{
 
}
      break;
    case 224: /* join_type */
{
 
}
      break;
    case 237: /* fill_mode */
{
 
}
      break;
    case 246: /* ordering_specification_opt */
{
 
}
      break;
    case 247: /* null_ordering_opt */
{
 
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
  {  160,   -4 }, /* (0) cmd ::= ALTER ACCOUNT NK_ID account_options */
  {  161,   -1 }, /* (1) account_options ::= account_option */
  {  161,   -2 }, /* (2) account_options ::= account_options account_option */
  {  162,   -2 }, /* (3) account_option ::= PASS literal */
  {  162,   -2 }, /* (4) account_option ::= PPS literal */
  {  162,   -2 }, /* (5) account_option ::= TSERIES literal */
  {  162,   -2 }, /* (6) account_option ::= STORAGE literal */
  {  162,   -2 }, /* (7) account_option ::= STREAMS literal */
  {  162,   -2 }, /* (8) account_option ::= QTIME literal */
  {  162,   -2 }, /* (9) account_option ::= DBS literal */
  {  162,   -2 }, /* (10) account_option ::= USERS literal */
  {  162,   -2 }, /* (11) account_option ::= CONNS literal */
  {  162,   -2 }, /* (12) account_option ::= STATE literal */
  {  160,   -5 }, /* (13) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  160,   -5 }, /* (14) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  160,   -5 }, /* (15) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  160,   -3 }, /* (16) cmd ::= DROP USER user_name */
  {  160,   -2 }, /* (17) cmd ::= SHOW USERS */
  {  160,   -3 }, /* (18) cmd ::= CREATE DNODE dnode_endpoint */
  {  160,   -5 }, /* (19) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  160,   -3 }, /* (20) cmd ::= DROP DNODE NK_INTEGER */
  {  160,   -3 }, /* (21) cmd ::= DROP DNODE dnode_endpoint */
  {  160,   -2 }, /* (22) cmd ::= SHOW DNODES */
  {  160,   -4 }, /* (23) cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
  {  160,   -5 }, /* (24) cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
  {  160,   -4 }, /* (25) cmd ::= ALTER ALL DNODES NK_STRING */
  {  160,   -5 }, /* (26) cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
  {  165,   -1 }, /* (27) dnode_endpoint ::= NK_STRING */
  {  166,   -1 }, /* (28) dnode_host_name ::= NK_ID */
  {  166,   -1 }, /* (29) dnode_host_name ::= NK_IPTOKEN */
  {  160,   -3 }, /* (30) cmd ::= ALTER LOCAL NK_STRING */
  {  160,   -4 }, /* (31) cmd ::= ALTER LOCAL NK_STRING NK_STRING */
  {  160,   -5 }, /* (32) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  160,   -5 }, /* (33) cmd ::= DROP QNODE ON DNODE NK_INTEGER */
  {  160,   -2 }, /* (34) cmd ::= SHOW QNODES */
  {  160,   -5 }, /* (35) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  160,   -4 }, /* (36) cmd ::= DROP DATABASE exists_opt db_name */
  {  160,   -2 }, /* (37) cmd ::= SHOW DATABASES */
  {  160,   -2 }, /* (38) cmd ::= USE db_name */
  {  160,   -4 }, /* (39) cmd ::= ALTER DATABASE db_name alter_db_options */
  {  167,   -3 }, /* (40) not_exists_opt ::= IF NOT EXISTS */
  {  167,    0 }, /* (41) not_exists_opt ::= */
  {  170,   -2 }, /* (42) exists_opt ::= IF EXISTS */
  {  170,    0 }, /* (43) exists_opt ::= */
  {  169,    0 }, /* (44) db_options ::= */
  {  169,   -3 }, /* (45) db_options ::= db_options BLOCKS NK_INTEGER */
  {  169,   -3 }, /* (46) db_options ::= db_options CACHE NK_INTEGER */
  {  169,   -3 }, /* (47) db_options ::= db_options CACHELAST NK_INTEGER */
  {  169,   -3 }, /* (48) db_options ::= db_options COMP NK_INTEGER */
  {  169,   -3 }, /* (49) db_options ::= db_options DAYS NK_INTEGER */
  {  169,   -3 }, /* (50) db_options ::= db_options FSYNC NK_INTEGER */
  {  169,   -3 }, /* (51) db_options ::= db_options MAXROWS NK_INTEGER */
  {  169,   -3 }, /* (52) db_options ::= db_options MINROWS NK_INTEGER */
  {  169,   -3 }, /* (53) db_options ::= db_options KEEP NK_INTEGER */
  {  169,   -3 }, /* (54) db_options ::= db_options PRECISION NK_STRING */
  {  169,   -3 }, /* (55) db_options ::= db_options QUORUM NK_INTEGER */
  {  169,   -3 }, /* (56) db_options ::= db_options REPLICA NK_INTEGER */
  {  169,   -3 }, /* (57) db_options ::= db_options TTL NK_INTEGER */
  {  169,   -3 }, /* (58) db_options ::= db_options WAL NK_INTEGER */
  {  169,   -3 }, /* (59) db_options ::= db_options VGROUPS NK_INTEGER */
  {  169,   -3 }, /* (60) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  169,   -3 }, /* (61) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  169,   -3 }, /* (62) db_options ::= db_options RETENTIONS NK_STRING */
  {  169,   -3 }, /* (63) db_options ::= db_options FILE_FACTOR NK_FLOAT */
  {  171,   -1 }, /* (64) alter_db_options ::= alter_db_option */
  {  171,   -2 }, /* (65) alter_db_options ::= alter_db_options alter_db_option */
  {  172,   -2 }, /* (66) alter_db_option ::= BLOCKS NK_INTEGER */
  {  172,   -2 }, /* (67) alter_db_option ::= FSYNC NK_INTEGER */
  {  172,   -2 }, /* (68) alter_db_option ::= KEEP NK_INTEGER */
  {  172,   -2 }, /* (69) alter_db_option ::= WAL NK_INTEGER */
  {  172,   -2 }, /* (70) alter_db_option ::= QUORUM NK_INTEGER */
  {  172,   -2 }, /* (71) alter_db_option ::= CACHELAST NK_INTEGER */
  {  160,   -9 }, /* (72) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  160,   -3 }, /* (73) cmd ::= CREATE TABLE multi_create_clause */
  {  160,   -9 }, /* (74) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  160,   -3 }, /* (75) cmd ::= DROP TABLE multi_drop_clause */
  {  160,   -4 }, /* (76) cmd ::= DROP STABLE exists_opt full_table_name */
  {  160,   -2 }, /* (77) cmd ::= SHOW TABLES */
  {  160,   -2 }, /* (78) cmd ::= SHOW STABLES */
  {  160,   -3 }, /* (79) cmd ::= ALTER TABLE alter_table_clause */
  {  160,   -3 }, /* (80) cmd ::= ALTER STABLE alter_table_clause */
  {  180,   -2 }, /* (81) alter_table_clause ::= full_table_name alter_table_options */
  {  180,   -5 }, /* (82) alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
  {  180,   -4 }, /* (83) alter_table_clause ::= full_table_name DROP COLUMN column_name */
  {  180,   -5 }, /* (84) alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
  {  180,   -5 }, /* (85) alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
  {  180,   -5 }, /* (86) alter_table_clause ::= full_table_name ADD TAG column_name type_name */
  {  180,   -4 }, /* (87) alter_table_clause ::= full_table_name DROP TAG column_name */
  {  180,   -5 }, /* (88) alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
  {  180,   -5 }, /* (89) alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
  {  180,   -6 }, /* (90) alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
  {  177,   -1 }, /* (91) multi_create_clause ::= create_subtable_clause */
  {  177,   -2 }, /* (92) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  184,   -9 }, /* (93) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  179,   -1 }, /* (94) multi_drop_clause ::= drop_table_clause */
  {  179,   -2 }, /* (95) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  187,   -2 }, /* (96) drop_table_clause ::= exists_opt full_table_name */
  {  185,    0 }, /* (97) specific_tags_opt ::= */
  {  185,   -3 }, /* (98) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  173,   -1 }, /* (99) full_table_name ::= table_name */
  {  173,   -3 }, /* (100) full_table_name ::= db_name NK_DOT table_name */
  {  174,   -1 }, /* (101) column_def_list ::= column_def */
  {  174,   -3 }, /* (102) column_def_list ::= column_def_list NK_COMMA column_def */
  {  190,   -2 }, /* (103) column_def ::= column_name type_name */
  {  190,   -4 }, /* (104) column_def ::= column_name type_name COMMENT NK_STRING */
  {  183,   -1 }, /* (105) type_name ::= BOOL */
  {  183,   -1 }, /* (106) type_name ::= TINYINT */
  {  183,   -1 }, /* (107) type_name ::= SMALLINT */
  {  183,   -1 }, /* (108) type_name ::= INT */
  {  183,   -1 }, /* (109) type_name ::= INTEGER */
  {  183,   -1 }, /* (110) type_name ::= BIGINT */
  {  183,   -1 }, /* (111) type_name ::= FLOAT */
  {  183,   -1 }, /* (112) type_name ::= DOUBLE */
  {  183,   -4 }, /* (113) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  183,   -1 }, /* (114) type_name ::= TIMESTAMP */
  {  183,   -4 }, /* (115) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  183,   -2 }, /* (116) type_name ::= TINYINT UNSIGNED */
  {  183,   -2 }, /* (117) type_name ::= SMALLINT UNSIGNED */
  {  183,   -2 }, /* (118) type_name ::= INT UNSIGNED */
  {  183,   -2 }, /* (119) type_name ::= BIGINT UNSIGNED */
  {  183,   -1 }, /* (120) type_name ::= JSON */
  {  183,   -4 }, /* (121) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  183,   -1 }, /* (122) type_name ::= MEDIUMBLOB */
  {  183,   -1 }, /* (123) type_name ::= BLOB */
  {  183,   -4 }, /* (124) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  183,   -1 }, /* (125) type_name ::= DECIMAL */
  {  183,   -4 }, /* (126) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  183,   -6 }, /* (127) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  175,    0 }, /* (128) tags_def_opt ::= */
  {  175,   -1 }, /* (129) tags_def_opt ::= tags_def */
  {  178,   -4 }, /* (130) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  176,    0 }, /* (131) table_options ::= */
  {  176,   -3 }, /* (132) table_options ::= table_options COMMENT NK_STRING */
  {  176,   -3 }, /* (133) table_options ::= table_options KEEP NK_INTEGER */
  {  176,   -3 }, /* (134) table_options ::= table_options TTL NK_INTEGER */
  {  176,   -5 }, /* (135) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  176,   -5 }, /* (136) table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
  {  181,   -1 }, /* (137) alter_table_options ::= alter_table_option */
  {  181,   -2 }, /* (138) alter_table_options ::= alter_table_options alter_table_option */
  {  192,   -2 }, /* (139) alter_table_option ::= COMMENT NK_STRING */
  {  192,   -2 }, /* (140) alter_table_option ::= KEEP NK_INTEGER */
  {  192,   -2 }, /* (141) alter_table_option ::= TTL NK_INTEGER */
  {  188,   -1 }, /* (142) col_name_list ::= col_name */
  {  188,   -3 }, /* (143) col_name_list ::= col_name_list NK_COMMA col_name */
  {  193,   -1 }, /* (144) col_name ::= column_name */
  {  191,   -1 }, /* (145) func_name_list ::= func_name */
  {  191,   -3 }, /* (146) func_name_list ::= func_name_list NK_COMMA col_name */
  {  194,   -1 }, /* (147) func_name ::= function_name */
  {  160,   -7 }, /* (148) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  160,   -9 }, /* (149) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  160,   -5 }, /* (150) cmd ::= DROP INDEX index_name ON table_name */
  {  197,    0 }, /* (151) index_options ::= */
  {  197,   -9 }, /* (152) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  197,  -11 }, /* (153) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  198,   -1 }, /* (154) func_list ::= func */
  {  198,   -3 }, /* (155) func_list ::= func_list NK_COMMA func */
  {  201,   -4 }, /* (156) func ::= function_name NK_LP expression_list NK_RP */
  {  160,   -6 }, /* (157) cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
  {  160,   -6 }, /* (158) cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
  {  160,   -4 }, /* (159) cmd ::= DROP TOPIC exists_opt topic_name */
  {  160,   -2 }, /* (160) cmd ::= SHOW VGROUPS */
  {  160,   -4 }, /* (161) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  160,   -2 }, /* (162) cmd ::= SHOW MNODES */
  {  160,   -1 }, /* (163) cmd ::= query_expression */
  {  163,   -1 }, /* (164) literal ::= NK_INTEGER */
  {  163,   -1 }, /* (165) literal ::= NK_FLOAT */
  {  163,   -1 }, /* (166) literal ::= NK_STRING */
  {  163,   -1 }, /* (167) literal ::= NK_BOOL */
  {  163,   -2 }, /* (168) literal ::= TIMESTAMP NK_STRING */
  {  163,   -1 }, /* (169) literal ::= duration_literal */
  {  199,   -1 }, /* (170) duration_literal ::= NK_VARIABLE */
  {  186,   -1 }, /* (171) literal_list ::= literal */
  {  186,   -3 }, /* (172) literal_list ::= literal_list NK_COMMA literal */
  {  168,   -1 }, /* (173) db_name ::= NK_ID */
  {  189,   -1 }, /* (174) table_name ::= NK_ID */
  {  182,   -1 }, /* (175) column_name ::= NK_ID */
  {  195,   -1 }, /* (176) function_name ::= NK_ID */
  {  205,   -1 }, /* (177) table_alias ::= NK_ID */
  {  206,   -1 }, /* (178) column_alias ::= NK_ID */
  {  164,   -1 }, /* (179) user_name ::= NK_ID */
  {  196,   -1 }, /* (180) index_name ::= NK_ID */
  {  203,   -1 }, /* (181) topic_name ::= NK_ID */
  {  207,   -1 }, /* (182) expression ::= literal */
  {  207,   -1 }, /* (183) expression ::= column_reference */
  {  207,   -4 }, /* (184) expression ::= function_name NK_LP expression_list NK_RP */
  {  207,   -4 }, /* (185) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  207,   -1 }, /* (186) expression ::= subquery */
  {  207,   -3 }, /* (187) expression ::= NK_LP expression NK_RP */
  {  207,   -2 }, /* (188) expression ::= NK_PLUS expression */
  {  207,   -2 }, /* (189) expression ::= NK_MINUS expression */
  {  207,   -3 }, /* (190) expression ::= expression NK_PLUS expression */
  {  207,   -3 }, /* (191) expression ::= expression NK_MINUS expression */
  {  207,   -3 }, /* (192) expression ::= expression NK_STAR expression */
  {  207,   -3 }, /* (193) expression ::= expression NK_SLASH expression */
  {  207,   -3 }, /* (194) expression ::= expression NK_REM expression */
  {  202,   -1 }, /* (195) expression_list ::= expression */
  {  202,   -3 }, /* (196) expression_list ::= expression_list NK_COMMA expression */
  {  208,   -1 }, /* (197) column_reference ::= column_name */
  {  208,   -3 }, /* (198) column_reference ::= table_name NK_DOT column_name */
  {  210,   -3 }, /* (199) predicate ::= expression compare_op expression */
  {  210,   -5 }, /* (200) predicate ::= expression BETWEEN expression AND expression */
  {  210,   -6 }, /* (201) predicate ::= expression NOT BETWEEN expression AND expression */
  {  210,   -3 }, /* (202) predicate ::= expression IS NULL */
  {  210,   -4 }, /* (203) predicate ::= expression IS NOT NULL */
  {  210,   -3 }, /* (204) predicate ::= expression in_op in_predicate_value */
  {  211,   -1 }, /* (205) compare_op ::= NK_LT */
  {  211,   -1 }, /* (206) compare_op ::= NK_GT */
  {  211,   -1 }, /* (207) compare_op ::= NK_LE */
  {  211,   -1 }, /* (208) compare_op ::= NK_GE */
  {  211,   -1 }, /* (209) compare_op ::= NK_NE */
  {  211,   -1 }, /* (210) compare_op ::= NK_EQ */
  {  211,   -1 }, /* (211) compare_op ::= LIKE */
  {  211,   -2 }, /* (212) compare_op ::= NOT LIKE */
  {  211,   -1 }, /* (213) compare_op ::= MATCH */
  {  211,   -1 }, /* (214) compare_op ::= NMATCH */
  {  212,   -1 }, /* (215) in_op ::= IN */
  {  212,   -2 }, /* (216) in_op ::= NOT IN */
  {  213,   -3 }, /* (217) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  214,   -1 }, /* (218) boolean_value_expression ::= boolean_primary */
  {  214,   -2 }, /* (219) boolean_value_expression ::= NOT boolean_primary */
  {  214,   -3 }, /* (220) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  214,   -3 }, /* (221) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  215,   -1 }, /* (222) boolean_primary ::= predicate */
  {  215,   -3 }, /* (223) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  216,   -1 }, /* (224) common_expression ::= expression */
  {  216,   -1 }, /* (225) common_expression ::= boolean_value_expression */
  {  217,   -2 }, /* (226) from_clause ::= FROM table_reference_list */
  {  218,   -1 }, /* (227) table_reference_list ::= table_reference */
  {  218,   -3 }, /* (228) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  219,   -1 }, /* (229) table_reference ::= table_primary */
  {  219,   -1 }, /* (230) table_reference ::= joined_table */
  {  220,   -2 }, /* (231) table_primary ::= table_name alias_opt */
  {  220,   -4 }, /* (232) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  220,   -2 }, /* (233) table_primary ::= subquery alias_opt */
  {  220,   -1 }, /* (234) table_primary ::= parenthesized_joined_table */
  {  222,    0 }, /* (235) alias_opt ::= */
  {  222,   -1 }, /* (236) alias_opt ::= table_alias */
  {  222,   -2 }, /* (237) alias_opt ::= AS table_alias */
  {  223,   -3 }, /* (238) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  223,   -3 }, /* (239) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  221,   -6 }, /* (240) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  224,    0 }, /* (241) join_type ::= */
  {  224,   -1 }, /* (242) join_type ::= INNER */
  {  226,   -9 }, /* (243) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  227,    0 }, /* (244) set_quantifier_opt ::= */
  {  227,   -1 }, /* (245) set_quantifier_opt ::= DISTINCT */
  {  227,   -1 }, /* (246) set_quantifier_opt ::= ALL */
  {  228,   -1 }, /* (247) select_list ::= NK_STAR */
  {  228,   -1 }, /* (248) select_list ::= select_sublist */
  {  234,   -1 }, /* (249) select_sublist ::= select_item */
  {  234,   -3 }, /* (250) select_sublist ::= select_sublist NK_COMMA select_item */
  {  235,   -1 }, /* (251) select_item ::= common_expression */
  {  235,   -2 }, /* (252) select_item ::= common_expression column_alias */
  {  235,   -3 }, /* (253) select_item ::= common_expression AS column_alias */
  {  235,   -3 }, /* (254) select_item ::= table_name NK_DOT NK_STAR */
  {  229,    0 }, /* (255) where_clause_opt ::= */
  {  229,   -2 }, /* (256) where_clause_opt ::= WHERE search_condition */
  {  230,    0 }, /* (257) partition_by_clause_opt ::= */
  {  230,   -3 }, /* (258) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  231,    0 }, /* (259) twindow_clause_opt ::= */
  {  231,   -6 }, /* (260) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  231,   -4 }, /* (261) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  231,   -6 }, /* (262) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  231,   -8 }, /* (263) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  200,    0 }, /* (264) sliding_opt ::= */
  {  200,   -4 }, /* (265) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  236,    0 }, /* (266) fill_opt ::= */
  {  236,   -4 }, /* (267) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  236,   -6 }, /* (268) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  237,   -1 }, /* (269) fill_mode ::= NONE */
  {  237,   -1 }, /* (270) fill_mode ::= PREV */
  {  237,   -1 }, /* (271) fill_mode ::= NULL */
  {  237,   -1 }, /* (272) fill_mode ::= LINEAR */
  {  237,   -1 }, /* (273) fill_mode ::= NEXT */
  {  232,    0 }, /* (274) group_by_clause_opt ::= */
  {  232,   -3 }, /* (275) group_by_clause_opt ::= GROUP BY group_by_list */
  {  238,   -1 }, /* (276) group_by_list ::= expression */
  {  238,   -3 }, /* (277) group_by_list ::= group_by_list NK_COMMA expression */
  {  233,    0 }, /* (278) having_clause_opt ::= */
  {  233,   -2 }, /* (279) having_clause_opt ::= HAVING search_condition */
  {  204,   -4 }, /* (280) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  239,   -1 }, /* (281) query_expression_body ::= query_primary */
  {  239,   -4 }, /* (282) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  243,   -1 }, /* (283) query_primary ::= query_specification */
  {  240,    0 }, /* (284) order_by_clause_opt ::= */
  {  240,   -3 }, /* (285) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  241,    0 }, /* (286) slimit_clause_opt ::= */
  {  241,   -2 }, /* (287) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  241,   -4 }, /* (288) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  241,   -4 }, /* (289) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  242,    0 }, /* (290) limit_clause_opt ::= */
  {  242,   -2 }, /* (291) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  242,   -4 }, /* (292) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  242,   -4 }, /* (293) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  209,   -3 }, /* (294) subquery ::= NK_LP query_expression NK_RP */
  {  225,   -1 }, /* (295) search_condition ::= common_expression */
  {  244,   -1 }, /* (296) sort_specification_list ::= sort_specification */
  {  244,   -3 }, /* (297) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  245,   -3 }, /* (298) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  246,    0 }, /* (299) ordering_specification_opt ::= */
  {  246,   -1 }, /* (300) ordering_specification_opt ::= ASC */
  {  246,   -1 }, /* (301) ordering_specification_opt ::= DESC */
  {  247,    0 }, /* (302) null_ordering_opt ::= */
  {  247,   -2 }, /* (303) null_ordering_opt ::= NULLS FIRST */
  {  247,   -2 }, /* (304) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= ALTER ACCOUNT NK_ID account_options */
{ pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
  yy_destructor(yypParser,161,&yymsp[0].minor);
        break;
      case 1: /* account_options ::= account_option */
{  yy_destructor(yypParser,162,&yymsp[0].minor);
{ }
}
        break;
      case 2: /* account_options ::= account_options account_option */
{  yy_destructor(yypParser,161,&yymsp[-1].minor);
{ }
  yy_destructor(yypParser,162,&yymsp[0].minor);
}
        break;
      case 3: /* account_option ::= PASS literal */
      case 4: /* account_option ::= PPS literal */ yytestcase(yyruleno==4);
      case 5: /* account_option ::= TSERIES literal */ yytestcase(yyruleno==5);
      case 6: /* account_option ::= STORAGE literal */ yytestcase(yyruleno==6);
      case 7: /* account_option ::= STREAMS literal */ yytestcase(yyruleno==7);
      case 8: /* account_option ::= QTIME literal */ yytestcase(yyruleno==8);
      case 9: /* account_option ::= DBS literal */ yytestcase(yyruleno==9);
      case 10: /* account_option ::= USERS literal */ yytestcase(yyruleno==10);
      case 11: /* account_option ::= CONNS literal */ yytestcase(yyruleno==11);
      case 12: /* account_option ::= STATE literal */ yytestcase(yyruleno==12);
{ }
  yy_destructor(yypParser,163,&yymsp[0].minor);
        break;
      case 13: /* cmd ::= CREATE USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy0); }
        break;
      case 14: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy241, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0); }
        break;
      case 15: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy241, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0); }
        break;
      case 16: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy241); }
        break;
      case 17: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 18: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy241, NULL); }
        break;
      case 19: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy0); }
        break;
      case 20: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 21: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy241); }
        break;
      case 22: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 23: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, NULL); }
        break;
      case 24: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 25: /* cmd ::= ALTER ALL DNODES NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[0].minor.yy0, NULL); }
        break;
      case 26: /* cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 27: /* dnode_endpoint ::= NK_STRING */
      case 28: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==28);
      case 29: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==29);
      case 173: /* db_name ::= NK_ID */ yytestcase(yyruleno==173);
      case 174: /* table_name ::= NK_ID */ yytestcase(yyruleno==174);
      case 175: /* column_name ::= NK_ID */ yytestcase(yyruleno==175);
      case 176: /* function_name ::= NK_ID */ yytestcase(yyruleno==176);
      case 177: /* table_alias ::= NK_ID */ yytestcase(yyruleno==177);
      case 178: /* column_alias ::= NK_ID */ yytestcase(yyruleno==178);
      case 179: /* user_name ::= NK_ID */ yytestcase(yyruleno==179);
      case 180: /* index_name ::= NK_ID */ yytestcase(yyruleno==180);
      case 181: /* topic_name ::= NK_ID */ yytestcase(yyruleno==181);
{ yylhsminor.yy241 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy241 = yylhsminor.yy241;
        break;
      case 30: /* cmd ::= ALTER LOCAL NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 31: /* cmd ::= ALTER LOCAL NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 32: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createCreateQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 33: /* cmd ::= DROP QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 34: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL); }
        break;
      case 35: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy457, &yymsp[-1].minor.yy241, yymsp[0].minor.yy168); }
        break;
      case 36: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy457, &yymsp[0].minor.yy241); }
        break;
      case 37: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 38: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy241); }
        break;
      case 39: /* cmd ::= ALTER DATABASE db_name alter_db_options */
{ pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &yymsp[-1].minor.yy241, yymsp[0].minor.yy168); }
        break;
      case 40: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy457 = true; }
        break;
      case 41: /* not_exists_opt ::= */
      case 43: /* exists_opt ::= */ yytestcase(yyruleno==43);
      case 244: /* set_quantifier_opt ::= */ yytestcase(yyruleno==244);
{ yymsp[1].minor.yy457 = false; }
        break;
      case 42: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy457 = true; }
        break;
      case 44: /* db_options ::= */
{ yymsp[1].minor.yy168 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 45: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 46: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 47: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 48: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 49: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 50: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 51: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 52: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 53: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 54: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 55: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 56: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 57: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 58: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 59: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 60: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_SINGLE_STABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 61: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_STREAM_MODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 62: /* db_options ::= db_options RETENTIONS NK_STRING */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_RETENTIONS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 63: /* db_options ::= db_options FILE_FACTOR NK_FLOAT */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-2].minor.yy168, DB_OPTION_FILE_FACTOR, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 64: /* alter_db_options ::= alter_db_option */
{ yylhsminor.yy168 = createDefaultAlterDatabaseOptions(pCxt); yylhsminor.yy168 = setDatabaseOption(pCxt, yylhsminor.yy168, yymsp[0].minor.yy349.type, &yymsp[0].minor.yy349.val); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 65: /* alter_db_options ::= alter_db_options alter_db_option */
{ yylhsminor.yy168 = setDatabaseOption(pCxt, yymsp[-1].minor.yy168, yymsp[0].minor.yy349.type, &yymsp[0].minor.yy349.val); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 66: /* alter_db_option ::= BLOCKS NK_INTEGER */
{ yymsp[-1].minor.yy349.type = DB_OPTION_BLOCKS; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 67: /* alter_db_option ::= FSYNC NK_INTEGER */
{ yymsp[-1].minor.yy349.type = DB_OPTION_FSYNC; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 68: /* alter_db_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy349.type = DB_OPTION_KEEP; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 69: /* alter_db_option ::= WAL NK_INTEGER */
{ yymsp[-1].minor.yy349.type = DB_OPTION_WAL; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 70: /* alter_db_option ::= QUORUM NK_INTEGER */
{ yymsp[-1].minor.yy349.type = DB_OPTION_QUORUM; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 71: /* alter_db_option ::= CACHELAST NK_INTEGER */
{ yymsp[-1].minor.yy349.type = DB_OPTION_CACHELAST; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 72: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 74: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==74);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy457, yymsp[-5].minor.yy168, yymsp[-3].minor.yy440, yymsp[-1].minor.yy440, yymsp[0].minor.yy168); }
        break;
      case 73: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy440); }
        break;
      case 75: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy440); }
        break;
      case 76: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy457, yymsp[0].minor.yy168); }
        break;
      case 77: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 78: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 79: /* cmd ::= ALTER TABLE alter_table_clause */
      case 80: /* cmd ::= ALTER STABLE alter_table_clause */ yytestcase(yyruleno==80);
      case 163: /* cmd ::= query_expression */ yytestcase(yyruleno==163);
{ pCxt->pRootNode = yymsp[0].minor.yy168; }
        break;
      case 81: /* alter_table_clause ::= full_table_name alter_table_options */
{ yylhsminor.yy168 = createAlterTableOption(pCxt, yymsp[-1].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 82: /* alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
      case 84: /* alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */ yytestcase(yyruleno==84);
      case 86: /* alter_table_clause ::= full_table_name ADD TAG column_name type_name */ yytestcase(yyruleno==86);
      case 88: /* alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */ yytestcase(yyruleno==88);
{ yylhsminor.yy168 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy168, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[-1].minor.yy241, yymsp[0].minor.yy208); }
  yymsp[-4].minor.yy168 = yylhsminor.yy168;
        break;
      case 83: /* alter_table_clause ::= full_table_name DROP COLUMN column_name */
      case 87: /* alter_table_clause ::= full_table_name DROP TAG column_name */ yytestcase(yyruleno==87);
{ yylhsminor.yy168 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy168, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[0].minor.yy241); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 85: /* alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
      case 89: /* alter_table_clause ::= full_table_name RENAME TAG column_name column_name */ yytestcase(yyruleno==89);
{ yylhsminor.yy168 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy168, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[-1].minor.yy241, &yymsp[0].minor.yy241); }
  yymsp[-4].minor.yy168 = yylhsminor.yy168;
        break;
      case 90: /* alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
{ yylhsminor.yy168 = createAlterTableSetTag(pCxt, yymsp[-5].minor.yy168, &yymsp[-2].minor.yy241, yymsp[0].minor.yy168); }
  yymsp[-5].minor.yy168 = yylhsminor.yy168;
        break;
      case 91: /* multi_create_clause ::= create_subtable_clause */
      case 94: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==94);
      case 101: /* column_def_list ::= column_def */ yytestcase(yyruleno==101);
      case 142: /* col_name_list ::= col_name */ yytestcase(yyruleno==142);
      case 145: /* func_name_list ::= func_name */ yytestcase(yyruleno==145);
      case 154: /* func_list ::= func */ yytestcase(yyruleno==154);
      case 249: /* select_sublist ::= select_item */ yytestcase(yyruleno==249);
      case 296: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==296);
{ yylhsminor.yy440 = createNodeList(pCxt, yymsp[0].minor.yy168); }
  yymsp[0].minor.yy440 = yylhsminor.yy440;
        break;
      case 92: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 95: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==95);
{ yylhsminor.yy440 = addNodeToList(pCxt, yymsp[-1].minor.yy440, yymsp[0].minor.yy168); }
  yymsp[-1].minor.yy440 = yylhsminor.yy440;
        break;
      case 93: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy168 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy457, yymsp[-7].minor.yy168, yymsp[-5].minor.yy168, yymsp[-4].minor.yy440, yymsp[-1].minor.yy440); }
  yymsp[-8].minor.yy168 = yylhsminor.yy168;
        break;
      case 96: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy168 = createDropTableClause(pCxt, yymsp[-1].minor.yy457, yymsp[0].minor.yy168); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 97: /* specific_tags_opt ::= */
      case 128: /* tags_def_opt ::= */ yytestcase(yyruleno==128);
      case 257: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==257);
      case 274: /* group_by_clause_opt ::= */ yytestcase(yyruleno==274);
      case 284: /* order_by_clause_opt ::= */ yytestcase(yyruleno==284);
{ yymsp[1].minor.yy440 = NULL; }
        break;
      case 98: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy440 = yymsp[-1].minor.yy440; }
        break;
      case 99: /* full_table_name ::= table_name */
{ yylhsminor.yy168 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy241, NULL); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 100: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy168 = createRealTableNode(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy241, NULL); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 102: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 143: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==143);
      case 146: /* func_name_list ::= func_name_list NK_COMMA col_name */ yytestcase(yyruleno==146);
      case 155: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==155);
      case 250: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==250);
      case 297: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==297);
{ yylhsminor.yy440 = addNodeToList(pCxt, yymsp[-2].minor.yy440, yymsp[0].minor.yy168); }
  yymsp[-2].minor.yy440 = yylhsminor.yy440;
        break;
      case 103: /* column_def ::= column_name type_name */
{ yylhsminor.yy168 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy241, yymsp[0].minor.yy208, NULL); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 104: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy168 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy241, yymsp[-2].minor.yy208, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 105: /* type_name ::= BOOL */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 106: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 107: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 108: /* type_name ::= INT */
      case 109: /* type_name ::= INTEGER */ yytestcase(yyruleno==109);
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 110: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 111: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 112: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 113: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy208 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 114: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 115: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy208 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 116: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy208 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 117: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy208 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 118: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy208 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 119: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy208 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 120: /* type_name ::= JSON */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 121: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy208 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 122: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 123: /* type_name ::= BLOB */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 124: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy208 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 125: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy208 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 126: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy208 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 127: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy208 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 129: /* tags_def_opt ::= tags_def */
      case 248: /* select_list ::= select_sublist */ yytestcase(yyruleno==248);
{ yylhsminor.yy440 = yymsp[0].minor.yy440; }
  yymsp[0].minor.yy440 = yylhsminor.yy440;
        break;
      case 130: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy440 = yymsp[-1].minor.yy440; }
        break;
      case 131: /* table_options ::= */
{ yymsp[1].minor.yy168 = createDefaultTableOptions(pCxt); }
        break;
      case 132: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy168 = setTableOption(pCxt, yymsp[-2].minor.yy168, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 133: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy168 = setTableOption(pCxt, yymsp[-2].minor.yy168, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 134: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy168 = setTableOption(pCxt, yymsp[-2].minor.yy168, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 135: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy168 = setTableSmaOption(pCxt, yymsp[-4].minor.yy168, yymsp[-1].minor.yy440); }
  yymsp[-4].minor.yy168 = yylhsminor.yy168;
        break;
      case 136: /* table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
{ yylhsminor.yy168 = setTableRollupOption(pCxt, yymsp[-4].minor.yy168, yymsp[-1].minor.yy440); }
  yymsp[-4].minor.yy168 = yylhsminor.yy168;
        break;
      case 137: /* alter_table_options ::= alter_table_option */
{ yylhsminor.yy168 = createDefaultAlterTableOptions(pCxt); yylhsminor.yy168 = setTableOption(pCxt, yylhsminor.yy168, yymsp[0].minor.yy349.type, &yymsp[0].minor.yy349.val); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 138: /* alter_table_options ::= alter_table_options alter_table_option */
{ yylhsminor.yy168 = setTableOption(pCxt, yymsp[-1].minor.yy168, yymsp[0].minor.yy349.type, &yymsp[0].minor.yy349.val); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 139: /* alter_table_option ::= COMMENT NK_STRING */
{ yymsp[-1].minor.yy349.type = TABLE_OPTION_COMMENT; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 140: /* alter_table_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy349.type = TABLE_OPTION_KEEP; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 141: /* alter_table_option ::= TTL NK_INTEGER */
{ yymsp[-1].minor.yy349.type = TABLE_OPTION_TTL; yymsp[-1].minor.yy349.val = yymsp[0].minor.yy0; }
        break;
      case 144: /* col_name ::= column_name */
{ yylhsminor.yy168 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy241); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 147: /* func_name ::= function_name */
{ yylhsminor.yy168 = createFunctionNode(pCxt, &yymsp[0].minor.yy241, NULL); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 148: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy241, &yymsp[-1].minor.yy241, NULL, yymsp[0].minor.yy168); }
        break;
      case 149: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy241, &yymsp[-3].minor.yy241, yymsp[-1].minor.yy440, NULL); }
        break;
      case 150: /* cmd ::= DROP INDEX index_name ON table_name */
{ pCxt->pRootNode = createDropIndexStmt(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy241); }
        break;
      case 151: /* index_options ::= */
      case 255: /* where_clause_opt ::= */ yytestcase(yyruleno==255);
      case 259: /* twindow_clause_opt ::= */ yytestcase(yyruleno==259);
      case 264: /* sliding_opt ::= */ yytestcase(yyruleno==264);
      case 266: /* fill_opt ::= */ yytestcase(yyruleno==266);
      case 278: /* having_clause_opt ::= */ yytestcase(yyruleno==278);
      case 286: /* slimit_clause_opt ::= */ yytestcase(yyruleno==286);
      case 290: /* limit_clause_opt ::= */ yytestcase(yyruleno==290);
{ yymsp[1].minor.yy168 = NULL; }
        break;
      case 152: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy168 = createIndexOption(pCxt, yymsp[-6].minor.yy440, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), NULL, yymsp[0].minor.yy168); }
        break;
      case 153: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy168 = createIndexOption(pCxt, yymsp[-8].minor.yy440, releaseRawExprNode(pCxt, yymsp[-4].minor.yy168), releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), yymsp[0].minor.yy168); }
        break;
      case 156: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy168 = createFunctionNode(pCxt, &yymsp[-3].minor.yy241, yymsp[-1].minor.yy440); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 157: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy457, &yymsp[-2].minor.yy241, yymsp[0].minor.yy168, NULL); }
        break;
      case 158: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy457, &yymsp[-2].minor.yy241, NULL, &yymsp[0].minor.yy241); }
        break;
      case 159: /* cmd ::= DROP TOPIC exists_opt topic_name */
{ pCxt->pRootNode = createDropTopicStmt(pCxt, yymsp[-1].minor.yy457, &yymsp[0].minor.yy241); }
        break;
      case 160: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 161: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy241); }
        break;
      case 162: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 164: /* literal ::= NK_INTEGER */
{ yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 165: /* literal ::= NK_FLOAT */
{ yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 166: /* literal ::= NK_STRING */
{ yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 167: /* literal ::= NK_BOOL */
{ yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 168: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 169: /* literal ::= duration_literal */
      case 182: /* expression ::= literal */ yytestcase(yyruleno==182);
      case 183: /* expression ::= column_reference */ yytestcase(yyruleno==183);
      case 186: /* expression ::= subquery */ yytestcase(yyruleno==186);
      case 218: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==218);
      case 222: /* boolean_primary ::= predicate */ yytestcase(yyruleno==222);
      case 224: /* common_expression ::= expression */ yytestcase(yyruleno==224);
      case 225: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==225);
      case 227: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==227);
      case 229: /* table_reference ::= table_primary */ yytestcase(yyruleno==229);
      case 230: /* table_reference ::= joined_table */ yytestcase(yyruleno==230);
      case 234: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==234);
      case 281: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==281);
      case 283: /* query_primary ::= query_specification */ yytestcase(yyruleno==283);
{ yylhsminor.yy168 = yymsp[0].minor.yy168; }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 170: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 171: /* literal_list ::= literal */
      case 195: /* expression_list ::= expression */ yytestcase(yyruleno==195);
{ yylhsminor.yy440 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[0].minor.yy440 = yylhsminor.yy440;
        break;
      case 172: /* literal_list ::= literal_list NK_COMMA literal */
      case 196: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==196);
{ yylhsminor.yy440 = addNodeToList(pCxt, yymsp[-2].minor.yy440, releaseRawExprNode(pCxt, yymsp[0].minor.yy168)); }
  yymsp[-2].minor.yy440 = yylhsminor.yy440;
        break;
      case 184: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy241, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy241, yymsp[-1].minor.yy440)); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 185: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy241, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy241, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 187: /* expression ::= NK_LP expression NK_RP */
      case 223: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==223);
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168)); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 188: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy168));
                                                                                  }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 189: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy168), NULL));
                                                                                  }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 190: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 191: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 192: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 193: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 194: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); 
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 197: /* column_reference ::= column_name */
{ yylhsminor.yy168 = createRawExprNode(pCxt, &yymsp[0].minor.yy241, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy241)); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 198: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy241, createColumnNode(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy241)); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 199: /* predicate ::= expression compare_op expression */
      case 204: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==204);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy476, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)));
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 200: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy168), releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)));
                                                                                  }
  yymsp[-4].minor.yy168 = yylhsminor.yy168;
        break;
      case 201: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[-5].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)));
                                                                                  }
  yymsp[-5].minor.yy168 = yylhsminor.yy168;
        break;
      case 202: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), NULL));
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 203: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy168), NULL));
                                                                                  }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 205: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy476 = OP_TYPE_LOWER_THAN; }
        break;
      case 206: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy476 = OP_TYPE_GREATER_THAN; }
        break;
      case 207: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy476 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 208: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy476 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 209: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy476 = OP_TYPE_NOT_EQUAL; }
        break;
      case 210: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy476 = OP_TYPE_EQUAL; }
        break;
      case 211: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy476 = OP_TYPE_LIKE; }
        break;
      case 212: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy476 = OP_TYPE_NOT_LIKE; }
        break;
      case 213: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy476 = OP_TYPE_MATCH; }
        break;
      case 214: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy476 = OP_TYPE_NMATCH; }
        break;
      case 215: /* in_op ::= IN */
{ yymsp[0].minor.yy476 = OP_TYPE_IN; }
        break;
      case 216: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy476 = OP_TYPE_NOT_IN; }
        break;
      case 217: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy440)); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 219: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy168), NULL));
                                                                                  }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 220: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)));
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 221: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy168);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), releaseRawExprNode(pCxt, yymsp[0].minor.yy168)));
                                                                                  }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 226: /* from_clause ::= FROM table_reference_list */
      case 256: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==256);
      case 279: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==279);
{ yymsp[-1].minor.yy168 = yymsp[0].minor.yy168; }
        break;
      case 228: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy168 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy168, yymsp[0].minor.yy168, NULL); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 231: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy168 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy241, &yymsp[0].minor.yy241); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 232: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy168 = createRealTableNode(pCxt, &yymsp[-3].minor.yy241, &yymsp[-1].minor.yy241, &yymsp[0].minor.yy241); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 233: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy168 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168), &yymsp[0].minor.yy241); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 235: /* alias_opt ::= */
{ yymsp[1].minor.yy241 = nil_token;  }
        break;
      case 236: /* alias_opt ::= table_alias */
{ yylhsminor.yy241 = yymsp[0].minor.yy241; }
  yymsp[0].minor.yy241 = yylhsminor.yy241;
        break;
      case 237: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy241 = yymsp[0].minor.yy241; }
        break;
      case 238: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 239: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==239);
{ yymsp[-2].minor.yy168 = yymsp[-1].minor.yy168; }
        break;
      case 240: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy168 = createJoinTableNode(pCxt, yymsp[-4].minor.yy228, yymsp[-5].minor.yy168, yymsp[-2].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-5].minor.yy168 = yylhsminor.yy168;
        break;
      case 241: /* join_type ::= */
{ yymsp[1].minor.yy228 = JOIN_TYPE_INNER; }
        break;
      case 242: /* join_type ::= INNER */
{ yymsp[0].minor.yy228 = JOIN_TYPE_INNER; }
        break;
      case 243: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy168 = createSelectStmt(pCxt, yymsp[-7].minor.yy457, yymsp[-6].minor.yy440, yymsp[-5].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addWhereClause(pCxt, yymsp[-8].minor.yy168, yymsp[-4].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addPartitionByClause(pCxt, yymsp[-8].minor.yy168, yymsp[-3].minor.yy440);
                                                                                    yymsp[-8].minor.yy168 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy168, yymsp[-2].minor.yy168);
                                                                                    yymsp[-8].minor.yy168 = addGroupByClause(pCxt, yymsp[-8].minor.yy168, yymsp[-1].minor.yy440);
                                                                                    yymsp[-8].minor.yy168 = addHavingClause(pCxt, yymsp[-8].minor.yy168, yymsp[0].minor.yy168);
                                                                                  }
        break;
      case 245: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy457 = true; }
        break;
      case 246: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy457 = false; }
        break;
      case 247: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy440 = NULL; }
        break;
      case 251: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy168);
                                                                                    yylhsminor.yy168 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy168), &t);
                                                                                  }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 252: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy168 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168), &yymsp[0].minor.yy241); }
  yymsp[-1].minor.yy168 = yylhsminor.yy168;
        break;
      case 253: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy168 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), &yymsp[0].minor.yy241); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 254: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy168 = createColumnNode(pCxt, &yymsp[-2].minor.yy241, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 258: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 275: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==275);
      case 285: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==285);
{ yymsp[-2].minor.yy440 = yymsp[0].minor.yy440; }
        break;
      case 260: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy168 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy168), &yymsp[-1].minor.yy0); }
        break;
      case 261: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy168 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy168)); }
        break;
      case 262: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy168 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy168), NULL, yymsp[-1].minor.yy168, yymsp[0].minor.yy168); }
        break;
      case 263: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy168 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy168), releaseRawExprNode(pCxt, yymsp[-3].minor.yy168), yymsp[-1].minor.yy168, yymsp[0].minor.yy168); }
        break;
      case 265: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy168 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy168); }
        break;
      case 267: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy168 = createFillNode(pCxt, yymsp[-1].minor.yy262, NULL); }
        break;
      case 268: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy168 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy440)); }
        break;
      case 269: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy262 = FILL_MODE_NONE; }
        break;
      case 270: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy262 = FILL_MODE_PREV; }
        break;
      case 271: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy262 = FILL_MODE_NULL; }
        break;
      case 272: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy262 = FILL_MODE_LINEAR; }
        break;
      case 273: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy262 = FILL_MODE_NEXT; }
        break;
      case 276: /* group_by_list ::= expression */
{ yylhsminor.yy440 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); }
  yymsp[0].minor.yy440 = yylhsminor.yy440;
        break;
      case 277: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy440 = addNodeToList(pCxt, yymsp[-2].minor.yy440, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy168))); }
  yymsp[-2].minor.yy440 = yylhsminor.yy440;
        break;
      case 280: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy168 = addOrderByClause(pCxt, yymsp[-3].minor.yy168, yymsp[-2].minor.yy440);
                                                                                    yylhsminor.yy168 = addSlimitClause(pCxt, yylhsminor.yy168, yymsp[-1].minor.yy168);
                                                                                    yylhsminor.yy168 = addLimitClause(pCxt, yylhsminor.yy168, yymsp[0].minor.yy168);
                                                                                  }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 282: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy168 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy168, yymsp[0].minor.yy168); }
  yymsp[-3].minor.yy168 = yylhsminor.yy168;
        break;
      case 287: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 291: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==291);
{ yymsp[-1].minor.yy168 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 288: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 292: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==292);
{ yymsp[-3].minor.yy168 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 289: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 293: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==293);
{ yymsp[-3].minor.yy168 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 294: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy168 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy168); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 295: /* search_condition ::= common_expression */
{ yylhsminor.yy168 = releaseRawExprNode(pCxt, yymsp[0].minor.yy168); }
  yymsp[0].minor.yy168 = yylhsminor.yy168;
        break;
      case 298: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy168 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy168), yymsp[-1].minor.yy258, yymsp[0].minor.yy425); }
  yymsp[-2].minor.yy168 = yylhsminor.yy168;
        break;
      case 299: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy258 = ORDER_ASC; }
        break;
      case 300: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy258 = ORDER_ASC; }
        break;
      case 301: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy258 = ORDER_DESC; }
        break;
      case 302: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy425 = NULL_ORDER_DEFAULT; }
        break;
      case 303: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy425 = NULL_ORDER_FIRST; }
        break;
      case 304: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy425 = NULL_ORDER_LAST; }
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

  if (pCxt->valid) {
    if(TOKEN.z) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
    } else {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
    }
    pCxt->valid = false;
  }
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
