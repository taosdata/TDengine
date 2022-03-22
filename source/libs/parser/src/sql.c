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
#define YYNOCODE 251
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int32_t yy88;
  EJoinType yy184;
  EOperatorType yy194;
  SDataType yy218;
  SAlterOption yy233;
  EFillMode yy256;
  SToken yy269;
  ENullOrder yy339;
  bool yy345;
  SNode* yy348;
  SNodeList* yy358;
  EOrder yy368;
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
#define YYNSTATE             417
#define YYNRULE              327
#define YYNTOKEN             160
#define YY_MAX_SHIFT         416
#define YY_MIN_SHIFTREDUCE   645
#define YY_MAX_SHIFTREDUCE   971
#define YY_ERROR_ACTION      972
#define YY_ACCEPT_ACTION     973
#define YY_NO_ACTION         974
#define YY_MIN_REDUCE        975
#define YY_MAX_REDUCE        1301
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
#define YY_ACTTAB_COUNT (1198)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */  1021, 1167,  180,   43, 1194, 1115,  215, 1163, 1169,   24,
 /*    10 */   162,  333,   88,   31,   29,   27,   26,   25,  976, 1167,
 /*    20 */  1080,  232,   27,   26,   25, 1163, 1168,   31,   29,   27,
 /*    30 */    26,   25,  306,  235, 1167, 1076, 1131, 1133, 1074,   77,
 /*    40 */  1163, 1168,   76,   75,   74,   73,   72,   71,   70,   69,
 /*    50 */    68,  201,  401,  400,  399,  398,  397,  396,  395,  394,
 /*    60 */   393,  392,  391,  390,  389,  388,  387,  386,  385,  384,
 /*    70 */   383,  840, 1155,   30,   28,  854,   31,   29,   27,   26,
 /*    80 */    25,  224,  348,  822,   77,  877,   43,   76,   75,   74,
 /*    90 */    73,   72,   71,   70,   69,   68,  277,  349,  272, 1130,
 /*   100 */   820,  276,  255, 1081,  275,  214,  273,    9,    8,  274,
 /*   110 */  1128,   12,  975,  336,  201,  348,  227,    6, 1085, 1140,
 /*   120 */    23,  222,  108,  872,  873,  874,  875,  876,  878,  880,
 /*   130 */   881,  882,  821, 1123,    1,   10,   86,   85,   84,   83,
 /*   140 */    82,   81,   80,   79,   78,  291,  409,  408,  877,  761,
 /*   150 */   372,  371,  370,  765,  369,  767,  768,  368,  770,  365,
 /*   160 */   413,  776,  362,  778,  779,  359,  356,  103,   10,  987,
 /*   170 */   967,  968,   31,   29,   27,   26,   25, 1179,  823,  826,
 /*   180 */   890,  292,  844,   23,  222,  117,  872,  873,  874,  875,
 /*   190 */   876,  878,  880,  881,  882,  117,  117, 1194,  126,  348,
 /*   200 */  1194,  842,  110, 1280,  333, 1280,  909,  333,  261, 1061,
 /*   210 */   335,  125,  349,  237, 1155, 1179, 1279, 1082,  116,  321,
 /*   220 */  1278,  102, 1278,  305,   61, 1180, 1183, 1219,  310, 1087,
 /*   230 */   228,  200, 1215, 1085,  854, 1194,  349,   44,  102, 1179,
 /*   240 */   123,  346,  320, 1280,  998,  234, 1087, 1179,  335,  312,
 /*   250 */   307,   52, 1155,  102,  105, 1050,  116, 1085,  256, 1194,
 /*   260 */  1278, 1087,   62, 1180, 1183, 1219,  333, 1194, 1078,  217,
 /*   270 */  1215,  111,  335,  117,  333,  122, 1155,  157, 1179,  120,
 /*   280 */   335, 1155,   59,  158, 1155,  997,  106, 1180, 1183,  298,
 /*   290 */  1246,  325,   92,  973,   63, 1180, 1183, 1219, 1194, 1077,
 /*   300 */   269, 1218, 1215,  822,  268,  320,   31,   29,   27,   26,
 /*   310 */    25,  335,  311,  185,  996, 1155,  256, 1179,  187,  684,
 /*   320 */   820,  683, 1155,  322, 1293,   62, 1180, 1183, 1219,  270,
 /*   330 */   186,  104,  217, 1215,  111,   20,  131, 1194,  239,  129,
 /*   340 */   685, 1179, 1233,  118,  333,   31,   29,   27,   26,   25,
 /*   350 */   335, 1155,  821, 1247, 1155,  326,  995,  336, 1130, 1230,
 /*   360 */  1233, 1194, 1280, 1141,   62, 1180, 1183, 1219,  333, 1132,
 /*   370 */  1130,  217, 1215, 1292,  335,  116,  229, 1229, 1155, 1278,
 /*   380 */   413, 1128, 1253, 1179,   30,   28,  914,  994,   62, 1180,
 /*   390 */  1183, 1219,  224, 1155,  822,  217, 1215, 1292,  823,  826,
 /*   400 */   349,  102,  349, 1194,   21,  347, 1276,   65, 1130, 1088,
 /*   410 */   333,  820,  879,  349,  265,  883,  335,  845,   65, 1129,
 /*   420 */  1155, 1085,   12, 1085, 1155,  271, 1179,  970,  971,  117,
 /*   430 */    62, 1180, 1183, 1219, 1085,  993, 1130,  217, 1215, 1292,
 /*   440 */  1016,  683,  236,  821,  349,    1, 1194, 1128, 1237,  175,
 /*   450 */   992, 1059,  324,  333, 1025,  843,   30,   28,  263,  335,
 /*   460 */  1238,  909,  278, 1155,  224, 1085,  822,  317,  321,   30,
 /*   470 */    28,  413, 1155,  190, 1180, 1183,  349,  224,  921,  822,
 /*   480 */   913,  238,  133,  820,  842,  132,  991, 1155,   91,  823,
 /*   490 */   826,  382, 1280,  375,   12,  990,  820, 1085, 1179,  989,
 /*   500 */   382,  986,  985,  321,  135,  116,  277,  134,  272, 1278,
 /*   510 */    89,  276,  144,  984,  275,  821,  273,    1, 1194,  274,
 /*   520 */   155, 1226,  316, 1155,  315,  333,  983, 1280,  821,  982,
 /*   530 */     7,  335, 1155,    9,    8, 1155, 1155, 1233, 1155, 1155,
 /*   540 */   116,   30,   28,  413, 1278,   63, 1180, 1183, 1219,  224,
 /*   550 */  1155,  822,  331, 1215, 1228,  317,  413,   30,   28,  334,
 /*   560 */   289,  823,  826, 1155,  981,  224, 1155,  822,  820,  416,
 /*   570 */    30,   28,  328,  287,  823,  826,   91,  980,  224,  137,
 /*   580 */   822,  294,  136,  178,  820,  244, 1179,   87,  829,  979,
 /*   590 */   978,  117,  828,  405, 1070,   96,  177,  820,   89,  323,
 /*   600 */   821, 1155,    7,  988, 1072, 1068, 1194,  319,  112, 1226,
 /*   610 */  1227,  912, 1231,  333, 1155, 1011,  821, 1009,    7,  335,
 /*   620 */   832,  332,   60, 1155,  831,  173, 1155, 1155,  413,  821,
 /*   630 */   374,    1, 1179,   63, 1180, 1183, 1219,  280,  329,  283,
 /*   640 */   935, 1216,  149,   58,  413,  208,  823,  826, 1051,  159,
 /*   650 */  1249,  303, 1194, 1124,   41,   54,  147,  413,  345,  333,
 /*   660 */   262,  297,  823,  826,  141,  335,  152, 1179,  884, 1155,
 /*   670 */   269, 1195,  223,  161,  268,  823,  826,  318,    2,  196,
 /*   680 */  1180, 1183,   32,  851, 1173, 1179,  240, 1194,  209,  840,
 /*   690 */   207,  206,  869,  267,  333,  848,  812,   32, 1171,  270,
 /*   700 */   335,  252,  938,  841, 1155, 1194,  167,  299,  119, 1179,
 /*   710 */    32,  253,  333, 1060,  196, 1180, 1183,  341,  335,  847,
 /*   720 */   165,  254, 1155,   42,  302,  936,  937,  939,  940, 1194,
 /*   730 */   257,   93,  195, 1180, 1183, 1179,  333,  172,  754,  840,
 /*   740 */   749,  846,  335,  264,  124,  266, 1155, 1075,  128,  251,
 /*   750 */  1071,   94,   96,   67,   41, 1194,  106, 1180, 1183,  250,
 /*   760 */  1179,  213,  333,  313,  249,  782,  248,  786,  335,  130,
 /*   770 */    98,   99, 1155,  379,  792,  221, 1073,  378, 1058,  354,
 /*   780 */  1194,   94,  196, 1180, 1183,  245, 1069,  333,   95,  791,
 /*   790 */    97,  100, 1179,  335, 1294,  247,  246, 1155,  140,  101,
 /*   800 */   225,  296,  380,   96,   94,  231,  230,  196, 1180, 1183,
 /*   810 */  1179,  293, 1194,  295,  845,  834,  304,  339, 1260,  333,
 /*   820 */   145,  377,  376, 1250,  826,  335,  301,  216, 1179, 1155,
 /*   830 */  1194, 1259,  827, 1179,  148,  241, 1179,  333,  379,  194,
 /*   840 */  1180, 1183,  378,  335,    5,  314,  300, 1155, 1194, 1240,
 /*   850 */     4,  109,  153, 1194,  844,  333, 1194,  197, 1180, 1183,
 /*   860 */   333,  335,  909,  333,  830, 1155,  335,  380, 1179,  335,
 /*   870 */  1155, 1179,  151, 1155,   90,  188, 1180, 1183,   33, 1234,
 /*   880 */   198, 1180, 1183,  189, 1180, 1183,  377,  376, 1194,  218,
 /*   890 */  1295, 1194,  350, 1277,  330,  333,  327,  154,  333,  160,
 /*   900 */    17,  335, 1201, 1179,  335, 1155, 1179,  337, 1155,  342,
 /*   910 */   835,  826, 1139, 1138,  344,  199, 1180, 1183, 1191, 1180,
 /*   920 */  1183,  343,  338, 1194,  226,  169, 1194,  179,   51, 1086,
 /*   930 */   333,   53,  352,  333,  181,  176,  335,  412,  191,  335,
 /*   940 */  1155,  192, 1149, 1155, 1179,  317,   22, 1179,  184,  183,
 /*   950 */  1190, 1180, 1183, 1189, 1180, 1183,   31,   29,   27,   26,
 /*   960 */    25, 1024, 1148, 1147, 1194,  242,   91, 1194,  243, 1146,
 /*   970 */  1064,  333, 1063, 1023,  333, 1020, 1008,  335, 1003, 1145,
 /*   980 */   335, 1155, 1136, 1179, 1155,  121, 1062,  698,   89, 1022,
 /*   990 */  1019,  204, 1180, 1183,  203, 1180, 1183,  258,  113, 1226,
 /*  1000 */  1227, 1179, 1231, 1194,  259, 1007, 1006, 1002,  260, 1066,
 /*  1010 */   333,   66,  127,  797,  795, 1065,  335, 1017, 1012, 1179,
 /*  1020 */  1155, 1194,  796,  727,  726,  210,  725,  724,  333, 1010,
 /*  1030 */   205, 1180, 1183,  317,  335,  723,  282,  722, 1155, 1194,
 /*  1040 */   281,  211,  212,  284, 1001,  286,  333, 1000,  202, 1180,
 /*  1050 */  1183,  290,  335,  288,   91,   64, 1155, 1144, 1143, 1135,
 /*  1060 */    36,  143,   14,  308,   45,  139,  193, 1180, 1183,  285,
 /*  1070 */   142,  146,   15,  934,  279,  107,   89,   11,  138,   34,
 /*  1080 */    48,    3,   32,   37,  150,   46,  114, 1226, 1227,  309,
 /*  1090 */  1231,  956,  955,  219,  960,  928,  927, 1171,   47,  959,
 /*  1100 */   220,   19,  906,  156,   40,    8, 1134,   39,  905,  170,
 /*  1110 */   961,  836,  852,   13,   18,   35,  164,  115,  353,  932,
 /*  1120 */   233,   16,  166,  168,  163,  357,  360,  340,   54,  870,
 /*  1130 */   783,  363,  366, 1170,  351,   49,   50,   38,  174,  355,
 /*  1140 */   760,  780,  358,  790,  777,  361,  789,  771,  364,  171,
 /*  1150 */   788,  769,  696,  367,  718,  381,   55,   56,   57,  719,
 /*  1160 */   717,  716,  715,  714,  713,  712,  711,  710,  709,  775,
 /*  1170 */   708,  373,  774,  707,  706,  773,  705,  704,  703,  772,
 /*  1180 */   702,  701, 1018,  402,  403, 1005,  407,  404,  406, 1004,
 /*  1190 */   999,  410,  411,  974,  824,  182,  414,  415,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     0,  200,  176,  171,  183,  179,  187,  206,  207,  214,
 /*    10 */   215,  190,  180,   12,   13,   14,   15,   16,    0,  200,
 /*    20 */   188,  187,   14,   15,   16,  206,  207,   12,   13,   14,
 /*    30 */    15,   16,  211,  192,  200,  163,  195,  196,  184,   21,
 /*    40 */   206,  207,   24,   25,   26,   27,   28,   29,   30,   31,
 /*    50 */    32,   50,   52,   53,   54,   55,   56,   57,   58,   59,
 /*    60 */    60,   61,   62,   63,   64,   65,   66,   67,   68,   69,
 /*    70 */    70,   20,  200,   12,   13,   74,   12,   13,   14,   15,
 /*    80 */    16,   20,   20,   22,   21,   84,  171,   24,   25,   26,
 /*    90 */    27,   28,   29,   30,   31,   32,   52,  169,   54,  183,
 /*   100 */    39,   57,  174,  188,   60,  189,   62,    1,    2,   65,
 /*   110 */   194,   50,    0,  196,   50,   20,  199,   44,  190,  202,
 /*   120 */   119,  120,  182,  122,  123,  124,  125,  126,  127,  128,
 /*   130 */   129,  130,   71,  193,   73,   73,   24,   25,   26,   27,
 /*   140 */    28,   29,   30,   31,   32,  169,  166,  167,   84,   90,
 /*   150 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   160 */    99,  102,  103,  104,  105,  106,  107,  162,   73,  164,
 /*   170 */   155,  156,   12,   13,   14,   15,   16,  163,  117,  118,
 /*   180 */    74,  205,   20,  119,  120,  134,  122,  123,  124,  125,
 /*   190 */   126,  127,  128,  129,  130,  134,  134,  183,   33,   20,
 /*   200 */   183,   20,   37,  229,  190,  229,  133,  190,   43,    0,
 /*   210 */   196,   46,  169,  175,  200,  163,  242,  174,  242,  205,
 /*   220 */   246,  183,  246,  113,  210,  211,  212,  213,  211,  191,
 /*   230 */   175,  217,  218,  190,   74,  183,  169,   72,  183,  163,
 /*   240 */    75,  174,  190,  229,  163,  175,  191,  163,  196,  139,
 /*   250 */   140,  168,  200,  183,  172,  173,  242,  190,   49,  183,
 /*   260 */   246,  191,  210,  211,  212,  213,  190,  183,  185,  217,
 /*   270 */   218,  219,  196,  134,  190,  110,  200,  115,  163,  114,
 /*   280 */   196,  200,  168,  231,  200,  163,  210,  211,  212,  237,
 /*   290 */   238,   88,  178,  160,  210,  211,  212,  213,  183,  185,
 /*   300 */    60,  217,  218,   22,   64,  190,   12,   13,   14,   15,
 /*   310 */    16,  196,   20,   18,  163,  200,   49,  163,   23,   20,
 /*   320 */    39,   22,  200,  247,  248,  210,  211,  212,  213,   89,
 /*   330 */    35,   36,  217,  218,  219,    2,   79,  183,  205,   82,
 /*   340 */    41,  163,  208,   48,  190,   12,   13,   14,   15,   16,
 /*   350 */   196,  200,   71,  238,  200,  152,  163,  196,  183,  225,
 /*   360 */   208,  183,  229,  202,  210,  211,  212,  213,  190,  194,
 /*   370 */   183,  217,  218,  219,  196,  242,  189,  225,  200,  246,
 /*   380 */    99,  194,  228,  163,   12,   13,   14,  163,  210,  211,
 /*   390 */   212,  213,   20,  200,   22,  217,  218,  219,  117,  118,
 /*   400 */   169,  183,  169,  183,  119,  174,  228,  174,  183,  191,
 /*   410 */   190,   39,  127,  169,  181,  130,  196,   20,  174,  194,
 /*   420 */   200,  190,   50,  190,  200,  181,  163,  158,  159,  134,
 /*   430 */   210,  211,  212,  213,  190,  163,  183,  217,  218,  219,
 /*   440 */     0,   22,  189,   71,  169,   73,  183,  194,  228,  174,
 /*   450 */   163,    0,    3,  190,    0,   20,   12,   13,   39,  196,
 /*   460 */   132,  133,   22,  200,   20,  190,   22,  169,  205,   12,
 /*   470 */    13,   99,  200,  210,  211,  212,  169,   20,   14,   22,
 /*   480 */     4,  174,   79,   39,   20,   82,  163,  200,  190,  117,
 /*   490 */   118,   49,  229,   86,   50,  163,   39,  190,  163,  163,
 /*   500 */    49,  163,  163,  205,   79,  242,   52,   82,   54,  246,
 /*   510 */   212,   57,  115,  163,   60,   71,   62,   73,  183,   65,
 /*   520 */   222,  223,  224,  200,  226,  190,  163,  229,   71,  163,
 /*   530 */    73,  196,  200,    1,    2,  200,  200,  208,  200,  200,
 /*   540 */   242,   12,   13,   99,  246,  210,  211,  212,  213,   20,
 /*   550 */   200,   22,  217,  218,  225,  169,   99,   12,   13,   14,
 /*   560 */    21,  117,  118,  200,  163,   20,  200,   22,   39,   19,
 /*   570 */    12,   13,   88,   34,  117,  118,  190,  163,   20,   79,
 /*   580 */    22,   74,   82,   33,   39,  169,  163,   37,   39,  163,
 /*   590 */   163,  134,   39,   43,  184,   88,   46,   39,  212,  150,
 /*   600 */    71,  200,   73,  164,  184,  184,  183,  221,  222,  223,
 /*   610 */   224,  135,  226,  190,  200,    0,   71,    0,   73,  196,
 /*   620 */    71,   50,   72,  200,   71,   75,  200,  200,   99,   71,
 /*   630 */   184,   73,  163,  210,  211,  212,  213,   22,  154,   22,
 /*   640 */    74,  218,   74,   73,   99,   35,  117,  118,  173,  249,
 /*   650 */   209,  240,  183,  193,   88,   85,   88,   99,  108,  190,
 /*   660 */   166,  111,  117,  118,  114,  196,  234,  163,   74,  200,
 /*   670 */    60,  183,  203,  243,   64,  117,  118,  227,  230,  210,
 /*   680 */   211,  212,   88,   74,   73,  163,  169,  183,   78,   20,
 /*   690 */    80,   81,  121,   83,  190,   20,   74,   88,   87,   89,
 /*   700 */   196,  204,  121,   20,  200,  183,   74,  203,  171,  163,
 /*   710 */    88,  190,  190,    0,  210,  211,  212,   74,  196,   20,
 /*   720 */    88,  197,  200,  171,  143,  144,  145,  146,  147,  183,
 /*   730 */   169,   88,  210,  211,  212,  163,  190,   74,   74,   20,
 /*   740 */    74,   20,  196,  165,  171,  183,  200,  183,  183,   30,
 /*   750 */   183,   88,   88,  169,   88,  183,  210,  211,  212,   40,
 /*   760 */   163,  165,  190,  241,   45,   74,   47,   74,  196,  183,
 /*   770 */   183,  183,  200,   60,   74,  203,  183,   64,    0,   88,
 /*   780 */   183,   88,  210,  211,  212,   66,  183,  190,   88,   74,
 /*   790 */    74,  183,  163,  196,  248,   76,   77,  200,  168,  183,
 /*   800 */   203,  197,   89,   88,   88,   12,   13,  210,  211,  212,
 /*   810 */   163,  204,  183,  190,   20,   22,  142,  141,  239,  190,
 /*   820 */   201,  108,  109,  209,  118,  196,  200,  200,  163,  200,
 /*   830 */   183,  239,   39,  163,  201,  116,  163,  190,   60,  210,
 /*   840 */   211,  212,   64,  196,  149,  148,  137,  200,  183,  236,
 /*   850 */   136,  233,  232,  183,   20,  190,  183,  210,  211,  212,
 /*   860 */   190,  196,  133,  190,   71,  200,  196,   89,  163,  196,
 /*   870 */   200,  163,  235,  200,  190,  210,  211,  212,  131,  208,
 /*   880 */   210,  211,  212,  210,  211,  212,  108,  109,  183,  157,
 /*   890 */   250,  183,   99,  245,  153,  190,  151,  220,  190,  244,
 /*   900 */    73,  196,  216,  163,  196,  200,  163,  200,  200,  112,
 /*   910 */   117,  118,  201,  201,  197,  210,  211,  212,  210,  211,
 /*   920 */   212,  198,  200,  183,  200,  190,  183,  179,  168,  190,
 /*   930 */   190,   73,  186,  190,  169,  168,  196,  165,  177,  196,
 /*   940 */   200,  177,    0,  200,  163,  169,    2,  163,  161,  170,
 /*   950 */   210,  211,  212,  210,  211,  212,   12,   13,   14,   15,
 /*   960 */    16,    0,    0,    0,  183,   66,  190,  183,   87,    0,
 /*   970 */     0,  190,    0,    0,  190,    0,    0,  196,    0,    0,
 /*   980 */   196,  200,    0,  163,  200,   44,    0,   51,  212,    0,
 /*   990 */     0,  210,  211,  212,  210,  211,  212,   39,  222,  223,
 /*  1000 */   224,  163,  226,  183,   37,    0,    0,    0,   44,    0,
 /*  1010 */   190,   84,   82,   39,   22,    0,  196,    0,    0,  163,
 /*  1020 */   200,  183,   39,   39,   39,   22,   39,   39,  190,    0,
 /*  1030 */   210,  211,  212,  169,  196,   39,    4,   39,  200,  183,
 /*  1040 */    40,   22,   22,   39,    0,   22,  190,    0,  210,  211,
 /*  1050 */   212,   19,  196,   22,  190,   20,  200,    0,    0,    0,
 /*  1060 */   115,  110,  138,   39,   73,   33,  210,  211,  212,   37,
 /*  1070 */    44,   74,  138,   74,   42,   73,  212,  138,   46,  132,
 /*  1080 */     4,   88,   88,   88,   73,   73,  222,  223,  224,   88,
 /*  1090 */   226,   39,   39,   39,   39,   74,   74,   87,   73,   39,
 /*  1100 */    39,   88,   74,   87,   72,    2,    0,   75,   74,   44,
 /*  1110 */    74,   22,   74,   73,   73,   88,   74,   87,   39,   74,
 /*  1120 */    39,   88,   73,   73,   87,   39,   39,  113,   85,  121,
 /*  1130 */    74,   39,   39,   87,   86,   73,   73,   73,   87,   73,
 /*  1140 */    22,   74,   73,   39,   74,   73,   39,   74,   73,  110,
 /*  1150 */    22,   74,   51,   73,   22,   50,   73,   73,   73,   71,
 /*  1160 */    39,   39,   39,   39,   39,   39,   39,   22,   39,  101,
 /*  1170 */    39,   89,  101,   39,   39,  101,   39,   39,   39,  101,
 /*  1180 */    39,   39,    0,   39,   37,    0,   38,   44,   39,    0,
 /*  1190 */     0,   22,   21,  251,   22,   22,   21,   20,  251,  251,
 /*  1200 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1210 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1220 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1230 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1240 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1250 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1260 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1270 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1280 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1290 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1300 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1310 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1320 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1330 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1340 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*  1350 */   251,  251,  251,  251,  251,  251,  251,  251,
};
#define YY_SHIFT_COUNT    (416)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (1190)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   295,   61,  372,  444,  444,  444,  444,  457,  444,  444,
 /*    10 */    62,  529,  558,  545,  529,  529,  529,  529,  529,  529,
 /*    20 */   529,  529,  529,  529,  529,  529,  529,  529,  529,  529,
 /*    30 */   529,  529,  529,   95,   95,   95,   51,  793,  793,  179,
 /*    40 */   179,  793,  179,  179,  267,  181,  292,  292,  139,  435,
 /*    50 */   181,  179,  179,  181,  179,  181,  435,  181,  181,  179,
 /*    60 */   442,    1,   64,   64,   63,  610,  281,   44,  281,  281,
 /*    70 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*    80 */   281,  281,  281,  281,  281,  281,  281,  299,  209,  162,
 /*    90 */   162,  162,  451,  435,  181,  181,  181,  407,   59,   59,
 /*   100 */    59,   59,   59,   18,  719,  454,   15,  581,  240,  110,
 /*   110 */   419,  397,  328,   73,  328,  464,  449,  476,  669,  675,
 /*   120 */   267,  683,  699,  267,  669,  267,  721,  181,  181,  181,
 /*   130 */   181,  181,  181,  181,  181,  181,  181,  181,  669,  721,
 /*   140 */   675,  442,  683,  699,  794,  674,  676,  706,  674,  676,
 /*   150 */   706,  695,  697,  709,  714,  729,  683,  834,  747,  732,
 /*   160 */   741,  745,  827,  181,  676,  706,  706,  676,  706,  797,
 /*   170 */   683,  699,  407,  442,  683,  858,  669,  442,  721, 1198,
 /*   180 */  1198, 1198, 1198,    0,  112,  550,  165, 1032,  333,  944,
 /*   190 */   160,  713,  778,  294,  294,  294,  294,  294,  294,  294,
 /*   200 */   106,  285,    8,    8,    8,    8,  257,  403,  425,  500,
 /*   210 */   440,  615,  617,  539,  507,  566,  568,  532,  269,  203,
 /*   220 */   484,  594,  571,  609,  611,  622,  632,  643,  663,  664,
 /*   230 */   549,  553,  666,  691,  693,  700,  715,  716,  570,  942,
 /*   240 */   961,  962,  963,  899,  881,  969,  970,  972,  973,  975,
 /*   250 */   976,  978,  979,  982,  941,  986,  936,  989,  990,  958,
 /*   260 */   967,  964, 1005, 1006, 1007, 1009,  927,  930,  974,  983,
 /*   270 */   992, 1015,  984,  985,  987,  988,  996,  998, 1017, 1003,
 /*   280 */  1018, 1019, 1000, 1029, 1020, 1004, 1044, 1023, 1047, 1031,
 /*   290 */  1035, 1057, 1058,  945, 1059,  991, 1026,  951,  993,  994,
 /*   300 */   924,  997,  995,  999, 1002, 1011, 1021, 1012, 1022, 1024,
 /*   310 */  1001, 1010, 1025, 1013,  934, 1028, 1034, 1016,  947, 1027,
 /*   320 */  1030, 1036, 1033,  939, 1076, 1052, 1053, 1054, 1055, 1060,
 /*   330 */  1061, 1103, 1008, 1037, 1038, 1040, 1041, 1042, 1045, 1049,
 /*   340 */  1050, 1014, 1062, 1106, 1065, 1039, 1063, 1043, 1046, 1051,
 /*   350 */  1089, 1064, 1048, 1056, 1079, 1081, 1066, 1067, 1086, 1069,
 /*   360 */  1070, 1087, 1072, 1073, 1092, 1075, 1077, 1093, 1080, 1068,
 /*   370 */  1071, 1074, 1078, 1118, 1082, 1083, 1084, 1085, 1104, 1107,
 /*   380 */  1128, 1101, 1105, 1088, 1132, 1121, 1122, 1123, 1124, 1125,
 /*   390 */  1126, 1127, 1145, 1129, 1131, 1134, 1135, 1137, 1138, 1139,
 /*   400 */  1141, 1142, 1182, 1144, 1147, 1143, 1185, 1149, 1148, 1189,
 /*   410 */  1190, 1169, 1171, 1172, 1173, 1175, 1177,
};
#define YY_REDUCE_COUNT (182)
#define YY_REDUCE_MIN   (-205)
#define YY_REDUCE_MAX   (864)
static const short yy_reduce_ofst[] = {
 /*     0 */   133,   14,   52,  115,  154,  178,  220,  263,   84,  335,
 /*    10 */   298,   76,  423,  469,  504,  522,  546,  572,  597,  629,
 /*    20 */   647,  665,  670,  673,  705,  708,  740,  743,  781,  784,
 /*    30 */   820,  838,  856,  386,  776,  864,  -24, -181, -166,  233,
 /*    40 */   244, -199,  -72,   43, -168,  -84, -179,   17,  -26,  -83,
 /*    50 */    55,   67,  231,  187,  275,   70, -159,  253,   38,  307,
 /*    60 */   114, -205, -205, -205,    5,  -60, -128,   82,   81,  122,
 /*    70 */   151,  193,  224,  272,  287,  323,  332,  336,  338,  339,
 /*    80 */   350,  363,  366,  401,  414,  426,  427,  -20,  -85,  134,
 /*    90 */   152,  329,   83,  161,  218,  175,  225, -174, -146,  410,
 /*   100 */   420,  421,  446,  439,  416,  475,  400,  411,  460,  432,
 /*   110 */   494,  441,  450,  450,  450,  488,  430,  448,  517,  497,
 /*   120 */   537,  521,  524,  552,  561,  573,  578,  562,  564,  565,
 /*   130 */   567,  586,  587,  588,  593,  603,  608,  616,  584,  596,
 /*   140 */   607,  630,  623,  604,  614,  579,  619,  626,  592,  633,
 /*   150 */   627,  613,  637,  618,  620,  450,  684,  671,  677,  640,
 /*   160 */   648,  655,  686,  488,  711,  707,  722,  712,  724,  723,
 /*   170 */   735,  717,  748,  760,  739,  746,  765,  767,  772,  761,
 /*   180 */   764,  779,  787,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*    10 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*    20 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*    30 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*    40 */   972,  972,  972,  972, 1029,  972,  972,  972,  972,  972,
 /*    50 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*    60 */  1027,  972, 1221,  972,  972,  972,  972,  972,  972,  972,
 /*    70 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*    80 */   972,  972,  972,  972,  972,  972,  972,  972, 1029, 1232,
 /*    90 */  1232, 1232, 1027,  972,  972,  972,  972, 1114,  972,  972,
 /*   100 */   972,  972,  972,  972,  972,  972, 1296,  972, 1067, 1256,
 /*   110 */   972, 1248, 1224, 1238, 1225,  972, 1281, 1241,  972,  972,
 /*   120 */  1029,  972,  972, 1029,  972, 1029,  972,  972,  972,  972,
 /*   130 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   140 */   972, 1027,  972,  972,  972, 1263, 1261,  972, 1263, 1261,
 /*   150 */   972, 1275, 1271, 1254, 1252, 1238,  972,  972,  972, 1299,
 /*   160 */  1287, 1283,  972,  972, 1261,  972,  972, 1261,  972, 1137,
 /*   170 */   972,  972,  972, 1027,  972, 1083,  972, 1027,  972, 1117,
 /*   180 */  1117, 1030,  977,  972,  972,  972,  972,  972,  972,  972,
 /*   190 */   972,  972,  972, 1193, 1274, 1273, 1192, 1198, 1197, 1196,
 /*   200 */   972,  972, 1187, 1188, 1186, 1185,  972,  972,  972,  972,
 /*   210 */   972,  972,  972,  972,  972,  972,  972, 1222,  972, 1284,
 /*   220 */  1288,  972,  972,  972, 1172,  972,  972,  972,  972,  972,
 /*   230 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   240 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   250 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   260 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   270 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   280 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   290 */   972,  972,  972,  972,  972,  972,  972,  972, 1245, 1255,
 /*   300 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   310 */   972, 1172,  972, 1272,  972, 1231, 1227,  972,  972, 1223,
 /*   320 */   972,  972, 1282,  972,  972,  972,  972,  972,  972,  972,
 /*   330 */   972, 1217,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   340 */   972,  972,  972,  972,  972,  972,  972,  972, 1171,  972,
 /*   350 */   972,  972,  972,  972,  972,  972, 1111,  972,  972,  972,
 /*   360 */   972,  972,  972,  972,  972,  972,  972,  972,  972, 1096,
 /*   370 */  1094, 1093, 1092,  972, 1089,  972,  972,  972,  972,  972,
 /*   380 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   390 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   400 */   972,  972,  972,  972,  972,  972,  972,  972,  972,  972,
 /*   410 */   972,  972,  972,  972,  972,  972,  972,
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
  /*   18 */ "CREATE",
  /*   19 */ "ACCOUNT",
  /*   20 */ "NK_ID",
  /*   21 */ "PASS",
  /*   22 */ "NK_STRING",
  /*   23 */ "ALTER",
  /*   24 */ "PPS",
  /*   25 */ "TSERIES",
  /*   26 */ "STORAGE",
  /*   27 */ "STREAMS",
  /*   28 */ "QTIME",
  /*   29 */ "DBS",
  /*   30 */ "USERS",
  /*   31 */ "CONNS",
  /*   32 */ "STATE",
  /*   33 */ "USER",
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
  /*  162 */ "alter_account_options",
  /*  163 */ "literal",
  /*  164 */ "alter_account_option",
  /*  165 */ "user_name",
  /*  166 */ "dnode_endpoint",
  /*  167 */ "dnode_host_name",
  /*  168 */ "not_exists_opt",
  /*  169 */ "db_name",
  /*  170 */ "db_options",
  /*  171 */ "exists_opt",
  /*  172 */ "alter_db_options",
  /*  173 */ "alter_db_option",
  /*  174 */ "full_table_name",
  /*  175 */ "column_def_list",
  /*  176 */ "tags_def_opt",
  /*  177 */ "table_options",
  /*  178 */ "multi_create_clause",
  /*  179 */ "tags_def",
  /*  180 */ "multi_drop_clause",
  /*  181 */ "alter_table_clause",
  /*  182 */ "alter_table_options",
  /*  183 */ "column_name",
  /*  184 */ "type_name",
  /*  185 */ "create_subtable_clause",
  /*  186 */ "specific_tags_opt",
  /*  187 */ "literal_list",
  /*  188 */ "drop_table_clause",
  /*  189 */ "col_name_list",
  /*  190 */ "table_name",
  /*  191 */ "column_def",
  /*  192 */ "func_name_list",
  /*  193 */ "alter_table_option",
  /*  194 */ "col_name",
  /*  195 */ "func_name",
  /*  196 */ "function_name",
  /*  197 */ "index_name",
  /*  198 */ "index_options",
  /*  199 */ "func_list",
  /*  200 */ "duration_literal",
  /*  201 */ "sliding_opt",
  /*  202 */ "func",
  /*  203 */ "expression_list",
  /*  204 */ "topic_name",
  /*  205 */ "query_expression",
  /*  206 */ "signed",
  /*  207 */ "signed_literal",
  /*  208 */ "table_alias",
  /*  209 */ "column_alias",
  /*  210 */ "expression",
  /*  211 */ "column_reference",
  /*  212 */ "subquery",
  /*  213 */ "predicate",
  /*  214 */ "compare_op",
  /*  215 */ "in_op",
  /*  216 */ "in_predicate_value",
  /*  217 */ "boolean_value_expression",
  /*  218 */ "boolean_primary",
  /*  219 */ "common_expression",
  /*  220 */ "from_clause",
  /*  221 */ "table_reference_list",
  /*  222 */ "table_reference",
  /*  223 */ "table_primary",
  /*  224 */ "joined_table",
  /*  225 */ "alias_opt",
  /*  226 */ "parenthesized_joined_table",
  /*  227 */ "join_type",
  /*  228 */ "search_condition",
  /*  229 */ "query_specification",
  /*  230 */ "set_quantifier_opt",
  /*  231 */ "select_list",
  /*  232 */ "where_clause_opt",
  /*  233 */ "partition_by_clause_opt",
  /*  234 */ "twindow_clause_opt",
  /*  235 */ "group_by_clause_opt",
  /*  236 */ "having_clause_opt",
  /*  237 */ "select_sublist",
  /*  238 */ "select_item",
  /*  239 */ "fill_opt",
  /*  240 */ "fill_mode",
  /*  241 */ "group_by_list",
  /*  242 */ "query_expression_body",
  /*  243 */ "order_by_clause_opt",
  /*  244 */ "slimit_clause_opt",
  /*  245 */ "limit_clause_opt",
  /*  246 */ "query_primary",
  /*  247 */ "sort_specification_list",
  /*  248 */ "sort_specification",
  /*  249 */ "ordering_specification_opt",
  /*  250 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options",
 /*   1 */ "cmd ::= ALTER ACCOUNT NK_ID alter_account_options",
 /*   2 */ "account_options ::=",
 /*   3 */ "account_options ::= account_options PPS literal",
 /*   4 */ "account_options ::= account_options TSERIES literal",
 /*   5 */ "account_options ::= account_options STORAGE literal",
 /*   6 */ "account_options ::= account_options STREAMS literal",
 /*   7 */ "account_options ::= account_options QTIME literal",
 /*   8 */ "account_options ::= account_options DBS literal",
 /*   9 */ "account_options ::= account_options USERS literal",
 /*  10 */ "account_options ::= account_options CONNS literal",
 /*  11 */ "account_options ::= account_options STATE literal",
 /*  12 */ "alter_account_options ::= alter_account_option",
 /*  13 */ "alter_account_options ::= alter_account_options alter_account_option",
 /*  14 */ "alter_account_option ::= PASS literal",
 /*  15 */ "alter_account_option ::= PPS literal",
 /*  16 */ "alter_account_option ::= TSERIES literal",
 /*  17 */ "alter_account_option ::= STORAGE literal",
 /*  18 */ "alter_account_option ::= STREAMS literal",
 /*  19 */ "alter_account_option ::= QTIME literal",
 /*  20 */ "alter_account_option ::= DBS literal",
 /*  21 */ "alter_account_option ::= USERS literal",
 /*  22 */ "alter_account_option ::= CONNS literal",
 /*  23 */ "alter_account_option ::= STATE literal",
 /*  24 */ "cmd ::= CREATE USER user_name PASS NK_STRING",
 /*  25 */ "cmd ::= ALTER USER user_name PASS NK_STRING",
 /*  26 */ "cmd ::= ALTER USER user_name PRIVILEGE NK_STRING",
 /*  27 */ "cmd ::= DROP USER user_name",
 /*  28 */ "cmd ::= SHOW USERS",
 /*  29 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*  30 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*  31 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*  32 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*  33 */ "cmd ::= SHOW DNODES",
 /*  34 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING",
 /*  35 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING",
 /*  36 */ "cmd ::= ALTER ALL DNODES NK_STRING",
 /*  37 */ "cmd ::= ALTER ALL DNODES NK_STRING NK_STRING",
 /*  38 */ "dnode_endpoint ::= NK_STRING",
 /*  39 */ "dnode_host_name ::= NK_ID",
 /*  40 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  41 */ "cmd ::= ALTER LOCAL NK_STRING",
 /*  42 */ "cmd ::= ALTER LOCAL NK_STRING NK_STRING",
 /*  43 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  44 */ "cmd ::= DROP QNODE ON DNODE NK_INTEGER",
 /*  45 */ "cmd ::= SHOW QNODES",
 /*  46 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  47 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  48 */ "cmd ::= SHOW DATABASES",
 /*  49 */ "cmd ::= USE db_name",
 /*  50 */ "cmd ::= ALTER DATABASE db_name alter_db_options",
 /*  51 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  52 */ "not_exists_opt ::=",
 /*  53 */ "exists_opt ::= IF EXISTS",
 /*  54 */ "exists_opt ::=",
 /*  55 */ "db_options ::=",
 /*  56 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  57 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  58 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  59 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  60 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  61 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  62 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  63 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  64 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  65 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  66 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  67 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  68 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  69 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  70 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  71 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  72 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  73 */ "db_options ::= db_options RETENTIONS NK_STRING",
 /*  74 */ "db_options ::= db_options FILE_FACTOR NK_FLOAT",
 /*  75 */ "alter_db_options ::= alter_db_option",
 /*  76 */ "alter_db_options ::= alter_db_options alter_db_option",
 /*  77 */ "alter_db_option ::= BLOCKS NK_INTEGER",
 /*  78 */ "alter_db_option ::= FSYNC NK_INTEGER",
 /*  79 */ "alter_db_option ::= KEEP NK_INTEGER",
 /*  80 */ "alter_db_option ::= WAL NK_INTEGER",
 /*  81 */ "alter_db_option ::= QUORUM NK_INTEGER",
 /*  82 */ "alter_db_option ::= CACHELAST NK_INTEGER",
 /*  83 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  84 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  85 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  86 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  87 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  88 */ "cmd ::= SHOW TABLES",
 /*  89 */ "cmd ::= SHOW STABLES",
 /*  90 */ "cmd ::= ALTER TABLE alter_table_clause",
 /*  91 */ "cmd ::= ALTER STABLE alter_table_clause",
 /*  92 */ "alter_table_clause ::= full_table_name alter_table_options",
 /*  93 */ "alter_table_clause ::= full_table_name ADD COLUMN column_name type_name",
 /*  94 */ "alter_table_clause ::= full_table_name DROP COLUMN column_name",
 /*  95 */ "alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name",
 /*  96 */ "alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name",
 /*  97 */ "alter_table_clause ::= full_table_name ADD TAG column_name type_name",
 /*  98 */ "alter_table_clause ::= full_table_name DROP TAG column_name",
 /*  99 */ "alter_table_clause ::= full_table_name MODIFY TAG column_name type_name",
 /* 100 */ "alter_table_clause ::= full_table_name RENAME TAG column_name column_name",
 /* 101 */ "alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal",
 /* 102 */ "multi_create_clause ::= create_subtable_clause",
 /* 103 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /* 104 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /* 105 */ "multi_drop_clause ::= drop_table_clause",
 /* 106 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /* 107 */ "drop_table_clause ::= exists_opt full_table_name",
 /* 108 */ "specific_tags_opt ::=",
 /* 109 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /* 110 */ "full_table_name ::= table_name",
 /* 111 */ "full_table_name ::= db_name NK_DOT table_name",
 /* 112 */ "column_def_list ::= column_def",
 /* 113 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /* 114 */ "column_def ::= column_name type_name",
 /* 115 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /* 116 */ "type_name ::= BOOL",
 /* 117 */ "type_name ::= TINYINT",
 /* 118 */ "type_name ::= SMALLINT",
 /* 119 */ "type_name ::= INT",
 /* 120 */ "type_name ::= INTEGER",
 /* 121 */ "type_name ::= BIGINT",
 /* 122 */ "type_name ::= FLOAT",
 /* 123 */ "type_name ::= DOUBLE",
 /* 124 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /* 125 */ "type_name ::= TIMESTAMP",
 /* 126 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /* 127 */ "type_name ::= TINYINT UNSIGNED",
 /* 128 */ "type_name ::= SMALLINT UNSIGNED",
 /* 129 */ "type_name ::= INT UNSIGNED",
 /* 130 */ "type_name ::= BIGINT UNSIGNED",
 /* 131 */ "type_name ::= JSON",
 /* 132 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /* 133 */ "type_name ::= MEDIUMBLOB",
 /* 134 */ "type_name ::= BLOB",
 /* 135 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /* 136 */ "type_name ::= DECIMAL",
 /* 137 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /* 138 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /* 139 */ "tags_def_opt ::=",
 /* 140 */ "tags_def_opt ::= tags_def",
 /* 141 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /* 142 */ "table_options ::=",
 /* 143 */ "table_options ::= table_options COMMENT NK_STRING",
 /* 144 */ "table_options ::= table_options KEEP NK_INTEGER",
 /* 145 */ "table_options ::= table_options TTL NK_INTEGER",
 /* 146 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /* 147 */ "table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP",
 /* 148 */ "alter_table_options ::= alter_table_option",
 /* 149 */ "alter_table_options ::= alter_table_options alter_table_option",
 /* 150 */ "alter_table_option ::= COMMENT NK_STRING",
 /* 151 */ "alter_table_option ::= KEEP NK_INTEGER",
 /* 152 */ "alter_table_option ::= TTL NK_INTEGER",
 /* 153 */ "col_name_list ::= col_name",
 /* 154 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /* 155 */ "col_name ::= column_name",
 /* 156 */ "func_name_list ::= func_name",
 /* 157 */ "func_name_list ::= func_name_list NK_COMMA col_name",
 /* 158 */ "func_name ::= function_name",
 /* 159 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /* 160 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /* 161 */ "cmd ::= DROP INDEX index_name ON table_name",
 /* 162 */ "index_options ::=",
 /* 163 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /* 164 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /* 165 */ "func_list ::= func",
 /* 166 */ "func_list ::= func_list NK_COMMA func",
 /* 167 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 168 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression",
 /* 169 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name",
 /* 170 */ "cmd ::= DROP TOPIC exists_opt topic_name",
 /* 171 */ "cmd ::= SHOW VGROUPS",
 /* 172 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /* 173 */ "cmd ::= SHOW MNODES",
 /* 174 */ "cmd ::= query_expression",
 /* 175 */ "literal ::= NK_INTEGER",
 /* 176 */ "literal ::= NK_FLOAT",
 /* 177 */ "literal ::= NK_STRING",
 /* 178 */ "literal ::= NK_BOOL",
 /* 179 */ "literal ::= TIMESTAMP NK_STRING",
 /* 180 */ "literal ::= duration_literal",
 /* 181 */ "duration_literal ::= NK_VARIABLE",
 /* 182 */ "signed ::= NK_INTEGER",
 /* 183 */ "signed ::= NK_PLUS NK_INTEGER",
 /* 184 */ "signed ::= NK_MINUS NK_INTEGER",
 /* 185 */ "signed ::= NK_FLOAT",
 /* 186 */ "signed ::= NK_PLUS NK_FLOAT",
 /* 187 */ "signed ::= NK_MINUS NK_FLOAT",
 /* 188 */ "signed_literal ::= signed",
 /* 189 */ "signed_literal ::= NK_STRING",
 /* 190 */ "signed_literal ::= NK_BOOL",
 /* 191 */ "signed_literal ::= TIMESTAMP NK_STRING",
 /* 192 */ "signed_literal ::= duration_literal",
 /* 193 */ "literal_list ::= signed_literal",
 /* 194 */ "literal_list ::= literal_list NK_COMMA signed_literal",
 /* 195 */ "db_name ::= NK_ID",
 /* 196 */ "table_name ::= NK_ID",
 /* 197 */ "column_name ::= NK_ID",
 /* 198 */ "function_name ::= NK_ID",
 /* 199 */ "table_alias ::= NK_ID",
 /* 200 */ "column_alias ::= NK_ID",
 /* 201 */ "user_name ::= NK_ID",
 /* 202 */ "index_name ::= NK_ID",
 /* 203 */ "topic_name ::= NK_ID",
 /* 204 */ "expression ::= literal",
 /* 205 */ "expression ::= column_reference",
 /* 206 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 207 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 208 */ "expression ::= subquery",
 /* 209 */ "expression ::= NK_LP expression NK_RP",
 /* 210 */ "expression ::= NK_PLUS expression",
 /* 211 */ "expression ::= NK_MINUS expression",
 /* 212 */ "expression ::= expression NK_PLUS expression",
 /* 213 */ "expression ::= expression NK_MINUS expression",
 /* 214 */ "expression ::= expression NK_STAR expression",
 /* 215 */ "expression ::= expression NK_SLASH expression",
 /* 216 */ "expression ::= expression NK_REM expression",
 /* 217 */ "expression_list ::= expression",
 /* 218 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 219 */ "column_reference ::= column_name",
 /* 220 */ "column_reference ::= table_name NK_DOT column_name",
 /* 221 */ "predicate ::= expression compare_op expression",
 /* 222 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 223 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 224 */ "predicate ::= expression IS NULL",
 /* 225 */ "predicate ::= expression IS NOT NULL",
 /* 226 */ "predicate ::= expression in_op in_predicate_value",
 /* 227 */ "compare_op ::= NK_LT",
 /* 228 */ "compare_op ::= NK_GT",
 /* 229 */ "compare_op ::= NK_LE",
 /* 230 */ "compare_op ::= NK_GE",
 /* 231 */ "compare_op ::= NK_NE",
 /* 232 */ "compare_op ::= NK_EQ",
 /* 233 */ "compare_op ::= LIKE",
 /* 234 */ "compare_op ::= NOT LIKE",
 /* 235 */ "compare_op ::= MATCH",
 /* 236 */ "compare_op ::= NMATCH",
 /* 237 */ "in_op ::= IN",
 /* 238 */ "in_op ::= NOT IN",
 /* 239 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 240 */ "boolean_value_expression ::= boolean_primary",
 /* 241 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 242 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 243 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 244 */ "boolean_primary ::= predicate",
 /* 245 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 246 */ "common_expression ::= expression",
 /* 247 */ "common_expression ::= boolean_value_expression",
 /* 248 */ "from_clause ::= FROM table_reference_list",
 /* 249 */ "table_reference_list ::= table_reference",
 /* 250 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 251 */ "table_reference ::= table_primary",
 /* 252 */ "table_reference ::= joined_table",
 /* 253 */ "table_primary ::= table_name alias_opt",
 /* 254 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 255 */ "table_primary ::= subquery alias_opt",
 /* 256 */ "table_primary ::= parenthesized_joined_table",
 /* 257 */ "alias_opt ::=",
 /* 258 */ "alias_opt ::= table_alias",
 /* 259 */ "alias_opt ::= AS table_alias",
 /* 260 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 261 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 262 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 263 */ "join_type ::=",
 /* 264 */ "join_type ::= INNER",
 /* 265 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 266 */ "set_quantifier_opt ::=",
 /* 267 */ "set_quantifier_opt ::= DISTINCT",
 /* 268 */ "set_quantifier_opt ::= ALL",
 /* 269 */ "select_list ::= NK_STAR",
 /* 270 */ "select_list ::= select_sublist",
 /* 271 */ "select_sublist ::= select_item",
 /* 272 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 273 */ "select_item ::= common_expression",
 /* 274 */ "select_item ::= common_expression column_alias",
 /* 275 */ "select_item ::= common_expression AS column_alias",
 /* 276 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 277 */ "where_clause_opt ::=",
 /* 278 */ "where_clause_opt ::= WHERE search_condition",
 /* 279 */ "partition_by_clause_opt ::=",
 /* 280 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 281 */ "twindow_clause_opt ::=",
 /* 282 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 283 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 284 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 285 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 286 */ "sliding_opt ::=",
 /* 287 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 288 */ "fill_opt ::=",
 /* 289 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 290 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 291 */ "fill_mode ::= NONE",
 /* 292 */ "fill_mode ::= PREV",
 /* 293 */ "fill_mode ::= NULL",
 /* 294 */ "fill_mode ::= LINEAR",
 /* 295 */ "fill_mode ::= NEXT",
 /* 296 */ "group_by_clause_opt ::=",
 /* 297 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 298 */ "group_by_list ::= expression",
 /* 299 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 300 */ "having_clause_opt ::=",
 /* 301 */ "having_clause_opt ::= HAVING search_condition",
 /* 302 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 303 */ "query_expression_body ::= query_primary",
 /* 304 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 305 */ "query_primary ::= query_specification",
 /* 306 */ "order_by_clause_opt ::=",
 /* 307 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 308 */ "slimit_clause_opt ::=",
 /* 309 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 310 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 311 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 312 */ "limit_clause_opt ::=",
 /* 313 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 314 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 315 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 316 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 317 */ "search_condition ::= common_expression",
 /* 318 */ "sort_specification_list ::= sort_specification",
 /* 319 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 320 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 321 */ "ordering_specification_opt ::=",
 /* 322 */ "ordering_specification_opt ::= ASC",
 /* 323 */ "ordering_specification_opt ::= DESC",
 /* 324 */ "null_ordering_opt ::=",
 /* 325 */ "null_ordering_opt ::= NULLS FIRST",
 /* 326 */ "null_ordering_opt ::= NULLS LAST",
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
    case 170: /* db_options */
    case 172: /* alter_db_options */
    case 174: /* full_table_name */
    case 177: /* table_options */
    case 181: /* alter_table_clause */
    case 182: /* alter_table_options */
    case 185: /* create_subtable_clause */
    case 188: /* drop_table_clause */
    case 191: /* column_def */
    case 194: /* col_name */
    case 195: /* func_name */
    case 198: /* index_options */
    case 200: /* duration_literal */
    case 201: /* sliding_opt */
    case 202: /* func */
    case 205: /* query_expression */
    case 206: /* signed */
    case 207: /* signed_literal */
    case 210: /* expression */
    case 211: /* column_reference */
    case 212: /* subquery */
    case 213: /* predicate */
    case 216: /* in_predicate_value */
    case 217: /* boolean_value_expression */
    case 218: /* boolean_primary */
    case 219: /* common_expression */
    case 220: /* from_clause */
    case 221: /* table_reference_list */
    case 222: /* table_reference */
    case 223: /* table_primary */
    case 224: /* joined_table */
    case 226: /* parenthesized_joined_table */
    case 228: /* search_condition */
    case 229: /* query_specification */
    case 232: /* where_clause_opt */
    case 234: /* twindow_clause_opt */
    case 236: /* having_clause_opt */
    case 238: /* select_item */
    case 239: /* fill_opt */
    case 242: /* query_expression_body */
    case 244: /* slimit_clause_opt */
    case 245: /* limit_clause_opt */
    case 246: /* query_primary */
    case 248: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy348)); 
}
      break;
    case 161: /* account_options */
    case 162: /* alter_account_options */
    case 164: /* alter_account_option */
{
 
}
      break;
    case 165: /* user_name */
    case 166: /* dnode_endpoint */
    case 167: /* dnode_host_name */
    case 169: /* db_name */
    case 183: /* column_name */
    case 190: /* table_name */
    case 196: /* function_name */
    case 197: /* index_name */
    case 204: /* topic_name */
    case 208: /* table_alias */
    case 209: /* column_alias */
    case 225: /* alias_opt */
{
 
}
      break;
    case 168: /* not_exists_opt */
    case 171: /* exists_opt */
    case 230: /* set_quantifier_opt */
{
 
}
      break;
    case 173: /* alter_db_option */
    case 193: /* alter_table_option */
{
 
}
      break;
    case 175: /* column_def_list */
    case 176: /* tags_def_opt */
    case 178: /* multi_create_clause */
    case 179: /* tags_def */
    case 180: /* multi_drop_clause */
    case 186: /* specific_tags_opt */
    case 187: /* literal_list */
    case 189: /* col_name_list */
    case 192: /* func_name_list */
    case 199: /* func_list */
    case 203: /* expression_list */
    case 231: /* select_list */
    case 233: /* partition_by_clause_opt */
    case 235: /* group_by_clause_opt */
    case 237: /* select_sublist */
    case 241: /* group_by_list */
    case 243: /* order_by_clause_opt */
    case 247: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy358)); 
}
      break;
    case 184: /* type_name */
{
 
}
      break;
    case 214: /* compare_op */
    case 215: /* in_op */
{
 
}
      break;
    case 227: /* join_type */
{
 
}
      break;
    case 240: /* fill_mode */
{
 
}
      break;
    case 249: /* ordering_specification_opt */
{
 
}
      break;
    case 250: /* null_ordering_opt */
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
  {  160,   -6 }, /* (0) cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
  {  160,   -4 }, /* (1) cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
  {  161,    0 }, /* (2) account_options ::= */
  {  161,   -3 }, /* (3) account_options ::= account_options PPS literal */
  {  161,   -3 }, /* (4) account_options ::= account_options TSERIES literal */
  {  161,   -3 }, /* (5) account_options ::= account_options STORAGE literal */
  {  161,   -3 }, /* (6) account_options ::= account_options STREAMS literal */
  {  161,   -3 }, /* (7) account_options ::= account_options QTIME literal */
  {  161,   -3 }, /* (8) account_options ::= account_options DBS literal */
  {  161,   -3 }, /* (9) account_options ::= account_options USERS literal */
  {  161,   -3 }, /* (10) account_options ::= account_options CONNS literal */
  {  161,   -3 }, /* (11) account_options ::= account_options STATE literal */
  {  162,   -1 }, /* (12) alter_account_options ::= alter_account_option */
  {  162,   -2 }, /* (13) alter_account_options ::= alter_account_options alter_account_option */
  {  164,   -2 }, /* (14) alter_account_option ::= PASS literal */
  {  164,   -2 }, /* (15) alter_account_option ::= PPS literal */
  {  164,   -2 }, /* (16) alter_account_option ::= TSERIES literal */
  {  164,   -2 }, /* (17) alter_account_option ::= STORAGE literal */
  {  164,   -2 }, /* (18) alter_account_option ::= STREAMS literal */
  {  164,   -2 }, /* (19) alter_account_option ::= QTIME literal */
  {  164,   -2 }, /* (20) alter_account_option ::= DBS literal */
  {  164,   -2 }, /* (21) alter_account_option ::= USERS literal */
  {  164,   -2 }, /* (22) alter_account_option ::= CONNS literal */
  {  164,   -2 }, /* (23) alter_account_option ::= STATE literal */
  {  160,   -5 }, /* (24) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  160,   -5 }, /* (25) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  160,   -5 }, /* (26) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  160,   -3 }, /* (27) cmd ::= DROP USER user_name */
  {  160,   -2 }, /* (28) cmd ::= SHOW USERS */
  {  160,   -3 }, /* (29) cmd ::= CREATE DNODE dnode_endpoint */
  {  160,   -5 }, /* (30) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  160,   -3 }, /* (31) cmd ::= DROP DNODE NK_INTEGER */
  {  160,   -3 }, /* (32) cmd ::= DROP DNODE dnode_endpoint */
  {  160,   -2 }, /* (33) cmd ::= SHOW DNODES */
  {  160,   -4 }, /* (34) cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
  {  160,   -5 }, /* (35) cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
  {  160,   -4 }, /* (36) cmd ::= ALTER ALL DNODES NK_STRING */
  {  160,   -5 }, /* (37) cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
  {  166,   -1 }, /* (38) dnode_endpoint ::= NK_STRING */
  {  167,   -1 }, /* (39) dnode_host_name ::= NK_ID */
  {  167,   -1 }, /* (40) dnode_host_name ::= NK_IPTOKEN */
  {  160,   -3 }, /* (41) cmd ::= ALTER LOCAL NK_STRING */
  {  160,   -4 }, /* (42) cmd ::= ALTER LOCAL NK_STRING NK_STRING */
  {  160,   -5 }, /* (43) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  160,   -5 }, /* (44) cmd ::= DROP QNODE ON DNODE NK_INTEGER */
  {  160,   -2 }, /* (45) cmd ::= SHOW QNODES */
  {  160,   -5 }, /* (46) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  160,   -4 }, /* (47) cmd ::= DROP DATABASE exists_opt db_name */
  {  160,   -2 }, /* (48) cmd ::= SHOW DATABASES */
  {  160,   -2 }, /* (49) cmd ::= USE db_name */
  {  160,   -4 }, /* (50) cmd ::= ALTER DATABASE db_name alter_db_options */
  {  168,   -3 }, /* (51) not_exists_opt ::= IF NOT EXISTS */
  {  168,    0 }, /* (52) not_exists_opt ::= */
  {  171,   -2 }, /* (53) exists_opt ::= IF EXISTS */
  {  171,    0 }, /* (54) exists_opt ::= */
  {  170,    0 }, /* (55) db_options ::= */
  {  170,   -3 }, /* (56) db_options ::= db_options BLOCKS NK_INTEGER */
  {  170,   -3 }, /* (57) db_options ::= db_options CACHE NK_INTEGER */
  {  170,   -3 }, /* (58) db_options ::= db_options CACHELAST NK_INTEGER */
  {  170,   -3 }, /* (59) db_options ::= db_options COMP NK_INTEGER */
  {  170,   -3 }, /* (60) db_options ::= db_options DAYS NK_INTEGER */
  {  170,   -3 }, /* (61) db_options ::= db_options FSYNC NK_INTEGER */
  {  170,   -3 }, /* (62) db_options ::= db_options MAXROWS NK_INTEGER */
  {  170,   -3 }, /* (63) db_options ::= db_options MINROWS NK_INTEGER */
  {  170,   -3 }, /* (64) db_options ::= db_options KEEP NK_INTEGER */
  {  170,   -3 }, /* (65) db_options ::= db_options PRECISION NK_STRING */
  {  170,   -3 }, /* (66) db_options ::= db_options QUORUM NK_INTEGER */
  {  170,   -3 }, /* (67) db_options ::= db_options REPLICA NK_INTEGER */
  {  170,   -3 }, /* (68) db_options ::= db_options TTL NK_INTEGER */
  {  170,   -3 }, /* (69) db_options ::= db_options WAL NK_INTEGER */
  {  170,   -3 }, /* (70) db_options ::= db_options VGROUPS NK_INTEGER */
  {  170,   -3 }, /* (71) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  170,   -3 }, /* (72) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  170,   -3 }, /* (73) db_options ::= db_options RETENTIONS NK_STRING */
  {  170,   -3 }, /* (74) db_options ::= db_options FILE_FACTOR NK_FLOAT */
  {  172,   -1 }, /* (75) alter_db_options ::= alter_db_option */
  {  172,   -2 }, /* (76) alter_db_options ::= alter_db_options alter_db_option */
  {  173,   -2 }, /* (77) alter_db_option ::= BLOCKS NK_INTEGER */
  {  173,   -2 }, /* (78) alter_db_option ::= FSYNC NK_INTEGER */
  {  173,   -2 }, /* (79) alter_db_option ::= KEEP NK_INTEGER */
  {  173,   -2 }, /* (80) alter_db_option ::= WAL NK_INTEGER */
  {  173,   -2 }, /* (81) alter_db_option ::= QUORUM NK_INTEGER */
  {  173,   -2 }, /* (82) alter_db_option ::= CACHELAST NK_INTEGER */
  {  160,   -9 }, /* (83) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  160,   -3 }, /* (84) cmd ::= CREATE TABLE multi_create_clause */
  {  160,   -9 }, /* (85) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  160,   -3 }, /* (86) cmd ::= DROP TABLE multi_drop_clause */
  {  160,   -4 }, /* (87) cmd ::= DROP STABLE exists_opt full_table_name */
  {  160,   -2 }, /* (88) cmd ::= SHOW TABLES */
  {  160,   -2 }, /* (89) cmd ::= SHOW STABLES */
  {  160,   -3 }, /* (90) cmd ::= ALTER TABLE alter_table_clause */
  {  160,   -3 }, /* (91) cmd ::= ALTER STABLE alter_table_clause */
  {  181,   -2 }, /* (92) alter_table_clause ::= full_table_name alter_table_options */
  {  181,   -5 }, /* (93) alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
  {  181,   -4 }, /* (94) alter_table_clause ::= full_table_name DROP COLUMN column_name */
  {  181,   -5 }, /* (95) alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
  {  181,   -5 }, /* (96) alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
  {  181,   -5 }, /* (97) alter_table_clause ::= full_table_name ADD TAG column_name type_name */
  {  181,   -4 }, /* (98) alter_table_clause ::= full_table_name DROP TAG column_name */
  {  181,   -5 }, /* (99) alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
  {  181,   -5 }, /* (100) alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
  {  181,   -6 }, /* (101) alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
  {  178,   -1 }, /* (102) multi_create_clause ::= create_subtable_clause */
  {  178,   -2 }, /* (103) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  185,   -9 }, /* (104) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  180,   -1 }, /* (105) multi_drop_clause ::= drop_table_clause */
  {  180,   -2 }, /* (106) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  188,   -2 }, /* (107) drop_table_clause ::= exists_opt full_table_name */
  {  186,    0 }, /* (108) specific_tags_opt ::= */
  {  186,   -3 }, /* (109) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  174,   -1 }, /* (110) full_table_name ::= table_name */
  {  174,   -3 }, /* (111) full_table_name ::= db_name NK_DOT table_name */
  {  175,   -1 }, /* (112) column_def_list ::= column_def */
  {  175,   -3 }, /* (113) column_def_list ::= column_def_list NK_COMMA column_def */
  {  191,   -2 }, /* (114) column_def ::= column_name type_name */
  {  191,   -4 }, /* (115) column_def ::= column_name type_name COMMENT NK_STRING */
  {  184,   -1 }, /* (116) type_name ::= BOOL */
  {  184,   -1 }, /* (117) type_name ::= TINYINT */
  {  184,   -1 }, /* (118) type_name ::= SMALLINT */
  {  184,   -1 }, /* (119) type_name ::= INT */
  {  184,   -1 }, /* (120) type_name ::= INTEGER */
  {  184,   -1 }, /* (121) type_name ::= BIGINT */
  {  184,   -1 }, /* (122) type_name ::= FLOAT */
  {  184,   -1 }, /* (123) type_name ::= DOUBLE */
  {  184,   -4 }, /* (124) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  184,   -1 }, /* (125) type_name ::= TIMESTAMP */
  {  184,   -4 }, /* (126) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  184,   -2 }, /* (127) type_name ::= TINYINT UNSIGNED */
  {  184,   -2 }, /* (128) type_name ::= SMALLINT UNSIGNED */
  {  184,   -2 }, /* (129) type_name ::= INT UNSIGNED */
  {  184,   -2 }, /* (130) type_name ::= BIGINT UNSIGNED */
  {  184,   -1 }, /* (131) type_name ::= JSON */
  {  184,   -4 }, /* (132) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  184,   -1 }, /* (133) type_name ::= MEDIUMBLOB */
  {  184,   -1 }, /* (134) type_name ::= BLOB */
  {  184,   -4 }, /* (135) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  184,   -1 }, /* (136) type_name ::= DECIMAL */
  {  184,   -4 }, /* (137) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  184,   -6 }, /* (138) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  176,    0 }, /* (139) tags_def_opt ::= */
  {  176,   -1 }, /* (140) tags_def_opt ::= tags_def */
  {  179,   -4 }, /* (141) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  177,    0 }, /* (142) table_options ::= */
  {  177,   -3 }, /* (143) table_options ::= table_options COMMENT NK_STRING */
  {  177,   -3 }, /* (144) table_options ::= table_options KEEP NK_INTEGER */
  {  177,   -3 }, /* (145) table_options ::= table_options TTL NK_INTEGER */
  {  177,   -5 }, /* (146) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  177,   -5 }, /* (147) table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
  {  182,   -1 }, /* (148) alter_table_options ::= alter_table_option */
  {  182,   -2 }, /* (149) alter_table_options ::= alter_table_options alter_table_option */
  {  193,   -2 }, /* (150) alter_table_option ::= COMMENT NK_STRING */
  {  193,   -2 }, /* (151) alter_table_option ::= KEEP NK_INTEGER */
  {  193,   -2 }, /* (152) alter_table_option ::= TTL NK_INTEGER */
  {  189,   -1 }, /* (153) col_name_list ::= col_name */
  {  189,   -3 }, /* (154) col_name_list ::= col_name_list NK_COMMA col_name */
  {  194,   -1 }, /* (155) col_name ::= column_name */
  {  192,   -1 }, /* (156) func_name_list ::= func_name */
  {  192,   -3 }, /* (157) func_name_list ::= func_name_list NK_COMMA col_name */
  {  195,   -1 }, /* (158) func_name ::= function_name */
  {  160,   -7 }, /* (159) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  160,   -9 }, /* (160) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  160,   -5 }, /* (161) cmd ::= DROP INDEX index_name ON table_name */
  {  198,    0 }, /* (162) index_options ::= */
  {  198,   -9 }, /* (163) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  198,  -11 }, /* (164) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  199,   -1 }, /* (165) func_list ::= func */
  {  199,   -3 }, /* (166) func_list ::= func_list NK_COMMA func */
  {  202,   -4 }, /* (167) func ::= function_name NK_LP expression_list NK_RP */
  {  160,   -6 }, /* (168) cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
  {  160,   -6 }, /* (169) cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
  {  160,   -4 }, /* (170) cmd ::= DROP TOPIC exists_opt topic_name */
  {  160,   -2 }, /* (171) cmd ::= SHOW VGROUPS */
  {  160,   -4 }, /* (172) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  160,   -2 }, /* (173) cmd ::= SHOW MNODES */
  {  160,   -1 }, /* (174) cmd ::= query_expression */
  {  163,   -1 }, /* (175) literal ::= NK_INTEGER */
  {  163,   -1 }, /* (176) literal ::= NK_FLOAT */
  {  163,   -1 }, /* (177) literal ::= NK_STRING */
  {  163,   -1 }, /* (178) literal ::= NK_BOOL */
  {  163,   -2 }, /* (179) literal ::= TIMESTAMP NK_STRING */
  {  163,   -1 }, /* (180) literal ::= duration_literal */
  {  200,   -1 }, /* (181) duration_literal ::= NK_VARIABLE */
  {  206,   -1 }, /* (182) signed ::= NK_INTEGER */
  {  206,   -2 }, /* (183) signed ::= NK_PLUS NK_INTEGER */
  {  206,   -2 }, /* (184) signed ::= NK_MINUS NK_INTEGER */
  {  206,   -1 }, /* (185) signed ::= NK_FLOAT */
  {  206,   -2 }, /* (186) signed ::= NK_PLUS NK_FLOAT */
  {  206,   -2 }, /* (187) signed ::= NK_MINUS NK_FLOAT */
  {  207,   -1 }, /* (188) signed_literal ::= signed */
  {  207,   -1 }, /* (189) signed_literal ::= NK_STRING */
  {  207,   -1 }, /* (190) signed_literal ::= NK_BOOL */
  {  207,   -2 }, /* (191) signed_literal ::= TIMESTAMP NK_STRING */
  {  207,   -1 }, /* (192) signed_literal ::= duration_literal */
  {  187,   -1 }, /* (193) literal_list ::= signed_literal */
  {  187,   -3 }, /* (194) literal_list ::= literal_list NK_COMMA signed_literal */
  {  169,   -1 }, /* (195) db_name ::= NK_ID */
  {  190,   -1 }, /* (196) table_name ::= NK_ID */
  {  183,   -1 }, /* (197) column_name ::= NK_ID */
  {  196,   -1 }, /* (198) function_name ::= NK_ID */
  {  208,   -1 }, /* (199) table_alias ::= NK_ID */
  {  209,   -1 }, /* (200) column_alias ::= NK_ID */
  {  165,   -1 }, /* (201) user_name ::= NK_ID */
  {  197,   -1 }, /* (202) index_name ::= NK_ID */
  {  204,   -1 }, /* (203) topic_name ::= NK_ID */
  {  210,   -1 }, /* (204) expression ::= literal */
  {  210,   -1 }, /* (205) expression ::= column_reference */
  {  210,   -4 }, /* (206) expression ::= function_name NK_LP expression_list NK_RP */
  {  210,   -4 }, /* (207) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  210,   -1 }, /* (208) expression ::= subquery */
  {  210,   -3 }, /* (209) expression ::= NK_LP expression NK_RP */
  {  210,   -2 }, /* (210) expression ::= NK_PLUS expression */
  {  210,   -2 }, /* (211) expression ::= NK_MINUS expression */
  {  210,   -3 }, /* (212) expression ::= expression NK_PLUS expression */
  {  210,   -3 }, /* (213) expression ::= expression NK_MINUS expression */
  {  210,   -3 }, /* (214) expression ::= expression NK_STAR expression */
  {  210,   -3 }, /* (215) expression ::= expression NK_SLASH expression */
  {  210,   -3 }, /* (216) expression ::= expression NK_REM expression */
  {  203,   -1 }, /* (217) expression_list ::= expression */
  {  203,   -3 }, /* (218) expression_list ::= expression_list NK_COMMA expression */
  {  211,   -1 }, /* (219) column_reference ::= column_name */
  {  211,   -3 }, /* (220) column_reference ::= table_name NK_DOT column_name */
  {  213,   -3 }, /* (221) predicate ::= expression compare_op expression */
  {  213,   -5 }, /* (222) predicate ::= expression BETWEEN expression AND expression */
  {  213,   -6 }, /* (223) predicate ::= expression NOT BETWEEN expression AND expression */
  {  213,   -3 }, /* (224) predicate ::= expression IS NULL */
  {  213,   -4 }, /* (225) predicate ::= expression IS NOT NULL */
  {  213,   -3 }, /* (226) predicate ::= expression in_op in_predicate_value */
  {  214,   -1 }, /* (227) compare_op ::= NK_LT */
  {  214,   -1 }, /* (228) compare_op ::= NK_GT */
  {  214,   -1 }, /* (229) compare_op ::= NK_LE */
  {  214,   -1 }, /* (230) compare_op ::= NK_GE */
  {  214,   -1 }, /* (231) compare_op ::= NK_NE */
  {  214,   -1 }, /* (232) compare_op ::= NK_EQ */
  {  214,   -1 }, /* (233) compare_op ::= LIKE */
  {  214,   -2 }, /* (234) compare_op ::= NOT LIKE */
  {  214,   -1 }, /* (235) compare_op ::= MATCH */
  {  214,   -1 }, /* (236) compare_op ::= NMATCH */
  {  215,   -1 }, /* (237) in_op ::= IN */
  {  215,   -2 }, /* (238) in_op ::= NOT IN */
  {  216,   -3 }, /* (239) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  217,   -1 }, /* (240) boolean_value_expression ::= boolean_primary */
  {  217,   -2 }, /* (241) boolean_value_expression ::= NOT boolean_primary */
  {  217,   -3 }, /* (242) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  217,   -3 }, /* (243) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  218,   -1 }, /* (244) boolean_primary ::= predicate */
  {  218,   -3 }, /* (245) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  219,   -1 }, /* (246) common_expression ::= expression */
  {  219,   -1 }, /* (247) common_expression ::= boolean_value_expression */
  {  220,   -2 }, /* (248) from_clause ::= FROM table_reference_list */
  {  221,   -1 }, /* (249) table_reference_list ::= table_reference */
  {  221,   -3 }, /* (250) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  222,   -1 }, /* (251) table_reference ::= table_primary */
  {  222,   -1 }, /* (252) table_reference ::= joined_table */
  {  223,   -2 }, /* (253) table_primary ::= table_name alias_opt */
  {  223,   -4 }, /* (254) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  223,   -2 }, /* (255) table_primary ::= subquery alias_opt */
  {  223,   -1 }, /* (256) table_primary ::= parenthesized_joined_table */
  {  225,    0 }, /* (257) alias_opt ::= */
  {  225,   -1 }, /* (258) alias_opt ::= table_alias */
  {  225,   -2 }, /* (259) alias_opt ::= AS table_alias */
  {  226,   -3 }, /* (260) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  226,   -3 }, /* (261) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  224,   -6 }, /* (262) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  227,    0 }, /* (263) join_type ::= */
  {  227,   -1 }, /* (264) join_type ::= INNER */
  {  229,   -9 }, /* (265) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  230,    0 }, /* (266) set_quantifier_opt ::= */
  {  230,   -1 }, /* (267) set_quantifier_opt ::= DISTINCT */
  {  230,   -1 }, /* (268) set_quantifier_opt ::= ALL */
  {  231,   -1 }, /* (269) select_list ::= NK_STAR */
  {  231,   -1 }, /* (270) select_list ::= select_sublist */
  {  237,   -1 }, /* (271) select_sublist ::= select_item */
  {  237,   -3 }, /* (272) select_sublist ::= select_sublist NK_COMMA select_item */
  {  238,   -1 }, /* (273) select_item ::= common_expression */
  {  238,   -2 }, /* (274) select_item ::= common_expression column_alias */
  {  238,   -3 }, /* (275) select_item ::= common_expression AS column_alias */
  {  238,   -3 }, /* (276) select_item ::= table_name NK_DOT NK_STAR */
  {  232,    0 }, /* (277) where_clause_opt ::= */
  {  232,   -2 }, /* (278) where_clause_opt ::= WHERE search_condition */
  {  233,    0 }, /* (279) partition_by_clause_opt ::= */
  {  233,   -3 }, /* (280) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  234,    0 }, /* (281) twindow_clause_opt ::= */
  {  234,   -6 }, /* (282) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  234,   -4 }, /* (283) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  234,   -6 }, /* (284) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  234,   -8 }, /* (285) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  201,    0 }, /* (286) sliding_opt ::= */
  {  201,   -4 }, /* (287) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  239,    0 }, /* (288) fill_opt ::= */
  {  239,   -4 }, /* (289) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  239,   -6 }, /* (290) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  240,   -1 }, /* (291) fill_mode ::= NONE */
  {  240,   -1 }, /* (292) fill_mode ::= PREV */
  {  240,   -1 }, /* (293) fill_mode ::= NULL */
  {  240,   -1 }, /* (294) fill_mode ::= LINEAR */
  {  240,   -1 }, /* (295) fill_mode ::= NEXT */
  {  235,    0 }, /* (296) group_by_clause_opt ::= */
  {  235,   -3 }, /* (297) group_by_clause_opt ::= GROUP BY group_by_list */
  {  241,   -1 }, /* (298) group_by_list ::= expression */
  {  241,   -3 }, /* (299) group_by_list ::= group_by_list NK_COMMA expression */
  {  236,    0 }, /* (300) having_clause_opt ::= */
  {  236,   -2 }, /* (301) having_clause_opt ::= HAVING search_condition */
  {  205,   -4 }, /* (302) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  242,   -1 }, /* (303) query_expression_body ::= query_primary */
  {  242,   -4 }, /* (304) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  246,   -1 }, /* (305) query_primary ::= query_specification */
  {  243,    0 }, /* (306) order_by_clause_opt ::= */
  {  243,   -3 }, /* (307) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  244,    0 }, /* (308) slimit_clause_opt ::= */
  {  244,   -2 }, /* (309) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  244,   -4 }, /* (310) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  244,   -4 }, /* (311) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  245,    0 }, /* (312) limit_clause_opt ::= */
  {  245,   -2 }, /* (313) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  245,   -4 }, /* (314) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  245,   -4 }, /* (315) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  212,   -3 }, /* (316) subquery ::= NK_LP query_expression NK_RP */
  {  228,   -1 }, /* (317) search_condition ::= common_expression */
  {  247,   -1 }, /* (318) sort_specification_list ::= sort_specification */
  {  247,   -3 }, /* (319) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  248,   -3 }, /* (320) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  249,    0 }, /* (321) ordering_specification_opt ::= */
  {  249,   -1 }, /* (322) ordering_specification_opt ::= ASC */
  {  249,   -1 }, /* (323) ordering_specification_opt ::= DESC */
  {  250,    0 }, /* (324) null_ordering_opt ::= */
  {  250,   -2 }, /* (325) null_ordering_opt ::= NULLS FIRST */
  {  250,   -2 }, /* (326) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
{ pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
  yy_destructor(yypParser,161,&yymsp[0].minor);
        break;
      case 1: /* cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
{ pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
  yy_destructor(yypParser,162,&yymsp[0].minor);
        break;
      case 2: /* account_options ::= */
{ }
        break;
      case 3: /* account_options ::= account_options PPS literal */
      case 4: /* account_options ::= account_options TSERIES literal */ yytestcase(yyruleno==4);
      case 5: /* account_options ::= account_options STORAGE literal */ yytestcase(yyruleno==5);
      case 6: /* account_options ::= account_options STREAMS literal */ yytestcase(yyruleno==6);
      case 7: /* account_options ::= account_options QTIME literal */ yytestcase(yyruleno==7);
      case 8: /* account_options ::= account_options DBS literal */ yytestcase(yyruleno==8);
      case 9: /* account_options ::= account_options USERS literal */ yytestcase(yyruleno==9);
      case 10: /* account_options ::= account_options CONNS literal */ yytestcase(yyruleno==10);
      case 11: /* account_options ::= account_options STATE literal */ yytestcase(yyruleno==11);
{  yy_destructor(yypParser,161,&yymsp[-2].minor);
{ }
  yy_destructor(yypParser,163,&yymsp[0].minor);
}
        break;
      case 12: /* alter_account_options ::= alter_account_option */
{  yy_destructor(yypParser,164,&yymsp[0].minor);
{ }
}
        break;
      case 13: /* alter_account_options ::= alter_account_options alter_account_option */
{  yy_destructor(yypParser,162,&yymsp[-1].minor);
{ }
  yy_destructor(yypParser,164,&yymsp[0].minor);
}
        break;
      case 14: /* alter_account_option ::= PASS literal */
      case 15: /* alter_account_option ::= PPS literal */ yytestcase(yyruleno==15);
      case 16: /* alter_account_option ::= TSERIES literal */ yytestcase(yyruleno==16);
      case 17: /* alter_account_option ::= STORAGE literal */ yytestcase(yyruleno==17);
      case 18: /* alter_account_option ::= STREAMS literal */ yytestcase(yyruleno==18);
      case 19: /* alter_account_option ::= QTIME literal */ yytestcase(yyruleno==19);
      case 20: /* alter_account_option ::= DBS literal */ yytestcase(yyruleno==20);
      case 21: /* alter_account_option ::= USERS literal */ yytestcase(yyruleno==21);
      case 22: /* alter_account_option ::= CONNS literal */ yytestcase(yyruleno==22);
      case 23: /* alter_account_option ::= STATE literal */ yytestcase(yyruleno==23);
{ }
  yy_destructor(yypParser,163,&yymsp[0].minor);
        break;
      case 24: /* cmd ::= CREATE USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy0); }
        break;
      case 25: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy269, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0); }
        break;
      case 26: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy269, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0); }
        break;
      case 27: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy269); }
        break;
      case 28: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 29: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy269, NULL); }
        break;
      case 30: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy0); }
        break;
      case 31: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 32: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy269); }
        break;
      case 33: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 34: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, NULL); }
        break;
      case 35: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 36: /* cmd ::= ALTER ALL DNODES NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[0].minor.yy0, NULL); }
        break;
      case 37: /* cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 38: /* dnode_endpoint ::= NK_STRING */
      case 39: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==39);
      case 40: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==40);
      case 195: /* db_name ::= NK_ID */ yytestcase(yyruleno==195);
      case 196: /* table_name ::= NK_ID */ yytestcase(yyruleno==196);
      case 197: /* column_name ::= NK_ID */ yytestcase(yyruleno==197);
      case 198: /* function_name ::= NK_ID */ yytestcase(yyruleno==198);
      case 199: /* table_alias ::= NK_ID */ yytestcase(yyruleno==199);
      case 200: /* column_alias ::= NK_ID */ yytestcase(yyruleno==200);
      case 201: /* user_name ::= NK_ID */ yytestcase(yyruleno==201);
      case 202: /* index_name ::= NK_ID */ yytestcase(yyruleno==202);
      case 203: /* topic_name ::= NK_ID */ yytestcase(yyruleno==203);
{ yylhsminor.yy269 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy269 = yylhsminor.yy269;
        break;
      case 41: /* cmd ::= ALTER LOCAL NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 42: /* cmd ::= ALTER LOCAL NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 43: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createCreateQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 44: /* cmd ::= DROP QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 45: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL); }
        break;
      case 46: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy345, &yymsp[-1].minor.yy269, yymsp[0].minor.yy348); }
        break;
      case 47: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy345, &yymsp[0].minor.yy269); }
        break;
      case 48: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 49: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy269); }
        break;
      case 50: /* cmd ::= ALTER DATABASE db_name alter_db_options */
{ pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &yymsp[-1].minor.yy269, yymsp[0].minor.yy348); }
        break;
      case 51: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy345 = true; }
        break;
      case 52: /* not_exists_opt ::= */
      case 54: /* exists_opt ::= */ yytestcase(yyruleno==54);
      case 266: /* set_quantifier_opt ::= */ yytestcase(yyruleno==266);
{ yymsp[1].minor.yy345 = false; }
        break;
      case 53: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy345 = true; }
        break;
      case 55: /* db_options ::= */
{ yymsp[1].minor.yy348 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 56: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 57: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 58: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 59: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 60: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 61: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 62: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 63: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 64: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 65: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 66: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 67: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 68: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 69: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 70: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 71: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_SINGLE_STABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 72: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_STREAM_MODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 73: /* db_options ::= db_options RETENTIONS NK_STRING */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_RETENTIONS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 74: /* db_options ::= db_options FILE_FACTOR NK_FLOAT */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-2].minor.yy348, DB_OPTION_FILE_FACTOR, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 75: /* alter_db_options ::= alter_db_option */
{ yylhsminor.yy348 = createDefaultAlterDatabaseOptions(pCxt); yylhsminor.yy348 = setDatabaseOption(pCxt, yylhsminor.yy348, yymsp[0].minor.yy233.type, &yymsp[0].minor.yy233.val); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 76: /* alter_db_options ::= alter_db_options alter_db_option */
{ yylhsminor.yy348 = setDatabaseOption(pCxt, yymsp[-1].minor.yy348, yymsp[0].minor.yy233.type, &yymsp[0].minor.yy233.val); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 77: /* alter_db_option ::= BLOCKS NK_INTEGER */
{ yymsp[-1].minor.yy233.type = DB_OPTION_BLOCKS; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 78: /* alter_db_option ::= FSYNC NK_INTEGER */
{ yymsp[-1].minor.yy233.type = DB_OPTION_FSYNC; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 79: /* alter_db_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy233.type = DB_OPTION_KEEP; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 80: /* alter_db_option ::= WAL NK_INTEGER */
{ yymsp[-1].minor.yy233.type = DB_OPTION_WAL; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 81: /* alter_db_option ::= QUORUM NK_INTEGER */
{ yymsp[-1].minor.yy233.type = DB_OPTION_QUORUM; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 82: /* alter_db_option ::= CACHELAST NK_INTEGER */
{ yymsp[-1].minor.yy233.type = DB_OPTION_CACHELAST; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 83: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 85: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==85);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy345, yymsp[-5].minor.yy348, yymsp[-3].minor.yy358, yymsp[-1].minor.yy358, yymsp[0].minor.yy348); }
        break;
      case 84: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy358); }
        break;
      case 86: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy358); }
        break;
      case 87: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy345, yymsp[0].minor.yy348); }
        break;
      case 88: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 89: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 90: /* cmd ::= ALTER TABLE alter_table_clause */
      case 91: /* cmd ::= ALTER STABLE alter_table_clause */ yytestcase(yyruleno==91);
      case 174: /* cmd ::= query_expression */ yytestcase(yyruleno==174);
{ pCxt->pRootNode = yymsp[0].minor.yy348; }
        break;
      case 92: /* alter_table_clause ::= full_table_name alter_table_options */
{ yylhsminor.yy348 = createAlterTableOption(pCxt, yymsp[-1].minor.yy348, yymsp[0].minor.yy348); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 93: /* alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
{ yylhsminor.yy348 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy348, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[-1].minor.yy269, yymsp[0].minor.yy218); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 94: /* alter_table_clause ::= full_table_name DROP COLUMN column_name */
{ yylhsminor.yy348 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy348, TSDB_ALTER_TABLE_DROP_COLUMN, &yymsp[0].minor.yy269); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 95: /* alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
{ yylhsminor.yy348 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy348, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, &yymsp[-1].minor.yy269, yymsp[0].minor.yy218); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 96: /* alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
{ yylhsminor.yy348 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy348, TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, &yymsp[-1].minor.yy269, &yymsp[0].minor.yy269); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 97: /* alter_table_clause ::= full_table_name ADD TAG column_name type_name */
{ yylhsminor.yy348 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy348, TSDB_ALTER_TABLE_ADD_TAG, &yymsp[-1].minor.yy269, yymsp[0].minor.yy218); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 98: /* alter_table_clause ::= full_table_name DROP TAG column_name */
{ yylhsminor.yy348 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy348, TSDB_ALTER_TABLE_DROP_TAG, &yymsp[0].minor.yy269); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 99: /* alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
{ yylhsminor.yy348 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy348, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, &yymsp[-1].minor.yy269, yymsp[0].minor.yy218); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 100: /* alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
{ yylhsminor.yy348 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy348, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, &yymsp[-1].minor.yy269, &yymsp[0].minor.yy269); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 101: /* alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
{ yylhsminor.yy348 = createAlterTableSetTag(pCxt, yymsp[-5].minor.yy348, &yymsp[-2].minor.yy269, yymsp[0].minor.yy348); }
  yymsp[-5].minor.yy348 = yylhsminor.yy348;
        break;
      case 102: /* multi_create_clause ::= create_subtable_clause */
      case 105: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==105);
      case 112: /* column_def_list ::= column_def */ yytestcase(yyruleno==112);
      case 153: /* col_name_list ::= col_name */ yytestcase(yyruleno==153);
      case 156: /* func_name_list ::= func_name */ yytestcase(yyruleno==156);
      case 165: /* func_list ::= func */ yytestcase(yyruleno==165);
      case 193: /* literal_list ::= signed_literal */ yytestcase(yyruleno==193);
      case 271: /* select_sublist ::= select_item */ yytestcase(yyruleno==271);
      case 318: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==318);
{ yylhsminor.yy358 = createNodeList(pCxt, yymsp[0].minor.yy348); }
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 103: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 106: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==106);
{ yylhsminor.yy358 = addNodeToList(pCxt, yymsp[-1].minor.yy358, yymsp[0].minor.yy348); }
  yymsp[-1].minor.yy358 = yylhsminor.yy358;
        break;
      case 104: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy348 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy345, yymsp[-7].minor.yy348, yymsp[-5].minor.yy348, yymsp[-4].minor.yy358, yymsp[-1].minor.yy358); }
  yymsp[-8].minor.yy348 = yylhsminor.yy348;
        break;
      case 107: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy348 = createDropTableClause(pCxt, yymsp[-1].minor.yy345, yymsp[0].minor.yy348); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 108: /* specific_tags_opt ::= */
      case 139: /* tags_def_opt ::= */ yytestcase(yyruleno==139);
      case 279: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==279);
      case 296: /* group_by_clause_opt ::= */ yytestcase(yyruleno==296);
      case 306: /* order_by_clause_opt ::= */ yytestcase(yyruleno==306);
{ yymsp[1].minor.yy358 = NULL; }
        break;
      case 109: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy358 = yymsp[-1].minor.yy358; }
        break;
      case 110: /* full_table_name ::= table_name */
{ yylhsminor.yy348 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy269, NULL); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 111: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy348 = createRealTableNode(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy269, NULL); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 113: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 154: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==154);
      case 157: /* func_name_list ::= func_name_list NK_COMMA col_name */ yytestcase(yyruleno==157);
      case 166: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==166);
      case 194: /* literal_list ::= literal_list NK_COMMA signed_literal */ yytestcase(yyruleno==194);
      case 272: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==272);
      case 319: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==319);
{ yylhsminor.yy358 = addNodeToList(pCxt, yymsp[-2].minor.yy358, yymsp[0].minor.yy348); }
  yymsp[-2].minor.yy358 = yylhsminor.yy358;
        break;
      case 114: /* column_def ::= column_name type_name */
{ yylhsminor.yy348 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy269, yymsp[0].minor.yy218, NULL); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 115: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy348 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy269, yymsp[-2].minor.yy218, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 116: /* type_name ::= BOOL */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 117: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 118: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 119: /* type_name ::= INT */
      case 120: /* type_name ::= INTEGER */ yytestcase(yyruleno==120);
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 121: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 122: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 123: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 124: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy218 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 125: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 126: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy218 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 127: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy218 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 128: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy218 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 129: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy218 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 130: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy218 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 131: /* type_name ::= JSON */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 132: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy218 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 133: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 134: /* type_name ::= BLOB */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 135: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy218 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 136: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy218 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 137: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy218 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 138: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy218 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 140: /* tags_def_opt ::= tags_def */
      case 270: /* select_list ::= select_sublist */ yytestcase(yyruleno==270);
{ yylhsminor.yy358 = yymsp[0].minor.yy358; }
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 141: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy358 = yymsp[-1].minor.yy358; }
        break;
      case 142: /* table_options ::= */
{ yymsp[1].minor.yy348 = createDefaultTableOptions(pCxt); }
        break;
      case 143: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy348 = setTableOption(pCxt, yymsp[-2].minor.yy348, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 144: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy348 = setTableOption(pCxt, yymsp[-2].minor.yy348, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 145: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy348 = setTableOption(pCxt, yymsp[-2].minor.yy348, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 146: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy348 = setTableSmaOption(pCxt, yymsp[-4].minor.yy348, yymsp[-1].minor.yy358); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 147: /* table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
{ yylhsminor.yy348 = setTableRollupOption(pCxt, yymsp[-4].minor.yy348, yymsp[-1].minor.yy358); }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 148: /* alter_table_options ::= alter_table_option */
{ yylhsminor.yy348 = createDefaultAlterTableOptions(pCxt); yylhsminor.yy348 = setTableOption(pCxt, yylhsminor.yy348, yymsp[0].minor.yy233.type, &yymsp[0].minor.yy233.val); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 149: /* alter_table_options ::= alter_table_options alter_table_option */
{ yylhsminor.yy348 = setTableOption(pCxt, yymsp[-1].minor.yy348, yymsp[0].minor.yy233.type, &yymsp[0].minor.yy233.val); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 150: /* alter_table_option ::= COMMENT NK_STRING */
{ yymsp[-1].minor.yy233.type = TABLE_OPTION_COMMENT; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 151: /* alter_table_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy233.type = TABLE_OPTION_KEEP; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 152: /* alter_table_option ::= TTL NK_INTEGER */
{ yymsp[-1].minor.yy233.type = TABLE_OPTION_TTL; yymsp[-1].minor.yy233.val = yymsp[0].minor.yy0; }
        break;
      case 155: /* col_name ::= column_name */
{ yylhsminor.yy348 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy269); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 158: /* func_name ::= function_name */
{ yylhsminor.yy348 = createFunctionNode(pCxt, &yymsp[0].minor.yy269, NULL); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 159: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy269, &yymsp[-1].minor.yy269, NULL, yymsp[0].minor.yy348); }
        break;
      case 160: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy269, &yymsp[-3].minor.yy269, yymsp[-1].minor.yy358, NULL); }
        break;
      case 161: /* cmd ::= DROP INDEX index_name ON table_name */
{ pCxt->pRootNode = createDropIndexStmt(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy269); }
        break;
      case 162: /* index_options ::= */
      case 277: /* where_clause_opt ::= */ yytestcase(yyruleno==277);
      case 281: /* twindow_clause_opt ::= */ yytestcase(yyruleno==281);
      case 286: /* sliding_opt ::= */ yytestcase(yyruleno==286);
      case 288: /* fill_opt ::= */ yytestcase(yyruleno==288);
      case 300: /* having_clause_opt ::= */ yytestcase(yyruleno==300);
      case 308: /* slimit_clause_opt ::= */ yytestcase(yyruleno==308);
      case 312: /* limit_clause_opt ::= */ yytestcase(yyruleno==312);
{ yymsp[1].minor.yy348 = NULL; }
        break;
      case 163: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy348 = createIndexOption(pCxt, yymsp[-6].minor.yy358, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), NULL, yymsp[0].minor.yy348); }
        break;
      case 164: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy348 = createIndexOption(pCxt, yymsp[-8].minor.yy358, releaseRawExprNode(pCxt, yymsp[-4].minor.yy348), releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), yymsp[0].minor.yy348); }
        break;
      case 167: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy348 = createFunctionNode(pCxt, &yymsp[-3].minor.yy269, yymsp[-1].minor.yy358); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 168: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy345, &yymsp[-2].minor.yy269, yymsp[0].minor.yy348, NULL); }
        break;
      case 169: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy345, &yymsp[-2].minor.yy269, NULL, &yymsp[0].minor.yy269); }
        break;
      case 170: /* cmd ::= DROP TOPIC exists_opt topic_name */
{ pCxt->pRootNode = createDropTopicStmt(pCxt, yymsp[-1].minor.yy345, &yymsp[0].minor.yy269); }
        break;
      case 171: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 172: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy269); }
        break;
      case 173: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 175: /* literal ::= NK_INTEGER */
{ yylhsminor.yy348 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 176: /* literal ::= NK_FLOAT */
{ yylhsminor.yy348 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 177: /* literal ::= NK_STRING */
{ yylhsminor.yy348 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 178: /* literal ::= NK_BOOL */
{ yylhsminor.yy348 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 179: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 180: /* literal ::= duration_literal */
      case 188: /* signed_literal ::= signed */ yytestcase(yyruleno==188);
      case 204: /* expression ::= literal */ yytestcase(yyruleno==204);
      case 205: /* expression ::= column_reference */ yytestcase(yyruleno==205);
      case 208: /* expression ::= subquery */ yytestcase(yyruleno==208);
      case 240: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==240);
      case 244: /* boolean_primary ::= predicate */ yytestcase(yyruleno==244);
      case 246: /* common_expression ::= expression */ yytestcase(yyruleno==246);
      case 247: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==247);
      case 249: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==249);
      case 251: /* table_reference ::= table_primary */ yytestcase(yyruleno==251);
      case 252: /* table_reference ::= joined_table */ yytestcase(yyruleno==252);
      case 256: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==256);
      case 303: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==303);
      case 305: /* query_primary ::= query_specification */ yytestcase(yyruleno==305);
{ yylhsminor.yy348 = yymsp[0].minor.yy348; }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 181: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy348 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 182: /* signed ::= NK_INTEGER */
{ yylhsminor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 183: /* signed ::= NK_PLUS NK_INTEGER */
{ yymsp[-1].minor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0); }
        break;
      case 184: /* signed ::= NK_MINUS NK_INTEGER */
{ 
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &t);
                                                                                  }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 185: /* signed ::= NK_FLOAT */
{ yylhsminor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 186: /* signed ::= NK_PLUS NK_FLOAT */
{ yymsp[-1].minor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0); }
        break;
      case 187: /* signed ::= NK_MINUS NK_FLOAT */
{ 
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &t);
                                                                                  }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 189: /* signed_literal ::= NK_STRING */
{ yylhsminor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 190: /* signed_literal ::= NK_BOOL */
{ yylhsminor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 191: /* signed_literal ::= TIMESTAMP NK_STRING */
{ yymsp[-1].minor.yy348 = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0); }
        break;
      case 192: /* signed_literal ::= duration_literal */
      case 317: /* search_condition ::= common_expression */ yytestcase(yyruleno==317);
{ yylhsminor.yy348 = releaseRawExprNode(pCxt, yymsp[0].minor.yy348); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 206: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy269, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy269, yymsp[-1].minor.yy358)); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 207: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy269, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy269, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 209: /* expression ::= NK_LP expression NK_RP */
      case 245: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==245);
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy348)); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 210: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy348));
                                                                                  }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 211: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy348), NULL));
                                                                                  }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 212: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); 
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 213: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); 
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 214: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); 
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 215: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); 
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 216: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); 
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 217: /* expression_list ::= expression */
{ yylhsminor.yy358 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy348)); }
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 218: /* expression_list ::= expression_list NK_COMMA expression */
{ yylhsminor.yy358 = addNodeToList(pCxt, yymsp[-2].minor.yy358, releaseRawExprNode(pCxt, yymsp[0].minor.yy348)); }
  yymsp[-2].minor.yy358 = yylhsminor.yy358;
        break;
      case 219: /* column_reference ::= column_name */
{ yylhsminor.yy348 = createRawExprNode(pCxt, &yymsp[0].minor.yy269, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy269)); }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 220: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy269, createColumnNode(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy269)); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 221: /* predicate ::= expression compare_op expression */
      case 226: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==226);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy194, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348)));
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 222: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy348), releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348)));
                                                                                  }
  yymsp[-4].minor.yy348 = yylhsminor.yy348;
        break;
      case 223: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[-5].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348)));
                                                                                  }
  yymsp[-5].minor.yy348 = yylhsminor.yy348;
        break;
      case 224: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), NULL));
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 225: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy348), NULL));
                                                                                  }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 227: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy194 = OP_TYPE_LOWER_THAN; }
        break;
      case 228: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy194 = OP_TYPE_GREATER_THAN; }
        break;
      case 229: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy194 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 230: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy194 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 231: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy194 = OP_TYPE_NOT_EQUAL; }
        break;
      case 232: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy194 = OP_TYPE_EQUAL; }
        break;
      case 233: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy194 = OP_TYPE_LIKE; }
        break;
      case 234: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy194 = OP_TYPE_NOT_LIKE; }
        break;
      case 235: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy194 = OP_TYPE_MATCH; }
        break;
      case 236: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy194 = OP_TYPE_NMATCH; }
        break;
      case 237: /* in_op ::= IN */
{ yymsp[0].minor.yy194 = OP_TYPE_IN; }
        break;
      case 238: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy194 = OP_TYPE_NOT_IN; }
        break;
      case 239: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy358)); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 241: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy348), NULL));
                                                                                  }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 242: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348)));
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 243: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy348);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), releaseRawExprNode(pCxt, yymsp[0].minor.yy348)));
                                                                                  }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 248: /* from_clause ::= FROM table_reference_list */
      case 278: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==278);
      case 301: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==301);
{ yymsp[-1].minor.yy348 = yymsp[0].minor.yy348; }
        break;
      case 250: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy348 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy348, yymsp[0].minor.yy348, NULL); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 253: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy348 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy269, &yymsp[0].minor.yy269); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 254: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy348 = createRealTableNode(pCxt, &yymsp[-3].minor.yy269, &yymsp[-1].minor.yy269, &yymsp[0].minor.yy269); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 255: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy348 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy348), &yymsp[0].minor.yy269); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 257: /* alias_opt ::= */
{ yymsp[1].minor.yy269 = nil_token;  }
        break;
      case 258: /* alias_opt ::= table_alias */
{ yylhsminor.yy269 = yymsp[0].minor.yy269; }
  yymsp[0].minor.yy269 = yylhsminor.yy269;
        break;
      case 259: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy269 = yymsp[0].minor.yy269; }
        break;
      case 260: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 261: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==261);
{ yymsp[-2].minor.yy348 = yymsp[-1].minor.yy348; }
        break;
      case 262: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy348 = createJoinTableNode(pCxt, yymsp[-4].minor.yy184, yymsp[-5].minor.yy348, yymsp[-2].minor.yy348, yymsp[0].minor.yy348); }
  yymsp[-5].minor.yy348 = yylhsminor.yy348;
        break;
      case 263: /* join_type ::= */
{ yymsp[1].minor.yy184 = JOIN_TYPE_INNER; }
        break;
      case 264: /* join_type ::= INNER */
{ yymsp[0].minor.yy184 = JOIN_TYPE_INNER; }
        break;
      case 265: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy348 = createSelectStmt(pCxt, yymsp[-7].minor.yy345, yymsp[-6].minor.yy358, yymsp[-5].minor.yy348);
                                                                                    yymsp[-8].minor.yy348 = addWhereClause(pCxt, yymsp[-8].minor.yy348, yymsp[-4].minor.yy348);
                                                                                    yymsp[-8].minor.yy348 = addPartitionByClause(pCxt, yymsp[-8].minor.yy348, yymsp[-3].minor.yy358);
                                                                                    yymsp[-8].minor.yy348 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy348, yymsp[-2].minor.yy348);
                                                                                    yymsp[-8].minor.yy348 = addGroupByClause(pCxt, yymsp[-8].minor.yy348, yymsp[-1].minor.yy358);
                                                                                    yymsp[-8].minor.yy348 = addHavingClause(pCxt, yymsp[-8].minor.yy348, yymsp[0].minor.yy348);
                                                                                  }
        break;
      case 267: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy345 = true; }
        break;
      case 268: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy345 = false; }
        break;
      case 269: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy358 = NULL; }
        break;
      case 273: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy348);
                                                                                    yylhsminor.yy348 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy348), &t);
                                                                                  }
  yymsp[0].minor.yy348 = yylhsminor.yy348;
        break;
      case 274: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy348 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy348), &yymsp[0].minor.yy269); }
  yymsp[-1].minor.yy348 = yylhsminor.yy348;
        break;
      case 275: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy348 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), &yymsp[0].minor.yy269); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 276: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy348 = createColumnNode(pCxt, &yymsp[-2].minor.yy269, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 280: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 297: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==297);
      case 307: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==307);
{ yymsp[-2].minor.yy358 = yymsp[0].minor.yy358; }
        break;
      case 282: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy348 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy348), &yymsp[-1].minor.yy0); }
        break;
      case 283: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy348 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy348)); }
        break;
      case 284: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy348 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy348), NULL, yymsp[-1].minor.yy348, yymsp[0].minor.yy348); }
        break;
      case 285: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy348 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy348), releaseRawExprNode(pCxt, yymsp[-3].minor.yy348), yymsp[-1].minor.yy348, yymsp[0].minor.yy348); }
        break;
      case 287: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy348 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy348); }
        break;
      case 289: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy348 = createFillNode(pCxt, yymsp[-1].minor.yy256, NULL); }
        break;
      case 290: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy348 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy358)); }
        break;
      case 291: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy256 = FILL_MODE_NONE; }
        break;
      case 292: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy256 = FILL_MODE_PREV; }
        break;
      case 293: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy256 = FILL_MODE_NULL; }
        break;
      case 294: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy256 = FILL_MODE_LINEAR; }
        break;
      case 295: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy256 = FILL_MODE_NEXT; }
        break;
      case 298: /* group_by_list ::= expression */
{ yylhsminor.yy358 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); }
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 299: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy358 = addNodeToList(pCxt, yymsp[-2].minor.yy358, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy348))); }
  yymsp[-2].minor.yy358 = yylhsminor.yy358;
        break;
      case 302: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy348 = addOrderByClause(pCxt, yymsp[-3].minor.yy348, yymsp[-2].minor.yy358);
                                                                                    yylhsminor.yy348 = addSlimitClause(pCxt, yylhsminor.yy348, yymsp[-1].minor.yy348);
                                                                                    yylhsminor.yy348 = addLimitClause(pCxt, yylhsminor.yy348, yymsp[0].minor.yy348);
                                                                                  }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 304: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy348 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy348, yymsp[0].minor.yy348); }
  yymsp[-3].minor.yy348 = yylhsminor.yy348;
        break;
      case 309: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 313: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==313);
{ yymsp[-1].minor.yy348 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 310: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 314: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==314);
{ yymsp[-3].minor.yy348 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 311: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 315: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==315);
{ yymsp[-3].minor.yy348 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 316: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy348 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy348); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 320: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy348 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy348), yymsp[-1].minor.yy368, yymsp[0].minor.yy339); }
  yymsp[-2].minor.yy348 = yylhsminor.yy348;
        break;
      case 321: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy368 = ORDER_ASC; }
        break;
      case 322: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy368 = ORDER_ASC; }
        break;
      case 323: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy368 = ORDER_DESC; }
        break;
      case 324: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy339 = NULL_ORDER_DEFAULT; }
        break;
      case 325: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy339 = NULL_ORDER_FIRST; }
        break;
      case 326: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy339 = NULL_ORDER_LAST; }
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
