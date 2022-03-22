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
#define YYCODETYPE unsigned short int
#define YYNOCODE 256
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SNodeList* yy24;
  int32_t yy68;
  ENullOrder yy73;
  EJoinType yy84;
  EOrder yy130;
  EOperatorType yy252;
  SAlterOption yy285;
  bool yy345;
  EFillMode yy358;
  SNode* yy360;
  SDataType yy400;
  SToken yy417;
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
#define YYNSTATE             424
#define YYNRULE              326
#define YYNTOKEN             163
#define YY_MAX_SHIFT         423
#define YY_MIN_SHIFTREDUCE   645
#define YY_MAX_SHIFTREDUCE   970
#define YY_ERROR_ACTION      971
#define YY_ACCEPT_ACTION     972
#define YY_NO_ACTION         973
#define YY_MIN_REDUCE        974
#define YY_MAX_REDUCE        1299
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
#define YY_ACTTAB_COUNT (1215)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */  1017,  357,  357,  105, 1192,  986,  263, 1075, 1278,  109,
 /*    10 */    42,  341, 1067,   31,   29,   27,   26,   25,   20,   89,
 /*    20 */  1116, 1277, 1078, 1078,  356, 1276,  325, 1073,   31,   29,
 /*    30 */    27,   26,   25, 1063,  314,   77,  912,  356,   76,   75,
 /*    40 */    74,   73,   72,   71,   70,   69,   68,   92,  207,  408,
 /*    50 */   407,  406,  405,  404,  403,  402,  401,  400,  399,  398,
 /*    60 */   397,  396,  395,  394,  393,  392,  391,  390,  975,   24,
 /*    70 */   167,   90,  853,   31,   29,   27,   26,   25,  302,  839,
 /*    80 */   876,  114, 1224, 1225, 1192, 1229,  356,   10,  264,   77,
 /*    90 */    98,  341,   76,   75,   74,   73,   72,   71,   70,   69,
 /*   100 */    68,  244,   31,   29,   27,   26,   25,  285,  207,  280,
 /*   110 */   106, 1045,  284, 1123,  318,  283,  877,  281,  357,  220,
 /*   120 */   282,  416,  415,  354, 1121,   23,  228,  205,  871,  872,
 /*   130 */   873,  874,  875,  879,  880,  881,   10, 1236,  908, 1078,
 /*   140 */   876,  755,  379,  378,  377,  759,  376,  761,  762,  375,
 /*   150 */   764,  372,  423,  770,  369,  772,  773,  366,  363,  104,
 /*   160 */  1177,   31,   29,   27,   26,   25,  183, 1081,  832,   88,
 /*   170 */   911,  239,   57,   30,   28,  412,  877,  182, 1143, 1145,
 /*   180 */  1192,  230,   53,  832,  830,   23,  228,  341,  871,  872,
 /*   190 */   873,  874,  875,  879,  880,  881,  118,  343,  843,  830,
 /*   200 */   277, 1164,   59,  118,  276,  178,  329, 1171,   12,   60,
 /*   210 */  1178, 1181, 1217,  252,  831, 1177,  206, 1213,  344, 1169,
 /*   220 */   190,  233,  234,  131, 1152,  192,  111,  278, 1278,  831,
 /*   230 */   104,    1,  269,  841,  130, 1192,  353,  191, 1080,  357,
 /*   240 */   420,  117,  328,  185,   65, 1276, 1108,  123,  966,  967,
 /*   250 */   305,  273,  343,  146,  313,  420, 1164,  122,  121,   43,
 /*   260 */  1078,  118,  128, 1069,   61, 1178, 1181, 1217,  833,  836,
 /*   270 */  1166,  223, 1213,  112,  238, 1177, 1166,  320,  315,   30,
 /*   280 */    28,  913,  104,  833,  836,  163,   58,  230,  389,  832,
 /*   290 */  1080,  306, 1244, 1177,  221, 1192,   93,  997,  118,  162,
 /*   300 */   236, 1056,  328, 1070, 1164,  830,  127,   63,  325,  878,
 /*   310 */   125, 1164,  343, 1192,   12,  319, 1164, 1164,   21,   42,
 /*   320 */   341, 1177, 1054,   51,   61, 1178, 1181, 1217,  882,   92,
 /*   330 */   343,  223, 1213,  112, 1164,  831, 1074,    1, 1164,  118,
 /*   340 */  1071, 1192,   61, 1178, 1181, 1217,  333,  264,  341,  223,
 /*   350 */  1213, 1290, 1245,   90, 1177,  682,  136,  681,  343,  134,
 /*   360 */  1251,  420, 1164,  115, 1224, 1225,  996, 1229,  389, 1231,
 /*   370 */    61, 1178, 1181, 1217, 1192,  683,  937,  223, 1213, 1290,
 /*   380 */   842,  341, 1177, 1123,  840,  241, 1228,  357, 1274,  833,
 /*   390 */   836,  343,  355,  104, 1144, 1164,  310,  935,  936,  938,
 /*   400 */   939, 1080, 1192,   61, 1178, 1181, 1217, 1164, 1078,  341,
 /*   410 */   223, 1213, 1290,  299,    9,    8,   30,   28,  334,  343,
 /*   420 */  1177, 1235,  382, 1164,  230, 1055,  832, 1231,  329,   30,
 /*   430 */    28,  195, 1178, 1181, 1053, 1123,  995,  230,  357,  832,
 /*   440 */  1192,  235,  830,  180, 1227,  325, 1121,  341,  844, 1065,
 /*   450 */  1278,   12,  920,  300, 1123,  830, 1177,  343,  841, 1078,
 /*   460 */   240, 1164, 1061,  117,  229, 1121,   92, 1276,  357,  201,
 /*   470 */  1178, 1181,  831,   65,    1, 1278, 1192, 1164,  994,  993,
 /*   480 */   279,  381,  386,  341,  889,  831,  385,    7,  117, 1078,
 /*   490 */    90,  386, 1276,  343, 1177,  385,  987, 1164,  420,  327,
 /*   500 */   113, 1224, 1225, 1140, 1229,   62, 1178, 1181, 1217,  387,
 /*   510 */   120,  420, 1216, 1213, 1192,  325, 1231,  311,  387, 1164,
 /*   520 */  1164,  341,   27,   26,   25, 1046,  833,  836,  384,  383,
 /*   530 */  1123,  343,  992, 1226,  991, 1164,   92,  384,  383,  833,
 /*   540 */   836, 1122,  344,   62, 1178, 1181, 1217,  357, 1153,  149,
 /*   550 */   339, 1213,  242,  164,  118,  329,   30,   28,  342,  990,
 /*   560 */    90,    9,    8,  989,  230,  988,  832, 1167, 1078,  985,
 /*   570 */   160, 1224,  324, 1164,  323, 1164, 1177, 1278,  984,   30,
 /*   580 */    28,  681,  830,   30,   28,  332, 1117,  230,  983,  832,
 /*   590 */   117,  230,  839,  832, 1276, 1177, 1192,  271, 1013,  245,
 /*   600 */  1164,  982,  257,  341, 1164,  830, 1164,  981, 1164,  830,
 /*   610 */  1164,  258,  831,  343,    7, 1192,  934, 1164,  980, 1164,
 /*   620 */   286,    6,  341,  979,  157,  107, 1178, 1181,   78, 1164,
 /*   630 */   138,  214,  343,  137, 1008,  831, 1164,    7,  420,  831,
 /*   640 */   140,    1, 1164,  139,   62, 1178, 1181, 1217, 1164,  340,
 /*   650 */  1177,  978, 1214,  277, 1006,  977,  288,  276,  336, 1164,
 /*   660 */   297,  420,  330, 1291, 1164,  420,  833,  836,  972,  215,
 /*   670 */  1192,  213,  212,  295,  275, 1177,  291,  341,  256,  974,
 /*   680 */   278,  251,  250,  249,  248,  247,  154,  343,  270,  833,
 /*   690 */   836, 1164, 1164,  833,  836, 1192, 1164, 1247,  152,  200,
 /*   700 */  1178, 1181,  341,   87,   86,   85,   84,   83,   82,   81,
 /*   710 */    80,   79,  343, 1177,  908,  142, 1164,  243,  141,  307,
 /*   720 */   326, 1193,  969,  970,  201, 1178, 1181,  166,  868,  883,
 /*   730 */   321, 1177,  337, 1192,    2,  331,  850,  825,  839, 1278,
 /*   740 */   341,   32, 1142,  172,  119,  253, 1177,  349,   32,   32,
 /*   750 */   343, 1192,  117,  177, 1164,  170, 1276, 1177,  341,   95,
 /*   760 */   254,  255,  107, 1178, 1181,   96, 1192,  748,  343,  246,
 /*   770 */   847,  259, 1164,  341,  743,  227,  776, 1192,  124,   98,
 /*   780 */   201, 1178, 1181,  343,  341,  260,   78, 1164,  361, 1177,
 /*   790 */   231,  780, 1177,  261,  343,  201, 1178, 1181, 1164,  846,
 /*   800 */  1292,  786,  785,   96,  265,  262,  199, 1178, 1181, 1192,
 /*   810 */    22,   41, 1192,   97,   98,   99,  341, 1177,  129,  341,
 /*   820 */    31,   29,   27,   26,   25,  845,  343,   96, 1177,  343,
 /*   830 */  1164,  272,  274, 1164, 1068,  133, 1064, 1192,  202, 1178,
 /*   840 */  1181,  193, 1178, 1181,  341,  135,  100,  101, 1192,   67,
 /*   850 */  1066,  219,  145, 1062,  343,  341, 1177,  102, 1164,  103,
 /*   860 */   304, 1177,  303,  301,  844,  343,  203, 1178, 1181, 1164,
 /*   870 */   312, 1248, 1177, 1258,  836,  309, 1192,  194, 1178, 1181,
 /*   880 */   347, 1192,  150,  341, 1257,    5,  153,  222,  341,  322,
 /*   890 */   908,   91, 1192,  343, 1238,  156,  308, 1164,  343,  341,
 /*   900 */  1177,    4, 1164,  843, 1232,  204, 1178, 1181,  110,  343,
 /*   910 */  1189, 1178, 1181, 1164,   33,  224,  338,  159, 1177,  335,
 /*   920 */  1192, 1188, 1178, 1181,  158,   17,  345,  341, 1199, 1151,
 /*   930 */  1293,  346,  350, 1177, 1275,  165, 1150,  343, 1192,  232,
 /*   940 */   351, 1164,  174,  352, 1177,  341,  184,   50,   52, 1187,
 /*   950 */  1178, 1181, 1079, 1192,  186,  343,  181,  359,  419, 1164,
 /*   960 */   341,  196,  188,  197, 1192,  189, 1158,  210, 1178, 1181,
 /*   970 */   343,  341,  290,  808, 1164, 1135, 1177, 1134,   94, 1177,
 /*   980 */  1133,  343,  209, 1178, 1181, 1164, 1132,  298, 1131, 1130,
 /*   990 */  1129, 1128,  810,  211, 1178, 1181, 1192, 1020, 1127, 1192,
 /*  1000 */  1126,  144, 1125,  341,  293, 1124,  341, 1019, 1157,  287,
 /*  1010 */  1148, 1057,  143,  343,  126,  694,  343, 1164, 1018, 1016,
 /*  1020 */  1164,  266,  268,  267, 1005,  208, 1178, 1181,  198, 1178,
 /*  1030 */  1181, 1004, 1001, 1059,   66,  132,  789,   38, 1058,  791,
 /*  1040 */    37,   31,   29,   27,   26,   25,  285,  790,  280, 1014,
 /*  1050 */   723,  284,  216,  722,  283,  721,  281, 1009,  720,  282,
 /*  1060 */   719,  718,  217, 1007,  289,  218,  292, 1000,  294,  999,
 /*  1070 */   296,   64, 1156, 1155,   36, 1147,   44,  147,  148,    3,
 /*  1080 */    14,   15,   32,  316,  151,   34,   39,  933,   11,   47,
 /*  1090 */     8,  108,  348, 1146,  955,  869,  155,  954,  927,  225,
 /*  1100 */   853,  176,  959,  958,  226,   45,  973,  926,  973,  973,
 /*  1110 */   973,   46,  973,  175,  973,  905,  973,  973,  904,  973,
 /*  1120 */   317, 1169,  973,  973,  960,   19,  973,  360,  237,  973,
 /*  1130 */   364,  851,  161,  169,  754,   35,  116,  367,   16,  931,
 /*  1140 */   168,   13,  370,  373,   18,  973,  171,  173,   48,   49,
 /*  1150 */   782, 1015, 1003,   40, 1002,  714,  706,  777,  774,  784,
 /*  1160 */    53,  362,  973, 1168,  179,  358,  365,  783,  771,  765,
 /*  1170 */   368,  371,  388,  763,  692,  713,  374,  712,  711,  710,
 /*  1180 */   709,  708,  707,  705,  410,  704,   54,  703,  702,   55,
 /*  1190 */   769,  768,   56,  998,  701,  700,  699,  698,  380,  767,
 /*  1200 */   715,  417,  697,  409,  766,  411,  413,  414,  418,  973,
 /*  1210 */   834,  187,  421,  973,  422,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     0,  172,  172,  165,  186,  167,  177,  177,  234,  185,
 /*    10 */   174,  193,  187,   12,   13,   14,   15,   16,    2,  183,
 /*    20 */   196,  247,  193,  193,   20,  251,  172,  191,   12,   13,
 /*    30 */    14,   15,   16,  187,  216,   21,    4,   20,   24,   25,
 /*    40 */    26,   27,   28,   29,   30,   31,   32,  193,   47,   49,
 /*    50 */    50,   51,   52,   53,   54,   55,   56,   57,   58,   59,
 /*    60 */    60,   61,   62,   63,   64,   65,   66,   67,    0,  219,
 /*    70 */   220,  217,   71,   12,   13,   14,   15,   16,   71,   20,
 /*    80 */    79,  227,  228,  229,  186,  231,   20,   70,   46,   21,
 /*    90 */    83,  193,   24,   25,   26,   27,   28,   29,   30,   31,
 /*   100 */    32,  172,   12,   13,   14,   15,   16,   49,   47,   51,
 /*   110 */   175,  176,   54,  186,  216,   57,  115,   59,  172,  192,
 /*   120 */    62,  169,  170,  177,  197,  124,  125,  198,  127,  128,
 /*   130 */   129,  130,  131,  132,  133,  134,   70,  135,  136,  193,
 /*   140 */    79,   85,   86,   87,   88,   89,   90,   91,   92,   93,
 /*   150 */    94,   95,   19,   97,   98,   99,  100,  101,  102,  186,
 /*   160 */   166,   12,   13,   14,   15,   16,   33,  194,   22,   36,
 /*   170 */   138,  195,   70,   12,   13,   42,  115,   44,  202,  203,
 /*   180 */   186,   20,   80,   22,   38,  124,  125,  193,  127,  128,
 /*   190 */   129,  130,  131,  132,  133,  134,  137,  203,   20,   38,
 /*   200 */    57,  207,   69,  137,   61,   72,  212,   70,   47,  215,
 /*   210 */   216,  217,  218,   63,   68,  166,  222,  223,  203,   82,
 /*   220 */    18,  206,  178,   33,  209,   23,   36,   84,  234,   68,
 /*   230 */   186,   70,   42,   20,   44,  186,  103,   35,  194,  172,
 /*   240 */    94,  247,  193,  179,  177,  251,  182,   45,  158,  159,
 /*   250 */   117,  184,  203,  120,  119,   94,  207,  107,  108,   69,
 /*   260 */   193,  137,   72,  166,  215,  216,  217,  218,  122,  123,
 /*   270 */   166,  222,  223,  224,  178,  166,  166,  142,  143,   12,
 /*   280 */    13,   14,  186,  122,  123,  236,  171,   20,   46,   22,
 /*   290 */   194,  242,  243,  166,  190,  186,  181,  166,  137,  121,
 /*   300 */   190,    0,  193,  188,  207,   38,  116,  105,  172,  115,
 /*   310 */   120,  207,  203,  186,   47,   20,  207,  207,  124,  174,
 /*   320 */   193,  166,    0,  171,  215,  216,  217,  218,  134,  193,
 /*   330 */   203,  222,  223,  224,  207,   68,  191,   70,  207,  137,
 /*   340 */   188,  186,  215,  216,  217,  218,   83,   46,  193,  222,
 /*   350 */   223,  224,  243,  217,  166,   20,   74,   22,  203,   77,
 /*   360 */   233,   94,  207,  227,  228,  229,  166,  231,   46,  213,
 /*   370 */   215,  216,  217,  218,  186,   40,  126,  222,  223,  224,
 /*   380 */    20,  193,  166,  186,   20,  178,  230,  172,  233,  122,
 /*   390 */   123,  203,  177,  186,  197,  207,  146,  147,  148,  149,
 /*   400 */   150,  194,  186,  215,  216,  217,  218,  207,  193,  193,
 /*   410 */   222,  223,  224,  172,    1,    2,   12,   13,  155,  203,
 /*   420 */   166,  233,   81,  207,   20,    0,   22,  213,  212,   12,
 /*   430 */    13,  215,  216,  217,    0,  186,  166,   20,  172,   22,
 /*   440 */   186,  192,   38,  177,  230,  172,  197,  193,   20,  187,
 /*   450 */   234,   47,   14,  212,  186,   38,  166,  203,   20,  193,
 /*   460 */   192,  207,  187,  247,  210,  197,  193,  251,  172,  215,
 /*   470 */   216,  217,   68,  177,   70,  234,  186,  207,  166,  166,
 /*   480 */   184,  187,   57,  193,   71,   68,   61,   70,  247,  193,
 /*   490 */   217,   57,  251,  203,  166,   61,  167,  207,   94,  226,
 /*   500 */   227,  228,  229,  193,  231,  215,  216,  217,  218,   84,
 /*   510 */   200,   94,  222,  223,  186,  172,  213,  245,   84,  207,
 /*   520 */   207,  193,   14,   15,   16,  176,  122,  123,  103,  104,
 /*   530 */   186,  203,  166,  230,  166,  207,  193,  103,  104,  122,
 /*   540 */   123,  197,  203,  215,  216,  217,  218,  172,  209,  121,
 /*   550 */   222,  223,  177,  254,  137,  212,   12,   13,   14,  166,
 /*   560 */   217,    1,    2,  166,   20,  166,   22,  166,  193,  166,
 /*   570 */   227,  228,  229,  207,  231,  207,  166,  234,  166,   12,
 /*   580 */    13,   22,   38,   12,   13,    3,  196,   20,  166,   22,
 /*   590 */   247,   20,   20,   22,  251,  166,  186,   38,    0,   27,
 /*   600 */   207,  166,   30,  193,  207,   38,  207,  166,  207,   38,
 /*   610 */   207,   39,   68,  203,   70,  186,   71,  207,  166,  207,
 /*   620 */    22,   43,  193,  166,  239,  215,  216,  217,   83,  207,
 /*   630 */    74,   35,  203,   77,    0,   68,  207,   70,   94,   68,
 /*   640 */    74,   70,  207,   77,  215,  216,  217,  218,  207,   47,
 /*   650 */   166,  166,  223,   57,    0,  166,   22,   61,   83,  207,
 /*   660 */    21,   94,  252,  253,  207,   94,  122,  123,  163,   73,
 /*   670 */   186,   75,   76,   34,   78,  166,   22,  193,  106,    0,
 /*   680 */    84,  109,  110,  111,  112,  113,   71,  203,  169,  122,
 /*   690 */   123,  207,  207,  122,  123,  186,  207,  214,   83,  215,
 /*   700 */   216,  217,  193,   24,   25,   26,   27,   28,   29,   30,
 /*   710 */    31,   32,  203,  166,  136,   74,  207,  212,   77,  210,
 /*   720 */   232,  186,  161,  162,  215,  216,  217,  248,  126,   71,
 /*   730 */   246,  166,  157,  186,  235,  153,   71,   71,   20,  234,
 /*   740 */   193,   83,  172,   71,  114,  199,  166,   71,   83,   83,
 /*   750 */   203,  186,  247,   71,  207,   83,  251,  166,  193,   83,
 /*   760 */   115,  199,  215,  216,  217,   83,  186,   71,  203,  201,
 /*   770 */    20,  172,  207,  193,   71,  210,   71,  186,  174,   83,
 /*   780 */   215,  216,  217,  203,  193,  211,   83,  207,   83,  166,
 /*   790 */   210,   71,  166,  193,  203,  215,  216,  217,  207,   20,
 /*   800 */   253,   71,   71,   83,  172,  204,  215,  216,  217,  186,
 /*   810 */     2,  174,  186,   83,   83,   71,  193,  166,  174,  193,
 /*   820 */    12,   13,   14,   15,   16,   20,  203,   83,  166,  203,
 /*   830 */   207,  168,  186,  207,  186,  186,  186,  186,  215,  216,
 /*   840 */   217,  215,  216,  217,  193,  186,  186,  186,  186,  172,
 /*   850 */   186,  168,  171,  186,  203,  193,  166,  186,  207,  186,
 /*   860 */   204,  166,  193,  211,   20,  203,  215,  216,  217,  207,
 /*   870 */   145,  214,  166,  244,  123,  207,  186,  215,  216,  217,
 /*   880 */   144,  186,  208,  193,  244,  152,  208,  207,  193,  151,
 /*   890 */   136,  193,  186,  203,  241,  240,  140,  207,  203,  193,
 /*   900 */   166,  139,  207,   20,  213,  215,  216,  217,  238,  203,
 /*   910 */   215,  216,  217,  207,  114,  160,  156,  225,  166,  154,
 /*   920 */   186,  215,  216,  217,  237,   70,  207,  193,  221,  208,
 /*   930 */   255,  207,  118,  166,  250,  249,  208,  203,  186,  207,
 /*   940 */   205,  207,  193,  204,  166,  193,  182,  171,   70,  215,
 /*   950 */   216,  217,  193,  186,  172,  203,  171,  189,  168,  207,
 /*   960 */   193,  180,  173,  180,  186,  164,    0,  215,  216,  217,
 /*   970 */   203,  193,    4,   82,  207,    0,  166,    0,  114,  166,
 /*   980 */     0,  203,  215,  216,  217,  207,    0,   19,    0,    0,
 /*   990 */     0,    0,   22,  215,  216,  217,  186,    0,    0,  186,
 /*  1000 */     0,   33,    0,  193,   36,    0,  193,    0,    0,   41,
 /*  1010 */     0,    0,   44,  203,   43,   48,  203,  207,    0,    0,
 /*  1020 */   207,   38,   43,   36,    0,  215,  216,  217,  215,  216,
 /*  1030 */   217,    0,    0,    0,   79,   77,   22,   69,    0,   38,
 /*  1040 */    72,   12,   13,   14,   15,   16,   49,   38,   51,    0,
 /*  1050 */    38,   54,   22,   38,   57,   38,   59,    0,   38,   62,
 /*  1060 */    38,   38,   22,    0,   39,   22,   38,    0,   22,    0,
 /*  1070 */    22,   20,    0,    0,  121,    0,   70,   43,  116,   83,
 /*  1080 */   141,  141,   83,   38,   71,  135,   83,   71,  141,    4,
 /*  1090 */     2,   70,  119,    0,   38,  126,   70,   38,   71,   38,
 /*  1100 */    71,  116,   38,   38,   38,   70,  256,   71,  256,  256,
 /*  1110 */   256,   70,  256,   43,  256,   71,  256,  256,   71,  256,
 /*  1120 */    83,   82,  256,  256,   71,   83,  256,   38,   38,  256,
 /*  1130 */    38,   71,   82,   71,   22,   83,   82,   38,   83,   71,
 /*  1140 */    82,   70,   38,   38,   70,  256,   70,   70,   70,   70,
 /*  1150 */    22,    0,    0,   70,    0,   22,   22,   71,   71,   38,
 /*  1160 */    80,   70,  256,   82,   82,   81,   70,   38,   71,   71,
 /*  1170 */    70,   70,   47,   71,   48,   38,   70,   38,   38,   38,
 /*  1180 */    38,   38,   38,   38,   36,   38,   70,   38,   38,   70,
 /*  1190 */    96,   96,   70,    0,   38,   38,   38,   38,   84,   96,
 /*  1200 */    68,   22,   38,   38,   96,   43,   38,   37,   21,  256,
 /*  1210 */    22,   22,   21,  256,   20,  256,  256,  256,  256,  256,
 /*  1220 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1230 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1240 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1250 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1260 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1270 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1280 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1290 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1300 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1310 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1320 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1330 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1340 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1350 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1360 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*  1370 */   256,  256,  256,  256,  256,  256,  256,  256,
};
#define YY_SHIFT_COUNT    (423)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (1194)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   202,  161,  267,  404,  404,  404,  404,  417,  404,  404,
 /*    10 */    66,  567,  571,  544,  567,  567,  567,  567,  567,  567,
 /*    20 */   567,  567,  567,  567,  567,  567,  567,  567,  567,  567,
 /*    30 */   567,  567,  567,   17,   17,   17,   59,    4,    4,  146,
 /*    40 */   146,    4,    4,   42,  213,  295,  295,  124,  360,  213,
 /*    50 */     4,    4,  213,    4,  213,  360,  213,  213,    4,  242,
 /*    60 */     1,   61,   61,  572,   14,  596,  146,   58,  146,  146,
 /*    70 */   146,  146,  146,  146,  146,  146,  146,  146,  146,  146,
 /*    80 */   146,  146,  146,  146,  146,  146,  146,  146,  335,  301,
 /*    90 */   178,  178,  178,  322,  364,  360,  213,  213,  213,  341,
 /*   100 */    56,   56,   56,   56,   56,   68,  997,   90,  250,  143,
 /*   110 */   135,  559,  428,    2,  578,    2,  438,  582,   32,  718,
 /*   120 */   630,  645,  645,  718,  750,   42,  364,  779,   42,  718,
 /*   130 */    42,  805,  213,  213,  213,  213,  213,  213,  213,  213,
 /*   140 */   213,  213,  213,  718,  805,  750,  242,  364,  779,  844,
 /*   150 */   725,  736,  751,  725,  736,  751,  733,  738,  756,  762,
 /*   160 */   754,  364,  883,  800,  755,  760,  765,  855,  213,  736,
 /*   170 */   751,  751,  736,  751,  814,  364,  779,  341,  242,  364,
 /*   180 */   878,  718,  242,  805, 1215, 1215, 1215, 1215,    0,  679,
 /*   190 */   133,  190,  968,   16,  808, 1029,  425,  434,  149,  149,
 /*   200 */   149,  149,  149,  149,  149,  150,  413,  194,  508,  508,
 /*   210 */   508,  508,  282,  556,  566,  641,  598,  634,  654,  639,
 /*   220 */     7,  545,  615,  560,  561,  263,  575,  658,  602,  665,
 /*   230 */   137,  666,  672,  676,  682,  696,  703,  705,  720,  730,
 /*   240 */   731,  744,  102,  966,  891,  975,  977,  864,  980,  986,
 /*   250 */   988,  989,  990,  991,  970,  998, 1000, 1002, 1005, 1007,
 /*   260 */  1008, 1010,  971, 1011,  967, 1018, 1019,  983,  987,  979,
 /*   270 */  1024, 1031, 1032, 1033,  955,  958, 1001, 1009, 1014, 1038,
 /*   280 */  1012, 1015, 1017, 1020, 1022, 1023, 1049, 1030, 1057, 1040,
 /*   290 */  1025, 1063, 1043, 1028, 1067, 1046, 1069, 1048, 1051, 1072,
 /*   300 */  1073,  953, 1075, 1006, 1034,  962,  996,  999,  939, 1013,
 /*   310 */  1003, 1016, 1021, 1026, 1027, 1035, 1036, 1045, 1037, 1039,
 /*   320 */  1041, 1042,  940, 1044, 1047, 1050,  950, 1052, 1054, 1053,
 /*   330 */  1055,  947, 1085, 1056, 1059, 1061, 1064, 1065, 1066, 1088,
 /*   340 */   969, 1058, 1060, 1071, 1074, 1062, 1068, 1076, 1077,  973,
 /*   350 */  1078, 1093, 1070,  985, 1079, 1080, 1081, 1082, 1083, 1084,
 /*   360 */  1086, 1089, 1090, 1091, 1087, 1092, 1096, 1097, 1099, 1100,
 /*   370 */  1098, 1104, 1101, 1102, 1105, 1106, 1094, 1095, 1103, 1108,
 /*   380 */  1112, 1114, 1116, 1119, 1122, 1121, 1129, 1128, 1126, 1125,
 /*   390 */  1132, 1133, 1137, 1139, 1140, 1141, 1142, 1143, 1144, 1134,
 /*   400 */  1145, 1147, 1149, 1150, 1156, 1157, 1158, 1159, 1164, 1151,
 /*   410 */  1165, 1148, 1162, 1152, 1168, 1170, 1154, 1193, 1179, 1187,
 /*   420 */  1188, 1189, 1191, 1194,
};
#define YY_REDUCE_COUNT (187)
#define YY_REDUCE_MIN   (-226)
#define YY_REDUCE_MAX   (813)
static const short yy_reduce_ofst[] = {
 /*     0 */   505,   -6,   49,  109,  127,  155,  188,  216,  290,  328,
 /*    10 */   343,  410,  429,  254,  509,  484,  547,  565,  580,  591,
 /*    20 */   623,  626,  651,  662,  690,  695,  706,  734,  752,  767,
 /*    30 */   778,  810,  813,  273, -146,  136,  241,   67,  296,  104,
 /*    40 */   110, -171, -170, -164,  -73, -182, -102, -226,   15,   44,
 /*    50 */   -54,  215,  249,  266,   96,  -24,  268,  207,  375,  115,
 /*    60 */  -150, -150, -150,  -71, -162, -176,   97,  -65,  131,  200,
 /*    70 */   270,  312,  313,  366,  368,  393,  397,  399,  401,  403,
 /*    80 */   412,  422,  435,  441,  452,  457,  485,  489,  -48,  145,
 /*    90 */   156,  214,  303,  152,  310,  339,  -27,  197,  344,   64,
 /*   100 */  -175, -154,  262,  275,  294,  329,  349,  299,  272,  390,
 /*   110 */   385,  519,  483,  488,  488,  488,  535,  479,  499,  570,
 /*   120 */   568,  546,  562,  599,  574,  604,  600,  601,  637,  632,
 /*   130 */   644,  663,  646,  648,  649,  650,  659,  660,  661,  664,
 /*   140 */   667,  671,  673,  677,  683,  652,  681,  669,  656,  657,
 /*   150 */   629,  674,  668,  640,  678,  680,  653,  655,  670,  687,
 /*   160 */   488,  698,  691,  692,  675,  684,  686,  707,  535,  721,
 /*   170 */   719,  724,  728,  732,  735,  749,  739,  764,  776,  759,
 /*   180 */   768,  782,  785,  790,  781,  783,  789,  801,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*    10 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*    20 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*    30 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*    40 */   971,  971,  971, 1024,  971,  971,  971,  971,  971,  971,
 /*    50 */   971,  971,  971,  971,  971,  971,  971,  971,  971, 1022,
 /*    60 */   971, 1219,  971, 1136,  971,  971,  971,  971,  971,  971,
 /*    70 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*    80 */   971,  971,  971,  971,  971,  971,  971,  971,  971, 1024,
 /*    90 */  1230, 1230, 1230, 1022,  971,  971,  971,  971,  971, 1107,
 /*   100 */   971,  971,  971,  971,  971,  971,  971, 1294,  971, 1060,
 /*   110 */  1254,  971, 1246, 1222, 1236, 1223,  971, 1279, 1239,  971,
 /*   120 */  1141, 1138, 1138,  971,  971, 1024,  971,  971, 1024,  971,
 /*   130 */  1024,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   140 */   971,  971,  971,  971,  971,  971, 1022,  971,  971,  971,
 /*   150 */  1261, 1259,  971, 1261, 1259,  971, 1273, 1269, 1252, 1250,
 /*   160 */  1236,  971,  971,  971, 1297, 1285, 1281,  971,  971, 1259,
 /*   170 */   971,  971, 1259,  971, 1149,  971,  971,  971, 1022,  971,
 /*   180 */  1076,  971, 1022,  971, 1110, 1110, 1025,  976,  971,  971,
 /*   190 */   971,  971,  971,  971,  971,  971,  971,  971, 1191, 1272,
 /*   200 */  1271, 1190, 1196, 1195, 1194,  971,  971,  971, 1185, 1186,
 /*   210 */  1184, 1183,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   220 */   971,  971,  971, 1220,  971, 1282, 1286,  971,  971,  971,
 /*   230 */  1170,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   240 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   250 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   260 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   270 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   280 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   290 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   300 */   971,  971,  971,  971,  971,  971, 1243, 1253,  971,  971,
 /*   310 */   971,  971,  971,  971,  971,  971,  971,  971,  971, 1170,
 /*   320 */   971, 1270,  971, 1229, 1225,  971,  971, 1221,  971,  971,
 /*   330 */  1280,  971,  971,  971,  971,  971,  971,  971,  971, 1215,
 /*   340 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   350 */   971,  971,  971,  971,  971,  971, 1169,  971,  971,  971,
 /*   360 */   971,  971,  971, 1104,  971,  971,  971,  971,  971,  971,
 /*   370 */   971,  971,  971,  971,  971,  971, 1089, 1087, 1086, 1085,
 /*   380 */   971, 1082,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   390 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   400 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   410 */   971,  971,  971,  971,  971,  971,  971,  971,  971,  971,
 /*   420 */   971,  971,  971,  971,
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
  /*   36 */ "DNODE",
  /*   37 */ "PORT",
  /*   38 */ "NK_INTEGER",
  /*   39 */ "DNODES",
  /*   40 */ "NK_IPTOKEN",
  /*   41 */ "LOCAL",
  /*   42 */ "QNODE",
  /*   43 */ "ON",
  /*   44 */ "DATABASE",
  /*   45 */ "USE",
  /*   46 */ "IF",
  /*   47 */ "NOT",
  /*   48 */ "EXISTS",
  /*   49 */ "BLOCKS",
  /*   50 */ "CACHE",
  /*   51 */ "CACHELAST",
  /*   52 */ "COMP",
  /*   53 */ "DAYS",
  /*   54 */ "FSYNC",
  /*   55 */ "MAXROWS",
  /*   56 */ "MINROWS",
  /*   57 */ "KEEP",
  /*   58 */ "PRECISION",
  /*   59 */ "QUORUM",
  /*   60 */ "REPLICA",
  /*   61 */ "TTL",
  /*   62 */ "WAL",
  /*   63 */ "VGROUPS",
  /*   64 */ "SINGLE_STABLE",
  /*   65 */ "STREAM_MODE",
  /*   66 */ "RETENTIONS",
  /*   67 */ "FILE_FACTOR",
  /*   68 */ "NK_FLOAT",
  /*   69 */ "TABLE",
  /*   70 */ "NK_LP",
  /*   71 */ "NK_RP",
  /*   72 */ "STABLE",
  /*   73 */ "ADD",
  /*   74 */ "COLUMN",
  /*   75 */ "MODIFY",
  /*   76 */ "RENAME",
  /*   77 */ "TAG",
  /*   78 */ "SET",
  /*   79 */ "NK_EQ",
  /*   80 */ "USING",
  /*   81 */ "TAGS",
  /*   82 */ "NK_DOT",
  /*   83 */ "NK_COMMA",
  /*   84 */ "COMMENT",
  /*   85 */ "BOOL",
  /*   86 */ "TINYINT",
  /*   87 */ "SMALLINT",
  /*   88 */ "INT",
  /*   89 */ "INTEGER",
  /*   90 */ "BIGINT",
  /*   91 */ "FLOAT",
  /*   92 */ "DOUBLE",
  /*   93 */ "BINARY",
  /*   94 */ "TIMESTAMP",
  /*   95 */ "NCHAR",
  /*   96 */ "UNSIGNED",
  /*   97 */ "JSON",
  /*   98 */ "VARCHAR",
  /*   99 */ "MEDIUMBLOB",
  /*  100 */ "BLOB",
  /*  101 */ "VARBINARY",
  /*  102 */ "DECIMAL",
  /*  103 */ "SMA",
  /*  104 */ "ROLLUP",
  /*  105 */ "SHOW",
  /*  106 */ "DATABASES",
  /*  107 */ "TABLES",
  /*  108 */ "STABLES",
  /*  109 */ "MNODES",
  /*  110 */ "MODULES",
  /*  111 */ "QNODES",
  /*  112 */ "FUNCTIONS",
  /*  113 */ "INDEXES",
  /*  114 */ "FROM",
  /*  115 */ "LIKE",
  /*  116 */ "INDEX",
  /*  117 */ "FULLTEXT",
  /*  118 */ "FUNCTION",
  /*  119 */ "INTERVAL",
  /*  120 */ "TOPIC",
  /*  121 */ "AS",
  /*  122 */ "NK_BOOL",
  /*  123 */ "NK_VARIABLE",
  /*  124 */ "BETWEEN",
  /*  125 */ "IS",
  /*  126 */ "NULL",
  /*  127 */ "NK_LT",
  /*  128 */ "NK_GT",
  /*  129 */ "NK_LE",
  /*  130 */ "NK_GE",
  /*  131 */ "NK_NE",
  /*  132 */ "MATCH",
  /*  133 */ "NMATCH",
  /*  134 */ "IN",
  /*  135 */ "JOIN",
  /*  136 */ "INNER",
  /*  137 */ "SELECT",
  /*  138 */ "DISTINCT",
  /*  139 */ "WHERE",
  /*  140 */ "PARTITION",
  /*  141 */ "BY",
  /*  142 */ "SESSION",
  /*  143 */ "STATE_WINDOW",
  /*  144 */ "SLIDING",
  /*  145 */ "FILL",
  /*  146 */ "VALUE",
  /*  147 */ "NONE",
  /*  148 */ "PREV",
  /*  149 */ "LINEAR",
  /*  150 */ "NEXT",
  /*  151 */ "GROUP",
  /*  152 */ "HAVING",
  /*  153 */ "ORDER",
  /*  154 */ "SLIMIT",
  /*  155 */ "SOFFSET",
  /*  156 */ "LIMIT",
  /*  157 */ "OFFSET",
  /*  158 */ "ASC",
  /*  159 */ "DESC",
  /*  160 */ "NULLS",
  /*  161 */ "FIRST",
  /*  162 */ "LAST",
  /*  163 */ "cmd",
  /*  164 */ "account_options",
  /*  165 */ "alter_account_options",
  /*  166 */ "literal",
  /*  167 */ "alter_account_option",
  /*  168 */ "user_name",
  /*  169 */ "dnode_endpoint",
  /*  170 */ "dnode_host_name",
  /*  171 */ "not_exists_opt",
  /*  172 */ "db_name",
  /*  173 */ "db_options",
  /*  174 */ "exists_opt",
  /*  175 */ "alter_db_options",
  /*  176 */ "alter_db_option",
  /*  177 */ "full_table_name",
  /*  178 */ "column_def_list",
  /*  179 */ "tags_def_opt",
  /*  180 */ "table_options",
  /*  181 */ "multi_create_clause",
  /*  182 */ "tags_def",
  /*  183 */ "multi_drop_clause",
  /*  184 */ "alter_table_clause",
  /*  185 */ "alter_table_options",
  /*  186 */ "column_name",
  /*  187 */ "type_name",
  /*  188 */ "create_subtable_clause",
  /*  189 */ "specific_tags_opt",
  /*  190 */ "literal_list",
  /*  191 */ "drop_table_clause",
  /*  192 */ "col_name_list",
  /*  193 */ "table_name",
  /*  194 */ "column_def",
  /*  195 */ "func_name_list",
  /*  196 */ "alter_table_option",
  /*  197 */ "col_name",
  /*  198 */ "db_name_cond_opt",
  /*  199 */ "like_pattern_opt",
  /*  200 */ "table_name_cond",
  /*  201 */ "from_db_opt",
  /*  202 */ "func_name",
  /*  203 */ "function_name",
  /*  204 */ "index_name",
  /*  205 */ "index_options",
  /*  206 */ "func_list",
  /*  207 */ "duration_literal",
  /*  208 */ "sliding_opt",
  /*  209 */ "func",
  /*  210 */ "expression_list",
  /*  211 */ "topic_name",
  /*  212 */ "query_expression",
  /*  213 */ "table_alias",
  /*  214 */ "column_alias",
  /*  215 */ "expression",
  /*  216 */ "column_reference",
  /*  217 */ "subquery",
  /*  218 */ "predicate",
  /*  219 */ "compare_op",
  /*  220 */ "in_op",
  /*  221 */ "in_predicate_value",
  /*  222 */ "boolean_value_expression",
  /*  223 */ "boolean_primary",
  /*  224 */ "common_expression",
  /*  225 */ "from_clause",
  /*  226 */ "table_reference_list",
  /*  227 */ "table_reference",
  /*  228 */ "table_primary",
  /*  229 */ "joined_table",
  /*  230 */ "alias_opt",
  /*  231 */ "parenthesized_joined_table",
  /*  232 */ "join_type",
  /*  233 */ "search_condition",
  /*  234 */ "query_specification",
  /*  235 */ "set_quantifier_opt",
  /*  236 */ "select_list",
  /*  237 */ "where_clause_opt",
  /*  238 */ "partition_by_clause_opt",
  /*  239 */ "twindow_clause_opt",
  /*  240 */ "group_by_clause_opt",
  /*  241 */ "having_clause_opt",
  /*  242 */ "select_sublist",
  /*  243 */ "select_item",
  /*  244 */ "fill_opt",
  /*  245 */ "fill_mode",
  /*  246 */ "group_by_list",
  /*  247 */ "query_expression_body",
  /*  248 */ "order_by_clause_opt",
  /*  249 */ "slimit_clause_opt",
  /*  250 */ "limit_clause_opt",
  /*  251 */ "query_primary",
  /*  252 */ "sort_specification_list",
  /*  253 */ "sort_specification",
  /*  254 */ "ordering_specification_opt",
  /*  255 */ "null_ordering_opt",
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
 /*  28 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*  29 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*  30 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*  31 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*  32 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING",
 /*  33 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING",
 /*  34 */ "cmd ::= ALTER ALL DNODES NK_STRING",
 /*  35 */ "cmd ::= ALTER ALL DNODES NK_STRING NK_STRING",
 /*  36 */ "dnode_endpoint ::= NK_STRING",
 /*  37 */ "dnode_host_name ::= NK_ID",
 /*  38 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  39 */ "cmd ::= ALTER LOCAL NK_STRING",
 /*  40 */ "cmd ::= ALTER LOCAL NK_STRING NK_STRING",
 /*  41 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  42 */ "cmd ::= DROP QNODE ON DNODE NK_INTEGER",
 /*  43 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  44 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  45 */ "cmd ::= USE db_name",
 /*  46 */ "cmd ::= ALTER DATABASE db_name alter_db_options",
 /*  47 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  48 */ "not_exists_opt ::=",
 /*  49 */ "exists_opt ::= IF EXISTS",
 /*  50 */ "exists_opt ::=",
 /*  51 */ "db_options ::=",
 /*  52 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  53 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  54 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  55 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  56 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  57 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  58 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  59 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  60 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  61 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  62 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  63 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  64 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  65 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  66 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  67 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  68 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  69 */ "db_options ::= db_options RETENTIONS NK_STRING",
 /*  70 */ "db_options ::= db_options FILE_FACTOR NK_FLOAT",
 /*  71 */ "alter_db_options ::= alter_db_option",
 /*  72 */ "alter_db_options ::= alter_db_options alter_db_option",
 /*  73 */ "alter_db_option ::= BLOCKS NK_INTEGER",
 /*  74 */ "alter_db_option ::= FSYNC NK_INTEGER",
 /*  75 */ "alter_db_option ::= KEEP NK_INTEGER",
 /*  76 */ "alter_db_option ::= WAL NK_INTEGER",
 /*  77 */ "alter_db_option ::= QUORUM NK_INTEGER",
 /*  78 */ "alter_db_option ::= CACHELAST NK_INTEGER",
 /*  79 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  80 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  81 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  82 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  83 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  84 */ "cmd ::= ALTER TABLE alter_table_clause",
 /*  85 */ "cmd ::= ALTER STABLE alter_table_clause",
 /*  86 */ "alter_table_clause ::= full_table_name alter_table_options",
 /*  87 */ "alter_table_clause ::= full_table_name ADD COLUMN column_name type_name",
 /*  88 */ "alter_table_clause ::= full_table_name DROP COLUMN column_name",
 /*  89 */ "alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name",
 /*  90 */ "alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name",
 /*  91 */ "alter_table_clause ::= full_table_name ADD TAG column_name type_name",
 /*  92 */ "alter_table_clause ::= full_table_name DROP TAG column_name",
 /*  93 */ "alter_table_clause ::= full_table_name MODIFY TAG column_name type_name",
 /*  94 */ "alter_table_clause ::= full_table_name RENAME TAG column_name column_name",
 /*  95 */ "alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal",
 /*  96 */ "multi_create_clause ::= create_subtable_clause",
 /*  97 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  98 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  99 */ "multi_drop_clause ::= drop_table_clause",
 /* 100 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /* 101 */ "drop_table_clause ::= exists_opt full_table_name",
 /* 102 */ "specific_tags_opt ::=",
 /* 103 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /* 104 */ "full_table_name ::= table_name",
 /* 105 */ "full_table_name ::= db_name NK_DOT table_name",
 /* 106 */ "column_def_list ::= column_def",
 /* 107 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /* 108 */ "column_def ::= column_name type_name",
 /* 109 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /* 110 */ "type_name ::= BOOL",
 /* 111 */ "type_name ::= TINYINT",
 /* 112 */ "type_name ::= SMALLINT",
 /* 113 */ "type_name ::= INT",
 /* 114 */ "type_name ::= INTEGER",
 /* 115 */ "type_name ::= BIGINT",
 /* 116 */ "type_name ::= FLOAT",
 /* 117 */ "type_name ::= DOUBLE",
 /* 118 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /* 119 */ "type_name ::= TIMESTAMP",
 /* 120 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /* 121 */ "type_name ::= TINYINT UNSIGNED",
 /* 122 */ "type_name ::= SMALLINT UNSIGNED",
 /* 123 */ "type_name ::= INT UNSIGNED",
 /* 124 */ "type_name ::= BIGINT UNSIGNED",
 /* 125 */ "type_name ::= JSON",
 /* 126 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /* 127 */ "type_name ::= MEDIUMBLOB",
 /* 128 */ "type_name ::= BLOB",
 /* 129 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /* 130 */ "type_name ::= DECIMAL",
 /* 131 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /* 132 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /* 133 */ "tags_def_opt ::=",
 /* 134 */ "tags_def_opt ::= tags_def",
 /* 135 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /* 136 */ "table_options ::=",
 /* 137 */ "table_options ::= table_options COMMENT NK_STRING",
 /* 138 */ "table_options ::= table_options KEEP NK_INTEGER",
 /* 139 */ "table_options ::= table_options TTL NK_INTEGER",
 /* 140 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /* 141 */ "table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP",
 /* 142 */ "alter_table_options ::= alter_table_option",
 /* 143 */ "alter_table_options ::= alter_table_options alter_table_option",
 /* 144 */ "alter_table_option ::= COMMENT NK_STRING",
 /* 145 */ "alter_table_option ::= KEEP NK_INTEGER",
 /* 146 */ "alter_table_option ::= TTL NK_INTEGER",
 /* 147 */ "col_name_list ::= col_name",
 /* 148 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /* 149 */ "col_name ::= column_name",
 /* 150 */ "cmd ::= SHOW DNODES",
 /* 151 */ "cmd ::= SHOW USERS",
 /* 152 */ "cmd ::= SHOW DATABASES",
 /* 153 */ "cmd ::= SHOW db_name_cond_opt TABLES like_pattern_opt",
 /* 154 */ "cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt",
 /* 155 */ "cmd ::= SHOW db_name_cond_opt VGROUPS",
 /* 156 */ "cmd ::= SHOW MNODES",
 /* 157 */ "cmd ::= SHOW MODULES",
 /* 158 */ "cmd ::= SHOW QNODES",
 /* 159 */ "cmd ::= SHOW FUNCTIONS",
 /* 160 */ "cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt",
 /* 161 */ "cmd ::= SHOW STREAMS",
 /* 162 */ "db_name_cond_opt ::=",
 /* 163 */ "db_name_cond_opt ::= db_name NK_DOT",
 /* 164 */ "like_pattern_opt ::=",
 /* 165 */ "like_pattern_opt ::= LIKE NK_STRING",
 /* 166 */ "table_name_cond ::= table_name",
 /* 167 */ "from_db_opt ::=",
 /* 168 */ "from_db_opt ::= FROM db_name",
 /* 169 */ "func_name_list ::= func_name",
 /* 170 */ "func_name_list ::= func_name_list NK_COMMA col_name",
 /* 171 */ "func_name ::= function_name",
 /* 172 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /* 173 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /* 174 */ "cmd ::= DROP INDEX index_name ON table_name",
 /* 175 */ "index_options ::=",
 /* 176 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /* 177 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /* 178 */ "func_list ::= func",
 /* 179 */ "func_list ::= func_list NK_COMMA func",
 /* 180 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 181 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression",
 /* 182 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name",
 /* 183 */ "cmd ::= DROP TOPIC exists_opt topic_name",
 /* 184 */ "cmd ::= query_expression",
 /* 185 */ "literal ::= NK_INTEGER",
 /* 186 */ "literal ::= NK_FLOAT",
 /* 187 */ "literal ::= NK_STRING",
 /* 188 */ "literal ::= NK_BOOL",
 /* 189 */ "literal ::= TIMESTAMP NK_STRING",
 /* 190 */ "literal ::= duration_literal",
 /* 191 */ "duration_literal ::= NK_VARIABLE",
 /* 192 */ "literal_list ::= literal",
 /* 193 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 194 */ "db_name ::= NK_ID",
 /* 195 */ "table_name ::= NK_ID",
 /* 196 */ "column_name ::= NK_ID",
 /* 197 */ "function_name ::= NK_ID",
 /* 198 */ "table_alias ::= NK_ID",
 /* 199 */ "column_alias ::= NK_ID",
 /* 200 */ "user_name ::= NK_ID",
 /* 201 */ "index_name ::= NK_ID",
 /* 202 */ "topic_name ::= NK_ID",
 /* 203 */ "expression ::= literal",
 /* 204 */ "expression ::= column_reference",
 /* 205 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 206 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 207 */ "expression ::= subquery",
 /* 208 */ "expression ::= NK_LP expression NK_RP",
 /* 209 */ "expression ::= NK_PLUS expression",
 /* 210 */ "expression ::= NK_MINUS expression",
 /* 211 */ "expression ::= expression NK_PLUS expression",
 /* 212 */ "expression ::= expression NK_MINUS expression",
 /* 213 */ "expression ::= expression NK_STAR expression",
 /* 214 */ "expression ::= expression NK_SLASH expression",
 /* 215 */ "expression ::= expression NK_REM expression",
 /* 216 */ "expression_list ::= expression",
 /* 217 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 218 */ "column_reference ::= column_name",
 /* 219 */ "column_reference ::= table_name NK_DOT column_name",
 /* 220 */ "predicate ::= expression compare_op expression",
 /* 221 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 222 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 223 */ "predicate ::= expression IS NULL",
 /* 224 */ "predicate ::= expression IS NOT NULL",
 /* 225 */ "predicate ::= expression in_op in_predicate_value",
 /* 226 */ "compare_op ::= NK_LT",
 /* 227 */ "compare_op ::= NK_GT",
 /* 228 */ "compare_op ::= NK_LE",
 /* 229 */ "compare_op ::= NK_GE",
 /* 230 */ "compare_op ::= NK_NE",
 /* 231 */ "compare_op ::= NK_EQ",
 /* 232 */ "compare_op ::= LIKE",
 /* 233 */ "compare_op ::= NOT LIKE",
 /* 234 */ "compare_op ::= MATCH",
 /* 235 */ "compare_op ::= NMATCH",
 /* 236 */ "in_op ::= IN",
 /* 237 */ "in_op ::= NOT IN",
 /* 238 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 239 */ "boolean_value_expression ::= boolean_primary",
 /* 240 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 241 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 242 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 243 */ "boolean_primary ::= predicate",
 /* 244 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 245 */ "common_expression ::= expression",
 /* 246 */ "common_expression ::= boolean_value_expression",
 /* 247 */ "from_clause ::= FROM table_reference_list",
 /* 248 */ "table_reference_list ::= table_reference",
 /* 249 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 250 */ "table_reference ::= table_primary",
 /* 251 */ "table_reference ::= joined_table",
 /* 252 */ "table_primary ::= table_name alias_opt",
 /* 253 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 254 */ "table_primary ::= subquery alias_opt",
 /* 255 */ "table_primary ::= parenthesized_joined_table",
 /* 256 */ "alias_opt ::=",
 /* 257 */ "alias_opt ::= table_alias",
 /* 258 */ "alias_opt ::= AS table_alias",
 /* 259 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 260 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 261 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 262 */ "join_type ::=",
 /* 263 */ "join_type ::= INNER",
 /* 264 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 265 */ "set_quantifier_opt ::=",
 /* 266 */ "set_quantifier_opt ::= DISTINCT",
 /* 267 */ "set_quantifier_opt ::= ALL",
 /* 268 */ "select_list ::= NK_STAR",
 /* 269 */ "select_list ::= select_sublist",
 /* 270 */ "select_sublist ::= select_item",
 /* 271 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 272 */ "select_item ::= common_expression",
 /* 273 */ "select_item ::= common_expression column_alias",
 /* 274 */ "select_item ::= common_expression AS column_alias",
 /* 275 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 276 */ "where_clause_opt ::=",
 /* 277 */ "where_clause_opt ::= WHERE search_condition",
 /* 278 */ "partition_by_clause_opt ::=",
 /* 279 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 280 */ "twindow_clause_opt ::=",
 /* 281 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 282 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 283 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 284 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 285 */ "sliding_opt ::=",
 /* 286 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 287 */ "fill_opt ::=",
 /* 288 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 289 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 290 */ "fill_mode ::= NONE",
 /* 291 */ "fill_mode ::= PREV",
 /* 292 */ "fill_mode ::= NULL",
 /* 293 */ "fill_mode ::= LINEAR",
 /* 294 */ "fill_mode ::= NEXT",
 /* 295 */ "group_by_clause_opt ::=",
 /* 296 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 297 */ "group_by_list ::= expression",
 /* 298 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 299 */ "having_clause_opt ::=",
 /* 300 */ "having_clause_opt ::= HAVING search_condition",
 /* 301 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 302 */ "query_expression_body ::= query_primary",
 /* 303 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 304 */ "query_primary ::= query_specification",
 /* 305 */ "order_by_clause_opt ::=",
 /* 306 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 307 */ "slimit_clause_opt ::=",
 /* 308 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 309 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 310 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 311 */ "limit_clause_opt ::=",
 /* 312 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 313 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 314 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 315 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 316 */ "search_condition ::= common_expression",
 /* 317 */ "sort_specification_list ::= sort_specification",
 /* 318 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 319 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 320 */ "ordering_specification_opt ::=",
 /* 321 */ "ordering_specification_opt ::= ASC",
 /* 322 */ "ordering_specification_opt ::= DESC",
 /* 323 */ "null_ordering_opt ::=",
 /* 324 */ "null_ordering_opt ::= NULLS FIRST",
 /* 325 */ "null_ordering_opt ::= NULLS LAST",
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
    case 163: /* cmd */
    case 166: /* literal */
    case 173: /* db_options */
    case 175: /* alter_db_options */
    case 177: /* full_table_name */
    case 180: /* table_options */
    case 184: /* alter_table_clause */
    case 185: /* alter_table_options */
    case 188: /* create_subtable_clause */
    case 191: /* drop_table_clause */
    case 194: /* column_def */
    case 197: /* col_name */
    case 198: /* db_name_cond_opt */
    case 199: /* like_pattern_opt */
    case 200: /* table_name_cond */
    case 201: /* from_db_opt */
    case 202: /* func_name */
    case 205: /* index_options */
    case 207: /* duration_literal */
    case 208: /* sliding_opt */
    case 209: /* func */
    case 212: /* query_expression */
    case 215: /* expression */
    case 216: /* column_reference */
    case 217: /* subquery */
    case 218: /* predicate */
    case 221: /* in_predicate_value */
    case 222: /* boolean_value_expression */
    case 223: /* boolean_primary */
    case 224: /* common_expression */
    case 225: /* from_clause */
    case 226: /* table_reference_list */
    case 227: /* table_reference */
    case 228: /* table_primary */
    case 229: /* joined_table */
    case 231: /* parenthesized_joined_table */
    case 233: /* search_condition */
    case 234: /* query_specification */
    case 237: /* where_clause_opt */
    case 239: /* twindow_clause_opt */
    case 241: /* having_clause_opt */
    case 243: /* select_item */
    case 244: /* fill_opt */
    case 247: /* query_expression_body */
    case 249: /* slimit_clause_opt */
    case 250: /* limit_clause_opt */
    case 251: /* query_primary */
    case 253: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy360)); 
}
      break;
    case 164: /* account_options */
    case 165: /* alter_account_options */
    case 167: /* alter_account_option */
{
 
}
      break;
    case 168: /* user_name */
    case 169: /* dnode_endpoint */
    case 170: /* dnode_host_name */
    case 172: /* db_name */
    case 186: /* column_name */
    case 193: /* table_name */
    case 203: /* function_name */
    case 204: /* index_name */
    case 211: /* topic_name */
    case 213: /* table_alias */
    case 214: /* column_alias */
    case 230: /* alias_opt */
{
 
}
      break;
    case 171: /* not_exists_opt */
    case 174: /* exists_opt */
    case 235: /* set_quantifier_opt */
{
 
}
      break;
    case 176: /* alter_db_option */
    case 196: /* alter_table_option */
{
 
}
      break;
    case 178: /* column_def_list */
    case 179: /* tags_def_opt */
    case 181: /* multi_create_clause */
    case 182: /* tags_def */
    case 183: /* multi_drop_clause */
    case 189: /* specific_tags_opt */
    case 190: /* literal_list */
    case 192: /* col_name_list */
    case 195: /* func_name_list */
    case 206: /* func_list */
    case 210: /* expression_list */
    case 236: /* select_list */
    case 238: /* partition_by_clause_opt */
    case 240: /* group_by_clause_opt */
    case 242: /* select_sublist */
    case 246: /* group_by_list */
    case 248: /* order_by_clause_opt */
    case 252: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy24)); 
}
      break;
    case 187: /* type_name */
{
 
}
      break;
    case 219: /* compare_op */
    case 220: /* in_op */
{
 
}
      break;
    case 232: /* join_type */
{
 
}
      break;
    case 245: /* fill_mode */
{
 
}
      break;
    case 254: /* ordering_specification_opt */
{
 
}
      break;
    case 255: /* null_ordering_opt */
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
  {  163,   -6 }, /* (0) cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
  {  163,   -4 }, /* (1) cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
  {  164,    0 }, /* (2) account_options ::= */
  {  164,   -3 }, /* (3) account_options ::= account_options PPS literal */
  {  164,   -3 }, /* (4) account_options ::= account_options TSERIES literal */
  {  164,   -3 }, /* (5) account_options ::= account_options STORAGE literal */
  {  164,   -3 }, /* (6) account_options ::= account_options STREAMS literal */
  {  164,   -3 }, /* (7) account_options ::= account_options QTIME literal */
  {  164,   -3 }, /* (8) account_options ::= account_options DBS literal */
  {  164,   -3 }, /* (9) account_options ::= account_options USERS literal */
  {  164,   -3 }, /* (10) account_options ::= account_options CONNS literal */
  {  164,   -3 }, /* (11) account_options ::= account_options STATE literal */
  {  165,   -1 }, /* (12) alter_account_options ::= alter_account_option */
  {  165,   -2 }, /* (13) alter_account_options ::= alter_account_options alter_account_option */
  {  167,   -2 }, /* (14) alter_account_option ::= PASS literal */
  {  167,   -2 }, /* (15) alter_account_option ::= PPS literal */
  {  167,   -2 }, /* (16) alter_account_option ::= TSERIES literal */
  {  167,   -2 }, /* (17) alter_account_option ::= STORAGE literal */
  {  167,   -2 }, /* (18) alter_account_option ::= STREAMS literal */
  {  167,   -2 }, /* (19) alter_account_option ::= QTIME literal */
  {  167,   -2 }, /* (20) alter_account_option ::= DBS literal */
  {  167,   -2 }, /* (21) alter_account_option ::= USERS literal */
  {  167,   -2 }, /* (22) alter_account_option ::= CONNS literal */
  {  167,   -2 }, /* (23) alter_account_option ::= STATE literal */
  {  163,   -5 }, /* (24) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  163,   -5 }, /* (25) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  163,   -5 }, /* (26) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  163,   -3 }, /* (27) cmd ::= DROP USER user_name */
  {  163,   -3 }, /* (28) cmd ::= CREATE DNODE dnode_endpoint */
  {  163,   -5 }, /* (29) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  163,   -3 }, /* (30) cmd ::= DROP DNODE NK_INTEGER */
  {  163,   -3 }, /* (31) cmd ::= DROP DNODE dnode_endpoint */
  {  163,   -4 }, /* (32) cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
  {  163,   -5 }, /* (33) cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
  {  163,   -4 }, /* (34) cmd ::= ALTER ALL DNODES NK_STRING */
  {  163,   -5 }, /* (35) cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
  {  169,   -1 }, /* (36) dnode_endpoint ::= NK_STRING */
  {  170,   -1 }, /* (37) dnode_host_name ::= NK_ID */
  {  170,   -1 }, /* (38) dnode_host_name ::= NK_IPTOKEN */
  {  163,   -3 }, /* (39) cmd ::= ALTER LOCAL NK_STRING */
  {  163,   -4 }, /* (40) cmd ::= ALTER LOCAL NK_STRING NK_STRING */
  {  163,   -5 }, /* (41) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  163,   -5 }, /* (42) cmd ::= DROP QNODE ON DNODE NK_INTEGER */
  {  163,   -5 }, /* (43) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  163,   -4 }, /* (44) cmd ::= DROP DATABASE exists_opt db_name */
  {  163,   -2 }, /* (45) cmd ::= USE db_name */
  {  163,   -4 }, /* (46) cmd ::= ALTER DATABASE db_name alter_db_options */
  {  171,   -3 }, /* (47) not_exists_opt ::= IF NOT EXISTS */
  {  171,    0 }, /* (48) not_exists_opt ::= */
  {  174,   -2 }, /* (49) exists_opt ::= IF EXISTS */
  {  174,    0 }, /* (50) exists_opt ::= */
  {  173,    0 }, /* (51) db_options ::= */
  {  173,   -3 }, /* (52) db_options ::= db_options BLOCKS NK_INTEGER */
  {  173,   -3 }, /* (53) db_options ::= db_options CACHE NK_INTEGER */
  {  173,   -3 }, /* (54) db_options ::= db_options CACHELAST NK_INTEGER */
  {  173,   -3 }, /* (55) db_options ::= db_options COMP NK_INTEGER */
  {  173,   -3 }, /* (56) db_options ::= db_options DAYS NK_INTEGER */
  {  173,   -3 }, /* (57) db_options ::= db_options FSYNC NK_INTEGER */
  {  173,   -3 }, /* (58) db_options ::= db_options MAXROWS NK_INTEGER */
  {  173,   -3 }, /* (59) db_options ::= db_options MINROWS NK_INTEGER */
  {  173,   -3 }, /* (60) db_options ::= db_options KEEP NK_INTEGER */
  {  173,   -3 }, /* (61) db_options ::= db_options PRECISION NK_STRING */
  {  173,   -3 }, /* (62) db_options ::= db_options QUORUM NK_INTEGER */
  {  173,   -3 }, /* (63) db_options ::= db_options REPLICA NK_INTEGER */
  {  173,   -3 }, /* (64) db_options ::= db_options TTL NK_INTEGER */
  {  173,   -3 }, /* (65) db_options ::= db_options WAL NK_INTEGER */
  {  173,   -3 }, /* (66) db_options ::= db_options VGROUPS NK_INTEGER */
  {  173,   -3 }, /* (67) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  173,   -3 }, /* (68) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  173,   -3 }, /* (69) db_options ::= db_options RETENTIONS NK_STRING */
  {  173,   -3 }, /* (70) db_options ::= db_options FILE_FACTOR NK_FLOAT */
  {  175,   -1 }, /* (71) alter_db_options ::= alter_db_option */
  {  175,   -2 }, /* (72) alter_db_options ::= alter_db_options alter_db_option */
  {  176,   -2 }, /* (73) alter_db_option ::= BLOCKS NK_INTEGER */
  {  176,   -2 }, /* (74) alter_db_option ::= FSYNC NK_INTEGER */
  {  176,   -2 }, /* (75) alter_db_option ::= KEEP NK_INTEGER */
  {  176,   -2 }, /* (76) alter_db_option ::= WAL NK_INTEGER */
  {  176,   -2 }, /* (77) alter_db_option ::= QUORUM NK_INTEGER */
  {  176,   -2 }, /* (78) alter_db_option ::= CACHELAST NK_INTEGER */
  {  163,   -9 }, /* (79) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  163,   -3 }, /* (80) cmd ::= CREATE TABLE multi_create_clause */
  {  163,   -9 }, /* (81) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  163,   -3 }, /* (82) cmd ::= DROP TABLE multi_drop_clause */
  {  163,   -4 }, /* (83) cmd ::= DROP STABLE exists_opt full_table_name */
  {  163,   -3 }, /* (84) cmd ::= ALTER TABLE alter_table_clause */
  {  163,   -3 }, /* (85) cmd ::= ALTER STABLE alter_table_clause */
  {  184,   -2 }, /* (86) alter_table_clause ::= full_table_name alter_table_options */
  {  184,   -5 }, /* (87) alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
  {  184,   -4 }, /* (88) alter_table_clause ::= full_table_name DROP COLUMN column_name */
  {  184,   -5 }, /* (89) alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
  {  184,   -5 }, /* (90) alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
  {  184,   -5 }, /* (91) alter_table_clause ::= full_table_name ADD TAG column_name type_name */
  {  184,   -4 }, /* (92) alter_table_clause ::= full_table_name DROP TAG column_name */
  {  184,   -5 }, /* (93) alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
  {  184,   -5 }, /* (94) alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
  {  184,   -6 }, /* (95) alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
  {  181,   -1 }, /* (96) multi_create_clause ::= create_subtable_clause */
  {  181,   -2 }, /* (97) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  188,   -9 }, /* (98) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  183,   -1 }, /* (99) multi_drop_clause ::= drop_table_clause */
  {  183,   -2 }, /* (100) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  191,   -2 }, /* (101) drop_table_clause ::= exists_opt full_table_name */
  {  189,    0 }, /* (102) specific_tags_opt ::= */
  {  189,   -3 }, /* (103) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  177,   -1 }, /* (104) full_table_name ::= table_name */
  {  177,   -3 }, /* (105) full_table_name ::= db_name NK_DOT table_name */
  {  178,   -1 }, /* (106) column_def_list ::= column_def */
  {  178,   -3 }, /* (107) column_def_list ::= column_def_list NK_COMMA column_def */
  {  194,   -2 }, /* (108) column_def ::= column_name type_name */
  {  194,   -4 }, /* (109) column_def ::= column_name type_name COMMENT NK_STRING */
  {  187,   -1 }, /* (110) type_name ::= BOOL */
  {  187,   -1 }, /* (111) type_name ::= TINYINT */
  {  187,   -1 }, /* (112) type_name ::= SMALLINT */
  {  187,   -1 }, /* (113) type_name ::= INT */
  {  187,   -1 }, /* (114) type_name ::= INTEGER */
  {  187,   -1 }, /* (115) type_name ::= BIGINT */
  {  187,   -1 }, /* (116) type_name ::= FLOAT */
  {  187,   -1 }, /* (117) type_name ::= DOUBLE */
  {  187,   -4 }, /* (118) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  187,   -1 }, /* (119) type_name ::= TIMESTAMP */
  {  187,   -4 }, /* (120) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  187,   -2 }, /* (121) type_name ::= TINYINT UNSIGNED */
  {  187,   -2 }, /* (122) type_name ::= SMALLINT UNSIGNED */
  {  187,   -2 }, /* (123) type_name ::= INT UNSIGNED */
  {  187,   -2 }, /* (124) type_name ::= BIGINT UNSIGNED */
  {  187,   -1 }, /* (125) type_name ::= JSON */
  {  187,   -4 }, /* (126) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  187,   -1 }, /* (127) type_name ::= MEDIUMBLOB */
  {  187,   -1 }, /* (128) type_name ::= BLOB */
  {  187,   -4 }, /* (129) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  187,   -1 }, /* (130) type_name ::= DECIMAL */
  {  187,   -4 }, /* (131) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  187,   -6 }, /* (132) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  179,    0 }, /* (133) tags_def_opt ::= */
  {  179,   -1 }, /* (134) tags_def_opt ::= tags_def */
  {  182,   -4 }, /* (135) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  180,    0 }, /* (136) table_options ::= */
  {  180,   -3 }, /* (137) table_options ::= table_options COMMENT NK_STRING */
  {  180,   -3 }, /* (138) table_options ::= table_options KEEP NK_INTEGER */
  {  180,   -3 }, /* (139) table_options ::= table_options TTL NK_INTEGER */
  {  180,   -5 }, /* (140) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  180,   -5 }, /* (141) table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
  {  185,   -1 }, /* (142) alter_table_options ::= alter_table_option */
  {  185,   -2 }, /* (143) alter_table_options ::= alter_table_options alter_table_option */
  {  196,   -2 }, /* (144) alter_table_option ::= COMMENT NK_STRING */
  {  196,   -2 }, /* (145) alter_table_option ::= KEEP NK_INTEGER */
  {  196,   -2 }, /* (146) alter_table_option ::= TTL NK_INTEGER */
  {  192,   -1 }, /* (147) col_name_list ::= col_name */
  {  192,   -3 }, /* (148) col_name_list ::= col_name_list NK_COMMA col_name */
  {  197,   -1 }, /* (149) col_name ::= column_name */
  {  163,   -2 }, /* (150) cmd ::= SHOW DNODES */
  {  163,   -2 }, /* (151) cmd ::= SHOW USERS */
  {  163,   -2 }, /* (152) cmd ::= SHOW DATABASES */
  {  163,   -4 }, /* (153) cmd ::= SHOW db_name_cond_opt TABLES like_pattern_opt */
  {  163,   -4 }, /* (154) cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt */
  {  163,   -3 }, /* (155) cmd ::= SHOW db_name_cond_opt VGROUPS */
  {  163,   -2 }, /* (156) cmd ::= SHOW MNODES */
  {  163,   -2 }, /* (157) cmd ::= SHOW MODULES */
  {  163,   -2 }, /* (158) cmd ::= SHOW QNODES */
  {  163,   -2 }, /* (159) cmd ::= SHOW FUNCTIONS */
  {  163,   -5 }, /* (160) cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt */
  {  163,   -2 }, /* (161) cmd ::= SHOW STREAMS */
  {  198,    0 }, /* (162) db_name_cond_opt ::= */
  {  198,   -2 }, /* (163) db_name_cond_opt ::= db_name NK_DOT */
  {  199,    0 }, /* (164) like_pattern_opt ::= */
  {  199,   -2 }, /* (165) like_pattern_opt ::= LIKE NK_STRING */
  {  200,   -1 }, /* (166) table_name_cond ::= table_name */
  {  201,    0 }, /* (167) from_db_opt ::= */
  {  201,   -2 }, /* (168) from_db_opt ::= FROM db_name */
  {  195,   -1 }, /* (169) func_name_list ::= func_name */
  {  195,   -3 }, /* (170) func_name_list ::= func_name_list NK_COMMA col_name */
  {  202,   -1 }, /* (171) func_name ::= function_name */
  {  163,   -7 }, /* (172) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  163,   -9 }, /* (173) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  163,   -5 }, /* (174) cmd ::= DROP INDEX index_name ON table_name */
  {  205,    0 }, /* (175) index_options ::= */
  {  205,   -9 }, /* (176) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  205,  -11 }, /* (177) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  206,   -1 }, /* (178) func_list ::= func */
  {  206,   -3 }, /* (179) func_list ::= func_list NK_COMMA func */
  {  209,   -4 }, /* (180) func ::= function_name NK_LP expression_list NK_RP */
  {  163,   -6 }, /* (181) cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
  {  163,   -6 }, /* (182) cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
  {  163,   -4 }, /* (183) cmd ::= DROP TOPIC exists_opt topic_name */
  {  163,   -1 }, /* (184) cmd ::= query_expression */
  {  166,   -1 }, /* (185) literal ::= NK_INTEGER */
  {  166,   -1 }, /* (186) literal ::= NK_FLOAT */
  {  166,   -1 }, /* (187) literal ::= NK_STRING */
  {  166,   -1 }, /* (188) literal ::= NK_BOOL */
  {  166,   -2 }, /* (189) literal ::= TIMESTAMP NK_STRING */
  {  166,   -1 }, /* (190) literal ::= duration_literal */
  {  207,   -1 }, /* (191) duration_literal ::= NK_VARIABLE */
  {  190,   -1 }, /* (192) literal_list ::= literal */
  {  190,   -3 }, /* (193) literal_list ::= literal_list NK_COMMA literal */
  {  172,   -1 }, /* (194) db_name ::= NK_ID */
  {  193,   -1 }, /* (195) table_name ::= NK_ID */
  {  186,   -1 }, /* (196) column_name ::= NK_ID */
  {  203,   -1 }, /* (197) function_name ::= NK_ID */
  {  213,   -1 }, /* (198) table_alias ::= NK_ID */
  {  214,   -1 }, /* (199) column_alias ::= NK_ID */
  {  168,   -1 }, /* (200) user_name ::= NK_ID */
  {  204,   -1 }, /* (201) index_name ::= NK_ID */
  {  211,   -1 }, /* (202) topic_name ::= NK_ID */
  {  215,   -1 }, /* (203) expression ::= literal */
  {  215,   -1 }, /* (204) expression ::= column_reference */
  {  215,   -4 }, /* (205) expression ::= function_name NK_LP expression_list NK_RP */
  {  215,   -4 }, /* (206) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  215,   -1 }, /* (207) expression ::= subquery */
  {  215,   -3 }, /* (208) expression ::= NK_LP expression NK_RP */
  {  215,   -2 }, /* (209) expression ::= NK_PLUS expression */
  {  215,   -2 }, /* (210) expression ::= NK_MINUS expression */
  {  215,   -3 }, /* (211) expression ::= expression NK_PLUS expression */
  {  215,   -3 }, /* (212) expression ::= expression NK_MINUS expression */
  {  215,   -3 }, /* (213) expression ::= expression NK_STAR expression */
  {  215,   -3 }, /* (214) expression ::= expression NK_SLASH expression */
  {  215,   -3 }, /* (215) expression ::= expression NK_REM expression */
  {  210,   -1 }, /* (216) expression_list ::= expression */
  {  210,   -3 }, /* (217) expression_list ::= expression_list NK_COMMA expression */
  {  216,   -1 }, /* (218) column_reference ::= column_name */
  {  216,   -3 }, /* (219) column_reference ::= table_name NK_DOT column_name */
  {  218,   -3 }, /* (220) predicate ::= expression compare_op expression */
  {  218,   -5 }, /* (221) predicate ::= expression BETWEEN expression AND expression */
  {  218,   -6 }, /* (222) predicate ::= expression NOT BETWEEN expression AND expression */
  {  218,   -3 }, /* (223) predicate ::= expression IS NULL */
  {  218,   -4 }, /* (224) predicate ::= expression IS NOT NULL */
  {  218,   -3 }, /* (225) predicate ::= expression in_op in_predicate_value */
  {  219,   -1 }, /* (226) compare_op ::= NK_LT */
  {  219,   -1 }, /* (227) compare_op ::= NK_GT */
  {  219,   -1 }, /* (228) compare_op ::= NK_LE */
  {  219,   -1 }, /* (229) compare_op ::= NK_GE */
  {  219,   -1 }, /* (230) compare_op ::= NK_NE */
  {  219,   -1 }, /* (231) compare_op ::= NK_EQ */
  {  219,   -1 }, /* (232) compare_op ::= LIKE */
  {  219,   -2 }, /* (233) compare_op ::= NOT LIKE */
  {  219,   -1 }, /* (234) compare_op ::= MATCH */
  {  219,   -1 }, /* (235) compare_op ::= NMATCH */
  {  220,   -1 }, /* (236) in_op ::= IN */
  {  220,   -2 }, /* (237) in_op ::= NOT IN */
  {  221,   -3 }, /* (238) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  222,   -1 }, /* (239) boolean_value_expression ::= boolean_primary */
  {  222,   -2 }, /* (240) boolean_value_expression ::= NOT boolean_primary */
  {  222,   -3 }, /* (241) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  222,   -3 }, /* (242) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  223,   -1 }, /* (243) boolean_primary ::= predicate */
  {  223,   -3 }, /* (244) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  224,   -1 }, /* (245) common_expression ::= expression */
  {  224,   -1 }, /* (246) common_expression ::= boolean_value_expression */
  {  225,   -2 }, /* (247) from_clause ::= FROM table_reference_list */
  {  226,   -1 }, /* (248) table_reference_list ::= table_reference */
  {  226,   -3 }, /* (249) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  227,   -1 }, /* (250) table_reference ::= table_primary */
  {  227,   -1 }, /* (251) table_reference ::= joined_table */
  {  228,   -2 }, /* (252) table_primary ::= table_name alias_opt */
  {  228,   -4 }, /* (253) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  228,   -2 }, /* (254) table_primary ::= subquery alias_opt */
  {  228,   -1 }, /* (255) table_primary ::= parenthesized_joined_table */
  {  230,    0 }, /* (256) alias_opt ::= */
  {  230,   -1 }, /* (257) alias_opt ::= table_alias */
  {  230,   -2 }, /* (258) alias_opt ::= AS table_alias */
  {  231,   -3 }, /* (259) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  231,   -3 }, /* (260) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  229,   -6 }, /* (261) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  232,    0 }, /* (262) join_type ::= */
  {  232,   -1 }, /* (263) join_type ::= INNER */
  {  234,   -9 }, /* (264) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  235,    0 }, /* (265) set_quantifier_opt ::= */
  {  235,   -1 }, /* (266) set_quantifier_opt ::= DISTINCT */
  {  235,   -1 }, /* (267) set_quantifier_opt ::= ALL */
  {  236,   -1 }, /* (268) select_list ::= NK_STAR */
  {  236,   -1 }, /* (269) select_list ::= select_sublist */
  {  242,   -1 }, /* (270) select_sublist ::= select_item */
  {  242,   -3 }, /* (271) select_sublist ::= select_sublist NK_COMMA select_item */
  {  243,   -1 }, /* (272) select_item ::= common_expression */
  {  243,   -2 }, /* (273) select_item ::= common_expression column_alias */
  {  243,   -3 }, /* (274) select_item ::= common_expression AS column_alias */
  {  243,   -3 }, /* (275) select_item ::= table_name NK_DOT NK_STAR */
  {  237,    0 }, /* (276) where_clause_opt ::= */
  {  237,   -2 }, /* (277) where_clause_opt ::= WHERE search_condition */
  {  238,    0 }, /* (278) partition_by_clause_opt ::= */
  {  238,   -3 }, /* (279) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  239,    0 }, /* (280) twindow_clause_opt ::= */
  {  239,   -6 }, /* (281) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  239,   -4 }, /* (282) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  239,   -6 }, /* (283) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  239,   -8 }, /* (284) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  208,    0 }, /* (285) sliding_opt ::= */
  {  208,   -4 }, /* (286) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  244,    0 }, /* (287) fill_opt ::= */
  {  244,   -4 }, /* (288) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  244,   -6 }, /* (289) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  245,   -1 }, /* (290) fill_mode ::= NONE */
  {  245,   -1 }, /* (291) fill_mode ::= PREV */
  {  245,   -1 }, /* (292) fill_mode ::= NULL */
  {  245,   -1 }, /* (293) fill_mode ::= LINEAR */
  {  245,   -1 }, /* (294) fill_mode ::= NEXT */
  {  240,    0 }, /* (295) group_by_clause_opt ::= */
  {  240,   -3 }, /* (296) group_by_clause_opt ::= GROUP BY group_by_list */
  {  246,   -1 }, /* (297) group_by_list ::= expression */
  {  246,   -3 }, /* (298) group_by_list ::= group_by_list NK_COMMA expression */
  {  241,    0 }, /* (299) having_clause_opt ::= */
  {  241,   -2 }, /* (300) having_clause_opt ::= HAVING search_condition */
  {  212,   -4 }, /* (301) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  247,   -1 }, /* (302) query_expression_body ::= query_primary */
  {  247,   -4 }, /* (303) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  251,   -1 }, /* (304) query_primary ::= query_specification */
  {  248,    0 }, /* (305) order_by_clause_opt ::= */
  {  248,   -3 }, /* (306) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  249,    0 }, /* (307) slimit_clause_opt ::= */
  {  249,   -2 }, /* (308) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  249,   -4 }, /* (309) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  249,   -4 }, /* (310) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  250,    0 }, /* (311) limit_clause_opt ::= */
  {  250,   -2 }, /* (312) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  250,   -4 }, /* (313) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  250,   -4 }, /* (314) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  217,   -3 }, /* (315) subquery ::= NK_LP query_expression NK_RP */
  {  233,   -1 }, /* (316) search_condition ::= common_expression */
  {  252,   -1 }, /* (317) sort_specification_list ::= sort_specification */
  {  252,   -3 }, /* (318) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  253,   -3 }, /* (319) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  254,    0 }, /* (320) ordering_specification_opt ::= */
  {  254,   -1 }, /* (321) ordering_specification_opt ::= ASC */
  {  254,   -1 }, /* (322) ordering_specification_opt ::= DESC */
  {  255,    0 }, /* (323) null_ordering_opt ::= */
  {  255,   -2 }, /* (324) null_ordering_opt ::= NULLS FIRST */
  {  255,   -2 }, /* (325) null_ordering_opt ::= NULLS LAST */
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
  yy_destructor(yypParser,164,&yymsp[0].minor);
        break;
      case 1: /* cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
{ pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
  yy_destructor(yypParser,165,&yymsp[0].minor);
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
{  yy_destructor(yypParser,164,&yymsp[-2].minor);
{ }
  yy_destructor(yypParser,166,&yymsp[0].minor);
}
        break;
      case 12: /* alter_account_options ::= alter_account_option */
{  yy_destructor(yypParser,167,&yymsp[0].minor);
{ }
}
        break;
      case 13: /* alter_account_options ::= alter_account_options alter_account_option */
{  yy_destructor(yypParser,165,&yymsp[-1].minor);
{ }
  yy_destructor(yypParser,167,&yymsp[0].minor);
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
  yy_destructor(yypParser,166,&yymsp[0].minor);
        break;
      case 24: /* cmd ::= CREATE USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy0); }
        break;
      case 25: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy417, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0); }
        break;
      case 26: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy417, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0); }
        break;
      case 27: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy417); }
        break;
      case 28: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy417, NULL); }
        break;
      case 29: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy0); }
        break;
      case 30: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 31: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy417); }
        break;
      case 32: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, NULL); }
        break;
      case 33: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 34: /* cmd ::= ALTER ALL DNODES NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[0].minor.yy0, NULL); }
        break;
      case 35: /* cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 36: /* dnode_endpoint ::= NK_STRING */
      case 37: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==37);
      case 38: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==38);
      case 194: /* db_name ::= NK_ID */ yytestcase(yyruleno==194);
      case 195: /* table_name ::= NK_ID */ yytestcase(yyruleno==195);
      case 196: /* column_name ::= NK_ID */ yytestcase(yyruleno==196);
      case 197: /* function_name ::= NK_ID */ yytestcase(yyruleno==197);
      case 198: /* table_alias ::= NK_ID */ yytestcase(yyruleno==198);
      case 199: /* column_alias ::= NK_ID */ yytestcase(yyruleno==199);
      case 200: /* user_name ::= NK_ID */ yytestcase(yyruleno==200);
      case 201: /* index_name ::= NK_ID */ yytestcase(yyruleno==201);
      case 202: /* topic_name ::= NK_ID */ yytestcase(yyruleno==202);
{ yylhsminor.yy417 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy417 = yylhsminor.yy417;
        break;
      case 39: /* cmd ::= ALTER LOCAL NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 40: /* cmd ::= ALTER LOCAL NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 41: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createCreateQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 42: /* cmd ::= DROP QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 43: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy345, &yymsp[-1].minor.yy417, yymsp[0].minor.yy360); }
        break;
      case 44: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy345, &yymsp[0].minor.yy417); }
        break;
      case 45: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy417); }
        break;
      case 46: /* cmd ::= ALTER DATABASE db_name alter_db_options */
{ pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &yymsp[-1].minor.yy417, yymsp[0].minor.yy360); }
        break;
      case 47: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy345 = true; }
        break;
      case 48: /* not_exists_opt ::= */
      case 50: /* exists_opt ::= */ yytestcase(yyruleno==50);
      case 265: /* set_quantifier_opt ::= */ yytestcase(yyruleno==265);
{ yymsp[1].minor.yy345 = false; }
        break;
      case 49: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy345 = true; }
        break;
      case 51: /* db_options ::= */
{ yymsp[1].minor.yy360 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 52: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 53: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 54: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 55: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 56: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 57: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 58: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 59: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 60: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 61: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 62: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 63: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 64: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 65: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 66: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 67: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_SINGLE_STABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 68: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_STREAM_MODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 69: /* db_options ::= db_options RETENTIONS NK_STRING */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_RETENTIONS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 70: /* db_options ::= db_options FILE_FACTOR NK_FLOAT */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-2].minor.yy360, DB_OPTION_FILE_FACTOR, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 71: /* alter_db_options ::= alter_db_option */
{ yylhsminor.yy360 = createDefaultAlterDatabaseOptions(pCxt); yylhsminor.yy360 = setDatabaseOption(pCxt, yylhsminor.yy360, yymsp[0].minor.yy285.type, &yymsp[0].minor.yy285.val); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 72: /* alter_db_options ::= alter_db_options alter_db_option */
{ yylhsminor.yy360 = setDatabaseOption(pCxt, yymsp[-1].minor.yy360, yymsp[0].minor.yy285.type, &yymsp[0].minor.yy285.val); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 73: /* alter_db_option ::= BLOCKS NK_INTEGER */
{ yymsp[-1].minor.yy285.type = DB_OPTION_BLOCKS; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 74: /* alter_db_option ::= FSYNC NK_INTEGER */
{ yymsp[-1].minor.yy285.type = DB_OPTION_FSYNC; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 75: /* alter_db_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy285.type = DB_OPTION_KEEP; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 76: /* alter_db_option ::= WAL NK_INTEGER */
{ yymsp[-1].minor.yy285.type = DB_OPTION_WAL; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 77: /* alter_db_option ::= QUORUM NK_INTEGER */
{ yymsp[-1].minor.yy285.type = DB_OPTION_QUORUM; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 78: /* alter_db_option ::= CACHELAST NK_INTEGER */
{ yymsp[-1].minor.yy285.type = DB_OPTION_CACHELAST; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 79: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 81: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==81);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy345, yymsp[-5].minor.yy360, yymsp[-3].minor.yy24, yymsp[-1].minor.yy24, yymsp[0].minor.yy360); }
        break;
      case 80: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy24); }
        break;
      case 82: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy24); }
        break;
      case 83: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy345, yymsp[0].minor.yy360); }
        break;
      case 84: /* cmd ::= ALTER TABLE alter_table_clause */
      case 85: /* cmd ::= ALTER STABLE alter_table_clause */ yytestcase(yyruleno==85);
      case 184: /* cmd ::= query_expression */ yytestcase(yyruleno==184);
{ pCxt->pRootNode = yymsp[0].minor.yy360; }
        break;
      case 86: /* alter_table_clause ::= full_table_name alter_table_options */
{ yylhsminor.yy360 = createAlterTableOption(pCxt, yymsp[-1].minor.yy360, yymsp[0].minor.yy360); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 87: /* alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
{ yylhsminor.yy360 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy360, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[-1].minor.yy417, yymsp[0].minor.yy400); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 88: /* alter_table_clause ::= full_table_name DROP COLUMN column_name */
{ yylhsminor.yy360 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy360, TSDB_ALTER_TABLE_DROP_COLUMN, &yymsp[0].minor.yy417); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 89: /* alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
{ yylhsminor.yy360 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy360, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, &yymsp[-1].minor.yy417, yymsp[0].minor.yy400); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 90: /* alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
{ yylhsminor.yy360 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy360, TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, &yymsp[-1].minor.yy417, &yymsp[0].minor.yy417); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 91: /* alter_table_clause ::= full_table_name ADD TAG column_name type_name */
{ yylhsminor.yy360 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy360, TSDB_ALTER_TABLE_ADD_TAG, &yymsp[-1].minor.yy417, yymsp[0].minor.yy400); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 92: /* alter_table_clause ::= full_table_name DROP TAG column_name */
{ yylhsminor.yy360 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy360, TSDB_ALTER_TABLE_DROP_TAG, &yymsp[0].minor.yy417); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 93: /* alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
{ yylhsminor.yy360 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy360, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, &yymsp[-1].minor.yy417, yymsp[0].minor.yy400); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 94: /* alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
{ yylhsminor.yy360 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy360, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, &yymsp[-1].minor.yy417, &yymsp[0].minor.yy417); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 95: /* alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
{ yylhsminor.yy360 = createAlterTableSetTag(pCxt, yymsp[-5].minor.yy360, &yymsp[-2].minor.yy417, yymsp[0].minor.yy360); }
  yymsp[-5].minor.yy360 = yylhsminor.yy360;
        break;
      case 96: /* multi_create_clause ::= create_subtable_clause */
      case 99: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==99);
      case 106: /* column_def_list ::= column_def */ yytestcase(yyruleno==106);
      case 147: /* col_name_list ::= col_name */ yytestcase(yyruleno==147);
      case 169: /* func_name_list ::= func_name */ yytestcase(yyruleno==169);
      case 178: /* func_list ::= func */ yytestcase(yyruleno==178);
      case 270: /* select_sublist ::= select_item */ yytestcase(yyruleno==270);
      case 317: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==317);
{ yylhsminor.yy24 = createNodeList(pCxt, yymsp[0].minor.yy360); }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 97: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 100: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==100);
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-1].minor.yy24, yymsp[0].minor.yy360); }
  yymsp[-1].minor.yy24 = yylhsminor.yy24;
        break;
      case 98: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy360 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy345, yymsp[-7].minor.yy360, yymsp[-5].minor.yy360, yymsp[-4].minor.yy24, yymsp[-1].minor.yy24); }
  yymsp[-8].minor.yy360 = yylhsminor.yy360;
        break;
      case 101: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy360 = createDropTableClause(pCxt, yymsp[-1].minor.yy345, yymsp[0].minor.yy360); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 102: /* specific_tags_opt ::= */
      case 133: /* tags_def_opt ::= */ yytestcase(yyruleno==133);
      case 278: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==278);
      case 295: /* group_by_clause_opt ::= */ yytestcase(yyruleno==295);
      case 305: /* order_by_clause_opt ::= */ yytestcase(yyruleno==305);
{ yymsp[1].minor.yy24 = NULL; }
        break;
      case 103: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy24 = yymsp[-1].minor.yy24; }
        break;
      case 104: /* full_table_name ::= table_name */
{ yylhsminor.yy360 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy417, NULL); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 105: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy360 = createRealTableNode(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy417, NULL); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 107: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 148: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==148);
      case 170: /* func_name_list ::= func_name_list NK_COMMA col_name */ yytestcase(yyruleno==170);
      case 179: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==179);
      case 271: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==271);
      case 318: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==318);
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-2].minor.yy24, yymsp[0].minor.yy360); }
  yymsp[-2].minor.yy24 = yylhsminor.yy24;
        break;
      case 108: /* column_def ::= column_name type_name */
{ yylhsminor.yy360 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy417, yymsp[0].minor.yy400, NULL); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 109: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy360 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy417, yymsp[-2].minor.yy400, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 110: /* type_name ::= BOOL */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 111: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 112: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 113: /* type_name ::= INT */
      case 114: /* type_name ::= INTEGER */ yytestcase(yyruleno==114);
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 115: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 116: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 117: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 118: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy400 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 119: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 120: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy400 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 121: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy400 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 122: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy400 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 123: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy400 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 124: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy400 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 125: /* type_name ::= JSON */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 126: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy400 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 127: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 128: /* type_name ::= BLOB */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 129: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy400 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 130: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy400 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 131: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy400 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 132: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy400 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 134: /* tags_def_opt ::= tags_def */
      case 269: /* select_list ::= select_sublist */ yytestcase(yyruleno==269);
{ yylhsminor.yy24 = yymsp[0].minor.yy24; }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 135: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy24 = yymsp[-1].minor.yy24; }
        break;
      case 136: /* table_options ::= */
{ yymsp[1].minor.yy360 = createDefaultTableOptions(pCxt); }
        break;
      case 137: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy360 = setTableOption(pCxt, yymsp[-2].minor.yy360, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 138: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy360 = setTableOption(pCxt, yymsp[-2].minor.yy360, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 139: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy360 = setTableOption(pCxt, yymsp[-2].minor.yy360, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 140: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy360 = setTableSmaOption(pCxt, yymsp[-4].minor.yy360, yymsp[-1].minor.yy24); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 141: /* table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
{ yylhsminor.yy360 = setTableRollupOption(pCxt, yymsp[-4].minor.yy360, yymsp[-1].minor.yy24); }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 142: /* alter_table_options ::= alter_table_option */
{ yylhsminor.yy360 = createDefaultAlterTableOptions(pCxt); yylhsminor.yy360 = setTableOption(pCxt, yylhsminor.yy360, yymsp[0].minor.yy285.type, &yymsp[0].minor.yy285.val); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 143: /* alter_table_options ::= alter_table_options alter_table_option */
{ yylhsminor.yy360 = setTableOption(pCxt, yymsp[-1].minor.yy360, yymsp[0].minor.yy285.type, &yymsp[0].minor.yy285.val); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 144: /* alter_table_option ::= COMMENT NK_STRING */
{ yymsp[-1].minor.yy285.type = TABLE_OPTION_COMMENT; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 145: /* alter_table_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy285.type = TABLE_OPTION_KEEP; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 146: /* alter_table_option ::= TTL NK_INTEGER */
{ yymsp[-1].minor.yy285.type = TABLE_OPTION_TTL; yymsp[-1].minor.yy285.val = yymsp[0].minor.yy0; }
        break;
      case 149: /* col_name ::= column_name */
{ yylhsminor.yy360 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy417); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 150: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL, NULL); }
        break;
      case 151: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL, NULL); }
        break;
      case 152: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL, NULL); }
        break;
      case 153: /* cmd ::= SHOW db_name_cond_opt TABLES like_pattern_opt */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, yymsp[-2].minor.yy360, yymsp[0].minor.yy360); }
        break;
      case 154: /* cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, yymsp[-2].minor.yy360, yymsp[0].minor.yy360); }
        break;
      case 155: /* cmd ::= SHOW db_name_cond_opt VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, yymsp[-1].minor.yy360, NULL); }
        break;
      case 156: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL, NULL); }
        break;
      case 157: /* cmd ::= SHOW MODULES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MODULES_STMT, NULL, NULL); }
        break;
      case 158: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL, NULL); }
        break;
      case 159: /* cmd ::= SHOW FUNCTIONS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_FUNCTIONS_STMT, NULL, NULL); }
        break;
      case 160: /* cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, yymsp[-1].minor.yy360, yymsp[0].minor.yy360); }
        break;
      case 161: /* cmd ::= SHOW STREAMS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STREAMS_STMT, NULL, NULL); }
        break;
      case 162: /* db_name_cond_opt ::= */
      case 167: /* from_db_opt ::= */ yytestcase(yyruleno==167);
{ yymsp[1].minor.yy360 = createDefaultDatabaseCondValue(pCxt); }
        break;
      case 163: /* db_name_cond_opt ::= db_name NK_DOT */
{ yylhsminor.yy360 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy417); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 164: /* like_pattern_opt ::= */
      case 175: /* index_options ::= */ yytestcase(yyruleno==175);
      case 276: /* where_clause_opt ::= */ yytestcase(yyruleno==276);
      case 280: /* twindow_clause_opt ::= */ yytestcase(yyruleno==280);
      case 285: /* sliding_opt ::= */ yytestcase(yyruleno==285);
      case 287: /* fill_opt ::= */ yytestcase(yyruleno==287);
      case 299: /* having_clause_opt ::= */ yytestcase(yyruleno==299);
      case 307: /* slimit_clause_opt ::= */ yytestcase(yyruleno==307);
      case 311: /* limit_clause_opt ::= */ yytestcase(yyruleno==311);
{ yymsp[1].minor.yy360 = NULL; }
        break;
      case 165: /* like_pattern_opt ::= LIKE NK_STRING */
{ yymsp[-1].minor.yy360 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0); }
        break;
      case 166: /* table_name_cond ::= table_name */
{ yylhsminor.yy360 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy417); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 168: /* from_db_opt ::= FROM db_name */
{ yymsp[-1].minor.yy360 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy417); }
        break;
      case 171: /* func_name ::= function_name */
{ yylhsminor.yy360 = createFunctionNode(pCxt, &yymsp[0].minor.yy417, NULL); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 172: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy417, &yymsp[-1].minor.yy417, NULL, yymsp[0].minor.yy360); }
        break;
      case 173: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy417, &yymsp[-3].minor.yy417, yymsp[-1].minor.yy24, NULL); }
        break;
      case 174: /* cmd ::= DROP INDEX index_name ON table_name */
{ pCxt->pRootNode = createDropIndexStmt(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy417); }
        break;
      case 176: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy360 = createIndexOption(pCxt, yymsp[-6].minor.yy24, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), NULL, yymsp[0].minor.yy360); }
        break;
      case 177: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy360 = createIndexOption(pCxt, yymsp[-8].minor.yy24, releaseRawExprNode(pCxt, yymsp[-4].minor.yy360), releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), yymsp[0].minor.yy360); }
        break;
      case 180: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy360 = createFunctionNode(pCxt, &yymsp[-3].minor.yy417, yymsp[-1].minor.yy24); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 181: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy345, &yymsp[-2].minor.yy417, yymsp[0].minor.yy360, NULL); }
        break;
      case 182: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy345, &yymsp[-2].minor.yy417, NULL, &yymsp[0].minor.yy417); }
        break;
      case 183: /* cmd ::= DROP TOPIC exists_opt topic_name */
{ pCxt->pRootNode = createDropTopicStmt(pCxt, yymsp[-1].minor.yy345, &yymsp[0].minor.yy417); }
        break;
      case 185: /* literal ::= NK_INTEGER */
{ yylhsminor.yy360 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 186: /* literal ::= NK_FLOAT */
{ yylhsminor.yy360 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 187: /* literal ::= NK_STRING */
{ yylhsminor.yy360 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 188: /* literal ::= NK_BOOL */
{ yylhsminor.yy360 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 189: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 190: /* literal ::= duration_literal */
      case 203: /* expression ::= literal */ yytestcase(yyruleno==203);
      case 204: /* expression ::= column_reference */ yytestcase(yyruleno==204);
      case 207: /* expression ::= subquery */ yytestcase(yyruleno==207);
      case 239: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==239);
      case 243: /* boolean_primary ::= predicate */ yytestcase(yyruleno==243);
      case 245: /* common_expression ::= expression */ yytestcase(yyruleno==245);
      case 246: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==246);
      case 248: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==248);
      case 250: /* table_reference ::= table_primary */ yytestcase(yyruleno==250);
      case 251: /* table_reference ::= joined_table */ yytestcase(yyruleno==251);
      case 255: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==255);
      case 302: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==302);
      case 304: /* query_primary ::= query_specification */ yytestcase(yyruleno==304);
{ yylhsminor.yy360 = yymsp[0].minor.yy360; }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 191: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy360 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 192: /* literal_list ::= literal */
      case 216: /* expression_list ::= expression */ yytestcase(yyruleno==216);
{ yylhsminor.yy24 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy360)); }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 193: /* literal_list ::= literal_list NK_COMMA literal */
      case 217: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==217);
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-2].minor.yy24, releaseRawExprNode(pCxt, yymsp[0].minor.yy360)); }
  yymsp[-2].minor.yy24 = yylhsminor.yy24;
        break;
      case 205: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy417, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy417, yymsp[-1].minor.yy24)); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 206: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy417, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy417, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 208: /* expression ::= NK_LP expression NK_RP */
      case 244: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==244);
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy360)); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 209: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy360));
                                                                                  }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 210: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy360), NULL));
                                                                                  }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 211: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); 
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 212: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); 
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 213: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); 
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 214: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); 
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 215: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); 
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 218: /* column_reference ::= column_name */
{ yylhsminor.yy360 = createRawExprNode(pCxt, &yymsp[0].minor.yy417, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy417)); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 219: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy417, createColumnNode(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy417)); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 220: /* predicate ::= expression compare_op expression */
      case 225: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==225);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy252, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360)));
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 221: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy360), releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360)));
                                                                                  }
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 222: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[-5].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360)));
                                                                                  }
  yymsp[-5].minor.yy360 = yylhsminor.yy360;
        break;
      case 223: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), NULL));
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 224: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy360), NULL));
                                                                                  }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 226: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy252 = OP_TYPE_LOWER_THAN; }
        break;
      case 227: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy252 = OP_TYPE_GREATER_THAN; }
        break;
      case 228: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy252 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 229: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy252 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 230: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy252 = OP_TYPE_NOT_EQUAL; }
        break;
      case 231: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy252 = OP_TYPE_EQUAL; }
        break;
      case 232: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy252 = OP_TYPE_LIKE; }
        break;
      case 233: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy252 = OP_TYPE_NOT_LIKE; }
        break;
      case 234: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy252 = OP_TYPE_MATCH; }
        break;
      case 235: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy252 = OP_TYPE_NMATCH; }
        break;
      case 236: /* in_op ::= IN */
{ yymsp[0].minor.yy252 = OP_TYPE_IN; }
        break;
      case 237: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy252 = OP_TYPE_NOT_IN; }
        break;
      case 238: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy24)); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 240: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy360), NULL));
                                                                                  }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 241: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360)));
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 242: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy360);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), releaseRawExprNode(pCxt, yymsp[0].minor.yy360)));
                                                                                  }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 247: /* from_clause ::= FROM table_reference_list */
      case 277: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==277);
      case 300: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==300);
{ yymsp[-1].minor.yy360 = yymsp[0].minor.yy360; }
        break;
      case 249: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy360 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy360, yymsp[0].minor.yy360, NULL); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 252: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy360 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy417, &yymsp[0].minor.yy417); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 253: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy360 = createRealTableNode(pCxt, &yymsp[-3].minor.yy417, &yymsp[-1].minor.yy417, &yymsp[0].minor.yy417); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 254: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy360 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy360), &yymsp[0].minor.yy417); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 256: /* alias_opt ::= */
{ yymsp[1].minor.yy417 = nil_token;  }
        break;
      case 257: /* alias_opt ::= table_alias */
{ yylhsminor.yy417 = yymsp[0].minor.yy417; }
  yymsp[0].minor.yy417 = yylhsminor.yy417;
        break;
      case 258: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy417 = yymsp[0].minor.yy417; }
        break;
      case 259: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 260: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==260);
{ yymsp[-2].minor.yy360 = yymsp[-1].minor.yy360; }
        break;
      case 261: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy360 = createJoinTableNode(pCxt, yymsp[-4].minor.yy84, yymsp[-5].minor.yy360, yymsp[-2].minor.yy360, yymsp[0].minor.yy360); }
  yymsp[-5].minor.yy360 = yylhsminor.yy360;
        break;
      case 262: /* join_type ::= */
{ yymsp[1].minor.yy84 = JOIN_TYPE_INNER; }
        break;
      case 263: /* join_type ::= INNER */
{ yymsp[0].minor.yy84 = JOIN_TYPE_INNER; }
        break;
      case 264: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy360 = createSelectStmt(pCxt, yymsp[-7].minor.yy345, yymsp[-6].minor.yy24, yymsp[-5].minor.yy360);
                                                                                    yymsp[-8].minor.yy360 = addWhereClause(pCxt, yymsp[-8].minor.yy360, yymsp[-4].minor.yy360);
                                                                                    yymsp[-8].minor.yy360 = addPartitionByClause(pCxt, yymsp[-8].minor.yy360, yymsp[-3].minor.yy24);
                                                                                    yymsp[-8].minor.yy360 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy360, yymsp[-2].minor.yy360);
                                                                                    yymsp[-8].minor.yy360 = addGroupByClause(pCxt, yymsp[-8].minor.yy360, yymsp[-1].minor.yy24);
                                                                                    yymsp[-8].minor.yy360 = addHavingClause(pCxt, yymsp[-8].minor.yy360, yymsp[0].minor.yy360);
                                                                                  }
        break;
      case 266: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy345 = true; }
        break;
      case 267: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy345 = false; }
        break;
      case 268: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy24 = NULL; }
        break;
      case 272: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy360);
                                                                                    yylhsminor.yy360 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy360), &t);
                                                                                  }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 273: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy360 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy360), &yymsp[0].minor.yy417); }
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 274: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy360 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), &yymsp[0].minor.yy417); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 275: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy360 = createColumnNode(pCxt, &yymsp[-2].minor.yy417, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 279: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 296: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==296);
      case 306: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==306);
{ yymsp[-2].minor.yy24 = yymsp[0].minor.yy24; }
        break;
      case 281: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy360 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy360), &yymsp[-1].minor.yy0); }
        break;
      case 282: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy360 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy360)); }
        break;
      case 283: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy360 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy360), NULL, yymsp[-1].minor.yy360, yymsp[0].minor.yy360); }
        break;
      case 284: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy360 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy360), releaseRawExprNode(pCxt, yymsp[-3].minor.yy360), yymsp[-1].minor.yy360, yymsp[0].minor.yy360); }
        break;
      case 286: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy360 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy360); }
        break;
      case 288: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy360 = createFillNode(pCxt, yymsp[-1].minor.yy358, NULL); }
        break;
      case 289: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy360 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy24)); }
        break;
      case 290: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy358 = FILL_MODE_NONE; }
        break;
      case 291: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy358 = FILL_MODE_PREV; }
        break;
      case 292: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy358 = FILL_MODE_NULL; }
        break;
      case 293: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy358 = FILL_MODE_LINEAR; }
        break;
      case 294: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy358 = FILL_MODE_NEXT; }
        break;
      case 297: /* group_by_list ::= expression */
{ yylhsminor.yy24 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); }
  yymsp[0].minor.yy24 = yylhsminor.yy24;
        break;
      case 298: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy24 = addNodeToList(pCxt, yymsp[-2].minor.yy24, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy360))); }
  yymsp[-2].minor.yy24 = yylhsminor.yy24;
        break;
      case 301: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy360 = addOrderByClause(pCxt, yymsp[-3].minor.yy360, yymsp[-2].minor.yy24);
                                                                                    yylhsminor.yy360 = addSlimitClause(pCxt, yylhsminor.yy360, yymsp[-1].minor.yy360);
                                                                                    yylhsminor.yy360 = addLimitClause(pCxt, yylhsminor.yy360, yymsp[0].minor.yy360);
                                                                                  }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 303: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy360 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy360, yymsp[0].minor.yy360); }
  yymsp[-3].minor.yy360 = yylhsminor.yy360;
        break;
      case 308: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 312: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==312);
{ yymsp[-1].minor.yy360 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 309: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 313: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==313);
{ yymsp[-3].minor.yy360 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 310: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 314: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==314);
{ yymsp[-3].minor.yy360 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 315: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy360 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy360); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 316: /* search_condition ::= common_expression */
{ yylhsminor.yy360 = releaseRawExprNode(pCxt, yymsp[0].minor.yy360); }
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 319: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy360 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy360), yymsp[-1].minor.yy130, yymsp[0].minor.yy73); }
  yymsp[-2].minor.yy360 = yylhsminor.yy360;
        break;
      case 320: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy130 = ORDER_ASC; }
        break;
      case 321: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy130 = ORDER_ASC; }
        break;
      case 322: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy130 = ORDER_DESC; }
        break;
      case 323: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy73 = NULL_ORDER_DEFAULT; }
        break;
      case 324: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy73 = NULL_ORDER_FIRST; }
        break;
      case 325: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy73 = NULL_ORDER_LAST; }
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
