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
#define YYNSTATE             403
#define YYNRULE              323
#define YYNTOKEN             205
#define YY_MAX_SHIFT         402
#define YY_MIN_SHIFTREDUCE   631
#define YY_MAX_SHIFTREDUCE   953
#define YY_ERROR_ACTION      954
#define YY_ACCEPT_ACTION     955
#define YY_NO_ACTION         956
#define YY_MIN_REDUCE        957
#define YY_MAX_REDUCE        1279
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
#define YY_ACTTAB_COUNT (886)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */  1194,  682, 1195,  318,  322,  101,  267,  100,  888,  683,
 /*    10 */   891,  682,  955,  402,   38,   39, 1142,   42,   43,  683,
 /*    20 */    86,  269,   31,   30,   29,  164,   24,   41,  352,   46,
 /*    30 */    44,   47,   45,   32,  401,  247, 1253,   37,   36,  219,
 /*    40 */   106,   35,   34,   33,   38,   39,  885,   42,   43, 1253,
 /*    50 */   179,  269,   31,   30,   29, 1099,  682,   41,  352,   46,
 /*    60 */    44,   47,   45,   32,  683, 1133, 1133,   37,   36,  308,
 /*    70 */   220,   35,   34,   33, 1097, 1098,   56, 1101,   38,   39,
 /*    80 */  1253,   42,   43,  292,  250,  269,   31,   30,   29, 1139,
 /*    90 */    89,   41,  352,   46,   44,   47,   45,   32,  862,  299,
 /*   100 */   298,   37,   36,  225,  348,   35,   34,   33,   38,   39,
 /*   110 */   256,   42,   43, 1253,  387,  269,   31,   30,   29,  279,
 /*   120 */    59,   41,  352,   46,   44,   47,   45,   32,  768,  896,
 /*   130 */   187,   37,   36,  109,  720,   35,   34,   33,  254,   38,
 /*   140 */    40,   52,   42,   43,   13, 1118,  269,   31,   30,   29,
 /*   150 */  1117,  882,   41,  352,   46,   44,   47,   45,   32,  861,
 /*   160 */   377,  376,   37,   36,  226,  682,   35,   34,   33,   39,
 /*   170 */  1102,   42,   43,  683, 1253,  269,   31,   30,   29,  108,
 /*   180 */    87,   41,  352,   46,   44,   47,   45,   32,  400,  398,
 /*   190 */   659,   37,   36,   95, 1275,   35,   34,   33,   68,  346,
 /*   200 */   394,  393,  345,  344,  343,  392,  342,  341,  340,  391,
 /*   210 */   339,  390,  389,  632,  633,  634,  635,  636,  637,  638,
 /*   220 */   639,  640,  641,  642,  643,  644,  645,  161, 1192,  248,
 /*   230 */  1193,   42,   43, 1267,   69,  269,   31,   30,   29,   60,
 /*   240 */   284,   41,  352,   46,   44,   47,   45,   32,  266,  288,
 /*   250 */   287,   37,   36,  300, 1108,   35,   34,   33, 1076, 1064,
 /*   260 */  1065, 1066, 1067, 1068, 1069, 1070, 1071, 1072, 1073, 1074,
 /*   270 */  1075, 1077, 1078,   25,   46,   44,   47,   45,   32,   35,
 /*   280 */    34,   33,   37,   36,  272,  249,   35,   34,   33,  301,
 /*   290 */   231,   32, 1115,  395, 1045,   37,   36,  233, 1205,   35,
 /*   300 */    34,   33,  262,  148,  147,  146,  232,  182,  241,  898,
 /*   310 */   360,   95,  886,  217,  889,  887,  892,  890,  241,  898,
 /*   320 */   227,  221,  886, 1253,  889, 1256,  892,    5,   63,  192,
 /*   330 */  1253, 1253,  279, 1255,  191,  115,  120,  111,  119,  278,
 /*   340 */   132,  126,  137,  189,  245,  246,  279,  136,  354,  142,
 /*   350 */   145,  135,   69,  334,  245,  246,  796,  353,  139,  793,
 /*   360 */   814,  794,  265,  795,  830,  273,  238,  271,  833,  363,
 /*   370 */   362,  212,  210,  208, 1100,  291, 1253,   85,  207,  152,
 /*   380 */   151,  150,  149,  270,  242,  842,  843,   68,  261,  394,
 /*   390 */   393,  274,  275,   48,  392, 1118,  304,  305,  391,  104,
 /*   400 */   390,  389, 1084,   48, 1082, 1083,   37,   36,  351, 1085,
 /*   410 */    35,   34,   33, 1086,   90, 1087, 1088,   60,   60,   60,
 /*   420 */   280,   60,  277,   60,  372,  371,  811,  221, 1244,   60,
 /*   430 */   350,  899,  893,  895,   60,   60,  897, 1253, 1253, 1256,
 /*   440 */    60,  899,  893,  895,   60,   60,  263,  268,  221,  818,
 /*   450 */   160,  158,  157, 1118,  289,   10,  894,  279, 1253, 1133,
 /*   460 */  1256,   84,  348,  259,  260,  364,  894,  365, 1116,    6,
 /*   470 */  1115, 1115, 1115, 1243, 1115,  366, 1114,  251,  797,  276,
 /*   480 */   367,  373, 1115, 1253,  252, 1242,  374, 1115, 1115,  357,
 /*   490 */   375,  379,  134, 1115,  767, 1253,  243, 1115, 1115,  244,
 /*   500 */   223,  293,  224,  228,  222,  387, 1253, 1005,  229, 1253,
 /*   510 */  1253,   92, 1253, 1253, 1253,  202,   61, 1015, 1253,  230,
 /*   520 */   235,  236,  237,  234,  218,  202,    1,  190, 1006, 1253,
 /*   530 */  1253, 1253, 1253, 1253, 1253,  103,  202,  102,    3,  203,
 /*   540 */    93,  303,  302,  839,  849,  850,  350,   77,   80,  356,
 /*   550 */   778,  326,  295,  780,  328,  779,   55,  378,  166,   72,
 /*   560 */    49,  928,  900,  321,  681,   61,   61, 1204,   72,  107,
 /*   570 */    72,  355,   15,  125,   14,  124,    9,    9,  257,    9,
 /*   580 */    17,  295,   16,  803, 1201,  804,  801,   19,  802,   18,
 /*   590 */   329,   81,   78,  369,  368, 1200,  131,  903,  130,  144,
 /*   600 */   143,   21,  258,   20,  162,  163, 1113, 1141, 1152,   26,
 /*   610 */  1149, 1150, 1154,  165, 1134,  296,  170,  314,  159,  183,
 /*   620 */  1184, 1183, 1182, 1181,  181, 1109,  294, 1111, 1107,  184,
 /*   630 */   185, 1020,  829,  331,  307,  332,  333,  337,  338,   70,
 /*   640 */   215,  253,   66,  349, 1014,  361, 1274,  122, 1273, 1270,
 /*   650 */   323,  193,  370,   82,  309,  311, 1266,  128, 1265, 1262,
 /*   660 */   194, 1131, 1042,   67,   79,  171,  172,   62,   71,  216,
 /*   670 */  1002,   28,  319,  138,  173,  317,  315, 1000,  174,  140,
 /*   680 */   141,  313,  177,  175,  998,  176,  997,  281,  205,  206,
 /*   690 */   994,  993,  992,  991,  990,  989,  988,  209,  211,  980,
 /*   700 */   213,  977,  214,  973,  310,  306,   91,   96,  312,   27,
 /*   710 */    88,  336,  335,   83,  388,  133,  380,  264,  330,  381,
 /*   720 */   382,  383,  384,  385,  386,  396,  239,  953,  283,  240,
 /*   730 */   282, 1019,  186,  952, 1018,  188,  116,  117,  285,  286,
 /*   740 */   951,  934,  933,  290,  996,   11,  995,  325,  153,  987,
 /*   750 */   197,  196, 1043,  200,  195,  154,  198,  199,  201,  155,
 /*   760 */   324,  986, 1080,  156,    2, 1044,  806,   54,  178,  979,
 /*   770 */   180,  295,  978,   94,   53,    4,  297,   97,  838, 1090,
 /*   780 */    75,  836,  835,  832,  831,   76,  169,  840,  167,  255,
 /*   790 */   851,  168,   64,  845,   98,  355,  847,   99,  316,  320,
 /*   800 */    22,   23,   12,   65,  105,   50,  327,   51,  108,  110,
 /*   810 */   113,   57,  696,  698,  112,  733,   58,  731,  730,  114,
 /*   820 */   729,  727,  726,  725,  722,  347,  686,  118,    7,  925,
 /*   830 */   923,  902,  926,  901,  924,    8,  359,  904,  358,   73,
 /*   840 */    61,  121,  770,  800,  799,  123,   74,  769,  127,  129,
 /*   850 */   766,  714,  712,  704,  710,  706,  708,  702,  700,  736,
 /*   860 */   735,  734,  732,  728,  724,  723,  204,  397,  649,  684,
 /*   870 */   658,  656,  957,  956,  956,  956,  956,  956,  956,  956,
 /*   880 */   956,  956,  956,  956,  956,  399,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   288,    1,  290,  291,  287,  288,    1,  290,    5,    9,
 /*    10 */     7,    1,  206,  207,   14,   15,  208,   17,   18,    9,
 /*    20 */   216,   21,   22,   23,   24,  208,  280,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,  208,  209,  290,   37,   38,  280,
 /*    40 */   216,   41,   42,   43,   14,   15,   41,   17,   18,  290,
 /*    50 */   267,   21,   22,   23,   24,  251,    1,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,    9,  259,  259,   37,   38,  286,
 /*    70 */   280,   41,   42,   43,  250,  251,  252,  253,   14,   15,
 /*    80 */   290,   17,   18,  277,  277,   21,   22,   23,   24,  281,
 /*    90 */    90,   27,   28,   29,   30,   31,   32,   33,   80,  282,
 /*   100 */   283,   37,   38,  280,   88,   41,   42,   43,   14,   15,
 /*   110 */     1,   17,   18,  290,   95,   21,   22,   23,   24,  208,
 /*   120 */    90,   27,   28,   29,   30,   31,   32,   33,    5,  126,
 /*   130 */   219,   37,   38,  216,    5,   41,   42,   43,  255,   14,
 /*   140 */    15,   86,   17,   18,   86,  262,   21,   22,   23,   24,
 /*   150 */   262,   87,   27,   28,   29,   30,   31,   32,   33,  141,
 /*   160 */    37,   38,   37,   38,  280,    1,   41,   42,   43,   15,
 /*   170 */   253,   17,   18,    9,  290,   21,   22,   23,   24,  121,
 /*   180 */   122,   27,   28,   29,   30,   31,   32,   33,   69,   70,
 /*   190 */    71,   37,   38,   86,  262,   41,   42,   43,  103,  104,
 /*   200 */   105,  106,  107,  108,  109,  110,  111,  112,  113,  114,
 /*   210 */   115,  116,  117,   49,   50,   51,   52,   53,   54,   55,
 /*   220 */    56,   57,   58,   59,   60,   61,   62,   63,  288,   65,
 /*   230 */   290,   17,   18,  262,  127,   21,   22,   23,   24,  208,
 /*   240 */   151,   27,   28,   29,   30,   31,   32,   33,  215,  160,
 /*   250 */   161,   37,   38,  285,  208,   41,   42,   43,  233,  234,
 /*   260 */   235,  236,  237,  238,  239,  240,  241,  242,  243,  244,
 /*   270 */   245,  246,  247,   48,   29,   30,   31,   32,   33,   41,
 /*   280 */    42,   43,   37,   38,   72,  254,   41,   42,   43,  285,
 /*   290 */    65,   33,  261,  231,  232,   37,   38,   72,  249,   41,
 /*   300 */    42,   43,  256,   78,   79,   80,   81,  257,    1,    2,
 /*   310 */    85,   86,    5,  280,    7,    5,    9,    7,    1,    2,
 /*   320 */   280,  280,    5,  290,    7,  292,    9,   66,   67,   68,
 /*   330 */   290,  290,  208,  292,   73,   74,   75,   76,   77,   72,
 /*   340 */    66,   67,   68,  219,   37,   38,  208,   73,   41,   75,
 /*   350 */    76,   77,  127,   92,   37,   38,    2,  219,   84,    5,
 /*   360 */    41,    7,  215,    9,    5,  153,  280,  155,    9,  157,
 /*   370 */   158,   66,   67,   68,    0,  150,  290,  152,   73,   74,
 /*   380 */    75,   76,   77,  215,  159,  133,  134,  103,  255,  105,
 /*   390 */   106,   37,   38,   86,  110,  262,   37,   38,  114,  263,
 /*   400 */   116,  117,  233,   86,  235,  236,   37,   38,   25,  240,
 /*   410 */    41,   42,   43,  244,  278,  246,  247,  208,  208,  208,
 /*   420 */   153,  208,  155,  208,  157,  158,  102,  280,  280,  208,
 /*   430 */    47,  124,  125,  126,  208,  208,  126,  290,  290,  292,
 /*   440 */   208,  124,  125,  126,  208,  208,  255,   64,  280,  130,
 /*   450 */    66,   67,   68,  262,  208,  131,  149,  208,  290,  259,
 /*   460 */   292,   86,   88,  254,  254,  254,  149,  254,  219,   86,
 /*   470 */   261,  261,  261,  280,  261,  254,  261,  277,  124,  125,
 /*   480 */   254,  254,  261,  290,  125,  280,  254,  261,  261,   16,
 /*   490 */   254,  254,   82,  261,  119,  290,  280,  261,  261,  280,
 /*   500 */   280,   87,  280,  280,  280,   95,  290,  214,  280,  290,
 /*   510 */   290,   87,  290,  290,  290,  222,  102,  214,  290,  280,
 /*   520 */   280,  280,  280,  280,  280,  222,  217,  218,  214,  290,
 /*   530 */   290,  290,  290,  290,  290,  288,  222,  290,  212,  213,
 /*   540 */    87,   37,   38,   87,   87,   87,   47,  102,  102,   25,
 /*   550 */    87,   87,  128,   87,   87,   87,   86,  249,  102,  102,
 /*   560 */   102,   87,   87,   64,   87,  102,  102,  249,  102,  102,
 /*   570 */   102,   47,  154,  154,  156,  156,  102,  102,  249,  102,
 /*   580 */   154,  128,  156,    5,  249,    7,    5,  154,    7,  156,
 /*   590 */   120,  145,  147,   37,   38,  249,  154,  124,  156,   82,
 /*   600 */    83,  154,  249,  156,  208,  208,  208,  208,  208,  279,
 /*   610 */   208,  208,  208,  208,  259,  259,  208,  208,   64,  208,
 /*   620 */   289,  289,  289,  289,  264,  259,  210,  210,  208,  208,
 /*   630 */   208,  208,  126,  208,  284,  208,  208,  208,  208,  208,
 /*   640 */   208,  284,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   650 */   139,  208,  208,  144,  284,  284,  208,  208,  208,  208,
 /*   660 */   208,  276,  208,  208,  146,  275,  274,  208,  208,  208,
 /*   670 */   208,  143,  142,  208,  273,  137,  136,  208,  272,  208,
 /*   680 */   208,  135,  269,  271,  208,  270,  208,  208,  208,  208,
 /*   690 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   700 */   208,  208,  208,  208,  138,  132,  210,  210,  210,  148,
 /*   710 */   123,   94,   93,  210,  118,  101,  100,  210,  210,   55,
 /*   720 */    97,   99,   59,   98,   96,   88,  210,    5,    5,  210,
 /*   730 */   162,  221,  220,    5,  221,  220,  216,  216,  162,    5,
 /*   740 */     5,  105,  104,  151,  210,   86,  210,  120,  211,  210,
 /*   750 */   224,  228,  230,  226,  229,  211,  227,  225,  223,  211,
 /*   760 */   258,  210,  248,  211,  217,  232,   87,  266,  268,  210,
 /*   770 */   265,  128,  210,  129,   86,  212,  102,  102,   87,  248,
 /*   780 */   102,  126,  126,    5,    5,   86,  102,   87,   86,    1,
 /*   790 */    87,   86,  102,   87,   86,   47,   87,   86,   86,    1,
 /*   800 */   140,  140,   86,  102,   90,   86,  120,   86,  121,   82,
 /*   810 */    74,   91,    5,    5,   90,    9,   91,    5,    5,   90,
 /*   820 */     5,    5,    5,    5,    5,   16,   89,   82,   86,    9,
 /*   830 */     9,   87,    9,   87,    9,   86,   63,  124,   28,   17,
 /*   840 */   102,  156,    5,  126,  126,  156,   17,    5,  156,  156,
 /*   850 */    87,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   860 */     5,    5,    5,    5,    5,    5,  102,   22,   64,   89,
 /*   870 */     9,    9,    0,  293,  293,  293,  293,  293,  293,  293,
 /*   880 */   293,  293,  293,  293,  293,   22,  293,  293,  293,  293,
 /*   890 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   900 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   910 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
 /*   920 */   293,  293,  293,  293,  293,  293,  293,  293,  293,  293,
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
 /*  1090 */   293,
};
#define YY_SHIFT_COUNT    (402)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (872)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   225,   95,   95,  284,  284,   16,  307,  317,  317,  317,
 /*    10 */    55,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*    20 */    10,   10,  109,  109,    0,  164,  317,  317,  317,  317,
 /*    30 */   317,  317,  317,  317,  317,  317,  317,  317,  317,  317,
 /*    40 */   317,  317,  317,  317,  317,  317,  317,  317,  317,  354,
 /*    50 */   354,  354,  107,  107,  252,   10,  374,   10,   10,   10,
 /*    60 */    10,   10,  410,   16,  109,  109,   19,   19,  129,  886,
 /*    70 */   886,  886,  354,  354,  354,  359,  359,  123,  123,  123,
 /*    80 */   123,  123,  123,   58,  123,   10,   10,   10,   10,   10,
 /*    90 */   319,   10,   10,   10,  107,  107,   10,   10,   10,   10,
 /*   100 */    18,   18,   18,   18,  324,  107,   10,   10,   10,   10,
 /*   110 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   120 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   130 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   140 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   150 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   160 */    10,   10,  554,  554,  554,  554,  506,  506,  506,  506,
 /*   170 */   554,  509,  518,  511,  528,  530,  538,  540,  546,  566,
 /*   180 */   573,  561,  587,  554,  554,  554,  617,  619,  617,  619,
 /*   190 */   596,   16,   16,  554,  554,  614,  616,  664,  623,  622,
 /*   200 */   663,  625,  628,  596,  129,  554,  554,  637,  637,  554,
 /*   210 */   637,  554,  637,  554,  554,  886,  886,   30,   64,   94,
 /*   220 */    94,   94,  125,  154,  214,  245,  245,  245,  245,  245,
 /*   230 */   245,  261,  274,  305,  258,  258,  258,  258,  369,  212,
 /*   240 */   267,  383,   89,  238,  238,    3,  310,  119,  384,  414,
 /*   250 */   424,  453,  504,  456,  457,  458,  499,  445,  446,  463,
 /*   260 */   464,  466,  467,  468,  470,  474,  475,  524,    5,  473,
 /*   270 */   477,  418,  419,  426,  578,  581,  556,  433,  442,  375,
 /*   280 */   447,  517,  722,  568,  723,  728,  576,  734,  735,  636,
 /*   290 */   638,  592,  643,  627,  659,  644,  679,  688,  674,  675,
 /*   300 */   691,  678,  655,  656,  778,  779,  699,  700,  702,  703,
 /*   310 */   705,  706,  684,  708,  709,  711,  788,  712,  690,  660,
 /*   320 */   748,  798,  701,  661,  714,  716,  627,  719,  686,  721,
 /*   330 */   687,  727,  720,  724,  736,  807,  808,  725,  729,  806,
 /*   340 */   812,  813,  815,  816,  817,  818,  819,  737,  809,  745,
 /*   350 */   820,  821,  742,  744,  746,  823,  825,  713,  749,  810,
 /*   360 */   773,  822,  685,  689,  738,  738,  738,  738,  717,  718,
 /*   370 */   829,  692,  693,  738,  738,  738,  837,  842,  763,  738,
 /*   380 */   846,  847,  848,  849,  850,  851,  852,  853,  854,  855,
 /*   390 */   856,  857,  858,  859,  860,  764,  780,  861,  845,  862,
 /*   400 */   863,  804,  872,
};
#define YY_REDUCE_COUNT (216)
#define YY_REDUCE_MIN   (-288)
#define YY_REDUCE_MAX   (563)
static const short yy_reduce_ofst[] = {
 /*     0 */  -194,   25,   25,  169,  169, -176,   33,  147,  168,   41,
 /*    10 */  -183,   31,  209,  210,  211,  213,  221,  226,  227,  232,
 /*    20 */   236,  237, -288, -283, -192, -174, -254, -241, -210, -177,
 /*    30 */  -116,   40,   86,  148,  193,  205,  216,  219,  220,  222,
 /*    40 */   223,  224,  228,  239,  240,  241,  242,  243,  244, -117,
 /*    50 */   133,  191, -193,  200, -217,   46,  -83,  -89,  124,  138,
 /*    60 */   249,  215,  293, -196,  -60,  247,  303,  314,   62,  136,
 /*    70 */   309,  326, -112,  -68,  -29,  -32,    4,   49,  318,  329,
 /*    80 */   335,  346,  353,   50,  308,  246,  396,  397,  398,  399,
 /*    90 */   330,  400,  402,  403,  355,  356,  404,  405,  408,  409,
 /*   100 */   331,  332,  333,  334,  360,  366,  411,  420,  421,  422,
 /*   110 */   423,  425,  427,  428,  429,  430,  431,  432,  434,  435,
 /*   120 */   436,  437,  438,  439,  440,  441,  443,  444,  448,  449,
 /*   130 */   450,  451,  452,  454,  455,  459,  460,  461,  462,  465,
 /*   140 */   469,  471,  472,  476,  478,  479,  480,  481,  482,  483,
 /*   150 */   484,  485,  486,  487,  488,  489,  490,  491,  492,  493,
 /*   160 */   494,  495,  416,  417,  496,  497,  350,  357,  370,  371,
 /*   170 */   498,  385,  390,  392,  401,  406,  412,  415,  413,  500,
 /*   180 */   501,  505,  502,  503,  507,  508,  510,  512,  513,  515,
 /*   190 */   514,  520,  521,  516,  519,  522,  525,  523,  526,  529,
 /*   200 */   532,  527,  535,  531,  533,  534,  536,  537,  544,  539,
 /*   210 */   548,  551,  552,  559,  562,  547,  563,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   954, 1079, 1016, 1089, 1003, 1013, 1258, 1258, 1258, 1258,
 /*    10 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*    20 */   954,  954,  954,  954, 1143,  974,  954,  954,  954,  954,
 /*    30 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*    40 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*    50 */   954,  954,  954,  954, 1167,  954, 1013,  954,  954,  954,
 /*    60 */   954,  954, 1025, 1013,  954,  954, 1025, 1025,  954, 1138,
 /*    70 */  1063, 1081,  954,  954,  954,  954,  954,  954,  954,  954,
 /*    80 */   954,  954,  954, 1110,  954,  954,  954,  954,  954,  954,
 /*    90 */  1145, 1151, 1148,  954,  954,  954, 1153,  954,  954,  954,
 /*   100 */  1189, 1189, 1189, 1189, 1136,  954,  954,  954,  954,  954,
 /*   110 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   120 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   130 */   954,  954,  954,  954,  954,  954,  954,  954, 1001,  954,
 /*   140 */   999,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   150 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   160 */   954,  972,  976,  976,  976,  976,  954,  954,  954,  954,
 /*   170 */   976, 1198, 1202, 1179, 1196, 1190, 1174, 1172, 1170, 1178,
 /*   180 */  1163, 1206, 1112,  976,  976,  976, 1023, 1021, 1023, 1021,
 /*   190 */  1017, 1013, 1013,  976,  976, 1041, 1039, 1037, 1029, 1035,
 /*   200 */  1031, 1033, 1027, 1004,  954,  976,  976, 1011, 1011,  976,
 /*   210 */  1011,  976, 1011,  976,  976, 1063, 1081, 1257,  954, 1207,
 /*   220 */  1197, 1257,  954, 1239, 1238, 1248, 1247, 1246, 1237, 1236,
 /*   230 */  1235,  954,  954,  954, 1231, 1234, 1233, 1232, 1245,  954,
 /*   240 */   954, 1209,  954, 1241, 1240,  954,  954,  954,  954,  954,
 /*   250 */   954,  954, 1160,  954,  954,  954, 1185, 1203, 1199,  954,
 /*   260 */   954,  954,  954,  954,  954,  954,  954, 1210,  954,  954,
 /*   270 */   954,  954,  954,  954,  954,  954, 1124,  954,  954, 1091,
 /*   280 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   290 */   954,  954, 1135,  954,  954,  954,  954,  954, 1147, 1146,
 /*   300 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   310 */   954,  954,  954,  954,  954,  954,  954,  954, 1191,  954,
 /*   320 */  1186,  954, 1180,  954,  954,  954, 1103,  954,  954,  954,
 /*   330 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   340 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   350 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   360 */   954,  954,  954,  954, 1276, 1271, 1272, 1269,  954,  954,
 /*   370 */   954,  954,  954, 1268, 1263, 1264,  954,  954,  954, 1261,
 /*   380 */   954,  954,  954,  954,  954,  954,  954,  954,  954,  954,
 /*   390 */   954,  954,  954,  954,  954, 1047,  954,  954,  983,  954,
 /*   400 */   981,  954,  954,
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
    1,  /*     PARAMS => ID */
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
  /*   94 */ "PARAMS",
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
  /*  163 */ "ABORT",
  /*  164 */ "AFTER",
  /*  165 */ "ATTACH",
  /*  166 */ "BEFORE",
  /*  167 */ "BEGIN",
  /*  168 */ "CASCADE",
  /*  169 */ "CLUSTER",
  /*  170 */ "CONFLICT",
  /*  171 */ "COPY",
  /*  172 */ "DEFERRED",
  /*  173 */ "DELIMITERS",
  /*  174 */ "DETACH",
  /*  175 */ "EACH",
  /*  176 */ "END",
  /*  177 */ "EXPLAIN",
  /*  178 */ "FAIL",
  /*  179 */ "FOR",
  /*  180 */ "IGNORE",
  /*  181 */ "IMMEDIATE",
  /*  182 */ "INITIALLY",
  /*  183 */ "INSTEAD",
  /*  184 */ "KEY",
  /*  185 */ "OF",
  /*  186 */ "RAISE",
  /*  187 */ "REPLACE",
  /*  188 */ "RESTRICT",
  /*  189 */ "ROW",
  /*  190 */ "STATEMENT",
  /*  191 */ "TRIGGER",
  /*  192 */ "VIEW",
  /*  193 */ "IPTOKEN",
  /*  194 */ "SEMI",
  /*  195 */ "NONE",
  /*  196 */ "PREV",
  /*  197 */ "LINEAR",
  /*  198 */ "IMPORT",
  /*  199 */ "TBNAME",
  /*  200 */ "JOIN",
  /*  201 */ "INSERT",
  /*  202 */ "INTO",
  /*  203 */ "VALUES",
  /*  204 */ "FILE",
  /*  205 */ "error",
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
  /*  221 */ "params",
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
 /*  61 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params",
 /*  62 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params",
 /*  63 */ "cmd ::= CREATE USER ids PASS ids",
 /*  64 */ "bufsize ::=",
 /*  65 */ "bufsize ::= BUFSIZE INTEGER",
 /*  66 */ "params ::=",
 /*  67 */ "params ::= PARAMS INTEGER",
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
 /* 289 */ "expr ::= expr LIKE expr",
 /* 290 */ "expr ::= expr MATCH expr",
 /* 291 */ "expr ::= expr NMATCH expr",
 /* 292 */ "expr ::= ID CONTAINS STRING",
 /* 293 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 294 */ "arrow ::= ID ARROW STRING",
 /* 295 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 296 */ "expr ::= arrow",
 /* 297 */ "expr ::= expr IN LP exprlist RP",
 /* 298 */ "exprlist ::= exprlist COMMA expritem",
 /* 299 */ "exprlist ::= expritem",
 /* 300 */ "expritem ::= expr",
 /* 301 */ "expritem ::=",
 /* 302 */ "cmd ::= RESET QUERY CACHE",
 /* 303 */ "cmd ::= SYNCDB ids REPLICA",
 /* 304 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 305 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 306 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 307 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 308 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 309 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 310 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 311 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 312 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 313 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 314 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 315 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 316 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 317 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 318 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 319 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 320 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 321 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 322 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 215: /* exprlist */
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
  {  206,   -1 }, /* (0) program ::= cmd */
  {  207,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  207,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  207,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  207,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  207,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  207,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  207,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  207,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  207,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  207,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  207,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  207,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  207,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  207,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  207,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  207,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  209,    0 }, /* (17) dbPrefix ::= */
  {  209,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  210,    0 }, /* (19) cpxName ::= */
  {  210,   -2 }, /* (20) cpxName ::= DOT ids */
  {  207,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  207,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  207,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  207,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  207,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
  {  207,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  207,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
  {  207,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  207,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  207,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  207,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  207,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  207,   -3 }, /* (33) cmd ::= DROP FUNCTION ids */
  {  207,   -3 }, /* (34) cmd ::= DROP DNODE ids */
  {  207,   -3 }, /* (35) cmd ::= DROP USER ids */
  {  207,   -3 }, /* (36) cmd ::= DROP ACCOUNT ids */
  {  207,   -2 }, /* (37) cmd ::= USE ids */
  {  207,   -3 }, /* (38) cmd ::= DESCRIBE ids cpxName */
  {  207,   -3 }, /* (39) cmd ::= DESC ids cpxName */
  {  207,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  207,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  207,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  207,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  207,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  207,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  207,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  207,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  207,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  207,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  207,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  208,   -1 }, /* (51) ids ::= ID */
  {  208,   -1 }, /* (52) ids ::= STRING */
  {  211,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  211,    0 }, /* (54) ifexists ::= */
  {  216,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  216,    0 }, /* (56) ifnotexists ::= */
  {  207,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  207,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  207,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  207,   -5 }, /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  207,   -9 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
  {  207,  -10 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
  {  207,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  220,    0 }, /* (64) bufsize ::= */
  {  220,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  221,    0 }, /* (66) params ::= */
  {  221,   -2 }, /* (67) params ::= PARAMS INTEGER */
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
  {  214,   -9 }, /* (86) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
  {  217,    0 }, /* (106) db_optr ::= */
  {  217,   -2 }, /* (107) db_optr ::= db_optr cache */
  {  217,   -2 }, /* (108) db_optr ::= db_optr replica */
  {  217,   -2 }, /* (109) db_optr ::= db_optr quorum */
  {  217,   -2 }, /* (110) db_optr ::= db_optr days */
  {  217,   -2 }, /* (111) db_optr ::= db_optr minrows */
  {  217,   -2 }, /* (112) db_optr ::= db_optr maxrows */
  {  217,   -2 }, /* (113) db_optr ::= db_optr blocks */
  {  217,   -2 }, /* (114) db_optr ::= db_optr ctime */
  {  217,   -2 }, /* (115) db_optr ::= db_optr wal */
  {  217,   -2 }, /* (116) db_optr ::= db_optr fsync */
  {  217,   -2 }, /* (117) db_optr ::= db_optr comp */
  {  217,   -2 }, /* (118) db_optr ::= db_optr prec */
  {  217,   -2 }, /* (119) db_optr ::= db_optr keep */
  {  217,   -2 }, /* (120) db_optr ::= db_optr update */
  {  217,   -2 }, /* (121) db_optr ::= db_optr cachelast */
  {  218,   -1 }, /* (122) topic_optr ::= db_optr */
  {  218,   -2 }, /* (123) topic_optr ::= topic_optr partitions */
  {  212,    0 }, /* (124) alter_db_optr ::= */
  {  212,   -2 }, /* (125) alter_db_optr ::= alter_db_optr replica */
  {  212,   -2 }, /* (126) alter_db_optr ::= alter_db_optr quorum */
  {  212,   -2 }, /* (127) alter_db_optr ::= alter_db_optr keep */
  {  212,   -2 }, /* (128) alter_db_optr ::= alter_db_optr blocks */
  {  212,   -2 }, /* (129) alter_db_optr ::= alter_db_optr comp */
  {  212,   -2 }, /* (130) alter_db_optr ::= alter_db_optr update */
  {  212,   -2 }, /* (131) alter_db_optr ::= alter_db_optr cachelast */
  {  213,   -1 }, /* (132) alter_topic_optr ::= alter_db_optr */
  {  213,   -2 }, /* (133) alter_topic_optr ::= alter_topic_optr partitions */
  {  219,   -1 }, /* (134) typename ::= ids */
  {  219,   -4 }, /* (135) typename ::= ids LP signed RP */
  {  219,   -2 }, /* (136) typename ::= ids UNSIGNED */
  {  249,   -1 }, /* (137) signed ::= INTEGER */
  {  249,   -2 }, /* (138) signed ::= PLUS INTEGER */
  {  249,   -2 }, /* (139) signed ::= MINUS INTEGER */
  {  207,   -3 }, /* (140) cmd ::= CREATE TABLE create_table_args */
  {  207,   -3 }, /* (141) cmd ::= CREATE TABLE create_stable_args */
  {  207,   -3 }, /* (142) cmd ::= CREATE STABLE create_stable_args */
  {  207,   -3 }, /* (143) cmd ::= CREATE TABLE create_table_list */
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
  {  207,   -1 }, /* (178) cmd ::= union */
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
  {  280,   -3 }, /* (289) expr ::= expr LIKE expr */
  {  280,   -3 }, /* (290) expr ::= expr MATCH expr */
  {  280,   -3 }, /* (291) expr ::= expr NMATCH expr */
  {  280,   -3 }, /* (292) expr ::= ID CONTAINS STRING */
  {  280,   -5 }, /* (293) expr ::= ID DOT ID CONTAINS STRING */
  {  290,   -3 }, /* (294) arrow ::= ID ARROW STRING */
  {  290,   -5 }, /* (295) arrow ::= ID DOT ID ARROW STRING */
  {  280,   -1 }, /* (296) expr ::= arrow */
  {  280,   -5 }, /* (297) expr ::= expr IN LP exprlist RP */
  {  215,   -3 }, /* (298) exprlist ::= exprlist COMMA expritem */
  {  215,   -1 }, /* (299) exprlist ::= expritem */
  {  292,   -1 }, /* (300) expritem ::= expr */
  {  292,    0 }, /* (301) expritem ::= */
  {  207,   -3 }, /* (302) cmd ::= RESET QUERY CACHE */
  {  207,   -3 }, /* (303) cmd ::= SYNCDB ids REPLICA */
  {  207,   -7 }, /* (304) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  207,   -7 }, /* (305) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  207,   -7 }, /* (306) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  207,   -7 }, /* (307) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  207,   -7 }, /* (308) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  207,   -8 }, /* (309) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  207,   -9 }, /* (310) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  207,   -7 }, /* (311) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  207,   -7 }, /* (312) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  207,   -7 }, /* (313) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  207,   -7 }, /* (314) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  207,   -7 }, /* (315) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  207,   -7 }, /* (316) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  207,   -8 }, /* (317) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  207,   -9 }, /* (318) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  207,   -7 }, /* (319) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  207,   -3 }, /* (320) cmd ::= KILL CONNECTION INTEGER */
  {  207,   -5 }, /* (321) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  207,   -5 }, /* (322) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 188: /* distinct ::= */ yytestcase(yyruleno==188);
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
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-6].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy307, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize params */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-6].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy307, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 2);}
        break;
      case 63: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 64: /* bufsize ::= */
      case 66: /* params ::= */ yytestcase(yyruleno==66);
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
      case 65: /* bufsize ::= BUFSIZE INTEGER */
      case 67: /* params ::= PARAMS INTEGER */ yytestcase(yyruleno==67);
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
      case 301: /* expritem ::= */ yytestcase(yyruleno==301);
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
      case 289: /* expr ::= expr LIKE expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_LIKE);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 290: /* expr ::= expr MATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_MATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 291: /* expr ::= expr NMATCH expr */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-2].minor.yy378, yymsp[0].minor.yy378, TK_NMATCH);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 292: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 293: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 294: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy378 = yylhsminor.yy378;
        break;
      case 295: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy378 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 296: /* expr ::= arrow */
      case 300: /* expritem ::= expr */ yytestcase(yyruleno==300);
{yylhsminor.yy378 = yymsp[0].minor.yy378;}
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 297: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy378 = tSqlExprCreate(yymsp[-4].minor.yy378, (tSqlExpr*)yymsp[-1].minor.yy367, TK_IN); }
  yymsp[-4].minor.yy378 = yylhsminor.yy378;
        break;
      case 298: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy367 = tSqlExprListAppend(yymsp[-2].minor.yy367,yymsp[0].minor.yy378,0, 0);}
  yymsp[-2].minor.yy367 = yylhsminor.yy367;
        break;
      case 299: /* exprlist ::= expritem */
{yylhsminor.yy367 = tSqlExprListAppend(0,yymsp[0].minor.yy378,0, 0);}
  yymsp[0].minor.yy367 = yylhsminor.yy367;
        break;
      case 302: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 303: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 306: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 307: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 310: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 318: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy410, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 319: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy367, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 320: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 321: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 322: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
