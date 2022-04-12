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
#define YYNOCODE 293
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
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             405
#define YYNRULE              322
#define YYNTOKEN             205
#define YY_MAX_SHIFT         404
#define YY_MIN_SHIFTREDUCE   633
#define YY_MAX_SHIFTREDUCE   954
#define YY_ERROR_ACTION      955
#define YY_ACCEPT_ACTION     956
#define YY_NO_ACTION         957
#define YY_MIN_REDUCE        958
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
#define YY_ACTTAB_COUNT (889)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   223,  684, 1107,  168,  956,  404,   24,  768, 1141,  685,
 /*    10 */  1252,  684, 1254,  256,   38,   39, 1252,   42,   43,  685,
 /*    20 */  1117,  271,   31,   30,   29,  403,  249,   41,  354,   46,
 /*    30 */    44,   47,   45,   32,  110,  258,  221,   37,   36,  379,
 /*    40 */   378,   35,   34,   33,   38,   39, 1252,   42,   43,  264,
 /*    50 */   183,  271,   31,   30,   29,  303, 1132,   41,  354,   46,
 /*    60 */    44,   47,   45,   32,   35,   34,   33,   37,   36,  311,
 /*    70 */  1101,   35,   34,   33,  295,  684,  302,  301,   38,   39,
 /*    80 */  1138,   42,   43,  685,  720,  271,   31,   30,   29,   60,
 /*    90 */    90,   41,  354,   46,   44,   47,   45,   32,  402,  400,
 /*   100 */   661,   37,   36,  222,  227,   35,   34,   33,   38,   39,
 /*   110 */    13,   42,   43, 1252, 1252,  271,   31,   30,   29,  296,
 /*   120 */    59,   41,  354,   46,   44,   47,   45,   32,  161,  159,
 /*   130 */   158,   37,   36,   61,  251,   35,   34,   33,  350,   38,
 /*   140 */    40, 1114,   42,   43,  109,   88,  271,   31,   30,   29,
 /*   150 */   281,  882,   41,  354,   46,   44,   47,   45,   32,  389,
 /*   160 */    52,  190,   37,   36,  228,  684,   35,   34,   33,   39,
 /*   170 */   229,   42,   43,  685, 1252,  271,   31,   30,   29,  240,
 /*   180 */  1252,   41,  354,   46,   44,   47,   45,   32,   60, 1252,
 /*   190 */    96,   37,   36,  842,  843,   35,   34,   33,   68,  348,
 /*   200 */   396,  395,  347,  346,  345,  394,  344,  343,  342,  393,
 /*   210 */   341,  392,  391,  634,  635,  636,  637,  638,  639,  640,
 /*   220 */   641,  642,  643,  644,  645,  646,  647,  162,   60,  250,
 /*   230 */    69,   42,   43,  261, 1116,  271,   31,   30,   29,  287,
 /*   240 */  1114,   41,  354,   46,   44,   47,   45,   32,  291,  290,
 /*   250 */   811,   37,   36,   60, 1274,   35,   34,   33,   25,  352,
 /*   260 */  1075, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070, 1071,
 /*   270 */  1072, 1073, 1074, 1076, 1077,  233,  324,  243,  898,   10,
 /*   280 */  1113,  886,  235,  889, 1193,  892, 1194,  321,  149,  148,
 /*   290 */   147,  234, 1243,  243,  898,  362,   96,  886,  262,  889,
 /*   300 */   304,  892, 1252,  359, 1191, 1114, 1192,   46,   44,   47,
 /*   310 */    45,   32, 1242,  247,  248,   37,   36,  356, 1241,   35,
 /*   320 */    34,   33, 1252, 1204,    5,   63,  194,  245, 1252,  247,
 /*   330 */   248,  193,  116,  121,  112,  120,   69, 1252,  796,  325,
 /*   340 */   102,  793,  101,  794,   32,  795, 1266,  830,   37,   36,
 /*   350 */   337,  833,   35,   34,   33, 1099,  214,  212,  210,  294,
 /*   360 */    60,   86,   48,  209,  153,  152,  151,  150,  244,  274,
 /*   370 */   268,  267,  284,  276,  277,  133,  127,  138,   48,  307,
 /*   380 */   308,  888,  137,  891,  143,  146,  136,  280,  272,  887,
 /*   390 */   104,  890,  103,  140,  246, 1083, 1203, 1081, 1082,  899,
 /*   400 */   893,  895, 1084,  107, 1252,  366, 1085,  263, 1086, 1087,
 /*   410 */   903,   60, 1114,  259, 1117,  899,  893,  895,   68,  353,
 /*   420 */   396,  395,   37,   36,  894,  394,   35,   34,   33,  393,
 /*   430 */    60,  392,  391,   60,  219,  223, 1096, 1097,   56, 1100,
 /*   440 */   894,  352,   60,  350, 1252, 1252, 1255, 1255,  186,  275,
 /*   450 */    60,  273,  223,  365,  364,   60,  367,  281,  270,  797,
 /*   460 */   278,   60, 1252, 1114, 1255,  814,  254,  282,  191,  279,
 /*   470 */   265,  374,  373,  281,  164,  368,  225, 1117,  369,  226,
 /*   480 */     6,  230, 1114,  224,  355, 1114, 1252,  375,  231, 1252,
 /*   490 */    87, 1252,  281, 1252, 1114,  376,  862, 1006, 1252,   93,
 /*   500 */   377,  896, 1114, 1115,  204,  232,  381, 1114,  135,  897,
 /*   510 */   105,  237,  238, 1114, 1132, 1252,  239,  236,  220, 1132,
 /*   520 */   389, 1252, 1252,   94, 1098,   91, 1252, 1252, 1252,  397,
 /*   530 */  1044, 1016,  252, 1007,  839,    1,  192,  253,  204,  298,
 /*   540 */   204,    3,  205,  306,  305,  849,  850,   77,  170,  269,
 /*   550 */   778,   80,  329,  818,  780,  331,  861,  779,   55,   72,
 /*   560 */    49,  928,  358,  298,   61,  900,   61,  683,   72,  108,
 /*   570 */    15,   72,   14,   84,  126,    9,  125,  292,   17,    9,
 /*   580 */    16,    9,  371,  370,  357,  803,  801,  804,  802,  885,
 /*   590 */   166,  332,   78,   19,   81,   18,  132,   21,  131,   20,
 /*   600 */   145,  144, 1200,  167, 1199,  767,  260,  380, 1112, 1140,
 /*   610 */    26, 1151, 1148, 1149, 1153, 1133,  169,  174,  299,  317,
 /*   620 */  1108, 1183, 1182, 1181,  185, 1180,  187, 1106,  188, 1279,
 /*   630 */   189, 1021,  160,  829,  398,  334,  335,  336,  339,  340,
 /*   640 */    70,  217,   66,  351,   27, 1015,  363, 1273, 1130,  123,
 /*   650 */  1272,  310,   82, 1269,  255,  312,  195,  372,  314, 1265,
 /*   660 */   129, 1264, 1261,  196, 1041,   67,   79,  175,   62,   71,
 /*   670 */   218,  326, 1003,   28,  139,  176,  322, 1001,  178,  141,
 /*   680 */   320,  142,  177,  999,  998,  283,  318,  207,  316,  208,
 /*   690 */   995,  994,  993,  992,  991,  313,  990,  989,  211,  213,
 /*   700 */   981,  215,  978,  216,  974,  309,   89,  338,  163,  390,
 /*   710 */    85,  297, 1110,   92,   97,  315,  134,  382,  383,  384,
 /*   720 */   385,  386,  387,   83,  388,  165,  266,  333,  953,  286,
 /*   730 */   285,  952,  288,  241,  242,  289,  951,  934,  933, 1020,
 /*   740 */  1019,  117,  118,  293,  298,  328,   11,   95,  806,  300,
 /*   750 */   997,   53,  996,  199,  154,  198, 1042,  197,  200,  202,
 /*   760 */   201,  203,  838,  155,  156,  988,  987,    2,  157, 1079,
 /*   770 */   179,  327, 1043,  980,  979,  184,  180,    4,  181,  182,
 /*   780 */    54,   98,   75,  836,  835, 1089,  832,  831,   76,  173,
 /*   790 */   840,  171,  257,  851,  172,   64,  845,   99,  357,  847,
 /*   800 */   100,  319,  323,   22,   12,   65,   23,  106,   50,  330,
 /*   810 */   111,   51,  109,  114,  698,  733,  731,  730,   57,  113,
 /*   820 */   729,  727,  726,   58,  115,  725,  722,  688,  119,  349,
 /*   830 */     7,  925,  923,  926,  902,  901,  924,  361,    8,  904,
 /*   840 */   122,  124,  360,   73,   61,  800,  799,   74,  128,  770,
 /*   850 */   130,  769,  766,  714,  712,  704,  710,  706,  708,  702,
 /*   860 */   700,  736,  735,  734,  732,  728,  724,  723,  206,  686,
 /*   870 */   958,  651,  660,  957,  658,  957,  957,  957,  957,  957,
 /*   880 */   957,  957,  957,  957,  957,  957,  399,  957,  401,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   279,    1,  208,  208,  206,  207,  279,    5,  208,    9,
 /*    10 */   289,    1,  291,  254,   14,   15,  289,   17,   18,    9,
 /*    20 */   261,   21,   22,   23,   24,  208,  209,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,  216,    1,  279,   37,   38,   37,
 /*    40 */    38,   41,   42,   43,   14,   15,  289,   17,   18,  255,
 /*    50 */   266,   21,   22,   23,   24,  284,  258,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,   41,   42,   43,   37,   38,  285,
 /*    70 */   252,   41,   42,   43,  276,    1,  281,  282,   14,   15,
 /*    80 */   280,   17,   18,    9,    5,   21,   22,   23,   24,  208,
 /*    90 */    90,   27,   28,   29,   30,   31,   32,   33,   69,   70,
 /*   100 */    71,   37,   38,  279,  279,   41,   42,   43,   14,   15,
 /*   110 */    86,   17,   18,  289,  289,   21,   22,   23,   24,   87,
 /*   120 */    90,   27,   28,   29,   30,   31,   32,   33,   66,   67,
 /*   130 */    68,   37,   38,  101,  253,   41,   42,   43,   88,   14,
 /*   140 */    15,  260,   17,   18,  120,  121,   21,   22,   23,   24,
 /*   150 */   208,   87,   27,   28,   29,   30,   31,   32,   33,   94,
 /*   160 */    86,  219,   37,   38,  279,    1,   41,   42,   43,   15,
 /*   170 */   279,   17,   18,    9,  289,   21,   22,   23,   24,  279,
 /*   180 */   289,   27,   28,   29,   30,   31,   32,   33,  208,  289,
 /*   190 */    86,   37,   38,  132,  133,   41,   42,   43,  102,  103,
 /*   200 */   104,  105,  106,  107,  108,  109,  110,  111,  112,  113,
 /*   210 */   114,  115,  116,   49,   50,   51,   52,   53,   54,   55,
 /*   220 */    56,   57,   58,   59,   60,   61,   62,   63,  208,   65,
 /*   230 */   126,   17,   18,  253,  261,   21,   22,   23,   24,  150,
 /*   240 */   260,   27,   28,   29,   30,   31,   32,   33,  159,  160,
 /*   250 */   101,   37,   38,  208,  261,   41,   42,   43,   48,   47,
 /*   260 */   232,  233,  234,  235,  236,  237,  238,  239,  240,  241,
 /*   270 */   242,  243,  244,  245,  246,   65,   64,    1,    2,  130,
 /*   280 */   260,    5,   72,    7,  287,    9,  289,  290,   78,   79,
 /*   290 */    80,   81,  279,    1,    2,   85,   86,    5,  253,    7,
 /*   300 */   284,    9,  289,   16,  287,  260,  289,   29,   30,   31,
 /*   310 */    32,   33,  279,   37,   38,   37,   38,   41,  279,   41,
 /*   320 */    42,   43,  289,  248,   66,   67,   68,  279,  289,   37,
 /*   330 */    38,   73,   74,   75,   76,   77,  126,  289,    2,  286,
 /*   340 */   287,    5,  289,    7,   33,    9,  261,    5,   37,   38,
 /*   350 */    92,    9,   41,   42,   43,    0,   66,   67,   68,  149,
 /*   360 */   208,  151,   86,   73,   74,   75,   76,   77,  158,   72,
 /*   370 */   215,  215,  162,   37,   38,   66,   67,   68,   86,   37,
 /*   380 */    38,    5,   73,    7,   75,   76,   77,   72,  215,    5,
 /*   390 */   287,    7,  289,   84,  279,  232,  248,  234,  235,  123,
 /*   400 */   124,  125,  239,  216,  289,  253,  243,  254,  245,  246,
 /*   410 */   123,  208,  260,  248,  261,  123,  124,  125,  102,   25,
 /*   420 */   104,  105,   37,   38,  148,  109,   41,   42,   43,  113,
 /*   430 */   208,  115,  116,  208,  279,  279,  249,  250,  251,  252,
 /*   440 */   148,   47,  208,   88,  289,  289,  291,  291,  256,  152,
 /*   450 */   208,  154,  279,  156,  157,  208,  253,  208,   64,  123,
 /*   460 */   124,  208,  289,  260,  291,   41,  124,  152,  219,  154,
 /*   470 */   254,  156,  157,  208,  208,  253,  279,  261,  253,  279,
 /*   480 */    86,  279,  260,  279,  219,  260,  289,  253,  279,  289,
 /*   490 */   216,  289,  208,  289,  260,  253,   80,  214,  289,   87,
 /*   500 */   253,  125,  260,  219,  221,  279,  253,  260,   82,  125,
 /*   510 */   262,  279,  279,  260,  258,  289,  279,  279,  279,  258,
 /*   520 */    94,  289,  289,   87,  250,  277,  289,  289,  289,  230,
 /*   530 */   231,  214,  276,  214,   87,  217,  218,  276,  221,  127,
 /*   540 */   221,  212,  213,   37,   38,   87,   87,  101,  101,    1,
 /*   550 */    87,  101,   87,  129,   87,   87,  140,   87,   86,  101,
 /*   560 */   101,   87,   25,  127,  101,   87,  101,   87,  101,  101,
 /*   570 */   153,  101,  155,   86,  153,  101,  155,  208,  153,  101,
 /*   580 */   155,  101,   37,   38,   47,    5,    5,    7,    7,   41,
 /*   590 */   208,  119,  146,  153,  144,  155,  153,  153,  155,  155,
 /*   600 */    82,   83,  248,  208,  248,  118,  248,  248,  208,  208,
 /*   610 */   278,  208,  208,  208,  208,  258,  208,  208,  258,  208,
 /*   620 */   258,  288,  288,  288,  263,  288,  208,  208,  208,  264,
 /*   630 */   208,  208,   64,  125,   88,  208,  208,  208,  208,  208,
 /*   640 */   208,  208,  208,  208,  147,  208,  208,  208,  275,  208,
 /*   650 */   208,  283,  143,  208,  283,  283,  208,  208,  283,  208,
 /*   660 */   208,  208,  208,  208,  208,  208,  145,  274,  208,  208,
 /*   670 */   208,  138,  208,  142,  208,  273,  141,  208,  271,  208,
 /*   680 */   136,  208,  272,  208,  208,  208,  135,  208,  134,  208,
 /*   690 */   208,  208,  208,  208,  208,  137,  208,  208,  208,  208,
 /*   700 */   208,  208,  208,  208,  208,  131,  122,   93,  210,  117,
 /*   710 */   211,  210,  210,  210,  210,  210,  100,   99,   55,   96,
 /*   720 */    98,   59,   97,  210,   95,  130,  210,  210,    5,    5,
 /*   730 */   161,    5,  161,  210,  210,    5,    5,  104,  103,  220,
 /*   740 */   220,  216,  216,  150,  127,  119,   86,  128,   87,  101,
 /*   750 */   210,   86,  210,  223,  211,  227,  229,  228,  226,  225,
 /*   760 */   224,  222,   87,  211,  211,  210,  210,  217,  211,  247,
 /*   770 */   270,  257,  231,  210,  210,  264,  269,  212,  268,  267,
 /*   780 */   265,  101,  101,  125,  125,  247,    5,    5,   86,  101,
 /*   790 */    87,   86,    1,   87,   86,  101,   87,   86,   47,   87,
 /*   800 */    86,   86,    1,  139,   86,  101,  139,   90,   86,  119,
 /*   810 */    82,   86,  120,   74,    5,    9,    5,    5,   91,   90,
 /*   820 */     5,    5,    5,   91,   90,    5,    5,   89,   82,   16,
 /*   830 */    86,    9,    9,    9,   87,   87,    9,   63,   86,  123,
 /*   840 */   155,  155,   28,   17,  101,  125,  125,   17,  155,    5,
 /*   850 */   155,    5,   87,    5,    5,    5,    5,    5,    5,    5,
 /*   860 */     5,    5,    5,    5,    5,    5,    5,    5,  101,   89,
 /*   870 */     0,   64,    9,  292,    9,  292,  292,  292,  292,  292,
 /*   880 */   292,  292,  292,  292,  292,  292,   22,  292,   22,  292,
 /*   890 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   900 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   910 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   920 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   930 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*   940 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
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
 /*  1090 */   292,  292,  292,  292,
};
#define YY_SHIFT_COUNT    (404)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (870)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   210,   96,   96,  316,  316,   50,  276,  292,  292,  292,
 /*    10 */    74,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*    20 */    10,   10,   34,   34,    0,  164,  292,  292,  292,  292,
 /*    30 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*    40 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  336,
 /*    50 */   336,  336,  104,  104,   61,   10,  355,   10,   10,   10,
 /*    60 */    10,   10,  426,   50,   34,   34,   65,   65,   79,  889,
 /*    70 */   889,  889,  336,  336,  336,  342,  342,    2,    2,    2,
 /*    80 */     2,    2,    2,   24,    2,   10,   10,   10,   10,   10,
 /*    90 */    10,  424,   10,   10,   10,  104,  104,   10,   10,   10,
 /*   100 */    10,  416,  416,  416,  416,  149,  104,   10,   10,   10,
 /*   110 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   120 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   130 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   140 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   150 */    10,   10,   10,   10,   10,   10,   10,   10,   10,   10,
 /*   160 */    10,   10,   10,  497,  568,  546,  568,  568,  568,  568,
 /*   170 */   508,  508,  508,  508,  568,  509,  521,  533,  531,  535,
 /*   180 */   544,  551,  554,  558,  574,  497,  584,  568,  568,  568,
 /*   190 */   614,  614,  592,   50,   50,  568,  568,  616,  618,  663,
 /*   200 */   623,  622,  662,  625,  629,  592,   79,  568,  568,  546,
 /*   210 */   546,  568,  546,  568,  546,  568,  568,  889,  889,   30,
 /*   220 */    64,   94,   94,   94,  125,  154,  214,  278,  278,  278,
 /*   230 */   278,  278,  278,  258,  309,  290,  311,  311,  311,  311,
 /*   240 */   385,  297,  315,  394,   89,   23,   23,  376,  384,   29,
 /*   250 */    62,   32,  412,  436,  506,  447,  458,  459,  212,  446,
 /*   260 */   450,  463,  465,  467,  468,  470,  472,  474,  478,  537,
 /*   270 */   548,  287,  480,  417,  421,  425,  580,  581,  545,  440,
 /*   280 */   443,  487,  444,  518,  595,  723,  569,  724,  726,  571,
 /*   290 */   730,  731,  633,  635,  593,  617,  626,  660,  619,  661,
 /*   300 */   665,  648,  680,  675,  681,  658,  659,  781,  782,  702,
 /*   310 */   703,  705,  706,  708,  709,  688,  711,  712,  714,  791,
 /*   320 */   715,  694,  664,  751,  801,  704,  667,  717,  718,  626,
 /*   330 */   722,  690,  725,  692,  728,  727,  729,  739,  809,  732,
 /*   340 */   734,  806,  811,  812,  815,  816,  817,  820,  821,  738,
 /*   350 */   813,  746,  822,  823,  744,  747,  748,  824,  827,  716,
 /*   360 */   752,  814,  774,  826,  685,  686,  743,  743,  743,  743,
 /*   370 */   720,  721,  830,  693,  695,  743,  743,  743,  844,  846,
 /*   380 */   765,  743,  848,  849,  850,  851,  852,  853,  854,  855,
 /*   390 */   856,  857,  858,  859,  860,  861,  862,  767,  780,  863,
 /*   400 */   864,  865,  866,  807,  870,
};
#define YY_REDUCE_COUNT (218)
#define YY_REDUCE_MIN   (-279)
#define YY_REDUCE_MAX   (565)
static const short yy_reduce_ofst[] = {
 /*     0 */  -202,   28,   28,  163,  163,  187,  155,  156,  173, -279,
 /*    10 */  -205, -119,  -20,   45,  152,  203,  222,  225,  234,  242,
 /*    20 */   247,  253,   -3,   53, -200, -183, -273, -243, -176, -175,
 /*    30 */  -115, -109, -100,   13,   33,   39,   48,  115,  197,  200,
 /*    40 */   202,  204,  209,  226,  232,  233,  237,  238,  239, -241,
 /*    50 */   153,  216,  256,  261, -216, -206, -182,  -58,  249,  265,
 /*    60 */   284,   20,  283,  274,   17,  103,  317,  319,  299,  248,
 /*    70 */   318,  329,  -27,   -7,   85, -229,   16,   75,  148,  165,
 /*    80 */   354,  356,  358,  192,  359,  266,  369,  382,  395,  400,
 /*    90 */   401,  332,  403,  404,  405,  357,  360,  406,  408,  409,
 /*   100 */   411,  333,  334,  335,  337,  361,  362,  418,  419,  420,
 /*   110 */   422,  423,  427,  428,  429,  430,  431,  432,  433,  434,
 /*   120 */   435,  437,  438,  439,  441,  442,  445,  448,  449,  451,
 /*   130 */   452,  453,  454,  455,  456,  457,  460,  461,  462,  464,
 /*   140 */   466,  469,  471,  473,  475,  476,  477,  479,  481,  482,
 /*   150 */   483,  484,  485,  486,  488,  489,  490,  491,  492,  493,
 /*   160 */   494,  495,  496,  365,  498,  499,  501,  502,  503,  504,
 /*   170 */   368,  371,  372,  375,  505,  373,  393,  402,  410,  407,
 /*   180 */   500,  507,  510,  512,  515,  511,  514,  513,  516,  517,
 /*   190 */   519,  520,  522,  525,  526,  523,  524,  527,  529,  528,
 /*   200 */   530,  532,  536,  534,  539,  538,  541,  540,  542,  543,
 /*   210 */   552,  555,  553,  556,  557,  563,  564,  550,  565,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   955, 1078, 1017, 1088, 1004, 1014, 1257, 1257, 1257, 1257,
 /*    10 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*    20 */   955,  955,  955,  955, 1142,  975,  955,  955,  955,  955,
 /*    30 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*    40 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*    50 */   955,  955,  955,  955, 1166,  955, 1014,  955,  955,  955,
 /*    60 */   955,  955, 1024, 1014,  955,  955, 1024, 1024,  955, 1137,
 /*    70 */  1062, 1080,  955,  955,  955,  955,  955,  955,  955,  955,
 /*    80 */   955,  955,  955, 1109,  955,  955,  955,  955,  955,  955,
 /*    90 */   955, 1144, 1150, 1147,  955,  955,  955, 1152,  955,  955,
 /*   100 */   955, 1188, 1188, 1188, 1188, 1135,  955,  955,  955,  955,
 /*   110 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   120 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   130 */   955,  955,  955,  955,  955,  955,  955,  955,  955, 1002,
 /*   140 */   955, 1000,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   150 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   160 */   955,  955,  973, 1205,  977, 1012,  977,  977,  977,  977,
 /*   170 */   955,  955,  955,  955,  977, 1197, 1201, 1178, 1195, 1189,
 /*   180 */  1173, 1171, 1169, 1177, 1162, 1205, 1111,  977,  977,  977,
 /*   190 */  1022, 1022, 1018, 1014, 1014,  977,  977, 1040, 1038, 1036,
 /*   200 */  1028, 1034, 1030, 1032, 1026, 1005,  955,  977,  977, 1012,
 /*   210 */  1012,  977, 1012,  977, 1012,  977,  977, 1062, 1080, 1256,
 /*   220 */   955, 1206, 1196, 1256,  955, 1238, 1237, 1247, 1246, 1245,
 /*   230 */  1236, 1235, 1234,  955,  955,  955, 1230, 1233, 1232, 1231,
 /*   240 */  1244,  955,  955, 1208,  955, 1240, 1239,  955,  955,  955,
 /*   250 */   955,  955,  955,  955, 1159,  955,  955,  955, 1184, 1202,
 /*   260 */  1198,  955,  955,  955,  955,  955,  955,  955,  955, 1209,
 /*   270 */   955,  955,  955,  955,  955,  955,  955,  955, 1123,  955,
 /*   280 */   955, 1090,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   290 */   955,  955,  955,  955,  955, 1134,  955,  955,  955,  955,
 /*   300 */   955, 1146, 1145,  955,  955,  955,  955,  955,  955,  955,
 /*   310 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   320 */   955, 1190,  955, 1185,  955, 1179,  955,  955,  955, 1102,
 /*   330 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   340 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   350 */   955,  955,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   360 */   955,  955,  955,  955,  955,  955, 1275, 1270, 1271, 1268,
 /*   370 */   955,  955,  955,  955,  955, 1267, 1262, 1263,  955,  955,
 /*   380 */   955, 1260,  955,  955,  955,  955,  955,  955,  955,  955,
 /*   390 */   955,  955,  955,  955,  955,  955,  955, 1046,  955,  955,
 /*   400 */   984,  955,  982,  955,  955,
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
  /*  162 */ "DELETE",
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
 /* 286 */ "expr ::= expr BITAND expr",
 /* 287 */ "expr ::= expr LIKE expr",
 /* 288 */ "expr ::= expr MATCH expr",
 /* 289 */ "expr ::= expr NMATCH expr",
 /* 290 */ "expr ::= ID CONTAINS STRING",
 /* 291 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 292 */ "arrow ::= ID ARROW STRING",
 /* 293 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 294 */ "expr ::= arrow",
 /* 295 */ "expr ::= expr IN LP exprlist RP",
 /* 296 */ "exprlist ::= exprlist COMMA expritem",
 /* 297 */ "exprlist ::= expritem",
 /* 298 */ "expritem ::= expr",
 /* 299 */ "expritem ::=",
 /* 300 */ "cmd ::= RESET QUERY CACHE",
 /* 301 */ "cmd ::= SYNCDB ids REPLICA",
 /* 302 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 303 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 304 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 305 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 306 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 307 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 308 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 309 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 310 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 311 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 312 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 313 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 314 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 315 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 316 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 317 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 318 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 319 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 320 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
 /* 321 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
  {  207,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  207,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  207,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  220,    0 }, /* (64) bufsize ::= */
  {  220,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  221,    0 }, /* (66) pps ::= */
  {  221,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  222,    0 }, /* (68) tseries ::= */
  {  222,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  223,    0 }, /* (70) dbs ::= */
  {  223,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  224,    0 }, /* (72) streams ::= */
  {  224,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  225,    0 }, /* (74) storage ::= */
  {  225,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  226,    0 }, /* (76) qtime ::= */
  {  226,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  227,    0 }, /* (78) users ::= */
  {  227,   -2 }, /* (79) users ::= USERS INTEGER */
  {  228,    0 }, /* (80) conns ::= */
  {  228,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  229,    0 }, /* (82) state ::= */
  {  229,   -2 }, /* (83) state ::= STATE ids */
  {  214,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  230,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  230,   -1 }, /* (86) intitemlist ::= intitem */
  {  231,   -1 }, /* (87) intitem ::= INTEGER */
  {  232,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  233,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  234,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  235,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  236,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  237,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  238,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  239,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  240,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  241,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  242,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  243,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  244,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  245,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  246,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  247,   -2 }, /* (103) partitions ::= PARTITIONS INTEGER */
  {  217,    0 }, /* (104) db_optr ::= */
  {  217,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  217,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  217,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  217,   -2 }, /* (108) db_optr ::= db_optr days */
  {  217,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  217,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  217,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  217,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  217,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  217,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  217,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  217,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  217,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  217,   -2 }, /* (118) db_optr ::= db_optr update */
  {  217,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  218,   -1 }, /* (120) topic_optr ::= db_optr */
  {  218,   -2 }, /* (121) topic_optr ::= topic_optr partitions */
  {  212,    0 }, /* (122) alter_db_optr ::= */
  {  212,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  212,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  212,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  212,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  212,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  212,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  212,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  213,   -1 }, /* (130) alter_topic_optr ::= alter_db_optr */
  {  213,   -2 }, /* (131) alter_topic_optr ::= alter_topic_optr partitions */
  {  219,   -1 }, /* (132) typename ::= ids */
  {  219,   -4 }, /* (133) typename ::= ids LP signed RP */
  {  219,   -2 }, /* (134) typename ::= ids UNSIGNED */
  {  248,   -1 }, /* (135) signed ::= INTEGER */
  {  248,   -2 }, /* (136) signed ::= PLUS INTEGER */
  {  248,   -2 }, /* (137) signed ::= MINUS INTEGER */
  {  207,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_args */
  {  207,   -3 }, /* (139) cmd ::= CREATE TABLE create_stable_args */
  {  207,   -3 }, /* (140) cmd ::= CREATE STABLE create_stable_args */
  {  207,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_list */
  {  251,   -1 }, /* (142) create_table_list ::= create_from_stable */
  {  251,   -2 }, /* (143) create_table_list ::= create_table_list create_from_stable */
  {  249,   -6 }, /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  250,  -10 }, /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  252,  -10 }, /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  252,  -13 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  255,   -3 }, /* (148) tagNamelist ::= tagNamelist COMMA ids */
  {  255,   -1 }, /* (149) tagNamelist ::= ids */
  {  249,   -7 }, /* (150) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  256,    0 }, /* (151) to_opt ::= */
  {  256,   -3 }, /* (152) to_opt ::= TO ids cpxName */
  {  257,    0 }, /* (153) split_opt ::= */
  {  257,   -2 }, /* (154) split_opt ::= SPLIT ids */
  {  253,   -3 }, /* (155) columnlist ::= columnlist COMMA column */
  {  253,   -1 }, /* (156) columnlist ::= column */
  {  260,   -2 }, /* (157) column ::= ids typename */
  {  254,   -3 }, /* (158) tagitemlist ::= tagitemlist COMMA tagitem */
  {  254,   -1 }, /* (159) tagitemlist ::= tagitem */
  {  261,   -1 }, /* (160) tagitem ::= INTEGER */
  {  261,   -1 }, /* (161) tagitem ::= FLOAT */
  {  261,   -1 }, /* (162) tagitem ::= STRING */
  {  261,   -1 }, /* (163) tagitem ::= BOOL */
  {  261,   -1 }, /* (164) tagitem ::= NULL */
  {  261,   -1 }, /* (165) tagitem ::= NOW */
  {  261,   -3 }, /* (166) tagitem ::= NOW PLUS VARIABLE */
  {  261,   -3 }, /* (167) tagitem ::= NOW MINUS VARIABLE */
  {  261,   -2 }, /* (168) tagitem ::= MINUS INTEGER */
  {  261,   -2 }, /* (169) tagitem ::= MINUS FLOAT */
  {  261,   -2 }, /* (170) tagitem ::= PLUS INTEGER */
  {  261,   -2 }, /* (171) tagitem ::= PLUS FLOAT */
  {  258,  -15 }, /* (172) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  258,   -3 }, /* (173) select ::= LP select RP */
  {  276,   -1 }, /* (174) union ::= select */
  {  276,   -4 }, /* (175) union ::= union UNION ALL select */
  {  207,   -1 }, /* (176) cmd ::= union */
  {  258,   -2 }, /* (177) select ::= SELECT selcollist */
  {  277,   -2 }, /* (178) sclp ::= selcollist COMMA */
  {  277,    0 }, /* (179) sclp ::= */
  {  262,   -4 }, /* (180) selcollist ::= sclp distinct expr as */
  {  262,   -2 }, /* (181) selcollist ::= sclp STAR */
  {  280,   -2 }, /* (182) as ::= AS ids */
  {  280,   -1 }, /* (183) as ::= ids */
  {  280,    0 }, /* (184) as ::= */
  {  278,   -1 }, /* (185) distinct ::= DISTINCT */
  {  278,    0 }, /* (186) distinct ::= */
  {  263,   -2 }, /* (187) from ::= FROM tablelist */
  {  263,   -2 }, /* (188) from ::= FROM sub */
  {  282,   -3 }, /* (189) sub ::= LP union RP */
  {  282,   -4 }, /* (190) sub ::= LP union RP ids */
  {  282,   -6 }, /* (191) sub ::= sub COMMA LP union RP ids */
  {  281,   -2 }, /* (192) tablelist ::= ids cpxName */
  {  281,   -3 }, /* (193) tablelist ::= ids cpxName ids */
  {  281,   -4 }, /* (194) tablelist ::= tablelist COMMA ids cpxName */
  {  281,   -5 }, /* (195) tablelist ::= tablelist COMMA ids cpxName ids */
  {  283,   -1 }, /* (196) tmvar ::= VARIABLE */
  {  284,   -1 }, /* (197) timestamp ::= INTEGER */
  {  284,   -2 }, /* (198) timestamp ::= MINUS INTEGER */
  {  284,   -2 }, /* (199) timestamp ::= PLUS INTEGER */
  {  284,   -1 }, /* (200) timestamp ::= STRING */
  {  284,   -1 }, /* (201) timestamp ::= NOW */
  {  284,   -3 }, /* (202) timestamp ::= NOW PLUS VARIABLE */
  {  284,   -3 }, /* (203) timestamp ::= NOW MINUS VARIABLE */
  {  265,    0 }, /* (204) range_option ::= */
  {  265,   -6 }, /* (205) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  266,   -4 }, /* (206) interval_option ::= intervalKey LP tmvar RP */
  {  266,   -6 }, /* (207) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  266,    0 }, /* (208) interval_option ::= */
  {  285,   -1 }, /* (209) intervalKey ::= INTERVAL */
  {  285,   -1 }, /* (210) intervalKey ::= EVERY */
  {  268,    0 }, /* (211) session_option ::= */
  {  268,   -7 }, /* (212) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  269,    0 }, /* (213) windowstate_option ::= */
  {  269,   -4 }, /* (214) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  270,    0 }, /* (215) fill_opt ::= */
  {  270,   -6 }, /* (216) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  270,   -4 }, /* (217) fill_opt ::= FILL LP ID RP */
  {  267,   -4 }, /* (218) sliding_opt ::= SLIDING LP tmvar RP */
  {  267,    0 }, /* (219) sliding_opt ::= */
  {  273,    0 }, /* (220) orderby_opt ::= */
  {  273,   -3 }, /* (221) orderby_opt ::= ORDER BY sortlist */
  {  286,   -4 }, /* (222) sortlist ::= sortlist COMMA item sortorder */
  {  286,   -4 }, /* (223) sortlist ::= sortlist COMMA arrow sortorder */
  {  286,   -2 }, /* (224) sortlist ::= item sortorder */
  {  286,   -2 }, /* (225) sortlist ::= arrow sortorder */
  {  287,   -1 }, /* (226) item ::= ID */
  {  287,   -3 }, /* (227) item ::= ID DOT ID */
  {  288,   -1 }, /* (228) sortorder ::= ASC */
  {  288,   -1 }, /* (229) sortorder ::= DESC */
  {  288,    0 }, /* (230) sortorder ::= */
  {  271,    0 }, /* (231) groupby_opt ::= */
  {  271,   -3 }, /* (232) groupby_opt ::= GROUP BY grouplist */
  {  290,   -3 }, /* (233) grouplist ::= grouplist COMMA item */
  {  290,   -3 }, /* (234) grouplist ::= grouplist COMMA arrow */
  {  290,   -1 }, /* (235) grouplist ::= item */
  {  290,   -1 }, /* (236) grouplist ::= arrow */
  {  272,    0 }, /* (237) having_opt ::= */
  {  272,   -2 }, /* (238) having_opt ::= HAVING expr */
  {  275,    0 }, /* (239) limit_opt ::= */
  {  275,   -2 }, /* (240) limit_opt ::= LIMIT signed */
  {  275,   -4 }, /* (241) limit_opt ::= LIMIT signed OFFSET signed */
  {  275,   -4 }, /* (242) limit_opt ::= LIMIT signed COMMA signed */
  {  274,    0 }, /* (243) slimit_opt ::= */
  {  274,   -2 }, /* (244) slimit_opt ::= SLIMIT signed */
  {  274,   -4 }, /* (245) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  274,   -4 }, /* (246) slimit_opt ::= SLIMIT signed COMMA signed */
  {  264,    0 }, /* (247) where_opt ::= */
  {  264,   -2 }, /* (248) where_opt ::= WHERE expr */
  {  279,   -3 }, /* (249) expr ::= LP expr RP */
  {  279,   -1 }, /* (250) expr ::= ID */
  {  279,   -3 }, /* (251) expr ::= ID DOT ID */
  {  279,   -3 }, /* (252) expr ::= ID DOT STAR */
  {  279,   -1 }, /* (253) expr ::= INTEGER */
  {  279,   -2 }, /* (254) expr ::= MINUS INTEGER */
  {  279,   -2 }, /* (255) expr ::= PLUS INTEGER */
  {  279,   -1 }, /* (256) expr ::= FLOAT */
  {  279,   -2 }, /* (257) expr ::= MINUS FLOAT */
  {  279,   -2 }, /* (258) expr ::= PLUS FLOAT */
  {  279,   -1 }, /* (259) expr ::= STRING */
  {  279,   -1 }, /* (260) expr ::= NOW */
  {  279,   -1 }, /* (261) expr ::= TODAY */
  {  279,   -1 }, /* (262) expr ::= VARIABLE */
  {  279,   -2 }, /* (263) expr ::= PLUS VARIABLE */
  {  279,   -2 }, /* (264) expr ::= MINUS VARIABLE */
  {  279,   -1 }, /* (265) expr ::= BOOL */
  {  279,   -1 }, /* (266) expr ::= NULL */
  {  279,   -4 }, /* (267) expr ::= ID LP exprlist RP */
  {  279,   -4 }, /* (268) expr ::= ID LP STAR RP */
  {  279,   -6 }, /* (269) expr ::= ID LP expr AS typename RP */
  {  279,   -3 }, /* (270) expr ::= expr IS NULL */
  {  279,   -4 }, /* (271) expr ::= expr IS NOT NULL */
  {  279,   -3 }, /* (272) expr ::= expr LT expr */
  {  279,   -3 }, /* (273) expr ::= expr GT expr */
  {  279,   -3 }, /* (274) expr ::= expr LE expr */
  {  279,   -3 }, /* (275) expr ::= expr GE expr */
  {  279,   -3 }, /* (276) expr ::= expr NE expr */
  {  279,   -3 }, /* (277) expr ::= expr EQ expr */
  {  279,   -5 }, /* (278) expr ::= expr BETWEEN expr AND expr */
  {  279,   -3 }, /* (279) expr ::= expr AND expr */
  {  279,   -3 }, /* (280) expr ::= expr OR expr */
  {  279,   -3 }, /* (281) expr ::= expr PLUS expr */
  {  279,   -3 }, /* (282) expr ::= expr MINUS expr */
  {  279,   -3 }, /* (283) expr ::= expr STAR expr */
  {  279,   -3 }, /* (284) expr ::= expr SLASH expr */
  {  279,   -3 }, /* (285) expr ::= expr REM expr */
  {  279,   -3 }, /* (286) expr ::= expr BITAND expr */
  {  279,   -3 }, /* (287) expr ::= expr LIKE expr */
  {  279,   -3 }, /* (288) expr ::= expr MATCH expr */
  {  279,   -3 }, /* (289) expr ::= expr NMATCH expr */
  {  279,   -3 }, /* (290) expr ::= ID CONTAINS STRING */
  {  279,   -5 }, /* (291) expr ::= ID DOT ID CONTAINS STRING */
  {  289,   -3 }, /* (292) arrow ::= ID ARROW STRING */
  {  289,   -5 }, /* (293) arrow ::= ID DOT ID ARROW STRING */
  {  279,   -1 }, /* (294) expr ::= arrow */
  {  279,   -5 }, /* (295) expr ::= expr IN LP exprlist RP */
  {  215,   -3 }, /* (296) exprlist ::= exprlist COMMA expritem */
  {  215,   -1 }, /* (297) exprlist ::= expritem */
  {  291,   -1 }, /* (298) expritem ::= expr */
  {  291,    0 }, /* (299) expritem ::= */
  {  207,   -3 }, /* (300) cmd ::= RESET QUERY CACHE */
  {  207,   -3 }, /* (301) cmd ::= SYNCDB ids REPLICA */
  {  207,   -7 }, /* (302) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  207,   -7 }, /* (303) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  207,   -7 }, /* (304) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  207,   -7 }, /* (305) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  207,   -7 }, /* (306) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  207,   -8 }, /* (307) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  207,   -9 }, /* (308) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  207,   -7 }, /* (309) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  207,   -7 }, /* (310) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  207,   -7 }, /* (311) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  207,   -7 }, /* (312) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  207,   -7 }, /* (313) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  207,   -7 }, /* (314) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  207,   -8 }, /* (315) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  207,   -9 }, /* (316) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  207,   -7 }, /* (317) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  207,   -3 }, /* (318) cmd ::= KILL CONNECTION INTEGER */
  {  207,   -5 }, /* (319) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  207,   -5 }, /* (320) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  207,   -6 }, /* (321) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy342, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy299);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy299);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy333);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy299);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy342, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy263, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy263, &yymsp[0].minor.yy0, 2);}
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
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 158: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==158);
{ yylhsminor.yy333 = tVariantListAppend(yymsp[-2].minor.yy333, &yymsp[0].minor.yy42, -1);    }
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 86: /* intitemlist ::= intitem */
      case 159: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==159);
{ yylhsminor.yy333 = tVariantListAppend(NULL, &yymsp[0].minor.yy42, -1); }
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 87: /* intitem ::= INTEGER */
      case 160: /* tagitem ::= INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= FLOAT */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= STRING */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= BOOL */ yytestcase(yyruleno==163);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy42, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy333 = yymsp[0].minor.yy333; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy342); yymsp[1].minor.yy342.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.keep = yymsp[0].minor.yy333; }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy342 = yymsp[0].minor.yy342; yylhsminor.yy342.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy342); yymsp[1].minor.yy342.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy263, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy263 = yylhsminor.yy263;
        break;
      case 133: /* typename ::= ids LP signed RP */
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
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy263, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy263 = yylhsminor.yy263;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy277 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy277 = yylhsminor.yy277;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy277 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy277 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy78;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy400);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy78 = pCreateTable;
}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy78->childTableInfo, &yymsp[0].minor.yy400);
  yylhsminor.yy78 = yymsp[-1].minor.yy78;
}
  yymsp[-1].minor.yy78 = yylhsminor.yy78;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy78 = tSetCreateTableInfo(yymsp[-1].minor.yy333, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy78, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy78 = yylhsminor.yy78;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy78 = tSetCreateTableInfo(yymsp[-5].minor.yy333, yymsp[-1].minor.yy333, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy78, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy78 = yylhsminor.yy78;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy400 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy333, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy400 = yylhsminor.yy400;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy400 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy333, yymsp[-1].minor.yy333, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy400 = yylhsminor.yy400;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy333, &yymsp[0].minor.yy0); yylhsminor.yy333 = yymsp[-2].minor.yy333;  }
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy333 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy333, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy78 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy144, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy78, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy78 = yylhsminor.yy78;
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
{taosArrayPush(yymsp[-2].minor.yy333, &yymsp[0].minor.yy263); yylhsminor.yy333 = yymsp[-2].minor.yy333;  }
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 156: /* columnlist ::= column */
{yylhsminor.yy333 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy333, &yymsp[0].minor.yy263);}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 157: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy263, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy263);
}
  yymsp[-1].minor.yy263 = yylhsminor.yy263;
        break;
      case 164: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy42, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 165: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy42, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 166: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy42, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 167: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy42, &yymsp[0].minor.yy0, TK_MINUS, true);
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
    tVariantCreate(&yylhsminor.yy42, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 172: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy144 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy333, yymsp[-12].minor.yy516, yymsp[-11].minor.yy194, yymsp[-4].minor.yy333, yymsp[-2].minor.yy333, &yymsp[-9].minor.yy200, &yymsp[-7].minor.yy235, &yymsp[-6].minor.yy248, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy333, &yymsp[0].minor.yy190, &yymsp[-1].minor.yy190, yymsp[-3].minor.yy194, &yymsp[-10].minor.yy132);
}
  yymsp[-14].minor.yy144 = yylhsminor.yy144;
        break;
      case 173: /* select ::= LP select RP */
{yymsp[-2].minor.yy144 = yymsp[-1].minor.yy144;}
        break;
      case 174: /* union ::= select */
{ yylhsminor.yy333 = setSubclause(NULL, yymsp[0].minor.yy144); }
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 175: /* union ::= union UNION ALL select */
{ yylhsminor.yy333 = appendSelectClause(yymsp[-3].minor.yy333, yymsp[0].minor.yy144); }
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 176: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy333, NULL, TSDB_SQL_SELECT); }
        break;
      case 177: /* select ::= SELECT selcollist */
{
  yylhsminor.yy144 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy333, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy144 = yylhsminor.yy144;
        break;
      case 178: /* sclp ::= selcollist COMMA */
{yylhsminor.yy333 = yymsp[-1].minor.yy333;}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 179: /* sclp ::= */
      case 220: /* orderby_opt ::= */ yytestcase(yyruleno==220);
{yymsp[1].minor.yy333 = 0;}
        break;
      case 180: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy333 = tSqlExprListAppend(yymsp[-3].minor.yy333, yymsp[-1].minor.yy194,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 181: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy333 = tSqlExprListAppend(yymsp[-1].minor.yy333, pNode, 0, 0);
}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
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
{yymsp[-1].minor.yy516 = yymsp[0].minor.yy516;}
        break;
      case 189: /* sub ::= LP union RP */
{yymsp[-2].minor.yy516 = addSubqueryElem(NULL, yymsp[-1].minor.yy333, NULL);}
        break;
      case 190: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy516 = addSubqueryElem(NULL, yymsp[-2].minor.yy333, &yymsp[0].minor.yy0);}
        break;
      case 191: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy516 = addSubqueryElem(yymsp[-5].minor.yy516, yymsp[-2].minor.yy333, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy516 = yylhsminor.yy516;
        break;
      case 192: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 193: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy516 = yylhsminor.yy516;
        break;
      case 194: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(yymsp[-3].minor.yy516, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy516 = yylhsminor.yy516;
        break;
      case 195: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy516 = setTableNameList(yymsp[-4].minor.yy516, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy516 = yylhsminor.yy516;
        break;
      case 196: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 197: /* timestamp ::= INTEGER */
{ yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 198: /* timestamp ::= MINUS INTEGER */
      case 199: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==199);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 200: /* timestamp ::= STRING */
{ yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 201: /* timestamp ::= NOW */
{ yylhsminor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 202: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 203: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy194 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 204: /* range_option ::= */
{yymsp[1].minor.yy132.start = 0; yymsp[1].minor.yy132.end = 0;}
        break;
      case 205: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy132.start = yymsp[-3].minor.yy194; yymsp[-5].minor.yy132.end = yymsp[-1].minor.yy194;}
        break;
      case 206: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy200.interval = yymsp[-1].minor.yy0; yylhsminor.yy200.offset.n = 0; yylhsminor.yy200.token = yymsp[-3].minor.yy44;}
  yymsp[-3].minor.yy200 = yylhsminor.yy200;
        break;
      case 207: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy200.interval = yymsp[-3].minor.yy0; yylhsminor.yy200.offset = yymsp[-1].minor.yy0;   yylhsminor.yy200.token = yymsp[-5].minor.yy44;}
  yymsp[-5].minor.yy200 = yylhsminor.yy200;
        break;
      case 208: /* interval_option ::= */
{memset(&yymsp[1].minor.yy200, 0, sizeof(yymsp[1].minor.yy200));}
        break;
      case 209: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy44 = TK_INTERVAL;}
        break;
      case 210: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy44 = TK_EVERY;   }
        break;
      case 211: /* session_option ::= */
{yymsp[1].minor.yy235.col.n = 0; yymsp[1].minor.yy235.gap.n = 0;}
        break;
      case 212: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy235.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy235.gap = yymsp[-1].minor.yy0;
}
        break;
      case 213: /* windowstate_option ::= */
{ yymsp[1].minor.yy248.col.n = 0; yymsp[1].minor.yy248.col.z = NULL;}
        break;
      case 214: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy248.col = yymsp[-1].minor.yy0; }
        break;
      case 215: /* fill_opt ::= */
{ yymsp[1].minor.yy333 = 0;     }
        break;
      case 216: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy333, &A, -1, 0);
    yymsp[-5].minor.yy333 = yymsp[-1].minor.yy333;
}
        break;
      case 217: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy333 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 218: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 219: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 221: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy333 = yymsp[0].minor.yy333;}
        break;
      case 222: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-3].minor.yy333, &yymsp[-1].minor.yy42, NULL, false, yymsp[0].minor.yy133);
}
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 223: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-3].minor.yy333, NULL, yymsp[-1].minor.yy194, true, yymsp[0].minor.yy133);
}
  yymsp[-3].minor.yy333 = yylhsminor.yy333;
        break;
      case 224: /* sortlist ::= item sortorder */
{
  yylhsminor.yy333 = commonItemAppend(NULL, &yymsp[-1].minor.yy42, NULL, false, yymsp[0].minor.yy133);
}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 225: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy333 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy194, true, yymsp[0].minor.yy133);
}
  yymsp[-1].minor.yy333 = yylhsminor.yy333;
        break;
      case 226: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy42, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 227: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy42, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 228: /* sortorder ::= ASC */
{ yymsp[0].minor.yy133 = TSDB_ORDER_ASC; }
        break;
      case 229: /* sortorder ::= DESC */
{ yymsp[0].minor.yy133 = TSDB_ORDER_DESC;}
        break;
      case 230: /* sortorder ::= */
{ yymsp[1].minor.yy133 = TSDB_ORDER_ASC; }
        break;
      case 231: /* groupby_opt ::= */
{ yymsp[1].minor.yy333 = 0;}
        break;
      case 232: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy333 = yymsp[0].minor.yy333;}
        break;
      case 233: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-2].minor.yy333, &yymsp[0].minor.yy42, NULL, false, -1);
}
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 234: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy333 = commonItemAppend(yymsp[-2].minor.yy333, NULL, yymsp[0].minor.yy194, true, -1);
}
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 235: /* grouplist ::= item */
{
  yylhsminor.yy333 = commonItemAppend(NULL, &yymsp[0].minor.yy42, NULL, false, -1);
}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 236: /* grouplist ::= arrow */
{
  yylhsminor.yy333 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy194, true, -1);
}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 237: /* having_opt ::= */
      case 247: /* where_opt ::= */ yytestcase(yyruleno==247);
      case 299: /* expritem ::= */ yytestcase(yyruleno==299);
{yymsp[1].minor.yy194 = 0;}
        break;
      case 238: /* having_opt ::= HAVING expr */
      case 248: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==248);
{yymsp[-1].minor.yy194 = yymsp[0].minor.yy194;}
        break;
      case 239: /* limit_opt ::= */
      case 243: /* slimit_opt ::= */ yytestcase(yyruleno==243);
{yymsp[1].minor.yy190.limit = -1; yymsp[1].minor.yy190.offset = 0;}
        break;
      case 240: /* limit_opt ::= LIMIT signed */
      case 244: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==244);
{yymsp[-1].minor.yy190.limit = yymsp[0].minor.yy277;  yymsp[-1].minor.yy190.offset = 0;}
        break;
      case 241: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy190.limit = yymsp[-2].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[0].minor.yy277;}
        break;
      case 242: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy190.limit = yymsp[0].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[-2].minor.yy277;}
        break;
      case 245: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy190.limit = yymsp[-2].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[0].minor.yy277;}
        break;
      case 246: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy190.limit = yymsp[0].minor.yy277;  yymsp[-3].minor.yy190.offset = yymsp[-2].minor.yy277;}
        break;
      case 249: /* expr ::= LP expr RP */
{yylhsminor.yy194 = yymsp[-1].minor.yy194; yylhsminor.yy194->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy194->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 250: /* expr ::= ID */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 251: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 252: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 253: /* expr ::= INTEGER */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 254: /* expr ::= MINUS INTEGER */
      case 255: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==255);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 256: /* expr ::= FLOAT */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 257: /* expr ::= MINUS FLOAT */
      case 258: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==258);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 259: /* expr ::= STRING */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 260: /* expr ::= NOW */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 261: /* expr ::= TODAY */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 262: /* expr ::= VARIABLE */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 263: /* expr ::= PLUS VARIABLE */
      case 264: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==264);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 265: /* expr ::= BOOL */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 266: /* expr ::= NULL */
{ yylhsminor.yy194 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 267: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy194 = tSqlExprCreateFunction(yymsp[-1].minor.yy333, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy194 = yylhsminor.yy194;
        break;
      case 268: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy194 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy194 = yylhsminor.yy194;
        break;
      case 269: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy194 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy194, &yymsp[-1].minor.yy263, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy194 = yylhsminor.yy194;
        break;
      case 270: /* expr ::= expr IS NULL */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 271: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-3].minor.yy194, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy194 = yylhsminor.yy194;
        break;
      case 272: /* expr ::= expr LT expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LT);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 273: /* expr ::= expr GT expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_GT);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 274: /* expr ::= expr LE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 275: /* expr ::= expr GE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_GE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 276: /* expr ::= expr NE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_NE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 277: /* expr ::= expr EQ expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_EQ);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 278: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy194); yylhsminor.yy194 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy194, yymsp[-2].minor.yy194, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy194, TK_LE), TK_AND);}
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 279: /* expr ::= expr AND expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_AND);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 280: /* expr ::= expr OR expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_OR); }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 281: /* expr ::= expr PLUS expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_PLUS);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 282: /* expr ::= expr MINUS expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_MINUS); }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 283: /* expr ::= expr STAR expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_STAR);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 284: /* expr ::= expr SLASH expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_DIVIDE);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 285: /* expr ::= expr REM expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_REM);   }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 286: /* expr ::= expr BITAND expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_BITAND);}
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 287: /* expr ::= expr LIKE expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_LIKE);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 288: /* expr ::= expr MATCH expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_MATCH);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 289: /* expr ::= expr NMATCH expr */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-2].minor.yy194, yymsp[0].minor.yy194, TK_NMATCH);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 290: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 291: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 292: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy194 = yylhsminor.yy194;
        break;
      case 293: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy194 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 294: /* expr ::= arrow */
      case 298: /* expritem ::= expr */ yytestcase(yyruleno==298);
{yylhsminor.yy194 = yymsp[0].minor.yy194;}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 295: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy194 = tSqlExprCreate(yymsp[-4].minor.yy194, (tSqlExpr*)yymsp[-1].minor.yy333, TK_IN); }
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 296: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy333 = tSqlExprListAppend(yymsp[-2].minor.yy333,yymsp[0].minor.yy194,0, 0);}
  yymsp[-2].minor.yy333 = yylhsminor.yy333;
        break;
      case 297: /* exprlist ::= expritem */
{yylhsminor.yy333 = tSqlExprListAppend(0,yymsp[0].minor.yy194,0, 0);}
  yymsp[0].minor.yy333 = yylhsminor.yy333;
        break;
      case 300: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 301: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 302: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 303: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 306: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 307: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 308: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy42, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 314: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 316: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy42, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 317: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy333, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 318: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 319: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 320: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      case 321: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
