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
#define YYNOCODE 286
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateAcctInfo yy31;
  SSqlNode* yy86;
  TAOS_FIELD yy103;
  tVariant yy176;
  tSqlExpr* yy226;
  SWindowStateVal yy228;
  SArray* yy231;
  SCreatedTableInfo yy306;
  int32_t yy310;
  SSessionWindowVal yy409;
  SCreateTableSql* yy422;
  SIntervalVal yy430;
  SLimitVal yy444;
  SRangeVal yy480;
  SRelationInfo* yy484;
  int yy502;
  SCreateDbInfo yy532;
  int64_t yy549;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             395
#define YYNRULE              316
#define YYNTOKEN             201
#define YY_MAX_SHIFT         394
#define YY_MIN_SHIFTREDUCE   619
#define YY_MAX_SHIFTREDUCE   934
#define YY_ERROR_ACTION      935
#define YY_ACCEPT_ACTION     936
#define YY_NO_ACTION         937
#define YY_MIN_REDUCE        938
#define YY_MAX_REDUCE        1253
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
#define YY_ACTTAB_COUNT (874)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   216,  670,  252,  262,  261, 1169,   24, 1170,  314,  671,
 /*    10 */  1227,  870, 1229,  873,   38,   39, 1227,   42,   43,  393,
 /*    20 */   243,  265,   31,   30,   29,  214,  670,   41,  346,   46,
 /*    30 */    44,   47,   45,   32,  671, 1227,  215,   37,   36,  220,
 /*    40 */   706,   35,   34,   33,   38,   39, 1227,   42,   43, 1227,
 /*    50 */  1117,  265,   31,   30,   29,   60, 1092,   41,  346,   46,
 /*    60 */    44,   47,   45,   32,  212,  216,  754,   37,   36,  351,
 /*    70 */   221,   35,   34,   33, 1227, 1227, 1230, 1230,   38,   39,
 /*    80 */  1227,   42,   43,  936,  394,  265,   31,   30,   29,  162,
 /*    90 */    86,   41,  346,   46,   44,   47,   45,   32,  371,  370,
 /*   100 */   245,   37,   36,  222, 1090,   35,   34,   33,   38,   39,
 /*   110 */   250,   42,   43, 1227, 1093,  265,   31,   30,   29, 1114,
 /*   120 */    59,   41,  346,   46,   44,   47,   45,   32,  233,  877,
 /*   130 */    13,   37,   36, 1108,  102,   35,   34,   33, 1227,   38,
 /*   140 */    40, 1087,   42,   43,   60,   60,  265,   31,   30,   29,
 /*   150 */   288,  864,   41,  346,   46,   44,   47,   45,   32,  295,
 /*   160 */   294,  342,   37,   36,  105,  670,   35,   34,   33,   39,
 /*   170 */   280,   42,   43,  671,  884,  265,   31,   30,   29,  284,
 /*   180 */   283,   41,  346,   46,   44,   47,   45,   32,  258,  255,
 /*   190 */   256,   37,   36, 1090, 1090,   35,   34,   33,   68,  340,
 /*   200 */   388,  387,  339,  338,  337,  386,  336,  335,  334,  385,
 /*   210 */   333,  384,  383,  620,  621,  622,  623,  624,  625,  626,
 /*   220 */   627,  628,  629,  630,  631,  632,  633,  160,  869,  244,
 /*   230 */   872,   42,   43,  824,  825,  265,   31,   30,   29,  101,
 /*   240 */    85,   41,  346,   46,   44,   47,   45,   32,  392,  391,
 /*   250 */   647,   37,   36,   60,   87,   35,   34,   33,   25, 1055,
 /*   260 */  1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 1051, 1052,
 /*   270 */  1053, 1054, 1056, 1057, 1078,  226, 1218,  236,  879,  381,
 /*   280 */    92,  868,  228,  871,  345,  874, 1227, 1249,  145,  144,
 /*   290 */   143,  227,  103,  236,  879,  354,   92,  868,  358,  871,
 /*   300 */   257,  874, 1090,  296, 1093,   60,  344,   46,   44,   47,
 /*   310 */    45,   32, 1217,  241,  242,   37,   36,  348,   69,   35,
 /*   320 */    34,   33, 1227,  264,  177, 1076, 1077,   56, 1080,  241,
 /*   330 */   242,    5,   63,  187,   69,   35,   34,   33,  186,  112,
 /*   340 */   117,  108,  116,  304,  778,    6,  878,  775,  259,  776,
 /*   350 */   359,  777, 1093,  670, 1090,  297,  287,  329,   84, 1079,
 /*   360 */  1216,  671,   48,  389, 1024,  237,  129,  123,  134,  268,
 /*   370 */  1227,  318,   98,  133,   97,  139,  142,  132,   48,  270,
 /*   380 */   271,  207,  205,  203,  136, 1108, 1108,   80,  202,  149,
 /*   390 */   148,  147,  146,   68,  274,  388,  387,  880,  875,  876,
 /*   400 */   386,  812,  246,  247,  385,  815,  384,  383,  266,   60,
 /*   410 */    60,   60,   60,  880,  875,  876, 1063,   60, 1061, 1062,
 /*   420 */    37,   36,  239, 1064,   35,   34,   33, 1065,   81, 1066,
 /*   430 */  1067,   32, 1227,  300,  301,   37,   36,   60,   52,   35,
 /*   440 */    34,   33,  159,  157,  156,  986,  269,  342,  267,  106,
 /*   450 */   357,  356,  197,  131,  360,  361,  367,  368, 1090, 1090,
 /*   460 */  1090, 1090,  369,  779,  272,  381, 1090,  240,  218,  216,
 /*   470 */   219,  276,   60,  273,  223,  366,  365, 1227, 1227, 1227,
 /*   480 */  1227, 1230,  373,  217, 1227, 1081, 1090,  224,  225,  230,
 /*   490 */   231,  232,  229, 1227,  213,    1,  185, 1227, 1227, 1227,
 /*   500 */  1227, 1227, 1227,  275, 1227,  275,  275,  796, 1167,  275,
 /*   510 */  1168,  100,  844,   99,  183,  793,  184,  347,  248,  996,
 /*   520 */  1091, 1089,  987,  289,    3,  198,  197,   89,   90,  197,
 /*   530 */   299,  298,  821,  831,  832,  344,   77,   61,  350,  764,
 /*   540 */   321,  766,   10,  323,  765,   55,  164,   72,   49,  909,
 /*   550 */   881,  263,  317,   61,   61,   72,   83,  104,   72,  669,
 /*   560 */   349,  363,  362,    9,    9,  291,  291,   15, 1241,   14,
 /*   570 */   843, 1180,  122,    9,  121,   17, 1179,   16,  324,   78,
 /*   580 */   785,  783,  786,  784,   19,  253,   18,  128,  753,  127,
 /*   590 */  1176,  867,   21,  800,   20, 1175,  141,  140,  254,  372,
 /*   600 */    26,  285, 1109,  161, 1116, 1127, 1124, 1125,  292, 1129,
 /*   610 */   163,  168,  310,  179, 1159, 1158, 1157, 1156, 1088,  180,
 /*   620 */  1086,  158,  181,  182, 1001,  326,  811,  327,  328,  303,
 /*   630 */   331,  332,   70,  210,   66,  343,  249,  995,  305,  307,
 /*   640 */  1106,  169,  355, 1248,  319,  170,   82,  119, 1247,   79,
 /*   650 */    28,  171, 1244,  188,  364, 1240,  315,  172,  173,  125,
 /*   660 */   313,  311, 1239,  309, 1236,  189,  174,  306, 1021,   67,
 /*   670 */   175,   62,   71,  211,  302,  983,  330,  135,  981,  137,
 /*   680 */   138,  979,  978,  277,  200,  201,  975,  974,  973,  972,
 /*   690 */   971,  970,   27,  969,  204,  206,  965,  382,  963,  961,
 /*   700 */   208,  958,  209,  954,  130,  374,  290,   88,   93,  375,
 /*   710 */   308,  376,  377,  379,  378,  380,  390,  934,  238,  278,
 /*   720 */   279,  260,  325,  933,  281,  282,  932,  915,  234,  235,
 /*   730 */   914,  286,  291,  113, 1000,  999,  320,  114,   11,   91,
 /*   740 */   788,  293,   53,   94,  820,  977,  976,   75,  968,  192,
 /*   750 */   150,  151, 1022,  190,  194,  152,  191,  193,  195,  196,
 /*   760 */   967,    4,  153, 1059,  960, 1023,  178,  176,  959,   54,
 /*   770 */   818,  817,    2,  814,  813,   76,  167,  822,  165, 1069,
 /*   780 */   833,  166,  251,  827,   95,   64,  829,   96,  312,  349,
 /*   790 */   316,   12,   65,   50,   51,   22,   23,  322,  107,  105,
 /*   800 */    57,  109,  110,  684,   58,  719,  717,  111,  716,  715,
 /*   810 */   713,  712,  711,  708,  674,  341,  115,    7,  906,  904,
 /*   820 */   883,  907,  882,  905,  885,    8,  353,   61,  352,  118,
 /*   830 */    73,  756,  120,  782,   74,  124,  755,  781,  126,  752,
 /*   840 */   700,  698,  690,  696,  692,  694,  688,  686,  722,  721,
 /*   850 */   720,  718,  714,  710,  709,  199,  672,  637,  938,  937,
 /*   860 */   937,  937,  937,  937,  937,  937,  937,  937,  937,  937,
 /*   870 */   937,  937,  154,  155,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   272,    1,    1,  211,  211,  280,  272,  282,  283,    9,
 /*    10 */   282,    5,  284,    7,   14,   15,  282,   17,   18,  204,
 /*    20 */   205,   21,   22,   23,   24,  272,    1,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,    9,  282,  272,   37,   38,  272,
 /*    40 */     5,   41,   42,   43,   14,   15,  282,   17,   18,  282,
 /*    50 */   204,   21,   22,   23,   24,  204,  254,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,  272,  272,    5,   37,   38,   16,
 /*    70 */   272,   41,   42,   43,  282,  282,  284,  284,   14,   15,
 /*    80 */   282,   17,   18,  202,  203,   21,   22,   23,   24,  204,
 /*    90 */    90,   27,   28,   29,   30,   31,   32,   33,   37,   38,
 /*   100 */   249,   37,   38,  272,  253,   41,   42,   43,   14,   15,
 /*   110 */   250,   17,   18,  282,  254,   21,   22,   23,   24,  273,
 /*   120 */    90,   27,   28,   29,   30,   31,   32,   33,  272,  123,
 /*   130 */    86,   37,   38,  252,   90,   41,   42,   43,  282,   14,
 /*   140 */    15,  204,   17,   18,  204,  204,   21,   22,   23,   24,
 /*   150 */   269,   87,   27,   28,   29,   30,   31,   32,   33,  274,
 /*   160 */   275,   88,   37,   38,  120,    1,   41,   42,   43,   15,
 /*   170 */   147,   17,   18,    9,  121,   21,   22,   23,   24,  156,
 /*   180 */   157,   27,   28,   29,   30,   31,   32,   33,  251,  249,
 /*   190 */   249,   37,   38,  253,  253,   41,   42,   43,  102,  103,
 /*   200 */   104,  105,  106,  107,  108,  109,  110,  111,  112,  113,
 /*   210 */   114,  115,  116,   49,   50,   51,   52,   53,   54,   55,
 /*   220 */    56,   57,   58,   59,   60,   61,   62,   63,    5,   65,
 /*   230 */     7,   17,   18,  130,  131,   21,   22,   23,   24,  255,
 /*   240 */   212,   27,   28,   29,   30,   31,   32,   33,   69,   70,
 /*   250 */    71,   37,   38,  204,  270,   41,   42,   43,   48,  228,
 /*   260 */   229,  230,  231,  232,  233,  234,  235,  236,  237,  238,
 /*   270 */   239,  240,  241,  242,  246,   65,  272,    1,    2,   94,
 /*   280 */    86,    5,   72,    7,   25,    9,  282,  254,   78,   79,
 /*   290 */    80,   81,  212,    1,    2,   85,   86,    5,  249,    7,
 /*   300 */   250,    9,  253,  277,  254,  204,   47,   29,   30,   31,
 /*   310 */    32,   33,  272,   37,   38,   37,   38,   41,  124,   41,
 /*   320 */    42,   43,  282,   64,  259,  245,  246,  247,  248,   37,
 /*   330 */    38,   66,   67,   68,  124,   41,   42,   43,   73,   74,
 /*   340 */    75,   76,   77,  278,    2,   86,  123,    5,  250,    7,
 /*   350 */   249,    9,  254,    1,  253,  277,  146,   92,  148,    0,
 /*   360 */   272,    9,   86,  226,  227,  155,   66,   67,   68,   72,
 /*   370 */   282,  279,  280,   73,  282,   75,   76,   77,   86,   37,
 /*   380 */    38,   66,   67,   68,   84,  252,  252,  101,   73,   74,
 /*   390 */    75,   76,   77,  102,   72,  104,  105,  121,  122,  123,
 /*   400 */   109,    5,  269,  269,  113,    9,  115,  116,  211,  204,
 /*   410 */   204,  204,  204,  121,  122,  123,  228,  204,  230,  231,
 /*   420 */    37,   38,  272,  235,   41,   42,   43,  239,  142,  241,
 /*   430 */   242,   33,  282,   37,   38,   37,   38,  204,   86,   41,
 /*   440 */    42,   43,   66,   67,   68,  210,  149,   88,  151,  212,
 /*   450 */   153,  154,  217,   82,  249,  249,  249,  249,  253,  253,
 /*   460 */   253,  253,  249,  121,  122,   94,  253,  272,  272,  272,
 /*   470 */   272,  149,  204,  151,  272,  153,  154,  282,  282,  282,
 /*   480 */   282,  284,  249,  272,  282,  248,  253,  272,  272,  272,
 /*   490 */   272,  272,  272,  282,  272,  213,  214,  282,  282,  282,
 /*   500 */   282,  282,  282,  204,  282,  204,  204,   41,  280,  204,
 /*   510 */   282,  280,   80,  282,  215,  101,  215,  215,  122,  210,
 /*   520 */   215,  253,  210,   87,  208,  209,  217,   87,   87,  217,
 /*   530 */    37,   38,   87,   87,   87,   47,  101,  101,   25,   87,
 /*   540 */    87,   87,  128,   87,   87,   86,  101,  101,  101,   87,
 /*   550 */    87,    1,   64,  101,  101,  101,   86,  101,  101,   87,
 /*   560 */    47,   37,   38,  101,  101,  125,  125,  150,  254,  152,
 /*   570 */   138,  244,  150,  101,  152,  150,  244,  152,  119,  144,
 /*   580 */     5,    5,    7,    7,  150,  244,  152,  150,  118,  152,
 /*   590 */   244,   41,  150,  127,  152,  244,   82,   83,  244,  244,
 /*   600 */   271,  204,  252,  204,  204,  204,  204,  204,  252,  204,
 /*   610 */   204,  204,  204,  256,  281,  281,  281,  281,  252,  204,
 /*   620 */   204,   64,  204,  204,  204,  204,  123,  204,  204,  276,
 /*   630 */   204,  204,  204,  204,  204,  204,  276,  204,  276,  276,
 /*   640 */   268,  267,  204,  204,  136,  266,  141,  204,  204,  143,
 /*   650 */   140,  265,  204,  204,  204,  204,  139,  264,  263,  204,
 /*   660 */   134,  133,  204,  132,  204,  204,  262,  135,  204,  204,
 /*   670 */   261,  204,  204,  204,  129,  204,   93,  204,  204,  204,
 /*   680 */   204,  204,  204,  204,  204,  204,  204,  204,  204,  204,
 /*   690 */   204,  204,  145,  204,  204,  204,  204,  117,  204,  204,
 /*   700 */   204,  204,  204,  204,  100,   99,  206,  206,  206,   55,
 /*   710 */   206,   96,   98,   97,   59,   95,   88,    5,  206,  158,
 /*   720 */     5,  206,  206,    5,  158,    5,    5,  104,  206,  206,
 /*   730 */   103,  147,  125,  212,  216,  216,  119,  212,   86,  126,
 /*   740 */    87,  101,   86,  101,   87,  206,  206,  101,  206,  219,
 /*   750 */   207,  207,  225,  224,  220,  207,  223,  222,  221,  218,
 /*   760 */   206,  208,  207,  243,  206,  227,  257,  260,  206,  258,
 /*   770 */   123,  123,  213,    5,    5,   86,  101,   87,   86,  243,
 /*   780 */    87,   86,    1,   87,   86,  101,   87,   86,   86,   47,
 /*   790 */     1,   86,  101,   86,   86,  137,  137,  119,   82,  120,
 /*   800 */    91,   90,   74,    5,   91,    9,    5,   90,    5,    5,
 /*   810 */     5,    5,    5,    5,   89,   16,   82,   86,    9,    9,
 /*   820 */    87,    9,   87,    9,  121,   86,   63,  101,   28,  152,
 /*   830 */    17,    5,  152,  123,   17,  152,    5,  123,  152,   87,
 /*   840 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   850 */     5,    5,    5,    5,    5,  101,   89,   64,    0,  285,
 /*   860 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   870 */   285,  285,   22,   22,  285,  285,  285,  285,  285,  285,
 /*   880 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   890 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   900 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   910 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   920 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   930 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   940 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   950 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   960 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   970 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   980 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   990 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1000 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1010 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1020 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1030 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1040 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1050 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1060 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*  1070 */   285,  285,  285,  285,  285,
};
#define YY_SHIFT_COUNT    (394)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (858)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   210,   96,   96,  291,  291,   73,  276,  292,  292,  292,
 /*    10 */   352,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*    20 */    25,   25,    1,    1,    0,  164,  292,  292,  292,  292,
 /*    30 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  292,
 /*    40 */   292,  292,  292,  292,  292,  292,  292,  292,  292,  342,
 /*    50 */   342,  342,  194,  194,  103,   25,  359,   25,   25,   25,
 /*    60 */    25,   25,  371,   73,    1,    1,  185,  185,   35,  874,
 /*    70 */   874,  874,  342,  342,  342,  396,  396,   61,   61,   61,
 /*    80 */    61,   61,   61,   61,   25,   25,   25,  466,   25,   25,
 /*    90 */    25,  194,  194,   25,   25,   25,   25,  432,  432,  432,
 /*   100 */   432,  414,  194,   25,   25,   25,   25,   25,   25,   25,
 /*   110 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   120 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   130 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   140 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   150 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   160 */    25,  557,  557,  557,  503,  503,  503,  503,  557,  505,
 /*   170 */   506,  508,  510,  517,  526,  528,  531,  532,  545,  547,
 /*   180 */   557,  557,  557,  583,  583,  580,   73,   73,  557,  557,
 /*   190 */   604,  606,  654,  615,  614,  655,  616,  620,  580,   35,
 /*   200 */   557,  557,  628,  628,  557,  628,  557,  628,  557,  557,
 /*   210 */   874,  874,   30,   64,   94,   94,   94,  125,  154,  214,
 /*   220 */   278,  278,  278,  278,  278,  278,  265,  300,  315,  398,
 /*   230 */   398,  398,  398,  383,  297,  322,  259,   23,   44,  294,
 /*   240 */   294,    6,  223,  179,  376,  436,  440,  441,  493,  445,
 /*   250 */   446,  447,  488,  435,  286,  452,  453,  454,  456,  457,
 /*   260 */   459,  462,  463,  513,  550,   53,  472,  417,  422,  425,
 /*   270 */   575,  576,  524,  434,  437,  470,  442,  514,  712,  561,
 /*   280 */   715,  718,  566,  720,  721,  623,  627,  584,  607,  617,
 /*   290 */   652,  613,  653,  656,  640,  642,  657,  646,  647,  648,
 /*   300 */   768,  769,  689,  690,  692,  693,  695,  696,  675,  698,
 /*   310 */   699,  701,  781,  702,  684,  658,  742,  789,  691,  659,
 /*   320 */   705,  617,  707,  678,  708,  679,  716,  709,  711,  728,
 /*   330 */   798,  713,  717,  796,  801,  803,  804,  805,  806,  807,
 /*   340 */   808,  725,  799,  734,  809,  810,  731,  733,  735,  812,
 /*   350 */   814,  703,  739,  800,  763,  813,  677,  680,  726,  726,
 /*   360 */   726,  726,  710,  714,  817,  683,  686,  726,  726,  726,
 /*   370 */   826,  831,  752,  726,  835,  836,  837,  838,  839,  840,
 /*   380 */   841,  842,  843,  844,  845,  846,  847,  848,  849,  754,
 /*   390 */   767,  850,  851,  793,  858,
};
#define YY_REDUCE_COUNT (211)
#define YY_REDUCE_MIN   (-275)
#define YY_REDUCE_MAX   (562)
static const short yy_reduce_ofst[] = {
 /*     0 */  -119,   31,   31,  188,  188,   80, -208, -207,  197, -272,
 /*    10 */  -115, -149,  -60,  -59,   49,  101,  205,  206,  207,  208,
 /*    20 */   213,  233, -275,   92, -154, -185, -266, -247, -236, -233,
 /*    30 */  -202, -169, -144,    4,   40,   88,  150,  195,  196,  198,
 /*    40 */   202,  211,  215,  216,  217,  218,  219,  220,  222, -140,
 /*    50 */    50,   98,  133,  134,   65,  -63,  237,  299,  301,  302,
 /*    60 */   305,  268,  235,   28,  228,  231,  309,  312,  137,  -16,
 /*    70 */   282,  316, -198,   33,  314,   26,   78,  327,  332,  341,
 /*    80 */   346,  351,  354,  355,  397,  399,  400,  329,  401,  402,
 /*    90 */   403,  350,  356,  405,  406,  407,  408,  333,  334,  335,
 /*   100 */   336,  357,  366,  415,  416,  418,  419,  420,  421,  423,
 /*   110 */   424,  426,  427,  428,  429,  430,  431,  433,  438,  439,
 /*   120 */   443,  444,  448,  449,  450,  451,  455,  458,  460,  461,
 /*   130 */   464,  465,  467,  468,  469,  471,  473,  474,  475,  476,
 /*   140 */   477,  478,  479,  480,  481,  482,  483,  484,  485,  486,
 /*   150 */   487,  489,  490,  491,  492,  494,  495,  496,  497,  498,
 /*   160 */   499,  500,  501,  502,  353,  360,  362,  363,  504,  372,
 /*   170 */   374,  379,  386,  393,  395,  404,  409,  507,  511,  509,
 /*   180 */   512,  515,  516,  518,  519,  520,  521,  525,  522,  523,
 /*   190 */   527,  529,  533,  530,  535,  534,  537,  541,  536,  538,
 /*   200 */   539,  540,  543,  544,  542,  548,  554,  555,  558,  562,
 /*   210 */   559,  553,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   935, 1058,  997, 1068,  984,  994, 1232, 1232, 1232, 1232,
 /*    10 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*    20 */   935,  935,  935,  935, 1118,  955,  935,  935,  935,  935,
 /*    30 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*    40 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*    50 */   935,  935,  935,  935, 1142,  935,  994,  935,  935,  935,
 /*    60 */   935,  935, 1004,  994,  935,  935, 1004, 1004,  935, 1113,
 /*    70 */  1042, 1060,  935,  935,  935,  935,  935,  935,  935,  935,
 /*    80 */   935,  935,  935,  935,  935,  935,  935, 1120, 1126, 1123,
 /*    90 */   935,  935,  935, 1128,  935,  935,  935, 1164, 1164, 1164,
 /*   100 */  1164, 1111,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   110 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   120 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   130 */   935,  935,  935,  935,  935,  982,  935,  980,  935,  935,
 /*   140 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   150 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   160 */   953,  957,  957,  957,  935,  935,  935,  935,  957, 1173,
 /*   170 */  1177, 1154, 1171, 1165, 1149, 1147, 1145, 1153, 1138, 1181,
 /*   180 */   957,  957,  957, 1002, 1002,  998,  994,  994,  957,  957,
 /*   190 */  1020, 1018, 1016, 1008, 1014, 1010, 1012, 1006,  985,  935,
 /*   200 */   957,  957,  992,  992,  957,  992,  957,  992,  957,  957,
 /*   210 */  1042, 1060, 1231,  935, 1182, 1172, 1231,  935, 1213, 1212,
 /*   220 */  1222, 1221, 1220, 1211, 1210, 1209,  935,  935,  935, 1205,
 /*   230 */  1208, 1207, 1206, 1219,  935,  935, 1184,  935,  935, 1215,
 /*   240 */  1214,  935,  935,  935,  935,  935,  935,  935, 1135,  935,
 /*   250 */   935,  935, 1160, 1178, 1174,  935,  935,  935,  935,  935,
 /*   260 */   935,  935,  935, 1185,  935,  935,  935,  935,  935,  935,
 /*   270 */   935,  935, 1099,  935,  935, 1070,  935,  935,  935,  935,
 /*   280 */   935,  935,  935,  935,  935,  935,  935,  935, 1110,  935,
 /*   290 */   935,  935,  935,  935, 1122, 1121,  935,  935,  935,  935,
 /*   300 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   310 */   935,  935,  935,  935, 1166,  935, 1161,  935, 1155,  935,
 /*   320 */   935, 1082,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   330 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   340 */   935,  935,  935,  935,  935,  935,  935,  935,  935,  935,
 /*   350 */   935,  935,  935,  935,  935,  935,  935,  935, 1250, 1245,
 /*   360 */  1246, 1243,  935,  935,  935,  935,  935, 1242, 1237, 1238,
 /*   370 */   935,  935,  935, 1235,  935,  935,  935,  935,  935,  935,
 /*   380 */   935,  935,  935,  935,  935,  935,  935,  935,  935, 1026,
 /*   390 */   935,  964,  962,  935,  935,
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
  /*  121 */ "NULL",
  /*  122 */ "NOW",
  /*  123 */ "VARIABLE",
  /*  124 */ "SELECT",
  /*  125 */ "UNION",
  /*  126 */ "ALL",
  /*  127 */ "DISTINCT",
  /*  128 */ "FROM",
  /*  129 */ "RANGE",
  /*  130 */ "INTERVAL",
  /*  131 */ "EVERY",
  /*  132 */ "SESSION",
  /*  133 */ "STATE_WINDOW",
  /*  134 */ "FILL",
  /*  135 */ "SLIDING",
  /*  136 */ "ORDER",
  /*  137 */ "BY",
  /*  138 */ "ASC",
  /*  139 */ "GROUP",
  /*  140 */ "HAVING",
  /*  141 */ "LIMIT",
  /*  142 */ "OFFSET",
  /*  143 */ "SLIMIT",
  /*  144 */ "SOFFSET",
  /*  145 */ "WHERE",
  /*  146 */ "RESET",
  /*  147 */ "QUERY",
  /*  148 */ "SYNCDB",
  /*  149 */ "ADD",
  /*  150 */ "COLUMN",
  /*  151 */ "MODIFY",
  /*  152 */ "TAG",
  /*  153 */ "CHANGE",
  /*  154 */ "SET",
  /*  155 */ "KILL",
  /*  156 */ "CONNECTION",
  /*  157 */ "STREAM",
  /*  158 */ "COLON",
  /*  159 */ "ABORT",
  /*  160 */ "AFTER",
  /*  161 */ "ATTACH",
  /*  162 */ "BEFORE",
  /*  163 */ "BEGIN",
  /*  164 */ "CASCADE",
  /*  165 */ "CLUSTER",
  /*  166 */ "CONFLICT",
  /*  167 */ "COPY",
  /*  168 */ "DEFERRED",
  /*  169 */ "DELIMITERS",
  /*  170 */ "DETACH",
  /*  171 */ "EACH",
  /*  172 */ "END",
  /*  173 */ "EXPLAIN",
  /*  174 */ "FAIL",
  /*  175 */ "FOR",
  /*  176 */ "IGNORE",
  /*  177 */ "IMMEDIATE",
  /*  178 */ "INITIALLY",
  /*  179 */ "INSTEAD",
  /*  180 */ "KEY",
  /*  181 */ "OF",
  /*  182 */ "RAISE",
  /*  183 */ "REPLACE",
  /*  184 */ "RESTRICT",
  /*  185 */ "ROW",
  /*  186 */ "STATEMENT",
  /*  187 */ "TRIGGER",
  /*  188 */ "VIEW",
  /*  189 */ "IPTOKEN",
  /*  190 */ "SEMI",
  /*  191 */ "NONE",
  /*  192 */ "PREV",
  /*  193 */ "LINEAR",
  /*  194 */ "IMPORT",
  /*  195 */ "TBNAME",
  /*  196 */ "JOIN",
  /*  197 */ "INSERT",
  /*  198 */ "INTO",
  /*  199 */ "VALUES",
  /*  200 */ "FILE",
  /*  201 */ "error",
  /*  202 */ "program",
  /*  203 */ "cmd",
  /*  204 */ "ids",
  /*  205 */ "dbPrefix",
  /*  206 */ "cpxName",
  /*  207 */ "ifexists",
  /*  208 */ "alter_db_optr",
  /*  209 */ "alter_topic_optr",
  /*  210 */ "acct_optr",
  /*  211 */ "exprlist",
  /*  212 */ "ifnotexists",
  /*  213 */ "db_optr",
  /*  214 */ "topic_optr",
  /*  215 */ "typename",
  /*  216 */ "bufsize",
  /*  217 */ "pps",
  /*  218 */ "tseries",
  /*  219 */ "dbs",
  /*  220 */ "streams",
  /*  221 */ "storage",
  /*  222 */ "qtime",
  /*  223 */ "users",
  /*  224 */ "conns",
  /*  225 */ "state",
  /*  226 */ "intitemlist",
  /*  227 */ "intitem",
  /*  228 */ "keep",
  /*  229 */ "cache",
  /*  230 */ "replica",
  /*  231 */ "quorum",
  /*  232 */ "days",
  /*  233 */ "minrows",
  /*  234 */ "maxrows",
  /*  235 */ "blocks",
  /*  236 */ "ctime",
  /*  237 */ "wal",
  /*  238 */ "fsync",
  /*  239 */ "comp",
  /*  240 */ "prec",
  /*  241 */ "update",
  /*  242 */ "cachelast",
  /*  243 */ "partitions",
  /*  244 */ "signed",
  /*  245 */ "create_table_args",
  /*  246 */ "create_stable_args",
  /*  247 */ "create_table_list",
  /*  248 */ "create_from_stable",
  /*  249 */ "columnlist",
  /*  250 */ "tagitemlist",
  /*  251 */ "tagNamelist",
  /*  252 */ "select",
  /*  253 */ "column",
  /*  254 */ "tagitem",
  /*  255 */ "selcollist",
  /*  256 */ "from",
  /*  257 */ "where_opt",
  /*  258 */ "range_option",
  /*  259 */ "interval_option",
  /*  260 */ "sliding_opt",
  /*  261 */ "session_option",
  /*  262 */ "windowstate_option",
  /*  263 */ "fill_opt",
  /*  264 */ "groupby_opt",
  /*  265 */ "having_opt",
  /*  266 */ "orderby_opt",
  /*  267 */ "slimit_opt",
  /*  268 */ "limit_opt",
  /*  269 */ "union",
  /*  270 */ "sclp",
  /*  271 */ "distinct",
  /*  272 */ "expr",
  /*  273 */ "as",
  /*  274 */ "tablelist",
  /*  275 */ "sub",
  /*  276 */ "tmvar",
  /*  277 */ "timestamp",
  /*  278 */ "intervalKey",
  /*  279 */ "sortlist",
  /*  280 */ "item",
  /*  281 */ "sortorder",
  /*  282 */ "arrow",
  /*  283 */ "grouplist",
  /*  284 */ "expritem",
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
 /* 162 */ "tagitem ::= NOW PLUS VARIABLE",
 /* 163 */ "tagitem ::= NOW MINUS VARIABLE",
 /* 164 */ "tagitem ::= MINUS INTEGER",
 /* 165 */ "tagitem ::= MINUS FLOAT",
 /* 166 */ "tagitem ::= PLUS INTEGER",
 /* 167 */ "tagitem ::= PLUS FLOAT",
 /* 168 */ "select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 169 */ "select ::= LP select RP",
 /* 170 */ "union ::= select",
 /* 171 */ "union ::= union UNION ALL select",
 /* 172 */ "cmd ::= union",
 /* 173 */ "select ::= SELECT selcollist",
 /* 174 */ "sclp ::= selcollist COMMA",
 /* 175 */ "sclp ::=",
 /* 176 */ "selcollist ::= sclp distinct expr as",
 /* 177 */ "selcollist ::= sclp STAR",
 /* 178 */ "as ::= AS ids",
 /* 179 */ "as ::= ids",
 /* 180 */ "as ::=",
 /* 181 */ "distinct ::= DISTINCT",
 /* 182 */ "distinct ::=",
 /* 183 */ "from ::= FROM tablelist",
 /* 184 */ "from ::= FROM sub",
 /* 185 */ "sub ::= LP union RP",
 /* 186 */ "sub ::= LP union RP ids",
 /* 187 */ "sub ::= sub COMMA LP union RP ids",
 /* 188 */ "tablelist ::= ids cpxName",
 /* 189 */ "tablelist ::= ids cpxName ids",
 /* 190 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 191 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 192 */ "tmvar ::= VARIABLE",
 /* 193 */ "timestamp ::= INTEGER",
 /* 194 */ "timestamp ::= MINUS INTEGER",
 /* 195 */ "timestamp ::= PLUS INTEGER",
 /* 196 */ "timestamp ::= STRING",
 /* 197 */ "timestamp ::= NOW",
 /* 198 */ "timestamp ::= NOW PLUS VARIABLE",
 /* 199 */ "timestamp ::= NOW MINUS VARIABLE",
 /* 200 */ "range_option ::=",
 /* 201 */ "range_option ::= RANGE LP timestamp COMMA timestamp RP",
 /* 202 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 203 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 204 */ "interval_option ::=",
 /* 205 */ "intervalKey ::= INTERVAL",
 /* 206 */ "intervalKey ::= EVERY",
 /* 207 */ "session_option ::=",
 /* 208 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 209 */ "windowstate_option ::=",
 /* 210 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 211 */ "fill_opt ::=",
 /* 212 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 213 */ "fill_opt ::= FILL LP ID RP",
 /* 214 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 215 */ "sliding_opt ::=",
 /* 216 */ "orderby_opt ::=",
 /* 217 */ "orderby_opt ::= ORDER BY sortlist",
 /* 218 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 219 */ "sortlist ::= sortlist COMMA arrow sortorder",
 /* 220 */ "sortlist ::= item sortorder",
 /* 221 */ "sortlist ::= arrow sortorder",
 /* 222 */ "item ::= ID",
 /* 223 */ "item ::= ID DOT ID",
 /* 224 */ "sortorder ::= ASC",
 /* 225 */ "sortorder ::= DESC",
 /* 226 */ "sortorder ::=",
 /* 227 */ "groupby_opt ::=",
 /* 228 */ "groupby_opt ::= GROUP BY grouplist",
 /* 229 */ "grouplist ::= grouplist COMMA item",
 /* 230 */ "grouplist ::= grouplist COMMA arrow",
 /* 231 */ "grouplist ::= item",
 /* 232 */ "grouplist ::= arrow",
 /* 233 */ "having_opt ::=",
 /* 234 */ "having_opt ::= HAVING expr",
 /* 235 */ "limit_opt ::=",
 /* 236 */ "limit_opt ::= LIMIT signed",
 /* 237 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 238 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 239 */ "slimit_opt ::=",
 /* 240 */ "slimit_opt ::= SLIMIT signed",
 /* 241 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 242 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 243 */ "where_opt ::=",
 /* 244 */ "where_opt ::= WHERE expr",
 /* 245 */ "expr ::= LP expr RP",
 /* 246 */ "expr ::= ID",
 /* 247 */ "expr ::= ID DOT ID",
 /* 248 */ "expr ::= ID DOT STAR",
 /* 249 */ "expr ::= INTEGER",
 /* 250 */ "expr ::= MINUS INTEGER",
 /* 251 */ "expr ::= PLUS INTEGER",
 /* 252 */ "expr ::= FLOAT",
 /* 253 */ "expr ::= MINUS FLOAT",
 /* 254 */ "expr ::= PLUS FLOAT",
 /* 255 */ "expr ::= STRING",
 /* 256 */ "expr ::= NOW",
 /* 257 */ "expr ::= VARIABLE",
 /* 258 */ "expr ::= PLUS VARIABLE",
 /* 259 */ "expr ::= MINUS VARIABLE",
 /* 260 */ "expr ::= BOOL",
 /* 261 */ "expr ::= NULL",
 /* 262 */ "expr ::= ID LP exprlist RP",
 /* 263 */ "expr ::= ID LP STAR RP",
 /* 264 */ "expr ::= ID LP expr AS typename RP",
 /* 265 */ "expr ::= expr IS NULL",
 /* 266 */ "expr ::= expr IS NOT NULL",
 /* 267 */ "expr ::= expr LT expr",
 /* 268 */ "expr ::= expr GT expr",
 /* 269 */ "expr ::= expr LE expr",
 /* 270 */ "expr ::= expr GE expr",
 /* 271 */ "expr ::= expr NE expr",
 /* 272 */ "expr ::= expr EQ expr",
 /* 273 */ "expr ::= expr BETWEEN expr AND expr",
 /* 274 */ "expr ::= expr AND expr",
 /* 275 */ "expr ::= expr OR expr",
 /* 276 */ "expr ::= expr PLUS expr",
 /* 277 */ "expr ::= expr MINUS expr",
 /* 278 */ "expr ::= expr STAR expr",
 /* 279 */ "expr ::= expr SLASH expr",
 /* 280 */ "expr ::= expr REM expr",
 /* 281 */ "expr ::= expr BITAND expr",
 /* 282 */ "expr ::= expr LIKE expr",
 /* 283 */ "expr ::= expr MATCH expr",
 /* 284 */ "expr ::= expr NMATCH expr",
 /* 285 */ "expr ::= ID CONTAINS STRING",
 /* 286 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 287 */ "arrow ::= ID ARROW STRING",
 /* 288 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 289 */ "expr ::= arrow",
 /* 290 */ "expr ::= expr IN LP exprlist RP",
 /* 291 */ "exprlist ::= exprlist COMMA expritem",
 /* 292 */ "exprlist ::= expritem",
 /* 293 */ "expritem ::= expr",
 /* 294 */ "expritem ::=",
 /* 295 */ "cmd ::= RESET QUERY CACHE",
 /* 296 */ "cmd ::= SYNCDB ids REPLICA",
 /* 297 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 298 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 299 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 300 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 301 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 302 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 303 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 304 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 305 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 306 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 307 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 308 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 309 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 310 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 311 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 312 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 313 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 314 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 315 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 211: /* exprlist */
    case 255: /* selcollist */
    case 270: /* sclp */
{
tSqlExprListDestroy((yypminor->yy231));
}
      break;
    case 226: /* intitemlist */
    case 228: /* keep */
    case 249: /* columnlist */
    case 250: /* tagitemlist */
    case 251: /* tagNamelist */
    case 263: /* fill_opt */
    case 264: /* groupby_opt */
    case 266: /* orderby_opt */
    case 279: /* sortlist */
    case 283: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy231));
}
      break;
    case 247: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy422));
}
      break;
    case 252: /* select */
{
destroySqlNode((yypminor->yy86));
}
      break;
    case 256: /* from */
    case 274: /* tablelist */
    case 275: /* sub */
{
destroyRelationInfo((yypminor->yy484));
}
      break;
    case 257: /* where_opt */
    case 265: /* having_opt */
    case 272: /* expr */
    case 277: /* timestamp */
    case 282: /* arrow */
    case 284: /* expritem */
{
tSqlExprDestroy((yypminor->yy226));
}
      break;
    case 269: /* union */
{
destroyAllSqlNode((yypminor->yy231));
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
  {  202,   -1 }, /* (0) program ::= cmd */
  {  203,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  203,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  203,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  203,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  203,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  203,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  203,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  203,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  203,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  203,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  203,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  203,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  203,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  203,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  203,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  203,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  205,    0 }, /* (17) dbPrefix ::= */
  {  205,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  206,    0 }, /* (19) cpxName ::= */
  {  206,   -2 }, /* (20) cpxName ::= DOT ids */
  {  203,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  203,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  203,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  203,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  203,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  203,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  203,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  203,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  203,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  203,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  203,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  203,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  203,   -3 }, /* (33) cmd ::= DROP FUNCTION ids */
  {  203,   -3 }, /* (34) cmd ::= DROP DNODE ids */
  {  203,   -3 }, /* (35) cmd ::= DROP USER ids */
  {  203,   -3 }, /* (36) cmd ::= DROP ACCOUNT ids */
  {  203,   -2 }, /* (37) cmd ::= USE ids */
  {  203,   -3 }, /* (38) cmd ::= DESCRIBE ids cpxName */
  {  203,   -3 }, /* (39) cmd ::= DESC ids cpxName */
  {  203,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  203,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  203,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  203,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  203,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  203,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  203,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  203,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  203,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  203,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  203,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  204,   -1 }, /* (51) ids ::= ID */
  {  204,   -1 }, /* (52) ids ::= STRING */
  {  207,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  207,    0 }, /* (54) ifexists ::= */
  {  212,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  212,    0 }, /* (56) ifnotexists ::= */
  {  203,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  203,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  203,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  203,   -5 }, /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  203,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  203,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  203,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  216,    0 }, /* (64) bufsize ::= */
  {  216,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  217,    0 }, /* (66) pps ::= */
  {  217,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  218,    0 }, /* (68) tseries ::= */
  {  218,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  219,    0 }, /* (70) dbs ::= */
  {  219,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  220,    0 }, /* (72) streams ::= */
  {  220,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  221,    0 }, /* (74) storage ::= */
  {  221,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  222,    0 }, /* (76) qtime ::= */
  {  222,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  223,    0 }, /* (78) users ::= */
  {  223,   -2 }, /* (79) users ::= USERS INTEGER */
  {  224,    0 }, /* (80) conns ::= */
  {  224,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  225,    0 }, /* (82) state ::= */
  {  225,   -2 }, /* (83) state ::= STATE ids */
  {  210,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  226,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  226,   -1 }, /* (86) intitemlist ::= intitem */
  {  227,   -1 }, /* (87) intitem ::= INTEGER */
  {  228,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  229,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  230,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  231,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  232,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  233,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  234,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  235,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  236,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  237,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  238,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  239,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  240,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  241,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  242,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  243,   -2 }, /* (103) partitions ::= PARTITIONS INTEGER */
  {  213,    0 }, /* (104) db_optr ::= */
  {  213,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  213,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  213,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  213,   -2 }, /* (108) db_optr ::= db_optr days */
  {  213,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  213,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  213,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  213,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  213,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  213,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  213,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  213,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  213,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  213,   -2 }, /* (118) db_optr ::= db_optr update */
  {  213,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  214,   -1 }, /* (120) topic_optr ::= db_optr */
  {  214,   -2 }, /* (121) topic_optr ::= topic_optr partitions */
  {  208,    0 }, /* (122) alter_db_optr ::= */
  {  208,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  208,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  208,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  208,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  208,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  208,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  208,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  209,   -1 }, /* (130) alter_topic_optr ::= alter_db_optr */
  {  209,   -2 }, /* (131) alter_topic_optr ::= alter_topic_optr partitions */
  {  215,   -1 }, /* (132) typename ::= ids */
  {  215,   -4 }, /* (133) typename ::= ids LP signed RP */
  {  215,   -2 }, /* (134) typename ::= ids UNSIGNED */
  {  244,   -1 }, /* (135) signed ::= INTEGER */
  {  244,   -2 }, /* (136) signed ::= PLUS INTEGER */
  {  244,   -2 }, /* (137) signed ::= MINUS INTEGER */
  {  203,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_args */
  {  203,   -3 }, /* (139) cmd ::= CREATE TABLE create_stable_args */
  {  203,   -3 }, /* (140) cmd ::= CREATE STABLE create_stable_args */
  {  203,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_list */
  {  247,   -1 }, /* (142) create_table_list ::= create_from_stable */
  {  247,   -2 }, /* (143) create_table_list ::= create_table_list create_from_stable */
  {  245,   -6 }, /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  246,  -10 }, /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  248,  -10 }, /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  248,  -13 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  251,   -3 }, /* (148) tagNamelist ::= tagNamelist COMMA ids */
  {  251,   -1 }, /* (149) tagNamelist ::= ids */
  {  245,   -5 }, /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
  {  249,   -3 }, /* (151) columnlist ::= columnlist COMMA column */
  {  249,   -1 }, /* (152) columnlist ::= column */
  {  253,   -2 }, /* (153) column ::= ids typename */
  {  250,   -3 }, /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
  {  250,   -1 }, /* (155) tagitemlist ::= tagitem */
  {  254,   -1 }, /* (156) tagitem ::= INTEGER */
  {  254,   -1 }, /* (157) tagitem ::= FLOAT */
  {  254,   -1 }, /* (158) tagitem ::= STRING */
  {  254,   -1 }, /* (159) tagitem ::= BOOL */
  {  254,   -1 }, /* (160) tagitem ::= NULL */
  {  254,   -1 }, /* (161) tagitem ::= NOW */
  {  254,   -3 }, /* (162) tagitem ::= NOW PLUS VARIABLE */
  {  254,   -3 }, /* (163) tagitem ::= NOW MINUS VARIABLE */
  {  254,   -2 }, /* (164) tagitem ::= MINUS INTEGER */
  {  254,   -2 }, /* (165) tagitem ::= MINUS FLOAT */
  {  254,   -2 }, /* (166) tagitem ::= PLUS INTEGER */
  {  254,   -2 }, /* (167) tagitem ::= PLUS FLOAT */
  {  252,  -15 }, /* (168) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  252,   -3 }, /* (169) select ::= LP select RP */
  {  269,   -1 }, /* (170) union ::= select */
  {  269,   -4 }, /* (171) union ::= union UNION ALL select */
  {  203,   -1 }, /* (172) cmd ::= union */
  {  252,   -2 }, /* (173) select ::= SELECT selcollist */
  {  270,   -2 }, /* (174) sclp ::= selcollist COMMA */
  {  270,    0 }, /* (175) sclp ::= */
  {  255,   -4 }, /* (176) selcollist ::= sclp distinct expr as */
  {  255,   -2 }, /* (177) selcollist ::= sclp STAR */
  {  273,   -2 }, /* (178) as ::= AS ids */
  {  273,   -1 }, /* (179) as ::= ids */
  {  273,    0 }, /* (180) as ::= */
  {  271,   -1 }, /* (181) distinct ::= DISTINCT */
  {  271,    0 }, /* (182) distinct ::= */
  {  256,   -2 }, /* (183) from ::= FROM tablelist */
  {  256,   -2 }, /* (184) from ::= FROM sub */
  {  275,   -3 }, /* (185) sub ::= LP union RP */
  {  275,   -4 }, /* (186) sub ::= LP union RP ids */
  {  275,   -6 }, /* (187) sub ::= sub COMMA LP union RP ids */
  {  274,   -2 }, /* (188) tablelist ::= ids cpxName */
  {  274,   -3 }, /* (189) tablelist ::= ids cpxName ids */
  {  274,   -4 }, /* (190) tablelist ::= tablelist COMMA ids cpxName */
  {  274,   -5 }, /* (191) tablelist ::= tablelist COMMA ids cpxName ids */
  {  276,   -1 }, /* (192) tmvar ::= VARIABLE */
  {  277,   -1 }, /* (193) timestamp ::= INTEGER */
  {  277,   -2 }, /* (194) timestamp ::= MINUS INTEGER */
  {  277,   -2 }, /* (195) timestamp ::= PLUS INTEGER */
  {  277,   -1 }, /* (196) timestamp ::= STRING */
  {  277,   -1 }, /* (197) timestamp ::= NOW */
  {  277,   -3 }, /* (198) timestamp ::= NOW PLUS VARIABLE */
  {  277,   -3 }, /* (199) timestamp ::= NOW MINUS VARIABLE */
  {  258,    0 }, /* (200) range_option ::= */
  {  258,   -6 }, /* (201) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  259,   -4 }, /* (202) interval_option ::= intervalKey LP tmvar RP */
  {  259,   -6 }, /* (203) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  259,    0 }, /* (204) interval_option ::= */
  {  278,   -1 }, /* (205) intervalKey ::= INTERVAL */
  {  278,   -1 }, /* (206) intervalKey ::= EVERY */
  {  261,    0 }, /* (207) session_option ::= */
  {  261,   -7 }, /* (208) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  262,    0 }, /* (209) windowstate_option ::= */
  {  262,   -4 }, /* (210) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  263,    0 }, /* (211) fill_opt ::= */
  {  263,   -6 }, /* (212) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  263,   -4 }, /* (213) fill_opt ::= FILL LP ID RP */
  {  260,   -4 }, /* (214) sliding_opt ::= SLIDING LP tmvar RP */
  {  260,    0 }, /* (215) sliding_opt ::= */
  {  266,    0 }, /* (216) orderby_opt ::= */
  {  266,   -3 }, /* (217) orderby_opt ::= ORDER BY sortlist */
  {  279,   -4 }, /* (218) sortlist ::= sortlist COMMA item sortorder */
  {  279,   -4 }, /* (219) sortlist ::= sortlist COMMA arrow sortorder */
  {  279,   -2 }, /* (220) sortlist ::= item sortorder */
  {  279,   -2 }, /* (221) sortlist ::= arrow sortorder */
  {  280,   -1 }, /* (222) item ::= ID */
  {  280,   -3 }, /* (223) item ::= ID DOT ID */
  {  281,   -1 }, /* (224) sortorder ::= ASC */
  {  281,   -1 }, /* (225) sortorder ::= DESC */
  {  281,    0 }, /* (226) sortorder ::= */
  {  264,    0 }, /* (227) groupby_opt ::= */
  {  264,   -3 }, /* (228) groupby_opt ::= GROUP BY grouplist */
  {  283,   -3 }, /* (229) grouplist ::= grouplist COMMA item */
  {  283,   -3 }, /* (230) grouplist ::= grouplist COMMA arrow */
  {  283,   -1 }, /* (231) grouplist ::= item */
  {  283,   -1 }, /* (232) grouplist ::= arrow */
  {  265,    0 }, /* (233) having_opt ::= */
  {  265,   -2 }, /* (234) having_opt ::= HAVING expr */
  {  268,    0 }, /* (235) limit_opt ::= */
  {  268,   -2 }, /* (236) limit_opt ::= LIMIT signed */
  {  268,   -4 }, /* (237) limit_opt ::= LIMIT signed OFFSET signed */
  {  268,   -4 }, /* (238) limit_opt ::= LIMIT signed COMMA signed */
  {  267,    0 }, /* (239) slimit_opt ::= */
  {  267,   -2 }, /* (240) slimit_opt ::= SLIMIT signed */
  {  267,   -4 }, /* (241) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  267,   -4 }, /* (242) slimit_opt ::= SLIMIT signed COMMA signed */
  {  257,    0 }, /* (243) where_opt ::= */
  {  257,   -2 }, /* (244) where_opt ::= WHERE expr */
  {  272,   -3 }, /* (245) expr ::= LP expr RP */
  {  272,   -1 }, /* (246) expr ::= ID */
  {  272,   -3 }, /* (247) expr ::= ID DOT ID */
  {  272,   -3 }, /* (248) expr ::= ID DOT STAR */
  {  272,   -1 }, /* (249) expr ::= INTEGER */
  {  272,   -2 }, /* (250) expr ::= MINUS INTEGER */
  {  272,   -2 }, /* (251) expr ::= PLUS INTEGER */
  {  272,   -1 }, /* (252) expr ::= FLOAT */
  {  272,   -2 }, /* (253) expr ::= MINUS FLOAT */
  {  272,   -2 }, /* (254) expr ::= PLUS FLOAT */
  {  272,   -1 }, /* (255) expr ::= STRING */
  {  272,   -1 }, /* (256) expr ::= NOW */
  {  272,   -1 }, /* (257) expr ::= VARIABLE */
  {  272,   -2 }, /* (258) expr ::= PLUS VARIABLE */
  {  272,   -2 }, /* (259) expr ::= MINUS VARIABLE */
  {  272,   -1 }, /* (260) expr ::= BOOL */
  {  272,   -1 }, /* (261) expr ::= NULL */
  {  272,   -4 }, /* (262) expr ::= ID LP exprlist RP */
  {  272,   -4 }, /* (263) expr ::= ID LP STAR RP */
  {  272,   -6 }, /* (264) expr ::= ID LP expr AS typename RP */
  {  272,   -3 }, /* (265) expr ::= expr IS NULL */
  {  272,   -4 }, /* (266) expr ::= expr IS NOT NULL */
  {  272,   -3 }, /* (267) expr ::= expr LT expr */
  {  272,   -3 }, /* (268) expr ::= expr GT expr */
  {  272,   -3 }, /* (269) expr ::= expr LE expr */
  {  272,   -3 }, /* (270) expr ::= expr GE expr */
  {  272,   -3 }, /* (271) expr ::= expr NE expr */
  {  272,   -3 }, /* (272) expr ::= expr EQ expr */
  {  272,   -5 }, /* (273) expr ::= expr BETWEEN expr AND expr */
  {  272,   -3 }, /* (274) expr ::= expr AND expr */
  {  272,   -3 }, /* (275) expr ::= expr OR expr */
  {  272,   -3 }, /* (276) expr ::= expr PLUS expr */
  {  272,   -3 }, /* (277) expr ::= expr MINUS expr */
  {  272,   -3 }, /* (278) expr ::= expr STAR expr */
  {  272,   -3 }, /* (279) expr ::= expr SLASH expr */
  {  272,   -3 }, /* (280) expr ::= expr REM expr */
  {  272,   -3 }, /* (281) expr ::= expr BITAND expr */
  {  272,   -3 }, /* (282) expr ::= expr LIKE expr */
  {  272,   -3 }, /* (283) expr ::= expr MATCH expr */
  {  272,   -3 }, /* (284) expr ::= expr NMATCH expr */
  {  272,   -3 }, /* (285) expr ::= ID CONTAINS STRING */
  {  272,   -5 }, /* (286) expr ::= ID DOT ID CONTAINS STRING */
  {  282,   -3 }, /* (287) arrow ::= ID ARROW STRING */
  {  282,   -5 }, /* (288) arrow ::= ID DOT ID ARROW STRING */
  {  272,   -1 }, /* (289) expr ::= arrow */
  {  272,   -5 }, /* (290) expr ::= expr IN LP exprlist RP */
  {  211,   -3 }, /* (291) exprlist ::= exprlist COMMA expritem */
  {  211,   -1 }, /* (292) exprlist ::= expritem */
  {  284,   -1 }, /* (293) expritem ::= expr */
  {  284,    0 }, /* (294) expritem ::= */
  {  203,   -3 }, /* (295) cmd ::= RESET QUERY CACHE */
  {  203,   -3 }, /* (296) cmd ::= SYNCDB ids REPLICA */
  {  203,   -7 }, /* (297) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  203,   -7 }, /* (298) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  203,   -7 }, /* (299) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  203,   -7 }, /* (300) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  203,   -7 }, /* (301) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  203,   -8 }, /* (302) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  203,   -9 }, /* (303) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  203,   -7 }, /* (304) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  203,   -7 }, /* (305) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  203,   -7 }, /* (306) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  203,   -7 }, /* (307) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  203,   -7 }, /* (308) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  203,   -7 }, /* (309) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  203,   -8 }, /* (310) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  203,   -9 }, /* (311) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  203,   -7 }, /* (312) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  203,   -3 }, /* (313) cmd ::= KILL CONNECTION INTEGER */
  {  203,   -5 }, /* (314) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  203,   -5 }, /* (315) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy532, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy31);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy31);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy231);}
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
      case 182: /* distinct ::= */ yytestcase(yyruleno==182);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy31);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy532, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy103, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy103, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy31.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy31.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy31.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy31.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy31.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy31.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy31.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy31.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy31.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy31 = yylhsminor.yy31;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
{ yylhsminor.yy231 = tVariantListAppend(yymsp[-2].minor.yy231, &yymsp[0].minor.yy176, -1);    }
  yymsp[-2].minor.yy231 = yylhsminor.yy231;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy231 = tVariantListAppend(NULL, &yymsp[0].minor.yy176, -1); }
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy176, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy176 = yylhsminor.yy176;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy231 = yymsp[0].minor.yy231; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy532); yymsp[1].minor.yy532.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.keep = yymsp[0].minor.yy231; }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy532 = yymsp[0].minor.yy532; yylhsminor.yy532.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy532 = yylhsminor.yy532;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy532); yymsp[1].minor.yy532.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy103, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy103 = yylhsminor.yy103;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy549 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy103, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy549;  // negative value of name length
    tSetColumnType(&yylhsminor.yy103, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy103 = yylhsminor.yy103;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy103, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy103 = yylhsminor.yy103;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy549 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy549 = yylhsminor.yy549;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy549 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy549 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy422;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy306);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy422 = pCreateTable;
}
  yymsp[0].minor.yy422 = yylhsminor.yy422;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy422->childTableInfo, &yymsp[0].minor.yy306);
  yylhsminor.yy422 = yymsp[-1].minor.yy422;
}
  yymsp[-1].minor.yy422 = yylhsminor.yy422;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy422 = tSetCreateTableInfo(yymsp[-1].minor.yy231, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy422 = yylhsminor.yy422;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy422 = tSetCreateTableInfo(yymsp[-5].minor.yy231, yymsp[-1].minor.yy231, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy422 = yylhsminor.yy422;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy306 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy231, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy306 = yylhsminor.yy306;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy306 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy231, yymsp[-1].minor.yy231, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy306 = yylhsminor.yy306;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy231, &yymsp[0].minor.yy0); yylhsminor.yy231 = yymsp[-2].minor.yy231;  }
  yymsp[-2].minor.yy231 = yylhsminor.yy231;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy231 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy231, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy422 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy86, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy422 = yylhsminor.yy422;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy231, &yymsp[0].minor.yy103); yylhsminor.yy231 = yymsp[-2].minor.yy231;  }
  yymsp[-2].minor.yy231 = yylhsminor.yy231;
        break;
      case 152: /* columnlist ::= column */
{yylhsminor.yy231 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy231, &yymsp[0].minor.yy103);}
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 153: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy103, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy103);
}
  yymsp[-1].minor.yy103 = yylhsminor.yy103;
        break;
      case 160: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy176, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy176 = yylhsminor.yy176;
        break;
      case 161: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy176, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy176 = yylhsminor.yy176;
        break;
      case 162: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy176, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 163: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy176, &yymsp[0].minor.yy0, TK_MINUS, true);
}
        break;
      case 164: /* tagitem ::= MINUS INTEGER */
      case 165: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==167);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy176, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy176 = yylhsminor.yy176;
        break;
      case 168: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy86 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy231, yymsp[-12].minor.yy484, yymsp[-11].minor.yy226, yymsp[-4].minor.yy231, yymsp[-2].minor.yy231, &yymsp[-9].minor.yy430, &yymsp[-7].minor.yy409, &yymsp[-6].minor.yy228, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy231, &yymsp[0].minor.yy444, &yymsp[-1].minor.yy444, yymsp[-3].minor.yy226, &yymsp[-10].minor.yy480);
}
  yymsp[-14].minor.yy86 = yylhsminor.yy86;
        break;
      case 169: /* select ::= LP select RP */
{yymsp[-2].minor.yy86 = yymsp[-1].minor.yy86;}
        break;
      case 170: /* union ::= select */
{ yylhsminor.yy231 = setSubclause(NULL, yymsp[0].minor.yy86); }
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 171: /* union ::= union UNION ALL select */
{ yylhsminor.yy231 = appendSelectClause(yymsp[-3].minor.yy231, yymsp[0].minor.yy86); }
  yymsp[-3].minor.yy231 = yylhsminor.yy231;
        break;
      case 172: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy231, NULL, TSDB_SQL_SELECT); }
        break;
      case 173: /* select ::= SELECT selcollist */
{
  yylhsminor.yy86 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy231, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy86 = yylhsminor.yy86;
        break;
      case 174: /* sclp ::= selcollist COMMA */
{yylhsminor.yy231 = yymsp[-1].minor.yy231;}
  yymsp[-1].minor.yy231 = yylhsminor.yy231;
        break;
      case 175: /* sclp ::= */
      case 216: /* orderby_opt ::= */ yytestcase(yyruleno==216);
{yymsp[1].minor.yy231 = 0;}
        break;
      case 176: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy231 = tSqlExprListAppend(yymsp[-3].minor.yy231, yymsp[-1].minor.yy226,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy231 = yylhsminor.yy231;
        break;
      case 177: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy231 = tSqlExprListAppend(yymsp[-1].minor.yy231, pNode, 0, 0);
}
  yymsp[-1].minor.yy231 = yylhsminor.yy231;
        break;
      case 178: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 179: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 180: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 181: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 183: /* from ::= FROM tablelist */
      case 184: /* from ::= FROM sub */ yytestcase(yyruleno==184);
{yymsp[-1].minor.yy484 = yymsp[0].minor.yy484;}
        break;
      case 185: /* sub ::= LP union RP */
{yymsp[-2].minor.yy484 = addSubqueryElem(NULL, yymsp[-1].minor.yy231, NULL);}
        break;
      case 186: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy484 = addSubqueryElem(NULL, yymsp[-2].minor.yy231, &yymsp[0].minor.yy0);}
        break;
      case 187: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy484 = addSubqueryElem(yymsp[-5].minor.yy484, yymsp[-2].minor.yy231, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy484 = yylhsminor.yy484;
        break;
      case 188: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy484 = yylhsminor.yy484;
        break;
      case 189: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(yymsp[-3].minor.yy484, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy484 = yylhsminor.yy484;
        break;
      case 191: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy484 = setTableNameList(yymsp[-4].minor.yy484, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy484 = yylhsminor.yy484;
        break;
      case 192: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 193: /* timestamp ::= INTEGER */
{ yylhsminor.yy226 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 194: /* timestamp ::= MINUS INTEGER */
      case 195: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==195);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy226 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 196: /* timestamp ::= STRING */
{ yylhsminor.yy226 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 197: /* timestamp ::= NOW */
{ yylhsminor.yy226 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 198: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy226 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 199: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy226 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 200: /* range_option ::= */
{yymsp[1].minor.yy480.start = 0; yymsp[1].minor.yy480.end = 0;}
        break;
      case 201: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy480.start = yymsp[-3].minor.yy226; yymsp[-5].minor.yy480.end = yymsp[-1].minor.yy226;}
        break;
      case 202: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy430.interval = yymsp[-1].minor.yy0; yylhsminor.yy430.offset.n = 0; yylhsminor.yy430.token = yymsp[-3].minor.yy310;}
  yymsp[-3].minor.yy430 = yylhsminor.yy430;
        break;
      case 203: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy430.interval = yymsp[-3].minor.yy0; yylhsminor.yy430.offset = yymsp[-1].minor.yy0;   yylhsminor.yy430.token = yymsp[-5].minor.yy310;}
  yymsp[-5].minor.yy430 = yylhsminor.yy430;
        break;
      case 204: /* interval_option ::= */
{memset(&yymsp[1].minor.yy430, 0, sizeof(yymsp[1].minor.yy430));}
        break;
      case 205: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy310 = TK_INTERVAL;}
        break;
      case 206: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy310 = TK_EVERY;   }
        break;
      case 207: /* session_option ::= */
{yymsp[1].minor.yy409.col.n = 0; yymsp[1].minor.yy409.gap.n = 0;}
        break;
      case 208: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy409.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy409.gap = yymsp[-1].minor.yy0;
}
        break;
      case 209: /* windowstate_option ::= */
{ yymsp[1].minor.yy228.col.n = 0; yymsp[1].minor.yy228.col.z = NULL;}
        break;
      case 210: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy228.col = yymsp[-1].minor.yy0; }
        break;
      case 211: /* fill_opt ::= */
{ yymsp[1].minor.yy231 = 0;     }
        break;
      case 212: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy231, &A, -1, 0);
    yymsp[-5].minor.yy231 = yymsp[-1].minor.yy231;
}
        break;
      case 213: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy231 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 214: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 215: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 217: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy231 = yymsp[0].minor.yy231;}
        break;
      case 218: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy231 = commonItemAppend(yymsp[-3].minor.yy231, &yymsp[-1].minor.yy176, NULL, false, yymsp[0].minor.yy502);
}
  yymsp[-3].minor.yy231 = yylhsminor.yy231;
        break;
      case 219: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy231 = commonItemAppend(yymsp[-3].minor.yy231, NULL, yymsp[-1].minor.yy226, true, yymsp[0].minor.yy502);
}
  yymsp[-3].minor.yy231 = yylhsminor.yy231;
        break;
      case 220: /* sortlist ::= item sortorder */
{
  yylhsminor.yy231 = commonItemAppend(NULL, &yymsp[-1].minor.yy176, NULL, false, yymsp[0].minor.yy502);
}
  yymsp[-1].minor.yy231 = yylhsminor.yy231;
        break;
      case 221: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy231 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy226, true, yymsp[0].minor.yy502);
}
  yymsp[-1].minor.yy231 = yylhsminor.yy231;
        break;
      case 222: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy176, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy176 = yylhsminor.yy176;
        break;
      case 223: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy176, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy176 = yylhsminor.yy176;
        break;
      case 224: /* sortorder ::= ASC */
{ yymsp[0].minor.yy502 = TSDB_ORDER_ASC; }
        break;
      case 225: /* sortorder ::= DESC */
{ yymsp[0].minor.yy502 = TSDB_ORDER_DESC;}
        break;
      case 226: /* sortorder ::= */
{ yymsp[1].minor.yy502 = TSDB_ORDER_ASC; }
        break;
      case 227: /* groupby_opt ::= */
{ yymsp[1].minor.yy231 = 0;}
        break;
      case 228: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy231 = yymsp[0].minor.yy231;}
        break;
      case 229: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy231 = commonItemAppend(yymsp[-2].minor.yy231, &yymsp[0].minor.yy176, NULL, false, -1);
}
  yymsp[-2].minor.yy231 = yylhsminor.yy231;
        break;
      case 230: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy231 = commonItemAppend(yymsp[-2].minor.yy231, NULL, yymsp[0].minor.yy226, true, -1);
}
  yymsp[-2].minor.yy231 = yylhsminor.yy231;
        break;
      case 231: /* grouplist ::= item */
{
  yylhsminor.yy231 = commonItemAppend(NULL, &yymsp[0].minor.yy176, NULL, false, -1);
}
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 232: /* grouplist ::= arrow */
{
  yylhsminor.yy231 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy226, true, -1);
}
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 233: /* having_opt ::= */
      case 243: /* where_opt ::= */ yytestcase(yyruleno==243);
      case 294: /* expritem ::= */ yytestcase(yyruleno==294);
{yymsp[1].minor.yy226 = 0;}
        break;
      case 234: /* having_opt ::= HAVING expr */
      case 244: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==244);
{yymsp[-1].minor.yy226 = yymsp[0].minor.yy226;}
        break;
      case 235: /* limit_opt ::= */
      case 239: /* slimit_opt ::= */ yytestcase(yyruleno==239);
{yymsp[1].minor.yy444.limit = -1; yymsp[1].minor.yy444.offset = 0;}
        break;
      case 236: /* limit_opt ::= LIMIT signed */
      case 240: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==240);
{yymsp[-1].minor.yy444.limit = yymsp[0].minor.yy549;  yymsp[-1].minor.yy444.offset = 0;}
        break;
      case 237: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy444.limit = yymsp[-2].minor.yy549;  yymsp[-3].minor.yy444.offset = yymsp[0].minor.yy549;}
        break;
      case 238: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy444.limit = yymsp[0].minor.yy549;  yymsp[-3].minor.yy444.offset = yymsp[-2].minor.yy549;}
        break;
      case 241: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy444.limit = yymsp[-2].minor.yy549;  yymsp[-3].minor.yy444.offset = yymsp[0].minor.yy549;}
        break;
      case 242: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy444.limit = yymsp[0].minor.yy549;  yymsp[-3].minor.yy444.offset = yymsp[-2].minor.yy549;}
        break;
      case 245: /* expr ::= LP expr RP */
{yylhsminor.yy226 = yymsp[-1].minor.yy226; yylhsminor.yy226->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy226->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 246: /* expr ::= ID */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 247: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 248: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 249: /* expr ::= INTEGER */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 250: /* expr ::= MINUS INTEGER */
      case 251: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==251);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 252: /* expr ::= FLOAT */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 253: /* expr ::= MINUS FLOAT */
      case 254: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==254);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 255: /* expr ::= STRING */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 256: /* expr ::= NOW */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 257: /* expr ::= VARIABLE */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 258: /* expr ::= PLUS VARIABLE */
      case 259: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==259);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 260: /* expr ::= BOOL */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 261: /* expr ::= NULL */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 262: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFunction(yymsp[-1].minor.yy231, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 263: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 264: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy226, &yymsp[-1].minor.yy103, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy226 = yylhsminor.yy226;
        break;
      case 265: /* expr ::= expr IS NULL */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 266: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-3].minor.yy226, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 267: /* expr ::= expr LT expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LT);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 268: /* expr ::= expr GT expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_GT);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 269: /* expr ::= expr LE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 270: /* expr ::= expr GE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_GE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 271: /* expr ::= expr NE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_NE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 272: /* expr ::= expr EQ expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_EQ);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 273: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy226); yylhsminor.yy226 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy226, yymsp[-2].minor.yy226, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy226, TK_LE), TK_AND);}
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 274: /* expr ::= expr AND expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_AND);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 275: /* expr ::= expr OR expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_OR); }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 276: /* expr ::= expr PLUS expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_PLUS);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 277: /* expr ::= expr MINUS expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_MINUS); }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 278: /* expr ::= expr STAR expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_STAR);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 279: /* expr ::= expr SLASH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_DIVIDE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 280: /* expr ::= expr REM expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_REM);   }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 281: /* expr ::= expr BITAND expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_BITAND);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 282: /* expr ::= expr LIKE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LIKE);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 283: /* expr ::= expr MATCH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_MATCH);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 284: /* expr ::= expr NMATCH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_NMATCH);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 285: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy226 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 286: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy226 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 287: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy226 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 288: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy226 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 289: /* expr ::= arrow */
      case 293: /* expritem ::= expr */ yytestcase(yyruleno==293);
{yylhsminor.yy226 = yymsp[0].minor.yy226;}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 290: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-4].minor.yy226, (tSqlExpr*)yymsp[-1].minor.yy231, TK_IN); }
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 291: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy231 = tSqlExprListAppend(yymsp[-2].minor.yy231,yymsp[0].minor.yy226,0, 0);}
  yymsp[-2].minor.yy231 = yylhsminor.yy231;
        break;
      case 292: /* exprlist ::= expritem */
{yylhsminor.yy231 = tSqlExprListAppend(0,yymsp[0].minor.yy226,0, 0);}
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 295: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 296: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 297: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 298: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 299: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 300: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 301: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 302: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 303: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy176, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 306: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 307: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 311: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy176, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy231, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 314: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 315: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
