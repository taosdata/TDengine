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
#define YYNOCODE 287
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy20;
  SWindowStateVal yy32;
  SCreateDbInfo yy42;
  tSqlExpr* yy46;
  SCreateAcctInfo yy55;
  SLimitVal yy86;
  SCreateTableSql* yy118;
  TAOS_FIELD yy119;
  int64_t yy129;
  tVariant yy186;
  SRelationInfo* yy192;
  SCreatedTableInfo yy228;
  SRangeVal yy229;
  int32_t yy332;
  SArray* yy373;
  SIntervalVal yy376;
  SSessionWindowVal yy435;
  SSqlNode* yy564;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             400
#define YYNRULE              317
#define YYNTOKEN             202
#define YY_MAX_SHIFT         399
#define YY_MIN_SHIFTREDUCE   625
#define YY_MAX_SHIFTREDUCE   941
#define YY_ERROR_ACTION      942
#define YY_ACCEPT_ACTION     943
#define YY_NO_ACTION         944
#define YY_MIN_REDUCE        945
#define YY_MAX_REDUCE        1261
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
#define YY_ACTTAB_COUNT (869)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   220,  676,  256,  266,  265, 1176,   24, 1177,  319,  677,
 /*    10 */  1234,  876, 1236,  879,   38,   39, 1234,   42,   43,  398,
 /*    20 */   247,  269,   31,   30,   29,  218,  676,   41,  351,   46,
 /*    30 */    44,   47,   45,   32,  677, 1234,  219,   37,   36,  224,
 /*    40 */   712,   35,   34,   33,   38,   39, 1234,   42,   43, 1234,
 /*    50 */  1124,  269,   31,   30,   29,   60, 1099,   41,  351,   46,
 /*    60 */    44,   47,   45,   32,  216,  220,  760,   37,   36,  356,
 /*    70 */   225,   35,   34,   33, 1234, 1234, 1237, 1237,   38,   39,
 /*    80 */  1234,   42,   43,  943,  399,  269,   31,   30,   29,  166,
 /*    90 */    87,   41,  351,   46,   44,   47,   45,   32,  376,  375,
 /*   100 */   249,   37,   36,  226, 1097,   35,   34,   33,   38,   39,
 /*   110 */   254,   42,   43, 1234, 1100,  269,   31,   30,   29, 1121,
 /*   120 */    59,   41,  351,   46,   44,   47,   45,   32,  237,  883,
 /*   130 */    13,   37,   36, 1115,  103,   35,   34,   33, 1234,   38,
 /*   140 */    40, 1094,   42,   43,   60,   60,  269,   31,   30,   29,
 /*   150 */   293,  870,   41,  351,   46,   44,   47,   45,   32,  300,
 /*   160 */   299,  347,   37,   36,  106,  676,   35,   34,   33,   39,
 /*   170 */   285,   42,   43,  677,  890,  269,   31,   30,   29,  289,
 /*   180 */   288,   41,  351,   46,   44,   47,   45,   32,  262,  259,
 /*   190 */   260,   37,   36, 1097, 1097,   35,   34,   33,   68,  345,
 /*   200 */   393,  392,  344,  343,  342,  391,  341,  340,  339,  390,
 /*   210 */   338,  389,  388,  626,  627,  628,  629,  630,  631,  632,
 /*   220 */   633,  634,  635,  636,  637,  638,  639,  161,  270,  248,
 /*   230 */   261,   42,   43,  386, 1100,  269,   31,   30,   29, 1225,
 /*   240 */   676,   41,  351,   46,   44,   47,   45,   32,  677, 1234,
 /*   250 */   263,   37,   36,   93, 1100,   35,   34,   33, 1062, 1050,
 /*   260 */  1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060,
 /*   270 */  1061, 1063, 1064,   25,   46,   44,   47,   45,   32,   35,
 /*   280 */    34,   33,   37,   36, 1224, 1223,   35,   34,   33,  220,
 /*   290 */   230,   69,  240,  885, 1234, 1234,  874,  232,  877, 1234,
 /*   300 */   880, 1237,  181,  146,  145,  144,  231,  243,  240,  885,
 /*   310 */   359,   93,  874,  244,  877,  132,  880, 1234,  102,  784,
 /*   320 */   107,  309,  781, 1234,  782,   52,  783,  386,  245,  246,
 /*   330 */    32,  104,  353,   88,   37,   36,  850,  818,   35,   34,
 /*   340 */    33,  821,   37,   36,  245,  246,   35,   34,   33,   69,
 /*   350 */     5,   63,  191,  222,  274,  275, 1088,  190,  113,  118,
 /*   360 */   109,  117,  301, 1234, 1083, 1084,   56, 1087,   60,  305,
 /*   370 */   306,  292,  875,   85,  878,   60,  334,   48,   60,  223,
 /*   380 */   241,  130,  124,  135,  282,  272, 1256,  350,  134, 1234,
 /*   390 */   140,  143,  133,   48,  849,  211,  209,  207, 1248,  137,
 /*   400 */    60, 1115,  206,  150,  149,  148,  147,  278,   68,  349,
 /*   410 */   393,  392,  886,  881,  882,  391,   60, 1096,  250,  390,
 /*   420 */   363,  389,  388,  364, 1097,   60,  268, 1097,  886,  881,
 /*   430 */   882,   60,   60, 1070,   60, 1068, 1069, 1086,  785,  276,
 /*   440 */  1071,  397,  396,  653, 1072,  365, 1073, 1074,    6, 1097,
 /*   450 */   227, 1115,  323,   99,  252,   98,  802,  160,  158,  157,
 /*   460 */  1234,  366,  273,  221,  271, 1097,  362,  361,  251,  302,
 /*   470 */   372,  993,  228, 1234, 1097,  229,  373,  374,  201,  378,
 /*   480 */  1097, 1097, 1234, 1097,  280, 1234,  277,   86,  371,  370,
 /*   490 */   884,  234,  235, 1187,  236,  233,  217,  279,  830,  831,
 /*   500 */   279, 1234, 1234,  279, 1234, 1234, 1234,  279,  187,  394,
 /*   510 */  1031,  188,  799, 1174,  352, 1175,   90,  101, 1098,  100,
 /*   520 */  1003, 1085,  994,    3,  202,  347,  294,  201,   91,  201,
 /*   530 */     1,  189,  304,  303,  827,  837,  838,   77,  349,   10,
 /*   540 */    61,   80,  806,  770,  326,  772,  328,  771,  168,   72,
 /*   550 */    49,   55,  355,  915,  296,  322,  887,   61,   61,   72,
 /*   560 */   105,   72,  267,  675,   83, 1186,  296,    9,  257,   15,
 /*   570 */     9,   14, 1183,  123,  354,  122,   17,    9,   16,  791,
 /*   580 */    78,  792,   81,  789,  329,  790,  368,  367,   19, 1182,
 /*   590 */    18,  258,  129,   21,  128,   20,  759,  142,  141,  377,
 /*   600 */   163,  290,  873,  165, 1123,   26, 1134, 1131, 1132, 1116,
 /*   610 */   297, 1136,  167,  172,  315,  183, 1166, 1095, 1165, 1164,
 /*   620 */  1163,  184, 1093,  185, 1261,  186, 1008,  331,  159,  332,
 /*   630 */   333,  395,  336,  337,  817,   70,  214,   66,  348, 1002,
 /*   640 */   360,  173,  308, 1113,  174,   82,   27,  253,  310, 1255,
 /*   650 */   120, 1254,  312,   79,  324, 1251,  192,  369, 1247,  126,
 /*   660 */  1246,   28,  320,  175,  318,  176, 1243,  193, 1028,   67,
 /*   670 */   316,  314,   62,   71,  177,  215,  990,  136,  988,  138,
 /*   680 */   139,  311,  986,  307,  985,  281,  204,  205,  982,  981,
 /*   690 */   980,  979,  978,  977,  976,  208,  335,  210,  972,  970,
 /*   700 */   387,  968,  212,  965,  213,  961,  131,  379,  380,  162,
 /*   710 */   381,   84,  295,   89,   94,  313,  382,  383,  384,  385,
 /*   720 */   164,  940,  284,  242,  283,  939,  264,  330,  287,  938,
 /*   730 */   286,  921,  920,  238,  291,  239,  296,   11,  325, 1007,
 /*   740 */    53, 1006,  114,   92,  115,  298,  794,   95,  826,   75,
 /*   750 */   820,  824,  984,  823,  983,  196,  151,  195, 1029,  194,
 /*   760 */   152,  197,  198,  200,  199,  975,    4,  153, 1030, 1066,
 /*   770 */   974,  182,  178,  154,  179,  180,   54,  967,  966,  819,
 /*   780 */  1076,    2,   76,  171,  828,  169,  255,  839,  170,   64,
 /*   790 */   833,   96,   22,  835,   97,  317,  354,  321,   12,   65,
 /*   800 */    23,   50,  327,  108,   51,  106,  111,  690,  725,  723,
 /*   810 */    57,  722,  110,  721,  719,  718,   58,  112,  717,  714,
 /*   820 */   680,  346,  116,    7,  912,  910,  889,  913,  888,  911,
 /*   830 */     8,  891,  358,  119,   73,  357,  121,   61,   74,  125,
 /*   840 */   762,  127,  761,  758,  706,  704,  696,  702,  698,  788,
 /*   850 */   700,  787,  694,  692,  728,  727,  726,  724,  720,  716,
 /*   860 */   715,  203,  678,  945,  643,  944,  155,  944,  156,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   273,    1,    1,  212,  212,  281,  273,  283,  284,    9,
 /*    10 */   283,    5,  285,    7,   14,   15,  283,   17,   18,  205,
 /*    20 */   206,   21,   22,   23,   24,  273,    1,   27,   28,   29,
 /*    30 */    30,   31,   32,   33,    9,  283,  273,   37,   38,  273,
 /*    40 */     5,   41,   42,   43,   14,   15,  283,   17,   18,  283,
 /*    50 */   205,   21,   22,   23,   24,  205,  255,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,  273,  273,    5,   37,   38,   16,
 /*    70 */   273,   41,   42,   43,  283,  283,  285,  285,   14,   15,
 /*    80 */   283,   17,   18,  203,  204,   21,   22,   23,   24,  205,
 /*    90 */    90,   27,   28,   29,   30,   31,   32,   33,   37,   38,
 /*   100 */   250,   37,   38,  273,  254,   41,   42,   43,   14,   15,
 /*   110 */   251,   17,   18,  283,  255,   21,   22,   23,   24,  274,
 /*   120 */    90,   27,   28,   29,   30,   31,   32,   33,  273,  123,
 /*   130 */    86,   37,   38,  253,   90,   41,   42,   43,  283,   14,
 /*   140 */    15,  205,   17,   18,  205,  205,   21,   22,   23,   24,
 /*   150 */   270,   87,   27,   28,   29,   30,   31,   32,   33,  275,
 /*   160 */   276,   88,   37,   38,  120,    1,   41,   42,   43,   15,
 /*   170 */   147,   17,   18,    9,  121,   21,   22,   23,   24,  156,
 /*   180 */   157,   27,   28,   29,   30,   31,   32,   33,  252,  250,
 /*   190 */   250,   37,   38,  254,  254,   41,   42,   43,  102,  103,
 /*   200 */   104,  105,  106,  107,  108,  109,  110,  111,  112,  113,
 /*   210 */   114,  115,  116,   49,   50,   51,   52,   53,   54,   55,
 /*   220 */    56,   57,   58,   59,   60,   61,   62,   63,  212,   65,
 /*   230 */   251,   17,   18,   94,  255,   21,   22,   23,   24,  273,
 /*   240 */     1,   27,   28,   29,   30,   31,   32,   33,    9,  283,
 /*   250 */   251,   37,   38,   86,  255,   41,   42,   43,  229,  230,
 /*   260 */   231,  232,  233,  234,  235,  236,  237,  238,  239,  240,
 /*   270 */   241,  242,  243,   48,   29,   30,   31,   32,   33,   41,
 /*   280 */    42,   43,   37,   38,  273,  273,   41,   42,   43,  273,
 /*   290 */    65,  124,    1,    2,  283,  283,    5,   72,    7,  283,
 /*   300 */     9,  285,  260,   78,   79,   80,   81,  273,    1,    2,
 /*   310 */    85,   86,    5,  273,    7,   82,    9,  283,  256,    2,
 /*   320 */   213,  279,    5,  283,    7,   86,    9,   94,   37,   38,
 /*   330 */    33,  213,   41,  271,   37,   38,   80,    5,   41,   42,
 /*   340 */    43,    9,   37,   38,   37,   38,   41,   42,   43,  124,
 /*   350 */    66,   67,   68,  273,   37,   38,  249,   73,   74,   75,
 /*   360 */    76,   77,  278,  283,  246,  247,  248,  249,  205,   37,
 /*   370 */    38,  146,    5,  148,    7,  205,   92,   86,  205,  273,
 /*   380 */   155,   66,   67,   68,  159,   72,  255,   25,   73,  283,
 /*   390 */    75,   76,   77,   86,  138,   66,   67,   68,  255,   84,
 /*   400 */   205,  253,   73,   74,   75,   76,   77,   72,  102,   47,
 /*   410 */   104,  105,  121,  122,  123,  109,  205,  254,  270,  113,
 /*   420 */   250,  115,  116,  250,  254,  205,   64,  254,  121,  122,
 /*   430 */   123,  205,  205,  229,  205,  231,  232,    0,  121,  122,
 /*   440 */   236,   69,   70,   71,  240,  250,  242,  243,   86,  254,
 /*   450 */   273,  253,  280,  281,  122,  283,   41,   66,   67,   68,
 /*   460 */   283,  250,  149,  273,  151,  254,  153,  154,  270,  278,
 /*   470 */   250,  211,  273,  283,  254,  273,  250,  250,  218,  250,
 /*   480 */   254,  254,  283,  254,  149,  283,  151,  213,  153,  154,
 /*   490 */   123,  273,  273,  245,  273,  273,  273,  205,  130,  131,
 /*   500 */   205,  283,  283,  205,  283,  283,  283,  205,  216,  227,
 /*   510 */   228,  216,  101,  281,  216,  283,   87,  281,  216,  283,
 /*   520 */   211,  247,  211,  209,  210,   88,   87,  218,   87,  218,
 /*   530 */   214,  215,   37,   38,   87,   87,   87,  101,   47,  128,
 /*   540 */   101,  101,  127,   87,   87,   87,   87,   87,  101,  101,
 /*   550 */   101,   86,   25,   87,  125,   64,   87,  101,  101,  101,
 /*   560 */   101,  101,    1,   87,   86,  245,  125,  101,  245,  150,
 /*   570 */   101,  152,  245,  150,   47,  152,  150,  101,  152,    5,
 /*   580 */   144,    7,  142,    5,  119,    7,   37,   38,  150,  245,
 /*   590 */   152,  245,  150,  150,  152,  152,  118,   82,   83,  245,
 /*   600 */   205,  205,   41,  205,  205,  272,  205,  205,  205,  253,
 /*   610 */   253,  205,  205,  205,  205,  257,  282,  253,  282,  282,
 /*   620 */   282,  205,  205,  205,  258,  205,  205,  205,   64,  205,
 /*   630 */   205,   88,  205,  205,  123,  205,  205,  205,  205,  205,
 /*   640 */   205,  268,  277,  269,  267,  141,  145,  277,  277,  205,
 /*   650 */   205,  205,  277,  143,  136,  205,  205,  205,  205,  205,
 /*   660 */   205,  140,  139,  266,  134,  265,  205,  205,  205,  205,
 /*   670 */   133,  132,  205,  205,  264,  205,  205,  205,  205,  205,
 /*   680 */   205,  135,  205,  129,  205,  205,  205,  205,  205,  205,
 /*   690 */   205,  205,  205,  205,  205,  205,   93,  205,  205,  205,
 /*   700 */   117,  205,  205,  205,  205,  205,  100,   99,   55,  207,
 /*   710 */    96,  208,  207,  207,  207,  207,   98,   59,   97,   95,
 /*   720 */   128,    5,    5,  207,  158,    5,  207,  207,    5,    5,
 /*   730 */   158,  104,  103,  207,  147,  207,  125,   86,  119,  217,
 /*   740 */    86,  217,  213,  126,  213,  101,   87,  101,   87,  101,
 /*   750 */     5,  123,  207,  123,  207,  220,  208,  224,  226,  225,
 /*   760 */   208,  223,  221,  219,  222,  207,  209,  208,  228,  244,
 /*   770 */   207,  258,  263,  208,  262,  261,  259,  207,  207,    5,
 /*   780 */   244,  214,   86,  101,   87,   86,    1,   87,   86,  101,
 /*   790 */    87,   86,  137,   87,   86,   86,   47,    1,   86,  101,
 /*   800 */   137,   86,  119,   82,   86,  120,   74,    5,    9,    5,
 /*   810 */    91,    5,   90,    5,    5,    5,   91,   90,    5,    5,
 /*   820 */    89,   16,   82,   86,    9,    9,   87,    9,   87,    9,
 /*   830 */    86,  121,   63,  152,   17,   28,  152,  101,   17,  152,
 /*   840 */     5,  152,    5,   87,    5,    5,    5,    5,    5,  123,
 /*   850 */     5,  123,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   860 */     5,  101,   89,    0,   64,  286,   22,  286,   22,  286,
 /*   870 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   880 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   890 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   900 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   910 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   920 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   930 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   940 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   950 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   960 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   970 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   980 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*   990 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1000 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1010 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1020 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1030 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1040 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1050 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1060 */   286,  286,  286,  286,  286,  286,  286,  286,  286,  286,
 /*  1070 */   286,
};
#define YY_SHIFT_COUNT    (399)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (863)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   225,   96,   96,  306,  306,   73,  291,  307,  307,  307,
 /*    10 */   239,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*    20 */    25,   25,    1,    1,    0,  164,  307,  307,  307,  307,
 /*    30 */   307,  307,  307,  307,  307,  307,  307,  307,  307,  307,
 /*    40 */   307,  307,  307,  307,  307,  307,  307,  307,  307,  317,
 /*    50 */   317,  317,  167,  167,  368,   25,  437,   25,   25,   25,
 /*    60 */    25,   25,  233,   73,    1,    1,  139,  139,   35,  869,
 /*    70 */   869,  869,  317,  317,  317,  332,  332,   61,   61,   61,
 /*    80 */    61,   61,   61,   61,   25,   25,   25,   25,  415,   25,
 /*    90 */    25,   25,  167,  167,   25,   25,   25,   25,  256,  256,
 /*   100 */   256,  256,  411,  167,   25,   25,   25,   25,   25,   25,
 /*   110 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   120 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   130 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   140 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   150 */    25,   25,   25,   25,   25,   25,   25,   25,   25,   25,
 /*   160 */    25,   25,  501,  564,  543,  564,  564,  564,  511,  511,
 /*   170 */   511,  511,  564,  504,  510,  518,  521,  523,  530,  537,
 /*   180 */   539,  546,  554,  501,  564,  564,  564,  603,  603,  583,
 /*   190 */    73,   73,  564,  564,  606,  608,  653,  614,  618,  658,
 /*   200 */   621,  624,  583,   35,  564,  564,  543,  543,  564,  543,
 /*   210 */   564,  543,  564,  564,  869,  869,   30,   64,   94,   94,
 /*   220 */    94,  125,  154,  214,  245,  245,  245,  245,  245,  245,
 /*   230 */   284,  315,  329,  297,  297,  297,  297,  305,  313,  335,
 /*   240 */   362,   23,   44,  238,  238,    6,  367,  372,  391,  439,
 /*   250 */   429,  441,  495,  447,  448,  449,  491,  436,  440,  456,
 /*   260 */   457,  458,  459,  460,  465,  466,  469,  527,  561,   53,
 /*   270 */   476,  419,  423,  426,  574,  578,  549,  438,  442,  478,
 /*   280 */   443,  515,  592,  716,  566,  717,  720,  572,  723,  724,
 /*   290 */   627,  629,  587,  611,  619,  651,  617,  659,  654,  644,
 /*   300 */   646,  661,  648,  628,  630,  745,  774,  696,  697,  699,
 /*   310 */   700,  702,  703,  682,  705,  706,  708,  785,  709,  688,
 /*   320 */   655,  749,  796,  698,  663,  712,  619,  715,  683,  718,
 /*   330 */   685,  721,  719,  722,  732,  802,  725,  727,  799,  804,
 /*   340 */   806,  808,  809,  810,  813,  814,  731,  805,  740,  815,
 /*   350 */   816,  737,  739,  741,  818,  820,  710,  744,  807,  769,
 /*   360 */   817,  681,  684,  736,  736,  736,  736,  726,  728,  821,
 /*   370 */   687,  689,  736,  736,  736,  835,  837,  756,  736,  839,
 /*   380 */   840,  841,  842,  843,  845,  847,  848,  849,  850,  851,
 /*   390 */   852,  853,  854,  855,  760,  773,  844,  846,  800,  863,
};
#define YY_REDUCE_COUNT (215)
#define YY_REDUCE_MIN   (-276)
#define YY_REDUCE_MAX   (571)
static const short yy_reduce_ofst[] = {
 /*     0 */  -120,   29,   29,  204,  204,  118, -209, -208,   16, -273,
 /*    10 */  -116, -150,  -61,  -60,  170,  173,  195,  211,  220,  226,
 /*    20 */   227,  229, -276,  172, -155, -186, -267, -248, -237, -234,
 /*    30 */  -203, -170, -145,  -34,   11,   12,   34,   40,   80,  106,
 /*    40 */   177,  190,  199,  202,  218,  219,  221,  222,  223, -141,
 /*    50 */   -21,   -1,  148,  198,   42,  -64,  107,  292,  295,  298,
 /*    60 */   302,  163,  260,  274,  232,  236,  309,  311,  282,   62,
 /*    70 */   316,  314, -199,  131,  143,   84,  191,  248,  320,  323,
 /*    80 */   327,  344,  346,  354,  395,  396,  398,  399,  333,  401,
 /*    90 */   402,  403,  356,  357,  406,  407,  408,  409,  334,  336,
 /*   100 */   337,  338,  358,  364,  416,  417,  418,  420,  421,  422,
 /*   110 */   424,  425,  427,  428,  430,  431,  432,  433,  434,  435,
 /*   120 */   444,  445,  446,  450,  451,  452,  453,  454,  455,  461,
 /*   130 */   462,  463,  464,  467,  468,  470,  471,  472,  473,  474,
 /*   140 */   475,  477,  479,  480,  481,  482,  483,  484,  485,  486,
 /*   150 */   487,  488,  489,  490,  492,  493,  494,  496,  497,  498,
 /*   160 */   499,  500,  366,  502,  503,  505,  506,  507,  365,  370,
 /*   170 */   371,  375,  508,  374,  373,  377,  397,  400,  410,  509,
 /*   180 */   512,  514,  517,  513,  516,  519,  520,  522,  524,  525,
 /*   190 */   529,  531,  526,  528,  532,  534,  533,  535,  538,  541,
 /*   200 */   542,  544,  536,  540,  545,  547,  548,  552,  558,  559,
 /*   210 */   563,  565,  570,  571,  567,  557,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   942, 1065, 1004, 1075,  991, 1001, 1239, 1239, 1239, 1239,
 /*    10 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*    20 */   942,  942,  942,  942, 1125,  962,  942,  942,  942,  942,
 /*    30 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*    40 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*    50 */   942,  942,  942,  942, 1149,  942, 1001,  942,  942,  942,
 /*    60 */   942,  942, 1011, 1001,  942,  942, 1011, 1011,  942, 1120,
 /*    70 */  1049, 1067,  942,  942,  942,  942,  942,  942,  942,  942,
 /*    80 */   942,  942,  942,  942,  942,  942,  942,  942, 1127, 1133,
 /*    90 */  1130,  942,  942,  942, 1135,  942,  942,  942, 1171, 1171,
 /*   100 */  1171, 1171, 1118,  942,  942,  942,  942,  942,  942,  942,
 /*   110 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   120 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   130 */   942,  942,  942,  942,  942,  942,  989,  942,  987,  942,
 /*   140 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   150 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   160 */   942,  960, 1188,  964,  999,  964,  964,  964,  942,  942,
 /*   170 */   942,  942,  964, 1180, 1184, 1161, 1178, 1172, 1156, 1154,
 /*   180 */  1152, 1160, 1145, 1188,  964,  964,  964, 1009, 1009, 1005,
 /*   190 */  1001, 1001,  964,  964, 1027, 1025, 1023, 1015, 1021, 1017,
 /*   200 */  1019, 1013,  992,  942,  964,  964,  999,  999,  964,  999,
 /*   210 */   964,  999,  964,  964, 1049, 1067, 1238,  942, 1189, 1179,
 /*   220 */  1238,  942, 1220, 1219, 1229, 1228, 1227, 1218, 1217, 1216,
 /*   230 */   942,  942,  942, 1212, 1215, 1214, 1213, 1226,  942,  942,
 /*   240 */  1191,  942,  942, 1222, 1221,  942,  942,  942,  942,  942,
 /*   250 */   942,  942, 1142,  942,  942,  942, 1167, 1185, 1181,  942,
 /*   260 */   942,  942,  942,  942,  942,  942,  942, 1192,  942,  942,
 /*   270 */   942,  942,  942,  942,  942,  942, 1106,  942,  942, 1077,
 /*   280 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   290 */   942,  942,  942, 1117,  942,  942,  942,  942,  942, 1129,
 /*   300 */  1128,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   310 */   942,  942,  942,  942,  942,  942,  942,  942,  942, 1173,
 /*   320 */   942, 1168,  942, 1162,  942,  942, 1089,  942,  942,  942,
 /*   330 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   340 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   350 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   360 */   942,  942,  942, 1257, 1252, 1253, 1250,  942,  942,  942,
 /*   370 */   942,  942, 1249, 1244, 1245,  942,  942,  942, 1242,  942,
 /*   380 */   942,  942,  942,  942,  942,  942,  942,  942,  942,  942,
 /*   390 */   942,  942,  942,  942, 1033,  942,  971,  969,  942,  942,
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
  /*  159 */ "DELETE",
  /*  160 */ "ABORT",
  /*  161 */ "AFTER",
  /*  162 */ "ATTACH",
  /*  163 */ "BEFORE",
  /*  164 */ "BEGIN",
  /*  165 */ "CASCADE",
  /*  166 */ "CLUSTER",
  /*  167 */ "CONFLICT",
  /*  168 */ "COPY",
  /*  169 */ "DEFERRED",
  /*  170 */ "DELIMITERS",
  /*  171 */ "DETACH",
  /*  172 */ "EACH",
  /*  173 */ "END",
  /*  174 */ "EXPLAIN",
  /*  175 */ "FAIL",
  /*  176 */ "FOR",
  /*  177 */ "IGNORE",
  /*  178 */ "IMMEDIATE",
  /*  179 */ "INITIALLY",
  /*  180 */ "INSTEAD",
  /*  181 */ "KEY",
  /*  182 */ "OF",
  /*  183 */ "RAISE",
  /*  184 */ "REPLACE",
  /*  185 */ "RESTRICT",
  /*  186 */ "ROW",
  /*  187 */ "STATEMENT",
  /*  188 */ "TRIGGER",
  /*  189 */ "VIEW",
  /*  190 */ "IPTOKEN",
  /*  191 */ "SEMI",
  /*  192 */ "NONE",
  /*  193 */ "PREV",
  /*  194 */ "LINEAR",
  /*  195 */ "IMPORT",
  /*  196 */ "TBNAME",
  /*  197 */ "JOIN",
  /*  198 */ "INSERT",
  /*  199 */ "INTO",
  /*  200 */ "VALUES",
  /*  201 */ "FILE",
  /*  202 */ "error",
  /*  203 */ "program",
  /*  204 */ "cmd",
  /*  205 */ "ids",
  /*  206 */ "dbPrefix",
  /*  207 */ "cpxName",
  /*  208 */ "ifexists",
  /*  209 */ "alter_db_optr",
  /*  210 */ "alter_topic_optr",
  /*  211 */ "acct_optr",
  /*  212 */ "exprlist",
  /*  213 */ "ifnotexists",
  /*  214 */ "db_optr",
  /*  215 */ "topic_optr",
  /*  216 */ "typename",
  /*  217 */ "bufsize",
  /*  218 */ "pps",
  /*  219 */ "tseries",
  /*  220 */ "dbs",
  /*  221 */ "streams",
  /*  222 */ "storage",
  /*  223 */ "qtime",
  /*  224 */ "users",
  /*  225 */ "conns",
  /*  226 */ "state",
  /*  227 */ "intitemlist",
  /*  228 */ "intitem",
  /*  229 */ "keep",
  /*  230 */ "cache",
  /*  231 */ "replica",
  /*  232 */ "quorum",
  /*  233 */ "days",
  /*  234 */ "minrows",
  /*  235 */ "maxrows",
  /*  236 */ "blocks",
  /*  237 */ "ctime",
  /*  238 */ "wal",
  /*  239 */ "fsync",
  /*  240 */ "comp",
  /*  241 */ "prec",
  /*  242 */ "update",
  /*  243 */ "cachelast",
  /*  244 */ "partitions",
  /*  245 */ "signed",
  /*  246 */ "create_table_args",
  /*  247 */ "create_stable_args",
  /*  248 */ "create_table_list",
  /*  249 */ "create_from_stable",
  /*  250 */ "columnlist",
  /*  251 */ "tagitemlist",
  /*  252 */ "tagNamelist",
  /*  253 */ "select",
  /*  254 */ "column",
  /*  255 */ "tagitem",
  /*  256 */ "selcollist",
  /*  257 */ "from",
  /*  258 */ "where_opt",
  /*  259 */ "range_option",
  /*  260 */ "interval_option",
  /*  261 */ "sliding_opt",
  /*  262 */ "session_option",
  /*  263 */ "windowstate_option",
  /*  264 */ "fill_opt",
  /*  265 */ "groupby_opt",
  /*  266 */ "having_opt",
  /*  267 */ "orderby_opt",
  /*  268 */ "slimit_opt",
  /*  269 */ "limit_opt",
  /*  270 */ "union",
  /*  271 */ "sclp",
  /*  272 */ "distinct",
  /*  273 */ "expr",
  /*  274 */ "as",
  /*  275 */ "tablelist",
  /*  276 */ "sub",
  /*  277 */ "tmvar",
  /*  278 */ "timestamp",
  /*  279 */ "intervalKey",
  /*  280 */ "sortlist",
  /*  281 */ "item",
  /*  282 */ "sortorder",
  /*  283 */ "arrow",
  /*  284 */ "grouplist",
  /*  285 */ "expritem",
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
 /* 316 */ "cmd ::= DELETE FROM ifexists ids cpxName where_opt",
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
    case 212: /* exprlist */
    case 256: /* selcollist */
    case 271: /* sclp */
{
tSqlExprListDestroy((yypminor->yy373));
}
      break;
    case 227: /* intitemlist */
    case 229: /* keep */
    case 250: /* columnlist */
    case 251: /* tagitemlist */
    case 252: /* tagNamelist */
    case 264: /* fill_opt */
    case 265: /* groupby_opt */
    case 267: /* orderby_opt */
    case 280: /* sortlist */
    case 284: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy373));
}
      break;
    case 248: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy118));
}
      break;
    case 253: /* select */
{
destroySqlNode((yypminor->yy564));
}
      break;
    case 257: /* from */
    case 275: /* tablelist */
    case 276: /* sub */
{
destroyRelationInfo((yypminor->yy192));
}
      break;
    case 258: /* where_opt */
    case 266: /* having_opt */
    case 273: /* expr */
    case 278: /* timestamp */
    case 283: /* arrow */
    case 285: /* expritem */
{
tSqlExprDestroy((yypminor->yy46));
}
      break;
    case 270: /* union */
{
destroyAllSqlNode((yypminor->yy373));
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
  {  203,   -1 }, /* (0) program ::= cmd */
  {  204,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  204,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  204,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  204,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  204,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  204,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  204,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  204,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  204,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  204,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  204,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  204,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  204,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  204,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  204,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  204,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  206,    0 }, /* (17) dbPrefix ::= */
  {  206,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  207,    0 }, /* (19) cpxName ::= */
  {  207,   -2 }, /* (20) cpxName ::= DOT ids */
  {  204,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  204,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  204,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  204,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  204,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  204,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  204,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  204,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  204,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  204,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  204,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  204,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  204,   -3 }, /* (33) cmd ::= DROP FUNCTION ids */
  {  204,   -3 }, /* (34) cmd ::= DROP DNODE ids */
  {  204,   -3 }, /* (35) cmd ::= DROP USER ids */
  {  204,   -3 }, /* (36) cmd ::= DROP ACCOUNT ids */
  {  204,   -2 }, /* (37) cmd ::= USE ids */
  {  204,   -3 }, /* (38) cmd ::= DESCRIBE ids cpxName */
  {  204,   -3 }, /* (39) cmd ::= DESC ids cpxName */
  {  204,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  204,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  204,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  204,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  204,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  204,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  204,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  204,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  204,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  204,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  204,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  205,   -1 }, /* (51) ids ::= ID */
  {  205,   -1 }, /* (52) ids ::= STRING */
  {  208,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  208,    0 }, /* (54) ifexists ::= */
  {  213,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  213,    0 }, /* (56) ifnotexists ::= */
  {  204,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  204,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  204,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  204,   -5 }, /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  204,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  204,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  204,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  217,    0 }, /* (64) bufsize ::= */
  {  217,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  218,    0 }, /* (66) pps ::= */
  {  218,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  219,    0 }, /* (68) tseries ::= */
  {  219,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  220,    0 }, /* (70) dbs ::= */
  {  220,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  221,    0 }, /* (72) streams ::= */
  {  221,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  222,    0 }, /* (74) storage ::= */
  {  222,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  223,    0 }, /* (76) qtime ::= */
  {  223,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  224,    0 }, /* (78) users ::= */
  {  224,   -2 }, /* (79) users ::= USERS INTEGER */
  {  225,    0 }, /* (80) conns ::= */
  {  225,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  226,    0 }, /* (82) state ::= */
  {  226,   -2 }, /* (83) state ::= STATE ids */
  {  211,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  227,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  227,   -1 }, /* (86) intitemlist ::= intitem */
  {  228,   -1 }, /* (87) intitem ::= INTEGER */
  {  229,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  230,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  231,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  232,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  233,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  234,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  235,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  236,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  237,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  238,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  239,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  240,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  241,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  242,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  243,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  244,   -2 }, /* (103) partitions ::= PARTITIONS INTEGER */
  {  214,    0 }, /* (104) db_optr ::= */
  {  214,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  214,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  214,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  214,   -2 }, /* (108) db_optr ::= db_optr days */
  {  214,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  214,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  214,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  214,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  214,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  214,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  214,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  214,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  214,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  214,   -2 }, /* (118) db_optr ::= db_optr update */
  {  214,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  215,   -1 }, /* (120) topic_optr ::= db_optr */
  {  215,   -2 }, /* (121) topic_optr ::= topic_optr partitions */
  {  209,    0 }, /* (122) alter_db_optr ::= */
  {  209,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  209,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  209,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  209,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  209,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  209,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  209,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  210,   -1 }, /* (130) alter_topic_optr ::= alter_db_optr */
  {  210,   -2 }, /* (131) alter_topic_optr ::= alter_topic_optr partitions */
  {  216,   -1 }, /* (132) typename ::= ids */
  {  216,   -4 }, /* (133) typename ::= ids LP signed RP */
  {  216,   -2 }, /* (134) typename ::= ids UNSIGNED */
  {  245,   -1 }, /* (135) signed ::= INTEGER */
  {  245,   -2 }, /* (136) signed ::= PLUS INTEGER */
  {  245,   -2 }, /* (137) signed ::= MINUS INTEGER */
  {  204,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_args */
  {  204,   -3 }, /* (139) cmd ::= CREATE TABLE create_stable_args */
  {  204,   -3 }, /* (140) cmd ::= CREATE STABLE create_stable_args */
  {  204,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_list */
  {  248,   -1 }, /* (142) create_table_list ::= create_from_stable */
  {  248,   -2 }, /* (143) create_table_list ::= create_table_list create_from_stable */
  {  246,   -6 }, /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  247,  -10 }, /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  249,  -10 }, /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  249,  -13 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  252,   -3 }, /* (148) tagNamelist ::= tagNamelist COMMA ids */
  {  252,   -1 }, /* (149) tagNamelist ::= ids */
  {  246,   -5 }, /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
  {  250,   -3 }, /* (151) columnlist ::= columnlist COMMA column */
  {  250,   -1 }, /* (152) columnlist ::= column */
  {  254,   -2 }, /* (153) column ::= ids typename */
  {  251,   -3 }, /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
  {  251,   -1 }, /* (155) tagitemlist ::= tagitem */
  {  255,   -1 }, /* (156) tagitem ::= INTEGER */
  {  255,   -1 }, /* (157) tagitem ::= FLOAT */
  {  255,   -1 }, /* (158) tagitem ::= STRING */
  {  255,   -1 }, /* (159) tagitem ::= BOOL */
  {  255,   -1 }, /* (160) tagitem ::= NULL */
  {  255,   -1 }, /* (161) tagitem ::= NOW */
  {  255,   -3 }, /* (162) tagitem ::= NOW PLUS VARIABLE */
  {  255,   -3 }, /* (163) tagitem ::= NOW MINUS VARIABLE */
  {  255,   -2 }, /* (164) tagitem ::= MINUS INTEGER */
  {  255,   -2 }, /* (165) tagitem ::= MINUS FLOAT */
  {  255,   -2 }, /* (166) tagitem ::= PLUS INTEGER */
  {  255,   -2 }, /* (167) tagitem ::= PLUS FLOAT */
  {  253,  -15 }, /* (168) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  253,   -3 }, /* (169) select ::= LP select RP */
  {  270,   -1 }, /* (170) union ::= select */
  {  270,   -4 }, /* (171) union ::= union UNION ALL select */
  {  204,   -1 }, /* (172) cmd ::= union */
  {  253,   -2 }, /* (173) select ::= SELECT selcollist */
  {  271,   -2 }, /* (174) sclp ::= selcollist COMMA */
  {  271,    0 }, /* (175) sclp ::= */
  {  256,   -4 }, /* (176) selcollist ::= sclp distinct expr as */
  {  256,   -2 }, /* (177) selcollist ::= sclp STAR */
  {  274,   -2 }, /* (178) as ::= AS ids */
  {  274,   -1 }, /* (179) as ::= ids */
  {  274,    0 }, /* (180) as ::= */
  {  272,   -1 }, /* (181) distinct ::= DISTINCT */
  {  272,    0 }, /* (182) distinct ::= */
  {  257,   -2 }, /* (183) from ::= FROM tablelist */
  {  257,   -2 }, /* (184) from ::= FROM sub */
  {  276,   -3 }, /* (185) sub ::= LP union RP */
  {  276,   -4 }, /* (186) sub ::= LP union RP ids */
  {  276,   -6 }, /* (187) sub ::= sub COMMA LP union RP ids */
  {  275,   -2 }, /* (188) tablelist ::= ids cpxName */
  {  275,   -3 }, /* (189) tablelist ::= ids cpxName ids */
  {  275,   -4 }, /* (190) tablelist ::= tablelist COMMA ids cpxName */
  {  275,   -5 }, /* (191) tablelist ::= tablelist COMMA ids cpxName ids */
  {  277,   -1 }, /* (192) tmvar ::= VARIABLE */
  {  278,   -1 }, /* (193) timestamp ::= INTEGER */
  {  278,   -2 }, /* (194) timestamp ::= MINUS INTEGER */
  {  278,   -2 }, /* (195) timestamp ::= PLUS INTEGER */
  {  278,   -1 }, /* (196) timestamp ::= STRING */
  {  278,   -1 }, /* (197) timestamp ::= NOW */
  {  278,   -3 }, /* (198) timestamp ::= NOW PLUS VARIABLE */
  {  278,   -3 }, /* (199) timestamp ::= NOW MINUS VARIABLE */
  {  259,    0 }, /* (200) range_option ::= */
  {  259,   -6 }, /* (201) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  260,   -4 }, /* (202) interval_option ::= intervalKey LP tmvar RP */
  {  260,   -6 }, /* (203) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  260,    0 }, /* (204) interval_option ::= */
  {  279,   -1 }, /* (205) intervalKey ::= INTERVAL */
  {  279,   -1 }, /* (206) intervalKey ::= EVERY */
  {  262,    0 }, /* (207) session_option ::= */
  {  262,   -7 }, /* (208) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  263,    0 }, /* (209) windowstate_option ::= */
  {  263,   -4 }, /* (210) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  264,    0 }, /* (211) fill_opt ::= */
  {  264,   -6 }, /* (212) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  264,   -4 }, /* (213) fill_opt ::= FILL LP ID RP */
  {  261,   -4 }, /* (214) sliding_opt ::= SLIDING LP tmvar RP */
  {  261,    0 }, /* (215) sliding_opt ::= */
  {  267,    0 }, /* (216) orderby_opt ::= */
  {  267,   -3 }, /* (217) orderby_opt ::= ORDER BY sortlist */
  {  280,   -4 }, /* (218) sortlist ::= sortlist COMMA item sortorder */
  {  280,   -4 }, /* (219) sortlist ::= sortlist COMMA arrow sortorder */
  {  280,   -2 }, /* (220) sortlist ::= item sortorder */
  {  280,   -2 }, /* (221) sortlist ::= arrow sortorder */
  {  281,   -1 }, /* (222) item ::= ID */
  {  281,   -3 }, /* (223) item ::= ID DOT ID */
  {  282,   -1 }, /* (224) sortorder ::= ASC */
  {  282,   -1 }, /* (225) sortorder ::= DESC */
  {  282,    0 }, /* (226) sortorder ::= */
  {  265,    0 }, /* (227) groupby_opt ::= */
  {  265,   -3 }, /* (228) groupby_opt ::= GROUP BY grouplist */
  {  284,   -3 }, /* (229) grouplist ::= grouplist COMMA item */
  {  284,   -3 }, /* (230) grouplist ::= grouplist COMMA arrow */
  {  284,   -1 }, /* (231) grouplist ::= item */
  {  284,   -1 }, /* (232) grouplist ::= arrow */
  {  266,    0 }, /* (233) having_opt ::= */
  {  266,   -2 }, /* (234) having_opt ::= HAVING expr */
  {  269,    0 }, /* (235) limit_opt ::= */
  {  269,   -2 }, /* (236) limit_opt ::= LIMIT signed */
  {  269,   -4 }, /* (237) limit_opt ::= LIMIT signed OFFSET signed */
  {  269,   -4 }, /* (238) limit_opt ::= LIMIT signed COMMA signed */
  {  268,    0 }, /* (239) slimit_opt ::= */
  {  268,   -2 }, /* (240) slimit_opt ::= SLIMIT signed */
  {  268,   -4 }, /* (241) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  268,   -4 }, /* (242) slimit_opt ::= SLIMIT signed COMMA signed */
  {  258,    0 }, /* (243) where_opt ::= */
  {  258,   -2 }, /* (244) where_opt ::= WHERE expr */
  {  273,   -3 }, /* (245) expr ::= LP expr RP */
  {  273,   -1 }, /* (246) expr ::= ID */
  {  273,   -3 }, /* (247) expr ::= ID DOT ID */
  {  273,   -3 }, /* (248) expr ::= ID DOT STAR */
  {  273,   -1 }, /* (249) expr ::= INTEGER */
  {  273,   -2 }, /* (250) expr ::= MINUS INTEGER */
  {  273,   -2 }, /* (251) expr ::= PLUS INTEGER */
  {  273,   -1 }, /* (252) expr ::= FLOAT */
  {  273,   -2 }, /* (253) expr ::= MINUS FLOAT */
  {  273,   -2 }, /* (254) expr ::= PLUS FLOAT */
  {  273,   -1 }, /* (255) expr ::= STRING */
  {  273,   -1 }, /* (256) expr ::= NOW */
  {  273,   -1 }, /* (257) expr ::= VARIABLE */
  {  273,   -2 }, /* (258) expr ::= PLUS VARIABLE */
  {  273,   -2 }, /* (259) expr ::= MINUS VARIABLE */
  {  273,   -1 }, /* (260) expr ::= BOOL */
  {  273,   -1 }, /* (261) expr ::= NULL */
  {  273,   -4 }, /* (262) expr ::= ID LP exprlist RP */
  {  273,   -4 }, /* (263) expr ::= ID LP STAR RP */
  {  273,   -6 }, /* (264) expr ::= ID LP expr AS typename RP */
  {  273,   -3 }, /* (265) expr ::= expr IS NULL */
  {  273,   -4 }, /* (266) expr ::= expr IS NOT NULL */
  {  273,   -3 }, /* (267) expr ::= expr LT expr */
  {  273,   -3 }, /* (268) expr ::= expr GT expr */
  {  273,   -3 }, /* (269) expr ::= expr LE expr */
  {  273,   -3 }, /* (270) expr ::= expr GE expr */
  {  273,   -3 }, /* (271) expr ::= expr NE expr */
  {  273,   -3 }, /* (272) expr ::= expr EQ expr */
  {  273,   -5 }, /* (273) expr ::= expr BETWEEN expr AND expr */
  {  273,   -3 }, /* (274) expr ::= expr AND expr */
  {  273,   -3 }, /* (275) expr ::= expr OR expr */
  {  273,   -3 }, /* (276) expr ::= expr PLUS expr */
  {  273,   -3 }, /* (277) expr ::= expr MINUS expr */
  {  273,   -3 }, /* (278) expr ::= expr STAR expr */
  {  273,   -3 }, /* (279) expr ::= expr SLASH expr */
  {  273,   -3 }, /* (280) expr ::= expr REM expr */
  {  273,   -3 }, /* (281) expr ::= expr BITAND expr */
  {  273,   -3 }, /* (282) expr ::= expr LIKE expr */
  {  273,   -3 }, /* (283) expr ::= expr MATCH expr */
  {  273,   -3 }, /* (284) expr ::= expr NMATCH expr */
  {  273,   -3 }, /* (285) expr ::= ID CONTAINS STRING */
  {  273,   -5 }, /* (286) expr ::= ID DOT ID CONTAINS STRING */
  {  283,   -3 }, /* (287) arrow ::= ID ARROW STRING */
  {  283,   -5 }, /* (288) arrow ::= ID DOT ID ARROW STRING */
  {  273,   -1 }, /* (289) expr ::= arrow */
  {  273,   -5 }, /* (290) expr ::= expr IN LP exprlist RP */
  {  212,   -3 }, /* (291) exprlist ::= exprlist COMMA expritem */
  {  212,   -1 }, /* (292) exprlist ::= expritem */
  {  285,   -1 }, /* (293) expritem ::= expr */
  {  285,    0 }, /* (294) expritem ::= */
  {  204,   -3 }, /* (295) cmd ::= RESET QUERY CACHE */
  {  204,   -3 }, /* (296) cmd ::= SYNCDB ids REPLICA */
  {  204,   -7 }, /* (297) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  204,   -7 }, /* (298) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  204,   -7 }, /* (299) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  204,   -7 }, /* (300) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  204,   -7 }, /* (301) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  204,   -8 }, /* (302) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  204,   -9 }, /* (303) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  204,   -7 }, /* (304) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  204,   -7 }, /* (305) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  204,   -7 }, /* (306) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  204,   -7 }, /* (307) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  204,   -7 }, /* (308) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  204,   -7 }, /* (309) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  204,   -8 }, /* (310) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  204,   -9 }, /* (311) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  204,   -7 }, /* (312) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  204,   -3 }, /* (313) cmd ::= KILL CONNECTION INTEGER */
  {  204,   -5 }, /* (314) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  204,   -5 }, /* (315) cmd ::= KILL QUERY INTEGER COLON INTEGER */
  {  204,   -6 }, /* (316) cmd ::= DELETE FROM ifexists ids cpxName where_opt */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy55);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy55);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy373);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy55);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy119, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy119, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy55.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy55.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy55.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy55.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy55.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy55.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy55.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy55.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy55.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy55 = yylhsminor.yy55;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
{ yylhsminor.yy373 = tVariantListAppend(yymsp[-2].minor.yy373, &yymsp[0].minor.yy186, -1);    }
  yymsp[-2].minor.yy373 = yylhsminor.yy373;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy373 = tVariantListAppend(NULL, &yymsp[0].minor.yy186, -1); }
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy186, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy186 = yylhsminor.yy186;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy373 = yymsp[0].minor.yy373; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy42); yymsp[1].minor.yy42.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.keep = yymsp[0].minor.yy373; }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy42 = yymsp[0].minor.yy42; yylhsminor.yy42.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy42); yymsp[1].minor.yy42.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy119, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy119 = yylhsminor.yy119;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy129 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy119, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy129;  // negative value of name length
    tSetColumnType(&yylhsminor.yy119, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy119 = yylhsminor.yy119;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy119, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy119 = yylhsminor.yy119;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy129 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy129 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy129 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy118;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy228);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy118 = pCreateTable;
}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy118->childTableInfo, &yymsp[0].minor.yy228);
  yylhsminor.yy118 = yymsp[-1].minor.yy118;
}
  yymsp[-1].minor.yy118 = yylhsminor.yy118;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy118 = tSetCreateTableInfo(yymsp[-1].minor.yy373, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy118, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy118 = yylhsminor.yy118;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy118 = tSetCreateTableInfo(yymsp[-5].minor.yy373, yymsp[-1].minor.yy373, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy118, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy118 = yylhsminor.yy118;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy228 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy373, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy228 = yylhsminor.yy228;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy228 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy373, yymsp[-1].minor.yy373, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy228 = yylhsminor.yy228;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy373, &yymsp[0].minor.yy0); yylhsminor.yy373 = yymsp[-2].minor.yy373;  }
  yymsp[-2].minor.yy373 = yylhsminor.yy373;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy373 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy373, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy118 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy564, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy118, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy118 = yylhsminor.yy118;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy373, &yymsp[0].minor.yy119); yylhsminor.yy373 = yymsp[-2].minor.yy373;  }
  yymsp[-2].minor.yy373 = yylhsminor.yy373;
        break;
      case 152: /* columnlist ::= column */
{yylhsminor.yy373 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy373, &yymsp[0].minor.yy119);}
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 153: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy119, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy119);
}
  yymsp[-1].minor.yy119 = yylhsminor.yy119;
        break;
      case 160: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy186, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy186 = yylhsminor.yy186;
        break;
      case 161: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy186, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy186 = yylhsminor.yy186;
        break;
      case 162: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy186, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 163: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy186, &yymsp[0].minor.yy0, TK_MINUS, true);
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
    tVariantCreate(&yylhsminor.yy186, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy186 = yylhsminor.yy186;
        break;
      case 168: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy564 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy373, yymsp[-12].minor.yy192, yymsp[-11].minor.yy46, yymsp[-4].minor.yy373, yymsp[-2].minor.yy373, &yymsp[-9].minor.yy376, &yymsp[-7].minor.yy435, &yymsp[-6].minor.yy32, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy373, &yymsp[0].minor.yy86, &yymsp[-1].minor.yy86, yymsp[-3].minor.yy46, &yymsp[-10].minor.yy229);
}
  yymsp[-14].minor.yy564 = yylhsminor.yy564;
        break;
      case 169: /* select ::= LP select RP */
{yymsp[-2].minor.yy564 = yymsp[-1].minor.yy564;}
        break;
      case 170: /* union ::= select */
{ yylhsminor.yy373 = setSubclause(NULL, yymsp[0].minor.yy564); }
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 171: /* union ::= union UNION ALL select */
{ yylhsminor.yy373 = appendSelectClause(yymsp[-3].minor.yy373, yymsp[0].minor.yy564); }
  yymsp[-3].minor.yy373 = yylhsminor.yy373;
        break;
      case 172: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy373, NULL, TSDB_SQL_SELECT); }
        break;
      case 173: /* select ::= SELECT selcollist */
{
  yylhsminor.yy564 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy373, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy564 = yylhsminor.yy564;
        break;
      case 174: /* sclp ::= selcollist COMMA */
{yylhsminor.yy373 = yymsp[-1].minor.yy373;}
  yymsp[-1].minor.yy373 = yylhsminor.yy373;
        break;
      case 175: /* sclp ::= */
      case 216: /* orderby_opt ::= */ yytestcase(yyruleno==216);
{yymsp[1].minor.yy373 = 0;}
        break;
      case 176: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy373 = tSqlExprListAppend(yymsp[-3].minor.yy373, yymsp[-1].minor.yy46,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy373 = yylhsminor.yy373;
        break;
      case 177: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy373 = tSqlExprListAppend(yymsp[-1].minor.yy373, pNode, 0, 0);
}
  yymsp[-1].minor.yy373 = yylhsminor.yy373;
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
{yymsp[-1].minor.yy192 = yymsp[0].minor.yy192;}
        break;
      case 185: /* sub ::= LP union RP */
{yymsp[-2].minor.yy192 = addSubqueryElem(NULL, yymsp[-1].minor.yy373, NULL);}
        break;
      case 186: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy192 = addSubqueryElem(NULL, yymsp[-2].minor.yy373, &yymsp[0].minor.yy0);}
        break;
      case 187: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy192 = addSubqueryElem(yymsp[-5].minor.yy192, yymsp[-2].minor.yy373, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy192 = yylhsminor.yy192;
        break;
      case 188: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy192 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy192 = yylhsminor.yy192;
        break;
      case 189: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy192 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy192 = yylhsminor.yy192;
        break;
      case 190: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy192 = setTableNameList(yymsp[-3].minor.yy192, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy192 = yylhsminor.yy192;
        break;
      case 191: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy192 = setTableNameList(yymsp[-4].minor.yy192, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy192 = yylhsminor.yy192;
        break;
      case 192: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 193: /* timestamp ::= INTEGER */
{ yylhsminor.yy46 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 194: /* timestamp ::= MINUS INTEGER */
      case 195: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==195);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy46 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 196: /* timestamp ::= STRING */
{ yylhsminor.yy46 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 197: /* timestamp ::= NOW */
{ yylhsminor.yy46 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 198: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy46 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 199: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy46 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 200: /* range_option ::= */
{yymsp[1].minor.yy229.start = 0; yymsp[1].minor.yy229.end = 0;}
        break;
      case 201: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy229.start = yymsp[-3].minor.yy46; yymsp[-5].minor.yy229.end = yymsp[-1].minor.yy46;}
        break;
      case 202: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy376.interval = yymsp[-1].minor.yy0; yylhsminor.yy376.offset.n = 0; yylhsminor.yy376.token = yymsp[-3].minor.yy332;}
  yymsp[-3].minor.yy376 = yylhsminor.yy376;
        break;
      case 203: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy376.interval = yymsp[-3].minor.yy0; yylhsminor.yy376.offset = yymsp[-1].minor.yy0;   yylhsminor.yy376.token = yymsp[-5].minor.yy332;}
  yymsp[-5].minor.yy376 = yylhsminor.yy376;
        break;
      case 204: /* interval_option ::= */
{memset(&yymsp[1].minor.yy376, 0, sizeof(yymsp[1].minor.yy376));}
        break;
      case 205: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy332 = TK_INTERVAL;}
        break;
      case 206: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy332 = TK_EVERY;   }
        break;
      case 207: /* session_option ::= */
{yymsp[1].minor.yy435.col.n = 0; yymsp[1].minor.yy435.gap.n = 0;}
        break;
      case 208: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy435.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy435.gap = yymsp[-1].minor.yy0;
}
        break;
      case 209: /* windowstate_option ::= */
{ yymsp[1].minor.yy32.col.n = 0; yymsp[1].minor.yy32.col.z = NULL;}
        break;
      case 210: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy32.col = yymsp[-1].minor.yy0; }
        break;
      case 211: /* fill_opt ::= */
{ yymsp[1].minor.yy373 = 0;     }
        break;
      case 212: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy373, &A, -1, 0);
    yymsp[-5].minor.yy373 = yymsp[-1].minor.yy373;
}
        break;
      case 213: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy373 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 214: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 215: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 217: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy373 = yymsp[0].minor.yy373;}
        break;
      case 218: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy373 = commonItemAppend(yymsp[-3].minor.yy373, &yymsp[-1].minor.yy186, NULL, false, yymsp[0].minor.yy20);
}
  yymsp[-3].minor.yy373 = yylhsminor.yy373;
        break;
      case 219: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy373 = commonItemAppend(yymsp[-3].minor.yy373, NULL, yymsp[-1].minor.yy46, true, yymsp[0].minor.yy20);
}
  yymsp[-3].minor.yy373 = yylhsminor.yy373;
        break;
      case 220: /* sortlist ::= item sortorder */
{
  yylhsminor.yy373 = commonItemAppend(NULL, &yymsp[-1].minor.yy186, NULL, false, yymsp[0].minor.yy20);
}
  yymsp[-1].minor.yy373 = yylhsminor.yy373;
        break;
      case 221: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy373 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy46, true, yymsp[0].minor.yy20);
}
  yymsp[-1].minor.yy373 = yylhsminor.yy373;
        break;
      case 222: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy186, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy186 = yylhsminor.yy186;
        break;
      case 223: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy186, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy186 = yylhsminor.yy186;
        break;
      case 224: /* sortorder ::= ASC */
{ yymsp[0].minor.yy20 = TSDB_ORDER_ASC; }
        break;
      case 225: /* sortorder ::= DESC */
{ yymsp[0].minor.yy20 = TSDB_ORDER_DESC;}
        break;
      case 226: /* sortorder ::= */
{ yymsp[1].minor.yy20 = TSDB_ORDER_ASC; }
        break;
      case 227: /* groupby_opt ::= */
{ yymsp[1].minor.yy373 = 0;}
        break;
      case 228: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy373 = yymsp[0].minor.yy373;}
        break;
      case 229: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy373 = commonItemAppend(yymsp[-2].minor.yy373, &yymsp[0].minor.yy186, NULL, false, -1);
}
  yymsp[-2].minor.yy373 = yylhsminor.yy373;
        break;
      case 230: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy373 = commonItemAppend(yymsp[-2].minor.yy373, NULL, yymsp[0].minor.yy46, true, -1);
}
  yymsp[-2].minor.yy373 = yylhsminor.yy373;
        break;
      case 231: /* grouplist ::= item */
{
  yylhsminor.yy373 = commonItemAppend(NULL, &yymsp[0].minor.yy186, NULL, false, -1);
}
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 232: /* grouplist ::= arrow */
{
  yylhsminor.yy373 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy46, true, -1);
}
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 233: /* having_opt ::= */
      case 243: /* where_opt ::= */ yytestcase(yyruleno==243);
      case 294: /* expritem ::= */ yytestcase(yyruleno==294);
{yymsp[1].minor.yy46 = 0;}
        break;
      case 234: /* having_opt ::= HAVING expr */
      case 244: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==244);
{yymsp[-1].minor.yy46 = yymsp[0].minor.yy46;}
        break;
      case 235: /* limit_opt ::= */
      case 239: /* slimit_opt ::= */ yytestcase(yyruleno==239);
{yymsp[1].minor.yy86.limit = -1; yymsp[1].minor.yy86.offset = 0;}
        break;
      case 236: /* limit_opt ::= LIMIT signed */
      case 240: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==240);
{yymsp[-1].minor.yy86.limit = yymsp[0].minor.yy129;  yymsp[-1].minor.yy86.offset = 0;}
        break;
      case 237: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy86.limit = yymsp[-2].minor.yy129;  yymsp[-3].minor.yy86.offset = yymsp[0].minor.yy129;}
        break;
      case 238: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy86.limit = yymsp[0].minor.yy129;  yymsp[-3].minor.yy86.offset = yymsp[-2].minor.yy129;}
        break;
      case 241: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy86.limit = yymsp[-2].minor.yy129;  yymsp[-3].minor.yy86.offset = yymsp[0].minor.yy129;}
        break;
      case 242: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy86.limit = yymsp[0].minor.yy129;  yymsp[-3].minor.yy86.offset = yymsp[-2].minor.yy129;}
        break;
      case 245: /* expr ::= LP expr RP */
{yylhsminor.yy46 = yymsp[-1].minor.yy46; yylhsminor.yy46->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy46->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 246: /* expr ::= ID */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 247: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 248: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 249: /* expr ::= INTEGER */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 250: /* expr ::= MINUS INTEGER */
      case 251: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==251);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 252: /* expr ::= FLOAT */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 253: /* expr ::= MINUS FLOAT */
      case 254: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==254);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 255: /* expr ::= STRING */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 256: /* expr ::= NOW */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 257: /* expr ::= VARIABLE */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 258: /* expr ::= PLUS VARIABLE */
      case 259: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==259);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 260: /* expr ::= BOOL */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 261: /* expr ::= NULL */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 262: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(yymsp[-1].minor.yy373, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 263: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 264: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy46, &yymsp[-1].minor.yy119, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy46 = yylhsminor.yy46;
        break;
      case 265: /* expr ::= expr IS NULL */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 266: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-3].minor.yy46, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 267: /* expr ::= expr LT expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LT);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 268: /* expr ::= expr GT expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GT);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 269: /* expr ::= expr LE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 270: /* expr ::= expr GE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 271: /* expr ::= expr NE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_NE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 272: /* expr ::= expr EQ expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_EQ);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 273: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy46); yylhsminor.yy46 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy46, yymsp[-2].minor.yy46, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy46, TK_LE), TK_AND);}
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 274: /* expr ::= expr AND expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_AND);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 275: /* expr ::= expr OR expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_OR); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 276: /* expr ::= expr PLUS expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_PLUS);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 277: /* expr ::= expr MINUS expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MINUS); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 278: /* expr ::= expr STAR expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_STAR);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 279: /* expr ::= expr SLASH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_DIVIDE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 280: /* expr ::= expr REM expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_REM);   }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 281: /* expr ::= expr BITAND expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_BITAND);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 282: /* expr ::= expr LIKE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LIKE);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 283: /* expr ::= expr MATCH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MATCH);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 284: /* expr ::= expr NMATCH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_NMATCH);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 285: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy46 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 286: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy46 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 287: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy46 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 288: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy46 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 289: /* expr ::= arrow */
      case 293: /* expritem ::= expr */ yytestcase(yyruleno==293);
{yylhsminor.yy46 = yymsp[0].minor.yy46;}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 290: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-4].minor.yy46, (tSqlExpr*)yymsp[-1].minor.yy373, TK_IN); }
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 291: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy373 = tSqlExprListAppend(yymsp[-2].minor.yy373,yymsp[0].minor.yy46,0, 0);}
  yymsp[-2].minor.yy373 = yylhsminor.yy373;
        break;
      case 292: /* exprlist ::= expritem */
{yylhsminor.yy373 = tSqlExprListAppend(0,yymsp[0].minor.yy46,0, 0);}
  yymsp[0].minor.yy373 = yylhsminor.yy373;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 300: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy186, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy186, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy373, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
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
      case 316: /* cmd ::= DELETE FROM ifexists ids cpxName where_opt */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n; 
  SDelData * pDelData = tGetDelData(&yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0, yymsp[0].minor.yy46);
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
