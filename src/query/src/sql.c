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
#define YYNOCODE 289
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SRangeVal yy22;
  SCreatedTableInfo yy34;
  tVariant yy54;
  int64_t yy55;
  SIntervalVal yy102;
  SSessionWindowVal yy115;
  SSqlNode* yy160;
  SCreateAcctInfo yy205;
  SArray* yy209;
  SWindowStateVal yy290;
  int yy332;
  TAOS_FIELD yy369;
  int32_t yy380;
  SCreateTableSql* yy404;
  SRelationInfo* yy530;
  SLimitVal yy534;
  SCreateDbInfo yy560;
  tSqlExpr* yy574;
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
#define YYNSTATE             398
#define YYNRULE              319
#define YYNTOKEN             203
#define YY_MAX_SHIFT         397
#define YY_MIN_SHIFTREDUCE   624
#define YY_MAX_SHIFTREDUCE   942
#define YY_ERROR_ACTION      943
#define YY_ACCEPT_ACTION     944
#define YY_NO_ACTION         945
#define YY_MIN_REDUCE        946
#define YY_MAX_REDUCE        1264
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
#define YY_ACTTAB_COUNT (864)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   105,  675, 1095, 1129,  944,  397,  262,  759,  675,  676,
 /*    10 */  1181,  711, 1182,  314,   37,   38,  676,   41,   42,  396,
 /*    20 */   243,  265,   31,   30,   29, 1087,  163,   40,  347,   45,
 /*    30 */    43,   46,   44, 1084, 1085,   55, 1088,   36,   35,  372,
 /*    40 */   371,   34,   33,   32,   37,   38,  252,   41,   42,  258,
 /*    50 */    85,  265,   31,   30,   29,   24, 1120,   40,  347,   45,
 /*    60 */    43,   46,   44,  318,  100, 1238,   99,   36,   35,  218,
 /*    70 */   214,   34,   33,   32,  288, 1126,  131,  125,  136, 1238,
 /*    80 */  1238, 1240, 1241,  135, 1086,  141,  144,  134,   37,   38,
 /*    90 */    88,   41,   42,   51,  138,  265,   31,   30,   29,  295,
 /*   100 */   294,   40,  347,   45,   43,   46,   44,  343,   34,   33,
 /*   110 */    32,   36,   35,  343,  216,   34,   33,   32,   37,   38,
 /*   120 */    58,   41,   42,   59, 1238,  265,   31,   30,   29,  275,
 /*   130 */   675,   40,  347,   45,   43,   46,   44,  879,  676,  882,
 /*   140 */   185,   36,   35,  675,  217,   34,   33,   32,   13,   37,
 /*   150 */    39,  676,   41,   42, 1238,  382,  265,   31,   30,   29,
 /*   160 */   280,  873,   40,  347,   45,   43,   46,   44,  245,  284,
 /*   170 */   283, 1104,   36,   35,   59, 1102,   34,   33,   32,  209,
 /*   180 */   207,  205,  107,   86,  390, 1032,  204,  151,  150,  149,
 /*   190 */   148,  625,  626,  627,  628,  629,  630,  631,  632,  633,
 /*   200 */   634,  635,  636,  637,  638,  160,  250,  244,   38, 1260,
 /*   210 */    41,   42, 1252, 1105,  265,   31,   30,   29,  289,  255,
 /*   220 */    40,  347,   45,   43,   46,   44, 1102,  395,  393,  652,
 /*   230 */    36,   35,   60,  257,   34,   33,   32,  223,   41,   42,
 /*   240 */  1105,  178,  265,   31,   30,   29,   59, 1238,   40,  347,
 /*   250 */    45,   43,   46,   44,  159,  157,  156,  886,   36,   35,
 /*   260 */   304,   94,   34,   33,   32,   67,  341,  389,  388,  340,
 /*   270 */   339,  338,  387,  337,  336,  335,  386,  334,  385,  384,
 /*   280 */    25, 1063, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058,
 /*   290 */  1059, 1060, 1061, 1062, 1064, 1065,  802,  222, 1101,  237,
 /*   300 */   888,   68,  296,  877,  230,  880,  878,  883,  881,  108,
 /*   310 */   147,  146,  145,  229,  224,  237,  888,  355,   94,  877,
 /*   320 */   259,  880,  275,  883, 1238,   10,   59, 1105,  261,   45,
 /*   330 */    43,   46,   44,  186,  225,  241,  242,   36,   35,  349,
 /*   340 */  1230,   34,   33,   32, 1238, 1089,    5,   62,  189,  297,
 /*   350 */  1238,  241,  242,  188,  114,  119,  110,  118,   68,  787,
 /*   360 */  1229,  268,  784, 1228,  785,  239,  786,  274, 1192,  240,
 /*   370 */  1238,  256,  330, 1238,   67, 1238,  389,  388, 1102, 1238,
 /*   380 */   287,  387,   84, 1120,   47,  386,  821,  385,  384,  238,
 /*   390 */   824,  266,  218,  346,  270,  271, 1071,  103, 1069, 1070,
 /*   400 */    47,  246, 1238, 1072, 1241,  833,  834, 1073,   59, 1074,
 /*   410 */  1075,   59,   89,  853,   59,  345,   59,   59,  300,  301,
 /*   420 */   805,  889,  884,  885,   36,   35,  887,   59,   34,   33,
 /*   430 */    32,   59,  264,  220, 1179,  830, 1180,  889,  884,  885,
 /*   440 */   269,   59,  267, 1238,  358,  357,  276,  221,  273,  165,
 /*   450 */   367,  366, 1120,  359,    6,  218,  360, 1238, 1191,  361,
 /*   460 */  1102,  362,  368, 1102,  352, 1238, 1102, 1241, 1102, 1102,
 /*   470 */   247,  275,  369,  852,  226,  219,  370,  227,  228, 1102,
 /*   480 */   788,  272,  348, 1102, 1238, 1238,  374, 1238, 1238,  275,
 /*   490 */   994,  232,  233, 1102,  234,   91,  231,  199,  133,  215,
 /*   500 */  1103, 1238, 1238,   76, 1238,  248, 1238, 1004,  809, 1238,
 /*   510 */   382,  102,   92,  101,  199,  995,    1,  187,    3,  200,
 /*   520 */   299,  298,  199,  840,  841,  345,  769,   79,  263,  322,
 /*   530 */   771,  324,  770,   54,  351,  291,  917,   71,   48,  890,
 /*   540 */    60,  674,  317,   60,   71,  106,   71,   15,   77,   14,
 /*   550 */     9,   83,  291,    9,  285,    9,  350,  124,   17,  123,
 /*   560 */    16,  794,  792,  795,  793,   19,  325,   18,  876,  253,
 /*   570 */    80,  893,  364,  363,  130,   21,  129,   20,  143,  142,
 /*   580 */  1188, 1187,  254,  758,  181,  373,   26,  161,  162, 1100,
 /*   590 */  1128, 1139, 1171, 1136, 1137, 1141, 1121,  292,  164,  169,
 /*   600 */  1170,  310, 1169, 1168,  180, 1096,  182,  158, 1094,  183,
 /*   610 */   303,  820,  184,  171, 1009,  327,  328,  329,  249,  332,
 /*   620 */   333,  172,   69,  212,   65,  344, 1003,  305, 1118,  356,
 /*   630 */   170,  307, 1259,  121, 1258, 1255,  190,  365, 1251,  127,
 /*   640 */  1250,   81,   78, 1247,  319,  191, 1029,   28,  315,  313,
 /*   650 */   311,   66,  309,  306,  302,   61,   70,  176,  213,  173,
 /*   660 */   991,  137,  989,  139,  140,  987,  986,  277,  202,  203,
 /*   670 */    87,  983,  982,  981,  980,  979,  978,  977,  206,  208,
 /*   680 */   969,  210,  966,   27,  211,  962,  331,  383,  290, 1098,
 /*   690 */   132,   90,   95,  375,  308,  376,  377,  378,  379,  380,
 /*   700 */   381,   82,  260,  391,  942,  278,  326,  279,  941,  281,
 /*   710 */   282,  235,  940,  923,  922,  286,  115, 1008, 1007,  236,
 /*   720 */   321,  116,  291,   11,   93,  797,  293,   52,   96,  829,
 /*   730 */    74,  985,  827,  984,  823,  193, 1030,  194,  152,  192,
 /*   740 */   153,  196,  195,  197,  198,  976, 1067,  320,  154,  975,
 /*   750 */     4,  968,  179,  177,   53,  174,  175, 1031,  155,  967,
 /*   760 */     2,  826,  822,   75,  168,  831, 1077,  166,  251,  842,
 /*   770 */   167,   63,  836,   97,   22,  838,   98,  312,  350,  316,
 /*   780 */    64,   12,   23,  104,   49,  323,   50,  107,  109,  112,
 /*   790 */   689,  724,   56,  722,  111,  721,  720,  718,  717,   57,
 /*   800 */   113,  716,  713,  679,  117,  342,    7,  914,  912,  915,
 /*   810 */   892,  891,  913,    8,  353,  894,  354,   60,   72,  120,
 /*   820 */   761,  791,  122,   73,  760,  126,  128,  757,  705,  703,
 /*   830 */   695,  701,  697,  699,  790,  693,  691,  727,  726,  725,
 /*   840 */   723,  719,  715,  714,  201,  677,  946,  642,  651,  649,
 /*   850 */   945,  392,  945,  945,  945,  945,  945,  945,  945,  945,
 /*   860 */   945,  945,  945,  394,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   213,    1,  205,  205,  203,  204,  212,    5,    1,    9,
 /*    10 */   284,    5,  286,  287,   14,   15,    9,   17,   18,  205,
 /*    20 */   206,   21,   22,   23,   24,    0,  205,   27,   28,   29,
 /*    30 */    30,   31,   32,  246,  247,  248,  249,   37,   38,   37,
 /*    40 */    38,   41,   42,   43,   14,   15,    1,   17,   18,  252,
 /*    50 */   213,   21,   22,   23,   24,  276,  255,   27,   28,   29,
 /*    60 */    30,   31,   32,  283,  284,  286,  286,   37,   38,  276,
 /*    70 */   276,   41,   42,   43,  273,  277,   66,   67,   68,  286,
 /*    80 */   286,  288,  288,   73,  247,   75,   76,   77,   14,   15,
 /*    90 */    90,   17,   18,   86,   84,   21,   22,   23,   24,  278,
 /*   100 */   279,   27,   28,   29,   30,   31,   32,   88,   41,   42,
 /*   110 */    43,   37,   38,   88,  276,   41,   42,   43,   14,   15,
 /*   120 */    90,   17,   18,  205,  286,   21,   22,   23,   24,  205,
 /*   130 */     1,   27,   28,   29,   30,   31,   32,    5,    9,    7,
 /*   140 */   216,   37,   38,    1,  276,   41,   42,   43,   86,   14,
 /*   150 */    15,    9,   17,   18,  286,   94,   21,   22,   23,   24,
 /*   160 */   149,   87,   27,   28,   29,   30,   31,   32,  250,  158,
 /*   170 */   159,  258,   37,   38,  205,  257,   41,   42,   43,   66,
 /*   180 */    67,   68,  120,  121,  227,  228,   73,   74,   75,   76,
 /*   190 */    77,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   200 */    58,   59,   60,   61,   62,   63,  251,   65,   15,  258,
 /*   210 */    17,   18,  258,  258,   21,   22,   23,   24,   87,  250,
 /*   220 */    27,   28,   29,   30,   31,   32,  257,   69,   70,   71,
 /*   230 */    37,   38,  101,  251,   41,   42,   43,  276,   17,   18,
 /*   240 */   258,  263,   21,   22,   23,   24,  205,  286,   27,   28,
 /*   250 */    29,   30,   31,   32,   66,   67,   68,  125,   37,   38,
 /*   260 */   282,   86,   41,   42,   43,  102,  103,  104,  105,  106,
 /*   270 */   107,  108,  109,  110,  111,  112,  113,  114,  115,  116,
 /*   280 */    48,  229,  230,  231,  232,  233,  234,  235,  236,  237,
 /*   290 */   238,  239,  240,  241,  242,  243,  101,   65,  257,    1,
 /*   300 */     2,  126,  281,    5,   72,    7,    5,    9,    7,  213,
 /*   310 */    78,   79,   80,   81,  276,    1,    2,   85,   86,    5,
 /*   320 */   251,    7,  205,    9,  286,  130,  205,  258,  212,   29,
 /*   330 */    30,   31,   32,  216,  276,   37,   38,   37,   38,   41,
 /*   340 */   276,   41,   42,   43,  286,  249,   66,   67,   68,  281,
 /*   350 */   286,   37,   38,   73,   74,   75,   76,   77,  126,    2,
 /*   360 */   276,   72,    5,  276,    7,  276,    9,   72,  245,  276,
 /*   370 */   286,  250,   92,  286,  102,  286,  104,  105,  257,  286,
 /*   380 */   148,  109,  150,  255,   86,  113,    5,  115,  116,  157,
 /*   390 */     9,  212,  276,   25,   37,   38,  229,  259,  231,  232,
 /*   400 */    86,  273,  286,  236,  288,  132,  133,  240,  205,  242,
 /*   410 */   243,  205,  274,   80,  205,   47,  205,  205,   37,   38,
 /*   420 */    41,  123,  124,  125,   37,   38,  125,  205,   41,   42,
 /*   430 */    43,  205,   64,  276,  284,   87,  286,  123,  124,  125,
 /*   440 */   151,  205,  153,  286,  155,  156,  151,  276,  153,  101,
 /*   450 */   155,  156,  255,  250,   86,  276,  250,  286,  245,  250,
 /*   460 */   257,  250,  250,  257,   16,  286,  257,  288,  257,  257,
 /*   470 */   273,  205,  250,  140,  276,  276,  250,  276,  276,  257,
 /*   480 */   123,  124,  216,  257,  286,  286,  250,  286,  286,  205,
 /*   490 */   211,  276,  276,  257,  276,   87,  276,  218,   82,  276,
 /*   500 */   216,  286,  286,  101,  286,  124,  286,  211,  129,  286,
 /*   510 */    94,  284,   87,  286,  218,  211,  214,  215,  209,  210,
 /*   520 */    37,   38,  218,   87,   87,   47,   87,  101,    1,   87,
 /*   530 */    87,   87,   87,   86,   25,  127,   87,  101,  101,   87,
 /*   540 */   101,   87,   64,  101,  101,  101,  101,  152,  146,  154,
 /*   550 */   101,   86,  127,  101,  205,  101,   47,  152,  152,  154,
 /*   560 */   154,    5,    5,    7,    7,  152,  119,  154,   41,  245,
 /*   570 */   144,  123,   37,   38,  152,  152,  154,  154,   82,   83,
 /*   580 */   245,  245,  245,  118,  253,  245,  275,  205,  205,  205,
 /*   590 */   205,  205,  285,  205,  205,  205,  255,  255,  205,  205,
 /*   600 */   285,  205,  285,  285,  260,  255,  205,   64,  205,  205,
 /*   610 */   280,  125,  205,  270,  205,  205,  205,  205,  280,  205,
 /*   620 */   205,  269,  205,  205,  205,  205,  205,  280,  272,  205,
 /*   630 */   271,  280,  205,  205,  205,  205,  205,  205,  205,  205,
 /*   640 */   205,  143,  145,  205,  138,  205,  205,  142,  141,  136,
 /*   650 */   135,  205,  134,  137,  131,  205,  205,  265,  205,  268,
 /*   660 */   205,  205,  205,  205,  205,  205,  205,  205,  205,  205,
 /*   670 */   122,  205,  205,  205,  205,  205,  205,  205,  205,  205,
 /*   680 */   205,  205,  205,  147,  205,  205,   93,  117,  207,  207,
 /*   690 */   100,  207,  207,   99,  207,   55,   96,   98,   59,   97,
 /*   700 */    95,  207,  207,   88,    5,  160,  207,    5,    5,  160,
 /*   710 */     5,  207,    5,  104,  103,  149,  213,  217,  217,  207,
 /*   720 */   119,  213,  127,   86,  128,   87,  101,   86,  101,   87,
 /*   730 */   101,  207,  125,  207,    5,  224,  226,  220,  208,  225,
 /*   740 */   208,  221,  223,  222,  219,  207,  244,  254,  208,  207,
 /*   750 */   209,  207,  261,  264,  262,  267,  266,  228,  208,  207,
 /*   760 */   214,  125,    5,   86,  101,   87,  244,   86,    1,   87,
 /*   770 */    86,  101,   87,   86,  139,   87,   86,   86,   47,    1,
 /*   780 */   101,   86,  139,   90,   86,  119,   86,  120,   82,   74,
 /*   790 */     5,    9,   91,    5,   90,    5,    5,    5,    5,   91,
 /*   800 */    90,    5,    5,   89,   82,   16,   86,    9,    9,    9,
 /*   810 */    87,   87,    9,   86,   28,  123,   63,  101,   17,  154,
 /*   820 */     5,  125,  154,   17,    5,  154,  154,   87,    5,    5,
 /*   830 */     5,    5,    5,    5,  125,    5,    5,    5,    5,    5,
 /*   840 */     5,    5,    5,    5,  101,   89,    0,   64,    9,    9,
 /*   850 */   289,   22,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   860 */   289,  289,  289,   22,  289,  289,  289,  289,  289,  289,
 /*   870 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   880 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   890 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   900 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   910 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   920 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   930 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   940 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   950 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   960 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   970 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   980 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   990 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1000 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1010 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1020 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1030 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1040 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1050 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*  1060 */   289,  289,  289,  289,  289,  289,  289,
};
#define YY_SHIFT_COUNT    (397)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (846)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   232,  163,  163,  272,  272,   19,  298,  314,  314,  314,
 /*    10 */     7,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*    20 */   129,  129,   45,   45,    0,  142,  314,  314,  314,  314,
 /*    30 */   314,  314,  314,  314,  314,  314,  314,  314,  314,  314,
 /*    40 */   314,  314,  314,  314,  314,  314,  314,  314,  357,  357,
 /*    50 */   357,  175,  175,  273,  129,   25,  129,  129,  129,  129,
 /*    60 */   129,  416,   19,   45,   45,   61,   61,    6,  864,  864,
 /*    70 */   864,  357,  357,  357,  381,  381,    2,    2,    2,    2,
 /*    80 */     2,    2,   62,    2,  129,  129,  129,  129,  129,  379,
 /*    90 */   129,  129,  129,  175,  175,  129,  129,  129,  129,  333,
 /*   100 */   333,  333,  333,  195,  175,  129,  129,  129,  129,  129,
 /*   110 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   120 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   130 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   140 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   150 */   129,  129,  129,  129,  129,  129,  129,  129,  129,  129,
 /*   160 */   129,  543,  543,  543,  543,  486,  486,  486,  486,  543,
 /*   170 */   498,  497,  506,  505,  507,  513,  515,  518,  516,  523,
 /*   180 */   536,  548,  543,  543,  543,  593,  593,  570,   19,   19,
 /*   190 */   543,  543,  590,  594,  640,  600,  599,  639,  602,  605,
 /*   200 */   570,    6,  543,  543,  615,  615,  543,  615,  543,  615,
 /*   210 */   543,  543,  864,  864,   30,   74,  104,  104,  104,  135,
 /*   220 */   193,  221,  280,  300,  300,  300,  300,  300,  300,   10,
 /*   230 */   113,  387,  387,  387,  387,  289,  295,  368,   11,   67,
 /*   240 */    67,  132,  301,  158,  188,  131,  408,  425,  483,  348,
 /*   250 */   436,  437,  478,  402,  426,  439,  442,  443,  444,  445,
 /*   260 */   447,  449,  452,  509,  527,  448,  454,  395,  405,  406,
 /*   270 */   556,  557,  535,  413,  422,  465,  423,  496,  699,  545,
 /*   280 */   702,  703,  549,  705,  707,  609,  611,  566,  595,  601,
 /*   290 */   637,  596,  638,  641,  625,  627,  642,  629,  607,  636,
 /*   300 */   729,  757,  677,  678,  681,  682,  684,  685,  663,  687,
 /*   310 */   688,  690,  767,  691,  670,  635,  731,  778,  679,  643,
 /*   320 */   693,  695,  601,  698,  666,  700,  667,  706,  701,  704,
 /*   330 */   715,  785,  708,  710,  782,  788,  790,  791,  792,  793,
 /*   340 */   796,  797,  714,  789,  722,  798,  799,  720,  723,  724,
 /*   350 */   800,  803,  692,  727,  786,  753,  801,  665,  668,  716,
 /*   360 */   716,  716,  716,  696,  709,  806,  671,  672,  716,  716,
 /*   370 */   716,  815,  819,  740,  716,  823,  824,  825,  826,  827,
 /*   380 */   828,  830,  831,  832,  833,  834,  835,  836,  837,  838,
 /*   390 */   743,  756,  839,  829,  840,  841,  783,  846,
};
#define YY_REDUCE_COUNT (213)
#define YY_REDUCE_MIN   (-274)
#define YY_REDUCE_MAX   (552)
static const short yy_reduce_ofst[] = {
 /*     0 */  -199,   52,   52,  167,  167, -213, -206,  116,  179, -207,
 /*    10 */  -179,  -82,  -31,  121,  203,  206,  209,  211,  212,  222,
 /*    20 */   226,  236, -274, -220, -202, -186, -221, -162, -132,  -39,
 /*    30 */    38,   58,   64,   84,   87,   89,   93,  157,  171,  198,
 /*    40 */   199,  201,  202,  215,  216,  218,  220,  223,  -45,  -18,
 /*    50 */    69,  128,  197,  -22, -203,   96,  -76,  117,  266,  284,
 /*    60 */    41,  279, -163,  150,  227,  296,  304,  -43,  138,  302,
 /*    70 */   309,  -87,  -49,  -46,   21,   68,  123,  213,  324,  335,
 /*    80 */   336,  337,  331,  340,  349,  382,  383,  384,  385,  311,
 /*    90 */   386,  388,  389,  341,  342,  390,  393,  394,  396,  307,
 /*   100 */   315,  317,  318,  344,  350,  401,  403,  404,  407,  409,
 /*   110 */   410,  411,  412,  414,  415,  417,  418,  419,  420,  421,
 /*   120 */   424,  427,  428,  429,  430,  431,  432,  433,  434,  435,
 /*   130 */   438,  440,  441,  446,  450,  451,  453,  455,  456,  457,
 /*   140 */   458,  459,  460,  461,  462,  463,  464,  466,  467,  468,
 /*   150 */   469,  470,  471,  472,  473,  474,  475,  476,  477,  479,
 /*   160 */   480,  481,  482,  484,  485,  330,  338,  347,  351,  487,
 /*   170 */   356,  359,  343,  352,  391,  488,  490,  392,  489,  492,
 /*   180 */   491,  493,  494,  495,  499,  500,  501,  502,  503,  508,
 /*   190 */   504,  512,  510,  514,  511,  517,  519,  520,  521,  525,
 /*   200 */   522,  529,  524,  526,  530,  532,  538,  540,  542,  550,
 /*   210 */   544,  552,  546,  541,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   943, 1066, 1005, 1076,  992, 1002, 1243, 1243, 1243, 1243,
 /*    10 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*    20 */   943,  943,  943,  943, 1130,  963,  943,  943,  943,  943,
 /*    30 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*    40 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*    50 */   943,  943,  943, 1154,  943, 1002,  943,  943,  943,  943,
 /*    60 */   943, 1012, 1002,  943,  943, 1012, 1012,  943, 1125, 1050,
 /*    70 */  1068,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*    80 */   943,  943, 1097,  943,  943,  943,  943,  943,  943, 1132,
 /*    90 */  1138, 1135,  943,  943,  943, 1140,  943,  943,  943, 1176,
 /*   100 */  1176, 1176, 1176, 1123,  943,  943,  943,  943,  943,  943,
 /*   110 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   120 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   130 */   943,  943,  943,  943,  943,  943,  943,  990,  943,  988,
 /*   140 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   150 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   160 */   961,  965,  965,  965,  965,  943,  943,  943,  943,  965,
 /*   170 */  1185, 1189, 1166, 1183, 1177, 1161, 1159, 1157, 1165, 1150,
 /*   180 */  1193, 1099,  965,  965,  965, 1010, 1010, 1006, 1002, 1002,
 /*   190 */   965,  965, 1028, 1026, 1024, 1016, 1022, 1018, 1020, 1014,
 /*   200 */   993,  943,  965,  965, 1000, 1000,  965, 1000,  965, 1000,
 /*   210 */   965,  965, 1050, 1068, 1242,  943, 1194, 1184, 1242,  943,
 /*   220 */  1225, 1224,  943, 1233, 1232, 1231, 1223, 1222, 1221,  943,
 /*   230 */   943, 1217, 1220, 1219, 1218,  943,  943, 1196,  943, 1227,
 /*   240 */  1226,  943,  943,  943,  943,  943,  943,  943, 1147,  943,
 /*   250 */   943,  943, 1172, 1190, 1186,  943,  943,  943,  943,  943,
 /*   260 */   943,  943,  943, 1197,  943,  943,  943,  943,  943,  943,
 /*   270 */   943,  943, 1111,  943,  943, 1078,  943,  943,  943,  943,
 /*   280 */   943,  943,  943,  943,  943,  943,  943,  943, 1122,  943,
 /*   290 */   943,  943,  943,  943, 1134, 1133,  943,  943,  943,  943,
 /*   300 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   310 */   943,  943,  943,  943, 1178,  943, 1173,  943, 1167,  943,
 /*   320 */   943,  943, 1090,  943,  943,  943,  943,  943,  943,  943,
 /*   330 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   340 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   350 */   943,  943,  943,  943,  943,  943,  943,  943,  943, 1261,
 /*   360 */  1256, 1257, 1254,  943,  943,  943,  943,  943, 1253, 1248,
 /*   370 */  1249,  943,  943,  943, 1246,  943,  943,  943,  943,  943,
 /*   380 */   943,  943,  943,  943,  943,  943,  943,  943,  943,  943,
 /*   390 */  1034,  943,  943,  972,  943,  970,  943,  943,
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
  /*  148 */ "RESET",
  /*  149 */ "QUERY",
  /*  150 */ "SYNCDB",
  /*  151 */ "ADD",
  /*  152 */ "COLUMN",
  /*  153 */ "MODIFY",
  /*  154 */ "TAG",
  /*  155 */ "CHANGE",
  /*  156 */ "SET",
  /*  157 */ "KILL",
  /*  158 */ "CONNECTION",
  /*  159 */ "STREAM",
  /*  160 */ "COLON",
  /*  161 */ "ABORT",
  /*  162 */ "AFTER",
  /*  163 */ "ATTACH",
  /*  164 */ "BEFORE",
  /*  165 */ "BEGIN",
  /*  166 */ "CASCADE",
  /*  167 */ "CLUSTER",
  /*  168 */ "CONFLICT",
  /*  169 */ "COPY",
  /*  170 */ "DEFERRED",
  /*  171 */ "DELIMITERS",
  /*  172 */ "DETACH",
  /*  173 */ "EACH",
  /*  174 */ "END",
  /*  175 */ "EXPLAIN",
  /*  176 */ "FAIL",
  /*  177 */ "FOR",
  /*  178 */ "IGNORE",
  /*  179 */ "IMMEDIATE",
  /*  180 */ "INITIALLY",
  /*  181 */ "INSTEAD",
  /*  182 */ "KEY",
  /*  183 */ "OF",
  /*  184 */ "RAISE",
  /*  185 */ "REPLACE",
  /*  186 */ "RESTRICT",
  /*  187 */ "ROW",
  /*  188 */ "STATEMENT",
  /*  189 */ "TRIGGER",
  /*  190 */ "VIEW",
  /*  191 */ "IPTOKEN",
  /*  192 */ "SEMI",
  /*  193 */ "NONE",
  /*  194 */ "PREV",
  /*  195 */ "LINEAR",
  /*  196 */ "IMPORT",
  /*  197 */ "TBNAME",
  /*  198 */ "JOIN",
  /*  199 */ "INSERT",
  /*  200 */ "INTO",
  /*  201 */ "VALUES",
  /*  202 */ "FILE",
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
  /*  253 */ "to_opt",
  /*  254 */ "split_opt",
  /*  255 */ "select",
  /*  256 */ "to_split",
  /*  257 */ "column",
  /*  258 */ "tagitem",
  /*  259 */ "selcollist",
  /*  260 */ "from",
  /*  261 */ "where_opt",
  /*  262 */ "range_option",
  /*  263 */ "interval_option",
  /*  264 */ "sliding_opt",
  /*  265 */ "session_option",
  /*  266 */ "windowstate_option",
  /*  267 */ "fill_opt",
  /*  268 */ "groupby_opt",
  /*  269 */ "having_opt",
  /*  270 */ "orderby_opt",
  /*  271 */ "slimit_opt",
  /*  272 */ "limit_opt",
  /*  273 */ "union",
  /*  274 */ "sclp",
  /*  275 */ "distinct",
  /*  276 */ "expr",
  /*  277 */ "as",
  /*  278 */ "tablelist",
  /*  279 */ "sub",
  /*  280 */ "tmvar",
  /*  281 */ "timestamp",
  /*  282 */ "intervalKey",
  /*  283 */ "sortlist",
  /*  284 */ "item",
  /*  285 */ "sortorder",
  /*  286 */ "arrow",
  /*  287 */ "grouplist",
  /*  288 */ "expritem",
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
 /* 261 */ "expr ::= VARIABLE",
 /* 262 */ "expr ::= PLUS VARIABLE",
 /* 263 */ "expr ::= MINUS VARIABLE",
 /* 264 */ "expr ::= BOOL",
 /* 265 */ "expr ::= NULL",
 /* 266 */ "expr ::= ID LP exprlist RP",
 /* 267 */ "expr ::= ID LP STAR RP",
 /* 268 */ "expr ::= ID LP expr AS typename RP",
 /* 269 */ "expr ::= expr IS NULL",
 /* 270 */ "expr ::= expr IS NOT NULL",
 /* 271 */ "expr ::= expr LT expr",
 /* 272 */ "expr ::= expr GT expr",
 /* 273 */ "expr ::= expr LE expr",
 /* 274 */ "expr ::= expr GE expr",
 /* 275 */ "expr ::= expr NE expr",
 /* 276 */ "expr ::= expr EQ expr",
 /* 277 */ "expr ::= expr BETWEEN expr AND expr",
 /* 278 */ "expr ::= expr AND expr",
 /* 279 */ "expr ::= expr OR expr",
 /* 280 */ "expr ::= expr PLUS expr",
 /* 281 */ "expr ::= expr MINUS expr",
 /* 282 */ "expr ::= expr STAR expr",
 /* 283 */ "expr ::= expr SLASH expr",
 /* 284 */ "expr ::= expr REM expr",
 /* 285 */ "expr ::= expr LIKE expr",
 /* 286 */ "expr ::= expr MATCH expr",
 /* 287 */ "expr ::= expr NMATCH expr",
 /* 288 */ "expr ::= ID CONTAINS STRING",
 /* 289 */ "expr ::= ID DOT ID CONTAINS STRING",
 /* 290 */ "arrow ::= ID ARROW STRING",
 /* 291 */ "arrow ::= ID DOT ID ARROW STRING",
 /* 292 */ "expr ::= arrow",
 /* 293 */ "expr ::= expr IN LP exprlist RP",
 /* 294 */ "exprlist ::= exprlist COMMA expritem",
 /* 295 */ "exprlist ::= expritem",
 /* 296 */ "expritem ::= expr",
 /* 297 */ "expritem ::=",
 /* 298 */ "cmd ::= RESET QUERY CACHE",
 /* 299 */ "cmd ::= SYNCDB ids REPLICA",
 /* 300 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 301 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 302 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 303 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 304 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 305 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 306 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 307 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 308 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 309 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 310 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 311 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 312 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 313 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 314 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 315 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 316 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 317 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 318 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 212: /* exprlist */
    case 259: /* selcollist */
    case 274: /* sclp */
{
tSqlExprListDestroy((yypminor->yy209));
}
      break;
    case 227: /* intitemlist */
    case 229: /* keep */
    case 250: /* columnlist */
    case 251: /* tagitemlist */
    case 252: /* tagNamelist */
    case 267: /* fill_opt */
    case 268: /* groupby_opt */
    case 270: /* orderby_opt */
    case 283: /* sortlist */
    case 287: /* grouplist */
{
taosArrayDestroy(&(yypminor->yy209));
}
      break;
    case 248: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy404));
}
      break;
    case 255: /* select */
{
destroySqlNode((yypminor->yy160));
}
      break;
    case 260: /* from */
    case 278: /* tablelist */
    case 279: /* sub */
{
destroyRelationInfo((yypminor->yy530));
}
      break;
    case 261: /* where_opt */
    case 269: /* having_opt */
    case 276: /* expr */
    case 281: /* timestamp */
    case 286: /* arrow */
    case 288: /* expritem */
{
tSqlExprDestroy((yypminor->yy574));
}
      break;
    case 273: /* union */
{
destroyAllSqlNode((yypminor->yy209));
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
  {  204,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
  {  204,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  204,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
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
  {  246,   -7 }, /* (150) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  253,    0 }, /* (151) to_opt ::= */
  {  253,   -3 }, /* (152) to_opt ::= TO ids cpxName */
  {  254,    0 }, /* (153) split_opt ::= */
  {  254,   -2 }, /* (154) split_opt ::= SPLIT ids */
  {  250,   -3 }, /* (155) columnlist ::= columnlist COMMA column */
  {  250,   -1 }, /* (156) columnlist ::= column */
  {  257,   -2 }, /* (157) column ::= ids typename */
  {  251,   -3 }, /* (158) tagitemlist ::= tagitemlist COMMA tagitem */
  {  251,   -1 }, /* (159) tagitemlist ::= tagitem */
  {  258,   -1 }, /* (160) tagitem ::= INTEGER */
  {  258,   -1 }, /* (161) tagitem ::= FLOAT */
  {  258,   -1 }, /* (162) tagitem ::= STRING */
  {  258,   -1 }, /* (163) tagitem ::= BOOL */
  {  258,   -1 }, /* (164) tagitem ::= NULL */
  {  258,   -1 }, /* (165) tagitem ::= NOW */
  {  258,   -3 }, /* (166) tagitem ::= NOW PLUS VARIABLE */
  {  258,   -3 }, /* (167) tagitem ::= NOW MINUS VARIABLE */
  {  258,   -2 }, /* (168) tagitem ::= MINUS INTEGER */
  {  258,   -2 }, /* (169) tagitem ::= MINUS FLOAT */
  {  258,   -2 }, /* (170) tagitem ::= PLUS INTEGER */
  {  258,   -2 }, /* (171) tagitem ::= PLUS FLOAT */
  {  255,  -15 }, /* (172) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  255,   -3 }, /* (173) select ::= LP select RP */
  {  273,   -1 }, /* (174) union ::= select */
  {  273,   -4 }, /* (175) union ::= union UNION ALL select */
  {  204,   -1 }, /* (176) cmd ::= union */
  {  255,   -2 }, /* (177) select ::= SELECT selcollist */
  {  274,   -2 }, /* (178) sclp ::= selcollist COMMA */
  {  274,    0 }, /* (179) sclp ::= */
  {  259,   -4 }, /* (180) selcollist ::= sclp distinct expr as */
  {  259,   -2 }, /* (181) selcollist ::= sclp STAR */
  {  277,   -2 }, /* (182) as ::= AS ids */
  {  277,   -1 }, /* (183) as ::= ids */
  {  277,    0 }, /* (184) as ::= */
  {  275,   -1 }, /* (185) distinct ::= DISTINCT */
  {  275,    0 }, /* (186) distinct ::= */
  {  260,   -2 }, /* (187) from ::= FROM tablelist */
  {  260,   -2 }, /* (188) from ::= FROM sub */
  {  279,   -3 }, /* (189) sub ::= LP union RP */
  {  279,   -4 }, /* (190) sub ::= LP union RP ids */
  {  279,   -6 }, /* (191) sub ::= sub COMMA LP union RP ids */
  {  278,   -2 }, /* (192) tablelist ::= ids cpxName */
  {  278,   -3 }, /* (193) tablelist ::= ids cpxName ids */
  {  278,   -4 }, /* (194) tablelist ::= tablelist COMMA ids cpxName */
  {  278,   -5 }, /* (195) tablelist ::= tablelist COMMA ids cpxName ids */
  {  280,   -1 }, /* (196) tmvar ::= VARIABLE */
  {  281,   -1 }, /* (197) timestamp ::= INTEGER */
  {  281,   -2 }, /* (198) timestamp ::= MINUS INTEGER */
  {  281,   -2 }, /* (199) timestamp ::= PLUS INTEGER */
  {  281,   -1 }, /* (200) timestamp ::= STRING */
  {  281,   -1 }, /* (201) timestamp ::= NOW */
  {  281,   -3 }, /* (202) timestamp ::= NOW PLUS VARIABLE */
  {  281,   -3 }, /* (203) timestamp ::= NOW MINUS VARIABLE */
  {  262,    0 }, /* (204) range_option ::= */
  {  262,   -6 }, /* (205) range_option ::= RANGE LP timestamp COMMA timestamp RP */
  {  263,   -4 }, /* (206) interval_option ::= intervalKey LP tmvar RP */
  {  263,   -6 }, /* (207) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  263,    0 }, /* (208) interval_option ::= */
  {  282,   -1 }, /* (209) intervalKey ::= INTERVAL */
  {  282,   -1 }, /* (210) intervalKey ::= EVERY */
  {  265,    0 }, /* (211) session_option ::= */
  {  265,   -7 }, /* (212) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  266,    0 }, /* (213) windowstate_option ::= */
  {  266,   -4 }, /* (214) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  267,    0 }, /* (215) fill_opt ::= */
  {  267,   -6 }, /* (216) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  267,   -4 }, /* (217) fill_opt ::= FILL LP ID RP */
  {  264,   -4 }, /* (218) sliding_opt ::= SLIDING LP tmvar RP */
  {  264,    0 }, /* (219) sliding_opt ::= */
  {  270,    0 }, /* (220) orderby_opt ::= */
  {  270,   -3 }, /* (221) orderby_opt ::= ORDER BY sortlist */
  {  283,   -4 }, /* (222) sortlist ::= sortlist COMMA item sortorder */
  {  283,   -4 }, /* (223) sortlist ::= sortlist COMMA arrow sortorder */
  {  283,   -2 }, /* (224) sortlist ::= item sortorder */
  {  283,   -2 }, /* (225) sortlist ::= arrow sortorder */
  {  284,   -1 }, /* (226) item ::= ID */
  {  284,   -3 }, /* (227) item ::= ID DOT ID */
  {  285,   -1 }, /* (228) sortorder ::= ASC */
  {  285,   -1 }, /* (229) sortorder ::= DESC */
  {  285,    0 }, /* (230) sortorder ::= */
  {  268,    0 }, /* (231) groupby_opt ::= */
  {  268,   -3 }, /* (232) groupby_opt ::= GROUP BY grouplist */
  {  287,   -3 }, /* (233) grouplist ::= grouplist COMMA item */
  {  287,   -3 }, /* (234) grouplist ::= grouplist COMMA arrow */
  {  287,   -1 }, /* (235) grouplist ::= item */
  {  287,   -1 }, /* (236) grouplist ::= arrow */
  {  269,    0 }, /* (237) having_opt ::= */
  {  269,   -2 }, /* (238) having_opt ::= HAVING expr */
  {  272,    0 }, /* (239) limit_opt ::= */
  {  272,   -2 }, /* (240) limit_opt ::= LIMIT signed */
  {  272,   -4 }, /* (241) limit_opt ::= LIMIT signed OFFSET signed */
  {  272,   -4 }, /* (242) limit_opt ::= LIMIT signed COMMA signed */
  {  271,    0 }, /* (243) slimit_opt ::= */
  {  271,   -2 }, /* (244) slimit_opt ::= SLIMIT signed */
  {  271,   -4 }, /* (245) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  271,   -4 }, /* (246) slimit_opt ::= SLIMIT signed COMMA signed */
  {  261,    0 }, /* (247) where_opt ::= */
  {  261,   -2 }, /* (248) where_opt ::= WHERE expr */
  {  276,   -3 }, /* (249) expr ::= LP expr RP */
  {  276,   -1 }, /* (250) expr ::= ID */
  {  276,   -3 }, /* (251) expr ::= ID DOT ID */
  {  276,   -3 }, /* (252) expr ::= ID DOT STAR */
  {  276,   -1 }, /* (253) expr ::= INTEGER */
  {  276,   -2 }, /* (254) expr ::= MINUS INTEGER */
  {  276,   -2 }, /* (255) expr ::= PLUS INTEGER */
  {  276,   -1 }, /* (256) expr ::= FLOAT */
  {  276,   -2 }, /* (257) expr ::= MINUS FLOAT */
  {  276,   -2 }, /* (258) expr ::= PLUS FLOAT */
  {  276,   -1 }, /* (259) expr ::= STRING */
  {  276,   -1 }, /* (260) expr ::= NOW */
  {  276,   -1 }, /* (261) expr ::= VARIABLE */
  {  276,   -2 }, /* (262) expr ::= PLUS VARIABLE */
  {  276,   -2 }, /* (263) expr ::= MINUS VARIABLE */
  {  276,   -1 }, /* (264) expr ::= BOOL */
  {  276,   -1 }, /* (265) expr ::= NULL */
  {  276,   -4 }, /* (266) expr ::= ID LP exprlist RP */
  {  276,   -4 }, /* (267) expr ::= ID LP STAR RP */
  {  276,   -6 }, /* (268) expr ::= ID LP expr AS typename RP */
  {  276,   -3 }, /* (269) expr ::= expr IS NULL */
  {  276,   -4 }, /* (270) expr ::= expr IS NOT NULL */
  {  276,   -3 }, /* (271) expr ::= expr LT expr */
  {  276,   -3 }, /* (272) expr ::= expr GT expr */
  {  276,   -3 }, /* (273) expr ::= expr LE expr */
  {  276,   -3 }, /* (274) expr ::= expr GE expr */
  {  276,   -3 }, /* (275) expr ::= expr NE expr */
  {  276,   -3 }, /* (276) expr ::= expr EQ expr */
  {  276,   -5 }, /* (277) expr ::= expr BETWEEN expr AND expr */
  {  276,   -3 }, /* (278) expr ::= expr AND expr */
  {  276,   -3 }, /* (279) expr ::= expr OR expr */
  {  276,   -3 }, /* (280) expr ::= expr PLUS expr */
  {  276,   -3 }, /* (281) expr ::= expr MINUS expr */
  {  276,   -3 }, /* (282) expr ::= expr STAR expr */
  {  276,   -3 }, /* (283) expr ::= expr SLASH expr */
  {  276,   -3 }, /* (284) expr ::= expr REM expr */
  {  276,   -3 }, /* (285) expr ::= expr LIKE expr */
  {  276,   -3 }, /* (286) expr ::= expr MATCH expr */
  {  276,   -3 }, /* (287) expr ::= expr NMATCH expr */
  {  276,   -3 }, /* (288) expr ::= ID CONTAINS STRING */
  {  276,   -5 }, /* (289) expr ::= ID DOT ID CONTAINS STRING */
  {  286,   -3 }, /* (290) arrow ::= ID ARROW STRING */
  {  286,   -5 }, /* (291) arrow ::= ID DOT ID ARROW STRING */
  {  276,   -1 }, /* (292) expr ::= arrow */
  {  276,   -5 }, /* (293) expr ::= expr IN LP exprlist RP */
  {  212,   -3 }, /* (294) exprlist ::= exprlist COMMA expritem */
  {  212,   -1 }, /* (295) exprlist ::= expritem */
  {  288,   -1 }, /* (296) expritem ::= expr */
  {  288,    0 }, /* (297) expritem ::= */
  {  204,   -3 }, /* (298) cmd ::= RESET QUERY CACHE */
  {  204,   -3 }, /* (299) cmd ::= SYNCDB ids REPLICA */
  {  204,   -7 }, /* (300) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  204,   -7 }, /* (301) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  204,   -7 }, /* (302) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  204,   -7 }, /* (303) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  204,   -7 }, /* (304) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  204,   -8 }, /* (305) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  204,   -9 }, /* (306) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  204,   -7 }, /* (307) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  204,   -7 }, /* (308) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  204,   -7 }, /* (309) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  204,   -7 }, /* (310) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  204,   -7 }, /* (311) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  204,   -7 }, /* (312) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  204,   -8 }, /* (313) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  204,   -9 }, /* (314) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  204,   -7 }, /* (315) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  204,   -3 }, /* (316) cmd ::= KILL CONNECTION INTEGER */
  {  204,   -5 }, /* (317) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  204,   -5 }, /* (318) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy560, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy205);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy205);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy209);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy205);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy560, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy369, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy369, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy205.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy205.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy205.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy205.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy205.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy205.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy205.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy205.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy205.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy205 = yylhsminor.yy205;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 158: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==158);
{ yylhsminor.yy209 = tVariantListAppend(yymsp[-2].minor.yy209, &yymsp[0].minor.yy54, -1);    }
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 86: /* intitemlist ::= intitem */
      case 159: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==159);
{ yylhsminor.yy209 = tVariantListAppend(NULL, &yymsp[0].minor.yy54, -1); }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 87: /* intitem ::= INTEGER */
      case 160: /* tagitem ::= INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= FLOAT */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= STRING */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= BOOL */ yytestcase(yyruleno==163);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy54, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy54 = yylhsminor.yy54;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy209 = yymsp[0].minor.yy209; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy560); yymsp[1].minor.yy560.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.keep = yymsp[0].minor.yy209; }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy560 = yymsp[0].minor.yy560; yylhsminor.yy560.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy560 = yylhsminor.yy560;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy560); yymsp[1].minor.yy560.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy369, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy55 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy369, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy55;  // negative value of name length
    tSetColumnType(&yylhsminor.yy369, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy369 = yylhsminor.yy369;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy369, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy369 = yylhsminor.yy369;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy55 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy55 = yylhsminor.yy55;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy55 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy55 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy404;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy34);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy404 = pCreateTable;
}
  yymsp[0].minor.yy404 = yylhsminor.yy404;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy404->childTableInfo, &yymsp[0].minor.yy34);
  yylhsminor.yy404 = yymsp[-1].minor.yy404;
}
  yymsp[-1].minor.yy404 = yylhsminor.yy404;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy404 = tSetCreateTableInfo(yymsp[-1].minor.yy209, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy404, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy404 = yylhsminor.yy404;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy404 = tSetCreateTableInfo(yymsp[-5].minor.yy209, yymsp[-1].minor.yy209, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy404, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy404 = yylhsminor.yy404;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy34 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy209, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy34 = yylhsminor.yy34;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy34 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy209, yymsp[-1].minor.yy209, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy34 = yylhsminor.yy34;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy209, &yymsp[0].minor.yy0); yylhsminor.yy209 = yymsp[-2].minor.yy209;  }
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy209 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy209, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy404 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy160, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy404, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy404 = yylhsminor.yy404;
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
{taosArrayPush(yymsp[-2].minor.yy209, &yymsp[0].minor.yy369); yylhsminor.yy209 = yymsp[-2].minor.yy209;  }
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 156: /* columnlist ::= column */
{yylhsminor.yy209 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy209, &yymsp[0].minor.yy369);}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 157: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy369, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy369);
}
  yymsp[-1].minor.yy369 = yylhsminor.yy369;
        break;
      case 164: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy54, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy54 = yylhsminor.yy54;
        break;
      case 165: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy54, &yymsp[0].minor.yy0, TK_NOW, true);}
  yymsp[0].minor.yy54 = yylhsminor.yy54;
        break;
      case 166: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy54, &yymsp[0].minor.yy0, TK_PLUS, true);
}
        break;
      case 167: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy54, &yymsp[0].minor.yy0, TK_MINUS, true);
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
    tVariantCreate(&yylhsminor.yy54, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy54 = yylhsminor.yy54;
        break;
      case 172: /* select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy160 = tSetQuerySqlNode(&yymsp[-14].minor.yy0, yymsp[-13].minor.yy209, yymsp[-12].minor.yy530, yymsp[-11].minor.yy574, yymsp[-4].minor.yy209, yymsp[-2].minor.yy209, &yymsp[-9].minor.yy102, &yymsp[-7].minor.yy115, &yymsp[-6].minor.yy290, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy209, &yymsp[0].minor.yy534, &yymsp[-1].minor.yy534, yymsp[-3].minor.yy574, &yymsp[-10].minor.yy22);
}
  yymsp[-14].minor.yy160 = yylhsminor.yy160;
        break;
      case 173: /* select ::= LP select RP */
{yymsp[-2].minor.yy160 = yymsp[-1].minor.yy160;}
        break;
      case 174: /* union ::= select */
{ yylhsminor.yy209 = setSubclause(NULL, yymsp[0].minor.yy160); }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 175: /* union ::= union UNION ALL select */
{ yylhsminor.yy209 = appendSelectClause(yymsp[-3].minor.yy209, yymsp[0].minor.yy160); }
  yymsp[-3].minor.yy209 = yylhsminor.yy209;
        break;
      case 176: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy209, NULL, TSDB_SQL_SELECT); }
        break;
      case 177: /* select ::= SELECT selcollist */
{
  yylhsminor.yy160 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy209, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy160 = yylhsminor.yy160;
        break;
      case 178: /* sclp ::= selcollist COMMA */
{yylhsminor.yy209 = yymsp[-1].minor.yy209;}
  yymsp[-1].minor.yy209 = yylhsminor.yy209;
        break;
      case 179: /* sclp ::= */
      case 220: /* orderby_opt ::= */ yytestcase(yyruleno==220);
{yymsp[1].minor.yy209 = 0;}
        break;
      case 180: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy209 = tSqlExprListAppend(yymsp[-3].minor.yy209, yymsp[-1].minor.yy574,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy209 = yylhsminor.yy209;
        break;
      case 181: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy209 = tSqlExprListAppend(yymsp[-1].minor.yy209, pNode, 0, 0);
}
  yymsp[-1].minor.yy209 = yylhsminor.yy209;
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
{yymsp[-1].minor.yy530 = yymsp[0].minor.yy530;}
        break;
      case 189: /* sub ::= LP union RP */
{yymsp[-2].minor.yy530 = addSubqueryElem(NULL, yymsp[-1].minor.yy209, NULL);}
        break;
      case 190: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy530 = addSubqueryElem(NULL, yymsp[-2].minor.yy209, &yymsp[0].minor.yy0);}
        break;
      case 191: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy530 = addSubqueryElem(yymsp[-5].minor.yy530, yymsp[-2].minor.yy209, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy530 = yylhsminor.yy530;
        break;
      case 192: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy530 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy530 = yylhsminor.yy530;
        break;
      case 193: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy530 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy530 = yylhsminor.yy530;
        break;
      case 194: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy530 = setTableNameList(yymsp[-3].minor.yy530, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy530 = yylhsminor.yy530;
        break;
      case 195: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy530 = setTableNameList(yymsp[-4].minor.yy530, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy530 = yylhsminor.yy530;
        break;
      case 196: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 197: /* timestamp ::= INTEGER */
{ yylhsminor.yy574 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 198: /* timestamp ::= MINUS INTEGER */
      case 199: /* timestamp ::= PLUS INTEGER */ yytestcase(yyruleno==199);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy574 = tSqlExprCreateTimestamp(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy574 = yylhsminor.yy574;
        break;
      case 200: /* timestamp ::= STRING */
{ yylhsminor.yy574 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 201: /* timestamp ::= NOW */
{ yylhsminor.yy574 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 202: /* timestamp ::= NOW PLUS VARIABLE */
{yymsp[-2].minor.yy574 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_PLUS);  }
        break;
      case 203: /* timestamp ::= NOW MINUS VARIABLE */
{yymsp[-2].minor.yy574 = tSqlExprCreateTimestamp(&yymsp[0].minor.yy0, TK_MINUS); }
        break;
      case 204: /* range_option ::= */
{yymsp[1].minor.yy22.start = 0; yymsp[1].minor.yy22.end = 0;}
        break;
      case 205: /* range_option ::= RANGE LP timestamp COMMA timestamp RP */
{yymsp[-5].minor.yy22.start = yymsp[-3].minor.yy574; yymsp[-5].minor.yy22.end = yymsp[-1].minor.yy574;}
        break;
      case 206: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy102.interval = yymsp[-1].minor.yy0; yylhsminor.yy102.offset.n = 0; yylhsminor.yy102.token = yymsp[-3].minor.yy380;}
  yymsp[-3].minor.yy102 = yylhsminor.yy102;
        break;
      case 207: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy102.interval = yymsp[-3].minor.yy0; yylhsminor.yy102.offset = yymsp[-1].minor.yy0;   yylhsminor.yy102.token = yymsp[-5].minor.yy380;}
  yymsp[-5].minor.yy102 = yylhsminor.yy102;
        break;
      case 208: /* interval_option ::= */
{memset(&yymsp[1].minor.yy102, 0, sizeof(yymsp[1].minor.yy102));}
        break;
      case 209: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy380 = TK_INTERVAL;}
        break;
      case 210: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy380 = TK_EVERY;   }
        break;
      case 211: /* session_option ::= */
{yymsp[1].minor.yy115.col.n = 0; yymsp[1].minor.yy115.gap.n = 0;}
        break;
      case 212: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy115.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy115.gap = yymsp[-1].minor.yy0;
}
        break;
      case 213: /* windowstate_option ::= */
{ yymsp[1].minor.yy290.col.n = 0; yymsp[1].minor.yy290.col.z = NULL;}
        break;
      case 214: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy290.col = yymsp[-1].minor.yy0; }
        break;
      case 215: /* fill_opt ::= */
{ yymsp[1].minor.yy209 = 0;     }
        break;
      case 216: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy209, &A, -1, 0);
    yymsp[-5].minor.yy209 = yymsp[-1].minor.yy209;
}
        break;
      case 217: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy209 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1, true);
}
        break;
      case 218: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 219: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 221: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy209 = yymsp[0].minor.yy209;}
        break;
      case 222: /* sortlist ::= sortlist COMMA item sortorder */
{
  yylhsminor.yy209 = commonItemAppend(yymsp[-3].minor.yy209, &yymsp[-1].minor.yy54, NULL, false, yymsp[0].minor.yy332);
}
  yymsp[-3].minor.yy209 = yylhsminor.yy209;
        break;
      case 223: /* sortlist ::= sortlist COMMA arrow sortorder */
{
  yylhsminor.yy209 = commonItemAppend(yymsp[-3].minor.yy209, NULL, yymsp[-1].minor.yy574, true, yymsp[0].minor.yy332);
}
  yymsp[-3].minor.yy209 = yylhsminor.yy209;
        break;
      case 224: /* sortlist ::= item sortorder */
{
  yylhsminor.yy209 = commonItemAppend(NULL, &yymsp[-1].minor.yy54, NULL, false, yymsp[0].minor.yy332);
}
  yymsp[-1].minor.yy209 = yylhsminor.yy209;
        break;
      case 225: /* sortlist ::= arrow sortorder */
{
  yylhsminor.yy209 = commonItemAppend(NULL, NULL, yymsp[-1].minor.yy574, true, yymsp[0].minor.yy332);
}
  yymsp[-1].minor.yy209 = yylhsminor.yy209;
        break;
      case 226: /* item ::= ID */
{
  toTSDBType(yymsp[0].minor.yy0.type);
  tVariantCreate(&yylhsminor.yy54, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy54 = yylhsminor.yy54;
        break;
      case 227: /* item ::= ID DOT ID */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n);
  tVariantCreate(&yylhsminor.yy54, &yymsp[-2].minor.yy0);
}
  yymsp[-2].minor.yy54 = yylhsminor.yy54;
        break;
      case 228: /* sortorder ::= ASC */
{ yymsp[0].minor.yy332 = TSDB_ORDER_ASC; }
        break;
      case 229: /* sortorder ::= DESC */
{ yymsp[0].minor.yy332 = TSDB_ORDER_DESC;}
        break;
      case 230: /* sortorder ::= */
{ yymsp[1].minor.yy332 = TSDB_ORDER_ASC; }
        break;
      case 231: /* groupby_opt ::= */
{ yymsp[1].minor.yy209 = 0;}
        break;
      case 232: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy209 = yymsp[0].minor.yy209;}
        break;
      case 233: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy209 = commonItemAppend(yymsp[-2].minor.yy209, &yymsp[0].minor.yy54, NULL, false, -1);
}
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 234: /* grouplist ::= grouplist COMMA arrow */
{
  yylhsminor.yy209 = commonItemAppend(yymsp[-2].minor.yy209, NULL, yymsp[0].minor.yy574, true, -1);
}
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 235: /* grouplist ::= item */
{
  yylhsminor.yy209 = commonItemAppend(NULL, &yymsp[0].minor.yy54, NULL, false, -1);
}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 236: /* grouplist ::= arrow */
{
  yylhsminor.yy209 = commonItemAppend(NULL, NULL, yymsp[0].minor.yy574, true, -1);
}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 237: /* having_opt ::= */
      case 247: /* where_opt ::= */ yytestcase(yyruleno==247);
      case 297: /* expritem ::= */ yytestcase(yyruleno==297);
{yymsp[1].minor.yy574 = 0;}
        break;
      case 238: /* having_opt ::= HAVING expr */
      case 248: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==248);
{yymsp[-1].minor.yy574 = yymsp[0].minor.yy574;}
        break;
      case 239: /* limit_opt ::= */
      case 243: /* slimit_opt ::= */ yytestcase(yyruleno==243);
{yymsp[1].minor.yy534.limit = -1; yymsp[1].minor.yy534.offset = 0;}
        break;
      case 240: /* limit_opt ::= LIMIT signed */
      case 244: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==244);
{yymsp[-1].minor.yy534.limit = yymsp[0].minor.yy55;  yymsp[-1].minor.yy534.offset = 0;}
        break;
      case 241: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy534.limit = yymsp[-2].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[0].minor.yy55;}
        break;
      case 242: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy534.limit = yymsp[0].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[-2].minor.yy55;}
        break;
      case 245: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy534.limit = yymsp[-2].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[0].minor.yy55;}
        break;
      case 246: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy534.limit = yymsp[0].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[-2].minor.yy55;}
        break;
      case 249: /* expr ::= LP expr RP */
{yylhsminor.yy574 = yymsp[-1].minor.yy574; yylhsminor.yy574->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy574->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 250: /* expr ::= ID */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 251: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 252: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 253: /* expr ::= INTEGER */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 254: /* expr ::= MINUS INTEGER */
      case 255: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==255);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy574 = yylhsminor.yy574;
        break;
      case 256: /* expr ::= FLOAT */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 257: /* expr ::= MINUS FLOAT */
      case 258: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==258);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy574 = yylhsminor.yy574;
        break;
      case 259: /* expr ::= STRING */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 260: /* expr ::= NOW */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 261: /* expr ::= VARIABLE */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 262: /* expr ::= PLUS VARIABLE */
      case 263: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==263);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy574 = yylhsminor.yy574;
        break;
      case 264: /* expr ::= BOOL */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 265: /* expr ::= NULL */
{ yylhsminor.yy574 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 266: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy574 = tSqlExprCreateFunction(yymsp[-1].minor.yy209, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy574 = yylhsminor.yy574;
        break;
      case 267: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy574 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy574 = yylhsminor.yy574;
        break;
      case 268: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy574 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy574, &yymsp[-1].minor.yy369, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy574 = yylhsminor.yy574;
        break;
      case 269: /* expr ::= expr IS NULL */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 270: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-3].minor.yy574, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy574 = yylhsminor.yy574;
        break;
      case 271: /* expr ::= expr LT expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_LT);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 272: /* expr ::= expr GT expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_GT);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 273: /* expr ::= expr LE expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_LE);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 274: /* expr ::= expr GE expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_GE);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 275: /* expr ::= expr NE expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_NE);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 276: /* expr ::= expr EQ expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_EQ);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 277: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy574); yylhsminor.yy574 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy574, yymsp[-2].minor.yy574, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy574, TK_LE), TK_AND);}
  yymsp[-4].minor.yy574 = yylhsminor.yy574;
        break;
      case 278: /* expr ::= expr AND expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_AND);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 279: /* expr ::= expr OR expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_OR); }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 280: /* expr ::= expr PLUS expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_PLUS);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 281: /* expr ::= expr MINUS expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_MINUS); }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 282: /* expr ::= expr STAR expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_STAR);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 283: /* expr ::= expr SLASH expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_DIVIDE);}
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 284: /* expr ::= expr REM expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_REM);   }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 285: /* expr ::= expr LIKE expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_LIKE);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 286: /* expr ::= expr MATCH expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_MATCH);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 287: /* expr ::= expr NMATCH expr */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-2].minor.yy574, yymsp[0].minor.yy574, TK_NMATCH);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 288: /* expr ::= ID CONTAINS STRING */
{ tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy574 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 289: /* expr ::= ID DOT ID CONTAINS STRING */
{ yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy574 = tSqlExprCreate(S, M, TK_CONTAINS);  }
  yymsp[-4].minor.yy574 = yylhsminor.yy574;
        break;
      case 290: /* arrow ::= ID ARROW STRING */
{tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy574 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-2].minor.yy574 = yylhsminor.yy574;
        break;
      case 291: /* arrow ::= ID DOT ID ARROW STRING */
{yymsp[-4].minor.yy0.n += (1+yymsp[-2].minor.yy0.n); tSqlExpr* S = tSqlExprCreateIdValue(pInfo, &yymsp[-4].minor.yy0, TK_ID); tSqlExpr* M = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING); yylhsminor.yy574 = tSqlExprCreate(S, M, TK_ARROW);  }
  yymsp[-4].minor.yy574 = yylhsminor.yy574;
        break;
      case 292: /* expr ::= arrow */
      case 296: /* expritem ::= expr */ yytestcase(yyruleno==296);
{yylhsminor.yy574 = yymsp[0].minor.yy574;}
  yymsp[0].minor.yy574 = yylhsminor.yy574;
        break;
      case 293: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy574 = tSqlExprCreate(yymsp[-4].minor.yy574, (tSqlExpr*)yymsp[-1].minor.yy209, TK_IN); }
  yymsp[-4].minor.yy574 = yylhsminor.yy574;
        break;
      case 294: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy209 = tSqlExprListAppend(yymsp[-2].minor.yy209,yymsp[0].minor.yy574,0, 0);}
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 295: /* exprlist ::= expritem */
{yylhsminor.yy209 = tSqlExprListAppend(0,yymsp[0].minor.yy574,0, 0);}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 298: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 299: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 300: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 301: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 302: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 303: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 304: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 305: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 306: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy54, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 307: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 308: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 309: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 310: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 311: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 312: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1, false);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 313: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 314: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1, false);
    A = tVariantListAppend(A, &yymsp[0].minor.yy54, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 315: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 316: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 317: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 318: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
