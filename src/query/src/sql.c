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
#define YYNOCODE 285
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
#define ParseARG_PARAM ,pInfo
#define ParseARG_FETCH SSqlInfo* pInfo=yypParser->pInfo;
#define ParseARG_STORE yypParser->pInfo=pInfo;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYFALLBACK 1
#define YYNSTATE             393
#define YYNRULE              316
#define YYNRULE_WITH_ACTION  316
#define YYNTOKEN             202
#define YY_MAX_SHIFT         392
#define YY_MIN_SHIFTREDUCE   618
#define YY_MAX_SHIFTREDUCE   933
#define YY_ERROR_ACTION      934
#define YY_ACCEPT_ACTION     935
#define YY_NO_ACTION         936
#define YY_MIN_REDUCE        937
#define YY_MAX_REDUCE        1252
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
#define YY_ACTTAB_COUNT (858)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   102,  669,  669, 1168,  159, 1169,  310,  811,  258,  670,
 /*    10 */   670,  814,  391,  239,   37,   38,   24,   41,   42, 1086,
 /*    20 */  1078,  261,   31,   30,   29, 1091, 1226,   40,  342,   45,
 /*    30 */    43,   46,   44, 1075, 1076,   55, 1079,   36,   35,  296,
 /*    40 */   297,   34,   33,   32,   37,   38,  211,   41,   42,  705,
 /*    50 */    84,  261,   31,   30,   29,  212, 1226,   40,  342,   45,
 /*    60 */    43,   46,   44,  935,  392, 1226,  254,   36,   35,  209,
 /*    70 */   213,   34,   33,   32,  291,  290,  128,  122,  133, 1226,
 /*    80 */  1226, 1229, 1228,  132, 1077,  138,  141,  131,   37,   38,
 /*    90 */    85,   41,   42,  985,  135,  261,   31,   30,   29,  669,
 /*   100 */   194,   40,  342,   45,   43,   46,   44,  670,  338,  285,
 /*   110 */    13,   36,   35, 1107,  101,   34,   33,   32,   37,   38,
 /*   120 */    58,   41,   42,   60,  244,  261,   31,   30,   29,  218,
 /*   130 */   284,   40,  342,   45,   43,   46,   44,  314,   97, 1226,
 /*   140 */    96,   36,   35,  669,  104,   34,   33,   32,  338,   37,
 /*   150 */    39,  670,   41,   42, 1107,  174,  261,   31,   30,   29,
 /*   160 */   276,  863,   40,  342,   45,   43,   46,   44, 1116,  280,
 /*   170 */   279,  242,   36,   35,  300,  219,   34,   33,   32,  204,
 /*   180 */   202,  200,  105,   59,   51, 1226,  199,  148,  147,  146,
 /*   190 */   145,  619,  620,  621,  622,  623,  624,  625,  626,  627,
 /*   200 */   628,  629,  630,  631,  632,  157,  995,  240,   38,  220,
 /*   210 */    41,   42,   59,  194,  261,   31,   30,   29, 1080, 1226,
 /*   220 */    40,  342,   45,   43,   46,   44,  823,  824,  241, 1107,
 /*   230 */    36,   35, 1089,  986,   34,   33,   32, 1113,   41,   42,
 /*   240 */   194,  248,  261,   31,   30,   29,  243,  377,   40,  342,
 /*   250 */    45,   43,   46,   44,   34,   33,   32,  251,   36,   35,
 /*   260 */  1248, 1089,   34,   33,   32,   67,  336,  384,  383,  335,
 /*   270 */   334,  333,  382,  332,  331,  330,  381,  329,  380,  379,
 /*   280 */  1054, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050,
 /*   290 */  1051, 1052, 1053, 1055, 1056,  232,  879,   25,  246,  867,
 /*   300 */  1218,  870, 1092,  873,  777, 1217,   59,  774, 1216,  775,
 /*   310 */  1226,  776, 1240,  235,  217, 1226,  232,  879, 1226,  820,
 /*   320 */   867,  225,  870, 1226,  873,  385, 1023,  144,  143,  142,
 /*   330 */   224,  237,  238,  161,  350,   91,    5,   62,  184,  266,
 /*   340 */   267,  292,  130,  183,  111,  116,  107,  115,  264,  293,
 /*   350 */    59,  252,  237,  238,  377, 1089,  344,   45,   43,   46,
 /*   360 */    44,   67,  325,  384,  383,   36,   35,  270,  382,   34,
 /*   370 */    33,   32,  381,   68,  380,  379, 1062,  271, 1060, 1061,
 /*   380 */    47,  753,  341, 1063,  869,  257,  872, 1064,  180, 1065,
 /*   390 */  1066,  262,  868,  253,  871,  354,  283, 1092,   83, 1089,
 /*   400 */   271,   47,   36,   35,  340,  233,   34,   33,   32,  236,
 /*   410 */    59,  181, 1179,  367,  366,  880,  874,  876,   54, 1226,
 /*   420 */    59,  260,   59,  778,  268,   79,  265,   59,  263,   59,
 /*   430 */   353,  352,  390,  388,  646,   59,  880,  874,  876,  795,
 /*   440 */   875,   59,   59,    6,  255,  272,  213,  269, 1092,  362,
 /*   450 */   361,  320,  213,  215,   91,  355, 1226,  271, 1229, 1089,
 /*   460 */   216,  875, 1226, 1226, 1229,  356,   80,  357,  343, 1089,
 /*   470 */  1226, 1089,  363,  843,  364,  221, 1089,  214, 1089,  830,
 /*   480 */   365,  156,  154,  153, 1089, 1226,  369, 1226,   88,  222,
 /*   490 */  1089, 1088,   68,   71,  223, 1166,  227, 1167,  228, 1226,
 /*   500 */   792,  229,  877,  271, 1226,  226, 1226,  210, 1226,  100,
 /*   510 */   878, 1226,    3,  195, 1090, 1226,   99, 1226,   98,    1,
 /*   520 */   182,   89,  295,  294,   86,  799,  287,   10,  831,  340,
 /*   530 */    76,  842,  346,  763,  317,  765,  319,  764,  908,  881,
 /*   540 */   259,  347,   48,  668, 1178,   82,  313,   60,   60,   71,
 /*   550 */   103,   71,    9,    9,  345,  249,   15,    9,   14,  287,
 /*   560 */   121,   17,  120,   16,  784,  782,  785,  783,  359,  358,
 /*   570 */    19, 1175,   18,   77,  127, 1174,  126,  752,  250,   21,
 /*   580 */   866,   20,  140,  139,  368,  281,  158, 1115,   26, 1126,
 /*   590 */  1123, 1108, 1124,  288, 1128,  160, 1087, 1158,  165,  306,
 /*   600 */  1157, 1156, 1155,  176,  177,  155, 1085,  178,  179,  810,
 /*   610 */  1000,  322,  323,  299,  245,  324,  327,  328,   69,  301,
 /*   620 */   207,   65,  339, 1105,  994,  351, 1247,  303,  118, 1246,
 /*   630 */  1243,  185,   81,   78,  360, 1239,  124, 1238,  166, 1235,
 /*   640 */   186, 1020,   66,   61,   70,  315,  884,  208,  982,  134,
 /*   650 */   167,   28,  311,  309,  168,  307,  305,  980,  136,  137,
 /*   660 */   169,  978,  977,  302,  273,  197,  198,  974,  973,  972,
 /*   670 */   971,  970,  969,  968,  201,  203,  960,  205,  957,  206,
 /*   680 */   953,  298,   27,  286,   87,   92,  304,  326,  378,  129,
 /*   690 */   370,  371,  372,  373,  374,  234,  375,  376,  256,  321,
 /*   700 */   386,  933,  274,  275,  230,  170,  932,  277,  231,  112,
 /*   710 */   999,  998,  278,  113,  931,  914,  913,  282,  287,  316,
 /*   720 */    11,  976,  975,  189, 1021,  149,  191,  187,  188,  967,
 /*   730 */   190,  192,  193,  150,  151,  966,   90,  152,  959, 1058,
 /*   740 */   958,  787,   53,  171,  172,  175,    2,  173, 1022,   52,
 /*   750 */     4,  289,   93,  819,   74, 1068,  817,  813,  812,   75,
 /*   760 */   816,  821,  162,  164,  832,  163,  247,  826,   94,   63,
 /*   770 */   828,   95,  308,  345,  312,   22,   64,   23,   12,   49,
 /*   780 */   318,   50,  106,  683,  104,   56,  109,  108,  718,  716,
 /*   790 */    57,  715,  110,  714,  712,  711,  710,  707,  337,  673,
 /*   800 */   114,    7,  905,  903,  883,  906,  882,  904,  885,    8,
 /*   810 */   349,  348,   72,  117,   60,  755,   73,  754,  119,  751,
 /*   820 */   123,  699,  125,  697,  689,  695,  781,  780,  691,  693,
 /*   830 */   687,  685,  721,  720,  719,  717,  713,  709,  708,  196,
 /*   840 */   671,  636,  937,  645,  387,  643,  936,  936,  936,  936,
 /*   850 */   936,  936,  936,  936,  936,  936,  936,  389,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   212,    1,    1,  280,  204,  282,  283,    5,  211,    9,
 /*    10 */     9,    9,  204,  205,   14,   15,  272,   17,   18,  204,
 /*    20 */     0,   21,   22,   23,   24,  254,  282,   27,   28,   29,
 /*    30 */    30,   31,   32,  245,  246,  247,  248,   37,   38,   37,
 /*    40 */    38,   41,   42,   43,   14,   15,  272,   17,   18,    5,
 /*    50 */   212,   21,   22,   23,   24,  272,  282,   27,   28,   29,
 /*    60 */    30,   31,   32,  202,  203,  282,  251,   37,   38,  272,
 /*    70 */   272,   41,   42,   43,  274,  275,   66,   67,   68,  282,
 /*    80 */   282,  284,  284,   73,  246,   75,   76,   77,   14,   15,
 /*    90 */    90,   17,   18,  210,   84,   21,   22,   23,   24,    1,
 /*   100 */   217,   27,   28,   29,   30,   31,   32,    9,   88,   87,
 /*   110 */    86,   37,   38,  252,   90,   41,   42,   43,   14,   15,
 /*   120 */    90,   17,   18,  101,  122,   21,   22,   23,   24,  272,
 /*   130 */   269,   27,   28,   29,   30,   31,   32,  279,  280,  282,
 /*   140 */   282,   37,   38,    1,  120,   41,   42,   43,   88,   14,
 /*   150 */    15,    9,   17,   18,  252,  259,   21,   22,   23,   24,
 /*   160 */   148,   87,   27,   28,   29,   30,   31,   32,  204,  157,
 /*   170 */   158,  269,   37,   38,  278,  272,   41,   42,   43,   66,
 /*   180 */    67,   68,  212,  204,   86,  282,   73,   74,   75,   76,
 /*   190 */    77,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   200 */    58,   59,   60,   61,   62,   63,  210,   65,   15,  272,
 /*   210 */    17,   18,  204,  217,   21,   22,   23,   24,  248,  282,
 /*   220 */    27,   28,   29,   30,   31,   32,  130,  131,  249,  252,
 /*   230 */    37,   38,  253,  210,   41,   42,   43,  273,   17,   18,
 /*   240 */   217,    1,   21,   22,   23,   24,  269,   94,   27,   28,
 /*   250 */    29,   30,   31,   32,   41,   42,   43,  249,   37,   38,
 /*   260 */   254,  253,   41,   42,   43,  102,  103,  104,  105,  106,
 /*   270 */   107,  108,  109,  110,  111,  112,  113,  114,  115,  116,
 /*   280 */   228,  229,  230,  231,  232,  233,  234,  235,  236,  237,
 /*   290 */   238,  239,  240,  241,  242,    1,    2,   48,  250,    5,
 /*   300 */   272,    7,  254,    9,    2,  272,  204,    5,  272,    7,
 /*   310 */   282,    9,  254,  272,   65,  282,    1,    2,  282,   87,
 /*   320 */     5,   72,    7,  282,    9,  226,  227,   78,   79,   80,
 /*   330 */    81,   37,   38,  101,   85,   86,   66,   67,   68,   37,
 /*   340 */    38,  277,   82,   73,   74,   75,   76,   77,   72,  277,
 /*   350 */   204,  249,   37,   38,   94,  253,   41,   29,   30,   31,
 /*   360 */    32,  102,   92,  104,  105,   37,   38,   72,  109,   41,
 /*   370 */    42,   43,  113,  124,  115,  116,  228,  204,  230,  231,
 /*   380 */    86,    5,   25,  235,    5,  211,    7,  239,  215,  241,
 /*   390 */   242,  211,    5,  250,    7,  249,  147,  254,  149,  253,
 /*   400 */   204,   86,   37,   38,   47,  156,   41,   42,   43,  272,
 /*   410 */   204,  215,  244,   37,   38,  121,  122,  123,   86,  282,
 /*   420 */   204,   64,  204,  121,  122,  101,  150,  204,  152,  204,
 /*   430 */   154,  155,   69,   70,   71,  204,  121,  122,  123,   41,
 /*   440 */   146,  204,  204,   86,  250,  150,  272,  152,  254,  154,
 /*   450 */   155,  119,  272,  272,   86,  249,  282,  204,  284,  253,
 /*   460 */   272,  146,  282,  282,  284,  249,  142,  249,  215,  253,
 /*   470 */   282,  253,  249,   80,  249,  272,  253,  272,  253,   87,
 /*   480 */   249,   66,   67,   68,  253,  282,  249,  282,   87,  272,
 /*   490 */   253,  253,  124,  101,  272,  280,  272,  282,  272,  282,
 /*   500 */   101,  272,  123,  204,  282,  272,  282,  272,  282,  255,
 /*   510 */   123,  282,  208,  209,  215,  282,  280,  282,  282,  213,
 /*   520 */   214,   87,   37,   38,  270,  127,  125,  128,   87,   47,
 /*   530 */   101,  138,   25,   87,   87,   87,   87,   87,   87,   87,
 /*   540 */     1,   16,  101,   87,  244,   86,   64,  101,  101,  101,
 /*   550 */   101,  101,  101,  101,   47,  244,  151,  101,  153,  125,
 /*   560 */   151,  151,  153,  153,    5,    5,    7,    7,   37,   38,
 /*   570 */   151,  244,  153,  144,  151,  244,  153,  118,  244,  151,
 /*   580 */    41,  153,   82,   83,  244,  204,  204,  204,  271,  204,
 /*   590 */   204,  252,  204,  252,  204,  204,  252,  281,  204,  204,
 /*   600 */   281,  281,  281,  256,  204,   64,  204,  204,  204,  123,
 /*   610 */   204,  204,  204,  276,  276,  204,  204,  204,  204,  276,
 /*   620 */   204,  204,  204,  268,  204,  204,  204,  276,  204,  204,
 /*   630 */   204,  204,  141,  143,  204,  204,  204,  204,  267,  204,
 /*   640 */   204,  204,  204,  204,  204,  136,  121,  204,  204,  204,
 /*   650 */   266,  140,  139,  134,  265,  133,  132,  204,  204,  204,
 /*   660 */   264,  204,  204,  135,  204,  204,  204,  204,  204,  204,
 /*   670 */   204,  204,  204,  204,  204,  204,  204,  204,  204,  204,
 /*   680 */   204,  129,  145,  206,  206,  206,  206,   93,  117,  100,
 /*   690 */    99,   55,   96,   98,   59,  206,   97,   95,  206,  206,
 /*   700 */    88,    5,  159,    5,  206,  263,    5,  159,  206,  212,
 /*   710 */   216,  216,    5,  212,    5,  104,  103,  148,  125,  119,
 /*   720 */    86,  206,  206,  219,  225,  207,  220,  224,  223,  206,
 /*   730 */   222,  221,  218,  207,  207,  206,  126,  207,  206,  243,
 /*   740 */   206,   87,  258,  262,  261,  257,  213,  260,  227,   86,
 /*   750 */   208,  101,  101,   87,  101,  243,  123,    5,    5,   86,
 /*   760 */   123,   87,   86,  101,   87,   86,    1,   87,   86,  101,
 /*   770 */    87,   86,   86,   47,    1,  137,  101,  137,   86,   86,
 /*   780 */   119,   86,   82,    5,  120,   91,   74,   90,    9,    5,
 /*   790 */    91,    5,   90,    5,    5,    5,    5,    5,   16,   89,
 /*   800 */    82,   86,    9,    9,   87,    9,   87,    9,  121,   86,
 /*   810 */    63,   28,   17,  153,  101,    5,   17,    5,  153,   87,
 /*   820 */   153,    5,  153,    5,    5,    5,  123,  123,    5,    5,
 /*   830 */     5,    5,    5,    5,    5,    5,    5,    5,    5,  101,
 /*   840 */    89,   64,    0,    9,   22,    9,  285,  285,  285,  285,
 /*   850 */   285,  285,  285,  285,  285,  285,  285,   22,  285,  285,
 /*   860 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
 /*   870 */   285,  285,  285,  285,  285,  285,  285,  285,  285,  285,
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
};
#define YY_SHIFT_COUNT    (392)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (842)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   249,  163,  163,  259,  259,   60,  315,  294,  294,  294,
 /*    10 */    98,    1,    1,    1,    1,    1,    1,    1,    1,    1,
 /*    20 */     1,    1,  240,  240,    0,  142,  294,  294,  294,  294,
 /*    30 */   294,  294,  294,  294,  294,  294,  294,  294,  294,  294,
 /*    40 */   294,  294,  294,  294,  294,  294,  294,  294,  302,  302,
 /*    50 */   302,  368,  368,   96,    1,   20,    1,    1,    1,    1,
 /*    60 */     1,  260,   60,  240,  240,  153,  153,   44,  858,  858,
 /*    70 */   858,  302,  302,  302,    2,    2,  376,  376,  376,  376,
 /*    80 */   376,  376,  376,    1,    1,    1,  398,    1,    1,    1,
 /*    90 */   368,  368,    1,    1,    1,    1,  393,  393,  393,  393,
 /*   100 */   399,  368,    1,    1,    1,    1,    1,    1,    1,    1,
 /*   110 */     1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
 /*   120 */     1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
 /*   130 */     1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
 /*   140 */     1,    1,    1,    1,    1,    1,    1,    1,    1,    1,
 /*   150 */     1,    1,    1,    1,    1,    1,    1,    1,  541,  541,
 /*   160 */   541,  486,  486,  486,  486,  541,  491,  490,  509,  511,
 /*   170 */   513,  519,  522,  524,  528,  552,  537,  541,  541,  541,
 /*   180 */   594,  594,  571,   60,   60,  541,  541,  589,  591,  636,
 /*   190 */   596,  595,  635,  599,  602,  571,   44,  541,  541,  612,
 /*   200 */   612,  541,  612,  541,  612,  541,  541,  858,  858,   30,
 /*   210 */    74,  104,  104,  104,  135,  193,  221,  270,  328,  328,
 /*   220 */   328,  328,  328,  328,   10,  113,  365,  365,  365,  365,
 /*   230 */   276,  295,  357,   12,   24,  213,  213,  379,  387,  363,
 /*   240 */   415,   22,  401,  434,  485,  232,  392,  441,  482,  429,
 /*   250 */   324,  446,  447,  448,  449,  450,  332,  451,  452,  507,
 /*   260 */   539,  525,  456,  405,  409,  410,  559,  560,  531,  419,
 /*   270 */   423,  459,  428,  500,  696,  543,  698,  701,  548,  707,
 /*   280 */   709,  611,  613,  569,  593,  600,  634,  610,  654,  663,
 /*   290 */   650,  651,  666,  653,  633,  637,  752,  753,  673,  674,
 /*   300 */   676,  677,  679,  680,  662,  682,  683,  685,  765,  686,
 /*   310 */   668,  638,  726,  773,  675,  640,  692,  600,  693,  661,
 /*   320 */   695,  664,  700,  694,  697,  712,  778,  699,  702,  779,
 /*   330 */   784,  786,  788,  789,  790,  791,  792,  710,  782,  718,
 /*   340 */   793,  794,  715,  717,  719,  796,  798,  687,  723,  783,
 /*   350 */   747,  795,  660,  665,  713,  713,  713,  713,  703,  704,
 /*   360 */   799,  667,  669,  713,  713,  713,  810,  812,  732,  713,
 /*   370 */   816,  818,  819,  820,  823,  824,  825,  826,  827,  828,
 /*   380 */   829,  830,  831,  832,  833,  738,  751,  834,  822,  836,
 /*   390 */   835,  777,  842,
};
#define YY_REDUCE_COUNT (208)
#define YY_REDUCE_MIN   (-277)
#define YY_REDUCE_MAX   (542)
static const short yy_reduce_ofst[] = {
 /*     0 */  -139,   52,   52,  148,  148, -212, -203,  174,  180, -202,
 /*    10 */  -200,  -21,    8,  102,  146,  206,  216,  218,  223,  225,
 /*    20 */   231,  237, -277, -142,  -36, -192, -256, -226, -217, -143,
 /*    30 */   -97,  -63,   28,   33,   36,   41,  137,  181,  188,  203,
 /*    40 */   205,  217,  222,  224,  226,  229,  233,  235,   48,  143,
 /*    50 */   194,  -98,  -23, -104, -185,  -30,  173,  196,  253,  299,
 /*    60 */   238, -117, -162,  215,  236,   -4,   23,   99,  254,  306,
 /*    70 */   304, -229,    6,   58,   64,   72,  168,  300,  311,  327,
 /*    80 */   331,  334,  340,  381,  382,  383,  317,  385,  386,  388,
 /*    90 */   339,  341,  390,  391,  394,  395,  316,  319,  320,  321,
 /*   100 */   347,  344,  400,  402,  403,  404,  406,  407,  408,  411,
 /*   110 */   412,  413,  414,  416,  417,  418,  420,  421,  422,  424,
 /*   120 */   425,  426,  427,  430,  431,  432,  433,  435,  436,  437,
 /*   130 */   438,  439,  440,  443,  444,  445,  453,  454,  455,  457,
 /*   140 */   458,  460,  461,  462,  463,  464,  465,  466,  467,  468,
 /*   150 */   469,  470,  471,  472,  473,  474,  475,  476,  477,  478,
 /*   160 */   479,  337,  338,  343,  351,  480,  355,  371,  384,  389,
 /*   170 */   396,  442,  481,  483,  487,  484,  488,  489,  492,  493,
 /*   180 */   494,  495,  496,  497,  501,  498,  502,  499,  503,  505,
 /*   190 */   504,  508,  506,  510,  514,  512,  521,  515,  516,  518,
 /*   200 */   526,  523,  527,  529,  530,  532,  534,  533,  542,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   934, 1057,  996, 1067,  983,  993, 1231, 1231, 1231, 1231,
 /*    10 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*    20 */   934,  934,  934,  934, 1117,  954,  934,  934,  934,  934,
 /*    30 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*    40 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*    50 */   934,  934,  934, 1141,  934,  993,  934,  934,  934,  934,
 /*    60 */   934, 1003,  993,  934,  934, 1003, 1003,  934, 1112, 1041,
 /*    70 */  1059,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*    80 */   934,  934,  934,  934,  934,  934, 1119, 1125, 1122,  934,
 /*    90 */   934,  934, 1127,  934,  934,  934, 1163, 1163, 1163, 1163,
 /*   100 */  1110,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   110 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   120 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   130 */   934,  934,  934,  934,  981,  934,  979,  934,  934,  934,
 /*   140 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   150 */   934,  934,  934,  934,  934,  934,  934,  952,  956,  956,
 /*   160 */   956,  934,  934,  934,  934,  956, 1172, 1176, 1153, 1170,
 /*   170 */  1164, 1148, 1146, 1144, 1152, 1137, 1180,  956,  956,  956,
 /*   180 */  1001, 1001,  997,  993,  993,  956,  956, 1019, 1017, 1015,
 /*   190 */  1007, 1013, 1009, 1011, 1005,  984,  934,  956,  956,  991,
 /*   200 */   991,  956,  991,  956,  991,  956,  956, 1041, 1059, 1230,
 /*   210 */   934, 1181, 1171, 1230,  934, 1213, 1212,  934, 1221, 1220,
 /*   220 */  1219, 1211, 1210, 1209,  934,  934, 1205, 1208, 1207, 1206,
 /*   230 */   934,  934, 1183,  934,  934, 1215, 1214,  934,  934,  934,
 /*   240 */   934,  934,  934,  934, 1134,  934,  934,  934, 1159, 1177,
 /*   250 */  1173,  934,  934,  934,  934,  934,  934,  934,  934, 1184,
 /*   260 */   934,  934,  934,  934,  934,  934,  934,  934, 1098,  934,
 /*   270 */   934, 1069,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   280 */   934,  934,  934,  934, 1109,  934,  934,  934,  934,  934,
 /*   290 */  1121, 1120,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   300 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   310 */  1165,  934, 1160,  934, 1154,  934,  934, 1081,  934,  934,
 /*   320 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   330 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   340 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   350 */   934,  934,  934,  934, 1249, 1244, 1245, 1242,  934,  934,
 /*   360 */   934,  934,  934, 1241, 1236, 1237,  934,  934,  934, 1234,
 /*   370 */   934,  934,  934,  934,  934,  934,  934,  934,  934,  934,
 /*   380 */   934,  934,  934,  934,  934, 1025,  934,  934,  963,  934,
 /*   390 */   961,  934,  934,
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
  /*  146 */ "TODAY",
  /*  147 */ "RESET",
  /*  148 */ "QUERY",
  /*  149 */ "SYNCDB",
  /*  150 */ "ADD",
  /*  151 */ "COLUMN",
  /*  152 */ "MODIFY",
  /*  153 */ "TAG",
  /*  154 */ "CHANGE",
  /*  155 */ "SET",
  /*  156 */ "KILL",
  /*  157 */ "CONNECTION",
  /*  158 */ "STREAM",
  /*  159 */ "COLON",
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
 /* 257 */ "expr ::= TODAY",
 /* 258 */ "expr ::= VARIABLE",
 /* 259 */ "expr ::= PLUS VARIABLE",
 /* 260 */ "expr ::= MINUS VARIABLE",
 /* 261 */ "expr ::= BOOL",
 /* 262 */ "expr ::= NULL",
 /* 263 */ "expr ::= ID LP exprlist RP",
 /* 264 */ "expr ::= ID LP STAR RP",
 /* 265 */ "expr ::= ID LP expr AS typename RP",
 /* 266 */ "expr ::= expr IS NULL",
 /* 267 */ "expr ::= expr IS NOT NULL",
 /* 268 */ "expr ::= expr LT expr",
 /* 269 */ "expr ::= expr GT expr",
 /* 270 */ "expr ::= expr LE expr",
 /* 271 */ "expr ::= expr GE expr",
 /* 272 */ "expr ::= expr NE expr",
 /* 273 */ "expr ::= expr EQ expr",
 /* 274 */ "expr ::= expr BETWEEN expr AND expr",
 /* 275 */ "expr ::= expr AND expr",
 /* 276 */ "expr ::= expr OR expr",
 /* 277 */ "expr ::= expr PLUS expr",
 /* 278 */ "expr ::= expr MINUS expr",
 /* 279 */ "expr ::= expr STAR expr",
 /* 280 */ "expr ::= expr SLASH expr",
 /* 281 */ "expr ::= expr REM expr",
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
   202,  /* (0) program ::= cmd */
   203,  /* (1) cmd ::= SHOW DATABASES */
   203,  /* (2) cmd ::= SHOW TOPICS */
   203,  /* (3) cmd ::= SHOW FUNCTIONS */
   203,  /* (4) cmd ::= SHOW MNODES */
   203,  /* (5) cmd ::= SHOW DNODES */
   203,  /* (6) cmd ::= SHOW ACCOUNTS */
   203,  /* (7) cmd ::= SHOW USERS */
   203,  /* (8) cmd ::= SHOW MODULES */
   203,  /* (9) cmd ::= SHOW QUERIES */
   203,  /* (10) cmd ::= SHOW CONNECTIONS */
   203,  /* (11) cmd ::= SHOW STREAMS */
   203,  /* (12) cmd ::= SHOW VARIABLES */
   203,  /* (13) cmd ::= SHOW SCORES */
   203,  /* (14) cmd ::= SHOW GRANTS */
   203,  /* (15) cmd ::= SHOW VNODES */
   203,  /* (16) cmd ::= SHOW VNODES ids */
   205,  /* (17) dbPrefix ::= */
   205,  /* (18) dbPrefix ::= ids DOT */
   206,  /* (19) cpxName ::= */
   206,  /* (20) cpxName ::= DOT ids */
   203,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   203,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   203,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   203,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   203,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
   203,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   203,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
   203,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   203,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   203,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   203,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   203,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   203,  /* (33) cmd ::= DROP FUNCTION ids */
   203,  /* (34) cmd ::= DROP DNODE ids */
   203,  /* (35) cmd ::= DROP USER ids */
   203,  /* (36) cmd ::= DROP ACCOUNT ids */
   203,  /* (37) cmd ::= USE ids */
   203,  /* (38) cmd ::= DESCRIBE ids cpxName */
   203,  /* (39) cmd ::= DESC ids cpxName */
   203,  /* (40) cmd ::= ALTER USER ids PASS ids */
   203,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   203,  /* (42) cmd ::= ALTER DNODE ids ids */
   203,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   203,  /* (44) cmd ::= ALTER LOCAL ids */
   203,  /* (45) cmd ::= ALTER LOCAL ids ids */
   203,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   203,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   203,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   203,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   203,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   204,  /* (51) ids ::= ID */
   204,  /* (52) ids ::= STRING */
   207,  /* (53) ifexists ::= IF EXISTS */
   207,  /* (54) ifexists ::= */
   212,  /* (55) ifnotexists ::= IF NOT EXISTS */
   212,  /* (56) ifnotexists ::= */
   203,  /* (57) cmd ::= CREATE DNODE ids */
   203,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   203,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   203,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   203,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   203,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   203,  /* (63) cmd ::= CREATE USER ids PASS ids */
   216,  /* (64) bufsize ::= */
   216,  /* (65) bufsize ::= BUFSIZE INTEGER */
   217,  /* (66) pps ::= */
   217,  /* (67) pps ::= PPS INTEGER */
   218,  /* (68) tseries ::= */
   218,  /* (69) tseries ::= TSERIES INTEGER */
   219,  /* (70) dbs ::= */
   219,  /* (71) dbs ::= DBS INTEGER */
   220,  /* (72) streams ::= */
   220,  /* (73) streams ::= STREAMS INTEGER */
   221,  /* (74) storage ::= */
   221,  /* (75) storage ::= STORAGE INTEGER */
   222,  /* (76) qtime ::= */
   222,  /* (77) qtime ::= QTIME INTEGER */
   223,  /* (78) users ::= */
   223,  /* (79) users ::= USERS INTEGER */
   224,  /* (80) conns ::= */
   224,  /* (81) conns ::= CONNS INTEGER */
   225,  /* (82) state ::= */
   225,  /* (83) state ::= STATE ids */
   210,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   226,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   226,  /* (86) intitemlist ::= intitem */
   227,  /* (87) intitem ::= INTEGER */
   228,  /* (88) keep ::= KEEP intitemlist */
   229,  /* (89) cache ::= CACHE INTEGER */
   230,  /* (90) replica ::= REPLICA INTEGER */
   231,  /* (91) quorum ::= QUORUM INTEGER */
   232,  /* (92) days ::= DAYS INTEGER */
   233,  /* (93) minrows ::= MINROWS INTEGER */
   234,  /* (94) maxrows ::= MAXROWS INTEGER */
   235,  /* (95) blocks ::= BLOCKS INTEGER */
   236,  /* (96) ctime ::= CTIME INTEGER */
   237,  /* (97) wal ::= WAL INTEGER */
   238,  /* (98) fsync ::= FSYNC INTEGER */
   239,  /* (99) comp ::= COMP INTEGER */
   240,  /* (100) prec ::= PRECISION STRING */
   241,  /* (101) update ::= UPDATE INTEGER */
   242,  /* (102) cachelast ::= CACHELAST INTEGER */
   243,  /* (103) partitions ::= PARTITIONS INTEGER */
   213,  /* (104) db_optr ::= */
   213,  /* (105) db_optr ::= db_optr cache */
   213,  /* (106) db_optr ::= db_optr replica */
   213,  /* (107) db_optr ::= db_optr quorum */
   213,  /* (108) db_optr ::= db_optr days */
   213,  /* (109) db_optr ::= db_optr minrows */
   213,  /* (110) db_optr ::= db_optr maxrows */
   213,  /* (111) db_optr ::= db_optr blocks */
   213,  /* (112) db_optr ::= db_optr ctime */
   213,  /* (113) db_optr ::= db_optr wal */
   213,  /* (114) db_optr ::= db_optr fsync */
   213,  /* (115) db_optr ::= db_optr comp */
   213,  /* (116) db_optr ::= db_optr prec */
   213,  /* (117) db_optr ::= db_optr keep */
   213,  /* (118) db_optr ::= db_optr update */
   213,  /* (119) db_optr ::= db_optr cachelast */
   214,  /* (120) topic_optr ::= db_optr */
   214,  /* (121) topic_optr ::= topic_optr partitions */
   208,  /* (122) alter_db_optr ::= */
   208,  /* (123) alter_db_optr ::= alter_db_optr replica */
   208,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   208,  /* (125) alter_db_optr ::= alter_db_optr keep */
   208,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   208,  /* (127) alter_db_optr ::= alter_db_optr comp */
   208,  /* (128) alter_db_optr ::= alter_db_optr update */
   208,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   209,  /* (130) alter_topic_optr ::= alter_db_optr */
   209,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   215,  /* (132) typename ::= ids */
   215,  /* (133) typename ::= ids LP signed RP */
   215,  /* (134) typename ::= ids UNSIGNED */
   244,  /* (135) signed ::= INTEGER */
   244,  /* (136) signed ::= PLUS INTEGER */
   244,  /* (137) signed ::= MINUS INTEGER */
   203,  /* (138) cmd ::= CREATE TABLE create_table_args */
   203,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   203,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   203,  /* (141) cmd ::= CREATE TABLE create_table_list */
   247,  /* (142) create_table_list ::= create_from_stable */
   247,  /* (143) create_table_list ::= create_table_list create_from_stable */
   245,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   246,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   248,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   248,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   251,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   251,  /* (149) tagNamelist ::= ids */
   245,  /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
   249,  /* (151) columnlist ::= columnlist COMMA column */
   249,  /* (152) columnlist ::= column */
   253,  /* (153) column ::= ids typename */
   250,  /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
   250,  /* (155) tagitemlist ::= tagitem */
   254,  /* (156) tagitem ::= INTEGER */
   254,  /* (157) tagitem ::= FLOAT */
   254,  /* (158) tagitem ::= STRING */
   254,  /* (159) tagitem ::= BOOL */
   254,  /* (160) tagitem ::= NULL */
   254,  /* (161) tagitem ::= NOW */
   254,  /* (162) tagitem ::= NOW PLUS VARIABLE */
   254,  /* (163) tagitem ::= NOW MINUS VARIABLE */
   254,  /* (164) tagitem ::= MINUS INTEGER */
   254,  /* (165) tagitem ::= MINUS FLOAT */
   254,  /* (166) tagitem ::= PLUS INTEGER */
   254,  /* (167) tagitem ::= PLUS FLOAT */
   252,  /* (168) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   252,  /* (169) select ::= LP select RP */
   269,  /* (170) union ::= select */
   269,  /* (171) union ::= union UNION ALL select */
   203,  /* (172) cmd ::= union */
   252,  /* (173) select ::= SELECT selcollist */
   270,  /* (174) sclp ::= selcollist COMMA */
   270,  /* (175) sclp ::= */
   255,  /* (176) selcollist ::= sclp distinct expr as */
   255,  /* (177) selcollist ::= sclp STAR */
   273,  /* (178) as ::= AS ids */
   273,  /* (179) as ::= ids */
   273,  /* (180) as ::= */
   271,  /* (181) distinct ::= DISTINCT */
   271,  /* (182) distinct ::= */
   256,  /* (183) from ::= FROM tablelist */
   256,  /* (184) from ::= FROM sub */
   275,  /* (185) sub ::= LP union RP */
   275,  /* (186) sub ::= LP union RP ids */
   275,  /* (187) sub ::= sub COMMA LP union RP ids */
   274,  /* (188) tablelist ::= ids cpxName */
   274,  /* (189) tablelist ::= ids cpxName ids */
   274,  /* (190) tablelist ::= tablelist COMMA ids cpxName */
   274,  /* (191) tablelist ::= tablelist COMMA ids cpxName ids */
   276,  /* (192) tmvar ::= VARIABLE */
   277,  /* (193) timestamp ::= INTEGER */
   277,  /* (194) timestamp ::= MINUS INTEGER */
   277,  /* (195) timestamp ::= PLUS INTEGER */
   277,  /* (196) timestamp ::= STRING */
   277,  /* (197) timestamp ::= NOW */
   277,  /* (198) timestamp ::= NOW PLUS VARIABLE */
   277,  /* (199) timestamp ::= NOW MINUS VARIABLE */
   258,  /* (200) range_option ::= */
   258,  /* (201) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   259,  /* (202) interval_option ::= intervalKey LP tmvar RP */
   259,  /* (203) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
   259,  /* (204) interval_option ::= */
   278,  /* (205) intervalKey ::= INTERVAL */
   278,  /* (206) intervalKey ::= EVERY */
   261,  /* (207) session_option ::= */
   261,  /* (208) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   262,  /* (209) windowstate_option ::= */
   262,  /* (210) windowstate_option ::= STATE_WINDOW LP ids RP */
   263,  /* (211) fill_opt ::= */
   263,  /* (212) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   263,  /* (213) fill_opt ::= FILL LP ID RP */
   260,  /* (214) sliding_opt ::= SLIDING LP tmvar RP */
   260,  /* (215) sliding_opt ::= */
   266,  /* (216) orderby_opt ::= */
   266,  /* (217) orderby_opt ::= ORDER BY sortlist */
   279,  /* (218) sortlist ::= sortlist COMMA item sortorder */
   279,  /* (219) sortlist ::= sortlist COMMA arrow sortorder */
   279,  /* (220) sortlist ::= item sortorder */
   279,  /* (221) sortlist ::= arrow sortorder */
   280,  /* (222) item ::= ID */
   280,  /* (223) item ::= ID DOT ID */
   281,  /* (224) sortorder ::= ASC */
   281,  /* (225) sortorder ::= DESC */
   281,  /* (226) sortorder ::= */
   264,  /* (227) groupby_opt ::= */
   264,  /* (228) groupby_opt ::= GROUP BY grouplist */
   283,  /* (229) grouplist ::= grouplist COMMA item */
   283,  /* (230) grouplist ::= grouplist COMMA arrow */
   283,  /* (231) grouplist ::= item */
   283,  /* (232) grouplist ::= arrow */
   265,  /* (233) having_opt ::= */
   265,  /* (234) having_opt ::= HAVING expr */
   268,  /* (235) limit_opt ::= */
   268,  /* (236) limit_opt ::= LIMIT signed */
   268,  /* (237) limit_opt ::= LIMIT signed OFFSET signed */
   268,  /* (238) limit_opt ::= LIMIT signed COMMA signed */
   267,  /* (239) slimit_opt ::= */
   267,  /* (240) slimit_opt ::= SLIMIT signed */
   267,  /* (241) slimit_opt ::= SLIMIT signed SOFFSET signed */
   267,  /* (242) slimit_opt ::= SLIMIT signed COMMA signed */
   257,  /* (243) where_opt ::= */
   257,  /* (244) where_opt ::= WHERE expr */
   272,  /* (245) expr ::= LP expr RP */
   272,  /* (246) expr ::= ID */
   272,  /* (247) expr ::= ID DOT ID */
   272,  /* (248) expr ::= ID DOT STAR */
   272,  /* (249) expr ::= INTEGER */
   272,  /* (250) expr ::= MINUS INTEGER */
   272,  /* (251) expr ::= PLUS INTEGER */
   272,  /* (252) expr ::= FLOAT */
   272,  /* (253) expr ::= MINUS FLOAT */
   272,  /* (254) expr ::= PLUS FLOAT */
   272,  /* (255) expr ::= STRING */
   272,  /* (256) expr ::= NOW */
   272,  /* (257) expr ::= TODAY */
   272,  /* (258) expr ::= VARIABLE */
   272,  /* (259) expr ::= PLUS VARIABLE */
   272,  /* (260) expr ::= MINUS VARIABLE */
   272,  /* (261) expr ::= BOOL */
   272,  /* (262) expr ::= NULL */
   272,  /* (263) expr ::= ID LP exprlist RP */
   272,  /* (264) expr ::= ID LP STAR RP */
   272,  /* (265) expr ::= ID LP expr AS typename RP */
   272,  /* (266) expr ::= expr IS NULL */
   272,  /* (267) expr ::= expr IS NOT NULL */
   272,  /* (268) expr ::= expr LT expr */
   272,  /* (269) expr ::= expr GT expr */
   272,  /* (270) expr ::= expr LE expr */
   272,  /* (271) expr ::= expr GE expr */
   272,  /* (272) expr ::= expr NE expr */
   272,  /* (273) expr ::= expr EQ expr */
   272,  /* (274) expr ::= expr BETWEEN expr AND expr */
   272,  /* (275) expr ::= expr AND expr */
   272,  /* (276) expr ::= expr OR expr */
   272,  /* (277) expr ::= expr PLUS expr */
   272,  /* (278) expr ::= expr MINUS expr */
   272,  /* (279) expr ::= expr STAR expr */
   272,  /* (280) expr ::= expr SLASH expr */
   272,  /* (281) expr ::= expr REM expr */
   272,  /* (282) expr ::= expr LIKE expr */
   272,  /* (283) expr ::= expr MATCH expr */
   272,  /* (284) expr ::= expr NMATCH expr */
   272,  /* (285) expr ::= ID CONTAINS STRING */
   272,  /* (286) expr ::= ID DOT ID CONTAINS STRING */
   282,  /* (287) arrow ::= ID ARROW STRING */
   282,  /* (288) arrow ::= ID DOT ID ARROW STRING */
   272,  /* (289) expr ::= arrow */
   272,  /* (290) expr ::= expr IN LP exprlist RP */
   211,  /* (291) exprlist ::= exprlist COMMA expritem */
   211,  /* (292) exprlist ::= expritem */
   284,  /* (293) expritem ::= expr */
   284,  /* (294) expritem ::= */
   203,  /* (295) cmd ::= RESET QUERY CACHE */
   203,  /* (296) cmd ::= SYNCDB ids REPLICA */
   203,  /* (297) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   203,  /* (298) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   203,  /* (299) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   203,  /* (300) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   203,  /* (301) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   203,  /* (302) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   203,  /* (303) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   203,  /* (304) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   203,  /* (305) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   203,  /* (306) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   203,  /* (307) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   203,  /* (308) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   203,  /* (309) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   203,  /* (310) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   203,  /* (311) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   203,  /* (312) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   203,  /* (313) cmd ::= KILL CONNECTION INTEGER */
   203,  /* (314) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   203,  /* (315) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE STRING */
   -3,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE STRING */
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
   -3,  /* (162) tagitem ::= NOW PLUS VARIABLE */
   -3,  /* (163) tagitem ::= NOW MINUS VARIABLE */
   -2,  /* (164) tagitem ::= MINUS INTEGER */
   -2,  /* (165) tagitem ::= MINUS FLOAT */
   -2,  /* (166) tagitem ::= PLUS INTEGER */
   -2,  /* (167) tagitem ::= PLUS FLOAT */
  -15,  /* (168) select ::= SELECT selcollist from where_opt range_option interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (169) select ::= LP select RP */
   -1,  /* (170) union ::= select */
   -4,  /* (171) union ::= union UNION ALL select */
   -1,  /* (172) cmd ::= union */
   -2,  /* (173) select ::= SELECT selcollist */
   -2,  /* (174) sclp ::= selcollist COMMA */
    0,  /* (175) sclp ::= */
   -4,  /* (176) selcollist ::= sclp distinct expr as */
   -2,  /* (177) selcollist ::= sclp STAR */
   -2,  /* (178) as ::= AS ids */
   -1,  /* (179) as ::= ids */
    0,  /* (180) as ::= */
   -1,  /* (181) distinct ::= DISTINCT */
    0,  /* (182) distinct ::= */
   -2,  /* (183) from ::= FROM tablelist */
   -2,  /* (184) from ::= FROM sub */
   -3,  /* (185) sub ::= LP union RP */
   -4,  /* (186) sub ::= LP union RP ids */
   -6,  /* (187) sub ::= sub COMMA LP union RP ids */
   -2,  /* (188) tablelist ::= ids cpxName */
   -3,  /* (189) tablelist ::= ids cpxName ids */
   -4,  /* (190) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (191) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (192) tmvar ::= VARIABLE */
   -1,  /* (193) timestamp ::= INTEGER */
   -2,  /* (194) timestamp ::= MINUS INTEGER */
   -2,  /* (195) timestamp ::= PLUS INTEGER */
   -1,  /* (196) timestamp ::= STRING */
   -1,  /* (197) timestamp ::= NOW */
   -3,  /* (198) timestamp ::= NOW PLUS VARIABLE */
   -3,  /* (199) timestamp ::= NOW MINUS VARIABLE */
    0,  /* (200) range_option ::= */
   -6,  /* (201) range_option ::= RANGE LP timestamp COMMA timestamp RP */
   -4,  /* (202) interval_option ::= intervalKey LP tmvar RP */
   -6,  /* (203) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
    0,  /* (204) interval_option ::= */
   -1,  /* (205) intervalKey ::= INTERVAL */
   -1,  /* (206) intervalKey ::= EVERY */
    0,  /* (207) session_option ::= */
   -7,  /* (208) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (209) windowstate_option ::= */
   -4,  /* (210) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (211) fill_opt ::= */
   -6,  /* (212) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (213) fill_opt ::= FILL LP ID RP */
   -4,  /* (214) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (215) sliding_opt ::= */
    0,  /* (216) orderby_opt ::= */
   -3,  /* (217) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (218) sortlist ::= sortlist COMMA item sortorder */
   -4,  /* (219) sortlist ::= sortlist COMMA arrow sortorder */
   -2,  /* (220) sortlist ::= item sortorder */
   -2,  /* (221) sortlist ::= arrow sortorder */
   -1,  /* (222) item ::= ID */
   -3,  /* (223) item ::= ID DOT ID */
   -1,  /* (224) sortorder ::= ASC */
   -1,  /* (225) sortorder ::= DESC */
    0,  /* (226) sortorder ::= */
    0,  /* (227) groupby_opt ::= */
   -3,  /* (228) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (229) grouplist ::= grouplist COMMA item */
   -3,  /* (230) grouplist ::= grouplist COMMA arrow */
   -1,  /* (231) grouplist ::= item */
   -1,  /* (232) grouplist ::= arrow */
    0,  /* (233) having_opt ::= */
   -2,  /* (234) having_opt ::= HAVING expr */
    0,  /* (235) limit_opt ::= */
   -2,  /* (236) limit_opt ::= LIMIT signed */
   -4,  /* (237) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (238) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (239) slimit_opt ::= */
   -2,  /* (240) slimit_opt ::= SLIMIT signed */
   -4,  /* (241) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (242) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (243) where_opt ::= */
   -2,  /* (244) where_opt ::= WHERE expr */
   -3,  /* (245) expr ::= LP expr RP */
   -1,  /* (246) expr ::= ID */
   -3,  /* (247) expr ::= ID DOT ID */
   -3,  /* (248) expr ::= ID DOT STAR */
   -1,  /* (249) expr ::= INTEGER */
   -2,  /* (250) expr ::= MINUS INTEGER */
   -2,  /* (251) expr ::= PLUS INTEGER */
   -1,  /* (252) expr ::= FLOAT */
   -2,  /* (253) expr ::= MINUS FLOAT */
   -2,  /* (254) expr ::= PLUS FLOAT */
   -1,  /* (255) expr ::= STRING */
   -1,  /* (256) expr ::= NOW */
   -1,  /* (257) expr ::= TODAY */
   -1,  /* (258) expr ::= VARIABLE */
   -2,  /* (259) expr ::= PLUS VARIABLE */
   -2,  /* (260) expr ::= MINUS VARIABLE */
   -1,  /* (261) expr ::= BOOL */
   -1,  /* (262) expr ::= NULL */
   -4,  /* (263) expr ::= ID LP exprlist RP */
   -4,  /* (264) expr ::= ID LP STAR RP */
   -6,  /* (265) expr ::= ID LP expr AS typename RP */
   -3,  /* (266) expr ::= expr IS NULL */
   -4,  /* (267) expr ::= expr IS NOT NULL */
   -3,  /* (268) expr ::= expr LT expr */
   -3,  /* (269) expr ::= expr GT expr */
   -3,  /* (270) expr ::= expr LE expr */
   -3,  /* (271) expr ::= expr GE expr */
   -3,  /* (272) expr ::= expr NE expr */
   -3,  /* (273) expr ::= expr EQ expr */
   -5,  /* (274) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (275) expr ::= expr AND expr */
   -3,  /* (276) expr ::= expr OR expr */
   -3,  /* (277) expr ::= expr PLUS expr */
   -3,  /* (278) expr ::= expr MINUS expr */
   -3,  /* (279) expr ::= expr STAR expr */
   -3,  /* (280) expr ::= expr SLASH expr */
   -3,  /* (281) expr ::= expr REM expr */
   -3,  /* (282) expr ::= expr LIKE expr */
   -3,  /* (283) expr ::= expr MATCH expr */
   -3,  /* (284) expr ::= expr NMATCH expr */
   -3,  /* (285) expr ::= ID CONTAINS STRING */
   -5,  /* (286) expr ::= ID DOT ID CONTAINS STRING */
   -3,  /* (287) arrow ::= ID ARROW STRING */
   -5,  /* (288) arrow ::= ID DOT ID ARROW STRING */
   -1,  /* (289) expr ::= arrow */
   -5,  /* (290) expr ::= expr IN LP exprlist RP */
   -3,  /* (291) exprlist ::= exprlist COMMA expritem */
   -1,  /* (292) exprlist ::= expritem */
   -1,  /* (293) expritem ::= expr */
    0,  /* (294) expritem ::= */
   -3,  /* (295) cmd ::= RESET QUERY CACHE */
   -3,  /* (296) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (297) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (298) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (299) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (300) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (301) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (302) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (303) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (304) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (305) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (306) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (307) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (308) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (309) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (310) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (311) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (312) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (313) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (314) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (315) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 257: /* expr ::= TODAY */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_TODAY); }
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 258: /* expr ::= VARIABLE */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 259: /* expr ::= PLUS VARIABLE */
      case 260: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==260);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy226 = yylhsminor.yy226;
        break;
      case 261: /* expr ::= BOOL */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 262: /* expr ::= NULL */
{ yylhsminor.yy226 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy226 = yylhsminor.yy226;
        break;
      case 263: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFunction(yymsp[-1].minor.yy231, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 264: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 265: /* expr ::= ID LP expr AS typename RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-5].minor.yy0); yylhsminor.yy226 = tSqlExprCreateFuncWithParams(pInfo, yymsp[-3].minor.yy226, &yymsp[-1].minor.yy103, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, yymsp[-5].minor.yy0.type); }
  yymsp[-5].minor.yy226 = yylhsminor.yy226;
        break;
      case 266: /* expr ::= expr IS NULL */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 267: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-3].minor.yy226, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy226 = yylhsminor.yy226;
        break;
      case 268: /* expr ::= expr LT expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LT);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 269: /* expr ::= expr GT expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_GT);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 270: /* expr ::= expr LE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_LE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 271: /* expr ::= expr GE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_GE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 272: /* expr ::= expr NE expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_NE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 273: /* expr ::= expr EQ expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_EQ);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 274: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy226); yylhsminor.yy226 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy226, yymsp[-2].minor.yy226, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy226, TK_LE), TK_AND);}
  yymsp[-4].minor.yy226 = yylhsminor.yy226;
        break;
      case 275: /* expr ::= expr AND expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_AND);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 276: /* expr ::= expr OR expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_OR); }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 277: /* expr ::= expr PLUS expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_PLUS);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 278: /* expr ::= expr MINUS expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_MINUS); }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 279: /* expr ::= expr STAR expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_STAR);  }
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 280: /* expr ::= expr SLASH expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_DIVIDE);}
  yymsp[-2].minor.yy226 = yylhsminor.yy226;
        break;
      case 281: /* expr ::= expr REM expr */
{yylhsminor.yy226 = tSqlExprCreate(yymsp[-2].minor.yy226, yymsp[0].minor.yy226, TK_REM);   }
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
