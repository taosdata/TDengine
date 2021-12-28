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
#include "astGenerator.h"
#include "tmsgtype.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "tvariant.h"
#include "parserInt.h"
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
#define YYNOCODE 273
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSqlNode* yy24;
  int yy60;
  SSubclause* yy129;
  SIntervalVal yy136;
  int64_t yy157;
  SCreateAcctInfo yy171;
  SSessionWindowVal yy251;
  SCreateDbInfo yy254;
  SWindowStateVal yy256;
  SField yy280;
  SRelationInfo* yy292;
  tSqlExpr* yy370;
  SArray* yy413;
  SCreateTableSql* yy438;
  SVariant yy461;
  SLimit yy503;
  int32_t yy516;
  SCreatedTableInfo yy544;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             365
#define YYNRULE              301
#define YYNTOKEN             191
#define YY_MAX_SHIFT         364
#define YY_MIN_SHIFTREDUCE   584
#define YY_MAX_SHIFTREDUCE   884
#define YY_ERROR_ACTION      885
#define YY_ACCEPT_ACTION     886
#define YY_NO_ACTION         887
#define YY_MIN_REDUCE        888
#define YY_MAX_REDUCE        1188
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
#define YY_ACTTAB_COUNT (779)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */  1073,  635,  155,  363,  230,  636,  671,   55,   56,  635,
 /*    10 */    59,   60, 1032,  636,  252,   49,   48,   47,  162,   58,
 /*    20 */   322,   63,   61,   64,   62,  236, 1050,  242,  206,   54,
 /*    30 */    53, 1038,  635,   52,   51,   50,  636,   55,   56, 1164,
 /*    40 */    59,   60,  936, 1063,  252,   49,   48,   47,  188,   58,
 /*    50 */   322,   63,   61,   64,   62, 1010,  243, 1008, 1009,   54,
 /*    60 */    53,  233, 1011,   52,   51,   50, 1012, 1070, 1013, 1014,
 /*    70 */   280,  279,  947,   55,   56,  246,   59,   60,  188, 1038,
 /*    80 */   252,   49,   48,   47,   81,   58,  322,   63,   61,   64,
 /*    90 */    62,  320, 1112,   99,  292,   54,   53,  352,  635,   52,
 /*   100 */    51,   50,  636,   55,   57,  261,   59,   60,  318,  821,
 /*   110 */   252,   49,   48,   47,  176,   58,  322,   63,   61,   64,
 /*   120 */    62,   42,  249,  358,  357,   54,   53, 1026,  356,   52,
 /*   130 */    51,   50,  355,   87,  354,  353,  886,  364,  585,  586,
 /*   140 */   587,  588,  589,  590,  591,  592,  593,  594,  595,  596,
 /*   150 */   597,  598,  153,   56,  231,   59,   60,  767, 1063,  252,
 /*   160 */    49,   48,   47,  171,   58,  322,   63,   61,   64,   62,
 /*   170 */   162,   43, 1024,   86,   54,   53,  234,   21,   52,   51,
 /*   180 */    50,  282,  206, 1063,  635,   59,   60,  203,  636,  252,
 /*   190 */    49,   48,   47, 1165,   58,  322,   63,   61,   64,   62,
 /*   200 */   162,  274,  827,  830,   54,   53,  786,  787,   52,   51,
 /*   210 */    50,   36,   42,  316,  358,  357,  315,  314,  313,  356,
 /*   220 */   312,  311,  310,  355,  309,  354,  353, 1004,  992,  993,
 /*   230 */   994,  995,  996,  997,  998,  999, 1000, 1001, 1002, 1003,
 /*   240 */  1005, 1006,  294,  771,   92,   22,   63,   61,   64,   62,
 /*   250 */   826,  829,  318,  232,   54,   53,  204, 1035,   52,   51,
 /*   260 */    50,   27,  215,   36,  251,  836,  825,  828,  831,  216,
 /*   270 */   750,  747,  748,  749, 1111,  137,  136,  135,  217,  162,
 /*   280 */   266,  255,  327,   87,  251,  836,  825,  828,  831,  270,
 /*   290 */   269,   96,  228,  229,  261,  261,  323,  257,  258,  742,
 /*   300 */   739,  740,  741,  177, 1036,  240,    3,   39,  178, 1035,
 /*   310 */   260,  209,  228,  229,  105,   77,  101,  108,  250,  248,
 /*   320 */   834,   43, 1021, 1022,   33, 1025,  244,  245,  197,  195,
 /*   330 */   193,   52,   51,   50,  305,  192,  141,  140,  139,  138,
 /*   340 */     4,   65,  253,  273,   80,   79,  122,  116,  126,  152,
 /*   350 */   150,  149,  224,   93,   36,  131,  134,  125,  256,   36,
 /*   360 */   254,   65,  330,  329,  128,   54,   53,  714,  835,   52,
 /*   370 */    51,   50,   87,   36,   36,   36, 1023,  837,  832,  206,
 /*   380 */    36,   36,  751,  752,  833,   36,   36,  262,   36,  259,
 /*   390 */  1165,  337,  336,  342,  341,  803,  241,  837,  832,  764,
 /*   400 */  1035,  331,  206,   12,  833, 1035,   84,   85,  937,   95,
 /*   410 */    43,  743,  744, 1165,  188,  332,  333,  334,  324, 1035,
 /*   420 */  1035, 1035,  338,  339,    7,  124, 1035, 1035,  340,   94,
 /*   430 */   344, 1034, 1035,  275, 1035,  362,  361,  146,   98,  352,
 /*   440 */   359,  974,  783,   82,   70,   70,  793,  794,   71,   37,
 /*   450 */   724,   74,  297,  802,  726,  299,  737,  738,  157,  735,
 /*   460 */   736,  725,   66,   24,   32,  823,   37,  859,   37,  838,
 /*   470 */    67,   97,  634,   14,  115,   13,  114,   67,   78,   16,
 /*   480 */    18,   15,   17,   23,  121,   23,  120,  210,   23,   72,
 /*   490 */    75,  211,  755,  756,  753,  754, 1159,  824,  300,   20,
 /*   500 */  1158,   19,  133,  132, 1157,  226, 1037, 1049, 1065,  227,
 /*   510 */   207,  713,  208,  212,  205,  213,  214,  219,  220,  221,
 /*   520 */   218,  202, 1184,  840, 1176,   44, 1122, 1121,  321,  271,
 /*   530 */   238, 1118, 1117,  239,  343,  154, 1104, 1072, 1083, 1080,
 /*   540 */  1081, 1085,  156,  161, 1064,  277,  288, 1103, 1033,  173,
 /*   550 */   151,  281,  172, 1031,  174,  175,  951,  782,  306,  302,
 /*   560 */   303,  170,  164,  304,  307,  163,  308, 1061,  295,  291,
 /*   570 */   165,  200,   40,  319,  946,  945,  235,  328, 1183,  112,
 /*   580 */   283, 1182,  285, 1179,   76,  179,  335,   73, 1175,  118,
 /*   590 */  1174, 1171,  180,  971,   46,   41,   38,  201,  934,  127,
 /*   600 */   932,  129,  130,  293,  930,  929,  263,  190,  191,  926,
 /*   610 */   925,  924,  923,  922,  921,  920,  194,  196,  917,  915,
 /*   620 */   913,  911,  289,  198,  908,  199,  904,  287,  284,  276,
 /*   630 */    83,   88,   45,  286, 1105,  123,  345,  346,  347,  348,
 /*   640 */   349,  225,  350,  247,  301,  351,  360,  884,  264,  265,
 /*   650 */   883,  222,  267,  950,  949,  106,  223,  268,  882,  865,
 /*   660 */   864,  272,   70,    8,  928,  296,  927,  758,  183,  182,
 /*   670 */   972,  181,  142,  184,  185,  187,  186,  143,  919,  918,
 /*   680 */   973,  144,  145,  910,  909,   28,  278,   89,   31,  784,
 /*   690 */     2,  166,  167,  158,  168,  169,  795,  159,    1,  789,
 /*   700 */   160,   90,  237,  791,   91,  290,   29,    9,   30,   10,
 /*   710 */    11,   25,  298,   26,   98,  100,   34,  103,  102,  649,
 /*   720 */   684,   35,  104,  682,  681,  680,  678,  677,  676,  673,
 /*   730 */   639,  107,  109,  325,  110,  839,  317,    5,    6,  841,
 /*   740 */   326,   68,  111,  113,   69,  716,  117,  119,   37,  715,
 /*   750 */   712,  665,  663,  655,  661,  657,  659,  653,  651,  686,
 /*   760 */   685,  683,  679,  675,  674,  189,  602,  637,  888,  887,
 /*   770 */   887,  887,  887,  887,  887,  887,  887,  147,  148,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   194,    1,  194,  194,  195,    5,    3,    7,    8,    1,
 /*    10 */    10,   11,  194,    5,   14,   15,   16,   17,  194,   19,
 /*    20 */    20,   21,   22,   23,   24,  242,  243,  237,  260,   29,
 /*    30 */    30,  241,    1,   33,   34,   35,    5,    7,    8,  271,
 /*    40 */    10,   11,  199,  239,   14,   15,   16,   17,  205,   19,
 /*    50 */    20,   21,   22,   23,   24,  216,  238,  218,  219,   29,
 /*    60 */    30,  257,  223,   33,   34,   35,  227,  261,  229,  230,
 /*    70 */   262,  263,  199,    7,    8,  237,   10,   11,  205,  241,
 /*    80 */    14,   15,   16,   17,   84,   19,   20,   21,   22,   23,
 /*    90 */    24,   83,  268,  201,  270,   29,   30,   88,    1,   33,
 /*   100 */    34,   35,    5,    7,    8,  194,   10,   11,   80,   79,
 /*   110 */    14,   15,   16,   17,  203,   19,   20,   21,   22,   23,
 /*   120 */    24,   96,  200,   98,   99,   29,   30,  235,  103,   33,
 /*   130 */    34,   35,  107,   78,  109,  110,  192,  193,   41,   42,
 /*   140 */    43,   44,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   150 */    53,   54,   55,    8,   57,   10,   11,   33,  239,   14,
 /*   160 */    15,   16,   17,  247,   19,   20,   21,   22,   23,   24,
 /*   170 */   194,  116,    0,  118,   29,   30,  257,  260,   33,   34,
 /*   180 */    35,  265,  260,  239,    1,   10,   11,  260,    5,   14,
 /*   190 */    15,   16,   17,  271,   19,   20,   21,   22,   23,   24,
 /*   200 */   194,  257,    3,    4,   29,   30,  122,  123,   33,   34,
 /*   210 */    35,  194,   96,   97,   98,   99,  100,  101,  102,  103,
 /*   220 */   104,  105,  106,  107,  108,  109,  110,  216,  217,  218,
 /*   230 */   219,  220,  221,  222,  223,  224,  225,  226,  227,  228,
 /*   240 */   229,  230,  266,  119,  268,   40,   21,   22,   23,   24,
 /*   250 */     3,    4,   80,  236,   29,   30,  260,  240,   33,   34,
 /*   260 */    35,   78,   57,  194,    1,    2,    3,    4,    5,   64,
 /*   270 */     2,    3,    4,    5,  268,   70,   71,   72,   73,  194,
 /*   280 */   139,   64,   77,   78,    1,    2,    3,    4,    5,  148,
 /*   290 */   149,  201,   29,   30,  194,  194,   33,   29,   30,    2,
 /*   300 */     3,    4,    5,  203,  203,  236,   58,   59,   60,  240,
 /*   310 */    64,  260,   29,   30,   66,   67,   68,   69,   56,  200,
 /*   320 */   121,  116,  232,  233,  234,  235,   29,   30,   58,   59,
 /*   330 */    60,   33,   34,   35,   86,   65,   66,   67,   68,   69,
 /*   340 */    78,   78,  200,  138,  201,  140,   58,   59,   60,   58,
 /*   350 */    59,   60,  147,  268,  194,   67,   68,   69,  141,  194,
 /*   360 */   143,   78,  145,  146,   76,   29,   30,    3,  121,   33,
 /*   370 */    34,   35,   78,  194,  194,  194,  233,  114,  115,  260,
 /*   380 */   194,  194,  114,  115,  121,  194,  194,  141,  194,  143,
 /*   390 */   271,  145,  146,   29,   30,   72,  236,  114,  115,   95,
 /*   400 */   240,  236,  260,   78,  121,  240,   79,   79,  199,   84,
 /*   410 */   116,  114,  115,  271,  205,  236,  236,  236,    9,  240,
 /*   420 */   240,  240,  236,  236,  120,   74,  240,  240,  236,  244,
 /*   430 */   236,  240,  240,   79,  240,   61,   62,   63,  113,   88,
 /*   440 */   214,  215,   79,  258,  117,  117,   79,   79,   95,   95,
 /*   450 */    79,   95,   79,  130,   79,   79,    3,    4,   95,    3,
 /*   460 */     4,   79,   95,   95,   78,    1,   95,   79,   95,   79,
 /*   470 */    95,   95,   79,  142,  142,  144,  144,   95,   78,  142,
 /*   480 */   142,  144,  144,   95,  142,   95,  144,  260,   95,  136,
 /*   490 */   134,  260,    3,    4,    3,    4,  260,   33,  112,  142,
 /*   500 */   260,  144,   74,   75,  260,  260,  241,  243,  239,  260,
 /*   510 */   260,  111,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   520 */   260,  260,  243,  114,  243,  259,  231,  231,  194,  194,
 /*   530 */   231,  231,  231,  231,  231,  194,  269,  194,  194,  194,
 /*   540 */   194,  194,  194,  194,  239,  239,  194,  269,  239,  194,
 /*   550 */    56,  264,  245,  194,  194,  194,  194,  121,   87,  194,
 /*   560 */   194,  248,  254,  194,  194,  255,  194,  256,  128,  126,
 /*   570 */   253,  194,  194,  194,  194,  194,  264,  194,  194,  194,
 /*   580 */   264,  194,  264,  194,  133,  194,  194,  135,  194,  194,
 /*   590 */   194,  194,  194,  194,  132,  194,  194,  194,  194,  194,
 /*   600 */   194,  194,  194,  131,  194,  194,  194,  194,  194,  194,
 /*   610 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   620 */   194,  194,  125,  194,  194,  194,  194,  124,  127,  196,
 /*   630 */   196,  196,  137,  196,  196,   94,   93,   47,   90,   92,
 /*   640 */    51,  196,   91,  196,  196,   89,   80,    3,  150,    3,
 /*   650 */     3,  196,  150,  204,  204,  201,  196,    3,    3,   98,
 /*   660 */    97,  139,  117,   78,  196,  112,  196,   79,  207,  211,
 /*   670 */   213,  212,  197,  210,  208,  206,  209,  197,  196,  196,
 /*   680 */   215,  197,  197,  196,  196,   78,   95,   95,  246,   79,
 /*   690 */   198,  252,  251,   78,  250,  249,   79,   78,  202,   79,
 /*   700 */    95,   78,    1,   79,   78,   78,   95,  129,   95,  129,
 /*   710 */    78,   78,  112,   78,  113,   74,   85,   66,   84,    3,
 /*   720 */     5,   85,   84,    3,    3,    3,    3,    3,    3,    3,
 /*   730 */    81,   74,   82,   20,   82,   79,    9,   78,   78,  114,
 /*   740 */    55,   10,  144,  144,   10,    3,  144,  144,   95,    3,
 /*   750 */    79,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   760 */     3,    3,    3,    3,    3,   95,   56,   81,    0,  272,
 /*   770 */   272,  272,  272,  272,  272,  272,  272,   15,   15,  272,
 /*   780 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   790 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   800 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   810 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   820 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   830 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   840 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   850 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   860 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   870 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   880 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   890 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   900 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   910 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   920 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   930 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   940 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   950 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   960 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
};
#define YY_SHIFT_COUNT    (364)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (768)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   205,  116,   25,   28,  263,  283,  283,  183,   31,   31,
 /*    10 */    31,   31,   31,   31,   31,   31,   31,   31,   31,   31,
 /*    20 */    31,    0,   97,  283,  268,  297,  297,  294,  294,   31,
 /*    30 */    31,   84,   31,  172,   31,   31,   31,   31,  351,   28,
 /*    40 */     9,    9,    3,  779,  283,  283,  283,  283,  283,  283,
 /*    50 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*    60 */   283,  283,  283,  283,  283,  283,  268,  297,  268,  268,
 /*    70 */    55,  364,  364,  364,  364,  364,  364,    8,  364,   31,
 /*    80 */    31,   31,  124,   31,   31,   31,  294,  294,   31,   31,
 /*    90 */    31,   31,  323,  323,  304,  294,   31,   31,   31,   31,
 /*   100 */    31,   31,   31,   31,   31,   31,   31,   31,   31,   31,
 /*   110 */    31,   31,   31,   31,   31,   31,   31,   31,   31,   31,
 /*   120 */    31,   31,   31,   31,   31,   31,   31,   31,   31,   31,
 /*   130 */    31,   31,   31,   31,   31,   31,   31,   31,   31,   31,
 /*   140 */    31,   31,   31,   31,   31,   31,   31,   31,   31,   31,
 /*   150 */    31,   31,   31,   31,  494,  494,  494,  436,  436,  436,
 /*   160 */   436,  494,  494,  451,  452,  440,  462,  472,  443,  497,
 /*   170 */   503,  501,  495,  494,  494,  494,  471,  471,   28,  494,
 /*   180 */   494,  541,  543,  590,  548,  547,  589,  551,  556,    3,
 /*   190 */   494,  494,  566,  566,  494,  566,  494,  566,  494,  494,
 /*   200 */   779,  779,   30,   66,   66,   96,   66,  145,  175,  225,
 /*   210 */   225,  225,  225,  225,  225,  248,  270,  288,  336,  336,
 /*   220 */   336,  336,  217,  246,  141,  325,  298,  298,  199,  247,
 /*   230 */   374,  291,  354,  327,  328,  363,  367,  368,  353,  356,
 /*   240 */   371,  373,  375,  376,  453,  456,  382,  386,  388,  390,
 /*   250 */   464,  262,  409,  393,  331,  332,  337,  489,  491,  338,
 /*   260 */   342,  400,  357,  428,  644,  498,  646,  647,  502,  654,
 /*   270 */   655,  561,  563,  522,  545,  553,  585,  588,  607,  591,
 /*   280 */   592,  610,  615,  617,  619,  620,  605,  623,  624,  626,
 /*   290 */   701,  627,  611,  578,  613,  580,  632,  553,  633,  600,
 /*   300 */   635,  601,  641,  631,  634,  651,  716,  636,  638,  715,
 /*   310 */   720,  721,  722,  723,  724,  725,  726,  649,  727,  657,
 /*   320 */   650,  652,  659,  656,  625,  660,  713,  685,  731,  598,
 /*   330 */   599,  653,  653,  653,  653,  734,  602,  603,  653,  653,
 /*   340 */   653,  742,  746,  671,  653,  748,  749,  750,  751,  752,
 /*   350 */   753,  754,  755,  756,  757,  758,  759,  760,  761,  670,
 /*   360 */   686,  762,  763,  710,  768,
};
#define YY_REDUCE_COUNT (201)
#define YY_REDUCE_MIN   (-232)
#define YY_REDUCE_MAX   (496)
static const short yy_reduce_ofst[] = {
 /*     0 */   -56,   11, -161,   90,  -78,  119,  142, -192,   17, -176,
 /*    10 */   -24,   69,  160,  165,  179,  180,  181,  186,  187,  192,
 /*    20 */   194, -194, -191, -232, -217, -210, -162, -196,  -81,    6,
 /*    30 */    85,  -84, -182, -108,  -89,  100,  101,  191, -157,  143,
 /*    40 */  -127,  209,  226,  185,  -83,  -73,   -4,   51,  227,  231,
 /*    50 */   236,  240,  244,  245,  249,  250,  252,  253,  254,  255,
 /*    60 */   256,  257,  258,  259,  260,  261,  264,  265,  279,  281,
 /*    70 */   269,  295,  296,  299,  300,  301,  302,  334,  303,  335,
 /*    80 */   341,  343,  266,  344,  345,  346,  305,  306,  347,  348,
 /*    90 */   349,  352,  267,  278,  307,  309,  355,  359,  360,  361,
 /*   100 */   362,  365,  366,  369,  370,  372,  377,  378,  379,  380,
 /*   110 */   381,  383,  384,  385,  387,  389,  391,  392,  394,  395,
 /*   120 */   396,  397,  398,  399,  401,  402,  403,  404,  405,  406,
 /*   130 */   407,  408,  410,  411,  412,  413,  414,  415,  416,  417,
 /*   140 */   418,  419,  420,  421,  422,  423,  424,  425,  426,  427,
 /*   150 */   429,  430,  431,  432,  433,  434,  435,  287,  312,  316,
 /*   160 */   318,  437,  438,  311,  310,  308,  317,  439,  441,  444,
 /*   170 */   446,  313,  442,  445,  447,  448,  449,  450,  454,  455,
 /*   180 */   460,  457,  459,  458,  461,  463,  466,  467,  469,  465,
 /*   190 */   468,  470,  475,  480,  482,  484,  483,  485,  487,  488,
 /*   200 */   496,  492,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   885,  948,  935,  944, 1167, 1167, 1167,  885,  885,  885,
 /*    10 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    20 */   885, 1074,  905, 1167,  885,  885,  885,  885,  885,  885,
 /*    30 */   885, 1089,  885,  944,  885,  885,  885,  885,  954,  944,
 /*    40 */   954,  954,  885, 1069,  885,  885,  885,  885,  885,  885,
 /*    50 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    60 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    70 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*    80 */   885,  885, 1076, 1082, 1079,  885,  885,  885, 1084,  885,
 /*    90 */   885,  885, 1108, 1108, 1067,  885,  885,  885,  885,  885,
 /*   100 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   110 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   120 */   885,  885,  885,  885,  885,  885,  885,  933,  885,  931,
 /*   130 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   140 */   885,  885,  885,  885,  885,  885,  916,  885,  885,  885,
 /*   150 */   885,  885,  885,  903,  907,  907,  907,  885,  885,  885,
 /*   160 */   885,  907,  907, 1115, 1119, 1101, 1113, 1109, 1096, 1094,
 /*   170 */  1092, 1100, 1123,  907,  907,  907,  952,  952,  944,  907,
 /*   180 */   907,  970,  968,  966,  958,  964,  960,  962,  956,  885,
 /*   190 */   907,  907,  942,  942,  907,  942,  907,  942,  907,  907,
 /*   200 */   991, 1007,  885, 1124, 1114,  885, 1166, 1154, 1153, 1162,
 /*   210 */  1161, 1160, 1152, 1151, 1150,  885,  885,  885, 1146, 1149,
 /*   220 */  1148, 1147,  885,  885,  885,  885, 1156, 1155,  885,  885,
 /*   230 */   885,  885,  885,  885,  885,  885,  885,  885, 1120, 1116,
 /*   240 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   250 */   885, 1126,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   260 */   885, 1015,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   270 */   885,  885,  885,  885, 1066,  885,  885,  885,  885, 1078,
 /*   280 */  1077,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   290 */   885,  885, 1110,  885, 1102,  885,  885, 1027,  885,  885,
 /*   300 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   310 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   320 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  885,
 /*   330 */   885, 1185, 1180, 1181, 1178,  885,  885,  885, 1177, 1172,
 /*   340 */  1173,  885,  885,  885, 1170,  885,  885,  885,  885,  885,
 /*   350 */   885,  885,  885,  885,  885,  885,  885,  885,  885,  976,
 /*   360 */   885,  914,  912,  885,  885,
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
    1,  /*    INTEGER => ID */
    1,  /*      FLOAT => ID */
    1,  /*     STRING => ID */
    1,  /*  TIMESTAMP => ID */
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
    0,  /*     CONCAT => nothing */
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
    0,  /*       PORT => nothing */
    1,  /*    IPTOKEN => ID */
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
  /*    3 */ "INTEGER",
  /*    4 */ "FLOAT",
  /*    5 */ "STRING",
  /*    6 */ "TIMESTAMP",
  /*    7 */ "OR",
  /*    8 */ "AND",
  /*    9 */ "NOT",
  /*   10 */ "EQ",
  /*   11 */ "NE",
  /*   12 */ "ISNULL",
  /*   13 */ "NOTNULL",
  /*   14 */ "IS",
  /*   15 */ "LIKE",
  /*   16 */ "MATCH",
  /*   17 */ "NMATCH",
  /*   18 */ "GLOB",
  /*   19 */ "BETWEEN",
  /*   20 */ "IN",
  /*   21 */ "GT",
  /*   22 */ "GE",
  /*   23 */ "LT",
  /*   24 */ "LE",
  /*   25 */ "BITAND",
  /*   26 */ "BITOR",
  /*   27 */ "LSHIFT",
  /*   28 */ "RSHIFT",
  /*   29 */ "PLUS",
  /*   30 */ "MINUS",
  /*   31 */ "DIVIDE",
  /*   32 */ "TIMES",
  /*   33 */ "STAR",
  /*   34 */ "SLASH",
  /*   35 */ "REM",
  /*   36 */ "CONCAT",
  /*   37 */ "UMINUS",
  /*   38 */ "UPLUS",
  /*   39 */ "BITNOT",
  /*   40 */ "SHOW",
  /*   41 */ "DATABASES",
  /*   42 */ "TOPICS",
  /*   43 */ "FUNCTIONS",
  /*   44 */ "MNODES",
  /*   45 */ "DNODES",
  /*   46 */ "ACCOUNTS",
  /*   47 */ "USERS",
  /*   48 */ "MODULES",
  /*   49 */ "QUERIES",
  /*   50 */ "CONNECTIONS",
  /*   51 */ "STREAMS",
  /*   52 */ "VARIABLES",
  /*   53 */ "SCORES",
  /*   54 */ "GRANTS",
  /*   55 */ "VNODES",
  /*   56 */ "DOT",
  /*   57 */ "CREATE",
  /*   58 */ "TABLE",
  /*   59 */ "STABLE",
  /*   60 */ "DATABASE",
  /*   61 */ "TABLES",
  /*   62 */ "STABLES",
  /*   63 */ "VGROUPS",
  /*   64 */ "DROP",
  /*   65 */ "TOPIC",
  /*   66 */ "FUNCTION",
  /*   67 */ "DNODE",
  /*   68 */ "USER",
  /*   69 */ "ACCOUNT",
  /*   70 */ "USE",
  /*   71 */ "DESCRIBE",
  /*   72 */ "DESC",
  /*   73 */ "ALTER",
  /*   74 */ "PASS",
  /*   75 */ "PRIVILEGE",
  /*   76 */ "LOCAL",
  /*   77 */ "COMPACT",
  /*   78 */ "LP",
  /*   79 */ "RP",
  /*   80 */ "IF",
  /*   81 */ "EXISTS",
  /*   82 */ "PORT",
  /*   83 */ "IPTOKEN",
  /*   84 */ "AS",
  /*   85 */ "OUTPUTTYPE",
  /*   86 */ "AGGREGATE",
  /*   87 */ "BUFSIZE",
  /*   88 */ "PPS",
  /*   89 */ "TSERIES",
  /*   90 */ "DBS",
  /*   91 */ "STORAGE",
  /*   92 */ "QTIME",
  /*   93 */ "CONNS",
  /*   94 */ "STATE",
  /*   95 */ "COMMA",
  /*   96 */ "KEEP",
  /*   97 */ "CACHE",
  /*   98 */ "REPLICA",
  /*   99 */ "QUORUM",
  /*  100 */ "DAYS",
  /*  101 */ "MINROWS",
  /*  102 */ "MAXROWS",
  /*  103 */ "BLOCKS",
  /*  104 */ "CTIME",
  /*  105 */ "WAL",
  /*  106 */ "FSYNC",
  /*  107 */ "COMP",
  /*  108 */ "PRECISION",
  /*  109 */ "UPDATE",
  /*  110 */ "CACHELAST",
  /*  111 */ "UNSIGNED",
  /*  112 */ "TAGS",
  /*  113 */ "USING",
  /*  114 */ "NULL",
  /*  115 */ "NOW",
  /*  116 */ "SELECT",
  /*  117 */ "UNION",
  /*  118 */ "ALL",
  /*  119 */ "DISTINCT",
  /*  120 */ "FROM",
  /*  121 */ "VARIABLE",
  /*  122 */ "INTERVAL",
  /*  123 */ "EVERY",
  /*  124 */ "SESSION",
  /*  125 */ "STATE_WINDOW",
  /*  126 */ "FILL",
  /*  127 */ "SLIDING",
  /*  128 */ "ORDER",
  /*  129 */ "BY",
  /*  130 */ "ASC",
  /*  131 */ "GROUP",
  /*  132 */ "HAVING",
  /*  133 */ "LIMIT",
  /*  134 */ "OFFSET",
  /*  135 */ "SLIMIT",
  /*  136 */ "SOFFSET",
  /*  137 */ "WHERE",
  /*  138 */ "RESET",
  /*  139 */ "QUERY",
  /*  140 */ "SYNCDB",
  /*  141 */ "ADD",
  /*  142 */ "COLUMN",
  /*  143 */ "MODIFY",
  /*  144 */ "TAG",
  /*  145 */ "CHANGE",
  /*  146 */ "SET",
  /*  147 */ "KILL",
  /*  148 */ "CONNECTION",
  /*  149 */ "STREAM",
  /*  150 */ "COLON",
  /*  151 */ "ABORT",
  /*  152 */ "AFTER",
  /*  153 */ "ATTACH",
  /*  154 */ "BEFORE",
  /*  155 */ "BEGIN",
  /*  156 */ "CASCADE",
  /*  157 */ "CLUSTER",
  /*  158 */ "CONFLICT",
  /*  159 */ "COPY",
  /*  160 */ "DEFERRED",
  /*  161 */ "DELIMITERS",
  /*  162 */ "DETACH",
  /*  163 */ "EACH",
  /*  164 */ "END",
  /*  165 */ "EXPLAIN",
  /*  166 */ "FAIL",
  /*  167 */ "FOR",
  /*  168 */ "IGNORE",
  /*  169 */ "IMMEDIATE",
  /*  170 */ "INITIALLY",
  /*  171 */ "INSTEAD",
  /*  172 */ "KEY",
  /*  173 */ "OF",
  /*  174 */ "RAISE",
  /*  175 */ "REPLACE",
  /*  176 */ "RESTRICT",
  /*  177 */ "ROW",
  /*  178 */ "STATEMENT",
  /*  179 */ "TRIGGER",
  /*  180 */ "VIEW",
  /*  181 */ "SEMI",
  /*  182 */ "NONE",
  /*  183 */ "PREV",
  /*  184 */ "LINEAR",
  /*  185 */ "IMPORT",
  /*  186 */ "TBNAME",
  /*  187 */ "JOIN",
  /*  188 */ "INSERT",
  /*  189 */ "INTO",
  /*  190 */ "VALUES",
  /*  191 */ "error",
  /*  192 */ "program",
  /*  193 */ "cmd",
  /*  194 */ "ids",
  /*  195 */ "dbPrefix",
  /*  196 */ "cpxName",
  /*  197 */ "ifexists",
  /*  198 */ "alter_db_optr",
  /*  199 */ "acct_optr",
  /*  200 */ "exprlist",
  /*  201 */ "ifnotexists",
  /*  202 */ "db_optr",
  /*  203 */ "typename",
  /*  204 */ "bufsize",
  /*  205 */ "pps",
  /*  206 */ "tseries",
  /*  207 */ "dbs",
  /*  208 */ "streams",
  /*  209 */ "storage",
  /*  210 */ "qtime",
  /*  211 */ "users",
  /*  212 */ "conns",
  /*  213 */ "state",
  /*  214 */ "intitemlist",
  /*  215 */ "intitem",
  /*  216 */ "keep",
  /*  217 */ "cache",
  /*  218 */ "replica",
  /*  219 */ "quorum",
  /*  220 */ "days",
  /*  221 */ "minrows",
  /*  222 */ "maxrows",
  /*  223 */ "blocks",
  /*  224 */ "ctime",
  /*  225 */ "wal",
  /*  226 */ "fsync",
  /*  227 */ "comp",
  /*  228 */ "prec",
  /*  229 */ "update",
  /*  230 */ "cachelast",
  /*  231 */ "signed",
  /*  232 */ "create_table_args",
  /*  233 */ "create_stable_args",
  /*  234 */ "create_table_list",
  /*  235 */ "create_from_stable",
  /*  236 */ "columnlist",
  /*  237 */ "tagitemlist1",
  /*  238 */ "tagNamelist",
  /*  239 */ "select",
  /*  240 */ "column",
  /*  241 */ "tagitem1",
  /*  242 */ "tagitemlist",
  /*  243 */ "tagitem",
  /*  244 */ "selcollist",
  /*  245 */ "from",
  /*  246 */ "where_opt",
  /*  247 */ "interval_option",
  /*  248 */ "sliding_opt",
  /*  249 */ "session_option",
  /*  250 */ "windowstate_option",
  /*  251 */ "fill_opt",
  /*  252 */ "groupby_opt",
  /*  253 */ "having_opt",
  /*  254 */ "orderby_opt",
  /*  255 */ "slimit_opt",
  /*  256 */ "limit_opt",
  /*  257 */ "union",
  /*  258 */ "sclp",
  /*  259 */ "distinct",
  /*  260 */ "expr",
  /*  261 */ "as",
  /*  262 */ "tablelist",
  /*  263 */ "sub",
  /*  264 */ "tmvar",
  /*  265 */ "intervalKey",
  /*  266 */ "sortlist",
  /*  267 */ "sortitem",
  /*  268 */ "item",
  /*  269 */ "sortorder",
  /*  270 */ "grouplist",
  /*  271 */ "expritem",
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
 /*  29 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  30 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  32 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  33 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  34 */ "cmd ::= DROP FUNCTION ids",
 /*  35 */ "cmd ::= DROP DNODE ids",
 /*  36 */ "cmd ::= DROP USER ids",
 /*  37 */ "cmd ::= DROP ACCOUNT ids",
 /*  38 */ "cmd ::= USE ids",
 /*  39 */ "cmd ::= DESCRIBE ids cpxName",
 /*  40 */ "cmd ::= DESC ids cpxName",
 /*  41 */ "cmd ::= ALTER USER ids PASS ids",
 /*  42 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids",
 /*  44 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids",
 /*  46 */ "cmd ::= ALTER LOCAL ids ids",
 /*  47 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  50 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  51 */ "ids ::= ID",
 /*  52 */ "ids ::= STRING",
 /*  53 */ "ifexists ::= IF EXISTS",
 /*  54 */ "ifexists ::=",
 /*  55 */ "ifnotexists ::= IF NOT EXISTS",
 /*  56 */ "ifnotexists ::=",
 /*  57 */ "cmd ::= CREATE DNODE ids PORT ids",
 /*  58 */ "cmd ::= CREATE DNODE IPTOKEN PORT ids",
 /*  59 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  60 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
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
 /* 103 */ "db_optr ::=",
 /* 104 */ "db_optr ::= db_optr cache",
 /* 105 */ "db_optr ::= db_optr replica",
 /* 106 */ "db_optr ::= db_optr quorum",
 /* 107 */ "db_optr ::= db_optr days",
 /* 108 */ "db_optr ::= db_optr minrows",
 /* 109 */ "db_optr ::= db_optr maxrows",
 /* 110 */ "db_optr ::= db_optr blocks",
 /* 111 */ "db_optr ::= db_optr ctime",
 /* 112 */ "db_optr ::= db_optr wal",
 /* 113 */ "db_optr ::= db_optr fsync",
 /* 114 */ "db_optr ::= db_optr comp",
 /* 115 */ "db_optr ::= db_optr prec",
 /* 116 */ "db_optr ::= db_optr keep",
 /* 117 */ "db_optr ::= db_optr update",
 /* 118 */ "db_optr ::= db_optr cachelast",
 /* 119 */ "alter_db_optr ::=",
 /* 120 */ "alter_db_optr ::= alter_db_optr replica",
 /* 121 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 122 */ "alter_db_optr ::= alter_db_optr keep",
 /* 123 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 124 */ "alter_db_optr ::= alter_db_optr comp",
 /* 125 */ "alter_db_optr ::= alter_db_optr update",
 /* 126 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 127 */ "typename ::= ids",
 /* 128 */ "typename ::= ids LP signed RP",
 /* 129 */ "typename ::= ids UNSIGNED",
 /* 130 */ "signed ::= INTEGER",
 /* 131 */ "signed ::= PLUS INTEGER",
 /* 132 */ "signed ::= MINUS INTEGER",
 /* 133 */ "cmd ::= CREATE TABLE create_table_args",
 /* 134 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE TABLE create_table_list",
 /* 137 */ "create_table_list ::= create_from_stable",
 /* 138 */ "create_table_list ::= create_table_list create_from_stable",
 /* 139 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 140 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP",
 /* 143 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 144 */ "tagNamelist ::= ids",
 /* 145 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 146 */ "columnlist ::= columnlist COMMA column",
 /* 147 */ "columnlist ::= column",
 /* 148 */ "column ::= ids typename",
 /* 149 */ "tagitemlist1 ::= tagitemlist1 COMMA tagitem1",
 /* 150 */ "tagitemlist1 ::= tagitem1",
 /* 151 */ "tagitem1 ::= MINUS INTEGER",
 /* 152 */ "tagitem1 ::= MINUS FLOAT",
 /* 153 */ "tagitem1 ::= PLUS INTEGER",
 /* 154 */ "tagitem1 ::= PLUS FLOAT",
 /* 155 */ "tagitem1 ::= INTEGER",
 /* 156 */ "tagitem1 ::= FLOAT",
 /* 157 */ "tagitem1 ::= STRING",
 /* 158 */ "tagitem1 ::= BOOL",
 /* 159 */ "tagitem1 ::= NULL",
 /* 160 */ "tagitem1 ::= NOW",
 /* 161 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 162 */ "tagitemlist ::= tagitem",
 /* 163 */ "tagitem ::= INTEGER",
 /* 164 */ "tagitem ::= FLOAT",
 /* 165 */ "tagitem ::= STRING",
 /* 166 */ "tagitem ::= BOOL",
 /* 167 */ "tagitem ::= NULL",
 /* 168 */ "tagitem ::= NOW",
 /* 169 */ "tagitem ::= MINUS INTEGER",
 /* 170 */ "tagitem ::= MINUS FLOAT",
 /* 171 */ "tagitem ::= PLUS INTEGER",
 /* 172 */ "tagitem ::= PLUS FLOAT",
 /* 173 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 174 */ "select ::= LP select RP",
 /* 175 */ "union ::= select",
 /* 176 */ "union ::= union UNION ALL select",
 /* 177 */ "union ::= union UNION select",
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
 /* 199 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 200 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 201 */ "interval_option ::=",
 /* 202 */ "intervalKey ::= INTERVAL",
 /* 203 */ "intervalKey ::= EVERY",
 /* 204 */ "session_option ::=",
 /* 205 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 206 */ "windowstate_option ::=",
 /* 207 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 208 */ "fill_opt ::=",
 /* 209 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 210 */ "fill_opt ::= FILL LP ID RP",
 /* 211 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 212 */ "sliding_opt ::=",
 /* 213 */ "orderby_opt ::=",
 /* 214 */ "orderby_opt ::= ORDER BY sortlist",
 /* 215 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 216 */ "sortlist ::= item sortorder",
 /* 217 */ "item ::= ids cpxName",
 /* 218 */ "sortorder ::= ASC",
 /* 219 */ "sortorder ::= DESC",
 /* 220 */ "sortorder ::=",
 /* 221 */ "groupby_opt ::=",
 /* 222 */ "groupby_opt ::= GROUP BY grouplist",
 /* 223 */ "grouplist ::= grouplist COMMA item",
 /* 224 */ "grouplist ::= item",
 /* 225 */ "having_opt ::=",
 /* 226 */ "having_opt ::= HAVING expr",
 /* 227 */ "limit_opt ::=",
 /* 228 */ "limit_opt ::= LIMIT signed",
 /* 229 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 230 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 231 */ "slimit_opt ::=",
 /* 232 */ "slimit_opt ::= SLIMIT signed",
 /* 233 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 234 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 235 */ "where_opt ::=",
 /* 236 */ "where_opt ::= WHERE expr",
 /* 237 */ "expr ::= LP expr RP",
 /* 238 */ "expr ::= ID",
 /* 239 */ "expr ::= ID DOT ID",
 /* 240 */ "expr ::= ID DOT STAR",
 /* 241 */ "expr ::= INTEGER",
 /* 242 */ "expr ::= MINUS INTEGER",
 /* 243 */ "expr ::= PLUS INTEGER",
 /* 244 */ "expr ::= FLOAT",
 /* 245 */ "expr ::= MINUS FLOAT",
 /* 246 */ "expr ::= PLUS FLOAT",
 /* 247 */ "expr ::= STRING",
 /* 248 */ "expr ::= NOW",
 /* 249 */ "expr ::= VARIABLE",
 /* 250 */ "expr ::= PLUS VARIABLE",
 /* 251 */ "expr ::= MINUS VARIABLE",
 /* 252 */ "expr ::= BOOL",
 /* 253 */ "expr ::= NULL",
 /* 254 */ "expr ::= ID LP exprlist RP",
 /* 255 */ "expr ::= ID LP STAR RP",
 /* 256 */ "expr ::= expr IS NULL",
 /* 257 */ "expr ::= expr IS NOT NULL",
 /* 258 */ "expr ::= expr LT expr",
 /* 259 */ "expr ::= expr GT expr",
 /* 260 */ "expr ::= expr LE expr",
 /* 261 */ "expr ::= expr GE expr",
 /* 262 */ "expr ::= expr NE expr",
 /* 263 */ "expr ::= expr EQ expr",
 /* 264 */ "expr ::= expr BETWEEN expr AND expr",
 /* 265 */ "expr ::= expr AND expr",
 /* 266 */ "expr ::= expr OR expr",
 /* 267 */ "expr ::= expr PLUS expr",
 /* 268 */ "expr ::= expr MINUS expr",
 /* 269 */ "expr ::= expr STAR expr",
 /* 270 */ "expr ::= expr SLASH expr",
 /* 271 */ "expr ::= expr REM expr",
 /* 272 */ "expr ::= expr LIKE expr",
 /* 273 */ "expr ::= expr MATCH expr",
 /* 274 */ "expr ::= expr NMATCH expr",
 /* 275 */ "expr ::= expr IN LP exprlist RP",
 /* 276 */ "exprlist ::= exprlist COMMA expritem",
 /* 277 */ "exprlist ::= expritem",
 /* 278 */ "expritem ::= expr",
 /* 279 */ "expritem ::=",
 /* 280 */ "cmd ::= RESET QUERY CACHE",
 /* 281 */ "cmd ::= SYNCDB ids REPLICA",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 289 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 297 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 298 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 299 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 300 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 200: /* exprlist */
    case 244: /* selcollist */
    case 258: /* sclp */
{
tSqlExprListDestroy((yypminor->yy413));
}
      break;
    case 214: /* intitemlist */
    case 216: /* keep */
    case 236: /* columnlist */
    case 237: /* tagitemlist1 */
    case 238: /* tagNamelist */
    case 242: /* tagitemlist */
    case 251: /* fill_opt */
    case 252: /* groupby_opt */
    case 254: /* orderby_opt */
    case 266: /* sortlist */
    case 270: /* grouplist */
{
taosArrayDestroy((yypminor->yy413));
}
      break;
    case 234: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy438));
}
      break;
    case 239: /* select */
{
destroySqlNode((yypminor->yy24));
}
      break;
    case 245: /* from */
    case 262: /* tablelist */
    case 263: /* sub */
{
destroyRelationInfo((yypminor->yy292));
}
      break;
    case 246: /* where_opt */
    case 253: /* having_opt */
    case 260: /* expr */
    case 271: /* expritem */
{
tSqlExprDestroy((yypminor->yy370));
}
      break;
    case 257: /* union */
{
destroyAllSqlNode((yypminor->yy129));
}
      break;
    case 267: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy461));
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
  {  192,   -1 }, /* (0) program ::= cmd */
  {  193,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  193,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  193,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  193,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  193,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  193,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  193,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  193,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  193,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  193,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  193,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  193,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  193,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  193,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  193,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  193,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  195,    0 }, /* (17) dbPrefix ::= */
  {  195,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  196,    0 }, /* (19) cpxName ::= */
  {  196,   -2 }, /* (20) cpxName ::= DOT ids */
  {  193,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  193,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  193,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  193,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  193,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  193,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  193,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  193,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  193,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  193,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  193,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  193,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  193,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  193,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  193,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  193,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  193,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  193,   -2 }, /* (38) cmd ::= USE ids */
  {  193,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  193,   -3 }, /* (40) cmd ::= DESC ids cpxName */
  {  193,   -5 }, /* (41) cmd ::= ALTER USER ids PASS ids */
  {  193,   -5 }, /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  193,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  193,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  193,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  193,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  193,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  193,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  193,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  193,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  194,   -1 }, /* (51) ids ::= ID */
  {  194,   -1 }, /* (52) ids ::= STRING */
  {  197,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  197,    0 }, /* (54) ifexists ::= */
  {  201,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  201,    0 }, /* (56) ifnotexists ::= */
  {  193,   -5 }, /* (57) cmd ::= CREATE DNODE ids PORT ids */
  {  193,   -5 }, /* (58) cmd ::= CREATE DNODE IPTOKEN PORT ids */
  {  193,   -6 }, /* (59) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  193,   -5 }, /* (60) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  193,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  204,    0 }, /* (64) bufsize ::= */
  {  204,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  205,    0 }, /* (66) pps ::= */
  {  205,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  206,    0 }, /* (68) tseries ::= */
  {  206,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  207,    0 }, /* (70) dbs ::= */
  {  207,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  208,    0 }, /* (72) streams ::= */
  {  208,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  209,    0 }, /* (74) storage ::= */
  {  209,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  210,    0 }, /* (76) qtime ::= */
  {  210,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  211,    0 }, /* (78) users ::= */
  {  211,   -2 }, /* (79) users ::= USERS INTEGER */
  {  212,    0 }, /* (80) conns ::= */
  {  212,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  213,    0 }, /* (82) state ::= */
  {  213,   -2 }, /* (83) state ::= STATE ids */
  {  199,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  214,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  214,   -1 }, /* (86) intitemlist ::= intitem */
  {  215,   -1 }, /* (87) intitem ::= INTEGER */
  {  216,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  217,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  218,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  219,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  220,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  221,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  222,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  223,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  224,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  225,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  226,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  227,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  228,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  229,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  230,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  202,    0 }, /* (103) db_optr ::= */
  {  202,   -2 }, /* (104) db_optr ::= db_optr cache */
  {  202,   -2 }, /* (105) db_optr ::= db_optr replica */
  {  202,   -2 }, /* (106) db_optr ::= db_optr quorum */
  {  202,   -2 }, /* (107) db_optr ::= db_optr days */
  {  202,   -2 }, /* (108) db_optr ::= db_optr minrows */
  {  202,   -2 }, /* (109) db_optr ::= db_optr maxrows */
  {  202,   -2 }, /* (110) db_optr ::= db_optr blocks */
  {  202,   -2 }, /* (111) db_optr ::= db_optr ctime */
  {  202,   -2 }, /* (112) db_optr ::= db_optr wal */
  {  202,   -2 }, /* (113) db_optr ::= db_optr fsync */
  {  202,   -2 }, /* (114) db_optr ::= db_optr comp */
  {  202,   -2 }, /* (115) db_optr ::= db_optr prec */
  {  202,   -2 }, /* (116) db_optr ::= db_optr keep */
  {  202,   -2 }, /* (117) db_optr ::= db_optr update */
  {  202,   -2 }, /* (118) db_optr ::= db_optr cachelast */
  {  198,    0 }, /* (119) alter_db_optr ::= */
  {  198,   -2 }, /* (120) alter_db_optr ::= alter_db_optr replica */
  {  198,   -2 }, /* (121) alter_db_optr ::= alter_db_optr quorum */
  {  198,   -2 }, /* (122) alter_db_optr ::= alter_db_optr keep */
  {  198,   -2 }, /* (123) alter_db_optr ::= alter_db_optr blocks */
  {  198,   -2 }, /* (124) alter_db_optr ::= alter_db_optr comp */
  {  198,   -2 }, /* (125) alter_db_optr ::= alter_db_optr update */
  {  198,   -2 }, /* (126) alter_db_optr ::= alter_db_optr cachelast */
  {  203,   -1 }, /* (127) typename ::= ids */
  {  203,   -4 }, /* (128) typename ::= ids LP signed RP */
  {  203,   -2 }, /* (129) typename ::= ids UNSIGNED */
  {  231,   -1 }, /* (130) signed ::= INTEGER */
  {  231,   -2 }, /* (131) signed ::= PLUS INTEGER */
  {  231,   -2 }, /* (132) signed ::= MINUS INTEGER */
  {  193,   -3 }, /* (133) cmd ::= CREATE TABLE create_table_args */
  {  193,   -3 }, /* (134) cmd ::= CREATE TABLE create_stable_args */
  {  193,   -3 }, /* (135) cmd ::= CREATE STABLE create_stable_args */
  {  193,   -3 }, /* (136) cmd ::= CREATE TABLE create_table_list */
  {  234,   -1 }, /* (137) create_table_list ::= create_from_stable */
  {  234,   -2 }, /* (138) create_table_list ::= create_table_list create_from_stable */
  {  232,   -6 }, /* (139) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  233,  -10 }, /* (140) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  235,  -10 }, /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
  {  235,  -13 }, /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
  {  238,   -3 }, /* (143) tagNamelist ::= tagNamelist COMMA ids */
  {  238,   -1 }, /* (144) tagNamelist ::= ids */
  {  232,   -5 }, /* (145) create_table_args ::= ifnotexists ids cpxName AS select */
  {  236,   -3 }, /* (146) columnlist ::= columnlist COMMA column */
  {  236,   -1 }, /* (147) columnlist ::= column */
  {  240,   -2 }, /* (148) column ::= ids typename */
  {  237,   -3 }, /* (149) tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
  {  237,   -1 }, /* (150) tagitemlist1 ::= tagitem1 */
  {  241,   -2 }, /* (151) tagitem1 ::= MINUS INTEGER */
  {  241,   -2 }, /* (152) tagitem1 ::= MINUS FLOAT */
  {  241,   -2 }, /* (153) tagitem1 ::= PLUS INTEGER */
  {  241,   -2 }, /* (154) tagitem1 ::= PLUS FLOAT */
  {  241,   -1 }, /* (155) tagitem1 ::= INTEGER */
  {  241,   -1 }, /* (156) tagitem1 ::= FLOAT */
  {  241,   -1 }, /* (157) tagitem1 ::= STRING */
  {  241,   -1 }, /* (158) tagitem1 ::= BOOL */
  {  241,   -1 }, /* (159) tagitem1 ::= NULL */
  {  241,   -1 }, /* (160) tagitem1 ::= NOW */
  {  242,   -3 }, /* (161) tagitemlist ::= tagitemlist COMMA tagitem */
  {  242,   -1 }, /* (162) tagitemlist ::= tagitem */
  {  243,   -1 }, /* (163) tagitem ::= INTEGER */
  {  243,   -1 }, /* (164) tagitem ::= FLOAT */
  {  243,   -1 }, /* (165) tagitem ::= STRING */
  {  243,   -1 }, /* (166) tagitem ::= BOOL */
  {  243,   -1 }, /* (167) tagitem ::= NULL */
  {  243,   -1 }, /* (168) tagitem ::= NOW */
  {  243,   -2 }, /* (169) tagitem ::= MINUS INTEGER */
  {  243,   -2 }, /* (170) tagitem ::= MINUS FLOAT */
  {  243,   -2 }, /* (171) tagitem ::= PLUS INTEGER */
  {  243,   -2 }, /* (172) tagitem ::= PLUS FLOAT */
  {  239,  -14 }, /* (173) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  239,   -3 }, /* (174) select ::= LP select RP */
  {  257,   -1 }, /* (175) union ::= select */
  {  257,   -4 }, /* (176) union ::= union UNION ALL select */
  {  257,   -3 }, /* (177) union ::= union UNION select */
  {  193,   -1 }, /* (178) cmd ::= union */
  {  239,   -2 }, /* (179) select ::= SELECT selcollist */
  {  258,   -2 }, /* (180) sclp ::= selcollist COMMA */
  {  258,    0 }, /* (181) sclp ::= */
  {  244,   -4 }, /* (182) selcollist ::= sclp distinct expr as */
  {  244,   -2 }, /* (183) selcollist ::= sclp STAR */
  {  261,   -2 }, /* (184) as ::= AS ids */
  {  261,   -1 }, /* (185) as ::= ids */
  {  261,    0 }, /* (186) as ::= */
  {  259,   -1 }, /* (187) distinct ::= DISTINCT */
  {  259,    0 }, /* (188) distinct ::= */
  {  245,   -2 }, /* (189) from ::= FROM tablelist */
  {  245,   -2 }, /* (190) from ::= FROM sub */
  {  263,   -3 }, /* (191) sub ::= LP union RP */
  {  263,   -4 }, /* (192) sub ::= LP union RP ids */
  {  263,   -6 }, /* (193) sub ::= sub COMMA LP union RP ids */
  {  262,   -2 }, /* (194) tablelist ::= ids cpxName */
  {  262,   -3 }, /* (195) tablelist ::= ids cpxName ids */
  {  262,   -4 }, /* (196) tablelist ::= tablelist COMMA ids cpxName */
  {  262,   -5 }, /* (197) tablelist ::= tablelist COMMA ids cpxName ids */
  {  264,   -1 }, /* (198) tmvar ::= VARIABLE */
  {  247,   -4 }, /* (199) interval_option ::= intervalKey LP tmvar RP */
  {  247,   -6 }, /* (200) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  247,    0 }, /* (201) interval_option ::= */
  {  265,   -1 }, /* (202) intervalKey ::= INTERVAL */
  {  265,   -1 }, /* (203) intervalKey ::= EVERY */
  {  249,    0 }, /* (204) session_option ::= */
  {  249,   -7 }, /* (205) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  250,    0 }, /* (206) windowstate_option ::= */
  {  250,   -4 }, /* (207) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  251,    0 }, /* (208) fill_opt ::= */
  {  251,   -6 }, /* (209) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  251,   -4 }, /* (210) fill_opt ::= FILL LP ID RP */
  {  248,   -4 }, /* (211) sliding_opt ::= SLIDING LP tmvar RP */
  {  248,    0 }, /* (212) sliding_opt ::= */
  {  254,    0 }, /* (213) orderby_opt ::= */
  {  254,   -3 }, /* (214) orderby_opt ::= ORDER BY sortlist */
  {  266,   -4 }, /* (215) sortlist ::= sortlist COMMA item sortorder */
  {  266,   -2 }, /* (216) sortlist ::= item sortorder */
  {  268,   -2 }, /* (217) item ::= ids cpxName */
  {  269,   -1 }, /* (218) sortorder ::= ASC */
  {  269,   -1 }, /* (219) sortorder ::= DESC */
  {  269,    0 }, /* (220) sortorder ::= */
  {  252,    0 }, /* (221) groupby_opt ::= */
  {  252,   -3 }, /* (222) groupby_opt ::= GROUP BY grouplist */
  {  270,   -3 }, /* (223) grouplist ::= grouplist COMMA item */
  {  270,   -1 }, /* (224) grouplist ::= item */
  {  253,    0 }, /* (225) having_opt ::= */
  {  253,   -2 }, /* (226) having_opt ::= HAVING expr */
  {  256,    0 }, /* (227) limit_opt ::= */
  {  256,   -2 }, /* (228) limit_opt ::= LIMIT signed */
  {  256,   -4 }, /* (229) limit_opt ::= LIMIT signed OFFSET signed */
  {  256,   -4 }, /* (230) limit_opt ::= LIMIT signed COMMA signed */
  {  255,    0 }, /* (231) slimit_opt ::= */
  {  255,   -2 }, /* (232) slimit_opt ::= SLIMIT signed */
  {  255,   -4 }, /* (233) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  255,   -4 }, /* (234) slimit_opt ::= SLIMIT signed COMMA signed */
  {  246,    0 }, /* (235) where_opt ::= */
  {  246,   -2 }, /* (236) where_opt ::= WHERE expr */
  {  260,   -3 }, /* (237) expr ::= LP expr RP */
  {  260,   -1 }, /* (238) expr ::= ID */
  {  260,   -3 }, /* (239) expr ::= ID DOT ID */
  {  260,   -3 }, /* (240) expr ::= ID DOT STAR */
  {  260,   -1 }, /* (241) expr ::= INTEGER */
  {  260,   -2 }, /* (242) expr ::= MINUS INTEGER */
  {  260,   -2 }, /* (243) expr ::= PLUS INTEGER */
  {  260,   -1 }, /* (244) expr ::= FLOAT */
  {  260,   -2 }, /* (245) expr ::= MINUS FLOAT */
  {  260,   -2 }, /* (246) expr ::= PLUS FLOAT */
  {  260,   -1 }, /* (247) expr ::= STRING */
  {  260,   -1 }, /* (248) expr ::= NOW */
  {  260,   -1 }, /* (249) expr ::= VARIABLE */
  {  260,   -2 }, /* (250) expr ::= PLUS VARIABLE */
  {  260,   -2 }, /* (251) expr ::= MINUS VARIABLE */
  {  260,   -1 }, /* (252) expr ::= BOOL */
  {  260,   -1 }, /* (253) expr ::= NULL */
  {  260,   -4 }, /* (254) expr ::= ID LP exprlist RP */
  {  260,   -4 }, /* (255) expr ::= ID LP STAR RP */
  {  260,   -3 }, /* (256) expr ::= expr IS NULL */
  {  260,   -4 }, /* (257) expr ::= expr IS NOT NULL */
  {  260,   -3 }, /* (258) expr ::= expr LT expr */
  {  260,   -3 }, /* (259) expr ::= expr GT expr */
  {  260,   -3 }, /* (260) expr ::= expr LE expr */
  {  260,   -3 }, /* (261) expr ::= expr GE expr */
  {  260,   -3 }, /* (262) expr ::= expr NE expr */
  {  260,   -3 }, /* (263) expr ::= expr EQ expr */
  {  260,   -5 }, /* (264) expr ::= expr BETWEEN expr AND expr */
  {  260,   -3 }, /* (265) expr ::= expr AND expr */
  {  260,   -3 }, /* (266) expr ::= expr OR expr */
  {  260,   -3 }, /* (267) expr ::= expr PLUS expr */
  {  260,   -3 }, /* (268) expr ::= expr MINUS expr */
  {  260,   -3 }, /* (269) expr ::= expr STAR expr */
  {  260,   -3 }, /* (270) expr ::= expr SLASH expr */
  {  260,   -3 }, /* (271) expr ::= expr REM expr */
  {  260,   -3 }, /* (272) expr ::= expr LIKE expr */
  {  260,   -3 }, /* (273) expr ::= expr MATCH expr */
  {  260,   -3 }, /* (274) expr ::= expr NMATCH expr */
  {  260,   -5 }, /* (275) expr ::= expr IN LP exprlist RP */
  {  200,   -3 }, /* (276) exprlist ::= exprlist COMMA expritem */
  {  200,   -1 }, /* (277) exprlist ::= expritem */
  {  271,   -1 }, /* (278) expritem ::= expr */
  {  271,    0 }, /* (279) expritem ::= */
  {  193,   -3 }, /* (280) cmd ::= RESET QUERY CACHE */
  {  193,   -3 }, /* (281) cmd ::= SYNCDB ids REPLICA */
  {  193,   -7 }, /* (282) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (283) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (284) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (286) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (287) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (288) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (289) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -7 }, /* (290) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (291) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (292) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (294) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (295) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (296) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (297) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -3 }, /* (298) cmd ::= KILL CONNECTION INTEGER */
  {  193,   -5 }, /* (299) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  193,   -5 }, /* (300) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 133: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==135);
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
    setShowOptions(pInfo, TSDB_MGMT_TABLE_STB, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_STB, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 35: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 36: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 38: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
      case 40: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==40);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 41: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 42: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 46: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 47: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy254, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy171);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy413);}
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
      case 57: /* cmd ::= CREATE DNODE ids PORT ids */
      case 58: /* cmd ::= CREATE DNODE IPTOKEN PORT ids */ yytestcase(yyruleno==58);
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 60: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy254, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy280, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy280, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy171.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy171.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy171.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy171.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy171.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy171.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy171.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy171.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy171.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy171 = yylhsminor.yy171;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 161: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==161);
{ yylhsminor.yy413 = tListItemAppend(yymsp[-2].minor.yy413, &yymsp[0].minor.yy461, -1);    }
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 86: /* intitemlist ::= intitem */
      case 162: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==162);
{ yylhsminor.yy413 = tListItemAppend(NULL, &yymsp[0].minor.yy461, -1); }
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 87: /* intitem ::= INTEGER */
      case 163: /* tagitem ::= INTEGER */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= FLOAT */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= STRING */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= BOOL */ yytestcase(yyruleno==166);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy461, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy461 = yylhsminor.yy461;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy413 = yymsp[0].minor.yy413; }
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
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 103: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy254);}
        break;
      case 104: /* db_optr ::= db_optr cache */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 105: /* db_optr ::= db_optr replica */
      case 120: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==120);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 106: /* db_optr ::= db_optr quorum */
      case 121: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==121);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 107: /* db_optr ::= db_optr days */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 108: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 109: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 110: /* db_optr ::= db_optr blocks */
      case 123: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==123);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 111: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 112: /* db_optr ::= db_optr wal */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 113: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 114: /* db_optr ::= db_optr comp */
      case 124: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==124);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 115: /* db_optr ::= db_optr prec */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 116: /* db_optr ::= db_optr keep */
      case 122: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==122);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.keep = yymsp[0].minor.yy413; }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 117: /* db_optr ::= db_optr update */
      case 125: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==125);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 118: /* db_optr ::= db_optr cachelast */
      case 126: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==126);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 119: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy254);}
        break;
      case 127: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy280, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy280 = yylhsminor.yy280;
        break;
      case 128: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy157 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy280, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy157;  // negative value of name length
    tSetColumnType(&yylhsminor.yy280, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy280 = yylhsminor.yy280;
        break;
      case 129: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy280, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy280 = yylhsminor.yy280;
        break;
      case 130: /* signed ::= INTEGER */
{ yylhsminor.yy157 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy157 = yylhsminor.yy157;
        break;
      case 131: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy157 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 132: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy157 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 136: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy438;}
        break;
      case 137: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy544);
  pCreateTable->type = TSQL_CREATE_CTABLE;
  yylhsminor.yy438 = pCreateTable;
}
  yymsp[0].minor.yy438 = yylhsminor.yy438;
        break;
      case 138: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy438->childTableInfo, &yymsp[0].minor.yy544);
  yylhsminor.yy438 = yymsp[-1].minor.yy438;
}
  yymsp[-1].minor.yy438 = yylhsminor.yy438;
        break;
      case 139: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-1].minor.yy413, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy438 = yylhsminor.yy438;
        break;
      case 140: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-5].minor.yy413, yymsp[-1].minor.yy413, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy438 = yylhsminor.yy438;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy544 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy413, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy544 = yylhsminor.yy544;
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy544 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy413, yymsp[-1].minor.yy413, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy544 = yylhsminor.yy544;
        break;
      case 143: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy413, &yymsp[0].minor.yy0); yylhsminor.yy413 = yymsp[-2].minor.yy413;  }
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 144: /* tagNamelist ::= ids */
{yylhsminor.yy413 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy413, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy438 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy24, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy438 = yylhsminor.yy438;
        break;
      case 146: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy413, &yymsp[0].minor.yy280); yylhsminor.yy413 = yymsp[-2].minor.yy413;  }
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 147: /* columnlist ::= column */
{yylhsminor.yy413 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy413, &yymsp[0].minor.yy280);}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 148: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy280, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy280);
}
  yymsp[-1].minor.yy280 = yylhsminor.yy280;
        break;
      case 149: /* tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
{ taosArrayPush(yymsp[-2].minor.yy413, &yymsp[0].minor.yy0); yylhsminor.yy413 = yymsp[-2].minor.yy413;}
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 150: /* tagitemlist1 ::= tagitem1 */
{ yylhsminor.yy413 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy413, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 151: /* tagitem1 ::= MINUS INTEGER */
      case 152: /* tagitem1 ::= MINUS FLOAT */ yytestcase(yyruleno==152);
      case 153: /* tagitem1 ::= PLUS INTEGER */ yytestcase(yyruleno==153);
      case 154: /* tagitem1 ::= PLUS FLOAT */ yytestcase(yyruleno==154);
{ yylhsminor.yy0.n = yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n; yylhsminor.yy0.type = yymsp[0].minor.yy0.type; }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 155: /* tagitem1 ::= INTEGER */
      case 156: /* tagitem1 ::= FLOAT */ yytestcase(yyruleno==156);
      case 157: /* tagitem1 ::= STRING */ yytestcase(yyruleno==157);
      case 158: /* tagitem1 ::= BOOL */ yytestcase(yyruleno==158);
      case 159: /* tagitem1 ::= NULL */ yytestcase(yyruleno==159);
      case 160: /* tagitem1 ::= NOW */ yytestcase(yyruleno==160);
{ yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 167: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy461, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy461 = yylhsminor.yy461;
        break;
      case 168: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy461, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy461 = yylhsminor.yy461;
        break;
      case 169: /* tagitem ::= MINUS INTEGER */
      case 170: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==170);
      case 171: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==171);
      case 172: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==172);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy461, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy461 = yylhsminor.yy461;
        break;
      case 173: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy24 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy413, yymsp[-11].minor.yy292, yymsp[-10].minor.yy370, yymsp[-4].minor.yy413, yymsp[-2].minor.yy413, &yymsp[-9].minor.yy136, &yymsp[-7].minor.yy251, &yymsp[-6].minor.yy256, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy413, &yymsp[0].minor.yy503, &yymsp[-1].minor.yy503, yymsp[-3].minor.yy370);
}
  yymsp[-13].minor.yy24 = yylhsminor.yy24;
        break;
      case 174: /* select ::= LP select RP */
{yymsp[-2].minor.yy24 = yymsp[-1].minor.yy24;}
        break;
      case 175: /* union ::= select */
{ yylhsminor.yy129 = setSubclause(NULL, yymsp[0].minor.yy24); }
  yymsp[0].minor.yy129 = yylhsminor.yy129;
        break;
      case 176: /* union ::= union UNION ALL select */
{ yylhsminor.yy129 = appendSelectClause(yymsp[-3].minor.yy129, SQL_TYPE_UNIONALL, yymsp[0].minor.yy24);  }
  yymsp[-3].minor.yy129 = yylhsminor.yy129;
        break;
      case 177: /* union ::= union UNION select */
{ yylhsminor.yy129 = appendSelectClause(yymsp[-2].minor.yy129, SQL_TYPE_UNION, yymsp[0].minor.yy24);  }
  yymsp[-2].minor.yy129 = yylhsminor.yy129;
        break;
      case 178: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy129, NULL, TSDB_SQL_SELECT); }
        break;
      case 179: /* select ::= SELECT selcollist */
{
  yylhsminor.yy24 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy413, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy24 = yylhsminor.yy24;
        break;
      case 180: /* sclp ::= selcollist COMMA */
{yylhsminor.yy413 = yymsp[-1].minor.yy413;}
  yymsp[-1].minor.yy413 = yylhsminor.yy413;
        break;
      case 181: /* sclp ::= */
      case 213: /* orderby_opt ::= */ yytestcase(yyruleno==213);
{yymsp[1].minor.yy413 = 0;}
        break;
      case 182: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy413 = tSqlExprListAppend(yymsp[-3].minor.yy413, yymsp[-1].minor.yy370,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy413 = yylhsminor.yy413;
        break;
      case 183: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy413 = tSqlExprListAppend(yymsp[-1].minor.yy413, pNode, 0, 0);
}
  yymsp[-1].minor.yy413 = yylhsminor.yy413;
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
{yymsp[-1].minor.yy292 = yymsp[0].minor.yy292;}
        break;
      case 191: /* sub ::= LP union RP */
{yymsp[-2].minor.yy292 = addSubquery(NULL, yymsp[-1].minor.yy129, NULL);}
        break;
      case 192: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy292 = addSubquery(NULL, yymsp[-2].minor.yy129, &yymsp[0].minor.yy0);}
        break;
      case 193: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy292 = addSubquery(yymsp[-5].minor.yy292, yymsp[-2].minor.yy129, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy292 = yylhsminor.yy292;
        break;
      case 194: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy292 = yylhsminor.yy292;
        break;
      case 195: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy292 = yylhsminor.yy292;
        break;
      case 196: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(yymsp[-3].minor.yy292, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy292 = yylhsminor.yy292;
        break;
      case 197: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(yymsp[-4].minor.yy292, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy292 = yylhsminor.yy292;
        break;
      case 198: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 199: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy136.interval = yymsp[-1].minor.yy0; yylhsminor.yy136.offset.n = 0; yylhsminor.yy136.token = yymsp[-3].minor.yy516;}
  yymsp[-3].minor.yy136 = yylhsminor.yy136;
        break;
      case 200: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy136.interval = yymsp[-3].minor.yy0; yylhsminor.yy136.offset = yymsp[-1].minor.yy0;   yylhsminor.yy136.token = yymsp[-5].minor.yy516;}
  yymsp[-5].minor.yy136 = yylhsminor.yy136;
        break;
      case 201: /* interval_option ::= */
{memset(&yymsp[1].minor.yy136, 0, sizeof(yymsp[1].minor.yy136));}
        break;
      case 202: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy516 = TK_INTERVAL;}
        break;
      case 203: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy516 = TK_EVERY;   }
        break;
      case 204: /* session_option ::= */
{yymsp[1].minor.yy251.col.n = 0; yymsp[1].minor.yy251.gap.n = 0;}
        break;
      case 205: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy251.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy251.gap = yymsp[-1].minor.yy0;
}
        break;
      case 206: /* windowstate_option ::= */
{ yymsp[1].minor.yy256.col.n = 0; yymsp[1].minor.yy256.col.z = NULL;}
        break;
      case 207: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy256.col = yymsp[-1].minor.yy0; }
        break;
      case 208: /* fill_opt ::= */
{ yymsp[1].minor.yy413 = 0;     }
        break;
      case 209: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy413, &A, -1, 0);
    yymsp[-5].minor.yy413 = yymsp[-1].minor.yy413;
}
        break;
      case 210: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy413 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 211: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 212: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 214: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy413 = yymsp[0].minor.yy413;}
        break;
      case 215: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy413 = tListItemAppend(yymsp[-3].minor.yy413, &yymsp[-1].minor.yy461, yymsp[0].minor.yy60);
}
  yymsp[-3].minor.yy413 = yylhsminor.yy413;
        break;
      case 216: /* sortlist ::= item sortorder */
{
  yylhsminor.yy413 = tListItemAppend(NULL, &yymsp[-1].minor.yy461, yymsp[0].minor.yy60);
}
  yymsp[-1].minor.yy413 = yylhsminor.yy413;
        break;
      case 217: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy461, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy461 = yylhsminor.yy461;
        break;
      case 218: /* sortorder ::= ASC */
{ yymsp[0].minor.yy60 = TSDB_ORDER_ASC; }
        break;
      case 219: /* sortorder ::= DESC */
{ yymsp[0].minor.yy60 = TSDB_ORDER_DESC;}
        break;
      case 220: /* sortorder ::= */
{ yymsp[1].minor.yy60 = TSDB_ORDER_ASC; }
        break;
      case 221: /* groupby_opt ::= */
{ yymsp[1].minor.yy413 = 0;}
        break;
      case 222: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy413 = yymsp[0].minor.yy413;}
        break;
      case 223: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy413 = tListItemAppend(yymsp[-2].minor.yy413, &yymsp[0].minor.yy461, -1);
}
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 224: /* grouplist ::= item */
{
  yylhsminor.yy413 = tListItemAppend(NULL, &yymsp[0].minor.yy461, -1);
}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 225: /* having_opt ::= */
      case 235: /* where_opt ::= */ yytestcase(yyruleno==235);
      case 279: /* expritem ::= */ yytestcase(yyruleno==279);
{yymsp[1].minor.yy370 = 0;}
        break;
      case 226: /* having_opt ::= HAVING expr */
      case 236: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==236);
{yymsp[-1].minor.yy370 = yymsp[0].minor.yy370;}
        break;
      case 227: /* limit_opt ::= */
      case 231: /* slimit_opt ::= */ yytestcase(yyruleno==231);
{yymsp[1].minor.yy503.limit = -1; yymsp[1].minor.yy503.offset = 0;}
        break;
      case 228: /* limit_opt ::= LIMIT signed */
      case 232: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==232);
{yymsp[-1].minor.yy503.limit = yymsp[0].minor.yy157;  yymsp[-1].minor.yy503.offset = 0;}
        break;
      case 229: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy503.limit = yymsp[-2].minor.yy157;  yymsp[-3].minor.yy503.offset = yymsp[0].minor.yy157;}
        break;
      case 230: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy503.limit = yymsp[0].minor.yy157;  yymsp[-3].minor.yy503.offset = yymsp[-2].minor.yy157;}
        break;
      case 233: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy503.limit = yymsp[-2].minor.yy157;  yymsp[-3].minor.yy503.offset = yymsp[0].minor.yy157;}
        break;
      case 234: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy503.limit = yymsp[0].minor.yy157;  yymsp[-3].minor.yy503.offset = yymsp[-2].minor.yy157;}
        break;
      case 237: /* expr ::= LP expr RP */
{yylhsminor.yy370 = yymsp[-1].minor.yy370; yylhsminor.yy370->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy370->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 238: /* expr ::= ID */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 239: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 240: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 241: /* expr ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 242: /* expr ::= MINUS INTEGER */
      case 243: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==243);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 244: /* expr ::= FLOAT */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 245: /* expr ::= MINUS FLOAT */
      case 246: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==246);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 247: /* expr ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 248: /* expr ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 249: /* expr ::= VARIABLE */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 250: /* expr ::= PLUS VARIABLE */
      case 251: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==251);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 252: /* expr ::= BOOL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 253: /* expr ::= NULL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 254: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(yymsp[-1].minor.yy413, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 255: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy370 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 256: /* expr ::= expr IS NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 257: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-3].minor.yy370, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 258: /* expr ::= expr LT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 259: /* expr ::= expr GT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 260: /* expr ::= expr LE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 261: /* expr ::= expr GE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 262: /* expr ::= expr NE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 263: /* expr ::= expr EQ expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_EQ);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 264: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy370); yylhsminor.yy370 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy370, yymsp[-2].minor.yy370, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy370, TK_LE), TK_AND);}
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 265: /* expr ::= expr AND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_AND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 266: /* expr ::= expr OR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_OR); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 267: /* expr ::= expr PLUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_PLUS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 268: /* expr ::= expr MINUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MINUS); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 269: /* expr ::= expr STAR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_STAR);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 270: /* expr ::= expr SLASH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_DIVIDE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 271: /* expr ::= expr REM expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_REM);   }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 272: /* expr ::= expr LIKE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LIKE);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 273: /* expr ::= expr MATCH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MATCH);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 274: /* expr ::= expr NMATCH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NMATCH);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 275: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-4].minor.yy370, (tSqlExpr*)yymsp[-1].minor.yy413, TK_IN); }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 276: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy413 = tSqlExprListAppend(yymsp[-2].minor.yy413,yymsp[0].minor.yy370,0, 0);}
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 277: /* exprlist ::= expritem */
{yylhsminor.yy413 = tSqlExprListAppend(0,yymsp[0].minor.yy370,0, 0);}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 278: /* expritem ::= expr */
{yylhsminor.yy370 = yymsp[0].minor.yy370;}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 280: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 281: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy461, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy461, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 298: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 299: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 300: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
