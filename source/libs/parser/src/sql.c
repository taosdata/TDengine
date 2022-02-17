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
#define YYNOCODE 274
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SWindowStateVal yy6;
  SRelationInfo* yy10;
  SCreateDbInfo yy16;
  int32_t yy46;
  int yy47;
  SSessionWindowVal yy97;
  SField yy106;
  SCreatedTableInfo yy150;
  SArray* yy165;
  tSqlExpr* yy202;
  int64_t yy207;
  SCreateAcctInfo yy211;
  SSqlNode* yy278;
  SCreateTableSql* yy326;
  SLimit yy367;
  SVariant yy425;
  SSubclause* yy503;
  SIntervalVal yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             366
#define YYNRULE              302
#define YYNTOKEN             191
#define YY_MAX_SHIFT         365
#define YY_MIN_SHIFTREDUCE   586
#define YY_MAX_SHIFTREDUCE   887
#define YY_ERROR_ACTION      888
#define YY_ACCEPT_ACTION     889
#define YY_NO_ACTION         890
#define YY_MIN_REDUCE        891
#define YY_MAX_REDUCE        1192
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
#define YY_ACTTAB_COUNT (777)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   249,  637,  637, 1077,   83,  717,  248,   55,   56,   88,
 /*    10 */    59,   60,  364,  230,  252,   49,   48,   47,   78,   58,
 /*    20 */   323,   63,   61,   64,   62,  236, 1054,  889,  365,   54,
 /*    30 */    53,  343,  342,   52,   51,   50,  253,   55,   56,  242,
 /*    40 */    59,   60, 1027, 1042,  252,   49,   48,   47,  104,   58,
 /*    50 */   323,   63,   61,   64,   62, 1014,  637, 1012, 1013,   54,
 /*    60 */    53,  206, 1015,   52,   51,   50, 1016,  206, 1017, 1018,
 /*    70 */   121, 1074, 1169,   55,   56, 1067,   59,   60, 1169,   27,
 /*    80 */   252,   49,   48,   47,   89,   58,  323,   63,   61,   64,
 /*    90 */    62,  206,   39,  274,  246,   54,   53,  206, 1042,   52,
 /*   100 */    51,   50, 1168,   55,   57, 1030,   59,   60, 1169,  824,
 /*   110 */   252,   49,   48,   47,   80,   58,  323,   63,   61,   64,
 /*   120 */    62,  294,  637,   81,  159,   54,   53,   91,  319,   52,
 /*   130 */    51,   50,  104, 1067,   56,  232,   59,   60,  353, 1039,
 /*   140 */   252,   49,   48,   47,  261,   58,  323,   63,   61,   64,
 /*   150 */    62,  233,   43,  124,   79,   54,   53,   80,   39,   52,
 /*   160 */    51,   50,  587,  588,  589,  590,  591,  592,  593,  594,
 /*   170 */   595,  596,  597,  598,  599,  600,  199,  309,  231,   59,
 /*   180 */    60,  939, 1067,  252,   49,   48,   47,  158,   58,  323,
 /*   190 */    63,   61,   64,   62,  353,   43,  280,  279,   54,   53,
 /*   200 */   234,  240,   52,   51,   50, 1039,   14, 1116,   13,  292,
 /*   210 */    42,  317,  359,  358,  316,  315,  314,  357,  313,  312,
 /*   220 */   311,  356,  310,  355,  354, 1007,  995,  996,  997,  998,
 /*   230 */   999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1008, 1009,
 /*   240 */  1010,  266,   12,   22,   63,   61,   64,   62,   84,   21,
 /*   250 */   270,  269,   54,   53,  789,  790,   52,   51,   50,  946,
 /*   260 */   215,  113,  251,  839,  828,  831,  834,  216,  753,  750,
 /*   270 */   751,  752,  672,  175,  174,  172,  217,  119,  261,  282,
 /*   280 */   328,   80,  251,  839,  828,  831,  834,  127,  116,  203,
 /*   290 */   228,  229,  255,   39,  324,  257,  258,  745,  742,  743,
 /*   300 */   744,  204,   39, 1036,    3,   32,  131,  261,  209,  260,
 /*   310 */   228,  229,  129,   85,  123,  133, 1040,   39,   39,   43,
 /*   320 */  1025, 1026,   30, 1029,  244,  245,  189,  186,  183,   52,
 /*   330 */    51,   50,  305,  181,  179,  178,  177,  176,  319,   65,
 /*   340 */  1038,  273,  637,   86,   42,  241,  359,  358,  243, 1039,
 /*   350 */   224,  357,  770,  830,  833,  356,   39,  355,  354,   65,
 /*   360 */   332,  333,   54,   53, 1039, 1039,   52,   51,   50,  256,
 /*   370 */    39,  254,   39,  331,  330,  840,  835,  104,   39,   39,
 /*   380 */   754,  755,  836,  149,  142,  162,  262,   39,  259,  806,
 /*   390 */   338,  337,  167,  170,  160,  840,  835,  829,  832,  334,
 /*   400 */   104,  164,  836, 1039,  826,  363,  362,  190,  767,  746,
 /*   410 */   747,   92,  949,  335,  940,  339,   93, 1039,  158, 1039,
 /*   420 */   158,  340,  341,   71,  321, 1039, 1039,  198,  195,  193,
 /*   430 */   345,  360,  976,    7, 1039,  275,  827,  786,  774,  796,
 /*   440 */   797,   74,  727,  297,  729,  299,  325,  805,   35,   70,
 /*   450 */   210,   40, 1115,   97,   70,   66,   24,  728,   40,   40,
 /*   460 */    67,  117,  740,  741,   72,  738,  739,  862,  841,  250,
 /*   470 */   636,  837,  140,   67,  139,   82,   16,   18,   15,   17,
 /*   480 */    75,  211,  300,   23,   23,   77,   23,  758,  759,  169,
 /*   490 */   168,    4,  756,  757,  147,   20,  146,   19, 1163, 1162,
 /*   500 */  1161,  226,  227,  207,  208,  212, 1053, 1041,  205, 1126,
 /*   510 */   213,  214,  219,  220,  221,  838,  218,  202,  716, 1188,
 /*   520 */  1180, 1125, 1069,  238, 1122, 1121,  239,   44, 1068,  344,
 /*   530 */   277,  114,  322, 1108, 1107,  196,  271,  785, 1037,  276,
 /*   540 */  1065,   87, 1076, 1087,  281,  235,   73,   90, 1084, 1085,
 /*   550 */    76,  843,  283, 1089,  295,   46,  105,  106,   94,  107,
 /*   560 */   293,   95,  101,  108,  285,  291,  288,  286,  109, 1109,
 /*   570 */   289,  110,  287,  111,  284,  112,   29,   45,  225,  115,
 /*   580 */  1035,  306,  150,  247,  118,  952,  346,  120,  301,  953,
 /*   590 */   302,  303,  304,  974,  307,  308,  200,  347,  348,   38,
 /*   600 */   320,  349,  948,  951,  947,  130,  329,  350, 1187,  351,
 /*   610 */   352,  361,  137, 1186, 1183,  887,  141,  222,  336, 1179,
 /*   620 */   152,  144, 1178,  265, 1175,  886,  148,  223,  973,  268,
 /*   630 */   885,  868,  867,   70,  296,    8,   28,   41,   31,  761,
 /*   640 */   278,   96,  153,  201,  937,  151,  154,  156,  157,  155,
 /*   650 */   163,  935,  165,    1,  166,  933,  264,  932,  263,  171,
 /*   660 */   267,  975,  931,  930,  173,  929,  928,  927,  926,  925,
 /*   670 */   924,  272,  923,  787,  180,  184,   98,  182,  922,  798,
 /*   680 */   185,  921,   99,  188,  187,  920,  918,  792,  100,  916,
 /*   690 */   914,  102,  194,  913,  911,  912,  197,  907,  794,    2,
 /*   700 */   237,  103,  290,    9,   33,   34,   10,   11,   25,  298,
 /*   710 */    26,  122,  119,   36,  126,  125,  650,  688,   37,  128,
 /*   720 */   685,  683,  682,  681,  679,  678,  677,  674,  640,  132,
 /*   730 */     5,  134,  135,  842,  318,  844,    6,  326,  327,   68,
 /*   740 */   136,   40,   69,  719,  718,  138,  143,  145,  715,  666,
 /*   750 */   664,  656,  662,  658,  660,  654,  652,  687,  686,  684,
 /*   760 */   680,  676,  675,  161,  604,  638,  891,  890,  890,  890,
 /*   770 */   890,  890,  890,  890,  890,  191,  192,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   200,    1,    1,  194,  245,    3,  200,    7,    8,  201,
 /*    10 */    10,   11,  194,  195,   14,   15,   16,   17,  259,   19,
 /*    20 */    20,   21,   22,   23,   24,  243,  244,  192,  193,   29,
 /*    30 */    30,   29,   30,   33,   34,   35,  200,    7,    8,  238,
 /*    40 */    10,   11,  234,  242,   14,   15,   16,   17,  194,   19,
 /*    50 */    20,   21,   22,   23,   24,  216,    1,  218,  219,   29,
 /*    60 */    30,  261,  223,   33,   34,   35,  227,  261,  229,  230,
 /*    70 */   201,  262,  272,    7,    8,  240,   10,   11,  272,   78,
 /*    80 */    14,   15,   16,   17,   84,   19,   20,   21,   22,   23,
 /*    90 */    24,  261,  194,  258,  238,   29,   30,  261,  242,   33,
 /*   100 */    34,   35,  272,    7,    8,  236,   10,   11,  272,   79,
 /*   110 */    14,   15,   16,   17,   78,   19,   20,   21,   22,   23,
 /*   120 */    24,  267,    1,  269,   74,   29,   30,  194,   80,   33,
 /*   130 */    34,   35,  194,  240,    8,  237,   10,   11,   88,  241,
 /*   140 */    14,   15,   16,   17,  194,   19,   20,   21,   22,   23,
 /*   150 */    24,  258,  116,  203,  118,   29,   30,   78,  194,   33,
 /*   160 */    34,   35,   41,   42,   43,   44,   45,   46,   47,   48,
 /*   170 */    49,   50,   51,   52,   53,   54,   55,   63,   57,   10,
 /*   180 */    11,  199,  240,   14,   15,   16,   17,  205,   19,   20,
 /*   190 */    21,   22,   23,   24,   88,  116,  263,  264,   29,   30,
 /*   200 */   258,  237,   33,   34,   35,  241,  142,  269,  144,  271,
 /*   210 */    96,   97,   98,   99,  100,  101,  102,  103,  104,  105,
 /*   220 */   106,  107,  108,  109,  110,  216,  217,  218,  219,  220,
 /*   230 */   221,  222,  223,  224,  225,  226,  227,  228,  229,  230,
 /*   240 */   231,  139,   78,   40,   21,   22,   23,   24,   84,  261,
 /*   250 */   148,  149,   29,   30,  122,  123,   33,   34,   35,    1,
 /*   260 */    57,  248,    1,    2,    3,    4,    5,   64,    2,    3,
 /*   270 */     4,    5,    3,   70,   71,   72,   73,  113,  194,  266,
 /*   280 */    77,   78,    1,    2,    3,    4,    5,  203,  201,  261,
 /*   290 */    29,   30,   64,  194,   33,   29,   30,    2,    3,    4,
 /*   300 */     5,  261,  194,  194,   58,   59,   60,  194,  261,   64,
 /*   310 */    29,   30,   66,   67,   68,   69,  203,  194,  194,  116,
 /*   320 */   233,  234,  235,  236,   29,   30,   58,   59,   60,   33,
 /*   330 */    34,   35,   86,   65,   66,   67,   68,   69,   80,   78,
 /*   340 */   241,  138,    1,  140,   96,  237,   98,   99,  239,  241,
 /*   350 */   147,  103,   33,    3,    4,  107,  194,  109,  110,   78,
 /*   360 */   237,  237,   29,   30,  241,  241,   33,   34,   35,  141,
 /*   370 */   194,  143,  194,  145,  146,  114,  115,  194,  194,  194,
 /*   380 */   114,  115,  121,   58,   59,   60,  141,  194,  143,   72,
 /*   390 */   145,  146,   67,   68,   69,  114,  115,    3,    4,  237,
 /*   400 */   194,   76,  121,  241,    1,   61,   62,   63,   95,  114,
 /*   410 */   115,   79,  199,  237,  199,  237,   79,  241,  205,  241,
 /*   420 */   205,  237,  237,   95,   83,  241,  241,   58,   59,   60,
 /*   430 */   237,  214,  215,  120,  241,   79,   33,   79,  119,   79,
 /*   440 */    79,   95,   79,   79,   79,   79,    9,  130,   78,  117,
 /*   450 */   261,   95,  269,   95,  117,   95,   95,   79,   95,   95,
 /*   460 */    95,   95,    3,    4,  136,    3,    4,   79,   79,   56,
 /*   470 */    79,  121,  142,   95,  144,  269,  142,  142,  144,  144,
 /*   480 */   134,  261,  112,   95,   95,   78,   95,    3,    4,   74,
 /*   490 */    75,   78,    3,    4,  142,  142,  144,  144,  261,  261,
 /*   500 */   261,  261,  261,  261,  261,  261,  244,  242,  261,  232,
 /*   510 */   261,  261,  261,  261,  261,  121,  261,  261,  111,  244,
 /*   520 */   244,  232,  240,  232,  232,  232,  232,  260,  240,  232,
 /*   530 */   240,  246,  194,  270,  270,   56,  194,  121,  240,  196,
 /*   540 */   257,  194,  194,  194,  265,  265,  135,  196,  194,  194,
 /*   550 */   133,  114,  265,  194,  128,  132,  256,  255,  196,  254,
 /*   560 */   131,  194,  194,  253,  265,  126,  194,  196,  252,  196,
 /*   570 */   125,  251,  124,  250,  127,  249,  247,  137,  196,  194,
 /*   580 */   194,   87,   94,  196,  194,  204,   93,  194,  196,  194,
 /*   590 */   194,  194,  194,  213,  194,  194,  194,   47,   90,  194,
 /*   600 */   194,   92,  194,  204,  194,  201,  194,   51,  194,   91,
 /*   610 */    89,   80,  194,  194,  194,    3,  194,  196,  194,  194,
 /*   620 */   211,  194,  194,    3,  194,    3,  194,  196,  194,    3,
 /*   630 */     3,   98,   97,  117,  112,   78,   78,  194,  194,   79,
 /*   640 */    95,   95,  207,  194,  194,  212,  210,  209,  206,  208,
 /*   650 */   194,  194,  194,  202,  194,  194,  150,  194,  194,  194,
 /*   660 */   150,  215,  196,  196,  194,  194,  194,  194,  194,  194,
 /*   670 */   194,  139,  194,   79,  197,  194,   78,  197,  196,   79,
 /*   680 */   197,  196,   78,  197,  194,  194,  194,   79,   95,  194,
 /*   690 */   194,   78,  194,  196,  194,  196,  194,  194,   79,  198,
 /*   700 */     1,   78,   78,  129,   95,   95,  129,   78,   78,  112,
 /*   710 */    78,   74,  113,   85,   66,   84,    3,    3,   85,   84,
 /*   720 */     5,    3,    3,    3,    3,    3,    3,    3,   81,   74,
 /*   730 */    78,   82,   82,   79,    9,  114,   78,   20,   55,   10,
 /*   740 */   144,   95,   10,    3,    3,  144,  144,  144,   79,    3,
 /*   750 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   760 */     3,    3,    3,   95,   56,   81,    0,  273,  273,  273,
 /*   770 */   273,  273,  273,  273,  273,   15,   15,  273,  273,  273,
 /*   780 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   790 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   800 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   810 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   820 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   830 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   840 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   850 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   860 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   870 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   880 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   890 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   900 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   910 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   920 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   930 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   940 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   950 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   960 */   273,  273,  273,  273,  273,  273,  273,  273,
};
#define YY_SHIFT_COUNT    (365)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (766)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   203,  114,  248,   48,  261,  281,  281,    1,   55,   55,
 /*    10 */    55,   55,   55,   55,   55,   55,   55,   55,   55,   55,
 /*    20 */    55,    0,  121,  281,  266,  295,  295,   79,   79,  132,
 /*    30 */   258,   50,   48,   55,   55,   55,   55,   55,  106,   55,
 /*    40 */    55,  106,  269,  777,  281,  281,  281,  281,  281,  281,
 /*    50 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*    60 */   281,  281,  281,  281,  281,  281,  266,  295,  266,  266,
 /*    70 */    36,    2,    2,    2,    2,    2,    2,    2,  319,   79,
 /*    80 */    79,  317,  317,  313,   79,  341,   55,  479,   55,   55,
 /*    90 */    55,  479,   55,   55,   55,  479,   55,  416,  416,  416,
 /*   100 */   416,  479,   55,   55,  479,  417,  411,  426,  423,  429,
 /*   110 */   439,  445,  448,  447,  440,  479,   55,   55,  479,   55,
 /*   120 */   479,   55,   55,   55,  494,   55,   55,  494,   55,   55,
 /*   130 */    55,   48,   55,   55,   55,   55,   55,   55,   55,   55,
 /*   140 */    55,  479,   55,   55,   55,   55,   55,   55,  479,   55,
 /*   150 */    55,  488,  493,  550,  508,  509,  556,  518,  521,   55,
 /*   160 */    55,  269,   55,   55,   55,   55,   55,   55,   55,   55,
 /*   170 */    55,  479,   55,  479,   55,   55,   55,   55,   55,   55,
 /*   180 */    55,  531,   55,  531,  479,   55,  531,  479,   55,  531,
 /*   190 */    55,   55,   55,   55,  479,   55,   55,  479,   55,   55,
 /*   200 */   777,  777,   30,   66,   66,   96,   66,  126,  169,  223,
 /*   210 */   223,  223,  223,  223,  223,  246,  268,  325,  333,  333,
 /*   220 */   333,  333,  228,  245,  102,  164,  296,  296,  350,  394,
 /*   230 */   344,  369,  356,  332,  337,  358,  360,  361,  328,  346,
 /*   240 */   363,  364,  365,  366,  459,  462,  378,  370,  388,  389,
 /*   250 */   403,  413,  437,  391,   64,  330,  334,  484,  489,  335,
 /*   260 */   352,  407,  353,  415,  612,  506,  620,  622,  510,  626,
 /*   270 */   627,  533,  535,  532,  516,  522,  557,  560,  558,  545,
 /*   280 */   546,  594,  598,  600,  604,  608,  593,  613,  619,  623,
 /*   290 */   699,  624,  609,  574,  610,  577,  629,  522,  630,  597,
 /*   300 */   632,  599,  637,  628,  631,  648,  713,  633,  635,  714,
 /*   310 */   715,  718,  719,  720,  721,  722,  723,  724,  647,  725,
 /*   320 */   655,  649,  650,  652,  654,  621,  658,  717,  683,  729,
 /*   330 */   596,  601,  646,  646,  646,  646,  732,  602,  603,  646,
 /*   340 */   646,  646,  740,  741,  669,  646,  746,  747,  748,  749,
 /*   350 */   750,  751,  752,  753,  754,  755,  756,  757,  758,  759,
 /*   360 */   668,  684,  760,  761,  708,  766,
};
#define YY_REDUCE_COUNT (201)
#define YY_REDUCE_MIN   (-241)
#define YY_REDUCE_MAX   (503)
static const short yy_reduce_ofst[] = {
 /*     0 */  -165,    9, -161,   87, -200, -194, -164,  -67, -102,  -62,
 /*    10 */  -146,  -36,  108,  123,  124,  162,  176,  178,  184,  185,
 /*    20 */   193, -191, -182, -170, -218, -199, -144, -107,  -58,   13,
 /*    30 */  -131,  -18, -192,  183,  206,  109,  -50,   84,  213,  113,
 /*    40 */    99,  215,  217, -241,  -12,   28,   40,   47,  189,  220,
 /*    50 */   237,  238,  239,  240,  241,  242,  243,  244,  247,  249,
 /*    60 */   250,  251,  252,  253,  255,  256,  262,  265,  275,  276,
 /*    70 */   282,  277,  289,  291,  292,  293,  294,  297,  267,  288,
 /*    80 */   290,  263,  264,  285,  298,  338,  342,  343,  347,  348,
 /*    90 */   349,  351,  354,  355,  359,  362,  367,  279,  280,  287,
 /*   100 */   299,  371,  368,  372,  373,  283,  300,  302,  305,  310,
 /*   110 */   316,  320,  323,  326,  329,  382,  385,  386,  387,  390,
 /*   120 */   392,  393,  395,  396,  381,  397,  398,  399,  400,  401,
 /*   130 */   402,  404,  405,  406,  408,  410,  412,  414,  418,  419,
 /*   140 */   420,  421,  422,  424,  425,  427,  428,  430,  431,  432,
 /*   150 */   434,  380,  433,  409,  435,  436,  441,  438,  442,  443,
 /*   160 */   444,  446,  449,  450,  456,  457,  458,  460,  461,  463,
 /*   170 */   464,  466,  465,  467,  470,  471,  472,  473,  474,  475,
 /*   180 */   476,  477,  478,  480,  482,  481,  483,  485,  490,  486,
 /*   190 */   491,  492,  495,  496,  497,  498,  500,  499,  502,  503,
 /*   200 */   451,  501,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   888,  950,  938,  946, 1171, 1171, 1171,  888,  888,  888,
 /*    10 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*    20 */   888, 1078,  908, 1171,  888,  888,  888,  888,  888, 1093,
 /*    30 */  1028,  956,  946,  888,  888,  888,  888,  888,  956,  888,
 /*    40 */   888,  956,  888, 1073,  888,  888,  888,  888,  888,  888,
 /*    50 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*    60 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*    70 */   888,  888,  888,  888,  888,  888,  888,  888, 1080,  888,
 /*    80 */   888, 1112, 1112, 1071,  888,  888,  888,  910,  888,  888,
 /*    90 */  1086,  910, 1083,  888, 1088,  910,  888,  888,  888,  888,
 /*   100 */   888,  910,  888,  888,  910, 1119, 1123, 1105, 1117, 1113,
 /*   110 */  1100, 1098, 1096, 1104, 1127,  910,  888,  888,  910,  888,
 /*   120 */   910,  888,  888,  888,  954,  888,  888,  954,  888,  888,
 /*   130 */   888,  946,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   140 */   888,  910,  888,  888,  888,  888,  888,  888,  910,  888,
 /*   150 */   888,  972,  970,  968,  960,  966,  962,  964,  958,  888,
 /*   160 */   888,  888,  888,  936,  888,  934,  888,  888,  888,  888,
 /*   170 */   888,  910,  888,  910,  888,  888,  888,  888,  888,  888,
 /*   180 */   888,  944,  888,  944,  910,  888,  944,  910,  888,  944,
 /*   190 */   919,  888,  888,  888,  910,  888,  888,  910,  888,  906,
 /*   200 */   994, 1011,  888, 1128, 1118,  888, 1170, 1158, 1157, 1166,
 /*   210 */  1165, 1164, 1156, 1155, 1154,  888,  888,  888, 1150, 1153,
 /*   220 */  1152, 1151,  888,  888,  888,  888, 1160, 1159,  888,  888,
 /*   230 */   888,  888,  888,  888,  888,  888,  888,  888, 1124, 1120,
 /*   240 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   250 */   888, 1130,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   260 */   888, 1019,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   270 */   888,  888,  888,  888, 1070,  888,  888,  888,  888, 1082,
 /*   280 */  1081,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   290 */   888,  888, 1114,  888, 1106,  888,  888, 1031,  888,  888,
 /*   300 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   310 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   320 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   330 */   888,  888, 1189, 1184, 1185, 1182,  888,  888,  888, 1181,
 /*   340 */  1176, 1177,  888,  888,  888, 1174,  888,  888,  888,  888,
 /*   350 */   888,  888,  888,  888,  888,  888,  888,  888,  888,  888,
 /*   360 */   978,  888,  917,  915,  888,  888,
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
  /*  231 */ "vgroups",
  /*  232 */ "signed",
  /*  233 */ "create_table_args",
  /*  234 */ "create_stable_args",
  /*  235 */ "create_table_list",
  /*  236 */ "create_from_stable",
  /*  237 */ "columnlist",
  /*  238 */ "tagitemlist1",
  /*  239 */ "tagNamelist",
  /*  240 */ "select",
  /*  241 */ "column",
  /*  242 */ "tagitem1",
  /*  243 */ "tagitemlist",
  /*  244 */ "tagitem",
  /*  245 */ "selcollist",
  /*  246 */ "from",
  /*  247 */ "where_opt",
  /*  248 */ "interval_option",
  /*  249 */ "sliding_opt",
  /*  250 */ "session_option",
  /*  251 */ "windowstate_option",
  /*  252 */ "fill_opt",
  /*  253 */ "groupby_opt",
  /*  254 */ "having_opt",
  /*  255 */ "orderby_opt",
  /*  256 */ "slimit_opt",
  /*  257 */ "limit_opt",
  /*  258 */ "union",
  /*  259 */ "sclp",
  /*  260 */ "distinct",
  /*  261 */ "expr",
  /*  262 */ "as",
  /*  263 */ "tablelist",
  /*  264 */ "sub",
  /*  265 */ "tmvar",
  /*  266 */ "intervalKey",
  /*  267 */ "sortlist",
  /*  268 */ "sortitem",
  /*  269 */ "item",
  /*  270 */ "sortorder",
  /*  271 */ "grouplist",
  /*  272 */ "expritem",
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
 /*  52 */ "ifexists ::= IF EXISTS",
 /*  53 */ "ifexists ::=",
 /*  54 */ "ifnotexists ::= IF NOT EXISTS",
 /*  55 */ "ifnotexists ::=",
 /*  56 */ "cmd ::= CREATE DNODE ids PORT ids",
 /*  57 */ "cmd ::= CREATE DNODE IPTOKEN PORT ids",
 /*  58 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  59 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  60 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  61 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  62 */ "cmd ::= CREATE USER ids PASS ids",
 /*  63 */ "bufsize ::=",
 /*  64 */ "bufsize ::= BUFSIZE INTEGER",
 /*  65 */ "pps ::=",
 /*  66 */ "pps ::= PPS INTEGER",
 /*  67 */ "tseries ::=",
 /*  68 */ "tseries ::= TSERIES INTEGER",
 /*  69 */ "dbs ::=",
 /*  70 */ "dbs ::= DBS INTEGER",
 /*  71 */ "streams ::=",
 /*  72 */ "streams ::= STREAMS INTEGER",
 /*  73 */ "storage ::=",
 /*  74 */ "storage ::= STORAGE INTEGER",
 /*  75 */ "qtime ::=",
 /*  76 */ "qtime ::= QTIME INTEGER",
 /*  77 */ "users ::=",
 /*  78 */ "users ::= USERS INTEGER",
 /*  79 */ "conns ::=",
 /*  80 */ "conns ::= CONNS INTEGER",
 /*  81 */ "state ::=",
 /*  82 */ "state ::= STATE ids",
 /*  83 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  84 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  85 */ "intitemlist ::= intitem",
 /*  86 */ "intitem ::= INTEGER",
 /*  87 */ "keep ::= KEEP intitemlist",
 /*  88 */ "cache ::= CACHE INTEGER",
 /*  89 */ "replica ::= REPLICA INTEGER",
 /*  90 */ "quorum ::= QUORUM INTEGER",
 /*  91 */ "days ::= DAYS INTEGER",
 /*  92 */ "minrows ::= MINROWS INTEGER",
 /*  93 */ "maxrows ::= MAXROWS INTEGER",
 /*  94 */ "blocks ::= BLOCKS INTEGER",
 /*  95 */ "ctime ::= CTIME INTEGER",
 /*  96 */ "wal ::= WAL INTEGER",
 /*  97 */ "fsync ::= FSYNC INTEGER",
 /*  98 */ "comp ::= COMP INTEGER",
 /*  99 */ "prec ::= PRECISION STRING",
 /* 100 */ "update ::= UPDATE INTEGER",
 /* 101 */ "cachelast ::= CACHELAST INTEGER",
 /* 102 */ "vgroups ::= VGROUPS INTEGER",
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
 /* 119 */ "db_optr ::= db_optr vgroups",
 /* 120 */ "alter_db_optr ::=",
 /* 121 */ "alter_db_optr ::= alter_db_optr replica",
 /* 122 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 123 */ "alter_db_optr ::= alter_db_optr keep",
 /* 124 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 125 */ "alter_db_optr ::= alter_db_optr comp",
 /* 126 */ "alter_db_optr ::= alter_db_optr update",
 /* 127 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 128 */ "typename ::= ids",
 /* 129 */ "typename ::= ids LP signed RP",
 /* 130 */ "typename ::= ids UNSIGNED",
 /* 131 */ "signed ::= INTEGER",
 /* 132 */ "signed ::= PLUS INTEGER",
 /* 133 */ "signed ::= MINUS INTEGER",
 /* 134 */ "cmd ::= CREATE TABLE create_table_args",
 /* 135 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 137 */ "cmd ::= CREATE TABLE create_table_list",
 /* 138 */ "create_table_list ::= create_from_stable",
 /* 139 */ "create_table_list ::= create_table_list create_from_stable",
 /* 140 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 141 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP",
 /* 143 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP",
 /* 144 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 145 */ "tagNamelist ::= ids",
 /* 146 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 147 */ "columnlist ::= columnlist COMMA column",
 /* 148 */ "columnlist ::= column",
 /* 149 */ "column ::= ids typename",
 /* 150 */ "tagitemlist1 ::= tagitemlist1 COMMA tagitem1",
 /* 151 */ "tagitemlist1 ::= tagitem1",
 /* 152 */ "tagitem1 ::= MINUS INTEGER",
 /* 153 */ "tagitem1 ::= MINUS FLOAT",
 /* 154 */ "tagitem1 ::= PLUS INTEGER",
 /* 155 */ "tagitem1 ::= PLUS FLOAT",
 /* 156 */ "tagitem1 ::= INTEGER",
 /* 157 */ "tagitem1 ::= FLOAT",
 /* 158 */ "tagitem1 ::= STRING",
 /* 159 */ "tagitem1 ::= BOOL",
 /* 160 */ "tagitem1 ::= NULL",
 /* 161 */ "tagitem1 ::= NOW",
 /* 162 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 163 */ "tagitemlist ::= tagitem",
 /* 164 */ "tagitem ::= INTEGER",
 /* 165 */ "tagitem ::= FLOAT",
 /* 166 */ "tagitem ::= STRING",
 /* 167 */ "tagitem ::= BOOL",
 /* 168 */ "tagitem ::= NULL",
 /* 169 */ "tagitem ::= NOW",
 /* 170 */ "tagitem ::= MINUS INTEGER",
 /* 171 */ "tagitem ::= MINUS FLOAT",
 /* 172 */ "tagitem ::= PLUS INTEGER",
 /* 173 */ "tagitem ::= PLUS FLOAT",
 /* 174 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 175 */ "select ::= LP select RP",
 /* 176 */ "union ::= select",
 /* 177 */ "union ::= union UNION ALL select",
 /* 178 */ "union ::= union UNION select",
 /* 179 */ "cmd ::= union",
 /* 180 */ "select ::= SELECT selcollist",
 /* 181 */ "sclp ::= selcollist COMMA",
 /* 182 */ "sclp ::=",
 /* 183 */ "selcollist ::= sclp distinct expr as",
 /* 184 */ "selcollist ::= sclp STAR",
 /* 185 */ "as ::= AS ids",
 /* 186 */ "as ::= ids",
 /* 187 */ "as ::=",
 /* 188 */ "distinct ::= DISTINCT",
 /* 189 */ "distinct ::=",
 /* 190 */ "from ::= FROM tablelist",
 /* 191 */ "from ::= FROM sub",
 /* 192 */ "sub ::= LP union RP",
 /* 193 */ "sub ::= LP union RP ids",
 /* 194 */ "sub ::= sub COMMA LP union RP ids",
 /* 195 */ "tablelist ::= ids cpxName",
 /* 196 */ "tablelist ::= ids cpxName ids",
 /* 197 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 198 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 199 */ "tmvar ::= VARIABLE",
 /* 200 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 201 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 202 */ "interval_option ::=",
 /* 203 */ "intervalKey ::= INTERVAL",
 /* 204 */ "intervalKey ::= EVERY",
 /* 205 */ "session_option ::=",
 /* 206 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 207 */ "windowstate_option ::=",
 /* 208 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 209 */ "fill_opt ::=",
 /* 210 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 211 */ "fill_opt ::= FILL LP ID RP",
 /* 212 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 213 */ "sliding_opt ::=",
 /* 214 */ "orderby_opt ::=",
 /* 215 */ "orderby_opt ::= ORDER BY sortlist",
 /* 216 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 217 */ "sortlist ::= item sortorder",
 /* 218 */ "item ::= ids cpxName",
 /* 219 */ "sortorder ::= ASC",
 /* 220 */ "sortorder ::= DESC",
 /* 221 */ "sortorder ::=",
 /* 222 */ "groupby_opt ::=",
 /* 223 */ "groupby_opt ::= GROUP BY grouplist",
 /* 224 */ "grouplist ::= grouplist COMMA item",
 /* 225 */ "grouplist ::= item",
 /* 226 */ "having_opt ::=",
 /* 227 */ "having_opt ::= HAVING expr",
 /* 228 */ "limit_opt ::=",
 /* 229 */ "limit_opt ::= LIMIT signed",
 /* 230 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 231 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 232 */ "slimit_opt ::=",
 /* 233 */ "slimit_opt ::= SLIMIT signed",
 /* 234 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 235 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 236 */ "where_opt ::=",
 /* 237 */ "where_opt ::= WHERE expr",
 /* 238 */ "expr ::= LP expr RP",
 /* 239 */ "expr ::= ID",
 /* 240 */ "expr ::= ID DOT ID",
 /* 241 */ "expr ::= ID DOT STAR",
 /* 242 */ "expr ::= INTEGER",
 /* 243 */ "expr ::= MINUS INTEGER",
 /* 244 */ "expr ::= PLUS INTEGER",
 /* 245 */ "expr ::= FLOAT",
 /* 246 */ "expr ::= MINUS FLOAT",
 /* 247 */ "expr ::= PLUS FLOAT",
 /* 248 */ "expr ::= STRING",
 /* 249 */ "expr ::= NOW",
 /* 250 */ "expr ::= VARIABLE",
 /* 251 */ "expr ::= PLUS VARIABLE",
 /* 252 */ "expr ::= MINUS VARIABLE",
 /* 253 */ "expr ::= BOOL",
 /* 254 */ "expr ::= NULL",
 /* 255 */ "expr ::= ID LP exprlist RP",
 /* 256 */ "expr ::= ID LP STAR RP",
 /* 257 */ "expr ::= expr IS NULL",
 /* 258 */ "expr ::= expr IS NOT NULL",
 /* 259 */ "expr ::= expr LT expr",
 /* 260 */ "expr ::= expr GT expr",
 /* 261 */ "expr ::= expr LE expr",
 /* 262 */ "expr ::= expr GE expr",
 /* 263 */ "expr ::= expr NE expr",
 /* 264 */ "expr ::= expr EQ expr",
 /* 265 */ "expr ::= expr BETWEEN expr AND expr",
 /* 266 */ "expr ::= expr AND expr",
 /* 267 */ "expr ::= expr OR expr",
 /* 268 */ "expr ::= expr PLUS expr",
 /* 269 */ "expr ::= expr MINUS expr",
 /* 270 */ "expr ::= expr STAR expr",
 /* 271 */ "expr ::= expr SLASH expr",
 /* 272 */ "expr ::= expr REM expr",
 /* 273 */ "expr ::= expr LIKE expr",
 /* 274 */ "expr ::= expr MATCH expr",
 /* 275 */ "expr ::= expr NMATCH expr",
 /* 276 */ "expr ::= expr IN LP exprlist RP",
 /* 277 */ "exprlist ::= exprlist COMMA expritem",
 /* 278 */ "exprlist ::= expritem",
 /* 279 */ "expritem ::= expr",
 /* 280 */ "expritem ::=",
 /* 281 */ "cmd ::= RESET QUERY CACHE",
 /* 282 */ "cmd ::= SYNCDB ids REPLICA",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 286 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 287 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 288 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 289 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 290 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 294 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 295 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 296 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 297 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 298 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 299 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 300 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 301 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 245: /* selcollist */
    case 259: /* sclp */
{
tSqlExprListDestroy((yypminor->yy165));
}
      break;
    case 214: /* intitemlist */
    case 216: /* keep */
    case 237: /* columnlist */
    case 238: /* tagitemlist1 */
    case 239: /* tagNamelist */
    case 243: /* tagitemlist */
    case 252: /* fill_opt */
    case 253: /* groupby_opt */
    case 255: /* orderby_opt */
    case 267: /* sortlist */
    case 271: /* grouplist */
{
taosArrayDestroy((yypminor->yy165));
}
      break;
    case 235: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy326));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy278));
}
      break;
    case 246: /* from */
    case 263: /* tablelist */
    case 264: /* sub */
{
destroyRelationInfo((yypminor->yy10));
}
      break;
    case 247: /* where_opt */
    case 254: /* having_opt */
    case 261: /* expr */
    case 272: /* expritem */
{
tSqlExprDestroy((yypminor->yy202));
}
      break;
    case 258: /* union */
{
destroyAllSqlNode((yypminor->yy503));
}
      break;
    case 268: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy425));
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
  {  197,   -2 }, /* (52) ifexists ::= IF EXISTS */
  {  197,    0 }, /* (53) ifexists ::= */
  {  201,   -3 }, /* (54) ifnotexists ::= IF NOT EXISTS */
  {  201,    0 }, /* (55) ifnotexists ::= */
  {  193,   -5 }, /* (56) cmd ::= CREATE DNODE ids PORT ids */
  {  193,   -5 }, /* (57) cmd ::= CREATE DNODE IPTOKEN PORT ids */
  {  193,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  193,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  193,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  193,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  204,    0 }, /* (63) bufsize ::= */
  {  204,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  205,    0 }, /* (65) pps ::= */
  {  205,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  206,    0 }, /* (67) tseries ::= */
  {  206,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  207,    0 }, /* (69) dbs ::= */
  {  207,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  208,    0 }, /* (71) streams ::= */
  {  208,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  209,    0 }, /* (73) storage ::= */
  {  209,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  210,    0 }, /* (75) qtime ::= */
  {  210,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  211,    0 }, /* (77) users ::= */
  {  211,   -2 }, /* (78) users ::= USERS INTEGER */
  {  212,    0 }, /* (79) conns ::= */
  {  212,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  213,    0 }, /* (81) state ::= */
  {  213,   -2 }, /* (82) state ::= STATE ids */
  {  199,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  214,   -3 }, /* (84) intitemlist ::= intitemlist COMMA intitem */
  {  214,   -1 }, /* (85) intitemlist ::= intitem */
  {  215,   -1 }, /* (86) intitem ::= INTEGER */
  {  216,   -2 }, /* (87) keep ::= KEEP intitemlist */
  {  217,   -2 }, /* (88) cache ::= CACHE INTEGER */
  {  218,   -2 }, /* (89) replica ::= REPLICA INTEGER */
  {  219,   -2 }, /* (90) quorum ::= QUORUM INTEGER */
  {  220,   -2 }, /* (91) days ::= DAYS INTEGER */
  {  221,   -2 }, /* (92) minrows ::= MINROWS INTEGER */
  {  222,   -2 }, /* (93) maxrows ::= MAXROWS INTEGER */
  {  223,   -2 }, /* (94) blocks ::= BLOCKS INTEGER */
  {  224,   -2 }, /* (95) ctime ::= CTIME INTEGER */
  {  225,   -2 }, /* (96) wal ::= WAL INTEGER */
  {  226,   -2 }, /* (97) fsync ::= FSYNC INTEGER */
  {  227,   -2 }, /* (98) comp ::= COMP INTEGER */
  {  228,   -2 }, /* (99) prec ::= PRECISION STRING */
  {  229,   -2 }, /* (100) update ::= UPDATE INTEGER */
  {  230,   -2 }, /* (101) cachelast ::= CACHELAST INTEGER */
  {  231,   -2 }, /* (102) vgroups ::= VGROUPS INTEGER */
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
  {  202,   -2 }, /* (119) db_optr ::= db_optr vgroups */
  {  198,    0 }, /* (120) alter_db_optr ::= */
  {  198,   -2 }, /* (121) alter_db_optr ::= alter_db_optr replica */
  {  198,   -2 }, /* (122) alter_db_optr ::= alter_db_optr quorum */
  {  198,   -2 }, /* (123) alter_db_optr ::= alter_db_optr keep */
  {  198,   -2 }, /* (124) alter_db_optr ::= alter_db_optr blocks */
  {  198,   -2 }, /* (125) alter_db_optr ::= alter_db_optr comp */
  {  198,   -2 }, /* (126) alter_db_optr ::= alter_db_optr update */
  {  198,   -2 }, /* (127) alter_db_optr ::= alter_db_optr cachelast */
  {  203,   -1 }, /* (128) typename ::= ids */
  {  203,   -4 }, /* (129) typename ::= ids LP signed RP */
  {  203,   -2 }, /* (130) typename ::= ids UNSIGNED */
  {  232,   -1 }, /* (131) signed ::= INTEGER */
  {  232,   -2 }, /* (132) signed ::= PLUS INTEGER */
  {  232,   -2 }, /* (133) signed ::= MINUS INTEGER */
  {  193,   -3 }, /* (134) cmd ::= CREATE TABLE create_table_args */
  {  193,   -3 }, /* (135) cmd ::= CREATE TABLE create_stable_args */
  {  193,   -3 }, /* (136) cmd ::= CREATE STABLE create_stable_args */
  {  193,   -3 }, /* (137) cmd ::= CREATE TABLE create_table_list */
  {  235,   -1 }, /* (138) create_table_list ::= create_from_stable */
  {  235,   -2 }, /* (139) create_table_list ::= create_table_list create_from_stable */
  {  233,   -6 }, /* (140) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  234,  -10 }, /* (141) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  236,  -10 }, /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
  {  236,  -13 }, /* (143) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
  {  239,   -3 }, /* (144) tagNamelist ::= tagNamelist COMMA ids */
  {  239,   -1 }, /* (145) tagNamelist ::= ids */
  {  233,   -5 }, /* (146) create_table_args ::= ifnotexists ids cpxName AS select */
  {  237,   -3 }, /* (147) columnlist ::= columnlist COMMA column */
  {  237,   -1 }, /* (148) columnlist ::= column */
  {  241,   -2 }, /* (149) column ::= ids typename */
  {  238,   -3 }, /* (150) tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
  {  238,   -1 }, /* (151) tagitemlist1 ::= tagitem1 */
  {  242,   -2 }, /* (152) tagitem1 ::= MINUS INTEGER */
  {  242,   -2 }, /* (153) tagitem1 ::= MINUS FLOAT */
  {  242,   -2 }, /* (154) tagitem1 ::= PLUS INTEGER */
  {  242,   -2 }, /* (155) tagitem1 ::= PLUS FLOAT */
  {  242,   -1 }, /* (156) tagitem1 ::= INTEGER */
  {  242,   -1 }, /* (157) tagitem1 ::= FLOAT */
  {  242,   -1 }, /* (158) tagitem1 ::= STRING */
  {  242,   -1 }, /* (159) tagitem1 ::= BOOL */
  {  242,   -1 }, /* (160) tagitem1 ::= NULL */
  {  242,   -1 }, /* (161) tagitem1 ::= NOW */
  {  243,   -3 }, /* (162) tagitemlist ::= tagitemlist COMMA tagitem */
  {  243,   -1 }, /* (163) tagitemlist ::= tagitem */
  {  244,   -1 }, /* (164) tagitem ::= INTEGER */
  {  244,   -1 }, /* (165) tagitem ::= FLOAT */
  {  244,   -1 }, /* (166) tagitem ::= STRING */
  {  244,   -1 }, /* (167) tagitem ::= BOOL */
  {  244,   -1 }, /* (168) tagitem ::= NULL */
  {  244,   -1 }, /* (169) tagitem ::= NOW */
  {  244,   -2 }, /* (170) tagitem ::= MINUS INTEGER */
  {  244,   -2 }, /* (171) tagitem ::= MINUS FLOAT */
  {  244,   -2 }, /* (172) tagitem ::= PLUS INTEGER */
  {  244,   -2 }, /* (173) tagitem ::= PLUS FLOAT */
  {  240,  -14 }, /* (174) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  240,   -3 }, /* (175) select ::= LP select RP */
  {  258,   -1 }, /* (176) union ::= select */
  {  258,   -4 }, /* (177) union ::= union UNION ALL select */
  {  258,   -3 }, /* (178) union ::= union UNION select */
  {  193,   -1 }, /* (179) cmd ::= union */
  {  240,   -2 }, /* (180) select ::= SELECT selcollist */
  {  259,   -2 }, /* (181) sclp ::= selcollist COMMA */
  {  259,    0 }, /* (182) sclp ::= */
  {  245,   -4 }, /* (183) selcollist ::= sclp distinct expr as */
  {  245,   -2 }, /* (184) selcollist ::= sclp STAR */
  {  262,   -2 }, /* (185) as ::= AS ids */
  {  262,   -1 }, /* (186) as ::= ids */
  {  262,    0 }, /* (187) as ::= */
  {  260,   -1 }, /* (188) distinct ::= DISTINCT */
  {  260,    0 }, /* (189) distinct ::= */
  {  246,   -2 }, /* (190) from ::= FROM tablelist */
  {  246,   -2 }, /* (191) from ::= FROM sub */
  {  264,   -3 }, /* (192) sub ::= LP union RP */
  {  264,   -4 }, /* (193) sub ::= LP union RP ids */
  {  264,   -6 }, /* (194) sub ::= sub COMMA LP union RP ids */
  {  263,   -2 }, /* (195) tablelist ::= ids cpxName */
  {  263,   -3 }, /* (196) tablelist ::= ids cpxName ids */
  {  263,   -4 }, /* (197) tablelist ::= tablelist COMMA ids cpxName */
  {  263,   -5 }, /* (198) tablelist ::= tablelist COMMA ids cpxName ids */
  {  265,   -1 }, /* (199) tmvar ::= VARIABLE */
  {  248,   -4 }, /* (200) interval_option ::= intervalKey LP tmvar RP */
  {  248,   -6 }, /* (201) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  248,    0 }, /* (202) interval_option ::= */
  {  266,   -1 }, /* (203) intervalKey ::= INTERVAL */
  {  266,   -1 }, /* (204) intervalKey ::= EVERY */
  {  250,    0 }, /* (205) session_option ::= */
  {  250,   -7 }, /* (206) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  251,    0 }, /* (207) windowstate_option ::= */
  {  251,   -4 }, /* (208) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  252,    0 }, /* (209) fill_opt ::= */
  {  252,   -6 }, /* (210) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  252,   -4 }, /* (211) fill_opt ::= FILL LP ID RP */
  {  249,   -4 }, /* (212) sliding_opt ::= SLIDING LP tmvar RP */
  {  249,    0 }, /* (213) sliding_opt ::= */
  {  255,    0 }, /* (214) orderby_opt ::= */
  {  255,   -3 }, /* (215) orderby_opt ::= ORDER BY sortlist */
  {  267,   -4 }, /* (216) sortlist ::= sortlist COMMA item sortorder */
  {  267,   -2 }, /* (217) sortlist ::= item sortorder */
  {  269,   -2 }, /* (218) item ::= ids cpxName */
  {  270,   -1 }, /* (219) sortorder ::= ASC */
  {  270,   -1 }, /* (220) sortorder ::= DESC */
  {  270,    0 }, /* (221) sortorder ::= */
  {  253,    0 }, /* (222) groupby_opt ::= */
  {  253,   -3 }, /* (223) groupby_opt ::= GROUP BY grouplist */
  {  271,   -3 }, /* (224) grouplist ::= grouplist COMMA item */
  {  271,   -1 }, /* (225) grouplist ::= item */
  {  254,    0 }, /* (226) having_opt ::= */
  {  254,   -2 }, /* (227) having_opt ::= HAVING expr */
  {  257,    0 }, /* (228) limit_opt ::= */
  {  257,   -2 }, /* (229) limit_opt ::= LIMIT signed */
  {  257,   -4 }, /* (230) limit_opt ::= LIMIT signed OFFSET signed */
  {  257,   -4 }, /* (231) limit_opt ::= LIMIT signed COMMA signed */
  {  256,    0 }, /* (232) slimit_opt ::= */
  {  256,   -2 }, /* (233) slimit_opt ::= SLIMIT signed */
  {  256,   -4 }, /* (234) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  256,   -4 }, /* (235) slimit_opt ::= SLIMIT signed COMMA signed */
  {  247,    0 }, /* (236) where_opt ::= */
  {  247,   -2 }, /* (237) where_opt ::= WHERE expr */
  {  261,   -3 }, /* (238) expr ::= LP expr RP */
  {  261,   -1 }, /* (239) expr ::= ID */
  {  261,   -3 }, /* (240) expr ::= ID DOT ID */
  {  261,   -3 }, /* (241) expr ::= ID DOT STAR */
  {  261,   -1 }, /* (242) expr ::= INTEGER */
  {  261,   -2 }, /* (243) expr ::= MINUS INTEGER */
  {  261,   -2 }, /* (244) expr ::= PLUS INTEGER */
  {  261,   -1 }, /* (245) expr ::= FLOAT */
  {  261,   -2 }, /* (246) expr ::= MINUS FLOAT */
  {  261,   -2 }, /* (247) expr ::= PLUS FLOAT */
  {  261,   -1 }, /* (248) expr ::= STRING */
  {  261,   -1 }, /* (249) expr ::= NOW */
  {  261,   -1 }, /* (250) expr ::= VARIABLE */
  {  261,   -2 }, /* (251) expr ::= PLUS VARIABLE */
  {  261,   -2 }, /* (252) expr ::= MINUS VARIABLE */
  {  261,   -1 }, /* (253) expr ::= BOOL */
  {  261,   -1 }, /* (254) expr ::= NULL */
  {  261,   -4 }, /* (255) expr ::= ID LP exprlist RP */
  {  261,   -4 }, /* (256) expr ::= ID LP STAR RP */
  {  261,   -3 }, /* (257) expr ::= expr IS NULL */
  {  261,   -4 }, /* (258) expr ::= expr IS NOT NULL */
  {  261,   -3 }, /* (259) expr ::= expr LT expr */
  {  261,   -3 }, /* (260) expr ::= expr GT expr */
  {  261,   -3 }, /* (261) expr ::= expr LE expr */
  {  261,   -3 }, /* (262) expr ::= expr GE expr */
  {  261,   -3 }, /* (263) expr ::= expr NE expr */
  {  261,   -3 }, /* (264) expr ::= expr EQ expr */
  {  261,   -5 }, /* (265) expr ::= expr BETWEEN expr AND expr */
  {  261,   -3 }, /* (266) expr ::= expr AND expr */
  {  261,   -3 }, /* (267) expr ::= expr OR expr */
  {  261,   -3 }, /* (268) expr ::= expr PLUS expr */
  {  261,   -3 }, /* (269) expr ::= expr MINUS expr */
  {  261,   -3 }, /* (270) expr ::= expr STAR expr */
  {  261,   -3 }, /* (271) expr ::= expr SLASH expr */
  {  261,   -3 }, /* (272) expr ::= expr REM expr */
  {  261,   -3 }, /* (273) expr ::= expr LIKE expr */
  {  261,   -3 }, /* (274) expr ::= expr MATCH expr */
  {  261,   -3 }, /* (275) expr ::= expr NMATCH expr */
  {  261,   -5 }, /* (276) expr ::= expr IN LP exprlist RP */
  {  200,   -3 }, /* (277) exprlist ::= exprlist COMMA expritem */
  {  200,   -1 }, /* (278) exprlist ::= expritem */
  {  272,   -1 }, /* (279) expritem ::= expr */
  {  272,    0 }, /* (280) expritem ::= */
  {  193,   -3 }, /* (281) cmd ::= RESET QUERY CACHE */
  {  193,   -3 }, /* (282) cmd ::= SYNCDB ids REPLICA */
  {  193,   -7 }, /* (283) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (284) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (286) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (287) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (288) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (289) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (290) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -7 }, /* (291) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  193,   -7 }, /* (292) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  193,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  193,   -7 }, /* (294) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  193,   -7 }, /* (295) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  193,   -8 }, /* (296) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  193,   -9 }, /* (297) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  193,   -7 }, /* (298) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  193,   -3 }, /* (299) cmd ::= KILL CONNECTION INTEGER */
  {  193,   -5 }, /* (300) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  193,   -5 }, /* (301) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 134: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==135);
      case 136: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==136);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNC, 0, 0);}
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
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TRANS, 0, 0);   }
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
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy16, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy211);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy165);}
        break;
      case 51: /* ids ::= ID */
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 52: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 53: /* ifexists ::= */
      case 55: /* ifnotexists ::= */ yytestcase(yyruleno==55);
      case 189: /* distinct ::= */ yytestcase(yyruleno==189);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 54: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 56: /* cmd ::= CREATE DNODE ids PORT ids */
      case 57: /* cmd ::= CREATE DNODE IPTOKEN PORT ids */ yytestcase(yyruleno==57);
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy16, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy106, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy106, &yymsp[0].minor.yy0, 2);}
        break;
      case 62: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 63: /* bufsize ::= */
      case 65: /* pps ::= */ yytestcase(yyruleno==65);
      case 67: /* tseries ::= */ yytestcase(yyruleno==67);
      case 69: /* dbs ::= */ yytestcase(yyruleno==69);
      case 71: /* streams ::= */ yytestcase(yyruleno==71);
      case 73: /* storage ::= */ yytestcase(yyruleno==73);
      case 75: /* qtime ::= */ yytestcase(yyruleno==75);
      case 77: /* users ::= */ yytestcase(yyruleno==77);
      case 79: /* conns ::= */ yytestcase(yyruleno==79);
      case 81: /* state ::= */ yytestcase(yyruleno==81);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 64: /* bufsize ::= BUFSIZE INTEGER */
      case 66: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==68);
      case 70: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==70);
      case 72: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==74);
      case 76: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==76);
      case 78: /* users ::= USERS INTEGER */ yytestcase(yyruleno==78);
      case 80: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==80);
      case 82: /* state ::= STATE ids */ yytestcase(yyruleno==82);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 83: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy211.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy211.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy211.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy211.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy211.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy211.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy211.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy211 = yylhsminor.yy211;
        break;
      case 84: /* intitemlist ::= intitemlist COMMA intitem */
      case 162: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==162);
{ yylhsminor.yy165 = tListItemAppend(yymsp[-2].minor.yy165, &yymsp[0].minor.yy425, -1);    }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 85: /* intitemlist ::= intitem */
      case 163: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==163);
{ yylhsminor.yy165 = tListItemAppend(NULL, &yymsp[0].minor.yy425, -1); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 86: /* intitem ::= INTEGER */
      case 164: /* tagitem ::= INTEGER */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= FLOAT */ yytestcase(yyruleno==165);
      case 166: /* tagitem ::= STRING */ yytestcase(yyruleno==166);
      case 167: /* tagitem ::= BOOL */ yytestcase(yyruleno==167);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy425, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy425 = yylhsminor.yy425;
        break;
      case 87: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy165 = yymsp[0].minor.yy165; }
        break;
      case 88: /* cache ::= CACHE INTEGER */
      case 89: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==89);
      case 90: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==90);
      case 91: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==91);
      case 92: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==92);
      case 93: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==95);
      case 96: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==96);
      case 97: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==97);
      case 98: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==98);
      case 99: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==99);
      case 100: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==100);
      case 101: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==101);
      case 102: /* vgroups ::= VGROUPS INTEGER */ yytestcase(yyruleno==102);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 103: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy16);}
        break;
      case 104: /* db_optr ::= db_optr cache */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 105: /* db_optr ::= db_optr replica */
      case 121: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==121);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 106: /* db_optr ::= db_optr quorum */
      case 122: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==122);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 107: /* db_optr ::= db_optr days */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 108: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 109: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 110: /* db_optr ::= db_optr blocks */
      case 124: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==124);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 111: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 112: /* db_optr ::= db_optr wal */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 113: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 114: /* db_optr ::= db_optr comp */
      case 125: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==125);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 115: /* db_optr ::= db_optr prec */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 116: /* db_optr ::= db_optr keep */
      case 123: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==123);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.keep = yymsp[0].minor.yy165; }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 117: /* db_optr ::= db_optr update */
      case 126: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==126);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 118: /* db_optr ::= db_optr cachelast */
      case 127: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==127);
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 119: /* db_optr ::= db_optr vgroups */
{ yylhsminor.yy16 = yymsp[-1].minor.yy16; yylhsminor.yy16.numOfVgroups = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy16 = yylhsminor.yy16;
        break;
      case 120: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy16);}
        break;
      case 128: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy106, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy106 = yylhsminor.yy106;
        break;
      case 129: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy207 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy106, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy207;  // negative value of name length
    tSetColumnType(&yylhsminor.yy106, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy106 = yylhsminor.yy106;
        break;
      case 130: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy106, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy106 = yylhsminor.yy106;
        break;
      case 131: /* signed ::= INTEGER */
{ yylhsminor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 132: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 133: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy207 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 137: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy326;}
        break;
      case 138: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy150);
  pCreateTable->type = TSDB_SQL_CREATE_TABLE;
  yylhsminor.yy326 = pCreateTable;
}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 139: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy326->childTableInfo, &yymsp[0].minor.yy150);
  yylhsminor.yy326 = yymsp[-1].minor.yy326;
}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 140: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy326 = tSetCreateTableInfo(yymsp[-1].minor.yy165, NULL, NULL, TSDB_SQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy326, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy326 = yylhsminor.yy326;
        break;
      case 141: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy326 = tSetCreateTableInfo(yymsp[-5].minor.yy165, yymsp[-1].minor.yy165, NULL, TSDB_SQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy326, NULL, TSDB_SQL_CREATE_STABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy326 = yylhsminor.yy326;
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist1 RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy150 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy165, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy150 = yylhsminor.yy150;
        break;
      case 143: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist1 RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy150 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy165, yymsp[-1].minor.yy165, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy150 = yylhsminor.yy150;
        break;
      case 144: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy0); yylhsminor.yy165 = yymsp[-2].minor.yy165;  }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 145: /* tagNamelist ::= ids */
{yylhsminor.yy165 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 146: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
//  yylhsminor.yy326 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy278, TSQL_CREATE_STREAM);
//  setSqlInfo(pInfo, yylhsminor.yy326, NULL, TSDB_SQL_CREATE_TABLE);
//
//  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
//  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy326 = yylhsminor.yy326;
        break;
      case 147: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy106); yylhsminor.yy165 = yymsp[-2].minor.yy165;  }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 148: /* columnlist ::= column */
{yylhsminor.yy165 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy106);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 149: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy106, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy106);
}
  yymsp[-1].minor.yy106 = yylhsminor.yy106;
        break;
      case 150: /* tagitemlist1 ::= tagitemlist1 COMMA tagitem1 */
{ taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy0); yylhsminor.yy165 = yymsp[-2].minor.yy165;}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 151: /* tagitemlist1 ::= tagitem1 */
{ yylhsminor.yy165 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 152: /* tagitem1 ::= MINUS INTEGER */
      case 153: /* tagitem1 ::= MINUS FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem1 ::= PLUS INTEGER */ yytestcase(yyruleno==154);
      case 155: /* tagitem1 ::= PLUS FLOAT */ yytestcase(yyruleno==155);
{ yylhsminor.yy0.n = yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n; yylhsminor.yy0.type = yymsp[0].minor.yy0.type; }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 156: /* tagitem1 ::= INTEGER */
      case 157: /* tagitem1 ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem1 ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem1 ::= BOOL */ yytestcase(yyruleno==159);
      case 160: /* tagitem1 ::= NULL */ yytestcase(yyruleno==160);
      case 161: /* tagitem1 ::= NOW */ yytestcase(yyruleno==161);
{ yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 168: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy425, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy425 = yylhsminor.yy425;
        break;
      case 169: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy425, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy425 = yylhsminor.yy425;
        break;
      case 170: /* tagitem ::= MINUS INTEGER */
      case 171: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==171);
      case 172: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==172);
      case 173: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==173);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy425, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy425 = yylhsminor.yy425;
        break;
      case 174: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy278 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy165, yymsp[-11].minor.yy10, yymsp[-10].minor.yy202, yymsp[-4].minor.yy165, yymsp[-2].minor.yy165, &yymsp[-9].minor.yy532, &yymsp[-7].minor.yy97, &yymsp[-6].minor.yy6, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy165, &yymsp[0].minor.yy367, &yymsp[-1].minor.yy367, yymsp[-3].minor.yy202);
}
  yymsp[-13].minor.yy278 = yylhsminor.yy278;
        break;
      case 175: /* select ::= LP select RP */
{yymsp[-2].minor.yy278 = yymsp[-1].minor.yy278;}
        break;
      case 176: /* union ::= select */
{ yylhsminor.yy503 = setSubclause(NULL, yymsp[0].minor.yy278); }
  yymsp[0].minor.yy503 = yylhsminor.yy503;
        break;
      case 177: /* union ::= union UNION ALL select */
{ yylhsminor.yy503 = appendSelectClause(yymsp[-3].minor.yy503, SQL_TYPE_UNIONALL, yymsp[0].minor.yy278);  }
  yymsp[-3].minor.yy503 = yylhsminor.yy503;
        break;
      case 178: /* union ::= union UNION select */
{ yylhsminor.yy503 = appendSelectClause(yymsp[-2].minor.yy503, SQL_TYPE_UNION, yymsp[0].minor.yy278);  }
  yymsp[-2].minor.yy503 = yylhsminor.yy503;
        break;
      case 179: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy503, NULL, TSDB_SQL_SELECT); }
        break;
      case 180: /* select ::= SELECT selcollist */
{
  yylhsminor.yy278 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy165, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy278 = yylhsminor.yy278;
        break;
      case 181: /* sclp ::= selcollist COMMA */
{yylhsminor.yy165 = yymsp[-1].minor.yy165;}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 182: /* sclp ::= */
      case 214: /* orderby_opt ::= */ yytestcase(yyruleno==214);
{yymsp[1].minor.yy165 = 0;}
        break;
      case 183: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy165 = tSqlExprListAppend(yymsp[-3].minor.yy165, yymsp[-1].minor.yy202,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy165 = yylhsminor.yy165;
        break;
      case 184: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy165 = tSqlExprListAppend(yymsp[-1].minor.yy165, pNode, 0, 0);
}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 185: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 186: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 187: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 188: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 190: /* from ::= FROM tablelist */
      case 191: /* from ::= FROM sub */ yytestcase(yyruleno==191);
{yymsp[-1].minor.yy10 = yymsp[0].minor.yy10;}
        break;
      case 192: /* sub ::= LP union RP */
{yymsp[-2].minor.yy10 = addSubquery(NULL, yymsp[-1].minor.yy503, NULL);}
        break;
      case 193: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy10 = addSubquery(NULL, yymsp[-2].minor.yy503, &yymsp[0].minor.yy0);}
        break;
      case 194: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy10 = addSubquery(yymsp[-5].minor.yy10, yymsp[-2].minor.yy503, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy10 = yylhsminor.yy10;
        break;
      case 195: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 196: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy10 = yylhsminor.yy10;
        break;
      case 197: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(yymsp[-3].minor.yy10, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy10 = yylhsminor.yy10;
        break;
      case 198: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy10 = setTableNameList(yymsp[-4].minor.yy10, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy10 = yylhsminor.yy10;
        break;
      case 199: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 200: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy532.interval = yymsp[-1].minor.yy0; yylhsminor.yy532.offset.n = 0; yylhsminor.yy532.token = yymsp[-3].minor.yy46;}
  yymsp[-3].minor.yy532 = yylhsminor.yy532;
        break;
      case 201: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy532.interval = yymsp[-3].minor.yy0; yylhsminor.yy532.offset = yymsp[-1].minor.yy0;   yylhsminor.yy532.token = yymsp[-5].minor.yy46;}
  yymsp[-5].minor.yy532 = yylhsminor.yy532;
        break;
      case 202: /* interval_option ::= */
{memset(&yymsp[1].minor.yy532, 0, sizeof(yymsp[1].minor.yy532));}
        break;
      case 203: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy46 = TK_INTERVAL;}
        break;
      case 204: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy46 = TK_EVERY;   }
        break;
      case 205: /* session_option ::= */
{yymsp[1].minor.yy97.col.n = 0; yymsp[1].minor.yy97.gap.n = 0;}
        break;
      case 206: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy97.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy97.gap = yymsp[-1].minor.yy0;
}
        break;
      case 207: /* windowstate_option ::= */
{ yymsp[1].minor.yy6.col.n = 0; yymsp[1].minor.yy6.col.z = NULL;}
        break;
      case 208: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy6.col = yymsp[-1].minor.yy0; }
        break;
      case 209: /* fill_opt ::= */
{ yymsp[1].minor.yy165 = 0;     }
        break;
      case 210: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy165, &A, -1, 0);
    yymsp[-5].minor.yy165 = yymsp[-1].minor.yy165;
}
        break;
      case 211: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy165 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 212: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 213: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 215: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy165 = yymsp[0].minor.yy165;}
        break;
      case 216: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy165 = tListItemAppend(yymsp[-3].minor.yy165, &yymsp[-1].minor.yy425, yymsp[0].minor.yy47);
}
  yymsp[-3].minor.yy165 = yylhsminor.yy165;
        break;
      case 217: /* sortlist ::= item sortorder */
{
  yylhsminor.yy165 = tListItemAppend(NULL, &yymsp[-1].minor.yy425, yymsp[0].minor.yy47);
}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 218: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy425, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy425 = yylhsminor.yy425;
        break;
      case 219: /* sortorder ::= ASC */
{ yymsp[0].minor.yy47 = TSDB_ORDER_ASC; }
        break;
      case 220: /* sortorder ::= DESC */
{ yymsp[0].minor.yy47 = TSDB_ORDER_DESC;}
        break;
      case 221: /* sortorder ::= */
{ yymsp[1].minor.yy47 = TSDB_ORDER_ASC; }
        break;
      case 222: /* groupby_opt ::= */
{ yymsp[1].minor.yy165 = 0;}
        break;
      case 223: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy165 = yymsp[0].minor.yy165;}
        break;
      case 224: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy165 = tListItemAppend(yymsp[-2].minor.yy165, &yymsp[0].minor.yy425, -1);
}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 225: /* grouplist ::= item */
{
  yylhsminor.yy165 = tListItemAppend(NULL, &yymsp[0].minor.yy425, -1);
}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 226: /* having_opt ::= */
      case 236: /* where_opt ::= */ yytestcase(yyruleno==236);
      case 280: /* expritem ::= */ yytestcase(yyruleno==280);
{yymsp[1].minor.yy202 = 0;}
        break;
      case 227: /* having_opt ::= HAVING expr */
      case 237: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==237);
{yymsp[-1].minor.yy202 = yymsp[0].minor.yy202;}
        break;
      case 228: /* limit_opt ::= */
      case 232: /* slimit_opt ::= */ yytestcase(yyruleno==232);
{yymsp[1].minor.yy367.limit = -1; yymsp[1].minor.yy367.offset = 0;}
        break;
      case 229: /* limit_opt ::= LIMIT signed */
      case 233: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==233);
{yymsp[-1].minor.yy367.limit = yymsp[0].minor.yy207;  yymsp[-1].minor.yy367.offset = 0;}
        break;
      case 230: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy367.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[0].minor.yy207;}
        break;
      case 231: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy367.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[-2].minor.yy207;}
        break;
      case 234: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy367.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[0].minor.yy207;}
        break;
      case 235: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy367.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy367.offset = yymsp[-2].minor.yy207;}
        break;
      case 238: /* expr ::= LP expr RP */
{yylhsminor.yy202 = yymsp[-1].minor.yy202; yylhsminor.yy202->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy202->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 239: /* expr ::= ID */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 240: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 241: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 242: /* expr ::= INTEGER */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 243: /* expr ::= MINUS INTEGER */
      case 244: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==244);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy202 = yylhsminor.yy202;
        break;
      case 245: /* expr ::= FLOAT */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 246: /* expr ::= MINUS FLOAT */
      case 247: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==247);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy202 = yylhsminor.yy202;
        break;
      case 248: /* expr ::= STRING */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 249: /* expr ::= NOW */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 250: /* expr ::= VARIABLE */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 251: /* expr ::= PLUS VARIABLE */
      case 252: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==252);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy202 = yylhsminor.yy202;
        break;
      case 253: /* expr ::= BOOL */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 254: /* expr ::= NULL */
{ yylhsminor.yy202 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 255: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy202 = tSqlExprCreateFunction(yymsp[-1].minor.yy165, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy202 = yylhsminor.yy202;
        break;
      case 256: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy202 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy202 = yylhsminor.yy202;
        break;
      case 257: /* expr ::= expr IS NULL */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 258: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-3].minor.yy202, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy202 = yylhsminor.yy202;
        break;
      case 259: /* expr ::= expr LT expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_LT);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 260: /* expr ::= expr GT expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_GT);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 261: /* expr ::= expr LE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_LE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 262: /* expr ::= expr GE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_GE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 263: /* expr ::= expr NE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_NE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 264: /* expr ::= expr EQ expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_EQ);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 265: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy202); yylhsminor.yy202 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy202, yymsp[-2].minor.yy202, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy202, TK_LE), TK_AND);}
  yymsp[-4].minor.yy202 = yylhsminor.yy202;
        break;
      case 266: /* expr ::= expr AND expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_AND);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 267: /* expr ::= expr OR expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_OR); }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 268: /* expr ::= expr PLUS expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_PLUS);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 269: /* expr ::= expr MINUS expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_MINUS); }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 270: /* expr ::= expr STAR expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_STAR);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 271: /* expr ::= expr SLASH expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_DIVIDE);}
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 272: /* expr ::= expr REM expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_REM);   }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 273: /* expr ::= expr LIKE expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_LIKE);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 274: /* expr ::= expr MATCH expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_MATCH);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 275: /* expr ::= expr NMATCH expr */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-2].minor.yy202, yymsp[0].minor.yy202, TK_NMATCH);  }
  yymsp[-2].minor.yy202 = yylhsminor.yy202;
        break;
      case 276: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy202 = tSqlExprCreate(yymsp[-4].minor.yy202, (tSqlExpr*)yymsp[-1].minor.yy165, TK_IN); }
  yymsp[-4].minor.yy202 = yylhsminor.yy202;
        break;
      case 277: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy165 = tSqlExprListAppend(yymsp[-2].minor.yy165,yymsp[0].minor.yy202,0, 0);}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 278: /* exprlist ::= expritem */
{yylhsminor.yy165 = tSqlExprListAppend(0,yymsp[0].minor.yy202,0, 0);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 279: /* expritem ::= expr */
{yylhsminor.yy202 = yymsp[0].minor.yy202;}
  yymsp[0].minor.yy202 = yylhsminor.yy202;
        break;
      case 281: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 282: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_TAG, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy425, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_TAG, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 295: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 296: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 297: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy425, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 298: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 299: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 300: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 301: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
