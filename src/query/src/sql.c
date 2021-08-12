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
#line 23 "sql.y"

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
#line 42 "sql.c"
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
#define YYNOCODE 275
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSessionWindowVal yy39;
  SCreateDbInfo yy42;
  int yy43;
  tSqlExpr* yy46;
  SCreatedTableInfo yy96;
  SArray* yy131;
  TAOS_FIELD yy163;
  SSqlNode* yy256;
  SCreateTableSql* yy272;
  SLimitVal yy284;
  SCreateAcctInfo yy341;
  int64_t yy459;
  tVariant yy516;
  SIntervalVal yy530;
  SWindowStateVal yy538;
  SRelationInfo* yy544;
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
#define YYNSTATE             362
#define YYNRULE              289
#define YYNRULE_WITH_ACTION  289
#define YYNTOKEN             195
#define YY_MAX_SHIFT         361
#define YY_MIN_SHIFTREDUCE   567
#define YY_MAX_SHIFTREDUCE   855
#define YY_ERROR_ACTION      856
#define YY_ACCEPT_ACTION     857
#define YY_NO_ACTION         858
#define YY_MIN_REDUCE        859
#define YY_MAX_REDUCE        1147
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
#define YY_ACTTAB_COUNT (754)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   207,  618,  246,  618,  618,  245,  360,  229,  160,  619,
 /*    10 */  1123,  619,  619,   56,   57, 1036,   60,   61,  857,  361,
 /*    20 */   249,   50,  618,   59,  318,   64,   62,   65,   63,  984,
 /*    30 */   619,  982,  983,   55,   54,  160,  985,   53,   52,   51,
 /*    40 */   986,  153,  987,  988,  356,  945,  654,  568,  569,  570,
 /*    50 */   571,  572,  573,  574,  575,  576,  577,  578,  579,  580,
 /*    60 */   581,  151,  207,  230,  907,  207,   56,   57, 1027,   60,
 /*    70 */    61,  189, 1124,  249,   50, 1124,   59,  318,   64,   62,
 /*    80 */    65,   63, 1072, 1033,  271,   79,   55,   54,    3,  190,
 /*    90 */    53,   52,   51,   56,   57,  250,   60,   61,  702, 1027,
 /*   100 */   249,   50,   29,   59,  318,   64,   62,   65,   63,   91,
 /*   110 */   278,  277,   37,   55,   54,  232,   94,   53,   52,   51,
 /*   120 */   235,  120,  114,  125, 1014,  241,  338,  337,  124, 1014,
 /*   130 */   130,  133,  123,   56,   58,  794,   60,   61,  127,   85,
 /*   140 */   249,   50,   92,   59,  318,   64,   62,   65,   63,  997,
 /*   150 */   998,   34, 1001,   55,   54,  207,   80,   53,   52,   51,
 /*   160 */    57, 1010,   60,   61,  316, 1124,  249,   50,  263,   59,
 /*   170 */   318,   64,   62,   65,   63,   37,   44,  267,  266,   55,
 /*   180 */    54,  348,  243,   53,   52,   51, 1014,  160,   43,  314,
 /*   190 */   355,  354,  313,  312,  311,  353,  310,  309,  308,  352,
 /*   200 */   307,  351,  350,  976,  964,  965,  966,  967,  968,  969,
 /*   210 */   970,  971,  972,  973,  974,  975,  977,  978,   60,   61,
 /*   220 */   231,  160,  249,   50, 1011,   59,  318,   64,   62,   65,
 /*   230 */    63, 1008, 1027,   24,  258,   55,   54, 1000,   97,   53,
 /*   240 */    52,   51,  252,  248,  809,  175, 1013,  798,  233,  801,
 /*   250 */   210,  804,  248,  809, 1143,  917,  798,  216,  801,  292,
 /*   260 */   804,   90,  189,  135,  134,  215,  258,   55,   54,  323,
 /*   270 */    85,   53,   52,   51, 1002,  227,  228,  176,  242,  319,
 /*   280 */     5,   40,  179,  258,  227,  228,   23,  178,  103,  108,
 /*   290 */    99,  107,  204,  726, 1012, 1073,  723,  290,  724,  908,
 /*   300 */   725,   64,   62,   65,   63,  303,  189,   44,  257,   55,
 /*   310 */    54,   37,   37,   53,   52,   51,  800,  253,  803,  251,
 /*   320 */   316,  326,  325,   66,  254,  255,  198,  196,  194,  270,
 /*   330 */    37,   77,   66,  193,  139,  138,  137,  136,  223,  742,
 /*   340 */   799,   43,  802,  355,  354,   37,   37,   37,  353,   53,
 /*   350 */    52,   51,  352,   37,  351,  350,  239,  240,  810,  805,
 /*   360 */  1011, 1011,  272,   78,   37,  806,  122,  810,  805,   37,
 /*   370 */    37,  359,  358,  144,  806,  327,   38,   14,  348, 1011,
 /*   380 */    82,   93,   70,  259,  739,  256,  320,  333,  332,   83,
 /*   390 */   328,  329,  330,   73, 1011, 1011, 1011,  999,  334,  150,
 /*   400 */   148,  147, 1011,    1,  177,  775,  776,  727,  728,  335,
 /*   410 */     9,   96,  796, 1011,  336,  340,  758,  274, 1011, 1011,
 /*   420 */  1083,  766,  767,  746,   71,  712,  274,  295,  714,  297,
 /*   430 */   155,  713,   33,   74,  807,   67,   26,  830,  811,   38,
 /*   440 */   247,   38,   67,   95,   76,   67,  617,   16,  797,   15,
 /*   450 */   205,   25,   25,  113,   18,  112,   17,  731,  808,  732,
 /*   460 */    25,    6,  729,  211,  730,  298,   20,  119,   19,  118,
 /*   470 */    22, 1120,   21,  132,  131, 1119,  701, 1118,  225,  226,
 /*   480 */   208,  209, 1135,  212,  206,  213,  214,  813,  218,  219,
 /*   490 */   220,  217,  203, 1082,  237, 1079, 1078,  238,  339,   47,
 /*   500 */  1028,  268,  152, 1065, 1064, 1035,  149,  275, 1009,  279,
 /*   510 */  1046, 1043, 1044, 1048,  154,  159,  286,  171,  172,  273,
 /*   520 */   234, 1007,  173,  162,  174,  922,  300,  301,  302,  305,
 /*   530 */   306,  757, 1025,   45,  281,  201,  161,  283,   41,  317,
 /*   540 */    75,  916,  293,   72,   49,  324,  164, 1142,  291,  110,
 /*   550 */  1141, 1138,  163,  289,  180,  331, 1134,  116, 1133, 1130,
 /*   560 */   287,  285,  181,  942,   42,   39,   46,  202,  282,  904,
 /*   570 */   126,  902,  128,  129,  900,  899,  260,  280,  192,  897,
 /*   580 */   896,  895,  894,  893,  892,  891,  195,  197,  888,  886,
 /*   590 */   884,  882,  199,   48,  879,  200,  875,  304,  349,   81,
 /*   600 */    86,  284, 1066,  121,  341,  342,  343,  344,  345,  346,
 /*   610 */   347,  357,  855,  262,  261,  854,  224,  244,  299,  264,
 /*   620 */   265,  853,  836,  221,  222,  835,  269,  294,  104,  921,
 /*   630 */   920,  274,  105,   10,  276,   87,   84,  898,  734,  140,
 /*   640 */    30,  156,  141,  184,  890,  183,  943,  182,  185,  187,
 /*   650 */   186,  142,  188,    2,  889,  759,  143,  881,  980,  165,
 /*   660 */   880,  166,  167,  944,  168,  169,  170,    4,  990,  768,
 /*   670 */   157,  158,  762,   88,  236,  764,   89,  288,   31,   11,
 /*   680 */    32,   12,   13,   27,  296,   28,   96,  101,   98,   35,
 /*   690 */   100,  632,   36,  667,  102,  665,  664,  663,  661,  660,
 /*   700 */   659,  656,  622,  315,  106,    7,  321,  812,  322,    8,
 /*   710 */   814,  109,  111,   68,   69,   38,  704,  703,  115,  700,
 /*   720 */   117,  648,  646,  638,  644,  640,  642,  636,  634,  670,
 /*   730 */   669,  668,  666,  662,  658,  657,  191,  620,  585,  859,
 /*   740 */   858,  858,  858,  858,  858,  858,  858,  858,  858,  858,
 /*   750 */   858,  858,  145,  146,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   264,    1,  204,    1,    1,  204,  197,  198,  197,    9,
 /*    10 */   274,    9,    9,   13,   14,  197,   16,   17,  195,  196,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  221,
 /*    30 */     9,  223,  224,   33,   34,  197,  228,   37,   38,   39,
 /*    40 */   232,  197,  234,  235,  219,  220,    5,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  264,   61,  203,  264,   13,   14,  245,   16,
 /*    70 */    17,  210,  274,   20,   21,  274,   23,   24,   25,   26,
 /*    80 */    27,   28,  271,  265,  261,   85,   33,   34,  201,  202,
 /*    90 */    37,   38,   39,   13,   14,  204,   16,   17,    5,  245,
 /*   100 */    20,   21,   81,   23,   24,   25,   26,   27,   28,  271,
 /*   110 */   266,  267,  197,   33,   34,  261,  205,   37,   38,   39,
 /*   120 */   243,   62,   63,   64,  247,  243,   33,   34,   69,  247,
 /*   130 */    71,   72,   73,   13,   14,   82,   16,   17,   79,   81,
 /*   140 */    20,   21,  248,   23,   24,   25,   26,   27,   28,  238,
 /*   150 */   239,  240,  241,   33,   34,  264,  262,   37,   38,   39,
 /*   160 */    14,  246,   16,   17,   83,  274,   20,   21,  141,   23,
 /*   170 */    24,   25,   26,   27,   28,  197,  118,  150,  151,   33,
 /*   180 */    34,   89,  243,   37,   38,   39,  247,  197,   97,   98,
 /*   190 */    99,  100,  101,  102,  103,  104,  105,  106,  107,  108,
 /*   200 */   109,  110,  111,  221,  222,  223,  224,  225,  226,  227,
 /*   210 */   228,  229,  230,  231,  232,  233,  234,  235,   16,   17,
 /*   220 */   242,  197,   20,   21,  246,   23,   24,   25,   26,   27,
 /*   230 */    28,  197,  245,   44,  197,   33,   34,    0,  205,   37,
 /*   240 */    38,   39,   68,    1,    2,  208,  247,    5,  261,    7,
 /*   250 */    61,    9,    1,    2,  247,  203,    5,   68,    7,  269,
 /*   260 */     9,  271,  210,   74,   75,   76,  197,   33,   34,   80,
 /*   270 */    81,   37,   38,   39,  241,   33,   34,  208,  244,   37,
 /*   280 */    62,   63,   64,  197,   33,   34,  264,   69,   70,   71,
 /*   290 */    72,   73,  264,    2,  208,  271,    5,  273,    7,  203,
 /*   300 */     9,   25,   26,   27,   28,   87,  210,  118,   68,   33,
 /*   310 */    34,  197,  197,   37,   38,   39,    5,  143,    7,  145,
 /*   320 */    83,  147,  148,   81,   33,   34,   62,   63,   64,  140,
 /*   330 */   197,  142,   81,   69,   70,   71,   72,   73,  149,   37,
 /*   340 */     5,   97,    7,   99,  100,  197,  197,  197,  104,   37,
 /*   350 */    38,   39,  108,  197,  110,  111,  242,  242,  116,  117,
 /*   360 */   246,  246,   82,  205,  197,  123,   77,  116,  117,  197,
 /*   370 */   197,   65,   66,   67,  123,  242,   96,   81,   89,  246,
 /*   380 */    82,   85,   96,  143,   96,  145,   15,  147,  148,   82,
 /*   390 */   242,  242,  242,   96,  246,  246,  246,  239,  242,   62,
 /*   400 */    63,   64,  246,  206,  207,  131,  132,  116,  117,  242,
 /*   410 */   122,  115,    1,  246,  242,  242,   82,  119,  246,  246,
 /*   420 */   237,   82,   82,  121,  138,   82,  119,   82,   82,   82,
 /*   430 */    96,   82,   81,  136,  123,   96,   96,   82,   82,   96,
 /*   440 */    60,   96,   96,   96,   81,   96,   82,  144,   37,  146,
 /*   450 */   264,   96,   96,  144,  144,  146,  146,    5,  123,    7,
 /*   460 */    96,   81,    5,  264,    7,  114,  144,  144,  146,  146,
 /*   470 */   144,  264,  146,   77,   78,  264,  113,  264,  264,  264,
 /*   480 */   264,  264,  247,  264,  264,  264,  264,  116,  264,  264,
 /*   490 */   264,  264,  264,  237,  237,  237,  237,  237,  237,  263,
 /*   500 */   245,  197,  197,  272,  272,  197,   60,  245,  245,  268,
 /*   510 */   197,  197,  197,  197,  197,  197,  197,  249,  197,  199,
 /*   520 */   268,  197,  197,  258,  197,  197,  197,  197,  197,  197,
 /*   530 */   197,  123,  260,  197,  268,  197,  259,  268,  197,  197,
 /*   540 */   135,  197,  129,  137,  134,  197,  256,  197,  133,  197,
 /*   550 */   197,  197,  257,  127,  197,  197,  197,  197,  197,  197,
 /*   560 */   126,  125,  197,  197,  197,  197,  197,  197,  128,  197,
 /*   570 */   197,  197,  197,  197,  197,  197,  197,  124,  197,  197,
 /*   580 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   590 */   197,  197,  197,  139,  197,  197,  197,   88,  112,  199,
 /*   600 */   199,  199,  199,   95,   94,   51,   91,   93,   55,   92,
 /*   610 */    90,   83,    5,    5,  152,    5,  199,  199,  199,  152,
 /*   620 */     5,    5,   99,  199,  199,   98,  141,  114,  205,  209,
 /*   630 */   209,  119,  205,   81,   96,   96,  120,  199,   82,  200,
 /*   640 */    81,   81,  200,  212,  199,  216,  218,  217,  215,  214,
 /*   650 */   213,  200,  211,  206,  199,   82,  200,  199,  236,  255,
 /*   660 */   199,  254,  253,  220,  252,  251,  250,  201,  236,   82,
 /*   670 */    81,   96,   82,   81,    1,   82,   81,   81,   96,  130,
 /*   680 */    96,  130,   81,   81,  114,   81,  115,   70,   77,   86,
 /*   690 */    85,    5,   86,    9,   85,    5,    5,    5,    5,    5,
 /*   700 */     5,    5,   84,   15,   77,   81,   24,   82,   59,   81,
 /*   710 */   116,  146,  146,   16,   16,   96,    5,    5,  146,   82,
 /*   720 */   146,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   730 */     5,    5,    5,    5,    5,    5,   96,   84,   60,    0,
 /*   740 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   750 */   275,  275,   21,   21,  275,  275,  275,  275,  275,  275,
 /*   760 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   770 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   780 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   790 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   800 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   810 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   820 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   830 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   840 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   850 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   860 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   870 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   880 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   890 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   900 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   910 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   920 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   930 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   940 */   275,  275,  275,  275,  275,  275,  275,  275,  275,
};
#define YY_SHIFT_COUNT    (361)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (739)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   189,   91,   91,  244,  244,   81,  242,  251,  251,   21,
 /*    10 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    20 */     3,    3,    3,    0,    2,  251,  291,  291,  291,   58,
 /*    30 */    58,    3,    3,    3,  237,    3,    3,    3,    3,  289,
 /*    40 */    81,   92,   92,   41,  754,  754,  754,  251,  251,  251,
 /*    50 */   251,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*    60 */   251,  251,  251,  251,  251,  251,  251,  291,  291,  291,
 /*    70 */    93,   93,   93,   93,   93,   93,   93,    3,    3,    3,
 /*    80 */   302,    3,    3,    3,   58,   58,    3,    3,    3,    3,
 /*    90 */   274,  274,  288,   58,    3,    3,    3,    3,    3,    3,
 /*   100 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   110 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   120 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   130 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   140 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   150 */     3,    3,  446,  446,  446,  408,  408,  408,  408,  446,
 /*   160 */   446,  405,  406,  413,  410,  415,  426,  434,  436,  440,
 /*   170 */   453,  454,  446,  446,  446,  509,  509,  486,   81,   81,
 /*   180 */   446,  446,  508,  510,  554,  515,  514,  553,  517,  520,
 /*   190 */   486,   41,  446,  528,  528,  446,  528,  446,  528,  446,
 /*   200 */   446,  754,  754,   53,   80,   80,  120,   80,  146,  202,
 /*   210 */   218,  276,  276,  276,  276,   59,  264,  234,  234,  234,
 /*   220 */   234,  174,  240,   27,  296,  312,  312,  311,  335,  306,
 /*   230 */   337,  280,  298,  307,  334,  339,  340,  286,  297,  343,
 /*   240 */   345,  346,  347,  349,  351,  355,  356,  411,  380,  371,
 /*   250 */   364,  303,  309,  310,  452,  457,  322,  323,  363,  326,
 /*   260 */   396,  607,  462,  608,  610,  467,  615,  616,  523,  527,
 /*   270 */   485,  512,  513,  552,  516,  556,  559,  538,  539,  573,
 /*   280 */   560,  587,  589,  590,  575,  592,  593,  595,  673,  596,
 /*   290 */   582,  549,  584,  551,  601,  513,  602,  570,  604,  571,
 /*   300 */   611,  603,  605,  617,  686,  606,  609,  684,  690,  691,
 /*   310 */   692,  693,  694,  695,  696,  618,  688,  627,  624,  625,
 /*   320 */   594,  628,  682,  649,  697,  565,  566,  619,  619,  619,
 /*   330 */   619,  698,  572,  574,  619,  619,  619,  711,  712,  637,
 /*   340 */   619,  716,  717,  718,  719,  720,  721,  722,  723,  724,
 /*   350 */   725,  726,  727,  728,  729,  730,  640,  653,  731,  732,
 /*   360 */   678,  739,
};
#define YY_REDUCE_COUNT (202)
#define YY_REDUCE_MIN   (-264)
#define YY_REDUCE_MAX   (466)
static const short yy_reduce_ofst[] = {
 /*     0 */  -177,  -18,  -18, -192, -192,  -89, -202, -199, -109, -156,
 /*    10 */   -22,   24,  -10,  114,  115,  133,  148,  149,  150,  156,
 /*    20 */   167,  172,  173, -182, -191, -264, -123, -118,  -61, -146,
 /*    30 */   -13, -189, -162,   34,   33,   37,   69,   86,  -85, -139,
 /*    40 */   158,   52,   96, -175, -106,  197, -113,   22,   28,  186,
 /*    50 */   199,  207,  211,  213,  214,  215,  216,  217,  219,  220,
 /*    60 */   221,  222,  224,  225,  226,  227,  228,   -1,    7,  235,
 /*    70 */   183,  256,  257,  258,  259,  260,  261,  304,  305,  308,
 /*    80 */   236,  313,  314,  315,  255,  262,  316,  317,  318,  319,
 /*    90 */   231,  232,  268,  263,  321,  324,  325,  327,  328,  329,
 /*   100 */   330,  331,  332,  333,  336,  338,  341,  342,  344,  348,
 /*   110 */   350,  352,  353,  354,  357,  358,  359,  360,  361,  362,
 /*   120 */   365,  366,  367,  368,  369,  370,  372,  373,  374,  375,
 /*   130 */   376,  377,  378,  379,  381,  382,  383,  384,  385,  386,
 /*   140 */   387,  388,  389,  390,  391,  392,  393,  394,  395,  397,
 /*   150 */   398,  399,  320,  400,  401,  241,  252,  266,  269,  402,
 /*   160 */   403,  272,  277,  265,  295,  290,  404,  407,  409,  412,
 /*   170 */   414,  416,  417,  418,  419,  420,  421,  422,  423,  427,
 /*   180 */   424,  425,  428,  430,  429,  431,  433,  437,  435,  441,
 /*   190 */   432,  443,  438,  439,  442,  445,  451,  455,  456,  458,
 /*   200 */   461,  447,  466,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   856,  979,  918,  989,  905,  915, 1126, 1126, 1126,  856,
 /*    10 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    20 */   856,  856,  856, 1037,  876, 1126,  856,  856,  856,  856,
 /*    30 */   856,  856,  856,  856,  915,  856,  856,  856,  856,  925,
 /*    40 */   915,  925,  925,  856, 1032,  963,  981,  856,  856,  856,
 /*    50 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    60 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    70 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*    80 */  1039, 1045, 1042,  856,  856,  856, 1047,  856,  856,  856,
 /*    90 */  1069, 1069, 1030,  856,  856,  856,  856,  856,  856,  856,
 /*   100 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   110 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   120 */   856,  856,  856,  856,  856,  856,  903,  856,  901,  856,
 /*   130 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   140 */   856,  856,  856,  856,  887,  856,  856,  856,  856,  856,
 /*   150 */   856,  874,  878,  878,  878,  856,  856,  856,  856,  878,
 /*   160 */   878, 1076, 1080, 1062, 1074, 1070, 1057, 1055, 1053, 1061,
 /*   170 */  1052, 1084,  878,  878,  878,  923,  923,  919,  915,  915,
 /*   180 */   878,  878,  941,  939,  937,  929,  935,  931,  933,  927,
 /*   190 */   906,  856,  878,  913,  913,  878,  913,  878,  913,  878,
 /*   200 */   878,  963,  981,  856, 1085, 1075,  856, 1125, 1115, 1114,
 /*   210 */   856, 1121, 1113, 1112, 1111,  856,  856, 1107, 1110, 1109,
 /*   220 */  1108,  856,  856,  856,  856, 1117, 1116,  856,  856,  856,
 /*   230 */   856,  856,  856,  856,  856,  856,  856, 1081, 1077,  856,
 /*   240 */   856,  856,  856,  856,  856,  856,  856,  856, 1087,  856,
 /*   250 */   856,  856,  856,  856,  856,  856,  856,  856,  991,  856,
 /*   260 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   270 */   856, 1029,  856,  856,  856,  856,  856, 1041, 1040,  856,
 /*   280 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   290 */  1071,  856, 1063,  856,  856, 1003,  856,  856,  856,  856,
 /*   300 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   310 */   856,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   320 */   856,  856,  856,  856,  856,  856,  856, 1144, 1139, 1140,
 /*   330 */  1137,  856,  856,  856, 1136, 1131, 1132,  856,  856,  856,
 /*   340 */  1129,  856,  856,  856,  856,  856,  856,  856,  856,  856,
 /*   350 */   856,  856,  856,  856,  856,  856,  947,  856,  885,  883,
 /*   360 */   856,  856,
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
    0,  /*         OR => nothing */
    0,  /*        AND => nothing */
    0,  /*        NOT => nothing */
    0,  /*         EQ => nothing */
    0,  /*         NE => nothing */
    0,  /*     ISNULL => nothing */
    0,  /*    NOTNULL => nothing */
    0,  /*         IS => nothing */
    1,  /*       LIKE => ID */
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
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    1,  /*       DESC => ID */
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
    1,  /*      MATCH => ID */
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
  /*   13 */ "OR",
  /*   14 */ "AND",
  /*   15 */ "NOT",
  /*   16 */ "EQ",
  /*   17 */ "NE",
  /*   18 */ "ISNULL",
  /*   19 */ "NOTNULL",
  /*   20 */ "IS",
  /*   21 */ "LIKE",
  /*   22 */ "GLOB",
  /*   23 */ "BETWEEN",
  /*   24 */ "IN",
  /*   25 */ "GT",
  /*   26 */ "GE",
  /*   27 */ "LT",
  /*   28 */ "LE",
  /*   29 */ "BITAND",
  /*   30 */ "BITOR",
  /*   31 */ "LSHIFT",
  /*   32 */ "RSHIFT",
  /*   33 */ "PLUS",
  /*   34 */ "MINUS",
  /*   35 */ "DIVIDE",
  /*   36 */ "TIMES",
  /*   37 */ "STAR",
  /*   38 */ "SLASH",
  /*   39 */ "REM",
  /*   40 */ "CONCAT",
  /*   41 */ "UMINUS",
  /*   42 */ "UPLUS",
  /*   43 */ "BITNOT",
  /*   44 */ "SHOW",
  /*   45 */ "DATABASES",
  /*   46 */ "TOPICS",
  /*   47 */ "FUNCTIONS",
  /*   48 */ "MNODES",
  /*   49 */ "DNODES",
  /*   50 */ "ACCOUNTS",
  /*   51 */ "USERS",
  /*   52 */ "MODULES",
  /*   53 */ "QUERIES",
  /*   54 */ "CONNECTIONS",
  /*   55 */ "STREAMS",
  /*   56 */ "VARIABLES",
  /*   57 */ "SCORES",
  /*   58 */ "GRANTS",
  /*   59 */ "VNODES",
  /*   60 */ "DOT",
  /*   61 */ "CREATE",
  /*   62 */ "TABLE",
  /*   63 */ "STABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "TOPIC",
  /*   70 */ "FUNCTION",
  /*   71 */ "DNODE",
  /*   72 */ "USER",
  /*   73 */ "ACCOUNT",
  /*   74 */ "USE",
  /*   75 */ "DESCRIBE",
  /*   76 */ "ALTER",
  /*   77 */ "PASS",
  /*   78 */ "PRIVILEGE",
  /*   79 */ "LOCAL",
  /*   80 */ "COMPACT",
  /*   81 */ "LP",
  /*   82 */ "RP",
  /*   83 */ "IF",
  /*   84 */ "EXISTS",
  /*   85 */ "AS",
  /*   86 */ "OUTPUTTYPE",
  /*   87 */ "AGGREGATE",
  /*   88 */ "BUFSIZE",
  /*   89 */ "PPS",
  /*   90 */ "TSERIES",
  /*   91 */ "DBS",
  /*   92 */ "STORAGE",
  /*   93 */ "QTIME",
  /*   94 */ "CONNS",
  /*   95 */ "STATE",
  /*   96 */ "COMMA",
  /*   97 */ "KEEP",
  /*   98 */ "CACHE",
  /*   99 */ "REPLICA",
  /*  100 */ "QUORUM",
  /*  101 */ "DAYS",
  /*  102 */ "MINROWS",
  /*  103 */ "MAXROWS",
  /*  104 */ "BLOCKS",
  /*  105 */ "CTIME",
  /*  106 */ "WAL",
  /*  107 */ "FSYNC",
  /*  108 */ "COMP",
  /*  109 */ "PRECISION",
  /*  110 */ "UPDATE",
  /*  111 */ "CACHELAST",
  /*  112 */ "PARTITIONS",
  /*  113 */ "UNSIGNED",
  /*  114 */ "TAGS",
  /*  115 */ "USING",
  /*  116 */ "NULL",
  /*  117 */ "NOW",
  /*  118 */ "SELECT",
  /*  119 */ "UNION",
  /*  120 */ "ALL",
  /*  121 */ "DISTINCT",
  /*  122 */ "FROM",
  /*  123 */ "VARIABLE",
  /*  124 */ "INTERVAL",
  /*  125 */ "SESSION",
  /*  126 */ "STATE_WINDOW",
  /*  127 */ "FILL",
  /*  128 */ "SLIDING",
  /*  129 */ "ORDER",
  /*  130 */ "BY",
  /*  131 */ "ASC",
  /*  132 */ "DESC",
  /*  133 */ "GROUP",
  /*  134 */ "HAVING",
  /*  135 */ "LIMIT",
  /*  136 */ "OFFSET",
  /*  137 */ "SLIMIT",
  /*  138 */ "SOFFSET",
  /*  139 */ "WHERE",
  /*  140 */ "RESET",
  /*  141 */ "QUERY",
  /*  142 */ "SYNCDB",
  /*  143 */ "ADD",
  /*  144 */ "COLUMN",
  /*  145 */ "MODIFY",
  /*  146 */ "TAG",
  /*  147 */ "CHANGE",
  /*  148 */ "SET",
  /*  149 */ "KILL",
  /*  150 */ "CONNECTION",
  /*  151 */ "STREAM",
  /*  152 */ "COLON",
  /*  153 */ "ABORT",
  /*  154 */ "AFTER",
  /*  155 */ "ATTACH",
  /*  156 */ "BEFORE",
  /*  157 */ "BEGIN",
  /*  158 */ "CASCADE",
  /*  159 */ "CLUSTER",
  /*  160 */ "CONFLICT",
  /*  161 */ "COPY",
  /*  162 */ "DEFERRED",
  /*  163 */ "DELIMITERS",
  /*  164 */ "DETACH",
  /*  165 */ "EACH",
  /*  166 */ "END",
  /*  167 */ "EXPLAIN",
  /*  168 */ "FAIL",
  /*  169 */ "FOR",
  /*  170 */ "IGNORE",
  /*  171 */ "IMMEDIATE",
  /*  172 */ "INITIALLY",
  /*  173 */ "INSTEAD",
  /*  174 */ "MATCH",
  /*  175 */ "KEY",
  /*  176 */ "OF",
  /*  177 */ "RAISE",
  /*  178 */ "REPLACE",
  /*  179 */ "RESTRICT",
  /*  180 */ "ROW",
  /*  181 */ "STATEMENT",
  /*  182 */ "TRIGGER",
  /*  183 */ "VIEW",
  /*  184 */ "IPTOKEN",
  /*  185 */ "SEMI",
  /*  186 */ "NONE",
  /*  187 */ "PREV",
  /*  188 */ "LINEAR",
  /*  189 */ "IMPORT",
  /*  190 */ "TBNAME",
  /*  191 */ "JOIN",
  /*  192 */ "INSERT",
  /*  193 */ "INTO",
  /*  194 */ "VALUES",
  /*  195 */ "program",
  /*  196 */ "cmd",
  /*  197 */ "ids",
  /*  198 */ "dbPrefix",
  /*  199 */ "cpxName",
  /*  200 */ "ifexists",
  /*  201 */ "alter_db_optr",
  /*  202 */ "alter_topic_optr",
  /*  203 */ "acct_optr",
  /*  204 */ "exprlist",
  /*  205 */ "ifnotexists",
  /*  206 */ "db_optr",
  /*  207 */ "topic_optr",
  /*  208 */ "typename",
  /*  209 */ "bufsize",
  /*  210 */ "pps",
  /*  211 */ "tseries",
  /*  212 */ "dbs",
  /*  213 */ "streams",
  /*  214 */ "storage",
  /*  215 */ "qtime",
  /*  216 */ "users",
  /*  217 */ "conns",
  /*  218 */ "state",
  /*  219 */ "intitemlist",
  /*  220 */ "intitem",
  /*  221 */ "keep",
  /*  222 */ "cache",
  /*  223 */ "replica",
  /*  224 */ "quorum",
  /*  225 */ "days",
  /*  226 */ "minrows",
  /*  227 */ "maxrows",
  /*  228 */ "blocks",
  /*  229 */ "ctime",
  /*  230 */ "wal",
  /*  231 */ "fsync",
  /*  232 */ "comp",
  /*  233 */ "prec",
  /*  234 */ "update",
  /*  235 */ "cachelast",
  /*  236 */ "partitions",
  /*  237 */ "signed",
  /*  238 */ "create_table_args",
  /*  239 */ "create_stable_args",
  /*  240 */ "create_table_list",
  /*  241 */ "create_from_stable",
  /*  242 */ "columnlist",
  /*  243 */ "tagitemlist",
  /*  244 */ "tagNamelist",
  /*  245 */ "select",
  /*  246 */ "column",
  /*  247 */ "tagitem",
  /*  248 */ "selcollist",
  /*  249 */ "from",
  /*  250 */ "where_opt",
  /*  251 */ "interval_opt",
  /*  252 */ "sliding_opt",
  /*  253 */ "session_option",
  /*  254 */ "windowstate_option",
  /*  255 */ "fill_opt",
  /*  256 */ "groupby_opt",
  /*  257 */ "having_opt",
  /*  258 */ "orderby_opt",
  /*  259 */ "slimit_opt",
  /*  260 */ "limit_opt",
  /*  261 */ "union",
  /*  262 */ "sclp",
  /*  263 */ "distinct",
  /*  264 */ "expr",
  /*  265 */ "as",
  /*  266 */ "tablelist",
  /*  267 */ "sub",
  /*  268 */ "tmvar",
  /*  269 */ "sortlist",
  /*  270 */ "sortitem",
  /*  271 */ "item",
  /*  272 */ "sortorder",
  /*  273 */ "grouplist",
  /*  274 */ "expritem",
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
 /* 162 */ "tagitem ::= MINUS INTEGER",
 /* 163 */ "tagitem ::= MINUS FLOAT",
 /* 164 */ "tagitem ::= PLUS INTEGER",
 /* 165 */ "tagitem ::= PLUS FLOAT",
 /* 166 */ "select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 167 */ "select ::= LP select RP",
 /* 168 */ "union ::= select",
 /* 169 */ "union ::= union UNION ALL select",
 /* 170 */ "cmd ::= union",
 /* 171 */ "select ::= SELECT selcollist",
 /* 172 */ "sclp ::= selcollist COMMA",
 /* 173 */ "sclp ::=",
 /* 174 */ "selcollist ::= sclp distinct expr as",
 /* 175 */ "selcollist ::= sclp STAR",
 /* 176 */ "as ::= AS ids",
 /* 177 */ "as ::= ids",
 /* 178 */ "as ::=",
 /* 179 */ "distinct ::= DISTINCT",
 /* 180 */ "distinct ::=",
 /* 181 */ "from ::= FROM tablelist",
 /* 182 */ "from ::= FROM sub",
 /* 183 */ "sub ::= LP union RP",
 /* 184 */ "sub ::= LP union RP ids",
 /* 185 */ "sub ::= sub COMMA LP union RP ids",
 /* 186 */ "tablelist ::= ids cpxName",
 /* 187 */ "tablelist ::= ids cpxName ids",
 /* 188 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 189 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 190 */ "tmvar ::= VARIABLE",
 /* 191 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 192 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 193 */ "interval_opt ::=",
 /* 194 */ "session_option ::=",
 /* 195 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 196 */ "windowstate_option ::=",
 /* 197 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 198 */ "fill_opt ::=",
 /* 199 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 200 */ "fill_opt ::= FILL LP ID RP",
 /* 201 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 202 */ "sliding_opt ::=",
 /* 203 */ "orderby_opt ::=",
 /* 204 */ "orderby_opt ::= ORDER BY sortlist",
 /* 205 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 206 */ "sortlist ::= item sortorder",
 /* 207 */ "item ::= ids cpxName",
 /* 208 */ "sortorder ::= ASC",
 /* 209 */ "sortorder ::= DESC",
 /* 210 */ "sortorder ::=",
 /* 211 */ "groupby_opt ::=",
 /* 212 */ "groupby_opt ::= GROUP BY grouplist",
 /* 213 */ "grouplist ::= grouplist COMMA item",
 /* 214 */ "grouplist ::= item",
 /* 215 */ "having_opt ::=",
 /* 216 */ "having_opt ::= HAVING expr",
 /* 217 */ "limit_opt ::=",
 /* 218 */ "limit_opt ::= LIMIT signed",
 /* 219 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 220 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 221 */ "slimit_opt ::=",
 /* 222 */ "slimit_opt ::= SLIMIT signed",
 /* 223 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 224 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 225 */ "where_opt ::=",
 /* 226 */ "where_opt ::= WHERE expr",
 /* 227 */ "expr ::= LP expr RP",
 /* 228 */ "expr ::= ID",
 /* 229 */ "expr ::= ID DOT ID",
 /* 230 */ "expr ::= ID DOT STAR",
 /* 231 */ "expr ::= INTEGER",
 /* 232 */ "expr ::= MINUS INTEGER",
 /* 233 */ "expr ::= PLUS INTEGER",
 /* 234 */ "expr ::= FLOAT",
 /* 235 */ "expr ::= MINUS FLOAT",
 /* 236 */ "expr ::= PLUS FLOAT",
 /* 237 */ "expr ::= STRING",
 /* 238 */ "expr ::= NOW",
 /* 239 */ "expr ::= VARIABLE",
 /* 240 */ "expr ::= PLUS VARIABLE",
 /* 241 */ "expr ::= MINUS VARIABLE",
 /* 242 */ "expr ::= BOOL",
 /* 243 */ "expr ::= NULL",
 /* 244 */ "expr ::= ID LP exprlist RP",
 /* 245 */ "expr ::= ID LP STAR RP",
 /* 246 */ "expr ::= expr IS NULL",
 /* 247 */ "expr ::= expr IS NOT NULL",
 /* 248 */ "expr ::= expr LT expr",
 /* 249 */ "expr ::= expr GT expr",
 /* 250 */ "expr ::= expr LE expr",
 /* 251 */ "expr ::= expr GE expr",
 /* 252 */ "expr ::= expr NE expr",
 /* 253 */ "expr ::= expr EQ expr",
 /* 254 */ "expr ::= expr BETWEEN expr AND expr",
 /* 255 */ "expr ::= expr AND expr",
 /* 256 */ "expr ::= expr OR expr",
 /* 257 */ "expr ::= expr PLUS expr",
 /* 258 */ "expr ::= expr MINUS expr",
 /* 259 */ "expr ::= expr STAR expr",
 /* 260 */ "expr ::= expr SLASH expr",
 /* 261 */ "expr ::= expr REM expr",
 /* 262 */ "expr ::= expr LIKE expr",
 /* 263 */ "expr ::= expr IN LP exprlist RP",
 /* 264 */ "exprlist ::= exprlist COMMA expritem",
 /* 265 */ "exprlist ::= expritem",
 /* 266 */ "expritem ::= expr",
 /* 267 */ "expritem ::=",
 /* 268 */ "cmd ::= RESET QUERY CACHE",
 /* 269 */ "cmd ::= SYNCDB ids REPLICA",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 286 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 287 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 288 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 204: /* exprlist */
    case 248: /* selcollist */
    case 262: /* sclp */
{
#line 750 "sql.y"
tSqlExprListDestroy((yypminor->yy131));
#line 1506 "sql.c"
}
      break;
    case 219: /* intitemlist */
    case 221: /* keep */
    case 242: /* columnlist */
    case 243: /* tagitemlist */
    case 244: /* tagNamelist */
    case 255: /* fill_opt */
    case 256: /* groupby_opt */
    case 258: /* orderby_opt */
    case 269: /* sortlist */
    case 273: /* grouplist */
{
#line 253 "sql.y"
taosArrayDestroy((yypminor->yy131));
#line 1522 "sql.c"
}
      break;
    case 240: /* create_table_list */
{
#line 361 "sql.y"
destroyCreateTableSql((yypminor->yy272));
#line 1529 "sql.c"
}
      break;
    case 245: /* select */
{
#line 481 "sql.y"
destroySqlNode((yypminor->yy256));
#line 1536 "sql.c"
}
      break;
    case 249: /* from */
    case 266: /* tablelist */
    case 267: /* sub */
{
#line 536 "sql.y"
destroyRelationInfo((yypminor->yy544));
#line 1545 "sql.c"
}
      break;
    case 250: /* where_opt */
    case 257: /* having_opt */
    case 264: /* expr */
    case 274: /* expritem */
{
#line 683 "sql.y"
tSqlExprDestroy((yypminor->yy46));
#line 1555 "sql.c"
}
      break;
    case 261: /* union */
{
#line 489 "sql.y"
destroyAllSqlNode((yypminor->yy131));
#line 1562 "sql.c"
}
      break;
    case 270: /* sortitem */
{
#line 616 "sql.y"
tVariantDestroy(&(yypminor->yy516));
#line 1569 "sql.c"
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
   195,  /* (0) program ::= cmd */
   196,  /* (1) cmd ::= SHOW DATABASES */
   196,  /* (2) cmd ::= SHOW TOPICS */
   196,  /* (3) cmd ::= SHOW FUNCTIONS */
   196,  /* (4) cmd ::= SHOW MNODES */
   196,  /* (5) cmd ::= SHOW DNODES */
   196,  /* (6) cmd ::= SHOW ACCOUNTS */
   196,  /* (7) cmd ::= SHOW USERS */
   196,  /* (8) cmd ::= SHOW MODULES */
   196,  /* (9) cmd ::= SHOW QUERIES */
   196,  /* (10) cmd ::= SHOW CONNECTIONS */
   196,  /* (11) cmd ::= SHOW STREAMS */
   196,  /* (12) cmd ::= SHOW VARIABLES */
   196,  /* (13) cmd ::= SHOW SCORES */
   196,  /* (14) cmd ::= SHOW GRANTS */
   196,  /* (15) cmd ::= SHOW VNODES */
   196,  /* (16) cmd ::= SHOW VNODES ids */
   198,  /* (17) dbPrefix ::= */
   198,  /* (18) dbPrefix ::= ids DOT */
   199,  /* (19) cpxName ::= */
   199,  /* (20) cpxName ::= DOT ids */
   196,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   196,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   196,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   196,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   196,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   196,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   196,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   196,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   196,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   196,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   196,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   196,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   196,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   196,  /* (34) cmd ::= DROP FUNCTION ids */
   196,  /* (35) cmd ::= DROP DNODE ids */
   196,  /* (36) cmd ::= DROP USER ids */
   196,  /* (37) cmd ::= DROP ACCOUNT ids */
   196,  /* (38) cmd ::= USE ids */
   196,  /* (39) cmd ::= DESCRIBE ids cpxName */
   196,  /* (40) cmd ::= ALTER USER ids PASS ids */
   196,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   196,  /* (42) cmd ::= ALTER DNODE ids ids */
   196,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   196,  /* (44) cmd ::= ALTER LOCAL ids */
   196,  /* (45) cmd ::= ALTER LOCAL ids ids */
   196,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   196,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   196,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   196,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   196,  /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
   197,  /* (51) ids ::= ID */
   197,  /* (52) ids ::= STRING */
   200,  /* (53) ifexists ::= IF EXISTS */
   200,  /* (54) ifexists ::= */
   205,  /* (55) ifnotexists ::= IF NOT EXISTS */
   205,  /* (56) ifnotexists ::= */
   196,  /* (57) cmd ::= CREATE DNODE ids */
   196,  /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   196,  /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   196,  /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   196,  /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   196,  /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   196,  /* (63) cmd ::= CREATE USER ids PASS ids */
   209,  /* (64) bufsize ::= */
   209,  /* (65) bufsize ::= BUFSIZE INTEGER */
   210,  /* (66) pps ::= */
   210,  /* (67) pps ::= PPS INTEGER */
   211,  /* (68) tseries ::= */
   211,  /* (69) tseries ::= TSERIES INTEGER */
   212,  /* (70) dbs ::= */
   212,  /* (71) dbs ::= DBS INTEGER */
   213,  /* (72) streams ::= */
   213,  /* (73) streams ::= STREAMS INTEGER */
   214,  /* (74) storage ::= */
   214,  /* (75) storage ::= STORAGE INTEGER */
   215,  /* (76) qtime ::= */
   215,  /* (77) qtime ::= QTIME INTEGER */
   216,  /* (78) users ::= */
   216,  /* (79) users ::= USERS INTEGER */
   217,  /* (80) conns ::= */
   217,  /* (81) conns ::= CONNS INTEGER */
   218,  /* (82) state ::= */
   218,  /* (83) state ::= STATE ids */
   203,  /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   219,  /* (85) intitemlist ::= intitemlist COMMA intitem */
   219,  /* (86) intitemlist ::= intitem */
   220,  /* (87) intitem ::= INTEGER */
   221,  /* (88) keep ::= KEEP intitemlist */
   222,  /* (89) cache ::= CACHE INTEGER */
   223,  /* (90) replica ::= REPLICA INTEGER */
   224,  /* (91) quorum ::= QUORUM INTEGER */
   225,  /* (92) days ::= DAYS INTEGER */
   226,  /* (93) minrows ::= MINROWS INTEGER */
   227,  /* (94) maxrows ::= MAXROWS INTEGER */
   228,  /* (95) blocks ::= BLOCKS INTEGER */
   229,  /* (96) ctime ::= CTIME INTEGER */
   230,  /* (97) wal ::= WAL INTEGER */
   231,  /* (98) fsync ::= FSYNC INTEGER */
   232,  /* (99) comp ::= COMP INTEGER */
   233,  /* (100) prec ::= PRECISION STRING */
   234,  /* (101) update ::= UPDATE INTEGER */
   235,  /* (102) cachelast ::= CACHELAST INTEGER */
   236,  /* (103) partitions ::= PARTITIONS INTEGER */
   206,  /* (104) db_optr ::= */
   206,  /* (105) db_optr ::= db_optr cache */
   206,  /* (106) db_optr ::= db_optr replica */
   206,  /* (107) db_optr ::= db_optr quorum */
   206,  /* (108) db_optr ::= db_optr days */
   206,  /* (109) db_optr ::= db_optr minrows */
   206,  /* (110) db_optr ::= db_optr maxrows */
   206,  /* (111) db_optr ::= db_optr blocks */
   206,  /* (112) db_optr ::= db_optr ctime */
   206,  /* (113) db_optr ::= db_optr wal */
   206,  /* (114) db_optr ::= db_optr fsync */
   206,  /* (115) db_optr ::= db_optr comp */
   206,  /* (116) db_optr ::= db_optr prec */
   206,  /* (117) db_optr ::= db_optr keep */
   206,  /* (118) db_optr ::= db_optr update */
   206,  /* (119) db_optr ::= db_optr cachelast */
   207,  /* (120) topic_optr ::= db_optr */
   207,  /* (121) topic_optr ::= topic_optr partitions */
   201,  /* (122) alter_db_optr ::= */
   201,  /* (123) alter_db_optr ::= alter_db_optr replica */
   201,  /* (124) alter_db_optr ::= alter_db_optr quorum */
   201,  /* (125) alter_db_optr ::= alter_db_optr keep */
   201,  /* (126) alter_db_optr ::= alter_db_optr blocks */
   201,  /* (127) alter_db_optr ::= alter_db_optr comp */
   201,  /* (128) alter_db_optr ::= alter_db_optr update */
   201,  /* (129) alter_db_optr ::= alter_db_optr cachelast */
   202,  /* (130) alter_topic_optr ::= alter_db_optr */
   202,  /* (131) alter_topic_optr ::= alter_topic_optr partitions */
   208,  /* (132) typename ::= ids */
   208,  /* (133) typename ::= ids LP signed RP */
   208,  /* (134) typename ::= ids UNSIGNED */
   237,  /* (135) signed ::= INTEGER */
   237,  /* (136) signed ::= PLUS INTEGER */
   237,  /* (137) signed ::= MINUS INTEGER */
   196,  /* (138) cmd ::= CREATE TABLE create_table_args */
   196,  /* (139) cmd ::= CREATE TABLE create_stable_args */
   196,  /* (140) cmd ::= CREATE STABLE create_stable_args */
   196,  /* (141) cmd ::= CREATE TABLE create_table_list */
   240,  /* (142) create_table_list ::= create_from_stable */
   240,  /* (143) create_table_list ::= create_table_list create_from_stable */
   238,  /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   239,  /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   241,  /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   241,  /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   244,  /* (148) tagNamelist ::= tagNamelist COMMA ids */
   244,  /* (149) tagNamelist ::= ids */
   238,  /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
   242,  /* (151) columnlist ::= columnlist COMMA column */
   242,  /* (152) columnlist ::= column */
   246,  /* (153) column ::= ids typename */
   243,  /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
   243,  /* (155) tagitemlist ::= tagitem */
   247,  /* (156) tagitem ::= INTEGER */
   247,  /* (157) tagitem ::= FLOAT */
   247,  /* (158) tagitem ::= STRING */
   247,  /* (159) tagitem ::= BOOL */
   247,  /* (160) tagitem ::= NULL */
   247,  /* (161) tagitem ::= NOW */
   247,  /* (162) tagitem ::= MINUS INTEGER */
   247,  /* (163) tagitem ::= MINUS FLOAT */
   247,  /* (164) tagitem ::= PLUS INTEGER */
   247,  /* (165) tagitem ::= PLUS FLOAT */
   245,  /* (166) select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   245,  /* (167) select ::= LP select RP */
   261,  /* (168) union ::= select */
   261,  /* (169) union ::= union UNION ALL select */
   196,  /* (170) cmd ::= union */
   245,  /* (171) select ::= SELECT selcollist */
   262,  /* (172) sclp ::= selcollist COMMA */
   262,  /* (173) sclp ::= */
   248,  /* (174) selcollist ::= sclp distinct expr as */
   248,  /* (175) selcollist ::= sclp STAR */
   265,  /* (176) as ::= AS ids */
   265,  /* (177) as ::= ids */
   265,  /* (178) as ::= */
   263,  /* (179) distinct ::= DISTINCT */
   263,  /* (180) distinct ::= */
   249,  /* (181) from ::= FROM tablelist */
   249,  /* (182) from ::= FROM sub */
   267,  /* (183) sub ::= LP union RP */
   267,  /* (184) sub ::= LP union RP ids */
   267,  /* (185) sub ::= sub COMMA LP union RP ids */
   266,  /* (186) tablelist ::= ids cpxName */
   266,  /* (187) tablelist ::= ids cpxName ids */
   266,  /* (188) tablelist ::= tablelist COMMA ids cpxName */
   266,  /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
   268,  /* (190) tmvar ::= VARIABLE */
   251,  /* (191) interval_opt ::= INTERVAL LP tmvar RP */
   251,  /* (192) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   251,  /* (193) interval_opt ::= */
   253,  /* (194) session_option ::= */
   253,  /* (195) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   254,  /* (196) windowstate_option ::= */
   254,  /* (197) windowstate_option ::= STATE_WINDOW LP ids RP */
   255,  /* (198) fill_opt ::= */
   255,  /* (199) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   255,  /* (200) fill_opt ::= FILL LP ID RP */
   252,  /* (201) sliding_opt ::= SLIDING LP tmvar RP */
   252,  /* (202) sliding_opt ::= */
   258,  /* (203) orderby_opt ::= */
   258,  /* (204) orderby_opt ::= ORDER BY sortlist */
   269,  /* (205) sortlist ::= sortlist COMMA item sortorder */
   269,  /* (206) sortlist ::= item sortorder */
   271,  /* (207) item ::= ids cpxName */
   272,  /* (208) sortorder ::= ASC */
   272,  /* (209) sortorder ::= DESC */
   272,  /* (210) sortorder ::= */
   256,  /* (211) groupby_opt ::= */
   256,  /* (212) groupby_opt ::= GROUP BY grouplist */
   273,  /* (213) grouplist ::= grouplist COMMA item */
   273,  /* (214) grouplist ::= item */
   257,  /* (215) having_opt ::= */
   257,  /* (216) having_opt ::= HAVING expr */
   260,  /* (217) limit_opt ::= */
   260,  /* (218) limit_opt ::= LIMIT signed */
   260,  /* (219) limit_opt ::= LIMIT signed OFFSET signed */
   260,  /* (220) limit_opt ::= LIMIT signed COMMA signed */
   259,  /* (221) slimit_opt ::= */
   259,  /* (222) slimit_opt ::= SLIMIT signed */
   259,  /* (223) slimit_opt ::= SLIMIT signed SOFFSET signed */
   259,  /* (224) slimit_opt ::= SLIMIT signed COMMA signed */
   250,  /* (225) where_opt ::= */
   250,  /* (226) where_opt ::= WHERE expr */
   264,  /* (227) expr ::= LP expr RP */
   264,  /* (228) expr ::= ID */
   264,  /* (229) expr ::= ID DOT ID */
   264,  /* (230) expr ::= ID DOT STAR */
   264,  /* (231) expr ::= INTEGER */
   264,  /* (232) expr ::= MINUS INTEGER */
   264,  /* (233) expr ::= PLUS INTEGER */
   264,  /* (234) expr ::= FLOAT */
   264,  /* (235) expr ::= MINUS FLOAT */
   264,  /* (236) expr ::= PLUS FLOAT */
   264,  /* (237) expr ::= STRING */
   264,  /* (238) expr ::= NOW */
   264,  /* (239) expr ::= VARIABLE */
   264,  /* (240) expr ::= PLUS VARIABLE */
   264,  /* (241) expr ::= MINUS VARIABLE */
   264,  /* (242) expr ::= BOOL */
   264,  /* (243) expr ::= NULL */
   264,  /* (244) expr ::= ID LP exprlist RP */
   264,  /* (245) expr ::= ID LP STAR RP */
   264,  /* (246) expr ::= expr IS NULL */
   264,  /* (247) expr ::= expr IS NOT NULL */
   264,  /* (248) expr ::= expr LT expr */
   264,  /* (249) expr ::= expr GT expr */
   264,  /* (250) expr ::= expr LE expr */
   264,  /* (251) expr ::= expr GE expr */
   264,  /* (252) expr ::= expr NE expr */
   264,  /* (253) expr ::= expr EQ expr */
   264,  /* (254) expr ::= expr BETWEEN expr AND expr */
   264,  /* (255) expr ::= expr AND expr */
   264,  /* (256) expr ::= expr OR expr */
   264,  /* (257) expr ::= expr PLUS expr */
   264,  /* (258) expr ::= expr MINUS expr */
   264,  /* (259) expr ::= expr STAR expr */
   264,  /* (260) expr ::= expr SLASH expr */
   264,  /* (261) expr ::= expr REM expr */
   264,  /* (262) expr ::= expr LIKE expr */
   264,  /* (263) expr ::= expr IN LP exprlist RP */
   204,  /* (264) exprlist ::= exprlist COMMA expritem */
   204,  /* (265) exprlist ::= expritem */
   274,  /* (266) expritem ::= expr */
   274,  /* (267) expritem ::= */
   196,  /* (268) cmd ::= RESET QUERY CACHE */
   196,  /* (269) cmd ::= SYNCDB ids REPLICA */
   196,  /* (270) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   196,  /* (271) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   196,  /* (272) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   196,  /* (273) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   196,  /* (274) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   196,  /* (275) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   196,  /* (276) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   196,  /* (277) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   196,  /* (278) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   196,  /* (279) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   196,  /* (280) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   196,  /* (281) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   196,  /* (282) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   196,  /* (283) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   196,  /* (284) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   196,  /* (285) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   196,  /* (286) cmd ::= KILL CONNECTION INTEGER */
   196,  /* (287) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   196,  /* (288) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (34) cmd ::= DROP FUNCTION ids */
   -3,  /* (35) cmd ::= DROP DNODE ids */
   -3,  /* (36) cmd ::= DROP USER ids */
   -3,  /* (37) cmd ::= DROP ACCOUNT ids */
   -2,  /* (38) cmd ::= USE ids */
   -3,  /* (39) cmd ::= DESCRIBE ids cpxName */
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
   -2,  /* (162) tagitem ::= MINUS INTEGER */
   -2,  /* (163) tagitem ::= MINUS FLOAT */
   -2,  /* (164) tagitem ::= PLUS INTEGER */
   -2,  /* (165) tagitem ::= PLUS FLOAT */
  -14,  /* (166) select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
   -3,  /* (167) select ::= LP select RP */
   -1,  /* (168) union ::= select */
   -4,  /* (169) union ::= union UNION ALL select */
   -1,  /* (170) cmd ::= union */
   -2,  /* (171) select ::= SELECT selcollist */
   -2,  /* (172) sclp ::= selcollist COMMA */
    0,  /* (173) sclp ::= */
   -4,  /* (174) selcollist ::= sclp distinct expr as */
   -2,  /* (175) selcollist ::= sclp STAR */
   -2,  /* (176) as ::= AS ids */
   -1,  /* (177) as ::= ids */
    0,  /* (178) as ::= */
   -1,  /* (179) distinct ::= DISTINCT */
    0,  /* (180) distinct ::= */
   -2,  /* (181) from ::= FROM tablelist */
   -2,  /* (182) from ::= FROM sub */
   -3,  /* (183) sub ::= LP union RP */
   -4,  /* (184) sub ::= LP union RP ids */
   -6,  /* (185) sub ::= sub COMMA LP union RP ids */
   -2,  /* (186) tablelist ::= ids cpxName */
   -3,  /* (187) tablelist ::= ids cpxName ids */
   -4,  /* (188) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (190) tmvar ::= VARIABLE */
   -4,  /* (191) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (192) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (193) interval_opt ::= */
    0,  /* (194) session_option ::= */
   -7,  /* (195) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (196) windowstate_option ::= */
   -4,  /* (197) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (198) fill_opt ::= */
   -6,  /* (199) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (200) fill_opt ::= FILL LP ID RP */
   -4,  /* (201) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (202) sliding_opt ::= */
    0,  /* (203) orderby_opt ::= */
   -3,  /* (204) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (205) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (206) sortlist ::= item sortorder */
   -2,  /* (207) item ::= ids cpxName */
   -1,  /* (208) sortorder ::= ASC */
   -1,  /* (209) sortorder ::= DESC */
    0,  /* (210) sortorder ::= */
    0,  /* (211) groupby_opt ::= */
   -3,  /* (212) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (213) grouplist ::= grouplist COMMA item */
   -1,  /* (214) grouplist ::= item */
    0,  /* (215) having_opt ::= */
   -2,  /* (216) having_opt ::= HAVING expr */
    0,  /* (217) limit_opt ::= */
   -2,  /* (218) limit_opt ::= LIMIT signed */
   -4,  /* (219) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (220) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (221) slimit_opt ::= */
   -2,  /* (222) slimit_opt ::= SLIMIT signed */
   -4,  /* (223) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (224) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (225) where_opt ::= */
   -2,  /* (226) where_opt ::= WHERE expr */
   -3,  /* (227) expr ::= LP expr RP */
   -1,  /* (228) expr ::= ID */
   -3,  /* (229) expr ::= ID DOT ID */
   -3,  /* (230) expr ::= ID DOT STAR */
   -1,  /* (231) expr ::= INTEGER */
   -2,  /* (232) expr ::= MINUS INTEGER */
   -2,  /* (233) expr ::= PLUS INTEGER */
   -1,  /* (234) expr ::= FLOAT */
   -2,  /* (235) expr ::= MINUS FLOAT */
   -2,  /* (236) expr ::= PLUS FLOAT */
   -1,  /* (237) expr ::= STRING */
   -1,  /* (238) expr ::= NOW */
   -1,  /* (239) expr ::= VARIABLE */
   -2,  /* (240) expr ::= PLUS VARIABLE */
   -2,  /* (241) expr ::= MINUS VARIABLE */
   -1,  /* (242) expr ::= BOOL */
   -1,  /* (243) expr ::= NULL */
   -4,  /* (244) expr ::= ID LP exprlist RP */
   -4,  /* (245) expr ::= ID LP STAR RP */
   -3,  /* (246) expr ::= expr IS NULL */
   -4,  /* (247) expr ::= expr IS NOT NULL */
   -3,  /* (248) expr ::= expr LT expr */
   -3,  /* (249) expr ::= expr GT expr */
   -3,  /* (250) expr ::= expr LE expr */
   -3,  /* (251) expr ::= expr GE expr */
   -3,  /* (252) expr ::= expr NE expr */
   -3,  /* (253) expr ::= expr EQ expr */
   -5,  /* (254) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (255) expr ::= expr AND expr */
   -3,  /* (256) expr ::= expr OR expr */
   -3,  /* (257) expr ::= expr PLUS expr */
   -3,  /* (258) expr ::= expr MINUS expr */
   -3,  /* (259) expr ::= expr STAR expr */
   -3,  /* (260) expr ::= expr SLASH expr */
   -3,  /* (261) expr ::= expr REM expr */
   -3,  /* (262) expr ::= expr LIKE expr */
   -5,  /* (263) expr ::= expr IN LP exprlist RP */
   -3,  /* (264) exprlist ::= exprlist COMMA expritem */
   -1,  /* (265) exprlist ::= expritem */
   -1,  /* (266) expritem ::= expr */
    0,  /* (267) expritem ::= */
   -3,  /* (268) cmd ::= RESET QUERY CACHE */
   -3,  /* (269) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (270) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (271) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (272) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (273) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (274) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (275) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (276) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (277) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (278) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (279) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (280) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (281) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (282) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (283) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (284) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (285) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (286) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (287) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (288) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
#line 63 "sql.y"
{}
#line 2536 "sql.c"
        break;
      case 1: /* cmd ::= SHOW DATABASES */
#line 66 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
#line 2541 "sql.c"
        break;
      case 2: /* cmd ::= SHOW TOPICS */
#line 67 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
#line 2546 "sql.c"
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
#line 68 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNCTION, 0, 0);}
#line 2551 "sql.c"
        break;
      case 4: /* cmd ::= SHOW MNODES */
#line 69 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
#line 2556 "sql.c"
        break;
      case 5: /* cmd ::= SHOW DNODES */
#line 70 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
#line 2561 "sql.c"
        break;
      case 6: /* cmd ::= SHOW ACCOUNTS */
#line 71 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
#line 2566 "sql.c"
        break;
      case 7: /* cmd ::= SHOW USERS */
#line 72 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
#line 2571 "sql.c"
        break;
      case 8: /* cmd ::= SHOW MODULES */
#line 74 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
#line 2576 "sql.c"
        break;
      case 9: /* cmd ::= SHOW QUERIES */
#line 75 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
#line 2581 "sql.c"
        break;
      case 10: /* cmd ::= SHOW CONNECTIONS */
#line 76 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
#line 2586 "sql.c"
        break;
      case 11: /* cmd ::= SHOW STREAMS */
#line 77 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
#line 2591 "sql.c"
        break;
      case 12: /* cmd ::= SHOW VARIABLES */
#line 78 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
#line 2596 "sql.c"
        break;
      case 13: /* cmd ::= SHOW SCORES */
#line 79 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
#line 2601 "sql.c"
        break;
      case 14: /* cmd ::= SHOW GRANTS */
#line 80 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
#line 2606 "sql.c"
        break;
      case 15: /* cmd ::= SHOW VNODES */
#line 82 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
#line 2611 "sql.c"
        break;
      case 16: /* cmd ::= SHOW VNODES ids */
#line 83 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
#line 2616 "sql.c"
        break;
      case 17: /* dbPrefix ::= */
#line 87 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
#line 2621 "sql.c"
        break;
      case 18: /* dbPrefix ::= ids DOT */
#line 88 "sql.y"
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
#line 2626 "sql.c"
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* cpxName ::= */
#line 91 "sql.y"
{yymsp[1].minor.yy0.n = 0;  }
#line 2632 "sql.c"
        break;
      case 20: /* cpxName ::= DOT ids */
#line 92 "sql.y"
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
#line 2637 "sql.c"
        break;
      case 21: /* cmd ::= SHOW CREATE TABLE ids cpxName */
#line 94 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2645 "sql.c"
        break;
      case 22: /* cmd ::= SHOW CREATE STABLE ids cpxName */
#line 98 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2653 "sql.c"
        break;
      case 23: /* cmd ::= SHOW CREATE DATABASE ids */
#line 103 "sql.y"
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
#line 2660 "sql.c"
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES */
#line 107 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
#line 2667 "sql.c"
        break;
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
#line 111 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
#line 2674 "sql.c"
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
#line 115 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
#line 2681 "sql.c"
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
#line 119 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
#line 2690 "sql.c"
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
#line 125 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
#line 2699 "sql.c"
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
#line 131 "sql.y"
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
#line 2708 "sql.c"
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
#line 138 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
#line 2716 "sql.c"
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
#line 144 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
#line 2724 "sql.c"
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
#line 149 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
#line 2729 "sql.c"
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
#line 150 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
#line 2734 "sql.c"
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
#line 151 "sql.y"
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
#line 2739 "sql.c"
        break;
      case 35: /* cmd ::= DROP DNODE ids */
#line 153 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
#line 2744 "sql.c"
        break;
      case 36: /* cmd ::= DROP USER ids */
#line 154 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
#line 2749 "sql.c"
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
#line 155 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
#line 2754 "sql.c"
        break;
      case 38: /* cmd ::= USE ids */
#line 158 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
#line 2759 "sql.c"
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
#line 161 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 2767 "sql.c"
        break;
      case 40: /* cmd ::= ALTER USER ids PASS ids */
#line 167 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
#line 2772 "sql.c"
        break;
      case 41: /* cmd ::= ALTER USER ids PRIVILEGE ids */
#line 168 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
#line 2777 "sql.c"
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids */
#line 169 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 2782 "sql.c"
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids ids */
#line 170 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
#line 2787 "sql.c"
        break;
      case 44: /* cmd ::= ALTER LOCAL ids */
#line 171 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
#line 2792 "sql.c"
        break;
      case 45: /* cmd ::= ALTER LOCAL ids ids */
#line 172 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 2797 "sql.c"
        break;
      case 46: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 47: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==47);
#line 173 "sql.y"
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &t);}
#line 2803 "sql.c"
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
#line 176 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy341);}
#line 2808 "sql.c"
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
#line 177 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy341);}
#line 2813 "sql.c"
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
#line 181 "sql.y"
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy131);}
#line 2818 "sql.c"
        break;
      case 51: /* ids ::= ID */
      case 52: /* ids ::= STRING */ yytestcase(yyruleno==52);
#line 187 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
#line 2824 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 53: /* ifexists ::= IF EXISTS */
#line 191 "sql.y"
{ yymsp[-1].minor.yy0.n = 1;}
#line 2830 "sql.c"
        break;
      case 54: /* ifexists ::= */
      case 56: /* ifnotexists ::= */ yytestcase(yyruleno==56);
      case 180: /* distinct ::= */ yytestcase(yyruleno==180);
#line 192 "sql.y"
{ yymsp[1].minor.yy0.n = 0;}
#line 2837 "sql.c"
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
#line 195 "sql.y"
{ yymsp[-2].minor.yy0.n = 1;}
#line 2842 "sql.c"
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
#line 200 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
#line 2847 "sql.c"
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
#line 202 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy341);}
#line 2852 "sql.c"
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
#line 203 "sql.y"
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &yymsp[-2].minor.yy0);}
#line 2858 "sql.c"
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
#line 205 "sql.y"
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy163, &yymsp[0].minor.yy0, 1);}
#line 2863 "sql.c"
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
#line 206 "sql.y"
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy163, &yymsp[0].minor.yy0, 2);}
#line 2868 "sql.c"
        break;
      case 63: /* cmd ::= CREATE USER ids PASS ids */
#line 207 "sql.y"
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
#line 2873 "sql.c"
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
#line 209 "sql.y"
{ yymsp[1].minor.yy0.n = 0;   }
#line 2887 "sql.c"
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
#line 210 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
#line 2901 "sql.c"
        break;
      case 84: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
#line 240 "sql.y"
{
    yylhsminor.yy341.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy341.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy341.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy341.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy341.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy341.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy341.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy341.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy341.stat    = yymsp[0].minor.yy0;
}
#line 2916 "sql.c"
  yymsp[-8].minor.yy341 = yylhsminor.yy341;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
#line 256 "sql.y"
{ yylhsminor.yy131 = tVariantListAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy516, -1);    }
#line 2923 "sql.c"
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
#line 257 "sql.y"
{ yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[0].minor.yy516, -1); }
#line 2930 "sql.c"
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
#line 259 "sql.y"
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0); }
#line 2940 "sql.c"
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 88: /* keep ::= KEEP intitemlist */
#line 263 "sql.y"
{ yymsp[-1].minor.yy131 = yymsp[0].minor.yy131; }
#line 2946 "sql.c"
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
#line 265 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
#line 2965 "sql.c"
        break;
      case 104: /* db_optr ::= */
#line 282 "sql.y"
{setDefaultCreateDbOption(&yymsp[1].minor.yy42); yymsp[1].minor.yy42.dbType = TSDB_DB_TYPE_DEFAULT;}
#line 2970 "sql.c"
        break;
      case 105: /* db_optr ::= db_optr cache */
#line 284 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2975 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
#line 285 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2982 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
#line 286 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2989 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 108: /* db_optr ::= db_optr days */
#line 287 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2995 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 109: /* db_optr ::= db_optr minrows */
#line 288 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 3001 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
#line 289 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 3007 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
#line 290 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3014 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 112: /* db_optr ::= db_optr ctime */
#line 291 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3020 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 113: /* db_optr ::= db_optr wal */
#line 292 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3026 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 114: /* db_optr ::= db_optr fsync */
#line 293 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3032 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
#line 294 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3039 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 116: /* db_optr ::= db_optr prec */
#line 295 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.precision = yymsp[0].minor.yy0; }
#line 3045 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
#line 296 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.keep = yymsp[0].minor.yy131; }
#line 3052 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
#line 297 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3059 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
#line 298 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3066 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
#line 302 "sql.y"
{ yylhsminor.yy42 = yymsp[0].minor.yy42; yylhsminor.yy42.dbType = TSDB_DB_TYPE_TOPIC; }
#line 3073 "sql.c"
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
#line 303 "sql.y"
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3080 "sql.c"
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 122: /* alter_db_optr ::= */
#line 306 "sql.y"
{ setDefaultCreateDbOption(&yymsp[1].minor.yy42); yymsp[1].minor.yy42.dbType = TSDB_DB_TYPE_DEFAULT;}
#line 3086 "sql.c"
        break;
      case 132: /* typename ::= ids */
#line 326 "sql.y"
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy163, &yymsp[0].minor.yy0);
}
#line 3094 "sql.c"
  yymsp[0].minor.yy163 = yylhsminor.yy163;
        break;
      case 133: /* typename ::= ids LP signed RP */
#line 332 "sql.y"
{
  if (yymsp[-1].minor.yy459 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy163, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy459;  // negative value of name length
    tSetColumnType(&yylhsminor.yy163, &yymsp[-3].minor.yy0);
  }
}
#line 3108 "sql.c"
  yymsp[-3].minor.yy163 = yylhsminor.yy163;
        break;
      case 134: /* typename ::= ids UNSIGNED */
#line 343 "sql.y"
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy163, &yymsp[-1].minor.yy0);
}
#line 3118 "sql.c"
  yymsp[-1].minor.yy163 = yylhsminor.yy163;
        break;
      case 135: /* signed ::= INTEGER */
#line 350 "sql.y"
{ yylhsminor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3124 "sql.c"
  yymsp[0].minor.yy459 = yylhsminor.yy459;
        break;
      case 136: /* signed ::= PLUS INTEGER */
#line 351 "sql.y"
{ yymsp[-1].minor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 3130 "sql.c"
        break;
      case 137: /* signed ::= MINUS INTEGER */
#line 352 "sql.y"
{ yymsp[-1].minor.yy459 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
#line 3135 "sql.c"
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
#line 358 "sql.y"
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy272;}
#line 3140 "sql.c"
        break;
      case 142: /* create_table_list ::= create_from_stable */
#line 362 "sql.y"
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy272 = pCreateTable;
}
#line 3152 "sql.c"
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
#line 371 "sql.y"
{
  taosArrayPush(yymsp[-1].minor.yy272->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy272 = yymsp[-1].minor.yy272;
}
#line 3161 "sql.c"
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
#line 377 "sql.y"
{
  yylhsminor.yy272 = tSetCreateTableInfo(yymsp[-1].minor.yy131, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
#line 3173 "sql.c"
  yymsp[-5].minor.yy272 = yylhsminor.yy272;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
#line 387 "sql.y"
{
  yylhsminor.yy272 = tSetCreateTableInfo(yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 3185 "sql.c"
  yymsp[-9].minor.yy272 = yylhsminor.yy272;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
#line 398 "sql.y"
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy131, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 3195 "sql.c"
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
#line 404 "sql.y"
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
#line 3205 "sql.c"
  yymsp[-12].minor.yy96 = yylhsminor.yy96;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
#line 412 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy0); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
#line 3211 "sql.c"
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 149: /* tagNamelist ::= ids */
#line 413 "sql.y"
{yylhsminor.yy131 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy0);}
#line 3217 "sql.c"
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
#line 417 "sql.y"
{
  yylhsminor.yy272 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy256, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
#line 3229 "sql.c"
  yymsp[-4].minor.yy272 = yylhsminor.yy272;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
#line 428 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy163); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
#line 3235 "sql.c"
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 152: /* columnlist ::= column */
#line 429 "sql.y"
{yylhsminor.yy131 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy163);}
#line 3241 "sql.c"
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 153: /* column ::= ids typename */
#line 433 "sql.y"
{
  tSetColumnInfo(&yylhsminor.yy163, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy163);
}
#line 3249 "sql.c"
  yymsp[-1].minor.yy163 = yylhsminor.yy163;
        break;
      case 160: /* tagitem ::= NULL */
#line 448 "sql.y"
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0); }
#line 3255 "sql.c"
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 161: /* tagitem ::= NOW */
#line 449 "sql.y"
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0);}
#line 3261 "sql.c"
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 162: /* tagitem ::= MINUS INTEGER */
      case 163: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==165);
#line 451 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy516, &yymsp[-1].minor.yy0);
}
#line 3275 "sql.c"
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 166: /* select ::= SELECT selcollist from where_opt interval_opt sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
#line 482 "sql.y"
{
  yylhsminor.yy256 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy131, yymsp[-11].minor.yy544, yymsp[-10].minor.yy46, yymsp[-4].minor.yy131, yymsp[-2].minor.yy131, &yymsp[-9].minor.yy530, &yymsp[-7].minor.yy39, &yymsp[-6].minor.yy538, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy131, &yymsp[0].minor.yy284, &yymsp[-1].minor.yy284, yymsp[-3].minor.yy46);
}
#line 3283 "sql.c"
  yymsp[-13].minor.yy256 = yylhsminor.yy256;
        break;
      case 167: /* select ::= LP select RP */
#line 486 "sql.y"
{yymsp[-2].minor.yy256 = yymsp[-1].minor.yy256;}
#line 3289 "sql.c"
        break;
      case 168: /* union ::= select */
#line 490 "sql.y"
{ yylhsminor.yy131 = setSubclause(NULL, yymsp[0].minor.yy256); }
#line 3294 "sql.c"
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 169: /* union ::= union UNION ALL select */
#line 491 "sql.y"
{ yylhsminor.yy131 = appendSelectClause(yymsp[-3].minor.yy131, yymsp[0].minor.yy256); }
#line 3300 "sql.c"
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 170: /* cmd ::= union */
#line 493 "sql.y"
{ setSqlInfo(pInfo, yymsp[0].minor.yy131, NULL, TSDB_SQL_SELECT); }
#line 3306 "sql.c"
        break;
      case 171: /* select ::= SELECT selcollist */
#line 500 "sql.y"
{
  yylhsminor.yy256 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy131, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
#line 3313 "sql.c"
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 172: /* sclp ::= selcollist COMMA */
#line 512 "sql.y"
{yylhsminor.yy131 = yymsp[-1].minor.yy131;}
#line 3319 "sql.c"
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 173: /* sclp ::= */
      case 203: /* orderby_opt ::= */ yytestcase(yyruleno==203);
#line 513 "sql.y"
{yymsp[1].minor.yy131 = 0;}
#line 3326 "sql.c"
        break;
      case 174: /* selcollist ::= sclp distinct expr as */
#line 514 "sql.y"
{
   yylhsminor.yy131 = tSqlExprListAppend(yymsp[-3].minor.yy131, yymsp[-1].minor.yy46,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
#line 3333 "sql.c"
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 175: /* selcollist ::= sclp STAR */
#line 518 "sql.y"
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy131 = tSqlExprListAppend(yymsp[-1].minor.yy131, pNode, 0, 0);
}
#line 3342 "sql.c"
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 176: /* as ::= AS ids */
#line 526 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
#line 3348 "sql.c"
        break;
      case 177: /* as ::= ids */
#line 527 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
#line 3353 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* as ::= */
#line 528 "sql.y"
{ yymsp[1].minor.yy0.n = 0;  }
#line 3359 "sql.c"
        break;
      case 179: /* distinct ::= DISTINCT */
#line 531 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
#line 3364 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* from ::= FROM tablelist */
      case 182: /* from ::= FROM sub */ yytestcase(yyruleno==182);
#line 537 "sql.y"
{yymsp[-1].minor.yy544 = yymsp[0].minor.yy544;}
#line 3371 "sql.c"
        break;
      case 183: /* sub ::= LP union RP */
#line 542 "sql.y"
{yymsp[-2].minor.yy544 = addSubqueryElem(NULL, yymsp[-1].minor.yy131, NULL);}
#line 3376 "sql.c"
        break;
      case 184: /* sub ::= LP union RP ids */
#line 543 "sql.y"
{yymsp[-3].minor.yy544 = addSubqueryElem(NULL, yymsp[-2].minor.yy131, &yymsp[0].minor.yy0);}
#line 3381 "sql.c"
        break;
      case 185: /* sub ::= sub COMMA LP union RP ids */
#line 544 "sql.y"
{yylhsminor.yy544 = addSubqueryElem(yymsp[-5].minor.yy544, yymsp[-2].minor.yy131, &yymsp[0].minor.yy0);}
#line 3386 "sql.c"
  yymsp[-5].minor.yy544 = yylhsminor.yy544;
        break;
      case 186: /* tablelist ::= ids cpxName */
#line 548 "sql.y"
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
#line 3395 "sql.c"
  yymsp[-1].minor.yy544 = yylhsminor.yy544;
        break;
      case 187: /* tablelist ::= ids cpxName ids */
#line 553 "sql.y"
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
#line 3404 "sql.c"
  yymsp[-2].minor.yy544 = yylhsminor.yy544;
        break;
      case 188: /* tablelist ::= tablelist COMMA ids cpxName */
#line 558 "sql.y"
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(yymsp[-3].minor.yy544, &yymsp[-1].minor.yy0, NULL);
}
#line 3413 "sql.c"
  yymsp[-3].minor.yy544 = yylhsminor.yy544;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName ids */
#line 563 "sql.y"
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(yymsp[-4].minor.yy544, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
#line 3422 "sql.c"
  yymsp[-4].minor.yy544 = yylhsminor.yy544;
        break;
      case 190: /* tmvar ::= VARIABLE */
#line 570 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
#line 3428 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 191: /* interval_opt ::= INTERVAL LP tmvar RP */
#line 573 "sql.y"
{yymsp[-3].minor.yy530.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy530.offset.n = 0;}
#line 3434 "sql.c"
        break;
      case 192: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
#line 574 "sql.y"
{yymsp[-5].minor.yy530.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy530.offset = yymsp[-1].minor.yy0;}
#line 3439 "sql.c"
        break;
      case 193: /* interval_opt ::= */
#line 575 "sql.y"
{memset(&yymsp[1].minor.yy530, 0, sizeof(yymsp[1].minor.yy530));}
#line 3444 "sql.c"
        break;
      case 194: /* session_option ::= */
#line 578 "sql.y"
{yymsp[1].minor.yy39.col.n = 0; yymsp[1].minor.yy39.gap.n = 0;}
#line 3449 "sql.c"
        break;
      case 195: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
#line 579 "sql.y"
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy39.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy39.gap = yymsp[-1].minor.yy0;
}
#line 3458 "sql.c"
        break;
      case 196: /* windowstate_option ::= */
#line 585 "sql.y"
{ yymsp[1].minor.yy538.col.n = 0; yymsp[1].minor.yy538.col.z = NULL;}
#line 3463 "sql.c"
        break;
      case 197: /* windowstate_option ::= STATE_WINDOW LP ids RP */
#line 586 "sql.y"
{ yymsp[-3].minor.yy538.col = yymsp[-1].minor.yy0; }
#line 3468 "sql.c"
        break;
      case 198: /* fill_opt ::= */
#line 590 "sql.y"
{ yymsp[1].minor.yy131 = 0;     }
#line 3473 "sql.c"
        break;
      case 199: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
#line 591 "sql.y"
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy131, &A, -1, 0);
    yymsp[-5].minor.yy131 = yymsp[-1].minor.yy131;
}
#line 3485 "sql.c"
        break;
      case 200: /* fill_opt ::= FILL LP ID RP */
#line 600 "sql.y"
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy131 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
#line 3493 "sql.c"
        break;
      case 201: /* sliding_opt ::= SLIDING LP tmvar RP */
#line 606 "sql.y"
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
#line 3498 "sql.c"
        break;
      case 202: /* sliding_opt ::= */
#line 607 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
#line 3503 "sql.c"
        break;
      case 204: /* orderby_opt ::= ORDER BY sortlist */
#line 619 "sql.y"
{yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
#line 3508 "sql.c"
        break;
      case 205: /* sortlist ::= sortlist COMMA item sortorder */
#line 621 "sql.y"
{
    yylhsminor.yy131 = tVariantListAppend(yymsp[-3].minor.yy131, &yymsp[-1].minor.yy516, yymsp[0].minor.yy43);
}
#line 3515 "sql.c"
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 206: /* sortlist ::= item sortorder */
#line 625 "sql.y"
{
  yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[-1].minor.yy516, yymsp[0].minor.yy43);
}
#line 3523 "sql.c"
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 207: /* item ::= ids cpxName */
#line 630 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy516, &yymsp[-1].minor.yy0);
}
#line 3534 "sql.c"
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 208: /* sortorder ::= ASC */
#line 638 "sql.y"
{ yymsp[0].minor.yy43 = TSDB_ORDER_ASC; }
#line 3540 "sql.c"
        break;
      case 209: /* sortorder ::= DESC */
#line 639 "sql.y"
{ yymsp[0].minor.yy43 = TSDB_ORDER_DESC;}
#line 3545 "sql.c"
        break;
      case 210: /* sortorder ::= */
#line 640 "sql.y"
{ yymsp[1].minor.yy43 = TSDB_ORDER_ASC; }
#line 3550 "sql.c"
        break;
      case 211: /* groupby_opt ::= */
#line 648 "sql.y"
{ yymsp[1].minor.yy131 = 0;}
#line 3555 "sql.c"
        break;
      case 212: /* groupby_opt ::= GROUP BY grouplist */
#line 649 "sql.y"
{ yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
#line 3560 "sql.c"
        break;
      case 213: /* grouplist ::= grouplist COMMA item */
#line 651 "sql.y"
{
  yylhsminor.yy131 = tVariantListAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy516, -1);
}
#line 3567 "sql.c"
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 214: /* grouplist ::= item */
#line 655 "sql.y"
{
  yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[0].minor.yy516, -1);
}
#line 3575 "sql.c"
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 215: /* having_opt ::= */
      case 225: /* where_opt ::= */ yytestcase(yyruleno==225);
      case 267: /* expritem ::= */ yytestcase(yyruleno==267);
#line 662 "sql.y"
{yymsp[1].minor.yy46 = 0;}
#line 3583 "sql.c"
        break;
      case 216: /* having_opt ::= HAVING expr */
      case 226: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==226);
#line 663 "sql.y"
{yymsp[-1].minor.yy46 = yymsp[0].minor.yy46;}
#line 3589 "sql.c"
        break;
      case 217: /* limit_opt ::= */
      case 221: /* slimit_opt ::= */ yytestcase(yyruleno==221);
#line 667 "sql.y"
{yymsp[1].minor.yy284.limit = -1; yymsp[1].minor.yy284.offset = 0;}
#line 3595 "sql.c"
        break;
      case 218: /* limit_opt ::= LIMIT signed */
      case 222: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==222);
#line 668 "sql.y"
{yymsp[-1].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-1].minor.yy284.offset = 0;}
#line 3601 "sql.c"
        break;
      case 219: /* limit_opt ::= LIMIT signed OFFSET signed */
#line 670 "sql.y"
{ yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy459;}
#line 3606 "sql.c"
        break;
      case 220: /* limit_opt ::= LIMIT signed COMMA signed */
#line 672 "sql.y"
{ yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy459;}
#line 3611 "sql.c"
        break;
      case 223: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
#line 678 "sql.y"
{yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy459;}
#line 3616 "sql.c"
        break;
      case 224: /* slimit_opt ::= SLIMIT signed COMMA signed */
#line 680 "sql.y"
{yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy459;}
#line 3621 "sql.c"
        break;
      case 227: /* expr ::= LP expr RP */
#line 693 "sql.y"
{yylhsminor.yy46 = yymsp[-1].minor.yy46; yylhsminor.yy46->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy46->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
#line 3626 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 228: /* expr ::= ID */
#line 695 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
#line 3632 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 229: /* expr ::= ID DOT ID */
#line 696 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
#line 3638 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 230: /* expr ::= ID DOT STAR */
#line 697 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
#line 3644 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 231: /* expr ::= INTEGER */
#line 699 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
#line 3650 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 232: /* expr ::= MINUS INTEGER */
      case 233: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==233);
#line 700 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
#line 3657 "sql.c"
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 234: /* expr ::= FLOAT */
#line 702 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
#line 3663 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 235: /* expr ::= MINUS FLOAT */
      case 236: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==236);
#line 703 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
#line 3670 "sql.c"
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 237: /* expr ::= STRING */
#line 705 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
#line 3676 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 238: /* expr ::= NOW */
#line 706 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
#line 3682 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 239: /* expr ::= VARIABLE */
#line 707 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
#line 3688 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 240: /* expr ::= PLUS VARIABLE */
      case 241: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==241);
#line 708 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
#line 3695 "sql.c"
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 242: /* expr ::= BOOL */
#line 710 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
#line 3701 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 243: /* expr ::= NULL */
#line 711 "sql.y"
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
#line 3707 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 244: /* expr ::= ID LP exprlist RP */
#line 714 "sql.y"
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(yymsp[-1].minor.yy131, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 3713 "sql.c"
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 245: /* expr ::= ID LP STAR RP */
#line 717 "sql.y"
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 3719 "sql.c"
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 246: /* expr ::= expr IS NULL */
#line 720 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, NULL, TK_ISNULL);}
#line 3725 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 247: /* expr ::= expr IS NOT NULL */
#line 721 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-3].minor.yy46, NULL, TK_NOTNULL);}
#line 3731 "sql.c"
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 248: /* expr ::= expr LT expr */
#line 724 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LT);}
#line 3737 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 249: /* expr ::= expr GT expr */
#line 725 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GT);}
#line 3743 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 250: /* expr ::= expr LE expr */
#line 726 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LE);}
#line 3749 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 251: /* expr ::= expr GE expr */
#line 727 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GE);}
#line 3755 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 252: /* expr ::= expr NE expr */
#line 728 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_NE);}
#line 3761 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 253: /* expr ::= expr EQ expr */
#line 729 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_EQ);}
#line 3767 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 254: /* expr ::= expr BETWEEN expr AND expr */
#line 731 "sql.y"
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy46); yylhsminor.yy46 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy46, yymsp[-2].minor.yy46, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy46, TK_LE), TK_AND);}
#line 3773 "sql.c"
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 255: /* expr ::= expr AND expr */
#line 733 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_AND);}
#line 3779 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 256: /* expr ::= expr OR expr */
#line 734 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_OR); }
#line 3785 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 257: /* expr ::= expr PLUS expr */
#line 737 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_PLUS);  }
#line 3791 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 258: /* expr ::= expr MINUS expr */
#line 738 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MINUS); }
#line 3797 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 259: /* expr ::= expr STAR expr */
#line 739 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_STAR);  }
#line 3803 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 260: /* expr ::= expr SLASH expr */
#line 740 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_DIVIDE);}
#line 3809 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 261: /* expr ::= expr REM expr */
#line 741 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_REM);   }
#line 3815 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 262: /* expr ::= expr LIKE expr */
#line 744 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LIKE);  }
#line 3821 "sql.c"
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 263: /* expr ::= expr IN LP exprlist RP */
#line 747 "sql.y"
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-4].minor.yy46, (tSqlExpr*)yymsp[-1].minor.yy131, TK_IN); }
#line 3827 "sql.c"
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 264: /* exprlist ::= exprlist COMMA expritem */
#line 755 "sql.y"
{yylhsminor.yy131 = tSqlExprListAppend(yymsp[-2].minor.yy131,yymsp[0].minor.yy46,0, 0);}
#line 3833 "sql.c"
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 265: /* exprlist ::= expritem */
#line 756 "sql.y"
{yylhsminor.yy131 = tSqlExprListAppend(0,yymsp[0].minor.yy46,0, 0);}
#line 3839 "sql.c"
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 266: /* expritem ::= expr */
#line 757 "sql.y"
{yylhsminor.yy46 = yymsp[0].minor.yy46;}
#line 3845 "sql.c"
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 268: /* cmd ::= RESET QUERY CACHE */
#line 761 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
#line 3851 "sql.c"
        break;
      case 269: /* cmd ::= SYNCDB ids REPLICA */
#line 764 "sql.y"
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
#line 3856 "sql.c"
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
#line 767 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3865 "sql.c"
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
#line 773 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3878 "sql.c"
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
#line 783 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3887 "sql.c"
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
#line 790 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3896 "sql.c"
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
#line 795 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3909 "sql.c"
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
#line 805 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3925 "sql.c"
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
#line 818 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy516, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3939 "sql.c"
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
#line 829 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3948 "sql.c"
        break;
      case 278: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
#line 836 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3957 "sql.c"
        break;
      case 279: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
#line 842 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3970 "sql.c"
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
#line 852 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3979 "sql.c"
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
#line 859 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 3988 "sql.c"
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
#line 864 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4001 "sql.c"
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
#line 874 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4017 "sql.c"
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
#line 887 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy516, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4031 "sql.c"
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
#line 898 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 4040 "sql.c"
        break;
      case 286: /* cmd ::= KILL CONNECTION INTEGER */
#line 905 "sql.y"
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
#line 4045 "sql.c"
        break;
      case 287: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
#line 906 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
#line 4050 "sql.c"
        break;
      case 288: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
#line 907 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
#line 4055 "sql.c"
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
#line 37 "sql.y"

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
#line 4140 "sql.c"
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
#line 61 "sql.y"
#line 4167 "sql.c"
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
