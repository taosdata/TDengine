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

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
#define YYNOCODE 264
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateTableSql* yy14;
  int yy20;
  SSqlNode* yy116;
  tSqlExpr* yy118;
  SArray* yy159;
  SIntervalVal yy184;
  SCreatedTableInfo yy206;
  SRelationInfo* yy236;
  SSessionWindowVal yy249;
  int64_t yy317;
  SCreateDbInfo yy322;
  SCreateAcctInfo yy351;
  TAOS_FIELD yy407;
  SLimitVal yy440;
  tVariant yy488;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             317
#define YYNRULE              270
#define YYNTOKEN             187
#define YY_MAX_SHIFT         316
#define YY_MIN_SHIFTREDUCE   511
#define YY_MAX_SHIFTREDUCE   780
#define YY_ERROR_ACTION      781
#define YY_ACCEPT_ACTION     782
#define YY_NO_ACTION         783
#define YY_MIN_REDUCE        784
#define YY_MAX_REDUCE        1053
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
#define YY_ACTTAB_COUNT (685)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   925,  559,  206,  314,  211,  141,  952,    3,  168,  560,
 /*    10 */   782,  316,  134,   47,   48,  141,   51,   52,   30,  183,
 /*    20 */   217,   41,  183,   50,  264,   55,   53,   57,   54, 1034,
 /*    30 */   931,  214, 1035,   46,   45,   17,  183,   44,   43,   42,
 /*    40 */    47,   48,  223,   51,   52,  213, 1035,  217,   41,  559,
 /*    50 */    50,  264,   55,   53,   57,   54,  943,  560,  181,  208,
 /*    60 */    46,   45,  928,  222,   44,   43,   42,   48,  949,   51,
 /*    70 */    52,  244,  983,  217,   41,  249,   50,  264,   55,   53,
 /*    80 */    57,   54,  984,  638,  259,   85,   46,   45,  280,  931,
 /*    90 */    44,   43,   42,  512,  513,  514,  515,  516,  517,  518,
 /*   100 */   519,  520,  521,  522,  523,  524,  315,  943,  187,  207,
 /*   110 */    70,  290,  289,   47,   48,   30,   51,   52,  300,  919,
 /*   120 */   217,   41,  209,   50,  264,   55,   53,   57,   54,   44,
 /*   130 */    43,   42,  724,   46,   45,  674,  224,   44,   43,   42,
 /*   140 */    47,   49,   24,   51,   52,  228,  141,  217,   41,  559,
 /*   150 */    50,  264,   55,   53,   57,   54,  220,  560,  105,  928,
 /*   160 */    46,   45,  931,  300,   44,   43,   42,   23,  278,  309,
 /*   170 */   308,  277,  276,  275,  307,  274,  306,  305,  304,  273,
 /*   180 */   303,  302,  891,   30,  879,  880,  881,  882,  883,  884,
 /*   190 */   885,  886,  887,  888,  889,  890,  892,  893,   51,   52,
 /*   200 */   830, 1031,  217,   41,  167,   50,  264,   55,   53,   57,
 /*   210 */    54,  261,   18,   78,  230,   46,   45,  287,  286,   44,
 /*   220 */    43,   42,  216,  739,  221,   30,  728,  928,  731,  192,
 /*   230 */   734,  216,  739,  310, 1030,  728,  193,  731,  236,  734,
 /*   240 */    30,  118,  117,  191,  677,  559,  240,  239,   55,   53,
 /*   250 */    57,   54,   25,  560,  202,  203,   46,   45,  263,  931,
 /*   260 */    44,   43,   42,  202,  203,   74,  283,   61,   23,  928,
 /*   270 */   309,  308,   74,   36,  730,  307,  733,  306,  305,  304,
 /*   280 */    36,  303,  302,  899,  927,  662,  897,  898,  659,   62,
 /*   290 */   660,  900,  661,  902,  903,  901,   82,  904,  905,  103,
 /*   300 */    97,  108,  243,  917,   68,   30,  107,  113,  116,  106,
 /*   310 */   199,    5,   33,  157,  141,  110,  231,  232,  156,   92,
 /*   320 */    87,   91,  681,  226,   30,   56,   30,  914,  915,   29,
 /*   330 */   918,  729,  740,  732,   56,  175,  173,  171,  736,    1,
 /*   340 */   155,  740,  170,  121,  120,  119,  284,  736,  229,  928,
 /*   350 */   265,   46,   45,   69,  735,   44,   43,   42,  839,  666,
 /*   360 */    12,  667,  167,  735,   84,  288,   81,  292,  928,  215,
 /*   370 */   928,  313,  312,  126,  132,  130,  129,   80,  705,  706,
 /*   380 */   831,   79,  280,  929,  167,  916,  737,  245,  726,  684,
 /*   390 */    71,   31,  227,  994,  663,  282,  690,  247,  696,  697,
 /*   400 */   136,  760,   60,   20,  741,   19,   64,  648,   19,  241,
 /*   410 */   267,   31,  650,    6,   31,  269,   60, 1029,  649,   83,
 /*   420 */    28,  200,   60,  270,  727,  201,   65,   96,   95,  185,
 /*   430 */    14,   13,  993,  102,  101,   67,  218,  637,   16,   15,
 /*   440 */   664,  186,  665,  738,  115,  114,  743,  188,  182,  189,
 /*   450 */   190,  196,  197,  195,  180,  194,  184,  133, 1045,  990,
 /*   460 */   930,  989,  219,  291,   39,  951,  959,  944,  961,  135,
 /*   470 */   139,  976,  248,  975,  926,  131,  152,  151,  924,  153,
 /*   480 */   250,  154,  689,  210,  252,  150,  257,  145,  142,  842,
 /*   490 */   941,  143,  272,  144,  262,   37,  146,   66,   58,  178,
 /*   500 */    63,  260,   34,  258,  256,  281,  838,  147, 1050,  254,
 /*   510 */    93, 1049, 1047,  158,  285, 1044,   99,  148, 1043, 1041,
 /*   520 */   159,  860,  251,   35,   32,   38,  149,  179,  827,  109,
 /*   530 */   825,  111,  112,  823,  822,  233,  169,  820,  819,  818,
 /*   540 */   817,  816,  815,  172,  174,   40,  812,  810,  808,  806,
 /*   550 */   176,  803,  177,  301,  246,   72,   75,  104,  253,  977,
 /*   560 */   293,  294,  295,  296,  297,  204,  225,  298,  271,  299,
 /*   570 */   311,  780,  205,  198,  234,   88,   89,  235,  779,  237,
 /*   580 */   238,  778,  766,  765,  242,  247,  821,  814,  162,  266,
 /*   590 */   122,  861,  160,  165,  161,  164,  163,  166,  123,  124,
 /*   600 */   813,  805,  895,  125,  804,    2,    8,   73,    4,  669,
 /*   610 */    76,  691,  137,  212,  694,   86,  138,   77,  907,  255,
 /*   620 */     9,  698,  140,   26,  742,    7,   27,   11,   10,   21,
 /*   630 */    84,  744,   22,  268,  601,  597,  595,  594,  593,  590,
 /*   640 */   563,  279,   94,   90,   31,   59,  640,  639,  636,  585,
 /*   650 */   583,   98,  575,  581,  577,  579,  573,  571,  604,  603,
 /*   660 */   602,  600,  599,  100,  598,  596,  592,  591,   60,  561,
 /*   670 */   528,  784,  526,  783,  783,  783,  783,  783,  783,  127,
 /*   680 */   783,  783,  783,  783,  128,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   191,    1,  190,  191,  210,  191,  191,  194,  195,    9,
 /*    10 */   188,  189,  191,   13,   14,  191,   16,   17,  191,  252,
 /*    20 */    20,   21,  252,   23,   24,   25,   26,   27,   28,  262,
 /*    30 */   236,  261,  262,   33,   34,  252,  252,   37,   38,   39,
 /*    40 */    13,   14,  233,   16,   17,  261,  262,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  234,    9,  252,  232,
 /*    60 */    33,   34,  235,  210,   37,   38,   39,   14,  253,   16,
 /*    70 */    17,  249,  258,   20,   21,  254,   23,   24,   25,   26,
 /*    80 */    27,   28,  258,    5,  260,  197,   33,   34,   79,  236,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  234,  252,   61,
 /*   110 */   110,   33,   34,   13,   14,  191,   16,   17,   81,  231,
 /*   120 */    20,   21,  249,   23,   24,   25,   26,   27,   28,   37,
 /*   130 */    38,   39,  105,   33,   34,  109,  210,   37,   38,   39,
 /*   140 */    13,   14,  116,   16,   17,   68,  191,   20,   21,    1,
 /*   150 */    23,   24,   25,   26,   27,   28,  232,    9,   76,  235,
 /*   160 */    33,   34,  236,   81,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  209,  191,  211,  212,  213,  214,  215,  216,
 /*   190 */   217,  218,  219,  220,  221,  222,  223,  224,   16,   17,
 /*   200 */   196,  252,   20,   21,  200,   23,   24,   25,   26,   27,
 /*   210 */    28,  256,   44,  258,  137,   33,   34,  140,  141,   37,
 /*   220 */    38,   39,    1,    2,  232,  191,    5,  235,    7,   61,
 /*   230 */     9,    1,    2,  210,  252,    5,   68,    7,  135,    9,
 /*   240 */   191,   73,   74,   75,   37,    1,  143,  144,   25,   26,
 /*   250 */    27,   28,  104,    9,   33,   34,   33,   34,   37,  236,
 /*   260 */    37,   38,   39,   33,   34,  104,  232,  109,   88,  235,
 /*   270 */    90,   91,  104,  112,    5,   95,    7,   97,   98,   99,
 /*   280 */   112,  101,  102,  209,  235,    2,  212,  213,    5,  131,
 /*   290 */     7,  217,    9,  219,  220,  221,  197,  223,  224,   62,
 /*   300 */    63,   64,  134,    0,  136,  191,   69,   70,   71,   72,
 /*   310 */   142,   62,   63,   64,  191,   78,   33,   34,   69,   70,
 /*   320 */    71,   72,  115,   68,  191,  104,  191,  228,  229,  230,
 /*   330 */   231,    5,  111,    7,  104,   62,   63,   64,  117,  198,
 /*   340 */   199,  111,   69,   70,   71,   72,  232,  117,  191,  235,
 /*   350 */    15,   33,   34,  197,  133,   37,   38,   39,  196,    5,
 /*   360 */   104,    7,  200,  133,  108,  232,  110,  232,  235,   60,
 /*   370 */   235,   65,   66,   67,   62,   63,   64,  237,  124,  125,
 /*   380 */   196,  258,   79,  226,  200,  229,  117,  105,    1,  105,
 /*   390 */   250,  109,  137,  227,  111,  140,  105,  113,  105,  105,
 /*   400 */   109,  105,  109,  109,  105,  109,  109,  105,  109,  191,
 /*   410 */   105,  109,  105,  104,  109,  105,  109,  252,  105,  109,
 /*   420 */   104,  252,  109,  107,   37,  252,  129,  138,  139,  252,
 /*   430 */   138,  139,  227,  138,  139,  104,  227,  106,  138,  139,
 /*   440 */     5,  252,    7,  117,   76,   77,  111,  252,  252,  252,
 /*   450 */   252,  252,  252,  252,  252,  252,  252,  191,  236,  227,
 /*   460 */   236,  227,  227,  227,  251,  191,  191,  234,  191,  191,
 /*   470 */   191,  259,  234,  259,  234,   60,  191,  238,  191,  191,
 /*   480 */   255,  191,  117,  255,  255,  239,  255,  244,  247,  191,
 /*   490 */   248,  246,  191,  245,  122,  191,  243,  128,  127,  191,
 /*   500 */   130,  126,  191,  121,  120,  191,  191,  242,  191,  119,
 /*   510 */   191,  191,  191,  191,  191,  191,  191,  241,  191,  191,
 /*   520 */   191,  191,  118,  191,  191,  191,  240,  191,  191,  191,
 /*   530 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   540 */   191,  191,  191,  191,  191,  132,  191,  191,  191,  191,
 /*   550 */   191,  191,  191,  103,  192,  192,  192,   87,  192,  192,
 /*   560 */    86,   50,   83,   85,   54,  192,  192,   84,  192,   82,
 /*   570 */    79,    5,  192,  192,  145,  197,  197,    5,    5,  145,
 /*   580 */     5,    5,   90,   89,  135,  113,  192,  192,  202,  107,
 /*   590 */   193,  208,  207,  204,  206,  203,  205,  201,  193,  193,
 /*   600 */   192,  192,  225,  193,  192,  198,  104,  114,  194,  105,
 /*   610 */   109,  105,  104,    1,  105,   76,  109,  104,  225,  104,
 /*   620 */   123,  105,  104,  109,  105,  104,  109,  104,  123,  104,
 /*   630 */   108,  111,  104,  107,    9,    5,    5,    5,    5,    5,
 /*   640 */    80,   15,  139,   76,  109,   16,    5,    5,  105,    5,
 /*   650 */     5,  139,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   660 */     5,    5,    5,  139,    5,    5,    5,    5,  109,   80,
 /*   670 */    60,    0,   59,  263,  263,  263,  263,  263,  263,   21,
 /*   680 */   263,  263,  263,  263,   21,  263,  263,  263,  263,  263,
 /*   690 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   700 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   710 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   720 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   730 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   740 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   750 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   760 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   770 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   780 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   790 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   800 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   810 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   820 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   830 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   840 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   850 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   860 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*   870 */   263,  263,
};
#define YY_SHIFT_COUNT    (316)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (671)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  180,  180,    9,  221,  230,  244,  244,
 /*    10 */   244,  244,  244,  244,  244,  244,  244,    0,   48,  230,
 /*    20 */   283,  283,  283,  283,  148,  161,  244,  244,  244,  303,
 /*    30 */   244,  244,   82,    9,   37,   37,  685,  685,  685,  230,
 /*    40 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    50 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  283,
 /*    60 */   283,   78,   78,   78,   78,   78,   78,   78,  244,  244,
 /*    70 */   244,  207,  244,  161,  161,  244,  244,  244,  254,  254,
 /*    80 */    26,  161,  244,  244,  244,  244,  244,  244,  244,  244,
 /*    90 */   244,  244,  244,  244,  244,  244,  244,  244,  244,  244,
 /*   100 */   244,  244,  244,  244,  244,  244,  244,  244,  244,  244,
 /*   110 */   244,  244,  244,  244,  244,  244,  244,  244,  244,  244,
 /*   120 */   244,  244,  244,  244,  244,  244,  244,  244,  244,  244,
 /*   130 */   244,  244,  244,  415,  415,  415,  365,  365,  365,  415,
 /*   140 */   365,  415,  369,  370,  371,  372,  375,  382,  384,  390,
 /*   150 */   404,  413,  415,  415,  415,  450,    9,    9,  415,  415,
 /*   160 */   470,  474,  511,  479,  478,  510,  483,  487,  450,  415,
 /*   170 */   491,  491,  415,  491,  415,  491,  415,  415,  685,  685,
 /*   180 */    27,  100,  127,  100,  100,   53,  182,  223,  223,  223,
 /*   190 */   223,  237,  249,  273,  318,  318,  318,  318,   77,  103,
 /*   200 */    92,   92,  269,  326,  256,  255,  306,  312,  282,  284,
 /*   210 */   291,  293,  294,  296,  299,  387,  309,  335,  158,  297,
 /*   220 */   302,  305,  307,  310,  313,  316,  289,  292,  295,  331,
 /*   230 */   300,  354,  435,  368,  566,  429,  572,  573,  434,  575,
 /*   240 */   576,  492,  494,  449,  472,  482,  502,  493,  504,  501,
 /*   250 */   506,  508,  509,  507,  513,  612,  515,  516,  518,  514,
 /*   260 */   497,  517,  505,  519,  521,  520,  523,  482,  525,  526,
 /*   270 */   528,  522,  539,  625,  630,  631,  632,  633,  634,  560,
 /*   280 */   626,  567,  503,  535,  535,  629,  512,  524,  535,  641,
 /*   290 */   642,  543,  535,  644,  645,  647,  648,  649,  650,  651,
 /*   300 */   652,  653,  654,  655,  656,  657,  659,  660,  661,  662,
 /*   310 */   559,  589,  658,  663,  610,  613,  671,
};
#define YY_REDUCE_COUNT (179)
#define YY_REDUCE_MIN   (-233)
#define YY_REDUCE_MAX   (414)
static const short yy_reduce_ofst[] = {
 /*     0 */  -178,  -27,  -27,   74,   74,   99, -230, -216, -173, -176,
 /*    10 */   -45,  -76,   -8,   34,  114,  133,  135, -185, -188, -233,
 /*    20 */  -206, -147,  -74,   23, -179, -127, -186,  123, -191, -112,
 /*    30 */   157,   49,    4,  156,  162,  184,  140,  141, -187, -217,
 /*    40 */  -194, -144,  -51,  -18,  165,  169,  173,  177,  189,  195,
 /*    50 */   196,  197,  198,  199,  200,  201,  202,  203,  204,  222,
 /*    60 */   224,  166,  205,  209,  232,  234,  235,  236,  218,  266,
 /*    70 */   274,  213,  275,  233,  238,  277,  278,  279,  212,  214,
 /*    80 */   239,  240,  285,  287,  288,  290,  298,  301,  304,  308,
 /*    90 */   311,  314,  315,  317,  319,  320,  321,  322,  323,  324,
 /*   100 */   325,  327,  328,  329,  330,  332,  333,  334,  336,  337,
 /*   110 */   338,  339,  340,  341,  342,  343,  344,  345,  346,  347,
 /*   120 */   348,  349,  350,  351,  352,  353,  355,  356,  357,  358,
 /*   130 */   359,  360,  361,  362,  363,  364,  225,  228,  229,  366,
 /*   140 */   231,  367,  242,  241,  245,  248,  243,  253,  265,  276,
 /*   150 */   286,  246,  373,  374,  376,  377,  378,  379,  380,  381,
 /*   160 */   383,  385,  388,  386,  391,  392,  389,  396,  393,  394,
 /*   170 */   397,  405,  395,  406,  408,  410,  409,  412,  407,  414,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   781,  894,  840,  906,  828,  837, 1037, 1037,  781,  781,
 /*    10 */   781,  781,  781,  781,  781,  781,  781,  953,  800, 1037,
 /*    20 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  837,
 /*    30 */   781,  781,  843,  837,  843,  843,  948,  878,  896,  781,
 /*    40 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*    50 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*    60 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*    70 */   781,  955,  958,  781,  781,  960,  781,  781,  980,  980,
 /*    80 */   946,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*    90 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*   100 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  826,
 /*   110 */   781,  824,  781,  781,  781,  781,  781,  781,  781,  781,
 /*   120 */   781,  781,  781,  781,  781,  781,  811,  781,  781,  781,
 /*   130 */   781,  781,  781,  802,  802,  802,  781,  781,  781,  802,
 /*   140 */   781,  802,  987,  991,  985,  973,  981,  972,  968,  966,
 /*   150 */   965,  995,  802,  802,  802,  841,  837,  837,  802,  802,
 /*   160 */   859,  857,  855,  847,  853,  849,  851,  845,  829,  802,
 /*   170 */   835,  835,  802,  835,  802,  835,  802,  802,  878,  896,
 /*   180 */   781,  996,  781, 1036,  986, 1026, 1025, 1032, 1024, 1023,
 /*   190 */  1022,  781,  781,  781, 1018, 1019, 1021, 1020,  781,  781,
 /*   200 */  1028, 1027,  781,  781,  781,  781,  781,  781,  781,  781,
 /*   210 */   781,  781,  781,  781,  781,  781,  998,  781,  992,  988,
 /*   220 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  908,
 /*   230 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*   240 */   781,  781,  781,  781,  945,  781,  781,  781,  781,  956,
 /*   250 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  982,
 /*   260 */   781,  974,  781,  781,  781,  781,  781,  920,  781,  781,
 /*   270 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*   280 */   781,  781,  781, 1048, 1046,  781,  781,  781, 1042,  781,
 /*   290 */   781,  781, 1040,  781,  781,  781,  781,  781,  781,  781,
 /*   300 */   781,  781,  781,  781,  781,  781,  781,  781,  781,  781,
 /*   310 */   862,  781,  809,  807,  781,  798,  781,
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
    1,  /*    IPTOKEN => ID */
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
    0,  /*      DNODE => nothing */
    0,  /*       USER => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
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
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*      COMMA => nothing */
    0,  /*         AS => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*    SESSION => nothing */
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
    1,  /*        NOW => ID */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*     SYNCDB => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
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
  /*   47 */ "MNODES",
  /*   48 */ "DNODES",
  /*   49 */ "ACCOUNTS",
  /*   50 */ "USERS",
  /*   51 */ "MODULES",
  /*   52 */ "QUERIES",
  /*   53 */ "CONNECTIONS",
  /*   54 */ "STREAMS",
  /*   55 */ "VARIABLES",
  /*   56 */ "SCORES",
  /*   57 */ "GRANTS",
  /*   58 */ "VNODES",
  /*   59 */ "IPTOKEN",
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
  /*   70 */ "DNODE",
  /*   71 */ "USER",
  /*   72 */ "ACCOUNT",
  /*   73 */ "USE",
  /*   74 */ "DESCRIBE",
  /*   75 */ "ALTER",
  /*   76 */ "PASS",
  /*   77 */ "PRIVILEGE",
  /*   78 */ "LOCAL",
  /*   79 */ "IF",
  /*   80 */ "EXISTS",
  /*   81 */ "PPS",
  /*   82 */ "TSERIES",
  /*   83 */ "DBS",
  /*   84 */ "STORAGE",
  /*   85 */ "QTIME",
  /*   86 */ "CONNS",
  /*   87 */ "STATE",
  /*   88 */ "KEEP",
  /*   89 */ "CACHE",
  /*   90 */ "REPLICA",
  /*   91 */ "QUORUM",
  /*   92 */ "DAYS",
  /*   93 */ "MINROWS",
  /*   94 */ "MAXROWS",
  /*   95 */ "BLOCKS",
  /*   96 */ "CTIME",
  /*   97 */ "WAL",
  /*   98 */ "FSYNC",
  /*   99 */ "COMP",
  /*  100 */ "PRECISION",
  /*  101 */ "UPDATE",
  /*  102 */ "CACHELAST",
  /*  103 */ "PARTITIONS",
  /*  104 */ "LP",
  /*  105 */ "RP",
  /*  106 */ "UNSIGNED",
  /*  107 */ "TAGS",
  /*  108 */ "USING",
  /*  109 */ "COMMA",
  /*  110 */ "AS",
  /*  111 */ "NULL",
  /*  112 */ "SELECT",
  /*  113 */ "UNION",
  /*  114 */ "ALL",
  /*  115 */ "DISTINCT",
  /*  116 */ "FROM",
  /*  117 */ "VARIABLE",
  /*  118 */ "INTERVAL",
  /*  119 */ "SESSION",
  /*  120 */ "FILL",
  /*  121 */ "SLIDING",
  /*  122 */ "ORDER",
  /*  123 */ "BY",
  /*  124 */ "ASC",
  /*  125 */ "DESC",
  /*  126 */ "GROUP",
  /*  127 */ "HAVING",
  /*  128 */ "LIMIT",
  /*  129 */ "OFFSET",
  /*  130 */ "SLIMIT",
  /*  131 */ "SOFFSET",
  /*  132 */ "WHERE",
  /*  133 */ "NOW",
  /*  134 */ "RESET",
  /*  135 */ "QUERY",
  /*  136 */ "SYNCDB",
  /*  137 */ "ADD",
  /*  138 */ "COLUMN",
  /*  139 */ "TAG",
  /*  140 */ "CHANGE",
  /*  141 */ "SET",
  /*  142 */ "KILL",
  /*  143 */ "CONNECTION",
  /*  144 */ "STREAM",
  /*  145 */ "COLON",
  /*  146 */ "ABORT",
  /*  147 */ "AFTER",
  /*  148 */ "ATTACH",
  /*  149 */ "BEFORE",
  /*  150 */ "BEGIN",
  /*  151 */ "CASCADE",
  /*  152 */ "CLUSTER",
  /*  153 */ "CONFLICT",
  /*  154 */ "COPY",
  /*  155 */ "DEFERRED",
  /*  156 */ "DELIMITERS",
  /*  157 */ "DETACH",
  /*  158 */ "EACH",
  /*  159 */ "END",
  /*  160 */ "EXPLAIN",
  /*  161 */ "FAIL",
  /*  162 */ "FOR",
  /*  163 */ "IGNORE",
  /*  164 */ "IMMEDIATE",
  /*  165 */ "INITIALLY",
  /*  166 */ "INSTEAD",
  /*  167 */ "MATCH",
  /*  168 */ "KEY",
  /*  169 */ "OF",
  /*  170 */ "RAISE",
  /*  171 */ "REPLACE",
  /*  172 */ "RESTRICT",
  /*  173 */ "ROW",
  /*  174 */ "STATEMENT",
  /*  175 */ "TRIGGER",
  /*  176 */ "VIEW",
  /*  177 */ "SEMI",
  /*  178 */ "NONE",
  /*  179 */ "PREV",
  /*  180 */ "LINEAR",
  /*  181 */ "IMPORT",
  /*  182 */ "TBNAME",
  /*  183 */ "JOIN",
  /*  184 */ "INSERT",
  /*  185 */ "INTO",
  /*  186 */ "VALUES",
  /*  187 */ "error",
  /*  188 */ "program",
  /*  189 */ "cmd",
  /*  190 */ "dbPrefix",
  /*  191 */ "ids",
  /*  192 */ "cpxName",
  /*  193 */ "ifexists",
  /*  194 */ "alter_db_optr",
  /*  195 */ "alter_topic_optr",
  /*  196 */ "acct_optr",
  /*  197 */ "ifnotexists",
  /*  198 */ "db_optr",
  /*  199 */ "topic_optr",
  /*  200 */ "pps",
  /*  201 */ "tseries",
  /*  202 */ "dbs",
  /*  203 */ "streams",
  /*  204 */ "storage",
  /*  205 */ "qtime",
  /*  206 */ "users",
  /*  207 */ "conns",
  /*  208 */ "state",
  /*  209 */ "keep",
  /*  210 */ "tagitemlist",
  /*  211 */ "cache",
  /*  212 */ "replica",
  /*  213 */ "quorum",
  /*  214 */ "days",
  /*  215 */ "minrows",
  /*  216 */ "maxrows",
  /*  217 */ "blocks",
  /*  218 */ "ctime",
  /*  219 */ "wal",
  /*  220 */ "fsync",
  /*  221 */ "comp",
  /*  222 */ "prec",
  /*  223 */ "update",
  /*  224 */ "cachelast",
  /*  225 */ "partitions",
  /*  226 */ "typename",
  /*  227 */ "signed",
  /*  228 */ "create_table_args",
  /*  229 */ "create_stable_args",
  /*  230 */ "create_table_list",
  /*  231 */ "create_from_stable",
  /*  232 */ "columnlist",
  /*  233 */ "tagNamelist",
  /*  234 */ "select",
  /*  235 */ "column",
  /*  236 */ "tagitem",
  /*  237 */ "selcollist",
  /*  238 */ "from",
  /*  239 */ "where_opt",
  /*  240 */ "interval_opt",
  /*  241 */ "session_option",
  /*  242 */ "fill_opt",
  /*  243 */ "sliding_opt",
  /*  244 */ "groupby_opt",
  /*  245 */ "orderby_opt",
  /*  246 */ "having_opt",
  /*  247 */ "slimit_opt",
  /*  248 */ "limit_opt",
  /*  249 */ "union",
  /*  250 */ "sclp",
  /*  251 */ "distinct",
  /*  252 */ "expr",
  /*  253 */ "as",
  /*  254 */ "tablelist",
  /*  255 */ "tmvar",
  /*  256 */ "sortlist",
  /*  257 */ "sortitem",
  /*  258 */ "item",
  /*  259 */ "sortorder",
  /*  260 */ "grouplist",
  /*  261 */ "exprlist",
  /*  262 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW MNODES",
 /*   4 */ "cmd ::= SHOW DNODES",
 /*   5 */ "cmd ::= SHOW ACCOUNTS",
 /*   6 */ "cmd ::= SHOW USERS",
 /*   7 */ "cmd ::= SHOW MODULES",
 /*   8 */ "cmd ::= SHOW QUERIES",
 /*   9 */ "cmd ::= SHOW CONNECTIONS",
 /*  10 */ "cmd ::= SHOW STREAMS",
 /*  11 */ "cmd ::= SHOW VARIABLES",
 /*  12 */ "cmd ::= SHOW SCORES",
 /*  13 */ "cmd ::= SHOW GRANTS",
 /*  14 */ "cmd ::= SHOW VNODES",
 /*  15 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  16 */ "dbPrefix ::=",
 /*  17 */ "dbPrefix ::= ids DOT",
 /*  18 */ "cpxName ::=",
 /*  19 */ "cpxName ::= DOT ids",
 /*  20 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  21 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
 /*  22 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  29 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  32 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  33 */ "cmd ::= DROP DNODE ids",
 /*  34 */ "cmd ::= DROP USER ids",
 /*  35 */ "cmd ::= DROP ACCOUNT ids",
 /*  36 */ "cmd ::= USE ids",
 /*  37 */ "cmd ::= DESCRIBE ids cpxName",
 /*  38 */ "cmd ::= ALTER USER ids PASS ids",
 /*  39 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids ids",
 /*  44 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  45 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "ids ::= ID",
 /*  49 */ "ids ::= STRING",
 /*  50 */ "ifexists ::= IF EXISTS",
 /*  51 */ "ifexists ::=",
 /*  52 */ "ifnotexists ::= IF NOT EXISTS",
 /*  53 */ "ifnotexists ::=",
 /*  54 */ "cmd ::= CREATE DNODE ids",
 /*  55 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  56 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  57 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  58 */ "cmd ::= CREATE USER ids PASS ids",
 /*  59 */ "pps ::=",
 /*  60 */ "pps ::= PPS INTEGER",
 /*  61 */ "tseries ::=",
 /*  62 */ "tseries ::= TSERIES INTEGER",
 /*  63 */ "dbs ::=",
 /*  64 */ "dbs ::= DBS INTEGER",
 /*  65 */ "streams ::=",
 /*  66 */ "streams ::= STREAMS INTEGER",
 /*  67 */ "storage ::=",
 /*  68 */ "storage ::= STORAGE INTEGER",
 /*  69 */ "qtime ::=",
 /*  70 */ "qtime ::= QTIME INTEGER",
 /*  71 */ "users ::=",
 /*  72 */ "users ::= USERS INTEGER",
 /*  73 */ "conns ::=",
 /*  74 */ "conns ::= CONNS INTEGER",
 /*  75 */ "state ::=",
 /*  76 */ "state ::= STATE ids",
 /*  77 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  78 */ "keep ::= KEEP tagitemlist",
 /*  79 */ "cache ::= CACHE INTEGER",
 /*  80 */ "replica ::= REPLICA INTEGER",
 /*  81 */ "quorum ::= QUORUM INTEGER",
 /*  82 */ "days ::= DAYS INTEGER",
 /*  83 */ "minrows ::= MINROWS INTEGER",
 /*  84 */ "maxrows ::= MAXROWS INTEGER",
 /*  85 */ "blocks ::= BLOCKS INTEGER",
 /*  86 */ "ctime ::= CTIME INTEGER",
 /*  87 */ "wal ::= WAL INTEGER",
 /*  88 */ "fsync ::= FSYNC INTEGER",
 /*  89 */ "comp ::= COMP INTEGER",
 /*  90 */ "prec ::= PRECISION STRING",
 /*  91 */ "update ::= UPDATE INTEGER",
 /*  92 */ "cachelast ::= CACHELAST INTEGER",
 /*  93 */ "partitions ::= PARTITIONS INTEGER",
 /*  94 */ "db_optr ::=",
 /*  95 */ "db_optr ::= db_optr cache",
 /*  96 */ "db_optr ::= db_optr replica",
 /*  97 */ "db_optr ::= db_optr quorum",
 /*  98 */ "db_optr ::= db_optr days",
 /*  99 */ "db_optr ::= db_optr minrows",
 /* 100 */ "db_optr ::= db_optr maxrows",
 /* 101 */ "db_optr ::= db_optr blocks",
 /* 102 */ "db_optr ::= db_optr ctime",
 /* 103 */ "db_optr ::= db_optr wal",
 /* 104 */ "db_optr ::= db_optr fsync",
 /* 105 */ "db_optr ::= db_optr comp",
 /* 106 */ "db_optr ::= db_optr prec",
 /* 107 */ "db_optr ::= db_optr keep",
 /* 108 */ "db_optr ::= db_optr update",
 /* 109 */ "db_optr ::= db_optr cachelast",
 /* 110 */ "topic_optr ::= db_optr",
 /* 111 */ "topic_optr ::= topic_optr partitions",
 /* 112 */ "alter_db_optr ::=",
 /* 113 */ "alter_db_optr ::= alter_db_optr replica",
 /* 114 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 115 */ "alter_db_optr ::= alter_db_optr keep",
 /* 116 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 117 */ "alter_db_optr ::= alter_db_optr comp",
 /* 118 */ "alter_db_optr ::= alter_db_optr wal",
 /* 119 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 120 */ "alter_db_optr ::= alter_db_optr update",
 /* 121 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 122 */ "alter_topic_optr ::= alter_db_optr",
 /* 123 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 124 */ "typename ::= ids",
 /* 125 */ "typename ::= ids LP signed RP",
 /* 126 */ "typename ::= ids UNSIGNED",
 /* 127 */ "signed ::= INTEGER",
 /* 128 */ "signed ::= PLUS INTEGER",
 /* 129 */ "signed ::= MINUS INTEGER",
 /* 130 */ "cmd ::= CREATE TABLE create_table_args",
 /* 131 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 133 */ "cmd ::= CREATE TABLE create_table_list",
 /* 134 */ "create_table_list ::= create_from_stable",
 /* 135 */ "create_table_list ::= create_table_list create_from_stable",
 /* 136 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 137 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 139 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 140 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 141 */ "tagNamelist ::= ids",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 143 */ "columnlist ::= columnlist COMMA column",
 /* 144 */ "columnlist ::= column",
 /* 145 */ "column ::= ids typename",
 /* 146 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 147 */ "tagitemlist ::= tagitem",
 /* 148 */ "tagitem ::= INTEGER",
 /* 149 */ "tagitem ::= FLOAT",
 /* 150 */ "tagitem ::= STRING",
 /* 151 */ "tagitem ::= BOOL",
 /* 152 */ "tagitem ::= NULL",
 /* 153 */ "tagitem ::= MINUS INTEGER",
 /* 154 */ "tagitem ::= MINUS FLOAT",
 /* 155 */ "tagitem ::= PLUS INTEGER",
 /* 156 */ "tagitem ::= PLUS FLOAT",
 /* 157 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 158 */ "select ::= LP select RP",
 /* 159 */ "union ::= select",
 /* 160 */ "union ::= union UNION ALL select",
 /* 161 */ "cmd ::= union",
 /* 162 */ "select ::= SELECT selcollist",
 /* 163 */ "sclp ::= selcollist COMMA",
 /* 164 */ "sclp ::=",
 /* 165 */ "selcollist ::= sclp distinct expr as",
 /* 166 */ "selcollist ::= sclp STAR",
 /* 167 */ "as ::= AS ids",
 /* 168 */ "as ::= ids",
 /* 169 */ "as ::=",
 /* 170 */ "distinct ::= DISTINCT",
 /* 171 */ "distinct ::=",
 /* 172 */ "from ::= FROM tablelist",
 /* 173 */ "from ::= FROM LP union RP",
 /* 174 */ "tablelist ::= ids cpxName",
 /* 175 */ "tablelist ::= ids cpxName ids",
 /* 176 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 177 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 178 */ "tmvar ::= VARIABLE",
 /* 179 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 180 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 181 */ "interval_opt ::=",
 /* 182 */ "session_option ::=",
 /* 183 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 184 */ "fill_opt ::=",
 /* 185 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 186 */ "fill_opt ::= FILL LP ID RP",
 /* 187 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 188 */ "sliding_opt ::=",
 /* 189 */ "orderby_opt ::=",
 /* 190 */ "orderby_opt ::= ORDER BY sortlist",
 /* 191 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 192 */ "sortlist ::= item sortorder",
 /* 193 */ "item ::= ids cpxName",
 /* 194 */ "sortorder ::= ASC",
 /* 195 */ "sortorder ::= DESC",
 /* 196 */ "sortorder ::=",
 /* 197 */ "groupby_opt ::=",
 /* 198 */ "groupby_opt ::= GROUP BY grouplist",
 /* 199 */ "grouplist ::= grouplist COMMA item",
 /* 200 */ "grouplist ::= item",
 /* 201 */ "having_opt ::=",
 /* 202 */ "having_opt ::= HAVING expr",
 /* 203 */ "limit_opt ::=",
 /* 204 */ "limit_opt ::= LIMIT signed",
 /* 205 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 206 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 207 */ "slimit_opt ::=",
 /* 208 */ "slimit_opt ::= SLIMIT signed",
 /* 209 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 210 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 211 */ "where_opt ::=",
 /* 212 */ "where_opt ::= WHERE expr",
 /* 213 */ "expr ::= LP expr RP",
 /* 214 */ "expr ::= ID",
 /* 215 */ "expr ::= ID DOT ID",
 /* 216 */ "expr ::= ID DOT STAR",
 /* 217 */ "expr ::= INTEGER",
 /* 218 */ "expr ::= MINUS INTEGER",
 /* 219 */ "expr ::= PLUS INTEGER",
 /* 220 */ "expr ::= FLOAT",
 /* 221 */ "expr ::= MINUS FLOAT",
 /* 222 */ "expr ::= PLUS FLOAT",
 /* 223 */ "expr ::= STRING",
 /* 224 */ "expr ::= NOW",
 /* 225 */ "expr ::= VARIABLE",
 /* 226 */ "expr ::= PLUS VARIABLE",
 /* 227 */ "expr ::= MINUS VARIABLE",
 /* 228 */ "expr ::= BOOL",
 /* 229 */ "expr ::= NULL",
 /* 230 */ "expr ::= ID LP exprlist RP",
 /* 231 */ "expr ::= ID LP STAR RP",
 /* 232 */ "expr ::= expr IS NULL",
 /* 233 */ "expr ::= expr IS NOT NULL",
 /* 234 */ "expr ::= expr LT expr",
 /* 235 */ "expr ::= expr GT expr",
 /* 236 */ "expr ::= expr LE expr",
 /* 237 */ "expr ::= expr GE expr",
 /* 238 */ "expr ::= expr NE expr",
 /* 239 */ "expr ::= expr EQ expr",
 /* 240 */ "expr ::= expr BETWEEN expr AND expr",
 /* 241 */ "expr ::= expr AND expr",
 /* 242 */ "expr ::= expr OR expr",
 /* 243 */ "expr ::= expr PLUS expr",
 /* 244 */ "expr ::= expr MINUS expr",
 /* 245 */ "expr ::= expr STAR expr",
 /* 246 */ "expr ::= expr SLASH expr",
 /* 247 */ "expr ::= expr REM expr",
 /* 248 */ "expr ::= expr LIKE expr",
 /* 249 */ "expr ::= expr IN LP exprlist RP",
 /* 250 */ "exprlist ::= exprlist COMMA expritem",
 /* 251 */ "exprlist ::= expritem",
 /* 252 */ "expritem ::= expr",
 /* 253 */ "expritem ::=",
 /* 254 */ "cmd ::= RESET QUERY CACHE",
 /* 255 */ "cmd ::= SYNCDB ids REPLICA",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 258 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 260 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 262 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 263 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 264 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 265 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 266 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 267 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 268 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 269 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 209: /* keep */
    case 210: /* tagitemlist */
    case 232: /* columnlist */
    case 233: /* tagNamelist */
    case 242: /* fill_opt */
    case 244: /* groupby_opt */
    case 245: /* orderby_opt */
    case 256: /* sortlist */
    case 260: /* grouplist */
{
taosArrayDestroy((yypminor->yy159));
}
      break;
    case 230: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy14));
}
      break;
    case 234: /* select */
{
destroySqlNode((yypminor->yy116));
}
      break;
    case 237: /* selcollist */
    case 250: /* sclp */
    case 261: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy159));
}
      break;
    case 238: /* from */
    case 254: /* tablelist */
{
destroyRelationInfo((yypminor->yy236));
}
      break;
    case 239: /* where_opt */
    case 246: /* having_opt */
    case 252: /* expr */
    case 262: /* expritem */
{
tSqlExprDestroy((yypminor->yy118));
}
      break;
    case 249: /* union */
{
destroyAllSqlNode((yypminor->yy159));
}
      break;
    case 257: /* sortitem */
{
tVariantDestroy(&(yypminor->yy488));
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
  {  188,   -1 }, /* (0) program ::= cmd */
  {  189,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  189,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  189,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  189,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  189,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  189,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  189,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  189,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  189,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  189,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  189,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  189,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  189,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  189,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  189,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  190,    0 }, /* (16) dbPrefix ::= */
  {  190,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  192,    0 }, /* (18) cpxName ::= */
  {  192,   -2 }, /* (19) cpxName ::= DOT ids */
  {  189,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  189,   -5 }, /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  189,   -4 }, /* (22) cmd ::= SHOW CREATE DATABASE ids */
  {  189,   -3 }, /* (23) cmd ::= SHOW dbPrefix TABLES */
  {  189,   -5 }, /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  189,   -3 }, /* (25) cmd ::= SHOW dbPrefix STABLES */
  {  189,   -5 }, /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  189,   -3 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS */
  {  189,   -4 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  189,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  189,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  189,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  189,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  189,   -3 }, /* (33) cmd ::= DROP DNODE ids */
  {  189,   -3 }, /* (34) cmd ::= DROP USER ids */
  {  189,   -3 }, /* (35) cmd ::= DROP ACCOUNT ids */
  {  189,   -2 }, /* (36) cmd ::= USE ids */
  {  189,   -3 }, /* (37) cmd ::= DESCRIBE ids cpxName */
  {  189,   -5 }, /* (38) cmd ::= ALTER USER ids PASS ids */
  {  189,   -5 }, /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  189,   -4 }, /* (40) cmd ::= ALTER DNODE ids ids */
  {  189,   -5 }, /* (41) cmd ::= ALTER DNODE ids ids ids */
  {  189,   -3 }, /* (42) cmd ::= ALTER LOCAL ids */
  {  189,   -4 }, /* (43) cmd ::= ALTER LOCAL ids ids */
  {  189,   -4 }, /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  189,   -4 }, /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  189,   -4 }, /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  189,   -6 }, /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  191,   -1 }, /* (48) ids ::= ID */
  {  191,   -1 }, /* (49) ids ::= STRING */
  {  193,   -2 }, /* (50) ifexists ::= IF EXISTS */
  {  193,    0 }, /* (51) ifexists ::= */
  {  197,   -3 }, /* (52) ifnotexists ::= IF NOT EXISTS */
  {  197,    0 }, /* (53) ifnotexists ::= */
  {  189,   -3 }, /* (54) cmd ::= CREATE DNODE ids */
  {  189,   -6 }, /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  189,   -5 }, /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  189,   -5 }, /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  189,   -5 }, /* (58) cmd ::= CREATE USER ids PASS ids */
  {  200,    0 }, /* (59) pps ::= */
  {  200,   -2 }, /* (60) pps ::= PPS INTEGER */
  {  201,    0 }, /* (61) tseries ::= */
  {  201,   -2 }, /* (62) tseries ::= TSERIES INTEGER */
  {  202,    0 }, /* (63) dbs ::= */
  {  202,   -2 }, /* (64) dbs ::= DBS INTEGER */
  {  203,    0 }, /* (65) streams ::= */
  {  203,   -2 }, /* (66) streams ::= STREAMS INTEGER */
  {  204,    0 }, /* (67) storage ::= */
  {  204,   -2 }, /* (68) storage ::= STORAGE INTEGER */
  {  205,    0 }, /* (69) qtime ::= */
  {  205,   -2 }, /* (70) qtime ::= QTIME INTEGER */
  {  206,    0 }, /* (71) users ::= */
  {  206,   -2 }, /* (72) users ::= USERS INTEGER */
  {  207,    0 }, /* (73) conns ::= */
  {  207,   -2 }, /* (74) conns ::= CONNS INTEGER */
  {  208,    0 }, /* (75) state ::= */
  {  208,   -2 }, /* (76) state ::= STATE ids */
  {  196,   -9 }, /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  209,   -2 }, /* (78) keep ::= KEEP tagitemlist */
  {  211,   -2 }, /* (79) cache ::= CACHE INTEGER */
  {  212,   -2 }, /* (80) replica ::= REPLICA INTEGER */
  {  213,   -2 }, /* (81) quorum ::= QUORUM INTEGER */
  {  214,   -2 }, /* (82) days ::= DAYS INTEGER */
  {  215,   -2 }, /* (83) minrows ::= MINROWS INTEGER */
  {  216,   -2 }, /* (84) maxrows ::= MAXROWS INTEGER */
  {  217,   -2 }, /* (85) blocks ::= BLOCKS INTEGER */
  {  218,   -2 }, /* (86) ctime ::= CTIME INTEGER */
  {  219,   -2 }, /* (87) wal ::= WAL INTEGER */
  {  220,   -2 }, /* (88) fsync ::= FSYNC INTEGER */
  {  221,   -2 }, /* (89) comp ::= COMP INTEGER */
  {  222,   -2 }, /* (90) prec ::= PRECISION STRING */
  {  223,   -2 }, /* (91) update ::= UPDATE INTEGER */
  {  224,   -2 }, /* (92) cachelast ::= CACHELAST INTEGER */
  {  225,   -2 }, /* (93) partitions ::= PARTITIONS INTEGER */
  {  198,    0 }, /* (94) db_optr ::= */
  {  198,   -2 }, /* (95) db_optr ::= db_optr cache */
  {  198,   -2 }, /* (96) db_optr ::= db_optr replica */
  {  198,   -2 }, /* (97) db_optr ::= db_optr quorum */
  {  198,   -2 }, /* (98) db_optr ::= db_optr days */
  {  198,   -2 }, /* (99) db_optr ::= db_optr minrows */
  {  198,   -2 }, /* (100) db_optr ::= db_optr maxrows */
  {  198,   -2 }, /* (101) db_optr ::= db_optr blocks */
  {  198,   -2 }, /* (102) db_optr ::= db_optr ctime */
  {  198,   -2 }, /* (103) db_optr ::= db_optr wal */
  {  198,   -2 }, /* (104) db_optr ::= db_optr fsync */
  {  198,   -2 }, /* (105) db_optr ::= db_optr comp */
  {  198,   -2 }, /* (106) db_optr ::= db_optr prec */
  {  198,   -2 }, /* (107) db_optr ::= db_optr keep */
  {  198,   -2 }, /* (108) db_optr ::= db_optr update */
  {  198,   -2 }, /* (109) db_optr ::= db_optr cachelast */
  {  199,   -1 }, /* (110) topic_optr ::= db_optr */
  {  199,   -2 }, /* (111) topic_optr ::= topic_optr partitions */
  {  194,    0 }, /* (112) alter_db_optr ::= */
  {  194,   -2 }, /* (113) alter_db_optr ::= alter_db_optr replica */
  {  194,   -2 }, /* (114) alter_db_optr ::= alter_db_optr quorum */
  {  194,   -2 }, /* (115) alter_db_optr ::= alter_db_optr keep */
  {  194,   -2 }, /* (116) alter_db_optr ::= alter_db_optr blocks */
  {  194,   -2 }, /* (117) alter_db_optr ::= alter_db_optr comp */
  {  194,   -2 }, /* (118) alter_db_optr ::= alter_db_optr wal */
  {  194,   -2 }, /* (119) alter_db_optr ::= alter_db_optr fsync */
  {  194,   -2 }, /* (120) alter_db_optr ::= alter_db_optr update */
  {  194,   -2 }, /* (121) alter_db_optr ::= alter_db_optr cachelast */
  {  195,   -1 }, /* (122) alter_topic_optr ::= alter_db_optr */
  {  195,   -2 }, /* (123) alter_topic_optr ::= alter_topic_optr partitions */
  {  226,   -1 }, /* (124) typename ::= ids */
  {  226,   -4 }, /* (125) typename ::= ids LP signed RP */
  {  226,   -2 }, /* (126) typename ::= ids UNSIGNED */
  {  227,   -1 }, /* (127) signed ::= INTEGER */
  {  227,   -2 }, /* (128) signed ::= PLUS INTEGER */
  {  227,   -2 }, /* (129) signed ::= MINUS INTEGER */
  {  189,   -3 }, /* (130) cmd ::= CREATE TABLE create_table_args */
  {  189,   -3 }, /* (131) cmd ::= CREATE TABLE create_stable_args */
  {  189,   -3 }, /* (132) cmd ::= CREATE STABLE create_stable_args */
  {  189,   -3 }, /* (133) cmd ::= CREATE TABLE create_table_list */
  {  230,   -1 }, /* (134) create_table_list ::= create_from_stable */
  {  230,   -2 }, /* (135) create_table_list ::= create_table_list create_from_stable */
  {  228,   -6 }, /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  229,  -10 }, /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  231,  -10 }, /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  231,  -13 }, /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  233,   -3 }, /* (140) tagNamelist ::= tagNamelist COMMA ids */
  {  233,   -1 }, /* (141) tagNamelist ::= ids */
  {  228,   -5 }, /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
  {  232,   -3 }, /* (143) columnlist ::= columnlist COMMA column */
  {  232,   -1 }, /* (144) columnlist ::= column */
  {  235,   -2 }, /* (145) column ::= ids typename */
  {  210,   -3 }, /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
  {  210,   -1 }, /* (147) tagitemlist ::= tagitem */
  {  236,   -1 }, /* (148) tagitem ::= INTEGER */
  {  236,   -1 }, /* (149) tagitem ::= FLOAT */
  {  236,   -1 }, /* (150) tagitem ::= STRING */
  {  236,   -1 }, /* (151) tagitem ::= BOOL */
  {  236,   -1 }, /* (152) tagitem ::= NULL */
  {  236,   -2 }, /* (153) tagitem ::= MINUS INTEGER */
  {  236,   -2 }, /* (154) tagitem ::= MINUS FLOAT */
  {  236,   -2 }, /* (155) tagitem ::= PLUS INTEGER */
  {  236,   -2 }, /* (156) tagitem ::= PLUS FLOAT */
  {  234,  -13 }, /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  234,   -3 }, /* (158) select ::= LP select RP */
  {  249,   -1 }, /* (159) union ::= select */
  {  249,   -4 }, /* (160) union ::= union UNION ALL select */
  {  189,   -1 }, /* (161) cmd ::= union */
  {  234,   -2 }, /* (162) select ::= SELECT selcollist */
  {  250,   -2 }, /* (163) sclp ::= selcollist COMMA */
  {  250,    0 }, /* (164) sclp ::= */
  {  237,   -4 }, /* (165) selcollist ::= sclp distinct expr as */
  {  237,   -2 }, /* (166) selcollist ::= sclp STAR */
  {  253,   -2 }, /* (167) as ::= AS ids */
  {  253,   -1 }, /* (168) as ::= ids */
  {  253,    0 }, /* (169) as ::= */
  {  251,   -1 }, /* (170) distinct ::= DISTINCT */
  {  251,    0 }, /* (171) distinct ::= */
  {  238,   -2 }, /* (172) from ::= FROM tablelist */
  {  238,   -4 }, /* (173) from ::= FROM LP union RP */
  {  254,   -2 }, /* (174) tablelist ::= ids cpxName */
  {  254,   -3 }, /* (175) tablelist ::= ids cpxName ids */
  {  254,   -4 }, /* (176) tablelist ::= tablelist COMMA ids cpxName */
  {  254,   -5 }, /* (177) tablelist ::= tablelist COMMA ids cpxName ids */
  {  255,   -1 }, /* (178) tmvar ::= VARIABLE */
  {  240,   -4 }, /* (179) interval_opt ::= INTERVAL LP tmvar RP */
  {  240,   -6 }, /* (180) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  240,    0 }, /* (181) interval_opt ::= */
  {  241,    0 }, /* (182) session_option ::= */
  {  241,   -7 }, /* (183) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  242,    0 }, /* (184) fill_opt ::= */
  {  242,   -6 }, /* (185) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  242,   -4 }, /* (186) fill_opt ::= FILL LP ID RP */
  {  243,   -4 }, /* (187) sliding_opt ::= SLIDING LP tmvar RP */
  {  243,    0 }, /* (188) sliding_opt ::= */
  {  245,    0 }, /* (189) orderby_opt ::= */
  {  245,   -3 }, /* (190) orderby_opt ::= ORDER BY sortlist */
  {  256,   -4 }, /* (191) sortlist ::= sortlist COMMA item sortorder */
  {  256,   -2 }, /* (192) sortlist ::= item sortorder */
  {  258,   -2 }, /* (193) item ::= ids cpxName */
  {  259,   -1 }, /* (194) sortorder ::= ASC */
  {  259,   -1 }, /* (195) sortorder ::= DESC */
  {  259,    0 }, /* (196) sortorder ::= */
  {  244,    0 }, /* (197) groupby_opt ::= */
  {  244,   -3 }, /* (198) groupby_opt ::= GROUP BY grouplist */
  {  260,   -3 }, /* (199) grouplist ::= grouplist COMMA item */
  {  260,   -1 }, /* (200) grouplist ::= item */
  {  246,    0 }, /* (201) having_opt ::= */
  {  246,   -2 }, /* (202) having_opt ::= HAVING expr */
  {  248,    0 }, /* (203) limit_opt ::= */
  {  248,   -2 }, /* (204) limit_opt ::= LIMIT signed */
  {  248,   -4 }, /* (205) limit_opt ::= LIMIT signed OFFSET signed */
  {  248,   -4 }, /* (206) limit_opt ::= LIMIT signed COMMA signed */
  {  247,    0 }, /* (207) slimit_opt ::= */
  {  247,   -2 }, /* (208) slimit_opt ::= SLIMIT signed */
  {  247,   -4 }, /* (209) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  247,   -4 }, /* (210) slimit_opt ::= SLIMIT signed COMMA signed */
  {  239,    0 }, /* (211) where_opt ::= */
  {  239,   -2 }, /* (212) where_opt ::= WHERE expr */
  {  252,   -3 }, /* (213) expr ::= LP expr RP */
  {  252,   -1 }, /* (214) expr ::= ID */
  {  252,   -3 }, /* (215) expr ::= ID DOT ID */
  {  252,   -3 }, /* (216) expr ::= ID DOT STAR */
  {  252,   -1 }, /* (217) expr ::= INTEGER */
  {  252,   -2 }, /* (218) expr ::= MINUS INTEGER */
  {  252,   -2 }, /* (219) expr ::= PLUS INTEGER */
  {  252,   -1 }, /* (220) expr ::= FLOAT */
  {  252,   -2 }, /* (221) expr ::= MINUS FLOAT */
  {  252,   -2 }, /* (222) expr ::= PLUS FLOAT */
  {  252,   -1 }, /* (223) expr ::= STRING */
  {  252,   -1 }, /* (224) expr ::= NOW */
  {  252,   -1 }, /* (225) expr ::= VARIABLE */
  {  252,   -2 }, /* (226) expr ::= PLUS VARIABLE */
  {  252,   -2 }, /* (227) expr ::= MINUS VARIABLE */
  {  252,   -1 }, /* (228) expr ::= BOOL */
  {  252,   -1 }, /* (229) expr ::= NULL */
  {  252,   -4 }, /* (230) expr ::= ID LP exprlist RP */
  {  252,   -4 }, /* (231) expr ::= ID LP STAR RP */
  {  252,   -3 }, /* (232) expr ::= expr IS NULL */
  {  252,   -4 }, /* (233) expr ::= expr IS NOT NULL */
  {  252,   -3 }, /* (234) expr ::= expr LT expr */
  {  252,   -3 }, /* (235) expr ::= expr GT expr */
  {  252,   -3 }, /* (236) expr ::= expr LE expr */
  {  252,   -3 }, /* (237) expr ::= expr GE expr */
  {  252,   -3 }, /* (238) expr ::= expr NE expr */
  {  252,   -3 }, /* (239) expr ::= expr EQ expr */
  {  252,   -5 }, /* (240) expr ::= expr BETWEEN expr AND expr */
  {  252,   -3 }, /* (241) expr ::= expr AND expr */
  {  252,   -3 }, /* (242) expr ::= expr OR expr */
  {  252,   -3 }, /* (243) expr ::= expr PLUS expr */
  {  252,   -3 }, /* (244) expr ::= expr MINUS expr */
  {  252,   -3 }, /* (245) expr ::= expr STAR expr */
  {  252,   -3 }, /* (246) expr ::= expr SLASH expr */
  {  252,   -3 }, /* (247) expr ::= expr REM expr */
  {  252,   -3 }, /* (248) expr ::= expr LIKE expr */
  {  252,   -5 }, /* (249) expr ::= expr IN LP exprlist RP */
  {  261,   -3 }, /* (250) exprlist ::= exprlist COMMA expritem */
  {  261,   -1 }, /* (251) exprlist ::= expritem */
  {  262,   -1 }, /* (252) expritem ::= expr */
  {  262,    0 }, /* (253) expritem ::= */
  {  189,   -3 }, /* (254) cmd ::= RESET QUERY CACHE */
  {  189,   -3 }, /* (255) cmd ::= SYNCDB ids REPLICA */
  {  189,   -7 }, /* (256) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  189,   -7 }, /* (257) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  189,   -7 }, /* (258) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  189,   -7 }, /* (259) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  189,   -8 }, /* (260) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  189,   -9 }, /* (261) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  189,   -7 }, /* (262) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  189,   -7 }, /* (263) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  189,   -7 }, /* (264) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  189,   -7 }, /* (265) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  189,   -8 }, /* (266) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  189,   -3 }, /* (267) cmd ::= KILL CONNECTION INTEGER */
  {  189,   -5 }, /* (268) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  189,   -5 }, /* (269) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 130: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==132);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 10: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 15: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 16: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 17: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 18: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 19: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 20: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
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
      case 33: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 34: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 35: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 36: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 38: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 39: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 45: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==45);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy322, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy351);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy351);}
        break;
      case 48: /* ids ::= ID */
      case 49: /* ids ::= STRING */ yytestcase(yyruleno==49);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 50: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 51: /* ifexists ::= */
      case 53: /* ifnotexists ::= */ yytestcase(yyruleno==53);
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 52: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy351);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy322, &yymsp[-2].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 59: /* pps ::= */
      case 61: /* tseries ::= */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= */ yytestcase(yyruleno==63);
      case 65: /* streams ::= */ yytestcase(yyruleno==65);
      case 67: /* storage ::= */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= */ yytestcase(yyruleno==69);
      case 71: /* users ::= */ yytestcase(yyruleno==71);
      case 73: /* conns ::= */ yytestcase(yyruleno==73);
      case 75: /* state ::= */ yytestcase(yyruleno==75);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 60: /* pps ::= PPS INTEGER */
      case 62: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==64);
      case 66: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==70);
      case 72: /* users ::= USERS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* state ::= STATE ids */ yytestcase(yyruleno==76);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 77: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy351.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy351.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy351.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy351.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy351.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy351.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy351.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy351.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy351.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy351 = yylhsminor.yy351;
        break;
      case 78: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy159 = yymsp[0].minor.yy159; }
        break;
      case 79: /* cache ::= CACHE INTEGER */
      case 80: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==80);
      case 81: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==81);
      case 82: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==86);
      case 87: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==87);
      case 88: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==88);
      case 89: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==89);
      case 90: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==90);
      case 91: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==91);
      case 92: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==92);
      case 93: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==93);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 94: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy322); yymsp[1].minor.yy322.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 95: /* db_optr ::= db_optr cache */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 96: /* db_optr ::= db_optr replica */
      case 113: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==113);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 97: /* db_optr ::= db_optr quorum */
      case 114: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==114);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 98: /* db_optr ::= db_optr days */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 99: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 100: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 101: /* db_optr ::= db_optr blocks */
      case 116: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==116);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 102: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 103: /* db_optr ::= db_optr wal */
      case 118: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==118);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 104: /* db_optr ::= db_optr fsync */
      case 119: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==119);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 105: /* db_optr ::= db_optr comp */
      case 117: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==117);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 106: /* db_optr ::= db_optr prec */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 107: /* db_optr ::= db_optr keep */
      case 115: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==115);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.keep = yymsp[0].minor.yy159; }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 108: /* db_optr ::= db_optr update */
      case 120: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==120);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 109: /* db_optr ::= db_optr cachelast */
      case 121: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==121);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 110: /* topic_optr ::= db_optr */
      case 122: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==122);
{ yylhsminor.yy322 = yymsp[0].minor.yy322; yylhsminor.yy322.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy322 = yylhsminor.yy322;
        break;
      case 111: /* topic_optr ::= topic_optr partitions */
      case 123: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==123);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 112: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy322); yymsp[1].minor.yy322.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 124: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy407, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy407 = yylhsminor.yy407;
        break;
      case 125: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy317 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy407, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy317;  // negative value of name length
    tSetColumnType(&yylhsminor.yy407, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy407 = yylhsminor.yy407;
        break;
      case 126: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy407, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy407 = yylhsminor.yy407;
        break;
      case 127: /* signed ::= INTEGER */
{ yylhsminor.yy317 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy317 = yylhsminor.yy317;
        break;
      case 128: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy317 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 129: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy317 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 133: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy14;}
        break;
      case 134: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy206);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy14 = pCreateTable;
}
  yymsp[0].minor.yy14 = yylhsminor.yy14;
        break;
      case 135: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy14->childTableInfo, &yymsp[0].minor.yy206);
  yylhsminor.yy14 = yymsp[-1].minor.yy14;
}
  yymsp[-1].minor.yy14 = yylhsminor.yy14;
        break;
      case 136: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy14 = tSetCreateTableInfo(yymsp[-1].minor.yy159, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy14, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy14 = yylhsminor.yy14;
        break;
      case 137: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy14 = tSetCreateTableInfo(yymsp[-5].minor.yy159, yymsp[-1].minor.yy159, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy14, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy14 = yylhsminor.yy14;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy206 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy159, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy206 = yylhsminor.yy206;
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy206 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy159, yymsp[-1].minor.yy159, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy206 = yylhsminor.yy206;
        break;
      case 140: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy159, &yymsp[0].minor.yy0); yylhsminor.yy159 = yymsp[-2].minor.yy159;  }
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 141: /* tagNamelist ::= ids */
{yylhsminor.yy159 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy159, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy14 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy116, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy14, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy14 = yylhsminor.yy14;
        break;
      case 143: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy159, &yymsp[0].minor.yy407); yylhsminor.yy159 = yymsp[-2].minor.yy159;  }
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 144: /* columnlist ::= column */
{yylhsminor.yy159 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy159, &yymsp[0].minor.yy407);}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 145: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy407, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy407);
}
  yymsp[-1].minor.yy407 = yylhsminor.yy407;
        break;
      case 146: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy159 = tVariantListAppend(yymsp[-2].minor.yy159, &yymsp[0].minor.yy488, -1);    }
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 147: /* tagitemlist ::= tagitem */
{ yylhsminor.yy159 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1); }
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 148: /* tagitem ::= INTEGER */
      case 149: /* tagitem ::= FLOAT */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= STRING */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= BOOL */ yytestcase(yyruleno==151);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 152: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 153: /* tagitem ::= MINUS INTEGER */
      case 154: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 157: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy116 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy159, yymsp[-10].minor.yy236, yymsp[-9].minor.yy118, yymsp[-4].minor.yy159, yymsp[-3].minor.yy159, &yymsp[-8].minor.yy184, &yymsp[-7].minor.yy249, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy159, &yymsp[0].minor.yy440, &yymsp[-1].minor.yy440, yymsp[-2].minor.yy118);
}
  yymsp[-12].minor.yy116 = yylhsminor.yy116;
        break;
      case 158: /* select ::= LP select RP */
{yymsp[-2].minor.yy116 = yymsp[-1].minor.yy116;}
        break;
      case 159: /* union ::= select */
{ yylhsminor.yy159 = setSubclause(NULL, yymsp[0].minor.yy116); }
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 160: /* union ::= union UNION ALL select */
{ yylhsminor.yy159 = appendSelectClause(yymsp[-3].minor.yy159, yymsp[0].minor.yy116); }
  yymsp[-3].minor.yy159 = yylhsminor.yy159;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy159, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy116 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy159, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy116 = yylhsminor.yy116;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy159 = yymsp[-1].minor.yy159;}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 164: /* sclp ::= */
      case 189: /* orderby_opt ::= */ yytestcase(yyruleno==189);
{yymsp[1].minor.yy159 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy159 = tSqlExprListAppend(yymsp[-3].minor.yy159, yymsp[-1].minor.yy118,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy159 = yylhsminor.yy159;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy159 = tSqlExprListAppend(yymsp[-1].minor.yy159, pNode, 0, 0);
}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 167: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 168: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 170: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy236 = yymsp[0].minor.yy236;}
        break;
      case 173: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy236 = setSubquery(NULL, yymsp[-1].minor.yy159);}
        break;
      case 174: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy236 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 175: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy236 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy236 = yylhsminor.yy236;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy236 = setTableNameList(yymsp[-3].minor.yy236, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy236 = yylhsminor.yy236;
        break;
      case 177: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy236 = setTableNameList(yymsp[-4].minor.yy236, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy236 = yylhsminor.yy236;
        break;
      case 178: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy184.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy184.offset.n = 0;}
        break;
      case 180: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy184.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy184.offset = yymsp[-1].minor.yy0;}
        break;
      case 181: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy184, 0, sizeof(yymsp[1].minor.yy184));}
        break;
      case 182: /* session_option ::= */
{yymsp[1].minor.yy249.col.n = 0; yymsp[1].minor.yy249.gap.n = 0;}
        break;
      case 183: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy249.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy249.gap = yymsp[-1].minor.yy0;
}
        break;
      case 184: /* fill_opt ::= */
{ yymsp[1].minor.yy159 = 0;     }
        break;
      case 185: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy159, &A, -1, 0);
    yymsp[-5].minor.yy159 = yymsp[-1].minor.yy159;
}
        break;
      case 186: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy159 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 187: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 188: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 190: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy159 = yymsp[0].minor.yy159;}
        break;
      case 191: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy159 = tVariantListAppend(yymsp[-3].minor.yy159, &yymsp[-1].minor.yy488, yymsp[0].minor.yy20);
}
  yymsp[-3].minor.yy159 = yylhsminor.yy159;
        break;
      case 192: /* sortlist ::= item sortorder */
{
  yylhsminor.yy159 = tVariantListAppend(NULL, &yymsp[-1].minor.yy488, yymsp[0].minor.yy20);
}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 193: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 194: /* sortorder ::= ASC */
{ yymsp[0].minor.yy20 = TSDB_ORDER_ASC; }
        break;
      case 195: /* sortorder ::= DESC */
{ yymsp[0].minor.yy20 = TSDB_ORDER_DESC;}
        break;
      case 196: /* sortorder ::= */
{ yymsp[1].minor.yy20 = TSDB_ORDER_ASC; }
        break;
      case 197: /* groupby_opt ::= */
{ yymsp[1].minor.yy159 = 0;}
        break;
      case 198: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy159 = yymsp[0].minor.yy159;}
        break;
      case 199: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy159 = tVariantListAppend(yymsp[-2].minor.yy159, &yymsp[0].minor.yy488, -1);
}
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 200: /* grouplist ::= item */
{
  yylhsminor.yy159 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1);
}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 201: /* having_opt ::= */
      case 211: /* where_opt ::= */ yytestcase(yyruleno==211);
      case 253: /* expritem ::= */ yytestcase(yyruleno==253);
{yymsp[1].minor.yy118 = 0;}
        break;
      case 202: /* having_opt ::= HAVING expr */
      case 212: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==212);
{yymsp[-1].minor.yy118 = yymsp[0].minor.yy118;}
        break;
      case 203: /* limit_opt ::= */
      case 207: /* slimit_opt ::= */ yytestcase(yyruleno==207);
{yymsp[1].minor.yy440.limit = -1; yymsp[1].minor.yy440.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed */
      case 208: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==208);
{yymsp[-1].minor.yy440.limit = yymsp[0].minor.yy317;  yymsp[-1].minor.yy440.offset = 0;}
        break;
      case 205: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy440.limit = yymsp[-2].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[0].minor.yy317;}
        break;
      case 206: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy440.limit = yymsp[0].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[-2].minor.yy317;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy440.limit = yymsp[-2].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[0].minor.yy317;}
        break;
      case 210: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy440.limit = yymsp[0].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[-2].minor.yy317;}
        break;
      case 213: /* expr ::= LP expr RP */
{yylhsminor.yy118 = yymsp[-1].minor.yy118; yylhsminor.yy118->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy118->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 214: /* expr ::= ID */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 215: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 216: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 217: /* expr ::= INTEGER */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 218: /* expr ::= MINUS INTEGER */
      case 219: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==219);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy118 = yylhsminor.yy118;
        break;
      case 220: /* expr ::= FLOAT */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 221: /* expr ::= MINUS FLOAT */
      case 222: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy118 = yylhsminor.yy118;
        break;
      case 223: /* expr ::= STRING */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 224: /* expr ::= NOW */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 225: /* expr ::= VARIABLE */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 226: /* expr ::= PLUS VARIABLE */
      case 227: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==227);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy118 = yylhsminor.yy118;
        break;
      case 228: /* expr ::= BOOL */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 229: /* expr ::= NULL */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 230: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy118 = tSqlExprCreateFunction(yymsp[-1].minor.yy159, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy118 = yylhsminor.yy118;
        break;
      case 231: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy118 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy118 = yylhsminor.yy118;
        break;
      case 232: /* expr ::= expr IS NULL */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 233: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-3].minor.yy118, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy118 = yylhsminor.yy118;
        break;
      case 234: /* expr ::= expr LT expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_LT);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 235: /* expr ::= expr GT expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_GT);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 236: /* expr ::= expr LE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_LE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 237: /* expr ::= expr GE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_GE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 238: /* expr ::= expr NE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_NE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 239: /* expr ::= expr EQ expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_EQ);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 240: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy118); yylhsminor.yy118 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy118, yymsp[-2].minor.yy118, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy118, TK_LE), TK_AND);}
  yymsp[-4].minor.yy118 = yylhsminor.yy118;
        break;
      case 241: /* expr ::= expr AND expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_AND);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 242: /* expr ::= expr OR expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_OR); }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 243: /* expr ::= expr PLUS expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_PLUS);  }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 244: /* expr ::= expr MINUS expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_MINUS); }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 245: /* expr ::= expr STAR expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_STAR);  }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 246: /* expr ::= expr SLASH expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_DIVIDE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 247: /* expr ::= expr REM expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_REM);   }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 248: /* expr ::= expr LIKE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_LIKE);  }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 249: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-4].minor.yy118, (tSqlExpr*)yymsp[-1].minor.yy159, TK_IN); }
  yymsp[-4].minor.yy118 = yylhsminor.yy118;
        break;
      case 250: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy159 = tSqlExprListAppend(yymsp[-2].minor.yy159,yymsp[0].minor.yy118,0, 0);}
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 251: /* exprlist ::= expritem */
{yylhsminor.yy159 = tSqlExprListAppend(0,yymsp[0].minor.yy118,0, 0);}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 252: /* expritem ::= expr */
{yylhsminor.yy118 = yymsp[0].minor.yy118;}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 254: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 255: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 268: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 269: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
