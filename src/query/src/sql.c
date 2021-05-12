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
#define YYNOCODE 270
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSessionWindowVal yy15;
  SIntervalVal yy42;
  tSqlExpr* yy68;
  SCreateAcctInfo yy77;
  SArray* yy93;
  int yy150;
  SSqlNode* yy224;
  int64_t yy279;
  SLimitVal yy284;
  TAOS_FIELD yy325;
  SRelationInfo* yy330;
  SCreateDbInfo yy372;
  tVariant yy518;
  SCreatedTableInfo yy528;
  SCreateTableSql* yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             332
#define YYNRULE              276
#define YYNTOKEN             192
#define YY_MAX_SHIFT         331
#define YY_MIN_SHIFTREDUCE   531
#define YY_MAX_SHIFTREDUCE   806
#define YY_ERROR_ACTION      807
#define YY_ACCEPT_ACTION     808
#define YY_NO_ACTION         809
#define YY_MIN_REDUCE        810
#define YY_MAX_REDUCE        1085
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
#define YY_ACTTAB_COUNT (707)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   984,  581,  192,  581,  215,  329,  238,   17,   84,  582,
 /*    10 */   581,  582, 1066,   49,   50,  162,   53,   54,  582,  190,
 /*    20 */   226,   43,  192,   52,  273,   57,   55,   59,   56,   32,
 /*    30 */   141,  223, 1067,   48,   47,  808,  331,   46,   45,   44,
 /*    40 */   946,  947,   29,  950,  756,  957,  759,  532,  533,  534,
 /*    50 */   535,  536,  537,  538,  539,  540,  541,  542,  543,  544,
 /*    60 */   545,  330,  220,  981,  216,   49,   50,  148,   53,   54,
 /*    70 */   148,  217,  226,   43,  960,   52,  273,   57,   55,   59,
 /*    80 */    56,  196,  975,   72,  295,   48,   47,  963,  232,   46,
 /*    90 */    45,   44,   49,   50,  258,   53,   54,  253,  315,  226,
 /*   100 */    43,  192,   52,  273,   57,   55,   59,   56,   71,   32,
 /*   110 */   222, 1067,   48,   47,   76,  238,   46,   45,   44,   49,
 /*   120 */    51,   38,   53,   54,  163, 1063,  226,   43,  274,   52,
 /*   130 */   273,   57,   55,   59,   56, 1016,  270,  268,   80,   48,
 /*   140 */    47,  948,  581,   46,   45,   44,   50,  975,   53,   54,
 /*   150 */   582,  229,  226,   43,  960,   52,  273,   57,   55,   59,
 /*   160 */    56,  763,  218,  750,   32,   48,   47,    1,  164,   46,
 /*   170 */    45,   44,   23,  293,  324,  323,  292,  291,  290,  322,
 /*   180 */   289,  321,  320,  319,  288,  318,  317,  923,  664,  911,
 /*   190 */   912,  913,  914,  915,  916,  917,  918,  919,  920,  921,
 /*   200 */   922,  924,  925,   53,   54,   18,  230,  226,   43,  960,
 /*   210 */    52,  273,   57,   55,   59,   56,  305,  304,    3,  177,
 /*   220 */    48,   47,   32,  200,   46,   45,   44,  225,  765,  769,
 /*   230 */   202,  754,  949,  757,  245,  760,  124,  123,  201,  148,
 /*   240 */   225,  765,  249,  248,  754,  231,  757,   32,  760,   32,
 /*   250 */   755,   25,  758, 1062,  931,   83,   32,  929,  930,  211,
 /*   260 */   212,  237,  932,  272,  934,  935,  933,  959,  936,  937,
 /*   270 */   963,   76,  211,  212,   23,  254,  324,  323,   38,   33,
 /*   280 */   235,  322,   12,  321,  320,  319,   86,  318,  317,  298,
 /*   290 */   688,  299,  960,  685,  960,  686, 1077,  687,  303,   32,
 /*   300 */   252,  960,   70,   57,   55,   59,   56, 1015,  208,  962,
 /*   310 */   233,   48,   47,  295,  325,   46,   45,   44,    5,   35,
 /*   320 */   166,  240,  241,  703,   66,  165,   93,   98,   89,   97,
 /*   330 */    46,   45,   44,  148,  239,  963,   58,  302,  301,  963,
 /*   340 */   284,  307,  766,   67,  960,  238,   48,   47,  762,   58,
 /*   350 */    46,   45,   44,  236,  961,  766,  297,  328,  327,  133,
 /*   360 */    87,  762,   82, 1061,  761,  731,  732,  764,  209,  109,
 /*   370 */   103,  114,  139,  137,  136,   73,  113,  761,  119,  122,
 /*   380 */   112,  184,  182,  180,  752,  111,  116,  858,  179,  128,
 /*   390 */   127,  126,  125,  176,  315,  951,  867,  859,  224,  700,
 /*   400 */   710,   81,  176,  176,  689,   24,  707,  256,  210,  716,
 /*   410 */   722,  723,  786,  143,   62,   20,   19,   63,  194,  767,
 /*   420 */   753,  195,  674,   19,  197,  276,   33,  676,  278,   33,
 /*   430 */   675,   62,   85,   28,   62,   69,  279,  663,   64,  102,
 /*   440 */   101,  692,  690,  693,  691,  121,  120,    6,   14,   13,
 /*   450 */  1026,  191,  198, 1025,  227,  108,  107,  199,   16,   15,
 /*   460 */   205,  206,  204,  189,  203,  193, 1022, 1021,  228,  250,
 /*   470 */   306,  140,  983,   41,  158,  991,  993,  142,  976,  146,
 /*   480 */   257,  958,  138, 1008, 1007,  259,  159,  285,  956,  219,
 /*   490 */   160,  161,  261,  872,  155,  153,  149,  715,  973,  281,
 /*   500 */   282,  150,  151,  266,  152,  154,  283,  156,  271,  263,
 /*   510 */   267,   68,   60,  269,   65,  265,  286,  287,   39,  260,
 /*   520 */   187,  316,   36,  296,  866, 1082,   99, 1081, 1079,  167,
 /*   530 */   300, 1076,  105,  110, 1075, 1073,  168,  892,   37,   34,
 /*   540 */    40,  188,  855,  115,  853,  117,  118,  851,  850,  242,
 /*   550 */   178,  848,  847,  846,  845,  844,  843,  842,  181,  183,
 /*   560 */   839,  837,  835,   42,  833,  185,  830,  186,  308,  255,
 /*   570 */    74,   77,  309,  262, 1009,  310,  311,  312,  313,  314,
 /*   580 */   326,  806,  243,  244,  213,  234,  280,  805,  246,  247,
 /*   590 */   804,  792,  214,  791,  251,  207,   94,  871,  870,   95,
 /*   600 */   256,  695,  275,    8,  849,  221,   75,  129,  130,   78,
 /*   610 */   841,  840,  171,  170,  893,  174,  169,  172,  173,  175,
 /*   620 */   131,  132,  832,    2,  831,  927,  717,    4,  144,  145,
 /*   630 */   720,  157,   26,   79,  264,    9,  724,  147,  939,   10,
 /*   640 */   768,   27,    7,   11,  770,   21,   22,  277,   88,   30,
 /*   650 */    90,   86,   91,  595,  627,   31,   92,  623,  621,  620,
 /*   660 */   619,  616,  585,   33,  294,   96,   61,  666,  665,  662,
 /*   670 */   611,  609,  601,  607,  100,  104,  603,  605,  599,  597,
 /*   680 */   630,  629,  106,  628,  626,  625,  624,  622,  618,  617,
 /*   690 */    62,  583,  549,  547,  810,  809,  809,  809,  134,  809,
 /*   700 */   809,  809,  809,  809,  809,  809,  135,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   196,    1,  258,    1,  195,  196,  196,  258,  202,    9,
 /*    10 */     1,    9,  268,   13,   14,  205,   16,   17,    9,  258,
 /*    20 */    20,   21,  258,   23,   24,   25,   26,   27,   28,  196,
 /*    30 */   196,  267,  268,   33,   34,  193,  194,   37,   38,   39,
 /*    40 */   234,  235,  236,  237,    5,  196,    7,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  217,  259,   62,   13,   14,  196,   16,   17,
 /*    70 */   196,  238,   20,   21,  241,   23,   24,   25,   26,   27,
 /*    80 */    28,  258,  240,   83,   81,   33,   34,  242,  239,   37,
 /*    90 */    38,   39,   13,   14,  260,   16,   17,  255,   87,   20,
 /*   100 */    21,  258,   23,   24,   25,   26,   27,   28,  202,  196,
 /*   110 */   267,  268,   33,   34,  110,  196,   37,   38,   39,   13,
 /*   120 */    14,  117,   16,   17,  205,  258,   20,   21,   15,   23,
 /*   130 */    24,   25,   26,   27,   28,  264,  262,  266,  264,   33,
 /*   140 */    34,  235,    1,   37,   38,   39,   14,  240,   16,   17,
 /*   150 */     9,  238,   20,   21,  241,   23,   24,   25,   26,   27,
 /*   160 */    28,  122,  255,  111,  196,   33,   34,  203,  204,   37,
 /*   170 */    38,   39,   94,   95,   96,   97,   98,   99,  100,  101,
 /*   180 */   102,  103,  104,  105,  106,  107,  108,  216,    5,  218,
 /*   190 */   219,  220,  221,  222,  223,  224,  225,  226,  227,  228,
 /*   200 */   229,  230,  231,   16,   17,   44,  238,   20,   21,  241,
 /*   210 */    23,   24,   25,   26,   27,   28,   33,   34,  199,  200,
 /*   220 */    33,   34,  196,   62,   37,   38,   39,    1,    2,  116,
 /*   230 */    69,    5,    0,    7,  140,    9,   75,   76,   77,  196,
 /*   240 */     1,    2,  148,  149,    5,  217,    7,  196,    9,  196,
 /*   250 */     5,  110,    7,  258,  216,   83,  196,  219,  220,   33,
 /*   260 */    34,   69,  224,   37,  226,  227,  228,  241,  230,  231,
 /*   270 */   242,  110,   33,   34,   94,  111,   96,   97,  117,  115,
 /*   280 */    69,  101,  110,  103,  104,  105,  114,  107,  108,  238,
 /*   290 */     2,  238,  241,    5,  241,    7,  242,    9,  238,  196,
 /*   300 */   139,  241,  141,   25,   26,   27,   28,  264,  147,  242,
 /*   310 */   217,   33,   34,   81,  217,   37,   38,   39,   63,   64,
 /*   320 */    65,   33,   34,   37,  115,   70,   71,   72,   73,   74,
 /*   330 */    37,   38,   39,  196,  142,  242,  110,  145,  146,  242,
 /*   340 */    85,  238,  116,  134,  241,  196,   33,   34,  122,  110,
 /*   350 */    37,   38,   39,  142,  205,  116,  145,   66,   67,   68,
 /*   360 */   202,  122,  243,  258,  138,  129,  130,  122,  258,   63,
 /*   370 */    64,   65,   63,   64,   65,  256,   70,  138,   72,   73,
 /*   380 */    74,   63,   64,   65,    1,   78,   80,  201,   70,   71,
 /*   390 */    72,   73,   74,  207,   87,  237,  201,  201,   61,  115,
 /*   400 */   111,  264,  207,  207,  116,  121,  120,  118,  258,  111,
 /*   410 */   111,  111,  111,  115,  115,  115,  115,  115,  258,  111,
 /*   420 */    37,  258,  111,  115,  258,  111,  115,  111,  111,  115,
 /*   430 */   111,  115,  115,  110,  115,  110,  113,  112,  136,  143,
 /*   440 */   144,    5,    5,    7,    7,   78,   79,  110,  143,  144,
 /*   450 */   233,  258,  258,  233,  233,  143,  144,  258,  143,  144,
 /*   460 */   258,  258,  258,  258,  258,  258,  233,  233,  233,  196,
 /*   470 */   233,  196,  196,  257,  244,  196,  196,  196,  240,  196,
 /*   480 */   240,  240,   61,  265,  265,  261,  196,   86,  196,  261,
 /*   490 */   196,  196,  261,  196,  247,  249,  253,  122,  254,  196,
 /*   500 */   196,  252,  251,  261,  250,  248,  196,  246,  127,  124,
 /*   510 */   126,  133,  132,  131,  135,  125,  196,  196,  196,  123,
 /*   520 */   196,  109,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   530 */   196,  196,  196,   93,  196,  196,  196,  196,  196,  196,
 /*   540 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   550 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   560 */   196,  196,  196,  137,  196,  196,  196,  196,   92,  197,
 /*   570 */   197,  197,   51,  197,  197,   89,   91,   55,   90,   88,
 /*   580 */    81,    5,  150,    5,  197,  197,  197,    5,  150,    5,
 /*   590 */     5,   96,  197,   95,  140,  197,  202,  206,  206,  202,
 /*   600 */   118,  111,  113,  110,  197,    1,  119,  198,  198,  115,
 /*   610 */   197,  197,  209,  213,  215,  211,  214,  212,  210,  208,
 /*   620 */   198,  198,  197,  203,  197,  232,  111,  199,  110,  115,
 /*   630 */   111,  245,  115,  110,  110,  128,  111,  110,  232,  128,
 /*   640 */   111,  115,  110,  110,  116,  110,  110,  113,   78,   84,
 /*   650 */    83,  114,   71,    5,    9,   84,   83,    5,    5,    5,
 /*   660 */     5,    5,   82,  115,   15,   78,   16,    5,    5,  111,
 /*   670 */     5,    5,    5,    5,  144,  144,    5,    5,    5,    5,
 /*   680 */     5,    5,  144,    5,    5,    5,    5,    5,    5,    5,
 /*   690 */   115,   82,   61,   60,    0,  269,  269,  269,   21,  269,
 /*   700 */   269,  269,  269,  269,  269,  269,   21,  269,  269,  269,
 /*   710 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   720 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   730 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   740 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   750 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   760 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   770 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   780 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   790 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   800 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   810 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   820 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   830 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   840 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   850 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   860 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   870 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   880 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   890 */   269,  269,  269,  269,  269,  269,  269,  269,  269,
};
#define YY_SHIFT_COUNT    (331)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (694)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   78,   78,  180,  180,    3,  226,  239,    9,    9,
 /*    10 */     9,    9,    9,    9,    9,    9,    9,    0,    2,  239,
 /*    20 */   288,  288,  288,  288,  141,    4,    9,    9,    9,  232,
 /*    30 */     9,    9,    9,    9,  307,    3,   11,   11,  707,  707,
 /*    40 */   707,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  288,  288,  183,  183,  183,  183,  183,  183,  183,
 /*    70 */     9,    9,    9,  286,    9,    4,    4,    9,    9,    9,
 /*    80 */   236,  236,  284,    4,    9,    9,    9,    9,    9,    9,
 /*    90 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   100 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   110 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   120 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   130 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   140 */   421,  421,  421,  375,  375,  375,  421,  375,  421,  378,
 /*   150 */   379,  380,  381,  382,  384,  390,  385,  396,  426,  421,
 /*   160 */   421,  421,  401,  401,  412,    3,    3,  421,  421,  440,
 /*   170 */   476,  521,  486,  485,  522,  488,  491,  412,  421,  499,
 /*   180 */   499,  421,  499,  421,  499,  421,  421,  707,  707,   52,
 /*   190 */    79,  106,   79,   79,  132,  187,  278,  278,  278,  278,
 /*   200 */   255,  306,  318,  313,  313,  313,  313,  192,   94,  293,
 /*   210 */   293,   39,  245,  172,  211,  291,  309,  164,  289,  298,
 /*   220 */   299,  300,  301,  308,  383,  337,  113,  302,  209,  311,
 /*   230 */   314,  316,  317,  319,  323,  296,  305,  312,  325,  315,
 /*   240 */   436,  437,  367,  576,  432,  578,  582,  438,  584,  585,
 /*   250 */   495,  498,  454,  482,  489,  493,  487,  490,  494,  515,
 /*   260 */   518,  519,  514,  523,  604,  524,  525,  527,  517,  507,
 /*   270 */   526,  511,  529,  532,  528,  533,  489,  535,  534,  536,
 /*   280 */   537,  570,  565,  567,  581,  648,  571,  573,  645,  652,
 /*   290 */   653,  654,  655,  656,  580,  649,  587,  530,  548,  548,
 /*   300 */   650,  531,  538,  548,  662,  663,  558,  548,  665,  666,
 /*   310 */   667,  668,  671,  672,  673,  674,  675,  676,  678,  679,
 /*   320 */   680,  681,  682,  683,  684,  575,  609,  677,  685,  631,
 /*   330 */   633,  694,
};
#define YY_REDUCE_COUNT (188)
#define YY_REDUCE_MIN   (-256)
#define YY_REDUCE_MAX   (428)
static const short yy_reduce_ofst[] = {
 /*     0 */  -158,  -29,  -29,   38,   38, -194, -236, -157, -167, -129,
 /*    10 */  -126,  -87,  -32,   51,   53,   60,  103, -196, -191, -256,
 /*    20 */  -155,   28,   93,   97, -166,  -93,   43,  137, -151,  158,
 /*    30 */  -190,  -81,  149,   26,  186,  -94,  195,  196,  119,  -36,
 /*    40 */    19, -251, -239, -177, -133,   -5,  105,  110,  150,  160,
 /*    50 */   163,  166,  193,  194,  199,  202,  203,  204,  205,  206,
 /*    60 */   207,   54,   67,  217,  220,  221,  233,  234,  235,  237,
 /*    70 */   273,  275,  276,  216,  279,  238,  240,  280,  281,  283,
 /*    80 */   218,  219,  230,  241,  290,  292,  294,  295,  297,  303,
 /*    90 */   304,  310,  320,  321,  322,  324,  326,  327,  328,  329,
 /*   100 */   330,  331,  332,  333,  334,  335,  336,  338,  339,  340,
 /*   110 */   341,  342,  343,  344,  345,  346,  347,  348,  349,  350,
 /*   120 */   351,  352,  353,  354,  355,  356,  357,  358,  359,  360,
 /*   130 */   361,  362,  363,  364,  365,  366,  368,  369,  370,  371,
 /*   140 */   372,  373,  374,  224,  228,  231,  376,  242,  377,  244,
 /*   150 */   243,  249,  251,  254,  246,  257,  247,  261,  386,  387,
 /*   160 */   388,  389,  391,  392,  393,  394,  397,  395,  398,  399,
 /*   170 */   402,  400,  403,  405,  408,  404,  411,  406,  407,  409,
 /*   180 */   410,  413,  422,  414,  423,  425,  427,  420,  428,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   807,  926,  868,  938,  856,  865, 1069, 1069,  807,  807,
 /*    10 */   807,  807,  807,  807,  807,  807,  807,  985,  827, 1069,
 /*    20 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  865,
 /*    30 */   807,  807,  807,  807,  875,  865,  875,  875,  980,  910,
 /*    40 */   928,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*    50 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*    60 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*    70 */   807,  807,  807,  987,  990,  807,  807,  992,  807,  807,
 /*    80 */  1012, 1012,  978,  807,  807,  807,  807,  807,  807,  807,
 /*    90 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   100 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   110 */   807,  807,  807,  807,  807,  854,  807,  852,  807,  807,
 /*   120 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   130 */   807,  807,  807,  838,  807,  807,  807,  807,  807,  807,
 /*   140 */   829,  829,  829,  807,  807,  807,  829,  807,  829, 1019,
 /*   150 */  1023, 1017, 1005, 1013, 1004, 1000,  998,  997, 1027,  829,
 /*   160 */   829,  829,  873,  873,  869,  865,  865,  829,  829,  891,
 /*   170 */   889,  887,  879,  885,  881,  883,  877,  857,  829,  863,
 /*   180 */   863,  829,  863,  829,  863,  829,  829,  910,  928,  807,
 /*   190 */  1028,  807, 1068, 1018, 1058, 1057, 1064, 1056, 1055, 1054,
 /*   200 */   807,  807,  807, 1050, 1051, 1053, 1052,  807,  807, 1060,
 /*   210 */  1059,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   220 */   807,  807,  807,  807,  807, 1030,  807, 1024, 1020,  807,
 /*   230 */   807,  807,  807,  807,  807,  807,  807,  807,  940,  807,
 /*   240 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   250 */   807,  807,  807,  977,  807,  807,  807,  807,  988,  807,
 /*   260 */   807,  807,  807,  807,  807,  807,  807,  807, 1014,  807,
 /*   270 */  1006,  807,  807,  807,  807,  807,  952,  807,  807,  807,
 /*   280 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   290 */   807,  807,  807,  807,  807,  807,  807,  807, 1080, 1078,
 /*   300 */   807,  807,  807, 1074,  807,  807,  807, 1072,  807,  807,
 /*   310 */   807,  807,  807,  807,  807,  807,  807,  807,  807,  807,
 /*   320 */   807,  807,  807,  807,  807,  894,  807,  836,  834,  807,
 /*   330 */   825,  807,
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
  /*   60 */ "IPTOKEN",
  /*   61 */ "DOT",
  /*   62 */ "CREATE",
  /*   63 */ "TABLE",
  /*   64 */ "STABLE",
  /*   65 */ "DATABASE",
  /*   66 */ "TABLES",
  /*   67 */ "STABLES",
  /*   68 */ "VGROUPS",
  /*   69 */ "DROP",
  /*   70 */ "TOPIC",
  /*   71 */ "FUNCTION",
  /*   72 */ "DNODE",
  /*   73 */ "USER",
  /*   74 */ "ACCOUNT",
  /*   75 */ "USE",
  /*   76 */ "DESCRIBE",
  /*   77 */ "ALTER",
  /*   78 */ "PASS",
  /*   79 */ "PRIVILEGE",
  /*   80 */ "LOCAL",
  /*   81 */ "IF",
  /*   82 */ "EXISTS",
  /*   83 */ "AS",
  /*   84 */ "OUTPUTTYPE",
  /*   85 */ "AGGREGATE",
  /*   86 */ "BUFSIZE",
  /*   87 */ "PPS",
  /*   88 */ "TSERIES",
  /*   89 */ "DBS",
  /*   90 */ "STORAGE",
  /*   91 */ "QTIME",
  /*   92 */ "CONNS",
  /*   93 */ "STATE",
  /*   94 */ "KEEP",
  /*   95 */ "CACHE",
  /*   96 */ "REPLICA",
  /*   97 */ "QUORUM",
  /*   98 */ "DAYS",
  /*   99 */ "MINROWS",
  /*  100 */ "MAXROWS",
  /*  101 */ "BLOCKS",
  /*  102 */ "CTIME",
  /*  103 */ "WAL",
  /*  104 */ "FSYNC",
  /*  105 */ "COMP",
  /*  106 */ "PRECISION",
  /*  107 */ "UPDATE",
  /*  108 */ "CACHELAST",
  /*  109 */ "PARTITIONS",
  /*  110 */ "LP",
  /*  111 */ "RP",
  /*  112 */ "UNSIGNED",
  /*  113 */ "TAGS",
  /*  114 */ "USING",
  /*  115 */ "COMMA",
  /*  116 */ "NULL",
  /*  117 */ "SELECT",
  /*  118 */ "UNION",
  /*  119 */ "ALL",
  /*  120 */ "DISTINCT",
  /*  121 */ "FROM",
  /*  122 */ "VARIABLE",
  /*  123 */ "INTERVAL",
  /*  124 */ "SESSION",
  /*  125 */ "FILL",
  /*  126 */ "SLIDING",
  /*  127 */ "ORDER",
  /*  128 */ "BY",
  /*  129 */ "ASC",
  /*  130 */ "DESC",
  /*  131 */ "GROUP",
  /*  132 */ "HAVING",
  /*  133 */ "LIMIT",
  /*  134 */ "OFFSET",
  /*  135 */ "SLIMIT",
  /*  136 */ "SOFFSET",
  /*  137 */ "WHERE",
  /*  138 */ "NOW",
  /*  139 */ "RESET",
  /*  140 */ "QUERY",
  /*  141 */ "SYNCDB",
  /*  142 */ "ADD",
  /*  143 */ "COLUMN",
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
  /*  172 */ "MATCH",
  /*  173 */ "KEY",
  /*  174 */ "OF",
  /*  175 */ "RAISE",
  /*  176 */ "REPLACE",
  /*  177 */ "RESTRICT",
  /*  178 */ "ROW",
  /*  179 */ "STATEMENT",
  /*  180 */ "TRIGGER",
  /*  181 */ "VIEW",
  /*  182 */ "SEMI",
  /*  183 */ "NONE",
  /*  184 */ "PREV",
  /*  185 */ "LINEAR",
  /*  186 */ "IMPORT",
  /*  187 */ "TBNAME",
  /*  188 */ "JOIN",
  /*  189 */ "INSERT",
  /*  190 */ "INTO",
  /*  191 */ "VALUES",
  /*  192 */ "error",
  /*  193 */ "program",
  /*  194 */ "cmd",
  /*  195 */ "dbPrefix",
  /*  196 */ "ids",
  /*  197 */ "cpxName",
  /*  198 */ "ifexists",
  /*  199 */ "alter_db_optr",
  /*  200 */ "alter_topic_optr",
  /*  201 */ "acct_optr",
  /*  202 */ "ifnotexists",
  /*  203 */ "db_optr",
  /*  204 */ "topic_optr",
  /*  205 */ "typename",
  /*  206 */ "bufsize",
  /*  207 */ "pps",
  /*  208 */ "tseries",
  /*  209 */ "dbs",
  /*  210 */ "streams",
  /*  211 */ "storage",
  /*  212 */ "qtime",
  /*  213 */ "users",
  /*  214 */ "conns",
  /*  215 */ "state",
  /*  216 */ "keep",
  /*  217 */ "tagitemlist",
  /*  218 */ "cache",
  /*  219 */ "replica",
  /*  220 */ "quorum",
  /*  221 */ "days",
  /*  222 */ "minrows",
  /*  223 */ "maxrows",
  /*  224 */ "blocks",
  /*  225 */ "ctime",
  /*  226 */ "wal",
  /*  227 */ "fsync",
  /*  228 */ "comp",
  /*  229 */ "prec",
  /*  230 */ "update",
  /*  231 */ "cachelast",
  /*  232 */ "partitions",
  /*  233 */ "signed",
  /*  234 */ "create_table_args",
  /*  235 */ "create_stable_args",
  /*  236 */ "create_table_list",
  /*  237 */ "create_from_stable",
  /*  238 */ "columnlist",
  /*  239 */ "tagNamelist",
  /*  240 */ "select",
  /*  241 */ "column",
  /*  242 */ "tagitem",
  /*  243 */ "selcollist",
  /*  244 */ "from",
  /*  245 */ "where_opt",
  /*  246 */ "interval_opt",
  /*  247 */ "session_option",
  /*  248 */ "fill_opt",
  /*  249 */ "sliding_opt",
  /*  250 */ "groupby_opt",
  /*  251 */ "orderby_opt",
  /*  252 */ "having_opt",
  /*  253 */ "slimit_opt",
  /*  254 */ "limit_opt",
  /*  255 */ "union",
  /*  256 */ "sclp",
  /*  257 */ "distinct",
  /*  258 */ "expr",
  /*  259 */ "as",
  /*  260 */ "tablelist",
  /*  261 */ "tmvar",
  /*  262 */ "sortlist",
  /*  263 */ "sortitem",
  /*  264 */ "item",
  /*  265 */ "sortorder",
  /*  266 */ "grouplist",
  /*  267 */ "exprlist",
  /*  268 */ "expritem",
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
 /*  16 */ "cmd ::= SHOW VNODES IPTOKEN",
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
 /*  50 */ "ids ::= ID",
 /*  51 */ "ids ::= STRING",
 /*  52 */ "ifexists ::= IF EXISTS",
 /*  53 */ "ifexists ::=",
 /*  54 */ "ifnotexists ::= IF NOT EXISTS",
 /*  55 */ "ifnotexists ::=",
 /*  56 */ "cmd ::= CREATE DNODE ids",
 /*  57 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  58 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  59 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
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
 /*  84 */ "keep ::= KEEP tagitemlist",
 /*  85 */ "cache ::= CACHE INTEGER",
 /*  86 */ "replica ::= REPLICA INTEGER",
 /*  87 */ "quorum ::= QUORUM INTEGER",
 /*  88 */ "days ::= DAYS INTEGER",
 /*  89 */ "minrows ::= MINROWS INTEGER",
 /*  90 */ "maxrows ::= MAXROWS INTEGER",
 /*  91 */ "blocks ::= BLOCKS INTEGER",
 /*  92 */ "ctime ::= CTIME INTEGER",
 /*  93 */ "wal ::= WAL INTEGER",
 /*  94 */ "fsync ::= FSYNC INTEGER",
 /*  95 */ "comp ::= COMP INTEGER",
 /*  96 */ "prec ::= PRECISION STRING",
 /*  97 */ "update ::= UPDATE INTEGER",
 /*  98 */ "cachelast ::= CACHELAST INTEGER",
 /*  99 */ "partitions ::= PARTITIONS INTEGER",
 /* 100 */ "db_optr ::=",
 /* 101 */ "db_optr ::= db_optr cache",
 /* 102 */ "db_optr ::= db_optr replica",
 /* 103 */ "db_optr ::= db_optr quorum",
 /* 104 */ "db_optr ::= db_optr days",
 /* 105 */ "db_optr ::= db_optr minrows",
 /* 106 */ "db_optr ::= db_optr maxrows",
 /* 107 */ "db_optr ::= db_optr blocks",
 /* 108 */ "db_optr ::= db_optr ctime",
 /* 109 */ "db_optr ::= db_optr wal",
 /* 110 */ "db_optr ::= db_optr fsync",
 /* 111 */ "db_optr ::= db_optr comp",
 /* 112 */ "db_optr ::= db_optr prec",
 /* 113 */ "db_optr ::= db_optr keep",
 /* 114 */ "db_optr ::= db_optr update",
 /* 115 */ "db_optr ::= db_optr cachelast",
 /* 116 */ "topic_optr ::= db_optr",
 /* 117 */ "topic_optr ::= topic_optr partitions",
 /* 118 */ "alter_db_optr ::=",
 /* 119 */ "alter_db_optr ::= alter_db_optr replica",
 /* 120 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 121 */ "alter_db_optr ::= alter_db_optr keep",
 /* 122 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 123 */ "alter_db_optr ::= alter_db_optr comp",
 /* 124 */ "alter_db_optr ::= alter_db_optr wal",
 /* 125 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 126 */ "alter_db_optr ::= alter_db_optr update",
 /* 127 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 128 */ "alter_topic_optr ::= alter_db_optr",
 /* 129 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 130 */ "typename ::= ids",
 /* 131 */ "typename ::= ids LP signed RP",
 /* 132 */ "typename ::= ids UNSIGNED",
 /* 133 */ "signed ::= INTEGER",
 /* 134 */ "signed ::= PLUS INTEGER",
 /* 135 */ "signed ::= MINUS INTEGER",
 /* 136 */ "cmd ::= CREATE TABLE create_table_args",
 /* 137 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 138 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 139 */ "cmd ::= CREATE TABLE create_table_list",
 /* 140 */ "create_table_list ::= create_from_stable",
 /* 141 */ "create_table_list ::= create_table_list create_from_stable",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 143 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 144 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 145 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 146 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 147 */ "tagNamelist ::= ids",
 /* 148 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 149 */ "columnlist ::= columnlist COMMA column",
 /* 150 */ "columnlist ::= column",
 /* 151 */ "column ::= ids typename",
 /* 152 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 153 */ "tagitemlist ::= tagitem",
 /* 154 */ "tagitem ::= INTEGER",
 /* 155 */ "tagitem ::= FLOAT",
 /* 156 */ "tagitem ::= STRING",
 /* 157 */ "tagitem ::= BOOL",
 /* 158 */ "tagitem ::= NULL",
 /* 159 */ "tagitem ::= MINUS INTEGER",
 /* 160 */ "tagitem ::= MINUS FLOAT",
 /* 161 */ "tagitem ::= PLUS INTEGER",
 /* 162 */ "tagitem ::= PLUS FLOAT",
 /* 163 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 164 */ "select ::= LP select RP",
 /* 165 */ "union ::= select",
 /* 166 */ "union ::= union UNION ALL select",
 /* 167 */ "cmd ::= union",
 /* 168 */ "select ::= SELECT selcollist",
 /* 169 */ "sclp ::= selcollist COMMA",
 /* 170 */ "sclp ::=",
 /* 171 */ "selcollist ::= sclp distinct expr as",
 /* 172 */ "selcollist ::= sclp STAR",
 /* 173 */ "as ::= AS ids",
 /* 174 */ "as ::= ids",
 /* 175 */ "as ::=",
 /* 176 */ "distinct ::= DISTINCT",
 /* 177 */ "distinct ::=",
 /* 178 */ "from ::= FROM tablelist",
 /* 179 */ "from ::= FROM LP union RP",
 /* 180 */ "tablelist ::= ids cpxName",
 /* 181 */ "tablelist ::= ids cpxName ids",
 /* 182 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 183 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 184 */ "tmvar ::= VARIABLE",
 /* 185 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 186 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 187 */ "interval_opt ::=",
 /* 188 */ "session_option ::=",
 /* 189 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 190 */ "fill_opt ::=",
 /* 191 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 192 */ "fill_opt ::= FILL LP ID RP",
 /* 193 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 194 */ "sliding_opt ::=",
 /* 195 */ "orderby_opt ::=",
 /* 196 */ "orderby_opt ::= ORDER BY sortlist",
 /* 197 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 198 */ "sortlist ::= item sortorder",
 /* 199 */ "item ::= ids cpxName",
 /* 200 */ "sortorder ::= ASC",
 /* 201 */ "sortorder ::= DESC",
 /* 202 */ "sortorder ::=",
 /* 203 */ "groupby_opt ::=",
 /* 204 */ "groupby_opt ::= GROUP BY grouplist",
 /* 205 */ "grouplist ::= grouplist COMMA item",
 /* 206 */ "grouplist ::= item",
 /* 207 */ "having_opt ::=",
 /* 208 */ "having_opt ::= HAVING expr",
 /* 209 */ "limit_opt ::=",
 /* 210 */ "limit_opt ::= LIMIT signed",
 /* 211 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 212 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 213 */ "slimit_opt ::=",
 /* 214 */ "slimit_opt ::= SLIMIT signed",
 /* 215 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 216 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 217 */ "where_opt ::=",
 /* 218 */ "where_opt ::= WHERE expr",
 /* 219 */ "expr ::= LP expr RP",
 /* 220 */ "expr ::= ID",
 /* 221 */ "expr ::= ID DOT ID",
 /* 222 */ "expr ::= ID DOT STAR",
 /* 223 */ "expr ::= INTEGER",
 /* 224 */ "expr ::= MINUS INTEGER",
 /* 225 */ "expr ::= PLUS INTEGER",
 /* 226 */ "expr ::= FLOAT",
 /* 227 */ "expr ::= MINUS FLOAT",
 /* 228 */ "expr ::= PLUS FLOAT",
 /* 229 */ "expr ::= STRING",
 /* 230 */ "expr ::= NOW",
 /* 231 */ "expr ::= VARIABLE",
 /* 232 */ "expr ::= PLUS VARIABLE",
 /* 233 */ "expr ::= MINUS VARIABLE",
 /* 234 */ "expr ::= BOOL",
 /* 235 */ "expr ::= NULL",
 /* 236 */ "expr ::= ID LP exprlist RP",
 /* 237 */ "expr ::= ID LP STAR RP",
 /* 238 */ "expr ::= expr IS NULL",
 /* 239 */ "expr ::= expr IS NOT NULL",
 /* 240 */ "expr ::= expr LT expr",
 /* 241 */ "expr ::= expr GT expr",
 /* 242 */ "expr ::= expr LE expr",
 /* 243 */ "expr ::= expr GE expr",
 /* 244 */ "expr ::= expr NE expr",
 /* 245 */ "expr ::= expr EQ expr",
 /* 246 */ "expr ::= expr BETWEEN expr AND expr",
 /* 247 */ "expr ::= expr AND expr",
 /* 248 */ "expr ::= expr OR expr",
 /* 249 */ "expr ::= expr PLUS expr",
 /* 250 */ "expr ::= expr MINUS expr",
 /* 251 */ "expr ::= expr STAR expr",
 /* 252 */ "expr ::= expr SLASH expr",
 /* 253 */ "expr ::= expr REM expr",
 /* 254 */ "expr ::= expr LIKE expr",
 /* 255 */ "expr ::= expr IN LP exprlist RP",
 /* 256 */ "exprlist ::= exprlist COMMA expritem",
 /* 257 */ "exprlist ::= expritem",
 /* 258 */ "expritem ::= expr",
 /* 259 */ "expritem ::=",
 /* 260 */ "cmd ::= RESET QUERY CACHE",
 /* 261 */ "cmd ::= SYNCDB ids REPLICA",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 268 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 269 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 270 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 271 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 272 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 273 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 274 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 275 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 216: /* keep */
    case 217: /* tagitemlist */
    case 238: /* columnlist */
    case 239: /* tagNamelist */
    case 248: /* fill_opt */
    case 250: /* groupby_opt */
    case 251: /* orderby_opt */
    case 262: /* sortlist */
    case 266: /* grouplist */
{
taosArrayDestroy((yypminor->yy93));
}
      break;
    case 236: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy532));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy224));
}
      break;
    case 243: /* selcollist */
    case 256: /* sclp */
    case 267: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy93));
}
      break;
    case 244: /* from */
    case 260: /* tablelist */
{
destroyRelationInfo((yypminor->yy330));
}
      break;
    case 245: /* where_opt */
    case 252: /* having_opt */
    case 258: /* expr */
    case 268: /* expritem */
{
tSqlExprDestroy((yypminor->yy68));
}
      break;
    case 255: /* union */
{
destroyAllSqlNode((yypminor->yy93));
}
      break;
    case 263: /* sortitem */
{
tVariantDestroy(&(yypminor->yy518));
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
  {  193,   -1 }, /* (0) program ::= cmd */
  {  194,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  194,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  194,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  194,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  194,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  194,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  194,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  194,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  194,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  194,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  194,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  194,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  194,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  194,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  194,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  194,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  195,    0 }, /* (17) dbPrefix ::= */
  {  195,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  197,    0 }, /* (19) cpxName ::= */
  {  197,   -2 }, /* (20) cpxName ::= DOT ids */
  {  194,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  194,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  194,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  194,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  194,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  194,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  194,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  194,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  194,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  194,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  194,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  194,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  194,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  194,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  194,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  194,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  194,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  194,   -2 }, /* (38) cmd ::= USE ids */
  {  194,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  194,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  194,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  194,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  194,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  194,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  194,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  194,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  194,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  194,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  194,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  196,   -1 }, /* (50) ids ::= ID */
  {  196,   -1 }, /* (51) ids ::= STRING */
  {  198,   -2 }, /* (52) ifexists ::= IF EXISTS */
  {  198,    0 }, /* (53) ifexists ::= */
  {  202,   -3 }, /* (54) ifnotexists ::= IF NOT EXISTS */
  {  202,    0 }, /* (55) ifnotexists ::= */
  {  194,   -3 }, /* (56) cmd ::= CREATE DNODE ids */
  {  194,   -6 }, /* (57) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  194,   -5 }, /* (58) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  194,   -5 }, /* (59) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  194,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  194,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  194,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  206,    0 }, /* (63) bufsize ::= */
  {  206,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  207,    0 }, /* (65) pps ::= */
  {  207,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  208,    0 }, /* (67) tseries ::= */
  {  208,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  209,    0 }, /* (69) dbs ::= */
  {  209,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  210,    0 }, /* (71) streams ::= */
  {  210,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  211,    0 }, /* (73) storage ::= */
  {  211,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  212,    0 }, /* (75) qtime ::= */
  {  212,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  213,    0 }, /* (77) users ::= */
  {  213,   -2 }, /* (78) users ::= USERS INTEGER */
  {  214,    0 }, /* (79) conns ::= */
  {  214,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  215,    0 }, /* (81) state ::= */
  {  215,   -2 }, /* (82) state ::= STATE ids */
  {  201,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  216,   -2 }, /* (84) keep ::= KEEP tagitemlist */
  {  218,   -2 }, /* (85) cache ::= CACHE INTEGER */
  {  219,   -2 }, /* (86) replica ::= REPLICA INTEGER */
  {  220,   -2 }, /* (87) quorum ::= QUORUM INTEGER */
  {  221,   -2 }, /* (88) days ::= DAYS INTEGER */
  {  222,   -2 }, /* (89) minrows ::= MINROWS INTEGER */
  {  223,   -2 }, /* (90) maxrows ::= MAXROWS INTEGER */
  {  224,   -2 }, /* (91) blocks ::= BLOCKS INTEGER */
  {  225,   -2 }, /* (92) ctime ::= CTIME INTEGER */
  {  226,   -2 }, /* (93) wal ::= WAL INTEGER */
  {  227,   -2 }, /* (94) fsync ::= FSYNC INTEGER */
  {  228,   -2 }, /* (95) comp ::= COMP INTEGER */
  {  229,   -2 }, /* (96) prec ::= PRECISION STRING */
  {  230,   -2 }, /* (97) update ::= UPDATE INTEGER */
  {  231,   -2 }, /* (98) cachelast ::= CACHELAST INTEGER */
  {  232,   -2 }, /* (99) partitions ::= PARTITIONS INTEGER */
  {  203,    0 }, /* (100) db_optr ::= */
  {  203,   -2 }, /* (101) db_optr ::= db_optr cache */
  {  203,   -2 }, /* (102) db_optr ::= db_optr replica */
  {  203,   -2 }, /* (103) db_optr ::= db_optr quorum */
  {  203,   -2 }, /* (104) db_optr ::= db_optr days */
  {  203,   -2 }, /* (105) db_optr ::= db_optr minrows */
  {  203,   -2 }, /* (106) db_optr ::= db_optr maxrows */
  {  203,   -2 }, /* (107) db_optr ::= db_optr blocks */
  {  203,   -2 }, /* (108) db_optr ::= db_optr ctime */
  {  203,   -2 }, /* (109) db_optr ::= db_optr wal */
  {  203,   -2 }, /* (110) db_optr ::= db_optr fsync */
  {  203,   -2 }, /* (111) db_optr ::= db_optr comp */
  {  203,   -2 }, /* (112) db_optr ::= db_optr prec */
  {  203,   -2 }, /* (113) db_optr ::= db_optr keep */
  {  203,   -2 }, /* (114) db_optr ::= db_optr update */
  {  203,   -2 }, /* (115) db_optr ::= db_optr cachelast */
  {  204,   -1 }, /* (116) topic_optr ::= db_optr */
  {  204,   -2 }, /* (117) topic_optr ::= topic_optr partitions */
  {  199,    0 }, /* (118) alter_db_optr ::= */
  {  199,   -2 }, /* (119) alter_db_optr ::= alter_db_optr replica */
  {  199,   -2 }, /* (120) alter_db_optr ::= alter_db_optr quorum */
  {  199,   -2 }, /* (121) alter_db_optr ::= alter_db_optr keep */
  {  199,   -2 }, /* (122) alter_db_optr ::= alter_db_optr blocks */
  {  199,   -2 }, /* (123) alter_db_optr ::= alter_db_optr comp */
  {  199,   -2 }, /* (124) alter_db_optr ::= alter_db_optr wal */
  {  199,   -2 }, /* (125) alter_db_optr ::= alter_db_optr fsync */
  {  199,   -2 }, /* (126) alter_db_optr ::= alter_db_optr update */
  {  199,   -2 }, /* (127) alter_db_optr ::= alter_db_optr cachelast */
  {  200,   -1 }, /* (128) alter_topic_optr ::= alter_db_optr */
  {  200,   -2 }, /* (129) alter_topic_optr ::= alter_topic_optr partitions */
  {  205,   -1 }, /* (130) typename ::= ids */
  {  205,   -4 }, /* (131) typename ::= ids LP signed RP */
  {  205,   -2 }, /* (132) typename ::= ids UNSIGNED */
  {  233,   -1 }, /* (133) signed ::= INTEGER */
  {  233,   -2 }, /* (134) signed ::= PLUS INTEGER */
  {  233,   -2 }, /* (135) signed ::= MINUS INTEGER */
  {  194,   -3 }, /* (136) cmd ::= CREATE TABLE create_table_args */
  {  194,   -3 }, /* (137) cmd ::= CREATE TABLE create_stable_args */
  {  194,   -3 }, /* (138) cmd ::= CREATE STABLE create_stable_args */
  {  194,   -3 }, /* (139) cmd ::= CREATE TABLE create_table_list */
  {  236,   -1 }, /* (140) create_table_list ::= create_from_stable */
  {  236,   -2 }, /* (141) create_table_list ::= create_table_list create_from_stable */
  {  234,   -6 }, /* (142) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  235,  -10 }, /* (143) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  237,  -10 }, /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  237,  -13 }, /* (145) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  239,   -3 }, /* (146) tagNamelist ::= tagNamelist COMMA ids */
  {  239,   -1 }, /* (147) tagNamelist ::= ids */
  {  234,   -5 }, /* (148) create_table_args ::= ifnotexists ids cpxName AS select */
  {  238,   -3 }, /* (149) columnlist ::= columnlist COMMA column */
  {  238,   -1 }, /* (150) columnlist ::= column */
  {  241,   -2 }, /* (151) column ::= ids typename */
  {  217,   -3 }, /* (152) tagitemlist ::= tagitemlist COMMA tagitem */
  {  217,   -1 }, /* (153) tagitemlist ::= tagitem */
  {  242,   -1 }, /* (154) tagitem ::= INTEGER */
  {  242,   -1 }, /* (155) tagitem ::= FLOAT */
  {  242,   -1 }, /* (156) tagitem ::= STRING */
  {  242,   -1 }, /* (157) tagitem ::= BOOL */
  {  242,   -1 }, /* (158) tagitem ::= NULL */
  {  242,   -2 }, /* (159) tagitem ::= MINUS INTEGER */
  {  242,   -2 }, /* (160) tagitem ::= MINUS FLOAT */
  {  242,   -2 }, /* (161) tagitem ::= PLUS INTEGER */
  {  242,   -2 }, /* (162) tagitem ::= PLUS FLOAT */
  {  240,  -13 }, /* (163) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  240,   -3 }, /* (164) select ::= LP select RP */
  {  255,   -1 }, /* (165) union ::= select */
  {  255,   -4 }, /* (166) union ::= union UNION ALL select */
  {  194,   -1 }, /* (167) cmd ::= union */
  {  240,   -2 }, /* (168) select ::= SELECT selcollist */
  {  256,   -2 }, /* (169) sclp ::= selcollist COMMA */
  {  256,    0 }, /* (170) sclp ::= */
  {  243,   -4 }, /* (171) selcollist ::= sclp distinct expr as */
  {  243,   -2 }, /* (172) selcollist ::= sclp STAR */
  {  259,   -2 }, /* (173) as ::= AS ids */
  {  259,   -1 }, /* (174) as ::= ids */
  {  259,    0 }, /* (175) as ::= */
  {  257,   -1 }, /* (176) distinct ::= DISTINCT */
  {  257,    0 }, /* (177) distinct ::= */
  {  244,   -2 }, /* (178) from ::= FROM tablelist */
  {  244,   -4 }, /* (179) from ::= FROM LP union RP */
  {  260,   -2 }, /* (180) tablelist ::= ids cpxName */
  {  260,   -3 }, /* (181) tablelist ::= ids cpxName ids */
  {  260,   -4 }, /* (182) tablelist ::= tablelist COMMA ids cpxName */
  {  260,   -5 }, /* (183) tablelist ::= tablelist COMMA ids cpxName ids */
  {  261,   -1 }, /* (184) tmvar ::= VARIABLE */
  {  246,   -4 }, /* (185) interval_opt ::= INTERVAL LP tmvar RP */
  {  246,   -6 }, /* (186) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  246,    0 }, /* (187) interval_opt ::= */
  {  247,    0 }, /* (188) session_option ::= */
  {  247,   -7 }, /* (189) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  248,    0 }, /* (190) fill_opt ::= */
  {  248,   -6 }, /* (191) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  248,   -4 }, /* (192) fill_opt ::= FILL LP ID RP */
  {  249,   -4 }, /* (193) sliding_opt ::= SLIDING LP tmvar RP */
  {  249,    0 }, /* (194) sliding_opt ::= */
  {  251,    0 }, /* (195) orderby_opt ::= */
  {  251,   -3 }, /* (196) orderby_opt ::= ORDER BY sortlist */
  {  262,   -4 }, /* (197) sortlist ::= sortlist COMMA item sortorder */
  {  262,   -2 }, /* (198) sortlist ::= item sortorder */
  {  264,   -2 }, /* (199) item ::= ids cpxName */
  {  265,   -1 }, /* (200) sortorder ::= ASC */
  {  265,   -1 }, /* (201) sortorder ::= DESC */
  {  265,    0 }, /* (202) sortorder ::= */
  {  250,    0 }, /* (203) groupby_opt ::= */
  {  250,   -3 }, /* (204) groupby_opt ::= GROUP BY grouplist */
  {  266,   -3 }, /* (205) grouplist ::= grouplist COMMA item */
  {  266,   -1 }, /* (206) grouplist ::= item */
  {  252,    0 }, /* (207) having_opt ::= */
  {  252,   -2 }, /* (208) having_opt ::= HAVING expr */
  {  254,    0 }, /* (209) limit_opt ::= */
  {  254,   -2 }, /* (210) limit_opt ::= LIMIT signed */
  {  254,   -4 }, /* (211) limit_opt ::= LIMIT signed OFFSET signed */
  {  254,   -4 }, /* (212) limit_opt ::= LIMIT signed COMMA signed */
  {  253,    0 }, /* (213) slimit_opt ::= */
  {  253,   -2 }, /* (214) slimit_opt ::= SLIMIT signed */
  {  253,   -4 }, /* (215) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  253,   -4 }, /* (216) slimit_opt ::= SLIMIT signed COMMA signed */
  {  245,    0 }, /* (217) where_opt ::= */
  {  245,   -2 }, /* (218) where_opt ::= WHERE expr */
  {  258,   -3 }, /* (219) expr ::= LP expr RP */
  {  258,   -1 }, /* (220) expr ::= ID */
  {  258,   -3 }, /* (221) expr ::= ID DOT ID */
  {  258,   -3 }, /* (222) expr ::= ID DOT STAR */
  {  258,   -1 }, /* (223) expr ::= INTEGER */
  {  258,   -2 }, /* (224) expr ::= MINUS INTEGER */
  {  258,   -2 }, /* (225) expr ::= PLUS INTEGER */
  {  258,   -1 }, /* (226) expr ::= FLOAT */
  {  258,   -2 }, /* (227) expr ::= MINUS FLOAT */
  {  258,   -2 }, /* (228) expr ::= PLUS FLOAT */
  {  258,   -1 }, /* (229) expr ::= STRING */
  {  258,   -1 }, /* (230) expr ::= NOW */
  {  258,   -1 }, /* (231) expr ::= VARIABLE */
  {  258,   -2 }, /* (232) expr ::= PLUS VARIABLE */
  {  258,   -2 }, /* (233) expr ::= MINUS VARIABLE */
  {  258,   -1 }, /* (234) expr ::= BOOL */
  {  258,   -1 }, /* (235) expr ::= NULL */
  {  258,   -4 }, /* (236) expr ::= ID LP exprlist RP */
  {  258,   -4 }, /* (237) expr ::= ID LP STAR RP */
  {  258,   -3 }, /* (238) expr ::= expr IS NULL */
  {  258,   -4 }, /* (239) expr ::= expr IS NOT NULL */
  {  258,   -3 }, /* (240) expr ::= expr LT expr */
  {  258,   -3 }, /* (241) expr ::= expr GT expr */
  {  258,   -3 }, /* (242) expr ::= expr LE expr */
  {  258,   -3 }, /* (243) expr ::= expr GE expr */
  {  258,   -3 }, /* (244) expr ::= expr NE expr */
  {  258,   -3 }, /* (245) expr ::= expr EQ expr */
  {  258,   -5 }, /* (246) expr ::= expr BETWEEN expr AND expr */
  {  258,   -3 }, /* (247) expr ::= expr AND expr */
  {  258,   -3 }, /* (248) expr ::= expr OR expr */
  {  258,   -3 }, /* (249) expr ::= expr PLUS expr */
  {  258,   -3 }, /* (250) expr ::= expr MINUS expr */
  {  258,   -3 }, /* (251) expr ::= expr STAR expr */
  {  258,   -3 }, /* (252) expr ::= expr SLASH expr */
  {  258,   -3 }, /* (253) expr ::= expr REM expr */
  {  258,   -3 }, /* (254) expr ::= expr LIKE expr */
  {  258,   -5 }, /* (255) expr ::= expr IN LP exprlist RP */
  {  267,   -3 }, /* (256) exprlist ::= exprlist COMMA expritem */
  {  267,   -1 }, /* (257) exprlist ::= expritem */
  {  268,   -1 }, /* (258) expritem ::= expr */
  {  268,    0 }, /* (259) expritem ::= */
  {  194,   -3 }, /* (260) cmd ::= RESET QUERY CACHE */
  {  194,   -3 }, /* (261) cmd ::= SYNCDB ids REPLICA */
  {  194,   -7 }, /* (262) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  194,   -7 }, /* (263) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  194,   -7 }, /* (264) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  194,   -7 }, /* (265) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  194,   -8 }, /* (266) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  194,   -9 }, /* (267) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  194,   -7 }, /* (268) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  194,   -7 }, /* (269) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  194,   -7 }, /* (270) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  194,   -7 }, /* (271) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  194,   -8 }, /* (272) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  194,   -3 }, /* (273) cmd ::= KILL CONNECTION INTEGER */
  {  194,   -5 }, /* (274) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  194,   -5 }, /* (275) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 136: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==136);
      case 137: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==137);
      case 138: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==138);
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
      case 16: /* cmd ::= SHOW VNODES IPTOKEN */
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
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy372, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy77);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy77);}
        break;
      case 50: /* ids ::= ID */
      case 51: /* ids ::= STRING */ yytestcase(yyruleno==51);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 52: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 53: /* ifexists ::= */
      case 55: /* ifnotexists ::= */ yytestcase(yyruleno==55);
      case 177: /* distinct ::= */ yytestcase(yyruleno==177);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 54: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 56: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 57: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy77);}
        break;
      case 58: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 59: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==59);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy372, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy325, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy325, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy77.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy77.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy77.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy77.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy77.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy77.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy77.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy77.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy77.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy77 = yylhsminor.yy77;
        break;
      case 84: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy93 = yymsp[0].minor.yy93; }
        break;
      case 85: /* cache ::= CACHE INTEGER */
      case 86: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==86);
      case 87: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==87);
      case 88: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==89);
      case 90: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==90);
      case 91: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==91);
      case 92: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==92);
      case 93: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==93);
      case 94: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==94);
      case 95: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==95);
      case 96: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==96);
      case 97: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==97);
      case 98: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==98);
      case 99: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==99);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 100: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy372); yymsp[1].minor.yy372.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 101: /* db_optr ::= db_optr cache */
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 102: /* db_optr ::= db_optr replica */
      case 119: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==119);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 103: /* db_optr ::= db_optr quorum */
      case 120: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==120);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 104: /* db_optr ::= db_optr days */
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 105: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 106: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 107: /* db_optr ::= db_optr blocks */
      case 122: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==122);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 108: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 109: /* db_optr ::= db_optr wal */
      case 124: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==124);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 110: /* db_optr ::= db_optr fsync */
      case 125: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==125);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 111: /* db_optr ::= db_optr comp */
      case 123: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==123);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 112: /* db_optr ::= db_optr prec */
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 113: /* db_optr ::= db_optr keep */
      case 121: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==121);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.keep = yymsp[0].minor.yy93; }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 114: /* db_optr ::= db_optr update */
      case 126: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==126);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 115: /* db_optr ::= db_optr cachelast */
      case 127: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==127);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 116: /* topic_optr ::= db_optr */
      case 128: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==128);
{ yylhsminor.yy372 = yymsp[0].minor.yy372; yylhsminor.yy372.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy372 = yylhsminor.yy372;
        break;
      case 117: /* topic_optr ::= topic_optr partitions */
      case 129: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==129);
{ yylhsminor.yy372 = yymsp[-1].minor.yy372; yylhsminor.yy372.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy372 = yylhsminor.yy372;
        break;
      case 118: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy372); yymsp[1].minor.yy372.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 130: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy325, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 131: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy279 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy325, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy279;  // negative value of name length
    tSetColumnType(&yylhsminor.yy325, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 132: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy325, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 133: /* signed ::= INTEGER */
{ yylhsminor.yy279 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy279 = yylhsminor.yy279;
        break;
      case 134: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy279 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 135: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy279 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 139: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy532;}
        break;
      case 140: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy528);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy532 = pCreateTable;
}
  yymsp[0].minor.yy532 = yylhsminor.yy532;
        break;
      case 141: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy532->childTableInfo, &yymsp[0].minor.yy528);
  yylhsminor.yy532 = yymsp[-1].minor.yy532;
}
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy532 = tSetCreateTableInfo(yymsp[-1].minor.yy93, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy532, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy532 = yylhsminor.yy532;
        break;
      case 143: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy532 = tSetCreateTableInfo(yymsp[-5].minor.yy93, yymsp[-1].minor.yy93, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy532, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy532 = yylhsminor.yy532;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy528 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy93, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy528 = yylhsminor.yy528;
        break;
      case 145: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy528 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy93, yymsp[-1].minor.yy93, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy528 = yylhsminor.yy528;
        break;
      case 146: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy93, &yymsp[0].minor.yy0); yylhsminor.yy93 = yymsp[-2].minor.yy93;  }
  yymsp[-2].minor.yy93 = yylhsminor.yy93;
        break;
      case 147: /* tagNamelist ::= ids */
{yylhsminor.yy93 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy93, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 148: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy532 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy224, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy532, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy532 = yylhsminor.yy532;
        break;
      case 149: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy93, &yymsp[0].minor.yy325); yylhsminor.yy93 = yymsp[-2].minor.yy93;  }
  yymsp[-2].minor.yy93 = yylhsminor.yy93;
        break;
      case 150: /* columnlist ::= column */
{yylhsminor.yy93 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy93, &yymsp[0].minor.yy325);}
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 151: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy325, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy325);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 152: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy93 = tVariantListAppend(yymsp[-2].minor.yy93, &yymsp[0].minor.yy518, -1);    }
  yymsp[-2].minor.yy93 = yylhsminor.yy93;
        break;
      case 153: /* tagitemlist ::= tagitem */
{ yylhsminor.yy93 = tVariantListAppend(NULL, &yymsp[0].minor.yy518, -1); }
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 154: /* tagitem ::= INTEGER */
      case 155: /* tagitem ::= FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= STRING */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= BOOL */ yytestcase(yyruleno==157);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy518, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy518 = yylhsminor.yy518;
        break;
      case 158: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy518, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy518 = yylhsminor.yy518;
        break;
      case 159: /* tagitem ::= MINUS INTEGER */
      case 160: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==162);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy518, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy518 = yylhsminor.yy518;
        break;
      case 163: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy224 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy93, yymsp[-10].minor.yy330, yymsp[-9].minor.yy68, yymsp[-4].minor.yy93, yymsp[-3].minor.yy93, &yymsp[-8].minor.yy42, &yymsp[-7].minor.yy15, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy93, &yymsp[0].minor.yy284, &yymsp[-1].minor.yy284, yymsp[-2].minor.yy68);
}
  yymsp[-12].minor.yy224 = yylhsminor.yy224;
        break;
      case 164: /* select ::= LP select RP */
{yymsp[-2].minor.yy224 = yymsp[-1].minor.yy224;}
        break;
      case 165: /* union ::= select */
{ yylhsminor.yy93 = setSubclause(NULL, yymsp[0].minor.yy224); }
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 166: /* union ::= union UNION ALL select */
{ yylhsminor.yy93 = appendSelectClause(yymsp[-3].minor.yy93, yymsp[0].minor.yy224); }
  yymsp[-3].minor.yy93 = yylhsminor.yy93;
        break;
      case 167: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy93, NULL, TSDB_SQL_SELECT); }
        break;
      case 168: /* select ::= SELECT selcollist */
{
  yylhsminor.yy224 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy93, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy224 = yylhsminor.yy224;
        break;
      case 169: /* sclp ::= selcollist COMMA */
{yylhsminor.yy93 = yymsp[-1].minor.yy93;}
  yymsp[-1].minor.yy93 = yylhsminor.yy93;
        break;
      case 170: /* sclp ::= */
      case 195: /* orderby_opt ::= */ yytestcase(yyruleno==195);
{yymsp[1].minor.yy93 = 0;}
        break;
      case 171: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy93 = tSqlExprListAppend(yymsp[-3].minor.yy93, yymsp[-1].minor.yy68,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy93 = yylhsminor.yy93;
        break;
      case 172: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy93 = tSqlExprListAppend(yymsp[-1].minor.yy93, pNode, 0, 0);
}
  yymsp[-1].minor.yy93 = yylhsminor.yy93;
        break;
      case 173: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 174: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 175: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 176: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy330 = yymsp[0].minor.yy330;}
        break;
      case 179: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy330 = setSubquery(NULL, yymsp[-1].minor.yy93);}
        break;
      case 180: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy330 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy330 = yylhsminor.yy330;
        break;
      case 181: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy330 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy330 = yylhsminor.yy330;
        break;
      case 182: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy330 = setTableNameList(yymsp[-3].minor.yy330, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy330 = yylhsminor.yy330;
        break;
      case 183: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy330 = setTableNameList(yymsp[-4].minor.yy330, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy330 = yylhsminor.yy330;
        break;
      case 184: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 185: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy42.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy42.offset.n = 0;}
        break;
      case 186: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy42.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy42.offset = yymsp[-1].minor.yy0;}
        break;
      case 187: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy42, 0, sizeof(yymsp[1].minor.yy42));}
        break;
      case 188: /* session_option ::= */
{yymsp[1].minor.yy15.col.n = 0; yymsp[1].minor.yy15.gap.n = 0;}
        break;
      case 189: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy15.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy15.gap = yymsp[-1].minor.yy0;
}
        break;
      case 190: /* fill_opt ::= */
{ yymsp[1].minor.yy93 = 0;     }
        break;
      case 191: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy93, &A, -1, 0);
    yymsp[-5].minor.yy93 = yymsp[-1].minor.yy93;
}
        break;
      case 192: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy93 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 193: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 194: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 196: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy93 = yymsp[0].minor.yy93;}
        break;
      case 197: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy93 = tVariantListAppend(yymsp[-3].minor.yy93, &yymsp[-1].minor.yy518, yymsp[0].minor.yy150);
}
  yymsp[-3].minor.yy93 = yylhsminor.yy93;
        break;
      case 198: /* sortlist ::= item sortorder */
{
  yylhsminor.yy93 = tVariantListAppend(NULL, &yymsp[-1].minor.yy518, yymsp[0].minor.yy150);
}
  yymsp[-1].minor.yy93 = yylhsminor.yy93;
        break;
      case 199: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy518, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy518 = yylhsminor.yy518;
        break;
      case 200: /* sortorder ::= ASC */
{ yymsp[0].minor.yy150 = TSDB_ORDER_ASC; }
        break;
      case 201: /* sortorder ::= DESC */
{ yymsp[0].minor.yy150 = TSDB_ORDER_DESC;}
        break;
      case 202: /* sortorder ::= */
{ yymsp[1].minor.yy150 = TSDB_ORDER_ASC; }
        break;
      case 203: /* groupby_opt ::= */
{ yymsp[1].minor.yy93 = 0;}
        break;
      case 204: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy93 = yymsp[0].minor.yy93;}
        break;
      case 205: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy93 = tVariantListAppend(yymsp[-2].minor.yy93, &yymsp[0].minor.yy518, -1);
}
  yymsp[-2].minor.yy93 = yylhsminor.yy93;
        break;
      case 206: /* grouplist ::= item */
{
  yylhsminor.yy93 = tVariantListAppend(NULL, &yymsp[0].minor.yy518, -1);
}
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 207: /* having_opt ::= */
      case 217: /* where_opt ::= */ yytestcase(yyruleno==217);
      case 259: /* expritem ::= */ yytestcase(yyruleno==259);
{yymsp[1].minor.yy68 = 0;}
        break;
      case 208: /* having_opt ::= HAVING expr */
      case 218: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==218);
{yymsp[-1].minor.yy68 = yymsp[0].minor.yy68;}
        break;
      case 209: /* limit_opt ::= */
      case 213: /* slimit_opt ::= */ yytestcase(yyruleno==213);
{yymsp[1].minor.yy284.limit = -1; yymsp[1].minor.yy284.offset = 0;}
        break;
      case 210: /* limit_opt ::= LIMIT signed */
      case 214: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==214);
{yymsp[-1].minor.yy284.limit = yymsp[0].minor.yy279;  yymsp[-1].minor.yy284.offset = 0;}
        break;
      case 211: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy279;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy279;}
        break;
      case 212: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy279;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy279;}
        break;
      case 215: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy279;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy279;}
        break;
      case 216: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy279;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy279;}
        break;
      case 219: /* expr ::= LP expr RP */
{yylhsminor.yy68 = yymsp[-1].minor.yy68; yylhsminor.yy68->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy68->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 220: /* expr ::= ID */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 221: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 222: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 223: /* expr ::= INTEGER */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 224: /* expr ::= MINUS INTEGER */
      case 225: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==225);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 226: /* expr ::= FLOAT */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 227: /* expr ::= MINUS FLOAT */
      case 228: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 229: /* expr ::= STRING */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 230: /* expr ::= NOW */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 231: /* expr ::= VARIABLE */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 232: /* expr ::= PLUS VARIABLE */
      case 233: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 234: /* expr ::= BOOL */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 235: /* expr ::= NULL */
{ yylhsminor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 236: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy68 = tSqlExprCreateFunction(yymsp[-1].minor.yy93, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 237: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy68 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 238: /* expr ::= expr IS NULL */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 239: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-3].minor.yy68, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 240: /* expr ::= expr LT expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_LT);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 241: /* expr ::= expr GT expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_GT);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 242: /* expr ::= expr LE expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_LE);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 243: /* expr ::= expr GE expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_GE);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 244: /* expr ::= expr NE expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_NE);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 245: /* expr ::= expr EQ expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_EQ);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 246: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy68); yylhsminor.yy68 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy68, yymsp[-2].minor.yy68, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy68, TK_LE), TK_AND);}
  yymsp[-4].minor.yy68 = yylhsminor.yy68;
        break;
      case 247: /* expr ::= expr AND expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_AND);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 248: /* expr ::= expr OR expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_OR); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 249: /* expr ::= expr PLUS expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_PLUS);  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 250: /* expr ::= expr MINUS expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_MINUS); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 251: /* expr ::= expr STAR expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_STAR);  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 252: /* expr ::= expr SLASH expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_DIVIDE);}
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 253: /* expr ::= expr REM expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_REM);   }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 254: /* expr ::= expr LIKE expr */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_LIKE);  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 255: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy68 = tSqlExprCreate(yymsp[-4].minor.yy68, (tSqlExpr*)yymsp[-1].minor.yy93, TK_IN); }
  yymsp[-4].minor.yy68 = yylhsminor.yy68;
        break;
      case 256: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy93 = tSqlExprListAppend(yymsp[-2].minor.yy93,yymsp[0].minor.yy68,0, 0);}
  yymsp[-2].minor.yy93 = yylhsminor.yy93;
        break;
      case 257: /* exprlist ::= expritem */
{yylhsminor.yy93 = tSqlExprListAppend(0,yymsp[0].minor.yy68,0, 0);}
  yymsp[0].minor.yy93 = yylhsminor.yy93;
        break;
      case 258: /* expritem ::= expr */
{yylhsminor.yy68 = yymsp[0].minor.yy68;}
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 260: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 261: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 267: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy518, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 273: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 274: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 275: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
