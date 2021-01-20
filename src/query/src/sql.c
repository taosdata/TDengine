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
#include "tstoken.h"
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
#define YYNOCODE 278
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateTableSQL* yy38;
  SCreateAcctSQL yy71;
  tSQLExpr* yy78;
  int yy96;
  SQuerySQL* yy148;
  SCreatedTableInfo yy152;
  SSubclauseInfo* yy153;
  tSQLExprList* yy166;
  SLimitVal yy167;
  TAOS_FIELD yy183;
  SCreateDBInfo yy234;
  int64_t yy325;
  SIntervalVal yy400;
  SArray* yy421;
  tVariant yy430;
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
#define YYNSTATE             282
#define YYNRULE              248
#define YYNRULE_WITH_ACTION  248
#define YYNTOKEN             209
#define YY_MAX_SHIFT         281
#define YY_MIN_SHIFTREDUCE   461
#define YY_MAX_SHIFTREDUCE   708
#define YY_ERROR_ACTION      709
#define YY_ACCEPT_ACTION     710
#define YY_NO_ACTION         711
#define YY_MIN_REDUCE        712
#define YY_MAX_REDUCE        959
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
#define YY_ACTTAB_COUNT (623)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   158,  505,  158,  710,  281,  857,  659,  578,  182,  506,
 /*    10 */   941,  185,  942,   41,   42,   15,   43,   44,   26,  179,
 /*    20 */   190,   35,  505,  505,  231,   47,   45,   49,   46,  868,
 /*    30 */   506,  506,  846,   40,   39,  256,  255,   38,   37,   36,
 /*    40 */    41,   42,  660,   43,   44,  857,  951,  190,   35,  178,
 /*    50 */   279,  231,   47,   45,   49,   46,  180,  195,  843,  214,
 /*    60 */    40,   39,  157,   61,   38,   37,   36,  462,  463,  464,
 /*    70 */   465,  466,  467,  468,  469,  470,  471,  472,  473,  280,
 /*    80 */   198,  846,  204,   41,   42,  865,   43,   44,  246,  266,
 /*    90 */   190,   35,  158,  834,  231,   47,   45,   49,   46,  122,
 /*   100 */   122,  184,  942,   40,   39,  122,   62,   38,   37,   36,
 /*   110 */    20,  244,  274,  273,  243,  242,  241,  272,  240,  271,
 /*   120 */   270,  269,  239,  268,  267,   38,   37,   36,  813,  657,
 /*   130 */   801,  802,  803,  804,  805,  806,  807,  808,  809,  810,
 /*   140 */   811,  812,  814,  815,   42,  200,   43,   44,  253,  252,
 /*   150 */   190,   35,  835,  275,  231,   47,   45,   49,   46,  228,
 /*   160 */   895,   66,  226,   40,   39,  115,  894,   38,   37,   36,
 /*   170 */    26,   43,   44,   32,   26,  190,   35,  846,  207,  231,
 /*   180 */    47,   45,   49,   46,   26,  211,  210,  162,   40,   39,
 /*   190 */   196,  845,   38,   37,   36,  189,  670,  117,  938,  661,
 /*   200 */    71,  664,   26,  667,  122,  189,  670,  188,  193,  661,
 /*   210 */   843,  664,  194,  667,  843,  189,  670,   16,   21,  661,
 /*   220 */    90,  664,  249,  667,  843,  266,   32,  186,  187,  246,
 /*   230 */    26,  230,  837,  166,  278,  277,  109,  186,  187,  167,
 /*   240 */   250,  615,  843,  199,  102,  101,  165,  186,  187,    4,
 /*   250 */    20,   26,  274,  273,  219,  197,   26,  272,  248,  271,
 /*   260 */   270,  269,   10,  268,  267,   67,   70,  132,  254,  819,
 /*   270 */   843,  217,  817,  818,   21,  844,   27,  820,  905,  822,
 /*   280 */   823,  821,   32,  824,  825,   47,   45,   49,   46,  258,
 /*   290 */   663,  843,  666,   40,   39,   48,  842,   38,   37,   36,
 /*   300 */   754,  232,  213,  146,   69,   48,  904,  669,  763,  173,
 /*   310 */   755,  146,   68,  146,  662,   48,  665,  669,  599,  638,
 /*   320 */   639,  596,  668,  597,   33,  598,  612,  669,  603,  607,
 /*   330 */   604,   22,  668,  832,  833,   25,  836,  216,  625,   88,
 /*   340 */    92,  937,  668,  119,  936,   82,   97,  100,   91,  201,
 /*   350 */   202,  191,  588,  629,   94,    3,  136,   27,   52,  152,
 /*   360 */   148,   29,   77,   73,   76,  150,  105,  104,  103,   40,
 /*   370 */    39,  630,  689,   38,   37,   36,   18,   17,  671,   53,
 /*   380 */    56,  174,  234,   17,  589,   81,   80,   27,  175,   52,
 /*   390 */    12,   11,   99,   98,  673,   87,   86,   57,   54,  160,
 /*   400 */    59,  161,  577,   14,   13,  163,  601,  164,  602,  114,
 /*   410 */   112,  170,  171,  901,  169,  900,  156,  168,  159,  192,
 /*   420 */   257,  116,  867,  859,  600,  872,   32,  874,  118,  133,
 /*   430 */   887,  886,  131,  134,  135,  765,  238,  154,   30,  215,
 /*   440 */   247,  762,  956,   78,  955,  953,  137,  251,  113,  950,
 /*   450 */    84,  949,  947,  138,  783,   31,   28,  155,  752,   93,
 /*   460 */   750,   95,  624,  220,   96,  181,  748,  747,  203,  147,
 /*   470 */    58,  745,  224,  744,  743,  856,  742,  741,  149,  151,
 /*   480 */   738,   55,   50,  123,  229,  227,  736,  734,  225,  732,
 /*   490 */   223,  730,  221,  153,   89,   34,  218,   63,  259,  260,
 /*   500 */    64,  888,  261,  262,  263,  264,  265,  276,  708,  176,
 /*   510 */   205,  206,  707,  208,  236,  237,  209,  177,  172,  706,
 /*   520 */    74,  694,  212,  216,  609,  746,   60,  106,  233,    6,
 /*   530 */   120,  141,  140,  784,  139,  142,  143,  740,  144,  145,
 /*   540 */   107,  739,    2,  108,  841,  731,   65,  626,    1,  129,
 /*   550 */   126,  124,  125,  183,  127,  128,  130,  222,    7,  631,
 /*   560 */   121,   23,   24,  672,    8,    5,  674,    9,   19,  235,
 /*   570 */    72,  546,  542,   70,  540,  539,  538,  535,  509,  245,
 /*   580 */    79,   27,   75,  580,   51,   83,   85,  579,  576,  530,
 /*   590 */   528,  520,  526,  522,  524,  518,  516,  548,  547,  545,
 /*   600 */   544,  543,  541,  537,  536,   52,  507,  477,  475,  712,
 /*   610 */   711,  711,  711,  711,  711,  711,  711,  711,  711,  711,
 /*   620 */   711,  110,  111,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   267,    1,  267,  209,  210,  251,    1,    5,  229,    9,
 /*    10 */   277,  276,  277,   13,   14,  267,   16,   17,  212,  265,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  212,
 /*    30 */     9,    9,  253,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,   37,   16,   17,  251,  253,   20,   21,  211,
 /*    50 */   212,   24,   25,   26,   27,   28,  250,  229,  252,  265,
 /*    60 */    33,   34,  267,  217,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    66,  253,   60,   13,   14,  268,   16,   17,   77,   79,
 /*    90 */    20,   21,  267,  247,   24,   25,   26,   27,   28,  212,
 /*   100 */   212,  276,  277,   33,   34,  212,  106,   37,   38,   39,
 /*   110 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   120 */    96,   97,   98,   99,  100,   37,   38,   39,  228,  102,
 /*   130 */   230,  231,  232,  233,  234,  235,  236,  237,  238,  239,
 /*   140 */   240,  241,  242,  243,   14,  131,   16,   17,  134,  135,
 /*   150 */    20,   21,    0,  229,   24,   25,   26,   27,   28,  271,
 /*   160 */   273,  273,  275,   33,   34,  101,  273,   37,   38,   39,
 /*   170 */   212,   16,   17,  109,  212,   20,   21,  253,  130,   24,
 /*   180 */    25,   26,   27,   28,  212,  137,  138,  267,   33,   34,
 /*   190 */    66,  253,   37,   38,   39,    1,    2,  212,  267,    5,
 /*   200 */   217,    7,  212,    9,  212,    1,    2,   59,  250,    5,
 /*   210 */   252,    7,  250,    9,  252,    1,    2,   44,  101,    5,
 /*   220 */    74,    7,  250,    9,  252,   79,  109,   33,   34,   77,
 /*   230 */   212,   37,  249,   60,   63,   64,   65,   33,   34,   66,
 /*   240 */   250,   37,  252,  212,   71,   72,   73,   33,   34,  101,
 /*   250 */    86,  212,   88,   89,  269,  131,  212,   93,  134,   95,
 /*   260 */    96,   97,  101,   99,  100,  273,  105,  106,  250,  228,
 /*   270 */   252,  102,  231,  232,  101,  244,  107,  236,  245,  238,
 /*   280 */   239,  240,  109,  242,  243,   25,   26,   27,   28,  250,
 /*   290 */     5,  252,    7,   33,   34,  101,  252,   37,   38,   39,
 /*   300 */   216,   15,  129,  219,  217,  101,  245,  113,  216,  136,
 /*   310 */   216,  219,  254,  219,    5,  101,    7,  113,    2,  119,
 /*   320 */   120,    5,  128,    7,  266,    9,  107,  113,    5,  102,
 /*   330 */     7,  112,  128,  246,  247,  248,  249,  110,  102,   61,
 /*   340 */    62,  267,  128,  107,  267,   67,   68,   69,   70,   33,
 /*   350 */    34,  245,  102,  102,   76,   61,   62,  107,  107,   61,
 /*   360 */    62,   67,   68,   69,   70,   67,   68,   69,   70,   33,
 /*   370 */    34,  102,  102,   37,   38,   39,  107,  107,  102,  107,
 /*   380 */   107,  267,  102,  107,  102,  132,  133,  107,  267,  107,
 /*   390 */   132,  133,   74,   75,  108,  132,  133,  124,  126,  267,
 /*   400 */   101,  267,  103,  132,  133,  267,    5,  267,    7,   61,
 /*   410 */    62,  267,  267,  245,  267,  245,  267,  267,  267,  245,
 /*   420 */   245,  212,  212,  251,  108,  212,  109,  212,  212,  212,
 /*   430 */   274,  274,  255,  212,  212,  212,  212,  212,  212,  251,
 /*   440 */   212,  212,  212,  212,  212,  212,  212,  212,   59,  212,
 /*   450 */   212,  212,  212,  212,  212,  212,  212,  212,  212,  212,
 /*   460 */   212,  212,  113,  270,  212,  270,  212,  212,  212,  212,
 /*   470 */   123,  212,  270,  212,  212,  264,  212,  212,  212,  212,
 /*   480 */   212,  125,  122,  263,  117,  121,  212,  212,  116,  212,
 /*   490 */   115,  212,  114,  212,   85,  127,  213,  213,   84,   49,
 /*   500 */   213,  213,   81,   83,   53,   82,   80,   77,    5,  213,
 /*   510 */   139,    5,    5,  139,  213,  213,    5,  213,  213,    5,
 /*   520 */   217,   87,  130,  110,  102,  213,  111,  214,  104,  101,
 /*   530 */   101,  221,  225,  227,  226,  224,  222,  213,  223,  220,
 /*   540 */   214,  213,  215,  214,  251,  213,  107,  102,  218,  257,
 /*   550 */   260,  262,  261,    1,  259,  258,  256,  101,  118,  102,
 /*   560 */   101,  107,  107,  102,  118,  101,  108,  101,  101,  104,
 /*   570 */    74,    9,    5,  105,    5,    5,    5,    5,   78,   15,
 /*   580 */   133,  107,   74,    5,   16,  133,  133,    5,  102,    5,
 /*   590 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   600 */     5,    5,    5,    5,    5,  107,   78,   59,   58,    0,
 /*   610 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   620 */   278,   21,   21,  278,  278,  278,  278,  278,  278,  278,
 /*   630 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   640 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   650 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   660 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   670 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   680 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   690 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   700 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   710 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   720 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   730 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   740 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   750 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   760 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   770 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   780 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   790 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   800 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   810 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   820 */   278,  278,  278,  278,  278,  278,  278,  278,  278,  278,
 /*   830 */   278,  278,
};
#define YY_SHIFT_COUNT    (281)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (609)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   173,   24,  164,   11,  194,  214,   21,   21,   21,   21,
 /*    10 */    21,   21,   21,   21,   21,    0,   22,  214,  316,  316,
 /*    20 */   316,  117,   21,   21,   21,  152,   21,   21,  146,   11,
 /*    30 */    10,   10,  623,  204,  214,  214,  214,  214,  214,  214,
 /*    40 */   214,  214,  214,  214,  214,  214,  214,  214,  214,  214,
 /*    50 */   214,  316,  316,    2,    2,    2,    2,    2,    2,    2,
 /*    60 */    64,   21,   21,   21,   21,   21,  200,  200,  219,   21,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,  317,  389,  389,  389,  349,
 /*   120 */   349,  349,  389,  347,  356,  360,  367,  364,  372,  375,
 /*   130 */   378,  368,  317,  389,  389,  389,   11,  389,  389,  409,
 /*   140 */   414,  450,  421,  420,  451,  423,  426,  389,  430,  389,
 /*   150 */   430,  389,  430,  389,  623,  623,   27,   70,   70,   70,
 /*   160 */   130,  155,  260,  260,  260,  278,  294,  298,  336,  336,
 /*   170 */   336,  336,   14,   48,   88,   88,  161,  124,  171,  227,
 /*   180 */   169,  236,  251,  269,  270,  276,  285,  309,    5,  148,
 /*   190 */   286,  272,  273,  250,  280,  282,  253,  258,  263,  299,
 /*   200 */   271,  323,  401,  318,  348,  503,  371,  506,  507,  374,
 /*   210 */   511,  514,  434,  392,  413,  422,  415,  424,  428,  439,
 /*   220 */   445,  429,  552,  456,  457,  459,  454,  440,  455,  446,
 /*   230 */   461,  464,  458,  466,  424,  467,  465,  468,  496,  562,
 /*   240 */   567,  569,  570,  571,  572,  500,  564,  508,  447,  474,
 /*   250 */   474,  568,  452,  453,  474,  578,  582,  486,  474,  584,
 /*   260 */   585,  586,  587,  588,  589,  590,  591,  592,  593,  594,
 /*   270 */   595,  596,  597,  598,  599,  498,  528,  600,  601,  548,
 /*   280 */   550,  609,
};
#define YY_REDUCE_COUNT (155)
#define YY_REDUCE_MIN   (-267)
#define YY_REDUCE_MAX   (332)
static const short yy_reduce_ofst[] = {
 /*     0 */  -206, -100,   41,   87, -265, -175, -194, -113, -112,  -42,
 /*    10 */   -38,  -28,  -10,   18,   39, -183, -162, -267, -221, -172,
 /*    20 */   -76, -246,  -15, -107,   -8,  -17,   31,   44,   84, -154,
 /*    30 */    92,   94,   58, -252, -205,  -80,  -69,   74,   77,  114,
 /*    40 */   121,  132,  134,  138,  140,  144,  145,  147,  149,  150,
 /*    50 */   151, -207,  -62,   33,   61,  106,  168,  170,  174,  175,
 /*    60 */   172,  209,  210,  213,  215,  216,  156,  157,  177,  217,
 /*    70 */   221,  222,  223,  224,  225,  226,  228,  229,  230,  231,
 /*    80 */   232,  233,  234,  235,  237,  238,  239,  240,  241,  242,
 /*    90 */   243,  244,  245,  246,  247,  248,  249,  252,  254,  255,
 /*   100 */   256,  257,  259,  261,  262,  264,  265,  266,  267,  268,
 /*   110 */   274,  275,  277,  279,  281,  188,  283,  284,  287,  193,
 /*   120 */   195,  202,  288,  211,  220,  289,  291,  290,  295,  297,
 /*   130 */   292,  300,  293,  296,  301,  302,  303,  304,  305,  306,
 /*   140 */   308,  307,  310,  311,  314,  315,  319,  312,  313,  324,
 /*   150 */   326,  328,  329,  332,  330,  327,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   709,  764,  753,  761,  944,  944,  709,  709,  709,  709,
 /*    10 */   709,  709,  709,  709,  709,  869,  727,  944,  709,  709,
 /*    20 */   709,  709,  709,  709,  709,  761,  709,  709,  766,  761,
 /*    30 */   766,  766,  864,  709,  709,  709,  709,  709,  709,  709,
 /*    40 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  709,
 /*    50 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  709,
 /*    60 */   709,  709,  709,  871,  873,  709,  891,  891,  862,  709,
 /*    70 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  709,
 /*    80 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  709,
 /*    90 */   709,  709,  709,  751,  709,  749,  709,  709,  709,  709,
 /*   100 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  737,
 /*   110 */   709,  709,  709,  709,  709,  709,  729,  729,  729,  709,
 /*   120 */   709,  709,  729,  898,  902,  896,  884,  892,  883,  879,
 /*   130 */   878,  906,  709,  729,  729,  729,  761,  729,  729,  782,
 /*   140 */   780,  778,  770,  776,  772,  774,  768,  729,  759,  729,
 /*   150 */   759,  729,  759,  729,  800,  816,  709,  907,  943,  897,
 /*   160 */   933,  932,  939,  931,  930,  709,  709,  709,  926,  927,
 /*   170 */   929,  928,  709,  709,  935,  934,  709,  709,  709,  709,
 /*   180 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  909,
 /*   190 */   709,  903,  899,  709,  709,  709,  709,  709,  709,  826,
 /*   200 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  709,
 /*   210 */   709,  709,  709,  709,  861,  709,  709,  709,  709,  870,
 /*   220 */   709,  709,  709,  709,  709,  709,  893,  709,  885,  709,
 /*   230 */   709,  709,  709,  709,  838,  709,  709,  709,  709,  709,
 /*   240 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  954,
 /*   250 */   952,  709,  709,  709,  948,  709,  709,  709,  946,  709,
 /*   260 */   709,  709,  709,  709,  709,  709,  709,  709,  709,  709,
 /*   270 */   709,  709,  709,  709,  709,  785,  709,  735,  733,  709,
 /*   280 */   725,  709,
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
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    1,  /*     STABLE => ID */
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
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*         AS => nothing */
    0,  /*      COMMA => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
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
    1,  /*      COUNT => ID */
    1,  /*        SUM => ID */
    1,  /*        AVG => ID */
    1,  /*        MIN => ID */
    1,  /*        MAX => ID */
    1,  /*      FIRST => ID */
    1,  /*       LAST => ID */
    1,  /*        TOP => ID */
    1,  /*     BOTTOM => ID */
    1,  /*     STDDEV => ID */
    1,  /* PERCENTILE => ID */
    1,  /* APERCENTILE => ID */
    1,  /* LEASTSQUARES => ID */
    1,  /*  HISTOGRAM => ID */
    1,  /*       DIFF => ID */
    1,  /*     SPREAD => ID */
    1,  /*        TWA => ID */
    1,  /*     INTERP => ID */
    1,  /*   LAST_ROW => ID */
    1,  /*       RATE => ID */
    1,  /*      IRATE => ID */
    1,  /*   SUM_RATE => ID */
    1,  /*  SUM_IRATE => ID */
    1,  /*   AVG_RATE => ID */
    1,  /*  AVG_IRATE => ID */
    1,  /*       TBID => ID */
    1,  /*       SEMI => ID */
    1,  /*       NONE => ID */
    1,  /*       PREV => ID */
    1,  /*     LINEAR => ID */
    1,  /*     IMPORT => ID */
    1,  /*     METRIC => ID */
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
    1,  /*    METRICS => ID */
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
  /*   46 */ "MNODES",
  /*   47 */ "DNODES",
  /*   48 */ "ACCOUNTS",
  /*   49 */ "USERS",
  /*   50 */ "MODULES",
  /*   51 */ "QUERIES",
  /*   52 */ "CONNECTIONS",
  /*   53 */ "STREAMS",
  /*   54 */ "VARIABLES",
  /*   55 */ "SCORES",
  /*   56 */ "GRANTS",
  /*   57 */ "VNODES",
  /*   58 */ "IPTOKEN",
  /*   59 */ "DOT",
  /*   60 */ "CREATE",
  /*   61 */ "TABLE",
  /*   62 */ "DATABASE",
  /*   63 */ "TABLES",
  /*   64 */ "STABLES",
  /*   65 */ "VGROUPS",
  /*   66 */ "DROP",
  /*   67 */ "STABLE",
  /*   68 */ "DNODE",
  /*   69 */ "USER",
  /*   70 */ "ACCOUNT",
  /*   71 */ "USE",
  /*   72 */ "DESCRIBE",
  /*   73 */ "ALTER",
  /*   74 */ "PASS",
  /*   75 */ "PRIVILEGE",
  /*   76 */ "LOCAL",
  /*   77 */ "IF",
  /*   78 */ "EXISTS",
  /*   79 */ "PPS",
  /*   80 */ "TSERIES",
  /*   81 */ "DBS",
  /*   82 */ "STORAGE",
  /*   83 */ "QTIME",
  /*   84 */ "CONNS",
  /*   85 */ "STATE",
  /*   86 */ "KEEP",
  /*   87 */ "CACHE",
  /*   88 */ "REPLICA",
  /*   89 */ "QUORUM",
  /*   90 */ "DAYS",
  /*   91 */ "MINROWS",
  /*   92 */ "MAXROWS",
  /*   93 */ "BLOCKS",
  /*   94 */ "CTIME",
  /*   95 */ "WAL",
  /*   96 */ "FSYNC",
  /*   97 */ "COMP",
  /*   98 */ "PRECISION",
  /*   99 */ "UPDATE",
  /*  100 */ "CACHELAST",
  /*  101 */ "LP",
  /*  102 */ "RP",
  /*  103 */ "UNSIGNED",
  /*  104 */ "TAGS",
  /*  105 */ "USING",
  /*  106 */ "AS",
  /*  107 */ "COMMA",
  /*  108 */ "NULL",
  /*  109 */ "SELECT",
  /*  110 */ "UNION",
  /*  111 */ "ALL",
  /*  112 */ "FROM",
  /*  113 */ "VARIABLE",
  /*  114 */ "INTERVAL",
  /*  115 */ "FILL",
  /*  116 */ "SLIDING",
  /*  117 */ "ORDER",
  /*  118 */ "BY",
  /*  119 */ "ASC",
  /*  120 */ "DESC",
  /*  121 */ "GROUP",
  /*  122 */ "HAVING",
  /*  123 */ "LIMIT",
  /*  124 */ "OFFSET",
  /*  125 */ "SLIMIT",
  /*  126 */ "SOFFSET",
  /*  127 */ "WHERE",
  /*  128 */ "NOW",
  /*  129 */ "RESET",
  /*  130 */ "QUERY",
  /*  131 */ "ADD",
  /*  132 */ "COLUMN",
  /*  133 */ "TAG",
  /*  134 */ "CHANGE",
  /*  135 */ "SET",
  /*  136 */ "KILL",
  /*  137 */ "CONNECTION",
  /*  138 */ "STREAM",
  /*  139 */ "COLON",
  /*  140 */ "ABORT",
  /*  141 */ "AFTER",
  /*  142 */ "ATTACH",
  /*  143 */ "BEFORE",
  /*  144 */ "BEGIN",
  /*  145 */ "CASCADE",
  /*  146 */ "CLUSTER",
  /*  147 */ "CONFLICT",
  /*  148 */ "COPY",
  /*  149 */ "DEFERRED",
  /*  150 */ "DELIMITERS",
  /*  151 */ "DETACH",
  /*  152 */ "EACH",
  /*  153 */ "END",
  /*  154 */ "EXPLAIN",
  /*  155 */ "FAIL",
  /*  156 */ "FOR",
  /*  157 */ "IGNORE",
  /*  158 */ "IMMEDIATE",
  /*  159 */ "INITIALLY",
  /*  160 */ "INSTEAD",
  /*  161 */ "MATCH",
  /*  162 */ "KEY",
  /*  163 */ "OF",
  /*  164 */ "RAISE",
  /*  165 */ "REPLACE",
  /*  166 */ "RESTRICT",
  /*  167 */ "ROW",
  /*  168 */ "STATEMENT",
  /*  169 */ "TRIGGER",
  /*  170 */ "VIEW",
  /*  171 */ "COUNT",
  /*  172 */ "SUM",
  /*  173 */ "AVG",
  /*  174 */ "MIN",
  /*  175 */ "MAX",
  /*  176 */ "FIRST",
  /*  177 */ "LAST",
  /*  178 */ "TOP",
  /*  179 */ "BOTTOM",
  /*  180 */ "STDDEV",
  /*  181 */ "PERCENTILE",
  /*  182 */ "APERCENTILE",
  /*  183 */ "LEASTSQUARES",
  /*  184 */ "HISTOGRAM",
  /*  185 */ "DIFF",
  /*  186 */ "SPREAD",
  /*  187 */ "TWA",
  /*  188 */ "INTERP",
  /*  189 */ "LAST_ROW",
  /*  190 */ "RATE",
  /*  191 */ "IRATE",
  /*  192 */ "SUM_RATE",
  /*  193 */ "SUM_IRATE",
  /*  194 */ "AVG_RATE",
  /*  195 */ "AVG_IRATE",
  /*  196 */ "TBID",
  /*  197 */ "SEMI",
  /*  198 */ "NONE",
  /*  199 */ "PREV",
  /*  200 */ "LINEAR",
  /*  201 */ "IMPORT",
  /*  202 */ "METRIC",
  /*  203 */ "TBNAME",
  /*  204 */ "JOIN",
  /*  205 */ "METRICS",
  /*  206 */ "INSERT",
  /*  207 */ "INTO",
  /*  208 */ "VALUES",
  /*  209 */ "program",
  /*  210 */ "cmd",
  /*  211 */ "dbPrefix",
  /*  212 */ "ids",
  /*  213 */ "cpxName",
  /*  214 */ "ifexists",
  /*  215 */ "alter_db_optr",
  /*  216 */ "acct_optr",
  /*  217 */ "ifnotexists",
  /*  218 */ "db_optr",
  /*  219 */ "pps",
  /*  220 */ "tseries",
  /*  221 */ "dbs",
  /*  222 */ "streams",
  /*  223 */ "storage",
  /*  224 */ "qtime",
  /*  225 */ "users",
  /*  226 */ "conns",
  /*  227 */ "state",
  /*  228 */ "keep",
  /*  229 */ "tagitemlist",
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
  /*  244 */ "typename",
  /*  245 */ "signed",
  /*  246 */ "create_table_args",
  /*  247 */ "create_stable_args",
  /*  248 */ "create_table_list",
  /*  249 */ "create_from_stable",
  /*  250 */ "columnlist",
  /*  251 */ "select",
  /*  252 */ "column",
  /*  253 */ "tagitem",
  /*  254 */ "selcollist",
  /*  255 */ "from",
  /*  256 */ "where_opt",
  /*  257 */ "interval_opt",
  /*  258 */ "fill_opt",
  /*  259 */ "sliding_opt",
  /*  260 */ "groupby_opt",
  /*  261 */ "orderby_opt",
  /*  262 */ "having_opt",
  /*  263 */ "slimit_opt",
  /*  264 */ "limit_opt",
  /*  265 */ "union",
  /*  266 */ "sclp",
  /*  267 */ "expr",
  /*  268 */ "as",
  /*  269 */ "tablelist",
  /*  270 */ "tmvar",
  /*  271 */ "sortlist",
  /*  272 */ "sortitem",
  /*  273 */ "item",
  /*  274 */ "sortorder",
  /*  275 */ "grouplist",
  /*  276 */ "exprlist",
  /*  277 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW MNODES",
 /*   3 */ "cmd ::= SHOW DNODES",
 /*   4 */ "cmd ::= SHOW ACCOUNTS",
 /*   5 */ "cmd ::= SHOW USERS",
 /*   6 */ "cmd ::= SHOW MODULES",
 /*   7 */ "cmd ::= SHOW QUERIES",
 /*   8 */ "cmd ::= SHOW CONNECTIONS",
 /*   9 */ "cmd ::= SHOW STREAMS",
 /*  10 */ "cmd ::= SHOW VARIABLES",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "cmd ::= SHOW VNODES",
 /*  14 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  15 */ "dbPrefix ::=",
 /*  16 */ "dbPrefix ::= ids DOT",
 /*  17 */ "cpxName ::=",
 /*  18 */ "cpxName ::= DOT ids",
 /*  19 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  20 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  27 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  28 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  29 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  30 */ "cmd ::= DROP DNODE ids",
 /*  31 */ "cmd ::= DROP USER ids",
 /*  32 */ "cmd ::= DROP ACCOUNT ids",
 /*  33 */ "cmd ::= USE ids",
 /*  34 */ "cmd ::= DESCRIBE ids cpxName",
 /*  35 */ "cmd ::= ALTER USER ids PASS ids",
 /*  36 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  37 */ "cmd ::= ALTER DNODE ids ids",
 /*  38 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  39 */ "cmd ::= ALTER LOCAL ids",
 /*  40 */ "cmd ::= ALTER LOCAL ids ids",
 /*  41 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  42 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  43 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  44 */ "ids ::= ID",
 /*  45 */ "ids ::= STRING",
 /*  46 */ "ifexists ::= IF EXISTS",
 /*  47 */ "ifexists ::=",
 /*  48 */ "ifnotexists ::= IF NOT EXISTS",
 /*  49 */ "ifnotexists ::=",
 /*  50 */ "cmd ::= CREATE DNODE ids",
 /*  51 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  52 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  53 */ "cmd ::= CREATE USER ids PASS ids",
 /*  54 */ "pps ::=",
 /*  55 */ "pps ::= PPS INTEGER",
 /*  56 */ "tseries ::=",
 /*  57 */ "tseries ::= TSERIES INTEGER",
 /*  58 */ "dbs ::=",
 /*  59 */ "dbs ::= DBS INTEGER",
 /*  60 */ "streams ::=",
 /*  61 */ "streams ::= STREAMS INTEGER",
 /*  62 */ "storage ::=",
 /*  63 */ "storage ::= STORAGE INTEGER",
 /*  64 */ "qtime ::=",
 /*  65 */ "qtime ::= QTIME INTEGER",
 /*  66 */ "users ::=",
 /*  67 */ "users ::= USERS INTEGER",
 /*  68 */ "conns ::=",
 /*  69 */ "conns ::= CONNS INTEGER",
 /*  70 */ "state ::=",
 /*  71 */ "state ::= STATE ids",
 /*  72 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  73 */ "keep ::= KEEP tagitemlist",
 /*  74 */ "cache ::= CACHE INTEGER",
 /*  75 */ "replica ::= REPLICA INTEGER",
 /*  76 */ "quorum ::= QUORUM INTEGER",
 /*  77 */ "days ::= DAYS INTEGER",
 /*  78 */ "minrows ::= MINROWS INTEGER",
 /*  79 */ "maxrows ::= MAXROWS INTEGER",
 /*  80 */ "blocks ::= BLOCKS INTEGER",
 /*  81 */ "ctime ::= CTIME INTEGER",
 /*  82 */ "wal ::= WAL INTEGER",
 /*  83 */ "fsync ::= FSYNC INTEGER",
 /*  84 */ "comp ::= COMP INTEGER",
 /*  85 */ "prec ::= PRECISION STRING",
 /*  86 */ "update ::= UPDATE INTEGER",
 /*  87 */ "cachelast ::= CACHELAST INTEGER",
 /*  88 */ "db_optr ::=",
 /*  89 */ "db_optr ::= db_optr cache",
 /*  90 */ "db_optr ::= db_optr replica",
 /*  91 */ "db_optr ::= db_optr quorum",
 /*  92 */ "db_optr ::= db_optr days",
 /*  93 */ "db_optr ::= db_optr minrows",
 /*  94 */ "db_optr ::= db_optr maxrows",
 /*  95 */ "db_optr ::= db_optr blocks",
 /*  96 */ "db_optr ::= db_optr ctime",
 /*  97 */ "db_optr ::= db_optr wal",
 /*  98 */ "db_optr ::= db_optr fsync",
 /*  99 */ "db_optr ::= db_optr comp",
 /* 100 */ "db_optr ::= db_optr prec",
 /* 101 */ "db_optr ::= db_optr keep",
 /* 102 */ "db_optr ::= db_optr update",
 /* 103 */ "db_optr ::= db_optr cachelast",
 /* 104 */ "alter_db_optr ::=",
 /* 105 */ "alter_db_optr ::= alter_db_optr replica",
 /* 106 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 107 */ "alter_db_optr ::= alter_db_optr keep",
 /* 108 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 109 */ "alter_db_optr ::= alter_db_optr comp",
 /* 110 */ "alter_db_optr ::= alter_db_optr wal",
 /* 111 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 112 */ "alter_db_optr ::= alter_db_optr update",
 /* 113 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 114 */ "typename ::= ids",
 /* 115 */ "typename ::= ids LP signed RP",
 /* 116 */ "typename ::= ids UNSIGNED",
 /* 117 */ "signed ::= INTEGER",
 /* 118 */ "signed ::= PLUS INTEGER",
 /* 119 */ "signed ::= MINUS INTEGER",
 /* 120 */ "cmd ::= CREATE TABLE create_table_args",
 /* 121 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 122 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 123 */ "cmd ::= CREATE TABLE create_table_list",
 /* 124 */ "create_table_list ::= create_from_stable",
 /* 125 */ "create_table_list ::= create_table_list create_from_stable",
 /* 126 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 127 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 128 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 129 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 130 */ "columnlist ::= columnlist COMMA column",
 /* 131 */ "columnlist ::= column",
 /* 132 */ "column ::= ids typename",
 /* 133 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 134 */ "tagitemlist ::= tagitem",
 /* 135 */ "tagitem ::= INTEGER",
 /* 136 */ "tagitem ::= FLOAT",
 /* 137 */ "tagitem ::= STRING",
 /* 138 */ "tagitem ::= BOOL",
 /* 139 */ "tagitem ::= NULL",
 /* 140 */ "tagitem ::= MINUS INTEGER",
 /* 141 */ "tagitem ::= MINUS FLOAT",
 /* 142 */ "tagitem ::= PLUS INTEGER",
 /* 143 */ "tagitem ::= PLUS FLOAT",
 /* 144 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 145 */ "union ::= select",
 /* 146 */ "union ::= LP union RP",
 /* 147 */ "union ::= union UNION ALL select",
 /* 148 */ "union ::= union UNION ALL LP select RP",
 /* 149 */ "cmd ::= union",
 /* 150 */ "select ::= SELECT selcollist",
 /* 151 */ "sclp ::= selcollist COMMA",
 /* 152 */ "sclp ::=",
 /* 153 */ "selcollist ::= sclp expr as",
 /* 154 */ "selcollist ::= sclp STAR",
 /* 155 */ "as ::= AS ids",
 /* 156 */ "as ::= ids",
 /* 157 */ "as ::=",
 /* 158 */ "from ::= FROM tablelist",
 /* 159 */ "tablelist ::= ids cpxName",
 /* 160 */ "tablelist ::= ids cpxName ids",
 /* 161 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 162 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 163 */ "tmvar ::= VARIABLE",
 /* 164 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 165 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 166 */ "interval_opt ::=",
 /* 167 */ "fill_opt ::=",
 /* 168 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 169 */ "fill_opt ::= FILL LP ID RP",
 /* 170 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 171 */ "sliding_opt ::=",
 /* 172 */ "orderby_opt ::=",
 /* 173 */ "orderby_opt ::= ORDER BY sortlist",
 /* 174 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 175 */ "sortlist ::= item sortorder",
 /* 176 */ "item ::= ids cpxName",
 /* 177 */ "sortorder ::= ASC",
 /* 178 */ "sortorder ::= DESC",
 /* 179 */ "sortorder ::=",
 /* 180 */ "groupby_opt ::=",
 /* 181 */ "groupby_opt ::= GROUP BY grouplist",
 /* 182 */ "grouplist ::= grouplist COMMA item",
 /* 183 */ "grouplist ::= item",
 /* 184 */ "having_opt ::=",
 /* 185 */ "having_opt ::= HAVING expr",
 /* 186 */ "limit_opt ::=",
 /* 187 */ "limit_opt ::= LIMIT signed",
 /* 188 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 189 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 190 */ "slimit_opt ::=",
 /* 191 */ "slimit_opt ::= SLIMIT signed",
 /* 192 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 193 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 194 */ "where_opt ::=",
 /* 195 */ "where_opt ::= WHERE expr",
 /* 196 */ "expr ::= LP expr RP",
 /* 197 */ "expr ::= ID",
 /* 198 */ "expr ::= ID DOT ID",
 /* 199 */ "expr ::= ID DOT STAR",
 /* 200 */ "expr ::= INTEGER",
 /* 201 */ "expr ::= MINUS INTEGER",
 /* 202 */ "expr ::= PLUS INTEGER",
 /* 203 */ "expr ::= FLOAT",
 /* 204 */ "expr ::= MINUS FLOAT",
 /* 205 */ "expr ::= PLUS FLOAT",
 /* 206 */ "expr ::= STRING",
 /* 207 */ "expr ::= NOW",
 /* 208 */ "expr ::= VARIABLE",
 /* 209 */ "expr ::= BOOL",
 /* 210 */ "expr ::= ID LP exprlist RP",
 /* 211 */ "expr ::= ID LP STAR RP",
 /* 212 */ "expr ::= expr IS NULL",
 /* 213 */ "expr ::= expr IS NOT NULL",
 /* 214 */ "expr ::= expr LT expr",
 /* 215 */ "expr ::= expr GT expr",
 /* 216 */ "expr ::= expr LE expr",
 /* 217 */ "expr ::= expr GE expr",
 /* 218 */ "expr ::= expr NE expr",
 /* 219 */ "expr ::= expr EQ expr",
 /* 220 */ "expr ::= expr AND expr",
 /* 221 */ "expr ::= expr OR expr",
 /* 222 */ "expr ::= expr PLUS expr",
 /* 223 */ "expr ::= expr MINUS expr",
 /* 224 */ "expr ::= expr STAR expr",
 /* 225 */ "expr ::= expr SLASH expr",
 /* 226 */ "expr ::= expr REM expr",
 /* 227 */ "expr ::= expr LIKE expr",
 /* 228 */ "expr ::= expr IN LP exprlist RP",
 /* 229 */ "exprlist ::= exprlist COMMA expritem",
 /* 230 */ "exprlist ::= expritem",
 /* 231 */ "expritem ::= expr",
 /* 232 */ "expritem ::=",
 /* 233 */ "cmd ::= RESET QUERY CACHE",
 /* 234 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 235 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 236 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 237 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 238 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 239 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 240 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 241 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 242 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 243 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 244 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 245 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 246 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 247 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 228: /* keep */
    case 229: /* tagitemlist */
    case 250: /* columnlist */
    case 258: /* fill_opt */
    case 260: /* groupby_opt */
    case 261: /* orderby_opt */
    case 271: /* sortlist */
    case 275: /* grouplist */
{
taosArrayDestroy((yypminor->yy421));
}
      break;
    case 248: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy38));
}
      break;
    case 251: /* select */
{
doDestroyQuerySql((yypminor->yy148));
}
      break;
    case 254: /* selcollist */
    case 266: /* sclp */
    case 276: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy166));
}
      break;
    case 256: /* where_opt */
    case 262: /* having_opt */
    case 267: /* expr */
    case 277: /* expritem */
{
tSqlExprDestroy((yypminor->yy78));
}
      break;
    case 265: /* union */
{
destroyAllSelectClause((yypminor->yy153));
}
      break;
    case 272: /* sortitem */
{
tVariantDestroy(&(yypminor->yy430));
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
   209,  /* (0) program ::= cmd */
   210,  /* (1) cmd ::= SHOW DATABASES */
   210,  /* (2) cmd ::= SHOW MNODES */
   210,  /* (3) cmd ::= SHOW DNODES */
   210,  /* (4) cmd ::= SHOW ACCOUNTS */
   210,  /* (5) cmd ::= SHOW USERS */
   210,  /* (6) cmd ::= SHOW MODULES */
   210,  /* (7) cmd ::= SHOW QUERIES */
   210,  /* (8) cmd ::= SHOW CONNECTIONS */
   210,  /* (9) cmd ::= SHOW STREAMS */
   210,  /* (10) cmd ::= SHOW VARIABLES */
   210,  /* (11) cmd ::= SHOW SCORES */
   210,  /* (12) cmd ::= SHOW GRANTS */
   210,  /* (13) cmd ::= SHOW VNODES */
   210,  /* (14) cmd ::= SHOW VNODES IPTOKEN */
   211,  /* (15) dbPrefix ::= */
   211,  /* (16) dbPrefix ::= ids DOT */
   213,  /* (17) cpxName ::= */
   213,  /* (18) cpxName ::= DOT ids */
   210,  /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
   210,  /* (20) cmd ::= SHOW CREATE DATABASE ids */
   210,  /* (21) cmd ::= SHOW dbPrefix TABLES */
   210,  /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   210,  /* (23) cmd ::= SHOW dbPrefix STABLES */
   210,  /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   210,  /* (25) cmd ::= SHOW dbPrefix VGROUPS */
   210,  /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
   210,  /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
   210,  /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
   210,  /* (29) cmd ::= DROP DATABASE ifexists ids */
   210,  /* (30) cmd ::= DROP DNODE ids */
   210,  /* (31) cmd ::= DROP USER ids */
   210,  /* (32) cmd ::= DROP ACCOUNT ids */
   210,  /* (33) cmd ::= USE ids */
   210,  /* (34) cmd ::= DESCRIBE ids cpxName */
   210,  /* (35) cmd ::= ALTER USER ids PASS ids */
   210,  /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
   210,  /* (37) cmd ::= ALTER DNODE ids ids */
   210,  /* (38) cmd ::= ALTER DNODE ids ids ids */
   210,  /* (39) cmd ::= ALTER LOCAL ids */
   210,  /* (40) cmd ::= ALTER LOCAL ids ids */
   210,  /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
   210,  /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
   210,  /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   212,  /* (44) ids ::= ID */
   212,  /* (45) ids ::= STRING */
   214,  /* (46) ifexists ::= IF EXISTS */
   214,  /* (47) ifexists ::= */
   217,  /* (48) ifnotexists ::= IF NOT EXISTS */
   217,  /* (49) ifnotexists ::= */
   210,  /* (50) cmd ::= CREATE DNODE ids */
   210,  /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   210,  /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   210,  /* (53) cmd ::= CREATE USER ids PASS ids */
   219,  /* (54) pps ::= */
   219,  /* (55) pps ::= PPS INTEGER */
   220,  /* (56) tseries ::= */
   220,  /* (57) tseries ::= TSERIES INTEGER */
   221,  /* (58) dbs ::= */
   221,  /* (59) dbs ::= DBS INTEGER */
   222,  /* (60) streams ::= */
   222,  /* (61) streams ::= STREAMS INTEGER */
   223,  /* (62) storage ::= */
   223,  /* (63) storage ::= STORAGE INTEGER */
   224,  /* (64) qtime ::= */
   224,  /* (65) qtime ::= QTIME INTEGER */
   225,  /* (66) users ::= */
   225,  /* (67) users ::= USERS INTEGER */
   226,  /* (68) conns ::= */
   226,  /* (69) conns ::= CONNS INTEGER */
   227,  /* (70) state ::= */
   227,  /* (71) state ::= STATE ids */
   216,  /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   228,  /* (73) keep ::= KEEP tagitemlist */
   230,  /* (74) cache ::= CACHE INTEGER */
   231,  /* (75) replica ::= REPLICA INTEGER */
   232,  /* (76) quorum ::= QUORUM INTEGER */
   233,  /* (77) days ::= DAYS INTEGER */
   234,  /* (78) minrows ::= MINROWS INTEGER */
   235,  /* (79) maxrows ::= MAXROWS INTEGER */
   236,  /* (80) blocks ::= BLOCKS INTEGER */
   237,  /* (81) ctime ::= CTIME INTEGER */
   238,  /* (82) wal ::= WAL INTEGER */
   239,  /* (83) fsync ::= FSYNC INTEGER */
   240,  /* (84) comp ::= COMP INTEGER */
   241,  /* (85) prec ::= PRECISION STRING */
   242,  /* (86) update ::= UPDATE INTEGER */
   243,  /* (87) cachelast ::= CACHELAST INTEGER */
   218,  /* (88) db_optr ::= */
   218,  /* (89) db_optr ::= db_optr cache */
   218,  /* (90) db_optr ::= db_optr replica */
   218,  /* (91) db_optr ::= db_optr quorum */
   218,  /* (92) db_optr ::= db_optr days */
   218,  /* (93) db_optr ::= db_optr minrows */
   218,  /* (94) db_optr ::= db_optr maxrows */
   218,  /* (95) db_optr ::= db_optr blocks */
   218,  /* (96) db_optr ::= db_optr ctime */
   218,  /* (97) db_optr ::= db_optr wal */
   218,  /* (98) db_optr ::= db_optr fsync */
   218,  /* (99) db_optr ::= db_optr comp */
   218,  /* (100) db_optr ::= db_optr prec */
   218,  /* (101) db_optr ::= db_optr keep */
   218,  /* (102) db_optr ::= db_optr update */
   218,  /* (103) db_optr ::= db_optr cachelast */
   215,  /* (104) alter_db_optr ::= */
   215,  /* (105) alter_db_optr ::= alter_db_optr replica */
   215,  /* (106) alter_db_optr ::= alter_db_optr quorum */
   215,  /* (107) alter_db_optr ::= alter_db_optr keep */
   215,  /* (108) alter_db_optr ::= alter_db_optr blocks */
   215,  /* (109) alter_db_optr ::= alter_db_optr comp */
   215,  /* (110) alter_db_optr ::= alter_db_optr wal */
   215,  /* (111) alter_db_optr ::= alter_db_optr fsync */
   215,  /* (112) alter_db_optr ::= alter_db_optr update */
   215,  /* (113) alter_db_optr ::= alter_db_optr cachelast */
   244,  /* (114) typename ::= ids */
   244,  /* (115) typename ::= ids LP signed RP */
   244,  /* (116) typename ::= ids UNSIGNED */
   245,  /* (117) signed ::= INTEGER */
   245,  /* (118) signed ::= PLUS INTEGER */
   245,  /* (119) signed ::= MINUS INTEGER */
   210,  /* (120) cmd ::= CREATE TABLE create_table_args */
   210,  /* (121) cmd ::= CREATE TABLE create_stable_args */
   210,  /* (122) cmd ::= CREATE STABLE create_stable_args */
   210,  /* (123) cmd ::= CREATE TABLE create_table_list */
   248,  /* (124) create_table_list ::= create_from_stable */
   248,  /* (125) create_table_list ::= create_table_list create_from_stable */
   246,  /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   247,  /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   249,  /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   246,  /* (129) create_table_args ::= ifnotexists ids cpxName AS select */
   250,  /* (130) columnlist ::= columnlist COMMA column */
   250,  /* (131) columnlist ::= column */
   252,  /* (132) column ::= ids typename */
   229,  /* (133) tagitemlist ::= tagitemlist COMMA tagitem */
   229,  /* (134) tagitemlist ::= tagitem */
   253,  /* (135) tagitem ::= INTEGER */
   253,  /* (136) tagitem ::= FLOAT */
   253,  /* (137) tagitem ::= STRING */
   253,  /* (138) tagitem ::= BOOL */
   253,  /* (139) tagitem ::= NULL */
   253,  /* (140) tagitem ::= MINUS INTEGER */
   253,  /* (141) tagitem ::= MINUS FLOAT */
   253,  /* (142) tagitem ::= PLUS INTEGER */
   253,  /* (143) tagitem ::= PLUS FLOAT */
   251,  /* (144) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   265,  /* (145) union ::= select */
   265,  /* (146) union ::= LP union RP */
   265,  /* (147) union ::= union UNION ALL select */
   265,  /* (148) union ::= union UNION ALL LP select RP */
   210,  /* (149) cmd ::= union */
   251,  /* (150) select ::= SELECT selcollist */
   266,  /* (151) sclp ::= selcollist COMMA */
   266,  /* (152) sclp ::= */
   254,  /* (153) selcollist ::= sclp expr as */
   254,  /* (154) selcollist ::= sclp STAR */
   268,  /* (155) as ::= AS ids */
   268,  /* (156) as ::= ids */
   268,  /* (157) as ::= */
   255,  /* (158) from ::= FROM tablelist */
   269,  /* (159) tablelist ::= ids cpxName */
   269,  /* (160) tablelist ::= ids cpxName ids */
   269,  /* (161) tablelist ::= tablelist COMMA ids cpxName */
   269,  /* (162) tablelist ::= tablelist COMMA ids cpxName ids */
   270,  /* (163) tmvar ::= VARIABLE */
   257,  /* (164) interval_opt ::= INTERVAL LP tmvar RP */
   257,  /* (165) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   257,  /* (166) interval_opt ::= */
   258,  /* (167) fill_opt ::= */
   258,  /* (168) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   258,  /* (169) fill_opt ::= FILL LP ID RP */
   259,  /* (170) sliding_opt ::= SLIDING LP tmvar RP */
   259,  /* (171) sliding_opt ::= */
   261,  /* (172) orderby_opt ::= */
   261,  /* (173) orderby_opt ::= ORDER BY sortlist */
   271,  /* (174) sortlist ::= sortlist COMMA item sortorder */
   271,  /* (175) sortlist ::= item sortorder */
   273,  /* (176) item ::= ids cpxName */
   274,  /* (177) sortorder ::= ASC */
   274,  /* (178) sortorder ::= DESC */
   274,  /* (179) sortorder ::= */
   260,  /* (180) groupby_opt ::= */
   260,  /* (181) groupby_opt ::= GROUP BY grouplist */
   275,  /* (182) grouplist ::= grouplist COMMA item */
   275,  /* (183) grouplist ::= item */
   262,  /* (184) having_opt ::= */
   262,  /* (185) having_opt ::= HAVING expr */
   264,  /* (186) limit_opt ::= */
   264,  /* (187) limit_opt ::= LIMIT signed */
   264,  /* (188) limit_opt ::= LIMIT signed OFFSET signed */
   264,  /* (189) limit_opt ::= LIMIT signed COMMA signed */
   263,  /* (190) slimit_opt ::= */
   263,  /* (191) slimit_opt ::= SLIMIT signed */
   263,  /* (192) slimit_opt ::= SLIMIT signed SOFFSET signed */
   263,  /* (193) slimit_opt ::= SLIMIT signed COMMA signed */
   256,  /* (194) where_opt ::= */
   256,  /* (195) where_opt ::= WHERE expr */
   267,  /* (196) expr ::= LP expr RP */
   267,  /* (197) expr ::= ID */
   267,  /* (198) expr ::= ID DOT ID */
   267,  /* (199) expr ::= ID DOT STAR */
   267,  /* (200) expr ::= INTEGER */
   267,  /* (201) expr ::= MINUS INTEGER */
   267,  /* (202) expr ::= PLUS INTEGER */
   267,  /* (203) expr ::= FLOAT */
   267,  /* (204) expr ::= MINUS FLOAT */
   267,  /* (205) expr ::= PLUS FLOAT */
   267,  /* (206) expr ::= STRING */
   267,  /* (207) expr ::= NOW */
   267,  /* (208) expr ::= VARIABLE */
   267,  /* (209) expr ::= BOOL */
   267,  /* (210) expr ::= ID LP exprlist RP */
   267,  /* (211) expr ::= ID LP STAR RP */
   267,  /* (212) expr ::= expr IS NULL */
   267,  /* (213) expr ::= expr IS NOT NULL */
   267,  /* (214) expr ::= expr LT expr */
   267,  /* (215) expr ::= expr GT expr */
   267,  /* (216) expr ::= expr LE expr */
   267,  /* (217) expr ::= expr GE expr */
   267,  /* (218) expr ::= expr NE expr */
   267,  /* (219) expr ::= expr EQ expr */
   267,  /* (220) expr ::= expr AND expr */
   267,  /* (221) expr ::= expr OR expr */
   267,  /* (222) expr ::= expr PLUS expr */
   267,  /* (223) expr ::= expr MINUS expr */
   267,  /* (224) expr ::= expr STAR expr */
   267,  /* (225) expr ::= expr SLASH expr */
   267,  /* (226) expr ::= expr REM expr */
   267,  /* (227) expr ::= expr LIKE expr */
   267,  /* (228) expr ::= expr IN LP exprlist RP */
   276,  /* (229) exprlist ::= exprlist COMMA expritem */
   276,  /* (230) exprlist ::= expritem */
   277,  /* (231) expritem ::= expr */
   277,  /* (232) expritem ::= */
   210,  /* (233) cmd ::= RESET QUERY CACHE */
   210,  /* (234) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   210,  /* (235) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   210,  /* (236) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   210,  /* (237) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   210,  /* (238) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   210,  /* (239) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   210,  /* (240) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   210,  /* (241) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   210,  /* (242) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   210,  /* (243) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   210,  /* (244) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   210,  /* (245) cmd ::= KILL CONNECTION INTEGER */
   210,  /* (246) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   210,  /* (247) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW MNODES */
   -2,  /* (3) cmd ::= SHOW DNODES */
   -2,  /* (4) cmd ::= SHOW ACCOUNTS */
   -2,  /* (5) cmd ::= SHOW USERS */
   -2,  /* (6) cmd ::= SHOW MODULES */
   -2,  /* (7) cmd ::= SHOW QUERIES */
   -2,  /* (8) cmd ::= SHOW CONNECTIONS */
   -2,  /* (9) cmd ::= SHOW STREAMS */
   -2,  /* (10) cmd ::= SHOW VARIABLES */
   -2,  /* (11) cmd ::= SHOW SCORES */
   -2,  /* (12) cmd ::= SHOW GRANTS */
   -2,  /* (13) cmd ::= SHOW VNODES */
   -3,  /* (14) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (15) dbPrefix ::= */
   -2,  /* (16) dbPrefix ::= ids DOT */
    0,  /* (17) cpxName ::= */
   -2,  /* (18) cpxName ::= DOT ids */
   -5,  /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
   -4,  /* (20) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (21) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (23) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (25) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (29) cmd ::= DROP DATABASE ifexists ids */
   -3,  /* (30) cmd ::= DROP DNODE ids */
   -3,  /* (31) cmd ::= DROP USER ids */
   -3,  /* (32) cmd ::= DROP ACCOUNT ids */
   -2,  /* (33) cmd ::= USE ids */
   -3,  /* (34) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (35) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (37) cmd ::= ALTER DNODE ids ids */
   -5,  /* (38) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (39) cmd ::= ALTER LOCAL ids */
   -4,  /* (40) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (44) ids ::= ID */
   -1,  /* (45) ids ::= STRING */
   -2,  /* (46) ifexists ::= IF EXISTS */
    0,  /* (47) ifexists ::= */
   -3,  /* (48) ifnotexists ::= IF NOT EXISTS */
    0,  /* (49) ifnotexists ::= */
   -3,  /* (50) cmd ::= CREATE DNODE ids */
   -6,  /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (53) cmd ::= CREATE USER ids PASS ids */
    0,  /* (54) pps ::= */
   -2,  /* (55) pps ::= PPS INTEGER */
    0,  /* (56) tseries ::= */
   -2,  /* (57) tseries ::= TSERIES INTEGER */
    0,  /* (58) dbs ::= */
   -2,  /* (59) dbs ::= DBS INTEGER */
    0,  /* (60) streams ::= */
   -2,  /* (61) streams ::= STREAMS INTEGER */
    0,  /* (62) storage ::= */
   -2,  /* (63) storage ::= STORAGE INTEGER */
    0,  /* (64) qtime ::= */
   -2,  /* (65) qtime ::= QTIME INTEGER */
    0,  /* (66) users ::= */
   -2,  /* (67) users ::= USERS INTEGER */
    0,  /* (68) conns ::= */
   -2,  /* (69) conns ::= CONNS INTEGER */
    0,  /* (70) state ::= */
   -2,  /* (71) state ::= STATE ids */
   -9,  /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (73) keep ::= KEEP tagitemlist */
   -2,  /* (74) cache ::= CACHE INTEGER */
   -2,  /* (75) replica ::= REPLICA INTEGER */
   -2,  /* (76) quorum ::= QUORUM INTEGER */
   -2,  /* (77) days ::= DAYS INTEGER */
   -2,  /* (78) minrows ::= MINROWS INTEGER */
   -2,  /* (79) maxrows ::= MAXROWS INTEGER */
   -2,  /* (80) blocks ::= BLOCKS INTEGER */
   -2,  /* (81) ctime ::= CTIME INTEGER */
   -2,  /* (82) wal ::= WAL INTEGER */
   -2,  /* (83) fsync ::= FSYNC INTEGER */
   -2,  /* (84) comp ::= COMP INTEGER */
   -2,  /* (85) prec ::= PRECISION STRING */
   -2,  /* (86) update ::= UPDATE INTEGER */
   -2,  /* (87) cachelast ::= CACHELAST INTEGER */
    0,  /* (88) db_optr ::= */
   -2,  /* (89) db_optr ::= db_optr cache */
   -2,  /* (90) db_optr ::= db_optr replica */
   -2,  /* (91) db_optr ::= db_optr quorum */
   -2,  /* (92) db_optr ::= db_optr days */
   -2,  /* (93) db_optr ::= db_optr minrows */
   -2,  /* (94) db_optr ::= db_optr maxrows */
   -2,  /* (95) db_optr ::= db_optr blocks */
   -2,  /* (96) db_optr ::= db_optr ctime */
   -2,  /* (97) db_optr ::= db_optr wal */
   -2,  /* (98) db_optr ::= db_optr fsync */
   -2,  /* (99) db_optr ::= db_optr comp */
   -2,  /* (100) db_optr ::= db_optr prec */
   -2,  /* (101) db_optr ::= db_optr keep */
   -2,  /* (102) db_optr ::= db_optr update */
   -2,  /* (103) db_optr ::= db_optr cachelast */
    0,  /* (104) alter_db_optr ::= */
   -2,  /* (105) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (106) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (107) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (108) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (109) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (110) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (111) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (112) alter_db_optr ::= alter_db_optr update */
   -2,  /* (113) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (114) typename ::= ids */
   -4,  /* (115) typename ::= ids LP signed RP */
   -2,  /* (116) typename ::= ids UNSIGNED */
   -1,  /* (117) signed ::= INTEGER */
   -2,  /* (118) signed ::= PLUS INTEGER */
   -2,  /* (119) signed ::= MINUS INTEGER */
   -3,  /* (120) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (121) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (122) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (123) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (124) create_table_list ::= create_from_stable */
   -2,  /* (125) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   -5,  /* (129) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (130) columnlist ::= columnlist COMMA column */
   -1,  /* (131) columnlist ::= column */
   -2,  /* (132) column ::= ids typename */
   -3,  /* (133) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (134) tagitemlist ::= tagitem */
   -1,  /* (135) tagitem ::= INTEGER */
   -1,  /* (136) tagitem ::= FLOAT */
   -1,  /* (137) tagitem ::= STRING */
   -1,  /* (138) tagitem ::= BOOL */
   -1,  /* (139) tagitem ::= NULL */
   -2,  /* (140) tagitem ::= MINUS INTEGER */
   -2,  /* (141) tagitem ::= MINUS FLOAT */
   -2,  /* (142) tagitem ::= PLUS INTEGER */
   -2,  /* (143) tagitem ::= PLUS FLOAT */
  -12,  /* (144) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -1,  /* (145) union ::= select */
   -3,  /* (146) union ::= LP union RP */
   -4,  /* (147) union ::= union UNION ALL select */
   -6,  /* (148) union ::= union UNION ALL LP select RP */
   -1,  /* (149) cmd ::= union */
   -2,  /* (150) select ::= SELECT selcollist */
   -2,  /* (151) sclp ::= selcollist COMMA */
    0,  /* (152) sclp ::= */
   -3,  /* (153) selcollist ::= sclp expr as */
   -2,  /* (154) selcollist ::= sclp STAR */
   -2,  /* (155) as ::= AS ids */
   -1,  /* (156) as ::= ids */
    0,  /* (157) as ::= */
   -2,  /* (158) from ::= FROM tablelist */
   -2,  /* (159) tablelist ::= ids cpxName */
   -3,  /* (160) tablelist ::= ids cpxName ids */
   -4,  /* (161) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (162) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (163) tmvar ::= VARIABLE */
   -4,  /* (164) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (165) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (166) interval_opt ::= */
    0,  /* (167) fill_opt ::= */
   -6,  /* (168) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (169) fill_opt ::= FILL LP ID RP */
   -4,  /* (170) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (171) sliding_opt ::= */
    0,  /* (172) orderby_opt ::= */
   -3,  /* (173) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (174) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (175) sortlist ::= item sortorder */
   -2,  /* (176) item ::= ids cpxName */
   -1,  /* (177) sortorder ::= ASC */
   -1,  /* (178) sortorder ::= DESC */
    0,  /* (179) sortorder ::= */
    0,  /* (180) groupby_opt ::= */
   -3,  /* (181) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (182) grouplist ::= grouplist COMMA item */
   -1,  /* (183) grouplist ::= item */
    0,  /* (184) having_opt ::= */
   -2,  /* (185) having_opt ::= HAVING expr */
    0,  /* (186) limit_opt ::= */
   -2,  /* (187) limit_opt ::= LIMIT signed */
   -4,  /* (188) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (189) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (190) slimit_opt ::= */
   -2,  /* (191) slimit_opt ::= SLIMIT signed */
   -4,  /* (192) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (193) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (194) where_opt ::= */
   -2,  /* (195) where_opt ::= WHERE expr */
   -3,  /* (196) expr ::= LP expr RP */
   -1,  /* (197) expr ::= ID */
   -3,  /* (198) expr ::= ID DOT ID */
   -3,  /* (199) expr ::= ID DOT STAR */
   -1,  /* (200) expr ::= INTEGER */
   -2,  /* (201) expr ::= MINUS INTEGER */
   -2,  /* (202) expr ::= PLUS INTEGER */
   -1,  /* (203) expr ::= FLOAT */
   -2,  /* (204) expr ::= MINUS FLOAT */
   -2,  /* (205) expr ::= PLUS FLOAT */
   -1,  /* (206) expr ::= STRING */
   -1,  /* (207) expr ::= NOW */
   -1,  /* (208) expr ::= VARIABLE */
   -1,  /* (209) expr ::= BOOL */
   -4,  /* (210) expr ::= ID LP exprlist RP */
   -4,  /* (211) expr ::= ID LP STAR RP */
   -3,  /* (212) expr ::= expr IS NULL */
   -4,  /* (213) expr ::= expr IS NOT NULL */
   -3,  /* (214) expr ::= expr LT expr */
   -3,  /* (215) expr ::= expr GT expr */
   -3,  /* (216) expr ::= expr LE expr */
   -3,  /* (217) expr ::= expr GE expr */
   -3,  /* (218) expr ::= expr NE expr */
   -3,  /* (219) expr ::= expr EQ expr */
   -3,  /* (220) expr ::= expr AND expr */
   -3,  /* (221) expr ::= expr OR expr */
   -3,  /* (222) expr ::= expr PLUS expr */
   -3,  /* (223) expr ::= expr MINUS expr */
   -3,  /* (224) expr ::= expr STAR expr */
   -3,  /* (225) expr ::= expr SLASH expr */
   -3,  /* (226) expr ::= expr REM expr */
   -3,  /* (227) expr ::= expr LIKE expr */
   -5,  /* (228) expr ::= expr IN LP exprlist RP */
   -3,  /* (229) exprlist ::= exprlist COMMA expritem */
   -1,  /* (230) exprlist ::= expritem */
   -1,  /* (231) expritem ::= expr */
    0,  /* (232) expritem ::= */
   -3,  /* (233) cmd ::= RESET QUERY CACHE */
   -7,  /* (234) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (235) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (236) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (237) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (238) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (239) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (240) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (241) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (242) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (243) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (244) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (245) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (246) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (247) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 120: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==120);
      case 121: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==121);
      case 122: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==122);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 7: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 9: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 10: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 12: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 14: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 15: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 16: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 17: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 18: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 19: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1);
}
        break;
      case 28: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, TSDB_SUPER_TABLE);
}
        break;
      case 29: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, -1); }
        break;
      case 30: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 31: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 32: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 33: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 34: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 35: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 36: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 38: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 39: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 40: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy234, &t);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy71);}
        break;
      case 43: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy71);}
        break;
      case 44: /* ids ::= ID */
      case 45: /* ids ::= STRING */ yytestcase(yyruleno==45);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 46: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 47: /* ifexists ::= */
      case 49: /* ifnotexists ::= */ yytestcase(yyruleno==49);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 48: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 50: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 51: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy71);}
        break;
      case 52: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy234, &yymsp[-2].minor.yy0);}
        break;
      case 53: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 54: /* pps ::= */
      case 56: /* tseries ::= */ yytestcase(yyruleno==56);
      case 58: /* dbs ::= */ yytestcase(yyruleno==58);
      case 60: /* streams ::= */ yytestcase(yyruleno==60);
      case 62: /* storage ::= */ yytestcase(yyruleno==62);
      case 64: /* qtime ::= */ yytestcase(yyruleno==64);
      case 66: /* users ::= */ yytestcase(yyruleno==66);
      case 68: /* conns ::= */ yytestcase(yyruleno==68);
      case 70: /* state ::= */ yytestcase(yyruleno==70);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 55: /* pps ::= PPS INTEGER */
      case 57: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==57);
      case 59: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==59);
      case 61: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==61);
      case 63: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==63);
      case 65: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==65);
      case 67: /* users ::= USERS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==69);
      case 71: /* state ::= STATE ids */ yytestcase(yyruleno==71);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 72: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy71.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy71.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy71.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy71.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy71.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy71.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy71.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy71.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy71.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy71 = yylhsminor.yy71;
        break;
      case 73: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy421 = yymsp[0].minor.yy421; }
        break;
      case 74: /* cache ::= CACHE INTEGER */
      case 75: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==75);
      case 76: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==76);
      case 77: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==79);
      case 80: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==80);
      case 81: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==81);
      case 82: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==82);
      case 83: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==83);
      case 84: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==84);
      case 85: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==85);
      case 86: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==86);
      case 87: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==87);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 88: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy234);}
        break;
      case 89: /* db_optr ::= db_optr cache */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 90: /* db_optr ::= db_optr replica */
      case 105: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==105);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 91: /* db_optr ::= db_optr quorum */
      case 106: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==106);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 92: /* db_optr ::= db_optr days */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 93: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 94: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 95: /* db_optr ::= db_optr blocks */
      case 108: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==108);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 96: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 97: /* db_optr ::= db_optr wal */
      case 110: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==110);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 98: /* db_optr ::= db_optr fsync */
      case 111: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==111);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 99: /* db_optr ::= db_optr comp */
      case 109: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==109);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 100: /* db_optr ::= db_optr prec */
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 101: /* db_optr ::= db_optr keep */
      case 107: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==107);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.keep = yymsp[0].minor.yy421; }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 102: /* db_optr ::= db_optr update */
      case 112: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==112);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 103: /* db_optr ::= db_optr cachelast */
      case 113: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==113);
{ yylhsminor.yy234 = yymsp[-1].minor.yy234; yylhsminor.yy234.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 104: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy234);}
        break;
      case 114: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy183, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy183 = yylhsminor.yy183;
        break;
      case 115: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy325 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy325;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy183, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy183 = yylhsminor.yy183;
        break;
      case 116: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy183, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 117: /* signed ::= INTEGER */
{ yylhsminor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 118: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy325 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 119: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy325 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 123: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy38;}
        break;
      case 124: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy152);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy38 = pCreateTable;
}
  yymsp[0].minor.yy38 = yylhsminor.yy38;
        break;
      case 125: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy38->childTableInfo, &yymsp[0].minor.yy152);
  yylhsminor.yy38 = yymsp[-1].minor.yy38;
}
  yymsp[-1].minor.yy38 = yylhsminor.yy38;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy38 = tSetCreateSqlElems(yymsp[-1].minor.yy421, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy38 = yylhsminor.yy38;
        break;
      case 127: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy38 = tSetCreateSqlElems(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy38 = yylhsminor.yy38;
        break;
      case 128: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy152 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy421, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy152 = yylhsminor.yy152;
        break;
      case 129: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy38 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy148, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy38 = yylhsminor.yy38;
        break;
      case 130: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy421, &yymsp[0].minor.yy183); yylhsminor.yy421 = yymsp[-2].minor.yy421;  }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 131: /* columnlist ::= column */
{yylhsminor.yy421 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy421, &yymsp[0].minor.yy183);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 132: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy183, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);
}
  yymsp[-1].minor.yy183 = yylhsminor.yy183;
        break;
      case 133: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);    }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 134: /* tagitemlist ::= tagitem */
{ yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1); }
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 135: /* tagitem ::= INTEGER */
      case 136: /* tagitem ::= FLOAT */ yytestcase(yyruleno==136);
      case 137: /* tagitem ::= STRING */ yytestcase(yyruleno==137);
      case 138: /* tagitem ::= BOOL */ yytestcase(yyruleno==138);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 139: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy430, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy430 = yylhsminor.yy430;
        break;
      case 140: /* tagitem ::= MINUS INTEGER */
      case 141: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==141);
      case 142: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==142);
      case 143: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==143);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 144: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy148 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy166, yymsp[-9].minor.yy421, yymsp[-8].minor.yy78, yymsp[-4].minor.yy421, yymsp[-3].minor.yy421, &yymsp[-7].minor.yy400, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy421, &yymsp[0].minor.yy167, &yymsp[-1].minor.yy167);
}
  yymsp[-11].minor.yy148 = yylhsminor.yy148;
        break;
      case 145: /* union ::= select */
{ yylhsminor.yy153 = setSubclause(NULL, yymsp[0].minor.yy148); }
  yymsp[0].minor.yy153 = yylhsminor.yy153;
        break;
      case 146: /* union ::= LP union RP */
{ yymsp[-2].minor.yy153 = yymsp[-1].minor.yy153; }
        break;
      case 147: /* union ::= union UNION ALL select */
{ yylhsminor.yy153 = appendSelectClause(yymsp[-3].minor.yy153, yymsp[0].minor.yy148); }
  yymsp[-3].minor.yy153 = yylhsminor.yy153;
        break;
      case 148: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy153 = appendSelectClause(yymsp[-5].minor.yy153, yymsp[-1].minor.yy148); }
  yymsp[-5].minor.yy153 = yylhsminor.yy153;
        break;
      case 149: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy153, NULL, TSDB_SQL_SELECT); }
        break;
      case 150: /* select ::= SELECT selcollist */
{
  yylhsminor.yy148 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy166, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 151: /* sclp ::= selcollist COMMA */
{yylhsminor.yy166 = yymsp[-1].minor.yy166;}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 152: /* sclp ::= */
{yymsp[1].minor.yy166 = 0;}
        break;
      case 153: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy166 = tSqlExprListAppend(yymsp[-2].minor.yy166, yymsp[-1].minor.yy78, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 154: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy166 = tSqlExprListAppend(yymsp[-1].minor.yy166, pNode, 0);
}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 155: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 156: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 157: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 158: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 159: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 160: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 161: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 162: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy421 = tVariantListAppendToken(yymsp[-4].minor.yy421, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy421 = tVariantListAppendToken(yylhsminor.yy421, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy421 = yylhsminor.yy421;
        break;
      case 163: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 164: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy400.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy400.offset.n = 0; yymsp[-3].minor.yy400.offset.z = NULL; yymsp[-3].minor.yy400.offset.type = 0;}
        break;
      case 165: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy400.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy400.offset = yymsp[-1].minor.yy0;}
        break;
      case 166: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 167: /* fill_opt ::= */
{yymsp[1].minor.yy421 = 0;     }
        break;
      case 168: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy421, &A, -1, 0);
    yymsp[-5].minor.yy421 = yymsp[-1].minor.yy421;
}
        break;
      case 169: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy421 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 170: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 171: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 172: /* orderby_opt ::= */
{yymsp[1].minor.yy421 = 0;}
        break;
      case 173: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 174: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy421 = tVariantListAppend(yymsp[-3].minor.yy421, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
  yymsp[-3].minor.yy421 = yylhsminor.yy421;
        break;
      case 175: /* sortlist ::= item sortorder */
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[-1].minor.yy430, yymsp[0].minor.yy96);
}
  yymsp[-1].minor.yy421 = yylhsminor.yy421;
        break;
      case 176: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy430, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy430 = yylhsminor.yy430;
        break;
      case 177: /* sortorder ::= ASC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 178: /* sortorder ::= DESC */
{ yymsp[0].minor.yy96 = TSDB_ORDER_DESC;}
        break;
      case 179: /* sortorder ::= */
{ yymsp[1].minor.yy96 = TSDB_ORDER_ASC; }
        break;
      case 180: /* groupby_opt ::= */
{ yymsp[1].minor.yy421 = 0;}
        break;
      case 181: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy421 = yymsp[0].minor.yy421;}
        break;
      case 182: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy421 = tVariantListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy430, -1);
}
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 183: /* grouplist ::= item */
{
  yylhsminor.yy421 = tVariantListAppend(NULL, &yymsp[0].minor.yy430, -1);
}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 184: /* having_opt ::= */
      case 194: /* where_opt ::= */ yytestcase(yyruleno==194);
      case 232: /* expritem ::= */ yytestcase(yyruleno==232);
{yymsp[1].minor.yy78 = 0;}
        break;
      case 185: /* having_opt ::= HAVING expr */
      case 195: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==195);
{yymsp[-1].minor.yy78 = yymsp[0].minor.yy78;}
        break;
      case 186: /* limit_opt ::= */
      case 190: /* slimit_opt ::= */ yytestcase(yyruleno==190);
{yymsp[1].minor.yy167.limit = -1; yymsp[1].minor.yy167.offset = 0;}
        break;
      case 187: /* limit_opt ::= LIMIT signed */
      case 191: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==191);
{yymsp[-1].minor.yy167.limit = yymsp[0].minor.yy325;  yymsp[-1].minor.yy167.offset = 0;}
        break;
      case 188: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy167.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[0].minor.yy325;}
        break;
      case 189: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy167.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[-2].minor.yy325;}
        break;
      case 192: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy167.limit = yymsp[-2].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[0].minor.yy325;}
        break;
      case 193: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy167.limit = yymsp[0].minor.yy325;  yymsp[-3].minor.yy167.offset = yymsp[-2].minor.yy325;}
        break;
      case 196: /* expr ::= LP expr RP */
{yylhsminor.yy78 = yymsp[-1].minor.yy78; yylhsminor.yy78->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy78->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 197: /* expr ::= ID */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 198: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 199: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 200: /* expr ::= INTEGER */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 201: /* expr ::= MINUS INTEGER */
      case 202: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==202);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy78 = yylhsminor.yy78;
        break;
      case 203: /* expr ::= FLOAT */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 204: /* expr ::= MINUS FLOAT */
      case 205: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==205);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy78 = yylhsminor.yy78;
        break;
      case 206: /* expr ::= STRING */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 207: /* expr ::= NOW */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 208: /* expr ::= VARIABLE */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 209: /* expr ::= BOOL */
{ yylhsminor.yy78 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 210: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy78 = tSqlExprCreateFunction(yymsp[-1].minor.yy166, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy78 = yylhsminor.yy78;
        break;
      case 211: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy78 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy78 = yylhsminor.yy78;
        break;
      case 212: /* expr ::= expr IS NULL */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 213: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-3].minor.yy78, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy78 = yylhsminor.yy78;
        break;
      case 214: /* expr ::= expr LT expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_LT);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 215: /* expr ::= expr GT expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_GT);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 216: /* expr ::= expr LE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_LE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 217: /* expr ::= expr GE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_GE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 218: /* expr ::= expr NE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_NE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 219: /* expr ::= expr EQ expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_EQ);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 220: /* expr ::= expr AND expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_AND);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 221: /* expr ::= expr OR expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_OR); }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 222: /* expr ::= expr PLUS expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_PLUS);  }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 223: /* expr ::= expr MINUS expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_MINUS); }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 224: /* expr ::= expr STAR expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_STAR);  }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 225: /* expr ::= expr SLASH expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_DIVIDE);}
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 226: /* expr ::= expr REM expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_REM);   }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 227: /* expr ::= expr LIKE expr */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-2].minor.yy78, yymsp[0].minor.yy78, TK_LIKE);  }
  yymsp[-2].minor.yy78 = yylhsminor.yy78;
        break;
      case 228: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy78 = tSqlExprCreate(yymsp[-4].minor.yy78, (tSQLExpr*)yymsp[-1].minor.yy166, TK_IN); }
  yymsp[-4].minor.yy78 = yylhsminor.yy78;
        break;
      case 229: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy166 = tSqlExprListAppend(yymsp[-2].minor.yy166,yymsp[0].minor.yy78,0);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 230: /* exprlist ::= expritem */
{yylhsminor.yy166 = tSqlExprListAppend(0,yymsp[0].minor.yy78,0);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 231: /* expritem ::= expr */
{yylhsminor.yy78 = yymsp[0].minor.yy78;}
  yymsp[0].minor.yy78 = yylhsminor.yy78;
        break;
      case 233: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 234: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 235: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 237: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 238: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 239: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy430, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 240: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 241: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 242: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 243: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 244: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 245: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 246: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 247: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  int32_t outputBufLen = tListLen(pInfo->pzErrMsg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->pzErrMsg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->pzErrMsg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->pzErrMsg, "Incomplete SQL statement");
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
