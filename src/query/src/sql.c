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
#define YYNOCODE 280
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SQuerySQL* yy30;
  SCreateTableSQL* yy38;
  SCreatedTableInfo yy78;
  SLimitVal yy126;
  int yy130;
  SArray* yy135;
  SSubclauseInfo* yy153;
  SIntervalVal yy160;
  TAOS_FIELD yy181;
  SCreateDbInfo yy256;
  tSQLExprList* yy266;
  SCreateAcctInfo yy277;
  tVariant yy308;
  tSQLExpr* yy316;
  int64_t yy531;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             282
#define YYNRULE              248
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
#define YY_ACTTAB_COUNT (622)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   158,  505,  158,  710,  281,  857,   15,  578,  182,  506,
 /*    10 */   941,  185,  942,   41,   42,  157,   43,   44,   26,  179,
 /*    20 */   190,   35,  505,  505,  231,   47,   45,   49,   46,  868,
 /*    30 */   506,  506,  846,   40,   39,  256,  255,   38,   37,   36,
 /*    40 */    41,   42,  162,   43,   44,  857,  951,  190,   35,  178,
 /*    50 */   279,  231,   47,   45,   49,   46,  180,  195,  843,  214,
 /*    60 */    40,   39,  938,   61,   38,   37,   36,  462,  463,  464,
 /*    70 */   465,  466,  467,  468,  469,  470,  471,  472,  473,  280,
 /*    80 */   198,  846,  204,   41,   42,  865,   43,   44,  246,  266,
 /*    90 */   190,   35,  158,  834,  231,   47,   45,   49,   46,  122,
 /*   100 */   122,  184,  942,   40,   39,  122,   62,   38,   37,   36,
 /*   110 */    20,  244,  274,  273,  243,  242,  241,  272,  240,  271,
 /*   120 */   270,  269,  239,  268,  267,   38,   37,   36,  813,  657,
 /*   130 */   801,  802,  803,  804,  805,  806,  807,  808,  809,  810,
 /*   140 */   811,  812,  814,  815,   42,  200,   43,   44,  253,  252,
 /*   150 */   190,   35,  937,  275,  231,   47,   45,   49,   46,  228,
 /*   160 */   895,   66,  226,   40,   39,  115,  894,   38,   37,   36,
 /*   170 */    26,   43,   44,   32,   26,  190,   35,  846,  207,  231,
 /*   180 */    47,   45,   49,   46,   26,  211,  210,  754,   40,   39,
 /*   190 */   146,  845,   38,   37,   36,  189,  670,  117,  835,  661,
 /*   200 */    71,  664,   26,  667,  763,  189,  670,  146,  193,  661,
 /*   210 */   843,  664,  194,  667,  843,  189,  670,   16,   26,  661,
 /*   220 */   936,  664,  249,  667,  843,   10,   21,  186,  187,   70,
 /*   230 */   132,  230,  837,  166,   32,  196,   69,  186,  187,  167,
 /*   240 */   250,  615,  843,  122,  102,  101,  165,  186,  187,  174,
 /*   250 */    20,   26,  274,  273,  219,  905,  254,  272,  843,  271,
 /*   260 */   270,  269,  607,  268,  267,  832,  833,   25,  836,  819,
 /*   270 */   216,   90,  817,  818,   21,  246,  266,  820,   68,  822,
 /*   280 */   823,  821,   32,  824,  825,   47,   45,   49,   46,  258,
 /*   290 */    33,  843,  199,   40,   39,   48,   26,   38,   37,   36,
 /*   300 */   197,  217,  213,  248,   67,   48,   27,  669,  659,  173,
 /*   310 */   278,  277,  109,  755,  232,   48,  146,  669,  599,  638,
 /*   320 */   639,  596,  668,  597,  844,  598,  612,  669,  175,   40,
 /*   330 */    39,   22,  668,   38,   37,   36,  842,  160,  625,   88,
 /*   340 */    92,  188,  668,  119,  660,   82,   97,  100,   91,  201,
 /*   350 */   202,   99,   98,  629,   94,    3,  136,  161,   52,  152,
 /*   360 */   148,   29,   77,   73,   76,  150,  105,  104,  103,  630,
 /*   370 */   689,  671,   53,   56,   18,   17,   17,  663,  163,  666,
 /*   380 */   662,  588,  665,    4,   81,   80,   27,  234,  589,  164,
 /*   390 */    57,   54,   27,   52,   12,   11,   87,   86,   59,  603,
 /*   400 */   577,  604,   14,   13,  601,  170,  602,  673,  114,  112,
 /*   410 */   171,  169,  156,  168,  159,  859,  904,  191,  901,  116,
 /*   420 */   900,  192,  257,  867,  600,  872,  874,  887,  118,  886,
 /*   430 */   133,  134,   32,  131,  135,  765,  238,  154,   30,  247,
 /*   440 */   762,  956,   78,  955,  953,  137,  251,  950,   84,  113,
 /*   450 */   949,  947,  138,  783,   31,   28,  155,  752,   93,  750,
 /*   460 */   215,   95,   96,  748,  624,   58,  747,  203,  147,  745,
 /*   470 */   744,  743,  742,  220,  741,   55,  149,   50,  181,  229,
 /*   480 */   224,  856,  227,  124,  151,  738,  736,  734,  732,  730,
 /*   490 */   225,  223,  221,  153,   34,  218,   89,   63,   64,  259,
 /*   500 */   260,  888,  261,  262,  264,  263,  265,  276,  708,  205,
 /*   510 */   206,  707,  208,  209,  706,  176,  236,  237,  694,  177,
 /*   520 */   172,  212,   74,  216,   65,  609,  746,   60,  233,  106,
 /*   530 */     6,  183,  740,  141,  107,  140,  784,  139,  142,  144,
 /*   540 */   143,  145,  739,    1,  108,  841,  731,    2,  626,  129,
 /*   550 */   127,  125,  123,  126,  128,  120,  222,  130,  631,  121,
 /*   560 */    23,    7,    8,  672,   24,    5,    9,  674,   19,   72,
 /*   570 */   235,  546,  542,   70,  540,  539,  538,  535,  509,  245,
 /*   580 */    79,   75,   27,   83,   51,  580,  579,  576,  530,  528,
 /*   590 */   520,  526,  522,   85,  524,  518,  516,  548,  547,  545,
 /*   600 */   544,  543,  541,  537,  536,   52,  507,  477,  475,  110,
 /*   610 */   712,  711,  711,  711,  711,  711,  711,  711,  711,  711,
 /*   620 */   711,  111,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   268,    1,  268,  210,  211,  252,  268,    5,  230,    9,
 /*    10 */   278,  277,  278,   13,   14,  268,   16,   17,  213,  266,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  213,
 /*    30 */     9,    9,  254,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  268,   16,   17,  252,  254,   20,   21,  212,
 /*    50 */   213,   24,   25,   26,   27,   28,  251,  230,  253,  266,
 /*    60 */    33,   34,  268,  218,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    66,  254,   60,   13,   14,  269,   16,   17,   77,   79,
 /*    90 */    20,   21,  268,  248,   24,   25,   26,   27,   28,  213,
 /*   100 */   213,  277,  278,   33,   34,  213,  106,   37,   38,   39,
 /*   110 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   120 */    96,   97,   98,   99,  100,   37,   38,   39,  229,  102,
 /*   130 */   231,  232,  233,  234,  235,  236,  237,  238,  239,  240,
 /*   140 */   241,  242,  243,  244,   14,  131,   16,   17,  134,  135,
 /*   150 */    20,   21,  268,  230,   24,   25,   26,   27,   28,  272,
 /*   160 */   274,  274,  276,   33,   34,  101,  274,   37,   38,   39,
 /*   170 */   213,   16,   17,  109,  213,   20,   21,  254,  130,   24,
 /*   180 */    25,   26,   27,   28,  213,  137,  138,  217,   33,   34,
 /*   190 */   220,  254,   37,   38,   39,    1,    2,  213,    0,    5,
 /*   200 */   218,    7,  213,    9,  217,    1,    2,  220,  251,    5,
 /*   210 */   253,    7,  251,    9,  253,    1,    2,   44,  213,    5,
 /*   220 */   268,    7,  251,    9,  253,  101,  101,   33,   34,  105,
 /*   230 */   106,   37,  250,   60,  109,   66,  218,   33,   34,   66,
 /*   240 */   251,   37,  253,  213,   71,   72,   73,   33,   34,  268,
 /*   250 */    86,  213,   88,   89,  270,  246,  251,   93,  253,   95,
 /*   260 */    96,   97,  102,   99,  100,  247,  248,  249,  250,  229,
 /*   270 */   110,   74,  232,  233,  101,   77,   79,  237,  255,  239,
 /*   280 */   240,  241,  109,  243,  244,   25,   26,   27,   28,  251,
 /*   290 */   267,  253,  213,   33,   34,  101,  213,   37,   38,   39,
 /*   300 */   131,  102,  129,  134,  274,  101,  107,  113,    1,  136,
 /*   310 */    63,   64,   65,  217,   15,  101,  220,  113,    2,  119,
 /*   320 */   120,    5,  128,    7,  245,    9,  107,  113,  268,   33,
 /*   330 */    34,  112,  128,   37,   38,   39,  253,  268,  102,   61,
 /*   340 */    62,   59,  128,  107,   37,   67,   68,   69,   70,   33,
 /*   350 */    34,   74,   75,  102,   76,   61,   62,  268,  107,   61,
 /*   360 */    62,   67,   68,   69,   70,   67,   68,   69,   70,  102,
 /*   370 */   102,  102,  107,  107,  107,  107,  107,    5,  268,    7,
 /*   380 */     5,  102,    7,  101,  132,  133,  107,  102,  102,  268,
 /*   390 */   124,  126,  107,  107,  132,  133,  132,  133,  101,    5,
 /*   400 */   103,    7,  132,  133,    5,  268,    7,  108,   61,   62,
 /*   410 */   268,  268,  268,  268,  268,  252,  246,  246,  246,  213,
 /*   420 */   246,  246,  246,  213,  108,  213,  213,  275,  213,  275,
 /*   430 */   213,  213,  109,  256,  213,  213,  213,  213,  213,  213,
 /*   440 */   213,  213,  213,  213,  213,  213,  213,  213,  213,   59,
 /*   450 */   213,  213,  213,  213,  213,  213,  213,  213,  213,  213,
 /*   460 */   252,  213,  213,  213,  113,  123,  213,  213,  213,  213,
 /*   470 */   213,  213,  213,  271,  213,  125,  213,  122,  271,  117,
 /*   480 */   271,  265,  121,  263,  213,  213,  213,  213,  213,  213,
 /*   490 */   116,  115,  114,  213,  127,  214,   85,  214,  214,   84,
 /*   500 */    49,  214,   81,   83,   82,   53,   80,   77,    5,  139,
 /*   510 */     5,    5,  139,    5,    5,  214,  214,  214,   87,  214,
 /*   520 */   214,  130,  218,  110,  107,  102,  214,  111,  104,  215,
 /*   530 */   101,    1,  214,  222,  215,  226,  228,  227,  225,  224,
 /*   540 */   223,  221,  214,  219,  215,  252,  214,  216,  102,  258,
 /*   550 */   260,  262,  264,  261,  259,  101,  101,  257,  102,  101,
 /*   560 */   107,  118,  118,  102,  107,  101,  101,  108,  101,   74,
 /*   570 */   104,    9,    5,  105,    5,    5,    5,    5,   78,   15,
 /*   580 */   133,   74,  107,  133,   16,    5,    5,  102,    5,    5,
 /*   590 */     5,    5,    5,  133,    5,    5,    5,    5,    5,    5,
 /*   600 */     5,    5,    5,    5,    5,  107,   78,   59,   58,   21,
 /*   610 */     0,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   620 */   279,   21,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   630 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   640 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   650 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   660 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   670 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   680 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   690 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   700 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   710 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   720 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   730 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   740 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   750 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   760 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   770 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   780 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   790 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   800 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   810 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   820 */   279,  279,  279,  279,  279,  279,  279,  279,  279,  279,
 /*   830 */   279,
};
#define YY_SHIFT_COUNT    (281)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (610)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   173,   24,  164,   11,  194,  214,   21,   21,   21,   21,
 /*    10 */    21,   21,   21,   21,   21,    0,   22,  214,  316,  316,
 /*    20 */   316,  125,   21,   21,   21,  198,   21,   21,  197,   11,
 /*    30 */    10,   10,  622,  204,  214,  214,  214,  214,  214,  214,
 /*    40 */   214,  214,  214,  214,  214,  214,  214,  214,  214,  214,
 /*    50 */   214,  316,  316,    2,    2,    2,    2,    2,    2,    2,
 /*    60 */    64,   21,   21,   21,   21,   21,  200,  200,  219,   21,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,  323,  390,  390,  390,  351,
 /*   120 */   351,  351,  390,  342,  350,  355,  362,  361,  374,  376,
 /*   130 */   378,  367,  323,  390,  390,  390,   11,  390,  390,  411,
 /*   140 */   415,  451,  421,  420,  452,  422,  426,  390,  430,  390,
 /*   150 */   430,  390,  430,  390,  622,  622,   27,   70,   70,   70,
 /*   160 */   130,  155,  260,  260,  260,  278,  294,  298,  296,  296,
 /*   170 */   296,  296,   14,   48,   88,   88,  124,  169,  247,  160,
 /*   180 */   199,  236,  251,  267,  268,  269,  372,  375,  307,  282,
 /*   190 */   299,  265,  266,  279,  285,  286,  252,  262,  264,  297,
 /*   200 */   270,  394,  399,  277,  347,  503,  370,  505,  506,  373,
 /*   210 */   508,  509,  431,  391,  413,  423,  416,  424,  429,  417,
 /*   220 */   446,  454,  530,  455,  456,  458,  453,  443,  457,  444,
 /*   230 */   461,  464,  459,  465,  424,  467,  466,  468,  495,  562,
 /*   240 */   567,  569,  570,  571,  572,  500,  564,  507,  447,  475,
 /*   250 */   475,  568,  450,  460,  475,  580,  581,  485,  475,  583,
 /*   260 */   584,  585,  586,  587,  589,  590,  591,  592,  593,  594,
 /*   270 */   595,  596,  597,  598,  599,  498,  528,  588,  600,  548,
 /*   280 */   550,  610,
};
#define YY_REDUCE_COUNT (155)
#define YY_REDUCE_MIN   (-268)
#define YY_REDUCE_MAX   (332)
static const short yy_reduce_ofst[] = {
 /*     0 */  -207, -101,   40,   18, -266, -176, -195, -114, -113,  -43,
 /*    10 */   -39,  -29,  -11,    5,   38, -184, -163, -268, -222, -173,
 /*    20 */   -77, -247,  -16, -108,   30,  -18,   79,   83,  -30, -155,
 /*    30 */   -13,   96,   23, -262, -253, -226, -206, -116,  -48,  -19,
 /*    40 */    60,   69,   89,  110,  121,  137,  142,  143,  144,  145,
 /*    50 */   146, -208,  -63,    9,  170,  171,  172,  174,  175,  176,
 /*    60 */   163,  206,  210,  212,  213,  215,  152,  154,  177,  217,
 /*    70 */   218,  221,  222,  223,  224,  225,  226,  227,  228,  229,
 /*    80 */   230,  231,  232,  233,  234,  235,  237,  238,  239,  240,
 /*    90 */   241,  242,  243,  244,  245,  246,  248,  249,  250,  253,
 /*   100 */   254,  255,  256,  257,  258,  259,  261,  263,  271,  272,
 /*   110 */   273,  274,  275,  276,  280,  208,  281,  283,  284,  202,
 /*   120 */   207,  209,  287,  216,  288,  220,  289,  292,  290,  295,
 /*   130 */   291,  300,  293,  301,  302,  303,  304,  305,  306,  308,
 /*   140 */   310,  309,  311,  313,  317,  315,  320,  312,  314,  318,
 /*   150 */   319,  328,  329,  332,  324,  331,
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
  /*  209 */ "error",
  /*  210 */ "program",
  /*  211 */ "cmd",
  /*  212 */ "dbPrefix",
  /*  213 */ "ids",
  /*  214 */ "cpxName",
  /*  215 */ "ifexists",
  /*  216 */ "alter_db_optr",
  /*  217 */ "acct_optr",
  /*  218 */ "ifnotexists",
  /*  219 */ "db_optr",
  /*  220 */ "pps",
  /*  221 */ "tseries",
  /*  222 */ "dbs",
  /*  223 */ "streams",
  /*  224 */ "storage",
  /*  225 */ "qtime",
  /*  226 */ "users",
  /*  227 */ "conns",
  /*  228 */ "state",
  /*  229 */ "keep",
  /*  230 */ "tagitemlist",
  /*  231 */ "cache",
  /*  232 */ "replica",
  /*  233 */ "quorum",
  /*  234 */ "days",
  /*  235 */ "minrows",
  /*  236 */ "maxrows",
  /*  237 */ "blocks",
  /*  238 */ "ctime",
  /*  239 */ "wal",
  /*  240 */ "fsync",
  /*  241 */ "comp",
  /*  242 */ "prec",
  /*  243 */ "update",
  /*  244 */ "cachelast",
  /*  245 */ "typename",
  /*  246 */ "signed",
  /*  247 */ "create_table_args",
  /*  248 */ "create_stable_args",
  /*  249 */ "create_table_list",
  /*  250 */ "create_from_stable",
  /*  251 */ "columnlist",
  /*  252 */ "select",
  /*  253 */ "column",
  /*  254 */ "tagitem",
  /*  255 */ "selcollist",
  /*  256 */ "from",
  /*  257 */ "where_opt",
  /*  258 */ "interval_opt",
  /*  259 */ "fill_opt",
  /*  260 */ "sliding_opt",
  /*  261 */ "groupby_opt",
  /*  262 */ "orderby_opt",
  /*  263 */ "having_opt",
  /*  264 */ "slimit_opt",
  /*  265 */ "limit_opt",
  /*  266 */ "union",
  /*  267 */ "sclp",
  /*  268 */ "expr",
  /*  269 */ "as",
  /*  270 */ "tablelist",
  /*  271 */ "tmvar",
  /*  272 */ "sortlist",
  /*  273 */ "sortitem",
  /*  274 */ "item",
  /*  275 */ "sortorder",
  /*  276 */ "grouplist",
  /*  277 */ "exprlist",
  /*  278 */ "expritem",
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
    case 229: /* keep */
    case 230: /* tagitemlist */
    case 251: /* columnlist */
    case 259: /* fill_opt */
    case 261: /* groupby_opt */
    case 262: /* orderby_opt */
    case 272: /* sortlist */
    case 276: /* grouplist */
{
taosArrayDestroy((yypminor->yy135));
}
      break;
    case 249: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy38));
}
      break;
    case 252: /* select */
{
doDestroyQuerySql((yypminor->yy30));
}
      break;
    case 255: /* selcollist */
    case 267: /* sclp */
    case 277: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy266));
}
      break;
    case 257: /* where_opt */
    case 263: /* having_opt */
    case 268: /* expr */
    case 278: /* expritem */
{
tSqlExprDestroy((yypminor->yy316));
}
      break;
    case 266: /* union */
{
destroyAllSelectClause((yypminor->yy153));
}
      break;
    case 273: /* sortitem */
{
tVariantDestroy(&(yypminor->yy308));
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
  {  210,   -1 }, /* (0) program ::= cmd */
  {  211,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  211,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  211,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  211,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  211,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  211,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  211,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  211,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  211,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  211,   -2 }, /* (10) cmd ::= SHOW VARIABLES */
  {  211,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  211,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  211,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  211,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  212,    0 }, /* (15) dbPrefix ::= */
  {  212,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  214,    0 }, /* (17) cpxName ::= */
  {  214,   -2 }, /* (18) cpxName ::= DOT ids */
  {  211,   -5 }, /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  211,   -4 }, /* (20) cmd ::= SHOW CREATE DATABASE ids */
  {  211,   -3 }, /* (21) cmd ::= SHOW dbPrefix TABLES */
  {  211,   -5 }, /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  211,   -3 }, /* (23) cmd ::= SHOW dbPrefix STABLES */
  {  211,   -5 }, /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  211,   -3 }, /* (25) cmd ::= SHOW dbPrefix VGROUPS */
  {  211,   -4 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  211,   -5 }, /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
  {  211,   -5 }, /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
  {  211,   -4 }, /* (29) cmd ::= DROP DATABASE ifexists ids */
  {  211,   -3 }, /* (30) cmd ::= DROP DNODE ids */
  {  211,   -3 }, /* (31) cmd ::= DROP USER ids */
  {  211,   -3 }, /* (32) cmd ::= DROP ACCOUNT ids */
  {  211,   -2 }, /* (33) cmd ::= USE ids */
  {  211,   -3 }, /* (34) cmd ::= DESCRIBE ids cpxName */
  {  211,   -5 }, /* (35) cmd ::= ALTER USER ids PASS ids */
  {  211,   -5 }, /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  211,   -4 }, /* (37) cmd ::= ALTER DNODE ids ids */
  {  211,   -5 }, /* (38) cmd ::= ALTER DNODE ids ids ids */
  {  211,   -3 }, /* (39) cmd ::= ALTER LOCAL ids */
  {  211,   -4 }, /* (40) cmd ::= ALTER LOCAL ids ids */
  {  211,   -4 }, /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  211,   -4 }, /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  211,   -6 }, /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  213,   -1 }, /* (44) ids ::= ID */
  {  213,   -1 }, /* (45) ids ::= STRING */
  {  215,   -2 }, /* (46) ifexists ::= IF EXISTS */
  {  215,    0 }, /* (47) ifexists ::= */
  {  218,   -3 }, /* (48) ifnotexists ::= IF NOT EXISTS */
  {  218,    0 }, /* (49) ifnotexists ::= */
  {  211,   -3 }, /* (50) cmd ::= CREATE DNODE ids */
  {  211,   -6 }, /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  211,   -5 }, /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  211,   -5 }, /* (53) cmd ::= CREATE USER ids PASS ids */
  {  220,    0 }, /* (54) pps ::= */
  {  220,   -2 }, /* (55) pps ::= PPS INTEGER */
  {  221,    0 }, /* (56) tseries ::= */
  {  221,   -2 }, /* (57) tseries ::= TSERIES INTEGER */
  {  222,    0 }, /* (58) dbs ::= */
  {  222,   -2 }, /* (59) dbs ::= DBS INTEGER */
  {  223,    0 }, /* (60) streams ::= */
  {  223,   -2 }, /* (61) streams ::= STREAMS INTEGER */
  {  224,    0 }, /* (62) storage ::= */
  {  224,   -2 }, /* (63) storage ::= STORAGE INTEGER */
  {  225,    0 }, /* (64) qtime ::= */
  {  225,   -2 }, /* (65) qtime ::= QTIME INTEGER */
  {  226,    0 }, /* (66) users ::= */
  {  226,   -2 }, /* (67) users ::= USERS INTEGER */
  {  227,    0 }, /* (68) conns ::= */
  {  227,   -2 }, /* (69) conns ::= CONNS INTEGER */
  {  228,    0 }, /* (70) state ::= */
  {  228,   -2 }, /* (71) state ::= STATE ids */
  {  217,   -9 }, /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  229,   -2 }, /* (73) keep ::= KEEP tagitemlist */
  {  231,   -2 }, /* (74) cache ::= CACHE INTEGER */
  {  232,   -2 }, /* (75) replica ::= REPLICA INTEGER */
  {  233,   -2 }, /* (76) quorum ::= QUORUM INTEGER */
  {  234,   -2 }, /* (77) days ::= DAYS INTEGER */
  {  235,   -2 }, /* (78) minrows ::= MINROWS INTEGER */
  {  236,   -2 }, /* (79) maxrows ::= MAXROWS INTEGER */
  {  237,   -2 }, /* (80) blocks ::= BLOCKS INTEGER */
  {  238,   -2 }, /* (81) ctime ::= CTIME INTEGER */
  {  239,   -2 }, /* (82) wal ::= WAL INTEGER */
  {  240,   -2 }, /* (83) fsync ::= FSYNC INTEGER */
  {  241,   -2 }, /* (84) comp ::= COMP INTEGER */
  {  242,   -2 }, /* (85) prec ::= PRECISION STRING */
  {  243,   -2 }, /* (86) update ::= UPDATE INTEGER */
  {  244,   -2 }, /* (87) cachelast ::= CACHELAST INTEGER */
  {  219,    0 }, /* (88) db_optr ::= */
  {  219,   -2 }, /* (89) db_optr ::= db_optr cache */
  {  219,   -2 }, /* (90) db_optr ::= db_optr replica */
  {  219,   -2 }, /* (91) db_optr ::= db_optr quorum */
  {  219,   -2 }, /* (92) db_optr ::= db_optr days */
  {  219,   -2 }, /* (93) db_optr ::= db_optr minrows */
  {  219,   -2 }, /* (94) db_optr ::= db_optr maxrows */
  {  219,   -2 }, /* (95) db_optr ::= db_optr blocks */
  {  219,   -2 }, /* (96) db_optr ::= db_optr ctime */
  {  219,   -2 }, /* (97) db_optr ::= db_optr wal */
  {  219,   -2 }, /* (98) db_optr ::= db_optr fsync */
  {  219,   -2 }, /* (99) db_optr ::= db_optr comp */
  {  219,   -2 }, /* (100) db_optr ::= db_optr prec */
  {  219,   -2 }, /* (101) db_optr ::= db_optr keep */
  {  219,   -2 }, /* (102) db_optr ::= db_optr update */
  {  219,   -2 }, /* (103) db_optr ::= db_optr cachelast */
  {  216,    0 }, /* (104) alter_db_optr ::= */
  {  216,   -2 }, /* (105) alter_db_optr ::= alter_db_optr replica */
  {  216,   -2 }, /* (106) alter_db_optr ::= alter_db_optr quorum */
  {  216,   -2 }, /* (107) alter_db_optr ::= alter_db_optr keep */
  {  216,   -2 }, /* (108) alter_db_optr ::= alter_db_optr blocks */
  {  216,   -2 }, /* (109) alter_db_optr ::= alter_db_optr comp */
  {  216,   -2 }, /* (110) alter_db_optr ::= alter_db_optr wal */
  {  216,   -2 }, /* (111) alter_db_optr ::= alter_db_optr fsync */
  {  216,   -2 }, /* (112) alter_db_optr ::= alter_db_optr update */
  {  216,   -2 }, /* (113) alter_db_optr ::= alter_db_optr cachelast */
  {  245,   -1 }, /* (114) typename ::= ids */
  {  245,   -4 }, /* (115) typename ::= ids LP signed RP */
  {  245,   -2 }, /* (116) typename ::= ids UNSIGNED */
  {  246,   -1 }, /* (117) signed ::= INTEGER */
  {  246,   -2 }, /* (118) signed ::= PLUS INTEGER */
  {  246,   -2 }, /* (119) signed ::= MINUS INTEGER */
  {  211,   -3 }, /* (120) cmd ::= CREATE TABLE create_table_args */
  {  211,   -3 }, /* (121) cmd ::= CREATE TABLE create_stable_args */
  {  211,   -3 }, /* (122) cmd ::= CREATE STABLE create_stable_args */
  {  211,   -3 }, /* (123) cmd ::= CREATE TABLE create_table_list */
  {  249,   -1 }, /* (124) create_table_list ::= create_from_stable */
  {  249,   -2 }, /* (125) create_table_list ::= create_table_list create_from_stable */
  {  247,   -6 }, /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  248,  -10 }, /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  250,  -10 }, /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  247,   -5 }, /* (129) create_table_args ::= ifnotexists ids cpxName AS select */
  {  251,   -3 }, /* (130) columnlist ::= columnlist COMMA column */
  {  251,   -1 }, /* (131) columnlist ::= column */
  {  253,   -2 }, /* (132) column ::= ids typename */
  {  230,   -3 }, /* (133) tagitemlist ::= tagitemlist COMMA tagitem */
  {  230,   -1 }, /* (134) tagitemlist ::= tagitem */
  {  254,   -1 }, /* (135) tagitem ::= INTEGER */
  {  254,   -1 }, /* (136) tagitem ::= FLOAT */
  {  254,   -1 }, /* (137) tagitem ::= STRING */
  {  254,   -1 }, /* (138) tagitem ::= BOOL */
  {  254,   -1 }, /* (139) tagitem ::= NULL */
  {  254,   -2 }, /* (140) tagitem ::= MINUS INTEGER */
  {  254,   -2 }, /* (141) tagitem ::= MINUS FLOAT */
  {  254,   -2 }, /* (142) tagitem ::= PLUS INTEGER */
  {  254,   -2 }, /* (143) tagitem ::= PLUS FLOAT */
  {  252,  -12 }, /* (144) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  266,   -1 }, /* (145) union ::= select */
  {  266,   -3 }, /* (146) union ::= LP union RP */
  {  266,   -4 }, /* (147) union ::= union UNION ALL select */
  {  266,   -6 }, /* (148) union ::= union UNION ALL LP select RP */
  {  211,   -1 }, /* (149) cmd ::= union */
  {  252,   -2 }, /* (150) select ::= SELECT selcollist */
  {  267,   -2 }, /* (151) sclp ::= selcollist COMMA */
  {  267,    0 }, /* (152) sclp ::= */
  {  255,   -3 }, /* (153) selcollist ::= sclp expr as */
  {  255,   -2 }, /* (154) selcollist ::= sclp STAR */
  {  269,   -2 }, /* (155) as ::= AS ids */
  {  269,   -1 }, /* (156) as ::= ids */
  {  269,    0 }, /* (157) as ::= */
  {  256,   -2 }, /* (158) from ::= FROM tablelist */
  {  270,   -2 }, /* (159) tablelist ::= ids cpxName */
  {  270,   -3 }, /* (160) tablelist ::= ids cpxName ids */
  {  270,   -4 }, /* (161) tablelist ::= tablelist COMMA ids cpxName */
  {  270,   -5 }, /* (162) tablelist ::= tablelist COMMA ids cpxName ids */
  {  271,   -1 }, /* (163) tmvar ::= VARIABLE */
  {  258,   -4 }, /* (164) interval_opt ::= INTERVAL LP tmvar RP */
  {  258,   -6 }, /* (165) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  258,    0 }, /* (166) interval_opt ::= */
  {  259,    0 }, /* (167) fill_opt ::= */
  {  259,   -6 }, /* (168) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  259,   -4 }, /* (169) fill_opt ::= FILL LP ID RP */
  {  260,   -4 }, /* (170) sliding_opt ::= SLIDING LP tmvar RP */
  {  260,    0 }, /* (171) sliding_opt ::= */
  {  262,    0 }, /* (172) orderby_opt ::= */
  {  262,   -3 }, /* (173) orderby_opt ::= ORDER BY sortlist */
  {  272,   -4 }, /* (174) sortlist ::= sortlist COMMA item sortorder */
  {  272,   -2 }, /* (175) sortlist ::= item sortorder */
  {  274,   -2 }, /* (176) item ::= ids cpxName */
  {  275,   -1 }, /* (177) sortorder ::= ASC */
  {  275,   -1 }, /* (178) sortorder ::= DESC */
  {  275,    0 }, /* (179) sortorder ::= */
  {  261,    0 }, /* (180) groupby_opt ::= */
  {  261,   -3 }, /* (181) groupby_opt ::= GROUP BY grouplist */
  {  276,   -3 }, /* (182) grouplist ::= grouplist COMMA item */
  {  276,   -1 }, /* (183) grouplist ::= item */
  {  263,    0 }, /* (184) having_opt ::= */
  {  263,   -2 }, /* (185) having_opt ::= HAVING expr */
  {  265,    0 }, /* (186) limit_opt ::= */
  {  265,   -2 }, /* (187) limit_opt ::= LIMIT signed */
  {  265,   -4 }, /* (188) limit_opt ::= LIMIT signed OFFSET signed */
  {  265,   -4 }, /* (189) limit_opt ::= LIMIT signed COMMA signed */
  {  264,    0 }, /* (190) slimit_opt ::= */
  {  264,   -2 }, /* (191) slimit_opt ::= SLIMIT signed */
  {  264,   -4 }, /* (192) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  264,   -4 }, /* (193) slimit_opt ::= SLIMIT signed COMMA signed */
  {  257,    0 }, /* (194) where_opt ::= */
  {  257,   -2 }, /* (195) where_opt ::= WHERE expr */
  {  268,   -3 }, /* (196) expr ::= LP expr RP */
  {  268,   -1 }, /* (197) expr ::= ID */
  {  268,   -3 }, /* (198) expr ::= ID DOT ID */
  {  268,   -3 }, /* (199) expr ::= ID DOT STAR */
  {  268,   -1 }, /* (200) expr ::= INTEGER */
  {  268,   -2 }, /* (201) expr ::= MINUS INTEGER */
  {  268,   -2 }, /* (202) expr ::= PLUS INTEGER */
  {  268,   -1 }, /* (203) expr ::= FLOAT */
  {  268,   -2 }, /* (204) expr ::= MINUS FLOAT */
  {  268,   -2 }, /* (205) expr ::= PLUS FLOAT */
  {  268,   -1 }, /* (206) expr ::= STRING */
  {  268,   -1 }, /* (207) expr ::= NOW */
  {  268,   -1 }, /* (208) expr ::= VARIABLE */
  {  268,   -1 }, /* (209) expr ::= BOOL */
  {  268,   -4 }, /* (210) expr ::= ID LP exprlist RP */
  {  268,   -4 }, /* (211) expr ::= ID LP STAR RP */
  {  268,   -3 }, /* (212) expr ::= expr IS NULL */
  {  268,   -4 }, /* (213) expr ::= expr IS NOT NULL */
  {  268,   -3 }, /* (214) expr ::= expr LT expr */
  {  268,   -3 }, /* (215) expr ::= expr GT expr */
  {  268,   -3 }, /* (216) expr ::= expr LE expr */
  {  268,   -3 }, /* (217) expr ::= expr GE expr */
  {  268,   -3 }, /* (218) expr ::= expr NE expr */
  {  268,   -3 }, /* (219) expr ::= expr EQ expr */
  {  268,   -3 }, /* (220) expr ::= expr AND expr */
  {  268,   -3 }, /* (221) expr ::= expr OR expr */
  {  268,   -3 }, /* (222) expr ::= expr PLUS expr */
  {  268,   -3 }, /* (223) expr ::= expr MINUS expr */
  {  268,   -3 }, /* (224) expr ::= expr STAR expr */
  {  268,   -3 }, /* (225) expr ::= expr SLASH expr */
  {  268,   -3 }, /* (226) expr ::= expr REM expr */
  {  268,   -3 }, /* (227) expr ::= expr LIKE expr */
  {  268,   -5 }, /* (228) expr ::= expr IN LP exprlist RP */
  {  277,   -3 }, /* (229) exprlist ::= exprlist COMMA expritem */
  {  277,   -1 }, /* (230) exprlist ::= expritem */
  {  278,   -1 }, /* (231) expritem ::= expr */
  {  278,    0 }, /* (232) expritem ::= */
  {  211,   -3 }, /* (233) cmd ::= RESET QUERY CACHE */
  {  211,   -7 }, /* (234) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  211,   -7 }, /* (235) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  211,   -7 }, /* (236) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  211,   -7 }, /* (237) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  211,   -8 }, /* (238) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  211,   -9 }, /* (239) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  211,   -7 }, /* (240) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  211,   -7 }, /* (241) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  211,   -7 }, /* (242) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  211,   -7 }, /* (243) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  211,   -8 }, /* (244) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  211,   -3 }, /* (245) cmd ::= KILL CONNECTION INTEGER */
  {  211,   -5 }, /* (246) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  211,   -5 }, /* (247) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};
        setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy256, &t);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy277);}
        break;
      case 43: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy277);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy277);}
        break;
      case 52: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{
        setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy256, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy277.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy277.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy277.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy277.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy277.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy277.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy277.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy277.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy277.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy277 = yylhsminor.yy277;
        break;
      case 73: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy135 = yymsp[0].minor.yy135; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy256);}
        break;
      case 89: /* db_optr ::= db_optr cache */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 90: /* db_optr ::= db_optr replica */
      case 105: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==105);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 91: /* db_optr ::= db_optr quorum */
      case 106: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==106);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 92: /* db_optr ::= db_optr days */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 93: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 94: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 95: /* db_optr ::= db_optr blocks */
      case 108: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==108);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 96: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 97: /* db_optr ::= db_optr wal */
      case 110: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==110);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 98: /* db_optr ::= db_optr fsync */
      case 111: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==111);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 99: /* db_optr ::= db_optr comp */
      case 109: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==109);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 100: /* db_optr ::= db_optr prec */
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 101: /* db_optr ::= db_optr keep */
      case 107: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==107);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.keep = yymsp[0].minor.yy135; }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 102: /* db_optr ::= db_optr update */
      case 112: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==112);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 103: /* db_optr ::= db_optr cachelast */
      case 113: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==113);
{ yylhsminor.yy256 = yymsp[-1].minor.yy256; yylhsminor.yy256.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 104: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy256);}
        break;
      case 114: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy181, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy181 = yylhsminor.yy181;
        break;
      case 115: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy531 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy531;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy181 = yylhsminor.yy181;
        break;
      case 116: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy181, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 117: /* signed ::= INTEGER */
{ yylhsminor.yy531 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy531 = yylhsminor.yy531;
        break;
      case 118: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy531 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 119: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy531 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 123: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy38;}
        break;
      case 124: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy78);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy38 = pCreateTable;
}
  yymsp[0].minor.yy38 = yylhsminor.yy38;
        break;
      case 125: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy38->childTableInfo, &yymsp[0].minor.yy78);
  yylhsminor.yy38 = yymsp[-1].minor.yy38;
}
  yymsp[-1].minor.yy38 = yylhsminor.yy38;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy38 = tSetCreateSqlElems(yymsp[-1].minor.yy135, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy38 = yylhsminor.yy38;
        break;
      case 127: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy38 = tSetCreateSqlElems(yymsp[-5].minor.yy135, yymsp[-1].minor.yy135, NULL, TSQL_CREATE_STABLE);
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
  yylhsminor.yy78 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy135, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy78 = yylhsminor.yy78;
        break;
      case 129: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy38 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy30, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy38, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy38 = yylhsminor.yy38;
        break;
      case 130: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy135, &yymsp[0].minor.yy181); yylhsminor.yy135 = yymsp[-2].minor.yy135;  }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 131: /* columnlist ::= column */
{yylhsminor.yy135 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy135, &yymsp[0].minor.yy181);}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 132: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy181, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy181);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 133: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy135 = tVariantListAppend(yymsp[-2].minor.yy135, &yymsp[0].minor.yy308, -1);    }
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 134: /* tagitemlist ::= tagitem */
{ yylhsminor.yy135 = tVariantListAppend(NULL, &yymsp[0].minor.yy308, -1); }
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 135: /* tagitem ::= INTEGER */
      case 136: /* tagitem ::= FLOAT */ yytestcase(yyruleno==136);
      case 137: /* tagitem ::= STRING */ yytestcase(yyruleno==137);
      case 138: /* tagitem ::= BOOL */ yytestcase(yyruleno==138);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy308, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy308 = yylhsminor.yy308;
        break;
      case 139: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy308, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy308 = yylhsminor.yy308;
        break;
      case 140: /* tagitem ::= MINUS INTEGER */
      case 141: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==141);
      case 142: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==142);
      case 143: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==143);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy308, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy308 = yylhsminor.yy308;
        break;
      case 144: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy30 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy266, yymsp[-9].minor.yy135, yymsp[-8].minor.yy316, yymsp[-4].minor.yy135, yymsp[-3].minor.yy135, &yymsp[-7].minor.yy160, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy135, &yymsp[0].minor.yy126, &yymsp[-1].minor.yy126);
}
  yymsp[-11].minor.yy30 = yylhsminor.yy30;
        break;
      case 145: /* union ::= select */
{ yylhsminor.yy153 = setSubclause(NULL, yymsp[0].minor.yy30); }
  yymsp[0].minor.yy153 = yylhsminor.yy153;
        break;
      case 146: /* union ::= LP union RP */
{ yymsp[-2].minor.yy153 = yymsp[-1].minor.yy153; }
        break;
      case 147: /* union ::= union UNION ALL select */
{ yylhsminor.yy153 = appendSelectClause(yymsp[-3].minor.yy153, yymsp[0].minor.yy30); }
  yymsp[-3].minor.yy153 = yylhsminor.yy153;
        break;
      case 148: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy153 = appendSelectClause(yymsp[-5].minor.yy153, yymsp[-1].minor.yy30); }
  yymsp[-5].minor.yy153 = yylhsminor.yy153;
        break;
      case 149: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy153, NULL, TSDB_SQL_SELECT); }
        break;
      case 150: /* select ::= SELECT selcollist */
{
  yylhsminor.yy30 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy266, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy30 = yylhsminor.yy30;
        break;
      case 151: /* sclp ::= selcollist COMMA */
{yylhsminor.yy266 = yymsp[-1].minor.yy266;}
  yymsp[-1].minor.yy266 = yylhsminor.yy266;
        break;
      case 152: /* sclp ::= */
{yymsp[1].minor.yy266 = 0;}
        break;
      case 153: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy266 = tSqlExprListAppend(yymsp[-2].minor.yy266, yymsp[-1].minor.yy316, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy266 = yylhsminor.yy266;
        break;
      case 154: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy266 = tSqlExprListAppend(yymsp[-1].minor.yy266, pNode, 0);
}
  yymsp[-1].minor.yy266 = yylhsminor.yy266;
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
{yymsp[-1].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 159: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy135 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy135 = tVariantListAppendToken(yylhsminor.yy135, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 160: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy135 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy135 = tVariantListAppendToken(yylhsminor.yy135, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 161: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy135 = tVariantListAppendToken(yymsp[-3].minor.yy135, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy135 = tVariantListAppendToken(yylhsminor.yy135, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 162: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy135 = tVariantListAppendToken(yymsp[-4].minor.yy135, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy135 = tVariantListAppendToken(yylhsminor.yy135, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy135 = yylhsminor.yy135;
        break;
      case 163: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 164: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy160.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy160.offset.n = 0; yymsp[-3].minor.yy160.offset.z = NULL; yymsp[-3].minor.yy160.offset.type = 0;}
        break;
      case 165: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy160.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy160.offset = yymsp[-1].minor.yy0;}
        break;
      case 166: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy160, 0, sizeof(yymsp[1].minor.yy160));}
        break;
      case 167: /* fill_opt ::= */
{yymsp[1].minor.yy135 = 0;     }
        break;
      case 168: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy135, &A, -1, 0);
    yymsp[-5].minor.yy135 = yymsp[-1].minor.yy135;
}
        break;
      case 169: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy135 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 170: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 171: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 172: /* orderby_opt ::= */
{yymsp[1].minor.yy135 = 0;}
        break;
      case 173: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 174: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy135 = tVariantListAppend(yymsp[-3].minor.yy135, &yymsp[-1].minor.yy308, yymsp[0].minor.yy130);
}
  yymsp[-3].minor.yy135 = yylhsminor.yy135;
        break;
      case 175: /* sortlist ::= item sortorder */
{
  yylhsminor.yy135 = tVariantListAppend(NULL, &yymsp[-1].minor.yy308, yymsp[0].minor.yy130);
}
  yymsp[-1].minor.yy135 = yylhsminor.yy135;
        break;
      case 176: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy308, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy308 = yylhsminor.yy308;
        break;
      case 177: /* sortorder ::= ASC */
{ yymsp[0].minor.yy130 = TSDB_ORDER_ASC; }
        break;
      case 178: /* sortorder ::= DESC */
{ yymsp[0].minor.yy130 = TSDB_ORDER_DESC;}
        break;
      case 179: /* sortorder ::= */
{ yymsp[1].minor.yy130 = TSDB_ORDER_ASC; }
        break;
      case 180: /* groupby_opt ::= */
{ yymsp[1].minor.yy135 = 0;}
        break;
      case 181: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy135 = yymsp[0].minor.yy135;}
        break;
      case 182: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy135 = tVariantListAppend(yymsp[-2].minor.yy135, &yymsp[0].minor.yy308, -1);
}
  yymsp[-2].minor.yy135 = yylhsminor.yy135;
        break;
      case 183: /* grouplist ::= item */
{
  yylhsminor.yy135 = tVariantListAppend(NULL, &yymsp[0].minor.yy308, -1);
}
  yymsp[0].minor.yy135 = yylhsminor.yy135;
        break;
      case 184: /* having_opt ::= */
      case 194: /* where_opt ::= */ yytestcase(yyruleno==194);
      case 232: /* expritem ::= */ yytestcase(yyruleno==232);
{yymsp[1].minor.yy316 = 0;}
        break;
      case 185: /* having_opt ::= HAVING expr */
      case 195: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==195);
{yymsp[-1].minor.yy316 = yymsp[0].minor.yy316;}
        break;
      case 186: /* limit_opt ::= */
      case 190: /* slimit_opt ::= */ yytestcase(yyruleno==190);
{yymsp[1].minor.yy126.limit = -1; yymsp[1].minor.yy126.offset = 0;}
        break;
      case 187: /* limit_opt ::= LIMIT signed */
      case 191: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==191);
{yymsp[-1].minor.yy126.limit = yymsp[0].minor.yy531;  yymsp[-1].minor.yy126.offset = 0;}
        break;
      case 188: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy126.limit = yymsp[-2].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[0].minor.yy531;}
        break;
      case 189: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy126.limit = yymsp[0].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[-2].minor.yy531;}
        break;
      case 192: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy126.limit = yymsp[-2].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[0].minor.yy531;}
        break;
      case 193: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy126.limit = yymsp[0].minor.yy531;  yymsp[-3].minor.yy126.offset = yymsp[-2].minor.yy531;}
        break;
      case 196: /* expr ::= LP expr RP */
{yylhsminor.yy316 = yymsp[-1].minor.yy316; yylhsminor.yy316->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy316->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 197: /* expr ::= ID */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 198: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 199: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 200: /* expr ::= INTEGER */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 201: /* expr ::= MINUS INTEGER */
      case 202: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==202);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy316 = yylhsminor.yy316;
        break;
      case 203: /* expr ::= FLOAT */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 204: /* expr ::= MINUS FLOAT */
      case 205: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==205);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy316 = yylhsminor.yy316;
        break;
      case 206: /* expr ::= STRING */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 207: /* expr ::= NOW */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 208: /* expr ::= VARIABLE */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 209: /* expr ::= BOOL */
{ yylhsminor.yy316 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 210: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy316 = tSqlExprCreateFunction(yymsp[-1].minor.yy266, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy316 = yylhsminor.yy316;
        break;
      case 211: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy316 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy316 = yylhsminor.yy316;
        break;
      case 212: /* expr ::= expr IS NULL */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 213: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-3].minor.yy316, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy316 = yylhsminor.yy316;
        break;
      case 214: /* expr ::= expr LT expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_LT);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 215: /* expr ::= expr GT expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_GT);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 216: /* expr ::= expr LE expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_LE);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 217: /* expr ::= expr GE expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_GE);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 218: /* expr ::= expr NE expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_NE);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 219: /* expr ::= expr EQ expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_EQ);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 220: /* expr ::= expr AND expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_AND);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 221: /* expr ::= expr OR expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_OR); }
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 222: /* expr ::= expr PLUS expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_PLUS);  }
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 223: /* expr ::= expr MINUS expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_MINUS); }
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 224: /* expr ::= expr STAR expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_STAR);  }
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 225: /* expr ::= expr SLASH expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_DIVIDE);}
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 226: /* expr ::= expr REM expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_REM);   }
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 227: /* expr ::= expr LIKE expr */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-2].minor.yy316, yymsp[0].minor.yy316, TK_LIKE);  }
  yymsp[-2].minor.yy316 = yylhsminor.yy316;
        break;
      case 228: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy316 = tSqlExprCreate(yymsp[-4].minor.yy316, (tSQLExpr*)yymsp[-1].minor.yy266, TK_IN); }
  yymsp[-4].minor.yy316 = yylhsminor.yy316;
        break;
      case 229: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy266 = tSqlExprListAppend(yymsp[-2].minor.yy266,yymsp[0].minor.yy316,0);}
  yymsp[-2].minor.yy266 = yylhsminor.yy266;
        break;
      case 230: /* exprlist ::= expritem */
{yylhsminor.yy266 = tSqlExprListAppend(0,yymsp[0].minor.yy316,0);}
  yymsp[0].minor.yy266 = yylhsminor.yy266;
        break;
      case 231: /* expritem ::= expr */
{yylhsminor.yy316 = yymsp[0].minor.yy316;}
  yymsp[0].minor.yy316 = yylhsminor.yy316;
        break;
      case 233: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 234: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 235: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 237: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
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

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 239: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy308, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 240: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 241: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 242: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy135, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 243: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
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

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
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
