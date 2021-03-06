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
#define YYNOCODE 256
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateAcctInfo yy1;
  SCreateTableSQL* yy8;
  tSQLExprList* yy9;
  SLimitVal yy24;
  SSubclauseInfo* yy63;
  SArray* yy141;
  tSQLExpr* yy220;
  SQuerySQL* yy234;
  SCreatedTableInfo yy306;
  SCreateDbInfo yy322;
  tVariant yy326;
  SIntervalVal yy340;
  TAOS_FIELD yy403;
  int64_t yy429;
  int yy502;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             283
#define YYNRULE              250
#define YYNTOKEN             184
#define YY_MAX_SHIFT         282
#define YY_MIN_SHIFTREDUCE   463
#define YY_MAX_SHIFTREDUCE   712
#define YY_ERROR_ACTION      713
#define YY_ACCEPT_ACTION     714
#define YY_NO_ACTION         715
#define YY_MIN_REDUCE        716
#define YY_MAX_REDUCE        965
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
#define YY_ACTTAB_COUNT (614)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   872,  507,  159,  714,  282,  159,   15,  580,  183,  508,
 /*    10 */   158,  186,  948,   41,   42,  947,   43,   44,   26,  163,
 /*    20 */   191,   35,  507,  507,  232,   47,   45,   49,   46,  944,
 /*    30 */   508,  508,  850,   40,   39,  257,  256,   38,   37,   36,
 /*    40 */    41,   42,  118,   43,   44,  861,  943,  191,   35,  179,
 /*    50 */   280,  232,   47,   45,   49,   46,  181,  869,  847,  215,
 /*    60 */    40,   39,  957,  123,   38,   37,   36,  464,  465,  466,
 /*    70 */   467,  468,  469,  470,  471,  472,  473,  474,  475,  281,
 /*    80 */    91,  196,  205,   41,   42,  267,   43,   44,  839,  247,
 /*    90 */   191,   35,  159,  942,  232,   47,   45,   49,   46,  123,
 /*   100 */   220,  185,  948,   40,   39,  850,   62,   38,   37,   36,
 /*   110 */    20,  245,  275,  274,  244,  243,  242,  273,  241,  272,
 /*   120 */   271,  270,  240,  269,  268,  901,  233,  227,  817,  661,
 /*   130 */   805,  806,  807,  808,  809,  810,  811,  812,  813,  814,
 /*   140 */   815,  816,  818,  819,   42,  208,   43,   44,  175,  276,
 /*   150 */   191,   35,  212,  211,  232,   47,   45,   49,   46,  229,
 /*   160 */   861,   67,   21,   40,   39,  247,   72,   38,   37,   36,
 /*   170 */    32,   43,   44,  850,  180,  191,   35,  123,   70,  232,
 /*   180 */    47,   45,   49,   46,   16,   38,   37,   36,   40,   39,
 /*   190 */    69,  123,   38,   37,   36,  190,  674,  200,  841,  665,
 /*   200 */   167,  668,   63,  671,   26,   26,  168,  836,  837,   25,
 /*   210 */   840,  103,  102,  166,  190,  674,  176,   26,  665,  677,
 /*   220 */   668,  617,  671,   20,   61,  275,  274,  187,  188,  848,
 /*   230 */   273,  231,  272,  271,  270,  601,  269,  268,  598,  900,
 /*   240 */   599,   21,  600,  194,  846,  847,  187,  188,  823,   32,
 /*   250 */   267,  821,  822,   68,  838,  195,  824,  847,  826,  827,
 /*   260 */   825,  758,  828,  829,  147,  614,  202,  203,  199,   56,
 /*   270 */   214,   22,   47,   45,   49,   46,  767,  174,  161,  147,
 /*   280 */    40,   39,   89,   93,   38,   37,   36,   57,   83,   98,
 /*   290 */   101,   92,    3,  137,  197,   48,  621,   95,   29,   78,
 /*   300 */    74,   77,  218,  153,  149,   26,   26,   27,  673,  151,
 /*   310 */   106,  105,  104,   26,   48,   40,   39,   26,  162,   38,
 /*   320 */    37,   36,  663,  672,   10,  642,  643,  673,   71,  133,
 /*   330 */   279,  278,  110,  759,  201,  189,  147,  254,  253,  116,
 /*   340 */   164,  602,  672,  250,  251,  847,  847,   32,  609,  165,
 /*   350 */   667,  255,  670,  847,  629,  259,  217,  847,  664,  120,
 /*   360 */   198,  633,  634,  249,  693,  675,   52,   18,   53,   17,
 /*   370 */    17,  666,  171,  669,  590,  235,  591,    4,  172,   27,
 /*   380 */    27,   52,   82,   81,  100,   99,   12,   11,   54,   88,
 /*   390 */    87,   59,  605,  579,  606,  115,  113,   14,   13,  603,
 /*   400 */   170,  604,  849,  157,  169,  160,  911,  910,  192,  907,
 /*   410 */   906,  193,  258,  117,  871,   33,  893,  878,  880,  863,
 /*   420 */   119,  892,  134,  135,  132,  136,   32,  769,  239,  155,
 /*   430 */   216,   30,  248,  766,  114,   58,  962,  628,   79,  961,
 /*   440 */    55,   50,  959,  138,  221,  252,  956,  860,  182,  124,
 /*   450 */   125,  225,  126,   85,  955,  230,  953,  139,  787,   31,
 /*   460 */   228,  226,  128,   28,  224,  156,  756,   94,  754,   96,
 /*   470 */   129,   97,  752,  751,  204,  148,  749,  748,  222,  747,
 /*   480 */   746,  745,  150,  152,  742,  740,  738,  736,  734,  154,
 /*   490 */    34,  219,   64,   65,  894,   90,  260,  261,  262,  263,
 /*   500 */   264,  177,  237,  265,  238,  266,  277,  712,  178,   75,
 /*   510 */   206,  173,  207,  711,  209,  210,  710,  698,  750,  213,
 /*   520 */   611,  107,  142,  788,    2,  144,  140,  141,  143,  145,
 /*   530 */   744,  146,  108,  109,    1,  845,  743,  217,  735,   60,
 /*   540 */   130,  131,  127,  234,    6,   66,  630,  121,  184,   23,
 /*   550 */   223,    7,  635,  122,    8,  676,    5,    9,   24,   19,
 /*   560 */    73,  236,  678,  548,   71,  544,  542,  541,  540,  537,
 /*   570 */   511,  246,   76,   27,   80,   51,  582,  581,  578,   84,
 /*   580 */   532,  530,  522,  528,  524,  526,  520,  518,  550,  549,
 /*   590 */   547,  546,  545,  543,   86,  539,  538,   52,  509,  479,
 /*   600 */   477,  716,  715,  715,  715,  715,  715,  715,  715,  715,
 /*   610 */   111,  715,  715,  112,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   188,    1,  244,  185,  186,  244,  244,    5,  205,    9,
 /*    10 */   244,  253,  254,   13,   14,  254,   16,   17,  188,  244,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  244,
 /*    30 */     9,    9,  229,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  188,   16,   17,  227,  244,   20,   21,  187,
 /*    50 */   188,   24,   25,   26,   27,   28,  226,  245,  228,  241,
 /*    60 */    33,   34,  229,  188,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    74,  205,   60,   13,   14,   79,   16,   17,    0,   77,
 /*    90 */    20,   21,  244,  244,   24,   25,   26,   27,   28,  188,
 /*   100 */   246,  253,  254,   33,   34,  229,  106,   37,   38,   39,
 /*   110 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   120 */    96,   97,   98,   99,  100,  250,   15,  252,  204,  102,
 /*   130 */   206,  207,  208,  209,  210,  211,  212,  213,  214,  215,
 /*   140 */   216,  217,  218,  219,   14,  131,   16,   17,  244,  205,
 /*   150 */    20,   21,  138,  139,   24,   25,   26,   27,   28,  248,
 /*   160 */   227,  250,  101,   33,   34,   77,  193,   37,   38,   39,
 /*   170 */   109,   16,   17,  229,  241,   20,   21,  188,  193,   24,
 /*   180 */    25,   26,   27,   28,   44,   37,   38,   39,   33,   34,
 /*   190 */   230,  188,   37,   38,   39,    1,    2,  188,  225,    5,
 /*   200 */    60,    7,  242,    9,  188,  188,   66,  222,  223,  224,
 /*   210 */   225,   71,   72,   73,    1,    2,  244,  188,    5,  108,
 /*   220 */     7,   37,    9,   86,  193,   88,   89,   33,   34,  220,
 /*   230 */    93,   37,   95,   96,   97,    2,   99,  100,    5,  250,
 /*   240 */     7,  101,    9,  226,  228,  228,   33,   34,  204,  109,
 /*   250 */    79,  207,  208,  250,  223,  226,  212,  228,  214,  215,
 /*   260 */   216,  192,  218,  219,  195,  107,   33,   34,   66,  107,
 /*   270 */   130,  113,   25,   26,   27,   28,  192,  137,  244,  195,
 /*   280 */    33,   34,   61,   62,   37,   38,   39,  125,   67,   68,
 /*   290 */    69,   70,   61,   62,   66,  101,  112,   76,   67,   68,
 /*   300 */    69,   70,  102,   61,   62,  188,  188,  107,  114,   67,
 /*   310 */    68,   69,   70,  188,  101,   33,   34,  188,  244,   37,
 /*   320 */    38,   39,    1,  129,  101,  120,  121,  114,  105,  106,
 /*   330 */    63,   64,   65,  192,  132,   59,  195,  135,  136,  101,
 /*   340 */   244,  108,  129,  226,  226,  228,  228,  109,  102,  244,
 /*   350 */     5,  226,    7,  228,  102,  226,  110,  228,   37,  107,
 /*   360 */   132,  102,  102,  135,  102,  102,  107,  107,  107,  107,
 /*   370 */   107,    5,  244,    7,  102,  102,  102,  101,  244,  107,
 /*   380 */   107,  107,  133,  134,   74,   75,  133,  134,  127,  133,
 /*   390 */   134,  101,    5,  103,    7,   61,   62,  133,  134,    5,
 /*   400 */   244,    7,  229,  244,  244,  244,  221,  221,  221,  221,
 /*   410 */   221,  221,  221,  188,  188,  243,  251,  188,  188,  227,
 /*   420 */   188,  251,  188,  188,  231,  188,  109,  188,  188,  188,
 /*   430 */   227,  188,  188,  188,   59,  124,  188,  114,  188,  188,
 /*   440 */   126,  123,  188,  188,  247,  188,  188,  240,  247,  239,
 /*   450 */   238,  247,  237,  188,  188,  118,  188,  188,  188,  188,
 /*   460 */   122,  117,  235,  188,  116,  188,  188,  188,  188,  188,
 /*   470 */   234,  188,  188,  188,  188,  188,  188,  188,  115,  188,
 /*   480 */   188,  188,  188,  188,  188,  188,  188,  188,  188,  188,
 /*   490 */   128,  189,  189,  189,  189,   85,   84,   49,   81,   83,
 /*   500 */    53,  189,  189,   82,  189,   80,   77,    5,  189,  193,
 /*   510 */   140,  189,    5,    5,  140,    5,    5,   87,  189,  131,
 /*   520 */   102,  190,  197,  203,  191,  198,  202,  201,  200,  199,
 /*   530 */   189,  196,  190,  190,  194,  227,  189,  110,  189,  111,
 /*   540 */   233,  232,  236,  104,  101,  107,  102,  101,    1,  107,
 /*   550 */   101,  119,  102,  101,  119,  102,  101,  101,  107,  101,
 /*   560 */    74,  104,  108,    9,  105,    5,    5,    5,    5,    5,
 /*   570 */    78,   15,   74,  107,  134,   16,    5,    5,  102,  134,
 /*   580 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   590 */     5,    5,    5,    5,  134,    5,    5,  107,   78,   59,
 /*   600 */    58,    0,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   610 */    21,  255,  255,   21,  255,  255,  255,  255,  255,  255,
 /*   620 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   630 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   640 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   650 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   660 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   670 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   680 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   690 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   700 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   710 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   720 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   730 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   740 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   750 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   760 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   770 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   780 */   255,  255,  255,  255,  255,  255,  255,  255,  255,  255,
 /*   790 */   255,  255,  255,  255,  255,  255,  255,  255,
};
#define YY_SHIFT_COUNT    (282)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (601)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   140,   24,  137,   12,  194,  213,   21,   21,   21,   21,
 /*    10 */    21,   21,   21,   21,   21,    0,   22,  213,  233,  233,
 /*    20 */   233,   61,   21,   21,   21,   88,   21,   21,    6,   12,
 /*    30 */   171,  171,  614,  213,  213,  213,  213,  213,  213,  213,
 /*    40 */   213,  213,  213,  213,  213,  213,  213,  213,  213,  213,
 /*    50 */   213,  233,  233,    2,    2,    2,    2,    2,    2,    2,
 /*    60 */   238,   21,   21,  184,   21,   21,   21,  205,  205,  158,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,  317,  375,  375,  375,
 /*   120 */   323,  323,  323,  375,  311,  314,  318,  337,  338,  344,
 /*   130 */   348,  363,  362,  317,  375,  375,  375,   12,  375,  375,
 /*   140 */   410,  412,  448,  417,  416,  447,  421,  425,  375,  429,
 /*   150 */   375,  429,  375,  429,  375,  614,  614,   27,   70,   70,
 /*   160 */    70,  130,  155,  247,  247,  247,  221,  231,  242,  282,
 /*   170 */   282,  282,  282,  202,   14,  148,  148,  223,  228,  267,
 /*   180 */   246,  200,  252,  259,  260,  262,  263,  345,  366,  321,
 /*   190 */   276,  111,  261,  162,  272,  273,  274,  249,  253,  256,
 /*   200 */   290,  264,  387,  394,  310,  334,  502,  370,  507,  508,
 /*   210 */   374,  510,  511,  430,  388,  427,  418,  428,  439,  443,
 /*   220 */   438,  444,  446,  547,  449,  450,  452,  442,  432,  451,
 /*   230 */   435,  453,  455,  454,  456,  439,  458,  457,  459,  486,
 /*   240 */   554,  560,  561,  562,  563,  564,  492,  556,  498,  440,
 /*   250 */   466,  466,  559,  445,  460,  466,  571,  572,  476,  466,
 /*   260 */   575,  576,  577,  578,  579,  580,  581,  582,  583,  584,
 /*   270 */   585,  586,  587,  588,  590,  591,  490,  520,  589,  592,
 /*   280 */   540,  542,  601,
};
#define YY_REDUCE_COUNT (156)
#define YY_REDUCE_MIN   (-242)
#define YY_REDUCE_MAX   (349)
static const short yy_reduce_ofst[] = {
 /*     0 */  -182,  -76,   44,  -15, -242, -152, -170, -125,  -89,   17,
 /*    10 */    29,  117,  118,  125,  129, -188, -138, -239, -197, -124,
 /*    20 */   -56,  -67, -146,  -11,    3,  -27,    9,   16,   69,   31,
 /*    30 */    84,  141,  -40, -238, -234, -225, -215, -198, -151,  -96,
 /*    40 */   -28,   34,   74,   96,  105,  128,  134,  156,  159,  160,
 /*    50 */   161, -167,  173,  185,  186,  187,  188,  189,  190,  191,
 /*    60 */   192,  225,  226,  172,  229,  230,  232,  165,  170,  193,
 /*    70 */   234,  235,  237,  239,  240,  241,  243,  244,  245,  248,
 /*    80 */   250,  251,  254,  255,  257,  258,  265,  266,  268,  269,
 /*    90 */   270,  271,  275,  277,  278,  279,  280,  281,  283,  284,
 /*   100 */   285,  286,  287,  288,  289,  291,  292,  293,  294,  295,
 /*   110 */   296,  297,  298,  299,  300,  301,  203,  302,  303,  304,
 /*   120 */   197,  201,  204,  305,  207,  210,  212,  215,  306,  227,
 /*   130 */   236,  307,  309,  308,  312,  313,  315,  316,  319,  322,
 /*   140 */   320,  324,  326,  325,  328,  327,  330,  335,  329,  331,
 /*   150 */   341,  342,  347,  343,  349,  340,  333,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   713,  768,  757,  765,  950,  950,  713,  713,  713,  713,
 /*    10 */   713,  713,  713,  713,  713,  873,  731,  950,  713,  713,
 /*    20 */   713,  713,  713,  713,  713,  765,  713,  713,  770,  765,
 /*    30 */   770,  770,  868,  713,  713,  713,  713,  713,  713,  713,
 /*    40 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*    50 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*    60 */   713,  713,  713,  875,  877,  879,  713,  897,  897,  866,
 /*    70 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*    80 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*    90 */   713,  713,  713,  713,  755,  713,  753,  713,  713,  713,
 /*   100 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*   110 */   741,  713,  713,  713,  713,  713,  713,  733,  733,  733,
 /*   120 */   713,  713,  713,  733,  904,  908,  902,  890,  898,  889,
 /*   130 */   885,  884,  912,  713,  733,  733,  733,  765,  733,  733,
 /*   140 */   786,  784,  782,  774,  780,  776,  778,  772,  733,  763,
 /*   150 */   733,  763,  733,  763,  733,  804,  820,  713,  913,  949,
 /*   160 */   903,  939,  938,  945,  937,  936,  713,  713,  713,  932,
 /*   170 */   933,  935,  934,  713,  713,  941,  940,  713,  713,  713,
 /*   180 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*   190 */   915,  713,  909,  905,  713,  713,  713,  713,  713,  713,
 /*   200 */   830,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*   210 */   713,  713,  713,  713,  713,  865,  713,  713,  713,  713,
 /*   220 */   876,  713,  713,  713,  713,  713,  713,  899,  713,  891,
 /*   230 */   713,  713,  713,  713,  713,  842,  713,  713,  713,  713,
 /*   240 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*   250 */   960,  958,  713,  713,  713,  954,  713,  713,  713,  952,
 /*   260 */   713,  713,  713,  713,  713,  713,  713,  713,  713,  713,
 /*   270 */   713,  713,  713,  713,  713,  713,  789,  713,  739,  737,
 /*   280 */   713,  729,  713,
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
    0,  /*   DISTINCT => nothing */
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
  /*  112 */ "DISTINCT",
  /*  113 */ "FROM",
  /*  114 */ "VARIABLE",
  /*  115 */ "INTERVAL",
  /*  116 */ "FILL",
  /*  117 */ "SLIDING",
  /*  118 */ "ORDER",
  /*  119 */ "BY",
  /*  120 */ "ASC",
  /*  121 */ "DESC",
  /*  122 */ "GROUP",
  /*  123 */ "HAVING",
  /*  124 */ "LIMIT",
  /*  125 */ "OFFSET",
  /*  126 */ "SLIMIT",
  /*  127 */ "SOFFSET",
  /*  128 */ "WHERE",
  /*  129 */ "NOW",
  /*  130 */ "RESET",
  /*  131 */ "QUERY",
  /*  132 */ "ADD",
  /*  133 */ "COLUMN",
  /*  134 */ "TAG",
  /*  135 */ "CHANGE",
  /*  136 */ "SET",
  /*  137 */ "KILL",
  /*  138 */ "CONNECTION",
  /*  139 */ "STREAM",
  /*  140 */ "COLON",
  /*  141 */ "ABORT",
  /*  142 */ "AFTER",
  /*  143 */ "ATTACH",
  /*  144 */ "BEFORE",
  /*  145 */ "BEGIN",
  /*  146 */ "CASCADE",
  /*  147 */ "CLUSTER",
  /*  148 */ "CONFLICT",
  /*  149 */ "COPY",
  /*  150 */ "DEFERRED",
  /*  151 */ "DELIMITERS",
  /*  152 */ "DETACH",
  /*  153 */ "EACH",
  /*  154 */ "END",
  /*  155 */ "EXPLAIN",
  /*  156 */ "FAIL",
  /*  157 */ "FOR",
  /*  158 */ "IGNORE",
  /*  159 */ "IMMEDIATE",
  /*  160 */ "INITIALLY",
  /*  161 */ "INSTEAD",
  /*  162 */ "MATCH",
  /*  163 */ "KEY",
  /*  164 */ "OF",
  /*  165 */ "RAISE",
  /*  166 */ "REPLACE",
  /*  167 */ "RESTRICT",
  /*  168 */ "ROW",
  /*  169 */ "STATEMENT",
  /*  170 */ "TRIGGER",
  /*  171 */ "VIEW",
  /*  172 */ "SEMI",
  /*  173 */ "NONE",
  /*  174 */ "PREV",
  /*  175 */ "LINEAR",
  /*  176 */ "IMPORT",
  /*  177 */ "METRIC",
  /*  178 */ "TBNAME",
  /*  179 */ "JOIN",
  /*  180 */ "METRICS",
  /*  181 */ "INSERT",
  /*  182 */ "INTO",
  /*  183 */ "VALUES",
  /*  184 */ "error",
  /*  185 */ "program",
  /*  186 */ "cmd",
  /*  187 */ "dbPrefix",
  /*  188 */ "ids",
  /*  189 */ "cpxName",
  /*  190 */ "ifexists",
  /*  191 */ "alter_db_optr",
  /*  192 */ "acct_optr",
  /*  193 */ "ifnotexists",
  /*  194 */ "db_optr",
  /*  195 */ "pps",
  /*  196 */ "tseries",
  /*  197 */ "dbs",
  /*  198 */ "streams",
  /*  199 */ "storage",
  /*  200 */ "qtime",
  /*  201 */ "users",
  /*  202 */ "conns",
  /*  203 */ "state",
  /*  204 */ "keep",
  /*  205 */ "tagitemlist",
  /*  206 */ "cache",
  /*  207 */ "replica",
  /*  208 */ "quorum",
  /*  209 */ "days",
  /*  210 */ "minrows",
  /*  211 */ "maxrows",
  /*  212 */ "blocks",
  /*  213 */ "ctime",
  /*  214 */ "wal",
  /*  215 */ "fsync",
  /*  216 */ "comp",
  /*  217 */ "prec",
  /*  218 */ "update",
  /*  219 */ "cachelast",
  /*  220 */ "typename",
  /*  221 */ "signed",
  /*  222 */ "create_table_args",
  /*  223 */ "create_stable_args",
  /*  224 */ "create_table_list",
  /*  225 */ "create_from_stable",
  /*  226 */ "columnlist",
  /*  227 */ "select",
  /*  228 */ "column",
  /*  229 */ "tagitem",
  /*  230 */ "selcollist",
  /*  231 */ "from",
  /*  232 */ "where_opt",
  /*  233 */ "interval_opt",
  /*  234 */ "fill_opt",
  /*  235 */ "sliding_opt",
  /*  236 */ "groupby_opt",
  /*  237 */ "orderby_opt",
  /*  238 */ "having_opt",
  /*  239 */ "slimit_opt",
  /*  240 */ "limit_opt",
  /*  241 */ "union",
  /*  242 */ "sclp",
  /*  243 */ "distinct",
  /*  244 */ "expr",
  /*  245 */ "as",
  /*  246 */ "tablelist",
  /*  247 */ "tmvar",
  /*  248 */ "sortlist",
  /*  249 */ "sortitem",
  /*  250 */ "item",
  /*  251 */ "sortorder",
  /*  252 */ "grouplist",
  /*  253 */ "exprlist",
  /*  254 */ "expritem",
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
 /* 153 */ "selcollist ::= sclp distinct expr as",
 /* 154 */ "selcollist ::= sclp STAR",
 /* 155 */ "as ::= AS ids",
 /* 156 */ "as ::= ids",
 /* 157 */ "as ::=",
 /* 158 */ "distinct ::= DISTINCT",
 /* 159 */ "distinct ::=",
 /* 160 */ "from ::= FROM tablelist",
 /* 161 */ "tablelist ::= ids cpxName",
 /* 162 */ "tablelist ::= ids cpxName ids",
 /* 163 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 164 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 165 */ "tmvar ::= VARIABLE",
 /* 166 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 167 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 168 */ "interval_opt ::=",
 /* 169 */ "fill_opt ::=",
 /* 170 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 171 */ "fill_opt ::= FILL LP ID RP",
 /* 172 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 173 */ "sliding_opt ::=",
 /* 174 */ "orderby_opt ::=",
 /* 175 */ "orderby_opt ::= ORDER BY sortlist",
 /* 176 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 177 */ "sortlist ::= item sortorder",
 /* 178 */ "item ::= ids cpxName",
 /* 179 */ "sortorder ::= ASC",
 /* 180 */ "sortorder ::= DESC",
 /* 181 */ "sortorder ::=",
 /* 182 */ "groupby_opt ::=",
 /* 183 */ "groupby_opt ::= GROUP BY grouplist",
 /* 184 */ "grouplist ::= grouplist COMMA item",
 /* 185 */ "grouplist ::= item",
 /* 186 */ "having_opt ::=",
 /* 187 */ "having_opt ::= HAVING expr",
 /* 188 */ "limit_opt ::=",
 /* 189 */ "limit_opt ::= LIMIT signed",
 /* 190 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 191 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 192 */ "slimit_opt ::=",
 /* 193 */ "slimit_opt ::= SLIMIT signed",
 /* 194 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 195 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 196 */ "where_opt ::=",
 /* 197 */ "where_opt ::= WHERE expr",
 /* 198 */ "expr ::= LP expr RP",
 /* 199 */ "expr ::= ID",
 /* 200 */ "expr ::= ID DOT ID",
 /* 201 */ "expr ::= ID DOT STAR",
 /* 202 */ "expr ::= INTEGER",
 /* 203 */ "expr ::= MINUS INTEGER",
 /* 204 */ "expr ::= PLUS INTEGER",
 /* 205 */ "expr ::= FLOAT",
 /* 206 */ "expr ::= MINUS FLOAT",
 /* 207 */ "expr ::= PLUS FLOAT",
 /* 208 */ "expr ::= STRING",
 /* 209 */ "expr ::= NOW",
 /* 210 */ "expr ::= VARIABLE",
 /* 211 */ "expr ::= BOOL",
 /* 212 */ "expr ::= ID LP exprlist RP",
 /* 213 */ "expr ::= ID LP STAR RP",
 /* 214 */ "expr ::= expr IS NULL",
 /* 215 */ "expr ::= expr IS NOT NULL",
 /* 216 */ "expr ::= expr LT expr",
 /* 217 */ "expr ::= expr GT expr",
 /* 218 */ "expr ::= expr LE expr",
 /* 219 */ "expr ::= expr GE expr",
 /* 220 */ "expr ::= expr NE expr",
 /* 221 */ "expr ::= expr EQ expr",
 /* 222 */ "expr ::= expr AND expr",
 /* 223 */ "expr ::= expr OR expr",
 /* 224 */ "expr ::= expr PLUS expr",
 /* 225 */ "expr ::= expr MINUS expr",
 /* 226 */ "expr ::= expr STAR expr",
 /* 227 */ "expr ::= expr SLASH expr",
 /* 228 */ "expr ::= expr REM expr",
 /* 229 */ "expr ::= expr LIKE expr",
 /* 230 */ "expr ::= expr IN LP exprlist RP",
 /* 231 */ "exprlist ::= exprlist COMMA expritem",
 /* 232 */ "exprlist ::= expritem",
 /* 233 */ "expritem ::= expr",
 /* 234 */ "expritem ::=",
 /* 235 */ "cmd ::= RESET QUERY CACHE",
 /* 236 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 237 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 238 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 239 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 240 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 241 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 242 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 243 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 244 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 245 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 246 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 247 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 248 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 249 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 204: /* keep */
    case 205: /* tagitemlist */
    case 226: /* columnlist */
    case 234: /* fill_opt */
    case 236: /* groupby_opt */
    case 237: /* orderby_opt */
    case 248: /* sortlist */
    case 252: /* grouplist */
{
taosArrayDestroy((yypminor->yy141));
}
      break;
    case 224: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy8));
}
      break;
    case 227: /* select */
{
doDestroyQuerySql((yypminor->yy234));
}
      break;
    case 230: /* selcollist */
    case 242: /* sclp */
    case 253: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy9));
}
      break;
    case 232: /* where_opt */
    case 238: /* having_opt */
    case 244: /* expr */
    case 254: /* expritem */
{
tSqlExprDestroy((yypminor->yy220));
}
      break;
    case 241: /* union */
{
destroyAllSelectClause((yypminor->yy63));
}
      break;
    case 249: /* sortitem */
{
tVariantDestroy(&(yypminor->yy326));
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
  {  185,   -1 }, /* (0) program ::= cmd */
  {  186,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  186,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  186,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  186,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  186,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  186,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  186,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  186,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  186,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  186,   -2 }, /* (10) cmd ::= SHOW VARIABLES */
  {  186,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  186,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  186,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  186,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  187,    0 }, /* (15) dbPrefix ::= */
  {  187,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  189,    0 }, /* (17) cpxName ::= */
  {  189,   -2 }, /* (18) cpxName ::= DOT ids */
  {  186,   -5 }, /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  186,   -4 }, /* (20) cmd ::= SHOW CREATE DATABASE ids */
  {  186,   -3 }, /* (21) cmd ::= SHOW dbPrefix TABLES */
  {  186,   -5 }, /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  186,   -3 }, /* (23) cmd ::= SHOW dbPrefix STABLES */
  {  186,   -5 }, /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  186,   -3 }, /* (25) cmd ::= SHOW dbPrefix VGROUPS */
  {  186,   -4 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  186,   -5 }, /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
  {  186,   -5 }, /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
  {  186,   -4 }, /* (29) cmd ::= DROP DATABASE ifexists ids */
  {  186,   -3 }, /* (30) cmd ::= DROP DNODE ids */
  {  186,   -3 }, /* (31) cmd ::= DROP USER ids */
  {  186,   -3 }, /* (32) cmd ::= DROP ACCOUNT ids */
  {  186,   -2 }, /* (33) cmd ::= USE ids */
  {  186,   -3 }, /* (34) cmd ::= DESCRIBE ids cpxName */
  {  186,   -5 }, /* (35) cmd ::= ALTER USER ids PASS ids */
  {  186,   -5 }, /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  186,   -4 }, /* (37) cmd ::= ALTER DNODE ids ids */
  {  186,   -5 }, /* (38) cmd ::= ALTER DNODE ids ids ids */
  {  186,   -3 }, /* (39) cmd ::= ALTER LOCAL ids */
  {  186,   -4 }, /* (40) cmd ::= ALTER LOCAL ids ids */
  {  186,   -4 }, /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  186,   -4 }, /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  186,   -6 }, /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  188,   -1 }, /* (44) ids ::= ID */
  {  188,   -1 }, /* (45) ids ::= STRING */
  {  190,   -2 }, /* (46) ifexists ::= IF EXISTS */
  {  190,    0 }, /* (47) ifexists ::= */
  {  193,   -3 }, /* (48) ifnotexists ::= IF NOT EXISTS */
  {  193,    0 }, /* (49) ifnotexists ::= */
  {  186,   -3 }, /* (50) cmd ::= CREATE DNODE ids */
  {  186,   -6 }, /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  186,   -5 }, /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  186,   -5 }, /* (53) cmd ::= CREATE USER ids PASS ids */
  {  195,    0 }, /* (54) pps ::= */
  {  195,   -2 }, /* (55) pps ::= PPS INTEGER */
  {  196,    0 }, /* (56) tseries ::= */
  {  196,   -2 }, /* (57) tseries ::= TSERIES INTEGER */
  {  197,    0 }, /* (58) dbs ::= */
  {  197,   -2 }, /* (59) dbs ::= DBS INTEGER */
  {  198,    0 }, /* (60) streams ::= */
  {  198,   -2 }, /* (61) streams ::= STREAMS INTEGER */
  {  199,    0 }, /* (62) storage ::= */
  {  199,   -2 }, /* (63) storage ::= STORAGE INTEGER */
  {  200,    0 }, /* (64) qtime ::= */
  {  200,   -2 }, /* (65) qtime ::= QTIME INTEGER */
  {  201,    0 }, /* (66) users ::= */
  {  201,   -2 }, /* (67) users ::= USERS INTEGER */
  {  202,    0 }, /* (68) conns ::= */
  {  202,   -2 }, /* (69) conns ::= CONNS INTEGER */
  {  203,    0 }, /* (70) state ::= */
  {  203,   -2 }, /* (71) state ::= STATE ids */
  {  192,   -9 }, /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  204,   -2 }, /* (73) keep ::= KEEP tagitemlist */
  {  206,   -2 }, /* (74) cache ::= CACHE INTEGER */
  {  207,   -2 }, /* (75) replica ::= REPLICA INTEGER */
  {  208,   -2 }, /* (76) quorum ::= QUORUM INTEGER */
  {  209,   -2 }, /* (77) days ::= DAYS INTEGER */
  {  210,   -2 }, /* (78) minrows ::= MINROWS INTEGER */
  {  211,   -2 }, /* (79) maxrows ::= MAXROWS INTEGER */
  {  212,   -2 }, /* (80) blocks ::= BLOCKS INTEGER */
  {  213,   -2 }, /* (81) ctime ::= CTIME INTEGER */
  {  214,   -2 }, /* (82) wal ::= WAL INTEGER */
  {  215,   -2 }, /* (83) fsync ::= FSYNC INTEGER */
  {  216,   -2 }, /* (84) comp ::= COMP INTEGER */
  {  217,   -2 }, /* (85) prec ::= PRECISION STRING */
  {  218,   -2 }, /* (86) update ::= UPDATE INTEGER */
  {  219,   -2 }, /* (87) cachelast ::= CACHELAST INTEGER */
  {  194,    0 }, /* (88) db_optr ::= */
  {  194,   -2 }, /* (89) db_optr ::= db_optr cache */
  {  194,   -2 }, /* (90) db_optr ::= db_optr replica */
  {  194,   -2 }, /* (91) db_optr ::= db_optr quorum */
  {  194,   -2 }, /* (92) db_optr ::= db_optr days */
  {  194,   -2 }, /* (93) db_optr ::= db_optr minrows */
  {  194,   -2 }, /* (94) db_optr ::= db_optr maxrows */
  {  194,   -2 }, /* (95) db_optr ::= db_optr blocks */
  {  194,   -2 }, /* (96) db_optr ::= db_optr ctime */
  {  194,   -2 }, /* (97) db_optr ::= db_optr wal */
  {  194,   -2 }, /* (98) db_optr ::= db_optr fsync */
  {  194,   -2 }, /* (99) db_optr ::= db_optr comp */
  {  194,   -2 }, /* (100) db_optr ::= db_optr prec */
  {  194,   -2 }, /* (101) db_optr ::= db_optr keep */
  {  194,   -2 }, /* (102) db_optr ::= db_optr update */
  {  194,   -2 }, /* (103) db_optr ::= db_optr cachelast */
  {  191,    0 }, /* (104) alter_db_optr ::= */
  {  191,   -2 }, /* (105) alter_db_optr ::= alter_db_optr replica */
  {  191,   -2 }, /* (106) alter_db_optr ::= alter_db_optr quorum */
  {  191,   -2 }, /* (107) alter_db_optr ::= alter_db_optr keep */
  {  191,   -2 }, /* (108) alter_db_optr ::= alter_db_optr blocks */
  {  191,   -2 }, /* (109) alter_db_optr ::= alter_db_optr comp */
  {  191,   -2 }, /* (110) alter_db_optr ::= alter_db_optr wal */
  {  191,   -2 }, /* (111) alter_db_optr ::= alter_db_optr fsync */
  {  191,   -2 }, /* (112) alter_db_optr ::= alter_db_optr update */
  {  191,   -2 }, /* (113) alter_db_optr ::= alter_db_optr cachelast */
  {  220,   -1 }, /* (114) typename ::= ids */
  {  220,   -4 }, /* (115) typename ::= ids LP signed RP */
  {  220,   -2 }, /* (116) typename ::= ids UNSIGNED */
  {  221,   -1 }, /* (117) signed ::= INTEGER */
  {  221,   -2 }, /* (118) signed ::= PLUS INTEGER */
  {  221,   -2 }, /* (119) signed ::= MINUS INTEGER */
  {  186,   -3 }, /* (120) cmd ::= CREATE TABLE create_table_args */
  {  186,   -3 }, /* (121) cmd ::= CREATE TABLE create_stable_args */
  {  186,   -3 }, /* (122) cmd ::= CREATE STABLE create_stable_args */
  {  186,   -3 }, /* (123) cmd ::= CREATE TABLE create_table_list */
  {  224,   -1 }, /* (124) create_table_list ::= create_from_stable */
  {  224,   -2 }, /* (125) create_table_list ::= create_table_list create_from_stable */
  {  222,   -6 }, /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  223,  -10 }, /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  225,  -10 }, /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  222,   -5 }, /* (129) create_table_args ::= ifnotexists ids cpxName AS select */
  {  226,   -3 }, /* (130) columnlist ::= columnlist COMMA column */
  {  226,   -1 }, /* (131) columnlist ::= column */
  {  228,   -2 }, /* (132) column ::= ids typename */
  {  205,   -3 }, /* (133) tagitemlist ::= tagitemlist COMMA tagitem */
  {  205,   -1 }, /* (134) tagitemlist ::= tagitem */
  {  229,   -1 }, /* (135) tagitem ::= INTEGER */
  {  229,   -1 }, /* (136) tagitem ::= FLOAT */
  {  229,   -1 }, /* (137) tagitem ::= STRING */
  {  229,   -1 }, /* (138) tagitem ::= BOOL */
  {  229,   -1 }, /* (139) tagitem ::= NULL */
  {  229,   -2 }, /* (140) tagitem ::= MINUS INTEGER */
  {  229,   -2 }, /* (141) tagitem ::= MINUS FLOAT */
  {  229,   -2 }, /* (142) tagitem ::= PLUS INTEGER */
  {  229,   -2 }, /* (143) tagitem ::= PLUS FLOAT */
  {  227,  -12 }, /* (144) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  241,   -1 }, /* (145) union ::= select */
  {  241,   -3 }, /* (146) union ::= LP union RP */
  {  241,   -4 }, /* (147) union ::= union UNION ALL select */
  {  241,   -6 }, /* (148) union ::= union UNION ALL LP select RP */
  {  186,   -1 }, /* (149) cmd ::= union */
  {  227,   -2 }, /* (150) select ::= SELECT selcollist */
  {  242,   -2 }, /* (151) sclp ::= selcollist COMMA */
  {  242,    0 }, /* (152) sclp ::= */
  {  230,   -4 }, /* (153) selcollist ::= sclp distinct expr as */
  {  230,   -2 }, /* (154) selcollist ::= sclp STAR */
  {  245,   -2 }, /* (155) as ::= AS ids */
  {  245,   -1 }, /* (156) as ::= ids */
  {  245,    0 }, /* (157) as ::= */
  {  243,   -1 }, /* (158) distinct ::= DISTINCT */
  {  243,    0 }, /* (159) distinct ::= */
  {  231,   -2 }, /* (160) from ::= FROM tablelist */
  {  246,   -2 }, /* (161) tablelist ::= ids cpxName */
  {  246,   -3 }, /* (162) tablelist ::= ids cpxName ids */
  {  246,   -4 }, /* (163) tablelist ::= tablelist COMMA ids cpxName */
  {  246,   -5 }, /* (164) tablelist ::= tablelist COMMA ids cpxName ids */
  {  247,   -1 }, /* (165) tmvar ::= VARIABLE */
  {  233,   -4 }, /* (166) interval_opt ::= INTERVAL LP tmvar RP */
  {  233,   -6 }, /* (167) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  233,    0 }, /* (168) interval_opt ::= */
  {  234,    0 }, /* (169) fill_opt ::= */
  {  234,   -6 }, /* (170) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  234,   -4 }, /* (171) fill_opt ::= FILL LP ID RP */
  {  235,   -4 }, /* (172) sliding_opt ::= SLIDING LP tmvar RP */
  {  235,    0 }, /* (173) sliding_opt ::= */
  {  237,    0 }, /* (174) orderby_opt ::= */
  {  237,   -3 }, /* (175) orderby_opt ::= ORDER BY sortlist */
  {  248,   -4 }, /* (176) sortlist ::= sortlist COMMA item sortorder */
  {  248,   -2 }, /* (177) sortlist ::= item sortorder */
  {  250,   -2 }, /* (178) item ::= ids cpxName */
  {  251,   -1 }, /* (179) sortorder ::= ASC */
  {  251,   -1 }, /* (180) sortorder ::= DESC */
  {  251,    0 }, /* (181) sortorder ::= */
  {  236,    0 }, /* (182) groupby_opt ::= */
  {  236,   -3 }, /* (183) groupby_opt ::= GROUP BY grouplist */
  {  252,   -3 }, /* (184) grouplist ::= grouplist COMMA item */
  {  252,   -1 }, /* (185) grouplist ::= item */
  {  238,    0 }, /* (186) having_opt ::= */
  {  238,   -2 }, /* (187) having_opt ::= HAVING expr */
  {  240,    0 }, /* (188) limit_opt ::= */
  {  240,   -2 }, /* (189) limit_opt ::= LIMIT signed */
  {  240,   -4 }, /* (190) limit_opt ::= LIMIT signed OFFSET signed */
  {  240,   -4 }, /* (191) limit_opt ::= LIMIT signed COMMA signed */
  {  239,    0 }, /* (192) slimit_opt ::= */
  {  239,   -2 }, /* (193) slimit_opt ::= SLIMIT signed */
  {  239,   -4 }, /* (194) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  239,   -4 }, /* (195) slimit_opt ::= SLIMIT signed COMMA signed */
  {  232,    0 }, /* (196) where_opt ::= */
  {  232,   -2 }, /* (197) where_opt ::= WHERE expr */
  {  244,   -3 }, /* (198) expr ::= LP expr RP */
  {  244,   -1 }, /* (199) expr ::= ID */
  {  244,   -3 }, /* (200) expr ::= ID DOT ID */
  {  244,   -3 }, /* (201) expr ::= ID DOT STAR */
  {  244,   -1 }, /* (202) expr ::= INTEGER */
  {  244,   -2 }, /* (203) expr ::= MINUS INTEGER */
  {  244,   -2 }, /* (204) expr ::= PLUS INTEGER */
  {  244,   -1 }, /* (205) expr ::= FLOAT */
  {  244,   -2 }, /* (206) expr ::= MINUS FLOAT */
  {  244,   -2 }, /* (207) expr ::= PLUS FLOAT */
  {  244,   -1 }, /* (208) expr ::= STRING */
  {  244,   -1 }, /* (209) expr ::= NOW */
  {  244,   -1 }, /* (210) expr ::= VARIABLE */
  {  244,   -1 }, /* (211) expr ::= BOOL */
  {  244,   -4 }, /* (212) expr ::= ID LP exprlist RP */
  {  244,   -4 }, /* (213) expr ::= ID LP STAR RP */
  {  244,   -3 }, /* (214) expr ::= expr IS NULL */
  {  244,   -4 }, /* (215) expr ::= expr IS NOT NULL */
  {  244,   -3 }, /* (216) expr ::= expr LT expr */
  {  244,   -3 }, /* (217) expr ::= expr GT expr */
  {  244,   -3 }, /* (218) expr ::= expr LE expr */
  {  244,   -3 }, /* (219) expr ::= expr GE expr */
  {  244,   -3 }, /* (220) expr ::= expr NE expr */
  {  244,   -3 }, /* (221) expr ::= expr EQ expr */
  {  244,   -3 }, /* (222) expr ::= expr AND expr */
  {  244,   -3 }, /* (223) expr ::= expr OR expr */
  {  244,   -3 }, /* (224) expr ::= expr PLUS expr */
  {  244,   -3 }, /* (225) expr ::= expr MINUS expr */
  {  244,   -3 }, /* (226) expr ::= expr STAR expr */
  {  244,   -3 }, /* (227) expr ::= expr SLASH expr */
  {  244,   -3 }, /* (228) expr ::= expr REM expr */
  {  244,   -3 }, /* (229) expr ::= expr LIKE expr */
  {  244,   -5 }, /* (230) expr ::= expr IN LP exprlist RP */
  {  253,   -3 }, /* (231) exprlist ::= exprlist COMMA expritem */
  {  253,   -1 }, /* (232) exprlist ::= expritem */
  {  254,   -1 }, /* (233) expritem ::= expr */
  {  254,    0 }, /* (234) expritem ::= */
  {  186,   -3 }, /* (235) cmd ::= RESET QUERY CACHE */
  {  186,   -7 }, /* (236) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  186,   -7 }, /* (237) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  186,   -7 }, /* (238) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  186,   -7 }, /* (239) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  186,   -8 }, /* (240) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  186,   -9 }, /* (241) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  186,   -7 }, /* (242) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  186,   -7 }, /* (243) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  186,   -7 }, /* (244) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  186,   -7 }, /* (245) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  186,   -8 }, /* (246) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  186,   -3 }, /* (247) cmd ::= KILL CONNECTION INTEGER */
  {  186,   -5 }, /* (248) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  186,   -5 }, /* (249) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy322, &t);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy1);}
        break;
      case 43: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy1);}
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
      case 159: /* distinct ::= */ yytestcase(yyruleno==159);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 48: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 50: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 51: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy1);}
        break;
      case 52: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy322, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy1.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy1.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy1.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy1.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy1.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy1.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy1.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy1.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy1.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy1 = yylhsminor.yy1;
        break;
      case 73: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy141 = yymsp[0].minor.yy141; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy322);}
        break;
      case 89: /* db_optr ::= db_optr cache */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 90: /* db_optr ::= db_optr replica */
      case 105: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==105);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 91: /* db_optr ::= db_optr quorum */
      case 106: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==106);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 92: /* db_optr ::= db_optr days */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 93: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 94: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 95: /* db_optr ::= db_optr blocks */
      case 108: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==108);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 96: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 97: /* db_optr ::= db_optr wal */
      case 110: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==110);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 98: /* db_optr ::= db_optr fsync */
      case 111: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==111);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 99: /* db_optr ::= db_optr comp */
      case 109: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==109);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 100: /* db_optr ::= db_optr prec */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 101: /* db_optr ::= db_optr keep */
      case 107: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==107);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.keep = yymsp[0].minor.yy141; }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 102: /* db_optr ::= db_optr update */
      case 112: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==112);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 103: /* db_optr ::= db_optr cachelast */
      case 113: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==113);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 104: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy322);}
        break;
      case 114: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy403, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 115: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy429 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy403, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy429;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy403, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 116: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy403, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 117: /* signed ::= INTEGER */
{ yylhsminor.yy429 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 118: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy429 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 119: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy429 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 123: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy8;}
        break;
      case 124: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy306);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy8 = pCreateTable;
}
  yymsp[0].minor.yy8 = yylhsminor.yy8;
        break;
      case 125: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy8->childTableInfo, &yymsp[0].minor.yy306);
  yylhsminor.yy8 = yymsp[-1].minor.yy8;
}
  yymsp[-1].minor.yy8 = yylhsminor.yy8;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy8 = tSetCreateSqlElems(yymsp[-1].minor.yy141, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy8, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy8 = yylhsminor.yy8;
        break;
      case 127: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy8 = tSetCreateSqlElems(yymsp[-5].minor.yy141, yymsp[-1].minor.yy141, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy8, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy8 = yylhsminor.yy8;
        break;
      case 128: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy306 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy141, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy306 = yylhsminor.yy306;
        break;
      case 129: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy8 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy234, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy8, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy8 = yylhsminor.yy8;
        break;
      case 130: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy141, &yymsp[0].minor.yy403); yylhsminor.yy141 = yymsp[-2].minor.yy141;  }
  yymsp[-2].minor.yy141 = yylhsminor.yy141;
        break;
      case 131: /* columnlist ::= column */
{yylhsminor.yy141 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy141, &yymsp[0].minor.yy403);}
  yymsp[0].minor.yy141 = yylhsminor.yy141;
        break;
      case 132: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy403, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy403);
}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 133: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy141 = tVariantListAppend(yymsp[-2].minor.yy141, &yymsp[0].minor.yy326, -1);    }
  yymsp[-2].minor.yy141 = yylhsminor.yy141;
        break;
      case 134: /* tagitemlist ::= tagitem */
{ yylhsminor.yy141 = tVariantListAppend(NULL, &yymsp[0].minor.yy326, -1); }
  yymsp[0].minor.yy141 = yylhsminor.yy141;
        break;
      case 135: /* tagitem ::= INTEGER */
      case 136: /* tagitem ::= FLOAT */ yytestcase(yyruleno==136);
      case 137: /* tagitem ::= STRING */ yytestcase(yyruleno==137);
      case 138: /* tagitem ::= BOOL */ yytestcase(yyruleno==138);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy326, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 139: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy326, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 140: /* tagitem ::= MINUS INTEGER */
      case 141: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==141);
      case 142: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==142);
      case 143: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==143);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy326, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 144: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy234 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy9, yymsp[-9].minor.yy141, yymsp[-8].minor.yy220, yymsp[-4].minor.yy141, yymsp[-3].minor.yy141, &yymsp[-7].minor.yy340, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy141, &yymsp[0].minor.yy24, &yymsp[-1].minor.yy24);
}
  yymsp[-11].minor.yy234 = yylhsminor.yy234;
        break;
      case 145: /* union ::= select */
{ yylhsminor.yy63 = setSubclause(NULL, yymsp[0].minor.yy234); }
  yymsp[0].minor.yy63 = yylhsminor.yy63;
        break;
      case 146: /* union ::= LP union RP */
{ yymsp[-2].minor.yy63 = yymsp[-1].minor.yy63; }
        break;
      case 147: /* union ::= union UNION ALL select */
{ yylhsminor.yy63 = appendSelectClause(yymsp[-3].minor.yy63, yymsp[0].minor.yy234); }
  yymsp[-3].minor.yy63 = yylhsminor.yy63;
        break;
      case 148: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy63 = appendSelectClause(yymsp[-5].minor.yy63, yymsp[-1].minor.yy234); }
  yymsp[-5].minor.yy63 = yylhsminor.yy63;
        break;
      case 149: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy63, NULL, TSDB_SQL_SELECT); }
        break;
      case 150: /* select ::= SELECT selcollist */
{
  yylhsminor.yy234 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy234 = yylhsminor.yy234;
        break;
      case 151: /* sclp ::= selcollist COMMA */
{yylhsminor.yy9 = yymsp[-1].minor.yy9;}
  yymsp[-1].minor.yy9 = yylhsminor.yy9;
        break;
      case 152: /* sclp ::= */
{yymsp[1].minor.yy9 = 0;}
        break;
      case 153: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy9 = tSqlExprListAppend(yymsp[-3].minor.yy9, yymsp[-1].minor.yy220,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy9 = yylhsminor.yy9;
        break;
      case 154: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy9 = tSqlExprListAppend(yymsp[-1].minor.yy9, pNode, 0, 0);
}
  yymsp[-1].minor.yy9 = yylhsminor.yy9;
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
      case 158: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 160: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy141 = yymsp[0].minor.yy141;}
        break;
      case 161: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy141 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy141 = tVariantListAppendToken(yylhsminor.yy141, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy141 = yylhsminor.yy141;
        break;
      case 162: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy141 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy141 = tVariantListAppendToken(yylhsminor.yy141, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy141 = yylhsminor.yy141;
        break;
      case 163: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy141 = tVariantListAppendToken(yymsp[-3].minor.yy141, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy141 = tVariantListAppendToken(yylhsminor.yy141, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy141 = yylhsminor.yy141;
        break;
      case 164: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy141 = tVariantListAppendToken(yymsp[-4].minor.yy141, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy141 = tVariantListAppendToken(yylhsminor.yy141, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy141 = yylhsminor.yy141;
        break;
      case 165: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 166: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy340.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy340.offset.n = 0; yymsp[-3].minor.yy340.offset.z = NULL; yymsp[-3].minor.yy340.offset.type = 0;}
        break;
      case 167: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy340.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy340.offset = yymsp[-1].minor.yy0;}
        break;
      case 168: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy340, 0, sizeof(yymsp[1].minor.yy340));}
        break;
      case 169: /* fill_opt ::= */
{yymsp[1].minor.yy141 = 0;     }
        break;
      case 170: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy141, &A, -1, 0);
    yymsp[-5].minor.yy141 = yymsp[-1].minor.yy141;
}
        break;
      case 171: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy141 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 172: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 173: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 174: /* orderby_opt ::= */
{yymsp[1].minor.yy141 = 0;}
        break;
      case 175: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy141 = yymsp[0].minor.yy141;}
        break;
      case 176: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy141 = tVariantListAppend(yymsp[-3].minor.yy141, &yymsp[-1].minor.yy326, yymsp[0].minor.yy502);
}
  yymsp[-3].minor.yy141 = yylhsminor.yy141;
        break;
      case 177: /* sortlist ::= item sortorder */
{
  yylhsminor.yy141 = tVariantListAppend(NULL, &yymsp[-1].minor.yy326, yymsp[0].minor.yy502);
}
  yymsp[-1].minor.yy141 = yylhsminor.yy141;
        break;
      case 178: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy326, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 179: /* sortorder ::= ASC */
{ yymsp[0].minor.yy502 = TSDB_ORDER_ASC; }
        break;
      case 180: /* sortorder ::= DESC */
{ yymsp[0].minor.yy502 = TSDB_ORDER_DESC;}
        break;
      case 181: /* sortorder ::= */
{ yymsp[1].minor.yy502 = TSDB_ORDER_ASC; }
        break;
      case 182: /* groupby_opt ::= */
{ yymsp[1].minor.yy141 = 0;}
        break;
      case 183: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy141 = yymsp[0].minor.yy141;}
        break;
      case 184: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy141 = tVariantListAppend(yymsp[-2].minor.yy141, &yymsp[0].minor.yy326, -1);
}
  yymsp[-2].minor.yy141 = yylhsminor.yy141;
        break;
      case 185: /* grouplist ::= item */
{
  yylhsminor.yy141 = tVariantListAppend(NULL, &yymsp[0].minor.yy326, -1);
}
  yymsp[0].minor.yy141 = yylhsminor.yy141;
        break;
      case 186: /* having_opt ::= */
      case 196: /* where_opt ::= */ yytestcase(yyruleno==196);
      case 234: /* expritem ::= */ yytestcase(yyruleno==234);
{yymsp[1].minor.yy220 = 0;}
        break;
      case 187: /* having_opt ::= HAVING expr */
      case 197: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==197);
{yymsp[-1].minor.yy220 = yymsp[0].minor.yy220;}
        break;
      case 188: /* limit_opt ::= */
      case 192: /* slimit_opt ::= */ yytestcase(yyruleno==192);
{yymsp[1].minor.yy24.limit = -1; yymsp[1].minor.yy24.offset = 0;}
        break;
      case 189: /* limit_opt ::= LIMIT signed */
      case 193: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==193);
{yymsp[-1].minor.yy24.limit = yymsp[0].minor.yy429;  yymsp[-1].minor.yy24.offset = 0;}
        break;
      case 190: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy429;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy429;}
        break;
      case 191: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy429;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy429;}
        break;
      case 194: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy24.limit = yymsp[-2].minor.yy429;  yymsp[-3].minor.yy24.offset = yymsp[0].minor.yy429;}
        break;
      case 195: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy24.limit = yymsp[0].minor.yy429;  yymsp[-3].minor.yy24.offset = yymsp[-2].minor.yy429;}
        break;
      case 198: /* expr ::= LP expr RP */
{yylhsminor.yy220 = yymsp[-1].minor.yy220; yylhsminor.yy220->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy220->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 199: /* expr ::= ID */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 200: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 201: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 202: /* expr ::= INTEGER */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 203: /* expr ::= MINUS INTEGER */
      case 204: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==204);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy220 = yylhsminor.yy220;
        break;
      case 205: /* expr ::= FLOAT */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 206: /* expr ::= MINUS FLOAT */
      case 207: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==207);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy220 = yylhsminor.yy220;
        break;
      case 208: /* expr ::= STRING */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 209: /* expr ::= NOW */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 210: /* expr ::= VARIABLE */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 211: /* expr ::= BOOL */
{ yylhsminor.yy220 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 212: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy220 = tSqlExprCreateFunction(yymsp[-1].minor.yy9, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy220 = yylhsminor.yy220;
        break;
      case 213: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy220 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy220 = yylhsminor.yy220;
        break;
      case 214: /* expr ::= expr IS NULL */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 215: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-3].minor.yy220, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy220 = yylhsminor.yy220;
        break;
      case 216: /* expr ::= expr LT expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_LT);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 217: /* expr ::= expr GT expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_GT);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 218: /* expr ::= expr LE expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_LE);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 219: /* expr ::= expr GE expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_GE);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 220: /* expr ::= expr NE expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_NE);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 221: /* expr ::= expr EQ expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_EQ);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 222: /* expr ::= expr AND expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_AND);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 223: /* expr ::= expr OR expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_OR); }
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 224: /* expr ::= expr PLUS expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_PLUS);  }
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 225: /* expr ::= expr MINUS expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_MINUS); }
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 226: /* expr ::= expr STAR expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_STAR);  }
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 227: /* expr ::= expr SLASH expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_DIVIDE);}
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 228: /* expr ::= expr REM expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_REM);   }
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 229: /* expr ::= expr LIKE expr */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-2].minor.yy220, yymsp[0].minor.yy220, TK_LIKE);  }
  yymsp[-2].minor.yy220 = yylhsminor.yy220;
        break;
      case 230: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy220 = tSqlExprCreate(yymsp[-4].minor.yy220, (tSQLExpr*)yymsp[-1].minor.yy9, TK_IN); }
  yymsp[-4].minor.yy220 = yylhsminor.yy220;
        break;
      case 231: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy9 = tSqlExprListAppend(yymsp[-2].minor.yy9,yymsp[0].minor.yy220,0, 0);}
  yymsp[-2].minor.yy9 = yylhsminor.yy9;
        break;
      case 232: /* exprlist ::= expritem */
{yylhsminor.yy9 = tSqlExprListAppend(0,yymsp[0].minor.yy220,0, 0);}
  yymsp[0].minor.yy9 = yylhsminor.yy9;
        break;
      case 233: /* expritem ::= expr */
{yylhsminor.yy220 = yymsp[0].minor.yy220;}
  yymsp[0].minor.yy220 = yylhsminor.yy220;
        break;
      case 235: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy141, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 237: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 238: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy141, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 239: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 240: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 241: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy326, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 242: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy141, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 243: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 244: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy141, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 245: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 246: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 247: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 248: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 249: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
