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
#define YYNOCODE 282
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy42;
  SCreateAcctInfo yy47;
  SQuerySQL* yy114;
  TAOS_FIELD yy179;
  SLimitVal yy204;
  SSubclauseInfo* yy219;
  int yy222;
  SArray* yy247;
  SCreateDbInfo yy262;
  tSQLExpr* yy326;
  SCreateTableSQL* yy358;
  tVariant yy378;
  int64_t yy403;
  SIntervalVal yy430;
  tSQLExprList* yy522;
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
#define YYNTOKEN             210
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
#define YY_ACTTAB_COUNT (615)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   872,  507,  159,  714,  282,  159,   15,  580,  183,  508,
 /*    10 */   663,  186,  948,   41,   42,  947,   43,   44,   26,  158,
 /*    20 */   191,   35,  507,  507,  232,   47,   45,   49,   46,  163,
 /*    30 */   508,  508,  850,   40,   39,  257,  256,   38,   37,   36,
 /*    40 */    41,   42,  118,   43,   44,  861,  664,  191,   35,  179,
 /*    50 */   280,  232,   47,   45,   49,   46,  181,  869,  847,  215,
 /*    60 */    40,   39,  957,  123,   38,   37,   36,  464,  465,  466,
 /*    70 */   467,  468,  469,  470,  471,  472,  473,  474,  475,  281,
 /*    80 */    91,  196,  205,   41,   42,  267,   43,   44,  839,  247,
 /*    90 */   191,   35,  159,  944,  232,   47,   45,   49,   46,  123,
 /*   100 */   220,  185,  948,   40,   39,  850,   62,   38,   37,   36,
 /*   110 */    20,  245,  275,  274,  244,  243,  242,  273,  241,  272,
 /*   120 */   271,  270,  240,  269,  268,  901,  267,  227,  817,  661,
 /*   130 */   805,  806,  807,  808,  809,  810,  811,  812,  813,  814,
 /*   140 */   815,  816,  818,  819,   42,  208,   43,   44,  642,  643,
 /*   150 */   191,   35,  212,  211,  232,   47,   45,   49,   46,  229,
 /*   160 */   861,   67,   21,   40,   39,  247,  276,   38,   37,   36,
 /*   170 */    32,   43,   44,  197,  180,  191,   35,  849,   70,  232,
 /*   180 */    47,   45,   49,   46,   16,   38,   37,   36,   40,   39,
 /*   190 */   850,  123,   38,   37,   36,  190,  674,   72,   26,  665,
 /*   200 */   167,  668,  667,  671,  670,   61,  168,  836,  837,   25,
 /*   210 */   840,  103,  102,  166,  190,  674,  758,  199,  665,  147,
 /*   220 */   668,  767,  671,   20,  147,  275,  274,  187,  188,  841,
 /*   230 */   273,  231,  272,  271,  270,  838,  269,  268,  846,  198,
 /*   240 */   823,   21,  249,  821,  822,  943,  187,  188,  824,   32,
 /*   250 */   826,  827,  825,  900,  828,  829,  123,    3,  137,   47,
 /*   260 */    45,   49,   46,   29,   78,   74,   77,   40,   39,  617,
 /*   270 */   214,   38,   37,   36,  233,  601,  759,  174,  598,  147,
 /*   280 */   599,   26,  600,  201,   89,   93,  254,  253,   26,  942,
 /*   290 */    83,   98,  101,   92,   26,   48,  153,  149,   26,   95,
 /*   300 */   175,   26,  151,  106,  105,  104,  202,  203,  673,   26,
 /*   310 */    69,   40,   39,  200,   48,   38,   37,   36,   68,  194,
 /*   320 */    10,  847,   63,  672,   71,  133,  195,  673,  847,  279,
 /*   330 */   278,  110,  250,  666,  847,  669,  251,  614,  847,  255,
 /*   340 */   116,  847,  672,   22,  621,  848,  609,  259,   32,  847,
 /*   350 */   218,  629,  633,  634,  217,   27,  120,   52,   18,  693,
 /*   360 */   675,  189,   53,  176,   17,   17,  590,  677,   56,  235,
 /*   370 */   591,   27,  100,   99,   27,   52,   82,   81,   12,   11,
 /*   380 */   161,  602,   54,   59,  162,  579,   57,   88,   87,   14,
 /*   390 */    13,  605,  603,  606,  604,  115,  113,  164,  165,  171,
 /*   400 */   911,  172,  170,    4,  157,  169,  160,  910,  192,  907,
 /*   410 */   906,  193,  258,  117,  871,   33,  878,  880,  119,  863,
 /*   420 */   893,  892,  134,  135,  132,  136,  769,   32,  239,  155,
 /*   430 */    30,  248,  766,  216,  962,   79,  961,  114,  959,  138,
 /*   440 */   252,  956,   85,  955,  628,  953,  221,  139,  182,  225,
 /*   450 */   787,   31,   28,   58,  156,  756,   94,  860,   55,   50,
 /*   460 */   754,   96,  230,  124,  228,  125,   97,  752,  126,  127,
 /*   470 */   226,  128,  224,  222,  751,   34,  204,  148,   90,  749,
 /*   480 */   260,  261,  262,  748,  747,  746,  745,  263,  150,  152,
 /*   490 */   742,  740,  264,  738,  736,  734,  265,  154,  266,  219,
 /*   500 */    64,   65,  894,  277,  712,  206,  207,  177,  237,  238,
 /*   510 */   711,  178,  173,  209,   75,  210,  710,  698,  217,  213,
 /*   520 */   750,  611,  107,   60,  744,  234,  142,  141,  788,  140,
 /*   530 */   143,  144,  146,  145,    1,  108,  743,    2,  109,  735,
 /*   540 */    66,    6,  184,  630,  845,  121,  223,  131,  129,  130,
 /*   550 */   635,  122,    5,   23,    7,    8,   24,  676,    9,   19,
 /*   560 */   678,   71,   73,  236,  548,  544,  542,  541,  540,  537,
 /*   570 */   511,  246,   76,   27,   51,  582,   80,   84,  581,  578,
 /*   580 */   532,   86,  530,  522,  528,  524,  526,  520,  518,  550,
 /*   590 */   549,  547,  546,  545,  543,  539,  538,   52,  509,  479,
 /*   600 */   477,  716,  715,  715,  715,  715,  715,  715,  715,  715,
 /*   610 */   715,  715,  715,  111,  112,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   214,    1,  270,  211,  212,  270,  270,    5,  231,    9,
 /*    10 */     1,  279,  280,   13,   14,  280,   16,   17,  214,  270,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  270,
 /*    30 */     9,    9,  255,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  214,   16,   17,  253,   37,   20,   21,  213,
 /*    50 */   214,   24,   25,   26,   27,   28,  252,  271,  254,  267,
 /*    60 */    33,   34,  255,  214,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    74,  231,   60,   13,   14,   79,   16,   17,    0,   77,
 /*    90 */    20,   21,  270,  270,   24,   25,   26,   27,   28,  214,
 /*   100 */   272,  279,  280,   33,   34,  255,  106,   37,   38,   39,
 /*   110 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   120 */    96,   97,   98,   99,  100,  276,   79,  278,  230,  102,
 /*   130 */   232,  233,  234,  235,  236,  237,  238,  239,  240,  241,
 /*   140 */   242,  243,  244,  245,   14,  131,   16,   17,  120,  121,
 /*   150 */    20,   21,  138,  139,   24,   25,   26,   27,   28,  274,
 /*   160 */   253,  276,  101,   33,   34,   77,  231,   37,   38,   39,
 /*   170 */   109,   16,   17,   66,  267,   20,   21,  255,  219,   24,
 /*   180 */    25,   26,   27,   28,   44,   37,   38,   39,   33,   34,
 /*   190 */   255,  214,   37,   38,   39,    1,    2,  219,  214,    5,
 /*   200 */    60,    7,    5,    9,    7,  219,   66,  248,  249,  250,
 /*   210 */   251,   71,   72,   73,    1,    2,  218,   66,    5,  221,
 /*   220 */     7,  218,    9,   86,  221,   88,   89,   33,   34,  251,
 /*   230 */    93,   37,   95,   96,   97,  249,   99,  100,  254,  132,
 /*   240 */   230,  101,  135,  233,  234,  270,   33,   34,  238,  109,
 /*   250 */   240,  241,  242,  276,  244,  245,  214,   61,   62,   25,
 /*   260 */    26,   27,   28,   67,   68,   69,   70,   33,   34,   37,
 /*   270 */   130,   37,   38,   39,   15,    2,  218,  137,    5,  221,
 /*   280 */     7,  214,    9,  132,   61,   62,  135,  136,  214,  270,
 /*   290 */    67,   68,   69,   70,  214,  101,   61,   62,  214,   76,
 /*   300 */   270,  214,   67,   68,   69,   70,   33,   34,  114,  214,
 /*   310 */   256,   33,   34,  214,  101,   37,   38,   39,  276,  252,
 /*   320 */   101,  254,  268,  129,  105,  106,  252,  114,  254,   63,
 /*   330 */    64,   65,  252,    5,  254,    7,  252,  107,  254,  252,
 /*   340 */   101,  254,  129,  113,  112,  246,  102,  252,  109,  254,
 /*   350 */   102,  102,  102,  102,  110,  107,  107,  107,  107,  102,
 /*   360 */   102,   59,  107,  270,  107,  107,  102,  108,  107,  102,
 /*   370 */   102,  107,   74,   75,  107,  107,  133,  134,  133,  134,
 /*   380 */   270,  108,  127,  101,  270,  103,  125,  133,  134,  133,
 /*   390 */   134,    5,    5,    7,    7,   61,   62,  270,  270,  270,
 /*   400 */   247,  270,  270,  101,  270,  270,  270,  247,  247,  247,
 /*   410 */   247,  247,  247,  214,  214,  269,  214,  214,  214,  253,
 /*   420 */   277,  277,  214,  214,  257,  214,  214,  109,  214,  214,
 /*   430 */   214,  214,  214,  253,  214,  214,  214,   59,  214,  214,
 /*   440 */   214,  214,  214,  214,  114,  214,  273,  214,  273,  273,
 /*   450 */   214,  214,  214,  124,  214,  214,  214,  266,  126,  123,
 /*   460 */   214,  214,  118,  265,  122,  264,  214,  214,  263,  262,
 /*   470 */   117,  261,  116,  115,  214,  128,  214,  214,   85,  214,
 /*   480 */    84,   49,   81,  214,  214,  214,  214,   83,  214,  214,
 /*   490 */   214,  214,   53,  214,  214,  214,   82,  214,   80,  215,
 /*   500 */   215,  215,  215,   77,    5,  140,    5,  215,  215,  215,
 /*   510 */     5,  215,  215,  140,  219,    5,    5,   87,  110,  131,
 /*   520 */   215,  102,  216,  111,  215,  104,  223,  227,  229,  228,
 /*   530 */   226,  224,  222,  225,  220,  216,  215,  217,  216,  215,
 /*   540 */   107,  101,    1,  102,  253,  101,  101,  258,  260,  259,
 /*   550 */   102,  101,  101,  107,  119,  119,  107,  102,  101,  101,
 /*   560 */   108,  105,   74,  104,    9,    5,    5,    5,    5,    5,
 /*   570 */    78,   15,   74,  107,   16,    5,  134,  134,    5,  102,
 /*   580 */     5,  134,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   590 */     5,    5,    5,    5,    5,    5,    5,  107,   78,   59,
 /*   600 */    58,    0,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   610 */   281,  281,  281,   21,   21,  281,  281,  281,  281,  281,
 /*   620 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   630 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   640 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   650 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   660 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   670 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   680 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   690 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   700 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   710 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   720 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   730 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   740 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   750 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   760 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   770 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   780 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   790 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   800 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   810 */   281,  281,  281,  281,  281,  281,  281,  281,  281,  281,
 /*   820 */   281,  281,  281,  281,  281,
};
#define YY_SHIFT_COUNT    (282)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (601)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   140,   24,  137,   12,  194,  213,   21,   21,   21,   21,
 /*    10 */    21,   21,   21,   21,   21,    0,   22,  213,  273,  273,
 /*    20 */   273,   61,   21,   21,   21,   88,   21,   21,    6,   12,
 /*    30 */    47,   47,  615,  213,  213,  213,  213,  213,  213,  213,
 /*    40 */   213,  213,  213,  213,  213,  213,  213,  213,  213,  213,
 /*    50 */   213,  273,  273,    2,    2,    2,    2,    2,    2,    2,
 /*    60 */   239,   21,   21,  232,   21,   21,   21,   28,   28,  230,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,  318,  378,  378,  378,
 /*   120 */   330,  330,  330,  378,  329,  332,  336,  344,  342,  353,
 /*   130 */   356,  358,  347,  318,  378,  378,  378,   12,  378,  378,
 /*   140 */   393,  396,  432,  401,  404,  439,  414,  418,  378,  426,
 /*   150 */   378,  426,  378,  426,  378,  615,  615,   27,   70,   70,
 /*   160 */    70,  130,  155,  234,  234,  234,  223,  196,  235,  278,
 /*   170 */   278,  278,  278,  151,   14,  148,  148,  219,  107,  266,
 /*   180 */   244,  248,  249,  250,  251,  257,  258,  197,  328,    9,
 /*   190 */   302,  259,  255,  261,  264,  267,  268,  243,  245,  254,
 /*   200 */   282,  256,  386,  387,  298,  334,  499,  365,  501,  505,
 /*   210 */   373,  510,  511,  430,  388,  408,  419,  412,  421,  440,
 /*   220 */   433,  441,  444,  541,  445,  448,  450,  446,  435,  449,
 /*   230 */   436,  455,  451,  452,  457,  421,  458,  459,  456,  488,
 /*   240 */   555,  560,  561,  562,  563,  564,  492,  556,  498,  442,
 /*   250 */   466,  466,  558,  443,  447,  466,  570,  573,  477,  466,
 /*   260 */   575,  577,  578,  579,  580,  581,  582,  583,  584,  585,
 /*   270 */   586,  587,  588,  589,  590,  591,  490,  520,  592,  593,
 /*   280 */   540,  542,  601,
};
#define YY_REDUCE_COUNT (156)
#define YY_REDUCE_MIN   (-268)
#define YY_REDUCE_MAX   (324)
static const short yy_reduce_ofst[] = {
 /*     0 */  -208, -102,   10,  -41, -268, -178, -196, -151, -115,   67,
 /*    10 */    74,   80,   84,   87,   95, -214, -164, -265, -223, -150,
 /*    20 */   -65,  -93, -172,  -23,   42,  -22,   99,  -16,   -2,  -14,
 /*    30 */     3,   58,   54, -264, -251, -241, -177,  -25,   19,   30,
 /*    40 */    93,  110,  114,  127,  128,  129,  131,  132,  134,  135,
 /*    50 */   136, -193,  -78,  153,  160,  161,  162,  163,  164,  165,
 /*    60 */   166,  199,  200,  146,  202,  203,  204,  143,  144,  167,
 /*    70 */   208,  209,  211,  212,  214,  215,  216,  217,  218,  220,
 /*    80 */   221,  222,  224,  225,  226,  227,  228,  229,  231,  233,
 /*    90 */   236,  237,  238,  240,  241,  242,  246,  247,  252,  253,
 /*   100 */   260,  262,  263,  265,  269,  270,  271,  272,  274,  275,
 /*   110 */   276,  277,  279,  280,  281,  283,  180,  284,  285,  286,
 /*   120 */   173,  175,  176,  287,  191,  198,  201,  205,  207,  210,
 /*   130 */   288,  290,  289,  291,  292,  293,  294,  295,  296,  297,
 /*   140 */   299,  301,  300,  303,  304,  307,  308,  310,  305,  306,
 /*   150 */   309,  319,  321,  322,  324,  314,  320,
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
  /*  172 */ "COUNT",
  /*  173 */ "SUM",
  /*  174 */ "AVG",
  /*  175 */ "MIN",
  /*  176 */ "MAX",
  /*  177 */ "FIRST",
  /*  178 */ "LAST",
  /*  179 */ "TOP",
  /*  180 */ "BOTTOM",
  /*  181 */ "STDDEV",
  /*  182 */ "PERCENTILE",
  /*  183 */ "APERCENTILE",
  /*  184 */ "LEASTSQUARES",
  /*  185 */ "HISTOGRAM",
  /*  186 */ "DIFF",
  /*  187 */ "SPREAD",
  /*  188 */ "TWA",
  /*  189 */ "INTERP",
  /*  190 */ "LAST_ROW",
  /*  191 */ "RATE",
  /*  192 */ "IRATE",
  /*  193 */ "SUM_RATE",
  /*  194 */ "SUM_IRATE",
  /*  195 */ "AVG_RATE",
  /*  196 */ "AVG_IRATE",
  /*  197 */ "TBID",
  /*  198 */ "SEMI",
  /*  199 */ "NONE",
  /*  200 */ "PREV",
  /*  201 */ "LINEAR",
  /*  202 */ "IMPORT",
  /*  203 */ "METRIC",
  /*  204 */ "TBNAME",
  /*  205 */ "JOIN",
  /*  206 */ "METRICS",
  /*  207 */ "INSERT",
  /*  208 */ "INTO",
  /*  209 */ "VALUES",
  /*  210 */ "error",
  /*  211 */ "program",
  /*  212 */ "cmd",
  /*  213 */ "dbPrefix",
  /*  214 */ "ids",
  /*  215 */ "cpxName",
  /*  216 */ "ifexists",
  /*  217 */ "alter_db_optr",
  /*  218 */ "acct_optr",
  /*  219 */ "ifnotexists",
  /*  220 */ "db_optr",
  /*  221 */ "pps",
  /*  222 */ "tseries",
  /*  223 */ "dbs",
  /*  224 */ "streams",
  /*  225 */ "storage",
  /*  226 */ "qtime",
  /*  227 */ "users",
  /*  228 */ "conns",
  /*  229 */ "state",
  /*  230 */ "keep",
  /*  231 */ "tagitemlist",
  /*  232 */ "cache",
  /*  233 */ "replica",
  /*  234 */ "quorum",
  /*  235 */ "days",
  /*  236 */ "minrows",
  /*  237 */ "maxrows",
  /*  238 */ "blocks",
  /*  239 */ "ctime",
  /*  240 */ "wal",
  /*  241 */ "fsync",
  /*  242 */ "comp",
  /*  243 */ "prec",
  /*  244 */ "update",
  /*  245 */ "cachelast",
  /*  246 */ "typename",
  /*  247 */ "signed",
  /*  248 */ "create_table_args",
  /*  249 */ "create_stable_args",
  /*  250 */ "create_table_list",
  /*  251 */ "create_from_stable",
  /*  252 */ "columnlist",
  /*  253 */ "select",
  /*  254 */ "column",
  /*  255 */ "tagitem",
  /*  256 */ "selcollist",
  /*  257 */ "from",
  /*  258 */ "where_opt",
  /*  259 */ "interval_opt",
  /*  260 */ "fill_opt",
  /*  261 */ "sliding_opt",
  /*  262 */ "groupby_opt",
  /*  263 */ "orderby_opt",
  /*  264 */ "having_opt",
  /*  265 */ "slimit_opt",
  /*  266 */ "limit_opt",
  /*  267 */ "union",
  /*  268 */ "sclp",
  /*  269 */ "distinct",
  /*  270 */ "expr",
  /*  271 */ "as",
  /*  272 */ "tablelist",
  /*  273 */ "tmvar",
  /*  274 */ "sortlist",
  /*  275 */ "sortitem",
  /*  276 */ "item",
  /*  277 */ "sortorder",
  /*  278 */ "grouplist",
  /*  279 */ "exprlist",
  /*  280 */ "expritem",
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
    case 230: /* keep */
    case 231: /* tagitemlist */
    case 252: /* columnlist */
    case 260: /* fill_opt */
    case 262: /* groupby_opt */
    case 263: /* orderby_opt */
    case 274: /* sortlist */
    case 278: /* grouplist */
{
taosArrayDestroy((yypminor->yy247));
}
      break;
    case 250: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy358));
}
      break;
    case 253: /* select */
{
doDestroyQuerySql((yypminor->yy114));
}
      break;
    case 256: /* selcollist */
    case 268: /* sclp */
    case 279: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy522));
}
      break;
    case 258: /* where_opt */
    case 264: /* having_opt */
    case 270: /* expr */
    case 280: /* expritem */
{
tSqlExprDestroy((yypminor->yy326));
}
      break;
    case 267: /* union */
{
destroyAllSelectClause((yypminor->yy219));
}
      break;
    case 275: /* sortitem */
{
tVariantDestroy(&(yypminor->yy378));
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
  {  211,   -1 }, /* (0) program ::= cmd */
  {  212,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  212,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  212,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  212,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  212,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  212,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  212,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  212,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  212,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  212,   -2 }, /* (10) cmd ::= SHOW VARIABLES */
  {  212,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  212,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  212,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  212,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  213,    0 }, /* (15) dbPrefix ::= */
  {  213,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  215,    0 }, /* (17) cpxName ::= */
  {  215,   -2 }, /* (18) cpxName ::= DOT ids */
  {  212,   -5 }, /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  212,   -4 }, /* (20) cmd ::= SHOW CREATE DATABASE ids */
  {  212,   -3 }, /* (21) cmd ::= SHOW dbPrefix TABLES */
  {  212,   -5 }, /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  212,   -3 }, /* (23) cmd ::= SHOW dbPrefix STABLES */
  {  212,   -5 }, /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  212,   -3 }, /* (25) cmd ::= SHOW dbPrefix VGROUPS */
  {  212,   -4 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  212,   -5 }, /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
  {  212,   -5 }, /* (28) cmd ::= DROP STABLE ifexists ids cpxName */
  {  212,   -4 }, /* (29) cmd ::= DROP DATABASE ifexists ids */
  {  212,   -3 }, /* (30) cmd ::= DROP DNODE ids */
  {  212,   -3 }, /* (31) cmd ::= DROP USER ids */
  {  212,   -3 }, /* (32) cmd ::= DROP ACCOUNT ids */
  {  212,   -2 }, /* (33) cmd ::= USE ids */
  {  212,   -3 }, /* (34) cmd ::= DESCRIBE ids cpxName */
  {  212,   -5 }, /* (35) cmd ::= ALTER USER ids PASS ids */
  {  212,   -5 }, /* (36) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  212,   -4 }, /* (37) cmd ::= ALTER DNODE ids ids */
  {  212,   -5 }, /* (38) cmd ::= ALTER DNODE ids ids ids */
  {  212,   -3 }, /* (39) cmd ::= ALTER LOCAL ids */
  {  212,   -4 }, /* (40) cmd ::= ALTER LOCAL ids ids */
  {  212,   -4 }, /* (41) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  212,   -4 }, /* (42) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  212,   -6 }, /* (43) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  214,   -1 }, /* (44) ids ::= ID */
  {  214,   -1 }, /* (45) ids ::= STRING */
  {  216,   -2 }, /* (46) ifexists ::= IF EXISTS */
  {  216,    0 }, /* (47) ifexists ::= */
  {  219,   -3 }, /* (48) ifnotexists ::= IF NOT EXISTS */
  {  219,    0 }, /* (49) ifnotexists ::= */
  {  212,   -3 }, /* (50) cmd ::= CREATE DNODE ids */
  {  212,   -6 }, /* (51) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  212,   -5 }, /* (52) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  212,   -5 }, /* (53) cmd ::= CREATE USER ids PASS ids */
  {  221,    0 }, /* (54) pps ::= */
  {  221,   -2 }, /* (55) pps ::= PPS INTEGER */
  {  222,    0 }, /* (56) tseries ::= */
  {  222,   -2 }, /* (57) tseries ::= TSERIES INTEGER */
  {  223,    0 }, /* (58) dbs ::= */
  {  223,   -2 }, /* (59) dbs ::= DBS INTEGER */
  {  224,    0 }, /* (60) streams ::= */
  {  224,   -2 }, /* (61) streams ::= STREAMS INTEGER */
  {  225,    0 }, /* (62) storage ::= */
  {  225,   -2 }, /* (63) storage ::= STORAGE INTEGER */
  {  226,    0 }, /* (64) qtime ::= */
  {  226,   -2 }, /* (65) qtime ::= QTIME INTEGER */
  {  227,    0 }, /* (66) users ::= */
  {  227,   -2 }, /* (67) users ::= USERS INTEGER */
  {  228,    0 }, /* (68) conns ::= */
  {  228,   -2 }, /* (69) conns ::= CONNS INTEGER */
  {  229,    0 }, /* (70) state ::= */
  {  229,   -2 }, /* (71) state ::= STATE ids */
  {  218,   -9 }, /* (72) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  230,   -2 }, /* (73) keep ::= KEEP tagitemlist */
  {  232,   -2 }, /* (74) cache ::= CACHE INTEGER */
  {  233,   -2 }, /* (75) replica ::= REPLICA INTEGER */
  {  234,   -2 }, /* (76) quorum ::= QUORUM INTEGER */
  {  235,   -2 }, /* (77) days ::= DAYS INTEGER */
  {  236,   -2 }, /* (78) minrows ::= MINROWS INTEGER */
  {  237,   -2 }, /* (79) maxrows ::= MAXROWS INTEGER */
  {  238,   -2 }, /* (80) blocks ::= BLOCKS INTEGER */
  {  239,   -2 }, /* (81) ctime ::= CTIME INTEGER */
  {  240,   -2 }, /* (82) wal ::= WAL INTEGER */
  {  241,   -2 }, /* (83) fsync ::= FSYNC INTEGER */
  {  242,   -2 }, /* (84) comp ::= COMP INTEGER */
  {  243,   -2 }, /* (85) prec ::= PRECISION STRING */
  {  244,   -2 }, /* (86) update ::= UPDATE INTEGER */
  {  245,   -2 }, /* (87) cachelast ::= CACHELAST INTEGER */
  {  220,    0 }, /* (88) db_optr ::= */
  {  220,   -2 }, /* (89) db_optr ::= db_optr cache */
  {  220,   -2 }, /* (90) db_optr ::= db_optr replica */
  {  220,   -2 }, /* (91) db_optr ::= db_optr quorum */
  {  220,   -2 }, /* (92) db_optr ::= db_optr days */
  {  220,   -2 }, /* (93) db_optr ::= db_optr minrows */
  {  220,   -2 }, /* (94) db_optr ::= db_optr maxrows */
  {  220,   -2 }, /* (95) db_optr ::= db_optr blocks */
  {  220,   -2 }, /* (96) db_optr ::= db_optr ctime */
  {  220,   -2 }, /* (97) db_optr ::= db_optr wal */
  {  220,   -2 }, /* (98) db_optr ::= db_optr fsync */
  {  220,   -2 }, /* (99) db_optr ::= db_optr comp */
  {  220,   -2 }, /* (100) db_optr ::= db_optr prec */
  {  220,   -2 }, /* (101) db_optr ::= db_optr keep */
  {  220,   -2 }, /* (102) db_optr ::= db_optr update */
  {  220,   -2 }, /* (103) db_optr ::= db_optr cachelast */
  {  217,    0 }, /* (104) alter_db_optr ::= */
  {  217,   -2 }, /* (105) alter_db_optr ::= alter_db_optr replica */
  {  217,   -2 }, /* (106) alter_db_optr ::= alter_db_optr quorum */
  {  217,   -2 }, /* (107) alter_db_optr ::= alter_db_optr keep */
  {  217,   -2 }, /* (108) alter_db_optr ::= alter_db_optr blocks */
  {  217,   -2 }, /* (109) alter_db_optr ::= alter_db_optr comp */
  {  217,   -2 }, /* (110) alter_db_optr ::= alter_db_optr wal */
  {  217,   -2 }, /* (111) alter_db_optr ::= alter_db_optr fsync */
  {  217,   -2 }, /* (112) alter_db_optr ::= alter_db_optr update */
  {  217,   -2 }, /* (113) alter_db_optr ::= alter_db_optr cachelast */
  {  246,   -1 }, /* (114) typename ::= ids */
  {  246,   -4 }, /* (115) typename ::= ids LP signed RP */
  {  246,   -2 }, /* (116) typename ::= ids UNSIGNED */
  {  247,   -1 }, /* (117) signed ::= INTEGER */
  {  247,   -2 }, /* (118) signed ::= PLUS INTEGER */
  {  247,   -2 }, /* (119) signed ::= MINUS INTEGER */
  {  212,   -3 }, /* (120) cmd ::= CREATE TABLE create_table_args */
  {  212,   -3 }, /* (121) cmd ::= CREATE TABLE create_stable_args */
  {  212,   -3 }, /* (122) cmd ::= CREATE STABLE create_stable_args */
  {  212,   -3 }, /* (123) cmd ::= CREATE TABLE create_table_list */
  {  250,   -1 }, /* (124) create_table_list ::= create_from_stable */
  {  250,   -2 }, /* (125) create_table_list ::= create_table_list create_from_stable */
  {  248,   -6 }, /* (126) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  249,  -10 }, /* (127) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  251,  -10 }, /* (128) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  248,   -5 }, /* (129) create_table_args ::= ifnotexists ids cpxName AS select */
  {  252,   -3 }, /* (130) columnlist ::= columnlist COMMA column */
  {  252,   -1 }, /* (131) columnlist ::= column */
  {  254,   -2 }, /* (132) column ::= ids typename */
  {  231,   -3 }, /* (133) tagitemlist ::= tagitemlist COMMA tagitem */
  {  231,   -1 }, /* (134) tagitemlist ::= tagitem */
  {  255,   -1 }, /* (135) tagitem ::= INTEGER */
  {  255,   -1 }, /* (136) tagitem ::= FLOAT */
  {  255,   -1 }, /* (137) tagitem ::= STRING */
  {  255,   -1 }, /* (138) tagitem ::= BOOL */
  {  255,   -1 }, /* (139) tagitem ::= NULL */
  {  255,   -2 }, /* (140) tagitem ::= MINUS INTEGER */
  {  255,   -2 }, /* (141) tagitem ::= MINUS FLOAT */
  {  255,   -2 }, /* (142) tagitem ::= PLUS INTEGER */
  {  255,   -2 }, /* (143) tagitem ::= PLUS FLOAT */
  {  253,  -12 }, /* (144) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  267,   -1 }, /* (145) union ::= select */
  {  267,   -3 }, /* (146) union ::= LP union RP */
  {  267,   -4 }, /* (147) union ::= union UNION ALL select */
  {  267,   -6 }, /* (148) union ::= union UNION ALL LP select RP */
  {  212,   -1 }, /* (149) cmd ::= union */
  {  253,   -2 }, /* (150) select ::= SELECT selcollist */
  {  268,   -2 }, /* (151) sclp ::= selcollist COMMA */
  {  268,    0 }, /* (152) sclp ::= */
  {  256,   -4 }, /* (153) selcollist ::= sclp distinct expr as */
  {  256,   -2 }, /* (154) selcollist ::= sclp STAR */
  {  271,   -2 }, /* (155) as ::= AS ids */
  {  271,   -1 }, /* (156) as ::= ids */
  {  271,    0 }, /* (157) as ::= */
  {  269,   -1 }, /* (158) distinct ::= DISTINCT */
  {  269,    0 }, /* (159) distinct ::= */
  {  257,   -2 }, /* (160) from ::= FROM tablelist */
  {  272,   -2 }, /* (161) tablelist ::= ids cpxName */
  {  272,   -3 }, /* (162) tablelist ::= ids cpxName ids */
  {  272,   -4 }, /* (163) tablelist ::= tablelist COMMA ids cpxName */
  {  272,   -5 }, /* (164) tablelist ::= tablelist COMMA ids cpxName ids */
  {  273,   -1 }, /* (165) tmvar ::= VARIABLE */
  {  259,   -4 }, /* (166) interval_opt ::= INTERVAL LP tmvar RP */
  {  259,   -6 }, /* (167) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  259,    0 }, /* (168) interval_opt ::= */
  {  260,    0 }, /* (169) fill_opt ::= */
  {  260,   -6 }, /* (170) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  260,   -4 }, /* (171) fill_opt ::= FILL LP ID RP */
  {  261,   -4 }, /* (172) sliding_opt ::= SLIDING LP tmvar RP */
  {  261,    0 }, /* (173) sliding_opt ::= */
  {  263,    0 }, /* (174) orderby_opt ::= */
  {  263,   -3 }, /* (175) orderby_opt ::= ORDER BY sortlist */
  {  274,   -4 }, /* (176) sortlist ::= sortlist COMMA item sortorder */
  {  274,   -2 }, /* (177) sortlist ::= item sortorder */
  {  276,   -2 }, /* (178) item ::= ids cpxName */
  {  277,   -1 }, /* (179) sortorder ::= ASC */
  {  277,   -1 }, /* (180) sortorder ::= DESC */
  {  277,    0 }, /* (181) sortorder ::= */
  {  262,    0 }, /* (182) groupby_opt ::= */
  {  262,   -3 }, /* (183) groupby_opt ::= GROUP BY grouplist */
  {  278,   -3 }, /* (184) grouplist ::= grouplist COMMA item */
  {  278,   -1 }, /* (185) grouplist ::= item */
  {  264,    0 }, /* (186) having_opt ::= */
  {  264,   -2 }, /* (187) having_opt ::= HAVING expr */
  {  266,    0 }, /* (188) limit_opt ::= */
  {  266,   -2 }, /* (189) limit_opt ::= LIMIT signed */
  {  266,   -4 }, /* (190) limit_opt ::= LIMIT signed OFFSET signed */
  {  266,   -4 }, /* (191) limit_opt ::= LIMIT signed COMMA signed */
  {  265,    0 }, /* (192) slimit_opt ::= */
  {  265,   -2 }, /* (193) slimit_opt ::= SLIMIT signed */
  {  265,   -4 }, /* (194) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  265,   -4 }, /* (195) slimit_opt ::= SLIMIT signed COMMA signed */
  {  258,    0 }, /* (196) where_opt ::= */
  {  258,   -2 }, /* (197) where_opt ::= WHERE expr */
  {  270,   -3 }, /* (198) expr ::= LP expr RP */
  {  270,   -1 }, /* (199) expr ::= ID */
  {  270,   -3 }, /* (200) expr ::= ID DOT ID */
  {  270,   -3 }, /* (201) expr ::= ID DOT STAR */
  {  270,   -1 }, /* (202) expr ::= INTEGER */
  {  270,   -2 }, /* (203) expr ::= MINUS INTEGER */
  {  270,   -2 }, /* (204) expr ::= PLUS INTEGER */
  {  270,   -1 }, /* (205) expr ::= FLOAT */
  {  270,   -2 }, /* (206) expr ::= MINUS FLOAT */
  {  270,   -2 }, /* (207) expr ::= PLUS FLOAT */
  {  270,   -1 }, /* (208) expr ::= STRING */
  {  270,   -1 }, /* (209) expr ::= NOW */
  {  270,   -1 }, /* (210) expr ::= VARIABLE */
  {  270,   -1 }, /* (211) expr ::= BOOL */
  {  270,   -4 }, /* (212) expr ::= ID LP exprlist RP */
  {  270,   -4 }, /* (213) expr ::= ID LP STAR RP */
  {  270,   -3 }, /* (214) expr ::= expr IS NULL */
  {  270,   -4 }, /* (215) expr ::= expr IS NOT NULL */
  {  270,   -3 }, /* (216) expr ::= expr LT expr */
  {  270,   -3 }, /* (217) expr ::= expr GT expr */
  {  270,   -3 }, /* (218) expr ::= expr LE expr */
  {  270,   -3 }, /* (219) expr ::= expr GE expr */
  {  270,   -3 }, /* (220) expr ::= expr NE expr */
  {  270,   -3 }, /* (221) expr ::= expr EQ expr */
  {  270,   -3 }, /* (222) expr ::= expr AND expr */
  {  270,   -3 }, /* (223) expr ::= expr OR expr */
  {  270,   -3 }, /* (224) expr ::= expr PLUS expr */
  {  270,   -3 }, /* (225) expr ::= expr MINUS expr */
  {  270,   -3 }, /* (226) expr ::= expr STAR expr */
  {  270,   -3 }, /* (227) expr ::= expr SLASH expr */
  {  270,   -3 }, /* (228) expr ::= expr REM expr */
  {  270,   -3 }, /* (229) expr ::= expr LIKE expr */
  {  270,   -5 }, /* (230) expr ::= expr IN LP exprlist RP */
  {  279,   -3 }, /* (231) exprlist ::= exprlist COMMA expritem */
  {  279,   -1 }, /* (232) exprlist ::= expritem */
  {  280,   -1 }, /* (233) expritem ::= expr */
  {  280,    0 }, /* (234) expritem ::= */
  {  212,   -3 }, /* (235) cmd ::= RESET QUERY CACHE */
  {  212,   -7 }, /* (236) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  212,   -7 }, /* (237) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  212,   -7 }, /* (238) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  212,   -7 }, /* (239) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  212,   -8 }, /* (240) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  212,   -9 }, /* (241) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  212,   -7 }, /* (242) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  212,   -7 }, /* (243) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  212,   -7 }, /* (244) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  212,   -7 }, /* (245) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  212,   -8 }, /* (246) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  212,   -3 }, /* (247) cmd ::= KILL CONNECTION INTEGER */
  {  212,   -5 }, /* (248) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  212,   -5 }, /* (249) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy47);}
        break;
      case 43: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);}
        break;
      case 52: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy47.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy47.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy47.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy47.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy47.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy47.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy47.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy47.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy47 = yylhsminor.yy47;
        break;
      case 73: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy247 = yymsp[0].minor.yy247; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
        break;
      case 89: /* db_optr ::= db_optr cache */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 90: /* db_optr ::= db_optr replica */
      case 105: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==105);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 91: /* db_optr ::= db_optr quorum */
      case 106: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==106);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 92: /* db_optr ::= db_optr days */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 93: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 94: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 95: /* db_optr ::= db_optr blocks */
      case 108: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==108);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 96: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 97: /* db_optr ::= db_optr wal */
      case 110: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==110);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 98: /* db_optr ::= db_optr fsync */
      case 111: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==111);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 99: /* db_optr ::= db_optr comp */
      case 109: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==109);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 100: /* db_optr ::= db_optr prec */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 101: /* db_optr ::= db_optr keep */
      case 107: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==107);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.keep = yymsp[0].minor.yy247; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 102: /* db_optr ::= db_optr update */
      case 112: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==112);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 103: /* db_optr ::= db_optr cachelast */
      case 113: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==113);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 104: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
        break;
      case 114: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy179, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy179 = yylhsminor.yy179;
        break;
      case 115: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy403 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy403;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy179, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy179 = yylhsminor.yy179;
        break;
      case 116: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy179, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 117: /* signed ::= INTEGER */
{ yylhsminor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 118: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy403 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 119: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy403 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 123: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy358;}
        break;
      case 124: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy42);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy358 = pCreateTable;
}
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 125: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy358->childTableInfo, &yymsp[0].minor.yy42);
  yylhsminor.yy358 = yymsp[-1].minor.yy358;
}
  yymsp[-1].minor.yy358 = yylhsminor.yy358;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy358 = tSetCreateSqlElems(yymsp[-1].minor.yy247, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy358 = yylhsminor.yy358;
        break;
      case 127: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy358 = tSetCreateSqlElems(yymsp[-5].minor.yy247, yymsp[-1].minor.yy247, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy358 = yylhsminor.yy358;
        break;
      case 128: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy42 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy247, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy42 = yylhsminor.yy42;
        break;
      case 129: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy358 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy114, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy358 = yylhsminor.yy358;
        break;
      case 130: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy247, &yymsp[0].minor.yy179); yylhsminor.yy247 = yymsp[-2].minor.yy247;  }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 131: /* columnlist ::= column */
{yylhsminor.yy247 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy247, &yymsp[0].minor.yy179);}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 132: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy179, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy179);
}
  yymsp[-1].minor.yy179 = yylhsminor.yy179;
        break;
      case 133: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);    }
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 134: /* tagitemlist ::= tagitem */
{ yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1); }
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 135: /* tagitem ::= INTEGER */
      case 136: /* tagitem ::= FLOAT */ yytestcase(yyruleno==136);
      case 137: /* tagitem ::= STRING */ yytestcase(yyruleno==137);
      case 138: /* tagitem ::= BOOL */ yytestcase(yyruleno==138);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 139: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy378, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy378 = yylhsminor.yy378;
        break;
      case 140: /* tagitem ::= MINUS INTEGER */
      case 141: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==141);
      case 142: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==142);
      case 143: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==143);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 144: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy114 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy522, yymsp[-9].minor.yy247, yymsp[-8].minor.yy326, yymsp[-4].minor.yy247, yymsp[-3].minor.yy247, &yymsp[-7].minor.yy430, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy247, &yymsp[0].minor.yy204, &yymsp[-1].minor.yy204);
}
  yymsp[-11].minor.yy114 = yylhsminor.yy114;
        break;
      case 145: /* union ::= select */
{ yylhsminor.yy219 = setSubclause(NULL, yymsp[0].minor.yy114); }
  yymsp[0].minor.yy219 = yylhsminor.yy219;
        break;
      case 146: /* union ::= LP union RP */
{ yymsp[-2].minor.yy219 = yymsp[-1].minor.yy219; }
        break;
      case 147: /* union ::= union UNION ALL select */
{ yylhsminor.yy219 = appendSelectClause(yymsp[-3].minor.yy219, yymsp[0].minor.yy114); }
  yymsp[-3].minor.yy219 = yylhsminor.yy219;
        break;
      case 148: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy219 = appendSelectClause(yymsp[-5].minor.yy219, yymsp[-1].minor.yy114); }
  yymsp[-5].minor.yy219 = yylhsminor.yy219;
        break;
      case 149: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy219, NULL, TSDB_SQL_SELECT); }
        break;
      case 150: /* select ::= SELECT selcollist */
{
  yylhsminor.yy114 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy522, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 151: /* sclp ::= selcollist COMMA */
{yylhsminor.yy522 = yymsp[-1].minor.yy522;}
  yymsp[-1].minor.yy522 = yylhsminor.yy522;
        break;
      case 152: /* sclp ::= */
{yymsp[1].minor.yy522 = 0;}
        break;
      case 153: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy522 = tSqlExprListAppend(yymsp[-3].minor.yy522, yymsp[-1].minor.yy326,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy522 = yylhsminor.yy522;
        break;
      case 154: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy522 = tSqlExprListAppend(yymsp[-1].minor.yy522, pNode, 0, 0);
}
  yymsp[-1].minor.yy522 = yylhsminor.yy522;
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
{yymsp[-1].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 161: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 162: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 163: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(yymsp[-3].minor.yy247, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 164: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy247 = tVariantListAppendToken(yymsp[-4].minor.yy247, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy247 = tVariantListAppendToken(yylhsminor.yy247, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy247 = yylhsminor.yy247;
        break;
      case 165: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 166: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy430.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy430.offset.n = 0; yymsp[-3].minor.yy430.offset.z = NULL; yymsp[-3].minor.yy430.offset.type = 0;}
        break;
      case 167: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy430.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy430.offset = yymsp[-1].minor.yy0;}
        break;
      case 168: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy430, 0, sizeof(yymsp[1].minor.yy430));}
        break;
      case 169: /* fill_opt ::= */
{yymsp[1].minor.yy247 = 0;     }
        break;
      case 170: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy247, &A, -1, 0);
    yymsp[-5].minor.yy247 = yymsp[-1].minor.yy247;
}
        break;
      case 171: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy247 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 172: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 173: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 174: /* orderby_opt ::= */
{yymsp[1].minor.yy247 = 0;}
        break;
      case 175: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 176: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy247 = tVariantListAppend(yymsp[-3].minor.yy247, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-3].minor.yy247 = yylhsminor.yy247;
        break;
      case 177: /* sortlist ::= item sortorder */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[-1].minor.yy378, yymsp[0].minor.yy222);
}
  yymsp[-1].minor.yy247 = yylhsminor.yy247;
        break;
      case 178: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy378, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 179: /* sortorder ::= ASC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 180: /* sortorder ::= DESC */
{ yymsp[0].minor.yy222 = TSDB_ORDER_DESC;}
        break;
      case 181: /* sortorder ::= */
{ yymsp[1].minor.yy222 = TSDB_ORDER_ASC; }
        break;
      case 182: /* groupby_opt ::= */
{ yymsp[1].minor.yy247 = 0;}
        break;
      case 183: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy247 = yymsp[0].minor.yy247;}
        break;
      case 184: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy247 = tVariantListAppend(yymsp[-2].minor.yy247, &yymsp[0].minor.yy378, -1);
}
  yymsp[-2].minor.yy247 = yylhsminor.yy247;
        break;
      case 185: /* grouplist ::= item */
{
  yylhsminor.yy247 = tVariantListAppend(NULL, &yymsp[0].minor.yy378, -1);
}
  yymsp[0].minor.yy247 = yylhsminor.yy247;
        break;
      case 186: /* having_opt ::= */
      case 196: /* where_opt ::= */ yytestcase(yyruleno==196);
      case 234: /* expritem ::= */ yytestcase(yyruleno==234);
{yymsp[1].minor.yy326 = 0;}
        break;
      case 187: /* having_opt ::= HAVING expr */
      case 197: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==197);
{yymsp[-1].minor.yy326 = yymsp[0].minor.yy326;}
        break;
      case 188: /* limit_opt ::= */
      case 192: /* slimit_opt ::= */ yytestcase(yyruleno==192);
{yymsp[1].minor.yy204.limit = -1; yymsp[1].minor.yy204.offset = 0;}
        break;
      case 189: /* limit_opt ::= LIMIT signed */
      case 193: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==193);
{yymsp[-1].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-1].minor.yy204.offset = 0;}
        break;
      case 190: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 191: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 194: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy204.limit = yymsp[-2].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[0].minor.yy403;}
        break;
      case 195: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy204.limit = yymsp[0].minor.yy403;  yymsp[-3].minor.yy204.offset = yymsp[-2].minor.yy403;}
        break;
      case 198: /* expr ::= LP expr RP */
{yylhsminor.yy326 = yymsp[-1].minor.yy326; yylhsminor.yy326->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy326->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 199: /* expr ::= ID */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 200: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 201: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 202: /* expr ::= INTEGER */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 203: /* expr ::= MINUS INTEGER */
      case 204: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==204);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 205: /* expr ::= FLOAT */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 206: /* expr ::= MINUS FLOAT */
      case 207: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==207);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy326 = yylhsminor.yy326;
        break;
      case 208: /* expr ::= STRING */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 209: /* expr ::= NOW */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 210: /* expr ::= VARIABLE */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 211: /* expr ::= BOOL */
{ yylhsminor.yy326 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 212: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy326 = tSqlExprCreateFunction(yymsp[-1].minor.yy522, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy326 = yylhsminor.yy326;
        break;
      case 213: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy326 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy326 = yylhsminor.yy326;
        break;
      case 214: /* expr ::= expr IS NULL */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 215: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-3].minor.yy326, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy326 = yylhsminor.yy326;
        break;
      case 216: /* expr ::= expr LT expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_LT);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 217: /* expr ::= expr GT expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_GT);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 218: /* expr ::= expr LE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_LE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 219: /* expr ::= expr GE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_GE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 220: /* expr ::= expr NE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_NE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 221: /* expr ::= expr EQ expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_EQ);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 222: /* expr ::= expr AND expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_AND);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 223: /* expr ::= expr OR expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_OR); }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 224: /* expr ::= expr PLUS expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_PLUS);  }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 225: /* expr ::= expr MINUS expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_MINUS); }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 226: /* expr ::= expr STAR expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_STAR);  }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 227: /* expr ::= expr SLASH expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_DIVIDE);}
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 228: /* expr ::= expr REM expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_REM);   }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 229: /* expr ::= expr LIKE expr */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-2].minor.yy326, yymsp[0].minor.yy326, TK_LIKE);  }
  yymsp[-2].minor.yy326 = yylhsminor.yy326;
        break;
      case 230: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy326 = tSqlExprCreate(yymsp[-4].minor.yy326, (tSQLExpr*)yymsp[-1].minor.yy522, TK_IN); }
  yymsp[-4].minor.yy326 = yylhsminor.yy326;
        break;
      case 231: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy522 = tSqlExprListAppend(yymsp[-2].minor.yy522,yymsp[0].minor.yy326,0, 0);}
  yymsp[-2].minor.yy522 = yylhsminor.yy522;
        break;
      case 232: /* exprlist ::= expritem */
{yylhsminor.yy522 = tSqlExprListAppend(0,yymsp[0].minor.yy326,0, 0);}
  yymsp[0].minor.yy522 = yylhsminor.yy522;
        break;
      case 233: /* expritem ::= expr */
{yylhsminor.yy326 = yymsp[0].minor.yy326;}
  yymsp[0].minor.yy326 = yylhsminor.yy326;
        break;
      case 235: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy378, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 242: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy247, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
