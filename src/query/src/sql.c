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
#define YYNOCODE 281
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tSQLExpr* yy50;
  SCreateAcctSQL yy79;
  tVariant yy106;
  int64_t yy109;
  int yy172;
  tSQLExprList* yy178;
  SArray* yy221;
  SSubclauseInfo* yy273;
  SIntervalVal yy280;
  SQuerySQL* yy344;
  SCreateTableSQL* yy358;
  SCreatedTableInfo yy416;
  SLimitVal yy454;
  SCreateDBInfo yy478;
  TAOS_FIELD yy503;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             259
#define YYNRULE              242
#define YYNTOKEN             210
#define YY_MAX_SHIFT         258
#define YY_MIN_SHIFTREDUCE   433
#define YY_MAX_SHIFTREDUCE   674
#define YY_ERROR_ACTION      675
#define YY_ACCEPT_ACTION     676
#define YY_NO_ACTION         677
#define YY_MIN_REDUCE        678
#define YY_MAX_REDUCE        919
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
#define YY_ACTTAB_COUNT (578)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   144,  476,  144,   23,  676,  258,  831,  549,   12,  477,
 /*    10 */   906,  169,  907,   37,   38,  820,   39,   40,  143,   23,
 /*    20 */   174,   31,  476,  476,  211,   43,   41,   45,   42,  164,
 /*    30 */   477,  477,  106,   36,   35,  233,  232,   34,   33,   32,
 /*    40 */    37,   38,  805,   39,   40,  820,  148,  174,   31,  163,
 /*    50 */   256,  211,   43,   41,   45,   42,  177,  166,  806,  196,
 /*    60 */    36,   35,  828,  903,   34,   33,   32,  434,  435,  436,
 /*    70 */   437,  438,  439,  440,  441,  442,  443,  444,  445,  257,
 /*    80 */   809,  226,  186,   37,   38,  719,   39,   40,  134,  199,
 /*    90 */   174,   31,  144,  798,  211,   43,   41,   45,   42,  111,
 /*   100 */   111,  168,  907,   36,   35,   57,  243,   34,   33,   32,
 /*   110 */    17,  224,  251,  250,  223,  222,  221,  249,  220,  248,
 /*   120 */   247,  246,  219,  245,  244,  179,  902,  778,  628,  766,
 /*   130 */   767,  768,  769,  770,  771,  772,  773,  774,  775,  776,
 /*   140 */   777,  779,  780,   38,   18,   39,   40,   23,  809,  174,
 /*   150 */    31,  901,   28,  211,   43,   41,   45,   42,  111,  208,
 /*   160 */   859,   62,   36,   35,   23,  212,   34,   33,   32,  226,
 /*   170 */    39,   40,  180,  111,  174,   31,  160,   65,  211,   43,
 /*   180 */    41,   45,   42,   13,  178,  181,  806,   36,   35,  189,
 /*   190 */   584,   34,   33,   32,  173,  641,  193,  192,  632,  156,
 /*   200 */   635,  231,  638,  806,  161,  157,  797,   22,  799,   93,
 /*   210 */    92,  151,  173,  641,  609,  610,  632,  807,  635,  860,
 /*   220 */   638,  206,   17,  252,  251,  250,  170,  171,   23,  249,
 /*   230 */   210,  248,  247,  246,   63,  245,  244,  182,    9,   18,
 /*   240 */   230,  229,   66,  121,  170,  171,  809,   28,  784,   67,
 /*   250 */   146,  782,  783,  255,  254,   99,  785,  644,  787,  788,
 /*   260 */   786,   81,  789,  790,  588,  235,  243,  806,  195,  916,
 /*   270 */    43,   41,   45,   42,  728,  159,  596,  134,   36,   35,
 /*   280 */   800,  108,   34,   33,   32,  568,  720,   64,  565,  134,
 /*   290 */   566,  630,  567,   44,   79,   83,  808,    3,  125,   58,
 /*   300 */    88,   91,   82,   73,   69,   72,  640,  147,   85,   36,
 /*   310 */    35,   44,  105,   34,   33,   32,  183,  184,  576,  581,
 /*   320 */    28,  639,  138,  136,  640,   19,  198,  631,   96,   95,
 /*   330 */    94,   34,   33,   32,  172,  600,  601,   49,  660,  639,
 /*   340 */    48,   15,  642,   14,  634,   52,  637,   14,  633,  557,
 /*   350 */   636,  214,  149,  558,   24,  150,   24,   50,   48,   78,
 /*   360 */    77,  154,   55,   53,  548,  572,  570,  573,  571,  155,
 /*   370 */    11,   10,   90,   89,  153,    4,  104,  102,  142,  152,
 /*   380 */   145,  870,  869,  175,  822,  830,   29,  866,  865,  176,
 /*   390 */   569,  234,  837,  839,  107,  852,  851,  122,  123,  120,
 /*   400 */   124,  730,  595,  218,   28,  140,   26,  197,  227,  727,
 /*   410 */   228,  915,   75,  914,  912,  126,  748,   27,   25,  141,
 /*   420 */   717,   84,  715,   86,   87,  103,   54,  819,  713,  712,
 /*   430 */   185,  200,  135,  710,  709,  708,  707,  706,  137,  704,
 /*   440 */   702,  165,  700,  698,  696,  204,  139,   51,   46,  112,
 /*   450 */   209,  207,  205,  203,  201,   59,   30,   60,  853,   80,
 /*   460 */   236,  237,  238,  239,  240,  241,  242,  253,  674,  188,
 /*   470 */   162,  187,  216,  217,  673,  158,  190,  191,   70,  672,
 /*   480 */   665,  194,  711,  198,  578,   97,   98,  597,  705,  129,
 /*   490 */    56,  128,  749,  127,  130,  131,  133,  132,    1,  697,
 /*   500 */   109,   61,    2,  167,  202,  804,  117,  113,  114,  602,
 /*   510 */   115,  116,  118,  119,  110,   20,    6,    7,  643,   21,
 /*   520 */     5,    8,  645,   16,   68,  213,  517,  215,  513,   66,
 /*   530 */   511,  510,  509,  506,  480,  225,   74,   47,   71,   76,
 /*   540 */    24,  551,  550,  547,  501,  499,  491,  497,  493,  495,
 /*   550 */   489,  487,  519,  518,  516,  515,  514,  512,  508,  507,
 /*   560 */    48,  478,  449,  447,  678,  677,  677,  677,  677,  677,
 /*   570 */   677,  677,  677,  677,  677,  677,  100,  101,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   269,    1,  269,  214,  211,  212,  214,    5,  269,    9,
 /*    10 */   279,  278,  279,   13,   14,  252,   16,   17,  269,  214,
 /*    20 */    20,   21,    1,    1,   24,   25,   26,   27,   28,  266,
 /*    30 */     9,    9,  214,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  253,   16,   17,  252,  269,   20,   21,  213,
 /*    50 */   214,   24,   25,   26,   27,   28,  251,  231,  253,  266,
 /*    60 */    33,   34,  270,  269,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */   254,   76,   60,   13,   14,  218,   16,   17,  221,  271,
 /*    90 */    20,   21,  269,    0,   24,   25,   26,   27,   28,  214,
 /*   100 */   214,  278,  279,   33,   34,  105,   78,   37,   38,   39,
 /*   110 */    85,   86,   87,   88,   89,   90,   91,   92,   93,   94,
 /*   120 */    95,   96,   97,   98,   99,  231,  269,  230,  101,  232,
 /*   130 */   233,  234,  235,  236,  237,  238,  239,  240,  241,  242,
 /*   140 */   243,  244,  245,   14,  100,   16,   17,  214,  254,   20,
 /*   150 */    21,  269,  108,   24,   25,   26,   27,   28,  214,  273,
 /*   160 */   275,  275,   33,   34,  214,   15,   37,   38,   39,   76,
 /*   170 */    16,   17,   66,  214,   20,   21,  269,  219,   24,   25,
 /*   180 */    26,   27,   28,   44,  251,  214,  253,   33,   34,  130,
 /*   190 */    37,   37,   38,   39,    1,    2,  137,  138,    5,   60,
 /*   200 */     7,  251,    9,  253,  269,   66,  248,  249,  250,   70,
 /*   210 */    71,   72,    1,    2,  119,  120,    5,  246,    7,  275,
 /*   220 */     9,  277,   85,  231,   87,   88,   33,   34,  214,   92,
 /*   230 */    37,   94,   95,   96,  275,   98,   99,  131,  100,  100,
 /*   240 */   134,  135,  104,  105,   33,   34,  254,  108,  230,  219,
 /*   250 */   269,  233,  234,   63,   64,   65,  238,  107,  240,  241,
 /*   260 */   242,   73,  244,  245,  111,  251,   78,  253,  129,  254,
 /*   270 */    25,   26,   27,   28,  218,  136,  101,  221,   33,   34,
 /*   280 */   250,  106,   37,   38,   39,    2,  218,  255,    5,  221,
 /*   290 */     7,    1,    9,  100,   61,   62,  254,   61,   62,  267,
 /*   300 */    67,   68,   69,   67,   68,   69,  113,  269,   75,   33,
 /*   310 */    34,  100,  100,   37,   38,   39,   33,   34,  101,  106,
 /*   320 */   108,  128,   61,   62,  113,  112,  109,   37,   67,   68,
 /*   330 */    69,   37,   38,   39,   59,  101,  101,  106,  101,  128,
 /*   340 */   106,  106,  101,  106,    5,  106,    7,  106,    5,  101,
 /*   350 */     7,  101,  269,  101,  106,  269,  106,  126,  106,  132,
 /*   360 */   133,  269,  100,  124,  102,    5,    5,    7,    7,  269,
 /*   370 */   132,  133,   73,   74,  269,  100,   61,   62,  269,  269,
 /*   380 */   269,  247,  247,  247,  252,  214,  268,  247,  247,  247,
 /*   390 */   107,  247,  214,  214,  214,  276,  276,  214,  214,  256,
 /*   400 */   214,  214,  113,  214,  108,  214,  214,  252,  214,  214,
 /*   410 */   214,  214,  214,  214,  214,  214,  214,  214,  214,  214,
 /*   420 */   214,  214,  214,  214,  214,   59,  123,  265,  214,  214,
 /*   430 */   214,  272,  214,  214,  214,  214,  214,  214,  214,  214,
 /*   440 */   214,  272,  214,  214,  214,  272,  214,  125,  122,  264,
 /*   450 */   117,  121,  116,  115,  114,  215,  127,  215,  215,   84,
 /*   460 */    83,   49,   80,   82,   53,   81,   79,   76,    5,    5,
 /*   470 */   215,  139,  215,  215,    5,  215,  139,    5,  219,    5,
 /*   480 */    86,  130,  215,  109,  101,  216,  216,  101,  215,  223,
 /*   490 */   110,  227,  229,  228,  226,  224,  222,  225,  220,  215,
 /*   500 */   100,  106,  217,    1,  100,  252,  259,  263,  262,  101,
 /*   510 */   261,  260,  258,  257,  100,  106,  118,  118,  101,  106,
 /*   520 */   100,  100,  107,  100,   73,  103,    9,  103,    5,  104,
 /*   530 */     5,    5,    5,    5,   77,   15,  133,   16,   73,  133,
 /*   540 */   106,    5,    5,  101,    5,    5,    5,    5,    5,    5,
 /*   550 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   560 */   106,   77,   59,   58,    0,  280,  280,  280,  280,  280,
 /*   570 */   280,  280,  280,  280,  280,  280,   21,   21,  280,  280,
 /*   580 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   590 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   600 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   610 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   620 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   630 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   640 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   650 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   660 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   670 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   680 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   690 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   700 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   710 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   720 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   730 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   740 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   750 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   760 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   770 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   780 */   280,  280,  280,  280,  280,  280,  280,  280,
};
#define YY_SHIFT_COUNT    (258)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (564)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   139,   25,  137,    5,  193,  211,   21,   21,   21,   21,
 /*    10 */    21,   21,    0,   22,  211,  283,  283,  283,   44,   21,
 /*    20 */    21,   21,   93,   21,   21,  188,   28,   28,  578,  211,
 /*    30 */   211,  211,  211,  211,  211,  211,  211,  211,  211,  211,
 /*    40 */   211,  211,  211,  211,  211,  211,  211,  283,  283,    2,
 /*    50 */     2,    2,    2,    2,    2,    2,  212,   21,  153,   21,
 /*    60 */    21,   21,   95,   95,  213,   21,   21,   21,   21,   21,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,  296,  366,  366,  289,  289,
 /*   110 */   289,  366,  303,  322,  326,  333,  330,  336,  338,  340,
 /*   120 */   329,  296,  366,  366,  366,    5,  366,  375,  377,  412,
 /*   130 */   382,  381,  411,  384,  387,  366,  391,  366,  391,  366,
 /*   140 */   578,  578,   27,   70,   70,   70,  129,  154,  245,  245,
 /*   150 */   245,  233,  276,  276,  276,  276,  236,  261,  106,   59,
 /*   160 */   294,  294,  138,  190,  217,  175,  234,  235,  237,  241,
 /*   170 */   339,  343,  290,  275,  150,  231,  239,  248,  250,  252,
 /*   180 */   227,  262,  238,  360,  361,  299,  315,  463,  332,  464,
 /*   190 */   469,  337,  472,  474,  394,  351,  374,  383,  380,  395,
 /*   200 */   386,  400,  502,  404,  408,  414,  409,  398,  413,  399,
 /*   210 */   417,  420,  415,  421,  422,  423,  424,  425,  451,  517,
 /*   220 */   523,  525,  526,  527,  528,  457,  520,  465,  521,  403,
 /*   230 */   406,  434,  536,  537,  442,  434,  539,  540,  541,  542,
 /*   240 */   543,  544,  545,  546,  547,  548,  549,  550,  551,  552,
 /*   250 */   553,  554,  454,  484,  555,  556,  503,  505,  564,
};
#define YY_REDUCE_COUNT (141)
#define YY_REDUCE_MIN   (-269)
#define YY_REDUCE_MAX   (285)
static const short yy_reduce_ofst[] = {
 /*     0 */  -207, -103,   18,  -42, -267, -177,  -56, -114, -195,  -67,
 /*    10 */   -50,   14, -208, -164, -269, -174, -106,   -8, -237, -182,
 /*    20 */  -115,  -41,   30,  -29, -211, -133,   56,   68,   32, -261,
 /*    30 */  -251, -223, -206, -143, -118,  -93,  -65,  -19,   38,   83,
 /*    40 */    86,   92,  100,  105,  109,  110,  111,   15,   42,  134,
 /*    50 */   135,  136,  140,  141,  142,  144,  132,  171,  118,  178,
 /*    60 */   179,  180,  119,  120,  143,  183,  184,  186,  187,  189,
 /*    70 */   191,  192,  194,  195,  196,  197,  198,  199,  200,  201,
 /*    80 */   202,  203,  204,  205,  206,  207,  208,  209,  210,  214,
 /*    90 */   215,  216,  218,  219,  220,  221,  222,  223,  224,  225,
 /*   100 */   226,  228,  229,  230,  232,  155,  240,  242,  159,  169,
 /*   110 */   173,  243,  162,  185,  244,  246,  249,  251,  247,  254,
 /*   120 */   256,  253,  255,  257,  258,  259,  260,  263,  265,  264,
 /*   130 */   266,  268,  271,  272,  274,  267,  269,  273,  270,  284,
 /*   140 */   278,  285,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   675,  729,  718,  726,  909,  909,  675,  675,  675,  675,
 /*    10 */   675,  675,  832,  693,  909,  675,  675,  675,  675,  675,
 /*    20 */   675,  675,  726,  675,  675,  731,  731,  731,  827,  675,
 /*    30 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*    40 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*    50 */   675,  675,  675,  675,  675,  675,  675,  675,  834,  836,
 /*    60 */   838,  675,  856,  856,  825,  675,  675,  675,  675,  675,
 /*    70 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*    80 */   675,  675,  675,  675,  716,  675,  714,  675,  675,  675,
 /*    90 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  703,
 /*   100 */   675,  675,  675,  675,  675,  675,  695,  695,  675,  675,
 /*   110 */   675,  695,  863,  867,  861,  849,  857,  848,  844,  843,
 /*   120 */   871,  675,  695,  695,  695,  726,  695,  747,  745,  743,
 /*   130 */   735,  741,  737,  739,  733,  695,  724,  695,  724,  695,
 /*   140 */   765,  781,  675,  872,  908,  862,  898,  897,  904,  896,
 /*   150 */   895,  675,  891,  892,  894,  893,  675,  675,  675,  675,
 /*   160 */   900,  899,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   170 */   675,  675,  675,  874,  675,  868,  864,  675,  675,  675,
 /*   180 */   675,  791,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   190 */   675,  675,  675,  675,  675,  675,  824,  675,  675,  835,
 /*   200 */   675,  675,  675,  675,  675,  675,  858,  675,  850,  675,
 /*   210 */   675,  675,  675,  675,  801,  675,  675,  675,  675,  675,
 /*   220 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   230 */   675,  913,  675,  675,  675,  911,  675,  675,  675,  675,
 /*   240 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   250 */   675,  675,  750,  675,  701,  699,  675,  691,  675,
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
    1,  /*     STABLE => ID */
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
  /*   67 */ "DNODE",
  /*   68 */ "USER",
  /*   69 */ "ACCOUNT",
  /*   70 */ "USE",
  /*   71 */ "DESCRIBE",
  /*   72 */ "ALTER",
  /*   73 */ "PASS",
  /*   74 */ "PRIVILEGE",
  /*   75 */ "LOCAL",
  /*   76 */ "IF",
  /*   77 */ "EXISTS",
  /*   78 */ "PPS",
  /*   79 */ "TSERIES",
  /*   80 */ "DBS",
  /*   81 */ "STORAGE",
  /*   82 */ "QTIME",
  /*   83 */ "CONNS",
  /*   84 */ "STATE",
  /*   85 */ "KEEP",
  /*   86 */ "CACHE",
  /*   87 */ "REPLICA",
  /*   88 */ "QUORUM",
  /*   89 */ "DAYS",
  /*   90 */ "MINROWS",
  /*   91 */ "MAXROWS",
  /*   92 */ "BLOCKS",
  /*   93 */ "CTIME",
  /*   94 */ "WAL",
  /*   95 */ "FSYNC",
  /*   96 */ "COMP",
  /*   97 */ "PRECISION",
  /*   98 */ "UPDATE",
  /*   99 */ "CACHELAST",
  /*  100 */ "LP",
  /*  101 */ "RP",
  /*  102 */ "UNSIGNED",
  /*  103 */ "TAGS",
  /*  104 */ "USING",
  /*  105 */ "AS",
  /*  106 */ "COMMA",
  /*  107 */ "NULL",
  /*  108 */ "SELECT",
  /*  109 */ "UNION",
  /*  110 */ "ALL",
  /*  111 */ "DISTINCT",
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
  /*  206 */ "STABLE",
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
  /*  268 */ "distinct",
  /*  269 */ "expr",
  /*  270 */ "as",
  /*  271 */ "tablelist",
  /*  272 */ "tmvar",
  /*  273 */ "sortlist",
  /*  274 */ "sortitem",
  /*  275 */ "item",
  /*  276 */ "sortorder",
  /*  277 */ "grouplist",
  /*  278 */ "exprlist",
  /*  279 */ "expritem",
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
 /*  28 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  29 */ "cmd ::= DROP DNODE ids",
 /*  30 */ "cmd ::= DROP USER ids",
 /*  31 */ "cmd ::= DROP ACCOUNT ids",
 /*  32 */ "cmd ::= USE ids",
 /*  33 */ "cmd ::= DESCRIBE ids cpxName",
 /*  34 */ "cmd ::= ALTER USER ids PASS ids",
 /*  35 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  36 */ "cmd ::= ALTER DNODE ids ids",
 /*  37 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  38 */ "cmd ::= ALTER LOCAL ids",
 /*  39 */ "cmd ::= ALTER LOCAL ids ids",
 /*  40 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  41 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  42 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  43 */ "ids ::= ID",
 /*  44 */ "ids ::= STRING",
 /*  45 */ "ifexists ::= IF EXISTS",
 /*  46 */ "ifexists ::=",
 /*  47 */ "ifnotexists ::= IF NOT EXISTS",
 /*  48 */ "ifnotexists ::=",
 /*  49 */ "cmd ::= CREATE DNODE ids",
 /*  50 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  51 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  52 */ "cmd ::= CREATE USER ids PASS ids",
 /*  53 */ "pps ::=",
 /*  54 */ "pps ::= PPS INTEGER",
 /*  55 */ "tseries ::=",
 /*  56 */ "tseries ::= TSERIES INTEGER",
 /*  57 */ "dbs ::=",
 /*  58 */ "dbs ::= DBS INTEGER",
 /*  59 */ "streams ::=",
 /*  60 */ "streams ::= STREAMS INTEGER",
 /*  61 */ "storage ::=",
 /*  62 */ "storage ::= STORAGE INTEGER",
 /*  63 */ "qtime ::=",
 /*  64 */ "qtime ::= QTIME INTEGER",
 /*  65 */ "users ::=",
 /*  66 */ "users ::= USERS INTEGER",
 /*  67 */ "conns ::=",
 /*  68 */ "conns ::= CONNS INTEGER",
 /*  69 */ "state ::=",
 /*  70 */ "state ::= STATE ids",
 /*  71 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  72 */ "keep ::= KEEP tagitemlist",
 /*  73 */ "cache ::= CACHE INTEGER",
 /*  74 */ "replica ::= REPLICA INTEGER",
 /*  75 */ "quorum ::= QUORUM INTEGER",
 /*  76 */ "days ::= DAYS INTEGER",
 /*  77 */ "minrows ::= MINROWS INTEGER",
 /*  78 */ "maxrows ::= MAXROWS INTEGER",
 /*  79 */ "blocks ::= BLOCKS INTEGER",
 /*  80 */ "ctime ::= CTIME INTEGER",
 /*  81 */ "wal ::= WAL INTEGER",
 /*  82 */ "fsync ::= FSYNC INTEGER",
 /*  83 */ "comp ::= COMP INTEGER",
 /*  84 */ "prec ::= PRECISION STRING",
 /*  85 */ "update ::= UPDATE INTEGER",
 /*  86 */ "cachelast ::= CACHELAST INTEGER",
 /*  87 */ "db_optr ::=",
 /*  88 */ "db_optr ::= db_optr cache",
 /*  89 */ "db_optr ::= db_optr replica",
 /*  90 */ "db_optr ::= db_optr quorum",
 /*  91 */ "db_optr ::= db_optr days",
 /*  92 */ "db_optr ::= db_optr minrows",
 /*  93 */ "db_optr ::= db_optr maxrows",
 /*  94 */ "db_optr ::= db_optr blocks",
 /*  95 */ "db_optr ::= db_optr ctime",
 /*  96 */ "db_optr ::= db_optr wal",
 /*  97 */ "db_optr ::= db_optr fsync",
 /*  98 */ "db_optr ::= db_optr comp",
 /*  99 */ "db_optr ::= db_optr prec",
 /* 100 */ "db_optr ::= db_optr keep",
 /* 101 */ "db_optr ::= db_optr update",
 /* 102 */ "db_optr ::= db_optr cachelast",
 /* 103 */ "alter_db_optr ::=",
 /* 104 */ "alter_db_optr ::= alter_db_optr replica",
 /* 105 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 106 */ "alter_db_optr ::= alter_db_optr keep",
 /* 107 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 108 */ "alter_db_optr ::= alter_db_optr comp",
 /* 109 */ "alter_db_optr ::= alter_db_optr wal",
 /* 110 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 111 */ "alter_db_optr ::= alter_db_optr update",
 /* 112 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 113 */ "typename ::= ids",
 /* 114 */ "typename ::= ids LP signed RP",
 /* 115 */ "typename ::= ids UNSIGNED",
 /* 116 */ "signed ::= INTEGER",
 /* 117 */ "signed ::= PLUS INTEGER",
 /* 118 */ "signed ::= MINUS INTEGER",
 /* 119 */ "cmd ::= CREATE TABLE create_table_args",
 /* 120 */ "cmd ::= CREATE TABLE create_table_list",
 /* 121 */ "create_table_list ::= create_from_stable",
 /* 122 */ "create_table_list ::= create_table_list create_from_stable",
 /* 123 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 124 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 125 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 126 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 127 */ "columnlist ::= columnlist COMMA column",
 /* 128 */ "columnlist ::= column",
 /* 129 */ "column ::= ids typename",
 /* 130 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 131 */ "tagitemlist ::= tagitem",
 /* 132 */ "tagitem ::= INTEGER",
 /* 133 */ "tagitem ::= FLOAT",
 /* 134 */ "tagitem ::= STRING",
 /* 135 */ "tagitem ::= BOOL",
 /* 136 */ "tagitem ::= NULL",
 /* 137 */ "tagitem ::= MINUS INTEGER",
 /* 138 */ "tagitem ::= MINUS FLOAT",
 /* 139 */ "tagitem ::= PLUS INTEGER",
 /* 140 */ "tagitem ::= PLUS FLOAT",
 /* 141 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 142 */ "union ::= select",
 /* 143 */ "union ::= LP union RP",
 /* 144 */ "union ::= union UNION ALL select",
 /* 145 */ "union ::= union UNION ALL LP select RP",
 /* 146 */ "cmd ::= union",
 /* 147 */ "select ::= SELECT selcollist",
 /* 148 */ "sclp ::= selcollist COMMA",
 /* 149 */ "sclp ::=",
 /* 150 */ "selcollist ::= sclp distinct expr as",
 /* 151 */ "selcollist ::= sclp STAR",
 /* 152 */ "as ::= AS ids",
 /* 153 */ "as ::= ids",
 /* 154 */ "as ::=",
 /* 155 */ "distinct ::= DISTINCT",
 /* 156 */ "distinct ::=",
 /* 157 */ "from ::= FROM tablelist",
 /* 158 */ "tablelist ::= ids cpxName",
 /* 159 */ "tablelist ::= ids cpxName ids",
 /* 160 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 161 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 162 */ "tmvar ::= VARIABLE",
 /* 163 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 164 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 165 */ "interval_opt ::=",
 /* 166 */ "fill_opt ::=",
 /* 167 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 168 */ "fill_opt ::= FILL LP ID RP",
 /* 169 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 170 */ "sliding_opt ::=",
 /* 171 */ "orderby_opt ::=",
 /* 172 */ "orderby_opt ::= ORDER BY sortlist",
 /* 173 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 174 */ "sortlist ::= item sortorder",
 /* 175 */ "item ::= ids cpxName",
 /* 176 */ "sortorder ::= ASC",
 /* 177 */ "sortorder ::= DESC",
 /* 178 */ "sortorder ::=",
 /* 179 */ "groupby_opt ::=",
 /* 180 */ "groupby_opt ::= GROUP BY grouplist",
 /* 181 */ "grouplist ::= grouplist COMMA item",
 /* 182 */ "grouplist ::= item",
 /* 183 */ "having_opt ::=",
 /* 184 */ "having_opt ::= HAVING expr",
 /* 185 */ "limit_opt ::=",
 /* 186 */ "limit_opt ::= LIMIT signed",
 /* 187 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 188 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 189 */ "slimit_opt ::=",
 /* 190 */ "slimit_opt ::= SLIMIT signed",
 /* 191 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 192 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 193 */ "where_opt ::=",
 /* 194 */ "where_opt ::= WHERE expr",
 /* 195 */ "expr ::= LP expr RP",
 /* 196 */ "expr ::= ID",
 /* 197 */ "expr ::= ID DOT ID",
 /* 198 */ "expr ::= ID DOT STAR",
 /* 199 */ "expr ::= INTEGER",
 /* 200 */ "expr ::= MINUS INTEGER",
 /* 201 */ "expr ::= PLUS INTEGER",
 /* 202 */ "expr ::= FLOAT",
 /* 203 */ "expr ::= MINUS FLOAT",
 /* 204 */ "expr ::= PLUS FLOAT",
 /* 205 */ "expr ::= STRING",
 /* 206 */ "expr ::= NOW",
 /* 207 */ "expr ::= VARIABLE",
 /* 208 */ "expr ::= BOOL",
 /* 209 */ "expr ::= ID LP exprlist RP",
 /* 210 */ "expr ::= ID LP STAR RP",
 /* 211 */ "expr ::= expr IS NULL",
 /* 212 */ "expr ::= expr IS NOT NULL",
 /* 213 */ "expr ::= expr LT expr",
 /* 214 */ "expr ::= expr GT expr",
 /* 215 */ "expr ::= expr LE expr",
 /* 216 */ "expr ::= expr GE expr",
 /* 217 */ "expr ::= expr NE expr",
 /* 218 */ "expr ::= expr EQ expr",
 /* 219 */ "expr ::= expr AND expr",
 /* 220 */ "expr ::= expr OR expr",
 /* 221 */ "expr ::= expr PLUS expr",
 /* 222 */ "expr ::= expr MINUS expr",
 /* 223 */ "expr ::= expr STAR expr",
 /* 224 */ "expr ::= expr SLASH expr",
 /* 225 */ "expr ::= expr REM expr",
 /* 226 */ "expr ::= expr LIKE expr",
 /* 227 */ "expr ::= expr IN LP exprlist RP",
 /* 228 */ "exprlist ::= exprlist COMMA expritem",
 /* 229 */ "exprlist ::= expritem",
 /* 230 */ "expritem ::= expr",
 /* 231 */ "expritem ::=",
 /* 232 */ "cmd ::= RESET QUERY CACHE",
 /* 233 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 234 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 235 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 236 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 237 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 238 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 239 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 240 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 241 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 251: /* columnlist */
    case 259: /* fill_opt */
    case 261: /* groupby_opt */
    case 262: /* orderby_opt */
    case 273: /* sortlist */
    case 277: /* grouplist */
{
taosArrayDestroy((yypminor->yy221));
}
      break;
    case 249: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy358));
}
      break;
    case 252: /* select */
{
doDestroyQuerySql((yypminor->yy344));
}
      break;
    case 255: /* selcollist */
    case 267: /* sclp */
    case 278: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy178));
}
      break;
    case 257: /* where_opt */
    case 263: /* having_opt */
    case 269: /* expr */
    case 279: /* expritem */
{
tSqlExprDestroy((yypminor->yy50));
}
      break;
    case 266: /* union */
{
destroyAllSelectClause((yypminor->yy273));
}
      break;
    case 274: /* sortitem */
{
tVariantDestroy(&(yypminor->yy106));
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
  {  212,   -4 }, /* (28) cmd ::= DROP DATABASE ifexists ids */
  {  212,   -3 }, /* (29) cmd ::= DROP DNODE ids */
  {  212,   -3 }, /* (30) cmd ::= DROP USER ids */
  {  212,   -3 }, /* (31) cmd ::= DROP ACCOUNT ids */
  {  212,   -2 }, /* (32) cmd ::= USE ids */
  {  212,   -3 }, /* (33) cmd ::= DESCRIBE ids cpxName */
  {  212,   -5 }, /* (34) cmd ::= ALTER USER ids PASS ids */
  {  212,   -5 }, /* (35) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  212,   -4 }, /* (36) cmd ::= ALTER DNODE ids ids */
  {  212,   -5 }, /* (37) cmd ::= ALTER DNODE ids ids ids */
  {  212,   -3 }, /* (38) cmd ::= ALTER LOCAL ids */
  {  212,   -4 }, /* (39) cmd ::= ALTER LOCAL ids ids */
  {  212,   -4 }, /* (40) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  212,   -4 }, /* (41) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  212,   -6 }, /* (42) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  214,   -1 }, /* (43) ids ::= ID */
  {  214,   -1 }, /* (44) ids ::= STRING */
  {  216,   -2 }, /* (45) ifexists ::= IF EXISTS */
  {  216,    0 }, /* (46) ifexists ::= */
  {  219,   -3 }, /* (47) ifnotexists ::= IF NOT EXISTS */
  {  219,    0 }, /* (48) ifnotexists ::= */
  {  212,   -3 }, /* (49) cmd ::= CREATE DNODE ids */
  {  212,   -6 }, /* (50) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  212,   -5 }, /* (51) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  212,   -5 }, /* (52) cmd ::= CREATE USER ids PASS ids */
  {  221,    0 }, /* (53) pps ::= */
  {  221,   -2 }, /* (54) pps ::= PPS INTEGER */
  {  222,    0 }, /* (55) tseries ::= */
  {  222,   -2 }, /* (56) tseries ::= TSERIES INTEGER */
  {  223,    0 }, /* (57) dbs ::= */
  {  223,   -2 }, /* (58) dbs ::= DBS INTEGER */
  {  224,    0 }, /* (59) streams ::= */
  {  224,   -2 }, /* (60) streams ::= STREAMS INTEGER */
  {  225,    0 }, /* (61) storage ::= */
  {  225,   -2 }, /* (62) storage ::= STORAGE INTEGER */
  {  226,    0 }, /* (63) qtime ::= */
  {  226,   -2 }, /* (64) qtime ::= QTIME INTEGER */
  {  227,    0 }, /* (65) users ::= */
  {  227,   -2 }, /* (66) users ::= USERS INTEGER */
  {  228,    0 }, /* (67) conns ::= */
  {  228,   -2 }, /* (68) conns ::= CONNS INTEGER */
  {  229,    0 }, /* (69) state ::= */
  {  229,   -2 }, /* (70) state ::= STATE ids */
  {  218,   -9 }, /* (71) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  230,   -2 }, /* (72) keep ::= KEEP tagitemlist */
  {  232,   -2 }, /* (73) cache ::= CACHE INTEGER */
  {  233,   -2 }, /* (74) replica ::= REPLICA INTEGER */
  {  234,   -2 }, /* (75) quorum ::= QUORUM INTEGER */
  {  235,   -2 }, /* (76) days ::= DAYS INTEGER */
  {  236,   -2 }, /* (77) minrows ::= MINROWS INTEGER */
  {  237,   -2 }, /* (78) maxrows ::= MAXROWS INTEGER */
  {  238,   -2 }, /* (79) blocks ::= BLOCKS INTEGER */
  {  239,   -2 }, /* (80) ctime ::= CTIME INTEGER */
  {  240,   -2 }, /* (81) wal ::= WAL INTEGER */
  {  241,   -2 }, /* (82) fsync ::= FSYNC INTEGER */
  {  242,   -2 }, /* (83) comp ::= COMP INTEGER */
  {  243,   -2 }, /* (84) prec ::= PRECISION STRING */
  {  244,   -2 }, /* (85) update ::= UPDATE INTEGER */
  {  245,   -2 }, /* (86) cachelast ::= CACHELAST INTEGER */
  {  220,    0 }, /* (87) db_optr ::= */
  {  220,   -2 }, /* (88) db_optr ::= db_optr cache */
  {  220,   -2 }, /* (89) db_optr ::= db_optr replica */
  {  220,   -2 }, /* (90) db_optr ::= db_optr quorum */
  {  220,   -2 }, /* (91) db_optr ::= db_optr days */
  {  220,   -2 }, /* (92) db_optr ::= db_optr minrows */
  {  220,   -2 }, /* (93) db_optr ::= db_optr maxrows */
  {  220,   -2 }, /* (94) db_optr ::= db_optr blocks */
  {  220,   -2 }, /* (95) db_optr ::= db_optr ctime */
  {  220,   -2 }, /* (96) db_optr ::= db_optr wal */
  {  220,   -2 }, /* (97) db_optr ::= db_optr fsync */
  {  220,   -2 }, /* (98) db_optr ::= db_optr comp */
  {  220,   -2 }, /* (99) db_optr ::= db_optr prec */
  {  220,   -2 }, /* (100) db_optr ::= db_optr keep */
  {  220,   -2 }, /* (101) db_optr ::= db_optr update */
  {  220,   -2 }, /* (102) db_optr ::= db_optr cachelast */
  {  217,    0 }, /* (103) alter_db_optr ::= */
  {  217,   -2 }, /* (104) alter_db_optr ::= alter_db_optr replica */
  {  217,   -2 }, /* (105) alter_db_optr ::= alter_db_optr quorum */
  {  217,   -2 }, /* (106) alter_db_optr ::= alter_db_optr keep */
  {  217,   -2 }, /* (107) alter_db_optr ::= alter_db_optr blocks */
  {  217,   -2 }, /* (108) alter_db_optr ::= alter_db_optr comp */
  {  217,   -2 }, /* (109) alter_db_optr ::= alter_db_optr wal */
  {  217,   -2 }, /* (110) alter_db_optr ::= alter_db_optr fsync */
  {  217,   -2 }, /* (111) alter_db_optr ::= alter_db_optr update */
  {  217,   -2 }, /* (112) alter_db_optr ::= alter_db_optr cachelast */
  {  246,   -1 }, /* (113) typename ::= ids */
  {  246,   -4 }, /* (114) typename ::= ids LP signed RP */
  {  246,   -2 }, /* (115) typename ::= ids UNSIGNED */
  {  247,   -1 }, /* (116) signed ::= INTEGER */
  {  247,   -2 }, /* (117) signed ::= PLUS INTEGER */
  {  247,   -2 }, /* (118) signed ::= MINUS INTEGER */
  {  212,   -3 }, /* (119) cmd ::= CREATE TABLE create_table_args */
  {  212,   -3 }, /* (120) cmd ::= CREATE TABLE create_table_list */
  {  249,   -1 }, /* (121) create_table_list ::= create_from_stable */
  {  249,   -2 }, /* (122) create_table_list ::= create_table_list create_from_stable */
  {  248,   -6 }, /* (123) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  248,  -10 }, /* (124) create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  250,  -10 }, /* (125) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  248,   -5 }, /* (126) create_table_args ::= ifnotexists ids cpxName AS select */
  {  251,   -3 }, /* (127) columnlist ::= columnlist COMMA column */
  {  251,   -1 }, /* (128) columnlist ::= column */
  {  253,   -2 }, /* (129) column ::= ids typename */
  {  231,   -3 }, /* (130) tagitemlist ::= tagitemlist COMMA tagitem */
  {  231,   -1 }, /* (131) tagitemlist ::= tagitem */
  {  254,   -1 }, /* (132) tagitem ::= INTEGER */
  {  254,   -1 }, /* (133) tagitem ::= FLOAT */
  {  254,   -1 }, /* (134) tagitem ::= STRING */
  {  254,   -1 }, /* (135) tagitem ::= BOOL */
  {  254,   -1 }, /* (136) tagitem ::= NULL */
  {  254,   -2 }, /* (137) tagitem ::= MINUS INTEGER */
  {  254,   -2 }, /* (138) tagitem ::= MINUS FLOAT */
  {  254,   -2 }, /* (139) tagitem ::= PLUS INTEGER */
  {  254,   -2 }, /* (140) tagitem ::= PLUS FLOAT */
  {  252,  -12 }, /* (141) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  266,   -1 }, /* (142) union ::= select */
  {  266,   -3 }, /* (143) union ::= LP union RP */
  {  266,   -4 }, /* (144) union ::= union UNION ALL select */
  {  266,   -6 }, /* (145) union ::= union UNION ALL LP select RP */
  {  212,   -1 }, /* (146) cmd ::= union */
  {  252,   -2 }, /* (147) select ::= SELECT selcollist */
  {  267,   -2 }, /* (148) sclp ::= selcollist COMMA */
  {  267,    0 }, /* (149) sclp ::= */
  {  255,   -4 }, /* (150) selcollist ::= sclp distinct expr as */
  {  255,   -2 }, /* (151) selcollist ::= sclp STAR */
  {  270,   -2 }, /* (152) as ::= AS ids */
  {  270,   -1 }, /* (153) as ::= ids */
  {  270,    0 }, /* (154) as ::= */
  {  268,   -1 }, /* (155) distinct ::= DISTINCT */
  {  268,    0 }, /* (156) distinct ::= */
  {  256,   -2 }, /* (157) from ::= FROM tablelist */
  {  271,   -2 }, /* (158) tablelist ::= ids cpxName */
  {  271,   -3 }, /* (159) tablelist ::= ids cpxName ids */
  {  271,   -4 }, /* (160) tablelist ::= tablelist COMMA ids cpxName */
  {  271,   -5 }, /* (161) tablelist ::= tablelist COMMA ids cpxName ids */
  {  272,   -1 }, /* (162) tmvar ::= VARIABLE */
  {  258,   -4 }, /* (163) interval_opt ::= INTERVAL LP tmvar RP */
  {  258,   -6 }, /* (164) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  258,    0 }, /* (165) interval_opt ::= */
  {  259,    0 }, /* (166) fill_opt ::= */
  {  259,   -6 }, /* (167) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  259,   -4 }, /* (168) fill_opt ::= FILL LP ID RP */
  {  260,   -4 }, /* (169) sliding_opt ::= SLIDING LP tmvar RP */
  {  260,    0 }, /* (170) sliding_opt ::= */
  {  262,    0 }, /* (171) orderby_opt ::= */
  {  262,   -3 }, /* (172) orderby_opt ::= ORDER BY sortlist */
  {  273,   -4 }, /* (173) sortlist ::= sortlist COMMA item sortorder */
  {  273,   -2 }, /* (174) sortlist ::= item sortorder */
  {  275,   -2 }, /* (175) item ::= ids cpxName */
  {  276,   -1 }, /* (176) sortorder ::= ASC */
  {  276,   -1 }, /* (177) sortorder ::= DESC */
  {  276,    0 }, /* (178) sortorder ::= */
  {  261,    0 }, /* (179) groupby_opt ::= */
  {  261,   -3 }, /* (180) groupby_opt ::= GROUP BY grouplist */
  {  277,   -3 }, /* (181) grouplist ::= grouplist COMMA item */
  {  277,   -1 }, /* (182) grouplist ::= item */
  {  263,    0 }, /* (183) having_opt ::= */
  {  263,   -2 }, /* (184) having_opt ::= HAVING expr */
  {  265,    0 }, /* (185) limit_opt ::= */
  {  265,   -2 }, /* (186) limit_opt ::= LIMIT signed */
  {  265,   -4 }, /* (187) limit_opt ::= LIMIT signed OFFSET signed */
  {  265,   -4 }, /* (188) limit_opt ::= LIMIT signed COMMA signed */
  {  264,    0 }, /* (189) slimit_opt ::= */
  {  264,   -2 }, /* (190) slimit_opt ::= SLIMIT signed */
  {  264,   -4 }, /* (191) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  264,   -4 }, /* (192) slimit_opt ::= SLIMIT signed COMMA signed */
  {  257,    0 }, /* (193) where_opt ::= */
  {  257,   -2 }, /* (194) where_opt ::= WHERE expr */
  {  269,   -3 }, /* (195) expr ::= LP expr RP */
  {  269,   -1 }, /* (196) expr ::= ID */
  {  269,   -3 }, /* (197) expr ::= ID DOT ID */
  {  269,   -3 }, /* (198) expr ::= ID DOT STAR */
  {  269,   -1 }, /* (199) expr ::= INTEGER */
  {  269,   -2 }, /* (200) expr ::= MINUS INTEGER */
  {  269,   -2 }, /* (201) expr ::= PLUS INTEGER */
  {  269,   -1 }, /* (202) expr ::= FLOAT */
  {  269,   -2 }, /* (203) expr ::= MINUS FLOAT */
  {  269,   -2 }, /* (204) expr ::= PLUS FLOAT */
  {  269,   -1 }, /* (205) expr ::= STRING */
  {  269,   -1 }, /* (206) expr ::= NOW */
  {  269,   -1 }, /* (207) expr ::= VARIABLE */
  {  269,   -1 }, /* (208) expr ::= BOOL */
  {  269,   -4 }, /* (209) expr ::= ID LP exprlist RP */
  {  269,   -4 }, /* (210) expr ::= ID LP STAR RP */
  {  269,   -3 }, /* (211) expr ::= expr IS NULL */
  {  269,   -4 }, /* (212) expr ::= expr IS NOT NULL */
  {  269,   -3 }, /* (213) expr ::= expr LT expr */
  {  269,   -3 }, /* (214) expr ::= expr GT expr */
  {  269,   -3 }, /* (215) expr ::= expr LE expr */
  {  269,   -3 }, /* (216) expr ::= expr GE expr */
  {  269,   -3 }, /* (217) expr ::= expr NE expr */
  {  269,   -3 }, /* (218) expr ::= expr EQ expr */
  {  269,   -3 }, /* (219) expr ::= expr AND expr */
  {  269,   -3 }, /* (220) expr ::= expr OR expr */
  {  269,   -3 }, /* (221) expr ::= expr PLUS expr */
  {  269,   -3 }, /* (222) expr ::= expr MINUS expr */
  {  269,   -3 }, /* (223) expr ::= expr STAR expr */
  {  269,   -3 }, /* (224) expr ::= expr SLASH expr */
  {  269,   -3 }, /* (225) expr ::= expr REM expr */
  {  269,   -3 }, /* (226) expr ::= expr LIKE expr */
  {  269,   -5 }, /* (227) expr ::= expr IN LP exprlist RP */
  {  278,   -3 }, /* (228) exprlist ::= exprlist COMMA expritem */
  {  278,   -1 }, /* (229) exprlist ::= expritem */
  {  279,   -1 }, /* (230) expritem ::= expr */
  {  279,    0 }, /* (231) expritem ::= */
  {  212,   -3 }, /* (232) cmd ::= RESET QUERY CACHE */
  {  212,   -7 }, /* (233) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  212,   -7 }, /* (234) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  212,   -7 }, /* (235) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  212,   -7 }, /* (236) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  212,   -8 }, /* (237) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  212,   -9 }, /* (238) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  212,   -3 }, /* (239) cmd ::= KILL CONNECTION INTEGER */
  {  212,   -5 }, /* (240) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  212,   -5 }, /* (241) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 119: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==119);
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
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 29: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 30: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 31: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 32: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 33: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 34: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 35: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 38: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 39: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy79);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy79);}
        break;
      case 43: /* ids ::= ID */
      case 44: /* ids ::= STRING */ yytestcase(yyruleno==44);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 45: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 46: /* ifexists ::= */
      case 48: /* ifnotexists ::= */ yytestcase(yyruleno==48);
      case 156: /* distinct ::= */ yytestcase(yyruleno==156);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 47: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 49: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 50: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy79);}
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
        break;
      case 52: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 53: /* pps ::= */
      case 55: /* tseries ::= */ yytestcase(yyruleno==55);
      case 57: /* dbs ::= */ yytestcase(yyruleno==57);
      case 59: /* streams ::= */ yytestcase(yyruleno==59);
      case 61: /* storage ::= */ yytestcase(yyruleno==61);
      case 63: /* qtime ::= */ yytestcase(yyruleno==63);
      case 65: /* users ::= */ yytestcase(yyruleno==65);
      case 67: /* conns ::= */ yytestcase(yyruleno==67);
      case 69: /* state ::= */ yytestcase(yyruleno==69);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 54: /* pps ::= PPS INTEGER */
      case 56: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==56);
      case 58: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==58);
      case 60: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==60);
      case 62: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==62);
      case 64: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==64);
      case 66: /* users ::= USERS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* state ::= STATE ids */ yytestcase(yyruleno==70);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 71: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy79.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy79.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy79.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy79.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy79.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy79.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy79.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy79.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy79.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy79 = yylhsminor.yy79;
        break;
      case 72: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy221 = yymsp[0].minor.yy221; }
        break;
      case 73: /* cache ::= CACHE INTEGER */
      case 74: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==74);
      case 75: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==75);
      case 76: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==79);
      case 80: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==80);
      case 81: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==81);
      case 82: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==82);
      case 83: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==83);
      case 84: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==84);
      case 85: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==85);
      case 86: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==86);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 87: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy478);}
        break;
      case 88: /* db_optr ::= db_optr cache */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 89: /* db_optr ::= db_optr replica */
      case 104: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==104);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 90: /* db_optr ::= db_optr quorum */
      case 105: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==105);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 91: /* db_optr ::= db_optr days */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 92: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 93: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 94: /* db_optr ::= db_optr blocks */
      case 107: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==107);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 95: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 96: /* db_optr ::= db_optr wal */
      case 109: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==109);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 97: /* db_optr ::= db_optr fsync */
      case 110: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==110);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 98: /* db_optr ::= db_optr comp */
      case 108: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==108);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 99: /* db_optr ::= db_optr prec */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 100: /* db_optr ::= db_optr keep */
      case 106: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==106);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.keep = yymsp[0].minor.yy221; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 101: /* db_optr ::= db_optr update */
      case 111: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==111);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 102: /* db_optr ::= db_optr cachelast */
      case 112: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==112);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 103: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy478);}
        break;
      case 113: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy503, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy503 = yylhsminor.yy503;
        break;
      case 114: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy109 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy503, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy109;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy503, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy503 = yylhsminor.yy503;
        break;
      case 115: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy503, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy503 = yylhsminor.yy503;
        break;
      case 116: /* signed ::= INTEGER */
{ yylhsminor.yy109 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy109 = yylhsminor.yy109;
        break;
      case 117: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy109 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 118: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy109 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 120: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy358;}
        break;
      case 121: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy416);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy358 = pCreateTable;
}
  yymsp[0].minor.yy358 = yylhsminor.yy358;
        break;
      case 122: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy358->childTableInfo, &yymsp[0].minor.yy416);
  yylhsminor.yy358 = yymsp[-1].minor.yy358;
}
  yymsp[-1].minor.yy358 = yylhsminor.yy358;
        break;
      case 123: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy358 = tSetCreateSqlElems(yymsp[-1].minor.yy221, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy358 = yylhsminor.yy358;
        break;
      case 124: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy358 = tSetCreateSqlElems(yymsp[-5].minor.yy221, yymsp[-1].minor.yy221, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy358 = yylhsminor.yy358;
        break;
      case 125: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy416 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy221, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy416 = yylhsminor.yy416;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy358 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy344, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy358 = yylhsminor.yy358;
        break;
      case 127: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy221, &yymsp[0].minor.yy503); yylhsminor.yy221 = yymsp[-2].minor.yy221;  }
  yymsp[-2].minor.yy221 = yylhsminor.yy221;
        break;
      case 128: /* columnlist ::= column */
{yylhsminor.yy221 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy221, &yymsp[0].minor.yy503);}
  yymsp[0].minor.yy221 = yylhsminor.yy221;
        break;
      case 129: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy503, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy503);
}
  yymsp[-1].minor.yy503 = yylhsminor.yy503;
        break;
      case 130: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy221 = tVariantListAppend(yymsp[-2].minor.yy221, &yymsp[0].minor.yy106, -1);    }
  yymsp[-2].minor.yy221 = yylhsminor.yy221;
        break;
      case 131: /* tagitemlist ::= tagitem */
{ yylhsminor.yy221 = tVariantListAppend(NULL, &yymsp[0].minor.yy106, -1); }
  yymsp[0].minor.yy221 = yylhsminor.yy221;
        break;
      case 132: /* tagitem ::= INTEGER */
      case 133: /* tagitem ::= FLOAT */ yytestcase(yyruleno==133);
      case 134: /* tagitem ::= STRING */ yytestcase(yyruleno==134);
      case 135: /* tagitem ::= BOOL */ yytestcase(yyruleno==135);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy106, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy106 = yylhsminor.yy106;
        break;
      case 136: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy106, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy106 = yylhsminor.yy106;
        break;
      case 137: /* tagitem ::= MINUS INTEGER */
      case 138: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==138);
      case 139: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==139);
      case 140: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==140);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy106, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy106 = yylhsminor.yy106;
        break;
      case 141: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy344 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy178, yymsp[-9].minor.yy221, yymsp[-8].minor.yy50, yymsp[-4].minor.yy221, yymsp[-3].minor.yy221, &yymsp[-7].minor.yy280, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy221, &yymsp[0].minor.yy454, &yymsp[-1].minor.yy454);
}
  yymsp[-11].minor.yy344 = yylhsminor.yy344;
        break;
      case 142: /* union ::= select */
{ yylhsminor.yy273 = setSubclause(NULL, yymsp[0].minor.yy344); }
  yymsp[0].minor.yy273 = yylhsminor.yy273;
        break;
      case 143: /* union ::= LP union RP */
{ yymsp[-2].minor.yy273 = yymsp[-1].minor.yy273; }
        break;
      case 144: /* union ::= union UNION ALL select */
{ yylhsminor.yy273 = appendSelectClause(yymsp[-3].minor.yy273, yymsp[0].minor.yy344); }
  yymsp[-3].minor.yy273 = yylhsminor.yy273;
        break;
      case 145: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy273 = appendSelectClause(yymsp[-5].minor.yy273, yymsp[-1].minor.yy344); }
  yymsp[-5].minor.yy273 = yylhsminor.yy273;
        break;
      case 146: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy273, NULL, TSDB_SQL_SELECT); }
        break;
      case 147: /* select ::= SELECT selcollist */
{
  yylhsminor.yy344 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy178, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 148: /* sclp ::= selcollist COMMA */
{yylhsminor.yy178 = yymsp[-1].minor.yy178;}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 149: /* sclp ::= */
{yymsp[1].minor.yy178 = 0;}
        break;
      case 150: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy178 = tSqlExprListAppend(yymsp[-3].minor.yy178, yymsp[-1].minor.yy50,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 151: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy178 = tSqlExprListAppend(yymsp[-1].minor.yy178, pNode, 0, 0);
}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 152: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 153: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 154: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 155: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 157: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy221 = yymsp[0].minor.yy221;}
        break;
      case 158: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy221 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy221 = tVariantListAppendToken(yylhsminor.yy221, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy221 = yylhsminor.yy221;
        break;
      case 159: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy221 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy221 = tVariantListAppendToken(yylhsminor.yy221, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy221 = yylhsminor.yy221;
        break;
      case 160: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy221 = tVariantListAppendToken(yymsp[-3].minor.yy221, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy221 = tVariantListAppendToken(yylhsminor.yy221, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy221 = yylhsminor.yy221;
        break;
      case 161: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy221 = tVariantListAppendToken(yymsp[-4].minor.yy221, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy221 = tVariantListAppendToken(yylhsminor.yy221, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy221 = yylhsminor.yy221;
        break;
      case 162: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 163: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy280.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy280.offset.n = 0; yymsp[-3].minor.yy280.offset.z = NULL; yymsp[-3].minor.yy280.offset.type = 0;}
        break;
      case 164: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy280.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy280.offset = yymsp[-1].minor.yy0;}
        break;
      case 165: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy280, 0, sizeof(yymsp[1].minor.yy280));}
        break;
      case 166: /* fill_opt ::= */
{yymsp[1].minor.yy221 = 0;     }
        break;
      case 167: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy221, &A, -1, 0);
    yymsp[-5].minor.yy221 = yymsp[-1].minor.yy221;
}
        break;
      case 168: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy221 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 169: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 170: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 171: /* orderby_opt ::= */
{yymsp[1].minor.yy221 = 0;}
        break;
      case 172: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy221 = yymsp[0].minor.yy221;}
        break;
      case 173: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy221 = tVariantListAppend(yymsp[-3].minor.yy221, &yymsp[-1].minor.yy106, yymsp[0].minor.yy172);
}
  yymsp[-3].minor.yy221 = yylhsminor.yy221;
        break;
      case 174: /* sortlist ::= item sortorder */
{
  yylhsminor.yy221 = tVariantListAppend(NULL, &yymsp[-1].minor.yy106, yymsp[0].minor.yy172);
}
  yymsp[-1].minor.yy221 = yylhsminor.yy221;
        break;
      case 175: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy106, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy106 = yylhsminor.yy106;
        break;
      case 176: /* sortorder ::= ASC */
{ yymsp[0].minor.yy172 = TSDB_ORDER_ASC; }
        break;
      case 177: /* sortorder ::= DESC */
{ yymsp[0].minor.yy172 = TSDB_ORDER_DESC;}
        break;
      case 178: /* sortorder ::= */
{ yymsp[1].minor.yy172 = TSDB_ORDER_ASC; }
        break;
      case 179: /* groupby_opt ::= */
{ yymsp[1].minor.yy221 = 0;}
        break;
      case 180: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy221 = yymsp[0].minor.yy221;}
        break;
      case 181: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy221 = tVariantListAppend(yymsp[-2].minor.yy221, &yymsp[0].minor.yy106, -1);
}
  yymsp[-2].minor.yy221 = yylhsminor.yy221;
        break;
      case 182: /* grouplist ::= item */
{
  yylhsminor.yy221 = tVariantListAppend(NULL, &yymsp[0].minor.yy106, -1);
}
  yymsp[0].minor.yy221 = yylhsminor.yy221;
        break;
      case 183: /* having_opt ::= */
      case 193: /* where_opt ::= */ yytestcase(yyruleno==193);
      case 231: /* expritem ::= */ yytestcase(yyruleno==231);
{yymsp[1].minor.yy50 = 0;}
        break;
      case 184: /* having_opt ::= HAVING expr */
      case 194: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==194);
{yymsp[-1].minor.yy50 = yymsp[0].minor.yy50;}
        break;
      case 185: /* limit_opt ::= */
      case 189: /* slimit_opt ::= */ yytestcase(yyruleno==189);
{yymsp[1].minor.yy454.limit = -1; yymsp[1].minor.yy454.offset = 0;}
        break;
      case 186: /* limit_opt ::= LIMIT signed */
      case 190: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==190);
{yymsp[-1].minor.yy454.limit = yymsp[0].minor.yy109;  yymsp[-1].minor.yy454.offset = 0;}
        break;
      case 187: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy454.limit = yymsp[-2].minor.yy109;  yymsp[-3].minor.yy454.offset = yymsp[0].minor.yy109;}
        break;
      case 188: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy454.limit = yymsp[0].minor.yy109;  yymsp[-3].minor.yy454.offset = yymsp[-2].minor.yy109;}
        break;
      case 191: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy454.limit = yymsp[-2].minor.yy109;  yymsp[-3].minor.yy454.offset = yymsp[0].minor.yy109;}
        break;
      case 192: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy454.limit = yymsp[0].minor.yy109;  yymsp[-3].minor.yy454.offset = yymsp[-2].minor.yy109;}
        break;
      case 195: /* expr ::= LP expr RP */
{yylhsminor.yy50 = yymsp[-1].minor.yy50; yylhsminor.yy50->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy50->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 196: /* expr ::= ID */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 197: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 198: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 199: /* expr ::= INTEGER */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 200: /* expr ::= MINUS INTEGER */
      case 201: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==201);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy50 = yylhsminor.yy50;
        break;
      case 202: /* expr ::= FLOAT */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 203: /* expr ::= MINUS FLOAT */
      case 204: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==204);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy50 = yylhsminor.yy50;
        break;
      case 205: /* expr ::= STRING */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 206: /* expr ::= NOW */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 207: /* expr ::= VARIABLE */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 208: /* expr ::= BOOL */
{ yylhsminor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 209: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy50 = tSqlExprCreateFunction(yymsp[-1].minor.yy178, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy50 = yylhsminor.yy50;
        break;
      case 210: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy50 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy50 = yylhsminor.yy50;
        break;
      case 211: /* expr ::= expr IS NULL */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 212: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-3].minor.yy50, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy50 = yylhsminor.yy50;
        break;
      case 213: /* expr ::= expr LT expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_LT);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 214: /* expr ::= expr GT expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_GT);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 215: /* expr ::= expr LE expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_LE);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 216: /* expr ::= expr GE expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_GE);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 217: /* expr ::= expr NE expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_NE);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 218: /* expr ::= expr EQ expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_EQ);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 219: /* expr ::= expr AND expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_AND);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 220: /* expr ::= expr OR expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_OR); }
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 221: /* expr ::= expr PLUS expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_PLUS);  }
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 222: /* expr ::= expr MINUS expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_MINUS); }
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 223: /* expr ::= expr STAR expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_STAR);  }
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 224: /* expr ::= expr SLASH expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_DIVIDE);}
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 225: /* expr ::= expr REM expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_REM);   }
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 226: /* expr ::= expr LIKE expr */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_LIKE);  }
  yymsp[-2].minor.yy50 = yylhsminor.yy50;
        break;
      case 227: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy50 = tSqlExprCreate(yymsp[-4].minor.yy50, (tSQLExpr*)yymsp[-1].minor.yy178, TK_IN); }
  yymsp[-4].minor.yy50 = yylhsminor.yy50;
        break;
      case 228: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy178 = tSqlExprListAppend(yymsp[-2].minor.yy178,yymsp[0].minor.yy50,0, 0);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 229: /* exprlist ::= expritem */
{yylhsminor.yy178 = tSqlExprListAppend(0,yymsp[0].minor.yy50,0, 0);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 230: /* expritem ::= expr */
{yylhsminor.yy50 = yymsp[0].minor.yy50;}
  yymsp[0].minor.yy50 = yylhsminor.yy50;
        break;
      case 232: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 233: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy221, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 234: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 235: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy221, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 237: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 238: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy106, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 239: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 240: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 241: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
