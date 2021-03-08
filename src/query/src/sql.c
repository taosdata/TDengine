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
#define YYNOCODE 257
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy32;
  tSQLExpr* yy114;
  TAOS_FIELD yy215;
  SQuerySQL* yy216;
  int64_t yy221;
  SCreateDbInfo yy222;
  tSQLExprList* yy258;
  SIntervalVal yy264;
  SCreateTableSQL* yy278;
  SCreateAcctInfo yy299;
  int yy348;
  SArray* yy349;
  SSubclauseInfo* yy417;
  tVariant yy426;
  SLimitVal yy454;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             294
#define YYNRULE              254
#define YYNTOKEN             184
#define YY_MAX_SHIFT         293
#define YY_MIN_SHIFTREDUCE   477
#define YY_MAX_SHIFTREDUCE   730
#define YY_ERROR_ACTION      731
#define YY_ACCEPT_ACTION     732
#define YY_NO_ACTION         733
#define YY_MIN_REDUCE        734
#define YY_MAX_REDUCE        987
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
#define YY_ACTTAB_COUNT (651)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    74,  521,  732,  293,  521,  165,  186,  291,   28,  522,
 /*    10 */   190,  893,  522,   43,   44,  969,   47,   48,   15,  776,
 /*    20 */   198,   37,  152,   46,  242,   51,   49,   53,   50,  854,
 /*    30 */   855,   27,  858,   42,   41,  871,  128,   40,   39,   38,
 /*    40 */    43,   44,  882,   47,   48,  882,  188,  198,   37,  868,
 /*    50 */    46,  242,   51,   49,   53,   50,  187,  128,  203,  225,
 /*    60 */    42,   41,  979,  165,   40,   39,   38,   43,   44,  890,
 /*    70 */    47,   48,  193,  970,  198,   37,  165,   46,  242,   51,
 /*    80 */    49,   53,   50,  871,  128,  192,  970,   42,   41,  258,
 /*    90 */   521,   40,   39,   38,  290,  289,  115,  239,  522,   71,
 /*   100 */    77,   43,   45,  128,   47,   48,  205,   66,  198,   37,
 /*   110 */    28,   46,  242,   51,   49,   53,   50,   40,   39,   38,
 /*   120 */   921,   42,   41,  278,   65,   40,   39,   38,  865,  678,
 /*   130 */   287,  871,  859,  210,  478,  479,  480,  481,  482,  483,
 /*   140 */   484,  485,  486,  487,  488,  489,  292,   72,  201,  215,
 /*   150 */    44,  868,   47,   48,  856,  871,  198,   37,  209,   46,
 /*   160 */   242,   51,   49,   53,   50,  869,  922,  204,  237,   42,
 /*   170 */    41,   96,  163,   40,   39,   38,  278,   21,  256,  286,
 /*   180 */   285,  255,  254,  253,  284,  252,  283,  282,  281,  251,
 /*   190 */   280,  279,  835,  594,  823,  824,  825,  826,  827,  828,
 /*   200 */   829,  830,  831,  832,  833,  834,  836,  837,   47,   48,
 /*   210 */    87,   86,  198,   37,   28,   46,  242,   51,   49,   53,
 /*   220 */    50,  268,  267,   16,  211,   42,   41,  265,  264,   40,
 /*   230 */    39,   38,  197,  691,   28,  634,  682,  123,  685,  174,
 /*   240 */   688,   22,   42,   41,   73,  175,   40,   39,   38,   34,
 /*   250 */   108,  107,  173,  197,  691,  867,   67,  682,  121,  685,
 /*   260 */    21,  688,  286,  285,  194,  195,   34,  284,  241,  283,
 /*   270 */   282,  281,  202,  280,  279,  868,  659,  660,  680,  841,
 /*   280 */    22,  857,  839,  840,  169,  194,  195,  842,   34,  844,
 /*   290 */   845,  843,  785,  846,  847,  152,  230,  631,  218,   51,
 /*   300 */    49,   53,   50,   28,   23,  222,  221,   42,   41,  224,
 /*   310 */   638,   40,   39,   38,  681,  618,  181,   10,  615,  207,
 /*   320 */   616,   76,  617,  138,  243,  932,  777,   94,   98,  152,
 /*   330 */   966,  196,   52,   88,  103,  106,   97,   28,   28,   28,
 /*   340 */   965,  261,  100,  626,  868,  690,  212,  213,    3,  142,
 /*   350 */   684,  227,  687,   52,   31,   83,   79,   82,  258,  228,
 /*   360 */   689,  158,  154,   29,   60,  964,  690,  156,  111,  110,
 /*   370 */   109,  182,  683,    4,  686,  262,  266,  270,  868,  868,
 /*   380 */   868,  689,  646,   61,   57,  208,  125,  622,  260,  623,
 /*   390 */   650,  651,  711,  692,   56,   18,   17,   17,  604,  245,
 /*   400 */   606,  870,   29,   29,   56,   58,  247,  605,  183,   26,
 /*   410 */    75,   56,  248,  105,  104,   12,   11,  694,  167,   93,
 /*   420 */    92,  619,   63,  168,  593,   14,   13,  620,  170,  621,
 /*   430 */   120,  118,  164,  931,  171,  172,  199,  122,  178,  179,
 /*   440 */   177,  162,  176,  928,  166,  927,  200,  269,  884,  892,
 /*   450 */    35,  899,  901,  124,  914,  913,  139,  864,  140,  137,
 /*   460 */    34,  141,  226,  787,  250,  119,  645,  160,  240,  231,
 /*   470 */    62,   32,  259,  784,  881,   59,  189,  235,  129,   54,
 /*   480 */   131,  984,   84,  983,  130,  238,  133,  981,  132,  143,
 /*   490 */   263,  978,  236,  234,   90,  977,  232,  134,  975,  144,
 /*   500 */   805,  135,   33,   30,  161,  774,   99,  772,  101,   95,
 /*   510 */   102,  770,  769,  214,  153,  767,  766,  765,  764,  763,
 /*   520 */   155,  157,  760,  758,  756,  754,  752,   36,  159,  271,
 /*   530 */   229,   68,   69,  915,  272,  273,  274,  275,  276,  277,
 /*   540 */   288,  730,  184,  206,  249,  216,  217,  185,  180,  729,
 /*   550 */    80,  219,  220,  728,  716,  768,  223,  227,   70,  628,
 /*   560 */    64,  147,  146,  806,  145,  148,  149,  151,  112,  150,
 /*   570 */   113,  647,  762,  761,  244,  114,  866,  753,    6,  126,
 /*   580 */   136,    1,    2,  191,  233,   24,   25,    7,  652,  127,
 /*   590 */     8,  693,    5,    9,   19,  695,   20,   78,  246,  562,
 /*   600 */   558,   76,  556,  555,  554,  551,  525,  257,   81,   85,
 /*   610 */    29,  596,   55,  595,  592,   89,   91,  546,  544,  536,
 /*   620 */   542,  538,  540,  534,  532,  564,  563,  561,  560,  559,
 /*   630 */   557,  553,  552,   56,  523,  493,  491,  734,  733,  733,
 /*   640 */   733,  733,  733,  733,  733,  733,  733,  733,  733,  116,
 /*   650 */   117,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   193,    1,  185,  186,    1,  245,  187,  188,  188,    9,
 /*    10 */   205,  188,    9,   13,   14,  255,   16,   17,  245,  192,
 /*    20 */    20,   21,  195,   23,   24,   25,   26,   27,   28,  222,
 /*    30 */   223,  224,  225,   33,   34,  230,  188,   37,   38,   39,
 /*    40 */    13,   14,  228,   16,   17,  228,  226,   20,   21,  229,
 /*    50 */    23,   24,   25,   26,   27,   28,  242,  188,  205,  242,
 /*    60 */    33,   34,  230,  245,   37,   38,   39,   13,   14,  246,
 /*    70 */    16,   17,  254,  255,   20,   21,  245,   23,   24,   25,
 /*    80 */    26,   27,   28,  230,  188,  254,  255,   33,   34,   77,
 /*    90 */     1,   37,   38,   39,   63,   64,   65,  249,    9,  251,
 /*   100 */   193,   13,   14,  188,   16,   17,  205,  107,   20,   21,
 /*   110 */   188,   23,   24,   25,   26,   27,   28,   37,   38,   39,
 /*   120 */   251,   33,   34,   79,  193,   37,   38,   39,  188,  102,
 /*   130 */   205,  230,  225,  188,   45,   46,   47,   48,   49,   50,
 /*   140 */    51,   52,   53,   54,   55,   56,   57,  251,  226,   60,
 /*   150 */    14,  229,   16,   17,  223,  230,   20,   21,   66,   23,
 /*   160 */    24,   25,   26,   27,   28,  220,  251,  227,  253,   33,
 /*   170 */    34,   74,  245,   37,   38,   39,   79,   86,   87,   88,
 /*   180 */    89,   90,   91,   92,   93,   94,   95,   96,   97,   98,
 /*   190 */    99,  100,  204,    5,  206,  207,  208,  209,  210,  211,
 /*   200 */   212,  213,  214,  215,  216,  217,  218,  219,   16,   17,
 /*   210 */   133,  134,   20,   21,  188,   23,   24,   25,   26,   27,
 /*   220 */    28,   33,   34,   44,  132,   33,   34,  135,  136,   37,
 /*   230 */    38,   39,    1,    2,  188,   37,    5,  188,    7,   60,
 /*   240 */     9,  101,   33,   34,  231,   66,   37,   38,   39,  109,
 /*   250 */    71,   72,   73,    1,    2,  229,  243,    5,  101,    7,
 /*   260 */    86,    9,   88,   89,   33,   34,  109,   93,   37,   95,
 /*   270 */    96,   97,  226,   99,  100,  229,  120,  121,    1,  204,
 /*   280 */   101,    0,  207,  208,  245,   33,   34,  212,  109,  214,
 /*   290 */   215,  216,  192,  218,  219,  195,  247,  106,  131,   25,
 /*   300 */    26,   27,   28,  188,  113,  138,  139,   33,   34,  130,
 /*   310 */   112,   37,   38,   39,   37,    2,  137,  101,    5,   66,
 /*   320 */     7,  105,    9,  107,   15,  221,  192,   61,   62,  195,
 /*   330 */   245,   59,  101,   67,   68,   69,   70,  188,  188,  188,
 /*   340 */   245,  226,   76,  102,  229,  114,   33,   34,   61,   62,
 /*   350 */     5,  110,    7,  101,   67,   68,   69,   70,   77,  102,
 /*   360 */   129,   61,   62,  106,  106,  245,  114,   67,   68,   69,
 /*   370 */    70,  245,    5,  101,    7,  226,  226,  226,  229,  229,
 /*   380 */   229,  129,  102,  125,  106,  132,  106,    5,  135,    7,
 /*   390 */   102,  102,  102,  102,  106,  106,  106,  106,  102,  102,
 /*   400 */   102,  230,  106,  106,  106,  127,  102,  102,  245,  101,
 /*   410 */   106,  106,  104,   74,   75,  133,  134,  108,  245,  133,
 /*   420 */   134,  108,  101,  245,  103,  133,  134,    5,  245,    7,
 /*   430 */    61,   62,  245,  221,  245,  245,  221,  188,  245,  245,
 /*   440 */   245,  245,  245,  221,  245,  221,  221,  221,  228,  188,
 /*   450 */   244,  188,  188,  188,  252,  252,  188,  188,  188,  232,
 /*   460 */   109,  188,  228,  188,  188,   59,  114,  188,  118,  248,
 /*   470 */   124,  188,  188,  188,  241,  126,  248,  248,  240,  123,
 /*   480 */   238,  188,  188,  188,  239,  122,  236,  188,  237,  188,
 /*   490 */   188,  188,  117,  116,  188,  188,  115,  235,  188,  188,
 /*   500 */   188,  234,  188,  188,  188,  188,  188,  188,  188,   85,
 /*   510 */   188,  188,  188,  188,  188,  188,  188,  188,  188,  188,
 /*   520 */   188,  188,  188,  188,  188,  188,  188,  128,  188,   84,
 /*   530 */   189,  189,  189,  189,   49,   81,   83,   53,   82,   80,
 /*   540 */    77,    5,  189,  189,  189,  140,    5,  189,  189,    5,
 /*   550 */   193,  140,    5,    5,   87,  189,  131,  110,  106,  102,
 /*   560 */   111,  197,  201,  203,  202,  200,  198,  196,  190,  199,
 /*   570 */   190,  102,  189,  189,  104,  190,  228,  189,  101,  101,
 /*   580 */   233,  194,  191,    1,  101,  106,  106,  119,  102,  101,
 /*   590 */   119,  102,  101,  101,  101,  108,  101,   74,  104,    9,
 /*   600 */     5,  105,    5,    5,    5,    5,   78,   15,   74,  134,
 /*   610 */   106,    5,   16,    5,  102,  134,  134,    5,    5,    5,
 /*   620 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   630 */     5,    5,    5,  106,   78,   59,   58,    0,  256,  256,
 /*   640 */   256,  256,  256,  256,  256,  256,  256,  256,  256,   21,
 /*   650 */    21,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   660 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   670 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   680 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   690 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   700 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   710 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   720 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   730 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   740 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   750 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   760 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   770 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   780 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   790 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   800 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   810 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   820 */   256,  256,  256,  256,  256,  256,  256,  256,  256,  256,
 /*   830 */   256,  256,  256,  256,  256,
};
#define YY_SHIFT_COUNT    (293)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (637)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   179,   91,  174,   12,  231,  252,    3,    3,    3,    3,
 /*    10 */     3,    3,    3,    3,    3,    0,   89,  252,  313,  313,
 /*    20 */   313,  313,  140,    3,    3,    3,    3,  281,    3,    3,
 /*    30 */    97,   12,   44,   44,  651,  252,  252,  252,  252,  252,
 /*    40 */   252,  252,  252,  252,  252,  252,  252,  252,  252,  252,
 /*    50 */   252,  252,  252,  252,  252,  313,  313,  188,  188,  188,
 /*    60 */   188,  188,  188,  188,  157,    3,    3,  198,    3,    3,
 /*    70 */     3,  156,  156,  191,    3,    3,    3,    3,    3,    3,
 /*    80 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    90 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   100 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   110 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   120 */     3,  351,  406,  406,  406,  352,  352,  352,  406,  346,
 /*   130 */   349,  356,  350,  363,  375,  377,  381,  399,  351,  406,
 /*   140 */   406,  406,   12,  406,  406,  424,  445,  485,  454,  453,
 /*   150 */   484,  456,  459,  406,  463,  406,  463,  406,  463,  406,
 /*   160 */   651,  651,   27,   54,   88,   54,   54,  136,  192,  274,
 /*   170 */   274,  274,  274,  266,  287,  300,  209,  209,  209,  209,
 /*   180 */    92,  167,   80,   80,  216,  253,   31,  241,  257,  280,
 /*   190 */   288,  289,  290,  291,  345,  367,  277,  272,  309,  278,
 /*   200 */   258,  296,  297,  298,  304,  305,  308,   77,  282,  286,
 /*   210 */   321,  292,  382,  422,  339,  369,  536,  405,  541,  544,
 /*   220 */   411,  547,  548,  467,  425,  447,  457,  449,  470,  477,
 /*   230 */   452,  469,  478,  582,  483,  486,  488,  479,  468,  480,
 /*   240 */   471,  489,  491,  487,  492,  470,  493,  494,  495,  496,
 /*   250 */   523,  590,  595,  597,  598,  599,  600,  528,  592,  534,
 /*   260 */   475,  504,  504,  596,  481,  482,  504,  606,  608,  512,
 /*   270 */   504,  612,  613,  614,  615,  616,  617,  618,  619,  620,
 /*   280 */   621,  622,  623,  624,  625,  626,  627,  527,  556,  628,
 /*   290 */   629,  576,  578,  637,
};
#define YY_REDUCE_COUNT (161)
#define YY_REDUCE_MIN   (-240)
#define YY_REDUCE_MAX   (391)
static const short yy_reduce_ofst[] = {
 /*     0 */  -183,  -12,   75, -193, -182, -169, -180,  -85, -152,  -78,
 /*    10 */    46,  115,  149,  150,  151, -177, -181, -240, -195, -147,
 /*    20 */   -99,  -75, -186,   49, -131, -104,  -60,  -93,  -55,   26,
 /*    30 */  -173,  -69,  100,  134,   13, -227,  -73,   39,   85,   95,
 /*    40 */   120,  126,  163,  173,  178,  183,  187,  189,  190,  193,
 /*    50 */   194,  195,  196,  197,  199, -168,  171,  104,  212,  215,
 /*    60 */   222,  224,  225,  226,  220,  249,  261,  206,  263,  264,
 /*    70 */   265,  202,  203,  227,  268,  269,  270,  273,  275,  276,
 /*    80 */   279,  283,  284,  285,  293,  294,  295,  299,  301,  302,
 /*    90 */   303,  306,  307,  310,  311,  312,  314,  315,  316,  317,
 /*   100 */   318,  319,  320,  322,  323,  324,  325,  326,  327,  328,
 /*   110 */   329,  330,  331,  332,  333,  334,  335,  336,  337,  338,
 /*   120 */   340,  234,  341,  342,  343,  221,  228,  229,  344,  233,
 /*   130 */   238,  245,  242,  251,  250,  262,  267,  347,  348,  353,
 /*   140 */   354,  355,  357,  358,  359,  360,  362,  361,  364,  365,
 /*   150 */   368,  370,  371,  366,  378,  383,  380,  384,  385,  388,
 /*   160 */   387,  391,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   731,  786,  775,  783,  972,  972,  731,  731,  731,  731,
 /*    10 */   731,  731,  731,  731,  731,  894,  749,  972,  731,  731,
 /*    20 */   731,  731,  731,  731,  731,  731,  731,  783,  731,  731,
 /*    30 */   788,  783,  788,  788,  889,  731,  731,  731,  731,  731,
 /*    40 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*    50 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*    60 */   731,  731,  731,  731,  731,  731,  731,  896,  898,  900,
 /*    70 */   731,  918,  918,  887,  731,  731,  731,  731,  731,  731,
 /*    80 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*    90 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  773,
 /*   100 */   731,  771,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   110 */   731,  731,  731,  731,  731,  759,  731,  731,  731,  731,
 /*   120 */   731,  731,  751,  751,  751,  731,  731,  731,  751,  925,
 /*   130 */   929,  923,  911,  919,  910,  906,  905,  933,  731,  751,
 /*   140 */   751,  751,  783,  751,  751,  804,  802,  800,  792,  798,
 /*   150 */   794,  796,  790,  751,  781,  751,  781,  751,  781,  751,
 /*   160 */   822,  838,  731,  934,  731,  971,  924,  961,  960,  967,
 /*   170 */   959,  958,  957,  731,  731,  731,  953,  954,  956,  955,
 /*   180 */   731,  731,  963,  962,  731,  731,  731,  731,  731,  731,
 /*   190 */   731,  731,  731,  731,  731,  731,  731,  936,  731,  930,
 /*   200 */   926,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   210 */   848,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   220 */   731,  731,  731,  731,  731,  886,  731,  731,  731,  731,
 /*   230 */   897,  731,  731,  731,  731,  731,  731,  920,  731,  912,
 /*   240 */   731,  731,  731,  731,  731,  860,  731,  731,  731,  731,
 /*   250 */   731,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   260 */   731,  982,  980,  731,  731,  731,  976,  731,  731,  731,
 /*   270 */   974,  731,  731,  731,  731,  731,  731,  731,  731,  731,
 /*   280 */   731,  731,  731,  731,  731,  731,  731,  807,  731,  757,
 /*   290 */   755,  731,  747,  731,
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
  /*  106 */ "COMMA",
  /*  107 */ "AS",
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
  /*  227 */ "tagNamelist",
  /*  228 */ "select",
  /*  229 */ "column",
  /*  230 */ "tagitem",
  /*  231 */ "selcollist",
  /*  232 */ "from",
  /*  233 */ "where_opt",
  /*  234 */ "interval_opt",
  /*  235 */ "fill_opt",
  /*  236 */ "sliding_opt",
  /*  237 */ "groupby_opt",
  /*  238 */ "orderby_opt",
  /*  239 */ "having_opt",
  /*  240 */ "slimit_opt",
  /*  241 */ "limit_opt",
  /*  242 */ "union",
  /*  243 */ "sclp",
  /*  244 */ "distinct",
  /*  245 */ "expr",
  /*  246 */ "as",
  /*  247 */ "tablelist",
  /*  248 */ "tmvar",
  /*  249 */ "sortlist",
  /*  250 */ "sortitem",
  /*  251 */ "item",
  /*  252 */ "sortorder",
  /*  253 */ "grouplist",
  /*  254 */ "exprlist",
  /*  255 */ "expritem",
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
 /* 129 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 130 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 131 */ "tagNamelist ::= ids",
 /* 132 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 133 */ "columnlist ::= columnlist COMMA column",
 /* 134 */ "columnlist ::= column",
 /* 135 */ "column ::= ids typename",
 /* 136 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 137 */ "tagitemlist ::= tagitem",
 /* 138 */ "tagitem ::= INTEGER",
 /* 139 */ "tagitem ::= FLOAT",
 /* 140 */ "tagitem ::= STRING",
 /* 141 */ "tagitem ::= BOOL",
 /* 142 */ "tagitem ::= NULL",
 /* 143 */ "tagitem ::= MINUS INTEGER",
 /* 144 */ "tagitem ::= MINUS FLOAT",
 /* 145 */ "tagitem ::= PLUS INTEGER",
 /* 146 */ "tagitem ::= PLUS FLOAT",
 /* 147 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 148 */ "union ::= select",
 /* 149 */ "union ::= LP union RP",
 /* 150 */ "union ::= union UNION ALL select",
 /* 151 */ "union ::= union UNION ALL LP select RP",
 /* 152 */ "cmd ::= union",
 /* 153 */ "select ::= SELECT selcollist",
 /* 154 */ "sclp ::= selcollist COMMA",
 /* 155 */ "sclp ::=",
 /* 156 */ "selcollist ::= sclp distinct expr as",
 /* 157 */ "selcollist ::= sclp STAR",
 /* 158 */ "as ::= AS ids",
 /* 159 */ "as ::= ids",
 /* 160 */ "as ::=",
 /* 161 */ "distinct ::= DISTINCT",
 /* 162 */ "distinct ::=",
 /* 163 */ "from ::= FROM tablelist",
 /* 164 */ "tablelist ::= ids cpxName",
 /* 165 */ "tablelist ::= ids cpxName ids",
 /* 166 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 167 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 168 */ "tmvar ::= VARIABLE",
 /* 169 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 170 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 171 */ "interval_opt ::=",
 /* 172 */ "fill_opt ::=",
 /* 173 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 174 */ "fill_opt ::= FILL LP ID RP",
 /* 175 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 176 */ "sliding_opt ::=",
 /* 177 */ "orderby_opt ::=",
 /* 178 */ "orderby_opt ::= ORDER BY sortlist",
 /* 179 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 180 */ "sortlist ::= item sortorder",
 /* 181 */ "item ::= ids cpxName",
 /* 182 */ "sortorder ::= ASC",
 /* 183 */ "sortorder ::= DESC",
 /* 184 */ "sortorder ::=",
 /* 185 */ "groupby_opt ::=",
 /* 186 */ "groupby_opt ::= GROUP BY grouplist",
 /* 187 */ "grouplist ::= grouplist COMMA item",
 /* 188 */ "grouplist ::= item",
 /* 189 */ "having_opt ::=",
 /* 190 */ "having_opt ::= HAVING expr",
 /* 191 */ "limit_opt ::=",
 /* 192 */ "limit_opt ::= LIMIT signed",
 /* 193 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 194 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 195 */ "slimit_opt ::=",
 /* 196 */ "slimit_opt ::= SLIMIT signed",
 /* 197 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 198 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 199 */ "where_opt ::=",
 /* 200 */ "where_opt ::= WHERE expr",
 /* 201 */ "expr ::= LP expr RP",
 /* 202 */ "expr ::= ID",
 /* 203 */ "expr ::= ID DOT ID",
 /* 204 */ "expr ::= ID DOT STAR",
 /* 205 */ "expr ::= INTEGER",
 /* 206 */ "expr ::= MINUS INTEGER",
 /* 207 */ "expr ::= PLUS INTEGER",
 /* 208 */ "expr ::= FLOAT",
 /* 209 */ "expr ::= MINUS FLOAT",
 /* 210 */ "expr ::= PLUS FLOAT",
 /* 211 */ "expr ::= STRING",
 /* 212 */ "expr ::= NOW",
 /* 213 */ "expr ::= VARIABLE",
 /* 214 */ "expr ::= BOOL",
 /* 215 */ "expr ::= ID LP exprlist RP",
 /* 216 */ "expr ::= ID LP STAR RP",
 /* 217 */ "expr ::= expr IS NULL",
 /* 218 */ "expr ::= expr IS NOT NULL",
 /* 219 */ "expr ::= expr LT expr",
 /* 220 */ "expr ::= expr GT expr",
 /* 221 */ "expr ::= expr LE expr",
 /* 222 */ "expr ::= expr GE expr",
 /* 223 */ "expr ::= expr NE expr",
 /* 224 */ "expr ::= expr EQ expr",
 /* 225 */ "expr ::= expr BETWEEN expr AND expr",
 /* 226 */ "expr ::= expr AND expr",
 /* 227 */ "expr ::= expr OR expr",
 /* 228 */ "expr ::= expr PLUS expr",
 /* 229 */ "expr ::= expr MINUS expr",
 /* 230 */ "expr ::= expr STAR expr",
 /* 231 */ "expr ::= expr SLASH expr",
 /* 232 */ "expr ::= expr REM expr",
 /* 233 */ "expr ::= expr LIKE expr",
 /* 234 */ "expr ::= expr IN LP exprlist RP",
 /* 235 */ "exprlist ::= exprlist COMMA expritem",
 /* 236 */ "exprlist ::= expritem",
 /* 237 */ "expritem ::= expr",
 /* 238 */ "expritem ::=",
 /* 239 */ "cmd ::= RESET QUERY CACHE",
 /* 240 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 241 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 242 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 243 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 244 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 245 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 246 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 247 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 248 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 249 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 250 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 251 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 252 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 253 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 227: /* tagNamelist */
    case 235: /* fill_opt */
    case 237: /* groupby_opt */
    case 238: /* orderby_opt */
    case 249: /* sortlist */
    case 253: /* grouplist */
{
taosArrayDestroy((yypminor->yy349));
}
      break;
    case 224: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy278));
}
      break;
    case 228: /* select */
{
doDestroyQuerySql((yypminor->yy216));
}
      break;
    case 231: /* selcollist */
    case 243: /* sclp */
    case 254: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy258));
}
      break;
    case 233: /* where_opt */
    case 239: /* having_opt */
    case 245: /* expr */
    case 255: /* expritem */
{
tSqlExprDestroy((yypminor->yy114));
}
      break;
    case 242: /* union */
{
destroyAllSelectClause((yypminor->yy417));
}
      break;
    case 250: /* sortitem */
{
tVariantDestroy(&(yypminor->yy426));
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
  {  225,  -13 }, /* (129) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  227,   -3 }, /* (130) tagNamelist ::= tagNamelist COMMA ids */
  {  227,   -1 }, /* (131) tagNamelist ::= ids */
  {  222,   -5 }, /* (132) create_table_args ::= ifnotexists ids cpxName AS select */
  {  226,   -3 }, /* (133) columnlist ::= columnlist COMMA column */
  {  226,   -1 }, /* (134) columnlist ::= column */
  {  229,   -2 }, /* (135) column ::= ids typename */
  {  205,   -3 }, /* (136) tagitemlist ::= tagitemlist COMMA tagitem */
  {  205,   -1 }, /* (137) tagitemlist ::= tagitem */
  {  230,   -1 }, /* (138) tagitem ::= INTEGER */
  {  230,   -1 }, /* (139) tagitem ::= FLOAT */
  {  230,   -1 }, /* (140) tagitem ::= STRING */
  {  230,   -1 }, /* (141) tagitem ::= BOOL */
  {  230,   -1 }, /* (142) tagitem ::= NULL */
  {  230,   -2 }, /* (143) tagitem ::= MINUS INTEGER */
  {  230,   -2 }, /* (144) tagitem ::= MINUS FLOAT */
  {  230,   -2 }, /* (145) tagitem ::= PLUS INTEGER */
  {  230,   -2 }, /* (146) tagitem ::= PLUS FLOAT */
  {  228,  -12 }, /* (147) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  242,   -1 }, /* (148) union ::= select */
  {  242,   -3 }, /* (149) union ::= LP union RP */
  {  242,   -4 }, /* (150) union ::= union UNION ALL select */
  {  242,   -6 }, /* (151) union ::= union UNION ALL LP select RP */
  {  186,   -1 }, /* (152) cmd ::= union */
  {  228,   -2 }, /* (153) select ::= SELECT selcollist */
  {  243,   -2 }, /* (154) sclp ::= selcollist COMMA */
  {  243,    0 }, /* (155) sclp ::= */
  {  231,   -4 }, /* (156) selcollist ::= sclp distinct expr as */
  {  231,   -2 }, /* (157) selcollist ::= sclp STAR */
  {  246,   -2 }, /* (158) as ::= AS ids */
  {  246,   -1 }, /* (159) as ::= ids */
  {  246,    0 }, /* (160) as ::= */
  {  244,   -1 }, /* (161) distinct ::= DISTINCT */
  {  244,    0 }, /* (162) distinct ::= */
  {  232,   -2 }, /* (163) from ::= FROM tablelist */
  {  247,   -2 }, /* (164) tablelist ::= ids cpxName */
  {  247,   -3 }, /* (165) tablelist ::= ids cpxName ids */
  {  247,   -4 }, /* (166) tablelist ::= tablelist COMMA ids cpxName */
  {  247,   -5 }, /* (167) tablelist ::= tablelist COMMA ids cpxName ids */
  {  248,   -1 }, /* (168) tmvar ::= VARIABLE */
  {  234,   -4 }, /* (169) interval_opt ::= INTERVAL LP tmvar RP */
  {  234,   -6 }, /* (170) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  234,    0 }, /* (171) interval_opt ::= */
  {  235,    0 }, /* (172) fill_opt ::= */
  {  235,   -6 }, /* (173) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  235,   -4 }, /* (174) fill_opt ::= FILL LP ID RP */
  {  236,   -4 }, /* (175) sliding_opt ::= SLIDING LP tmvar RP */
  {  236,    0 }, /* (176) sliding_opt ::= */
  {  238,    0 }, /* (177) orderby_opt ::= */
  {  238,   -3 }, /* (178) orderby_opt ::= ORDER BY sortlist */
  {  249,   -4 }, /* (179) sortlist ::= sortlist COMMA item sortorder */
  {  249,   -2 }, /* (180) sortlist ::= item sortorder */
  {  251,   -2 }, /* (181) item ::= ids cpxName */
  {  252,   -1 }, /* (182) sortorder ::= ASC */
  {  252,   -1 }, /* (183) sortorder ::= DESC */
  {  252,    0 }, /* (184) sortorder ::= */
  {  237,    0 }, /* (185) groupby_opt ::= */
  {  237,   -3 }, /* (186) groupby_opt ::= GROUP BY grouplist */
  {  253,   -3 }, /* (187) grouplist ::= grouplist COMMA item */
  {  253,   -1 }, /* (188) grouplist ::= item */
  {  239,    0 }, /* (189) having_opt ::= */
  {  239,   -2 }, /* (190) having_opt ::= HAVING expr */
  {  241,    0 }, /* (191) limit_opt ::= */
  {  241,   -2 }, /* (192) limit_opt ::= LIMIT signed */
  {  241,   -4 }, /* (193) limit_opt ::= LIMIT signed OFFSET signed */
  {  241,   -4 }, /* (194) limit_opt ::= LIMIT signed COMMA signed */
  {  240,    0 }, /* (195) slimit_opt ::= */
  {  240,   -2 }, /* (196) slimit_opt ::= SLIMIT signed */
  {  240,   -4 }, /* (197) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  240,   -4 }, /* (198) slimit_opt ::= SLIMIT signed COMMA signed */
  {  233,    0 }, /* (199) where_opt ::= */
  {  233,   -2 }, /* (200) where_opt ::= WHERE expr */
  {  245,   -3 }, /* (201) expr ::= LP expr RP */
  {  245,   -1 }, /* (202) expr ::= ID */
  {  245,   -3 }, /* (203) expr ::= ID DOT ID */
  {  245,   -3 }, /* (204) expr ::= ID DOT STAR */
  {  245,   -1 }, /* (205) expr ::= INTEGER */
  {  245,   -2 }, /* (206) expr ::= MINUS INTEGER */
  {  245,   -2 }, /* (207) expr ::= PLUS INTEGER */
  {  245,   -1 }, /* (208) expr ::= FLOAT */
  {  245,   -2 }, /* (209) expr ::= MINUS FLOAT */
  {  245,   -2 }, /* (210) expr ::= PLUS FLOAT */
  {  245,   -1 }, /* (211) expr ::= STRING */
  {  245,   -1 }, /* (212) expr ::= NOW */
  {  245,   -1 }, /* (213) expr ::= VARIABLE */
  {  245,   -1 }, /* (214) expr ::= BOOL */
  {  245,   -4 }, /* (215) expr ::= ID LP exprlist RP */
  {  245,   -4 }, /* (216) expr ::= ID LP STAR RP */
  {  245,   -3 }, /* (217) expr ::= expr IS NULL */
  {  245,   -4 }, /* (218) expr ::= expr IS NOT NULL */
  {  245,   -3 }, /* (219) expr ::= expr LT expr */
  {  245,   -3 }, /* (220) expr ::= expr GT expr */
  {  245,   -3 }, /* (221) expr ::= expr LE expr */
  {  245,   -3 }, /* (222) expr ::= expr GE expr */
  {  245,   -3 }, /* (223) expr ::= expr NE expr */
  {  245,   -3 }, /* (224) expr ::= expr EQ expr */
  {  245,   -5 }, /* (225) expr ::= expr BETWEEN expr AND expr */
  {  245,   -3 }, /* (226) expr ::= expr AND expr */
  {  245,   -3 }, /* (227) expr ::= expr OR expr */
  {  245,   -3 }, /* (228) expr ::= expr PLUS expr */
  {  245,   -3 }, /* (229) expr ::= expr MINUS expr */
  {  245,   -3 }, /* (230) expr ::= expr STAR expr */
  {  245,   -3 }, /* (231) expr ::= expr SLASH expr */
  {  245,   -3 }, /* (232) expr ::= expr REM expr */
  {  245,   -3 }, /* (233) expr ::= expr LIKE expr */
  {  245,   -5 }, /* (234) expr ::= expr IN LP exprlist RP */
  {  254,   -3 }, /* (235) exprlist ::= exprlist COMMA expritem */
  {  254,   -1 }, /* (236) exprlist ::= expritem */
  {  255,   -1 }, /* (237) expritem ::= expr */
  {  255,    0 }, /* (238) expritem ::= */
  {  186,   -3 }, /* (239) cmd ::= RESET QUERY CACHE */
  {  186,   -7 }, /* (240) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  186,   -7 }, /* (241) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  186,   -7 }, /* (242) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  186,   -7 }, /* (243) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  186,   -8 }, /* (244) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  186,   -9 }, /* (245) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  186,   -7 }, /* (246) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  186,   -7 }, /* (247) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  186,   -7 }, /* (248) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  186,   -7 }, /* (249) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  186,   -8 }, /* (250) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  186,   -3 }, /* (251) cmd ::= KILL CONNECTION INTEGER */
  {  186,   -5 }, /* (252) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  186,   -5 }, /* (253) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy222, &t);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy299);}
        break;
      case 43: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy299);}
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
      case 162: /* distinct ::= */ yytestcase(yyruleno==162);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 48: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 50: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 51: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy299);}
        break;
      case 52: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy222, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy299.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy299.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy299.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy299.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy299.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy299.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy299.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy299.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy299.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy299 = yylhsminor.yy299;
        break;
      case 73: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy349 = yymsp[0].minor.yy349; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy222);}
        break;
      case 89: /* db_optr ::= db_optr cache */
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 90: /* db_optr ::= db_optr replica */
      case 105: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==105);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 91: /* db_optr ::= db_optr quorum */
      case 106: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==106);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 92: /* db_optr ::= db_optr days */
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 93: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 94: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 95: /* db_optr ::= db_optr blocks */
      case 108: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==108);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 96: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 97: /* db_optr ::= db_optr wal */
      case 110: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==110);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 98: /* db_optr ::= db_optr fsync */
      case 111: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==111);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 99: /* db_optr ::= db_optr comp */
      case 109: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==109);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 100: /* db_optr ::= db_optr prec */
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 101: /* db_optr ::= db_optr keep */
      case 107: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==107);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.keep = yymsp[0].minor.yy349; }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 102: /* db_optr ::= db_optr update */
      case 112: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==112);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 103: /* db_optr ::= db_optr cachelast */
      case 113: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==113);
{ yylhsminor.yy222 = yymsp[-1].minor.yy222; yylhsminor.yy222.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy222 = yylhsminor.yy222;
        break;
      case 104: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy222);}
        break;
      case 114: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy215, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy215 = yylhsminor.yy215;
        break;
      case 115: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy221 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy215, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy221;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy215, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy215 = yylhsminor.yy215;
        break;
      case 116: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy215, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy215 = yylhsminor.yy215;
        break;
      case 117: /* signed ::= INTEGER */
{ yylhsminor.yy221 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy221 = yylhsminor.yy221;
        break;
      case 118: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy221 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 119: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy221 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 123: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy278;}
        break;
      case 124: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy32);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy278 = pCreateTable;
}
  yymsp[0].minor.yy278 = yylhsminor.yy278;
        break;
      case 125: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy278->childTableInfo, &yymsp[0].minor.yy32);
  yylhsminor.yy278 = yymsp[-1].minor.yy278;
}
  yymsp[-1].minor.yy278 = yylhsminor.yy278;
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy278 = tSetCreateSqlElems(yymsp[-1].minor.yy349, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy278, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy278 = yylhsminor.yy278;
        break;
      case 127: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy278 = tSetCreateSqlElems(yymsp[-5].minor.yy349, yymsp[-1].minor.yy349, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy278, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy278 = yylhsminor.yy278;
        break;
      case 128: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy32 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy349, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy32 = yylhsminor.yy32;
        break;
      case 129: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy32 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy349, yymsp[-1].minor.yy349, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy32 = yylhsminor.yy32;
        break;
      case 130: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy349, &yymsp[0].minor.yy0); yylhsminor.yy349 = yymsp[-2].minor.yy349;  }
  yymsp[-2].minor.yy349 = yylhsminor.yy349;
        break;
      case 131: /* tagNamelist ::= ids */
{yylhsminor.yy349 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy349, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy349 = yylhsminor.yy349;
        break;
      case 132: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy278 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy216, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy278, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy278 = yylhsminor.yy278;
        break;
      case 133: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy349, &yymsp[0].minor.yy215); yylhsminor.yy349 = yymsp[-2].minor.yy349;  }
  yymsp[-2].minor.yy349 = yylhsminor.yy349;
        break;
      case 134: /* columnlist ::= column */
{yylhsminor.yy349 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy349, &yymsp[0].minor.yy215);}
  yymsp[0].minor.yy349 = yylhsminor.yy349;
        break;
      case 135: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy215, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy215);
}
  yymsp[-1].minor.yy215 = yylhsminor.yy215;
        break;
      case 136: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy349 = tVariantListAppend(yymsp[-2].minor.yy349, &yymsp[0].minor.yy426, -1);    }
  yymsp[-2].minor.yy349 = yylhsminor.yy349;
        break;
      case 137: /* tagitemlist ::= tagitem */
{ yylhsminor.yy349 = tVariantListAppend(NULL, &yymsp[0].minor.yy426, -1); }
  yymsp[0].minor.yy349 = yylhsminor.yy349;
        break;
      case 138: /* tagitem ::= INTEGER */
      case 139: /* tagitem ::= FLOAT */ yytestcase(yyruleno==139);
      case 140: /* tagitem ::= STRING */ yytestcase(yyruleno==140);
      case 141: /* tagitem ::= BOOL */ yytestcase(yyruleno==141);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy426, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy426 = yylhsminor.yy426;
        break;
      case 142: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy426, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy426 = yylhsminor.yy426;
        break;
      case 143: /* tagitem ::= MINUS INTEGER */
      case 144: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==144);
      case 145: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==145);
      case 146: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==146);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy426, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy426 = yylhsminor.yy426;
        break;
      case 147: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy216 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy258, yymsp[-9].minor.yy349, yymsp[-8].minor.yy114, yymsp[-4].minor.yy349, yymsp[-3].minor.yy349, &yymsp[-7].minor.yy264, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy349, &yymsp[0].minor.yy454, &yymsp[-1].minor.yy454);
}
  yymsp[-11].minor.yy216 = yylhsminor.yy216;
        break;
      case 148: /* union ::= select */
{ yylhsminor.yy417 = setSubclause(NULL, yymsp[0].minor.yy216); }
  yymsp[0].minor.yy417 = yylhsminor.yy417;
        break;
      case 149: /* union ::= LP union RP */
{ yymsp[-2].minor.yy417 = yymsp[-1].minor.yy417; }
        break;
      case 150: /* union ::= union UNION ALL select */
{ yylhsminor.yy417 = appendSelectClause(yymsp[-3].minor.yy417, yymsp[0].minor.yy216); }
  yymsp[-3].minor.yy417 = yylhsminor.yy417;
        break;
      case 151: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy417 = appendSelectClause(yymsp[-5].minor.yy417, yymsp[-1].minor.yy216); }
  yymsp[-5].minor.yy417 = yylhsminor.yy417;
        break;
      case 152: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy417, NULL, TSDB_SQL_SELECT); }
        break;
      case 153: /* select ::= SELECT selcollist */
{
  yylhsminor.yy216 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy258, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy216 = yylhsminor.yy216;
        break;
      case 154: /* sclp ::= selcollist COMMA */
{yylhsminor.yy258 = yymsp[-1].minor.yy258;}
  yymsp[-1].minor.yy258 = yylhsminor.yy258;
        break;
      case 155: /* sclp ::= */
{yymsp[1].minor.yy258 = 0;}
        break;
      case 156: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy258 = tSqlExprListAppend(yymsp[-3].minor.yy258, yymsp[-1].minor.yy114,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy258 = yylhsminor.yy258;
        break;
      case 157: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy258 = tSqlExprListAppend(yymsp[-1].minor.yy258, pNode, 0, 0);
}
  yymsp[-1].minor.yy258 = yylhsminor.yy258;
        break;
      case 158: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 159: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 160: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 161: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 163: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy349 = yymsp[0].minor.yy349;}
        break;
      case 164: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy349 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy349 = tVariantListAppendToken(yylhsminor.yy349, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy349 = yylhsminor.yy349;
        break;
      case 165: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy349 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy349 = tVariantListAppendToken(yylhsminor.yy349, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy349 = yylhsminor.yy349;
        break;
      case 166: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy349 = tVariantListAppendToken(yymsp[-3].minor.yy349, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy349 = tVariantListAppendToken(yylhsminor.yy349, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy349 = yylhsminor.yy349;
        break;
      case 167: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy349 = tVariantListAppendToken(yymsp[-4].minor.yy349, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy349 = tVariantListAppendToken(yylhsminor.yy349, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy349 = yylhsminor.yy349;
        break;
      case 168: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy264.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy264.offset.n = 0; yymsp[-3].minor.yy264.offset.z = NULL; yymsp[-3].minor.yy264.offset.type = 0;}
        break;
      case 170: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy264.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy264.offset = yymsp[-1].minor.yy0;}
        break;
      case 171: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy264, 0, sizeof(yymsp[1].minor.yy264));}
        break;
      case 172: /* fill_opt ::= */
{yymsp[1].minor.yy349 = 0;     }
        break;
      case 173: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy349, &A, -1, 0);
    yymsp[-5].minor.yy349 = yymsp[-1].minor.yy349;
}
        break;
      case 174: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy349 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 175: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 176: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 177: /* orderby_opt ::= */
{yymsp[1].minor.yy349 = 0;}
        break;
      case 178: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy349 = yymsp[0].minor.yy349;}
        break;
      case 179: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy349 = tVariantListAppend(yymsp[-3].minor.yy349, &yymsp[-1].minor.yy426, yymsp[0].minor.yy348);
}
  yymsp[-3].minor.yy349 = yylhsminor.yy349;
        break;
      case 180: /* sortlist ::= item sortorder */
{
  yylhsminor.yy349 = tVariantListAppend(NULL, &yymsp[-1].minor.yy426, yymsp[0].minor.yy348);
}
  yymsp[-1].minor.yy349 = yylhsminor.yy349;
        break;
      case 181: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy426, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy426 = yylhsminor.yy426;
        break;
      case 182: /* sortorder ::= ASC */
{ yymsp[0].minor.yy348 = TSDB_ORDER_ASC; }
        break;
      case 183: /* sortorder ::= DESC */
{ yymsp[0].minor.yy348 = TSDB_ORDER_DESC;}
        break;
      case 184: /* sortorder ::= */
{ yymsp[1].minor.yy348 = TSDB_ORDER_ASC; }
        break;
      case 185: /* groupby_opt ::= */
{ yymsp[1].minor.yy349 = 0;}
        break;
      case 186: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy349 = yymsp[0].minor.yy349;}
        break;
      case 187: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy349 = tVariantListAppend(yymsp[-2].minor.yy349, &yymsp[0].minor.yy426, -1);
}
  yymsp[-2].minor.yy349 = yylhsminor.yy349;
        break;
      case 188: /* grouplist ::= item */
{
  yylhsminor.yy349 = tVariantListAppend(NULL, &yymsp[0].minor.yy426, -1);
}
  yymsp[0].minor.yy349 = yylhsminor.yy349;
        break;
      case 189: /* having_opt ::= */
      case 199: /* where_opt ::= */ yytestcase(yyruleno==199);
      case 238: /* expritem ::= */ yytestcase(yyruleno==238);
{yymsp[1].minor.yy114 = 0;}
        break;
      case 190: /* having_opt ::= HAVING expr */
      case 200: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==200);
{yymsp[-1].minor.yy114 = yymsp[0].minor.yy114;}
        break;
      case 191: /* limit_opt ::= */
      case 195: /* slimit_opt ::= */ yytestcase(yyruleno==195);
{yymsp[1].minor.yy454.limit = -1; yymsp[1].minor.yy454.offset = 0;}
        break;
      case 192: /* limit_opt ::= LIMIT signed */
      case 196: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==196);
{yymsp[-1].minor.yy454.limit = yymsp[0].minor.yy221;  yymsp[-1].minor.yy454.offset = 0;}
        break;
      case 193: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy454.limit = yymsp[-2].minor.yy221;  yymsp[-3].minor.yy454.offset = yymsp[0].minor.yy221;}
        break;
      case 194: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy454.limit = yymsp[0].minor.yy221;  yymsp[-3].minor.yy454.offset = yymsp[-2].minor.yy221;}
        break;
      case 197: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy454.limit = yymsp[-2].minor.yy221;  yymsp[-3].minor.yy454.offset = yymsp[0].minor.yy221;}
        break;
      case 198: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy454.limit = yymsp[0].minor.yy221;  yymsp[-3].minor.yy454.offset = yymsp[-2].minor.yy221;}
        break;
      case 201: /* expr ::= LP expr RP */
{yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy114->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 202: /* expr ::= ID */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 203: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 204: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 205: /* expr ::= INTEGER */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 206: /* expr ::= MINUS INTEGER */
      case 207: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==207);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 208: /* expr ::= FLOAT */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 209: /* expr ::= MINUS FLOAT */
      case 210: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==210);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 211: /* expr ::= STRING */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 212: /* expr ::= NOW */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 213: /* expr ::= VARIABLE */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 214: /* expr ::= BOOL */
{ yylhsminor.yy114 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 215: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy114 = tSqlExprCreateFunction(yymsp[-1].minor.yy258, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy114 = yylhsminor.yy114;
        break;
      case 216: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy114 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy114 = yylhsminor.yy114;
        break;
      case 217: /* expr ::= expr IS NULL */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 218: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-3].minor.yy114, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy114 = yylhsminor.yy114;
        break;
      case 219: /* expr ::= expr LT expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_LT);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 220: /* expr ::= expr GT expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_GT);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 221: /* expr ::= expr LE expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_LE);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 222: /* expr ::= expr GE expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_GE);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 223: /* expr ::= expr NE expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_NE);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 224: /* expr ::= expr EQ expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_EQ);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 225: /* expr ::= expr BETWEEN expr AND expr */
{ tSQLExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy114); yylhsminor.yy114 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy114, yymsp[-2].minor.yy114, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy114, TK_LE), TK_AND);}
  yymsp[-4].minor.yy114 = yylhsminor.yy114;
        break;
      case 226: /* expr ::= expr AND expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_AND);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 227: /* expr ::= expr OR expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_OR); }
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 228: /* expr ::= expr PLUS expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_PLUS);  }
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 229: /* expr ::= expr MINUS expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_MINUS); }
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 230: /* expr ::= expr STAR expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_STAR);  }
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 231: /* expr ::= expr SLASH expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_DIVIDE);}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 232: /* expr ::= expr REM expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_REM);   }
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 233: /* expr ::= expr LIKE expr */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-2].minor.yy114, yymsp[0].minor.yy114, TK_LIKE);  }
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 234: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy114 = tSqlExprCreate(yymsp[-4].minor.yy114, (tSQLExpr*)yymsp[-1].minor.yy258, TK_IN); }
  yymsp[-4].minor.yy114 = yylhsminor.yy114;
        break;
      case 235: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy258 = tSqlExprListAppend(yymsp[-2].minor.yy258,yymsp[0].minor.yy114,0, 0);}
  yymsp[-2].minor.yy258 = yylhsminor.yy258;
        break;
      case 236: /* exprlist ::= expritem */
{yylhsminor.yy258 = tSqlExprListAppend(0,yymsp[0].minor.yy114,0, 0);}
  yymsp[0].minor.yy258 = yylhsminor.yy258;
        break;
      case 237: /* expritem ::= expr */
{yylhsminor.yy114 = yymsp[0].minor.yy114;}
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 239: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 240: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy349, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 241: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 242: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy349, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 243: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 244: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 245: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy426, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 246: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy349, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 247: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 248: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy349, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 249: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 250: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 251: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 252: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 253: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
