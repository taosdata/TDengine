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
#define YYNOCODE 264
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateTableSql* yy14;
  int yy20;
  tSqlExpr* yy118;
  SArray* yy159;
  SIntervalVal yy184;
  SCreatedTableInfo yy206;
  SSessionWindowVal yy249;
  SQuerySqlNode* yy272;
  int64_t yy317;
  SCreateDbInfo yy322;
  SCreateAcctInfo yy351;
  SSubclauseInfo* yy391;
  TAOS_FIELD yy407;
  SLimitVal yy440;
  tVariant yy488;
  SFromInfo* yy514;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             315
#define YYNRULE              266
#define YYNTOKEN             187
#define YY_MAX_SHIFT         314
#define YY_MIN_SHIFTREDUCE   505
#define YY_MAX_SHIFTREDUCE   770
#define YY_ERROR_ACTION      771
#define YY_ACCEPT_ACTION     772
#define YY_NO_ACTION         773
#define YY_MIN_REDUCE        774
#define YY_MAX_REDUCE        1039
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
#define YY_ACTTAB_COUNT (679)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   133,  552,  202,  312,  206,  140,  941,   17,   85,  553,
 /*    10 */   772,  314,  179,   47,   48,  140,   51,   52,   30,  181,
 /*    20 */   214,   41,  181,   50,  262,   55,   53,   57,   54, 1020,
 /*    30 */   920,  209, 1021,   46,   45,  185,  181,   44,   43,   42,
 /*    40 */    47,   48,  908,   51,   52,  208, 1021,  214,   41,  552,
 /*    50 */    50,  262,   55,   53,   57,   54,  932,  553, 1017,  203,
 /*    60 */    46,   45,  917,  247,   44,   43,   42,   48,  938,   51,
 /*    70 */    52,  242,  972,  214,   41,  552,   50,  262,   55,   53,
 /*    80 */    57,   54,  973,  553,  257,  278,   46,   45,  298,  225,
 /*    90 */    44,   43,   42,  506,  507,  508,  509,  510,  511,  512,
 /*   100 */   513,  514,  515,  516,  517,  518,  313,  631, 1016,  231,
 /*   110 */    70,  552,   30,   47,   48, 1015,   51,   52,  819,  553,
 /*   120 */   214,   41,  166,   50,  262,   55,   53,   57,   54,   44,
 /*   130 */    43,   42,  717,   46,   45,  288,  287,   44,   43,   42,
 /*   140 */    47,   49,  198,   51,   52,  140,  140,  214,   41,  234,
 /*   150 */    50,  262,   55,   53,   57,   54,  916,  238,  237,  227,
 /*   160 */    46,   45,  285,  284,   44,   43,   42,   23,  276,  307,
 /*   170 */   306,  275,  274,  273,  305,  272,  304,  303,  302,  271,
 /*   180 */   301,  300,  880,   30,  868,  869,  870,  871,  872,  873,
 /*   190 */   874,  875,  876,  877,  878,  879,  881,  882,   51,   52,
 /*   200 */    18,   30,  214,   41,  906,   50,  262,   55,   53,   57,
 /*   210 */    54,  259,   79,   78,   25,   46,   45,  190,  199,   44,
 /*   220 */    43,   42,   82,  191,  217,   28,   30,  917,  268,  118,
 /*   230 */   117,  189,   12,  213,  730,  932,   84,  721,   81,  724,
 /*   240 */    74,  727,  218,  213,  730,  917,   80,  721,   36,  724,
 /*   250 */   204,  727,   30,  903,  904,   29,  907,   46,   45,   71,
 /*   260 */    74,   44,   43,   42,  223,  210,  211,  281,   36,  261,
 /*   270 */   917,   23,  914,  307,  306,  210,  211,  723,  305,  726,
 /*   280 */   304,  303,  302,  278,  301,  300,  311,  310,  126,  677,
 /*   290 */   241,  888,   68,  282,  886,  887,  917,  245,  197,  889,
 /*   300 */   219,  891,  892,  890,  670,  893,  894,   55,   53,   57,
 /*   310 */    54,    1,  154,  263,  220,   46,   45,   30,  221,   44,
 /*   320 */    43,   42,  105,  103,  108,  308,  920,  298,   69,   97,
 /*   330 */   107,  113,  116,  106,  224,  655,   56,  280,  652,  110,
 /*   340 */   653,  226,  654,   30,  920,  667,   56,    5,  156,  729,
 /*   350 */   183,  920,   24,   33,  155,   92,   87,   91,  286,  729,
 /*   360 */   905,  917,  174,  170,  719,  728,  228,  229,  172,  169,
 /*   370 */   121,  120,  119,  828,  820,  728,  918,  166,  166,    3,
 /*   380 */   167,  243,  674,  212,  290,   31,  683,  917,  698,  699,
 /*   390 */   135,  689,  690,  750,  731,   60,   20,   19,   19,  722,
 /*   400 */   720,  725,   61,   64,  641,  184,  265,  643,   31,  733,
 /*   410 */    31,   60,  267,  642,  115,  114,   83,   60,   96,   95,
 /*   420 */   186,   14,   13,   65,   62,  180,  187,    6,  102,  101,
 /*   430 */    67,  188,  630,   16,   15,  659,  657,  660,  658,  131,
 /*   440 */   129,  194,  195,  193,  656,  178,  192,  182, 1031,  919,
 /*   450 */   983,  239,  982,  215,  979,  978,  216,  289,   39,  132,
 /*   460 */   940,  948,  950,  130,  134,  933,  138,  246,  965,  964,
 /*   470 */   151,  915,  150,  682,  248,  913,  205,  299,  104,  884,
 /*   480 */   160,  260,  152,  153,  145,  143,  141,  831,  270,   66,
 /*   490 */   250,  930,   63,  255,   37,  176,   34,  279,   58,  142,
 /*   500 */   827, 1036,   93, 1035, 1033,  157,  283, 1030,   99, 1029,
 /*   510 */  1027,  158,  849,   35,  258,   32,   38,  256,  177,  816,
 /*   520 */   109,  814,  111,  112,  254,  812,  811,  230,  168,  252,
 /*   530 */   809,  808,  807,  806,  805,  804,  171,  173,  801,  799,
 /*   540 */   797,  795,  793,  175,  249,  244,   72,   75,   40,  251,
 /*   550 */   966,  291,  292,  293,  294,  295,  296,  297,  309,  200,
 /*   560 */   222,  770,  269,  232,  233,  769,  235,  201,  196,   88,
 /*   570 */    89,  236,  768,  756,  755,  240,  245,    8,  810,  662,
 /*   580 */   122,  161,  123,  165,  163,  803,  850,  159,  162,  164,
 /*   590 */   124,  802,   73,  125,  794,    4,    2,  264,   76,  684,
 /*   600 */   136,  137,  687,   77,  144,  148,  146,  147,  149,  896,
 /*   610 */   207,  253,   26,  691,  139,   27,    9,  732,   10,    7,
 /*   620 */   734,   11,   21,  266,   22,   86,  594,   84,  590,  588,
 /*   630 */   587,  586,  583,  556,  277,   31,   59,   90,  633,   94,
 /*   640 */   632,  629,  578,   98,  100,  576,  568,  574,  570,  572,
 /*   650 */   566,  564,  597,  596,  595,  593,  592,  591,  589,  585,
 /*   660 */   584,   60,  554,  522,  520,  774,  773,  773,  773,  773,
 /*   670 */   773,  773,  773,  773,  773,  773,  773,  127,  128,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   191,    1,  190,  191,  210,  191,  191,  252,  197,    9,
 /*    10 */   188,  189,  252,   13,   14,  191,   16,   17,  191,  252,
 /*    20 */    20,   21,  252,   23,   24,   25,   26,   27,   28,  262,
 /*    30 */   236,  261,  262,   33,   34,  252,  252,   37,   38,   39,
 /*    40 */    13,   14,  231,   16,   17,  261,  262,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  234,    9,  252,  232,
 /*    60 */    33,   34,  235,  254,   37,   38,   39,   14,  253,   16,
 /*    70 */    17,  249,  258,   20,   21,    1,   23,   24,   25,   26,
 /*    80 */    27,   28,  258,    9,  260,   79,   33,   34,   81,   67,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,    5,  252,   61,
 /*   110 */   110,    1,  191,   13,   14,  252,   16,   17,  196,    9,
 /*   120 */    20,   21,  200,   23,   24,   25,   26,   27,   28,   37,
 /*   130 */    38,   39,  105,   33,   34,   33,   34,   37,   38,   39,
 /*   140 */    13,   14,  252,   16,   17,  191,  191,   20,   21,  135,
 /*   150 */    23,   24,   25,   26,   27,   28,  235,  143,  144,  137,
 /*   160 */    33,   34,  140,  141,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  209,  191,  211,  212,  213,  214,  215,  216,
 /*   190 */   217,  218,  219,  220,  221,  222,  223,  224,   16,   17,
 /*   200 */    44,  191,   20,   21,    0,   23,   24,   25,   26,   27,
 /*   210 */    28,  256,  258,  258,  104,   33,   34,   61,  252,   37,
 /*   220 */    38,   39,  197,   67,  232,  104,  191,  235,  107,   73,
 /*   230 */    74,   75,  104,    1,    2,  234,  108,    5,  110,    7,
 /*   240 */   104,    9,  232,    1,    2,  235,  237,    5,  112,    7,
 /*   250 */   249,    9,  191,  228,  229,  230,  231,   33,   34,  250,
 /*   260 */   104,   37,   38,   39,   67,   33,   34,  232,  112,   37,
 /*   270 */   235,   88,  191,   90,   91,   33,   34,    5,   95,    7,
 /*   280 */    97,   98,   99,   79,  101,  102,   64,   65,   66,  105,
 /*   290 */   134,  209,  136,  232,  212,  213,  235,  113,  142,  217,
 /*   300 */   210,  219,  220,  221,   37,  223,  224,   25,   26,   27,
 /*   310 */    28,  198,  199,   15,  233,   33,   34,  191,  210,   37,
 /*   320 */    38,   39,   76,   62,   63,  210,  236,   81,  197,   68,
 /*   330 */    69,   70,   71,   72,  137,    2,  104,  140,    5,   78,
 /*   340 */     7,  191,    9,  191,  236,  109,  104,   62,   63,  117,
 /*   350 */   252,  236,  116,   68,   69,   70,   71,   72,  232,  117,
 /*   360 */   229,  235,   62,   63,    1,  133,   33,   34,   68,   69,
 /*   370 */    70,   71,   72,  196,  196,  133,  226,  200,  200,  194,
 /*   380 */   195,  105,  115,   60,  232,  109,  105,  235,  124,  125,
 /*   390 */   109,  105,  105,  105,  105,  109,  109,  109,  109,    5,
 /*   400 */    37,    7,  109,  109,  105,  252,  105,  105,  109,  111,
 /*   410 */   109,  109,  105,  105,   76,   77,  109,  109,  138,  139,
 /*   420 */   252,  138,  139,  129,  131,  252,  252,  104,  138,  139,
 /*   430 */   104,  252,  106,  138,  139,    5,    5,    7,    7,   62,
 /*   440 */    63,  252,  252,  252,  111,  252,  252,  252,  236,  236,
 /*   450 */   227,  191,  227,  227,  227,  227,  227,  227,  251,  191,
 /*   460 */   191,  191,  191,   60,  191,  234,  191,  234,  259,  259,
 /*   470 */   191,  234,  238,  117,  255,  191,  255,  103,   87,  225,
 /*   480 */   206,  122,  191,  191,  243,  245,  247,  191,  191,  128,
 /*   490 */   255,  248,  130,  255,  191,  191,  191,  191,  127,  246,
 /*   500 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   510 */   191,  191,  191,  191,  126,  191,  191,  121,  191,  191,
 /*   520 */   191,  191,  191,  191,  120,  191,  191,  191,  191,  119,
 /*   530 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   540 */   191,  191,  191,  191,  118,  192,  192,  192,  132,  192,
 /*   550 */   192,   86,   50,   83,   85,   54,   84,   82,   79,  192,
 /*   560 */   192,    5,  192,  145,    5,    5,  145,  192,  192,  197,
 /*   570 */   197,    5,    5,   90,   89,  135,  113,  104,  192,  105,
 /*   580 */   193,  202,  193,  201,  203,  192,  208,  207,  205,  204,
 /*   590 */   193,  192,  114,  193,  192,  194,  198,  107,  109,  105,
 /*   600 */   104,  109,  105,  104,  244,  240,  242,  241,  239,  225,
 /*   610 */     1,  104,  109,  105,  104,  109,  123,  105,  123,  104,
 /*   620 */   111,  104,  104,  107,  104,   76,    9,  108,    5,    5,
 /*   630 */     5,    5,    5,   80,   15,  109,   16,   76,    5,  139,
 /*   640 */     5,  105,    5,  139,  139,    5,    5,    5,    5,    5,
 /*   650 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   660 */     5,  109,   80,   60,   59,    0,  263,  263,  263,  263,
 /*   670 */   263,  263,  263,  263,  263,  263,  263,   21,   21,  263,
 /*   680 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
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
 /*   860 */   263,  263,  263,  263,  263,  263,
};
#define YY_SHIFT_COUNT    (314)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (665)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   156,   79,   79,  183,  183,    6,  232,  242,   74,   74,
 /*    10 */    74,   74,   74,   74,   74,   74,   74,    0,   48,  242,
 /*    20 */   333,  333,  333,  333,  110,  136,   74,   74,   74,  204,
 /*    30 */    74,   74,  246,    6,    7,    7,  679,  679,  679,  242,
 /*    40 */   242,  242,  242,  242,  242,  242,  242,  242,  242,  242,
 /*    50 */   242,  242,  242,  242,  242,  242,  242,  242,  242,  333,
 /*    60 */   333,  102,  102,  102,  102,  102,  102,  102,   74,   74,
 /*    70 */    74,  267,   74,  136,  136,   74,   74,   74,  264,  264,
 /*    80 */   236,  136,   74,   74,   74,   74,   74,   74,   74,   74,
 /*    90 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   100 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   110 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   120 */    74,   74,   74,   74,   74,   74,   74,   74,   74,   74,
 /*   130 */    74,   74,  403,  403,  403,  356,  356,  356,  403,  356,
 /*   140 */   403,  361,  362,  371,  359,  388,  396,  404,  410,  426,
 /*   150 */   416,  403,  403,  403,  374,    6,    6,  403,  403,  391,
 /*   160 */   465,  502,  470,  469,  501,  472,  475,  374,  403,  479,
 /*   170 */   479,  403,  479,  403,  479,  403,  679,  679,   27,  100,
 /*   180 */   127,  100,  100,   53,  182,  282,  282,  282,  282,  261,
 /*   190 */   285,  300,  224,  224,  224,  224,   22,   14,   92,   92,
 /*   200 */   128,  197,  222,  276,  184,  281,  286,  287,  288,  289,
 /*   210 */   272,  394,  363,  323,  298,  293,  294,  299,  301,  302,
 /*   220 */   307,  308,  121,  280,  283,  290,  326,  295,  430,  431,
 /*   230 */   338,  377,  556,  418,  559,  560,  421,  566,  567,  483,
 /*   240 */   485,  440,  463,  490,  473,  478,  474,  489,  494,  496,
 /*   250 */   497,  492,  499,  609,  507,  508,  510,  503,  493,  506,
 /*   260 */   495,  512,  515,  509,  517,  490,  518,  516,  520,  519,
 /*   270 */   549,  617,  623,  624,  625,  626,  627,  553,  619,  561,
 /*   280 */   500,  526,  526,  620,  504,  505,  526,  633,  635,  536,
 /*   290 */   526,  637,  640,  641,  642,  643,  644,  645,  646,  647,
 /*   300 */   648,  649,  650,  651,  652,  653,  654,  655,  552,  582,
 /*   310 */   656,  657,  603,  605,  665,
};
#define YY_REDUCE_COUNT (177)
#define YY_REDUCE_MIN   (-245)
#define YY_REDUCE_MAX   (402)
static const short yy_reduce_ofst[] = {
 /*     0 */  -178,  -27,  -27,   82,   82,   25, -230, -216, -173, -176,
 /*    10 */   -45,   -8,   10,   35,   61,  126,  152, -185, -188, -233,
 /*    20 */  -206,   90,  108,  115, -191,    1, -186,  -46,   81, -189,
 /*    30 */   150,  -79,  -78,  131,  177,  178,    9,  113,  185, -245,
 /*    40 */  -240, -217, -194, -144, -137, -110,  -34,   98,  153,  168,
 /*    50 */   173,  174,  179,  189,  190,  191,  193,  194,  195,  212,
 /*    60 */   213,  223,  225,  226,  227,  228,  229,  230,  260,  268,
 /*    70 */   269,  207,  270,  231,  233,  271,  273,  275,  209,  210,
 /*    80 */   234,  237,  279,  284,  291,  292,  296,  297,  303,  304,
 /*    90 */   305,  306,  309,  310,  311,  312,  313,  314,  315,  316,
 /*   100 */   317,  318,  319,  320,  321,  322,  324,  325,  327,  328,
 /*   110 */   329,  330,  331,  332,  334,  335,  336,  337,  339,  340,
 /*   120 */   341,  342,  343,  344,  345,  346,  347,  348,  349,  350,
 /*   130 */   351,  352,  353,  354,  355,  219,  221,  235,  357,  238,
 /*   140 */   358,  243,  239,  253,  240,  360,  241,  364,  366,  365,
 /*   150 */   369,  367,  368,  370,  254,  372,  373,  375,  376,  378,
 /*   160 */   380,  274,  379,  383,  381,  385,  382,  384,  386,  387,
 /*   170 */   389,  393,  397,  399,  400,  402,  398,  401,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   771,  883,  829,  895,  817,  826, 1023, 1023,  771,  771,
 /*    10 */   771,  771,  771,  771,  771,  771,  771,  942,  790, 1023,
 /*    20 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  826,
 /*    30 */   771,  771,  832,  826,  832,  832,  937,  867,  885,  771,
 /*    40 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    50 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    60 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    70 */   771,  944,  947,  771,  771,  949,  771,  771,  969,  969,
 /*    80 */   935,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    90 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   100 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  815,
 /*   110 */   771,  813,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   120 */   771,  771,  771,  771,  771,  771,  800,  771,  771,  771,
 /*   130 */   771,  771,  792,  792,  792,  771,  771,  771,  792,  771,
 /*   140 */   792,  976,  980,  974,  962,  970,  961,  957,  955,  954,
 /*   150 */   984,  792,  792,  792,  830,  826,  826,  792,  792,  848,
 /*   160 */   846,  844,  836,  842,  838,  840,  834,  818,  792,  824,
 /*   170 */   824,  792,  824,  792,  824,  792,  867,  885,  771,  985,
 /*   180 */   771, 1022,  975, 1012, 1011, 1018, 1010, 1009, 1008,  771,
 /*   190 */   771,  771, 1004, 1005, 1007, 1006,  771,  771, 1014, 1013,
 /*   200 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   210 */   771,  771,  771,  987,  771,  981,  977,  771,  771,  771,
 /*   220 */   771,  771,  771,  771,  771,  771,  897,  771,  771,  771,
 /*   230 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   240 */   771,  771,  934,  771,  771,  771,  771,  945,  771,  771,
 /*   250 */   771,  771,  771,  771,  771,  771,  771,  971,  771,  963,
 /*   260 */   771,  771,  771,  771,  771,  909,  771,  771,  771,  771,
 /*   270 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   280 */   771, 1034, 1032,  771,  771,  771, 1028,  771,  771,  771,
 /*   290 */  1026,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   300 */   771,  771,  771,  771,  771,  771,  771,  771,  851,  771,
 /*   310 */   798,  796,  771,  788,  771,
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
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    1,  /*     STABLE => ID */
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
  /*   63 */ "DATABASE",
  /*   64 */ "TABLES",
  /*   65 */ "STABLES",
  /*   66 */ "VGROUPS",
  /*   67 */ "DROP",
  /*   68 */ "STABLE",
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
 /*  21 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  28 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  29 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  31 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  32 */ "cmd ::= DROP DNODE ids",
 /*  33 */ "cmd ::= DROP USER ids",
 /*  34 */ "cmd ::= DROP ACCOUNT ids",
 /*  35 */ "cmd ::= USE ids",
 /*  36 */ "cmd ::= DESCRIBE ids cpxName",
 /*  37 */ "cmd ::= ALTER USER ids PASS ids",
 /*  38 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  39 */ "cmd ::= ALTER DNODE ids ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  41 */ "cmd ::= ALTER LOCAL ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids ids",
 /*  43 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  44 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  45 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  47 */ "ids ::= ID",
 /*  48 */ "ids ::= STRING",
 /*  49 */ "ifexists ::= IF EXISTS",
 /*  50 */ "ifexists ::=",
 /*  51 */ "ifnotexists ::= IF NOT EXISTS",
 /*  52 */ "ifnotexists ::=",
 /*  53 */ "cmd ::= CREATE DNODE ids",
 /*  54 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  55 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  56 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  57 */ "cmd ::= CREATE USER ids PASS ids",
 /*  58 */ "pps ::=",
 /*  59 */ "pps ::= PPS INTEGER",
 /*  60 */ "tseries ::=",
 /*  61 */ "tseries ::= TSERIES INTEGER",
 /*  62 */ "dbs ::=",
 /*  63 */ "dbs ::= DBS INTEGER",
 /*  64 */ "streams ::=",
 /*  65 */ "streams ::= STREAMS INTEGER",
 /*  66 */ "storage ::=",
 /*  67 */ "storage ::= STORAGE INTEGER",
 /*  68 */ "qtime ::=",
 /*  69 */ "qtime ::= QTIME INTEGER",
 /*  70 */ "users ::=",
 /*  71 */ "users ::= USERS INTEGER",
 /*  72 */ "conns ::=",
 /*  73 */ "conns ::= CONNS INTEGER",
 /*  74 */ "state ::=",
 /*  75 */ "state ::= STATE ids",
 /*  76 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  77 */ "keep ::= KEEP tagitemlist",
 /*  78 */ "cache ::= CACHE INTEGER",
 /*  79 */ "replica ::= REPLICA INTEGER",
 /*  80 */ "quorum ::= QUORUM INTEGER",
 /*  81 */ "days ::= DAYS INTEGER",
 /*  82 */ "minrows ::= MINROWS INTEGER",
 /*  83 */ "maxrows ::= MAXROWS INTEGER",
 /*  84 */ "blocks ::= BLOCKS INTEGER",
 /*  85 */ "ctime ::= CTIME INTEGER",
 /*  86 */ "wal ::= WAL INTEGER",
 /*  87 */ "fsync ::= FSYNC INTEGER",
 /*  88 */ "comp ::= COMP INTEGER",
 /*  89 */ "prec ::= PRECISION STRING",
 /*  90 */ "update ::= UPDATE INTEGER",
 /*  91 */ "cachelast ::= CACHELAST INTEGER",
 /*  92 */ "partitions ::= PARTITIONS INTEGER",
 /*  93 */ "db_optr ::=",
 /*  94 */ "db_optr ::= db_optr cache",
 /*  95 */ "db_optr ::= db_optr replica",
 /*  96 */ "db_optr ::= db_optr quorum",
 /*  97 */ "db_optr ::= db_optr days",
 /*  98 */ "db_optr ::= db_optr minrows",
 /*  99 */ "db_optr ::= db_optr maxrows",
 /* 100 */ "db_optr ::= db_optr blocks",
 /* 101 */ "db_optr ::= db_optr ctime",
 /* 102 */ "db_optr ::= db_optr wal",
 /* 103 */ "db_optr ::= db_optr fsync",
 /* 104 */ "db_optr ::= db_optr comp",
 /* 105 */ "db_optr ::= db_optr prec",
 /* 106 */ "db_optr ::= db_optr keep",
 /* 107 */ "db_optr ::= db_optr update",
 /* 108 */ "db_optr ::= db_optr cachelast",
 /* 109 */ "topic_optr ::= db_optr",
 /* 110 */ "topic_optr ::= topic_optr partitions",
 /* 111 */ "alter_db_optr ::=",
 /* 112 */ "alter_db_optr ::= alter_db_optr replica",
 /* 113 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 114 */ "alter_db_optr ::= alter_db_optr keep",
 /* 115 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 116 */ "alter_db_optr ::= alter_db_optr comp",
 /* 117 */ "alter_db_optr ::= alter_db_optr wal",
 /* 118 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 119 */ "alter_db_optr ::= alter_db_optr update",
 /* 120 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 121 */ "alter_topic_optr ::= alter_db_optr",
 /* 122 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 123 */ "typename ::= ids",
 /* 124 */ "typename ::= ids LP signed RP",
 /* 125 */ "typename ::= ids UNSIGNED",
 /* 126 */ "signed ::= INTEGER",
 /* 127 */ "signed ::= PLUS INTEGER",
 /* 128 */ "signed ::= MINUS INTEGER",
 /* 129 */ "cmd ::= CREATE TABLE create_table_args",
 /* 130 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 131 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE TABLE create_table_list",
 /* 133 */ "create_table_list ::= create_from_stable",
 /* 134 */ "create_table_list ::= create_table_list create_from_stable",
 /* 135 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 136 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 137 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 139 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 140 */ "tagNamelist ::= ids",
 /* 141 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 142 */ "columnlist ::= columnlist COMMA column",
 /* 143 */ "columnlist ::= column",
 /* 144 */ "column ::= ids typename",
 /* 145 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 146 */ "tagitemlist ::= tagitem",
 /* 147 */ "tagitem ::= INTEGER",
 /* 148 */ "tagitem ::= FLOAT",
 /* 149 */ "tagitem ::= STRING",
 /* 150 */ "tagitem ::= BOOL",
 /* 151 */ "tagitem ::= NULL",
 /* 152 */ "tagitem ::= MINUS INTEGER",
 /* 153 */ "tagitem ::= MINUS FLOAT",
 /* 154 */ "tagitem ::= PLUS INTEGER",
 /* 155 */ "tagitem ::= PLUS FLOAT",
 /* 156 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 157 */ "select ::= LP select RP",
 /* 158 */ "union ::= select",
 /* 159 */ "union ::= union UNION ALL select",
 /* 160 */ "cmd ::= union",
 /* 161 */ "select ::= SELECT selcollist",
 /* 162 */ "sclp ::= selcollist COMMA",
 /* 163 */ "sclp ::=",
 /* 164 */ "selcollist ::= sclp distinct expr as",
 /* 165 */ "selcollist ::= sclp STAR",
 /* 166 */ "as ::= AS ids",
 /* 167 */ "as ::= ids",
 /* 168 */ "as ::=",
 /* 169 */ "distinct ::= DISTINCT",
 /* 170 */ "distinct ::=",
 /* 171 */ "from ::= FROM tablelist",
 /* 172 */ "from ::= FROM LP union RP",
 /* 173 */ "tablelist ::= ids cpxName",
 /* 174 */ "tablelist ::= ids cpxName ids",
 /* 175 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 176 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 177 */ "tmvar ::= VARIABLE",
 /* 178 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 179 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 180 */ "interval_opt ::=",
 /* 181 */ "session_option ::=",
 /* 182 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 183 */ "fill_opt ::=",
 /* 184 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 185 */ "fill_opt ::= FILL LP ID RP",
 /* 186 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 187 */ "sliding_opt ::=",
 /* 188 */ "orderby_opt ::=",
 /* 189 */ "orderby_opt ::= ORDER BY sortlist",
 /* 190 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 191 */ "sortlist ::= item sortorder",
 /* 192 */ "item ::= ids cpxName",
 /* 193 */ "sortorder ::= ASC",
 /* 194 */ "sortorder ::= DESC",
 /* 195 */ "sortorder ::=",
 /* 196 */ "groupby_opt ::=",
 /* 197 */ "groupby_opt ::= GROUP BY grouplist",
 /* 198 */ "grouplist ::= grouplist COMMA item",
 /* 199 */ "grouplist ::= item",
 /* 200 */ "having_opt ::=",
 /* 201 */ "having_opt ::= HAVING expr",
 /* 202 */ "limit_opt ::=",
 /* 203 */ "limit_opt ::= LIMIT signed",
 /* 204 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 205 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 206 */ "slimit_opt ::=",
 /* 207 */ "slimit_opt ::= SLIMIT signed",
 /* 208 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 209 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 210 */ "where_opt ::=",
 /* 211 */ "where_opt ::= WHERE expr",
 /* 212 */ "expr ::= LP expr RP",
 /* 213 */ "expr ::= ID",
 /* 214 */ "expr ::= ID DOT ID",
 /* 215 */ "expr ::= ID DOT STAR",
 /* 216 */ "expr ::= INTEGER",
 /* 217 */ "expr ::= MINUS INTEGER",
 /* 218 */ "expr ::= PLUS INTEGER",
 /* 219 */ "expr ::= FLOAT",
 /* 220 */ "expr ::= MINUS FLOAT",
 /* 221 */ "expr ::= PLUS FLOAT",
 /* 222 */ "expr ::= STRING",
 /* 223 */ "expr ::= NOW",
 /* 224 */ "expr ::= VARIABLE",
 /* 225 */ "expr ::= BOOL",
 /* 226 */ "expr ::= ID LP exprlist RP",
 /* 227 */ "expr ::= ID LP STAR RP",
 /* 228 */ "expr ::= expr IS NULL",
 /* 229 */ "expr ::= expr IS NOT NULL",
 /* 230 */ "expr ::= expr LT expr",
 /* 231 */ "expr ::= expr GT expr",
 /* 232 */ "expr ::= expr LE expr",
 /* 233 */ "expr ::= expr GE expr",
 /* 234 */ "expr ::= expr NE expr",
 /* 235 */ "expr ::= expr EQ expr",
 /* 236 */ "expr ::= expr BETWEEN expr AND expr",
 /* 237 */ "expr ::= expr AND expr",
 /* 238 */ "expr ::= expr OR expr",
 /* 239 */ "expr ::= expr PLUS expr",
 /* 240 */ "expr ::= expr MINUS expr",
 /* 241 */ "expr ::= expr STAR expr",
 /* 242 */ "expr ::= expr SLASH expr",
 /* 243 */ "expr ::= expr REM expr",
 /* 244 */ "expr ::= expr LIKE expr",
 /* 245 */ "expr ::= expr IN LP exprlist RP",
 /* 246 */ "exprlist ::= exprlist COMMA expritem",
 /* 247 */ "exprlist ::= expritem",
 /* 248 */ "expritem ::= expr",
 /* 249 */ "expritem ::=",
 /* 250 */ "cmd ::= RESET QUERY CACHE",
 /* 251 */ "cmd ::= SYNCDB ids REPLICA",
 /* 252 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 253 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 254 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 255 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 258 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 259 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 260 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 261 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 262 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 263 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 264 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 265 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
destroyQuerySqlNode((yypminor->yy272));
}
      break;
    case 237: /* selcollist */
    case 250: /* sclp */
    case 261: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy159));
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
destroyAllSelectClause((yypminor->yy391));
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
  {  189,   -4 }, /* (21) cmd ::= SHOW CREATE DATABASE ids */
  {  189,   -3 }, /* (22) cmd ::= SHOW dbPrefix TABLES */
  {  189,   -5 }, /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  189,   -3 }, /* (24) cmd ::= SHOW dbPrefix STABLES */
  {  189,   -5 }, /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  189,   -3 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS */
  {  189,   -4 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  189,   -5 }, /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
  {  189,   -5 }, /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
  {  189,   -4 }, /* (30) cmd ::= DROP DATABASE ifexists ids */
  {  189,   -4 }, /* (31) cmd ::= DROP TOPIC ifexists ids */
  {  189,   -3 }, /* (32) cmd ::= DROP DNODE ids */
  {  189,   -3 }, /* (33) cmd ::= DROP USER ids */
  {  189,   -3 }, /* (34) cmd ::= DROP ACCOUNT ids */
  {  189,   -2 }, /* (35) cmd ::= USE ids */
  {  189,   -3 }, /* (36) cmd ::= DESCRIBE ids cpxName */
  {  189,   -5 }, /* (37) cmd ::= ALTER USER ids PASS ids */
  {  189,   -5 }, /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  189,   -4 }, /* (39) cmd ::= ALTER DNODE ids ids */
  {  189,   -5 }, /* (40) cmd ::= ALTER DNODE ids ids ids */
  {  189,   -3 }, /* (41) cmd ::= ALTER LOCAL ids */
  {  189,   -4 }, /* (42) cmd ::= ALTER LOCAL ids ids */
  {  189,   -4 }, /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  189,   -4 }, /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  189,   -4 }, /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  189,   -6 }, /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  191,   -1 }, /* (47) ids ::= ID */
  {  191,   -1 }, /* (48) ids ::= STRING */
  {  193,   -2 }, /* (49) ifexists ::= IF EXISTS */
  {  193,    0 }, /* (50) ifexists ::= */
  {  197,   -3 }, /* (51) ifnotexists ::= IF NOT EXISTS */
  {  197,    0 }, /* (52) ifnotexists ::= */
  {  189,   -3 }, /* (53) cmd ::= CREATE DNODE ids */
  {  189,   -6 }, /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  189,   -5 }, /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  189,   -5 }, /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  189,   -5 }, /* (57) cmd ::= CREATE USER ids PASS ids */
  {  200,    0 }, /* (58) pps ::= */
  {  200,   -2 }, /* (59) pps ::= PPS INTEGER */
  {  201,    0 }, /* (60) tseries ::= */
  {  201,   -2 }, /* (61) tseries ::= TSERIES INTEGER */
  {  202,    0 }, /* (62) dbs ::= */
  {  202,   -2 }, /* (63) dbs ::= DBS INTEGER */
  {  203,    0 }, /* (64) streams ::= */
  {  203,   -2 }, /* (65) streams ::= STREAMS INTEGER */
  {  204,    0 }, /* (66) storage ::= */
  {  204,   -2 }, /* (67) storage ::= STORAGE INTEGER */
  {  205,    0 }, /* (68) qtime ::= */
  {  205,   -2 }, /* (69) qtime ::= QTIME INTEGER */
  {  206,    0 }, /* (70) users ::= */
  {  206,   -2 }, /* (71) users ::= USERS INTEGER */
  {  207,    0 }, /* (72) conns ::= */
  {  207,   -2 }, /* (73) conns ::= CONNS INTEGER */
  {  208,    0 }, /* (74) state ::= */
  {  208,   -2 }, /* (75) state ::= STATE ids */
  {  196,   -9 }, /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  209,   -2 }, /* (77) keep ::= KEEP tagitemlist */
  {  211,   -2 }, /* (78) cache ::= CACHE INTEGER */
  {  212,   -2 }, /* (79) replica ::= REPLICA INTEGER */
  {  213,   -2 }, /* (80) quorum ::= QUORUM INTEGER */
  {  214,   -2 }, /* (81) days ::= DAYS INTEGER */
  {  215,   -2 }, /* (82) minrows ::= MINROWS INTEGER */
  {  216,   -2 }, /* (83) maxrows ::= MAXROWS INTEGER */
  {  217,   -2 }, /* (84) blocks ::= BLOCKS INTEGER */
  {  218,   -2 }, /* (85) ctime ::= CTIME INTEGER */
  {  219,   -2 }, /* (86) wal ::= WAL INTEGER */
  {  220,   -2 }, /* (87) fsync ::= FSYNC INTEGER */
  {  221,   -2 }, /* (88) comp ::= COMP INTEGER */
  {  222,   -2 }, /* (89) prec ::= PRECISION STRING */
  {  223,   -2 }, /* (90) update ::= UPDATE INTEGER */
  {  224,   -2 }, /* (91) cachelast ::= CACHELAST INTEGER */
  {  225,   -2 }, /* (92) partitions ::= PARTITIONS INTEGER */
  {  198,    0 }, /* (93) db_optr ::= */
  {  198,   -2 }, /* (94) db_optr ::= db_optr cache */
  {  198,   -2 }, /* (95) db_optr ::= db_optr replica */
  {  198,   -2 }, /* (96) db_optr ::= db_optr quorum */
  {  198,   -2 }, /* (97) db_optr ::= db_optr days */
  {  198,   -2 }, /* (98) db_optr ::= db_optr minrows */
  {  198,   -2 }, /* (99) db_optr ::= db_optr maxrows */
  {  198,   -2 }, /* (100) db_optr ::= db_optr blocks */
  {  198,   -2 }, /* (101) db_optr ::= db_optr ctime */
  {  198,   -2 }, /* (102) db_optr ::= db_optr wal */
  {  198,   -2 }, /* (103) db_optr ::= db_optr fsync */
  {  198,   -2 }, /* (104) db_optr ::= db_optr comp */
  {  198,   -2 }, /* (105) db_optr ::= db_optr prec */
  {  198,   -2 }, /* (106) db_optr ::= db_optr keep */
  {  198,   -2 }, /* (107) db_optr ::= db_optr update */
  {  198,   -2 }, /* (108) db_optr ::= db_optr cachelast */
  {  199,   -1 }, /* (109) topic_optr ::= db_optr */
  {  199,   -2 }, /* (110) topic_optr ::= topic_optr partitions */
  {  194,    0 }, /* (111) alter_db_optr ::= */
  {  194,   -2 }, /* (112) alter_db_optr ::= alter_db_optr replica */
  {  194,   -2 }, /* (113) alter_db_optr ::= alter_db_optr quorum */
  {  194,   -2 }, /* (114) alter_db_optr ::= alter_db_optr keep */
  {  194,   -2 }, /* (115) alter_db_optr ::= alter_db_optr blocks */
  {  194,   -2 }, /* (116) alter_db_optr ::= alter_db_optr comp */
  {  194,   -2 }, /* (117) alter_db_optr ::= alter_db_optr wal */
  {  194,   -2 }, /* (118) alter_db_optr ::= alter_db_optr fsync */
  {  194,   -2 }, /* (119) alter_db_optr ::= alter_db_optr update */
  {  194,   -2 }, /* (120) alter_db_optr ::= alter_db_optr cachelast */
  {  195,   -1 }, /* (121) alter_topic_optr ::= alter_db_optr */
  {  195,   -2 }, /* (122) alter_topic_optr ::= alter_topic_optr partitions */
  {  226,   -1 }, /* (123) typename ::= ids */
  {  226,   -4 }, /* (124) typename ::= ids LP signed RP */
  {  226,   -2 }, /* (125) typename ::= ids UNSIGNED */
  {  227,   -1 }, /* (126) signed ::= INTEGER */
  {  227,   -2 }, /* (127) signed ::= PLUS INTEGER */
  {  227,   -2 }, /* (128) signed ::= MINUS INTEGER */
  {  189,   -3 }, /* (129) cmd ::= CREATE TABLE create_table_args */
  {  189,   -3 }, /* (130) cmd ::= CREATE TABLE create_stable_args */
  {  189,   -3 }, /* (131) cmd ::= CREATE STABLE create_stable_args */
  {  189,   -3 }, /* (132) cmd ::= CREATE TABLE create_table_list */
  {  230,   -1 }, /* (133) create_table_list ::= create_from_stable */
  {  230,   -2 }, /* (134) create_table_list ::= create_table_list create_from_stable */
  {  228,   -6 }, /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  229,  -10 }, /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  231,  -10 }, /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  231,  -13 }, /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  233,   -3 }, /* (139) tagNamelist ::= tagNamelist COMMA ids */
  {  233,   -1 }, /* (140) tagNamelist ::= ids */
  {  228,   -5 }, /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
  {  232,   -3 }, /* (142) columnlist ::= columnlist COMMA column */
  {  232,   -1 }, /* (143) columnlist ::= column */
  {  235,   -2 }, /* (144) column ::= ids typename */
  {  210,   -3 }, /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
  {  210,   -1 }, /* (146) tagitemlist ::= tagitem */
  {  236,   -1 }, /* (147) tagitem ::= INTEGER */
  {  236,   -1 }, /* (148) tagitem ::= FLOAT */
  {  236,   -1 }, /* (149) tagitem ::= STRING */
  {  236,   -1 }, /* (150) tagitem ::= BOOL */
  {  236,   -1 }, /* (151) tagitem ::= NULL */
  {  236,   -2 }, /* (152) tagitem ::= MINUS INTEGER */
  {  236,   -2 }, /* (153) tagitem ::= MINUS FLOAT */
  {  236,   -2 }, /* (154) tagitem ::= PLUS INTEGER */
  {  236,   -2 }, /* (155) tagitem ::= PLUS FLOAT */
  {  234,  -13 }, /* (156) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  234,   -3 }, /* (157) select ::= LP select RP */
  {  249,   -1 }, /* (158) union ::= select */
  {  249,   -4 }, /* (159) union ::= union UNION ALL select */
  {  189,   -1 }, /* (160) cmd ::= union */
  {  234,   -2 }, /* (161) select ::= SELECT selcollist */
  {  250,   -2 }, /* (162) sclp ::= selcollist COMMA */
  {  250,    0 }, /* (163) sclp ::= */
  {  237,   -4 }, /* (164) selcollist ::= sclp distinct expr as */
  {  237,   -2 }, /* (165) selcollist ::= sclp STAR */
  {  253,   -2 }, /* (166) as ::= AS ids */
  {  253,   -1 }, /* (167) as ::= ids */
  {  253,    0 }, /* (168) as ::= */
  {  251,   -1 }, /* (169) distinct ::= DISTINCT */
  {  251,    0 }, /* (170) distinct ::= */
  {  238,   -2 }, /* (171) from ::= FROM tablelist */
  {  238,   -4 }, /* (172) from ::= FROM LP union RP */
  {  254,   -2 }, /* (173) tablelist ::= ids cpxName */
  {  254,   -3 }, /* (174) tablelist ::= ids cpxName ids */
  {  254,   -4 }, /* (175) tablelist ::= tablelist COMMA ids cpxName */
  {  254,   -5 }, /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
  {  255,   -1 }, /* (177) tmvar ::= VARIABLE */
  {  240,   -4 }, /* (178) interval_opt ::= INTERVAL LP tmvar RP */
  {  240,   -6 }, /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  240,    0 }, /* (180) interval_opt ::= */
  {  241,    0 }, /* (181) session_option ::= */
  {  241,   -7 }, /* (182) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  242,    0 }, /* (183) fill_opt ::= */
  {  242,   -6 }, /* (184) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  242,   -4 }, /* (185) fill_opt ::= FILL LP ID RP */
  {  243,   -4 }, /* (186) sliding_opt ::= SLIDING LP tmvar RP */
  {  243,    0 }, /* (187) sliding_opt ::= */
  {  245,    0 }, /* (188) orderby_opt ::= */
  {  245,   -3 }, /* (189) orderby_opt ::= ORDER BY sortlist */
  {  256,   -4 }, /* (190) sortlist ::= sortlist COMMA item sortorder */
  {  256,   -2 }, /* (191) sortlist ::= item sortorder */
  {  258,   -2 }, /* (192) item ::= ids cpxName */
  {  259,   -1 }, /* (193) sortorder ::= ASC */
  {  259,   -1 }, /* (194) sortorder ::= DESC */
  {  259,    0 }, /* (195) sortorder ::= */
  {  244,    0 }, /* (196) groupby_opt ::= */
  {  244,   -3 }, /* (197) groupby_opt ::= GROUP BY grouplist */
  {  260,   -3 }, /* (198) grouplist ::= grouplist COMMA item */
  {  260,   -1 }, /* (199) grouplist ::= item */
  {  246,    0 }, /* (200) having_opt ::= */
  {  246,   -2 }, /* (201) having_opt ::= HAVING expr */
  {  248,    0 }, /* (202) limit_opt ::= */
  {  248,   -2 }, /* (203) limit_opt ::= LIMIT signed */
  {  248,   -4 }, /* (204) limit_opt ::= LIMIT signed OFFSET signed */
  {  248,   -4 }, /* (205) limit_opt ::= LIMIT signed COMMA signed */
  {  247,    0 }, /* (206) slimit_opt ::= */
  {  247,   -2 }, /* (207) slimit_opt ::= SLIMIT signed */
  {  247,   -4 }, /* (208) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  247,   -4 }, /* (209) slimit_opt ::= SLIMIT signed COMMA signed */
  {  239,    0 }, /* (210) where_opt ::= */
  {  239,   -2 }, /* (211) where_opt ::= WHERE expr */
  {  252,   -3 }, /* (212) expr ::= LP expr RP */
  {  252,   -1 }, /* (213) expr ::= ID */
  {  252,   -3 }, /* (214) expr ::= ID DOT ID */
  {  252,   -3 }, /* (215) expr ::= ID DOT STAR */
  {  252,   -1 }, /* (216) expr ::= INTEGER */
  {  252,   -2 }, /* (217) expr ::= MINUS INTEGER */
  {  252,   -2 }, /* (218) expr ::= PLUS INTEGER */
  {  252,   -1 }, /* (219) expr ::= FLOAT */
  {  252,   -2 }, /* (220) expr ::= MINUS FLOAT */
  {  252,   -2 }, /* (221) expr ::= PLUS FLOAT */
  {  252,   -1 }, /* (222) expr ::= STRING */
  {  252,   -1 }, /* (223) expr ::= NOW */
  {  252,   -1 }, /* (224) expr ::= VARIABLE */
  {  252,   -1 }, /* (225) expr ::= BOOL */
  {  252,   -4 }, /* (226) expr ::= ID LP exprlist RP */
  {  252,   -4 }, /* (227) expr ::= ID LP STAR RP */
  {  252,   -3 }, /* (228) expr ::= expr IS NULL */
  {  252,   -4 }, /* (229) expr ::= expr IS NOT NULL */
  {  252,   -3 }, /* (230) expr ::= expr LT expr */
  {  252,   -3 }, /* (231) expr ::= expr GT expr */
  {  252,   -3 }, /* (232) expr ::= expr LE expr */
  {  252,   -3 }, /* (233) expr ::= expr GE expr */
  {  252,   -3 }, /* (234) expr ::= expr NE expr */
  {  252,   -3 }, /* (235) expr ::= expr EQ expr */
  {  252,   -5 }, /* (236) expr ::= expr BETWEEN expr AND expr */
  {  252,   -3 }, /* (237) expr ::= expr AND expr */
  {  252,   -3 }, /* (238) expr ::= expr OR expr */
  {  252,   -3 }, /* (239) expr ::= expr PLUS expr */
  {  252,   -3 }, /* (240) expr ::= expr MINUS expr */
  {  252,   -3 }, /* (241) expr ::= expr STAR expr */
  {  252,   -3 }, /* (242) expr ::= expr SLASH expr */
  {  252,   -3 }, /* (243) expr ::= expr REM expr */
  {  252,   -3 }, /* (244) expr ::= expr LIKE expr */
  {  252,   -5 }, /* (245) expr ::= expr IN LP exprlist RP */
  {  261,   -3 }, /* (246) exprlist ::= exprlist COMMA expritem */
  {  261,   -1 }, /* (247) exprlist ::= expritem */
  {  262,   -1 }, /* (248) expritem ::= expr */
  {  262,    0 }, /* (249) expritem ::= */
  {  189,   -3 }, /* (250) cmd ::= RESET QUERY CACHE */
  {  189,   -3 }, /* (251) cmd ::= SYNCDB ids REPLICA */
  {  189,   -7 }, /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  189,   -7 }, /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  189,   -7 }, /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  189,   -7 }, /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  189,   -8 }, /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  189,   -9 }, /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  189,   -7 }, /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  189,   -7 }, /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  189,   -7 }, /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  189,   -7 }, /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  189,   -8 }, /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  189,   -3 }, /* (263) cmd ::= KILL CONNECTION INTEGER */
  {  189,   -5 }, /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  189,   -5 }, /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 129: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==129);
      case 130: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==131);
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
      case 21: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 29: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 30: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 31: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 32: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 33: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 34: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 35: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 37: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 38: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 41: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 44: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==44);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy322, &t);}
        break;
      case 45: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy351);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy351);}
        break;
      case 47: /* ids ::= ID */
      case 48: /* ids ::= STRING */ yytestcase(yyruleno==48);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 49: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 50: /* ifexists ::= */
      case 52: /* ifnotexists ::= */ yytestcase(yyruleno==52);
      case 170: /* distinct ::= */ yytestcase(yyruleno==170);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 51: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 53: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 54: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy351);}
        break;
      case 55: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 56: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==56);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy322, &yymsp[-2].minor.yy0);}
        break;
      case 57: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 58: /* pps ::= */
      case 60: /* tseries ::= */ yytestcase(yyruleno==60);
      case 62: /* dbs ::= */ yytestcase(yyruleno==62);
      case 64: /* streams ::= */ yytestcase(yyruleno==64);
      case 66: /* storage ::= */ yytestcase(yyruleno==66);
      case 68: /* qtime ::= */ yytestcase(yyruleno==68);
      case 70: /* users ::= */ yytestcase(yyruleno==70);
      case 72: /* conns ::= */ yytestcase(yyruleno==72);
      case 74: /* state ::= */ yytestcase(yyruleno==74);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 59: /* pps ::= PPS INTEGER */
      case 61: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==63);
      case 65: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==69);
      case 71: /* users ::= USERS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* state ::= STATE ids */ yytestcase(yyruleno==75);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 76: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
      case 77: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy159 = yymsp[0].minor.yy159; }
        break;
      case 78: /* cache ::= CACHE INTEGER */
      case 79: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==79);
      case 80: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==80);
      case 81: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==81);
      case 82: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==85);
      case 86: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==86);
      case 87: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==87);
      case 88: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==88);
      case 89: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==89);
      case 90: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==90);
      case 91: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==91);
      case 92: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==92);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 93: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy322); yymsp[1].minor.yy322.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 94: /* db_optr ::= db_optr cache */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 95: /* db_optr ::= db_optr replica */
      case 112: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==112);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 96: /* db_optr ::= db_optr quorum */
      case 113: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==113);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 97: /* db_optr ::= db_optr days */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 98: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 99: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 100: /* db_optr ::= db_optr blocks */
      case 115: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==115);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 101: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 102: /* db_optr ::= db_optr wal */
      case 117: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==117);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 103: /* db_optr ::= db_optr fsync */
      case 118: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==118);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 104: /* db_optr ::= db_optr comp */
      case 116: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==116);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 105: /* db_optr ::= db_optr prec */
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 106: /* db_optr ::= db_optr keep */
      case 114: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==114);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.keep = yymsp[0].minor.yy159; }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 107: /* db_optr ::= db_optr update */
      case 119: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==119);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 108: /* db_optr ::= db_optr cachelast */
      case 120: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==120);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 109: /* topic_optr ::= db_optr */
      case 121: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==121);
{ yylhsminor.yy322 = yymsp[0].minor.yy322; yylhsminor.yy322.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy322 = yylhsminor.yy322;
        break;
      case 110: /* topic_optr ::= topic_optr partitions */
      case 122: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==122);
{ yylhsminor.yy322 = yymsp[-1].minor.yy322; yylhsminor.yy322.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 111: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy322); yymsp[1].minor.yy322.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 123: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy407, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy407 = yylhsminor.yy407;
        break;
      case 124: /* typename ::= ids LP signed RP */
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
      case 125: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy407, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy407 = yylhsminor.yy407;
        break;
      case 126: /* signed ::= INTEGER */
{ yylhsminor.yy317 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy317 = yylhsminor.yy317;
        break;
      case 127: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy317 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 128: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy317 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 132: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy14;}
        break;
      case 133: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy206);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy14 = pCreateTable;
}
  yymsp[0].minor.yy14 = yylhsminor.yy14;
        break;
      case 134: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy14->childTableInfo, &yymsp[0].minor.yy206);
  yylhsminor.yy14 = yymsp[-1].minor.yy14;
}
  yymsp[-1].minor.yy14 = yylhsminor.yy14;
        break;
      case 135: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy14 = tSetCreateTableInfo(yymsp[-1].minor.yy159, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy14, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy14 = yylhsminor.yy14;
        break;
      case 136: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy14 = tSetCreateTableInfo(yymsp[-5].minor.yy159, yymsp[-1].minor.yy159, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy14, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy14 = yylhsminor.yy14;
        break;
      case 137: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy206 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy159, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy206 = yylhsminor.yy206;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy206 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy159, yymsp[-1].minor.yy159, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy206 = yylhsminor.yy206;
        break;
      case 139: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy159, &yymsp[0].minor.yy0); yylhsminor.yy159 = yymsp[-2].minor.yy159;  }
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 140: /* tagNamelist ::= ids */
{yylhsminor.yy159 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy159, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy14 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy272, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy14, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy14 = yylhsminor.yy14;
        break;
      case 142: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy159, &yymsp[0].minor.yy407); yylhsminor.yy159 = yymsp[-2].minor.yy159;  }
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 143: /* columnlist ::= column */
{yylhsminor.yy159 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy159, &yymsp[0].minor.yy407);}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 144: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy407, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy407);
}
  yymsp[-1].minor.yy407 = yylhsminor.yy407;
        break;
      case 145: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy159 = tVariantListAppend(yymsp[-2].minor.yy159, &yymsp[0].minor.yy488, -1);    }
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 146: /* tagitemlist ::= tagitem */
{ yylhsminor.yy159 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1); }
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 147: /* tagitem ::= INTEGER */
      case 148: /* tagitem ::= FLOAT */ yytestcase(yyruleno==148);
      case 149: /* tagitem ::= STRING */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= BOOL */ yytestcase(yyruleno==150);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 151: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 152: /* tagitem ::= MINUS INTEGER */
      case 153: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==155);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 156: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy272 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy159, yymsp[-10].minor.yy514, yymsp[-9].minor.yy118, yymsp[-4].minor.yy159, yymsp[-3].minor.yy159, &yymsp[-8].minor.yy184, &yymsp[-7].minor.yy249, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy159, &yymsp[0].minor.yy440, &yymsp[-1].minor.yy440);
}
  yymsp[-12].minor.yy272 = yylhsminor.yy272;
        break;
      case 157: /* select ::= LP select RP */
{yymsp[-2].minor.yy272 = yymsp[-1].minor.yy272;}
        break;
      case 158: /* union ::= select */
{ yylhsminor.yy391 = setSubclause(NULL, yymsp[0].minor.yy272); }
  yymsp[0].minor.yy391 = yylhsminor.yy391;
        break;
      case 159: /* union ::= union UNION ALL select */
{ yylhsminor.yy391 = appendSelectClause(yymsp[-3].minor.yy391, yymsp[0].minor.yy272); }
  yymsp[-3].minor.yy391 = yylhsminor.yy391;
        break;
      case 160: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy391, NULL, TSDB_SQL_SELECT); }
        break;
      case 161: /* select ::= SELECT selcollist */
{
  yylhsminor.yy272 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy159, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 162: /* sclp ::= selcollist COMMA */
{yylhsminor.yy159 = yymsp[-1].minor.yy159;}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 163: /* sclp ::= */
      case 188: /* orderby_opt ::= */ yytestcase(yyruleno==188);
{yymsp[1].minor.yy159 = 0;}
        break;
      case 164: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy159 = tSqlExprListAppend(yymsp[-3].minor.yy159, yymsp[-1].minor.yy118,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy159 = yylhsminor.yy159;
        break;
      case 165: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy159 = tSqlExprListAppend(yymsp[-1].minor.yy159, pNode, 0, 0);
}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 166: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 167: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 168: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 169: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 171: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy514 = yymsp[0].minor.yy159;}
        break;
      case 172: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy514 = yymsp[-1].minor.yy391;}
        break;
      case 173: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy159 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 174: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy159 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 175: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy159 = setTableNameList(yymsp[-3].minor.yy159, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy159 = yylhsminor.yy159;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;

  yylhsminor.yy159 = setTableNameList(yymsp[-4].minor.yy159, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy159 = yylhsminor.yy159;
        break;
      case 177: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy184.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy184.offset.n = 0;}
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy184.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy184.offset = yymsp[-1].minor.yy0;}
        break;
      case 180: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy184, 0, sizeof(yymsp[1].minor.yy184));}
        break;
      case 181: /* session_option ::= */
{yymsp[1].minor.yy249.col.n = 0; yymsp[1].minor.yy249.gap.n = 0;}
        break;
      case 182: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy249.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy249.gap = yymsp[-1].minor.yy0;
}
        break;
      case 183: /* fill_opt ::= */
{ yymsp[1].minor.yy159 = 0;     }
        break;
      case 184: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy159, &A, -1, 0);
    yymsp[-5].minor.yy159 = yymsp[-1].minor.yy159;
}
        break;
      case 185: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy159 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 186: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 187: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 189: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy159 = yymsp[0].minor.yy159;}
        break;
      case 190: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy159 = tVariantListAppend(yymsp[-3].minor.yy159, &yymsp[-1].minor.yy488, yymsp[0].minor.yy20);
}
  yymsp[-3].minor.yy159 = yylhsminor.yy159;
        break;
      case 191: /* sortlist ::= item sortorder */
{
  yylhsminor.yy159 = tVariantListAppend(NULL, &yymsp[-1].minor.yy488, yymsp[0].minor.yy20);
}
  yymsp[-1].minor.yy159 = yylhsminor.yy159;
        break;
      case 192: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 193: /* sortorder ::= ASC */
{ yymsp[0].minor.yy20 = TSDB_ORDER_ASC; }
        break;
      case 194: /* sortorder ::= DESC */
{ yymsp[0].minor.yy20 = TSDB_ORDER_DESC;}
        break;
      case 195: /* sortorder ::= */
{ yymsp[1].minor.yy20 = TSDB_ORDER_ASC; }
        break;
      case 196: /* groupby_opt ::= */
{ yymsp[1].minor.yy159 = 0;}
        break;
      case 197: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy159 = yymsp[0].minor.yy159;}
        break;
      case 198: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy159 = tVariantListAppend(yymsp[-2].minor.yy159, &yymsp[0].minor.yy488, -1);
}
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 199: /* grouplist ::= item */
{
  yylhsminor.yy159 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1);
}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 200: /* having_opt ::= */
      case 210: /* where_opt ::= */ yytestcase(yyruleno==210);
      case 249: /* expritem ::= */ yytestcase(yyruleno==249);
{yymsp[1].minor.yy118 = 0;}
        break;
      case 201: /* having_opt ::= HAVING expr */
      case 211: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==211);
{yymsp[-1].minor.yy118 = yymsp[0].minor.yy118;}
        break;
      case 202: /* limit_opt ::= */
      case 206: /* slimit_opt ::= */ yytestcase(yyruleno==206);
{yymsp[1].minor.yy440.limit = -1; yymsp[1].minor.yy440.offset = 0;}
        break;
      case 203: /* limit_opt ::= LIMIT signed */
      case 207: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==207);
{yymsp[-1].minor.yy440.limit = yymsp[0].minor.yy317;  yymsp[-1].minor.yy440.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy440.limit = yymsp[-2].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[0].minor.yy317;}
        break;
      case 205: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy440.limit = yymsp[0].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[-2].minor.yy317;}
        break;
      case 208: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy440.limit = yymsp[-2].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[0].minor.yy317;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy440.limit = yymsp[0].minor.yy317;  yymsp[-3].minor.yy440.offset = yymsp[-2].minor.yy317;}
        break;
      case 212: /* expr ::= LP expr RP */
{yylhsminor.yy118 = yymsp[-1].minor.yy118; yylhsminor.yy118->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy118->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 213: /* expr ::= ID */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 214: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 215: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 216: /* expr ::= INTEGER */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 217: /* expr ::= MINUS INTEGER */
      case 218: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==218);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy118 = yylhsminor.yy118;
        break;
      case 219: /* expr ::= FLOAT */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 220: /* expr ::= MINUS FLOAT */
      case 221: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==221);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy118 = yylhsminor.yy118;
        break;
      case 222: /* expr ::= STRING */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 223: /* expr ::= NOW */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 224: /* expr ::= VARIABLE */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 225: /* expr ::= BOOL */
{ yylhsminor.yy118 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 226: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy118 = tSqlExprCreateFunction(yymsp[-1].minor.yy159, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy118 = yylhsminor.yy118;
        break;
      case 227: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy118 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy118 = yylhsminor.yy118;
        break;
      case 228: /* expr ::= expr IS NULL */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 229: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-3].minor.yy118, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy118 = yylhsminor.yy118;
        break;
      case 230: /* expr ::= expr LT expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_LT);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 231: /* expr ::= expr GT expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_GT);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 232: /* expr ::= expr LE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_LE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 233: /* expr ::= expr GE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_GE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 234: /* expr ::= expr NE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_NE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 235: /* expr ::= expr EQ expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_EQ);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 236: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy118); yylhsminor.yy118 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy118, yymsp[-2].minor.yy118, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy118, TK_LE), TK_AND);}
  yymsp[-4].minor.yy118 = yylhsminor.yy118;
        break;
      case 237: /* expr ::= expr AND expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_AND);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 238: /* expr ::= expr OR expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_OR); }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 239: /* expr ::= expr PLUS expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_PLUS);  }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 240: /* expr ::= expr MINUS expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_MINUS); }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 241: /* expr ::= expr STAR expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_STAR);  }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 242: /* expr ::= expr SLASH expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_DIVIDE);}
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 243: /* expr ::= expr REM expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_REM);   }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 244: /* expr ::= expr LIKE expr */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-2].minor.yy118, yymsp[0].minor.yy118, TK_LIKE);  }
  yymsp[-2].minor.yy118 = yylhsminor.yy118;
        break;
      case 245: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy118 = tSqlExprCreate(yymsp[-4].minor.yy118, (tSqlExpr*)yymsp[-1].minor.yy159, TK_IN); }
  yymsp[-4].minor.yy118 = yylhsminor.yy118;
        break;
      case 246: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy159 = tSqlExprListAppend(yymsp[-2].minor.yy159,yymsp[0].minor.yy118,0, 0);}
  yymsp[-2].minor.yy159 = yylhsminor.yy159;
        break;
      case 247: /* exprlist ::= expritem */
{yylhsminor.yy159 = tSqlExprListAppend(0,yymsp[0].minor.yy118,0, 0);}
  yymsp[0].minor.yy159 = yylhsminor.yy159;
        break;
      case 248: /* expritem ::= expr */
{yylhsminor.yy118 = yymsp[0].minor.yy118;}
  yymsp[0].minor.yy118 = yylhsminor.yy118;
        break;
      case 250: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 251: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 252: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 253: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 254: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 257: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy159, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 263: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 264: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 265: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
