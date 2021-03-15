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
#define YYNOCODE 290
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tSQLExpr* yy70;
  SArray* yy161;
  TAOS_FIELD yy223;
  SCreateAcctInfo yy231;
  SSubclauseInfo* yy233;
  SIntervalVal yy300;
  SCreateDbInfo yy302;
  SCreatedTableInfo yy356;
  int64_t yy369;
  SLimitVal yy394;
  int yy452;
  tSQLExprList* yy458;
  SCreateTableSQL* yy518;
  tVariant yy526;
  SQuerySQL* yy544;
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
#define YYNSTATE             312
#define YYNRULE              266
#define YYNRULE_WITH_ACTION  266
#define YYNTOKEN             216
#define YY_MAX_SHIFT         311
#define YY_MIN_SHIFTREDUCE   503
#define YY_MAX_SHIFTREDUCE   768
#define YY_ERROR_ACTION      769
#define YY_ACCEPT_ACTION     770
#define YY_NO_ACTION         771
#define YY_MIN_REDUCE        772
#define YY_MAX_REDUCE        1037
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
#define YY_ACTTAB_COUNT (667)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    78,  552,  943,  552,  201,  309,    1,  153,   17,  553,
 /*    10 */    79,  553,   72,   48,   49,  205,   52,   53,  139,  180,
 /*    20 */   213,   42,  180,   51,  257,   56,   54,   58,   55, 1019,
 /*    30 */    31,  208, 1020,   47,   46,  134,  180,   45,   44,   43,
 /*    40 */   921,  904,  905,   29,  908,  207, 1020,  504,  505,  506,
 /*    50 */   507,  508,  509,  510,  511,  512,  513,  514,  515,  516,
 /*    60 */   517,  310,  218,  940,  230,   48,   49,  932,   52,   53,
 /*    70 */   139,  203,  213,   42,  918,   51,  257,   56,   54,   58,
 /*    80 */    55,  202,  254,   71,   76,   47,   46,  921,  275,   45,
 /*    90 */    44,   43,   48,   49,   82,   52,   53,  245,  295,  213,
 /*   100 */    42,  139,   51,  257,   56,   54,   58,   55,  770,  311,
 /*   110 */     3,  166,   47,   46,   24,  225,   45,   44,   43,   48,
 /*   120 */    50,   37,   52,   53,  831,  672,  213,   42,  909,   51,
 /*   130 */   257,   56,   54,   58,   55,  632,  972,  220,  252,   47,
 /*   140 */    46,  669,  552,   45,   44,   43,   49,   25,   52,   53,
 /*   150 */   553,  178,  213,   42,  932,   51,  257,   56,   54,   58,
 /*   160 */    55,  716,  921,  285,  284,   47,   46,  971,  240,   45,
 /*   170 */    44,   43,   23,  273,  304,  303,  272,  271,  270,  302,
 /*   180 */   269,  301,  300,  299,  268,  298,  297,  881,  915,  869,
 /*   190 */   870,  871,  872,  873,  874,  875,  876,  877,  878,  879,
 /*   200 */   880,  882,  883,   52,   53,   18,  676,  213,   42,  233,
 /*   210 */    51,  257,   56,   54,   58,   55,  237,  236,  697,  698,
 /*   220 */    47,   46,  718,  188,   45,   44,   43,  212,  729,  190,
 /*   230 */   219,  720,  305,  723,  819,  726,  117,  116,  189,  165,
 /*   240 */   212,  729,   31,   31,  720,  104,  723,  224,  726,   45,
 /*   250 */    44,   43,  295,   56,   54,   58,   55,  921,  719,  209,
 /*   260 */   210,   47,   46,  256,   31,   45,   44,   43,   23,   24,
 /*   270 */   304,  303,  209,  210,  184,  302,   37,  301,  300,  299,
 /*   280 */   828,  298,  297,  216,  217,  165,  918,  918,  889,   47,
 /*   290 */    46,  887,  888,   45,   44,   43,  890,  239,  892,  893,
 /*   300 */   891,  907,  894,  895,  196,    5,  155,  258,  917,   31,
 /*   310 */  1016,   34,  154,   86,   91,   84,   90,  226,  102,  107,
 /*   320 */   282,  281,   31,   31,   96,  106,  149,  112,  115,  105,
 /*   330 */   173,  169,   31,  139,   57,  109,  171,  168,  121,  120,
 /*   340 */   119,  118,  222,  308,  307,  126,  728,   57,  225,  656,
 /*   350 */   278,   12,  653,  918,  654,   81,  655,  919,   70,  728,
 /*   360 */   820,  727,  132,  279,  283,  165,  918,  918,  642,   37,
 /*   370 */   664,  243,   32,  287,  727,   32,  918,  242,  211,  684,
 /*   380 */   227,  228,  275,  136,   62,  688,   95,   94,  689,   61,
 /*   390 */   906,  749,   20,  730,  722,   19,  725,   19,  721,   77,
 /*   400 */   724,   65, 1015,  260,   63,  644,  732,   32,   28,   61,
 /*   410 */  1014,  263,  223,  262,  643,  277,  197,   80,   61,   66,
 /*   420 */    14,   13,  101,  100,   68,    6,  631,  198,   16,   15,
 /*   430 */   660,  658,  661,  659,  114,  113,  131,  129,  182,  183,
 /*   440 */   982,  185,  179,  186,  187,  193, 1029,  194,  192,  177,
 /*   450 */   191,  181,  920,  981,  214,  978,  977,  215,  286,  934,
 /*   460 */   133,  657,   40,  942,  949,  951,  964,  135,  150,  963,
 /*   470 */   148,  914,  151,   37,  130,  241,  152,  832,  246,  265,
 /*   480 */   683,   67,  266,  267,   38,  175,  931,   35,  276,  204,
 /*   490 */   250,  827, 1034,   64,   92,   59, 1033,  140,  141, 1031,
 /*   500 */   255,  142,  253,  143,  156,  280, 1028,  251,  144,   98,
 /*   510 */  1027,  247, 1025,  249,  157,   41,  850,   36,   33,  145,
 /*   520 */   103,   39,  176,  296,  288,  816,  108,  814,  110,  111,
 /*   530 */   812,  811,  229,  167,  809,  289,  290,  808,  807,  806,
 /*   540 */   805,  804,  803,  170,  172,  800,  798,  796,  794,  792,
 /*   550 */   174,  291,  244,   73,   74,  965,  292,  293,  294,  199,
 /*   560 */   221,  264,  306,  768,  231,  200,  195,  232,   87,   88,
 /*   570 */   767,  235,  766,  234,  238,  754,  242,  666,   69,  810,
 /*   580 */    75,  259,    8,  160,  159,  851,  158,  161,  163,  162,
 /*   590 */   122,  123,  164,  124,    2,  802,  885,  801,  793,  916,
 /*   600 */   125,  685,    4,  137,  146,  147,  206,  248,  897,  690,
 /*   610 */   138,   26,    9,   10,  731,    7,   27,   11,  733,   21,
 /*   620 */    22,  261,   83,   30,   85,   81,  595,  591,  589,  588,
 /*   630 */   587,  584,  556,  274,   89,   32,   93,   60,  634,  633,
 /*   640 */   630,  579,  577,  569,  575,  571,   97,  573,   99,  567,
 /*   650 */   565,  598,  597,  596,  594,  593,  592,  590,  586,  585,
 /*   660 */    61,  554,  127,  521,  128,  772,  519,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   265,    1,  219,    1,  218,  219,  226,  227,  279,    9,
 /*    10 */   225,    9,  277,   13,   14,  239,   16,   17,  219,  279,
 /*    20 */    20,   21,  279,   23,   24,   25,   26,   27,   28,  289,
 /*    30 */   219,  288,  289,   33,   34,  219,  279,   37,   38,   39,
 /*    40 */   264,  256,  257,  258,  259,  288,  289,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  239,  280,   62,   13,   14,  262,   16,   17,
 /*    70 */   219,  260,   20,   21,  263,   23,   24,   25,   26,   27,
 /*    80 */    28,  276,  283,   83,  285,   33,   34,  264,   81,   37,
 /*    90 */    38,   39,   13,   14,  225,   16,   17,  281,   85,   20,
 /*   100 */    21,  219,   23,   24,   25,   26,   27,   28,  216,  217,
 /*   110 */   222,  223,   33,   34,  108,  219,   37,   38,   39,   13,
 /*   120 */    14,  115,   16,   17,  228,   37,   20,   21,  259,   23,
 /*   130 */    24,   25,   26,   27,   28,    5,  285,  239,  287,   33,
 /*   140 */    34,  113,    1,   37,   38,   39,   14,  119,   16,   17,
 /*   150 */     9,  279,   20,   21,  262,   23,   24,   25,   26,   27,
 /*   160 */    28,  109,  264,   33,   34,   33,   34,  285,  276,   37,
 /*   170 */    38,   39,   92,   93,   94,   95,   96,   97,   98,   99,
 /*   180 */   100,  101,  102,  103,  104,  105,  106,  238,  219,  240,
 /*   190 */   241,  242,  243,  244,  245,  246,  247,  248,  249,  250,
 /*   200 */   251,  252,  253,   16,   17,   44,  118,   20,   21,  137,
 /*   210 */    23,   24,   25,   26,   27,   28,  144,  145,  126,  127,
 /*   220 */    33,   34,    1,   62,   37,   38,   39,    1,    2,   68,
 /*   230 */   261,    5,  239,    7,  224,    9,   75,   76,   77,  229,
 /*   240 */     1,    2,  219,  219,    5,   78,    7,   68,    9,   37,
 /*   250 */    38,   39,   85,   25,   26,   27,   28,  264,   37,   33,
 /*   260 */    34,   33,   34,   37,  219,   37,   38,   39,   92,  108,
 /*   270 */    94,   95,   33,   34,  279,   99,  115,  101,  102,  103,
 /*   280 */   224,  105,  106,  260,  260,  229,  263,  263,  238,   33,
 /*   290 */    34,  241,  242,   37,   38,   39,  246,  136,  248,  249,
 /*   300 */   250,    0,  252,  253,  143,   63,   64,   15,  263,  219,
 /*   310 */   279,   69,   70,   71,   72,   73,   74,  138,   63,   64,
 /*   320 */   141,  142,  219,  219,   69,   70,   83,   72,   73,   74,
 /*   330 */    63,   64,  219,  219,  108,   80,   69,   70,   71,   72,
 /*   340 */    73,   74,   68,   65,   66,   67,  120,  108,  219,    2,
 /*   350 */   260,  108,    5,  263,    7,  112,    9,  228,  225,  120,
 /*   360 */   224,  135,  108,  260,  260,  229,  263,  263,  109,  115,
 /*   370 */   109,  109,  113,  260,  135,  113,  263,  116,   61,  109,
 /*   380 */    33,   34,   81,  113,  113,  109,  139,  140,  109,  113,
 /*   390 */   257,  109,  113,  109,    5,  113,    7,  113,    5,  285,
 /*   400 */     7,  113,  279,  109,  133,  109,  114,  113,  108,  113,
 /*   410 */   279,  111,  138,  109,  109,  141,  279,  113,  113,  131,
 /*   420 */   139,  140,  139,  140,  108,  108,  110,  279,  139,  140,
 /*   430 */     5,    5,    7,    7,   78,   79,   63,   64,  279,  279,
 /*   440 */   255,  279,  279,  279,  279,  279,  264,  279,  279,  279,
 /*   450 */   279,  279,  264,  255,  255,  255,  255,  255,  255,  262,
 /*   460 */   219,  114,  278,  219,  219,  219,  286,  219,  219,  286,
 /*   470 */   266,  219,  219,  115,   61,  262,  219,  219,  282,  219,
 /*   480 */   120,  130,  219,  219,  219,  219,  275,  219,  219,  282,
 /*   490 */   282,  219,  219,  132,  219,  129,  219,  274,  273,  219,
 /*   500 */   124,  272,  128,  271,  219,  219,  219,  123,  270,  219,
 /*   510 */   219,  121,  219,  122,  219,  134,  219,  219,  219,  269,
 /*   520 */    91,  219,  219,  107,   90,  219,  219,  219,  219,  219,
 /*   530 */   219,  219,  219,  219,  219,   51,   87,  219,  219,  219,
 /*   540 */   219,  219,  219,  219,  219,  219,  219,  219,  219,  219,
 /*   550 */   219,   89,  220,  220,  220,  220,   55,   88,   86,  220,
 /*   560 */   220,  220,   81,    5,  146,  220,  220,    5,  225,  225,
 /*   570 */     5,    5,    5,  146,  137,   93,  116,  109,  117,  220,
 /*   580 */   113,  111,  108,  231,  235,  237,  236,  234,  233,  232,
 /*   590 */   221,  221,  230,  221,  226,  220,  254,  220,  220,  262,
 /*   600 */   221,  109,  222,  108,  268,  267,    1,  108,  254,  109,
 /*   610 */   108,  113,  125,  125,  109,  108,  113,  108,  114,  108,
 /*   620 */   108,  111,   78,   84,   83,  112,    9,    5,    5,    5,
 /*   630 */     5,    5,   82,   15,   78,  113,  140,   16,    5,    5,
 /*   640 */   109,    5,    5,    5,    5,    5,  140,    5,  140,    5,
 /*   650 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   660 */   113,   82,   21,   61,   21,    0,   60,  290,  290,  290,
 /*   670 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   680 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   690 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   700 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   710 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   720 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   730 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   740 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   750 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   760 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   770 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   780 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   790 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   800 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   810 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   820 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   830 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   840 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   850 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   860 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   870 */   290,  290,  290,  290,  290,  290,  290,  290,  290,  290,
 /*   880 */   290,  290,  290,
};
#define YY_SHIFT_COUNT    (311)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (665)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   80,   80,  176,  176,    7,  226,  239,  141,  141,
 /*    10 */   141,  141,  141,  141,  141,  141,  141,    0,    2,  239,
 /*    20 */   347,  347,  347,  347,    6,  141,  141,  141,  141,  301,
 /*    30 */   141,  141,  141,  167,    7,   13,   13,  667,  667,  667,
 /*    40 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   347,  347,  130,  130,  130,  130,  130,  130,  130,  254,
 /*    70 */   141,  141,   88,  141,  141,  141,   92,   92,   28,  141,
 /*    80 */   141,  141,  141,  141,  141,  141,  141,  141,  141,  141,
 /*    90 */   141,  141,  141,  141,  141,  141,  141,  141,  141,  141,
 /*   100 */   141,  141,  141,  141,  141,  141,  141,  141,  141,  141,
 /*   110 */   141,  141,  141,  141,  141,  141,  141,  141,  141,  141,
 /*   120 */   141,  141,  141,  141,  141,  141,  141,  141,  141,  141,
 /*   130 */   141,  141,  358,  413,  413,  413,  360,  360,  360,  413,
 /*   140 */   351,  361,  366,  376,  374,  384,  391,  390,  381,  358,
 /*   150 */   413,  413,  413,  416,    7,    7,  413,  413,  429,  434,
 /*   160 */   484,  449,  462,  501,  469,  472,  416,  413,  481,  481,
 /*   170 */   413,  481,  413,  481,  413,  667,  667,   52,   79,  106,
 /*   180 */    79,   79,  132,  187,  228,  228,  228,  228,  242,  255,
 /*   190 */   267,  256,  256,  256,  256,  179,   72,  212,  212,  243,
 /*   200 */   274,  278,  261,  262,  270,  276,  279,  282,  284,  389,
 /*   210 */   393,  221,  317,  292,  271,  288,  259,  294,  296,  304,
 /*   220 */   305,  300,  247,  281,  283,  316,  289,  425,  426,  356,
 /*   230 */   373,  558,  418,  562,  565,  427,  566,  567,  482,  437,
 /*   240 */   460,  468,  461,  470,  474,  467,  492,  495,  605,  499,
 /*   250 */   500,  502,  498,  487,  503,  488,  505,  507,  504,  509,
 /*   260 */   470,  511,  510,  512,  513,  544,  539,  541,  617,  622,
 /*   270 */   623,  624,  625,  626,  550,  618,  556,  496,  522,  522,
 /*   280 */   621,  506,  508,  522,  633,  634,  531,  522,  636,  637,
 /*   290 */   638,  639,  640,  642,  644,  645,  646,  647,  648,  649,
 /*   300 */   650,  651,  652,  653,  654,  547,  579,  641,  643,  602,
 /*   310 */   606,  665,
};
#define YY_REDUCE_COUNT (176)
#define YY_REDUCE_MIN   (-271)
#define YY_REDUCE_MAX   (380)
static const short yy_reduce_ofst[] = {
 /*     0 */  -108,  -51,  -51,   50,   50, -215, -257, -243, -189, -149,
 /*    10 */  -201,   23,   24,   90,  103,  104,  113, -217, -214, -260,
 /*    20 */  -224, -177, -102,   -7, -195, -184, -118,  114,  -31, -131,
 /*    30 */  -104,  129,   45,   10,  133,   56,  136, -265, -220, -112,
 /*    40 */  -271, -128,   -5,   31,  123,  131,  137,  148,  159,  160,
 /*    50 */   162,  163,  164,  165,  166,  168,  169,  170,  171,  172,
 /*    60 */   182,  188,  185,  198,  199,  200,  201,  202,  203,  197,
 /*    70 */   241,  244,  184,  245,  246,  248,  180,  183,  204,  249,
 /*    80 */   252,  253,  257,  258,  260,  263,  264,  265,  266,  268,
 /*    90 */   269,  272,  273,  275,  277,  280,  285,  286,  287,  290,
 /*   100 */   291,  293,  295,  297,  298,  299,  302,  303,  306,  307,
 /*   110 */   308,  309,  310,  311,  312,  313,  314,  315,  318,  319,
 /*   120 */   320,  321,  322,  323,  324,  325,  326,  327,  328,  329,
 /*   130 */   330,  331,  213,  332,  333,  334,  196,  207,  208,  335,
 /*   140 */   211,  223,  225,  229,  232,  238,  250,  336,  338,  337,
 /*   150 */   339,  340,  341,  342,  343,  344,  345,  346,  348,  350,
 /*   160 */   349,  352,  353,  357,  355,  362,  354,  359,  369,  370,
 /*   170 */   375,  372,  377,  379,  378,  368,  380,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   769,  884,  829,  896,  817,  826, 1022, 1022,  769,  769,
 /*    10 */   769,  769,  769,  769,  769,  769,  769,  944,  789, 1022,
 /*    20 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  826,
 /*    30 */   769,  769,  769,  833,  826,  833,  833,  939,  868,  886,
 /*    40 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*    50 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*    60 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*    70 */   769,  769,  946,  948,  950,  769,  968,  968,  937,  769,
 /*    80 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*    90 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*   100 */   769,  769,  769,  769,  769,  769,  769,  769,  815,  769,
 /*   110 */   813,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*   120 */   769,  769,  769,  769,  769,  769,  799,  769,  769,  769,
 /*   130 */   769,  769,  769,  791,  791,  791,  769,  769,  769,  791,
 /*   140 */   975,  979,  973,  961,  969,  960,  956,  955,  983,  769,
 /*   150 */   791,  791,  791,  830,  826,  826,  791,  791,  849,  847,
 /*   160 */   845,  837,  843,  839,  841,  835,  818,  791,  824,  824,
 /*   170 */   791,  824,  791,  824,  791,  868,  886,  769,  984,  769,
 /*   180 */  1021,  974, 1011, 1010, 1017, 1009, 1008, 1007,  769,  769,
 /*   190 */   769, 1003, 1004, 1006, 1005,  769,  769, 1013, 1012,  769,
 /*   200 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*   210 */   769,  769,  986,  769,  980,  976,  769,  769,  769,  769,
 /*   220 */   769,  769,  769,  769,  769,  898,  769,  769,  769,  769,
 /*   230 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*   240 */   936,  769,  769,  769,  769,  947,  769,  769,  769,  769,
 /*   250 */   769,  769,  970,  769,  962,  769,  769,  769,  769,  769,
 /*   260 */   910,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*   270 */   769,  769,  769,  769,  769,  769,  769,  769, 1032, 1030,
 /*   280 */   769,  769,  769, 1026,  769,  769,  769, 1024,  769,  769,
 /*   290 */   769,  769,  769,  769,  769,  769,  769,  769,  769,  769,
 /*   300 */   769,  769,  769,  769,  769,  852,  769,  797,  795,  769,
 /*   310 */   787,  769,
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
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    1,  /*     STABLE => ID */
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
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "STABLE",
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
  /*   85 */ "PPS",
  /*   86 */ "TSERIES",
  /*   87 */ "DBS",
  /*   88 */ "STORAGE",
  /*   89 */ "QTIME",
  /*   90 */ "CONNS",
  /*   91 */ "STATE",
  /*   92 */ "KEEP",
  /*   93 */ "CACHE",
  /*   94 */ "REPLICA",
  /*   95 */ "QUORUM",
  /*   96 */ "DAYS",
  /*   97 */ "MINROWS",
  /*   98 */ "MAXROWS",
  /*   99 */ "BLOCKS",
  /*  100 */ "CTIME",
  /*  101 */ "WAL",
  /*  102 */ "FSYNC",
  /*  103 */ "COMP",
  /*  104 */ "PRECISION",
  /*  105 */ "UPDATE",
  /*  106 */ "CACHELAST",
  /*  107 */ "PARTITIONS",
  /*  108 */ "LP",
  /*  109 */ "RP",
  /*  110 */ "UNSIGNED",
  /*  111 */ "TAGS",
  /*  112 */ "USING",
  /*  113 */ "COMMA",
  /*  114 */ "NULL",
  /*  115 */ "SELECT",
  /*  116 */ "UNION",
  /*  117 */ "ALL",
  /*  118 */ "DISTINCT",
  /*  119 */ "FROM",
  /*  120 */ "VARIABLE",
  /*  121 */ "INTERVAL",
  /*  122 */ "FILL",
  /*  123 */ "SLIDING",
  /*  124 */ "ORDER",
  /*  125 */ "BY",
  /*  126 */ "ASC",
  /*  127 */ "DESC",
  /*  128 */ "GROUP",
  /*  129 */ "HAVING",
  /*  130 */ "LIMIT",
  /*  131 */ "OFFSET",
  /*  132 */ "SLIMIT",
  /*  133 */ "SOFFSET",
  /*  134 */ "WHERE",
  /*  135 */ "NOW",
  /*  136 */ "RESET",
  /*  137 */ "QUERY",
  /*  138 */ "ADD",
  /*  139 */ "COLUMN",
  /*  140 */ "TAG",
  /*  141 */ "CHANGE",
  /*  142 */ "SET",
  /*  143 */ "KILL",
  /*  144 */ "CONNECTION",
  /*  145 */ "STREAM",
  /*  146 */ "COLON",
  /*  147 */ "ABORT",
  /*  148 */ "AFTER",
  /*  149 */ "ATTACH",
  /*  150 */ "BEFORE",
  /*  151 */ "BEGIN",
  /*  152 */ "CASCADE",
  /*  153 */ "CLUSTER",
  /*  154 */ "CONFLICT",
  /*  155 */ "COPY",
  /*  156 */ "DEFERRED",
  /*  157 */ "DELIMITERS",
  /*  158 */ "DETACH",
  /*  159 */ "EACH",
  /*  160 */ "END",
  /*  161 */ "EXPLAIN",
  /*  162 */ "FAIL",
  /*  163 */ "FOR",
  /*  164 */ "IGNORE",
  /*  165 */ "IMMEDIATE",
  /*  166 */ "INITIALLY",
  /*  167 */ "INSTEAD",
  /*  168 */ "MATCH",
  /*  169 */ "KEY",
  /*  170 */ "OF",
  /*  171 */ "RAISE",
  /*  172 */ "REPLACE",
  /*  173 */ "RESTRICT",
  /*  174 */ "ROW",
  /*  175 */ "STATEMENT",
  /*  176 */ "TRIGGER",
  /*  177 */ "VIEW",
  /*  178 */ "COUNT",
  /*  179 */ "SUM",
  /*  180 */ "AVG",
  /*  181 */ "MIN",
  /*  182 */ "MAX",
  /*  183 */ "FIRST",
  /*  184 */ "LAST",
  /*  185 */ "TOP",
  /*  186 */ "BOTTOM",
  /*  187 */ "STDDEV",
  /*  188 */ "PERCENTILE",
  /*  189 */ "APERCENTILE",
  /*  190 */ "LEASTSQUARES",
  /*  191 */ "HISTOGRAM",
  /*  192 */ "DIFF",
  /*  193 */ "SPREAD",
  /*  194 */ "TWA",
  /*  195 */ "INTERP",
  /*  196 */ "LAST_ROW",
  /*  197 */ "RATE",
  /*  198 */ "IRATE",
  /*  199 */ "SUM_RATE",
  /*  200 */ "SUM_IRATE",
  /*  201 */ "AVG_RATE",
  /*  202 */ "AVG_IRATE",
  /*  203 */ "TBID",
  /*  204 */ "SEMI",
  /*  205 */ "NONE",
  /*  206 */ "PREV",
  /*  207 */ "LINEAR",
  /*  208 */ "IMPORT",
  /*  209 */ "METRIC",
  /*  210 */ "TBNAME",
  /*  211 */ "JOIN",
  /*  212 */ "METRICS",
  /*  213 */ "INSERT",
  /*  214 */ "INTO",
  /*  215 */ "VALUES",
  /*  216 */ "program",
  /*  217 */ "cmd",
  /*  218 */ "dbPrefix",
  /*  219 */ "ids",
  /*  220 */ "cpxName",
  /*  221 */ "ifexists",
  /*  222 */ "alter_db_optr",
  /*  223 */ "alter_topic_optr",
  /*  224 */ "acct_optr",
  /*  225 */ "ifnotexists",
  /*  226 */ "db_optr",
  /*  227 */ "topic_optr",
  /*  228 */ "typename",
  /*  229 */ "pps",
  /*  230 */ "tseries",
  /*  231 */ "dbs",
  /*  232 */ "streams",
  /*  233 */ "storage",
  /*  234 */ "qtime",
  /*  235 */ "users",
  /*  236 */ "conns",
  /*  237 */ "state",
  /*  238 */ "keep",
  /*  239 */ "tagitemlist",
  /*  240 */ "cache",
  /*  241 */ "replica",
  /*  242 */ "quorum",
  /*  243 */ "days",
  /*  244 */ "minrows",
  /*  245 */ "maxrows",
  /*  246 */ "blocks",
  /*  247 */ "ctime",
  /*  248 */ "wal",
  /*  249 */ "fsync",
  /*  250 */ "comp",
  /*  251 */ "prec",
  /*  252 */ "update",
  /*  253 */ "cachelast",
  /*  254 */ "partitions",
  /*  255 */ "signed",
  /*  256 */ "create_table_args",
  /*  257 */ "create_stable_args",
  /*  258 */ "create_table_list",
  /*  259 */ "create_from_stable",
  /*  260 */ "columnlist",
  /*  261 */ "tagNamelist",
  /*  262 */ "select",
  /*  263 */ "column",
  /*  264 */ "tagitem",
  /*  265 */ "selcollist",
  /*  266 */ "from",
  /*  267 */ "where_opt",
  /*  268 */ "interval_opt",
  /*  269 */ "fill_opt",
  /*  270 */ "sliding_opt",
  /*  271 */ "groupby_opt",
  /*  272 */ "orderby_opt",
  /*  273 */ "having_opt",
  /*  274 */ "slimit_opt",
  /*  275 */ "limit_opt",
  /*  276 */ "union",
  /*  277 */ "sclp",
  /*  278 */ "distinct",
  /*  279 */ "expr",
  /*  280 */ "as",
  /*  281 */ "tablelist",
  /*  282 */ "tmvar",
  /*  283 */ "sortlist",
  /*  284 */ "sortitem",
  /*  285 */ "item",
  /*  286 */ "sortorder",
  /*  287 */ "grouplist",
  /*  288 */ "exprlist",
  /*  289 */ "expritem",
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
 /*  33 */ "cmd ::= DROP FUNCTION ids",
 /*  34 */ "cmd ::= DROP DNODE ids",
 /*  35 */ "cmd ::= DROP USER ids",
 /*  36 */ "cmd ::= DROP ACCOUNT ids",
 /*  37 */ "cmd ::= USE ids",
 /*  38 */ "cmd ::= DESCRIBE ids cpxName",
 /*  39 */ "cmd ::= ALTER USER ids PASS ids",
 /*  40 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids",
 /*  42 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids",
 /*  44 */ "cmd ::= ALTER LOCAL ids ids",
 /*  45 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  46 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  49 */ "ids ::= ID",
 /*  50 */ "ids ::= STRING",
 /*  51 */ "ifexists ::= IF EXISTS",
 /*  52 */ "ifexists ::=",
 /*  53 */ "ifnotexists ::= IF NOT EXISTS",
 /*  54 */ "ifnotexists ::=",
 /*  55 */ "cmd ::= CREATE DNODE ids",
 /*  56 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  57 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  58 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  59 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename",
 /*  60 */ "cmd ::= CREATE USER ids PASS ids",
 /*  61 */ "pps ::=",
 /*  62 */ "pps ::= PPS INTEGER",
 /*  63 */ "tseries ::=",
 /*  64 */ "tseries ::= TSERIES INTEGER",
 /*  65 */ "dbs ::=",
 /*  66 */ "dbs ::= DBS INTEGER",
 /*  67 */ "streams ::=",
 /*  68 */ "streams ::= STREAMS INTEGER",
 /*  69 */ "storage ::=",
 /*  70 */ "storage ::= STORAGE INTEGER",
 /*  71 */ "qtime ::=",
 /*  72 */ "qtime ::= QTIME INTEGER",
 /*  73 */ "users ::=",
 /*  74 */ "users ::= USERS INTEGER",
 /*  75 */ "conns ::=",
 /*  76 */ "conns ::= CONNS INTEGER",
 /*  77 */ "state ::=",
 /*  78 */ "state ::= STATE ids",
 /*  79 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  80 */ "keep ::= KEEP tagitemlist",
 /*  81 */ "cache ::= CACHE INTEGER",
 /*  82 */ "replica ::= REPLICA INTEGER",
 /*  83 */ "quorum ::= QUORUM INTEGER",
 /*  84 */ "days ::= DAYS INTEGER",
 /*  85 */ "minrows ::= MINROWS INTEGER",
 /*  86 */ "maxrows ::= MAXROWS INTEGER",
 /*  87 */ "blocks ::= BLOCKS INTEGER",
 /*  88 */ "ctime ::= CTIME INTEGER",
 /*  89 */ "wal ::= WAL INTEGER",
 /*  90 */ "fsync ::= FSYNC INTEGER",
 /*  91 */ "comp ::= COMP INTEGER",
 /*  92 */ "prec ::= PRECISION STRING",
 /*  93 */ "update ::= UPDATE INTEGER",
 /*  94 */ "cachelast ::= CACHELAST INTEGER",
 /*  95 */ "partitions ::= PARTITIONS INTEGER",
 /*  96 */ "db_optr ::=",
 /*  97 */ "db_optr ::= db_optr cache",
 /*  98 */ "db_optr ::= db_optr replica",
 /*  99 */ "db_optr ::= db_optr quorum",
 /* 100 */ "db_optr ::= db_optr days",
 /* 101 */ "db_optr ::= db_optr minrows",
 /* 102 */ "db_optr ::= db_optr maxrows",
 /* 103 */ "db_optr ::= db_optr blocks",
 /* 104 */ "db_optr ::= db_optr ctime",
 /* 105 */ "db_optr ::= db_optr wal",
 /* 106 */ "db_optr ::= db_optr fsync",
 /* 107 */ "db_optr ::= db_optr comp",
 /* 108 */ "db_optr ::= db_optr prec",
 /* 109 */ "db_optr ::= db_optr keep",
 /* 110 */ "db_optr ::= db_optr update",
 /* 111 */ "db_optr ::= db_optr cachelast",
 /* 112 */ "topic_optr ::= db_optr",
 /* 113 */ "topic_optr ::= topic_optr partitions",
 /* 114 */ "alter_db_optr ::=",
 /* 115 */ "alter_db_optr ::= alter_db_optr replica",
 /* 116 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 117 */ "alter_db_optr ::= alter_db_optr keep",
 /* 118 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 119 */ "alter_db_optr ::= alter_db_optr comp",
 /* 120 */ "alter_db_optr ::= alter_db_optr wal",
 /* 121 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 122 */ "alter_db_optr ::= alter_db_optr update",
 /* 123 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 124 */ "alter_topic_optr ::= alter_db_optr",
 /* 125 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 126 */ "typename ::= ids",
 /* 127 */ "typename ::= ids LP signed RP",
 /* 128 */ "typename ::= ids UNSIGNED",
 /* 129 */ "signed ::= INTEGER",
 /* 130 */ "signed ::= PLUS INTEGER",
 /* 131 */ "signed ::= MINUS INTEGER",
 /* 132 */ "cmd ::= CREATE TABLE create_table_args",
 /* 133 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 134 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE TABLE create_table_list",
 /* 136 */ "create_table_list ::= create_from_stable",
 /* 137 */ "create_table_list ::= create_table_list create_from_stable",
 /* 138 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 139 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 140 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 142 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 143 */ "tagNamelist ::= ids",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 145 */ "columnlist ::= columnlist COMMA column",
 /* 146 */ "columnlist ::= column",
 /* 147 */ "column ::= ids typename",
 /* 148 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 149 */ "tagitemlist ::= tagitem",
 /* 150 */ "tagitem ::= INTEGER",
 /* 151 */ "tagitem ::= FLOAT",
 /* 152 */ "tagitem ::= STRING",
 /* 153 */ "tagitem ::= BOOL",
 /* 154 */ "tagitem ::= NULL",
 /* 155 */ "tagitem ::= MINUS INTEGER",
 /* 156 */ "tagitem ::= MINUS FLOAT",
 /* 157 */ "tagitem ::= PLUS INTEGER",
 /* 158 */ "tagitem ::= PLUS FLOAT",
 /* 159 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 160 */ "union ::= select",
 /* 161 */ "union ::= LP union RP",
 /* 162 */ "union ::= union UNION ALL select",
 /* 163 */ "union ::= union UNION ALL LP select RP",
 /* 164 */ "cmd ::= union",
 /* 165 */ "select ::= SELECT selcollist",
 /* 166 */ "sclp ::= selcollist COMMA",
 /* 167 */ "sclp ::=",
 /* 168 */ "selcollist ::= sclp distinct expr as",
 /* 169 */ "selcollist ::= sclp STAR",
 /* 170 */ "as ::= AS ids",
 /* 171 */ "as ::= ids",
 /* 172 */ "as ::=",
 /* 173 */ "distinct ::= DISTINCT",
 /* 174 */ "distinct ::=",
 /* 175 */ "from ::= FROM tablelist",
 /* 176 */ "tablelist ::= ids cpxName",
 /* 177 */ "tablelist ::= ids cpxName ids",
 /* 178 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 179 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 180 */ "tmvar ::= VARIABLE",
 /* 181 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 182 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 183 */ "interval_opt ::=",
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
 /* 226 */ "expr ::= BOOL",
 /* 227 */ "expr ::= ID LP exprlist RP",
 /* 228 */ "expr ::= ID LP STAR RP",
 /* 229 */ "expr ::= expr IS NULL",
 /* 230 */ "expr ::= expr IS NOT NULL",
 /* 231 */ "expr ::= expr LT expr",
 /* 232 */ "expr ::= expr GT expr",
 /* 233 */ "expr ::= expr LE expr",
 /* 234 */ "expr ::= expr GE expr",
 /* 235 */ "expr ::= expr NE expr",
 /* 236 */ "expr ::= expr EQ expr",
 /* 237 */ "expr ::= expr BETWEEN expr AND expr",
 /* 238 */ "expr ::= expr AND expr",
 /* 239 */ "expr ::= expr OR expr",
 /* 240 */ "expr ::= expr PLUS expr",
 /* 241 */ "expr ::= expr MINUS expr",
 /* 242 */ "expr ::= expr STAR expr",
 /* 243 */ "expr ::= expr SLASH expr",
 /* 244 */ "expr ::= expr REM expr",
 /* 245 */ "expr ::= expr LIKE expr",
 /* 246 */ "expr ::= expr IN LP exprlist RP",
 /* 247 */ "exprlist ::= exprlist COMMA expritem",
 /* 248 */ "exprlist ::= expritem",
 /* 249 */ "expritem ::= expr",
 /* 250 */ "expritem ::=",
 /* 251 */ "cmd ::= RESET QUERY CACHE",
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
    case 238: /* keep */
    case 239: /* tagitemlist */
    case 260: /* columnlist */
    case 261: /* tagNamelist */
    case 269: /* fill_opt */
    case 271: /* groupby_opt */
    case 272: /* orderby_opt */
    case 283: /* sortlist */
    case 287: /* grouplist */
{
taosArrayDestroy((yypminor->yy161));
}
      break;
    case 258: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy518));
}
      break;
    case 262: /* select */
{
doDestroyQuerySql((yypminor->yy544));
}
      break;
    case 265: /* selcollist */
    case 277: /* sclp */
    case 288: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy458));
}
      break;
    case 267: /* where_opt */
    case 273: /* having_opt */
    case 279: /* expr */
    case 289: /* expritem */
{
tSqlExprDestroy((yypminor->yy70));
}
      break;
    case 276: /* union */
{
destroyAllSelectClause((yypminor->yy233));
}
      break;
    case 284: /* sortitem */
{
tVariantDestroy(&(yypminor->yy526));
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
   216,  /* (0) program ::= cmd */
   217,  /* (1) cmd ::= SHOW DATABASES */
   217,  /* (2) cmd ::= SHOW TOPICS */
   217,  /* (3) cmd ::= SHOW FUNCTIONS */
   217,  /* (4) cmd ::= SHOW MNODES */
   217,  /* (5) cmd ::= SHOW DNODES */
   217,  /* (6) cmd ::= SHOW ACCOUNTS */
   217,  /* (7) cmd ::= SHOW USERS */
   217,  /* (8) cmd ::= SHOW MODULES */
   217,  /* (9) cmd ::= SHOW QUERIES */
   217,  /* (10) cmd ::= SHOW CONNECTIONS */
   217,  /* (11) cmd ::= SHOW STREAMS */
   217,  /* (12) cmd ::= SHOW VARIABLES */
   217,  /* (13) cmd ::= SHOW SCORES */
   217,  /* (14) cmd ::= SHOW GRANTS */
   217,  /* (15) cmd ::= SHOW VNODES */
   217,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
   218,  /* (17) dbPrefix ::= */
   218,  /* (18) dbPrefix ::= ids DOT */
   220,  /* (19) cpxName ::= */
   220,  /* (20) cpxName ::= DOT ids */
   217,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   217,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   217,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   217,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   217,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   217,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   217,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   217,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   217,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   217,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   217,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   217,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   217,  /* (33) cmd ::= DROP FUNCTION ids */
   217,  /* (34) cmd ::= DROP DNODE ids */
   217,  /* (35) cmd ::= DROP USER ids */
   217,  /* (36) cmd ::= DROP ACCOUNT ids */
   217,  /* (37) cmd ::= USE ids */
   217,  /* (38) cmd ::= DESCRIBE ids cpxName */
   217,  /* (39) cmd ::= ALTER USER ids PASS ids */
   217,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   217,  /* (41) cmd ::= ALTER DNODE ids ids */
   217,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   217,  /* (43) cmd ::= ALTER LOCAL ids */
   217,  /* (44) cmd ::= ALTER LOCAL ids ids */
   217,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   217,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   217,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   217,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   219,  /* (49) ids ::= ID */
   219,  /* (50) ids ::= STRING */
   221,  /* (51) ifexists ::= IF EXISTS */
   221,  /* (52) ifexists ::= */
   225,  /* (53) ifnotexists ::= IF NOT EXISTS */
   225,  /* (54) ifnotexists ::= */
   217,  /* (55) cmd ::= CREATE DNODE ids */
   217,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   217,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   217,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   217,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
   217,  /* (60) cmd ::= CREATE USER ids PASS ids */
   229,  /* (61) pps ::= */
   229,  /* (62) pps ::= PPS INTEGER */
   230,  /* (63) tseries ::= */
   230,  /* (64) tseries ::= TSERIES INTEGER */
   231,  /* (65) dbs ::= */
   231,  /* (66) dbs ::= DBS INTEGER */
   232,  /* (67) streams ::= */
   232,  /* (68) streams ::= STREAMS INTEGER */
   233,  /* (69) storage ::= */
   233,  /* (70) storage ::= STORAGE INTEGER */
   234,  /* (71) qtime ::= */
   234,  /* (72) qtime ::= QTIME INTEGER */
   235,  /* (73) users ::= */
   235,  /* (74) users ::= USERS INTEGER */
   236,  /* (75) conns ::= */
   236,  /* (76) conns ::= CONNS INTEGER */
   237,  /* (77) state ::= */
   237,  /* (78) state ::= STATE ids */
   224,  /* (79) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   238,  /* (80) keep ::= KEEP tagitemlist */
   240,  /* (81) cache ::= CACHE INTEGER */
   241,  /* (82) replica ::= REPLICA INTEGER */
   242,  /* (83) quorum ::= QUORUM INTEGER */
   243,  /* (84) days ::= DAYS INTEGER */
   244,  /* (85) minrows ::= MINROWS INTEGER */
   245,  /* (86) maxrows ::= MAXROWS INTEGER */
   246,  /* (87) blocks ::= BLOCKS INTEGER */
   247,  /* (88) ctime ::= CTIME INTEGER */
   248,  /* (89) wal ::= WAL INTEGER */
   249,  /* (90) fsync ::= FSYNC INTEGER */
   250,  /* (91) comp ::= COMP INTEGER */
   251,  /* (92) prec ::= PRECISION STRING */
   252,  /* (93) update ::= UPDATE INTEGER */
   253,  /* (94) cachelast ::= CACHELAST INTEGER */
   254,  /* (95) partitions ::= PARTITIONS INTEGER */
   226,  /* (96) db_optr ::= */
   226,  /* (97) db_optr ::= db_optr cache */
   226,  /* (98) db_optr ::= db_optr replica */
   226,  /* (99) db_optr ::= db_optr quorum */
   226,  /* (100) db_optr ::= db_optr days */
   226,  /* (101) db_optr ::= db_optr minrows */
   226,  /* (102) db_optr ::= db_optr maxrows */
   226,  /* (103) db_optr ::= db_optr blocks */
   226,  /* (104) db_optr ::= db_optr ctime */
   226,  /* (105) db_optr ::= db_optr wal */
   226,  /* (106) db_optr ::= db_optr fsync */
   226,  /* (107) db_optr ::= db_optr comp */
   226,  /* (108) db_optr ::= db_optr prec */
   226,  /* (109) db_optr ::= db_optr keep */
   226,  /* (110) db_optr ::= db_optr update */
   226,  /* (111) db_optr ::= db_optr cachelast */
   227,  /* (112) topic_optr ::= db_optr */
   227,  /* (113) topic_optr ::= topic_optr partitions */
   222,  /* (114) alter_db_optr ::= */
   222,  /* (115) alter_db_optr ::= alter_db_optr replica */
   222,  /* (116) alter_db_optr ::= alter_db_optr quorum */
   222,  /* (117) alter_db_optr ::= alter_db_optr keep */
   222,  /* (118) alter_db_optr ::= alter_db_optr blocks */
   222,  /* (119) alter_db_optr ::= alter_db_optr comp */
   222,  /* (120) alter_db_optr ::= alter_db_optr wal */
   222,  /* (121) alter_db_optr ::= alter_db_optr fsync */
   222,  /* (122) alter_db_optr ::= alter_db_optr update */
   222,  /* (123) alter_db_optr ::= alter_db_optr cachelast */
   223,  /* (124) alter_topic_optr ::= alter_db_optr */
   223,  /* (125) alter_topic_optr ::= alter_topic_optr partitions */
   228,  /* (126) typename ::= ids */
   228,  /* (127) typename ::= ids LP signed RP */
   228,  /* (128) typename ::= ids UNSIGNED */
   255,  /* (129) signed ::= INTEGER */
   255,  /* (130) signed ::= PLUS INTEGER */
   255,  /* (131) signed ::= MINUS INTEGER */
   217,  /* (132) cmd ::= CREATE TABLE create_table_args */
   217,  /* (133) cmd ::= CREATE TABLE create_stable_args */
   217,  /* (134) cmd ::= CREATE STABLE create_stable_args */
   217,  /* (135) cmd ::= CREATE TABLE create_table_list */
   258,  /* (136) create_table_list ::= create_from_stable */
   258,  /* (137) create_table_list ::= create_table_list create_from_stable */
   256,  /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   257,  /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   259,  /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   259,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   261,  /* (142) tagNamelist ::= tagNamelist COMMA ids */
   261,  /* (143) tagNamelist ::= ids */
   256,  /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
   260,  /* (145) columnlist ::= columnlist COMMA column */
   260,  /* (146) columnlist ::= column */
   263,  /* (147) column ::= ids typename */
   239,  /* (148) tagitemlist ::= tagitemlist COMMA tagitem */
   239,  /* (149) tagitemlist ::= tagitem */
   264,  /* (150) tagitem ::= INTEGER */
   264,  /* (151) tagitem ::= FLOAT */
   264,  /* (152) tagitem ::= STRING */
   264,  /* (153) tagitem ::= BOOL */
   264,  /* (154) tagitem ::= NULL */
   264,  /* (155) tagitem ::= MINUS INTEGER */
   264,  /* (156) tagitem ::= MINUS FLOAT */
   264,  /* (157) tagitem ::= PLUS INTEGER */
   264,  /* (158) tagitem ::= PLUS FLOAT */
   262,  /* (159) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   276,  /* (160) union ::= select */
   276,  /* (161) union ::= LP union RP */
   276,  /* (162) union ::= union UNION ALL select */
   276,  /* (163) union ::= union UNION ALL LP select RP */
   217,  /* (164) cmd ::= union */
   262,  /* (165) select ::= SELECT selcollist */
   277,  /* (166) sclp ::= selcollist COMMA */
   277,  /* (167) sclp ::= */
   265,  /* (168) selcollist ::= sclp distinct expr as */
   265,  /* (169) selcollist ::= sclp STAR */
   280,  /* (170) as ::= AS ids */
   280,  /* (171) as ::= ids */
   280,  /* (172) as ::= */
   278,  /* (173) distinct ::= DISTINCT */
   278,  /* (174) distinct ::= */
   266,  /* (175) from ::= FROM tablelist */
   281,  /* (176) tablelist ::= ids cpxName */
   281,  /* (177) tablelist ::= ids cpxName ids */
   281,  /* (178) tablelist ::= tablelist COMMA ids cpxName */
   281,  /* (179) tablelist ::= tablelist COMMA ids cpxName ids */
   282,  /* (180) tmvar ::= VARIABLE */
   268,  /* (181) interval_opt ::= INTERVAL LP tmvar RP */
   268,  /* (182) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   268,  /* (183) interval_opt ::= */
   269,  /* (184) fill_opt ::= */
   269,  /* (185) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   269,  /* (186) fill_opt ::= FILL LP ID RP */
   270,  /* (187) sliding_opt ::= SLIDING LP tmvar RP */
   270,  /* (188) sliding_opt ::= */
   272,  /* (189) orderby_opt ::= */
   272,  /* (190) orderby_opt ::= ORDER BY sortlist */
   283,  /* (191) sortlist ::= sortlist COMMA item sortorder */
   283,  /* (192) sortlist ::= item sortorder */
   285,  /* (193) item ::= ids cpxName */
   286,  /* (194) sortorder ::= ASC */
   286,  /* (195) sortorder ::= DESC */
   286,  /* (196) sortorder ::= */
   271,  /* (197) groupby_opt ::= */
   271,  /* (198) groupby_opt ::= GROUP BY grouplist */
   287,  /* (199) grouplist ::= grouplist COMMA item */
   287,  /* (200) grouplist ::= item */
   273,  /* (201) having_opt ::= */
   273,  /* (202) having_opt ::= HAVING expr */
   275,  /* (203) limit_opt ::= */
   275,  /* (204) limit_opt ::= LIMIT signed */
   275,  /* (205) limit_opt ::= LIMIT signed OFFSET signed */
   275,  /* (206) limit_opt ::= LIMIT signed COMMA signed */
   274,  /* (207) slimit_opt ::= */
   274,  /* (208) slimit_opt ::= SLIMIT signed */
   274,  /* (209) slimit_opt ::= SLIMIT signed SOFFSET signed */
   274,  /* (210) slimit_opt ::= SLIMIT signed COMMA signed */
   267,  /* (211) where_opt ::= */
   267,  /* (212) where_opt ::= WHERE expr */
   279,  /* (213) expr ::= LP expr RP */
   279,  /* (214) expr ::= ID */
   279,  /* (215) expr ::= ID DOT ID */
   279,  /* (216) expr ::= ID DOT STAR */
   279,  /* (217) expr ::= INTEGER */
   279,  /* (218) expr ::= MINUS INTEGER */
   279,  /* (219) expr ::= PLUS INTEGER */
   279,  /* (220) expr ::= FLOAT */
   279,  /* (221) expr ::= MINUS FLOAT */
   279,  /* (222) expr ::= PLUS FLOAT */
   279,  /* (223) expr ::= STRING */
   279,  /* (224) expr ::= NOW */
   279,  /* (225) expr ::= VARIABLE */
   279,  /* (226) expr ::= BOOL */
   279,  /* (227) expr ::= ID LP exprlist RP */
   279,  /* (228) expr ::= ID LP STAR RP */
   279,  /* (229) expr ::= expr IS NULL */
   279,  /* (230) expr ::= expr IS NOT NULL */
   279,  /* (231) expr ::= expr LT expr */
   279,  /* (232) expr ::= expr GT expr */
   279,  /* (233) expr ::= expr LE expr */
   279,  /* (234) expr ::= expr GE expr */
   279,  /* (235) expr ::= expr NE expr */
   279,  /* (236) expr ::= expr EQ expr */
   279,  /* (237) expr ::= expr BETWEEN expr AND expr */
   279,  /* (238) expr ::= expr AND expr */
   279,  /* (239) expr ::= expr OR expr */
   279,  /* (240) expr ::= expr PLUS expr */
   279,  /* (241) expr ::= expr MINUS expr */
   279,  /* (242) expr ::= expr STAR expr */
   279,  /* (243) expr ::= expr SLASH expr */
   279,  /* (244) expr ::= expr REM expr */
   279,  /* (245) expr ::= expr LIKE expr */
   279,  /* (246) expr ::= expr IN LP exprlist RP */
   288,  /* (247) exprlist ::= exprlist COMMA expritem */
   288,  /* (248) exprlist ::= expritem */
   289,  /* (249) expritem ::= expr */
   289,  /* (250) expritem ::= */
   217,  /* (251) cmd ::= RESET QUERY CACHE */
   217,  /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   217,  /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   217,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   217,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   217,  /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   217,  /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   217,  /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   217,  /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   217,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   217,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   217,  /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   217,  /* (263) cmd ::= KILL CONNECTION INTEGER */
   217,  /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   217,  /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -3,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (17) dbPrefix ::= */
   -2,  /* (18) dbPrefix ::= ids DOT */
    0,  /* (19) cpxName ::= */
   -2,  /* (20) cpxName ::= DOT ids */
   -5,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   -4,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (33) cmd ::= DROP FUNCTION ids */
   -3,  /* (34) cmd ::= DROP DNODE ids */
   -3,  /* (35) cmd ::= DROP USER ids */
   -3,  /* (36) cmd ::= DROP ACCOUNT ids */
   -2,  /* (37) cmd ::= USE ids */
   -3,  /* (38) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (39) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (41) cmd ::= ALTER DNODE ids ids */
   -5,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (43) cmd ::= ALTER LOCAL ids */
   -4,  /* (44) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (49) ids ::= ID */
   -1,  /* (50) ids ::= STRING */
   -2,  /* (51) ifexists ::= IF EXISTS */
    0,  /* (52) ifexists ::= */
   -3,  /* (53) ifnotexists ::= IF NOT EXISTS */
    0,  /* (54) ifnotexists ::= */
   -3,  /* (55) cmd ::= CREATE DNODE ids */
   -6,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -7,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
   -5,  /* (60) cmd ::= CREATE USER ids PASS ids */
    0,  /* (61) pps ::= */
   -2,  /* (62) pps ::= PPS INTEGER */
    0,  /* (63) tseries ::= */
   -2,  /* (64) tseries ::= TSERIES INTEGER */
    0,  /* (65) dbs ::= */
   -2,  /* (66) dbs ::= DBS INTEGER */
    0,  /* (67) streams ::= */
   -2,  /* (68) streams ::= STREAMS INTEGER */
    0,  /* (69) storage ::= */
   -2,  /* (70) storage ::= STORAGE INTEGER */
    0,  /* (71) qtime ::= */
   -2,  /* (72) qtime ::= QTIME INTEGER */
    0,  /* (73) users ::= */
   -2,  /* (74) users ::= USERS INTEGER */
    0,  /* (75) conns ::= */
   -2,  /* (76) conns ::= CONNS INTEGER */
    0,  /* (77) state ::= */
   -2,  /* (78) state ::= STATE ids */
   -9,  /* (79) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (80) keep ::= KEEP tagitemlist */
   -2,  /* (81) cache ::= CACHE INTEGER */
   -2,  /* (82) replica ::= REPLICA INTEGER */
   -2,  /* (83) quorum ::= QUORUM INTEGER */
   -2,  /* (84) days ::= DAYS INTEGER */
   -2,  /* (85) minrows ::= MINROWS INTEGER */
   -2,  /* (86) maxrows ::= MAXROWS INTEGER */
   -2,  /* (87) blocks ::= BLOCKS INTEGER */
   -2,  /* (88) ctime ::= CTIME INTEGER */
   -2,  /* (89) wal ::= WAL INTEGER */
   -2,  /* (90) fsync ::= FSYNC INTEGER */
   -2,  /* (91) comp ::= COMP INTEGER */
   -2,  /* (92) prec ::= PRECISION STRING */
   -2,  /* (93) update ::= UPDATE INTEGER */
   -2,  /* (94) cachelast ::= CACHELAST INTEGER */
   -2,  /* (95) partitions ::= PARTITIONS INTEGER */
    0,  /* (96) db_optr ::= */
   -2,  /* (97) db_optr ::= db_optr cache */
   -2,  /* (98) db_optr ::= db_optr replica */
   -2,  /* (99) db_optr ::= db_optr quorum */
   -2,  /* (100) db_optr ::= db_optr days */
   -2,  /* (101) db_optr ::= db_optr minrows */
   -2,  /* (102) db_optr ::= db_optr maxrows */
   -2,  /* (103) db_optr ::= db_optr blocks */
   -2,  /* (104) db_optr ::= db_optr ctime */
   -2,  /* (105) db_optr ::= db_optr wal */
   -2,  /* (106) db_optr ::= db_optr fsync */
   -2,  /* (107) db_optr ::= db_optr comp */
   -2,  /* (108) db_optr ::= db_optr prec */
   -2,  /* (109) db_optr ::= db_optr keep */
   -2,  /* (110) db_optr ::= db_optr update */
   -2,  /* (111) db_optr ::= db_optr cachelast */
   -1,  /* (112) topic_optr ::= db_optr */
   -2,  /* (113) topic_optr ::= topic_optr partitions */
    0,  /* (114) alter_db_optr ::= */
   -2,  /* (115) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (116) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (117) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (118) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (119) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (120) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (121) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (122) alter_db_optr ::= alter_db_optr update */
   -2,  /* (123) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (124) alter_topic_optr ::= alter_db_optr */
   -2,  /* (125) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (126) typename ::= ids */
   -4,  /* (127) typename ::= ids LP signed RP */
   -2,  /* (128) typename ::= ids UNSIGNED */
   -1,  /* (129) signed ::= INTEGER */
   -2,  /* (130) signed ::= PLUS INTEGER */
   -2,  /* (131) signed ::= MINUS INTEGER */
   -3,  /* (132) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (133) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (134) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (135) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (136) create_table_list ::= create_from_stable */
   -2,  /* (137) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (142) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (143) tagNamelist ::= ids */
   -5,  /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (145) columnlist ::= columnlist COMMA column */
   -1,  /* (146) columnlist ::= column */
   -2,  /* (147) column ::= ids typename */
   -3,  /* (148) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (149) tagitemlist ::= tagitem */
   -1,  /* (150) tagitem ::= INTEGER */
   -1,  /* (151) tagitem ::= FLOAT */
   -1,  /* (152) tagitem ::= STRING */
   -1,  /* (153) tagitem ::= BOOL */
   -1,  /* (154) tagitem ::= NULL */
   -2,  /* (155) tagitem ::= MINUS INTEGER */
   -2,  /* (156) tagitem ::= MINUS FLOAT */
   -2,  /* (157) tagitem ::= PLUS INTEGER */
   -2,  /* (158) tagitem ::= PLUS FLOAT */
  -12,  /* (159) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -1,  /* (160) union ::= select */
   -3,  /* (161) union ::= LP union RP */
   -4,  /* (162) union ::= union UNION ALL select */
   -6,  /* (163) union ::= union UNION ALL LP select RP */
   -1,  /* (164) cmd ::= union */
   -2,  /* (165) select ::= SELECT selcollist */
   -2,  /* (166) sclp ::= selcollist COMMA */
    0,  /* (167) sclp ::= */
   -4,  /* (168) selcollist ::= sclp distinct expr as */
   -2,  /* (169) selcollist ::= sclp STAR */
   -2,  /* (170) as ::= AS ids */
   -1,  /* (171) as ::= ids */
    0,  /* (172) as ::= */
   -1,  /* (173) distinct ::= DISTINCT */
    0,  /* (174) distinct ::= */
   -2,  /* (175) from ::= FROM tablelist */
   -2,  /* (176) tablelist ::= ids cpxName */
   -3,  /* (177) tablelist ::= ids cpxName ids */
   -4,  /* (178) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (179) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (180) tmvar ::= VARIABLE */
   -4,  /* (181) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (182) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (183) interval_opt ::= */
    0,  /* (184) fill_opt ::= */
   -6,  /* (185) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (186) fill_opt ::= FILL LP ID RP */
   -4,  /* (187) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (188) sliding_opt ::= */
    0,  /* (189) orderby_opt ::= */
   -3,  /* (190) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (191) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (192) sortlist ::= item sortorder */
   -2,  /* (193) item ::= ids cpxName */
   -1,  /* (194) sortorder ::= ASC */
   -1,  /* (195) sortorder ::= DESC */
    0,  /* (196) sortorder ::= */
    0,  /* (197) groupby_opt ::= */
   -3,  /* (198) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (199) grouplist ::= grouplist COMMA item */
   -1,  /* (200) grouplist ::= item */
    0,  /* (201) having_opt ::= */
   -2,  /* (202) having_opt ::= HAVING expr */
    0,  /* (203) limit_opt ::= */
   -2,  /* (204) limit_opt ::= LIMIT signed */
   -4,  /* (205) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (206) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (207) slimit_opt ::= */
   -2,  /* (208) slimit_opt ::= SLIMIT signed */
   -4,  /* (209) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (210) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (211) where_opt ::= */
   -2,  /* (212) where_opt ::= WHERE expr */
   -3,  /* (213) expr ::= LP expr RP */
   -1,  /* (214) expr ::= ID */
   -3,  /* (215) expr ::= ID DOT ID */
   -3,  /* (216) expr ::= ID DOT STAR */
   -1,  /* (217) expr ::= INTEGER */
   -2,  /* (218) expr ::= MINUS INTEGER */
   -2,  /* (219) expr ::= PLUS INTEGER */
   -1,  /* (220) expr ::= FLOAT */
   -2,  /* (221) expr ::= MINUS FLOAT */
   -2,  /* (222) expr ::= PLUS FLOAT */
   -1,  /* (223) expr ::= STRING */
   -1,  /* (224) expr ::= NOW */
   -1,  /* (225) expr ::= VARIABLE */
   -1,  /* (226) expr ::= BOOL */
   -4,  /* (227) expr ::= ID LP exprlist RP */
   -4,  /* (228) expr ::= ID LP STAR RP */
   -3,  /* (229) expr ::= expr IS NULL */
   -4,  /* (230) expr ::= expr IS NOT NULL */
   -3,  /* (231) expr ::= expr LT expr */
   -3,  /* (232) expr ::= expr GT expr */
   -3,  /* (233) expr ::= expr LE expr */
   -3,  /* (234) expr ::= expr GE expr */
   -3,  /* (235) expr ::= expr NE expr */
   -3,  /* (236) expr ::= expr EQ expr */
   -5,  /* (237) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (238) expr ::= expr AND expr */
   -3,  /* (239) expr ::= expr OR expr */
   -3,  /* (240) expr ::= expr PLUS expr */
   -3,  /* (241) expr ::= expr MINUS expr */
   -3,  /* (242) expr ::= expr STAR expr */
   -3,  /* (243) expr ::= expr SLASH expr */
   -3,  /* (244) expr ::= expr REM expr */
   -3,  /* (245) expr ::= expr LIKE expr */
   -5,  /* (246) expr ::= expr IN LP exprlist RP */
   -3,  /* (247) exprlist ::= exprlist COMMA expritem */
   -1,  /* (248) exprlist ::= expritem */
   -1,  /* (249) expritem ::= expr */
    0,  /* (250) expritem ::= */
   -3,  /* (251) cmd ::= RESET QUERY CACHE */
   -7,  /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (263) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 132: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==134);
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
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
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
    setDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-2].minor.yy0);
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
      case 33: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 34: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 35: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 36: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 37: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 38: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 39: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 40: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 44: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 45: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 46: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==46);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy302, &t);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy231);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy231);}
        break;
      case 49: /* ids ::= ID */
      case 50: /* ids ::= STRING */ yytestcase(yyruleno==50);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 51: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 52: /* ifexists ::= */
      case 54: /* ifnotexists ::= */ yytestcase(yyruleno==54);
      case 174: /* distinct ::= */ yytestcase(yyruleno==174);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 53: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy231);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy302, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy223);}
        break;
      case 60: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 61: /* pps ::= */
      case 63: /* tseries ::= */ yytestcase(yyruleno==63);
      case 65: /* dbs ::= */ yytestcase(yyruleno==65);
      case 67: /* streams ::= */ yytestcase(yyruleno==67);
      case 69: /* storage ::= */ yytestcase(yyruleno==69);
      case 71: /* qtime ::= */ yytestcase(yyruleno==71);
      case 73: /* users ::= */ yytestcase(yyruleno==73);
      case 75: /* conns ::= */ yytestcase(yyruleno==75);
      case 77: /* state ::= */ yytestcase(yyruleno==77);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 62: /* pps ::= PPS INTEGER */
      case 64: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==64);
      case 66: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==70);
      case 72: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==72);
      case 74: /* users ::= USERS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==76);
      case 78: /* state ::= STATE ids */ yytestcase(yyruleno==78);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 79: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy231.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy231.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy231.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy231.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy231.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy231.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy231.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy231.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy231.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy231 = yylhsminor.yy231;
        break;
      case 80: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy161 = yymsp[0].minor.yy161; }
        break;
      case 81: /* cache ::= CACHE INTEGER */
      case 82: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==82);
      case 83: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==83);
      case 84: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==88);
      case 89: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==89);
      case 90: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==90);
      case 91: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==91);
      case 92: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==92);
      case 93: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==93);
      case 94: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==94);
      case 95: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==95);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 96: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy302); yymsp[1].minor.yy302.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 97: /* db_optr ::= db_optr cache */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 98: /* db_optr ::= db_optr replica */
      case 115: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==115);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 99: /* db_optr ::= db_optr quorum */
      case 116: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==116);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 100: /* db_optr ::= db_optr days */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 101: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 102: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 103: /* db_optr ::= db_optr blocks */
      case 118: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==118);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 104: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 105: /* db_optr ::= db_optr wal */
      case 120: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==120);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 106: /* db_optr ::= db_optr fsync */
      case 121: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==121);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 107: /* db_optr ::= db_optr comp */
      case 119: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==119);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 108: /* db_optr ::= db_optr prec */
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 109: /* db_optr ::= db_optr keep */
      case 117: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==117);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.keep = yymsp[0].minor.yy161; }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 110: /* db_optr ::= db_optr update */
      case 122: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==122);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 111: /* db_optr ::= db_optr cachelast */
      case 123: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==123);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 112: /* topic_optr ::= db_optr */
      case 124: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==124);
{ yylhsminor.yy302 = yymsp[0].minor.yy302; yylhsminor.yy302.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy302 = yylhsminor.yy302;
        break;
      case 113: /* topic_optr ::= topic_optr partitions */
      case 125: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==125);
{ yylhsminor.yy302 = yymsp[-1].minor.yy302; yylhsminor.yy302.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy302 = yylhsminor.yy302;
        break;
      case 114: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy302); yymsp[1].minor.yy302.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 126: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy223, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy223 = yylhsminor.yy223;
        break;
      case 127: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy369 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy223 = yylhsminor.yy223;
        break;
      case 128: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy223, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 129: /* signed ::= INTEGER */
{ yylhsminor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 130: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 131: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 135: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy518;}
        break;
      case 136: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy356);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy518 = pCreateTable;
}
  yymsp[0].minor.yy518 = yylhsminor.yy518;
        break;
      case 137: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy518->childTableInfo, &yymsp[0].minor.yy356);
  yylhsminor.yy518 = yymsp[-1].minor.yy518;
}
  yymsp[-1].minor.yy518 = yylhsminor.yy518;
        break;
      case 138: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy518 = tSetCreateSqlElems(yymsp[-1].minor.yy161, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy518, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy518 = yylhsminor.yy518;
        break;
      case 139: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy518 = tSetCreateSqlElems(yymsp[-5].minor.yy161, yymsp[-1].minor.yy161, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy518, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy518 = yylhsminor.yy518;
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy356 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy161, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy356 = yylhsminor.yy356;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy356 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy161, yymsp[-1].minor.yy161, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy356 = yylhsminor.yy356;
        break;
      case 142: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy161, &yymsp[0].minor.yy0); yylhsminor.yy161 = yymsp[-2].minor.yy161;  }
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 143: /* tagNamelist ::= ids */
{yylhsminor.yy161 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy161, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy518 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy544, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy518, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy518 = yylhsminor.yy518;
        break;
      case 145: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy161, &yymsp[0].minor.yy223); yylhsminor.yy161 = yymsp[-2].minor.yy161;  }
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 146: /* columnlist ::= column */
{yylhsminor.yy161 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy161, &yymsp[0].minor.yy223);}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 147: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 148: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy161 = tVariantListAppend(yymsp[-2].minor.yy161, &yymsp[0].minor.yy526, -1);    }
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 149: /* tagitemlist ::= tagitem */
{ yylhsminor.yy161 = tVariantListAppend(NULL, &yymsp[0].minor.yy526, -1); }
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 150: /* tagitem ::= INTEGER */
      case 151: /* tagitem ::= FLOAT */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= STRING */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= BOOL */ yytestcase(yyruleno==153);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy526, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 154: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy526, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 155: /* tagitem ::= MINUS INTEGER */
      case 156: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==158);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy526, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 159: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy544 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy458, yymsp[-9].minor.yy161, yymsp[-8].minor.yy70, yymsp[-4].minor.yy161, yymsp[-3].minor.yy161, &yymsp[-7].minor.yy300, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy161, &yymsp[0].minor.yy394, &yymsp[-1].minor.yy394);
}
  yymsp[-11].minor.yy544 = yylhsminor.yy544;
        break;
      case 160: /* union ::= select */
{ yylhsminor.yy233 = setSubclause(NULL, yymsp[0].minor.yy544); }
  yymsp[0].minor.yy233 = yylhsminor.yy233;
        break;
      case 161: /* union ::= LP union RP */
{ yymsp[-2].minor.yy233 = yymsp[-1].minor.yy233; }
        break;
      case 162: /* union ::= union UNION ALL select */
{ yylhsminor.yy233 = appendSelectClause(yymsp[-3].minor.yy233, yymsp[0].minor.yy544); }
  yymsp[-3].minor.yy233 = yylhsminor.yy233;
        break;
      case 163: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy233 = appendSelectClause(yymsp[-5].minor.yy233, yymsp[-1].minor.yy544); }
  yymsp[-5].minor.yy233 = yylhsminor.yy233;
        break;
      case 164: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy233, NULL, TSDB_SQL_SELECT); }
        break;
      case 165: /* select ::= SELECT selcollist */
{
  yylhsminor.yy544 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy458, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy544 = yylhsminor.yy544;
        break;
      case 166: /* sclp ::= selcollist COMMA */
{yylhsminor.yy458 = yymsp[-1].minor.yy458;}
  yymsp[-1].minor.yy458 = yylhsminor.yy458;
        break;
      case 167: /* sclp ::= */
{yymsp[1].minor.yy458 = 0;}
        break;
      case 168: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy458 = tSqlExprListAppend(yymsp[-3].minor.yy458, yymsp[-1].minor.yy70,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy458 = yylhsminor.yy458;
        break;
      case 169: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy458 = tSqlExprListAppend(yymsp[-1].minor.yy458, pNode, 0, 0);
}
  yymsp[-1].minor.yy458 = yylhsminor.yy458;
        break;
      case 170: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 171: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 173: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 175: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy161 = yymsp[0].minor.yy161;}
        break;
      case 176: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy161 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy161 = tVariantListAppendToken(yylhsminor.yy161, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy161 = yylhsminor.yy161;
        break;
      case 177: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy161 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy161 = tVariantListAppendToken(yylhsminor.yy161, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 178: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy161 = tVariantListAppendToken(yymsp[-3].minor.yy161, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy161 = tVariantListAppendToken(yylhsminor.yy161, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy161 = yylhsminor.yy161;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy161 = tVariantListAppendToken(yymsp[-4].minor.yy161, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy161 = tVariantListAppendToken(yylhsminor.yy161, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy161 = yylhsminor.yy161;
        break;
      case 180: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy300.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy300.offset.n = 0; yymsp[-3].minor.yy300.offset.z = NULL; yymsp[-3].minor.yy300.offset.type = 0;}
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy300.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy300.offset = yymsp[-1].minor.yy0;}
        break;
      case 183: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy300, 0, sizeof(yymsp[1].minor.yy300));}
        break;
      case 184: /* fill_opt ::= */
{yymsp[1].minor.yy161 = 0;     }
        break;
      case 185: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy161, &A, -1, 0);
    yymsp[-5].minor.yy161 = yymsp[-1].minor.yy161;
}
        break;
      case 186: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy161 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 187: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 188: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 189: /* orderby_opt ::= */
{yymsp[1].minor.yy161 = 0;}
        break;
      case 190: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy161 = yymsp[0].minor.yy161;}
        break;
      case 191: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy161 = tVariantListAppend(yymsp[-3].minor.yy161, &yymsp[-1].minor.yy526, yymsp[0].minor.yy452);
}
  yymsp[-3].minor.yy161 = yylhsminor.yy161;
        break;
      case 192: /* sortlist ::= item sortorder */
{
  yylhsminor.yy161 = tVariantListAppend(NULL, &yymsp[-1].minor.yy526, yymsp[0].minor.yy452);
}
  yymsp[-1].minor.yy161 = yylhsminor.yy161;
        break;
      case 193: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy526, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 194: /* sortorder ::= ASC */
{ yymsp[0].minor.yy452 = TSDB_ORDER_ASC; }
        break;
      case 195: /* sortorder ::= DESC */
{ yymsp[0].minor.yy452 = TSDB_ORDER_DESC;}
        break;
      case 196: /* sortorder ::= */
{ yymsp[1].minor.yy452 = TSDB_ORDER_ASC; }
        break;
      case 197: /* groupby_opt ::= */
{ yymsp[1].minor.yy161 = 0;}
        break;
      case 198: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy161 = yymsp[0].minor.yy161;}
        break;
      case 199: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy161 = tVariantListAppend(yymsp[-2].minor.yy161, &yymsp[0].minor.yy526, -1);
}
  yymsp[-2].minor.yy161 = yylhsminor.yy161;
        break;
      case 200: /* grouplist ::= item */
{
  yylhsminor.yy161 = tVariantListAppend(NULL, &yymsp[0].minor.yy526, -1);
}
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 201: /* having_opt ::= */
      case 211: /* where_opt ::= */ yytestcase(yyruleno==211);
      case 250: /* expritem ::= */ yytestcase(yyruleno==250);
{yymsp[1].minor.yy70 = 0;}
        break;
      case 202: /* having_opt ::= HAVING expr */
      case 212: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==212);
{yymsp[-1].minor.yy70 = yymsp[0].minor.yy70;}
        break;
      case 203: /* limit_opt ::= */
      case 207: /* slimit_opt ::= */ yytestcase(yyruleno==207);
{yymsp[1].minor.yy394.limit = -1; yymsp[1].minor.yy394.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed */
      case 208: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==208);
{yymsp[-1].minor.yy394.limit = yymsp[0].minor.yy369;  yymsp[-1].minor.yy394.offset = 0;}
        break;
      case 205: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy394.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[0].minor.yy369;}
        break;
      case 206: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy394.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[-2].minor.yy369;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy394.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[0].minor.yy369;}
        break;
      case 210: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy394.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy394.offset = yymsp[-2].minor.yy369;}
        break;
      case 213: /* expr ::= LP expr RP */
{yylhsminor.yy70 = yymsp[-1].minor.yy70; yylhsminor.yy70->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy70->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 214: /* expr ::= ID */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 215: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 216: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 217: /* expr ::= INTEGER */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 218: /* expr ::= MINUS INTEGER */
      case 219: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==219);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy70 = yylhsminor.yy70;
        break;
      case 220: /* expr ::= FLOAT */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 221: /* expr ::= MINUS FLOAT */
      case 222: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy70 = yylhsminor.yy70;
        break;
      case 223: /* expr ::= STRING */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 224: /* expr ::= NOW */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 225: /* expr ::= VARIABLE */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 226: /* expr ::= BOOL */
{ yylhsminor.yy70 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 227: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy70 = tSqlExprCreateFunction(yymsp[-1].minor.yy458, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy70 = yylhsminor.yy70;
        break;
      case 228: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy70 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy70 = yylhsminor.yy70;
        break;
      case 229: /* expr ::= expr IS NULL */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 230: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-3].minor.yy70, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy70 = yylhsminor.yy70;
        break;
      case 231: /* expr ::= expr LT expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_LT);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 232: /* expr ::= expr GT expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_GT);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 233: /* expr ::= expr LE expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_LE);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 234: /* expr ::= expr GE expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_GE);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 235: /* expr ::= expr NE expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_NE);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 236: /* expr ::= expr EQ expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_EQ);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 237: /* expr ::= expr BETWEEN expr AND expr */
{ tSQLExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy70); yylhsminor.yy70 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy70, yymsp[-2].minor.yy70, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy70, TK_LE), TK_AND);}
  yymsp[-4].minor.yy70 = yylhsminor.yy70;
        break;
      case 238: /* expr ::= expr AND expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_AND);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 239: /* expr ::= expr OR expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_OR); }
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 240: /* expr ::= expr PLUS expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_PLUS);  }
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 241: /* expr ::= expr MINUS expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_MINUS); }
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 242: /* expr ::= expr STAR expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_STAR);  }
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 243: /* expr ::= expr SLASH expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_DIVIDE);}
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 244: /* expr ::= expr REM expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_REM);   }
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 245: /* expr ::= expr LIKE expr */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-2].minor.yy70, yymsp[0].minor.yy70, TK_LIKE);  }
  yymsp[-2].minor.yy70 = yylhsminor.yy70;
        break;
      case 246: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy70 = tSqlExprCreate(yymsp[-4].minor.yy70, (tSQLExpr*)yymsp[-1].minor.yy458, TK_IN); }
  yymsp[-4].minor.yy70 = yylhsminor.yy70;
        break;
      case 247: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy458 = tSqlExprListAppend(yymsp[-2].minor.yy458,yymsp[0].minor.yy70,0, 0);}
  yymsp[-2].minor.yy458 = yylhsminor.yy458;
        break;
      case 248: /* exprlist ::= expritem */
{yylhsminor.yy458 = tSqlExprListAppend(0,yymsp[0].minor.yy70,0, 0);}
  yymsp[0].minor.yy458 = yylhsminor.yy458;
        break;
      case 249: /* expritem ::= expr */
{yylhsminor.yy70 = yymsp[0].minor.yy70;}
  yymsp[0].minor.yy70 = yylhsminor.yy70;
        break;
      case 251: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 252: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 253: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 254: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
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

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy526, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy161, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
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

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
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
