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
#define YYNOCODE 287
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateDbInfo yy100;
  int yy116;
  SIntervalVal yy126;
  tSQLExprList* yy178;
  SArray* yy207;
  int64_t yy208;
  tVariant yy232;
  SLimitVal yy314;
  SCreateTableSQL* yy414;
  SSubclauseInfo* yy441;
  tSQLExpr* yy484;
  SCreateAcctInfo yy505;
  TAOS_FIELD yy517;
  SQuerySQL* yy526;
  SCreatedTableInfo yy542;
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
#define YYNSTATE             306
#define YYNRULE              263
#define YYNRULE_WITH_ACTION  263
#define YYNTOKEN             213
#define YY_MAX_SHIFT         305
#define YY_MIN_SHIFTREDUCE   494
#define YY_MAX_SHIFTREDUCE   756
#define YY_ERROR_ACTION      757
#define YY_ACCEPT_ACTION     758
#define YY_NO_ACTION         759
#define YY_MIN_REDUCE        760
#define YY_MAX_REDUCE        1022
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
#define YY_ACTTAB_COUNT (668)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   176,  541,  176,  197,  303,   17,  135,  620,  174,  542,
 /*    10 */  1004,  204, 1005,   47,   48,   30,   51,   52,  135,  201,
 /*    20 */   209,   41,  176,   50,  253,   55,   53,   57,   54,  758,
 /*    30 */   305,  203, 1005,   46,   45,  279,  278,   44,   43,   42,
 /*    40 */    47,   48,  214,   51,   52,  906,  928,  209,   41,  541,
 /*    50 */    50,  253,   55,   53,   57,   54,  199,  542,  660,  903,
 /*    60 */    46,   45,  216,  135,   44,   43,   42,   48,  906,   51,
 /*    70 */    52,   30,  956,  209,   41,  917,   50,  253,   55,   53,
 /*    80 */    57,   54,  250,  130,   75,  289,   46,   45,  906,  236,
 /*    90 */    44,   43,   42,  495,  496,  497,  498,  499,  500,  501,
 /*   100 */   502,  503,  504,  505,  506,  507,  304,  925,   81,  226,
 /*   110 */    70,  541,  212,   47,   48,  903,   51,   52,   30,  542,
 /*   120 */   209,   41,   24,   50,  253,   55,   53,   57,   54,  957,
 /*   130 */    36,  248,  704,   46,   45,  135,  664,   44,   43,   42,
 /*   140 */    47,   49,  894,   51,   52,  241,  892,  209,   41,  229,
 /*   150 */    50,  253,   55,   53,   57,   54,  233,  232,  101,  213,
 /*   160 */    46,   45,  903,  289,   44,   43,   42,   23,  267,  298,
 /*   170 */   297,  266,  265,  264,  296,  263,  295,  294,  293,  262,
 /*   180 */   292,  291,  866,   30,  854,  855,  856,  857,  858,  859,
 /*   190 */   860,  861,  862,  863,  864,  865,  867,  868,   51,   52,
 /*   200 */   917,   76,  209,   41,  900,   50,  253,   55,   53,   57,
 /*   210 */    54,  299,   18,  180,  198,   46,   45,   30,  269,   44,
 /*   220 */    43,   42,  208,  717,  272,  269,  708,  903,  711,  185,
 /*   230 */   714, 1001,  208,  717,   69,  186,  708,  906,  711,   77,
 /*   240 */   714,  114,  113,  184, 1000,  644,  215,  221,  641,  657,
 /*   250 */   642,   71,  643,   12,  205,  206,   25,   80,  252,  145,
 /*   260 */    23,  902,  298,  297,  205,  206,  891,  296,  999,  295,
 /*   270 */   294,  293,   24,  292,  291,  874,  223,  224,  872,  873,
 /*   280 */    36,  193,  904,  875,  805,  877,  878,  876,  161,  879,
 /*   290 */   880,   55,   53,   57,   54,   67,  220,  619,  814,   46,
 /*   300 */    45,  235,  161,   44,   43,   42,   99,  104,  192,   44,
 /*   310 */    43,   42,   93,  103,  109,  112,  102,  239,   78,  254,
 /*   320 */   218,   31,  106,    5,  151,   56,    1,  149,  194,   33,
 /*   330 */   150,   88,   83,   87,   30,   56,  169,  165,  716,  128,
 /*   340 */    30,   30,  167,  164,  117,  116,  115,   36,  716,  889,
 /*   350 */   890,   29,  893,  715,  645,  806,   46,   45, 1014,  161,
 /*   360 */    44,   43,   42,  715,  222,  685,  686,  276,  275,  302,
 /*   370 */   301,  122,    3,  162,  706,  273,  672,  710,  903,  713,
 /*   380 */   132,  277,  281,  676,  903,  903,  652,   60,  219,  677,
 /*   390 */   207,  271,  737,   20,  238,  718,   19,   61,  709,   19,
 /*   400 */   712,   64,  630,  905,  256,  632,   31,  258,   31,   60,
 /*   410 */   707,   79,  631,  178,   28,  720,   60,  259,   62,  179,
 /*   420 */    65,   92,   91,  111,  110,   14,   13,   98,   97,   16,
 /*   430 */    15,  648,  181,  649,    6,  646,  175,  647,  127,  125,
 /*   440 */   182,  183,  189,  190,  967,  188,  173,  187,  966,  177,
 /*   450 */   210,  963,  962,  211,  280,  129,  919,  927,   39,  949,
 /*   460 */   948,  934,  936,  131,  146,  899,  144,  147,  148,   36,
 /*   470 */   817,  261,   37,  171,   34,  270,  813,  237, 1019,   89,
 /*   480 */  1018, 1016,  126,  152,  274, 1013,   95,  671,  242, 1012,
 /*   490 */   200, 1010,  153,  835,  246,   35,   32,  916,   38,  172,
 /*   500 */    63,   66,  802,  136,   58,  251,  137,  138,  249,  247,
 /*   510 */   105,  800,  107,  245,  108,  243,   40,  798,  139,  797,
 /*   520 */   290,  100,  225,  282,  163,  795,  283,  794,  793,  792,
 /*   530 */   791,  790,  166,  168,  787,  785,  783,  781,  779,  284,
 /*   540 */   170,  286,  240,   72,   73,  950,  285,  287,  288,  300,
 /*   550 */   756,  195,  227,  217,  260,  228,  755,  230,  196,  191,
 /*   560 */   231,   84,   85,  754,  742,  234,  796,  238,   74,  654,
 /*   570 */   673,  118,  789,  156,  119,  155,  836,  154,  157,  158,
 /*   580 */   160,  159,  120,  788,    2,  121,  780,    4,  870,  255,
 /*   590 */     8,  901,   68,  133,  202,  244,  140,  141,  142,  143,
 /*   600 */   882,  678,  134,   26,   27,    9,   10,  719,    7,   11,
 /*   610 */   721,   21,   22,  257,   82,  583,  579,   80,  577,  576,
 /*   620 */   575,  572,  545,  268,   86,   90,   31,   94,   59,   96,
 /*   630 */   622,  621,  618,  567,  565,  557,  563,  559,  561,  555,
 /*   640 */   553,  586,  585,  584,  582,  581,  580,  578,  574,  573,
 /*   650 */    60,  543,  511,  509,  760,  759,  759,  759,  759,  759,
 /*   660 */   759,  759,  759,  759,  759,  759,  123,  124,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   276,    1,  276,  215,  216,  276,  216,    5,  276,    9,
 /*    10 */   286,  285,  286,   13,   14,  216,   16,   17,  216,  235,
 /*    20 */    20,   21,  276,   23,   24,   25,   26,   27,   28,  213,
 /*    30 */   214,  285,  286,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  235,   16,   17,  261,  216,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  257,    9,   37,  260,
 /*    60 */    33,   34,  235,  216,   37,   38,   39,   14,  261,   16,
 /*    70 */    17,  216,  282,   20,   21,  259,   23,   24,   25,   26,
 /*    80 */    27,   28,  280,  216,  282,   81,   33,   34,  261,  273,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  277,  222,   61,
 /*   110 */   110,    1,  257,   13,   14,  260,   16,   17,  216,    9,
 /*   120 */    20,   21,  104,   23,   24,   25,   26,   27,   28,  282,
 /*   130 */   112,  284,  105,   33,   34,  216,  115,   37,   38,   39,
 /*   140 */    13,   14,  256,   16,   17,  278,    0,   20,   21,  134,
 /*   150 */    23,   24,   25,   26,   27,   28,  141,  142,   76,  257,
 /*   160 */    33,   34,  260,   81,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  234,  216,  236,  237,  238,  239,  240,  241,
 /*   190 */   242,  243,  244,  245,  246,  247,  248,  249,   16,   17,
 /*   200 */   259,  282,   20,   21,  216,   23,   24,   25,   26,   27,
 /*   210 */    28,  235,   44,  276,  273,   33,   34,  216,   79,   37,
 /*   220 */    38,   39,    1,    2,  257,   79,    5,  260,    7,   61,
 /*   230 */     9,  276,    1,    2,  222,   67,    5,  261,    7,  262,
 /*   240 */     9,   73,   74,   75,  276,    2,  258,  216,    5,  109,
 /*   250 */     7,  274,    9,  104,   33,   34,  116,  108,   37,  110,
 /*   260 */    88,  260,   90,   91,   33,   34,  254,   95,  276,   97,
 /*   270 */    98,   99,  104,  101,  102,  234,   33,   34,  237,  238,
 /*   280 */   112,  276,  251,  242,  221,  244,  245,  246,  225,  248,
 /*   290 */   249,   25,   26,   27,   28,  104,   67,  106,  221,   33,
 /*   300 */    34,  133,  225,   37,   38,   39,   62,   63,  140,   37,
 /*   310 */    38,   39,   68,   69,   70,   71,   72,  105,  222,   15,
 /*   320 */    67,  109,   78,   62,   63,  104,  223,  224,  276,   68,
 /*   330 */    69,   70,   71,   72,  216,  104,   62,   63,  117,  104,
 /*   340 */   216,  216,   68,   69,   70,   71,   72,  112,  117,  253,
 /*   350 */   254,  255,  256,  132,  111,  221,   33,   34,  261,  225,
 /*   360 */    37,   38,   39,  132,  135,  123,  124,  138,  139,   64,
 /*   370 */    65,   66,  219,  220,    1,  257,  105,    5,  260,    7,
 /*   380 */   109,  257,  257,  105,  260,  260,  105,  109,  135,  105,
 /*   390 */    60,  138,  105,  109,  113,  105,  109,  109,    5,  109,
 /*   400 */     7,  109,  105,  261,  105,  105,  109,  105,  109,  109,
 /*   410 */    37,  109,  105,  276,  104,  111,  109,  107,  130,  276,
 /*   420 */   128,  136,  137,   76,   77,  136,  137,  136,  137,  136,
 /*   430 */   137,    5,  276,    7,  104,    5,  276,    7,   62,   63,
 /*   440 */   276,  276,  276,  276,  252,  276,  276,  276,  252,  276,
 /*   450 */   252,  252,  252,  252,  252,  216,  259,  216,  275,  283,
 /*   460 */   283,  216,  216,  216,  216,  216,  263,  216,  216,  112,
 /*   470 */   216,  216,  216,  216,  216,  216,  216,  259,  216,  216,
 /*   480 */   216,  216,   60,  216,  216,  216,  216,  117,  279,  216,
 /*   490 */   279,  216,  216,  216,  279,  216,  216,  272,  216,  216,
 /*   500 */   129,  127,  216,  271,  126,  121,  270,  269,  125,  120,
 /*   510 */   216,  216,  216,  119,  216,  118,  131,  216,  268,  216,
 /*   520 */   103,   87,  216,   86,  216,  216,   50,  216,  216,  216,
 /*   530 */   216,  216,  216,  216,  216,  216,  216,  216,  216,   83,
 /*   540 */   216,   54,  217,  217,  217,  217,   85,   84,   82,   79,
 /*   550 */     5,  217,  143,  217,  217,    5,    5,  143,  217,  217,
 /*   560 */     5,  222,  222,    5,   89,  134,  217,  113,  109,  105,
 /*   570 */   105,  218,  217,  227,  218,  231,  233,  232,  230,  228,
 /*   580 */   226,  229,  218,  217,  223,  218,  217,  219,  250,  107,
 /*   590 */   104,  259,  114,  104,    1,  104,  267,  266,  265,  264,
 /*   600 */   250,  105,  104,  109,  109,  122,  122,  105,  104,  104,
 /*   610 */   111,  104,  104,  107,   76,    9,    5,  108,    5,    5,
 /*   620 */     5,    5,   80,   15,   76,  137,  109,  137,   16,  137,
 /*   630 */     5,    5,  105,    5,    5,    5,    5,    5,    5,    5,
 /*   640 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   650 */   109,   80,   60,   59,    0,  287,  287,  287,  287,  287,
 /*   660 */   287,  287,  287,  287,  287,  287,   21,   21,  287,  287,
 /*   670 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   680 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   690 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   700 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   710 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   720 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   730 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   740 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   750 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   760 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   770 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   780 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   790 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   800 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   810 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   820 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   830 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   840 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   850 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   860 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   870 */   287,  287,  287,  287,  287,  287,  287,  287,  287,  287,
 /*   880 */   287,
};
#define YY_SHIFT_COUNT    (305)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (654)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  172,  172,  139,  221,  231,  110,  110,
 /*    10 */   110,  110,  110,  110,  110,  110,  110,    0,   48,  231,
 /*    20 */   243,  243,  243,  243,   18,  110,  110,  110,  110,  146,
 /*    30 */   110,  110,   82,  139,    4,    4,  668,  668,  668,  231,
 /*    40 */   231,  231,  231,  231,  231,  231,  231,  231,  231,  231,
 /*    50 */   231,  231,  231,  231,  231,  231,  231,  231,  231,  243,
 /*    60 */   243,    2,    2,    2,    2,    2,    2,    2,  235,  110,
 /*    70 */   110,   21,  110,  110,  110,  242,  242,  140,  110,  110,
 /*    80 */   110,  110,  110,  110,  110,  110,  110,  110,  110,  110,
 /*    90 */   110,  110,  110,  110,  110,  110,  110,  110,  110,  110,
 /*   100 */   110,  110,  110,  110,  110,  110,  110,  110,  110,  110,
 /*   110 */   110,  110,  110,  110,  110,  110,  110,  110,  110,  110,
 /*   120 */   110,  110,  110,  110,  110,  110,  110,  110,  357,  422,
 /*   130 */   422,  422,  370,  370,  370,  422,  374,  371,  378,  384,
 /*   140 */   383,  389,  394,  397,  385,  357,  422,  422,  422,  417,
 /*   150 */   139,  139,  422,  422,  434,  437,  476,  456,  461,  487,
 /*   160 */   463,  466,  417,  422,  470,  470,  422,  470,  422,  470,
 /*   170 */   422,  668,  668,   27,  100,  127,  100,  100,   53,  182,
 /*   180 */   266,  266,  266,  266,  244,  261,  274,  323,  323,  323,
 /*   190 */   323,  229,   15,  272,  272,  149,  253,  305,  281,  212,
 /*   200 */   271,  278,  284,  287,  290,  372,  393,  373,  330,  304,
 /*   210 */   288,  292,  297,  299,  300,  302,  307,  310,  285,  289,
 /*   220 */   291,  191,  293,  426,  430,  347,  376,  545,  409,  550,
 /*   230 */   551,  414,  555,  558,  475,  431,  454,  464,  478,  482,
 /*   240 */   486,  459,  465,  489,  593,  491,  496,  498,  494,  483,
 /*   250 */   495,  484,  502,  504,  499,  505,  482,  507,  506,  508,
 /*   260 */   509,  538,  606,  611,  613,  614,  615,  616,  542,  608,
 /*   270 */   548,  488,  517,  517,  612,  490,  492,  517,  625,  626,
 /*   280 */   527,  517,  628,  629,  630,  631,  632,  633,  634,  635,
 /*   290 */   636,  637,  638,  639,  640,  641,  642,  643,  644,  541,
 /*   300 */   571,  645,  646,  592,  594,  654,
};
#define YY_REDUCE_COUNT (172)
#define YY_REDUCE_MIN   (-276)
#define YY_REDUCE_MAX   (369)
static const short yy_reduce_ofst[] = {
 /*     0 */  -184,  -52,  -52,   41,   41,   96, -274, -254, -201, -153,
 /*    10 */  -198, -145,  -98,  -33,  118,  124,  125, -170, -212, -276,
 /*    20 */  -216, -193, -173,  -24,  -59, -133, -210,  -81,  -12, -114,
 /*    30 */    31,    1,   63,   12,   77,  134,  -23,  103,  153, -271,
 /*    40 */  -268,  -63,  -45,  -32,   -8,    5,   52,  137,  143,  156,
 /*    50 */   160,  164,  165,  166,  167,  169,  170,  171,  173,   97,
 /*    60 */   142,  192,  196,  198,  199,  200,  201,  202,  197,  239,
 /*    70 */   241,  183,  245,  246,  247,  176,  177,  203,  248,  249,
 /*    80 */   251,  252,  254,  255,  256,  257,  258,  259,  260,  262,
 /*    90 */   263,  264,  265,  267,  268,  269,  270,  273,  275,  276,
 /*   100 */   277,  279,  280,  282,  283,  286,  294,  295,  296,  298,
 /*   110 */   301,  303,  306,  308,  309,  311,  312,  313,  314,  315,
 /*   120 */   316,  317,  318,  319,  320,  321,  322,  324,  218,  325,
 /*   130 */   326,  327,  209,  211,  215,  328,  225,  232,  236,  238,
 /*   140 */   250,  329,  331,  333,  335,  332,  334,  336,  337,  338,
 /*   150 */   339,  340,  341,  342,  343,  345,  344,  346,  348,  351,
 /*   160 */   352,  354,  350,  349,  353,  356,  355,  364,  366,  367,
 /*   170 */   369,  361,  368,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   757,  869,  815,  881,  803,  812, 1007, 1007,  757,  757,
 /*    10 */   757,  757,  757,  757,  757,  757,  757,  929,  776, 1007,
 /*    20 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  812,
 /*    30 */   757,  757,  818,  812,  818,  818,  924,  853,  871,  757,
 /*    40 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*    50 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*    60 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*    70 */   757,  931,  933,  935,  757,  953,  953,  922,  757,  757,
 /*    80 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*    90 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*   100 */   757,  757,  757,  757,  757,  801,  757,  799,  757,  757,
 /*   110 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*   120 */   757,  757,  786,  757,  757,  757,  757,  757,  757,  778,
 /*   130 */   778,  778,  757,  757,  757,  778,  960,  964,  958,  946,
 /*   140 */   954,  945,  941,  940,  968,  757,  778,  778,  778,  816,
 /*   150 */   812,  812,  778,  778,  834,  832,  830,  822,  828,  824,
 /*   160 */   826,  820,  804,  778,  810,  810,  778,  810,  778,  810,
 /*   170 */   778,  853,  871,  757,  969,  757, 1006,  959,  996,  995,
 /*   180 */  1002,  994,  993,  992,  757,  757,  757,  988,  989,  991,
 /*   190 */   990,  757,  757,  998,  997,  757,  757,  757,  757,  757,
 /*   200 */   757,  757,  757,  757,  757,  757,  757,  757,  971,  757,
 /*   210 */   965,  961,  757,  757,  757,  757,  757,  757,  757,  757,
 /*   220 */   757,  883,  757,  757,  757,  757,  757,  757,  757,  757,
 /*   230 */   757,  757,  757,  757,  757,  757,  921,  757,  757,  757,
 /*   240 */   757,  932,  757,  757,  757,  757,  757,  757,  955,  757,
 /*   250 */   947,  757,  757,  757,  757,  757,  895,  757,  757,  757,
 /*   260 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  757,
 /*   270 */   757,  757, 1017, 1015,  757,  757,  757, 1011,  757,  757,
 /*   280 */   757, 1009,  757,  757,  757,  757,  757,  757,  757,  757,
 /*   290 */   757,  757,  757,  757,  757,  757,  757,  757,  757,  837,
 /*   300 */   757,  784,  782,  757,  774,  757,
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
  /*  119 */ "FILL",
  /*  120 */ "SLIDING",
  /*  121 */ "ORDER",
  /*  122 */ "BY",
  /*  123 */ "ASC",
  /*  124 */ "DESC",
  /*  125 */ "GROUP",
  /*  126 */ "HAVING",
  /*  127 */ "LIMIT",
  /*  128 */ "OFFSET",
  /*  129 */ "SLIMIT",
  /*  130 */ "SOFFSET",
  /*  131 */ "WHERE",
  /*  132 */ "NOW",
  /*  133 */ "RESET",
  /*  134 */ "QUERY",
  /*  135 */ "ADD",
  /*  136 */ "COLUMN",
  /*  137 */ "TAG",
  /*  138 */ "CHANGE",
  /*  139 */ "SET",
  /*  140 */ "KILL",
  /*  141 */ "CONNECTION",
  /*  142 */ "STREAM",
  /*  143 */ "COLON",
  /*  144 */ "ABORT",
  /*  145 */ "AFTER",
  /*  146 */ "ATTACH",
  /*  147 */ "BEFORE",
  /*  148 */ "BEGIN",
  /*  149 */ "CASCADE",
  /*  150 */ "CLUSTER",
  /*  151 */ "CONFLICT",
  /*  152 */ "COPY",
  /*  153 */ "DEFERRED",
  /*  154 */ "DELIMITERS",
  /*  155 */ "DETACH",
  /*  156 */ "EACH",
  /*  157 */ "END",
  /*  158 */ "EXPLAIN",
  /*  159 */ "FAIL",
  /*  160 */ "FOR",
  /*  161 */ "IGNORE",
  /*  162 */ "IMMEDIATE",
  /*  163 */ "INITIALLY",
  /*  164 */ "INSTEAD",
  /*  165 */ "MATCH",
  /*  166 */ "KEY",
  /*  167 */ "OF",
  /*  168 */ "RAISE",
  /*  169 */ "REPLACE",
  /*  170 */ "RESTRICT",
  /*  171 */ "ROW",
  /*  172 */ "STATEMENT",
  /*  173 */ "TRIGGER",
  /*  174 */ "VIEW",
  /*  175 */ "COUNT",
  /*  176 */ "SUM",
  /*  177 */ "AVG",
  /*  178 */ "MIN",
  /*  179 */ "MAX",
  /*  180 */ "FIRST",
  /*  181 */ "LAST",
  /*  182 */ "TOP",
  /*  183 */ "BOTTOM",
  /*  184 */ "STDDEV",
  /*  185 */ "PERCENTILE",
  /*  186 */ "APERCENTILE",
  /*  187 */ "LEASTSQUARES",
  /*  188 */ "HISTOGRAM",
  /*  189 */ "DIFF",
  /*  190 */ "SPREAD",
  /*  191 */ "TWA",
  /*  192 */ "INTERP",
  /*  193 */ "LAST_ROW",
  /*  194 */ "RATE",
  /*  195 */ "IRATE",
  /*  196 */ "SUM_RATE",
  /*  197 */ "SUM_IRATE",
  /*  198 */ "AVG_RATE",
  /*  199 */ "AVG_IRATE",
  /*  200 */ "TBID",
  /*  201 */ "SEMI",
  /*  202 */ "NONE",
  /*  203 */ "PREV",
  /*  204 */ "LINEAR",
  /*  205 */ "IMPORT",
  /*  206 */ "METRIC",
  /*  207 */ "TBNAME",
  /*  208 */ "JOIN",
  /*  209 */ "METRICS",
  /*  210 */ "INSERT",
  /*  211 */ "INTO",
  /*  212 */ "VALUES",
  /*  213 */ "program",
  /*  214 */ "cmd",
  /*  215 */ "dbPrefix",
  /*  216 */ "ids",
  /*  217 */ "cpxName",
  /*  218 */ "ifexists",
  /*  219 */ "alter_db_optr",
  /*  220 */ "alter_topic_optr",
  /*  221 */ "acct_optr",
  /*  222 */ "ifnotexists",
  /*  223 */ "db_optr",
  /*  224 */ "topic_optr",
  /*  225 */ "pps",
  /*  226 */ "tseries",
  /*  227 */ "dbs",
  /*  228 */ "streams",
  /*  229 */ "storage",
  /*  230 */ "qtime",
  /*  231 */ "users",
  /*  232 */ "conns",
  /*  233 */ "state",
  /*  234 */ "keep",
  /*  235 */ "tagitemlist",
  /*  236 */ "cache",
  /*  237 */ "replica",
  /*  238 */ "quorum",
  /*  239 */ "days",
  /*  240 */ "minrows",
  /*  241 */ "maxrows",
  /*  242 */ "blocks",
  /*  243 */ "ctime",
  /*  244 */ "wal",
  /*  245 */ "fsync",
  /*  246 */ "comp",
  /*  247 */ "prec",
  /*  248 */ "update",
  /*  249 */ "cachelast",
  /*  250 */ "partitions",
  /*  251 */ "typename",
  /*  252 */ "signed",
  /*  253 */ "create_table_args",
  /*  254 */ "create_stable_args",
  /*  255 */ "create_table_list",
  /*  256 */ "create_from_stable",
  /*  257 */ "columnlist",
  /*  258 */ "tagNamelist",
  /*  259 */ "select",
  /*  260 */ "column",
  /*  261 */ "tagitem",
  /*  262 */ "selcollist",
  /*  263 */ "from",
  /*  264 */ "where_opt",
  /*  265 */ "interval_opt",
  /*  266 */ "fill_opt",
  /*  267 */ "sliding_opt",
  /*  268 */ "groupby_opt",
  /*  269 */ "orderby_opt",
  /*  270 */ "having_opt",
  /*  271 */ "slimit_opt",
  /*  272 */ "limit_opt",
  /*  273 */ "union",
  /*  274 */ "sclp",
  /*  275 */ "distinct",
  /*  276 */ "expr",
  /*  277 */ "as",
  /*  278 */ "tablelist",
  /*  279 */ "tmvar",
  /*  280 */ "sortlist",
  /*  281 */ "sortitem",
  /*  282 */ "item",
  /*  283 */ "sortorder",
  /*  284 */ "grouplist",
  /*  285 */ "exprlist",
  /*  286 */ "expritem",
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
 /* 156 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 157 */ "union ::= select",
 /* 158 */ "union ::= LP union RP",
 /* 159 */ "union ::= union UNION ALL select",
 /* 160 */ "union ::= union UNION ALL LP select RP",
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
 /* 173 */ "tablelist ::= ids cpxName",
 /* 174 */ "tablelist ::= ids cpxName ids",
 /* 175 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 176 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 177 */ "tmvar ::= VARIABLE",
 /* 178 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 179 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 180 */ "interval_opt ::=",
 /* 181 */ "fill_opt ::=",
 /* 182 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 183 */ "fill_opt ::= FILL LP ID RP",
 /* 184 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 185 */ "sliding_opt ::=",
 /* 186 */ "orderby_opt ::=",
 /* 187 */ "orderby_opt ::= ORDER BY sortlist",
 /* 188 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 189 */ "sortlist ::= item sortorder",
 /* 190 */ "item ::= ids cpxName",
 /* 191 */ "sortorder ::= ASC",
 /* 192 */ "sortorder ::= DESC",
 /* 193 */ "sortorder ::=",
 /* 194 */ "groupby_opt ::=",
 /* 195 */ "groupby_opt ::= GROUP BY grouplist",
 /* 196 */ "grouplist ::= grouplist COMMA item",
 /* 197 */ "grouplist ::= item",
 /* 198 */ "having_opt ::=",
 /* 199 */ "having_opt ::= HAVING expr",
 /* 200 */ "limit_opt ::=",
 /* 201 */ "limit_opt ::= LIMIT signed",
 /* 202 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 203 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 204 */ "slimit_opt ::=",
 /* 205 */ "slimit_opt ::= SLIMIT signed",
 /* 206 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 207 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 208 */ "where_opt ::=",
 /* 209 */ "where_opt ::= WHERE expr",
 /* 210 */ "expr ::= LP expr RP",
 /* 211 */ "expr ::= ID",
 /* 212 */ "expr ::= ID DOT ID",
 /* 213 */ "expr ::= ID DOT STAR",
 /* 214 */ "expr ::= INTEGER",
 /* 215 */ "expr ::= MINUS INTEGER",
 /* 216 */ "expr ::= PLUS INTEGER",
 /* 217 */ "expr ::= FLOAT",
 /* 218 */ "expr ::= MINUS FLOAT",
 /* 219 */ "expr ::= PLUS FLOAT",
 /* 220 */ "expr ::= STRING",
 /* 221 */ "expr ::= NOW",
 /* 222 */ "expr ::= VARIABLE",
 /* 223 */ "expr ::= BOOL",
 /* 224 */ "expr ::= ID LP exprlist RP",
 /* 225 */ "expr ::= ID LP STAR RP",
 /* 226 */ "expr ::= expr IS NULL",
 /* 227 */ "expr ::= expr IS NOT NULL",
 /* 228 */ "expr ::= expr LT expr",
 /* 229 */ "expr ::= expr GT expr",
 /* 230 */ "expr ::= expr LE expr",
 /* 231 */ "expr ::= expr GE expr",
 /* 232 */ "expr ::= expr NE expr",
 /* 233 */ "expr ::= expr EQ expr",
 /* 234 */ "expr ::= expr BETWEEN expr AND expr",
 /* 235 */ "expr ::= expr AND expr",
 /* 236 */ "expr ::= expr OR expr",
 /* 237 */ "expr ::= expr PLUS expr",
 /* 238 */ "expr ::= expr MINUS expr",
 /* 239 */ "expr ::= expr STAR expr",
 /* 240 */ "expr ::= expr SLASH expr",
 /* 241 */ "expr ::= expr REM expr",
 /* 242 */ "expr ::= expr LIKE expr",
 /* 243 */ "expr ::= expr IN LP exprlist RP",
 /* 244 */ "exprlist ::= exprlist COMMA expritem",
 /* 245 */ "exprlist ::= expritem",
 /* 246 */ "expritem ::= expr",
 /* 247 */ "expritem ::=",
 /* 248 */ "cmd ::= RESET QUERY CACHE",
 /* 249 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 250 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 251 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 252 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 253 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 254 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 255 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 256 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 257 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 258 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 259 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 260 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 261 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 262 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 234: /* keep */
    case 235: /* tagitemlist */
    case 257: /* columnlist */
    case 258: /* tagNamelist */
    case 266: /* fill_opt */
    case 268: /* groupby_opt */
    case 269: /* orderby_opt */
    case 280: /* sortlist */
    case 284: /* grouplist */
{
taosArrayDestroy((yypminor->yy207));
}
      break;
    case 255: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy414));
}
      break;
    case 259: /* select */
{
doDestroyQuerySql((yypminor->yy526));
}
      break;
    case 262: /* selcollist */
    case 274: /* sclp */
    case 285: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy178));
}
      break;
    case 264: /* where_opt */
    case 270: /* having_opt */
    case 276: /* expr */
    case 286: /* expritem */
{
tSqlExprDestroy((yypminor->yy484));
}
      break;
    case 273: /* union */
{
destroyAllSelectClause((yypminor->yy441));
}
      break;
    case 281: /* sortitem */
{
tVariantDestroy(&(yypminor->yy232));
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
   213,  /* (0) program ::= cmd */
   214,  /* (1) cmd ::= SHOW DATABASES */
   214,  /* (2) cmd ::= SHOW TOPICS */
   214,  /* (3) cmd ::= SHOW MNODES */
   214,  /* (4) cmd ::= SHOW DNODES */
   214,  /* (5) cmd ::= SHOW ACCOUNTS */
   214,  /* (6) cmd ::= SHOW USERS */
   214,  /* (7) cmd ::= SHOW MODULES */
   214,  /* (8) cmd ::= SHOW QUERIES */
   214,  /* (9) cmd ::= SHOW CONNECTIONS */
   214,  /* (10) cmd ::= SHOW STREAMS */
   214,  /* (11) cmd ::= SHOW VARIABLES */
   214,  /* (12) cmd ::= SHOW SCORES */
   214,  /* (13) cmd ::= SHOW GRANTS */
   214,  /* (14) cmd ::= SHOW VNODES */
   214,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
   215,  /* (16) dbPrefix ::= */
   215,  /* (17) dbPrefix ::= ids DOT */
   217,  /* (18) cpxName ::= */
   217,  /* (19) cpxName ::= DOT ids */
   214,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   214,  /* (21) cmd ::= SHOW CREATE DATABASE ids */
   214,  /* (22) cmd ::= SHOW dbPrefix TABLES */
   214,  /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   214,  /* (24) cmd ::= SHOW dbPrefix STABLES */
   214,  /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   214,  /* (26) cmd ::= SHOW dbPrefix VGROUPS */
   214,  /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
   214,  /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
   214,  /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
   214,  /* (30) cmd ::= DROP DATABASE ifexists ids */
   214,  /* (31) cmd ::= DROP TOPIC ifexists ids */
   214,  /* (32) cmd ::= DROP DNODE ids */
   214,  /* (33) cmd ::= DROP USER ids */
   214,  /* (34) cmd ::= DROP ACCOUNT ids */
   214,  /* (35) cmd ::= USE ids */
   214,  /* (36) cmd ::= DESCRIBE ids cpxName */
   214,  /* (37) cmd ::= ALTER USER ids PASS ids */
   214,  /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
   214,  /* (39) cmd ::= ALTER DNODE ids ids */
   214,  /* (40) cmd ::= ALTER DNODE ids ids ids */
   214,  /* (41) cmd ::= ALTER LOCAL ids */
   214,  /* (42) cmd ::= ALTER LOCAL ids ids */
   214,  /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
   214,  /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
   214,  /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
   214,  /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   216,  /* (47) ids ::= ID */
   216,  /* (48) ids ::= STRING */
   218,  /* (49) ifexists ::= IF EXISTS */
   218,  /* (50) ifexists ::= */
   222,  /* (51) ifnotexists ::= IF NOT EXISTS */
   222,  /* (52) ifnotexists ::= */
   214,  /* (53) cmd ::= CREATE DNODE ids */
   214,  /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   214,  /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   214,  /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   214,  /* (57) cmd ::= CREATE USER ids PASS ids */
   225,  /* (58) pps ::= */
   225,  /* (59) pps ::= PPS INTEGER */
   226,  /* (60) tseries ::= */
   226,  /* (61) tseries ::= TSERIES INTEGER */
   227,  /* (62) dbs ::= */
   227,  /* (63) dbs ::= DBS INTEGER */
   228,  /* (64) streams ::= */
   228,  /* (65) streams ::= STREAMS INTEGER */
   229,  /* (66) storage ::= */
   229,  /* (67) storage ::= STORAGE INTEGER */
   230,  /* (68) qtime ::= */
   230,  /* (69) qtime ::= QTIME INTEGER */
   231,  /* (70) users ::= */
   231,  /* (71) users ::= USERS INTEGER */
   232,  /* (72) conns ::= */
   232,  /* (73) conns ::= CONNS INTEGER */
   233,  /* (74) state ::= */
   233,  /* (75) state ::= STATE ids */
   221,  /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   234,  /* (77) keep ::= KEEP tagitemlist */
   236,  /* (78) cache ::= CACHE INTEGER */
   237,  /* (79) replica ::= REPLICA INTEGER */
   238,  /* (80) quorum ::= QUORUM INTEGER */
   239,  /* (81) days ::= DAYS INTEGER */
   240,  /* (82) minrows ::= MINROWS INTEGER */
   241,  /* (83) maxrows ::= MAXROWS INTEGER */
   242,  /* (84) blocks ::= BLOCKS INTEGER */
   243,  /* (85) ctime ::= CTIME INTEGER */
   244,  /* (86) wal ::= WAL INTEGER */
   245,  /* (87) fsync ::= FSYNC INTEGER */
   246,  /* (88) comp ::= COMP INTEGER */
   247,  /* (89) prec ::= PRECISION STRING */
   248,  /* (90) update ::= UPDATE INTEGER */
   249,  /* (91) cachelast ::= CACHELAST INTEGER */
   250,  /* (92) partitions ::= PARTITIONS INTEGER */
   223,  /* (93) db_optr ::= */
   223,  /* (94) db_optr ::= db_optr cache */
   223,  /* (95) db_optr ::= db_optr replica */
   223,  /* (96) db_optr ::= db_optr quorum */
   223,  /* (97) db_optr ::= db_optr days */
   223,  /* (98) db_optr ::= db_optr minrows */
   223,  /* (99) db_optr ::= db_optr maxrows */
   223,  /* (100) db_optr ::= db_optr blocks */
   223,  /* (101) db_optr ::= db_optr ctime */
   223,  /* (102) db_optr ::= db_optr wal */
   223,  /* (103) db_optr ::= db_optr fsync */
   223,  /* (104) db_optr ::= db_optr comp */
   223,  /* (105) db_optr ::= db_optr prec */
   223,  /* (106) db_optr ::= db_optr keep */
   223,  /* (107) db_optr ::= db_optr update */
   223,  /* (108) db_optr ::= db_optr cachelast */
   224,  /* (109) topic_optr ::= db_optr */
   224,  /* (110) topic_optr ::= topic_optr partitions */
   219,  /* (111) alter_db_optr ::= */
   219,  /* (112) alter_db_optr ::= alter_db_optr replica */
   219,  /* (113) alter_db_optr ::= alter_db_optr quorum */
   219,  /* (114) alter_db_optr ::= alter_db_optr keep */
   219,  /* (115) alter_db_optr ::= alter_db_optr blocks */
   219,  /* (116) alter_db_optr ::= alter_db_optr comp */
   219,  /* (117) alter_db_optr ::= alter_db_optr wal */
   219,  /* (118) alter_db_optr ::= alter_db_optr fsync */
   219,  /* (119) alter_db_optr ::= alter_db_optr update */
   219,  /* (120) alter_db_optr ::= alter_db_optr cachelast */
   220,  /* (121) alter_topic_optr ::= alter_db_optr */
   220,  /* (122) alter_topic_optr ::= alter_topic_optr partitions */
   251,  /* (123) typename ::= ids */
   251,  /* (124) typename ::= ids LP signed RP */
   251,  /* (125) typename ::= ids UNSIGNED */
   252,  /* (126) signed ::= INTEGER */
   252,  /* (127) signed ::= PLUS INTEGER */
   252,  /* (128) signed ::= MINUS INTEGER */
   214,  /* (129) cmd ::= CREATE TABLE create_table_args */
   214,  /* (130) cmd ::= CREATE TABLE create_stable_args */
   214,  /* (131) cmd ::= CREATE STABLE create_stable_args */
   214,  /* (132) cmd ::= CREATE TABLE create_table_list */
   255,  /* (133) create_table_list ::= create_from_stable */
   255,  /* (134) create_table_list ::= create_table_list create_from_stable */
   253,  /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   254,  /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   256,  /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   256,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   258,  /* (139) tagNamelist ::= tagNamelist COMMA ids */
   258,  /* (140) tagNamelist ::= ids */
   253,  /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
   257,  /* (142) columnlist ::= columnlist COMMA column */
   257,  /* (143) columnlist ::= column */
   260,  /* (144) column ::= ids typename */
   235,  /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
   235,  /* (146) tagitemlist ::= tagitem */
   261,  /* (147) tagitem ::= INTEGER */
   261,  /* (148) tagitem ::= FLOAT */
   261,  /* (149) tagitem ::= STRING */
   261,  /* (150) tagitem ::= BOOL */
   261,  /* (151) tagitem ::= NULL */
   261,  /* (152) tagitem ::= MINUS INTEGER */
   261,  /* (153) tagitem ::= MINUS FLOAT */
   261,  /* (154) tagitem ::= PLUS INTEGER */
   261,  /* (155) tagitem ::= PLUS FLOAT */
   259,  /* (156) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   273,  /* (157) union ::= select */
   273,  /* (158) union ::= LP union RP */
   273,  /* (159) union ::= union UNION ALL select */
   273,  /* (160) union ::= union UNION ALL LP select RP */
   214,  /* (161) cmd ::= union */
   259,  /* (162) select ::= SELECT selcollist */
   274,  /* (163) sclp ::= selcollist COMMA */
   274,  /* (164) sclp ::= */
   262,  /* (165) selcollist ::= sclp distinct expr as */
   262,  /* (166) selcollist ::= sclp STAR */
   277,  /* (167) as ::= AS ids */
   277,  /* (168) as ::= ids */
   277,  /* (169) as ::= */
   275,  /* (170) distinct ::= DISTINCT */
   275,  /* (171) distinct ::= */
   263,  /* (172) from ::= FROM tablelist */
   278,  /* (173) tablelist ::= ids cpxName */
   278,  /* (174) tablelist ::= ids cpxName ids */
   278,  /* (175) tablelist ::= tablelist COMMA ids cpxName */
   278,  /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
   279,  /* (177) tmvar ::= VARIABLE */
   265,  /* (178) interval_opt ::= INTERVAL LP tmvar RP */
   265,  /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   265,  /* (180) interval_opt ::= */
   266,  /* (181) fill_opt ::= */
   266,  /* (182) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   266,  /* (183) fill_opt ::= FILL LP ID RP */
   267,  /* (184) sliding_opt ::= SLIDING LP tmvar RP */
   267,  /* (185) sliding_opt ::= */
   269,  /* (186) orderby_opt ::= */
   269,  /* (187) orderby_opt ::= ORDER BY sortlist */
   280,  /* (188) sortlist ::= sortlist COMMA item sortorder */
   280,  /* (189) sortlist ::= item sortorder */
   282,  /* (190) item ::= ids cpxName */
   283,  /* (191) sortorder ::= ASC */
   283,  /* (192) sortorder ::= DESC */
   283,  /* (193) sortorder ::= */
   268,  /* (194) groupby_opt ::= */
   268,  /* (195) groupby_opt ::= GROUP BY grouplist */
   284,  /* (196) grouplist ::= grouplist COMMA item */
   284,  /* (197) grouplist ::= item */
   270,  /* (198) having_opt ::= */
   270,  /* (199) having_opt ::= HAVING expr */
   272,  /* (200) limit_opt ::= */
   272,  /* (201) limit_opt ::= LIMIT signed */
   272,  /* (202) limit_opt ::= LIMIT signed OFFSET signed */
   272,  /* (203) limit_opt ::= LIMIT signed COMMA signed */
   271,  /* (204) slimit_opt ::= */
   271,  /* (205) slimit_opt ::= SLIMIT signed */
   271,  /* (206) slimit_opt ::= SLIMIT signed SOFFSET signed */
   271,  /* (207) slimit_opt ::= SLIMIT signed COMMA signed */
   264,  /* (208) where_opt ::= */
   264,  /* (209) where_opt ::= WHERE expr */
   276,  /* (210) expr ::= LP expr RP */
   276,  /* (211) expr ::= ID */
   276,  /* (212) expr ::= ID DOT ID */
   276,  /* (213) expr ::= ID DOT STAR */
   276,  /* (214) expr ::= INTEGER */
   276,  /* (215) expr ::= MINUS INTEGER */
   276,  /* (216) expr ::= PLUS INTEGER */
   276,  /* (217) expr ::= FLOAT */
   276,  /* (218) expr ::= MINUS FLOAT */
   276,  /* (219) expr ::= PLUS FLOAT */
   276,  /* (220) expr ::= STRING */
   276,  /* (221) expr ::= NOW */
   276,  /* (222) expr ::= VARIABLE */
   276,  /* (223) expr ::= BOOL */
   276,  /* (224) expr ::= ID LP exprlist RP */
   276,  /* (225) expr ::= ID LP STAR RP */
   276,  /* (226) expr ::= expr IS NULL */
   276,  /* (227) expr ::= expr IS NOT NULL */
   276,  /* (228) expr ::= expr LT expr */
   276,  /* (229) expr ::= expr GT expr */
   276,  /* (230) expr ::= expr LE expr */
   276,  /* (231) expr ::= expr GE expr */
   276,  /* (232) expr ::= expr NE expr */
   276,  /* (233) expr ::= expr EQ expr */
   276,  /* (234) expr ::= expr BETWEEN expr AND expr */
   276,  /* (235) expr ::= expr AND expr */
   276,  /* (236) expr ::= expr OR expr */
   276,  /* (237) expr ::= expr PLUS expr */
   276,  /* (238) expr ::= expr MINUS expr */
   276,  /* (239) expr ::= expr STAR expr */
   276,  /* (240) expr ::= expr SLASH expr */
   276,  /* (241) expr ::= expr REM expr */
   276,  /* (242) expr ::= expr LIKE expr */
   276,  /* (243) expr ::= expr IN LP exprlist RP */
   285,  /* (244) exprlist ::= exprlist COMMA expritem */
   285,  /* (245) exprlist ::= expritem */
   286,  /* (246) expritem ::= expr */
   286,  /* (247) expritem ::= */
   214,  /* (248) cmd ::= RESET QUERY CACHE */
   214,  /* (249) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   214,  /* (250) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   214,  /* (251) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   214,  /* (252) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   214,  /* (253) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   214,  /* (254) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   214,  /* (255) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   214,  /* (256) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   214,  /* (257) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   214,  /* (258) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   214,  /* (259) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   214,  /* (260) cmd ::= KILL CONNECTION INTEGER */
   214,  /* (261) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   214,  /* (262) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW TOPICS */
   -2,  /* (3) cmd ::= SHOW MNODES */
   -2,  /* (4) cmd ::= SHOW DNODES */
   -2,  /* (5) cmd ::= SHOW ACCOUNTS */
   -2,  /* (6) cmd ::= SHOW USERS */
   -2,  /* (7) cmd ::= SHOW MODULES */
   -2,  /* (8) cmd ::= SHOW QUERIES */
   -2,  /* (9) cmd ::= SHOW CONNECTIONS */
   -2,  /* (10) cmd ::= SHOW STREAMS */
   -2,  /* (11) cmd ::= SHOW VARIABLES */
   -2,  /* (12) cmd ::= SHOW SCORES */
   -2,  /* (13) cmd ::= SHOW GRANTS */
   -2,  /* (14) cmd ::= SHOW VNODES */
   -3,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (16) dbPrefix ::= */
   -2,  /* (17) dbPrefix ::= ids DOT */
    0,  /* (18) cpxName ::= */
   -2,  /* (19) cpxName ::= DOT ids */
   -5,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   -4,  /* (21) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (22) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (24) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (26) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (30) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (31) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (32) cmd ::= DROP DNODE ids */
   -3,  /* (33) cmd ::= DROP USER ids */
   -3,  /* (34) cmd ::= DROP ACCOUNT ids */
   -2,  /* (35) cmd ::= USE ids */
   -3,  /* (36) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (37) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (39) cmd ::= ALTER DNODE ids ids */
   -5,  /* (40) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (41) cmd ::= ALTER LOCAL ids */
   -4,  /* (42) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (47) ids ::= ID */
   -1,  /* (48) ids ::= STRING */
   -2,  /* (49) ifexists ::= IF EXISTS */
    0,  /* (50) ifexists ::= */
   -3,  /* (51) ifnotexists ::= IF NOT EXISTS */
    0,  /* (52) ifnotexists ::= */
   -3,  /* (53) cmd ::= CREATE DNODE ids */
   -6,  /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -5,  /* (57) cmd ::= CREATE USER ids PASS ids */
    0,  /* (58) pps ::= */
   -2,  /* (59) pps ::= PPS INTEGER */
    0,  /* (60) tseries ::= */
   -2,  /* (61) tseries ::= TSERIES INTEGER */
    0,  /* (62) dbs ::= */
   -2,  /* (63) dbs ::= DBS INTEGER */
    0,  /* (64) streams ::= */
   -2,  /* (65) streams ::= STREAMS INTEGER */
    0,  /* (66) storage ::= */
   -2,  /* (67) storage ::= STORAGE INTEGER */
    0,  /* (68) qtime ::= */
   -2,  /* (69) qtime ::= QTIME INTEGER */
    0,  /* (70) users ::= */
   -2,  /* (71) users ::= USERS INTEGER */
    0,  /* (72) conns ::= */
   -2,  /* (73) conns ::= CONNS INTEGER */
    0,  /* (74) state ::= */
   -2,  /* (75) state ::= STATE ids */
   -9,  /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (77) keep ::= KEEP tagitemlist */
   -2,  /* (78) cache ::= CACHE INTEGER */
   -2,  /* (79) replica ::= REPLICA INTEGER */
   -2,  /* (80) quorum ::= QUORUM INTEGER */
   -2,  /* (81) days ::= DAYS INTEGER */
   -2,  /* (82) minrows ::= MINROWS INTEGER */
   -2,  /* (83) maxrows ::= MAXROWS INTEGER */
   -2,  /* (84) blocks ::= BLOCKS INTEGER */
   -2,  /* (85) ctime ::= CTIME INTEGER */
   -2,  /* (86) wal ::= WAL INTEGER */
   -2,  /* (87) fsync ::= FSYNC INTEGER */
   -2,  /* (88) comp ::= COMP INTEGER */
   -2,  /* (89) prec ::= PRECISION STRING */
   -2,  /* (90) update ::= UPDATE INTEGER */
   -2,  /* (91) cachelast ::= CACHELAST INTEGER */
   -2,  /* (92) partitions ::= PARTITIONS INTEGER */
    0,  /* (93) db_optr ::= */
   -2,  /* (94) db_optr ::= db_optr cache */
   -2,  /* (95) db_optr ::= db_optr replica */
   -2,  /* (96) db_optr ::= db_optr quorum */
   -2,  /* (97) db_optr ::= db_optr days */
   -2,  /* (98) db_optr ::= db_optr minrows */
   -2,  /* (99) db_optr ::= db_optr maxrows */
   -2,  /* (100) db_optr ::= db_optr blocks */
   -2,  /* (101) db_optr ::= db_optr ctime */
   -2,  /* (102) db_optr ::= db_optr wal */
   -2,  /* (103) db_optr ::= db_optr fsync */
   -2,  /* (104) db_optr ::= db_optr comp */
   -2,  /* (105) db_optr ::= db_optr prec */
   -2,  /* (106) db_optr ::= db_optr keep */
   -2,  /* (107) db_optr ::= db_optr update */
   -2,  /* (108) db_optr ::= db_optr cachelast */
   -1,  /* (109) topic_optr ::= db_optr */
   -2,  /* (110) topic_optr ::= topic_optr partitions */
    0,  /* (111) alter_db_optr ::= */
   -2,  /* (112) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (113) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (114) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (115) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (116) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (117) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (118) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (119) alter_db_optr ::= alter_db_optr update */
   -2,  /* (120) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (121) alter_topic_optr ::= alter_db_optr */
   -2,  /* (122) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (123) typename ::= ids */
   -4,  /* (124) typename ::= ids LP signed RP */
   -2,  /* (125) typename ::= ids UNSIGNED */
   -1,  /* (126) signed ::= INTEGER */
   -2,  /* (127) signed ::= PLUS INTEGER */
   -2,  /* (128) signed ::= MINUS INTEGER */
   -3,  /* (129) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (130) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (131) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (132) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (133) create_table_list ::= create_from_stable */
   -2,  /* (134) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (139) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (140) tagNamelist ::= ids */
   -5,  /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (142) columnlist ::= columnlist COMMA column */
   -1,  /* (143) columnlist ::= column */
   -2,  /* (144) column ::= ids typename */
   -3,  /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (146) tagitemlist ::= tagitem */
   -1,  /* (147) tagitem ::= INTEGER */
   -1,  /* (148) tagitem ::= FLOAT */
   -1,  /* (149) tagitem ::= STRING */
   -1,  /* (150) tagitem ::= BOOL */
   -1,  /* (151) tagitem ::= NULL */
   -2,  /* (152) tagitem ::= MINUS INTEGER */
   -2,  /* (153) tagitem ::= MINUS FLOAT */
   -2,  /* (154) tagitem ::= PLUS INTEGER */
   -2,  /* (155) tagitem ::= PLUS FLOAT */
  -12,  /* (156) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -1,  /* (157) union ::= select */
   -3,  /* (158) union ::= LP union RP */
   -4,  /* (159) union ::= union UNION ALL select */
   -6,  /* (160) union ::= union UNION ALL LP select RP */
   -1,  /* (161) cmd ::= union */
   -2,  /* (162) select ::= SELECT selcollist */
   -2,  /* (163) sclp ::= selcollist COMMA */
    0,  /* (164) sclp ::= */
   -4,  /* (165) selcollist ::= sclp distinct expr as */
   -2,  /* (166) selcollist ::= sclp STAR */
   -2,  /* (167) as ::= AS ids */
   -1,  /* (168) as ::= ids */
    0,  /* (169) as ::= */
   -1,  /* (170) distinct ::= DISTINCT */
    0,  /* (171) distinct ::= */
   -2,  /* (172) from ::= FROM tablelist */
   -2,  /* (173) tablelist ::= ids cpxName */
   -3,  /* (174) tablelist ::= ids cpxName ids */
   -4,  /* (175) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (177) tmvar ::= VARIABLE */
   -4,  /* (178) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (180) interval_opt ::= */
    0,  /* (181) fill_opt ::= */
   -6,  /* (182) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (183) fill_opt ::= FILL LP ID RP */
   -4,  /* (184) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (185) sliding_opt ::= */
    0,  /* (186) orderby_opt ::= */
   -3,  /* (187) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (188) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (189) sortlist ::= item sortorder */
   -2,  /* (190) item ::= ids cpxName */
   -1,  /* (191) sortorder ::= ASC */
   -1,  /* (192) sortorder ::= DESC */
    0,  /* (193) sortorder ::= */
    0,  /* (194) groupby_opt ::= */
   -3,  /* (195) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (196) grouplist ::= grouplist COMMA item */
   -1,  /* (197) grouplist ::= item */
    0,  /* (198) having_opt ::= */
   -2,  /* (199) having_opt ::= HAVING expr */
    0,  /* (200) limit_opt ::= */
   -2,  /* (201) limit_opt ::= LIMIT signed */
   -4,  /* (202) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (203) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (204) slimit_opt ::= */
   -2,  /* (205) slimit_opt ::= SLIMIT signed */
   -4,  /* (206) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (207) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (208) where_opt ::= */
   -2,  /* (209) where_opt ::= WHERE expr */
   -3,  /* (210) expr ::= LP expr RP */
   -1,  /* (211) expr ::= ID */
   -3,  /* (212) expr ::= ID DOT ID */
   -3,  /* (213) expr ::= ID DOT STAR */
   -1,  /* (214) expr ::= INTEGER */
   -2,  /* (215) expr ::= MINUS INTEGER */
   -2,  /* (216) expr ::= PLUS INTEGER */
   -1,  /* (217) expr ::= FLOAT */
   -2,  /* (218) expr ::= MINUS FLOAT */
   -2,  /* (219) expr ::= PLUS FLOAT */
   -1,  /* (220) expr ::= STRING */
   -1,  /* (221) expr ::= NOW */
   -1,  /* (222) expr ::= VARIABLE */
   -1,  /* (223) expr ::= BOOL */
   -4,  /* (224) expr ::= ID LP exprlist RP */
   -4,  /* (225) expr ::= ID LP STAR RP */
   -3,  /* (226) expr ::= expr IS NULL */
   -4,  /* (227) expr ::= expr IS NOT NULL */
   -3,  /* (228) expr ::= expr LT expr */
   -3,  /* (229) expr ::= expr GT expr */
   -3,  /* (230) expr ::= expr LE expr */
   -3,  /* (231) expr ::= expr GE expr */
   -3,  /* (232) expr ::= expr NE expr */
   -3,  /* (233) expr ::= expr EQ expr */
   -5,  /* (234) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (235) expr ::= expr AND expr */
   -3,  /* (236) expr ::= expr OR expr */
   -3,  /* (237) expr ::= expr PLUS expr */
   -3,  /* (238) expr ::= expr MINUS expr */
   -3,  /* (239) expr ::= expr STAR expr */
   -3,  /* (240) expr ::= expr SLASH expr */
   -3,  /* (241) expr ::= expr REM expr */
   -3,  /* (242) expr ::= expr LIKE expr */
   -5,  /* (243) expr ::= expr IN LP exprlist RP */
   -3,  /* (244) exprlist ::= exprlist COMMA expritem */
   -1,  /* (245) exprlist ::= expritem */
   -1,  /* (246) expritem ::= expr */
    0,  /* (247) expritem ::= */
   -3,  /* (248) cmd ::= RESET QUERY CACHE */
   -7,  /* (249) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (250) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (251) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (252) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (253) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (254) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (255) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (256) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (257) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (258) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (259) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (260) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (261) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (262) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
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
    setDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-2].minor.yy0);
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
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 33: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 34: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 35: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 37: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 38: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 41: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 44: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==44);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy100, &t);}
        break;
      case 45: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy505);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy505);}
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
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 51: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 53: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 54: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy505);}
        break;
      case 55: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 56: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==56);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy100, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy505.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy505.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy505.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy505.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy505.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy505.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy505.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy505.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy505.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy505 = yylhsminor.yy505;
        break;
      case 77: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy207 = yymsp[0].minor.yy207; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy100);}
        break;
      case 94: /* db_optr ::= db_optr cache */
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 95: /* db_optr ::= db_optr replica */
      case 112: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==112);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 96: /* db_optr ::= db_optr quorum */
      case 113: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==113);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 97: /* db_optr ::= db_optr days */
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 98: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 99: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 100: /* db_optr ::= db_optr blocks */
      case 115: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==115);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 101: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 102: /* db_optr ::= db_optr wal */
      case 117: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==117);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 103: /* db_optr ::= db_optr fsync */
      case 118: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==118);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 104: /* db_optr ::= db_optr comp */
      case 116: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==116);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 105: /* db_optr ::= db_optr prec */
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 106: /* db_optr ::= db_optr keep */
      case 114: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==114);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.keep = yymsp[0].minor.yy207; }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 107: /* db_optr ::= db_optr update */
      case 119: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==119);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 108: /* db_optr ::= db_optr cachelast */
      case 120: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==120);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 109: /* topic_optr ::= db_optr */
      case 121: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==121);
{ yylhsminor.yy100 = yymsp[0].minor.yy100; yylhsminor.yy100.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy100 = yylhsminor.yy100;
        break;
      case 110: /* topic_optr ::= topic_optr partitions */
      case 122: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==122);
{ yylhsminor.yy100 = yymsp[-1].minor.yy100; yylhsminor.yy100.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy100 = yylhsminor.yy100;
        break;
      case 111: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy100);}
        break;
      case 123: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy517, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy517 = yylhsminor.yy517;
        break;
      case 124: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy208 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy517, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy208;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy517, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy517 = yylhsminor.yy517;
        break;
      case 125: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy517, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy517 = yylhsminor.yy517;
        break;
      case 126: /* signed ::= INTEGER */
{ yylhsminor.yy208 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy208 = yylhsminor.yy208;
        break;
      case 127: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy208 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 128: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy208 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 132: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy414;}
        break;
      case 133: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy542);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy414 = pCreateTable;
}
  yymsp[0].minor.yy414 = yylhsminor.yy414;
        break;
      case 134: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy414->childTableInfo, &yymsp[0].minor.yy542);
  yylhsminor.yy414 = yymsp[-1].minor.yy414;
}
  yymsp[-1].minor.yy414 = yylhsminor.yy414;
        break;
      case 135: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy414 = tSetCreateSqlElems(yymsp[-1].minor.yy207, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy414, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy414 = yylhsminor.yy414;
        break;
      case 136: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy414 = tSetCreateSqlElems(yymsp[-5].minor.yy207, yymsp[-1].minor.yy207, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy414, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy414 = yylhsminor.yy414;
        break;
      case 137: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy542 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy207, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy542 = yylhsminor.yy542;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy542 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy207, yymsp[-1].minor.yy207, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy542 = yylhsminor.yy542;
        break;
      case 139: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy207, &yymsp[0].minor.yy0); yylhsminor.yy207 = yymsp[-2].minor.yy207;  }
  yymsp[-2].minor.yy207 = yylhsminor.yy207;
        break;
      case 140: /* tagNamelist ::= ids */
{yylhsminor.yy207 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy207, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy414 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy526, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy414, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy414 = yylhsminor.yy414;
        break;
      case 142: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy207, &yymsp[0].minor.yy517); yylhsminor.yy207 = yymsp[-2].minor.yy207;  }
  yymsp[-2].minor.yy207 = yylhsminor.yy207;
        break;
      case 143: /* columnlist ::= column */
{yylhsminor.yy207 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy207, &yymsp[0].minor.yy517);}
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 144: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy517, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy517);
}
  yymsp[-1].minor.yy517 = yylhsminor.yy517;
        break;
      case 145: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy207 = tVariantListAppend(yymsp[-2].minor.yy207, &yymsp[0].minor.yy232, -1);    }
  yymsp[-2].minor.yy207 = yylhsminor.yy207;
        break;
      case 146: /* tagitemlist ::= tagitem */
{ yylhsminor.yy207 = tVariantListAppend(NULL, &yymsp[0].minor.yy232, -1); }
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 147: /* tagitem ::= INTEGER */
      case 148: /* tagitem ::= FLOAT */ yytestcase(yyruleno==148);
      case 149: /* tagitem ::= STRING */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= BOOL */ yytestcase(yyruleno==150);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy232, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy232 = yylhsminor.yy232;
        break;
      case 151: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy232, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy232 = yylhsminor.yy232;
        break;
      case 152: /* tagitem ::= MINUS INTEGER */
      case 153: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==155);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy232, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy232 = yylhsminor.yy232;
        break;
      case 156: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy526 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy178, yymsp[-9].minor.yy207, yymsp[-8].minor.yy484, yymsp[-4].minor.yy207, yymsp[-3].minor.yy207, &yymsp[-7].minor.yy126, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy207, &yymsp[0].minor.yy314, &yymsp[-1].minor.yy314);
}
  yymsp[-11].minor.yy526 = yylhsminor.yy526;
        break;
      case 157: /* union ::= select */
{ yylhsminor.yy441 = setSubclause(NULL, yymsp[0].minor.yy526); }
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 158: /* union ::= LP union RP */
{ yymsp[-2].minor.yy441 = yymsp[-1].minor.yy441; }
        break;
      case 159: /* union ::= union UNION ALL select */
{ yylhsminor.yy441 = appendSelectClause(yymsp[-3].minor.yy441, yymsp[0].minor.yy526); }
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 160: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy441 = appendSelectClause(yymsp[-5].minor.yy441, yymsp[-1].minor.yy526); }
  yymsp[-5].minor.yy441 = yylhsminor.yy441;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy441, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy526 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy178, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy178 = yymsp[-1].minor.yy178;}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 164: /* sclp ::= */
{yymsp[1].minor.yy178 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy178 = tSqlExprListAppend(yymsp[-3].minor.yy178, yymsp[-1].minor.yy484,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy178 = tSqlExprListAppend(yymsp[-1].minor.yy178, pNode, 0, 0);
}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
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
{yymsp[-1].minor.yy207 = yymsp[0].minor.yy207;}
        break;
      case 173: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy207 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy207 = tVariantListAppendToken(yylhsminor.yy207, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy207 = yylhsminor.yy207;
        break;
      case 174: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy207 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy207 = tVariantListAppendToken(yylhsminor.yy207, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy207 = yylhsminor.yy207;
        break;
      case 175: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy207 = tVariantListAppendToken(yymsp[-3].minor.yy207, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy207 = tVariantListAppendToken(yylhsminor.yy207, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy207 = yylhsminor.yy207;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy207 = tVariantListAppendToken(yymsp[-4].minor.yy207, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy207 = tVariantListAppendToken(yylhsminor.yy207, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy207 = yylhsminor.yy207;
        break;
      case 177: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy126.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy126.offset.n = 0; yymsp[-3].minor.yy126.offset.z = NULL; yymsp[-3].minor.yy126.offset.type = 0;}
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy126.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy126.offset = yymsp[-1].minor.yy0;}
        break;
      case 180: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy126, 0, sizeof(yymsp[1].minor.yy126));}
        break;
      case 181: /* fill_opt ::= */
{yymsp[1].minor.yy207 = 0;     }
        break;
      case 182: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy207, &A, -1, 0);
    yymsp[-5].minor.yy207 = yymsp[-1].minor.yy207;
}
        break;
      case 183: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy207 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 184: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 185: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 186: /* orderby_opt ::= */
{yymsp[1].minor.yy207 = 0;}
        break;
      case 187: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy207 = yymsp[0].minor.yy207;}
        break;
      case 188: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy207 = tVariantListAppend(yymsp[-3].minor.yy207, &yymsp[-1].minor.yy232, yymsp[0].minor.yy116);
}
  yymsp[-3].minor.yy207 = yylhsminor.yy207;
        break;
      case 189: /* sortlist ::= item sortorder */
{
  yylhsminor.yy207 = tVariantListAppend(NULL, &yymsp[-1].minor.yy232, yymsp[0].minor.yy116);
}
  yymsp[-1].minor.yy207 = yylhsminor.yy207;
        break;
      case 190: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy232, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy232 = yylhsminor.yy232;
        break;
      case 191: /* sortorder ::= ASC */
{ yymsp[0].minor.yy116 = TSDB_ORDER_ASC; }
        break;
      case 192: /* sortorder ::= DESC */
{ yymsp[0].minor.yy116 = TSDB_ORDER_DESC;}
        break;
      case 193: /* sortorder ::= */
{ yymsp[1].minor.yy116 = TSDB_ORDER_ASC; }
        break;
      case 194: /* groupby_opt ::= */
{ yymsp[1].minor.yy207 = 0;}
        break;
      case 195: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy207 = yymsp[0].minor.yy207;}
        break;
      case 196: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy207 = tVariantListAppend(yymsp[-2].minor.yy207, &yymsp[0].minor.yy232, -1);
}
  yymsp[-2].minor.yy207 = yylhsminor.yy207;
        break;
      case 197: /* grouplist ::= item */
{
  yylhsminor.yy207 = tVariantListAppend(NULL, &yymsp[0].minor.yy232, -1);
}
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 198: /* having_opt ::= */
      case 208: /* where_opt ::= */ yytestcase(yyruleno==208);
      case 247: /* expritem ::= */ yytestcase(yyruleno==247);
{yymsp[1].minor.yy484 = 0;}
        break;
      case 199: /* having_opt ::= HAVING expr */
      case 209: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==209);
{yymsp[-1].minor.yy484 = yymsp[0].minor.yy484;}
        break;
      case 200: /* limit_opt ::= */
      case 204: /* slimit_opt ::= */ yytestcase(yyruleno==204);
{yymsp[1].minor.yy314.limit = -1; yymsp[1].minor.yy314.offset = 0;}
        break;
      case 201: /* limit_opt ::= LIMIT signed */
      case 205: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==205);
{yymsp[-1].minor.yy314.limit = yymsp[0].minor.yy208;  yymsp[-1].minor.yy314.offset = 0;}
        break;
      case 202: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy314.limit = yymsp[-2].minor.yy208;  yymsp[-3].minor.yy314.offset = yymsp[0].minor.yy208;}
        break;
      case 203: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy314.limit = yymsp[0].minor.yy208;  yymsp[-3].minor.yy314.offset = yymsp[-2].minor.yy208;}
        break;
      case 206: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy314.limit = yymsp[-2].minor.yy208;  yymsp[-3].minor.yy314.offset = yymsp[0].minor.yy208;}
        break;
      case 207: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy314.limit = yymsp[0].minor.yy208;  yymsp[-3].minor.yy314.offset = yymsp[-2].minor.yy208;}
        break;
      case 210: /* expr ::= LP expr RP */
{yylhsminor.yy484 = yymsp[-1].minor.yy484; yylhsminor.yy484->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy484->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 211: /* expr ::= ID */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 212: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 213: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 214: /* expr ::= INTEGER */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 215: /* expr ::= MINUS INTEGER */
      case 216: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==216);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy484 = yylhsminor.yy484;
        break;
      case 217: /* expr ::= FLOAT */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 218: /* expr ::= MINUS FLOAT */
      case 219: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==219);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy484 = yylhsminor.yy484;
        break;
      case 220: /* expr ::= STRING */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 221: /* expr ::= NOW */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 222: /* expr ::= VARIABLE */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 223: /* expr ::= BOOL */
{ yylhsminor.yy484 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 224: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy484 = tSqlExprCreateFunction(yymsp[-1].minor.yy178, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy484 = yylhsminor.yy484;
        break;
      case 225: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy484 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy484 = yylhsminor.yy484;
        break;
      case 226: /* expr ::= expr IS NULL */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 227: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-3].minor.yy484, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy484 = yylhsminor.yy484;
        break;
      case 228: /* expr ::= expr LT expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_LT);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 229: /* expr ::= expr GT expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_GT);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 230: /* expr ::= expr LE expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_LE);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 231: /* expr ::= expr GE expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_GE);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 232: /* expr ::= expr NE expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_NE);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 233: /* expr ::= expr EQ expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_EQ);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 234: /* expr ::= expr BETWEEN expr AND expr */
{ tSQLExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy484); yylhsminor.yy484 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy484, yymsp[-2].minor.yy484, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy484, TK_LE), TK_AND);}
  yymsp[-4].minor.yy484 = yylhsminor.yy484;
        break;
      case 235: /* expr ::= expr AND expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_AND);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 236: /* expr ::= expr OR expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_OR); }
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 237: /* expr ::= expr PLUS expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_PLUS);  }
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 238: /* expr ::= expr MINUS expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_MINUS); }
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 239: /* expr ::= expr STAR expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_STAR);  }
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 240: /* expr ::= expr SLASH expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_DIVIDE);}
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 241: /* expr ::= expr REM expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_REM);   }
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 242: /* expr ::= expr LIKE expr */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-2].minor.yy484, yymsp[0].minor.yy484, TK_LIKE);  }
  yymsp[-2].minor.yy484 = yylhsminor.yy484;
        break;
      case 243: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy484 = tSqlExprCreate(yymsp[-4].minor.yy484, (tSQLExpr*)yymsp[-1].minor.yy178, TK_IN); }
  yymsp[-4].minor.yy484 = yylhsminor.yy484;
        break;
      case 244: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy178 = tSqlExprListAppend(yymsp[-2].minor.yy178,yymsp[0].minor.yy484,0, 0);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 245: /* exprlist ::= expritem */
{yylhsminor.yy178 = tSqlExprListAppend(0,yymsp[0].minor.yy484,0, 0);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 246: /* expritem ::= expr */
{yylhsminor.yy484 = yymsp[0].minor.yy484;}
  yymsp[0].minor.yy484 = yylhsminor.yy484;
        break;
      case 248: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 249: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy207, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 250: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 251: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy207, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 252: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 253: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 254: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy232, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy207, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy207, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 260: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 261: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 262: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
