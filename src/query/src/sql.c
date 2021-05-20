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
#define YYNOCODE 266
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSqlNode* yy6;
  SLimitVal yy74;
  SCreateDbInfo yy122;
  SSessionWindowVal yy139;
  TAOS_FIELD yy153;
  SWindowStateVal yy158;
  int64_t yy179;
  SCreateAcctInfo yy211;
  tVariant yy216;
  SRelationInfo* yy254;
  SArray* yy291;
  int yy382;
  SIntervalVal yy400;
  SCreateTableSql* yy412;
  tSqlExpr* yy436;
  SCreatedTableInfo yy446;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             321
#define YYNRULE              272
#define YYNTOKEN             188
#define YY_MAX_SHIFT         320
#define YY_MIN_SHIFTREDUCE   516
#define YY_MAX_SHIFTREDUCE   787
#define YY_ERROR_ACTION      788
#define YY_ACCEPT_ACTION     789
#define YY_NO_ACTION         790
#define YY_MIN_REDUCE        791
#define YY_MAX_REDUCE        1062
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
#define YY_ACTTAB_COUNT (683)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   932,  564,  208,  318,  213,  959,  671,  142,  672,  565,
 /*    10 */   789,  320,   17,   47,   48,  142,   51,   52,   30,  185,
 /*    20 */   219,   41,  185,   50,  268,   55,   53,   57,   54, 1043,
 /*    30 */   938,  216, 1044,   46,   45,  183,  185,   44,   43,   42,
 /*    40 */    47,   48,  225,   51,   52,  215, 1044,  219,   41,  564,
 /*    50 */    50,  268,   55,   53,   57,   54,  950,  565,  224,  210,
 /*    60 */    46,   45,  935,  142,   44,   43,   42,   48,  956,   51,
 /*    70 */    52,   30,  246,  219,   41,  992,   50,  268,   55,   53,
 /*    80 */    57,   54,  135,  993,  938,  263,   46,   45,  284,  230,
 /*    90 */    44,   43,   42,  517,  518,  519,  520,  521,  522,  523,
 /*   100 */   524,  525,  526,  527,  528,  529,  319,  643,   86,  209,
 /*   110 */    70,  564,  222,   47,   48,  935,   51,   52,  304,  565,
 /*   120 */   219,   41,  564,   50,  268,   55,   53,   57,   54,  265,
 /*   130 */   565,   79,  731,   46,   45,  294,  293,   44,   43,   42,
 /*   140 */    47,   49,  926,   51,   52,  837,  251,  219,   41,  169,
 /*   150 */    50,  268,   55,   53,   57,   54,  317,  316,  127,  232,
 /*   160 */    46,   45,  291,  290,   44,   43,   42,   23,  282,  313,
 /*   170 */   312,  281,  280,  279,  311,  278,  310,  309,  308,  277,
 /*   180 */   307,  306,  898,   30,  886,  887,  888,  889,  890,  891,
 /*   190 */   892,  893,  894,  895,  896,  897,  899,  900,   51,   52,
 /*   200 */   189,  950,  219,   41,  924,   50,  268,   55,   53,   57,
 /*   210 */    54, 1040,   18,   83,   25,   46,   45,  211,  231,   44,
 /*   220 */    43,   42,  218,  746,  223,   30,  735,  935,  738,  194,
 /*   230 */   741,  218,  746,  226,   12,  735,  195,  738,   85,  741,
 /*   240 */    82,  119,  118,  193,  921,  922,   29,  925,   55,   53,
 /*   250 */    57,   54,  269,  936,  204,  205,   46,   45,  267,  938,
 /*   260 */    44,   43,   42,  204,  205,  238,  287,  228,   23,  935,
 /*   270 */   313,  312,   74,  242,  241,  311, 1054,  310,  309,  308,
 /*   280 */    36,  307,  306,  284,  906,   46,   45,  904,  905,   44,
 /*   290 */    43,   42,  907,  142,  909,  910,  908,  314,  911,  912,
 /*   300 */   104,   98,  109,  245,  737,   68,  740,  108,  114,  117,
 /*   310 */   107,  201,  667,  682,   69,  664,  111,  665,   30,  666,
 /*   320 */     5,   33,  159,  938,   30,   56,   30,  158,   93,   88,
 /*   330 */    92,   30,  747,  736,   56,  739,   28,  229,  743,  274,
 /*   340 */   286,  747,   81,  233,  234,   74,  923,  743,  750,  106,
 /*   350 */   177,  175,  173,   36,  304,  742,   71,  172,  122,  121,
 /*   360 */   120,   80,  934, 1039,  742,  288,  689,  292,  935,  846,
 /*   370 */   935,  217,  296,  169,  249,  935,   44,   43,   42,  133,
 /*   380 */   131,  130,  838,    1,  157, 1038,  169,    3,  170,  712,
 /*   390 */   713,  686,  679,  733,  247,   61,  695,  703,   31,   24,
 /*   400 */   137,   60,  704,  767,   64,  748,   20,   19,  653,   19,
 /*   410 */   271,  202,   31,  655,   31,    6,  744,   60,   62,  273,
 /*   420 */   654,  668,   39,   84,   60,   65,   97,   96,   67,  734,
 /*   430 */   642,  203,   14,   13, 1003,  103,  102,  116,  115, 1002,
 /*   440 */    16,   15,  669,  187,  670,  745,  937,  188,  190,  184,
 /*   450 */   191,  192,  198,  220,  199,  197,  182,  196,  243,  186,
 /*   460 */   134,  999,  998,  221,  295,  153,  958,  966,  985,  968,
 /*   470 */   136,  140,  257,  984,  951,  250,  154,  933,  132,  252,
 /*   480 */   143,  212,  931,  694,  254,  261,  155,  305,  152,  150,
 /*   490 */   148,  146,  144,  948,  156,  266,  849,   66,  276,   37,
 /*   500 */    63,  180,   58,  145,   34,  285,  264,  262,  845,  260,
 /*   510 */  1059,   94, 1058, 1056,  160,  289,  147, 1053,  100, 1052,
 /*   520 */   258, 1050,  161,  867,   35,   32,   38,  181,  834,  110,
 /*   530 */   832,  256,  112,  113,  830,  829,  235,  171,  827,  826,
 /*   540 */   825,  824,  823,  822,  174,  176,  819,  817,  815,  813,
 /*   550 */   178,  810,  179,  253,   40,  248,   72,   75,  255,  105,
 /*   560 */   986,  297,  298,  299,  300,  301,  206,  302,  227,  275,
 /*   570 */   303,  315,  787,  236,  207,  200,   89,  237,   90,  786,
 /*   580 */   239,  240,  785,  773,  772,  244,  249,  674,  828,  821,
 /*   590 */   164,  820,  868,  123,  162,  167,  163,  165,  166,  168,
 /*   600 */   124,  125,  126,  902,  812,    4,  811,  270,    2,    8,
 /*   610 */    73,   76,  151,  149,  696,  699,  139,  138,   77,  914,
 /*   620 */   701,   78,  214,  259,   85,  705,  141,   26,    9,  749,
 /*   630 */    27,    7,   10,   11,  751,   21,   22,  272,   87,  606,
 /*   640 */   602,  600,  599,  598,  595,  568,  283,   91,   31,   59,
 /*   650 */   645,  644,  641,  590,  588,  580,  586,  582,  584,  578,
 /*   660 */   576,  609,  608,  607,  605,   95,   99,  604,  101,  603,
 /*   670 */   601,  597,  596,   60,  566,  533,  531,  791,  790,  790,
 /*   680 */   790,  128,  129,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   192,    1,  191,  192,  211,  192,    5,  192,    7,    9,
 /*    10 */   189,  190,  254,   13,   14,  192,   16,   17,  192,  254,
 /*    20 */    20,   21,  254,   23,   24,   25,   26,   27,   28,  264,
 /*    30 */   237,  263,  264,   33,   34,  254,  254,   37,   38,   39,
 /*    40 */    13,   14,  234,   16,   17,  263,  264,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  235,    9,  211,  233,
 /*    60 */    33,   34,  236,  192,   37,   38,   39,   14,  255,   16,
 /*    70 */    17,  192,  251,   20,   21,  260,   23,   24,   25,   26,
 /*    80 */    27,   28,  192,  260,  237,  262,   33,   34,   79,   68,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,    5,  198,   61,
 /*   110 */   110,    1,  233,   13,   14,  236,   16,   17,   81,    9,
 /*   120 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  258,
 /*   130 */     9,  260,  105,   33,   34,   33,   34,   37,   38,   39,
 /*   140 */    13,   14,  232,   16,   17,  197,  256,   20,   21,  201,
 /*   150 */    23,   24,   25,   26,   27,   28,   65,   66,   67,  138,
 /*   160 */    33,   34,  141,  142,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  210,  192,  212,  213,  214,  215,  216,  217,
 /*   190 */   218,  219,  220,  221,  222,  223,  224,  225,   16,   17,
 /*   200 */   254,  235,   20,   21,    0,   23,   24,   25,   26,   27,
 /*   210 */    28,  254,   44,  198,  104,   33,   34,  251,  192,   37,
 /*   220 */    38,   39,    1,    2,  233,  192,    5,  236,    7,   61,
 /*   230 */     9,    1,    2,  211,  104,    5,   68,    7,  108,    9,
 /*   240 */   110,   73,   74,   75,  229,  230,  231,  232,   25,   26,
 /*   250 */    27,   28,   15,  227,   33,   34,   33,   34,   37,  237,
 /*   260 */    37,   38,   39,   33,   34,  136,  233,   68,   88,  236,
 /*   270 */    90,   91,  104,  144,  145,   95,  237,   97,   98,   99,
 /*   280 */   112,  101,  102,   79,  210,   33,   34,  213,  214,   37,
 /*   290 */    38,   39,  218,  192,  220,  221,  222,  211,  224,  225,
 /*   300 */    62,   63,   64,  135,    5,  137,    7,   69,   70,   71,
 /*   310 */    72,  143,    2,   37,  198,    5,   78,    7,  192,    9,
 /*   320 */    62,   63,   64,  237,  192,  104,  192,   69,   70,   71,
 /*   330 */    72,  192,  111,    5,  104,    7,  104,  138,  117,  107,
 /*   340 */   141,  111,  238,   33,   34,  104,  230,  117,  111,   76,
 /*   350 */    62,   63,   64,  112,   81,  134,  252,   69,   70,   71,
 /*   360 */    72,  260,  236,  254,  134,  233,  105,  233,  236,  197,
 /*   370 */   236,   60,  233,  201,  113,  236,   37,   38,   39,   62,
 /*   380 */    63,   64,  197,  199,  200,  254,  201,  195,  196,  125,
 /*   390 */   126,  115,  109,    1,  105,  109,  105,  105,  109,  116,
 /*   400 */   109,  109,  105,  105,  109,  105,  109,  109,  105,  109,
 /*   410 */   105,  254,  109,  105,  109,  104,  117,  109,  132,  105,
 /*   420 */   105,  111,  253,  109,  109,  130,  139,  140,  104,   37,
 /*   430 */   106,  254,  139,  140,  228,  139,  140,   76,   77,  228,
 /*   440 */   139,  140,    5,  254,    7,  117,  237,  254,  254,  254,
 /*   450 */   254,  254,  254,  228,  254,  254,  254,  254,  192,  254,
 /*   460 */   192,  228,  228,  228,  228,  239,  192,  192,  261,  192,
 /*   470 */   192,  192,  192,  261,  235,  235,  192,  235,   60,  257,
 /*   480 */   249,  257,  192,  117,  257,  257,  192,  103,  240,  242,
 /*   490 */   244,  246,  248,  250,  192,  123,  192,  129,  192,  192,
 /*   500 */   131,  192,  128,  247,  192,  192,  127,  122,  192,  121,
 /*   510 */   192,  192,  192,  192,  192,  192,  245,  192,  192,  192,
 /*   520 */   120,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   530 */   192,  119,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   540 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   550 */   192,  192,  192,  118,  133,  193,  193,  193,  193,   87,
 /*   560 */   193,   86,   50,   83,   85,   54,  193,   84,  193,  193,
 /*   570 */    82,   79,    5,  146,  193,  193,  198,    5,  198,    5,
 /*   580 */   146,    5,    5,   90,   89,  136,  113,  105,  193,  193,
 /*   590 */   203,  193,  209,  194,  208,  205,  207,  206,  204,  202,
 /*   600 */   194,  194,  194,  226,  193,  195,  193,  107,  199,  104,
 /*   610 */   114,  109,  241,  243,  105,  105,  109,  104,  104,  226,
 /*   620 */   105,  104,    1,  104,  108,  105,  104,  109,  124,  105,
 /*   630 */   109,  104,  124,  104,  111,  104,  104,  107,   76,    9,
 /*   640 */     5,    5,    5,    5,    5,   80,   15,   76,  109,   16,
 /*   650 */     5,    5,  105,    5,    5,    5,    5,    5,    5,    5,
 /*   660 */     5,    5,    5,    5,    5,  140,  140,    5,  140,    5,
 /*   670 */     5,    5,    5,  109,   80,   60,   59,    0,  265,  265,
 /*   680 */   265,   21,   21,  265,  265,  265,  265,  265,  265,  265,
 /*   690 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   700 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   710 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   720 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   730 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   740 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   750 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   760 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   770 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   780 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   790 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   800 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   810 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   820 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   830 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   840 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   850 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   860 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   870 */   265,
};
#define YY_SHIFT_COUNT    (320)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (677)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  180,  180,    9,  221,  230,  121,  121,
 /*    10 */   121,  121,  121,  121,  121,  121,  121,    0,   48,  230,
 /*    20 */   310,  310,  310,  310,  110,  241,  121,  121,  121,  204,
 /*    30 */   121,  121,  273,    9,   37,   37,  683,  683,  683,  230,
 /*    40 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    50 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  310,
 /*    60 */   310,  102,  102,  102,  102,  102,  102,  102,  121,  121,
 /*    70 */   121,  276,  121,  241,  241,  121,  121,  121,  121,  264,
 /*    80 */   264,  283,  241,  121,  121,  121,  121,  121,  121,  121,
 /*    90 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   100 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   110 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   120 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   130 */   121,  121,  121,  121,  418,  418,  418,  366,  366,  366,
 /*   140 */   418,  366,  418,  368,  369,  374,  372,  379,  385,  388,
 /*   150 */   400,  412,  435,  421,  418,  418,  418,  384,    9,    9,
 /*   160 */   418,  418,  472,  475,  512,  480,  479,  511,  483,  488,
 /*   170 */   384,  418,  492,  492,  418,  492,  418,  492,  418,  418,
 /*   180 */   683,  683,   27,  100,  127,  100,  100,   53,  182,  223,
 /*   190 */   223,  223,  223,  238,  258,  288,  252,  252,  252,  252,
 /*   200 */    21,  129,  339,  339,  299,  328,  130,  199,   91,  317,
 /*   210 */   289,  261,  291,  292,  297,  298,  300,  392,  311,  237,
 /*   220 */   286,  295,  303,  305,  308,  314,  315,  232,  287,  293,
 /*   230 */   296,  324,  301,    1,  437,  361,  567,  427,  572,  574,
 /*   240 */   434,  576,  577,  493,  495,  449,  473,  500,  505,  496,
 /*   250 */   482,  502,  509,  513,  510,  507,  514,  515,  517,  621,
 /*   260 */   519,  520,  522,  518,  504,  521,  508,  524,  527,  523,
 /*   270 */   529,  500,  531,  530,  532,  516,  562,  630,  635,  636,
 /*   280 */   637,  638,  639,  565,  631,  571,  525,  539,  539,  633,
 /*   290 */   526,  528,  539,  645,  646,  547,  539,  648,  649,  650,
 /*   300 */   651,  652,  653,  654,  655,  656,  657,  658,  659,  662,
 /*   310 */   664,  665,  666,  667,  564,  594,  660,  661,  615,  617,
 /*   320 */   677,
};
#define YY_REDUCE_COUNT (181)
#define YY_REDUCE_MIN   (-242)
#define YY_REDUCE_MAX   (413)
static const short yy_reduce_ofst[] = {
 /*     0 */  -179,  -28,  -28,   74,   74,   15, -232, -218, -174, -177,
 /*    10 */  -129, -121,   -9,   33,  132,  134,  139, -187, -189, -235,
 /*    20 */  -207, -153,   22,   86, -110,  -34, -185,  101, -192,  -90,
 /*    30 */    26,  126,  -52,  116,  172,  185,  104,  184,  192, -242,
 /*    40 */  -219,  -54,  -43,  109,  131,  157,  177,  189,  193,  194,
 /*    50 */   195,  196,  197,  198,  200,  201,  202,  203,  205,   39,
 /*    60 */   209,  206,  211,  225,  233,  234,  235,  236,  266,  268,
 /*    70 */   274,  169,  275,  239,  240,  277,  278,  279,  280,  207,
 /*    80 */   212,  226,  242,  284,  290,  294,  302,  304,  306,  307,
 /*    90 */   309,  312,  313,  316,  318,  319,  320,  321,  322,  323,
 /*   100 */   325,  326,  327,  329,  330,  331,  332,  333,  334,  335,
 /*   110 */   336,  337,  338,  340,  341,  342,  343,  344,  345,  346,
 /*   120 */   347,  348,  349,  350,  351,  352,  353,  354,  355,  356,
 /*   130 */   357,  358,  359,  360,  362,  363,  364,  222,  224,  227,
 /*   140 */   365,  228,  367,  243,  231,  244,  256,  245,  271,  246,
 /*   150 */   370,  247,  371,  248,  373,  375,  376,  377,  378,  380,
 /*   160 */   381,  382,  383,  386,  389,  387,  391,  394,  390,  397,
 /*   170 */   393,  395,  399,  406,  396,  407,  398,  408,  411,  413,
 /*   180 */   409,  410,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   788,  901,  847,  913,  835,  844, 1046, 1046,  788,  788,
 /*    10 */   788,  788,  788,  788,  788,  788,  788,  960,  807, 1046,
 /*    20 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  844,
 /*    30 */   788,  788,  850,  844,  850,  850,  955,  885,  903,  788,
 /*    40 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    50 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    60 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    70 */   788,  962,  965,  788,  788,  967,  788,  788,  788,  989,
 /*    80 */   989,  953,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    90 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   100 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   110 */   833,  788,  831,  788,  788,  788,  788,  788,  788,  788,
 /*   120 */   788,  788,  788,  788,  788,  788,  788,  818,  788,  788,
 /*   130 */   788,  788,  788,  788,  809,  809,  809,  788,  788,  788,
 /*   140 */   809,  788,  809,  996, 1000,  994,  982,  990,  981,  977,
 /*   150 */   975,  973,  972, 1004,  809,  809,  809,  848,  844,  844,
 /*   160 */   809,  809,  866,  864,  862,  854,  860,  856,  858,  852,
 /*   170 */   836,  809,  842,  842,  809,  842,  809,  842,  809,  809,
 /*   180 */   885,  903,  788, 1005,  788, 1045,  995, 1035, 1034, 1041,
 /*   190 */  1033, 1032, 1031,  788,  788,  788, 1027, 1028, 1030, 1029,
 /*   200 */   788,  788, 1037, 1036,  788,  788,  788,  788,  788,  788,
 /*   210 */   788,  788,  788,  788,  788,  788,  788,  788, 1007,  788,
 /*   220 */  1001,  997,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   230 */   788,  915,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   240 */   788,  788,  788,  788,  788,  788,  952,  788,  788,  788,
 /*   250 */   788,  963,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   260 */   788,  788,  788,  991,  788,  983,  788,  788,  788,  788,
 /*   270 */   788,  927,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   280 */   788,  788,  788,  788,  788,  788,  788, 1057, 1055,  788,
 /*   290 */   788,  788, 1051,  788,  788,  788, 1049,  788,  788,  788,
 /*   300 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   310 */   788,  788,  788,  788,  869,  788,  816,  814,  788,  805,
 /*   320 */   788,
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
    1,  /*     STABLE => ID */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
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
    0,  /* STATE_WINDOW => nothing */
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
  /*   63 */ "STABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
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
  /*  120 */ "STATE_WINDOW",
  /*  121 */ "FILL",
  /*  122 */ "SLIDING",
  /*  123 */ "ORDER",
  /*  124 */ "BY",
  /*  125 */ "ASC",
  /*  126 */ "DESC",
  /*  127 */ "GROUP",
  /*  128 */ "HAVING",
  /*  129 */ "LIMIT",
  /*  130 */ "OFFSET",
  /*  131 */ "SLIMIT",
  /*  132 */ "SOFFSET",
  /*  133 */ "WHERE",
  /*  134 */ "NOW",
  /*  135 */ "RESET",
  /*  136 */ "QUERY",
  /*  137 */ "SYNCDB",
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
  /*  178 */ "SEMI",
  /*  179 */ "NONE",
  /*  180 */ "PREV",
  /*  181 */ "LINEAR",
  /*  182 */ "IMPORT",
  /*  183 */ "TBNAME",
  /*  184 */ "JOIN",
  /*  185 */ "INSERT",
  /*  186 */ "INTO",
  /*  187 */ "VALUES",
  /*  188 */ "error",
  /*  189 */ "program",
  /*  190 */ "cmd",
  /*  191 */ "dbPrefix",
  /*  192 */ "ids",
  /*  193 */ "cpxName",
  /*  194 */ "ifexists",
  /*  195 */ "alter_db_optr",
  /*  196 */ "alter_topic_optr",
  /*  197 */ "acct_optr",
  /*  198 */ "ifnotexists",
  /*  199 */ "db_optr",
  /*  200 */ "topic_optr",
  /*  201 */ "pps",
  /*  202 */ "tseries",
  /*  203 */ "dbs",
  /*  204 */ "streams",
  /*  205 */ "storage",
  /*  206 */ "qtime",
  /*  207 */ "users",
  /*  208 */ "conns",
  /*  209 */ "state",
  /*  210 */ "keep",
  /*  211 */ "tagitemlist",
  /*  212 */ "cache",
  /*  213 */ "replica",
  /*  214 */ "quorum",
  /*  215 */ "days",
  /*  216 */ "minrows",
  /*  217 */ "maxrows",
  /*  218 */ "blocks",
  /*  219 */ "ctime",
  /*  220 */ "wal",
  /*  221 */ "fsync",
  /*  222 */ "comp",
  /*  223 */ "prec",
  /*  224 */ "update",
  /*  225 */ "cachelast",
  /*  226 */ "partitions",
  /*  227 */ "typename",
  /*  228 */ "signed",
  /*  229 */ "create_table_args",
  /*  230 */ "create_stable_args",
  /*  231 */ "create_table_list",
  /*  232 */ "create_from_stable",
  /*  233 */ "columnlist",
  /*  234 */ "tagNamelist",
  /*  235 */ "select",
  /*  236 */ "column",
  /*  237 */ "tagitem",
  /*  238 */ "selcollist",
  /*  239 */ "from",
  /*  240 */ "where_opt",
  /*  241 */ "interval_opt",
  /*  242 */ "session_option",
  /*  243 */ "windowstate_option",
  /*  244 */ "fill_opt",
  /*  245 */ "sliding_opt",
  /*  246 */ "groupby_opt",
  /*  247 */ "orderby_opt",
  /*  248 */ "having_opt",
  /*  249 */ "slimit_opt",
  /*  250 */ "limit_opt",
  /*  251 */ "union",
  /*  252 */ "sclp",
  /*  253 */ "distinct",
  /*  254 */ "expr",
  /*  255 */ "as",
  /*  256 */ "tablelist",
  /*  257 */ "tmvar",
  /*  258 */ "sortlist",
  /*  259 */ "sortitem",
  /*  260 */ "item",
  /*  261 */ "sortorder",
  /*  262 */ "grouplist",
  /*  263 */ "exprlist",
  /*  264 */ "expritem",
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
 /*  21 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
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
 /*  33 */ "cmd ::= DROP DNODE ids",
 /*  34 */ "cmd ::= DROP USER ids",
 /*  35 */ "cmd ::= DROP ACCOUNT ids",
 /*  36 */ "cmd ::= USE ids",
 /*  37 */ "cmd ::= DESCRIBE ids cpxName",
 /*  38 */ "cmd ::= ALTER USER ids PASS ids",
 /*  39 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids ids",
 /*  44 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  45 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "ids ::= ID",
 /*  49 */ "ids ::= STRING",
 /*  50 */ "ifexists ::= IF EXISTS",
 /*  51 */ "ifexists ::=",
 /*  52 */ "ifnotexists ::= IF NOT EXISTS",
 /*  53 */ "ifnotexists ::=",
 /*  54 */ "cmd ::= CREATE DNODE ids",
 /*  55 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  56 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  57 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  58 */ "cmd ::= CREATE USER ids PASS ids",
 /*  59 */ "pps ::=",
 /*  60 */ "pps ::= PPS INTEGER",
 /*  61 */ "tseries ::=",
 /*  62 */ "tseries ::= TSERIES INTEGER",
 /*  63 */ "dbs ::=",
 /*  64 */ "dbs ::= DBS INTEGER",
 /*  65 */ "streams ::=",
 /*  66 */ "streams ::= STREAMS INTEGER",
 /*  67 */ "storage ::=",
 /*  68 */ "storage ::= STORAGE INTEGER",
 /*  69 */ "qtime ::=",
 /*  70 */ "qtime ::= QTIME INTEGER",
 /*  71 */ "users ::=",
 /*  72 */ "users ::= USERS INTEGER",
 /*  73 */ "conns ::=",
 /*  74 */ "conns ::= CONNS INTEGER",
 /*  75 */ "state ::=",
 /*  76 */ "state ::= STATE ids",
 /*  77 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  78 */ "keep ::= KEEP tagitemlist",
 /*  79 */ "cache ::= CACHE INTEGER",
 /*  80 */ "replica ::= REPLICA INTEGER",
 /*  81 */ "quorum ::= QUORUM INTEGER",
 /*  82 */ "days ::= DAYS INTEGER",
 /*  83 */ "minrows ::= MINROWS INTEGER",
 /*  84 */ "maxrows ::= MAXROWS INTEGER",
 /*  85 */ "blocks ::= BLOCKS INTEGER",
 /*  86 */ "ctime ::= CTIME INTEGER",
 /*  87 */ "wal ::= WAL INTEGER",
 /*  88 */ "fsync ::= FSYNC INTEGER",
 /*  89 */ "comp ::= COMP INTEGER",
 /*  90 */ "prec ::= PRECISION STRING",
 /*  91 */ "update ::= UPDATE INTEGER",
 /*  92 */ "cachelast ::= CACHELAST INTEGER",
 /*  93 */ "partitions ::= PARTITIONS INTEGER",
 /*  94 */ "db_optr ::=",
 /*  95 */ "db_optr ::= db_optr cache",
 /*  96 */ "db_optr ::= db_optr replica",
 /*  97 */ "db_optr ::= db_optr quorum",
 /*  98 */ "db_optr ::= db_optr days",
 /*  99 */ "db_optr ::= db_optr minrows",
 /* 100 */ "db_optr ::= db_optr maxrows",
 /* 101 */ "db_optr ::= db_optr blocks",
 /* 102 */ "db_optr ::= db_optr ctime",
 /* 103 */ "db_optr ::= db_optr wal",
 /* 104 */ "db_optr ::= db_optr fsync",
 /* 105 */ "db_optr ::= db_optr comp",
 /* 106 */ "db_optr ::= db_optr prec",
 /* 107 */ "db_optr ::= db_optr keep",
 /* 108 */ "db_optr ::= db_optr update",
 /* 109 */ "db_optr ::= db_optr cachelast",
 /* 110 */ "topic_optr ::= db_optr",
 /* 111 */ "topic_optr ::= topic_optr partitions",
 /* 112 */ "alter_db_optr ::=",
 /* 113 */ "alter_db_optr ::= alter_db_optr replica",
 /* 114 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 115 */ "alter_db_optr ::= alter_db_optr keep",
 /* 116 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 117 */ "alter_db_optr ::= alter_db_optr comp",
 /* 118 */ "alter_db_optr ::= alter_db_optr wal",
 /* 119 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 120 */ "alter_db_optr ::= alter_db_optr update",
 /* 121 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 122 */ "alter_topic_optr ::= alter_db_optr",
 /* 123 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 124 */ "typename ::= ids",
 /* 125 */ "typename ::= ids LP signed RP",
 /* 126 */ "typename ::= ids UNSIGNED",
 /* 127 */ "signed ::= INTEGER",
 /* 128 */ "signed ::= PLUS INTEGER",
 /* 129 */ "signed ::= MINUS INTEGER",
 /* 130 */ "cmd ::= CREATE TABLE create_table_args",
 /* 131 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 133 */ "cmd ::= CREATE TABLE create_table_list",
 /* 134 */ "create_table_list ::= create_from_stable",
 /* 135 */ "create_table_list ::= create_table_list create_from_stable",
 /* 136 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 137 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 139 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 140 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 141 */ "tagNamelist ::= ids",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 143 */ "columnlist ::= columnlist COMMA column",
 /* 144 */ "columnlist ::= column",
 /* 145 */ "column ::= ids typename",
 /* 146 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 147 */ "tagitemlist ::= tagitem",
 /* 148 */ "tagitem ::= INTEGER",
 /* 149 */ "tagitem ::= FLOAT",
 /* 150 */ "tagitem ::= STRING",
 /* 151 */ "tagitem ::= BOOL",
 /* 152 */ "tagitem ::= NULL",
 /* 153 */ "tagitem ::= MINUS INTEGER",
 /* 154 */ "tagitem ::= MINUS FLOAT",
 /* 155 */ "tagitem ::= PLUS INTEGER",
 /* 156 */ "tagitem ::= PLUS FLOAT",
 /* 157 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 158 */ "select ::= LP select RP",
 /* 159 */ "union ::= select",
 /* 160 */ "union ::= union UNION ALL select",
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
 /* 173 */ "from ::= FROM LP union RP",
 /* 174 */ "tablelist ::= ids cpxName",
 /* 175 */ "tablelist ::= ids cpxName ids",
 /* 176 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 177 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 178 */ "tmvar ::= VARIABLE",
 /* 179 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 180 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 181 */ "interval_opt ::=",
 /* 182 */ "session_option ::=",
 /* 183 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 184 */ "windowstate_option ::=",
 /* 185 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 186 */ "fill_opt ::=",
 /* 187 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 188 */ "fill_opt ::= FILL LP ID RP",
 /* 189 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 190 */ "sliding_opt ::=",
 /* 191 */ "orderby_opt ::=",
 /* 192 */ "orderby_opt ::= ORDER BY sortlist",
 /* 193 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 194 */ "sortlist ::= item sortorder",
 /* 195 */ "item ::= ids cpxName",
 /* 196 */ "sortorder ::= ASC",
 /* 197 */ "sortorder ::= DESC",
 /* 198 */ "sortorder ::=",
 /* 199 */ "groupby_opt ::=",
 /* 200 */ "groupby_opt ::= GROUP BY grouplist",
 /* 201 */ "grouplist ::= grouplist COMMA item",
 /* 202 */ "grouplist ::= item",
 /* 203 */ "having_opt ::=",
 /* 204 */ "having_opt ::= HAVING expr",
 /* 205 */ "limit_opt ::=",
 /* 206 */ "limit_opt ::= LIMIT signed",
 /* 207 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 208 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 209 */ "slimit_opt ::=",
 /* 210 */ "slimit_opt ::= SLIMIT signed",
 /* 211 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 212 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 213 */ "where_opt ::=",
 /* 214 */ "where_opt ::= WHERE expr",
 /* 215 */ "expr ::= LP expr RP",
 /* 216 */ "expr ::= ID",
 /* 217 */ "expr ::= ID DOT ID",
 /* 218 */ "expr ::= ID DOT STAR",
 /* 219 */ "expr ::= INTEGER",
 /* 220 */ "expr ::= MINUS INTEGER",
 /* 221 */ "expr ::= PLUS INTEGER",
 /* 222 */ "expr ::= FLOAT",
 /* 223 */ "expr ::= MINUS FLOAT",
 /* 224 */ "expr ::= PLUS FLOAT",
 /* 225 */ "expr ::= STRING",
 /* 226 */ "expr ::= NOW",
 /* 227 */ "expr ::= VARIABLE",
 /* 228 */ "expr ::= PLUS VARIABLE",
 /* 229 */ "expr ::= MINUS VARIABLE",
 /* 230 */ "expr ::= BOOL",
 /* 231 */ "expr ::= NULL",
 /* 232 */ "expr ::= ID LP exprlist RP",
 /* 233 */ "expr ::= ID LP STAR RP",
 /* 234 */ "expr ::= expr IS NULL",
 /* 235 */ "expr ::= expr IS NOT NULL",
 /* 236 */ "expr ::= expr LT expr",
 /* 237 */ "expr ::= expr GT expr",
 /* 238 */ "expr ::= expr LE expr",
 /* 239 */ "expr ::= expr GE expr",
 /* 240 */ "expr ::= expr NE expr",
 /* 241 */ "expr ::= expr EQ expr",
 /* 242 */ "expr ::= expr BETWEEN expr AND expr",
 /* 243 */ "expr ::= expr AND expr",
 /* 244 */ "expr ::= expr OR expr",
 /* 245 */ "expr ::= expr PLUS expr",
 /* 246 */ "expr ::= expr MINUS expr",
 /* 247 */ "expr ::= expr STAR expr",
 /* 248 */ "expr ::= expr SLASH expr",
 /* 249 */ "expr ::= expr REM expr",
 /* 250 */ "expr ::= expr LIKE expr",
 /* 251 */ "expr ::= expr IN LP exprlist RP",
 /* 252 */ "exprlist ::= exprlist COMMA expritem",
 /* 253 */ "exprlist ::= expritem",
 /* 254 */ "expritem ::= expr",
 /* 255 */ "expritem ::=",
 /* 256 */ "cmd ::= RESET QUERY CACHE",
 /* 257 */ "cmd ::= SYNCDB ids REPLICA",
 /* 258 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 260 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 264 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 265 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 266 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 267 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 268 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 269 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 270 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 271 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 210: /* keep */
    case 211: /* tagitemlist */
    case 233: /* columnlist */
    case 234: /* tagNamelist */
    case 244: /* fill_opt */
    case 246: /* groupby_opt */
    case 247: /* orderby_opt */
    case 258: /* sortlist */
    case 262: /* grouplist */
{
taosArrayDestroy((yypminor->yy291));
}
      break;
    case 231: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy412));
}
      break;
    case 235: /* select */
{
destroySqlNode((yypminor->yy6));
}
      break;
    case 238: /* selcollist */
    case 252: /* sclp */
    case 263: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy291));
}
      break;
    case 239: /* from */
    case 256: /* tablelist */
{
destroyRelationInfo((yypminor->yy254));
}
      break;
    case 240: /* where_opt */
    case 248: /* having_opt */
    case 254: /* expr */
    case 264: /* expritem */
{
tSqlExprDestroy((yypminor->yy436));
}
      break;
    case 251: /* union */
{
destroyAllSqlNode((yypminor->yy291));
}
      break;
    case 259: /* sortitem */
{
tVariantDestroy(&(yypminor->yy216));
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
  {  189,   -1 }, /* (0) program ::= cmd */
  {  190,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  190,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  190,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  190,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  190,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  190,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  190,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  190,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  190,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  190,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  190,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  190,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  190,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  190,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  190,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  191,    0 }, /* (16) dbPrefix ::= */
  {  191,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  193,    0 }, /* (18) cpxName ::= */
  {  193,   -2 }, /* (19) cpxName ::= DOT ids */
  {  190,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  190,   -5 }, /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  190,   -4 }, /* (22) cmd ::= SHOW CREATE DATABASE ids */
  {  190,   -3 }, /* (23) cmd ::= SHOW dbPrefix TABLES */
  {  190,   -5 }, /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  190,   -3 }, /* (25) cmd ::= SHOW dbPrefix STABLES */
  {  190,   -5 }, /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  190,   -3 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS */
  {  190,   -4 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  190,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  190,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  190,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  190,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  190,   -3 }, /* (33) cmd ::= DROP DNODE ids */
  {  190,   -3 }, /* (34) cmd ::= DROP USER ids */
  {  190,   -3 }, /* (35) cmd ::= DROP ACCOUNT ids */
  {  190,   -2 }, /* (36) cmd ::= USE ids */
  {  190,   -3 }, /* (37) cmd ::= DESCRIBE ids cpxName */
  {  190,   -5 }, /* (38) cmd ::= ALTER USER ids PASS ids */
  {  190,   -5 }, /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  190,   -4 }, /* (40) cmd ::= ALTER DNODE ids ids */
  {  190,   -5 }, /* (41) cmd ::= ALTER DNODE ids ids ids */
  {  190,   -3 }, /* (42) cmd ::= ALTER LOCAL ids */
  {  190,   -4 }, /* (43) cmd ::= ALTER LOCAL ids ids */
  {  190,   -4 }, /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  190,   -4 }, /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  190,   -4 }, /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  190,   -6 }, /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  192,   -1 }, /* (48) ids ::= ID */
  {  192,   -1 }, /* (49) ids ::= STRING */
  {  194,   -2 }, /* (50) ifexists ::= IF EXISTS */
  {  194,    0 }, /* (51) ifexists ::= */
  {  198,   -3 }, /* (52) ifnotexists ::= IF NOT EXISTS */
  {  198,    0 }, /* (53) ifnotexists ::= */
  {  190,   -3 }, /* (54) cmd ::= CREATE DNODE ids */
  {  190,   -6 }, /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  190,   -5 }, /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  190,   -5 }, /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  190,   -5 }, /* (58) cmd ::= CREATE USER ids PASS ids */
  {  201,    0 }, /* (59) pps ::= */
  {  201,   -2 }, /* (60) pps ::= PPS INTEGER */
  {  202,    0 }, /* (61) tseries ::= */
  {  202,   -2 }, /* (62) tseries ::= TSERIES INTEGER */
  {  203,    0 }, /* (63) dbs ::= */
  {  203,   -2 }, /* (64) dbs ::= DBS INTEGER */
  {  204,    0 }, /* (65) streams ::= */
  {  204,   -2 }, /* (66) streams ::= STREAMS INTEGER */
  {  205,    0 }, /* (67) storage ::= */
  {  205,   -2 }, /* (68) storage ::= STORAGE INTEGER */
  {  206,    0 }, /* (69) qtime ::= */
  {  206,   -2 }, /* (70) qtime ::= QTIME INTEGER */
  {  207,    0 }, /* (71) users ::= */
  {  207,   -2 }, /* (72) users ::= USERS INTEGER */
  {  208,    0 }, /* (73) conns ::= */
  {  208,   -2 }, /* (74) conns ::= CONNS INTEGER */
  {  209,    0 }, /* (75) state ::= */
  {  209,   -2 }, /* (76) state ::= STATE ids */
  {  197,   -9 }, /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  210,   -2 }, /* (78) keep ::= KEEP tagitemlist */
  {  212,   -2 }, /* (79) cache ::= CACHE INTEGER */
  {  213,   -2 }, /* (80) replica ::= REPLICA INTEGER */
  {  214,   -2 }, /* (81) quorum ::= QUORUM INTEGER */
  {  215,   -2 }, /* (82) days ::= DAYS INTEGER */
  {  216,   -2 }, /* (83) minrows ::= MINROWS INTEGER */
  {  217,   -2 }, /* (84) maxrows ::= MAXROWS INTEGER */
  {  218,   -2 }, /* (85) blocks ::= BLOCKS INTEGER */
  {  219,   -2 }, /* (86) ctime ::= CTIME INTEGER */
  {  220,   -2 }, /* (87) wal ::= WAL INTEGER */
  {  221,   -2 }, /* (88) fsync ::= FSYNC INTEGER */
  {  222,   -2 }, /* (89) comp ::= COMP INTEGER */
  {  223,   -2 }, /* (90) prec ::= PRECISION STRING */
  {  224,   -2 }, /* (91) update ::= UPDATE INTEGER */
  {  225,   -2 }, /* (92) cachelast ::= CACHELAST INTEGER */
  {  226,   -2 }, /* (93) partitions ::= PARTITIONS INTEGER */
  {  199,    0 }, /* (94) db_optr ::= */
  {  199,   -2 }, /* (95) db_optr ::= db_optr cache */
  {  199,   -2 }, /* (96) db_optr ::= db_optr replica */
  {  199,   -2 }, /* (97) db_optr ::= db_optr quorum */
  {  199,   -2 }, /* (98) db_optr ::= db_optr days */
  {  199,   -2 }, /* (99) db_optr ::= db_optr minrows */
  {  199,   -2 }, /* (100) db_optr ::= db_optr maxrows */
  {  199,   -2 }, /* (101) db_optr ::= db_optr blocks */
  {  199,   -2 }, /* (102) db_optr ::= db_optr ctime */
  {  199,   -2 }, /* (103) db_optr ::= db_optr wal */
  {  199,   -2 }, /* (104) db_optr ::= db_optr fsync */
  {  199,   -2 }, /* (105) db_optr ::= db_optr comp */
  {  199,   -2 }, /* (106) db_optr ::= db_optr prec */
  {  199,   -2 }, /* (107) db_optr ::= db_optr keep */
  {  199,   -2 }, /* (108) db_optr ::= db_optr update */
  {  199,   -2 }, /* (109) db_optr ::= db_optr cachelast */
  {  200,   -1 }, /* (110) topic_optr ::= db_optr */
  {  200,   -2 }, /* (111) topic_optr ::= topic_optr partitions */
  {  195,    0 }, /* (112) alter_db_optr ::= */
  {  195,   -2 }, /* (113) alter_db_optr ::= alter_db_optr replica */
  {  195,   -2 }, /* (114) alter_db_optr ::= alter_db_optr quorum */
  {  195,   -2 }, /* (115) alter_db_optr ::= alter_db_optr keep */
  {  195,   -2 }, /* (116) alter_db_optr ::= alter_db_optr blocks */
  {  195,   -2 }, /* (117) alter_db_optr ::= alter_db_optr comp */
  {  195,   -2 }, /* (118) alter_db_optr ::= alter_db_optr wal */
  {  195,   -2 }, /* (119) alter_db_optr ::= alter_db_optr fsync */
  {  195,   -2 }, /* (120) alter_db_optr ::= alter_db_optr update */
  {  195,   -2 }, /* (121) alter_db_optr ::= alter_db_optr cachelast */
  {  196,   -1 }, /* (122) alter_topic_optr ::= alter_db_optr */
  {  196,   -2 }, /* (123) alter_topic_optr ::= alter_topic_optr partitions */
  {  227,   -1 }, /* (124) typename ::= ids */
  {  227,   -4 }, /* (125) typename ::= ids LP signed RP */
  {  227,   -2 }, /* (126) typename ::= ids UNSIGNED */
  {  228,   -1 }, /* (127) signed ::= INTEGER */
  {  228,   -2 }, /* (128) signed ::= PLUS INTEGER */
  {  228,   -2 }, /* (129) signed ::= MINUS INTEGER */
  {  190,   -3 }, /* (130) cmd ::= CREATE TABLE create_table_args */
  {  190,   -3 }, /* (131) cmd ::= CREATE TABLE create_stable_args */
  {  190,   -3 }, /* (132) cmd ::= CREATE STABLE create_stable_args */
  {  190,   -3 }, /* (133) cmd ::= CREATE TABLE create_table_list */
  {  231,   -1 }, /* (134) create_table_list ::= create_from_stable */
  {  231,   -2 }, /* (135) create_table_list ::= create_table_list create_from_stable */
  {  229,   -6 }, /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  230,  -10 }, /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  232,  -10 }, /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  232,  -13 }, /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  234,   -3 }, /* (140) tagNamelist ::= tagNamelist COMMA ids */
  {  234,   -1 }, /* (141) tagNamelist ::= ids */
  {  229,   -5 }, /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
  {  233,   -3 }, /* (143) columnlist ::= columnlist COMMA column */
  {  233,   -1 }, /* (144) columnlist ::= column */
  {  236,   -2 }, /* (145) column ::= ids typename */
  {  211,   -3 }, /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
  {  211,   -1 }, /* (147) tagitemlist ::= tagitem */
  {  237,   -1 }, /* (148) tagitem ::= INTEGER */
  {  237,   -1 }, /* (149) tagitem ::= FLOAT */
  {  237,   -1 }, /* (150) tagitem ::= STRING */
  {  237,   -1 }, /* (151) tagitem ::= BOOL */
  {  237,   -1 }, /* (152) tagitem ::= NULL */
  {  237,   -2 }, /* (153) tagitem ::= MINUS INTEGER */
  {  237,   -2 }, /* (154) tagitem ::= MINUS FLOAT */
  {  237,   -2 }, /* (155) tagitem ::= PLUS INTEGER */
  {  237,   -2 }, /* (156) tagitem ::= PLUS FLOAT */
  {  235,  -14 }, /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  235,   -3 }, /* (158) select ::= LP select RP */
  {  251,   -1 }, /* (159) union ::= select */
  {  251,   -4 }, /* (160) union ::= union UNION ALL select */
  {  190,   -1 }, /* (161) cmd ::= union */
  {  235,   -2 }, /* (162) select ::= SELECT selcollist */
  {  252,   -2 }, /* (163) sclp ::= selcollist COMMA */
  {  252,    0 }, /* (164) sclp ::= */
  {  238,   -4 }, /* (165) selcollist ::= sclp distinct expr as */
  {  238,   -2 }, /* (166) selcollist ::= sclp STAR */
  {  255,   -2 }, /* (167) as ::= AS ids */
  {  255,   -1 }, /* (168) as ::= ids */
  {  255,    0 }, /* (169) as ::= */
  {  253,   -1 }, /* (170) distinct ::= DISTINCT */
  {  253,    0 }, /* (171) distinct ::= */
  {  239,   -2 }, /* (172) from ::= FROM tablelist */
  {  239,   -4 }, /* (173) from ::= FROM LP union RP */
  {  256,   -2 }, /* (174) tablelist ::= ids cpxName */
  {  256,   -3 }, /* (175) tablelist ::= ids cpxName ids */
  {  256,   -4 }, /* (176) tablelist ::= tablelist COMMA ids cpxName */
  {  256,   -5 }, /* (177) tablelist ::= tablelist COMMA ids cpxName ids */
  {  257,   -1 }, /* (178) tmvar ::= VARIABLE */
  {  241,   -4 }, /* (179) interval_opt ::= INTERVAL LP tmvar RP */
  {  241,   -6 }, /* (180) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  241,    0 }, /* (181) interval_opt ::= */
  {  242,    0 }, /* (182) session_option ::= */
  {  242,   -7 }, /* (183) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  243,    0 }, /* (184) windowstate_option ::= */
  {  243,   -4 }, /* (185) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  244,    0 }, /* (186) fill_opt ::= */
  {  244,   -6 }, /* (187) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  244,   -4 }, /* (188) fill_opt ::= FILL LP ID RP */
  {  245,   -4 }, /* (189) sliding_opt ::= SLIDING LP tmvar RP */
  {  245,    0 }, /* (190) sliding_opt ::= */
  {  247,    0 }, /* (191) orderby_opt ::= */
  {  247,   -3 }, /* (192) orderby_opt ::= ORDER BY sortlist */
  {  258,   -4 }, /* (193) sortlist ::= sortlist COMMA item sortorder */
  {  258,   -2 }, /* (194) sortlist ::= item sortorder */
  {  260,   -2 }, /* (195) item ::= ids cpxName */
  {  261,   -1 }, /* (196) sortorder ::= ASC */
  {  261,   -1 }, /* (197) sortorder ::= DESC */
  {  261,    0 }, /* (198) sortorder ::= */
  {  246,    0 }, /* (199) groupby_opt ::= */
  {  246,   -3 }, /* (200) groupby_opt ::= GROUP BY grouplist */
  {  262,   -3 }, /* (201) grouplist ::= grouplist COMMA item */
  {  262,   -1 }, /* (202) grouplist ::= item */
  {  248,    0 }, /* (203) having_opt ::= */
  {  248,   -2 }, /* (204) having_opt ::= HAVING expr */
  {  250,    0 }, /* (205) limit_opt ::= */
  {  250,   -2 }, /* (206) limit_opt ::= LIMIT signed */
  {  250,   -4 }, /* (207) limit_opt ::= LIMIT signed OFFSET signed */
  {  250,   -4 }, /* (208) limit_opt ::= LIMIT signed COMMA signed */
  {  249,    0 }, /* (209) slimit_opt ::= */
  {  249,   -2 }, /* (210) slimit_opt ::= SLIMIT signed */
  {  249,   -4 }, /* (211) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  249,   -4 }, /* (212) slimit_opt ::= SLIMIT signed COMMA signed */
  {  240,    0 }, /* (213) where_opt ::= */
  {  240,   -2 }, /* (214) where_opt ::= WHERE expr */
  {  254,   -3 }, /* (215) expr ::= LP expr RP */
  {  254,   -1 }, /* (216) expr ::= ID */
  {  254,   -3 }, /* (217) expr ::= ID DOT ID */
  {  254,   -3 }, /* (218) expr ::= ID DOT STAR */
  {  254,   -1 }, /* (219) expr ::= INTEGER */
  {  254,   -2 }, /* (220) expr ::= MINUS INTEGER */
  {  254,   -2 }, /* (221) expr ::= PLUS INTEGER */
  {  254,   -1 }, /* (222) expr ::= FLOAT */
  {  254,   -2 }, /* (223) expr ::= MINUS FLOAT */
  {  254,   -2 }, /* (224) expr ::= PLUS FLOAT */
  {  254,   -1 }, /* (225) expr ::= STRING */
  {  254,   -1 }, /* (226) expr ::= NOW */
  {  254,   -1 }, /* (227) expr ::= VARIABLE */
  {  254,   -2 }, /* (228) expr ::= PLUS VARIABLE */
  {  254,   -2 }, /* (229) expr ::= MINUS VARIABLE */
  {  254,   -1 }, /* (230) expr ::= BOOL */
  {  254,   -1 }, /* (231) expr ::= NULL */
  {  254,   -4 }, /* (232) expr ::= ID LP exprlist RP */
  {  254,   -4 }, /* (233) expr ::= ID LP STAR RP */
  {  254,   -3 }, /* (234) expr ::= expr IS NULL */
  {  254,   -4 }, /* (235) expr ::= expr IS NOT NULL */
  {  254,   -3 }, /* (236) expr ::= expr LT expr */
  {  254,   -3 }, /* (237) expr ::= expr GT expr */
  {  254,   -3 }, /* (238) expr ::= expr LE expr */
  {  254,   -3 }, /* (239) expr ::= expr GE expr */
  {  254,   -3 }, /* (240) expr ::= expr NE expr */
  {  254,   -3 }, /* (241) expr ::= expr EQ expr */
  {  254,   -5 }, /* (242) expr ::= expr BETWEEN expr AND expr */
  {  254,   -3 }, /* (243) expr ::= expr AND expr */
  {  254,   -3 }, /* (244) expr ::= expr OR expr */
  {  254,   -3 }, /* (245) expr ::= expr PLUS expr */
  {  254,   -3 }, /* (246) expr ::= expr MINUS expr */
  {  254,   -3 }, /* (247) expr ::= expr STAR expr */
  {  254,   -3 }, /* (248) expr ::= expr SLASH expr */
  {  254,   -3 }, /* (249) expr ::= expr REM expr */
  {  254,   -3 }, /* (250) expr ::= expr LIKE expr */
  {  254,   -5 }, /* (251) expr ::= expr IN LP exprlist RP */
  {  263,   -3 }, /* (252) exprlist ::= exprlist COMMA expritem */
  {  263,   -1 }, /* (253) exprlist ::= expritem */
  {  264,   -1 }, /* (254) expritem ::= expr */
  {  264,    0 }, /* (255) expritem ::= */
  {  190,   -3 }, /* (256) cmd ::= RESET QUERY CACHE */
  {  190,   -3 }, /* (257) cmd ::= SYNCDB ids REPLICA */
  {  190,   -7 }, /* (258) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  190,   -7 }, /* (259) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  190,   -7 }, /* (260) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  190,   -7 }, /* (261) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  190,   -8 }, /* (262) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  190,   -9 }, /* (263) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  190,   -7 }, /* (264) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  190,   -7 }, /* (265) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  190,   -7 }, /* (266) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  190,   -7 }, /* (267) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  190,   -8 }, /* (268) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  190,   -3 }, /* (269) cmd ::= KILL CONNECTION INTEGER */
  {  190,   -5 }, /* (270) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  190,   -5 }, /* (271) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 130: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==132);
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
      case 21: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
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
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
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
      case 33: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 34: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 35: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 36: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 38: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 39: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 45: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==45);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy122, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy211);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 48: /* ids ::= ID */
      case 49: /* ids ::= STRING */ yytestcase(yyruleno==49);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 50: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 51: /* ifexists ::= */
      case 53: /* ifnotexists ::= */ yytestcase(yyruleno==53);
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 52: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy122, &yymsp[-2].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 59: /* pps ::= */
      case 61: /* tseries ::= */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= */ yytestcase(yyruleno==63);
      case 65: /* streams ::= */ yytestcase(yyruleno==65);
      case 67: /* storage ::= */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= */ yytestcase(yyruleno==69);
      case 71: /* users ::= */ yytestcase(yyruleno==71);
      case 73: /* conns ::= */ yytestcase(yyruleno==73);
      case 75: /* state ::= */ yytestcase(yyruleno==75);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 60: /* pps ::= PPS INTEGER */
      case 62: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==64);
      case 66: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==70);
      case 72: /* users ::= USERS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* state ::= STATE ids */ yytestcase(yyruleno==76);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 77: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy211.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy211.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy211.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy211.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy211.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy211.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy211.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy211 = yylhsminor.yy211;
        break;
      case 78: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy291 = yymsp[0].minor.yy291; }
        break;
      case 79: /* cache ::= CACHE INTEGER */
      case 80: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==80);
      case 81: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==81);
      case 82: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==86);
      case 87: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==87);
      case 88: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==88);
      case 89: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==89);
      case 90: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==90);
      case 91: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==91);
      case 92: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==92);
      case 93: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==93);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 94: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy122); yymsp[1].minor.yy122.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 95: /* db_optr ::= db_optr cache */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 96: /* db_optr ::= db_optr replica */
      case 113: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==113);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 97: /* db_optr ::= db_optr quorum */
      case 114: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==114);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 98: /* db_optr ::= db_optr days */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 99: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 100: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 101: /* db_optr ::= db_optr blocks */
      case 116: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==116);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 102: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 103: /* db_optr ::= db_optr wal */
      case 118: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==118);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 104: /* db_optr ::= db_optr fsync */
      case 119: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==119);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 105: /* db_optr ::= db_optr comp */
      case 117: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==117);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 106: /* db_optr ::= db_optr prec */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 107: /* db_optr ::= db_optr keep */
      case 115: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==115);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.keep = yymsp[0].minor.yy291; }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 108: /* db_optr ::= db_optr update */
      case 120: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==120);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 109: /* db_optr ::= db_optr cachelast */
      case 121: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==121);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 110: /* topic_optr ::= db_optr */
      case 122: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==122);
{ yylhsminor.yy122 = yymsp[0].minor.yy122; yylhsminor.yy122.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy122 = yylhsminor.yy122;
        break;
      case 111: /* topic_optr ::= topic_optr partitions */
      case 123: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==123);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 112: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy122); yymsp[1].minor.yy122.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 124: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy153, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy153 = yylhsminor.yy153;
        break;
      case 125: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy179 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy153, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy179;  // negative value of name length
    tSetColumnType(&yylhsminor.yy153, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy153 = yylhsminor.yy153;
        break;
      case 126: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy153, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy153 = yylhsminor.yy153;
        break;
      case 127: /* signed ::= INTEGER */
{ yylhsminor.yy179 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy179 = yylhsminor.yy179;
        break;
      case 128: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy179 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 129: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy179 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 133: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy412;}
        break;
      case 134: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy446);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy412 = pCreateTable;
}
  yymsp[0].minor.yy412 = yylhsminor.yy412;
        break;
      case 135: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy412->childTableInfo, &yymsp[0].minor.yy446);
  yylhsminor.yy412 = yymsp[-1].minor.yy412;
}
  yymsp[-1].minor.yy412 = yylhsminor.yy412;
        break;
      case 136: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy412 = tSetCreateTableInfo(yymsp[-1].minor.yy291, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy412, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy412 = yylhsminor.yy412;
        break;
      case 137: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy412 = tSetCreateTableInfo(yymsp[-5].minor.yy291, yymsp[-1].minor.yy291, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy412, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy412 = yylhsminor.yy412;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy446 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy291, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy446 = yylhsminor.yy446;
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy446 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy291, yymsp[-1].minor.yy291, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy446 = yylhsminor.yy446;
        break;
      case 140: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy291, &yymsp[0].minor.yy0); yylhsminor.yy291 = yymsp[-2].minor.yy291;  }
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 141: /* tagNamelist ::= ids */
{yylhsminor.yy291 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy291, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy412 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy6, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy412, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy412 = yylhsminor.yy412;
        break;
      case 143: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy291, &yymsp[0].minor.yy153); yylhsminor.yy291 = yymsp[-2].minor.yy291;  }
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 144: /* columnlist ::= column */
{yylhsminor.yy291 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy291, &yymsp[0].minor.yy153);}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 145: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy153, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy153);
}
  yymsp[-1].minor.yy153 = yylhsminor.yy153;
        break;
      case 146: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy291 = tVariantListAppend(yymsp[-2].minor.yy291, &yymsp[0].minor.yy216, -1);    }
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 147: /* tagitemlist ::= tagitem */
{ yylhsminor.yy291 = tVariantListAppend(NULL, &yymsp[0].minor.yy216, -1); }
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 148: /* tagitem ::= INTEGER */
      case 149: /* tagitem ::= FLOAT */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= STRING */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= BOOL */ yytestcase(yyruleno==151);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy216, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy216 = yylhsminor.yy216;
        break;
      case 152: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy216, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy216 = yylhsminor.yy216;
        break;
      case 153: /* tagitem ::= MINUS INTEGER */
      case 154: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy216, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy216 = yylhsminor.yy216;
        break;
      case 157: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy6 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy291, yymsp[-11].minor.yy254, yymsp[-10].minor.yy436, yymsp[-4].minor.yy291, yymsp[-3].minor.yy291, &yymsp[-9].minor.yy400, &yymsp[-8].minor.yy139, &yymsp[-7].minor.yy158, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy291, &yymsp[0].minor.yy74, &yymsp[-1].minor.yy74, yymsp[-2].minor.yy436);
}
  yymsp[-13].minor.yy6 = yylhsminor.yy6;
        break;
      case 158: /* select ::= LP select RP */
{yymsp[-2].minor.yy6 = yymsp[-1].minor.yy6;}
        break;
      case 159: /* union ::= select */
{ yylhsminor.yy291 = setSubclause(NULL, yymsp[0].minor.yy6); }
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 160: /* union ::= union UNION ALL select */
{ yylhsminor.yy291 = appendSelectClause(yymsp[-3].minor.yy291, yymsp[0].minor.yy6); }
  yymsp[-3].minor.yy291 = yylhsminor.yy291;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy291, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy6 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy291, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy6 = yylhsminor.yy6;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy291 = yymsp[-1].minor.yy291;}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
        break;
      case 164: /* sclp ::= */
      case 191: /* orderby_opt ::= */ yytestcase(yyruleno==191);
{yymsp[1].minor.yy291 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy291 = tSqlExprListAppend(yymsp[-3].minor.yy291, yymsp[-1].minor.yy436,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy291 = yylhsminor.yy291;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy291 = tSqlExprListAppend(yymsp[-1].minor.yy291, pNode, 0, 0);
}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
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
{yymsp[-1].minor.yy254 = yymsp[0].minor.yy254;}
        break;
      case 173: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy254 = setSubquery(NULL, yymsp[-1].minor.yy291);}
        break;
      case 174: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy254 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 175: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy254 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy254 = setTableNameList(yymsp[-3].minor.yy254, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy254 = yylhsminor.yy254;
        break;
      case 177: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy254 = setTableNameList(yymsp[-4].minor.yy254, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy254 = yylhsminor.yy254;
        break;
      case 178: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy400.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy400.offset.n = 0;}
        break;
      case 180: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy400.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy400.offset = yymsp[-1].minor.yy0;}
        break;
      case 181: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 182: /* session_option ::= */
{yymsp[1].minor.yy139.col.n = 0; yymsp[1].minor.yy139.gap.n = 0;}
        break;
      case 183: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy139.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy139.gap = yymsp[-1].minor.yy0;
}
        break;
      case 184: /* windowstate_option ::= */
{yymsp[1].minor.yy158.col.n = 0;}
        break;
      case 185: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yymsp[-3].minor.yy158.col = yymsp[-1].minor.yy0;
}
        break;
      case 186: /* fill_opt ::= */
{ yymsp[1].minor.yy291 = 0;     }
        break;
      case 187: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy291, &A, -1, 0);
    yymsp[-5].minor.yy291 = yymsp[-1].minor.yy291;
}
        break;
      case 188: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy291 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 189: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 190: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 192: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy291 = yymsp[0].minor.yy291;}
        break;
      case 193: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy291 = tVariantListAppend(yymsp[-3].minor.yy291, &yymsp[-1].minor.yy216, yymsp[0].minor.yy382);
}
  yymsp[-3].minor.yy291 = yylhsminor.yy291;
        break;
      case 194: /* sortlist ::= item sortorder */
{
  yylhsminor.yy291 = tVariantListAppend(NULL, &yymsp[-1].minor.yy216, yymsp[0].minor.yy382);
}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
        break;
      case 195: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy216, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy216 = yylhsminor.yy216;
        break;
      case 196: /* sortorder ::= ASC */
{ yymsp[0].minor.yy382 = TSDB_ORDER_ASC; }
        break;
      case 197: /* sortorder ::= DESC */
{ yymsp[0].minor.yy382 = TSDB_ORDER_DESC;}
        break;
      case 198: /* sortorder ::= */
{ yymsp[1].minor.yy382 = TSDB_ORDER_ASC; }
        break;
      case 199: /* groupby_opt ::= */
{ yymsp[1].minor.yy291 = 0;}
        break;
      case 200: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy291 = yymsp[0].minor.yy291;}
        break;
      case 201: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy291 = tVariantListAppend(yymsp[-2].minor.yy291, &yymsp[0].minor.yy216, -1);
}
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 202: /* grouplist ::= item */
{
  yylhsminor.yy291 = tVariantListAppend(NULL, &yymsp[0].minor.yy216, -1);
}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 203: /* having_opt ::= */
      case 213: /* where_opt ::= */ yytestcase(yyruleno==213);
      case 255: /* expritem ::= */ yytestcase(yyruleno==255);
{yymsp[1].minor.yy436 = 0;}
        break;
      case 204: /* having_opt ::= HAVING expr */
      case 214: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==214);
{yymsp[-1].minor.yy436 = yymsp[0].minor.yy436;}
        break;
      case 205: /* limit_opt ::= */
      case 209: /* slimit_opt ::= */ yytestcase(yyruleno==209);
{yymsp[1].minor.yy74.limit = -1; yymsp[1].minor.yy74.offset = 0;}
        break;
      case 206: /* limit_opt ::= LIMIT signed */
      case 210: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==210);
{yymsp[-1].minor.yy74.limit = yymsp[0].minor.yy179;  yymsp[-1].minor.yy74.offset = 0;}
        break;
      case 207: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy74.limit = yymsp[-2].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[0].minor.yy179;}
        break;
      case 208: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy74.limit = yymsp[0].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[-2].minor.yy179;}
        break;
      case 211: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy74.limit = yymsp[-2].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[0].minor.yy179;}
        break;
      case 212: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy74.limit = yymsp[0].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[-2].minor.yy179;}
        break;
      case 215: /* expr ::= LP expr RP */
{yylhsminor.yy436 = yymsp[-1].minor.yy436; yylhsminor.yy436->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy436->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 216: /* expr ::= ID */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 217: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 218: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 219: /* expr ::= INTEGER */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 220: /* expr ::= MINUS INTEGER */
      case 221: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==221);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 222: /* expr ::= FLOAT */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 223: /* expr ::= MINUS FLOAT */
      case 224: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==224);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 225: /* expr ::= STRING */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 226: /* expr ::= NOW */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 227: /* expr ::= VARIABLE */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 228: /* expr ::= PLUS VARIABLE */
      case 229: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==229);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 230: /* expr ::= BOOL */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 231: /* expr ::= NULL */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 232: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy436 = tSqlExprCreateFunction(yymsp[-1].minor.yy291, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy436 = yylhsminor.yy436;
        break;
      case 233: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy436 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy436 = yylhsminor.yy436;
        break;
      case 234: /* expr ::= expr IS NULL */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 235: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-3].minor.yy436, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy436 = yylhsminor.yy436;
        break;
      case 236: /* expr ::= expr LT expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_LT);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 237: /* expr ::= expr GT expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_GT);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 238: /* expr ::= expr LE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_LE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 239: /* expr ::= expr GE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_GE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 240: /* expr ::= expr NE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_NE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 241: /* expr ::= expr EQ expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_EQ);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 242: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy436); yylhsminor.yy436 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy436, yymsp[-2].minor.yy436, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy436, TK_LE), TK_AND);}
  yymsp[-4].minor.yy436 = yylhsminor.yy436;
        break;
      case 243: /* expr ::= expr AND expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_AND);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 244: /* expr ::= expr OR expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_OR); }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 245: /* expr ::= expr PLUS expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_PLUS);  }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 246: /* expr ::= expr MINUS expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_MINUS); }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 247: /* expr ::= expr STAR expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_STAR);  }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 248: /* expr ::= expr SLASH expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_DIVIDE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 249: /* expr ::= expr REM expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_REM);   }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 250: /* expr ::= expr LIKE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_LIKE);  }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 251: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-4].minor.yy436, (tSqlExpr*)yymsp[-1].minor.yy291, TK_IN); }
  yymsp[-4].minor.yy436 = yylhsminor.yy436;
        break;
      case 252: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy291 = tSqlExprListAppend(yymsp[-2].minor.yy291,yymsp[0].minor.yy436,0, 0);}
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 253: /* exprlist ::= expritem */
{yylhsminor.yy291 = tSqlExprListAppend(0,yymsp[0].minor.yy436,0, 0);}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 254: /* expritem ::= expr */
{yylhsminor.yy436 = yymsp[0].minor.yy436;}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 256: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 257: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 258: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 263: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy216, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 269: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 270: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 271: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
