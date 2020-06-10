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

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "qsqlparser.h"
#include "tstoken.h"
#include "tutil.h"
#include "tvariant.h"
#include "ttokendef.h"
#include "qsqltype.h"

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
#define YYNOCODE 270
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy150;
  SQuerySQL* yy190;
  SCreateAcctSQL yy219;
  tSQLExprList* yy260;
  SSubclauseInfo* yy263;
  int64_t yy279;
  SLimitVal yy284;
  tVariantList* yy322;
  TAOS_FIELD yy325;
  tFieldList* yy369;
  SCreateDBInfo yy374;
  SCreateTableSQL* yy408;
  tSQLExpr* yy500;
  tVariant yy518;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             241
#define YYNRULE              220
#define YYNTOKEN             205
#define YY_MAX_SHIFT         240
#define YY_MIN_SHIFTREDUCE   397
#define YY_MAX_SHIFTREDUCE   616
#define YY_ERROR_ACTION      617
#define YY_ACCEPT_ACTION     618
#define YY_NO_ACTION         619
#define YY_MIN_REDUCE        620
#define YY_MAX_REDUCE        839
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
#define YY_ACTTAB_COUNT (541)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   718,  438,  717,   11,  716,  134,  618,  240,  719,  439,
 /*    10 */   721,  720,  758,   41,   43,   21,   35,   36,  153,  238,
 /*    20 */   135,   29,  135,  438,  197,   39,   37,   40,   38,  158,
 /*    30 */   827,  439,  826,   34,   33,  139,  135,   32,   31,   30,
 /*    40 */    41,   43,  747,   35,   36,  157,  827,  166,   29,  733,
 /*    50 */   103,  197,   39,   37,   40,   38,  182,   21,  103,   99,
 /*    60 */    34,   33,  755,  155,   32,   31,   30,  398,  399,  400,
 /*    70 */   401,  402,  403,  404,  405,  406,  407,  408,  409,  239,
 /*    80 */   438,  736,   41,   43,  103,   35,   36,  103,  439,  168,
 /*    90 */    29,  732,   21,  197,   39,   37,   40,   38,   32,   31,
 /*   100 */    30,   56,   34,   33,  747,  781,   32,   31,   30,   43,
 /*   110 */   185,   35,   36,  782,  823,  192,   29,   21,  154,  197,
 /*   120 */    39,   37,   40,   38,  167,  572,  733,    8,   34,   33,
 /*   130 */    61,  113,   32,   31,   30,  659,   35,   36,  126,   59,
 /*   140 */   194,   29,   58,   17,  197,   39,   37,   40,   38,  215,
 /*   150 */    26,  733,  169,   34,   33,  214,  213,   32,   31,   30,
 /*   160 */    16,  233,  208,  232,  207,  206,  205,  231,  204,  230,
 /*   170 */   229,  203,  714,  219,  703,  704,  705,  706,  707,  708,
 /*   180 */   709,  710,  711,  712,  713,  162,  585,   50,   60,  576,
 /*   190 */   175,  579,  165,  582,  234,  162,  585,  179,  178,  576,
 /*   200 */    27,  579,  734,  582,   51,  162,  585,   12,   98,  576,
 /*   210 */   736,  579,  736,  582,  228,   26,   21,  159,  160,   34,
 /*   220 */    33,  196,  836,   32,   31,   30,  148,  159,  160,   76,
 /*   230 */   822,  533,   88,   87,  142,  228,  668,  159,  160,  126,
 /*   240 */   147,  553,  554,   39,   37,   40,   38,  821,  220,  544,
 /*   250 */   733,   34,   33,   46,  501,   32,   31,   30,  517,  525,
 /*   260 */    17,  514,  151,  515,  152,  516,  184,   26,   16,  233,
 /*   270 */   140,  232,  237,  236,   95,  231,  660,  230,  229,  126,
 /*   280 */   530,   42,  217,  216,  578,   18,  581,  181,  161,  170,
 /*   290 */   171,   42,  584,  577,  150,  580,   74,   78,   83,   86,
 /*   300 */    77,   42,  584,  574,  545,  602,   80,  583,   14,   13,
 /*   310 */   141,  586,  584,  143,  507,   13,   47,  583,   46,   73,
 /*   320 */    72,  116,  117,   68,   64,   67,    3,  583,  130,  128,
 /*   330 */    91,   90,   89,  506,  201,   48,  144,   22,   22,  575,
 /*   340 */   521,  519,  522,  520,   10,    9,   85,   84,  145,  146,
 /*   350 */   137,  133,  138,  735,  136,  792,  791,  163,  788,  518,
 /*   360 */   787,  164,  757,  727,  218,  100,  749,  774,  773,  114,
 /*   370 */    26,  115,  112,  670,  202,  131,   24,  211,  667,  212,
 /*   380 */   835,   70,  834,  832,  118,  688,   25,  183,   23,  132,
 /*   390 */   657,   79,   93,  540,  655,  186,   81,   82,  653,  652,
 /*   400 */   172,  190,  127,  746,  650,  649,  648,  647,  646,  638,
 /*   410 */   129,  644,  642,   52,  640,   44,   49,  195,  761,  762,
 /*   420 */   775,  191,  193,  189,  187,   28,  210,   75,  221,  222,
 /*   430 */   223,  224,  225,  199,  226,  227,  235,   53,  616,  174,
 /*   440 */   615,  149,   62,  173,   65,  176,  177,  614,  180,  651,
 /*   450 */   607,  184,   92,  527,  645,  541,  120,  689,  121,  122,
 /*   460 */   119,  123,  125,  124,   94,  104,    1,  731,  105,  111,
 /*   470 */   108,  106,  107,  109,  110,    2,   55,   57,  101,  156,
 /*   480 */   188,    5,  546,  102,   19,    6,  587,   20,    4,   15,
 /*   490 */    63,    7,  198,  478,  200,  475,  473,  472,  471,  469,
 /*   500 */   442,  209,   66,   45,   69,   71,   22,  503,  502,  500,
 /*   510 */    54,  463,  461,  453,  459,  455,  457,  451,  449,  477,
 /*   520 */   476,  474,  470,  468,   46,  440,   96,  413,  411,  620,
 /*   530 */   619,  619,  619,  619,  619,  619,  619,  619,  619,  619,
 /*   540 */    97,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   225,    1,  227,  258,  229,  258,  206,  207,  233,    9,
 /*    10 */   235,  236,  209,   13,   14,  209,   16,   17,  208,  209,
 /*    20 */   258,   21,  258,    1,   24,   25,   26,   27,   28,  267,
 /*    30 */   268,    9,  268,   33,   34,  258,  258,   37,   38,   39,
 /*    40 */    13,   14,  242,   16,   17,  267,  268,  241,   21,  243,
 /*    50 */   209,   24,   25,   26,   27,   28,  256,  209,  209,  209,
 /*    60 */    33,   34,  259,  226,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */     1,  244,   13,   14,  209,   16,   17,  209,    9,   63,
 /*    90 */    21,  243,  209,   24,   25,   26,   27,   28,   37,   38,
 /*   100 */    39,  101,   33,   34,  242,  264,   37,   38,   39,   14,
 /*   110 */   260,   16,   17,  264,  258,  266,   21,  209,  256,   24,
 /*   120 */    25,   26,   27,   28,  241,   98,  243,   97,   33,   34,
 /*   130 */   100,  101,   37,   38,   39,  213,   16,   17,  216,  264,
 /*   140 */   262,   21,  264,   97,   24,   25,   26,   27,   28,  241,
 /*   150 */   104,  243,  126,   33,   34,  129,  130,   37,   38,   39,
 /*   160 */    85,   86,   87,   88,   89,   90,   91,   92,   93,   94,
 /*   170 */    95,   96,  225,  209,  227,  228,  229,  230,  231,  232,
 /*   180 */   233,  234,  235,  236,  237,    1,    2,  102,  245,    5,
 /*   190 */   125,    7,  226,    9,  226,    1,    2,  132,  133,    5,
 /*   200 */   257,    7,  238,    9,  119,    1,    2,   44,   97,    5,
 /*   210 */   244,    7,  244,    9,   78,  104,  209,   33,   34,   33,
 /*   220 */    34,   37,  244,   37,   38,   39,   63,   33,   34,   72,
 /*   230 */   258,   37,   69,   70,   71,   78,  213,   33,   34,  216,
 /*   240 */    77,  114,  115,   25,   26,   27,   28,  258,  241,   98,
 /*   250 */   243,   33,   34,  102,    5,   37,   38,   39,    2,   98,
 /*   260 */    97,    5,  258,    7,  258,    9,  105,  104,   85,   86,
 /*   270 */   258,   88,   60,   61,   62,   92,  213,   94,   95,  216,
 /*   280 */   102,   97,   33,   34,    5,  107,    7,  124,   59,   33,
 /*   290 */    34,   97,  108,    5,  131,    7,   64,   65,   66,   67,
 /*   300 */    68,   97,  108,    1,   98,   98,   74,  123,  102,  102,
 /*   310 */   258,   98,  108,  258,   98,  102,  102,  123,  102,  127,
 /*   320 */   128,   64,   65,   66,   67,   68,   97,  123,   64,   65,
 /*   330 */    66,   67,   68,   98,   98,  121,  258,  102,  102,   37,
 /*   340 */     5,    5,    7,    7,  127,  128,   72,   73,  258,  258,
 /*   350 */   258,  258,  258,  244,  258,  239,  239,  239,  239,  103,
 /*   360 */   239,  239,  209,  240,  239,  209,  242,  265,  265,  209,
 /*   370 */   104,  209,  246,  209,  209,  209,  209,  209,  209,  209,
 /*   380 */   209,  209,  209,  209,  209,  209,  209,  242,  209,  209,
 /*   390 */   209,  209,   59,  108,  209,  261,  209,  209,  209,  209,
 /*   400 */   209,  261,  209,  255,  209,  209,  209,  209,  209,  209,
 /*   410 */   209,  209,  209,  118,  209,  117,  120,  112,  210,  210,
 /*   420 */   210,  111,  116,  110,  109,  122,   75,   84,   83,   49,
 /*   430 */    80,   82,   53,  210,   81,   79,   75,  210,    5,    5,
 /*   440 */     5,  210,  214,  134,  214,  134,    5,    5,  125,  210,
 /*   450 */    87,  105,  211,   98,  210,   98,  222,  224,  218,  221,
 /*   460 */   223,  219,  217,  220,  211,  254,  215,  242,  253,  247,
 /*   470 */   250,  252,  251,  249,  248,  212,  106,  102,   97,    1,
 /*   480 */    97,  113,   98,   97,  102,  113,   98,  102,   97,   97,
 /*   490 */    72,   97,   99,    9,   99,    5,    5,    5,    5,    5,
 /*   500 */    76,   15,   72,   16,  128,  128,  102,    5,    5,   98,
 /*   510 */    97,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   520 */     5,    5,    5,    5,  102,   76,   21,   59,   58,    0,
 /*   530 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   540 */    21,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   550 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   560 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   570 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   580 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   590 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   600 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   610 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   620 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   630 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   640 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   650 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   660 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   670 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   680 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   690 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   700 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   710 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   720 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   730 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   740 */   269,  269,  269,  269,  269,  269,
};
#define YY_SHIFT_COUNT    (240)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (529)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   163,   75,  183,  184,  204,   79,   79,   79,   79,   79,
 /*    10 */    79,    0,   22,  204,  256,  256,  256,   46,   79,   79,
 /*    20 */    79,   79,   79,  157,  136,  136,  541,  194,  204,  204,
 /*    30 */   204,  204,  204,  204,  204,  204,  204,  204,  204,  204,
 /*    40 */   204,  204,  204,  204,  204,  256,  256,  249,  249,  249,
 /*    50 */   249,  249,  249,   30,  249,  111,   79,   79,  127,  127,
 /*    60 */   178,   79,   79,   79,   79,   79,   79,   79,   79,   79,
 /*    70 */    79,   79,   79,   79,   79,   79,   79,   79,   79,   79,
 /*    80 */    79,   79,   79,   79,   79,   79,   79,   79,   79,   79,
 /*    90 */    79,   79,   79,   79,   79,   79,   79,   79,  266,  333,
 /*   100 */   333,  285,  285,  333,  295,  296,  298,  305,  306,  310,
 /*   110 */   313,  315,  303,  266,  333,  333,  351,  351,  333,  343,
 /*   120 */   345,  380,  350,  349,  379,  353,  356,  333,  361,  333,
 /*   130 */   361,  541,  541,   27,   69,   69,   69,   95,  120,  218,
 /*   140 */   218,  218,  232,  186,  186,  186,  186,  257,  264,   26,
 /*   150 */    65,   61,   61,  212,  161,  151,  206,  207,  213,  279,
 /*   160 */   288,  302,  229,  214,   85,  216,  235,  236,  192,  217,
 /*   170 */   335,  336,  274,  433,  309,  434,  435,  311,  441,  442,
 /*   180 */   363,  323,  346,  355,  370,  375,  357,  381,  478,  383,
 /*   190 */   384,  386,  382,  368,  385,  372,  388,  391,  392,  393,
 /*   200 */   394,  395,  418,  484,  490,  491,  492,  493,  494,  424,
 /*   210 */   486,  430,  487,  376,  377,  404,  502,  503,  411,  413,
 /*   220 */   404,  506,  507,  508,  509,  510,  511,  512,  513,  514,
 /*   230 */   515,  516,  517,  518,  422,  449,  505,  519,  468,  470,
 /*   240 */   529,
};
#define YY_REDUCE_COUNT (132)
#define YY_REDUCE_MIN   (-255)
#define YY_REDUCE_MAX   (263)
static const short yy_reduce_ofst[] = {
 /*     0 */  -200,  -53, -225, -238, -222, -151, -122, -194, -117,  -92,
 /*    10 */     7, -197, -190, -236, -163,  -34,  -32, -138, -150, -159,
 /*    20 */  -125,  -36, -152,  -78,   23,   63,  -57, -255, -253, -223,
 /*    30 */  -144,  -28,  -11,    4,    6,   12,   52,   55,   78,   90,
 /*    40 */    91,   92,   93,   94,   96,  -22,  109,  116,  117,  118,
 /*    50 */   119,  121,  122,  123,  125,  124,  153,  156,  102,  103,
 /*    60 */   126,  160,  162,  164,  165,  166,  167,  168,  169,  170,
 /*    70 */   171,  172,  173,  174,  175,  176,  177,  179,  180,  181,
 /*    80 */   182,  185,  187,  188,  189,  190,  191,  193,  195,  196,
 /*    90 */   197,  198,  199,  200,  201,  202,  203,  205,  145,  208,
 /*   100 */   209,  134,  140,  210,  148,  211,  215,  219,  221,  220,
 /*   110 */   224,  226,  222,  225,  223,  227,  228,  230,  231,  233,
 /*   120 */   237,  234,  240,  238,  242,  243,  245,  239,  241,  244,
 /*   130 */   253,  251,  263,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   617,  669,  658,  829,  829,  617,  617,  617,  617,  617,
 /*    10 */   617,  759,  635,  829,  617,  617,  617,  617,  617,  617,
 /*    20 */   617,  617,  617,  671,  671,  671,  754,  617,  617,  617,
 /*    30 */   617,  617,  617,  617,  617,  617,  617,  617,  617,  617,
 /*    40 */   617,  617,  617,  617,  617,  617,  617,  617,  617,  617,
 /*    50 */   617,  617,  617,  617,  617,  617,  617,  617,  778,  778,
 /*    60 */   752,  617,  617,  617,  617,  617,  617,  617,  617,  617,
 /*    70 */   617,  617,  617,  617,  617,  617,  617,  617,  617,  656,
 /*    80 */   617,  654,  617,  617,  617,  617,  617,  617,  617,  617,
 /*    90 */   617,  617,  617,  617,  617,  643,  617,  617,  617,  637,
 /*   100 */   637,  617,  617,  637,  785,  789,  783,  771,  779,  770,
 /*   110 */   766,  765,  793,  617,  637,  637,  666,  666,  637,  687,
 /*   120 */   685,  683,  675,  681,  677,  679,  673,  637,  664,  637,
 /*   130 */   664,  702,  715,  617,  794,  828,  784,  812,  811,  824,
 /*   140 */   818,  817,  617,  816,  815,  814,  813,  617,  617,  617,
 /*   150 */   617,  820,  819,  617,  617,  617,  617,  617,  617,  617,
 /*   160 */   617,  617,  796,  790,  786,  617,  617,  617,  617,  617,
 /*   170 */   617,  617,  617,  617,  617,  617,  617,  617,  617,  617,
 /*   180 */   617,  617,  751,  617,  617,  760,  617,  617,  617,  617,
 /*   190 */   617,  617,  780,  617,  772,  617,  617,  617,  617,  617,
 /*   200 */   617,  728,  617,  617,  617,  617,  617,  617,  617,  617,
 /*   210 */   617,  617,  617,  617,  617,  833,  617,  617,  617,  722,
 /*   220 */   831,  617,  617,  617,  617,  617,  617,  617,  617,  617,
 /*   230 */   617,  617,  617,  617,  690,  617,  641,  639,  617,  633,
 /*   240 */   617,
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
    0,  /*    CONFIGS => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*     VNODES => nothing */
    1,  /*    IPTOKEN => ID */
    0,  /*        DOT => nothing */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
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
    0,  /*     CREATE => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*       KEEP => nothing */
    0,  /*  MAXTABLES => nothing */
    0,  /*      CACHE => nothing */
    0,  /*    REPLICA => nothing */
    0,  /*       DAYS => nothing */
    0,  /*    MINROWS => nothing */
    0,  /*    MAXROWS => nothing */
    0,  /*     BLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*        WAL => nothing */
    0,  /*       COMP => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
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
  /*   54 */ "CONFIGS",
  /*   55 */ "SCORES",
  /*   56 */ "GRANTS",
  /*   57 */ "VNODES",
  /*   58 */ "IPTOKEN",
  /*   59 */ "DOT",
  /*   60 */ "TABLES",
  /*   61 */ "STABLES",
  /*   62 */ "VGROUPS",
  /*   63 */ "DROP",
  /*   64 */ "TABLE",
  /*   65 */ "DATABASE",
  /*   66 */ "DNODE",
  /*   67 */ "USER",
  /*   68 */ "ACCOUNT",
  /*   69 */ "USE",
  /*   70 */ "DESCRIBE",
  /*   71 */ "ALTER",
  /*   72 */ "PASS",
  /*   73 */ "PRIVILEGE",
  /*   74 */ "LOCAL",
  /*   75 */ "IF",
  /*   76 */ "EXISTS",
  /*   77 */ "CREATE",
  /*   78 */ "PPS",
  /*   79 */ "TSERIES",
  /*   80 */ "DBS",
  /*   81 */ "STORAGE",
  /*   82 */ "QTIME",
  /*   83 */ "CONNS",
  /*   84 */ "STATE",
  /*   85 */ "KEEP",
  /*   86 */ "MAXTABLES",
  /*   87 */ "CACHE",
  /*   88 */ "REPLICA",
  /*   89 */ "DAYS",
  /*   90 */ "MINROWS",
  /*   91 */ "MAXROWS",
  /*   92 */ "BLOCKS",
  /*   93 */ "CTIME",
  /*   94 */ "WAL",
  /*   95 */ "COMP",
  /*   96 */ "PRECISION",
  /*   97 */ "LP",
  /*   98 */ "RP",
  /*   99 */ "TAGS",
  /*  100 */ "USING",
  /*  101 */ "AS",
  /*  102 */ "COMMA",
  /*  103 */ "NULL",
  /*  104 */ "SELECT",
  /*  105 */ "UNION",
  /*  106 */ "ALL",
  /*  107 */ "FROM",
  /*  108 */ "VARIABLE",
  /*  109 */ "INTERVAL",
  /*  110 */ "FILL",
  /*  111 */ "SLIDING",
  /*  112 */ "ORDER",
  /*  113 */ "BY",
  /*  114 */ "ASC",
  /*  115 */ "DESC",
  /*  116 */ "GROUP",
  /*  117 */ "HAVING",
  /*  118 */ "LIMIT",
  /*  119 */ "OFFSET",
  /*  120 */ "SLIMIT",
  /*  121 */ "SOFFSET",
  /*  122 */ "WHERE",
  /*  123 */ "NOW",
  /*  124 */ "RESET",
  /*  125 */ "QUERY",
  /*  126 */ "ADD",
  /*  127 */ "COLUMN",
  /*  128 */ "TAG",
  /*  129 */ "CHANGE",
  /*  130 */ "SET",
  /*  131 */ "KILL",
  /*  132 */ "CONNECTION",
  /*  133 */ "STREAM",
  /*  134 */ "COLON",
  /*  135 */ "ABORT",
  /*  136 */ "AFTER",
  /*  137 */ "ATTACH",
  /*  138 */ "BEFORE",
  /*  139 */ "BEGIN",
  /*  140 */ "CASCADE",
  /*  141 */ "CLUSTER",
  /*  142 */ "CONFLICT",
  /*  143 */ "COPY",
  /*  144 */ "DEFERRED",
  /*  145 */ "DELIMITERS",
  /*  146 */ "DETACH",
  /*  147 */ "EACH",
  /*  148 */ "END",
  /*  149 */ "EXPLAIN",
  /*  150 */ "FAIL",
  /*  151 */ "FOR",
  /*  152 */ "IGNORE",
  /*  153 */ "IMMEDIATE",
  /*  154 */ "INITIALLY",
  /*  155 */ "INSTEAD",
  /*  156 */ "MATCH",
  /*  157 */ "KEY",
  /*  158 */ "OF",
  /*  159 */ "RAISE",
  /*  160 */ "REPLACE",
  /*  161 */ "RESTRICT",
  /*  162 */ "ROW",
  /*  163 */ "STATEMENT",
  /*  164 */ "TRIGGER",
  /*  165 */ "VIEW",
  /*  166 */ "COUNT",
  /*  167 */ "SUM",
  /*  168 */ "AVG",
  /*  169 */ "MIN",
  /*  170 */ "MAX",
  /*  171 */ "FIRST",
  /*  172 */ "LAST",
  /*  173 */ "TOP",
  /*  174 */ "BOTTOM",
  /*  175 */ "STDDEV",
  /*  176 */ "PERCENTILE",
  /*  177 */ "APERCENTILE",
  /*  178 */ "LEASTSQUARES",
  /*  179 */ "HISTOGRAM",
  /*  180 */ "DIFF",
  /*  181 */ "SPREAD",
  /*  182 */ "TWA",
  /*  183 */ "INTERP",
  /*  184 */ "LAST_ROW",
  /*  185 */ "RATE",
  /*  186 */ "IRATE",
  /*  187 */ "SUM_RATE",
  /*  188 */ "SUM_IRATE",
  /*  189 */ "AVG_RATE",
  /*  190 */ "AVG_IRATE",
  /*  191 */ "TBID",
  /*  192 */ "SEMI",
  /*  193 */ "NONE",
  /*  194 */ "PREV",
  /*  195 */ "LINEAR",
  /*  196 */ "IMPORT",
  /*  197 */ "METRIC",
  /*  198 */ "TBNAME",
  /*  199 */ "JOIN",
  /*  200 */ "METRICS",
  /*  201 */ "STABLE",
  /*  202 */ "INSERT",
  /*  203 */ "INTO",
  /*  204 */ "VALUES",
  /*  205 */ "error",
  /*  206 */ "program",
  /*  207 */ "cmd",
  /*  208 */ "dbPrefix",
  /*  209 */ "ids",
  /*  210 */ "cpxName",
  /*  211 */ "ifexists",
  /*  212 */ "alter_db_optr",
  /*  213 */ "acct_optr",
  /*  214 */ "ifnotexists",
  /*  215 */ "db_optr",
  /*  216 */ "pps",
  /*  217 */ "tseries",
  /*  218 */ "dbs",
  /*  219 */ "streams",
  /*  220 */ "storage",
  /*  221 */ "qtime",
  /*  222 */ "users",
  /*  223 */ "conns",
  /*  224 */ "state",
  /*  225 */ "keep",
  /*  226 */ "tagitemlist",
  /*  227 */ "tables",
  /*  228 */ "cache",
  /*  229 */ "replica",
  /*  230 */ "days",
  /*  231 */ "minrows",
  /*  232 */ "maxrows",
  /*  233 */ "blocks",
  /*  234 */ "ctime",
  /*  235 */ "wal",
  /*  236 */ "comp",
  /*  237 */ "prec",
  /*  238 */ "typename",
  /*  239 */ "signed",
  /*  240 */ "create_table_args",
  /*  241 */ "columnlist",
  /*  242 */ "select",
  /*  243 */ "column",
  /*  244 */ "tagitem",
  /*  245 */ "selcollist",
  /*  246 */ "from",
  /*  247 */ "where_opt",
  /*  248 */ "interval_opt",
  /*  249 */ "fill_opt",
  /*  250 */ "sliding_opt",
  /*  251 */ "groupby_opt",
  /*  252 */ "orderby_opt",
  /*  253 */ "having_opt",
  /*  254 */ "slimit_opt",
  /*  255 */ "limit_opt",
  /*  256 */ "union",
  /*  257 */ "sclp",
  /*  258 */ "expr",
  /*  259 */ "as",
  /*  260 */ "tablelist",
  /*  261 */ "tmvar",
  /*  262 */ "sortlist",
  /*  263 */ "sortitem",
  /*  264 */ "item",
  /*  265 */ "sortorder",
  /*  266 */ "grouplist",
  /*  267 */ "exprlist",
  /*  268 */ "expritem",
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
 /*  10 */ "cmd ::= SHOW CONFIGS",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "cmd ::= SHOW VNODES",
 /*  14 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  15 */ "dbPrefix ::=",
 /*  16 */ "dbPrefix ::= ids DOT",
 /*  17 */ "cpxName ::=",
 /*  18 */ "cpxName ::= DOT ids",
 /*  19 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  20 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  22 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  24 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  25 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  26 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  27 */ "cmd ::= DROP DNODE ids",
 /*  28 */ "cmd ::= DROP USER ids",
 /*  29 */ "cmd ::= DROP ACCOUNT ids",
 /*  30 */ "cmd ::= USE ids",
 /*  31 */ "cmd ::= DESCRIBE ids cpxName",
 /*  32 */ "cmd ::= ALTER USER ids PASS ids",
 /*  33 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  34 */ "cmd ::= ALTER DNODE ids ids",
 /*  35 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  36 */ "cmd ::= ALTER LOCAL ids",
 /*  37 */ "cmd ::= ALTER LOCAL ids ids",
 /*  38 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  39 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  40 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  41 */ "ids ::= ID",
 /*  42 */ "ids ::= STRING",
 /*  43 */ "ifexists ::= IF EXISTS",
 /*  44 */ "ifexists ::=",
 /*  45 */ "ifnotexists ::= IF NOT EXISTS",
 /*  46 */ "ifnotexists ::=",
 /*  47 */ "cmd ::= CREATE DNODE ids",
 /*  48 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  49 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  50 */ "cmd ::= CREATE USER ids PASS ids",
 /*  51 */ "pps ::=",
 /*  52 */ "pps ::= PPS INTEGER",
 /*  53 */ "tseries ::=",
 /*  54 */ "tseries ::= TSERIES INTEGER",
 /*  55 */ "dbs ::=",
 /*  56 */ "dbs ::= DBS INTEGER",
 /*  57 */ "streams ::=",
 /*  58 */ "streams ::= STREAMS INTEGER",
 /*  59 */ "storage ::=",
 /*  60 */ "storage ::= STORAGE INTEGER",
 /*  61 */ "qtime ::=",
 /*  62 */ "qtime ::= QTIME INTEGER",
 /*  63 */ "users ::=",
 /*  64 */ "users ::= USERS INTEGER",
 /*  65 */ "conns ::=",
 /*  66 */ "conns ::= CONNS INTEGER",
 /*  67 */ "state ::=",
 /*  68 */ "state ::= STATE ids",
 /*  69 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  70 */ "keep ::= KEEP tagitemlist",
 /*  71 */ "tables ::= MAXTABLES INTEGER",
 /*  72 */ "cache ::= CACHE INTEGER",
 /*  73 */ "replica ::= REPLICA INTEGER",
 /*  74 */ "days ::= DAYS INTEGER",
 /*  75 */ "minrows ::= MINROWS INTEGER",
 /*  76 */ "maxrows ::= MAXROWS INTEGER",
 /*  77 */ "blocks ::= BLOCKS INTEGER",
 /*  78 */ "ctime ::= CTIME INTEGER",
 /*  79 */ "wal ::= WAL INTEGER",
 /*  80 */ "comp ::= COMP INTEGER",
 /*  81 */ "prec ::= PRECISION STRING",
 /*  82 */ "db_optr ::=",
 /*  83 */ "db_optr ::= db_optr tables",
 /*  84 */ "db_optr ::= db_optr cache",
 /*  85 */ "db_optr ::= db_optr replica",
 /*  86 */ "db_optr ::= db_optr days",
 /*  87 */ "db_optr ::= db_optr minrows",
 /*  88 */ "db_optr ::= db_optr maxrows",
 /*  89 */ "db_optr ::= db_optr blocks",
 /*  90 */ "db_optr ::= db_optr ctime",
 /*  91 */ "db_optr ::= db_optr wal",
 /*  92 */ "db_optr ::= db_optr comp",
 /*  93 */ "db_optr ::= db_optr prec",
 /*  94 */ "db_optr ::= db_optr keep",
 /*  95 */ "alter_db_optr ::=",
 /*  96 */ "alter_db_optr ::= alter_db_optr replica",
 /*  97 */ "alter_db_optr ::= alter_db_optr tables",
 /*  98 */ "alter_db_optr ::= alter_db_optr keep",
 /*  99 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 100 */ "alter_db_optr ::= alter_db_optr comp",
 /* 101 */ "alter_db_optr ::= alter_db_optr wal",
 /* 102 */ "typename ::= ids",
 /* 103 */ "typename ::= ids LP signed RP",
 /* 104 */ "signed ::= INTEGER",
 /* 105 */ "signed ::= PLUS INTEGER",
 /* 106 */ "signed ::= MINUS INTEGER",
 /* 107 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 108 */ "create_table_args ::= LP columnlist RP",
 /* 109 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 110 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 111 */ "create_table_args ::= AS select",
 /* 112 */ "columnlist ::= columnlist COMMA column",
 /* 113 */ "columnlist ::= column",
 /* 114 */ "column ::= ids typename",
 /* 115 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 116 */ "tagitemlist ::= tagitem",
 /* 117 */ "tagitem ::= INTEGER",
 /* 118 */ "tagitem ::= FLOAT",
 /* 119 */ "tagitem ::= STRING",
 /* 120 */ "tagitem ::= BOOL",
 /* 121 */ "tagitem ::= NULL",
 /* 122 */ "tagitem ::= MINUS INTEGER",
 /* 123 */ "tagitem ::= MINUS FLOAT",
 /* 124 */ "tagitem ::= PLUS INTEGER",
 /* 125 */ "tagitem ::= PLUS FLOAT",
 /* 126 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 127 */ "union ::= select",
 /* 128 */ "union ::= LP union RP",
 /* 129 */ "union ::= union UNION ALL select",
 /* 130 */ "union ::= union UNION ALL LP select RP",
 /* 131 */ "cmd ::= union",
 /* 132 */ "select ::= SELECT selcollist",
 /* 133 */ "sclp ::= selcollist COMMA",
 /* 134 */ "sclp ::=",
 /* 135 */ "selcollist ::= sclp expr as",
 /* 136 */ "selcollist ::= sclp STAR",
 /* 137 */ "as ::= AS ids",
 /* 138 */ "as ::= ids",
 /* 139 */ "as ::=",
 /* 140 */ "from ::= FROM tablelist",
 /* 141 */ "tablelist ::= ids cpxName",
 /* 142 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 143 */ "tmvar ::= VARIABLE",
 /* 144 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 145 */ "interval_opt ::=",
 /* 146 */ "fill_opt ::=",
 /* 147 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 148 */ "fill_opt ::= FILL LP ID RP",
 /* 149 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 150 */ "sliding_opt ::=",
 /* 151 */ "orderby_opt ::=",
 /* 152 */ "orderby_opt ::= ORDER BY sortlist",
 /* 153 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 154 */ "sortlist ::= item sortorder",
 /* 155 */ "item ::= ids cpxName",
 /* 156 */ "sortorder ::= ASC",
 /* 157 */ "sortorder ::= DESC",
 /* 158 */ "sortorder ::=",
 /* 159 */ "groupby_opt ::=",
 /* 160 */ "groupby_opt ::= GROUP BY grouplist",
 /* 161 */ "grouplist ::= grouplist COMMA item",
 /* 162 */ "grouplist ::= item",
 /* 163 */ "having_opt ::=",
 /* 164 */ "having_opt ::= HAVING expr",
 /* 165 */ "limit_opt ::=",
 /* 166 */ "limit_opt ::= LIMIT signed",
 /* 167 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 168 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 169 */ "slimit_opt ::=",
 /* 170 */ "slimit_opt ::= SLIMIT signed",
 /* 171 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 172 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 173 */ "where_opt ::=",
 /* 174 */ "where_opt ::= WHERE expr",
 /* 175 */ "expr ::= LP expr RP",
 /* 176 */ "expr ::= ID",
 /* 177 */ "expr ::= ID DOT ID",
 /* 178 */ "expr ::= ID DOT STAR",
 /* 179 */ "expr ::= INTEGER",
 /* 180 */ "expr ::= MINUS INTEGER",
 /* 181 */ "expr ::= PLUS INTEGER",
 /* 182 */ "expr ::= FLOAT",
 /* 183 */ "expr ::= MINUS FLOAT",
 /* 184 */ "expr ::= PLUS FLOAT",
 /* 185 */ "expr ::= STRING",
 /* 186 */ "expr ::= NOW",
 /* 187 */ "expr ::= VARIABLE",
 /* 188 */ "expr ::= BOOL",
 /* 189 */ "expr ::= ID LP exprlist RP",
 /* 190 */ "expr ::= ID LP STAR RP",
 /* 191 */ "expr ::= expr AND expr",
 /* 192 */ "expr ::= expr OR expr",
 /* 193 */ "expr ::= expr LT expr",
 /* 194 */ "expr ::= expr GT expr",
 /* 195 */ "expr ::= expr LE expr",
 /* 196 */ "expr ::= expr GE expr",
 /* 197 */ "expr ::= expr NE expr",
 /* 198 */ "expr ::= expr EQ expr",
 /* 199 */ "expr ::= expr PLUS expr",
 /* 200 */ "expr ::= expr MINUS expr",
 /* 201 */ "expr ::= expr STAR expr",
 /* 202 */ "expr ::= expr SLASH expr",
 /* 203 */ "expr ::= expr REM expr",
 /* 204 */ "expr ::= expr LIKE expr",
 /* 205 */ "expr ::= expr IN LP exprlist RP",
 /* 206 */ "exprlist ::= exprlist COMMA expritem",
 /* 207 */ "exprlist ::= expritem",
 /* 208 */ "expritem ::= expr",
 /* 209 */ "expritem ::=",
 /* 210 */ "cmd ::= RESET QUERY CACHE",
 /* 211 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 212 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 213 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 214 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 215 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 216 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 217 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 218 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 219 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 225: /* keep */
    case 226: /* tagitemlist */
    case 249: /* fill_opt */
    case 251: /* groupby_opt */
    case 252: /* orderby_opt */
    case 262: /* sortlist */
    case 266: /* grouplist */
{
tVariantListDestroy((yypminor->yy322));
}
      break;
    case 241: /* columnlist */
{
tFieldListDestroy((yypminor->yy369));
}
      break;
    case 242: /* select */
{
doDestroyQuerySql((yypminor->yy190));
}
      break;
    case 245: /* selcollist */
    case 257: /* sclp */
    case 267: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy260));
}
      break;
    case 247: /* where_opt */
    case 253: /* having_opt */
    case 258: /* expr */
    case 268: /* expritem */
{
tSQLExprDestroy((yypminor->yy500));
}
      break;
    case 256: /* union */
{
destroyAllSelectClause((yypminor->yy263));
}
      break;
    case 263: /* sortitem */
{
tVariantDestroy(&(yypminor->yy518));
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
  {  206,   -1 }, /* (0) program ::= cmd */
  {  207,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  207,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  207,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  207,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  207,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  207,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  207,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  207,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  207,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  207,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  207,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  207,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  207,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  207,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  208,    0 }, /* (15) dbPrefix ::= */
  {  208,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  210,    0 }, /* (17) cpxName ::= */
  {  210,   -2 }, /* (18) cpxName ::= DOT ids */
  {  207,   -3 }, /* (19) cmd ::= SHOW dbPrefix TABLES */
  {  207,   -5 }, /* (20) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  207,   -3 }, /* (21) cmd ::= SHOW dbPrefix STABLES */
  {  207,   -5 }, /* (22) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  207,   -3 }, /* (23) cmd ::= SHOW dbPrefix VGROUPS */
  {  207,   -4 }, /* (24) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  207,   -5 }, /* (25) cmd ::= DROP TABLE ifexists ids cpxName */
  {  207,   -4 }, /* (26) cmd ::= DROP DATABASE ifexists ids */
  {  207,   -3 }, /* (27) cmd ::= DROP DNODE ids */
  {  207,   -3 }, /* (28) cmd ::= DROP USER ids */
  {  207,   -3 }, /* (29) cmd ::= DROP ACCOUNT ids */
  {  207,   -2 }, /* (30) cmd ::= USE ids */
  {  207,   -3 }, /* (31) cmd ::= DESCRIBE ids cpxName */
  {  207,   -5 }, /* (32) cmd ::= ALTER USER ids PASS ids */
  {  207,   -5 }, /* (33) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  207,   -4 }, /* (34) cmd ::= ALTER DNODE ids ids */
  {  207,   -5 }, /* (35) cmd ::= ALTER DNODE ids ids ids */
  {  207,   -3 }, /* (36) cmd ::= ALTER LOCAL ids */
  {  207,   -4 }, /* (37) cmd ::= ALTER LOCAL ids ids */
  {  207,   -4 }, /* (38) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  207,   -4 }, /* (39) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  207,   -6 }, /* (40) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  209,   -1 }, /* (41) ids ::= ID */
  {  209,   -1 }, /* (42) ids ::= STRING */
  {  211,   -2 }, /* (43) ifexists ::= IF EXISTS */
  {  211,    0 }, /* (44) ifexists ::= */
  {  214,   -3 }, /* (45) ifnotexists ::= IF NOT EXISTS */
  {  214,    0 }, /* (46) ifnotexists ::= */
  {  207,   -3 }, /* (47) cmd ::= CREATE DNODE ids */
  {  207,   -6 }, /* (48) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  207,   -5 }, /* (49) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  207,   -5 }, /* (50) cmd ::= CREATE USER ids PASS ids */
  {  216,    0 }, /* (51) pps ::= */
  {  216,   -2 }, /* (52) pps ::= PPS INTEGER */
  {  217,    0 }, /* (53) tseries ::= */
  {  217,   -2 }, /* (54) tseries ::= TSERIES INTEGER */
  {  218,    0 }, /* (55) dbs ::= */
  {  218,   -2 }, /* (56) dbs ::= DBS INTEGER */
  {  219,    0 }, /* (57) streams ::= */
  {  219,   -2 }, /* (58) streams ::= STREAMS INTEGER */
  {  220,    0 }, /* (59) storage ::= */
  {  220,   -2 }, /* (60) storage ::= STORAGE INTEGER */
  {  221,    0 }, /* (61) qtime ::= */
  {  221,   -2 }, /* (62) qtime ::= QTIME INTEGER */
  {  222,    0 }, /* (63) users ::= */
  {  222,   -2 }, /* (64) users ::= USERS INTEGER */
  {  223,    0 }, /* (65) conns ::= */
  {  223,   -2 }, /* (66) conns ::= CONNS INTEGER */
  {  224,    0 }, /* (67) state ::= */
  {  224,   -2 }, /* (68) state ::= STATE ids */
  {  213,   -9 }, /* (69) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  225,   -2 }, /* (70) keep ::= KEEP tagitemlist */
  {  227,   -2 }, /* (71) tables ::= MAXTABLES INTEGER */
  {  228,   -2 }, /* (72) cache ::= CACHE INTEGER */
  {  229,   -2 }, /* (73) replica ::= REPLICA INTEGER */
  {  230,   -2 }, /* (74) days ::= DAYS INTEGER */
  {  231,   -2 }, /* (75) minrows ::= MINROWS INTEGER */
  {  232,   -2 }, /* (76) maxrows ::= MAXROWS INTEGER */
  {  233,   -2 }, /* (77) blocks ::= BLOCKS INTEGER */
  {  234,   -2 }, /* (78) ctime ::= CTIME INTEGER */
  {  235,   -2 }, /* (79) wal ::= WAL INTEGER */
  {  236,   -2 }, /* (80) comp ::= COMP INTEGER */
  {  237,   -2 }, /* (81) prec ::= PRECISION STRING */
  {  215,    0 }, /* (82) db_optr ::= */
  {  215,   -2 }, /* (83) db_optr ::= db_optr tables */
  {  215,   -2 }, /* (84) db_optr ::= db_optr cache */
  {  215,   -2 }, /* (85) db_optr ::= db_optr replica */
  {  215,   -2 }, /* (86) db_optr ::= db_optr days */
  {  215,   -2 }, /* (87) db_optr ::= db_optr minrows */
  {  215,   -2 }, /* (88) db_optr ::= db_optr maxrows */
  {  215,   -2 }, /* (89) db_optr ::= db_optr blocks */
  {  215,   -2 }, /* (90) db_optr ::= db_optr ctime */
  {  215,   -2 }, /* (91) db_optr ::= db_optr wal */
  {  215,   -2 }, /* (92) db_optr ::= db_optr comp */
  {  215,   -2 }, /* (93) db_optr ::= db_optr prec */
  {  215,   -2 }, /* (94) db_optr ::= db_optr keep */
  {  212,    0 }, /* (95) alter_db_optr ::= */
  {  212,   -2 }, /* (96) alter_db_optr ::= alter_db_optr replica */
  {  212,   -2 }, /* (97) alter_db_optr ::= alter_db_optr tables */
  {  212,   -2 }, /* (98) alter_db_optr ::= alter_db_optr keep */
  {  212,   -2 }, /* (99) alter_db_optr ::= alter_db_optr blocks */
  {  212,   -2 }, /* (100) alter_db_optr ::= alter_db_optr comp */
  {  212,   -2 }, /* (101) alter_db_optr ::= alter_db_optr wal */
  {  238,   -1 }, /* (102) typename ::= ids */
  {  238,   -4 }, /* (103) typename ::= ids LP signed RP */
  {  239,   -1 }, /* (104) signed ::= INTEGER */
  {  239,   -2 }, /* (105) signed ::= PLUS INTEGER */
  {  239,   -2 }, /* (106) signed ::= MINUS INTEGER */
  {  207,   -6 }, /* (107) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  240,   -3 }, /* (108) create_table_args ::= LP columnlist RP */
  {  240,   -7 }, /* (109) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  240,   -7 }, /* (110) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  240,   -2 }, /* (111) create_table_args ::= AS select */
  {  241,   -3 }, /* (112) columnlist ::= columnlist COMMA column */
  {  241,   -1 }, /* (113) columnlist ::= column */
  {  243,   -2 }, /* (114) column ::= ids typename */
  {  226,   -3 }, /* (115) tagitemlist ::= tagitemlist COMMA tagitem */
  {  226,   -1 }, /* (116) tagitemlist ::= tagitem */
  {  244,   -1 }, /* (117) tagitem ::= INTEGER */
  {  244,   -1 }, /* (118) tagitem ::= FLOAT */
  {  244,   -1 }, /* (119) tagitem ::= STRING */
  {  244,   -1 }, /* (120) tagitem ::= BOOL */
  {  244,   -1 }, /* (121) tagitem ::= NULL */
  {  244,   -2 }, /* (122) tagitem ::= MINUS INTEGER */
  {  244,   -2 }, /* (123) tagitem ::= MINUS FLOAT */
  {  244,   -2 }, /* (124) tagitem ::= PLUS INTEGER */
  {  244,   -2 }, /* (125) tagitem ::= PLUS FLOAT */
  {  242,  -12 }, /* (126) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  256,   -1 }, /* (127) union ::= select */
  {  256,   -3 }, /* (128) union ::= LP union RP */
  {  256,   -4 }, /* (129) union ::= union UNION ALL select */
  {  256,   -6 }, /* (130) union ::= union UNION ALL LP select RP */
  {  207,   -1 }, /* (131) cmd ::= union */
  {  242,   -2 }, /* (132) select ::= SELECT selcollist */
  {  257,   -2 }, /* (133) sclp ::= selcollist COMMA */
  {  257,    0 }, /* (134) sclp ::= */
  {  245,   -3 }, /* (135) selcollist ::= sclp expr as */
  {  245,   -2 }, /* (136) selcollist ::= sclp STAR */
  {  259,   -2 }, /* (137) as ::= AS ids */
  {  259,   -1 }, /* (138) as ::= ids */
  {  259,    0 }, /* (139) as ::= */
  {  246,   -2 }, /* (140) from ::= FROM tablelist */
  {  260,   -2 }, /* (141) tablelist ::= ids cpxName */
  {  260,   -4 }, /* (142) tablelist ::= tablelist COMMA ids cpxName */
  {  261,   -1 }, /* (143) tmvar ::= VARIABLE */
  {  248,   -4 }, /* (144) interval_opt ::= INTERVAL LP tmvar RP */
  {  248,    0 }, /* (145) interval_opt ::= */
  {  249,    0 }, /* (146) fill_opt ::= */
  {  249,   -6 }, /* (147) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  249,   -4 }, /* (148) fill_opt ::= FILL LP ID RP */
  {  250,   -4 }, /* (149) sliding_opt ::= SLIDING LP tmvar RP */
  {  250,    0 }, /* (150) sliding_opt ::= */
  {  252,    0 }, /* (151) orderby_opt ::= */
  {  252,   -3 }, /* (152) orderby_opt ::= ORDER BY sortlist */
  {  262,   -4 }, /* (153) sortlist ::= sortlist COMMA item sortorder */
  {  262,   -2 }, /* (154) sortlist ::= item sortorder */
  {  264,   -2 }, /* (155) item ::= ids cpxName */
  {  265,   -1 }, /* (156) sortorder ::= ASC */
  {  265,   -1 }, /* (157) sortorder ::= DESC */
  {  265,    0 }, /* (158) sortorder ::= */
  {  251,    0 }, /* (159) groupby_opt ::= */
  {  251,   -3 }, /* (160) groupby_opt ::= GROUP BY grouplist */
  {  266,   -3 }, /* (161) grouplist ::= grouplist COMMA item */
  {  266,   -1 }, /* (162) grouplist ::= item */
  {  253,    0 }, /* (163) having_opt ::= */
  {  253,   -2 }, /* (164) having_opt ::= HAVING expr */
  {  255,    0 }, /* (165) limit_opt ::= */
  {  255,   -2 }, /* (166) limit_opt ::= LIMIT signed */
  {  255,   -4 }, /* (167) limit_opt ::= LIMIT signed OFFSET signed */
  {  255,   -4 }, /* (168) limit_opt ::= LIMIT signed COMMA signed */
  {  254,    0 }, /* (169) slimit_opt ::= */
  {  254,   -2 }, /* (170) slimit_opt ::= SLIMIT signed */
  {  254,   -4 }, /* (171) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  254,   -4 }, /* (172) slimit_opt ::= SLIMIT signed COMMA signed */
  {  247,    0 }, /* (173) where_opt ::= */
  {  247,   -2 }, /* (174) where_opt ::= WHERE expr */
  {  258,   -3 }, /* (175) expr ::= LP expr RP */
  {  258,   -1 }, /* (176) expr ::= ID */
  {  258,   -3 }, /* (177) expr ::= ID DOT ID */
  {  258,   -3 }, /* (178) expr ::= ID DOT STAR */
  {  258,   -1 }, /* (179) expr ::= INTEGER */
  {  258,   -2 }, /* (180) expr ::= MINUS INTEGER */
  {  258,   -2 }, /* (181) expr ::= PLUS INTEGER */
  {  258,   -1 }, /* (182) expr ::= FLOAT */
  {  258,   -2 }, /* (183) expr ::= MINUS FLOAT */
  {  258,   -2 }, /* (184) expr ::= PLUS FLOAT */
  {  258,   -1 }, /* (185) expr ::= STRING */
  {  258,   -1 }, /* (186) expr ::= NOW */
  {  258,   -1 }, /* (187) expr ::= VARIABLE */
  {  258,   -1 }, /* (188) expr ::= BOOL */
  {  258,   -4 }, /* (189) expr ::= ID LP exprlist RP */
  {  258,   -4 }, /* (190) expr ::= ID LP STAR RP */
  {  258,   -3 }, /* (191) expr ::= expr AND expr */
  {  258,   -3 }, /* (192) expr ::= expr OR expr */
  {  258,   -3 }, /* (193) expr ::= expr LT expr */
  {  258,   -3 }, /* (194) expr ::= expr GT expr */
  {  258,   -3 }, /* (195) expr ::= expr LE expr */
  {  258,   -3 }, /* (196) expr ::= expr GE expr */
  {  258,   -3 }, /* (197) expr ::= expr NE expr */
  {  258,   -3 }, /* (198) expr ::= expr EQ expr */
  {  258,   -3 }, /* (199) expr ::= expr PLUS expr */
  {  258,   -3 }, /* (200) expr ::= expr MINUS expr */
  {  258,   -3 }, /* (201) expr ::= expr STAR expr */
  {  258,   -3 }, /* (202) expr ::= expr SLASH expr */
  {  258,   -3 }, /* (203) expr ::= expr REM expr */
  {  258,   -3 }, /* (204) expr ::= expr LIKE expr */
  {  258,   -5 }, /* (205) expr ::= expr IN LP exprlist RP */
  {  267,   -3 }, /* (206) exprlist ::= exprlist COMMA expritem */
  {  267,   -1 }, /* (207) exprlist ::= expritem */
  {  268,   -1 }, /* (208) expritem ::= expr */
  {  268,    0 }, /* (209) expritem ::= */
  {  207,   -3 }, /* (210) cmd ::= RESET QUERY CACHE */
  {  207,   -7 }, /* (211) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  207,   -7 }, /* (212) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  207,   -7 }, /* (213) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  207,   -7 }, /* (214) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  207,   -8 }, /* (215) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  207,   -9 }, /* (216) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  207,   -3 }, /* (217) cmd ::= KILL CONNECTION INTEGER */
  {  207,   -5 }, /* (218) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  207,   -5 }, /* (219) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 10: /* cmd ::= SHOW CONFIGS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONFIGS, 0, 0);  }
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
      case 19: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 20: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-2].minor.yy0);    
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDBTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 26: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDBTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 27: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 28: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 29: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 30: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 31: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 32: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSQL(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 33: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSQL(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 34: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 35: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 36: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 37: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 38: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy374, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy219);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy219);}
        break;
      case 41: /* ids ::= ID */
      case 42: /* ids ::= STRING */ yytestcase(yyruleno==42);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 43: /* ifexists ::= IF EXISTS */
{yymsp[-1].minor.yy0.n = 1;}
        break;
      case 44: /* ifexists ::= */
      case 46: /* ifnotexists ::= */ yytestcase(yyruleno==46);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 45: /* ifnotexists ::= IF NOT EXISTS */
{yymsp[-2].minor.yy0.n = 1;}
        break;
      case 47: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 48: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy219);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy374, &yymsp[-2].minor.yy0);}
        break;
      case 50: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSQL(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 51: /* pps ::= */
      case 53: /* tseries ::= */ yytestcase(yyruleno==53);
      case 55: /* dbs ::= */ yytestcase(yyruleno==55);
      case 57: /* streams ::= */ yytestcase(yyruleno==57);
      case 59: /* storage ::= */ yytestcase(yyruleno==59);
      case 61: /* qtime ::= */ yytestcase(yyruleno==61);
      case 63: /* users ::= */ yytestcase(yyruleno==63);
      case 65: /* conns ::= */ yytestcase(yyruleno==65);
      case 67: /* state ::= */ yytestcase(yyruleno==67);
{yymsp[1].minor.yy0.n = 0;   }
        break;
      case 52: /* pps ::= PPS INTEGER */
      case 54: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==54);
      case 56: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==56);
      case 58: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==58);
      case 60: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==60);
      case 62: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==62);
      case 64: /* users ::= USERS INTEGER */ yytestcase(yyruleno==64);
      case 66: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* state ::= STATE ids */ yytestcase(yyruleno==68);
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 69: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy219.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy219.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy219.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy219.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy219.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy219.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy219.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy219.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy219.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy219 = yylhsminor.yy219;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy322 = yymsp[0].minor.yy322; }
        break;
      case 71: /* tables ::= MAXTABLES INTEGER */
      case 72: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==72);
      case 73: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==73);
      case 74: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==74);
      case 75: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==75);
      case 76: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==78);
      case 79: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==79);
      case 80: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==80);
      case 81: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==81);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 82: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy374);}
        break;
      case 83: /* db_optr ::= db_optr tables */
      case 97: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==97);
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.maxTablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 84: /* db_optr ::= db_optr cache */
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 85: /* db_optr ::= db_optr replica */
      case 96: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==96);
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 86: /* db_optr ::= db_optr days */
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 87: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 88: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 89: /* db_optr ::= db_optr blocks */
      case 99: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==99);
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 90: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 91: /* db_optr ::= db_optr wal */
      case 101: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==101);
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 92: /* db_optr ::= db_optr comp */
      case 100: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==100);
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 93: /* db_optr ::= db_optr prec */
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 94: /* db_optr ::= db_optr keep */
      case 98: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==98);
{ yylhsminor.yy374 = yymsp[-1].minor.yy374; yylhsminor.yy374.keep = yymsp[0].minor.yy322; }
  yymsp[-1].minor.yy374 = yylhsminor.yy374;
        break;
      case 95: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy374);}
        break;
      case 102: /* typename ::= ids */
{ tSQLSetColumnType (&yylhsminor.yy325, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 103: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy279;          // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy325, &yymsp[-3].minor.yy0);
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 104: /* signed ::= INTEGER */
{ yylhsminor.yy279 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy279 = yylhsminor.yy279;
        break;
      case 105: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy279 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy279 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 107: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 108: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy408 = tSetCreateSQLElems(yymsp[-1].minor.yy369, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy408, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 109: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy408 = tSetCreateSQLElems(yymsp[-5].minor.yy369, yymsp[-1].minor.yy369, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy408, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 110: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy408 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy322, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy408, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 111: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy408 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy190, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy408, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 112: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy369 = tFieldListAppend(yymsp[-2].minor.yy369, &yymsp[0].minor.yy325);   }
  yymsp[-2].minor.yy369 = yylhsminor.yy369;
        break;
      case 113: /* columnlist ::= column */
{yylhsminor.yy369 = tFieldListAppend(NULL, &yymsp[0].minor.yy325);}
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 114: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy325, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy325);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 115: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy322 = tVariantListAppend(yymsp[-2].minor.yy322, &yymsp[0].minor.yy518, -1);    }
  yymsp[-2].minor.yy322 = yylhsminor.yy322;
        break;
      case 116: /* tagitemlist ::= tagitem */
{ yylhsminor.yy322 = tVariantListAppend(NULL, &yymsp[0].minor.yy518, -1); }
  yymsp[0].minor.yy322 = yylhsminor.yy322;
        break;
      case 117: /* tagitem ::= INTEGER */
      case 118: /* tagitem ::= FLOAT */ yytestcase(yyruleno==118);
      case 119: /* tagitem ::= STRING */ yytestcase(yyruleno==119);
      case 120: /* tagitem ::= BOOL */ yytestcase(yyruleno==120);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy518, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy518 = yylhsminor.yy518;
        break;
      case 121: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy518, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy518 = yylhsminor.yy518;
        break;
      case 122: /* tagitem ::= MINUS INTEGER */
      case 123: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==123);
      case 124: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==124);
      case 125: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==125);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy518, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy518 = yylhsminor.yy518;
        break;
      case 126: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy190 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy260, yymsp[-9].minor.yy322, yymsp[-8].minor.yy500, yymsp[-4].minor.yy322, yymsp[-3].minor.yy322, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy322, &yymsp[0].minor.yy284, &yymsp[-1].minor.yy284);
}
  yymsp[-11].minor.yy190 = yylhsminor.yy190;
        break;
      case 127: /* union ::= select */
{ yylhsminor.yy263 = setSubclause(NULL, yymsp[0].minor.yy190); }
  yymsp[0].minor.yy263 = yylhsminor.yy263;
        break;
      case 128: /* union ::= LP union RP */
{ yymsp[-2].minor.yy263 = yymsp[-1].minor.yy263; }
        break;
      case 129: /* union ::= union UNION ALL select */
{ yylhsminor.yy263 = appendSelectClause(yymsp[-3].minor.yy263, yymsp[0].minor.yy190); }
  yymsp[-3].minor.yy263 = yylhsminor.yy263;
        break;
      case 130: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy263 = appendSelectClause(yymsp[-5].minor.yy263, yymsp[-1].minor.yy190); }
  yymsp[-5].minor.yy263 = yylhsminor.yy263;
        break;
      case 131: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy263, NULL, TSDB_SQL_SELECT); }
        break;
      case 132: /* select ::= SELECT selcollist */
{
  yylhsminor.yy190 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy260, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy190 = yylhsminor.yy190;
        break;
      case 133: /* sclp ::= selcollist COMMA */
{yylhsminor.yy260 = yymsp[-1].minor.yy260;}
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 134: /* sclp ::= */
{yymsp[1].minor.yy260 = 0;}
        break;
      case 135: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy260 = tSQLExprListAppend(yymsp[-2].minor.yy260, yymsp[-1].minor.yy500, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 136: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy260 = tSQLExprListAppend(yymsp[-1].minor.yy260, pNode, 0);
}
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 137: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 138: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 139: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 140: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy322 = yymsp[0].minor.yy322;}
        break;
      case 141: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy322 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 142: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy322 = tVariantListAppendToken(yymsp[-3].minor.yy322, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy322 = yylhsminor.yy322;
        break;
      case 143: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 144: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 149: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==149);
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 145: /* interval_opt ::= */
      case 150: /* sliding_opt ::= */ yytestcase(yyruleno==150);
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 146: /* fill_opt ::= */
{yymsp[1].minor.yy322 = 0;     }
        break;
      case 147: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy322, &A, -1, 0);
    yymsp[-5].minor.yy322 = yymsp[-1].minor.yy322;
}
        break;
      case 148: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy322 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 151: /* orderby_opt ::= */
      case 159: /* groupby_opt ::= */ yytestcase(yyruleno==159);
{yymsp[1].minor.yy322 = 0;}
        break;
      case 152: /* orderby_opt ::= ORDER BY sortlist */
      case 160: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==160);
{yymsp[-2].minor.yy322 = yymsp[0].minor.yy322;}
        break;
      case 153: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy322 = tVariantListAppend(yymsp[-3].minor.yy322, &yymsp[-1].minor.yy518, yymsp[0].minor.yy150);
}
  yymsp[-3].minor.yy322 = yylhsminor.yy322;
        break;
      case 154: /* sortlist ::= item sortorder */
{
  yylhsminor.yy322 = tVariantListAppend(NULL, &yymsp[-1].minor.yy518, yymsp[0].minor.yy150);
}
  yymsp[-1].minor.yy322 = yylhsminor.yy322;
        break;
      case 155: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy518, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy518 = yylhsminor.yy518;
        break;
      case 156: /* sortorder ::= ASC */
{yymsp[0].minor.yy150 = TSDB_ORDER_ASC; }
        break;
      case 157: /* sortorder ::= DESC */
{yymsp[0].minor.yy150 = TSDB_ORDER_DESC;}
        break;
      case 158: /* sortorder ::= */
{yymsp[1].minor.yy150 = TSDB_ORDER_ASC;}
        break;
      case 161: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy322 = tVariantListAppend(yymsp[-2].minor.yy322, &yymsp[0].minor.yy518, -1);
}
  yymsp[-2].minor.yy322 = yylhsminor.yy322;
        break;
      case 162: /* grouplist ::= item */
{
  yylhsminor.yy322 = tVariantListAppend(NULL, &yymsp[0].minor.yy518, -1);
}
  yymsp[0].minor.yy322 = yylhsminor.yy322;
        break;
      case 163: /* having_opt ::= */
      case 173: /* where_opt ::= */ yytestcase(yyruleno==173);
      case 209: /* expritem ::= */ yytestcase(yyruleno==209);
{yymsp[1].minor.yy500 = 0;}
        break;
      case 164: /* having_opt ::= HAVING expr */
      case 174: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==174);
{yymsp[-1].minor.yy500 = yymsp[0].minor.yy500;}
        break;
      case 165: /* limit_opt ::= */
      case 169: /* slimit_opt ::= */ yytestcase(yyruleno==169);
{yymsp[1].minor.yy284.limit = -1; yymsp[1].minor.yy284.offset = 0;}
        break;
      case 166: /* limit_opt ::= LIMIT signed */
      case 170: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==170);
{yymsp[-1].minor.yy284.limit = yymsp[0].minor.yy279;  yymsp[-1].minor.yy284.offset = 0;}
        break;
      case 167: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 171: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==171);
{yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy279;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy279;}
        break;
      case 168: /* limit_opt ::= LIMIT signed COMMA signed */
      case 172: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==172);
{yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy279;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy279;}
        break;
      case 175: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy500 = yymsp[-1].minor.yy500; }
        break;
      case 176: /* expr ::= ID */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 177: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 178: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 179: /* expr ::= INTEGER */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 180: /* expr ::= MINUS INTEGER */
      case 181: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==181);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy500 = yylhsminor.yy500;
        break;
      case 182: /* expr ::= FLOAT */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 183: /* expr ::= MINUS FLOAT */
      case 184: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==184);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy500 = yylhsminor.yy500;
        break;
      case 185: /* expr ::= STRING */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 186: /* expr ::= NOW */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 187: /* expr ::= VARIABLE */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 188: /* expr ::= BOOL */
{yylhsminor.yy500 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 189: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy500 = tSQLExprCreateFunction(yymsp[-1].minor.yy260, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy500 = yylhsminor.yy500;
        break;
      case 190: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy500 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy500 = yylhsminor.yy500;
        break;
      case 191: /* expr ::= expr AND expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_AND);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 192: /* expr ::= expr OR expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_OR); }
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 193: /* expr ::= expr LT expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_LT);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 194: /* expr ::= expr GT expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_GT);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 195: /* expr ::= expr LE expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_LE);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 196: /* expr ::= expr GE expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_GE);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 197: /* expr ::= expr NE expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_NE);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 198: /* expr ::= expr EQ expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_EQ);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 199: /* expr ::= expr PLUS expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_PLUS);  }
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 200: /* expr ::= expr MINUS expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_MINUS); }
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 201: /* expr ::= expr STAR expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_STAR);  }
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 202: /* expr ::= expr SLASH expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_DIVIDE);}
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 203: /* expr ::= expr REM expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_REM);   }
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 204: /* expr ::= expr LIKE expr */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-2].minor.yy500, yymsp[0].minor.yy500, TK_LIKE);  }
  yymsp[-2].minor.yy500 = yylhsminor.yy500;
        break;
      case 205: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy500 = tSQLExprCreate(yymsp[-4].minor.yy500, (tSQLExpr*)yymsp[-1].minor.yy260, TK_IN); }
  yymsp[-4].minor.yy500 = yylhsminor.yy500;
        break;
      case 206: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy260 = tSQLExprListAppend(yymsp[-2].minor.yy260,yymsp[0].minor.yy500,0);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 207: /* exprlist ::= expritem */
{yylhsminor.yy260 = tSQLExprListAppend(0,yymsp[0].minor.yy500,0);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 208: /* expritem ::= expr */
{yylhsminor.yy500 = yymsp[0].minor.yy500;}
  yymsp[0].minor.yy500 = yylhsminor.yy500;
        break;
      case 210: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 211: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy369, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 212: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 213: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy369, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 214: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 215: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 216: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy518, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 217: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 218: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 219: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
