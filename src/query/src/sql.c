/* Driver template for the LEMON parser generator.
** The author disclaims copyright to this source code.
*/
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */
#include <stdio.h>

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
/* Next is all token values, in a form suitable for use by makeheaders.
** This section will be null unless lemon is run with the -m switch.
*/
/* 
** These constants (all generated automatically by the parser generator)
** specify the various kinds of tokens (terminals) that the parser
** understands. 
**
** Each symbol here is a terminal symbol in the grammar.
*/
/* Make sure the INTERFACE macro is defined.
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/* The next thing included is series of defines which control
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 terminals
**                       and nonterminals.  "int" is used otherwise.
**    YYNOCODE           is a number of type YYCODETYPE which corresponds
**                       to no legal terminal or nonterminal number.  This
**                       number is used to fill in empty slots of the hash 
**                       table.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       have fall-back values which should be used if the
**                       original value of the token will not parse.
**    YYACTIONTYPE       is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 rules and
**                       states combined.  "int" is used otherwise.
**    ParseTOKENTYPE     is the data type used for minor tokens given 
**                       directly to the parser from the tokenizer.
**    YYMINORTYPE        is the data type used for all minor tokens.
**                       This is typically a union of many types, one of
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for base tokens is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    ParseARG_SDECL     A static variable declaration for the %extra_argument
**    ParseARG_PDECL     A parameter declaration for the %extra_argument
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
*/
#define YYCODETYPE unsigned short int
#define YYNOCODE 265
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy96;
  SRelationInfo* yy148;
  tSqlExpr* yy178;
  SCreateAcctInfo yy187;
  SArray* yy285;
  TAOS_FIELD yy295;
  SSqlNode* yy344;
  tVariant yy362;
  SIntervalVal yy376;
  SLimitVal yy438;
  int yy460;
  SCreateTableSql* yy470;
  SSessionWindowVal yy523;
  int64_t yy525;
  SCreateDbInfo yy526;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 517
#define YYNRULE 271
#define YYFALLBACK 1
#define YY_NO_ACTION      (YYNSTATE+YYNRULE+2)
#define YY_ACCEPT_ACTION  (YYNSTATE+YYNRULE+1)
#define YY_ERROR_ACTION   (YYNSTATE+YYNRULE)

/* The yyzerominor constant is used to initialize instances of
** YYMINORTYPE objects to zero. */
static const YYMINORTYPE yyzerominor = { 0 };

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
**   0 <= N < YYNSTATE                  Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   YYNSTATE <= N < YYNSTATE+YYNRULE   Reduce by rule N-YYNSTATE.
**
**   N == YYNSTATE+YYNRULE              A syntax error has occurred.
**
**   N == YYNSTATE+YYNRULE+1            The parser accepts its input.
**
**   N == YYNSTATE+YYNRULE+2            No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as
**
**      yy_action[ yy_shift_ofst[S] + X ]
**
** If the index value yy_shift_ofst[S]+X is out of range or if the value
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
** is equal to YY_SHIFT_USE_DFLT, it means that the action is not in the table
** and that yy_default[S] should be used instead.  
**
** The formula above is for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array and YY_REDUCE_USE_DFLT is used in place of
** YY_SHIFT_USE_DFLT.
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
*/
#define YY_ACTTAB_COUNT (804)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   500,   48,   47,   31,   68,   46,   45,   44,  499,  789,
 /*    10 */   321,  517,   49,   50,  503,   53,   54,  116,  115,  226,
 /*    20 */    43,  224,   52,  281,   57,   55,   58,   56,  133,  131,
 /*    30 */   130,  431,   48,   47,   17,   16,   46,   45,   44,   49,
 /*    40 */    50,    6,   53,   54,   29,  297,  226,   43,  429,   52,
 /*    50 */   281,   57,   55,   58,   56,  472,  336,  471,  283,   48,
 /*    60 */    47,  103,  102,   46,   45,   44,   49,   50,  339,   53,
 /*    70 */    54,  246,  269,  226,   43,   25,   52,  281,   57,   55,
 /*    80 */    58,   56,  318,  317,  127,  238,   48,   47,  500,  502,
 /*    90 */    46,   45,   44,  242,  241,  230,  499,   83,   46,   45,
 /*   100 */    44,   49,   51,  129,   53,   54,  228,  420,  226,   43,
 /*   110 */    71,   52,  281,   57,   55,   58,   56,  414,  411,  413,
 /*   120 */   410,   48,   47,  434,   62,   46,   45,   44,  368,  367,
 /*   130 */    30,  361,  516,  515,  514,  513,  512,  511,  510,  509,
 /*   140 */   508,  507,  506,  505,  504,  320,   63,   50,  208,   53,
 /*   150 */    54,  295,  294,  226,   43,  419,   52,  281,   57,   55,
 /*   160 */    58,   56,   15,   14,  421,  232,   48,   47,  292,  291,
 /*   170 */    46,   45,   44,   53,   54,  128,  229,  226,   43,  287,
 /*   180 */    52,  281,   57,   55,   58,   56,  470,  447,  469,  651,
 /*   190 */    48,   47,  168,   20,   46,   45,   44,  373,  491,  385,
 /*   200 */   384,  383,  382,  381,  380,  379,  378,  377,  376,  375,
 /*   210 */   374,  372,  371,   24,  277,  314,  313,  276,  275,  274,
 /*   220 */   312,  273,  311,  310,  309,  272,  308,  307,  225,  404,
 /*   230 */   412,  409,  415,  467,  408,  185,  407,  402,  500,   19,
 /*   240 */    57,   55,   58,   56,  395,  227,  499,  142,   48,   47,
 /*   250 */    97,   96,   46,   45,   44,   13,  193,   31,   31,  397,
 /*   260 */   204,  205,  417,  194,  282,   65,   20,  365,  119,  118,
 /*   270 */   192,  279,  225,  404,  286,   75,  415,  268,  408,  457,
 /*   280 */   407,  466,  459,  458,   85,   66,   82,  456,   20,  454,
 /*   290 */   453,  455,  142,  452,  451,   31,   61,  329,  416,  293,
 /*   300 */   289,  185,  429,  429,  204,  205,   84,   59,   37,   24,
 /*   310 */   396,  314,  313,  263,  222,   79,  312,   26,  311,  310,
 /*   320 */   309,  223,  308,  307,    3,  169,  104,   98,  109,  435,
 /*   330 */   245,   81,   69,  108,  114,  117,  107,  288,  200,  403,
 /*   340 */   429,  465,  111,   31,   72,  405,  176,  174,  172,  364,
 /*   350 */    31,   59,   31,  171,  122,  121,  120,    5,   34,  158,
 /*   360 */   347,  406,  261,   31,  157,   93,   88,   92,  474,  266,
 /*   370 */   185,  477,  360,  476,  343,  475,  328,  185,   61,  396,
 /*   380 */   344,  341,  334,  403,  247,  217,  396,   75,  429,  405,
 /*   390 */   352,  351,  216,   70,  209,  429,  500,  429,   32,  233,
 /*   400 */   234,   32,  231,   21,  499,  406,    1,  156,  436,   61,
 /*   410 */   137,  393,   86,   32,  448,  249,  168,  362,  142,  168,
 /*   420 */    37,  106,  142,  135,  336,  327,  207,  319,  315,  305,
 /*   430 */   330,  220,  218,  212,   61,  464,  463,  462,  430,  210,
 /*   440 */   461,  460,  450,  446,  445,  444,  366,  443,  442,  441,
 /*   450 */   440,  439,   32,  433,  468,  432,  101,  468,  468,  468,
 /*   460 */   219,   99,   95,   60,  285,  284,    8,  418,  401,    7,
 /*   470 */    91,  278,  391,  392,  390,  389,  388,  387,  473,   87,
 /*   480 */    85,   23,  386,  267,   22,  265,   80,  251,   12,   11,
 /*   490 */   348,   28,   10,  332,   27,  141,  213,  345,  257,   78,
 /*   500 */   138,  139,  342,  340,  337,    9,   77,   74,  244,  326,
 /*   510 */   324,  249,  240,  239,  323,  237,  236,  325,  322,    2,
 /*   520 */     4,  498,  132,  497,  449,  126,  492,  164,  125,  124,
 /*   530 */   490,  316,  123,  483,  306,  167,  304,  302,  299,  303,
 /*   540 */   300,  301,  199,  206,  298,  166,  165,  163,  105,  270,
 /*   550 */    90,   89,  162,  370,  221,  161,  279,  437,  151,   41,
 /*   560 */   150,  146,  201,  143,   64,  259,  149,  350,  254,  255,
 /*   570 */   211,  148,   76,   73,  147,  248,  253,  178,  790,  256,
 /*   580 */   501,  258,  790,  260,  346,  177,  790,  264,  790,  790,
 /*   590 */   262,  496,  495,   42,  494,  145,  359,   67,  152,  493,
 /*   600 */   250,  144,  175,  173,  358,  489,  488,  296,  790,  335,
 /*   610 */   790,  790,  215,  790,  252,  790,   40,  790,  790,  487,
 /*   620 */   353,  486,  790,  349,  790,  790,  485,  357,  790,  790,
 /*   630 */   790,  790,  790,  790,  356,  305,  790,  790,  214,  790,
 /*   640 */   790,  355,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   650 */   790,  790,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   660 */   790,  790,  790,  790,  790,  790,  790,  484,  170,  235,
 /*   670 */   482,  481,  113,  112,  480,  790,  110,  479,  180,   39,
 /*   680 */   790,   33,   36,  438,  160,  428,  427,  790,  100,  426,
 /*   690 */   290,  159,  424,  423,   94,  422,  394,  790,  280,  790,
 /*   700 */    35,  790,  790,  179,   38,  790,  271,  369,  155,  154,
 /*   710 */   363,  153,  140,  136,  338,  333,  331,  134,  243,  790,
 /*   720 */   790,  790,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   730 */   790,  790,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   740 */   790,  790,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   750 */   790,  790,  790,  790,  790,  790,  354,  790,  790,  790,
 /*   760 */   790,  790,  790,  790,  790,  790,  478,  425,  790,  790,
 /*   770 */   790,  790,  790,  790,  790,  790,  790,  790,  790,  790,
 /*   780 */   790,  790,  790,  790,  181,  195,  198,  197,  196,  191,
 /*   790 */   190,  184,  189,  187,  186,  203,  202,  400,  399,  398,
 /*   800 */   188,  183,  182,   18,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,  192,   80,   37,   38,   39,    9,  189,
 /*    10 */   190,    0,   13,   14,   59,   16,   17,   76,   77,   20,
 /*    20 */    21,   60,   23,   24,   25,   26,   27,   28,   62,   63,
 /*    30 */    64,  107,   33,   34,  139,  140,   37,   38,   39,   13,
 /*    40 */    14,   80,   16,   17,   80,  234,   20,   21,  237,   23,
 /*    50 */    24,   25,   26,   27,   28,    5,  236,    7,   15,   33,
 /*    60 */    34,  139,  140,   37,   38,   39,   13,   14,  110,   16,
 /*    70 */    17,  251,  108,   20,   21,  117,   23,   24,   25,   26,
 /*    80 */    27,   28,   65,   66,   67,  136,   33,   34,    1,   60,
 /*    90 */    37,   38,   39,  144,  145,   68,    9,  199,   37,   38,
 /*   100 */    39,   13,   14,   21,   16,   17,   68,   81,   20,   21,
 /*   110 */   111,   23,   24,   25,   26,   27,   28,    5,    5,    7,
 /*   120 */     7,   33,   34,    5,  110,   37,   38,   39,  230,  231,
 /*   130 */   232,  233,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,   56,   57,   58,  132,   14,   61,   16,
 /*   150 */    17,   33,   34,   20,   21,  112,   23,   24,   25,   26,
 /*   160 */    27,   28,  139,  140,   81,  138,   33,   34,  141,  142,
 /*   170 */    37,   38,   39,   16,   17,   21,  138,   20,   21,  141,
 /*   180 */    23,   24,   25,   26,   27,   28,    5,  197,    7,    0,
 /*   190 */    33,   34,  202,  110,   37,   38,   39,  211,   83,  213,
 /*   200 */   214,  215,  216,  217,  218,  219,  220,  221,  222,  223,
 /*   210 */   224,  225,  226,   91,   92,   93,   94,   95,   96,   97,
 /*   220 */    98,   99,  100,  101,  102,  103,  104,  105,    1,    2,
 /*   230 */   118,  118,    5,    5,    7,  254,    9,   81,    1,   44,
 /*   240 */    25,   26,   27,   28,  263,  198,    9,  192,   33,   34,
 /*   250 */   139,  140,   37,   38,   39,   80,   61,  192,  192,   81,
 /*   260 */    33,   34,    1,   68,   37,  110,  110,   81,   73,   74,
 /*   270 */    75,   82,    1,    2,   79,   80,    5,   81,    7,  211,
 /*   280 */     9,    5,  214,  215,  109,  130,  111,  219,  110,  221,
 /*   290 */   222,  223,  192,  225,  226,  192,  110,   37,   37,  234,
 /*   300 */   234,  254,  237,  237,   33,   34,  110,   80,  113,   91,
 /*   310 */   263,   93,   94,  258,  198,  260,   98,   80,  100,  101,
 /*   320 */   102,  198,  104,  105,  195,  196,   62,   63,   64,   81,
 /*   330 */   135,  239,  137,   69,   70,   71,   72,  234,  143,  112,
 /*   340 */   237,    5,   78,  192,  252,  118,   62,   63,   64,   81,
 /*   350 */   192,   80,  192,   69,   70,   71,   72,   62,   63,   64,
 /*   360 */   260,  134,  262,  192,   69,   70,   71,   72,    2,   81,
 /*   370 */   254,    5,   81,    7,   81,    9,  116,  254,  110,  263,
 /*   380 */    81,   81,   81,  112,   81,  234,  263,   80,  237,  118,
 /*   390 */   125,  126,  234,  199,  234,  237,    1,  237,  110,   33,
 /*   400 */    34,  110,  192,  110,    9,  134,  200,  201,  237,  110,
 /*   410 */   110,  197,  199,  110,  197,  114,  202,  192,  192,  202,
 /*   420 */   113,   76,  192,  192,  236,  231,  191,  192,  212,   84,
 /*   430 */   192,  212,  212,  212,  110,    5,    5,    5,  228,  251,
 /*   440 */     5,    5,    5,    5,    5,    5,  233,    5,    5,    5,
 /*   450 */     5,    5,  110,    5,  238,    5,  140,  238,  238,  238,
 /*   460 */   235,  140,  140,   16,   58,   24,   80,  112,   81,   80,
 /*   470 */    76,   15,    5,   83,    5,    5,    5,    5,  112,   76,
 /*   480 */   109,   80,    9,  108,   80,  108,  260,  256,   80,  124,
 /*   490 */   260,  110,  124,  255,  110,   80,    1,   81,   80,   80,
 /*   500 */    80,  110,   81,   81,   81,   80,  110,  115,  136,   92,
 /*   510 */     5,  114,    5,  146,    5,    5,  146,   93,    5,  200,
 /*   520 */   195,  193,   60,  193,  227,  194,  193,  207,  194,  194,
 /*   530 */   193,   82,  194,  193,  106,  203,   85,   54,   50,   87,
 /*   540 */    86,   88,  193,  193,   89,  206,  205,  204,   90,  193,
 /*   550 */   199,  199,  208,  227,  193,  209,   82,  210,  241,  133,
 /*   560 */   242,  246,  193,  249,  131,  257,  243,  193,  257,  193,
 /*   570 */   257,  244,  193,  193,  245,  193,  119,  192,  264,  120,
 /*   580 */   192,  121,  264,  122,  118,  192,  264,  123,  264,  264,
 /*   590 */   127,  192,  192,  128,  192,  247,  236,  129,  240,  192,
 /*   600 */   236,  248,  192,  192,  250,  192,  192,  229,  264,  236,
 /*   610 */   264,  264,  229,  264,  257,  264,  253,  264,  264,  192,
 /*   620 */   261,  192,  264,  261,  264,  264,  192,  229,  264,  264,
 /*   630 */   264,  264,  264,  264,  229,   84,  264,  264,  229,  264,
 /*   640 */   264,  229,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   650 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   660 */   264,  264,  264,  264,  264,  264,  264,  192,  192,  192,
 /*   670 */   192,  192,  192,  192,  192,  264,  192,  192,  192,  192,
 /*   680 */   264,  192,  192,  192,  192,  192,  192,  264,  192,  192,
 /*   690 */   192,  192,  192,  192,  192,  192,  192,  264,  192,  264,
 /*   700 */   192,  264,  264,  192,  192,  264,  192,  192,  192,  192,
 /*   710 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  264,
 /*   720 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   730 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   740 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   750 */   264,  264,  264,  264,  264,  264,  229,  264,  264,  264,
 /*   760 */   264,  264,  264,  264,  264,  264,  238,  238,  264,  264,
 /*   770 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   780 */   264,  264,  264,  264,  254,  254,  254,  254,  254,  254,
 /*   790 */   254,  254,  254,  254,  254,  254,  254,  254,  254,  254,
 /*   800 */   254,  254,  254,  254,
};
#define YY_SHIFT_USE_DFLT (-106)
#define YY_SHIFT_COUNT (321)
#define YY_SHIFT_MIN   (-105)
#define YY_SHIFT_MAX   (551)
static const short yy_shift_ofst[] = {
 /*     0 */   195,  122,  122,  218,  218,  474,  227,  271,  271,  395,
 /*    10 */   395,  395,  395,  395,  395,  395,  395,  395,   -1,   87,
 /*    20 */   271,  366,  366,  366,  366,  237,  307,  395,  395,  395,
 /*    30 */   189,  395,  395,  345,  474,  551,  551, -106, -106, -106,
 /*    40 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*    50 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*    60 */   366,  366,  118,  118,  118,  118,  118,  118,  118,  395,
 /*    70 */   395,  395,  260,  395,  307,  307,  395,  395,  395,  265,
 /*    80 */   265,  -42,  307,  395,  395,  395,  395,  395,  395,  395,
 /*    90 */   395,  395,  395,  395,  395,  395,  395,  395,  395,  395,
 /*   100 */   395,  395,  395,  395,  395,  395,  395,  395,  395,  395,
 /*   110 */   395,  395,  395,  395,  395,  395,  395,  395,  395,  395,
 /*   120 */   395,  395,  395,  395,  395,  395,  395,  395,  395,  395,
 /*   130 */   395,  395,  395,  395,  462,  462,  462,  466,  466,  466,
 /*   140 */   462,  466,  462,  468,  433,  465,  464,  463,  461,  460,
 /*   150 */   459,  457,  426,  462,  462,  462,  428,  474,  474,  462,
 /*   160 */   462,  458,  455,  488,  454,  453,  483,  452,  451,  428,
 /*   170 */   462,  449,  449,  462,  449,  462,  449,  462,  462, -106,
 /*   180 */  -106,   26,   53,   53,   88,   53,  133,  157,  215,  215,
 /*   190 */   215,  215,  264,  295,  284,  -32,  -32,  -32,  -32,   27,
 /*   200 */   -51,  175,   61,   61,  113,  112,   38,   17,  -34,  303,
 /*   210 */   301,  300,  299,  293,   14,  155,  291,  288,  268,  196,
 /*   220 */   186,  -36,  178,  156,  261,  -39,   43,   83,  111,   23,
 /*   230 */   -78,  -76, -105,  181,   50,  -59,  513,  370,  510,  509,
 /*   240 */   367,  507,  505,  424,  417,  372,  397,  377,  425,  392,
 /*   250 */   423,  396,  422,  420,  421,  391,  419,  495,  418,  416,
 /*   260 */   415,  384,  368,  381,  365,  408,  377,  404,  375,  401,
 /*   270 */   371,  403,  473,  472,  471,  470,  469,  467,  390,  456,
 /*   280 */   394,  389,  387,  355,  386,  441,  406,  322,  342,  342,
 /*   290 */   447,  321,  316,  342,  450,  448,  248,  342,  446,  445,
 /*   300 */   444,  443,  442,  440,  439,  438,  437,  436,  435,  432,
 /*   310 */   431,  430,  336,  276,  228,  324,  115,  154,   82,   29,
 /*   320 */   -45,   11,
};
#define YY_REDUCE_USE_DFLT (-190)
#define YY_REDUCE_COUNT (180)
#define YY_REDUCE_MIN   (-189)
#define YY_REDUCE_MAX   (549)
static const short yy_reduce_ofst[] = {
 /*     0 */  -180,  -14,  -14,   68,   68, -102,  123,  116,   47,  160,
 /*    10 */   100,   55,  158,  151,  103,   66,   65, -189,  238,  235,
 /*    20 */   -19,  221,  220,  219,  216,  231,  188,  230,  226,  225,
 /*    30 */   213,  210,  171,  217,  194,  214,  -10,   92,  206,  129,
 /*    40 */   549,  548,  547,  546,  545,  544,  543,  542,  541,  540,
 /*    50 */   539,  538,  537,  536,  535,  534,  533,  532,  531,  530,
 /*    60 */   529,  528,  527,  412,  409,  405,  398,  383,  378,  526,
 /*    70 */   525,  524,  363,  523,  373,  364,  522,  521,  520,  362,
 /*    80 */   359,  358,  360,  519,  518,  517,  516,  515,  514,  512,
 /*    90 */   511,  508,  506,  504,  503,  502,  501,  500,  499,  498,
 /*   100 */   497,  496,  494,  493,  492,  491,  490,  489,  487,  486,
 /*   110 */   485,  484,  482,  481,  480,  479,  478,  477,  476,  475,
 /*   120 */   434,  429,  427,  414,  413,  411,  410,  407,  402,  400,
 /*   130 */   399,  393,  388,  385,  382,  380,  379,  357,  313,  311,
 /*   140 */   376,  308,  374,  354,  314,  353,  348,  315,  329,  327,
 /*   150 */   323,  318,  317,  369,  361,  356,  326,  352,  351,  350,
 /*   160 */   349,  347,  346,  344,  343,  320,  341,  339,  332,  297,
 /*   170 */   340,  338,  335,  337,  334,  333,  331,  330,  328,  319,
 /*   180 */   325,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   788,  628,  574,  640,  561,  571,  771,  771,  771,  788,
 /*    10 */   788,  788,  788,  788,  788,  788,  788,  788,  687,  533,
 /*    20 */   771,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    30 */   571,  788,  788,  577,  571,  577,  577,  682,  612,  630,
 /*    40 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    50 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    60 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    70 */   788,  788,  689,  692,  788,  788,  694,  788,  788,  714,
 /*    80 */   714,  680,  788,  788,  788,  788,  788,  788,  788,  788,
 /*    90 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   100 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   110 */   559,  788,  557,  788,  788,  788,  788,  788,  788,  788,
 /*   120 */   788,  788,  788,  788,  788,  788,  788,  544,  788,  788,
 /*   130 */   788,  788,  788,  788,  535,  535,  535,  788,  788,  788,
 /*   140 */   535,  788,  535,  721,  725,  719,  707,  715,  706,  702,
 /*   150 */   700,  699,  729,  535,  535,  535,  575,  571,  571,  535,
 /*   160 */   535,  593,  591,  589,  581,  587,  583,  585,  579,  562,
 /*   170 */   535,  569,  569,  535,  569,  535,  569,  535,  535,  612,
 /*   180 */   630,  788,  730,  720,  788,  770,  760,  759,  766,  758,
 /*   190 */   757,  756,  788,  788,  788,  752,  755,  754,  753,  788,
 /*   200 */   788,  788,  762,  761,  788,  788,  788,  788,  788,  788,
 /*   210 */   788,  788,  788,  788,  726,  722,  788,  788,  788,  788,
 /*   220 */   788,  788,  788,  788,  788,  732,  788,  788,  788,  788,
 /*   230 */   788,  642,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   240 */   788,  788,  788,  788,  788,  788,  679,  788,  788,  788,
 /*   250 */   788,  690,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   260 */   788,  716,  788,  708,  788,  788,  654,  788,  788,  788,
 /*   270 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   280 */   788,  788,  788,  788,  788,  788,  788,  788,  782,  780,
 /*   290 */   788,  788,  788,  776,  788,  788,  788,  774,  788,  788,
 /*   300 */   788,  788,  788,  788,  788,  788,  788,  788,  788,  788,
 /*   310 */   788,  788,  788,  788,  788,  596,  788,  542,  540,  788,
 /*   320 */   531,  788,  787,  786,  785,  773,  772,  650,  688,  684,
 /*   330 */   686,  685,  683,  693,  691,  678,  677,  676,  695,  681,
 /*   340 */   698,  697,  701,  704,  703,  705,  696,  718,  717,  710,
 /*   350 */   711,  713,  712,  709,  728,  727,  724,  723,  675,  660,
 /*   360 */   655,  652,  659,  658,  657,  656,  653,  649,  648,  576,
 /*   370 */   629,  627,  626,  625,  624,  623,  622,  621,  620,  619,
 /*   380 */   618,  617,  616,  615,  614,  613,  608,  604,  602,  601,
 /*   390 */   600,  597,  570,  573,  572,  768,  769,  767,  765,  764,
 /*   400 */   763,  749,  748,  747,  746,  743,  742,  741,  738,  744,
 /*   410 */   740,  737,  745,  739,  736,  735,  734,  733,  751,  750,
 /*   420 */   731,  565,  784,  783,  781,  779,  778,  777,  775,  662,
 /*   430 */   663,  644,  647,  646,  645,  643,  661,  595,  594,  592,
 /*   440 */   590,  582,  588,  584,  586,  580,  578,  564,  563,  641,
 /*   450 */   611,  639,  638,  637,  636,  635,  634,  633,  632,  631,
 /*   460 */   610,  609,  607,  606,  605,  603,  599,  598,  665,  674,
 /*   470 */   673,  672,  671,  670,  669,  668,  667,  666,  664,  560,
 /*   480 */   558,  556,  555,  554,  553,  552,  551,  550,  549,  548,
 /*   490 */   547,  568,  546,  545,  543,  541,  539,  538,  537,  567,
 /*   500 */   566,  536,  534,  532,  530,  529,  528,  527,  526,  525,
 /*   510 */   524,  523,  522,  521,  520,  519,  518,
};

/* The next table maps tokens into fallback tokens.  If a construct
** like the following:
** 
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
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
    0,  /*    COMPACT => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
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
*/
struct yyStackEntry {
  YYACTIONTYPE stateno;  /* The state-number */
  YYCODETYPE major;      /* The major token value.  This is the code
                         ** number for the token at this stack level */
  YYMINORTYPE minor;     /* The user-supplied minor token value.  This
                         ** is the value of the token  */
};
typedef struct yyStackEntry yyStackEntry;

/* The state of the parser is completely contained in an instance of
** the following structure */
struct yyParser {
  int yyidx;                    /* Index of top element in stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyidxMax;                 /* Maximum value of yyidx */
#endif
  int yyerrcnt;                 /* Shifts left before out of the error */
  ParseARG_SDECL                /* A place to hold %extra_argument */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
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

#ifndef NDEBUG
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  "$",             "ID",            "BOOL",          "TINYINT",     
  "SMALLINT",      "INTEGER",       "BIGINT",        "FLOAT",       
  "DOUBLE",        "STRING",        "TIMESTAMP",     "BINARY",      
  "NCHAR",         "OR",            "AND",           "NOT",         
  "EQ",            "NE",            "ISNULL",        "NOTNULL",     
  "IS",            "LIKE",          "GLOB",          "BETWEEN",     
  "IN",            "GT",            "GE",            "LT",          
  "LE",            "BITAND",        "BITOR",         "LSHIFT",      
  "RSHIFT",        "PLUS",          "MINUS",         "DIVIDE",      
  "TIMES",         "STAR",          "SLASH",         "REM",         
  "CONCAT",        "UMINUS",        "UPLUS",         "BITNOT",      
  "SHOW",          "DATABASES",     "TOPICS",        "MNODES",      
  "DNODES",        "ACCOUNTS",      "USERS",         "MODULES",     
  "QUERIES",       "CONNECTIONS",   "STREAMS",       "VARIABLES",   
  "SCORES",        "GRANTS",        "VNODES",        "IPTOKEN",     
  "DOT",           "CREATE",        "TABLE",         "STABLE",      
  "DATABASE",      "TABLES",        "STABLES",       "VGROUPS",     
  "DROP",          "TOPIC",         "DNODE",         "USER",        
  "ACCOUNT",       "USE",           "DESCRIBE",      "ALTER",       
  "PASS",          "PRIVILEGE",     "LOCAL",         "COMPACT",     
  "LP",            "RP",            "IF",            "EXISTS",      
  "PPS",           "TSERIES",       "DBS",           "STORAGE",     
  "QTIME",         "CONNS",         "STATE",         "KEEP",        
  "CACHE",         "REPLICA",       "QUORUM",        "DAYS",        
  "MINROWS",       "MAXROWS",       "BLOCKS",        "CTIME",       
  "WAL",           "FSYNC",         "COMP",          "PRECISION",   
  "UPDATE",        "CACHELAST",     "PARTITIONS",    "UNSIGNED",    
  "TAGS",          "USING",         "COMMA",         "AS",          
  "NULL",          "SELECT",        "UNION",         "ALL",         
  "DISTINCT",      "FROM",          "VARIABLE",      "INTERVAL",    
  "SESSION",       "FILL",          "SLIDING",       "ORDER",       
  "BY",            "ASC",           "DESC",          "GROUP",       
  "HAVING",        "LIMIT",         "OFFSET",        "SLIMIT",      
  "SOFFSET",       "WHERE",         "NOW",           "RESET",       
  "QUERY",         "SYNCDB",        "ADD",           "COLUMN",      
  "TAG",           "CHANGE",        "SET",           "KILL",        
  "CONNECTION",    "STREAM",        "COLON",         "ABORT",       
  "AFTER",         "ATTACH",        "BEFORE",        "BEGIN",       
  "CASCADE",       "CLUSTER",       "CONFLICT",      "COPY",        
  "DEFERRED",      "DELIMITERS",    "DETACH",        "EACH",        
  "END",           "EXPLAIN",       "FAIL",          "FOR",         
  "IGNORE",        "IMMEDIATE",     "INITIALLY",     "INSTEAD",     
  "MATCH",         "KEY",           "OF",            "RAISE",       
  "REPLACE",       "RESTRICT",      "ROW",           "STATEMENT",   
  "TRIGGER",       "VIEW",          "SEMI",          "NONE",        
  "PREV",          "LINEAR",        "IMPORT",        "TBNAME",      
  "JOIN",          "INSERT",        "INTO",          "VALUES",      
  "error",         "program",       "cmd",           "dbPrefix",    
  "ids",           "cpxName",       "ifexists",      "alter_db_optr",
  "alter_topic_optr",  "acct_optr",     "exprlist",      "ifnotexists", 
  "db_optr",       "topic_optr",    "pps",           "tseries",     
  "dbs",           "streams",       "storage",       "qtime",       
  "users",         "conns",         "state",         "keep",        
  "tagitemlist",   "cache",         "replica",       "quorum",      
  "days",          "minrows",       "maxrows",       "blocks",      
  "ctime",         "wal",           "fsync",         "comp",        
  "prec",          "update",        "cachelast",     "partitions",  
  "typename",      "signed",        "create_table_args",  "create_stable_args",
  "create_table_list",  "create_from_stable",  "columnlist",    "tagNamelist", 
  "select",        "column",        "tagitem",       "selcollist",  
  "from",          "where_opt",     "interval_opt",  "session_option",
  "fill_opt",      "sliding_opt",   "groupby_opt",   "orderby_opt", 
  "having_opt",    "slimit_opt",    "limit_opt",     "union",       
  "sclp",          "distinct",      "expr",          "as",          
  "tablelist",     "tmvar",         "sortlist",      "sortitem",    
  "item",          "sortorder",     "grouplist",     "expritem",    
};
#endif /* NDEBUG */

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
 /*  48 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
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
 /*  59 */ "cmd ::= CREATE USER ids PASS ids",
 /*  60 */ "pps ::=",
 /*  61 */ "pps ::= PPS INTEGER",
 /*  62 */ "tseries ::=",
 /*  63 */ "tseries ::= TSERIES INTEGER",
 /*  64 */ "dbs ::=",
 /*  65 */ "dbs ::= DBS INTEGER",
 /*  66 */ "streams ::=",
 /*  67 */ "streams ::= STREAMS INTEGER",
 /*  68 */ "storage ::=",
 /*  69 */ "storage ::= STORAGE INTEGER",
 /*  70 */ "qtime ::=",
 /*  71 */ "qtime ::= QTIME INTEGER",
 /*  72 */ "users ::=",
 /*  73 */ "users ::= USERS INTEGER",
 /*  74 */ "conns ::=",
 /*  75 */ "conns ::= CONNS INTEGER",
 /*  76 */ "state ::=",
 /*  77 */ "state ::= STATE ids",
 /*  78 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  79 */ "keep ::= KEEP tagitemlist",
 /*  80 */ "cache ::= CACHE INTEGER",
 /*  81 */ "replica ::= REPLICA INTEGER",
 /*  82 */ "quorum ::= QUORUM INTEGER",
 /*  83 */ "days ::= DAYS INTEGER",
 /*  84 */ "minrows ::= MINROWS INTEGER",
 /*  85 */ "maxrows ::= MAXROWS INTEGER",
 /*  86 */ "blocks ::= BLOCKS INTEGER",
 /*  87 */ "ctime ::= CTIME INTEGER",
 /*  88 */ "wal ::= WAL INTEGER",
 /*  89 */ "fsync ::= FSYNC INTEGER",
 /*  90 */ "comp ::= COMP INTEGER",
 /*  91 */ "prec ::= PRECISION STRING",
 /*  92 */ "update ::= UPDATE INTEGER",
 /*  93 */ "cachelast ::= CACHELAST INTEGER",
 /*  94 */ "partitions ::= PARTITIONS INTEGER",
 /*  95 */ "db_optr ::=",
 /*  96 */ "db_optr ::= db_optr cache",
 /*  97 */ "db_optr ::= db_optr replica",
 /*  98 */ "db_optr ::= db_optr quorum",
 /*  99 */ "db_optr ::= db_optr days",
 /* 100 */ "db_optr ::= db_optr minrows",
 /* 101 */ "db_optr ::= db_optr maxrows",
 /* 102 */ "db_optr ::= db_optr blocks",
 /* 103 */ "db_optr ::= db_optr ctime",
 /* 104 */ "db_optr ::= db_optr wal",
 /* 105 */ "db_optr ::= db_optr fsync",
 /* 106 */ "db_optr ::= db_optr comp",
 /* 107 */ "db_optr ::= db_optr prec",
 /* 108 */ "db_optr ::= db_optr keep",
 /* 109 */ "db_optr ::= db_optr update",
 /* 110 */ "db_optr ::= db_optr cachelast",
 /* 111 */ "topic_optr ::= db_optr",
 /* 112 */ "topic_optr ::= topic_optr partitions",
 /* 113 */ "alter_db_optr ::=",
 /* 114 */ "alter_db_optr ::= alter_db_optr replica",
 /* 115 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 116 */ "alter_db_optr ::= alter_db_optr keep",
 /* 117 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 118 */ "alter_db_optr ::= alter_db_optr comp",
 /* 119 */ "alter_db_optr ::= alter_db_optr wal",
 /* 120 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 121 */ "alter_db_optr ::= alter_db_optr update",
 /* 122 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 123 */ "alter_topic_optr ::= alter_db_optr",
 /* 124 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 125 */ "typename ::= ids",
 /* 126 */ "typename ::= ids LP signed RP",
 /* 127 */ "typename ::= ids UNSIGNED",
 /* 128 */ "signed ::= INTEGER",
 /* 129 */ "signed ::= PLUS INTEGER",
 /* 130 */ "signed ::= MINUS INTEGER",
 /* 131 */ "cmd ::= CREATE TABLE create_table_args",
 /* 132 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 133 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 134 */ "cmd ::= CREATE TABLE create_table_list",
 /* 135 */ "create_table_list ::= create_from_stable",
 /* 136 */ "create_table_list ::= create_table_list create_from_stable",
 /* 137 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 138 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 139 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 140 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 141 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 142 */ "tagNamelist ::= ids",
 /* 143 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 144 */ "columnlist ::= columnlist COMMA column",
 /* 145 */ "columnlist ::= column",
 /* 146 */ "column ::= ids typename",
 /* 147 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 148 */ "tagitemlist ::= tagitem",
 /* 149 */ "tagitem ::= INTEGER",
 /* 150 */ "tagitem ::= FLOAT",
 /* 151 */ "tagitem ::= STRING",
 /* 152 */ "tagitem ::= BOOL",
 /* 153 */ "tagitem ::= NULL",
 /* 154 */ "tagitem ::= MINUS INTEGER",
 /* 155 */ "tagitem ::= MINUS FLOAT",
 /* 156 */ "tagitem ::= PLUS INTEGER",
 /* 157 */ "tagitem ::= PLUS FLOAT",
 /* 158 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 159 */ "select ::= LP select RP",
 /* 160 */ "union ::= select",
 /* 161 */ "union ::= union UNION ALL select",
 /* 162 */ "cmd ::= union",
 /* 163 */ "select ::= SELECT selcollist",
 /* 164 */ "sclp ::= selcollist COMMA",
 /* 165 */ "sclp ::=",
 /* 166 */ "selcollist ::= sclp distinct expr as",
 /* 167 */ "selcollist ::= sclp STAR",
 /* 168 */ "as ::= AS ids",
 /* 169 */ "as ::= ids",
 /* 170 */ "as ::=",
 /* 171 */ "distinct ::= DISTINCT",
 /* 172 */ "distinct ::=",
 /* 173 */ "from ::= FROM tablelist",
 /* 174 */ "from ::= FROM LP union RP",
 /* 175 */ "tablelist ::= ids cpxName",
 /* 176 */ "tablelist ::= ids cpxName ids",
 /* 177 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 178 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 179 */ "tmvar ::= VARIABLE",
 /* 180 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 181 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 182 */ "interval_opt ::=",
 /* 183 */ "session_option ::=",
 /* 184 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 185 */ "fill_opt ::=",
 /* 186 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 187 */ "fill_opt ::= FILL LP ID RP",
 /* 188 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 189 */ "sliding_opt ::=",
 /* 190 */ "orderby_opt ::=",
 /* 191 */ "orderby_opt ::= ORDER BY sortlist",
 /* 192 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 193 */ "sortlist ::= item sortorder",
 /* 194 */ "item ::= ids cpxName",
 /* 195 */ "sortorder ::= ASC",
 /* 196 */ "sortorder ::= DESC",
 /* 197 */ "sortorder ::=",
 /* 198 */ "groupby_opt ::=",
 /* 199 */ "groupby_opt ::= GROUP BY grouplist",
 /* 200 */ "grouplist ::= grouplist COMMA item",
 /* 201 */ "grouplist ::= item",
 /* 202 */ "having_opt ::=",
 /* 203 */ "having_opt ::= HAVING expr",
 /* 204 */ "limit_opt ::=",
 /* 205 */ "limit_opt ::= LIMIT signed",
 /* 206 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 207 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 208 */ "slimit_opt ::=",
 /* 209 */ "slimit_opt ::= SLIMIT signed",
 /* 210 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 211 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 212 */ "where_opt ::=",
 /* 213 */ "where_opt ::= WHERE expr",
 /* 214 */ "expr ::= LP expr RP",
 /* 215 */ "expr ::= ID",
 /* 216 */ "expr ::= ID DOT ID",
 /* 217 */ "expr ::= ID DOT STAR",
 /* 218 */ "expr ::= INTEGER",
 /* 219 */ "expr ::= MINUS INTEGER",
 /* 220 */ "expr ::= PLUS INTEGER",
 /* 221 */ "expr ::= FLOAT",
 /* 222 */ "expr ::= MINUS FLOAT",
 /* 223 */ "expr ::= PLUS FLOAT",
 /* 224 */ "expr ::= STRING",
 /* 225 */ "expr ::= NOW",
 /* 226 */ "expr ::= VARIABLE",
 /* 227 */ "expr ::= PLUS VARIABLE",
 /* 228 */ "expr ::= MINUS VARIABLE",
 /* 229 */ "expr ::= BOOL",
 /* 230 */ "expr ::= NULL",
 /* 231 */ "expr ::= ID LP exprlist RP",
 /* 232 */ "expr ::= ID LP STAR RP",
 /* 233 */ "expr ::= expr IS NULL",
 /* 234 */ "expr ::= expr IS NOT NULL",
 /* 235 */ "expr ::= expr LT expr",
 /* 236 */ "expr ::= expr GT expr",
 /* 237 */ "expr ::= expr LE expr",
 /* 238 */ "expr ::= expr GE expr",
 /* 239 */ "expr ::= expr NE expr",
 /* 240 */ "expr ::= expr EQ expr",
 /* 241 */ "expr ::= expr BETWEEN expr AND expr",
 /* 242 */ "expr ::= expr AND expr",
 /* 243 */ "expr ::= expr OR expr",
 /* 244 */ "expr ::= expr PLUS expr",
 /* 245 */ "expr ::= expr MINUS expr",
 /* 246 */ "expr ::= expr STAR expr",
 /* 247 */ "expr ::= expr SLASH expr",
 /* 248 */ "expr ::= expr REM expr",
 /* 249 */ "expr ::= expr LIKE expr",
 /* 250 */ "expr ::= expr IN LP exprlist RP",
 /* 251 */ "exprlist ::= exprlist COMMA expritem",
 /* 252 */ "exprlist ::= expritem",
 /* 253 */ "expritem ::= expr",
 /* 254 */ "expritem ::=",
 /* 255 */ "cmd ::= RESET QUERY CACHE",
 /* 256 */ "cmd ::= SYNCDB ids REPLICA",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 258 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 260 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 263 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 264 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 265 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 266 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 267 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 268 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 269 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 270 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.
*/
static void yyGrowStack(yyParser *p){
  int newSize;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  if( pNew ){
    p->yystack = pNew;
    p->yystksz = newSize;
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sStack grows to %d entries!\n",
              yyTracePrompt, p->yystksz);
    }
#endif
  }
}
#endif

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
void *ParseAlloc(void *(*mallocProc)(size_t)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (size_t)sizeof(yyParser) );
  if( pParser ){
    pParser->yyidx = -1;
#ifdef YYTRACKMAXSTACKDEPTH
    pParser->yyidxMax = 0;
#endif
#if YYSTACKDEPTH<=0
    pParser->yystack = NULL;
    pParser->yystksz = 0;
    yyGrowStack(pParser);
#endif
  }
  return pParser;
}

/* The following function deletes the value associated with a
** symbol.  The symbol can be either a terminal or nonterminal.
** "yymajor" is the symbol code, and "yypminor" is a pointer to
** the value.
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
    ** which appear on the RHS of the rule, but which are not used
    ** inside the C code.
    */
    case 198: /* exprlist */
    case 239: /* selcollist */
    case 252: /* sclp */
{
tSqlExprListDestroy((yypminor->yy285));
}
      break;
    case 211: /* keep */
    case 212: /* tagitemlist */
    case 234: /* columnlist */
    case 235: /* tagNamelist */
    case 244: /* fill_opt */
    case 246: /* groupby_opt */
    case 247: /* orderby_opt */
    case 258: /* sortlist */
    case 262: /* grouplist */
{
taosArrayDestroy((yypminor->yy285));
}
      break;
    case 232: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy470));
}
      break;
    case 236: /* select */
{
destroySqlNode((yypminor->yy344));
}
      break;
    case 240: /* from */
    case 256: /* tablelist */
{
destroyRelationInfo((yypminor->yy148));
}
      break;
    case 241: /* where_opt */
    case 248: /* having_opt */
    case 254: /* expr */
    case 263: /* expritem */
{
tSqlExprDestroy((yypminor->yy178));
}
      break;
    case 251: /* union */
{
destroyAllSqlNode((yypminor->yy285));
}
      break;
    case 259: /* sortitem */
{
tVariantDestroy(&(yypminor->yy362));
}
      break;
    default:  break;   /* If no destructor action specified: do nothing */
  }
}

/*
** Pop the parser's stack once.
**
** If there is a destructor routine associated with the token which
** is popped from the stack, then call it.
**
** Return the major token number for the symbol popped.
*/
static int yy_pop_parser_stack(yyParser *pParser){
  YYCODETYPE yymajor;
  yyStackEntry *yytos = &pParser->yystack[pParser->yyidx];

  if( pParser->yyidx<0 ) return 0;
#ifndef NDEBUG
  if( yyTraceFILE && pParser->yyidx>=0 ){
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yymajor = yytos->major;
  yy_destructor(pParser, yymajor, &yytos->minor);
  pParser->yyidx--;
  return yymajor;
}

/* 
** Deallocate and destroy a parser.  Destructors are all called for
** all stack elements before shutting the parser down.
**
** Inputs:
** <ul>
** <li>  A pointer to the parser.  This should be a pointer
**       obtained from ParseAlloc.
** <li>  A pointer to a function used to reclaim memory obtained
**       from malloc.
** </ul>
*/
void ParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
  yyParser *pParser = (yyParser*)p;
  if( pParser==0 ) return;
  while( pParser->yyidx>=0 ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  free(pParser->yystack);
#endif
  (*freeProc)((void*)pParser);
}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyidxMax;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
**
** If the look-ahead token is YYNOCODE, then check to see if the action is
** independent of the look-ahead.  If it is, return the action, otherwise
** return YY_NO_ACTION.
*/
static int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yystack[pParser->yyidx].stateno;
 
  if( stateno>YY_SHIFT_COUNT
   || (i = yy_shift_ofst[stateno])==YY_SHIFT_USE_DFLT ){
    return yy_default[stateno];
  }
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
  if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
    if( iLookAhead>0 ){
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
        return yy_find_shift_action(pParser, iFallback);
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
          yy_lookahead[j]==YYWILDCARD
        ){
#ifndef NDEBUG
          if( yyTraceFILE ){
            fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[YYWILDCARD]);
          }
#endif /* NDEBUG */
          return yy_action[j];
        }
      }
#endif /* YYWILDCARD */
    }
    return yy_default[stateno];
  }else{
    return yy_action[i];
  }
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
**
** If the look-ahead token is YYNOCODE, then check to see if the action is
** independent of the look-ahead.  If it is, return the action, otherwise
** return YY_NO_ACTION.
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
  assert( i!=YY_REDUCE_USE_DFLT );
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
static void yyStackOverflow(yyParser *yypParser, YYMINORTYPE *yypMinor){
   ParseARG_FETCH;
   yypParser->yyidx--;
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
   /* Here code is inserted which will execute if the parser
   ** stack every overflows */
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
}

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
  YYMINORTYPE *yypMinor         /* Pointer to the minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yyidx++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( yypParser->yyidx>yypParser->yyidxMax ){
    yypParser->yyidxMax = yypParser->yyidx;
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yyidx>=YYSTACKDEPTH ){
    yyStackOverflow(yypParser, yypMinor);
    return;
  }
#else
  if( yypParser->yyidx>=yypParser->yystksz ){
    yyGrowStack(yypParser);
    if( yypParser->yyidx>=yypParser->yystksz ){
      yyStackOverflow(yypParser, yypMinor);
      return;
    }
  }
#endif
  yytos = &yypParser->yystack[yypParser->yyidx];
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
  yytos->minor = *yypMinor;
#ifndef NDEBUG
  if( yyTraceFILE && yypParser->yyidx>0 ){
    int i;
    fprintf(yyTraceFILE,"%sShift %d\n",yyTracePrompt,yyNewState);
    fprintf(yyTraceFILE,"%sStack:",yyTracePrompt);
    for(i=1; i<=yypParser->yyidx; i++)
      fprintf(yyTraceFILE," %s",yyTokenName[yypParser->yystack[i].major]);
    fprintf(yyTraceFILE,"\n");
  }
#endif
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
  unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
} yyRuleInfo[] = {
  { 189, 1 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 2 },
  { 190, 3 },
  { 191, 0 },
  { 191, 2 },
  { 193, 0 },
  { 193, 2 },
  { 190, 5 },
  { 190, 5 },
  { 190, 4 },
  { 190, 3 },
  { 190, 5 },
  { 190, 3 },
  { 190, 5 },
  { 190, 3 },
  { 190, 4 },
  { 190, 5 },
  { 190, 5 },
  { 190, 4 },
  { 190, 4 },
  { 190, 3 },
  { 190, 3 },
  { 190, 3 },
  { 190, 2 },
  { 190, 3 },
  { 190, 5 },
  { 190, 5 },
  { 190, 4 },
  { 190, 5 },
  { 190, 3 },
  { 190, 4 },
  { 190, 4 },
  { 190, 4 },
  { 190, 4 },
  { 190, 6 },
  { 190, 6 },
  { 192, 1 },
  { 192, 1 },
  { 194, 2 },
  { 194, 0 },
  { 199, 3 },
  { 199, 0 },
  { 190, 3 },
  { 190, 6 },
  { 190, 5 },
  { 190, 5 },
  { 190, 5 },
  { 202, 0 },
  { 202, 2 },
  { 203, 0 },
  { 203, 2 },
  { 204, 0 },
  { 204, 2 },
  { 205, 0 },
  { 205, 2 },
  { 206, 0 },
  { 206, 2 },
  { 207, 0 },
  { 207, 2 },
  { 208, 0 },
  { 208, 2 },
  { 209, 0 },
  { 209, 2 },
  { 210, 0 },
  { 210, 2 },
  { 197, 9 },
  { 211, 2 },
  { 213, 2 },
  { 214, 2 },
  { 215, 2 },
  { 216, 2 },
  { 217, 2 },
  { 218, 2 },
  { 219, 2 },
  { 220, 2 },
  { 221, 2 },
  { 222, 2 },
  { 223, 2 },
  { 224, 2 },
  { 225, 2 },
  { 226, 2 },
  { 227, 2 },
  { 200, 0 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 200, 2 },
  { 201, 1 },
  { 201, 2 },
  { 195, 0 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 196, 1 },
  { 196, 2 },
  { 228, 1 },
  { 228, 4 },
  { 228, 2 },
  { 229, 1 },
  { 229, 2 },
  { 229, 2 },
  { 190, 3 },
  { 190, 3 },
  { 190, 3 },
  { 190, 3 },
  { 232, 1 },
  { 232, 2 },
  { 230, 6 },
  { 231, 10 },
  { 233, 10 },
  { 233, 13 },
  { 235, 3 },
  { 235, 1 },
  { 230, 5 },
  { 234, 3 },
  { 234, 1 },
  { 237, 2 },
  { 212, 3 },
  { 212, 1 },
  { 238, 1 },
  { 238, 1 },
  { 238, 1 },
  { 238, 1 },
  { 238, 1 },
  { 238, 2 },
  { 238, 2 },
  { 238, 2 },
  { 238, 2 },
  { 236, 13 },
  { 236, 3 },
  { 251, 1 },
  { 251, 4 },
  { 190, 1 },
  { 236, 2 },
  { 252, 2 },
  { 252, 0 },
  { 239, 4 },
  { 239, 2 },
  { 255, 2 },
  { 255, 1 },
  { 255, 0 },
  { 253, 1 },
  { 253, 0 },
  { 240, 2 },
  { 240, 4 },
  { 256, 2 },
  { 256, 3 },
  { 256, 4 },
  { 256, 5 },
  { 257, 1 },
  { 242, 4 },
  { 242, 6 },
  { 242, 0 },
  { 243, 0 },
  { 243, 7 },
  { 244, 0 },
  { 244, 6 },
  { 244, 4 },
  { 245, 4 },
  { 245, 0 },
  { 247, 0 },
  { 247, 3 },
  { 258, 4 },
  { 258, 2 },
  { 260, 2 },
  { 261, 1 },
  { 261, 1 },
  { 261, 0 },
  { 246, 0 },
  { 246, 3 },
  { 262, 3 },
  { 262, 1 },
  { 248, 0 },
  { 248, 2 },
  { 250, 0 },
  { 250, 2 },
  { 250, 4 },
  { 250, 4 },
  { 249, 0 },
  { 249, 2 },
  { 249, 4 },
  { 249, 4 },
  { 241, 0 },
  { 241, 2 },
  { 254, 3 },
  { 254, 1 },
  { 254, 3 },
  { 254, 3 },
  { 254, 1 },
  { 254, 2 },
  { 254, 2 },
  { 254, 1 },
  { 254, 2 },
  { 254, 2 },
  { 254, 1 },
  { 254, 1 },
  { 254, 1 },
  { 254, 2 },
  { 254, 2 },
  { 254, 1 },
  { 254, 1 },
  { 254, 4 },
  { 254, 4 },
  { 254, 3 },
  { 254, 4 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 5 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 3 },
  { 254, 5 },
  { 198, 3 },
  { 198, 1 },
  { 263, 1 },
  { 263, 0 },
  { 190, 3 },
  { 190, 3 },
  { 190, 7 },
  { 190, 7 },
  { 190, 7 },
  { 190, 7 },
  { 190, 8 },
  { 190, 9 },
  { 190, 7 },
  { 190, 7 },
  { 190, 7 },
  { 190, 7 },
  { 190, 8 },
  { 190, 3 },
  { 190, 5 },
  { 190, 5 },
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
*/
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  int yyruleno                 /* Number of the rule by which to reduce */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  YYMINORTYPE yygotominor;        /* The LHS of the rule reduced */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
  yymsp = &yypParser->yystack[yypParser->yyidx];
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno>=0 
        && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    fprintf(yyTraceFILE, "%sReduce [%s].\n", yyTracePrompt,
      yyRuleName[yyruleno]);
  }
#endif /* NDEBUG */

  /* Silence complaints from purify about yygotominor being uninitialized
  ** in some cases when it is copied into the stack after the following
  ** switch.  yygotominor is uninitialized when a rule reduces that does
  ** not set the value of its left-hand side nonterminal.  Leaving the
  ** value of the nonterminal uninitialized is utterly harmless as long
  ** as the value is never used.  So really the only thing this code
  ** accomplishes is to quieten purify.  
  **
  ** 2007-01-16:  The wireshark project (www.wireshark.org) reports that
  ** without this code, their parser segfaults.  I'm not sure what there
  ** parser is doing to make this happen.  This is the second bug report
  ** from wireshark this week.  Clearly they are stressing Lemon in ways
  ** that it has not been previously stressed...  (SQLite ticket #2172)
  */
  /*memset(&yygotominor, 0, sizeof(yygotominor));*/
  yygotominor = yyzerominor;


  switch( yyruleno ){
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
      case 0: /* program ::= cmd */
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
{yygotominor.yy0.n = 0; yygotominor.yy0.type = 0;}
        break;
      case 17: /* dbPrefix ::= ids DOT */
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
        break;
      case 18: /* cpxName ::= */
{yygotominor.yy0.n = 0;  }
        break;
      case 19: /* cpxName ::= DOT ids */
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy187);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 48: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy285);}
        break;
      case 49: /* ids ::= ID */
      case 50: /* ids ::= STRING */ yytestcase(yyruleno==50);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 51: /* ifexists ::= IF EXISTS */
      case 53: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==53);
{ yygotominor.yy0.n = 1;}
        break;
      case 52: /* ifexists ::= */
      case 54: /* ifnotexists ::= */ yytestcase(yyruleno==54);
      case 172: /* distinct ::= */ yytestcase(yyruleno==172);
{ yygotominor.yy0.n = 0;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 60: /* pps ::= */
      case 62: /* tseries ::= */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= */ yytestcase(yyruleno==64);
      case 66: /* streams ::= */ yytestcase(yyruleno==66);
      case 68: /* storage ::= */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= */ yytestcase(yyruleno==70);
      case 72: /* users ::= */ yytestcase(yyruleno==72);
      case 74: /* conns ::= */ yytestcase(yyruleno==74);
      case 76: /* state ::= */ yytestcase(yyruleno==76);
{ yygotominor.yy0.n = 0;   }
        break;
      case 61: /* pps ::= PPS INTEGER */
      case 63: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==63);
      case 65: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==69);
      case 71: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==71);
      case 73: /* users ::= USERS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==75);
      case 77: /* state ::= STATE ids */ yytestcase(yyruleno==77);
{ yygotominor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 78: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yygotominor.yy187.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy187.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy187.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy187.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy187.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy187.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy187.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy187.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy187.stat    = yymsp[0].minor.yy0;
}
        break;
      case 79: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy285 = yymsp[0].minor.yy285; }
        break;
      case 80: /* cache ::= CACHE INTEGER */
      case 81: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==81);
      case 82: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==82);
      case 83: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==87);
      case 88: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==88);
      case 89: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==89);
      case 90: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==90);
      case 91: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==91);
      case 92: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==92);
      case 93: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==93);
      case 94: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==94);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 95: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy526); yygotominor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 96: /* db_optr ::= db_optr cache */
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 97: /* db_optr ::= db_optr replica */
      case 114: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==114);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 98: /* db_optr ::= db_optr quorum */
      case 115: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==115);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* db_optr ::= db_optr days */
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 100: /* db_optr ::= db_optr minrows */
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 101: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 102: /* db_optr ::= db_optr blocks */
      case 117: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==117);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 103: /* db_optr ::= db_optr ctime */
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 104: /* db_optr ::= db_optr wal */
      case 119: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==119);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 105: /* db_optr ::= db_optr fsync */
      case 120: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==120);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* db_optr ::= db_optr comp */
      case 118: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==118);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 107: /* db_optr ::= db_optr prec */
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.precision = yymsp[0].minor.yy0; }
        break;
      case 108: /* db_optr ::= db_optr keep */
      case 116: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==116);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.keep = yymsp[0].minor.yy285; }
        break;
      case 109: /* db_optr ::= db_optr update */
      case 121: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==121);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 110: /* db_optr ::= db_optr cachelast */
      case 122: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==122);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 111: /* topic_optr ::= db_optr */
      case 123: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==123);
{ yygotominor.yy526 = yymsp[0].minor.yy526; yygotominor.yy526.dbType = TSDB_DB_TYPE_TOPIC; }
        break;
      case 112: /* topic_optr ::= topic_optr partitions */
      case 124: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==124);
{ yygotominor.yy526 = yymsp[-1].minor.yy526; yygotominor.yy526.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 113: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy526); yygotominor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 125: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yygotominor.yy295, &yymsp[0].minor.yy0);
}
        break;
      case 126: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy525 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yygotominor.yy295, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy525;  // negative value of name length
    tSetColumnType(&yygotominor.yy295, &yymsp[-3].minor.yy0);
  }
}
        break;
      case 127: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yygotominor.yy295, &yymsp[-1].minor.yy0);
}
        break;
      case 128: /* signed ::= INTEGER */
      case 129: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==129);
{ yygotominor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 130: /* signed ::= MINUS INTEGER */
      case 131: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==133);
{ yygotominor.yy525 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 134: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy470;}
        break;
      case 135: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yygotominor.yy470 = pCreateTable;
}
        break;
      case 136: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy470->childTableInfo, &yymsp[0].minor.yy96);
  yygotominor.yy470 = yymsp[-1].minor.yy470;
}
        break;
      case 137: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yygotominor.yy470 = tSetCreateTableInfo(yymsp[-1].minor.yy285, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
        break;
      case 138: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yygotominor.yy470 = tSetCreateTableInfo(yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yygotominor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy285, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yygotominor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
        break;
      case 141: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy0); yygotominor.yy285 = yymsp[-2].minor.yy285;  }
        break;
      case 142: /* tagNamelist ::= ids */
{yygotominor.yy285 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yygotominor.yy285, &yymsp[0].minor.yy0);}
        break;
      case 143: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yygotominor.yy470 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy344, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
        break;
      case 144: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy295); yygotominor.yy285 = yymsp[-2].minor.yy285;  }
        break;
      case 145: /* columnlist ::= column */
{yygotominor.yy285 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yygotominor.yy285, &yymsp[0].minor.yy295);}
        break;
      case 146: /* column ::= ids typename */
{
  tSetColumnInfo(&yygotominor.yy295, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy295);
}
        break;
      case 147: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);    }
        break;
      case 148: /* tagitemlist ::= tagitem */
{ yygotominor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1); }
        break;
      case 149: /* tagitem ::= INTEGER */
      case 150: /* tagitem ::= FLOAT */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= STRING */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= BOOL */ yytestcase(yyruleno==152);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy362, &yymsp[0].minor.yy0); }
        break;
      case 153: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy362, &yymsp[0].minor.yy0); }
        break;
      case 154: /* tagitem ::= MINUS INTEGER */
      case 155: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==157);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy362, &yymsp[-1].minor.yy0);
}
        break;
      case 158: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy344 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy285, yymsp[-10].minor.yy148, yymsp[-9].minor.yy178, yymsp[-4].minor.yy285, yymsp[-3].minor.yy285, &yymsp[-8].minor.yy376, &yymsp[-7].minor.yy523, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy285, &yymsp[0].minor.yy438, &yymsp[-1].minor.yy438, yymsp[-2].minor.yy178);
}
        break;
      case 159: /* select ::= LP select RP */
{yygotominor.yy344 = yymsp[-1].minor.yy344;}
        break;
      case 160: /* union ::= select */
{ yygotominor.yy285 = setSubclause(NULL, yymsp[0].minor.yy344); }
        break;
      case 161: /* union ::= union UNION ALL select */
{ yygotominor.yy285 = appendSelectClause(yymsp[-3].minor.yy285, yymsp[0].minor.yy344); }
        break;
      case 162: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy285, NULL, TSDB_SQL_SELECT); }
        break;
      case 163: /* select ::= SELECT selcollist */
{
  yygotominor.yy344 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy285, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 164: /* sclp ::= selcollist COMMA */
{yygotominor.yy285 = yymsp[-1].minor.yy285;}
        break;
      case 165: /* sclp ::= */
      case 190: /* orderby_opt ::= */ yytestcase(yyruleno==190);
{yygotominor.yy285 = 0;}
        break;
      case 166: /* selcollist ::= sclp distinct expr as */
{
   yygotominor.yy285 = tSqlExprListAppend(yymsp[-3].minor.yy285, yymsp[-1].minor.yy178,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 167: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yygotominor.yy285 = tSqlExprListAppend(yymsp[-1].minor.yy285, pNode, 0, 0);
}
        break;
      case 168: /* as ::= AS ids */
      case 169: /* as ::= ids */ yytestcase(yyruleno==169);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 170: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 171: /* distinct ::= DISTINCT */
{ yygotominor.yy0 = yymsp[0].minor.yy0;  }
        break;
      case 173: /* from ::= FROM tablelist */
{yygotominor.yy148 = yymsp[0].minor.yy148;}
        break;
      case 174: /* from ::= FROM LP union RP */
{yygotominor.yy148 = setSubquery(NULL, yymsp[-1].minor.yy285);}
        break;
      case 175: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy148 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 176: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy148 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 177: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy148 = setTableNameList(yymsp[-3].minor.yy148, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 178: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy148 = setTableNameList(yymsp[-4].minor.yy148, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 179: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 180: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy376.interval = yymsp[-1].minor.yy0; yygotominor.yy376.offset.n = 0;}
        break;
      case 181: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy376.interval = yymsp[-3].minor.yy0; yygotominor.yy376.offset = yymsp[-1].minor.yy0;}
        break;
      case 182: /* interval_opt ::= */
{memset(&yygotominor.yy376, 0, sizeof(yygotominor.yy376));}
        break;
      case 183: /* session_option ::= */
{yygotominor.yy523.col.n = 0; yygotominor.yy523.gap.n = 0;}
        break;
      case 184: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yygotominor.yy523.col = yymsp[-4].minor.yy0;
   yygotominor.yy523.gap = yymsp[-1].minor.yy0;
}
        break;
      case 185: /* fill_opt ::= */
{ yygotominor.yy285 = 0;     }
        break;
      case 186: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy285, &A, -1, 0);
    yygotominor.yy285 = yymsp[-1].minor.yy285;
}
        break;
      case 187: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy285 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 188: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 189: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 191: /* orderby_opt ::= ORDER BY sortlist */
{yygotominor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 192: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy285 = tVariantListAppend(yymsp[-3].minor.yy285, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
        break;
      case 193: /* sortlist ::= item sortorder */
{
  yygotominor.yy285 = tVariantListAppend(NULL, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
        break;
      case 194: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy362, &yymsp[-1].minor.yy0);
}
        break;
      case 195: /* sortorder ::= ASC */
      case 197: /* sortorder ::= */ yytestcase(yyruleno==197);
{ yygotominor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 196: /* sortorder ::= DESC */
{ yygotominor.yy460 = TSDB_ORDER_DESC;}
        break;
      case 198: /* groupby_opt ::= */
{ yygotominor.yy285 = 0;}
        break;
      case 199: /* groupby_opt ::= GROUP BY grouplist */
{ yygotominor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 200: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);
}
        break;
      case 201: /* grouplist ::= item */
{
  yygotominor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1);
}
        break;
      case 202: /* having_opt ::= */
      case 212: /* where_opt ::= */ yytestcase(yyruleno==212);
      case 254: /* expritem ::= */ yytestcase(yyruleno==254);
{yygotominor.yy178 = 0;}
        break;
      case 203: /* having_opt ::= HAVING expr */
      case 213: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==213);
      case 253: /* expritem ::= expr */ yytestcase(yyruleno==253);
{yygotominor.yy178 = yymsp[0].minor.yy178;}
        break;
      case 204: /* limit_opt ::= */
      case 208: /* slimit_opt ::= */ yytestcase(yyruleno==208);
{yygotominor.yy438.limit = -1; yygotominor.yy438.offset = 0;}
        break;
      case 205: /* limit_opt ::= LIMIT signed */
      case 209: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==209);
{yygotominor.yy438.limit = yymsp[0].minor.yy525;  yygotominor.yy438.offset = 0;}
        break;
      case 206: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yygotominor.yy438.limit = yymsp[-2].minor.yy525;  yygotominor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 207: /* limit_opt ::= LIMIT signed COMMA signed */
{ yygotominor.yy438.limit = yymsp[0].minor.yy525;  yygotominor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 210: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yygotominor.yy438.limit = yymsp[-2].minor.yy525;  yygotominor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 211: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yygotominor.yy438.limit = yymsp[0].minor.yy525;  yygotominor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 214: /* expr ::= LP expr RP */
{yygotominor.yy178 = yymsp[-1].minor.yy178; yygotominor.yy178->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy178->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
        break;
      case 215: /* expr ::= ID */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 216: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 217: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 218: /* expr ::= INTEGER */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 219: /* expr ::= MINUS INTEGER */
      case 220: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==220);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 221: /* expr ::= FLOAT */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 222: /* expr ::= MINUS FLOAT */
      case 223: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==223);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 224: /* expr ::= STRING */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 225: /* expr ::= NOW */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 226: /* expr ::= VARIABLE */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 227: /* expr ::= PLUS VARIABLE */
      case 228: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
        break;
      case 229: /* expr ::= BOOL */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 230: /* expr ::= NULL */
{ yygotominor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
        break;
      case 231: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy178 = tSqlExprCreateFunction(yymsp[-1].minor.yy285, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 232: /* expr ::= ID LP STAR RP */
{ yygotominor.yy178 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 233: /* expr ::= expr IS NULL */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, NULL, TK_ISNULL);}
        break;
      case 234: /* expr ::= expr IS NOT NULL */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-3].minor.yy178, NULL, TK_NOTNULL);}
        break;
      case 235: /* expr ::= expr LT expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LT);}
        break;
      case 236: /* expr ::= expr GT expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GT);}
        break;
      case 237: /* expr ::= expr LE expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LE);}
        break;
      case 238: /* expr ::= expr GE expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GE);}
        break;
      case 239: /* expr ::= expr NE expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_NE);}
        break;
      case 240: /* expr ::= expr EQ expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_EQ);}
        break;
      case 241: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy178); yygotominor.yy178 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy178, yymsp[-2].minor.yy178, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy178, TK_LE), TK_AND);}
        break;
      case 242: /* expr ::= expr AND expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_AND);}
        break;
      case 243: /* expr ::= expr OR expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_OR); }
        break;
      case 244: /* expr ::= expr PLUS expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_PLUS);  }
        break;
      case 245: /* expr ::= expr MINUS expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_MINUS); }
        break;
      case 246: /* expr ::= expr STAR expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_STAR);  }
        break;
      case 247: /* expr ::= expr SLASH expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_DIVIDE);}
        break;
      case 248: /* expr ::= expr REM expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_REM);   }
        break;
      case 249: /* expr ::= expr LIKE expr */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LIKE);  }
        break;
      case 250: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy178 = tSqlExprCreate(yymsp[-4].minor.yy178, (tSqlExpr*)yymsp[-1].minor.yy285, TK_IN); }
        break;
      case 251: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy285 = tSqlExprListAppend(yymsp[-2].minor.yy285,yymsp[0].minor.yy178,0, 0);}
        break;
      case 252: /* exprlist ::= expritem */
{yygotominor.yy285 = tSqlExprListAppend(0,yymsp[0].minor.yy178,0, 0);}
        break;
      case 255: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 256: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 257: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 262: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy362, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 268: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 269: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 270: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      default:
        break;
  };
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yypParser->yyidx -= yysize;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if( yyact < YYNSTATE ){
#ifdef NDEBUG
    /* If we are not debugging and the reduce action popped at least
    ** one element off the stack, then we can push the new element back
    ** onto the stack here, and skip the stack overflow test in yy_shift().
    ** That gives a significant speed improvement. */
    if( yysize ){
      yypParser->yyidx++;
      yymsp -= yysize-1;
      yymsp->stateno = (YYACTIONTYPE)yyact;
      yymsp->major = (YYCODETYPE)yygoto;
      yymsp->minor = yygotominor;
    }else
#endif
    {
      yy_shift(yypParser,yyact,yygoto,&yygotominor);
    }
  }else{
    assert( yyact == YYNSTATE + YYNRULE + 1 );
    yy_accept(yypParser);
  }
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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser fails */
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  YYMINORTYPE yyminor            /* The minor type of the error token */
){
  ParseARG_FETCH;
#define TOKEN (yyminor.yy0)

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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */

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
  int yyact;            /* The parser action. */
  int yyendofinput;     /* True if we are at the end of input */
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  /* (re)initialize the parser, if necessary */
  yypParser = (yyParser*)yyp;
  if( yypParser->yyidx<0 ){
#if YYSTACKDEPTH<=0
    if( yypParser->yystksz <=0 ){
      /*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
      yyminorunion = yyzerominor;
      yyStackOverflow(yypParser, &yyminorunion);
      return;
    }
#endif
    yypParser->yyidx = 0;
    yypParser->yyerrcnt = -1;
    yypParser->yystack[0].stateno = 0;
    yypParser->yystack[0].major = 0;
  }
  yyminorunion.yy0 = yyminor;
  yyendofinput = (yymajor==0);
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sInput %s\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact<YYNSTATE ){
      assert( !yyendofinput );  /* Impossible to shift the $ token */
      yy_shift(yypParser,yyact,yymajor,&yyminorunion);
      yypParser->yyerrcnt--;
      yymajor = YYNOCODE;
    }else if( yyact < YYNSTATE + YYNRULE ){
      yy_reduce(yypParser,yyact-YYNSTATE);
    }else{
      assert( yyact == YY_ERROR_ACTION );
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
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yymx = yypParser->yystack[yypParser->yyidx].major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor,&yyminorunion);
        yymajor = YYNOCODE;
      }else{
         while(
          yypParser->yyidx >= 0 &&
          yymx != YYERRORSYMBOL &&
          (yyact = yy_find_reduce_action(
                        yypParser->yystack[yypParser->yyidx].stateno,
                        YYERRORSYMBOL)) >= YYNSTATE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yyidx < 0 || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          YYMINORTYPE u2;
          u2.YYERRSYMDT = 0;
          yy_shift(yypParser,yyact,YYERRORSYMBOL,&u2);
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
      yy_syntax_error(yypParser,yymajor,yyminorunion);
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
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
      }
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yyidx>=0 );
  return;
}
