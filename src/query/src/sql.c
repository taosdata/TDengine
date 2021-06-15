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
#include "ttoken.h"
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
#define YYNOCODE 270
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSessionWindowVal yy15;
  SIntervalVal yy42;
  tSqlExpr* yy68;
  SCreateAcctInfo yy77;
  SArray* yy93;
  int yy150;
  SSqlNode* yy224;
  SWindowStateVal yy274;
  int64_t yy279;
  SLimitVal yy284;
  TAOS_FIELD yy325;
  SRelationInfo* yy330;
  SCreateDbInfo yy372;
  tVariant yy518;
  SCreatedTableInfo yy528;
  SCreateTableSql* yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 543
#define YYNRULE 284
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
#define YY_ACTTAB_COUNT (843)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   526,   51,   50,  125,  124,   49,   48,   47,  525,  142,
 /*    10 */   140,  139,   52,   53,   32,   56,   57,  289,  543,  230,
 /*    20 */    46,  529,   55,  283,   60,   58,   62,   59,  338,  337,
 /*    30 */   136,  528,   51,   50,  828,  341,   49,   48,   47,   52,
 /*    40 */    53,  349,   56,   57,  481,   70,  230,   46,  178,   55,
 /*    50 */   283,   60,   58,   62,   59,  151,  395,   13,  394,   51,
 /*    60 */    50,   93,   90,   49,   48,   47,   52,   53,  245,   56,
 /*    70 */    57,   64,  410,  230,   46,  251,   55,  283,   60,   58,
 /*    80 */    62,   59,  357,  468,  255,  254,   51,   50,   71,  415,
 /*    90 */    49,   48,   47,   52,   54,   34,   56,   57,  259,  144,
 /*   100 */   230,   46,  526,   55,  283,   60,   58,   62,   59,   76,
 /*   110 */   525,  315,  314,   51,   50,   92,  236,   49,   48,   47,
 /*   120 */   348,   64,   60,   58,   62,   59,  369,   21,  278,   20,
 /*   130 */    51,   50,  399,  288,   49,   48,   47,  317,  247,  412,
 /*   140 */   244,  463,  310,  309,   91,   67,  542,  541,  540,  539,
 /*   150 */   538,  537,  536,  535,  534,  533,  532,  531,  530,  340,
 /*   160 */   360,   53,  219,   56,   57,  266,  265,  230,   46,  393,
 /*   170 */    55,  283,   60,   58,   62,   59,  418,  417,   33,  409,
 /*   180 */    51,   50,  240,  228,   49,   48,   47,   56,   57,    8,
 /*   190 */    68,  230,   46,  517,   55,  283,   60,   58,   62,   59,
 /*   200 */    49,   48,   47,  151,   51,   50,    1,  166,   49,   48,
 /*   210 */    47,  423,  435,  434,  433,  432,  431,  430,  429,  428,
 /*   220 */   427,  426,  425,  424,  422,  421,  229,  385,    6,  392,
 /*   230 */   396,  391,  389,   34,  388,  374,  373,   40,  297,  334,
 /*   240 */   333,  296,  295,  294,  332,  293,  331,  330,  329,  292,
 /*   250 */   328,  327,  241,   23,  239,   35,  303,  302,  215,  216,
 /*   260 */   229,  385,  282,  180,  396,   73,  389,  465,  388,  112,
 /*   270 */   204,  111,  280,  286,   87,  313,   19,  205,   18,  463,
 /*   280 */    80,   34,  128,  127,  203,   35,  138,  491,  262,  493,
 /*   290 */   492,  443,  215,  216,  490,  178,  488,  487,  489,   34,
 /*   300 */   486,  485,   40,  408,  334,  333,  453,  195,  452,  332,
 /*   310 */   284,  331,  330,  329,   82,  328,  327,  226,  378,  451,
 /*   320 */   398,  450,   41,  312,  113,  107,  118,  463,  195,  137,
 /*   330 */    61,  117,  123,  126,  116,  501,  384,  387,  227,  378,
 /*   340 */   120,  311,  390,  386,  258,  463,   74,  456,   34,   34,
 /*   350 */   459,   34,  458,  212,  457,   24,  397,   34,   34,  187,
 /*   360 */   185,  183,  679,   34,   61,   34,  182,  131,  130,  129,
 /*   370 */   384,  387,  526,  383,    5,   37,  168,  386,  242,  243,
 /*   380 */   525,  167,  101,   96,  100,   17,  106,   16,  105,   75,
 /*   390 */   307,  306,   24,  305,  463,  463,   15,  463,   14,  304,
 /*   400 */   234,   25,   94,  463,  463,  233,  401,  220,   64,  463,
 /*   410 */   379,  463,  146,   79,   35,  526,   82,    3,  179,  365,
 /*   420 */    89,  262,  347,  525,   41,  482,  366,  335,  502,  178,
 /*   430 */   362,  115,  260,   34,   77,  246,  325,  416,  151,  357,
 /*   440 */   237,  299,  151,  357,  413,  350,  235,  224,  500,  195,
 /*   450 */   413,  413,  218,  339,  499,  222,  455,  454,  498,  221,
 /*   460 */   377,  497,  496,  495,  494,  484,  480,  479,  478,  477,
 /*   470 */   476,  464,  475,  474,  473,   35,   28,  467,  469,  470,
 /*   480 */   466,  110,  108,  104,  102,   99,   66,   65,  298,  441,
 /*   490 */   442,  440,  439,  438,  437,   95,   27,   93,  287,  436,
 /*   500 */    26,  285,   12,  400,    7,   11,  376,   10,   31,   88,
 /*   510 */   352,   30,  150,  370,  367,  225,  274,   86,  148,  364,
 /*   520 */    85,   84,  363,  147,  264,  257,  345,  361,  358,   29,
 /*   530 */     9,  346,   81,  344,  253,  343,  250,  342,    2,  141,
 /*   540 */   262,  252,    4,  249,  524,  523,  504,  135,  518,  503,
 /*   550 */   483,  134,  516,  336,  133,  132,  326,  509,  324,  176,
 /*   560 */   177,  323,  322,  321,  320,  319,  175,  114,  318,  173,
 /*   570 */   211,  174,  210,   98,  172,  299,  171,  420,  471,  290,
 /*   580 */    97,   45,  161,  158,  156,  268,  153,   69,  160,  276,
 /*   590 */   269,  829,  238,  159,  157,  217,  372,  270,   83,   78,
 /*   600 */   261,  829,  829,  271,  829,  829,  273,  829,  829,  829,
 /*   610 */   829,  829,  829,  275,  829,  277,  368,  829,  155,  281,
 /*   620 */   154,  279,  189,   63,  527,  152,  162,   72,  406,  829,
 /*   630 */   188,  522,  521,  520,  829,  519,  829,  186,  223,  184,
 /*   640 */   267,  515,  829,  514,  513,  829,  829,  512,  375,  829,
 /*   650 */   829,  371,  829,  829,  829,  829,  829,  829,  829,  829,
 /*   660 */   829,  829,  829,  829,  829,  407,  263,  325,  829,  829,
 /*   670 */   356,  829,  829,  829,  829,  316,  829,  829,  829,  829,
 /*   680 */   829,  829,  829,  829,  829,  829,  829,  829,  829,  829,
 /*   690 */    44,  829,  511,  510,  181,  248,  508,  507,  122,  121,
 /*   700 */   506,  829,  119,  505,  191,   43,   36,  829,  829,   39,
 /*   710 */   472,  170,  462,  461,  109,  460,  308,  169,  448,  447,
 /*   720 */   103,  829,  829,  829,  829,  829,  446,  301,  444,  300,
 /*   730 */    38,  190,   42,  291,  419,  165,  164,  829,  411,  163,
 /*   740 */   272,  149,  145,  359,  355,  354,  353,  351,  143,  256,
 /*   750 */   829,  829,  829,  829,  829,  829,  829,  829,  829,  829,
 /*   760 */   829,  829,  829,  829,  829,  829,  829,  829,  829,  829,
 /*   770 */   829,  829,  829,  829,  829,  829,  829,  829,  829,  829,
 /*   780 */   829,  829,  829,  829,  829,  829,  829,  232,  405,  404,
 /*   790 */   231,  403,  402,  829,  829,  829,  829,  829,  829,  829,
 /*   800 */   829,  829,  829,  449,  445,  414,  829,  829,  829,  829,
 /*   810 */   829,  829,  829,  829,  829,  829,  829,  829,  829,  829,
 /*   820 */   829,  829,  829,  196,  206,  192,  207,  209,  208,  202,
 /*   830 */   201,  194,  200,  198,  197,  214,  213,  382,  381,  380,
 /*   840 */   199,  193,   22,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,   76,   77,   37,   38,   39,    9,   62,
 /*    10 */    63,   64,   13,   14,  105,   16,   17,  108,    0,   20,
 /*    20 */    21,   59,   23,   24,   25,   26,   27,   28,   65,   66,
 /*    30 */    67,   60,   33,   34,  190,  191,   37,   38,   39,   13,
 /*    40 */    14,   37,   16,   17,  198,   88,   20,   21,  202,   23,
 /*    50 */    24,   25,   26,   27,   28,  193,    5,  105,    7,   33,
 /*    60 */    34,  109,  110,   37,   38,   39,   13,   14,   68,   16,
 /*    70 */    17,   88,  193,   20,   21,  136,   23,   24,   25,   26,
 /*    80 */    27,   28,  238,    5,  145,  146,   33,   34,  131,  106,
 /*    90 */    37,   38,   39,   13,   14,  193,   16,   17,  254,  193,
 /*   100 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  110,
 /*   110 */     9,   33,   34,   33,   34,   88,  237,   37,   38,   39,
 /*   120 */   116,   88,   25,   26,   27,   28,  264,  139,  266,  141,
 /*   130 */    33,   34,  106,  106,   37,   38,   39,  235,  138,  106,
 /*   140 */   140,  239,  142,  143,  199,   88,   45,   46,   47,   48,
 /*   150 */    49,   50,   51,   52,   53,   54,   55,   56,   57,   58,
 /*   160 */    88,   14,   61,   16,   17,  259,  260,   20,   21,  118,
 /*   170 */    23,   24,   25,   26,   27,   28,  231,  232,  233,  234,
 /*   180 */    33,   34,   68,   60,   37,   38,   39,   16,   17,  117,
 /*   190 */   133,   20,   21,   80,   23,   24,   25,   26,   27,   28,
 /*   200 */    37,   38,   39,  193,   33,   34,  200,  201,   37,   38,
 /*   210 */    39,  213,  214,  215,  216,  217,  218,  219,  220,  221,
 /*   220 */   222,  223,  224,  225,  226,  227,    1,    2,  105,    5,
 /*   230 */     5,    7,    7,  193,    9,  126,  127,   89,   90,   91,
 /*   240 */    92,   93,   94,   95,   96,   97,   98,   99,  100,  101,
 /*   250 */   102,  103,  138,   44,  140,   88,  142,  143,   33,   34,
 /*   260 */     1,    2,   37,   88,    5,  105,    7,  107,    9,  139,
 /*   270 */    61,  141,  262,  106,  264,  235,  139,   68,  141,  239,
 /*   280 */   106,  193,   73,   74,   75,   88,   21,  213,  114,  215,
 /*   290 */   216,  198,   33,   34,  220,  202,  222,  223,  224,  193,
 /*   300 */   226,  227,   89,  106,   91,   92,    5,  257,    7,   96,
 /*   310 */    15,   98,   99,  100,  105,  102,  103,  267,  268,    5,
 /*   320 */     1,    7,  113,  235,   62,   63,   64,  239,  257,   21,
 /*   330 */   105,   69,   70,   71,   72,    5,  111,  112,  267,  268,
 /*   340 */    78,  235,  118,  118,  135,  239,  137,    2,  193,  193,
 /*   350 */     5,  193,    7,  144,    9,   88,   37,  193,  193,   62,
 /*   360 */    63,   64,    0,  193,  105,  193,   69,   70,   71,   72,
 /*   370 */   111,  112,    1,  106,   62,   63,   64,  118,   33,   34,
 /*   380 */     9,   69,   70,   71,   72,  139,  139,  141,  141,  199,
 /*   390 */   235,  235,   88,  235,  239,  239,  139,  239,  141,  235,
 /*   400 */   235,   88,  199,  239,  239,  235,  111,  235,   88,  239,
 /*   410 */   106,  239,   88,  106,   88,    1,  105,  196,  197,  106,
 /*   420 */   241,  114,  232,    9,  113,  198,  106,  211,  212,  202,
 /*   430 */   106,   76,  106,  193,  255,  193,   81,  234,  193,  238,
 /*   440 */   236,   79,  193,  238,  240,  193,  236,  236,    5,  257,
 /*   450 */   240,  240,  192,  193,    5,  254,  111,  112,    5,  254,
 /*   460 */   268,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   470 */     5,  229,    5,    5,    5,   88,  105,    5,  106,  239,
 /*   480 */     5,  141,  141,  141,  141,   76,   16,   16,   15,    5,
 /*   490 */    80,    5,    5,    5,    5,   76,  105,  109,  108,    9,
 /*   500 */   105,  108,  105,  111,  105,  125,  106,  125,   88,  264,
 /*   510 */   258,   88,  105,  264,  106,    1,  105,  105,   88,  106,
 /*   520 */   105,   88,  106,  105,   88,  136,   91,  106,  106,  105,
 /*   530 */   105,   90,  115,    5,    5,    5,    5,    5,  200,   60,
 /*   540 */   114,  147,  196,  147,  194,  194,  212,  195,  194,    5,
 /*   550 */   228,  195,  194,   79,  195,  195,  104,  194,   82,  206,
 /*   560 */   203,   84,   54,   85,   83,   50,  205,   87,   86,  204,
 /*   570 */   194,  207,  194,  199,  208,   79,  209,  228,  210,  194,
 /*   580 */   199,  134,  243,  246,  248,  119,  251,  132,  244,  261,
 /*   590 */   261,  269,  194,  245,  247,  194,  194,  194,  194,  194,
 /*   600 */   194,  269,  269,  120,  269,  269,  121,  269,  269,  269,
 /*   610 */   269,  269,  269,  122,  269,  123,  118,  269,  249,  124,
 /*   620 */   250,  128,  193,  129,  193,  252,  242,  130,  253,  269,
 /*   630 */   193,  193,  193,  193,  269,  193,  269,  193,  261,  193,
 /*   640 */   261,  193,  269,  193,  193,  269,  269,  193,  265,  269,
 /*   650 */   269,  265,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   660 */   269,  269,  269,  269,  269,  238,  238,   81,  269,  269,
 /*   670 */   238,  269,  269,  269,  269,  230,  269,  269,  269,  269,
 /*   680 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   690 */   256,  269,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   700 */   193,  269,  193,  193,  193,  193,  193,  269,  269,  193,
 /*   710 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   720 */   193,  269,  269,  269,  269,  269,  193,  193,  193,  193,
 /*   730 */   193,  193,  193,  193,  193,  193,  193,  269,  193,  193,
 /*   740 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   750 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   760 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   770 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   780 */   269,  269,  269,  269,  269,  269,  269,  230,  230,  230,
 /*   790 */   230,  230,  230,  269,  269,  269,  269,  269,  269,  269,
 /*   800 */   269,  269,  269,  240,  240,  240,  269,  269,  269,  269,
 /*   810 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   820 */   269,  269,  269,  257,  257,  257,  257,  257,  257,  257,
 /*   830 */   257,  257,  257,  257,  257,  257,  257,  257,  257,  257,
 /*   840 */   257,  257,  257,
};
#define YY_SHIFT_USE_DFLT (-92)
#define YY_SHIFT_COUNT (341)
#define YY_SHIFT_MIN   (-91)
#define YY_SHIFT_MAX   (586)
static const short yy_shift_ofst[] = {
 /*     0 */   209,  148,  148,  213,  213,  496,  225,  259,  371,  414,
 /*    10 */   414,  414,  414,  414,  414,  414,  414,  414,  414,  414,
 /*    20 */   414,  414,   -1,  101,  259,  345,  345,  345,  311,  311,
 /*    30 */   414,  414,  414,  362,  414,  414,  355,  496,  586,  586,
 /*    40 */   544,  -92,  -92,  -92,  259,  259,  259,  259,  259,  259,
 /*    50 */   259,  259,  259,  259,  259,  259,  259,  259,  259,  259,
 /*    60 */   259,  259,  259,  259,  345,  345,  345,   78,   78,   78,
 /*    70 */    78,   78,   78,   78,  414,  414,  414,    4,  414,  414,
 /*    80 */   414,  311,  311,  414,  414,  414,  414,  109,  109,   72,
 /*    90 */   311,  414,  414,  414,  414,  414,  414,  414,  414,  414,
 /*   100 */   414,  414,  414,  414,  414,  414,  414,  414,  414,  414,
 /*   110 */   414,  414,  414,  414,  414,  414,  414,  414,  414,  414,
 /*   120 */   414,  414,  414,  414,  414,  414,  414,  414,  414,  414,
 /*   130 */   414,  414,  414,  414,  414,  414,  414,  414,  414,  414,
 /*   140 */   414,  414,  414,  479,  479,  479,  498,  498,  498,  479,
 /*   150 */   498,  479,  497,  455,  494,  495,  493,  492,  491,  485,
 /*   160 */   483,  466,  447,  479,  479,  479,  452,  496,  496,  479,
 /*   170 */   479,  480,  482,  515,  481,  478,  508,  477,  476,  452,
 /*   180 */   544,  479,  474,  474,  479,  474,  479,  474,  479,  479,
 /*   190 */   -92,  -92,   26,   53,   80,   53,   53,  147,  171,   97,
 /*   200 */    97,   97,   97,  262,  312,  297,  -32,  -32,  -32,  -32,
 /*   210 */   114,    0,  -61,  163,  163,  224,   51,  -48,  -37,  -53,
 /*   220 */   326,  307,  174,  324,  320,  313,  304,  267,  319,  123,
 /*   230 */   295,   57,  -43,  197,  167,   33,   27,  -17,  -91,  257,
 /*   240 */   247,  246,  314,  301,  137,  130,  160,  -12,  -73,  532,
 /*   250 */   396,  531,  530,  394,  529,  528,  435,  441,  389,  426,
 /*   260 */   393,  425,  417,  422,  424,  436,  433,  421,  418,  416,
 /*   270 */   430,  415,  413,  412,  514,  411,  408,  407,  423,  382,
 /*   280 */   420,  380,  400,  399,  392,  397,  393,  395,  390,  391,
 /*   290 */   388,  419,  490,  489,  488,  487,  486,  484,  410,  473,
 /*   300 */   409,  471,  343,  342,  387,  387,  387,  387,  470,  341,
 /*   310 */   340,  387,  387,  387,  475,  472,  372,  387,  469,  468,
 /*   320 */   467,  465,  464,  463,  462,  461,  460,  459,  458,  457,
 /*   330 */   456,  453,  449,  443,  330,  175,  113,  308,  265,  -29,
 /*   340 */   -38,   18,
};
#define YY_REDUCE_USE_DFLT (-157)
#define YY_REDUCE_COUNT (191)
#define YY_REDUCE_MIN   (-156)
#define YY_REDUCE_MAX   (585)
static const short yy_reduce_ofst[] = {
 /*     0 */  -156,   -2,   -2,   74,   74,  -55,   71,   50,  -94,  172,
 /*    10 */  -138,   10,  170,  165,  164,  158,  156,  155,  106,   88,
 /*    20 */    40,  -98,  252,  260,  192,  211,  210,  204,  205,  201,
 /*    30 */   249,  245, -121,  203,  242,  240,  227,  190,   93, -154,
 /*    40 */   216,  179,    6,  221,  585,  584,  583,  582,  581,  580,
 /*    50 */   579,  578,  577,  576,  575,  574,  573,  572,  571,  570,
 /*    60 */   569,  568,  567,  566,  565,  564,  563,  562,  561,  560,
 /*    70 */   559,  558,  557,  445,  556,  555,  554,  434,  553,  552,
 /*    80 */   551,  432,  428,  550,  549,  548,  547,  386,  383,  384,
 /*    90 */   427,  546,  545,  543,  542,  541,  540,  539,  538,  537,
 /*   100 */   536,  535,  534,  533,  527,  526,  525,  524,  523,  522,
 /*   110 */   521,  520,  519,  518,  517,  516,  513,  512,  511,  510,
 /*   120 */   509,  507,  506,  505,  504,  503,  502,  501,  500,  499,
 /*   130 */   454,  451,  450,  448,  446,  444,  442,  440,  439,  438,
 /*   140 */   437,  431,  429,  406,  405,  404,  379,  377,  329,  403,
 /*   150 */   328,  402,  375,  373,  335,  370,  369,  336,  347,  337,
 /*   160 */   348,  344,  339,  401,  398,  385,  349,  381,  374,  378,
 /*   170 */   376,  368,  367,  366,  365,  364,  361,  353,  357,  322,
 /*   180 */   334,  363,  360,  359,  358,  356,  354,  352,  351,  350,
 /*   190 */   338,  346,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   827,  656,  599,  668,  587,  596,  805,  805,  827,  827,
 /*    10 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    20 */   827,  827,  716,  559,  805,  827,  827,  827,  827,  827,
 /*    30 */   827,  827,  827,  596,  827,  827,  602,  596,  602,  602,
 /*    40 */   827,  711,  640,  658,  827,  827,  827,  827,  827,  827,
 /*    50 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    60 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    70 */   827,  827,  827,  827,  827,  827,  827,  718,  724,  721,
 /*    80 */   827,  827,  827,  726,  827,  827,  827,  748,  748,  709,
 /*    90 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   100 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   110 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  585,
 /*   120 */   827,  583,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   130 */   827,  827,  827,  827,  827,  827,  570,  827,  827,  827,
 /*   140 */   827,  827,  827,  561,  561,  561,  827,  827,  827,  561,
 /*   150 */   827,  561,  755,  759,  753,  741,  749,  740,  736,  734,
 /*   160 */   732,  731,  763,  561,  561,  561,  600,  596,  596,  561,
 /*   170 */   561,  618,  616,  614,  606,  612,  608,  610,  604,  588,
 /*   180 */   827,  561,  594,  594,  561,  594,  561,  594,  561,  561,
 /*   190 */   640,  658,  827,  764,  827,  804,  754,  794,  793,  800,
 /*   200 */   792,  791,  790,  827,  827,  827,  786,  787,  789,  788,
 /*   210 */   827,  827,  827,  796,  795,  827,  827,  827,  827,  827,
 /*   220 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  766,
 /*   230 */   827,  760,  756,  827,  827,  827,  827,  827,  827,  827,
 /*   240 */   827,  827,  827,  827,  827,  827,  670,  827,  827,  827,
 /*   250 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  708,
 /*   260 */   827,  827,  827,  827,  827,  720,  719,  827,  827,  827,
 /*   270 */   827,  827,  827,  827,  827,  827,  827,  827,  750,  827,
 /*   280 */   742,  827,  827,  827,  827,  827,  682,  827,  827,  827,
 /*   290 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   300 */   827,  827,  827,  827,  823,  818,  819,  816,  827,  827,
 /*   310 */   827,  815,  810,  811,  827,  827,  827,  808,  827,  827,
 /*   320 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   330 */   827,  827,  827,  827,  827,  624,  827,  568,  566,  827,
 /*   340 */   557,  827,  826,  825,  824,  807,  806,  678,  717,  713,
 /*   350 */   715,  714,  712,  725,  722,  723,  707,  706,  705,  727,
 /*   360 */   710,  730,  729,  733,  735,  738,  737,  739,  728,  752,
 /*   370 */   751,  744,  745,  747,  746,  743,  783,  802,  803,  801,
 /*   380 */   799,  798,  797,  782,  781,  780,  777,  776,  775,  772,
 /*   390 */   778,  774,  771,  779,  773,  770,  769,  768,  767,  765,
 /*   400 */   785,  784,  762,  761,  758,  757,  704,  688,  683,  680,
 /*   410 */   687,  686,  685,  693,  692,  684,  681,  677,  676,  601,
 /*   420 */   657,  655,  654,  653,  652,  651,  650,  649,  648,  647,
 /*   430 */   646,  645,  644,  643,  642,  641,  636,  632,  630,  629,
 /*   440 */   628,  625,  595,  598,  597,  822,  821,  820,  817,  814,
 /*   450 */   703,  702,  701,  700,  699,  698,  697,  696,  695,  694,
 /*   460 */   813,  812,  809,  690,  691,  672,  675,  674,  673,  671,
 /*   470 */   689,  620,  619,  617,  615,  607,  613,  609,  611,  605,
 /*   480 */   603,  590,  589,  669,  639,  667,  666,  665,  664,  663,
 /*   490 */   662,  661,  660,  659,  638,  637,  635,  634,  633,  631,
 /*   500 */   627,  626,  622,  623,  621,  586,  584,  582,  581,  580,
 /*   510 */   579,  578,  577,  576,  575,  574,  573,  593,  572,  571,
 /*   520 */   569,  567,  565,  564,  563,  592,  591,  562,  560,  558,
 /*   530 */   556,  555,  554,  553,  552,  551,  550,  549,  548,  547,
 /*   540 */   546,  545,  544,
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
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*      COMMA => nothing */
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
    0,  /*         AS => nothing */
    1,  /*       NULL => ID */
    1,  /*        NOW => ID */
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
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*     SYNCDB => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*     MODIFY => nothing */
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
  "PASS",          "PRIVILEGE",     "LOCAL",         "IF",          
  "EXISTS",        "PPS",           "TSERIES",       "DBS",         
  "STORAGE",       "QTIME",         "CONNS",         "STATE",       
  "COMMA",         "KEEP",          "CACHE",         "REPLICA",     
  "QUORUM",        "DAYS",          "MINROWS",       "MAXROWS",     
  "BLOCKS",        "CTIME",         "WAL",           "FSYNC",       
  "COMP",          "PRECISION",     "UPDATE",        "CACHELAST",   
  "PARTITIONS",    "LP",            "RP",            "UNSIGNED",    
  "TAGS",          "USING",         "AS",            "NULL",        
  "NOW",           "SELECT",        "UNION",         "ALL",         
  "DISTINCT",      "FROM",          "VARIABLE",      "INTERVAL",    
  "SESSION",       "STATE_WINDOW",  "FILL",          "SLIDING",     
  "ORDER",         "BY",            "ASC",           "DESC",        
  "GROUP",         "HAVING",        "LIMIT",         "OFFSET",      
  "SLIMIT",        "SOFFSET",       "WHERE",         "RESET",       
  "QUERY",         "SYNCDB",        "ADD",           "COLUMN",      
  "MODIFY",        "TAG",           "CHANGE",        "SET",         
  "KILL",          "CONNECTION",    "STREAM",        "COLON",       
  "ABORT",         "AFTER",         "ATTACH",        "BEFORE",      
  "BEGIN",         "CASCADE",       "CLUSTER",       "CONFLICT",    
  "COPY",          "DEFERRED",      "DELIMITERS",    "DETACH",      
  "EACH",          "END",           "EXPLAIN",       "FAIL",        
  "FOR",           "IGNORE",        "IMMEDIATE",     "INITIALLY",   
  "INSTEAD",       "MATCH",         "KEY",           "OF",          
  "RAISE",         "REPLACE",       "RESTRICT",      "ROW",         
  "STATEMENT",     "TRIGGER",       "VIEW",          "SEMI",        
  "NONE",          "PREV",          "LINEAR",        "IMPORT",      
  "TBNAME",        "JOIN",          "INSERT",        "INTO",        
  "VALUES",        "error",         "program",       "cmd",         
  "dbPrefix",      "ids",           "cpxName",       "ifexists",    
  "alter_db_optr",  "alter_topic_optr",  "acct_optr",     "ifnotexists", 
  "db_optr",       "topic_optr",    "pps",           "tseries",     
  "dbs",           "streams",       "storage",       "qtime",       
  "users",         "conns",         "state",         "intitemlist", 
  "intitem",       "keep",          "cache",         "replica",     
  "quorum",        "days",          "minrows",       "maxrows",     
  "blocks",        "ctime",         "wal",           "fsync",       
  "comp",          "prec",          "update",        "cachelast",   
  "partitions",    "typename",      "signed",        "create_table_args",
  "create_stable_args",  "create_table_list",  "create_from_stable",  "columnlist",  
  "tagitemlist",   "tagNamelist",   "select",        "column",      
  "tagitem",       "selcollist",    "from",          "where_opt",   
  "interval_opt",  "session_option",  "windowstate_option",  "fill_opt",    
  "sliding_opt",   "groupby_opt",   "orderby_opt",   "having_opt",  
  "slimit_opt",    "limit_opt",     "union",         "sclp",        
  "distinct",      "expr",          "as",            "tablelist",   
  "sub",           "tmvar",         "sortlist",      "sortitem",    
  "item",          "sortorder",     "grouplist",     "exprlist",    
  "expritem",    
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
 /*  78 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  79 */ "intitemlist ::= intitem",
 /*  80 */ "intitem ::= INTEGER",
 /*  81 */ "keep ::= KEEP intitemlist",
 /*  82 */ "cache ::= CACHE INTEGER",
 /*  83 */ "replica ::= REPLICA INTEGER",
 /*  84 */ "quorum ::= QUORUM INTEGER",
 /*  85 */ "days ::= DAYS INTEGER",
 /*  86 */ "minrows ::= MINROWS INTEGER",
 /*  87 */ "maxrows ::= MAXROWS INTEGER",
 /*  88 */ "blocks ::= BLOCKS INTEGER",
 /*  89 */ "ctime ::= CTIME INTEGER",
 /*  90 */ "wal ::= WAL INTEGER",
 /*  91 */ "fsync ::= FSYNC INTEGER",
 /*  92 */ "comp ::= COMP INTEGER",
 /*  93 */ "prec ::= PRECISION STRING",
 /*  94 */ "update ::= UPDATE INTEGER",
 /*  95 */ "cachelast ::= CACHELAST INTEGER",
 /*  96 */ "partitions ::= PARTITIONS INTEGER",
 /*  97 */ "db_optr ::=",
 /*  98 */ "db_optr ::= db_optr cache",
 /*  99 */ "db_optr ::= db_optr replica",
 /* 100 */ "db_optr ::= db_optr quorum",
 /* 101 */ "db_optr ::= db_optr days",
 /* 102 */ "db_optr ::= db_optr minrows",
 /* 103 */ "db_optr ::= db_optr maxrows",
 /* 104 */ "db_optr ::= db_optr blocks",
 /* 105 */ "db_optr ::= db_optr ctime",
 /* 106 */ "db_optr ::= db_optr wal",
 /* 107 */ "db_optr ::= db_optr fsync",
 /* 108 */ "db_optr ::= db_optr comp",
 /* 109 */ "db_optr ::= db_optr prec",
 /* 110 */ "db_optr ::= db_optr keep",
 /* 111 */ "db_optr ::= db_optr update",
 /* 112 */ "db_optr ::= db_optr cachelast",
 /* 113 */ "topic_optr ::= db_optr",
 /* 114 */ "topic_optr ::= topic_optr partitions",
 /* 115 */ "alter_db_optr ::=",
 /* 116 */ "alter_db_optr ::= alter_db_optr replica",
 /* 117 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 118 */ "alter_db_optr ::= alter_db_optr keep",
 /* 119 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 120 */ "alter_db_optr ::= alter_db_optr comp",
 /* 121 */ "alter_db_optr ::= alter_db_optr wal",
 /* 122 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 123 */ "alter_db_optr ::= alter_db_optr update",
 /* 124 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 125 */ "alter_topic_optr ::= alter_db_optr",
 /* 126 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 127 */ "typename ::= ids",
 /* 128 */ "typename ::= ids LP signed RP",
 /* 129 */ "typename ::= ids UNSIGNED",
 /* 130 */ "signed ::= INTEGER",
 /* 131 */ "signed ::= PLUS INTEGER",
 /* 132 */ "signed ::= MINUS INTEGER",
 /* 133 */ "cmd ::= CREATE TABLE create_table_args",
 /* 134 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE TABLE create_table_list",
 /* 137 */ "create_table_list ::= create_from_stable",
 /* 138 */ "create_table_list ::= create_table_list create_from_stable",
 /* 139 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 140 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 143 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 144 */ "tagNamelist ::= ids",
 /* 145 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 146 */ "columnlist ::= columnlist COMMA column",
 /* 147 */ "columnlist ::= column",
 /* 148 */ "column ::= ids typename",
 /* 149 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 150 */ "tagitemlist ::= tagitem",
 /* 151 */ "tagitem ::= INTEGER",
 /* 152 */ "tagitem ::= FLOAT",
 /* 153 */ "tagitem ::= STRING",
 /* 154 */ "tagitem ::= BOOL",
 /* 155 */ "tagitem ::= NULL",
 /* 156 */ "tagitem ::= NOW",
 /* 157 */ "tagitem ::= MINUS INTEGER",
 /* 158 */ "tagitem ::= MINUS FLOAT",
 /* 159 */ "tagitem ::= PLUS INTEGER",
 /* 160 */ "tagitem ::= PLUS FLOAT",
 /* 161 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 162 */ "select ::= LP select RP",
 /* 163 */ "union ::= select",
 /* 164 */ "union ::= union UNION ALL select",
 /* 165 */ "cmd ::= union",
 /* 166 */ "select ::= SELECT selcollist",
 /* 167 */ "sclp ::= selcollist COMMA",
 /* 168 */ "sclp ::=",
 /* 169 */ "selcollist ::= sclp distinct expr as",
 /* 170 */ "selcollist ::= sclp STAR",
 /* 171 */ "as ::= AS ids",
 /* 172 */ "as ::= ids",
 /* 173 */ "as ::=",
 /* 174 */ "distinct ::= DISTINCT",
 /* 175 */ "distinct ::=",
 /* 176 */ "from ::= FROM tablelist",
 /* 177 */ "from ::= FROM sub",
 /* 178 */ "sub ::= LP union RP",
 /* 179 */ "sub ::= LP union RP ids",
 /* 180 */ "sub ::= sub COMMA LP union RP ids",
 /* 181 */ "tablelist ::= ids cpxName",
 /* 182 */ "tablelist ::= ids cpxName ids",
 /* 183 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 184 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 185 */ "tmvar ::= VARIABLE",
 /* 186 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 187 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 188 */ "interval_opt ::=",
 /* 189 */ "session_option ::=",
 /* 190 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 191 */ "windowstate_option ::=",
 /* 192 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 193 */ "fill_opt ::=",
 /* 194 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 195 */ "fill_opt ::= FILL LP ID RP",
 /* 196 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 197 */ "sliding_opt ::=",
 /* 198 */ "orderby_opt ::=",
 /* 199 */ "orderby_opt ::= ORDER BY sortlist",
 /* 200 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 201 */ "sortlist ::= item sortorder",
 /* 202 */ "item ::= ids cpxName",
 /* 203 */ "sortorder ::= ASC",
 /* 204 */ "sortorder ::= DESC",
 /* 205 */ "sortorder ::=",
 /* 206 */ "groupby_opt ::=",
 /* 207 */ "groupby_opt ::= GROUP BY grouplist",
 /* 208 */ "grouplist ::= grouplist COMMA item",
 /* 209 */ "grouplist ::= item",
 /* 210 */ "having_opt ::=",
 /* 211 */ "having_opt ::= HAVING expr",
 /* 212 */ "limit_opt ::=",
 /* 213 */ "limit_opt ::= LIMIT signed",
 /* 214 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 215 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 216 */ "slimit_opt ::=",
 /* 217 */ "slimit_opt ::= SLIMIT signed",
 /* 218 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 219 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 220 */ "where_opt ::=",
 /* 221 */ "where_opt ::= WHERE expr",
 /* 222 */ "expr ::= LP expr RP",
 /* 223 */ "expr ::= ID",
 /* 224 */ "expr ::= ID DOT ID",
 /* 225 */ "expr ::= ID DOT STAR",
 /* 226 */ "expr ::= INTEGER",
 /* 227 */ "expr ::= MINUS INTEGER",
 /* 228 */ "expr ::= PLUS INTEGER",
 /* 229 */ "expr ::= FLOAT",
 /* 230 */ "expr ::= MINUS FLOAT",
 /* 231 */ "expr ::= PLUS FLOAT",
 /* 232 */ "expr ::= STRING",
 /* 233 */ "expr ::= NOW",
 /* 234 */ "expr ::= VARIABLE",
 /* 235 */ "expr ::= PLUS VARIABLE",
 /* 236 */ "expr ::= MINUS VARIABLE",
 /* 237 */ "expr ::= BOOL",
 /* 238 */ "expr ::= NULL",
 /* 239 */ "expr ::= ID LP exprlist RP",
 /* 240 */ "expr ::= ID LP STAR RP",
 /* 241 */ "expr ::= expr IS NULL",
 /* 242 */ "expr ::= expr IS NOT NULL",
 /* 243 */ "expr ::= expr LT expr",
 /* 244 */ "expr ::= expr GT expr",
 /* 245 */ "expr ::= expr LE expr",
 /* 246 */ "expr ::= expr GE expr",
 /* 247 */ "expr ::= expr NE expr",
 /* 248 */ "expr ::= expr EQ expr",
 /* 249 */ "expr ::= expr BETWEEN expr AND expr",
 /* 250 */ "expr ::= expr AND expr",
 /* 251 */ "expr ::= expr OR expr",
 /* 252 */ "expr ::= expr PLUS expr",
 /* 253 */ "expr ::= expr MINUS expr",
 /* 254 */ "expr ::= expr STAR expr",
 /* 255 */ "expr ::= expr SLASH expr",
 /* 256 */ "expr ::= expr REM expr",
 /* 257 */ "expr ::= expr LIKE expr",
 /* 258 */ "expr ::= expr IN LP exprlist RP",
 /* 259 */ "exprlist ::= exprlist COMMA expritem",
 /* 260 */ "exprlist ::= expritem",
 /* 261 */ "expritem ::= expr",
 /* 262 */ "expritem ::=",
 /* 263 */ "cmd ::= RESET QUERY CACHE",
 /* 264 */ "cmd ::= SYNCDB ids REPLICA",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 281 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 282 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 283 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 211: /* intitemlist */
    case 213: /* keep */
    case 235: /* columnlist */
    case 236: /* tagitemlist */
    case 237: /* tagNamelist */
    case 247: /* fill_opt */
    case 249: /* groupby_opt */
    case 250: /* orderby_opt */
    case 262: /* sortlist */
    case 266: /* grouplist */
{
taosArrayDestroy((yypminor->yy93));
}
      break;
    case 233: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy532));
}
      break;
    case 238: /* select */
{
destroySqlNode((yypminor->yy224));
}
      break;
    case 241: /* selcollist */
    case 255: /* sclp */
    case 267: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy93));
}
      break;
    case 242: /* from */
    case 259: /* tablelist */
    case 260: /* sub */
{
destroyRelationInfo((yypminor->yy330));
}
      break;
    case 243: /* where_opt */
    case 251: /* having_opt */
    case 257: /* expr */
    case 268: /* expritem */
{
tSqlExprDestroy((yypminor->yy68));
}
      break;
    case 254: /* union */
{
destroyAllSqlNode((yypminor->yy93));
}
      break;
    case 263: /* sortitem */
{
tVariantDestroy(&(yypminor->yy518));
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
  { 190, 1 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 2 },
  { 191, 3 },
  { 192, 0 },
  { 192, 2 },
  { 194, 0 },
  { 194, 2 },
  { 191, 5 },
  { 191, 5 },
  { 191, 4 },
  { 191, 3 },
  { 191, 5 },
  { 191, 3 },
  { 191, 5 },
  { 191, 3 },
  { 191, 4 },
  { 191, 5 },
  { 191, 5 },
  { 191, 4 },
  { 191, 4 },
  { 191, 3 },
  { 191, 3 },
  { 191, 3 },
  { 191, 2 },
  { 191, 3 },
  { 191, 5 },
  { 191, 5 },
  { 191, 4 },
  { 191, 5 },
  { 191, 3 },
  { 191, 4 },
  { 191, 4 },
  { 191, 4 },
  { 191, 4 },
  { 191, 6 },
  { 193, 1 },
  { 193, 1 },
  { 195, 2 },
  { 195, 0 },
  { 199, 3 },
  { 199, 0 },
  { 191, 3 },
  { 191, 6 },
  { 191, 5 },
  { 191, 5 },
  { 191, 5 },
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
  { 198, 9 },
  { 211, 3 },
  { 211, 1 },
  { 212, 1 },
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
  { 228, 2 },
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
  { 196, 0 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 196, 2 },
  { 197, 1 },
  { 197, 2 },
  { 229, 1 },
  { 229, 4 },
  { 229, 2 },
  { 230, 1 },
  { 230, 2 },
  { 230, 2 },
  { 191, 3 },
  { 191, 3 },
  { 191, 3 },
  { 191, 3 },
  { 233, 1 },
  { 233, 2 },
  { 231, 6 },
  { 232, 10 },
  { 234, 10 },
  { 234, 13 },
  { 237, 3 },
  { 237, 1 },
  { 231, 5 },
  { 235, 3 },
  { 235, 1 },
  { 239, 2 },
  { 236, 3 },
  { 236, 1 },
  { 240, 1 },
  { 240, 1 },
  { 240, 1 },
  { 240, 1 },
  { 240, 1 },
  { 240, 1 },
  { 240, 2 },
  { 240, 2 },
  { 240, 2 },
  { 240, 2 },
  { 238, 14 },
  { 238, 3 },
  { 254, 1 },
  { 254, 4 },
  { 191, 1 },
  { 238, 2 },
  { 255, 2 },
  { 255, 0 },
  { 241, 4 },
  { 241, 2 },
  { 258, 2 },
  { 258, 1 },
  { 258, 0 },
  { 256, 1 },
  { 256, 0 },
  { 242, 2 },
  { 242, 2 },
  { 260, 3 },
  { 260, 4 },
  { 260, 6 },
  { 259, 2 },
  { 259, 3 },
  { 259, 4 },
  { 259, 5 },
  { 261, 1 },
  { 244, 4 },
  { 244, 6 },
  { 244, 0 },
  { 245, 0 },
  { 245, 7 },
  { 246, 0 },
  { 246, 4 },
  { 247, 0 },
  { 247, 6 },
  { 247, 4 },
  { 248, 4 },
  { 248, 0 },
  { 250, 0 },
  { 250, 3 },
  { 262, 4 },
  { 262, 2 },
  { 264, 2 },
  { 265, 1 },
  { 265, 1 },
  { 265, 0 },
  { 249, 0 },
  { 249, 3 },
  { 266, 3 },
  { 266, 1 },
  { 251, 0 },
  { 251, 2 },
  { 253, 0 },
  { 253, 2 },
  { 253, 4 },
  { 253, 4 },
  { 252, 0 },
  { 252, 2 },
  { 252, 4 },
  { 252, 4 },
  { 243, 0 },
  { 243, 2 },
  { 257, 3 },
  { 257, 1 },
  { 257, 3 },
  { 257, 3 },
  { 257, 1 },
  { 257, 2 },
  { 257, 2 },
  { 257, 1 },
  { 257, 2 },
  { 257, 2 },
  { 257, 1 },
  { 257, 1 },
  { 257, 1 },
  { 257, 2 },
  { 257, 2 },
  { 257, 1 },
  { 257, 1 },
  { 257, 4 },
  { 257, 4 },
  { 257, 3 },
  { 257, 4 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 5 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 3 },
  { 257, 5 },
  { 267, 3 },
  { 267, 1 },
  { 268, 1 },
  { 268, 0 },
  { 191, 3 },
  { 191, 3 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 8 },
  { 191, 9 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 7 },
  { 191, 8 },
  { 191, 9 },
  { 191, 7 },
  { 191, 3 },
  { 191, 5 },
  { 191, 5 },
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy372, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy77);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy77);}
        break;
      case 48: /* ids ::= ID */
      case 49: /* ids ::= STRING */ yytestcase(yyruleno==49);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 50: /* ifexists ::= IF EXISTS */
      case 52: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==52);
{ yygotominor.yy0.n = 1;}
        break;
      case 51: /* ifexists ::= */
      case 53: /* ifnotexists ::= */ yytestcase(yyruleno==53);
      case 175: /* distinct ::= */ yytestcase(yyruleno==175);
{ yygotominor.yy0.n = 0;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy77);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy372, &yymsp[-2].minor.yy0);}
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
{ yygotominor.yy0.n = 0;   }
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
{ yygotominor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 77: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yygotominor.yy77.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy77.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy77.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy77.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy77.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy77.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy77.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy77.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy77.stat    = yymsp[0].minor.yy0;
}
        break;
      case 78: /* intitemlist ::= intitemlist COMMA intitem */
      case 149: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==149);
{ yygotominor.yy93 = tVariantListAppend(yymsp[-2].minor.yy93, &yymsp[0].minor.yy518, -1);    }
        break;
      case 79: /* intitemlist ::= intitem */
      case 150: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==150);
{ yygotominor.yy93 = tVariantListAppend(NULL, &yymsp[0].minor.yy518, -1); }
        break;
      case 80: /* intitem ::= INTEGER */
      case 151: /* tagitem ::= INTEGER */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= FLOAT */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= STRING */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= BOOL */ yytestcase(yyruleno==154);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy518, &yymsp[0].minor.yy0); }
        break;
      case 81: /* keep ::= KEEP intitemlist */
{ yygotominor.yy93 = yymsp[0].minor.yy93; }
        break;
      case 82: /* cache ::= CACHE INTEGER */
      case 83: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==83);
      case 84: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==84);
      case 85: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==89);
      case 90: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==90);
      case 91: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==91);
      case 92: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==92);
      case 93: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==93);
      case 94: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==94);
      case 95: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==95);
      case 96: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==96);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 97: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy372); yygotominor.yy372.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 98: /* db_optr ::= db_optr cache */
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* db_optr ::= db_optr replica */
      case 116: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==116);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 100: /* db_optr ::= db_optr quorum */
      case 117: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==117);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 101: /* db_optr ::= db_optr days */
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 102: /* db_optr ::= db_optr minrows */
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 103: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 104: /* db_optr ::= db_optr blocks */
      case 119: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==119);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 105: /* db_optr ::= db_optr ctime */
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* db_optr ::= db_optr wal */
      case 121: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==121);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 107: /* db_optr ::= db_optr fsync */
      case 122: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==122);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 108: /* db_optr ::= db_optr comp */
      case 120: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==120);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 109: /* db_optr ::= db_optr prec */
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.precision = yymsp[0].minor.yy0; }
        break;
      case 110: /* db_optr ::= db_optr keep */
      case 118: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==118);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.keep = yymsp[0].minor.yy93; }
        break;
      case 111: /* db_optr ::= db_optr update */
      case 123: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==123);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 112: /* db_optr ::= db_optr cachelast */
      case 124: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==124);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 113: /* topic_optr ::= db_optr */
      case 125: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==125);
{ yygotominor.yy372 = yymsp[0].minor.yy372; yygotominor.yy372.dbType = TSDB_DB_TYPE_TOPIC; }
        break;
      case 114: /* topic_optr ::= topic_optr partitions */
      case 126: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==126);
{ yygotominor.yy372 = yymsp[-1].minor.yy372; yygotominor.yy372.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 115: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy372); yygotominor.yy372.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 127: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yygotominor.yy325, &yymsp[0].minor.yy0);
}
        break;
      case 128: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy279 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yygotominor.yy325, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy279;  // negative value of name length
    tSetColumnType(&yygotominor.yy325, &yymsp[-3].minor.yy0);
  }
}
        break;
      case 129: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yygotominor.yy325, &yymsp[-1].minor.yy0);
}
        break;
      case 130: /* signed ::= INTEGER */
      case 131: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==131);
{ yygotominor.yy279 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 132: /* signed ::= MINUS INTEGER */
      case 133: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==135);
{ yygotominor.yy279 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 136: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy532;}
        break;
      case 137: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy528);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yygotominor.yy532 = pCreateTable;
}
        break;
      case 138: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy532->childTableInfo, &yymsp[0].minor.yy528);
  yygotominor.yy532 = yymsp[-1].minor.yy532;
}
        break;
      case 139: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yygotominor.yy532 = tSetCreateTableInfo(yymsp[-1].minor.yy93, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yygotominor.yy532, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
        break;
      case 140: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yygotominor.yy532 = tSetCreateTableInfo(yymsp[-5].minor.yy93, yymsp[-1].minor.yy93, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yygotominor.yy532, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yygotominor.yy528 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy93, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yygotominor.yy528 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy93, yymsp[-1].minor.yy93, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
        break;
      case 143: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy93, &yymsp[0].minor.yy0); yygotominor.yy93 = yymsp[-2].minor.yy93;  }
        break;
      case 144: /* tagNamelist ::= ids */
{yygotominor.yy93 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yygotominor.yy93, &yymsp[0].minor.yy0);}
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yygotominor.yy532 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy224, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yygotominor.yy532, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
        break;
      case 146: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy93, &yymsp[0].minor.yy325); yygotominor.yy93 = yymsp[-2].minor.yy93;  }
        break;
      case 147: /* columnlist ::= column */
{yygotominor.yy93 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yygotominor.yy93, &yymsp[0].minor.yy325);}
        break;
      case 148: /* column ::= ids typename */
{
  tSetColumnInfo(&yygotominor.yy325, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy325);
}
        break;
      case 155: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy518, &yymsp[0].minor.yy0); }
        break;
      case 156: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yygotominor.yy518, &yymsp[0].minor.yy0);}
        break;
      case 157: /* tagitem ::= MINUS INTEGER */
      case 158: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==160);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy518, &yymsp[-1].minor.yy0);
}
        break;
      case 161: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy224 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy93, yymsp[-11].minor.yy330, yymsp[-10].minor.yy68, yymsp[-4].minor.yy93, yymsp[-3].minor.yy93, &yymsp[-9].minor.yy42, &yymsp[-8].minor.yy15, &yymsp[-7].minor.yy274, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy93, &yymsp[0].minor.yy284, &yymsp[-1].minor.yy284, yymsp[-2].minor.yy68);
}
        break;
      case 162: /* select ::= LP select RP */
{yygotominor.yy224 = yymsp[-1].minor.yy224;}
        break;
      case 163: /* union ::= select */
{ yygotominor.yy93 = setSubclause(NULL, yymsp[0].minor.yy224); }
        break;
      case 164: /* union ::= union UNION ALL select */
{ yygotominor.yy93 = appendSelectClause(yymsp[-3].minor.yy93, yymsp[0].minor.yy224); }
        break;
      case 165: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy93, NULL, TSDB_SQL_SELECT); }
        break;
      case 166: /* select ::= SELECT selcollist */
{
  yygotominor.yy224 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy93, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 167: /* sclp ::= selcollist COMMA */
{yygotominor.yy93 = yymsp[-1].minor.yy93;}
        break;
      case 168: /* sclp ::= */
      case 198: /* orderby_opt ::= */ yytestcase(yyruleno==198);
{yygotominor.yy93 = 0;}
        break;
      case 169: /* selcollist ::= sclp distinct expr as */
{
   yygotominor.yy93 = tSqlExprListAppend(yymsp[-3].minor.yy93, yymsp[-1].minor.yy68,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 170: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yygotominor.yy93 = tSqlExprListAppend(yymsp[-1].minor.yy93, pNode, 0, 0);
}
        break;
      case 171: /* as ::= AS ids */
      case 172: /* as ::= ids */ yytestcase(yyruleno==172);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 173: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 174: /* distinct ::= DISTINCT */
{ yygotominor.yy0 = yymsp[0].minor.yy0;  }
        break;
      case 176: /* from ::= FROM tablelist */
      case 177: /* from ::= FROM sub */ yytestcase(yyruleno==177);
{yygotominor.yy330 = yymsp[0].minor.yy330;}
        break;
      case 178: /* sub ::= LP union RP */
{yygotominor.yy330 = addSubqueryElem(NULL, yymsp[-1].minor.yy93, NULL);}
        break;
      case 179: /* sub ::= LP union RP ids */
{yygotominor.yy330 = addSubqueryElem(NULL, yymsp[-2].minor.yy93, &yymsp[0].minor.yy0);}
        break;
      case 180: /* sub ::= sub COMMA LP union RP ids */
{yygotominor.yy330 = addSubqueryElem(yymsp[-5].minor.yy330, yymsp[-2].minor.yy93, &yymsp[0].minor.yy0);}
        break;
      case 181: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy330 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 182: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy330 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 183: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy330 = setTableNameList(yymsp[-3].minor.yy330, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 184: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy330 = setTableNameList(yymsp[-4].minor.yy330, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 185: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 186: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy42.interval = yymsp[-1].minor.yy0; yygotominor.yy42.offset.n = 0;}
        break;
      case 187: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy42.interval = yymsp[-3].minor.yy0; yygotominor.yy42.offset = yymsp[-1].minor.yy0;}
        break;
      case 188: /* interval_opt ::= */
{memset(&yygotominor.yy42, 0, sizeof(yygotominor.yy42));}
        break;
      case 189: /* session_option ::= */
{yygotominor.yy15.col.n = 0; yygotominor.yy15.gap.n = 0;}
        break;
      case 190: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yygotominor.yy15.col = yymsp[-4].minor.yy0;
   yygotominor.yy15.gap = yymsp[-1].minor.yy0;
}
        break;
      case 191: /* windowstate_option ::= */
{ yygotominor.yy274.col.n = 0; yygotominor.yy274.col.z = NULL;}
        break;
      case 192: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yygotominor.yy274.col = yymsp[-1].minor.yy0; }
        break;
      case 193: /* fill_opt ::= */
{ yygotominor.yy93 = 0;     }
        break;
      case 194: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy93, &A, -1, 0);
    yygotominor.yy93 = yymsp[-1].minor.yy93;
}
        break;
      case 195: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy93 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 196: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 197: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 199: /* orderby_opt ::= ORDER BY sortlist */
{yygotominor.yy93 = yymsp[0].minor.yy93;}
        break;
      case 200: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy93 = tVariantListAppend(yymsp[-3].minor.yy93, &yymsp[-1].minor.yy518, yymsp[0].minor.yy150);
}
        break;
      case 201: /* sortlist ::= item sortorder */
{
  yygotominor.yy93 = tVariantListAppend(NULL, &yymsp[-1].minor.yy518, yymsp[0].minor.yy150);
}
        break;
      case 202: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy518, &yymsp[-1].minor.yy0);
}
        break;
      case 203: /* sortorder ::= ASC */
      case 205: /* sortorder ::= */ yytestcase(yyruleno==205);
{ yygotominor.yy150 = TSDB_ORDER_ASC; }
        break;
      case 204: /* sortorder ::= DESC */
{ yygotominor.yy150 = TSDB_ORDER_DESC;}
        break;
      case 206: /* groupby_opt ::= */
{ yygotominor.yy93 = 0;}
        break;
      case 207: /* groupby_opt ::= GROUP BY grouplist */
{ yygotominor.yy93 = yymsp[0].minor.yy93;}
        break;
      case 208: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy93 = tVariantListAppend(yymsp[-2].minor.yy93, &yymsp[0].minor.yy518, -1);
}
        break;
      case 209: /* grouplist ::= item */
{
  yygotominor.yy93 = tVariantListAppend(NULL, &yymsp[0].minor.yy518, -1);
}
        break;
      case 210: /* having_opt ::= */
      case 220: /* where_opt ::= */ yytestcase(yyruleno==220);
      case 262: /* expritem ::= */ yytestcase(yyruleno==262);
{yygotominor.yy68 = 0;}
        break;
      case 211: /* having_opt ::= HAVING expr */
      case 221: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==221);
      case 261: /* expritem ::= expr */ yytestcase(yyruleno==261);
{yygotominor.yy68 = yymsp[0].minor.yy68;}
        break;
      case 212: /* limit_opt ::= */
      case 216: /* slimit_opt ::= */ yytestcase(yyruleno==216);
{yygotominor.yy284.limit = -1; yygotominor.yy284.offset = 0;}
        break;
      case 213: /* limit_opt ::= LIMIT signed */
      case 217: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==217);
{yygotominor.yy284.limit = yymsp[0].minor.yy279;  yygotominor.yy284.offset = 0;}
        break;
      case 214: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yygotominor.yy284.limit = yymsp[-2].minor.yy279;  yygotominor.yy284.offset = yymsp[0].minor.yy279;}
        break;
      case 215: /* limit_opt ::= LIMIT signed COMMA signed */
{ yygotominor.yy284.limit = yymsp[0].minor.yy279;  yygotominor.yy284.offset = yymsp[-2].minor.yy279;}
        break;
      case 218: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yygotominor.yy284.limit = yymsp[-2].minor.yy279;  yygotominor.yy284.offset = yymsp[0].minor.yy279;}
        break;
      case 219: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yygotominor.yy284.limit = yymsp[0].minor.yy279;  yygotominor.yy284.offset = yymsp[-2].minor.yy279;}
        break;
      case 222: /* expr ::= LP expr RP */
{yygotominor.yy68 = yymsp[-1].minor.yy68; yygotominor.yy68->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy68->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
        break;
      case 223: /* expr ::= ID */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 224: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 225: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 226: /* expr ::= INTEGER */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 227: /* expr ::= MINUS INTEGER */
      case 228: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 229: /* expr ::= FLOAT */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 230: /* expr ::= MINUS FLOAT */
      case 231: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==231);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 232: /* expr ::= STRING */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 233: /* expr ::= NOW */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 234: /* expr ::= VARIABLE */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 235: /* expr ::= PLUS VARIABLE */
      case 236: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
        break;
      case 237: /* expr ::= BOOL */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 238: /* expr ::= NULL */
{ yygotominor.yy68 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
        break;
      case 239: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy68 = tSqlExprCreateFunction(yymsp[-1].minor.yy93, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 240: /* expr ::= ID LP STAR RP */
{ yygotominor.yy68 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 241: /* expr ::= expr IS NULL */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, NULL, TK_ISNULL);}
        break;
      case 242: /* expr ::= expr IS NOT NULL */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-3].minor.yy68, NULL, TK_NOTNULL);}
        break;
      case 243: /* expr ::= expr LT expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_LT);}
        break;
      case 244: /* expr ::= expr GT expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_GT);}
        break;
      case 245: /* expr ::= expr LE expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_LE);}
        break;
      case 246: /* expr ::= expr GE expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_GE);}
        break;
      case 247: /* expr ::= expr NE expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_NE);}
        break;
      case 248: /* expr ::= expr EQ expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_EQ);}
        break;
      case 249: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy68); yygotominor.yy68 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy68, yymsp[-2].minor.yy68, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy68, TK_LE), TK_AND);}
        break;
      case 250: /* expr ::= expr AND expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_AND);}
        break;
      case 251: /* expr ::= expr OR expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_OR); }
        break;
      case 252: /* expr ::= expr PLUS expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_PLUS);  }
        break;
      case 253: /* expr ::= expr MINUS expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_MINUS); }
        break;
      case 254: /* expr ::= expr STAR expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_STAR);  }
        break;
      case 255: /* expr ::= expr SLASH expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_DIVIDE);}
        break;
      case 256: /* expr ::= expr REM expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_REM);   }
        break;
      case 257: /* expr ::= expr LIKE expr */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-2].minor.yy68, yymsp[0].minor.yy68, TK_LIKE);  }
        break;
      case 258: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy68 = tSqlExprCreate(yymsp[-4].minor.yy68, (tSqlExpr*)yymsp[-1].minor.yy93, TK_IN); }
        break;
      case 259: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy93 = tSqlExprListAppend(yymsp[-2].minor.yy93,yymsp[0].minor.yy68,0, 0);}
        break;
      case 260: /* exprlist ::= expritem */
{yygotominor.yy93 = tSqlExprListAppend(0,yymsp[0].minor.yy68,0, 0);}
        break;
      case 263: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 264: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 271: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy518, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 279: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy518, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy93, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 282: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 283: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
