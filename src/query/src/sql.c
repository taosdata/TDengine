/* Driver template for the LEMON parser generator.
** The author disclaims copyright to this source code.
*/
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */
#include <stdio.h>
#line 23 "sql.y"

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
#line 21 "sql.c"
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
#define YYNSTATE 433
#define YYNRULE 242
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
#define YY_ACTTAB_COUNT (690)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   417,   36,   35,   79,   83,   34,   33,   32,  416,   88,
 /*    10 */    91,   82,   37,   38,   67,   39,   40,   85,  212,  174,
 /*    20 */    31,  676,  258,  211,   43,   41,   45,   42,  255,  254,
 /*    30 */    99,  172,   36,   35,  104,  102,   34,   33,   32,   37,
 /*    40 */    38,  433,   39,   40,    9,  320,  174,   31,   66,  121,
 /*    50 */   211,   43,   41,   45,   42,   34,   33,   32,  144,   36,
 /*    60 */    35,  180,  266,   34,   33,   32,   37,   38,  289,   39,
 /*    70 */    40,  356,    4,  174,   31,  144,  196,  211,   43,   41,
 /*    80 */    45,   42,   65,  419,  168,  290,   36,   35,  417,  307,
 /*    90 */    34,   33,   32,   90,   89,   38,  416,   39,   40,  233,
 /*   100 */   232,  174,   31,   52,   57,  211,   43,   41,   45,   42,
 /*   110 */   310,  321,   22,  318,   36,   35,  138,  136,   34,   33,
 /*   120 */    32,   53,   96,   95,   94,  306,  182,  308,  369,  230,
 /*   130 */   229,  134,  432,  431,  430,  429,  428,  427,  426,  425,
 /*   140 */   424,  423,  422,  421,  257,  101,  325,  186,  337,  336,
 /*   150 */   335,  334,  333,  332,  331,  330,  329,  328,  327,  326,
 /*   160 */   324,  323,  420,   17,  224,  251,  250,  223,  222,  221,
 /*   170 */   249,  220,  248,  247,  246,  219,  245,  244,   39,   40,
 /*   180 */    23,  553,  174,   31,  189,   23,  211,   43,   41,   45,
 /*   190 */    42,  193,  192,   11,   10,   36,   35,  111,  274,   34,
 /*   200 */    33,   32,  173,  296,   19,   13,  305,  111,  300,  392,
 /*   210 */   299,  391,  173,  296,   78,   77,  305,  235,  300,  351,
 /*   220 */   299,  156,  231,  394,  351,   23,  397,  157,  396,   49,
 /*   230 */   395,   93,   92,  151,  170,  171,  286,  285,  210,   43,
 /*   240 */    41,   45,   42,  345,  170,  171,  134,   36,   35,   50,
 /*   250 */   144,   34,   33,   32,  183,  184,  208,  226,   62,  169,
 /*   260 */   290,   18,  178,  377,  351,   23,  379,  378,  281,   28,
 /*   270 */   206,  376,  268,  374,  373,  375,  370,  372,  371,  134,
 /*   280 */     3,  125,   17,  252,  251,  250,   73,   69,   72,  249,
 /*   290 */   195,  248,  247,  246,  319,  245,  244,  159,  390,   48,
 /*   300 */   389,   44,  177,   55,  351,  353,  388,  214,  317,   23,
 /*   310 */   295,   44,   24,   24,  297,   14,  291,  304,  302,  303,
 /*   320 */   301,   14,  181,   64,  297,  277,  278,  276,  393,  298,
 /*   330 */    15,   48,  108,  265,  417,   58,  105,  266,   81,  298,
 /*   340 */   111,  198,  416,  243,   28,  111,  267,  106,  358,   18,
 /*   350 */   179,  164,  166,  269,  352,  163,  256,   28,  100,  409,
 /*   360 */    48,  387,  386,  385,  384,  383,  382,  381,  380,  368,
 /*   370 */   367,  366,  365,  388,  364,  388,  363,  362,  361,   24,
 /*   380 */   357,  355,  354,   76,   74,  344,   47,   71,  343,  225,
 /*   390 */   342,  341,  340,  339,   68,   66,   16,  215,  338,    8,
 /*   400 */   213,   63,  309,    5,  199,  288,  282,    7,   21,  271,
 /*   410 */     6,   20,  279,  167,  194,  110,  202,  109,  198,  275,
 /*   420 */    56,  263,  262,  261,  191,   61,  190,  260,  188,  187,
 /*   430 */   259,  253,    1,    2,  103,  415,  130,  131,   98,   97,
 /*   440 */   133,  237,  410,  403,  240,  127,  316,  242,   28,  158,
 /*   450 */    30,  117,  119,  129,  132,  217,   70,  216,  128,  241,
 /*   460 */   238,  239,  359,  162,  236,  201,   80,  203,  116,  226,
 /*   470 */   205,  207,   51,  284,  204,   46,   60,  165,   59,  677,
 /*   480 */   139,  418,  414,  413,  412,  411,  137,  677,  408,  407,
 /*   490 */   406,  405,  404,  677,  135,  185,  118,  402,  677,  197,
 /*   500 */   115,  209,  401,  114,  234,  243,  113,  280,  112,  120,
 /*   510 */   315,  264,   87,   54,   86,  400,  398,  677,  200,  677,
 /*   520 */    84,  399,  677,  677,   29,  677,  677,  677,  287,  145,
 /*   530 */   677,  283,  176,  314,  677,  677,  141,  677,   25,   27,
 /*   540 */   360,  126,  350,  349,   75,  348,  228,  677,  346,  227,
 /*   550 */   677,  677,   26,  140,  677,  218,  322,  124,  123,  122,
 /*   560 */   107,  677,  273,  272,  677,  677,  677,  677,  677,  677,
 /*   570 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   580 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   590 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   600 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  270,
 /*   610 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   620 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   630 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   640 */   677,  677,  677,  313,  175,  312,  311,  677,  677,  677,
 /*   650 */   677,  677,  677,  677,  347,  677,  677,  677,  677,  677,
 /*   660 */   677,  677,  677,  677,  677,  677,  677,  677,  677,  677,
 /*   670 */   152,  142,  153,  155,  154,  150,  149,  147,  146,  161,
 /*   680 */   160,  677,  294,  293,  292,  148,  143,  677,  677,   12,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,   61,   62,   37,   38,   39,    9,   67,
 /*    10 */    68,   69,   13,   14,  219,   16,   17,   75,   15,   20,
 /*    20 */    21,  211,  212,   24,   25,   26,   27,   28,   63,   64,
 /*    30 */    65,   59,   33,   34,   61,   62,   37,   38,   39,   13,
 /*    40 */    14,    0,   16,   17,  100,  250,   20,   21,  104,  105,
 /*    50 */    24,   25,   26,   27,   28,   37,   38,   39,  269,   33,
 /*    60 */    34,   66,  252,   37,   38,   39,   13,   14,  279,   16,
 /*    70 */    17,    5,  100,   20,   21,  269,  266,   24,   25,   26,
 /*    80 */    27,   28,  219,   59,  278,  279,   33,   34,    1,    1,
 /*    90 */    37,   38,   39,   73,   74,   14,    9,   16,   17,   33,
 /*   100 */    34,   20,   21,  106,  105,   24,   25,   26,   27,   28,
 /*   110 */   107,  248,  249,  250,   33,   34,   61,   62,   37,   38,
 /*   120 */    39,  124,   67,   68,   69,   37,  131,  101,  218,  134,
 /*   130 */   135,  221,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,   56,   57,   21,  230,   60,  232,  233,
 /*   150 */   234,  235,  236,  237,  238,  239,  240,  241,  242,  243,
 /*   160 */   244,  245,   58,   85,   86,   87,   88,   89,   90,   91,
 /*   170 */    92,   93,   94,   95,   96,   97,   98,   99,   16,   17,
 /*   180 */   214,    0,   20,   21,  130,  214,   24,   25,   26,   27,
 /*   190 */    28,  137,  138,  132,  133,   33,   34,  214,  106,   37,
 /*   200 */    38,   39,    1,    2,  112,   44,    5,  214,    7,    5,
 /*   210 */     9,    7,    1,    2,  132,  133,    5,  251,    7,  253,
 /*   220 */     9,   60,  251,    2,  253,  214,    5,   66,    7,  106,
 /*   230 */     9,   70,   71,   72,   33,   34,  119,  120,   37,   25,
 /*   240 */    26,   27,   28,  218,   33,   34,  221,   33,   34,  126,
 /*   250 */   269,   37,   38,   39,   33,   34,  273,   76,  275,  278,
 /*   260 */   279,  100,  251,  230,  253,  214,  233,  234,  275,  108,
 /*   270 */   277,  238,   37,  240,  241,  242,  218,  244,  245,  221,
 /*   280 */    61,   62,   85,  231,   87,   88,   67,   68,   69,   92,
 /*   290 */   129,   94,   95,   96,  101,   98,   99,  136,    5,  106,
 /*   300 */     7,  100,  251,  100,  253,  102,  254,  101,  101,  214,
 /*   310 */   101,  100,  106,  106,  113,  106,  101,    5,    5,    7,
 /*   320 */     7,  106,  214,  255,  113,  101,  101,  101,  107,  128,
 /*   330 */   106,  106,  106,  101,    1,  267,  100,  252,   73,  128,
 /*   340 */   214,  109,    9,   78,  108,  214,  111,  214,  253,  100,
 /*   350 */   231,  266,  231,  214,  246,  213,  214,  108,   21,   77,
 /*   360 */   106,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   370 */     5,    5,    5,  254,    5,  254,    5,    5,    5,  106,
 /*   380 */   101,    5,    5,  133,  133,   77,   16,   73,    5,   15,
 /*   390 */     5,    5,    5,    5,   73,  104,  100,  103,    9,  100,
 /*   400 */   103,  275,  107,  100,  271,  101,  275,  118,  106,  270,
 /*   410 */   118,  106,  101,    1,  130,  100,  100,  100,  109,  101,
 /*   420 */   110,  101,   86,    5,    5,  106,  139,    5,    5,  139,
 /*   430 */     5,   76,  220,  217,   59,  215,  226,  224,  216,  216,
 /*   440 */   222,   49,  215,  215,   53,  228,  252,   79,  108,  215,
 /*   450 */   127,  259,  257,  223,  225,  215,  219,  215,  227,   81,
 /*   460 */    80,   82,  229,  215,   83,  114,   84,  115,  260,   76,
 /*   470 */   116,  121,  125,  215,  272,  122,  215,  272,  215,  280,
 /*   480 */   214,  214,  214,  214,  214,  214,  214,  280,  214,  214,
 /*   490 */   214,  214,  214,  280,  214,  214,  258,  214,  280,  252,
 /*   500 */   261,  117,  214,  262,  247,   78,  263,  113,  264,  256,
 /*   510 */   265,  252,  214,  123,  214,  214,  254,  280,  272,  280,
 /*   520 */   214,  214,  280,  280,  268,  280,  280,  280,  276,  269,
 /*   530 */   280,  276,  247,  247,  280,  280,  214,  280,  214,  214,
 /*   540 */   214,  214,  214,  214,  214,  214,  214,  280,  214,  214,
 /*   550 */   280,  280,  214,  214,  280,  214,  214,  214,  214,  214,
 /*   560 */   214,  280,  214,  214,  280,  280,  280,  280,  280,  280,
 /*   570 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   580 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   590 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   600 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  214,
 /*   610 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   620 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   630 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   640 */   280,  280,  280,  247,  247,  247,  247,  280,  280,  280,
 /*   650 */   280,  280,  280,  280,  254,  280,  280,  280,  280,  280,
 /*   660 */   280,  280,  280,  280,  280,  280,  280,  280,  280,  280,
 /*   670 */   269,  269,  269,  269,  269,  269,  269,  269,  269,  269,
 /*   680 */   269,  280,  269,  269,  269,  269,  269,  280,  280,  269,
};
#define YY_SHIFT_USE_DFLT (-59)
#define YY_SHIFT_COUNT (258)
#define YY_SHIFT_MIN   (-58)
#define YY_SHIFT_MAX   (427)
static const short yy_shift_ofst[] = {
 /*     0 */   161,   78,  197,  393,  201,  211,  333,  333,  333,  333,
 /*    10 */   333,  333,   -1,   87,  211,  221,  221,  221,  249,  333,
 /*    20 */   333,  333,  181,  333,  333,  265,  427,  427,  -59,  211,
 /*    30 */   211,  211,  211,  211,  211,  211,  211,  211,  211,  211,
 /*    40 */   211,  211,  211,  211,  211,  211,  211,  221,  221,   66,
 /*    50 */    66,   66,   66,   66,   66,   66,  236,  333,  235,  333,
 /*    60 */   333,  333,  117,  117,   92,  333,  333,  333,  333,  333,
 /*    70 */   333,  333,  333,  333,  333,  333,  333,  333,  333,  333,
 /*    80 */   333,  333,  333,  333,  333,  333,  333,  333,  333,  333,
 /*    90 */   333,  333,  333,  333,  333,  333,  333,  333,  333,  333,
 /*   100 */   333,  333,  333,  333,  333,  340,  375,  375,  394,  394,
 /*   110 */   394,  375,  390,  347,  353,  384,  350,  354,  352,  351,
 /*   120 */   323,  340,  375,  375,  375,  393,  375,  382,  381,  392,
 /*   130 */   380,  379,  391,  378,  368,  375,  355,  375,  355,  375,
 /*   140 */   -59,  -59,   26,   53,   53,   53,   81,  162,  214,  214,
 /*   150 */   214,  -58,  -32,  -32,  -32,  -32,  219,   55,   -5,   54,
 /*   160 */    18,   18,  -56,  -35,  232,  226,  225,  224,  215,  209,
 /*   170 */   313,  312,   88,  -28,    3,  123,   -3,  207,  206,  193,
 /*   180 */    82,  203,   61,  293,  204,   20,  -27,  425,  290,  423,
 /*   190 */   422,  287,  419,  418,  336,  284,  309,  320,  310,  319,
 /*   200 */   318,  317,  412,  316,  311,  315,  305,  292,  302,  289,
 /*   210 */   304,  303,  295,  299,  297,  296,  294,  291,  321,  389,
 /*   220 */   388,  387,  386,  385,  383,  308,  374,  314,  370,  251,
 /*   230 */   250,  273,  377,  376,  279,  273,  373,  372,  371,  369,
 /*   240 */   367,  366,  365,  364,  363,  362,  361,  360,  359,  358,
 /*   250 */   357,  356,  254,  282,  337,  124,   24,  104,   41,
};
#define YY_REDUCE_USE_DFLT (-212)
#define YY_REDUCE_COUNT (141)
#define YY_REDUCE_MIN   (-211)
#define YY_REDUCE_MAX   (420)
static const short yy_reduce_ofst[] = {
 /*     0 */  -190,  -84,   33, -137,  -19, -194,   -7,  -17,   51,   11,
 /*    10 */   -29,  -34,  139,  142, -211,  121,  119,   52,   85,  133,
 /*    20 */   131,  126, -205,  108,   95,   58,   25,  -90,   68,  420,
 /*    30 */   417,  416,  415,  414,  413,  411,  410,  409,  408,  407,
 /*    40 */   406,  405,  404,  403,  402,  401,  260,  400,  262,  399,
 /*    50 */   398,  397,  396,  286,  285,  257,  259,  395,  256,  349,
 /*    60 */   348,  346,  255,  252,  253,  345,  344,  343,  342,  341,
 /*    70 */   339,  338,  335,  334,  332,  331,  330,  329,  328,  327,
 /*    80 */   326,  325,  324,  322,  307,  306,  301,  300,  298,  288,
 /*    90 */   283,  281,  280,  278,  277,  276,  275,  274,  272,  271,
 /*   100 */   270,  269,  268,  267,  266,  247,  263,  261,  246,  205,
 /*   110 */   202,  258,  245,  244,  243,  241,  239,  208,  192,  238,
 /*   120 */   195,  194,  248,  242,  240,  237,  234,  233,  217,  231,
 /*   130 */   230,  210,  213,  229,  218,  228,  223,  227,  222,  220,
 /*   140 */   212,  216,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   675,  484,  473,  481,  664,  664,  675,  675,  675,  675,
 /*    10 */   675,  675,  587,  448,  664,  675,  675,  675,  675,  675,
 /*    20 */   675,  675,  481,  675,  675,  486,  486,  486,  582,  675,
 /*    30 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*    40 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*    50 */   675,  675,  675,  675,  675,  675,  675,  675,  589,  591,
 /*    60 */   593,  675,  611,  611,  580,  675,  675,  675,  675,  675,
 /*    70 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*    80 */   675,  675,  675,  675,  471,  675,  469,  675,  675,  675,
 /*    90 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  458,
 /*   100 */   675,  675,  675,  675,  675,  675,  450,  450,  675,  675,
 /*   110 */   675,  450,  618,  622,  616,  604,  612,  603,  599,  598,
 /*   120 */   626,  675,  450,  450,  450,  481,  450,  502,  500,  498,
 /*   130 */   490,  496,  492,  494,  488,  450,  479,  450,  479,  450,
 /*   140 */   520,  536,  675,  627,  663,  617,  653,  652,  659,  651,
 /*   150 */   650,  675,  646,  647,  649,  648,  675,  675,  675,  675,
 /*   160 */   655,  654,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   170 */   675,  675,  675,  629,  675,  623,  619,  675,  675,  675,
 /*   180 */   675,  546,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   190 */   675,  675,  675,  675,  675,  675,  579,  675,  675,  590,
 /*   200 */   675,  675,  675,  675,  675,  675,  613,  675,  605,  675,
 /*   210 */   675,  675,  675,  675,  556,  675,  675,  675,  675,  675,
 /*   220 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   230 */   675,  668,  675,  675,  675,  666,  675,  675,  675,  675,
 /*   240 */   675,  675,  675,  675,  675,  675,  675,  675,  675,  675,
 /*   250 */   675,  675,  505,  675,  456,  454,  675,  446,  675,  674,
 /*   260 */   673,  672,  665,  578,  577,  576,  575,  588,  584,  586,
 /*   270 */   585,  583,  592,  594,  581,  597,  596,  601,  600,  602,
 /*   280 */   595,  615,  614,  607,  608,  610,  609,  606,  643,  661,
 /*   290 */   662,  660,  658,  657,  656,  642,  641,  640,  639,  638,
 /*   300 */   635,  637,  634,  636,  633,  632,  631,  630,  628,  645,
 /*   310 */   644,  625,  624,  621,  620,  574,  559,  557,  554,  558,
 /*   320 */   555,  552,  485,  535,  534,  533,  532,  531,  530,  529,
 /*   330 */   528,  527,  526,  525,  524,  523,  522,  521,  517,  513,
 /*   340 */   511,  510,  509,  506,  480,  483,  482,  671,  670,  669,
 /*   350 */   667,  561,  562,  548,  551,  550,  549,  547,  560,  504,
 /*   360 */   503,  501,  499,  491,  497,  493,  495,  489,  487,  475,
 /*   370 */   474,  545,  544,  543,  542,  541,  540,  539,  538,  537,
 /*   380 */   519,  518,  516,  515,  514,  512,  508,  507,  564,  573,
 /*   390 */   572,  571,  570,  569,  568,  567,  566,  565,  563,  472,
 /*   400 */   470,  468,  467,  466,  465,  464,  463,  462,  461,  478,
 /*   410 */   460,  459,  457,  455,  453,  452,  477,  476,  451,  449,
 /*   420 */   447,  445,  444,  443,  442,  441,  440,  439,  438,  437,
 /*   430 */   436,  435,  434,
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
  "SHOW",          "DATABASES",     "MNODES",        "DNODES",      
  "ACCOUNTS",      "USERS",         "MODULES",       "QUERIES",     
  "CONNECTIONS",   "STREAMS",       "VARIABLES",     "SCORES",      
  "GRANTS",        "VNODES",        "IPTOKEN",       "DOT",         
  "CREATE",        "TABLE",         "DATABASE",      "TABLES",      
  "STABLES",       "VGROUPS",       "DROP",          "DNODE",       
  "USER",          "ACCOUNT",       "USE",           "DESCRIBE",    
  "ALTER",         "PASS",          "PRIVILEGE",     "LOCAL",       
  "IF",            "EXISTS",        "PPS",           "TSERIES",     
  "DBS",           "STORAGE",       "QTIME",         "CONNS",       
  "STATE",         "KEEP",          "CACHE",         "REPLICA",     
  "QUORUM",        "DAYS",          "MINROWS",       "MAXROWS",     
  "BLOCKS",        "CTIME",         "WAL",           "FSYNC",       
  "COMP",          "PRECISION",     "UPDATE",        "CACHELAST",   
  "LP",            "RP",            "UNSIGNED",      "TAGS",        
  "USING",         "AS",            "COMMA",         "NULL",        
  "SELECT",        "UNION",         "ALL",           "DISTINCT",    
  "FROM",          "VARIABLE",      "INTERVAL",      "FILL",        
  "SLIDING",       "ORDER",         "BY",            "ASC",         
  "DESC",          "GROUP",         "HAVING",        "LIMIT",       
  "OFFSET",        "SLIMIT",        "SOFFSET",       "WHERE",       
  "NOW",           "RESET",         "QUERY",         "ADD",         
  "COLUMN",        "TAG",           "CHANGE",        "SET",         
  "KILL",          "CONNECTION",    "STREAM",        "COLON",       
  "ABORT",         "AFTER",         "ATTACH",        "BEFORE",      
  "BEGIN",         "CASCADE",       "CLUSTER",       "CONFLICT",    
  "COPY",          "DEFERRED",      "DELIMITERS",    "DETACH",      
  "EACH",          "END",           "EXPLAIN",       "FAIL",        
  "FOR",           "IGNORE",        "IMMEDIATE",     "INITIALLY",   
  "INSTEAD",       "MATCH",         "KEY",           "OF",          
  "RAISE",         "REPLACE",       "RESTRICT",      "ROW",         
  "STATEMENT",     "TRIGGER",       "VIEW",          "COUNT",       
  "SUM",           "AVG",           "MIN",           "MAX",         
  "FIRST",         "LAST",          "TOP",           "BOTTOM",      
  "STDDEV",        "PERCENTILE",    "APERCENTILE",   "LEASTSQUARES",
  "HISTOGRAM",     "DIFF",          "SPREAD",        "TWA",         
  "INTERP",        "LAST_ROW",      "RATE",          "IRATE",       
  "SUM_RATE",      "SUM_IRATE",     "AVG_RATE",      "AVG_IRATE",   
  "TBID",          "SEMI",          "NONE",          "PREV",        
  "LINEAR",        "IMPORT",        "METRIC",        "TBNAME",      
  "JOIN",          "METRICS",       "STABLE",        "INSERT",      
  "INTO",          "VALUES",        "error",         "program",     
  "cmd",           "dbPrefix",      "ids",           "cpxName",     
  "ifexists",      "alter_db_optr",  "acct_optr",     "ifnotexists", 
  "db_optr",       "pps",           "tseries",       "dbs",         
  "streams",       "storage",       "qtime",         "users",       
  "conns",         "state",         "keep",          "tagitemlist", 
  "cache",         "replica",       "quorum",        "days",        
  "minrows",       "maxrows",       "blocks",        "ctime",       
  "wal",           "fsync",         "comp",          "prec",        
  "update",        "cachelast",     "typename",      "signed",      
  "create_table_args",  "create_table_list",  "create_from_stable",  "columnlist",  
  "select",        "column",        "tagitem",       "selcollist",  
  "from",          "where_opt",     "interval_opt",  "fill_opt",    
  "sliding_opt",   "groupby_opt",   "orderby_opt",   "having_opt",  
  "slimit_opt",    "limit_opt",     "union",         "sclp",        
  "distinct",      "expr",          "as",            "tablelist",   
  "tmvar",         "sortlist",      "sortitem",      "item",        
  "sortorder",     "grouplist",     "exprlist",      "expritem",    
};
#endif /* NDEBUG */

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
    case 230: /* keep */
    case 231: /* tagitemlist */
    case 251: /* columnlist */
    case 259: /* fill_opt */
    case 261: /* groupby_opt */
    case 262: /* orderby_opt */
    case 273: /* sortlist */
    case 277: /* grouplist */
{
#line 227 "sql.y"
taosArrayDestroy((yypminor->yy221));
#line 1123 "sql.c"
}
      break;
    case 249: /* create_table_list */
{
#line 311 "sql.y"
destroyCreateTableSql((yypminor->yy358));
#line 1130 "sql.c"
}
      break;
    case 252: /* select */
{
#line 418 "sql.y"
doDestroyQuerySql((yypminor->yy344));
#line 1137 "sql.c"
}
      break;
    case 255: /* selcollist */
    case 267: /* sclp */
    case 278: /* exprlist */
{
#line 445 "sql.y"
tSqlExprListDestroy((yypminor->yy178));
#line 1146 "sql.c"
}
      break;
    case 257: /* where_opt */
    case 263: /* having_opt */
    case 269: /* expr */
    case 279: /* expritem */
{
#line 612 "sql.y"
tSqlExprDestroy((yypminor->yy50));
#line 1156 "sql.c"
}
      break;
    case 266: /* union */
{
#line 424 "sql.y"
destroyAllSelectClause((yypminor->yy273));
#line 1163 "sql.c"
}
      break;
    case 274: /* sortitem */
{
#line 545 "sql.y"
tVariantDestroy(&(yypminor->yy106));
#line 1170 "sql.c"
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
  { 211, 1 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 2 },
  { 212, 3 },
  { 213, 0 },
  { 213, 2 },
  { 215, 0 },
  { 215, 2 },
  { 212, 5 },
  { 212, 4 },
  { 212, 3 },
  { 212, 5 },
  { 212, 3 },
  { 212, 5 },
  { 212, 3 },
  { 212, 4 },
  { 212, 5 },
  { 212, 4 },
  { 212, 3 },
  { 212, 3 },
  { 212, 3 },
  { 212, 2 },
  { 212, 3 },
  { 212, 5 },
  { 212, 5 },
  { 212, 4 },
  { 212, 5 },
  { 212, 3 },
  { 212, 4 },
  { 212, 4 },
  { 212, 4 },
  { 212, 6 },
  { 214, 1 },
  { 214, 1 },
  { 216, 2 },
  { 216, 0 },
  { 219, 3 },
  { 219, 0 },
  { 212, 3 },
  { 212, 6 },
  { 212, 5 },
  { 212, 5 },
  { 221, 0 },
  { 221, 2 },
  { 222, 0 },
  { 222, 2 },
  { 223, 0 },
  { 223, 2 },
  { 224, 0 },
  { 224, 2 },
  { 225, 0 },
  { 225, 2 },
  { 226, 0 },
  { 226, 2 },
  { 227, 0 },
  { 227, 2 },
  { 228, 0 },
  { 228, 2 },
  { 229, 0 },
  { 229, 2 },
  { 218, 9 },
  { 230, 2 },
  { 232, 2 },
  { 233, 2 },
  { 234, 2 },
  { 235, 2 },
  { 236, 2 },
  { 237, 2 },
  { 238, 2 },
  { 239, 2 },
  { 240, 2 },
  { 241, 2 },
  { 242, 2 },
  { 243, 2 },
  { 244, 2 },
  { 245, 2 },
  { 220, 0 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 220, 2 },
  { 217, 0 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 246, 1 },
  { 246, 4 },
  { 246, 2 },
  { 247, 1 },
  { 247, 2 },
  { 247, 2 },
  { 212, 3 },
  { 212, 3 },
  { 249, 1 },
  { 249, 2 },
  { 248, 6 },
  { 248, 10 },
  { 250, 10 },
  { 248, 5 },
  { 251, 3 },
  { 251, 1 },
  { 253, 2 },
  { 231, 3 },
  { 231, 1 },
  { 254, 1 },
  { 254, 1 },
  { 254, 1 },
  { 254, 1 },
  { 254, 1 },
  { 254, 2 },
  { 254, 2 },
  { 254, 2 },
  { 254, 2 },
  { 252, 12 },
  { 266, 1 },
  { 266, 3 },
  { 266, 4 },
  { 266, 6 },
  { 212, 1 },
  { 252, 2 },
  { 267, 2 },
  { 267, 0 },
  { 255, 4 },
  { 255, 2 },
  { 270, 2 },
  { 270, 1 },
  { 270, 0 },
  { 268, 1 },
  { 268, 0 },
  { 256, 2 },
  { 271, 2 },
  { 271, 3 },
  { 271, 4 },
  { 271, 5 },
  { 272, 1 },
  { 258, 4 },
  { 258, 6 },
  { 258, 0 },
  { 259, 0 },
  { 259, 6 },
  { 259, 4 },
  { 260, 4 },
  { 260, 0 },
  { 262, 0 },
  { 262, 3 },
  { 273, 4 },
  { 273, 2 },
  { 275, 2 },
  { 276, 1 },
  { 276, 1 },
  { 276, 0 },
  { 261, 0 },
  { 261, 3 },
  { 277, 3 },
  { 277, 1 },
  { 263, 0 },
  { 263, 2 },
  { 265, 0 },
  { 265, 2 },
  { 265, 4 },
  { 265, 4 },
  { 264, 0 },
  { 264, 2 },
  { 264, 4 },
  { 264, 4 },
  { 257, 0 },
  { 257, 2 },
  { 269, 3 },
  { 269, 1 },
  { 269, 3 },
  { 269, 3 },
  { 269, 1 },
  { 269, 2 },
  { 269, 2 },
  { 269, 1 },
  { 269, 2 },
  { 269, 2 },
  { 269, 1 },
  { 269, 1 },
  { 269, 1 },
  { 269, 1 },
  { 269, 4 },
  { 269, 4 },
  { 269, 3 },
  { 269, 4 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 3 },
  { 269, 5 },
  { 278, 3 },
  { 278, 1 },
  { 279, 1 },
  { 279, 0 },
  { 212, 3 },
  { 212, 7 },
  { 212, 7 },
  { 212, 7 },
  { 212, 7 },
  { 212, 8 },
  { 212, 9 },
  { 212, 3 },
  { 212, 5 },
  { 212, 5 },
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
#line 63 "sql.y"
{}
#line 1707 "sql.c"
        break;
      case 1: /* cmd ::= SHOW DATABASES */
#line 66 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
#line 1712 "sql.c"
        break;
      case 2: /* cmd ::= SHOW MNODES */
#line 67 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
#line 1717 "sql.c"
        break;
      case 3: /* cmd ::= SHOW DNODES */
#line 68 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
#line 1722 "sql.c"
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
#line 69 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
#line 1727 "sql.c"
        break;
      case 5: /* cmd ::= SHOW USERS */
#line 70 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
#line 1732 "sql.c"
        break;
      case 6: /* cmd ::= SHOW MODULES */
#line 72 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
#line 1737 "sql.c"
        break;
      case 7: /* cmd ::= SHOW QUERIES */
#line 73 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
#line 1742 "sql.c"
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
#line 74 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
#line 1747 "sql.c"
        break;
      case 9: /* cmd ::= SHOW STREAMS */
#line 75 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
#line 1752 "sql.c"
        break;
      case 10: /* cmd ::= SHOW VARIABLES */
#line 76 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
#line 1757 "sql.c"
        break;
      case 11: /* cmd ::= SHOW SCORES */
#line 77 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
#line 1762 "sql.c"
        break;
      case 12: /* cmd ::= SHOW GRANTS */
#line 78 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
#line 1767 "sql.c"
        break;
      case 13: /* cmd ::= SHOW VNODES */
#line 80 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
#line 1772 "sql.c"
        break;
      case 14: /* cmd ::= SHOW VNODES IPTOKEN */
#line 81 "sql.y"
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
#line 1777 "sql.c"
        break;
      case 15: /* dbPrefix ::= */
#line 85 "sql.y"
{yygotominor.yy0.n = 0; yygotominor.yy0.type = 0;}
#line 1782 "sql.c"
        break;
      case 16: /* dbPrefix ::= ids DOT */
#line 86 "sql.y"
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
#line 1787 "sql.c"
        break;
      case 17: /* cpxName ::= */
#line 89 "sql.y"
{yygotominor.yy0.n = 0;  }
#line 1792 "sql.c"
        break;
      case 18: /* cpxName ::= DOT ids */
#line 90 "sql.y"
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
#line 1797 "sql.c"
        break;
      case 19: /* cmd ::= SHOW CREATE TABLE ids cpxName */
#line 92 "sql.y"
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 1805 "sql.c"
        break;
      case 20: /* cmd ::= SHOW CREATE DATABASE ids */
#line 97 "sql.y"
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
#line 1812 "sql.c"
        break;
      case 21: /* cmd ::= SHOW dbPrefix TABLES */
#line 101 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
#line 1819 "sql.c"
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
#line 105 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
#line 1826 "sql.c"
        break;
      case 23: /* cmd ::= SHOW dbPrefix STABLES */
#line 109 "sql.y"
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
#line 1833 "sql.c"
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
#line 113 "sql.y"
{
    SStrToken token;
    setDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
#line 1842 "sql.c"
        break;
      case 25: /* cmd ::= SHOW dbPrefix VGROUPS */
#line 119 "sql.y"
{
    SStrToken token;
    setDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
#line 1851 "sql.c"
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS ids */
#line 125 "sql.y"
{
    SStrToken token;
    setDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
#line 1860 "sql.c"
        break;
      case 27: /* cmd ::= DROP TABLE ifexists ids cpxName */
#line 132 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
#line 1868 "sql.c"
        break;
      case 28: /* cmd ::= DROP DATABASE ifexists ids */
#line 137 "sql.y"
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
#line 1873 "sql.c"
        break;
      case 29: /* cmd ::= DROP DNODE ids */
#line 138 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
#line 1878 "sql.c"
        break;
      case 30: /* cmd ::= DROP USER ids */
#line 139 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
#line 1883 "sql.c"
        break;
      case 31: /* cmd ::= DROP ACCOUNT ids */
#line 140 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
#line 1888 "sql.c"
        break;
      case 32: /* cmd ::= USE ids */
#line 143 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
#line 1893 "sql.c"
        break;
      case 33: /* cmd ::= DESCRIBE ids cpxName */
#line 146 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 1901 "sql.c"
        break;
      case 34: /* cmd ::= ALTER USER ids PASS ids */
#line 152 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
#line 1906 "sql.c"
        break;
      case 35: /* cmd ::= ALTER USER ids PRIVILEGE ids */
#line 153 "sql.y"
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
#line 1911 "sql.c"
        break;
      case 36: /* cmd ::= ALTER DNODE ids ids */
#line 154 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 1916 "sql.c"
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids ids */
#line 155 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
#line 1921 "sql.c"
        break;
      case 38: /* cmd ::= ALTER LOCAL ids */
#line 156 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
#line 1926 "sql.c"
        break;
      case 39: /* cmd ::= ALTER LOCAL ids ids */
#line 157 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 1931 "sql.c"
        break;
      case 40: /* cmd ::= ALTER DATABASE ids alter_db_optr */
#line 158 "sql.y"
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
#line 1936 "sql.c"
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
#line 160 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy79);}
#line 1941 "sql.c"
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
#line 161 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy79);}
#line 1946 "sql.c"
        break;
      case 43: /* ids ::= ID */
      case 44: /* ids ::= STRING */ yytestcase(yyruleno==44);
#line 167 "sql.y"
{yygotominor.yy0 = yymsp[0].minor.yy0; }
#line 1952 "sql.c"
        break;
      case 45: /* ifexists ::= IF EXISTS */
      case 47: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==47);
#line 171 "sql.y"
{ yygotominor.yy0.n = 1;}
#line 1958 "sql.c"
        break;
      case 46: /* ifexists ::= */
      case 48: /* ifnotexists ::= */ yytestcase(yyruleno==48);
      case 156: /* distinct ::= */ yytestcase(yyruleno==156);
#line 172 "sql.y"
{ yygotominor.yy0.n = 0;}
#line 1965 "sql.c"
        break;
      case 49: /* cmd ::= CREATE DNODE ids */
#line 180 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
#line 1970 "sql.c"
        break;
      case 50: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
#line 182 "sql.y"
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy79);}
#line 1975 "sql.c"
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
#line 183 "sql.y"
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
#line 1980 "sql.c"
        break;
      case 52: /* cmd ::= CREATE USER ids PASS ids */
#line 184 "sql.y"
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
#line 1985 "sql.c"
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
#line 186 "sql.y"
{ yygotominor.yy0.n = 0;   }
#line 1998 "sql.c"
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
#line 187 "sql.y"
{ yygotominor.yy0 = yymsp[0].minor.yy0;     }
#line 2011 "sql.c"
        break;
      case 71: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
#line 214 "sql.y"
{
    yygotominor.yy79.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy79.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy79.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy79.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy79.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy79.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy79.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy79.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy79.stat    = yymsp[0].minor.yy0;
}
#line 2026 "sql.c"
        break;
      case 72: /* keep ::= KEEP tagitemlist */
#line 228 "sql.y"
{ yygotominor.yy221 = yymsp[0].minor.yy221; }
#line 2031 "sql.c"
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
#line 230 "sql.y"
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
#line 2049 "sql.c"
        break;
      case 87: /* db_optr ::= */
#line 246 "sql.y"
{setDefaultCreateDbOption(&yygotominor.yy478);}
#line 2054 "sql.c"
        break;
      case 88: /* db_optr ::= db_optr cache */
#line 248 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2059 "sql.c"
        break;
      case 89: /* db_optr ::= db_optr replica */
      case 104: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==104);
#line 249 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2065 "sql.c"
        break;
      case 90: /* db_optr ::= db_optr quorum */
      case 105: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==105);
#line 250 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2071 "sql.c"
        break;
      case 91: /* db_optr ::= db_optr days */
#line 251 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2076 "sql.c"
        break;
      case 92: /* db_optr ::= db_optr minrows */
#line 252 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 2081 "sql.c"
        break;
      case 93: /* db_optr ::= db_optr maxrows */
#line 253 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 2086 "sql.c"
        break;
      case 94: /* db_optr ::= db_optr blocks */
      case 107: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==107);
#line 254 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2092 "sql.c"
        break;
      case 95: /* db_optr ::= db_optr ctime */
#line 255 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2097 "sql.c"
        break;
      case 96: /* db_optr ::= db_optr wal */
      case 109: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==109);
#line 256 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2103 "sql.c"
        break;
      case 97: /* db_optr ::= db_optr fsync */
      case 110: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==110);
#line 257 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2109 "sql.c"
        break;
      case 98: /* db_optr ::= db_optr comp */
      case 108: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==108);
#line 258 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2115 "sql.c"
        break;
      case 99: /* db_optr ::= db_optr prec */
#line 259 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.precision = yymsp[0].minor.yy0; }
#line 2120 "sql.c"
        break;
      case 100: /* db_optr ::= db_optr keep */
      case 106: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==106);
#line 260 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.keep = yymsp[0].minor.yy221; }
#line 2126 "sql.c"
        break;
      case 101: /* db_optr ::= db_optr update */
      case 111: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==111);
#line 261 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2132 "sql.c"
        break;
      case 102: /* db_optr ::= db_optr cachelast */
      case 112: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==112);
#line 262 "sql.y"
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2138 "sql.c"
        break;
      case 103: /* alter_db_optr ::= */
#line 265 "sql.y"
{ setDefaultCreateDbOption(&yygotominor.yy478);}
#line 2143 "sql.c"
        break;
      case 113: /* typename ::= ids */
#line 278 "sql.y"
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yygotominor.yy503, &yymsp[0].minor.yy0);
}
#line 2151 "sql.c"
        break;
      case 114: /* typename ::= ids LP signed RP */
#line 284 "sql.y"
{
  if (yymsp[-1].minor.yy109 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yygotominor.yy503, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy109;  // negative value of name length
    tSqlSetColumnType(&yygotominor.yy503, &yymsp[-3].minor.yy0);
  }
}
#line 2164 "sql.c"
        break;
      case 115: /* typename ::= ids UNSIGNED */
#line 295 "sql.y"
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yygotominor.yy503, &yymsp[-1].minor.yy0);
}
#line 2173 "sql.c"
        break;
      case 116: /* signed ::= INTEGER */
      case 117: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==117);
#line 302 "sql.y"
{ yygotominor.yy109 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2179 "sql.c"
        break;
      case 118: /* signed ::= MINUS INTEGER */
      case 119: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==119);
#line 304 "sql.y"
{ yygotominor.yy109 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
#line 2185 "sql.c"
        break;
      case 120: /* cmd ::= CREATE TABLE create_table_list */
#line 308 "sql.y"
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy358;}
#line 2190 "sql.c"
        break;
      case 121: /* create_table_list ::= create_from_stable */
#line 312 "sql.y"
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy416);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yygotominor.yy358 = pCreateTable;
}
#line 2202 "sql.c"
        break;
      case 122: /* create_table_list ::= create_table_list create_from_stable */
#line 321 "sql.y"
{
  taosArrayPush(yymsp[-1].minor.yy358->childTableInfo, &yymsp[0].minor.yy416);
  yygotominor.yy358 = yymsp[-1].minor.yy358;
}
#line 2210 "sql.c"
        break;
      case 123: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
#line 327 "sql.y"
{
  yygotominor.yy358 = tSetCreateSqlElems(yymsp[-1].minor.yy221, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yygotominor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
#line 2221 "sql.c"
        break;
      case 124: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
#line 336 "sql.y"
{
  yygotominor.yy358 = tSetCreateSqlElems(yymsp[-5].minor.yy221, yymsp[-1].minor.yy221, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yygotominor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 2232 "sql.c"
        break;
      case 125: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
#line 347 "sql.y"
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yygotominor.yy416 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy221, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
#line 2241 "sql.c"
        break;
      case 126: /* create_table_args ::= ifnotexists ids cpxName AS select */
#line 355 "sql.y"
{
  yygotominor.yy358 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy344, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yygotominor.yy358, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
#line 2252 "sql.c"
        break;
      case 127: /* columnlist ::= columnlist COMMA column */
#line 366 "sql.y"
{taosArrayPush(yymsp[-2].minor.yy221, &yymsp[0].minor.yy503); yygotominor.yy221 = yymsp[-2].minor.yy221;  }
#line 2257 "sql.c"
        break;
      case 128: /* columnlist ::= column */
#line 367 "sql.y"
{yygotominor.yy221 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yygotominor.yy221, &yymsp[0].minor.yy503);}
#line 2262 "sql.c"
        break;
      case 129: /* column ::= ids typename */
#line 371 "sql.y"
{
  tSqlSetColumnInfo(&yygotominor.yy503, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy503);
}
#line 2269 "sql.c"
        break;
      case 130: /* tagitemlist ::= tagitemlist COMMA tagitem */
#line 379 "sql.y"
{ yygotominor.yy221 = tVariantListAppend(yymsp[-2].minor.yy221, &yymsp[0].minor.yy106, -1);    }
#line 2274 "sql.c"
        break;
      case 131: /* tagitemlist ::= tagitem */
#line 380 "sql.y"
{ yygotominor.yy221 = tVariantListAppend(NULL, &yymsp[0].minor.yy106, -1); }
#line 2279 "sql.c"
        break;
      case 132: /* tagitem ::= INTEGER */
      case 133: /* tagitem ::= FLOAT */ yytestcase(yyruleno==133);
      case 134: /* tagitem ::= STRING */ yytestcase(yyruleno==134);
      case 135: /* tagitem ::= BOOL */ yytestcase(yyruleno==135);
#line 382 "sql.y"
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy106, &yymsp[0].minor.yy0); }
#line 2287 "sql.c"
        break;
      case 136: /* tagitem ::= NULL */
#line 386 "sql.y"
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy106, &yymsp[0].minor.yy0); }
#line 2292 "sql.c"
        break;
      case 137: /* tagitem ::= MINUS INTEGER */
      case 138: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==138);
      case 139: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==139);
      case 140: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==140);
#line 388 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy106, &yymsp[-1].minor.yy0);
}
#line 2305 "sql.c"
        break;
      case 141: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
#line 419 "sql.y"
{
  yygotominor.yy344 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy178, yymsp[-9].minor.yy221, yymsp[-8].minor.yy50, yymsp[-4].minor.yy221, yymsp[-3].minor.yy221, &yymsp[-7].minor.yy280, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy221, &yymsp[0].minor.yy454, &yymsp[-1].minor.yy454);
}
#line 2312 "sql.c"
        break;
      case 142: /* union ::= select */
#line 426 "sql.y"
{ yygotominor.yy273 = setSubclause(NULL, yymsp[0].minor.yy344); }
#line 2317 "sql.c"
        break;
      case 143: /* union ::= LP union RP */
#line 427 "sql.y"
{ yygotominor.yy273 = yymsp[-1].minor.yy273; }
#line 2322 "sql.c"
        break;
      case 144: /* union ::= union UNION ALL select */
#line 428 "sql.y"
{ yygotominor.yy273 = appendSelectClause(yymsp[-3].minor.yy273, yymsp[0].minor.yy344); }
#line 2327 "sql.c"
        break;
      case 145: /* union ::= union UNION ALL LP select RP */
#line 429 "sql.y"
{ yygotominor.yy273 = appendSelectClause(yymsp[-5].minor.yy273, yymsp[-1].minor.yy344); }
#line 2332 "sql.c"
        break;
      case 146: /* cmd ::= union */
#line 431 "sql.y"
{ setSqlInfo(pInfo, yymsp[0].minor.yy273, NULL, TSDB_SQL_SELECT); }
#line 2337 "sql.c"
        break;
      case 147: /* select ::= SELECT selcollist */
#line 437 "sql.y"
{
  yygotominor.yy344 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy178, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
#line 2344 "sql.c"
        break;
      case 148: /* sclp ::= selcollist COMMA */
#line 449 "sql.y"
{yygotominor.yy178 = yymsp[-1].minor.yy178;}
#line 2349 "sql.c"
        break;
      case 149: /* sclp ::= */
#line 450 "sql.y"
{yygotominor.yy178 = 0;}
#line 2354 "sql.c"
        break;
      case 150: /* selcollist ::= sclp distinct expr as */
#line 451 "sql.y"
{
   yygotominor.yy178 = tSqlExprListAppend(yymsp[-3].minor.yy178, yymsp[-1].minor.yy50,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
#line 2361 "sql.c"
        break;
      case 151: /* selcollist ::= sclp STAR */
#line 455 "sql.y"
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yygotominor.yy178 = tSqlExprListAppend(yymsp[-1].minor.yy178, pNode, 0, 0);
}
#line 2369 "sql.c"
        break;
      case 152: /* as ::= AS ids */
      case 153: /* as ::= ids */ yytestcase(yyruleno==153);
#line 464 "sql.y"
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
#line 2375 "sql.c"
        break;
      case 154: /* as ::= */
#line 466 "sql.y"
{ yygotominor.yy0.n = 0;  }
#line 2380 "sql.c"
        break;
      case 155: /* distinct ::= DISTINCT */
#line 469 "sql.y"
{ yygotominor.yy0 = yymsp[0].minor.yy0;  }
#line 2385 "sql.c"
        break;
      case 157: /* from ::= FROM tablelist */
      case 172: /* orderby_opt ::= ORDER BY sortlist */ yytestcase(yyruleno==172);
#line 475 "sql.y"
{yygotominor.yy221 = yymsp[0].minor.yy221;}
#line 2391 "sql.c"
        break;
      case 158: /* tablelist ::= ids cpxName */
#line 478 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy221 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yygotominor.yy221 = tVariantListAppendToken(yygotominor.yy221, &yymsp[-1].minor.yy0, -1);  // table alias name
}
#line 2401 "sql.c"
        break;
      case 159: /* tablelist ::= ids cpxName ids */
#line 485 "sql.y"
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy221 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yygotominor.yy221 = tVariantListAppendToken(yygotominor.yy221, &yymsp[0].minor.yy0, -1);
}
#line 2412 "sql.c"
        break;
      case 160: /* tablelist ::= tablelist COMMA ids cpxName */
#line 493 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy221 = tVariantListAppendToken(yymsp[-3].minor.yy221, &yymsp[-1].minor.yy0, -1);
  yygotominor.yy221 = tVariantListAppendToken(yygotominor.yy221, &yymsp[-1].minor.yy0, -1);
}
#line 2422 "sql.c"
        break;
      case 161: /* tablelist ::= tablelist COMMA ids cpxName ids */
#line 500 "sql.y"
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy221 = tVariantListAppendToken(yymsp[-4].minor.yy221, &yymsp[-2].minor.yy0, -1);
  yygotominor.yy221 = tVariantListAppendToken(yygotominor.yy221, &yymsp[0].minor.yy0, -1);
}
#line 2433 "sql.c"
        break;
      case 162: /* tmvar ::= VARIABLE */
#line 510 "sql.y"
{yygotominor.yy0 = yymsp[0].minor.yy0;}
#line 2438 "sql.c"
        break;
      case 163: /* interval_opt ::= INTERVAL LP tmvar RP */
#line 513 "sql.y"
{yygotominor.yy280.interval = yymsp[-1].minor.yy0; yygotominor.yy280.offset.n = 0; yygotominor.yy280.offset.z = NULL; yygotominor.yy280.offset.type = 0;}
#line 2443 "sql.c"
        break;
      case 164: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
#line 514 "sql.y"
{yygotominor.yy280.interval = yymsp[-3].minor.yy0; yygotominor.yy280.offset = yymsp[-1].minor.yy0;}
#line 2448 "sql.c"
        break;
      case 165: /* interval_opt ::= */
#line 515 "sql.y"
{memset(&yygotominor.yy280, 0, sizeof(yygotominor.yy280));}
#line 2453 "sql.c"
        break;
      case 166: /* fill_opt ::= */
#line 519 "sql.y"
{yygotominor.yy221 = 0;     }
#line 2458 "sql.c"
        break;
      case 167: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
#line 520 "sql.y"
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy221, &A, -1, 0);
    yygotominor.yy221 = yymsp[-1].minor.yy221;
}
#line 2470 "sql.c"
        break;
      case 168: /* fill_opt ::= FILL LP ID RP */
#line 529 "sql.y"
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy221 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
#line 2478 "sql.c"
        break;
      case 169: /* sliding_opt ::= SLIDING LP tmvar RP */
#line 535 "sql.y"
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
#line 2483 "sql.c"
        break;
      case 170: /* sliding_opt ::= */
#line 536 "sql.y"
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
#line 2488 "sql.c"
        break;
      case 171: /* orderby_opt ::= */
#line 547 "sql.y"
{yygotominor.yy221 = 0;}
#line 2493 "sql.c"
        break;
      case 173: /* sortlist ::= sortlist COMMA item sortorder */
#line 550 "sql.y"
{
    yygotominor.yy221 = tVariantListAppend(yymsp[-3].minor.yy221, &yymsp[-1].minor.yy106, yymsp[0].minor.yy172);
}
#line 2500 "sql.c"
        break;
      case 174: /* sortlist ::= item sortorder */
#line 554 "sql.y"
{
  yygotominor.yy221 = tVariantListAppend(NULL, &yymsp[-1].minor.yy106, yymsp[0].minor.yy172);
}
#line 2507 "sql.c"
        break;
      case 175: /* item ::= ids cpxName */
#line 559 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy106, &yymsp[-1].minor.yy0);
}
#line 2517 "sql.c"
        break;
      case 176: /* sortorder ::= ASC */
      case 178: /* sortorder ::= */ yytestcase(yyruleno==178);
#line 567 "sql.y"
{ yygotominor.yy172 = TSDB_ORDER_ASC; }
#line 2523 "sql.c"
        break;
      case 177: /* sortorder ::= DESC */
#line 568 "sql.y"
{ yygotominor.yy172 = TSDB_ORDER_DESC;}
#line 2528 "sql.c"
        break;
      case 179: /* groupby_opt ::= */
#line 577 "sql.y"
{ yygotominor.yy221 = 0;}
#line 2533 "sql.c"
        break;
      case 180: /* groupby_opt ::= GROUP BY grouplist */
#line 578 "sql.y"
{ yygotominor.yy221 = yymsp[0].minor.yy221;}
#line 2538 "sql.c"
        break;
      case 181: /* grouplist ::= grouplist COMMA item */
#line 580 "sql.y"
{
  yygotominor.yy221 = tVariantListAppend(yymsp[-2].minor.yy221, &yymsp[0].minor.yy106, -1);
}
#line 2545 "sql.c"
        break;
      case 182: /* grouplist ::= item */
#line 584 "sql.y"
{
  yygotominor.yy221 = tVariantListAppend(NULL, &yymsp[0].minor.yy106, -1);
}
#line 2552 "sql.c"
        break;
      case 183: /* having_opt ::= */
      case 193: /* where_opt ::= */ yytestcase(yyruleno==193);
      case 231: /* expritem ::= */ yytestcase(yyruleno==231);
#line 591 "sql.y"
{yygotominor.yy50 = 0;}
#line 2559 "sql.c"
        break;
      case 184: /* having_opt ::= HAVING expr */
      case 194: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==194);
      case 230: /* expritem ::= expr */ yytestcase(yyruleno==230);
#line 592 "sql.y"
{yygotominor.yy50 = yymsp[0].minor.yy50;}
#line 2566 "sql.c"
        break;
      case 185: /* limit_opt ::= */
      case 189: /* slimit_opt ::= */ yytestcase(yyruleno==189);
#line 596 "sql.y"
{yygotominor.yy454.limit = -1; yygotominor.yy454.offset = 0;}
#line 2572 "sql.c"
        break;
      case 186: /* limit_opt ::= LIMIT signed */
      case 190: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==190);
#line 597 "sql.y"
{yygotominor.yy454.limit = yymsp[0].minor.yy109;  yygotominor.yy454.offset = 0;}
#line 2578 "sql.c"
        break;
      case 187: /* limit_opt ::= LIMIT signed OFFSET signed */
#line 599 "sql.y"
{ yygotominor.yy454.limit = yymsp[-2].minor.yy109;  yygotominor.yy454.offset = yymsp[0].minor.yy109;}
#line 2583 "sql.c"
        break;
      case 188: /* limit_opt ::= LIMIT signed COMMA signed */
#line 601 "sql.y"
{ yygotominor.yy454.limit = yymsp[0].minor.yy109;  yygotominor.yy454.offset = yymsp[-2].minor.yy109;}
#line 2588 "sql.c"
        break;
      case 191: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
#line 607 "sql.y"
{yygotominor.yy454.limit = yymsp[-2].minor.yy109;  yygotominor.yy454.offset = yymsp[0].minor.yy109;}
#line 2593 "sql.c"
        break;
      case 192: /* slimit_opt ::= SLIMIT signed COMMA signed */
#line 609 "sql.y"
{yygotominor.yy454.limit = yymsp[0].minor.yy109;  yygotominor.yy454.offset = yymsp[-2].minor.yy109;}
#line 2598 "sql.c"
        break;
      case 195: /* expr ::= LP expr RP */
#line 622 "sql.y"
{yygotominor.yy50 = yymsp[-1].minor.yy50; yygotominor.yy50->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy50->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
#line 2603 "sql.c"
        break;
      case 196: /* expr ::= ID */
#line 624 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
#line 2608 "sql.c"
        break;
      case 197: /* expr ::= ID DOT ID */
#line 625 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
#line 2613 "sql.c"
        break;
      case 198: /* expr ::= ID DOT STAR */
#line 626 "sql.y"
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
#line 2618 "sql.c"
        break;
      case 199: /* expr ::= INTEGER */
#line 628 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
#line 2623 "sql.c"
        break;
      case 200: /* expr ::= MINUS INTEGER */
      case 201: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==201);
#line 629 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
#line 2629 "sql.c"
        break;
      case 202: /* expr ::= FLOAT */
#line 631 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
#line 2634 "sql.c"
        break;
      case 203: /* expr ::= MINUS FLOAT */
      case 204: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==204);
#line 632 "sql.y"
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
#line 2640 "sql.c"
        break;
      case 205: /* expr ::= STRING */
#line 634 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
#line 2645 "sql.c"
        break;
      case 206: /* expr ::= NOW */
#line 635 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
#line 2650 "sql.c"
        break;
      case 207: /* expr ::= VARIABLE */
#line 636 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
#line 2655 "sql.c"
        break;
      case 208: /* expr ::= BOOL */
#line 637 "sql.y"
{ yygotominor.yy50 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
#line 2660 "sql.c"
        break;
      case 209: /* expr ::= ID LP exprlist RP */
#line 640 "sql.y"
{ yygotominor.yy50 = tSqlExprCreateFunction(yymsp[-1].minor.yy178, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 2665 "sql.c"
        break;
      case 210: /* expr ::= ID LP STAR RP */
#line 643 "sql.y"
{ yygotominor.yy50 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
#line 2670 "sql.c"
        break;
      case 211: /* expr ::= expr IS NULL */
#line 646 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, NULL, TK_ISNULL);}
#line 2675 "sql.c"
        break;
      case 212: /* expr ::= expr IS NOT NULL */
#line 647 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-3].minor.yy50, NULL, TK_NOTNULL);}
#line 2680 "sql.c"
        break;
      case 213: /* expr ::= expr LT expr */
#line 650 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_LT);}
#line 2685 "sql.c"
        break;
      case 214: /* expr ::= expr GT expr */
#line 651 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_GT);}
#line 2690 "sql.c"
        break;
      case 215: /* expr ::= expr LE expr */
#line 652 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_LE);}
#line 2695 "sql.c"
        break;
      case 216: /* expr ::= expr GE expr */
#line 653 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_GE);}
#line 2700 "sql.c"
        break;
      case 217: /* expr ::= expr NE expr */
#line 654 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_NE);}
#line 2705 "sql.c"
        break;
      case 218: /* expr ::= expr EQ expr */
#line 655 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_EQ);}
#line 2710 "sql.c"
        break;
      case 219: /* expr ::= expr AND expr */
#line 657 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_AND);}
#line 2715 "sql.c"
        break;
      case 220: /* expr ::= expr OR expr */
#line 658 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_OR); }
#line 2720 "sql.c"
        break;
      case 221: /* expr ::= expr PLUS expr */
#line 661 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_PLUS);  }
#line 2725 "sql.c"
        break;
      case 222: /* expr ::= expr MINUS expr */
#line 662 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_MINUS); }
#line 2730 "sql.c"
        break;
      case 223: /* expr ::= expr STAR expr */
#line 663 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_STAR);  }
#line 2735 "sql.c"
        break;
      case 224: /* expr ::= expr SLASH expr */
#line 664 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_DIVIDE);}
#line 2740 "sql.c"
        break;
      case 225: /* expr ::= expr REM expr */
#line 665 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_REM);   }
#line 2745 "sql.c"
        break;
      case 226: /* expr ::= expr LIKE expr */
#line 668 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-2].minor.yy50, yymsp[0].minor.yy50, TK_LIKE);  }
#line 2750 "sql.c"
        break;
      case 227: /* expr ::= expr IN LP exprlist RP */
#line 671 "sql.y"
{yygotominor.yy50 = tSqlExprCreate(yymsp[-4].minor.yy50, (tSQLExpr*)yymsp[-1].minor.yy178, TK_IN); }
#line 2755 "sql.c"
        break;
      case 228: /* exprlist ::= exprlist COMMA expritem */
#line 679 "sql.y"
{yygotominor.yy178 = tSqlExprListAppend(yymsp[-2].minor.yy178,yymsp[0].minor.yy50,0, 0);}
#line 2760 "sql.c"
        break;
      case 229: /* exprlist ::= expritem */
#line 680 "sql.y"
{yygotominor.yy178 = tSqlExprListAppend(0,yymsp[0].minor.yy50,0, 0);}
#line 2765 "sql.c"
        break;
      case 232: /* cmd ::= RESET QUERY CACHE */
#line 685 "sql.y"
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
#line 2770 "sql.c"
        break;
      case 233: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
#line 688 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy221, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 2779 "sql.c"
        break;
      case 234: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
#line 694 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 2792 "sql.c"
        break;
      case 235: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
#line 705 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy221, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 2801 "sql.c"
        break;
      case 236: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
#line 710 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 2814 "sql.c"
        break;
      case 237: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
#line 720 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 2830 "sql.c"
        break;
      case 238: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
#line 733 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy106, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
#line 2844 "sql.c"
        break;
      case 239: /* cmd ::= KILL CONNECTION INTEGER */
#line 745 "sql.y"
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
#line 2849 "sql.c"
        break;
      case 240: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
#line 746 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
#line 2854 "sql.c"
        break;
      case 241: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
#line 747 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
#line 2859 "sql.c"
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
#line 37 "sql.y"

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
#line 2944 "sql.c"
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
#line 61 "sql.y"
#line 2964 "sql.c"
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
