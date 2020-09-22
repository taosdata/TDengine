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
#define YYNOCODE 274
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy46;
  tSQLExpr* yy64;
  tVariant yy134;
  SCreateAcctSQL yy149;
  int64_t yy207;
  SLimitVal yy216;
  TAOS_FIELD yy223;
  SSubclauseInfo* yy231;
  SCreateDBInfo yy268;
  tSQLExprList* yy290;
  SQuerySQL* yy414;
  SCreateTableSQL* yy470;
  tVariantList* yy498;
  tFieldList* yy523;
  SIntervalVal yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 414
#define YYNRULE 231
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
#define YY_ACTTAB_COUNT (665)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   398,   76,   80,   85,   88,   79,  341,  173,  397,  263,
 /*    10 */   180,   82,   35,   36,   18,   37,   38,  184,  183,  167,
 /*    20 */    29,   87,   86,  202,   41,   39,   43,   40,   10,    9,
 /*    30 */   646,  248,   34,   33,  223,  222,   32,   31,   30,   35,
 /*    40 */    36,    8,   37,   38,   63,  116,  167,   29,  101,   21,
 /*    50 */   202,   41,   39,   43,   40,  245,  244,   97,  414,   34,
 /*    60 */    33,   75,   74,   32,   31,   30,   35,   36,  256,   37,
 /*    70 */    38,  401,  174,  167,   29,  220,  219,  202,   41,   39,
 /*    80 */    43,   40,  187,  226,  207,  337,   34,   33,   22,  398,
 /*    90 */    32,   31,   30,   36,  307,   37,   38,  397,   22,  167,
 /*   100 */    29,  190,   56,  202,   41,   39,   43,   40,   32,   31,
 /*   110 */    30,  361,   34,   33,  363,  362,   32,   31,   30,  360,
 /*   120 */   306,  358,  357,  359,   46,  356,  297,  400,  133,  131,
 /*   130 */    93,   92,   91,  413,  412,  411,  410,  409,  408,  407,
 /*   140 */   406,  405,  404,  403,  402,  247,  375,  138,  374,  106,
 /*   150 */    37,   38,   50,   99,  167,   29,  161,  279,  202,   41,
 /*   160 */    39,   43,   40,  106,  373,  165,  372,   34,   33,   51,
 /*   170 */   203,   32,   31,   30,  311,   98,  323,  322,  321,  320,
 /*   180 */   319,  318,  317,  316,  315,  314,  313,  312,  310,   16,
 /*   190 */   214,  241,  240,  213,  212,  211,  239,  210,  238,  237,
 /*   200 */   236,  209,  235,  166,  285,    3,  270,  294,  197,  289,
 /*   210 */   293,  288,  292,  166,  285,   47,   78,  294,  199,  289,
 /*   220 */    60,  288,  234,  166,  285,   12,  291,  294,  290,  289,
 /*   230 */    46,  288,   21,  255,   48,  163,  164,   34,   33,  257,
 /*   240 */   189,   32,   31,   30,  151,  163,  164,  296,  370,  201,
 /*   250 */    90,   89,  145,  284,  392,  163,  164,   13,  150,  369,
 /*   260 */   299,   41,   39,   43,   40,  368,  221,  138,  337,   34,
 /*   270 */    33,  100,   21,   32,   31,   30,  162,  279,   26,   16,
 /*   280 */    17,  241,  240,  295,  275,  274,  239,   26,  238,  237,
 /*   290 */   236,  280,  235,  377,   21,   13,  380,  398,  379,  266,
 /*   300 */   378,   42,  106,   14,   21,  397,  172,  186,  337,  267,
 /*   310 */   106,   42,  286,   46,  153,  119,  120,   70,   66,   69,
 /*   320 */   225,   42,  286,   62,  175,  176,  242,  287,  171,  265,
 /*   330 */   337,  354,  286,  103,  129,   27,  331,  287,  355,  129,
 /*   340 */   343,  129,  256,   17,  138,  170,  371,  287,  159,  258,
 /*   350 */    26,  338,  156,  246,  278,  367,  157,  366,  365,   61,
 /*   360 */   364,  353,  352,  351,  350,  371,  349,  271,  371,  348,
 /*   370 */   347,  346,   22,   54,  340,  342,  339,   73,   71,   45,
 /*   380 */    68,  330,  215,  329,  328,  327,  326,  325,   65,  206,
 /*   390 */   204,    7,  324,   15,    4,  298,  376,  277,    6,    5,
 /*   400 */   105,  260,   20,   19,  268,  193,  160,  264,  104,  185,
 /*   410 */    55,  253,  252,  251,   59,  182,  181,  250,  179,  189,
 /*   420 */   249,  178,    2,    1,   96,  393,  243,   95,   94,  386,
 /*   430 */   232,  123,  127,  128,  305,  228,  230,  126,  231,  233,
 /*   440 */   124,  125,  229,  152,  122,   53,   67,  205,  216,  344,
 /*   450 */    64,   26,  114,  227,   28,  113,   77,  192,  198,  194,
 /*   460 */    44,  196,  273,   52,   49,  195,  276,   58,   57,  399,
 /*   470 */   396,  647,  395,  132,  394,  391,  390,  112,  389,  111,
 /*   480 */   388,  110,  387,  109,  130,  108,  269,  188,  200,  177,
 /*   490 */   107,  647,  385,  308,  234,  115,  304,  384,   84,  254,
 /*   500 */   224,  647,  647,  158,  191,  647,  647,  647,  647,  272,
 /*   510 */   647,   83,  647,  169,  383,  303,  647,  647,  647,  302,
 /*   520 */   168,  647,  301,  647,   81,  382,  135,  647,   23,   25,
 /*   530 */   345,  121,  336,  335,   72,  334,  218,  647,  332,  217,
 /*   540 */    24,  381,  134,  208,  309,  118,  117,  102,  262,  261,
 /*   550 */   259,  647,  647,  647,  647,  647,  647,  647,  647,  647,
 /*   560 */   647,  647,  647,  647,  647,  647,  647,  647,  647,  647,
 /*   570 */   647,  647,  647,  647,  647,  647,  647,  647,  647,  647,
 /*   580 */   647,  647,  647,  647,  647,  647,  647,  300,  647,  647,
 /*   590 */   647,  647,  647,  647,  647,  647,  647,  647,  647,  647,
 /*   600 */   647,  647,  647,  647,  647,  647,  647,  647,  647,  647,
 /*   610 */   647,  647,  647,  647,  647,  647,  647,  647,  647,  647,
 /*   620 */   647,  647,  647,  333,  647,  647,  647,  647,  647,  647,
 /*   630 */   647,  647,  647,  647,  647,  647,  647,  647,  647,  139,
 /*   640 */   647,  647,  647,  647,  647,  647,  146,  647,  647,  136,
 /*   650 */   147,  149,  148,  144,  143,  141,  140,  155,  154,  283,
 /*   660 */   282,  281,  142,  137,   11,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   64,   65,   66,   67,   68,    5,   63,    9,  104,
 /*    10 */   127,   74,   13,   14,  109,   16,   17,  134,  135,   20,
 /*    20 */    21,   72,   73,   24,   25,   26,   27,   28,  129,  130,
 /*    30 */   208,  209,   33,   34,   33,   34,   37,   38,   39,   13,
 /*    40 */    14,   99,   16,   17,  102,  103,   20,   21,  211,  211,
 /*    50 */    24,   25,   26,   27,   28,   60,   61,   62,    0,   33,
 /*    60 */    34,  129,  130,   37,   38,   39,   13,   14,  246,   16,
 /*    70 */    17,   58,  128,   20,   21,  131,  132,   24,   25,   26,
 /*    80 */    27,   28,  260,  245,  100,  247,   33,   34,  104,    1,
 /*    90 */    37,   38,   39,   14,  100,   16,   17,    9,  104,   20,
 /*   100 */    21,  264,  103,   24,   25,   26,   27,   28,   37,   38,
 /*   110 */    39,  227,   33,   34,  230,  231,   37,   38,   39,  235,
 /*   120 */   100,  237,  238,  239,  104,  241,  100,   59,   64,   65,
 /*   130 */    66,   67,   68,   45,   46,   47,   48,   49,   50,   51,
 /*   140 */    52,   53,   54,   55,   56,   57,    5,  262,    7,  211,
 /*   150 */    16,   17,  104,   21,   20,   21,  271,  272,   24,   25,
 /*   160 */    26,   27,   28,  211,    5,   59,    7,   33,   34,  121,
 /*   170 */    15,   37,   38,   39,  227,   21,  229,  230,  231,  232,
 /*   180 */   233,  234,  235,  236,  237,  238,  239,  240,  241,   85,
 /*   190 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   200 */    96,   97,   98,    1,    2,   99,  268,    5,  270,    7,
 /*   210 */     5,    9,    7,    1,    2,  104,   72,    5,  266,    7,
 /*   220 */   268,    9,   78,    1,    2,   44,    5,    5,    7,    7,
 /*   230 */   104,    9,  211,  100,  123,   33,   34,   33,   34,   37,
 /*   240 */   107,   37,   38,   39,   63,   33,   34,    1,    5,   37,
 /*   250 */    69,   70,   71,  100,   76,   33,   34,  104,   77,    5,
 /*   260 */   105,   25,   26,   27,   28,    5,  245,  262,  247,   33,
 /*   270 */    34,   99,  211,   37,   38,   39,  271,  272,  106,   85,
 /*   280 */    99,   87,   88,   37,  116,  117,   92,  106,   94,   95,
 /*   290 */    96,  100,   98,    2,  211,  104,    5,    1,    7,  100,
 /*   300 */     9,   99,  211,  104,  211,    9,  245,  126,  247,  100,
 /*   310 */   211,   99,  110,  104,  133,   64,   65,   66,   67,   68,
 /*   320 */   211,   99,  110,  249,   33,   34,  228,  125,  245,  100,
 /*   330 */   247,  215,  110,  104,  218,  261,  215,  125,  215,  218,
 /*   340 */   247,  218,  246,   99,  262,  228,  248,  125,  228,  211,
 /*   350 */   106,  242,  210,  211,  272,    5,  260,    5,    5,  268,
 /*   360 */     5,    5,    5,    5,    5,  248,    5,  268,  248,    5,
 /*   370 */     5,    5,  104,   99,    5,  100,    5,  130,  130,   16,
 /*   380 */    72,   76,   15,    5,    5,    5,    5,    5,   72,  101,
 /*   390 */   101,   99,    9,   99,   99,  105,  105,  100,  115,  115,
 /*   400 */    99,  263,  104,  104,  100,   99,    1,  100,   99,  127,
 /*   410 */   108,  100,   86,    5,  104,    5,  136,    5,    5,  107,
 /*   420 */     5,  136,  214,  217,  213,  212,   75,   59,  213,  212,
 /*   430 */    81,  224,  222,  219,  246,   49,   82,  221,   53,   79,
 /*   440 */   220,  223,   80,  212,  225,  212,  216,  212,   75,  226,
 /*   450 */   216,  106,  251,   83,  124,  252,   84,  111,  118,  112,
 /*   460 */   119,  113,  212,  120,  122,  265,  269,  212,  212,  211,
 /*   470 */   211,  273,  211,  211,  211,  211,  211,  253,  211,  254,
 /*   480 */   211,  255,  211,  256,  211,  257,  110,  246,  114,  211,
 /*   490 */   258,  273,  211,  244,   78,  250,  259,  211,  211,  246,
 /*   500 */   243,  273,  273,  265,  265,  273,  273,  273,  273,  269,
 /*   510 */   273,  211,  273,  243,  211,  243,  273,  273,  273,  243,
 /*   520 */   243,  273,  243,  273,  211,  211,  211,  273,  211,  211,
 /*   530 */   211,  211,  211,  211,  211,  211,  211,  273,  211,  211,
 /*   540 */   211,  248,  211,  211,  211,  211,  211,  211,  211,  211,
 /*   550 */   211,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   560 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   570 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   580 */   273,  273,  273,  273,  273,  273,  273,  243,  273,  273,
 /*   590 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   600 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   610 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   620 */   273,  273,  273,  248,  273,  273,  273,  273,  273,  273,
 /*   630 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  262,
 /*   640 */   273,  273,  273,  273,  273,  273,  262,  273,  273,  262,
 /*   650 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   660 */   262,  262,  262,  262,  262,
};
#define YY_SHIFT_USE_DFLT (-118)
#define YY_SHIFT_COUNT (248)
#define YY_SHIFT_MIN   (-117)
#define YY_SHIFT_MAX   (416)
static const short yy_shift_ofst[] = {
 /*     0 */   181,  104,  194,  212,  222,  296,  296,  296,  296,  296,
 /*    10 */   296,   -1,   88,  222,  291,  291,  291,  244,  296,  296,
 /*    20 */   296,  296,  296,  144,  416,  416, -118,  202,  222,  222,
 /*    30 */   222,  222,  222,  222,  222,  222,  222,  222,  222,  222,
 /*    40 */   222,  222,  222,  222,  222,  291,  291,    1,    1,    1,
 /*    50 */     1,    1,    1,  -58,    1,  172,  296,  296,  296,  296,
 /*    60 */   168,  168,  -95,  296,  296,  296,  296,  296,  296,  296,
 /*    70 */   296,  296,  296,  296,  296,  296,  296,  296,  296,  296,
 /*    80 */   296,  296,  296,  296,  296,  296,  296,  296,  296,  296,
 /*    90 */   296,  296,  296,  296,  296,  296,  296,  296,  296,  296,
 /*   100 */   345,  368,  368,  376,  376,  376,  368,  343,  342,  341,
 /*   110 */   374,  340,  348,  347,  346,  330,  345,  368,  368,  373,
 /*   120 */   373,  368,  372,  370,  386,  362,  354,  385,  349,  360,
 /*   130 */   368,  351,  368,  351, -118, -118,   26,   53,   53,   53,
 /*   140 */    79,  134,  236,  236,  236,  -63,  204,  204,  204,  204,
 /*   150 */   251,   64,  -56, -117,   71,   71,   -5,  133,  229,  209,
 /*   160 */   199,  191,  153,  221,  205,  246,  106,  155,  111,   48,
 /*   170 */    20,   -6,  -16,  -68, -101,  159,  141,  -51,  415,  285,
 /*   180 */   413,  412,  280,  410,  408,  326,  282,  312,  311,  302,
 /*   190 */   310,  307,  309,  405,  306,  304,  301,  299,  284,  298,
 /*   200 */   283,  297,  295,  290,  294,  289,  292,  288,  316,  383,
 /*   210 */   382,  381,  380,  379,  378,  305,  367,  308,  363,  248,
 /*   220 */   247,  268,  371,  369,  275,  274,  268,  366,  365,  364,
 /*   230 */   361,  359,  358,  357,  356,  355,  353,  352,  350,  260,
 /*   240 */   254,  243,  126,  178,  154,  132,   68,   13,   58,
};
#define YY_REDUCE_USE_DFLT (-179)
#define YY_REDUCE_COUNT (135)
#define YY_REDUCE_MIN   (-178)
#define YY_REDUCE_MAX   (402)
static const short yy_reduce_ofst[] = {
 /*     0 */  -178,  -53, -116,    5, -115,  -62,  -48,   83,   61,   21,
 /*    10 */  -162,  138,  142,   82,  120,  117,   98,   96, -163,   99,
 /*    20 */    91,  109,   93,  123,  121,  116,   74,  402,  401,  400,
 /*    30 */   399,  398,  397,  396,  395,  394,  393,  392,  391,  390,
 /*    40 */   389,  388,  387,  384,  377,  375,  293,  344,  279,  277,
 /*    50 */   276,  272,  270,  249,  257,  253,  339,  338,  337,  336,
 /*    60 */   240,  197,  245,  335,  334,  333,  332,  331,  329,  328,
 /*    70 */   327,  325,  324,  323,  322,  321,  320,  319,  318,  317,
 /*    80 */   315,  314,  313,  303,  300,  287,  286,  281,  278,  273,
 /*    90 */   271,  269,  267,  265,  264,  263,  262,  261,  259,  258,
 /*   100 */   241,  256,  255,  239,  238,  200,  250,  237,  232,  228,
 /*   110 */   227,  226,  225,  224,  203,  201,  188,  235,  233,  234,
 /*   120 */   230,  231,  223,  219,  207,  220,  218,  216,  210,  214,
 /*   130 */   217,  215,  213,  211,  206,  208,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   645,  463,  452,  634,  634,  645,  645,  645,  645,  645,
 /*    10 */   645,  559,  429,  634,  645,  645,  645,  645,  645,  645,
 /*    20 */   645,  645,  645,  465,  465,  465,  554,  645,  645,  645,
 /*    30 */   645,  645,  645,  645,  645,  645,  645,  645,  645,  645,
 /*    40 */   645,  645,  645,  645,  645,  645,  645,  645,  645,  645,
 /*    50 */   645,  645,  645,  645,  645,  645,  645,  561,  563,  645,
 /*    60 */   581,  581,  552,  645,  645,  645,  645,  645,  645,  645,
 /*    70 */   645,  645,  645,  645,  645,  645,  645,  645,  645,  645,
 /*    80 */   645,  450,  645,  448,  645,  645,  645,  645,  645,  645,
 /*    90 */   645,  645,  645,  645,  645,  645,  645,  437,  645,  645,
 /*   100 */   645,  431,  431,  645,  645,  645,  431,  588,  592,  586,
 /*   110 */   574,  582,  573,  569,  568,  596,  645,  431,  431,  460,
 /*   120 */   460,  431,  481,  479,  477,  469,  475,  471,  473,  467,
 /*   130 */   431,  458,  431,  458,  498,  513,  645,  597,  633,  587,
 /*   140 */   623,  622,  629,  621,  620,  645,  616,  617,  619,  618,
 /*   150 */   645,  645,  645,  645,  625,  624,  645,  645,  645,  645,
 /*   160 */   645,  645,  645,  645,  645,  645,  599,  645,  593,  589,
 /*   170 */   645,  645,  645,  645,  645,  645,  645,  645,  645,  645,
 /*   180 */   645,  645,  645,  645,  645,  645,  645,  551,  645,  645,
 /*   190 */   560,  645,  645,  645,  645,  645,  645,  583,  645,  575,
 /*   200 */   645,  645,  645,  645,  645,  645,  645,  528,  645,  645,
 /*   210 */   645,  645,  645,  645,  645,  645,  645,  645,  645,  645,
 /*   220 */   645,  638,  645,  645,  645,  522,  636,  645,  645,  645,
 /*   230 */   645,  645,  645,  645,  645,  645,  645,  645,  645,  645,
 /*   240 */   645,  645,  484,  645,  435,  433,  645,  427,  645,  644,
 /*   250 */   643,  642,  635,  550,  549,  548,  547,  556,  558,  557,
 /*   260 */   555,  562,  564,  553,  567,  566,  571,  570,  572,  565,
 /*   270 */   585,  584,  577,  578,  580,  579,  576,  613,  631,  632,
 /*   280 */   630,  628,  627,  626,  612,  611,  610,  609,  608,  605,
 /*   290 */   607,  604,  606,  603,  602,  601,  600,  598,  615,  614,
 /*   300 */   595,  594,  591,  590,  546,  531,  530,  529,  527,  464,
 /*   310 */   512,  511,  510,  509,  508,  507,  506,  505,  504,  503,
 /*   320 */   502,  501,  500,  499,  496,  492,  490,  489,  488,  485,
 /*   330 */   459,  462,  461,  641,  640,  639,  637,  533,  534,  526,
 /*   340 */   525,  524,  523,  532,  483,  482,  480,  478,  470,  476,
 /*   350 */   472,  474,  468,  466,  454,  453,  521,  520,  519,  518,
 /*   360 */   517,  516,  515,  514,  497,  495,  494,  493,  491,  487,
 /*   370 */   486,  536,  545,  544,  543,  542,  541,  540,  539,  538,
 /*   380 */   537,  535,  451,  449,  447,  446,  445,  444,  443,  442,
 /*   390 */   441,  440,  457,  439,  432,  438,  436,  456,  455,  434,
 /*   400 */   430,  428,  426,  425,  424,  423,  422,  421,  420,  419,
 /*   410 */   418,  417,  416,  415,
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
  "TABLES",        "STABLES",       "VGROUPS",       "DROP",        
  "TABLE",         "DATABASE",      "DNODE",         "USER",        
  "ACCOUNT",       "USE",           "DESCRIBE",      "ALTER",       
  "PASS",          "PRIVILEGE",     "LOCAL",         "IF",          
  "EXISTS",        "CREATE",        "PPS",           "TSERIES",     
  "DBS",           "STORAGE",       "QTIME",         "CONNS",       
  "STATE",         "KEEP",          "CACHE",         "REPLICA",     
  "QUORUM",        "DAYS",          "MINROWS",       "MAXROWS",     
  "BLOCKS",        "CTIME",         "WAL",           "FSYNC",       
  "COMP",          "PRECISION",     "UPDATE",        "LP",          
  "RP",            "TAGS",          "USING",         "AS",          
  "COMMA",         "NULL",          "SELECT",        "UNION",       
  "ALL",           "FROM",          "VARIABLE",      "INTERVAL",    
  "FILL",          "SLIDING",       "ORDER",         "BY",          
  "ASC",           "DESC",          "GROUP",         "HAVING",      
  "LIMIT",         "OFFSET",        "SLIMIT",        "SOFFSET",     
  "WHERE",         "NOW",           "RESET",         "QUERY",       
  "ADD",           "COLUMN",        "TAG",           "CHANGE",      
  "SET",           "KILL",          "CONNECTION",    "STREAM",      
  "COLON",         "ABORT",         "AFTER",         "ATTACH",      
  "BEFORE",        "BEGIN",         "CASCADE",       "CLUSTER",     
  "CONFLICT",      "COPY",          "DEFERRED",      "DELIMITERS",  
  "DETACH",        "EACH",          "END",           "EXPLAIN",     
  "FAIL",          "FOR",           "IGNORE",        "IMMEDIATE",   
  "INITIALLY",     "INSTEAD",       "MATCH",         "KEY",         
  "OF",            "RAISE",         "REPLACE",       "RESTRICT",    
  "ROW",           "STATEMENT",     "TRIGGER",       "VIEW",        
  "COUNT",         "SUM",           "AVG",           "MIN",         
  "MAX",           "FIRST",         "LAST",          "TOP",         
  "BOTTOM",        "STDDEV",        "PERCENTILE",    "APERCENTILE", 
  "LEASTSQUARES",  "HISTOGRAM",     "DIFF",          "SPREAD",      
  "TWA",           "INTERP",        "LAST_ROW",      "RATE",        
  "IRATE",         "SUM_RATE",      "SUM_IRATE",     "AVG_RATE",    
  "AVG_IRATE",     "TBID",          "SEMI",          "NONE",        
  "PREV",          "LINEAR",        "IMPORT",        "METRIC",      
  "TBNAME",        "JOIN",          "METRICS",       "STABLE",      
  "INSERT",        "INTO",          "VALUES",        "error",       
  "program",       "cmd",           "dbPrefix",      "ids",         
  "cpxName",       "ifexists",      "alter_db_optr",  "acct_optr",   
  "ifnotexists",   "db_optr",       "pps",           "tseries",     
  "dbs",           "streams",       "storage",       "qtime",       
  "users",         "conns",         "state",         "keep",        
  "tagitemlist",   "cache",         "replica",       "quorum",      
  "days",          "minrows",       "maxrows",       "blocks",      
  "ctime",         "wal",           "fsync",         "comp",        
  "prec",          "update",        "typename",      "signed",      
  "create_table_args",  "columnlist",    "select",        "column",      
  "tagitem",       "selcollist",    "from",          "where_opt",   
  "interval_opt",  "fill_opt",      "sliding_opt",   "groupby_opt", 
  "orderby_opt",   "having_opt",    "slimit_opt",    "limit_opt",   
  "union",         "sclp",          "expr",          "as",          
  "tablelist",     "tmvar",         "sortlist",      "sortitem",    
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
 /*  71 */ "cache ::= CACHE INTEGER",
 /*  72 */ "replica ::= REPLICA INTEGER",
 /*  73 */ "quorum ::= QUORUM INTEGER",
 /*  74 */ "days ::= DAYS INTEGER",
 /*  75 */ "minrows ::= MINROWS INTEGER",
 /*  76 */ "maxrows ::= MAXROWS INTEGER",
 /*  77 */ "blocks ::= BLOCKS INTEGER",
 /*  78 */ "ctime ::= CTIME INTEGER",
 /*  79 */ "wal ::= WAL INTEGER",
 /*  80 */ "fsync ::= FSYNC INTEGER",
 /*  81 */ "comp ::= COMP INTEGER",
 /*  82 */ "prec ::= PRECISION STRING",
 /*  83 */ "update ::= UPDATE INTEGER",
 /*  84 */ "db_optr ::=",
 /*  85 */ "db_optr ::= db_optr cache",
 /*  86 */ "db_optr ::= db_optr replica",
 /*  87 */ "db_optr ::= db_optr quorum",
 /*  88 */ "db_optr ::= db_optr days",
 /*  89 */ "db_optr ::= db_optr minrows",
 /*  90 */ "db_optr ::= db_optr maxrows",
 /*  91 */ "db_optr ::= db_optr blocks",
 /*  92 */ "db_optr ::= db_optr ctime",
 /*  93 */ "db_optr ::= db_optr wal",
 /*  94 */ "db_optr ::= db_optr fsync",
 /*  95 */ "db_optr ::= db_optr comp",
 /*  96 */ "db_optr ::= db_optr prec",
 /*  97 */ "db_optr ::= db_optr keep",
 /*  98 */ "db_optr ::= db_optr update",
 /*  99 */ "alter_db_optr ::=",
 /* 100 */ "alter_db_optr ::= alter_db_optr replica",
 /* 101 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 102 */ "alter_db_optr ::= alter_db_optr keep",
 /* 103 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 104 */ "alter_db_optr ::= alter_db_optr comp",
 /* 105 */ "alter_db_optr ::= alter_db_optr wal",
 /* 106 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 107 */ "alter_db_optr ::= alter_db_optr update",
 /* 108 */ "typename ::= ids",
 /* 109 */ "typename ::= ids LP signed RP",
 /* 110 */ "signed ::= INTEGER",
 /* 111 */ "signed ::= PLUS INTEGER",
 /* 112 */ "signed ::= MINUS INTEGER",
 /* 113 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 114 */ "create_table_args ::= LP columnlist RP",
 /* 115 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 116 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 117 */ "create_table_args ::= AS select",
 /* 118 */ "columnlist ::= columnlist COMMA column",
 /* 119 */ "columnlist ::= column",
 /* 120 */ "column ::= ids typename",
 /* 121 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 122 */ "tagitemlist ::= tagitem",
 /* 123 */ "tagitem ::= INTEGER",
 /* 124 */ "tagitem ::= FLOAT",
 /* 125 */ "tagitem ::= STRING",
 /* 126 */ "tagitem ::= BOOL",
 /* 127 */ "tagitem ::= NULL",
 /* 128 */ "tagitem ::= MINUS INTEGER",
 /* 129 */ "tagitem ::= MINUS FLOAT",
 /* 130 */ "tagitem ::= PLUS INTEGER",
 /* 131 */ "tagitem ::= PLUS FLOAT",
 /* 132 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 133 */ "union ::= select",
 /* 134 */ "union ::= LP union RP",
 /* 135 */ "union ::= union UNION ALL select",
 /* 136 */ "union ::= union UNION ALL LP select RP",
 /* 137 */ "cmd ::= union",
 /* 138 */ "select ::= SELECT selcollist",
 /* 139 */ "sclp ::= selcollist COMMA",
 /* 140 */ "sclp ::=",
 /* 141 */ "selcollist ::= sclp expr as",
 /* 142 */ "selcollist ::= sclp STAR",
 /* 143 */ "as ::= AS ids",
 /* 144 */ "as ::= ids",
 /* 145 */ "as ::=",
 /* 146 */ "from ::= FROM tablelist",
 /* 147 */ "tablelist ::= ids cpxName",
 /* 148 */ "tablelist ::= ids cpxName ids",
 /* 149 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 150 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 151 */ "tmvar ::= VARIABLE",
 /* 152 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 153 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 154 */ "interval_opt ::=",
 /* 155 */ "fill_opt ::=",
 /* 156 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 157 */ "fill_opt ::= FILL LP ID RP",
 /* 158 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 159 */ "sliding_opt ::=",
 /* 160 */ "orderby_opt ::=",
 /* 161 */ "orderby_opt ::= ORDER BY sortlist",
 /* 162 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 163 */ "sortlist ::= item sortorder",
 /* 164 */ "item ::= ids cpxName",
 /* 165 */ "sortorder ::= ASC",
 /* 166 */ "sortorder ::= DESC",
 /* 167 */ "sortorder ::=",
 /* 168 */ "groupby_opt ::=",
 /* 169 */ "groupby_opt ::= GROUP BY grouplist",
 /* 170 */ "grouplist ::= grouplist COMMA item",
 /* 171 */ "grouplist ::= item",
 /* 172 */ "having_opt ::=",
 /* 173 */ "having_opt ::= HAVING expr",
 /* 174 */ "limit_opt ::=",
 /* 175 */ "limit_opt ::= LIMIT signed",
 /* 176 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 177 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 178 */ "slimit_opt ::=",
 /* 179 */ "slimit_opt ::= SLIMIT signed",
 /* 180 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 181 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 182 */ "where_opt ::=",
 /* 183 */ "where_opt ::= WHERE expr",
 /* 184 */ "expr ::= LP expr RP",
 /* 185 */ "expr ::= ID",
 /* 186 */ "expr ::= ID DOT ID",
 /* 187 */ "expr ::= ID DOT STAR",
 /* 188 */ "expr ::= INTEGER",
 /* 189 */ "expr ::= MINUS INTEGER",
 /* 190 */ "expr ::= PLUS INTEGER",
 /* 191 */ "expr ::= FLOAT",
 /* 192 */ "expr ::= MINUS FLOAT",
 /* 193 */ "expr ::= PLUS FLOAT",
 /* 194 */ "expr ::= STRING",
 /* 195 */ "expr ::= NOW",
 /* 196 */ "expr ::= VARIABLE",
 /* 197 */ "expr ::= BOOL",
 /* 198 */ "expr ::= ID LP exprlist RP",
 /* 199 */ "expr ::= ID LP STAR RP",
 /* 200 */ "expr ::= expr IS NULL",
 /* 201 */ "expr ::= expr IS NOT NULL",
 /* 202 */ "expr ::= expr LT expr",
 /* 203 */ "expr ::= expr GT expr",
 /* 204 */ "expr ::= expr LE expr",
 /* 205 */ "expr ::= expr GE expr",
 /* 206 */ "expr ::= expr NE expr",
 /* 207 */ "expr ::= expr EQ expr",
 /* 208 */ "expr ::= expr AND expr",
 /* 209 */ "expr ::= expr OR expr",
 /* 210 */ "expr ::= expr PLUS expr",
 /* 211 */ "expr ::= expr MINUS expr",
 /* 212 */ "expr ::= expr STAR expr",
 /* 213 */ "expr ::= expr SLASH expr",
 /* 214 */ "expr ::= expr REM expr",
 /* 215 */ "expr ::= expr LIKE expr",
 /* 216 */ "expr ::= expr IN LP exprlist RP",
 /* 217 */ "exprlist ::= exprlist COMMA expritem",
 /* 218 */ "exprlist ::= expritem",
 /* 219 */ "expritem ::= expr",
 /* 220 */ "expritem ::=",
 /* 221 */ "cmd ::= RESET QUERY CACHE",
 /* 222 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 223 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 224 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 225 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 226 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 227 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 228 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 229 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 230 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 227: /* keep */
    case 228: /* tagitemlist */
    case 253: /* fill_opt */
    case 255: /* groupby_opt */
    case 256: /* orderby_opt */
    case 266: /* sortlist */
    case 270: /* grouplist */
{
tVariantListDestroy((yypminor->yy498));
}
      break;
    case 245: /* columnlist */
{
tFieldListDestroy((yypminor->yy523));
}
      break;
    case 246: /* select */
{
doDestroyQuerySql((yypminor->yy414));
}
      break;
    case 249: /* selcollist */
    case 261: /* sclp */
    case 271: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy290));
}
      break;
    case 251: /* where_opt */
    case 257: /* having_opt */
    case 262: /* expr */
    case 272: /* expritem */
{
tSQLExprDestroy((yypminor->yy64));
}
      break;
    case 260: /* union */
{
destroyAllSelectClause((yypminor->yy231));
}
      break;
    case 267: /* sortitem */
{
tVariantDestroy(&(yypminor->yy134));
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
  { 208, 1 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 3 },
  { 210, 0 },
  { 210, 2 },
  { 212, 0 },
  { 212, 2 },
  { 209, 3 },
  { 209, 5 },
  { 209, 3 },
  { 209, 5 },
  { 209, 3 },
  { 209, 4 },
  { 209, 5 },
  { 209, 4 },
  { 209, 3 },
  { 209, 3 },
  { 209, 3 },
  { 209, 2 },
  { 209, 3 },
  { 209, 5 },
  { 209, 5 },
  { 209, 4 },
  { 209, 5 },
  { 209, 3 },
  { 209, 4 },
  { 209, 4 },
  { 209, 4 },
  { 209, 6 },
  { 211, 1 },
  { 211, 1 },
  { 213, 2 },
  { 213, 0 },
  { 216, 3 },
  { 216, 0 },
  { 209, 3 },
  { 209, 6 },
  { 209, 5 },
  { 209, 5 },
  { 218, 0 },
  { 218, 2 },
  { 219, 0 },
  { 219, 2 },
  { 220, 0 },
  { 220, 2 },
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
  { 215, 9 },
  { 227, 2 },
  { 229, 2 },
  { 230, 2 },
  { 231, 2 },
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
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 214, 0 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 242, 1 },
  { 242, 4 },
  { 243, 1 },
  { 243, 2 },
  { 243, 2 },
  { 209, 6 },
  { 244, 3 },
  { 244, 7 },
  { 244, 7 },
  { 244, 2 },
  { 245, 3 },
  { 245, 1 },
  { 247, 2 },
  { 228, 3 },
  { 228, 1 },
  { 248, 1 },
  { 248, 1 },
  { 248, 1 },
  { 248, 1 },
  { 248, 1 },
  { 248, 2 },
  { 248, 2 },
  { 248, 2 },
  { 248, 2 },
  { 246, 12 },
  { 260, 1 },
  { 260, 3 },
  { 260, 4 },
  { 260, 6 },
  { 209, 1 },
  { 246, 2 },
  { 261, 2 },
  { 261, 0 },
  { 249, 3 },
  { 249, 2 },
  { 263, 2 },
  { 263, 1 },
  { 263, 0 },
  { 250, 2 },
  { 264, 2 },
  { 264, 3 },
  { 264, 4 },
  { 264, 5 },
  { 265, 1 },
  { 252, 4 },
  { 252, 6 },
  { 252, 0 },
  { 253, 0 },
  { 253, 6 },
  { 253, 4 },
  { 254, 4 },
  { 254, 0 },
  { 256, 0 },
  { 256, 3 },
  { 266, 4 },
  { 266, 2 },
  { 268, 2 },
  { 269, 1 },
  { 269, 1 },
  { 269, 0 },
  { 255, 0 },
  { 255, 3 },
  { 270, 3 },
  { 270, 1 },
  { 257, 0 },
  { 257, 2 },
  { 259, 0 },
  { 259, 2 },
  { 259, 4 },
  { 259, 4 },
  { 258, 0 },
  { 258, 2 },
  { 258, 4 },
  { 258, 4 },
  { 251, 0 },
  { 251, 2 },
  { 262, 3 },
  { 262, 1 },
  { 262, 3 },
  { 262, 3 },
  { 262, 1 },
  { 262, 2 },
  { 262, 2 },
  { 262, 1 },
  { 262, 2 },
  { 262, 2 },
  { 262, 1 },
  { 262, 1 },
  { 262, 1 },
  { 262, 1 },
  { 262, 4 },
  { 262, 4 },
  { 262, 3 },
  { 262, 4 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 5 },
  { 271, 3 },
  { 271, 1 },
  { 272, 1 },
  { 272, 0 },
  { 209, 3 },
  { 209, 7 },
  { 209, 7 },
  { 209, 7 },
  { 209, 7 },
  { 209, 8 },
  { 209, 9 },
  { 209, 3 },
  { 209, 5 },
  { 209, 5 },
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
{yygotominor.yy0.n = 0; yygotominor.yy0.type = 0;}
        break;
      case 16: /* dbPrefix ::= ids DOT */
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
        break;
      case 17: /* cpxName ::= */
{yygotominor.yy0.n = 0;  }
        break;
      case 18: /* cpxName ::= DOT ids */
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
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
    SStrToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
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
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy149);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 41: /* ids ::= ID */
      case 42: /* ids ::= STRING */ yytestcase(yyruleno==42);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 43: /* ifexists ::= IF EXISTS */
      case 45: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==45);
{yygotominor.yy0.n = 1;}
        break;
      case 44: /* ifexists ::= */
      case 46: /* ifnotexists ::= */ yytestcase(yyruleno==46);
{yygotominor.yy0.n = 0;}
        break;
      case 47: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 48: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &yymsp[-2].minor.yy0);}
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
{yygotominor.yy0.n = 0;   }
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
{yygotominor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 69: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yygotominor.yy149.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy149.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy149.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy149.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy149.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy149.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy149.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy149.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy149.stat    = yymsp[0].minor.yy0;
}
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy498 = yymsp[0].minor.yy498; }
        break;
      case 71: /* cache ::= CACHE INTEGER */
      case 72: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==72);
      case 73: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==73);
      case 74: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==74);
      case 75: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==75);
      case 76: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==78);
      case 79: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==79);
      case 80: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==80);
      case 81: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==81);
      case 82: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==82);
      case 83: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==83);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 84: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy268);}
        break;
      case 85: /* db_optr ::= db_optr cache */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 86: /* db_optr ::= db_optr replica */
      case 100: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==100);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 87: /* db_optr ::= db_optr quorum */
      case 101: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==101);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 88: /* db_optr ::= db_optr days */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 89: /* db_optr ::= db_optr minrows */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 90: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 91: /* db_optr ::= db_optr blocks */
      case 103: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==103);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 92: /* db_optr ::= db_optr ctime */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 93: /* db_optr ::= db_optr wal */
      case 105: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==105);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 94: /* db_optr ::= db_optr fsync */
      case 106: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==106);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 95: /* db_optr ::= db_optr comp */
      case 104: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==104);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 96: /* db_optr ::= db_optr prec */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.precision = yymsp[0].minor.yy0; }
        break;
      case 97: /* db_optr ::= db_optr keep */
      case 102: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==102);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.keep = yymsp[0].minor.yy498; }
        break;
      case 98: /* db_optr ::= db_optr update */
      case 107: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==107);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy268);}
        break;
      case 108: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSQLSetColumnType (&yygotominor.yy223, &yymsp[0].minor.yy0); 
}
        break;
      case 109: /* typename ::= ids LP signed RP */
{
    if (yymsp[-1].minor.yy207 <= 0) {
      yymsp[-3].minor.yy0.type = 0;
      tSQLSetColumnType(&yygotominor.yy223, &yymsp[-3].minor.yy0);
    } else {
      yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy207;          // negative value of name length
      tSQLSetColumnType(&yygotominor.yy223, &yymsp[-3].minor.yy0);
    }
}
        break;
      case 110: /* signed ::= INTEGER */
      case 111: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==111);
{ yygotominor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 112: /* signed ::= MINUS INTEGER */
{ yygotominor.yy207 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 113: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 114: /* create_table_args ::= LP columnlist RP */
{
    yygotominor.yy470 = tSetCreateSQLElems(yymsp[-1].minor.yy523, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 115: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yygotominor.yy470 = tSetCreateSQLElems(yymsp[-5].minor.yy523, yymsp[-1].minor.yy523, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 116: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yygotominor.yy470 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy498, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 117: /* create_table_args ::= AS select */
{
    yygotominor.yy470 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy414, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 118: /* columnlist ::= columnlist COMMA column */
{yygotominor.yy523 = tFieldListAppend(yymsp[-2].minor.yy523, &yymsp[0].minor.yy223);   }
        break;
      case 119: /* columnlist ::= column */
{yygotominor.yy523 = tFieldListAppend(NULL, &yymsp[0].minor.yy223);}
        break;
      case 120: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yygotominor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
        break;
      case 121: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy498 = tVariantListAppend(yymsp[-2].minor.yy498, &yymsp[0].minor.yy134, -1);    }
        break;
      case 122: /* tagitemlist ::= tagitem */
{ yygotominor.yy498 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1); }
        break;
      case 123: /* tagitem ::= INTEGER */
      case 124: /* tagitem ::= FLOAT */ yytestcase(yyruleno==124);
      case 125: /* tagitem ::= STRING */ yytestcase(yyruleno==125);
      case 126: /* tagitem ::= BOOL */ yytestcase(yyruleno==126);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy134, &yymsp[0].minor.yy0); }
        break;
      case 127: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy134, &yymsp[0].minor.yy0); }
        break;
      case 128: /* tagitem ::= MINUS INTEGER */
      case 129: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==129);
      case 130: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==130);
      case 131: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==131);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy134, &yymsp[-1].minor.yy0);
}
        break;
      case 132: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy414 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy290, yymsp[-9].minor.yy498, yymsp[-8].minor.yy64, yymsp[-4].minor.yy498, yymsp[-3].minor.yy498, &yymsp[-7].minor.yy532, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy498, &yymsp[0].minor.yy216, &yymsp[-1].minor.yy216);
}
        break;
      case 133: /* union ::= select */
{ yygotominor.yy231 = setSubclause(NULL, yymsp[0].minor.yy414); }
        break;
      case 134: /* union ::= LP union RP */
{ yygotominor.yy231 = yymsp[-1].minor.yy231; }
        break;
      case 135: /* union ::= union UNION ALL select */
{ yygotominor.yy231 = appendSelectClause(yymsp[-3].minor.yy231, yymsp[0].minor.yy414); }
        break;
      case 136: /* union ::= union UNION ALL LP select RP */
{ yygotominor.yy231 = appendSelectClause(yymsp[-5].minor.yy231, yymsp[-1].minor.yy414); }
        break;
      case 137: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy231, NULL, TSDB_SQL_SELECT); }
        break;
      case 138: /* select ::= SELECT selcollist */
{
  yygotominor.yy414 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy290, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 139: /* sclp ::= selcollist COMMA */
{yygotominor.yy290 = yymsp[-1].minor.yy290;}
        break;
      case 140: /* sclp ::= */
{yygotominor.yy290 = 0;}
        break;
      case 141: /* selcollist ::= sclp expr as */
{
   yygotominor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290, yymsp[-1].minor.yy64, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 142: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yygotominor.yy290 = tSQLExprListAppend(yymsp[-1].minor.yy290, pNode, 0);
}
        break;
      case 143: /* as ::= AS ids */
      case 144: /* as ::= ids */ yytestcase(yyruleno==144);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 145: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 146: /* from ::= FROM tablelist */
      case 161: /* orderby_opt ::= ORDER BY sortlist */ yytestcase(yyruleno==161);
      case 169: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==169);
{yygotominor.yy498 = yymsp[0].minor.yy498;}
        break;
      case 147: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy498 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[-1].minor.yy0, -1);  // table alias name
}
        break;
      case 148: /* tablelist ::= ids cpxName ids */
{
   toTSDBType(yymsp[-2].minor.yy0.type);
   toTSDBType(yymsp[0].minor.yy0.type);
   yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
   yygotominor.yy498 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
   yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[0].minor.yy0, -1);
}
        break;
      case 149: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy498 = tVariantListAppendToken(yymsp[-3].minor.yy498, &yymsp[-1].minor.yy0, -1);
  yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 150: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
   toTSDBType(yymsp[-2].minor.yy0.type);
   toTSDBType(yymsp[0].minor.yy0.type);
   yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
   yygotominor.yy498 = tVariantListAppendToken(yymsp[-4].minor.yy498, &yymsp[-2].minor.yy0, -1);
   yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[0].minor.yy0, -1);
}
        break;
      case 151: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 152: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy532.interval = yymsp[-1].minor.yy0; yygotominor.yy532.offset.n = 0; yygotominor.yy532.offset.z = NULL; yygotominor.yy532.offset.type = 0;}
        break;
      case 153: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy532.interval = yymsp[-3].minor.yy0; yygotominor.yy532.offset = yymsp[-1].minor.yy0;}
        break;
      case 154: /* interval_opt ::= */
{memset(&yygotominor.yy532, 0, sizeof(yygotominor.yy532));}
        break;
      case 155: /* fill_opt ::= */
{yygotominor.yy498 = 0;     }
        break;
      case 156: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy498, &A, -1, 0);
    yygotominor.yy498 = yymsp[-1].minor.yy498;
}
        break;
      case 157: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy498 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 158: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 159: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 160: /* orderby_opt ::= */
      case 168: /* groupby_opt ::= */ yytestcase(yyruleno==168);
{yygotominor.yy498 = 0;}
        break;
      case 162: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy498 = tVariantListAppend(yymsp[-3].minor.yy498, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
        break;
      case 163: /* sortlist ::= item sortorder */
{
  yygotominor.yy498 = tVariantListAppend(NULL, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
        break;
      case 164: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy134, &yymsp[-1].minor.yy0);
}
        break;
      case 165: /* sortorder ::= ASC */
{yygotominor.yy46 = TSDB_ORDER_ASC; }
        break;
      case 166: /* sortorder ::= DESC */
{yygotominor.yy46 = TSDB_ORDER_DESC;}
        break;
      case 167: /* sortorder ::= */
{yygotominor.yy46 = TSDB_ORDER_ASC;}
        break;
      case 170: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy498 = tVariantListAppend(yymsp[-2].minor.yy498, &yymsp[0].minor.yy134, -1);
}
        break;
      case 171: /* grouplist ::= item */
{
  yygotominor.yy498 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1);
}
        break;
      case 172: /* having_opt ::= */
      case 182: /* where_opt ::= */ yytestcase(yyruleno==182);
      case 220: /* expritem ::= */ yytestcase(yyruleno==220);
{yygotominor.yy64 = 0;}
        break;
      case 173: /* having_opt ::= HAVING expr */
      case 183: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==183);
      case 219: /* expritem ::= expr */ yytestcase(yyruleno==219);
{yygotominor.yy64 = yymsp[0].minor.yy64;}
        break;
      case 174: /* limit_opt ::= */
      case 178: /* slimit_opt ::= */ yytestcase(yyruleno==178);
{yygotominor.yy216.limit = -1; yygotominor.yy216.offset = 0;}
        break;
      case 175: /* limit_opt ::= LIMIT signed */
      case 179: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==179);
{yygotominor.yy216.limit = yymsp[0].minor.yy207;  yygotominor.yy216.offset = 0;}
        break;
      case 176: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 180: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==180);
{yygotominor.yy216.limit = yymsp[-2].minor.yy207;  yygotominor.yy216.offset = yymsp[0].minor.yy207;}
        break;
      case 177: /* limit_opt ::= LIMIT signed COMMA signed */
      case 181: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==181);
{yygotominor.yy216.limit = yymsp[0].minor.yy207;  yygotominor.yy216.offset = yymsp[-2].minor.yy207;}
        break;
      case 184: /* expr ::= LP expr RP */
{yygotominor.yy64 = yymsp[-1].minor.yy64; }
        break;
      case 185: /* expr ::= ID */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 186: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 187: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 188: /* expr ::= INTEGER */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 189: /* expr ::= MINUS INTEGER */
      case 190: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==190);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 191: /* expr ::= FLOAT */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 192: /* expr ::= MINUS FLOAT */
      case 193: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==193);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 194: /* expr ::= STRING */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 195: /* expr ::= NOW */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 196: /* expr ::= VARIABLE */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 197: /* expr ::= BOOL */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 198: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy64 = tSQLExprCreateFunction(yymsp[-1].minor.yy290, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 199: /* expr ::= ID LP STAR RP */
{ yygotominor.yy64 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 200: /* expr ::= expr IS NULL */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, NULL, TK_ISNULL);}
        break;
      case 201: /* expr ::= expr IS NOT NULL */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-3].minor.yy64, NULL, TK_NOTNULL);}
        break;
      case 202: /* expr ::= expr LT expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LT);}
        break;
      case 203: /* expr ::= expr GT expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GT);}
        break;
      case 204: /* expr ::= expr LE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LE);}
        break;
      case 205: /* expr ::= expr GE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GE);}
        break;
      case 206: /* expr ::= expr NE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_NE);}
        break;
      case 207: /* expr ::= expr EQ expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_EQ);}
        break;
      case 208: /* expr ::= expr AND expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_AND);}
        break;
      case 209: /* expr ::= expr OR expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_OR); }
        break;
      case 210: /* expr ::= expr PLUS expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_PLUS);  }
        break;
      case 211: /* expr ::= expr MINUS expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_MINUS); }
        break;
      case 212: /* expr ::= expr STAR expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_STAR);  }
        break;
      case 213: /* expr ::= expr SLASH expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_DIVIDE);}
        break;
      case 214: /* expr ::= expr REM expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_REM);   }
        break;
      case 215: /* expr ::= expr LIKE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LIKE);  }
        break;
      case 216: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-4].minor.yy64, (tSQLExpr*)yymsp[-1].minor.yy290, TK_IN); }
        break;
      case 217: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290,yymsp[0].minor.yy64,0);}
        break;
      case 218: /* exprlist ::= expritem */
{yygotominor.yy290 = tSQLExprListAppend(0,yymsp[0].minor.yy64,0);}
        break;
      case 221: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 222: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy523, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 223: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 224: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy523, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 225: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 226: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 227: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 228: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 229: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 230: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
