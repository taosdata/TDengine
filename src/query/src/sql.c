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
#define YYNOCODE 267
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  TAOS_FIELD yy27;
  SWindowStateVal yy76;
  SCreateDbInfo yy114;
  SSqlNode* yy124;
  SCreateAcctInfo yy183;
  SCreatedTableInfo yy192;
  SArray* yy193;
  SCreateTableSql* yy270;
  int yy312;
  SRelationInfo* yy332;
  SIntervalVal yy392;
  tVariant yy442;
  SSessionWindowVal yy447;
  tSqlExpr* yy454;
  int64_t yy473;
  SLimitVal yy482;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 523
#define YYNRULE 275
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
#define YY_ACTTAB_COUNT (811)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   506,   47,   46,  523,   31,   45,   44,   43,  505,  242,
 /*    10 */   799,  326,   48,   49,  345,   52,   53,  246,  245,  223,
 /*    20 */    42,    8,   51,  274,   56,   54,   58,   55,  136,  134,
 /*    30 */   133,  398,   47,   46,  138,   61,   45,   44,   43,   48,
 /*    40 */    49,  509,   52,   53,   31,  302,  223,   42,  435,   51,
 /*    50 */   274,   56,   54,   58,   55,  234,  342,  119,  118,   47,
 /*    60 */    46,   17,   16,   45,   44,   43,   48,   49,   29,   52,
 /*    70 */    53,  280,  250,  223,   42,  383,   51,  274,   56,   54,
 /*    80 */    58,   55,  323,  322,  130,  298,   47,   46,  435,  508,
 /*    90 */    45,   44,   43,   48,   50,  497,   52,   53,  257,  256,
 /*   100 */   223,   42,  506,   51,  274,   56,   54,   58,   55,   71,
 /*   110 */   505,  382,  463,   47,   46,  465,  464,   45,   44,   43,
 /*   120 */   462,  221,  460,  459,  461,  236,  458,  457,  297,  296,
 /*   130 */   406,  384,  418,  417,  416,  415,  414,  413,  412,  411,
 /*   140 */   410,  409,  408,  407,  405,  404,  522,  521,  520,  519,
 /*   150 */   518,  517,  516,  515,  514,  513,  512,  511,  510,  325,
 /*   160 */    62,   49,  212,   52,   53,    6,  232,  223,   42,   86,
 /*   170 */    51,  274,   56,   54,   58,   55,  132,  380,  145,  379,
 /*   180 */    47,   46,  506,   63,   45,   44,   43,   52,   53,  279,
 /*   190 */   505,  223,   42,   87,   51,  274,   56,   54,   58,   55,
 /*   200 */   401,  400,   30,  394,   47,   46,  131,   13,   45,   44,
 /*   210 */    43,   88,   31,   85,  397,   45,   44,   43,   61,   24,
 /*   220 */   288,  319,  318,  287,  286,  285,  317,  284,  316,  315,
 /*   230 */   314,  283,  313,  312,  222,  370,  233,   89,  381,  292,
 /*   240 */   374,  377,  373,  376,  222,  370,   31,  354,  381,  269,
 /*   250 */   374,  440,  373,  294,  106,  105,  435,   31,   15,   14,
 /*   260 */    56,   54,   58,   55,   19,   65,  207,  208,   47,   46,
 /*   270 */   273,  399,   45,   44,   43,   31,  207,  208,  275,  300,
 /*   280 */   299,  197,  478,  506,  477,   25,   66,  293,  198,  378,
 /*   290 */   435,  505,  188,  122,  121,  196,  289,   24,  227,  319,
 /*   300 */   318,  435,  219,  363,  317,   31,  316,  315,  314,   61,
 /*   310 */   313,  312,  473,  107,  101,  112,  226,  145,  472,  435,
 /*   320 */   111,  117,  120,  110,   77,  471,  180,  178,  176,  114,
 /*   330 */   100,   99,   37,  175,  125,  124,  123,   57,  480,  334,
 /*   340 */   476,  483,  475,  482,  369,  481,  213,   57,  188,  435,
 /*   350 */   371,  656,  277,  375,  369,  249,   32,   69,  220,  363,
 /*   360 */   371,  393,   68,  204,  437,   32,  145,  372,   84,  237,
 /*   370 */   238,    5,   34,  162,  386,  359,  358,  372,  161,   96,
 /*   380 */    91,   95,   72,  368,  271,  364,   82,   20,  350,   20,
 /*   390 */   351,  347,   21,   75,   61,  140,   74,   70,  251,   77,
 /*   400 */    31,  253,   32,  109,  253,    3,  173,   37,  310,  453,
 /*   410 */     1,  160,  426,  172,  454,  235,  172,  333,  172,  395,
 /*   420 */   145,  342,  342,  188,  335,  320,  470,  230,  469,  332,
 /*   430 */   290,  468,  228,  217,  362,   83,  467,  215,  214,  211,
 /*   440 */   324,  466,  456,  452,  442,  451,  450,  479,  449,  448,
 /*   450 */   436,  474,  447,  474,  446,  445,   32,  441,  474,  474,
 /*   460 */   439,  229,  438,  104,  102,   98,   94,   60,  425,  424,
 /*   470 */   423,  422,  421,  420,   90,   88,   23,  278,  419,   22,
 /*   480 */   276,   12,  385,    7,   11,  361,   10,  337,  144,  355,
 /*   490 */    28,   27,  352,  265,  218,   81,  253,  349,   80,   76,
 /*   500 */   348,  141,  142,  331,  346,   79,   26,  330,  343,  255,
 /*   510 */     9,  329,  248,  244,  243,  328,  241,  327,    2,  135,
 /*   520 */   240,    4,  504,  503,  321,  129,  498,  455,  128,  307,
 /*   530 */   496,  311,  127,  126,  489,  309,  308,  170,  304,  306,
 /*   540 */   290,  305,  171,  108,  403,  155,  169,  167,  303,  168,
 /*   550 */   203,  210,  166,   93,  165,   41,  281,  443,  231,   92,
 /*   560 */   259,  151,  209,  154,  152,  262,  270,  800,  264,  267,
 /*   570 */   153,  357,  260,  216,  258,  800,  800,  800,  800,  261,
 /*   580 */   800,  266,  800,  800,  800,  268,  800,  800,  800,  800,
 /*   590 */   353,  800,  301,  272,  150,  800,  149,   78,   73,   59,
 /*   600 */   148,  252,  147,   64,  182,  146,   67,  507,  181,  391,
 /*   610 */   502,  501,  500,  800,  800,  800,  499,  179,  800,  177,
 /*   620 */   800,  495,  494,  800,  493,  492,  491,  490,  360,  800,
 /*   630 */   800,  800,  800,  800,  800,  800,  392,  800,  310,  800,
 /*   640 */   800,  225,  800,  800,  254,  341,  156,  800,  800,  800,
 /*   650 */   800,  800,  800,  800,  800,  800,  800,  800,  800,  800,
 /*   660 */   800,  800,  800,  800,   40,  800,  174,  800,  239,  800,
 /*   670 */   488,  487,  116,  115,  486,  113,  485,  184,   39,   33,
 /*   680 */   800,   36,  444,  164,  434,  800,  433,  103,  432,  295,
 /*   690 */   163,  430,  429,   97,  428,  427,  291,   35,  800,  183,
 /*   700 */   800,   38,  800,  800,  800,  282,  402,  159,  158,  396,
 /*   710 */   157,  800,  263,  143,  139,  344,  340,  339,  338,  336,
 /*   720 */   137,  247,  800,  800,  800,  800,  800,  800,  800,  800,
 /*   730 */   800,  800,  800,  800,  800,  800,  800,  800,  800,  800,
 /*   740 */   800,  800,  800,  800,  800,  800,  800,  800,  800,  800,
 /*   750 */   800,  800,  800,  800,  800,  800,  800,  800,  390,  389,
 /*   760 */   224,  388,  387,  800,  800,  800,  800,  800,  800,  800,
 /*   770 */   800,  800,  484,  431,  800,  800,  800,  800,  800,  800,
 /*   780 */   800,  356,  800,  800,  800,  800,  800,  800,  800,  800,
 /*   790 */   800,  189,  199,  185,  200,  202,  201,  195,  194,  187,
 /*   800 */   193,  191,  190,  206,  205,  367,  366,  365,  192,  186,
 /*   810 */    18,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,    0,  192,   37,   38,   39,    9,  136,
 /*    10 */   189,  190,   13,   14,  109,   16,   17,  144,  145,   20,
 /*    20 */    21,  116,   23,   24,   25,   26,   27,   28,   62,   63,
 /*    30 */    64,  105,   33,   34,  192,  109,   37,   38,   39,   13,
 /*    40 */    14,   59,   16,   17,  192,  233,   20,   21,  236,   23,
 /*    50 */    24,   25,   26,   27,   28,   68,  235,   76,   77,   33,
 /*    60 */    34,  139,  140,   37,   38,   39,   13,   14,  104,   16,
 /*    70 */    17,  107,  251,   20,   21,    1,   23,   24,   25,   26,
 /*    80 */    27,   28,   65,   66,   67,  233,   33,   34,  236,   60,
 /*    90 */    37,   38,   39,   13,   14,   80,   16,   17,  256,  257,
 /*   100 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  110,
 /*   110 */     9,   37,  210,   33,   34,  213,  214,   37,   38,   39,
 /*   120 */   218,   60,  220,  221,  222,  138,  224,  225,  141,  142,
 /*   130 */   210,  105,  212,  213,  214,  215,  216,  217,  218,  219,
 /*   140 */   220,  221,  222,  223,  224,  225,   45,   46,   47,   48,
 /*   150 */    49,   50,   51,   52,   53,   54,   55,   56,   57,   58,
 /*   160 */   109,   14,   61,   16,   17,  104,   68,   20,   21,  198,
 /*   170 */    23,   24,   25,   26,   27,   28,   21,    5,  192,    7,
 /*   180 */    33,   34,    1,  132,   37,   38,   39,   16,   17,  105,
 /*   190 */     9,   20,   21,  109,   23,   24,   25,   26,   27,   28,
 /*   200 */   229,  230,  231,  232,   33,   34,   21,  104,   37,   38,
 /*   210 */    39,  108,  192,  110,  105,   37,   38,   39,  109,   88,
 /*   220 */    89,   90,   91,   92,   93,   94,   95,   96,   97,   98,
 /*   230 */    99,  100,  101,  102,    1,    2,  138,  198,    5,  141,
 /*   240 */     7,    5,    9,    7,    1,    2,  192,  261,    5,  263,
 /*   250 */     7,    5,    9,  233,  139,  140,  236,  192,  139,  140,
 /*   260 */    25,   26,   27,   28,   44,  109,   33,   34,   33,   34,
 /*   270 */    37,  232,   37,   38,   39,  192,   33,   34,   15,   33,
 /*   280 */    34,   61,    5,    1,    7,  104,  130,  233,   68,  117,
 /*   290 */   236,    9,  254,   73,   74,   75,   15,   88,  233,   90,
 /*   300 */    91,  236,  264,  265,   95,  192,   97,   98,   99,  109,
 /*   310 */   101,  102,    5,   62,   63,   64,  233,  192,    5,  236,
 /*   320 */    69,   70,   71,   72,  104,    5,   62,   63,   64,   78,
 /*   330 */   139,  140,  112,   69,   70,   71,   72,  104,    2,   37,
 /*   340 */     5,    5,    7,    7,  111,    9,  233,  104,  254,  236,
 /*   350 */   117,    0,  105,  117,  111,  135,  109,  137,  264,  265,
 /*   360 */   117,  105,  104,  143,  106,  109,  192,  134,  238,   33,
 /*   370 */    34,   62,   63,   64,  111,  125,  126,  134,   69,   70,
 /*   380 */    71,   72,  252,  105,  259,  105,  261,  109,  105,  109,
 /*   390 */   105,  105,  109,  105,  109,  109,  105,  198,  105,  104,
 /*   400 */   192,  113,  109,   76,  113,  195,  196,  112,   81,  197,
 /*   410 */   199,  200,  197,  201,  197,  192,  201,  115,  201,  192,
 /*   420 */   192,  235,  235,  254,  192,  211,    5,  211,    5,  230,
 /*   430 */    79,    5,  211,  211,  265,  261,    5,  251,  251,  191,
 /*   440 */   192,    5,    5,    5,  236,    5,    5,  111,    5,    5,
 /*   450 */   227,  237,    5,  237,    5,    5,  109,  105,  237,  237,
 /*   460 */     5,  234,    5,  140,  140,  140,   76,   16,   80,    5,
 /*   470 */     5,    5,    5,    5,   76,  108,  104,  107,    9,  104,
 /*   480 */   107,  104,  111,  104,  124,  105,  124,  255,  104,  261,
 /*   490 */   109,  109,  105,  104,    1,  104,  113,  105,  104,  114,
 /*   500 */   105,  104,  109,   89,  105,  109,  104,   90,  105,  109,
 /*   510 */   104,    5,  136,    5,  146,    5,    5,    5,  199,   60,
 /*   520 */   146,  195,  193,  193,   79,  194,  193,  226,  194,   54,
 /*   530 */   193,  103,  194,  194,  193,   82,   84,  205,   50,   85,
 /*   540 */    79,   83,  202,   87,  226,  240,  204,  203,   86,  206,
 /*   550 */   193,  193,  207,  198,  208,  133,  193,  209,  193,  198,
 /*   560 */   118,  244,  193,  241,  243,  119,  127,  266,  120,  258,
 /*   570 */   242,  193,  258,  258,  258,  266,  266,  266,  266,  193,
 /*   580 */   266,  121,  266,  266,  266,  122,  266,  266,  266,  266,
 /*   590 */   117,  266,  228,  123,  245,  266,  246,  193,  193,  128,
 /*   600 */   247,  193,  248,  131,  192,  249,  129,  192,  192,  250,
 /*   610 */   192,  192,  192,  266,  266,  266,  192,  192,  266,  192,
 /*   620 */   266,  192,  192,  266,  192,  192,  192,  192,  262,  266,
 /*   630 */   266,  266,  266,  266,  266,  266,  235,  266,   81,  266,
 /*   640 */   266,  228,  266,  266,  235,  235,  239,  266,  266,  266,
 /*   650 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   660 */   266,  266,  266,  266,  253,  266,  192,  266,  192,  266,
 /*   670 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   680 */   266,  192,  192,  192,  192,  266,  192,  192,  192,  192,
 /*   690 */   192,  192,  192,  192,  192,  192,  192,  192,  266,  192,
 /*   700 */   266,  192,  266,  266,  266,  192,  192,  192,  192,  192,
 /*   710 */   192,  266,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   720 */   192,  192,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   730 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   740 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   750 */   266,  266,  266,  266,  266,  266,  266,  266,  228,  228,
 /*   760 */   228,  228,  228,  266,  266,  266,  266,  266,  266,  266,
 /*   770 */   266,  266,  237,  237,  266,  266,  266,  266,  266,  266,
 /*   780 */   266,  262,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   790 */   266,  254,  254,  254,  254,  254,  254,  254,  254,  254,
 /*   800 */   254,  254,  254,  254,  254,  254,  254,  254,  254,  254,
 /*   810 */   254,
};
#define YY_SHIFT_USE_DFLT (-128)
#define YY_SHIFT_COUNT (326)
#define YY_SHIFT_MIN   (-127)
#define YY_SHIFT_MAX   (557)
static const short yy_shift_ofst[] = {
 /*     0 */   220,  131,  131,  209,  209,  461,  233,  243,  181,  282,
 /*    10 */   282,  282,  282,  282,  282,  282,  282,  282,   -1,  101,
 /*    20 */   243,  336,  336,  336,  336,  295,  295,  282,  282,  282,
 /*    30 */   351,  282,  282,  327,  461,  557,  557, -128, -128, -128,
 /*    40 */   243,  243,  243,  243,  243,  243,  243,  243,  243,  243,
 /*    50 */   243,  243,  243,  243,  243,  243,  243,  243,  243,  243,
 /*    60 */   336,  336,  246,  246,  246,  246,  246,  246,  246,  282,
 /*    70 */   282,  282,  302,  282,  282,  282,  295,  295,  282,  282,
 /*    80 */   282,  282,  250,  250,  -95,  295,  282,  282,  282,  282,
 /*    90 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   100 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   110 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   120 */   282,  282,  282,  282,  282,  282,  282,  282,  282,  282,
 /*   130 */   282,  282,  282,  282,  282,  282,  282,  459,  459,  459,
 /*   140 */   473,  473,  473,  459,  473,  459,  477,  472,  471,  470,
 /*   150 */   439,  463,  460,  448,  446,  442,  422,  459,  459,  459,
 /*   160 */   428,  461,  461,  459,  459,  456,  462,  488,  458,  454,
 /*   170 */   475,  452,  453,  428,  459,  445,  445,  459,  445,  459,
 /*   180 */   445,  459,  459, -128, -128,   26,   53,   80,   53,   53,
 /*   190 */   147,  171,  235,  235,  235,  235,  251,  309,  264,  -32,
 /*   200 */   -32,  -32,  -32,  -13, -127,  178,  178,  236,  172,  103,
 /*   210 */    98,   17,  -34,  293,  291,  288,  286,  285,  283,  280,
 /*   220 */   278,   74,   61,  263,   51,  156,  256,  247,  109,   84,
 /*   230 */   -74,  -36,  191,  119,  115,  258,  -78,  335,  277,  -19,
 /*   240 */   512,  374,  511,  510,  368,  508,  506,  417,  414,  376,
 /*   250 */   383,  373,  406,  385,  403,  402,  400,  396,  399,  397,
 /*   260 */   395,  393,  394,  392,  391,  493,  389,  387,  384,  382,
 /*   270 */   362,  381,  360,  380,  379,  371,  377,  373,  375,  370,
 /*   280 */   372,  367,  398,  469,  468,  467,  466,  465,  464,  388,
 /*   290 */   281,  390,  325,  347,  347,  451,  324,  323,  347,  457,
 /*   300 */   455,  352,  347,  450,  449,  447,  444,  443,  441,  440,
 /*   310 */   438,  437,  436,  431,  426,  423,  421,  320,  313,  307,
 /*   320 */   200,   15,  185,  155,   29,  -18,    3,
};
#define YY_REDUCE_USE_DFLT (-189)
#define YY_REDUCE_COUNT (184)
#define YY_REDUCE_MIN   (-188)
#define YY_REDUCE_MAX   (556)
static const short yy_reduce_ofst[] = {
 /*     0 */  -179,  -80,  -80,  -98,  -98,  -29,   94,   38, -158,  113,
 /*    10 */   -14,  125,   83,   65,   54,   20, -148, -188,  232,  248,
 /*    20 */   169,  222,  221,  216,  214,  187,  186,  228,  174,  227,
 /*    30 */    39,  223,  208,  217,  199,  215,  212,  130,  211,  210,
 /*    40 */   556,  555,  554,  553,  552,  551,  550,  549,  548,  547,
 /*    50 */   546,  545,  544,  543,  542,  541,  540,  539,  538,  537,
 /*    60 */   536,  535,  534,  533,  532,  531,  530,  413,  364,  529,
 /*    70 */   528,  527,  411,  526,  525,  524,  410,  409,  523,  522,
 /*    80 */   521,  520,  519,  366,  407,  401,  518,  517,  516,  515,
 /*    90 */   514,  513,  509,  507,  505,  504,  503,  502,  501,  500,
 /*   100 */   499,  498,  497,  496,  495,  494,  492,  491,  490,  489,
 /*   110 */   487,  486,  485,  484,  483,  482,  481,  480,  479,  478,
 /*   120 */   476,  474,  435,  434,  433,  432,  430,  429,  427,  425,
 /*   130 */   424,  420,  419,  418,  416,  415,  412,  408,  405,  404,
 /*   140 */   316,  315,  314,  386,  311,  378,  359,  356,  354,  353,
 /*   150 */   350,  349,  317,  321,  328,  322,  305,  369,  365,  363,
 /*   160 */   318,  361,  355,  358,  357,  348,  346,  345,  344,  343,
 /*   170 */   342,  332,  340,  301,  341,  339,  338,  337,  334,  333,
 /*   180 */   331,  330,  329,  319,  326,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   798,  633,  579,  645,  567,  576,  781,  781,  798,  798,
 /*    10 */   798,  798,  798,  798,  798,  798,  798,  798,  692,  539,
 /*    20 */   781,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*    30 */   576,  798,  798,  582,  576,  582,  582,  687,  617,  635,
 /*    40 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*    50 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*    60 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*    70 */   798,  798,  694,  700,  697,  798,  798,  798,  702,  798,
 /*    80 */   798,  798,  724,  724,  685,  798,  798,  798,  798,  798,
 /*    90 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   100 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   110 */   798,  798,  798,  565,  798,  563,  798,  798,  798,  798,
 /*   120 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   130 */   550,  798,  798,  798,  798,  798,  798,  541,  541,  541,
 /*   140 */   798,  798,  798,  541,  798,  541,  731,  735,  729,  717,
 /*   150 */   725,  716,  712,  710,  708,  707,  739,  541,  541,  541,
 /*   160 */   580,  576,  576,  541,  541,  598,  596,  594,  586,  592,
 /*   170 */   588,  590,  584,  568,  541,  574,  574,  541,  574,  541,
 /*   180 */   574,  541,  541,  617,  635,  798,  740,  798,  780,  730,
 /*   190 */   770,  769,  776,  768,  767,  766,  798,  798,  798,  762,
 /*   200 */   763,  765,  764,  798,  798,  772,  771,  798,  798,  798,
 /*   210 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   220 */   798,  798,  742,  798,  736,  732,  798,  798,  798,  798,
 /*   230 */   798,  798,  798,  798,  798,  647,  798,  798,  798,  798,
 /*   240 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   250 */   684,  798,  798,  798,  798,  798,  696,  695,  798,  798,
 /*   260 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  726,
 /*   270 */   798,  718,  798,  798,  798,  798,  798,  659,  798,  798,
 /*   280 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   290 */   798,  798,  798,  792,  790,  798,  798,  798,  786,  798,
 /*   300 */   798,  798,  784,  798,  798,  798,  798,  798,  798,  798,
 /*   310 */   798,  798,  798,  798,  798,  798,  798,  798,  798,  798,
 /*   320 */   601,  798,  548,  546,  798,  537,  798,  797,  796,  795,
 /*   330 */   783,  782,  655,  693,  689,  691,  690,  688,  701,  698,
 /*   340 */   699,  683,  682,  681,  703,  686,  706,  705,  709,  711,
 /*   350 */   714,  713,  715,  704,  728,  727,  720,  721,  723,  722,
 /*   360 */   719,  759,  778,  779,  777,  775,  774,  773,  758,  757,
 /*   370 */   756,  753,  752,  751,  748,  754,  750,  747,  755,  749,
 /*   380 */   746,  745,  744,  743,  741,  761,  760,  738,  737,  734,
 /*   390 */   733,  680,  665,  660,  657,  664,  663,  662,  661,  658,
 /*   400 */   654,  653,  581,  634,  632,  631,  630,  629,  628,  627,
 /*   410 */   626,  625,  624,  623,  622,  621,  620,  619,  618,  613,
 /*   420 */   609,  607,  606,  605,  602,  575,  578,  577,  794,  793,
 /*   430 */   791,  789,  788,  787,  785,  667,  668,  649,  652,  651,
 /*   440 */   650,  648,  666,  600,  599,  597,  595,  587,  593,  589,
 /*   450 */   591,  585,  583,  570,  569,  646,  616,  644,  643,  642,
 /*   460 */   641,  640,  639,  638,  637,  636,  615,  614,  612,  611,
 /*   470 */   610,  608,  604,  603,  670,  679,  678,  677,  676,  675,
 /*   480 */   674,  673,  672,  671,  669,  566,  564,  562,  561,  560,
 /*   490 */   559,  558,  557,  556,  555,  554,  553,  573,  552,  551,
 /*   500 */   549,  547,  545,  544,  543,  572,  571,  542,  540,  538,
 /*   510 */   536,  535,  534,  533,  532,  531,  530,  529,  528,  527,
 /*   520 */   526,  525,  524,
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
  "KEEP",          "CACHE",         "REPLICA",       "QUORUM",      
  "DAYS",          "MINROWS",       "MAXROWS",       "BLOCKS",      
  "CTIME",         "WAL",           "FSYNC",         "COMP",        
  "PRECISION",     "UPDATE",        "CACHELAST",     "PARTITIONS",  
  "LP",            "RP",            "UNSIGNED",      "TAGS",        
  "USING",         "COMMA",         "AS",            "NULL",        
  "SELECT",        "UNION",         "ALL",           "DISTINCT",    
  "FROM",          "VARIABLE",      "INTERVAL",      "SESSION",     
  "STATE_WINDOW",  "FILL",          "SLIDING",       "ORDER",       
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
  "alter_topic_optr",  "acct_optr",     "ifnotexists",   "db_optr",     
  "topic_optr",    "pps",           "tseries",       "dbs",         
  "streams",       "storage",       "qtime",         "users",       
  "conns",         "state",         "keep",          "tagitemlist", 
  "cache",         "replica",       "quorum",        "days",        
  "minrows",       "maxrows",       "blocks",        "ctime",       
  "wal",           "fsync",         "comp",          "prec",        
  "update",        "cachelast",     "partitions",    "typename",    
  "signed",        "create_table_args",  "create_stable_args",  "create_table_list",
  "create_from_stable",  "columnlist",    "tagNamelist",   "select",      
  "column",        "tagitem",       "selcollist",    "from",        
  "where_opt",     "interval_opt",  "session_option",  "windowstate_option",
  "fill_opt",      "sliding_opt",   "groupby_opt",   "orderby_opt", 
  "having_opt",    "slimit_opt",    "limit_opt",     "union",       
  "sclp",          "distinct",      "expr",          "as",          
  "tablelist",     "sub",           "tmvar",         "sortlist",    
  "sortitem",      "item",          "sortorder",     "grouplist",   
  "exprlist",      "expritem",    
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
 /* 173 */ "from ::= FROM sub",
 /* 174 */ "sub ::= LP union RP",
 /* 175 */ "sub ::= LP union RP ids",
 /* 176 */ "sub ::= sub COMMA LP union RP ids",
 /* 177 */ "tablelist ::= ids cpxName",
 /* 178 */ "tablelist ::= ids cpxName ids",
 /* 179 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 180 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 181 */ "tmvar ::= VARIABLE",
 /* 182 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 183 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 184 */ "interval_opt ::=",
 /* 185 */ "session_option ::=",
 /* 186 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 187 */ "windowstate_option ::=",
 /* 188 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 189 */ "fill_opt ::=",
 /* 190 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 191 */ "fill_opt ::= FILL LP ID RP",
 /* 192 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 193 */ "sliding_opt ::=",
 /* 194 */ "orderby_opt ::=",
 /* 195 */ "orderby_opt ::= ORDER BY sortlist",
 /* 196 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 197 */ "sortlist ::= item sortorder",
 /* 198 */ "item ::= ids cpxName",
 /* 199 */ "sortorder ::= ASC",
 /* 200 */ "sortorder ::= DESC",
 /* 201 */ "sortorder ::=",
 /* 202 */ "groupby_opt ::=",
 /* 203 */ "groupby_opt ::= GROUP BY grouplist",
 /* 204 */ "grouplist ::= grouplist COMMA item",
 /* 205 */ "grouplist ::= item",
 /* 206 */ "having_opt ::=",
 /* 207 */ "having_opt ::= HAVING expr",
 /* 208 */ "limit_opt ::=",
 /* 209 */ "limit_opt ::= LIMIT signed",
 /* 210 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 211 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 212 */ "slimit_opt ::=",
 /* 213 */ "slimit_opt ::= SLIMIT signed",
 /* 214 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 215 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 216 */ "where_opt ::=",
 /* 217 */ "where_opt ::= WHERE expr",
 /* 218 */ "expr ::= LP expr RP",
 /* 219 */ "expr ::= ID",
 /* 220 */ "expr ::= ID DOT ID",
 /* 221 */ "expr ::= ID DOT STAR",
 /* 222 */ "expr ::= INTEGER",
 /* 223 */ "expr ::= MINUS INTEGER",
 /* 224 */ "expr ::= PLUS INTEGER",
 /* 225 */ "expr ::= FLOAT",
 /* 226 */ "expr ::= MINUS FLOAT",
 /* 227 */ "expr ::= PLUS FLOAT",
 /* 228 */ "expr ::= STRING",
 /* 229 */ "expr ::= NOW",
 /* 230 */ "expr ::= VARIABLE",
 /* 231 */ "expr ::= PLUS VARIABLE",
 /* 232 */ "expr ::= MINUS VARIABLE",
 /* 233 */ "expr ::= BOOL",
 /* 234 */ "expr ::= NULL",
 /* 235 */ "expr ::= ID LP exprlist RP",
 /* 236 */ "expr ::= ID LP STAR RP",
 /* 237 */ "expr ::= expr IS NULL",
 /* 238 */ "expr ::= expr IS NOT NULL",
 /* 239 */ "expr ::= expr LT expr",
 /* 240 */ "expr ::= expr GT expr",
 /* 241 */ "expr ::= expr LE expr",
 /* 242 */ "expr ::= expr GE expr",
 /* 243 */ "expr ::= expr NE expr",
 /* 244 */ "expr ::= expr EQ expr",
 /* 245 */ "expr ::= expr BETWEEN expr AND expr",
 /* 246 */ "expr ::= expr AND expr",
 /* 247 */ "expr ::= expr OR expr",
 /* 248 */ "expr ::= expr PLUS expr",
 /* 249 */ "expr ::= expr MINUS expr",
 /* 250 */ "expr ::= expr STAR expr",
 /* 251 */ "expr ::= expr SLASH expr",
 /* 252 */ "expr ::= expr REM expr",
 /* 253 */ "expr ::= expr LIKE expr",
 /* 254 */ "expr ::= expr IN LP exprlist RP",
 /* 255 */ "exprlist ::= exprlist COMMA expritem",
 /* 256 */ "exprlist ::= expritem",
 /* 257 */ "expritem ::= expr",
 /* 258 */ "expritem ::=",
 /* 259 */ "cmd ::= RESET QUERY CACHE",
 /* 260 */ "cmd ::= SYNCDB ids REPLICA",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 267 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 268 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 269 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 270 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 271 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 272 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 273 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 274 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 210: /* keep */
    case 211: /* tagitemlist */
    case 233: /* columnlist */
    case 234: /* tagNamelist */
    case 244: /* fill_opt */
    case 246: /* groupby_opt */
    case 247: /* orderby_opt */
    case 259: /* sortlist */
    case 263: /* grouplist */
{
taosArrayDestroy((yypminor->yy193));
}
      break;
    case 231: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy270));
}
      break;
    case 235: /* select */
{
destroySqlNode((yypminor->yy124));
}
      break;
    case 238: /* selcollist */
    case 252: /* sclp */
    case 264: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy193));
}
      break;
    case 239: /* from */
    case 256: /* tablelist */
    case 257: /* sub */
{
destroyRelationInfo((yypminor->yy332));
}
      break;
    case 240: /* where_opt */
    case 248: /* having_opt */
    case 254: /* expr */
    case 265: /* expritem */
{
tSqlExprDestroy((yypminor->yy454));
}
      break;
    case 251: /* union */
{
destroyAllSqlNode((yypminor->yy193));
}
      break;
    case 260: /* sortitem */
{
tVariantDestroy(&(yypminor->yy442));
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
  { 192, 1 },
  { 192, 1 },
  { 194, 2 },
  { 194, 0 },
  { 198, 3 },
  { 198, 0 },
  { 190, 3 },
  { 190, 6 },
  { 190, 5 },
  { 190, 5 },
  { 190, 5 },
  { 201, 0 },
  { 201, 2 },
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
  { 197, 9 },
  { 210, 2 },
  { 212, 2 },
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
  { 199, 0 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 199, 2 },
  { 200, 1 },
  { 200, 2 },
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
  { 227, 1 },
  { 227, 4 },
  { 227, 2 },
  { 228, 1 },
  { 228, 2 },
  { 228, 2 },
  { 190, 3 },
  { 190, 3 },
  { 190, 3 },
  { 190, 3 },
  { 231, 1 },
  { 231, 2 },
  { 229, 6 },
  { 230, 10 },
  { 232, 10 },
  { 232, 13 },
  { 234, 3 },
  { 234, 1 },
  { 229, 5 },
  { 233, 3 },
  { 233, 1 },
  { 236, 2 },
  { 211, 3 },
  { 211, 1 },
  { 237, 1 },
  { 237, 1 },
  { 237, 1 },
  { 237, 1 },
  { 237, 1 },
  { 237, 2 },
  { 237, 2 },
  { 237, 2 },
  { 237, 2 },
  { 235, 14 },
  { 235, 3 },
  { 251, 1 },
  { 251, 4 },
  { 190, 1 },
  { 235, 2 },
  { 252, 2 },
  { 252, 0 },
  { 238, 4 },
  { 238, 2 },
  { 255, 2 },
  { 255, 1 },
  { 255, 0 },
  { 253, 1 },
  { 253, 0 },
  { 239, 2 },
  { 239, 2 },
  { 257, 3 },
  { 257, 4 },
  { 257, 6 },
  { 256, 2 },
  { 256, 3 },
  { 256, 4 },
  { 256, 5 },
  { 258, 1 },
  { 241, 4 },
  { 241, 6 },
  { 241, 0 },
  { 242, 0 },
  { 242, 7 },
  { 243, 0 },
  { 243, 4 },
  { 244, 0 },
  { 244, 6 },
  { 244, 4 },
  { 245, 4 },
  { 245, 0 },
  { 247, 0 },
  { 247, 3 },
  { 259, 4 },
  { 259, 2 },
  { 261, 2 },
  { 262, 1 },
  { 262, 1 },
  { 262, 0 },
  { 246, 0 },
  { 246, 3 },
  { 263, 3 },
  { 263, 1 },
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
  { 240, 0 },
  { 240, 2 },
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
  { 264, 3 },
  { 264, 1 },
  { 265, 1 },
  { 265, 0 },
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy114, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy183);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);}
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
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yygotominor.yy0.n = 0;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy114, &yymsp[-2].minor.yy0);}
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
    yygotominor.yy183.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy183.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy183.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy183.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy183.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy183.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy183.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy183.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy183.stat    = yymsp[0].minor.yy0;
}
        break;
      case 78: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy193 = yymsp[0].minor.yy193; }
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
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 94: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy114); yygotominor.yy114.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 95: /* db_optr ::= db_optr cache */
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 96: /* db_optr ::= db_optr replica */
      case 113: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==113);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 97: /* db_optr ::= db_optr quorum */
      case 114: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==114);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 98: /* db_optr ::= db_optr days */
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* db_optr ::= db_optr minrows */
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 100: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 101: /* db_optr ::= db_optr blocks */
      case 116: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==116);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 102: /* db_optr ::= db_optr ctime */
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 103: /* db_optr ::= db_optr wal */
      case 118: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==118);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 104: /* db_optr ::= db_optr fsync */
      case 119: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==119);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 105: /* db_optr ::= db_optr comp */
      case 117: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==117);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* db_optr ::= db_optr prec */
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.precision = yymsp[0].minor.yy0; }
        break;
      case 107: /* db_optr ::= db_optr keep */
      case 115: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==115);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.keep = yymsp[0].minor.yy193; }
        break;
      case 108: /* db_optr ::= db_optr update */
      case 120: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==120);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 109: /* db_optr ::= db_optr cachelast */
      case 121: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==121);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 110: /* topic_optr ::= db_optr */
      case 122: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==122);
{ yygotominor.yy114 = yymsp[0].minor.yy114; yygotominor.yy114.dbType = TSDB_DB_TYPE_TOPIC; }
        break;
      case 111: /* topic_optr ::= topic_optr partitions */
      case 123: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==123);
{ yygotominor.yy114 = yymsp[-1].minor.yy114; yygotominor.yy114.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 112: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy114); yygotominor.yy114.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 124: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yygotominor.yy27, &yymsp[0].minor.yy0);
}
        break;
      case 125: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy473 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yygotominor.yy27, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy473;  // negative value of name length
    tSetColumnType(&yygotominor.yy27, &yymsp[-3].minor.yy0);
  }
}
        break;
      case 126: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yygotominor.yy27, &yymsp[-1].minor.yy0);
}
        break;
      case 127: /* signed ::= INTEGER */
      case 128: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==128);
{ yygotominor.yy473 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 129: /* signed ::= MINUS INTEGER */
      case 130: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==132);
{ yygotominor.yy473 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 133: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy270;}
        break;
      case 134: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy192);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yygotominor.yy270 = pCreateTable;
}
        break;
      case 135: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy270->childTableInfo, &yymsp[0].minor.yy192);
  yygotominor.yy270 = yymsp[-1].minor.yy270;
}
        break;
      case 136: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yygotominor.yy270 = tSetCreateTableInfo(yymsp[-1].minor.yy193, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yygotominor.yy270, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
        break;
      case 137: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yygotominor.yy270 = tSetCreateTableInfo(yymsp[-5].minor.yy193, yymsp[-1].minor.yy193, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yygotominor.yy270, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yygotominor.yy192 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy193, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yygotominor.yy192 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy193, yymsp[-1].minor.yy193, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
        break;
      case 140: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy193, &yymsp[0].minor.yy0); yygotominor.yy193 = yymsp[-2].minor.yy193;  }
        break;
      case 141: /* tagNamelist ::= ids */
{yygotominor.yy193 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yygotominor.yy193, &yymsp[0].minor.yy0);}
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yygotominor.yy270 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy124, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yygotominor.yy270, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
        break;
      case 143: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy193, &yymsp[0].minor.yy27); yygotominor.yy193 = yymsp[-2].minor.yy193;  }
        break;
      case 144: /* columnlist ::= column */
{yygotominor.yy193 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yygotominor.yy193, &yymsp[0].minor.yy27);}
        break;
      case 145: /* column ::= ids typename */
{
  tSetColumnInfo(&yygotominor.yy27, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy27);
}
        break;
      case 146: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy193 = tVariantListAppend(yymsp[-2].minor.yy193, &yymsp[0].minor.yy442, -1);    }
        break;
      case 147: /* tagitemlist ::= tagitem */
{ yygotominor.yy193 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
        break;
      case 148: /* tagitem ::= INTEGER */
      case 149: /* tagitem ::= FLOAT */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= STRING */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= BOOL */ yytestcase(yyruleno==151);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy442, &yymsp[0].minor.yy0); }
        break;
      case 152: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy442, &yymsp[0].minor.yy0); }
        break;
      case 153: /* tagitem ::= MINUS INTEGER */
      case 154: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy442, &yymsp[-1].minor.yy0);
}
        break;
      case 157: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy124 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy193, yymsp[-11].minor.yy332, yymsp[-10].minor.yy454, yymsp[-4].minor.yy193, yymsp[-3].minor.yy193, &yymsp[-9].minor.yy392, &yymsp[-8].minor.yy447, &yymsp[-7].minor.yy76, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy193, &yymsp[0].minor.yy482, &yymsp[-1].minor.yy482, yymsp[-2].minor.yy454);
}
        break;
      case 158: /* select ::= LP select RP */
{yygotominor.yy124 = yymsp[-1].minor.yy124;}
        break;
      case 159: /* union ::= select */
{ yygotominor.yy193 = setSubclause(NULL, yymsp[0].minor.yy124); }
        break;
      case 160: /* union ::= union UNION ALL select */
{ yygotominor.yy193 = appendSelectClause(yymsp[-3].minor.yy193, yymsp[0].minor.yy124); }
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy193, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yygotominor.yy124 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy193, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yygotominor.yy193 = yymsp[-1].minor.yy193;}
        break;
      case 164: /* sclp ::= */
      case 194: /* orderby_opt ::= */ yytestcase(yyruleno==194);
{yygotominor.yy193 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yygotominor.yy193 = tSqlExprListAppend(yymsp[-3].minor.yy193, yymsp[-1].minor.yy454,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yygotominor.yy193 = tSqlExprListAppend(yymsp[-1].minor.yy193, pNode, 0, 0);
}
        break;
      case 167: /* as ::= AS ids */
      case 168: /* as ::= ids */ yytestcase(yyruleno==168);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 169: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 170: /* distinct ::= DISTINCT */
{ yygotominor.yy0 = yymsp[0].minor.yy0;  }
        break;
      case 172: /* from ::= FROM tablelist */
      case 173: /* from ::= FROM sub */ yytestcase(yyruleno==173);
{yygotominor.yy332 = yymsp[0].minor.yy332;}
        break;
      case 174: /* sub ::= LP union RP */
{yygotominor.yy332 = addSubqueryElem(NULL, yymsp[-1].minor.yy193, NULL);}
        break;
      case 175: /* sub ::= LP union RP ids */
{yygotominor.yy332 = addSubqueryElem(NULL, yymsp[-2].minor.yy193, &yymsp[0].minor.yy0);}
        break;
      case 176: /* sub ::= sub COMMA LP union RP ids */
{yygotominor.yy332 = addSubqueryElem(yymsp[-5].minor.yy332, yymsp[-2].minor.yy193, &yymsp[0].minor.yy0);}
        break;
      case 177: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy332 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 178: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy332 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy332 = setTableNameList(yymsp[-3].minor.yy332, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 180: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy332 = setTableNameList(yymsp[-4].minor.yy332, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 181: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy392.interval = yymsp[-1].minor.yy0; yygotominor.yy392.offset.n = 0;}
        break;
      case 183: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy392.interval = yymsp[-3].minor.yy0; yygotominor.yy392.offset = yymsp[-1].minor.yy0;}
        break;
      case 184: /* interval_opt ::= */
{memset(&yygotominor.yy392, 0, sizeof(yygotominor.yy392));}
        break;
      case 185: /* session_option ::= */
{yygotominor.yy447.col.n = 0; yygotominor.yy447.gap.n = 0;}
        break;
      case 186: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yygotominor.yy447.col = yymsp[-4].minor.yy0;
   yygotominor.yy447.gap = yymsp[-1].minor.yy0;
}
        break;
      case 187: /* windowstate_option ::= */
{yygotominor.yy76.col.n = 0;}
        break;
      case 188: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yygotominor.yy76.col = yymsp[-1].minor.yy0;
}
        break;
      case 189: /* fill_opt ::= */
{ yygotominor.yy193 = 0;     }
        break;
      case 190: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy193, &A, -1, 0);
    yygotominor.yy193 = yymsp[-1].minor.yy193;
}
        break;
      case 191: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy193 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 192: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 193: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 195: /* orderby_opt ::= ORDER BY sortlist */
{yygotominor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 196: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy193 = tVariantListAppend(yymsp[-3].minor.yy193, &yymsp[-1].minor.yy442, yymsp[0].minor.yy312);
}
        break;
      case 197: /* sortlist ::= item sortorder */
{
  yygotominor.yy193 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy312);
}
        break;
      case 198: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy442, &yymsp[-1].minor.yy0);
}
        break;
      case 199: /* sortorder ::= ASC */
      case 201: /* sortorder ::= */ yytestcase(yyruleno==201);
{ yygotominor.yy312 = TSDB_ORDER_ASC; }
        break;
      case 200: /* sortorder ::= DESC */
{ yygotominor.yy312 = TSDB_ORDER_DESC;}
        break;
      case 202: /* groupby_opt ::= */
{ yygotominor.yy193 = 0;}
        break;
      case 203: /* groupby_opt ::= GROUP BY grouplist */
{ yygotominor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 204: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy193 = tVariantListAppend(yymsp[-2].minor.yy193, &yymsp[0].minor.yy442, -1);
}
        break;
      case 205: /* grouplist ::= item */
{
  yygotominor.yy193 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
        break;
      case 206: /* having_opt ::= */
      case 216: /* where_opt ::= */ yytestcase(yyruleno==216);
      case 258: /* expritem ::= */ yytestcase(yyruleno==258);
{yygotominor.yy454 = 0;}
        break;
      case 207: /* having_opt ::= HAVING expr */
      case 217: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==217);
      case 257: /* expritem ::= expr */ yytestcase(yyruleno==257);
{yygotominor.yy454 = yymsp[0].minor.yy454;}
        break;
      case 208: /* limit_opt ::= */
      case 212: /* slimit_opt ::= */ yytestcase(yyruleno==212);
{yygotominor.yy482.limit = -1; yygotominor.yy482.offset = 0;}
        break;
      case 209: /* limit_opt ::= LIMIT signed */
      case 213: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==213);
{yygotominor.yy482.limit = yymsp[0].minor.yy473;  yygotominor.yy482.offset = 0;}
        break;
      case 210: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yygotominor.yy482.limit = yymsp[-2].minor.yy473;  yygotominor.yy482.offset = yymsp[0].minor.yy473;}
        break;
      case 211: /* limit_opt ::= LIMIT signed COMMA signed */
{ yygotominor.yy482.limit = yymsp[0].minor.yy473;  yygotominor.yy482.offset = yymsp[-2].minor.yy473;}
        break;
      case 214: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yygotominor.yy482.limit = yymsp[-2].minor.yy473;  yygotominor.yy482.offset = yymsp[0].minor.yy473;}
        break;
      case 215: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yygotominor.yy482.limit = yymsp[0].minor.yy473;  yygotominor.yy482.offset = yymsp[-2].minor.yy473;}
        break;
      case 218: /* expr ::= LP expr RP */
{yygotominor.yy454 = yymsp[-1].minor.yy454; yygotominor.yy454->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy454->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
        break;
      case 219: /* expr ::= ID */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 220: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 221: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 222: /* expr ::= INTEGER */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 223: /* expr ::= MINUS INTEGER */
      case 224: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==224);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 225: /* expr ::= FLOAT */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 226: /* expr ::= MINUS FLOAT */
      case 227: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==227);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 228: /* expr ::= STRING */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 229: /* expr ::= NOW */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 230: /* expr ::= VARIABLE */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 231: /* expr ::= PLUS VARIABLE */
      case 232: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==232);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
        break;
      case 233: /* expr ::= BOOL */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 234: /* expr ::= NULL */
{ yygotominor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
        break;
      case 235: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy454 = tSqlExprCreateFunction(yymsp[-1].minor.yy193, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 236: /* expr ::= ID LP STAR RP */
{ yygotominor.yy454 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 237: /* expr ::= expr IS NULL */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, NULL, TK_ISNULL);}
        break;
      case 238: /* expr ::= expr IS NOT NULL */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-3].minor.yy454, NULL, TK_NOTNULL);}
        break;
      case 239: /* expr ::= expr LT expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_LT);}
        break;
      case 240: /* expr ::= expr GT expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_GT);}
        break;
      case 241: /* expr ::= expr LE expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_LE);}
        break;
      case 242: /* expr ::= expr GE expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_GE);}
        break;
      case 243: /* expr ::= expr NE expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_NE);}
        break;
      case 244: /* expr ::= expr EQ expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_EQ);}
        break;
      case 245: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy454); yygotominor.yy454 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy454, yymsp[-2].minor.yy454, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy454, TK_LE), TK_AND);}
        break;
      case 246: /* expr ::= expr AND expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_AND);}
        break;
      case 247: /* expr ::= expr OR expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_OR); }
        break;
      case 248: /* expr ::= expr PLUS expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_PLUS);  }
        break;
      case 249: /* expr ::= expr MINUS expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_MINUS); }
        break;
      case 250: /* expr ::= expr STAR expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_STAR);  }
        break;
      case 251: /* expr ::= expr SLASH expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_DIVIDE);}
        break;
      case 252: /* expr ::= expr REM expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_REM);   }
        break;
      case 253: /* expr ::= expr LIKE expr */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_LIKE);  }
        break;
      case 254: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy454 = tSqlExprCreate(yymsp[-4].minor.yy454, (tSqlExpr*)yymsp[-1].minor.yy193, TK_IN); }
        break;
      case 255: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy193 = tSqlExprListAppend(yymsp[-2].minor.yy193,yymsp[0].minor.yy454,0, 0);}
        break;
      case 256: /* exprlist ::= expritem */
{yygotominor.yy193 = tSqlExprListAppend(0,yymsp[0].minor.yy454,0, 0);}
        break;
      case 259: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 260: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 266: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 272: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 273: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 274: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
