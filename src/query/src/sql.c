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
#define YYNOCODE 271
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy112;
  SCreateAcctInfo yy151;
  tSqlExpr* yy166;
  SCreateTableSql* yy182;
  SSqlNode* yy236;
  SRelationInfo* yy244;
  SSessionWindowVal yy259;
  SIntervalVal yy340;
  TAOS_FIELD yy343;
  SWindowStateVal yy348;
  int64_t yy369;
  SCreateDbInfo yy382;
  SLimitVal yy414;
  SArray* yy441;
  SCreatedTableInfo yy456;
  tVariant yy506;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 549
#define YYNRULE 285
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
#define YY_ACTTAB_COUNT (854)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   532,   53,   52,  126,  125,   51,   50,   49,  531,  143,
 /*    10 */   141,  140,   54,   55,   14,   58,   59,  379,  378,  239,
 /*    20 */    48,  450,   57,  300,   62,   60,   63,   61,  343,  342,
 /*    30 */   137,   25,   53,   52,  835,  346,   51,   50,   49,   54,
 /*    40 */    55,  549,   58,   59,   94,   91,  239,   48,  237,   57,
 /*    50 */   300,   62,   60,   63,   61,  443,   71,  442,  152,   53,
 /*    60 */    52,  389,  253,   51,   50,   49,   54,   55,    6,   58,
 /*    70 */    59,  257,  256,  239,   48,   74,   57,  300,   62,   60,
 /*    80 */    63,   61,   22,  362,   21,   92,   53,   52,  532,   95,
 /*    90 */    51,   50,   49,   51,   50,   49,  531,   72,  440,  261,
 /*   100 */   439,   54,   56,  471,   58,   59,  232,  449,  239,   48,
 /*   110 */    77,   57,  300,   62,   60,   63,   61,  397,  396,   34,
 /*   120 */   388,   53,   52,  474,  395,   51,   50,   49,  282,  113,
 /*   130 */    88,  112,  548,  547,  546,  545,  544,  543,  542,  541,
 /*   140 */   540,  539,  538,  537,  536,  345,   35,   55,  220,   58,
 /*   150 */    59,  320,  319,  239,   48,  535,   57,  300,   62,   60,
 /*   160 */    63,   61,  534,   33,  145,  247,   53,   52,  532,  441,
 /*   170 */    51,   50,   49,   58,   59,  152,  531,  239,   48,   81,
 /*   180 */    57,  300,   62,   60,   63,   61,   20,  302,   19,  322,
 /*   190 */    53,   52,  288,  469,   51,   50,   49,  402,  414,  413,
 /*   200 */   412,  411,  410,  409,  408,  407,  406,  405,  404,  403,
 /*   210 */   401,  400,  438,  264,   41,  296,  339,  338,  295,  294,
 /*   220 */   293,  337,  292,  336,  335,  334,  291,  333,  332,  238,
 /*   230 */   433,  268,  267,  444,  446,  437,  249,  436,  246,  523,
 /*   240 */   315,  314,   24,   62,   60,   63,   61,  374,   35,  280,
 /*   250 */   139,   53,   52,   83,   35,   51,   50,   49,  459,  205,
 /*   260 */   458,  217,  218,    3,  180,  301,  206,  457,  240,  456,
 /*   270 */   445,  129,  128,  204,   18,   68,   17,  305,   83,  497,
 /*   280 */    76,  499,  498,  354,  448,   35,  496,   42,  494,  493,
 /*   290 */   495,  318,  492,  491,  431,  469,   41,  317,  339,  338,
 /*   300 */   138,  469,   35,  337,   25,  336,  335,  334,   64,  333,
 /*   310 */   332,  242,   42,  352,  114,  108,  119,  532,   69,  235,
 /*   320 */   236,  118,  124,  127,  117,  531,  462,  197,  316,  465,
 /*   330 */   121,  464,  469,  463,  260,  507,   75,  425,  238,  433,
 /*   340 */   432,  435,  444,  213,  437,  312,  436,  434,   35,  469,
 /*   350 */   365,   80,  188,  186,  184,   35,   35,  244,  245,  183,
 /*   360 */   132,  131,  130,  353,   35,   35,    5,   38,  169,  181,
 /*   370 */   217,  218,   35,  168,  102,   97,  101,    9,  197,  197,
 /*   380 */   686,  107,  243,  106,  241,  264,  308,  307,  425,  425,
 /*   390 */    16,  311,   15,  116,  426,  469,   29,  152,  310,  309,
 /*   400 */   487,  330,  469,  469,   25,  179,  394,  230,  229,  152,
 /*   410 */   287,  469,  469,  391,  285,  221,   65,   64,  506,  469,
 /*   420 */    93,  387,  370,   65,   36,  371,    1,  167,  367,  262,
 /*   430 */    90,   36,   26,  340,  508,   65,  461,  460,  147,   36,
 /*   440 */   505,  422,   35,  488,   78,  362,  179,  362,  179,  432,
 /*   450 */   435,  248,  355,  504,  233,  231,  434,  503,  392,  392,
 /*   460 */   225,  223,  298,  222,  392,  197,  219,  344,  502,   89,
 /*   470 */   501,  500,  490,  486,  485,  424,  484,  483,  482,  481,
 /*   480 */   480,  375,  479,   36,  475,  473,  472,  111,  470,  476,
 /*   490 */   304,  109,   67,  105,  103,   66,    8,  447,  430,    7,
 /*   500 */   100,  297,  303,  421,  420,  419,  418,  417,  416,   96,
 /*   510 */    94,   28,   12,  415,  286,  284,   27,   13,  357,   32,
 /*   520 */    11,   31,  151,  226,  372,  276,   87,   82,  369,   86,
 /*   530 */   149,  368,  366,  148,  259,   85,   30,  264,  363,   10,
 /*   540 */   349,  255,  348,  254,  252,  266,  251,  347,  351,  142,
 /*   550 */     4,  530,  350,  529,  509,  136,    2,  524,  489,  135,
 /*   560 */   522,  327,  134,  133,  341,  515,  178,  331,  329,  510,
 /*   570 */   173,  328,  177,  326,  325,  176,  324,  174,  175,  323,
 /*   580 */    99,  115,  212,  172,  211,  477,  289,  298,  234,   98,
 /*   590 */   270,  214,  161,  157,  155,  162,   46,   70,  278,  377,
 /*   600 */   836,  271,  272,   84,  399,  159,  380,   79,  273,  263,
 /*   610 */   836,  836,  190,  836,  275,  533,  277,  189,  279,  528,
 /*   620 */   527,  283,  160,  526,  281,  158,  836,   47,  156,   73,
 /*   630 */   373,  154,  525,  836,  187,  153,  836,  385,  836,  386,
 /*   640 */   836,  836,  265,  836,  836,  163,  224,  269,  836,  361,
 /*   650 */   836,  836,  836,  836,  836,  836,  185,  376,  836,  321,
 /*   660 */   836,  836,  836,  836,  836,  836,  228,  836,   45,  836,
 /*   670 */   836,  836,  836,  384,  193,  836,  836,  836,  836,  330,
 /*   680 */   836,  836,  836,  836,  836,  836,  836,  836,  836,  836,
 /*   690 */   836,  836,  836,  836,  836,  836,  836,  836,  836,  836,
 /*   700 */   521,  836,  520,  519,  518,  836,  517,  516,  182,  250,
 /*   710 */   514,  513,  123,  122,  512,  120,  836,  511,  192,   44,
 /*   720 */   836,   37,   40,  478,  171,  468,  467,  110,  466,  836,
 /*   730 */   836,  836,  313,  836,  170,  454,  836,  453,  104,  452,
 /*   740 */   306,  423,  299,  836,   39,  191,   43,  290,  398,  166,
 /*   750 */   165,  390,  164,  274,  150,  146,  364,  360,  359,  358,
 /*   760 */   356,  144,  258,  836,  836,  836,  836,  836,  836,  836,
 /*   770 */   836,  836,  836,  836,  836,  836,  836,  836,  836,  836,
 /*   780 */   836,  836,  836,  836,  836,  836,  836,  836,  836,  836,
 /*   790 */   836,  836,  836,  836,  836,  836,  836,  836,  836,  836,
 /*   800 */   836,  383,  227,  382,  381,  836,  836,  836,  836,  836,
 /*   810 */   836,  836,  836,  836,  836,  455,  451,  393,  836,  836,
 /*   820 */   836,  836,  836,  836,  836,  836,  836,  836,  836,  836,
 /*   830 */   836,  836,  836,  836,  836,  207,  210,  209,  208,  203,
 /*   840 */   202,  196,  201,  199,  198,  216,  215,  429,  428,  427,
 /*   850 */   200,  195,  194,   23,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,   76,   77,   37,   38,   39,    9,   62,
 /*    10 */    63,   64,   13,   14,   80,   16,   17,  127,  128,   20,
 /*    20 */    21,   81,   23,   24,   25,   26,   27,   28,   65,   66,
 /*    30 */    67,   91,   33,   34,  191,  192,   37,   38,   39,   13,
 /*    40 */    14,    0,   16,   17,  110,  111,   20,   21,   60,   23,
 /*    50 */    24,   25,   26,   27,   28,    5,   91,    7,  194,   33,
 /*    60 */    34,  194,  137,   37,   38,   39,   13,   14,   80,   16,
 /*    70 */    17,  146,  147,   20,   21,   80,   23,   24,   25,   26,
 /*    80 */    27,   28,  140,  240,  142,  201,   33,   34,    1,  201,
 /*    90 */    37,   38,   39,   37,   38,   39,    9,  132,    5,  256,
 /*   100 */     7,   13,   14,  108,   16,   17,  239,   81,   20,   21,
 /*   110 */   111,   23,   24,   25,   26,   27,   28,  233,  234,  235,
 /*   120 */   236,   33,   34,    5,  236,   37,   38,   39,  264,  140,
 /*   130 */   266,  142,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,   56,   57,   58,  194,   14,   61,   16,
 /*   150 */    17,   33,   34,   20,   21,   59,   23,   24,   25,   26,
 /*   160 */    27,   28,   60,   80,  194,   68,   33,   34,    1,  119,
 /*   170 */    37,   38,   39,   16,   17,  194,    9,   20,   21,   81,
 /*   180 */    23,   24,   25,   26,   27,   28,  140,   15,  142,  237,
 /*   190 */    33,   34,  109,  241,   37,   38,   39,  215,  216,  217,
 /*   200 */   218,  219,  220,  221,  222,  223,  224,  225,  226,  227,
 /*   210 */   228,  229,  119,  115,   92,   93,   94,   95,   96,   97,
 /*   220 */    98,   99,  100,  101,  102,  103,  104,  105,  106,    1,
 /*   230 */     2,  261,  262,    5,    1,    7,  139,    9,  141,   83,
 /*   240 */   143,  144,   44,   25,   26,   27,   28,  266,  194,  268,
 /*   250 */    21,   33,   34,   80,  194,   37,   38,   39,    5,   61,
 /*   260 */     7,   33,   34,  197,  198,   37,   68,    5,  200,    7,
 /*   270 */    37,   73,   74,   75,  140,   91,  142,   79,   80,  215,
 /*   280 */   201,  217,  218,   37,  112,  194,  222,  114,  224,  225,
 /*   290 */   226,  237,  228,  229,   81,  241,   92,  237,   94,   95,
 /*   300 */    21,  241,  194,   99,   91,  101,  102,  103,   80,  105,
 /*   310 */   106,   68,  114,  234,   62,   63,   64,    1,  134,  200,
 /*   320 */   200,   69,   70,   71,   72,    9,    2,  259,  237,    5,
 /*   330 */    78,    7,  241,    9,  136,    5,  138,  269,    1,    2,
 /*   340 */   112,  113,    5,  145,    7,  237,    9,  119,  194,  241,
 /*   350 */    91,   81,   62,   63,   64,  194,  194,   33,   34,   69,
 /*   360 */    70,   71,   72,  117,  194,  194,   62,   63,   64,   91,
 /*   370 */    33,   34,  194,   69,   70,   71,   72,  118,  259,  259,
 /*   380 */     0,  140,  139,  142,  141,  115,  143,  144,  269,  269,
 /*   390 */   140,  237,  142,   76,   81,  241,   80,  194,  237,  237,
 /*   400 */   199,   84,  241,  241,   91,  204,   81,  237,  237,  194,
 /*   410 */    81,  241,  241,   81,   81,  237,   91,   80,    5,  241,
 /*   420 */    91,   81,   81,   91,   91,   81,  202,  203,   81,   81,
 /*   430 */   243,   91,   91,  213,  214,   91,  112,  113,   91,   91,
 /*   440 */     5,  199,  194,  199,  257,  240,  204,  240,  204,  112,
 /*   450 */   113,  194,  194,    5,  238,  238,  119,    5,  242,  242,
 /*   460 */   238,  256,   82,  256,  242,  259,  193,  194,    5,  266,
 /*   470 */     5,    5,    5,    5,    5,  269,    5,    5,    5,    5,
 /*   480 */     5,  266,    5,   91,   81,    5,    5,  142,  231,  241,
 /*   490 */    58,  142,   16,  142,  142,   16,   80,  112,   81,   80,
 /*   500 */    76,   15,   24,   83,    5,    5,    5,    5,    5,   76,
 /*   510 */   110,   80,  126,    9,  109,  109,   80,   80,  260,   91,
 /*   520 */   126,   91,   80,    1,   81,   80,   80,  116,   81,   80,
 /*   530 */    91,   81,   81,   80,  137,   91,   80,  115,   81,   80,
 /*   540 */     5,    5,    5,  148,    5,   91,  148,    5,   93,   60,
 /*   550 */   197,  195,   94,  195,    5,  196,  202,  195,  230,  196,
 /*   560 */   195,   54,  196,  196,   82,  195,  205,  107,   85,  214,
 /*   570 */   210,   87,  208,   88,   86,  207,   50,  206,  209,   89,
 /*   580 */   201,   90,  195,  211,  195,  212,  195,   82,  195,  201,
 /*   590 */   120,  195,  246,  250,  252,  245,  135,  133,  263,  195,
 /*   600 */   270,  263,  195,  195,  230,  248,  267,  195,  121,  195,
 /*   610 */   270,  270,  194,  270,  122,  194,  123,  194,  124,  194,
 /*   620 */   194,  125,  247,  194,  129,  249,  270,  130,  251,  131,
 /*   630 */   119,  253,  194,  270,  194,  254,  270,  255,  270,  240,
 /*   640 */   270,  270,  240,  270,  270,  244,  263,  263,  270,  240,
 /*   650 */   270,  270,  270,  270,  270,  270,  194,  267,  270,  232,
 /*   660 */   270,  270,  270,  270,  270,  270,  232,  270,  258,  270,
 /*   670 */   270,  270,  270,  232,  259,  270,  270,  270,  270,   84,
 /*   680 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   690 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   700 */   194,  270,  194,  194,  194,  270,  194,  194,  194,  194,
 /*   710 */   194,  194,  194,  194,  194,  194,  270,  194,  194,  194,
 /*   720 */   270,  194,  194,  194,  194,  194,  194,  194,  194,  270,
 /*   730 */   270,  270,  194,  270,  194,  194,  270,  194,  194,  194,
 /*   740 */   194,  194,  194,  270,  194,  194,  194,  194,  194,  194,
 /*   750 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   760 */   194,  194,  194,  270,  270,  270,  270,  270,  270,  270,
 /*   770 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   780 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   790 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   800 */   270,  232,  232,  232,  232,  270,  270,  270,  270,  270,
 /*   810 */   270,  270,  270,  270,  270,  242,  242,  242,  270,  270,
 /*   820 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   830 */   270,  270,  270,  270,  270,  259,  259,  259,  259,  259,
 /*   840 */   259,  259,  259,  259,  259,  259,  259,  259,  259,  259,
 /*   850 */   259,  259,  259,  259,
};
#define YY_SHIFT_USE_DFLT (-111)
#define YY_SHIFT_COUNT (346)
#define YY_SHIFT_MIN   (-110)
#define YY_SHIFT_MAX   (595)
static const short yy_shift_ofst[] = {
 /*     0 */   198,  122,  122,  204,  204,  505,  228,  337,  337,  316,
 /*    10 */   167,  167,  167,  167,  167,  167,  167,  167,  167,  167,
 /*    20 */   167,  167,  167,   -1,   87,  337,  324,  324,  324,  173,
 /*    30 */   173,  167,  167,  167,  380,  167,  167,  317,  505,  595,
 /*    40 */   595,  549, -111, -111, -111,  337,  337,  337,  337,  337,
 /*    50 */   337,  337,  337,  337,  337,  337,  337,  337,  337,  337,
 /*    60 */   337,  337,  337,  337,  337,  324,  324,  324,  118,  118,
 /*    70 */   118,  118,  118,  118,  118,  167,  167,  167,  246,  167,
 /*    80 */   167,  167,  173,  173,  167,  167,  167,  167, -110, -110,
 /*    90 */   259,  173,  167,  167,  167,  167,  167,  167,  167,  167,
 /*   100 */   167,  167,  167,  167,  167,  167,  167,  167,  167,  167,
 /*   110 */   167,  167,  167,  167,  167,  167,  167,  167,  167,  167,
 /*   120 */   167,  167,  167,  167,  167,  167,  167,  167,  167,  167,
 /*   130 */   167,  167,  167,  167,  167,  167,  167,  167,  167,  167,
 /*   140 */   167,  167,  167,  167,  489,  489,  489,  511,  511,  511,
 /*   150 */   489,  511,  489,  498,  464,  497,  496,  495,  494,  493,
 /*   160 */   492,  487,  470,  461,  489,  489,  489,  460,  505,  505,
 /*   170 */   489,  489,  491,  490,  526,  488,  485,  507,  484,  483,
 /*   180 */   460,  549,  489,  482,  482,  489,  482,  489,  482,  489,
 /*   190 */   489, -111, -111,   26,   53,   53,   88,   53,  133,  157,
 /*   200 */   218,  218,  218,  218,  252,  304,  290,  -32,  -32,  -32,
 /*   210 */   -32,  243,   97,  -75,  -66,   56,   56,   93,   50,  -37,
 /*   220 */   -53,  348,  270,   98,  347,  344,  341,  184,  -35,  340,
 /*   230 */   333,  332,  329,  325,   83,  313,  213,  233,  -12,  172,
 /*   240 */   -60,  250,  241,  134,  262,  253,   46,  -11,   -5,  -58,
 /*   250 */   -73,  542,  398,  539,  537,  395,  536,  535,  458,  455,
 /*   260 */   397,  422,  406,  459,  411,  457,  456,  454,  444,  451,
 /*   270 */   453,  450,  439,  449,  447,  446,  522,  445,  443,  442,
 /*   280 */   430,  394,  428,  386,  437,  406,  436,  405,  431,  400,
 /*   290 */   433,  504,  503,  502,  501,  500,  499,  420,  486,  424,
 /*   300 */   419,  417,  385,  416,  478,  432,  479,  352,  351,  392,
 /*   310 */   392,  392,  392,  476,  349,  345,  392,  392,  392,  481,
 /*   320 */   480,  403,  392,  477,  475,  474,  473,  472,  471,  469,
 /*   330 */   468,  467,  466,  465,  463,  452,  448,  435,  413,  330,
 /*   340 */   278,  156,  279,  229,  102,   96,   41,
};
#define YY_REDUCE_USE_DFLT (-158)
#define YY_REDUCE_COUNT (192)
#define YY_REDUCE_MIN   (-157)
#define YY_REDUCE_MAX   (594)
static const short yy_reduce_ofst[] = {
 /*     0 */  -157,  -18,  -18,   64,   64, -116,  120,  119,   68,  -30,
 /*    10 */   178,  -19, -136,  171,  170,  162,  161,  154,  108,   91,
 /*    20 */    60,   54,  -48,  258,  273,  206,  222,  217,  216,  207,
 /*    30 */   205,  215,  203, -133, -112,  257,  248,  244,   79,  242,
 /*    40 */   201,  220,  187,  224,   66,  594,  593,  592,  591,  590,
 /*    50 */   589,  588,  587,  586,  585,  584,  583,  582,  581,  580,
 /*    60 */   579,  578,  577,  576,  415,  575,  574,  573,  572,  571,
 /*    70 */   570,  569,  441,  434,  427,  568,  567,  566,  410,  565,
 /*    80 */   564,  563,  409,  402,  562,  561,  560,  559,  390,  339,
 /*    90 */   401,  399,  558,  557,  556,  555,  554,  553,  552,  551,
 /*   100 */   550,  548,  547,  546,  545,  544,  543,  541,  540,  538,
 /*   110 */   534,  533,  532,  531,  530,  529,  528,  527,  525,  524,
 /*   120 */   523,  521,  520,  519,  518,  517,  516,  515,  514,  513,
 /*   130 */   512,  510,  509,  508,  506,  462,  440,  438,  429,  426,
 /*   140 */   425,  423,  421,  418,  414,  412,  408,  384,  383,  338,
 /*   150 */   407,  335,  404,  382,  381,  378,  342,  377,  343,  376,
 /*   160 */   357,  375,  346,  350,  396,  393,  391,  374,  388,  379,
 /*   170 */   389,  387,  373,  372,  360,  371,  369,  368,  364,  361,
 /*   180 */   328,  355,  370,  367,  366,  365,  363,  362,  359,  358,
 /*   190 */   356,  354,  353,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   834,  663,  606,  675,  593,  603,  812,  812,  812,  834,
 /*    10 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*    20 */   834,  834,  834,  723,  565,  812,  834,  834,  834,  834,
 /*    30 */   834,  834,  834,  834,  603,  834,  834,  609,  603,  609,
 /*    40 */   609,  834,  718,  647,  665,  834,  834,  834,  834,  834,
 /*    50 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*    60 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*    70 */   834,  834,  834,  834,  834,  834,  834,  834,  725,  731,
 /*    80 */   728,  834,  834,  834,  733,  834,  834,  834,  755,  755,
 /*    90 */   716,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   100 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   110 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   120 */   591,  834,  589,  834,  834,  834,  834,  834,  834,  834,
 /*   130 */   834,  834,  834,  834,  834,  834,  834,  576,  834,  834,
 /*   140 */   834,  834,  834,  834,  567,  567,  567,  834,  834,  834,
 /*   150 */   567,  834,  567,  762,  766,  760,  748,  756,  747,  743,
 /*   160 */   741,  739,  738,  770,  567,  567,  567,  607,  603,  603,
 /*   170 */   567,  567,  625,  623,  621,  613,  619,  615,  617,  611,
 /*   180 */   594,  834,  567,  601,  601,  567,  601,  567,  601,  567,
 /*   190 */   567,  647,  665,  834,  771,  761,  834,  811,  801,  800,
 /*   200 */   807,  799,  798,  797,  834,  834,  834,  793,  796,  795,
 /*   210 */   794,  834,  834,  834,  834,  803,  802,  834,  834,  834,
 /*   220 */   834,  834,  834,  834,  834,  834,  834,  767,  763,  834,
 /*   230 */   834,  834,  834,  834,  834,  834,  834,  834,  773,  834,
 /*   240 */   834,  834,  834,  834,  834,  834,  834,  834,  677,  834,
 /*   250 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   260 */   834,  715,  834,  834,  834,  834,  834,  727,  726,  834,
 /*   270 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   280 */   757,  834,  749,  834,  834,  689,  834,  834,  834,  834,
 /*   290 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   300 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  830,
 /*   310 */   825,  826,  823,  834,  834,  834,  822,  817,  818,  834,
 /*   320 */   834,  834,  815,  834,  834,  834,  834,  834,  834,  834,
 /*   330 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   340 */   631,  834,  574,  572,  834,  563,  834,  833,  832,  831,
 /*   350 */   814,  813,  685,  724,  720,  722,  721,  719,  732,  729,
 /*   360 */   730,  714,  713,  712,  734,  717,  737,  736,  740,  742,
 /*   370 */   745,  744,  746,  735,  759,  758,  751,  752,  754,  753,
 /*   380 */   750,  769,  768,  765,  764,  711,  695,  690,  687,  694,
 /*   390 */   693,  692,  700,  699,  691,  688,  684,  683,  608,  664,
 /*   400 */   662,  661,  660,  659,  658,  657,  656,  655,  654,  653,
 /*   410 */   652,  651,  650,  649,  648,  643,  639,  637,  636,  635,
 /*   420 */   632,  602,  605,  604,  809,  810,  808,  806,  805,  804,
 /*   430 */   790,  789,  788,  787,  784,  783,  782,  779,  785,  781,
 /*   440 */   778,  786,  780,  777,  776,  775,  774,  792,  791,  772,
 /*   450 */   597,  829,  828,  827,  824,  821,  710,  709,  708,  707,
 /*   460 */   706,  705,  704,  703,  702,  701,  820,  819,  816,  697,
 /*   470 */   698,  679,  682,  681,  680,  678,  696,  627,  626,  624,
 /*   480 */   622,  614,  620,  616,  618,  612,  610,  596,  595,  676,
 /*   490 */   646,  674,  673,  672,  671,  670,  669,  668,  667,  666,
 /*   500 */   645,  644,  642,  641,  640,  638,  634,  633,  629,  630,
 /*   510 */   628,  592,  590,  588,  587,  586,  585,  584,  583,  582,
 /*   520 */   581,  580,  579,  600,  578,  577,  575,  573,  571,  570,
 /*   530 */   569,  599,  598,  568,  566,  564,  562,  561,  560,  559,
 /*   540 */   558,  557,  556,  555,  554,  553,  552,  551,  550,
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
  "PASS",          "PRIVILEGE",     "LOCAL",         "COMPACT",     
  "LP",            "RP",            "IF",            "EXISTS",      
  "PPS",           "TSERIES",       "DBS",           "STORAGE",     
  "QTIME",         "CONNS",         "STATE",         "COMMA",       
  "KEEP",          "CACHE",         "REPLICA",       "QUORUM",      
  "DAYS",          "MINROWS",       "MAXROWS",       "BLOCKS",      
  "CTIME",         "WAL",           "FSYNC",         "COMP",        
  "PRECISION",     "UPDATE",        "CACHELAST",     "PARTITIONS",  
  "UNSIGNED",      "TAGS",          "USING",         "AS",          
  "NULL",          "NOW",           "SELECT",        "UNION",       
  "ALL",           "DISTINCT",      "FROM",          "VARIABLE",    
  "INTERVAL",      "SESSION",       "STATE_WINDOW",  "FILL",        
  "SLIDING",       "ORDER",         "BY",            "ASC",         
  "DESC",          "GROUP",         "HAVING",        "LIMIT",       
  "OFFSET",        "SLIMIT",        "SOFFSET",       "WHERE",       
  "RESET",         "QUERY",         "SYNCDB",        "ADD",         
  "COLUMN",        "MODIFY",        "TAG",           "CHANGE",      
  "SET",           "KILL",          "CONNECTION",    "STREAM",      
  "COLON",         "ABORT",         "AFTER",         "ATTACH",      
  "BEFORE",        "BEGIN",         "CASCADE",       "CLUSTER",     
  "CONFLICT",      "COPY",          "DEFERRED",      "DELIMITERS",  
  "DETACH",        "EACH",          "END",           "EXPLAIN",     
  "FAIL",          "FOR",           "IGNORE",        "IMMEDIATE",   
  "INITIALLY",     "INSTEAD",       "MATCH",         "KEY",         
  "OF",            "RAISE",         "REPLACE",       "RESTRICT",    
  "ROW",           "STATEMENT",     "TRIGGER",       "VIEW",        
  "SEMI",          "NONE",          "PREV",          "LINEAR",      
  "IMPORT",        "TBNAME",        "JOIN",          "INSERT",      
  "INTO",          "VALUES",        "error",         "program",     
  "cmd",           "dbPrefix",      "ids",           "cpxName",     
  "ifexists",      "alter_db_optr",  "alter_topic_optr",  "acct_optr",   
  "exprlist",      "ifnotexists",   "db_optr",       "topic_optr",  
  "pps",           "tseries",       "dbs",           "streams",     
  "storage",       "qtime",         "users",         "conns",       
  "state",         "intitemlist",   "intitem",       "keep",        
  "cache",         "replica",       "quorum",        "days",        
  "minrows",       "maxrows",       "blocks",        "ctime",       
  "wal",           "fsync",         "comp",          "prec",        
  "update",        "cachelast",     "partitions",    "typename",    
  "signed",        "create_table_args",  "create_stable_args",  "create_table_list",
  "create_from_stable",  "columnlist",    "tagitemlist",   "tagNamelist", 
  "select",        "column",        "tagitem",       "selcollist",  
  "from",          "where_opt",     "interval_opt",  "session_option",
  "windowstate_option",  "fill_opt",      "sliding_opt",   "groupby_opt", 
  "orderby_opt",   "having_opt",    "slimit_opt",    "limit_opt",   
  "union",         "sclp",          "distinct",      "expr",        
  "as",            "tablelist",     "sub",           "tmvar",       
  "sortlist",      "sortitem",      "item",          "sortorder",   
  "grouplist",     "expritem",    
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
 /*  79 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  80 */ "intitemlist ::= intitem",
 /*  81 */ "intitem ::= INTEGER",
 /*  82 */ "keep ::= KEEP intitemlist",
 /*  83 */ "cache ::= CACHE INTEGER",
 /*  84 */ "replica ::= REPLICA INTEGER",
 /*  85 */ "quorum ::= QUORUM INTEGER",
 /*  86 */ "days ::= DAYS INTEGER",
 /*  87 */ "minrows ::= MINROWS INTEGER",
 /*  88 */ "maxrows ::= MAXROWS INTEGER",
 /*  89 */ "blocks ::= BLOCKS INTEGER",
 /*  90 */ "ctime ::= CTIME INTEGER",
 /*  91 */ "wal ::= WAL INTEGER",
 /*  92 */ "fsync ::= FSYNC INTEGER",
 /*  93 */ "comp ::= COMP INTEGER",
 /*  94 */ "prec ::= PRECISION STRING",
 /*  95 */ "update ::= UPDATE INTEGER",
 /*  96 */ "cachelast ::= CACHELAST INTEGER",
 /*  97 */ "partitions ::= PARTITIONS INTEGER",
 /*  98 */ "db_optr ::=",
 /*  99 */ "db_optr ::= db_optr cache",
 /* 100 */ "db_optr ::= db_optr replica",
 /* 101 */ "db_optr ::= db_optr quorum",
 /* 102 */ "db_optr ::= db_optr days",
 /* 103 */ "db_optr ::= db_optr minrows",
 /* 104 */ "db_optr ::= db_optr maxrows",
 /* 105 */ "db_optr ::= db_optr blocks",
 /* 106 */ "db_optr ::= db_optr ctime",
 /* 107 */ "db_optr ::= db_optr wal",
 /* 108 */ "db_optr ::= db_optr fsync",
 /* 109 */ "db_optr ::= db_optr comp",
 /* 110 */ "db_optr ::= db_optr prec",
 /* 111 */ "db_optr ::= db_optr keep",
 /* 112 */ "db_optr ::= db_optr update",
 /* 113 */ "db_optr ::= db_optr cachelast",
 /* 114 */ "topic_optr ::= db_optr",
 /* 115 */ "topic_optr ::= topic_optr partitions",
 /* 116 */ "alter_db_optr ::=",
 /* 117 */ "alter_db_optr ::= alter_db_optr replica",
 /* 118 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 119 */ "alter_db_optr ::= alter_db_optr keep",
 /* 120 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 121 */ "alter_db_optr ::= alter_db_optr comp",
 /* 122 */ "alter_db_optr ::= alter_db_optr wal",
 /* 123 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 124 */ "alter_db_optr ::= alter_db_optr update",
 /* 125 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 126 */ "alter_topic_optr ::= alter_db_optr",
 /* 127 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 128 */ "typename ::= ids",
 /* 129 */ "typename ::= ids LP signed RP",
 /* 130 */ "typename ::= ids UNSIGNED",
 /* 131 */ "signed ::= INTEGER",
 /* 132 */ "signed ::= PLUS INTEGER",
 /* 133 */ "signed ::= MINUS INTEGER",
 /* 134 */ "cmd ::= CREATE TABLE create_table_args",
 /* 135 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 137 */ "cmd ::= CREATE TABLE create_table_list",
 /* 138 */ "create_table_list ::= create_from_stable",
 /* 139 */ "create_table_list ::= create_table_list create_from_stable",
 /* 140 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 141 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 143 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 144 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 145 */ "tagNamelist ::= ids",
 /* 146 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 147 */ "columnlist ::= columnlist COMMA column",
 /* 148 */ "columnlist ::= column",
 /* 149 */ "column ::= ids typename",
 /* 150 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 151 */ "tagitemlist ::= tagitem",
 /* 152 */ "tagitem ::= INTEGER",
 /* 153 */ "tagitem ::= FLOAT",
 /* 154 */ "tagitem ::= STRING",
 /* 155 */ "tagitem ::= BOOL",
 /* 156 */ "tagitem ::= NULL",
 /* 157 */ "tagitem ::= NOW",
 /* 158 */ "tagitem ::= MINUS INTEGER",
 /* 159 */ "tagitem ::= MINUS FLOAT",
 /* 160 */ "tagitem ::= PLUS INTEGER",
 /* 161 */ "tagitem ::= PLUS FLOAT",
 /* 162 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 163 */ "select ::= LP select RP",
 /* 164 */ "union ::= select",
 /* 165 */ "union ::= union UNION ALL select",
 /* 166 */ "cmd ::= union",
 /* 167 */ "select ::= SELECT selcollist",
 /* 168 */ "sclp ::= selcollist COMMA",
 /* 169 */ "sclp ::=",
 /* 170 */ "selcollist ::= sclp distinct expr as",
 /* 171 */ "selcollist ::= sclp STAR",
 /* 172 */ "as ::= AS ids",
 /* 173 */ "as ::= ids",
 /* 174 */ "as ::=",
 /* 175 */ "distinct ::= DISTINCT",
 /* 176 */ "distinct ::=",
 /* 177 */ "from ::= FROM tablelist",
 /* 178 */ "from ::= FROM sub",
 /* 179 */ "sub ::= LP union RP",
 /* 180 */ "sub ::= LP union RP ids",
 /* 181 */ "sub ::= sub COMMA LP union RP ids",
 /* 182 */ "tablelist ::= ids cpxName",
 /* 183 */ "tablelist ::= ids cpxName ids",
 /* 184 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 185 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 186 */ "tmvar ::= VARIABLE",
 /* 187 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 188 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 189 */ "interval_opt ::=",
 /* 190 */ "session_option ::=",
 /* 191 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 192 */ "windowstate_option ::=",
 /* 193 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 194 */ "fill_opt ::=",
 /* 195 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 196 */ "fill_opt ::= FILL LP ID RP",
 /* 197 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 198 */ "sliding_opt ::=",
 /* 199 */ "orderby_opt ::=",
 /* 200 */ "orderby_opt ::= ORDER BY sortlist",
 /* 201 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 202 */ "sortlist ::= item sortorder",
 /* 203 */ "item ::= ids cpxName",
 /* 204 */ "sortorder ::= ASC",
 /* 205 */ "sortorder ::= DESC",
 /* 206 */ "sortorder ::=",
 /* 207 */ "groupby_opt ::=",
 /* 208 */ "groupby_opt ::= GROUP BY grouplist",
 /* 209 */ "grouplist ::= grouplist COMMA item",
 /* 210 */ "grouplist ::= item",
 /* 211 */ "having_opt ::=",
 /* 212 */ "having_opt ::= HAVING expr",
 /* 213 */ "limit_opt ::=",
 /* 214 */ "limit_opt ::= LIMIT signed",
 /* 215 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 216 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 217 */ "slimit_opt ::=",
 /* 218 */ "slimit_opt ::= SLIMIT signed",
 /* 219 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 220 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 221 */ "where_opt ::=",
 /* 222 */ "where_opt ::= WHERE expr",
 /* 223 */ "expr ::= LP expr RP",
 /* 224 */ "expr ::= ID",
 /* 225 */ "expr ::= ID DOT ID",
 /* 226 */ "expr ::= ID DOT STAR",
 /* 227 */ "expr ::= INTEGER",
 /* 228 */ "expr ::= MINUS INTEGER",
 /* 229 */ "expr ::= PLUS INTEGER",
 /* 230 */ "expr ::= FLOAT",
 /* 231 */ "expr ::= MINUS FLOAT",
 /* 232 */ "expr ::= PLUS FLOAT",
 /* 233 */ "expr ::= STRING",
 /* 234 */ "expr ::= NOW",
 /* 235 */ "expr ::= VARIABLE",
 /* 236 */ "expr ::= PLUS VARIABLE",
 /* 237 */ "expr ::= MINUS VARIABLE",
 /* 238 */ "expr ::= BOOL",
 /* 239 */ "expr ::= NULL",
 /* 240 */ "expr ::= ID LP exprlist RP",
 /* 241 */ "expr ::= ID LP STAR RP",
 /* 242 */ "expr ::= expr IS NULL",
 /* 243 */ "expr ::= expr IS NOT NULL",
 /* 244 */ "expr ::= expr LT expr",
 /* 245 */ "expr ::= expr GT expr",
 /* 246 */ "expr ::= expr LE expr",
 /* 247 */ "expr ::= expr GE expr",
 /* 248 */ "expr ::= expr NE expr",
 /* 249 */ "expr ::= expr EQ expr",
 /* 250 */ "expr ::= expr BETWEEN expr AND expr",
 /* 251 */ "expr ::= expr AND expr",
 /* 252 */ "expr ::= expr OR expr",
 /* 253 */ "expr ::= expr PLUS expr",
 /* 254 */ "expr ::= expr MINUS expr",
 /* 255 */ "expr ::= expr STAR expr",
 /* 256 */ "expr ::= expr SLASH expr",
 /* 257 */ "expr ::= expr REM expr",
 /* 258 */ "expr ::= expr LIKE expr",
 /* 259 */ "expr ::= expr IN LP exprlist RP",
 /* 260 */ "exprlist ::= exprlist COMMA expritem",
 /* 261 */ "exprlist ::= expritem",
 /* 262 */ "expritem ::= expr",
 /* 263 */ "expritem ::=",
 /* 264 */ "cmd ::= RESET QUERY CACHE",
 /* 265 */ "cmd ::= SYNCDB ids REPLICA",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 282 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 283 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 284 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 200: /* exprlist */
    case 243: /* selcollist */
    case 257: /* sclp */
{
tSqlExprListDestroy((yypminor->yy441));
}
      break;
    case 213: /* intitemlist */
    case 215: /* keep */
    case 237: /* columnlist */
    case 238: /* tagitemlist */
    case 239: /* tagNamelist */
    case 249: /* fill_opt */
    case 251: /* groupby_opt */
    case 252: /* orderby_opt */
    case 264: /* sortlist */
    case 268: /* grouplist */
{
taosArrayDestroy((yypminor->yy441));
}
      break;
    case 235: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy182));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy236));
}
      break;
    case 244: /* from */
    case 261: /* tablelist */
    case 262: /* sub */
{
destroyRelationInfo((yypminor->yy244));
}
      break;
    case 245: /* where_opt */
    case 253: /* having_opt */
    case 259: /* expr */
    case 269: /* expritem */
{
tSqlExprDestroy((yypminor->yy166));
}
      break;
    case 256: /* union */
{
destroyAllSqlNode((yypminor->yy441));
}
      break;
    case 265: /* sortitem */
{
tVariantDestroy(&(yypminor->yy506));
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
  { 191, 1 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 2 },
  { 192, 3 },
  { 193, 0 },
  { 193, 2 },
  { 195, 0 },
  { 195, 2 },
  { 192, 5 },
  { 192, 5 },
  { 192, 4 },
  { 192, 3 },
  { 192, 5 },
  { 192, 3 },
  { 192, 5 },
  { 192, 3 },
  { 192, 4 },
  { 192, 5 },
  { 192, 5 },
  { 192, 4 },
  { 192, 4 },
  { 192, 3 },
  { 192, 3 },
  { 192, 3 },
  { 192, 2 },
  { 192, 3 },
  { 192, 5 },
  { 192, 5 },
  { 192, 4 },
  { 192, 5 },
  { 192, 3 },
  { 192, 4 },
  { 192, 4 },
  { 192, 4 },
  { 192, 4 },
  { 192, 6 },
  { 192, 6 },
  { 194, 1 },
  { 194, 1 },
  { 196, 2 },
  { 196, 0 },
  { 201, 3 },
  { 201, 0 },
  { 192, 3 },
  { 192, 6 },
  { 192, 5 },
  { 192, 5 },
  { 192, 5 },
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
  { 211, 0 },
  { 211, 2 },
  { 212, 0 },
  { 212, 2 },
  { 199, 9 },
  { 213, 3 },
  { 213, 1 },
  { 214, 1 },
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
  { 229, 2 },
  { 230, 2 },
  { 202, 0 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 202, 2 },
  { 203, 1 },
  { 203, 2 },
  { 197, 0 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 198, 1 },
  { 198, 2 },
  { 231, 1 },
  { 231, 4 },
  { 231, 2 },
  { 232, 1 },
  { 232, 2 },
  { 232, 2 },
  { 192, 3 },
  { 192, 3 },
  { 192, 3 },
  { 192, 3 },
  { 235, 1 },
  { 235, 2 },
  { 233, 6 },
  { 234, 10 },
  { 236, 10 },
  { 236, 13 },
  { 239, 3 },
  { 239, 1 },
  { 233, 5 },
  { 237, 3 },
  { 237, 1 },
  { 241, 2 },
  { 238, 3 },
  { 238, 1 },
  { 242, 1 },
  { 242, 1 },
  { 242, 1 },
  { 242, 1 },
  { 242, 1 },
  { 242, 1 },
  { 242, 2 },
  { 242, 2 },
  { 242, 2 },
  { 242, 2 },
  { 240, 14 },
  { 240, 3 },
  { 256, 1 },
  { 256, 4 },
  { 192, 1 },
  { 240, 2 },
  { 257, 2 },
  { 257, 0 },
  { 243, 4 },
  { 243, 2 },
  { 260, 2 },
  { 260, 1 },
  { 260, 0 },
  { 258, 1 },
  { 258, 0 },
  { 244, 2 },
  { 244, 2 },
  { 262, 3 },
  { 262, 4 },
  { 262, 6 },
  { 261, 2 },
  { 261, 3 },
  { 261, 4 },
  { 261, 5 },
  { 263, 1 },
  { 246, 4 },
  { 246, 6 },
  { 246, 0 },
  { 247, 0 },
  { 247, 7 },
  { 248, 0 },
  { 248, 4 },
  { 249, 0 },
  { 249, 6 },
  { 249, 4 },
  { 250, 4 },
  { 250, 0 },
  { 252, 0 },
  { 252, 3 },
  { 264, 4 },
  { 264, 2 },
  { 266, 2 },
  { 267, 1 },
  { 267, 1 },
  { 267, 0 },
  { 251, 0 },
  { 251, 3 },
  { 268, 3 },
  { 268, 1 },
  { 253, 0 },
  { 253, 2 },
  { 255, 0 },
  { 255, 2 },
  { 255, 4 },
  { 255, 4 },
  { 254, 0 },
  { 254, 2 },
  { 254, 4 },
  { 254, 4 },
  { 245, 0 },
  { 245, 2 },
  { 259, 3 },
  { 259, 1 },
  { 259, 3 },
  { 259, 3 },
  { 259, 1 },
  { 259, 2 },
  { 259, 2 },
  { 259, 1 },
  { 259, 2 },
  { 259, 2 },
  { 259, 1 },
  { 259, 1 },
  { 259, 1 },
  { 259, 2 },
  { 259, 2 },
  { 259, 1 },
  { 259, 1 },
  { 259, 4 },
  { 259, 4 },
  { 259, 3 },
  { 259, 4 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 5 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 3 },
  { 259, 5 },
  { 200, 3 },
  { 200, 1 },
  { 269, 1 },
  { 269, 0 },
  { 192, 3 },
  { 192, 3 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 8 },
  { 192, 9 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 7 },
  { 192, 8 },
  { 192, 9 },
  { 192, 7 },
  { 192, 3 },
  { 192, 5 },
  { 192, 5 },
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy382, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy151);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy151);}
        break;
      case 48: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy441);}
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
      case 176: /* distinct ::= */ yytestcase(yyruleno==176);
{ yygotominor.yy0.n = 0;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy151);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy382, &yymsp[-2].minor.yy0);}
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
    yygotominor.yy151.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy151.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy151.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy151.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy151.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy151.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy151.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy151.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy151.stat    = yymsp[0].minor.yy0;
}
        break;
      case 79: /* intitemlist ::= intitemlist COMMA intitem */
      case 150: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==150);
{ yygotominor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);    }
        break;
      case 80: /* intitemlist ::= intitem */
      case 151: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==151);
{ yygotominor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1); }
        break;
      case 81: /* intitem ::= INTEGER */
      case 152: /* tagitem ::= INTEGER */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= STRING */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= BOOL */ yytestcase(yyruleno==155);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy506, &yymsp[0].minor.yy0); }
        break;
      case 82: /* keep ::= KEEP intitemlist */
{ yygotominor.yy441 = yymsp[0].minor.yy441; }
        break;
      case 83: /* cache ::= CACHE INTEGER */
      case 84: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==84);
      case 85: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==85);
      case 86: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==89);
      case 90: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==90);
      case 91: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==91);
      case 92: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==92);
      case 93: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==93);
      case 94: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==94);
      case 95: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==95);
      case 96: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==96);
      case 97: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==97);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 98: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy382); yygotominor.yy382.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 99: /* db_optr ::= db_optr cache */
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 100: /* db_optr ::= db_optr replica */
      case 117: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==117);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 101: /* db_optr ::= db_optr quorum */
      case 118: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==118);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 102: /* db_optr ::= db_optr days */
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 103: /* db_optr ::= db_optr minrows */
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 104: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 105: /* db_optr ::= db_optr blocks */
      case 120: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==120);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* db_optr ::= db_optr ctime */
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 107: /* db_optr ::= db_optr wal */
      case 122: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==122);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 108: /* db_optr ::= db_optr fsync */
      case 123: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==123);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 109: /* db_optr ::= db_optr comp */
      case 121: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==121);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 110: /* db_optr ::= db_optr prec */
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.precision = yymsp[0].minor.yy0; }
        break;
      case 111: /* db_optr ::= db_optr keep */
      case 119: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==119);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.keep = yymsp[0].minor.yy441; }
        break;
      case 112: /* db_optr ::= db_optr update */
      case 124: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==124);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 113: /* db_optr ::= db_optr cachelast */
      case 125: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==125);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 114: /* topic_optr ::= db_optr */
      case 126: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==126);
{ yygotominor.yy382 = yymsp[0].minor.yy382; yygotominor.yy382.dbType = TSDB_DB_TYPE_TOPIC; }
        break;
      case 115: /* topic_optr ::= topic_optr partitions */
      case 127: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==127);
{ yygotominor.yy382 = yymsp[-1].minor.yy382; yygotominor.yy382.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 116: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy382); yygotominor.yy382.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 128: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yygotominor.yy343, &yymsp[0].minor.yy0);
}
        break;
      case 129: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy369 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yygotominor.yy343, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;  // negative value of name length
    tSetColumnType(&yygotominor.yy343, &yymsp[-3].minor.yy0);
  }
}
        break;
      case 130: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yygotominor.yy343, &yymsp[-1].minor.yy0);
}
        break;
      case 131: /* signed ::= INTEGER */
      case 132: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==132);
{ yygotominor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 133: /* signed ::= MINUS INTEGER */
      case 134: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==135);
      case 136: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==136);
{ yygotominor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 137: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy182;}
        break;
      case 138: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy456);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yygotominor.yy182 = pCreateTable;
}
        break;
      case 139: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy182->childTableInfo, &yymsp[0].minor.yy456);
  yygotominor.yy182 = yymsp[-1].minor.yy182;
}
        break;
      case 140: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yygotominor.yy182 = tSetCreateTableInfo(yymsp[-1].minor.yy441, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yygotominor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
        break;
      case 141: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yygotominor.yy182 = tSetCreateTableInfo(yymsp[-5].minor.yy441, yymsp[-1].minor.yy441, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yygotominor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yygotominor.yy456 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy441, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 143: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yygotominor.yy456 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy441, yymsp[-1].minor.yy441, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
        break;
      case 144: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy441, &yymsp[0].minor.yy0); yygotominor.yy441 = yymsp[-2].minor.yy441;  }
        break;
      case 145: /* tagNamelist ::= ids */
{yygotominor.yy441 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yygotominor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 146: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yygotominor.yy182 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy236, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yygotominor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
        break;
      case 147: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy441, &yymsp[0].minor.yy343); yygotominor.yy441 = yymsp[-2].minor.yy441;  }
        break;
      case 148: /* columnlist ::= column */
{yygotominor.yy441 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yygotominor.yy441, &yymsp[0].minor.yy343);}
        break;
      case 149: /* column ::= ids typename */
{
  tSetColumnInfo(&yygotominor.yy343, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy343);
}
        break;
      case 156: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy506, &yymsp[0].minor.yy0); }
        break;
      case 157: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yygotominor.yy506, &yymsp[0].minor.yy0);}
        break;
      case 158: /* tagitem ::= MINUS INTEGER */
      case 159: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==161);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy506, &yymsp[-1].minor.yy0);
}
        break;
      case 162: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy236 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy441, yymsp[-11].minor.yy244, yymsp[-10].minor.yy166, yymsp[-4].minor.yy441, yymsp[-3].minor.yy441, &yymsp[-9].minor.yy340, &yymsp[-8].minor.yy259, &yymsp[-7].minor.yy348, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy441, &yymsp[0].minor.yy414, &yymsp[-1].minor.yy414, yymsp[-2].minor.yy166);
}
        break;
      case 163: /* select ::= LP select RP */
{yygotominor.yy236 = yymsp[-1].minor.yy236;}
        break;
      case 164: /* union ::= select */
{ yygotominor.yy441 = setSubclause(NULL, yymsp[0].minor.yy236); }
        break;
      case 165: /* union ::= union UNION ALL select */
{ yygotominor.yy441 = appendSelectClause(yymsp[-3].minor.yy441, yymsp[0].minor.yy236); }
        break;
      case 166: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy441, NULL, TSDB_SQL_SELECT); }
        break;
      case 167: /* select ::= SELECT selcollist */
{
  yygotominor.yy236 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy441, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 168: /* sclp ::= selcollist COMMA */
{yygotominor.yy441 = yymsp[-1].minor.yy441;}
        break;
      case 169: /* sclp ::= */
      case 199: /* orderby_opt ::= */ yytestcase(yyruleno==199);
{yygotominor.yy441 = 0;}
        break;
      case 170: /* selcollist ::= sclp distinct expr as */
{
   yygotominor.yy441 = tSqlExprListAppend(yymsp[-3].minor.yy441, yymsp[-1].minor.yy166,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 171: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yygotominor.yy441 = tSqlExprListAppend(yymsp[-1].minor.yy441, pNode, 0, 0);
}
        break;
      case 172: /* as ::= AS ids */
      case 173: /* as ::= ids */ yytestcase(yyruleno==173);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 174: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 175: /* distinct ::= DISTINCT */
{ yygotominor.yy0 = yymsp[0].minor.yy0;  }
        break;
      case 177: /* from ::= FROM tablelist */
      case 178: /* from ::= FROM sub */ yytestcase(yyruleno==178);
{yygotominor.yy244 = yymsp[0].minor.yy244;}
        break;
      case 179: /* sub ::= LP union RP */
{yygotominor.yy244 = addSubqueryElem(NULL, yymsp[-1].minor.yy441, NULL);}
        break;
      case 180: /* sub ::= LP union RP ids */
{yygotominor.yy244 = addSubqueryElem(NULL, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 181: /* sub ::= sub COMMA LP union RP ids */
{yygotominor.yy244 = addSubqueryElem(yymsp[-5].minor.yy244, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 182: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 183: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 184: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(yymsp[-3].minor.yy244, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 185: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(yymsp[-4].minor.yy244, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 186: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 187: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy340.interval = yymsp[-1].minor.yy0; yygotominor.yy340.offset.n = 0;}
        break;
      case 188: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy340.interval = yymsp[-3].minor.yy0; yygotominor.yy340.offset = yymsp[-1].minor.yy0;}
        break;
      case 189: /* interval_opt ::= */
{memset(&yygotominor.yy340, 0, sizeof(yygotominor.yy340));}
        break;
      case 190: /* session_option ::= */
{yygotominor.yy259.col.n = 0; yygotominor.yy259.gap.n = 0;}
        break;
      case 191: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yygotominor.yy259.col = yymsp[-4].minor.yy0;
   yygotominor.yy259.gap = yymsp[-1].minor.yy0;
}
        break;
      case 192: /* windowstate_option ::= */
{ yygotominor.yy348.col.n = 0; yygotominor.yy348.col.z = NULL;}
        break;
      case 193: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yygotominor.yy348.col = yymsp[-1].minor.yy0; }
        break;
      case 194: /* fill_opt ::= */
{ yygotominor.yy441 = 0;     }
        break;
      case 195: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy441, &A, -1, 0);
    yygotominor.yy441 = yymsp[-1].minor.yy441;
}
        break;
      case 196: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy441 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 197: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 198: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 200: /* orderby_opt ::= ORDER BY sortlist */
{yygotominor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 201: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy441 = tVariantListAppend(yymsp[-3].minor.yy441, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
        break;
      case 202: /* sortlist ::= item sortorder */
{
  yygotominor.yy441 = tVariantListAppend(NULL, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
        break;
      case 203: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy506, &yymsp[-1].minor.yy0);
}
        break;
      case 204: /* sortorder ::= ASC */
      case 206: /* sortorder ::= */ yytestcase(yyruleno==206);
{ yygotominor.yy112 = TSDB_ORDER_ASC; }
        break;
      case 205: /* sortorder ::= DESC */
{ yygotominor.yy112 = TSDB_ORDER_DESC;}
        break;
      case 207: /* groupby_opt ::= */
{ yygotominor.yy441 = 0;}
        break;
      case 208: /* groupby_opt ::= GROUP BY grouplist */
{ yygotominor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 209: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);
}
        break;
      case 210: /* grouplist ::= item */
{
  yygotominor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1);
}
        break;
      case 211: /* having_opt ::= */
      case 221: /* where_opt ::= */ yytestcase(yyruleno==221);
      case 263: /* expritem ::= */ yytestcase(yyruleno==263);
{yygotominor.yy166 = 0;}
        break;
      case 212: /* having_opt ::= HAVING expr */
      case 222: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==222);
      case 262: /* expritem ::= expr */ yytestcase(yyruleno==262);
{yygotominor.yy166 = yymsp[0].minor.yy166;}
        break;
      case 213: /* limit_opt ::= */
      case 217: /* slimit_opt ::= */ yytestcase(yyruleno==217);
{yygotominor.yy414.limit = -1; yygotominor.yy414.offset = 0;}
        break;
      case 214: /* limit_opt ::= LIMIT signed */
      case 218: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==218);
{yygotominor.yy414.limit = yymsp[0].minor.yy369;  yygotominor.yy414.offset = 0;}
        break;
      case 215: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yygotominor.yy414.limit = yymsp[-2].minor.yy369;  yygotominor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 216: /* limit_opt ::= LIMIT signed COMMA signed */
{ yygotominor.yy414.limit = yymsp[0].minor.yy369;  yygotominor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 219: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yygotominor.yy414.limit = yymsp[-2].minor.yy369;  yygotominor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 220: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yygotominor.yy414.limit = yymsp[0].minor.yy369;  yygotominor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 223: /* expr ::= LP expr RP */
{yygotominor.yy166 = yymsp[-1].minor.yy166; yygotominor.yy166->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy166->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
        break;
      case 224: /* expr ::= ID */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 225: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 226: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 227: /* expr ::= INTEGER */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 228: /* expr ::= MINUS INTEGER */
      case 229: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==229);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 230: /* expr ::= FLOAT */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 231: /* expr ::= MINUS FLOAT */
      case 232: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==232);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 233: /* expr ::= STRING */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 234: /* expr ::= NOW */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 235: /* expr ::= VARIABLE */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 236: /* expr ::= PLUS VARIABLE */
      case 237: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==237);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
        break;
      case 238: /* expr ::= BOOL */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 239: /* expr ::= NULL */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
        break;
      case 240: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy166 = tSqlExprCreateFunction(yymsp[-1].minor.yy441, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 241: /* expr ::= ID LP STAR RP */
{ yygotominor.yy166 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 242: /* expr ::= expr IS NULL */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, NULL, TK_ISNULL);}
        break;
      case 243: /* expr ::= expr IS NOT NULL */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-3].minor.yy166, NULL, TK_NOTNULL);}
        break;
      case 244: /* expr ::= expr LT expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LT);}
        break;
      case 245: /* expr ::= expr GT expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GT);}
        break;
      case 246: /* expr ::= expr LE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LE);}
        break;
      case 247: /* expr ::= expr GE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GE);}
        break;
      case 248: /* expr ::= expr NE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_NE);}
        break;
      case 249: /* expr ::= expr EQ expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_EQ);}
        break;
      case 250: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy166); yygotominor.yy166 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy166, yymsp[-2].minor.yy166, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy166, TK_LE), TK_AND);}
        break;
      case 251: /* expr ::= expr AND expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_AND);}
        break;
      case 252: /* expr ::= expr OR expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_OR); }
        break;
      case 253: /* expr ::= expr PLUS expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_PLUS);  }
        break;
      case 254: /* expr ::= expr MINUS expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_MINUS); }
        break;
      case 255: /* expr ::= expr STAR expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_STAR);  }
        break;
      case 256: /* expr ::= expr SLASH expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_DIVIDE);}
        break;
      case 257: /* expr ::= expr REM expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_REM);   }
        break;
      case 258: /* expr ::= expr LIKE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LIKE);  }
        break;
      case 259: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-4].minor.yy166, (tSqlExpr*)yymsp[-1].minor.yy441, TK_IN); }
        break;
      case 260: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy441 = tSqlExprListAppend(yymsp[-2].minor.yy441,yymsp[0].minor.yy166,0, 0);}
        break;
      case 261: /* exprlist ::= expritem */
{yygotominor.yy441 = tSqlExprListAppend(0,yymsp[0].minor.yy166,0, 0);}
        break;
      case 264: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 265: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 272: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 280: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 283: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 284: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
