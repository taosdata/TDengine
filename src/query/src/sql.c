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
#define YYNSTATE 548
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
#define YY_ACTTAB_COUNT (852)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   531,   53,   52,  126,  125,   51,   50,   49,  530,  143,
 /*    10 */   141,  140,   54,   55,   14,   58,   59,  379,  378,  239,
 /*    20 */    48,  450,   57,  300,   62,   60,   63,   61,  343,  342,
 /*    30 */   137,   25,   53,   52,  833,  346,   51,   50,   49,   54,
 /*    40 */    55,  548,   58,   59,   94,   91,  239,   48,  237,   57,
 /*    50 */   300,   62,   60,   63,   61,  443,  440,  442,  439,   53,
 /*    60 */    52,   74,  253,   51,   50,   49,   54,   55,    6,   58,
 /*    70 */    59,  257,  256,  239,   48,  533,   57,  300,   62,   60,
 /*    80 */    63,   61,   68,  362,  145,   92,   53,   52,  531,  470,
 /*    90 */    51,   50,   49,   51,   50,   49,  530,   81,   80,  261,
 /*   100 */   473,   54,   56,  534,   58,   59,  240,  449,  239,   48,
 /*   110 */    77,   57,  300,   62,   60,   63,   61,  397,  396,   34,
 /*   120 */   388,   53,   52,   71,   69,   51,   50,   49,  320,  319,
 /*   130 */   264,  264,  547,  546,  545,  544,  543,  542,  541,  540,
 /*   140 */   539,  538,  537,  536,  535,  345,   35,   55,  220,   58,
 /*   150 */    59,  268,  267,  239,   48,  248,   57,  300,   62,   60,
 /*   160 */    63,   61,  302,   72,  152,  197,   53,   52,  441,  438,
 /*   170 */    51,   50,   49,   58,   59,  425,  446,  239,   48,  235,
 /*   180 */    57,  300,   62,   60,   63,   61,   22,  236,   21,  322,
 /*   190 */    53,   52,  469,  468,   51,   50,   49,  402,  414,  413,
 /*   200 */   412,  411,  410,  409,  408,  407,  406,  405,  404,  403,
 /*   210 */   401,  400,  445,  152,   41,  296,  339,  338,  295,  294,
 /*   220 */   293,  337,  292,  336,  335,  334,  291,  333,  332,  238,
 /*   230 */   433,  531,  365,  444,  282,  437,   88,  436,  197,  530,
 /*   240 */    24,   62,   60,   63,   61,  113,  197,  112,  425,   53,
 /*   250 */    52,  181,   35,   51,   50,   49,  425,  205,    9,  448,
 /*   260 */    35,  217,  218,  522,  206,  301,   20,   33,   19,  129,
 /*   270 */   128,  204,   36,  238,  433,  305,   83,  444,  247,  437,
 /*   280 */   496,  436,  498,  497,  459,   89,  458,  495,  431,  493,
 /*   290 */   492,  494,  531,  491,  490,  318,  288,  139,   25,  468,
 /*   300 */   530,    1,  167,  317,  138,  217,  218,  468,   64,   42,
 /*   310 */    41,  461,  339,  338,  464,  152,  463,  337,  462,  336,
 /*   320 */   335,  334,  242,  333,  332,  457,  474,  456,  114,  108,
 /*   330 */   119,   18,  260,   17,   75,  118,  124,  127,  117,   35,
 /*   340 */   432,  213,  244,  245,  121,   35,  434,   83,  107,  249,
 /*   350 */   106,  246,   64,  315,  314,  354,   35,  188,  186,  184,
 /*   360 */    90,   35,   35,  435,  183,  132,  131,  130,    5,   38,
 /*   370 */   169,   29,   35,   35,   78,  168,  102,   97,  101,   35,
 /*   380 */    42,   16,  316,   15,  432,  685,  468,  374,  312,  280,
 /*   390 */   434,  426,  468,  243,   95,  241,  394,  308,  307,  311,
 /*   400 */   287,   25,  391,  468,  310,  309,   65,  435,  468,  468,
 /*   410 */    93,   35,   65,  285,  387,  230,  229,  370,  371,  468,
 /*   420 */   468,  460,  221,   36,   36,  367,  468,   26,   65,  395,
 /*   430 */   262,    3,  180,   76,  353,  147,  340,  507,  486,  422,
 /*   440 */    36,  487,  116,  179,  179,  389,  179,  152,  506,  362,
 /*   450 */   330,  233,  362,  231,  197,  392,  355,  392,  475,  225,
 /*   460 */   505,  219,  344,  392,  424,  223,  352,  298,  222,  504,
 /*   470 */   503,  502,  501,  500,  499,  489,  485,  484,  483,  482,
 /*   480 */   481,  480,  479,  478,  472,  471,  304,  111,  109,  105,
 /*   490 */   232,    8,  103,  447,  430,    7,  100,   67,   66,  297,
 /*   500 */   421,  420,  419,  418,  417,  416,   96,  303,   94,   28,
 /*   510 */   415,  286,   27,  284,   13,   12,   32,   11,   31,  375,
 /*   520 */   151,  276,  357,  372,  226,   87,  149,  369,   86,   85,
 /*   530 */   368,  148,  264,  366,   30,  266,   82,  363,   10,  349,
 /*   540 */   259,  350,  255,  351,  254,  348,  252,  251,  347,    4,
 /*   550 */     2,  529,  142,  528,  509,  136,  523,  508,  135,  521,
 /*   560 */   488,  134,  178,  133,  514,  327,  331,  341,  177,  324,
 /*   570 */   176,  329,  212,  211,  328,  325,  326,  175,  174,  323,
 /*   580 */   173,  115,  172,   99,  298,  476,  289,   98,  234,  214,
 /*   590 */   160,   46,  157,  155,   70,  377,  162,  278,  271,  161,
 /*   600 */   272,  224,  159,   84,  399,   79,  156,  263,  190,  834,
 /*   610 */   532,  834,  270,  189,  273,  834,  275,  527,  526,  834,
 /*   620 */   525,  834,  834,  834,  158,  277,  373,  279,  386,  154,
 /*   630 */   283,  524,  834,  281,  153,  265,   47,  834,  385,   73,
 /*   640 */   834,  163,  269,  455,  834,   45,  361,  187,  834,  451,
 /*   650 */   834,  380,  834,  834,  185,  520,  376,  519,  834,  834,
 /*   660 */   834,  834,  834,  321,  393,  228,  834,  834,  384,  834,
 /*   670 */   834,  383,  834,  834,  834,  834,  834,  330,  834,  834,
 /*   680 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   690 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   700 */   834,  834,  834,  834,  518,  834,  517,  516,  515,  182,
 /*   710 */   250,  513,  512,  834,  123,  122,  511,  834,  120,  510,
 /*   720 */   192,   44,   37,   40,  477,  171,  467,  466,  834,  110,
 /*   730 */   465,  834,  313,  170,  834,  834,  454,  834,  453,  104,
 /*   740 */   452,  306,  423,  299,   39,  191,  834,   43,  290,  398,
 /*   750 */   166,  165,  390,  164,  274,  150,  146,  364,  360,  359,
 /*   760 */   358,  356,  144,  258,  834,  834,  834,  834,  834,  834,
 /*   770 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   780 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   790 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   800 */   834,  834,  227,  382,  381,  834,  834,  834,  834,  834,
 /*   810 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   820 */   834,  834,  834,  834,  834,  834,  834,  834,  834,  834,
 /*   830 */   834,  834,  193,  207,  210,  209,  208,  203,  202,  196,
 /*   840 */   201,  199,  198,  216,  215,  429,  428,  427,  200,  195,
 /*   850 */   194,   23,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,   76,   77,   37,   38,   39,    9,   62,
 /*    10 */    63,   64,   13,   14,   80,   16,   17,  126,  127,   20,
 /*    20 */    21,   81,   23,   24,   25,   26,   27,   28,   65,   66,
 /*    30 */    67,   91,   33,   34,  191,  192,   37,   38,   39,   13,
 /*    40 */    14,    0,   16,   17,  110,  111,   20,   21,   60,   23,
 /*    50 */    24,   25,   26,   27,   28,    5,    5,    7,    7,   33,
 /*    60 */    34,   80,  137,   37,   38,   39,   13,   14,   80,   16,
 /*    70 */    17,  146,  147,   20,   21,   60,   23,   24,   25,   26,
 /*    80 */    27,   28,   91,  240,  194,  201,   33,   34,    1,  108,
 /*    90 */    37,   38,   39,   37,   38,   39,    9,   81,   81,  256,
 /*   100 */     5,   13,   14,   59,   16,   17,  200,   81,   20,   21,
 /*   110 */   111,   23,   24,   25,   26,   27,   28,  233,  234,  235,
 /*   120 */   236,   33,   34,   91,  133,   37,   38,   39,   33,   34,
 /*   130 */   114,  114,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,   56,   57,   58,  194,   14,   61,   16,
 /*   150 */    17,  261,  262,   20,   21,  194,   23,   24,   25,   26,
 /*   160 */    27,   28,   15,  131,  194,  259,   33,   34,  118,  118,
 /*   170 */    37,   38,   39,   16,   17,  269,    1,   20,   21,  200,
 /*   180 */    23,   24,   25,   26,   27,   28,  140,  200,  142,  237,
 /*   190 */    33,   34,  231,  241,   37,   38,   39,  215,  216,  217,
 /*   200 */   218,  219,  220,  221,  222,  223,  224,  225,  226,  227,
 /*   210 */   228,  229,   37,  194,   92,   93,   94,   95,   96,   97,
 /*   220 */    98,   99,  100,  101,  102,  103,  104,  105,  106,    1,
 /*   230 */     2,    1,   91,    5,  264,    7,  266,    9,  259,    9,
 /*   240 */    44,   25,   26,   27,   28,  140,  259,  142,  269,   33,
 /*   250 */    34,   91,  194,   37,   38,   39,  269,   61,  117,  112,
 /*   260 */   194,   33,   34,   83,   68,   37,  140,   80,  142,   73,
 /*   270 */    74,   75,   91,    1,    2,   79,   80,    5,   68,    7,
 /*   280 */   215,    9,  217,  218,    5,  266,    7,  222,   81,  224,
 /*   290 */   225,  226,    1,  228,  229,  237,  109,   21,   91,  241,
 /*   300 */     9,  202,  203,  237,   21,   33,   34,  241,   80,  113,
 /*   310 */    92,    2,   94,   95,    5,  194,    7,   99,    9,  101,
 /*   320 */   102,  103,   68,  105,  106,    5,   81,    7,   62,   63,
 /*   330 */    64,  140,  136,  142,  138,   69,   70,   71,   72,  194,
 /*   340 */   112,  145,   33,   34,   78,  194,  118,   80,  140,  139,
 /*   350 */   142,  141,   80,  143,  144,   37,  194,   62,   63,   64,
 /*   360 */   243,  194,  194,  135,   69,   70,   71,   72,   62,   63,
 /*   370 */    64,   80,  194,  194,  257,   69,   70,   71,   72,  194,
 /*   380 */   113,  140,  237,  142,  112,    0,  241,  266,  237,  268,
 /*   390 */   118,   81,  241,  139,  201,  141,   81,  143,  144,  237,
 /*   400 */    81,   91,   81,  241,  237,  237,   91,  135,  241,  241,
 /*   410 */    91,  194,   91,   81,   81,  237,  237,   81,   81,  241,
 /*   420 */   241,  112,  237,   91,   91,   81,  241,   91,   91,  236,
 /*   430 */    81,  197,  198,  201,  116,   91,  213,  214,  199,  199,
 /*   440 */    91,  199,   76,  204,  204,  194,  204,  194,    5,  240,
 /*   450 */    84,  238,  240,  238,  259,  242,  194,  242,  241,  238,
 /*   460 */     5,  193,  194,  242,  269,  256,  234,   82,  256,    5,
 /*   470 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   480 */     5,    5,    5,    5,    5,    5,   58,  142,  142,  142,
 /*   490 */   239,   80,  142,  112,   81,   80,   76,   16,   16,   15,
 /*   500 */    83,    5,    5,    5,    5,    5,   76,   24,  110,   80,
 /*   510 */     9,  109,   80,  109,   80,  125,   91,  125,   91,  266,
 /*   520 */    80,   80,  260,   81,    1,   80,   91,   81,   80,   91,
 /*   530 */    81,   80,  114,   81,   80,   91,  115,   81,   80,    5,
 /*   540 */   137,   94,    5,   93,  148,    5,    5,  148,    5,  197,
 /*   550 */   202,  195,   60,  195,  214,  196,  195,    5,  196,  195,
 /*   560 */   230,  196,  205,  196,  195,   54,  107,   82,  208,   50,
 /*   570 */   207,   85,  195,  195,   87,   86,   88,  209,  206,   89,
 /*   580 */   210,   90,  211,  201,   82,  212,  195,  201,  195,  195,
 /*   590 */   247,  134,  250,  252,  132,  195,  245,  263,  263,  246,
 /*   600 */   195,  263,  248,  195,  230,  195,  251,  195,  194,  270,
 /*   610 */   194,  270,  119,  194,  120,  270,  121,  194,  194,  270,
 /*   620 */   194,  270,  270,  270,  249,  122,  118,  123,  240,  253,
 /*   630 */   124,  194,  270,  128,  254,  240,  129,  270,  255,  130,
 /*   640 */   270,  244,  263,  242,  270,  258,  240,  194,  270,  242,
 /*   650 */   270,  267,  270,  270,  194,  194,  267,  194,  270,  270,
 /*   660 */   270,  270,  270,  232,  242,  232,  270,  270,  232,  270,
 /*   670 */   270,  232,  270,  270,  270,  270,  270,   84,  270,  270,
 /*   680 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   690 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   700 */   270,  270,  270,  270,  194,  270,  194,  194,  194,  194,
 /*   710 */   194,  194,  194,  270,  194,  194,  194,  270,  194,  194,
 /*   720 */   194,  194,  194,  194,  194,  194,  194,  194,  270,  194,
 /*   730 */   194,  270,  194,  194,  270,  270,  194,  270,  194,  194,
 /*   740 */   194,  194,  194,  194,  194,  194,  270,  194,  194,  194,
 /*   750 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   760 */   194,  194,  194,  194,  270,  270,  270,  270,  270,  270,
 /*   770 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   780 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   790 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   800 */   270,  270,  232,  232,  232,  270,  270,  270,  270,  270,
 /*   810 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   820 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   830 */   270,  270,  259,  259,  259,  259,  259,  259,  259,  259,
 /*   840 */   259,  259,  259,  259,  259,  259,  259,  259,  259,  259,
 /*   850 */   259,  259,
};
#define YY_SHIFT_USE_DFLT (-110)
#define YY_SHIFT_COUNT (346)
#define YY_SHIFT_MIN   (-109)
#define YY_SHIFT_MAX   (593)
static const short yy_shift_ofst[] = {
 /*     0 */   196,  122,  122,  218,  218,  502,  228,  272,  272,  291,
 /*    10 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    20 */   230,  230,  230,   -1,   87,  272,  309,  309,  309,  267,
 /*    30 */   267,  230,  230,  230,  385,  230,  230,  366,  502,  593,
 /*    40 */   593,  552, -110, -110, -110,  272,  272,  272,  272,  272,
 /*    50 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*    60 */   272,  272,  272,  272,  272,  309,  309,  309,   95,   95,
 /*    70 */    95,   95,   95,   95,   95,  230,  230,  230,  318,  230,
 /*    80 */   230,  230,  267,  267,  230,  230,  230,  230, -109, -109,
 /*    90 */   141,  267,  230,  230,  230,  230,  230,  230,  230,  230,
 /*   100 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*   110 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*   120 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*   130 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*   140 */   230,  230,  230,  230,  492,  492,  492,  508,  508,  508,
 /*   150 */   492,  508,  492,  509,  462,  507,  506,  505,  504,  503,
 /*   160 */   495,  494,  493,  457,  492,  492,  492,  459,  502,  502,
 /*   170 */   492,  492,  491,  490,  519,  489,  488,  511,  487,  486,
 /*   180 */   459,  552,  492,  485,  485,  492,  485,  492,  485,  492,
 /*   190 */   492, -110, -110,   26,   53,   53,   88,   53,  133,  157,
 /*   200 */   216,  216,  216,  216,  266,  306,  295,  -32,  -32,  -32,
 /*   210 */   -32,  254,  210,  -75,  -66,   56,   56,   51,   50,  -37,
 /*   220 */   -53,  349,   17,   16,  344,  337,  336,   -9,   32,  333,
 /*   230 */   332,  321,  319,  315,  187,  310,  207,  175,  -12,  147,
 /*   240 */   -60,  241,  208,  191,  320,  279,  126,  105,  -19,   46,
 /*   250 */   -73,  543,  399,  541,  540,  396,  537,  534,  447,  450,
 /*   260 */   403,  418,  404,  458,  421,  456,  454,  444,  438,  452,
 /*   270 */   451,  449,  435,  448,  446,  445,  523,  441,  442,  440,
 /*   280 */   427,  392,  425,  390,  434,  404,  432,  402,  429,  398,
 /*   290 */   430,  501,  500,  499,  498,  497,  496,  417,  484,  420,
 /*   300 */   415,  413,  381,  411,  483,  428,  482,  350,  347,  181,
 /*   310 */   181,  181,  181,  481,  346,  345,  181,  181,  181,  480,
 /*   320 */   479,  245,  181,  478,  477,  476,  475,  474,  473,  472,
 /*   330 */   471,  470,  469,  468,  467,  466,  465,  464,  455,  443,
 /*   340 */   160,  180,  283,  276,   15,   44,   41,
};
#define YY_REDUCE_USE_DFLT (-158)
#define YY_REDUCE_COUNT (192)
#define YY_REDUCE_MIN   (-157)
#define YY_REDUCE_MAX   (592)
static const short yy_reduce_ofst[] = {
 /*     0 */  -157,  -18,  -18,   65,   65, -116,  -13,  -21,  -94, -110,
 /*    10 */   185,  121,  -30,  179,  178,  168,  167,  162,  151,  145,
 /*    20 */    66,   58,  -48,  262,  268,  195,  221,  215,  213,  212,
 /*    30 */   209,  253,   19,  251,  193,  -39,  217,  242,  232,  240,
 /*    40 */   239,  223,  117,   99,  234,  592,  591,  590,  589,  588,
 /*    50 */   587,  586,  585,  584,  583,  582,  581,  580,  579,  578,
 /*    60 */   577,  576,  575,  574,  573,  422,  407,  401,  572,  571,
 /*    70 */   570,  439,  436,  433,  431,  569,  568,  567,  387,  566,
 /*    80 */   565,  564,  406,  395,  563,  562,  561,  560,  389,  384,
 /*    90 */   397,  388,  559,  558,  557,  556,  555,  554,  553,  551,
 /*   100 */   550,  549,  548,  547,  546,  545,  544,  542,  539,  538,
 /*   110 */   536,  535,  533,  532,  531,  530,  529,  528,  527,  526,
 /*   120 */   525,  524,  522,  521,  520,  518,  517,  516,  515,  514,
 /*   130 */   513,  512,  510,  463,  461,  460,  453,  437,  426,  424,
 /*   140 */   423,  419,  416,  414,  412,  410,  408,  379,  338,  335,
 /*   150 */   405,  334,  400,  383,  380,  376,  341,  355,  342,  375,
 /*   160 */   354,  343,  353,  351,  394,  393,  391,  374,  386,  382,
 /*   170 */   378,  377,  373,  371,  370,  372,  368,  363,  360,  357,
 /*   180 */   330,  340,  369,  367,  365,  364,  362,  361,  359,  358,
 /*   190 */   356,  348,  352,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   832,  662,  605,  674,  592,  602,  810,  810,  810,  832,
 /*    10 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*    20 */   832,  832,  832,  721,  564,  810,  832,  832,  832,  832,
 /*    30 */   832,  832,  832,  832,  602,  832,  832,  608,  602,  608,
 /*    40 */   608,  832,  716,  646,  664,  832,  832,  832,  832,  832,
 /*    50 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*    60 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*    70 */   832,  832,  832,  832,  832,  832,  832,  832,  723,  729,
 /*    80 */   726,  832,  832,  832,  731,  832,  832,  832,  753,  753,
 /*    90 */   714,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   100 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   110 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   120 */   590,  832,  588,  832,  832,  832,  832,  832,  832,  832,
 /*   130 */   832,  832,  832,  832,  832,  832,  832,  575,  832,  832,
 /*   140 */   832,  832,  832,  832,  566,  566,  566,  832,  832,  832,
 /*   150 */   566,  832,  566,  760,  764,  758,  746,  754,  745,  741,
 /*   160 */   739,  737,  736,  768,  566,  566,  566,  606,  602,  602,
 /*   170 */   566,  566,  624,  622,  620,  612,  618,  614,  616,  610,
 /*   180 */   593,  832,  566,  600,  600,  566,  600,  566,  600,  566,
 /*   190 */   566,  646,  664,  832,  769,  759,  832,  809,  799,  798,
 /*   200 */   805,  797,  796,  795,  832,  832,  832,  791,  794,  793,
 /*   210 */   792,  832,  832,  832,  832,  801,  800,  832,  832,  832,
 /*   220 */   832,  832,  832,  832,  832,  832,  832,  765,  761,  832,
 /*   230 */   832,  832,  832,  832,  832,  832,  832,  832,  771,  832,
 /*   240 */   832,  832,  832,  832,  832,  832,  832,  832,  676,  832,
 /*   250 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   260 */   832,  713,  832,  832,  832,  832,  832,  725,  724,  832,
 /*   270 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   280 */   755,  832,  747,  832,  832,  688,  832,  832,  832,  832,
 /*   290 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   300 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  828,
 /*   310 */   823,  824,  821,  832,  832,  832,  820,  815,  816,  832,
 /*   320 */   832,  832,  813,  832,  832,  832,  832,  832,  832,  832,
 /*   330 */   832,  832,  832,  832,  832,  832,  832,  832,  832,  832,
 /*   340 */   630,  832,  573,  571,  832,  562,  832,  831,  830,  829,
 /*   350 */   812,  811,  684,  722,  718,  720,  719,  717,  730,  727,
 /*   360 */   728,  712,  711,  710,  732,  715,  735,  734,  738,  740,
 /*   370 */   743,  742,  744,  733,  757,  756,  749,  750,  752,  751,
 /*   380 */   748,  767,  766,  763,  762,  709,  694,  689,  686,  693,
 /*   390 */   692,  691,  699,  698,  690,  687,  683,  682,  607,  663,
 /*   400 */   661,  660,  659,  658,  657,  656,  655,  654,  653,  652,
 /*   410 */   651,  650,  649,  648,  647,  642,  638,  636,  635,  634,
 /*   420 */   631,  601,  604,  603,  807,  808,  806,  804,  803,  802,
 /*   430 */   788,  787,  786,  785,  782,  781,  780,  777,  783,  779,
 /*   440 */   776,  784,  778,  775,  774,  773,  772,  790,  789,  770,
 /*   450 */   596,  827,  826,  825,  822,  819,  708,  707,  706,  705,
 /*   460 */   704,  703,  702,  701,  700,  818,  817,  814,  696,  697,
 /*   470 */   678,  681,  680,  679,  677,  695,  626,  625,  623,  621,
 /*   480 */   613,  619,  615,  617,  611,  609,  595,  594,  675,  645,
 /*   490 */   673,  672,  671,  670,  669,  668,  667,  666,  665,  644,
 /*   500 */   643,  641,  640,  639,  637,  633,  632,  628,  629,  627,
 /*   510 */   591,  589,  587,  586,  585,  584,  583,  582,  581,  580,
 /*   520 */   579,  578,  599,  577,  576,  574,  572,  570,  569,  568,
 /*   530 */   598,  597,  567,  565,  563,  561,  560,  559,  558,  557,
 /*   540 */   556,  555,  554,  553,  552,  551,  550,  549,
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
  "NULL",          "SELECT",        "UNION",         "ALL",         
  "DISTINCT",      "FROM",          "VARIABLE",      "INTERVAL",    
  "SESSION",       "STATE_WINDOW",  "FILL",          "SLIDING",     
  "ORDER",         "BY",            "ASC",           "DESC",        
  "GROUP",         "HAVING",        "LIMIT",         "OFFSET",      
  "SLIMIT",        "SOFFSET",       "WHERE",         "NOW",         
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
      case 175: /* distinct ::= */ yytestcase(yyruleno==175);
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
      case 157: /* tagitem ::= MINUS INTEGER */
      case 158: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==160);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy506, &yymsp[-1].minor.yy0);
}
        break;
      case 161: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy236 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy441, yymsp[-11].minor.yy244, yymsp[-10].minor.yy166, yymsp[-4].minor.yy441, yymsp[-3].minor.yy441, &yymsp[-9].minor.yy340, &yymsp[-8].minor.yy259, &yymsp[-7].minor.yy348, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy441, &yymsp[0].minor.yy414, &yymsp[-1].minor.yy414, yymsp[-2].minor.yy166);
}
        break;
      case 162: /* select ::= LP select RP */
{yygotominor.yy236 = yymsp[-1].minor.yy236;}
        break;
      case 163: /* union ::= select */
{ yygotominor.yy441 = setSubclause(NULL, yymsp[0].minor.yy236); }
        break;
      case 164: /* union ::= union UNION ALL select */
{ yygotominor.yy441 = appendSelectClause(yymsp[-3].minor.yy441, yymsp[0].minor.yy236); }
        break;
      case 165: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy441, NULL, TSDB_SQL_SELECT); }
        break;
      case 166: /* select ::= SELECT selcollist */
{
  yygotominor.yy236 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy441, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 167: /* sclp ::= selcollist COMMA */
{yygotominor.yy441 = yymsp[-1].minor.yy441;}
        break;
      case 168: /* sclp ::= */
      case 198: /* orderby_opt ::= */ yytestcase(yyruleno==198);
{yygotominor.yy441 = 0;}
        break;
      case 169: /* selcollist ::= sclp distinct expr as */
{
   yygotominor.yy441 = tSqlExprListAppend(yymsp[-3].minor.yy441, yymsp[-1].minor.yy166,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 170: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yygotominor.yy441 = tSqlExprListAppend(yymsp[-1].minor.yy441, pNode, 0, 0);
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
{yygotominor.yy244 = yymsp[0].minor.yy244;}
        break;
      case 178: /* sub ::= LP union RP */
{yygotominor.yy244 = addSubqueryElem(NULL, yymsp[-1].minor.yy441, NULL);}
        break;
      case 179: /* sub ::= LP union RP ids */
{yygotominor.yy244 = addSubqueryElem(NULL, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 180: /* sub ::= sub COMMA LP union RP ids */
{yygotominor.yy244 = addSubqueryElem(yymsp[-5].minor.yy244, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 181: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 182: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 183: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(yymsp[-3].minor.yy244, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 184: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy244 = setTableNameList(yymsp[-4].minor.yy244, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 185: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 186: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy340.interval = yymsp[-1].minor.yy0; yygotominor.yy340.offset.n = 0;}
        break;
      case 187: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy340.interval = yymsp[-3].minor.yy0; yygotominor.yy340.offset = yymsp[-1].minor.yy0;}
        break;
      case 188: /* interval_opt ::= */
{memset(&yygotominor.yy340, 0, sizeof(yygotominor.yy340));}
        break;
      case 189: /* session_option ::= */
{yygotominor.yy259.col.n = 0; yygotominor.yy259.gap.n = 0;}
        break;
      case 190: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yygotominor.yy259.col = yymsp[-4].minor.yy0;
   yygotominor.yy259.gap = yymsp[-1].minor.yy0;
}
        break;
      case 191: /* windowstate_option ::= */
{ yygotominor.yy348.col.n = 0; yygotominor.yy348.col.z = NULL;}
        break;
      case 192: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yygotominor.yy348.col = yymsp[-1].minor.yy0; }
        break;
      case 193: /* fill_opt ::= */
{ yygotominor.yy441 = 0;     }
        break;
      case 194: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy441, &A, -1, 0);
    yygotominor.yy441 = yymsp[-1].minor.yy441;
}
        break;
      case 195: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy441 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 196: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 197: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 199: /* orderby_opt ::= ORDER BY sortlist */
{yygotominor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 200: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy441 = tVariantListAppend(yymsp[-3].minor.yy441, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
        break;
      case 201: /* sortlist ::= item sortorder */
{
  yygotominor.yy441 = tVariantListAppend(NULL, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
        break;
      case 202: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy506, &yymsp[-1].minor.yy0);
}
        break;
      case 203: /* sortorder ::= ASC */
      case 205: /* sortorder ::= */ yytestcase(yyruleno==205);
{ yygotominor.yy112 = TSDB_ORDER_ASC; }
        break;
      case 204: /* sortorder ::= DESC */
{ yygotominor.yy112 = TSDB_ORDER_DESC;}
        break;
      case 206: /* groupby_opt ::= */
{ yygotominor.yy441 = 0;}
        break;
      case 207: /* groupby_opt ::= GROUP BY grouplist */
{ yygotominor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 208: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);
}
        break;
      case 209: /* grouplist ::= item */
{
  yygotominor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1);
}
        break;
      case 210: /* having_opt ::= */
      case 220: /* where_opt ::= */ yytestcase(yyruleno==220);
      case 262: /* expritem ::= */ yytestcase(yyruleno==262);
{yygotominor.yy166 = 0;}
        break;
      case 211: /* having_opt ::= HAVING expr */
      case 221: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==221);
      case 261: /* expritem ::= expr */ yytestcase(yyruleno==261);
{yygotominor.yy166 = yymsp[0].minor.yy166;}
        break;
      case 212: /* limit_opt ::= */
      case 216: /* slimit_opt ::= */ yytestcase(yyruleno==216);
{yygotominor.yy414.limit = -1; yygotominor.yy414.offset = 0;}
        break;
      case 213: /* limit_opt ::= LIMIT signed */
      case 217: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==217);
{yygotominor.yy414.limit = yymsp[0].minor.yy369;  yygotominor.yy414.offset = 0;}
        break;
      case 214: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yygotominor.yy414.limit = yymsp[-2].minor.yy369;  yygotominor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 215: /* limit_opt ::= LIMIT signed COMMA signed */
{ yygotominor.yy414.limit = yymsp[0].minor.yy369;  yygotominor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 218: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yygotominor.yy414.limit = yymsp[-2].minor.yy369;  yygotominor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 219: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yygotominor.yy414.limit = yymsp[0].minor.yy369;  yygotominor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 222: /* expr ::= LP expr RP */
{yygotominor.yy166 = yymsp[-1].minor.yy166; yygotominor.yy166->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy166->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
        break;
      case 223: /* expr ::= ID */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 224: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 225: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 226: /* expr ::= INTEGER */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 227: /* expr ::= MINUS INTEGER */
      case 228: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 229: /* expr ::= FLOAT */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 230: /* expr ::= MINUS FLOAT */
      case 231: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==231);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 232: /* expr ::= STRING */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 233: /* expr ::= NOW */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 234: /* expr ::= VARIABLE */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 235: /* expr ::= PLUS VARIABLE */
      case 236: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
        break;
      case 237: /* expr ::= BOOL */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 238: /* expr ::= NULL */
{ yygotominor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
        break;
      case 239: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy166 = tSqlExprCreateFunction(yymsp[-1].minor.yy441, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 240: /* expr ::= ID LP STAR RP */
{ yygotominor.yy166 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 241: /* expr ::= expr IS NULL */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, NULL, TK_ISNULL);}
        break;
      case 242: /* expr ::= expr IS NOT NULL */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-3].minor.yy166, NULL, TK_NOTNULL);}
        break;
      case 243: /* expr ::= expr LT expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LT);}
        break;
      case 244: /* expr ::= expr GT expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GT);}
        break;
      case 245: /* expr ::= expr LE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LE);}
        break;
      case 246: /* expr ::= expr GE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GE);}
        break;
      case 247: /* expr ::= expr NE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_NE);}
        break;
      case 248: /* expr ::= expr EQ expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_EQ);}
        break;
      case 249: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy166); yygotominor.yy166 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy166, yymsp[-2].minor.yy166, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy166, TK_LE), TK_AND);}
        break;
      case 250: /* expr ::= expr AND expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_AND);}
        break;
      case 251: /* expr ::= expr OR expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_OR); }
        break;
      case 252: /* expr ::= expr PLUS expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_PLUS);  }
        break;
      case 253: /* expr ::= expr MINUS expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_MINUS); }
        break;
      case 254: /* expr ::= expr STAR expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_STAR);  }
        break;
      case 255: /* expr ::= expr SLASH expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_DIVIDE);}
        break;
      case 256: /* expr ::= expr REM expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_REM);   }
        break;
      case 257: /* expr ::= expr LIKE expr */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LIKE);  }
        break;
      case 258: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy166 = tSqlExprCreate(yymsp[-4].minor.yy166, (tSqlExpr*)yymsp[-1].minor.yy441, TK_IN); }
        break;
      case 259: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy441 = tSqlExprListAppend(yymsp[-2].minor.yy441,yymsp[0].minor.yy166,0, 0);}
        break;
      case 260: /* exprlist ::= expritem */
{yygotominor.yy441 = tSqlExprListAppend(0,yymsp[0].minor.yy166,0, 0);}
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
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
