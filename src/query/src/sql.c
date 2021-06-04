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
#define YYNOCODE 269
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateDbInfo yy22;
  TAOS_FIELD yy47;
  SRelationInfo* yy52;
  SCreateAcctInfo yy83;
  SSessionWindowVal yy84;
  tSqlExpr* yy162;
  SWindowStateVal yy176;
  int yy196;
  SLimitVal yy230;
  SArray* yy325;
  SIntervalVal yy328;
  int64_t yy373;
  SCreateTableSql* yy422;
  tVariant yy442;
  SCreatedTableInfo yy504;
  SSqlNode* yy536;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 544
#define YYNRULE 281
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
#define YY_ACTTAB_COUNT (845)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   527,   53,   52,  544,  239,   51,   50,   49,  526,  826,
 /*    10 */   345,  236,   54,   55,   74,   58,   59,  126,  125,  238,
 /*    20 */    48,  530,   57,  299,   62,   60,   63,   61,  143,  141,
 /*    30 */   140,    6,   53,   52,   90,  252,   51,   50,   49,   54,
 /*    40 */    55,  458,   58,   59,  256,  255,  238,   48,   78,   57,
 /*    50 */   299,   62,   60,   63,   61,  440,  361,  439,   36,   53,
 /*    60 */    52,  196,  443,   51,   50,   49,   54,   55,  364,   58,
 /*    70 */    59,  422,  260,  238,   48,    9,   57,  299,   62,   60,
 /*    80 */    63,   61,  342,  341,  137,   71,   53,   52,  527,   14,
 /*    90 */    51,   50,   49,   51,   50,   49,  526,   92,  442,  529,
 /*   100 */   321,   54,   56,  456,   58,   59,   72,  446,  238,   48,
 /*   110 */    77,   57,  299,   62,   60,   63,   61,  437,   94,  436,
 /*   120 */    91,   53,   52,  461,  139,   51,   50,   49,  394,  393,
 /*   130 */    35,  387,  543,  542,  541,  540,  539,  538,  537,  536,
 /*   140 */   535,  534,  533,  532,  531,  344,  447,   55,  219,   58,
 /*   150 */    59,  319,  318,  238,   48,  138,   57,  299,   62,   60,
 /*   160 */    63,   61,  301,  152,  428,  152,   53,   52,  438,   68,
 /*   170 */    51,   50,   49,   58,   59,   25,  678,  238,   48,  145,
 /*   180 */    57,  299,   62,   60,   63,   61,  499,  497,  498,  496,
 /*   190 */    53,   52,   69,   25,   51,   50,   49,  399,  353,  411,
 /*   200 */   410,  409,  408,  407,  406,  405,  404,  403,  402,  401,
 /*   210 */   400,  398,  397,   29,  295,  338,  337,  294,  293,  292,
 /*   220 */   336,  291,  335,  334,  333,  290,  332,  331,  237,  430,
 /*   230 */   435,  281,  441,   88,  434,  373,  433,  279,  527,   24,
 /*   240 */    62,   60,   63,   61,  267,  266,  526,  527,   53,   52,
 /*   250 */   378,  377,   51,   50,   49,  526,  204,  234,  297,  445,
 /*   260 */   216,  217,  339,  205,  300,  361,  423,  391,  129,  128,
 /*   270 */   203,  518,  237,  430,  304,   83,  441,  352,  434,  484,
 /*   280 */   433,  222,  486,  485,   22,  111,   21,  483,  495,  481,
 /*   290 */   480,  482,  286,  479,  478,   25,   67,  244,  113,   20,
 /*   300 */   112,   19,  494,  241,  216,  217,   76,   64,   42,   29,
 /*   310 */   501,  338,  337,  504,  196,  503,  336,  502,  335,  334,
 /*   320 */   333,   93,  332,  331,  422,  245,   30,  114,  108,  119,
 /*   330 */    18,  259,   17,   75,  118,  124,  127,  117,  351,  429,
 /*   340 */   212,  247,  248,  121,  116,  431,  187,  185,  183,   36,
 /*   350 */   235,   64,  329,  182,  132,  131,  130,    5,   39,  169,
 /*   360 */    34,  457,  432,   36,  168,  102,   97,  101,  246,   36,
 /*   370 */   243,  390,  314,  313,  242,  284,  240,   36,  307,  306,
 /*   380 */   107,  386,  106,  429,   67,   36,   36,  369,  287,  431,
 /*   390 */    36,  317,   36,   16,  456,   15,   36,   36,    3,  180,
 /*   400 */    67,    1,  167,   81,   37,  316,  432,  196,  456,  493,
 /*   410 */    37,  315,   80,   83,  456,  370,   26,  422,  366,  311,
 /*   420 */   500,  261,  456,   95,  474,   36,  388,  310,  309,  179,
 /*   430 */   456,  456,  308,  152,  229,  456,  263,  456,  228,  220,
 /*   440 */   232,  456,  456,  419,   67,  263,   42,  147,  179,  475,
 /*   450 */    37,  152,  361,  354,  179,  230,  224,  392,  492,  218,
 /*   460 */   343,  196,  491,  490,  489,  488,  495,  487,  221,  231,
 /*   470 */   463,  421,  477,  473,  472,  471,  470,  469,  468,  467,
 /*   480 */   466,  495,  495,   37,  462,  460,  459,  109,  105,  103,
 /*   490 */   303,    8,   66,   65,  444,  427,    7,  100,  296,  417,
 /*   500 */   418,  416,  302,   89,  415,  414,  413,  412,   94,   96,
 /*   510 */    28,   27,   12,  285,  283,   13,   11,  356,   33,   32,
 /*   520 */   151,  374,  371,  275,  225,   87,  263,  368,   86,  149,
 /*   530 */   367,  148,   85,  365,  265,   31,  350,  362,   82,  258,
 /*   540 */   349,   10,  348,  254,  253,  347,  251,  346,  250,    4,
 /*   550 */     2,  525,  142,  524,  476,  136,  519,  177,  135,  517,
 /*   560 */   510,  340,  134,  133,  176,  178,  326,  328,  323,  330,
 /*   570 */   211,  327,  175,  325,  174,  324,  322,   99,  115,  173,
 /*   580 */   210,  172,   98,  464,  297,  288,  162,  233,  213,  160,
 /*   590 */   157,  155,  384,  280,  161,   47,  376,  159,   46,  277,
 /*   600 */   158,  396,  271,  156,  189,  269,  154,  827,   84,  528,
 /*   610 */   272,  827,  274,   79,  188,  276,  827,  827,  278,  320,
 /*   620 */   262,  282,  523,  372,  385,  163,  153,  264,  522,  227,
 /*   630 */   521,  520,  186,  184,   73,   70,  516,  505,  270,  223,
 /*   640 */   268,  827,  827,  827,  515,  379,  827,  827,  514,  375,
 /*   650 */    45,  827,  192,  827,  827,  827,  827,  827,  383,  827,
 /*   660 */   382,  827,  827,  827,  360,  827,  827,  827,  827,  827,
 /*   670 */   827,  827,  329,  827,  827,  827,  827,  827,  827,  827,
 /*   680 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   690 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   700 */   513,  512,  511,  181,  249,  509,  827,  508,  123,  122,
 /*   710 */   507,  120,  827,  506,  191,   44,   38,  827,   41,  465,
 /*   720 */   171,  455,  454,  110,  453,  312,  170,  451,  450,  104,
 /*   730 */   449,  827,  827,  305,  827,  827,  827,  420,  298,   40,
 /*   740 */   190,   43,  289,  395,  166,  165,  389,  164,  273,  150,
 /*   750 */   146,  363,  359,  358,  357,  355,  144,  257,  827,  827,
 /*   760 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   770 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   780 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   790 */   827,  827,  827,  827,  827,  226,  381,  380,  827,  827,
 /*   800 */   827,  827,  827,  827,  827,  827,  827,  452,  448,  827,
 /*   810 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   820 */   827,  827,  827,  827,  827,  827,  206,  209,  208,  207,
 /*   830 */   202,  201,  195,  200,  198,  197,  215,  214,  426,  425,
 /*   840 */   424,  199,  194,  193,   23,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,    0,  200,   37,   38,   39,    9,  191,
 /*    10 */   192,   60,   13,   14,   80,   16,   17,   76,   77,   20,
 /*    20 */    21,   59,   23,   24,   25,   26,   27,   28,   62,   63,
 /*    30 */    64,   80,   33,   34,  241,  137,   37,   38,   39,   13,
 /*    40 */    14,  107,   16,   17,  146,  147,   20,   21,  255,   23,
 /*    50 */    24,   25,   26,   27,   28,    5,  238,    7,  194,   33,
 /*    60 */    34,  257,    1,   37,   38,   39,   13,   14,  110,   16,
 /*    70 */    17,  267,  254,   20,   21,  117,   23,   24,   25,   26,
 /*    80 */    27,   28,   65,   66,   67,  110,   33,   34,    1,   80,
 /*    90 */    37,   38,   39,   37,   38,   39,    9,  201,   37,   60,
 /*   100 */   236,   13,   14,  239,   16,   17,  131,   81,   20,   21,
 /*   110 */   111,   23,   24,   25,   26,   27,   28,    5,  109,    7,
 /*   120 */   111,   33,   34,    5,   21,   37,   38,   39,  232,  233,
 /*   130 */   234,  235,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,   56,   57,   58,   81,   14,   61,   16,
 /*   150 */    17,   33,   34,   20,   21,   21,   23,   24,   25,   26,
 /*   160 */    27,   28,   15,  194,   81,  194,   33,   34,  118,  110,
 /*   170 */    37,   38,   39,   16,   17,  110,    0,   20,   21,  194,
 /*   180 */    23,   24,   25,   26,   27,   28,    5,    5,    7,    7,
 /*   190 */    33,   34,  133,  110,   37,   38,   39,  213,   37,  215,
 /*   200 */   216,  217,  218,  219,  220,  221,  222,  223,  224,  225,
 /*   210 */   226,  227,  228,   91,   92,   93,   94,   95,   96,   97,
 /*   220 */    98,   99,  100,  101,  102,  103,  104,  105,    1,    2,
 /*   230 */   118,  262,    5,  264,    7,  264,    9,  266,    1,   44,
 /*   240 */    25,   26,   27,   28,  259,  260,    9,    1,   33,   34,
 /*   250 */   126,  127,   37,   38,   39,    9,   61,  200,   82,  112,
 /*   260 */    33,   34,  214,   68,   37,  238,   81,   81,   73,   74,
 /*   270 */    75,   83,    1,    2,   79,   80,    5,  116,    7,  213,
 /*   280 */     9,  254,  216,  217,  140,  142,  142,  221,  240,  223,
 /*   290 */   224,  225,   81,  227,  228,  110,  110,   68,  140,  140,
 /*   300 */   142,  142,    5,   68,   33,   34,  201,   80,  113,   91,
 /*   310 */     2,   93,   94,    5,  257,    7,   98,    9,  100,  101,
 /*   320 */   102,  110,  104,  105,  267,  194,   80,   62,   63,   64,
 /*   330 */   140,  136,  142,  138,   69,   70,   71,   72,  233,  112,
 /*   340 */   145,   33,   34,   78,   76,  118,   62,   63,   64,  194,
 /*   350 */   200,   80,   84,   69,   70,   71,   72,   62,   63,   64,
 /*   360 */    80,  230,  135,  194,   69,   70,   71,   72,  139,  194,
 /*   370 */   141,   81,  143,  144,  139,   81,  141,  194,  143,  144,
 /*   380 */   140,   81,  142,  112,  110,  194,  194,   81,  108,  118,
 /*   390 */   194,  236,  194,  140,  239,  142,  194,  194,  197,  198,
 /*   400 */   110,  202,  203,   81,  110,  236,  135,  257,  239,    5,
 /*   410 */   110,  236,   81,   80,  239,   81,  110,  267,   81,  236,
 /*   420 */   112,   81,  239,  201,  199,  194,  194,  236,  236,  204,
 /*   430 */   239,  239,  236,  194,  236,  239,  114,  239,  236,  236,
 /*   440 */   214,  239,  239,  199,  110,  114,  113,  110,  204,  199,
 /*   450 */   110,  194,  238,  194,  204,  214,  214,  235,    5,  193,
 /*   460 */   194,  257,    5,    5,    5,    5,  240,    5,  254,  237,
 /*   470 */   239,  267,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   480 */     5,  240,  240,  110,   81,    5,    5,  142,  142,  142,
 /*   490 */    58,   80,   16,   16,  112,   81,   80,   76,   15,    5,
 /*   500 */    83,    5,   24,  264,    5,    5,    5,    9,  109,   76,
 /*   510 */    80,   80,  125,  108,  108,   80,  125,  258,  110,  110,
 /*   520 */    80,  264,   81,   80,    1,   80,  114,   81,   80,  110,
 /*   530 */    81,   80,  110,   81,  110,   80,   92,   81,  115,  137,
 /*   540 */    93,   80,    5,    5,  148,    5,    5,    5,  148,  197,
 /*   550 */   202,  195,   60,  195,  229,  196,  195,  208,  196,  195,
 /*   560 */   195,   82,  196,  196,  207,  205,   54,   85,   50,  106,
 /*   570 */   195,   87,  209,   88,  206,   86,   89,  201,   90,  210,
 /*   580 */   195,  211,  201,  212,   82,  195,  243,  195,  195,  245,
 /*   590 */   248,  250,  253,  128,  244,  129,  195,  246,  134,  261,
 /*   600 */   247,  229,  195,  249,  194,  119,  251,  268,  195,  194,
 /*   610 */   120,  268,  121,  195,  194,  122,  268,  268,  123,  231,
 /*   620 */   195,  124,  194,  118,  238,  242,  252,  238,  194,  231,
 /*   630 */   194,  194,  194,  194,  130,  132,  194,  240,  261,  261,
 /*   640 */   261,  268,  268,  268,  194,  265,  268,  268,  194,  265,
 /*   650 */   256,  268,  257,  268,  268,  268,  268,  268,  231,  268,
 /*   660 */   231,  268,  268,  268,  238,  268,  268,  268,  268,  268,
 /*   670 */   268,  268,   84,  268,  268,  268,  268,  268,  268,  268,
 /*   680 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   690 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   700 */   194,  194,  194,  194,  194,  194,  268,  194,  194,  194,
 /*   710 */   194,  194,  268,  194,  194,  194,  194,  268,  194,  194,
 /*   720 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   730 */   194,  268,  268,  194,  268,  268,  268,  194,  194,  194,
 /*   740 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   750 */   194,  194,  194,  194,  194,  194,  194,  194,  268,  268,
 /*   760 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   770 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   780 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   790 */   268,  268,  268,  268,  268,  231,  231,  231,  268,  268,
 /*   800 */   268,  268,  268,  268,  268,  268,  268,  240,  240,  268,
 /*   810 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   820 */   268,  268,  268,  268,  268,  268,  257,  257,  257,  257,
 /*   830 */   257,  257,  257,  257,  257,  257,  257,  257,  257,  257,
 /*   840 */   257,  257,  257,  257,  257,
};
#define YY_SHIFT_USE_DFLT (-103)
#define YY_SHIFT_COUNT (345)
#define YY_SHIFT_MIN   (-102)
#define YY_SHIFT_MAX   (588)
static const short yy_shift_ofst[] = {
 /*     0 */   195,  122,  122,  218,  218,  502,  227,  271,  271,  246,
 /*    10 */   237,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*    20 */   237,  237,  237,   -1,   87,  271,  308,  308,  308,  308,
 /*    30 */   333,  333,  237,  237,  237,  176,  237,  237,  268,  502,
 /*    40 */   588,  588, -103, -103, -103,  271,  271,  271,  271,  271,
 /*    50 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*    60 */   271,  271,  271,  271,  271,  308,  308,  308,  118,  118,
 /*    70 */   118,  118,  118,  118,  118,  237,  237,  237,  161,  237,
 /*    80 */   237,  237,  333,  333,  237,  237,  237,  237,  124,  124,
 /*    90 */   -42,  333,  237,  237,  237,  237,  237,  237,  237,  237,
 /*   100 */   237,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*   110 */   237,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*   120 */   237,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*   130 */   237,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*   140 */   237,  237,  237,  237,  492,  492,  492,  505,  505,  505,
 /*   150 */   492,  505,  492,  504,  503,  466,  497,  465,  495,  493,
 /*   160 */   491,  490,  486,  464,  492,  492,  492,  463,  502,  502,
 /*   170 */   492,  492,  488,  487,  518,  489,  485,  512,  484,  482,
 /*   180 */   463,  492,  479,  479,  492,  479,  492,  479,  492,  492,
 /*   190 */  -103, -103,   26,   53,   53,   88,   53,  133,  157,  215,
 /*   200 */   215,  215,  215,  265,  295,  284,  -32,  -32,  -32,  -32,
 /*   210 */   235,  229, -102,    9,   56,   56,  112,   50,   17,  -34,
 /*   220 */   340,  331,  322,  337,  334,  306,   59,  -25,  300,  294,
 /*   230 */   290,  211,  186,  280,  185,   83,   61,  -49,  147,   65,
 /*   240 */   253,  240,  190,  159,  158,  -66,  144,  182,  181,  -59,
 /*   250 */   542,  400,  541,  540,  396,  538,  537,  447,  444,  402,
 /*   260 */   412,  406,  461,  423,  456,  455,  424,  422,  452,  451,
 /*   270 */   449,  419,  448,  446,  445,  523,  443,  441,  440,  409,
 /*   280 */   391,  408,  387,  435,  406,  431,  405,  430,  399,  433,
 /*   290 */   498,  501,  500,  499,  496,  494,  417,  483,  421,  416,
 /*   300 */   414,  382,  411,  478,  432,  477,  347,  346,  373,  373,
 /*   310 */   373,  373,  476,  345,  143,  373,  373,  373,  481,  480,
 /*   320 */   403,  373,  475,  474,  473,  472,  471,  470,  469,  468,
 /*   330 */   467,  462,  460,  459,  458,  457,  453,  404,  297,  274,
 /*   340 */   188,  134,  103,   39,  -38,    3,
};
#define YY_REDUCE_USE_DFLT (-208)
#define YY_REDUCE_COUNT (191)
#define YY_REDUCE_MIN   (-207)
#define YY_REDUCE_MAX   (587)
static const short yy_reduce_ofst[] = {
 /*     0 */  -182,  -16,  -16,   66,   66, -104,  150,   57, -196,  -15,
 /*    10 */   203,  -29,  -31,  202,  198,  196,  192,  191,  183,  175,
 /*    20 */   169,  155, -136,  259,  266,  204,  242,  241,  226,   48,
 /*    30 */   214,   27,  257,  239,  232,  222,  131,  231,  250,  105,
 /*    40 */   244,  225, -207,  199,  201,  587,  586,  585,  584,  583,
 /*    50 */   582,  581,  580,  579,  578,  577,  576,  575,  574,  573,
 /*    60 */   572,  571,  570,  569,  395,  568,  567,  397,  566,  565,
 /*    70 */   564,  429,  427,  398,  388,  563,  562,  561,  394,  560,
 /*    80 */   559,  558,  426,  389,  557,  556,  555,  554,  384,  380,
 /*    90 */   383,  386,  553,  552,  551,  550,  549,  548,  547,  546,
 /*   100 */   545,  544,  543,  539,  536,  535,  534,  533,  532,  531,
 /*   110 */   530,  529,  528,  527,  526,  525,  524,  522,  521,  520,
 /*   120 */   519,  517,  516,  515,  514,  513,  511,  510,  509,  508,
 /*   130 */   507,  506,  454,  450,  442,  439,  438,  437,  436,  434,
 /*   140 */   428,  420,  415,  410,  425,  418,  413,  379,  378,  377,
 /*   150 */   407,  338,  401,  339,  374,  355,  341,  354,  342,  353,
 /*   160 */   351,  344,  350,  343,  393,  392,  390,  372,  381,  376,
 /*   170 */   385,  375,  371,  370,  369,  368,  363,  357,  349,  360,
 /*   180 */   325,  365,  367,  366,  364,  362,  361,  359,  358,  356,
 /*   190 */   348,  352,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   825,  655,  601,  667,  588,  598,  803,  803,  803,  825,
 /*    10 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*    20 */   825,  825,  825,  714,  560,  803,  825,  825,  825,  825,
 /*    30 */   825,  825,  825,  825,  825,  598,  825,  825,  604,  598,
 /*    40 */   604,  604,  709,  639,  657,  825,  825,  825,  825,  825,
 /*    50 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*    60 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*    70 */   825,  825,  825,  825,  825,  825,  825,  825,  716,  722,
 /*    80 */   719,  825,  825,  825,  724,  825,  825,  825,  746,  746,
 /*    90 */   707,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   100 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   110 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   120 */   586,  825,  584,  825,  825,  825,  825,  825,  825,  825,
 /*   130 */   825,  825,  825,  825,  825,  825,  825,  571,  825,  825,
 /*   140 */   825,  825,  825,  825,  562,  562,  562,  825,  825,  825,
 /*   150 */   562,  825,  562,  753,  757,  751,  739,  747,  738,  734,
 /*   160 */   732,  730,  729,  761,  562,  562,  562,  602,  598,  598,
 /*   170 */   562,  562,  620,  618,  616,  608,  614,  610,  612,  606,
 /*   180 */   589,  562,  596,  596,  562,  596,  562,  596,  562,  562,
 /*   190 */   639,  657,  825,  762,  752,  825,  802,  792,  791,  798,
 /*   200 */   790,  789,  788,  825,  825,  825,  784,  787,  786,  785,
 /*   210 */   825,  825,  825,  825,  794,  793,  825,  825,  825,  825,
 /*   220 */   825,  825,  825,  825,  825,  825,  758,  754,  825,  825,
 /*   230 */   825,  825,  825,  825,  825,  825,  825,  764,  825,  825,
 /*   240 */   825,  825,  825,  825,  825,  669,  825,  825,  825,  825,
 /*   250 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   260 */   706,  825,  825,  825,  825,  825,  718,  717,  825,  825,
 /*   270 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  748,
 /*   280 */   825,  740,  825,  825,  681,  825,  825,  825,  825,  825,
 /*   290 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   300 */   825,  825,  825,  825,  825,  825,  825,  825,  821,  816,
 /*   310 */   817,  814,  825,  825,  825,  813,  808,  809,  825,  825,
 /*   320 */   825,  806,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   330 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  623,
 /*   340 */   825,  569,  567,  825,  558,  825,  824,  823,  822,  805,
 /*   350 */   804,  677,  715,  711,  713,  712,  710,  723,  720,  721,
 /*   360 */   705,  704,  703,  725,  708,  728,  727,  731,  733,  736,
 /*   370 */   735,  737,  726,  750,  749,  742,  743,  745,  744,  741,
 /*   380 */   760,  759,  756,  755,  702,  687,  682,  679,  686,  685,
 /*   390 */   684,  683,  680,  676,  675,  603,  656,  654,  653,  652,
 /*   400 */   651,  650,  649,  648,  647,  646,  645,  644,  643,  642,
 /*   410 */   641,  640,  635,  631,  629,  628,  627,  624,  597,  600,
 /*   420 */   599,  800,  801,  799,  797,  796,  795,  781,  780,  779,
 /*   430 */   778,  775,  774,  773,  770,  776,  772,  769,  777,  771,
 /*   440 */   768,  767,  766,  765,  783,  782,  763,  592,  820,  819,
 /*   450 */   818,  815,  812,  811,  810,  807,  689,  690,  671,  674,
 /*   460 */   673,  672,  670,  688,  622,  621,  619,  617,  609,  615,
 /*   470 */   611,  613,  607,  605,  591,  590,  668,  638,  666,  665,
 /*   480 */   664,  663,  662,  661,  660,  659,  658,  637,  636,  634,
 /*   490 */   633,  632,  630,  626,  625,  692,  701,  700,  699,  698,
 /*   500 */   697,  696,  695,  694,  693,  691,  587,  585,  583,  582,
 /*   510 */   581,  580,  579,  578,  577,  576,  575,  574,  595,  573,
 /*   520 */   572,  570,  568,  566,  565,  564,  594,  593,  563,  561,
 /*   530 */   559,  557,  556,  555,  554,  553,  552,  551,  550,  549,
 /*   540 */   548,  547,  546,  545,
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
  "QTIME",         "CONNS",         "STATE",         "KEEP",        
  "CACHE",         "REPLICA",       "QUORUM",        "DAYS",        
  "MINROWS",       "MAXROWS",       "BLOCKS",        "CTIME",       
  "WAL",           "FSYNC",         "COMP",          "PRECISION",   
  "UPDATE",        "CACHELAST",     "PARTITIONS",    "UNSIGNED",    
  "TAGS",          "USING",         "COMMA",         "AS",          
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
  "state",         "keep",          "tagitemlist",   "cache",       
  "replica",       "quorum",        "days",          "minrows",     
  "maxrows",       "blocks",        "ctime",         "wal",         
  "fsync",         "comp",          "prec",          "update",      
  "cachelast",     "partitions",    "typename",      "signed",      
  "create_table_args",  "create_stable_args",  "create_table_list",  "create_from_stable",
  "columnlist",    "tagNamelist",   "select",        "column",      
  "tagitem",       "selcollist",    "from",          "where_opt",   
  "interval_opt",  "session_option",  "windowstate_option",  "fill_opt",    
  "sliding_opt",   "groupby_opt",   "orderby_opt",   "having_opt",  
  "slimit_opt",    "limit_opt",     "union",         "sclp",        
  "distinct",      "expr",          "as",            "tablelist",   
  "sub",           "tmvar",         "sortlist",      "sortitem",    
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
 /* 158 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
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
 /* 174 */ "from ::= FROM sub",
 /* 175 */ "sub ::= LP union RP",
 /* 176 */ "sub ::= LP union RP ids",
 /* 177 */ "sub ::= sub COMMA LP union RP ids",
 /* 178 */ "tablelist ::= ids cpxName",
 /* 179 */ "tablelist ::= ids cpxName ids",
 /* 180 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 181 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 182 */ "tmvar ::= VARIABLE",
 /* 183 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 184 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 185 */ "interval_opt ::=",
 /* 186 */ "session_option ::=",
 /* 187 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 188 */ "windowstate_option ::=",
 /* 189 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 190 */ "fill_opt ::=",
 /* 191 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 192 */ "fill_opt ::= FILL LP ID RP",
 /* 193 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 194 */ "sliding_opt ::=",
 /* 195 */ "orderby_opt ::=",
 /* 196 */ "orderby_opt ::= ORDER BY sortlist",
 /* 197 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 198 */ "sortlist ::= item sortorder",
 /* 199 */ "item ::= ids cpxName",
 /* 200 */ "sortorder ::= ASC",
 /* 201 */ "sortorder ::= DESC",
 /* 202 */ "sortorder ::=",
 /* 203 */ "groupby_opt ::=",
 /* 204 */ "groupby_opt ::= GROUP BY grouplist",
 /* 205 */ "grouplist ::= grouplist COMMA item",
 /* 206 */ "grouplist ::= item",
 /* 207 */ "having_opt ::=",
 /* 208 */ "having_opt ::= HAVING expr",
 /* 209 */ "limit_opt ::=",
 /* 210 */ "limit_opt ::= LIMIT signed",
 /* 211 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 212 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 213 */ "slimit_opt ::=",
 /* 214 */ "slimit_opt ::= SLIMIT signed",
 /* 215 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 216 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 217 */ "where_opt ::=",
 /* 218 */ "where_opt ::= WHERE expr",
 /* 219 */ "expr ::= LP expr RP",
 /* 220 */ "expr ::= ID",
 /* 221 */ "expr ::= ID DOT ID",
 /* 222 */ "expr ::= ID DOT STAR",
 /* 223 */ "expr ::= INTEGER",
 /* 224 */ "expr ::= MINUS INTEGER",
 /* 225 */ "expr ::= PLUS INTEGER",
 /* 226 */ "expr ::= FLOAT",
 /* 227 */ "expr ::= MINUS FLOAT",
 /* 228 */ "expr ::= PLUS FLOAT",
 /* 229 */ "expr ::= STRING",
 /* 230 */ "expr ::= NOW",
 /* 231 */ "expr ::= VARIABLE",
 /* 232 */ "expr ::= PLUS VARIABLE",
 /* 233 */ "expr ::= MINUS VARIABLE",
 /* 234 */ "expr ::= BOOL",
 /* 235 */ "expr ::= NULL",
 /* 236 */ "expr ::= ID LP exprlist RP",
 /* 237 */ "expr ::= ID LP STAR RP",
 /* 238 */ "expr ::= expr IS NULL",
 /* 239 */ "expr ::= expr IS NOT NULL",
 /* 240 */ "expr ::= expr LT expr",
 /* 241 */ "expr ::= expr GT expr",
 /* 242 */ "expr ::= expr LE expr",
 /* 243 */ "expr ::= expr GE expr",
 /* 244 */ "expr ::= expr NE expr",
 /* 245 */ "expr ::= expr EQ expr",
 /* 246 */ "expr ::= expr BETWEEN expr AND expr",
 /* 247 */ "expr ::= expr AND expr",
 /* 248 */ "expr ::= expr OR expr",
 /* 249 */ "expr ::= expr PLUS expr",
 /* 250 */ "expr ::= expr MINUS expr",
 /* 251 */ "expr ::= expr STAR expr",
 /* 252 */ "expr ::= expr SLASH expr",
 /* 253 */ "expr ::= expr REM expr",
 /* 254 */ "expr ::= expr LIKE expr",
 /* 255 */ "expr ::= expr IN LP exprlist RP",
 /* 256 */ "exprlist ::= exprlist COMMA expritem",
 /* 257 */ "exprlist ::= expritem",
 /* 258 */ "expritem ::= expr",
 /* 259 */ "expritem ::=",
 /* 260 */ "cmd ::= RESET QUERY CACHE",
 /* 261 */ "cmd ::= SYNCDB ids REPLICA",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 270 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 271 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 272 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 278 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 279 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 280 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 241: /* selcollist */
    case 255: /* sclp */
{
tSqlExprListDestroy((yypminor->yy325));
}
      break;
    case 213: /* keep */
    case 214: /* tagitemlist */
    case 236: /* columnlist */
    case 237: /* tagNamelist */
    case 247: /* fill_opt */
    case 249: /* groupby_opt */
    case 250: /* orderby_opt */
    case 262: /* sortlist */
    case 266: /* grouplist */
{
taosArrayDestroy((yypminor->yy325));
}
      break;
    case 234: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy422));
}
      break;
    case 238: /* select */
{
destroySqlNode((yypminor->yy536));
}
      break;
    case 242: /* from */
    case 259: /* tablelist */
    case 260: /* sub */
{
destroyRelationInfo((yypminor->yy52));
}
      break;
    case 243: /* where_opt */
    case 251: /* having_opt */
    case 257: /* expr */
    case 267: /* expritem */
{
tSqlExprDestroy((yypminor->yy162));
}
      break;
    case 254: /* union */
{
destroyAllSqlNode((yypminor->yy325));
}
      break;
    case 263: /* sortitem */
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
  { 213, 2 },
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
  { 230, 1 },
  { 230, 4 },
  { 230, 2 },
  { 231, 1 },
  { 231, 2 },
  { 231, 2 },
  { 192, 3 },
  { 192, 3 },
  { 192, 3 },
  { 192, 3 },
  { 234, 1 },
  { 234, 2 },
  { 232, 6 },
  { 233, 10 },
  { 235, 10 },
  { 235, 13 },
  { 237, 3 },
  { 237, 1 },
  { 232, 5 },
  { 236, 3 },
  { 236, 1 },
  { 239, 2 },
  { 214, 3 },
  { 214, 1 },
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
  { 192, 1 },
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
  { 200, 3 },
  { 200, 1 },
  { 267, 1 },
  { 267, 0 },
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy22, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy83);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy83);}
        break;
      case 48: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy325);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy83);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy22, &yymsp[-2].minor.yy0);}
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
    yygotominor.yy83.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy83.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy83.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy83.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy83.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy83.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy83.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy83.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy83.stat    = yymsp[0].minor.yy0;
}
        break;
      case 79: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy325 = yymsp[0].minor.yy325; }
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
{setDefaultCreateDbOption(&yygotominor.yy22); yygotominor.yy22.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 96: /* db_optr ::= db_optr cache */
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 97: /* db_optr ::= db_optr replica */
      case 114: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==114);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 98: /* db_optr ::= db_optr quorum */
      case 115: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==115);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* db_optr ::= db_optr days */
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 100: /* db_optr ::= db_optr minrows */
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 101: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 102: /* db_optr ::= db_optr blocks */
      case 117: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==117);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 103: /* db_optr ::= db_optr ctime */
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 104: /* db_optr ::= db_optr wal */
      case 119: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==119);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 105: /* db_optr ::= db_optr fsync */
      case 120: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==120);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* db_optr ::= db_optr comp */
      case 118: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==118);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 107: /* db_optr ::= db_optr prec */
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.precision = yymsp[0].minor.yy0; }
        break;
      case 108: /* db_optr ::= db_optr keep */
      case 116: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==116);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.keep = yymsp[0].minor.yy325; }
        break;
      case 109: /* db_optr ::= db_optr update */
      case 121: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==121);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 110: /* db_optr ::= db_optr cachelast */
      case 122: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==122);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 111: /* topic_optr ::= db_optr */
      case 123: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==123);
{ yygotominor.yy22 = yymsp[0].minor.yy22; yygotominor.yy22.dbType = TSDB_DB_TYPE_TOPIC; }
        break;
      case 112: /* topic_optr ::= topic_optr partitions */
      case 124: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==124);
{ yygotominor.yy22 = yymsp[-1].minor.yy22; yygotominor.yy22.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 113: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy22); yygotominor.yy22.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 125: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yygotominor.yy47, &yymsp[0].minor.yy0);
}
        break;
      case 126: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy373 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yygotominor.yy47, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy373;  // negative value of name length
    tSetColumnType(&yygotominor.yy47, &yymsp[-3].minor.yy0);
  }
}
        break;
      case 127: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yygotominor.yy47, &yymsp[-1].minor.yy0);
}
        break;
      case 128: /* signed ::= INTEGER */
      case 129: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==129);
{ yygotominor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 130: /* signed ::= MINUS INTEGER */
      case 131: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==133);
{ yygotominor.yy373 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 134: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy422;}
        break;
      case 135: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy504);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yygotominor.yy422 = pCreateTable;
}
        break;
      case 136: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy422->childTableInfo, &yymsp[0].minor.yy504);
  yygotominor.yy422 = yymsp[-1].minor.yy422;
}
        break;
      case 137: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yygotominor.yy422 = tSetCreateTableInfo(yymsp[-1].minor.yy325, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yygotominor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
        break;
      case 138: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yygotominor.yy422 = tSetCreateTableInfo(yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yygotominor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yygotominor.yy504 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy325, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yygotominor.yy504 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
        break;
      case 141: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy325, &yymsp[0].minor.yy0); yygotominor.yy325 = yymsp[-2].minor.yy325;  }
        break;
      case 142: /* tagNamelist ::= ids */
{yygotominor.yy325 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yygotominor.yy325, &yymsp[0].minor.yy0);}
        break;
      case 143: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yygotominor.yy422 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy536, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yygotominor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
        break;
      case 144: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy325, &yymsp[0].minor.yy47); yygotominor.yy325 = yymsp[-2].minor.yy325;  }
        break;
      case 145: /* columnlist ::= column */
{yygotominor.yy325 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yygotominor.yy325, &yymsp[0].minor.yy47);}
        break;
      case 146: /* column ::= ids typename */
{
  tSetColumnInfo(&yygotominor.yy47, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);
}
        break;
      case 147: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy325 = tVariantListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy442, -1);    }
        break;
      case 148: /* tagitemlist ::= tagitem */
{ yygotominor.yy325 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
        break;
      case 149: /* tagitem ::= INTEGER */
      case 150: /* tagitem ::= FLOAT */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= STRING */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= BOOL */ yytestcase(yyruleno==152);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy442, &yymsp[0].minor.yy0); }
        break;
      case 153: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy442, &yymsp[0].minor.yy0); }
        break;
      case 154: /* tagitem ::= MINUS INTEGER */
      case 155: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==157);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy442, &yymsp[-1].minor.yy0);
}
        break;
      case 158: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy536 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy325, yymsp[-11].minor.yy52, yymsp[-10].minor.yy162, yymsp[-4].minor.yy325, yymsp[-3].minor.yy325, &yymsp[-9].minor.yy328, &yymsp[-8].minor.yy84, &yymsp[-7].minor.yy176, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy325, &yymsp[0].minor.yy230, &yymsp[-1].minor.yy230, yymsp[-2].minor.yy162);
}
        break;
      case 159: /* select ::= LP select RP */
{yygotominor.yy536 = yymsp[-1].minor.yy536;}
        break;
      case 160: /* union ::= select */
{ yygotominor.yy325 = setSubclause(NULL, yymsp[0].minor.yy536); }
        break;
      case 161: /* union ::= union UNION ALL select */
{ yygotominor.yy325 = appendSelectClause(yymsp[-3].minor.yy325, yymsp[0].minor.yy536); }
        break;
      case 162: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy325, NULL, TSDB_SQL_SELECT); }
        break;
      case 163: /* select ::= SELECT selcollist */
{
  yygotominor.yy536 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy325, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 164: /* sclp ::= selcollist COMMA */
{yygotominor.yy325 = yymsp[-1].minor.yy325;}
        break;
      case 165: /* sclp ::= */
      case 195: /* orderby_opt ::= */ yytestcase(yyruleno==195);
{yygotominor.yy325 = 0;}
        break;
      case 166: /* selcollist ::= sclp distinct expr as */
{
   yygotominor.yy325 = tSqlExprListAppend(yymsp[-3].minor.yy325, yymsp[-1].minor.yy162,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 167: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yygotominor.yy325 = tSqlExprListAppend(yymsp[-1].minor.yy325, pNode, 0, 0);
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
      case 174: /* from ::= FROM sub */ yytestcase(yyruleno==174);
{yygotominor.yy52 = yymsp[0].minor.yy52;}
        break;
      case 175: /* sub ::= LP union RP */
{yygotominor.yy52 = addSubqueryElem(NULL, yymsp[-1].minor.yy325, NULL);}
        break;
      case 176: /* sub ::= LP union RP ids */
{yygotominor.yy52 = addSubqueryElem(NULL, yymsp[-2].minor.yy325, &yymsp[0].minor.yy0);}
        break;
      case 177: /* sub ::= sub COMMA LP union RP ids */
{yygotominor.yy52 = addSubqueryElem(yymsp[-5].minor.yy52, yymsp[-2].minor.yy325, &yymsp[0].minor.yy0);}
        break;
      case 178: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy52 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 179: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy52 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 180: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy52 = setTableNameList(yymsp[-3].minor.yy52, &yymsp[-1].minor.yy0, NULL);
}
        break;
      case 181: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yygotominor.yy52 = setTableNameList(yymsp[-4].minor.yy52, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 182: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 183: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy328.interval = yymsp[-1].minor.yy0; yygotominor.yy328.offset.n = 0;}
        break;
      case 184: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy328.interval = yymsp[-3].minor.yy0; yygotominor.yy328.offset = yymsp[-1].minor.yy0;}
        break;
      case 185: /* interval_opt ::= */
{memset(&yygotominor.yy328, 0, sizeof(yygotominor.yy328));}
        break;
      case 186: /* session_option ::= */
{yygotominor.yy84.col.n = 0; yygotominor.yy84.gap.n = 0;}
        break;
      case 187: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yygotominor.yy84.col = yymsp[-4].minor.yy0;
   yygotominor.yy84.gap = yymsp[-1].minor.yy0;
}
        break;
      case 188: /* windowstate_option ::= */
{yygotominor.yy176.col.n = 0;}
        break;
      case 189: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yygotominor.yy176.col = yymsp[-1].minor.yy0;
}
        break;
      case 190: /* fill_opt ::= */
{ yygotominor.yy325 = 0;     }
        break;
      case 191: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy325, &A, -1, 0);
    yygotominor.yy325 = yymsp[-1].minor.yy325;
}
        break;
      case 192: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy325 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 193: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 194: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 196: /* orderby_opt ::= ORDER BY sortlist */
{yygotominor.yy325 = yymsp[0].minor.yy325;}
        break;
      case 197: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy325 = tVariantListAppend(yymsp[-3].minor.yy325, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
        break;
      case 198: /* sortlist ::= item sortorder */
{
  yygotominor.yy325 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
        break;
      case 199: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy442, &yymsp[-1].minor.yy0);
}
        break;
      case 200: /* sortorder ::= ASC */
      case 202: /* sortorder ::= */ yytestcase(yyruleno==202);
{ yygotominor.yy196 = TSDB_ORDER_ASC; }
        break;
      case 201: /* sortorder ::= DESC */
{ yygotominor.yy196 = TSDB_ORDER_DESC;}
        break;
      case 203: /* groupby_opt ::= */
{ yygotominor.yy325 = 0;}
        break;
      case 204: /* groupby_opt ::= GROUP BY grouplist */
{ yygotominor.yy325 = yymsp[0].minor.yy325;}
        break;
      case 205: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy325 = tVariantListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy442, -1);
}
        break;
      case 206: /* grouplist ::= item */
{
  yygotominor.yy325 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
        break;
      case 207: /* having_opt ::= */
      case 217: /* where_opt ::= */ yytestcase(yyruleno==217);
      case 259: /* expritem ::= */ yytestcase(yyruleno==259);
{yygotominor.yy162 = 0;}
        break;
      case 208: /* having_opt ::= HAVING expr */
      case 218: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==218);
      case 258: /* expritem ::= expr */ yytestcase(yyruleno==258);
{yygotominor.yy162 = yymsp[0].minor.yy162;}
        break;
      case 209: /* limit_opt ::= */
      case 213: /* slimit_opt ::= */ yytestcase(yyruleno==213);
{yygotominor.yy230.limit = -1; yygotominor.yy230.offset = 0;}
        break;
      case 210: /* limit_opt ::= LIMIT signed */
      case 214: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==214);
{yygotominor.yy230.limit = yymsp[0].minor.yy373;  yygotominor.yy230.offset = 0;}
        break;
      case 211: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yygotominor.yy230.limit = yymsp[-2].minor.yy373;  yygotominor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 212: /* limit_opt ::= LIMIT signed COMMA signed */
{ yygotominor.yy230.limit = yymsp[0].minor.yy373;  yygotominor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 215: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yygotominor.yy230.limit = yymsp[-2].minor.yy373;  yygotominor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 216: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yygotominor.yy230.limit = yymsp[0].minor.yy373;  yygotominor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 219: /* expr ::= LP expr RP */
{yygotominor.yy162 = yymsp[-1].minor.yy162; yygotominor.yy162->token.z = yymsp[-2].minor.yy0.z; yygotominor.yy162->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
        break;
      case 220: /* expr ::= ID */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 221: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 222: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 223: /* expr ::= INTEGER */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 224: /* expr ::= MINUS INTEGER */
      case 225: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==225);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 226: /* expr ::= FLOAT */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 227: /* expr ::= MINUS FLOAT */
      case 228: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 229: /* expr ::= STRING */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 230: /* expr ::= NOW */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 231: /* expr ::= VARIABLE */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 232: /* expr ::= PLUS VARIABLE */
      case 233: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
        break;
      case 234: /* expr ::= BOOL */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 235: /* expr ::= NULL */
{ yygotominor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
        break;
      case 236: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy162 = tSqlExprCreateFunction(yymsp[-1].minor.yy325, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 237: /* expr ::= ID LP STAR RP */
{ yygotominor.yy162 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 238: /* expr ::= expr IS NULL */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, NULL, TK_ISNULL);}
        break;
      case 239: /* expr ::= expr IS NOT NULL */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-3].minor.yy162, NULL, TK_NOTNULL);}
        break;
      case 240: /* expr ::= expr LT expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LT);}
        break;
      case 241: /* expr ::= expr GT expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_GT);}
        break;
      case 242: /* expr ::= expr LE expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LE);}
        break;
      case 243: /* expr ::= expr GE expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_GE);}
        break;
      case 244: /* expr ::= expr NE expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_NE);}
        break;
      case 245: /* expr ::= expr EQ expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_EQ);}
        break;
      case 246: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy162); yygotominor.yy162 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy162, yymsp[-2].minor.yy162, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy162, TK_LE), TK_AND);}
        break;
      case 247: /* expr ::= expr AND expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_AND);}
        break;
      case 248: /* expr ::= expr OR expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_OR); }
        break;
      case 249: /* expr ::= expr PLUS expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_PLUS);  }
        break;
      case 250: /* expr ::= expr MINUS expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_MINUS); }
        break;
      case 251: /* expr ::= expr STAR expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_STAR);  }
        break;
      case 252: /* expr ::= expr SLASH expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_DIVIDE);}
        break;
      case 253: /* expr ::= expr REM expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_REM);   }
        break;
      case 254: /* expr ::= expr LIKE expr */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LIKE);  }
        break;
      case 255: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy162 = tSqlExprCreate(yymsp[-4].minor.yy162, (tSqlExpr*)yymsp[-1].minor.yy325, TK_IN); }
        break;
      case 256: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy325 = tSqlExprListAppend(yymsp[-2].minor.yy325,yymsp[0].minor.yy162,0, 0);}
        break;
      case 257: /* exprlist ::= expritem */
{yygotominor.yy325 = tSqlExprListAppend(0,yymsp[0].minor.yy162,0, 0);}
        break;
      case 260: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 261: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 268: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 276: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 279: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 280: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
