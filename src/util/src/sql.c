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

#include "tsql.h"
#include "tutil.h"
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
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_MIN_REDUCE      Maximum value for reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/************* Begin control #defines *****************************************/
#define YYCODETYPE unsigned short int
#define YYNOCODE 261
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SQuerySQL* yy24;
  tVariantList* yy56;
  tSQLExprListList* yy74;
  tSQLExpr* yy90;
  SCreateTableSQL* yy158;
  tVariant yy186;
  TAOS_FIELD yy223;
  SCreateAcctSQL yy279;
  SLimitVal yy294;
  int yy332;
  int64_t yy389;
  SCreateDBInfo yy398;
  tFieldList* yy471;
  tSQLExprList* yy498;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             251
#define YYNRULE              213
#define YY_MAX_SHIFT         250
#define YY_MIN_SHIFTREDUCE   401
#define YY_MAX_SHIFTREDUCE   613
#define YY_MIN_REDUCE        614
#define YY_MAX_REDUCE        826
#define YY_ERROR_ACTION      827
#define YY_ACCEPT_ACTION     828
#define YY_NO_ACTION         829
/************* End control #defines *******************************************/

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
**   0 <= N <= YY_MAX_SHIFT             Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   N between YY_MIN_SHIFTREDUCE       Shift to an arbitrary state then
**     and YY_MAX_SHIFTREDUCE           reduce by rule N-YY_MIN_SHIFTREDUCE.
**
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE

**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
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
**
*********** Begin parsing tables **********************************************/
#define YY_ACTTAB_COUNT (529)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   439,   36,   35,  153,  249,   34,   33,   32,  440,   34,
 /*    10 */    33,   32,   43,   45,   49,   37,   38,   74,   78,  244,
 /*    20 */    31,   85,   77,  205,   41,   39,   42,   40,   80,  133,
 /*    30 */   101,   50,   36,   35,  527,  171,   34,   33,   32,   43,
 /*    40 */    45,  154,   37,   38,  114,  115,  224,   31,   65,   68,
 /*    50 */   205,   41,   39,   42,   40,   76,  133,  828,  250,   36,
 /*    60 */    35,  241,  241,   34,   33,   32,   43,   45,  155,   37,
 /*    70 */    38,  128,  126,  245,   31,   89,   88,  205,   41,   39,
 /*    80 */    42,   40,  202,  524,   59,  135,   36,   35,  439,   21,
 /*    90 */    34,   33,   32,  520,  159,  596,  440,   10,   57,  172,
 /*   100 */   135,  135,  227,  226,  101,   45,  439,   37,   38,  158,
 /*   110 */   596,  595,   31,  156,  440,  205,   41,   39,   42,   40,
 /*   120 */   232,  167,  564,  507,   36,   35,  166,   21,   34,   33,
 /*   130 */    32,  510,  402,  403,  404,  405,  406,  407,  408,  409,
 /*   140 */   410,  411,  412,  413,  510,   37,   38,  243,  132,  508,
 /*   150 */    31,  220,  101,  205,   41,   39,   42,   40,  551,  168,
 /*   160 */   200,  507,   36,   35,   97,  134,   34,   33,   32,  510,
 /*   170 */    21,  139,  101,   17,  219,  242,  218,  217,  216,  215,
 /*   180 */   214,  213,  212,  211,  492,   21,  481,  482,  483,  484,
 /*   190 */   485,  486,  487,  488,  489,  490,  491,  163,  577,   11,
 /*   200 */   243,  568,  228,  571,  507,  574,  550,  163,  577,  498,
 /*   210 */    21,  568,  509,  571,  193,  574,  148,  233,    7,  507,
 /*   220 */   561,   62,  111,   87,   86,  142,   60,  178,  242,  160,
 /*   230 */   161,  147,  437,  204,  186,  124,  183,  230,  229,  160,
 /*   240 */   161,  163,  577,  525,  506,  568,  570,  571,  573,  574,
 /*   250 */    41,   39,   42,   40,  495,   61,  494,   27,   36,   35,
 /*   260 */   545,  546,   34,   33,   32,  514,   28,  600,  511,  162,
 /*   270 */   512,   29,  513,  160,  161,  192,  446,  566,  188,  124,
 /*   280 */   248,  247,  422,  438,  522,  150,  124,   18,  601,  536,
 /*   290 */   537,   44,   29,   47,   15,  594,  169,  170,  578,   14,
 /*   300 */   576,   44,   14,  569,  518,  572,  519,    2,   52,  516,
 /*   310 */   576,  517,  504,  567,  503,  575,   47,  592,   22,  591,
 /*   320 */   209,   73,   72,   53,   22,  575,    9,    8,   84,   83,
 /*   330 */   590,  151,  152,  140,  501,   44,  610,  141,  560,  143,
 /*   340 */   144,  145,  146,  137,  576,  131,  138,  136,  164,  557,
 /*   350 */   556,  165,  526,  231,  110,   98,  112,  113,  448,  575,
 /*   360 */   543,  542,  210,  129,  515,   25,  223,  225,  609,   70,
 /*   370 */   189,  608,  606,  116,  466,   26,   23,  130,  435,   79,
 /*   380 */   433,   81,  431,  191,  430,  173,  125,  428,  427,  426,
 /*   390 */   424,   91,  532,  194,  198,   54,  417,  127,   51,  521,
 /*   400 */   421,  203,  419,   46,  102,   95,  201,  530,  103,  531,
 /*   410 */   544,  195,  199,  197,   30,   27,  222,  235,   75,  234,
 /*   420 */   236,  207,  238,   55,  237,  239,  240,  246,  149,  613,
 /*   430 */    63,   66,  174,  429,  175,  176,   90,   92,  177,  423,
 /*   440 */   119,  612,  118,  467,  117,  120,  121,  179,  122,  123,
 /*   450 */     1,  505,  108,  104,  105,  106,  107,  109,   24,  180,
 /*   460 */   181,  182,  611,  184,  185,  604,   12,   13,  187,  190,
 /*   470 */    96,  533,   99,  157,   58,  538,  196,  100,   19,    4,
 /*   480 */   579,    3,   16,   20,   64,    5,  206,    6,  208,  479,
 /*   490 */   478,  477,  476,  475,  474,  473,  472,  470,   47,  221,
 /*   500 */   443,   67,  445,   22,  500,   48,  499,  497,  464,   56,
 /*   510 */   462,  454,   69,  460,  456,   71,  458,  452,  450,  471,
 /*   520 */   469,   82,  441,  425,  415,   93,  614,  616,   94,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,  198,  199,   37,   38,   39,    9,   37,
 /*    10 */    38,   39,   13,   14,  100,   16,   17,   62,   63,   64,
 /*    20 */    21,   66,   67,   24,   25,   26,   27,   28,   73,  247,
 /*    30 */   199,  117,   33,   34,  199,   61,   37,   38,   39,   13,
 /*    40 */    14,  259,   16,   17,   62,   63,   64,   21,   66,   67,
 /*    50 */    24,   25,   26,   27,   28,   71,  247,  196,  197,   33,
 /*    60 */    34,   77,   77,   37,   38,   39,   13,   14,  259,   16,
 /*    70 */    17,   62,   63,   64,   21,   66,   67,   24,   25,   26,
 /*    80 */    27,   28,  251,  248,  253,  247,   33,   34,    1,  199,
 /*    90 */    37,   38,   39,  232,  256,  257,    9,  247,   99,  125,
 /*   100 */   247,  247,  128,  129,  199,   14,    1,   16,   17,  256,
 /*   110 */   257,  257,   21,  216,    9,   24,   25,   26,   27,   28,
 /*   120 */   199,  231,   96,  233,   33,   34,  216,  199,   37,   38,
 /*   130 */    39,  234,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,   56,  234,   16,   17,   58,  247,  228,
 /*   150 */    21,  216,  199,   24,   25,   26,   27,   28,  253,  231,
 /*   160 */   255,  233,   33,   34,  199,  247,   37,   38,   39,  234,
 /*   170 */   199,  247,  199,   84,   85,   86,   87,   88,   89,   90,
 /*   180 */    91,   92,   93,   94,  215,  199,  217,  218,  219,  220,
 /*   190 */   221,  222,  223,  224,  225,  226,  227,    1,    2,   44,
 /*   200 */    58,    5,  231,    7,  233,    9,  253,    1,    2,    5,
 /*   210 */   199,    5,  234,    7,  249,    9,   61,  231,   95,  233,
 /*   220 */   229,   98,   99,   68,   69,   70,  253,  124,   86,   33,
 /*   230 */    34,   76,  203,   37,  131,  206,  133,   33,   34,   33,
 /*   240 */    34,    1,    2,   37,  233,    5,    5,    7,    7,    9,
 /*   250 */    25,   26,   27,   28,  217,  235,  219,  102,   33,   34,
 /*   260 */   110,  111,   37,   38,   39,    2,  246,   96,    5,   57,
 /*   270 */     7,  100,    9,   33,   34,  120,  203,    1,  123,  206,
 /*   280 */    58,   59,   60,  203,  100,  130,  206,  103,   96,   96,
 /*   290 */    96,   95,  100,  100,  100,   96,   33,   34,   96,  100,
 /*   300 */   104,   95,  100,    5,    5,    7,    7,   95,  100,    5,
 /*   310 */   104,    7,   96,   37,   96,  119,  100,  247,  100,  247,
 /*   320 */    96,  126,  127,  115,  100,  119,  126,  127,   71,   72,
 /*   330 */   247,  247,  247,  247,  230,   95,  234,  247,  229,  247,
 /*   340 */   247,  247,  247,  247,  104,  247,  247,  247,  229,  229,
 /*   350 */   229,  229,  199,  229,  236,  199,  199,  199,  199,  119,
 /*   360 */   254,  254,  199,  199,  101,  199,  199,  199,  199,  199,
 /*   370 */   122,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   380 */   199,  199,  199,  258,  199,  199,  199,  199,  199,  199,
 /*   390 */   199,   57,  104,  250,  250,  114,  199,  199,  116,  245,
 /*   400 */   199,  108,  199,  113,  244,  200,  112,  200,  243,  200,
 /*   410 */   200,  105,  107,  106,  118,  102,   74,   49,   83,   82,
 /*   420 */    79,  200,   53,  200,   81,   80,   78,   74,  200,    5,
 /*   430 */   204,  204,  132,  200,    5,  132,  201,  201,   65,  200,
 /*   440 */   208,    5,  212,  214,  213,  211,  209,  132,  210,  207,
 /*   450 */   205,  232,  238,  242,  241,  240,  239,  237,  202,    5,
 /*   460 */   132,   65,    5,  132,   65,   85,   95,   95,  124,  122,
 /*   470 */   121,   96,   95,    1,  100,   96,   95,   95,  100,  109,
 /*   480 */    96,   95,   95,  100,   71,  109,   97,   95,   97,    9,
 /*   490 */     5,    5,    5,    5,    1,    5,    5,    5,  100,   15,
 /*   500 */    75,   71,   65,  100,    5,   16,    5,   96,    5,   95,
 /*   510 */     5,    5,  127,    5,    5,  127,    5,    5,    5,    5,
 /*   520 */     5,   65,   75,   65,   57,   21,    0,  260,   21,
};
#define YY_SHIFT_USE_DFLT (-87)
#define YY_SHIFT_COUNT (250)
#define YY_SHIFT_MIN   (-86)
#define YY_SHIFT_MAX   (526)
static const short yy_shift_ofst[] = {
 /*     0 */   155,   89,  196,  240,  105,  105,  105,  105,  105,  105,
 /*    10 */    -1,   87,  240,  240,  240,  263,  263,  263,  105,  105,
 /*    20 */   105,  105,  105,  -16,  142,  -15,  -15,  -87,  206,  240,
 /*    30 */   240,  240,  240,  240,  240,  240,  240,  240,  240,  240,
 /*    40 */   240,  240,  240,  240,  240,  240,  240,  263,  263,  204,
 /*    50 */   204,  204,  204,  204,  204,  123,  204,  105,  105,  150,
 /*    60 */   150,  184,  105,  105,  105,  105,  105,  105,  105,  105,
 /*    70 */   105,  105,  105,  105,  105,  105,  105,  105,  105,  105,
 /*    80 */   105,  105,  105,  105,  105,  105,  105,  105,  105,  105,
 /*    90 */   105,  105,  105,  105,  105,  248,  334,  334,  334,  288,
 /*   100 */   288,  334,  281,  282,  290,  293,  294,  305,  307,  306,
 /*   110 */   296,  313,  334,  334,  342,  342,  334,  335,  337,  368,
 /*   120 */   341,  343,  369,  345,  348,  334,  353,  334,  353,  -87,
 /*   130 */   -87,   26,   53,   53,   53,   53,   53,   91,  129,  225,
 /*   140 */   225,  225,  -45,  -32,  -32,  -32,  -32,  -18,    9,  -26,
 /*   150 */   103,  -28,  -28,  222,  171,  192,  193,  194,  199,  202,
 /*   160 */   241,  298,  276,  212,  -86,  208,  216,  218,  224,  299,
 /*   170 */   304,  195,  200,  257,  424,  300,  429,  303,  373,  436,
 /*   180 */   315,  454,  328,  396,  457,  331,  399,  380,  344,  371,
 /*   190 */   372,  347,  349,  374,  375,  377,  472,  381,  379,  382,
 /*   200 */   378,  370,  383,  376,  384,  386,  387,  389,  392,  391,
 /*   210 */   413,  480,  485,  486,  487,  488,  493,  490,  491,  492,
 /*   220 */   398,  425,  484,  430,  437,  489,  385,  388,  403,  499,
 /*   230 */   501,  411,  414,  403,  503,  505,  506,  508,  509,  511,
 /*   240 */   512,  513,  514,  515,  456,  458,  447,  504,  507,  467,
 /*   250 */   526,
};
#define YY_REDUCE_USE_DFLT (-219)
#define YY_REDUCE_COUNT (130)
#define YY_REDUCE_MIN   (-218)
#define YY_REDUCE_MAX   (256)
static const short yy_reduce_ofst[] = {
 /*     0 */  -139,  -31, -162, -147,  -95, -169, -110,  -72,  -29,  -14,
 /*    10 */  -165, -195, -218, -191, -146, -103,  -90,  -65,  -35,  -47,
 /*    20 */   -27,  -79,   11,   29,   37,   73,   80,   20, -150,  -99,
 /*    30 */   -82,  -76,   70,   72,   83,   84,   85,   86,   90,   92,
 /*    40 */    93,   94,   95,   96,   98,   99,  100,  -22,  102,   -9,
 /*    50 */   109,  119,  120,  121,  122,  104,  124,  153,  156,  106,
 /*    60 */   107,  118,  157,  158,  159,  163,  164,  166,  167,  168,
 /*    70 */   169,  170,  172,  173,  174,  175,  176,  177,  178,  179,
 /*    80 */   180,  181,  182,  183,  185,  186,  187,  188,  189,  190,
 /*    90 */   191,  197,  198,  201,  203,  125,  205,  207,  209,  143,
 /*   100 */   144,  210,  154,  160,  165,  211,  213,  215,  217,  214,
 /*   110 */   220,  219,  221,  223,  226,  227,  228,  229,  231,  230,
 /*   120 */   232,  234,  237,  238,  242,  233,  235,  239,  236,  245,
 /*   130 */   256,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   827,  660,  811,  811,  827,  827,  827,  827,  827,  827,
 /*    10 */   741,  627,  827,  827,  811,  827,  827,  827,  827,  827,
 /*    20 */   827,  827,  827,  662,  649,  662,  662,  736,  827,  827,
 /*    30 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    40 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    50 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  760,
 /*    60 */   760,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    70 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  647,
 /*    80 */   827,  645,  827,  827,  827,  827,  827,  827,  827,  827,
 /*    90 */   827,  827,  827,  827,  827,  827,  629,  629,  629,  827,
 /*   100 */   827,  629,  767,  771,  765,  753,  761,  752,  748,  747,
 /*   110 */   775,  827,  629,  629,  657,  657,  629,  678,  676,  674,
 /*   120 */   666,  672,  668,  670,  664,  629,  655,  629,  655,  693,
 /*   130 */   706,  827,  815,  816,  776,  810,  766,  794,  793,  806,
 /*   140 */   800,  799,  827,  798,  797,  796,  795,  827,  827,  827,
 /*   150 */   827,  802,  801,  827,  827,  827,  827,  827,  827,  827,
 /*   160 */   827,  827,  827,  778,  772,  768,  827,  827,  827,  827,
 /*   170 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   180 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   190 */   827,  812,  827,  742,  827,  827,  827,  827,  827,  827,
 /*   200 */   762,  827,  754,  827,  827,  827,  827,  827,  827,  715,
 /*   210 */   827,  827,  827,  827,  827,  827,  827,  827,  827,  827,
 /*   220 */   681,  827,  827,  827,  827,  827,  827,  827,  820,  827,
 /*   230 */   827,  827,  709,  818,  827,  827,  827,  827,  827,  827,
 /*   240 */   827,  827,  827,  827,  827,  827,  827,  633,  631,  827,
 /*   250 */   827,
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
    0,  /*        DOT => nothing */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
    0,  /*      DNODE => nothing */
    1,  /*         IP => ID */
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
    0,  /*       DAYS => nothing */
    0,  /*       ROWS => nothing */
    0,  /*    ABLOCKS => nothing */
    0,  /*    TBLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*       CLOG => nothing */
    0,  /*       COMP => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*         AS => nothing */
    0,  /*      COMMA => nothing */
    0,  /*       NULL => nothing */
    0,  /*     SELECT => nothing */
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
    0,  /*     INSERT => nothing */
    0,  /*       INTO => nothing */
    0,  /*     VALUES => nothing */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*        TAG => nothing */
    0,  /*     CHANGE => nothing */
    0,  /*        SET => nothing */
    0,  /*       KILL => nothing */
    0,  /* CONNECTION => nothing */
    0,  /*      COLON => nothing */
    0,  /*     STREAM => nothing */
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
    1,  /*        ALL => ID */
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
  "CONNECTIONS",   "STREAMS",       "CONFIGS",       "SCORES",      
  "GRANTS",        "DOT",           "TABLES",        "STABLES",     
  "VGROUPS",       "DROP",          "TABLE",         "DATABASE",    
  "DNODE",         "IP",            "USER",          "ACCOUNT",     
  "USE",           "DESCRIBE",      "ALTER",         "PASS",        
  "PRIVILEGE",     "LOCAL",         "IF",            "EXISTS",      
  "CREATE",        "PPS",           "TSERIES",       "DBS",         
  "STORAGE",       "QTIME",         "CONNS",         "STATE",       
  "KEEP",          "CACHE",         "REPLICA",       "DAYS",        
  "ROWS",          "ABLOCKS",       "TBLOCKS",       "CTIME",       
  "CLOG",          "COMP",          "PRECISION",     "LP",          
  "RP",            "TAGS",          "USING",         "AS",          
  "COMMA",         "NULL",          "SELECT",        "FROM",        
  "VARIABLE",      "INTERVAL",      "FILL",          "SLIDING",     
  "ORDER",         "BY",            "ASC",           "DESC",        
  "GROUP",         "HAVING",        "LIMIT",         "OFFSET",      
  "SLIMIT",        "SOFFSET",       "WHERE",         "NOW",         
  "INSERT",        "INTO",          "VALUES",        "RESET",       
  "QUERY",         "ADD",           "COLUMN",        "TAG",         
  "CHANGE",        "SET",           "KILL",          "CONNECTION",  
  "COLON",         "STREAM",        "ABORT",         "AFTER",       
  "ATTACH",        "BEFORE",        "BEGIN",         "CASCADE",     
  "CLUSTER",       "CONFLICT",      "COPY",          "DEFERRED",    
  "DELIMITERS",    "DETACH",        "EACH",          "END",         
  "EXPLAIN",       "FAIL",          "FOR",           "IGNORE",      
  "IMMEDIATE",     "INITIALLY",     "INSTEAD",       "MATCH",       
  "KEY",           "OF",            "RAISE",         "REPLACE",     
  "RESTRICT",      "ROW",           "STATEMENT",     "TRIGGER",     
  "VIEW",          "ALL",           "COUNT",         "SUM",         
  "AVG",           "MIN",           "MAX",           "FIRST",       
  "LAST",          "TOP",           "BOTTOM",        "STDDEV",      
  "PERCENTILE",    "APERCENTILE",   "LEASTSQUARES",  "HISTOGRAM",   
  "DIFF",          "SPREAD",        "TWA",           "INTERP",      
  "LAST_ROW",      "SEMI",          "NONE",          "PREV",        
  "LINEAR",        "IMPORT",        "METRIC",        "TBNAME",      
  "JOIN",          "METRICS",       "STABLE",        "error",       
  "program",       "cmd",           "dbPrefix",      "ids",         
  "cpxName",       "ifexists",      "alter_db_optr",  "acct_optr",   
  "ifnotexists",   "db_optr",       "pps",           "tseries",     
  "dbs",           "streams",       "storage",       "qtime",       
  "users",         "conns",         "state",         "keep",        
  "tagitemlist",   "tables",        "cache",         "replica",     
  "days",          "rows",          "ablocks",       "tblocks",     
  "ctime",         "clog",          "comp",          "prec",        
  "typename",      "signed",        "create_table_args",  "columnlist",  
  "select",        "column",        "tagitem",       "selcollist",  
  "from",          "where_opt",     "interval_opt",  "fill_opt",    
  "sliding_opt",   "groupby_opt",   "orderby_opt",   "having_opt",  
  "slimit_opt",    "limit_opt",     "sclp",          "expr",        
  "as",            "tablelist",     "tmvar",         "sortlist",    
  "sortitem",      "item",          "sortorder",     "grouplist",   
  "exprlist",      "expritem",      "insert_value_list",  "itemlist",    
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
 /*  10 */ "cmd ::= SHOW CONFIGS",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "dbPrefix ::=",
 /*  14 */ "dbPrefix ::= ids DOT",
 /*  15 */ "cpxName ::=",
 /*  16 */ "cpxName ::= DOT ids",
 /*  17 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  18 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  19 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  20 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  22 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  23 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  24 */ "cmd ::= DROP DNODE IP",
 /*  25 */ "cmd ::= DROP USER ids",
 /*  26 */ "cmd ::= DROP ACCOUNT ids",
 /*  27 */ "cmd ::= USE ids",
 /*  28 */ "cmd ::= DESCRIBE ids cpxName",
 /*  29 */ "cmd ::= ALTER USER ids PASS ids",
 /*  30 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  31 */ "cmd ::= ALTER DNODE IP ids",
 /*  32 */ "cmd ::= ALTER DNODE IP ids ids",
 /*  33 */ "cmd ::= ALTER LOCAL ids",
 /*  34 */ "cmd ::= ALTER LOCAL ids ids",
 /*  35 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  36 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  37 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  38 */ "ids ::= ID",
 /*  39 */ "ids ::= STRING",
 /*  40 */ "ifexists ::= IF EXISTS",
 /*  41 */ "ifexists ::=",
 /*  42 */ "ifnotexists ::= IF NOT EXISTS",
 /*  43 */ "ifnotexists ::=",
 /*  44 */ "cmd ::= CREATE DNODE IP",
 /*  45 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  46 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  47 */ "cmd ::= CREATE USER ids PASS ids",
 /*  48 */ "pps ::=",
 /*  49 */ "pps ::= PPS INTEGER",
 /*  50 */ "tseries ::=",
 /*  51 */ "tseries ::= TSERIES INTEGER",
 /*  52 */ "dbs ::=",
 /*  53 */ "dbs ::= DBS INTEGER",
 /*  54 */ "streams ::=",
 /*  55 */ "streams ::= STREAMS INTEGER",
 /*  56 */ "storage ::=",
 /*  57 */ "storage ::= STORAGE INTEGER",
 /*  58 */ "qtime ::=",
 /*  59 */ "qtime ::= QTIME INTEGER",
 /*  60 */ "users ::=",
 /*  61 */ "users ::= USERS INTEGER",
 /*  62 */ "conns ::=",
 /*  63 */ "conns ::= CONNS INTEGER",
 /*  64 */ "state ::=",
 /*  65 */ "state ::= STATE ids",
 /*  66 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  67 */ "keep ::= KEEP tagitemlist",
 /*  68 */ "tables ::= TABLES INTEGER",
 /*  69 */ "cache ::= CACHE INTEGER",
 /*  70 */ "replica ::= REPLICA INTEGER",
 /*  71 */ "days ::= DAYS INTEGER",
 /*  72 */ "rows ::= ROWS INTEGER",
 /*  73 */ "ablocks ::= ABLOCKS ID",
 /*  74 */ "tblocks ::= TBLOCKS INTEGER",
 /*  75 */ "ctime ::= CTIME INTEGER",
 /*  76 */ "clog ::= CLOG INTEGER",
 /*  77 */ "comp ::= COMP INTEGER",
 /*  78 */ "prec ::= PRECISION STRING",
 /*  79 */ "db_optr ::=",
 /*  80 */ "db_optr ::= db_optr tables",
 /*  81 */ "db_optr ::= db_optr cache",
 /*  82 */ "db_optr ::= db_optr replica",
 /*  83 */ "db_optr ::= db_optr days",
 /*  84 */ "db_optr ::= db_optr rows",
 /*  85 */ "db_optr ::= db_optr ablocks",
 /*  86 */ "db_optr ::= db_optr tblocks",
 /*  87 */ "db_optr ::= db_optr ctime",
 /*  88 */ "db_optr ::= db_optr clog",
 /*  89 */ "db_optr ::= db_optr comp",
 /*  90 */ "db_optr ::= db_optr prec",
 /*  91 */ "db_optr ::= db_optr keep",
 /*  92 */ "alter_db_optr ::=",
 /*  93 */ "alter_db_optr ::= alter_db_optr replica",
 /*  94 */ "alter_db_optr ::= alter_db_optr tables",
 /*  95 */ "typename ::= ids",
 /*  96 */ "typename ::= ids LP signed RP",
 /*  97 */ "signed ::= INTEGER",
 /*  98 */ "signed ::= PLUS INTEGER",
 /*  99 */ "signed ::= MINUS INTEGER",
 /* 100 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 101 */ "create_table_args ::= LP columnlist RP",
 /* 102 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 103 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 104 */ "create_table_args ::= AS select",
 /* 105 */ "columnlist ::= columnlist COMMA column",
 /* 106 */ "columnlist ::= column",
 /* 107 */ "column ::= ids typename",
 /* 108 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 109 */ "tagitemlist ::= tagitem",
 /* 110 */ "tagitem ::= INTEGER",
 /* 111 */ "tagitem ::= FLOAT",
 /* 112 */ "tagitem ::= STRING",
 /* 113 */ "tagitem ::= BOOL",
 /* 114 */ "tagitem ::= NULL",
 /* 115 */ "tagitem ::= MINUS INTEGER",
 /* 116 */ "tagitem ::= MINUS FLOAT",
 /* 117 */ "tagitem ::= PLUS INTEGER",
 /* 118 */ "tagitem ::= PLUS FLOAT",
 /* 119 */ "cmd ::= select",
 /* 120 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 121 */ "sclp ::= selcollist COMMA",
 /* 122 */ "sclp ::=",
 /* 123 */ "selcollist ::= sclp expr as",
 /* 124 */ "selcollist ::= sclp STAR",
 /* 125 */ "as ::= AS ids",
 /* 126 */ "as ::= ids",
 /* 127 */ "as ::=",
 /* 128 */ "from ::= FROM tablelist",
 /* 129 */ "tablelist ::= ids cpxName",
 /* 130 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 131 */ "tmvar ::= VARIABLE",
 /* 132 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 133 */ "interval_opt ::=",
 /* 134 */ "fill_opt ::=",
 /* 135 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 136 */ "fill_opt ::= FILL LP ID RP",
 /* 137 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 138 */ "sliding_opt ::=",
 /* 139 */ "orderby_opt ::=",
 /* 140 */ "orderby_opt ::= ORDER BY sortlist",
 /* 141 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 142 */ "sortlist ::= item sortorder",
 /* 143 */ "item ::= ids cpxName",
 /* 144 */ "sortorder ::= ASC",
 /* 145 */ "sortorder ::= DESC",
 /* 146 */ "sortorder ::=",
 /* 147 */ "groupby_opt ::=",
 /* 148 */ "groupby_opt ::= GROUP BY grouplist",
 /* 149 */ "grouplist ::= grouplist COMMA item",
 /* 150 */ "grouplist ::= item",
 /* 151 */ "having_opt ::=",
 /* 152 */ "having_opt ::= HAVING expr",
 /* 153 */ "limit_opt ::=",
 /* 154 */ "limit_opt ::= LIMIT signed",
 /* 155 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 156 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 157 */ "slimit_opt ::=",
 /* 158 */ "slimit_opt ::= SLIMIT signed",
 /* 159 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 160 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 161 */ "where_opt ::=",
 /* 162 */ "where_opt ::= WHERE expr",
 /* 163 */ "expr ::= LP expr RP",
 /* 164 */ "expr ::= ID",
 /* 165 */ "expr ::= ID DOT ID",
 /* 166 */ "expr ::= ID DOT STAR",
 /* 167 */ "expr ::= INTEGER",
 /* 168 */ "expr ::= MINUS INTEGER",
 /* 169 */ "expr ::= PLUS INTEGER",
 /* 170 */ "expr ::= FLOAT",
 /* 171 */ "expr ::= MINUS FLOAT",
 /* 172 */ "expr ::= PLUS FLOAT",
 /* 173 */ "expr ::= STRING",
 /* 174 */ "expr ::= NOW",
 /* 175 */ "expr ::= VARIABLE",
 /* 176 */ "expr ::= BOOL",
 /* 177 */ "expr ::= ID LP exprlist RP",
 /* 178 */ "expr ::= ID LP STAR RP",
 /* 179 */ "expr ::= expr AND expr",
 /* 180 */ "expr ::= expr OR expr",
 /* 181 */ "expr ::= expr LT expr",
 /* 182 */ "expr ::= expr GT expr",
 /* 183 */ "expr ::= expr LE expr",
 /* 184 */ "expr ::= expr GE expr",
 /* 185 */ "expr ::= expr NE expr",
 /* 186 */ "expr ::= expr EQ expr",
 /* 187 */ "expr ::= expr PLUS expr",
 /* 188 */ "expr ::= expr MINUS expr",
 /* 189 */ "expr ::= expr STAR expr",
 /* 190 */ "expr ::= expr SLASH expr",
 /* 191 */ "expr ::= expr REM expr",
 /* 192 */ "expr ::= expr LIKE expr",
 /* 193 */ "expr ::= expr IN LP exprlist RP",
 /* 194 */ "exprlist ::= exprlist COMMA expritem",
 /* 195 */ "exprlist ::= expritem",
 /* 196 */ "expritem ::= expr",
 /* 197 */ "expritem ::=",
 /* 198 */ "cmd ::= INSERT INTO cpxName insert_value_list",
 /* 199 */ "insert_value_list ::= VALUES LP itemlist RP",
 /* 200 */ "insert_value_list ::= insert_value_list VALUES LP itemlist RP",
 /* 201 */ "itemlist ::= itemlist COMMA expr",
 /* 202 */ "itemlist ::= expr",
 /* 203 */ "cmd ::= RESET QUERY CACHE",
 /* 204 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 205 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 206 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 207 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 208 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 209 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 210 */ "cmd ::= KILL CONNECTION IP COLON INTEGER",
 /* 211 */ "cmd ::= KILL STREAM IP COLON INTEGER COLON INTEGER",
 /* 212 */ "cmd ::= KILL QUERY IP COLON INTEGER COLON INTEGER",
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

/* Datatype of the argument to the memory allocated passed as the
** second argument to ParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
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
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
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
    case 215: /* keep */
    case 216: /* tagitemlist */
    case 239: /* fill_opt */
    case 241: /* groupby_opt */
    case 242: /* orderby_opt */
    case 251: /* sortlist */
    case 255: /* grouplist */
{
tVariantListDestroy((yypminor->yy56));
}
      break;
    case 231: /* columnlist */
{
tFieldListDestroy((yypminor->yy471));
}
      break;
    case 232: /* select */
{
destroyQuerySql((yypminor->yy24));
}
      break;
    case 235: /* selcollist */
    case 246: /* sclp */
    case 256: /* exprlist */
    case 259: /* itemlist */
{
tSQLExprListDestroy((yypminor->yy498));
}
      break;
    case 237: /* where_opt */
    case 243: /* having_opt */
    case 247: /* expr */
    case 257: /* expritem */
{
tSQLExprDestroy((yypminor->yy90));
}
      break;
    case 252: /* sortitem */
{
tVariantDestroy(&(yypminor->yy186));
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
  assert( pParser->yyidx>=0 );
  yytos = &pParser->yystack[pParser->yyidx--];
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
  yyParser *pParser = (yyParser*)p;
#ifndef YYPARSEFREENEVERNULL
  if( pParser==0 ) return;
#endif
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
*/
static int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yystack[pParser->yyidx].stateno;
 
  if( stateno>=YY_MIN_REDUCE ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
  do{
    i = yy_shift_ofst[stateno];
    if( i==YY_SHIFT_USE_DFLT ) return yy_default[stateno];
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
            yy_lookahead[j]==YYWILDCARD
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
      }
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
/******** Begin %stack_overflow code ******************************************/
/******** End %stack_overflow code ********************************************/
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
}

/*
** Print tracing information for a SHIFT action
*/
#ifndef NDEBUG
static void yyTraceShift(yyParser *yypParser, int yyNewState){
  if( yyTraceFILE ){
    if( yyNewState<YYNSTATE ){
      fprintf(yyTraceFILE,"%sShift '%s', go to state %d\n",
         yyTracePrompt,yyTokenName[yypParser->yystack[yypParser->yyidx].major],
         yyNewState);
    }else{
      fprintf(yyTraceFILE,"%sShift '%s'\n",
         yyTracePrompt,yyTokenName[yypParser->yystack[yypParser->yyidx].major]);
    }
  }
}
#else
# define yyTraceShift(X,Y)
#endif

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
  yyTraceShift(yypParser, yyNewState);
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
  unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
} yyRuleInfo[] = {
  { 196, 1 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 197, 2 },
  { 198, 0 },
  { 198, 2 },
  { 200, 0 },
  { 200, 2 },
  { 197, 3 },
  { 197, 5 },
  { 197, 3 },
  { 197, 5 },
  { 197, 3 },
  { 197, 5 },
  { 197, 4 },
  { 197, 3 },
  { 197, 3 },
  { 197, 3 },
  { 197, 2 },
  { 197, 3 },
  { 197, 5 },
  { 197, 5 },
  { 197, 4 },
  { 197, 5 },
  { 197, 3 },
  { 197, 4 },
  { 197, 4 },
  { 197, 4 },
  { 197, 6 },
  { 199, 1 },
  { 199, 1 },
  { 201, 2 },
  { 201, 0 },
  { 204, 3 },
  { 204, 0 },
  { 197, 3 },
  { 197, 6 },
  { 197, 5 },
  { 197, 5 },
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
  { 213, 0 },
  { 213, 2 },
  { 214, 0 },
  { 214, 2 },
  { 203, 9 },
  { 215, 2 },
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
  { 205, 0 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 205, 2 },
  { 202, 0 },
  { 202, 2 },
  { 202, 2 },
  { 228, 1 },
  { 228, 4 },
  { 229, 1 },
  { 229, 2 },
  { 229, 2 },
  { 197, 6 },
  { 230, 3 },
  { 230, 7 },
  { 230, 7 },
  { 230, 2 },
  { 231, 3 },
  { 231, 1 },
  { 233, 2 },
  { 216, 3 },
  { 216, 1 },
  { 234, 1 },
  { 234, 1 },
  { 234, 1 },
  { 234, 1 },
  { 234, 1 },
  { 234, 2 },
  { 234, 2 },
  { 234, 2 },
  { 234, 2 },
  { 197, 1 },
  { 232, 12 },
  { 246, 2 },
  { 246, 0 },
  { 235, 3 },
  { 235, 2 },
  { 248, 2 },
  { 248, 1 },
  { 248, 0 },
  { 236, 2 },
  { 249, 2 },
  { 249, 4 },
  { 250, 1 },
  { 238, 4 },
  { 238, 0 },
  { 239, 0 },
  { 239, 6 },
  { 239, 4 },
  { 240, 4 },
  { 240, 0 },
  { 242, 0 },
  { 242, 3 },
  { 251, 4 },
  { 251, 2 },
  { 253, 2 },
  { 254, 1 },
  { 254, 1 },
  { 254, 0 },
  { 241, 0 },
  { 241, 3 },
  { 255, 3 },
  { 255, 1 },
  { 243, 0 },
  { 243, 2 },
  { 245, 0 },
  { 245, 2 },
  { 245, 4 },
  { 245, 4 },
  { 244, 0 },
  { 244, 2 },
  { 244, 4 },
  { 244, 4 },
  { 237, 0 },
  { 237, 2 },
  { 247, 3 },
  { 247, 1 },
  { 247, 3 },
  { 247, 3 },
  { 247, 1 },
  { 247, 2 },
  { 247, 2 },
  { 247, 1 },
  { 247, 2 },
  { 247, 2 },
  { 247, 1 },
  { 247, 1 },
  { 247, 1 },
  { 247, 1 },
  { 247, 4 },
  { 247, 4 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 3 },
  { 247, 5 },
  { 256, 3 },
  { 256, 1 },
  { 257, 1 },
  { 257, 0 },
  { 197, 4 },
  { 258, 4 },
  { 258, 5 },
  { 259, 3 },
  { 259, 1 },
  { 197, 3 },
  { 197, 7 },
  { 197, 7 },
  { 197, 7 },
  { 197, 7 },
  { 197, 8 },
  { 197, 9 },
  { 197, 5 },
  { 197, 7 },
  { 197, 7 },
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
    yysize = yyRuleInfo[yyruleno].nrhs;
    fprintf(yyTraceFILE, "%sReduce [%s], go to state %d.\n", yyTracePrompt,
      yyRuleName[yyruleno], yymsp[-yysize].stateno);
  }
#endif /* NDEBUG */
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
/********** Begin reduce actions **********************************************/
      case 0: /* program ::= cmd */
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setDCLSQLElems(pInfo, SHOW_DATABASES, 0);}
        break;
      case 2: /* cmd ::= SHOW MNODES */
{ setDCLSQLElems(pInfo, SHOW_MNODES, 0);}
        break;
      case 3: /* cmd ::= SHOW DNODES */
{ setDCLSQLElems(pInfo, SHOW_DNODES, 0);}
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
{ setDCLSQLElems(pInfo, SHOW_ACCOUNTS, 0);}
        break;
      case 5: /* cmd ::= SHOW USERS */
{ setDCLSQLElems(pInfo, SHOW_USERS, 0);}
        break;
      case 6: /* cmd ::= SHOW MODULES */
{ setDCLSQLElems(pInfo, SHOW_MODULES, 0);  }
        break;
      case 7: /* cmd ::= SHOW QUERIES */
{ setDCLSQLElems(pInfo, SHOW_QUERIES, 0);  }
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
{ setDCLSQLElems(pInfo, SHOW_CONNECTIONS, 0);}
        break;
      case 9: /* cmd ::= SHOW STREAMS */
{ setDCLSQLElems(pInfo, SHOW_STREAMS, 0);  }
        break;
      case 10: /* cmd ::= SHOW CONFIGS */
{ setDCLSQLElems(pInfo, SHOW_CONFIGS, 0);  }
        break;
      case 11: /* cmd ::= SHOW SCORES */
{ setDCLSQLElems(pInfo, SHOW_SCORES, 0);   }
        break;
      case 12: /* cmd ::= SHOW GRANTS */
{ setDCLSQLElems(pInfo, SHOW_GRANTS, 0);   }
        break;
      case 13: /* dbPrefix ::= */
      case 41: /* ifexists ::= */ yytestcase(yyruleno==41);
      case 43: /* ifnotexists ::= */ yytestcase(yyruleno==43);
{yygotominor.yy0.n = 0;}
        break;
      case 14: /* dbPrefix ::= ids DOT */
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
        break;
      case 15: /* cpxName ::= */
{yygotominor.yy0.n = 0;  }
        break;
      case 16: /* cpxName ::= DOT ids */
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
        break;
      case 17: /* cmd ::= SHOW dbPrefix TABLES */
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 18: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 2, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 19: /* cmd ::= SHOW dbPrefix STABLES */
{
    setDCLSQLElems(pInfo, SHOW_STABLES, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_STABLES, 2, &token, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_VGROUPS, 1, &token);
}
        break;
      case 22: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DROP_TABLE, 2, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 23: /* cmd ::= DROP DATABASE ifexists ids */
{ setDCLSQLElems(pInfo, DROP_DATABASE, 2, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 24: /* cmd ::= DROP DNODE IP */
{ setDCLSQLElems(pInfo, DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 25: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 26: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, DROP_ACCOUNT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 27: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, USE_DATABASE, 1, &yymsp[0].minor.yy0);}
        break;
      case 28: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 29: /* cmd ::= ALTER USER ids PASS ids */
{ setDCLSQLElems(pInfo, ALTER_USER_PASSWD, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);    }
        break;
      case 30: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setDCLSQLElems(pInfo, ALTER_USER_PRIVILEGES, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 31: /* cmd ::= ALTER DNODE IP ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 32: /* cmd ::= ALTER DNODE IP ids ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 33: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 34: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 35: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, ALTER_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy398, &t);}
        break;
      case 36: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ SSQLToken t = {0};  setCreateAcctSQL(pInfo, ALTER_ACCT, &yymsp[-1].minor.yy0, &t, &yymsp[0].minor.yy279);}
        break;
      case 37: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy279);}
        break;
      case 38: /* ids ::= ID */
      case 39: /* ids ::= STRING */ yytestcase(yyruleno==39);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 40: /* ifexists ::= IF EXISTS */
      case 42: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==42);
{yygotominor.yy0.n = 1;}
        break;
      case 44: /* cmd ::= CREATE DNODE IP */
{ setDCLSQLElems(pInfo, CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 45: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, CREATE_ACCOUNT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy279);}
        break;
      case 46: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, CREATE_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy398, &yymsp[-2].minor.yy0);}
        break;
      case 47: /* cmd ::= CREATE USER ids PASS ids */
{ setDCLSQLElems(pInfo, CREATE_USER, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 48: /* pps ::= */
      case 50: /* tseries ::= */ yytestcase(yyruleno==50);
      case 52: /* dbs ::= */ yytestcase(yyruleno==52);
      case 54: /* streams ::= */ yytestcase(yyruleno==54);
      case 56: /* storage ::= */ yytestcase(yyruleno==56);
      case 58: /* qtime ::= */ yytestcase(yyruleno==58);
      case 60: /* users ::= */ yytestcase(yyruleno==60);
      case 62: /* conns ::= */ yytestcase(yyruleno==62);
      case 64: /* state ::= */ yytestcase(yyruleno==64);
      case 133: /* interval_opt ::= */ yytestcase(yyruleno==133);
      case 138: /* sliding_opt ::= */ yytestcase(yyruleno==138);
{yygotominor.yy0.n = 0;   }
        break;
      case 49: /* pps ::= PPS INTEGER */
      case 51: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==51);
      case 53: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==53);
      case 55: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==55);
      case 57: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==57);
      case 59: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==59);
      case 61: /* users ::= USERS INTEGER */ yytestcase(yyruleno==61);
      case 63: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==63);
      case 65: /* state ::= STATE ids */ yytestcase(yyruleno==65);
{yygotominor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 66: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yygotominor.yy279.users   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy279.dbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy279.tseries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy279.streams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy279.pps     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy279.storage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy279.qtime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy279.conns   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy279.stat    = yymsp[0].minor.yy0;
}
        break;
      case 67: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy56 = yymsp[0].minor.yy56; }
        break;
      case 68: /* tables ::= TABLES INTEGER */
      case 69: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==69);
      case 70: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==70);
      case 71: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==71);
      case 72: /* rows ::= ROWS INTEGER */ yytestcase(yyruleno==72);
      case 73: /* ablocks ::= ABLOCKS ID */ yytestcase(yyruleno==73);
      case 74: /* tblocks ::= TBLOCKS INTEGER */ yytestcase(yyruleno==74);
      case 75: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==75);
      case 76: /* clog ::= CLOG INTEGER */ yytestcase(yyruleno==76);
      case 77: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==77);
      case 78: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==78);
      case 79: /* db_optr ::= */ yytestcase(yyruleno==79);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 80: /* db_optr ::= db_optr tables */
      case 94: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==94);
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 81: /* db_optr ::= db_optr cache */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 82: /* db_optr ::= db_optr replica */
      case 93: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==93);
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 83: /* db_optr ::= db_optr days */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 84: /* db_optr ::= db_optr rows */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 85: /* db_optr ::= db_optr ablocks */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 86: /* db_optr ::= db_optr tblocks */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 87: /* db_optr ::= db_optr ctime */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 88: /* db_optr ::= db_optr clog */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 89: /* db_optr ::= db_optr comp */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 90: /* db_optr ::= db_optr prec */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.precision = yymsp[0].minor.yy0; }
        break;
      case 91: /* db_optr ::= db_optr keep */
{ yygotominor.yy398 = yymsp[-1].minor.yy398; yygotominor.yy398.keep = yymsp[0].minor.yy56; }
        break;
      case 92: /* alter_db_optr ::= */
{ memset(&yygotominor.yy398, 0, sizeof(SCreateDBInfo));}
        break;
      case 95: /* typename ::= ids */
{ tSQLSetColumnType (&yygotominor.yy223, &yymsp[0].minor.yy0); }
        break;
      case 96: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy389;          // negative value of name length
    tSQLSetColumnType(&yygotominor.yy223, &yymsp[-3].minor.yy0);
}
        break;
      case 97: /* signed ::= INTEGER */
      case 98: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==98);
{ yygotominor.yy389 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* signed ::= MINUS INTEGER */
{ yygotominor.yy389 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 100: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedMeterName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 101: /* create_table_args ::= LP columnlist RP */
{
    yygotominor.yy158 = tSetCreateSQLElems(yymsp[-1].minor.yy471, NULL, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METER);
    setSQLInfo(pInfo, yygotominor.yy158, NULL, TSQL_CREATE_NORMAL_METER);
}
        break;
      case 102: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yygotominor.yy158 = tSetCreateSQLElems(yymsp[-5].minor.yy471, yymsp[-1].minor.yy471, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METRIC);
    setSQLInfo(pInfo, yygotominor.yy158, NULL, TSQL_CREATE_NORMAL_METRIC);
}
        break;
      case 103: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yygotominor.yy158 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy56, NULL, TSQL_CREATE_METER_FROM_METRIC);
    setSQLInfo(pInfo, yygotominor.yy158, NULL, TSQL_CREATE_METER_FROM_METRIC);
}
        break;
      case 104: /* create_table_args ::= AS select */
{
    yygotominor.yy158 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy24, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yygotominor.yy158, NULL, TSQL_CREATE_STREAM);
}
        break;
      case 105: /* columnlist ::= columnlist COMMA column */
{yygotominor.yy471 = tFieldListAppend(yymsp[-2].minor.yy471, &yymsp[0].minor.yy223);   }
        break;
      case 106: /* columnlist ::= column */
{yygotominor.yy471 = tFieldListAppend(NULL, &yymsp[0].minor.yy223);}
        break;
      case 107: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yygotominor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
        break;
      case 108: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy56 = tVariantListAppend(yymsp[-2].minor.yy56, &yymsp[0].minor.yy186, -1);    }
        break;
      case 109: /* tagitemlist ::= tagitem */
{ yygotominor.yy56 = tVariantListAppend(NULL, &yymsp[0].minor.yy186, -1); }
        break;
      case 110: /* tagitem ::= INTEGER */
      case 111: /* tagitem ::= FLOAT */ yytestcase(yyruleno==111);
      case 112: /* tagitem ::= STRING */ yytestcase(yyruleno==112);
      case 113: /* tagitem ::= BOOL */ yytestcase(yyruleno==113);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy186, &yymsp[0].minor.yy0); }
        break;
      case 114: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy186, &yymsp[0].minor.yy0); }
        break;
      case 115: /* tagitem ::= MINUS INTEGER */
      case 116: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==116);
      case 117: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==117);
      case 118: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==118);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy186, &yymsp[-1].minor.yy0);
}
        break;
      case 119: /* cmd ::= select */
{
    setSQLInfo(pInfo, yymsp[0].minor.yy24, NULL, TSQL_QUERY_METER);
}
        break;
      case 120: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy24 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy498, yymsp[-9].minor.yy56, yymsp[-8].minor.yy90, yymsp[-4].minor.yy56, yymsp[-3].minor.yy56, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy56, &yymsp[0].minor.yy294, &yymsp[-1].minor.yy294);
}
        break;
      case 121: /* sclp ::= selcollist COMMA */
{yygotominor.yy498 = yymsp[-1].minor.yy498;}
        break;
      case 122: /* sclp ::= */
{yygotominor.yy498 = 0;}
        break;
      case 123: /* selcollist ::= sclp expr as */
{
   yygotominor.yy498 = tSQLExprListAppend(yymsp[-2].minor.yy498, yymsp[-1].minor.yy90, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 124: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yygotominor.yy498 = tSQLExprListAppend(yymsp[-1].minor.yy498, pNode, 0);
}
        break;
      case 125: /* as ::= AS ids */
      case 126: /* as ::= ids */ yytestcase(yyruleno==126);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 127: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 128: /* from ::= FROM tablelist */
      case 140: /* orderby_opt ::= ORDER BY sortlist */ yytestcase(yyruleno==140);
      case 148: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==148);
{yygotominor.yy56 = yymsp[0].minor.yy56;}
        break;
      case 129: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yygotominor.yy56 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
        break;
      case 130: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yygotominor.yy56 = tVariantListAppendToken(yymsp[-3].minor.yy56, &yymsp[-1].minor.yy0, -1);   }
        break;
      case 131: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 132: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 137: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==137);
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 134: /* fill_opt ::= */
{yygotominor.yy56 = 0;     }
        break;
      case 135: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy56, &A, -1, 0);
    yygotominor.yy56 = yymsp[-1].minor.yy56;
}
        break;
      case 136: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy56 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 139: /* orderby_opt ::= */
      case 147: /* groupby_opt ::= */ yytestcase(yyruleno==147);
{yygotominor.yy56 = 0;}
        break;
      case 141: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy56 = tVariantListAppend(yymsp[-3].minor.yy56, &yymsp[-1].minor.yy186, yymsp[0].minor.yy332);
}
        break;
      case 142: /* sortlist ::= item sortorder */
{
  yygotominor.yy56 = tVariantListAppend(NULL, &yymsp[-1].minor.yy186, yymsp[0].minor.yy332);
}
        break;
      case 143: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy186, &yymsp[-1].minor.yy0);
}
        break;
      case 144: /* sortorder ::= ASC */
{yygotominor.yy332 = TSQL_SO_ASC; }
        break;
      case 145: /* sortorder ::= DESC */
{yygotominor.yy332 = TSQL_SO_DESC;}
        break;
      case 146: /* sortorder ::= */
{yygotominor.yy332 = TSQL_SO_ASC;}
        break;
      case 149: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy56 = tVariantListAppend(yymsp[-2].minor.yy56, &yymsp[0].minor.yy186, -1);
}
        break;
      case 150: /* grouplist ::= item */
{
  yygotominor.yy56 = tVariantListAppend(NULL, &yymsp[0].minor.yy186, -1);
}
        break;
      case 151: /* having_opt ::= */
      case 161: /* where_opt ::= */ yytestcase(yyruleno==161);
      case 197: /* expritem ::= */ yytestcase(yyruleno==197);
{yygotominor.yy90 = 0;}
        break;
      case 152: /* having_opt ::= HAVING expr */
      case 162: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==162);
      case 196: /* expritem ::= expr */ yytestcase(yyruleno==196);
{yygotominor.yy90 = yymsp[0].minor.yy90;}
        break;
      case 153: /* limit_opt ::= */
      case 157: /* slimit_opt ::= */ yytestcase(yyruleno==157);
{yygotominor.yy294.limit = -1; yygotominor.yy294.offset = 0;}
        break;
      case 154: /* limit_opt ::= LIMIT signed */
      case 158: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==158);
{yygotominor.yy294.limit = yymsp[0].minor.yy389;  yygotominor.yy294.offset = 0;}
        break;
      case 155: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 159: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==159);
{yygotominor.yy294.limit = yymsp[-2].minor.yy389;  yygotominor.yy294.offset = yymsp[0].minor.yy389;}
        break;
      case 156: /* limit_opt ::= LIMIT signed COMMA signed */
      case 160: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==160);
{yygotominor.yy294.limit = yymsp[0].minor.yy389;  yygotominor.yy294.offset = yymsp[-2].minor.yy389;}
        break;
      case 163: /* expr ::= LP expr RP */
{yygotominor.yy90 = yymsp[-1].minor.yy90; }
        break;
      case 164: /* expr ::= ID */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 165: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 166: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 167: /* expr ::= INTEGER */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 168: /* expr ::= MINUS INTEGER */
      case 169: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==169);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 170: /* expr ::= FLOAT */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 171: /* expr ::= MINUS FLOAT */
      case 172: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==172);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 173: /* expr ::= STRING */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 174: /* expr ::= NOW */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 175: /* expr ::= VARIABLE */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 176: /* expr ::= BOOL */
{yygotominor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 177: /* expr ::= ID LP exprlist RP */
{
  yygotominor.yy90 = tSQLExprCreateFunction(yymsp[-1].minor.yy498, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
        break;
      case 178: /* expr ::= ID LP STAR RP */
{
  yygotominor.yy90 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
        break;
      case 179: /* expr ::= expr AND expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_AND);}
        break;
      case 180: /* expr ::= expr OR expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_OR); }
        break;
      case 181: /* expr ::= expr LT expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_LT);}
        break;
      case 182: /* expr ::= expr GT expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_GT);}
        break;
      case 183: /* expr ::= expr LE expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_LE);}
        break;
      case 184: /* expr ::= expr GE expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_GE);}
        break;
      case 185: /* expr ::= expr NE expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_NE);}
        break;
      case 186: /* expr ::= expr EQ expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_EQ);}
        break;
      case 187: /* expr ::= expr PLUS expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_PLUS);  }
        break;
      case 188: /* expr ::= expr MINUS expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_MINUS); }
        break;
      case 189: /* expr ::= expr STAR expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_STAR);  }
        break;
      case 190: /* expr ::= expr SLASH expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_DIVIDE);}
        break;
      case 191: /* expr ::= expr REM expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_REM);   }
        break;
      case 192: /* expr ::= expr LIKE expr */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_LIKE);  }
        break;
      case 193: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy90 = tSQLExprCreate(yymsp[-4].minor.yy90, (tSQLExpr*)yymsp[-1].minor.yy498, TK_IN); }
        break;
      case 194: /* exprlist ::= exprlist COMMA expritem */
      case 201: /* itemlist ::= itemlist COMMA expr */ yytestcase(yyruleno==201);
{yygotominor.yy498 = tSQLExprListAppend(yymsp[-2].minor.yy498,yymsp[0].minor.yy90,0);}
        break;
      case 195: /* exprlist ::= expritem */
      case 202: /* itemlist ::= expr */ yytestcase(yyruleno==202);
{yygotominor.yy498 = tSQLExprListAppend(0,yymsp[0].minor.yy90,0);}
        break;
      case 198: /* cmd ::= INSERT INTO cpxName insert_value_list */
{
    tSetInsertSQLElems(pInfo, &yymsp[-1].minor.yy0, yymsp[0].minor.yy74);
}
        break;
      case 199: /* insert_value_list ::= VALUES LP itemlist RP */
{yygotominor.yy74 = tSQLListListAppend(NULL, yymsp[-1].minor.yy498);}
        break;
      case 200: /* insert_value_list ::= insert_value_list VALUES LP itemlist RP */
{yygotominor.yy74 = tSQLListListAppend(yymsp[-4].minor.yy74, yymsp[-1].minor.yy498);}
        break;
      case 203: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, RESET_QUERY_CACHE, 0);}
        break;
      case 204: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy471, NULL, ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_ADD_COLUMN);
}
        break;
      case 205: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_DROP_COLUMN);
}
        break;
      case 206: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy471, NULL, ALTER_TABLE_TAGS_ADD);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_ADD);
}
        break;
      case 207: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, ALTER_TABLE_TAGS_DROP);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_DROP);
}
        break;
      case 208: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, ALTER_TABLE_TAGS_CHG);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_CHG);
}
        break;
      case 209: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy186, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, ALTER_TABLE_TAGS_SET);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_SET);
}
        break;
      case 210: /* cmd ::= KILL CONNECTION IP COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_CONNECTION, 1, &yymsp[-2].minor.yy0);}
        break;
      case 211: /* cmd ::= KILL STREAM IP COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_STREAM, 1, &yymsp[-4].minor.yy0);}
        break;
      case 212: /* cmd ::= KILL QUERY IP COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_QUERY, 1, &yymsp[-4].minor.yy0);}
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno>=0 && yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yypParser->yyidx -= yysize;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if( yyact <= YY_MAX_SHIFTREDUCE ){
    if( yyact>YY_MAX_SHIFT ) yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
    /* If the reduce action popped at least
    ** one element off the stack, then we can push the new element back
    ** onto the stack here, and skip the stack overflow test in yy_shift().
    ** That gives a significant speed improvement. */
    if( yysize ){
      yypParser->yyidx++;
      yymsp -= yysize-1;
      yymsp->stateno = (YYACTIONTYPE)yyact;
      yymsp->major = (YYCODETYPE)yygoto;
      yymsp->minor = yygotominor;
      yyTraceShift(yypParser, yyact);
    }else{
      yy_shift(yypParser,yyact,yygoto,&yygotominor);
    }
  }else{
    assert( yyact == YY_ACCEPT_ACTION );
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
  YYMINORTYPE yyminor            /* The minor type of the error token */
){
  ParseARG_FETCH;
#define TOKEN (yyminor.yy0)
/************ Begin %syntax_error code ****************************************/

  pInfo->validSql = false;
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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
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
  int yyact;            /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
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
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sInitialize. Empty stack. State 0\n",
              yyTracePrompt);
    }
#endif
  }
  yyminorunion.yy0 = yyminor;
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sInput '%s'\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact <= YY_MAX_SHIFTREDUCE ){
      if( yyact > YY_MAX_SHIFT ) yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
      yy_shift(yypParser,yyact,yymajor,&yyminorunion);
      yypParser->yyerrcnt--;
      yymajor = YYNOCODE;
    }else if( yyact <= YY_MAX_REDUCE ){
      yy_reduce(yypParser,yyact-YY_MIN_REDUCE);
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
                        YYERRORSYMBOL)) >= YY_MIN_REDUCE
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
#ifndef NDEBUG
  if( yyTraceFILE ){
    int i;
    fprintf(yyTraceFILE,"%sReturn. Stack=",yyTracePrompt);
    for(i=1; i<=yypParser->yyidx; i++)
      fprintf(yyTraceFILE,"%c%s", i==1 ? '[' : ' ', 
              yyTokenName[yypParser->yystack[i].major]);
    fprintf(yyTraceFILE,"]\n");
  }
#endif
  return;
}
