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
#include "tscSQLParser.h"
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
#define YYNRULE              214
#define YYNTOKEN             195
#define YY_MAX_SHIFT         250
#define YY_MIN_SHIFTREDUCE   401
#define YY_MAX_SHIFTREDUCE   614
#define YY_ERROR_ACTION      615
#define YY_ACCEPT_ACTION     616
#define YY_NO_ACTION         617
#define YY_MIN_REDUCE        618
#define YY_MAX_REDUCE        831
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
#define YY_ACTTAB_COUNT (530)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    97,  439,  135,  101,  101,  156,  616,  250,  135,  440,
 /*    10 */   135,  159,  814,   43,   45,   21,   37,   38,  813,  158,
 /*    20 */   814,   31,  439,  727,  205,   41,   39,   42,   40,   10,
 /*    30 */   440,  153,  249,   36,   35,  745,  726,   34,   33,   32,
 /*    40 */    43,   45,  737,   37,   38,  166,  132,  167,   31,  724,
 /*    50 */   193,  205,   41,   39,   42,   40,  202,  769,   59,  200,
 /*    60 */    36,   35,   21,  727,   34,   33,   32,   43,   45,  134,
 /*    70 */    37,   38,   74,   78,  244,   31,   85,   77,  205,   41,
 /*    80 */    39,   42,   40,   80,  742,  220,  498,   36,   35,  439,
 /*    90 */    21,   34,   33,   32,  168,  101,  724,  440,  101,   57,
 /*   100 */   114,  115,  224,  727,   65,   68,   45,    7,   37,   38,
 /*   110 */    62,  111,  241,   31,  230,  229,  205,   41,   39,   42,
 /*   120 */    40,  232,  228,  565,  724,   36,   35,   21,  139,   34,
 /*   130 */    33,   32,   21,  402,  403,  404,  405,  406,  407,  408,
 /*   140 */   409,  410,  411,  412,  413,  810,   37,   38,  243,  768,
 /*   150 */   725,   31,   60,  178,  205,   41,   39,   42,   40,  233,
 /*   160 */   186,  724,  183,   36,   35,  809,  723,   34,   33,   32,
 /*   170 */   654,  171,  808,  124,   17,  219,  242,  218,  217,  216,
 /*   180 */   215,  214,  213,  212,  211,  709,  151,  698,  699,  700,
 /*   190 */   701,  702,  703,  704,  705,  706,  707,  708,  163,  578,
 /*   200 */    11,  133,  569,  133,  572,   76,  575,  663,  163,  578,
 /*   210 */   124,  241,  569,  154,  572,  155,  575,  148,   34,   33,
 /*   220 */    32,  248,  247,  422,   87,   86,  142,  243,  546,  547,
 /*   230 */   160,  161,  147,  523,  204,  172,   18,  152,  227,  226,
 /*   240 */   160,  161,  163,  578,  526,  712,  569,  711,  572,  140,
 /*   250 */   575,   41,   39,   42,   40,  242,  141,   61,   27,   36,
 /*   260 */    35,   73,   72,   34,   33,   32,  514,  655,   28,  511,
 /*   270 */   124,  512,  143,  513,  160,  161,  192,   36,   35,  188,
 /*   280 */   601,   34,   33,   32,   29,  571,  150,  574,  567,  128,
 /*   290 */   126,  245,   44,   89,   88,  602,  537,  169,  170,   29,
 /*   300 */    47,  577,   44,  162,  538,  595,  579,  144,   15,   14,
 /*   310 */    14,  577,  570,   49,  573,  504,  576,   52,  503,   47,
 /*   320 */   145,  209,   22,  146,  568,   22,  576,  518,  828,  519,
 /*   330 */    50,  516,   53,  517,   84,   83,   44,    9,    8,  718,
 /*   340 */   137,    2,  131,  138,  136,  577,  779,  744,  778,  164,
 /*   350 */   775,  774,  165,  231,   98,  761,  760,  112,  113,  665,
 /*   360 */   576,  110,  189,  210,  129,  515,   25,  223,   91,  225,
 /*   370 */   827,   70,  826,  824,  116,  683,   26,   23,  130,  652,
 /*   380 */   533,   79,  650,   54,   81,  648,  647,  173,  125,  645,
 /*   390 */   644,  643,  641,  634,  127,  638,  191,  636,  738,  194,
 /*   400 */   198,   95,  748,  749,  762,   51,  102,   46,  203,  103,
 /*   410 */   201,  199,  197,  195,   30,   27,  222,   75,  234,  235,
 /*   420 */   207,   55,  236,  240,  238,  237,  239,   63,   66,  149,
 /*   430 */   246,  614,  175,  174,  176,  646,  613,   90,  640,  119,
 /*   440 */   123,  177,  684,  117,  118,  120,  106,  104,  722,  122,
 /*   450 */    92,  121,  108,  105,  107,  109,    1,   24,  180,  179,
 /*   460 */   181,  182,  612,  184,  185,  605,   58,   12,   13,   99,
 /*   470 */   190,  187,   96,  534,  157,  539,  196,  100,   19,   64,
 /*   480 */   479,  580,    3,   20,    4,   16,  206,    6,  208,  478,
 /*   490 */   477,  476,  475,    5,  474,  473,  472,  470,   47,  443,
 /*   500 */    67,  445,   22,  221,  500,   48,  499,  497,   56,  464,
 /*   510 */   462,  454,  460,   69,  456,   71,  458,  452,  450,  471,
 /*   520 */   469,   82,  441,  425,   93,  415,  618,  617,  617,   94,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   199,    1,  247,  199,  199,  216,  196,  197,  247,    9,
 /*    10 */   247,  256,  257,   13,   14,  199,   16,   17,  257,  256,
 /*    20 */   257,   21,    1,  234,   24,   25,   26,   27,   28,  247,
 /*    30 */     9,  198,  199,   33,   34,  199,  234,   37,   38,   39,
 /*    40 */    13,   14,  232,   16,   17,  216,  247,  231,   21,  233,
 /*    50 */   249,   24,   25,   26,   27,   28,  251,  253,  253,  255,
 /*    60 */    33,   34,  199,  234,   37,   38,   39,   13,   14,  247,
 /*    70 */    16,   17,   62,   63,   64,   21,   66,   67,   24,   25,
 /*    80 */    26,   27,   28,   73,  248,  216,    5,   33,   34,    1,
 /*    90 */   199,   37,   38,   39,  231,  199,  233,    9,  199,   99,
 /*   100 */    62,   63,   64,  234,   66,   67,   14,   95,   16,   17,
 /*   110 */    98,   99,   77,   21,   33,   34,   24,   25,   26,   27,
 /*   120 */    28,  199,  231,   96,  233,   33,   34,  199,  247,   37,
 /*   130 */    38,   39,  199,   45,   46,   47,   48,   49,   50,   51,
 /*   140 */    52,   53,   54,   55,   56,  247,   16,   17,   58,  253,
 /*   150 */   228,   21,  253,  124,   24,   25,   26,   27,   28,  231,
 /*   160 */   131,  233,  133,   33,   34,  247,  233,   37,   38,   39,
 /*   170 */   203,   61,  247,  206,   84,   85,   86,   87,   88,   89,
 /*   180 */    90,   91,   92,   93,   94,  215,  247,  217,  218,  219,
 /*   190 */   220,  221,  222,  223,  224,  225,  226,  227,    1,    2,
 /*   200 */    44,  247,    5,  247,    7,   71,    9,  203,    1,    2,
 /*   210 */   206,   77,    5,  259,    7,  259,    9,   61,   37,   38,
 /*   220 */    39,   58,   59,   60,   68,   69,   70,   58,  110,  111,
 /*   230 */    33,   34,   76,  100,   37,  125,  103,  247,  128,  129,
 /*   240 */    33,   34,    1,    2,   37,  217,    5,  219,    7,  247,
 /*   250 */     9,   25,   26,   27,   28,   86,  247,  235,  102,   33,
 /*   260 */    34,  126,  127,   37,   38,   39,    2,  203,  246,    5,
 /*   270 */   206,    7,  247,    9,   33,   34,  120,   33,   34,  123,
 /*   280 */    96,   37,   38,   39,  100,    5,  130,    7,    1,   62,
 /*   290 */    63,   64,   95,   66,   67,   96,   96,   33,   34,  100,
 /*   300 */   100,  104,   95,   57,   96,   96,   96,  247,  100,  100,
 /*   310 */   100,  104,    5,  100,    7,   96,  119,  100,   96,  100,
 /*   320 */   247,   96,  100,  247,   37,  100,  119,    5,  234,    7,
 /*   330 */   117,    5,  115,    7,   71,   72,   95,  126,  127,  230,
 /*   340 */   247,   95,  247,  247,  247,  104,  229,  199,  229,  229,
 /*   350 */   229,  229,  229,  229,  199,  254,  254,  199,  199,  199,
 /*   360 */   119,  236,  122,  199,  199,  101,  199,  199,   57,  199,
 /*   370 */   199,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   380 */   104,  199,  199,  114,  199,  199,  199,  199,  199,  199,
 /*   390 */   199,  199,  199,  199,  199,  199,  258,  199,  245,  250,
 /*   400 */   250,  200,  200,  200,  200,  116,  244,  113,  108,  243,
 /*   410 */   112,  107,  106,  105,  118,  102,   74,   83,   82,   49,
 /*   420 */   200,  200,   79,   78,   53,   81,   80,  204,  204,  200,
 /*   430 */    74,    5,    5,  132,  132,  200,    5,  201,  200,  208,
 /*   440 */   207,   65,  214,  213,  212,  211,  240,  242,  232,  210,
 /*   450 */   201,  209,  238,  241,  239,  237,  205,  202,    5,  132,
 /*   460 */   132,   65,    5,  132,   65,   85,  100,   95,   95,   95,
 /*   470 */   122,  124,  121,   96,    1,   96,   95,   95,  100,   71,
 /*   480 */     9,   96,   95,  100,  109,   95,   97,   95,   97,    5,
 /*   490 */     5,    5,    5,  109,    1,    5,    5,    5,  100,   75,
 /*   500 */    71,   65,  100,   15,    5,   16,    5,   96,   95,    5,
 /*   510 */     5,    5,    5,  127,    5,  127,    5,    5,    5,    5,
 /*   520 */     5,   65,   75,   65,   21,   57,    0,  260,  260,   21,
 /*   530 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   540 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   550 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   560 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   570 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   580 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   590 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   600 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   610 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   620 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   630 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   640 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   650 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   660 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   670 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   680 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   690 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   700 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   710 */   260,  260,  260,  260,  260,  260,  260,  260,  260,  260,
 /*   720 */   260,  260,  260,  260,  260,
};
#define YY_SHIFT_COUNT    (250)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (526)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   156,   90,  197,  241,   21,   21,   21,   21,   21,   21,
 /*    10 */     0,   88,  241,  241,  241,  264,  264,  264,   21,   21,
 /*    20 */    21,   21,   21,  134,  169,   35,   35,  530,  207,  241,
 /*    30 */   241,  241,  241,  241,  241,  241,  241,  241,  241,  241,
 /*    40 */   241,  241,  241,  241,  241,  241,  241,  264,  264,   81,
 /*    50 */    81,   81,   81,   81,   81,   12,   81,   21,   21,  118,
 /*    60 */   118,  133,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    70 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    80 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    90 */    21,   21,   21,   21,   21,  240,  311,  311,  311,  276,
 /*   100 */   276,  311,  269,  289,  294,  300,  298,  304,  306,  308,
 /*   110 */   296,  313,  311,  311,  342,  342,  311,  334,  336,  370,
 /*   120 */   343,  344,  371,  346,  345,  311,  356,  311,  356,  530,
 /*   130 */   530,   27,   54,   54,   54,   54,   54,   92,  130,  226,
 /*   140 */   226,  226,   10,  244,  244,  244,  244,   38,  227,  110,
 /*   150 */    29,  181,  181,  163,  184,  199,  200,  208,  209,  210,
 /*   160 */   280,  307,  287,  246,  213,  217,  219,  222,  225,  322,
 /*   170 */   326,  135,  211,  263,  426,  301,  427,  302,  376,  431,
 /*   180 */   327,  453,  328,  396,  457,  331,  399,  380,  347,  372,
 /*   190 */   373,  348,  351,  366,  377,  374,  473,  381,  379,  382,
 /*   200 */   378,  375,  383,  384,  385,  387,  390,  389,  392,  391,
 /*   210 */   408,  471,  484,  485,  486,  487,  493,  490,  491,  492,
 /*   220 */   398,  424,  488,  429,  436,  489,  386,  388,  402,  499,
 /*   230 */   501,  411,  413,  402,  504,  505,  506,  507,  509,  511,
 /*   240 */   512,  513,  514,  515,  456,  458,  447,  503,  508,  468,
 /*   250 */   526,
};
#define YY_REDUCE_COUNT (130)
#define YY_REDUCE_MIN   (-245)
#define YY_REDUCE_MAX   (255)
static const short yy_reduce_ofst[] = {
 /*     0 */  -190,  -30, -245, -237, -196, -195, -184, -137, -109,  -72,
 /*    10 */  -164, -167,  -46,  -44, -239, -211, -171, -131, -199, -104,
 /*    20 */  -101,  -78,  -67,  -33,   28,    4,   64,   22, -218, -201,
 /*    30 */  -178, -119, -102,  -82,  -75,  -61,  -10,    2,    9,   25,
 /*    40 */    60,   73,   76,   93,   95,   96,   97, -198,   94,  117,
 /*    50 */   119,  120,  121,  122,  123,  109,  124,  148,  155,  101,
 /*    60 */   102,  125,  158,  159,  160,  164,  165,  167,  168,  170,
 /*    70 */   171,  172,  173,  174,  175,  176,  177,  178,  179,  180,
 /*    80 */   182,  183,  185,  186,  187,  188,  189,  190,  191,  192,
 /*    90 */   193,  194,  195,  196,  198,  138,  201,  202,  203,  149,
 /*   100 */   150,  204,  153,  162,  166,  205,  212,  206,  215,  214,
 /*   110 */   218,  216,  220,  221,  223,  224,  229,  228,  230,  232,
 /*   120 */   231,  234,  242,  239,  233,  235,  236,  238,  249,  251,
 /*   130 */   255,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   615,  664,  816,  816,  615,  615,  615,  615,  615,  615,
 /*    10 */   746,  631,  615,  615,  816,  615,  615,  615,  615,  615,
 /*    20 */   615,  615,  615,  666,  653,  666,  666,  741,  615,  615,
 /*    30 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    40 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    50 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  765,
 /*    60 */   765,  739,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    70 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  651,
 /*    80 */   615,  649,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    90 */   615,  615,  615,  615,  615,  615,  633,  633,  633,  615,
 /*   100 */   615,  633,  772,  776,  770,  758,  766,  757,  753,  752,
 /*   110 */   780,  615,  633,  633,  661,  661,  633,  682,  680,  678,
 /*   120 */   670,  676,  672,  674,  668,  633,  659,  633,  659,  697,
 /*   130 */   710,  615,  820,  821,  781,  815,  771,  799,  798,  811,
 /*   140 */   805,  804,  615,  803,  802,  801,  800,  615,  615,  615,
 /*   150 */   615,  807,  806,  615,  615,  615,  615,  615,  615,  615,
 /*   160 */   615,  615,  615,  783,  777,  773,  615,  615,  615,  615,
 /*   170 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   180 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   190 */   615,  817,  615,  747,  615,  615,  615,  615,  615,  615,
 /*   200 */   767,  615,  759,  615,  615,  615,  615,  615,  615,  719,
 /*   210 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   220 */   685,  615,  615,  615,  615,  615,  615,  615,  825,  615,
 /*   230 */   615,  615,  713,  823,  615,  615,  615,  615,  615,  615,
 /*   240 */   615,  615,  615,  615,  615,  615,  615,  637,  635,  615,
 /*   250 */   615,
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
    1,  /*    IPTOKEN => ID */
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
    1,  /*       NULL => ID */
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
  /*   57 */ "DOT",
  /*   58 */ "TABLES",
  /*   59 */ "STABLES",
  /*   60 */ "VGROUPS",
  /*   61 */ "DROP",
  /*   62 */ "TABLE",
  /*   63 */ "DATABASE",
  /*   64 */ "DNODE",
  /*   65 */ "IPTOKEN",
  /*   66 */ "USER",
  /*   67 */ "ACCOUNT",
  /*   68 */ "USE",
  /*   69 */ "DESCRIBE",
  /*   70 */ "ALTER",
  /*   71 */ "PASS",
  /*   72 */ "PRIVILEGE",
  /*   73 */ "LOCAL",
  /*   74 */ "IF",
  /*   75 */ "EXISTS",
  /*   76 */ "CREATE",
  /*   77 */ "PPS",
  /*   78 */ "TSERIES",
  /*   79 */ "DBS",
  /*   80 */ "STORAGE",
  /*   81 */ "QTIME",
  /*   82 */ "CONNS",
  /*   83 */ "STATE",
  /*   84 */ "KEEP",
  /*   85 */ "CACHE",
  /*   86 */ "REPLICA",
  /*   87 */ "DAYS",
  /*   88 */ "ROWS",
  /*   89 */ "ABLOCKS",
  /*   90 */ "TBLOCKS",
  /*   91 */ "CTIME",
  /*   92 */ "CLOG",
  /*   93 */ "COMP",
  /*   94 */ "PRECISION",
  /*   95 */ "LP",
  /*   96 */ "RP",
  /*   97 */ "TAGS",
  /*   98 */ "USING",
  /*   99 */ "AS",
  /*  100 */ "COMMA",
  /*  101 */ "NULL",
  /*  102 */ "SELECT",
  /*  103 */ "FROM",
  /*  104 */ "VARIABLE",
  /*  105 */ "INTERVAL",
  /*  106 */ "FILL",
  /*  107 */ "SLIDING",
  /*  108 */ "ORDER",
  /*  109 */ "BY",
  /*  110 */ "ASC",
  /*  111 */ "DESC",
  /*  112 */ "GROUP",
  /*  113 */ "HAVING",
  /*  114 */ "LIMIT",
  /*  115 */ "OFFSET",
  /*  116 */ "SLIMIT",
  /*  117 */ "SOFFSET",
  /*  118 */ "WHERE",
  /*  119 */ "NOW",
  /*  120 */ "INSERT",
  /*  121 */ "INTO",
  /*  122 */ "VALUES",
  /*  123 */ "RESET",
  /*  124 */ "QUERY",
  /*  125 */ "ADD",
  /*  126 */ "COLUMN",
  /*  127 */ "TAG",
  /*  128 */ "CHANGE",
  /*  129 */ "SET",
  /*  130 */ "KILL",
  /*  131 */ "CONNECTION",
  /*  132 */ "COLON",
  /*  133 */ "STREAM",
  /*  134 */ "ABORT",
  /*  135 */ "AFTER",
  /*  136 */ "ATTACH",
  /*  137 */ "BEFORE",
  /*  138 */ "BEGIN",
  /*  139 */ "CASCADE",
  /*  140 */ "CLUSTER",
  /*  141 */ "CONFLICT",
  /*  142 */ "COPY",
  /*  143 */ "DEFERRED",
  /*  144 */ "DELIMITERS",
  /*  145 */ "DETACH",
  /*  146 */ "EACH",
  /*  147 */ "END",
  /*  148 */ "EXPLAIN",
  /*  149 */ "FAIL",
  /*  150 */ "FOR",
  /*  151 */ "IGNORE",
  /*  152 */ "IMMEDIATE",
  /*  153 */ "INITIALLY",
  /*  154 */ "INSTEAD",
  /*  155 */ "MATCH",
  /*  156 */ "KEY",
  /*  157 */ "OF",
  /*  158 */ "RAISE",
  /*  159 */ "REPLACE",
  /*  160 */ "RESTRICT",
  /*  161 */ "ROW",
  /*  162 */ "STATEMENT",
  /*  163 */ "TRIGGER",
  /*  164 */ "VIEW",
  /*  165 */ "ALL",
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
  /*  185 */ "SEMI",
  /*  186 */ "NONE",
  /*  187 */ "PREV",
  /*  188 */ "LINEAR",
  /*  189 */ "IMPORT",
  /*  190 */ "METRIC",
  /*  191 */ "TBNAME",
  /*  192 */ "JOIN",
  /*  193 */ "METRICS",
  /*  194 */ "STABLE",
  /*  195 */ "error",
  /*  196 */ "program",
  /*  197 */ "cmd",
  /*  198 */ "dbPrefix",
  /*  199 */ "ids",
  /*  200 */ "cpxName",
  /*  201 */ "ifexists",
  /*  202 */ "alter_db_optr",
  /*  203 */ "acct_optr",
  /*  204 */ "ifnotexists",
  /*  205 */ "db_optr",
  /*  206 */ "pps",
  /*  207 */ "tseries",
  /*  208 */ "dbs",
  /*  209 */ "streams",
  /*  210 */ "storage",
  /*  211 */ "qtime",
  /*  212 */ "users",
  /*  213 */ "conns",
  /*  214 */ "state",
  /*  215 */ "keep",
  /*  216 */ "tagitemlist",
  /*  217 */ "tables",
  /*  218 */ "cache",
  /*  219 */ "replica",
  /*  220 */ "days",
  /*  221 */ "rows",
  /*  222 */ "ablocks",
  /*  223 */ "tblocks",
  /*  224 */ "ctime",
  /*  225 */ "clog",
  /*  226 */ "comp",
  /*  227 */ "prec",
  /*  228 */ "typename",
  /*  229 */ "signed",
  /*  230 */ "create_table_args",
  /*  231 */ "columnlist",
  /*  232 */ "select",
  /*  233 */ "column",
  /*  234 */ "tagitem",
  /*  235 */ "selcollist",
  /*  236 */ "from",
  /*  237 */ "where_opt",
  /*  238 */ "interval_opt",
  /*  239 */ "fill_opt",
  /*  240 */ "sliding_opt",
  /*  241 */ "groupby_opt",
  /*  242 */ "orderby_opt",
  /*  243 */ "having_opt",
  /*  244 */ "slimit_opt",
  /*  245 */ "limit_opt",
  /*  246 */ "sclp",
  /*  247 */ "expr",
  /*  248 */ "as",
  /*  249 */ "tablelist",
  /*  250 */ "tmvar",
  /*  251 */ "sortlist",
  /*  252 */ "sortitem",
  /*  253 */ "item",
  /*  254 */ "sortorder",
  /*  255 */ "grouplist",
  /*  256 */ "exprlist",
  /*  257 */ "expritem",
  /*  258 */ "insert_value_list",
  /*  259 */ "itemlist",
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
 /*  24 */ "cmd ::= DROP DNODE IPTOKEN",
 /*  25 */ "cmd ::= DROP USER ids",
 /*  26 */ "cmd ::= DROP ACCOUNT ids",
 /*  27 */ "cmd ::= USE ids",
 /*  28 */ "cmd ::= DESCRIBE ids cpxName",
 /*  29 */ "cmd ::= ALTER USER ids PASS ids",
 /*  30 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  31 */ "cmd ::= ALTER DNODE IPTOKEN ids",
 /*  32 */ "cmd ::= ALTER DNODE IPTOKEN ids ids",
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
 /*  44 */ "cmd ::= CREATE DNODE IPTOKEN",
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
 /* 121 */ "select ::= SELECT selcollist",
 /* 122 */ "sclp ::= selcollist COMMA",
 /* 123 */ "sclp ::=",
 /* 124 */ "selcollist ::= sclp expr as",
 /* 125 */ "selcollist ::= sclp STAR",
 /* 126 */ "as ::= AS ids",
 /* 127 */ "as ::= ids",
 /* 128 */ "as ::=",
 /* 129 */ "from ::= FROM tablelist",
 /* 130 */ "tablelist ::= ids cpxName",
 /* 131 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 132 */ "tmvar ::= VARIABLE",
 /* 133 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 134 */ "interval_opt ::=",
 /* 135 */ "fill_opt ::=",
 /* 136 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 137 */ "fill_opt ::= FILL LP ID RP",
 /* 138 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 139 */ "sliding_opt ::=",
 /* 140 */ "orderby_opt ::=",
 /* 141 */ "orderby_opt ::= ORDER BY sortlist",
 /* 142 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 143 */ "sortlist ::= item sortorder",
 /* 144 */ "item ::= ids cpxName",
 /* 145 */ "sortorder ::= ASC",
 /* 146 */ "sortorder ::= DESC",
 /* 147 */ "sortorder ::=",
 /* 148 */ "groupby_opt ::=",
 /* 149 */ "groupby_opt ::= GROUP BY grouplist",
 /* 150 */ "grouplist ::= grouplist COMMA item",
 /* 151 */ "grouplist ::= item",
 /* 152 */ "having_opt ::=",
 /* 153 */ "having_opt ::= HAVING expr",
 /* 154 */ "limit_opt ::=",
 /* 155 */ "limit_opt ::= LIMIT signed",
 /* 156 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 157 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 158 */ "slimit_opt ::=",
 /* 159 */ "slimit_opt ::= SLIMIT signed",
 /* 160 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 161 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 162 */ "where_opt ::=",
 /* 163 */ "where_opt ::= WHERE expr",
 /* 164 */ "expr ::= LP expr RP",
 /* 165 */ "expr ::= ID",
 /* 166 */ "expr ::= ID DOT ID",
 /* 167 */ "expr ::= ID DOT STAR",
 /* 168 */ "expr ::= INTEGER",
 /* 169 */ "expr ::= MINUS INTEGER",
 /* 170 */ "expr ::= PLUS INTEGER",
 /* 171 */ "expr ::= FLOAT",
 /* 172 */ "expr ::= MINUS FLOAT",
 /* 173 */ "expr ::= PLUS FLOAT",
 /* 174 */ "expr ::= STRING",
 /* 175 */ "expr ::= NOW",
 /* 176 */ "expr ::= VARIABLE",
 /* 177 */ "expr ::= BOOL",
 /* 178 */ "expr ::= ID LP exprlist RP",
 /* 179 */ "expr ::= ID LP STAR RP",
 /* 180 */ "expr ::= expr AND expr",
 /* 181 */ "expr ::= expr OR expr",
 /* 182 */ "expr ::= expr LT expr",
 /* 183 */ "expr ::= expr GT expr",
 /* 184 */ "expr ::= expr LE expr",
 /* 185 */ "expr ::= expr GE expr",
 /* 186 */ "expr ::= expr NE expr",
 /* 187 */ "expr ::= expr EQ expr",
 /* 188 */ "expr ::= expr PLUS expr",
 /* 189 */ "expr ::= expr MINUS expr",
 /* 190 */ "expr ::= expr STAR expr",
 /* 191 */ "expr ::= expr SLASH expr",
 /* 192 */ "expr ::= expr REM expr",
 /* 193 */ "expr ::= expr LIKE expr",
 /* 194 */ "expr ::= expr IN LP exprlist RP",
 /* 195 */ "exprlist ::= exprlist COMMA expritem",
 /* 196 */ "exprlist ::= expritem",
 /* 197 */ "expritem ::= expr",
 /* 198 */ "expritem ::=",
 /* 199 */ "cmd ::= INSERT INTO cpxName insert_value_list",
 /* 200 */ "insert_value_list ::= VALUES LP itemlist RP",
 /* 201 */ "insert_value_list ::= insert_value_list VALUES LP itemlist RP",
 /* 202 */ "itemlist ::= itemlist COMMA expr",
 /* 203 */ "itemlist ::= expr",
 /* 204 */ "cmd ::= RESET QUERY CACHE",
 /* 205 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 206 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 207 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 208 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 209 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 210 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 211 */ "cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER",
 /* 212 */ "cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER",
 /* 213 */ "cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER",
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
  {  196,   -1 }, /* (0) program ::= cmd */
  {  197,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  197,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  197,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  197,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  197,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  197,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  197,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  197,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  197,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  197,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  197,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  197,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  198,    0 }, /* (13) dbPrefix ::= */
  {  198,   -2 }, /* (14) dbPrefix ::= ids DOT */
  {  200,    0 }, /* (15) cpxName ::= */
  {  200,   -2 }, /* (16) cpxName ::= DOT ids */
  {  197,   -3 }, /* (17) cmd ::= SHOW dbPrefix TABLES */
  {  197,   -5 }, /* (18) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  197,   -3 }, /* (19) cmd ::= SHOW dbPrefix STABLES */
  {  197,   -5 }, /* (20) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  197,   -3 }, /* (21) cmd ::= SHOW dbPrefix VGROUPS */
  {  197,   -5 }, /* (22) cmd ::= DROP TABLE ifexists ids cpxName */
  {  197,   -4 }, /* (23) cmd ::= DROP DATABASE ifexists ids */
  {  197,   -3 }, /* (24) cmd ::= DROP DNODE IPTOKEN */
  {  197,   -3 }, /* (25) cmd ::= DROP USER ids */
  {  197,   -3 }, /* (26) cmd ::= DROP ACCOUNT ids */
  {  197,   -2 }, /* (27) cmd ::= USE ids */
  {  197,   -3 }, /* (28) cmd ::= DESCRIBE ids cpxName */
  {  197,   -5 }, /* (29) cmd ::= ALTER USER ids PASS ids */
  {  197,   -5 }, /* (30) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  197,   -4 }, /* (31) cmd ::= ALTER DNODE IPTOKEN ids */
  {  197,   -5 }, /* (32) cmd ::= ALTER DNODE IPTOKEN ids ids */
  {  197,   -3 }, /* (33) cmd ::= ALTER LOCAL ids */
  {  197,   -4 }, /* (34) cmd ::= ALTER LOCAL ids ids */
  {  197,   -4 }, /* (35) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  197,   -4 }, /* (36) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  197,   -6 }, /* (37) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  199,   -1 }, /* (38) ids ::= ID */
  {  199,   -1 }, /* (39) ids ::= STRING */
  {  201,   -2 }, /* (40) ifexists ::= IF EXISTS */
  {  201,    0 }, /* (41) ifexists ::= */
  {  204,   -3 }, /* (42) ifnotexists ::= IF NOT EXISTS */
  {  204,    0 }, /* (43) ifnotexists ::= */
  {  197,   -3 }, /* (44) cmd ::= CREATE DNODE IPTOKEN */
  {  197,   -6 }, /* (45) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  197,   -5 }, /* (46) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  197,   -5 }, /* (47) cmd ::= CREATE USER ids PASS ids */
  {  206,    0 }, /* (48) pps ::= */
  {  206,   -2 }, /* (49) pps ::= PPS INTEGER */
  {  207,    0 }, /* (50) tseries ::= */
  {  207,   -2 }, /* (51) tseries ::= TSERIES INTEGER */
  {  208,    0 }, /* (52) dbs ::= */
  {  208,   -2 }, /* (53) dbs ::= DBS INTEGER */
  {  209,    0 }, /* (54) streams ::= */
  {  209,   -2 }, /* (55) streams ::= STREAMS INTEGER */
  {  210,    0 }, /* (56) storage ::= */
  {  210,   -2 }, /* (57) storage ::= STORAGE INTEGER */
  {  211,    0 }, /* (58) qtime ::= */
  {  211,   -2 }, /* (59) qtime ::= QTIME INTEGER */
  {  212,    0 }, /* (60) users ::= */
  {  212,   -2 }, /* (61) users ::= USERS INTEGER */
  {  213,    0 }, /* (62) conns ::= */
  {  213,   -2 }, /* (63) conns ::= CONNS INTEGER */
  {  214,    0 }, /* (64) state ::= */
  {  214,   -2 }, /* (65) state ::= STATE ids */
  {  203,   -9 }, /* (66) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  215,   -2 }, /* (67) keep ::= KEEP tagitemlist */
  {  217,   -2 }, /* (68) tables ::= TABLES INTEGER */
  {  218,   -2 }, /* (69) cache ::= CACHE INTEGER */
  {  219,   -2 }, /* (70) replica ::= REPLICA INTEGER */
  {  220,   -2 }, /* (71) days ::= DAYS INTEGER */
  {  221,   -2 }, /* (72) rows ::= ROWS INTEGER */
  {  222,   -2 }, /* (73) ablocks ::= ABLOCKS ID */
  {  223,   -2 }, /* (74) tblocks ::= TBLOCKS INTEGER */
  {  224,   -2 }, /* (75) ctime ::= CTIME INTEGER */
  {  225,   -2 }, /* (76) clog ::= CLOG INTEGER */
  {  226,   -2 }, /* (77) comp ::= COMP INTEGER */
  {  227,   -2 }, /* (78) prec ::= PRECISION STRING */
  {  205,    0 }, /* (79) db_optr ::= */
  {  205,   -2 }, /* (80) db_optr ::= db_optr tables */
  {  205,   -2 }, /* (81) db_optr ::= db_optr cache */
  {  205,   -2 }, /* (82) db_optr ::= db_optr replica */
  {  205,   -2 }, /* (83) db_optr ::= db_optr days */
  {  205,   -2 }, /* (84) db_optr ::= db_optr rows */
  {  205,   -2 }, /* (85) db_optr ::= db_optr ablocks */
  {  205,   -2 }, /* (86) db_optr ::= db_optr tblocks */
  {  205,   -2 }, /* (87) db_optr ::= db_optr ctime */
  {  205,   -2 }, /* (88) db_optr ::= db_optr clog */
  {  205,   -2 }, /* (89) db_optr ::= db_optr comp */
  {  205,   -2 }, /* (90) db_optr ::= db_optr prec */
  {  205,   -2 }, /* (91) db_optr ::= db_optr keep */
  {  202,    0 }, /* (92) alter_db_optr ::= */
  {  202,   -2 }, /* (93) alter_db_optr ::= alter_db_optr replica */
  {  202,   -2 }, /* (94) alter_db_optr ::= alter_db_optr tables */
  {  228,   -1 }, /* (95) typename ::= ids */
  {  228,   -4 }, /* (96) typename ::= ids LP signed RP */
  {  229,   -1 }, /* (97) signed ::= INTEGER */
  {  229,   -2 }, /* (98) signed ::= PLUS INTEGER */
  {  229,   -2 }, /* (99) signed ::= MINUS INTEGER */
  {  197,   -6 }, /* (100) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  230,   -3 }, /* (101) create_table_args ::= LP columnlist RP */
  {  230,   -7 }, /* (102) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  230,   -7 }, /* (103) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  230,   -2 }, /* (104) create_table_args ::= AS select */
  {  231,   -3 }, /* (105) columnlist ::= columnlist COMMA column */
  {  231,   -1 }, /* (106) columnlist ::= column */
  {  233,   -2 }, /* (107) column ::= ids typename */
  {  216,   -3 }, /* (108) tagitemlist ::= tagitemlist COMMA tagitem */
  {  216,   -1 }, /* (109) tagitemlist ::= tagitem */
  {  234,   -1 }, /* (110) tagitem ::= INTEGER */
  {  234,   -1 }, /* (111) tagitem ::= FLOAT */
  {  234,   -1 }, /* (112) tagitem ::= STRING */
  {  234,   -1 }, /* (113) tagitem ::= BOOL */
  {  234,   -1 }, /* (114) tagitem ::= NULL */
  {  234,   -2 }, /* (115) tagitem ::= MINUS INTEGER */
  {  234,   -2 }, /* (116) tagitem ::= MINUS FLOAT */
  {  234,   -2 }, /* (117) tagitem ::= PLUS INTEGER */
  {  234,   -2 }, /* (118) tagitem ::= PLUS FLOAT */
  {  197,   -1 }, /* (119) cmd ::= select */
  {  232,  -12 }, /* (120) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  232,   -2 }, /* (121) select ::= SELECT selcollist */
  {  246,   -2 }, /* (122) sclp ::= selcollist COMMA */
  {  246,    0 }, /* (123) sclp ::= */
  {  235,   -3 }, /* (124) selcollist ::= sclp expr as */
  {  235,   -2 }, /* (125) selcollist ::= sclp STAR */
  {  248,   -2 }, /* (126) as ::= AS ids */
  {  248,   -1 }, /* (127) as ::= ids */
  {  248,    0 }, /* (128) as ::= */
  {  236,   -2 }, /* (129) from ::= FROM tablelist */
  {  249,   -2 }, /* (130) tablelist ::= ids cpxName */
  {  249,   -4 }, /* (131) tablelist ::= tablelist COMMA ids cpxName */
  {  250,   -1 }, /* (132) tmvar ::= VARIABLE */
  {  238,   -4 }, /* (133) interval_opt ::= INTERVAL LP tmvar RP */
  {  238,    0 }, /* (134) interval_opt ::= */
  {  239,    0 }, /* (135) fill_opt ::= */
  {  239,   -6 }, /* (136) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  239,   -4 }, /* (137) fill_opt ::= FILL LP ID RP */
  {  240,   -4 }, /* (138) sliding_opt ::= SLIDING LP tmvar RP */
  {  240,    0 }, /* (139) sliding_opt ::= */
  {  242,    0 }, /* (140) orderby_opt ::= */
  {  242,   -3 }, /* (141) orderby_opt ::= ORDER BY sortlist */
  {  251,   -4 }, /* (142) sortlist ::= sortlist COMMA item sortorder */
  {  251,   -2 }, /* (143) sortlist ::= item sortorder */
  {  253,   -2 }, /* (144) item ::= ids cpxName */
  {  254,   -1 }, /* (145) sortorder ::= ASC */
  {  254,   -1 }, /* (146) sortorder ::= DESC */
  {  254,    0 }, /* (147) sortorder ::= */
  {  241,    0 }, /* (148) groupby_opt ::= */
  {  241,   -3 }, /* (149) groupby_opt ::= GROUP BY grouplist */
  {  255,   -3 }, /* (150) grouplist ::= grouplist COMMA item */
  {  255,   -1 }, /* (151) grouplist ::= item */
  {  243,    0 }, /* (152) having_opt ::= */
  {  243,   -2 }, /* (153) having_opt ::= HAVING expr */
  {  245,    0 }, /* (154) limit_opt ::= */
  {  245,   -2 }, /* (155) limit_opt ::= LIMIT signed */
  {  245,   -4 }, /* (156) limit_opt ::= LIMIT signed OFFSET signed */
  {  245,   -4 }, /* (157) limit_opt ::= LIMIT signed COMMA signed */
  {  244,    0 }, /* (158) slimit_opt ::= */
  {  244,   -2 }, /* (159) slimit_opt ::= SLIMIT signed */
  {  244,   -4 }, /* (160) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  244,   -4 }, /* (161) slimit_opt ::= SLIMIT signed COMMA signed */
  {  237,    0 }, /* (162) where_opt ::= */
  {  237,   -2 }, /* (163) where_opt ::= WHERE expr */
  {  247,   -3 }, /* (164) expr ::= LP expr RP */
  {  247,   -1 }, /* (165) expr ::= ID */
  {  247,   -3 }, /* (166) expr ::= ID DOT ID */
  {  247,   -3 }, /* (167) expr ::= ID DOT STAR */
  {  247,   -1 }, /* (168) expr ::= INTEGER */
  {  247,   -2 }, /* (169) expr ::= MINUS INTEGER */
  {  247,   -2 }, /* (170) expr ::= PLUS INTEGER */
  {  247,   -1 }, /* (171) expr ::= FLOAT */
  {  247,   -2 }, /* (172) expr ::= MINUS FLOAT */
  {  247,   -2 }, /* (173) expr ::= PLUS FLOAT */
  {  247,   -1 }, /* (174) expr ::= STRING */
  {  247,   -1 }, /* (175) expr ::= NOW */
  {  247,   -1 }, /* (176) expr ::= VARIABLE */
  {  247,   -1 }, /* (177) expr ::= BOOL */
  {  247,   -4 }, /* (178) expr ::= ID LP exprlist RP */
  {  247,   -4 }, /* (179) expr ::= ID LP STAR RP */
  {  247,   -3 }, /* (180) expr ::= expr AND expr */
  {  247,   -3 }, /* (181) expr ::= expr OR expr */
  {  247,   -3 }, /* (182) expr ::= expr LT expr */
  {  247,   -3 }, /* (183) expr ::= expr GT expr */
  {  247,   -3 }, /* (184) expr ::= expr LE expr */
  {  247,   -3 }, /* (185) expr ::= expr GE expr */
  {  247,   -3 }, /* (186) expr ::= expr NE expr */
  {  247,   -3 }, /* (187) expr ::= expr EQ expr */
  {  247,   -3 }, /* (188) expr ::= expr PLUS expr */
  {  247,   -3 }, /* (189) expr ::= expr MINUS expr */
  {  247,   -3 }, /* (190) expr ::= expr STAR expr */
  {  247,   -3 }, /* (191) expr ::= expr SLASH expr */
  {  247,   -3 }, /* (192) expr ::= expr REM expr */
  {  247,   -3 }, /* (193) expr ::= expr LIKE expr */
  {  247,   -5 }, /* (194) expr ::= expr IN LP exprlist RP */
  {  256,   -3 }, /* (195) exprlist ::= exprlist COMMA expritem */
  {  256,   -1 }, /* (196) exprlist ::= expritem */
  {  257,   -1 }, /* (197) expritem ::= expr */
  {  257,    0 }, /* (198) expritem ::= */
  {  197,   -4 }, /* (199) cmd ::= INSERT INTO cpxName insert_value_list */
  {  258,   -4 }, /* (200) insert_value_list ::= VALUES LP itemlist RP */
  {  258,   -5 }, /* (201) insert_value_list ::= insert_value_list VALUES LP itemlist RP */
  {  259,   -3 }, /* (202) itemlist ::= itemlist COMMA expr */
  {  259,   -1 }, /* (203) itemlist ::= expr */
  {  197,   -3 }, /* (204) cmd ::= RESET QUERY CACHE */
  {  197,   -7 }, /* (205) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  197,   -7 }, /* (206) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  197,   -7 }, /* (207) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  197,   -7 }, /* (208) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  197,   -8 }, /* (209) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  197,   -9 }, /* (210) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  197,   -5 }, /* (211) cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
  {  197,   -7 }, /* (212) cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
  {  197,   -7 }, /* (213) cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
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
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 14: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 15: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 16: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
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
      case 24: /* cmd ::= DROP DNODE IPTOKEN */
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
      case 31: /* cmd ::= ALTER DNODE IPTOKEN ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 32: /* cmd ::= ALTER DNODE IPTOKEN ids ids */
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
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 40: /* ifexists ::= IF EXISTS */
{yymsp[-1].minor.yy0.n = 1;}
        break;
      case 42: /* ifnotexists ::= IF NOT EXISTS */
{yymsp[-2].minor.yy0.n = 1;}
        break;
      case 44: /* cmd ::= CREATE DNODE IPTOKEN */
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
{yymsp[1].minor.yy0.n = 0;   }
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
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 66: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy279.users   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy279.dbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy279.tseries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy279.streams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy279.pps     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy279.storage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy279.qtime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy279.conns   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy279.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy279 = yylhsminor.yy279;
        break;
      case 67: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy56 = yymsp[0].minor.yy56; }
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
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 79: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy398);}
        break;
      case 80: /* db_optr ::= db_optr tables */
      case 94: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==94);
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 81: /* db_optr ::= db_optr cache */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 82: /* db_optr ::= db_optr replica */
      case 93: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==93);
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 83: /* db_optr ::= db_optr days */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 84: /* db_optr ::= db_optr rows */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 85: /* db_optr ::= db_optr ablocks */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 86: /* db_optr ::= db_optr tblocks */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 87: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 88: /* db_optr ::= db_optr clog */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 89: /* db_optr ::= db_optr comp */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 90: /* db_optr ::= db_optr prec */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 91: /* db_optr ::= db_optr keep */
{ yylhsminor.yy398 = yymsp[-1].minor.yy398; yylhsminor.yy398.keep = yymsp[0].minor.yy56; }
  yymsp[-1].minor.yy398 = yylhsminor.yy398;
        break;
      case 92: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy398);}
        break;
      case 95: /* typename ::= ids */
{ tSQLSetColumnType (&yylhsminor.yy223, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy223 = yylhsminor.yy223;
        break;
      case 96: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy389;          // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
}
  yymsp[-3].minor.yy223 = yylhsminor.yy223;
        break;
      case 97: /* signed ::= INTEGER */
{ yylhsminor.yy389 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy389 = yylhsminor.yy389;
        break;
      case 98: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy389 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 99: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy389 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 100: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedMeterName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 101: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy158 = tSetCreateSQLElems(yymsp[-1].minor.yy471, NULL, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METER);
    setSQLInfo(pInfo, yymsp[-2].minor.yy158, NULL, TSQL_CREATE_NORMAL_METER);
}
        break;
      case 102: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy158 = tSetCreateSQLElems(yymsp[-5].minor.yy471, yymsp[-1].minor.yy471, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METRIC);
    setSQLInfo(pInfo, yymsp[-6].minor.yy158, NULL, TSQL_CREATE_NORMAL_METRIC);
}
        break;
      case 103: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy158 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy56, NULL, TSQL_CREATE_METER_FROM_METRIC);
    setSQLInfo(pInfo, yymsp[-6].minor.yy158, NULL, TSQL_CREATE_METER_FROM_METRIC);
}
        break;
      case 104: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy158 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy24, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy158, NULL, TSQL_CREATE_STREAM);
}
        break;
      case 105: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy471 = tFieldListAppend(yymsp[-2].minor.yy471, &yymsp[0].minor.yy223);   }
  yymsp[-2].minor.yy471 = yylhsminor.yy471;
        break;
      case 106: /* columnlist ::= column */
{yylhsminor.yy471 = tFieldListAppend(NULL, &yymsp[0].minor.yy223);}
  yymsp[0].minor.yy471 = yylhsminor.yy471;
        break;
      case 107: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 108: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy56 = tVariantListAppend(yymsp[-2].minor.yy56, &yymsp[0].minor.yy186, -1);    }
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 109: /* tagitemlist ::= tagitem */
{ yylhsminor.yy56 = tVariantListAppend(NULL, &yymsp[0].minor.yy186, -1); }
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 110: /* tagitem ::= INTEGER */
      case 111: /* tagitem ::= FLOAT */ yytestcase(yyruleno==111);
      case 112: /* tagitem ::= STRING */ yytestcase(yyruleno==112);
      case 113: /* tagitem ::= BOOL */ yytestcase(yyruleno==113);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy186, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy186 = yylhsminor.yy186;
        break;
      case 114: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy186, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy186 = yylhsminor.yy186;
        break;
      case 115: /* tagitem ::= MINUS INTEGER */
      case 116: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==116);
      case 117: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==117);
      case 118: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==118);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy186, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy186 = yylhsminor.yy186;
        break;
      case 119: /* cmd ::= select */
{
    setSQLInfo(pInfo, yymsp[0].minor.yy24, NULL, TSQL_QUERY_METER);
}
        break;
      case 120: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy24 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy498, yymsp[-9].minor.yy56, yymsp[-8].minor.yy90, yymsp[-4].minor.yy56, yymsp[-3].minor.yy56, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy56, &yymsp[0].minor.yy294, &yymsp[-1].minor.yy294);
}
  yymsp[-11].minor.yy24 = yylhsminor.yy24;
        break;
      case 121: /* select ::= SELECT selcollist */
{
  yylhsminor.yy24 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy498, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy24 = yylhsminor.yy24;
        break;
      case 122: /* sclp ::= selcollist COMMA */
{yylhsminor.yy498 = yymsp[-1].minor.yy498;}
  yymsp[-1].minor.yy498 = yylhsminor.yy498;
        break;
      case 123: /* sclp ::= */
{yymsp[1].minor.yy498 = 0;}
        break;
      case 124: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy498 = tSQLExprListAppend(yymsp[-2].minor.yy498, yymsp[-1].minor.yy90, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy498 = yylhsminor.yy498;
        break;
      case 125: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy498 = tSQLExprListAppend(yymsp[-1].minor.yy498, pNode, 0);
}
  yymsp[-1].minor.yy498 = yylhsminor.yy498;
        break;
      case 126: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 127: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 128: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 129: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy56 = yymsp[0].minor.yy56;}
        break;
      case 130: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy56 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 131: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy56 = tVariantListAppendToken(yymsp[-3].minor.yy56, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 132: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 133: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 138: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==138);
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 134: /* interval_opt ::= */
      case 139: /* sliding_opt ::= */ yytestcase(yyruleno==139);
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 135: /* fill_opt ::= */
{yymsp[1].minor.yy56 = 0;     }
        break;
      case 136: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy56, &A, -1, 0);
    yymsp[-5].minor.yy56 = yymsp[-1].minor.yy56;
}
        break;
      case 137: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy56 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 140: /* orderby_opt ::= */
      case 148: /* groupby_opt ::= */ yytestcase(yyruleno==148);
{yymsp[1].minor.yy56 = 0;}
        break;
      case 141: /* orderby_opt ::= ORDER BY sortlist */
      case 149: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==149);
{yymsp[-2].minor.yy56 = yymsp[0].minor.yy56;}
        break;
      case 142: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy56 = tVariantListAppend(yymsp[-3].minor.yy56, &yymsp[-1].minor.yy186, yymsp[0].minor.yy332);
}
  yymsp[-3].minor.yy56 = yylhsminor.yy56;
        break;
      case 143: /* sortlist ::= item sortorder */
{
  yylhsminor.yy56 = tVariantListAppend(NULL, &yymsp[-1].minor.yy186, yymsp[0].minor.yy332);
}
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 144: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy186, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy186 = yylhsminor.yy186;
        break;
      case 145: /* sortorder ::= ASC */
{yymsp[0].minor.yy332 = TSQL_SO_ASC; }
        break;
      case 146: /* sortorder ::= DESC */
{yymsp[0].minor.yy332 = TSQL_SO_DESC;}
        break;
      case 147: /* sortorder ::= */
{yymsp[1].minor.yy332 = TSQL_SO_ASC;}
        break;
      case 150: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy56 = tVariantListAppend(yymsp[-2].minor.yy56, &yymsp[0].minor.yy186, -1);
}
  yymsp[-2].minor.yy56 = yylhsminor.yy56;
        break;
      case 151: /* grouplist ::= item */
{
  yylhsminor.yy56 = tVariantListAppend(NULL, &yymsp[0].minor.yy186, -1);
}
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 152: /* having_opt ::= */
      case 162: /* where_opt ::= */ yytestcase(yyruleno==162);
      case 198: /* expritem ::= */ yytestcase(yyruleno==198);
{yymsp[1].minor.yy90 = 0;}
        break;
      case 153: /* having_opt ::= HAVING expr */
      case 163: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==163);
{yymsp[-1].minor.yy90 = yymsp[0].minor.yy90;}
        break;
      case 154: /* limit_opt ::= */
      case 158: /* slimit_opt ::= */ yytestcase(yyruleno==158);
{yymsp[1].minor.yy294.limit = -1; yymsp[1].minor.yy294.offset = 0;}
        break;
      case 155: /* limit_opt ::= LIMIT signed */
      case 159: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==159);
{yymsp[-1].minor.yy294.limit = yymsp[0].minor.yy389;  yymsp[-1].minor.yy294.offset = 0;}
        break;
      case 156: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 160: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==160);
{yymsp[-3].minor.yy294.limit = yymsp[-2].minor.yy389;  yymsp[-3].minor.yy294.offset = yymsp[0].minor.yy389;}
        break;
      case 157: /* limit_opt ::= LIMIT signed COMMA signed */
      case 161: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==161);
{yymsp[-3].minor.yy294.limit = yymsp[0].minor.yy389;  yymsp[-3].minor.yy294.offset = yymsp[-2].minor.yy389;}
        break;
      case 164: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy90 = yymsp[-1].minor.yy90; }
        break;
      case 165: /* expr ::= ID */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 166: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 167: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 168: /* expr ::= INTEGER */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 169: /* expr ::= MINUS INTEGER */
      case 170: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==170);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 171: /* expr ::= FLOAT */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 172: /* expr ::= MINUS FLOAT */
      case 173: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==173);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy90 = yylhsminor.yy90;
        break;
      case 174: /* expr ::= STRING */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 175: /* expr ::= NOW */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 176: /* expr ::= VARIABLE */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 177: /* expr ::= BOOL */
{yylhsminor.yy90 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 178: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy90 = tSQLExprCreateFunction(yymsp[-1].minor.yy498, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy90 = yylhsminor.yy90;
        break;
      case 179: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy90 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy90 = yylhsminor.yy90;
        break;
      case 180: /* expr ::= expr AND expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_AND);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 181: /* expr ::= expr OR expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_OR); }
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 182: /* expr ::= expr LT expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_LT);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 183: /* expr ::= expr GT expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_GT);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 184: /* expr ::= expr LE expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_LE);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 185: /* expr ::= expr GE expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_GE);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 186: /* expr ::= expr NE expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_NE);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 187: /* expr ::= expr EQ expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_EQ);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 188: /* expr ::= expr PLUS expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_PLUS);  }
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 189: /* expr ::= expr MINUS expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_MINUS); }
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 190: /* expr ::= expr STAR expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_STAR);  }
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 191: /* expr ::= expr SLASH expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_DIVIDE);}
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 192: /* expr ::= expr REM expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_REM);   }
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 193: /* expr ::= expr LIKE expr */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-2].minor.yy90, yymsp[0].minor.yy90, TK_LIKE);  }
  yymsp[-2].minor.yy90 = yylhsminor.yy90;
        break;
      case 194: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy90 = tSQLExprCreate(yymsp[-4].minor.yy90, (tSQLExpr*)yymsp[-1].minor.yy498, TK_IN); }
  yymsp[-4].minor.yy90 = yylhsminor.yy90;
        break;
      case 195: /* exprlist ::= exprlist COMMA expritem */
      case 202: /* itemlist ::= itemlist COMMA expr */ yytestcase(yyruleno==202);
{yylhsminor.yy498 = tSQLExprListAppend(yymsp[-2].minor.yy498,yymsp[0].minor.yy90,0);}
  yymsp[-2].minor.yy498 = yylhsminor.yy498;
        break;
      case 196: /* exprlist ::= expritem */
      case 203: /* itemlist ::= expr */ yytestcase(yyruleno==203);
{yylhsminor.yy498 = tSQLExprListAppend(0,yymsp[0].minor.yy90,0);}
  yymsp[0].minor.yy498 = yylhsminor.yy498;
        break;
      case 197: /* expritem ::= expr */
{yylhsminor.yy90 = yymsp[0].minor.yy90;}
  yymsp[0].minor.yy90 = yylhsminor.yy90;
        break;
      case 199: /* cmd ::= INSERT INTO cpxName insert_value_list */
{
    tSetInsertSQLElems(pInfo, &yymsp[-1].minor.yy0, yymsp[0].minor.yy74);
}
        break;
      case 200: /* insert_value_list ::= VALUES LP itemlist RP */
{yymsp[-3].minor.yy74 = tSQLListListAppend(NULL, yymsp[-1].minor.yy498);}
        break;
      case 201: /* insert_value_list ::= insert_value_list VALUES LP itemlist RP */
{yylhsminor.yy74 = tSQLListListAppend(yymsp[-4].minor.yy74, yymsp[-1].minor.yy498);}
  yymsp[-4].minor.yy74 = yylhsminor.yy74;
        break;
      case 204: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, RESET_QUERY_CACHE, 0);}
        break;
      case 205: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy471, NULL, ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_ADD_COLUMN);
}
        break;
      case 206: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_DROP_COLUMN);
}
        break;
      case 207: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy471, NULL, ALTER_TABLE_TAGS_ADD);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_ADD);
}
        break;
      case 208: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, ALTER_TABLE_TAGS_DROP);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_DROP);
}
        break;
      case 209: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 210: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy186, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, ALTER_TABLE_TAGS_SET);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_SET);
}
        break;
      case 211: /* cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_CONNECTION, 1, &yymsp[-2].minor.yy0);}
        break;
      case 212: /* cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_STREAM, 1, &yymsp[-4].minor.yy0);}
        break;
      case 213: /* cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_QUERY, 1, &yymsp[-4].minor.yy0);}
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
