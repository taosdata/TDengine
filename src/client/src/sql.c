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
#define YYNOCODE 262
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSubclauseInfo* yy117;
  SQuerySQL* yy138;
  SCreateAcctSQL yy155;
  SLimitVal yy162;
  int yy220;
  tVariant yy236;
  tSQLExpr* yy244;
  SCreateDBInfo yy262;
  tSQLExprList* yy284;
  SCreateTableSQL* yy344;
  int64_t yy369;
  TAOS_FIELD yy397;
  tFieldList* yy421;
  tVariantList* yy480;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             247
#define YYNRULE              216
#define YYNTOKEN             197
#define YY_MAX_SHIFT         246
#define YY_MIN_SHIFTREDUCE   399
#define YY_MAX_SHIFTREDUCE   614
#define YY_ERROR_ACTION      615
#define YY_ACCEPT_ACTION     616
#define YY_NO_ACTION         617
#define YY_MIN_REDUCE        618
#define YY_MAX_REDUCE        833
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
#define YY_ACTTAB_COUNT (529)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   752,  440,  132,  150,  244,   10,  616,  246,  132,  441,
 /*    10 */   132,  155,  821,   41,   43,   20,   35,   36,  820,  154,
 /*    20 */   821,   29,  741,  440,  200,   39,   37,   40,   38,  131,
 /*    30 */   499,  441,   96,   34,   33,  100,  151,   32,   31,   30,
 /*    40 */    41,   43,  741,   35,   36,  152,  136,  163,   29,  727,
 /*    50 */   749,  200,   39,   37,   40,   38,  185,  100,  225,  224,
 /*    60 */    34,   33,  162,  730,   32,   31,   30,  400,  401,  402,
 /*    70 */   403,  404,  405,  406,  407,  408,  409,  410,  411,  245,
 /*    80 */   730,   41,   43,  188,   35,   36,  215,  817,  197,   29,
 /*    90 */    58,   20,  200,   39,   37,   40,   38,   32,   31,   30,
 /*   100 */    56,   34,   33,   75,  730,   32,   31,   30,   43,  236,
 /*   110 */    35,   36,  776,  236,  195,   29,   20,   20,  200,   39,
 /*   120 */    37,   40,   38,  164,  570,  727,  227,   34,   33,  440,
 /*   130 */   167,   32,   31,   30,  238,   35,   36,  441,    7,  816,
 /*   140 */    29,   61,  110,  200,   39,   37,   40,   38,  223,  228,
 /*   150 */   727,  727,   34,   33,   50,  728,   32,   31,   30,   15,
 /*   160 */   214,  237,  213,  212,  211,  210,  209,  208,  207,  206,
 /*   170 */   712,   51,  701,  702,  703,  704,  705,  706,  707,  708,
 /*   180 */   709,  710,  711,  159,  583,   11,  815,  574,  100,  577,
 /*   190 */   100,  580,  168,  159,  583,  222,  221,  574,   16,  577,
 /*   200 */    20,  580,   34,   33,  145,   26,   32,   31,   30,  238,
 /*   210 */    86,   85,  139,  174,  657,  156,  157,  123,  144,  199,
 /*   220 */   182,  715,  179,  714,  148,  156,  157,  159,  583,  531,
 /*   230 */    60,  574,  149,  577,  726,  580,  237,   16,   39,   37,
 /*   240 */    40,   38,   27,  775,   26,   59,   34,   33,  551,  552,
 /*   250 */    32,   31,   30,  137,  113,  114,  219,   64,   67,  156,
 /*   260 */   157,   95,  515,  666,  184,  512,  123,  513,   26,  514,
 /*   270 */   523,  147,  127,  125,  240,   88,   87,  187,   42,  158,
 /*   280 */    73,   77,  239,   84,   76,  572,  528,  138,   42,  582,
 /*   290 */    79,   17,  658,  165,  166,  123,  243,  242,   92,  582,
 /*   300 */    47,  542,  543,  600,  581,   45,   13,   12,  584,  576,
 /*   310 */   786,  579,   12,  575,  581,  578,    2,   72,   71,   48,
 /*   320 */   505,  573,   42,  140,   45,  504,  204,    9,    8,   21,
 /*   330 */    21,  141,  519,  582,  520,  517,  142,  518,   83,   82,
 /*   340 */   143,  134,  130,  135,  729,  133,  830,  785,  581,  160,
 /*   350 */   109,  782,  781,  161,  751,  721,  226,   97,  743,  111,
 /*   360 */   768,  767,  516,  112,  668,  205,   26,  128,   24,  218,
 /*   370 */   186,  220,  829,   69,   90,  828,  826,  115,  686,   25,
 /*   380 */    22,  129,  655,  538,   78,  653,  189,   80,  651,  650,
 /*   390 */   169,  193,   52,  740,  124,  648,  647,  646,  644,  636,
 /*   400 */    49,  126,   44,  642,  640,  101,  102,  198,  638,  196,
 /*   410 */   194,  755,  756,  190,  769,  192,   28,  217,   74,  229,
 /*   420 */   230,  231,  232,  233,  202,  234,  235,   53,  241,  614,
 /*   430 */   170,  171,  173,   62,  146,  172,   65,  613,  176,  175,
 /*   440 */   649,  177,   89,  643,  118,  178,  612,  687,  116,  117,
 /*   450 */   119,  120,  122,  725,  121,   91,    1,  105,  103,  106,
 /*   460 */   104,  107,   23,  108,  180,  181,  605,  183,  187,  525,
 /*   470 */    55,  153,  539,   98,   57,  191,  201,   18,  544,   99,
 /*   480 */     4,    5,  585,   19,    3,   14,    6,   63,  480,  203,
 /*   490 */   479,  478,  477,  476,  475,  474,  473,  471,   45,  444,
 /*   500 */    66,  446,   21,  501,  216,  500,  498,   54,  465,  463,
 /*   510 */    46,   68,  455,   70,  461,  457,  459,  453,  451,  472,
 /*   520 */   470,   81,  426,  442,  415,  413,   93,   94,  618,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   201,    1,  250,  200,  201,  250,  198,  199,  250,    9,
 /*    10 */   250,  259,  260,   13,   14,  201,   16,   17,  260,  259,
 /*    20 */   260,   21,  234,    1,   24,   25,   26,   27,   28,  250,
 /*    30 */     5,    9,  201,   33,   34,  201,  248,   37,   38,   39,
 /*    40 */    13,   14,  234,   16,   17,  218,  250,  233,   21,  235,
 /*    50 */   251,   24,   25,   26,   27,   28,  248,  201,   33,   34,
 /*    60 */    33,   34,  218,  236,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */   236,   13,   14,  252,   16,   17,  218,  250,  254,   21,
 /*    90 */   256,  201,   24,   25,   26,   27,   28,   37,   38,   39,
 /*   100 */   100,   33,   34,   72,  236,   37,   38,   39,   14,   78,
 /*   110 */    16,   17,  256,   78,  258,   21,  201,  201,   24,   25,
 /*   120 */    26,   27,   28,  233,   97,  235,  201,   33,   34,    1,
 /*   130 */    63,   37,   38,   39,   60,   16,   17,    9,   96,  250,
 /*   140 */    21,   99,  100,   24,   25,   26,   27,   28,  233,  233,
 /*   150 */   235,  235,   33,   34,  101,  230,   37,   38,   39,   85,
 /*   160 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   170 */   217,  118,  219,  220,  221,  222,  223,  224,  225,  226,
 /*   180 */   227,  228,  229,    1,    2,   44,  250,    5,  201,    7,
 /*   190 */   201,    9,  125,    1,    2,  128,  129,    5,   96,    7,
 /*   200 */   201,    9,   33,   34,   63,  103,   37,   38,   39,   60,
 /*   210 */    69,   70,   71,  124,  205,   33,   34,  208,   77,   37,
 /*   220 */   131,  219,  133,  221,  250,   33,   34,    1,    2,   37,
 /*   230 */   237,    5,  250,    7,  235,    9,   87,   96,   25,   26,
 /*   240 */    27,   28,  249,  256,  103,  256,   33,   34,  113,  114,
 /*   250 */    37,   38,   39,  250,   64,   65,   66,   67,   68,   33,
 /*   260 */    34,   96,    2,  205,  123,    5,  208,    7,  103,    9,
 /*   270 */    97,  130,   64,   65,   66,   67,   68,  104,   96,   59,
 /*   280 */    64,   65,   66,   67,   68,    1,  101,  250,   96,  107,
 /*   290 */    74,  106,  205,   33,   34,  208,   60,   61,   62,  107,
 /*   300 */   101,   97,   97,   97,  122,  101,  101,  101,   97,    5,
 /*   310 */   231,    7,  101,    5,  122,    7,   96,  126,  127,  120,
 /*   320 */    97,   37,   96,  250,  101,   97,   97,  126,  127,  101,
 /*   330 */   101,  250,    5,  107,    7,    5,  250,    7,   72,   73,
 /*   340 */   250,  250,  250,  250,  236,  250,  236,  231,  122,  231,
 /*   350 */   238,  231,  231,  231,  201,  232,  231,  201,  234,  201,
 /*   360 */   257,  257,  102,  201,  201,  201,  103,  201,  201,  201,
 /*   370 */   234,  201,  201,  201,   59,  201,  201,  201,  201,  201,
 /*   380 */   201,  201,  201,  107,  201,  201,  253,  201,  201,  201,
 /*   390 */   201,  253,  117,  247,  201,  201,  201,  201,  201,  201,
 /*   400 */   119,  201,  116,  201,  201,  246,  245,  111,  201,  115,
 /*   410 */   110,  202,  202,  108,  202,  109,  121,   75,   84,   83,
 /*   420 */    49,   80,   82,   53,  202,   81,   79,  202,   75,    5,
 /*   430 */   132,    5,   58,  206,  202,  132,  206,    5,    5,  132,
 /*   440 */   202,  132,  203,  202,  210,   58,    5,  216,  215,  214,
 /*   450 */   213,  211,  209,  234,  212,  203,  207,  242,  244,  241,
 /*   460 */   243,  240,  204,  239,  132,   58,   86,  124,  104,   97,
 /*   470 */   105,    1,   97,   96,  101,   96,   98,  101,   97,   96,
 /*   480 */   112,  112,   97,  101,   96,   96,   96,   72,    9,   98,
 /*   490 */     5,    5,    5,    5,    1,    5,    5,    5,  101,   76,
 /*   500 */    72,   58,  101,    5,   15,    5,   97,   96,    5,    5,
 /*   510 */    16,  127,    5,  127,    5,    5,    5,    5,    5,    5,
 /*   520 */     5,   58,   58,   76,   59,   58,   21,   21,    0,  261,
 /*   530 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   540 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   550 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   560 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   570 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   580 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   590 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   600 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   610 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   620 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   630 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   640 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   650 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   660 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   670 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   680 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   690 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   700 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   710 */   261,  261,  261,  261,  261,  261,  261,  261,  261,  261,
 /*   720 */   261,  261,  261,  261,  261,  261,
};
#define YY_SHIFT_COUNT    (246)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (528)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   141,   74,  182,  226,  128,  128,  128,  128,  128,  128,
 /*    10 */     0,   22,  226,  260,  260,  260,  102,  128,  128,  128,
 /*    20 */   128,  128,   31,  149,   35,   35,  529,  192,  226,  226,
 /*    30 */   226,  226,  226,  226,  226,  226,  226,  226,  226,  226,
 /*    40 */   226,  226,  226,  226,  226,  260,  260,   25,   25,   25,
 /*    50 */    25,   25,   25,   42,   25,  165,  128,  128,  135,  135,
 /*    60 */   185,  128,  128,  128,  128,  128,  128,  128,  128,  128,
 /*    70 */   128,  128,  128,  128,  128,  128,  128,  128,  128,  128,
 /*    80 */   128,  128,  128,  128,  128,  128,  128,  128,  128,  128,
 /*    90 */   128,  128,  128,  128,  128,  263,  315,  315,  276,  276,
 /*   100 */   315,  275,  281,  286,  296,  294,  300,  306,  305,  295,
 /*   110 */   263,  315,  315,  342,  342,  315,  334,  336,  371,  341,
 /*   120 */   340,  370,  344,  347,  315,  353,  315,  353,  529,  529,
 /*   130 */    27,   68,   68,   68,   94,  119,  213,  213,  213,  216,
 /*   140 */   169,  169,  169,  169,  190,  208,   67,   89,   60,   60,
 /*   150 */   236,  173,  204,  205,  206,  211,  304,  308,  284,  220,
 /*   160 */   199,   53,  223,  228,  229,  327,  330,  191,  201,  266,
 /*   170 */   424,  298,  426,  303,  374,  432,  307,  433,  309,  387,
 /*   180 */   441,  332,  407,  380,  343,  364,  372,  365,  373,  375,
 /*   190 */   377,  470,  379,  381,  383,  376,  368,  382,  369,  385,
 /*   200 */   388,  389,  378,  390,  391,  415,  479,  485,  486,  487,
 /*   210 */   488,  493,  490,  491,  492,  397,  423,  489,  428,  443,
 /*   220 */   494,  384,  386,  401,  498,  500,  409,  411,  401,  503,
 /*   230 */   504,  507,  509,  510,  511,  512,  513,  514,  515,  463,
 /*   240 */   464,  447,  505,  506,  465,  467,  528,
};
#define YY_REDUCE_COUNT (129)
#define YY_REDUCE_MIN   (-248)
#define YY_REDUCE_MAX   (258)
static const short yy_reduce_ofst[] = {
 /*     0 */  -192,  -47, -248, -240, -144, -166, -186, -110,  -85,  -84,
 /*    10 */  -201, -197, -242, -173, -156, -132, -212, -169,  -13,  -11,
 /*    20 */   -75,   -1,    9,    2,   58,   87,   -7, -245, -221, -204,
 /*    30 */  -163, -111,  -64,  -26,  -18,    3,   37,   73,   81,   86,
 /*    40 */    90,   91,   92,   93,   95,  108,  110,   79,  116,  118,
 /*    50 */   120,  121,  122,  123,  125,  124,  153,  156,  103,  104,
 /*    60 */   112,  158,  162,  163,  164,  166,  167,  168,  170,  171,
 /*    70 */   172,  174,  175,  176,  177,  178,  179,  180,  181,  183,
 /*    80 */   184,  186,  187,  188,  189,  193,  194,  195,  196,  197,
 /*    90 */   198,  200,  202,  203,  207,  136,  209,  210,  133,  138,
 /*   100 */   212,  146,  159,  161,  214,  217,  215,  218,  221,  224,
 /*   110 */   219,  222,  225,  227,  230,  232,  231,  233,  235,  234,
 /*   120 */   237,  240,  242,  243,  238,  239,  241,  252,  249,  258,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   615,  667,  823,  823,  615,  615,  615,  615,  615,  615,
 /*    10 */   753,  633,  823,  615,  615,  615,  615,  615,  615,  615,
 /*    20 */   615,  615,  669,  656,  669,  669,  748,  615,  615,  615,
 /*    30 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    40 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    50 */   615,  615,  615,  615,  615,  615,  615,  615,  772,  772,
 /*    60 */   746,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    70 */   615,  615,  615,  615,  615,  615,  615,  615,  654,  615,
 /*    80 */   652,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    90 */   615,  615,  641,  615,  615,  615,  635,  635,  615,  615,
 /*   100 */   635,  779,  783,  777,  765,  773,  764,  760,  759,  787,
 /*   110 */   615,  635,  635,  664,  664,  635,  685,  683,  681,  673,
 /*   120 */   679,  675,  677,  671,  635,  662,  635,  662,  700,  713,
 /*   130 */   615,  788,  822,  778,  806,  805,  818,  812,  811,  615,
 /*   140 */   810,  809,  808,  807,  615,  615,  615,  615,  814,  813,
 /*   150 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  790,
 /*   160 */   784,  780,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   170 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   180 */   615,  615,  615,  615,  615,  745,  615,  615,  754,  615,
 /*   190 */   615,  615,  615,  615,  615,  774,  615,  766,  615,  615,
 /*   200 */   615,  615,  615,  615,  722,  615,  615,  615,  615,  615,
 /*   210 */   615,  615,  615,  615,  615,  688,  615,  615,  615,  615,
 /*   220 */   615,  615,  615,  827,  615,  615,  615,  716,  825,  615,
 /*   230 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   240 */   615,  615,  639,  637,  615,  631,  615,
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
  /*   57 */ "VNODES",
  /*   58 */ "IPTOKEN",
  /*   59 */ "DOT",
  /*   60 */ "TABLES",
  /*   61 */ "STABLES",
  /*   62 */ "VGROUPS",
  /*   63 */ "DROP",
  /*   64 */ "TABLE",
  /*   65 */ "DATABASE",
  /*   66 */ "DNODE",
  /*   67 */ "USER",
  /*   68 */ "ACCOUNT",
  /*   69 */ "USE",
  /*   70 */ "DESCRIBE",
  /*   71 */ "ALTER",
  /*   72 */ "PASS",
  /*   73 */ "PRIVILEGE",
  /*   74 */ "LOCAL",
  /*   75 */ "IF",
  /*   76 */ "EXISTS",
  /*   77 */ "CREATE",
  /*   78 */ "PPS",
  /*   79 */ "TSERIES",
  /*   80 */ "DBS",
  /*   81 */ "STORAGE",
  /*   82 */ "QTIME",
  /*   83 */ "CONNS",
  /*   84 */ "STATE",
  /*   85 */ "KEEP",
  /*   86 */ "CACHE",
  /*   87 */ "REPLICA",
  /*   88 */ "DAYS",
  /*   89 */ "ROWS",
  /*   90 */ "ABLOCKS",
  /*   91 */ "TBLOCKS",
  /*   92 */ "CTIME",
  /*   93 */ "CLOG",
  /*   94 */ "COMP",
  /*   95 */ "PRECISION",
  /*   96 */ "LP",
  /*   97 */ "RP",
  /*   98 */ "TAGS",
  /*   99 */ "USING",
  /*  100 */ "AS",
  /*  101 */ "COMMA",
  /*  102 */ "NULL",
  /*  103 */ "SELECT",
  /*  104 */ "UNION",
  /*  105 */ "ALL",
  /*  106 */ "FROM",
  /*  107 */ "VARIABLE",
  /*  108 */ "INTERVAL",
  /*  109 */ "FILL",
  /*  110 */ "SLIDING",
  /*  111 */ "ORDER",
  /*  112 */ "BY",
  /*  113 */ "ASC",
  /*  114 */ "DESC",
  /*  115 */ "GROUP",
  /*  116 */ "HAVING",
  /*  117 */ "LIMIT",
  /*  118 */ "OFFSET",
  /*  119 */ "SLIMIT",
  /*  120 */ "SOFFSET",
  /*  121 */ "WHERE",
  /*  122 */ "NOW",
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
  /*  165 */ "COUNT",
  /*  166 */ "SUM",
  /*  167 */ "AVG",
  /*  168 */ "MIN",
  /*  169 */ "MAX",
  /*  170 */ "FIRST",
  /*  171 */ "LAST",
  /*  172 */ "TOP",
  /*  173 */ "BOTTOM",
  /*  174 */ "STDDEV",
  /*  175 */ "PERCENTILE",
  /*  176 */ "APERCENTILE",
  /*  177 */ "LEASTSQUARES",
  /*  178 */ "HISTOGRAM",
  /*  179 */ "DIFF",
  /*  180 */ "SPREAD",
  /*  181 */ "TWA",
  /*  182 */ "INTERP",
  /*  183 */ "LAST_ROW",
  /*  184 */ "SEMI",
  /*  185 */ "NONE",
  /*  186 */ "PREV",
  /*  187 */ "LINEAR",
  /*  188 */ "IMPORT",
  /*  189 */ "METRIC",
  /*  190 */ "TBNAME",
  /*  191 */ "JOIN",
  /*  192 */ "METRICS",
  /*  193 */ "STABLE",
  /*  194 */ "INSERT",
  /*  195 */ "INTO",
  /*  196 */ "VALUES",
  /*  197 */ "error",
  /*  198 */ "program",
  /*  199 */ "cmd",
  /*  200 */ "dbPrefix",
  /*  201 */ "ids",
  /*  202 */ "cpxName",
  /*  203 */ "ifexists",
  /*  204 */ "alter_db_optr",
  /*  205 */ "acct_optr",
  /*  206 */ "ifnotexists",
  /*  207 */ "db_optr",
  /*  208 */ "pps",
  /*  209 */ "tseries",
  /*  210 */ "dbs",
  /*  211 */ "streams",
  /*  212 */ "storage",
  /*  213 */ "qtime",
  /*  214 */ "users",
  /*  215 */ "conns",
  /*  216 */ "state",
  /*  217 */ "keep",
  /*  218 */ "tagitemlist",
  /*  219 */ "tables",
  /*  220 */ "cache",
  /*  221 */ "replica",
  /*  222 */ "days",
  /*  223 */ "rows",
  /*  224 */ "ablocks",
  /*  225 */ "tblocks",
  /*  226 */ "ctime",
  /*  227 */ "clog",
  /*  228 */ "comp",
  /*  229 */ "prec",
  /*  230 */ "typename",
  /*  231 */ "signed",
  /*  232 */ "create_table_args",
  /*  233 */ "columnlist",
  /*  234 */ "select",
  /*  235 */ "column",
  /*  236 */ "tagitem",
  /*  237 */ "selcollist",
  /*  238 */ "from",
  /*  239 */ "where_opt",
  /*  240 */ "interval_opt",
  /*  241 */ "fill_opt",
  /*  242 */ "sliding_opt",
  /*  243 */ "groupby_opt",
  /*  244 */ "orderby_opt",
  /*  245 */ "having_opt",
  /*  246 */ "slimit_opt",
  /*  247 */ "limit_opt",
  /*  248 */ "union",
  /*  249 */ "sclp",
  /*  250 */ "expr",
  /*  251 */ "as",
  /*  252 */ "tablelist",
  /*  253 */ "tmvar",
  /*  254 */ "sortlist",
  /*  255 */ "sortitem",
  /*  256 */ "item",
  /*  257 */ "sortorder",
  /*  258 */ "grouplist",
  /*  259 */ "exprlist",
  /*  260 */ "expritem",
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
 /*  27 */ "cmd ::= DROP DNODE IPTOKEN",
 /*  28 */ "cmd ::= DROP USER ids",
 /*  29 */ "cmd ::= DROP ACCOUNT ids",
 /*  30 */ "cmd ::= USE ids",
 /*  31 */ "cmd ::= DESCRIBE ids cpxName",
 /*  32 */ "cmd ::= ALTER USER ids PASS ids",
 /*  33 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  34 */ "cmd ::= ALTER DNODE IPTOKEN ids",
 /*  35 */ "cmd ::= ALTER DNODE IPTOKEN ids ids",
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
 /*  47 */ "cmd ::= CREATE DNODE IPTOKEN",
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
 /*  71 */ "tables ::= TABLES INTEGER",
 /*  72 */ "cache ::= CACHE INTEGER",
 /*  73 */ "replica ::= REPLICA INTEGER",
 /*  74 */ "days ::= DAYS INTEGER",
 /*  75 */ "rows ::= ROWS INTEGER",
 /*  76 */ "ablocks ::= ABLOCKS ID",
 /*  77 */ "tblocks ::= TBLOCKS INTEGER",
 /*  78 */ "ctime ::= CTIME INTEGER",
 /*  79 */ "clog ::= CLOG INTEGER",
 /*  80 */ "comp ::= COMP INTEGER",
 /*  81 */ "prec ::= PRECISION STRING",
 /*  82 */ "db_optr ::=",
 /*  83 */ "db_optr ::= db_optr tables",
 /*  84 */ "db_optr ::= db_optr cache",
 /*  85 */ "db_optr ::= db_optr replica",
 /*  86 */ "db_optr ::= db_optr days",
 /*  87 */ "db_optr ::= db_optr rows",
 /*  88 */ "db_optr ::= db_optr ablocks",
 /*  89 */ "db_optr ::= db_optr tblocks",
 /*  90 */ "db_optr ::= db_optr ctime",
 /*  91 */ "db_optr ::= db_optr clog",
 /*  92 */ "db_optr ::= db_optr comp",
 /*  93 */ "db_optr ::= db_optr prec",
 /*  94 */ "db_optr ::= db_optr keep",
 /*  95 */ "alter_db_optr ::=",
 /*  96 */ "alter_db_optr ::= alter_db_optr replica",
 /*  97 */ "alter_db_optr ::= alter_db_optr tables",
 /*  98 */ "typename ::= ids",
 /*  99 */ "typename ::= ids LP signed RP",
 /* 100 */ "signed ::= INTEGER",
 /* 101 */ "signed ::= PLUS INTEGER",
 /* 102 */ "signed ::= MINUS INTEGER",
 /* 103 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 104 */ "create_table_args ::= LP columnlist RP",
 /* 105 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 106 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 107 */ "create_table_args ::= AS select",
 /* 108 */ "columnlist ::= columnlist COMMA column",
 /* 109 */ "columnlist ::= column",
 /* 110 */ "column ::= ids typename",
 /* 111 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 112 */ "tagitemlist ::= tagitem",
 /* 113 */ "tagitem ::= INTEGER",
 /* 114 */ "tagitem ::= FLOAT",
 /* 115 */ "tagitem ::= STRING",
 /* 116 */ "tagitem ::= BOOL",
 /* 117 */ "tagitem ::= NULL",
 /* 118 */ "tagitem ::= MINUS INTEGER",
 /* 119 */ "tagitem ::= MINUS FLOAT",
 /* 120 */ "tagitem ::= PLUS INTEGER",
 /* 121 */ "tagitem ::= PLUS FLOAT",
 /* 122 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 123 */ "union ::= select",
 /* 124 */ "union ::= LP union RP",
 /* 125 */ "union ::= union UNION ALL select",
 /* 126 */ "union ::= union UNION ALL LP select RP",
 /* 127 */ "cmd ::= union",
 /* 128 */ "select ::= SELECT selcollist",
 /* 129 */ "sclp ::= selcollist COMMA",
 /* 130 */ "sclp ::=",
 /* 131 */ "selcollist ::= sclp expr as",
 /* 132 */ "selcollist ::= sclp STAR",
 /* 133 */ "as ::= AS ids",
 /* 134 */ "as ::= ids",
 /* 135 */ "as ::=",
 /* 136 */ "from ::= FROM tablelist",
 /* 137 */ "tablelist ::= ids cpxName",
 /* 138 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 139 */ "tmvar ::= VARIABLE",
 /* 140 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 141 */ "interval_opt ::=",
 /* 142 */ "fill_opt ::=",
 /* 143 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 144 */ "fill_opt ::= FILL LP ID RP",
 /* 145 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 146 */ "sliding_opt ::=",
 /* 147 */ "orderby_opt ::=",
 /* 148 */ "orderby_opt ::= ORDER BY sortlist",
 /* 149 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 150 */ "sortlist ::= item sortorder",
 /* 151 */ "item ::= ids cpxName",
 /* 152 */ "sortorder ::= ASC",
 /* 153 */ "sortorder ::= DESC",
 /* 154 */ "sortorder ::=",
 /* 155 */ "groupby_opt ::=",
 /* 156 */ "groupby_opt ::= GROUP BY grouplist",
 /* 157 */ "grouplist ::= grouplist COMMA item",
 /* 158 */ "grouplist ::= item",
 /* 159 */ "having_opt ::=",
 /* 160 */ "having_opt ::= HAVING expr",
 /* 161 */ "limit_opt ::=",
 /* 162 */ "limit_opt ::= LIMIT signed",
 /* 163 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 164 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 165 */ "slimit_opt ::=",
 /* 166 */ "slimit_opt ::= SLIMIT signed",
 /* 167 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 168 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 169 */ "where_opt ::=",
 /* 170 */ "where_opt ::= WHERE expr",
 /* 171 */ "expr ::= LP expr RP",
 /* 172 */ "expr ::= ID",
 /* 173 */ "expr ::= ID DOT ID",
 /* 174 */ "expr ::= ID DOT STAR",
 /* 175 */ "expr ::= INTEGER",
 /* 176 */ "expr ::= MINUS INTEGER",
 /* 177 */ "expr ::= PLUS INTEGER",
 /* 178 */ "expr ::= FLOAT",
 /* 179 */ "expr ::= MINUS FLOAT",
 /* 180 */ "expr ::= PLUS FLOAT",
 /* 181 */ "expr ::= STRING",
 /* 182 */ "expr ::= NOW",
 /* 183 */ "expr ::= VARIABLE",
 /* 184 */ "expr ::= BOOL",
 /* 185 */ "expr ::= ID LP exprlist RP",
 /* 186 */ "expr ::= ID LP STAR RP",
 /* 187 */ "expr ::= expr AND expr",
 /* 188 */ "expr ::= expr OR expr",
 /* 189 */ "expr ::= expr LT expr",
 /* 190 */ "expr ::= expr GT expr",
 /* 191 */ "expr ::= expr LE expr",
 /* 192 */ "expr ::= expr GE expr",
 /* 193 */ "expr ::= expr NE expr",
 /* 194 */ "expr ::= expr EQ expr",
 /* 195 */ "expr ::= expr PLUS expr",
 /* 196 */ "expr ::= expr MINUS expr",
 /* 197 */ "expr ::= expr STAR expr",
 /* 198 */ "expr ::= expr SLASH expr",
 /* 199 */ "expr ::= expr REM expr",
 /* 200 */ "expr ::= expr LIKE expr",
 /* 201 */ "expr ::= expr IN LP exprlist RP",
 /* 202 */ "exprlist ::= exprlist COMMA expritem",
 /* 203 */ "exprlist ::= expritem",
 /* 204 */ "expritem ::= expr",
 /* 205 */ "expritem ::=",
 /* 206 */ "cmd ::= RESET QUERY CACHE",
 /* 207 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 208 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 209 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 210 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 211 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 212 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 213 */ "cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER",
 /* 214 */ "cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER",
 /* 215 */ "cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER",
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
    case 217: /* keep */
    case 218: /* tagitemlist */
    case 241: /* fill_opt */
    case 243: /* groupby_opt */
    case 244: /* orderby_opt */
    case 254: /* sortlist */
    case 258: /* grouplist */
{
tVariantListDestroy((yypminor->yy480));
}
      break;
    case 233: /* columnlist */
{
tFieldListDestroy((yypminor->yy421));
}
      break;
    case 234: /* select */
{
doDestroyQuerySql((yypminor->yy138));
}
      break;
    case 237: /* selcollist */
    case 249: /* sclp */
    case 259: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy284));
}
      break;
    case 239: /* where_opt */
    case 245: /* having_opt */
    case 250: /* expr */
    case 260: /* expritem */
{
tSQLExprDestroy((yypminor->yy244));
}
      break;
    case 248: /* union */
{
destroyAllSelectClause((yypminor->yy117));
}
      break;
    case 255: /* sortitem */
{
tVariantDestroy(&(yypminor->yy236));
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
  {  198,   -1 }, /* (0) program ::= cmd */
  {  199,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  199,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  199,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  199,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  199,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  199,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  199,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  199,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  199,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  199,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  199,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  199,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  199,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  199,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  200,    0 }, /* (15) dbPrefix ::= */
  {  200,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  202,    0 }, /* (17) cpxName ::= */
  {  202,   -2 }, /* (18) cpxName ::= DOT ids */
  {  199,   -3 }, /* (19) cmd ::= SHOW dbPrefix TABLES */
  {  199,   -5 }, /* (20) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  199,   -3 }, /* (21) cmd ::= SHOW dbPrefix STABLES */
  {  199,   -5 }, /* (22) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  199,   -3 }, /* (23) cmd ::= SHOW dbPrefix VGROUPS */
  {  199,   -4 }, /* (24) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  199,   -5 }, /* (25) cmd ::= DROP TABLE ifexists ids cpxName */
  {  199,   -4 }, /* (26) cmd ::= DROP DATABASE ifexists ids */
  {  199,   -3 }, /* (27) cmd ::= DROP DNODE IPTOKEN */
  {  199,   -3 }, /* (28) cmd ::= DROP USER ids */
  {  199,   -3 }, /* (29) cmd ::= DROP ACCOUNT ids */
  {  199,   -2 }, /* (30) cmd ::= USE ids */
  {  199,   -3 }, /* (31) cmd ::= DESCRIBE ids cpxName */
  {  199,   -5 }, /* (32) cmd ::= ALTER USER ids PASS ids */
  {  199,   -5 }, /* (33) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  199,   -4 }, /* (34) cmd ::= ALTER DNODE IPTOKEN ids */
  {  199,   -5 }, /* (35) cmd ::= ALTER DNODE IPTOKEN ids ids */
  {  199,   -3 }, /* (36) cmd ::= ALTER LOCAL ids */
  {  199,   -4 }, /* (37) cmd ::= ALTER LOCAL ids ids */
  {  199,   -4 }, /* (38) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  199,   -4 }, /* (39) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  199,   -6 }, /* (40) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  201,   -1 }, /* (41) ids ::= ID */
  {  201,   -1 }, /* (42) ids ::= STRING */
  {  203,   -2 }, /* (43) ifexists ::= IF EXISTS */
  {  203,    0 }, /* (44) ifexists ::= */
  {  206,   -3 }, /* (45) ifnotexists ::= IF NOT EXISTS */
  {  206,    0 }, /* (46) ifnotexists ::= */
  {  199,   -3 }, /* (47) cmd ::= CREATE DNODE IPTOKEN */
  {  199,   -6 }, /* (48) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  199,   -5 }, /* (49) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  199,   -5 }, /* (50) cmd ::= CREATE USER ids PASS ids */
  {  208,    0 }, /* (51) pps ::= */
  {  208,   -2 }, /* (52) pps ::= PPS INTEGER */
  {  209,    0 }, /* (53) tseries ::= */
  {  209,   -2 }, /* (54) tseries ::= TSERIES INTEGER */
  {  210,    0 }, /* (55) dbs ::= */
  {  210,   -2 }, /* (56) dbs ::= DBS INTEGER */
  {  211,    0 }, /* (57) streams ::= */
  {  211,   -2 }, /* (58) streams ::= STREAMS INTEGER */
  {  212,    0 }, /* (59) storage ::= */
  {  212,   -2 }, /* (60) storage ::= STORAGE INTEGER */
  {  213,    0 }, /* (61) qtime ::= */
  {  213,   -2 }, /* (62) qtime ::= QTIME INTEGER */
  {  214,    0 }, /* (63) users ::= */
  {  214,   -2 }, /* (64) users ::= USERS INTEGER */
  {  215,    0 }, /* (65) conns ::= */
  {  215,   -2 }, /* (66) conns ::= CONNS INTEGER */
  {  216,    0 }, /* (67) state ::= */
  {  216,   -2 }, /* (68) state ::= STATE ids */
  {  205,   -9 }, /* (69) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  217,   -2 }, /* (70) keep ::= KEEP tagitemlist */
  {  219,   -2 }, /* (71) tables ::= TABLES INTEGER */
  {  220,   -2 }, /* (72) cache ::= CACHE INTEGER */
  {  221,   -2 }, /* (73) replica ::= REPLICA INTEGER */
  {  222,   -2 }, /* (74) days ::= DAYS INTEGER */
  {  223,   -2 }, /* (75) rows ::= ROWS INTEGER */
  {  224,   -2 }, /* (76) ablocks ::= ABLOCKS ID */
  {  225,   -2 }, /* (77) tblocks ::= TBLOCKS INTEGER */
  {  226,   -2 }, /* (78) ctime ::= CTIME INTEGER */
  {  227,   -2 }, /* (79) clog ::= CLOG INTEGER */
  {  228,   -2 }, /* (80) comp ::= COMP INTEGER */
  {  229,   -2 }, /* (81) prec ::= PRECISION STRING */
  {  207,    0 }, /* (82) db_optr ::= */
  {  207,   -2 }, /* (83) db_optr ::= db_optr tables */
  {  207,   -2 }, /* (84) db_optr ::= db_optr cache */
  {  207,   -2 }, /* (85) db_optr ::= db_optr replica */
  {  207,   -2 }, /* (86) db_optr ::= db_optr days */
  {  207,   -2 }, /* (87) db_optr ::= db_optr rows */
  {  207,   -2 }, /* (88) db_optr ::= db_optr ablocks */
  {  207,   -2 }, /* (89) db_optr ::= db_optr tblocks */
  {  207,   -2 }, /* (90) db_optr ::= db_optr ctime */
  {  207,   -2 }, /* (91) db_optr ::= db_optr clog */
  {  207,   -2 }, /* (92) db_optr ::= db_optr comp */
  {  207,   -2 }, /* (93) db_optr ::= db_optr prec */
  {  207,   -2 }, /* (94) db_optr ::= db_optr keep */
  {  204,    0 }, /* (95) alter_db_optr ::= */
  {  204,   -2 }, /* (96) alter_db_optr ::= alter_db_optr replica */
  {  204,   -2 }, /* (97) alter_db_optr ::= alter_db_optr tables */
  {  230,   -1 }, /* (98) typename ::= ids */
  {  230,   -4 }, /* (99) typename ::= ids LP signed RP */
  {  231,   -1 }, /* (100) signed ::= INTEGER */
  {  231,   -2 }, /* (101) signed ::= PLUS INTEGER */
  {  231,   -2 }, /* (102) signed ::= MINUS INTEGER */
  {  199,   -6 }, /* (103) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  232,   -3 }, /* (104) create_table_args ::= LP columnlist RP */
  {  232,   -7 }, /* (105) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  232,   -7 }, /* (106) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  232,   -2 }, /* (107) create_table_args ::= AS select */
  {  233,   -3 }, /* (108) columnlist ::= columnlist COMMA column */
  {  233,   -1 }, /* (109) columnlist ::= column */
  {  235,   -2 }, /* (110) column ::= ids typename */
  {  218,   -3 }, /* (111) tagitemlist ::= tagitemlist COMMA tagitem */
  {  218,   -1 }, /* (112) tagitemlist ::= tagitem */
  {  236,   -1 }, /* (113) tagitem ::= INTEGER */
  {  236,   -1 }, /* (114) tagitem ::= FLOAT */
  {  236,   -1 }, /* (115) tagitem ::= STRING */
  {  236,   -1 }, /* (116) tagitem ::= BOOL */
  {  236,   -1 }, /* (117) tagitem ::= NULL */
  {  236,   -2 }, /* (118) tagitem ::= MINUS INTEGER */
  {  236,   -2 }, /* (119) tagitem ::= MINUS FLOAT */
  {  236,   -2 }, /* (120) tagitem ::= PLUS INTEGER */
  {  236,   -2 }, /* (121) tagitem ::= PLUS FLOAT */
  {  234,  -12 }, /* (122) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  248,   -1 }, /* (123) union ::= select */
  {  248,   -3 }, /* (124) union ::= LP union RP */
  {  248,   -4 }, /* (125) union ::= union UNION ALL select */
  {  248,   -6 }, /* (126) union ::= union UNION ALL LP select RP */
  {  199,   -1 }, /* (127) cmd ::= union */
  {  234,   -2 }, /* (128) select ::= SELECT selcollist */
  {  249,   -2 }, /* (129) sclp ::= selcollist COMMA */
  {  249,    0 }, /* (130) sclp ::= */
  {  237,   -3 }, /* (131) selcollist ::= sclp expr as */
  {  237,   -2 }, /* (132) selcollist ::= sclp STAR */
  {  251,   -2 }, /* (133) as ::= AS ids */
  {  251,   -1 }, /* (134) as ::= ids */
  {  251,    0 }, /* (135) as ::= */
  {  238,   -2 }, /* (136) from ::= FROM tablelist */
  {  252,   -2 }, /* (137) tablelist ::= ids cpxName */
  {  252,   -4 }, /* (138) tablelist ::= tablelist COMMA ids cpxName */
  {  253,   -1 }, /* (139) tmvar ::= VARIABLE */
  {  240,   -4 }, /* (140) interval_opt ::= INTERVAL LP tmvar RP */
  {  240,    0 }, /* (141) interval_opt ::= */
  {  241,    0 }, /* (142) fill_opt ::= */
  {  241,   -6 }, /* (143) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  241,   -4 }, /* (144) fill_opt ::= FILL LP ID RP */
  {  242,   -4 }, /* (145) sliding_opt ::= SLIDING LP tmvar RP */
  {  242,    0 }, /* (146) sliding_opt ::= */
  {  244,    0 }, /* (147) orderby_opt ::= */
  {  244,   -3 }, /* (148) orderby_opt ::= ORDER BY sortlist */
  {  254,   -4 }, /* (149) sortlist ::= sortlist COMMA item sortorder */
  {  254,   -2 }, /* (150) sortlist ::= item sortorder */
  {  256,   -2 }, /* (151) item ::= ids cpxName */
  {  257,   -1 }, /* (152) sortorder ::= ASC */
  {  257,   -1 }, /* (153) sortorder ::= DESC */
  {  257,    0 }, /* (154) sortorder ::= */
  {  243,    0 }, /* (155) groupby_opt ::= */
  {  243,   -3 }, /* (156) groupby_opt ::= GROUP BY grouplist */
  {  258,   -3 }, /* (157) grouplist ::= grouplist COMMA item */
  {  258,   -1 }, /* (158) grouplist ::= item */
  {  245,    0 }, /* (159) having_opt ::= */
  {  245,   -2 }, /* (160) having_opt ::= HAVING expr */
  {  247,    0 }, /* (161) limit_opt ::= */
  {  247,   -2 }, /* (162) limit_opt ::= LIMIT signed */
  {  247,   -4 }, /* (163) limit_opt ::= LIMIT signed OFFSET signed */
  {  247,   -4 }, /* (164) limit_opt ::= LIMIT signed COMMA signed */
  {  246,    0 }, /* (165) slimit_opt ::= */
  {  246,   -2 }, /* (166) slimit_opt ::= SLIMIT signed */
  {  246,   -4 }, /* (167) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  246,   -4 }, /* (168) slimit_opt ::= SLIMIT signed COMMA signed */
  {  239,    0 }, /* (169) where_opt ::= */
  {  239,   -2 }, /* (170) where_opt ::= WHERE expr */
  {  250,   -3 }, /* (171) expr ::= LP expr RP */
  {  250,   -1 }, /* (172) expr ::= ID */
  {  250,   -3 }, /* (173) expr ::= ID DOT ID */
  {  250,   -3 }, /* (174) expr ::= ID DOT STAR */
  {  250,   -1 }, /* (175) expr ::= INTEGER */
  {  250,   -2 }, /* (176) expr ::= MINUS INTEGER */
  {  250,   -2 }, /* (177) expr ::= PLUS INTEGER */
  {  250,   -1 }, /* (178) expr ::= FLOAT */
  {  250,   -2 }, /* (179) expr ::= MINUS FLOAT */
  {  250,   -2 }, /* (180) expr ::= PLUS FLOAT */
  {  250,   -1 }, /* (181) expr ::= STRING */
  {  250,   -1 }, /* (182) expr ::= NOW */
  {  250,   -1 }, /* (183) expr ::= VARIABLE */
  {  250,   -1 }, /* (184) expr ::= BOOL */
  {  250,   -4 }, /* (185) expr ::= ID LP exprlist RP */
  {  250,   -4 }, /* (186) expr ::= ID LP STAR RP */
  {  250,   -3 }, /* (187) expr ::= expr AND expr */
  {  250,   -3 }, /* (188) expr ::= expr OR expr */
  {  250,   -3 }, /* (189) expr ::= expr LT expr */
  {  250,   -3 }, /* (190) expr ::= expr GT expr */
  {  250,   -3 }, /* (191) expr ::= expr LE expr */
  {  250,   -3 }, /* (192) expr ::= expr GE expr */
  {  250,   -3 }, /* (193) expr ::= expr NE expr */
  {  250,   -3 }, /* (194) expr ::= expr EQ expr */
  {  250,   -3 }, /* (195) expr ::= expr PLUS expr */
  {  250,   -3 }, /* (196) expr ::= expr MINUS expr */
  {  250,   -3 }, /* (197) expr ::= expr STAR expr */
  {  250,   -3 }, /* (198) expr ::= expr SLASH expr */
  {  250,   -3 }, /* (199) expr ::= expr REM expr */
  {  250,   -3 }, /* (200) expr ::= expr LIKE expr */
  {  250,   -5 }, /* (201) expr ::= expr IN LP exprlist RP */
  {  259,   -3 }, /* (202) exprlist ::= exprlist COMMA expritem */
  {  259,   -1 }, /* (203) exprlist ::= expritem */
  {  260,   -1 }, /* (204) expritem ::= expr */
  {  260,    0 }, /* (205) expritem ::= */
  {  199,   -3 }, /* (206) cmd ::= RESET QUERY CACHE */
  {  199,   -7 }, /* (207) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  199,   -7 }, /* (208) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  199,   -7 }, /* (209) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  199,   -7 }, /* (210) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  199,   -8 }, /* (211) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  199,   -9 }, /* (212) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  199,   -5 }, /* (213) cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
  {  199,   -7 }, /* (214) cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
  {  199,   -7 }, /* (215) cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
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
      case 10: /* cmd ::= SHOW CONFIGS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONFIGS, 0, 0);  }
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
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 16: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 17: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 18: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
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
    SSQLToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SSQLToken token;
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
      case 27: /* cmd ::= DROP DNODE IPTOKEN */
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
      case 34: /* cmd ::= ALTER DNODE IPTOKEN ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 35: /* cmd ::= ALTER DNODE IPTOKEN ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 36: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 37: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 38: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy155);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy155);}
        break;
      case 41: /* ids ::= ID */
      case 42: /* ids ::= STRING */ yytestcase(yyruleno==42);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 43: /* ifexists ::= IF EXISTS */
{yymsp[-1].minor.yy0.n = 1;}
        break;
      case 44: /* ifexists ::= */
      case 46: /* ifnotexists ::= */ yytestcase(yyruleno==46);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 45: /* ifnotexists ::= IF NOT EXISTS */
{yymsp[-2].minor.yy0.n = 1;}
        break;
      case 47: /* cmd ::= CREATE DNODE IPTOKEN */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 48: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy155);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
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
{yymsp[1].minor.yy0.n = 0;   }
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
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 69: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy155.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy155.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy155.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy155.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy155.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy155.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy155.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy155.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy155.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy155 = yylhsminor.yy155;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy480 = yymsp[0].minor.yy480; }
        break;
      case 71: /* tables ::= TABLES INTEGER */
      case 72: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==72);
      case 73: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==73);
      case 74: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==74);
      case 75: /* rows ::= ROWS INTEGER */ yytestcase(yyruleno==75);
      case 76: /* ablocks ::= ABLOCKS ID */ yytestcase(yyruleno==76);
      case 77: /* tblocks ::= TBLOCKS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==78);
      case 79: /* clog ::= CLOG INTEGER */ yytestcase(yyruleno==79);
      case 80: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==80);
      case 81: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==81);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 82: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
        break;
      case 83: /* db_optr ::= db_optr tables */
      case 97: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==97);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 84: /* db_optr ::= db_optr cache */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 85: /* db_optr ::= db_optr replica */
      case 96: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==96);
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 86: /* db_optr ::= db_optr days */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 87: /* db_optr ::= db_optr rows */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 88: /* db_optr ::= db_optr ablocks */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 89: /* db_optr ::= db_optr tblocks */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 90: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 91: /* db_optr ::= db_optr clog */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 92: /* db_optr ::= db_optr comp */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 93: /* db_optr ::= db_optr prec */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 94: /* db_optr ::= db_optr keep */
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.keep = yymsp[0].minor.yy480; }
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 95: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
        break;
      case 98: /* typename ::= ids */
{ tSQLSetColumnType (&yylhsminor.yy397, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy397 = yylhsminor.yy397;
        break;
      case 99: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;          // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy397, &yymsp[-3].minor.yy0);
}
  yymsp[-3].minor.yy397 = yylhsminor.yy397;
        break;
      case 100: /* signed ::= INTEGER */
{ yylhsminor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 101: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 102: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 103: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedMeterName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 104: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy344 = tSetCreateSQLElems(yymsp[-1].minor.yy421, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy344, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 105: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy344 = tSetCreateSQLElems(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy344, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 106: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy344 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy480, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy344, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 107: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy344 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy138, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy344, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 108: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy421 = tFieldListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy397);   }
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 109: /* columnlist ::= column */
{yylhsminor.yy421 = tFieldListAppend(NULL, &yymsp[0].minor.yy397);}
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 110: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy397, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy397);
}
  yymsp[-1].minor.yy397 = yylhsminor.yy397;
        break;
      case 111: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy480 = tVariantListAppend(yymsp[-2].minor.yy480, &yymsp[0].minor.yy236, -1);    }
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 112: /* tagitemlist ::= tagitem */
{ yylhsminor.yy480 = tVariantListAppend(NULL, &yymsp[0].minor.yy236, -1); }
  yymsp[0].minor.yy480 = yylhsminor.yy480;
        break;
      case 113: /* tagitem ::= INTEGER */
      case 114: /* tagitem ::= FLOAT */ yytestcase(yyruleno==114);
      case 115: /* tagitem ::= STRING */ yytestcase(yyruleno==115);
      case 116: /* tagitem ::= BOOL */ yytestcase(yyruleno==116);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy236, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy236 = yylhsminor.yy236;
        break;
      case 117: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy236, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy236 = yylhsminor.yy236;
        break;
      case 118: /* tagitem ::= MINUS INTEGER */
      case 119: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==119);
      case 120: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==120);
      case 121: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==121);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy236, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 122: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy138 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy284, yymsp[-9].minor.yy480, yymsp[-8].minor.yy244, yymsp[-4].minor.yy480, yymsp[-3].minor.yy480, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy480, &yymsp[0].minor.yy162, &yymsp[-1].minor.yy162);
}
  yymsp[-11].minor.yy138 = yylhsminor.yy138;
        break;
      case 123: /* union ::= select */
{ yylhsminor.yy117 = setSubclause(NULL, yymsp[0].minor.yy138); }
  yymsp[0].minor.yy117 = yylhsminor.yy117;
        break;
      case 124: /* union ::= LP union RP */
{ yymsp[-2].minor.yy117 = yymsp[-1].minor.yy117; }
        break;
      case 125: /* union ::= union UNION ALL select */
{ yylhsminor.yy117 = appendSelectClause(yymsp[-3].minor.yy117, yymsp[0].minor.yy138); }
  yymsp[-3].minor.yy117 = yylhsminor.yy117;
        break;
      case 126: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy117 = appendSelectClause(yymsp[-5].minor.yy117, yymsp[-1].minor.yy138); }
  yymsp[-5].minor.yy117 = yylhsminor.yy117;
        break;
      case 127: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy117, NULL, TSDB_SQL_SELECT); }
        break;
      case 128: /* select ::= SELECT selcollist */
{
  yylhsminor.yy138 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy284, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy138 = yylhsminor.yy138;
        break;
      case 129: /* sclp ::= selcollist COMMA */
{yylhsminor.yy284 = yymsp[-1].minor.yy284;}
  yymsp[-1].minor.yy284 = yylhsminor.yy284;
        break;
      case 130: /* sclp ::= */
{yymsp[1].minor.yy284 = 0;}
        break;
      case 131: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy284 = tSQLExprListAppend(yymsp[-2].minor.yy284, yymsp[-1].minor.yy244, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy284 = yylhsminor.yy284;
        break;
      case 132: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy284 = tSQLExprListAppend(yymsp[-1].minor.yy284, pNode, 0);
}
  yymsp[-1].minor.yy284 = yylhsminor.yy284;
        break;
      case 133: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 134: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 135: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 136: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy480 = yymsp[0].minor.yy480;}
        break;
      case 137: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy480 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 138: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy480 = tVariantListAppendToken(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 139: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 140: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 145: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==145);
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 141: /* interval_opt ::= */
      case 146: /* sliding_opt ::= */ yytestcase(yyruleno==146);
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 142: /* fill_opt ::= */
{yymsp[1].minor.yy480 = 0;     }
        break;
      case 143: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy480, &A, -1, 0);
    yymsp[-5].minor.yy480 = yymsp[-1].minor.yy480;
}
        break;
      case 144: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy480 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 147: /* orderby_opt ::= */
      case 155: /* groupby_opt ::= */ yytestcase(yyruleno==155);
{yymsp[1].minor.yy480 = 0;}
        break;
      case 148: /* orderby_opt ::= ORDER BY sortlist */
      case 156: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==156);
{yymsp[-2].minor.yy480 = yymsp[0].minor.yy480;}
        break;
      case 149: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy480 = tVariantListAppend(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy236, yymsp[0].minor.yy220);
}
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 150: /* sortlist ::= item sortorder */
{
  yylhsminor.yy480 = tVariantListAppend(NULL, &yymsp[-1].minor.yy236, yymsp[0].minor.yy220);
}
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 151: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy236, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 152: /* sortorder ::= ASC */
{yymsp[0].minor.yy220 = TSQL_SO_ASC; }
        break;
      case 153: /* sortorder ::= DESC */
{yymsp[0].minor.yy220 = TSQL_SO_DESC;}
        break;
      case 154: /* sortorder ::= */
{yymsp[1].minor.yy220 = TSQL_SO_ASC;}
        break;
      case 157: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy480 = tVariantListAppend(yymsp[-2].minor.yy480, &yymsp[0].minor.yy236, -1);
}
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 158: /* grouplist ::= item */
{
  yylhsminor.yy480 = tVariantListAppend(NULL, &yymsp[0].minor.yy236, -1);
}
  yymsp[0].minor.yy480 = yylhsminor.yy480;
        break;
      case 159: /* having_opt ::= */
      case 169: /* where_opt ::= */ yytestcase(yyruleno==169);
      case 205: /* expritem ::= */ yytestcase(yyruleno==205);
{yymsp[1].minor.yy244 = 0;}
        break;
      case 160: /* having_opt ::= HAVING expr */
      case 170: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==170);
{yymsp[-1].minor.yy244 = yymsp[0].minor.yy244;}
        break;
      case 161: /* limit_opt ::= */
      case 165: /* slimit_opt ::= */ yytestcase(yyruleno==165);
{yymsp[1].minor.yy162.limit = -1; yymsp[1].minor.yy162.offset = 0;}
        break;
      case 162: /* limit_opt ::= LIMIT signed */
      case 166: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==166);
{yymsp[-1].minor.yy162.limit = yymsp[0].minor.yy369;  yymsp[-1].minor.yy162.offset = 0;}
        break;
      case 163: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 167: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==167);
{yymsp[-3].minor.yy162.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy162.offset = yymsp[0].minor.yy369;}
        break;
      case 164: /* limit_opt ::= LIMIT signed COMMA signed */
      case 168: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==168);
{yymsp[-3].minor.yy162.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy162.offset = yymsp[-2].minor.yy369;}
        break;
      case 171: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy244 = yymsp[-1].minor.yy244; }
        break;
      case 172: /* expr ::= ID */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 173: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 174: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 175: /* expr ::= INTEGER */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 176: /* expr ::= MINUS INTEGER */
      case 177: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==177);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy244 = yylhsminor.yy244;
        break;
      case 178: /* expr ::= FLOAT */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 179: /* expr ::= MINUS FLOAT */
      case 180: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==180);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy244 = yylhsminor.yy244;
        break;
      case 181: /* expr ::= STRING */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 182: /* expr ::= NOW */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 183: /* expr ::= VARIABLE */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 184: /* expr ::= BOOL */
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 185: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy244 = tSQLExprCreateFunction(yymsp[-1].minor.yy284, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy244 = yylhsminor.yy244;
        break;
      case 186: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy244 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy244 = yylhsminor.yy244;
        break;
      case 187: /* expr ::= expr AND expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_AND);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 188: /* expr ::= expr OR expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_OR); }
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 189: /* expr ::= expr LT expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LT);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 190: /* expr ::= expr GT expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_GT);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 191: /* expr ::= expr LE expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LE);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 192: /* expr ::= expr GE expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_GE);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 193: /* expr ::= expr NE expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_NE);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 194: /* expr ::= expr EQ expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_EQ);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 195: /* expr ::= expr PLUS expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_PLUS);  }
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 196: /* expr ::= expr MINUS expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_MINUS); }
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 197: /* expr ::= expr STAR expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_STAR);  }
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 198: /* expr ::= expr SLASH expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_DIVIDE);}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 199: /* expr ::= expr REM expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_REM);   }
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 200: /* expr ::= expr LIKE expr */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LIKE);  }
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 201: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-4].minor.yy244, (tSQLExpr*)yymsp[-1].minor.yy284, TK_IN); }
  yymsp[-4].minor.yy244 = yylhsminor.yy244;
        break;
      case 202: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy284 = tSQLExprListAppend(yymsp[-2].minor.yy284,yymsp[0].minor.yy244,0);}
  yymsp[-2].minor.yy284 = yylhsminor.yy284;
        break;
      case 203: /* exprlist ::= expritem */
{yylhsminor.yy284 = tSQLExprListAppend(0,yymsp[0].minor.yy244,0);}
  yymsp[0].minor.yy284 = yylhsminor.yy284;
        break;
      case 204: /* expritem ::= expr */
{yylhsminor.yy244 = yymsp[0].minor.yy244;}
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 206: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 207: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 208: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 209: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 210: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 211: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 212: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy236, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 213: /* cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[-2].minor.yy0);}
        break;
      case 214: /* cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-4].minor.yy0);}
        break;
      case 215: /* cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-4].minor.yy0);}
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
