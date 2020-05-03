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

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "qsqlparser.h"
#include "tstoken.h"
#include "tutil.h"
#include "tvariant.h"
#include "ttokendef.h"
#include "qsqltype.h"

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
#define YYNOCODE 268
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tVariantList* yy30;
  SLimitVal yy150;
  SCreateTableSQL* yy212;
  SCreateAcctSQL yy239;
  int yy250;
  SSubclauseInfo* yy309;
  tFieldList* yy325;
  tVariant yy380;
  tSQLExpr* yy388;
  SQuerySQL* yy444;
  int64_t yy489;
  TAOS_FIELD yy505;
  tSQLExprList* yy506;
  SCreateDBInfo yy532;
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
#define YYNTOKEN             203
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
 /*     0 */   752,  440,  135,  153,  244,   10,  616,  246,  135,  441,
 /*    10 */   135,  158,  821,   41,   43,   20,   35,   36,  820,  157,
 /*    20 */   821,   29,  741,  440,  203,   39,   37,   40,   38,  134,
 /*    30 */   499,  441,   99,   34,   33,  103,  154,   32,   31,   30,
 /*    40 */    41,   43,  741,   35,   36,  155,  139,  166,   29,  727,
 /*    50 */   749,  203,   39,   37,   40,   38,  188,  103,  227,  226,
 /*    60 */    34,   33,  165,  730,   32,   31,   30,  400,  401,  402,
 /*    70 */   403,  404,  405,  406,  407,  408,  409,  410,  411,  245,
 /*    80 */   730,   41,   43,  191,   35,   36,  218,  238,  200,   29,
 /*    90 */    58,   20,  203,   39,   37,   40,   38,   32,   31,   30,
 /*   100 */    56,   34,   33,   76,  730,   32,   31,   30,   43,  238,
 /*   110 */    35,   36,  776,  817,  198,   29,   20,   20,  203,   39,
 /*   120 */    37,   40,   38,  167,  570,  727,  229,   34,   33,  440,
 /*   130 */   170,   32,   31,   30,  240,   35,   36,  441,    7,  816,
 /*   140 */    29,   61,  113,  203,   39,   37,   40,   38,  225,  230,
 /*   150 */   727,  727,   34,   33,   50,  728,   32,   31,   30,   15,
 /*   160 */   217,  239,  216,  215,  214,  213,  212,  211,  210,  209,
 /*   170 */   712,   51,  701,  702,  703,  704,  705,  706,  707,  708,
 /*   180 */   709,  710,  711,  162,  583,   11,  815,  574,  103,  577,
 /*   190 */   103,  580,  171,  162,  583,  224,  223,  574,   16,  577,
 /*   200 */    20,  580,   34,   33,  148,   26,   32,   31,   30,  240,
 /*   210 */    88,   87,  142,  177,  657,  159,  160,  126,  147,  202,
 /*   220 */   185,  715,  182,  714,  151,  159,  160,  162,  583,  531,
 /*   230 */    60,  574,  152,  577,  726,  580,  239,   16,   39,   37,
 /*   240 */    40,   38,   27,  775,   26,   59,   34,   33,  551,  552,
 /*   250 */    32,   31,   30,  140,  116,  117,   68,   64,   67,  159,
 /*   260 */   160,   98,  515,  666,  187,  512,  126,  513,   26,  514,
 /*   270 */   523,  150,  130,  128,   91,   90,   89,  190,   42,  161,
 /*   280 */    74,   78,   83,   86,   77,  572,  528,  729,   42,  582,
 /*   290 */    80,   17,  658,  168,  169,  126,  243,  242,   95,  582,
 /*   300 */    47,  542,  543,  600,  581,   45,   13,   12,  584,  576,
 /*   310 */   141,  579,   12,  575,  581,  578,    2,   73,   72,   48,
 /*   320 */   505,  573,   42,  743,   45,  504,  207,    9,    8,   21,
 /*   330 */    21,  143,  519,  582,  520,  517,  144,  518,   85,   84,
 /*   340 */   145,  146,  137,  133,  138,  830,  136,  786,  581,  785,
 /*   350 */   163,  782,  781,  164,  751,  721,  768,  228,  100,  767,
 /*   360 */   114,  115,  516,  668,  208,  112,  131,   24,  221,  665,
 /*   370 */   222,  829,   26,   70,  828,  826,  189,  118,  686,   25,
 /*   380 */    93,   22,  132,  655,   79,  653,   81,   82,  651,  538,
 /*   390 */   650,  172,  192,  127,  648,  647,  646,  196,   52,  740,
 /*   400 */   645,  644,  636,  129,  642,  640,   49,  638,   44,  105,
 /*   410 */   755,  756,  201,  199,  195,  769,  197,  193,   28,  220,
 /*   420 */    75,  231,  232,  233,  234,  235,  236,  205,  237,  241,
 /*   430 */    53,  614,  173,  174,  175,  149,   62,   65,  176,  613,
 /*   440 */   179,  181,  649,  178,  180,  612,   92,   94,  183,  121,
 /*   450 */   184,  120,  687,  119,  122,  725,  123,  125,  124,  109,
 /*   460 */   106,  104,  643,  107,  110,  108,  111,   23,    1,  190,
 /*   470 */   605,  186,  525,   55,  539,   57,  156,  101,  194,   18,
 /*   480 */    19,    4,  544,  102,  204,  585,    3,   14,    5,    6,
 /*   490 */    63,  480,  206,  479,  478,  477,  476,  475,  474,  473,
 /*   500 */   471,   45,  219,  444,   66,   21,  501,  500,   46,  498,
 /*   510 */    54,  465,  463,  455,  461,  457,   69,  459,  453,  451,
 /*   520 */   472,  470,   71,  442,   96,   97,  415,  413,  618,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   207,    1,  256,  206,  207,  256,  204,  205,  256,    9,
 /*    10 */   256,  265,  266,   13,   14,  207,   16,   17,  266,  265,
 /*    20 */   266,   21,  240,    1,   24,   25,   26,   27,   28,  256,
 /*    30 */     5,    9,  207,   33,   34,  207,  254,   37,   38,   39,
 /*    40 */    13,   14,  240,   16,   17,  224,  256,  239,   21,  241,
 /*    50 */   257,   24,   25,   26,   27,   28,  254,  207,   33,   34,
 /*    60 */    33,   34,  224,  242,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */   242,   13,   14,  258,   16,   17,  224,   78,  260,   21,
 /*    90 */   262,  207,   24,   25,   26,   27,   28,   37,   38,   39,
 /*   100 */   100,   33,   34,   72,  242,   37,   38,   39,   14,   78,
 /*   110 */    16,   17,  262,  256,  264,   21,  207,  207,   24,   25,
 /*   120 */    26,   27,   28,  239,   97,  241,  207,   33,   34,    1,
 /*   130 */    63,   37,   38,   39,   60,   16,   17,    9,   96,  256,
 /*   140 */    21,   99,  100,   24,   25,   26,   27,   28,  239,  239,
 /*   150 */   241,  241,   33,   34,  101,  236,   37,   38,   39,   85,
 /*   160 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   170 */   223,  118,  225,  226,  227,  228,  229,  230,  231,  232,
 /*   180 */   233,  234,  235,    1,    2,   44,  256,    5,  207,    7,
 /*   190 */   207,    9,  125,    1,    2,  128,  129,    5,   96,    7,
 /*   200 */   207,    9,   33,   34,   63,  103,   37,   38,   39,   60,
 /*   210 */    69,   70,   71,  124,  211,   33,   34,  214,   77,   37,
 /*   220 */   131,  225,  133,  227,  256,   33,   34,    1,    2,   37,
 /*   230 */   243,    5,  256,    7,  241,    9,   87,   96,   25,   26,
 /*   240 */    27,   28,  255,  262,  103,  262,   33,   34,  113,  114,
 /*   250 */    37,   38,   39,  256,   64,   65,   66,   67,   68,   33,
 /*   260 */    34,   96,    2,  211,  123,    5,  214,    7,  103,    9,
 /*   270 */    97,  130,   64,   65,   66,   67,   68,  104,   96,   59,
 /*   280 */    64,   65,   66,   67,   68,    1,  101,  242,   96,  107,
 /*   290 */    74,  106,  211,   33,   34,  214,   60,   61,   62,  107,
 /*   300 */   101,   97,   97,   97,  122,  101,  101,  101,   97,    5,
 /*   310 */   256,    7,  101,    5,  122,    7,   96,  126,  127,  120,
 /*   320 */    97,   37,   96,  240,  101,   97,   97,  126,  127,  101,
 /*   330 */   101,  256,    5,  107,    7,    5,  256,    7,   72,   73,
 /*   340 */   256,  256,  256,  256,  256,  242,  256,  237,  122,  237,
 /*   350 */   237,  237,  237,  237,  207,  238,  263,  237,  207,  263,
 /*   360 */   207,  207,  102,  207,  207,  244,  207,  207,  207,  207,
 /*   370 */   207,  207,  103,  207,  207,  207,  240,  207,  207,  207,
 /*   380 */    59,  207,  207,  207,  207,  207,  207,  207,  207,  107,
 /*   390 */   207,  207,  259,  207,  207,  207,  207,  259,  117,  253,
 /*   400 */   207,  207,  207,  207,  207,  207,  119,  207,  116,  251,
 /*   410 */   208,  208,  111,  115,  109,  208,  110,  108,  121,   75,
 /*   420 */    84,   83,   49,   80,   82,   53,   81,  208,   79,   75,
 /*   430 */   208,    5,  132,    5,  132,  208,  212,  212,   58,    5,
 /*   440 */     5,   58,  208,  132,  132,    5,  209,  209,  132,  216,
 /*   450 */    58,  220,  222,  221,  219,  240,  217,  215,  218,  247,
 /*   460 */   250,  252,  208,  249,  246,  248,  245,  210,  213,  104,
 /*   470 */    86,  124,   97,  105,   97,  101,    1,   96,   96,  101,
 /*   480 */   101,  112,   97,   96,   98,   97,   96,   96,  112,   96,
 /*   490 */    72,    9,   98,    5,    5,    5,    5,    1,    5,    5,
 /*   500 */     5,  101,   15,   76,   72,  101,    5,    5,   16,   97,
 /*   510 */    96,    5,    5,    5,    5,    5,  127,    5,    5,    5,
 /*   520 */     5,    5,  127,   76,   21,   21,   59,   58,    0,  267,
 /*   530 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   540 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   550 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   560 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   570 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   580 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   590 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   600 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   610 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   620 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   630 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   640 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   650 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   660 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   670 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   680 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   690 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   700 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   710 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   720 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   730 */   267,  267,
};
#define YY_SHIFT_COUNT    (246)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (528)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   141,   74,  182,  226,  128,  128,  128,  128,  128,  128,
 /*    10 */     0,   22,  226,  260,  260,  260,  102,  128,  128,  128,
 /*    20 */   128,  128,   31,  149,    9,    9,  529,  192,  226,  226,
 /*    30 */   226,  226,  226,  226,  226,  226,  226,  226,  226,  226,
 /*    40 */   226,  226,  226,  226,  226,  260,  260,   25,   25,   25,
 /*    50 */    25,   25,   25,   42,   25,  165,  128,  128,  135,  135,
 /*    60 */   185,  128,  128,  128,  128,  128,  128,  128,  128,  128,
 /*    70 */   128,  128,  128,  128,  128,  128,  128,  128,  128,  128,
 /*    80 */   128,  128,  128,  128,  128,  128,  128,  128,  128,  128,
 /*    90 */   128,  128,  128,  128,  128,  128,  128,  128,  269,  321,
 /*   100 */   321,  282,  282,  321,  281,  287,  292,  301,  298,  306,
 /*   110 */   305,  309,  297,  269,  321,  321,  344,  344,  321,  336,
 /*   120 */   338,  373,  343,  342,  372,  345,  349,  321,  354,  321,
 /*   130 */   354,  529,  529,   27,   68,   68,   68,   94,  119,  213,
 /*   140 */   213,  213,  216,  169,  169,  169,  169,  190,  208,   67,
 /*   150 */    89,   60,   60,  236,  173,  204,  205,  206,  211,  304,
 /*   160 */   308,  284,  220,  199,   53,  223,  228,  229,  327,  330,
 /*   170 */   191,  201,  266,  426,  300,  428,  302,  380,  434,  311,
 /*   180 */   435,  312,  383,  440,  316,  392,  384,  347,  365,  375,
 /*   190 */   368,  374,  377,  381,  475,  382,  385,  387,  378,  369,
 /*   200 */   379,  376,  388,  390,  391,  386,  393,  394,  418,  482,
 /*   210 */   488,  489,  490,  491,  496,  493,  494,  495,  400,  427,
 /*   220 */   487,  432,  492,  389,  395,  404,  501,  502,  412,  414,
 /*   230 */   404,  506,  507,  508,  509,  510,  512,  513,  514,  515,
 /*   240 */   516,  447,  503,  504,  467,  469,  528,
};
#define YY_REDUCE_COUNT (132)
#define YY_REDUCE_MIN   (-254)
#define YY_REDUCE_MAX   (257)
static const short yy_reduce_ofst[] = {
 /*     0 */  -198,  -53, -254, -246, -150, -172, -192, -116,  -91,  -90,
 /*    10 */  -207, -203, -248, -179, -162, -138, -218, -175,  -19,  -17,
 /*    20 */   -81,   -7,    3,   -4,   52,   81,  -13, -251, -227, -210,
 /*    30 */  -143, -117,  -70,  -32,  -24,   -3,   54,   75,   80,   84,
 /*    40 */    85,   86,   87,   88,   90,   45,  103,  110,  112,  113,
 /*    50 */   114,  115,  116,  117,  120,   83,  147,  151,   93,   96,
 /*    60 */   121,  153,  154,  156,  157,  159,  160,  161,  162,  163,
 /*    70 */   164,  166,  167,  168,  170,  171,  172,  174,  175,  176,
 /*    80 */   177,  178,  179,  180,  181,  183,  184,  186,  187,  188,
 /*    90 */   189,  193,  194,  195,  196,  197,  198,  200,  136,  202,
 /*   100 */   203,  133,  138,  207,  146,  209,  158,  210,  214,  217,
 /*   110 */   212,  218,  221,  215,  219,  222,  224,  225,  227,  230,
 /*   120 */   232,  231,  233,  235,  239,  240,  242,  234,  237,  254,
 /*   130 */   238,  255,  257,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   615,  667,  823,  823,  615,  615,  615,  615,  615,  615,
 /*    10 */   753,  633,  823,  615,  615,  615,  615,  615,  615,  615,
 /*    20 */   615,  615,  669,  656,  669,  669,  748,  615,  615,  615,
 /*    30 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    40 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    50 */   615,  615,  615,  615,  615,  615,  615,  615,  772,  772,
 /*    60 */   746,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    70 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  654,
 /*    80 */   615,  652,  615,  615,  615,  615,  615,  615,  615,  615,
 /*    90 */   615,  615,  615,  615,  615,  641,  615,  615,  615,  635,
 /*   100 */   635,  615,  615,  635,  779,  783,  777,  765,  773,  764,
 /*   110 */   760,  759,  787,  615,  635,  635,  664,  664,  635,  685,
 /*   120 */   683,  681,  673,  679,  675,  677,  671,  635,  662,  635,
 /*   130 */   662,  700,  713,  615,  788,  822,  778,  806,  805,  818,
 /*   140 */   812,  811,  615,  810,  809,  808,  807,  615,  615,  615,
 /*   150 */   615,  814,  813,  615,  615,  615,  615,  615,  615,  615,
 /*   160 */   615,  615,  790,  784,  780,  615,  615,  615,  615,  615,
 /*   170 */   615,  615,  615,  615,  615,  615,  615,  615,  615,  615,
 /*   180 */   615,  615,  615,  615,  615,  615,  615,  615,  745,  615,
 /*   190 */   615,  754,  615,  615,  615,  615,  615,  615,  774,  615,
 /*   200 */   766,  615,  615,  615,  615,  615,  615,  722,  615,  615,
 /*   210 */   615,  615,  615,  615,  615,  615,  615,  615,  688,  615,
 /*   220 */   615,  615,  615,  615,  615,  827,  615,  615,  615,  716,
 /*   230 */   825,  615,  615,  615,  615,  615,  615,  615,  615,  615,
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
    1,  /*       RATE => ID */
    1,  /*      IRATE => ID */
    1,  /*   SUM_RATE => ID */
    1,  /*  SUM_IRATE => ID */
    1,  /*   AVG_RATE => ID */
    1,  /*  AVG_IRATE => ID */
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
  /*  184 */ "RATE",
  /*  185 */ "IRATE",
  /*  186 */ "SUM_RATE",
  /*  187 */ "SUM_IRATE",
  /*  188 */ "AVG_RATE",
  /*  189 */ "AVG_IRATE",
  /*  190 */ "SEMI",
  /*  191 */ "NONE",
  /*  192 */ "PREV",
  /*  193 */ "LINEAR",
  /*  194 */ "IMPORT",
  /*  195 */ "METRIC",
  /*  196 */ "TBNAME",
  /*  197 */ "JOIN",
  /*  198 */ "METRICS",
  /*  199 */ "STABLE",
  /*  200 */ "INSERT",
  /*  201 */ "INTO",
  /*  202 */ "VALUES",
  /*  203 */ "error",
  /*  204 */ "program",
  /*  205 */ "cmd",
  /*  206 */ "dbPrefix",
  /*  207 */ "ids",
  /*  208 */ "cpxName",
  /*  209 */ "ifexists",
  /*  210 */ "alter_db_optr",
  /*  211 */ "acct_optr",
  /*  212 */ "ifnotexists",
  /*  213 */ "db_optr",
  /*  214 */ "pps",
  /*  215 */ "tseries",
  /*  216 */ "dbs",
  /*  217 */ "streams",
  /*  218 */ "storage",
  /*  219 */ "qtime",
  /*  220 */ "users",
  /*  221 */ "conns",
  /*  222 */ "state",
  /*  223 */ "keep",
  /*  224 */ "tagitemlist",
  /*  225 */ "tables",
  /*  226 */ "cache",
  /*  227 */ "replica",
  /*  228 */ "days",
  /*  229 */ "rows",
  /*  230 */ "ablocks",
  /*  231 */ "tblocks",
  /*  232 */ "ctime",
  /*  233 */ "clog",
  /*  234 */ "comp",
  /*  235 */ "prec",
  /*  236 */ "typename",
  /*  237 */ "signed",
  /*  238 */ "create_table_args",
  /*  239 */ "columnlist",
  /*  240 */ "select",
  /*  241 */ "column",
  /*  242 */ "tagitem",
  /*  243 */ "selcollist",
  /*  244 */ "from",
  /*  245 */ "where_opt",
  /*  246 */ "interval_opt",
  /*  247 */ "fill_opt",
  /*  248 */ "sliding_opt",
  /*  249 */ "groupby_opt",
  /*  250 */ "orderby_opt",
  /*  251 */ "having_opt",
  /*  252 */ "slimit_opt",
  /*  253 */ "limit_opt",
  /*  254 */ "union",
  /*  255 */ "sclp",
  /*  256 */ "expr",
  /*  257 */ "as",
  /*  258 */ "tablelist",
  /*  259 */ "tmvar",
  /*  260 */ "sortlist",
  /*  261 */ "sortitem",
  /*  262 */ "item",
  /*  263 */ "sortorder",
  /*  264 */ "grouplist",
  /*  265 */ "exprlist",
  /*  266 */ "expritem",
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
    case 223: /* keep */
    case 224: /* tagitemlist */
    case 247: /* fill_opt */
    case 249: /* groupby_opt */
    case 250: /* orderby_opt */
    case 260: /* sortlist */
    case 264: /* grouplist */
{
tVariantListDestroy((yypminor->yy30));
}
      break;
    case 239: /* columnlist */
{
tFieldListDestroy((yypminor->yy325));
}
      break;
    case 240: /* select */
{
doDestroyQuerySql((yypminor->yy444));
}
      break;
    case 243: /* selcollist */
    case 255: /* sclp */
    case 265: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy506));
}
      break;
    case 245: /* where_opt */
    case 251: /* having_opt */
    case 256: /* expr */
    case 266: /* expritem */
{
tSQLExprDestroy((yypminor->yy388));
}
      break;
    case 254: /* union */
{
destroyAllSelectClause((yypminor->yy309));
}
      break;
    case 261: /* sortitem */
{
tVariantDestroy(&(yypminor->yy380));
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
  {  204,   -1 }, /* (0) program ::= cmd */
  {  205,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  205,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  205,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  205,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  205,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  205,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  205,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  205,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  205,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  205,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  205,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  205,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  205,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  205,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  206,    0 }, /* (15) dbPrefix ::= */
  {  206,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  208,    0 }, /* (17) cpxName ::= */
  {  208,   -2 }, /* (18) cpxName ::= DOT ids */
  {  205,   -3 }, /* (19) cmd ::= SHOW dbPrefix TABLES */
  {  205,   -5 }, /* (20) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  205,   -3 }, /* (21) cmd ::= SHOW dbPrefix STABLES */
  {  205,   -5 }, /* (22) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  205,   -3 }, /* (23) cmd ::= SHOW dbPrefix VGROUPS */
  {  205,   -4 }, /* (24) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  205,   -5 }, /* (25) cmd ::= DROP TABLE ifexists ids cpxName */
  {  205,   -4 }, /* (26) cmd ::= DROP DATABASE ifexists ids */
  {  205,   -3 }, /* (27) cmd ::= DROP DNODE ids */
  {  205,   -3 }, /* (28) cmd ::= DROP USER ids */
  {  205,   -3 }, /* (29) cmd ::= DROP ACCOUNT ids */
  {  205,   -2 }, /* (30) cmd ::= USE ids */
  {  205,   -3 }, /* (31) cmd ::= DESCRIBE ids cpxName */
  {  205,   -5 }, /* (32) cmd ::= ALTER USER ids PASS ids */
  {  205,   -5 }, /* (33) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  205,   -4 }, /* (34) cmd ::= ALTER DNODE ids ids */
  {  205,   -5 }, /* (35) cmd ::= ALTER DNODE ids ids ids */
  {  205,   -3 }, /* (36) cmd ::= ALTER LOCAL ids */
  {  205,   -4 }, /* (37) cmd ::= ALTER LOCAL ids ids */
  {  205,   -4 }, /* (38) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  205,   -4 }, /* (39) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  205,   -6 }, /* (40) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  207,   -1 }, /* (41) ids ::= ID */
  {  207,   -1 }, /* (42) ids ::= STRING */
  {  209,   -2 }, /* (43) ifexists ::= IF EXISTS */
  {  209,    0 }, /* (44) ifexists ::= */
  {  212,   -3 }, /* (45) ifnotexists ::= IF NOT EXISTS */
  {  212,    0 }, /* (46) ifnotexists ::= */
  {  205,   -3 }, /* (47) cmd ::= CREATE DNODE ids */
  {  205,   -6 }, /* (48) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  205,   -5 }, /* (49) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  205,   -5 }, /* (50) cmd ::= CREATE USER ids PASS ids */
  {  214,    0 }, /* (51) pps ::= */
  {  214,   -2 }, /* (52) pps ::= PPS INTEGER */
  {  215,    0 }, /* (53) tseries ::= */
  {  215,   -2 }, /* (54) tseries ::= TSERIES INTEGER */
  {  216,    0 }, /* (55) dbs ::= */
  {  216,   -2 }, /* (56) dbs ::= DBS INTEGER */
  {  217,    0 }, /* (57) streams ::= */
  {  217,   -2 }, /* (58) streams ::= STREAMS INTEGER */
  {  218,    0 }, /* (59) storage ::= */
  {  218,   -2 }, /* (60) storage ::= STORAGE INTEGER */
  {  219,    0 }, /* (61) qtime ::= */
  {  219,   -2 }, /* (62) qtime ::= QTIME INTEGER */
  {  220,    0 }, /* (63) users ::= */
  {  220,   -2 }, /* (64) users ::= USERS INTEGER */
  {  221,    0 }, /* (65) conns ::= */
  {  221,   -2 }, /* (66) conns ::= CONNS INTEGER */
  {  222,    0 }, /* (67) state ::= */
  {  222,   -2 }, /* (68) state ::= STATE ids */
  {  211,   -9 }, /* (69) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  223,   -2 }, /* (70) keep ::= KEEP tagitemlist */
  {  225,   -2 }, /* (71) tables ::= TABLES INTEGER */
  {  226,   -2 }, /* (72) cache ::= CACHE INTEGER */
  {  227,   -2 }, /* (73) replica ::= REPLICA INTEGER */
  {  228,   -2 }, /* (74) days ::= DAYS INTEGER */
  {  229,   -2 }, /* (75) rows ::= ROWS INTEGER */
  {  230,   -2 }, /* (76) ablocks ::= ABLOCKS ID */
  {  231,   -2 }, /* (77) tblocks ::= TBLOCKS INTEGER */
  {  232,   -2 }, /* (78) ctime ::= CTIME INTEGER */
  {  233,   -2 }, /* (79) clog ::= CLOG INTEGER */
  {  234,   -2 }, /* (80) comp ::= COMP INTEGER */
  {  235,   -2 }, /* (81) prec ::= PRECISION STRING */
  {  213,    0 }, /* (82) db_optr ::= */
  {  213,   -2 }, /* (83) db_optr ::= db_optr tables */
  {  213,   -2 }, /* (84) db_optr ::= db_optr cache */
  {  213,   -2 }, /* (85) db_optr ::= db_optr replica */
  {  213,   -2 }, /* (86) db_optr ::= db_optr days */
  {  213,   -2 }, /* (87) db_optr ::= db_optr rows */
  {  213,   -2 }, /* (88) db_optr ::= db_optr ablocks */
  {  213,   -2 }, /* (89) db_optr ::= db_optr tblocks */
  {  213,   -2 }, /* (90) db_optr ::= db_optr ctime */
  {  213,   -2 }, /* (91) db_optr ::= db_optr clog */
  {  213,   -2 }, /* (92) db_optr ::= db_optr comp */
  {  213,   -2 }, /* (93) db_optr ::= db_optr prec */
  {  213,   -2 }, /* (94) db_optr ::= db_optr keep */
  {  210,    0 }, /* (95) alter_db_optr ::= */
  {  210,   -2 }, /* (96) alter_db_optr ::= alter_db_optr replica */
  {  210,   -2 }, /* (97) alter_db_optr ::= alter_db_optr tables */
  {  236,   -1 }, /* (98) typename ::= ids */
  {  236,   -4 }, /* (99) typename ::= ids LP signed RP */
  {  237,   -1 }, /* (100) signed ::= INTEGER */
  {  237,   -2 }, /* (101) signed ::= PLUS INTEGER */
  {  237,   -2 }, /* (102) signed ::= MINUS INTEGER */
  {  205,   -6 }, /* (103) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  238,   -3 }, /* (104) create_table_args ::= LP columnlist RP */
  {  238,   -7 }, /* (105) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  238,   -7 }, /* (106) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  238,   -2 }, /* (107) create_table_args ::= AS select */
  {  239,   -3 }, /* (108) columnlist ::= columnlist COMMA column */
  {  239,   -1 }, /* (109) columnlist ::= column */
  {  241,   -2 }, /* (110) column ::= ids typename */
  {  224,   -3 }, /* (111) tagitemlist ::= tagitemlist COMMA tagitem */
  {  224,   -1 }, /* (112) tagitemlist ::= tagitem */
  {  242,   -1 }, /* (113) tagitem ::= INTEGER */
  {  242,   -1 }, /* (114) tagitem ::= FLOAT */
  {  242,   -1 }, /* (115) tagitem ::= STRING */
  {  242,   -1 }, /* (116) tagitem ::= BOOL */
  {  242,   -1 }, /* (117) tagitem ::= NULL */
  {  242,   -2 }, /* (118) tagitem ::= MINUS INTEGER */
  {  242,   -2 }, /* (119) tagitem ::= MINUS FLOAT */
  {  242,   -2 }, /* (120) tagitem ::= PLUS INTEGER */
  {  242,   -2 }, /* (121) tagitem ::= PLUS FLOAT */
  {  240,  -12 }, /* (122) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  254,   -1 }, /* (123) union ::= select */
  {  254,   -3 }, /* (124) union ::= LP union RP */
  {  254,   -4 }, /* (125) union ::= union UNION ALL select */
  {  254,   -6 }, /* (126) union ::= union UNION ALL LP select RP */
  {  205,   -1 }, /* (127) cmd ::= union */
  {  240,   -2 }, /* (128) select ::= SELECT selcollist */
  {  255,   -2 }, /* (129) sclp ::= selcollist COMMA */
  {  255,    0 }, /* (130) sclp ::= */
  {  243,   -3 }, /* (131) selcollist ::= sclp expr as */
  {  243,   -2 }, /* (132) selcollist ::= sclp STAR */
  {  257,   -2 }, /* (133) as ::= AS ids */
  {  257,   -1 }, /* (134) as ::= ids */
  {  257,    0 }, /* (135) as ::= */
  {  244,   -2 }, /* (136) from ::= FROM tablelist */
  {  258,   -2 }, /* (137) tablelist ::= ids cpxName */
  {  258,   -4 }, /* (138) tablelist ::= tablelist COMMA ids cpxName */
  {  259,   -1 }, /* (139) tmvar ::= VARIABLE */
  {  246,   -4 }, /* (140) interval_opt ::= INTERVAL LP tmvar RP */
  {  246,    0 }, /* (141) interval_opt ::= */
  {  247,    0 }, /* (142) fill_opt ::= */
  {  247,   -6 }, /* (143) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  247,   -4 }, /* (144) fill_opt ::= FILL LP ID RP */
  {  248,   -4 }, /* (145) sliding_opt ::= SLIDING LP tmvar RP */
  {  248,    0 }, /* (146) sliding_opt ::= */
  {  250,    0 }, /* (147) orderby_opt ::= */
  {  250,   -3 }, /* (148) orderby_opt ::= ORDER BY sortlist */
  {  260,   -4 }, /* (149) sortlist ::= sortlist COMMA item sortorder */
  {  260,   -2 }, /* (150) sortlist ::= item sortorder */
  {  262,   -2 }, /* (151) item ::= ids cpxName */
  {  263,   -1 }, /* (152) sortorder ::= ASC */
  {  263,   -1 }, /* (153) sortorder ::= DESC */
  {  263,    0 }, /* (154) sortorder ::= */
  {  249,    0 }, /* (155) groupby_opt ::= */
  {  249,   -3 }, /* (156) groupby_opt ::= GROUP BY grouplist */
  {  264,   -3 }, /* (157) grouplist ::= grouplist COMMA item */
  {  264,   -1 }, /* (158) grouplist ::= item */
  {  251,    0 }, /* (159) having_opt ::= */
  {  251,   -2 }, /* (160) having_opt ::= HAVING expr */
  {  253,    0 }, /* (161) limit_opt ::= */
  {  253,   -2 }, /* (162) limit_opt ::= LIMIT signed */
  {  253,   -4 }, /* (163) limit_opt ::= LIMIT signed OFFSET signed */
  {  253,   -4 }, /* (164) limit_opt ::= LIMIT signed COMMA signed */
  {  252,    0 }, /* (165) slimit_opt ::= */
  {  252,   -2 }, /* (166) slimit_opt ::= SLIMIT signed */
  {  252,   -4 }, /* (167) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  252,   -4 }, /* (168) slimit_opt ::= SLIMIT signed COMMA signed */
  {  245,    0 }, /* (169) where_opt ::= */
  {  245,   -2 }, /* (170) where_opt ::= WHERE expr */
  {  256,   -3 }, /* (171) expr ::= LP expr RP */
  {  256,   -1 }, /* (172) expr ::= ID */
  {  256,   -3 }, /* (173) expr ::= ID DOT ID */
  {  256,   -3 }, /* (174) expr ::= ID DOT STAR */
  {  256,   -1 }, /* (175) expr ::= INTEGER */
  {  256,   -2 }, /* (176) expr ::= MINUS INTEGER */
  {  256,   -2 }, /* (177) expr ::= PLUS INTEGER */
  {  256,   -1 }, /* (178) expr ::= FLOAT */
  {  256,   -2 }, /* (179) expr ::= MINUS FLOAT */
  {  256,   -2 }, /* (180) expr ::= PLUS FLOAT */
  {  256,   -1 }, /* (181) expr ::= STRING */
  {  256,   -1 }, /* (182) expr ::= NOW */
  {  256,   -1 }, /* (183) expr ::= VARIABLE */
  {  256,   -1 }, /* (184) expr ::= BOOL */
  {  256,   -4 }, /* (185) expr ::= ID LP exprlist RP */
  {  256,   -4 }, /* (186) expr ::= ID LP STAR RP */
  {  256,   -3 }, /* (187) expr ::= expr AND expr */
  {  256,   -3 }, /* (188) expr ::= expr OR expr */
  {  256,   -3 }, /* (189) expr ::= expr LT expr */
  {  256,   -3 }, /* (190) expr ::= expr GT expr */
  {  256,   -3 }, /* (191) expr ::= expr LE expr */
  {  256,   -3 }, /* (192) expr ::= expr GE expr */
  {  256,   -3 }, /* (193) expr ::= expr NE expr */
  {  256,   -3 }, /* (194) expr ::= expr EQ expr */
  {  256,   -3 }, /* (195) expr ::= expr PLUS expr */
  {  256,   -3 }, /* (196) expr ::= expr MINUS expr */
  {  256,   -3 }, /* (197) expr ::= expr STAR expr */
  {  256,   -3 }, /* (198) expr ::= expr SLASH expr */
  {  256,   -3 }, /* (199) expr ::= expr REM expr */
  {  256,   -3 }, /* (200) expr ::= expr LIKE expr */
  {  256,   -5 }, /* (201) expr ::= expr IN LP exprlist RP */
  {  265,   -3 }, /* (202) exprlist ::= exprlist COMMA expritem */
  {  265,   -1 }, /* (203) exprlist ::= expritem */
  {  266,   -1 }, /* (204) expritem ::= expr */
  {  266,    0 }, /* (205) expritem ::= */
  {  205,   -3 }, /* (206) cmd ::= RESET QUERY CACHE */
  {  205,   -7 }, /* (207) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  205,   -7 }, /* (208) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  205,   -7 }, /* (209) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  205,   -7 }, /* (210) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  205,   -8 }, /* (211) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  205,   -9 }, /* (212) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  205,   -5 }, /* (213) cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
  {  205,   -7 }, /* (214) cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
  {  205,   -7 }, /* (215) cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
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
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy532, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy239);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy239);}
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
      case 47: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 48: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy239);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy532, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy239.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy239.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy239.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy239.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy239.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy239.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy239.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy239.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy239.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy239 = yylhsminor.yy239;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy30 = yymsp[0].minor.yy30; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy532);}
        break;
      case 83: /* db_optr ::= db_optr tables */
      case 97: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==97);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 84: /* db_optr ::= db_optr cache */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 85: /* db_optr ::= db_optr replica */
      case 96: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==96);
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 86: /* db_optr ::= db_optr days */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 87: /* db_optr ::= db_optr rows */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 88: /* db_optr ::= db_optr ablocks */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 89: /* db_optr ::= db_optr tblocks */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 90: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 91: /* db_optr ::= db_optr clog */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 92: /* db_optr ::= db_optr comp */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 93: /* db_optr ::= db_optr prec */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 94: /* db_optr ::= db_optr keep */
{ yylhsminor.yy532 = yymsp[-1].minor.yy532; yylhsminor.yy532.keep = yymsp[0].minor.yy30; }
  yymsp[-1].minor.yy532 = yylhsminor.yy532;
        break;
      case 95: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy532);}
        break;
      case 98: /* typename ::= ids */
{ tSQLSetColumnType (&yylhsminor.yy505, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy505 = yylhsminor.yy505;
        break;
      case 99: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy489;          // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy505, &yymsp[-3].minor.yy0);
}
  yymsp[-3].minor.yy505 = yylhsminor.yy505;
        break;
      case 100: /* signed ::= INTEGER */
{ yylhsminor.yy489 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy489 = yylhsminor.yy489;
        break;
      case 101: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy489 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 102: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy489 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 103: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 104: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy212 = tSetCreateSQLElems(yymsp[-1].minor.yy325, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy212, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 105: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy212 = tSetCreateSQLElems(yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy212, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 106: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy212 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy30, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy212, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 107: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy212 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy444, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy212, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 108: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy325 = tFieldListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy505);   }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 109: /* columnlist ::= column */
{yylhsminor.yy325 = tFieldListAppend(NULL, &yymsp[0].minor.yy505);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 110: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy505, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy505);
}
  yymsp[-1].minor.yy505 = yylhsminor.yy505;
        break;
      case 111: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy30 = tVariantListAppend(yymsp[-2].minor.yy30, &yymsp[0].minor.yy380, -1);    }
  yymsp[-2].minor.yy30 = yylhsminor.yy30;
        break;
      case 112: /* tagitemlist ::= tagitem */
{ yylhsminor.yy30 = tVariantListAppend(NULL, &yymsp[0].minor.yy380, -1); }
  yymsp[0].minor.yy30 = yylhsminor.yy30;
        break;
      case 113: /* tagitem ::= INTEGER */
      case 114: /* tagitem ::= FLOAT */ yytestcase(yyruleno==114);
      case 115: /* tagitem ::= STRING */ yytestcase(yyruleno==115);
      case 116: /* tagitem ::= BOOL */ yytestcase(yyruleno==116);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy380, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy380 = yylhsminor.yy380;
        break;
      case 117: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy380, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy380 = yylhsminor.yy380;
        break;
      case 118: /* tagitem ::= MINUS INTEGER */
      case 119: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==119);
      case 120: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==120);
      case 121: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==121);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy380, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy380 = yylhsminor.yy380;
        break;
      case 122: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy444 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy506, yymsp[-9].minor.yy30, yymsp[-8].minor.yy388, yymsp[-4].minor.yy30, yymsp[-3].minor.yy30, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy30, &yymsp[0].minor.yy150, &yymsp[-1].minor.yy150);
}
  yymsp[-11].minor.yy444 = yylhsminor.yy444;
        break;
      case 123: /* union ::= select */
{ yylhsminor.yy309 = setSubclause(NULL, yymsp[0].minor.yy444); }
  yymsp[0].minor.yy309 = yylhsminor.yy309;
        break;
      case 124: /* union ::= LP union RP */
{ yymsp[-2].minor.yy309 = yymsp[-1].minor.yy309; }
        break;
      case 125: /* union ::= union UNION ALL select */
{ yylhsminor.yy309 = appendSelectClause(yymsp[-3].minor.yy309, yymsp[0].minor.yy444); }
  yymsp[-3].minor.yy309 = yylhsminor.yy309;
        break;
      case 126: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy309 = appendSelectClause(yymsp[-5].minor.yy309, yymsp[-1].minor.yy444); }
  yymsp[-5].minor.yy309 = yylhsminor.yy309;
        break;
      case 127: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy309, NULL, TSDB_SQL_SELECT); }
        break;
      case 128: /* select ::= SELECT selcollist */
{
  yylhsminor.yy444 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy506, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy444 = yylhsminor.yy444;
        break;
      case 129: /* sclp ::= selcollist COMMA */
{yylhsminor.yy506 = yymsp[-1].minor.yy506;}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 130: /* sclp ::= */
{yymsp[1].minor.yy506 = 0;}
        break;
      case 131: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy506 = tSQLExprListAppend(yymsp[-2].minor.yy506, yymsp[-1].minor.yy388, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy506 = yylhsminor.yy506;
        break;
      case 132: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy506 = tSQLExprListAppend(yymsp[-1].minor.yy506, pNode, 0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
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
{yymsp[-1].minor.yy30 = yymsp[0].minor.yy30;}
        break;
      case 137: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy30 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy30 = yylhsminor.yy30;
        break;
      case 138: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy30 = tVariantListAppendToken(yymsp[-3].minor.yy30, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy30 = yylhsminor.yy30;
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
{yymsp[1].minor.yy30 = 0;     }
        break;
      case 143: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy30, &A, -1, 0);
    yymsp[-5].minor.yy30 = yymsp[-1].minor.yy30;
}
        break;
      case 144: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy30 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 147: /* orderby_opt ::= */
      case 155: /* groupby_opt ::= */ yytestcase(yyruleno==155);
{yymsp[1].minor.yy30 = 0;}
        break;
      case 148: /* orderby_opt ::= ORDER BY sortlist */
      case 156: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==156);
{yymsp[-2].minor.yy30 = yymsp[0].minor.yy30;}
        break;
      case 149: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy30 = tVariantListAppend(yymsp[-3].minor.yy30, &yymsp[-1].minor.yy380, yymsp[0].minor.yy250);
}
  yymsp[-3].minor.yy30 = yylhsminor.yy30;
        break;
      case 150: /* sortlist ::= item sortorder */
{
  yylhsminor.yy30 = tVariantListAppend(NULL, &yymsp[-1].minor.yy380, yymsp[0].minor.yy250);
}
  yymsp[-1].minor.yy30 = yylhsminor.yy30;
        break;
      case 151: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy380, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy380 = yylhsminor.yy380;
        break;
      case 152: /* sortorder ::= ASC */
{yymsp[0].minor.yy250 = TSDB_ORDER_ASC; }
        break;
      case 153: /* sortorder ::= DESC */
{yymsp[0].minor.yy250 = TSDB_ORDER_DESC;}
        break;
      case 154: /* sortorder ::= */
{yymsp[1].minor.yy250 = TSDB_ORDER_ASC;}
        break;
      case 157: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy30 = tVariantListAppend(yymsp[-2].minor.yy30, &yymsp[0].minor.yy380, -1);
}
  yymsp[-2].minor.yy30 = yylhsminor.yy30;
        break;
      case 158: /* grouplist ::= item */
{
  yylhsminor.yy30 = tVariantListAppend(NULL, &yymsp[0].minor.yy380, -1);
}
  yymsp[0].minor.yy30 = yylhsminor.yy30;
        break;
      case 159: /* having_opt ::= */
      case 169: /* where_opt ::= */ yytestcase(yyruleno==169);
      case 205: /* expritem ::= */ yytestcase(yyruleno==205);
{yymsp[1].minor.yy388 = 0;}
        break;
      case 160: /* having_opt ::= HAVING expr */
      case 170: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==170);
{yymsp[-1].minor.yy388 = yymsp[0].minor.yy388;}
        break;
      case 161: /* limit_opt ::= */
      case 165: /* slimit_opt ::= */ yytestcase(yyruleno==165);
{yymsp[1].minor.yy150.limit = -1; yymsp[1].minor.yy150.offset = 0;}
        break;
      case 162: /* limit_opt ::= LIMIT signed */
      case 166: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==166);
{yymsp[-1].minor.yy150.limit = yymsp[0].minor.yy489;  yymsp[-1].minor.yy150.offset = 0;}
        break;
      case 163: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 167: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==167);
{yymsp[-3].minor.yy150.limit = yymsp[-2].minor.yy489;  yymsp[-3].minor.yy150.offset = yymsp[0].minor.yy489;}
        break;
      case 164: /* limit_opt ::= LIMIT signed COMMA signed */
      case 168: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==168);
{yymsp[-3].minor.yy150.limit = yymsp[0].minor.yy489;  yymsp[-3].minor.yy150.offset = yymsp[-2].minor.yy489;}
        break;
      case 171: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy388 = yymsp[-1].minor.yy388; }
        break;
      case 172: /* expr ::= ID */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 173: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 174: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 175: /* expr ::= INTEGER */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 176: /* expr ::= MINUS INTEGER */
      case 177: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==177);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy388 = yylhsminor.yy388;
        break;
      case 178: /* expr ::= FLOAT */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 179: /* expr ::= MINUS FLOAT */
      case 180: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==180);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy388 = yylhsminor.yy388;
        break;
      case 181: /* expr ::= STRING */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 182: /* expr ::= NOW */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 183: /* expr ::= VARIABLE */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 184: /* expr ::= BOOL */
{yylhsminor.yy388 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 185: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy388 = tSQLExprCreateFunction(yymsp[-1].minor.yy506, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy388 = yylhsminor.yy388;
        break;
      case 186: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy388 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy388 = yylhsminor.yy388;
        break;
      case 187: /* expr ::= expr AND expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_AND);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 188: /* expr ::= expr OR expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_OR); }
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 189: /* expr ::= expr LT expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_LT);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 190: /* expr ::= expr GT expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_GT);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 191: /* expr ::= expr LE expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_LE);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 192: /* expr ::= expr GE expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_GE);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 193: /* expr ::= expr NE expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_NE);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 194: /* expr ::= expr EQ expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_EQ);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 195: /* expr ::= expr PLUS expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_PLUS);  }
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 196: /* expr ::= expr MINUS expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_MINUS); }
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 197: /* expr ::= expr STAR expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_STAR);  }
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 198: /* expr ::= expr SLASH expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_DIVIDE);}
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 199: /* expr ::= expr REM expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_REM);   }
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 200: /* expr ::= expr LIKE expr */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-2].minor.yy388, yymsp[0].minor.yy388, TK_LIKE);  }
  yymsp[-2].minor.yy388 = yylhsminor.yy388;
        break;
      case 201: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy388 = tSQLExprCreate(yymsp[-4].minor.yy388, (tSQLExpr*)yymsp[-1].minor.yy506, TK_IN); }
  yymsp[-4].minor.yy388 = yylhsminor.yy388;
        break;
      case 202: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy506 = tSQLExprListAppend(yymsp[-2].minor.yy506,yymsp[0].minor.yy388,0);}
  yymsp[-2].minor.yy506 = yylhsminor.yy506;
        break;
      case 203: /* exprlist ::= expritem */
{yylhsminor.yy506 = tSQLExprListAppend(0,yymsp[0].minor.yy388,0);}
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 204: /* expritem ::= expr */
{yylhsminor.yy388 = yymsp[0].minor.yy388;}
  yymsp[0].minor.yy388 = yylhsminor.yy388;
        break;
      case 206: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 207: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
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
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy380, -1);

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
