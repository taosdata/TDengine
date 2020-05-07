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
#define YYNOCODE 269
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tSQLExpr* yy2;
  tSQLExprList* yy10;
  TAOS_FIELD yy47;
  SCreateAcctSQL yy63;
  SSubclauseInfo* yy145;
  int yy196;
  SLimitVal yy230;
  int64_t yy373;
  SQuerySQL* yy392;
  tVariant yy442;
  tVariantList* yy456;
  SCreateDBInfo yy478;
  SCreateTableSQL* yy494;
  tFieldList* yy503;
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
#define YYNRULE              220
#define YYNTOKEN             204
#define YY_MAX_SHIFT         246
#define YY_MIN_SHIFTREDUCE   403
#define YY_MAX_SHIFTREDUCE   622
#define YY_ERROR_ACTION      623
#define YY_ACCEPT_ACTION     624
#define YY_NO_ACTION         625
#define YY_MIN_REDUCE        626
#define YY_MAX_REDUCE        845
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
#define YY_ACTTAB_COUNT (547)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   724,  444,  723,   11,  722,  134,  624,  246,  725,  445,
 /*    10 */   727,  726,  764,   41,   43,   21,   35,   36,  153,  244,
 /*    20 */   135,   29,  135,  444,  203,   39,   37,   40,   38,  158,
 /*    30 */   833,  445,  832,   34,   33,  139,  135,   32,   31,   30,
 /*    40 */    41,   43,  753,   35,   36,  157,  833,  166,   29,  739,
 /*    50 */   103,  203,   39,   37,   40,   38,  188,   21,  103,   99,
 /*    60 */    34,   33,  761,  155,   32,   31,   30,  404,  405,  406,
 /*    70 */   407,  408,  409,  410,  411,  412,  413,  414,  415,  245,
 /*    80 */   444,  742,   41,   43,  103,   35,   36,  103,  445,  168,
 /*    90 */    29,  738,   21,  203,   39,   37,   40,   38,   32,   31,
 /*   100 */    30,   56,   34,   33,  753,  787,   32,   31,   30,   43,
 /*   110 */   191,   35,   36,  788,  829,  198,   29,   21,  154,  203,
 /*   120 */    39,   37,   40,   38,  167,  578,  739,    8,   34,   33,
 /*   130 */    61,  113,   32,   31,   30,  665,   35,   36,  126,   59,
 /*   140 */   200,   29,   58,   17,  203,   39,   37,   40,   38,  221,
 /*   150 */    26,  739,  169,   34,   33,  220,  219,   32,   31,   30,
 /*   160 */    16,  239,  214,  238,  213,  212,  211,  237,  210,  236,
 /*   170 */   235,  209,  720,  828,  709,  710,  711,  712,  713,  714,
 /*   180 */   715,  716,  717,  718,  719,  162,  591,  225,  234,  582,
 /*   190 */   165,  585,  240,  588,   76,  162,  591,   98,  827,  582,
 /*   200 */   234,  585,   60,  588,   26,  162,  591,   12,  742,  582,
 /*   210 */   742,  585,  151,  588,   27,   21,  740,  159,  160,   34,
 /*   220 */    33,  202,  531,   32,   31,   30,  148,  159,  160,  190,
 /*   230 */   536,  539,   88,   87,  142,   18,  674,  159,  160,  126,
 /*   240 */   147,  177,  152,   39,   37,   40,   38,  226,  185,  739,
 /*   250 */   182,   34,   33,  559,  560,   32,   31,   30,  523,  666,
 /*   260 */    17,  520,  126,  521,  842,  522,  550,   26,   16,  239,
 /*   270 */    46,  238,  243,  242,   95,  237,  551,  236,  235,  608,
 /*   280 */    14,   42,  584,   13,  587,  140,  583,  187,  586,  170,
 /*   290 */   171,   42,  590,   50,  150,   47,   74,   78,   83,   86,
 /*   300 */    77,   42,  590,  580,  592,  507,   80,  589,   13,  161,
 /*   310 */    51,  527,  590,  528,   48,  513,  525,  589,  526,   46,
 /*   320 */   141,  116,  117,   68,   64,   67,  741,  589,  130,  128,
 /*   330 */    91,   90,   89,  223,  222,  143,  512,  207,  144,  581,
 /*   340 */    22,   22,   73,   72,   85,   84,  145,    3,   10,    9,
 /*   350 */   146,  137,  798,  133,  138,  136,  797,  163,  755,  524,
 /*   360 */   733,  794,  763,  793,  164,  224,  100,  780,  779,  114,
 /*   370 */   115,   26,  676,  208,  112,  131,  189,   24,  217,  673,
 /*   380 */   218,  841,   70,  840,  838,  118,   93,  694,   25,   52,
 /*   390 */    23,  546,  192,  132,  196,  663,   79,  661,   81,  752,
 /*   400 */    82,  104,   49,  659,  658,  172,  127,  656,  655,  654,
 /*   410 */   653,  652,   44,  644,  199,  129,  650,  648,  646,  201,
 /*   420 */   767,  197,  768,  781,  195,  193,   28,  216,   75,  227,
 /*   430 */   228,  229,  230,  231,  205,  232,   53,  233,  241,  622,
 /*   440 */   149,  173,   62,   65,  174,  175,  657,  176,  621,  179,
 /*   450 */   178,  180,  651,  121,   92,  120,  695,  119,   94,  123,
 /*   460 */   122,  124,  125,    1,    2,  737,  181,  620,  105,  108,
 /*   470 */   106,  109,  107,  110,  111,  183,  184,  613,  186,  190,
 /*   480 */   533,   55,  547,  101,  156,   57,  552,   19,  194,  102,
 /*   490 */     5,    6,  593,    4,   15,   20,    7,  204,   63,  206,
 /*   500 */   484,  481,  479,  478,  477,  475,  448,  215,   66,   45,
 /*   510 */   509,   22,  508,  506,   54,   69,  469,  467,  459,  465,
 /*   520 */   461,  463,  457,   71,  455,  483,  482,  480,  476,  474,
 /*   530 */    46,  446,  419,  417,  626,  625,  625,  625,  625,   96,
 /*   540 */   625,  625,  625,  625,  625,  625,   97,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   224,    1,  226,  257,  228,  257,  205,  206,  232,    9,
 /*    10 */   234,  235,  208,   13,   14,  208,   16,   17,  207,  208,
 /*    20 */   257,   21,  257,    1,   24,   25,   26,   27,   28,  266,
 /*    30 */   267,    9,  267,   33,   34,  257,  257,   37,   38,   39,
 /*    40 */    13,   14,  241,   16,   17,  266,  267,  240,   21,  242,
 /*    50 */   208,   24,   25,   26,   27,   28,  255,  208,  208,  208,
 /*    60 */    33,   34,  258,  225,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */     1,  243,   13,   14,  208,   16,   17,  208,    9,   63,
 /*    90 */    21,  242,  208,   24,   25,   26,   27,   28,   37,   38,
 /*   100 */    39,  101,   33,   34,  241,  263,   37,   38,   39,   14,
 /*   110 */   259,   16,   17,  263,  257,  265,   21,  208,  255,   24,
 /*   120 */    25,   26,   27,   28,  240,   98,  242,   97,   33,   34,
 /*   130 */   100,  101,   37,   38,   39,  212,   16,   17,  215,  263,
 /*   140 */   261,   21,  263,   97,   24,   25,   26,   27,   28,  240,
 /*   150 */   104,  242,  126,   33,   34,  129,  130,   37,   38,   39,
 /*   160 */    85,   86,   87,   88,   89,   90,   91,   92,   93,   94,
 /*   170 */    95,   96,  224,  257,  226,  227,  228,  229,  230,  231,
 /*   180 */   232,  233,  234,  235,  236,    1,    2,  208,   78,    5,
 /*   190 */   225,    7,  225,    9,   72,    1,    2,   97,  257,    5,
 /*   200 */    78,    7,  244,    9,  104,    1,    2,   44,  243,    5,
 /*   210 */   243,    7,  257,    9,  256,  208,  237,   33,   34,   33,
 /*   220 */    34,   37,   98,   37,   38,   39,   63,   33,   34,  105,
 /*   230 */   102,   37,   69,   70,   71,  107,  212,   33,   34,  215,
 /*   240 */    77,  125,  257,   25,   26,   27,   28,  240,  132,  242,
 /*   250 */   134,   33,   34,  114,  115,   37,   38,   39,    2,  212,
 /*   260 */    97,    5,  215,    7,  243,    9,   98,  104,   85,   86,
 /*   270 */   102,   88,   60,   61,   62,   92,   98,   94,   95,   98,
 /*   280 */   102,   97,    5,  102,    7,  257,    5,  124,    7,   33,
 /*   290 */    34,   97,  108,  102,  131,  102,   64,   65,   66,   67,
 /*   300 */    68,   97,  108,    1,   98,    5,   74,  123,  102,   59,
 /*   310 */   119,    5,  108,    7,  121,   98,    5,  123,    7,  102,
 /*   320 */   257,   64,   65,   66,   67,   68,  243,  123,   64,   65,
 /*   330 */    66,   67,   68,   33,   34,  257,   98,   98,  257,   37,
 /*   340 */   102,  102,  127,  128,   72,   73,  257,   97,  127,  128,
 /*   350 */   257,  257,  238,  257,  257,  257,  238,  238,  241,  103,
 /*   360 */   239,  238,  208,  238,  238,  238,  208,  264,  264,  208,
 /*   370 */   208,  104,  208,  208,  245,  208,  241,  208,  208,  208,
 /*   380 */   208,  208,  208,  208,  208,  208,   59,  208,  208,  118,
 /*   390 */   208,  108,  260,  208,  260,  208,  208,  208,  208,  254,
 /*   400 */   208,  253,  120,  208,  208,  208,  208,  208,  208,  208,
 /*   410 */   208,  208,  117,  208,  116,  208,  208,  208,  208,  112,
 /*   420 */   209,  111,  209,  209,  110,  109,  122,   75,   84,   83,
 /*   430 */    49,   80,   82,   53,  209,   81,  209,   79,   75,    5,
 /*   440 */   209,  133,  213,  213,    5,  133,  209,   58,    5,    5,
 /*   450 */   133,  133,  209,  217,  210,  221,  223,  222,  210,  218,
 /*   460 */   220,  219,  216,  214,  211,  241,   58,    5,  252,  249,
 /*   470 */   251,  248,  250,  247,  246,  133,   58,   87,  125,  105,
 /*   480 */    98,  106,   98,   97,    1,  102,   98,  102,   97,   97,
 /*   490 */   113,  113,   98,   97,   97,  102,   97,   99,   72,   99,
 /*   500 */     9,    5,    5,    5,    5,    5,   76,   15,   72,   16,
 /*   510 */     5,  102,    5,   98,   97,  128,    5,    5,    5,    5,
 /*   520 */     5,    5,    5,  128,    5,    5,    5,    5,    5,    5,
 /*   530 */   102,   76,   59,   58,    0,  268,  268,  268,  268,   21,
 /*   540 */   268,  268,  268,  268,  268,  268,   21,  268,  268,  268,
 /*   550 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   560 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   570 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   580 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   590 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   600 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   610 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   620 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   630 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   640 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   650 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   660 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   670 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   680 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   690 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   700 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   710 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   720 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   730 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   740 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   750 */   268,
};
#define YY_SHIFT_COUNT    (246)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (534)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   163,   75,  183,  184,  204,   79,   79,   79,   79,   79,
 /*    10 */    79,    0,   22,  204,  256,  256,  256,   46,   79,   79,
 /*    20 */    79,   79,   79,  122,  110,  110,  547,  194,  204,  204,
 /*    30 */   204,  204,  204,  204,  204,  204,  204,  204,  204,  204,
 /*    40 */   204,  204,  204,  204,  204,  256,  256,  300,  300,  300,
 /*    50 */   300,  300,  300,   30,  300,  100,   79,   79,  139,  139,
 /*    60 */   128,   79,   79,   79,   79,   79,   79,   79,   79,   79,
 /*    70 */    79,   79,   79,   79,   79,   79,   79,   79,   79,   79,
 /*    80 */    79,   79,   79,   79,   79,   79,   79,   79,   79,   79,
 /*    90 */    79,   79,   79,   79,   79,   79,   79,   79,  267,  327,
 /*   100 */   327,  283,  283,  327,  271,  282,  295,  307,  298,  310,
 /*   110 */   314,  316,  304,  267,  327,  327,  352,  352,  327,  344,
 /*   120 */   346,  381,  351,  350,  380,  354,  358,  327,  363,  327,
 /*   130 */   363,  547,  547,   27,   69,   69,   69,   95,  120,  218,
 /*   140 */   218,  218,  232,  186,  186,  186,  186,  257,  264,   26,
 /*   150 */   116,   61,   61,  212,  124,  168,  178,  181,  206,  277,
 /*   160 */   281,  302,  250,  193,  191,  217,  238,  239,  215,  221,
 /*   170 */   306,  311,  272,  434,  308,  439,  312,  389,  443,  317,
 /*   180 */   444,  318,  408,  462,  342,  418,  390,  353,  374,  382,
 /*   190 */   375,  383,  384,  386,  483,  391,  388,  392,  385,  377,
 /*   200 */   393,  378,  394,  396,  397,  398,  399,  400,  426,  491,
 /*   210 */   496,  497,  498,  499,  500,  430,  492,  436,  493,  387,
 /*   220 */   395,  409,  505,  507,  415,  417,  409,  511,  512,  513,
 /*   230 */   514,  515,  516,  517,  519,  520,  521,  522,  523,  524,
 /*   240 */   428,  455,  518,  525,  473,  475,  534,
};
#define YY_REDUCE_COUNT (132)
#define YY_REDUCE_MIN   (-254)
#define YY_REDUCE_MAX   (253)
static const short yy_reduce_ofst[] = {
 /*     0 */  -199,  -52, -224, -237, -221, -150, -121, -193, -116,  -91,
 /*    10 */     7, -196, -189, -235, -162,  -35,  -33, -137, -149, -158,
 /*    20 */  -124,  -21, -151,  -77,   24,   47,  -42, -254, -252, -222,
 /*    30 */  -143,  -84,  -59,  -45,  -15,   28,   63,   78,   81,   89,
 /*    40 */    93,   94,   96,   97,   98,   21,   83,  114,  118,  119,
 /*    50 */   123,  125,  126,  121,  127,  117,  154,  158,  103,  104,
 /*    60 */   129,  161,  162,  164,  165,  167,  169,  170,  171,  172,
 /*    70 */   173,  174,  175,  176,  177,  179,  180,  182,  185,  187,
 /*    80 */   188,  189,  190,  192,  195,  196,  197,  198,  199,  200,
 /*    90 */   201,  202,  203,  205,  207,  208,  209,  210,  135,  211,
 /*   100 */   213,  132,  134,  214,  145,  148,  216,  219,  222,  220,
 /*   110 */   223,  226,  228,  224,  225,  227,  229,  230,  231,  233,
 /*   120 */   235,  234,  236,  240,  241,  242,  246,  237,  244,  243,
 /*   130 */   248,  249,  253,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   623,  675,  664,  835,  835,  623,  623,  623,  623,  623,
 /*    10 */   623,  765,  641,  835,  623,  623,  623,  623,  623,  623,
 /*    20 */   623,  623,  623,  677,  677,  677,  760,  623,  623,  623,
 /*    30 */   623,  623,  623,  623,  623,  623,  623,  623,  623,  623,
 /*    40 */   623,  623,  623,  623,  623,  623,  623,  623,  623,  623,
 /*    50 */   623,  623,  623,  623,  623,  623,  623,  623,  784,  784,
 /*    60 */   758,  623,  623,  623,  623,  623,  623,  623,  623,  623,
 /*    70 */   623,  623,  623,  623,  623,  623,  623,  623,  623,  662,
 /*    80 */   623,  660,  623,  623,  623,  623,  623,  623,  623,  623,
 /*    90 */   623,  623,  623,  623,  623,  649,  623,  623,  623,  643,
 /*   100 */   643,  623,  623,  643,  791,  795,  789,  777,  785,  776,
 /*   110 */   772,  771,  799,  623,  643,  643,  672,  672,  643,  693,
 /*   120 */   691,  689,  681,  687,  683,  685,  679,  643,  670,  643,
 /*   130 */   670,  708,  721,  623,  800,  834,  790,  818,  817,  830,
 /*   140 */   824,  823,  623,  822,  821,  820,  819,  623,  623,  623,
 /*   150 */   623,  826,  825,  623,  623,  623,  623,  623,  623,  623,
 /*   160 */   623,  623,  802,  796,  792,  623,  623,  623,  623,  623,
 /*   170 */   623,  623,  623,  623,  623,  623,  623,  623,  623,  623,
 /*   180 */   623,  623,  623,  623,  623,  623,  623,  623,  757,  623,
 /*   190 */   623,  766,  623,  623,  623,  623,  623,  623,  786,  623,
 /*   200 */   778,  623,  623,  623,  623,  623,  623,  734,  623,  623,
 /*   210 */   623,  623,  623,  623,  623,  623,  623,  623,  623,  623,
 /*   220 */   623,  839,  623,  623,  623,  728,  837,  623,  623,  623,
 /*   230 */   623,  623,  623,  623,  623,  623,  623,  623,  623,  623,
 /*   240 */   696,  623,  647,  645,  623,  639,  623,
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
    0,  /*  MAXTABLES => nothing */
    0,  /*      CACHE => nothing */
    0,  /*    REPLICA => nothing */
    0,  /*       DAYS => nothing */
    0,  /*    MINROWS => nothing */
    0,  /*    MAXROWS => nothing */
    0,  /*     BLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*        WAL => nothing */
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
  /*   86 */ "MAXTABLES",
  /*   87 */ "CACHE",
  /*   88 */ "REPLICA",
  /*   89 */ "DAYS",
  /*   90 */ "MINROWS",
  /*   91 */ "MAXROWS",
  /*   92 */ "BLOCKS",
  /*   93 */ "CTIME",
  /*   94 */ "WAL",
  /*   95 */ "COMP",
  /*   96 */ "PRECISION",
  /*   97 */ "LP",
  /*   98 */ "RP",
  /*   99 */ "TAGS",
  /*  100 */ "USING",
  /*  101 */ "AS",
  /*  102 */ "COMMA",
  /*  103 */ "NULL",
  /*  104 */ "SELECT",
  /*  105 */ "UNION",
  /*  106 */ "ALL",
  /*  107 */ "FROM",
  /*  108 */ "VARIABLE",
  /*  109 */ "INTERVAL",
  /*  110 */ "FILL",
  /*  111 */ "SLIDING",
  /*  112 */ "ORDER",
  /*  113 */ "BY",
  /*  114 */ "ASC",
  /*  115 */ "DESC",
  /*  116 */ "GROUP",
  /*  117 */ "HAVING",
  /*  118 */ "LIMIT",
  /*  119 */ "OFFSET",
  /*  120 */ "SLIMIT",
  /*  121 */ "SOFFSET",
  /*  122 */ "WHERE",
  /*  123 */ "NOW",
  /*  124 */ "RESET",
  /*  125 */ "QUERY",
  /*  126 */ "ADD",
  /*  127 */ "COLUMN",
  /*  128 */ "TAG",
  /*  129 */ "CHANGE",
  /*  130 */ "SET",
  /*  131 */ "KILL",
  /*  132 */ "CONNECTION",
  /*  133 */ "COLON",
  /*  134 */ "STREAM",
  /*  135 */ "ABORT",
  /*  136 */ "AFTER",
  /*  137 */ "ATTACH",
  /*  138 */ "BEFORE",
  /*  139 */ "BEGIN",
  /*  140 */ "CASCADE",
  /*  141 */ "CLUSTER",
  /*  142 */ "CONFLICT",
  /*  143 */ "COPY",
  /*  144 */ "DEFERRED",
  /*  145 */ "DELIMITERS",
  /*  146 */ "DETACH",
  /*  147 */ "EACH",
  /*  148 */ "END",
  /*  149 */ "EXPLAIN",
  /*  150 */ "FAIL",
  /*  151 */ "FOR",
  /*  152 */ "IGNORE",
  /*  153 */ "IMMEDIATE",
  /*  154 */ "INITIALLY",
  /*  155 */ "INSTEAD",
  /*  156 */ "MATCH",
  /*  157 */ "KEY",
  /*  158 */ "OF",
  /*  159 */ "RAISE",
  /*  160 */ "REPLACE",
  /*  161 */ "RESTRICT",
  /*  162 */ "ROW",
  /*  163 */ "STATEMENT",
  /*  164 */ "TRIGGER",
  /*  165 */ "VIEW",
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
  /*  185 */ "RATE",
  /*  186 */ "IRATE",
  /*  187 */ "SUM_RATE",
  /*  188 */ "SUM_IRATE",
  /*  189 */ "AVG_RATE",
  /*  190 */ "AVG_IRATE",
  /*  191 */ "SEMI",
  /*  192 */ "NONE",
  /*  193 */ "PREV",
  /*  194 */ "LINEAR",
  /*  195 */ "IMPORT",
  /*  196 */ "METRIC",
  /*  197 */ "TBNAME",
  /*  198 */ "JOIN",
  /*  199 */ "METRICS",
  /*  200 */ "STABLE",
  /*  201 */ "INSERT",
  /*  202 */ "INTO",
  /*  203 */ "VALUES",
  /*  204 */ "error",
  /*  205 */ "program",
  /*  206 */ "cmd",
  /*  207 */ "dbPrefix",
  /*  208 */ "ids",
  /*  209 */ "cpxName",
  /*  210 */ "ifexists",
  /*  211 */ "alter_db_optr",
  /*  212 */ "acct_optr",
  /*  213 */ "ifnotexists",
  /*  214 */ "db_optr",
  /*  215 */ "pps",
  /*  216 */ "tseries",
  /*  217 */ "dbs",
  /*  218 */ "streams",
  /*  219 */ "storage",
  /*  220 */ "qtime",
  /*  221 */ "users",
  /*  222 */ "conns",
  /*  223 */ "state",
  /*  224 */ "keep",
  /*  225 */ "tagitemlist",
  /*  226 */ "tables",
  /*  227 */ "cache",
  /*  228 */ "replica",
  /*  229 */ "days",
  /*  230 */ "minrows",
  /*  231 */ "maxrows",
  /*  232 */ "blocks",
  /*  233 */ "ctime",
  /*  234 */ "wal",
  /*  235 */ "comp",
  /*  236 */ "prec",
  /*  237 */ "typename",
  /*  238 */ "signed",
  /*  239 */ "create_table_args",
  /*  240 */ "columnlist",
  /*  241 */ "select",
  /*  242 */ "column",
  /*  243 */ "tagitem",
  /*  244 */ "selcollist",
  /*  245 */ "from",
  /*  246 */ "where_opt",
  /*  247 */ "interval_opt",
  /*  248 */ "fill_opt",
  /*  249 */ "sliding_opt",
  /*  250 */ "groupby_opt",
  /*  251 */ "orderby_opt",
  /*  252 */ "having_opt",
  /*  253 */ "slimit_opt",
  /*  254 */ "limit_opt",
  /*  255 */ "union",
  /*  256 */ "sclp",
  /*  257 */ "expr",
  /*  258 */ "as",
  /*  259 */ "tablelist",
  /*  260 */ "tmvar",
  /*  261 */ "sortlist",
  /*  262 */ "sortitem",
  /*  263 */ "item",
  /*  264 */ "sortorder",
  /*  265 */ "grouplist",
  /*  266 */ "exprlist",
  /*  267 */ "expritem",
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
 /*  71 */ "tables ::= MAXTABLES INTEGER",
 /*  72 */ "cache ::= CACHE INTEGER",
 /*  73 */ "replica ::= REPLICA INTEGER",
 /*  74 */ "days ::= DAYS INTEGER",
 /*  75 */ "minrows ::= MINROWS INTEGER",
 /*  76 */ "maxrows ::= MAXROWS INTEGER",
 /*  77 */ "blocks ::= BLOCKS INTEGER",
 /*  78 */ "ctime ::= CTIME INTEGER",
 /*  79 */ "wal ::= WAL INTEGER",
 /*  80 */ "comp ::= COMP INTEGER",
 /*  81 */ "prec ::= PRECISION STRING",
 /*  82 */ "db_optr ::=",
 /*  83 */ "db_optr ::= db_optr tables",
 /*  84 */ "db_optr ::= db_optr cache",
 /*  85 */ "db_optr ::= db_optr replica",
 /*  86 */ "db_optr ::= db_optr days",
 /*  87 */ "db_optr ::= db_optr minrows",
 /*  88 */ "db_optr ::= db_optr maxrows",
 /*  89 */ "db_optr ::= db_optr blocks",
 /*  90 */ "db_optr ::= db_optr ctime",
 /*  91 */ "db_optr ::= db_optr wal",
 /*  92 */ "db_optr ::= db_optr comp",
 /*  93 */ "db_optr ::= db_optr prec",
 /*  94 */ "db_optr ::= db_optr keep",
 /*  95 */ "alter_db_optr ::=",
 /*  96 */ "alter_db_optr ::= alter_db_optr replica",
 /*  97 */ "alter_db_optr ::= alter_db_optr tables",
 /*  98 */ "alter_db_optr ::= alter_db_optr keep",
 /*  99 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 100 */ "alter_db_optr ::= alter_db_optr comp",
 /* 101 */ "alter_db_optr ::= alter_db_optr wal",
 /* 102 */ "typename ::= ids",
 /* 103 */ "typename ::= ids LP signed RP",
 /* 104 */ "signed ::= INTEGER",
 /* 105 */ "signed ::= PLUS INTEGER",
 /* 106 */ "signed ::= MINUS INTEGER",
 /* 107 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 108 */ "create_table_args ::= LP columnlist RP",
 /* 109 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 110 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 111 */ "create_table_args ::= AS select",
 /* 112 */ "columnlist ::= columnlist COMMA column",
 /* 113 */ "columnlist ::= column",
 /* 114 */ "column ::= ids typename",
 /* 115 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 116 */ "tagitemlist ::= tagitem",
 /* 117 */ "tagitem ::= INTEGER",
 /* 118 */ "tagitem ::= FLOAT",
 /* 119 */ "tagitem ::= STRING",
 /* 120 */ "tagitem ::= BOOL",
 /* 121 */ "tagitem ::= NULL",
 /* 122 */ "tagitem ::= MINUS INTEGER",
 /* 123 */ "tagitem ::= MINUS FLOAT",
 /* 124 */ "tagitem ::= PLUS INTEGER",
 /* 125 */ "tagitem ::= PLUS FLOAT",
 /* 126 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 127 */ "union ::= select",
 /* 128 */ "union ::= LP union RP",
 /* 129 */ "union ::= union UNION ALL select",
 /* 130 */ "union ::= union UNION ALL LP select RP",
 /* 131 */ "cmd ::= union",
 /* 132 */ "select ::= SELECT selcollist",
 /* 133 */ "sclp ::= selcollist COMMA",
 /* 134 */ "sclp ::=",
 /* 135 */ "selcollist ::= sclp expr as",
 /* 136 */ "selcollist ::= sclp STAR",
 /* 137 */ "as ::= AS ids",
 /* 138 */ "as ::= ids",
 /* 139 */ "as ::=",
 /* 140 */ "from ::= FROM tablelist",
 /* 141 */ "tablelist ::= ids cpxName",
 /* 142 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 143 */ "tmvar ::= VARIABLE",
 /* 144 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 145 */ "interval_opt ::=",
 /* 146 */ "fill_opt ::=",
 /* 147 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 148 */ "fill_opt ::= FILL LP ID RP",
 /* 149 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 150 */ "sliding_opt ::=",
 /* 151 */ "orderby_opt ::=",
 /* 152 */ "orderby_opt ::= ORDER BY sortlist",
 /* 153 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 154 */ "sortlist ::= item sortorder",
 /* 155 */ "item ::= ids cpxName",
 /* 156 */ "sortorder ::= ASC",
 /* 157 */ "sortorder ::= DESC",
 /* 158 */ "sortorder ::=",
 /* 159 */ "groupby_opt ::=",
 /* 160 */ "groupby_opt ::= GROUP BY grouplist",
 /* 161 */ "grouplist ::= grouplist COMMA item",
 /* 162 */ "grouplist ::= item",
 /* 163 */ "having_opt ::=",
 /* 164 */ "having_opt ::= HAVING expr",
 /* 165 */ "limit_opt ::=",
 /* 166 */ "limit_opt ::= LIMIT signed",
 /* 167 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 168 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 169 */ "slimit_opt ::=",
 /* 170 */ "slimit_opt ::= SLIMIT signed",
 /* 171 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 172 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 173 */ "where_opt ::=",
 /* 174 */ "where_opt ::= WHERE expr",
 /* 175 */ "expr ::= LP expr RP",
 /* 176 */ "expr ::= ID",
 /* 177 */ "expr ::= ID DOT ID",
 /* 178 */ "expr ::= ID DOT STAR",
 /* 179 */ "expr ::= INTEGER",
 /* 180 */ "expr ::= MINUS INTEGER",
 /* 181 */ "expr ::= PLUS INTEGER",
 /* 182 */ "expr ::= FLOAT",
 /* 183 */ "expr ::= MINUS FLOAT",
 /* 184 */ "expr ::= PLUS FLOAT",
 /* 185 */ "expr ::= STRING",
 /* 186 */ "expr ::= NOW",
 /* 187 */ "expr ::= VARIABLE",
 /* 188 */ "expr ::= BOOL",
 /* 189 */ "expr ::= ID LP exprlist RP",
 /* 190 */ "expr ::= ID LP STAR RP",
 /* 191 */ "expr ::= expr AND expr",
 /* 192 */ "expr ::= expr OR expr",
 /* 193 */ "expr ::= expr LT expr",
 /* 194 */ "expr ::= expr GT expr",
 /* 195 */ "expr ::= expr LE expr",
 /* 196 */ "expr ::= expr GE expr",
 /* 197 */ "expr ::= expr NE expr",
 /* 198 */ "expr ::= expr EQ expr",
 /* 199 */ "expr ::= expr PLUS expr",
 /* 200 */ "expr ::= expr MINUS expr",
 /* 201 */ "expr ::= expr STAR expr",
 /* 202 */ "expr ::= expr SLASH expr",
 /* 203 */ "expr ::= expr REM expr",
 /* 204 */ "expr ::= expr LIKE expr",
 /* 205 */ "expr ::= expr IN LP exprlist RP",
 /* 206 */ "exprlist ::= exprlist COMMA expritem",
 /* 207 */ "exprlist ::= expritem",
 /* 208 */ "expritem ::= expr",
 /* 209 */ "expritem ::=",
 /* 210 */ "cmd ::= RESET QUERY CACHE",
 /* 211 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 212 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 213 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 214 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 215 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 216 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 217 */ "cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER",
 /* 218 */ "cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER",
 /* 219 */ "cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER",
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
    case 224: /* keep */
    case 225: /* tagitemlist */
    case 248: /* fill_opt */
    case 250: /* groupby_opt */
    case 251: /* orderby_opt */
    case 261: /* sortlist */
    case 265: /* grouplist */
{
tVariantListDestroy((yypminor->yy456));
}
      break;
    case 240: /* columnlist */
{
tFieldListDestroy((yypminor->yy503));
}
      break;
    case 241: /* select */
{
doDestroyQuerySql((yypminor->yy392));
}
      break;
    case 244: /* selcollist */
    case 256: /* sclp */
    case 266: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy10));
}
      break;
    case 246: /* where_opt */
    case 252: /* having_opt */
    case 257: /* expr */
    case 267: /* expritem */
{
tSQLExprDestroy((yypminor->yy2));
}
      break;
    case 255: /* union */
{
destroyAllSelectClause((yypminor->yy145));
}
      break;
    case 262: /* sortitem */
{
tVariantDestroy(&(yypminor->yy442));
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
  {  205,   -1 }, /* (0) program ::= cmd */
  {  206,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  206,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  206,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  206,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  206,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  206,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  206,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  206,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  206,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  206,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  206,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  206,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  206,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  206,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  207,    0 }, /* (15) dbPrefix ::= */
  {  207,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  209,    0 }, /* (17) cpxName ::= */
  {  209,   -2 }, /* (18) cpxName ::= DOT ids */
  {  206,   -3 }, /* (19) cmd ::= SHOW dbPrefix TABLES */
  {  206,   -5 }, /* (20) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  206,   -3 }, /* (21) cmd ::= SHOW dbPrefix STABLES */
  {  206,   -5 }, /* (22) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  206,   -3 }, /* (23) cmd ::= SHOW dbPrefix VGROUPS */
  {  206,   -4 }, /* (24) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  206,   -5 }, /* (25) cmd ::= DROP TABLE ifexists ids cpxName */
  {  206,   -4 }, /* (26) cmd ::= DROP DATABASE ifexists ids */
  {  206,   -3 }, /* (27) cmd ::= DROP DNODE ids */
  {  206,   -3 }, /* (28) cmd ::= DROP USER ids */
  {  206,   -3 }, /* (29) cmd ::= DROP ACCOUNT ids */
  {  206,   -2 }, /* (30) cmd ::= USE ids */
  {  206,   -3 }, /* (31) cmd ::= DESCRIBE ids cpxName */
  {  206,   -5 }, /* (32) cmd ::= ALTER USER ids PASS ids */
  {  206,   -5 }, /* (33) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  206,   -4 }, /* (34) cmd ::= ALTER DNODE ids ids */
  {  206,   -5 }, /* (35) cmd ::= ALTER DNODE ids ids ids */
  {  206,   -3 }, /* (36) cmd ::= ALTER LOCAL ids */
  {  206,   -4 }, /* (37) cmd ::= ALTER LOCAL ids ids */
  {  206,   -4 }, /* (38) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  206,   -4 }, /* (39) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  206,   -6 }, /* (40) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  208,   -1 }, /* (41) ids ::= ID */
  {  208,   -1 }, /* (42) ids ::= STRING */
  {  210,   -2 }, /* (43) ifexists ::= IF EXISTS */
  {  210,    0 }, /* (44) ifexists ::= */
  {  213,   -3 }, /* (45) ifnotexists ::= IF NOT EXISTS */
  {  213,    0 }, /* (46) ifnotexists ::= */
  {  206,   -3 }, /* (47) cmd ::= CREATE DNODE ids */
  {  206,   -6 }, /* (48) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  206,   -5 }, /* (49) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  206,   -5 }, /* (50) cmd ::= CREATE USER ids PASS ids */
  {  215,    0 }, /* (51) pps ::= */
  {  215,   -2 }, /* (52) pps ::= PPS INTEGER */
  {  216,    0 }, /* (53) tseries ::= */
  {  216,   -2 }, /* (54) tseries ::= TSERIES INTEGER */
  {  217,    0 }, /* (55) dbs ::= */
  {  217,   -2 }, /* (56) dbs ::= DBS INTEGER */
  {  218,    0 }, /* (57) streams ::= */
  {  218,   -2 }, /* (58) streams ::= STREAMS INTEGER */
  {  219,    0 }, /* (59) storage ::= */
  {  219,   -2 }, /* (60) storage ::= STORAGE INTEGER */
  {  220,    0 }, /* (61) qtime ::= */
  {  220,   -2 }, /* (62) qtime ::= QTIME INTEGER */
  {  221,    0 }, /* (63) users ::= */
  {  221,   -2 }, /* (64) users ::= USERS INTEGER */
  {  222,    0 }, /* (65) conns ::= */
  {  222,   -2 }, /* (66) conns ::= CONNS INTEGER */
  {  223,    0 }, /* (67) state ::= */
  {  223,   -2 }, /* (68) state ::= STATE ids */
  {  212,   -9 }, /* (69) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  224,   -2 }, /* (70) keep ::= KEEP tagitemlist */
  {  226,   -2 }, /* (71) tables ::= MAXTABLES INTEGER */
  {  227,   -2 }, /* (72) cache ::= CACHE INTEGER */
  {  228,   -2 }, /* (73) replica ::= REPLICA INTEGER */
  {  229,   -2 }, /* (74) days ::= DAYS INTEGER */
  {  230,   -2 }, /* (75) minrows ::= MINROWS INTEGER */
  {  231,   -2 }, /* (76) maxrows ::= MAXROWS INTEGER */
  {  232,   -2 }, /* (77) blocks ::= BLOCKS INTEGER */
  {  233,   -2 }, /* (78) ctime ::= CTIME INTEGER */
  {  234,   -2 }, /* (79) wal ::= WAL INTEGER */
  {  235,   -2 }, /* (80) comp ::= COMP INTEGER */
  {  236,   -2 }, /* (81) prec ::= PRECISION STRING */
  {  214,    0 }, /* (82) db_optr ::= */
  {  214,   -2 }, /* (83) db_optr ::= db_optr tables */
  {  214,   -2 }, /* (84) db_optr ::= db_optr cache */
  {  214,   -2 }, /* (85) db_optr ::= db_optr replica */
  {  214,   -2 }, /* (86) db_optr ::= db_optr days */
  {  214,   -2 }, /* (87) db_optr ::= db_optr minrows */
  {  214,   -2 }, /* (88) db_optr ::= db_optr maxrows */
  {  214,   -2 }, /* (89) db_optr ::= db_optr blocks */
  {  214,   -2 }, /* (90) db_optr ::= db_optr ctime */
  {  214,   -2 }, /* (91) db_optr ::= db_optr wal */
  {  214,   -2 }, /* (92) db_optr ::= db_optr comp */
  {  214,   -2 }, /* (93) db_optr ::= db_optr prec */
  {  214,   -2 }, /* (94) db_optr ::= db_optr keep */
  {  211,    0 }, /* (95) alter_db_optr ::= */
  {  211,   -2 }, /* (96) alter_db_optr ::= alter_db_optr replica */
  {  211,   -2 }, /* (97) alter_db_optr ::= alter_db_optr tables */
  {  211,   -2 }, /* (98) alter_db_optr ::= alter_db_optr keep */
  {  211,   -2 }, /* (99) alter_db_optr ::= alter_db_optr blocks */
  {  211,   -2 }, /* (100) alter_db_optr ::= alter_db_optr comp */
  {  211,   -2 }, /* (101) alter_db_optr ::= alter_db_optr wal */
  {  237,   -1 }, /* (102) typename ::= ids */
  {  237,   -4 }, /* (103) typename ::= ids LP signed RP */
  {  238,   -1 }, /* (104) signed ::= INTEGER */
  {  238,   -2 }, /* (105) signed ::= PLUS INTEGER */
  {  238,   -2 }, /* (106) signed ::= MINUS INTEGER */
  {  206,   -6 }, /* (107) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  239,   -3 }, /* (108) create_table_args ::= LP columnlist RP */
  {  239,   -7 }, /* (109) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  239,   -7 }, /* (110) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  239,   -2 }, /* (111) create_table_args ::= AS select */
  {  240,   -3 }, /* (112) columnlist ::= columnlist COMMA column */
  {  240,   -1 }, /* (113) columnlist ::= column */
  {  242,   -2 }, /* (114) column ::= ids typename */
  {  225,   -3 }, /* (115) tagitemlist ::= tagitemlist COMMA tagitem */
  {  225,   -1 }, /* (116) tagitemlist ::= tagitem */
  {  243,   -1 }, /* (117) tagitem ::= INTEGER */
  {  243,   -1 }, /* (118) tagitem ::= FLOAT */
  {  243,   -1 }, /* (119) tagitem ::= STRING */
  {  243,   -1 }, /* (120) tagitem ::= BOOL */
  {  243,   -1 }, /* (121) tagitem ::= NULL */
  {  243,   -2 }, /* (122) tagitem ::= MINUS INTEGER */
  {  243,   -2 }, /* (123) tagitem ::= MINUS FLOAT */
  {  243,   -2 }, /* (124) tagitem ::= PLUS INTEGER */
  {  243,   -2 }, /* (125) tagitem ::= PLUS FLOAT */
  {  241,  -12 }, /* (126) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  255,   -1 }, /* (127) union ::= select */
  {  255,   -3 }, /* (128) union ::= LP union RP */
  {  255,   -4 }, /* (129) union ::= union UNION ALL select */
  {  255,   -6 }, /* (130) union ::= union UNION ALL LP select RP */
  {  206,   -1 }, /* (131) cmd ::= union */
  {  241,   -2 }, /* (132) select ::= SELECT selcollist */
  {  256,   -2 }, /* (133) sclp ::= selcollist COMMA */
  {  256,    0 }, /* (134) sclp ::= */
  {  244,   -3 }, /* (135) selcollist ::= sclp expr as */
  {  244,   -2 }, /* (136) selcollist ::= sclp STAR */
  {  258,   -2 }, /* (137) as ::= AS ids */
  {  258,   -1 }, /* (138) as ::= ids */
  {  258,    0 }, /* (139) as ::= */
  {  245,   -2 }, /* (140) from ::= FROM tablelist */
  {  259,   -2 }, /* (141) tablelist ::= ids cpxName */
  {  259,   -4 }, /* (142) tablelist ::= tablelist COMMA ids cpxName */
  {  260,   -1 }, /* (143) tmvar ::= VARIABLE */
  {  247,   -4 }, /* (144) interval_opt ::= INTERVAL LP tmvar RP */
  {  247,    0 }, /* (145) interval_opt ::= */
  {  248,    0 }, /* (146) fill_opt ::= */
  {  248,   -6 }, /* (147) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  248,   -4 }, /* (148) fill_opt ::= FILL LP ID RP */
  {  249,   -4 }, /* (149) sliding_opt ::= SLIDING LP tmvar RP */
  {  249,    0 }, /* (150) sliding_opt ::= */
  {  251,    0 }, /* (151) orderby_opt ::= */
  {  251,   -3 }, /* (152) orderby_opt ::= ORDER BY sortlist */
  {  261,   -4 }, /* (153) sortlist ::= sortlist COMMA item sortorder */
  {  261,   -2 }, /* (154) sortlist ::= item sortorder */
  {  263,   -2 }, /* (155) item ::= ids cpxName */
  {  264,   -1 }, /* (156) sortorder ::= ASC */
  {  264,   -1 }, /* (157) sortorder ::= DESC */
  {  264,    0 }, /* (158) sortorder ::= */
  {  250,    0 }, /* (159) groupby_opt ::= */
  {  250,   -3 }, /* (160) groupby_opt ::= GROUP BY grouplist */
  {  265,   -3 }, /* (161) grouplist ::= grouplist COMMA item */
  {  265,   -1 }, /* (162) grouplist ::= item */
  {  252,    0 }, /* (163) having_opt ::= */
  {  252,   -2 }, /* (164) having_opt ::= HAVING expr */
  {  254,    0 }, /* (165) limit_opt ::= */
  {  254,   -2 }, /* (166) limit_opt ::= LIMIT signed */
  {  254,   -4 }, /* (167) limit_opt ::= LIMIT signed OFFSET signed */
  {  254,   -4 }, /* (168) limit_opt ::= LIMIT signed COMMA signed */
  {  253,    0 }, /* (169) slimit_opt ::= */
  {  253,   -2 }, /* (170) slimit_opt ::= SLIMIT signed */
  {  253,   -4 }, /* (171) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  253,   -4 }, /* (172) slimit_opt ::= SLIMIT signed COMMA signed */
  {  246,    0 }, /* (173) where_opt ::= */
  {  246,   -2 }, /* (174) where_opt ::= WHERE expr */
  {  257,   -3 }, /* (175) expr ::= LP expr RP */
  {  257,   -1 }, /* (176) expr ::= ID */
  {  257,   -3 }, /* (177) expr ::= ID DOT ID */
  {  257,   -3 }, /* (178) expr ::= ID DOT STAR */
  {  257,   -1 }, /* (179) expr ::= INTEGER */
  {  257,   -2 }, /* (180) expr ::= MINUS INTEGER */
  {  257,   -2 }, /* (181) expr ::= PLUS INTEGER */
  {  257,   -1 }, /* (182) expr ::= FLOAT */
  {  257,   -2 }, /* (183) expr ::= MINUS FLOAT */
  {  257,   -2 }, /* (184) expr ::= PLUS FLOAT */
  {  257,   -1 }, /* (185) expr ::= STRING */
  {  257,   -1 }, /* (186) expr ::= NOW */
  {  257,   -1 }, /* (187) expr ::= VARIABLE */
  {  257,   -1 }, /* (188) expr ::= BOOL */
  {  257,   -4 }, /* (189) expr ::= ID LP exprlist RP */
  {  257,   -4 }, /* (190) expr ::= ID LP STAR RP */
  {  257,   -3 }, /* (191) expr ::= expr AND expr */
  {  257,   -3 }, /* (192) expr ::= expr OR expr */
  {  257,   -3 }, /* (193) expr ::= expr LT expr */
  {  257,   -3 }, /* (194) expr ::= expr GT expr */
  {  257,   -3 }, /* (195) expr ::= expr LE expr */
  {  257,   -3 }, /* (196) expr ::= expr GE expr */
  {  257,   -3 }, /* (197) expr ::= expr NE expr */
  {  257,   -3 }, /* (198) expr ::= expr EQ expr */
  {  257,   -3 }, /* (199) expr ::= expr PLUS expr */
  {  257,   -3 }, /* (200) expr ::= expr MINUS expr */
  {  257,   -3 }, /* (201) expr ::= expr STAR expr */
  {  257,   -3 }, /* (202) expr ::= expr SLASH expr */
  {  257,   -3 }, /* (203) expr ::= expr REM expr */
  {  257,   -3 }, /* (204) expr ::= expr LIKE expr */
  {  257,   -5 }, /* (205) expr ::= expr IN LP exprlist RP */
  {  266,   -3 }, /* (206) exprlist ::= exprlist COMMA expritem */
  {  266,   -1 }, /* (207) exprlist ::= expritem */
  {  267,   -1 }, /* (208) expritem ::= expr */
  {  267,    0 }, /* (209) expritem ::= */
  {  206,   -3 }, /* (210) cmd ::= RESET QUERY CACHE */
  {  206,   -7 }, /* (211) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  206,   -7 }, /* (212) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  206,   -7 }, /* (213) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  206,   -7 }, /* (214) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  206,   -8 }, /* (215) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  206,   -9 }, /* (216) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  206,   -5 }, /* (217) cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
  {  206,   -7 }, /* (218) cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
  {  206,   -7 }, /* (219) cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
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
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy63);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy63);}
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
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy63);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy63.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy63.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy63.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy63.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy63.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy63.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy63.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy63.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy63.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy63 = yylhsminor.yy63;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy456 = yymsp[0].minor.yy456; }
        break;
      case 71: /* tables ::= MAXTABLES INTEGER */
      case 72: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==72);
      case 73: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==73);
      case 74: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==74);
      case 75: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==75);
      case 76: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==78);
      case 79: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==79);
      case 80: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==80);
      case 81: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==81);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 82: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy478);}
        break;
      case 83: /* db_optr ::= db_optr tables */
      case 97: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==97);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.maxTablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 84: /* db_optr ::= db_optr cache */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 85: /* db_optr ::= db_optr replica */
      case 96: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==96);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 86: /* db_optr ::= db_optr days */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 87: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 88: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 89: /* db_optr ::= db_optr blocks */
      case 99: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==99);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 90: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 91: /* db_optr ::= db_optr wal */
      case 101: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==101);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 92: /* db_optr ::= db_optr comp */
      case 100: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==100);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 93: /* db_optr ::= db_optr prec */
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 94: /* db_optr ::= db_optr keep */
      case 98: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==98);
{ yylhsminor.yy478 = yymsp[-1].minor.yy478; yylhsminor.yy478.keep = yymsp[0].minor.yy456; }
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 95: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy478);}
        break;
      case 102: /* typename ::= ids */
{ tSQLSetColumnType (&yylhsminor.yy47, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy47 = yylhsminor.yy47;
        break;
      case 103: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy373;          // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy47, &yymsp[-3].minor.yy0);
}
  yymsp[-3].minor.yy47 = yylhsminor.yy47;
        break;
      case 104: /* signed ::= INTEGER */
{ yylhsminor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 105: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 106: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy373 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 107: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 108: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy494 = tSetCreateSQLElems(yymsp[-1].minor.yy503, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy494, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 109: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy494 = tSetCreateSQLElems(yymsp[-5].minor.yy503, yymsp[-1].minor.yy503, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy494, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 110: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy494 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy456, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy494, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 111: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy494 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy392, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy494, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 112: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy503 = tFieldListAppend(yymsp[-2].minor.yy503, &yymsp[0].minor.yy47);   }
  yymsp[-2].minor.yy503 = yylhsminor.yy503;
        break;
      case 113: /* columnlist ::= column */
{yylhsminor.yy503 = tFieldListAppend(NULL, &yymsp[0].minor.yy47);}
  yymsp[0].minor.yy503 = yylhsminor.yy503;
        break;
      case 114: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy47, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);
}
  yymsp[-1].minor.yy47 = yylhsminor.yy47;
        break;
      case 115: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy456 = tVariantListAppend(yymsp[-2].minor.yy456, &yymsp[0].minor.yy442, -1);    }
  yymsp[-2].minor.yy456 = yylhsminor.yy456;
        break;
      case 116: /* tagitemlist ::= tagitem */
{ yylhsminor.yy456 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
  yymsp[0].minor.yy456 = yylhsminor.yy456;
        break;
      case 117: /* tagitem ::= INTEGER */
      case 118: /* tagitem ::= FLOAT */ yytestcase(yyruleno==118);
      case 119: /* tagitem ::= STRING */ yytestcase(yyruleno==119);
      case 120: /* tagitem ::= BOOL */ yytestcase(yyruleno==120);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 121: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 122: /* tagitem ::= MINUS INTEGER */
      case 123: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==123);
      case 124: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==124);
      case 125: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==125);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 126: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy392 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy10, yymsp[-9].minor.yy456, yymsp[-8].minor.yy2, yymsp[-4].minor.yy456, yymsp[-3].minor.yy456, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy456, &yymsp[0].minor.yy230, &yymsp[-1].minor.yy230);
}
  yymsp[-11].minor.yy392 = yylhsminor.yy392;
        break;
      case 127: /* union ::= select */
{ yylhsminor.yy145 = setSubclause(NULL, yymsp[0].minor.yy392); }
  yymsp[0].minor.yy145 = yylhsminor.yy145;
        break;
      case 128: /* union ::= LP union RP */
{ yymsp[-2].minor.yy145 = yymsp[-1].minor.yy145; }
        break;
      case 129: /* union ::= union UNION ALL select */
{ yylhsminor.yy145 = appendSelectClause(yymsp[-3].minor.yy145, yymsp[0].minor.yy392); }
  yymsp[-3].minor.yy145 = yylhsminor.yy145;
        break;
      case 130: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy145 = appendSelectClause(yymsp[-5].minor.yy145, yymsp[-1].minor.yy392); }
  yymsp[-5].minor.yy145 = yylhsminor.yy145;
        break;
      case 131: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy145, NULL, TSDB_SQL_SELECT); }
        break;
      case 132: /* select ::= SELECT selcollist */
{
  yylhsminor.yy392 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy10, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 133: /* sclp ::= selcollist COMMA */
{yylhsminor.yy10 = yymsp[-1].minor.yy10;}
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 134: /* sclp ::= */
{yymsp[1].minor.yy10 = 0;}
        break;
      case 135: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy10 = tSQLExprListAppend(yymsp[-2].minor.yy10, yymsp[-1].minor.yy2, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy10 = yylhsminor.yy10;
        break;
      case 136: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy10 = tSQLExprListAppend(yymsp[-1].minor.yy10, pNode, 0);
}
  yymsp[-1].minor.yy10 = yylhsminor.yy10;
        break;
      case 137: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 138: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 139: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 140: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy456 = yymsp[0].minor.yy456;}
        break;
      case 141: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy456 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy456 = yylhsminor.yy456;
        break;
      case 142: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy456 = tVariantListAppendToken(yymsp[-3].minor.yy456, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy456 = yylhsminor.yy456;
        break;
      case 143: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 144: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 149: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==149);
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 145: /* interval_opt ::= */
      case 150: /* sliding_opt ::= */ yytestcase(yyruleno==150);
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 146: /* fill_opt ::= */
{yymsp[1].minor.yy456 = 0;     }
        break;
      case 147: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy456, &A, -1, 0);
    yymsp[-5].minor.yy456 = yymsp[-1].minor.yy456;
}
        break;
      case 148: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy456 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 151: /* orderby_opt ::= */
      case 159: /* groupby_opt ::= */ yytestcase(yyruleno==159);
{yymsp[1].minor.yy456 = 0;}
        break;
      case 152: /* orderby_opt ::= ORDER BY sortlist */
      case 160: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==160);
{yymsp[-2].minor.yy456 = yymsp[0].minor.yy456;}
        break;
      case 153: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy456 = tVariantListAppend(yymsp[-3].minor.yy456, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
  yymsp[-3].minor.yy456 = yylhsminor.yy456;
        break;
      case 154: /* sortlist ::= item sortorder */
{
  yylhsminor.yy456 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
  yymsp[-1].minor.yy456 = yylhsminor.yy456;
        break;
      case 155: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 156: /* sortorder ::= ASC */
{yymsp[0].minor.yy196 = TSDB_ORDER_ASC; }
        break;
      case 157: /* sortorder ::= DESC */
{yymsp[0].minor.yy196 = TSDB_ORDER_DESC;}
        break;
      case 158: /* sortorder ::= */
{yymsp[1].minor.yy196 = TSDB_ORDER_ASC;}
        break;
      case 161: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy456 = tVariantListAppend(yymsp[-2].minor.yy456, &yymsp[0].minor.yy442, -1);
}
  yymsp[-2].minor.yy456 = yylhsminor.yy456;
        break;
      case 162: /* grouplist ::= item */
{
  yylhsminor.yy456 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
  yymsp[0].minor.yy456 = yylhsminor.yy456;
        break;
      case 163: /* having_opt ::= */
      case 173: /* where_opt ::= */ yytestcase(yyruleno==173);
      case 209: /* expritem ::= */ yytestcase(yyruleno==209);
{yymsp[1].minor.yy2 = 0;}
        break;
      case 164: /* having_opt ::= HAVING expr */
      case 174: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==174);
{yymsp[-1].minor.yy2 = yymsp[0].minor.yy2;}
        break;
      case 165: /* limit_opt ::= */
      case 169: /* slimit_opt ::= */ yytestcase(yyruleno==169);
{yymsp[1].minor.yy230.limit = -1; yymsp[1].minor.yy230.offset = 0;}
        break;
      case 166: /* limit_opt ::= LIMIT signed */
      case 170: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==170);
{yymsp[-1].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-1].minor.yy230.offset = 0;}
        break;
      case 167: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 171: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==171);
{yymsp[-3].minor.yy230.limit = yymsp[-2].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 168: /* limit_opt ::= LIMIT signed COMMA signed */
      case 172: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==172);
{yymsp[-3].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 175: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy2 = yymsp[-1].minor.yy2; }
        break;
      case 176: /* expr ::= ID */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 177: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 178: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 179: /* expr ::= INTEGER */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 180: /* expr ::= MINUS INTEGER */
      case 181: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==181);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy2 = yylhsminor.yy2;
        break;
      case 182: /* expr ::= FLOAT */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 183: /* expr ::= MINUS FLOAT */
      case 184: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==184);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy2 = yylhsminor.yy2;
        break;
      case 185: /* expr ::= STRING */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 186: /* expr ::= NOW */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 187: /* expr ::= VARIABLE */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 188: /* expr ::= BOOL */
{yylhsminor.yy2 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 189: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy2 = tSQLExprCreateFunction(yymsp[-1].minor.yy10, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy2 = yylhsminor.yy2;
        break;
      case 190: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy2 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy2 = yylhsminor.yy2;
        break;
      case 191: /* expr ::= expr AND expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_AND);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 192: /* expr ::= expr OR expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_OR); }
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 193: /* expr ::= expr LT expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_LT);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 194: /* expr ::= expr GT expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_GT);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 195: /* expr ::= expr LE expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_LE);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 196: /* expr ::= expr GE expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_GE);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 197: /* expr ::= expr NE expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_NE);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 198: /* expr ::= expr EQ expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_EQ);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 199: /* expr ::= expr PLUS expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_PLUS);  }
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 200: /* expr ::= expr MINUS expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_MINUS); }
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 201: /* expr ::= expr STAR expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_STAR);  }
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 202: /* expr ::= expr SLASH expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_DIVIDE);}
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 203: /* expr ::= expr REM expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_REM);   }
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 204: /* expr ::= expr LIKE expr */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-2].minor.yy2, yymsp[0].minor.yy2, TK_LIKE);  }
  yymsp[-2].minor.yy2 = yylhsminor.yy2;
        break;
      case 205: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy2 = tSQLExprCreate(yymsp[-4].minor.yy2, (tSQLExpr*)yymsp[-1].minor.yy10, TK_IN); }
  yymsp[-4].minor.yy2 = yylhsminor.yy2;
        break;
      case 206: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy10 = tSQLExprListAppend(yymsp[-2].minor.yy10,yymsp[0].minor.yy2,0);}
  yymsp[-2].minor.yy10 = yylhsminor.yy10;
        break;
      case 207: /* exprlist ::= expritem */
{yylhsminor.yy10 = tSQLExprListAppend(0,yymsp[0].minor.yy2,0);}
  yymsp[0].minor.yy10 = yylhsminor.yy10;
        break;
      case 208: /* expritem ::= expr */
{yylhsminor.yy2 = yymsp[0].minor.yy2;}
  yymsp[0].minor.yy2 = yylhsminor.yy2;
        break;
      case 210: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 211: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy503, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 212: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 213: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy503, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 214: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 215: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 216: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 217: /* cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[-2].minor.yy0);}
        break;
      case 218: /* cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-4].minor.yy0);}
        break;
      case 219: /* cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
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
