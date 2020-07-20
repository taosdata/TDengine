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
#include "qSqlparser.h"
#include "tcmdtype.h"
#include "tstoken.h"
#include "ttokendef.h"
#include "tutil.h"
#include "tvariant.h"
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
#define YYNOCODE 272
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSubclauseInfo* yy25;
  tSQLExpr* yy66;
  SCreateAcctSQL yy73;
  int yy82;
  SQuerySQL* yy150;
  SCreateDBInfo yy158;
  TAOS_FIELD yy181;
  SLimitVal yy188;
  tSQLExprList* yy224;
  int64_t yy271;
  tVariant yy312;
  SCreateTableSQL* yy374;
  tFieldList* yy449;
  tVariantList* yy494;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             242
#define YYNRULE              223
#define YYNTOKEN             206
#define YY_MAX_SHIFT         241
#define YY_MIN_SHIFTREDUCE   401
#define YY_MAX_SHIFTREDUCE   623
#define YY_ERROR_ACTION      624
#define YY_ACCEPT_ACTION     625
#define YY_NO_ACTION         626
#define YY_MIN_REDUCE        627
#define YY_MAX_REDUCE        849
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
 /*     0 */   727,  442,  726,   11,  725,  625,  241,  508,  728,  443,
 /*    10 */   730,  731,  729,   41,   43,  524,   35,   36,  521,  134,
 /*    20 */   522,   29,  523,  442,  197,   39,   37,   40,   38,  153,
 /*    30 */   239,  443,  219,   34,   33,  217,  216,   32,   31,   30,
 /*    40 */    41,   43,  757,   35,   36,  139,  170,  171,   29,  135,
 /*    50 */    21,  197,   39,   37,   40,   38,  182,  833,  158,  837,
 /*    60 */    34,   33,  744,  768,   32,   31,   30,  402,  403,  404,
 /*    70 */   405,  406,  407,  408,  409,  410,  411,  412,  413,  240,
 /*    80 */    41,   43,  846,   35,   36,  742,   60,  135,   29,  135,
 /*    90 */    21,  197,   39,   37,   40,   38,  157,  837,   27,  836,
 /*   100 */    34,   33,   56,  228,   32,   31,   30,  103,   43,    8,
 /*   110 */    35,   36,   61,  113,  765,   29,  757,  525,  197,   39,
 /*   120 */    37,   40,   38,  166,  537,  743,  579,   34,   33,   18,
 /*   130 */   154,   32,   31,   30,   16,  234,  208,  233,  207,  206,
 /*   140 */   205,  232,  204,  231,  230,  229,  203,  723,  168,  711,
 /*   150 */   712,  713,  714,  715,  716,  717,  718,  719,  720,  721,
 /*   160 */   722,   35,   36,  792,  103,  192,   29,  175,  155,  197,
 /*   170 */    39,   37,   40,   38,  179,  178,   21,  581,   34,   33,
 /*   180 */   442,   12,   32,   31,   30,  162,  592,  746,  443,  583,
 /*   190 */    17,  586,   76,  589,  103,  162,  592,   26,  228,  583,
 /*   200 */   148,  586,   99,  589,  103,   21,   88,   87,  142,  167,
 /*   210 */   165,  743,  169,  582,  147,  214,  213,  159,  160,   50,
 /*   220 */   791,  196,   74,   78,   83,   86,   77,  159,  160,  746,
 /*   230 */   235,  540,   80,  162,  592,   17,   51,  583,  215,  586,
 /*   240 */   743,  589,   26,   39,   37,   40,   38,  832,  194,  746,
 /*   250 */    58,   34,   33,   47,  185,   32,   31,   30,  666,  831,
 /*   260 */    59,  126,  181,  560,  561,  159,  160,   16,  234,  150,
 /*   270 */   233,   21,   48,  585,  232,  588,  231,  230,  229,   34,
 /*   280 */    33,  745,   42,   32,   31,   30,  116,  117,   68,   64,
 /*   290 */    67,  151,   42,  591,   32,   31,   30,  130,  128,   91,
 /*   300 */    90,   89,  675,  591,  220,  126,  743,   98,  590,  238,
 /*   310 */   237,   95,  667,  152,   26,  126,  532,  584,  590,  587,
 /*   320 */   551,  161,  552,  184,   46,  609,   14,  593,  514,   13,
 /*   330 */    42,   13,   46,  513,  201,   73,   72,   22,   22,   10,
 /*   340 */     9,  591,  528,  526,  529,  527,   85,   84,  802,  140,
 /*   350 */   141,  143,  801,  144,  163,  145,  590,  146,  798,  137,
 /*   360 */     3,  133,  138,  136,  797,  164,  759,  737,  218,  767,
 /*   370 */   100,  784,  783,  114,  112,  115,  677,  202,  131,   24,
 /*   380 */   211,  674,  212,  845,   70,  844,   26,  842,  118,   93,
 /*   390 */   695,   25,  547,   23,  186,  132,  664,   79,  662,   81,
 /*   400 */    82,  660,  659,  172,  190,  127,  657,  656,  655,  654,
 /*   410 */   653,  645,  129,  651,  649,  647,   52,   49,  756,  771,
 /*   420 */   104,  772,   44,  785,  195,  193,  191,  189,  187,   28,
 /*   430 */   210,   75,  221,  222,  223,  224,  225,  226,  199,  227,
 /*   440 */   236,   53,  623,  174,  173,  622,   62,  177,  149,  621,
 /*   450 */   183,   65,  176,  614,  658,  180,   92,   94,  652,  121,
 /*   460 */   125,  534,  120,  696,  741,  119,  110,  107,  105,  106,
 /*   470 */   122,  108,  123,    2,  111,  109,  124,    1,  184,   57,
 /*   480 */    55,  548,  156,  101,  188,    5,  553,  102,    6,   63,
 /*   490 */   594,   19,    4,   20,   15,  198,    7,  200,  483,  479,
 /*   500 */   477,  476,  475,  473,  446,  209,   66,   45,   69,   71,
 /*   510 */    22,  510,  509,  507,   54,  467,  465,  457,  463,  459,
 /*   520 */   461,  455,  453,  482,  481,  480,  478,  474,  472,   46,
 /*   530 */   444,  417,  415,  627,  626,  626,  626,  626,  626,  626,
 /*   540 */   626,  626,  626,  626,  626,   96,   97,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   226,    1,  228,  260,  230,  207,  208,    5,  234,    9,
 /*    10 */   236,  237,  238,   13,   14,    2,   16,   17,    5,  260,
 /*    20 */     7,   21,    9,    1,   24,   25,   26,   27,   28,  209,
 /*    30 */   210,    9,  210,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  244,   16,   17,  260,   33,   34,   21,  260,
 /*    50 */   210,   24,   25,   26,   27,   28,  258,  260,  269,  270,
 /*    60 */    33,   34,  240,  210,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    13,   14,  246,   16,   17,  245,  247,  260,   21,  260,
 /*    90 */   210,   24,   25,   26,   27,   28,  269,  270,  259,  270,
 /*   100 */    33,   34,  102,   78,   37,   38,   39,  210,   14,   98,
 /*   110 */    16,   17,  101,  102,  261,   21,  244,  104,   24,   25,
 /*   120 */    26,   27,   28,  243,  103,  245,   99,   33,   34,  108,
 /*   130 */   258,   37,   38,   39,   85,   86,   87,   88,   89,   90,
 /*   140 */    91,   92,   93,   94,   95,   96,   97,  226,   63,  228,
 /*   150 */   229,  230,  231,  232,  233,  234,  235,  236,  237,  238,
 /*   160 */   239,   16,   17,  266,  210,  268,   21,  126,  227,   24,
 /*   170 */    25,   26,   27,   28,  133,  134,  210,    1,   33,   34,
 /*   180 */     1,   44,   37,   38,   39,    1,    2,  246,    9,    5,
 /*   190 */    98,    7,   72,    9,  210,    1,    2,  105,   78,    5,
 /*   200 */    63,    7,  210,    9,  210,  210,   69,   70,   71,  243,
 /*   210 */   227,  245,  127,   37,   77,  130,  131,   33,   34,  103,
 /*   220 */   266,   37,   64,   65,   66,   67,   68,   33,   34,  246,
 /*   230 */   227,   37,   74,    1,    2,   98,  120,    5,  243,    7,
 /*   240 */   245,    9,  105,   25,   26,   27,   28,  260,  264,  246,
 /*   250 */   266,   33,   34,  103,  262,   37,   38,   39,  214,  260,
 /*   260 */   266,  217,  125,  115,  116,   33,   34,   85,   86,  132,
 /*   270 */    88,  210,  122,    5,   92,    7,   94,   95,   96,   33,
 /*   280 */    34,  246,   98,   37,   38,   39,   64,   65,   66,   67,
 /*   290 */    68,  260,   98,  109,   37,   38,   39,   64,   65,   66,
 /*   300 */    67,   68,  214,  109,  243,  217,  245,   98,  124,   60,
 /*   310 */    61,   62,  214,  260,  105,  217,   99,    5,  124,    7,
 /*   320 */    99,   59,   99,  106,  103,   99,  103,   99,   99,  103,
 /*   330 */    98,  103,  103,   99,   99,  128,  129,  103,  103,  128,
 /*   340 */   129,  109,    5,    5,    7,    7,   72,   73,  241,  260,
 /*   350 */   260,  260,  241,  260,  241,  260,  124,  260,  241,  260,
 /*   360 */    98,  260,  260,  260,  241,  241,  244,  242,  241,  210,
 /*   370 */   210,  267,  267,  210,  248,  210,  210,  210,  210,  210,
 /*   380 */   210,  210,  210,  210,  210,  210,  105,  210,  210,   59,
 /*   390 */   210,  210,  109,  210,  263,  210,  210,  210,  210,  210,
 /*   400 */   210,  210,  210,  210,  263,  210,  210,  210,  210,  210,
 /*   410 */   210,  210,  210,  210,  210,  210,  119,  121,  257,  211,
 /*   420 */   256,  211,  118,  211,  113,  117,  112,  111,  110,  123,
 /*   430 */    75,   84,   83,   49,   80,   82,   53,   81,  211,   79,
 /*   440 */    75,  211,    5,    5,  135,    5,  215,    5,  211,    5,
 /*   450 */   244,  215,  135,   87,  211,  126,  212,  212,  211,  219,
 /*   460 */   218,   99,  223,  225,  244,  224,  250,  253,  255,  254,
 /*   470 */   222,  252,  220,  213,  249,  251,  221,  216,  106,  103,
 /*   480 */   107,   99,    1,   98,   98,  114,   99,   98,  114,   72,
 /*   490 */    99,  103,   98,  103,   98,  100,   98,  100,    9,    5,
 /*   500 */     5,    5,    5,    5,   76,   15,   72,   16,  129,  129,
 /*   510 */   103,    5,    5,   99,   98,    5,    5,    5,    5,    5,
 /*   520 */     5,    5,    5,    5,    5,    5,    5,    5,    5,  103,
 /*   530 */    76,   59,   58,    0,  271,  271,  271,  271,  271,  271,
 /*   540 */   271,  271,  271,  271,  271,   21,   21,  271,  271,  271,
 /*   550 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   560 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   570 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   580 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   590 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   600 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   610 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   620 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   630 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   640 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   650 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   660 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   670 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   680 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   690 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   700 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   710 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   720 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   730 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   740 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   750 */   271,  271,  271,
};
#define YY_SHIFT_COUNT    (241)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (533)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   137,   49,  182,  184,  232,  179,  179,  179,  179,  179,
 /*    10 */   179,    0,   22,  232,   13,   13,   13,   92,  179,  179,
 /*    20 */   179,  179,  179,  120,   25,   25,  547,  194,  232,  232,
 /*    30 */   232,  232,  232,  232,  232,  232,  232,  232,  232,  232,
 /*    40 */   232,  232,  232,  232,  232,   13,   13,    2,    2,    2,
 /*    50 */     2,    2,    2,   11,    2,  209,  179,  179,  148,  148,
 /*    60 */    21,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*    70 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*    80 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*    90 */   179,  179,  179,  179,  179,  179,  179,  179,  281,  330,
 /*   100 */   330,  283,  283,  330,  297,  296,  304,  311,  308,  314,
 /*   110 */   316,  318,  306,  281,  330,  330,  355,  355,  330,  347,
 /*   120 */   349,  384,  354,  353,  383,  356,  360,  330,  365,  330,
 /*   130 */   365,  547,  547,   27,   67,   67,   67,   94,  145,  218,
 /*   140 */   218,  218,  158,  246,  246,  246,  246,  222,  233,   85,
 /*   150 */    41,  257,  257,  249,  217,  221,  223,  226,  228,  268,
 /*   160 */   312,  176,  262,  150,  116,  229,  234,  235,  207,  211,
 /*   170 */   337,  338,  274,  437,  309,  438,  440,  317,  442,  444,
 /*   180 */   366,  329,  372,  362,  373,  376,  382,  385,  481,  386,
 /*   190 */   387,  389,  388,  371,  390,  374,  391,  394,  396,  395,
 /*   200 */   398,  397,  417,  489,  494,  495,  496,  497,  498,  428,
 /*   210 */   490,  434,  491,  379,  380,  407,  506,  507,  414,  416,
 /*   220 */   407,  510,  511,  512,  513,  514,  515,  516,  517,  518,
 /*   230 */   519,  520,  521,  522,  523,  426,  454,  524,  525,  472,
 /*   240 */   474,  533,
};
#define YY_REDUCE_COUNT (132)
#define YY_REDUCE_MIN   (-257)
#define YY_REDUCE_MAX   (261)
static const short yy_reduce_ofst[] = {
 /*     0 */  -202,  -79, -226, -211, -173, -103,  -16, -120,  -34,   -5,
 /*    10 */    61, -147, -180, -171,  -59,  -17,    3, -128,   -8,  -46,
 /*    20 */    -6, -178, -160,   44,   88,   98, -161, -257, -241, -215,
 /*    30 */  -203,  -13,   -1,   31,   53,   89,   90,   91,   93,   95,
 /*    40 */    97,   99,  101,  102,  103, -164,   35,  107,  111,  113,
 /*    50 */   117,  123,  124,  125,  127,  122,  159,  160,  104,  105,
 /*    60 */   126,  163,  165,  166,  167,  168,  169,  170,  171,  172,
 /*    70 */   173,  174,  175,  177,  178,  180,  181,  183,  185,  186,
 /*    80 */   187,  188,  189,  190,  191,  192,  193,  195,  196,  197,
 /*    90 */   198,  199,  200,  201,  202,  203,  204,  205,  206,  208,
 /*   100 */   210,  131,  141,  212,  161,  164,  213,  215,  214,  219,
 /*   110 */   224,  216,  225,  220,  227,  230,  231,  236,  237,  238,
 /*   120 */   241,  239,  240,  248,  252,  255,  242,  243,  244,  247,
 /*   130 */   245,  261,  260,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   624,  676,  665,  839,  839,  624,  624,  624,  624,  624,
 /*    10 */   624,  769,  642,  839,  624,  624,  624,  624,  624,  624,
 /*    20 */   624,  624,  624,  678,  678,  678,  764,  624,  624,  624,
 /*    30 */   624,  624,  624,  624,  624,  624,  624,  624,  624,  624,
 /*    40 */   624,  624,  624,  624,  624,  624,  624,  624,  624,  624,
 /*    50 */   624,  624,  624,  624,  624,  624,  624,  624,  788,  788,
 /*    60 */   762,  624,  624,  624,  624,  624,  624,  624,  624,  624,
 /*    70 */   624,  624,  624,  624,  624,  624,  624,  624,  624,  663,
 /*    80 */   624,  661,  624,  624,  624,  624,  624,  624,  624,  624,
 /*    90 */   624,  624,  624,  624,  624,  650,  624,  624,  624,  644,
 /*   100 */   644,  624,  624,  644,  795,  799,  793,  781,  789,  780,
 /*   110 */   776,  775,  803,  624,  644,  644,  673,  673,  644,  694,
 /*   120 */   692,  690,  682,  688,  684,  686,  680,  644,  671,  644,
 /*   130 */   671,  710,  724,  624,  804,  838,  794,  822,  821,  834,
 /*   140 */   828,  827,  624,  826,  825,  824,  823,  624,  624,  624,
 /*   150 */   624,  830,  829,  624,  624,  624,  624,  624,  624,  624,
 /*   160 */   624,  624,  806,  800,  796,  624,  624,  624,  624,  624,
 /*   170 */   624,  624,  624,  624,  624,  624,  624,  624,  624,  624,
 /*   180 */   624,  624,  761,  624,  624,  770,  624,  624,  624,  624,
 /*   190 */   624,  624,  790,  624,  782,  624,  624,  624,  624,  624,
 /*   200 */   624,  738,  624,  624,  624,  624,  624,  624,  624,  624,
 /*   210 */   624,  624,  624,  624,  624,  843,  624,  624,  624,  732,
 /*   220 */   841,  624,  624,  624,  624,  624,  624,  624,  624,  624,
 /*   230 */   624,  624,  624,  624,  624,  697,  624,  648,  646,  624,
 /*   240 */   640,  624,
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
    0,  /*      FSYNC => nothing */
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
  /*   95 */ "FSYNC",
  /*   96 */ "COMP",
  /*   97 */ "PRECISION",
  /*   98 */ "LP",
  /*   99 */ "RP",
  /*  100 */ "TAGS",
  /*  101 */ "USING",
  /*  102 */ "AS",
  /*  103 */ "COMMA",
  /*  104 */ "NULL",
  /*  105 */ "SELECT",
  /*  106 */ "UNION",
  /*  107 */ "ALL",
  /*  108 */ "FROM",
  /*  109 */ "VARIABLE",
  /*  110 */ "INTERVAL",
  /*  111 */ "FILL",
  /*  112 */ "SLIDING",
  /*  113 */ "ORDER",
  /*  114 */ "BY",
  /*  115 */ "ASC",
  /*  116 */ "DESC",
  /*  117 */ "GROUP",
  /*  118 */ "HAVING",
  /*  119 */ "LIMIT",
  /*  120 */ "OFFSET",
  /*  121 */ "SLIMIT",
  /*  122 */ "SOFFSET",
  /*  123 */ "WHERE",
  /*  124 */ "NOW",
  /*  125 */ "RESET",
  /*  126 */ "QUERY",
  /*  127 */ "ADD",
  /*  128 */ "COLUMN",
  /*  129 */ "TAG",
  /*  130 */ "CHANGE",
  /*  131 */ "SET",
  /*  132 */ "KILL",
  /*  133 */ "CONNECTION",
  /*  134 */ "STREAM",
  /*  135 */ "COLON",
  /*  136 */ "ABORT",
  /*  137 */ "AFTER",
  /*  138 */ "ATTACH",
  /*  139 */ "BEFORE",
  /*  140 */ "BEGIN",
  /*  141 */ "CASCADE",
  /*  142 */ "CLUSTER",
  /*  143 */ "CONFLICT",
  /*  144 */ "COPY",
  /*  145 */ "DEFERRED",
  /*  146 */ "DELIMITERS",
  /*  147 */ "DETACH",
  /*  148 */ "EACH",
  /*  149 */ "END",
  /*  150 */ "EXPLAIN",
  /*  151 */ "FAIL",
  /*  152 */ "FOR",
  /*  153 */ "IGNORE",
  /*  154 */ "IMMEDIATE",
  /*  155 */ "INITIALLY",
  /*  156 */ "INSTEAD",
  /*  157 */ "MATCH",
  /*  158 */ "KEY",
  /*  159 */ "OF",
  /*  160 */ "RAISE",
  /*  161 */ "REPLACE",
  /*  162 */ "RESTRICT",
  /*  163 */ "ROW",
  /*  164 */ "STATEMENT",
  /*  165 */ "TRIGGER",
  /*  166 */ "VIEW",
  /*  167 */ "COUNT",
  /*  168 */ "SUM",
  /*  169 */ "AVG",
  /*  170 */ "MIN",
  /*  171 */ "MAX",
  /*  172 */ "FIRST",
  /*  173 */ "LAST",
  /*  174 */ "TOP",
  /*  175 */ "BOTTOM",
  /*  176 */ "STDDEV",
  /*  177 */ "PERCENTILE",
  /*  178 */ "APERCENTILE",
  /*  179 */ "LEASTSQUARES",
  /*  180 */ "HISTOGRAM",
  /*  181 */ "DIFF",
  /*  182 */ "SPREAD",
  /*  183 */ "TWA",
  /*  184 */ "INTERP",
  /*  185 */ "LAST_ROW",
  /*  186 */ "RATE",
  /*  187 */ "IRATE",
  /*  188 */ "SUM_RATE",
  /*  189 */ "SUM_IRATE",
  /*  190 */ "AVG_RATE",
  /*  191 */ "AVG_IRATE",
  /*  192 */ "TBID",
  /*  193 */ "SEMI",
  /*  194 */ "NONE",
  /*  195 */ "PREV",
  /*  196 */ "LINEAR",
  /*  197 */ "IMPORT",
  /*  198 */ "METRIC",
  /*  199 */ "TBNAME",
  /*  200 */ "JOIN",
  /*  201 */ "METRICS",
  /*  202 */ "STABLE",
  /*  203 */ "INSERT",
  /*  204 */ "INTO",
  /*  205 */ "VALUES",
  /*  206 */ "error",
  /*  207 */ "program",
  /*  208 */ "cmd",
  /*  209 */ "dbPrefix",
  /*  210 */ "ids",
  /*  211 */ "cpxName",
  /*  212 */ "ifexists",
  /*  213 */ "alter_db_optr",
  /*  214 */ "acct_optr",
  /*  215 */ "ifnotexists",
  /*  216 */ "db_optr",
  /*  217 */ "pps",
  /*  218 */ "tseries",
  /*  219 */ "dbs",
  /*  220 */ "streams",
  /*  221 */ "storage",
  /*  222 */ "qtime",
  /*  223 */ "users",
  /*  224 */ "conns",
  /*  225 */ "state",
  /*  226 */ "keep",
  /*  227 */ "tagitemlist",
  /*  228 */ "tables",
  /*  229 */ "cache",
  /*  230 */ "replica",
  /*  231 */ "days",
  /*  232 */ "minrows",
  /*  233 */ "maxrows",
  /*  234 */ "blocks",
  /*  235 */ "ctime",
  /*  236 */ "wal",
  /*  237 */ "fsync",
  /*  238 */ "comp",
  /*  239 */ "prec",
  /*  240 */ "typename",
  /*  241 */ "signed",
  /*  242 */ "create_table_args",
  /*  243 */ "columnlist",
  /*  244 */ "select",
  /*  245 */ "column",
  /*  246 */ "tagitem",
  /*  247 */ "selcollist",
  /*  248 */ "from",
  /*  249 */ "where_opt",
  /*  250 */ "interval_opt",
  /*  251 */ "fill_opt",
  /*  252 */ "sliding_opt",
  /*  253 */ "groupby_opt",
  /*  254 */ "orderby_opt",
  /*  255 */ "having_opt",
  /*  256 */ "slimit_opt",
  /*  257 */ "limit_opt",
  /*  258 */ "union",
  /*  259 */ "sclp",
  /*  260 */ "expr",
  /*  261 */ "as",
  /*  262 */ "tablelist",
  /*  263 */ "tmvar",
  /*  264 */ "sortlist",
  /*  265 */ "sortitem",
  /*  266 */ "item",
  /*  267 */ "sortorder",
  /*  268 */ "grouplist",
  /*  269 */ "exprlist",
  /*  270 */ "expritem",
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
 /*  80 */ "fsync ::= FSYNC INTEGER",
 /*  81 */ "comp ::= COMP INTEGER",
 /*  82 */ "prec ::= PRECISION STRING",
 /*  83 */ "db_optr ::=",
 /*  84 */ "db_optr ::= db_optr tables",
 /*  85 */ "db_optr ::= db_optr cache",
 /*  86 */ "db_optr ::= db_optr replica",
 /*  87 */ "db_optr ::= db_optr days",
 /*  88 */ "db_optr ::= db_optr minrows",
 /*  89 */ "db_optr ::= db_optr maxrows",
 /*  90 */ "db_optr ::= db_optr blocks",
 /*  91 */ "db_optr ::= db_optr ctime",
 /*  92 */ "db_optr ::= db_optr wal",
 /*  93 */ "db_optr ::= db_optr fsync",
 /*  94 */ "db_optr ::= db_optr comp",
 /*  95 */ "db_optr ::= db_optr prec",
 /*  96 */ "db_optr ::= db_optr keep",
 /*  97 */ "alter_db_optr ::=",
 /*  98 */ "alter_db_optr ::= alter_db_optr replica",
 /*  99 */ "alter_db_optr ::= alter_db_optr tables",
 /* 100 */ "alter_db_optr ::= alter_db_optr keep",
 /* 101 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 102 */ "alter_db_optr ::= alter_db_optr comp",
 /* 103 */ "alter_db_optr ::= alter_db_optr wal",
 /* 104 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 105 */ "typename ::= ids",
 /* 106 */ "typename ::= ids LP signed RP",
 /* 107 */ "signed ::= INTEGER",
 /* 108 */ "signed ::= PLUS INTEGER",
 /* 109 */ "signed ::= MINUS INTEGER",
 /* 110 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 111 */ "create_table_args ::= LP columnlist RP",
 /* 112 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 113 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 114 */ "create_table_args ::= AS select",
 /* 115 */ "columnlist ::= columnlist COMMA column",
 /* 116 */ "columnlist ::= column",
 /* 117 */ "column ::= ids typename",
 /* 118 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 119 */ "tagitemlist ::= tagitem",
 /* 120 */ "tagitem ::= INTEGER",
 /* 121 */ "tagitem ::= FLOAT",
 /* 122 */ "tagitem ::= STRING",
 /* 123 */ "tagitem ::= BOOL",
 /* 124 */ "tagitem ::= NULL",
 /* 125 */ "tagitem ::= MINUS INTEGER",
 /* 126 */ "tagitem ::= MINUS FLOAT",
 /* 127 */ "tagitem ::= PLUS INTEGER",
 /* 128 */ "tagitem ::= PLUS FLOAT",
 /* 129 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 130 */ "union ::= select",
 /* 131 */ "union ::= LP union RP",
 /* 132 */ "union ::= union UNION ALL select",
 /* 133 */ "union ::= union UNION ALL LP select RP",
 /* 134 */ "cmd ::= union",
 /* 135 */ "select ::= SELECT selcollist",
 /* 136 */ "sclp ::= selcollist COMMA",
 /* 137 */ "sclp ::=",
 /* 138 */ "selcollist ::= sclp expr as",
 /* 139 */ "selcollist ::= sclp STAR",
 /* 140 */ "as ::= AS ids",
 /* 141 */ "as ::= ids",
 /* 142 */ "as ::=",
 /* 143 */ "from ::= FROM tablelist",
 /* 144 */ "tablelist ::= ids cpxName",
 /* 145 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 146 */ "tmvar ::= VARIABLE",
 /* 147 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 148 */ "interval_opt ::=",
 /* 149 */ "fill_opt ::=",
 /* 150 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 151 */ "fill_opt ::= FILL LP ID RP",
 /* 152 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 153 */ "sliding_opt ::=",
 /* 154 */ "orderby_opt ::=",
 /* 155 */ "orderby_opt ::= ORDER BY sortlist",
 /* 156 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 157 */ "sortlist ::= item sortorder",
 /* 158 */ "item ::= ids cpxName",
 /* 159 */ "sortorder ::= ASC",
 /* 160 */ "sortorder ::= DESC",
 /* 161 */ "sortorder ::=",
 /* 162 */ "groupby_opt ::=",
 /* 163 */ "groupby_opt ::= GROUP BY grouplist",
 /* 164 */ "grouplist ::= grouplist COMMA item",
 /* 165 */ "grouplist ::= item",
 /* 166 */ "having_opt ::=",
 /* 167 */ "having_opt ::= HAVING expr",
 /* 168 */ "limit_opt ::=",
 /* 169 */ "limit_opt ::= LIMIT signed",
 /* 170 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 171 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 172 */ "slimit_opt ::=",
 /* 173 */ "slimit_opt ::= SLIMIT signed",
 /* 174 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 175 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 176 */ "where_opt ::=",
 /* 177 */ "where_opt ::= WHERE expr",
 /* 178 */ "expr ::= LP expr RP",
 /* 179 */ "expr ::= ID",
 /* 180 */ "expr ::= ID DOT ID",
 /* 181 */ "expr ::= ID DOT STAR",
 /* 182 */ "expr ::= INTEGER",
 /* 183 */ "expr ::= MINUS INTEGER",
 /* 184 */ "expr ::= PLUS INTEGER",
 /* 185 */ "expr ::= FLOAT",
 /* 186 */ "expr ::= MINUS FLOAT",
 /* 187 */ "expr ::= PLUS FLOAT",
 /* 188 */ "expr ::= STRING",
 /* 189 */ "expr ::= NOW",
 /* 190 */ "expr ::= VARIABLE",
 /* 191 */ "expr ::= BOOL",
 /* 192 */ "expr ::= ID LP exprlist RP",
 /* 193 */ "expr ::= ID LP STAR RP",
 /* 194 */ "expr ::= expr AND expr",
 /* 195 */ "expr ::= expr OR expr",
 /* 196 */ "expr ::= expr LT expr",
 /* 197 */ "expr ::= expr GT expr",
 /* 198 */ "expr ::= expr LE expr",
 /* 199 */ "expr ::= expr GE expr",
 /* 200 */ "expr ::= expr NE expr",
 /* 201 */ "expr ::= expr EQ expr",
 /* 202 */ "expr ::= expr PLUS expr",
 /* 203 */ "expr ::= expr MINUS expr",
 /* 204 */ "expr ::= expr STAR expr",
 /* 205 */ "expr ::= expr SLASH expr",
 /* 206 */ "expr ::= expr REM expr",
 /* 207 */ "expr ::= expr LIKE expr",
 /* 208 */ "expr ::= expr IN LP exprlist RP",
 /* 209 */ "exprlist ::= exprlist COMMA expritem",
 /* 210 */ "exprlist ::= expritem",
 /* 211 */ "expritem ::= expr",
 /* 212 */ "expritem ::=",
 /* 213 */ "cmd ::= RESET QUERY CACHE",
 /* 214 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 215 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 216 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 217 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 218 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 219 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 220 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 221 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 222 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 226: /* keep */
    case 227: /* tagitemlist */
    case 251: /* fill_opt */
    case 253: /* groupby_opt */
    case 254: /* orderby_opt */
    case 264: /* sortlist */
    case 268: /* grouplist */
{
tVariantListDestroy((yypminor->yy494));
}
      break;
    case 243: /* columnlist */
{
tFieldListDestroy((yypminor->yy449));
}
      break;
    case 244: /* select */
{
doDestroyQuerySql((yypminor->yy150));
}
      break;
    case 247: /* selcollist */
    case 259: /* sclp */
    case 269: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy224));
}
      break;
    case 249: /* where_opt */
    case 255: /* having_opt */
    case 260: /* expr */
    case 270: /* expritem */
{
tSQLExprDestroy((yypminor->yy66));
}
      break;
    case 258: /* union */
{
destroyAllSelectClause((yypminor->yy25));
}
      break;
    case 265: /* sortitem */
{
tVariantDestroy(&(yypminor->yy312));
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
  {  207,   -1 }, /* (0) program ::= cmd */
  {  208,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  208,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  208,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  208,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  208,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  208,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  208,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  208,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  208,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  208,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  208,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  208,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  208,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  208,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  209,    0 }, /* (15) dbPrefix ::= */
  {  209,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  211,    0 }, /* (17) cpxName ::= */
  {  211,   -2 }, /* (18) cpxName ::= DOT ids */
  {  208,   -3 }, /* (19) cmd ::= SHOW dbPrefix TABLES */
  {  208,   -5 }, /* (20) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  208,   -3 }, /* (21) cmd ::= SHOW dbPrefix STABLES */
  {  208,   -5 }, /* (22) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  208,   -3 }, /* (23) cmd ::= SHOW dbPrefix VGROUPS */
  {  208,   -4 }, /* (24) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  208,   -5 }, /* (25) cmd ::= DROP TABLE ifexists ids cpxName */
  {  208,   -4 }, /* (26) cmd ::= DROP DATABASE ifexists ids */
  {  208,   -3 }, /* (27) cmd ::= DROP DNODE ids */
  {  208,   -3 }, /* (28) cmd ::= DROP USER ids */
  {  208,   -3 }, /* (29) cmd ::= DROP ACCOUNT ids */
  {  208,   -2 }, /* (30) cmd ::= USE ids */
  {  208,   -3 }, /* (31) cmd ::= DESCRIBE ids cpxName */
  {  208,   -5 }, /* (32) cmd ::= ALTER USER ids PASS ids */
  {  208,   -5 }, /* (33) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  208,   -4 }, /* (34) cmd ::= ALTER DNODE ids ids */
  {  208,   -5 }, /* (35) cmd ::= ALTER DNODE ids ids ids */
  {  208,   -3 }, /* (36) cmd ::= ALTER LOCAL ids */
  {  208,   -4 }, /* (37) cmd ::= ALTER LOCAL ids ids */
  {  208,   -4 }, /* (38) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  208,   -4 }, /* (39) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  208,   -6 }, /* (40) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  210,   -1 }, /* (41) ids ::= ID */
  {  210,   -1 }, /* (42) ids ::= STRING */
  {  212,   -2 }, /* (43) ifexists ::= IF EXISTS */
  {  212,    0 }, /* (44) ifexists ::= */
  {  215,   -3 }, /* (45) ifnotexists ::= IF NOT EXISTS */
  {  215,    0 }, /* (46) ifnotexists ::= */
  {  208,   -3 }, /* (47) cmd ::= CREATE DNODE ids */
  {  208,   -6 }, /* (48) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  208,   -5 }, /* (49) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  208,   -5 }, /* (50) cmd ::= CREATE USER ids PASS ids */
  {  217,    0 }, /* (51) pps ::= */
  {  217,   -2 }, /* (52) pps ::= PPS INTEGER */
  {  218,    0 }, /* (53) tseries ::= */
  {  218,   -2 }, /* (54) tseries ::= TSERIES INTEGER */
  {  219,    0 }, /* (55) dbs ::= */
  {  219,   -2 }, /* (56) dbs ::= DBS INTEGER */
  {  220,    0 }, /* (57) streams ::= */
  {  220,   -2 }, /* (58) streams ::= STREAMS INTEGER */
  {  221,    0 }, /* (59) storage ::= */
  {  221,   -2 }, /* (60) storage ::= STORAGE INTEGER */
  {  222,    0 }, /* (61) qtime ::= */
  {  222,   -2 }, /* (62) qtime ::= QTIME INTEGER */
  {  223,    0 }, /* (63) users ::= */
  {  223,   -2 }, /* (64) users ::= USERS INTEGER */
  {  224,    0 }, /* (65) conns ::= */
  {  224,   -2 }, /* (66) conns ::= CONNS INTEGER */
  {  225,    0 }, /* (67) state ::= */
  {  225,   -2 }, /* (68) state ::= STATE ids */
  {  214,   -9 }, /* (69) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  226,   -2 }, /* (70) keep ::= KEEP tagitemlist */
  {  228,   -2 }, /* (71) tables ::= MAXTABLES INTEGER */
  {  229,   -2 }, /* (72) cache ::= CACHE INTEGER */
  {  230,   -2 }, /* (73) replica ::= REPLICA INTEGER */
  {  231,   -2 }, /* (74) days ::= DAYS INTEGER */
  {  232,   -2 }, /* (75) minrows ::= MINROWS INTEGER */
  {  233,   -2 }, /* (76) maxrows ::= MAXROWS INTEGER */
  {  234,   -2 }, /* (77) blocks ::= BLOCKS INTEGER */
  {  235,   -2 }, /* (78) ctime ::= CTIME INTEGER */
  {  236,   -2 }, /* (79) wal ::= WAL INTEGER */
  {  237,   -2 }, /* (80) fsync ::= FSYNC INTEGER */
  {  238,   -2 }, /* (81) comp ::= COMP INTEGER */
  {  239,   -2 }, /* (82) prec ::= PRECISION STRING */
  {  216,    0 }, /* (83) db_optr ::= */
  {  216,   -2 }, /* (84) db_optr ::= db_optr tables */
  {  216,   -2 }, /* (85) db_optr ::= db_optr cache */
  {  216,   -2 }, /* (86) db_optr ::= db_optr replica */
  {  216,   -2 }, /* (87) db_optr ::= db_optr days */
  {  216,   -2 }, /* (88) db_optr ::= db_optr minrows */
  {  216,   -2 }, /* (89) db_optr ::= db_optr maxrows */
  {  216,   -2 }, /* (90) db_optr ::= db_optr blocks */
  {  216,   -2 }, /* (91) db_optr ::= db_optr ctime */
  {  216,   -2 }, /* (92) db_optr ::= db_optr wal */
  {  216,   -2 }, /* (93) db_optr ::= db_optr fsync */
  {  216,   -2 }, /* (94) db_optr ::= db_optr comp */
  {  216,   -2 }, /* (95) db_optr ::= db_optr prec */
  {  216,   -2 }, /* (96) db_optr ::= db_optr keep */
  {  213,    0 }, /* (97) alter_db_optr ::= */
  {  213,   -2 }, /* (98) alter_db_optr ::= alter_db_optr replica */
  {  213,   -2 }, /* (99) alter_db_optr ::= alter_db_optr tables */
  {  213,   -2 }, /* (100) alter_db_optr ::= alter_db_optr keep */
  {  213,   -2 }, /* (101) alter_db_optr ::= alter_db_optr blocks */
  {  213,   -2 }, /* (102) alter_db_optr ::= alter_db_optr comp */
  {  213,   -2 }, /* (103) alter_db_optr ::= alter_db_optr wal */
  {  213,   -2 }, /* (104) alter_db_optr ::= alter_db_optr fsync */
  {  240,   -1 }, /* (105) typename ::= ids */
  {  240,   -4 }, /* (106) typename ::= ids LP signed RP */
  {  241,   -1 }, /* (107) signed ::= INTEGER */
  {  241,   -2 }, /* (108) signed ::= PLUS INTEGER */
  {  241,   -2 }, /* (109) signed ::= MINUS INTEGER */
  {  208,   -6 }, /* (110) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  242,   -3 }, /* (111) create_table_args ::= LP columnlist RP */
  {  242,   -7 }, /* (112) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  242,   -7 }, /* (113) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  242,   -2 }, /* (114) create_table_args ::= AS select */
  {  243,   -3 }, /* (115) columnlist ::= columnlist COMMA column */
  {  243,   -1 }, /* (116) columnlist ::= column */
  {  245,   -2 }, /* (117) column ::= ids typename */
  {  227,   -3 }, /* (118) tagitemlist ::= tagitemlist COMMA tagitem */
  {  227,   -1 }, /* (119) tagitemlist ::= tagitem */
  {  246,   -1 }, /* (120) tagitem ::= INTEGER */
  {  246,   -1 }, /* (121) tagitem ::= FLOAT */
  {  246,   -1 }, /* (122) tagitem ::= STRING */
  {  246,   -1 }, /* (123) tagitem ::= BOOL */
  {  246,   -1 }, /* (124) tagitem ::= NULL */
  {  246,   -2 }, /* (125) tagitem ::= MINUS INTEGER */
  {  246,   -2 }, /* (126) tagitem ::= MINUS FLOAT */
  {  246,   -2 }, /* (127) tagitem ::= PLUS INTEGER */
  {  246,   -2 }, /* (128) tagitem ::= PLUS FLOAT */
  {  244,  -12 }, /* (129) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  258,   -1 }, /* (130) union ::= select */
  {  258,   -3 }, /* (131) union ::= LP union RP */
  {  258,   -4 }, /* (132) union ::= union UNION ALL select */
  {  258,   -6 }, /* (133) union ::= union UNION ALL LP select RP */
  {  208,   -1 }, /* (134) cmd ::= union */
  {  244,   -2 }, /* (135) select ::= SELECT selcollist */
  {  259,   -2 }, /* (136) sclp ::= selcollist COMMA */
  {  259,    0 }, /* (137) sclp ::= */
  {  247,   -3 }, /* (138) selcollist ::= sclp expr as */
  {  247,   -2 }, /* (139) selcollist ::= sclp STAR */
  {  261,   -2 }, /* (140) as ::= AS ids */
  {  261,   -1 }, /* (141) as ::= ids */
  {  261,    0 }, /* (142) as ::= */
  {  248,   -2 }, /* (143) from ::= FROM tablelist */
  {  262,   -2 }, /* (144) tablelist ::= ids cpxName */
  {  262,   -4 }, /* (145) tablelist ::= tablelist COMMA ids cpxName */
  {  263,   -1 }, /* (146) tmvar ::= VARIABLE */
  {  250,   -4 }, /* (147) interval_opt ::= INTERVAL LP tmvar RP */
  {  250,    0 }, /* (148) interval_opt ::= */
  {  251,    0 }, /* (149) fill_opt ::= */
  {  251,   -6 }, /* (150) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  251,   -4 }, /* (151) fill_opt ::= FILL LP ID RP */
  {  252,   -4 }, /* (152) sliding_opt ::= SLIDING LP tmvar RP */
  {  252,    0 }, /* (153) sliding_opt ::= */
  {  254,    0 }, /* (154) orderby_opt ::= */
  {  254,   -3 }, /* (155) orderby_opt ::= ORDER BY sortlist */
  {  264,   -4 }, /* (156) sortlist ::= sortlist COMMA item sortorder */
  {  264,   -2 }, /* (157) sortlist ::= item sortorder */
  {  266,   -2 }, /* (158) item ::= ids cpxName */
  {  267,   -1 }, /* (159) sortorder ::= ASC */
  {  267,   -1 }, /* (160) sortorder ::= DESC */
  {  267,    0 }, /* (161) sortorder ::= */
  {  253,    0 }, /* (162) groupby_opt ::= */
  {  253,   -3 }, /* (163) groupby_opt ::= GROUP BY grouplist */
  {  268,   -3 }, /* (164) grouplist ::= grouplist COMMA item */
  {  268,   -1 }, /* (165) grouplist ::= item */
  {  255,    0 }, /* (166) having_opt ::= */
  {  255,   -2 }, /* (167) having_opt ::= HAVING expr */
  {  257,    0 }, /* (168) limit_opt ::= */
  {  257,   -2 }, /* (169) limit_opt ::= LIMIT signed */
  {  257,   -4 }, /* (170) limit_opt ::= LIMIT signed OFFSET signed */
  {  257,   -4 }, /* (171) limit_opt ::= LIMIT signed COMMA signed */
  {  256,    0 }, /* (172) slimit_opt ::= */
  {  256,   -2 }, /* (173) slimit_opt ::= SLIMIT signed */
  {  256,   -4 }, /* (174) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  256,   -4 }, /* (175) slimit_opt ::= SLIMIT signed COMMA signed */
  {  249,    0 }, /* (176) where_opt ::= */
  {  249,   -2 }, /* (177) where_opt ::= WHERE expr */
  {  260,   -3 }, /* (178) expr ::= LP expr RP */
  {  260,   -1 }, /* (179) expr ::= ID */
  {  260,   -3 }, /* (180) expr ::= ID DOT ID */
  {  260,   -3 }, /* (181) expr ::= ID DOT STAR */
  {  260,   -1 }, /* (182) expr ::= INTEGER */
  {  260,   -2 }, /* (183) expr ::= MINUS INTEGER */
  {  260,   -2 }, /* (184) expr ::= PLUS INTEGER */
  {  260,   -1 }, /* (185) expr ::= FLOAT */
  {  260,   -2 }, /* (186) expr ::= MINUS FLOAT */
  {  260,   -2 }, /* (187) expr ::= PLUS FLOAT */
  {  260,   -1 }, /* (188) expr ::= STRING */
  {  260,   -1 }, /* (189) expr ::= NOW */
  {  260,   -1 }, /* (190) expr ::= VARIABLE */
  {  260,   -1 }, /* (191) expr ::= BOOL */
  {  260,   -4 }, /* (192) expr ::= ID LP exprlist RP */
  {  260,   -4 }, /* (193) expr ::= ID LP STAR RP */
  {  260,   -3 }, /* (194) expr ::= expr AND expr */
  {  260,   -3 }, /* (195) expr ::= expr OR expr */
  {  260,   -3 }, /* (196) expr ::= expr LT expr */
  {  260,   -3 }, /* (197) expr ::= expr GT expr */
  {  260,   -3 }, /* (198) expr ::= expr LE expr */
  {  260,   -3 }, /* (199) expr ::= expr GE expr */
  {  260,   -3 }, /* (200) expr ::= expr NE expr */
  {  260,   -3 }, /* (201) expr ::= expr EQ expr */
  {  260,   -3 }, /* (202) expr ::= expr PLUS expr */
  {  260,   -3 }, /* (203) expr ::= expr MINUS expr */
  {  260,   -3 }, /* (204) expr ::= expr STAR expr */
  {  260,   -3 }, /* (205) expr ::= expr SLASH expr */
  {  260,   -3 }, /* (206) expr ::= expr REM expr */
  {  260,   -3 }, /* (207) expr ::= expr LIKE expr */
  {  260,   -5 }, /* (208) expr ::= expr IN LP exprlist RP */
  {  269,   -3 }, /* (209) exprlist ::= exprlist COMMA expritem */
  {  269,   -1 }, /* (210) exprlist ::= expritem */
  {  270,   -1 }, /* (211) expritem ::= expr */
  {  270,    0 }, /* (212) expritem ::= */
  {  208,   -3 }, /* (213) cmd ::= RESET QUERY CACHE */
  {  208,   -7 }, /* (214) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  208,   -7 }, /* (215) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  208,   -7 }, /* (216) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  208,   -7 }, /* (217) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  208,   -8 }, /* (218) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  208,   -9 }, /* (219) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  208,   -3 }, /* (220) cmd ::= KILL CONNECTION INTEGER */
  {  208,   -5 }, /* (221) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  208,   -5 }, /* (222) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy158, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy73);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy73);}
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
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy73);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy158, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy73.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy73.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy73.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy73.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy73.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy73.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy73.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy73.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy73.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy73 = yylhsminor.yy73;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy494 = yymsp[0].minor.yy494; }
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
      case 80: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==80);
      case 81: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==81);
      case 82: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==82);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 83: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy158);}
        break;
      case 84: /* db_optr ::= db_optr tables */
      case 99: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==99);
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.maxTablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 85: /* db_optr ::= db_optr cache */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 86: /* db_optr ::= db_optr replica */
      case 98: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==98);
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 87: /* db_optr ::= db_optr days */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 88: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 89: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 90: /* db_optr ::= db_optr blocks */
      case 101: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==101);
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 91: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 92: /* db_optr ::= db_optr wal */
      case 103: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==103);
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 93: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 94: /* db_optr ::= db_optr comp */
      case 102: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==102);
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 95: /* db_optr ::= db_optr prec */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 96: /* db_optr ::= db_optr keep */
      case 100: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==100);
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.keep = yymsp[0].minor.yy494; }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 97: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy158);}
        break;
      case 104: /* alter_db_optr ::= alter_db_optr fsync */
{ yylhsminor.yy158 = yymsp[-1].minor.yy158; yylhsminor.yy158.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy158 = yylhsminor.yy158;
        break;
      case 105: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSQLSetColumnType (&yylhsminor.yy181, &yymsp[0].minor.yy0); 
}
  yymsp[0].minor.yy181 = yylhsminor.yy181;
        break;
      case 106: /* typename ::= ids LP signed RP */
{
    if (yymsp[-1].minor.yy271 <= 0) {
      yymsp[-3].minor.yy0.type = 0;
      tSQLSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
    } else {
      yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy271;          // negative value of name length
      tSQLSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
    }
}
  yymsp[-3].minor.yy181 = yylhsminor.yy181;
        break;
      case 107: /* signed ::= INTEGER */
{ yylhsminor.yy271 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy271 = yylhsminor.yy271;
        break;
      case 108: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy271 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 109: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy271 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 110: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 111: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy374 = tSetCreateSQLElems(yymsp[-1].minor.yy449, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy374, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 112: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy374 = tSetCreateSQLElems(yymsp[-5].minor.yy449, yymsp[-1].minor.yy449, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy374, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 113: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy374 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy494, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy374, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 114: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy374 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy150, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy374, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 115: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy449 = tFieldListAppend(yymsp[-2].minor.yy449, &yymsp[0].minor.yy181);   }
  yymsp[-2].minor.yy449 = yylhsminor.yy449;
        break;
      case 116: /* columnlist ::= column */
{yylhsminor.yy449 = tFieldListAppend(NULL, &yymsp[0].minor.yy181);}
  yymsp[0].minor.yy449 = yylhsminor.yy449;
        break;
      case 117: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy181, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy181);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 118: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy494 = tVariantListAppend(yymsp[-2].minor.yy494, &yymsp[0].minor.yy312, -1);    }
  yymsp[-2].minor.yy494 = yylhsminor.yy494;
        break;
      case 119: /* tagitemlist ::= tagitem */
{ yylhsminor.yy494 = tVariantListAppend(NULL, &yymsp[0].minor.yy312, -1); }
  yymsp[0].minor.yy494 = yylhsminor.yy494;
        break;
      case 120: /* tagitem ::= INTEGER */
      case 121: /* tagitem ::= FLOAT */ yytestcase(yyruleno==121);
      case 122: /* tagitem ::= STRING */ yytestcase(yyruleno==122);
      case 123: /* tagitem ::= BOOL */ yytestcase(yyruleno==123);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy312, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy312 = yylhsminor.yy312;
        break;
      case 124: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy312, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy312 = yylhsminor.yy312;
        break;
      case 125: /* tagitem ::= MINUS INTEGER */
      case 126: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==126);
      case 127: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==127);
      case 128: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==128);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy312, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy312 = yylhsminor.yy312;
        break;
      case 129: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy150 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy224, yymsp[-9].minor.yy494, yymsp[-8].minor.yy66, yymsp[-4].minor.yy494, yymsp[-3].minor.yy494, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy494, &yymsp[0].minor.yy188, &yymsp[-1].minor.yy188);
}
  yymsp[-11].minor.yy150 = yylhsminor.yy150;
        break;
      case 130: /* union ::= select */
{ yylhsminor.yy25 = setSubclause(NULL, yymsp[0].minor.yy150); }
  yymsp[0].minor.yy25 = yylhsminor.yy25;
        break;
      case 131: /* union ::= LP union RP */
{ yymsp[-2].minor.yy25 = yymsp[-1].minor.yy25; }
        break;
      case 132: /* union ::= union UNION ALL select */
{ yylhsminor.yy25 = appendSelectClause(yymsp[-3].minor.yy25, yymsp[0].minor.yy150); }
  yymsp[-3].minor.yy25 = yylhsminor.yy25;
        break;
      case 133: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy25 = appendSelectClause(yymsp[-5].minor.yy25, yymsp[-1].minor.yy150); }
  yymsp[-5].minor.yy25 = yylhsminor.yy25;
        break;
      case 134: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy25, NULL, TSDB_SQL_SELECT); }
        break;
      case 135: /* select ::= SELECT selcollist */
{
  yylhsminor.yy150 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy224, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy150 = yylhsminor.yy150;
        break;
      case 136: /* sclp ::= selcollist COMMA */
{yylhsminor.yy224 = yymsp[-1].minor.yy224;}
  yymsp[-1].minor.yy224 = yylhsminor.yy224;
        break;
      case 137: /* sclp ::= */
{yymsp[1].minor.yy224 = 0;}
        break;
      case 138: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy224 = tSQLExprListAppend(yymsp[-2].minor.yy224, yymsp[-1].minor.yy66, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy224 = yylhsminor.yy224;
        break;
      case 139: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy224 = tSQLExprListAppend(yymsp[-1].minor.yy224, pNode, 0);
}
  yymsp[-1].minor.yy224 = yylhsminor.yy224;
        break;
      case 140: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 141: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 142: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 143: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy494 = yymsp[0].minor.yy494;}
        break;
      case 144: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy494 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy494 = yylhsminor.yy494;
        break;
      case 145: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy494 = tVariantListAppendToken(yymsp[-3].minor.yy494, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy494 = yylhsminor.yy494;
        break;
      case 146: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 147: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 152: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==152);
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 148: /* interval_opt ::= */
      case 153: /* sliding_opt ::= */ yytestcase(yyruleno==153);
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 149: /* fill_opt ::= */
{yymsp[1].minor.yy494 = 0;     }
        break;
      case 150: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy494, &A, -1, 0);
    yymsp[-5].minor.yy494 = yymsp[-1].minor.yy494;
}
        break;
      case 151: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy494 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 154: /* orderby_opt ::= */
      case 162: /* groupby_opt ::= */ yytestcase(yyruleno==162);
{yymsp[1].minor.yy494 = 0;}
        break;
      case 155: /* orderby_opt ::= ORDER BY sortlist */
      case 163: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==163);
{yymsp[-2].minor.yy494 = yymsp[0].minor.yy494;}
        break;
      case 156: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy494 = tVariantListAppend(yymsp[-3].minor.yy494, &yymsp[-1].minor.yy312, yymsp[0].minor.yy82);
}
  yymsp[-3].minor.yy494 = yylhsminor.yy494;
        break;
      case 157: /* sortlist ::= item sortorder */
{
  yylhsminor.yy494 = tVariantListAppend(NULL, &yymsp[-1].minor.yy312, yymsp[0].minor.yy82);
}
  yymsp[-1].minor.yy494 = yylhsminor.yy494;
        break;
      case 158: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy312, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy312 = yylhsminor.yy312;
        break;
      case 159: /* sortorder ::= ASC */
{yymsp[0].minor.yy82 = TSDB_ORDER_ASC; }
        break;
      case 160: /* sortorder ::= DESC */
{yymsp[0].minor.yy82 = TSDB_ORDER_DESC;}
        break;
      case 161: /* sortorder ::= */
{yymsp[1].minor.yy82 = TSDB_ORDER_ASC;}
        break;
      case 164: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy494 = tVariantListAppend(yymsp[-2].minor.yy494, &yymsp[0].minor.yy312, -1);
}
  yymsp[-2].minor.yy494 = yylhsminor.yy494;
        break;
      case 165: /* grouplist ::= item */
{
  yylhsminor.yy494 = tVariantListAppend(NULL, &yymsp[0].minor.yy312, -1);
}
  yymsp[0].minor.yy494 = yylhsminor.yy494;
        break;
      case 166: /* having_opt ::= */
      case 176: /* where_opt ::= */ yytestcase(yyruleno==176);
      case 212: /* expritem ::= */ yytestcase(yyruleno==212);
{yymsp[1].minor.yy66 = 0;}
        break;
      case 167: /* having_opt ::= HAVING expr */
      case 177: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==177);
{yymsp[-1].minor.yy66 = yymsp[0].minor.yy66;}
        break;
      case 168: /* limit_opt ::= */
      case 172: /* slimit_opt ::= */ yytestcase(yyruleno==172);
{yymsp[1].minor.yy188.limit = -1; yymsp[1].minor.yy188.offset = 0;}
        break;
      case 169: /* limit_opt ::= LIMIT signed */
      case 173: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==173);
{yymsp[-1].minor.yy188.limit = yymsp[0].minor.yy271;  yymsp[-1].minor.yy188.offset = 0;}
        break;
      case 170: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 174: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==174);
{yymsp[-3].minor.yy188.limit = yymsp[-2].minor.yy271;  yymsp[-3].minor.yy188.offset = yymsp[0].minor.yy271;}
        break;
      case 171: /* limit_opt ::= LIMIT signed COMMA signed */
      case 175: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==175);
{yymsp[-3].minor.yy188.limit = yymsp[0].minor.yy271;  yymsp[-3].minor.yy188.offset = yymsp[-2].minor.yy271;}
        break;
      case 178: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy66 = yymsp[-1].minor.yy66; }
        break;
      case 179: /* expr ::= ID */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 180: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 181: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 182: /* expr ::= INTEGER */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 183: /* expr ::= MINUS INTEGER */
      case 184: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==184);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy66 = yylhsminor.yy66;
        break;
      case 185: /* expr ::= FLOAT */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 186: /* expr ::= MINUS FLOAT */
      case 187: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==187);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy66 = yylhsminor.yy66;
        break;
      case 188: /* expr ::= STRING */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 189: /* expr ::= NOW */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 190: /* expr ::= VARIABLE */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 191: /* expr ::= BOOL */
{yylhsminor.yy66 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 192: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy66 = tSQLExprCreateFunction(yymsp[-1].minor.yy224, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy66 = yylhsminor.yy66;
        break;
      case 193: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy66 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy66 = yylhsminor.yy66;
        break;
      case 194: /* expr ::= expr AND expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_AND);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 195: /* expr ::= expr OR expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_OR); }
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 196: /* expr ::= expr LT expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_LT);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 197: /* expr ::= expr GT expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_GT);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 198: /* expr ::= expr LE expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_LE);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 199: /* expr ::= expr GE expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_GE);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 200: /* expr ::= expr NE expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_NE);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 201: /* expr ::= expr EQ expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_EQ);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 202: /* expr ::= expr PLUS expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_PLUS);  }
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 203: /* expr ::= expr MINUS expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_MINUS); }
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 204: /* expr ::= expr STAR expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_STAR);  }
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 205: /* expr ::= expr SLASH expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_DIVIDE);}
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 206: /* expr ::= expr REM expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_REM);   }
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 207: /* expr ::= expr LIKE expr */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-2].minor.yy66, yymsp[0].minor.yy66, TK_LIKE);  }
  yymsp[-2].minor.yy66 = yylhsminor.yy66;
        break;
      case 208: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy66 = tSQLExprCreate(yymsp[-4].minor.yy66, (tSQLExpr*)yymsp[-1].minor.yy224, TK_IN); }
  yymsp[-4].minor.yy66 = yylhsminor.yy66;
        break;
      case 209: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy224 = tSQLExprListAppend(yymsp[-2].minor.yy224,yymsp[0].minor.yy66,0);}
  yymsp[-2].minor.yy224 = yylhsminor.yy224;
        break;
      case 210: /* exprlist ::= expritem */
{yylhsminor.yy224 = tSQLExprListAppend(0,yymsp[0].minor.yy66,0);}
  yymsp[0].minor.yy224 = yylhsminor.yy224;
        break;
      case 211: /* expritem ::= expr */
{yylhsminor.yy66 = yymsp[0].minor.yy66;}
  yymsp[0].minor.yy66 = yylhsminor.yy66;
        break;
      case 213: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 214: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy449, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 215: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 216: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy449, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 217: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 218: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 219: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy312, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 220: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 221: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 222: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
