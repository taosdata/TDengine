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
#define YYNOCODE 274
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy46;
  tSQLExpr* yy64;
  tVariant yy134;
  SCreateAcctSQL yy149;
  int64_t yy207;
  SLimitVal yy216;
  TAOS_FIELD yy223;
  SSubclauseInfo* yy231;
  SCreateDBInfo yy268;
  tSQLExprList* yy290;
  SQuerySQL* yy414;
  SCreateTableSQL* yy470;
  tVariantList* yy498;
  tFieldList* yy523;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             243
#define YYNRULE              226
#define YYNTOKEN             207
#define YY_MAX_SHIFT         242
#define YY_MIN_SHIFTREDUCE   405
#define YY_MAX_SHIFTREDUCE   630
#define YY_ERROR_ACTION      631
#define YY_ACCEPT_ACTION     632
#define YY_NO_ACTION         633
#define YY_MIN_REDUCE        634
#define YY_MAX_REDUCE        859
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
#define YY_ACTTAB_COUNT (552)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   103,  446,  135,  673,  632,  242,  126,  515,  135,  447,
 /*    10 */   135,  158,  847,   41,   43,   11,   35,   36,  846,  157,
 /*    20 */   847,   29,  134,  446,  197,   39,   37,   40,   38,  155,
 /*    30 */   103,  447,  139,   34,   33,  217,  216,   32,   31,   30,
 /*    40 */    41,   43,  767,   35,   36,   32,   31,   30,   29,  756,
 /*    50 */   446,  197,   39,   37,   40,   38,  182,  802,  447,  192,
 /*    60 */    34,   33,   21,   21,   32,   31,   30,  406,  407,  408,
 /*    70 */   409,  410,  411,  412,  413,  414,  415,  416,  417,  241,
 /*    80 */    41,   43,  228,   35,   36,  194,  843,   58,   29,   21,
 /*    90 */   842,  197,   39,   37,   40,   38,  166,  167,  753,  753,
 /*   100 */    34,   33,  168,   56,   32,   31,   30,  778,  841,   16,
 /*   110 */   235,  208,  234,  233,  207,  206,  205,  232,  204,  231,
 /*   120 */   230,  229,  203,  215,  151,  753,  732,  586,  719,  720,
 /*   130 */   721,  722,  723,  724,  725,  726,  727,  728,  729,  730,
 /*   140 */   731,   43,    8,   35,   36,   61,  113,   21,   29,  153,
 /*   150 */   240,  197,   39,   37,   40,   38,  239,  238,   95,  775,
 /*   160 */    34,   33,  165,   99,   32,   31,   30,  169,   35,   36,
 /*   170 */   214,  213,  592,   29,  595,  103,  197,   39,   37,   40,
 /*   180 */    38,  220,  756,  753,  236,   34,   33,  175,   12,   32,
 /*   190 */    31,   30,  162,  599,  179,  178,  590,  767,  593,  103,
 /*   200 */   596,  161,  162,  599,  756,   17,  590,  148,  593,  152,
 /*   210 */   596,  154,   26,   88,   87,  142,  185,  567,  568,   16,
 /*   220 */   235,  147,  234,  233,  159,  160,  219,  232,  196,  231,
 /*   230 */   230,  229,  801,   76,  159,  160,  162,  599,  547,  228,
 /*   240 */   590,    3,  593,   17,  596,   74,   78,   83,   86,   77,
 /*   250 */    26,   39,   37,   40,   38,   80,   59,  754,   21,   34,
 /*   260 */    33,  544,   60,   32,   31,   30,   18,  140,  159,  160,
 /*   270 */   181,  737,  539,  736,   27,  734,  735,  150,  682,  184,
 /*   280 */   738,  126,  740,  741,  739,  674,  531,  141,  126,  528,
 /*   290 */    42,  529,  558,  530,  752,  591,   46,  594,   34,   33,
 /*   300 */    42,  598,   32,   31,   30,  116,  117,   68,   64,   67,
 /*   310 */   588,  598,  143,   50,   73,   72,  597,  170,  171,  130,
 /*   320 */   128,   91,   90,   89,   98,   47,  597,  144,  559,  616,
 /*   330 */    51,   26,   14,   13,   42,  145,  600,  521,  520,  201,
 /*   340 */    13,   46,   22,   22,   48,  598,  589,   10,    9,  535,
 /*   350 */   533,  536,  534,   85,   84,  146,  137,  133,  856,  138,
 /*   360 */   597,  136,  755,  812,  811,  163,  808,  807,  164,  777,
 /*   370 */   747,  218,  794,  100,  793,  769,  114,  115,   26,  684,
 /*   380 */   112,  202,  131,  183,   24,  211,  681,  212,  855,  532,
 /*   390 */    70,  854,   93,  852,  118,  702,  554,   25,   23,  132,
 /*   400 */   671,   79,   52,  186,  669,   81,   82,  667,  666,  172,
 /*   410 */   127,  664,  190,  663,  662,  661,  660,  652,  129,  658,
 /*   420 */    49,  656,  654,  766,  781,  782,  795,  104,  195,   44,
 /*   430 */   193,  191,  189,  187,  210,  105,   75,   28,  221,  199,
 /*   440 */   222,  223,   53,  225,  224,  149,  226,   62,   65,  703,
 /*   450 */   227,  237,  630,  173,  174,  629,  177,  665,  628,  119,
 /*   460 */   176,   92,  121,  125,  120,  751,  122,  123,  659,  124,
 /*   470 */   108,  106,  107,  109,  110,  111,   94,    1,    2,  621,
 /*   480 */   180,  184,  541,   55,   57,  555,  101,  156,  188,  198,
 /*   490 */    19,   63,    5,  560,  102,    4,    6,  601,   20,   15,
 /*   500 */     7,  488,  484,  200,  482,  481,  480,  477,  450,  209,
 /*   510 */    66,   45,   22,   69,   71,  517,  516,  514,  471,   54,
 /*   520 */   469,  461,  467,  463,  465,  459,  457,  487,  486,  485,
 /*   530 */   483,  479,  478,  476,   46,  448,  421,  419,  634,  633,
 /*   540 */   633,  633,  633,  633,  633,  633,  633,  633,  633,  633,
 /*   550 */    96,   97,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   211,    1,  262,  215,  208,  209,  218,    5,  262,    9,
 /*    10 */   262,  271,  272,   13,   14,  262,   16,   17,  272,  271,
 /*    20 */   272,   21,  262,    1,   24,   25,   26,   27,   28,  228,
 /*    30 */   211,    9,  262,   33,   34,   33,   34,   37,   38,   39,
 /*    40 */    13,   14,  246,   16,   17,   37,   38,   39,   21,  248,
 /*    50 */     1,   24,   25,   26,   27,   28,  260,  268,    9,  270,
 /*    60 */    33,   34,  211,  211,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    13,   14,   78,   16,   17,  266,  262,  268,   21,  211,
 /*    90 */   262,   24,   25,   26,   27,   28,  245,  245,  247,  247,
 /*   100 */    33,   34,   63,  103,   37,   38,   39,  211,  262,   85,
 /*   110 */    86,   87,   88,   89,   90,   91,   92,   93,   94,   95,
 /*   120 */    96,   97,   98,  245,  262,  247,  227,  100,  229,  230,
 /*   130 */   231,  232,  233,  234,  235,  236,  237,  238,  239,  240,
 /*   140 */   241,   14,   99,   16,   17,  102,  103,  211,   21,  210,
 /*   150 */   211,   24,   25,   26,   27,   28,   60,   61,   62,  263,
 /*   160 */    33,   34,  228,  211,   37,   38,   39,  128,   16,   17,
 /*   170 */   131,  132,    5,   21,    7,  211,   24,   25,   26,   27,
 /*   180 */    28,  245,  248,  247,  228,   33,   34,  127,   44,   37,
 /*   190 */    38,   39,    1,    2,  134,  135,    5,  246,    7,  211,
 /*   200 */     9,   59,    1,    2,  248,   99,    5,   63,    7,  262,
 /*   210 */     9,  260,  106,   69,   70,   71,  264,  116,  117,   85,
 /*   220 */    86,   77,   88,   89,   33,   34,  211,   93,   37,   95,
 /*   230 */    96,   97,  268,   72,   33,   34,    1,    2,   37,   78,
 /*   240 */     5,   99,    7,   99,    9,   64,   65,   66,   67,   68,
 /*   250 */   106,   25,   26,   27,   28,   74,  268,  242,  211,   33,
 /*   260 */    34,  104,  249,   37,   38,   39,  109,  262,   33,   34,
 /*   270 */   126,  227,  100,  229,  261,  231,  232,  133,  215,  107,
 /*   280 */   236,  218,  238,  239,  240,  215,    2,  262,  218,    5,
 /*   290 */    99,    7,  100,    9,  247,    5,  104,    7,   33,   34,
 /*   300 */    99,  110,   37,   38,   39,   64,   65,   66,   67,   68,
 /*   310 */     1,  110,  262,  104,  129,  130,  125,   33,   34,   64,
 /*   320 */    65,   66,   67,   68,   99,  104,  125,  262,  100,  100,
 /*   330 */   121,  106,  104,  104,   99,  262,  100,  100,  100,  100,
 /*   340 */   104,  104,  104,  104,  123,  110,   37,  129,  130,    5,
 /*   350 */     5,    7,    7,   72,   73,  262,  262,  262,  248,  262,
 /*   360 */   125,  262,  248,  243,  243,  243,  243,  243,  243,  211,
 /*   370 */   244,  243,  269,  211,  269,  246,  211,  211,  106,  211,
 /*   380 */   250,  211,  211,  246,  211,  211,  211,  211,  211,  105,
 /*   390 */   211,  211,   59,  211,  211,  211,  110,  211,  211,  211,
 /*   400 */   211,  211,  120,  265,  211,  211,  211,  211,  211,  211,
 /*   410 */   211,  211,  265,  211,  211,  211,  211,  211,  211,  211,
 /*   420 */   122,  211,  211,  259,  212,  212,  212,  258,  114,  119,
 /*   430 */   118,  113,  112,  111,   75,  257,   84,  124,   83,  212,
 /*   440 */    49,   80,  212,   53,   82,  212,   81,  216,  216,  226,
 /*   450 */    79,   75,    5,  136,    5,    5,    5,  212,    5,  225,
 /*   460 */   136,  213,  220,  219,  224,  246,  223,  221,  212,  222,
 /*   470 */   254,  256,  255,  253,  252,  251,  213,  217,  214,   87,
 /*   480 */   127,  107,  100,  108,  104,  100,   99,    1,   99,  101,
 /*   490 */   104,   72,  115,  100,   99,   99,  115,  100,  104,   99,
 /*   500 */    99,    9,    5,  101,    5,    5,    5,    5,   76,   15,
 /*   510 */    72,   16,  104,  130,  130,    5,    5,  100,    5,   99,
 /*   520 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   530 */     5,    5,    5,    5,  104,   76,   59,   58,    0,  273,
 /*   540 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   550 */    21,   21,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   560 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   570 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   580 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   590 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   600 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   610 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   620 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   630 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   640 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   650 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   660 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   670 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   680 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   690 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   700 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   710 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   720 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   730 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   740 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   750 */   273,  273,  273,  273,  273,  273,  273,  273,  273,
};
#define YY_SHIFT_COUNT    (242)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (538)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   144,   24,  134,  191,  235,   49,   49,   49,   49,   49,
 /*    10 */    49,    0,   22,  235,  284,  284,  284,  106,   49,   49,
 /*    20 */    49,   49,   49,  161,    4,    4,  552,  201,  235,  235,
 /*    30 */   235,  235,  235,  235,  235,  235,  235,  235,  235,  235,
 /*    40 */   235,  235,  235,  235,  235,  284,  284,    2,    2,    2,
 /*    50 */     2,    2,    2,   43,    2,  225,   49,   49,  101,  101,
 /*    60 */   157,   49,   49,   49,   49,   49,   49,   49,   49,   49,
 /*    70 */    49,   49,   49,   49,   49,   49,   49,   49,   49,   49,
 /*    80 */    49,   49,   49,   49,   49,   49,   49,   49,   49,   49,
 /*    90 */    49,   49,   49,   49,   49,   49,   49,   49,  272,  333,
 /*   100 */   333,  286,  286,  333,  282,  298,  310,  314,  312,  318,
 /*   110 */   320,  322,  313,  272,  333,  333,  359,  359,  333,  352,
 /*   120 */   355,  391,  361,  362,  390,  365,  371,  333,  376,  333,
 /*   130 */   376,  552,  552,   27,   67,   67,   67,  127,  152,  226,
 /*   140 */   226,  226,  181,  265,  265,  265,  265,  241,  255,   39,
 /*   150 */    60,    8,    8,   96,  172,  192,  228,  229,  236,  167,
 /*   160 */   290,  309,  142,  221,  209,  237,  238,  239,  185,  218,
 /*   170 */   344,  345,  281,  447,  317,  449,  450,  324,  451,  453,
 /*   180 */   392,  353,  374,  382,  375,  380,  385,  387,  486,  389,
 /*   190 */   393,  395,  386,  377,  394,  381,  397,  396,  400,  388,
 /*   200 */   401,  402,  419,  492,  497,  499,  500,  501,  502,  432,
 /*   210 */   494,  438,  495,  383,  384,  408,  510,  511,  417,  420,
 /*   220 */   408,  513,  515,  516,  517,  518,  519,  520,  521,  522,
 /*   230 */   523,  524,  525,  526,  527,  528,  430,  459,  529,  530,
 /*   240 */   477,  479,  538,
};
#define YY_REDUCE_COUNT (132)
#define YY_REDUCE_MIN   (-260)
#define YY_REDUCE_MAX   (264)
static const short yy_reduce_ofst[] = {
 /*     0 */  -204, -101,   44, -260, -252, -211, -181, -149, -148, -122,
 /*    10 */   -64, -104,  -61, -254, -199,  -66,  -44,  -49,  -48,  -36,
 /*    20 */   -12,   15,   47, -212,   63,   70,   13, -247, -240, -230,
 /*    30 */  -176, -172, -154, -138,  -53,    5,   25,   50,   65,   73,
 /*    40 */    93,   94,   95,   97,   99,  110,  114,  120,  121,  122,
 /*    50 */   123,  124,  125,  126,  128,  129,  158,  162,  103,  105,
 /*    60 */   130,  165,  166,  168,  170,  171,  173,  174,  175,  176,
 /*    70 */   177,  179,  180,  182,  183,  184,  186,  187,  188,  189,
 /*    80 */   190,  193,  194,  195,  196,  197,  198,  199,  200,  202,
 /*    90 */   203,  204,  205,  206,  207,  208,  210,  211,  137,  212,
 /*   100 */   213,  138,  147,  214,  164,  169,  178,  215,  217,  216,
 /*   110 */   220,  222,  224,  219,  227,  230,  231,  232,  233,  223,
 /*   120 */   234,  240,  242,  243,  246,  247,  244,  245,  248,  256,
 /*   130 */   263,  260,  264,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   631,  683,  672,  849,  849,  631,  631,  631,  631,  631,
 /*    10 */   631,  779,  649,  849,  631,  631,  631,  631,  631,  631,
 /*    20 */   631,  631,  631,  685,  685,  685,  774,  631,  631,  631,
 /*    30 */   631,  631,  631,  631,  631,  631,  631,  631,  631,  631,
 /*    40 */   631,  631,  631,  631,  631,  631,  631,  631,  631,  631,
 /*    50 */   631,  631,  631,  631,  631,  631,  631,  631,  798,  798,
 /*    60 */   772,  631,  631,  631,  631,  631,  631,  631,  631,  631,
 /*    70 */   631,  631,  631,  631,  631,  631,  631,  631,  631,  670,
 /*    80 */   631,  668,  631,  631,  631,  631,  631,  631,  631,  631,
 /*    90 */   631,  631,  631,  631,  631,  657,  631,  631,  631,  651,
 /*   100 */   651,  631,  631,  651,  805,  809,  803,  791,  799,  790,
 /*   110 */   786,  785,  813,  631,  651,  651,  680,  680,  651,  701,
 /*   120 */   699,  697,  689,  695,  691,  693,  687,  651,  678,  651,
 /*   130 */   678,  718,  733,  631,  814,  848,  804,  832,  831,  844,
 /*   140 */   838,  837,  631,  836,  835,  834,  833,  631,  631,  631,
 /*   150 */   631,  840,  839,  631,  631,  631,  631,  631,  631,  631,
 /*   160 */   631,  631,  816,  810,  806,  631,  631,  631,  631,  631,
 /*   170 */   631,  631,  631,  631,  631,  631,  631,  631,  631,  631,
 /*   180 */   631,  631,  771,  631,  631,  780,  631,  631,  631,  631,
 /*   190 */   631,  631,  800,  631,  792,  631,  631,  631,  631,  631,
 /*   200 */   631,  748,  631,  631,  631,  631,  631,  631,  631,  631,
 /*   210 */   631,  631,  631,  631,  631,  853,  631,  631,  631,  742,
 /*   220 */   851,  631,  631,  631,  631,  631,  631,  631,  631,  631,
 /*   230 */   631,  631,  631,  631,  631,  631,  704,  631,  655,  653,
 /*   240 */   631,  647,  631,
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
  /*   89 */ "QUORUM",
  /*   90 */ "DAYS",
  /*   91 */ "MINROWS",
  /*   92 */ "MAXROWS",
  /*   93 */ "BLOCKS",
  /*   94 */ "CTIME",
  /*   95 */ "WAL",
  /*   96 */ "FSYNC",
  /*   97 */ "COMP",
  /*   98 */ "PRECISION",
  /*   99 */ "LP",
  /*  100 */ "RP",
  /*  101 */ "TAGS",
  /*  102 */ "USING",
  /*  103 */ "AS",
  /*  104 */ "COMMA",
  /*  105 */ "NULL",
  /*  106 */ "SELECT",
  /*  107 */ "UNION",
  /*  108 */ "ALL",
  /*  109 */ "FROM",
  /*  110 */ "VARIABLE",
  /*  111 */ "INTERVAL",
  /*  112 */ "FILL",
  /*  113 */ "SLIDING",
  /*  114 */ "ORDER",
  /*  115 */ "BY",
  /*  116 */ "ASC",
  /*  117 */ "DESC",
  /*  118 */ "GROUP",
  /*  119 */ "HAVING",
  /*  120 */ "LIMIT",
  /*  121 */ "OFFSET",
  /*  122 */ "SLIMIT",
  /*  123 */ "SOFFSET",
  /*  124 */ "WHERE",
  /*  125 */ "NOW",
  /*  126 */ "RESET",
  /*  127 */ "QUERY",
  /*  128 */ "ADD",
  /*  129 */ "COLUMN",
  /*  130 */ "TAG",
  /*  131 */ "CHANGE",
  /*  132 */ "SET",
  /*  133 */ "KILL",
  /*  134 */ "CONNECTION",
  /*  135 */ "STREAM",
  /*  136 */ "COLON",
  /*  137 */ "ABORT",
  /*  138 */ "AFTER",
  /*  139 */ "ATTACH",
  /*  140 */ "BEFORE",
  /*  141 */ "BEGIN",
  /*  142 */ "CASCADE",
  /*  143 */ "CLUSTER",
  /*  144 */ "CONFLICT",
  /*  145 */ "COPY",
  /*  146 */ "DEFERRED",
  /*  147 */ "DELIMITERS",
  /*  148 */ "DETACH",
  /*  149 */ "EACH",
  /*  150 */ "END",
  /*  151 */ "EXPLAIN",
  /*  152 */ "FAIL",
  /*  153 */ "FOR",
  /*  154 */ "IGNORE",
  /*  155 */ "IMMEDIATE",
  /*  156 */ "INITIALLY",
  /*  157 */ "INSTEAD",
  /*  158 */ "MATCH",
  /*  159 */ "KEY",
  /*  160 */ "OF",
  /*  161 */ "RAISE",
  /*  162 */ "REPLACE",
  /*  163 */ "RESTRICT",
  /*  164 */ "ROW",
  /*  165 */ "STATEMENT",
  /*  166 */ "TRIGGER",
  /*  167 */ "VIEW",
  /*  168 */ "COUNT",
  /*  169 */ "SUM",
  /*  170 */ "AVG",
  /*  171 */ "MIN",
  /*  172 */ "MAX",
  /*  173 */ "FIRST",
  /*  174 */ "LAST",
  /*  175 */ "TOP",
  /*  176 */ "BOTTOM",
  /*  177 */ "STDDEV",
  /*  178 */ "PERCENTILE",
  /*  179 */ "APERCENTILE",
  /*  180 */ "LEASTSQUARES",
  /*  181 */ "HISTOGRAM",
  /*  182 */ "DIFF",
  /*  183 */ "SPREAD",
  /*  184 */ "TWA",
  /*  185 */ "INTERP",
  /*  186 */ "LAST_ROW",
  /*  187 */ "RATE",
  /*  188 */ "IRATE",
  /*  189 */ "SUM_RATE",
  /*  190 */ "SUM_IRATE",
  /*  191 */ "AVG_RATE",
  /*  192 */ "AVG_IRATE",
  /*  193 */ "TBID",
  /*  194 */ "SEMI",
  /*  195 */ "NONE",
  /*  196 */ "PREV",
  /*  197 */ "LINEAR",
  /*  198 */ "IMPORT",
  /*  199 */ "METRIC",
  /*  200 */ "TBNAME",
  /*  201 */ "JOIN",
  /*  202 */ "METRICS",
  /*  203 */ "STABLE",
  /*  204 */ "INSERT",
  /*  205 */ "INTO",
  /*  206 */ "VALUES",
  /*  207 */ "error",
  /*  208 */ "program",
  /*  209 */ "cmd",
  /*  210 */ "dbPrefix",
  /*  211 */ "ids",
  /*  212 */ "cpxName",
  /*  213 */ "ifexists",
  /*  214 */ "alter_db_optr",
  /*  215 */ "acct_optr",
  /*  216 */ "ifnotexists",
  /*  217 */ "db_optr",
  /*  218 */ "pps",
  /*  219 */ "tseries",
  /*  220 */ "dbs",
  /*  221 */ "streams",
  /*  222 */ "storage",
  /*  223 */ "qtime",
  /*  224 */ "users",
  /*  225 */ "conns",
  /*  226 */ "state",
  /*  227 */ "keep",
  /*  228 */ "tagitemlist",
  /*  229 */ "tables",
  /*  230 */ "cache",
  /*  231 */ "replica",
  /*  232 */ "quorum",
  /*  233 */ "days",
  /*  234 */ "minrows",
  /*  235 */ "maxrows",
  /*  236 */ "blocks",
  /*  237 */ "ctime",
  /*  238 */ "wal",
  /*  239 */ "fsync",
  /*  240 */ "comp",
  /*  241 */ "prec",
  /*  242 */ "typename",
  /*  243 */ "signed",
  /*  244 */ "create_table_args",
  /*  245 */ "columnlist",
  /*  246 */ "select",
  /*  247 */ "column",
  /*  248 */ "tagitem",
  /*  249 */ "selcollist",
  /*  250 */ "from",
  /*  251 */ "where_opt",
  /*  252 */ "interval_opt",
  /*  253 */ "fill_opt",
  /*  254 */ "sliding_opt",
  /*  255 */ "groupby_opt",
  /*  256 */ "orderby_opt",
  /*  257 */ "having_opt",
  /*  258 */ "slimit_opt",
  /*  259 */ "limit_opt",
  /*  260 */ "union",
  /*  261 */ "sclp",
  /*  262 */ "expr",
  /*  263 */ "as",
  /*  264 */ "tablelist",
  /*  265 */ "tmvar",
  /*  266 */ "sortlist",
  /*  267 */ "sortitem",
  /*  268 */ "item",
  /*  269 */ "sortorder",
  /*  270 */ "grouplist",
  /*  271 */ "exprlist",
  /*  272 */ "expritem",
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
 /*  74 */ "quorum ::= QUORUM INTEGER",
 /*  75 */ "days ::= DAYS INTEGER",
 /*  76 */ "minrows ::= MINROWS INTEGER",
 /*  77 */ "maxrows ::= MAXROWS INTEGER",
 /*  78 */ "blocks ::= BLOCKS INTEGER",
 /*  79 */ "ctime ::= CTIME INTEGER",
 /*  80 */ "wal ::= WAL INTEGER",
 /*  81 */ "fsync ::= FSYNC INTEGER",
 /*  82 */ "comp ::= COMP INTEGER",
 /*  83 */ "prec ::= PRECISION STRING",
 /*  84 */ "db_optr ::=",
 /*  85 */ "db_optr ::= db_optr tables",
 /*  86 */ "db_optr ::= db_optr cache",
 /*  87 */ "db_optr ::= db_optr replica",
 /*  88 */ "db_optr ::= db_optr quorum",
 /*  89 */ "db_optr ::= db_optr days",
 /*  90 */ "db_optr ::= db_optr minrows",
 /*  91 */ "db_optr ::= db_optr maxrows",
 /*  92 */ "db_optr ::= db_optr blocks",
 /*  93 */ "db_optr ::= db_optr ctime",
 /*  94 */ "db_optr ::= db_optr wal",
 /*  95 */ "db_optr ::= db_optr fsync",
 /*  96 */ "db_optr ::= db_optr comp",
 /*  97 */ "db_optr ::= db_optr prec",
 /*  98 */ "db_optr ::= db_optr keep",
 /*  99 */ "alter_db_optr ::=",
 /* 100 */ "alter_db_optr ::= alter_db_optr replica",
 /* 101 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 102 */ "alter_db_optr ::= alter_db_optr tables",
 /* 103 */ "alter_db_optr ::= alter_db_optr keep",
 /* 104 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 105 */ "alter_db_optr ::= alter_db_optr comp",
 /* 106 */ "alter_db_optr ::= alter_db_optr wal",
 /* 107 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 108 */ "typename ::= ids",
 /* 109 */ "typename ::= ids LP signed RP",
 /* 110 */ "signed ::= INTEGER",
 /* 111 */ "signed ::= PLUS INTEGER",
 /* 112 */ "signed ::= MINUS INTEGER",
 /* 113 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 114 */ "create_table_args ::= LP columnlist RP",
 /* 115 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 116 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 117 */ "create_table_args ::= AS select",
 /* 118 */ "columnlist ::= columnlist COMMA column",
 /* 119 */ "columnlist ::= column",
 /* 120 */ "column ::= ids typename",
 /* 121 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 122 */ "tagitemlist ::= tagitem",
 /* 123 */ "tagitem ::= INTEGER",
 /* 124 */ "tagitem ::= FLOAT",
 /* 125 */ "tagitem ::= STRING",
 /* 126 */ "tagitem ::= BOOL",
 /* 127 */ "tagitem ::= NULL",
 /* 128 */ "tagitem ::= MINUS INTEGER",
 /* 129 */ "tagitem ::= MINUS FLOAT",
 /* 130 */ "tagitem ::= PLUS INTEGER",
 /* 131 */ "tagitem ::= PLUS FLOAT",
 /* 132 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 133 */ "union ::= select",
 /* 134 */ "union ::= LP union RP",
 /* 135 */ "union ::= union UNION ALL select",
 /* 136 */ "union ::= union UNION ALL LP select RP",
 /* 137 */ "cmd ::= union",
 /* 138 */ "select ::= SELECT selcollist",
 /* 139 */ "sclp ::= selcollist COMMA",
 /* 140 */ "sclp ::=",
 /* 141 */ "selcollist ::= sclp expr as",
 /* 142 */ "selcollist ::= sclp STAR",
 /* 143 */ "as ::= AS ids",
 /* 144 */ "as ::= ids",
 /* 145 */ "as ::=",
 /* 146 */ "from ::= FROM tablelist",
 /* 147 */ "tablelist ::= ids cpxName",
 /* 148 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 149 */ "tmvar ::= VARIABLE",
 /* 150 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 151 */ "interval_opt ::=",
 /* 152 */ "fill_opt ::=",
 /* 153 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 154 */ "fill_opt ::= FILL LP ID RP",
 /* 155 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 156 */ "sliding_opt ::=",
 /* 157 */ "orderby_opt ::=",
 /* 158 */ "orderby_opt ::= ORDER BY sortlist",
 /* 159 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 160 */ "sortlist ::= item sortorder",
 /* 161 */ "item ::= ids cpxName",
 /* 162 */ "sortorder ::= ASC",
 /* 163 */ "sortorder ::= DESC",
 /* 164 */ "sortorder ::=",
 /* 165 */ "groupby_opt ::=",
 /* 166 */ "groupby_opt ::= GROUP BY grouplist",
 /* 167 */ "grouplist ::= grouplist COMMA item",
 /* 168 */ "grouplist ::= item",
 /* 169 */ "having_opt ::=",
 /* 170 */ "having_opt ::= HAVING expr",
 /* 171 */ "limit_opt ::=",
 /* 172 */ "limit_opt ::= LIMIT signed",
 /* 173 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 174 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 175 */ "slimit_opt ::=",
 /* 176 */ "slimit_opt ::= SLIMIT signed",
 /* 177 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 178 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 179 */ "where_opt ::=",
 /* 180 */ "where_opt ::= WHERE expr",
 /* 181 */ "expr ::= LP expr RP",
 /* 182 */ "expr ::= ID",
 /* 183 */ "expr ::= ID DOT ID",
 /* 184 */ "expr ::= ID DOT STAR",
 /* 185 */ "expr ::= INTEGER",
 /* 186 */ "expr ::= MINUS INTEGER",
 /* 187 */ "expr ::= PLUS INTEGER",
 /* 188 */ "expr ::= FLOAT",
 /* 189 */ "expr ::= MINUS FLOAT",
 /* 190 */ "expr ::= PLUS FLOAT",
 /* 191 */ "expr ::= STRING",
 /* 192 */ "expr ::= NOW",
 /* 193 */ "expr ::= VARIABLE",
 /* 194 */ "expr ::= BOOL",
 /* 195 */ "expr ::= ID LP exprlist RP",
 /* 196 */ "expr ::= ID LP STAR RP",
 /* 197 */ "expr ::= expr AND expr",
 /* 198 */ "expr ::= expr OR expr",
 /* 199 */ "expr ::= expr LT expr",
 /* 200 */ "expr ::= expr GT expr",
 /* 201 */ "expr ::= expr LE expr",
 /* 202 */ "expr ::= expr GE expr",
 /* 203 */ "expr ::= expr NE expr",
 /* 204 */ "expr ::= expr EQ expr",
 /* 205 */ "expr ::= expr PLUS expr",
 /* 206 */ "expr ::= expr MINUS expr",
 /* 207 */ "expr ::= expr STAR expr",
 /* 208 */ "expr ::= expr SLASH expr",
 /* 209 */ "expr ::= expr REM expr",
 /* 210 */ "expr ::= expr LIKE expr",
 /* 211 */ "expr ::= expr IN LP exprlist RP",
 /* 212 */ "exprlist ::= exprlist COMMA expritem",
 /* 213 */ "exprlist ::= expritem",
 /* 214 */ "expritem ::= expr",
 /* 215 */ "expritem ::=",
 /* 216 */ "cmd ::= RESET QUERY CACHE",
 /* 217 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 218 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 219 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 220 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 221 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 222 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 223 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 224 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 225 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 227: /* keep */
    case 228: /* tagitemlist */
    case 253: /* fill_opt */
    case 255: /* groupby_opt */
    case 256: /* orderby_opt */
    case 266: /* sortlist */
    case 270: /* grouplist */
{
tVariantListDestroy((yypminor->yy498));
}
      break;
    case 245: /* columnlist */
{
tFieldListDestroy((yypminor->yy523));
}
      break;
    case 246: /* select */
{
doDestroyQuerySql((yypminor->yy414));
}
      break;
    case 249: /* selcollist */
    case 261: /* sclp */
    case 271: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy290));
}
      break;
    case 251: /* where_opt */
    case 257: /* having_opt */
    case 262: /* expr */
    case 272: /* expritem */
{
tSQLExprDestroy((yypminor->yy64));
}
      break;
    case 260: /* union */
{
destroyAllSelectClause((yypminor->yy231));
}
      break;
    case 267: /* sortitem */
{
tVariantDestroy(&(yypminor->yy134));
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
  {  208,   -1 }, /* (0) program ::= cmd */
  {  209,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  209,   -2 }, /* (2) cmd ::= SHOW MNODES */
  {  209,   -2 }, /* (3) cmd ::= SHOW DNODES */
  {  209,   -2 }, /* (4) cmd ::= SHOW ACCOUNTS */
  {  209,   -2 }, /* (5) cmd ::= SHOW USERS */
  {  209,   -2 }, /* (6) cmd ::= SHOW MODULES */
  {  209,   -2 }, /* (7) cmd ::= SHOW QUERIES */
  {  209,   -2 }, /* (8) cmd ::= SHOW CONNECTIONS */
  {  209,   -2 }, /* (9) cmd ::= SHOW STREAMS */
  {  209,   -2 }, /* (10) cmd ::= SHOW CONFIGS */
  {  209,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  209,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  209,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  209,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  210,    0 }, /* (15) dbPrefix ::= */
  {  210,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  212,    0 }, /* (17) cpxName ::= */
  {  212,   -2 }, /* (18) cpxName ::= DOT ids */
  {  209,   -3 }, /* (19) cmd ::= SHOW dbPrefix TABLES */
  {  209,   -5 }, /* (20) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  209,   -3 }, /* (21) cmd ::= SHOW dbPrefix STABLES */
  {  209,   -5 }, /* (22) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  209,   -3 }, /* (23) cmd ::= SHOW dbPrefix VGROUPS */
  {  209,   -4 }, /* (24) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  209,   -5 }, /* (25) cmd ::= DROP TABLE ifexists ids cpxName */
  {  209,   -4 }, /* (26) cmd ::= DROP DATABASE ifexists ids */
  {  209,   -3 }, /* (27) cmd ::= DROP DNODE ids */
  {  209,   -3 }, /* (28) cmd ::= DROP USER ids */
  {  209,   -3 }, /* (29) cmd ::= DROP ACCOUNT ids */
  {  209,   -2 }, /* (30) cmd ::= USE ids */
  {  209,   -3 }, /* (31) cmd ::= DESCRIBE ids cpxName */
  {  209,   -5 }, /* (32) cmd ::= ALTER USER ids PASS ids */
  {  209,   -5 }, /* (33) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  209,   -4 }, /* (34) cmd ::= ALTER DNODE ids ids */
  {  209,   -5 }, /* (35) cmd ::= ALTER DNODE ids ids ids */
  {  209,   -3 }, /* (36) cmd ::= ALTER LOCAL ids */
  {  209,   -4 }, /* (37) cmd ::= ALTER LOCAL ids ids */
  {  209,   -4 }, /* (38) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  209,   -4 }, /* (39) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  209,   -6 }, /* (40) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  211,   -1 }, /* (41) ids ::= ID */
  {  211,   -1 }, /* (42) ids ::= STRING */
  {  213,   -2 }, /* (43) ifexists ::= IF EXISTS */
  {  213,    0 }, /* (44) ifexists ::= */
  {  216,   -3 }, /* (45) ifnotexists ::= IF NOT EXISTS */
  {  216,    0 }, /* (46) ifnotexists ::= */
  {  209,   -3 }, /* (47) cmd ::= CREATE DNODE ids */
  {  209,   -6 }, /* (48) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  209,   -5 }, /* (49) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  209,   -5 }, /* (50) cmd ::= CREATE USER ids PASS ids */
  {  218,    0 }, /* (51) pps ::= */
  {  218,   -2 }, /* (52) pps ::= PPS INTEGER */
  {  219,    0 }, /* (53) tseries ::= */
  {  219,   -2 }, /* (54) tseries ::= TSERIES INTEGER */
  {  220,    0 }, /* (55) dbs ::= */
  {  220,   -2 }, /* (56) dbs ::= DBS INTEGER */
  {  221,    0 }, /* (57) streams ::= */
  {  221,   -2 }, /* (58) streams ::= STREAMS INTEGER */
  {  222,    0 }, /* (59) storage ::= */
  {  222,   -2 }, /* (60) storage ::= STORAGE INTEGER */
  {  223,    0 }, /* (61) qtime ::= */
  {  223,   -2 }, /* (62) qtime ::= QTIME INTEGER */
  {  224,    0 }, /* (63) users ::= */
  {  224,   -2 }, /* (64) users ::= USERS INTEGER */
  {  225,    0 }, /* (65) conns ::= */
  {  225,   -2 }, /* (66) conns ::= CONNS INTEGER */
  {  226,    0 }, /* (67) state ::= */
  {  226,   -2 }, /* (68) state ::= STATE ids */
  {  215,   -9 }, /* (69) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  227,   -2 }, /* (70) keep ::= KEEP tagitemlist */
  {  229,   -2 }, /* (71) tables ::= MAXTABLES INTEGER */
  {  230,   -2 }, /* (72) cache ::= CACHE INTEGER */
  {  231,   -2 }, /* (73) replica ::= REPLICA INTEGER */
  {  232,   -2 }, /* (74) quorum ::= QUORUM INTEGER */
  {  233,   -2 }, /* (75) days ::= DAYS INTEGER */
  {  234,   -2 }, /* (76) minrows ::= MINROWS INTEGER */
  {  235,   -2 }, /* (77) maxrows ::= MAXROWS INTEGER */
  {  236,   -2 }, /* (78) blocks ::= BLOCKS INTEGER */
  {  237,   -2 }, /* (79) ctime ::= CTIME INTEGER */
  {  238,   -2 }, /* (80) wal ::= WAL INTEGER */
  {  239,   -2 }, /* (81) fsync ::= FSYNC INTEGER */
  {  240,   -2 }, /* (82) comp ::= COMP INTEGER */
  {  241,   -2 }, /* (83) prec ::= PRECISION STRING */
  {  217,    0 }, /* (84) db_optr ::= */
  {  217,   -2 }, /* (85) db_optr ::= db_optr tables */
  {  217,   -2 }, /* (86) db_optr ::= db_optr cache */
  {  217,   -2 }, /* (87) db_optr ::= db_optr replica */
  {  217,   -2 }, /* (88) db_optr ::= db_optr quorum */
  {  217,   -2 }, /* (89) db_optr ::= db_optr days */
  {  217,   -2 }, /* (90) db_optr ::= db_optr minrows */
  {  217,   -2 }, /* (91) db_optr ::= db_optr maxrows */
  {  217,   -2 }, /* (92) db_optr ::= db_optr blocks */
  {  217,   -2 }, /* (93) db_optr ::= db_optr ctime */
  {  217,   -2 }, /* (94) db_optr ::= db_optr wal */
  {  217,   -2 }, /* (95) db_optr ::= db_optr fsync */
  {  217,   -2 }, /* (96) db_optr ::= db_optr comp */
  {  217,   -2 }, /* (97) db_optr ::= db_optr prec */
  {  217,   -2 }, /* (98) db_optr ::= db_optr keep */
  {  214,    0 }, /* (99) alter_db_optr ::= */
  {  214,   -2 }, /* (100) alter_db_optr ::= alter_db_optr replica */
  {  214,   -2 }, /* (101) alter_db_optr ::= alter_db_optr quorum */
  {  214,   -2 }, /* (102) alter_db_optr ::= alter_db_optr tables */
  {  214,   -2 }, /* (103) alter_db_optr ::= alter_db_optr keep */
  {  214,   -2 }, /* (104) alter_db_optr ::= alter_db_optr blocks */
  {  214,   -2 }, /* (105) alter_db_optr ::= alter_db_optr comp */
  {  214,   -2 }, /* (106) alter_db_optr ::= alter_db_optr wal */
  {  214,   -2 }, /* (107) alter_db_optr ::= alter_db_optr fsync */
  {  242,   -1 }, /* (108) typename ::= ids */
  {  242,   -4 }, /* (109) typename ::= ids LP signed RP */
  {  243,   -1 }, /* (110) signed ::= INTEGER */
  {  243,   -2 }, /* (111) signed ::= PLUS INTEGER */
  {  243,   -2 }, /* (112) signed ::= MINUS INTEGER */
  {  209,   -6 }, /* (113) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  244,   -3 }, /* (114) create_table_args ::= LP columnlist RP */
  {  244,   -7 }, /* (115) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  244,   -7 }, /* (116) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  244,   -2 }, /* (117) create_table_args ::= AS select */
  {  245,   -3 }, /* (118) columnlist ::= columnlist COMMA column */
  {  245,   -1 }, /* (119) columnlist ::= column */
  {  247,   -2 }, /* (120) column ::= ids typename */
  {  228,   -3 }, /* (121) tagitemlist ::= tagitemlist COMMA tagitem */
  {  228,   -1 }, /* (122) tagitemlist ::= tagitem */
  {  248,   -1 }, /* (123) tagitem ::= INTEGER */
  {  248,   -1 }, /* (124) tagitem ::= FLOAT */
  {  248,   -1 }, /* (125) tagitem ::= STRING */
  {  248,   -1 }, /* (126) tagitem ::= BOOL */
  {  248,   -1 }, /* (127) tagitem ::= NULL */
  {  248,   -2 }, /* (128) tagitem ::= MINUS INTEGER */
  {  248,   -2 }, /* (129) tagitem ::= MINUS FLOAT */
  {  248,   -2 }, /* (130) tagitem ::= PLUS INTEGER */
  {  248,   -2 }, /* (131) tagitem ::= PLUS FLOAT */
  {  246,  -12 }, /* (132) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  260,   -1 }, /* (133) union ::= select */
  {  260,   -3 }, /* (134) union ::= LP union RP */
  {  260,   -4 }, /* (135) union ::= union UNION ALL select */
  {  260,   -6 }, /* (136) union ::= union UNION ALL LP select RP */
  {  209,   -1 }, /* (137) cmd ::= union */
  {  246,   -2 }, /* (138) select ::= SELECT selcollist */
  {  261,   -2 }, /* (139) sclp ::= selcollist COMMA */
  {  261,    0 }, /* (140) sclp ::= */
  {  249,   -3 }, /* (141) selcollist ::= sclp expr as */
  {  249,   -2 }, /* (142) selcollist ::= sclp STAR */
  {  263,   -2 }, /* (143) as ::= AS ids */
  {  263,   -1 }, /* (144) as ::= ids */
  {  263,    0 }, /* (145) as ::= */
  {  250,   -2 }, /* (146) from ::= FROM tablelist */
  {  264,   -2 }, /* (147) tablelist ::= ids cpxName */
  {  264,   -4 }, /* (148) tablelist ::= tablelist COMMA ids cpxName */
  {  265,   -1 }, /* (149) tmvar ::= VARIABLE */
  {  252,   -4 }, /* (150) interval_opt ::= INTERVAL LP tmvar RP */
  {  252,    0 }, /* (151) interval_opt ::= */
  {  253,    0 }, /* (152) fill_opt ::= */
  {  253,   -6 }, /* (153) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  253,   -4 }, /* (154) fill_opt ::= FILL LP ID RP */
  {  254,   -4 }, /* (155) sliding_opt ::= SLIDING LP tmvar RP */
  {  254,    0 }, /* (156) sliding_opt ::= */
  {  256,    0 }, /* (157) orderby_opt ::= */
  {  256,   -3 }, /* (158) orderby_opt ::= ORDER BY sortlist */
  {  266,   -4 }, /* (159) sortlist ::= sortlist COMMA item sortorder */
  {  266,   -2 }, /* (160) sortlist ::= item sortorder */
  {  268,   -2 }, /* (161) item ::= ids cpxName */
  {  269,   -1 }, /* (162) sortorder ::= ASC */
  {  269,   -1 }, /* (163) sortorder ::= DESC */
  {  269,    0 }, /* (164) sortorder ::= */
  {  255,    0 }, /* (165) groupby_opt ::= */
  {  255,   -3 }, /* (166) groupby_opt ::= GROUP BY grouplist */
  {  270,   -3 }, /* (167) grouplist ::= grouplist COMMA item */
  {  270,   -1 }, /* (168) grouplist ::= item */
  {  257,    0 }, /* (169) having_opt ::= */
  {  257,   -2 }, /* (170) having_opt ::= HAVING expr */
  {  259,    0 }, /* (171) limit_opt ::= */
  {  259,   -2 }, /* (172) limit_opt ::= LIMIT signed */
  {  259,   -4 }, /* (173) limit_opt ::= LIMIT signed OFFSET signed */
  {  259,   -4 }, /* (174) limit_opt ::= LIMIT signed COMMA signed */
  {  258,    0 }, /* (175) slimit_opt ::= */
  {  258,   -2 }, /* (176) slimit_opt ::= SLIMIT signed */
  {  258,   -4 }, /* (177) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  258,   -4 }, /* (178) slimit_opt ::= SLIMIT signed COMMA signed */
  {  251,    0 }, /* (179) where_opt ::= */
  {  251,   -2 }, /* (180) where_opt ::= WHERE expr */
  {  262,   -3 }, /* (181) expr ::= LP expr RP */
  {  262,   -1 }, /* (182) expr ::= ID */
  {  262,   -3 }, /* (183) expr ::= ID DOT ID */
  {  262,   -3 }, /* (184) expr ::= ID DOT STAR */
  {  262,   -1 }, /* (185) expr ::= INTEGER */
  {  262,   -2 }, /* (186) expr ::= MINUS INTEGER */
  {  262,   -2 }, /* (187) expr ::= PLUS INTEGER */
  {  262,   -1 }, /* (188) expr ::= FLOAT */
  {  262,   -2 }, /* (189) expr ::= MINUS FLOAT */
  {  262,   -2 }, /* (190) expr ::= PLUS FLOAT */
  {  262,   -1 }, /* (191) expr ::= STRING */
  {  262,   -1 }, /* (192) expr ::= NOW */
  {  262,   -1 }, /* (193) expr ::= VARIABLE */
  {  262,   -1 }, /* (194) expr ::= BOOL */
  {  262,   -4 }, /* (195) expr ::= ID LP exprlist RP */
  {  262,   -4 }, /* (196) expr ::= ID LP STAR RP */
  {  262,   -3 }, /* (197) expr ::= expr AND expr */
  {  262,   -3 }, /* (198) expr ::= expr OR expr */
  {  262,   -3 }, /* (199) expr ::= expr LT expr */
  {  262,   -3 }, /* (200) expr ::= expr GT expr */
  {  262,   -3 }, /* (201) expr ::= expr LE expr */
  {  262,   -3 }, /* (202) expr ::= expr GE expr */
  {  262,   -3 }, /* (203) expr ::= expr NE expr */
  {  262,   -3 }, /* (204) expr ::= expr EQ expr */
  {  262,   -3 }, /* (205) expr ::= expr PLUS expr */
  {  262,   -3 }, /* (206) expr ::= expr MINUS expr */
  {  262,   -3 }, /* (207) expr ::= expr STAR expr */
  {  262,   -3 }, /* (208) expr ::= expr SLASH expr */
  {  262,   -3 }, /* (209) expr ::= expr REM expr */
  {  262,   -3 }, /* (210) expr ::= expr LIKE expr */
  {  262,   -5 }, /* (211) expr ::= expr IN LP exprlist RP */
  {  271,   -3 }, /* (212) exprlist ::= exprlist COMMA expritem */
  {  271,   -1 }, /* (213) exprlist ::= expritem */
  {  272,   -1 }, /* (214) expritem ::= expr */
  {  272,    0 }, /* (215) expritem ::= */
  {  209,   -3 }, /* (216) cmd ::= RESET QUERY CACHE */
  {  209,   -7 }, /* (217) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  209,   -7 }, /* (218) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  209,   -7 }, /* (219) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  209,   -7 }, /* (220) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  209,   -8 }, /* (221) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  209,   -9 }, /* (222) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  209,   -3 }, /* (223) cmd ::= KILL CONNECTION INTEGER */
  {  209,   -5 }, /* (224) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  209,   -5 }, /* (225) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &t);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy149);}
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
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
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy149.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy149.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy149.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy149.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy149.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy149.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy149.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy149.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy149.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy149 = yylhsminor.yy149;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy498 = yymsp[0].minor.yy498; }
        break;
      case 71: /* tables ::= MAXTABLES INTEGER */
      case 72: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==72);
      case 73: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==73);
      case 74: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==74);
      case 75: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==75);
      case 76: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==79);
      case 80: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==80);
      case 81: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==81);
      case 82: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==82);
      case 83: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==83);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 84: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy268);}
        break;
      case 85: /* db_optr ::= db_optr tables */
      case 102: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==102);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.maxTablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 86: /* db_optr ::= db_optr cache */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 87: /* db_optr ::= db_optr replica */
      case 100: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==100);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 88: /* db_optr ::= db_optr quorum */
      case 101: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==101);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 89: /* db_optr ::= db_optr days */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 90: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 91: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 92: /* db_optr ::= db_optr blocks */
      case 104: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==104);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 93: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 94: /* db_optr ::= db_optr wal */
      case 106: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==106);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 95: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 96: /* db_optr ::= db_optr comp */
      case 105: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==105);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 97: /* db_optr ::= db_optr prec */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 98: /* db_optr ::= db_optr keep */
      case 103: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==103);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.keep = yymsp[0].minor.yy498; }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 99: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy268);}
        break;
      case 107: /* alter_db_optr ::= alter_db_optr fsync */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 108: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSQLSetColumnType (&yylhsminor.yy223, &yymsp[0].minor.yy0); 
}
  yymsp[0].minor.yy223 = yylhsminor.yy223;
        break;
      case 109: /* typename ::= ids LP signed RP */
{
    if (yymsp[-1].minor.yy207 <= 0) {
      yymsp[-3].minor.yy0.type = 0;
      tSQLSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
    } else {
      yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy207;          // negative value of name length
      tSQLSetColumnType(&yylhsminor.yy223, &yymsp[-3].minor.yy0);
    }
}
  yymsp[-3].minor.yy223 = yylhsminor.yy223;
        break;
      case 110: /* signed ::= INTEGER */
{ yylhsminor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 111: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 112: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy207 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 113: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 114: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy470 = tSetCreateSQLElems(yymsp[-1].minor.yy523, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 115: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy470 = tSetCreateSQLElems(yymsp[-5].minor.yy523, yymsp[-1].minor.yy523, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 116: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy470 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy498, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 117: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy470 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy414, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 118: /* columnlist ::= columnlist COMMA column */
{yylhsminor.yy523 = tFieldListAppend(yymsp[-2].minor.yy523, &yymsp[0].minor.yy223);   }
  yymsp[-2].minor.yy523 = yylhsminor.yy523;
        break;
      case 119: /* columnlist ::= column */
{yylhsminor.yy523 = tFieldListAppend(NULL, &yymsp[0].minor.yy223);}
  yymsp[0].minor.yy523 = yylhsminor.yy523;
        break;
      case 120: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 121: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy498 = tVariantListAppend(yymsp[-2].minor.yy498, &yymsp[0].minor.yy134, -1);    }
  yymsp[-2].minor.yy498 = yylhsminor.yy498;
        break;
      case 122: /* tagitemlist ::= tagitem */
{ yylhsminor.yy498 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1); }
  yymsp[0].minor.yy498 = yylhsminor.yy498;
        break;
      case 123: /* tagitem ::= INTEGER */
      case 124: /* tagitem ::= FLOAT */ yytestcase(yyruleno==124);
      case 125: /* tagitem ::= STRING */ yytestcase(yyruleno==125);
      case 126: /* tagitem ::= BOOL */ yytestcase(yyruleno==126);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 127: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 128: /* tagitem ::= MINUS INTEGER */
      case 129: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==129);
      case 130: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==130);
      case 131: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==131);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy134, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 132: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy414 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy290, yymsp[-9].minor.yy498, yymsp[-8].minor.yy64, yymsp[-4].minor.yy498, yymsp[-3].minor.yy498, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy498, &yymsp[0].minor.yy216, &yymsp[-1].minor.yy216);
}
  yymsp[-11].minor.yy414 = yylhsminor.yy414;
        break;
      case 133: /* union ::= select */
{ yylhsminor.yy231 = setSubclause(NULL, yymsp[0].minor.yy414); }
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 134: /* union ::= LP union RP */
{ yymsp[-2].minor.yy231 = yymsp[-1].minor.yy231; }
        break;
      case 135: /* union ::= union UNION ALL select */
{ yylhsminor.yy231 = appendSelectClause(yymsp[-3].minor.yy231, yymsp[0].minor.yy414); }
  yymsp[-3].minor.yy231 = yylhsminor.yy231;
        break;
      case 136: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy231 = appendSelectClause(yymsp[-5].minor.yy231, yymsp[-1].minor.yy414); }
  yymsp[-5].minor.yy231 = yylhsminor.yy231;
        break;
      case 137: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy231, NULL, TSDB_SQL_SELECT); }
        break;
      case 138: /* select ::= SELECT selcollist */
{
  yylhsminor.yy414 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy290, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy414 = yylhsminor.yy414;
        break;
      case 139: /* sclp ::= selcollist COMMA */
{yylhsminor.yy290 = yymsp[-1].minor.yy290;}
  yymsp[-1].minor.yy290 = yylhsminor.yy290;
        break;
      case 140: /* sclp ::= */
{yymsp[1].minor.yy290 = 0;}
        break;
      case 141: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290, yymsp[-1].minor.yy64, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy290 = yylhsminor.yy290;
        break;
      case 142: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy290 = tSQLExprListAppend(yymsp[-1].minor.yy290, pNode, 0);
}
  yymsp[-1].minor.yy290 = yylhsminor.yy290;
        break;
      case 143: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 144: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 145: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 146: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy498 = yymsp[0].minor.yy498;}
        break;
      case 147: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy498 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
  yymsp[-1].minor.yy498 = yylhsminor.yy498;
        break;
      case 148: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy498 = tVariantListAppendToken(yymsp[-3].minor.yy498, &yymsp[-1].minor.yy0, -1);   }
  yymsp[-3].minor.yy498 = yylhsminor.yy498;
        break;
      case 149: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 150: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 155: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==155);
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 151: /* interval_opt ::= */
      case 156: /* sliding_opt ::= */ yytestcase(yyruleno==156);
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 152: /* fill_opt ::= */
{yymsp[1].minor.yy498 = 0;     }
        break;
      case 153: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy498, &A, -1, 0);
    yymsp[-5].minor.yy498 = yymsp[-1].minor.yy498;
}
        break;
      case 154: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy498 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 157: /* orderby_opt ::= */
      case 165: /* groupby_opt ::= */ yytestcase(yyruleno==165);
{yymsp[1].minor.yy498 = 0;}
        break;
      case 158: /* orderby_opt ::= ORDER BY sortlist */
      case 166: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==166);
{yymsp[-2].minor.yy498 = yymsp[0].minor.yy498;}
        break;
      case 159: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy498 = tVariantListAppend(yymsp[-3].minor.yy498, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
  yymsp[-3].minor.yy498 = yylhsminor.yy498;
        break;
      case 160: /* sortlist ::= item sortorder */
{
  yylhsminor.yy498 = tVariantListAppend(NULL, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
  yymsp[-1].minor.yy498 = yylhsminor.yy498;
        break;
      case 161: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy134, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 162: /* sortorder ::= ASC */
{yymsp[0].minor.yy46 = TSDB_ORDER_ASC; }
        break;
      case 163: /* sortorder ::= DESC */
{yymsp[0].minor.yy46 = TSDB_ORDER_DESC;}
        break;
      case 164: /* sortorder ::= */
{yymsp[1].minor.yy46 = TSDB_ORDER_ASC;}
        break;
      case 167: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy498 = tVariantListAppend(yymsp[-2].minor.yy498, &yymsp[0].minor.yy134, -1);
}
  yymsp[-2].minor.yy498 = yylhsminor.yy498;
        break;
      case 168: /* grouplist ::= item */
{
  yylhsminor.yy498 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1);
}
  yymsp[0].minor.yy498 = yylhsminor.yy498;
        break;
      case 169: /* having_opt ::= */
      case 179: /* where_opt ::= */ yytestcase(yyruleno==179);
      case 215: /* expritem ::= */ yytestcase(yyruleno==215);
{yymsp[1].minor.yy64 = 0;}
        break;
      case 170: /* having_opt ::= HAVING expr */
      case 180: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==180);
{yymsp[-1].minor.yy64 = yymsp[0].minor.yy64;}
        break;
      case 171: /* limit_opt ::= */
      case 175: /* slimit_opt ::= */ yytestcase(yyruleno==175);
{yymsp[1].minor.yy216.limit = -1; yymsp[1].minor.yy216.offset = 0;}
        break;
      case 172: /* limit_opt ::= LIMIT signed */
      case 176: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==176);
{yymsp[-1].minor.yy216.limit = yymsp[0].minor.yy207;  yymsp[-1].minor.yy216.offset = 0;}
        break;
      case 173: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 177: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==177);
{yymsp[-3].minor.yy216.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy216.offset = yymsp[0].minor.yy207;}
        break;
      case 174: /* limit_opt ::= LIMIT signed COMMA signed */
      case 178: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==178);
{yymsp[-3].minor.yy216.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy216.offset = yymsp[-2].minor.yy207;}
        break;
      case 181: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy64 = yymsp[-1].minor.yy64; }
        break;
      case 182: /* expr ::= ID */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 183: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 184: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 185: /* expr ::= INTEGER */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 186: /* expr ::= MINUS INTEGER */
      case 187: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==187);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy64 = yylhsminor.yy64;
        break;
      case 188: /* expr ::= FLOAT */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 189: /* expr ::= MINUS FLOAT */
      case 190: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==190);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy64 = yylhsminor.yy64;
        break;
      case 191: /* expr ::= STRING */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 192: /* expr ::= NOW */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 193: /* expr ::= VARIABLE */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 194: /* expr ::= BOOL */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 195: /* expr ::= ID LP exprlist RP */
{
  yylhsminor.yy64 = tSQLExprCreateFunction(yymsp[-1].minor.yy290, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy64 = yylhsminor.yy64;
        break;
      case 196: /* expr ::= ID LP STAR RP */
{
  yylhsminor.yy64 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
  yymsp[-3].minor.yy64 = yylhsminor.yy64;
        break;
      case 197: /* expr ::= expr AND expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_AND);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 198: /* expr ::= expr OR expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_OR); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 199: /* expr ::= expr LT expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LT);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 200: /* expr ::= expr GT expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GT);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 201: /* expr ::= expr LE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 202: /* expr ::= expr GE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 203: /* expr ::= expr NE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_NE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 204: /* expr ::= expr EQ expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_EQ);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 205: /* expr ::= expr PLUS expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_PLUS);  }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 206: /* expr ::= expr MINUS expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_MINUS); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 207: /* expr ::= expr STAR expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_STAR);  }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 208: /* expr ::= expr SLASH expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_DIVIDE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 209: /* expr ::= expr REM expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_REM);   }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 210: /* expr ::= expr LIKE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LIKE);  }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 211: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-4].minor.yy64, (tSQLExpr*)yymsp[-1].minor.yy290, TK_IN); }
  yymsp[-4].minor.yy64 = yylhsminor.yy64;
        break;
      case 212: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290,yymsp[0].minor.yy64,0);}
  yymsp[-2].minor.yy290 = yylhsminor.yy290;
        break;
      case 213: /* exprlist ::= expritem */
{yylhsminor.yy290 = tSQLExprListAppend(0,yymsp[0].minor.yy64,0);}
  yymsp[0].minor.yy290 = yylhsminor.yy290;
        break;
      case 214: /* expritem ::= expr */
{yylhsminor.yy64 = yymsp[0].minor.yy64;}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 216: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 217: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy523, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 218: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 219: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy523, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 220: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 221: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 222: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 223: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 224: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 225: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
