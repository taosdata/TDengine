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
#define YYNOCODE 276
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy42;
  SQuerySQL* yy84;
  SCreatedTableInfo yy96;
  SArray* yy131;
  SCreateDBInfo yy148;
  TAOS_FIELD yy163;
  SLimitVal yy284;
  SCreateAcctSQL yy309;
  tSQLExpr* yy420;
  int64_t yy459;
  tSQLExprList* yy478;
  SSubclauseInfo* yy513;
  tVariant yy516;
  SIntervalVal yy530;
  SCreateTableSQL* yy538;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             257
#define YYNRULE              236
#define YYNTOKEN             207
#define YY_MAX_SHIFT         256
#define YY_MIN_SHIFTREDUCE   426
#define YY_MAX_SHIFTREDUCE   661
#define YY_ERROR_ACTION      662
#define YY_ACCEPT_ACTION     663
#define YY_NO_ACTION         664
#define YY_MIN_REDUCE        665
#define YY_MAX_REDUCE        900
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
#define YY_ACTTAB_COUNT (579)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   143,  469,  663,  256,  469,  162,  254,   12,  814,  470,
 /*    10 */   887,  142,  470,   37,   38,  147,   39,   40,  803,  233,
 /*    20 */   173,   31,  884,  469,  209,   43,   41,   45,   42,   64,
 /*    30 */   883,  470,  163,   36,   35,  105,  143,   34,   33,   32,
 /*    40 */    37,   38,  803,   39,   40,  168,  888,  173,   31,  110,
 /*    50 */   790,  209,   43,   41,   45,   42,  194,  780,   22,  782,
 /*    60 */    36,   35,  811,  882,   34,   33,   32,  427,  428,  429,
 /*    70 */   430,  431,  432,  433,  434,  435,  436,  437,  438,  255,
 /*    80 */    37,   38,  184,   39,   40,  538,  224,  173,   31,  143,
 /*    90 */   197,  209,   43,   41,   45,   42,  165,   23,  167,  888,
 /*   100 */    36,   35,  242,   57,   34,   33,   32,  179,  841,   38,
 /*   110 */   204,   39,   40,  231,  230,  173,   31,   49,  792,  209,
 /*   120 */    43,   41,   45,   42,  253,  252,   98,  615,   36,   35,
 /*   130 */   178,  781,   34,   33,   32,  788,   50,   17,  222,  249,
 /*   140 */   248,  221,  220,  219,  247,  218,  246,  245,  244,  217,
 /*   150 */   243,  764,  792,  752,  753,  754,  755,  756,  757,  758,
 /*   160 */   759,  760,  761,  762,  763,  765,   39,   40,  110,  180,
 /*   170 */   173,   31,  228,  227,  209,   43,   41,   45,   42,   34,
 /*   180 */    33,   32,    9,   36,   35,   65,  120,   34,   33,   32,
 /*   190 */   172,  628,   18,   13,  619,  110,  622,  210,  625,   28,
 /*   200 */   172,  628,  110,  159,  619,  187,  622,  224,  625,  155,
 /*   210 */   172,  628,  191,  190,  619,  156,  622,   66,  625,   92,
 /*   220 */    91,  150,  169,  170,   36,   35,  208,  840,   34,   33,
 /*   230 */    32,  160,  169,  170,  617,  250,  573,   43,   41,   45,
 /*   240 */    42,  706,  169,  170,  133,   36,   35,  783,   18,   34,
 /*   250 */    33,   32,  206,  104,   61,   28,   17,  792,  249,  248,
 /*   260 */    28,   62,  145,  247,   23,  246,  245,  244,  146,  243,
 /*   270 */   618,  715,   78,   82,  133,  193,  565,   23,   87,   90,
 /*   280 */    81,  769,  158,  196,  767,  768,   84,  631,   44,  770,
 /*   290 */    23,  772,  773,  771,  148,  774,   80,  149,   44,  627,
 /*   300 */   176,  242,  789,  707,    3,  124,  133,   23,   44,  627,
 /*   310 */    72,   68,   71,  177,  626,  789,  596,  597,  570,  627,
 /*   320 */   805,   63,  557,   19,  626,  554,  229,  555,  789,  556,
 /*   330 */   171,  137,  135,   29,  626,  153,  583,   95,   94,   93,
 /*   340 */   107,  154,  587,  234,  588,  789,   48,  647,   15,   52,
 /*   350 */   152,   14,  629,  181,  182,  141,   14,  621,  620,  624,
 /*   360 */   623,  546,   89,   88,  212,   24,   53,  547,   24,  151,
 /*   370 */     4,   48,  561,  144,  562,   77,   76,   11,   10,  897,
 /*   380 */   559,  851,  560,  103,  101,  791,  850,  174,  847,  846,
 /*   390 */   175,  232,  813,  833,  818,  820,  106,  832,  121,  122,
 /*   400 */    28,  123,  102,  119,  717,  216,  139,   26,  225,  714,
 /*   410 */   195,  582,  226,  896,   74,  895,  893,  125,  735,   27,
 /*   420 */   198,   25,  164,  202,  140,  558,  704,   83,  702,   85,
 /*   430 */    86,  700,  699,  183,   54,  134,  697,  696,  695,  694,
 /*   440 */   693,  136,  691,  689,   46,  687,  685,  802,  683,  138,
 /*   450 */    51,   58,   59,  834,  207,  205,  199,  203,  201,   30,
 /*   460 */    79,  235,  236,  237,  238,  239,  240,  241,  251,  161,
 /*   470 */   661,  214,  215,  186,  185,  660,  189,  157,  188,   69,
 /*   480 */   659,  652,  192,   60,  196,  166,  567,  698,   56,  128,
 /*   490 */    96,  692,  736,  126,  130,   97,  127,  129,  131,  132,
 /*   500 */   684,    1,  584,  787,    2,  108,  200,  117,  113,  111,
 /*   510 */   112,  114,  115,  116,  589,  118,  109,    5,    6,   20,
 /*   520 */    21,  630,    8,    7,  632,  211,   16,  213,   67,  510,
 /*   530 */    65,  506,  504,  503,  502,  499,  473,  223,   24,   70,
 /*   540 */    47,   73,  540,  539,  537,   55,  494,  492,  484,  490,
 /*   550 */    75,  486,  488,  482,  480,  511,  509,  508,  507,  505,
 /*   560 */   501,  500,   48,  471,  442,  440,   99,  665,  664,  664,
 /*   570 */   664,  664,  664,  664,  664,  664,  664,  664,  100,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   264,    1,  208,  209,    1,  210,  211,  264,  211,    9,
 /*    10 */   274,  264,    9,   13,   14,  264,   16,   17,  248,  211,
 /*    20 */    20,   21,  264,    1,   24,   25,   26,   27,   28,  216,
 /*    30 */   264,    9,  262,   33,   34,  211,  264,   37,   38,   39,
 /*    40 */    13,   14,  248,   16,   17,  273,  274,   20,   21,  211,
 /*    50 */   242,   24,   25,   26,   27,   28,  262,  244,  245,  246,
 /*    60 */    33,   34,  265,  264,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    13,   14,   60,   16,   17,    5,   76,   20,   21,  264,
 /*    90 */   266,   24,   25,   26,   27,   28,  228,  211,  273,  274,
 /*   100 */    33,   34,   78,  103,   37,   38,   39,   66,  270,   14,
 /*   110 */   272,   16,   17,   33,   34,   20,   21,  104,  250,   24,
 /*   120 */    25,   26,   27,   28,   63,   64,   65,  100,   33,   34,
 /*   130 */   228,    0,   37,   38,   39,  249,  123,   85,   86,   87,
 /*   140 */    88,   89,   90,   91,   92,   93,   94,   95,   96,   97,
 /*   150 */    98,  227,  250,  229,  230,  231,  232,  233,  234,  235,
 /*   160 */   236,  237,  238,  239,  240,  241,   16,   17,  211,  128,
 /*   170 */    20,   21,  131,  132,   24,   25,   26,   27,   28,   37,
 /*   180 */    38,   39,   99,   33,   34,  102,  103,   37,   38,   39,
 /*   190 */     1,    2,   99,   44,    5,  211,    7,   15,    9,  106,
 /*   200 */     1,    2,  211,  264,    5,  127,    7,   76,    9,   60,
 /*   210 */     1,    2,  134,  135,    5,   66,    7,  216,    9,   70,
 /*   220 */    71,   72,   33,   34,   33,   34,   37,  270,   37,   38,
 /*   230 */    39,  264,   33,   34,    1,  228,   37,   25,   26,   27,
 /*   240 */    28,  215,   33,   34,  218,   33,   34,  246,   99,   37,
 /*   250 */    38,   39,  268,   99,  270,  106,   85,  250,   87,   88,
 /*   260 */   106,  270,  264,   92,  211,   94,   95,   96,  264,   98,
 /*   270 */    37,  215,   61,   62,  218,  126,  100,  211,   67,   68,
 /*   280 */    69,  227,  133,  107,  230,  231,   75,  105,   99,  235,
 /*   290 */   211,  237,  238,  239,  264,  241,   73,  264,   99,  110,
 /*   300 */   247,   78,  249,  215,   61,   62,  218,  211,   99,  110,
 /*   310 */    67,   68,   69,  247,  125,  249,  116,  117,  104,  110,
 /*   320 */   248,  251,    2,  109,  125,    5,  247,    7,  249,    9,
 /*   330 */    59,   61,   62,  263,  125,  264,  100,   67,   68,   69,
 /*   340 */   104,  264,  100,  247,  100,  249,  104,  100,  104,  104,
 /*   350 */   264,  104,  100,   33,   34,  264,  104,    5,    5,    7,
 /*   360 */     7,  100,   73,   74,  100,  104,  121,  100,  104,  264,
 /*   370 */    99,  104,    5,  264,    7,  129,  130,  129,  130,  250,
 /*   380 */     5,  243,    7,   61,   62,  250,  243,  243,  243,  243,
 /*   390 */   243,  243,  211,  271,  211,  211,  211,  271,  211,  211,
 /*   400 */   106,  211,   59,  252,  211,  211,  211,  211,  211,  211,
 /*   410 */   248,  110,  211,  211,  211,  211,  211,  211,  211,  211,
 /*   420 */   267,  211,  267,  267,  211,  105,  211,  211,  211,  211,
 /*   430 */   211,  211,  211,  211,  120,  211,  211,  211,  211,  211,
 /*   440 */   211,  211,  211,  211,  119,  211,  211,  261,  211,  211,
 /*   450 */   122,  212,  212,  212,  114,  118,  111,  113,  112,  124,
 /*   460 */    84,   83,   49,   80,   82,   53,   81,   79,   76,  212,
 /*   470 */     5,  212,  212,    5,  136,    5,    5,  212,  136,  216,
 /*   480 */     5,   86,  127,  104,  107,    1,  100,  212,  108,  220,
 /*   490 */   213,  212,  226,  225,  221,  213,  224,  223,  222,  219,
 /*   500 */   212,  217,  100,  248,  214,   99,   99,  254,  258,  260,
 /*   510 */   259,  257,  256,  255,  100,  253,   99,   99,  115,  104,
 /*   520 */   104,  100,   99,  115,  105,  101,   99,  101,   73,    9,
 /*   530 */   102,    5,    5,    5,    5,    5,   77,   15,  104,   73,
 /*   540 */    16,  130,    5,    5,  100,   99,    5,    5,    5,    5,
 /*   550 */   130,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   560 */     5,    5,  104,   77,   59,   58,   21,    0,  275,  275,
 /*   570 */   275,  275,  275,  275,  275,  275,  275,  275,   21,  275,
 /*   580 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   590 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   600 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   610 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   620 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   630 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   640 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   650 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   660 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   670 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   680 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   690 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   700 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   710 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   720 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   730 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   740 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   750 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   760 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   770 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   780 */   275,  275,  275,  275,  275,  275,
};
#define YY_SHIFT_COUNT    (256)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (567)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   149,   52,  171,   10,  189,  209,    3,    3,    3,    3,
 /*    10 */     3,    3,    0,   22,  209,  320,  320,  320,   93,    3,
 /*    20 */     3,    3,  131,    3,    3,  223,   24,   24,  579,  199,
 /*    30 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*    40 */   209,  209,  209,  209,  209,  209,  209,  320,  320,   80,
 /*    50 */    80,   80,   80,   80,   80,   80,  154,    3,    3,    3,
 /*    60 */     3,  200,  200,  214,    3,    3,    3,    3,    3,    3,
 /*    70 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    80 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*    90 */     3,    3,    3,    3,    3,    3,    3,    3,    3,    3,
 /*   100 */     3,    3,    3,    3,  294,  343,  343,  301,  301,  301,
 /*   110 */   343,  314,  328,  325,  340,  337,  344,  346,  345,  335,
 /*   120 */   294,  343,  343,  343,   10,  343,  376,  378,  413,  383,
 /*   130 */   382,  412,  385,  388,  343,  392,  343,  392,  343,  579,
 /*   140 */   579,   27,   67,   67,   67,   95,  150,  212,  212,  212,
 /*   150 */   211,  191,  191,  191,  191,  243,  270,   41,   78,  142,
 /*   160 */   142,   83,   61,  176,  236,  242,  244,  247,  252,  352,
 /*   170 */   353,  233,  271,  182,   13,  245,  261,  264,  267,  246,
 /*   180 */   248,  367,  375,  289,  322,  465,  338,  468,  470,  342,
 /*   190 */   471,  475,  395,  355,  377,  386,  380,  379,  402,  406,
 /*   200 */   484,  407,  414,  417,  415,  403,  416,  408,  421,  418,
 /*   210 */   419,  423,  424,  427,  426,  428,  455,  520,  526,  527,
 /*   220 */   528,  529,  530,  459,  522,  466,  524,  411,  420,  434,
 /*   230 */   537,  538,  444,  446,  434,  541,  542,  543,  544,  546,
 /*   240 */   547,  548,  549,  550,  551,  552,  553,  554,  555,  556,
 /*   250 */   458,  486,  545,  557,  505,  507,  567,
};
#define YY_REDUCE_COUNT (140)
#define YY_REDUCE_MIN   (-264)
#define YY_REDUCE_MAX   (290)
static const short yy_reduce_ofst[] = {
 /*     0 */  -206,  -76,   54, -187, -228, -175, -162,  -16,   53,   66,
 /*    10 */    79,   96, -203, -205, -264, -132,  -98,    7, -230, -176,
 /*    20 */   -43,   -9,    1, -192, -114,   26,   56,   88,   70, -257,
 /*    30 */  -253, -249, -242, -234, -201,  -61,  -33,   -2,    4,   30,
 /*    40 */    33,   71,   77,   86,   91,  105,  109,  129,  135,  138,
 /*    50 */   143,  144,  145,  146,  147,  148,   72,  181,  183,  184,
 /*    60 */   185,  122,  126,  151,  187,  188,  190,  193,  194,  195,
 /*    70 */   196,  197,  198,  201,  202,  203,  204,  205,  206,  207,
 /*    80 */   208,  210,  213,  215,  216,  217,  218,  219,  220,  221,
 /*    90 */   222,  224,  225,  226,  227,  228,  229,  230,  231,  232,
 /*   100 */   234,  235,  237,  238,  162,  239,  240,  153,  155,  156,
 /*   110 */   241,  186,  249,  251,  250,  254,  256,  258,  253,  262,
 /*   120 */   255,  257,  259,  260,  263,  265,  266,  268,  272,  269,
 /*   130 */   274,  273,  276,  280,  275,  277,  279,  282,  288,  284,
 /*   140 */   290,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   662,  716,  705,  713,  890,  890,  662,  662,  662,  662,
 /*    10 */   662,  662,  815,  680,  890,  662,  662,  662,  662,  662,
 /*    20 */   662,  662,  713,  662,  662,  718,  718,  718,  810,  662,
 /*    30 */   662,  662,  662,  662,  662,  662,  662,  662,  662,  662,
 /*    40 */   662,  662,  662,  662,  662,  662,  662,  662,  662,  662,
 /*    50 */   662,  662,  662,  662,  662,  662,  662,  662,  817,  819,
 /*    60 */   662,  837,  837,  808,  662,  662,  662,  662,  662,  662,
 /*    70 */   662,  662,  662,  662,  662,  662,  662,  662,  662,  662,
 /*    80 */   662,  662,  662,  703,  662,  701,  662,  662,  662,  662,
 /*    90 */   662,  662,  662,  662,  662,  662,  662,  662,  690,  662,
 /*   100 */   662,  662,  662,  662,  662,  682,  682,  662,  662,  662,
 /*   110 */   682,  844,  848,  842,  830,  838,  829,  825,  824,  852,
 /*   120 */   662,  682,  682,  682,  713,  682,  734,  732,  730,  722,
 /*   130 */   728,  724,  726,  720,  682,  711,  682,  711,  682,  751,
 /*   140 */   766,  662,  853,  889,  843,  879,  878,  885,  877,  876,
 /*   150 */   662,  872,  873,  875,  874,  662,  662,  662,  662,  881,
 /*   160 */   880,  662,  662,  662,  662,  662,  662,  662,  662,  662,
 /*   170 */   662,  662,  855,  662,  849,  845,  662,  662,  662,  662,
 /*   180 */   662,  662,  662,  662,  662,  662,  662,  662,  662,  662,
 /*   190 */   662,  662,  662,  662,  807,  662,  662,  816,  662,  662,
 /*   200 */   662,  662,  662,  662,  839,  662,  831,  662,  662,  662,
 /*   210 */   662,  662,  784,  662,  662,  662,  662,  662,  662,  662,
 /*   220 */   662,  662,  662,  662,  662,  662,  662,  662,  662,  894,
 /*   230 */   662,  662,  662,  775,  892,  662,  662,  662,  662,  662,
 /*   240 */   662,  662,  662,  662,  662,  662,  662,  662,  662,  662,
 /*   250 */   737,  662,  688,  686,  662,  678,  662,
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
    0,  /*  VARIABLES => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*     VNODES => nothing */
    1,  /*    IPTOKEN => ID */
    0,  /*        DOT => nothing */
    0,  /*     CREATE => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
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
  /*   54 */ "VARIABLES",
  /*   55 */ "SCORES",
  /*   56 */ "GRANTS",
  /*   57 */ "VNODES",
  /*   58 */ "IPTOKEN",
  /*   59 */ "DOT",
  /*   60 */ "CREATE",
  /*   61 */ "TABLE",
  /*   62 */ "DATABASE",
  /*   63 */ "TABLES",
  /*   64 */ "STABLES",
  /*   65 */ "VGROUPS",
  /*   66 */ "DROP",
  /*   67 */ "DNODE",
  /*   68 */ "USER",
  /*   69 */ "ACCOUNT",
  /*   70 */ "USE",
  /*   71 */ "DESCRIBE",
  /*   72 */ "ALTER",
  /*   73 */ "PASS",
  /*   74 */ "PRIVILEGE",
  /*   75 */ "LOCAL",
  /*   76 */ "IF",
  /*   77 */ "EXISTS",
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
  /*   88 */ "QUORUM",
  /*   89 */ "DAYS",
  /*   90 */ "MINROWS",
  /*   91 */ "MAXROWS",
  /*   92 */ "BLOCKS",
  /*   93 */ "CTIME",
  /*   94 */ "WAL",
  /*   95 */ "FSYNC",
  /*   96 */ "COMP",
  /*   97 */ "PRECISION",
  /*   98 */ "UPDATE",
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
  /*  229 */ "cache",
  /*  230 */ "replica",
  /*  231 */ "quorum",
  /*  232 */ "days",
  /*  233 */ "minrows",
  /*  234 */ "maxrows",
  /*  235 */ "blocks",
  /*  236 */ "ctime",
  /*  237 */ "wal",
  /*  238 */ "fsync",
  /*  239 */ "comp",
  /*  240 */ "prec",
  /*  241 */ "update",
  /*  242 */ "typename",
  /*  243 */ "signed",
  /*  244 */ "create_table_args",
  /*  245 */ "create_table_list",
  /*  246 */ "create_from_stable",
  /*  247 */ "columnlist",
  /*  248 */ "select",
  /*  249 */ "column",
  /*  250 */ "tagitem",
  /*  251 */ "selcollist",
  /*  252 */ "from",
  /*  253 */ "where_opt",
  /*  254 */ "interval_opt",
  /*  255 */ "fill_opt",
  /*  256 */ "sliding_opt",
  /*  257 */ "groupby_opt",
  /*  258 */ "orderby_opt",
  /*  259 */ "having_opt",
  /*  260 */ "slimit_opt",
  /*  261 */ "limit_opt",
  /*  262 */ "union",
  /*  263 */ "sclp",
  /*  264 */ "expr",
  /*  265 */ "as",
  /*  266 */ "tablelist",
  /*  267 */ "tmvar",
  /*  268 */ "sortlist",
  /*  269 */ "sortitem",
  /*  270 */ "item",
  /*  271 */ "sortorder",
  /*  272 */ "grouplist",
  /*  273 */ "exprlist",
  /*  274 */ "expritem",
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
 /*  10 */ "cmd ::= SHOW VARIABLES",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "cmd ::= SHOW VNODES",
 /*  14 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  15 */ "dbPrefix ::=",
 /*  16 */ "dbPrefix ::= ids DOT",
 /*  17 */ "cpxName ::=",
 /*  18 */ "cpxName ::= DOT ids",
 /*  19 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  20 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  27 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  28 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  29 */ "cmd ::= DROP DNODE ids",
 /*  30 */ "cmd ::= DROP USER ids",
 /*  31 */ "cmd ::= DROP ACCOUNT ids",
 /*  32 */ "cmd ::= USE ids",
 /*  33 */ "cmd ::= DESCRIBE ids cpxName",
 /*  34 */ "cmd ::= ALTER USER ids PASS ids",
 /*  35 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  36 */ "cmd ::= ALTER DNODE ids ids",
 /*  37 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  38 */ "cmd ::= ALTER LOCAL ids",
 /*  39 */ "cmd ::= ALTER LOCAL ids ids",
 /*  40 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  41 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  42 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  43 */ "ids ::= ID",
 /*  44 */ "ids ::= STRING",
 /*  45 */ "ifexists ::= IF EXISTS",
 /*  46 */ "ifexists ::=",
 /*  47 */ "ifnotexists ::= IF NOT EXISTS",
 /*  48 */ "ifnotexists ::=",
 /*  49 */ "cmd ::= CREATE DNODE ids",
 /*  50 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  51 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  52 */ "cmd ::= CREATE USER ids PASS ids",
 /*  53 */ "pps ::=",
 /*  54 */ "pps ::= PPS INTEGER",
 /*  55 */ "tseries ::=",
 /*  56 */ "tseries ::= TSERIES INTEGER",
 /*  57 */ "dbs ::=",
 /*  58 */ "dbs ::= DBS INTEGER",
 /*  59 */ "streams ::=",
 /*  60 */ "streams ::= STREAMS INTEGER",
 /*  61 */ "storage ::=",
 /*  62 */ "storage ::= STORAGE INTEGER",
 /*  63 */ "qtime ::=",
 /*  64 */ "qtime ::= QTIME INTEGER",
 /*  65 */ "users ::=",
 /*  66 */ "users ::= USERS INTEGER",
 /*  67 */ "conns ::=",
 /*  68 */ "conns ::= CONNS INTEGER",
 /*  69 */ "state ::=",
 /*  70 */ "state ::= STATE ids",
 /*  71 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  72 */ "keep ::= KEEP tagitemlist",
 /*  73 */ "cache ::= CACHE INTEGER",
 /*  74 */ "replica ::= REPLICA INTEGER",
 /*  75 */ "quorum ::= QUORUM INTEGER",
 /*  76 */ "days ::= DAYS INTEGER",
 /*  77 */ "minrows ::= MINROWS INTEGER",
 /*  78 */ "maxrows ::= MAXROWS INTEGER",
 /*  79 */ "blocks ::= BLOCKS INTEGER",
 /*  80 */ "ctime ::= CTIME INTEGER",
 /*  81 */ "wal ::= WAL INTEGER",
 /*  82 */ "fsync ::= FSYNC INTEGER",
 /*  83 */ "comp ::= COMP INTEGER",
 /*  84 */ "prec ::= PRECISION STRING",
 /*  85 */ "update ::= UPDATE INTEGER",
 /*  86 */ "db_optr ::=",
 /*  87 */ "db_optr ::= db_optr cache",
 /*  88 */ "db_optr ::= db_optr replica",
 /*  89 */ "db_optr ::= db_optr quorum",
 /*  90 */ "db_optr ::= db_optr days",
 /*  91 */ "db_optr ::= db_optr minrows",
 /*  92 */ "db_optr ::= db_optr maxrows",
 /*  93 */ "db_optr ::= db_optr blocks",
 /*  94 */ "db_optr ::= db_optr ctime",
 /*  95 */ "db_optr ::= db_optr wal",
 /*  96 */ "db_optr ::= db_optr fsync",
 /*  97 */ "db_optr ::= db_optr comp",
 /*  98 */ "db_optr ::= db_optr prec",
 /*  99 */ "db_optr ::= db_optr keep",
 /* 100 */ "db_optr ::= db_optr update",
 /* 101 */ "alter_db_optr ::=",
 /* 102 */ "alter_db_optr ::= alter_db_optr replica",
 /* 103 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 104 */ "alter_db_optr ::= alter_db_optr keep",
 /* 105 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 106 */ "alter_db_optr ::= alter_db_optr comp",
 /* 107 */ "alter_db_optr ::= alter_db_optr wal",
 /* 108 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 109 */ "alter_db_optr ::= alter_db_optr update",
 /* 110 */ "typename ::= ids",
 /* 111 */ "typename ::= ids LP signed RP",
 /* 112 */ "signed ::= INTEGER",
 /* 113 */ "signed ::= PLUS INTEGER",
 /* 114 */ "signed ::= MINUS INTEGER",
 /* 115 */ "cmd ::= CREATE TABLE create_table_args",
 /* 116 */ "cmd ::= CREATE TABLE create_table_list",
 /* 117 */ "create_table_list ::= create_from_stable",
 /* 118 */ "create_table_list ::= create_table_list create_from_stable",
 /* 119 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 120 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 121 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 122 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 123 */ "columnlist ::= columnlist COMMA column",
 /* 124 */ "columnlist ::= column",
 /* 125 */ "column ::= ids typename",
 /* 126 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 127 */ "tagitemlist ::= tagitem",
 /* 128 */ "tagitem ::= INTEGER",
 /* 129 */ "tagitem ::= FLOAT",
 /* 130 */ "tagitem ::= STRING",
 /* 131 */ "tagitem ::= BOOL",
 /* 132 */ "tagitem ::= NULL",
 /* 133 */ "tagitem ::= MINUS INTEGER",
 /* 134 */ "tagitem ::= MINUS FLOAT",
 /* 135 */ "tagitem ::= PLUS INTEGER",
 /* 136 */ "tagitem ::= PLUS FLOAT",
 /* 137 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 138 */ "union ::= select",
 /* 139 */ "union ::= LP union RP",
 /* 140 */ "union ::= union UNION ALL select",
 /* 141 */ "union ::= union UNION ALL LP select RP",
 /* 142 */ "cmd ::= union",
 /* 143 */ "select ::= SELECT selcollist",
 /* 144 */ "sclp ::= selcollist COMMA",
 /* 145 */ "sclp ::=",
 /* 146 */ "selcollist ::= sclp expr as",
 /* 147 */ "selcollist ::= sclp STAR",
 /* 148 */ "as ::= AS ids",
 /* 149 */ "as ::= ids",
 /* 150 */ "as ::=",
 /* 151 */ "from ::= FROM tablelist",
 /* 152 */ "tablelist ::= ids cpxName",
 /* 153 */ "tablelist ::= ids cpxName ids",
 /* 154 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 155 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 156 */ "tmvar ::= VARIABLE",
 /* 157 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 158 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 159 */ "interval_opt ::=",
 /* 160 */ "fill_opt ::=",
 /* 161 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 162 */ "fill_opt ::= FILL LP ID RP",
 /* 163 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 164 */ "sliding_opt ::=",
 /* 165 */ "orderby_opt ::=",
 /* 166 */ "orderby_opt ::= ORDER BY sortlist",
 /* 167 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 168 */ "sortlist ::= item sortorder",
 /* 169 */ "item ::= ids cpxName",
 /* 170 */ "sortorder ::= ASC",
 /* 171 */ "sortorder ::= DESC",
 /* 172 */ "sortorder ::=",
 /* 173 */ "groupby_opt ::=",
 /* 174 */ "groupby_opt ::= GROUP BY grouplist",
 /* 175 */ "grouplist ::= grouplist COMMA item",
 /* 176 */ "grouplist ::= item",
 /* 177 */ "having_opt ::=",
 /* 178 */ "having_opt ::= HAVING expr",
 /* 179 */ "limit_opt ::=",
 /* 180 */ "limit_opt ::= LIMIT signed",
 /* 181 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 182 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 183 */ "slimit_opt ::=",
 /* 184 */ "slimit_opt ::= SLIMIT signed",
 /* 185 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 186 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 187 */ "where_opt ::=",
 /* 188 */ "where_opt ::= WHERE expr",
 /* 189 */ "expr ::= LP expr RP",
 /* 190 */ "expr ::= ID",
 /* 191 */ "expr ::= ID DOT ID",
 /* 192 */ "expr ::= ID DOT STAR",
 /* 193 */ "expr ::= INTEGER",
 /* 194 */ "expr ::= MINUS INTEGER",
 /* 195 */ "expr ::= PLUS INTEGER",
 /* 196 */ "expr ::= FLOAT",
 /* 197 */ "expr ::= MINUS FLOAT",
 /* 198 */ "expr ::= PLUS FLOAT",
 /* 199 */ "expr ::= STRING",
 /* 200 */ "expr ::= NOW",
 /* 201 */ "expr ::= VARIABLE",
 /* 202 */ "expr ::= BOOL",
 /* 203 */ "expr ::= ID LP exprlist RP",
 /* 204 */ "expr ::= ID LP STAR RP",
 /* 205 */ "expr ::= expr IS NULL",
 /* 206 */ "expr ::= expr IS NOT NULL",
 /* 207 */ "expr ::= expr LT expr",
 /* 208 */ "expr ::= expr GT expr",
 /* 209 */ "expr ::= expr LE expr",
 /* 210 */ "expr ::= expr GE expr",
 /* 211 */ "expr ::= expr NE expr",
 /* 212 */ "expr ::= expr EQ expr",
 /* 213 */ "expr ::= expr AND expr",
 /* 214 */ "expr ::= expr OR expr",
 /* 215 */ "expr ::= expr PLUS expr",
 /* 216 */ "expr ::= expr MINUS expr",
 /* 217 */ "expr ::= expr STAR expr",
 /* 218 */ "expr ::= expr SLASH expr",
 /* 219 */ "expr ::= expr REM expr",
 /* 220 */ "expr ::= expr LIKE expr",
 /* 221 */ "expr ::= expr IN LP exprlist RP",
 /* 222 */ "exprlist ::= exprlist COMMA expritem",
 /* 223 */ "exprlist ::= expritem",
 /* 224 */ "expritem ::= expr",
 /* 225 */ "expritem ::=",
 /* 226 */ "cmd ::= RESET QUERY CACHE",
 /* 227 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 228 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 229 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 230 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 231 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 232 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 233 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 234 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 235 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 247: /* columnlist */
    case 255: /* fill_opt */
    case 257: /* groupby_opt */
    case 258: /* orderby_opt */
    case 268: /* sortlist */
    case 272: /* grouplist */
{
taosArrayDestroy((yypminor->yy131));
}
      break;
    case 245: /* create_table_list */
{
destroyCreateTableSQL((yypminor->yy538));
}
      break;
    case 248: /* select */
{
doDestroyQuerySql((yypminor->yy84));
}
      break;
    case 251: /* selcollist */
    case 263: /* sclp */
    case 273: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy478));
}
      break;
    case 253: /* where_opt */
    case 259: /* having_opt */
    case 264: /* expr */
    case 274: /* expritem */
{
tSQLExprDestroy((yypminor->yy420));
}
      break;
    case 262: /* union */
{
destroyAllSelectClause((yypminor->yy513));
}
      break;
    case 269: /* sortitem */
{
tVariantDestroy(&(yypminor->yy516));
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
  {  209,   -2 }, /* (10) cmd ::= SHOW VARIABLES */
  {  209,   -2 }, /* (11) cmd ::= SHOW SCORES */
  {  209,   -2 }, /* (12) cmd ::= SHOW GRANTS */
  {  209,   -2 }, /* (13) cmd ::= SHOW VNODES */
  {  209,   -3 }, /* (14) cmd ::= SHOW VNODES IPTOKEN */
  {  210,    0 }, /* (15) dbPrefix ::= */
  {  210,   -2 }, /* (16) dbPrefix ::= ids DOT */
  {  212,    0 }, /* (17) cpxName ::= */
  {  212,   -2 }, /* (18) cpxName ::= DOT ids */
  {  209,   -5 }, /* (19) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  209,   -4 }, /* (20) cmd ::= SHOW CREATE DATABASE ids */
  {  209,   -3 }, /* (21) cmd ::= SHOW dbPrefix TABLES */
  {  209,   -5 }, /* (22) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  209,   -3 }, /* (23) cmd ::= SHOW dbPrefix STABLES */
  {  209,   -5 }, /* (24) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  209,   -3 }, /* (25) cmd ::= SHOW dbPrefix VGROUPS */
  {  209,   -4 }, /* (26) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  209,   -5 }, /* (27) cmd ::= DROP TABLE ifexists ids cpxName */
  {  209,   -4 }, /* (28) cmd ::= DROP DATABASE ifexists ids */
  {  209,   -3 }, /* (29) cmd ::= DROP DNODE ids */
  {  209,   -3 }, /* (30) cmd ::= DROP USER ids */
  {  209,   -3 }, /* (31) cmd ::= DROP ACCOUNT ids */
  {  209,   -2 }, /* (32) cmd ::= USE ids */
  {  209,   -3 }, /* (33) cmd ::= DESCRIBE ids cpxName */
  {  209,   -5 }, /* (34) cmd ::= ALTER USER ids PASS ids */
  {  209,   -5 }, /* (35) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  209,   -4 }, /* (36) cmd ::= ALTER DNODE ids ids */
  {  209,   -5 }, /* (37) cmd ::= ALTER DNODE ids ids ids */
  {  209,   -3 }, /* (38) cmd ::= ALTER LOCAL ids */
  {  209,   -4 }, /* (39) cmd ::= ALTER LOCAL ids ids */
  {  209,   -4 }, /* (40) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  209,   -4 }, /* (41) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  209,   -6 }, /* (42) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  211,   -1 }, /* (43) ids ::= ID */
  {  211,   -1 }, /* (44) ids ::= STRING */
  {  213,   -2 }, /* (45) ifexists ::= IF EXISTS */
  {  213,    0 }, /* (46) ifexists ::= */
  {  216,   -3 }, /* (47) ifnotexists ::= IF NOT EXISTS */
  {  216,    0 }, /* (48) ifnotexists ::= */
  {  209,   -3 }, /* (49) cmd ::= CREATE DNODE ids */
  {  209,   -6 }, /* (50) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  209,   -5 }, /* (51) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  209,   -5 }, /* (52) cmd ::= CREATE USER ids PASS ids */
  {  218,    0 }, /* (53) pps ::= */
  {  218,   -2 }, /* (54) pps ::= PPS INTEGER */
  {  219,    0 }, /* (55) tseries ::= */
  {  219,   -2 }, /* (56) tseries ::= TSERIES INTEGER */
  {  220,    0 }, /* (57) dbs ::= */
  {  220,   -2 }, /* (58) dbs ::= DBS INTEGER */
  {  221,    0 }, /* (59) streams ::= */
  {  221,   -2 }, /* (60) streams ::= STREAMS INTEGER */
  {  222,    0 }, /* (61) storage ::= */
  {  222,   -2 }, /* (62) storage ::= STORAGE INTEGER */
  {  223,    0 }, /* (63) qtime ::= */
  {  223,   -2 }, /* (64) qtime ::= QTIME INTEGER */
  {  224,    0 }, /* (65) users ::= */
  {  224,   -2 }, /* (66) users ::= USERS INTEGER */
  {  225,    0 }, /* (67) conns ::= */
  {  225,   -2 }, /* (68) conns ::= CONNS INTEGER */
  {  226,    0 }, /* (69) state ::= */
  {  226,   -2 }, /* (70) state ::= STATE ids */
  {  215,   -9 }, /* (71) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  227,   -2 }, /* (72) keep ::= KEEP tagitemlist */
  {  229,   -2 }, /* (73) cache ::= CACHE INTEGER */
  {  230,   -2 }, /* (74) replica ::= REPLICA INTEGER */
  {  231,   -2 }, /* (75) quorum ::= QUORUM INTEGER */
  {  232,   -2 }, /* (76) days ::= DAYS INTEGER */
  {  233,   -2 }, /* (77) minrows ::= MINROWS INTEGER */
  {  234,   -2 }, /* (78) maxrows ::= MAXROWS INTEGER */
  {  235,   -2 }, /* (79) blocks ::= BLOCKS INTEGER */
  {  236,   -2 }, /* (80) ctime ::= CTIME INTEGER */
  {  237,   -2 }, /* (81) wal ::= WAL INTEGER */
  {  238,   -2 }, /* (82) fsync ::= FSYNC INTEGER */
  {  239,   -2 }, /* (83) comp ::= COMP INTEGER */
  {  240,   -2 }, /* (84) prec ::= PRECISION STRING */
  {  241,   -2 }, /* (85) update ::= UPDATE INTEGER */
  {  217,    0 }, /* (86) db_optr ::= */
  {  217,   -2 }, /* (87) db_optr ::= db_optr cache */
  {  217,   -2 }, /* (88) db_optr ::= db_optr replica */
  {  217,   -2 }, /* (89) db_optr ::= db_optr quorum */
  {  217,   -2 }, /* (90) db_optr ::= db_optr days */
  {  217,   -2 }, /* (91) db_optr ::= db_optr minrows */
  {  217,   -2 }, /* (92) db_optr ::= db_optr maxrows */
  {  217,   -2 }, /* (93) db_optr ::= db_optr blocks */
  {  217,   -2 }, /* (94) db_optr ::= db_optr ctime */
  {  217,   -2 }, /* (95) db_optr ::= db_optr wal */
  {  217,   -2 }, /* (96) db_optr ::= db_optr fsync */
  {  217,   -2 }, /* (97) db_optr ::= db_optr comp */
  {  217,   -2 }, /* (98) db_optr ::= db_optr prec */
  {  217,   -2 }, /* (99) db_optr ::= db_optr keep */
  {  217,   -2 }, /* (100) db_optr ::= db_optr update */
  {  214,    0 }, /* (101) alter_db_optr ::= */
  {  214,   -2 }, /* (102) alter_db_optr ::= alter_db_optr replica */
  {  214,   -2 }, /* (103) alter_db_optr ::= alter_db_optr quorum */
  {  214,   -2 }, /* (104) alter_db_optr ::= alter_db_optr keep */
  {  214,   -2 }, /* (105) alter_db_optr ::= alter_db_optr blocks */
  {  214,   -2 }, /* (106) alter_db_optr ::= alter_db_optr comp */
  {  214,   -2 }, /* (107) alter_db_optr ::= alter_db_optr wal */
  {  214,   -2 }, /* (108) alter_db_optr ::= alter_db_optr fsync */
  {  214,   -2 }, /* (109) alter_db_optr ::= alter_db_optr update */
  {  242,   -1 }, /* (110) typename ::= ids */
  {  242,   -4 }, /* (111) typename ::= ids LP signed RP */
  {  243,   -1 }, /* (112) signed ::= INTEGER */
  {  243,   -2 }, /* (113) signed ::= PLUS INTEGER */
  {  243,   -2 }, /* (114) signed ::= MINUS INTEGER */
  {  209,   -3 }, /* (115) cmd ::= CREATE TABLE create_table_args */
  {  209,   -3 }, /* (116) cmd ::= CREATE TABLE create_table_list */
  {  245,   -1 }, /* (117) create_table_list ::= create_from_stable */
  {  245,   -2 }, /* (118) create_table_list ::= create_table_list create_from_stable */
  {  244,   -6 }, /* (119) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  244,  -10 }, /* (120) create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  246,  -10 }, /* (121) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  244,   -5 }, /* (122) create_table_args ::= ifnotexists ids cpxName AS select */
  {  247,   -3 }, /* (123) columnlist ::= columnlist COMMA column */
  {  247,   -1 }, /* (124) columnlist ::= column */
  {  249,   -2 }, /* (125) column ::= ids typename */
  {  228,   -3 }, /* (126) tagitemlist ::= tagitemlist COMMA tagitem */
  {  228,   -1 }, /* (127) tagitemlist ::= tagitem */
  {  250,   -1 }, /* (128) tagitem ::= INTEGER */
  {  250,   -1 }, /* (129) tagitem ::= FLOAT */
  {  250,   -1 }, /* (130) tagitem ::= STRING */
  {  250,   -1 }, /* (131) tagitem ::= BOOL */
  {  250,   -1 }, /* (132) tagitem ::= NULL */
  {  250,   -2 }, /* (133) tagitem ::= MINUS INTEGER */
  {  250,   -2 }, /* (134) tagitem ::= MINUS FLOAT */
  {  250,   -2 }, /* (135) tagitem ::= PLUS INTEGER */
  {  250,   -2 }, /* (136) tagitem ::= PLUS FLOAT */
  {  248,  -12 }, /* (137) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  262,   -1 }, /* (138) union ::= select */
  {  262,   -3 }, /* (139) union ::= LP union RP */
  {  262,   -4 }, /* (140) union ::= union UNION ALL select */
  {  262,   -6 }, /* (141) union ::= union UNION ALL LP select RP */
  {  209,   -1 }, /* (142) cmd ::= union */
  {  248,   -2 }, /* (143) select ::= SELECT selcollist */
  {  263,   -2 }, /* (144) sclp ::= selcollist COMMA */
  {  263,    0 }, /* (145) sclp ::= */
  {  251,   -3 }, /* (146) selcollist ::= sclp expr as */
  {  251,   -2 }, /* (147) selcollist ::= sclp STAR */
  {  265,   -2 }, /* (148) as ::= AS ids */
  {  265,   -1 }, /* (149) as ::= ids */
  {  265,    0 }, /* (150) as ::= */
  {  252,   -2 }, /* (151) from ::= FROM tablelist */
  {  266,   -2 }, /* (152) tablelist ::= ids cpxName */
  {  266,   -3 }, /* (153) tablelist ::= ids cpxName ids */
  {  266,   -4 }, /* (154) tablelist ::= tablelist COMMA ids cpxName */
  {  266,   -5 }, /* (155) tablelist ::= tablelist COMMA ids cpxName ids */
  {  267,   -1 }, /* (156) tmvar ::= VARIABLE */
  {  254,   -4 }, /* (157) interval_opt ::= INTERVAL LP tmvar RP */
  {  254,   -6 }, /* (158) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  254,    0 }, /* (159) interval_opt ::= */
  {  255,    0 }, /* (160) fill_opt ::= */
  {  255,   -6 }, /* (161) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  255,   -4 }, /* (162) fill_opt ::= FILL LP ID RP */
  {  256,   -4 }, /* (163) sliding_opt ::= SLIDING LP tmvar RP */
  {  256,    0 }, /* (164) sliding_opt ::= */
  {  258,    0 }, /* (165) orderby_opt ::= */
  {  258,   -3 }, /* (166) orderby_opt ::= ORDER BY sortlist */
  {  268,   -4 }, /* (167) sortlist ::= sortlist COMMA item sortorder */
  {  268,   -2 }, /* (168) sortlist ::= item sortorder */
  {  270,   -2 }, /* (169) item ::= ids cpxName */
  {  271,   -1 }, /* (170) sortorder ::= ASC */
  {  271,   -1 }, /* (171) sortorder ::= DESC */
  {  271,    0 }, /* (172) sortorder ::= */
  {  257,    0 }, /* (173) groupby_opt ::= */
  {  257,   -3 }, /* (174) groupby_opt ::= GROUP BY grouplist */
  {  272,   -3 }, /* (175) grouplist ::= grouplist COMMA item */
  {  272,   -1 }, /* (176) grouplist ::= item */
  {  259,    0 }, /* (177) having_opt ::= */
  {  259,   -2 }, /* (178) having_opt ::= HAVING expr */
  {  261,    0 }, /* (179) limit_opt ::= */
  {  261,   -2 }, /* (180) limit_opt ::= LIMIT signed */
  {  261,   -4 }, /* (181) limit_opt ::= LIMIT signed OFFSET signed */
  {  261,   -4 }, /* (182) limit_opt ::= LIMIT signed COMMA signed */
  {  260,    0 }, /* (183) slimit_opt ::= */
  {  260,   -2 }, /* (184) slimit_opt ::= SLIMIT signed */
  {  260,   -4 }, /* (185) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  260,   -4 }, /* (186) slimit_opt ::= SLIMIT signed COMMA signed */
  {  253,    0 }, /* (187) where_opt ::= */
  {  253,   -2 }, /* (188) where_opt ::= WHERE expr */
  {  264,   -3 }, /* (189) expr ::= LP expr RP */
  {  264,   -1 }, /* (190) expr ::= ID */
  {  264,   -3 }, /* (191) expr ::= ID DOT ID */
  {  264,   -3 }, /* (192) expr ::= ID DOT STAR */
  {  264,   -1 }, /* (193) expr ::= INTEGER */
  {  264,   -2 }, /* (194) expr ::= MINUS INTEGER */
  {  264,   -2 }, /* (195) expr ::= PLUS INTEGER */
  {  264,   -1 }, /* (196) expr ::= FLOAT */
  {  264,   -2 }, /* (197) expr ::= MINUS FLOAT */
  {  264,   -2 }, /* (198) expr ::= PLUS FLOAT */
  {  264,   -1 }, /* (199) expr ::= STRING */
  {  264,   -1 }, /* (200) expr ::= NOW */
  {  264,   -1 }, /* (201) expr ::= VARIABLE */
  {  264,   -1 }, /* (202) expr ::= BOOL */
  {  264,   -4 }, /* (203) expr ::= ID LP exprlist RP */
  {  264,   -4 }, /* (204) expr ::= ID LP STAR RP */
  {  264,   -3 }, /* (205) expr ::= expr IS NULL */
  {  264,   -4 }, /* (206) expr ::= expr IS NOT NULL */
  {  264,   -3 }, /* (207) expr ::= expr LT expr */
  {  264,   -3 }, /* (208) expr ::= expr GT expr */
  {  264,   -3 }, /* (209) expr ::= expr LE expr */
  {  264,   -3 }, /* (210) expr ::= expr GE expr */
  {  264,   -3 }, /* (211) expr ::= expr NE expr */
  {  264,   -3 }, /* (212) expr ::= expr EQ expr */
  {  264,   -3 }, /* (213) expr ::= expr AND expr */
  {  264,   -3 }, /* (214) expr ::= expr OR expr */
  {  264,   -3 }, /* (215) expr ::= expr PLUS expr */
  {  264,   -3 }, /* (216) expr ::= expr MINUS expr */
  {  264,   -3 }, /* (217) expr ::= expr STAR expr */
  {  264,   -3 }, /* (218) expr ::= expr SLASH expr */
  {  264,   -3 }, /* (219) expr ::= expr REM expr */
  {  264,   -3 }, /* (220) expr ::= expr LIKE expr */
  {  264,   -5 }, /* (221) expr ::= expr IN LP exprlist RP */
  {  273,   -3 }, /* (222) exprlist ::= exprlist COMMA expritem */
  {  273,   -1 }, /* (223) exprlist ::= expritem */
  {  274,   -1 }, /* (224) expritem ::= expr */
  {  274,    0 }, /* (225) expritem ::= */
  {  209,   -3 }, /* (226) cmd ::= RESET QUERY CACHE */
  {  209,   -7 }, /* (227) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  209,   -7 }, /* (228) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  209,   -7 }, /* (229) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  209,   -7 }, /* (230) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  209,   -8 }, /* (231) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  209,   -9 }, /* (232) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  209,   -3 }, /* (233) cmd ::= KILL CONNECTION INTEGER */
  {  209,   -5 }, /* (234) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  209,   -5 }, /* (235) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 115: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==115);
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
      case 10: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
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
      case 19: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDBName(&token, &yymsp[-2].minor.yy0);    
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDBTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDBTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 29: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 30: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 31: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 32: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 33: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 34: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSQL(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 35: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSQL(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 38: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 39: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy148, &t);}
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy309);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy309);}
        break;
      case 43: /* ids ::= ID */
      case 44: /* ids ::= STRING */ yytestcase(yyruleno==44);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 45: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 46: /* ifexists ::= */
      case 48: /* ifnotexists ::= */ yytestcase(yyruleno==48);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 47: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 49: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 50: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy309);}
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy148, &yymsp[-2].minor.yy0);}
        break;
      case 52: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSQL(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 53: /* pps ::= */
      case 55: /* tseries ::= */ yytestcase(yyruleno==55);
      case 57: /* dbs ::= */ yytestcase(yyruleno==57);
      case 59: /* streams ::= */ yytestcase(yyruleno==59);
      case 61: /* storage ::= */ yytestcase(yyruleno==61);
      case 63: /* qtime ::= */ yytestcase(yyruleno==63);
      case 65: /* users ::= */ yytestcase(yyruleno==65);
      case 67: /* conns ::= */ yytestcase(yyruleno==67);
      case 69: /* state ::= */ yytestcase(yyruleno==69);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 54: /* pps ::= PPS INTEGER */
      case 56: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==56);
      case 58: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==58);
      case 60: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==60);
      case 62: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==62);
      case 64: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==64);
      case 66: /* users ::= USERS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* state ::= STATE ids */ yytestcase(yyruleno==70);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 71: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy309.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy309.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy309.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy309.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy309.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy309.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy309.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy309.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy309.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy309 = yylhsminor.yy309;
        break;
      case 72: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy131 = yymsp[0].minor.yy131; }
        break;
      case 73: /* cache ::= CACHE INTEGER */
      case 74: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==74);
      case 75: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==75);
      case 76: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==79);
      case 80: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==80);
      case 81: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==81);
      case 82: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==82);
      case 83: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==83);
      case 84: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==84);
      case 85: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==85);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 86: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy148);}
        break;
      case 87: /* db_optr ::= db_optr cache */
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 88: /* db_optr ::= db_optr replica */
      case 102: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==102);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 89: /* db_optr ::= db_optr quorum */
      case 103: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==103);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 90: /* db_optr ::= db_optr days */
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 91: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 92: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 93: /* db_optr ::= db_optr blocks */
      case 105: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==105);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 94: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 95: /* db_optr ::= db_optr wal */
      case 107: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==107);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 96: /* db_optr ::= db_optr fsync */
      case 108: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==108);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 97: /* db_optr ::= db_optr comp */
      case 106: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==106);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 98: /* db_optr ::= db_optr prec */
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 99: /* db_optr ::= db_optr keep */
      case 104: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==104);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.keep = yymsp[0].minor.yy131; }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 100: /* db_optr ::= db_optr update */
      case 109: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==109);
{ yylhsminor.yy148 = yymsp[-1].minor.yy148; yylhsminor.yy148.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 101: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy148);}
        break;
      case 110: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSQLSetColumnType (&yylhsminor.yy163, &yymsp[0].minor.yy0); 
}
  yymsp[0].minor.yy163 = yylhsminor.yy163;
        break;
      case 111: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy459 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSQLSetColumnType(&yylhsminor.yy163, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy459;  // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy163, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy163 = yylhsminor.yy163;
        break;
      case 112: /* signed ::= INTEGER */
{ yylhsminor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy459 = yylhsminor.yy459;
        break;
      case 113: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 114: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy459 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 116: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy538;}
        break;
      case 117: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy538 = pCreateTable;
}
  yymsp[0].minor.yy538 = yylhsminor.yy538;
        break;
      case 118: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy538->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy538 = yymsp[-1].minor.yy538;
}
  yymsp[-1].minor.yy538 = yylhsminor.yy538;
        break;
      case 119: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy538 = tSetCreateSQLElems(yymsp[-1].minor.yy131, NULL, NULL, TSQL_CREATE_TABLE);
  setSQLInfo(pInfo, yylhsminor.yy538, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy538 = yylhsminor.yy538;
        break;
      case 120: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy538 = tSetCreateSQLElems(yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, NULL, TSQL_CREATE_STABLE);
  setSQLInfo(pInfo, yylhsminor.yy538, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy538 = yylhsminor.yy538;
        break;
      case 121: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, yymsp[-1].minor.yy131, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 122: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy538 = tSetCreateSQLElems(NULL, NULL, yymsp[0].minor.yy84, TSQL_CREATE_STREAM);
  setSQLInfo(pInfo, yylhsminor.yy538, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy538 = yylhsminor.yy538;
        break;
      case 123: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy163); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 124: /* columnlist ::= column */
{yylhsminor.yy131 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy163);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 125: /* column ::= ids typename */
{
  tSQLSetColumnInfo(&yylhsminor.yy163, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy163);
}
  yymsp[-1].minor.yy163 = yylhsminor.yy163;
        break;
      case 126: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy131 = tVariantListAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy516, -1);    }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 127: /* tagitemlist ::= tagitem */
{ yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[0].minor.yy516, -1); }
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 128: /* tagitem ::= INTEGER */
      case 129: /* tagitem ::= FLOAT */ yytestcase(yyruleno==129);
      case 130: /* tagitem ::= STRING */ yytestcase(yyruleno==130);
      case 131: /* tagitem ::= BOOL */ yytestcase(yyruleno==131);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 132: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy516, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy516 = yylhsminor.yy516;
        break;
      case 133: /* tagitem ::= MINUS INTEGER */
      case 134: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==134);
      case 135: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==135);
      case 136: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==136);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy516, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 137: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy84 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy478, yymsp[-9].minor.yy131, yymsp[-8].minor.yy420, yymsp[-4].minor.yy131, yymsp[-3].minor.yy131, &yymsp[-7].minor.yy530, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy131, &yymsp[0].minor.yy284, &yymsp[-1].minor.yy284);
}
  yymsp[-11].minor.yy84 = yylhsminor.yy84;
        break;
      case 138: /* union ::= select */
{ yylhsminor.yy513 = setSubclause(NULL, yymsp[0].minor.yy84); }
  yymsp[0].minor.yy513 = yylhsminor.yy513;
        break;
      case 139: /* union ::= LP union RP */
{ yymsp[-2].minor.yy513 = yymsp[-1].minor.yy513; }
        break;
      case 140: /* union ::= union UNION ALL select */
{ yylhsminor.yy513 = appendSelectClause(yymsp[-3].minor.yy513, yymsp[0].minor.yy84); }
  yymsp[-3].minor.yy513 = yylhsminor.yy513;
        break;
      case 141: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy513 = appendSelectClause(yymsp[-5].minor.yy513, yymsp[-1].minor.yy84); }
  yymsp[-5].minor.yy513 = yylhsminor.yy513;
        break;
      case 142: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy513, NULL, TSDB_SQL_SELECT); }
        break;
      case 143: /* select ::= SELECT selcollist */
{
  yylhsminor.yy84 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy478, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy84 = yylhsminor.yy84;
        break;
      case 144: /* sclp ::= selcollist COMMA */
{yylhsminor.yy478 = yymsp[-1].minor.yy478;}
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 145: /* sclp ::= */
{yymsp[1].minor.yy478 = 0;}
        break;
      case 146: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy478 = tSQLExprListAppend(yymsp[-2].minor.yy478, yymsp[-1].minor.yy420, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy478 = yylhsminor.yy478;
        break;
      case 147: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy478 = tSQLExprListAppend(yymsp[-1].minor.yy478, pNode, 0);
}
  yymsp[-1].minor.yy478 = yylhsminor.yy478;
        break;
      case 148: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 149: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 150: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 151: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 152: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy131 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy131 = tVariantListAppendToken(yylhsminor.yy131, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 153: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy131 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy131 = tVariantListAppendToken(yylhsminor.yy131, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 154: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy131 = tVariantListAppendToken(yymsp[-3].minor.yy131, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy131 = tVariantListAppendToken(yylhsminor.yy131, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 155: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy131 = tVariantListAppendToken(yymsp[-4].minor.yy131, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy131 = tVariantListAppendToken(yylhsminor.yy131, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy131 = yylhsminor.yy131;
        break;
      case 156: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 157: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy530.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy530.offset.n = 0; yymsp[-3].minor.yy530.offset.z = NULL; yymsp[-3].minor.yy530.offset.type = 0;}
        break;
      case 158: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy530.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy530.offset = yymsp[-1].minor.yy0;}
        break;
      case 159: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy530, 0, sizeof(yymsp[1].minor.yy530));}
        break;
      case 160: /* fill_opt ::= */
{yymsp[1].minor.yy131 = 0;     }
        break;
      case 161: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy131, &A, -1, 0);
    yymsp[-5].minor.yy131 = yymsp[-1].minor.yy131;
}
        break;
      case 162: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy131 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 163: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 164: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 165: /* orderby_opt ::= */
{yymsp[1].minor.yy131 = 0;}
        break;
      case 166: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 167: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy131 = tVariantListAppend(yymsp[-3].minor.yy131, &yymsp[-1].minor.yy516, yymsp[0].minor.yy42);
}
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 168: /* sortlist ::= item sortorder */
{
  yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[-1].minor.yy516, yymsp[0].minor.yy42);
}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 169: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy516, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy516 = yylhsminor.yy516;
        break;
      case 170: /* sortorder ::= ASC */
{ yymsp[0].minor.yy42 = TSDB_ORDER_ASC; }
        break;
      case 171: /* sortorder ::= DESC */
{ yymsp[0].minor.yy42 = TSDB_ORDER_DESC;}
        break;
      case 172: /* sortorder ::= */
{ yymsp[1].minor.yy42 = TSDB_ORDER_ASC; }
        break;
      case 173: /* groupby_opt ::= */
{ yymsp[1].minor.yy131 = 0;}
        break;
      case 174: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 175: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy131 = tVariantListAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy516, -1);
}
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 176: /* grouplist ::= item */
{
  yylhsminor.yy131 = tVariantListAppend(NULL, &yymsp[0].minor.yy516, -1);
}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 177: /* having_opt ::= */
      case 187: /* where_opt ::= */ yytestcase(yyruleno==187);
      case 225: /* expritem ::= */ yytestcase(yyruleno==225);
{yymsp[1].minor.yy420 = 0;}
        break;
      case 178: /* having_opt ::= HAVING expr */
      case 188: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==188);
{yymsp[-1].minor.yy420 = yymsp[0].minor.yy420;}
        break;
      case 179: /* limit_opt ::= */
      case 183: /* slimit_opt ::= */ yytestcase(yyruleno==183);
{yymsp[1].minor.yy284.limit = -1; yymsp[1].minor.yy284.offset = 0;}
        break;
      case 180: /* limit_opt ::= LIMIT signed */
      case 184: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==184);
{yymsp[-1].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-1].minor.yy284.offset = 0;}
        break;
      case 181: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy459;}
        break;
      case 182: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy459;}
        break;
      case 185: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy284.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[0].minor.yy459;}
        break;
      case 186: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy284.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy284.offset = yymsp[-2].minor.yy459;}
        break;
      case 189: /* expr ::= LP expr RP */
{yylhsminor.yy420 = yymsp[-1].minor.yy420; yylhsminor.yy420->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy420->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 190: /* expr ::= ID */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 191: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 192: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 193: /* expr ::= INTEGER */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 194: /* expr ::= MINUS INTEGER */
      case 195: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==195);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy420 = yylhsminor.yy420;
        break;
      case 196: /* expr ::= FLOAT */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 197: /* expr ::= MINUS FLOAT */
      case 198: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==198);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy420 = yylhsminor.yy420;
        break;
      case 199: /* expr ::= STRING */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 200: /* expr ::= NOW */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 201: /* expr ::= VARIABLE */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 202: /* expr ::= BOOL */
{ yylhsminor.yy420 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 203: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy420 = tSQLExprCreateFunction(yymsp[-1].minor.yy478, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy420 = yylhsminor.yy420;
        break;
      case 204: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy420 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy420 = yylhsminor.yy420;
        break;
      case 205: /* expr ::= expr IS NULL */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 206: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-3].minor.yy420, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy420 = yylhsminor.yy420;
        break;
      case 207: /* expr ::= expr LT expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_LT);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 208: /* expr ::= expr GT expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_GT);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 209: /* expr ::= expr LE expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_LE);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 210: /* expr ::= expr GE expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_GE);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 211: /* expr ::= expr NE expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_NE);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 212: /* expr ::= expr EQ expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_EQ);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 213: /* expr ::= expr AND expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_AND);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 214: /* expr ::= expr OR expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_OR); }
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 215: /* expr ::= expr PLUS expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_PLUS);  }
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 216: /* expr ::= expr MINUS expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_MINUS); }
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 217: /* expr ::= expr STAR expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_STAR);  }
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 218: /* expr ::= expr SLASH expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_DIVIDE);}
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 219: /* expr ::= expr REM expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_REM);   }
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 220: /* expr ::= expr LIKE expr */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-2].minor.yy420, yymsp[0].minor.yy420, TK_LIKE);  }
  yymsp[-2].minor.yy420 = yylhsminor.yy420;
        break;
      case 221: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy420 = tSQLExprCreate(yymsp[-4].minor.yy420, (tSQLExpr*)yymsp[-1].minor.yy478, TK_IN); }
  yymsp[-4].minor.yy420 = yylhsminor.yy420;
        break;
      case 222: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy478 = tSQLExprListAppend(yymsp[-2].minor.yy478,yymsp[0].minor.yy420,0);}
  yymsp[-2].minor.yy478 = yylhsminor.yy478;
        break;
      case 223: /* exprlist ::= expritem */
{yylhsminor.yy478 = tSQLExprListAppend(0,yymsp[0].minor.yy420,0);}
  yymsp[0].minor.yy478 = yylhsminor.yy478;
        break;
      case 224: /* expritem ::= expr */
{yylhsminor.yy420 = yymsp[0].minor.yy420;}
  yymsp[0].minor.yy420 = yylhsminor.yy420;
        break;
      case 226: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 227: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 228: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 229: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 230: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 231: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 232: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy516, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 233: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 234: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 235: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
