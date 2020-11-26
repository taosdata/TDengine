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
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy46;
  tSQLExpr* yy64;
  tVariant yy134;
  SCreateAcctSQL yy149;
  SArray* yy165;
  int64_t yy207;
  SLimitVal yy216;
  TAOS_FIELD yy223;
  SSubclauseInfo* yy231;
  SCreateDBInfo yy268;
  tSQLExprList* yy290;
  SQuerySQL* yy414;
  SCreateTableSQL* yy470;
  SIntervalVal yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             253
#define YYNRULE              233
#define YYNTOKEN             207
#define YY_MAX_SHIFT         252
#define YY_MIN_SHIFTREDUCE   420
#define YY_MAX_SHIFTREDUCE   652
#define YY_ERROR_ACTION      653
#define YY_ACCEPT_ACTION     654
#define YY_NO_ACTION         655
#define YY_MIN_REDUCE        656
#define YY_MAX_REDUCE        888
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
#define YY_ACTTAB_COUNT (571)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   108,  463,  141,   11,  654,  252,  802,  463,  140,  464,
 /*    10 */   162,  165,  876,   35,   36,  464,   37,   38,  159,  250,
 /*    20 */   170,   29,  141,  463,  206,   41,   39,   43,   40,  173,
 /*    30 */   780,  464,  875,   34,   33,  145,  141,   32,   31,   30,
 /*    40 */    35,   36,  791,   37,   38,  164,  876,  170,   29,  780,
 /*    50 */    21,  206,   41,   39,   43,   40,  191,  829,  799,  201,
 /*    60 */    34,   33,   21,   21,   32,   31,   30,  421,  422,  423,
 /*    70 */   424,  425,  426,  427,  428,  429,  430,  431,  432,  251,
 /*    80 */    35,   36,  181,   37,   38,  532,  776,  170,   29,  238,
 /*    90 */   246,  206,   41,   39,   43,   40,  174,  175,  777,  777,
 /*   100 */    34,   33,  872,   56,   32,   31,   30,  176,  871,   36,
 /*   110 */   780,   37,   38,  227,  226,  170,   29,  791,   17,  206,
 /*   120 */    41,   39,   43,   40,  108,   26,  870,  606,   34,   33,
 /*   130 */    78,  160,   32,   31,   30,  238,  157,   16,  218,  245,
 /*   140 */   244,  217,  216,  215,  243,  214,  242,  241,  240,  213,
 /*   150 */   239,  755,  103,  743,  744,  745,  746,  747,  748,  749,
 /*   160 */   750,  751,  752,  753,  754,  756,   37,   38,  229,  177,
 /*   170 */   170,   29,  224,  223,  206,   41,   39,   43,   40,  203,
 /*   180 */    62,   60,    8,   34,   33,   63,  118,   32,   31,   30,
 /*   190 */   169,  619,   27,   12,  610,  184,  613,  158,  616,  778,
 /*   200 */   169,  619,  188,  187,  610,  194,  613,  108,  616,  153,
 /*   210 */   169,  619,  561,  108,  610,  154,  613,   18,  616,   90,
 /*   220 */    89,  148,  166,  167,   34,   33,  205,  143,   32,   31,
 /*   230 */    30,  697,  166,  167,  131,  144,  564,   41,   39,   43,
 /*   240 */    40,  706,  166,  167,  131,   34,   33,  146,   17,   32,
 /*   250 */    31,   30,   32,   31,   30,   26,   16,  207,  245,  244,
 /*   260 */    21,  587,  588,  243,  828,  242,  241,  240,  698,  239,
 /*   270 */    61,  131,   76,   80,  147,  190,  102,  151,   85,   88,
 /*   280 */    79,  760,  156,   26,  758,  759,   82,   21,   42,  761,
 /*   290 */   556,  763,  764,  762,  225,  765,  777,  193,   42,  618,
 /*   300 */   249,  248,   96,  574,  121,  122,  608,  105,   42,  618,
 /*   310 */    70,   66,   69,  578,  617,  168,  579,   46,  152,  618,
 /*   320 */    14,  230,  548,  777,  617,  545,  638,  546,  150,  547,
 /*   330 */    13,  135,  133,  612,  617,  615,  139,   93,   92,   91,
 /*   340 */   620,  611,  609,  614,   13,   47,  538,  622,   50,  552,
 /*   350 */    46,  553,  537,  178,  179,    3,   22,  211,   75,   74,
 /*   360 */   149,   22,   10,    9,   48,   51,  142,  550,  885,  551,
 /*   370 */    87,   86,  101,   99,  779,  839,  838,  171,  835,  834,
 /*   380 */   172,  801,  771,  228,  806,  793,  808,  104,  821,  119,
 /*   390 */   820,  117,  120,  708,  212,  137,   24,  221,  705,  222,
 /*   400 */    26,  192,  100,  884,   72,  883,  881,  123,  726,   25,
 /*   410 */   573,   23,  138,  695,   49,   81,  693,   83,   84,  691,
 /*   420 */   790,  690,  195,  161,  199,  549,   57,   52,  180,  132,
 /*   430 */   688,  687,  686,  685,  684,  134,  682,  109,  680,  678,
 /*   440 */    44,  676,  674,  136,  204,  202,   58,  822,  200,  198,
 /*   450 */   196,  220,   77,   28,  231,  232,  233,  235,  652,  234,
 /*   460 */   236,  237,  247,  209,  183,   53,  651,  182,  185,  186,
 /*   470 */    64,   67,  155,  650,  643,  189,  193,  689,  558,   94,
 /*   480 */   683,  675,  126,  125,  727,  129,  124,  127,  128,   95,
 /*   490 */   130,    1,  114,  110,  111,  775,    2,   55,   59,  116,
 /*   500 */   112,  113,  115,  575,  106,  163,  197,    5,  580,  107,
 /*   510 */     6,   65,  621,   19,    4,   20,   15,  208,  623,    7,
 /*   520 */   210,  504,  500,  498,  497,  496,  493,  467,  219,   68,
 /*   530 */    45,   71,   73,   22,  534,  533,  531,  488,   54,  486,
 /*   540 */   478,  484,  480,  482,  476,  474,  505,  503,  502,  501,
 /*   550 */   499,  495,  494,   46,  465,  436,  434,  656,  655,  655,
 /*   560 */   655,  655,  655,  655,  655,  655,  655,  655,  655,   97,
 /*   570 */    98,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   211,    1,  262,  262,  208,  209,  211,    1,  262,    9,
 /*    10 */   228,  271,  272,   13,   14,    9,   16,   17,  210,  211,
 /*    20 */    20,   21,  262,    1,   24,   25,   26,   27,   28,  228,
 /*    30 */   248,    9,  272,   33,   34,  262,  262,   37,   38,   39,
 /*    40 */    13,   14,  246,   16,   17,  271,  272,   20,   21,  248,
 /*    50 */   211,   24,   25,   26,   27,   28,  260,  268,  263,  270,
 /*    60 */    33,   34,  211,  211,   37,   38,   39,   45,   46,   47,
 /*    70 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    80 */    13,   14,   60,   16,   17,    5,  247,   20,   21,   78,
 /*    90 */   228,   24,   25,   26,   27,   28,  245,  245,  247,  247,
 /*   100 */    33,   34,  262,  103,   37,   38,   39,   66,  262,   14,
 /*   110 */   248,   16,   17,   33,   34,   20,   21,  246,   99,   24,
 /*   120 */    25,   26,   27,   28,  211,  106,  262,  100,   33,   34,
 /*   130 */    73,  260,   37,   38,   39,   78,  262,   85,   86,   87,
 /*   140 */    88,   89,   90,   91,   92,   93,   94,   95,   96,   97,
 /*   150 */    98,  227,  211,  229,  230,  231,  232,  233,  234,  235,
 /*   160 */   236,  237,  238,  239,  240,  241,   16,   17,  211,  128,
 /*   170 */    20,   21,  131,  132,   24,   25,   26,   27,   28,  266,
 /*   180 */   249,  268,   99,   33,   34,  102,  103,   37,   38,   39,
 /*   190 */     1,    2,  261,   44,    5,  127,    7,  262,    9,  242,
 /*   200 */     1,    2,  134,  135,    5,  264,    7,  211,    9,   60,
 /*   210 */     1,    2,  104,  211,    5,   66,    7,  109,    9,   70,
 /*   220 */    71,   72,   33,   34,   33,   34,   37,  262,   37,   38,
 /*   230 */    39,  215,   33,   34,  218,  262,   37,   25,   26,   27,
 /*   240 */    28,  215,   33,   34,  218,   33,   34,  262,   99,   37,
 /*   250 */    38,   39,   37,   38,   39,  106,   85,   15,   87,   88,
 /*   260 */   211,  116,  117,   92,  268,   94,   95,   96,  215,   98,
 /*   270 */   268,  218,   61,   62,  262,  126,   99,  262,   67,   68,
 /*   280 */    69,  227,  133,  106,  230,  231,   75,  211,   99,  235,
 /*   290 */   100,  237,  238,  239,  245,  241,  247,  107,   99,  110,
 /*   300 */    63,   64,   65,  100,   61,   62,    1,  104,   99,  110,
 /*   310 */    67,   68,   69,  100,  125,   59,  100,  104,  262,  110,
 /*   320 */   104,  245,    2,  247,  125,    5,  100,    7,  262,    9,
 /*   330 */   104,   61,   62,    5,  125,    7,  262,   67,   68,   69,
 /*   340 */   100,    5,   37,    7,  104,  104,  100,  105,  104,    5,
 /*   350 */   104,    7,  100,   33,   34,   99,  104,  100,  129,  130,
 /*   360 */   262,  104,  129,  130,  123,  121,  262,    5,  248,    7,
 /*   370 */    73,   74,   61,   62,  248,  243,  243,  243,  243,  243,
 /*   380 */   243,  211,  244,  243,  211,  246,  211,  211,  269,  211,
 /*   390 */   269,  250,  211,  211,  211,  211,  211,  211,  211,  211,
 /*   400 */   106,  246,   59,  211,  211,  211,  211,  211,  211,  211,
 /*   410 */   110,  211,  211,  211,  122,  211,  211,  211,  211,  211,
 /*   420 */   259,  211,  265,  265,  265,  105,  212,  120,  211,  211,
 /*   430 */   211,  211,  211,  211,  211,  211,  211,  258,  211,  211,
 /*   440 */   119,  211,  211,  211,  114,  118,  212,  212,  113,  112,
 /*   450 */   111,   76,   84,  124,   83,   49,   80,   53,    5,   82,
 /*   460 */    81,   79,   76,  212,    5,  212,    5,  136,  136,    5,
 /*   470 */   216,  216,  212,    5,   86,  127,  107,  212,  100,  213,
 /*   480 */   212,  212,  220,  224,  226,  222,  225,  223,  221,  213,
 /*   490 */   219,  217,  253,  257,  256,  246,  214,  108,  104,  251,
 /*   500 */   255,  254,  252,  100,   99,    1,   99,  115,  100,   99,
 /*   510 */   115,   73,  100,  104,   99,  104,   99,  101,  105,   99,
 /*   520 */   101,    9,    5,    5,    5,    5,    5,   77,   15,   73,
 /*   530 */    16,  130,  130,  104,    5,    5,  100,    5,   99,    5,
 /*   540 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   550 */     5,    5,    5,  104,   77,   59,   58,    0,  273,  273,
 /*   560 */   273,  273,  273,  273,  273,  273,  273,  273,  273,   21,
 /*   570 */    21,  273,  273,  273,  273,  273,  273,  273,  273,  273,
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
 /*   750 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   760 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   770 */   273,  273,  273,  273,  273,  273,  273,  273,
};
#define YY_SHIFT_COUNT    (252)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (557)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   149,   52,  171,  189,  209,    6,    6,    6,    6,    6,
 /*    10 */     6,    0,   22,  209,  320,  320,  320,   19,    6,    6,
 /*    20 */     6,    6,    6,   57,   11,   11,  571,  199,  209,  209,
 /*    30 */   209,  209,  209,  209,  209,  209,  209,  209,  209,  209,
 /*    40 */   209,  209,  209,  209,  209,  320,  320,   80,   80,   80,
 /*    50 */    80,   80,   80,   83,   80,  177,    6,    6,    6,    6,
 /*    60 */   145,  145,  108,    6,    6,    6,    6,    6,    6,    6,
 /*    70 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*    80 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*    90 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   100 */     6,    6,  294,  343,  343,  300,  300,  300,  343,  307,
 /*   110 */   292,  321,  330,  327,  335,  337,  339,  329,  294,  343,
 /*   120 */   343,  375,  375,  343,  368,  371,  406,  376,  377,  404,
 /*   130 */   379,  382,  343,  386,  343,  386,  343,  571,  571,   27,
 /*   140 */    67,   67,   67,   95,  150,  212,  212,  212,  211,  191,
 /*   150 */   191,  191,  191,  243,  270,   41,   68,  215,  215,  237,
 /*   160 */   190,  203,  213,  216,  226,  240,  328,  336,  305,  256,
 /*   170 */   242,  241,  244,  246,  252,  257,  229,  233,  344,  362,
 /*   180 */   297,  311,  453,  331,  459,  461,  332,  464,  468,  388,
 /*   190 */   348,  369,  378,  389,  394,  403,  405,  504,  407,  408,
 /*   200 */   410,  409,  392,  411,  395,  412,  415,  413,  417,  416,
 /*   210 */   420,  419,  438,  512,  517,  518,  519,  520,  521,  450,
 /*   220 */   513,  456,  514,  401,  402,  429,  529,  530,  436,  439,
 /*   230 */   429,  532,  534,  535,  536,  537,  538,  539,  540,  541,
 /*   240 */   542,  543,  544,  545,  546,  547,  449,  477,  548,  549,
 /*   250 */   496,  498,  557,
};
#define YY_REDUCE_COUNT (138)
#define YY_REDUCE_MIN   (-260)
#define YY_REDUCE_MAX   (282)
static const short yy_reduce_ofst[] = {
 /*     0 */  -204,  -76,   54, -260, -226, -211,  -87, -149, -148,   49,
 /*    10 */    76, -205, -192, -240, -218, -199, -138, -129,  -59,   -4,
 /*    20 */     2,  -43, -161,   16,   26,   53,  -69, -259, -254, -227,
 /*    30 */  -160, -154, -136, -126,  -65,  -35,  -27,  -15,   12,   15,
 /*    40 */    56,   66,   74,   98,  104,  120,  126,  132,  133,  134,
 /*    50 */   135,  136,  137,  138,  140,  139,  170,  173,  175,  176,
 /*    60 */   119,  121,  141,  178,  181,  182,  183,  184,  185,  186,
 /*    70 */   187,  188,  192,  193,  194,  195,  196,  197,  198,  200,
 /*    80 */   201,  202,  204,  205,  206,  207,  208,  210,  217,  218,
 /*    90 */   219,  220,  221,  222,  223,  224,  225,  227,  228,  230,
 /*   100 */   231,  232,  155,  214,  234,  157,  158,  159,  235,  161,
 /*   110 */   179,  236,  238,  245,  247,  239,  250,  248,  249,  251,
 /*   120 */   253,  254,  255,  260,  258,  261,  259,  262,  264,  267,
 /*   130 */   263,  271,  265,  266,  268,  276,  269,  274,  282,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   653,  707,  696,  878,  878,  653,  653,  653,  653,  653,
 /*    10 */   653,  803,  671,  878,  653,  653,  653,  653,  653,  653,
 /*    20 */   653,  653,  653,  709,  709,  709,  798,  653,  653,  653,
 /*    30 */   653,  653,  653,  653,  653,  653,  653,  653,  653,  653,
 /*    40 */   653,  653,  653,  653,  653,  653,  653,  653,  653,  653,
 /*    50 */   653,  653,  653,  653,  653,  653,  653,  805,  807,  653,
 /*    60 */   825,  825,  796,  653,  653,  653,  653,  653,  653,  653,
 /*    70 */   653,  653,  653,  653,  653,  653,  653,  653,  653,  653,
 /*    80 */   653,  694,  653,  692,  653,  653,  653,  653,  653,  653,
 /*    90 */   653,  653,  653,  653,  653,  653,  681,  653,  653,  653,
 /*   100 */   653,  653,  653,  673,  673,  653,  653,  653,  673,  832,
 /*   110 */   836,  830,  818,  826,  817,  813,  812,  840,  653,  673,
 /*   120 */   673,  704,  704,  673,  725,  723,  721,  713,  719,  715,
 /*   130 */   717,  711,  673,  702,  673,  702,  673,  742,  757,  653,
 /*   140 */   841,  877,  831,  867,  866,  873,  865,  864,  653,  860,
 /*   150 */   861,  863,  862,  653,  653,  653,  653,  869,  868,  653,
 /*   160 */   653,  653,  653,  653,  653,  653,  653,  653,  653,  843,
 /*   170 */   653,  837,  833,  653,  653,  653,  653,  653,  653,  653,
 /*   180 */   653,  653,  653,  653,  653,  653,  653,  653,  653,  653,
 /*   190 */   653,  795,  653,  653,  804,  653,  653,  653,  653,  653,
 /*   200 */   653,  827,  653,  819,  653,  653,  653,  653,  653,  653,
 /*   210 */   653,  772,  653,  653,  653,  653,  653,  653,  653,  653,
 /*   220 */   653,  653,  653,  653,  653,  882,  653,  653,  653,  766,
 /*   230 */   880,  653,  653,  653,  653,  653,  653,  653,  653,  653,
 /*   240 */   653,  653,  653,  653,  653,  653,  728,  653,  679,  677,
 /*   250 */   653,  669,  653,
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
 /* 115 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 116 */ "create_table_args ::= LP columnlist RP",
 /* 117 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 118 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 119 */ "create_table_args ::= AS select",
 /* 120 */ "columnlist ::= columnlist COMMA column",
 /* 121 */ "columnlist ::= column",
 /* 122 */ "column ::= ids typename",
 /* 123 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 124 */ "tagitemlist ::= tagitem",
 /* 125 */ "tagitem ::= INTEGER",
 /* 126 */ "tagitem ::= FLOAT",
 /* 127 */ "tagitem ::= STRING",
 /* 128 */ "tagitem ::= BOOL",
 /* 129 */ "tagitem ::= NULL",
 /* 130 */ "tagitem ::= MINUS INTEGER",
 /* 131 */ "tagitem ::= MINUS FLOAT",
 /* 132 */ "tagitem ::= PLUS INTEGER",
 /* 133 */ "tagitem ::= PLUS FLOAT",
 /* 134 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 135 */ "union ::= select",
 /* 136 */ "union ::= LP union RP",
 /* 137 */ "union ::= union UNION ALL select",
 /* 138 */ "union ::= union UNION ALL LP select RP",
 /* 139 */ "cmd ::= union",
 /* 140 */ "select ::= SELECT selcollist",
 /* 141 */ "sclp ::= selcollist COMMA",
 /* 142 */ "sclp ::=",
 /* 143 */ "selcollist ::= sclp expr as",
 /* 144 */ "selcollist ::= sclp STAR",
 /* 145 */ "as ::= AS ids",
 /* 146 */ "as ::= ids",
 /* 147 */ "as ::=",
 /* 148 */ "from ::= FROM tablelist",
 /* 149 */ "tablelist ::= ids cpxName",
 /* 150 */ "tablelist ::= ids cpxName ids",
 /* 151 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 152 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 153 */ "tmvar ::= VARIABLE",
 /* 154 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 155 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 156 */ "interval_opt ::=",
 /* 157 */ "fill_opt ::=",
 /* 158 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 159 */ "fill_opt ::= FILL LP ID RP",
 /* 160 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 161 */ "sliding_opt ::=",
 /* 162 */ "orderby_opt ::=",
 /* 163 */ "orderby_opt ::= ORDER BY sortlist",
 /* 164 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 165 */ "sortlist ::= item sortorder",
 /* 166 */ "item ::= ids cpxName",
 /* 167 */ "sortorder ::= ASC",
 /* 168 */ "sortorder ::= DESC",
 /* 169 */ "sortorder ::=",
 /* 170 */ "groupby_opt ::=",
 /* 171 */ "groupby_opt ::= GROUP BY grouplist",
 /* 172 */ "grouplist ::= grouplist COMMA item",
 /* 173 */ "grouplist ::= item",
 /* 174 */ "having_opt ::=",
 /* 175 */ "having_opt ::= HAVING expr",
 /* 176 */ "limit_opt ::=",
 /* 177 */ "limit_opt ::= LIMIT signed",
 /* 178 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 179 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 180 */ "slimit_opt ::=",
 /* 181 */ "slimit_opt ::= SLIMIT signed",
 /* 182 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 183 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 184 */ "where_opt ::=",
 /* 185 */ "where_opt ::= WHERE expr",
 /* 186 */ "expr ::= LP expr RP",
 /* 187 */ "expr ::= ID",
 /* 188 */ "expr ::= ID DOT ID",
 /* 189 */ "expr ::= ID DOT STAR",
 /* 190 */ "expr ::= INTEGER",
 /* 191 */ "expr ::= MINUS INTEGER",
 /* 192 */ "expr ::= PLUS INTEGER",
 /* 193 */ "expr ::= FLOAT",
 /* 194 */ "expr ::= MINUS FLOAT",
 /* 195 */ "expr ::= PLUS FLOAT",
 /* 196 */ "expr ::= STRING",
 /* 197 */ "expr ::= NOW",
 /* 198 */ "expr ::= VARIABLE",
 /* 199 */ "expr ::= BOOL",
 /* 200 */ "expr ::= ID LP exprlist RP",
 /* 201 */ "expr ::= ID LP STAR RP",
 /* 202 */ "expr ::= expr IS NULL",
 /* 203 */ "expr ::= expr IS NOT NULL",
 /* 204 */ "expr ::= expr LT expr",
 /* 205 */ "expr ::= expr GT expr",
 /* 206 */ "expr ::= expr LE expr",
 /* 207 */ "expr ::= expr GE expr",
 /* 208 */ "expr ::= expr NE expr",
 /* 209 */ "expr ::= expr EQ expr",
 /* 210 */ "expr ::= expr AND expr",
 /* 211 */ "expr ::= expr OR expr",
 /* 212 */ "expr ::= expr PLUS expr",
 /* 213 */ "expr ::= expr MINUS expr",
 /* 214 */ "expr ::= expr STAR expr",
 /* 215 */ "expr ::= expr SLASH expr",
 /* 216 */ "expr ::= expr REM expr",
 /* 217 */ "expr ::= expr LIKE expr",
 /* 218 */ "expr ::= expr IN LP exprlist RP",
 /* 219 */ "exprlist ::= exprlist COMMA expritem",
 /* 220 */ "exprlist ::= expritem",
 /* 221 */ "expritem ::= expr",
 /* 222 */ "expritem ::=",
 /* 223 */ "cmd ::= RESET QUERY CACHE",
 /* 224 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 225 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 226 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 227 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 228 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 229 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 230 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 231 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 232 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 245: /* columnlist */
    case 253: /* fill_opt */
    case 255: /* groupby_opt */
    case 256: /* orderby_opt */
    case 266: /* sortlist */
    case 270: /* grouplist */
{
taosArrayDestroy((yypminor->yy165));
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
  {  209,   -6 }, /* (115) cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
  {  244,   -3 }, /* (116) create_table_args ::= LP columnlist RP */
  {  244,   -7 }, /* (117) create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
  {  244,   -7 }, /* (118) create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
  {  244,   -2 }, /* (119) create_table_args ::= AS select */
  {  245,   -3 }, /* (120) columnlist ::= columnlist COMMA column */
  {  245,   -1 }, /* (121) columnlist ::= column */
  {  247,   -2 }, /* (122) column ::= ids typename */
  {  228,   -3 }, /* (123) tagitemlist ::= tagitemlist COMMA tagitem */
  {  228,   -1 }, /* (124) tagitemlist ::= tagitem */
  {  248,   -1 }, /* (125) tagitem ::= INTEGER */
  {  248,   -1 }, /* (126) tagitem ::= FLOAT */
  {  248,   -1 }, /* (127) tagitem ::= STRING */
  {  248,   -1 }, /* (128) tagitem ::= BOOL */
  {  248,   -1 }, /* (129) tagitem ::= NULL */
  {  248,   -2 }, /* (130) tagitem ::= MINUS INTEGER */
  {  248,   -2 }, /* (131) tagitem ::= MINUS FLOAT */
  {  248,   -2 }, /* (132) tagitem ::= PLUS INTEGER */
  {  248,   -2 }, /* (133) tagitem ::= PLUS FLOAT */
  {  246,  -12 }, /* (134) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  260,   -1 }, /* (135) union ::= select */
  {  260,   -3 }, /* (136) union ::= LP union RP */
  {  260,   -4 }, /* (137) union ::= union UNION ALL select */
  {  260,   -6 }, /* (138) union ::= union UNION ALL LP select RP */
  {  209,   -1 }, /* (139) cmd ::= union */
  {  246,   -2 }, /* (140) select ::= SELECT selcollist */
  {  261,   -2 }, /* (141) sclp ::= selcollist COMMA */
  {  261,    0 }, /* (142) sclp ::= */
  {  249,   -3 }, /* (143) selcollist ::= sclp expr as */
  {  249,   -2 }, /* (144) selcollist ::= sclp STAR */
  {  263,   -2 }, /* (145) as ::= AS ids */
  {  263,   -1 }, /* (146) as ::= ids */
  {  263,    0 }, /* (147) as ::= */
  {  250,   -2 }, /* (148) from ::= FROM tablelist */
  {  264,   -2 }, /* (149) tablelist ::= ids cpxName */
  {  264,   -3 }, /* (150) tablelist ::= ids cpxName ids */
  {  264,   -4 }, /* (151) tablelist ::= tablelist COMMA ids cpxName */
  {  264,   -5 }, /* (152) tablelist ::= tablelist COMMA ids cpxName ids */
  {  265,   -1 }, /* (153) tmvar ::= VARIABLE */
  {  252,   -4 }, /* (154) interval_opt ::= INTERVAL LP tmvar RP */
  {  252,   -6 }, /* (155) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  252,    0 }, /* (156) interval_opt ::= */
  {  253,    0 }, /* (157) fill_opt ::= */
  {  253,   -6 }, /* (158) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  253,   -4 }, /* (159) fill_opt ::= FILL LP ID RP */
  {  254,   -4 }, /* (160) sliding_opt ::= SLIDING LP tmvar RP */
  {  254,    0 }, /* (161) sliding_opt ::= */
  {  256,    0 }, /* (162) orderby_opt ::= */
  {  256,   -3 }, /* (163) orderby_opt ::= ORDER BY sortlist */
  {  266,   -4 }, /* (164) sortlist ::= sortlist COMMA item sortorder */
  {  266,   -2 }, /* (165) sortlist ::= item sortorder */
  {  268,   -2 }, /* (166) item ::= ids cpxName */
  {  269,   -1 }, /* (167) sortorder ::= ASC */
  {  269,   -1 }, /* (168) sortorder ::= DESC */
  {  269,    0 }, /* (169) sortorder ::= */
  {  255,    0 }, /* (170) groupby_opt ::= */
  {  255,   -3 }, /* (171) groupby_opt ::= GROUP BY grouplist */
  {  270,   -3 }, /* (172) grouplist ::= grouplist COMMA item */
  {  270,   -1 }, /* (173) grouplist ::= item */
  {  257,    0 }, /* (174) having_opt ::= */
  {  257,   -2 }, /* (175) having_opt ::= HAVING expr */
  {  259,    0 }, /* (176) limit_opt ::= */
  {  259,   -2 }, /* (177) limit_opt ::= LIMIT signed */
  {  259,   -4 }, /* (178) limit_opt ::= LIMIT signed OFFSET signed */
  {  259,   -4 }, /* (179) limit_opt ::= LIMIT signed COMMA signed */
  {  258,    0 }, /* (180) slimit_opt ::= */
  {  258,   -2 }, /* (181) slimit_opt ::= SLIMIT signed */
  {  258,   -4 }, /* (182) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  258,   -4 }, /* (183) slimit_opt ::= SLIMIT signed COMMA signed */
  {  251,    0 }, /* (184) where_opt ::= */
  {  251,   -2 }, /* (185) where_opt ::= WHERE expr */
  {  262,   -3 }, /* (186) expr ::= LP expr RP */
  {  262,   -1 }, /* (187) expr ::= ID */
  {  262,   -3 }, /* (188) expr ::= ID DOT ID */
  {  262,   -3 }, /* (189) expr ::= ID DOT STAR */
  {  262,   -1 }, /* (190) expr ::= INTEGER */
  {  262,   -2 }, /* (191) expr ::= MINUS INTEGER */
  {  262,   -2 }, /* (192) expr ::= PLUS INTEGER */
  {  262,   -1 }, /* (193) expr ::= FLOAT */
  {  262,   -2 }, /* (194) expr ::= MINUS FLOAT */
  {  262,   -2 }, /* (195) expr ::= PLUS FLOAT */
  {  262,   -1 }, /* (196) expr ::= STRING */
  {  262,   -1 }, /* (197) expr ::= NOW */
  {  262,   -1 }, /* (198) expr ::= VARIABLE */
  {  262,   -1 }, /* (199) expr ::= BOOL */
  {  262,   -4 }, /* (200) expr ::= ID LP exprlist RP */
  {  262,   -4 }, /* (201) expr ::= ID LP STAR RP */
  {  262,   -3 }, /* (202) expr ::= expr IS NULL */
  {  262,   -4 }, /* (203) expr ::= expr IS NOT NULL */
  {  262,   -3 }, /* (204) expr ::= expr LT expr */
  {  262,   -3 }, /* (205) expr ::= expr GT expr */
  {  262,   -3 }, /* (206) expr ::= expr LE expr */
  {  262,   -3 }, /* (207) expr ::= expr GE expr */
  {  262,   -3 }, /* (208) expr ::= expr NE expr */
  {  262,   -3 }, /* (209) expr ::= expr EQ expr */
  {  262,   -3 }, /* (210) expr ::= expr AND expr */
  {  262,   -3 }, /* (211) expr ::= expr OR expr */
  {  262,   -3 }, /* (212) expr ::= expr PLUS expr */
  {  262,   -3 }, /* (213) expr ::= expr MINUS expr */
  {  262,   -3 }, /* (214) expr ::= expr STAR expr */
  {  262,   -3 }, /* (215) expr ::= expr SLASH expr */
  {  262,   -3 }, /* (216) expr ::= expr REM expr */
  {  262,   -3 }, /* (217) expr ::= expr LIKE expr */
  {  262,   -5 }, /* (218) expr ::= expr IN LP exprlist RP */
  {  271,   -3 }, /* (219) exprlist ::= exprlist COMMA expritem */
  {  271,   -1 }, /* (220) exprlist ::= expritem */
  {  272,   -1 }, /* (221) expritem ::= expr */
  {  272,    0 }, /* (222) expritem ::= */
  {  209,   -3 }, /* (223) cmd ::= RESET QUERY CACHE */
  {  209,   -7 }, /* (224) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  209,   -7 }, /* (225) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  209,   -7 }, /* (226) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  209,   -7 }, /* (227) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  209,   -8 }, /* (228) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  209,   -9 }, /* (229) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  209,   -3 }, /* (230) cmd ::= KILL CONNECTION INTEGER */
  {  209,   -5 }, /* (231) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  209,   -5 }, /* (232) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &t);}
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy149);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 43: /* ids ::= ID */
      case 44: /* ids ::= STRING */ yytestcase(yyruleno==44);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 45: /* ifexists ::= IF EXISTS */
{yymsp[-1].minor.yy0.n = 1;}
        break;
      case 46: /* ifexists ::= */
      case 48: /* ifnotexists ::= */ yytestcase(yyruleno==48);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 47: /* ifnotexists ::= IF NOT EXISTS */
{yymsp[-2].minor.yy0.n = 1;}
        break;
      case 49: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 50: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &yymsp[-2].minor.yy0);}
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
{yymsp[1].minor.yy0.n = 0;   }
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
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 71: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
      case 72: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy165 = yymsp[0].minor.yy165; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy268);}
        break;
      case 87: /* db_optr ::= db_optr cache */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 88: /* db_optr ::= db_optr replica */
      case 102: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==102);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 89: /* db_optr ::= db_optr quorum */
      case 103: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==103);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 90: /* db_optr ::= db_optr days */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 91: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 92: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 93: /* db_optr ::= db_optr blocks */
      case 105: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==105);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 94: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 95: /* db_optr ::= db_optr wal */
      case 107: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==107);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 96: /* db_optr ::= db_optr fsync */
      case 108: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==108);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 97: /* db_optr ::= db_optr comp */
      case 106: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==106);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 98: /* db_optr ::= db_optr prec */
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 99: /* db_optr ::= db_optr keep */
      case 104: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==104);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.keep = yymsp[0].minor.yy165; }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 100: /* db_optr ::= db_optr update */
      case 109: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==109);
{ yylhsminor.yy268 = yymsp[-1].minor.yy268; yylhsminor.yy268.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy268 = yylhsminor.yy268;
        break;
      case 101: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy268);}
        break;
      case 110: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSQLSetColumnType (&yylhsminor.yy223, &yymsp[0].minor.yy0); 
}
  yymsp[0].minor.yy223 = yylhsminor.yy223;
        break;
      case 111: /* typename ::= ids LP signed RP */
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
      case 112: /* signed ::= INTEGER */
{ yylhsminor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy207 = yylhsminor.yy207;
        break;
      case 113: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 114: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy207 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 115: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 116: /* create_table_args ::= LP columnlist RP */
{
    yymsp[-2].minor.yy470 = tSetCreateSQLElems(yymsp[-1].minor.yy165, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yymsp[-2].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 117: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yymsp[-6].minor.yy470 = tSetCreateSQLElems(yymsp[-5].minor.yy165, yymsp[-1].minor.yy165, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 118: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy470 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy165, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yymsp[-6].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 119: /* create_table_args ::= AS select */
{
    yymsp[-1].minor.yy470 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy414, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 120: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy165, &yymsp[0].minor.yy223); yylhsminor.yy165 = yymsp[-2].minor.yy165;  }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 121: /* columnlist ::= column */
{yylhsminor.yy165 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy165, &yymsp[0].minor.yy223);}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 122: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yylhsminor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
  yymsp[-1].minor.yy223 = yylhsminor.yy223;
        break;
      case 123: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy165 = tVariantListAppend(yymsp[-2].minor.yy165, &yymsp[0].minor.yy134, -1);    }
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 124: /* tagitemlist ::= tagitem */
{ yylhsminor.yy165 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1); }
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 125: /* tagitem ::= INTEGER */
      case 126: /* tagitem ::= FLOAT */ yytestcase(yyruleno==126);
      case 127: /* tagitem ::= STRING */ yytestcase(yyruleno==127);
      case 128: /* tagitem ::= BOOL */ yytestcase(yyruleno==128);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 129: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy134, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 130: /* tagitem ::= MINUS INTEGER */
      case 131: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==131);
      case 132: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==132);
      case 133: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==133);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy134, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 134: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy414 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy290, yymsp[-9].minor.yy165, yymsp[-8].minor.yy64, yymsp[-4].minor.yy165, yymsp[-3].minor.yy165, &yymsp[-7].minor.yy532, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy165, &yymsp[0].minor.yy216, &yymsp[-1].minor.yy216);
}
  yymsp[-11].minor.yy414 = yylhsminor.yy414;
        break;
      case 135: /* union ::= select */
{ yylhsminor.yy231 = setSubclause(NULL, yymsp[0].minor.yy414); }
  yymsp[0].minor.yy231 = yylhsminor.yy231;
        break;
      case 136: /* union ::= LP union RP */
{ yymsp[-2].minor.yy231 = yymsp[-1].minor.yy231; }
        break;
      case 137: /* union ::= union UNION ALL select */
{ yylhsminor.yy231 = appendSelectClause(yymsp[-3].minor.yy231, yymsp[0].minor.yy414); }
  yymsp[-3].minor.yy231 = yylhsminor.yy231;
        break;
      case 138: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy231 = appendSelectClause(yymsp[-5].minor.yy231, yymsp[-1].minor.yy414); }
  yymsp[-5].minor.yy231 = yylhsminor.yy231;
        break;
      case 139: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy231, NULL, TSDB_SQL_SELECT); }
        break;
      case 140: /* select ::= SELECT selcollist */
{
  yylhsminor.yy414 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy290, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy414 = yylhsminor.yy414;
        break;
      case 141: /* sclp ::= selcollist COMMA */
{yylhsminor.yy290 = yymsp[-1].minor.yy290;}
  yymsp[-1].minor.yy290 = yylhsminor.yy290;
        break;
      case 142: /* sclp ::= */
{yymsp[1].minor.yy290 = 0;}
        break;
      case 143: /* selcollist ::= sclp expr as */
{
   yylhsminor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290, yymsp[-1].minor.yy64, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-2].minor.yy290 = yylhsminor.yy290;
        break;
      case 144: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy290 = tSQLExprListAppend(yymsp[-1].minor.yy290, pNode, 0);
}
  yymsp[-1].minor.yy290 = yylhsminor.yy290;
        break;
      case 145: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 146: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 147: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 148: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy165 = yymsp[0].minor.yy165;}
        break;
      case 149: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy165 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy165 = tVariantListAppendToken(yylhsminor.yy165, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 150: /* tablelist ::= ids cpxName ids */
{
   toTSDBType(yymsp[-2].minor.yy0.type);
   toTSDBType(yymsp[0].minor.yy0.type);
   yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
   yylhsminor.yy165 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
   yylhsminor.yy165 = tVariantListAppendToken(yylhsminor.yy165, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 151: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy165 = tVariantListAppendToken(yymsp[-3].minor.yy165, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy165 = tVariantListAppendToken(yylhsminor.yy165, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy165 = yylhsminor.yy165;
        break;
      case 152: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
   toTSDBType(yymsp[-2].minor.yy0.type);
   toTSDBType(yymsp[0].minor.yy0.type);
   yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
   yylhsminor.yy165 = tVariantListAppendToken(yymsp[-4].minor.yy165, &yymsp[-2].minor.yy0, -1);
   yylhsminor.yy165 = tVariantListAppendToken(yylhsminor.yy165, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy165 = yylhsminor.yy165;
        break;
      case 153: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 154: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy532.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy532.offset.n = 0; yymsp[-3].minor.yy532.offset.z = NULL; yymsp[-3].minor.yy532.offset.type = 0;}
        break;
      case 155: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy532.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy532.offset = yymsp[-1].minor.yy0;}
        break;
      case 156: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy532, 0, sizeof(yymsp[1].minor.yy532));}
        break;
      case 157: /* fill_opt ::= */
{yymsp[1].minor.yy165 = 0;     }
        break;
      case 158: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy165, &A, -1, 0);
    yymsp[-5].minor.yy165 = yymsp[-1].minor.yy165;
}
        break;
      case 159: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy165 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 160: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 161: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 162: /* orderby_opt ::= */
      case 170: /* groupby_opt ::= */ yytestcase(yyruleno==170);
{yymsp[1].minor.yy165 = 0;}
        break;
      case 163: /* orderby_opt ::= ORDER BY sortlist */
      case 171: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==171);
{yymsp[-2].minor.yy165 = yymsp[0].minor.yy165;}
        break;
      case 164: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy165 = tVariantListAppend(yymsp[-3].minor.yy165, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
  yymsp[-3].minor.yy165 = yylhsminor.yy165;
        break;
      case 165: /* sortlist ::= item sortorder */
{
  yylhsminor.yy165 = tVariantListAppend(NULL, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
  yymsp[-1].minor.yy165 = yylhsminor.yy165;
        break;
      case 166: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy134, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 167: /* sortorder ::= ASC */
{yymsp[0].minor.yy46 = TSDB_ORDER_ASC; }
        break;
      case 168: /* sortorder ::= DESC */
{yymsp[0].minor.yy46 = TSDB_ORDER_DESC;}
        break;
      case 169: /* sortorder ::= */
{yymsp[1].minor.yy46 = TSDB_ORDER_ASC;}
        break;
      case 172: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy165 = tVariantListAppend(yymsp[-2].minor.yy165, &yymsp[0].minor.yy134, -1);
}
  yymsp[-2].minor.yy165 = yylhsminor.yy165;
        break;
      case 173: /* grouplist ::= item */
{
  yylhsminor.yy165 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1);
}
  yymsp[0].minor.yy165 = yylhsminor.yy165;
        break;
      case 174: /* having_opt ::= */
      case 184: /* where_opt ::= */ yytestcase(yyruleno==184);
      case 222: /* expritem ::= */ yytestcase(yyruleno==222);
{yymsp[1].minor.yy64 = 0;}
        break;
      case 175: /* having_opt ::= HAVING expr */
      case 185: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==185);
{yymsp[-1].minor.yy64 = yymsp[0].minor.yy64;}
        break;
      case 176: /* limit_opt ::= */
      case 180: /* slimit_opt ::= */ yytestcase(yyruleno==180);
{yymsp[1].minor.yy216.limit = -1; yymsp[1].minor.yy216.offset = 0;}
        break;
      case 177: /* limit_opt ::= LIMIT signed */
{yymsp[-1].minor.yy216.limit = yymsp[0].minor.yy207;  yymsp[-1].minor.yy216.offset = 0;}
        break;
      case 178: /* limit_opt ::= LIMIT signed OFFSET signed */
{yymsp[-3].minor.yy216.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy216.offset = yymsp[0].minor.yy207;}
        break;
      case 179: /* limit_opt ::= LIMIT signed COMMA signed */
{yymsp[-3].minor.yy216.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy216.offset = yymsp[-2].minor.yy207;}
        break;
      case 181: /* slimit_opt ::= SLIMIT signed */
{yymsp[-1].minor.yy216.limit = yymsp[0].minor.yy207;  yymsp[-1].minor.yy216.offset = 0;}
        break;
      case 182: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy216.limit = yymsp[-2].minor.yy207;  yymsp[-3].minor.yy216.offset = yymsp[0].minor.yy207;}
        break;
      case 183: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy216.limit = yymsp[0].minor.yy207;  yymsp[-3].minor.yy216.offset = yymsp[-2].minor.yy207;}
        break;
      case 186: /* expr ::= LP expr RP */
{yymsp[-2].minor.yy64 = yymsp[-1].minor.yy64; }
        break;
      case 187: /* expr ::= ID */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 188: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 189: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 190: /* expr ::= INTEGER */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 191: /* expr ::= MINUS INTEGER */
      case 192: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==192);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy64 = yylhsminor.yy64;
        break;
      case 193: /* expr ::= FLOAT */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 194: /* expr ::= MINUS FLOAT */
      case 195: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==195);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy64 = yylhsminor.yy64;
        break;
      case 196: /* expr ::= STRING */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 197: /* expr ::= NOW */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 198: /* expr ::= VARIABLE */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 199: /* expr ::= BOOL */
{yylhsminor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 200: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy64 = tSQLExprCreateFunction(yymsp[-1].minor.yy290, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy64 = yylhsminor.yy64;
        break;
      case 201: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy64 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy64 = yylhsminor.yy64;
        break;
      case 202: /* expr ::= expr IS NULL */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 203: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-3].minor.yy64, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy64 = yylhsminor.yy64;
        break;
      case 204: /* expr ::= expr LT expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LT);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 205: /* expr ::= expr GT expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GT);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 206: /* expr ::= expr LE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 207: /* expr ::= expr GE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 208: /* expr ::= expr NE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_NE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 209: /* expr ::= expr EQ expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_EQ);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 210: /* expr ::= expr AND expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_AND);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 211: /* expr ::= expr OR expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_OR); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 212: /* expr ::= expr PLUS expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_PLUS);  }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 213: /* expr ::= expr MINUS expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_MINUS); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 214: /* expr ::= expr STAR expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_STAR);  }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 215: /* expr ::= expr SLASH expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_DIVIDE);}
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 216: /* expr ::= expr REM expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_REM);   }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 217: /* expr ::= expr LIKE expr */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LIKE);  }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 218: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy64 = tSQLExprCreate(yymsp[-4].minor.yy64, (tSQLExpr*)yymsp[-1].minor.yy290, TK_IN); }
  yymsp[-4].minor.yy64 = yylhsminor.yy64;
        break;
      case 219: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290,yymsp[0].minor.yy64,0);}
  yymsp[-2].minor.yy290 = yylhsminor.yy290;
        break;
      case 220: /* exprlist ::= expritem */
{yylhsminor.yy290 = tSQLExprListAppend(0,yymsp[0].minor.yy64,0);}
  yymsp[0].minor.yy290 = yylhsminor.yy290;
        break;
      case 221: /* expritem ::= expr */
{yylhsminor.yy64 = yymsp[0].minor.yy64;}
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 223: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 224: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 225: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 226: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy165, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 227: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 228: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 229: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 230: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 231: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 232: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
