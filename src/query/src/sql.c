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
#include <assert.h>
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
**    ParseARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    ParseCTX_*         As ParseARG_ except for %extra_context
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
#define YYNOCODE 266
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSubclauseInfo* yy21;
  TAOS_FIELD yy27;
  SCreateDbInfo yy114;
  tSQLExpr* yy134;
  tSQLExprList* yy178;
  SCreateAcctInfo yy183;
  SCreateTableSQL* yy190;
  SCreatedTableInfo yy192;
  SArray* yy193;
  SQuerySQL* yy288;
  int yy312;
  SIntervalVal yy392;
  tVariant yy442;
  SSessionWindowVal yy447;
  int64_t yy473;
  SLimitVal yy482;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_PARAM ,pInfo
#define ParseARG_FETCH SSqlInfo* pInfo=yypParser->pInfo;
#define ParseARG_STORE yypParser->pInfo=pInfo;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYFALLBACK 1
#define YYNSTATE             319
#define YYNRULE              268
#define YYNRULE_WITH_ACTION  268
#define YYNTOKEN             191
#define YY_MAX_SHIFT         318
#define YY_MIN_SHIFTREDUCE   511
#define YY_MAX_SHIFTREDUCE   778
#define YY_ERROR_ACTION      779
#define YY_ACCEPT_ACTION     780
#define YY_NO_ACTION         781
#define YY_MIN_REDUCE        782
#define YY_MAX_REDUCE        1049
/************* End control #defines *******************************************/
#define YY_NLOOKAHEAD ((int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])))

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
#define YY_ACTTAB_COUNT (675)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   953,  560,  184,  560,  205,  316,   79,   17,   70,  561,
 /*    10 */    80,  561, 1031,   48,   49,  142,   52,   53,  135,   72,
 /*    20 */   217,   42,  184,   51,  264,   56,   54,   58,   55,  265,
 /*    30 */    31,  212, 1032,   47,   46,  182,  184,   45,   44,   43,
 /*    40 */   916,  914,  915,   29,  918,  211, 1032,  512,  513,  514,
 /*    50 */   515,  516,  517,  518,  519,  520,  521,  522,  523,  524,
 /*    60 */   525,  317,  950,  209,  234,   48,   49,   31,   52,   53,
 /*    70 */   142,  207,  217,   42,  928,   51,  264,   56,   54,   58,
 /*    80 */    55,  249,  984,   71,  259,   47,   46,  917,  931,   45,
 /*    90 */    44,   43,   48,   49,  925,   52,   53,   31,  302,  217,
 /*   100 */    42,  560,   51,  264,   56,   54,   58,   55,  220,  561,
 /*   110 */    31,  928,   47,   46,   24,  229,   45,   44,   43,   48,
 /*   120 */    50,   37,   52,   53,  841,  222,  217,   42,  742,   51,
 /*   130 */   264,   56,   54,   58,   55,  261,  223,   77,  221,   47,
 /*   140 */    46,  928,  224,   45,   44,   43,   49,  282,   52,   53,
 /*   150 */   931,  285,  217,   42,  928,   51,  264,   56,   54,   58,
 /*   160 */    55,  726,  315,  314,  127,   47,   46,  931,  282,   45,
 /*   170 */    44,   43,   23,  280,  311,  310,  279,  278,  277,  309,
 /*   180 */   276,  308,  307,  306,  275,  305,  304,  891,   83,  879,
 /*   190 */   880,  881,  882,  883,  884,  885,  886,  887,  888,  889,
 /*   200 */   890,  892,  893,   52,   53,   18,  188,  217,   42,  312,
 /*   210 */    51,  264,   56,   54,   58,   55, 1028,  228,    1,  157,
 /*   220 */    47,   46,  919,  192,   45,   44,   43,  216,  739,  194,
 /*   230 */   732,  730,  735,  733,  931,  736,  118,  117,  193,  899,
 /*   240 */   216,  739,  897,  898,  730,   31,  733,  900,  736,  902,
 /*   250 */   903,  901,  226,  904,  905,   45,   44,   43,  153,  213,
 /*   260 */   214,   47,   46,  263, 1027,   45,   44,   43,   23,   24,
 /*   270 */   311,  310,  213,  214, 1026,  309,   37,  308,  307,  306,
 /*   280 */   201,  305,  304,   12,  780,  318,  286,   82,  230,  928,
 /*   290 */   142,  289,  288,  237,   56,   54,   58,   55,  243,  942,
 /*   300 */   241,  240,   47,   46,  640,  200,   45,   44,   43,  202,
 /*   310 */     5,  159,    3,  170,  206,  142,   34,  158,   87,   92,
 /*   320 */    85,   91,   31,  227,  664,  680,  284,  661,   31,  662,
 /*   330 */   942,  663,  292,  291,   57,  105,  829,  707,  708,  229,
 /*   340 */    31,  169,  302,  103,  108,  244,  738,   57,  929,   97,
 /*   350 */   107,  133,  113,  116,  106,  231,  232,  983,   37,  738,
 /*   360 */   110,  677,  737,  290,  215,  728,  928,   25,  838,  294,
 /*   370 */   177,  173,  928,  169,  186,  737,  175,  172,  122,  121,
 /*   380 */   120,  119,   78,  247,  927,  830,  672,   32,  692,   62,
 /*   390 */   169,  698,  137,  246,  699,   61,  759,   65,   20,  740,
 /*   400 */    19,  729,  731,   19,  734,  650,  684,   96,   95,   32,
 /*   410 */    63,    6,  187,  267,  652,  269,   66,   32,   61,   81,
 /*   420 */   651,   68,   28,  639,   61,  270,   14,   13,  189,  102,
 /*   430 */   101,   16,   15,  668,  183,  669,  665,  666,  190,  667,
 /*   440 */   115,  114,  132,  130,  191,  197,  198,  196,  181,  195,
 /*   450 */   185, 1041,  930,  994,  993,  134,  944,  218,  952,  990,
 /*   460 */   989,  219,  293,   40,  959,  961,  136,  140,  976,  975,
 /*   470 */    37,  245,  154,  131,  152,  924,  104,  155,  691,  250,
 /*   480 */   208,  926,  303,  148,  146,  144,  156,  842,  272,  273,
 /*   490 */    59,  274,   38,  252,  179,   35,  283,   67,  257,  262,
 /*   500 */   941,   64,  837, 1046,   93,  143, 1045,  145,  258, 1043,
 /*   510 */   160,  287, 1040,   99, 1039, 1037,  260,  161,  860,   36,
 /*   520 */    33,   39,  180,  826,  109,  824,  111,  112,  822,  821,
 /*   530 */   233,  171,  819,  818,  817,  816,  815,  814,  813,  174,
 /*   540 */   176,  810,  808,  806,  804,  802,  178,  256,  248,   73,
 /*   550 */   251,   74,  253,  254,  977,   41,  295,  296,  297,  298,
 /*   560 */   299,  300,  203,  301,  313,  225,  271,  778,  235,  236,
 /*   570 */   777,  239,  204,   88,  238,  199,   89,  776,  764,  242,
 /*   580 */   246,  674,   69,    8,  138,  266,  820,  693,  164,  123,
 /*   590 */   861,  124,  162,  163,  812,  165,  166,  168,  167,  125,
 /*   600 */   811,  895,  126,  803,    4,    2,  150,  147,   75,  149,
 /*   610 */   696,  151,  139,  210,    9,   76,  255,  907,  700,  141,
 /*   620 */    10,   84,  741,   26,    7,   27,   11,   21,  743,   22,
 /*   630 */    30,  268,   86,  603,   82,  599,  597,  596,  595,  592,
 /*   640 */   564,  281,   32,   94,   60,   90,  642,  641,  638,  587,
 /*   650 */   585,  577,  583,  579,  581,  575,   98,  573,  100,  606,
 /*   660 */   605,  604,  602,  601,  600,  598,  594,  593,   61,  562,
 /*   670 */   529,  527,  128,  782,  129,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   194,    1,  255,    1,  193,  194,  240,  255,  200,    9,
 /*    10 */   200,    9,  265,   13,   14,  194,   16,   17,  194,  253,
 /*    20 */    20,   21,  255,   23,   24,   25,   26,   27,   28,   15,
 /*    30 */   194,  264,  265,   33,   34,  255,  255,   37,   38,   39,
 /*    40 */   232,  231,  232,  233,  234,  264,  265,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  256,  214,   62,   13,   14,  194,   16,   17,
 /*    70 */   194,  235,   20,   21,  238,   23,   24,   25,   26,   27,
 /*    80 */    28,  257,  261,   83,  263,   33,   34,    0,  239,   37,
 /*    90 */    38,   39,   13,   14,  194,   16,   17,  194,   85,   20,
 /*   100 */    21,    1,   23,   24,   25,   26,   27,   28,  235,    9,
 /*   110 */   194,  238,   33,   34,  108,  194,   37,   38,   39,   13,
 /*   120 */    14,  115,   16,   17,  203,  214,   20,   21,  114,   23,
 /*   130 */    24,   25,   26,   27,   28,  259,  236,  261,  235,   33,
 /*   140 */    34,  238,  214,   37,   38,   39,   14,   81,   16,   17,
 /*   150 */   239,  235,   20,   21,  238,   23,   24,   25,   26,   27,
 /*   160 */    28,  109,   65,   66,   67,   33,   34,  239,   81,   37,
 /*   170 */    38,   39,   92,   93,   94,   95,   96,   97,   98,   99,
 /*   180 */   100,  101,  102,  103,  104,  105,  106,  213,  200,  215,
 /*   190 */   216,  217,  218,  219,  220,  221,  222,  223,  224,  225,
 /*   200 */   226,  227,  228,   16,   17,   44,  255,   20,   21,  214,
 /*   210 */    23,   24,   25,   26,   27,   28,  255,   68,  201,  202,
 /*   220 */    33,   34,  234,   62,   37,   38,   39,    1,    2,   68,
 /*   230 */     5,    5,    7,    7,  239,    9,   75,   76,   77,  213,
 /*   240 */     1,    2,  216,  217,    5,  194,    7,  221,    9,  223,
 /*   250 */   224,  225,   68,  227,  228,   37,   38,   39,   83,   33,
 /*   260 */    34,   33,   34,   37,  255,   37,   38,   39,   92,  108,
 /*   270 */    94,   95,   33,   34,  255,   99,  115,  101,  102,  103,
 /*   280 */   255,  105,  106,  108,  191,  192,  235,  112,  139,  238,
 /*   290 */   194,  142,  143,  138,   25,   26,   27,   28,  137,  237,
 /*   300 */   145,  146,   33,   34,    5,  144,   37,   38,   39,  255,
 /*   310 */    63,   64,  197,  198,  252,  194,   69,   70,   71,   72,
 /*   320 */    73,   74,  194,  139,    2,   37,  142,    5,  194,    7,
 /*   330 */   237,    9,   33,   34,  108,   78,  199,  127,  128,  194,
 /*   340 */   194,  204,   85,   63,   64,  252,  120,  108,  203,   69,
 /*   350 */    70,  108,   72,   73,   74,   33,   34,  261,  115,  120,
 /*   360 */    80,  113,  136,  235,   61,    1,  238,  119,  199,  235,
 /*   370 */    63,   64,  238,  204,  255,  136,   69,   70,   71,   72,
 /*   380 */    73,   74,  261,  109,  238,  199,  109,  113,  109,  113,
 /*   390 */   204,  109,  113,  116,  109,  113,  109,  113,  113,  109,
 /*   400 */   113,   37,    5,  113,    7,  109,  118,  140,  141,  113,
 /*   410 */   134,  108,  255,  109,  109,  109,  132,  113,  113,  113,
 /*   420 */   109,  108,  108,  110,  113,  111,  140,  141,  255,  140,
 /*   430 */   141,  140,  141,    5,  255,    7,  114,    5,  255,    7,
 /*   440 */    78,   79,   63,   64,  255,  255,  255,  255,  255,  255,
 /*   450 */   255,  239,  239,  230,  230,  194,  237,  230,  194,  230,
 /*   460 */   230,  230,  230,  254,  194,  194,  194,  194,  262,  262,
 /*   470 */   115,  237,  194,   61,  241,  194,   91,  194,  120,  258,
 /*   480 */   258,  237,  107,  245,  247,  249,  194,  194,  194,  194,
 /*   490 */   130,  194,  194,  258,  194,  194,  194,  131,  258,  125,
 /*   500 */   251,  133,  194,  194,  194,  250,  194,  248,  124,  194,
 /*   510 */   194,  194,  194,  194,  194,  194,  129,  194,  194,  194,
 /*   520 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   530 */   194,  194,  194,  194,  194,  194,  194,  194,  194,  194,
 /*   540 */   194,  194,  194,  194,  194,  194,  194,  123,  195,  195,
 /*   550 */   121,  195,  195,  122,  195,  135,   90,   51,   87,   89,
 /*   560 */    55,   88,  195,   86,   81,  195,  195,    5,  147,    5,
 /*   570 */     5,    5,  195,  200,  147,  195,  200,    5,   93,  138,
 /*   580 */   116,  109,  117,  108,  108,  111,  195,  109,  206,  196,
 /*   590 */   212,  196,  211,  210,  195,  209,  207,  205,  208,  196,
 /*   600 */   195,  229,  196,  195,  197,  201,  243,  246,  113,  244,
 /*   610 */   109,  242,  113,    1,  126,  108,  108,  229,  109,  108,
 /*   620 */   126,   78,  109,  113,  108,  113,  108,  108,  114,  108,
 /*   630 */    84,  111,   83,    9,  112,    5,    5,    5,    5,    5,
 /*   640 */    82,   15,  113,  141,   16,   78,    5,    5,  109,    5,
 /*   650 */     5,    5,    5,    5,    5,    5,  141,    5,  141,    5,
 /*   660 */     5,    5,    5,    5,    5,    5,    5,    5,  113,   82,
 /*   670 */    61,   60,   21,    0,   21,  266,  266,  266,  266,  266,
 /*   680 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   690 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   700 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   710 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   720 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   730 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   740 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   750 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   760 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   770 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   780 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   790 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   800 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   810 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   820 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   830 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   840 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   850 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   860 */   266,  266,  266,  266,  266,  266,
};
#define YY_SHIFT_COUNT    (318)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (673)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   80,   80,  176,  176,   66,  226,  239,  100,  100,
 /*    10 */   100,  100,  100,  100,  100,  100,  100,    0,    2,  239,
 /*    20 */   322,  322,  322,  322,    6,  100,  100,  100,  100,   87,
 /*    30 */   100,  100,  100,  257,   66,   13,   13,  675,  675,  675,
 /*    40 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   322,  322,  299,  299,  299,  299,  299,  299,  299,  243,
 /*    70 */   100,  100,  288,  100,  100,  100,  100,  210,  210,  248,
 /*    80 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*    90 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   100 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   110 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   120 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   130 */   100,  100,  100,  355,  412,  412,  412,  358,  358,  358,
 /*   140 */   412,  358,  412,  366,  368,  360,  374,  387,  384,  424,
 /*   150 */   431,  429,  420,  355,  412,  412,  412,  375,   66,   66,
 /*   160 */   412,  412,  385,  466,  506,  471,  470,  505,  473,  477,
 /*   170 */   375,  412,  483,  483,  412,  483,  412,  483,  412,  675,
 /*   180 */   675,   52,   79,  106,   79,   79,  132,  187,  269,  269,
 /*   190 */   269,  269,  247,  280,  307,  228,  228,  228,  228,  149,
 /*   200 */   155,  218,  218,  175,  184,   97,  277,  274,  279,  282,
 /*   210 */   285,  287,  290,  225,  397,  364,  303,   14,  276,  284,
 /*   220 */   296,  304,  305,  306,  311,  314,  267,  286,  289,  313,
 /*   230 */   291,  428,  432,  362,  379,  562,  421,  564,  565,  427,
 /*   240 */   566,  572,  485,  441,  464,  472,  465,  474,  475,  495,
 /*   250 */   478,  476,  501,  499,  507,  612,  508,  509,  511,  510,
 /*   260 */   488,  512,  494,  513,  516,  514,  518,  474,  519,  520,
 /*   270 */   521,  522,  543,  546,  549,  624,  630,  631,  632,  633,
 /*   280 */   634,  558,  626,  567,  502,  529,  529,  628,  515,  517,
 /*   290 */   529,  641,  642,  539,  529,  644,  645,  646,  647,  648,
 /*   300 */   649,  650,  652,  654,  655,  656,  657,  658,  659,  660,
 /*   310 */   661,  662,  555,  587,  651,  653,  609,  611,  673,
};
#define YY_REDUCE_COUNT (180)
#define YY_REDUCE_MIN   (-253)
#define YY_REDUCE_MAX   (408)
static const short yy_reduce_ofst[] = {
 /*     0 */    93,  -26,  -26,   26,   26, -190, -233, -219, -164, -179,
 /*    10 */  -124, -127,  -97,  -84,   51,  128,  134, -194, -189, -253,
 /*    20 */  -151,  -89,  -72,   -5,   62, -176,   96,  121, -100,  -12,
 /*    30 */   -79,  145,  146,  137, -192,  169,  186, -234,   17,  115,
 /*    40 */  -248, -220,  -49,  -39,    9,   19,   25,   54,  119,  157,
 /*    50 */   173,  179,  183,  189,  190,  191,  192,  193,  194,  195,
 /*    60 */   212,  213,  223,  224,  227,  229,  230,  231,  232,  219,
 /*    70 */   261,  264,  209,  270,  271,  272,  273,  206,  207,  233,
 /*    80 */   278,  281,  283,  292,  293,  294,  295,  297,  298,  300,
 /*    90 */   301,  302,  308,  309,  310,  312,  315,  316,  317,  318,
 /*   100 */   319,  320,  321,  323,  324,  325,  326,  327,  328,  329,
 /*   110 */   330,  331,  332,  333,  334,  335,  336,  337,  338,  339,
 /*   120 */   340,  341,  342,  343,  344,  345,  346,  347,  348,  349,
 /*   130 */   350,  351,  352,  234,  353,  354,  356,  221,  222,  235,
 /*   140 */   357,  240,  359,  249,  255,  236,  259,  237,  361,  238,
 /*   150 */   365,  363,  369,  244,  367,  370,  371,  372,  373,  376,
 /*   160 */   377,  380,  378,  381,  383,  382,  386,  389,  390,  392,
 /*   170 */   388,  391,  393,  395,  399,  403,  405,  406,  408,  404,
 /*   180 */   407,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   779,  894,  839,  906,  827,  836, 1034, 1034,  779,  779,
 /*    10 */   779,  779,  779,  779,  779,  779,  779,  954,  799, 1034,
 /*    20 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  836,
 /*    30 */   779,  779,  779,  843,  836,  843,  843,  949,  878,  896,
 /*    40 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*    50 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*    60 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*    70 */   779,  779,  956,  958,  960,  779,  779,  980,  980,  947,
 /*    80 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*    90 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*   100 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  825,
 /*   110 */   779,  823,  779,  779,  779,  779,  779,  779,  779,  779,
 /*   120 */   779,  779,  779,  779,  779,  779,  779,  809,  779,  779,
 /*   130 */   779,  779,  779,  779,  801,  801,  801,  779,  779,  779,
 /*   140 */   801,  779,  801,  987,  991,  985,  973,  981,  972,  968,
 /*   150 */   966,  965,  995,  779,  801,  801,  801,  840,  836,  836,
 /*   160 */   801,  801,  859,  857,  855,  847,  853,  849,  851,  845,
 /*   170 */   828,  801,  834,  834,  801,  834,  801,  834,  801,  878,
 /*   180 */   896,  779,  996,  779, 1033,  986, 1023, 1022, 1029, 1021,
 /*   190 */  1020, 1019,  779,  779,  779, 1015, 1016, 1018, 1017,  779,
 /*   200 */   779, 1025, 1024,  779,  779,  779,  779,  779,  779,  779,
 /*   210 */   779,  779,  779,  779,  779,  779,  998,  779,  992,  988,
 /*   220 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  908,
 /*   230 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*   240 */   779,  779,  779,  779,  946,  779,  779,  779,  779,  957,
 /*   250 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  982,
 /*   260 */   779,  974,  779,  779,  779,  779,  779,  920,  779,  779,
 /*   270 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*   280 */   779,  779,  779,  779,  779, 1044, 1042,  779,  779,  779,
 /*   290 */  1038,  779,  779,  779, 1036,  779,  779,  779,  779,  779,
 /*   300 */   779,  779,  779,  779,  779,  779,  779,  779,  779,  779,
 /*   310 */   779,  779,  862,  779,  807,  805,  779,  797,  779,
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
    0,  /*     TOPICS => nothing */
    0,  /*  FUNCTIONS => nothing */
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
    1,  /*     STABLE => ID */
    0,  /*      TOPIC => nothing */
    0,  /*   FUNCTION => nothing */
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
    0,  /*         AS => nothing */
    0,  /* OUTPUTTYPE => nothing */
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
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*      COMMA => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*    SESSION => nothing */
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
    1,  /*       SEMI => ID */
    1,  /*       NONE => ID */
    1,  /*       PREV => ID */
    1,  /*     LINEAR => ID */
    1,  /*     IMPORT => ID */
    1,  /*     METRIC => ID */
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
    1,  /*    METRICS => ID */
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
  ParseCTX_SDECL                /* A place to hold %extra_context */
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
  /*   46 */ "TOPICS",
  /*   47 */ "FUNCTIONS",
  /*   48 */ "MNODES",
  /*   49 */ "DNODES",
  /*   50 */ "ACCOUNTS",
  /*   51 */ "USERS",
  /*   52 */ "MODULES",
  /*   53 */ "QUERIES",
  /*   54 */ "CONNECTIONS",
  /*   55 */ "STREAMS",
  /*   56 */ "VARIABLES",
  /*   57 */ "SCORES",
  /*   58 */ "GRANTS",
  /*   59 */ "VNODES",
  /*   60 */ "IPTOKEN",
  /*   61 */ "DOT",
  /*   62 */ "CREATE",
  /*   63 */ "TABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "STABLE",
  /*   70 */ "TOPIC",
  /*   71 */ "FUNCTION",
  /*   72 */ "DNODE",
  /*   73 */ "USER",
  /*   74 */ "ACCOUNT",
  /*   75 */ "USE",
  /*   76 */ "DESCRIBE",
  /*   77 */ "ALTER",
  /*   78 */ "PASS",
  /*   79 */ "PRIVILEGE",
  /*   80 */ "LOCAL",
  /*   81 */ "IF",
  /*   82 */ "EXISTS",
  /*   83 */ "AS",
  /*   84 */ "OUTPUTTYPE",
  /*   85 */ "PPS",
  /*   86 */ "TSERIES",
  /*   87 */ "DBS",
  /*   88 */ "STORAGE",
  /*   89 */ "QTIME",
  /*   90 */ "CONNS",
  /*   91 */ "STATE",
  /*   92 */ "KEEP",
  /*   93 */ "CACHE",
  /*   94 */ "REPLICA",
  /*   95 */ "QUORUM",
  /*   96 */ "DAYS",
  /*   97 */ "MINROWS",
  /*   98 */ "MAXROWS",
  /*   99 */ "BLOCKS",
  /*  100 */ "CTIME",
  /*  101 */ "WAL",
  /*  102 */ "FSYNC",
  /*  103 */ "COMP",
  /*  104 */ "PRECISION",
  /*  105 */ "UPDATE",
  /*  106 */ "CACHELAST",
  /*  107 */ "PARTITIONS",
  /*  108 */ "LP",
  /*  109 */ "RP",
  /*  110 */ "UNSIGNED",
  /*  111 */ "TAGS",
  /*  112 */ "USING",
  /*  113 */ "COMMA",
  /*  114 */ "NULL",
  /*  115 */ "SELECT",
  /*  116 */ "UNION",
  /*  117 */ "ALL",
  /*  118 */ "DISTINCT",
  /*  119 */ "FROM",
  /*  120 */ "VARIABLE",
  /*  121 */ "INTERVAL",
  /*  122 */ "SESSION",
  /*  123 */ "FILL",
  /*  124 */ "SLIDING",
  /*  125 */ "ORDER",
  /*  126 */ "BY",
  /*  127 */ "ASC",
  /*  128 */ "DESC",
  /*  129 */ "GROUP",
  /*  130 */ "HAVING",
  /*  131 */ "LIMIT",
  /*  132 */ "OFFSET",
  /*  133 */ "SLIMIT",
  /*  134 */ "SOFFSET",
  /*  135 */ "WHERE",
  /*  136 */ "NOW",
  /*  137 */ "RESET",
  /*  138 */ "QUERY",
  /*  139 */ "ADD",
  /*  140 */ "COLUMN",
  /*  141 */ "TAG",
  /*  142 */ "CHANGE",
  /*  143 */ "SET",
  /*  144 */ "KILL",
  /*  145 */ "CONNECTION",
  /*  146 */ "STREAM",
  /*  147 */ "COLON",
  /*  148 */ "ABORT",
  /*  149 */ "AFTER",
  /*  150 */ "ATTACH",
  /*  151 */ "BEFORE",
  /*  152 */ "BEGIN",
  /*  153 */ "CASCADE",
  /*  154 */ "CLUSTER",
  /*  155 */ "CONFLICT",
  /*  156 */ "COPY",
  /*  157 */ "DEFERRED",
  /*  158 */ "DELIMITERS",
  /*  159 */ "DETACH",
  /*  160 */ "EACH",
  /*  161 */ "END",
  /*  162 */ "EXPLAIN",
  /*  163 */ "FAIL",
  /*  164 */ "FOR",
  /*  165 */ "IGNORE",
  /*  166 */ "IMMEDIATE",
  /*  167 */ "INITIALLY",
  /*  168 */ "INSTEAD",
  /*  169 */ "MATCH",
  /*  170 */ "KEY",
  /*  171 */ "OF",
  /*  172 */ "RAISE",
  /*  173 */ "REPLACE",
  /*  174 */ "RESTRICT",
  /*  175 */ "ROW",
  /*  176 */ "STATEMENT",
  /*  177 */ "TRIGGER",
  /*  178 */ "VIEW",
  /*  179 */ "SEMI",
  /*  180 */ "NONE",
  /*  181 */ "PREV",
  /*  182 */ "LINEAR",
  /*  183 */ "IMPORT",
  /*  184 */ "METRIC",
  /*  185 */ "TBNAME",
  /*  186 */ "JOIN",
  /*  187 */ "METRICS",
  /*  188 */ "INSERT",
  /*  189 */ "INTO",
  /*  190 */ "VALUES",
  /*  191 */ "program",
  /*  192 */ "cmd",
  /*  193 */ "dbPrefix",
  /*  194 */ "ids",
  /*  195 */ "cpxName",
  /*  196 */ "ifexists",
  /*  197 */ "alter_db_optr",
  /*  198 */ "alter_topic_optr",
  /*  199 */ "acct_optr",
  /*  200 */ "ifnotexists",
  /*  201 */ "db_optr",
  /*  202 */ "topic_optr",
  /*  203 */ "typename",
  /*  204 */ "pps",
  /*  205 */ "tseries",
  /*  206 */ "dbs",
  /*  207 */ "streams",
  /*  208 */ "storage",
  /*  209 */ "qtime",
  /*  210 */ "users",
  /*  211 */ "conns",
  /*  212 */ "state",
  /*  213 */ "keep",
  /*  214 */ "tagitemlist",
  /*  215 */ "cache",
  /*  216 */ "replica",
  /*  217 */ "quorum",
  /*  218 */ "days",
  /*  219 */ "minrows",
  /*  220 */ "maxrows",
  /*  221 */ "blocks",
  /*  222 */ "ctime",
  /*  223 */ "wal",
  /*  224 */ "fsync",
  /*  225 */ "comp",
  /*  226 */ "prec",
  /*  227 */ "update",
  /*  228 */ "cachelast",
  /*  229 */ "partitions",
  /*  230 */ "signed",
  /*  231 */ "create_table_args",
  /*  232 */ "create_stable_args",
  /*  233 */ "create_table_list",
  /*  234 */ "create_from_stable",
  /*  235 */ "columnlist",
  /*  236 */ "tagNamelist",
  /*  237 */ "select",
  /*  238 */ "column",
  /*  239 */ "tagitem",
  /*  240 */ "selcollist",
  /*  241 */ "from",
  /*  242 */ "where_opt",
  /*  243 */ "interval_opt",
  /*  244 */ "session_option",
  /*  245 */ "fill_opt",
  /*  246 */ "sliding_opt",
  /*  247 */ "groupby_opt",
  /*  248 */ "orderby_opt",
  /*  249 */ "having_opt",
  /*  250 */ "slimit_opt",
  /*  251 */ "limit_opt",
  /*  252 */ "union",
  /*  253 */ "sclp",
  /*  254 */ "distinct",
  /*  255 */ "expr",
  /*  256 */ "as",
  /*  257 */ "tablelist",
  /*  258 */ "tmvar",
  /*  259 */ "sortlist",
  /*  260 */ "sortitem",
  /*  261 */ "item",
  /*  262 */ "sortorder",
  /*  263 */ "grouplist",
  /*  264 */ "exprlist",
  /*  265 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW FUNCTIONS",
 /*   4 */ "cmd ::= SHOW MNODES",
 /*   5 */ "cmd ::= SHOW DNODES",
 /*   6 */ "cmd ::= SHOW ACCOUNTS",
 /*   7 */ "cmd ::= SHOW USERS",
 /*   8 */ "cmd ::= SHOW MODULES",
 /*   9 */ "cmd ::= SHOW QUERIES",
 /*  10 */ "cmd ::= SHOW CONNECTIONS",
 /*  11 */ "cmd ::= SHOW STREAMS",
 /*  12 */ "cmd ::= SHOW VARIABLES",
 /*  13 */ "cmd ::= SHOW SCORES",
 /*  14 */ "cmd ::= SHOW GRANTS",
 /*  15 */ "cmd ::= SHOW VNODES",
 /*  16 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  17 */ "dbPrefix ::=",
 /*  18 */ "dbPrefix ::= ids DOT",
 /*  19 */ "cpxName ::=",
 /*  20 */ "cpxName ::= DOT ids",
 /*  21 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
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
 /*  33 */ "cmd ::= DROP FUNCTION ids",
 /*  34 */ "cmd ::= DROP DNODE ids",
 /*  35 */ "cmd ::= DROP USER ids",
 /*  36 */ "cmd ::= DROP ACCOUNT ids",
 /*  37 */ "cmd ::= USE ids",
 /*  38 */ "cmd ::= DESCRIBE ids cpxName",
 /*  39 */ "cmd ::= ALTER USER ids PASS ids",
 /*  40 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids",
 /*  42 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids",
 /*  44 */ "cmd ::= ALTER LOCAL ids ids",
 /*  45 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  46 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
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
 /*  59 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename",
 /*  60 */ "cmd ::= CREATE USER ids PASS ids",
 /*  61 */ "pps ::=",
 /*  62 */ "pps ::= PPS INTEGER",
 /*  63 */ "tseries ::=",
 /*  64 */ "tseries ::= TSERIES INTEGER",
 /*  65 */ "dbs ::=",
 /*  66 */ "dbs ::= DBS INTEGER",
 /*  67 */ "streams ::=",
 /*  68 */ "streams ::= STREAMS INTEGER",
 /*  69 */ "storage ::=",
 /*  70 */ "storage ::= STORAGE INTEGER",
 /*  71 */ "qtime ::=",
 /*  72 */ "qtime ::= QTIME INTEGER",
 /*  73 */ "users ::=",
 /*  74 */ "users ::= USERS INTEGER",
 /*  75 */ "conns ::=",
 /*  76 */ "conns ::= CONNS INTEGER",
 /*  77 */ "state ::=",
 /*  78 */ "state ::= STATE ids",
 /*  79 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  80 */ "keep ::= KEEP tagitemlist",
 /*  81 */ "cache ::= CACHE INTEGER",
 /*  82 */ "replica ::= REPLICA INTEGER",
 /*  83 */ "quorum ::= QUORUM INTEGER",
 /*  84 */ "days ::= DAYS INTEGER",
 /*  85 */ "minrows ::= MINROWS INTEGER",
 /*  86 */ "maxrows ::= MAXROWS INTEGER",
 /*  87 */ "blocks ::= BLOCKS INTEGER",
 /*  88 */ "ctime ::= CTIME INTEGER",
 /*  89 */ "wal ::= WAL INTEGER",
 /*  90 */ "fsync ::= FSYNC INTEGER",
 /*  91 */ "comp ::= COMP INTEGER",
 /*  92 */ "prec ::= PRECISION STRING",
 /*  93 */ "update ::= UPDATE INTEGER",
 /*  94 */ "cachelast ::= CACHELAST INTEGER",
 /*  95 */ "partitions ::= PARTITIONS INTEGER",
 /*  96 */ "db_optr ::=",
 /*  97 */ "db_optr ::= db_optr cache",
 /*  98 */ "db_optr ::= db_optr replica",
 /*  99 */ "db_optr ::= db_optr quorum",
 /* 100 */ "db_optr ::= db_optr days",
 /* 101 */ "db_optr ::= db_optr minrows",
 /* 102 */ "db_optr ::= db_optr maxrows",
 /* 103 */ "db_optr ::= db_optr blocks",
 /* 104 */ "db_optr ::= db_optr ctime",
 /* 105 */ "db_optr ::= db_optr wal",
 /* 106 */ "db_optr ::= db_optr fsync",
 /* 107 */ "db_optr ::= db_optr comp",
 /* 108 */ "db_optr ::= db_optr prec",
 /* 109 */ "db_optr ::= db_optr keep",
 /* 110 */ "db_optr ::= db_optr update",
 /* 111 */ "db_optr ::= db_optr cachelast",
 /* 112 */ "topic_optr ::= db_optr",
 /* 113 */ "topic_optr ::= topic_optr partitions",
 /* 114 */ "alter_db_optr ::=",
 /* 115 */ "alter_db_optr ::= alter_db_optr replica",
 /* 116 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 117 */ "alter_db_optr ::= alter_db_optr keep",
 /* 118 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 119 */ "alter_db_optr ::= alter_db_optr comp",
 /* 120 */ "alter_db_optr ::= alter_db_optr wal",
 /* 121 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 122 */ "alter_db_optr ::= alter_db_optr update",
 /* 123 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 124 */ "alter_topic_optr ::= alter_db_optr",
 /* 125 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 126 */ "typename ::= ids",
 /* 127 */ "typename ::= ids LP signed RP",
 /* 128 */ "typename ::= ids UNSIGNED",
 /* 129 */ "signed ::= INTEGER",
 /* 130 */ "signed ::= PLUS INTEGER",
 /* 131 */ "signed ::= MINUS INTEGER",
 /* 132 */ "cmd ::= CREATE TABLE create_table_args",
 /* 133 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 134 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE TABLE create_table_list",
 /* 136 */ "create_table_list ::= create_from_stable",
 /* 137 */ "create_table_list ::= create_table_list create_from_stable",
 /* 138 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 139 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 140 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 142 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 143 */ "tagNamelist ::= ids",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 145 */ "columnlist ::= columnlist COMMA column",
 /* 146 */ "columnlist ::= column",
 /* 147 */ "column ::= ids typename",
 /* 148 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 149 */ "tagitemlist ::= tagitem",
 /* 150 */ "tagitem ::= INTEGER",
 /* 151 */ "tagitem ::= FLOAT",
 /* 152 */ "tagitem ::= STRING",
 /* 153 */ "tagitem ::= BOOL",
 /* 154 */ "tagitem ::= NULL",
 /* 155 */ "tagitem ::= MINUS INTEGER",
 /* 156 */ "tagitem ::= MINUS FLOAT",
 /* 157 */ "tagitem ::= PLUS INTEGER",
 /* 158 */ "tagitem ::= PLUS FLOAT",
 /* 159 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 160 */ "union ::= select",
 /* 161 */ "union ::= LP union RP",
 /* 162 */ "union ::= union UNION ALL select",
 /* 163 */ "union ::= union UNION ALL LP select RP",
 /* 164 */ "cmd ::= union",
 /* 165 */ "select ::= SELECT selcollist",
 /* 166 */ "sclp ::= selcollist COMMA",
 /* 167 */ "sclp ::=",
 /* 168 */ "selcollist ::= sclp distinct expr as",
 /* 169 */ "selcollist ::= sclp STAR",
 /* 170 */ "as ::= AS ids",
 /* 171 */ "as ::= ids",
 /* 172 */ "as ::=",
 /* 173 */ "distinct ::= DISTINCT",
 /* 174 */ "distinct ::=",
 /* 175 */ "from ::= FROM tablelist",
 /* 176 */ "tablelist ::= ids cpxName",
 /* 177 */ "tablelist ::= ids cpxName ids",
 /* 178 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 179 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 180 */ "tmvar ::= VARIABLE",
 /* 181 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 182 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 183 */ "interval_opt ::=",
 /* 184 */ "session_option ::=",
 /* 185 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 186 */ "fill_opt ::=",
 /* 187 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 188 */ "fill_opt ::= FILL LP ID RP",
 /* 189 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 190 */ "sliding_opt ::=",
 /* 191 */ "orderby_opt ::=",
 /* 192 */ "orderby_opt ::= ORDER BY sortlist",
 /* 193 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 194 */ "sortlist ::= item sortorder",
 /* 195 */ "item ::= ids cpxName",
 /* 196 */ "sortorder ::= ASC",
 /* 197 */ "sortorder ::= DESC",
 /* 198 */ "sortorder ::=",
 /* 199 */ "groupby_opt ::=",
 /* 200 */ "groupby_opt ::= GROUP BY grouplist",
 /* 201 */ "grouplist ::= grouplist COMMA item",
 /* 202 */ "grouplist ::= item",
 /* 203 */ "having_opt ::=",
 /* 204 */ "having_opt ::= HAVING expr",
 /* 205 */ "limit_opt ::=",
 /* 206 */ "limit_opt ::= LIMIT signed",
 /* 207 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 208 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 209 */ "slimit_opt ::=",
 /* 210 */ "slimit_opt ::= SLIMIT signed",
 /* 211 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 212 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 213 */ "where_opt ::=",
 /* 214 */ "where_opt ::= WHERE expr",
 /* 215 */ "expr ::= LP expr RP",
 /* 216 */ "expr ::= ID",
 /* 217 */ "expr ::= ID DOT ID",
 /* 218 */ "expr ::= ID DOT STAR",
 /* 219 */ "expr ::= INTEGER",
 /* 220 */ "expr ::= MINUS INTEGER",
 /* 221 */ "expr ::= PLUS INTEGER",
 /* 222 */ "expr ::= FLOAT",
 /* 223 */ "expr ::= MINUS FLOAT",
 /* 224 */ "expr ::= PLUS FLOAT",
 /* 225 */ "expr ::= STRING",
 /* 226 */ "expr ::= NOW",
 /* 227 */ "expr ::= VARIABLE",
 /* 228 */ "expr ::= BOOL",
 /* 229 */ "expr ::= ID LP exprlist RP",
 /* 230 */ "expr ::= ID LP STAR RP",
 /* 231 */ "expr ::= expr IS NULL",
 /* 232 */ "expr ::= expr IS NOT NULL",
 /* 233 */ "expr ::= expr LT expr",
 /* 234 */ "expr ::= expr GT expr",
 /* 235 */ "expr ::= expr LE expr",
 /* 236 */ "expr ::= expr GE expr",
 /* 237 */ "expr ::= expr NE expr",
 /* 238 */ "expr ::= expr EQ expr",
 /* 239 */ "expr ::= expr BETWEEN expr AND expr",
 /* 240 */ "expr ::= expr AND expr",
 /* 241 */ "expr ::= expr OR expr",
 /* 242 */ "expr ::= expr PLUS expr",
 /* 243 */ "expr ::= expr MINUS expr",
 /* 244 */ "expr ::= expr STAR expr",
 /* 245 */ "expr ::= expr SLASH expr",
 /* 246 */ "expr ::= expr REM expr",
 /* 247 */ "expr ::= expr LIKE expr",
 /* 248 */ "expr ::= expr IN LP exprlist RP",
 /* 249 */ "exprlist ::= exprlist COMMA expritem",
 /* 250 */ "exprlist ::= expritem",
 /* 251 */ "expritem ::= expr",
 /* 252 */ "expritem ::=",
 /* 253 */ "cmd ::= RESET QUERY CACHE",
 /* 254 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 255 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 258 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 260 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 261 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 262 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 263 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 264 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 265 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 266 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 267 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
void ParseInit(void *yypRawParser ParseCTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  ParseCTX_STORE
#ifdef YYTRACKMAXSTACKDEPTH
  yypParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  yypParser->yytos = NULL;
  yypParser->yystack = NULL;
  yypParser->yystksz = 0;
  if( yyGrowStack(yypParser) ){
    yypParser->yystack = &yypParser->yystk0;
    yypParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  yypParser->yytos = yypParser->yystack;
  yypParser->yystack[0].stateno = 0;
  yypParser->yystack[0].major = 0;
#if YYSTACKDEPTH>0
  yypParser->yystackEnd = &yypParser->yystack[YYSTACKDEPTH-1];
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
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE) ParseCTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    ParseCTX_STORE
    ParseInit(yypParser ParseCTX_PARAM);
  }
  return (void*)yypParser;
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
  ParseARG_FETCH
  ParseCTX_FETCH
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
    case 213: /* keep */
    case 214: /* tagitemlist */
    case 235: /* columnlist */
    case 236: /* tagNamelist */
    case 245: /* fill_opt */
    case 247: /* groupby_opt */
    case 248: /* orderby_opt */
    case 259: /* sortlist */
    case 263: /* grouplist */
{
taosArrayDestroy((yypminor->yy193));
}
      break;
    case 233: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy190));
}
      break;
    case 237: /* select */
{
doDestroyQuerySql((yypminor->yy288));
}
      break;
    case 240: /* selcollist */
    case 253: /* sclp */
    case 264: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy178));
}
      break;
    case 242: /* where_opt */
    case 249: /* having_opt */
    case 255: /* expr */
    case 265: /* expritem */
{
tSqlExprDestroy((yypminor->yy134));
}
      break;
    case 252: /* union */
{
destroyAllSelectClause((yypminor->yy21));
}
      break;
    case 260: /* sortitem */
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
static YYACTIONTYPE yy_find_shift_action(
  YYCODETYPE iLookAhead,    /* The look-ahead token */
  YYACTIONTYPE stateno      /* Current state number */
){
  int i;

  if( stateno>YY_MAX_SHIFT ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
#if defined(YYCOVERAGE)
  yycoverage[stateno][iLookAhead] = 1;
#endif
  do{
    i = yy_shift_ofst[stateno];
    assert( i>=0 );
    assert( i<=YY_ACTTAB_COUNT );
    assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD );
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    assert( i<(int)YY_NLOOKAHEAD );
    if( yy_lookahead[i]!=iLookAhead ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      assert( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0]) );
      iFallback = yyFallback[iLookAhead];
      if( iFallback!=0 ){
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
        assert( j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) );
        if( yy_lookahead[j]==YYWILDCARD && iLookAhead>0 ){
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
      assert( i>=0 && i<sizeof(yy_action)/sizeof(yy_action[0]) );
      return yy_action[i];
    }
  }while(1);
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
*/
static YYACTIONTYPE yy_find_reduce_action(
  YYACTIONTYPE stateno,     /* Current state number */
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
   ParseARG_FETCH
   ParseCTX_FETCH
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
   ParseARG_STORE /* Suppress warning about unused %extra_argument var */
   ParseCTX_STORE
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
  YYACTIONTYPE yyNewState,      /* The new state to shift in */
  YYCODETYPE yyMajor,           /* The major token to shift in */
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
  yytos->stateno = yyNewState;
  yytos->major = yyMajor;
  yytos->minor.yy0 = yyMinor;
  yyTraceShift(yypParser, yyNewState, "Shift");
}

/* For rule J, yyRuleInfoLhs[J] contains the symbol on the left-hand side
** of that rule */
static const YYCODETYPE yyRuleInfoLhs[] = {
   191,  /* (0) program ::= cmd */
   192,  /* (1) cmd ::= SHOW DATABASES */
   192,  /* (2) cmd ::= SHOW TOPICS */
   192,  /* (3) cmd ::= SHOW FUNCTIONS */
   192,  /* (4) cmd ::= SHOW MNODES */
   192,  /* (5) cmd ::= SHOW DNODES */
   192,  /* (6) cmd ::= SHOW ACCOUNTS */
   192,  /* (7) cmd ::= SHOW USERS */
   192,  /* (8) cmd ::= SHOW MODULES */
   192,  /* (9) cmd ::= SHOW QUERIES */
   192,  /* (10) cmd ::= SHOW CONNECTIONS */
   192,  /* (11) cmd ::= SHOW STREAMS */
   192,  /* (12) cmd ::= SHOW VARIABLES */
   192,  /* (13) cmd ::= SHOW SCORES */
   192,  /* (14) cmd ::= SHOW GRANTS */
   192,  /* (15) cmd ::= SHOW VNODES */
   192,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
   193,  /* (17) dbPrefix ::= */
   193,  /* (18) dbPrefix ::= ids DOT */
   195,  /* (19) cpxName ::= */
   195,  /* (20) cpxName ::= DOT ids */
   192,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   192,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   192,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   192,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   192,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   192,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   192,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   192,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   192,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   192,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   192,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   192,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   192,  /* (33) cmd ::= DROP FUNCTION ids */
   192,  /* (34) cmd ::= DROP DNODE ids */
   192,  /* (35) cmd ::= DROP USER ids */
   192,  /* (36) cmd ::= DROP ACCOUNT ids */
   192,  /* (37) cmd ::= USE ids */
   192,  /* (38) cmd ::= DESCRIBE ids cpxName */
   192,  /* (39) cmd ::= ALTER USER ids PASS ids */
   192,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   192,  /* (41) cmd ::= ALTER DNODE ids ids */
   192,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   192,  /* (43) cmd ::= ALTER LOCAL ids */
   192,  /* (44) cmd ::= ALTER LOCAL ids ids */
   192,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   192,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   192,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   192,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   194,  /* (49) ids ::= ID */
   194,  /* (50) ids ::= STRING */
   196,  /* (51) ifexists ::= IF EXISTS */
   196,  /* (52) ifexists ::= */
   200,  /* (53) ifnotexists ::= IF NOT EXISTS */
   200,  /* (54) ifnotexists ::= */
   192,  /* (55) cmd ::= CREATE DNODE ids */
   192,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   192,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   192,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   192,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
   192,  /* (60) cmd ::= CREATE USER ids PASS ids */
   204,  /* (61) pps ::= */
   204,  /* (62) pps ::= PPS INTEGER */
   205,  /* (63) tseries ::= */
   205,  /* (64) tseries ::= TSERIES INTEGER */
   206,  /* (65) dbs ::= */
   206,  /* (66) dbs ::= DBS INTEGER */
   207,  /* (67) streams ::= */
   207,  /* (68) streams ::= STREAMS INTEGER */
   208,  /* (69) storage ::= */
   208,  /* (70) storage ::= STORAGE INTEGER */
   209,  /* (71) qtime ::= */
   209,  /* (72) qtime ::= QTIME INTEGER */
   210,  /* (73) users ::= */
   210,  /* (74) users ::= USERS INTEGER */
   211,  /* (75) conns ::= */
   211,  /* (76) conns ::= CONNS INTEGER */
   212,  /* (77) state ::= */
   212,  /* (78) state ::= STATE ids */
   199,  /* (79) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   213,  /* (80) keep ::= KEEP tagitemlist */
   215,  /* (81) cache ::= CACHE INTEGER */
   216,  /* (82) replica ::= REPLICA INTEGER */
   217,  /* (83) quorum ::= QUORUM INTEGER */
   218,  /* (84) days ::= DAYS INTEGER */
   219,  /* (85) minrows ::= MINROWS INTEGER */
   220,  /* (86) maxrows ::= MAXROWS INTEGER */
   221,  /* (87) blocks ::= BLOCKS INTEGER */
   222,  /* (88) ctime ::= CTIME INTEGER */
   223,  /* (89) wal ::= WAL INTEGER */
   224,  /* (90) fsync ::= FSYNC INTEGER */
   225,  /* (91) comp ::= COMP INTEGER */
   226,  /* (92) prec ::= PRECISION STRING */
   227,  /* (93) update ::= UPDATE INTEGER */
   228,  /* (94) cachelast ::= CACHELAST INTEGER */
   229,  /* (95) partitions ::= PARTITIONS INTEGER */
   201,  /* (96) db_optr ::= */
   201,  /* (97) db_optr ::= db_optr cache */
   201,  /* (98) db_optr ::= db_optr replica */
   201,  /* (99) db_optr ::= db_optr quorum */
   201,  /* (100) db_optr ::= db_optr days */
   201,  /* (101) db_optr ::= db_optr minrows */
   201,  /* (102) db_optr ::= db_optr maxrows */
   201,  /* (103) db_optr ::= db_optr blocks */
   201,  /* (104) db_optr ::= db_optr ctime */
   201,  /* (105) db_optr ::= db_optr wal */
   201,  /* (106) db_optr ::= db_optr fsync */
   201,  /* (107) db_optr ::= db_optr comp */
   201,  /* (108) db_optr ::= db_optr prec */
   201,  /* (109) db_optr ::= db_optr keep */
   201,  /* (110) db_optr ::= db_optr update */
   201,  /* (111) db_optr ::= db_optr cachelast */
   202,  /* (112) topic_optr ::= db_optr */
   202,  /* (113) topic_optr ::= topic_optr partitions */
   197,  /* (114) alter_db_optr ::= */
   197,  /* (115) alter_db_optr ::= alter_db_optr replica */
   197,  /* (116) alter_db_optr ::= alter_db_optr quorum */
   197,  /* (117) alter_db_optr ::= alter_db_optr keep */
   197,  /* (118) alter_db_optr ::= alter_db_optr blocks */
   197,  /* (119) alter_db_optr ::= alter_db_optr comp */
   197,  /* (120) alter_db_optr ::= alter_db_optr wal */
   197,  /* (121) alter_db_optr ::= alter_db_optr fsync */
   197,  /* (122) alter_db_optr ::= alter_db_optr update */
   197,  /* (123) alter_db_optr ::= alter_db_optr cachelast */
   198,  /* (124) alter_topic_optr ::= alter_db_optr */
   198,  /* (125) alter_topic_optr ::= alter_topic_optr partitions */
   203,  /* (126) typename ::= ids */
   203,  /* (127) typename ::= ids LP signed RP */
   203,  /* (128) typename ::= ids UNSIGNED */
   230,  /* (129) signed ::= INTEGER */
   230,  /* (130) signed ::= PLUS INTEGER */
   230,  /* (131) signed ::= MINUS INTEGER */
   192,  /* (132) cmd ::= CREATE TABLE create_table_args */
   192,  /* (133) cmd ::= CREATE TABLE create_stable_args */
   192,  /* (134) cmd ::= CREATE STABLE create_stable_args */
   192,  /* (135) cmd ::= CREATE TABLE create_table_list */
   233,  /* (136) create_table_list ::= create_from_stable */
   233,  /* (137) create_table_list ::= create_table_list create_from_stable */
   231,  /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   232,  /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   234,  /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   234,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   236,  /* (142) tagNamelist ::= tagNamelist COMMA ids */
   236,  /* (143) tagNamelist ::= ids */
   231,  /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
   235,  /* (145) columnlist ::= columnlist COMMA column */
   235,  /* (146) columnlist ::= column */
   238,  /* (147) column ::= ids typename */
   214,  /* (148) tagitemlist ::= tagitemlist COMMA tagitem */
   214,  /* (149) tagitemlist ::= tagitem */
   239,  /* (150) tagitem ::= INTEGER */
   239,  /* (151) tagitem ::= FLOAT */
   239,  /* (152) tagitem ::= STRING */
   239,  /* (153) tagitem ::= BOOL */
   239,  /* (154) tagitem ::= NULL */
   239,  /* (155) tagitem ::= MINUS INTEGER */
   239,  /* (156) tagitem ::= MINUS FLOAT */
   239,  /* (157) tagitem ::= PLUS INTEGER */
   239,  /* (158) tagitem ::= PLUS FLOAT */
   237,  /* (159) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   252,  /* (160) union ::= select */
   252,  /* (161) union ::= LP union RP */
   252,  /* (162) union ::= union UNION ALL select */
   252,  /* (163) union ::= union UNION ALL LP select RP */
   192,  /* (164) cmd ::= union */
   237,  /* (165) select ::= SELECT selcollist */
   253,  /* (166) sclp ::= selcollist COMMA */
   253,  /* (167) sclp ::= */
   240,  /* (168) selcollist ::= sclp distinct expr as */
   240,  /* (169) selcollist ::= sclp STAR */
   256,  /* (170) as ::= AS ids */
   256,  /* (171) as ::= ids */
   256,  /* (172) as ::= */
   254,  /* (173) distinct ::= DISTINCT */
   254,  /* (174) distinct ::= */
   241,  /* (175) from ::= FROM tablelist */
   257,  /* (176) tablelist ::= ids cpxName */
   257,  /* (177) tablelist ::= ids cpxName ids */
   257,  /* (178) tablelist ::= tablelist COMMA ids cpxName */
   257,  /* (179) tablelist ::= tablelist COMMA ids cpxName ids */
   258,  /* (180) tmvar ::= VARIABLE */
   243,  /* (181) interval_opt ::= INTERVAL LP tmvar RP */
   243,  /* (182) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   243,  /* (183) interval_opt ::= */
   244,  /* (184) session_option ::= */
   244,  /* (185) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   245,  /* (186) fill_opt ::= */
   245,  /* (187) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   245,  /* (188) fill_opt ::= FILL LP ID RP */
   246,  /* (189) sliding_opt ::= SLIDING LP tmvar RP */
   246,  /* (190) sliding_opt ::= */
   248,  /* (191) orderby_opt ::= */
   248,  /* (192) orderby_opt ::= ORDER BY sortlist */
   259,  /* (193) sortlist ::= sortlist COMMA item sortorder */
   259,  /* (194) sortlist ::= item sortorder */
   261,  /* (195) item ::= ids cpxName */
   262,  /* (196) sortorder ::= ASC */
   262,  /* (197) sortorder ::= DESC */
   262,  /* (198) sortorder ::= */
   247,  /* (199) groupby_opt ::= */
   247,  /* (200) groupby_opt ::= GROUP BY grouplist */
   263,  /* (201) grouplist ::= grouplist COMMA item */
   263,  /* (202) grouplist ::= item */
   249,  /* (203) having_opt ::= */
   249,  /* (204) having_opt ::= HAVING expr */
   251,  /* (205) limit_opt ::= */
   251,  /* (206) limit_opt ::= LIMIT signed */
   251,  /* (207) limit_opt ::= LIMIT signed OFFSET signed */
   251,  /* (208) limit_opt ::= LIMIT signed COMMA signed */
   250,  /* (209) slimit_opt ::= */
   250,  /* (210) slimit_opt ::= SLIMIT signed */
   250,  /* (211) slimit_opt ::= SLIMIT signed SOFFSET signed */
   250,  /* (212) slimit_opt ::= SLIMIT signed COMMA signed */
   242,  /* (213) where_opt ::= */
   242,  /* (214) where_opt ::= WHERE expr */
   255,  /* (215) expr ::= LP expr RP */
   255,  /* (216) expr ::= ID */
   255,  /* (217) expr ::= ID DOT ID */
   255,  /* (218) expr ::= ID DOT STAR */
   255,  /* (219) expr ::= INTEGER */
   255,  /* (220) expr ::= MINUS INTEGER */
   255,  /* (221) expr ::= PLUS INTEGER */
   255,  /* (222) expr ::= FLOAT */
   255,  /* (223) expr ::= MINUS FLOAT */
   255,  /* (224) expr ::= PLUS FLOAT */
   255,  /* (225) expr ::= STRING */
   255,  /* (226) expr ::= NOW */
   255,  /* (227) expr ::= VARIABLE */
   255,  /* (228) expr ::= BOOL */
   255,  /* (229) expr ::= ID LP exprlist RP */
   255,  /* (230) expr ::= ID LP STAR RP */
   255,  /* (231) expr ::= expr IS NULL */
   255,  /* (232) expr ::= expr IS NOT NULL */
   255,  /* (233) expr ::= expr LT expr */
   255,  /* (234) expr ::= expr GT expr */
   255,  /* (235) expr ::= expr LE expr */
   255,  /* (236) expr ::= expr GE expr */
   255,  /* (237) expr ::= expr NE expr */
   255,  /* (238) expr ::= expr EQ expr */
   255,  /* (239) expr ::= expr BETWEEN expr AND expr */
   255,  /* (240) expr ::= expr AND expr */
   255,  /* (241) expr ::= expr OR expr */
   255,  /* (242) expr ::= expr PLUS expr */
   255,  /* (243) expr ::= expr MINUS expr */
   255,  /* (244) expr ::= expr STAR expr */
   255,  /* (245) expr ::= expr SLASH expr */
   255,  /* (246) expr ::= expr REM expr */
   255,  /* (247) expr ::= expr LIKE expr */
   255,  /* (248) expr ::= expr IN LP exprlist RP */
   264,  /* (249) exprlist ::= exprlist COMMA expritem */
   264,  /* (250) exprlist ::= expritem */
   265,  /* (251) expritem ::= expr */
   265,  /* (252) expritem ::= */
   192,  /* (253) cmd ::= RESET QUERY CACHE */
   192,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   192,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   192,  /* (256) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   192,  /* (257) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   192,  /* (258) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   192,  /* (259) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   192,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   192,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   192,  /* (262) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   192,  /* (263) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   192,  /* (264) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   192,  /* (265) cmd ::= KILL CONNECTION INTEGER */
   192,  /* (266) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   192,  /* (267) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW TOPICS */
   -2,  /* (3) cmd ::= SHOW FUNCTIONS */
   -2,  /* (4) cmd ::= SHOW MNODES */
   -2,  /* (5) cmd ::= SHOW DNODES */
   -2,  /* (6) cmd ::= SHOW ACCOUNTS */
   -2,  /* (7) cmd ::= SHOW USERS */
   -2,  /* (8) cmd ::= SHOW MODULES */
   -2,  /* (9) cmd ::= SHOW QUERIES */
   -2,  /* (10) cmd ::= SHOW CONNECTIONS */
   -2,  /* (11) cmd ::= SHOW STREAMS */
   -2,  /* (12) cmd ::= SHOW VARIABLES */
   -2,  /* (13) cmd ::= SHOW SCORES */
   -2,  /* (14) cmd ::= SHOW GRANTS */
   -2,  /* (15) cmd ::= SHOW VNODES */
   -3,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (17) dbPrefix ::= */
   -2,  /* (18) dbPrefix ::= ids DOT */
    0,  /* (19) cpxName ::= */
   -2,  /* (20) cpxName ::= DOT ids */
   -5,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   -4,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (33) cmd ::= DROP FUNCTION ids */
   -3,  /* (34) cmd ::= DROP DNODE ids */
   -3,  /* (35) cmd ::= DROP USER ids */
   -3,  /* (36) cmd ::= DROP ACCOUNT ids */
   -2,  /* (37) cmd ::= USE ids */
   -3,  /* (38) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (39) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (41) cmd ::= ALTER DNODE ids ids */
   -5,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (43) cmd ::= ALTER LOCAL ids */
   -4,  /* (44) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (49) ids ::= ID */
   -1,  /* (50) ids ::= STRING */
   -2,  /* (51) ifexists ::= IF EXISTS */
    0,  /* (52) ifexists ::= */
   -3,  /* (53) ifnotexists ::= IF NOT EXISTS */
    0,  /* (54) ifnotexists ::= */
   -3,  /* (55) cmd ::= CREATE DNODE ids */
   -6,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -7,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
   -5,  /* (60) cmd ::= CREATE USER ids PASS ids */
    0,  /* (61) pps ::= */
   -2,  /* (62) pps ::= PPS INTEGER */
    0,  /* (63) tseries ::= */
   -2,  /* (64) tseries ::= TSERIES INTEGER */
    0,  /* (65) dbs ::= */
   -2,  /* (66) dbs ::= DBS INTEGER */
    0,  /* (67) streams ::= */
   -2,  /* (68) streams ::= STREAMS INTEGER */
    0,  /* (69) storage ::= */
   -2,  /* (70) storage ::= STORAGE INTEGER */
    0,  /* (71) qtime ::= */
   -2,  /* (72) qtime ::= QTIME INTEGER */
    0,  /* (73) users ::= */
   -2,  /* (74) users ::= USERS INTEGER */
    0,  /* (75) conns ::= */
   -2,  /* (76) conns ::= CONNS INTEGER */
    0,  /* (77) state ::= */
   -2,  /* (78) state ::= STATE ids */
   -9,  /* (79) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (80) keep ::= KEEP tagitemlist */
   -2,  /* (81) cache ::= CACHE INTEGER */
   -2,  /* (82) replica ::= REPLICA INTEGER */
   -2,  /* (83) quorum ::= QUORUM INTEGER */
   -2,  /* (84) days ::= DAYS INTEGER */
   -2,  /* (85) minrows ::= MINROWS INTEGER */
   -2,  /* (86) maxrows ::= MAXROWS INTEGER */
   -2,  /* (87) blocks ::= BLOCKS INTEGER */
   -2,  /* (88) ctime ::= CTIME INTEGER */
   -2,  /* (89) wal ::= WAL INTEGER */
   -2,  /* (90) fsync ::= FSYNC INTEGER */
   -2,  /* (91) comp ::= COMP INTEGER */
   -2,  /* (92) prec ::= PRECISION STRING */
   -2,  /* (93) update ::= UPDATE INTEGER */
   -2,  /* (94) cachelast ::= CACHELAST INTEGER */
   -2,  /* (95) partitions ::= PARTITIONS INTEGER */
    0,  /* (96) db_optr ::= */
   -2,  /* (97) db_optr ::= db_optr cache */
   -2,  /* (98) db_optr ::= db_optr replica */
   -2,  /* (99) db_optr ::= db_optr quorum */
   -2,  /* (100) db_optr ::= db_optr days */
   -2,  /* (101) db_optr ::= db_optr minrows */
   -2,  /* (102) db_optr ::= db_optr maxrows */
   -2,  /* (103) db_optr ::= db_optr blocks */
   -2,  /* (104) db_optr ::= db_optr ctime */
   -2,  /* (105) db_optr ::= db_optr wal */
   -2,  /* (106) db_optr ::= db_optr fsync */
   -2,  /* (107) db_optr ::= db_optr comp */
   -2,  /* (108) db_optr ::= db_optr prec */
   -2,  /* (109) db_optr ::= db_optr keep */
   -2,  /* (110) db_optr ::= db_optr update */
   -2,  /* (111) db_optr ::= db_optr cachelast */
   -1,  /* (112) topic_optr ::= db_optr */
   -2,  /* (113) topic_optr ::= topic_optr partitions */
    0,  /* (114) alter_db_optr ::= */
   -2,  /* (115) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (116) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (117) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (118) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (119) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (120) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (121) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (122) alter_db_optr ::= alter_db_optr update */
   -2,  /* (123) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (124) alter_topic_optr ::= alter_db_optr */
   -2,  /* (125) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (126) typename ::= ids */
   -4,  /* (127) typename ::= ids LP signed RP */
   -2,  /* (128) typename ::= ids UNSIGNED */
   -1,  /* (129) signed ::= INTEGER */
   -2,  /* (130) signed ::= PLUS INTEGER */
   -2,  /* (131) signed ::= MINUS INTEGER */
   -3,  /* (132) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (133) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (134) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (135) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (136) create_table_list ::= create_from_stable */
   -2,  /* (137) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (142) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (143) tagNamelist ::= ids */
   -5,  /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (145) columnlist ::= columnlist COMMA column */
   -1,  /* (146) columnlist ::= column */
   -2,  /* (147) column ::= ids typename */
   -3,  /* (148) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (149) tagitemlist ::= tagitem */
   -1,  /* (150) tagitem ::= INTEGER */
   -1,  /* (151) tagitem ::= FLOAT */
   -1,  /* (152) tagitem ::= STRING */
   -1,  /* (153) tagitem ::= BOOL */
   -1,  /* (154) tagitem ::= NULL */
   -2,  /* (155) tagitem ::= MINUS INTEGER */
   -2,  /* (156) tagitem ::= MINUS FLOAT */
   -2,  /* (157) tagitem ::= PLUS INTEGER */
   -2,  /* (158) tagitem ::= PLUS FLOAT */
  -13,  /* (159) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -1,  /* (160) union ::= select */
   -3,  /* (161) union ::= LP union RP */
   -4,  /* (162) union ::= union UNION ALL select */
   -6,  /* (163) union ::= union UNION ALL LP select RP */
   -1,  /* (164) cmd ::= union */
   -2,  /* (165) select ::= SELECT selcollist */
   -2,  /* (166) sclp ::= selcollist COMMA */
    0,  /* (167) sclp ::= */
   -4,  /* (168) selcollist ::= sclp distinct expr as */
   -2,  /* (169) selcollist ::= sclp STAR */
   -2,  /* (170) as ::= AS ids */
   -1,  /* (171) as ::= ids */
    0,  /* (172) as ::= */
   -1,  /* (173) distinct ::= DISTINCT */
    0,  /* (174) distinct ::= */
   -2,  /* (175) from ::= FROM tablelist */
   -2,  /* (176) tablelist ::= ids cpxName */
   -3,  /* (177) tablelist ::= ids cpxName ids */
   -4,  /* (178) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (179) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (180) tmvar ::= VARIABLE */
   -4,  /* (181) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (182) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (183) interval_opt ::= */
    0,  /* (184) session_option ::= */
   -7,  /* (185) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (186) fill_opt ::= */
   -6,  /* (187) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (188) fill_opt ::= FILL LP ID RP */
   -4,  /* (189) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (190) sliding_opt ::= */
    0,  /* (191) orderby_opt ::= */
   -3,  /* (192) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (193) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (194) sortlist ::= item sortorder */
   -2,  /* (195) item ::= ids cpxName */
   -1,  /* (196) sortorder ::= ASC */
   -1,  /* (197) sortorder ::= DESC */
    0,  /* (198) sortorder ::= */
    0,  /* (199) groupby_opt ::= */
   -3,  /* (200) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (201) grouplist ::= grouplist COMMA item */
   -1,  /* (202) grouplist ::= item */
    0,  /* (203) having_opt ::= */
   -2,  /* (204) having_opt ::= HAVING expr */
    0,  /* (205) limit_opt ::= */
   -2,  /* (206) limit_opt ::= LIMIT signed */
   -4,  /* (207) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (208) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (209) slimit_opt ::= */
   -2,  /* (210) slimit_opt ::= SLIMIT signed */
   -4,  /* (211) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (212) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (213) where_opt ::= */
   -2,  /* (214) where_opt ::= WHERE expr */
   -3,  /* (215) expr ::= LP expr RP */
   -1,  /* (216) expr ::= ID */
   -3,  /* (217) expr ::= ID DOT ID */
   -3,  /* (218) expr ::= ID DOT STAR */
   -1,  /* (219) expr ::= INTEGER */
   -2,  /* (220) expr ::= MINUS INTEGER */
   -2,  /* (221) expr ::= PLUS INTEGER */
   -1,  /* (222) expr ::= FLOAT */
   -2,  /* (223) expr ::= MINUS FLOAT */
   -2,  /* (224) expr ::= PLUS FLOAT */
   -1,  /* (225) expr ::= STRING */
   -1,  /* (226) expr ::= NOW */
   -1,  /* (227) expr ::= VARIABLE */
   -1,  /* (228) expr ::= BOOL */
   -4,  /* (229) expr ::= ID LP exprlist RP */
   -4,  /* (230) expr ::= ID LP STAR RP */
   -3,  /* (231) expr ::= expr IS NULL */
   -4,  /* (232) expr ::= expr IS NOT NULL */
   -3,  /* (233) expr ::= expr LT expr */
   -3,  /* (234) expr ::= expr GT expr */
   -3,  /* (235) expr ::= expr LE expr */
   -3,  /* (236) expr ::= expr GE expr */
   -3,  /* (237) expr ::= expr NE expr */
   -3,  /* (238) expr ::= expr EQ expr */
   -5,  /* (239) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (240) expr ::= expr AND expr */
   -3,  /* (241) expr ::= expr OR expr */
   -3,  /* (242) expr ::= expr PLUS expr */
   -3,  /* (243) expr ::= expr MINUS expr */
   -3,  /* (244) expr ::= expr STAR expr */
   -3,  /* (245) expr ::= expr SLASH expr */
   -3,  /* (246) expr ::= expr REM expr */
   -3,  /* (247) expr ::= expr LIKE expr */
   -5,  /* (248) expr ::= expr IN LP exprlist RP */
   -3,  /* (249) exprlist ::= exprlist COMMA expritem */
   -1,  /* (250) exprlist ::= expritem */
   -1,  /* (251) expritem ::= expr */
    0,  /* (252) expritem ::= */
   -3,  /* (253) cmd ::= RESET QUERY CACHE */
   -7,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (256) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (257) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (258) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (259) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (262) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (263) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (264) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (265) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (266) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (267) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
static YYACTIONTYPE yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno,       /* Number of the rule by which to reduce */
  int yyLookahead,             /* Lookahead token, or YYNOCODE if none */
  ParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  ParseCTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  YYACTIONTYPE yyact;             /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH
  (void)yyLookahead;
  (void)yyLookaheadToken;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfoNRhs[yyruleno];
    if( yysize ){
      fprintf(yyTraceFILE, "%sReduce %d [%s]%s, pop back to state %d.\n",
        yyTracePrompt,
        yyruleno, yyRuleName[yyruleno],
        yyruleno<YYNRULE_WITH_ACTION ? "" : " without external action",
        yymsp[yysize].stateno);
    }else{
      fprintf(yyTraceFILE, "%sReduce %d [%s]%s.\n",
        yyTracePrompt, yyruleno, yyRuleName[yyruleno],
        yyruleno<YYNRULE_WITH_ACTION ? "" : " without external action");
    }
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if( yyRuleInfoNRhs[yyruleno]==0 ){
#ifdef YYTRACKMAXSTACKDEPTH
    if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
      yypParser->yyhwm++;
      assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack));
    }
#endif
#if YYSTACKDEPTH>0 
    if( yypParser->yytos>=yypParser->yystackEnd ){
      yyStackOverflow(yypParser);
      /* The call to yyStackOverflow() above pops the stack until it is
      ** empty, causing the main parser loop to exit.  So the return value
      ** is never used and does not matter. */
      return 0;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        /* The call to yyStackOverflow() above pops the stack until it is
        ** empty, causing the main parser loop to exit.  So the return value
        ** is never used and does not matter. */
        return 0;
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
      case 132: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==134);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNCTION, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 8: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 10: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 11: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 13: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 15: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 16: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 17: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 18: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 20: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 21: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
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
    setDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDbName(&token, &yymsp[-2].minor.yy0);
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
      case 33: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 34: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 35: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 36: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 37: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 38: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 39: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 40: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 44: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 45: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 46: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==46);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy114, &t);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy183);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);}
        break;
      case 49: /* ids ::= ID */
      case 50: /* ids ::= STRING */ yytestcase(yyruleno==50);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 51: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 52: /* ifexists ::= */
      case 54: /* ifnotexists ::= */ yytestcase(yyruleno==54);
      case 174: /* distinct ::= */ yytestcase(yyruleno==174);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 53: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy114, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy27);}
        break;
      case 60: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 61: /* pps ::= */
      case 63: /* tseries ::= */ yytestcase(yyruleno==63);
      case 65: /* dbs ::= */ yytestcase(yyruleno==65);
      case 67: /* streams ::= */ yytestcase(yyruleno==67);
      case 69: /* storage ::= */ yytestcase(yyruleno==69);
      case 71: /* qtime ::= */ yytestcase(yyruleno==71);
      case 73: /* users ::= */ yytestcase(yyruleno==73);
      case 75: /* conns ::= */ yytestcase(yyruleno==75);
      case 77: /* state ::= */ yytestcase(yyruleno==77);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 62: /* pps ::= PPS INTEGER */
      case 64: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==64);
      case 66: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==70);
      case 72: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==72);
      case 74: /* users ::= USERS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==76);
      case 78: /* state ::= STATE ids */ yytestcase(yyruleno==78);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 79: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy183.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy183.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy183.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy183.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy183.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy183.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy183.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy183.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy183.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy183 = yylhsminor.yy183;
        break;
      case 80: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy193 = yymsp[0].minor.yy193; }
        break;
      case 81: /* cache ::= CACHE INTEGER */
      case 82: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==82);
      case 83: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==83);
      case 84: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==88);
      case 89: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==89);
      case 90: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==90);
      case 91: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==91);
      case 92: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==92);
      case 93: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==93);
      case 94: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==94);
      case 95: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==95);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 96: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy114); yymsp[1].minor.yy114.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 97: /* db_optr ::= db_optr cache */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 98: /* db_optr ::= db_optr replica */
      case 115: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==115);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 99: /* db_optr ::= db_optr quorum */
      case 116: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==116);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 100: /* db_optr ::= db_optr days */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 101: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 102: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 103: /* db_optr ::= db_optr blocks */
      case 118: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==118);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 104: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 105: /* db_optr ::= db_optr wal */
      case 120: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==120);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 106: /* db_optr ::= db_optr fsync */
      case 121: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==121);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 107: /* db_optr ::= db_optr comp */
      case 119: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==119);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 108: /* db_optr ::= db_optr prec */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 109: /* db_optr ::= db_optr keep */
      case 117: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==117);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.keep = yymsp[0].minor.yy193; }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 110: /* db_optr ::= db_optr update */
      case 122: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==122);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 111: /* db_optr ::= db_optr cachelast */
      case 123: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==123);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 112: /* topic_optr ::= db_optr */
      case 124: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==124);
{ yylhsminor.yy114 = yymsp[0].minor.yy114; yylhsminor.yy114.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 113: /* topic_optr ::= topic_optr partitions */
      case 125: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==125);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 114: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy114); yymsp[1].minor.yy114.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 126: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy27, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy27 = yylhsminor.yy27;
        break;
      case 127: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy473 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy27, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy473;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy27, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy27 = yylhsminor.yy27;
        break;
      case 128: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy27, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy27 = yylhsminor.yy27;
        break;
      case 129: /* signed ::= INTEGER */
{ yylhsminor.yy473 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy473 = yylhsminor.yy473;
        break;
      case 130: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy473 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 131: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy473 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 135: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy190;}
        break;
      case 136: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy192);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy190 = pCreateTable;
}
  yymsp[0].minor.yy190 = yylhsminor.yy190;
        break;
      case 137: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy190->childTableInfo, &yymsp[0].minor.yy192);
  yylhsminor.yy190 = yymsp[-1].minor.yy190;
}
  yymsp[-1].minor.yy190 = yylhsminor.yy190;
        break;
      case 138: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy190 = tSetCreateSqlElems(yymsp[-1].minor.yy193, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy190, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy190 = yylhsminor.yy190;
        break;
      case 139: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy190 = tSetCreateSqlElems(yymsp[-5].minor.yy193, yymsp[-1].minor.yy193, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy190, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy190 = yylhsminor.yy190;
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy193, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy192 = yylhsminor.yy192;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy193, yymsp[-1].minor.yy193, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy192 = yylhsminor.yy192;
        break;
      case 142: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy193, &yymsp[0].minor.yy0); yylhsminor.yy193 = yymsp[-2].minor.yy193;  }
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 143: /* tagNamelist ::= ids */
{yylhsminor.yy193 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy193, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy190 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy288, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy190, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy190 = yylhsminor.yy190;
        break;
      case 145: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy193, &yymsp[0].minor.yy27); yylhsminor.yy193 = yymsp[-2].minor.yy193;  }
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 146: /* columnlist ::= column */
{yylhsminor.yy193 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy193, &yymsp[0].minor.yy27);}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 147: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy27, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy27);
}
  yymsp[-1].minor.yy27 = yylhsminor.yy27;
        break;
      case 148: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy193 = tVariantListAppend(yymsp[-2].minor.yy193, &yymsp[0].minor.yy442, -1);    }
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 149: /* tagitemlist ::= tagitem */
{ yylhsminor.yy193 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 150: /* tagitem ::= INTEGER */
      case 151: /* tagitem ::= FLOAT */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= STRING */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= BOOL */ yytestcase(yyruleno==153);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 154: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 155: /* tagitem ::= MINUS INTEGER */
      case 156: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==158);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 159: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy288 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy178, yymsp[-10].minor.yy193, yymsp[-9].minor.yy134, yymsp[-4].minor.yy193, yymsp[-3].minor.yy193, &yymsp[-8].minor.yy392, &yymsp[-7].minor.yy447, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy193, &yymsp[0].minor.yy482, &yymsp[-1].minor.yy482);
}
  yymsp[-12].minor.yy288 = yylhsminor.yy288;
        break;
      case 160: /* union ::= select */
{ yylhsminor.yy21 = setSubclause(NULL, yymsp[0].minor.yy288); }
  yymsp[0].minor.yy21 = yylhsminor.yy21;
        break;
      case 161: /* union ::= LP union RP */
{ yymsp[-2].minor.yy21 = yymsp[-1].minor.yy21; }
        break;
      case 162: /* union ::= union UNION ALL select */
{ yylhsminor.yy21 = appendSelectClause(yymsp[-3].minor.yy21, yymsp[0].minor.yy288); }
  yymsp[-3].minor.yy21 = yylhsminor.yy21;
        break;
      case 163: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy21 = appendSelectClause(yymsp[-5].minor.yy21, yymsp[-1].minor.yy288); }
  yymsp[-5].minor.yy21 = yylhsminor.yy21;
        break;
      case 164: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy21, NULL, TSDB_SQL_SELECT); }
        break;
      case 165: /* select ::= SELECT selcollist */
{
  yylhsminor.yy288 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy178, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy288 = yylhsminor.yy288;
        break;
      case 166: /* sclp ::= selcollist COMMA */
{yylhsminor.yy178 = yymsp[-1].minor.yy178;}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 167: /* sclp ::= */
{yymsp[1].minor.yy178 = 0;}
        break;
      case 168: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy178 = tSqlExprListAppend(yymsp[-3].minor.yy178, yymsp[-1].minor.yy134,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 169: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy178 = tSqlExprListAppend(yymsp[-1].minor.yy178, pNode, 0, 0);
}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 170: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 171: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 173: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 175: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 176: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy193 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy193 = tVariantListAppendToken(yylhsminor.yy193, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy193 = yylhsminor.yy193;
        break;
      case 177: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy193 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy193 = tVariantListAppendToken(yylhsminor.yy193, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 178: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy193 = tVariantListAppendToken(yymsp[-3].minor.yy193, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy193 = tVariantListAppendToken(yylhsminor.yy193, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy193 = yylhsminor.yy193;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy193 = tVariantListAppendToken(yymsp[-4].minor.yy193, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy193 = tVariantListAppendToken(yylhsminor.yy193, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy193 = yylhsminor.yy193;
        break;
      case 180: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy392.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy392.offset.n = 0;}
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy392.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy392.offset = yymsp[-1].minor.yy0;}
        break;
      case 183: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy392, 0, sizeof(yymsp[1].minor.yy392));}
        break;
      case 184: /* session_option ::= */
{yymsp[1].minor.yy447.col.n = 0; yymsp[1].minor.yy447.gap.n = 0;}
        break;
      case 185: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy447.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy447.gap = yymsp[-1].minor.yy0;
}
        break;
      case 186: /* fill_opt ::= */
{ yymsp[1].minor.yy193 = 0;     }
        break;
      case 187: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy193, &A, -1, 0);
    yymsp[-5].minor.yy193 = yymsp[-1].minor.yy193;
}
        break;
      case 188: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy193 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 189: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 190: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 191: /* orderby_opt ::= */
{yymsp[1].minor.yy193 = 0;}
        break;
      case 192: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 193: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy193 = tVariantListAppend(yymsp[-3].minor.yy193, &yymsp[-1].minor.yy442, yymsp[0].minor.yy312);
}
  yymsp[-3].minor.yy193 = yylhsminor.yy193;
        break;
      case 194: /* sortlist ::= item sortorder */
{
  yylhsminor.yy193 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy312);
}
  yymsp[-1].minor.yy193 = yylhsminor.yy193;
        break;
      case 195: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 196: /* sortorder ::= ASC */
{ yymsp[0].minor.yy312 = TSDB_ORDER_ASC; }
        break;
      case 197: /* sortorder ::= DESC */
{ yymsp[0].minor.yy312 = TSDB_ORDER_DESC;}
        break;
      case 198: /* sortorder ::= */
{ yymsp[1].minor.yy312 = TSDB_ORDER_ASC; }
        break;
      case 199: /* groupby_opt ::= */
{ yymsp[1].minor.yy193 = 0;}
        break;
      case 200: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 201: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy193 = tVariantListAppend(yymsp[-2].minor.yy193, &yymsp[0].minor.yy442, -1);
}
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 202: /* grouplist ::= item */
{
  yylhsminor.yy193 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 203: /* having_opt ::= */
      case 213: /* where_opt ::= */ yytestcase(yyruleno==213);
      case 252: /* expritem ::= */ yytestcase(yyruleno==252);
{yymsp[1].minor.yy134 = 0;}
        break;
      case 204: /* having_opt ::= HAVING expr */
      case 214: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==214);
{yymsp[-1].minor.yy134 = yymsp[0].minor.yy134;}
        break;
      case 205: /* limit_opt ::= */
      case 209: /* slimit_opt ::= */ yytestcase(yyruleno==209);
{yymsp[1].minor.yy482.limit = -1; yymsp[1].minor.yy482.offset = 0;}
        break;
      case 206: /* limit_opt ::= LIMIT signed */
      case 210: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==210);
{yymsp[-1].minor.yy482.limit = yymsp[0].minor.yy473;  yymsp[-1].minor.yy482.offset = 0;}
        break;
      case 207: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy482.limit = yymsp[-2].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[0].minor.yy473;}
        break;
      case 208: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy482.limit = yymsp[0].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[-2].minor.yy473;}
        break;
      case 211: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy482.limit = yymsp[-2].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[0].minor.yy473;}
        break;
      case 212: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy482.limit = yymsp[0].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[-2].minor.yy473;}
        break;
      case 215: /* expr ::= LP expr RP */
{yylhsminor.yy134 = yymsp[-1].minor.yy134; yylhsminor.yy134->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy134->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 216: /* expr ::= ID */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 217: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 218: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 219: /* expr ::= INTEGER */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 220: /* expr ::= MINUS INTEGER */
      case 221: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==221);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 222: /* expr ::= FLOAT */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 223: /* expr ::= MINUS FLOAT */
      case 224: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==224);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 225: /* expr ::= STRING */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 226: /* expr ::= NOW */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 227: /* expr ::= VARIABLE */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 228: /* expr ::= BOOL */
{ yylhsminor.yy134 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 229: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy134 = tSqlExprCreateFunction(yymsp[-1].minor.yy178, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy134 = yylhsminor.yy134;
        break;
      case 230: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy134 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy134 = yylhsminor.yy134;
        break;
      case 231: /* expr ::= expr IS NULL */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 232: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-3].minor.yy134, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy134 = yylhsminor.yy134;
        break;
      case 233: /* expr ::= expr LT expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_LT);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 234: /* expr ::= expr GT expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_GT);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 235: /* expr ::= expr LE expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_LE);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 236: /* expr ::= expr GE expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_GE);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 237: /* expr ::= expr NE expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_NE);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 238: /* expr ::= expr EQ expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_EQ);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 239: /* expr ::= expr BETWEEN expr AND expr */
{ tSQLExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy134); yylhsminor.yy134 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy134, yymsp[-2].minor.yy134, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy134, TK_LE), TK_AND);}
  yymsp[-4].minor.yy134 = yylhsminor.yy134;
        break;
      case 240: /* expr ::= expr AND expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_AND);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 241: /* expr ::= expr OR expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_OR); }
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 242: /* expr ::= expr PLUS expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_PLUS);  }
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 243: /* expr ::= expr MINUS expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_MINUS); }
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 244: /* expr ::= expr STAR expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_STAR);  }
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 245: /* expr ::= expr SLASH expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_DIVIDE);}
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 246: /* expr ::= expr REM expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_REM);   }
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 247: /* expr ::= expr LIKE expr */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-2].minor.yy134, yymsp[0].minor.yy134, TK_LIKE);  }
  yymsp[-2].minor.yy134 = yylhsminor.yy134;
        break;
      case 248: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy134 = tSqlExprCreate(yymsp[-4].minor.yy134, (tSQLExpr*)yymsp[-1].minor.yy178, TK_IN); }
  yymsp[-4].minor.yy134 = yylhsminor.yy134;
        break;
      case 249: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy178 = tSqlExprListAppend(yymsp[-2].minor.yy178,yymsp[0].minor.yy134,0, 0);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 250: /* exprlist ::= expritem */
{yylhsminor.yy178 = tSqlExprListAppend(0,yymsp[0].minor.yy134,0, 0);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 251: /* expritem ::= expr */
{yylhsminor.yy134 = yymsp[0].minor.yy134;}
  yymsp[0].minor.yy134 = yylhsminor.yy134;
        break;
      case 253: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 254: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 266: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 267: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfoLhs)/sizeof(yyRuleInfoLhs[0]) );
  yygoto = yyRuleInfoLhs[yyruleno];
  yysize = yyRuleInfoNRhs[yyruleno];
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
  return yyact;
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH
  ParseCTX_FETCH
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
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
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
  ParseARG_FETCH
  ParseCTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/

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
/************ End %syntax_error code ******************************************/
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH
  ParseCTX_FETCH
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
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
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
  YYACTIONTYPE yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser = (yyParser*)yyp;  /* The parser */
  ParseCTX_FETCH
  ParseARG_STORE

  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif

  yyact = yypParser->yytos->stateno;
#ifndef NDEBUG
  if( yyTraceFILE ){
    if( yyact < YY_MIN_REDUCE ){
      fprintf(yyTraceFILE,"%sInput '%s' in state %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact);
    }else{
      fprintf(yyTraceFILE,"%sInput '%s' with pending reduce %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact-YY_MIN_REDUCE);
    }
  }
#endif

  do{
    assert( yyact==yypParser->yytos->stateno );
    yyact = yy_find_shift_action((YYCODETYPE)yymajor,yyact);
    if( yyact >= YY_MIN_REDUCE ){
      yyact = yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,
                        yyminor ParseCTX_PARAM);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,(YYCODETYPE)yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      break;
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
            && (yyact = yy_find_reduce_action(
                        yypParser->yytos->stateno,
                        YYERRORSYMBOL)) > YY_MAX_SHIFTREDUCE
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
      if( yymajor==YYNOCODE ) break;
      yyact = yypParser->yytos->stateno;
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
      break;
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
      break;
#endif
    }
  }while( yypParser->yytos>yypParser->yystack );
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

/*
** Return the fallback token corresponding to canonical token iToken, or
** 0 if iToken has no fallback.
*/
int ParseFallback(int iToken){
#ifdef YYFALLBACK
  assert( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) );
  return yyFallback[iToken];
#else
  (void)iToken;
  return 0;
#endif
}
