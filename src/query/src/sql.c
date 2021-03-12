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
#define YYNOCODE 289
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy34;
  tVariant yy54;
  int64_t yy55;
  SIntervalVal yy102;
  SCreateTableSQL* yy144;
  SCreateAcctInfo yy205;
  SArray* yy209;
  tSQLExprList* yy246;
  tSQLExpr* yy254;
  int yy332;
  TAOS_FIELD yy369;
  SSubclauseInfo* yy437;
  SLimitVal yy534;
  SQuerySQL* yy540;
  SCreateDbInfo yy560;
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
#define YYNSTATE             310
#define YYNRULE              266
#define YYNRULE_WITH_ACTION  266
#define YYNTOKEN             215
#define YY_MAX_SHIFT         309
#define YY_MIN_SHIFTREDUCE   501
#define YY_MAX_SHIFTREDUCE   766
#define YY_ERROR_ACTION      767
#define YY_ACCEPT_ACTION     768
#define YY_NO_ACTION         769
#define YY_MIN_REDUCE        770
#define YY_MAX_REDUCE        1035
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
#define YY_ACTTAB_COUNT (674)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   224,  550,  941,  550,  200,  307,  905,   17,  817,  551,
 /*    10 */    78,  551,  164,   47,   48,  177,   51,   52,  138,  179,
 /*    20 */   212,   41,  179,   50,  256,   55,   53,   57,   54, 1017,
 /*    30 */    30,  207, 1018,   46,   45,  917,  179,   44,   43,   42,
 /*    40 */   183,  902,  903,   29,  906,  206, 1018,  502,  503,  504,
 /*    50 */   505,  506,  507,  508,  509,  510,  511,  512,  513,  514,
 /*    60 */   515,  308,  550,  938,  229,   47,   48,  930,   51,   52,
 /*    70 */   551,  202,  212,   41,  916,   50,  256,   55,   53,   57,
 /*    80 */    54,  201,  253,   70,   75,   46,   45,  273,  204,   44,
 /*    90 */    43,   42,   47,   48,  273,   51,   52,  148,  217,  212,
 /*   100 */    41,  133,   50,  256,   55,   53,   57,   54,  630,  219,
 /*   110 */   768,  309,   46,   45,  919,  293,   44,   43,   42,   47,
 /*   120 */    49,   12,   51,   52,  919,   80,  212,   41,  303,   50,
 /*   130 */   256,   55,   53,   57,   54,  919,  283,  282,  887,   46,
 /*   140 */    45,  885,  886,   44,   43,   42,  888,  826,  890,  891,
 /*   150 */   889,  164,  892,  893,  919,   48,  930,   51,   52,  138,
 /*   160 */   714,  212,   41,  244,   50,  256,   55,   53,   57,   54,
 /*   170 */   239,   44,   43,   42,   46,   45,  913,  138,   44,   43,
 /*   180 */    42,   23,  271,  302,  301,  270,  269,  268,  300,  267,
 /*   190 */   299,  298,  297,  266,  296,  295,  879,   81,  867,  868,
 /*   200 */   869,  870,  871,  872,  873,  874,  875,  876,  877,  878,
 /*   210 */   880,  881,   51,   52,   18,  210,  212,   41,  218,   50,
 /*   220 */   256,   55,   53,   57,   54,  970,   67,  251,  629,   46,
 /*   230 */    45,  907,  187,   44,   43,   42,  211,  727,  189,   77,
 /*   240 */   718, 1014,  721,  969,  724,  116,  115,  188,  138,  211,
 /*   250 */   727,   71,   64,  718,  221,  721,   69,  724,   46,   45,
 /*   260 */    30,    6,   44,   43,   42,  103,  232,   30,  208,  209,
 /*   270 */    65,  293,  255,  236,  235, 1013,   23,   24,  302,  301,
 /*   280 */  1027,  208,  209,  300,   36,  299,  298,  297,  904,  296,
 /*   290 */   295,  670,  223,   30,   55,   53,   57,   54,  306,  305,
 /*   300 */   125,  215,   46,   45,  916,  238,   44,   43,   42,    5,
 /*   310 */   154,  915,  195,  257,   76,   33,  153,   85,   90,   83,
 /*   320 */    89,  101,  106,  222,    1,  152,  275,   95,  105,   30,
 /*   330 */   111,  114,  104,  667,  216,   30,   30,  916,  108,   25,
 /*   340 */   172,  168,   56,    3,  165,   30,  170,  167,  120,  119,
 /*   350 */   118,  117,   24,  654,  726,   56,  651,  918,  652,   36,
 /*   360 */   653,  225,  818, 1012,  280,  279,  164,  726,  131,  725,
 /*   370 */   276,  674,  716,  916,  662,   36,  277,  281,  196,  916,
 /*   380 */   916,  241,  725,  242,  226,  227,  285,   31,  682,  916,
 /*   390 */   695,  696,  135,  686,  687,  747,  728,   60,   20,   19,
 /*   400 */    19,  720,  719,  723,  722,   61,   94,   93,  717,  640,
 /*   410 */   259,  730,  642,   31,   31,  261,   60,  641,  962,   79,
 /*   420 */    28,   60,  658,  262,  659,   62,  197,   14,   13,  100,
 /*   430 */    99,  181,   16,   15,  656,  182,  657,  113,  112,  130,
 /*   440 */   128,  184,  178,  185,  186,  192,  193,  191,  980,  176,
 /*   450 */   190,  180,  979,  213,  976,  975,  214,  284,  932,  132,
 /*   460 */   940,   39,  947,  961,  655,  949,   36,  134,  149,  147,
 /*   470 */   912,  150,  240,  151,  830,  264,  129,  829,  265,  681,
 /*   480 */    37,  174,   34,   66,  929,  274,  245,  203,  825, 1032,
 /*   490 */    91, 1031, 1029,  249,  155,   63,  278, 1026,   58,   97,
 /*   500 */  1025, 1023,  156,  139,  140,  254,  252,  141,  848,  142,
 /*   510 */    35,  250,  143,   32,   38,  175,  248,  814,  107,  144,
 /*   520 */   246,  812,  294,  109,  110,   40,  810,  102,  809,  228,
 /*   530 */   166,  286,  807,  806,  805,  804,  803,  802,  801,  169,
 /*   540 */   171,  798,  796,  794,  792,  790,  173,  287,  243,   72,
 /*   550 */    73,  963,  288,  289,  290,  198,  220,  263,  291,  292,
 /*   560 */   304,  199,  194,  766,   86,   87,  230,  808,  231,  765,
 /*   570 */   233,  234,  764,  752,  237,  241,  159,  121,  158,  849,
 /*   580 */   157,  160,  122,  161,  163,  800,  162,  123,  799,    4,
 /*   590 */     2,  883,  124,  791,  664,  914,   68,  258,    8,  205,
 /*   600 */   145,  146,  895,  683,   74,  136,   26,  247,    9,  688,
 /*   610 */   137,    7,   10,  729,   27,   11,   21,  260,   22,  731,
 /*   620 */    82,   80,   84,  593,  589,  587,  586,  585,  582,  554,
 /*   630 */   272,   88,   92,   31,   59,  632,   96,  631,  628,  577,
 /*   640 */   575,  567,  573,  569,  571,  565,  563,  596,  595,  594,
 /*   650 */   592,   98,  591,  590,  588,  584,  583,   60,  552,  770,
 /*   660 */   519,  517,  769,  769,  769,  769,  769,  126,  769,  769,
 /*   670 */   769,  769,  769,  127,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   218,    1,  218,    1,  217,  218,    0,  278,  223,    9,
 /*    10 */   224,    9,  227,   13,   14,  278,   16,   17,  218,  278,
 /*    20 */    20,   21,  278,   23,   24,   25,   26,   27,   28,  288,
 /*    30 */   218,  287,  288,   33,   34,  253,  278,   37,   38,   39,
 /*    40 */   278,  255,  256,  257,  258,  287,  288,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,    1,  279,   62,   13,   14,  261,   16,   17,
 /*    70 */     9,  259,   20,   21,  262,   23,   24,   25,   26,   27,
 /*    80 */    28,  275,  282,   83,  284,   33,   34,   81,  237,   37,
 /*    90 */    38,   39,   13,   14,   81,   16,   17,   83,  237,   20,
 /*   100 */    21,  218,   23,   24,   25,   26,   27,   28,    5,  237,
 /*   110 */   215,  216,   33,   34,  263,   84,   37,   38,   39,   13,
 /*   120 */    14,  107,   16,   17,  263,  111,   20,   21,  237,   23,
 /*   130 */    24,   25,   26,   27,   28,  263,   33,   34,  236,   33,
 /*   140 */    34,  239,  240,   37,   38,   39,  244,  223,  246,  247,
 /*   150 */   248,  227,  250,  251,  263,   14,  261,   16,   17,  218,
 /*   160 */   108,   20,   21,  280,   23,   24,   25,   26,   27,   28,
 /*   170 */   275,   37,   38,   39,   33,   34,  218,  218,   37,   38,
 /*   180 */    39,   91,   92,   93,   94,   95,   96,   97,   98,   99,
 /*   190 */   100,  101,  102,  103,  104,  105,  236,  224,  238,  239,
 /*   200 */   240,  241,  242,  243,  244,  245,  246,  247,  248,  249,
 /*   210 */   250,  251,   16,   17,   44,   61,   20,   21,  260,   23,
 /*   220 */    24,   25,   26,   27,   28,  284,  107,  286,  109,   33,
 /*   230 */    34,  258,   62,   37,   38,   39,    1,    2,   68,  264,
 /*   240 */     5,  278,    7,  284,    9,   75,   76,   77,  218,    1,
 /*   250 */     2,  276,  112,    5,   68,    7,  224,    9,   33,   34,
 /*   260 */   218,  107,   37,   38,   39,   78,  136,  218,   33,   34,
 /*   270 */   130,   84,   37,  143,  144,  278,   91,  107,   93,   94,
 /*   280 */   263,   33,   34,   98,  114,  100,  101,  102,  256,  104,
 /*   290 */   105,   37,   68,  218,   25,   26,   27,   28,   65,   66,
 /*   300 */    67,  259,   33,   34,  262,  135,   37,   38,   39,   63,
 /*   310 */    64,  262,  142,   15,  284,   69,   70,   71,   72,   73,
 /*   320 */    74,   63,   64,  137,  225,  226,  140,   69,   70,  218,
 /*   330 */    72,   73,   74,  112,  259,  218,  218,  262,   80,  118,
 /*   340 */    63,   64,  107,  221,  222,  218,   69,   70,   71,   72,
 /*   350 */    73,   74,  107,    2,  119,  107,    5,  263,    7,  114,
 /*   360 */     9,  137,  223,  278,  140,  141,  227,  119,  107,  134,
 /*   370 */   259,  117,    1,  262,  108,  114,  259,  259,  278,  262,
 /*   380 */   262,  115,  134,  108,   33,   34,  259,  112,  108,  262,
 /*   390 */   125,  126,  112,  108,  108,  108,  108,  112,  112,  112,
 /*   400 */   112,    5,    5,    7,    7,  112,  138,  139,   37,  108,
 /*   410 */   108,  113,  108,  112,  112,  108,  112,  108,  285,  112,
 /*   420 */   107,  112,    5,  110,    7,  132,  278,  138,  139,  138,
 /*   430 */   139,  278,  138,  139,    5,  278,    7,   78,   79,   63,
 /*   440 */    64,  278,  278,  278,  278,  278,  278,  278,  254,  278,
 /*   450 */   278,  278,  254,  254,  254,  254,  254,  254,  261,  218,
 /*   460 */   218,  277,  218,  285,  113,  218,  114,  218,  218,  265,
 /*   470 */   218,  218,  261,  218,  218,  218,   61,  218,  218,  119,
 /*   480 */   218,  218,  218,  129,  274,  218,  281,  281,  218,  218,
 /*   490 */   218,  218,  218,  281,  218,  131,  218,  218,  128,  218,
 /*   500 */   218,  218,  218,  273,  272,  123,  127,  271,  218,  270,
 /*   510 */   218,  122,  269,  218,  218,  218,  121,  218,  218,  268,
 /*   520 */   120,  218,  106,  218,  218,  133,  218,   90,  218,  218,
 /*   530 */   218,   89,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   540 */   218,  218,  218,  218,  218,  218,  218,   51,  219,  219,
 /*   550 */   219,  219,   86,   88,   55,  219,  219,  219,   87,   85,
 /*   560 */    81,  219,  219,    5,  224,  224,  145,  219,    5,    5,
 /*   570 */   145,    5,    5,   92,  136,  115,  229,  220,  233,  235,
 /*   580 */   234,  232,  220,  230,  228,  219,  231,  220,  219,  221,
 /*   590 */   225,  252,  220,  219,  108,  261,  116,  110,  107,    1,
 /*   600 */   267,  266,  252,  108,  112,  107,  112,  107,  124,  108,
 /*   610 */   107,  107,  124,  108,  112,  107,  107,  110,  107,  113,
 /*   620 */    78,  111,   83,    9,    5,    5,    5,    5,    5,   82,
 /*   630 */    15,   78,  139,  112,   16,    5,  139,    5,  108,    5,
 /*   640 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   650 */     5,  139,    5,    5,    5,    5,    5,  112,   82,    0,
 /*   660 */    61,   60,  289,  289,  289,  289,  289,   21,  289,  289,
 /*   670 */   289,  289,  289,   21,  289,  289,  289,  289,  289,  289,
 /*   680 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   690 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   700 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   710 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   720 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   730 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   740 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   750 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   760 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   770 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   780 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   790 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   800 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   810 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   820 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   830 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   840 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   850 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   860 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   870 */   289,  289,  289,  289,  289,  289,  289,  289,  289,  289,
 /*   880 */   289,  289,  289,  289,  289,  289,  289,  289,  289,
};
#define YY_SHIFT_COUNT    (309)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (659)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   170,   90,   90,  185,  185,   13,  235,  248,   61,   61,
 /*    10 */    61,   61,   61,   61,   61,   61,   61,    0,    2,  248,
 /*    20 */   351,  351,  351,  351,  245,   61,   61,   61,   61,    6,
 /*    30 */    61,   61,  187,   13,   31,   31,  674,  674,  674,  248,
 /*    40 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  248,
 /*    50 */   248,  248,  248,  248,  248,  248,  248,  248,  248,  351,
 /*    60 */   351,  103,  103,  103,  103,  103,  103,  103,  261,   61,
 /*    70 */    61,  254,   61,   61,   61,  265,  265,  221,   61,   61,
 /*    80 */    61,   61,   61,   61,   61,   61,   61,   61,   61,   61,
 /*    90 */    61,   61,   61,   61,   61,   61,   61,   61,   61,   61,
 /*   100 */    61,   61,   61,   61,   61,   61,   61,   61,   61,   61,
 /*   110 */    61,   61,   61,   61,   61,   61,   61,   61,   61,   61,
 /*   120 */    61,   61,   61,   61,   61,   61,   61,   61,   61,   61,
 /*   130 */    61,  352,  415,  415,  415,  360,  360,  360,  415,  354,
 /*   140 */   364,  370,  382,  379,  389,  395,  400,  392,  352,  415,
 /*   150 */   415,  415,  416,   13,   13,  415,  415,  437,  442,  496,
 /*   160 */   466,  465,  499,  471,  474,  416,  415,  479,  479,  415,
 /*   170 */   479,  415,  479,  415,  674,  674,   52,   79,  106,   79,
 /*   180 */    79,  141,  196,  269,  269,  269,  269,  246,  258,  277,
 /*   190 */   225,  225,  225,  225,  224,  130,  134,  134,   14,  186,
 /*   200 */   233,  266,  275,  280,  285,  286,  287,  288,  396,  397,
 /*   210 */   371,  154,  298,  293,  140,  301,  302,  304,  307,  309,
 /*   220 */   313,  268,  289,  291,  119,  294,  417,  429,  359,  376,
 /*   230 */   558,  421,  563,  564,  425,  566,  567,  481,  438,  460,
 /*   240 */   486,  480,  487,  491,  492,  495,  498,  598,  500,  501,
 /*   250 */   503,  494,  484,  502,  488,  505,  504,  506,  508,  487,
 /*   260 */   509,  507,  511,  510,  542,  539,  614,  619,  620,  621,
 /*   270 */   622,  623,  547,  615,  553,  493,  521,  521,  618,  497,
 /*   280 */   512,  521,  630,  632,  530,  521,  634,  635,  636,  637,
 /*   290 */   638,  639,  640,  641,  642,  643,  644,  645,  647,  648,
 /*   300 */   649,  650,  651,  545,  576,  646,  652,  599,  601,  659,
};
#define YY_REDUCE_COUNT (175)
#define YY_REDUCE_MIN   (-271)
#define YY_REDUCE_MAX   (374)
static const short yy_reduce_ofst[] = {
 /*     0 */  -105,  -40,  -40,  -98,  -98, -214, -256, -242, -188,  -59,
 /*    10 */  -200,   42,   75,  111,  117,  118,  127, -216, -213, -259,
 /*    20 */  -149, -139, -128, -109, -194, -117,  -41,   30,  -42,  -27,
 /*    30 */  -218,   49, -215,   32,  -76,  139,  -25,   99,  122, -271,
 /*    40 */  -263, -238,  -37,   -3,   85,  100,  148,  153,  157,  163,
 /*    50 */   164,  165,  166,  167,  168,  169,  171,  172,  173,   17,
 /*    60 */    94,  194,  198,  199,  200,  201,  202,  203,  197,  241,
 /*    70 */   242,  184,  244,  247,  249,  133,  178,  204,  250,  252,
 /*    80 */   253,  255,  256,  257,  259,  260,  262,  263,  264,  267,
 /*    90 */   270,  271,  272,  273,  274,  276,  278,  279,  281,  282,
 /*   100 */   283,  284,  290,  292,  295,  296,  297,  299,  300,  303,
 /*   110 */   305,  306,  308,  310,  311,  312,  314,  315,  316,  317,
 /*   120 */   318,  319,  320,  321,  322,  323,  324,  325,  326,  327,
 /*   130 */   328,  211,  329,  330,  331,  205,  206,  212,  332,  210,
 /*   140 */   230,  232,  236,  239,  243,  251,  333,  335,  334,  336,
 /*   150 */   337,  338,  339,  340,  341,  342,  343,  344,  346,  345,
 /*   160 */   347,  349,  353,  355,  356,  350,  348,  357,  362,  366,
 /*   170 */   367,  369,  372,  374,  365,  368,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   767,  882,  827,  894,  815,  824, 1020, 1020,  767,  767,
 /*    10 */   767,  767,  767,  767,  767,  767,  767,  942,  787, 1020,
 /*    20 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  824,
 /*    30 */   767,  767,  831,  824,  831,  831,  937,  866,  884,  767,
 /*    40 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    50 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    60 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    70 */   767,  944,  946,  948,  767,  966,  966,  935,  767,  767,
 /*    80 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*    90 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   100 */   767,  767,  767,  767,  767,  767,  767,  813,  767,  811,
 /*   110 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   120 */   767,  767,  767,  767,  767,  797,  767,  767,  767,  767,
 /*   130 */   767,  767,  789,  789,  789,  767,  767,  767,  789,  973,
 /*   140 */   977,  971,  959,  967,  958,  954,  953,  981,  767,  789,
 /*   150 */   789,  789,  828,  824,  824,  789,  789,  847,  845,  843,
 /*   160 */   835,  841,  837,  839,  833,  816,  789,  822,  822,  789,
 /*   170 */   822,  789,  822,  789,  866,  884,  767,  982,  767, 1019,
 /*   180 */   972, 1009, 1008, 1015, 1007, 1006, 1005,  767,  767,  767,
 /*   190 */  1001, 1002, 1004, 1003,  767,  767, 1011, 1010,  767,  767,
 /*   200 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   210 */   767,  984,  767,  978,  974,  767,  767,  767,  767,  767,
 /*   220 */   767,  767,  767,  767,  896,  767,  767,  767,  767,  767,
 /*   230 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  934,
 /*   240 */   767,  767,  767,  767,  945,  767,  767,  767,  767,  767,
 /*   250 */   767,  968,  767,  960,  767,  767,  767,  767,  767,  908,
 /*   260 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   270 */   767,  767,  767,  767,  767,  767, 1030, 1028,  767,  767,
 /*   280 */   767, 1024,  767,  767,  767, 1022,  767,  767,  767,  767,
 /*   290 */   767,  767,  767,  767,  767,  767,  767,  767,  767,  767,
 /*   300 */   767,  767,  767,  850,  767,  795,  793,  767,  785,  767,
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
  /*   84 */ "PPS",
  /*   85 */ "TSERIES",
  /*   86 */ "DBS",
  /*   87 */ "STORAGE",
  /*   88 */ "QTIME",
  /*   89 */ "CONNS",
  /*   90 */ "STATE",
  /*   91 */ "KEEP",
  /*   92 */ "CACHE",
  /*   93 */ "REPLICA",
  /*   94 */ "QUORUM",
  /*   95 */ "DAYS",
  /*   96 */ "MINROWS",
  /*   97 */ "MAXROWS",
  /*   98 */ "BLOCKS",
  /*   99 */ "CTIME",
  /*  100 */ "WAL",
  /*  101 */ "FSYNC",
  /*  102 */ "COMP",
  /*  103 */ "PRECISION",
  /*  104 */ "UPDATE",
  /*  105 */ "CACHELAST",
  /*  106 */ "PARTITIONS",
  /*  107 */ "LP",
  /*  108 */ "RP",
  /*  109 */ "UNSIGNED",
  /*  110 */ "TAGS",
  /*  111 */ "USING",
  /*  112 */ "COMMA",
  /*  113 */ "NULL",
  /*  114 */ "SELECT",
  /*  115 */ "UNION",
  /*  116 */ "ALL",
  /*  117 */ "DISTINCT",
  /*  118 */ "FROM",
  /*  119 */ "VARIABLE",
  /*  120 */ "INTERVAL",
  /*  121 */ "FILL",
  /*  122 */ "SLIDING",
  /*  123 */ "ORDER",
  /*  124 */ "BY",
  /*  125 */ "ASC",
  /*  126 */ "DESC",
  /*  127 */ "GROUP",
  /*  128 */ "HAVING",
  /*  129 */ "LIMIT",
  /*  130 */ "OFFSET",
  /*  131 */ "SLIMIT",
  /*  132 */ "SOFFSET",
  /*  133 */ "WHERE",
  /*  134 */ "NOW",
  /*  135 */ "RESET",
  /*  136 */ "QUERY",
  /*  137 */ "ADD",
  /*  138 */ "COLUMN",
  /*  139 */ "TAG",
  /*  140 */ "CHANGE",
  /*  141 */ "SET",
  /*  142 */ "KILL",
  /*  143 */ "CONNECTION",
  /*  144 */ "STREAM",
  /*  145 */ "COLON",
  /*  146 */ "ABORT",
  /*  147 */ "AFTER",
  /*  148 */ "ATTACH",
  /*  149 */ "BEFORE",
  /*  150 */ "BEGIN",
  /*  151 */ "CASCADE",
  /*  152 */ "CLUSTER",
  /*  153 */ "CONFLICT",
  /*  154 */ "COPY",
  /*  155 */ "DEFERRED",
  /*  156 */ "DELIMITERS",
  /*  157 */ "DETACH",
  /*  158 */ "EACH",
  /*  159 */ "END",
  /*  160 */ "EXPLAIN",
  /*  161 */ "FAIL",
  /*  162 */ "FOR",
  /*  163 */ "IGNORE",
  /*  164 */ "IMMEDIATE",
  /*  165 */ "INITIALLY",
  /*  166 */ "INSTEAD",
  /*  167 */ "MATCH",
  /*  168 */ "KEY",
  /*  169 */ "OF",
  /*  170 */ "RAISE",
  /*  171 */ "REPLACE",
  /*  172 */ "RESTRICT",
  /*  173 */ "ROW",
  /*  174 */ "STATEMENT",
  /*  175 */ "TRIGGER",
  /*  176 */ "VIEW",
  /*  177 */ "COUNT",
  /*  178 */ "SUM",
  /*  179 */ "AVG",
  /*  180 */ "MIN",
  /*  181 */ "MAX",
  /*  182 */ "FIRST",
  /*  183 */ "LAST",
  /*  184 */ "TOP",
  /*  185 */ "BOTTOM",
  /*  186 */ "STDDEV",
  /*  187 */ "PERCENTILE",
  /*  188 */ "APERCENTILE",
  /*  189 */ "LEASTSQUARES",
  /*  190 */ "HISTOGRAM",
  /*  191 */ "DIFF",
  /*  192 */ "SPREAD",
  /*  193 */ "TWA",
  /*  194 */ "INTERP",
  /*  195 */ "LAST_ROW",
  /*  196 */ "RATE",
  /*  197 */ "IRATE",
  /*  198 */ "SUM_RATE",
  /*  199 */ "SUM_IRATE",
  /*  200 */ "AVG_RATE",
  /*  201 */ "AVG_IRATE",
  /*  202 */ "TBID",
  /*  203 */ "SEMI",
  /*  204 */ "NONE",
  /*  205 */ "PREV",
  /*  206 */ "LINEAR",
  /*  207 */ "IMPORT",
  /*  208 */ "METRIC",
  /*  209 */ "TBNAME",
  /*  210 */ "JOIN",
  /*  211 */ "METRICS",
  /*  212 */ "INSERT",
  /*  213 */ "INTO",
  /*  214 */ "VALUES",
  /*  215 */ "program",
  /*  216 */ "cmd",
  /*  217 */ "dbPrefix",
  /*  218 */ "ids",
  /*  219 */ "cpxName",
  /*  220 */ "ifexists",
  /*  221 */ "alter_db_optr",
  /*  222 */ "alter_topic_optr",
  /*  223 */ "acct_optr",
  /*  224 */ "ifnotexists",
  /*  225 */ "db_optr",
  /*  226 */ "topic_optr",
  /*  227 */ "pps",
  /*  228 */ "tseries",
  /*  229 */ "dbs",
  /*  230 */ "streams",
  /*  231 */ "storage",
  /*  232 */ "qtime",
  /*  233 */ "users",
  /*  234 */ "conns",
  /*  235 */ "state",
  /*  236 */ "keep",
  /*  237 */ "tagitemlist",
  /*  238 */ "cache",
  /*  239 */ "replica",
  /*  240 */ "quorum",
  /*  241 */ "days",
  /*  242 */ "minrows",
  /*  243 */ "maxrows",
  /*  244 */ "blocks",
  /*  245 */ "ctime",
  /*  246 */ "wal",
  /*  247 */ "fsync",
  /*  248 */ "comp",
  /*  249 */ "prec",
  /*  250 */ "update",
  /*  251 */ "cachelast",
  /*  252 */ "partitions",
  /*  253 */ "typename",
  /*  254 */ "signed",
  /*  255 */ "create_table_args",
  /*  256 */ "create_stable_args",
  /*  257 */ "create_table_list",
  /*  258 */ "create_from_stable",
  /*  259 */ "columnlist",
  /*  260 */ "tagNamelist",
  /*  261 */ "select",
  /*  262 */ "column",
  /*  263 */ "tagitem",
  /*  264 */ "selcollist",
  /*  265 */ "from",
  /*  266 */ "where_opt",
  /*  267 */ "interval_opt",
  /*  268 */ "fill_opt",
  /*  269 */ "sliding_opt",
  /*  270 */ "groupby_opt",
  /*  271 */ "orderby_opt",
  /*  272 */ "having_opt",
  /*  273 */ "slimit_opt",
  /*  274 */ "limit_opt",
  /*  275 */ "union",
  /*  276 */ "sclp",
  /*  277 */ "distinct",
  /*  278 */ "expr",
  /*  279 */ "as",
  /*  280 */ "tablelist",
  /*  281 */ "tmvar",
  /*  282 */ "sortlist",
  /*  283 */ "sortitem",
  /*  284 */ "item",
  /*  285 */ "sortorder",
  /*  286 */ "grouplist",
  /*  287 */ "exprlist",
  /*  288 */ "expritem",
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
 /*  59 */ "cmd ::= CREATE FUNCTION ids AS ids",
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
 /* 159 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
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
 /* 184 */ "fill_opt ::=",
 /* 185 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 186 */ "fill_opt ::= FILL LP ID RP",
 /* 187 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 188 */ "sliding_opt ::=",
 /* 189 */ "orderby_opt ::=",
 /* 190 */ "orderby_opt ::= ORDER BY sortlist",
 /* 191 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 192 */ "sortlist ::= item sortorder",
 /* 193 */ "item ::= ids cpxName",
 /* 194 */ "sortorder ::= ASC",
 /* 195 */ "sortorder ::= DESC",
 /* 196 */ "sortorder ::=",
 /* 197 */ "groupby_opt ::=",
 /* 198 */ "groupby_opt ::= GROUP BY grouplist",
 /* 199 */ "grouplist ::= grouplist COMMA item",
 /* 200 */ "grouplist ::= item",
 /* 201 */ "having_opt ::=",
 /* 202 */ "having_opt ::= HAVING expr",
 /* 203 */ "limit_opt ::=",
 /* 204 */ "limit_opt ::= LIMIT signed",
 /* 205 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 206 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 207 */ "slimit_opt ::=",
 /* 208 */ "slimit_opt ::= SLIMIT signed",
 /* 209 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 210 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 211 */ "where_opt ::=",
 /* 212 */ "where_opt ::= WHERE expr",
 /* 213 */ "expr ::= LP expr RP",
 /* 214 */ "expr ::= ID",
 /* 215 */ "expr ::= ID DOT ID",
 /* 216 */ "expr ::= ID DOT STAR",
 /* 217 */ "expr ::= INTEGER",
 /* 218 */ "expr ::= MINUS INTEGER",
 /* 219 */ "expr ::= PLUS INTEGER",
 /* 220 */ "expr ::= FLOAT",
 /* 221 */ "expr ::= MINUS FLOAT",
 /* 222 */ "expr ::= PLUS FLOAT",
 /* 223 */ "expr ::= STRING",
 /* 224 */ "expr ::= NOW",
 /* 225 */ "expr ::= VARIABLE",
 /* 226 */ "expr ::= BOOL",
 /* 227 */ "expr ::= ID LP exprlist RP",
 /* 228 */ "expr ::= ID LP STAR RP",
 /* 229 */ "expr ::= expr IS NULL",
 /* 230 */ "expr ::= expr IS NOT NULL",
 /* 231 */ "expr ::= expr LT expr",
 /* 232 */ "expr ::= expr GT expr",
 /* 233 */ "expr ::= expr LE expr",
 /* 234 */ "expr ::= expr GE expr",
 /* 235 */ "expr ::= expr NE expr",
 /* 236 */ "expr ::= expr EQ expr",
 /* 237 */ "expr ::= expr BETWEEN expr AND expr",
 /* 238 */ "expr ::= expr AND expr",
 /* 239 */ "expr ::= expr OR expr",
 /* 240 */ "expr ::= expr PLUS expr",
 /* 241 */ "expr ::= expr MINUS expr",
 /* 242 */ "expr ::= expr STAR expr",
 /* 243 */ "expr ::= expr SLASH expr",
 /* 244 */ "expr ::= expr REM expr",
 /* 245 */ "expr ::= expr LIKE expr",
 /* 246 */ "expr ::= expr IN LP exprlist RP",
 /* 247 */ "exprlist ::= exprlist COMMA expritem",
 /* 248 */ "exprlist ::= expritem",
 /* 249 */ "expritem ::= expr",
 /* 250 */ "expritem ::=",
 /* 251 */ "cmd ::= RESET QUERY CACHE",
 /* 252 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 253 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 254 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 255 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 258 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 259 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 260 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 261 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 262 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 263 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 264 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 265 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 236: /* keep */
    case 237: /* tagitemlist */
    case 259: /* columnlist */
    case 260: /* tagNamelist */
    case 268: /* fill_opt */
    case 270: /* groupby_opt */
    case 271: /* orderby_opt */
    case 282: /* sortlist */
    case 286: /* grouplist */
{
taosArrayDestroy((yypminor->yy209));
}
      break;
    case 257: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy144));
}
      break;
    case 261: /* select */
{
doDestroyQuerySql((yypminor->yy540));
}
      break;
    case 264: /* selcollist */
    case 276: /* sclp */
    case 287: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy246));
}
      break;
    case 266: /* where_opt */
    case 272: /* having_opt */
    case 278: /* expr */
    case 288: /* expritem */
{
tSqlExprDestroy((yypminor->yy254));
}
      break;
    case 275: /* union */
{
destroyAllSelectClause((yypminor->yy437));
}
      break;
    case 283: /* sortitem */
{
tVariantDestroy(&(yypminor->yy54));
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
   215,  /* (0) program ::= cmd */
   216,  /* (1) cmd ::= SHOW DATABASES */
   216,  /* (2) cmd ::= SHOW TOPICS */
   216,  /* (3) cmd ::= SHOW FUNCTIONS */
   216,  /* (4) cmd ::= SHOW MNODES */
   216,  /* (5) cmd ::= SHOW DNODES */
   216,  /* (6) cmd ::= SHOW ACCOUNTS */
   216,  /* (7) cmd ::= SHOW USERS */
   216,  /* (8) cmd ::= SHOW MODULES */
   216,  /* (9) cmd ::= SHOW QUERIES */
   216,  /* (10) cmd ::= SHOW CONNECTIONS */
   216,  /* (11) cmd ::= SHOW STREAMS */
   216,  /* (12) cmd ::= SHOW VARIABLES */
   216,  /* (13) cmd ::= SHOW SCORES */
   216,  /* (14) cmd ::= SHOW GRANTS */
   216,  /* (15) cmd ::= SHOW VNODES */
   216,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
   217,  /* (17) dbPrefix ::= */
   217,  /* (18) dbPrefix ::= ids DOT */
   219,  /* (19) cpxName ::= */
   219,  /* (20) cpxName ::= DOT ids */
   216,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   216,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   216,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   216,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   216,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   216,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   216,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   216,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   216,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   216,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   216,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   216,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   216,  /* (33) cmd ::= DROP FUNCTION ids */
   216,  /* (34) cmd ::= DROP DNODE ids */
   216,  /* (35) cmd ::= DROP USER ids */
   216,  /* (36) cmd ::= DROP ACCOUNT ids */
   216,  /* (37) cmd ::= USE ids */
   216,  /* (38) cmd ::= DESCRIBE ids cpxName */
   216,  /* (39) cmd ::= ALTER USER ids PASS ids */
   216,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   216,  /* (41) cmd ::= ALTER DNODE ids ids */
   216,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   216,  /* (43) cmd ::= ALTER LOCAL ids */
   216,  /* (44) cmd ::= ALTER LOCAL ids ids */
   216,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   216,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   216,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   216,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   218,  /* (49) ids ::= ID */
   218,  /* (50) ids ::= STRING */
   220,  /* (51) ifexists ::= IF EXISTS */
   220,  /* (52) ifexists ::= */
   224,  /* (53) ifnotexists ::= IF NOT EXISTS */
   224,  /* (54) ifnotexists ::= */
   216,  /* (55) cmd ::= CREATE DNODE ids */
   216,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   216,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   216,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   216,  /* (59) cmd ::= CREATE FUNCTION ids AS ids */
   216,  /* (60) cmd ::= CREATE USER ids PASS ids */
   227,  /* (61) pps ::= */
   227,  /* (62) pps ::= PPS INTEGER */
   228,  /* (63) tseries ::= */
   228,  /* (64) tseries ::= TSERIES INTEGER */
   229,  /* (65) dbs ::= */
   229,  /* (66) dbs ::= DBS INTEGER */
   230,  /* (67) streams ::= */
   230,  /* (68) streams ::= STREAMS INTEGER */
   231,  /* (69) storage ::= */
   231,  /* (70) storage ::= STORAGE INTEGER */
   232,  /* (71) qtime ::= */
   232,  /* (72) qtime ::= QTIME INTEGER */
   233,  /* (73) users ::= */
   233,  /* (74) users ::= USERS INTEGER */
   234,  /* (75) conns ::= */
   234,  /* (76) conns ::= CONNS INTEGER */
   235,  /* (77) state ::= */
   235,  /* (78) state ::= STATE ids */
   223,  /* (79) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   236,  /* (80) keep ::= KEEP tagitemlist */
   238,  /* (81) cache ::= CACHE INTEGER */
   239,  /* (82) replica ::= REPLICA INTEGER */
   240,  /* (83) quorum ::= QUORUM INTEGER */
   241,  /* (84) days ::= DAYS INTEGER */
   242,  /* (85) minrows ::= MINROWS INTEGER */
   243,  /* (86) maxrows ::= MAXROWS INTEGER */
   244,  /* (87) blocks ::= BLOCKS INTEGER */
   245,  /* (88) ctime ::= CTIME INTEGER */
   246,  /* (89) wal ::= WAL INTEGER */
   247,  /* (90) fsync ::= FSYNC INTEGER */
   248,  /* (91) comp ::= COMP INTEGER */
   249,  /* (92) prec ::= PRECISION STRING */
   250,  /* (93) update ::= UPDATE INTEGER */
   251,  /* (94) cachelast ::= CACHELAST INTEGER */
   252,  /* (95) partitions ::= PARTITIONS INTEGER */
   225,  /* (96) db_optr ::= */
   225,  /* (97) db_optr ::= db_optr cache */
   225,  /* (98) db_optr ::= db_optr replica */
   225,  /* (99) db_optr ::= db_optr quorum */
   225,  /* (100) db_optr ::= db_optr days */
   225,  /* (101) db_optr ::= db_optr minrows */
   225,  /* (102) db_optr ::= db_optr maxrows */
   225,  /* (103) db_optr ::= db_optr blocks */
   225,  /* (104) db_optr ::= db_optr ctime */
   225,  /* (105) db_optr ::= db_optr wal */
   225,  /* (106) db_optr ::= db_optr fsync */
   225,  /* (107) db_optr ::= db_optr comp */
   225,  /* (108) db_optr ::= db_optr prec */
   225,  /* (109) db_optr ::= db_optr keep */
   225,  /* (110) db_optr ::= db_optr update */
   225,  /* (111) db_optr ::= db_optr cachelast */
   226,  /* (112) topic_optr ::= db_optr */
   226,  /* (113) topic_optr ::= topic_optr partitions */
   221,  /* (114) alter_db_optr ::= */
   221,  /* (115) alter_db_optr ::= alter_db_optr replica */
   221,  /* (116) alter_db_optr ::= alter_db_optr quorum */
   221,  /* (117) alter_db_optr ::= alter_db_optr keep */
   221,  /* (118) alter_db_optr ::= alter_db_optr blocks */
   221,  /* (119) alter_db_optr ::= alter_db_optr comp */
   221,  /* (120) alter_db_optr ::= alter_db_optr wal */
   221,  /* (121) alter_db_optr ::= alter_db_optr fsync */
   221,  /* (122) alter_db_optr ::= alter_db_optr update */
   221,  /* (123) alter_db_optr ::= alter_db_optr cachelast */
   222,  /* (124) alter_topic_optr ::= alter_db_optr */
   222,  /* (125) alter_topic_optr ::= alter_topic_optr partitions */
   253,  /* (126) typename ::= ids */
   253,  /* (127) typename ::= ids LP signed RP */
   253,  /* (128) typename ::= ids UNSIGNED */
   254,  /* (129) signed ::= INTEGER */
   254,  /* (130) signed ::= PLUS INTEGER */
   254,  /* (131) signed ::= MINUS INTEGER */
   216,  /* (132) cmd ::= CREATE TABLE create_table_args */
   216,  /* (133) cmd ::= CREATE TABLE create_stable_args */
   216,  /* (134) cmd ::= CREATE STABLE create_stable_args */
   216,  /* (135) cmd ::= CREATE TABLE create_table_list */
   257,  /* (136) create_table_list ::= create_from_stable */
   257,  /* (137) create_table_list ::= create_table_list create_from_stable */
   255,  /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   256,  /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   258,  /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   258,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   260,  /* (142) tagNamelist ::= tagNamelist COMMA ids */
   260,  /* (143) tagNamelist ::= ids */
   255,  /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
   259,  /* (145) columnlist ::= columnlist COMMA column */
   259,  /* (146) columnlist ::= column */
   262,  /* (147) column ::= ids typename */
   237,  /* (148) tagitemlist ::= tagitemlist COMMA tagitem */
   237,  /* (149) tagitemlist ::= tagitem */
   263,  /* (150) tagitem ::= INTEGER */
   263,  /* (151) tagitem ::= FLOAT */
   263,  /* (152) tagitem ::= STRING */
   263,  /* (153) tagitem ::= BOOL */
   263,  /* (154) tagitem ::= NULL */
   263,  /* (155) tagitem ::= MINUS INTEGER */
   263,  /* (156) tagitem ::= MINUS FLOAT */
   263,  /* (157) tagitem ::= PLUS INTEGER */
   263,  /* (158) tagitem ::= PLUS FLOAT */
   261,  /* (159) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   275,  /* (160) union ::= select */
   275,  /* (161) union ::= LP union RP */
   275,  /* (162) union ::= union UNION ALL select */
   275,  /* (163) union ::= union UNION ALL LP select RP */
   216,  /* (164) cmd ::= union */
   261,  /* (165) select ::= SELECT selcollist */
   276,  /* (166) sclp ::= selcollist COMMA */
   276,  /* (167) sclp ::= */
   264,  /* (168) selcollist ::= sclp distinct expr as */
   264,  /* (169) selcollist ::= sclp STAR */
   279,  /* (170) as ::= AS ids */
   279,  /* (171) as ::= ids */
   279,  /* (172) as ::= */
   277,  /* (173) distinct ::= DISTINCT */
   277,  /* (174) distinct ::= */
   265,  /* (175) from ::= FROM tablelist */
   280,  /* (176) tablelist ::= ids cpxName */
   280,  /* (177) tablelist ::= ids cpxName ids */
   280,  /* (178) tablelist ::= tablelist COMMA ids cpxName */
   280,  /* (179) tablelist ::= tablelist COMMA ids cpxName ids */
   281,  /* (180) tmvar ::= VARIABLE */
   267,  /* (181) interval_opt ::= INTERVAL LP tmvar RP */
   267,  /* (182) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   267,  /* (183) interval_opt ::= */
   268,  /* (184) fill_opt ::= */
   268,  /* (185) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   268,  /* (186) fill_opt ::= FILL LP ID RP */
   269,  /* (187) sliding_opt ::= SLIDING LP tmvar RP */
   269,  /* (188) sliding_opt ::= */
   271,  /* (189) orderby_opt ::= */
   271,  /* (190) orderby_opt ::= ORDER BY sortlist */
   282,  /* (191) sortlist ::= sortlist COMMA item sortorder */
   282,  /* (192) sortlist ::= item sortorder */
   284,  /* (193) item ::= ids cpxName */
   285,  /* (194) sortorder ::= ASC */
   285,  /* (195) sortorder ::= DESC */
   285,  /* (196) sortorder ::= */
   270,  /* (197) groupby_opt ::= */
   270,  /* (198) groupby_opt ::= GROUP BY grouplist */
   286,  /* (199) grouplist ::= grouplist COMMA item */
   286,  /* (200) grouplist ::= item */
   272,  /* (201) having_opt ::= */
   272,  /* (202) having_opt ::= HAVING expr */
   274,  /* (203) limit_opt ::= */
   274,  /* (204) limit_opt ::= LIMIT signed */
   274,  /* (205) limit_opt ::= LIMIT signed OFFSET signed */
   274,  /* (206) limit_opt ::= LIMIT signed COMMA signed */
   273,  /* (207) slimit_opt ::= */
   273,  /* (208) slimit_opt ::= SLIMIT signed */
   273,  /* (209) slimit_opt ::= SLIMIT signed SOFFSET signed */
   273,  /* (210) slimit_opt ::= SLIMIT signed COMMA signed */
   266,  /* (211) where_opt ::= */
   266,  /* (212) where_opt ::= WHERE expr */
   278,  /* (213) expr ::= LP expr RP */
   278,  /* (214) expr ::= ID */
   278,  /* (215) expr ::= ID DOT ID */
   278,  /* (216) expr ::= ID DOT STAR */
   278,  /* (217) expr ::= INTEGER */
   278,  /* (218) expr ::= MINUS INTEGER */
   278,  /* (219) expr ::= PLUS INTEGER */
   278,  /* (220) expr ::= FLOAT */
   278,  /* (221) expr ::= MINUS FLOAT */
   278,  /* (222) expr ::= PLUS FLOAT */
   278,  /* (223) expr ::= STRING */
   278,  /* (224) expr ::= NOW */
   278,  /* (225) expr ::= VARIABLE */
   278,  /* (226) expr ::= BOOL */
   278,  /* (227) expr ::= ID LP exprlist RP */
   278,  /* (228) expr ::= ID LP STAR RP */
   278,  /* (229) expr ::= expr IS NULL */
   278,  /* (230) expr ::= expr IS NOT NULL */
   278,  /* (231) expr ::= expr LT expr */
   278,  /* (232) expr ::= expr GT expr */
   278,  /* (233) expr ::= expr LE expr */
   278,  /* (234) expr ::= expr GE expr */
   278,  /* (235) expr ::= expr NE expr */
   278,  /* (236) expr ::= expr EQ expr */
   278,  /* (237) expr ::= expr BETWEEN expr AND expr */
   278,  /* (238) expr ::= expr AND expr */
   278,  /* (239) expr ::= expr OR expr */
   278,  /* (240) expr ::= expr PLUS expr */
   278,  /* (241) expr ::= expr MINUS expr */
   278,  /* (242) expr ::= expr STAR expr */
   278,  /* (243) expr ::= expr SLASH expr */
   278,  /* (244) expr ::= expr REM expr */
   278,  /* (245) expr ::= expr LIKE expr */
   278,  /* (246) expr ::= expr IN LP exprlist RP */
   287,  /* (247) exprlist ::= exprlist COMMA expritem */
   287,  /* (248) exprlist ::= expritem */
   288,  /* (249) expritem ::= expr */
   288,  /* (250) expritem ::= */
   216,  /* (251) cmd ::= RESET QUERY CACHE */
   216,  /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   216,  /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   216,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   216,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   216,  /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   216,  /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   216,  /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   216,  /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   216,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   216,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   216,  /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   216,  /* (263) cmd ::= KILL CONNECTION INTEGER */
   216,  /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   216,  /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (59) cmd ::= CREATE FUNCTION ids AS ids */
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
  -12,  /* (159) select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
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
    0,  /* (184) fill_opt ::= */
   -6,  /* (185) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (186) fill_opt ::= FILL LP ID RP */
   -4,  /* (187) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (188) sliding_opt ::= */
    0,  /* (189) orderby_opt ::= */
   -3,  /* (190) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (191) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (192) sortlist ::= item sortorder */
   -2,  /* (193) item ::= ids cpxName */
   -1,  /* (194) sortorder ::= ASC */
   -1,  /* (195) sortorder ::= DESC */
    0,  /* (196) sortorder ::= */
    0,  /* (197) groupby_opt ::= */
   -3,  /* (198) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (199) grouplist ::= grouplist COMMA item */
   -1,  /* (200) grouplist ::= item */
    0,  /* (201) having_opt ::= */
   -2,  /* (202) having_opt ::= HAVING expr */
    0,  /* (203) limit_opt ::= */
   -2,  /* (204) limit_opt ::= LIMIT signed */
   -4,  /* (205) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (206) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (207) slimit_opt ::= */
   -2,  /* (208) slimit_opt ::= SLIMIT signed */
   -4,  /* (209) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (210) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (211) where_opt ::= */
   -2,  /* (212) where_opt ::= WHERE expr */
   -3,  /* (213) expr ::= LP expr RP */
   -1,  /* (214) expr ::= ID */
   -3,  /* (215) expr ::= ID DOT ID */
   -3,  /* (216) expr ::= ID DOT STAR */
   -1,  /* (217) expr ::= INTEGER */
   -2,  /* (218) expr ::= MINUS INTEGER */
   -2,  /* (219) expr ::= PLUS INTEGER */
   -1,  /* (220) expr ::= FLOAT */
   -2,  /* (221) expr ::= MINUS FLOAT */
   -2,  /* (222) expr ::= PLUS FLOAT */
   -1,  /* (223) expr ::= STRING */
   -1,  /* (224) expr ::= NOW */
   -1,  /* (225) expr ::= VARIABLE */
   -1,  /* (226) expr ::= BOOL */
   -4,  /* (227) expr ::= ID LP exprlist RP */
   -4,  /* (228) expr ::= ID LP STAR RP */
   -3,  /* (229) expr ::= expr IS NULL */
   -4,  /* (230) expr ::= expr IS NOT NULL */
   -3,  /* (231) expr ::= expr LT expr */
   -3,  /* (232) expr ::= expr GT expr */
   -3,  /* (233) expr ::= expr LE expr */
   -3,  /* (234) expr ::= expr GE expr */
   -3,  /* (235) expr ::= expr NE expr */
   -3,  /* (236) expr ::= expr EQ expr */
   -5,  /* (237) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (238) expr ::= expr AND expr */
   -3,  /* (239) expr ::= expr OR expr */
   -3,  /* (240) expr ::= expr PLUS expr */
   -3,  /* (241) expr ::= expr MINUS expr */
   -3,  /* (242) expr ::= expr STAR expr */
   -3,  /* (243) expr ::= expr SLASH expr */
   -3,  /* (244) expr ::= expr REM expr */
   -3,  /* (245) expr ::= expr LIKE expr */
   -5,  /* (246) expr ::= expr IN LP exprlist RP */
   -3,  /* (247) exprlist ::= exprlist COMMA expritem */
   -1,  /* (248) exprlist ::= expritem */
   -1,  /* (249) expritem ::= expr */
    0,  /* (250) expritem ::= */
   -3,  /* (251) cmd ::= RESET QUERY CACHE */
   -7,  /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (263) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy560, &t);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy205);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy205);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy205);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy560, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE FUNCTION ids AS ids */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
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
    yylhsminor.yy205.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy205.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy205.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy205.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy205.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy205.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy205.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy205.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy205.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy205 = yylhsminor.yy205;
        break;
      case 80: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy209 = yymsp[0].minor.yy209; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy560); yymsp[1].minor.yy560.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 97: /* db_optr ::= db_optr cache */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 98: /* db_optr ::= db_optr replica */
      case 115: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==115);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 99: /* db_optr ::= db_optr quorum */
      case 116: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==116);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 100: /* db_optr ::= db_optr days */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 101: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 102: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 103: /* db_optr ::= db_optr blocks */
      case 118: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==118);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 104: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 105: /* db_optr ::= db_optr wal */
      case 120: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==120);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 106: /* db_optr ::= db_optr fsync */
      case 121: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==121);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 107: /* db_optr ::= db_optr comp */
      case 119: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==119);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 108: /* db_optr ::= db_optr prec */
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 109: /* db_optr ::= db_optr keep */
      case 117: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==117);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.keep = yymsp[0].minor.yy209; }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 110: /* db_optr ::= db_optr update */
      case 122: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==122);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 111: /* db_optr ::= db_optr cachelast */
      case 123: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==123);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 112: /* topic_optr ::= db_optr */
      case 124: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==124);
{ yylhsminor.yy560 = yymsp[0].minor.yy560; yylhsminor.yy560.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy560 = yylhsminor.yy560;
        break;
      case 113: /* topic_optr ::= topic_optr partitions */
      case 125: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==125);
{ yylhsminor.yy560 = yymsp[-1].minor.yy560; yylhsminor.yy560.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy560 = yylhsminor.yy560;
        break;
      case 114: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy560); yymsp[1].minor.yy560.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 126: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSqlSetColumnType (&yylhsminor.yy369, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 127: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy55 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSqlSetColumnType(&yylhsminor.yy369, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy55;  // negative value of name length
    tSqlSetColumnType(&yylhsminor.yy369, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy369 = yylhsminor.yy369;
        break;
      case 128: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSqlSetColumnType (&yylhsminor.yy369, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy369 = yylhsminor.yy369;
        break;
      case 129: /* signed ::= INTEGER */
{ yylhsminor.yy55 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy55 = yylhsminor.yy55;
        break;
      case 130: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy55 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 131: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy55 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 135: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy144;}
        break;
      case 136: /* create_table_list ::= create_from_stable */
{
  SCreateTableSQL* pCreateTable = calloc(1, sizeof(SCreateTableSQL));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy34);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy144 = pCreateTable;
}
  yymsp[0].minor.yy144 = yylhsminor.yy144;
        break;
      case 137: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy144->childTableInfo, &yymsp[0].minor.yy34);
  yylhsminor.yy144 = yymsp[-1].minor.yy144;
}
  yymsp[-1].minor.yy144 = yylhsminor.yy144;
        break;
      case 138: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy144 = tSetCreateSqlElems(yymsp[-1].minor.yy209, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy144, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy144 = yylhsminor.yy144;
        break;
      case 139: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy144 = tSetCreateSqlElems(yymsp[-5].minor.yy209, yymsp[-1].minor.yy209, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy144, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy144 = yylhsminor.yy144;
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy34 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy209, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy34 = yylhsminor.yy34;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy34 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy209, yymsp[-1].minor.yy209, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy34 = yylhsminor.yy34;
        break;
      case 142: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy209, &yymsp[0].minor.yy0); yylhsminor.yy209 = yymsp[-2].minor.yy209;  }
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 143: /* tagNamelist ::= ids */
{yylhsminor.yy209 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy209, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy144 = tSetCreateSqlElems(NULL, NULL, yymsp[0].minor.yy540, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy144, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy144 = yylhsminor.yy144;
        break;
      case 145: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy209, &yymsp[0].minor.yy369); yylhsminor.yy209 = yymsp[-2].minor.yy209;  }
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 146: /* columnlist ::= column */
{yylhsminor.yy209 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy209, &yymsp[0].minor.yy369);}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 147: /* column ::= ids typename */
{
  tSqlSetColumnInfo(&yylhsminor.yy369, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy369);
}
  yymsp[-1].minor.yy369 = yylhsminor.yy369;
        break;
      case 148: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy209 = tVariantListAppend(yymsp[-2].minor.yy209, &yymsp[0].minor.yy54, -1);    }
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 149: /* tagitemlist ::= tagitem */
{ yylhsminor.yy209 = tVariantListAppend(NULL, &yymsp[0].minor.yy54, -1); }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 150: /* tagitem ::= INTEGER */
      case 151: /* tagitem ::= FLOAT */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= STRING */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= BOOL */ yytestcase(yyruleno==153);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy54, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy54 = yylhsminor.yy54;
        break;
      case 154: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy54, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy54 = yylhsminor.yy54;
        break;
      case 155: /* tagitem ::= MINUS INTEGER */
      case 156: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==158);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy54, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy54 = yylhsminor.yy54;
        break;
      case 159: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy540 = tSetQuerySqlElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy246, yymsp[-9].minor.yy209, yymsp[-8].minor.yy254, yymsp[-4].minor.yy209, yymsp[-3].minor.yy209, &yymsp[-7].minor.yy102, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy209, &yymsp[0].minor.yy534, &yymsp[-1].minor.yy534);
}
  yymsp[-11].minor.yy540 = yylhsminor.yy540;
        break;
      case 160: /* union ::= select */
{ yylhsminor.yy437 = setSubclause(NULL, yymsp[0].minor.yy540); }
  yymsp[0].minor.yy437 = yylhsminor.yy437;
        break;
      case 161: /* union ::= LP union RP */
{ yymsp[-2].minor.yy437 = yymsp[-1].minor.yy437; }
        break;
      case 162: /* union ::= union UNION ALL select */
{ yylhsminor.yy437 = appendSelectClause(yymsp[-3].minor.yy437, yymsp[0].minor.yy540); }
  yymsp[-3].minor.yy437 = yylhsminor.yy437;
        break;
      case 163: /* union ::= union UNION ALL LP select RP */
{ yylhsminor.yy437 = appendSelectClause(yymsp[-5].minor.yy437, yymsp[-1].minor.yy540); }
  yymsp[-5].minor.yy437 = yylhsminor.yy437;
        break;
      case 164: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy437, NULL, TSDB_SQL_SELECT); }
        break;
      case 165: /* select ::= SELECT selcollist */
{
  yylhsminor.yy540 = tSetQuerySqlElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy246, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 166: /* sclp ::= selcollist COMMA */
{yylhsminor.yy246 = yymsp[-1].minor.yy246;}
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 167: /* sclp ::= */
{yymsp[1].minor.yy246 = 0;}
        break;
      case 168: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy246 = tSqlExprListAppend(yymsp[-3].minor.yy246, yymsp[-1].minor.yy254,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy246 = yylhsminor.yy246;
        break;
      case 169: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSqlExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy246 = tSqlExprListAppend(yymsp[-1].minor.yy246, pNode, 0, 0);
}
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
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
{yymsp[-1].minor.yy209 = yymsp[0].minor.yy209;}
        break;
      case 176: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy209 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy209 = tVariantListAppendToken(yylhsminor.yy209, &yymsp[-1].minor.yy0, -1);  // table alias name
}
  yymsp[-1].minor.yy209 = yylhsminor.yy209;
        break;
      case 177: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy209 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy209 = tVariantListAppendToken(yylhsminor.yy209, &yymsp[0].minor.yy0, -1);
}
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 178: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy209 = tVariantListAppendToken(yymsp[-3].minor.yy209, &yymsp[-1].minor.yy0, -1);
  yylhsminor.yy209 = tVariantListAppendToken(yylhsminor.yy209, &yymsp[-1].minor.yy0, -1);
}
  yymsp[-3].minor.yy209 = yylhsminor.yy209;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy209 = tVariantListAppendToken(yymsp[-4].minor.yy209, &yymsp[-2].minor.yy0, -1);
  yylhsminor.yy209 = tVariantListAppendToken(yylhsminor.yy209, &yymsp[0].minor.yy0, -1);
}
  yymsp[-4].minor.yy209 = yylhsminor.yy209;
        break;
      case 180: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy102.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy102.offset.n = 0; yymsp[-3].minor.yy102.offset.z = NULL; yymsp[-3].minor.yy102.offset.type = 0;}
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy102.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy102.offset = yymsp[-1].minor.yy0;}
        break;
      case 183: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy102, 0, sizeof(yymsp[1].minor.yy102));}
        break;
      case 184: /* fill_opt ::= */
{yymsp[1].minor.yy209 = 0;     }
        break;
      case 185: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy209, &A, -1, 0);
    yymsp[-5].minor.yy209 = yymsp[-1].minor.yy209;
}
        break;
      case 186: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy209 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 187: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 188: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 189: /* orderby_opt ::= */
{yymsp[1].minor.yy209 = 0;}
        break;
      case 190: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy209 = yymsp[0].minor.yy209;}
        break;
      case 191: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy209 = tVariantListAppend(yymsp[-3].minor.yy209, &yymsp[-1].minor.yy54, yymsp[0].minor.yy332);
}
  yymsp[-3].minor.yy209 = yylhsminor.yy209;
        break;
      case 192: /* sortlist ::= item sortorder */
{
  yylhsminor.yy209 = tVariantListAppend(NULL, &yymsp[-1].minor.yy54, yymsp[0].minor.yy332);
}
  yymsp[-1].minor.yy209 = yylhsminor.yy209;
        break;
      case 193: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy54, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy54 = yylhsminor.yy54;
        break;
      case 194: /* sortorder ::= ASC */
{ yymsp[0].minor.yy332 = TSDB_ORDER_ASC; }
        break;
      case 195: /* sortorder ::= DESC */
{ yymsp[0].minor.yy332 = TSDB_ORDER_DESC;}
        break;
      case 196: /* sortorder ::= */
{ yymsp[1].minor.yy332 = TSDB_ORDER_ASC; }
        break;
      case 197: /* groupby_opt ::= */
{ yymsp[1].minor.yy209 = 0;}
        break;
      case 198: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy209 = yymsp[0].minor.yy209;}
        break;
      case 199: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy209 = tVariantListAppend(yymsp[-2].minor.yy209, &yymsp[0].minor.yy54, -1);
}
  yymsp[-2].minor.yy209 = yylhsminor.yy209;
        break;
      case 200: /* grouplist ::= item */
{
  yylhsminor.yy209 = tVariantListAppend(NULL, &yymsp[0].minor.yy54, -1);
}
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 201: /* having_opt ::= */
      case 211: /* where_opt ::= */ yytestcase(yyruleno==211);
      case 250: /* expritem ::= */ yytestcase(yyruleno==250);
{yymsp[1].minor.yy254 = 0;}
        break;
      case 202: /* having_opt ::= HAVING expr */
      case 212: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==212);
{yymsp[-1].minor.yy254 = yymsp[0].minor.yy254;}
        break;
      case 203: /* limit_opt ::= */
      case 207: /* slimit_opt ::= */ yytestcase(yyruleno==207);
{yymsp[1].minor.yy534.limit = -1; yymsp[1].minor.yy534.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed */
      case 208: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==208);
{yymsp[-1].minor.yy534.limit = yymsp[0].minor.yy55;  yymsp[-1].minor.yy534.offset = 0;}
        break;
      case 205: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy534.limit = yymsp[-2].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[0].minor.yy55;}
        break;
      case 206: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy534.limit = yymsp[0].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[-2].minor.yy55;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy534.limit = yymsp[-2].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[0].minor.yy55;}
        break;
      case 210: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy534.limit = yymsp[0].minor.yy55;  yymsp[-3].minor.yy534.offset = yymsp[-2].minor.yy55;}
        break;
      case 213: /* expr ::= LP expr RP */
{yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy254->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 214: /* expr ::= ID */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 215: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 216: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 217: /* expr ::= INTEGER */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 218: /* expr ::= MINUS INTEGER */
      case 219: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==219);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 220: /* expr ::= FLOAT */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 221: /* expr ::= MINUS FLOAT */
      case 222: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 223: /* expr ::= STRING */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 224: /* expr ::= NOW */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 225: /* expr ::= VARIABLE */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 226: /* expr ::= BOOL */
{ yylhsminor.yy254 = tSqlExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 227: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy254 = tSqlExprCreateFunction(yymsp[-1].minor.yy246, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy254 = yylhsminor.yy254;
        break;
      case 228: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy254 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy254 = yylhsminor.yy254;
        break;
      case 229: /* expr ::= expr IS NULL */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 230: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-3].minor.yy254, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy254 = yylhsminor.yy254;
        break;
      case 231: /* expr ::= expr LT expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_LT);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 232: /* expr ::= expr GT expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_GT);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 233: /* expr ::= expr LE expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_LE);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 234: /* expr ::= expr GE expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_GE);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 235: /* expr ::= expr NE expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_NE);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 236: /* expr ::= expr EQ expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_EQ);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 237: /* expr ::= expr BETWEEN expr AND expr */
{ tSQLExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy254); yylhsminor.yy254 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy254, yymsp[-2].minor.yy254, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy254, TK_LE), TK_AND);}
  yymsp[-4].minor.yy254 = yylhsminor.yy254;
        break;
      case 238: /* expr ::= expr AND expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_AND);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 239: /* expr ::= expr OR expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_OR); }
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 240: /* expr ::= expr PLUS expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_PLUS);  }
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 241: /* expr ::= expr MINUS expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_MINUS); }
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 242: /* expr ::= expr STAR expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_STAR);  }
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 243: /* expr ::= expr SLASH expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_DIVIDE);}
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 244: /* expr ::= expr REM expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_REM);   }
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 245: /* expr ::= expr LIKE expr */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-2].minor.yy254, yymsp[0].minor.yy254, TK_LIKE);  }
  yymsp[-2].minor.yy254 = yylhsminor.yy254;
        break;
      case 246: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy254 = tSqlExprCreate(yymsp[-4].minor.yy254, (tSQLExpr*)yymsp[-1].minor.yy246, TK_IN); }
  yymsp[-4].minor.yy254 = yylhsminor.yy254;
        break;
      case 247: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy246 = tSqlExprListAppend(yymsp[-2].minor.yy246,yymsp[0].minor.yy254,0, 0);}
  yymsp[-2].minor.yy246 = yylhsminor.yy246;
        break;
      case 248: /* exprlist ::= expritem */
{yylhsminor.yy246 = tSqlExprListAppend(0,yymsp[0].minor.yy254,0, 0);}
  yymsp[0].minor.yy246 = yylhsminor.yy246;
        break;
      case 249: /* expritem ::= expr */
{yylhsminor.yy254 = yymsp[0].minor.yy254;}
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 251: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 252: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 253: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 254: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 257: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy54, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy209, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tAlterTableSqlElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 263: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 264: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 265: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
