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
#include "ttoken.h"
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
  TAOS_FIELD yy27;
  SWindowStateVal yy76;
  SCreateDbInfo yy114;
  SSqlNode* yy124;
  SCreateAcctInfo yy183;
  SCreatedTableInfo yy192;
  SArray* yy193;
  SCreateTableSql* yy270;
  int yy312;
  SRelationInfo* yy332;
  SIntervalVal yy392;
  tVariant yy442;
  SSessionWindowVal yy447;
  tSqlExpr* yy454;
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
#define YYNSTATE             341
#define YYNRULE              280
#define YYNRULE_WITH_ACTION  280
#define YYNTOKEN             189
#define YY_MAX_SHIFT         340
#define YY_MIN_SHIFTREDUCE   538
#define YY_MAX_SHIFTREDUCE   817
#define YY_ERROR_ACTION      818
#define YY_ACCEPT_ACTION     819
#define YY_NO_ACTION         820
#define YY_MIN_REDUCE        821
#define YY_MAX_REDUCE        1100
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
#define YY_ACTTAB_COUNT (722)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   989,  586,  217,  338,  954,   22,  223,  192,  194,  587,
 /*    10 */   819,  340,  198,   52,   53,  151,   56,   57,  226, 1077,
 /*    20 */   229,   46,  283,   55,  282,   60,   58,   62,   59, 1073,
 /*    30 */   665,  194,  968,   51,   50,  194,  234,   49,   48,   47,
 /*    40 */    52,   53, 1076,   56,   57,  225, 1077,  229,   46,  586,
 /*    50 */    55,  282,   60,   58,   62,   59,  980,  587,  314,  313,
 /*    60 */    51,   50,  968,  986,   49,   48,   47,   53,   35,   56,
 /*    70 */    57,  144,  258,  229,   46,   75,   55,  282,   60,   58,
 /*    80 */    62,   59,  279,  298,   87,  867,   51,   50,   94,  178,
 /*    90 */    49,   48,   47,  539,  540,  541,  542,  543,  544,  545,
 /*   100 */   546,  547,  548,  549,  550,  551,  339,  953,  298,  218,
 /*   110 */    76,  586,  964,   52,   53,   35,   56,   57,  775,  587,
 /*   120 */   229,   46,  956,   55,  282,   60,   58,   62,   59,   49,
 /*   130 */    48,   47,  756,   51,   50,  265,  264,   49,   48,   47,
 /*   140 */    52,   54,  980,   56,   57,  324,  980,  229,   46,  586,
 /*   150 */    55,  282,   60,   58,   62,   59,  219,  587,  220,  965,
 /*   160 */    51,   50,  221, 1072,   49,   48,   47,   28,  296,  333,
 /*   170 */   332,  295,  294,  293,  331,  292,  330,  329,  328,  291,
 /*   180 */   327,  326,  928,   35,  916,  917,  918,  919,  920,  921,
 /*   190 */   922,  923,  924,  925,  926,  927,  929,  930,   56,   57,
 /*   200 */   876, 1071,  229,   46,  178,   55,  282,   60,   58,   62,
 /*   210 */    59,  962,   23,   91,   29,   51,   50,    1,  166,   49,
 /*   220 */    48,   47,  228,  771,  232,   79,  760,  965,  763,  203,
 /*   230 */   766,  228,  771,  261,   13,  760,  204,  763,   93,  766,
 /*   240 */    90,  128,  127,  202,  951,  952,   34,  955,   60,   58,
 /*   250 */    62,   59,   89,  235,  214,  215,   51,   50,  281,  151,
 /*   260 */    49,   48,   47,  214,  215,  762,   77,  765,   28, 1096,
 /*   270 */   333,  332,   82,   35,   35,  331,  701,  330,  329,  328,
 /*   280 */    41,  327,  326,    8,  936,   51,   50,  934,  935,   49,
 /*   290 */    48,   47,  937,  868,  939,  940,  938,  178,  941,  942,
 /*   300 */   113,  107,  118,  257,  239,   74,  704,  117,  123,  126,
 /*   310 */   116,  242,  211,   35,  233,  303,  120,  965,  965,  689,
 /*   320 */   212,  761,  686,  764,  687,   61,  688,  213, 1026,   35,
 /*   330 */   277,   35,  772, 1036,   61,    5,   38,  168,  768,  151,
 /*   340 */   196,  772,  167,  101,   96,  100,   35,  768,   35,  151,
 /*   350 */   245,  246,   35,   35,  304,  767,  236,  965,  186,  184,
 /*   360 */   182,  142,  140,  139,  767,  181,  131,  130,  129,  334,
 /*   370 */   305,  243,  306,  965,  240,  965,  238,  769,  302,  301,
 /*   380 */    82,  244,  968,  241,  708,  309,  308,  310,   41,  311,
 /*   390 */   965,  250,  965,  312,  316,  968,  965,  965,    3,  179,
 /*   400 */   254,  253,  337,  336,  136,  115,  966,   80, 1025,  259,
 /*   410 */   324,  737,  738,   36,  758,  261,  720,  728,   88,  729,
 /*   420 */   146,   66,  227,   25,   67,  792,  197,  773,  690,   24,
 /*   430 */   675,   24,   70,  770,   36,  285,  677,  287,  676,   36,
 /*   440 */    66,   92,   66,   33,  125,  124,  288,   68,  199,   15,
 /*   450 */   759,   14,  106,   71,  105,  193,  200,   17,   19,   16,
 /*   460 */    18,  201,   73,  112,  664,  111,    6,  207,  693,  691,
 /*   470 */   694,  692,  208,  206,   21, 1035,   20, 1088,  191,  205,
 /*   480 */   195,  967,  230,  255, 1032, 1031,  231,  315,   44,  143,
 /*   490 */   988, 1018,  999,  996, 1017,  997,  981,  262, 1001,  145,
 /*   500 */   149,  271,  162,  963,  141,  114,  266,  222,  719,  268,
 /*   510 */   158,  275,  154,  163,  978,  152,  155,  276,  961,  164,
 /*   520 */   165,  280,  153,   72,  156,   63,  879,   69,  290,  278,
 /*   530 */    42,  274,  189,   39,  299,  875,  300, 1095,  103, 1094,
 /*   540 */  1091,  169,  307, 1087,  109, 1086, 1083,  170,  897,   40,
 /*   550 */   272,   37,   43,  190,  270,  864,  119,  862,  121,  122,
 /*   560 */   267,  860,  859,  247,  180,  857,  856,  855,  854,  853,
 /*   570 */   852,  183,  185,  849,  847,  845,  843,  187,  840,  188,
 /*   580 */    45,  260,   78,   83,  325,  269, 1019,  317,  318,  319,
 /*   590 */   320,  321,  322,  323,  335,  817,  248,  216,  237,  289,
 /*   600 */   249,  816,  251,  252,  209,  210,   97,   98,  815,  798,
 /*   610 */   797,  256,  261,  263,  858,  696,  284,    9,  132,  851,
 /*   620 */   173,  133,  172,  898,  171,  174,  175,  177,  176,    4,
 /*   630 */   134,  850,  842,  932,  135,   30,  841,   81,   84,  721,
 /*   640 */     2,  161,  159,  157,  160,  147,  944,  724,  148,   85,
 /*   650 */   224,  726,   86,  273,   10,  730,  150,   11,  776,  774,
 /*   660 */    31,    7,   32,   12,   26,  286,   27,   95,  628,   93,
 /*   670 */   624,  622,  621,  620,  617,  297,   99,  590,   64,   36,
 /*   680 */    65,  102,  667,  666,  104,  108,  663,  612,  610,  602,
 /*   690 */   608,  604,  606,  600,  598,  631,  110,  630,  629,  627,
 /*   700 */   626,  625,  623,  619,  618,  588,  555,  553,   66,  821,
 /*   710 */   820,  820,  137,  820,  820,  820,  820,  820,  820,  820,
 /*   720 */   820,  138,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   192,    1,  191,  192,    0,  254,  211,  254,  254,    9,
 /*    10 */   189,  190,  254,   13,   14,  192,   16,   17,  264,  265,
 /*    20 */    20,   21,   15,   23,   24,   25,   26,   27,   28,  254,
 /*    30 */     5,  254,  237,   33,   34,  254,  211,   37,   38,   39,
 /*    40 */    13,   14,  265,   16,   17,  264,  265,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  235,    9,   33,   34,
 /*    60 */    33,   34,  237,  255,   37,   38,   39,   14,  192,   16,
 /*    70 */    17,  192,  251,   20,   21,  198,   23,   24,   25,   26,
 /*    80 */    27,   28,  259,   79,  261,  197,   33,   34,  198,  201,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  230,   79,   61,
 /*   110 */   110,    1,  236,   13,   14,  192,   16,   17,  111,    9,
 /*   120 */    20,   21,  232,   23,   24,   25,   26,   27,   28,   37,
 /*   130 */    38,   39,  105,   33,   34,  256,  257,   37,   38,   39,
 /*   140 */    13,   14,  235,   16,   17,   81,  235,   20,   21,    1,
 /*   150 */    23,   24,   25,   26,   27,   28,  233,    9,  251,  236,
 /*   160 */    33,   34,  251,  254,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  210,  192,  212,  213,  214,  215,  216,  217,
 /*   190 */   218,  219,  220,  221,  222,  223,  224,  225,   16,   17,
 /*   200 */   197,  254,   20,   21,  201,   23,   24,   25,   26,   27,
 /*   210 */    28,  192,   44,  198,  104,   33,   34,  199,  200,   37,
 /*   220 */    38,   39,    1,    2,  233,  105,    5,  236,    7,   61,
 /*   230 */     9,    1,    2,  113,  104,    5,   68,    7,  108,    9,
 /*   240 */   110,   73,   74,   75,  229,  230,  231,  232,   25,   26,
 /*   250 */    27,   28,  238,  234,   33,   34,   33,   34,   37,  192,
 /*   260 */    37,   38,   39,   33,   34,    5,  252,    7,   88,  237,
 /*   270 */    90,   91,  104,  192,  192,   95,  109,   97,   98,   99,
 /*   280 */   112,  101,  102,  116,  210,   33,   34,  213,  214,   37,
 /*   290 */    38,   39,  218,  197,  220,  221,  222,  201,  224,  225,
 /*   300 */    62,   63,   64,  135,   68,  137,   37,   69,   70,   71,
 /*   310 */    72,   68,  144,  192,  233,  233,   78,  236,  236,    2,
 /*   320 */   254,    5,    5,    7,    7,  104,    9,  254,  261,  192,
 /*   330 */   263,  192,  111,  228,  104,   62,   63,   64,  117,  192,
 /*   340 */   254,  111,   69,   70,   71,   72,  192,  117,  192,  192,
 /*   350 */    33,   34,  192,  192,  233,  134,  211,  236,   62,   63,
 /*   360 */    64,   62,   63,   64,  134,   69,   70,   71,   72,  211,
 /*   370 */   233,  192,  233,  236,  138,  236,  140,  117,  142,  143,
 /*   380 */   104,  138,  237,  140,  115,  142,  143,  233,  112,  233,
 /*   390 */   236,  136,  236,  233,  233,  237,  236,  236,  195,  196,
 /*   400 */   145,  146,   65,   66,   67,   76,  227,  105,  261,  105,
 /*   410 */    81,  125,  126,  109,    1,  113,  105,  105,  261,  105,
 /*   420 */   109,  109,   60,  109,  109,  105,  254,  105,  111,  109,
 /*   430 */   105,  109,  109,  117,  109,  105,  105,  105,  105,  109,
 /*   440 */   109,  109,  109,  104,   76,   77,  107,  132,  254,  139,
 /*   450 */    37,  141,  139,  130,  141,  254,  254,  139,  139,  141,
 /*   460 */   141,  254,  104,  139,  106,  141,  104,  254,    5,    5,
 /*   470 */     7,    7,  254,  254,  139,  228,  141,  237,  254,  254,
 /*   480 */   254,  237,  228,  192,  228,  228,  228,  228,  253,  192,
 /*   490 */   192,  262,  192,  192,  262,  192,  235,  235,  192,  192,
 /*   500 */   192,  192,  239,  235,   60,   87,  258,  258,  117,  258,
 /*   510 */   243,  258,  247,  192,  250,  249,  246,  122,  192,  192,
 /*   520 */   192,  123,  248,  129,  245,  128,  192,  131,  192,  127,
 /*   530 */   192,  121,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   540 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   550 */   120,  192,  192,  192,  119,  192,  192,  192,  192,  192,
 /*   560 */   118,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   570 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   580 */   133,  193,  193,  193,  103,  193,  193,   86,   50,   83,
 /*   590 */    85,   54,   84,   82,   79,    5,  147,  193,  193,  193,
 /*   600 */     5,    5,  147,    5,  193,  193,  198,  198,    5,   90,
 /*   610 */    89,  136,  113,  109,  193,  105,  107,  104,  194,  193,
 /*   620 */   203,  194,  207,  209,  208,  206,  204,  202,  205,  195,
 /*   630 */   194,  193,  193,  226,  194,  104,  193,  114,  109,  105,
 /*   640 */   199,  240,  242,  244,  241,  104,  226,  105,  109,  104,
 /*   650 */     1,  105,  104,  104,  124,  105,  104,  124,  111,  105,
 /*   660 */   109,  104,  109,  104,  104,  107,  104,   76,    9,  108,
 /*   670 */     5,    5,    5,    5,    5,   15,   76,   80,   16,  109,
 /*   680 */    16,  141,    5,    5,  141,  141,  105,    5,    5,    5,
 /*   690 */     5,    5,    5,    5,    5,    5,  141,    5,    5,    5,
 /*   700 */     5,    5,    5,    5,    5,   80,   60,   59,  109,    0,
 /*   710 */   266,  266,   21,  266,  266,  266,  266,  266,  266,  266,
 /*   720 */   266,   21,  266,  266,  266,  266,  266,  266,  266,  266,
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
 /*   860 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   870 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   880 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   890 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   900 */   266,  266,  266,  266,  266,  266,  266,  266,  266,  266,
 /*   910 */   266,
};
#define YY_SHIFT_COUNT    (340)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (709)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  180,  180,   29,  221,  230,  110,  148,
 /*    10 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*    20 */   148,  148,    0,   48,  230,  317,  317,  317,  317,  276,
 /*    30 */   276,  148,  148,  148,    4,  148,  148,  329,   29,   64,
 /*    40 */    64,  722,  722,  722,  230,  230,  230,  230,  230,  230,
 /*    50 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    60 */   230,  230,  230,  230,  317,  317,  317,   25,   25,   25,
 /*    70 */    25,   25,   25,   25,  148,  148,  148,  269,  148,  148,
 /*    80 */   148,  276,  276,  148,  148,  148,  148,  286,  286,  167,
 /*    90 */   276,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   100 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   110 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   120 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   130 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   140 */   148,  148,  148,  444,  444,  444,  391,  391,  391,  444,
 /*   150 */   391,  444,  394,  396,  397,  398,  402,  395,  410,  430,
 /*   160 */   435,  442,  447,  444,  444,  444,  481,   29,   29,  444,
 /*   170 */   444,  418,  501,  538,  506,  505,  537,  508,  511,  481,
 /*   180 */   444,  515,  515,  444,  515,  444,  515,  444,  444,  722,
 /*   190 */   722,   27,  100,  127,  100,  100,   53,  182,  223,  223,
 /*   200 */   223,  223,  238,  273,  296,  252,  252,  252,  252,  236,
 /*   210 */   243,  255,   92,   92,  260,  316,  130,  337,  299,  304,
 /*   220 */   120,  302,  311,  312,  314,  320,  322,  413,  362,    7,
 /*   230 */   315,  323,  325,  330,  331,  332,  333,  339,  310,  313,
 /*   240 */   318,  319,  324,  358,  335,  463,  464,  368,  590,  449,
 /*   250 */   595,  596,  455,  598,  603,  519,  521,  475,  499,  509,
 /*   260 */   513,  523,  510,  531,  504,  529,  534,  541,  542,  539,
 /*   270 */   545,  546,  548,  649,  549,  550,  552,  551,  530,  553,
 /*   280 */   533,  554,  557,  547,  559,  509,  560,  558,  562,  561,
 /*   290 */   591,  659,  665,  666,  667,  668,  669,  597,  660,  600,
 /*   300 */   662,  540,  543,  570,  570,  570,  570,  664,  544,  555,
 /*   310 */   570,  570,  570,  677,  678,  581,  570,  682,  683,  684,
 /*   320 */   685,  686,  687,  688,  689,  690,  692,  693,  694,  695,
 /*   330 */   696,  697,  698,  699,  599,  625,  691,  700,  646,  648,
 /*   340 */   709,
};
#define YY_REDUCE_COUNT (190)
#define YY_REDUCE_MIN   (-249)
#define YY_REDUCE_MAX   (443)
static const short yy_reduce_ofst[] = {
 /*     0 */  -179,  -28,  -28,   74,   74,   15, -246, -219, -121,  -77,
 /*    10 */    67, -177,   -9,   81,   82,  121,  137,  139,  154,  156,
 /*    20 */   160,  161, -192, -189, -223, -205, -175,  145,  158,  -93,
 /*    30 */   -89,  147,  157,   19, -110,  179, -124, -112, -123,    3,
 /*    40 */    96,   14,   18,  203, -249, -247, -242, -225,  -91,  -53,
 /*    50 */    66,   73,   86,  172,  194,  201,  202,  207,  213,  218,
 /*    60 */   219,  224,  225,  226,   32,  240,  244,  105,  247,  254,
 /*    70 */   256,  257,  258,  259,  291,  297,  298,  235,  300,  301,
 /*    80 */   303,  261,  262,  306,  307,  308,  309,  229,  232,  263,
 /*    90 */   268,  321,  326,  327,  328,  334,  336,  338,  340,  341,
 /*   100 */   342,  343,  344,  345,  346,  347,  348,  349,  350,  351,
 /*   110 */   352,  353,  354,  355,  356,  357,  359,  360,  361,  363,
 /*   120 */   364,  365,  366,  367,  369,  370,  371,  372,  373,  374,
 /*   130 */   375,  376,  377,  378,  379,  380,  381,  382,  383,  384,
 /*   140 */   385,  386,  387,  388,  389,  390,  248,  249,  251,  392,
 /*   150 */   253,  393,  264,  266,  274,  265,  270,  279,  399,  267,
 /*   160 */   400,  403,  401,  404,  405,  406,  407,  408,  409,  411,
 /*   170 */   412,  414,  416,  415,  417,  419,  422,  423,  425,  420,
 /*   180 */   421,  424,  427,  426,  436,  438,  440,  439,  443,  441,
 /*   190 */   434,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   818,  931,  877,  943,  865,  874, 1079, 1079,  818,  818,
 /*    10 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*    20 */   818,  818,  990,  837, 1079,  818,  818,  818,  818,  818,
 /*    30 */   818,  818,  818,  818,  874,  818,  818,  880,  874,  880,
 /*    40 */   880,  985,  915,  933,  818,  818,  818,  818,  818,  818,
 /*    50 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*    60 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*    70 */   818,  818,  818,  818,  818,  818,  818,  992,  998,  995,
 /*    80 */   818,  818,  818, 1000,  818,  818,  818, 1022, 1022,  983,
 /*    90 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*   100 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*   110 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  863,
 /*   120 */   818,  861,  818,  818,  818,  818,  818,  818,  818,  818,
 /*   130 */   818,  818,  818,  818,  818,  818,  848,  818,  818,  818,
 /*   140 */   818,  818,  818,  839,  839,  839,  818,  818,  818,  839,
 /*   150 */   818,  839, 1029, 1033, 1027, 1015, 1023, 1014, 1010, 1008,
 /*   160 */  1006, 1005, 1037,  839,  839,  839,  878,  874,  874,  839,
 /*   170 */   839,  896,  894,  892,  884,  890,  886,  888,  882,  866,
 /*   180 */   839,  872,  872,  839,  872,  839,  872,  839,  839,  915,
 /*   190 */   933,  818, 1038,  818, 1078, 1028, 1068, 1067, 1074, 1066,
 /*   200 */  1065, 1064,  818,  818,  818, 1060, 1061, 1063, 1062,  818,
 /*   210 */   818,  818, 1070, 1069,  818,  818,  818,  818,  818,  818,
 /*   220 */   818,  818,  818,  818,  818,  818,  818,  818, 1040,  818,
 /*   230 */  1034, 1030,  818,  818,  818,  818,  818,  818,  818,  818,
 /*   240 */   818,  818,  818,  945,  818,  818,  818,  818,  818,  818,
 /*   250 */   818,  818,  818,  818,  818,  818,  818,  818,  982,  818,
 /*   260 */   818,  818,  818,  818,  994,  993,  818,  818,  818,  818,
 /*   270 */   818,  818,  818,  818,  818,  818,  818, 1024,  818, 1016,
 /*   280 */   818,  818,  818,  818,  818,  957,  818,  818,  818,  818,
 /*   290 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*   300 */   818,  818,  818, 1097, 1092, 1093, 1090,  818,  818,  818,
 /*   310 */  1089, 1084, 1085,  818,  818,  818, 1082,  818,  818,  818,
 /*   320 */   818,  818,  818,  818,  818,  818,  818,  818,  818,  818,
 /*   330 */   818,  818,  818,  818,  899,  818,  846,  844,  818,  835,
 /*   340 */   818,
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
    1,  /*     STABLE => ID */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TOPIC => nothing */
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
    0,  /*  CACHELAST => nothing */
    0,  /* PARTITIONS => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*      COMMA => nothing */
    0,  /*         AS => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
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
    0,  /*     SYNCDB => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*     MODIFY => nothing */
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
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
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
  /*   47 */ "MNODES",
  /*   48 */ "DNODES",
  /*   49 */ "ACCOUNTS",
  /*   50 */ "USERS",
  /*   51 */ "MODULES",
  /*   52 */ "QUERIES",
  /*   53 */ "CONNECTIONS",
  /*   54 */ "STREAMS",
  /*   55 */ "VARIABLES",
  /*   56 */ "SCORES",
  /*   57 */ "GRANTS",
  /*   58 */ "VNODES",
  /*   59 */ "IPTOKEN",
  /*   60 */ "DOT",
  /*   61 */ "CREATE",
  /*   62 */ "TABLE",
  /*   63 */ "STABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "TOPIC",
  /*   70 */ "DNODE",
  /*   71 */ "USER",
  /*   72 */ "ACCOUNT",
  /*   73 */ "USE",
  /*   74 */ "DESCRIBE",
  /*   75 */ "ALTER",
  /*   76 */ "PASS",
  /*   77 */ "PRIVILEGE",
  /*   78 */ "LOCAL",
  /*   79 */ "IF",
  /*   80 */ "EXISTS",
  /*   81 */ "PPS",
  /*   82 */ "TSERIES",
  /*   83 */ "DBS",
  /*   84 */ "STORAGE",
  /*   85 */ "QTIME",
  /*   86 */ "CONNS",
  /*   87 */ "STATE",
  /*   88 */ "KEEP",
  /*   89 */ "CACHE",
  /*   90 */ "REPLICA",
  /*   91 */ "QUORUM",
  /*   92 */ "DAYS",
  /*   93 */ "MINROWS",
  /*   94 */ "MAXROWS",
  /*   95 */ "BLOCKS",
  /*   96 */ "CTIME",
  /*   97 */ "WAL",
  /*   98 */ "FSYNC",
  /*   99 */ "COMP",
  /*  100 */ "PRECISION",
  /*  101 */ "UPDATE",
  /*  102 */ "CACHELAST",
  /*  103 */ "PARTITIONS",
  /*  104 */ "LP",
  /*  105 */ "RP",
  /*  106 */ "UNSIGNED",
  /*  107 */ "TAGS",
  /*  108 */ "USING",
  /*  109 */ "COMMA",
  /*  110 */ "AS",
  /*  111 */ "NULL",
  /*  112 */ "SELECT",
  /*  113 */ "UNION",
  /*  114 */ "ALL",
  /*  115 */ "DISTINCT",
  /*  116 */ "FROM",
  /*  117 */ "VARIABLE",
  /*  118 */ "INTERVAL",
  /*  119 */ "SESSION",
  /*  120 */ "STATE_WINDOW",
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
  /*  137 */ "SYNCDB",
  /*  138 */ "ADD",
  /*  139 */ "COLUMN",
  /*  140 */ "MODIFY",
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
  /*  184 */ "TBNAME",
  /*  185 */ "JOIN",
  /*  186 */ "INSERT",
  /*  187 */ "INTO",
  /*  188 */ "VALUES",
  /*  189 */ "program",
  /*  190 */ "cmd",
  /*  191 */ "dbPrefix",
  /*  192 */ "ids",
  /*  193 */ "cpxName",
  /*  194 */ "ifexists",
  /*  195 */ "alter_db_optr",
  /*  196 */ "alter_topic_optr",
  /*  197 */ "acct_optr",
  /*  198 */ "ifnotexists",
  /*  199 */ "db_optr",
  /*  200 */ "topic_optr",
  /*  201 */ "pps",
  /*  202 */ "tseries",
  /*  203 */ "dbs",
  /*  204 */ "streams",
  /*  205 */ "storage",
  /*  206 */ "qtime",
  /*  207 */ "users",
  /*  208 */ "conns",
  /*  209 */ "state",
  /*  210 */ "keep",
  /*  211 */ "tagitemlist",
  /*  212 */ "cache",
  /*  213 */ "replica",
  /*  214 */ "quorum",
  /*  215 */ "days",
  /*  216 */ "minrows",
  /*  217 */ "maxrows",
  /*  218 */ "blocks",
  /*  219 */ "ctime",
  /*  220 */ "wal",
  /*  221 */ "fsync",
  /*  222 */ "comp",
  /*  223 */ "prec",
  /*  224 */ "update",
  /*  225 */ "cachelast",
  /*  226 */ "partitions",
  /*  227 */ "typename",
  /*  228 */ "signed",
  /*  229 */ "create_table_args",
  /*  230 */ "create_stable_args",
  /*  231 */ "create_table_list",
  /*  232 */ "create_from_stable",
  /*  233 */ "columnlist",
  /*  234 */ "tagNamelist",
  /*  235 */ "select",
  /*  236 */ "column",
  /*  237 */ "tagitem",
  /*  238 */ "selcollist",
  /*  239 */ "from",
  /*  240 */ "where_opt",
  /*  241 */ "interval_opt",
  /*  242 */ "session_option",
  /*  243 */ "windowstate_option",
  /*  244 */ "fill_opt",
  /*  245 */ "sliding_opt",
  /*  246 */ "groupby_opt",
  /*  247 */ "orderby_opt",
  /*  248 */ "having_opt",
  /*  249 */ "slimit_opt",
  /*  250 */ "limit_opt",
  /*  251 */ "union",
  /*  252 */ "sclp",
  /*  253 */ "distinct",
  /*  254 */ "expr",
  /*  255 */ "as",
  /*  256 */ "tablelist",
  /*  257 */ "sub",
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
 /*   3 */ "cmd ::= SHOW MNODES",
 /*   4 */ "cmd ::= SHOW DNODES",
 /*   5 */ "cmd ::= SHOW ACCOUNTS",
 /*   6 */ "cmd ::= SHOW USERS",
 /*   7 */ "cmd ::= SHOW MODULES",
 /*   8 */ "cmd ::= SHOW QUERIES",
 /*   9 */ "cmd ::= SHOW CONNECTIONS",
 /*  10 */ "cmd ::= SHOW STREAMS",
 /*  11 */ "cmd ::= SHOW VARIABLES",
 /*  12 */ "cmd ::= SHOW SCORES",
 /*  13 */ "cmd ::= SHOW GRANTS",
 /*  14 */ "cmd ::= SHOW VNODES",
 /*  15 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  16 */ "dbPrefix ::=",
 /*  17 */ "dbPrefix ::= ids DOT",
 /*  18 */ "cpxName ::=",
 /*  19 */ "cpxName ::= DOT ids",
 /*  20 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  21 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
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
 /*  33 */ "cmd ::= DROP DNODE ids",
 /*  34 */ "cmd ::= DROP USER ids",
 /*  35 */ "cmd ::= DROP ACCOUNT ids",
 /*  36 */ "cmd ::= USE ids",
 /*  37 */ "cmd ::= DESCRIBE ids cpxName",
 /*  38 */ "cmd ::= ALTER USER ids PASS ids",
 /*  39 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids ids",
 /*  44 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  45 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "ids ::= ID",
 /*  49 */ "ids ::= STRING",
 /*  50 */ "ifexists ::= IF EXISTS",
 /*  51 */ "ifexists ::=",
 /*  52 */ "ifnotexists ::= IF NOT EXISTS",
 /*  53 */ "ifnotexists ::=",
 /*  54 */ "cmd ::= CREATE DNODE ids",
 /*  55 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  56 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  57 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  58 */ "cmd ::= CREATE USER ids PASS ids",
 /*  59 */ "pps ::=",
 /*  60 */ "pps ::= PPS INTEGER",
 /*  61 */ "tseries ::=",
 /*  62 */ "tseries ::= TSERIES INTEGER",
 /*  63 */ "dbs ::=",
 /*  64 */ "dbs ::= DBS INTEGER",
 /*  65 */ "streams ::=",
 /*  66 */ "streams ::= STREAMS INTEGER",
 /*  67 */ "storage ::=",
 /*  68 */ "storage ::= STORAGE INTEGER",
 /*  69 */ "qtime ::=",
 /*  70 */ "qtime ::= QTIME INTEGER",
 /*  71 */ "users ::=",
 /*  72 */ "users ::= USERS INTEGER",
 /*  73 */ "conns ::=",
 /*  74 */ "conns ::= CONNS INTEGER",
 /*  75 */ "state ::=",
 /*  76 */ "state ::= STATE ids",
 /*  77 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  78 */ "keep ::= KEEP tagitemlist",
 /*  79 */ "cache ::= CACHE INTEGER",
 /*  80 */ "replica ::= REPLICA INTEGER",
 /*  81 */ "quorum ::= QUORUM INTEGER",
 /*  82 */ "days ::= DAYS INTEGER",
 /*  83 */ "minrows ::= MINROWS INTEGER",
 /*  84 */ "maxrows ::= MAXROWS INTEGER",
 /*  85 */ "blocks ::= BLOCKS INTEGER",
 /*  86 */ "ctime ::= CTIME INTEGER",
 /*  87 */ "wal ::= WAL INTEGER",
 /*  88 */ "fsync ::= FSYNC INTEGER",
 /*  89 */ "comp ::= COMP INTEGER",
 /*  90 */ "prec ::= PRECISION STRING",
 /*  91 */ "update ::= UPDATE INTEGER",
 /*  92 */ "cachelast ::= CACHELAST INTEGER",
 /*  93 */ "partitions ::= PARTITIONS INTEGER",
 /*  94 */ "db_optr ::=",
 /*  95 */ "db_optr ::= db_optr cache",
 /*  96 */ "db_optr ::= db_optr replica",
 /*  97 */ "db_optr ::= db_optr quorum",
 /*  98 */ "db_optr ::= db_optr days",
 /*  99 */ "db_optr ::= db_optr minrows",
 /* 100 */ "db_optr ::= db_optr maxrows",
 /* 101 */ "db_optr ::= db_optr blocks",
 /* 102 */ "db_optr ::= db_optr ctime",
 /* 103 */ "db_optr ::= db_optr wal",
 /* 104 */ "db_optr ::= db_optr fsync",
 /* 105 */ "db_optr ::= db_optr comp",
 /* 106 */ "db_optr ::= db_optr prec",
 /* 107 */ "db_optr ::= db_optr keep",
 /* 108 */ "db_optr ::= db_optr update",
 /* 109 */ "db_optr ::= db_optr cachelast",
 /* 110 */ "topic_optr ::= db_optr",
 /* 111 */ "topic_optr ::= topic_optr partitions",
 /* 112 */ "alter_db_optr ::=",
 /* 113 */ "alter_db_optr ::= alter_db_optr replica",
 /* 114 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 115 */ "alter_db_optr ::= alter_db_optr keep",
 /* 116 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 117 */ "alter_db_optr ::= alter_db_optr comp",
 /* 118 */ "alter_db_optr ::= alter_db_optr wal",
 /* 119 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 120 */ "alter_db_optr ::= alter_db_optr update",
 /* 121 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 122 */ "alter_topic_optr ::= alter_db_optr",
 /* 123 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 124 */ "typename ::= ids",
 /* 125 */ "typename ::= ids LP signed RP",
 /* 126 */ "typename ::= ids UNSIGNED",
 /* 127 */ "signed ::= INTEGER",
 /* 128 */ "signed ::= PLUS INTEGER",
 /* 129 */ "signed ::= MINUS INTEGER",
 /* 130 */ "cmd ::= CREATE TABLE create_table_args",
 /* 131 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 133 */ "cmd ::= CREATE TABLE create_table_list",
 /* 134 */ "create_table_list ::= create_from_stable",
 /* 135 */ "create_table_list ::= create_table_list create_from_stable",
 /* 136 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 137 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 139 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 140 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 141 */ "tagNamelist ::= ids",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 143 */ "columnlist ::= columnlist COMMA column",
 /* 144 */ "columnlist ::= column",
 /* 145 */ "column ::= ids typename",
 /* 146 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 147 */ "tagitemlist ::= tagitem",
 /* 148 */ "tagitem ::= INTEGER",
 /* 149 */ "tagitem ::= FLOAT",
 /* 150 */ "tagitem ::= STRING",
 /* 151 */ "tagitem ::= BOOL",
 /* 152 */ "tagitem ::= NULL",
 /* 153 */ "tagitem ::= MINUS INTEGER",
 /* 154 */ "tagitem ::= MINUS FLOAT",
 /* 155 */ "tagitem ::= PLUS INTEGER",
 /* 156 */ "tagitem ::= PLUS FLOAT",
 /* 157 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 158 */ "select ::= LP select RP",
 /* 159 */ "union ::= select",
 /* 160 */ "union ::= union UNION ALL select",
 /* 161 */ "cmd ::= union",
 /* 162 */ "select ::= SELECT selcollist",
 /* 163 */ "sclp ::= selcollist COMMA",
 /* 164 */ "sclp ::=",
 /* 165 */ "selcollist ::= sclp distinct expr as",
 /* 166 */ "selcollist ::= sclp STAR",
 /* 167 */ "as ::= AS ids",
 /* 168 */ "as ::= ids",
 /* 169 */ "as ::=",
 /* 170 */ "distinct ::= DISTINCT",
 /* 171 */ "distinct ::=",
 /* 172 */ "from ::= FROM tablelist",
 /* 173 */ "from ::= FROM sub",
 /* 174 */ "sub ::= LP union RP",
 /* 175 */ "sub ::= LP union RP ids",
 /* 176 */ "sub ::= sub COMMA LP union RP ids",
 /* 177 */ "tablelist ::= ids cpxName",
 /* 178 */ "tablelist ::= ids cpxName ids",
 /* 179 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 180 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 181 */ "tmvar ::= VARIABLE",
 /* 182 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 183 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 184 */ "interval_opt ::=",
 /* 185 */ "session_option ::=",
 /* 186 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 187 */ "windowstate_option ::=",
 /* 188 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 189 */ "fill_opt ::=",
 /* 190 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 191 */ "fill_opt ::= FILL LP ID RP",
 /* 192 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 193 */ "sliding_opt ::=",
 /* 194 */ "orderby_opt ::=",
 /* 195 */ "orderby_opt ::= ORDER BY sortlist",
 /* 196 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 197 */ "sortlist ::= item sortorder",
 /* 198 */ "item ::= ids cpxName",
 /* 199 */ "sortorder ::= ASC",
 /* 200 */ "sortorder ::= DESC",
 /* 201 */ "sortorder ::=",
 /* 202 */ "groupby_opt ::=",
 /* 203 */ "groupby_opt ::= GROUP BY grouplist",
 /* 204 */ "grouplist ::= grouplist COMMA item",
 /* 205 */ "grouplist ::= item",
 /* 206 */ "having_opt ::=",
 /* 207 */ "having_opt ::= HAVING expr",
 /* 208 */ "limit_opt ::=",
 /* 209 */ "limit_opt ::= LIMIT signed",
 /* 210 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 211 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 212 */ "slimit_opt ::=",
 /* 213 */ "slimit_opt ::= SLIMIT signed",
 /* 214 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 215 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 216 */ "where_opt ::=",
 /* 217 */ "where_opt ::= WHERE expr",
 /* 218 */ "expr ::= LP expr RP",
 /* 219 */ "expr ::= ID",
 /* 220 */ "expr ::= ID DOT ID",
 /* 221 */ "expr ::= ID DOT STAR",
 /* 222 */ "expr ::= INTEGER",
 /* 223 */ "expr ::= MINUS INTEGER",
 /* 224 */ "expr ::= PLUS INTEGER",
 /* 225 */ "expr ::= FLOAT",
 /* 226 */ "expr ::= MINUS FLOAT",
 /* 227 */ "expr ::= PLUS FLOAT",
 /* 228 */ "expr ::= STRING",
 /* 229 */ "expr ::= NOW",
 /* 230 */ "expr ::= VARIABLE",
 /* 231 */ "expr ::= PLUS VARIABLE",
 /* 232 */ "expr ::= MINUS VARIABLE",
 /* 233 */ "expr ::= BOOL",
 /* 234 */ "expr ::= NULL",
 /* 235 */ "expr ::= ID LP exprlist RP",
 /* 236 */ "expr ::= ID LP STAR RP",
 /* 237 */ "expr ::= expr IS NULL",
 /* 238 */ "expr ::= expr IS NOT NULL",
 /* 239 */ "expr ::= expr LT expr",
 /* 240 */ "expr ::= expr GT expr",
 /* 241 */ "expr ::= expr LE expr",
 /* 242 */ "expr ::= expr GE expr",
 /* 243 */ "expr ::= expr NE expr",
 /* 244 */ "expr ::= expr EQ expr",
 /* 245 */ "expr ::= expr BETWEEN expr AND expr",
 /* 246 */ "expr ::= expr AND expr",
 /* 247 */ "expr ::= expr OR expr",
 /* 248 */ "expr ::= expr PLUS expr",
 /* 249 */ "expr ::= expr MINUS expr",
 /* 250 */ "expr ::= expr STAR expr",
 /* 251 */ "expr ::= expr SLASH expr",
 /* 252 */ "expr ::= expr REM expr",
 /* 253 */ "expr ::= expr LIKE expr",
 /* 254 */ "expr ::= expr IN LP exprlist RP",
 /* 255 */ "exprlist ::= exprlist COMMA expritem",
 /* 256 */ "exprlist ::= expritem",
 /* 257 */ "expritem ::= expr",
 /* 258 */ "expritem ::=",
 /* 259 */ "cmd ::= RESET QUERY CACHE",
 /* 260 */ "cmd ::= SYNCDB ids REPLICA",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 269 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 270 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 271 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 272 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 277 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 278 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 279 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 210: /* keep */
    case 211: /* tagitemlist */
    case 233: /* columnlist */
    case 234: /* tagNamelist */
    case 244: /* fill_opt */
    case 246: /* groupby_opt */
    case 247: /* orderby_opt */
    case 259: /* sortlist */
    case 263: /* grouplist */
{
taosArrayDestroy((yypminor->yy193));
}
      break;
    case 231: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy270));
}
      break;
    case 235: /* select */
{
destroySqlNode((yypminor->yy124));
}
      break;
    case 238: /* selcollist */
    case 252: /* sclp */
    case 264: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy193));
}
      break;
    case 239: /* from */
    case 256: /* tablelist */
    case 257: /* sub */
{
destroyRelationInfo((yypminor->yy332));
}
      break;
    case 240: /* where_opt */
    case 248: /* having_opt */
    case 254: /* expr */
    case 265: /* expritem */
{
tSqlExprDestroy((yypminor->yy454));
}
      break;
    case 251: /* union */
{
destroyAllSqlNode((yypminor->yy193));
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
   189,  /* (0) program ::= cmd */
   190,  /* (1) cmd ::= SHOW DATABASES */
   190,  /* (2) cmd ::= SHOW TOPICS */
   190,  /* (3) cmd ::= SHOW MNODES */
   190,  /* (4) cmd ::= SHOW DNODES */
   190,  /* (5) cmd ::= SHOW ACCOUNTS */
   190,  /* (6) cmd ::= SHOW USERS */
   190,  /* (7) cmd ::= SHOW MODULES */
   190,  /* (8) cmd ::= SHOW QUERIES */
   190,  /* (9) cmd ::= SHOW CONNECTIONS */
   190,  /* (10) cmd ::= SHOW STREAMS */
   190,  /* (11) cmd ::= SHOW VARIABLES */
   190,  /* (12) cmd ::= SHOW SCORES */
   190,  /* (13) cmd ::= SHOW GRANTS */
   190,  /* (14) cmd ::= SHOW VNODES */
   190,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
   191,  /* (16) dbPrefix ::= */
   191,  /* (17) dbPrefix ::= ids DOT */
   193,  /* (18) cpxName ::= */
   193,  /* (19) cpxName ::= DOT ids */
   190,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   190,  /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
   190,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   190,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   190,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   190,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   190,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   190,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   190,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   190,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   190,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   190,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   190,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   190,  /* (33) cmd ::= DROP DNODE ids */
   190,  /* (34) cmd ::= DROP USER ids */
   190,  /* (35) cmd ::= DROP ACCOUNT ids */
   190,  /* (36) cmd ::= USE ids */
   190,  /* (37) cmd ::= DESCRIBE ids cpxName */
   190,  /* (38) cmd ::= ALTER USER ids PASS ids */
   190,  /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
   190,  /* (40) cmd ::= ALTER DNODE ids ids */
   190,  /* (41) cmd ::= ALTER DNODE ids ids ids */
   190,  /* (42) cmd ::= ALTER LOCAL ids */
   190,  /* (43) cmd ::= ALTER LOCAL ids ids */
   190,  /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
   190,  /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
   190,  /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
   190,  /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   192,  /* (48) ids ::= ID */
   192,  /* (49) ids ::= STRING */
   194,  /* (50) ifexists ::= IF EXISTS */
   194,  /* (51) ifexists ::= */
   198,  /* (52) ifnotexists ::= IF NOT EXISTS */
   198,  /* (53) ifnotexists ::= */
   190,  /* (54) cmd ::= CREATE DNODE ids */
   190,  /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   190,  /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   190,  /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   190,  /* (58) cmd ::= CREATE USER ids PASS ids */
   201,  /* (59) pps ::= */
   201,  /* (60) pps ::= PPS INTEGER */
   202,  /* (61) tseries ::= */
   202,  /* (62) tseries ::= TSERIES INTEGER */
   203,  /* (63) dbs ::= */
   203,  /* (64) dbs ::= DBS INTEGER */
   204,  /* (65) streams ::= */
   204,  /* (66) streams ::= STREAMS INTEGER */
   205,  /* (67) storage ::= */
   205,  /* (68) storage ::= STORAGE INTEGER */
   206,  /* (69) qtime ::= */
   206,  /* (70) qtime ::= QTIME INTEGER */
   207,  /* (71) users ::= */
   207,  /* (72) users ::= USERS INTEGER */
   208,  /* (73) conns ::= */
   208,  /* (74) conns ::= CONNS INTEGER */
   209,  /* (75) state ::= */
   209,  /* (76) state ::= STATE ids */
   197,  /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   210,  /* (78) keep ::= KEEP tagitemlist */
   212,  /* (79) cache ::= CACHE INTEGER */
   213,  /* (80) replica ::= REPLICA INTEGER */
   214,  /* (81) quorum ::= QUORUM INTEGER */
   215,  /* (82) days ::= DAYS INTEGER */
   216,  /* (83) minrows ::= MINROWS INTEGER */
   217,  /* (84) maxrows ::= MAXROWS INTEGER */
   218,  /* (85) blocks ::= BLOCKS INTEGER */
   219,  /* (86) ctime ::= CTIME INTEGER */
   220,  /* (87) wal ::= WAL INTEGER */
   221,  /* (88) fsync ::= FSYNC INTEGER */
   222,  /* (89) comp ::= COMP INTEGER */
   223,  /* (90) prec ::= PRECISION STRING */
   224,  /* (91) update ::= UPDATE INTEGER */
   225,  /* (92) cachelast ::= CACHELAST INTEGER */
   226,  /* (93) partitions ::= PARTITIONS INTEGER */
   199,  /* (94) db_optr ::= */
   199,  /* (95) db_optr ::= db_optr cache */
   199,  /* (96) db_optr ::= db_optr replica */
   199,  /* (97) db_optr ::= db_optr quorum */
   199,  /* (98) db_optr ::= db_optr days */
   199,  /* (99) db_optr ::= db_optr minrows */
   199,  /* (100) db_optr ::= db_optr maxrows */
   199,  /* (101) db_optr ::= db_optr blocks */
   199,  /* (102) db_optr ::= db_optr ctime */
   199,  /* (103) db_optr ::= db_optr wal */
   199,  /* (104) db_optr ::= db_optr fsync */
   199,  /* (105) db_optr ::= db_optr comp */
   199,  /* (106) db_optr ::= db_optr prec */
   199,  /* (107) db_optr ::= db_optr keep */
   199,  /* (108) db_optr ::= db_optr update */
   199,  /* (109) db_optr ::= db_optr cachelast */
   200,  /* (110) topic_optr ::= db_optr */
   200,  /* (111) topic_optr ::= topic_optr partitions */
   195,  /* (112) alter_db_optr ::= */
   195,  /* (113) alter_db_optr ::= alter_db_optr replica */
   195,  /* (114) alter_db_optr ::= alter_db_optr quorum */
   195,  /* (115) alter_db_optr ::= alter_db_optr keep */
   195,  /* (116) alter_db_optr ::= alter_db_optr blocks */
   195,  /* (117) alter_db_optr ::= alter_db_optr comp */
   195,  /* (118) alter_db_optr ::= alter_db_optr wal */
   195,  /* (119) alter_db_optr ::= alter_db_optr fsync */
   195,  /* (120) alter_db_optr ::= alter_db_optr update */
   195,  /* (121) alter_db_optr ::= alter_db_optr cachelast */
   196,  /* (122) alter_topic_optr ::= alter_db_optr */
   196,  /* (123) alter_topic_optr ::= alter_topic_optr partitions */
   227,  /* (124) typename ::= ids */
   227,  /* (125) typename ::= ids LP signed RP */
   227,  /* (126) typename ::= ids UNSIGNED */
   228,  /* (127) signed ::= INTEGER */
   228,  /* (128) signed ::= PLUS INTEGER */
   228,  /* (129) signed ::= MINUS INTEGER */
   190,  /* (130) cmd ::= CREATE TABLE create_table_args */
   190,  /* (131) cmd ::= CREATE TABLE create_stable_args */
   190,  /* (132) cmd ::= CREATE STABLE create_stable_args */
   190,  /* (133) cmd ::= CREATE TABLE create_table_list */
   231,  /* (134) create_table_list ::= create_from_stable */
   231,  /* (135) create_table_list ::= create_table_list create_from_stable */
   229,  /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   230,  /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   232,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   232,  /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   234,  /* (140) tagNamelist ::= tagNamelist COMMA ids */
   234,  /* (141) tagNamelist ::= ids */
   229,  /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
   233,  /* (143) columnlist ::= columnlist COMMA column */
   233,  /* (144) columnlist ::= column */
   236,  /* (145) column ::= ids typename */
   211,  /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
   211,  /* (147) tagitemlist ::= tagitem */
   237,  /* (148) tagitem ::= INTEGER */
   237,  /* (149) tagitem ::= FLOAT */
   237,  /* (150) tagitem ::= STRING */
   237,  /* (151) tagitem ::= BOOL */
   237,  /* (152) tagitem ::= NULL */
   237,  /* (153) tagitem ::= MINUS INTEGER */
   237,  /* (154) tagitem ::= MINUS FLOAT */
   237,  /* (155) tagitem ::= PLUS INTEGER */
   237,  /* (156) tagitem ::= PLUS FLOAT */
   235,  /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   235,  /* (158) select ::= LP select RP */
   251,  /* (159) union ::= select */
   251,  /* (160) union ::= union UNION ALL select */
   190,  /* (161) cmd ::= union */
   235,  /* (162) select ::= SELECT selcollist */
   252,  /* (163) sclp ::= selcollist COMMA */
   252,  /* (164) sclp ::= */
   238,  /* (165) selcollist ::= sclp distinct expr as */
   238,  /* (166) selcollist ::= sclp STAR */
   255,  /* (167) as ::= AS ids */
   255,  /* (168) as ::= ids */
   255,  /* (169) as ::= */
   253,  /* (170) distinct ::= DISTINCT */
   253,  /* (171) distinct ::= */
   239,  /* (172) from ::= FROM tablelist */
   239,  /* (173) from ::= FROM sub */
   257,  /* (174) sub ::= LP union RP */
   257,  /* (175) sub ::= LP union RP ids */
   257,  /* (176) sub ::= sub COMMA LP union RP ids */
   256,  /* (177) tablelist ::= ids cpxName */
   256,  /* (178) tablelist ::= ids cpxName ids */
   256,  /* (179) tablelist ::= tablelist COMMA ids cpxName */
   256,  /* (180) tablelist ::= tablelist COMMA ids cpxName ids */
   258,  /* (181) tmvar ::= VARIABLE */
   241,  /* (182) interval_opt ::= INTERVAL LP tmvar RP */
   241,  /* (183) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   241,  /* (184) interval_opt ::= */
   242,  /* (185) session_option ::= */
   242,  /* (186) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   243,  /* (187) windowstate_option ::= */
   243,  /* (188) windowstate_option ::= STATE_WINDOW LP ids RP */
   244,  /* (189) fill_opt ::= */
   244,  /* (190) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   244,  /* (191) fill_opt ::= FILL LP ID RP */
   245,  /* (192) sliding_opt ::= SLIDING LP tmvar RP */
   245,  /* (193) sliding_opt ::= */
   247,  /* (194) orderby_opt ::= */
   247,  /* (195) orderby_opt ::= ORDER BY sortlist */
   259,  /* (196) sortlist ::= sortlist COMMA item sortorder */
   259,  /* (197) sortlist ::= item sortorder */
   261,  /* (198) item ::= ids cpxName */
   262,  /* (199) sortorder ::= ASC */
   262,  /* (200) sortorder ::= DESC */
   262,  /* (201) sortorder ::= */
   246,  /* (202) groupby_opt ::= */
   246,  /* (203) groupby_opt ::= GROUP BY grouplist */
   263,  /* (204) grouplist ::= grouplist COMMA item */
   263,  /* (205) grouplist ::= item */
   248,  /* (206) having_opt ::= */
   248,  /* (207) having_opt ::= HAVING expr */
   250,  /* (208) limit_opt ::= */
   250,  /* (209) limit_opt ::= LIMIT signed */
   250,  /* (210) limit_opt ::= LIMIT signed OFFSET signed */
   250,  /* (211) limit_opt ::= LIMIT signed COMMA signed */
   249,  /* (212) slimit_opt ::= */
   249,  /* (213) slimit_opt ::= SLIMIT signed */
   249,  /* (214) slimit_opt ::= SLIMIT signed SOFFSET signed */
   249,  /* (215) slimit_opt ::= SLIMIT signed COMMA signed */
   240,  /* (216) where_opt ::= */
   240,  /* (217) where_opt ::= WHERE expr */
   254,  /* (218) expr ::= LP expr RP */
   254,  /* (219) expr ::= ID */
   254,  /* (220) expr ::= ID DOT ID */
   254,  /* (221) expr ::= ID DOT STAR */
   254,  /* (222) expr ::= INTEGER */
   254,  /* (223) expr ::= MINUS INTEGER */
   254,  /* (224) expr ::= PLUS INTEGER */
   254,  /* (225) expr ::= FLOAT */
   254,  /* (226) expr ::= MINUS FLOAT */
   254,  /* (227) expr ::= PLUS FLOAT */
   254,  /* (228) expr ::= STRING */
   254,  /* (229) expr ::= NOW */
   254,  /* (230) expr ::= VARIABLE */
   254,  /* (231) expr ::= PLUS VARIABLE */
   254,  /* (232) expr ::= MINUS VARIABLE */
   254,  /* (233) expr ::= BOOL */
   254,  /* (234) expr ::= NULL */
   254,  /* (235) expr ::= ID LP exprlist RP */
   254,  /* (236) expr ::= ID LP STAR RP */
   254,  /* (237) expr ::= expr IS NULL */
   254,  /* (238) expr ::= expr IS NOT NULL */
   254,  /* (239) expr ::= expr LT expr */
   254,  /* (240) expr ::= expr GT expr */
   254,  /* (241) expr ::= expr LE expr */
   254,  /* (242) expr ::= expr GE expr */
   254,  /* (243) expr ::= expr NE expr */
   254,  /* (244) expr ::= expr EQ expr */
   254,  /* (245) expr ::= expr BETWEEN expr AND expr */
   254,  /* (246) expr ::= expr AND expr */
   254,  /* (247) expr ::= expr OR expr */
   254,  /* (248) expr ::= expr PLUS expr */
   254,  /* (249) expr ::= expr MINUS expr */
   254,  /* (250) expr ::= expr STAR expr */
   254,  /* (251) expr ::= expr SLASH expr */
   254,  /* (252) expr ::= expr REM expr */
   254,  /* (253) expr ::= expr LIKE expr */
   254,  /* (254) expr ::= expr IN LP exprlist RP */
   264,  /* (255) exprlist ::= exprlist COMMA expritem */
   264,  /* (256) exprlist ::= expritem */
   265,  /* (257) expritem ::= expr */
   265,  /* (258) expritem ::= */
   190,  /* (259) cmd ::= RESET QUERY CACHE */
   190,  /* (260) cmd ::= SYNCDB ids REPLICA */
   190,  /* (261) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   190,  /* (262) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   190,  /* (263) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   190,  /* (264) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   190,  /* (265) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   190,  /* (266) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   190,  /* (267) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   190,  /* (268) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   190,  /* (269) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   190,  /* (270) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   190,  /* (271) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   190,  /* (272) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   190,  /* (273) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   190,  /* (274) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   190,  /* (275) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   190,  /* (276) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   190,  /* (277) cmd ::= KILL CONNECTION INTEGER */
   190,  /* (278) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   190,  /* (279) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW TOPICS */
   -2,  /* (3) cmd ::= SHOW MNODES */
   -2,  /* (4) cmd ::= SHOW DNODES */
   -2,  /* (5) cmd ::= SHOW ACCOUNTS */
   -2,  /* (6) cmd ::= SHOW USERS */
   -2,  /* (7) cmd ::= SHOW MODULES */
   -2,  /* (8) cmd ::= SHOW QUERIES */
   -2,  /* (9) cmd ::= SHOW CONNECTIONS */
   -2,  /* (10) cmd ::= SHOW STREAMS */
   -2,  /* (11) cmd ::= SHOW VARIABLES */
   -2,  /* (12) cmd ::= SHOW SCORES */
   -2,  /* (13) cmd ::= SHOW GRANTS */
   -2,  /* (14) cmd ::= SHOW VNODES */
   -3,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (16) dbPrefix ::= */
   -2,  /* (17) dbPrefix ::= ids DOT */
    0,  /* (18) cpxName ::= */
   -2,  /* (19) cpxName ::= DOT ids */
   -5,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   -5,  /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
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
   -3,  /* (33) cmd ::= DROP DNODE ids */
   -3,  /* (34) cmd ::= DROP USER ids */
   -3,  /* (35) cmd ::= DROP ACCOUNT ids */
   -2,  /* (36) cmd ::= USE ids */
   -3,  /* (37) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (38) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (40) cmd ::= ALTER DNODE ids ids */
   -5,  /* (41) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (42) cmd ::= ALTER LOCAL ids */
   -4,  /* (43) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (48) ids ::= ID */
   -1,  /* (49) ids ::= STRING */
   -2,  /* (50) ifexists ::= IF EXISTS */
    0,  /* (51) ifexists ::= */
   -3,  /* (52) ifnotexists ::= IF NOT EXISTS */
    0,  /* (53) ifnotexists ::= */
   -3,  /* (54) cmd ::= CREATE DNODE ids */
   -6,  /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -5,  /* (58) cmd ::= CREATE USER ids PASS ids */
    0,  /* (59) pps ::= */
   -2,  /* (60) pps ::= PPS INTEGER */
    0,  /* (61) tseries ::= */
   -2,  /* (62) tseries ::= TSERIES INTEGER */
    0,  /* (63) dbs ::= */
   -2,  /* (64) dbs ::= DBS INTEGER */
    0,  /* (65) streams ::= */
   -2,  /* (66) streams ::= STREAMS INTEGER */
    0,  /* (67) storage ::= */
   -2,  /* (68) storage ::= STORAGE INTEGER */
    0,  /* (69) qtime ::= */
   -2,  /* (70) qtime ::= QTIME INTEGER */
    0,  /* (71) users ::= */
   -2,  /* (72) users ::= USERS INTEGER */
    0,  /* (73) conns ::= */
   -2,  /* (74) conns ::= CONNS INTEGER */
    0,  /* (75) state ::= */
   -2,  /* (76) state ::= STATE ids */
   -9,  /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (78) keep ::= KEEP tagitemlist */
   -2,  /* (79) cache ::= CACHE INTEGER */
   -2,  /* (80) replica ::= REPLICA INTEGER */
   -2,  /* (81) quorum ::= QUORUM INTEGER */
   -2,  /* (82) days ::= DAYS INTEGER */
   -2,  /* (83) minrows ::= MINROWS INTEGER */
   -2,  /* (84) maxrows ::= MAXROWS INTEGER */
   -2,  /* (85) blocks ::= BLOCKS INTEGER */
   -2,  /* (86) ctime ::= CTIME INTEGER */
   -2,  /* (87) wal ::= WAL INTEGER */
   -2,  /* (88) fsync ::= FSYNC INTEGER */
   -2,  /* (89) comp ::= COMP INTEGER */
   -2,  /* (90) prec ::= PRECISION STRING */
   -2,  /* (91) update ::= UPDATE INTEGER */
   -2,  /* (92) cachelast ::= CACHELAST INTEGER */
   -2,  /* (93) partitions ::= PARTITIONS INTEGER */
    0,  /* (94) db_optr ::= */
   -2,  /* (95) db_optr ::= db_optr cache */
   -2,  /* (96) db_optr ::= db_optr replica */
   -2,  /* (97) db_optr ::= db_optr quorum */
   -2,  /* (98) db_optr ::= db_optr days */
   -2,  /* (99) db_optr ::= db_optr minrows */
   -2,  /* (100) db_optr ::= db_optr maxrows */
   -2,  /* (101) db_optr ::= db_optr blocks */
   -2,  /* (102) db_optr ::= db_optr ctime */
   -2,  /* (103) db_optr ::= db_optr wal */
   -2,  /* (104) db_optr ::= db_optr fsync */
   -2,  /* (105) db_optr ::= db_optr comp */
   -2,  /* (106) db_optr ::= db_optr prec */
   -2,  /* (107) db_optr ::= db_optr keep */
   -2,  /* (108) db_optr ::= db_optr update */
   -2,  /* (109) db_optr ::= db_optr cachelast */
   -1,  /* (110) topic_optr ::= db_optr */
   -2,  /* (111) topic_optr ::= topic_optr partitions */
    0,  /* (112) alter_db_optr ::= */
   -2,  /* (113) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (114) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (115) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (116) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (117) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (118) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (119) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (120) alter_db_optr ::= alter_db_optr update */
   -2,  /* (121) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (122) alter_topic_optr ::= alter_db_optr */
   -2,  /* (123) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (124) typename ::= ids */
   -4,  /* (125) typename ::= ids LP signed RP */
   -2,  /* (126) typename ::= ids UNSIGNED */
   -1,  /* (127) signed ::= INTEGER */
   -2,  /* (128) signed ::= PLUS INTEGER */
   -2,  /* (129) signed ::= MINUS INTEGER */
   -3,  /* (130) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (131) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (132) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (133) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (134) create_table_list ::= create_from_stable */
   -2,  /* (135) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (140) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (141) tagNamelist ::= ids */
   -5,  /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (143) columnlist ::= columnlist COMMA column */
   -1,  /* (144) columnlist ::= column */
   -2,  /* (145) column ::= ids typename */
   -3,  /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (147) tagitemlist ::= tagitem */
   -1,  /* (148) tagitem ::= INTEGER */
   -1,  /* (149) tagitem ::= FLOAT */
   -1,  /* (150) tagitem ::= STRING */
   -1,  /* (151) tagitem ::= BOOL */
   -1,  /* (152) tagitem ::= NULL */
   -2,  /* (153) tagitem ::= MINUS INTEGER */
   -2,  /* (154) tagitem ::= MINUS FLOAT */
   -2,  /* (155) tagitem ::= PLUS INTEGER */
   -2,  /* (156) tagitem ::= PLUS FLOAT */
  -14,  /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -3,  /* (158) select ::= LP select RP */
   -1,  /* (159) union ::= select */
   -4,  /* (160) union ::= union UNION ALL select */
   -1,  /* (161) cmd ::= union */
   -2,  /* (162) select ::= SELECT selcollist */
   -2,  /* (163) sclp ::= selcollist COMMA */
    0,  /* (164) sclp ::= */
   -4,  /* (165) selcollist ::= sclp distinct expr as */
   -2,  /* (166) selcollist ::= sclp STAR */
   -2,  /* (167) as ::= AS ids */
   -1,  /* (168) as ::= ids */
    0,  /* (169) as ::= */
   -1,  /* (170) distinct ::= DISTINCT */
    0,  /* (171) distinct ::= */
   -2,  /* (172) from ::= FROM tablelist */
   -2,  /* (173) from ::= FROM sub */
   -3,  /* (174) sub ::= LP union RP */
   -4,  /* (175) sub ::= LP union RP ids */
   -6,  /* (176) sub ::= sub COMMA LP union RP ids */
   -2,  /* (177) tablelist ::= ids cpxName */
   -3,  /* (178) tablelist ::= ids cpxName ids */
   -4,  /* (179) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (180) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (181) tmvar ::= VARIABLE */
   -4,  /* (182) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (183) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (184) interval_opt ::= */
    0,  /* (185) session_option ::= */
   -7,  /* (186) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (187) windowstate_option ::= */
   -4,  /* (188) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (189) fill_opt ::= */
   -6,  /* (190) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (191) fill_opt ::= FILL LP ID RP */
   -4,  /* (192) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (193) sliding_opt ::= */
    0,  /* (194) orderby_opt ::= */
   -3,  /* (195) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (196) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (197) sortlist ::= item sortorder */
   -2,  /* (198) item ::= ids cpxName */
   -1,  /* (199) sortorder ::= ASC */
   -1,  /* (200) sortorder ::= DESC */
    0,  /* (201) sortorder ::= */
    0,  /* (202) groupby_opt ::= */
   -3,  /* (203) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (204) grouplist ::= grouplist COMMA item */
   -1,  /* (205) grouplist ::= item */
    0,  /* (206) having_opt ::= */
   -2,  /* (207) having_opt ::= HAVING expr */
    0,  /* (208) limit_opt ::= */
   -2,  /* (209) limit_opt ::= LIMIT signed */
   -4,  /* (210) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (211) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (212) slimit_opt ::= */
   -2,  /* (213) slimit_opt ::= SLIMIT signed */
   -4,  /* (214) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (215) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (216) where_opt ::= */
   -2,  /* (217) where_opt ::= WHERE expr */
   -3,  /* (218) expr ::= LP expr RP */
   -1,  /* (219) expr ::= ID */
   -3,  /* (220) expr ::= ID DOT ID */
   -3,  /* (221) expr ::= ID DOT STAR */
   -1,  /* (222) expr ::= INTEGER */
   -2,  /* (223) expr ::= MINUS INTEGER */
   -2,  /* (224) expr ::= PLUS INTEGER */
   -1,  /* (225) expr ::= FLOAT */
   -2,  /* (226) expr ::= MINUS FLOAT */
   -2,  /* (227) expr ::= PLUS FLOAT */
   -1,  /* (228) expr ::= STRING */
   -1,  /* (229) expr ::= NOW */
   -1,  /* (230) expr ::= VARIABLE */
   -2,  /* (231) expr ::= PLUS VARIABLE */
   -2,  /* (232) expr ::= MINUS VARIABLE */
   -1,  /* (233) expr ::= BOOL */
   -1,  /* (234) expr ::= NULL */
   -4,  /* (235) expr ::= ID LP exprlist RP */
   -4,  /* (236) expr ::= ID LP STAR RP */
   -3,  /* (237) expr ::= expr IS NULL */
   -4,  /* (238) expr ::= expr IS NOT NULL */
   -3,  /* (239) expr ::= expr LT expr */
   -3,  /* (240) expr ::= expr GT expr */
   -3,  /* (241) expr ::= expr LE expr */
   -3,  /* (242) expr ::= expr GE expr */
   -3,  /* (243) expr ::= expr NE expr */
   -3,  /* (244) expr ::= expr EQ expr */
   -5,  /* (245) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (246) expr ::= expr AND expr */
   -3,  /* (247) expr ::= expr OR expr */
   -3,  /* (248) expr ::= expr PLUS expr */
   -3,  /* (249) expr ::= expr MINUS expr */
   -3,  /* (250) expr ::= expr STAR expr */
   -3,  /* (251) expr ::= expr SLASH expr */
   -3,  /* (252) expr ::= expr REM expr */
   -3,  /* (253) expr ::= expr LIKE expr */
   -5,  /* (254) expr ::= expr IN LP exprlist RP */
   -3,  /* (255) exprlist ::= exprlist COMMA expritem */
   -1,  /* (256) exprlist ::= expritem */
   -1,  /* (257) expritem ::= expr */
    0,  /* (258) expritem ::= */
   -3,  /* (259) cmd ::= RESET QUERY CACHE */
   -3,  /* (260) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (261) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (262) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (263) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (264) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (265) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (266) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (267) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (268) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (269) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (270) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (271) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (272) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (273) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (274) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (275) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (276) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (277) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (278) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (279) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 130: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==132);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 10: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 15: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 16: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 17: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 18: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 19: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 20: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
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
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
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
      case 33: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 34: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 35: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 36: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 38: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 39: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 45: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==45);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy114, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy183);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);}
        break;
      case 48: /* ids ::= ID */
      case 49: /* ids ::= STRING */ yytestcase(yyruleno==49);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 50: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 51: /* ifexists ::= */
      case 53: /* ifnotexists ::= */ yytestcase(yyruleno==53);
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 52: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy183);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy114, &yymsp[-2].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 59: /* pps ::= */
      case 61: /* tseries ::= */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= */ yytestcase(yyruleno==63);
      case 65: /* streams ::= */ yytestcase(yyruleno==65);
      case 67: /* storage ::= */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= */ yytestcase(yyruleno==69);
      case 71: /* users ::= */ yytestcase(yyruleno==71);
      case 73: /* conns ::= */ yytestcase(yyruleno==73);
      case 75: /* state ::= */ yytestcase(yyruleno==75);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 60: /* pps ::= PPS INTEGER */
      case 62: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==64);
      case 66: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==70);
      case 72: /* users ::= USERS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* state ::= STATE ids */ yytestcase(yyruleno==76);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 77: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
      case 78: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy193 = yymsp[0].minor.yy193; }
        break;
      case 79: /* cache ::= CACHE INTEGER */
      case 80: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==80);
      case 81: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==81);
      case 82: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==86);
      case 87: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==87);
      case 88: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==88);
      case 89: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==89);
      case 90: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==90);
      case 91: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==91);
      case 92: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==92);
      case 93: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==93);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 94: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy114); yymsp[1].minor.yy114.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 95: /* db_optr ::= db_optr cache */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 96: /* db_optr ::= db_optr replica */
      case 113: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==113);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 97: /* db_optr ::= db_optr quorum */
      case 114: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==114);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 98: /* db_optr ::= db_optr days */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 99: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 100: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 101: /* db_optr ::= db_optr blocks */
      case 116: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==116);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 102: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 103: /* db_optr ::= db_optr wal */
      case 118: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==118);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 104: /* db_optr ::= db_optr fsync */
      case 119: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==119);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 105: /* db_optr ::= db_optr comp */
      case 117: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==117);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 106: /* db_optr ::= db_optr prec */
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 107: /* db_optr ::= db_optr keep */
      case 115: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==115);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.keep = yymsp[0].minor.yy193; }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 108: /* db_optr ::= db_optr update */
      case 120: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==120);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 109: /* db_optr ::= db_optr cachelast */
      case 121: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==121);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 110: /* topic_optr ::= db_optr */
      case 122: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==122);
{ yylhsminor.yy114 = yymsp[0].minor.yy114; yylhsminor.yy114.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy114 = yylhsminor.yy114;
        break;
      case 111: /* topic_optr ::= topic_optr partitions */
      case 123: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==123);
{ yylhsminor.yy114 = yymsp[-1].minor.yy114; yylhsminor.yy114.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 112: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy114); yymsp[1].minor.yy114.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 124: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy27, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy27 = yylhsminor.yy27;
        break;
      case 125: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy473 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy27, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy473;  // negative value of name length
    tSetColumnType(&yylhsminor.yy27, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy27 = yylhsminor.yy27;
        break;
      case 126: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy27, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy27 = yylhsminor.yy27;
        break;
      case 127: /* signed ::= INTEGER */
{ yylhsminor.yy473 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy473 = yylhsminor.yy473;
        break;
      case 128: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy473 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 129: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy473 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 133: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy270;}
        break;
      case 134: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy192);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy270 = pCreateTable;
}
  yymsp[0].minor.yy270 = yylhsminor.yy270;
        break;
      case 135: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy270->childTableInfo, &yymsp[0].minor.yy192);
  yylhsminor.yy270 = yymsp[-1].minor.yy270;
}
  yymsp[-1].minor.yy270 = yylhsminor.yy270;
        break;
      case 136: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy270 = tSetCreateTableInfo(yymsp[-1].minor.yy193, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy270, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy270 = yylhsminor.yy270;
        break;
      case 137: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy270 = tSetCreateTableInfo(yymsp[-5].minor.yy193, yymsp[-1].minor.yy193, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy270, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy270 = yylhsminor.yy270;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy193, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy192 = yylhsminor.yy192;
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy192 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy193, yymsp[-1].minor.yy193, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy192 = yylhsminor.yy192;
        break;
      case 140: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy193, &yymsp[0].minor.yy0); yylhsminor.yy193 = yymsp[-2].minor.yy193;  }
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 141: /* tagNamelist ::= ids */
{yylhsminor.yy193 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy193, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy270 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy124, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy270, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy270 = yylhsminor.yy270;
        break;
      case 143: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy193, &yymsp[0].minor.yy27); yylhsminor.yy193 = yymsp[-2].minor.yy193;  }
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 144: /* columnlist ::= column */
{yylhsminor.yy193 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy193, &yymsp[0].minor.yy27);}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 145: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy27, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy27);
}
  yymsp[-1].minor.yy27 = yylhsminor.yy27;
        break;
      case 146: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy193 = tVariantListAppend(yymsp[-2].minor.yy193, &yymsp[0].minor.yy442, -1);    }
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 147: /* tagitemlist ::= tagitem */
{ yylhsminor.yy193 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 148: /* tagitem ::= INTEGER */
      case 149: /* tagitem ::= FLOAT */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= STRING */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= BOOL */ yytestcase(yyruleno==151);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 152: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 153: /* tagitem ::= MINUS INTEGER */
      case 154: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 157: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy124 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy193, yymsp[-11].minor.yy332, yymsp[-10].minor.yy454, yymsp[-4].minor.yy193, yymsp[-3].minor.yy193, &yymsp[-9].minor.yy392, &yymsp[-8].minor.yy447, &yymsp[-7].minor.yy76, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy193, &yymsp[0].minor.yy482, &yymsp[-1].minor.yy482, yymsp[-2].minor.yy454);
}
  yymsp[-13].minor.yy124 = yylhsminor.yy124;
        break;
      case 158: /* select ::= LP select RP */
{yymsp[-2].minor.yy124 = yymsp[-1].minor.yy124;}
        break;
      case 159: /* union ::= select */
{ yylhsminor.yy193 = setSubclause(NULL, yymsp[0].minor.yy124); }
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 160: /* union ::= union UNION ALL select */
{ yylhsminor.yy193 = appendSelectClause(yymsp[-3].minor.yy193, yymsp[0].minor.yy124); }
  yymsp[-3].minor.yy193 = yylhsminor.yy193;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy193, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy124 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy193, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy124 = yylhsminor.yy124;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy193 = yymsp[-1].minor.yy193;}
  yymsp[-1].minor.yy193 = yylhsminor.yy193;
        break;
      case 164: /* sclp ::= */
      case 194: /* orderby_opt ::= */ yytestcase(yyruleno==194);
{yymsp[1].minor.yy193 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy193 = tSqlExprListAppend(yymsp[-3].minor.yy193, yymsp[-1].minor.yy454,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy193 = yylhsminor.yy193;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy193 = tSqlExprListAppend(yymsp[-1].minor.yy193, pNode, 0, 0);
}
  yymsp[-1].minor.yy193 = yylhsminor.yy193;
        break;
      case 167: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 168: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 170: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* from ::= FROM tablelist */
      case 173: /* from ::= FROM sub */ yytestcase(yyruleno==173);
{yymsp[-1].minor.yy332 = yymsp[0].minor.yy332;}
        break;
      case 174: /* sub ::= LP union RP */
{yymsp[-2].minor.yy332 = addSubqueryElem(NULL, yymsp[-1].minor.yy193, NULL);}
        break;
      case 175: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy332 = addSubqueryElem(NULL, yymsp[-2].minor.yy193, &yymsp[0].minor.yy0);}
        break;
      case 176: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy332 = addSubqueryElem(yymsp[-5].minor.yy332, yymsp[-2].minor.yy193, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy332 = yylhsminor.yy332;
        break;
      case 177: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy332 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy332 = yylhsminor.yy332;
        break;
      case 178: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy332 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy332 = yylhsminor.yy332;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy332 = setTableNameList(yymsp[-3].minor.yy332, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy332 = yylhsminor.yy332;
        break;
      case 180: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy332 = setTableNameList(yymsp[-4].minor.yy332, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy332 = yylhsminor.yy332;
        break;
      case 181: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy392.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy392.offset.n = 0;}
        break;
      case 183: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy392.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy392.offset = yymsp[-1].minor.yy0;}
        break;
      case 184: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy392, 0, sizeof(yymsp[1].minor.yy392));}
        break;
      case 185: /* session_option ::= */
{yymsp[1].minor.yy447.col.n = 0; yymsp[1].minor.yy447.gap.n = 0;}
        break;
      case 186: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy447.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy447.gap = yymsp[-1].minor.yy0;
}
        break;
      case 187: /* windowstate_option ::= */
{yymsp[1].minor.yy76.col.n = 0;}
        break;
      case 188: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yymsp[-3].minor.yy76.col = yymsp[-1].minor.yy0;
}
        break;
      case 189: /* fill_opt ::= */
{ yymsp[1].minor.yy193 = 0;     }
        break;
      case 190: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy193, &A, -1, 0);
    yymsp[-5].minor.yy193 = yymsp[-1].minor.yy193;
}
        break;
      case 191: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy193 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 192: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 193: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 195: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 196: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy193 = tVariantListAppend(yymsp[-3].minor.yy193, &yymsp[-1].minor.yy442, yymsp[0].minor.yy312);
}
  yymsp[-3].minor.yy193 = yylhsminor.yy193;
        break;
      case 197: /* sortlist ::= item sortorder */
{
  yylhsminor.yy193 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy312);
}
  yymsp[-1].minor.yy193 = yylhsminor.yy193;
        break;
      case 198: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 199: /* sortorder ::= ASC */
{ yymsp[0].minor.yy312 = TSDB_ORDER_ASC; }
        break;
      case 200: /* sortorder ::= DESC */
{ yymsp[0].minor.yy312 = TSDB_ORDER_DESC;}
        break;
      case 201: /* sortorder ::= */
{ yymsp[1].minor.yy312 = TSDB_ORDER_ASC; }
        break;
      case 202: /* groupby_opt ::= */
{ yymsp[1].minor.yy193 = 0;}
        break;
      case 203: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy193 = yymsp[0].minor.yy193;}
        break;
      case 204: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy193 = tVariantListAppend(yymsp[-2].minor.yy193, &yymsp[0].minor.yy442, -1);
}
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 205: /* grouplist ::= item */
{
  yylhsminor.yy193 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 206: /* having_opt ::= */
      case 216: /* where_opt ::= */ yytestcase(yyruleno==216);
      case 258: /* expritem ::= */ yytestcase(yyruleno==258);
{yymsp[1].minor.yy454 = 0;}
        break;
      case 207: /* having_opt ::= HAVING expr */
      case 217: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==217);
{yymsp[-1].minor.yy454 = yymsp[0].minor.yy454;}
        break;
      case 208: /* limit_opt ::= */
      case 212: /* slimit_opt ::= */ yytestcase(yyruleno==212);
{yymsp[1].minor.yy482.limit = -1; yymsp[1].minor.yy482.offset = 0;}
        break;
      case 209: /* limit_opt ::= LIMIT signed */
      case 213: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==213);
{yymsp[-1].minor.yy482.limit = yymsp[0].minor.yy473;  yymsp[-1].minor.yy482.offset = 0;}
        break;
      case 210: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy482.limit = yymsp[-2].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[0].minor.yy473;}
        break;
      case 211: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy482.limit = yymsp[0].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[-2].minor.yy473;}
        break;
      case 214: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy482.limit = yymsp[-2].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[0].minor.yy473;}
        break;
      case 215: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy482.limit = yymsp[0].minor.yy473;  yymsp[-3].minor.yy482.offset = yymsp[-2].minor.yy473;}
        break;
      case 218: /* expr ::= LP expr RP */
{yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy454->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 219: /* expr ::= ID */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 220: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 221: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 222: /* expr ::= INTEGER */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 223: /* expr ::= MINUS INTEGER */
      case 224: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==224);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 225: /* expr ::= FLOAT */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 226: /* expr ::= MINUS FLOAT */
      case 227: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==227);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 228: /* expr ::= STRING */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 229: /* expr ::= NOW */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 230: /* expr ::= VARIABLE */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 231: /* expr ::= PLUS VARIABLE */
      case 232: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==232);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 233: /* expr ::= BOOL */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 234: /* expr ::= NULL */
{ yylhsminor.yy454 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 235: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy454 = tSqlExprCreateFunction(yymsp[-1].minor.yy193, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy454 = yylhsminor.yy454;
        break;
      case 236: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy454 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy454 = yylhsminor.yy454;
        break;
      case 237: /* expr ::= expr IS NULL */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 238: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-3].minor.yy454, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy454 = yylhsminor.yy454;
        break;
      case 239: /* expr ::= expr LT expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_LT);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 240: /* expr ::= expr GT expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_GT);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 241: /* expr ::= expr LE expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_LE);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 242: /* expr ::= expr GE expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_GE);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 243: /* expr ::= expr NE expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_NE);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 244: /* expr ::= expr EQ expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_EQ);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 245: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy454); yylhsminor.yy454 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy454, yymsp[-2].minor.yy454, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy454, TK_LE), TK_AND);}
  yymsp[-4].minor.yy454 = yylhsminor.yy454;
        break;
      case 246: /* expr ::= expr AND expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_AND);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 247: /* expr ::= expr OR expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_OR); }
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 248: /* expr ::= expr PLUS expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_PLUS);  }
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 249: /* expr ::= expr MINUS expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_MINUS); }
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 250: /* expr ::= expr STAR expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_STAR);  }
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 251: /* expr ::= expr SLASH expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_DIVIDE);}
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 252: /* expr ::= expr REM expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_REM);   }
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 253: /* expr ::= expr LIKE expr */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-2].minor.yy454, yymsp[0].minor.yy454, TK_LIKE);  }
  yymsp[-2].minor.yy454 = yylhsminor.yy454;
        break;
      case 254: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy454 = tSqlExprCreate(yymsp[-4].minor.yy454, (tSqlExpr*)yymsp[-1].minor.yy193, TK_IN); }
  yymsp[-4].minor.yy454 = yylhsminor.yy454;
        break;
      case 255: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy193 = tSqlExprListAppend(yymsp[-2].minor.yy193,yymsp[0].minor.yy454,0, 0);}
  yymsp[-2].minor.yy193 = yylhsminor.yy193;
        break;
      case 256: /* exprlist ::= expritem */
{yylhsminor.yy193 = tSqlExprListAppend(0,yymsp[0].minor.yy454,0, 0);}
  yymsp[0].minor.yy193 = yylhsminor.yy193;
        break;
      case 257: /* expritem ::= expr */
{yylhsminor.yy454 = yymsp[0].minor.yy454;}
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 259: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 260: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy193, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 278: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 279: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
