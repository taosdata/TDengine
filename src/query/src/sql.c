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
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SArray* yy15;
  SWindowStateVal yy96;
  SSqlNode* yy134;
  SLimitVal yy150;
  SSessionWindowVal yy151;
  SRelationInfo* yy160;
  int yy250;
  tSqlExpr* yy328;
  tVariant yy380;
  SCreatedTableInfo yy390;
  SCreateAcctInfo yy397;
  SCreateDbInfo yy454;
  SCreateTableSql* yy482;
  int64_t yy489;
  SIntervalVal yy496;
  TAOS_FIELD yy505;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             341
#define YYNRULE              280
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
#define YY_ACTTAB_COUNT (721)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   989,  586,  217,  338,  954,   22,  223,  192,  194,  587,
 /*    10 */   819,  340,  198,   52,   53,  151,   56,   57,  226, 1077,
 /*    20 */   229,   46, 1073,   55,  282,   60,   58,   62,   59, 1072,
 /*    30 */   665,  194,  968,   51,   50,  194,  234,   49,   48,   47,
 /*    40 */    52,   53, 1076,   56,   57,  225, 1077,  229,   46,  586,
 /*    50 */    55,  282,   60,   58,   62,   59,  980,  587,  314,  313,
 /*    60 */    51,   50,  968,  986,   49,   48,   47,   53,   35,   56,
 /*    70 */    57,  144,  258,  229,   46,   75,   55,  282,   60,   58,
 /*    80 */    62,   59,  279,  298,   87,  867,   51,   50,   94,  178,
 /*    90 */    49,   48,   47,  539,  540,  541,  542,  543,  544,  545,
 /*   100 */   546,  547,  548,  549,  550,  551,  339,  953,  298,  218,
 /*   110 */    76,  586,  964,   52,   53,   35,   56,   57,  324,  587,
 /*   120 */   229,   46,  956,   55,  282,   60,   58,   62,   59,   49,
 /*   130 */    48,   47,  756,   51,   50,  265,  264,   49,   48,   47,
 /*   140 */    52,   54,  980,   56,   57, 1071,  980,  229,   46,  586,
 /*   150 */    55,  282,   60,   58,   62,   59,  219,  587,  220,  965,
 /*   160 */    51,   50,  221,  212,   49,   48,   47,   28,  296,  333,
 /*   170 */   332,  295,  294,  293,  331,  292,  330,  329,  328,  291,
 /*   180 */   327,  326,  928,   35,  916,  917,  918,  919,  920,  921,
 /*   190 */   922,  923,  924,  925,  926,  927,  929,  930,   56,   57,
 /*   200 */   151,  213,  229,   46,  151,   55,  282,   60,   58,   62,
 /*   210 */    59,  962,   23,   91,   29,   51,   50,  243,  196,   49,
 /*   220 */    48,   47,  228,  771,  232,  197,  760,  965,  763,  203,
 /*   230 */   766,  228,  771,  151,   13,  760,  204,  763,   93,  766,
 /*   240 */    90,  128,  127,  202,  951,  952,   34,  955,   60,   58,
 /*   250 */    62,   59,  966,  235,  214,  215,   51,   50,  281, 1096,
 /*   260 */    49,   48,   47,  214,  215,  337,  336,  136,   28, 1025,
 /*   270 */   333,  332,   82,   88,  250,  331,  704,  330,  329,  328,
 /*   280 */    41,  327,  326,  254,  253,  936,   35,  199,  934,  935,
 /*   290 */   142,  140,  139,  937,  876,  939,  940,  938,  178,  941,
 /*   300 */   942,   82, 1026,  257,  277,   74,  113,  107,  118,   41,
 /*   310 */   236,  334,  211,  117,  123,  126,  116,  239,  762,  193,
 /*   320 */   765,   35,  120,  242,  761,   61,  764,  233,  689, 1088,
 /*   330 */   965,  686,  772,  687,   61,  688,  968,  968,  768,  200,
 /*   340 */   115,  772,    5,   38,  168,  324,  283,  768,   79,  167,
 /*   350 */   101,   96,  100,  868,  708,  767,  261,  178,   35,  245,
 /*   360 */   246,  201,  303,   35,  767,  965,  186,  184,  182,   35,
 /*   370 */    35,   35,   89,  181,  131,  130,  129,   51,   50,   35,
 /*   380 */    35,   49,   48,   47,    1,  166,   77,  240,  967,  238,
 /*   390 */   259,  302,  301,  244,   36,  241,  701,  309,  308,  304,
 /*   400 */     3,  179,  965,    8,  305,  758,  227,  965,  737,  738,
 /*   410 */   306,  310,  311,  965,  965,  965,   67,   80,  207,   70,
 /*   420 */   312,  316,  720,  965,  965,  261,  146,  728,  729,  792,
 /*   430 */   769,   66,   25,   24,  773, 1036,  770,  690,   24,   68,
 /*   440 */    71,  759,  775,  675,  285,  208,   33,   36,   36,  288,
 /*   450 */     6,  206,  677,  287,  676,  255,   66,   92,   66,   73,
 /*   460 */    15,  664,   14,  693,  106,  694,  105,   17,  691,   16,
 /*   470 */   692,   19,  112,   18,  111,   21, 1035,   20,  125,  124,
 /*   480 */   191,  205,  195,  230,  143, 1032, 1031,  231,  315,  988,
 /*   490 */    44,  999,  996,  997, 1001,  145,  981,  262,  149,  271,
 /*   500 */  1018, 1017,  963,  162,  163,  141,  932,  961,  164,  165,
 /*   510 */   719,  879,  266,  290,  325,  160,  158,  276,  153,  978,
 /*   520 */    42,  152,   63,  189,  222,   72,  268,   39,  299,  275,
 /*   530 */   875,   69,  280,  300, 1095,  278,  103,  154, 1094,  155,
 /*   540 */   156,  274,  157, 1091,  169,  307, 1087,  109,  272, 1086,
 /*   550 */  1083,  170,  897,   40,   37,   43,  190,  864,  119,  862,
 /*   560 */   121,  122,  860,  859,  247,  180,  857,  856,  855,  854,
 /*   570 */   853,  852,  183,  185,  849,  847,  845,  843,  187,  840,
 /*   580 */   188,  270,  260,   78,   83,  269, 1019,  267,   45,  114,
 /*   590 */   317,  318,  319,  216,  321,  237,  320,  289,  322,  323,
 /*   600 */   335,  209,  817,  248,   97,   98,  210,  249,  816,  252,
 /*   610 */   815,  251,  798,  797,  256,  858,  261,  132,  173,  133,
 /*   620 */   898,  175,  171,  172,  174,  176,  177,  851,    4,  134,
 /*   630 */   850,    2,  135,  842,  841,  284,    9,   81,  696,  161,
 /*   640 */   159,   30,  147,  263,   84,  944,  224,  721,  724,   85,
 /*   650 */    10,  726,   86,  148,  273,   11,  730,  150,   31,  774,
 /*   660 */     7,   32,   12,   26,  286,   27,  776,   95,   93,  628,
 /*   670 */   624,  622,  621,  620,  617,  590,  297,   99,   64,   36,
 /*   680 */   102,  667,  666,  663,  612,  610,   65,  602,  104,  608,
 /*   690 */   604,  606,  600,  598,  631,  630,  629,  627,  626,  625,
 /*   700 */   623,  619,  618,  108,  110,   66,  588,  555,  553,  821,
 /*   710 */   820,  820,  820,  820,  820,  820,  820,  820,  820,  137,
 /*   720 */   138,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   193,    1,  192,  193,    0,  255,  212,  255,  255,    9,
 /*    10 */   190,  191,  255,   13,   14,  193,   16,   17,  265,  266,
 /*    20 */    20,   21,  255,   23,   24,   25,   26,   27,   28,  255,
 /*    30 */     5,  255,  238,   33,   34,  255,  212,   37,   38,   39,
 /*    40 */    13,   14,  266,   16,   17,  265,  266,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  236,    9,   33,   34,
 /*    60 */    33,   34,  238,  256,   37,   38,   39,   14,  193,   16,
 /*    70 */    17,  193,  252,   20,   21,  199,   23,   24,   25,   26,
 /*    80 */    27,   28,  260,   79,  262,  198,   33,   34,  199,  202,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  231,   79,   61,
 /*   110 */   110,    1,  237,   13,   14,  193,   16,   17,   81,    9,
 /*   120 */    20,   21,  233,   23,   24,   25,   26,   27,   28,   37,
 /*   130 */    38,   39,  105,   33,   34,  257,  258,   37,   38,   39,
 /*   140 */    13,   14,  236,   16,   17,  255,  236,   20,   21,    1,
 /*   150 */    23,   24,   25,   26,   27,   28,  234,    9,  252,  237,
 /*   160 */    33,   34,  252,  255,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  211,  193,  213,  214,  215,  216,  217,  218,
 /*   190 */   219,  220,  221,  222,  223,  224,  225,  226,   16,   17,
 /*   200 */   193,  255,   20,   21,  193,   23,   24,   25,   26,   27,
 /*   210 */    28,  193,   44,  199,  104,   33,   34,  193,  255,   37,
 /*   220 */    38,   39,    1,    2,  234,  255,    5,  237,    7,   61,
 /*   230 */     9,    1,    2,  193,  104,    5,   68,    7,  108,    9,
 /*   240 */   110,   73,   74,   75,  230,  231,  232,  233,   25,   26,
 /*   250 */    27,   28,  228,  235,   33,   34,   33,   34,   37,  238,
 /*   260 */    37,   38,   39,   33,   34,   65,   66,   67,   88,  262,
 /*   270 */    90,   91,  104,  262,  136,   95,   37,   97,   98,   99,
 /*   280 */   112,  101,  102,  145,  146,  211,  193,  255,  214,  215,
 /*   290 */    62,   63,   64,  219,  198,  221,  222,  223,  202,  225,
 /*   300 */   226,  104,  262,  135,  264,  137,   62,   63,   64,  112,
 /*   310 */   212,  212,  144,   69,   70,   71,   72,   68,    5,  255,
 /*   320 */     7,  193,   78,   68,    5,  104,    7,  234,    2,  238,
 /*   330 */   237,    5,  111,    7,  104,    9,  238,  238,  117,  255,
 /*   340 */    76,  111,   62,   63,   64,   81,   15,  117,  105,   69,
 /*   350 */    70,   71,   72,  198,  115,  134,  113,  202,  193,   33,
 /*   360 */    34,  255,  234,  193,  134,  237,   62,   63,   64,  193,
 /*   370 */   193,  193,  239,   69,   70,   71,   72,   33,   34,  193,
 /*   380 */   193,   37,   38,   39,  200,  201,  253,  138,  238,  140,
 /*   390 */   105,  142,  143,  138,  109,  140,  109,  142,  143,  234,
 /*   400 */   196,  197,  237,  116,  234,    1,   60,  237,  125,  126,
 /*   410 */   234,  234,  234,  237,  237,  237,  109,  105,  255,  109,
 /*   420 */   234,  234,  105,  237,  237,  113,  109,  105,  105,  105,
 /*   430 */   117,  109,  109,  109,  105,  229,  117,  111,  109,  132,
 /*   440 */   130,   37,  111,  105,  105,  255,  104,  109,  109,  107,
 /*   450 */   104,  255,  105,  105,  105,  193,  109,  109,  109,  104,
 /*   460 */   139,  106,  141,    5,  139,    7,  141,  139,    5,  141,
 /*   470 */     7,  139,  139,  141,  141,  139,  229,  141,   76,   77,
 /*   480 */   255,  255,  255,  229,  193,  229,  229,  229,  229,  193,
 /*   490 */   254,  193,  193,  193,  193,  193,  236,  236,  193,  193,
 /*   500 */   263,  263,  236,  240,  193,   60,  227,  193,  193,  193,
 /*   510 */   117,  193,  259,  193,  103,  242,  244,  122,  249,  251,
 /*   520 */   193,  250,  128,  193,  259,  129,  259,  193,  193,  259,
 /*   530 */   193,  131,  123,  193,  193,  127,  193,  248,  193,  247,
 /*   540 */   246,  121,  245,  193,  193,  193,  193,  193,  120,  193,
 /*   550 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   560 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   570 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   580 */   193,  119,  194,  194,  194,  194,  194,  118,  133,   87,
 /*   590 */    86,   50,   83,  194,   54,  194,   85,  194,   84,   82,
 /*   600 */    79,  194,    5,  147,  199,  199,  194,    5,    5,    5,
 /*   610 */     5,  147,   90,   89,  136,  194,  113,  195,  204,  195,
 /*   620 */   210,  205,  209,  208,  207,  206,  203,  194,  196,  195,
 /*   630 */   194,  200,  195,  194,  194,  107,  104,  114,  105,  241,
 /*   640 */   243,  104,  104,  109,  109,  227,    1,  105,  105,  104,
 /*   650 */   124,  105,  104,  109,  104,  124,  105,  104,  109,  105,
 /*   660 */   104,  109,  104,  104,  107,  104,  111,   76,  108,    9,
 /*   670 */     5,    5,    5,    5,    5,   80,   15,   76,   16,  109,
 /*   680 */   141,    5,    5,  105,    5,    5,   16,    5,  141,    5,
 /*   690 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   700 */     5,    5,    5,  141,  141,  109,   80,   60,   59,    0,
 /*   710 */   267,  267,  267,  267,  267,  267,  267,  267,  267,   21,
 /*   720 */    21,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   730 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   740 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   750 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   760 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   770 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   780 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   790 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   800 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   810 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   820 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   830 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   840 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   850 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   860 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   870 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   880 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   890 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
 /*   900 */   267,  267,  267,  267,  267,  267,  267,  267,  267,  267,
};
#define YY_SHIFT_COUNT    (340)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (709)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  180,  180,   29,  221,  230,  110,  148,
 /*    10 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*    20 */   148,  148,    0,   48,  230,  326,  326,  326,  326,  197,
 /*    30 */   197,  148,  148,  148,    4,  148,  148,  264,   29,   37,
 /*    40 */    37,  721,  721,  721,  230,  230,  230,  230,  230,  230,
 /*    50 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    60 */   230,  230,  230,  230,  326,  326,  326,   25,   25,   25,
 /*    70 */    25,   25,   25,   25,  148,  148,  148,  239,  148,  148,
 /*    80 */   148,  197,  197,  148,  148,  148,  148,  283,  283,  287,
 /*    90 */   197,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   100 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   110 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   120 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   130 */   148,  148,  148,  148,  148,  148,  148,  148,  148,  148,
 /*   140 */   148,  148,  148,  445,  445,  445,  393,  393,  393,  445,
 /*   150 */   393,  445,  396,  400,  394,  409,  408,  395,  420,  428,
 /*   160 */   462,  469,  455,  445,  445,  445,  411,   29,   29,  445,
 /*   170 */   445,  502,  504,  541,  509,  511,  540,  514,  517,  411,
 /*   180 */   445,  521,  521,  445,  521,  445,  521,  445,  445,  721,
 /*   190 */   721,   27,  100,  127,  100,  100,   53,  182,  223,  223,
 /*   200 */   223,  223,  244,  280,  304,  344,  344,  344,  344,  249,
 /*   210 */   255,  138,   92,   92,  313,  319,  130,  200,  228,  285,
 /*   220 */   243,  312,  317,  322,  323,  324,  329,  404,  346,  331,
 /*   230 */   307,  310,  338,  339,  347,  348,  349,  342,  321,  325,
 /*   240 */   328,  332,  333,  355,  336,  458,  463,  402,  597,  456,
 /*   250 */   602,  603,  464,  604,  605,  522,  524,  478,  503,  528,
 /*   260 */   532,  523,  533,  537,  534,  535,  542,  538,  543,  544,
 /*   270 */   545,  546,  548,  645,  550,  551,  553,  549,  526,  552,
 /*   280 */   531,  554,  556,  555,  558,  528,  559,  557,  561,  560,
 /*   290 */   591,  660,  665,  666,  667,  668,  669,  595,  661,  601,
 /*   300 */   662,  539,  547,  570,  570,  570,  570,  670,  562,  563,
 /*   310 */   570,  570,  570,  676,  677,  578,  570,  679,  680,  682,
 /*   320 */   684,  685,  686,  687,  688,  689,  690,  691,  692,  693,
 /*   330 */   694,  695,  696,  697,  596,  626,  698,  699,  647,  649,
 /*   340 */   709,
};
#define YY_REDUCE_COUNT (190)
#define YY_REDUCE_MIN   (-250)
#define YY_REDUCE_MAX   (440)
static const short yy_reduce_ofst[] = {
 /*     0 */  -180,  -29,  -29,   74,   74,   14, -247, -220, -122,  -78,
 /*    10 */    40, -178,  -10,   93,  128,  165,  170,  176,  177,  178,
 /*    20 */   186,  187, -193, -190, -224, -206, -176,   98,   99,  -94,
 /*    30 */   -90,    7,   11,   18, -111,   24, -125, -113, -124,   96,
 /*    40 */   155,  133,  184,  204, -250, -248, -243, -233, -226, -110,
 /*    50 */   -92,  -54,  -37,  -30,   32,   64,   84,  106,  163,  190,
 /*    60 */   196,  225,  226,  227,   21,   91,  150,  206,  247,  254,
 /*    70 */   256,  257,  258,  259,  262,  291,  296,  236,  298,  299,
 /*    80 */   300,  260,  261,  301,  302,  305,  306,  237,  238,  263,
 /*    90 */   266,  311,  314,  315,  316,  318,  320,  327,  330,  334,
 /*   100 */   335,  337,  340,  341,  343,  345,  350,  351,  352,  353,
 /*   110 */   354,  356,  357,  358,  359,  360,  361,  362,  363,  364,
 /*   120 */   365,  366,  367,  368,  369,  370,  371,  372,  373,  374,
 /*   130 */   375,  376,  377,  378,  379,  380,  381,  382,  383,  384,
 /*   140 */   385,  386,  387,  388,  389,  390,  253,  265,  267,  391,
 /*   150 */   270,  392,  268,  271,  269,  289,  292,  294,  297,  272,
 /*   160 */   397,  273,  398,  399,  401,  403,  279,  405,  406,  407,
 /*   170 */   412,  410,  413,  415,  414,  417,  416,  419,  423,  418,
 /*   180 */   421,  422,  424,  433,  434,  436,  437,  439,  440,  431,
 /*   190 */   432,
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
  /*  189 */ "error",
  /*  190 */ "program",
  /*  191 */ "cmd",
  /*  192 */ "dbPrefix",
  /*  193 */ "ids",
  /*  194 */ "cpxName",
  /*  195 */ "ifexists",
  /*  196 */ "alter_db_optr",
  /*  197 */ "alter_topic_optr",
  /*  198 */ "acct_optr",
  /*  199 */ "ifnotexists",
  /*  200 */ "db_optr",
  /*  201 */ "topic_optr",
  /*  202 */ "pps",
  /*  203 */ "tseries",
  /*  204 */ "dbs",
  /*  205 */ "streams",
  /*  206 */ "storage",
  /*  207 */ "qtime",
  /*  208 */ "users",
  /*  209 */ "conns",
  /*  210 */ "state",
  /*  211 */ "keep",
  /*  212 */ "tagitemlist",
  /*  213 */ "cache",
  /*  214 */ "replica",
  /*  215 */ "quorum",
  /*  216 */ "days",
  /*  217 */ "minrows",
  /*  218 */ "maxrows",
  /*  219 */ "blocks",
  /*  220 */ "ctime",
  /*  221 */ "wal",
  /*  222 */ "fsync",
  /*  223 */ "comp",
  /*  224 */ "prec",
  /*  225 */ "update",
  /*  226 */ "cachelast",
  /*  227 */ "partitions",
  /*  228 */ "typename",
  /*  229 */ "signed",
  /*  230 */ "create_table_args",
  /*  231 */ "create_stable_args",
  /*  232 */ "create_table_list",
  /*  233 */ "create_from_stable",
  /*  234 */ "columnlist",
  /*  235 */ "tagNamelist",
  /*  236 */ "select",
  /*  237 */ "column",
  /*  238 */ "tagitem",
  /*  239 */ "selcollist",
  /*  240 */ "from",
  /*  241 */ "where_opt",
  /*  242 */ "interval_opt",
  /*  243 */ "session_option",
  /*  244 */ "windowstate_option",
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
  /*  258 */ "sub",
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
    case 211: /* keep */
    case 212: /* tagitemlist */
    case 234: /* columnlist */
    case 235: /* tagNamelist */
    case 245: /* fill_opt */
    case 247: /* groupby_opt */
    case 248: /* orderby_opt */
    case 260: /* sortlist */
    case 264: /* grouplist */
{
taosArrayDestroy((yypminor->yy15));
}
      break;
    case 232: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy482));
}
      break;
    case 236: /* select */
{
destroySqlNode((yypminor->yy134));
}
      break;
    case 239: /* selcollist */
    case 253: /* sclp */
    case 265: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy15));
}
      break;
    case 240: /* from */
    case 257: /* tablelist */
    case 258: /* sub */
{
destroyRelationInfo((yypminor->yy160));
}
      break;
    case 241: /* where_opt */
    case 249: /* having_opt */
    case 255: /* expr */
    case 266: /* expritem */
{
tSqlExprDestroy((yypminor->yy328));
}
      break;
    case 252: /* union */
{
destroyAllSqlNode((yypminor->yy15));
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
  {  190,   -1 }, /* (0) program ::= cmd */
  {  191,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  191,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  191,   -2 }, /* (3) cmd ::= SHOW MNODES */
  {  191,   -2 }, /* (4) cmd ::= SHOW DNODES */
  {  191,   -2 }, /* (5) cmd ::= SHOW ACCOUNTS */
  {  191,   -2 }, /* (6) cmd ::= SHOW USERS */
  {  191,   -2 }, /* (7) cmd ::= SHOW MODULES */
  {  191,   -2 }, /* (8) cmd ::= SHOW QUERIES */
  {  191,   -2 }, /* (9) cmd ::= SHOW CONNECTIONS */
  {  191,   -2 }, /* (10) cmd ::= SHOW STREAMS */
  {  191,   -2 }, /* (11) cmd ::= SHOW VARIABLES */
  {  191,   -2 }, /* (12) cmd ::= SHOW SCORES */
  {  191,   -2 }, /* (13) cmd ::= SHOW GRANTS */
  {  191,   -2 }, /* (14) cmd ::= SHOW VNODES */
  {  191,   -3 }, /* (15) cmd ::= SHOW VNODES IPTOKEN */
  {  192,    0 }, /* (16) dbPrefix ::= */
  {  192,   -2 }, /* (17) dbPrefix ::= ids DOT */
  {  194,    0 }, /* (18) cpxName ::= */
  {  194,   -2 }, /* (19) cpxName ::= DOT ids */
  {  191,   -5 }, /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  191,   -5 }, /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  191,   -4 }, /* (22) cmd ::= SHOW CREATE DATABASE ids */
  {  191,   -3 }, /* (23) cmd ::= SHOW dbPrefix TABLES */
  {  191,   -5 }, /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  191,   -3 }, /* (25) cmd ::= SHOW dbPrefix STABLES */
  {  191,   -5 }, /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  191,   -3 }, /* (27) cmd ::= SHOW dbPrefix VGROUPS */
  {  191,   -4 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  191,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  191,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  191,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  191,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  191,   -3 }, /* (33) cmd ::= DROP DNODE ids */
  {  191,   -3 }, /* (34) cmd ::= DROP USER ids */
  {  191,   -3 }, /* (35) cmd ::= DROP ACCOUNT ids */
  {  191,   -2 }, /* (36) cmd ::= USE ids */
  {  191,   -3 }, /* (37) cmd ::= DESCRIBE ids cpxName */
  {  191,   -5 }, /* (38) cmd ::= ALTER USER ids PASS ids */
  {  191,   -5 }, /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  191,   -4 }, /* (40) cmd ::= ALTER DNODE ids ids */
  {  191,   -5 }, /* (41) cmd ::= ALTER DNODE ids ids ids */
  {  191,   -3 }, /* (42) cmd ::= ALTER LOCAL ids */
  {  191,   -4 }, /* (43) cmd ::= ALTER LOCAL ids ids */
  {  191,   -4 }, /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  191,   -4 }, /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  191,   -4 }, /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  191,   -6 }, /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  193,   -1 }, /* (48) ids ::= ID */
  {  193,   -1 }, /* (49) ids ::= STRING */
  {  195,   -2 }, /* (50) ifexists ::= IF EXISTS */
  {  195,    0 }, /* (51) ifexists ::= */
  {  199,   -3 }, /* (52) ifnotexists ::= IF NOT EXISTS */
  {  199,    0 }, /* (53) ifnotexists ::= */
  {  191,   -3 }, /* (54) cmd ::= CREATE DNODE ids */
  {  191,   -6 }, /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  191,   -5 }, /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  191,   -5 }, /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  191,   -5 }, /* (58) cmd ::= CREATE USER ids PASS ids */
  {  202,    0 }, /* (59) pps ::= */
  {  202,   -2 }, /* (60) pps ::= PPS INTEGER */
  {  203,    0 }, /* (61) tseries ::= */
  {  203,   -2 }, /* (62) tseries ::= TSERIES INTEGER */
  {  204,    0 }, /* (63) dbs ::= */
  {  204,   -2 }, /* (64) dbs ::= DBS INTEGER */
  {  205,    0 }, /* (65) streams ::= */
  {  205,   -2 }, /* (66) streams ::= STREAMS INTEGER */
  {  206,    0 }, /* (67) storage ::= */
  {  206,   -2 }, /* (68) storage ::= STORAGE INTEGER */
  {  207,    0 }, /* (69) qtime ::= */
  {  207,   -2 }, /* (70) qtime ::= QTIME INTEGER */
  {  208,    0 }, /* (71) users ::= */
  {  208,   -2 }, /* (72) users ::= USERS INTEGER */
  {  209,    0 }, /* (73) conns ::= */
  {  209,   -2 }, /* (74) conns ::= CONNS INTEGER */
  {  210,    0 }, /* (75) state ::= */
  {  210,   -2 }, /* (76) state ::= STATE ids */
  {  198,   -9 }, /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  211,   -2 }, /* (78) keep ::= KEEP tagitemlist */
  {  213,   -2 }, /* (79) cache ::= CACHE INTEGER */
  {  214,   -2 }, /* (80) replica ::= REPLICA INTEGER */
  {  215,   -2 }, /* (81) quorum ::= QUORUM INTEGER */
  {  216,   -2 }, /* (82) days ::= DAYS INTEGER */
  {  217,   -2 }, /* (83) minrows ::= MINROWS INTEGER */
  {  218,   -2 }, /* (84) maxrows ::= MAXROWS INTEGER */
  {  219,   -2 }, /* (85) blocks ::= BLOCKS INTEGER */
  {  220,   -2 }, /* (86) ctime ::= CTIME INTEGER */
  {  221,   -2 }, /* (87) wal ::= WAL INTEGER */
  {  222,   -2 }, /* (88) fsync ::= FSYNC INTEGER */
  {  223,   -2 }, /* (89) comp ::= COMP INTEGER */
  {  224,   -2 }, /* (90) prec ::= PRECISION STRING */
  {  225,   -2 }, /* (91) update ::= UPDATE INTEGER */
  {  226,   -2 }, /* (92) cachelast ::= CACHELAST INTEGER */
  {  227,   -2 }, /* (93) partitions ::= PARTITIONS INTEGER */
  {  200,    0 }, /* (94) db_optr ::= */
  {  200,   -2 }, /* (95) db_optr ::= db_optr cache */
  {  200,   -2 }, /* (96) db_optr ::= db_optr replica */
  {  200,   -2 }, /* (97) db_optr ::= db_optr quorum */
  {  200,   -2 }, /* (98) db_optr ::= db_optr days */
  {  200,   -2 }, /* (99) db_optr ::= db_optr minrows */
  {  200,   -2 }, /* (100) db_optr ::= db_optr maxrows */
  {  200,   -2 }, /* (101) db_optr ::= db_optr blocks */
  {  200,   -2 }, /* (102) db_optr ::= db_optr ctime */
  {  200,   -2 }, /* (103) db_optr ::= db_optr wal */
  {  200,   -2 }, /* (104) db_optr ::= db_optr fsync */
  {  200,   -2 }, /* (105) db_optr ::= db_optr comp */
  {  200,   -2 }, /* (106) db_optr ::= db_optr prec */
  {  200,   -2 }, /* (107) db_optr ::= db_optr keep */
  {  200,   -2 }, /* (108) db_optr ::= db_optr update */
  {  200,   -2 }, /* (109) db_optr ::= db_optr cachelast */
  {  201,   -1 }, /* (110) topic_optr ::= db_optr */
  {  201,   -2 }, /* (111) topic_optr ::= topic_optr partitions */
  {  196,    0 }, /* (112) alter_db_optr ::= */
  {  196,   -2 }, /* (113) alter_db_optr ::= alter_db_optr replica */
  {  196,   -2 }, /* (114) alter_db_optr ::= alter_db_optr quorum */
  {  196,   -2 }, /* (115) alter_db_optr ::= alter_db_optr keep */
  {  196,   -2 }, /* (116) alter_db_optr ::= alter_db_optr blocks */
  {  196,   -2 }, /* (117) alter_db_optr ::= alter_db_optr comp */
  {  196,   -2 }, /* (118) alter_db_optr ::= alter_db_optr wal */
  {  196,   -2 }, /* (119) alter_db_optr ::= alter_db_optr fsync */
  {  196,   -2 }, /* (120) alter_db_optr ::= alter_db_optr update */
  {  196,   -2 }, /* (121) alter_db_optr ::= alter_db_optr cachelast */
  {  197,   -1 }, /* (122) alter_topic_optr ::= alter_db_optr */
  {  197,   -2 }, /* (123) alter_topic_optr ::= alter_topic_optr partitions */
  {  228,   -1 }, /* (124) typename ::= ids */
  {  228,   -4 }, /* (125) typename ::= ids LP signed RP */
  {  228,   -2 }, /* (126) typename ::= ids UNSIGNED */
  {  229,   -1 }, /* (127) signed ::= INTEGER */
  {  229,   -2 }, /* (128) signed ::= PLUS INTEGER */
  {  229,   -2 }, /* (129) signed ::= MINUS INTEGER */
  {  191,   -3 }, /* (130) cmd ::= CREATE TABLE create_table_args */
  {  191,   -3 }, /* (131) cmd ::= CREATE TABLE create_stable_args */
  {  191,   -3 }, /* (132) cmd ::= CREATE STABLE create_stable_args */
  {  191,   -3 }, /* (133) cmd ::= CREATE TABLE create_table_list */
  {  232,   -1 }, /* (134) create_table_list ::= create_from_stable */
  {  232,   -2 }, /* (135) create_table_list ::= create_table_list create_from_stable */
  {  230,   -6 }, /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  231,  -10 }, /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  233,  -10 }, /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  233,  -13 }, /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  235,   -3 }, /* (140) tagNamelist ::= tagNamelist COMMA ids */
  {  235,   -1 }, /* (141) tagNamelist ::= ids */
  {  230,   -5 }, /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
  {  234,   -3 }, /* (143) columnlist ::= columnlist COMMA column */
  {  234,   -1 }, /* (144) columnlist ::= column */
  {  237,   -2 }, /* (145) column ::= ids typename */
  {  212,   -3 }, /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
  {  212,   -1 }, /* (147) tagitemlist ::= tagitem */
  {  238,   -1 }, /* (148) tagitem ::= INTEGER */
  {  238,   -1 }, /* (149) tagitem ::= FLOAT */
  {  238,   -1 }, /* (150) tagitem ::= STRING */
  {  238,   -1 }, /* (151) tagitem ::= BOOL */
  {  238,   -1 }, /* (152) tagitem ::= NULL */
  {  238,   -2 }, /* (153) tagitem ::= MINUS INTEGER */
  {  238,   -2 }, /* (154) tagitem ::= MINUS FLOAT */
  {  238,   -2 }, /* (155) tagitem ::= PLUS INTEGER */
  {  238,   -2 }, /* (156) tagitem ::= PLUS FLOAT */
  {  236,  -14 }, /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  236,   -3 }, /* (158) select ::= LP select RP */
  {  252,   -1 }, /* (159) union ::= select */
  {  252,   -4 }, /* (160) union ::= union UNION ALL select */
  {  191,   -1 }, /* (161) cmd ::= union */
  {  236,   -2 }, /* (162) select ::= SELECT selcollist */
  {  253,   -2 }, /* (163) sclp ::= selcollist COMMA */
  {  253,    0 }, /* (164) sclp ::= */
  {  239,   -4 }, /* (165) selcollist ::= sclp distinct expr as */
  {  239,   -2 }, /* (166) selcollist ::= sclp STAR */
  {  256,   -2 }, /* (167) as ::= AS ids */
  {  256,   -1 }, /* (168) as ::= ids */
  {  256,    0 }, /* (169) as ::= */
  {  254,   -1 }, /* (170) distinct ::= DISTINCT */
  {  254,    0 }, /* (171) distinct ::= */
  {  240,   -2 }, /* (172) from ::= FROM tablelist */
  {  240,   -2 }, /* (173) from ::= FROM sub */
  {  258,   -3 }, /* (174) sub ::= LP union RP */
  {  258,   -4 }, /* (175) sub ::= LP union RP ids */
  {  258,   -6 }, /* (176) sub ::= sub COMMA LP union RP ids */
  {  257,   -2 }, /* (177) tablelist ::= ids cpxName */
  {  257,   -3 }, /* (178) tablelist ::= ids cpxName ids */
  {  257,   -4 }, /* (179) tablelist ::= tablelist COMMA ids cpxName */
  {  257,   -5 }, /* (180) tablelist ::= tablelist COMMA ids cpxName ids */
  {  259,   -1 }, /* (181) tmvar ::= VARIABLE */
  {  242,   -4 }, /* (182) interval_opt ::= INTERVAL LP tmvar RP */
  {  242,   -6 }, /* (183) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  242,    0 }, /* (184) interval_opt ::= */
  {  243,    0 }, /* (185) session_option ::= */
  {  243,   -7 }, /* (186) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  244,    0 }, /* (187) windowstate_option ::= */
  {  244,   -4 }, /* (188) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  245,    0 }, /* (189) fill_opt ::= */
  {  245,   -6 }, /* (190) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  245,   -4 }, /* (191) fill_opt ::= FILL LP ID RP */
  {  246,   -4 }, /* (192) sliding_opt ::= SLIDING LP tmvar RP */
  {  246,    0 }, /* (193) sliding_opt ::= */
  {  248,    0 }, /* (194) orderby_opt ::= */
  {  248,   -3 }, /* (195) orderby_opt ::= ORDER BY sortlist */
  {  260,   -4 }, /* (196) sortlist ::= sortlist COMMA item sortorder */
  {  260,   -2 }, /* (197) sortlist ::= item sortorder */
  {  262,   -2 }, /* (198) item ::= ids cpxName */
  {  263,   -1 }, /* (199) sortorder ::= ASC */
  {  263,   -1 }, /* (200) sortorder ::= DESC */
  {  263,    0 }, /* (201) sortorder ::= */
  {  247,    0 }, /* (202) groupby_opt ::= */
  {  247,   -3 }, /* (203) groupby_opt ::= GROUP BY grouplist */
  {  264,   -3 }, /* (204) grouplist ::= grouplist COMMA item */
  {  264,   -1 }, /* (205) grouplist ::= item */
  {  249,    0 }, /* (206) having_opt ::= */
  {  249,   -2 }, /* (207) having_opt ::= HAVING expr */
  {  251,    0 }, /* (208) limit_opt ::= */
  {  251,   -2 }, /* (209) limit_opt ::= LIMIT signed */
  {  251,   -4 }, /* (210) limit_opt ::= LIMIT signed OFFSET signed */
  {  251,   -4 }, /* (211) limit_opt ::= LIMIT signed COMMA signed */
  {  250,    0 }, /* (212) slimit_opt ::= */
  {  250,   -2 }, /* (213) slimit_opt ::= SLIMIT signed */
  {  250,   -4 }, /* (214) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  250,   -4 }, /* (215) slimit_opt ::= SLIMIT signed COMMA signed */
  {  241,    0 }, /* (216) where_opt ::= */
  {  241,   -2 }, /* (217) where_opt ::= WHERE expr */
  {  255,   -3 }, /* (218) expr ::= LP expr RP */
  {  255,   -1 }, /* (219) expr ::= ID */
  {  255,   -3 }, /* (220) expr ::= ID DOT ID */
  {  255,   -3 }, /* (221) expr ::= ID DOT STAR */
  {  255,   -1 }, /* (222) expr ::= INTEGER */
  {  255,   -2 }, /* (223) expr ::= MINUS INTEGER */
  {  255,   -2 }, /* (224) expr ::= PLUS INTEGER */
  {  255,   -1 }, /* (225) expr ::= FLOAT */
  {  255,   -2 }, /* (226) expr ::= MINUS FLOAT */
  {  255,   -2 }, /* (227) expr ::= PLUS FLOAT */
  {  255,   -1 }, /* (228) expr ::= STRING */
  {  255,   -1 }, /* (229) expr ::= NOW */
  {  255,   -1 }, /* (230) expr ::= VARIABLE */
  {  255,   -2 }, /* (231) expr ::= PLUS VARIABLE */
  {  255,   -2 }, /* (232) expr ::= MINUS VARIABLE */
  {  255,   -1 }, /* (233) expr ::= BOOL */
  {  255,   -1 }, /* (234) expr ::= NULL */
  {  255,   -4 }, /* (235) expr ::= ID LP exprlist RP */
  {  255,   -4 }, /* (236) expr ::= ID LP STAR RP */
  {  255,   -3 }, /* (237) expr ::= expr IS NULL */
  {  255,   -4 }, /* (238) expr ::= expr IS NOT NULL */
  {  255,   -3 }, /* (239) expr ::= expr LT expr */
  {  255,   -3 }, /* (240) expr ::= expr GT expr */
  {  255,   -3 }, /* (241) expr ::= expr LE expr */
  {  255,   -3 }, /* (242) expr ::= expr GE expr */
  {  255,   -3 }, /* (243) expr ::= expr NE expr */
  {  255,   -3 }, /* (244) expr ::= expr EQ expr */
  {  255,   -5 }, /* (245) expr ::= expr BETWEEN expr AND expr */
  {  255,   -3 }, /* (246) expr ::= expr AND expr */
  {  255,   -3 }, /* (247) expr ::= expr OR expr */
  {  255,   -3 }, /* (248) expr ::= expr PLUS expr */
  {  255,   -3 }, /* (249) expr ::= expr MINUS expr */
  {  255,   -3 }, /* (250) expr ::= expr STAR expr */
  {  255,   -3 }, /* (251) expr ::= expr SLASH expr */
  {  255,   -3 }, /* (252) expr ::= expr REM expr */
  {  255,   -3 }, /* (253) expr ::= expr LIKE expr */
  {  255,   -5 }, /* (254) expr ::= expr IN LP exprlist RP */
  {  265,   -3 }, /* (255) exprlist ::= exprlist COMMA expritem */
  {  265,   -1 }, /* (256) exprlist ::= expritem */
  {  266,   -1 }, /* (257) expritem ::= expr */
  {  266,    0 }, /* (258) expritem ::= */
  {  191,   -3 }, /* (259) cmd ::= RESET QUERY CACHE */
  {  191,   -3 }, /* (260) cmd ::= SYNCDB ids REPLICA */
  {  191,   -7 }, /* (261) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  191,   -7 }, /* (262) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  191,   -7 }, /* (263) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  191,   -7 }, /* (264) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  191,   -7 }, /* (265) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  191,   -8 }, /* (266) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  191,   -9 }, /* (267) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  191,   -7 }, /* (268) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  191,   -7 }, /* (269) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  191,   -7 }, /* (270) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  191,   -7 }, /* (271) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  191,   -7 }, /* (272) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  191,   -7 }, /* (273) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  191,   -8 }, /* (274) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  191,   -9 }, /* (275) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  191,   -7 }, /* (276) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  191,   -3 }, /* (277) cmd ::= KILL CONNECTION INTEGER */
  {  191,   -5 }, /* (278) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  191,   -5 }, /* (279) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy454, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy397);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy397);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy397);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy454, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy397.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy397.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy397.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy397.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy397.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy397.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy397.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy397.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy397.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy397 = yylhsminor.yy397;
        break;
      case 78: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy15 = yymsp[0].minor.yy15; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy454); yymsp[1].minor.yy454.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 95: /* db_optr ::= db_optr cache */
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 96: /* db_optr ::= db_optr replica */
      case 113: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==113);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 97: /* db_optr ::= db_optr quorum */
      case 114: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==114);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 98: /* db_optr ::= db_optr days */
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 99: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 100: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 101: /* db_optr ::= db_optr blocks */
      case 116: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==116);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 102: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 103: /* db_optr ::= db_optr wal */
      case 118: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==118);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 104: /* db_optr ::= db_optr fsync */
      case 119: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==119);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 105: /* db_optr ::= db_optr comp */
      case 117: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==117);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 106: /* db_optr ::= db_optr prec */
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 107: /* db_optr ::= db_optr keep */
      case 115: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==115);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.keep = yymsp[0].minor.yy15; }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 108: /* db_optr ::= db_optr update */
      case 120: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==120);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 109: /* db_optr ::= db_optr cachelast */
      case 121: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==121);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 110: /* topic_optr ::= db_optr */
      case 122: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==122);
{ yylhsminor.yy454 = yymsp[0].minor.yy454; yylhsminor.yy454.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy454 = yylhsminor.yy454;
        break;
      case 111: /* topic_optr ::= topic_optr partitions */
      case 123: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==123);
{ yylhsminor.yy454 = yymsp[-1].minor.yy454; yylhsminor.yy454.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy454 = yylhsminor.yy454;
        break;
      case 112: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy454); yymsp[1].minor.yy454.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 124: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy505, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy505 = yylhsminor.yy505;
        break;
      case 125: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy489 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy505, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy489;  // negative value of name length
    tSetColumnType(&yylhsminor.yy505, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy505 = yylhsminor.yy505;
        break;
      case 126: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy505, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy505 = yylhsminor.yy505;
        break;
      case 127: /* signed ::= INTEGER */
{ yylhsminor.yy489 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy489 = yylhsminor.yy489;
        break;
      case 128: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy489 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 129: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy489 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 133: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy482;}
        break;
      case 134: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy390);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy482 = pCreateTable;
}
  yymsp[0].minor.yy482 = yylhsminor.yy482;
        break;
      case 135: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy482->childTableInfo, &yymsp[0].minor.yy390);
  yylhsminor.yy482 = yymsp[-1].minor.yy482;
}
  yymsp[-1].minor.yy482 = yylhsminor.yy482;
        break;
      case 136: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy482 = tSetCreateTableInfo(yymsp[-1].minor.yy15, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy482, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy482 = yylhsminor.yy482;
        break;
      case 137: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy482 = tSetCreateTableInfo(yymsp[-5].minor.yy15, yymsp[-1].minor.yy15, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy482, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy482 = yylhsminor.yy482;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy390 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy15, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy390 = yylhsminor.yy390;
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy390 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy15, yymsp[-1].minor.yy15, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy390 = yylhsminor.yy390;
        break;
      case 140: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy15, &yymsp[0].minor.yy0); yylhsminor.yy15 = yymsp[-2].minor.yy15;  }
  yymsp[-2].minor.yy15 = yylhsminor.yy15;
        break;
      case 141: /* tagNamelist ::= ids */
{yylhsminor.yy15 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy15, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy15 = yylhsminor.yy15;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy482 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy134, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy482, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy482 = yylhsminor.yy482;
        break;
      case 143: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy15, &yymsp[0].minor.yy505); yylhsminor.yy15 = yymsp[-2].minor.yy15;  }
  yymsp[-2].minor.yy15 = yylhsminor.yy15;
        break;
      case 144: /* columnlist ::= column */
{yylhsminor.yy15 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy15, &yymsp[0].minor.yy505);}
  yymsp[0].minor.yy15 = yylhsminor.yy15;
        break;
      case 145: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy505, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy505);
}
  yymsp[-1].minor.yy505 = yylhsminor.yy505;
        break;
      case 146: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy15 = tVariantListAppend(yymsp[-2].minor.yy15, &yymsp[0].minor.yy380, -1);    }
  yymsp[-2].minor.yy15 = yylhsminor.yy15;
        break;
      case 147: /* tagitemlist ::= tagitem */
{ yylhsminor.yy15 = tVariantListAppend(NULL, &yymsp[0].minor.yy380, -1); }
  yymsp[0].minor.yy15 = yylhsminor.yy15;
        break;
      case 148: /* tagitem ::= INTEGER */
      case 149: /* tagitem ::= FLOAT */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= STRING */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= BOOL */ yytestcase(yyruleno==151);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy380, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy380 = yylhsminor.yy380;
        break;
      case 152: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy380, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy380 = yylhsminor.yy380;
        break;
      case 153: /* tagitem ::= MINUS INTEGER */
      case 154: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy380, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy380 = yylhsminor.yy380;
        break;
      case 157: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy134 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy15, yymsp[-11].minor.yy160, yymsp[-10].minor.yy328, yymsp[-4].minor.yy15, yymsp[-3].minor.yy15, &yymsp[-9].minor.yy496, &yymsp[-8].minor.yy151, &yymsp[-7].minor.yy96, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy15, &yymsp[0].minor.yy150, &yymsp[-1].minor.yy150, yymsp[-2].minor.yy328);
}
  yymsp[-13].minor.yy134 = yylhsminor.yy134;
        break;
      case 158: /* select ::= LP select RP */
{yymsp[-2].minor.yy134 = yymsp[-1].minor.yy134;}
        break;
      case 159: /* union ::= select */
{ yylhsminor.yy15 = setSubclause(NULL, yymsp[0].minor.yy134); }
  yymsp[0].minor.yy15 = yylhsminor.yy15;
        break;
      case 160: /* union ::= union UNION ALL select */
{ yylhsminor.yy15 = appendSelectClause(yymsp[-3].minor.yy15, yymsp[0].minor.yy134); }
  yymsp[-3].minor.yy15 = yylhsminor.yy15;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy15, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy134 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy15, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy134 = yylhsminor.yy134;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy15 = yymsp[-1].minor.yy15;}
  yymsp[-1].minor.yy15 = yylhsminor.yy15;
        break;
      case 164: /* sclp ::= */
      case 194: /* orderby_opt ::= */ yytestcase(yyruleno==194);
{yymsp[1].minor.yy15 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy15 = tSqlExprListAppend(yymsp[-3].minor.yy15, yymsp[-1].minor.yy328,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy15 = yylhsminor.yy15;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy15 = tSqlExprListAppend(yymsp[-1].minor.yy15, pNode, 0, 0);
}
  yymsp[-1].minor.yy15 = yylhsminor.yy15;
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
{yymsp[-1].minor.yy160 = yymsp[0].minor.yy160;}
        break;
      case 174: /* sub ::= LP union RP */
{yymsp[-2].minor.yy160 = addSubqueryElem(NULL, yymsp[-1].minor.yy15, NULL);}
        break;
      case 175: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy160 = addSubqueryElem(NULL, yymsp[-2].minor.yy15, &yymsp[0].minor.yy0);}
        break;
      case 176: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy160 = addSubqueryElem(yymsp[-5].minor.yy160, yymsp[-2].minor.yy15, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy160 = yylhsminor.yy160;
        break;
      case 177: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy160 = yylhsminor.yy160;
        break;
      case 178: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy160 = yylhsminor.yy160;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(yymsp[-3].minor.yy160, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy160 = yylhsminor.yy160;
        break;
      case 180: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy160 = setTableNameList(yymsp[-4].minor.yy160, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy160 = yylhsminor.yy160;
        break;
      case 181: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy496.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy496.offset.n = 0;}
        break;
      case 183: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy496.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy496.offset = yymsp[-1].minor.yy0;}
        break;
      case 184: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy496, 0, sizeof(yymsp[1].minor.yy496));}
        break;
      case 185: /* session_option ::= */
{yymsp[1].minor.yy151.col.n = 0; yymsp[1].minor.yy151.gap.n = 0;}
        break;
      case 186: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy151.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy151.gap = yymsp[-1].minor.yy0;
}
        break;
      case 187: /* windowstate_option ::= */
{ yymsp[1].minor.yy96.col.n = 0; yymsp[1].minor.yy96.col.z = NULL;}
        break;
      case 188: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy96.col = yymsp[-1].minor.yy0; }
        break;
      case 189: /* fill_opt ::= */
{ yymsp[1].minor.yy15 = 0;     }
        break;
      case 190: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy15, &A, -1, 0);
    yymsp[-5].minor.yy15 = yymsp[-1].minor.yy15;
}
        break;
      case 191: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy15 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 192: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 193: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 195: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy15 = yymsp[0].minor.yy15;}
        break;
      case 196: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy15 = tVariantListAppend(yymsp[-3].minor.yy15, &yymsp[-1].minor.yy380, yymsp[0].minor.yy250);
}
  yymsp[-3].minor.yy15 = yylhsminor.yy15;
        break;
      case 197: /* sortlist ::= item sortorder */
{
  yylhsminor.yy15 = tVariantListAppend(NULL, &yymsp[-1].minor.yy380, yymsp[0].minor.yy250);
}
  yymsp[-1].minor.yy15 = yylhsminor.yy15;
        break;
      case 198: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy380, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy380 = yylhsminor.yy380;
        break;
      case 199: /* sortorder ::= ASC */
{ yymsp[0].minor.yy250 = TSDB_ORDER_ASC; }
        break;
      case 200: /* sortorder ::= DESC */
{ yymsp[0].minor.yy250 = TSDB_ORDER_DESC;}
        break;
      case 201: /* sortorder ::= */
{ yymsp[1].minor.yy250 = TSDB_ORDER_ASC; }
        break;
      case 202: /* groupby_opt ::= */
{ yymsp[1].minor.yy15 = 0;}
        break;
      case 203: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy15 = yymsp[0].minor.yy15;}
        break;
      case 204: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy15 = tVariantListAppend(yymsp[-2].minor.yy15, &yymsp[0].minor.yy380, -1);
}
  yymsp[-2].minor.yy15 = yylhsminor.yy15;
        break;
      case 205: /* grouplist ::= item */
{
  yylhsminor.yy15 = tVariantListAppend(NULL, &yymsp[0].minor.yy380, -1);
}
  yymsp[0].minor.yy15 = yylhsminor.yy15;
        break;
      case 206: /* having_opt ::= */
      case 216: /* where_opt ::= */ yytestcase(yyruleno==216);
      case 258: /* expritem ::= */ yytestcase(yyruleno==258);
{yymsp[1].minor.yy328 = 0;}
        break;
      case 207: /* having_opt ::= HAVING expr */
      case 217: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==217);
{yymsp[-1].minor.yy328 = yymsp[0].minor.yy328;}
        break;
      case 208: /* limit_opt ::= */
      case 212: /* slimit_opt ::= */ yytestcase(yyruleno==212);
{yymsp[1].minor.yy150.limit = -1; yymsp[1].minor.yy150.offset = 0;}
        break;
      case 209: /* limit_opt ::= LIMIT signed */
      case 213: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==213);
{yymsp[-1].minor.yy150.limit = yymsp[0].minor.yy489;  yymsp[-1].minor.yy150.offset = 0;}
        break;
      case 210: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy150.limit = yymsp[-2].minor.yy489;  yymsp[-3].minor.yy150.offset = yymsp[0].minor.yy489;}
        break;
      case 211: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy150.limit = yymsp[0].minor.yy489;  yymsp[-3].minor.yy150.offset = yymsp[-2].minor.yy489;}
        break;
      case 214: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy150.limit = yymsp[-2].minor.yy489;  yymsp[-3].minor.yy150.offset = yymsp[0].minor.yy489;}
        break;
      case 215: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy150.limit = yymsp[0].minor.yy489;  yymsp[-3].minor.yy150.offset = yymsp[-2].minor.yy489;}
        break;
      case 218: /* expr ::= LP expr RP */
{yylhsminor.yy328 = yymsp[-1].minor.yy328; yylhsminor.yy328->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy328->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 219: /* expr ::= ID */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 220: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 221: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 222: /* expr ::= INTEGER */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 223: /* expr ::= MINUS INTEGER */
      case 224: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==224);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy328 = yylhsminor.yy328;
        break;
      case 225: /* expr ::= FLOAT */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 226: /* expr ::= MINUS FLOAT */
      case 227: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==227);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy328 = yylhsminor.yy328;
        break;
      case 228: /* expr ::= STRING */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 229: /* expr ::= NOW */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 230: /* expr ::= VARIABLE */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 231: /* expr ::= PLUS VARIABLE */
      case 232: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==232);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy328 = yylhsminor.yy328;
        break;
      case 233: /* expr ::= BOOL */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 234: /* expr ::= NULL */
{ yylhsminor.yy328 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
        break;
      case 235: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy328 = tSqlExprCreateFunction(yymsp[-1].minor.yy15, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy328 = yylhsminor.yy328;
        break;
      case 236: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy328 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy328 = yylhsminor.yy328;
        break;
      case 237: /* expr ::= expr IS NULL */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 238: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-3].minor.yy328, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy328 = yylhsminor.yy328;
        break;
      case 239: /* expr ::= expr LT expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_LT);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 240: /* expr ::= expr GT expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_GT);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 241: /* expr ::= expr LE expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_LE);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 242: /* expr ::= expr GE expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_GE);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 243: /* expr ::= expr NE expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_NE);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 244: /* expr ::= expr EQ expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_EQ);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 245: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy328); yylhsminor.yy328 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy328, yymsp[-2].minor.yy328, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy328, TK_LE), TK_AND);}
  yymsp[-4].minor.yy328 = yylhsminor.yy328;
        break;
      case 246: /* expr ::= expr AND expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_AND);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 247: /* expr ::= expr OR expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_OR); }
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 248: /* expr ::= expr PLUS expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_PLUS);  }
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 249: /* expr ::= expr MINUS expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_MINUS); }
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 250: /* expr ::= expr STAR expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_STAR);  }
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 251: /* expr ::= expr SLASH expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_DIVIDE);}
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 252: /* expr ::= expr REM expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_REM);   }
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 253: /* expr ::= expr LIKE expr */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-2].minor.yy328, yymsp[0].minor.yy328, TK_LIKE);  }
  yymsp[-2].minor.yy328 = yylhsminor.yy328;
        break;
      case 254: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy328 = tSqlExprCreate(yymsp[-4].minor.yy328, (tSqlExpr*)yymsp[-1].minor.yy15, TK_IN); }
  yymsp[-4].minor.yy328 = yylhsminor.yy328;
        break;
      case 255: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy15 = tSqlExprListAppend(yymsp[-2].minor.yy15,yymsp[0].minor.yy328,0, 0);}
  yymsp[-2].minor.yy15 = yylhsminor.yy15;
        break;
      case 256: /* exprlist ::= expritem */
{yylhsminor.yy15 = tSqlExprListAppend(0,yymsp[0].minor.yy328,0, 0);}
  yymsp[0].minor.yy15 = yylhsminor.yy15;
        break;
      case 257: /* expritem ::= expr */
{yylhsminor.yy328 = yymsp[0].minor.yy328;}
  yymsp[0].minor.yy328 = yylhsminor.yy328;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy380, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy380, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy15, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
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
