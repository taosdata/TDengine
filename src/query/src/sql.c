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
#define YYNOCODE 277
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateTableSql* yy56;
  int yy70;
  SCreatedTableInfo yy84;
  SRelationInfo* yy114;
  int32_t yy202;
  SIntervalVal yy222;
  SSqlNode* yy224;
  SCreateDbInfo yy246;
  tSqlExpr* yy260;
  TAOS_FIELD yy363;
  SSessionWindowVal yy365;
  SCreateAcctInfo yy377;
  int64_t yy387;
  SArray* yy403;
  SLimitVal yy404;
  tVariant yy488;
  SWindowStateVal yy544;
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
#define YYNSTATE             363
#define YYNRULE              291
#define YYNTOKEN             196
#define YY_MAX_SHIFT         362
#define YY_MIN_SHIFTREDUCE   571
#define YY_MAX_SHIFTREDUCE   861
#define YY_ERROR_ACTION      862
#define YY_ACCEPT_ACTION     863
#define YY_NO_ACTION         864
#define YY_MIN_REDUCE        865
#define YY_MAX_REDUCE        1155
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
#define YY_ACTTAB_COUNT (757)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   169,  622,  235,  622,  229,  360, 1020, 1042,  241,  623,
 /*    10 */   246,  623, 1020,   57,   58,  153,   61,   62,  280,   38,
 /*    20 */   249,   51,  622,   60,  318,   65,   63,   66,   64,  990,
 /*    30 */   623,  988,  989,   56,   55,  160,  991,   54,   53,   52,
 /*    40 */   992,  160,  993,  994,  863,  362, 1033,  572,  573,  574,
 /*    50 */   575,  576,  577,  578,  579,  580,  581,  582,  583,  584,
 /*    60 */   585,  361,  232,  231,  230,   57,   58, 1017,   61,   62,
 /*    70 */   207,  658,  249,   51, 1039,   60,  318,   65,   63,   66,
 /*    80 */    64, 1132, 1006,  278,  277,   56,   55,   80,   98,   54,
 /*    90 */    53,   52,   57,   58, 1033,   61,   62,  207,   86,  249,
 /*   100 */    51, 1014,   60,  318,   65,   63,   66,   64, 1131, 1081,
 /*   110 */   271,  290,   56,   55,  316, 1080,   54,   53,   52,   57,
 /*   120 */    59,  243,   61,   62, 1008, 1020,  249,   51,   95,   60,
 /*   130 */   318,   65,   63,   66,   64,   45,  800,   23, 1033,   56,
 /*   140 */    55,  160,  348,   54,   53,   52,   58,  242,   61,   62,
 /*   150 */   765,  766,  249,   51,  233,   60,  318,   65,   63,   66,
 /*   160 */    64, 1003, 1004,   35, 1007,   56,   55,  316,  245,   54,
 /*   170 */    53,   52,   44,  314,  355,  354,  313,  312,  311,  353,
 /*   180 */   310,  309,  308,  352,  307,  351,  350,  982,  970,  971,
 /*   190 */   972,  973,  974,  975,  976,  977,  978,  979,  980,  981,
 /*   200 */   983,  984,   61,   62,   24,  204,  249,   51,  263,   60,
 /*   210 */   318,   65,   63,   66,   64,   92,   74,  267,  266,   56,
 /*   220 */    55,  258,  210,   54,   53,   52,  248,  815,  207,  216,
 /*   230 */   804,  174,  807,  123,  810,  137,  136,  135,  215, 1132,
 /*   240 */   248,  815,  323,   86,  804,  348,  807,  160,  810,  250,
 /*   250 */   913,  205,   65,   63,   66,   64,   75,  188,  227,  228,
 /*   260 */    56,   55,  319,  923,   54,   53,   52,    5,   41,  178,
 /*   270 */   188,  622,  227,  228,  177,  104,  109,  100,  108,  623,
 /*   280 */    45,  730,  356,  951,  727,   38,  728,   38,  729,  121,
 /*   290 */   115,  126,  252,  303,   38,  806,  125,  809,  131,  134,
 /*   300 */   124,  805,  270,  808,   78,  257,   38,  128,   67,  207,
 /*   310 */   211,  223,  254,  255,    3,  189,  198,  196,  194,  292,
 /*   320 */  1132,   91,   67,  193,  141,  140,  139,  138, 1128,  239,
 /*   330 */    38,  240,   38, 1017,   44, 1017,  355,  354,  327,  258,
 /*   340 */    38,  353, 1017,  816,  811,  352,   38,  351,  350,  175,
 /*   350 */   812,  746,   38,   29, 1016,   38,  743,  816,  811, 1127,
 /*   360 */    38,   56,   55,  706,  812,   54,   53,   52,  253,  258,
 /*   370 */   251,   14,  326,  325,  328,   94,  329,  782, 1017, 1018,
 /*   380 */  1017,  259,    9,  256,  330,  333,  332,  272, 1017,   79,
 /*   390 */   334,  338,  337,   93, 1017,  914,  335,  731,  732,  336,
 /*   400 */  1017,   39,  188, 1017,  340,   97,   83,   81, 1017,   54,
 /*   410 */    53,   52,  359,  358,  599,  813,  151,  149,  148,    1,
 /*   420 */   176,  814,   84, 1005,  762,  772,  773,  716,   71,  802,
 /*   430 */   247,  295,  718,  297,  781,  717,  836,  750,  155,   68,
 /*   440 */    26,   39,   34,  274,  320,   39,   68,   96,  817,   68,
 /*   450 */    25,   16,    6,   15,  621,  114,   18,  113,   17,  274,
 /*   460 */    77,  735,   25,  736,  733,  803,  734,   20,   25,   19,
 /*   470 */    72,  120,   22,  119,   21,  298,  133,  132, 1126,  225,
 /*   480 */   226,  208,  209,  212,  206,  213,  214,  218, 1019,  219,
 /*   490 */  1151, 1143,  705,  220,  217,  203,  268, 1091, 1090,  237,
 /*   500 */  1087, 1086,  238,  339,  170,  152, 1041, 1052,   48,  150,
 /*   510 */  1049, 1050, 1034,  275, 1054,  154,  159,  286, 1015, 1073,
 /*   520 */  1072,  279,  171, 1013,  234,  281,  172,  163,  173, 1031,
 /*   530 */   928,  300,  301,  302,  305,  306,   46,  761,  201,  162,
 /*   540 */   283,  161,  293,   42,  317,  165,   76,  819,  291,  922,
 /*   550 */    73,   50,  164,  324, 1150,  111, 1149,  289,  285, 1146,
 /*   560 */   179,  331, 1142,  287,  117, 1141, 1138,  180,  166,  282,
 /*   570 */   948,   33,   43,   40,   47,  202,   49,  910,  127,  908,
 /*   580 */   129,  130,  906,  905,  260,  191,  192,  902,  901,  900,
 /*   590 */   899,  898,  897,  896,  195,  197,  304,  892,  890,  888,
 /*   600 */   199,  885,  200,  349,  273,   82,   87,  284, 1074,  122,
 /*   610 */   341,  342,  343,  344,  345,  346,  347,  224,  244,  299,
 /*   620 */   357,  861,  261,  262,  221,  222,  860,  264,  265,  105,
 /*   630 */   927,  926,  106,  859,  842,  269,  841,  274,  294,   10,
 /*   640 */   276,  738,   85,   88,  904,  903,   30,  142,  187,  182,
 /*   650 */   949,  181,  183,  184,  185,  143,  186,  895,  894,  986,
 /*   660 */     2,  144,  145,  887,  950,  886,    4,  763,  167,  168,
 /*   670 */   156,  158,  774,  157,  236,  768,   89,   31,  770,  996,
 /*   680 */    90,  288,   11,   32,   12,   13,   27,  296,   28,   97,
 /*   690 */    99,  102,   36,  101,  636,   37,  103,  671,  669,  668,
 /*   700 */   667,  665,  664,  663,  660,  626,  315,    7,  107,  818,
 /*   710 */   820,  321,  322,    8,  110,  112,   69,   70,  116,  708,
 /*   720 */    39,  707,  118,  704,  652,  650,  642,  648,  644,  646,
 /*   730 */   640,  638,  674,  673,  672,  670,  666,  662,  661,  190,
 /*   740 */   624,  589,  587,  865,  864,  864,  864,  864,  864,  864,
 /*   750 */   864,  864,  864,  864,  864,  146,  147,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   252,    1,  244,    1,  198,  199,  248,  199,  244,    9,
 /*    10 */   205,    9,  248,   13,   14,  199,   16,   17,  270,  199,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  222,
 /*    30 */     9,  224,  225,   33,   34,  199,  229,   37,   38,   39,
 /*    40 */   233,  199,  235,  236,  196,  197,  246,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  262,  243,   62,   13,   14,  247,   16,   17,
 /*    70 */   265,    5,   20,   21,  266,   23,   24,   25,   26,   27,
 /*    80 */    28,  276,    0,  267,  268,   33,   34,   87,  206,   37,
 /*    90 */    38,   39,   13,   14,  246,   16,   17,  265,   83,   20,
 /*   100 */    21,  199,   23,   24,   25,   26,   27,   28,  276,  273,
 /*   110 */   262,  275,   33,   34,   85,  273,   37,   38,   39,   13,
 /*   120 */    14,  244,   16,   17,  242,  248,   20,   21,  206,   23,
 /*   130 */    24,   25,   26,   27,   28,  120,   84,  265,  246,   33,
 /*   140 */    34,  199,   91,   37,   38,   39,   14,  245,   16,   17,
 /*   150 */   126,  127,   20,   21,  262,   23,   24,   25,   26,   27,
 /*   160 */    28,  239,  240,  241,  242,   33,   34,   85,  205,   37,
 /*   170 */    38,   39,   99,  100,  101,  102,  103,  104,  105,  106,
 /*   180 */   107,  108,  109,  110,  111,  112,  113,  222,  223,  224,
 /*   190 */   225,  226,  227,  228,  229,  230,  231,  232,  233,  234,
 /*   200 */   235,  236,   16,   17,   44,  265,   20,   21,  143,   23,
 /*   210 */    24,   25,   26,   27,   28,  273,   98,  152,  153,   33,
 /*   220 */    34,  199,   62,   37,   38,   39,    1,    2,  265,   69,
 /*   230 */     5,  209,    7,   79,    9,   75,   76,   77,   78,  276,
 /*   240 */     1,    2,   82,   83,    5,   91,    7,  199,    9,  205,
 /*   250 */   204,  265,   25,   26,   27,   28,  138,  211,   33,   34,
 /*   260 */    33,   34,   37,  204,   37,   38,   39,   63,   64,   65,
 /*   270 */   211,    1,   33,   34,   70,   71,   72,   73,   74,    9,
 /*   280 */   120,    2,  220,  221,    5,  199,    7,  199,    9,   63,
 /*   290 */    64,   65,   69,   89,  199,    5,   70,    7,   72,   73,
 /*   300 */    74,    5,  142,    7,  144,   69,  199,   81,   83,  265,
 /*   310 */   265,  151,   33,   34,  202,  203,   63,   64,   65,  271,
 /*   320 */   276,  273,   83,   70,   71,   72,   73,   74,  265,  243,
 /*   330 */   199,  243,  199,  247,   99,  247,  101,  102,  243,  199,
 /*   340 */   199,  106,  247,  118,  119,  110,  199,  112,  113,  209,
 /*   350 */   125,   37,  199,   83,  247,  199,   98,  118,  119,  265,
 /*   360 */   199,   33,   34,    5,  125,   37,   38,   39,  145,  199,
 /*   370 */   147,   83,  149,  150,  243,   87,  243,   77,  247,  209,
 /*   380 */   247,  145,  124,  147,  243,  149,  150,   84,  247,  206,
 /*   390 */   243,   33,   34,  249,  247,  204,  243,  118,  119,  243,
 /*   400 */   247,   98,  211,  247,  243,  117,   84,  263,  247,   37,
 /*   410 */    38,   39,   66,   67,   68,  125,   63,   64,   65,  207,
 /*   420 */   208,  125,   84,  240,   84,   84,   84,   84,   98,    1,
 /*   430 */    61,   84,   84,   84,  134,   84,   84,  123,   98,   98,
 /*   440 */    98,   98,   83,  121,   15,   98,   98,   98,   84,   98,
 /*   450 */    98,  146,   83,  148,   84,  146,  146,  148,  148,  121,
 /*   460 */    83,    5,   98,    7,    5,   37,    7,  146,   98,  148,
 /*   470 */   140,  146,  146,  148,  148,  116,   79,   80,  265,  265,
 /*   480 */   265,  265,  265,  265,  265,  265,  265,  265,  248,  265,
 /*   490 */   248,  248,  115,  265,  265,  265,  199,  238,  238,  238,
 /*   500 */   238,  238,  238,  238,  250,  199,  199,  199,  264,   61,
 /*   510 */   199,  199,  246,  246,  199,  199,  199,  199,  246,  274,
 /*   520 */   274,  269,  199,  199,  269,  269,  199,  258,  199,  261,
 /*   530 */   199,  199,  199,  199,  199,  199,  199,  125,  199,  259,
 /*   540 */   269,  260,  132,  199,  199,  256,  137,  118,  135,  199,
 /*   550 */   139,  136,  257,  199,  199,  199,  199,  130,  128,  199,
 /*   560 */   199,  199,  199,  129,  199,  199,  199,  199,  255,  131,
 /*   570 */   199,  251,  199,  199,  199,  199,  141,  199,  199,  199,
 /*   580 */   199,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   590 */   199,  199,  199,  199,  199,  199,   90,  199,  199,  199,
 /*   600 */   199,  199,  199,  114,  200,  200,  200,  200,  200,   97,
 /*   610 */    96,   51,   93,   95,   55,   94,   92,  200,  200,  200,
 /*   620 */    85,    5,  154,    5,  200,  200,    5,  154,    5,  206,
 /*   630 */   210,  210,  206,    5,  101,  143,  100,  121,  116,   83,
 /*   640 */    98,   84,  122,   98,  200,  200,   83,  201,  212,  217,
 /*   650 */   219,  218,  213,  216,  214,  201,  215,  200,  200,  237,
 /*   660 */   207,  201,  201,  200,  221,  200,  202,   84,  254,  253,
 /*   670 */    83,   98,   84,   83,    1,   84,   83,   98,   84,  237,
 /*   680 */    83,   83,  133,   98,  133,   83,   83,  116,   83,  117,
 /*   690 */    79,   71,   88,   87,    5,   88,   87,    9,    5,    5,
 /*   700 */     5,    5,    5,    5,    5,   86,   15,   83,   79,   84,
 /*   710 */   118,   24,   59,   83,  148,  148,   16,   16,  148,    5,
 /*   720 */    98,    5,  148,   84,    5,    5,    5,    5,    5,    5,
 /*   730 */     5,    5,    5,    5,    5,    5,    5,    5,    5,   98,
 /*   740 */    86,   61,   60,    0,  277,  277,  277,  277,  277,  277,
 /*   750 */   277,  277,  277,  277,  277,   21,   21,  277,  277,  277,
 /*   760 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   770 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   780 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   790 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   800 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   810 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   820 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   830 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   840 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   850 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   860 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   870 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   880 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   890 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   900 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   910 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   920 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   930 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   940 */   277,  277,  277,  277,  277,  277,  277,  277,  277,  277,
 /*   950 */   277,  277,  277,
};
#define YY_SHIFT_COUNT    (362)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (743)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   160,   73,   73,  235,  235,   29,  225,  239,  239,  270,
 /*    10 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    20 */    21,   21,   21,    0,    2,  239,  279,  279,  279,   15,
 /*    30 */    15,   21,   21,   24,   21,   82,   21,   21,   21,   21,
 /*    40 */   154,   29,   51,   51,   66,  757,  757,  757,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  239,  239,  239,  239,  239,  239,  279,  279,
 /*    70 */   279,  358,  358,  358,  358,  358,  358,  358,   21,   21,
 /*    80 */    21,  314,   21,   21,   21,   15,   15,   21,   21,   21,
 /*    90 */    21,  300,  300,  258,   15,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   120 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   130 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   140 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   150 */    21,   21,  448,  448,  448,  412,  412,  412,  412,  448,
 /*   160 */   448,  409,  411,  410,  415,  413,  427,  434,  430,  438,
 /*   170 */   435,  448,  448,  448,  506,  506,  489,   29,   29,  448,
 /*   180 */   448,  512,  514,  560,  519,  518,  559,  521,  524,  489,
 /*   190 */    66,  448,  448,  535,  535,  448,  535,  448,  535,  448,
 /*   200 */   448,  757,  757,   52,   79,   79,  106,   79,  132,  186,
 /*   210 */   204,  227,  227,  227,  227,  226,  253,  328,  328,  328,
 /*   220 */   328,  223,  236,   65,  288,  372,  372,  290,  296,  346,
 /*   230 */   353,  303,  322,  338,  340,  341,  342,  330,  118,  343,
 /*   240 */   347,  348,  349,  351,  359,  352,  364,  428,  369,  429,
 /*   250 */   370,  305,  309,  310,  456,  459,  321,  325,  377,  326,
 /*   260 */   397,  616,  468,  618,  621,  473,  623,  628,  533,  536,
 /*   270 */   492,  516,  522,  556,  520,  557,  563,  542,  545,  583,
 /*   280 */   587,  588,  590,  591,  573,  593,  594,  597,  673,  598,
 /*   290 */   579,  549,  585,  551,  602,  522,  603,  571,  605,  572,
 /*   300 */   611,  604,  606,  620,  689,  607,  609,  688,  693,  694,
 /*   310 */   695,  696,  697,  698,  699,  619,  691,  629,  624,  625,
 /*   320 */   592,  630,  687,  653,  700,  566,  567,  622,  622,  622,
 /*   330 */   622,  701,  570,  574,  622,  622,  622,  714,  716,  639,
 /*   340 */   622,  719,  720,  721,  722,  723,  724,  725,  726,  727,
 /*   350 */   728,  729,  730,  731,  732,  733,  641,  654,  734,  735,
 /*   360 */   680,  682,  743,
};
#define YY_REDUCE_COUNT (202)
#define YY_REDUCE_MIN   (-252)
#define YY_REDUCE_MAX   (465)
static const short yy_reduce_ofst[] = {
 /*     0 */  -152,  -35,  -35, -193, -193,  -78, -195,  -37,   44, -184,
 /*    10 */  -180, -164,   48,   86,   88,   95,  131,  133,  141,  147,
 /*    20 */   153,  156,  161, -192, -194, -168, -242, -236, -123, -200,
 /*    30 */  -108, -158,  -58, -252,  -98, -118,   22,  140,  170,  107,
 /*    40 */    46,  183,   59,  191,   62,  144,  212,  112, -128,  -60,
 /*    50 */   -14,   45,   63,   94,  213,  214,  215,  216,  217,  218,
 /*    60 */   219,  220,  221,  222,  224,  228,  229,  230,  240,  242,
 /*    70 */   243,  259,  260,  261,  262,  263,  264,  265,  297,  306,
 /*    80 */   307,  244,  308,  311,  312,  266,  267,  315,  316,  317,
 /*    90 */   318,  245,  246,  254,  272,  323,  324,  327,  329,  331,
 /*   100 */   332,  333,  334,  335,  336,  337,  339,  344,  345,  350,
 /*   110 */   354,  355,  356,  357,  360,  361,  362,  363,  365,  366,
 /*   120 */   367,  368,  371,  373,  374,  375,  376,  378,  379,  380,
 /*   130 */   381,  382,  383,  384,  385,  386,  387,  388,  389,  390,
 /*   140 */   391,  392,  393,  394,  395,  396,  398,  399,  400,  401,
 /*   150 */   402,  403,  404,  405,  406,  252,  255,  256,  271,  407,
 /*   160 */   408,  268,  281,  280,  269,  295,  289,  313,  414,  416,
 /*   170 */   320,  417,  418,  419,  420,  421,  422,  423,  426,  424,
 /*   180 */   425,  431,  433,  432,  439,  437,  440,  441,  436,  442,
 /*   190 */   443,  444,  445,  446,  454,  457,  460,  458,  461,  463,
 /*   200 */   465,  453,  464,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   862,  985,  924,  995,  911,  921, 1134, 1134, 1134,  862,
 /*    10 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*    20 */   862,  862,  862, 1043,  882, 1134,  862,  862,  862,  862,
 /*    30 */   862,  862,  862, 1058,  862,  921,  862,  862,  862,  862,
 /*    40 */   931,  921,  931,  931,  862, 1038,  969,  987,  862,  862,
 /*    50 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*    60 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*    70 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*    80 */   862, 1045, 1051, 1048,  862,  862,  862, 1053,  862,  862,
 /*    90 */   862, 1077, 1077, 1036,  862,  862,  862,  862,  862,  862,
 /*   100 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   110 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   120 */   862,  862,  862,  862,  862,  862,  862,  909,  862,  907,
 /*   130 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   140 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   150 */   862,  862,  884,  884,  884,  862,  862,  862,  862,  884,
 /*   160 */   884, 1084, 1088, 1070, 1082, 1078, 1065, 1063, 1061, 1069,
 /*   170 */  1092,  884,  884,  884,  929,  929,  925,  921,  921,  884,
 /*   180 */   884,  947,  945,  943,  935,  941,  937,  939,  933,  912,
 /*   190 */   862,  884,  884,  919,  919,  884,  919,  884,  919,  884,
 /*   200 */   884,  969,  987,  862, 1093, 1083,  862, 1133, 1123, 1122,
 /*   210 */   862, 1129, 1121, 1120, 1119,  862,  862, 1115, 1118, 1117,
 /*   220 */  1116,  862,  862,  862,  862, 1125, 1124,  862,  862,  862,
 /*   230 */   862,  862,  862,  862,  862,  862,  862, 1089, 1085,  862,
 /*   240 */   862,  862,  862,  862,  862,  862,  862,  862, 1095,  862,
 /*   250 */   862,  862,  862,  862,  862,  862,  862,  862,  997,  862,
 /*   260 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   270 */   862, 1035,  862,  862,  862,  862,  862, 1047, 1046,  862,
 /*   280 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   290 */  1079,  862, 1071,  862,  862, 1009,  862,  862,  862,  862,
 /*   300 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   310 */   862,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   320 */   862,  862,  862,  862,  862,  862,  862, 1152, 1147, 1148,
 /*   330 */  1145,  862,  862,  862, 1144, 1139, 1140,  862,  862,  862,
 /*   340 */  1137,  862,  862,  862,  862,  862,  862,  862,  862,  862,
 /*   350 */   862,  862,  862,  862,  862,  862,  953,  862,  891,  889,
 /*   360 */   862,  880,  862,
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
    1,  /*     STABLE => ID */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TOPIC => nothing */
    0,  /*   FUNCTION => nothing */
    0,  /*      DNODE => nothing */
    0,  /*       USER => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    1,  /*       DESC => ID */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*    COMPACT => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*         AS => nothing */
    0,  /* OUTPUTTYPE => nothing */
    0,  /*  AGGREGATE => nothing */
    0,  /*    BUFSIZE => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*      COMMA => nothing */
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
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    1,  /*       NULL => ID */
    1,  /*        NOW => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*      EVERY => nothing */
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    0,  /*      GROUP => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      LIMIT => nothing */
    1,  /*     OFFSET => ID */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      WHERE => nothing */
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
  /*   64 */ "STABLE",
  /*   65 */ "DATABASE",
  /*   66 */ "TABLES",
  /*   67 */ "STABLES",
  /*   68 */ "VGROUPS",
  /*   69 */ "DROP",
  /*   70 */ "TOPIC",
  /*   71 */ "FUNCTION",
  /*   72 */ "DNODE",
  /*   73 */ "USER",
  /*   74 */ "ACCOUNT",
  /*   75 */ "USE",
  /*   76 */ "DESCRIBE",
  /*   77 */ "DESC",
  /*   78 */ "ALTER",
  /*   79 */ "PASS",
  /*   80 */ "PRIVILEGE",
  /*   81 */ "LOCAL",
  /*   82 */ "COMPACT",
  /*   83 */ "LP",
  /*   84 */ "RP",
  /*   85 */ "IF",
  /*   86 */ "EXISTS",
  /*   87 */ "AS",
  /*   88 */ "OUTPUTTYPE",
  /*   89 */ "AGGREGATE",
  /*   90 */ "BUFSIZE",
  /*   91 */ "PPS",
  /*   92 */ "TSERIES",
  /*   93 */ "DBS",
  /*   94 */ "STORAGE",
  /*   95 */ "QTIME",
  /*   96 */ "CONNS",
  /*   97 */ "STATE",
  /*   98 */ "COMMA",
  /*   99 */ "KEEP",
  /*  100 */ "CACHE",
  /*  101 */ "REPLICA",
  /*  102 */ "QUORUM",
  /*  103 */ "DAYS",
  /*  104 */ "MINROWS",
  /*  105 */ "MAXROWS",
  /*  106 */ "BLOCKS",
  /*  107 */ "CTIME",
  /*  108 */ "WAL",
  /*  109 */ "FSYNC",
  /*  110 */ "COMP",
  /*  111 */ "PRECISION",
  /*  112 */ "UPDATE",
  /*  113 */ "CACHELAST",
  /*  114 */ "PARTITIONS",
  /*  115 */ "UNSIGNED",
  /*  116 */ "TAGS",
  /*  117 */ "USING",
  /*  118 */ "NULL",
  /*  119 */ "NOW",
  /*  120 */ "SELECT",
  /*  121 */ "UNION",
  /*  122 */ "ALL",
  /*  123 */ "DISTINCT",
  /*  124 */ "FROM",
  /*  125 */ "VARIABLE",
  /*  126 */ "INTERVAL",
  /*  127 */ "EVERY",
  /*  128 */ "SESSION",
  /*  129 */ "STATE_WINDOW",
  /*  130 */ "FILL",
  /*  131 */ "SLIDING",
  /*  132 */ "ORDER",
  /*  133 */ "BY",
  /*  134 */ "ASC",
  /*  135 */ "GROUP",
  /*  136 */ "HAVING",
  /*  137 */ "LIMIT",
  /*  138 */ "OFFSET",
  /*  139 */ "SLIMIT",
  /*  140 */ "SOFFSET",
  /*  141 */ "WHERE",
  /*  142 */ "RESET",
  /*  143 */ "QUERY",
  /*  144 */ "SYNCDB",
  /*  145 */ "ADD",
  /*  146 */ "COLUMN",
  /*  147 */ "MODIFY",
  /*  148 */ "TAG",
  /*  149 */ "CHANGE",
  /*  150 */ "SET",
  /*  151 */ "KILL",
  /*  152 */ "CONNECTION",
  /*  153 */ "STREAM",
  /*  154 */ "COLON",
  /*  155 */ "ABORT",
  /*  156 */ "AFTER",
  /*  157 */ "ATTACH",
  /*  158 */ "BEFORE",
  /*  159 */ "BEGIN",
  /*  160 */ "CASCADE",
  /*  161 */ "CLUSTER",
  /*  162 */ "CONFLICT",
  /*  163 */ "COPY",
  /*  164 */ "DEFERRED",
  /*  165 */ "DELIMITERS",
  /*  166 */ "DETACH",
  /*  167 */ "EACH",
  /*  168 */ "END",
  /*  169 */ "EXPLAIN",
  /*  170 */ "FAIL",
  /*  171 */ "FOR",
  /*  172 */ "IGNORE",
  /*  173 */ "IMMEDIATE",
  /*  174 */ "INITIALLY",
  /*  175 */ "INSTEAD",
  /*  176 */ "MATCH",
  /*  177 */ "KEY",
  /*  178 */ "OF",
  /*  179 */ "RAISE",
  /*  180 */ "REPLACE",
  /*  181 */ "RESTRICT",
  /*  182 */ "ROW",
  /*  183 */ "STATEMENT",
  /*  184 */ "TRIGGER",
  /*  185 */ "VIEW",
  /*  186 */ "SEMI",
  /*  187 */ "NONE",
  /*  188 */ "PREV",
  /*  189 */ "LINEAR",
  /*  190 */ "IMPORT",
  /*  191 */ "TBNAME",
  /*  192 */ "JOIN",
  /*  193 */ "INSERT",
  /*  194 */ "INTO",
  /*  195 */ "VALUES",
  /*  196 */ "program",
  /*  197 */ "cmd",
  /*  198 */ "dbPrefix",
  /*  199 */ "ids",
  /*  200 */ "cpxName",
  /*  201 */ "ifexists",
  /*  202 */ "alter_db_optr",
  /*  203 */ "alter_topic_optr",
  /*  204 */ "acct_optr",
  /*  205 */ "exprlist",
  /*  206 */ "ifnotexists",
  /*  207 */ "db_optr",
  /*  208 */ "topic_optr",
  /*  209 */ "typename",
  /*  210 */ "bufsize",
  /*  211 */ "pps",
  /*  212 */ "tseries",
  /*  213 */ "dbs",
  /*  214 */ "streams",
  /*  215 */ "storage",
  /*  216 */ "qtime",
  /*  217 */ "users",
  /*  218 */ "conns",
  /*  219 */ "state",
  /*  220 */ "intitemlist",
  /*  221 */ "intitem",
  /*  222 */ "keep",
  /*  223 */ "cache",
  /*  224 */ "replica",
  /*  225 */ "quorum",
  /*  226 */ "days",
  /*  227 */ "minrows",
  /*  228 */ "maxrows",
  /*  229 */ "blocks",
  /*  230 */ "ctime",
  /*  231 */ "wal",
  /*  232 */ "fsync",
  /*  233 */ "comp",
  /*  234 */ "prec",
  /*  235 */ "update",
  /*  236 */ "cachelast",
  /*  237 */ "partitions",
  /*  238 */ "signed",
  /*  239 */ "create_table_args",
  /*  240 */ "create_stable_args",
  /*  241 */ "create_table_list",
  /*  242 */ "create_from_stable",
  /*  243 */ "columnlist",
  /*  244 */ "tagitemlist",
  /*  245 */ "tagNamelist",
  /*  246 */ "select",
  /*  247 */ "column",
  /*  248 */ "tagitem",
  /*  249 */ "selcollist",
  /*  250 */ "from",
  /*  251 */ "where_opt",
  /*  252 */ "interval_option",
  /*  253 */ "sliding_opt",
  /*  254 */ "session_option",
  /*  255 */ "windowstate_option",
  /*  256 */ "fill_opt",
  /*  257 */ "groupby_opt",
  /*  258 */ "having_opt",
  /*  259 */ "orderby_opt",
  /*  260 */ "slimit_opt",
  /*  261 */ "limit_opt",
  /*  262 */ "union",
  /*  263 */ "sclp",
  /*  264 */ "distinct",
  /*  265 */ "expr",
  /*  266 */ "as",
  /*  267 */ "tablelist",
  /*  268 */ "sub",
  /*  269 */ "tmvar",
  /*  270 */ "intervalKey",
  /*  271 */ "sortlist",
  /*  272 */ "sortitem",
  /*  273 */ "item",
  /*  274 */ "sortorder",
  /*  275 */ "grouplist",
  /*  276 */ "expritem",
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
 /*  22 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
 /*  23 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  25 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  27 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS",
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
 /*  39 */ "cmd ::= DESC ids cpxName",
 /*  40 */ "cmd ::= ALTER USER ids PASS ids",
 /*  41 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  42 */ "cmd ::= ALTER DNODE ids ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  44 */ "cmd ::= ALTER LOCAL ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids ids",
 /*  46 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  47 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  50 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  51 */ "ids ::= ID",
 /*  52 */ "ids ::= STRING",
 /*  53 */ "ifexists ::= IF EXISTS",
 /*  54 */ "ifexists ::=",
 /*  55 */ "ifnotexists ::= IF NOT EXISTS",
 /*  56 */ "ifnotexists ::=",
 /*  57 */ "cmd ::= CREATE DNODE ids",
 /*  58 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  59 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  60 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  61 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  62 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  63 */ "cmd ::= CREATE USER ids PASS ids",
 /*  64 */ "bufsize ::=",
 /*  65 */ "bufsize ::= BUFSIZE INTEGER",
 /*  66 */ "pps ::=",
 /*  67 */ "pps ::= PPS INTEGER",
 /*  68 */ "tseries ::=",
 /*  69 */ "tseries ::= TSERIES INTEGER",
 /*  70 */ "dbs ::=",
 /*  71 */ "dbs ::= DBS INTEGER",
 /*  72 */ "streams ::=",
 /*  73 */ "streams ::= STREAMS INTEGER",
 /*  74 */ "storage ::=",
 /*  75 */ "storage ::= STORAGE INTEGER",
 /*  76 */ "qtime ::=",
 /*  77 */ "qtime ::= QTIME INTEGER",
 /*  78 */ "users ::=",
 /*  79 */ "users ::= USERS INTEGER",
 /*  80 */ "conns ::=",
 /*  81 */ "conns ::= CONNS INTEGER",
 /*  82 */ "state ::=",
 /*  83 */ "state ::= STATE ids",
 /*  84 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  85 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  86 */ "intitemlist ::= intitem",
 /*  87 */ "intitem ::= INTEGER",
 /*  88 */ "keep ::= KEEP intitemlist",
 /*  89 */ "cache ::= CACHE INTEGER",
 /*  90 */ "replica ::= REPLICA INTEGER",
 /*  91 */ "quorum ::= QUORUM INTEGER",
 /*  92 */ "days ::= DAYS INTEGER",
 /*  93 */ "minrows ::= MINROWS INTEGER",
 /*  94 */ "maxrows ::= MAXROWS INTEGER",
 /*  95 */ "blocks ::= BLOCKS INTEGER",
 /*  96 */ "ctime ::= CTIME INTEGER",
 /*  97 */ "wal ::= WAL INTEGER",
 /*  98 */ "fsync ::= FSYNC INTEGER",
 /*  99 */ "comp ::= COMP INTEGER",
 /* 100 */ "prec ::= PRECISION STRING",
 /* 101 */ "update ::= UPDATE INTEGER",
 /* 102 */ "cachelast ::= CACHELAST INTEGER",
 /* 103 */ "partitions ::= PARTITIONS INTEGER",
 /* 104 */ "db_optr ::=",
 /* 105 */ "db_optr ::= db_optr cache",
 /* 106 */ "db_optr ::= db_optr replica",
 /* 107 */ "db_optr ::= db_optr quorum",
 /* 108 */ "db_optr ::= db_optr days",
 /* 109 */ "db_optr ::= db_optr minrows",
 /* 110 */ "db_optr ::= db_optr maxrows",
 /* 111 */ "db_optr ::= db_optr blocks",
 /* 112 */ "db_optr ::= db_optr ctime",
 /* 113 */ "db_optr ::= db_optr wal",
 /* 114 */ "db_optr ::= db_optr fsync",
 /* 115 */ "db_optr ::= db_optr comp",
 /* 116 */ "db_optr ::= db_optr prec",
 /* 117 */ "db_optr ::= db_optr keep",
 /* 118 */ "db_optr ::= db_optr update",
 /* 119 */ "db_optr ::= db_optr cachelast",
 /* 120 */ "topic_optr ::= db_optr",
 /* 121 */ "topic_optr ::= topic_optr partitions",
 /* 122 */ "alter_db_optr ::=",
 /* 123 */ "alter_db_optr ::= alter_db_optr replica",
 /* 124 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 125 */ "alter_db_optr ::= alter_db_optr keep",
 /* 126 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 127 */ "alter_db_optr ::= alter_db_optr comp",
 /* 128 */ "alter_db_optr ::= alter_db_optr update",
 /* 129 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 130 */ "alter_topic_optr ::= alter_db_optr",
 /* 131 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 132 */ "typename ::= ids",
 /* 133 */ "typename ::= ids LP signed RP",
 /* 134 */ "typename ::= ids UNSIGNED",
 /* 135 */ "signed ::= INTEGER",
 /* 136 */ "signed ::= PLUS INTEGER",
 /* 137 */ "signed ::= MINUS INTEGER",
 /* 138 */ "cmd ::= CREATE TABLE create_table_args",
 /* 139 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 140 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 141 */ "cmd ::= CREATE TABLE create_table_list",
 /* 142 */ "create_table_list ::= create_from_stable",
 /* 143 */ "create_table_list ::= create_table_list create_from_stable",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 145 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 146 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 147 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 148 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 149 */ "tagNamelist ::= ids",
 /* 150 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 151 */ "columnlist ::= columnlist COMMA column",
 /* 152 */ "columnlist ::= column",
 /* 153 */ "column ::= ids typename",
 /* 154 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 155 */ "tagitemlist ::= tagitem",
 /* 156 */ "tagitem ::= INTEGER",
 /* 157 */ "tagitem ::= FLOAT",
 /* 158 */ "tagitem ::= STRING",
 /* 159 */ "tagitem ::= BOOL",
 /* 160 */ "tagitem ::= NULL",
 /* 161 */ "tagitem ::= NOW",
 /* 162 */ "tagitem ::= MINUS INTEGER",
 /* 163 */ "tagitem ::= MINUS FLOAT",
 /* 164 */ "tagitem ::= PLUS INTEGER",
 /* 165 */ "tagitem ::= PLUS FLOAT",
 /* 166 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 167 */ "select ::= LP select RP",
 /* 168 */ "union ::= select",
 /* 169 */ "union ::= union UNION ALL select",
 /* 170 */ "cmd ::= union",
 /* 171 */ "select ::= SELECT selcollist",
 /* 172 */ "sclp ::= selcollist COMMA",
 /* 173 */ "sclp ::=",
 /* 174 */ "selcollist ::= sclp distinct expr as",
 /* 175 */ "selcollist ::= sclp STAR",
 /* 176 */ "as ::= AS ids",
 /* 177 */ "as ::= ids",
 /* 178 */ "as ::=",
 /* 179 */ "distinct ::= DISTINCT",
 /* 180 */ "distinct ::=",
 /* 181 */ "from ::= FROM tablelist",
 /* 182 */ "from ::= FROM sub",
 /* 183 */ "sub ::= LP union RP",
 /* 184 */ "sub ::= LP union RP ids",
 /* 185 */ "sub ::= sub COMMA LP union RP ids",
 /* 186 */ "tablelist ::= ids cpxName",
 /* 187 */ "tablelist ::= ids cpxName ids",
 /* 188 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 189 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 190 */ "tmvar ::= VARIABLE",
 /* 191 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 192 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 193 */ "interval_option ::=",
 /* 194 */ "intervalKey ::= INTERVAL",
 /* 195 */ "intervalKey ::= EVERY",
 /* 196 */ "session_option ::=",
 /* 197 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 198 */ "windowstate_option ::=",
 /* 199 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 200 */ "fill_opt ::=",
 /* 201 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 202 */ "fill_opt ::= FILL LP ID RP",
 /* 203 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 204 */ "sliding_opt ::=",
 /* 205 */ "orderby_opt ::=",
 /* 206 */ "orderby_opt ::= ORDER BY sortlist",
 /* 207 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 208 */ "sortlist ::= item sortorder",
 /* 209 */ "item ::= ids cpxName",
 /* 210 */ "sortorder ::= ASC",
 /* 211 */ "sortorder ::= DESC",
 /* 212 */ "sortorder ::=",
 /* 213 */ "groupby_opt ::=",
 /* 214 */ "groupby_opt ::= GROUP BY grouplist",
 /* 215 */ "grouplist ::= grouplist COMMA item",
 /* 216 */ "grouplist ::= item",
 /* 217 */ "having_opt ::=",
 /* 218 */ "having_opt ::= HAVING expr",
 /* 219 */ "limit_opt ::=",
 /* 220 */ "limit_opt ::= LIMIT signed",
 /* 221 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 222 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 223 */ "slimit_opt ::=",
 /* 224 */ "slimit_opt ::= SLIMIT signed",
 /* 225 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 226 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 227 */ "where_opt ::=",
 /* 228 */ "where_opt ::= WHERE expr",
 /* 229 */ "expr ::= LP expr RP",
 /* 230 */ "expr ::= ID",
 /* 231 */ "expr ::= ID DOT ID",
 /* 232 */ "expr ::= ID DOT STAR",
 /* 233 */ "expr ::= INTEGER",
 /* 234 */ "expr ::= MINUS INTEGER",
 /* 235 */ "expr ::= PLUS INTEGER",
 /* 236 */ "expr ::= FLOAT",
 /* 237 */ "expr ::= MINUS FLOAT",
 /* 238 */ "expr ::= PLUS FLOAT",
 /* 239 */ "expr ::= STRING",
 /* 240 */ "expr ::= NOW",
 /* 241 */ "expr ::= VARIABLE",
 /* 242 */ "expr ::= PLUS VARIABLE",
 /* 243 */ "expr ::= MINUS VARIABLE",
 /* 244 */ "expr ::= BOOL",
 /* 245 */ "expr ::= NULL",
 /* 246 */ "expr ::= ID LP exprlist RP",
 /* 247 */ "expr ::= ID LP STAR RP",
 /* 248 */ "expr ::= expr IS NULL",
 /* 249 */ "expr ::= expr IS NOT NULL",
 /* 250 */ "expr ::= expr LT expr",
 /* 251 */ "expr ::= expr GT expr",
 /* 252 */ "expr ::= expr LE expr",
 /* 253 */ "expr ::= expr GE expr",
 /* 254 */ "expr ::= expr NE expr",
 /* 255 */ "expr ::= expr EQ expr",
 /* 256 */ "expr ::= expr BETWEEN expr AND expr",
 /* 257 */ "expr ::= expr AND expr",
 /* 258 */ "expr ::= expr OR expr",
 /* 259 */ "expr ::= expr PLUS expr",
 /* 260 */ "expr ::= expr MINUS expr",
 /* 261 */ "expr ::= expr STAR expr",
 /* 262 */ "expr ::= expr SLASH expr",
 /* 263 */ "expr ::= expr REM expr",
 /* 264 */ "expr ::= expr LIKE expr",
 /* 265 */ "expr ::= expr IN LP exprlist RP",
 /* 266 */ "exprlist ::= exprlist COMMA expritem",
 /* 267 */ "exprlist ::= expritem",
 /* 268 */ "expritem ::= expr",
 /* 269 */ "expritem ::=",
 /* 270 */ "cmd ::= RESET QUERY CACHE",
 /* 271 */ "cmd ::= SYNCDB ids REPLICA",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 288 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 289 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 290 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 205: /* exprlist */
    case 249: /* selcollist */
    case 263: /* sclp */
{
tSqlExprListDestroy((yypminor->yy403));
}
      break;
    case 220: /* intitemlist */
    case 222: /* keep */
    case 243: /* columnlist */
    case 244: /* tagitemlist */
    case 245: /* tagNamelist */
    case 256: /* fill_opt */
    case 257: /* groupby_opt */
    case 259: /* orderby_opt */
    case 271: /* sortlist */
    case 275: /* grouplist */
{
taosArrayDestroy((yypminor->yy403));
}
      break;
    case 241: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy56));
}
      break;
    case 246: /* select */
{
destroySqlNode((yypminor->yy224));
}
      break;
    case 250: /* from */
    case 267: /* tablelist */
    case 268: /* sub */
{
destroyRelationInfo((yypminor->yy114));
}
      break;
    case 251: /* where_opt */
    case 258: /* having_opt */
    case 265: /* expr */
    case 276: /* expritem */
{
tSqlExprDestroy((yypminor->yy260));
}
      break;
    case 262: /* union */
{
destroyAllSqlNode((yypminor->yy403));
}
      break;
    case 272: /* sortitem */
{
tVariantDestroy(&(yypminor->yy488));
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
    /* assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD ); */
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( i>=YY_NLOOKAHEAD || yy_lookahead[i]!=iLookAhead ){
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
          j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) &&
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

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;       /* Symbol on the left-hand side of the rule */
  signed char nrhs;     /* Negative of the number of RHS symbols in the rule */
} yyRuleInfo[] = {
  {  196,   -1 }, /* (0) program ::= cmd */
  {  197,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  197,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  197,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  197,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  197,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  197,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  197,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  197,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  197,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  197,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  197,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  197,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  197,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  197,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  197,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  197,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  198,    0 }, /* (17) dbPrefix ::= */
  {  198,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  200,    0 }, /* (19) cpxName ::= */
  {  200,   -2 }, /* (20) cpxName ::= DOT ids */
  {  197,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  197,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  197,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  197,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  197,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  197,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  197,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  197,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  197,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  197,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  197,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  197,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  197,   -3 }, /* (33) cmd ::= DROP FUNCTION ids */
  {  197,   -3 }, /* (34) cmd ::= DROP DNODE ids */
  {  197,   -3 }, /* (35) cmd ::= DROP USER ids */
  {  197,   -3 }, /* (36) cmd ::= DROP ACCOUNT ids */
  {  197,   -2 }, /* (37) cmd ::= USE ids */
  {  197,   -3 }, /* (38) cmd ::= DESCRIBE ids cpxName */
  {  197,   -3 }, /* (39) cmd ::= DESC ids cpxName */
  {  197,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  197,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  197,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  197,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  197,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  197,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  197,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  197,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  197,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  197,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  197,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  199,   -1 }, /* (51) ids ::= ID */
  {  199,   -1 }, /* (52) ids ::= STRING */
  {  201,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  201,    0 }, /* (54) ifexists ::= */
  {  206,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  206,    0 }, /* (56) ifnotexists ::= */
  {  197,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  197,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  197,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  197,   -5 }, /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  197,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  197,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  197,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  210,    0 }, /* (64) bufsize ::= */
  {  210,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  211,    0 }, /* (66) pps ::= */
  {  211,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  212,    0 }, /* (68) tseries ::= */
  {  212,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  213,    0 }, /* (70) dbs ::= */
  {  213,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  214,    0 }, /* (72) streams ::= */
  {  214,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  215,    0 }, /* (74) storage ::= */
  {  215,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  216,    0 }, /* (76) qtime ::= */
  {  216,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  217,    0 }, /* (78) users ::= */
  {  217,   -2 }, /* (79) users ::= USERS INTEGER */
  {  218,    0 }, /* (80) conns ::= */
  {  218,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  219,    0 }, /* (82) state ::= */
  {  219,   -2 }, /* (83) state ::= STATE ids */
  {  204,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  220,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  220,   -1 }, /* (86) intitemlist ::= intitem */
  {  221,   -1 }, /* (87) intitem ::= INTEGER */
  {  222,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  223,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  224,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  225,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  226,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  227,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  228,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  229,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  230,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  231,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  232,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  233,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  234,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  235,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  236,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  237,   -2 }, /* (103) partitions ::= PARTITIONS INTEGER */
  {  207,    0 }, /* (104) db_optr ::= */
  {  207,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  207,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  207,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  207,   -2 }, /* (108) db_optr ::= db_optr days */
  {  207,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  207,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  207,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  207,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  207,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  207,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  207,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  207,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  207,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  207,   -2 }, /* (118) db_optr ::= db_optr update */
  {  207,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  208,   -1 }, /* (120) topic_optr ::= db_optr */
  {  208,   -2 }, /* (121) topic_optr ::= topic_optr partitions */
  {  202,    0 }, /* (122) alter_db_optr ::= */
  {  202,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  202,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  202,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  202,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  202,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  202,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  202,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  203,   -1 }, /* (130) alter_topic_optr ::= alter_db_optr */
  {  203,   -2 }, /* (131) alter_topic_optr ::= alter_topic_optr partitions */
  {  209,   -1 }, /* (132) typename ::= ids */
  {  209,   -4 }, /* (133) typename ::= ids LP signed RP */
  {  209,   -2 }, /* (134) typename ::= ids UNSIGNED */
  {  238,   -1 }, /* (135) signed ::= INTEGER */
  {  238,   -2 }, /* (136) signed ::= PLUS INTEGER */
  {  238,   -2 }, /* (137) signed ::= MINUS INTEGER */
  {  197,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_args */
  {  197,   -3 }, /* (139) cmd ::= CREATE TABLE create_stable_args */
  {  197,   -3 }, /* (140) cmd ::= CREATE STABLE create_stable_args */
  {  197,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_list */
  {  241,   -1 }, /* (142) create_table_list ::= create_from_stable */
  {  241,   -2 }, /* (143) create_table_list ::= create_table_list create_from_stable */
  {  239,   -6 }, /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  240,  -10 }, /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  242,  -10 }, /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  242,  -13 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  245,   -3 }, /* (148) tagNamelist ::= tagNamelist COMMA ids */
  {  245,   -1 }, /* (149) tagNamelist ::= ids */
  {  239,   -5 }, /* (150) create_table_args ::= ifnotexists ids cpxName AS select */
  {  243,   -3 }, /* (151) columnlist ::= columnlist COMMA column */
  {  243,   -1 }, /* (152) columnlist ::= column */
  {  247,   -2 }, /* (153) column ::= ids typename */
  {  244,   -3 }, /* (154) tagitemlist ::= tagitemlist COMMA tagitem */
  {  244,   -1 }, /* (155) tagitemlist ::= tagitem */
  {  248,   -1 }, /* (156) tagitem ::= INTEGER */
  {  248,   -1 }, /* (157) tagitem ::= FLOAT */
  {  248,   -1 }, /* (158) tagitem ::= STRING */
  {  248,   -1 }, /* (159) tagitem ::= BOOL */
  {  248,   -1 }, /* (160) tagitem ::= NULL */
  {  248,   -1 }, /* (161) tagitem ::= NOW */
  {  248,   -2 }, /* (162) tagitem ::= MINUS INTEGER */
  {  248,   -2 }, /* (163) tagitem ::= MINUS FLOAT */
  {  248,   -2 }, /* (164) tagitem ::= PLUS INTEGER */
  {  248,   -2 }, /* (165) tagitem ::= PLUS FLOAT */
  {  246,  -14 }, /* (166) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  246,   -3 }, /* (167) select ::= LP select RP */
  {  262,   -1 }, /* (168) union ::= select */
  {  262,   -4 }, /* (169) union ::= union UNION ALL select */
  {  197,   -1 }, /* (170) cmd ::= union */
  {  246,   -2 }, /* (171) select ::= SELECT selcollist */
  {  263,   -2 }, /* (172) sclp ::= selcollist COMMA */
  {  263,    0 }, /* (173) sclp ::= */
  {  249,   -4 }, /* (174) selcollist ::= sclp distinct expr as */
  {  249,   -2 }, /* (175) selcollist ::= sclp STAR */
  {  266,   -2 }, /* (176) as ::= AS ids */
  {  266,   -1 }, /* (177) as ::= ids */
  {  266,    0 }, /* (178) as ::= */
  {  264,   -1 }, /* (179) distinct ::= DISTINCT */
  {  264,    0 }, /* (180) distinct ::= */
  {  250,   -2 }, /* (181) from ::= FROM tablelist */
  {  250,   -2 }, /* (182) from ::= FROM sub */
  {  268,   -3 }, /* (183) sub ::= LP union RP */
  {  268,   -4 }, /* (184) sub ::= LP union RP ids */
  {  268,   -6 }, /* (185) sub ::= sub COMMA LP union RP ids */
  {  267,   -2 }, /* (186) tablelist ::= ids cpxName */
  {  267,   -3 }, /* (187) tablelist ::= ids cpxName ids */
  {  267,   -4 }, /* (188) tablelist ::= tablelist COMMA ids cpxName */
  {  267,   -5 }, /* (189) tablelist ::= tablelist COMMA ids cpxName ids */
  {  269,   -1 }, /* (190) tmvar ::= VARIABLE */
  {  252,   -4 }, /* (191) interval_option ::= intervalKey LP tmvar RP */
  {  252,   -6 }, /* (192) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  252,    0 }, /* (193) interval_option ::= */
  {  270,   -1 }, /* (194) intervalKey ::= INTERVAL */
  {  270,   -1 }, /* (195) intervalKey ::= EVERY */
  {  254,    0 }, /* (196) session_option ::= */
  {  254,   -7 }, /* (197) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  255,    0 }, /* (198) windowstate_option ::= */
  {  255,   -4 }, /* (199) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  256,    0 }, /* (200) fill_opt ::= */
  {  256,   -6 }, /* (201) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  256,   -4 }, /* (202) fill_opt ::= FILL LP ID RP */
  {  253,   -4 }, /* (203) sliding_opt ::= SLIDING LP tmvar RP */
  {  253,    0 }, /* (204) sliding_opt ::= */
  {  259,    0 }, /* (205) orderby_opt ::= */
  {  259,   -3 }, /* (206) orderby_opt ::= ORDER BY sortlist */
  {  271,   -4 }, /* (207) sortlist ::= sortlist COMMA item sortorder */
  {  271,   -2 }, /* (208) sortlist ::= item sortorder */
  {  273,   -2 }, /* (209) item ::= ids cpxName */
  {  274,   -1 }, /* (210) sortorder ::= ASC */
  {  274,   -1 }, /* (211) sortorder ::= DESC */
  {  274,    0 }, /* (212) sortorder ::= */
  {  257,    0 }, /* (213) groupby_opt ::= */
  {  257,   -3 }, /* (214) groupby_opt ::= GROUP BY grouplist */
  {  275,   -3 }, /* (215) grouplist ::= grouplist COMMA item */
  {  275,   -1 }, /* (216) grouplist ::= item */
  {  258,    0 }, /* (217) having_opt ::= */
  {  258,   -2 }, /* (218) having_opt ::= HAVING expr */
  {  261,    0 }, /* (219) limit_opt ::= */
  {  261,   -2 }, /* (220) limit_opt ::= LIMIT signed */
  {  261,   -4 }, /* (221) limit_opt ::= LIMIT signed OFFSET signed */
  {  261,   -4 }, /* (222) limit_opt ::= LIMIT signed COMMA signed */
  {  260,    0 }, /* (223) slimit_opt ::= */
  {  260,   -2 }, /* (224) slimit_opt ::= SLIMIT signed */
  {  260,   -4 }, /* (225) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  260,   -4 }, /* (226) slimit_opt ::= SLIMIT signed COMMA signed */
  {  251,    0 }, /* (227) where_opt ::= */
  {  251,   -2 }, /* (228) where_opt ::= WHERE expr */
  {  265,   -3 }, /* (229) expr ::= LP expr RP */
  {  265,   -1 }, /* (230) expr ::= ID */
  {  265,   -3 }, /* (231) expr ::= ID DOT ID */
  {  265,   -3 }, /* (232) expr ::= ID DOT STAR */
  {  265,   -1 }, /* (233) expr ::= INTEGER */
  {  265,   -2 }, /* (234) expr ::= MINUS INTEGER */
  {  265,   -2 }, /* (235) expr ::= PLUS INTEGER */
  {  265,   -1 }, /* (236) expr ::= FLOAT */
  {  265,   -2 }, /* (237) expr ::= MINUS FLOAT */
  {  265,   -2 }, /* (238) expr ::= PLUS FLOAT */
  {  265,   -1 }, /* (239) expr ::= STRING */
  {  265,   -1 }, /* (240) expr ::= NOW */
  {  265,   -1 }, /* (241) expr ::= VARIABLE */
  {  265,   -2 }, /* (242) expr ::= PLUS VARIABLE */
  {  265,   -2 }, /* (243) expr ::= MINUS VARIABLE */
  {  265,   -1 }, /* (244) expr ::= BOOL */
  {  265,   -1 }, /* (245) expr ::= NULL */
  {  265,   -4 }, /* (246) expr ::= ID LP exprlist RP */
  {  265,   -4 }, /* (247) expr ::= ID LP STAR RP */
  {  265,   -3 }, /* (248) expr ::= expr IS NULL */
  {  265,   -4 }, /* (249) expr ::= expr IS NOT NULL */
  {  265,   -3 }, /* (250) expr ::= expr LT expr */
  {  265,   -3 }, /* (251) expr ::= expr GT expr */
  {  265,   -3 }, /* (252) expr ::= expr LE expr */
  {  265,   -3 }, /* (253) expr ::= expr GE expr */
  {  265,   -3 }, /* (254) expr ::= expr NE expr */
  {  265,   -3 }, /* (255) expr ::= expr EQ expr */
  {  265,   -5 }, /* (256) expr ::= expr BETWEEN expr AND expr */
  {  265,   -3 }, /* (257) expr ::= expr AND expr */
  {  265,   -3 }, /* (258) expr ::= expr OR expr */
  {  265,   -3 }, /* (259) expr ::= expr PLUS expr */
  {  265,   -3 }, /* (260) expr ::= expr MINUS expr */
  {  265,   -3 }, /* (261) expr ::= expr STAR expr */
  {  265,   -3 }, /* (262) expr ::= expr SLASH expr */
  {  265,   -3 }, /* (263) expr ::= expr REM expr */
  {  265,   -3 }, /* (264) expr ::= expr LIKE expr */
  {  265,   -5 }, /* (265) expr ::= expr IN LP exprlist RP */
  {  205,   -3 }, /* (266) exprlist ::= exprlist COMMA expritem */
  {  205,   -1 }, /* (267) exprlist ::= expritem */
  {  276,   -1 }, /* (268) expritem ::= expr */
  {  276,    0 }, /* (269) expritem ::= */
  {  197,   -3 }, /* (270) cmd ::= RESET QUERY CACHE */
  {  197,   -3 }, /* (271) cmd ::= SYNCDB ids REPLICA */
  {  197,   -7 }, /* (272) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  197,   -7 }, /* (273) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  197,   -7 }, /* (274) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  197,   -7 }, /* (275) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  197,   -7 }, /* (276) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  197,   -8 }, /* (277) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  197,   -9 }, /* (278) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  197,   -7 }, /* (279) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  197,   -7 }, /* (280) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  197,   -7 }, /* (281) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  197,   -7 }, /* (282) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  197,   -7 }, /* (283) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  197,   -7 }, /* (284) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  197,   -8 }, /* (285) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  197,   -9 }, /* (286) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  197,   -7 }, /* (287) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  197,   -3 }, /* (288) cmd ::= KILL CONNECTION INTEGER */
  {  197,   -5 }, /* (289) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  197,   -5 }, /* (290) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 138: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==138);
      case 139: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==139);
      case 140: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==140);
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
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
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
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 35: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 36: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 37: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 38: /* cmd ::= DESCRIBE ids cpxName */
      case 39: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==39);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 40: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 41: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 44: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 46: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 47: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==47);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy246, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy377);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy377);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy403);}
        break;
      case 51: /* ids ::= ID */
      case 52: /* ids ::= STRING */ yytestcase(yyruleno==52);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 53: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 54: /* ifexists ::= */
      case 56: /* ifnotexists ::= */ yytestcase(yyruleno==56);
      case 180: /* distinct ::= */ yytestcase(yyruleno==180);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy377);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy246, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy363, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy363, &yymsp[0].minor.yy0, 2);}
        break;
      case 63: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 64: /* bufsize ::= */
      case 66: /* pps ::= */ yytestcase(yyruleno==66);
      case 68: /* tseries ::= */ yytestcase(yyruleno==68);
      case 70: /* dbs ::= */ yytestcase(yyruleno==70);
      case 72: /* streams ::= */ yytestcase(yyruleno==72);
      case 74: /* storage ::= */ yytestcase(yyruleno==74);
      case 76: /* qtime ::= */ yytestcase(yyruleno==76);
      case 78: /* users ::= */ yytestcase(yyruleno==78);
      case 80: /* conns ::= */ yytestcase(yyruleno==80);
      case 82: /* state ::= */ yytestcase(yyruleno==82);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 65: /* bufsize ::= BUFSIZE INTEGER */
      case 67: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==69);
      case 71: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==75);
      case 77: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==77);
      case 79: /* users ::= USERS INTEGER */ yytestcase(yyruleno==79);
      case 81: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==81);
      case 83: /* state ::= STATE ids */ yytestcase(yyruleno==83);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 84: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy377.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy377.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy377.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy377.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy377.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy377.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy377.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy377.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy377.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy377 = yylhsminor.yy377;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 154: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==154);
{ yylhsminor.yy403 = tVariantListAppend(yymsp[-2].minor.yy403, &yymsp[0].minor.yy488, -1);    }
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 86: /* intitemlist ::= intitem */
      case 155: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==155);
{ yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 87: /* intitem ::= INTEGER */
      case 156: /* tagitem ::= INTEGER */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= STRING */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= BOOL */ yytestcase(yyruleno==159);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy403 = yymsp[0].minor.yy403; }
        break;
      case 89: /* cache ::= CACHE INTEGER */
      case 90: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==90);
      case 91: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==91);
      case 92: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==92);
      case 93: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==96);
      case 97: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==97);
      case 98: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==98);
      case 99: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==99);
      case 100: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==100);
      case 101: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==101);
      case 102: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==102);
      case 103: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==103);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 104: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy246); yymsp[1].minor.yy246.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.keep = yymsp[0].minor.yy403; }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy246 = yymsp[0].minor.yy246; yylhsminor.yy246.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy246 = yylhsminor.yy246;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy246 = yymsp[-1].minor.yy246; yylhsminor.yy246.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy246 = yylhsminor.yy246;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy246); yymsp[1].minor.yy246.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy363, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy363 = yylhsminor.yy363;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy387 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy363, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy387;  // negative value of name length
    tSetColumnType(&yylhsminor.yy363, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy363 = yylhsminor.yy363;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy363, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy363 = yylhsminor.yy363;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy387 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy387 = yylhsminor.yy387;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy387 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy387 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy56;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy84);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy56 = pCreateTable;
}
  yymsp[0].minor.yy56 = yylhsminor.yy56;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy56->childTableInfo, &yymsp[0].minor.yy84);
  yylhsminor.yy56 = yymsp[-1].minor.yy56;
}
  yymsp[-1].minor.yy56 = yylhsminor.yy56;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy56 = tSetCreateTableInfo(yymsp[-1].minor.yy403, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy56, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy56 = yylhsminor.yy56;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy56 = tSetCreateTableInfo(yymsp[-5].minor.yy403, yymsp[-1].minor.yy403, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy56, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy56 = yylhsminor.yy56;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy84 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy403, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy84 = yylhsminor.yy84;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy84 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy403, yymsp[-1].minor.yy403, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy84 = yylhsminor.yy84;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy403, &yymsp[0].minor.yy0); yylhsminor.yy403 = yymsp[-2].minor.yy403;  }
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy403 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy403, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy56 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy224, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy56, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy56 = yylhsminor.yy56;
        break;
      case 151: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy403, &yymsp[0].minor.yy363); yylhsminor.yy403 = yymsp[-2].minor.yy403;  }
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 152: /* columnlist ::= column */
{yylhsminor.yy403 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy403, &yymsp[0].minor.yy363);}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 153: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy363, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy363);
}
  yymsp[-1].minor.yy363 = yylhsminor.yy363;
        break;
      case 160: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 161: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy488, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy488 = yylhsminor.yy488;
        break;
      case 162: /* tagitem ::= MINUS INTEGER */
      case 163: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==163);
      case 164: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==164);
      case 165: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==165);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 166: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy224 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy403, yymsp[-11].minor.yy114, yymsp[-10].minor.yy260, yymsp[-4].minor.yy403, yymsp[-2].minor.yy403, &yymsp[-9].minor.yy222, &yymsp[-7].minor.yy365, &yymsp[-6].minor.yy544, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy403, &yymsp[0].minor.yy404, &yymsp[-1].minor.yy404, yymsp[-3].minor.yy260);
}
  yymsp[-13].minor.yy224 = yylhsminor.yy224;
        break;
      case 167: /* select ::= LP select RP */
{yymsp[-2].minor.yy224 = yymsp[-1].minor.yy224;}
        break;
      case 168: /* union ::= select */
{ yylhsminor.yy403 = setSubclause(NULL, yymsp[0].minor.yy224); }
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 169: /* union ::= union UNION ALL select */
{ yylhsminor.yy403 = appendSelectClause(yymsp[-3].minor.yy403, yymsp[0].minor.yy224); }
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 170: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy403, NULL, TSDB_SQL_SELECT); }
        break;
      case 171: /* select ::= SELECT selcollist */
{
  yylhsminor.yy224 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy403, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy224 = yylhsminor.yy224;
        break;
      case 172: /* sclp ::= selcollist COMMA */
{yylhsminor.yy403 = yymsp[-1].minor.yy403;}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 173: /* sclp ::= */
      case 205: /* orderby_opt ::= */ yytestcase(yyruleno==205);
{yymsp[1].minor.yy403 = 0;}
        break;
      case 174: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy403 = tSqlExprListAppend(yymsp[-3].minor.yy403, yymsp[-1].minor.yy260,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 175: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy403 = tSqlExprListAppend(yymsp[-1].minor.yy403, pNode, 0, 0);
}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 176: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 177: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 179: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 181: /* from ::= FROM tablelist */
      case 182: /* from ::= FROM sub */ yytestcase(yyruleno==182);
{yymsp[-1].minor.yy114 = yymsp[0].minor.yy114;}
        break;
      case 183: /* sub ::= LP union RP */
{yymsp[-2].minor.yy114 = addSubqueryElem(NULL, yymsp[-1].minor.yy403, NULL);}
        break;
      case 184: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy114 = addSubqueryElem(NULL, yymsp[-2].minor.yy403, &yymsp[0].minor.yy0);}
        break;
      case 185: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy114 = addSubqueryElem(yymsp[-5].minor.yy114, yymsp[-2].minor.yy403, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy114 = yylhsminor.yy114;
        break;
      case 186: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy114 = yylhsminor.yy114;
        break;
      case 187: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy114 = yylhsminor.yy114;
        break;
      case 188: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(yymsp[-3].minor.yy114, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy114 = yylhsminor.yy114;
        break;
      case 189: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy114 = setTableNameList(yymsp[-4].minor.yy114, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy114 = yylhsminor.yy114;
        break;
      case 190: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 191: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy222.interval = yymsp[-1].minor.yy0; yylhsminor.yy222.offset.n = 0; yylhsminor.yy222.token = yymsp[-3].minor.yy202;}
  yymsp[-3].minor.yy222 = yylhsminor.yy222;
        break;
      case 192: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy222.interval = yymsp[-3].minor.yy0; yylhsminor.yy222.offset = yymsp[-1].minor.yy0;   yylhsminor.yy222.token = yymsp[-5].minor.yy202;}
  yymsp[-5].minor.yy222 = yylhsminor.yy222;
        break;
      case 193: /* interval_option ::= */
{memset(&yymsp[1].minor.yy222, 0, sizeof(yymsp[1].minor.yy222));}
        break;
      case 194: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy202 = TK_INTERVAL;}
        break;
      case 195: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy202 = TK_EVERY;   }
        break;
      case 196: /* session_option ::= */
{yymsp[1].minor.yy365.col.n = 0; yymsp[1].minor.yy365.gap.n = 0;}
        break;
      case 197: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy365.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy365.gap = yymsp[-1].minor.yy0;
}
        break;
      case 198: /* windowstate_option ::= */
{ yymsp[1].minor.yy544.col.n = 0; yymsp[1].minor.yy544.col.z = NULL;}
        break;
      case 199: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy544.col = yymsp[-1].minor.yy0; }
        break;
      case 200: /* fill_opt ::= */
{ yymsp[1].minor.yy403 = 0;     }
        break;
      case 201: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy403, &A, -1, 0);
    yymsp[-5].minor.yy403 = yymsp[-1].minor.yy403;
}
        break;
      case 202: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy403 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 203: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 204: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 206: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy403 = yymsp[0].minor.yy403;}
        break;
      case 207: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy403 = tVariantListAppend(yymsp[-3].minor.yy403, &yymsp[-1].minor.yy488, yymsp[0].minor.yy70);
}
  yymsp[-3].minor.yy403 = yylhsminor.yy403;
        break;
      case 208: /* sortlist ::= item sortorder */
{
  yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[-1].minor.yy488, yymsp[0].minor.yy70);
}
  yymsp[-1].minor.yy403 = yylhsminor.yy403;
        break;
      case 209: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy488, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy488 = yylhsminor.yy488;
        break;
      case 210: /* sortorder ::= ASC */
{ yymsp[0].minor.yy70 = TSDB_ORDER_ASC; }
        break;
      case 211: /* sortorder ::= DESC */
{ yymsp[0].minor.yy70 = TSDB_ORDER_DESC;}
        break;
      case 212: /* sortorder ::= */
{ yymsp[1].minor.yy70 = TSDB_ORDER_ASC; }
        break;
      case 213: /* groupby_opt ::= */
{ yymsp[1].minor.yy403 = 0;}
        break;
      case 214: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy403 = yymsp[0].minor.yy403;}
        break;
      case 215: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy403 = tVariantListAppend(yymsp[-2].minor.yy403, &yymsp[0].minor.yy488, -1);
}
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 216: /* grouplist ::= item */
{
  yylhsminor.yy403 = tVariantListAppend(NULL, &yymsp[0].minor.yy488, -1);
}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 217: /* having_opt ::= */
      case 227: /* where_opt ::= */ yytestcase(yyruleno==227);
      case 269: /* expritem ::= */ yytestcase(yyruleno==269);
{yymsp[1].minor.yy260 = 0;}
        break;
      case 218: /* having_opt ::= HAVING expr */
      case 228: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==228);
{yymsp[-1].minor.yy260 = yymsp[0].minor.yy260;}
        break;
      case 219: /* limit_opt ::= */
      case 223: /* slimit_opt ::= */ yytestcase(yyruleno==223);
{yymsp[1].minor.yy404.limit = -1; yymsp[1].minor.yy404.offset = 0;}
        break;
      case 220: /* limit_opt ::= LIMIT signed */
      case 224: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==224);
{yymsp[-1].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-1].minor.yy404.offset = 0;}
        break;
      case 221: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy404.limit = yymsp[-2].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[0].minor.yy387;}
        break;
      case 222: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[-2].minor.yy387;}
        break;
      case 225: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy404.limit = yymsp[-2].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[0].minor.yy387;}
        break;
      case 226: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy404.limit = yymsp[0].minor.yy387;  yymsp[-3].minor.yy404.offset = yymsp[-2].minor.yy387;}
        break;
      case 229: /* expr ::= LP expr RP */
{yylhsminor.yy260 = yymsp[-1].minor.yy260; yylhsminor.yy260->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy260->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 230: /* expr ::= ID */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 231: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 232: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 233: /* expr ::= INTEGER */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 234: /* expr ::= MINUS INTEGER */
      case 235: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==235);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 236: /* expr ::= FLOAT */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 237: /* expr ::= MINUS FLOAT */
      case 238: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==238);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 239: /* expr ::= STRING */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 240: /* expr ::= NOW */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 241: /* expr ::= VARIABLE */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 242: /* expr ::= PLUS VARIABLE */
      case 243: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==243);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy260 = yylhsminor.yy260;
        break;
      case 244: /* expr ::= BOOL */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 245: /* expr ::= NULL */
{ yylhsminor.yy260 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 246: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy260 = tSqlExprCreateFunction(yymsp[-1].minor.yy403, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy260 = yylhsminor.yy260;
        break;
      case 247: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy260 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy260 = yylhsminor.yy260;
        break;
      case 248: /* expr ::= expr IS NULL */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 249: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-3].minor.yy260, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy260 = yylhsminor.yy260;
        break;
      case 250: /* expr ::= expr LT expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_LT);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 251: /* expr ::= expr GT expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_GT);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 252: /* expr ::= expr LE expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_LE);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 253: /* expr ::= expr GE expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_GE);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 254: /* expr ::= expr NE expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_NE);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 255: /* expr ::= expr EQ expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_EQ);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 256: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy260); yylhsminor.yy260 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy260, yymsp[-2].minor.yy260, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy260, TK_LE), TK_AND);}
  yymsp[-4].minor.yy260 = yylhsminor.yy260;
        break;
      case 257: /* expr ::= expr AND expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_AND);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 258: /* expr ::= expr OR expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_OR); }
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 259: /* expr ::= expr PLUS expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_PLUS);  }
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 260: /* expr ::= expr MINUS expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_MINUS); }
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 261: /* expr ::= expr STAR expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_STAR);  }
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 262: /* expr ::= expr SLASH expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_DIVIDE);}
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 263: /* expr ::= expr REM expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_REM);   }
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 264: /* expr ::= expr LIKE expr */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-2].minor.yy260, yymsp[0].minor.yy260, TK_LIKE);  }
  yymsp[-2].minor.yy260 = yylhsminor.yy260;
        break;
      case 265: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy260 = tSqlExprCreate(yymsp[-4].minor.yy260, (tSqlExpr*)yymsp[-1].minor.yy403, TK_IN); }
  yymsp[-4].minor.yy260 = yylhsminor.yy260;
        break;
      case 266: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy403 = tSqlExprListAppend(yymsp[-2].minor.yy403,yymsp[0].minor.yy260,0, 0);}
  yymsp[-2].minor.yy403 = yylhsminor.yy403;
        break;
      case 267: /* exprlist ::= expritem */
{yylhsminor.yy403 = tSqlExprListAppend(0,yymsp[0].minor.yy260,0, 0);}
  yymsp[0].minor.yy403 = yylhsminor.yy403;
        break;
      case 268: /* expritem ::= expr */
{yylhsminor.yy260 = yymsp[0].minor.yy260;}
  yymsp[0].minor.yy260 = yylhsminor.yy260;
        break;
      case 270: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 271: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 278: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 286: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy488, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy403, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 289: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 290: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
